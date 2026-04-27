# Databricks/Fabric Notebook
# Name: nb_match
# Description: GENERIC matching engine — reads entity config from mdm_config,
#              produces match candidates for ANY entity with match_engine != 'none'.
#              Replaces nb_match_location.py (which becomes a thin wrapper).
# Triggerowany przez: PL_MDM_Master_* (z parametrem entity_id)

# ---------------------------------------------------------------------------
# CELL 1: Parameters
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
entity_id = dbutils.widgets.get("entity_id") if "dbutils" in dir() else "business_location"

print(f"run_id={run_id}, entity_id={entity_id}")

# ---------------------------------------------------------------------------
# CELL 2: Imports
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType
from collections import defaultdict
import uuid

try:
    import jellyfish
except ImportError as exc:
    raise ImportError(
        "Pakiet 'jellyfish' nie jest zainstalowany w Fabric environment. "
        "Uruchom: %pip install jellyfish==1.0.3"
    ) from exc

# ---------------------------------------------------------------------------
# CELL 3: Load entity config
# ---------------------------------------------------------------------------

entity_cfg = spark.sql(f"""
    SELECT * FROM mdm_config.entity_config
    WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()
if not entity_cfg:
    raise ValueError(f"Entity '{entity_id}' not found or inactive")
entity_cfg = entity_cfg[0].asDict()

match_engine = entity_cfg.get('match_engine', 'none')
if match_engine == 'none':
    print(f"Matching disabled for entity '{entity_id}' (match_engine='none'). Exiting.")
    dbutils.notebook.exit("0") if "dbutils" in dir() else exit(0)

HUB_TABLE = f"silver_dv.{entity_cfg['hub_table']}"
HK_COL = entity_cfg['hub_table'].replace('hub_', '') + '_hk'
BV_MATCH_TABLE = f"silver_dv.{entity_cfg['bv_match_table']}"
BV_RESOLUTION_TABLE = f"silver_dv.{entity_cfg['bv_resolution_table']}"

match_threshold = entity_cfg['match_threshold']
auto_accept_threshold = entity_cfg['auto_accept_threshold']

print(f"Entity: {entity_id}, engine={match_engine}, threshold={match_threshold}, auto_accept={auto_accept_threshold}")

# ---------------------------------------------------------------------------
# CELL 4: Load field config (weights, blocking keys)
# ---------------------------------------------------------------------------

field_cfg = spark.sql(f"""
    SELECT field_name, match_weight, is_blocking_key, standardizer
    FROM mdm_config.field_config
    WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()

MATCH_WEIGHTS = {row.field_name: row.match_weight for row in field_cfg if row.match_weight > 0}
BLOCKING_KEYS = [row.field_name for row in field_cfg if row.is_blocking_key]
TOTAL_WEIGHT = sum(MATCH_WEIGHTS.values()) or 1.0

print(f"Match weights: {MATCH_WEIGHTS}")
print(f"Blocking keys: {BLOCKING_KEYS}")

# Source priorities
source_prio = spark.sql(f"""
    SELECT source_system, priority
    FROM mdm_config.source_priority
    WHERE entity_id = '{entity_id}' AND field_name = '*'
    ORDER BY priority
""").collect()
SOURCE_PRIORITY = {row.source_system: row.priority for row in source_prio}
if not SOURCE_PRIORITY:
    SOURCE_PRIORITY = {"manual": 1}

# Hash config (satellite table names per source)
hash_cfg = spark.sql(f"""
    SELECT source_system FROM mdm_config.hash_config WHERE entity_id = '{entity_id}'
""").collect()
SAT_TABLES = {row.source_system: f"silver_dv.sat_{entity_id}_{row.source_system}" for row in hash_cfg}

print(f"Source priority: {SOURCE_PRIORITY}, Satellite tables: {SAT_TABLES}")

# ---------------------------------------------------------------------------
# CELL 5: UDFs
# ---------------------------------------------------------------------------

@F.udf(DoubleType())
def jaro_winkler_udf(s1, s2):
    if not s1 or not s2:
        return 0.0
    if s1 == s2:
        return 1.0
    return float(jellyfish.jaro_winkler_similarity(s1, s2))

@F.udf(DoubleType())
def geo_score_udf(lat1, lon1, lat2, lon2):
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None
    from math import radians, sin, cos, sqrt, atan2
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    dist_km = 2 * R * atan2(sqrt(a), sqrt(1 - a))
    if dist_km < 0.5:
        return 1.0
    elif dist_km < 2.0:
        return 0.5
    else:
        return 0.0

# ---------------------------------------------------------------------------
# CELL 6: Build candidate view — hub + latest attributes from ALL satellites
# ---------------------------------------------------------------------------

# Build dynamic SQL: JOIN hub with all satellite tables for this entity
sat_joins = []
coalesce_fields = defaultdict(list)

for idx, (source, sat_table) in enumerate(SAT_TABLES.items()):
    alias = f"s{idx}"
    sat_joins.append(
        f"LEFT JOIN {sat_table} {alias} ON h.{HK_COL} = {alias}.{HK_COL} AND {alias}.load_end_date IS NULL"
    )
    # Discover columns in satellite (excluding DV control cols)
    try:
        sat_cols = [c.name for c in spark.table(sat_table).schema
                    if c.name not in (HK_COL, 'load_date', 'load_end_date', 'hash_diff',
                                      'record_source', 'created_by', 'created_at')]
        for col in sat_cols:
            coalesce_fields[col].append(f"{alias}.{col}")
    except Exception:
        print(f"WARNING: satellite table {sat_table} not found, skipping")

# Build COALESCE expressions for each field
coalesce_exprs = []
for field, aliases in coalesce_fields.items():
    coalesce_exprs.append(f"COALESCE({', '.join(aliases)}) AS {field}")

joins_sql = "\n    ".join(sat_joins)
coalesce_sql = ",\n        ".join(coalesce_exprs) if coalesce_exprs else "h.business_key AS _placeholder"

# Exclude already-resolved hubs (if resolution table exists)
exclude_clause = ""
if entity_cfg.get('bv_resolution_table'):
    exclude_clause = f"""
    WHERE h.{HK_COL} NOT IN (
        SELECT source_hk FROM {BV_RESOLUTION_TABLE}
    )"""

candidates_sql = f"""
    SELECT
        h.{HK_COL},
        h.record_source,
        {coalesce_sql}
    FROM {HUB_TABLE} h
    {joins_sql}
    {exclude_clause}
"""

candidates = spark.sql(candidates_sql)
candidates.cache()
cand_count = candidates.count()
print(f"Candidates to match: {cand_count}")

if cand_count < 2:
    print("Not enough candidates for matching. Exiting.")
    spark.sql(f"""
        INSERT INTO mdm_config.execution_log
          (run_id, entity_id, process_name, status, records_matched, started_at, completed_at)
        VALUES ('{run_id}', '{entity_id}', 'nb_match', 'Completed', 0, current_timestamp(), current_timestamp())
    """)
    dbutils.notebook.exit("0") if "dbutils" in dir() else print("EXIT: 0")

# ---------------------------------------------------------------------------
# CELL 7: Blocking + Self-join
# ---------------------------------------------------------------------------

@F.udf(StringType())
def source_priority_udf(src):
    return str(SOURCE_PRIORITY.get(src, 99))

candidates_prio = candidates.withColumn("src_priority", source_priority_udf(F.col("record_source")))

# Build blocking condition from BLOCKING_KEYS
if BLOCKING_KEYS:
    blocking_conds = [f'(left.{bk}_std = right.{bk}_std)' if f'{bk}_std' in [c.name for c in candidates.schema]
                      else f'(left.{bk} = right.{bk})'
                      for bk in BLOCKING_KEYS]
    blocking_expr = " AND ".join(blocking_conds)
else:
    blocking_expr = "1=1"  # no blocking = full cross join (careful with large datasets)

# Build pairs
pairs = (
    candidates_prio.alias("left")
    .join(
        candidates_prio.alias("right"),
        F.expr(blocking_expr) &
        (F.col("left.src_priority") < F.col("right.src_priority"))
    )
    .filter(F.col(f"left.{HK_COL}") != F.col(f"right.{HK_COL}"))
)

print(f"Candidate pairs after blocking: {pairs.count()}")

# ---------------------------------------------------------------------------
# CELL 8: Scoring — dynamic based on MATCH_WEIGHTS
# ---------------------------------------------------------------------------

score_expr = F.lit(0.0)
component_cols = {}

for field, weight in MATCH_WEIGHTS.items():
    normalized_weight = weight / TOTAL_WEIGHT
    # Use _std columns if available
    std_field = f"{field}_std" if f"{field}_std" in [c.name for c in candidates.schema] else field
    col_name = f"{field}_score"

    if field in ('latitude', 'longitude'):
        # geo scoring handled separately
        continue
    elif match_engine == 'jaro_winkler':
        pairs = pairs.withColumn(col_name,
            jaro_winkler_udf(F.col(f"left.{std_field}"), F.col(f"right.{std_field}")))
    else:
        # exact match = 1.0, else 0.0
        pairs = pairs.withColumn(col_name,
            F.when(F.col(f"left.{std_field}") == F.col(f"right.{std_field}"), F.lit(1.0))
             .otherwise(F.lit(0.0)))

    score_expr = score_expr + F.col(col_name) * F.lit(normalized_weight)
    component_cols[col_name] = True

# Add geo score if latitude/longitude have weights
if 'latitude' in MATCH_WEIGHTS or 'longitude' in MATCH_WEIGHTS:
    geo_weight = (MATCH_WEIGHTS.get('latitude', 0) + MATCH_WEIGHTS.get('longitude', 0)) / TOTAL_WEIGHT
    pairs = pairs.withColumn("geo_score",
        geo_score_udf(F.col("left.latitude"), F.col("left.longitude"),
                       F.col("right.latitude"), F.col("right.longitude")))
    score_expr = score_expr + F.coalesce(F.col("geo_score"), F.lit(0.0)) * F.lit(geo_weight)
    component_cols["geo_score"] = True

pairs = pairs.withColumn("match_score", score_expr)

# Match type classification
pairs = pairs.withColumn("match_type",
    F.when(F.col("match_score") >= 0.99, F.lit("exact"))
     .when(F.col("match_score") >= auto_accept_threshold, F.lit("composite_high"))
     .otherwise(F.lit("fuzzy_composite")))

scored_pairs = pairs.filter(F.col("match_score") >= match_threshold)
print(f"Pairs above threshold ({match_threshold}): {scored_pairs.count()}")

# ---------------------------------------------------------------------------
# CELL 9: Insert to bv_match_candidates (only new pairs)
# ---------------------------------------------------------------------------

existing_pairs = spark.sql(f"SELECT hk_left, hk_right FROM {BV_MATCH_TABLE}")

# Build select list for component scores (dynamic)
component_selects = [F.col(c) if c in component_cols else F.lit(None).cast(DoubleType()).alias(c)
                     for c in ["name_score", "city_match", "zip_match", "geo_score"]]

new_candidates = (
    scored_pairs
    .withColumn("pair_id", F.expr("uuid()"))
    .withColumn("entity_id", F.lit(entity_id))
    .withColumn("status",
        F.when(F.col("match_score") >= auto_accept_threshold, F.lit("auto_accepted"))
         .otherwise(F.lit("pending")))
    .withColumn("reviewed_by",
        F.when(F.col("match_score") >= auto_accept_threshold, F.lit("system_auto"))
         .otherwise(F.lit(None).cast(StringType())))
    .withColumn("reviewed_at",
        F.when(F.col("match_score") >= auto_accept_threshold, F.current_timestamp())
         .otherwise(F.lit(None).cast("timestamp")))
    .select(
        F.col("pair_id"),
        F.col(f"left.{HK_COL}").alias("hk_left"),
        F.col(f"right.{HK_COL}").alias("hk_right"),
        F.col("match_score"), F.col("match_type"),
        *[F.col(c) if c in [f.name for f in scored_pairs.schema] else F.lit(None).cast(DoubleType()).alias(c)
          for c in ["name_score", "geo_score"]],
        *[F.col(c) if c in [f.name for f in scored_pairs.schema] else F.lit(None).cast(BooleanType()).alias(c)
          for c in ["city_match", "zip_match"]],
        F.col("entity_id"),
        F.col("status"),
        F.current_timestamp().alias("created_at"),
        F.col("reviewed_by"), F.col("reviewed_at"),
        F.lit(None).cast(StringType()).alias("review_note")
    )
    .join(
        existing_pairs.alias("ep"),
        (F.col("hk_left") == F.col("ep.hk_left")) & (F.col("hk_right") == F.col("ep.hk_right")),
        "left_anti"
    )
)

inserted_count = new_candidates.count()
new_candidates.write.format("delta").mode("append").saveAsTable(BV_MATCH_TABLE)
print(f"New match candidates inserted: {inserted_count}")

# ---------------------------------------------------------------------------
# CELL 10: Auto-accepted → key resolution
# ---------------------------------------------------------------------------

auto_accepted = spark.sql(f"""
    SELECT pair_id, hk_left, hk_right
    FROM {BV_MATCH_TABLE}
    WHERE status = 'auto_accepted' AND entity_id = '{entity_id}'
      AND pair_id NOT IN (SELECT DISTINCT pair_id FROM {BV_RESOLUTION_TABLE} WHERE pair_id IS NOT NULL)
""")

if auto_accepted.count() > 0:
    auto_resolution = (
        auto_accepted
        .withColumn("source_hk", F.col("hk_right"))
        .withColumn("canonical_hk", F.col("hk_left"))
        .withColumn("resolved_by", F.lit("system_auto"))
        .withColumn("resolved_at", F.current_timestamp())
        .withColumn("resolution_type", F.lit("auto"))
        .withColumn("entity_id", F.lit(entity_id))
        .select("source_hk", "canonical_hk", "resolved_by", "resolved_at", "pair_id",
                "resolution_type", "entity_id")
    )
    auto_resolution.write.format("delta").mode("append").saveAsTable(BV_RESOLUTION_TABLE)
    print(f"Auto-accepted resolutions: {auto_accepted.count()}")

pending_count = spark.sql(
    f"SELECT COUNT(*) FROM {BV_MATCH_TABLE} WHERE status = 'pending' AND entity_id = '{entity_id}'"
).collect()[0][0]

print(f"Pending pairs for steward review: {pending_count}")

# ---------------------------------------------------------------------------
# CELL 11: Log
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, process_name, status, records_matched, started_at, completed_at)
    VALUES
      ('{run_id}', '{entity_id}', 'nb_match',
       'Completed', {inserted_count}, current_timestamp(), current_timestamp())
""")

dbutils.notebook.exit(str(pending_count)) if "dbutils" in dir() else print(f"EXIT: {pending_count}")
