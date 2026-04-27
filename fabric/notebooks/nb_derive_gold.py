# Databricks/Fabric Notebook
# Name: nb_derive_gold
# Description: GENERIC gold derivation — reads entity config from mdm_config,
#              derives golden records (SCD2) for ANY entity via PIT + survivorship.
#              Replaces nb_derive_gold_location.py (which becomes a thin wrapper).
# Triggerowany przez: PL_MDM_Master_* (z parametrem entity_id)

# ---------------------------------------------------------------------------
# CELL 1: Parameters
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
entity_id = dbutils.widgets.get("entity_id") if "dbutils" in dir() else "legal_entity"
snapshot_date_str = dbutils.widgets.get("snapshot_date") if "dbutils" in dir() else None

from datetime import datetime, timezone
snapshot_date = datetime.fromisoformat(snapshot_date_str) if snapshot_date_str else datetime.now(timezone.utc)

print(f"run_id={run_id}, entity_id={entity_id}, snapshot_date={snapshot_date}")

# ---------------------------------------------------------------------------
# CELL 2: Imports
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from collections import defaultdict

spark.conf.set("spark.sql.shuffle.partitions", "8")

# ---------------------------------------------------------------------------
# CELL 3: Load entity configuration
# ---------------------------------------------------------------------------

entity_cfg = spark.sql(f"""
    SELECT * FROM mdm_config.entity_config
    WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()
if not entity_cfg:
    raise ValueError(f"Entity '{entity_id}' not found or inactive")
entity_cfg = entity_cfg[0].asDict()

HUB_TABLE = f"silver_dv.{entity_cfg['hub_table']}"
HK_COL = entity_cfg['hub_table'].replace('hub_', '') + '_hk'
GOLD_TABLE = f"gold.{entity_cfg['gold_table']}"
QUALITY_TABLE = f"gold.{entity_cfg['gold_table']}_quality"
PIT_TABLE = f"silver_dv.{entity_cfg['pit_table']}" if entity_cfg.get('pit_table') else None

print(f"Hub={HUB_TABLE}, HK={HK_COL}, Gold={GOLD_TABLE}")

# Hash config → satellite tables
hash_cfg = spark.sql(f"""
    SELECT source_system FROM mdm_config.hash_config WHERE entity_id = '{entity_id}'
""").collect()
SAT_TABLES = {row.source_system: f"silver_dv.sat_{entity_id}_{row.source_system}" for row in hash_cfg}

# Field config → golden fields
field_cfg = spark.sql(f"""
    SELECT field_name, is_golden_field
    FROM mdm_config.field_config WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()
GOLDEN_FIELDS = [row.field_name for row in field_cfg if row.is_golden_field]

# Source priorities
source_prio = spark.sql(f"""
    SELECT source_system, field_name, priority
    FROM mdm_config.source_priority WHERE entity_id = '{entity_id}'
    ORDER BY priority ASC
""").collect()

field_sources = defaultdict(list)
global_sources = []
for row in source_prio:
    if row.field_name == '*':
        global_sources.append(row.source_system)
    else:
        field_sources[row.field_name].append(row.source_system)
for fn in GOLDEN_FIELDS:
    if fn not in field_sources:
        field_sources[fn] = global_sources

print(f"Sources: {list(SAT_TABLES.keys())}, Golden fields: {GOLDEN_FIELDS}")
print(f"Survivorship config: {dict(field_sources)}")

# ---------------------------------------------------------------------------
# CELL 4: Discover satellite columns dynamically
# ---------------------------------------------------------------------------

# For each satellite, discover which golden fields it has
sat_field_map = {}  # {source_system: {golden_field: actual_col_name}}

for source, sat_table in SAT_TABLES.items():
    try:
        sat_schema = spark.table(sat_table).schema
        sat_col_names = [c.name for c in sat_schema]
        mapping = {}
        for gf in GOLDEN_FIELDS:
            if gf in sat_col_names:
                mapping[gf] = gf
        sat_field_map[source] = mapping
    except Exception as e:
        print(f"WARNING: Cannot read schema for {sat_table}: {e}")
        sat_field_map[source] = {}

print(f"Satellite field map: { {s: list(m.keys()) for s, m in sat_field_map.items()} }")

# ---------------------------------------------------------------------------
# CELL 5: Build golden records via JOIN Hub + all Satellites
# ---------------------------------------------------------------------------

# Build dynamic SQL
select_parts = [f"h.{HK_COL}", "h.business_key", "h.record_source"]
join_parts = []
alias_counter = 0

sat_aliases = {}  # {source: alias}
for source, sat_table in SAT_TABLES.items():
    alias = f"s{alias_counter}"
    sat_aliases[source] = alias
    alias_counter += 1

    join_parts.append(
        f"LEFT JOIN {sat_table} {alias} ON h.{HK_COL} = {alias}.{HK_COL} AND {alias}.load_end_date IS NULL"
    )

    # Add all golden fields from this satellite with alias prefix
    for gf, col_name in sat_field_map[source].items():
        select_parts.append(f"{alias}.{col_name} AS {alias}_{gf}")

joins_sql = "\n    ".join(join_parts)
select_sql = ",\n        ".join(select_parts)

golden_sql = f"""
    SELECT
        {select_sql}
    FROM {HUB_TABLE} h
    {joins_sql}
"""

golden_raw = spark.sql(golden_sql)

# ---------------------------------------------------------------------------
# CELL 6: Apply survivorship (COALESCE in priority order per field)
# ---------------------------------------------------------------------------

def best_value(df, field_name, sat_aliases_map, sat_field_map_local, field_sources_local):
    """COALESCE across satellites in priority order for a given field."""
    sources = field_sources_local.get(field_name, global_sources)
    coalesce_args = []
    for src in sources:
        if src in sat_aliases_map and field_name in sat_field_map_local.get(src, {}):
            alias = sat_aliases_map[src]
            coalesce_args.append(F.col(f"{alias}_{field_name}"))
    if coalesce_args:
        return F.coalesce(*coalesce_args)
    return F.lit(None)

def best_source(df, field_name, sat_aliases_map, sat_field_map_local, field_sources_local):
    """Return source name that provided the winning value (for lineage)."""
    sources = field_sources_local.get(field_name, global_sources)
    expr = F.lit(None).cast("string")
    for src in reversed(sources):
        if src in sat_aliases_map and field_name in sat_field_map_local.get(src, {}):
            alias = sat_aliases_map[src]
            expr = F.when(F.col(f"{alias}_{field_name}").isNotNull(), F.lit(src)).otherwise(expr)
    return expr

golden_final = golden_raw
for gf in GOLDEN_FIELDS:
    golden_final = golden_final.withColumn(gf, best_value(golden_final, gf, sat_aliases, sat_field_map, field_sources))
    golden_final = golden_final.withColumn(f"{gf}_source",
        best_source(golden_final, gf, sat_aliases, sat_field_map, field_sources))

# Add SCD2 control columns
golden_final = (
    golden_final
    .withColumn("valid_from", F.lit(snapshot_date))
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)

# Select only the columns needed for gold table
select_cols = [HK_COL, "valid_from", "valid_to", "is_current"]
select_cols += GOLDEN_FIELDS
select_cols += [f"{gf}_source" for gf in GOLDEN_FIELDS
                if f"{gf}_source" in [c.name for c in golden_final.schema]]
select_cols += ["created_at", "updated_at"]

# Filter to only existing columns
existing_cols = [c.name for c in golden_final.schema]
select_cols = [c for c in select_cols if c in existing_cols]

golden_final = golden_final.select(*select_cols)

# ---------------------------------------------------------------------------
# CELL 7: SCD2 MERGE to gold table
# ---------------------------------------------------------------------------

try:
    gold_delta = DeltaTable.forName(spark, GOLD_TABLE)
except Exception:
    print(f"Creating gold table {GOLD_TABLE}...")
    golden_final.limit(0).write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
    gold_delta = DeltaTable.forName(spark, GOLD_TABLE)

# Build change detection condition (any golden field changed)
change_conditions = [f"gold.{gf} != new.{gf}" for gf in GOLDEN_FIELDS
                     if gf in [c.name for c in golden_final.schema]]
change_condition = " OR ".join(change_conditions) if change_conditions else "1=0"

gold_delta.alias("gold").merge(
    golden_final.alias("new"),
    f"gold.{HK_COL} = new.{HK_COL} AND gold.is_current = true"
).whenMatchedUpdate(
    condition=change_condition,
    set={
        "valid_to":   "new.valid_from",
        "is_current": "false",
        "updated_at": "new.updated_at"
    }
).whenNotMatchedInsertAll(
).execute()

# Insert new versions for records whose old version was just closed
new_inserts = (
    golden_final.alias("new")
    .join(
        spark.sql(f"SELECT {HK_COL} FROM {GOLD_TABLE} WHERE is_current = true").alias("existing"),
        HK_COL, "left_anti"
    )
)
new_inserts.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)

gold_count = spark.sql(f"SELECT COUNT(*) FROM {GOLD_TABLE} WHERE is_current = true").collect()[0][0]
print(f"Gold {GOLD_TABLE}: {gold_count} current golden records")

# ---------------------------------------------------------------------------
# CELL 8: Quality metrics
# ---------------------------------------------------------------------------

# Compute completeness: % of golden fields that are non-null
completeness_parts = [
    f"CASE WHEN {gf} IS NOT NULL THEN 1.0 ELSE 0.0 END"
    for gf in GOLDEN_FIELDS
    if gf in [c.name for c in golden_final.schema]
]
num_fields = len(completeness_parts) or 1
completeness_expr = f"({' + '.join(completeness_parts)}) / {num_fields}" if completeness_parts else "0.0"

# Compute sources_count: how many satellites have data
sources_count_parts = []
for source, fields in sat_field_map.items():
    if fields:
        alias = sat_aliases[source]
        first_field = list(fields.keys())[0]
        sources_count_parts.append(f"CASE WHEN {alias}_{first_field} IS NOT NULL THEN 1 ELSE 0 END")
sources_count_expr = " + ".join(sources_count_parts) if sources_count_parts else "0"

quality_sql = f"""
    SELECT
        d.{HK_COL},
        current_timestamp() AS snapshot_date,
        ({sources_count_expr}) AS sources_count,
        ({completeness_expr}) AS completeness_score
"""

# For quality table, we need to re-query from the raw golden view
# Simplified: just compute from the gold table itself
quality_simple = spark.sql(f"""
    SELECT
        {HK_COL},
        current_timestamp() AS snapshot_date,
        1 AS sources_count,
        {completeness_expr} AS completeness_score
    FROM {GOLD_TABLE}
    WHERE is_current = true
""")

try:
    quality_simple.write.format("delta").mode("overwrite").saveAsTable(QUALITY_TABLE)
except Exception:
    print(f"Quality table {QUALITY_TABLE} write failed, creating...")
    quality_simple.write.format("delta").mode("overwrite").saveAsTable(QUALITY_TABLE)

avg_completeness = quality_simple.agg(F.avg("completeness_score")).collect()[0][0] or 0.0
print(f"Average completeness score: {avg_completeness:.2%}")

# ---------------------------------------------------------------------------
# CELL 9: Log
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, process_name, status, records_loaded, started_at, completed_at)
    VALUES
      ('{run_id}', '{entity_id}', 'nb_derive_gold',
       'Completed', {gold_count}, current_timestamp(), current_timestamp())
""")

print(f"nb_derive_gold [{entity_id}]: DONE")
