# Databricks/Fabric Notebook
# Name: nb_match_location
# Description: Wykrywa kandydatów do merge wśród rekordów hub_location
#              Używa blocking (country+city) + Jaro-Winkler scoring
#              Output: silver_dv.bv_location_match_candidates
# Triggerowany przez: PL_MDM_Master_Location (po nb_load_raw_vault_location)

# ---------------------------------------------------------------------------
# CELL 1: Parametry
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
match_threshold = float(dbutils.widgets.get("match_threshold") if "dbutils" in dir() else "0.85")
auto_accept_threshold = float(dbutils.widgets.get("auto_accept") if "dbutils" in dir() else "0.97")

print(f"run_id={run_id}, threshold={match_threshold}, auto_accept={auto_accept_threshold}")

# ---------------------------------------------------------------------------
# CELL 2: Imports
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType
import uuid

# Jaro-Winkler przez jellyfish (pip install jellyfish w Fabric)
try:
    import jellyfish
    HAS_JELLYFISH = True
except ImportError:
    HAS_JELLYFISH = False
    print("WARNING: jellyfish not available, falling back to Levenshtein ratio")

@F.udf(DoubleType())
def jaro_winkler_udf(s1, s2):
    """Jaro-Winkler similarity [0.0, 1.0]."""
    if not s1 or not s2:
        return 0.0
    if s1 == s2:
        return 1.0
    if HAS_JELLYFISH:
        return float(jellyfish.jaro_winkler_similarity(s1, s2))
    # Fallback: simple ratio
    shorter = min(len(s1), len(s2))
    longer  = max(len(s1), len(s2))
    return shorter / longer if longer > 0 else 0.0

@F.udf(DoubleType())
def geo_score_udf(lat1, lon1, lat2, lon2):
    """Zwraca score 1.0 jeśli < 0.5km, 0.5 jeśli < 2km, 0.0 jeśli brak danych."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None
    from math import radians, sin, cos, sqrt, atan2
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    dist_km = 2 * R * atan2(sqrt(a), sqrt(1-a))
    if dist_km < 0.5:
        return 1.0
    elif dist_km < 2.0:
        return 0.5
    else:
        return 0.0

# ---------------------------------------------------------------------------
# CELL 3: Zbuduj widok kandydatów (hub + najnowsze atrybuty ze wszystkich Satellites)
# ---------------------------------------------------------------------------

# Pobierz najnowsze atrybuty per hub_location ze wszystkich Satellites
# Używamy UNION ALL + window function bo DV-lite = nie wszystkie hubi mają wszystkie saty
candidates = spark.sql("""
    SELECT
        h.location_hk,
        h.record_source,
        COALESCE(ls.name_std, ys.name_std, ms.name_std, gs.name_std) AS name_std,
        COALESCE(ls.country_std, ys.country_std, ms.country_std, gs.country_std) AS country_std,
        COALESCE(ls.city_std, ys.city_std, ms.city_std, gs.city_std) AS city_std,
        COALESCE(ls.name, ys.name, ms.restaurant_name, gs.location_name) AS name_raw,
        ys.postal_code AS zip_code,
        ys.latitude,
        ys.longitude
    FROM silver_dv.hub_location h
    LEFT JOIN silver_dv.sat_location_lightspeed ls
        ON h.location_hk = ls.location_hk AND ls.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_yext ys
        ON h.location_hk = ys.location_hk AND ys.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_mcwin ms
        ON h.location_hk = ms.location_hk AND ms.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_gopos gs
        ON h.location_hk = gs.location_hk AND gs.load_end_date IS NULL
    -- Pomiń hubi, które już mają resolved canonical key (po stronie source)
    WHERE h.location_hk NOT IN (
        SELECT source_hk FROM silver_dv.bv_location_key_resolution
    )
""")

candidates.cache()
print(f"Candidates to match: {candidates.count()}")

# ---------------------------------------------------------------------------
# CELL 4: Blocking — Self-join po (country_std, city_std)
# Bez blocking: O(n²) = 200² = 40k par → OK dla 200 rekordów
# Ale wzorzec blocking jest wymagany dla przyszłych encji (Items = tysiące)
# ---------------------------------------------------------------------------

# Cross-join tylko w ramach bloku (country + city)
# Aliasy: left (wyższy priorytet), right (niższy priorytet)
SOURCE_PRIORITY = {"lightspeed": 1, "mcwin": 2, "yext": 3, "gopos": 4}

try:
    _prio_df = spark.sql("""
        SELECT source_system, priority
        FROM mdm_config.source_priority
        WHERE entity_id = 'business_location' AND field_name = '__default__'
        ORDER BY priority
    """).collect()
    if _prio_df:
        SOURCE_PRIORITY = {row["source_system"]: row["priority"] for row in _prio_df}
        print(f"SOURCE_PRIORITY from config: {SOURCE_PRIORITY}")
    else:
        print("WARNING: source_priority config empty, using hardcoded defaults")
except Exception as _e:
    print(f"WARNING: could not read source_priority from config ({_e}), using hardcoded defaults")

# UDF do porządkowania par (left = wyższy priorytet źródła)
@F.udf(StringType())
def source_priority_udf(src):
    return str(SOURCE_PRIORITY.get(src, 99))

candidates_with_prio = candidates.withColumn(
    "src_priority", source_priority_udf(F.col("record_source"))
)

# Self-join z blocking key (country + city)
pairs = (
    candidates_with_prio.alias("left")
    .join(
        candidates_with_prio.alias("right"),
        (F.col("left.country_std") == F.col("right.country_std")) &
        (F.col("left.city_std") == F.col("right.city_std")) &
        # Unikaj duplikatów par (A,B) vs (B,A) — left ma wyższy priorytet lub lex-mniejszy
        (F.col("left.src_priority") < F.col("right.src_priority"))
    )
    .filter(F.col("left.location_hk") != F.col("right.location_hk"))
)

print(f"Candidate pairs after blocking: {pairs.count()}")

# ---------------------------------------------------------------------------
# CELL 5: Scoring
# score = 0.50 * jaro_winkler(name) + 0.30 * zip_exact + 0.20 * geo_score
# ---------------------------------------------------------------------------

scored_pairs = (
    pairs
    .withColumn("name_score",
        jaro_winkler_udf(F.col("left.name_std"), F.col("right.name_std")))
    .withColumn("zip_match",
        (F.col("left.zip_code").isNotNull()) &
        (F.col("left.zip_code") == F.col("right.zip_code")))
    .withColumn("geo_score",
        geo_score_udf(
            F.col("left.latitude"), F.col("left.longitude"),
            F.col("right.latitude"), F.col("right.longitude")))
    .withColumn("match_score",
        F.col("name_score") * 0.50
        + F.when(F.col("zip_match"), F.lit(1.0)).otherwise(F.lit(0.0)) * 0.30
        + F.coalesce(F.col("geo_score"), F.lit(0.0)) * 0.20
    )
    .withColumn("match_type",
        F.when(F.col("name_score") == 1.0, F.lit("exact_name"))
         .when(F.col("match_score") >= 0.97, F.lit("composite_high"))
         .when(F.col("geo_score") == 1.0, F.lit("geo_proximity"))
         .otherwise(F.lit("fuzzy_name_city")))
    .filter(F.col("match_score") >= match_threshold)
)

print(f"Pairs above threshold ({match_threshold}): {scored_pairs.count()}")

# ---------------------------------------------------------------------------
# CELL 6: Wstaw do bv_location_match_candidates
#         (tylko nowe pary, które jeszcze nie istnieją)
# ---------------------------------------------------------------------------

existing_pairs = spark.sql("""
    SELECT hk_left, hk_right FROM silver_dv.bv_location_match_candidates
""")

new_candidates = (
    scored_pairs
    .withColumn("pair_id", F.expr("uuid()"))
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
        F.col("left.location_hk").alias("hk_left"),
        F.col("right.location_hk").alias("hk_right"),
        F.col("match_score"),
        F.col("match_type"),
        F.col("name_score"),
        F.col("zip_match"),
        F.col("geo_score"),
        F.col("status"),
        F.current_timestamp().alias("created_at"),
        F.col("reviewed_by"),
        F.col("reviewed_at"),
        F.lit(None).cast(StringType()).alias("review_note")
    )
    .join(
        existing_pairs.alias("ep"),
        (F.col("hk_left") == F.col("ep.hk_left")) & (F.col("hk_right") == F.col("ep.hk_right")),
        "left_anti"
    )
)

inserted_count = new_candidates.count()
new_candidates.write.format("delta").mode("append").saveAsTable(
    "silver_dv.bv_location_match_candidates"
)

print(f"New match candidates inserted: {inserted_count}")

# ---------------------------------------------------------------------------
# CELL 7: Auto-accepted pairs → wstaw do bv_location_key_resolution
# ---------------------------------------------------------------------------

auto_accepted = spark.sql(f"""
    SELECT pair_id, hk_left, hk_right
    FROM silver_dv.bv_location_match_candidates
    WHERE status = 'auto_accepted'
      AND pair_id NOT IN (SELECT DISTINCT pair_id FROM silver_dv.bv_location_key_resolution WHERE pair_id IS NOT NULL)
""")

if auto_accepted.count() > 0:
    auto_resolution = (
        auto_accepted
        .withColumn("source_hk", F.col("hk_right"))        # niższy priorytet → zastępowany
        .withColumn("canonical_hk", F.col("hk_left"))       # wyższy priorytet → canonical
        .withColumn("resolved_by", F.lit("system_auto"))
        .withColumn("resolved_at", F.current_timestamp())
        .withColumn("resolution_type", F.lit("auto"))
        .select("source_hk", "canonical_hk", "resolved_by", "resolved_at", "pair_id", "resolution_type")
    )
    auto_resolution.write.format("delta").mode("append").saveAsTable(
        "silver_dv.bv_location_key_resolution"
    )
    print(f"Auto-accepted resolutions: {auto_accepted.count()}")

pending_count = spark.sql(
    "SELECT COUNT(*) FROM silver_dv.bv_location_match_candidates WHERE status = 'pending'"
).collect()[0][0]

print(f"Pending pairs for steward review: {pending_count}")

# ---------------------------------------------------------------------------
# CELL 8: Log
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, process_name, status, records_matched, started_at, completed_at)
    VALUES
      ('{run_id}', 'business_location', 'nb_match_location',
       'Completed', {inserted_count}, current_timestamp(), current_timestamp())
""")

# Zwróć pending count do pipeline'u (Fabric Notebook output)
dbutils.notebook.exit(str(pending_count)) if "dbutils" in dir() else print(f"EXIT: {pending_count}")
