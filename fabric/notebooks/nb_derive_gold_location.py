# Databricks/Fabric Notebook
# Name: nb_derive_gold_location
# Description: Derivuje golden record dim_location z Data Vault przez PIT + survivorship
#              Survivorship config z mdm_config.source_priority
# Triggerowany przez: PL_MDM_Master_Location (po nb_match_location + steward review)

# ---------------------------------------------------------------------------
# CELL 1: Parametry
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
snapshot_date_str = dbutils.widgets.get("snapshot_date") if "dbutils" in dir() else None

from datetime import datetime, timezone
snapshot_date = datetime.fromisoformat(snapshot_date_str) if snapshot_date_str else datetime.now(timezone.utc)

print(f"run_id={run_id}, snapshot_date={snapshot_date}")

# ---------------------------------------------------------------------------
# CELL 2: Imports
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.shuffle.partitions", "8")

# ---------------------------------------------------------------------------
# CELL 3: Aktualizuj PIT (Point-In-Time)
# Jeden wiersz per canonical_hk z najnowszym load_date z każdego Satellite
# ---------------------------------------------------------------------------

pit_new = spark.sql(f"""
    SELECT
        h.location_hk,
        TIMESTAMP('{snapshot_date}') AS snapshot_date,
        MAX(ls.load_date) AS sat_lightspeed_ld,
        MAX(ys.load_date) AS sat_yext_ld,
        MAX(ms.load_date) AS sat_mcwin_ld,
        MAX(gs.load_date) AS sat_gopos_ld
    FROM silver_dv.hub_location h
    LEFT JOIN silver_dv.sat_location_lightspeed ls
        ON h.location_hk = ls.location_hk AND ls.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_yext ys
        ON h.location_hk = ys.location_hk AND ys.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_mcwin ms
        ON h.location_hk = ms.location_hk AND ms.load_end_date IS NULL
    LEFT JOIN silver_dv.sat_location_gopos gs
        ON h.location_hk = gs.location_hk AND gs.load_end_date IS NULL
    GROUP BY h.location_hk
""")

# MERGE do pit_location
pit_delta = DeltaTable.forName(spark, "silver_dv.pit_location")
pit_delta.alias("pit").merge(
    pit_new.alias("src"), "pit.location_hk = src.location_hk"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"PIT updated: {pit_new.count()} locations")

# ---------------------------------------------------------------------------
# CELL 4: Załaduj priorytety źródeł z config
# ---------------------------------------------------------------------------
source_prio = spark.sql("""
    SELECT source_system, field_name, priority
    FROM mdm_config.source_priority
    WHERE entity_id = 'business_location'
    ORDER BY priority ASC
""").collect()

# Zbuduj mapę: field_name → [source_system_w_kolejności_priorytetu]
from collections import defaultdict
field_sources = defaultdict(list)
global_sources = []
for row in source_prio:
    if row.field_name == '*':
        global_sources.append(row.source_system)
    else:
        field_sources[row.field_name].append(row.source_system)

# Uzupełnij brakujące pola globalną kolejnością
for fn in ["name", "city", "zip_code", "country", "address", "phone", "timezone", "currency_code"]:
    if fn not in field_sources:
        field_sources[fn] = global_sources

print("Field survivorship config:", dict(field_sources))

# ---------------------------------------------------------------------------
# CELL 5: Derivuj golden record przez JOIN PIT → Satellites
# Survivorship: COALESCE w kolejności priorytetów
# ---------------------------------------------------------------------------

# Helper: zbuduj COALESCE wyrażenie dla danego pola
def best_value(field_name: str, sat_aliases: dict) -> F.Column:
    """Zwraca kolumnę = COALESCE(sat1.field, sat2.field, ...) wg priorytetu."""
    sources = field_sources.get(field_name, global_sources)
    coalesce_args = []
    for src in sources:
        if src in sat_aliases:
            col_name = sat_aliases[src].get(field_name)
            if col_name:
                coalesce_args.append(F.col(col_name))
    return F.coalesce(*coalesce_args) if coalesce_args else F.lit(None)

def best_source(field_name: str, sat_aliases: dict) -> F.Column:
    """Zwraca nazwę źródła, z którego pochodzi wartość (dla lineage)."""
    sources = field_sources.get(field_name, global_sources)
    expr = F.lit(None).cast("string")
    for src in reversed(sources):  # reversed bo WHEN cascade
        if src in sat_aliases:
            col_name = sat_aliases[src].get(field_name)
            if col_name:
                expr = F.when(F.col(col_name).isNotNull(), F.lit(src)).otherwise(expr)
    return expr

golden = spark.sql("""
    SELECT
        pit.location_hk,
        pit.snapshot_date,
        h.business_key AS h_business_key,
        h.record_source AS record_source,
        -- Lightspeed attributes
        ls.name        AS ls_name,
        ls.country     AS ls_country,
        ls.city_std    AS ls_city,
        ls.timezone    AS ls_timezone,
        ls.currency_code AS ls_currency_code,
        ls.bl_id       AS ls_bl_id,
        -- Yext attributes
        ys.name        AS ys_name,
        ys.city        AS ys_city,
        ys.postal_code AS ys_zip,
        ys.country_code AS ys_country,
        ys.phone       AS ys_phone,
        ys.website_url AS ys_website,
        ys.latitude    AS ys_lat,
        ys.longitude   AS ys_lon,
        ys.avg_rating  AS ys_rating,
        ys.review_count AS ys_reviews,
        -- McWin attributes
        ms.restaurant_name AS ms_name,
        ms.city            AS ms_city,
        ms.zip_code        AS ms_zip,
        ms.country         AS ms_country,
        ms.cost_center     AS ms_cost_center,
        ms.region          AS ms_region,
        -- GoPOS attributes
        gs.location_name   AS gs_name,
        gs.city            AS gs_city,
        gs.zip_code        AS gs_zip,
        gs.country         AS gs_country,
        gs.phone           AS gs_phone
    FROM silver_dv.pit_location pit
    JOIN silver_dv.hub_location h ON pit.location_hk = h.location_hk
    LEFT JOIN silver_dv.sat_location_lightspeed ls
        ON pit.location_hk = ls.location_hk AND ls.load_date = pit.sat_lightspeed_ld
    LEFT JOIN silver_dv.sat_location_yext ys
        ON pit.location_hk = ys.location_hk AND ys.load_date = pit.sat_yext_ld
    LEFT JOIN silver_dv.sat_location_mcwin ms
        ON pit.location_hk = ms.location_hk AND ms.load_date = pit.sat_mcwin_ld
    LEFT JOIN silver_dv.sat_location_gopos gs
        ON pit.location_hk = gs.location_hk AND gs.load_date = pit.sat_gopos_ld
""")

# Survivorship (config-driven via best_value / best_source helpers)
# Mapuje aliasy kolumn satellite na nazwy pól — używane przez best_value() i best_source()
sat_aliases = {
    "lightspeed": {
        "name":          "ls_name",
        "city":          "ls_city",
        "country":       "ls_country",
        "timezone":      "ls_timezone",
        "currency_code": "ls_currency_code",
    },
    "yext": {
        "name":        "ys_name",
        "city":        "ys_city",
        "zip_code":    "ys_zip",
        "country":     "ys_country",
        "phone":       "ys_phone",
        "website_url": "ys_website",
        "latitude":    "ys_lat",
        "longitude":   "ys_lon",
    },
    "mcwin": {
        "name":        "ms_name",
        "city":        "ms_city",
        "zip_code":    "ms_zip",
        "country":     "ms_country",
        "cost_center": "ms_cost_center",
        "region":      "ms_region",
    },
    "gopos": {
        "name":     "gs_name",
        "city":     "gs_city",
        "zip_code": "gs_zip",
        "country":  "gs_country",
        "phone":    "gs_phone",
    },
}

golden_final = (
    golden
    .withColumn("name",          best_value("name",          sat_aliases))
    .withColumn("city",          best_value("city",          sat_aliases))
    .withColumn("zip_code",      best_value("zip_code",      sat_aliases))
    .withColumn("country",       best_value("country",       sat_aliases))
    .withColumn("phone",         best_value("phone",         sat_aliases))
    .withColumn("website_url",   best_value("website_url",   sat_aliases))
    .withColumn("latitude",      best_value("latitude",      sat_aliases))
    .withColumn("longitude",     best_value("longitude",     sat_aliases))
    .withColumn("timezone",      best_value("timezone",      sat_aliases))
    .withColumn("currency_code", best_value("currency_code", sat_aliases))
    .withColumn("avg_rating",    F.col("ys_rating"))
    .withColumn("review_count",  F.col("ys_reviews"))
    .withColumn("cost_center",   best_value("cost_center",   sat_aliases))
    .withColumn("region",        best_value("region",        sat_aliases))
    # Lineage: skąd pochodzi pole (config-driven)
    .withColumn("name_source",    best_source("name",    sat_aliases))
    .withColumn("country_source", best_source("country", sat_aliases))
    .withColumn("city_source",    best_source("city",    sat_aliases))
    # Source crosswalk IDs — extracted from hub.business_key (format: "source|id")
    .withColumn("lightspeed_bl_id",
        F.when(F.col("record_source") == "lightspeed",
               F.split(F.col("h_business_key"), "\\|").getItem(1).cast("long"))
         .otherwise(F.lit(None).cast("long")))
    .withColumn("yext_id",
        F.when(F.col("record_source") == "yext",
               F.split(F.col("h_business_key"), "\\|").getItem(1))
         .otherwise(F.lit(None).cast("string")))
    .withColumn("mcwin_restaurant_id",
        F.when(F.col("record_source") == "mcwin",
               F.split(F.col("h_business_key"), "\\|").getItem(1))
         .otherwise(F.lit(None).cast("string")))
    .withColumn("gopos_location_id",
        F.when(F.col("record_source") == "gopos",
               F.split(F.col("h_business_key"), "\\|").getItem(1))
         .otherwise(F.lit(None).cast("string")))
    .withColumn("valid_from", F.col("snapshot_date"))
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "location_hk", "valid_from", "valid_to", "is_current",
        "name", "country", "city", "zip_code", "phone", "website_url",
        "latitude", "longitude", "timezone", "currency_code", "avg_rating",
        "review_count", "cost_center", "region",
        "name_source", "country_source", "city_source",
        "created_at", "updated_at",
        "lightspeed_bl_id", "yext_id", "mcwin_restaurant_id", "gopos_location_id"
    )
)

# ---------------------------------------------------------------------------
# CELL 6: SCD2 MERGE do gold.dim_location
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CELL 6: SCD2 MERGE do gold.dim_location — atomowy (jeden MERGE zamiast dwóch)
# Zamknięcie starych + wstawienie nowych w jednej operacji Delta MERGE,
# co eliminuje ryzyko duplikatów przy double-run w tym samym batchu.
# ---------------------------------------------------------------------------

gold_delta = DeltaTable.forName(spark, "gold.dim_location")

gold_delta.alias("gold").merge(
    golden_final.alias("new"),
    "gold.location_hk = new.location_hk AND gold.is_current = true"
).whenMatchedUpdate(
    condition="""
        gold.name != new.name OR gold.city != new.city OR gold.zip_code != new.zip_code
        OR gold.country != new.country OR gold.phone != new.phone
    """,
    set={
        "valid_to":   "new.valid_from",
        "is_current": "false",
        "updated_at": "new.updated_at"
    }
).whenNotMatchedInsertAll(
).execute()

# Wstaw nowe wersje dla rekordów, których stara wersja właśnie została zamknięta
# (po MERGE powyżej ich is_current = false, więc left_anti je znajdzie)
new_inserts = (
    golden_final.alias("new")
    .join(
        spark.sql("SELECT location_hk FROM gold.dim_location WHERE is_current = true").alias("existing"),
        "location_hk",
        "left_anti"
    )
)

new_inserts.write.format("delta").mode("append").saveAsTable("gold.dim_location")

gold_count = spark.sql("SELECT COUNT(*) FROM gold.dim_location WHERE is_current = true").collect()[0][0]
print(f"Gold dim_location: {gold_count} current golden records")

# ---------------------------------------------------------------------------
# CELL 7: Quality metrics
# ---------------------------------------------------------------------------

quality = spark.sql("""
    SELECT
        d.location_hk,
        current_timestamp() AS snapshot_date,
        (CASE WHEN d.lightspeed_bl_id IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN d.yext_id IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN d.mcwin_restaurant_id IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN d.gopos_location_id IS NOT NULL THEN 1 ELSE 0 END) AS sources_count,
        (CASE WHEN d.name IS NOT NULL THEN 1.0 ELSE 0.0 END
         + CASE WHEN d.city IS NOT NULL THEN 1.0 ELSE 0.0 END
         + CASE WHEN d.zip_code IS NOT NULL THEN 1.0 ELSE 0.0 END
         + CASE WHEN d.country IS NOT NULL THEN 1.0 ELSE 0.0 END
         + CASE WHEN d.latitude IS NOT NULL THEN 1.0 ELSE 0.0 END) / 5.0 AS completeness_score,
        d.lightspeed_bl_id IS NOT NULL AS has_lightspeed,
        d.yext_id IS NOT NULL AS has_yext,
        d.mcwin_restaurant_id IS NOT NULL AS has_mcwin,
        d.gopos_location_id IS NOT NULL AS has_gopos,
        m.match_score AS last_match_score
    FROM gold.dim_location d
    LEFT JOIN (
        SELECT hk_left AS location_hk, MAX(match_score) AS match_score
        FROM silver_dv.bv_location_match_candidates
        WHERE status IN ('accepted', 'auto_accepted')
        GROUP BY hk_left
    ) m ON d.location_hk = m.location_hk
    WHERE d.is_current = true
""")

quality.write.format("delta").mode("overwrite").saveAsTable("gold.dim_location_quality")

avg_completeness = quality.agg(F.avg("completeness_score")).collect()[0][0]
print(f"Average completeness score: {avg_completeness:.2%}")

# ---------------------------------------------------------------------------
# CELL 8: Log
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, process_name, status, records_loaded, started_at, completed_at)
    VALUES
      ('{run_id}', 'business_location', 'nb_derive_gold_location',
       'Completed', {gold_count}, current_timestamp(), current_timestamp())
""")

print("nb_derive_gold_location: DONE")
