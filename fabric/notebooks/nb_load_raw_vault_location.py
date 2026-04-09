# Databricks/Fabric Notebook
# Name: nb_load_raw_vault_location
# Description: Ładuje dane z Bronze do Silver Data Vault (Hub + Satellites)
#              Obsługuje bv_location_key_resolution dla MDM key merging
# Triggerowany przez: PL_MDM_Master_Location

# ---------------------------------------------------------------------------
# CELL 1: Parametry (Fabric Notebook parameters cell)
# ---------------------------------------------------------------------------
# dbutils.widgets.text("run_id", "", "Run ID")
# dbutils.widgets.text("tenant_name", "losteria", "Tenant Name")
# dbutils.widgets.text("full_load", "false", "Full Load")

run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
tenant_name = dbutils.widgets.get("tenant_name") if "dbutils" in dir() else "losteria"
full_load = (dbutils.widgets.get("full_load") if "dbutils" in dir() else "false").lower() == "true"

print(f"run_id={run_id}, tenant_name={tenant_name}, full_load={full_load}")

# ---------------------------------------------------------------------------
# CELL 2: Imports & Spark setup
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, TimestampType, BooleanType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
import hashlib

spark.conf.set("spark.sql.shuffle.partitions", "8")  # small dataset ~200 records

LAKEHOUSE = "lh_mdm"
LOAD_DATE = datetime.now(timezone.utc)

# ---------------------------------------------------------------------------
# CELL 3: Helper functions
# ---------------------------------------------------------------------------

def compute_hk(business_key: str) -> bytes:
    """Oblicza SHA256 hash key z business_key (source_system|source_id)."""
    return hashlib.sha256(business_key.encode("utf-8")).digest()

def compute_hash_diff(*values) -> bytes:
    """Oblicza SHA256 hash_diff z listy atrybutów (wykrywanie zmian w Satellite)."""
    concat = "|".join([str(v) if v is not None else "" for v in values])
    return hashlib.sha256(concat.encode("utf-8")).digest()

# Spark UDFs
@F.udf(BinaryType())
def udf_hk(business_key):
    if business_key is None:
        return None
    return hashlib.sha256(business_key.encode("utf-8")).digest()

@F.udf(BinaryType())
def udf_hash_diff(*values):
    concat = "|".join([str(v) if v is not None else "" for v in values])
    return hashlib.sha256(concat.encode("utf-8")).digest()

@F.udf(StringType())
def udf_normalize_country(country_raw):
    """Mapuje nazwy krajów na ISO 2-letter codes."""
    if not country_raw:
        return None
    mapping = {
        "DE": "DE", "GERMANY": "DE", "DEUTSCHLAND": "DE",
        "FR": "FR", "FRANCE": "FR", "FRANKREICH": "FR",
        "CH": "CH", "SWITZERLAND": "CH", "SCHWEIZ": "CH",
        "LU": "LU", "LUXEMBOURG": "LU",
        "CZ": "CZ", "CZECH": "CZ", "TSCHECHIEN": "CZ",
        "NL": "NL", "NETHERLANDS": "NL", "NIEDERLANDE": "NL",
        "GB": "GB", "UK": "GB", "UNITED KINGDOM": "GB",
        "AT": "AT", "AUSTRIA": "AT", "ÖSTERREICH": "AT",
    }
    return mapping.get(country_raw.strip().upper(), country_raw.strip().upper())

# ---------------------------------------------------------------------------
# CELL 4: Załaduj Key Resolution Table
# (decyzje stewarda: source_hk → canonical_hk)
# ---------------------------------------------------------------------------
resolution_df = spark.sql("""
    SELECT source_hk, canonical_hk
    FROM silver_dv.bv_location_key_resolution
""")

resolution_broadcast = F.broadcast(resolution_df)

def apply_key_resolution(df, hk_col="location_hk"):
    """Zastępuje source_hk canonical_hk jeśli steward zdecydował o merge."""
    return (
        df
        .join(resolution_broadcast.alias("res"),
              df[hk_col] == F.col("res.source_hk"), "left")
        .withColumn(hk_col,
                    F.when(F.col("res.canonical_hk").isNotNull(), F.col("res.canonical_hk"))
                     .otherwise(F.col(hk_col)))
        .drop("source_hk", "canonical_hk")
    )

# ---------------------------------------------------------------------------
# CELL 5: Pobierz watermark (incremental load)
# ---------------------------------------------------------------------------
if not full_load:
    watermarks = spark.sql("""
        SELECT source_system, last_load_date
        FROM mdm_config.source_watermark
        WHERE entity_id = 'business_location'
    """).collect()
    wm = {row.source_system: row.last_load_date for row in watermarks}
else:
    wm = {s: datetime(1900, 1, 1, tzinfo=timezone.utc)
          for s in ["lightspeed", "yext", "mcwin", "gopos"]}

print("Watermarks:", wm)

# ---------------------------------------------------------------------------
# CELL 6: Lightspeed → Hub + Satellite
# ---------------------------------------------------------------------------

ls_raw = spark.sql(f"""
    SELECT *
    FROM bronze.lightspeed_businesses
    WHERE _load_date > '{wm.get("lightspeed", "1900-01-01")}'
      AND _tenant_name = '{tenant_name}'
""")

if ls_raw.count() > 0:
    ls_prep = (
        ls_raw
        .withColumn("business_key", F.concat(F.lit("lightspeed|"), F.col("blId").cast("string")))
        .withColumn("location_hk", udf_hk(F.col("business_key")))
        .withColumn("name_std", F.upper(F.trim(F.col("blName"))))
        .withColumn("country_std", udf_normalize_country(F.col("country")))
        .withColumn("city_std", F.lit(None))  # Lightspeed Businesses nie ma city na tym poziomie
    )

    ls_prep = apply_key_resolution(ls_prep)

    # MERGE do hub_location
    hub_delta = DeltaTable.forName(spark, "silver_dv.hub_location")
    hub_delta.alias("hub").merge(
        ls_prep.alias("src"),
        "hub.location_hk = src.location_hk"
    ).whenNotMatchedInsert(values={
        "location_hk":   "src.location_hk",
        "business_key":  "src.business_key",
        "load_date":     F.lit(LOAD_DATE),
        "record_source": F.lit("lightspeed")
    }).execute()

    # Satellite: zamknij stare rekordy + wstaw nowe (jeśli hash_diff się zmienił)
    ls_sat = (
        ls_prep
        .withColumn("hash_diff", udf_hash_diff(
            F.col("blName"), F.col("country"), F.col("timezone"), F.col("currencyCode")))
        .withColumn("load_date", F.lit(LOAD_DATE))
        .withColumn("load_end_date", F.lit(None).cast(TimestampType()))
        .withColumn("record_source", F.lit("lightspeed"))
        .select(
            "location_hk", "load_date", "load_end_date", "hash_diff", "record_source",
            F.col("blName").alias("name"),
            F.col("country"),
            F.lit(None).cast(StringType()).alias("city"),
            F.col("timezone"),
            F.col("currencyCode").alias("currency_code"),
            F.col("blId").alias("bl_id"),
            F.lit(True).alias("is_active"),
            "name_std", "country_std",
            F.lit(None).cast(StringType()).alias("city_std")
        )
    )

    # Zamknij stare wiersze (load_end_date = teraz) gdzie hash_diff się zmienił
    sat_delta = DeltaTable.forName(spark, "silver_dv.sat_location_lightspeed")
    sat_delta.alias("sat").merge(
        ls_sat.alias("src"),
        "sat.location_hk = src.location_hk AND sat.load_end_date IS NULL AND sat.hash_diff != src.hash_diff"
    ).whenMatchedUpdate(set={"load_end_date": F.lit(LOAD_DATE)}).execute()

    # Wstaw nowe wiersze (tylko jeśli nie istnieje aktualny z tym samym hash_diff)
    new_rows = ls_sat.alias("src").join(
        spark.sql("SELECT location_hk, hash_diff FROM silver_dv.sat_location_lightspeed WHERE load_end_date IS NULL").alias("existing"),
        (F.col("src.location_hk") == F.col("existing.location_hk")) &
        (F.col("src.hash_diff") == F.col("existing.hash_diff")),
        "left_anti"
    )
    new_rows.write.format("delta").mode("append").saveAsTable("silver_dv.sat_location_lightspeed")

    print(f"Lightspeed: {ls_raw.count()} source rows processed")

    # Zaktualizuj watermark
    spark.sql(f"""
        MERGE INTO mdm_config.source_watermark AS w
        USING (SELECT 'business_location' as entity_id, 'lightspeed' as source_system,
                      current_timestamp() as ts, '{run_id}' as run_id) AS s
        ON w.entity_id = s.entity_id AND w.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET last_load_date = s.ts, last_run_id = s.run_id, updated_at = s.ts
        WHEN NOT MATCHED THEN INSERT *
    """)

# ---------------------------------------------------------------------------
# CELL 7: Yext → Hub + Satellite
# ---------------------------------------------------------------------------

yext_raw = spark.sql(f"""
    SELECT * FROM bronze.yext_locations
    WHERE _load_date > '{wm.get("yext", "1900-01-01")}'
      AND _tenant_name = '{tenant_name}'
""")

if yext_raw.count() > 0:
    yext_prep = (
        yext_raw
        .withColumn("business_key", F.concat(F.lit("yext|"), F.col("id")))
        .withColumn("location_hk", udf_hk(F.col("business_key")))
        .withColumn("name_std", F.upper(F.trim(F.col("name"))))
        .withColumn("country_std", udf_normalize_country(F.col("address_country_code")))
        .withColumn("city_std", F.upper(F.trim(F.col("address_city"))))
    )

    yext_prep = apply_key_resolution(yext_prep)

    # MERGE Hub
    hub_delta.alias("hub").merge(
        yext_prep.alias("src"), "hub.location_hk = src.location_hk"
    ).whenNotMatchedInsert(values={
        "location_hk": "src.location_hk", "business_key": "src.business_key",
        "load_date": F.lit(LOAD_DATE), "record_source": F.lit("yext")
    }).execute()

    # Satellite Yext
    yext_sat = (
        yext_prep
        .withColumn("hash_diff", udf_hash_diff(
            F.col("name"), F.col("address_line1"), F.col("address_city"),
            F.col("address_postal_code"), F.col("address_country_code"),
            F.col("phone"), F.col("avg_rating"), F.col("review_count")))
        .withColumn("load_date", F.lit(LOAD_DATE))
        .withColumn("load_end_date", F.lit(None).cast(TimestampType()))
        .withColumn("record_source", F.lit("yext"))
        .select(
            "location_hk", "load_date", "load_end_date", "hash_diff", "record_source",
            F.col("name"), F.col("address_line1"),
            F.col("address_city").alias("city"),
            F.col("address_postal_code").alias("postal_code"),
            F.col("address_country_code").alias("country_code"),
            F.col("phone"), F.col("website_url"),
            F.col("display_lat").alias("latitude"),
            F.col("display_lng").alias("longitude"),
            F.col("avg_rating"), F.col("review_count"),
            "name_std", "country_std", "city_std"
        )
    )

    sat_yext = DeltaTable.forName(spark, "silver_dv.sat_location_yext")
    sat_yext.alias("sat").merge(
        yext_sat.alias("src"),
        "sat.location_hk = src.location_hk AND sat.load_end_date IS NULL AND sat.hash_diff != src.hash_diff"
    ).whenMatchedUpdate(set={"load_end_date": F.lit(LOAD_DATE)}).execute()

    yext_sat.alias("src").join(
        spark.sql("SELECT location_hk, hash_diff FROM silver_dv.sat_location_yext WHERE load_end_date IS NULL").alias("e"),
        (F.col("src.location_hk") == F.col("e.location_hk")) & (F.col("src.hash_diff") == F.col("e.hash_diff")),
        "left_anti"
    ).write.format("delta").mode("append").saveAsTable("silver_dv.sat_location_yext")

    print(f"Yext: {yext_raw.count()} source rows processed")

    spark.sql(f"""
        MERGE INTO mdm_config.source_watermark AS w
        USING (SELECT 'business_location' as entity_id, 'yext' as source_system,
                      current_timestamp() as ts, '{run_id}' as run_id) AS s
        ON w.entity_id = s.entity_id AND w.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET last_load_date = s.ts, last_run_id = s.run_id, updated_at = s.ts
    """)

# ---------------------------------------------------------------------------
# CELL 8: McWin → Hub + Satellite
# ---------------------------------------------------------------------------

mcwin_raw = spark.sql(f"""
    SELECT * FROM bronze.mcwin_restaurant_masterdata
    WHERE _load_date > '{wm.get("mcwin", "1900-01-01")}'
      AND _tenant_name = '{tenant_name}'
""")

if mcwin_raw.count() > 0:
    mcwin_prep = (
        mcwin_raw
        .withColumn("business_key", F.concat(F.lit("mcwin|"), F.col("restaurant_id")))
        .withColumn("location_hk", udf_hk(F.col("business_key")))
        .withColumn("name_std", F.upper(F.trim(F.col("restaurant_name"))))
        .withColumn("country_std", udf_normalize_country(F.col("country")))
        .withColumn("city_std", F.upper(F.trim(F.col("city"))))
    )
    mcwin_prep = apply_key_resolution(mcwin_prep)

    hub_delta.alias("hub").merge(
        mcwin_prep.alias("src"), "hub.location_hk = src.location_hk"
    ).whenNotMatchedInsert(values={
        "location_hk": "src.location_hk", "business_key": "src.business_key",
        "load_date": F.lit(LOAD_DATE), "record_source": F.lit("mcwin")
    }).execute()

    mcwin_sat = (
        mcwin_prep
        .withColumn("hash_diff", udf_hash_diff(
            F.col("restaurant_name"), F.col("city"), F.col("zip_code"),
            F.col("country"), F.col("cost_center"), F.col("region"), F.col("is_active")))
        .withColumn("load_date", F.lit(LOAD_DATE))
        .withColumn("load_end_date", F.lit(None).cast(TimestampType()))
        .withColumn("record_source", F.lit("mcwin"))
        .select(
            "location_hk", "load_date", "load_end_date", "hash_diff", "record_source",
            F.col("restaurant_name"), F.col("cost_center"), F.col("region"),
            F.col("country"), F.col("city"), F.col("zip_code"),
            F.col("address"), F.col("is_active"),
            "name_std", "country_std", "city_std"
        )
    )

    sat_mcwin = DeltaTable.forName(spark, "silver_dv.sat_location_mcwin")
    sat_mcwin.alias("sat").merge(
        mcwin_sat.alias("src"),
        "sat.location_hk = src.location_hk AND sat.load_end_date IS NULL AND sat.hash_diff != src.hash_diff"
    ).whenMatchedUpdate(set={"load_end_date": F.lit(LOAD_DATE)}).execute()

    mcwin_sat.alias("src").join(
        spark.sql("SELECT location_hk, hash_diff FROM silver_dv.sat_location_mcwin WHERE load_end_date IS NULL").alias("e"),
        (F.col("src.location_hk") == F.col("e.location_hk")) & (F.col("src.hash_diff") == F.col("e.hash_diff")),
        "left_anti"
    ).write.format("delta").mode("append").saveAsTable("silver_dv.sat_location_mcwin")

    print(f"McWin: {mcwin_raw.count()} source rows processed")

    spark.sql(f"""
        MERGE INTO mdm_config.source_watermark AS w
        USING (SELECT 'business_location' as entity_id, 'mcwin' as source_system,
                      current_timestamp() as ts, '{run_id}' as run_id) AS s
        ON w.entity_id = s.entity_id AND w.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET last_load_date = s.ts, last_run_id = s.run_id, updated_at = s.ts
    """)

# ---------------------------------------------------------------------------
# CELL 9: GoPOS → Hub + Satellite
# ---------------------------------------------------------------------------

gopos_raw = spark.sql(f"""
    SELECT * FROM bronze.gopos_locations
    WHERE _load_date > '{wm.get("gopos", "1900-01-01")}'
      AND _tenant_name = '{tenant_name}'
""")

if gopos_raw.count() > 0:
    gopos_prep = (
        gopos_raw
        .withColumn("business_key", F.concat(F.lit("gopos|"), F.col("location_id")))
        .withColumn("location_hk", udf_hk(F.col("business_key")))
        .withColumn("name_std", F.upper(F.trim(F.col("location_name"))))
        .withColumn("country_std", udf_normalize_country(F.col("country")))
        .withColumn("city_std", F.upper(F.trim(F.col("city"))))
    )
    gopos_prep = apply_key_resolution(gopos_prep)

    hub_delta.alias("hub").merge(
        gopos_prep.alias("src"), "hub.location_hk = src.location_hk"
    ).whenNotMatchedInsert(values={
        "location_hk": "src.location_hk", "business_key": "src.business_key",
        "load_date": F.lit(LOAD_DATE), "record_source": F.lit("gopos")
    }).execute()

    gopos_sat = (
        gopos_prep
        .withColumn("hash_diff", udf_hash_diff(
            F.col("location_name"), F.col("city"), F.col("zip_code"),
            F.col("country"), F.col("phone"), F.col("is_active").cast("string")))
        .withColumn("load_date", F.lit(LOAD_DATE))
        .withColumn("load_end_date", F.lit(None).cast(TimestampType()))
        .withColumn("record_source", F.lit("gopos"))
        .select(
            "location_hk", "load_date", "load_end_date", "hash_diff", "record_source",
            F.col("location_name"), F.col("address"), F.col("city"),
            F.col("zip_code"), F.col("country"), F.col("phone"), F.col("is_active"),
            "name_std", "country_std", "city_std"
        )
    )

    sat_gopos = DeltaTable.forName(spark, "silver_dv.sat_location_gopos")
    sat_gopos.alias("sat").merge(
        gopos_sat.alias("src"),
        "sat.location_hk = src.location_hk AND sat.load_end_date IS NULL AND sat.hash_diff != src.hash_diff"
    ).whenMatchedUpdate(set={"load_end_date": F.lit(LOAD_DATE)}).execute()

    gopos_sat.alias("src").join(
        spark.sql("SELECT location_hk, hash_diff FROM silver_dv.sat_location_gopos WHERE load_end_date IS NULL").alias("e"),
        (F.col("src.location_hk") == F.col("e.location_hk")) & (F.col("src.hash_diff") == F.col("e.hash_diff")),
        "left_anti"
    ).write.format("delta").mode("append").saveAsTable("silver_dv.sat_location_gopos")

    print(f"GoPOS: {gopos_raw.count()} source rows processed")

    spark.sql(f"""
        MERGE INTO mdm_config.source_watermark AS w
        USING (SELECT 'business_location' as entity_id, 'gopos' as source_system,
                      current_timestamp() as ts, '{run_id}' as run_id) AS s
        ON w.entity_id = s.entity_id AND w.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET last_load_date = s.ts, last_run_id = s.run_id, updated_at = s.ts
    """)

# ---------------------------------------------------------------------------
# CELL 10: Log wykonania
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, source_system, process_name, status, started_at, completed_at)
    VALUES
      ('{run_id}', 'business_location', 'all', 'nb_load_raw_vault_location',
       'Completed', current_timestamp(), current_timestamp())
""")

print("nb_load_raw_vault_location: DONE")
