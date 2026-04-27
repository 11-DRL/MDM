# Databricks/Fabric Notebook
# Name: nb_load_raw_vault
# Description: GENERIC raw vault loader — reads entity config from mdm_config,
#              loads Bronze → Silver DV (Hub + Satellites) for ANY entity.
#              Replaces per-entity notebooks (nb_load_raw_vault_location.py is now a thin wrapper).
# Triggerowany przez: PL_MDM_Master_* (z parametrem entity_id)

# ---------------------------------------------------------------------------
# CELL 1: Parameters
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
entity_id = dbutils.widgets.get("entity_id") if "dbutils" in dir() else "legal_entity"
tenant_name = dbutils.widgets.get("tenant_name") if "dbutils" in dir() else "losteria"
full_load = (dbutils.widgets.get("full_load") if "dbutils" in dir() else "false").lower() == "true"

print(f"run_id={run_id}, entity_id={entity_id}, tenant_name={tenant_name}, full_load={full_load}")

# ---------------------------------------------------------------------------
# CELL 2: Imports & Spark setup
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime, timezone
import hashlib, json

spark.conf.set("spark.sql.shuffle.partitions", "8")

LOAD_DATE = datetime.now(timezone.utc)

# ---------------------------------------------------------------------------
# CELL 3: Load entity configuration from mdm_config
# ---------------------------------------------------------------------------

# Entity config
entity_cfg_rows = spark.sql(f"""
    SELECT * FROM mdm_config.entity_config WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()
if not entity_cfg_rows:
    raise ValueError(f"Entity '{entity_id}' not found or inactive in mdm_config.entity_config")
entity_cfg = entity_cfg_rows[0].asDict()

HUB_TABLE = f"silver_dv.{entity_cfg['hub_table']}"
HK_COL = entity_cfg['hub_table'].replace('hub_', '') + '_hk'  # e.g. hub_legal_entity → legal_entity_hk
HAS_MATCHING = entity_cfg.get('match_engine', 'none') != 'none'

# Hash config (sources + business key templates)
hash_cfg = spark.sql(f"""
    SELECT source_system, source_id_column, business_key_template
    FROM mdm_config.hash_config WHERE entity_id = '{entity_id}'
""").collect()

SOURCES = {row.source_system: {
    'id_col': row.source_id_column,
    'bk_template': row.business_key_template
} for row in hash_cfg}

# Field config (for standardization)
field_cfg = spark.sql(f"""
    SELECT field_name, standardizer, is_golden_field
    FROM mdm_config.field_config WHERE entity_id = '{entity_id}' AND is_active = true
""").collect()

FIELD_STANDARDIZERS = {row.field_name: row.standardizer for row in field_cfg if row.standardizer}
GOLDEN_FIELDS = [row.field_name for row in field_cfg if row.is_golden_field]

print(f"Entity config loaded: hub={HUB_TABLE}, hk_col={HK_COL}, sources={list(SOURCES.keys())}")
print(f"Matching enabled: {HAS_MATCHING}, Golden fields: {GOLDEN_FIELDS}")

# ---------------------------------------------------------------------------
# CELL 4: Helper functions (same as location loader, but generic)
# ---------------------------------------------------------------------------

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
        "PL": "PL", "POLAND": "PL", "POLSKA": "PL",
    }
    return mapping.get(country_raw.strip().upper(), country_raw.strip().upper())

@F.udf(StringType())
def udf_normalize_tax_id(tax_id_raw):
    """Normalize tax ID: uppercase, remove dashes/spaces, keep country prefix."""
    if not tax_id_raw:
        return None
    return tax_id_raw.strip().upper().replace("-", "").replace(" ", "").replace(".", "")

def apply_standardizer(df, field_name, standardizer):
    """Apply a named standardizer to a column, returning a new _std column."""
    if standardizer == 'uppercase_strip_accents' or standardizer == 'uppercase':
        return df.withColumn(f"{field_name}_std", F.upper(F.trim(F.col(field_name))))
    elif standardizer == 'iso2_country_code':
        return df.withColumn(f"{field_name}_std", udf_normalize_country(F.col(field_name)))
    elif standardizer == 'strip_whitespace':
        return df.withColumn(f"{field_name}_std", F.regexp_replace(F.col(field_name), r'\s+', ''))
    elif standardizer == 'normalize_tax_id':
        return df.withColumn(f"{field_name}_std", udf_normalize_tax_id(F.col(field_name)))
    elif standardizer == 'round_3_decimals':
        return df.withColumn(f"{field_name}_std", F.round(F.col(field_name), 3).cast(StringType()))
    else:
        return df  # unknown standardizer, skip

# ---------------------------------------------------------------------------
# CELL 5: Load Key Resolution (if matching is enabled for this entity)
# ---------------------------------------------------------------------------

resolution_broadcast = None
if HAS_MATCHING and entity_cfg.get('bv_resolution_table'):
    res_table = f"silver_dv.{entity_cfg['bv_resolution_table']}"
    resolution_df = spark.sql(f"SELECT source_hk, canonical_hk FROM {res_table}")
    resolution_broadcast = F.broadcast(resolution_df)

def apply_key_resolution(df, hk_col=HK_COL):
    if resolution_broadcast is None:
        return df
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
# CELL 6: Watermarks
# ---------------------------------------------------------------------------
if not full_load:
    watermarks = spark.sql(f"""
        SELECT source_system, last_load_date
        FROM mdm_config.source_watermark WHERE entity_id = '{entity_id}'
    """).collect()
    wm = {row.source_system: row.last_load_date for row in watermarks}
else:
    wm = {s: datetime(1900, 1, 1, tzinfo=timezone.utc) for s in SOURCES.keys()}

print("Watermarks:", wm)

# ---------------------------------------------------------------------------
# CELL 7: Generic source loader — processes each source configured in hash_config
# ---------------------------------------------------------------------------

for source_system, src_cfg in SOURCES.items():
    sat_table_name = f"sat_{entity_id}_{source_system}"
    sat_full_name = f"silver_dv.{sat_table_name}"

    # Determine bronze table name (convention: <source_system>_<entity_plural> or manual table)
    # For now: read from a naming convention. Could be extended to store in config.
    # bronze table discovery: entity_id + source_system → bronze.<entity_id>_<source_system>
    bronze_table = f"bronze.{entity_id}_{source_system}"

    # Check if bronze table exists
    try:
        bronze_raw = spark.sql(f"""
            SELECT * FROM {bronze_table}
            WHERE _load_date > '{wm.get(source_system, "1900-01-01")}'
              AND _tenant_name = '{tenant_name}'
        """)
    except Exception as e:
        print(f"SKIP {source_system}: bronze table {bronze_table} not found ({e})")
        continue

    row_count = bronze_raw.count()
    if row_count == 0:
        print(f"SKIP {source_system}: no new rows in {bronze_table}")
        continue

    # Build business_key using template from hash_config
    id_col = src_cfg['id_col']
    bk_prefix = src_cfg['bk_template'].split('{')[0]  # e.g. 'manual|'

    prep = bronze_raw.withColumn(
        "business_key", F.concat(F.lit(bk_prefix), F.col(id_col).cast("string"))
    ).withColumn(HK_COL, udf_hk(F.col("business_key")))

    # Apply standardizers from field_config
    for field_name, standardizer in FIELD_STANDARDIZERS.items():
        if field_name in [c.name for c in prep.schema]:
            prep = apply_standardizer(prep, field_name, standardizer)

    # Apply key resolution (if matching enabled)
    prep = apply_key_resolution(prep, HK_COL)

    # MERGE to Hub
    hub_delta = DeltaTable.forName(spark, HUB_TABLE)
    hub_delta.alias("hub").merge(
        prep.alias("src"),
        f"hub.{HK_COL} = src.{HK_COL}"
    ).whenNotMatchedInsert(values={
        HK_COL:         f"src.{HK_COL}",
        "business_key":  "src.business_key",
        "entity_id":     F.lit(entity_id),
        "load_date":     F.lit(LOAD_DATE),
        "record_source": F.lit(source_system)
    }).execute()

    # Satellite: determine columns to hash for hash_diff (golden fields present in source)
    source_cols = [c.name for c in prep.schema]
    golden_in_source = [f for f in GOLDEN_FIELDS if f in source_cols]

    if golden_in_source:
        hash_cols = [F.col(f) for f in golden_in_source]
        sat_df = (
            prep
            .withColumn("hash_diff", udf_hash_diff(*hash_cols))
            .withColumn("load_date", F.lit(LOAD_DATE))
            .withColumn("load_end_date", F.lit(None).cast(TimestampType()))
            .withColumn("record_source", F.lit(source_system))
        )

        # Select: hk, DV control columns, all golden fields present, all _std columns
        std_cols = [c for c in source_cols if c.endswith('_std')]
        select_cols = [HK_COL, "load_date", "load_end_date", "hash_diff", "record_source"]
        select_cols += golden_in_source
        select_cols += std_cols
        # Add metadata columns if present
        for meta_col in ['created_by', 'created_at']:
            if meta_col in source_cols:
                select_cols.append(meta_col)

        sat_df = sat_df.select(*[c for c in select_cols if c in [col.name for col in sat_df.schema]])

        # Check if satellite table exists, create if not
        try:
            sat_delta = DeltaTable.forName(spark, sat_full_name)
        except Exception:
            print(f"Creating satellite table {sat_full_name}...")
            sat_df.limit(0).write.format("delta").mode("overwrite").saveAsTable(sat_full_name)
            sat_delta = DeltaTable.forName(spark, sat_full_name)

        # Close old rows where hash_diff changed
        sat_delta.alias("sat").merge(
            sat_df.alias("src"),
            f"sat.{HK_COL} = src.{HK_COL} AND sat.load_end_date IS NULL AND sat.hash_diff != src.hash_diff"
        ).whenMatchedUpdate(set={"load_end_date": F.lit(LOAD_DATE)}).execute()

        # Insert new rows (anti-join on current matching hash_diff)
        new_rows = sat_df.alias("src").join(
            spark.sql(f"SELECT {HK_COL}, hash_diff FROM {sat_full_name} WHERE load_end_date IS NULL").alias("e"),
            (F.col(f"src.{HK_COL}") == F.col(f"e.{HK_COL}")) &
            (F.col("src.hash_diff") == F.col("e.hash_diff")),
            "left_anti"
        )
        new_rows.write.format("delta").mode("append").saveAsTable(sat_full_name)

    print(f"{source_system}: {row_count} source rows processed → {HUB_TABLE} + {sat_full_name}")

    # Update watermark
    spark.sql(f"""
        MERGE INTO mdm_config.source_watermark AS w
        USING (SELECT '{entity_id}' as entity_id, '{source_system}' as source_system,
                      current_timestamp() as ts, '{run_id}' as run_id) AS s
        ON w.entity_id = s.entity_id AND w.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET last_load_date = s.ts, last_run_id = s.run_id, updated_at = s.ts
        WHEN NOT MATCHED THEN INSERT (entity_id, source_system, last_load_date, last_run_id, updated_at)
             VALUES (s.entity_id, s.source_system, s.ts, s.run_id, s.ts)
    """)

# ---------------------------------------------------------------------------
# CELL 8: Execution log
# ---------------------------------------------------------------------------
spark.sql(f"""
    INSERT INTO mdm_config.execution_log
      (run_id, entity_id, source_system, process_name, status, started_at, completed_at)
    VALUES
      ('{run_id}', '{entity_id}', 'all', 'nb_load_raw_vault',
       'Completed', current_timestamp(), current_timestamp())
""")

print(f"nb_load_raw_vault [{entity_id}]: DONE")
