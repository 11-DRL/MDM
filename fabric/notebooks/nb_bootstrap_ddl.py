# Fabric Notebook
# Name: nb_bootstrap_ddl
# Description: Jednorazowy bootstrap — tworzy wszystkie schematy i tabele Delta w lh_mdm.
#              Uruchom JEDNORAZOWO na pustym Lakehouse przed pierwszym deployem.
#              Idempotentny: wszystkie CREATE TABLE mają IF NOT EXISTS.
# Triggerowany przez: deploy-fabric.ps1 (po utworzeniu Lakehouse)

# ---------------------------------------------------------------------------
# CELL 1: Spark setup
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
print("Starting DDL bootstrap for lh_mdm...")

# ---------------------------------------------------------------------------
# CELL 2: Schema — mdm_config
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS mdm_config")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.entity_config (
  entity_id        STRING  NOT NULL,
  entity_name      STRING  NOT NULL,
  hub_table        STRING  NOT NULL,
  is_active        BOOLEAN NOT NULL,
  match_threshold  DOUBLE  NOT NULL,
  auto_accept_threshold DOUBLE NOT NULL,
  created_at       TIMESTAMP NOT NULL,
  updated_at       TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.field_config (
  entity_id        STRING  NOT NULL,
  field_name       STRING  NOT NULL,
  match_weight     DOUBLE  NOT NULL,
  is_blocking_key  BOOLEAN NOT NULL,
  standardizer     STRING,
  is_active        BOOLEAN NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.source_priority (
  entity_id        STRING  NOT NULL,
  source_system    STRING  NOT NULL,
  field_name       STRING  NOT NULL,
  priority         INT     NOT NULL,
  created_at       TIMESTAMP NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.hash_config (
  entity_id             STRING NOT NULL,
  source_system         STRING NOT NULL,
  source_id_column      STRING NOT NULL,
  business_key_template STRING NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.source_watermark (
  entity_id     STRING    NOT NULL,
  source_system STRING    NOT NULL,
  last_load_date TIMESTAMP NOT NULL,
  last_run_id   STRING,
  updated_at    TIMESTAMP NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS mdm_config.execution_log (
  run_id         STRING    NOT NULL,
  entity_id      STRING    NOT NULL,
  source_system  STRING,
  process_name   STRING    NOT NULL,
  process_params STRING,
  status         STRING    NOT NULL,
  records_loaded BIGINT,
  records_matched BIGINT,
  started_at     TIMESTAMP NOT NULL,
  completed_at   TIMESTAMP,
  error_message  STRING
) USING DELTA
""")

print("OK: mdm_config schema")

# ---------------------------------------------------------------------------
# CELL 3: Schema — bronze
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.lightspeed_businesses (
  businessId     BIGINT,
  businessName   STRING,
  currencyCode   STRING,
  blId           BIGINT,
  blName         STRING,
  country        STRING,
  timezone       STRING,
  _source_system STRING  NOT NULL,
  _load_date     TIMESTAMP NOT NULL,
  _run_id        STRING  NOT NULL,
  _tenant_name   STRING  NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.yext_locations (
  id                   STRING,
  name                 STRING,
  address_line1        STRING,
  address_city         STRING,
  address_postal_code  STRING,
  address_country_code STRING,
  phone                STRING,
  website_url          STRING,
  display_lat          DOUBLE,
  display_lng          DOUBLE,
  avg_rating           DOUBLE,
  review_count         INT,
  _source_system       STRING  NOT NULL,
  _load_date           TIMESTAMP NOT NULL,
  _run_id              STRING  NOT NULL,
  _tenant_name         STRING  NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.mcwin_restaurant_masterdata (
  restaurant_id   STRING,
  restaurant_name STRING,
  cost_center     STRING,
  region          STRING,
  country         STRING,
  city            STRING,
  zip_code        STRING,
  address         STRING,
  is_active       STRING,
  _source_system  STRING  NOT NULL,
  _load_date      TIMESTAMP NOT NULL,
  _run_id         STRING  NOT NULL,
  _file_name      STRING,
  _tenant_name    STRING  NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.gopos_locations (
  location_id    STRING,
  location_name  STRING,
  address        STRING,
  city           STRING,
  zip_code       STRING,
  country        STRING,
  phone          STRING,
  is_active      BOOLEAN,
  _source_system STRING  NOT NULL,
  _load_date     TIMESTAMP NOT NULL,
  _run_id        STRING  NOT NULL,
  _tenant_name   STRING  NOT NULL
) USING DELTA
""")

print("OK: bronze schema")

# ---------------------------------------------------------------------------
# CELL 4: Schema — silver_dv
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_dv")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.hub_location (
  location_hk   BINARY    NOT NULL,
  business_key  STRING    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  record_source STRING    NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_lightspeed (
  location_hk   BINARY    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  load_end_date TIMESTAMP,
  hash_diff     BINARY    NOT NULL,
  record_source STRING    NOT NULL,
  name          STRING,
  country       STRING,
  city          STRING,
  timezone      STRING,
  currency_code STRING,
  bl_id         BIGINT,
  is_active     BOOLEAN,
  name_std      STRING,
  country_std   STRING,
  city_std      STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_yext (
  location_hk   BINARY    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  load_end_date TIMESTAMP,
  hash_diff     BINARY    NOT NULL,
  record_source STRING    NOT NULL,
  name          STRING,
  address_line1 STRING,
  city          STRING,
  postal_code   STRING,
  country_code  STRING,
  phone         STRING,
  website_url   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  avg_rating    DOUBLE,
  review_count  INT,
  name_std      STRING,
  country_std   STRING,
  city_std      STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_mcwin (
  location_hk     BINARY    NOT NULL,
  load_date       TIMESTAMP NOT NULL,
  load_end_date   TIMESTAMP,
  hash_diff       BINARY    NOT NULL,
  record_source   STRING    NOT NULL,
  restaurant_name STRING,
  cost_center     STRING,
  region          STRING,
  country         STRING,
  city            STRING,
  zip_code        STRING,
  address         STRING,
  is_active       STRING,
  name_std        STRING,
  country_std     STRING,
  city_std        STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_gopos (
  location_hk   BINARY    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  load_end_date TIMESTAMP,
  hash_diff     BINARY    NOT NULL,
  record_source STRING    NOT NULL,
  location_name STRING,
  address       STRING,
  city          STRING,
  zip_code      STRING,
  country       STRING,
  phone         STRING,
  is_active     BOOLEAN,
  name_std      STRING,
  country_std   STRING,
  city_std      STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_manual (
  location_hk   BINARY    NOT NULL,
  load_date     TIMESTAMP NOT NULL,
  load_end_date TIMESTAMP,
  hash_diff     BINARY    NOT NULL,
  record_source STRING    NOT NULL,
  name          STRING,
  country       STRING,
  city          STRING,
  zip_code      STRING,
  address       STRING,
  phone         STRING,
  website_url   STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  timezone      STRING,
  currency_code STRING,
  cost_center   STRING,
  region        STRING,
  notes         STRING,
  name_std      STRING,
  country_std   STRING,
  city_std      STRING,
  created_by    STRING,
  created_at    TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.pit_location (
  location_hk       BINARY    NOT NULL,
  snapshot_date     TIMESTAMP NOT NULL,
  sat_lightspeed_ld TIMESTAMP,
  sat_yext_ld       TIMESTAMP,
  sat_mcwin_ld      TIMESTAMP,
  sat_gopos_ld      TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.bv_location_match_candidates (
  pair_id       STRING    NOT NULL,
  hk_left       BINARY    NOT NULL,
  hk_right      BINARY    NOT NULL,
  match_score   DOUBLE    NOT NULL,
  match_type    STRING,
  name_score    DOUBLE,
  city_match    BOOLEAN,
  zip_match     BOOLEAN,
  geo_score     DOUBLE,
  status        STRING    NOT NULL,
  created_at    TIMESTAMP NOT NULL,
  reviewed_by   STRING,
  reviewed_at   TIMESTAMP,
  review_note   STRING,
  run_id        STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.bv_location_key_resolution (
  source_hk       BINARY    NOT NULL,
  canonical_hk    BINARY    NOT NULL,
  resolved_by     STRING    NOT NULL,
  resolved_at     TIMESTAMP NOT NULL,
  pair_id         STRING,
  resolution_type STRING    NOT NULL
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dv.stewardship_log (
  log_id       STRING    NOT NULL,
  canonical_hk BINARY    NOT NULL,
  action       STRING    NOT NULL,
  field_name   STRING,
  old_value    STRING,
  new_value    STRING,
  changed_by   STRING    NOT NULL,
  changed_at   TIMESTAMP NOT NULL,
  pair_id      STRING,
  reason       STRING
) USING DELTA
""")

print("OK: silver_dv schema")

# ---------------------------------------------------------------------------
# CELL 5: Schema — gold
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_location (
  location_sk          BIGINT    NOT NULL,
  location_hk          BINARY    NOT NULL,
  valid_from           TIMESTAMP NOT NULL,
  valid_to             TIMESTAMP,
  is_current           BOOLEAN   NOT NULL,
  name                 STRING,
  country              STRING,
  city                 STRING,
  zip_code             STRING,
  address              STRING,
  phone                STRING,
  latitude             DOUBLE,
  longitude            DOUBLE,
  website_url          STRING,
  timezone             STRING,
  currency_code        STRING,
  avg_rating           DOUBLE,
  review_count         INT,
  cost_center          STRING,
  region               STRING,
  name_source          STRING,
  country_source       STRING,
  city_source          STRING,
  created_at           TIMESTAMP NOT NULL,
  updated_at           TIMESTAMP,
  lightspeed_bl_id     BIGINT,
  yext_id              STRING,
  mcwin_restaurant_id  STRING,
  gopos_location_id    STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_location_quality (
  location_hk        BINARY    NOT NULL,
  snapshot_date      TIMESTAMP NOT NULL,
  sources_count      INT,
  completeness_score DOUBLE,
  has_lightspeed     BOOLEAN,
  has_yext           BOOLEAN,
  has_mcwin          BOOLEAN,
  has_gopos          BOOLEAN,
  last_match_score   DOUBLE
) USING DELTA
""")

print("OK: gold schema")

# ---------------------------------------------------------------------------
# CELL 6: Podsumowanie
# ---------------------------------------------------------------------------
schemas = ["mdm_config", "bronze", "silver_dv", "gold"]
for s in schemas:
    tables = spark.sql(f"SHOW TABLES IN {s}").collect()
    print(f"  {s}: {len(tables)} tabel")

print("✅ DDL bootstrap zakończony pomyślnie!")
