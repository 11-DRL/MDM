-- =============================================================================
-- Fabric Warehouse: wh_mdm — DDL
-- Type mapping: STRING→VARCHAR(4000), BOOLEAN→BIT, DOUBLE→FLOAT,
--               TIMESTAMP→DATETIME2(6), BINARY→VARBINARY(32)
-- Idempotent: IF NOT EXISTS wrappers for schemas + tables.
-- =============================================================================

-- ──────── SCHEMAS ───────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{SCHEMA_CONFIG}}') EXEC('CREATE SCHEMA {{SCHEMA_CONFIG}}');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{SCHEMA_SILVER}}')  EXEC('CREATE SCHEMA {{SCHEMA_SILVER}}');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{SCHEMA_GOLD}}')       EXEC('CREATE SCHEMA {{SCHEMA_GOLD}}');

-- ──────── {{SCHEMA_CONFIG}} ────────────────────────────────────────────────────────
DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.entity_config;
CREATE TABLE {{SCHEMA_CONFIG}}.entity_config (
  entity_id              VARCHAR(64)    NOT NULL,
  entity_name            VARCHAR(200)   NOT NULL,
  hub_table              VARCHAR(200)   NOT NULL,
  is_active              BIT            NOT NULL,
  match_threshold        FLOAT          NOT NULL,
  auto_accept_threshold  FLOAT          NOT NULL,
  created_at             DATETIME2(6)   NOT NULL,
  updated_at             DATETIME2(6) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.field_config;
CREATE TABLE {{SCHEMA_CONFIG}}.field_config (
  entity_id        VARCHAR(64)    NOT NULL,
  field_name       VARCHAR(100)   NOT NULL,
  match_weight     FLOAT          NOT NULL,
  is_blocking_key  BIT            NOT NULL,
  standardizer     VARCHAR(100) NULL,
  is_active        BIT            NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.source_priority;
CREATE TABLE {{SCHEMA_CONFIG}}.source_priority (
  entity_id      VARCHAR(64)    NOT NULL,
  source_system  VARCHAR(64)    NOT NULL,
  field_name     VARCHAR(100)   NOT NULL,
  priority       INT            NOT NULL,
  created_at     DATETIME2(6)   NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.hash_config;
CREATE TABLE {{SCHEMA_CONFIG}}.hash_config (
  entity_id             VARCHAR(64)    NOT NULL,
  source_system         VARCHAR(64)    NOT NULL,
  source_id_column      VARCHAR(200)   NOT NULL,
  business_key_template VARCHAR(400)   NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.source_watermark;
CREATE TABLE {{SCHEMA_CONFIG}}.source_watermark (
  entity_id       VARCHAR(64)    NOT NULL,
  source_system   VARCHAR(64)    NOT NULL,
  last_load_date  DATETIME2(6)   NOT NULL,
  last_run_id     VARCHAR(100) NULL,
  updated_at      DATETIME2(6)   NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_CONFIG}}.execution_log;
CREATE TABLE {{SCHEMA_CONFIG}}.execution_log (
  run_id          VARCHAR(100)   NOT NULL,
  entity_id       VARCHAR(64)    NOT NULL,
  source_system   VARCHAR(64) NULL,
  process_name    VARCHAR(200)   NOT NULL,
  process_params  VARCHAR(4000) NULL,
  status          VARCHAR(32)    NOT NULL,
  records_loaded  BIGINT NULL,
  records_matched BIGINT NULL,
  started_at      DATETIME2(6)   NOT NULL,
  completed_at    DATETIME2(6) NULL,
  error_message   VARCHAR(4000) NULL
);

-- ──────── {{SCHEMA_SILVER}} ─────────────────────────────────────────────────────────
DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.hub_location;
CREATE TABLE {{SCHEMA_SILVER}}.hub_location (
  location_hk   VARBINARY(32)     NOT NULL,
  business_key  VARCHAR(200)   NOT NULL,
  load_date     DATETIME2(6)   NOT NULL,
  record_source VARCHAR(64)    NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.sat_location_lightspeed;
CREATE TABLE {{SCHEMA_SILVER}}.sat_location_lightspeed (
  location_hk   VARBINARY(32)     NOT NULL,
  load_date     DATETIME2(6)   NOT NULL,
  load_end_date DATETIME2(6) NULL,
  hash_diff     VARBINARY(32)     NOT NULL,
  record_source VARCHAR(64)    NOT NULL,
  name          VARCHAR(400) NULL,
  country       VARCHAR(100) NULL,
  city          VARCHAR(200) NULL,
  timezone      VARCHAR(64) NULL,
  currency_code VARCHAR(8) NULL,
  bl_id         BIGINT NULL,
  is_active     BIT NULL,
  name_std      VARCHAR(400) NULL,
  country_std   VARCHAR(100) NULL,
  city_std      VARCHAR(200) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.sat_location_yext;
CREATE TABLE {{SCHEMA_SILVER}}.sat_location_yext (
  location_hk   VARBINARY(32)     NOT NULL,
  load_date     DATETIME2(6)   NOT NULL,
  load_end_date DATETIME2(6) NULL,
  hash_diff     VARBINARY(32)     NOT NULL,
  record_source VARCHAR(64)    NOT NULL,
  name          VARCHAR(400) NULL,
  address_line1 VARCHAR(400) NULL,
  city          VARCHAR(200) NULL,
  postal_code   VARCHAR(32) NULL,
  country_code  VARCHAR(8) NULL,
  phone         VARCHAR(64) NULL,
  website_url   VARCHAR(500) NULL,
  latitude      FLOAT NULL,
  longitude     FLOAT NULL,
  avg_rating    FLOAT NULL,
  review_count  INT NULL,
  name_std      VARCHAR(400) NULL,
  country_std   VARCHAR(100) NULL,
  city_std      VARCHAR(200) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.sat_location_mcwin;
CREATE TABLE {{SCHEMA_SILVER}}.sat_location_mcwin (
  location_hk     VARBINARY(32)     NOT NULL,
  load_date       DATETIME2(6)   NOT NULL,
  load_end_date   DATETIME2(6) NULL,
  hash_diff       VARBINARY(32)     NOT NULL,
  record_source   VARCHAR(64)    NOT NULL,
  restaurant_name VARCHAR(400) NULL,
  cost_center     VARCHAR(64) NULL,
  region          VARCHAR(100) NULL,
  country         VARCHAR(100) NULL,
  city            VARCHAR(200) NULL,
  zip_code        VARCHAR(32) NULL,
  address         VARCHAR(400) NULL,
  is_active       VARCHAR(16) NULL,
  name_std        VARCHAR(400) NULL,
  country_std     VARCHAR(100) NULL,
  city_std        VARCHAR(200) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.sat_location_gopos;
CREATE TABLE {{SCHEMA_SILVER}}.sat_location_gopos (
  location_hk   VARBINARY(32)     NOT NULL,
  load_date     DATETIME2(6)   NOT NULL,
  load_end_date DATETIME2(6) NULL,
  hash_diff     VARBINARY(32)     NOT NULL,
  record_source VARCHAR(64)    NOT NULL,
  location_name VARCHAR(400) NULL,
  address       VARCHAR(400) NULL,
  city          VARCHAR(200) NULL,
  zip_code      VARCHAR(32) NULL,
  country       VARCHAR(100) NULL,
  phone         VARCHAR(64) NULL,
  is_active     BIT NULL,
  name_std      VARCHAR(400) NULL,
  country_std   VARCHAR(100) NULL,
  city_std      VARCHAR(200) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.sat_location_manual;
CREATE TABLE {{SCHEMA_SILVER}}.sat_location_manual (
  location_hk   VARBINARY(32)     NOT NULL,
  load_date     DATETIME2(6)   NOT NULL,
  load_end_date DATETIME2(6) NULL,
  hash_diff     VARBINARY(32)     NOT NULL,
  record_source VARCHAR(64)    NOT NULL,
  name          VARCHAR(400) NULL,
  country       VARCHAR(100) NULL,
  city          VARCHAR(200) NULL,
  zip_code      VARCHAR(32) NULL,
  address       VARCHAR(400) NULL,
  phone         VARCHAR(64) NULL,
  website_url   VARCHAR(500) NULL,
  latitude      FLOAT NULL,
  longitude     FLOAT NULL,
  timezone      VARCHAR(64) NULL,
  currency_code VARCHAR(8) NULL,
  cost_center   VARCHAR(64) NULL,
  region        VARCHAR(100) NULL,
  notes         VARCHAR(4000) NULL,
  name_std      VARCHAR(400) NULL,
  country_std   VARCHAR(100) NULL,
  city_std      VARCHAR(200) NULL,
  created_by    VARCHAR(200) NULL,
  created_at    DATETIME2(6) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.pit_location;
CREATE TABLE {{SCHEMA_SILVER}}.pit_location (
  location_hk       VARBINARY(32)     NOT NULL,
  snapshot_date     DATETIME2(6)   NOT NULL,
  sat_lightspeed_ld DATETIME2(6) NULL,
  sat_yext_ld       DATETIME2(6) NULL,
  sat_mcwin_ld      DATETIME2(6) NULL,
  sat_gopos_ld      DATETIME2(6) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.bv_location_match_candidates;
CREATE TABLE {{SCHEMA_SILVER}}.bv_location_match_candidates (
  pair_id       VARCHAR(64)    NOT NULL,
  hk_left       VARBINARY(32)     NOT NULL,
  hk_right      VARBINARY(32)     NOT NULL,
  match_score   FLOAT          NOT NULL,
  match_type    VARCHAR(64) NULL,
  name_score    FLOAT NULL,
  city_match    BIT NULL,
  zip_match     BIT NULL,
  geo_score     FLOAT NULL,
  status        VARCHAR(32)    NOT NULL,
  created_at    DATETIME2(6)   NOT NULL,
  reviewed_by   VARCHAR(200) NULL,
  reviewed_at   DATETIME2(6) NULL,
  review_note   VARCHAR(4000) NULL,
  run_id        VARCHAR(100) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.bv_location_key_resolution;
CREATE TABLE {{SCHEMA_SILVER}}.bv_location_key_resolution (
  source_hk       VARBINARY(32)     NOT NULL,
  canonical_hk    VARBINARY(32)     NOT NULL,
  resolved_by     VARCHAR(200)   NOT NULL,
  resolved_at     DATETIME2(6)   NOT NULL,
  pair_id         VARCHAR(64) NULL,
  resolution_type VARCHAR(32)    NOT NULL
);

DROP TABLE IF EXISTS {{SCHEMA_SILVER}}.stewardship_log;
CREATE TABLE {{SCHEMA_SILVER}}.stewardship_log (
  log_id       VARCHAR(64)    NOT NULL,
  canonical_hk VARBINARY(32)     NOT NULL,
  action       VARCHAR(64)    NOT NULL,
  field_name   VARCHAR(100) NULL,
  old_value    VARCHAR(4000) NULL,
  new_value    VARCHAR(4000) NULL,
  changed_by   VARCHAR(200)   NOT NULL,
  changed_at   DATETIME2(6)   NOT NULL,
  pair_id      VARCHAR(64) NULL,
  reason       VARCHAR(4000) NULL
);

-- ──────── {{SCHEMA_GOLD}} ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS {{SCHEMA_GOLD}}.dim_location;
CREATE TABLE {{SCHEMA_GOLD}}.dim_location (
  location_sk          BIGINT         NOT NULL,
  location_hk          VARBINARY(32)     NOT NULL,
  valid_from           DATETIME2(6)   NOT NULL,
  valid_to             DATETIME2(6) NULL,
  is_current           BIT            NOT NULL,
  name                 VARCHAR(400) NULL,
  country              VARCHAR(100) NULL,
  city                 VARCHAR(200) NULL,
  zip_code             VARCHAR(32) NULL,
  address              VARCHAR(400) NULL,
  phone                VARCHAR(64) NULL,
  latitude             FLOAT NULL,
  longitude            FLOAT NULL,
  website_url          VARCHAR(500) NULL,
  timezone             VARCHAR(64) NULL,
  currency_code        VARCHAR(8) NULL,
  avg_rating           FLOAT NULL,
  review_count         INT NULL,
  cost_center          VARCHAR(64) NULL,
  region               VARCHAR(100) NULL,
  name_source          VARCHAR(64) NULL,
  country_source       VARCHAR(64) NULL,
  city_source          VARCHAR(64) NULL,
  created_at           DATETIME2(6)   NOT NULL,
  updated_at           DATETIME2(6) NULL,
  lightspeed_bl_id     BIGINT NULL,
  yext_id              VARCHAR(100) NULL,
  mcwin_restaurant_id  VARCHAR(100) NULL,
  gopos_location_id    VARCHAR(100) NULL
);

DROP TABLE IF EXISTS {{SCHEMA_GOLD}}.dim_location_quality;
CREATE TABLE {{SCHEMA_GOLD}}.dim_location_quality (
  location_hk        VARBINARY(32)     NOT NULL,
  snapshot_date      DATETIME2(6)   NOT NULL,
  sources_count      INT NULL,
  completeness_score FLOAT NULL,
  has_lightspeed     BIT NULL,
  has_yext           BIT NULL,
  has_mcwin          BIT NULL,
  has_gopos          BIT NULL,
  last_match_score   FLOAT NULL
);