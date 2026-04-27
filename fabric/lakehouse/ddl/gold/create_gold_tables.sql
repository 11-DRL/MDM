-- =============================================================================
-- {{SCHEMA_GOLD}} Layer — Golden Records (dim_location SCD2)
-- Lakehouse: lh_mdm | Schema: {{SCHEMA_GOLD}}
-- Derivowany z {{SCHEMA_SILVER}} przez nb_derive_gold_location
-- Survivorship: Lightspeed (1) > McWin (2) > Yext (3) > GoPOS (4)
-- =============================================================================

CREATE TABLE IF NOT EXISTS {{SCHEMA_GOLD}}.dim_location (
  -- Klucz surrogate
  location_sk          BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY,
  -- Klucz MDM (Hub key)
  location_hk          BINARY    NOT NULL,
  -- SCD2 kontrola
  valid_from           TIMESTAMP NOT NULL,
  valid_to             TIMESTAMP,             -- NULL = aktualny rekord
  is_current           BOOLEAN   NOT NULL DEFAULT true,
  -- Atrybuty golden record (best-source-wins)
  name                 STRING,               -- źródło: wg source_priority
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
  avg_rating           DOUBLE,               -- tylko Yext
  review_count         INT,                  -- tylko Yext
  cost_center          STRING,               -- tylko McWin
  region               STRING,               -- tylko McWin
  -- Lineage: skąd pochodzi każde kluczowe pole
  name_source          STRING,
  country_source       STRING,
  city_source          STRING,
  -- Metadata
  created_at           TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  updated_at           TIMESTAMP,
  -- Source crosswalk
  lightspeed_bl_id     BIGINT,
  yext_id              STRING,
  mcwin_restaurant_id  STRING,
  gopos_location_id    STRING
) USING DELTA;

-- Quality metrics per golden record
CREATE TABLE IF NOT EXISTS {{SCHEMA_GOLD}}.dim_location_quality (
  location_hk          BINARY    NOT NULL,
  snapshot_date        TIMESTAMP NOT NULL,
  sources_count        INT,                  -- ile źródeł dostarcza dane
  completeness_score   DOUBLE,              -- % wypełnionych kluczowych pól
  has_lightspeed       BOOLEAN,
  has_yext             BOOLEAN,
  has_mcwin            BOOLEAN,
  has_gopos            BOOLEAN,
  last_match_score     DOUBLE               -- najwyższy match_score z par
) USING DELTA;
