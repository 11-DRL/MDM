-- =============================================================================
-- Silver Layer — Data Vault Lite (Hubs + Satellites)
-- Lakehouse: lh_mdm | Schema: silver_dv
-- =============================================================================

-- ---------------------------------------------------------------------------
-- HUB: hub_location
-- Jeden wiersz per unikalna lokalizacja (business entity identity)
-- Hub key = SHA256(source_system || '|' || source_id)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.hub_location (
  location_hk      BINARY  NOT NULL,   -- SHA256 hash key
  business_key     STRING  NOT NULL,   -- 'lightspeed|41839-1' (source_system|source_id)
  load_date        TIMESTAMP NOT NULL,
  record_source    STRING  NOT NULL    -- 'lightspeed', 'yext', 'mcwin', 'gopos'
) USING DELTA;

-- Indeks na business_key dla szybkiego MERGE
CREATE INDEX IF NOT EXISTS idx_hub_location_bk ON silver_dv.hub_location (business_key);

-- ---------------------------------------------------------------------------
-- SATELLITE: sat_location_lightspeed
-- Historyzowane atrybuty z Lightspeed (load_end_date = NULL = aktualny)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_lightspeed (
  location_hk      BINARY    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  load_end_date    TIMESTAMP,           -- NULL = aktualny rekord
  hash_diff        BINARY    NOT NULL,  -- SHA256 wszystkich atrybutów
  record_source    STRING    NOT NULL DEFAULT 'lightspeed',
  -- Atrybuty
  name             STRING,
  country          STRING,
  city             STRING,
  timezone         STRING,
  currency_code    STRING,
  bl_id            BIGINT,             -- businessLocationId
  is_active        BOOLEAN,
  -- Standaryzowane
  name_std         STRING,             -- upper(trim(name))
  country_std      STRING,             -- ISO 2-letter
  city_std         STRING              -- upper(trim(city))
) USING DELTA;

-- ---------------------------------------------------------------------------
-- SATELLITE: sat_location_yext
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_yext (
  location_hk      BINARY    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  load_end_date    TIMESTAMP,
  hash_diff        BINARY    NOT NULL,
  record_source    STRING    NOT NULL DEFAULT 'yext',
  -- Atrybuty
  name             STRING,
  address_line1    STRING,
  city             STRING,
  postal_code      STRING,
  country_code     STRING,
  phone            STRING,
  website_url      STRING,
  latitude         DOUBLE,
  longitude        DOUBLE,
  avg_rating       DOUBLE,
  review_count     INT,
  -- Standaryzowane
  name_std         STRING,
  country_std      STRING,
  city_std         STRING
) USING DELTA;

-- ---------------------------------------------------------------------------
-- SATELLITE: sat_location_mcwin
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_mcwin (
  location_hk      BINARY    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  load_end_date    TIMESTAMP,
  hash_diff        BINARY    NOT NULL,
  record_source    STRING    NOT NULL DEFAULT 'mcwin',
  -- Atrybuty
  restaurant_name  STRING,
  cost_center      STRING,
  region           STRING,
  country          STRING,
  city             STRING,
  zip_code         STRING,
  address          STRING,
  is_active        STRING,
  -- Standaryzowane
  name_std         STRING,
  country_std      STRING,
  city_std         STRING
) USING DELTA;

-- ---------------------------------------------------------------------------
-- SATELLITE: sat_location_gopos
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.sat_location_gopos (
  location_hk      BINARY    NOT NULL,
  load_date        TIMESTAMP NOT NULL,
  load_end_date    TIMESTAMP,
  hash_diff        BINARY    NOT NULL,
  record_source    STRING    NOT NULL DEFAULT 'gopos',
  -- Atrybuty
  location_name    STRING,
  address          STRING,
  city             STRING,
  zip_code         STRING,
  country          STRING,
  phone            STRING,
  is_active        BOOLEAN,
  -- Standaryzowane
  name_std         STRING,
  country_std      STRING,
  city_std         STRING
) USING DELTA;

-- ---------------------------------------------------------------------------
-- BUSINESS VAULT: Match Candidates
-- Pary hub_location do przejrzenia przez stewarda
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.bv_location_match_candidates (
  pair_id          STRING    NOT NULL,  -- UUID
  hk_left          BINARY    NOT NULL,  -- hub_location_hk (wyższy priorytet)
  hk_right         BINARY    NOT NULL,  -- hub_location_hk (niższy priorytet)
  match_score      DOUBLE    NOT NULL,  -- 0.0 - 1.0
  match_type       STRING,              -- 'exact_name', 'fuzzy_name_city', 'geo_proximity', 'composite'
  name_score       DOUBLE,
  city_match       BOOLEAN,
  zip_match        BOOLEAN,
  geo_score        DOUBLE,
  status           STRING    NOT NULL DEFAULT 'pending', -- pending | accepted | rejected | auto_accepted
  created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  reviewed_by      STRING,
  reviewed_at      TIMESTAMP,
  review_note      STRING
) USING DELTA;

-- ---------------------------------------------------------------------------
-- BUSINESS VAULT: Key Resolution (decyzje stewarda)
-- Steward decyduje: source_hk X = canonical_hk Y → ta sama restauracja
-- Przy następnym DV load: dane z X lądują w Satellite Y
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.bv_location_key_resolution (
  source_hk        BINARY    NOT NULL,  -- hash key do zastąpienia
  canonical_hk     BINARY    NOT NULL,  -- docelowy canonical hash key
  resolved_by      STRING    NOT NULL,  -- user email / 'auto_match'
  resolved_at      TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  pair_id          STRING,              -- link do bv_location_match_candidates
  resolution_type  STRING    NOT NULL DEFAULT 'manual' -- 'manual' | 'auto'
) USING DELTA;

-- ---------------------------------------------------------------------------
-- PIT: Point-In-Time (snapshot aktualnych Satellite load_date per Hub)
-- Przyspiesza derivację Gold — bez PIT każdy SELECT to multi-join
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.pit_location (
  location_hk           BINARY    NOT NULL,
  snapshot_date         TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  -- Latest load_date per satellite (NULL jeśli brak danych z tego źródła)
  sat_lightspeed_ld     TIMESTAMP,
  sat_yext_ld           TIMESTAMP,
  sat_mcwin_ld          TIMESTAMP,
  sat_gopos_ld          TIMESTAMP
) USING DELTA;

-- ---------------------------------------------------------------------------
-- AUDIT: Stewardship Log
-- Append-only, każda zmiana przez UI lub notebook
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_dv.stewardship_log (
  log_id           STRING    NOT NULL DEFAULT uuid(),
  canonical_hk     BINARY    NOT NULL,
  action           STRING    NOT NULL, -- 'accept_match', 'reject_match', 'override_field', 'manual_create'
  field_name       STRING,
  old_value        STRING,
  new_value        STRING,
  changed_by       STRING    NOT NULL,
  changed_at       TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  pair_id          STRING,
  reason           STRING
) USING DELTA;
