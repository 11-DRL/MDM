-- =============================================================================
-- Migration: Extend mdm_config tables for multi-entity support
-- Run AFTER create_mdm_config_tables.sql, BEFORE any new entity seeds
-- Backward-compatible: existing business_location data remains intact.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. entity_config — new columns for generic table routing + display
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_CONFIG}}.entity_config ADD COLUMNS (
  bv_match_table       STRING,    -- 'bv_location_match_candidates' (NULL = no matching for this entity)
  bv_resolution_table  STRING,    -- 'bv_location_key_resolution'
  gold_table           STRING,    -- 'dim_location'
  pit_table            STRING,    -- 'pit_location' (NULL = no PIT)
  match_engine         STRING  DEFAULT 'jaro_winkler', -- 'jaro_winkler' | 'exact' | 'none'
  display_label_pl     STRING,
  display_label_en     STRING,
  icon                 STRING,    -- icon name for UI sidebar (e.g. 'MapPin', 'Building2', 'Users')
  display_order        INT DEFAULT 100
);

-- Backfill for existing business_location entity
UPDATE {{SCHEMA_CONFIG}}.entity_config
SET bv_match_table      = 'bv_location_match_candidates',
    bv_resolution_table = 'bv_location_key_resolution',
    gold_table          = 'dim_location',
    pit_table           = 'pit_location',
    match_engine        = 'jaro_winkler',
    display_label_pl    = 'Lokalizacje biznesowe',
    display_label_en    = 'Business Locations',
    icon                = 'MapPin',
    display_order       = 10
WHERE entity_id = 'business_location';

-- ---------------------------------------------------------------------------
-- 2. field_config — new columns for UI rendering + validation + overrides
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_CONFIG}}.field_config ADD COLUMNS (
  display_name_pl  STRING,
  display_name_en  STRING,
  display_order    INT     DEFAULT 100,
  ui_widget        STRING  DEFAULT 'text',  -- 'text'|'select'|'date'|'number'|'boolean'|'textarea'
  is_overridable   BOOLEAN DEFAULT true,    -- can steward override in GoldenViewer?
  is_required      BOOLEAN DEFAULT false,
  validators_json  STRING,                  -- JSON: [{"type":"maxLength","value":255}]
  lookup_entity    STRING,                  -- FK to another entity_id for select widget
  lookup_field     STRING,                  -- field in lookup entity to display
  is_golden_field  BOOLEAN DEFAULT true,    -- appears in gold table
  group_name       STRING                   -- UI grouping (e.g. 'Address', 'Financial', 'Contact')
);

-- Backfill display metadata for existing business_location fields
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Nazwa',         display_name_en='Name',          display_order=10,  ui_widget='text',   is_required=true,  group_name='Podstawowe' WHERE entity_id='business_location' AND field_name='name';
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Miasto',        display_name_en='City',          display_order=20,  ui_widget='text',   is_required=true,  group_name='Adres'      WHERE entity_id='business_location' AND field_name='city';
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Kraj',          display_name_en='Country',       display_order=30,  ui_widget='select', is_required=true,  group_name='Adres'      WHERE entity_id='business_location' AND field_name='country';
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Kod pocztowy',  display_name_en='Zip Code',      display_order=40,  ui_widget='text',   is_required=false, group_name='Adres'      WHERE entity_id='business_location' AND field_name='zip_code';
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Szerokość geo', display_name_en='Latitude',      display_order=50,  ui_widget='number', is_required=false, group_name='Geolocation' WHERE entity_id='business_location' AND field_name='latitude';
UPDATE {{SCHEMA_CONFIG}}.field_config SET display_name_pl='Długość geo',   display_name_en='Longitude',     display_order=60,  ui_widget='number', is_required=false, group_name='Geolocation' WHERE entity_id='business_location' AND field_name='longitude';

-- ---------------------------------------------------------------------------
-- 3. NEW TABLE: entity_relationship (domain hierarchy / navigation)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA_CONFIG}}.entity_relationship (
  parent_entity_id STRING NOT NULL,   -- e.g. 'legal_entity'
  child_entity_id  STRING NOT NULL,   -- e.g. 'chart_of_accounts'
  relationship     STRING NOT NULL,   -- 'parent_child' | 'reference' | 'mapping'
  fk_field         STRING,            -- field in child that references parent (e.g. 'legal_entity_code')
  description      STRING,
  created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA;

-- ---------------------------------------------------------------------------
-- 4. NEW TABLE: coa_mapping_rule (auto-mapping rules for Chart of Accounts)
-- Needed in F2 but created now so DDL is complete.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA_CONFIG}}.coa_mapping_rule (
  rule_id          STRING  NOT NULL DEFAULT uuid(),
  rule_name        STRING  NOT NULL,
  match_type       STRING  NOT NULL,   -- 'prefix' | 'regex' | 'keyword' | 'range'
  match_pattern    STRING  NOT NULL,   -- e.g. '^4[0-9]{3}' or 'revenue'
  target_group_account_code STRING NOT NULL,
  confidence       STRING  NOT NULL DEFAULT 'medium', -- 'high' | 'medium' | 'low'
  is_active        BOOLEAN NOT NULL DEFAULT true,
  created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA;
