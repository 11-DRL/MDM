-- =============================================================================
-- Migration: Add entity_id to Silver DV tables for multi-entity support
-- Run AFTER alter_mdm_config_multi_entity.sql
-- Backward-compatible: backfills 'business_location' for all existing rows.
-- Existing table names (hub_location, sat_location_*, etc.) are KEPT —
-- new entities get new tables (hub_legal_entity, sat_legal_entity_*, etc.)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- hub_location: add entity_id
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_SILVER}}.hub_location ADD COLUMNS (
  entity_id STRING DEFAULT 'business_location'
);
UPDATE {{SCHEMA_SILVER}}.hub_location SET entity_id = 'business_location' WHERE entity_id IS NULL;

-- ---------------------------------------------------------------------------
-- bv_location_match_candidates: add entity_id
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_SILVER}}.bv_location_match_candidates ADD COLUMNS (
  entity_id STRING DEFAULT 'business_location'
);
UPDATE {{SCHEMA_SILVER}}.bv_location_match_candidates SET entity_id = 'business_location' WHERE entity_id IS NULL;

-- ---------------------------------------------------------------------------
-- bv_location_key_resolution: add entity_id
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_SILVER}}.bv_location_key_resolution ADD COLUMNS (
  entity_id STRING DEFAULT 'business_location'
);
UPDATE {{SCHEMA_SILVER}}.bv_location_key_resolution SET entity_id = 'business_location' WHERE entity_id IS NULL;

-- ---------------------------------------------------------------------------
-- pit_location: add entity_id
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_SILVER}}.pit_location ADD COLUMNS (
  entity_id STRING DEFAULT 'business_location'
);
UPDATE {{SCHEMA_SILVER}}.pit_location SET entity_id = 'business_location' WHERE entity_id IS NULL;

-- ---------------------------------------------------------------------------
-- stewardship_log: add entity_id (log was already generic but lacked entity FK)
-- ---------------------------------------------------------------------------
ALTER TABLE {{SCHEMA_SILVER}}.stewardship_log ADD COLUMNS (
  entity_id STRING DEFAULT 'business_location'
);
UPDATE {{SCHEMA_SILVER}}.stewardship_log SET entity_id = 'business_location' WHERE entity_id IS NULL;
