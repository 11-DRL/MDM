-- =============================================================================
-- Silver Layer — Data Vault for Legal Entity
-- Lakehouse: lh_mdm | Schema: {{SCHEMA_SILVER}}
-- No matching engine — legal_entity is a reference entity (match_engine='none')
-- =============================================================================

-- ---------------------------------------------------------------------------
-- HUB: hub_legal_entity
-- One row per unique legal entity.
-- Hub key = SHA256('manual|' || legal_entity_code)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA_SILVER}}.hub_legal_entity (
  legal_entity_hk  BINARY    NOT NULL,   -- SHA256 hash key
  business_key     STRING    NOT NULL,   -- 'manual|LO-PL-001'
  entity_id        STRING    NOT NULL DEFAULT 'legal_entity',
  load_date        TIMESTAMP NOT NULL,
  record_source    STRING    NOT NULL    -- 'manual'
) USING DELTA;

CREATE INDEX IF NOT EXISTS idx_hub_legal_entity_bk
  ON {{SCHEMA_SILVER}}.hub_legal_entity (business_key);

-- ---------------------------------------------------------------------------
-- SATELLITE: sat_legal_entity_manual
-- Historized attributes from manual/CSV ingestion.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA_SILVER}}.sat_legal_entity_manual (
  legal_entity_hk      BINARY    NOT NULL,
  load_date            TIMESTAMP NOT NULL,
  load_end_date        TIMESTAMP,           -- NULL = current record
  hash_diff            BINARY    NOT NULL,
  record_source        STRING    NOT NULL DEFAULT 'manual',
  -- Attributes
  name                 STRING    NOT NULL,
  tax_id               STRING,              -- NIP / VAT-EU (normalized: no dashes, with prefix)
  country              STRING,              -- ISO 2-letter
  currency_code        STRING,              -- ISO 4217
  parent_entity_code   STRING,              -- self-ref to legal_entity_code
  consolidation_method STRING,              -- 'full' | 'equity' | 'proportional' | 'none'
  ownership_pct        DOUBLE,
  valid_from           DATE,
  valid_to             DATE,
  is_active            BOOLEAN   NOT NULL DEFAULT true,
  -- Standardized
  name_std             STRING,
  country_std          STRING,
  tax_id_std           STRING,              -- normalized tax_id (uppercase, no dashes/spaces)
  -- Metadata
  created_by           STRING,
  created_at           TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA;
