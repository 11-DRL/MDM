-- =============================================================================
-- Bronze Layer — Legal Entity (manual / CSV ingestion)
-- Lakehouse: lh_mdm | Schema: {{SCHEMA_BRONZE}}
-- =============================================================================

CREATE TABLE IF NOT EXISTS {{SCHEMA_BRONZE}}.legal_entity_manual (
  -- Business fields
  legal_entity_code     STRING,       -- PK biznesowy, np. 'LO-PL-001'
  name                  STRING,
  tax_id                STRING,       -- NIP / VAT-EU, np. 'PL1234567890'
  country               STRING,       -- ISO 2-letter
  currency_code         STRING,       -- ISO 4217
  parent_entity_code    STRING,       -- self-ref hierarchy (NULL = top-level)
  consolidation_method  STRING,       -- 'full' | 'equity' | 'proportional' | 'none'
  ownership_pct         DOUBLE,       -- 0.0-100.0 (percentage)
  valid_from            DATE,
  valid_to              DATE,
  is_active             BOOLEAN,
  -- Ingestion metadata
  _source_system        STRING  NOT NULL DEFAULT 'manual',
  _load_date            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  _run_id               STRING  NOT NULL,
  _file_name            STRING,
  _tenant_name          STRING  NOT NULL
) USING DELTA;
