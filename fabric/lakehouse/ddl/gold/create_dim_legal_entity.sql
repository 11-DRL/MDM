-- =============================================================================
-- Gold Layer — Legal Entity (dim_legal_entity SCD2 + ownership hierarchy view)
-- Lakehouse: lh_mdm | Schema: {{SCHEMA_GOLD}}
-- Derived from Silver DV by nb_derive_gold (entity_id='legal_entity')
-- =============================================================================

CREATE TABLE IF NOT EXISTS {{SCHEMA_GOLD}}.dim_legal_entity (
  -- Surrogate key
  legal_entity_sk       BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY,
  -- MDM key (Hub key)
  legal_entity_hk       BINARY    NOT NULL,
  -- SCD2 control
  valid_from            TIMESTAMP NOT NULL,
  valid_to              TIMESTAMP,
  is_current            BOOLEAN   NOT NULL DEFAULT true,
  -- Golden attributes
  legal_entity_code     STRING    NOT NULL,  -- business PK ('LO-PL-001')
  name                  STRING    NOT NULL,
  tax_id                STRING,
  country               STRING,
  currency_code         STRING,
  parent_entity_code    STRING,             -- self-ref hierarchy
  consolidation_method  STRING    NOT NULL DEFAULT 'full',
  ownership_pct         DOUBLE    NOT NULL DEFAULT 100.0,
  is_active             BOOLEAN   NOT NULL DEFAULT true,
  -- Lineage
  name_source           STRING,
  -- Metadata
  created_at            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  updated_at            TIMESTAMP
) USING DELTA;

-- Quality metrics per legal entity
CREATE TABLE IF NOT EXISTS {{SCHEMA_GOLD}}.dim_legal_entity_quality (
  legal_entity_hk       BINARY    NOT NULL,
  snapshot_date         TIMESTAMP NOT NULL,
  sources_count         INT,
  completeness_score    DOUBLE,
  has_tax_id            BOOLEAN,
  has_parent            BOOLEAN,
  has_currency          BOOLEAN
) USING DELTA;

-- ---------------------------------------------------------------------------
-- Recursive hierarchy view: ownership tree with path
-- Usage: SELECT * FROM {{SCHEMA_GOLD}}.vw_legal_entity_hierarchy
-- ---------------------------------------------------------------------------
-- NOTE: Fabric Lakehouse (Spark SQL) does not support recursive CTEs natively.
-- This view is for Fabric Warehouse (T-SQL endpoint). For Lakehouse access,
-- use the notebook helper function build_hierarchy_df() in nb_derive_gold.
-- ---------------------------------------------------------------------------
-- CREATE VIEW {{SCHEMA_GOLD}}.vw_legal_entity_hierarchy AS
-- WITH RECURSIVE hierarchy AS (
--   SELECT legal_entity_code, name, parent_entity_code, consolidation_method,
--          ownership_pct, 1 AS level,
--          CAST(legal_entity_code AS VARCHAR(4000)) AS ownership_path,
--          ownership_pct AS effective_ownership_pct
--   FROM {{SCHEMA_GOLD}}.dim_legal_entity
--   WHERE parent_entity_code IS NULL AND is_current = true
--
--   UNION ALL
--
--   SELECT c.legal_entity_code, c.name, c.parent_entity_code, c.consolidation_method,
--          c.ownership_pct, h.level + 1,
--          CAST(h.ownership_path + ' > ' + c.legal_entity_code AS VARCHAR(4000)),
--          h.effective_ownership_pct * c.ownership_pct / 100.0
--   FROM {{SCHEMA_GOLD}}.dim_legal_entity c
--   JOIN hierarchy h ON c.parent_entity_code = h.legal_entity_code
--   WHERE c.is_current = true
-- )
-- SELECT * FROM hierarchy;
