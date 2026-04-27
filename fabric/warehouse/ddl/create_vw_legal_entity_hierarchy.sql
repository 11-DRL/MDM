-- =============================================================================
-- Warehouse DDL: Legal Entity Hierarchy View (T-SQL recursive CTE)
-- Fabric Warehouse: wh_mdm
-- Recursive ownership tree from gold.dim_legal_entity
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_legal_entity_hierarchy AS
WITH hierarchy AS (
    -- Anchor: top-level entities (no parent)
    SELECT
        legal_entity_code,
        name,
        parent_entity_code,
        consolidation_method,
        ownership_pct,
        country,
        currency_code,
        is_active,
        1 AS [level],
        CAST(legal_entity_code AS VARCHAR(4000)) AS ownership_path,
        ownership_pct AS effective_ownership_pct
    FROM gold.dim_legal_entity
    WHERE parent_entity_code IS NULL
      AND is_current = 1

    UNION ALL

    -- Recursive: children
    SELECT
        c.legal_entity_code,
        c.name,
        c.parent_entity_code,
        c.consolidation_method,
        c.ownership_pct,
        c.country,
        c.currency_code,
        c.is_active,
        h.[level] + 1,
        CAST(h.ownership_path + ' > ' + c.legal_entity_code AS VARCHAR(4000)),
        h.effective_ownership_pct * c.ownership_pct / 100.0
    FROM gold.dim_legal_entity c
    INNER JOIN hierarchy h ON c.parent_entity_code = h.legal_entity_code
    WHERE c.is_current = 1
)
SELECT
    legal_entity_code,
    name,
    parent_entity_code,
    consolidation_method,
    ownership_pct,
    effective_ownership_pct,
    country,
    currency_code,
    is_active,
    [level],
    ownership_path
FROM hierarchy;
GO
