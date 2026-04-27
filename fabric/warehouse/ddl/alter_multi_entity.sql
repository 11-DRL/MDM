-- =============================================================================
-- Warehouse T-SQL: Extend mdm_config for multi-entity + seed legal_entity
-- =============================================================================
GO

-- 1. entity_config — new columns
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='bv_match_table')
  ALTER TABLE mdm_config.entity_config ADD bv_match_table VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='bv_resolution_table')
  ALTER TABLE mdm_config.entity_config ADD bv_resolution_table VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='gold_table')
  ALTER TABLE mdm_config.entity_config ADD gold_table VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='pit_table')
  ALTER TABLE mdm_config.entity_config ADD pit_table VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='match_engine')
  ALTER TABLE mdm_config.entity_config ADD match_engine VARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='display_label_pl')
  ALTER TABLE mdm_config.entity_config ADD display_label_pl VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='display_label_en')
  ALTER TABLE mdm_config.entity_config ADD display_label_en VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='icon')
  ALTER TABLE mdm_config.entity_config ADD icon VARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.entity_config') AND name='display_order')
  ALTER TABLE mdm_config.entity_config ADD display_order INT NULL;
GO

-- 2. field_config — new columns
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='display_name_pl')
  ALTER TABLE mdm_config.field_config ADD display_name_pl VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='display_name_en')
  ALTER TABLE mdm_config.field_config ADD display_name_en VARCHAR(200) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='display_order')
  ALTER TABLE mdm_config.field_config ADD display_order INT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='ui_widget')
  ALTER TABLE mdm_config.field_config ADD ui_widget VARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='is_overridable')
  ALTER TABLE mdm_config.field_config ADD is_overridable BIT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='is_required')
  ALTER TABLE mdm_config.field_config ADD is_required BIT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='validators_json')
  ALTER TABLE mdm_config.field_config ADD validators_json VARCHAR(2000) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='lookup_entity')
  ALTER TABLE mdm_config.field_config ADD lookup_entity VARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='lookup_field')
  ALTER TABLE mdm_config.field_config ADD lookup_field VARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='is_golden_field')
  ALTER TABLE mdm_config.field_config ADD is_golden_field BIT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('mdm_config.field_config') AND name='group_name')
  ALTER TABLE mdm_config.field_config ADD group_name VARCHAR(100) NULL;
GO

-- 3. entity_relationship table
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='mdm_config' AND t.name='entity_relationship')
  CREATE TABLE mdm_config.entity_relationship (
    parent_entity_id VARCHAR(100) NOT NULL,
    child_entity_id  VARCHAR(100) NOT NULL,
    relationship     VARCHAR(50)  NOT NULL,
    fk_field         VARCHAR(100) NULL,
    description      VARCHAR(500) NULL,
    created_at       DATETIME2    NOT NULL DEFAULT GETUTCDATE()
  );
GO

-- 4. Backfill business_location entity_config
UPDATE mdm_config.entity_config SET
  bv_match_table='bv_location_match_candidates',
  bv_resolution_table='bv_location_key_resolution',
  gold_table='dim_location',
  pit_table='pit_location',
  match_engine='jaro_winkler',
  display_label_pl='Lokalizacje biznesowe',
  display_label_en='Business Locations',
  icon='MapPin',
  display_order=10
WHERE entity_id='business_location';
GO

-- 5. Backfill business_location field_config display metadata
UPDATE mdm_config.field_config SET display_name_pl='Nazwa',         display_name_en='Name',      display_order=10, ui_widget='text',   is_required=1, is_overridable=1, is_golden_field=1, group_name='Podstawowe'  WHERE entity_id='business_location' AND field_name='name';
UPDATE mdm_config.field_config SET display_name_pl='Miasto',        display_name_en='City',      display_order=20, ui_widget='text',   is_required=1, is_overridable=1, is_golden_field=1, group_name='Adres'       WHERE entity_id='business_location' AND field_name='city';
UPDATE mdm_config.field_config SET display_name_pl='Kraj',          display_name_en='Country',   display_order=30, ui_widget='select', is_required=1, is_overridable=1, is_golden_field=1, group_name='Adres'       WHERE entity_id='business_location' AND field_name='country';
UPDATE mdm_config.field_config SET display_name_pl='Kod pocztowy',  display_name_en='Zip Code',  display_order=40, ui_widget='text',   is_required=0, is_overridable=1, is_golden_field=1, group_name='Adres'       WHERE entity_id='business_location' AND field_name='zip_code';
UPDATE mdm_config.field_config SET display_name_pl='Szerokość geo', display_name_en='Latitude',  display_order=50, ui_widget='number', is_required=0, is_overridable=1, is_golden_field=1, group_name='Geolocation' WHERE entity_id='business_location' AND field_name='latitude';
UPDATE mdm_config.field_config SET display_name_pl='Długość geo',   display_name_en='Longitude', display_order=60, ui_widget='number', is_required=0, is_overridable=1, is_golden_field=1, group_name='Geolocation' WHERE entity_id='business_location' AND field_name='longitude';
GO

-- 6. Seed legal_entity entity_config
DELETE FROM mdm_config.entity_config WHERE entity_id = 'legal_entity';
INSERT INTO mdm_config.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold,
   bv_match_table, bv_resolution_table, gold_table, pit_table,
   match_engine, display_label_pl, display_label_en, icon, display_order)
VALUES
  ('legal_entity', 'Legal Entity (Spolka)', 'hub_legal_entity', 1, 0.0, 0.0,
   NULL, NULL, 'dim_legal_entity', NULL,
   'none', 'Spolki', 'Legal Entities', 'Building2', 20);
GO

-- 7. Seed legal_entity field_config
DELETE FROM mdm_config.field_config WHERE entity_id = 'legal_entity';
INSERT INTO mdm_config.field_config
  (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active,
   display_name_pl, display_name_en, display_order, ui_widget, is_overridable,
   is_required, validators_json, group_name, is_golden_field)
VALUES
  ('legal_entity', 'legal_entity_code', 0.0, 0, NULL,                      1, 'Kod spolki',           'Entity Code',          10, 'text',    0, 1, '[{"type":"maxLength","value":50}]',                                    'Identyfikacja', 1),
  ('legal_entity', 'name',              0.0, 0, 'uppercase_strip_accents',  1, 'Nazwa spolki',         'Company Name',          20, 'text',    1, 1, '[{"type":"maxLength","value":255}]',                                   'Identyfikacja', 1),
  ('legal_entity', 'tax_id',            0.0, 0, 'normalize_tax_id',         1, 'NIP / VAT-EU',         'Tax ID',                30, 'text',    1, 0, '[{"type":"maxLength","value":30}]',                                    'Identyfikacja', 1),
  ('legal_entity', 'country',           0.0, 0, 'iso2_country_code',        1, 'Kraj',                 'Country',               40, 'select',  1, 1, NULL,                                                                   'Adres',          1),
  ('legal_entity', 'currency_code',     0.0, 0, NULL,                       1, 'Waluta',               'Currency',              50, 'select',  1, 1, NULL,                                                                   'Finansowe',      1),
  ('legal_entity', 'parent_entity_code',0.0, 0, NULL,                       1, 'Spolka nadrzedna',     'Parent Entity',         60, 'select',  1, 0, NULL,                                                                   'Hierarchia',     1),
  ('legal_entity', 'consolidation_method',0.0,0, NULL,                      1, 'Metoda konsolidacji',  'Consolidation Method',  70, 'select',  1, 1, '[{"type":"enum","value":["full","equity","proportional","none"]}]',     'Finansowe',      1),
  ('legal_entity', 'ownership_pct',     0.0, 0, NULL,                       1, 'Udzial wlasnosciowy %','Ownership %',           80, 'number',  1, 1, '[{"type":"min","value":0},{"type":"max","value":100}]',                'Finansowe',      1),
  ('legal_entity', 'valid_from',        0.0, 0, NULL,                       1, 'Wazne od',             'Valid From',            90, 'date',    1, 0, NULL,                                                                   'Okres',          1),
  ('legal_entity', 'valid_to',          0.0, 0, NULL,                       1, 'Wazne do',             'Valid To',              100,'date',    1, 0, NULL,                                                                   'Okres',          1),
  ('legal_entity', 'is_active',         0.0, 0, NULL,                       1, 'Aktywna',              'Active',                110,'boolean', 1, 1, NULL,                                                                   'Status',         1);
GO

-- 8. Seed legal_entity source_priority
DELETE FROM mdm_config.source_priority WHERE entity_id = 'legal_entity';
INSERT INTO mdm_config.source_priority (entity_id, source_system, field_name, priority)
VALUES ('legal_entity', 'manual', '*', 1);
GO

-- 9. Seed legal_entity hash_config
DELETE FROM mdm_config.hash_config WHERE entity_id = 'legal_entity';
INSERT INTO mdm_config.hash_config (entity_id, source_system, source_id_column, business_key_template)
VALUES ('legal_entity', 'manual', 'legal_entity_code', 'manual|{legal_entity_code}');
GO

-- 10. Seed legal_entity watermark
DELETE FROM mdm_config.source_watermark WHERE entity_id = 'legal_entity';
INSERT INTO mdm_config.source_watermark (entity_id, source_system, last_load_date)
VALUES ('legal_entity', 'manual', '1900-01-01T00:00:00');
GO

-- 11. Entity relationships
DELETE FROM mdm_config.entity_relationship WHERE parent_entity_id = 'legal_entity';
INSERT INTO mdm_config.entity_relationship (parent_entity_id, child_entity_id, relationship, fk_field, description)
VALUES
  ('legal_entity', 'chart_of_accounts', 'parent_child', 'legal_entity_code', 'Each CoA belongs to a legal entity'),
  ('legal_entity', 'counterparty',      'reference',    'linked_legal_entity_code', 'Internal counterparties link to legal entities');
GO

-- 12. Legal entity gold + quality tables
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='gold' AND t.name='dim_legal_entity')
  CREATE TABLE gold.dim_legal_entity (
    legal_entity_hk        VARCHAR(64)  NOT NULL,
    legal_entity_code      VARCHAR(50)  NOT NULL,
    name                   VARCHAR(255) NULL,
    tax_id                 VARCHAR(30)  NULL,
    country                VARCHAR(10)  NULL,
    currency_code          VARCHAR(10)  NULL,
    parent_entity_code     VARCHAR(50)  NULL,
    consolidation_method   VARCHAR(20)  NULL,
    ownership_pct          DECIMAL(5,2) NULL,
    valid_from             DATE         NULL,
    valid_to               DATE         NULL,
    is_active              BIT          NULL,
    is_current             BIT          NOT NULL DEFAULT 1,
    effective_from         DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    effective_to           DATETIME2    NULL,
    source_system          VARCHAR(50)  NULL,
    created_at             DATETIME2    NOT NULL DEFAULT GETUTCDATE()
  );
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='gold' AND t.name='dim_legal_entity_quality')
  CREATE TABLE gold.dim_legal_entity_quality (
    legal_entity_hk   VARCHAR(64) NOT NULL,
    source_system      VARCHAR(50) NOT NULL,
    completeness_score DECIMAL(5,4) NULL,
    calculated_at      DATETIME2   NOT NULL DEFAULT GETUTCDATE()
  );
GO
