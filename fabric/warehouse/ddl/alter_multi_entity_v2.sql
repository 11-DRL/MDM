-- Warehouse T-SQL: Extend mdm_config for multi-entity + seed legal_entity
-- Split on ; (apply-warehouse-ddl.js standard)

-- 1. entity_config — new columns (Fabric Warehouse doesn't support IF NOT EXISTS for ALTER, but ignores duplicate ADD)
ALTER TABLE mdm_config.entity_config ADD bv_match_table VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD bv_resolution_table VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD gold_table VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD pit_table VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD match_engine VARCHAR(50) NULL;
ALTER TABLE mdm_config.entity_config ADD display_label_pl VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD display_label_en VARCHAR(200) NULL;
ALTER TABLE mdm_config.entity_config ADD icon VARCHAR(50) NULL;
ALTER TABLE mdm_config.entity_config ADD display_order INT NULL;

-- 2. field_config — new columns
ALTER TABLE mdm_config.field_config ADD display_name_pl VARCHAR(200) NULL;
ALTER TABLE mdm_config.field_config ADD display_name_en VARCHAR(200) NULL;
ALTER TABLE mdm_config.field_config ADD display_order INT NULL;
ALTER TABLE mdm_config.field_config ADD ui_widget VARCHAR(50) NULL;
ALTER TABLE mdm_config.field_config ADD is_overridable BIT NULL;
ALTER TABLE mdm_config.field_config ADD is_required BIT NULL;
ALTER TABLE mdm_config.field_config ADD validators_json VARCHAR(2000) NULL;
ALTER TABLE mdm_config.field_config ADD lookup_entity VARCHAR(100) NULL;
ALTER TABLE mdm_config.field_config ADD lookup_field VARCHAR(100) NULL;
ALTER TABLE mdm_config.field_config ADD is_golden_field BIT NULL;
ALTER TABLE mdm_config.field_config ADD group_name VARCHAR(100) NULL;

-- 3. entity_relationship table
CREATE TABLE mdm_config.entity_relationship (
    parent_entity_id VARCHAR(100) NOT NULL,
    child_entity_id  VARCHAR(100) NOT NULL,
    relationship     VARCHAR(50)  NOT NULL,
    fk_field         VARCHAR(100) NULL,
    description      VARCHAR(500) NULL,
    created_at       DATETIME2    NOT NULL
);

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

-- 5. Backfill business_location field_config
UPDATE mdm_config.field_config SET display_name_pl='Nazwa', display_name_en='Name', display_order=10, ui_widget='text', is_required=1, is_overridable=1, is_golden_field=1, group_name='Podstawowe' WHERE entity_id='business_location' AND field_name='name';

UPDATE mdm_config.field_config SET display_name_pl='Miasto', display_name_en='City', display_order=20, ui_widget='text', is_required=1, is_overridable=1, is_golden_field=1, group_name='Adres' WHERE entity_id='business_location' AND field_name='city';

UPDATE mdm_config.field_config SET display_name_pl='Kraj', display_name_en='Country', display_order=30, ui_widget='select', is_required=1, is_overridable=1, is_golden_field=1, group_name='Adres' WHERE entity_id='business_location' AND field_name='country';

UPDATE mdm_config.field_config SET display_name_pl='Kod pocztowy', display_name_en='Zip Code', display_order=40, ui_widget='text', is_required=0, is_overridable=1, is_golden_field=1, group_name='Adres' WHERE entity_id='business_location' AND field_name='zip_code';

-- 6. Seed legal_entity entity_config
DELETE FROM mdm_config.entity_config WHERE entity_id = 'legal_entity';

INSERT INTO mdm_config.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold, created_at,
   bv_match_table, bv_resolution_table, gold_table, pit_table,
   match_engine, display_label_pl, display_label_en, icon, display_order)
VALUES
  ('legal_entity', 'Legal Entity', 'hub_legal_entity', 1, 0.0, 0.0, GETUTCDATE(),
   NULL, NULL, 'dim_legal_entity', NULL,
   'none', 'Spolki', 'Legal Entities', 'Building2', 20);

-- 7. Seed legal_entity field_config
DELETE FROM mdm_config.field_config WHERE entity_id = 'legal_entity';

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'legal_entity_code', 0.0, 0, NULL, 1, 'Kod spolki', 'Entity Code', 10, 'text', 0, 1, NULL, 'Identyfikacja', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'name', 0.0, 0, NULL, 1, 'Nazwa spolki', 'Company Name', 20, 'text', 1, 1, NULL, 'Identyfikacja', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'tax_id', 0.0, 0, NULL, 1, 'NIP / VAT-EU', 'Tax ID', 30, 'text', 1, 0, NULL, 'Identyfikacja', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'country', 0.0, 0, NULL, 1, 'Kraj', 'Country', 40, 'select', 1, 1, NULL, 'Adres', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'currency_code', 0.0, 0, NULL, 1, 'Waluta', 'Currency', 50, 'select', 1, 1, NULL, 'Finansowe', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'parent_entity_code', 0.0, 0, NULL, 1, 'Spolka nadrzedna', 'Parent Entity', 60, 'select', 1, 0, NULL, 'Hierarchia', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'consolidation_method', 0.0, 0, NULL, 1, 'Metoda konsolidacji', 'Consolidation Method', 70, 'select', 1, 1, NULL, 'Finansowe', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'ownership_pct', 0.0, 0, NULL, 1, 'Udzial wlasnosciowy', 'Ownership pct', 80, 'number', 1, 1, NULL, 'Finansowe', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'valid_from', 0.0, 0, NULL, 1, 'Wazne od', 'Valid From', 90, 'date', 1, 0, NULL, 'Okres', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'valid_to', 0.0, 0, NULL, 1, 'Wazne do', 'Valid To', 100, 'date', 1, 0, NULL, 'Okres', 1);

INSERT INTO mdm_config.field_config (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active, display_name_pl, display_name_en, display_order, ui_widget, is_overridable, is_required, validators_json, group_name, is_golden_field)
VALUES ('legal_entity', 'is_active', 0.0, 0, NULL, 1, 'Aktywna', 'Active', 110, 'boolean', 1, 1, NULL, 'Status', 1);

-- 8. Seed legal_entity source_priority
DELETE FROM mdm_config.source_priority WHERE entity_id = 'legal_entity';

INSERT INTO mdm_config.source_priority (entity_id, source_system, field_name, priority, created_at)
VALUES ('legal_entity', 'manual', '*', 1, GETUTCDATE());

-- 9. Seed legal_entity hash_config
DELETE FROM mdm_config.hash_config WHERE entity_id = 'legal_entity';

INSERT INTO mdm_config.hash_config (entity_id, source_system, source_id_column, business_key_template)
VALUES ('legal_entity', 'manual', 'legal_entity_code', 'manual|{legal_entity_code}');

-- 10. Seed legal_entity watermark
DELETE FROM mdm_config.source_watermark WHERE entity_id = 'legal_entity';

INSERT INTO mdm_config.source_watermark (entity_id, source_system, last_load_date, updated_at)
VALUES ('legal_entity', 'manual', '1900-01-01T00:00:00', GETUTCDATE());

-- 11. Gold tables for legal_entity
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
    is_current             BIT          NOT NULL,
    effective_from         DATETIME2    NOT NULL,
    effective_to           DATETIME2    NULL,
    source_system          VARCHAR(50)  NULL,
    created_at             DATETIME2    NOT NULL
);

CREATE TABLE gold.dim_legal_entity_quality (
    legal_entity_hk    VARCHAR(64)  NOT NULL,
    source_system      VARCHAR(50)  NOT NULL,
    completeness_score DECIMAL(5,4) NULL,
    calculated_at      DATETIME2    NOT NULL
);
