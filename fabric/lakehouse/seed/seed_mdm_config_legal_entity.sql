-- =============================================================================
-- Seed: MDM configuration for entity Legal Entity
-- Run after alter_mdm_config_multi_entity.sql
-- =============================================================================

-- Entity definition
INSERT INTO {{SCHEMA_CONFIG}}.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold,
   bv_match_table, bv_resolution_table, gold_table, pit_table,
   match_engine, display_label_pl, display_label_en, icon, display_order)
VALUES
  ('legal_entity', 'Legal Entity (Spółka)', 'hub_legal_entity', true, 0.0, 0.0,
   NULL, NULL, 'dim_legal_entity', NULL,
   'none', 'Spółki', 'Legal Entities', 'Building2', 20);

-- Field config (all fields, with UI metadata)
INSERT INTO {{SCHEMA_CONFIG}}.field_config
  (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active,
   display_name_pl, display_name_en, display_order, ui_widget, is_overridable,
   is_required, validators_json, group_name, is_golden_field)
VALUES
  ('legal_entity', 'legal_entity_code', 0.0, false, NULL,                     true,  'Kod spółki',          'Entity Code',          10,  'text',    false, true,  '[{"type":"maxLength","value":50},{"type":"pattern","value":"^[A-Z0-9\\\\-]+$"}]', 'Identyfikacja', true),
  ('legal_entity', 'name',              0.0, false, 'uppercase_strip_accents', true,  'Nazwa spółki',        'Company Name',          20,  'text',    true,  true,  '[{"type":"maxLength","value":255}]',                                              'Identyfikacja', true),
  ('legal_entity', 'tax_id',            0.0, false, 'normalize_tax_id',        true,  'NIP / VAT-EU',        'Tax ID',                30,  'text',    true,  false, '[{"type":"maxLength","value":30}]',                                               'Identyfikacja', true),
  ('legal_entity', 'country',           0.0, false, 'iso2_country_code',       true,  'Kraj',                'Country',               40,  'select',  true,  true,  NULL,                                                                              'Adres',          true),
  ('legal_entity', 'currency_code',     0.0, false, NULL,                      true,  'Waluta',              'Currency',              50,  'select',  true,  true,  NULL,                                                                              'Finansowe',      true),
  ('legal_entity', 'parent_entity_code',0.0, false, NULL,                      true,  'Spółka nadrzędna',    'Parent Entity',         60,  'select',  true,  false, NULL,                                                                              'Hierarchia',     true),
  ('legal_entity', 'consolidation_method', 0.0, false, NULL,                   true,  'Metoda konsolidacji', 'Consolidation Method',  70,  'select',  true,  true,  '[{"type":"enum","value":["full","equity","proportional","none"]}]',                'Finansowe',      true),
  ('legal_entity', 'ownership_pct',     0.0, false, NULL,                      true,  'Udział własnościowy %','Ownership %',           80,  'number',  true,  true,  '[{"type":"min","value":0},{"type":"max","value":100}]',                           'Finansowe',      true),
  ('legal_entity', 'valid_from',        0.0, false, NULL,                      true,  'Ważne od',            'Valid From',            90,  'date',    true,  false, NULL,                                                                              'Okres',          true),
  ('legal_entity', 'valid_to',          0.0, false, NULL,                      true,  'Ważne do',            'Valid To',              100, 'date',    true,  false, NULL,                                                                              'Okres',          true),
  ('legal_entity', 'is_active',         0.0, false, NULL,                      true,  'Aktywna',             'Active',                110, 'boolean', true,  true,  NULL,                                                                              'Status',         true);

-- Source priority (only manual source for now)
INSERT INTO {{SCHEMA_CONFIG}}.source_priority
  (entity_id, source_system, field_name, priority)
VALUES
  ('legal_entity', 'manual', '*', 1);

-- Hash config
INSERT INTO {{SCHEMA_CONFIG}}.hash_config
  (entity_id, source_system, source_id_column, business_key_template)
VALUES
  ('legal_entity', 'manual', 'legal_entity_code', 'manual|{legal_entity_code}');

-- Watermark
INSERT INTO {{SCHEMA_CONFIG}}.source_watermark
  (entity_id, source_system, last_load_date)
VALUES
  ('legal_entity', 'manual', '1900-01-01T00:00:00');

-- Entity relationships (legal_entity is parent for future chart_of_accounts, counterparty)
INSERT INTO {{SCHEMA_CONFIG}}.entity_relationship
  (parent_entity_id, child_entity_id, relationship, fk_field, description)
VALUES
  ('legal_entity', 'chart_of_accounts', 'parent_child', 'legal_entity_code', 'Each CoA belongs to a legal entity'),
  ('legal_entity', 'counterparty',      'reference',    'linked_legal_entity_code', 'Internal counterparties link to legal entities');
