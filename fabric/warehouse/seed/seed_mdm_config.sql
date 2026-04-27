-- =============================================================================
-- Fabric Warehouse: wh_mdm — MDM Config seed (entity: business_location)
-- Idempotent: DELETE-then-INSERT per entity.
-- =============================================================================

-- entity_config
DELETE FROM {{SCHEMA_CONFIG}}.entity_config WHERE entity_id = 'business_location';
INSERT INTO {{SCHEMA_CONFIG}}.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold, created_at)
VALUES
  ('business_location', 'Business Location', 'hub_location', 1, 0.85, 0.97, SYSUTCDATETIME());

-- field_config
DELETE FROM {{SCHEMA_CONFIG}}.field_config WHERE entity_id = 'business_location';
INSERT INTO {{SCHEMA_CONFIG}}.field_config
  (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active)
VALUES
  ('business_location', 'name',      0.50, 0, 'strip_accents_upper', 1),
  ('business_location', 'city',      0.00, 1, 'strip_accents_upper', 1),
  ('business_location', 'country',   0.00, 1, 'iso2_upper',          1),
  ('business_location', 'zip_code',  0.30, 0, 'strip_spaces',        1),
  ('business_location', 'phone',     0.00, 0, 'e164_normalize',      1),
  ('business_location', 'address',   0.00, 0, 'strip_accents_upper', 1),
  ('business_location', 'latitude',  0.20, 0, NULL,                  1),
  ('business_location', 'longitude', 0.20, 0, NULL,                  1);

-- source_priority
DELETE FROM {{SCHEMA_CONFIG}}.source_priority WHERE entity_id = 'business_location';
INSERT INTO {{SCHEMA_CONFIG}}.source_priority
  (entity_id, source_system, field_name, priority, created_at)
VALUES
  ('business_location', 'lightspeed', '*',           1, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      '*',           2, SYSUTCDATETIME()),
  ('business_location', 'yext',       '*',           3, SYSUTCDATETIME()),
  ('business_location', 'gopos',      '*',           4, SYSUTCDATETIME()),
  ('business_location', 'yext',       'latitude',    1, SYSUTCDATETIME()),
  ('business_location', 'yext',       'longitude',   1, SYSUTCDATETIME()),
  ('business_location', 'yext',       'avg_rating',  1, SYSUTCDATETIME()),
  ('business_location', 'yext',       'review_count',1, SYSUTCDATETIME()),
  ('business_location', 'yext',       'website_url', 1, SYSUTCDATETIME()),
  ('business_location', 'yext',       'phone',       1, SYSUTCDATETIME()),
  ('business_location', 'gopos',      'phone',       2, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      'zip_code',    1, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      'address',     1, SYSUTCDATETIME()),
  ('business_location', 'gopos',      'address',     2, SYSUTCDATETIME()),
  ('business_location', 'yext',       'address',     3, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      'cost_center', 1, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      'region',      1, SYSUTCDATETIME());

-- hash_config
DELETE FROM {{SCHEMA_CONFIG}}.hash_config WHERE entity_id = 'business_location';
INSERT INTO {{SCHEMA_CONFIG}}.hash_config
  (entity_id, source_system, source_id_column, business_key_template)
VALUES
  ('business_location', 'lightspeed', 'blId',          'lightspeed|{blId}'),
  ('business_location', 'yext',       'id',            'yext|{id}'),
  ('business_location', 'mcwin',      'restaurant_id', 'mcwin|{restaurant_id}'),
  ('business_location', 'gopos',      'location_id',   'gopos|{location_id}');

-- source_watermark — initial epoch-start watermarks
DELETE FROM {{SCHEMA_CONFIG}}.source_watermark WHERE entity_id = 'business_location';
INSERT INTO {{SCHEMA_CONFIG}}.source_watermark
  (entity_id, source_system, last_load_date, last_run_id, updated_at)
VALUES
  ('business_location', 'lightspeed', '1900-01-01', NULL, SYSUTCDATETIME()),
  ('business_location', 'yext',       '1900-01-01', NULL, SYSUTCDATETIME()),
  ('business_location', 'mcwin',      '1900-01-01', NULL, SYSUTCDATETIME()),
  ('business_location', 'gopos',      '1900-01-01', NULL, SYSUTCDATETIME());
