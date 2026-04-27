-- =============================================================================
-- Seed: konfiguracja MDM dla encji Business Location
-- Uruchom po create_mdm_config_tables.sql
-- =============================================================================

-- Encja: business_location
INSERT INTO {{SCHEMA_CONFIG}}.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold)
VALUES
  ('business_location', 'Business Location (Restauracja)', 'hub_location', true, 0.85, 0.97);

-- Konfiguracja pól do matchingu
INSERT INTO {{SCHEMA_CONFIG}}.field_config
  (entity_id, field_name, match_weight, is_blocking_key, standardizer)
VALUES
  ('business_location', 'name',     0.50, false, 'uppercase_strip_accents'),
  ('business_location', 'city',     0.00, true,  'uppercase'),         -- blocking key
  ('business_location', 'country',  0.00, true,  'iso2_country_code'), -- blocking key
  ('business_location', 'zip_code', 0.30, false, 'strip_whitespace'),
  ('business_location', 'latitude', 0.20, false, 'round_3_decimals'),  -- geo score
  ('business_location', 'longitude',0.00, false, 'round_3_decimals');  -- powiązane z lat

-- Priorytety źródeł (survivorship: Lightspeed > McWin > Yext > GoPOS)
-- '*' = wszystkie pola, chyba że jest nadpisane per pole
INSERT INTO {{SCHEMA_CONFIG}}.source_priority
  (entity_id, source_system, field_name, priority)
VALUES
  ('business_location', 'lightspeed', '*',          1),
  ('business_location', 'mcwin',      '*',          2),
  ('business_location', 'yext',       '*',          3),
  ('business_location', 'gopos',      '*',          4),
  -- Yext ma lepsze dane dla ratingu i lokalizacji geo
  ('business_location', 'yext',       'avg_rating', 1),
  ('business_location', 'yext',       'review_count',1),
  ('business_location', 'yext',       'latitude',   1),
  ('business_location', 'yext',       'longitude',  1),
  ('business_location', 'yext',       'website_url',1),
  -- McWin ma lepsze dane finansowe
  ('business_location', 'mcwin',      'cost_center',1),
  ('business_location', 'mcwin',      'region',     1);

-- Hash config: jak budować business_key per źródło
INSERT INTO {{SCHEMA_CONFIG}}.hash_config
  (entity_id, source_system, source_id_column, business_key_template)
VALUES
  ('business_location', 'lightspeed', 'blId',          'lightspeed|{blId}'),
  ('business_location', 'yext',       'id',             'yext|{id}'),
  ('business_location', 'mcwin',      'restaurant_id',  'mcwin|{restaurant_id}'),
  ('business_location', 'gopos',      'location_id',    'gopos|{location_id}');

-- Watermark: start od zera (full load first run)
INSERT INTO {{SCHEMA_CONFIG}}.source_watermark
  (entity_id, source_system, last_load_date)
VALUES
  ('business_location', 'lightspeed', '1900-01-01T00:00:00'),
  ('business_location', 'yext',       '1900-01-01T00:00:00'),
  ('business_location', 'mcwin',      '1900-01-01T00:00:00'),
  ('business_location', 'gopos',      '1900-01-01T00:00:00');
