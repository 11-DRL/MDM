# Fabric Notebook
# Name: nb_seed_mdm_config
# Description: Seeduje tabele konfiguracyjne MDM dla encji business_location (L'Osteria).
#              Uruchom po nb_bootstrap_ddl, przed nb_seed_demo_data.
#              Idempotentny: INSERT OR IGNORE / MERGE pattern.
# Triggerowany przez: deploy-fabric.ps1

# ---------------------------------------------------------------------------
# CELL 1: Setup
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from datetime import datetime, timezone

print("Seeding MDM config for entity: business_location...")

# ---------------------------------------------------------------------------
# CELL 2: entity_config
# ---------------------------------------------------------------------------
spark.sql("""
INSERT INTO mdm_config.entity_config
  (entity_id, entity_name, hub_table, is_active, match_threshold, auto_accept_threshold, created_at)
SELECT
  'business_location', 'Business Location', 'hub_location',
  true, 0.85, 0.97, current_timestamp()
WHERE NOT EXISTS (
  SELECT 1 FROM mdm_config.entity_config WHERE entity_id = 'business_location'
)
""")
print("OK: entity_config")

# ---------------------------------------------------------------------------
# CELL 3: field_config
# ---------------------------------------------------------------------------
spark.sql("DELETE FROM mdm_config.field_config WHERE entity_id = 'business_location'")

spark.sql("""
INSERT INTO mdm_config.field_config
  (entity_id, field_name, match_weight, is_blocking_key, standardizer, is_active)
VALUES
  ('business_location', 'name',         0.50, false, 'strip_accents_upper', true),
  ('business_location', 'city',         0.00, true,  'strip_accents_upper', true),
  ('business_location', 'country',      0.00, true,  'iso2_upper',          true),
  ('business_location', 'zip_code',     0.30, false, 'strip_spaces',        true),
  ('business_location', 'phone',        0.00, false, 'e164_normalize',      true),
  ('business_location', 'address',      0.00, false, 'strip_accents_upper', true),
  ('business_location', 'latitude',     0.20, false, null,                  true),
  ('business_location', 'longitude',    0.20, false, null,                  true)
""")
print("OK: field_config")

# ---------------------------------------------------------------------------
# CELL 4: source_priority — survivorship order
# ---------------------------------------------------------------------------
spark.sql("DELETE FROM mdm_config.source_priority WHERE entity_id = 'business_location'")

spark.sql("""
INSERT INTO mdm_config.source_priority
  (entity_id, source_system, field_name, priority, created_at)
VALUES
  -- Global order (field_name = '*')
  ('business_location', 'lightspeed', '*', 1, current_timestamp()),
  ('business_location', 'mcwin',      '*', 2, current_timestamp()),
  ('business_location', 'yext',       '*', 3, current_timestamp()),
  ('business_location', 'gopos',      '*', 4, current_timestamp()),
  -- Field-level overrides: Yext wins for geo + rating + website + address
  ('business_location', 'yext',       'latitude',    1, current_timestamp()),
  ('business_location', 'yext',       'longitude',   1, current_timestamp()),
  ('business_location', 'yext',       'avg_rating',  1, current_timestamp()),
  ('business_location', 'yext',       'review_count',1, current_timestamp()),
  ('business_location', 'yext',       'website_url', 1, current_timestamp()),
  ('business_location', 'yext',       'phone',       1, current_timestamp()),
  ('business_location', 'gopos',      'phone',       2, current_timestamp()),
  -- McWin wins for zip_code + address + cost_center + region
  ('business_location', 'mcwin',      'zip_code',    1, current_timestamp()),
  ('business_location', 'mcwin',      'address',     1, current_timestamp()),
  ('business_location', 'gopos',      'address',     2, current_timestamp()),
  ('business_location', 'yext',       'address',     3, current_timestamp()),
  ('business_location', 'mcwin',      'cost_center', 1, current_timestamp()),
  ('business_location', 'mcwin',      'region',      1, current_timestamp())
""")
print("OK: source_priority")

# ---------------------------------------------------------------------------
# CELL 5: hash_config
# ---------------------------------------------------------------------------
spark.sql("DELETE FROM mdm_config.hash_config WHERE entity_id = 'business_location'")

spark.sql("""
INSERT INTO mdm_config.hash_config
  (entity_id, source_system, source_id_column, business_key_template)
VALUES
  ('business_location', 'lightspeed', 'blId',          'lightspeed|{blId}'),
  ('business_location', 'yext',       'id',             'yext|{id}'),
  ('business_location', 'mcwin',      'restaurant_id',  'mcwin|{restaurant_id}'),
  ('business_location', 'gopos',      'location_id',    'gopos|{location_id}')
""")
print("OK: hash_config")

# ---------------------------------------------------------------------------
# CELL 6: source_watermark — inicjalizacja (epoch start)
# ---------------------------------------------------------------------------
spark.sql("""
INSERT INTO mdm_config.source_watermark
  (entity_id, source_system, last_load_date, last_run_id, updated_at)
SELECT s.entity_id, s.source_system, TIMESTAMP('1900-01-01'), null, current_timestamp()
FROM (VALUES
  ('business_location', 'lightspeed'),
  ('business_location', 'yext'),
  ('business_location', 'mcwin'),
  ('business_location', 'gopos')
) AS s(entity_id, source_system)
WHERE NOT EXISTS (
  SELECT 1 FROM mdm_config.source_watermark w
  WHERE w.entity_id = s.entity_id AND w.source_system = s.source_system
)
""")
print("OK: source_watermark")

# ---------------------------------------------------------------------------
# CELL 7: Podsumowanie
# ---------------------------------------------------------------------------
counts = {
    "entity_config":    spark.sql("SELECT COUNT(*) FROM mdm_config.entity_config").collect()[0][0],
    "field_config":     spark.sql("SELECT COUNT(*) FROM mdm_config.field_config").collect()[0][0],
    "source_priority":  spark.sql("SELECT COUNT(*) FROM mdm_config.source_priority").collect()[0][0],
    "hash_config":      spark.sql("SELECT COUNT(*) FROM mdm_config.hash_config").collect()[0][0],
    "source_watermark": spark.sql("SELECT COUNT(*) FROM mdm_config.source_watermark").collect()[0][0],
}
for tbl, cnt in counts.items():
    print(f"  mdm_config.{tbl}: {cnt} wierszy")

print("✅ MDM config seed zakończony!")
