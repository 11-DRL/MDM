-- =============================================================================
-- Copy seed data Lakehouse (lh_mdm SQL endpoint) -> Warehouse (wh_mdm)
-- Same workspace -> cross-DB INSERT SELECT.
-- Idempotent: TRUNCATE target first.
-- =============================================================================

-- silver_dv
TRUNCATE TABLE silver_dv.hub_location;
INSERT INTO silver_dv.hub_location (location_hk, business_key, load_date, record_source)
SELECT location_hk, business_key, load_date, record_source FROM lh_mdm.silver_dv.hub_location;

TRUNCATE TABLE silver_dv.sat_location_lightspeed;
INSERT INTO silver_dv.sat_location_lightspeed
  (location_hk, load_date, load_end_date, hash_diff, record_source, name, country, city, timezone, currency_code, bl_id, is_active, name_std, country_std, city_std)
SELECT location_hk, load_date, load_end_date, hash_diff, record_source, name, country, city, timezone, currency_code, bl_id, is_active, name_std, country_std, city_std
FROM lh_mdm.silver_dv.sat_location_lightspeed;

TRUNCATE TABLE silver_dv.sat_location_yext;
INSERT INTO silver_dv.sat_location_yext
  (location_hk, load_date, load_end_date, hash_diff, record_source, name, address_line1, city, postal_code, country_code, phone, website_url, latitude, longitude, avg_rating, review_count, name_std, country_std, city_std)
SELECT location_hk, load_date, load_end_date, hash_diff, record_source, name, address_line1, city, postal_code, country_code, phone, website_url, latitude, longitude, avg_rating, review_count, name_std, country_std, city_std
FROM lh_mdm.silver_dv.sat_location_yext;

TRUNCATE TABLE silver_dv.sat_location_mcwin;
INSERT INTO silver_dv.sat_location_mcwin
  (location_hk, load_date, load_end_date, hash_diff, record_source, restaurant_name, cost_center, region, country, city, zip_code, address, is_active, name_std, country_std, city_std)
SELECT location_hk, load_date, load_end_date, hash_diff, record_source, restaurant_name, cost_center, region, country, city, zip_code, address, is_active, name_std, country_std, city_std
FROM lh_mdm.silver_dv.sat_location_mcwin;

TRUNCATE TABLE silver_dv.sat_location_gopos;
INSERT INTO silver_dv.sat_location_gopos
  (location_hk, load_date, load_end_date, hash_diff, record_source, location_name, address, city, zip_code, country, phone, is_active, name_std, country_std, city_std)
SELECT location_hk, load_date, load_end_date, hash_diff, record_source, location_name, address, city, zip_code, country, phone, is_active, name_std, country_std, city_std
FROM lh_mdm.silver_dv.sat_location_gopos;

TRUNCATE TABLE silver_dv.pit_location;
INSERT INTO silver_dv.pit_location (location_hk, snapshot_date, sat_lightspeed_ld, sat_yext_ld, sat_mcwin_ld, sat_gopos_ld)
SELECT location_hk, snapshot_date, sat_lightspeed_ld, sat_yext_ld, sat_mcwin_ld, sat_gopos_ld FROM lh_mdm.silver_dv.pit_location;

TRUNCATE TABLE silver_dv.bv_location_match_candidates;
INSERT INTO silver_dv.bv_location_match_candidates
  (pair_id, hk_left, hk_right, match_score, match_type, name_score, city_match, zip_match, geo_score, status, created_at, reviewed_by, reviewed_at, review_note, run_id)
SELECT pair_id, hk_left, hk_right, match_score, match_type, name_score, city_match, zip_match, geo_score, status, created_at, reviewed_by, reviewed_at, review_note, run_id
FROM lh_mdm.silver_dv.bv_location_match_candidates;

TRUNCATE TABLE silver_dv.bv_location_key_resolution;
INSERT INTO silver_dv.bv_location_key_resolution
  (source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type)
SELECT source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type
FROM lh_mdm.silver_dv.bv_location_key_resolution;

TRUNCATE TABLE silver_dv.stewardship_log;
INSERT INTO silver_dv.stewardship_log
  (log_id, canonical_hk, action, field_name, old_value, new_value, changed_by, changed_at, pair_id, reason)
SELECT log_id, canonical_hk, action, field_name, old_value, new_value, changed_by, changed_at, pair_id, reason
FROM lh_mdm.silver_dv.stewardship_log;

-- gold
TRUNCATE TABLE gold.dim_location;
INSERT INTO gold.dim_location
  (location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at, lightspeed_bl_id, yext_id, mcwin_restaurant_id, gopos_location_id)
SELECT location_sk, location_hk, valid_from, valid_to, is_current, name, country, city, zip_code, address, phone, latitude, longitude, website_url, timezone, currency_code, avg_rating, review_count, cost_center, region, name_source, country_source, city_source, created_at, updated_at, lightspeed_bl_id, yext_id, mcwin_restaurant_id, gopos_location_id
FROM lh_mdm.gold.dim_location;

TRUNCATE TABLE gold.dim_location_quality;
INSERT INTO gold.dim_location_quality
  (location_hk, snapshot_date, sources_count, completeness_score, has_lightspeed, has_yext, has_mcwin, has_gopos, last_match_score)
SELECT location_hk, snapshot_date, sources_count, completeness_score, has_lightspeed, has_yext, has_mcwin, has_gopos, last_match_score
FROM lh_mdm.gold.dim_location_quality;
