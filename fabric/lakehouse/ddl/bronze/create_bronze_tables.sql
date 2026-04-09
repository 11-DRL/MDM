-- =============================================================================
-- Bronze Layer — Raw Landing Zone
-- Lakehouse: lh_mdm | Schema: bronze
-- Append-only Delta tables, dane 1:1 ze źródeł
-- =============================================================================

-- Lightspeed: GET /f/data/businesses
CREATE TABLE IF NOT EXISTS bronze.lightspeed_businesses (
  -- Lightspeed native fields
  businessId            BIGINT,
  businessName          STRING,
  currencyCode          STRING,
  -- businessLocations (exploded)
  blId                  BIGINT,
  blName                STRING,
  country               STRING,
  timezone              STRING,
  -- Ingestion metadata
  _source_system        STRING  NOT NULL DEFAULT 'lightspeed',
  _load_date            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  _run_id               STRING  NOT NULL,
  _tenant_name          STRING  NOT NULL   -- __paramTenantName z pipeline
) USING DELTA;

-- Yext: Locations API
CREATE TABLE IF NOT EXISTS bronze.yext_locations (
  -- Yext native fields
  id                    STRING,
  name                  STRING,
  address_line1         STRING,
  address_city          STRING,
  address_postal_code   STRING,
  address_country_code  STRING,
  phone                 STRING,
  website_url           STRING,
  display_lat           DOUBLE,
  display_lng           DOUBLE,
  avg_rating            DOUBLE,
  review_count          INT,
  -- Ingestion metadata
  _source_system        STRING  NOT NULL DEFAULT 'yext',
  _load_date            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  _run_id               STRING  NOT NULL,
  _tenant_name          STRING  NOT NULL
) USING DELTA;

-- McWin: Restaurant Masterdata export (SFTP/ADLS file)
CREATE TABLE IF NOT EXISTS bronze.mcwin_restaurant_masterdata (
  -- McWin native fields (CSV/Excel export)
  restaurant_id         STRING,
  restaurant_name       STRING,
  cost_center           STRING,
  region                STRING,
  country               STRING,
  city                  STRING,
  zip_code              STRING,
  address               STRING,
  is_active             STRING,
  -- Ingestion metadata
  _source_system        STRING  NOT NULL DEFAULT 'mcwin',
  _load_date            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  _run_id               STRING  NOT NULL,
  _file_name            STRING,
  _tenant_name          STRING  NOT NULL
) USING DELTA;

-- GoPOS: Locations API
CREATE TABLE IF NOT EXISTS bronze.gopos_locations (
  -- GoPOS native fields
  location_id           STRING,
  location_name         STRING,
  address               STRING,
  city                  STRING,
  zip_code              STRING,
  country               STRING,
  phone                 STRING,
  is_active             BOOLEAN,
  -- Ingestion metadata
  _source_system        STRING  NOT NULL DEFAULT 'gopos',
  _load_date            TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  _run_id               STRING  NOT NULL,
  _tenant_name          STRING  NOT NULL
) USING DELTA;
