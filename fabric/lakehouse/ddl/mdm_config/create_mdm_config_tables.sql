-- =============================================================================
-- MDM Configuration Tables
-- Lakehouse: lh_mdm | Schema: mdm_config
-- Wzorzec: maintenance.tblDwhMigrationTable z adf-manage-dwh-gwc
-- =============================================================================

-- Definicje encji MDM (które encje są aktywne, jaki próg matchingu)
CREATE TABLE IF NOT EXISTS mdm_config.entity_config (
  entity_id        STRING  NOT NULL,   -- 'business_location', 'item', 'employee'
  entity_name      STRING  NOT NULL,
  hub_table        STRING  NOT NULL,   -- 'hub_location'
  is_active        BOOLEAN NOT NULL DEFAULT true,
  match_threshold  DOUBLE  NOT NULL DEFAULT 0.85,
  auto_accept_threshold DOUBLE NOT NULL DEFAULT 0.97,
  created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  updated_at       TIMESTAMP
) USING DELTA;

-- Konfiguracja pól encji: wagi do matchingu, standaryzacja
CREATE TABLE IF NOT EXISTS mdm_config.field_config (
  entity_id        STRING  NOT NULL,
  field_name       STRING  NOT NULL,   -- 'name', 'city', 'zip_code'
  match_weight     DOUBLE  NOT NULL DEFAULT 0.0,
  is_blocking_key  BOOLEAN NOT NULL DEFAULT false,
  standardizer     STRING,             -- 'uppercase', 'strip_accents', 'normalize_address'
  is_active        BOOLEAN NOT NULL DEFAULT true
) USING DELTA;

-- Priorytety źródeł do survivorship (niższy numer = wyższy priorytet)
CREATE TABLE IF NOT EXISTS mdm_config.source_priority (
  entity_id        STRING  NOT NULL,
  source_system    STRING  NOT NULL,   -- 'lightspeed', 'yext', 'mcwin', 'gopos'
  field_name       STRING  NOT NULL,   -- '*' = wszystkie pola
  priority         INT     NOT NULL,   -- 1 = najwyższy
  created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA;

-- Konfiguracja hashowania dla Hub keys
CREATE TABLE IF NOT EXISTS mdm_config.hash_config (
  entity_id        STRING  NOT NULL,
  source_system    STRING  NOT NULL,
  source_id_column STRING  NOT NULL,   -- kolumna z ID źródłowym
  business_key_template STRING NOT NULL -- np. 'lightspeed|{businessId}'
) USING DELTA;

-- Watermark na potrzeby incremental load (wzorzec: tblExtractionLog)
CREATE TABLE IF NOT EXISTS mdm_config.source_watermark (
  entity_id        STRING    NOT NULL,
  source_system    STRING    NOT NULL,
  last_load_date   TIMESTAMP NOT NULL DEFAULT '1900-01-01T00:00:00',
  last_run_id      STRING,
  updated_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA;

-- Log wykonań (wzorzec: logging.spInsertMainLogging)
CREATE TABLE IF NOT EXISTS mdm_config.execution_log (
  run_id           STRING    NOT NULL,
  entity_id        STRING    NOT NULL,
  source_system    STRING,
  process_name     STRING    NOT NULL,
  process_params   STRING,             -- JSON
  status           STRING    NOT NULL, -- 'Starting', 'Completed', 'Failed'
  records_loaded   BIGINT,
  records_matched  BIGINT,
  started_at       TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  completed_at     TIMESTAMP,
  error_message    STRING
) USING DELTA;
