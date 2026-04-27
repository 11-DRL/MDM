// Centralna konfiguracja nazw schematów dla MDM.
//
// Pozwala instalować rozwiązanie do istniejącego Warehouse, gdzie nazwy `silver_dv`,
// `gold`, `mdm_config`, `bronze` mogą już być zajęte. Konfigurowane przez Function App
// settings (MDM_SCHEMA_*); domyślne wartości zachowują wsteczną kompatybilność z obecnym
// deploy'em — żaden istniejący env nie wymaga zmiany.
//
// Sposób użycia w SQL stringach:
//   import { S } from '../lib/schemas';
//   await execSql(`SELECT * FROM ${S.silver}.bv_location_match_candidates WHERE ...`);

export const S = {
  bronze: process.env.MDM_SCHEMA_BRONZE ?? 'bronze',
  silver: process.env.MDM_SCHEMA_SILVER ?? 'silver_dv',
  gold:   process.env.MDM_SCHEMA_GOLD   ?? 'gold',
  config: process.env.MDM_SCHEMA_CONFIG ?? 'mdm_config',
} as const;

export type SchemaMap = typeof S;
