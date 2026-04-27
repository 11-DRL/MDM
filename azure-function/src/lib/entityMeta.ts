// Entity metadata service — loads entity config from mdm_config tables and caches it.
// Used by v2 generic API routes to build SQL dynamically.

import { S } from './schemas';

export interface EntityMeta {
  entityId: string;
  entityName: string;
  hubTable: string;
  hkColumn: string;           // e.g. 'location_hk', 'legal_entity_hk'
  bvMatchTable: string | null;
  bvResolutionTable: string | null;
  goldTable: string;
  qualityTable: string;
  pitTable: string | null;
  matchEngine: string;        // 'jaro_winkler' | 'exact' | 'none'
  matchThreshold: number;
  autoAcceptThreshold: number;
  displayLabelPl: string;
  displayLabelEn: string;
  icon: string;
  displayOrder: number;
}

export interface FieldMeta {
  fieldName: string;
  displayNamePl: string;
  displayNameEn: string;
  displayOrder: number;
  uiWidget: string;
  isOverridable: boolean;
  isRequired: boolean;
  validatorsJson: string | null;
  lookupEntity: string | null;
  lookupField: string | null;
  isGoldenField: boolean;
  groupName: string | null;
  matchWeight: number;
  isBlockingKey: boolean;
  standardizer: string | null;
}

export interface SourceMeta {
  sourceSystem: string;
  satTable: string;           // full: 'silver_dv.sat_legal_entity_manual'
  alias: string;              // 's0', 's1', ...
}

// In-memory cache with TTL
const META_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

const entityCache = new Map<string, CacheEntry<EntityMeta>>();
const fieldCache = new Map<string, CacheEntry<FieldMeta[]>>();
const sourceCache = new Map<string, CacheEntry<SourceMeta[]>>();

function isFresh<T>(entry: CacheEntry<T> | undefined): entry is CacheEntry<T> {
  return !!entry && Date.now() < entry.expiresAt;
}

// Type for the querySql function passed in (avoid circular dependency on mdmWrite)
type QueryFn = <T = Record<string, unknown>>(sql: string, params?: Record<string, unknown>) => Promise<T[]>;

export async function getEntityMeta(entityId: string, querySql: QueryFn): Promise<EntityMeta | null> {
  const cached = entityCache.get(entityId);
  if (isFresh(cached)) return cached.data;

  const rows = await querySql<Record<string, unknown>>(`
    SELECT
      entity_id, entity_name, hub_table, is_active,
      CAST(match_threshold AS FLOAT) AS match_threshold,
      CAST(auto_accept_threshold AS FLOAT) AS auto_accept_threshold,
      bv_match_table, bv_resolution_table, gold_table, pit_table,
      match_engine, display_label_pl, display_label_en, icon, display_order
    FROM ${S.config}.entity_config
    WHERE entity_id = @entityId AND is_active = 1
  `, { entityId });

  if (rows.length === 0) return null;
  const r = rows[0];

  const hubTable = String(r.hub_table ?? '');
  const goldTable = String(r.gold_table ?? '');
  const hkColumn = hubTable.replace(/^hub_/, '') + '_hk';

  const meta: EntityMeta = {
    entityId: String(r.entity_id),
    entityName: String(r.entity_name ?? ''),
    hubTable,
    hkColumn,
    bvMatchTable: r.bv_match_table ? String(r.bv_match_table) : null,
    bvResolutionTable: r.bv_resolution_table ? String(r.bv_resolution_table) : null,
    goldTable,
    qualityTable: goldTable + '_quality',
    pitTable: r.pit_table ? String(r.pit_table) : null,
    matchEngine: String(r.match_engine ?? 'none'),
    matchThreshold: Number(r.match_threshold ?? 0.85),
    autoAcceptThreshold: Number(r.auto_accept_threshold ?? 0.97),
    displayLabelPl: String(r.display_label_pl ?? ''),
    displayLabelEn: String(r.display_label_en ?? ''),
    icon: String(r.icon ?? 'Database'),
    displayOrder: Number(r.display_order ?? 100),
  };

  entityCache.set(entityId, { data: meta, expiresAt: Date.now() + META_CACHE_TTL });
  return meta;
}

export async function getFieldMetas(entityId: string, querySql: QueryFn): Promise<FieldMeta[]> {
  const cached = fieldCache.get(entityId);
  if (isFresh(cached)) return cached.data;

  const rows = await querySql<Record<string, unknown>>(`
    SELECT
      field_name, display_name_pl, display_name_en, display_order,
      ui_widget, is_overridable, is_required, validators_json,
      lookup_entity, lookup_field, is_golden_field, group_name,
      CAST(match_weight AS FLOAT) AS match_weight, is_blocking_key, standardizer
    FROM ${S.config}.field_config
    WHERE entity_id = @entityId AND is_active = 1
    ORDER BY display_order, field_name
  `, { entityId });

  const fields: FieldMeta[] = rows.map(r => ({
    fieldName: String(r.field_name ?? ''),
    displayNamePl: String(r.display_name_pl ?? r.field_name ?? ''),
    displayNameEn: String(r.display_name_en ?? r.field_name ?? ''),
    displayOrder: Number(r.display_order ?? 100),
    uiWidget: String(r.ui_widget ?? 'text'),
    isOverridable: r.is_overridable === true || r.is_overridable === 1,
    isRequired: r.is_required === true || r.is_required === 1,
    validatorsJson: r.validators_json ? String(r.validators_json) : null,
    lookupEntity: r.lookup_entity ? String(r.lookup_entity) : null,
    lookupField: r.lookup_field ? String(r.lookup_field) : null,
    isGoldenField: r.is_golden_field === true || r.is_golden_field === 1,
    groupName: r.group_name ? String(r.group_name) : null,
    matchWeight: Number(r.match_weight ?? 0),
    isBlockingKey: r.is_blocking_key === true || r.is_blocking_key === 1,
    standardizer: r.standardizer ? String(r.standardizer) : null,
  }));

  fieldCache.set(entityId, { data: fields, expiresAt: Date.now() + META_CACHE_TTL });
  return fields;
}

export async function getSourceMetas(entityId: string, querySql: QueryFn): Promise<SourceMeta[]> {
  const cached = sourceCache.get(entityId);
  if (isFresh(cached)) return cached.data;

  const rows = await querySql<Record<string, unknown>>(`
    SELECT source_system
    FROM ${S.config}.hash_config
    WHERE entity_id = @entityId
    ORDER BY source_system
  `, { entityId });

  const sources: SourceMeta[] = rows.map((r, idx) => ({
    sourceSystem: String(r.source_system ?? ''),
    satTable: `${S.silver}.sat_${entityId}_${r.source_system}`,
    alias: `s${idx}`,
  }));

  sourceCache.set(entityId, { data: sources, expiresAt: Date.now() + META_CACHE_TTL });
  return sources;
}

export async function getAllEntities(querySql: QueryFn): Promise<EntityMeta[]> {
  const rows = await querySql<Record<string, unknown>>(`
    SELECT
      entity_id, entity_name, hub_table, is_active,
      CAST(match_threshold AS FLOAT) AS match_threshold,
      CAST(auto_accept_threshold AS FLOAT) AS auto_accept_threshold,
      bv_match_table, bv_resolution_table, gold_table, pit_table,
      match_engine, display_label_pl, display_label_en, icon, display_order
    FROM ${S.config}.entity_config
    WHERE is_active = 1
    ORDER BY display_order, entity_id
  `);

  return rows.map(r => {
    const hubTable = String(r.hub_table ?? '');
    const goldTable = String(r.gold_table ?? '');
    return {
      entityId: String(r.entity_id),
      entityName: String(r.entity_name ?? ''),
      hubTable,
      hkColumn: hubTable.replace(/^hub_/, '') + '_hk',
      bvMatchTable: r.bv_match_table ? String(r.bv_match_table) : null,
      bvResolutionTable: r.bv_resolution_table ? String(r.bv_resolution_table) : null,
      goldTable,
      qualityTable: goldTable + '_quality',
      pitTable: r.pit_table ? String(r.pit_table) : null,
      matchEngine: String(r.match_engine ?? 'none'),
      matchThreshold: Number(r.match_threshold ?? 0.85),
      autoAcceptThreshold: Number(r.auto_accept_threshold ?? 0.97),
      displayLabelPl: String(r.display_label_pl ?? ''),
      displayLabelEn: String(r.display_label_en ?? ''),
      icon: String(r.icon ?? 'Database'),
      displayOrder: Number(r.display_order ?? 100),
    };
  });
}

export function clearEntityCache(): void {
  entityCache.clear();
  fieldCache.clear();
  sourceCache.clear();
}
