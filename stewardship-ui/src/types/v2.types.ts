// V2 API types — generic, entity-aware types for multi-entity MDM.
// Keep legacy types in mdm.types.ts for backward compat.

// ---------- Entity schema (from /api/v2/entities/{entityId}/schema) ----------

export interface EntityInfo {
  entityId: string;
  entityName: string;
  displayLabelPl: string;
  displayLabelEn: string;
  icon: string;
  matchEngine: string;
  hasMatching: boolean;
  displayOrder?: number;
}

export interface FieldSchema {
  fieldName: string;
  displayNamePl: string;
  displayNameEn: string;
  displayOrder: number;
  uiWidget: 'text' | 'select' | 'date' | 'number' | 'boolean' | 'textarea';
  isOverridable: boolean;
  isRequired: boolean;
  validators: Array<{ type: string; value?: unknown }>;
  lookupEntity: string | null;
  lookupField: string | null;
  isGoldenField: boolean;
  groupName: string | null;
}

export interface EntitySchema {
  entity: EntityInfo;
  fields: FieldSchema[];
  sources: string[];
}

// ---------- Generic golden record ----------

export interface GenericGoldenRecord {
  hk: string;
  entityId?: string;
  attributes: Record<string, unknown>;
  validFrom?: string;
  validTo?: string;
  isCurrent: boolean;
}

export interface GenericGoldenPage {
  items: GenericGoldenRecord[];
  total: number;
  page: number;
  pageSize: number;
}

// ---------- Queue stats (same shape as v1, but entity-scoped) ----------

export interface QueueStats {
  pendingCount: number;
  autoAcceptedCount: number;
  acceptedCount: number;
  rejectedCount: number;
  totalGoldenRecords: number;
  avgCompletenessScore: number;
}

// ---------- Stewardship Log ----------

export interface LogEntry {
  logId: string;
  canonicalHk: string;
  action: string;
  fieldName?: string;
  oldValue?: string;
  newValue?: string;
  changedBy: string;
  changedAt: string;
  pairId?: string;
  reason?: string;
}
