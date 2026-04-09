// Typy domenowe dla MDM Stewardship UI
// Odpowiadają tabelom silver_dv i gold w Fabric Lakehouse

// ---------- Data Vault ----------

export interface HubLocation {
  locationHk: string;        // hex-encoded binary hash key
  businessKey: string;       // 'lightspeed|41839-1'
  loadDate: string;
  recordSource: MatchSource;
}

export type MatchSource = 'lightspeed' | 'yext' | 'mcwin' | 'gopos';

export interface LocationAttributes {
  name?: string;
  country?: string;
  city?: string;
  zipCode?: string;
  address?: string;
  phone?: string;
  latitude?: number;
  longitude?: number;
  websiteUrl?: string;
  timezone?: string;
  currencyCode?: string;
  avgRating?: number;
  reviewCount?: number;
  costCenter?: string;
  region?: string;
}

export interface SatelliteLocation extends LocationAttributes {
  locationHk: string;
  loadDate: string;
  loadEndDate?: string;
  recordSource: MatchSource;
  nameStd?: string;
  countryStd?: string;
  cityStd?: string;
}

// ---------- Business Vault: Match Candidates ----------

export type MatchStatus = 'pending' | 'accepted' | 'rejected' | 'auto_accepted';
export type MatchType = 'exact_name' | 'fuzzy_name_city' | 'geo_proximity' | 'composite_high' | 'composite';

export interface MatchCandidate {
  pairId: string;
  hkLeft: string;
  hkRight: string;
  matchScore: number;        // 0.0 - 1.0
  matchType: MatchType;
  nameScore?: number;
  cityMatch?: boolean;
  zipMatch?: boolean;
  geoScore?: number;
  status: MatchStatus;
  createdAt: string;
  reviewedBy?: string;
  reviewedAt?: string;
  reviewNote?: string;
  // Enriched from satellites (loaded by API)
  leftAttributes?: SatelliteLocation;
  rightAttributes?: SatelliteLocation;
}

export interface MatchCandidatePage {
  items: MatchCandidate[];
  total: number;
  page: number;
  pageSize: number;
}

// ---------- Gold ----------

export interface GoldenLocation extends LocationAttributes {
  locationHk: string;
  validFrom: string;
  validTo?: string;
  isCurrent: boolean;
  // Lineage
  nameSource?: MatchSource;
  countrySource?: MatchSource;
  citySource?: MatchSource;
  // Crosswalk
  lightspeedBlId?: number;
  yextId?: string;
  mcwinRestaurantId?: string;
  goposLocationId?: string;
  // Quality
  completenessScore?: number;
  sourcesCount?: number;
}

// ---------- Audit ----------

export type StewardshipAction =
  | 'accept_match'
  | 'reject_match'
  | 'override_field'
  | 'manual_create';

export interface StewardshipLogEntry {
  logId: string;
  canonicalHk: string;
  action: StewardshipAction;
  fieldName?: string;
  oldValue?: string;
  newValue?: string;
  changedBy: string;
  changedAt: string;
  pairId?: string;
  reason?: string;
}

// ---------- Config ----------

export interface EntityConfig {
  entityId: string;
  entityName: string;
  hubTable: string;
  isActive: boolean;
  matchThreshold: number;
  autoAcceptThreshold: number;
}

export interface SourcePriorityConfig {
  sourceSystem: MatchSource;
  fieldName: string;
  priority: number;
}

// ---------- API responses ----------

export interface ReviewQueueStats {
  pendingCount: number;
  autoAcceptedCount: number;
  acceptedCount: number;
  rejectedCount: number;
  totalGoldenRecords: number;
  avgCompletenessScore: number;
}

export interface PairReviewAction {
  pairId: string;
  action: 'accept' | 'reject';
  canonicalHk?: string;   // accept: który hk zostaje canonical
  reason?: string;
}
