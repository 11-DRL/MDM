// Mock data dla local dev — odpowiada realnym danym z Fabric Lakehouse
import type {
  MatchCandidatePage, ReviewQueueStats, GoldenLocation,
  StewardshipLogEntry, PairReviewAction, FieldConfig, SourcePriorityConfig
} from '../types/mdm.types';

// ─── Stats ───────────────────────────────────────────────────────────────────
export const mockStats: ReviewQueueStats = {
  pendingCount: 7,
  autoAcceptedCount: 142,
  acceptedCount: 38,
  rejectedCount: 11,
  totalGoldenRecords: 203,
  avgCompletenessScore: 0.86,
};

// ─── Match Candidates ────────────────────────────────────────────────────────
export const mockCandidates: MatchCandidatePage = {
  total: 7,
  page: 1,
  pageSize: 25,
  items: [
    {
      pairId: 'pair-001',
      hkLeft:  'a1b2c3d4e5f6a1b2',
      hkRight: 'b2c3d4e5f6a1b2c3',
      matchScore: 0.94,
      matchType: 'fuzzy_name_city',
      nameScore: 0.96,
      cityMatch: true,
      zipMatch: false,
      geoScore: 0.88,
      status: 'pending',
      createdAt: '2024-01-15T08:22:00Z',
      leftAttributes: {
        locationHk: 'a1b2c3d4e5f6a1b2',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria München Marienplatz",
        city: 'München',
        country: 'DE',
        zipCode: '80331',
        address: 'Marienplatz 8',
        phone: '+49 89 12345678',
        latitude: 48.1374,
        longitude: 11.5755,
        costCenter: 'DE-MUC-001',
        completenessScore: 0.95,
      },
      rightAttributes: {
        locationHk: 'b2c3d4e5f6a1b2c3',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'yext',
        name: "L'Osteria Marienplatz München",
        city: 'Munich',
        country: 'DE',
        zipCode: '80331',
        address: 'Marienplatz 8',
        phone: '+49 89 12345678',
        latitude: 48.1374,
        longitude: 11.5756,
        avgRating: 4.3,
        reviewCount: 2847,
        websiteUrl: 'https://losteria.net/de/muc-marienplatz',
        completenessScore: 0.82,
      },
    },
    {
      pairId: 'pair-002',
      hkLeft:  'c3d4e5f6a1b2c3d4',
      hkRight: 'd4e5f6a1b2c3d4e5',
      matchScore: 0.91,
      matchType: 'composite_high',
      nameScore: 0.93,
      cityMatch: true,
      zipMatch: true,
      geoScore: 0.92,
      status: 'pending',
      createdAt: '2024-01-15T08:25:00Z',
      leftAttributes: {
        locationHk: 'c3d4e5f6a1b2c3d4',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Frankfurt Sachsenhausen",
        city: 'Frankfurt',
        country: 'DE',
        zipCode: '60594',
        address: 'Schweizer Str. 62',
        costCenter: 'DE-FRA-003',
      },
      rightAttributes: {
        locationHk: 'd4e5f6a1b2c3d4e5',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'mcwin',
        name: "LOsteria Frankfurt Sachsenhausen",
        city: 'Frankfurt am Main',
        country: 'DE',
        zipCode: '60594',
        address: 'Schweizer Straße 62',
        region: 'Germany West',
      },
    },
    {
      pairId: 'pair-003',
      hkLeft:  'e5f6a1b2c3d4e5f6',
      hkRight: 'f6a1b2c3d4e5f6a1',
      matchScore: 0.88,
      matchType: 'fuzzy_name_city',
      nameScore: 0.85,
      cityMatch: true,
      zipMatch: false,
      geoScore: 0.94,
      status: 'pending',
      createdAt: '2024-01-15T08:30:00Z',
      leftAttributes: {
        locationHk: 'e5f6a1b2c3d4e5f6',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Wien Naschmarkt",
        city: 'Wien',
        country: 'AT',
        zipCode: '1060',
        address: 'Linke Wienzeile 4',
        latitude: 48.1994,
        longitude: 16.3659,
        costCenter: 'AT-VIE-002',
      },
      rightAttributes: {
        locationHk: 'f6a1b2c3d4e5f6a1',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'gopos',
        name: "L'Osteria Vienna Naschmarkt",
        city: 'Vienna',
        country: 'AT',
        zipCode: '1060',
        address: 'Linke Wienzeile 4',
        latitude: 48.1994,
        longitude: 16.3658,
      },
    },
    {
      pairId: 'pair-004',
      hkLeft:  'a2b3c4d5e6f7a2b3',
      hkRight: 'b3c4d5e6f7a2b3c4',
      matchScore: 0.87,
      matchType: 'composite',
      nameScore: 0.89,
      cityMatch: true,
      zipMatch: true,
      status: 'pending',
      createdAt: '2024-01-15T09:00:00Z',
      leftAttributes: {
        locationHk: 'a2b3c4d5e6f7a2b3',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Hamburg Altona",
        city: 'Hamburg',
        country: 'DE',
        zipCode: '22765',
        address: 'Große Elbstraße 145',
        costCenter: 'DE-HAM-001',
      },
      rightAttributes: {
        locationHk: 'b3c4d5e6f7a2b3c4',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'yext',
        name: "L'Osteria Altona Hamburg",
        city: 'Hamburg',
        country: 'DE',
        zipCode: '22765',
        address: 'Große Elbstraße 145',
        avgRating: 4.1,
        reviewCount: 1523,
      },
    },
    {
      pairId: 'pair-005',
      hkLeft:  'c4d5e6f7a2b3c4d5',
      hkRight: 'd5e6f7a2b3c4d5e6',
      matchScore: 0.86,
      matchType: 'geo_proximity',
      nameScore: 0.72,
      cityMatch: true,
      zipMatch: false,
      geoScore: 0.99,
      status: 'pending',
      createdAt: '2024-01-15T09:10:00Z',
      leftAttributes: {
        locationHk: 'c4d5e6f7a2b3c4d5',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Köln Rudolfplatz",
        city: 'Köln',
        country: 'DE',
        zipCode: '50674',
        address: 'Hahnenstrasse 16',
        latitude: 50.9333,
        longitude: 6.9402,
        costCenter: 'DE-CGN-002',
      },
      rightAttributes: {
        locationHk: 'd5e6f7a2b3c4d5e6',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'mcwin',
        name: "L'Osteria Rudolfplatz",
        city: 'Köln',
        country: 'DE',
        zipCode: '50676',
        address: 'Hahnenstr. 16',
        latitude: 50.9333,
        longitude: 6.9402,
        region: 'Germany West',
      },
    },
    {
      pairId: 'pair-006',
      hkLeft:  'e6f7a2b3c4d5e6f7',
      hkRight: 'f7a2b3c4d5e6f7a2',
      matchScore: 0.85,
      matchType: 'fuzzy_name_city',
      nameScore: 0.86,
      cityMatch: true,
      zipMatch: true,
      status: 'pending',
      createdAt: '2024-01-15T09:15:00Z',
      leftAttributes: {
        locationHk: 'e6f7a2b3c4d5e6f7',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Zürich Langstrasse",
        city: 'Zürich',
        country: 'CH',
        zipCode: '8004',
        address: 'Langstrasse 197',
        currencyCode: 'CHF',
        costCenter: 'CH-ZRH-001',
      },
      rightAttributes: {
        locationHk: 'f7a2b3c4d5e6f7a2',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'yext',
        name: "L'Osteria Langstrasse",
        city: 'Zürich',
        country: 'CH',
        zipCode: '8004',
        address: 'Langstrasse 197',
        avgRating: 4.5,
        reviewCount: 892,
        websiteUrl: 'https://losteria.net/ch/zrh-langstrasse',
      },
    },
    {
      pairId: 'pair-007',
      hkLeft:  'a3b4c5d6e7f8a3b4',
      hkRight: 'b4c5d6e7f8a3b4c5',
      matchScore: 0.85,
      matchType: 'composite',
      nameScore: 0.87,
      cityMatch: false,
      zipMatch: false,
      status: 'pending',
      createdAt: '2024-01-15T09:20:00Z',
      leftAttributes: {
        locationHk: 'a3b4c5d6e7f8a3b4',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'lightspeed',
        name: "L'Osteria Nürnberg Königstraße",
        city: 'Nürnberg',
        country: 'DE',
        zipCode: '90402',
        address: 'Königstraße 17',
        costCenter: 'DE-NUE-001',
      },
      rightAttributes: {
        locationHk: 'b4c5d6e7f8a3b4c5',
        loadDate: '2024-01-15T00:00:00Z',
        recordSource: 'gopos',
        name: "L'Osteria Nuernberg Koenigstrasse",
        city: 'Nuremberg',
        country: 'DE',
        zipCode: '90403',
        address: 'Königstrasse 17',
      },
    },
  ],
};

// ─── Golden Records List ──────────────────────────────────────────────────────
export const mockGoldenList: GoldenLocation[] = [
  { locationHk: 'a1b2c3d4e5f6a1b2', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria München Marienplatz", city: 'München', country: 'DE', zipCode: '80331', completenessScore: 0.97, sourcesCount: 2 },
  { locationHk: 'c3d4e5f6a1b2c3d4', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Frankfurt Sachsenhausen", city: 'Frankfurt', country: 'DE', zipCode: '60594', completenessScore: 0.78, sourcesCount: 1 },
  { locationHk: 'e5f6a1b2c3d4e5f6', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Wien Naschmarkt", city: 'Wien', country: 'AT', zipCode: '1060', completenessScore: 0.91, sourcesCount: 2 },
  { locationHk: 'a2b3c4d5e6f7a2b3', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Hamburg Altona", city: 'Hamburg', country: 'DE', zipCode: '22765', completenessScore: 0.85, sourcesCount: 2 },
  { locationHk: 'c4d5e6f7a2b3c4d5', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Köln Rudolfplatz", city: 'Köln', country: 'DE', zipCode: '50674', completenessScore: 0.88, sourcesCount: 2 },
  { locationHk: 'e6f7a2b3c4d5e6f7', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Zürich Langstrasse", city: 'Zürich', country: 'CH', zipCode: '8004', completenessScore: 0.93, sourcesCount: 2 },
  { locationHk: 'a3b4c5d6e7f8a3b4', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Nürnberg Königstraße", city: 'Nürnberg', country: 'DE', zipCode: '90402', completenessScore: 0.76, sourcesCount: 2 },
  { locationHk: 'b5c6d7e8f9a0b5c6', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Berlin Mitte", city: 'Berlin', country: 'DE', zipCode: '10115', completenessScore: 0.95, sourcesCount: 3 },
  { locationHk: 'c6d7e8f9a0b5c6d7', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Stuttgart Stadtmitte", city: 'Stuttgart', country: 'DE', zipCode: '70173', completenessScore: 0.82, sourcesCount: 2 },
  { locationHk: 'd7e8f9a0b5c6d7e8', validFrom: '2024-01-01T00:00:00Z', isCurrent: true, name: "L'Osteria Kraków Stare Miasto", city: 'Kraków', country: 'PL', zipCode: '31-001', completenessScore: 0.71, sourcesCount: 1 },
];

// ─── Config mock data ─────────────────────────────────────────────────────────
export const mockFieldConfigs: FieldConfig[] = [
  { entityId: 'business_location', fieldName: 'name',        matchWeight: 0.50, isBlockingKey: false, standardizer: 'uppercase_trim', isActive: true },
  { entityId: 'business_location', fieldName: 'city',        matchWeight: 0.10, isBlockingKey: true,  standardizer: 'uppercase_trim', isActive: true },
  { entityId: 'business_location', fieldName: 'country',     matchWeight: 0.10, isBlockingKey: true,  standardizer: 'iso_country',    isActive: true },
  { entityId: 'business_location', fieldName: 'zip_code',    matchWeight: 0.30, isBlockingKey: false, standardizer: 'strip_spaces',   isActive: true },
  { entityId: 'business_location', fieldName: 'latitude',    matchWeight: 0.20, isBlockingKey: false, standardizer: undefined,        isActive: true },
  { entityId: 'business_location', fieldName: 'longitude',   matchWeight: 0.20, isBlockingKey: false, standardizer: undefined,        isActive: true },
  { entityId: 'business_location', fieldName: 'phone',       matchWeight: 0.00, isBlockingKey: false, standardizer: undefined,        isActive: false },
  { entityId: 'business_location', fieldName: 'website_url', matchWeight: 0.00, isBlockingKey: false, standardizer: undefined,        isActive: false },
];

export const mockSourcePriorities: SourcePriorityConfig[] = [
  { entityId: 'business_location', sourceSystem: 'lightspeed', fieldName: '__default__', priority: 1 },
  { entityId: 'business_location', sourceSystem: 'mcwin',      fieldName: '__default__', priority: 2 },
  { entityId: 'business_location', sourceSystem: 'yext',       fieldName: '__default__', priority: 3 },
  { entityId: 'business_location', sourceSystem: 'gopos',      fieldName: '__default__', priority: 4 },
  { entityId: 'business_location', sourceSystem: 'yext',       fieldName: 'avg_rating',  priority: 1 },
  { entityId: 'business_location', sourceSystem: 'yext',       fieldName: 'latitude',    priority: 1 },
  { entityId: 'business_location', sourceSystem: 'yext',       fieldName: 'longitude',   priority: 1 },
  { entityId: 'business_location', sourceSystem: 'yext',       fieldName: 'website_url', priority: 1 },
  { entityId: 'business_location', sourceSystem: 'mcwin',      fieldName: 'cost_center', priority: 1 },
  { entityId: 'business_location', sourceSystem: 'mcwin',      fieldName: 'region',      priority: 1 },
];

// ─── Golden Record ────────────────────────────────────────────────────────────
export const mockGoldenRecords: Record<string, GoldenLocation> = {
  'a1b2c3d4e5f6a1b2': {
    locationHk: 'a1b2c3d4e5f6a1b2',
    validFrom: '2024-01-01T00:00:00Z',
    isCurrent: true,
    name: "L'Osteria München Marienplatz",
    city: 'München',
    country: 'DE',
    zipCode: '80331',
    address: 'Marienplatz 8',
    phone: '+49 89 12345678',
    latitude: 48.1374,
    longitude: 11.5755,
    websiteUrl: 'https://losteria.net/de/muc-marienplatz',
    timezone: 'Europe/Berlin',
    currencyCode: 'EUR',
    costCenter: 'DE-MUC-001',
    region: 'Germany South',
    avgRating: 4.3,
    reviewCount: 2847,
    nameSource: 'lightspeed',
    countrySource: 'lightspeed',
    citySource: 'lightspeed',
    lightspeedBlId: 41839,
    yextId: 'yext-muc-marienplatz',
    completenessScore: 0.97,
    sourcesCount: 2,
  },
  'c3d4e5f6a1b2c3d4': {
    locationHk: 'c3d4e5f6a1b2c3d4',
    validFrom: '2024-01-01T00:00:00Z',
    isCurrent: true,
    name: "L'Osteria Frankfurt Sachsenhausen",
    city: 'Frankfurt',
    country: 'DE',
    zipCode: '60594',
    address: 'Schweizer Str. 62',
    timezone: 'Europe/Berlin',
    currencyCode: 'EUR',
    costCenter: 'DE-FRA-003',
    region: 'Germany West',
    nameSource: 'lightspeed',
    lightspeedBlId: 41902,
    completenessScore: 0.78,
    sourcesCount: 1,
  },
};

// ─── Stewardship Log ─────────────────────────────────────────────────────────
export const mockStewardshipLog: StewardshipLogEntry[] = [
  {
    logId: 'log-001',
    canonicalHk: 'a1b2c3d4e5f6a1b2',
    action: 'accept_match',
    changedBy: 'jan.kowalski@losteria.com',
    changedAt: '2024-01-14T14:22:00Z',
    pairId: 'pair-000',
    reason: 'Same address and phone — confirmed same location',
  },
  {
    logId: 'log-002',
    canonicalHk: 'a1b2c3d4e5f6a1b2',
    action: 'override_field',
    fieldName: 'phone',
    oldValue: '+49 89 12345600',
    newValue: '+49 89 12345678',
    changedBy: 'anna.nowak@losteria.com',
    changedAt: '2024-01-14T16:05:00Z',
    reason: 'Confirmed with restaurant manager',
  },
];

// ─── Mock API implementacja ───────────────────────────────────────────────────
const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

let localCandidates = { ...mockCandidates, items: [...mockCandidates.items] };
let localStats = { ...mockStats };

export const mockApi = {
  getQueueStats: async () => {
    await delay(300);
    return { ...localStats };
  },

  getMatchCandidates: async (page: number, pageSize: number, status: string) => {
    await delay(400);
    const filtered = localCandidates.items.filter(c => c.status === status);
    const start = (page - 1) * pageSize;
    return {
      items: filtered.slice(start, start + pageSize),
      total: filtered.length,
      page,
      pageSize,
    };
  },

  submitPairReview: async (action: PairReviewAction) => {
    await delay(500);
    const item = localCandidates.items.find(c => c.pairId === action.pairId);
    if (item) {
      item.status = action.action === 'accept' ? 'accepted' : 'rejected';
      item.reviewedBy = 'demo@losteria.com';
      item.reviewedAt = new Date().toISOString();
      // Update stats
      localStats.pendingCount = Math.max(0, localStats.pendingCount - 1);
      if (action.action === 'accept') localStats.acceptedCount++;
      else localStats.rejectedCount = (localStats.rejectedCount ?? 0) + 1;
    }
    return { ok: true };
  },

  getGoldenLocation: async (locationHk: string) => {
    await delay(350);
    return mockGoldenRecords[locationHk] ?? mockGoldenRecords['a1b2c3d4e5f6a1b2'];
  },

  getStewardshipLog: async (_locationHk: string) => {
    await delay(250);
    return [...mockStewardshipLog];
  },

  overrideField: async (locationHk: string, fieldName: string, newValue: string, reason: string) => {
    await delay(400);
    const rec = mockGoldenRecords[locationHk];
    if (rec) (rec as any)[fieldName] = newValue;
    mockStewardshipLog.unshift({
      logId: `log-${Date.now()}`,
      canonicalHk: locationHk,
      action: 'override_field',
      fieldName,
      newValue,
      changedBy: 'demo@losteria.com',
      changedAt: new Date().toISOString(),
      reason,
    });
    return { ok: true };
  },

  getGoldenLocations: async (page: number, pageSize: number) => {
    await delay(400);
    const start = (page - 1) * pageSize;
    const items = mockGoldenList.slice(start, start + pageSize);
    return { items, total: mockGoldenList.length, page, pageSize };
  },

  getFieldConfigs: async (_entityId: string): Promise<FieldConfig[]> => {
    await delay(200);
    return [...mockFieldConfigs];
  },

  getSourcePriorities: async (_entityId: string): Promise<SourcePriorityConfig[]> => {
    await delay(200);
    return [...mockSourcePriorities];
  },

  createLocation: async (data: CreateLocationInput) => {
    await delay(600);
    const locationHk = `manual-${Date.now().toString(16)}`;
    mockGoldenRecords[locationHk] = {
      locationHk,
      validFrom: new Date().toISOString(),
      isCurrent: true,
      name: data.name,
      country: data.country,
      city: data.city,
      zipCode: data.zipCode,
      address: data.address,
      phone: data.phone,
      websiteUrl: data.websiteUrl,
      latitude: data.latitude,
      longitude: data.longitude,
      timezone: data.timezone,
      currencyCode: data.currencyCode,
      costCenter: data.costCenter,
      region: data.region,
      nameSource: 'manual' as any,
      countrySource: 'manual' as any,
      citySource: 'manual' as any,
      completenessScore: [data.name, data.country, data.city, data.zipCode, data.phone, data.address].filter(Boolean).length / 6,
      sourcesCount: 1,
    };
    mockStewardshipLog.unshift({
      logId: `log-${Date.now()}`,
      canonicalHk: locationHk,
      action: 'manual_create',
      changedBy: 'demo@losteria.com',
      changedAt: new Date().toISOString(),
      reason: `Ręczne dodanie: ${data.name}`,
    });
    localStats.totalGoldenRecords++;
    return { ok: true, locationHk };
  },
};

// Typ dla createLocation — eksportowany dla mdmApi.ts
export interface CreateLocationInput {
  name:          string;
  country:       string;
  city:          string;
  zipCode?:      string;
  address?:      string;
  phone?:        string;
  websiteUrl?:   string;
  latitude?:     number;
  longitude?:    number;
  timezone?:     string;
  currencyCode?: string;
  costCenter?:   string;
  region?:       string;
  notes?:        string;
}
