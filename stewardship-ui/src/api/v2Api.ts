// V2 API client — generic, entity-aware endpoints.
// Lives alongside mdmApi.ts (which handles v1 location-specific endpoints).

import axios from 'axios';
import type {
  EntityInfo,
  EntitySchema,
  GenericGoldenPage,
  GenericGoldenRecord,
  QueueStats,
  LogEntry,
} from '../types/v2.types';
import { MOCK_MODE, msalInstance, API_SCOPE } from './mdmApi';
import { fabricHost } from '../lib/fabricHost';

const WRITE_API_URL = import.meta.env.VITE_WRITE_API_URL ?? '';

// Re-use the same token acquisition from mdmApi
async function getAccessToken(): Promise<string> {
  const fabricToken = fabricHost.getToken();
  if (fabricToken) return fabricToken;

  const accounts = msalInstance.getAllAccounts();
  if (accounts.length === 0) throw new Error('Not authenticated');
  const result = await msalInstance.acquireTokenSilent({
    scopes: [API_SCOPE],
    account: accounts[0],
  });
  return result.accessToken;
}

async function v2Client() {
  const token = await getAccessToken();
  return axios.create({
    baseURL: `${WRITE_API_URL}/api/v2`,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
}

// ---------- Entities ----------

export async function listEntities(): Promise<EntityInfo[]> {
  if (MOCK_MODE) {
    return [
      {
        entityId: 'business_location',
        entityName: 'Business Location (Restauracja)',
        displayLabelPl: 'Lokalizacje biznesowe',
        displayLabelEn: 'Business Locations',
        icon: 'MapPin',
        matchEngine: 'jaro_winkler',
        hasMatching: true,
      },
      {
        entityId: 'legal_entity',
        entityName: 'Legal Entity (Spółka)',
        displayLabelPl: 'Spółki',
        displayLabelEn: 'Legal Entities',
        icon: 'Building2',
        matchEngine: 'none',
        hasMatching: false,
      },
    ];
  }
  const client = await v2Client();
  const { data } = await client.get('/entities');
  return data;
}

// ---------- Schema ----------

const MOCK_SCHEMAS: Record<string, EntitySchema> = {
  business_location: {
    entity: {
      entityId: 'business_location', entityName: 'Business Location',
      displayLabelPl: 'Lokalizacje biznesowe', displayLabelEn: 'Business Locations',
      icon: 'MapPin', matchEngine: 'jaro_winkler', hasMatching: true,
    },
    fields: [
      { fieldName: 'name', displayNamePl: 'Nazwa', displayNameEn: 'Name', displayOrder: 10, uiWidget: 'text', isOverridable: true, isRequired: true, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Podstawowe' },
      { fieldName: 'city', displayNamePl: 'Miasto', displayNameEn: 'City', displayOrder: 20, uiWidget: 'text', isOverridable: true, isRequired: true, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Adres' },
      { fieldName: 'country', displayNamePl: 'Kraj', displayNameEn: 'Country', displayOrder: 30, uiWidget: 'select', isOverridable: true, isRequired: true, validators: [{ type: 'enum', value: ['DE', 'AT', 'CH', 'NL', 'CZ', 'FR', 'GB', 'PL'] }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Adres' },
      { fieldName: 'zip_code', displayNamePl: 'Kod pocztowy', displayNameEn: 'Zip Code', displayOrder: 40, uiWidget: 'text', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Adres' },
      { fieldName: 'address', displayNamePl: 'Adres', displayNameEn: 'Address', displayOrder: 50, uiWidget: 'text', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Adres' },
      { fieldName: 'cost_center', displayNamePl: 'Cost Center', displayNameEn: 'Cost Center', displayOrder: 60, uiWidget: 'text', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Finanse' },
    ],
    sources: ['lightspeed', 'yext', 'mcwin', 'gopos', 'manual'],
  },
  legal_entity: {
    entity: {
      entityId: 'legal_entity', entityName: 'Legal Entity',
      displayLabelPl: 'Spółki', displayLabelEn: 'Legal Entities',
      icon: 'Building2', matchEngine: 'none', hasMatching: false,
    },
    fields: [
      { fieldName: 'legal_entity_code', displayNamePl: 'Kod spółki', displayNameEn: 'Entity Code', displayOrder: 10, uiWidget: 'text', isOverridable: false, isRequired: true, validators: [{ type: 'maxLength', value: 20 }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Identyfikacja' },
      { fieldName: 'name', displayNamePl: 'Nazwa', displayNameEn: 'Name', displayOrder: 20, uiWidget: 'text', isOverridable: true, isRequired: true, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Identyfikacja' },
      { fieldName: 'tax_id', displayNamePl: 'NIP / VAT ID', displayNameEn: 'Tax ID', displayOrder: 30, uiWidget: 'text', isOverridable: true, isRequired: true, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Identyfikacja' },
      { fieldName: 'country', displayNamePl: 'Kraj', displayNameEn: 'Country', displayOrder: 40, uiWidget: 'select', isOverridable: true, isRequired: true, validators: [{ type: 'enum', value: ['DE', 'AT', 'CH', 'NL', 'CZ', 'FR', 'GB', 'PL'] }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Lokalizacja' },
      { fieldName: 'currency_code', displayNamePl: 'Waluta', displayNameEn: 'Currency', displayOrder: 50, uiWidget: 'select', isOverridable: true, isRequired: true, validators: [{ type: 'enum', value: ['EUR', 'CHF', 'CZK', 'GBP', 'PLN'] }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Lokalizacja' },
      { fieldName: 'parent_entity_code', displayNamePl: 'Spółka nadrzędna', displayNameEn: 'Parent Entity', displayOrder: 60, uiWidget: 'text', isOverridable: true, isRequired: false, validators: [], lookupEntity: 'legal_entity', lookupField: 'legal_entity_code', isGoldenField: true, groupName: 'Hierarchia' },
      { fieldName: 'consolidation_method', displayNamePl: 'Metoda konsolidacji', displayNameEn: 'Consolidation Method', displayOrder: 70, uiWidget: 'select', isOverridable: true, isRequired: false, validators: [{ type: 'enum', value: ['full', 'proportional', 'equity', 'none'] }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Hierarchia' },
      { fieldName: 'ownership_pct', displayNamePl: '% udziałów', displayNameEn: 'Ownership %', displayOrder: 80, uiWidget: 'number', isOverridable: true, isRequired: false, validators: [{ type: 'range', value: [0, 100] }], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Hierarchia' },
      { fieldName: 'valid_from', displayNamePl: 'Ważne od', displayNameEn: 'Valid From', displayOrder: 90, uiWidget: 'date', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Okres' },
      { fieldName: 'valid_to', displayNamePl: 'Ważne do', displayNameEn: 'Valid To', displayOrder: 100, uiWidget: 'date', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Okres' },
      { fieldName: 'is_active', displayNamePl: 'Aktywna', displayNameEn: 'Active', displayOrder: 110, uiWidget: 'boolean', isOverridable: true, isRequired: false, validators: [], lookupEntity: null, lookupField: null, isGoldenField: true, groupName: 'Status' },
    ],
    sources: ['manual'],
  },
};

export async function getEntitySchema(entityId: string): Promise<EntitySchema> {
  if (MOCK_MODE) {
    return MOCK_SCHEMAS[entityId] ?? MOCK_SCHEMAS.business_location;
  }
  const client = await v2Client();
  const { data } = await client.get(`/entities/${encodeURIComponent(entityId)}/schema`);
  return data;
}

// ---------- Golden Records ----------

export async function getGoldenRecords(entityId: string, page = 1, pageSize = 25): Promise<GenericGoldenPage> {
  if (MOCK_MODE) {
    if (entityId === 'legal_entity') {
      return {
        items: [
          { hk: 'aa11bb22cc33dd44ee55ff6677889900aabb1122334455667788990011223344', entityId, attributes: { legal_entity_code: 'DE-HQ', name: "L'Osteria SE", tax_id: 'DE123456789', country: 'DE', currency_code: 'EUR', parent_entity_code: null, consolidation_method: 'none', ownership_pct: 100, is_active: true }, isCurrent: true },
          { hk: 'bb22cc33dd44ee55ff6677889900aabb1122334455667788990011223344aa11', entityId, attributes: { legal_entity_code: 'DE-MUC', name: "L'Osteria München GmbH", tax_id: 'DE987654321', country: 'DE', currency_code: 'EUR', parent_entity_code: 'DE-HQ', consolidation_method: 'full', ownership_pct: 100, is_active: true }, isCurrent: true },
          { hk: 'cc33dd44ee55ff6677889900aabb1122334455667788990011223344aa11bb22', entityId, attributes: { legal_entity_code: 'AT-VIE', name: "L'Osteria Wien GmbH", tax_id: 'ATU12345678', country: 'AT', currency_code: 'EUR', parent_entity_code: 'DE-HQ', consolidation_method: 'full', ownership_pct: 100, is_active: true }, isCurrent: true },
          { hk: 'dd44ee55ff6677889900aabb1122334455667788990011223344aa11bb22cc33', entityId, attributes: { legal_entity_code: 'CH-ZRH', name: "L'Osteria Zürich AG", tax_id: 'CHE-123.456.789', country: 'CH', currency_code: 'CHF', parent_entity_code: 'DE-HQ', consolidation_method: 'full', ownership_pct: 80, is_active: true }, isCurrent: true },
          { hk: 'ee55ff6677889900aabb1122334455667788990011223344aa11bb22cc33dd44', entityId, attributes: { legal_entity_code: 'NL-AMS', name: "L'Osteria Amsterdam B.V.", tax_id: 'NL123456789B01', country: 'NL', currency_code: 'EUR', parent_entity_code: 'DE-HQ', consolidation_method: 'proportional', ownership_pct: 51, is_active: true }, isCurrent: true },
          { hk: 'ff6677889900aabb1122334455667788990011223344aa11bb22cc33dd44ee55', entityId, attributes: { legal_entity_code: 'CZ-PRG', name: "L'Osteria Praha s.r.o.", tax_id: 'CZ12345678', country: 'CZ', currency_code: 'CZK', parent_entity_code: 'DE-HQ', consolidation_method: 'equity', ownership_pct: 30, is_active: true }, isCurrent: true },
        ],
        total: 6,
        page,
        pageSize,
      };
    }
    return { items: [], total: 0, page, pageSize };
  }
  const client = await v2Client();
  const { data } = await client.get(`/entities/${encodeURIComponent(entityId)}/golden`, { params: { page, pageSize } });
  return data;
}

export async function getGoldenRecord(entityId: string, hk: string): Promise<GenericGoldenRecord> {
  if (MOCK_MODE) {
    return { hk, entityId, attributes: {}, isCurrent: true };
  }
  const client = await v2Client();
  const { data } = await client.get(`/entities/${encodeURIComponent(entityId)}/golden/${encodeURIComponent(hk)}`);
  return data;
}

// ---------- Queue ----------

export async function getQueueStatsV2(entityId: string): Promise<QueueStats> {
  if (MOCK_MODE) {
    return { pendingCount: 0, autoAcceptedCount: 0, acceptedCount: 0, rejectedCount: 0, totalGoldenRecords: 0, avgCompletenessScore: 0 };
  }
  const client = await v2Client();
  const { data } = await client.get(`/entities/${encodeURIComponent(entityId)}/queue/stats`);
  return data;
}

// ---------- Log ----------

export async function getLogV2(entityId: string, hk: string): Promise<LogEntry[]> {
  if (MOCK_MODE) return [];
  const client = await v2Client();
  const { data } = await client.get(`/entities/${encodeURIComponent(entityId)}/log/${encodeURIComponent(hk)}`);
  return data;
}

// ---------- Write ----------

export async function overrideFieldV2(
  entityId: string, hk: string, fieldName: string,
  newValue: string, reason: string, expectedOldValue?: string | null,
): Promise<void> {
  if (MOCK_MODE) return;
  const client = await v2Client();
  await client.post(`/entities/${encodeURIComponent(entityId)}/override`, {
    hk, fieldName, newValue, reason, expectedOldValue: expectedOldValue ?? null,
  });
}

export async function createRecordV2(
  entityId: string, attributes: Record<string, unknown>,
): Promise<{ hk: string; businessKey: string }> {
  if (MOCK_MODE) return { hk: 'mock-hk', businessKey: 'manual|mock' };
  const client = await v2Client();
  const { data } = await client.post(`/entities/${encodeURIComponent(entityId)}/create`, { attributes });
  return data;
}
