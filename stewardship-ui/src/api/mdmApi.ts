/// <reference types="vite/client" />
// API client - all live read/write goes through Azure Function proxy.
// In mock mode data comes from local fixtures.

import axios, { AxiosInstance, AxiosError } from 'axios';
import { PublicClientApplication } from '@azure/msal-browser';
import type {
  MatchCandidatePage,
  MatchCandidate,
  GoldenLocation,
  StewardshipLogEntry,
  ReviewQueueStats,
  PairReviewAction,
  EntityConfig,
} from '../types/mdm.types';
import { mockApi, type CreateLocationInput } from './mockData';
import { fabricHost } from '../lib/fabricHost';

export const MOCK_MODE = import.meta.env.VITE_MOCK_MODE === 'true';
const WRITE_API_URL = import.meta.env.VITE_WRITE_API_URL ?? '';
const TENANT_ID = import.meta.env.VITE_TENANT_ID ?? 'mock-tenant';
const CLIENT_ID = import.meta.env.VITE_CLIENT_ID ?? 'mock-client';

// Scope dla naszego API w Function App. Token musi mieć aud = api://<clientId>,
// inaczej Function odrzuci go (EXPECTED_AUDIENCE check).
// Wymaga zdefiniowania w App Registration: "Expose an API" → scope `access_as_user`.
export const API_SCOPE = import.meta.env.VITE_API_SCOPE ?? `api://${CLIENT_ID}/access_as_user`;

export const msalInstance = new PublicClientApplication({
  auth: {
    clientId: CLIENT_ID,
    authority: `https://login.microsoftonline.com/${TENANT_ID}`,
    redirectUri: window.location.origin,
  },
  cache: { cacheLocation: 'sessionStorage', storeAuthStateInCookie: false },
});

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

async function createApiClient(): Promise<AxiosInstance> {
  const token = await getAccessToken();
  return axios.create({
    baseURL: WRITE_API_URL,
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  });
}

// ---------- Typed errors ----------

export class ApiConflictError extends Error {
  readonly status = 409;
  constructor(
    public readonly currentStatus?: string,
    public readonly reviewedBy?: string,
    public readonly reviewedAt?: string,
    message = 'Rekord został już zmieniony przez innego użytkownika',
  ) {
    super(message);
    this.name = 'ApiConflictError';
  }
}

export class ApiPreconditionError extends Error {
  readonly status = 412;
  constructor(
    public readonly currentValue?: unknown,
    message = 'Rekord zmieniony od ostatniego odczytu — odśwież widok',
  ) {
    super(message);
    this.name = 'ApiPreconditionError';
  }
}

function mapAxiosError(err: unknown): never {
  if (axios.isAxiosError(err)) {
    const ax = err as AxiosError<{ error?: string; currentStatus?: string; reviewedBy?: string; reviewedAt?: string; currentValue?: unknown }>;
    const body = ax.response?.data;
    if (ax.response?.status === 409) {
      throw new ApiConflictError(body?.currentStatus, body?.reviewedBy, body?.reviewedAt, body?.error);
    }
    if (ax.response?.status === 412) {
      throw new ApiPreconditionError(body?.currentValue, body?.error);
    }
  }
  throw err;
}

// ---------- Queue ----------

export async function getQueueStats(): Promise<ReviewQueueStats> {
  if (MOCK_MODE) return mockApi.getQueueStats();
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/queue/stats');
  return data;
}

export async function getMatchCandidates(
  page = 1,
  pageSize = 20,
  status: 'pending' | 'all' = 'pending'
): Promise<MatchCandidatePage> {
  if (MOCK_MODE) return mockApi.getMatchCandidates(page, pageSize, status);
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/location/candidates', {
    params: { page, pageSize, status },
  });
  return data;
}

export async function getPairById(pairId: string): Promise<MatchCandidate | null> {
  if (MOCK_MODE) {
    const page = await mockApi.getMatchCandidates(1, 1000, 'all');
    return page.items.find(c => c.pairId === pairId) ?? null;
  }
  const client = await createApiClient();
  const { data } = await client.get(`/api/mdm/location/pair/${encodeURIComponent(pairId)}`);
  return data ?? null;
}

// ---------- Golden ----------

export async function getGoldenLocations(
  page = 1,
  pageSize = 25
): Promise<import('../types/mdm.types').GoldenLocationPage> {
  if (MOCK_MODE) return mockApi.getGoldenLocations(page, pageSize);
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/location/golden', { params: { page, pageSize } });
  return data;
}

export async function getGoldenLocation(locationHk: string): Promise<GoldenLocation> {
  if (MOCK_MODE) return mockApi.getGoldenLocation(locationHk);
  const client = await createApiClient();
  const { data } = await client.get(`/api/mdm/location/golden/${encodeURIComponent(locationHk)}`);
  return data;
}

export async function getStewardshipLog(locationHk: string): Promise<StewardshipLogEntry[]> {
  if (MOCK_MODE) return mockApi.getStewardshipLog(locationHk);
  const client = await createApiClient();
  const { data } = await client.get(`/api/mdm/location/log/${encodeURIComponent(locationHk)}`);
  return data;
}

// ---------- Write ----------

export async function submitPairReview(action: PairReviewAction): Promise<void> {
  if (MOCK_MODE) {
    await mockApi.submitPairReview(action);
    return;
  }
  const client = await createApiClient();
  try {
    await client.post('/api/mdm/location/review', action);
  } catch (err) {
    mapAxiosError(err);
  }
}

export async function overrideField(
  locationHk: string,
  fieldName: string,
  newValue: string,
  reason: string,
  expectedOldValue?: string | null,
): Promise<void> {
  if (MOCK_MODE) {
    await mockApi.overrideField(locationHk, fieldName, newValue, reason);
    return;
  }
  const client = await createApiClient();
  try {
    await client.post('/api/mdm/location/override', {
      locationHk,
      fieldName,
      newValue,
      reason,
      expectedOldValue: expectedOldValue ?? null,
    });
  } catch (err) {
    mapAxiosError(err);
  }
}

export type { CreateLocationInput };

export async function createLocation(data: CreateLocationInput): Promise<{ locationHk: string }> {
  if (MOCK_MODE) return mockApi.createLocation(data);
  const client = await createApiClient();
  try {
    const { data: result } = await client.post('/api/mdm/location/create', data);
    return result;
  } catch (err) {
    mapAxiosError(err);
  }
}

// ---------- Config ----------

export async function getEntityConfig(entityId = 'business_location'): Promise<EntityConfig> {
  if (MOCK_MODE) {
    return {
      entityId: 'business_location',
      entityName: 'Business Location',
      hubTable: 'hub_location',
      isActive: true,
      matchThreshold: 0.85,
      autoAcceptThreshold: 0.97,
    };
  }
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/config/entity', { params: { entityId } });
  return data;
}

export async function getFieldConfigs(
  entityId = 'business_location'
): Promise<import('../types/mdm.types').FieldConfig[]> {
  if (MOCK_MODE) return mockApi.getFieldConfigs(entityId);
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/config/field-config', { params: { entityId } });
  return data;
}

export async function getSourcePriorities(
  entityId = 'business_location'
): Promise<import('../types/mdm.types').SourcePriorityConfig[]> {
  if (MOCK_MODE) return mockApi.getSourcePriorities(entityId);
  const client = await createApiClient();
  const { data } = await client.get('/api/mdm/config/source-priority', { params: { entityId } });
  return data;
}

