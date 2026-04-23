/// <reference types="vite/client" />
// API client - all live read/write goes through Azure Function proxy.
// In mock mode data comes from local fixtures.

import axios, { AxiosInstance } from 'axios';
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

// For standalone mode we request Graph User.Read token.
const DEFAULT_SCOPE = 'User.Read';

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
    scopes: [DEFAULT_SCOPE],
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
  await client.post('/api/mdm/location/review', action);
}

export async function overrideField(
  locationHk: string,
  fieldName: string,
  newValue: string,
  reason: string
): Promise<void> {
  if (MOCK_MODE) {
    await mockApi.overrideField(locationHk, fieldName, newValue, reason);
    return;
  }
  const client = await createApiClient();
  await client.post('/api/mdm/location/override', { locationHk, fieldName, newValue, reason });
}

export type { CreateLocationInput };

export async function createLocation(data: CreateLocationInput): Promise<{ locationHk: string }> {
  if (MOCK_MODE) return mockApi.createLocation(data);
  const client = await createApiClient();
  const { data: result } = await client.post('/api/mdm/location/create', data);
  return result;
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

