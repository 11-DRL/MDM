/// <reference types="vite/client" />
// API client — komunikacja z Fabric SQL Analytics Endpoint + Azure Function write proxy
// Auth: Azure AD (MSAL) — ten sam wzorzec co MSI w PL_sFTP_State_Manager.json
// Mock mode: VITE_MOCK_MODE=true → omija Azure AD, używa lokalnych danych demo
// Fabric iFrame mode: token pochodzi z FabricHostBridge (nie MSAL)

import axios, { AxiosInstance } from 'axios';
import { PublicClientApplication } from '@azure/msal-browser';
import type {
  MatchCandidatePage, GoldenLocation,
  StewardshipLogEntry, ReviewQueueStats, PairReviewAction,
  EntityConfig
} from '../types/mdm.types';
import { mockApi } from './mockData';
import { fabricHost } from '../lib/fabricHost';

// ---------- Konfiguracja (env vars w .env) ----------
export const MOCK_MODE = import.meta.env.VITE_MOCK_MODE === 'true';
const FABRIC_SQL_ENDPOINT = import.meta.env.VITE_FABRIC_SQL_ENDPOINT;
const WRITE_API_URL       = import.meta.env.VITE_WRITE_API_URL;
const TENANT_ID           = import.meta.env.VITE_TENANT_ID  ?? 'mock-tenant';
const CLIENT_ID           = import.meta.env.VITE_CLIENT_ID  ?? 'mock-client';

// MSAL scope dla Fabric SQL Endpoint
const FABRIC_SCOPE = 'https://analysis.windows.net/powerbi/api/.default';

// ---------- MSAL setup ----------
export const msalInstance = new PublicClientApplication({
  auth: {
    clientId: CLIENT_ID,
    authority: `https://login.microsoftonline.com/${TENANT_ID}`,
    redirectUri: window.location.origin,
  },
  cache: { cacheLocation: 'sessionStorage', storeAuthStateInCookie: false },
});

async function getAccessToken(): Promise<string> {
  // Priorytet 1: token z Fabric host (gdy działamy wewnątrz Fabric iFrame)
  const fabricToken = fabricHost.getToken();
  if (fabricToken) return fabricToken;

  // Priorytet 2: MSAL (standalone, poza Fabric)
  const accounts = msalInstance.getAllAccounts();
  if (accounts.length === 0) throw new Error('Not authenticated');
  const result = await msalInstance.acquireTokenSilent({
    scopes: [FABRIC_SCOPE],
    account: accounts[0],
  });
  return result.accessToken;
}

// ---------- HTTP clients ----------
async function createReadClient(): Promise<AxiosInstance> {
  const token = await getAccessToken();
  return axios.create({
    baseURL: FABRIC_SQL_ENDPOINT,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
}

async function createWriteClient(): Promise<AxiosInstance> {
  const token = await getAccessToken();
  return axios.create({
    baseURL: WRITE_API_URL,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
}

// ---------- Read: Review Queue ----------

export async function getQueueStats(): Promise<ReviewQueueStats> {
  if (MOCK_MODE) return mockApi.getQueueStats();
  const client = await createReadClient();
  const { data } = await client.get('/query', {
    params: {
      q: `
        SELECT
          SUM(CASE WHEN status = 'pending'       THEN 1 ELSE 0 END) AS pendingCount,
          SUM(CASE WHEN status = 'auto_accepted' THEN 1 ELSE 0 END) AS autoAcceptedCount,
          SUM(CASE WHEN status = 'accepted'      THEN 1 ELSE 0 END) AS acceptedCount,
          SUM(CASE WHEN status = 'rejected'      THEN 1 ELSE 0 END) AS rejectedCount,
          (SELECT COUNT(*) FROM gold.dim_location WHERE is_current = true) AS totalGoldenRecords,
          (SELECT AVG(completeness_score) FROM gold.dim_location_quality) AS avgCompletenessScore
        FROM silver_dv.bv_location_match_candidates
      `
    }
  });
  return data[0];
}

export async function getMatchCandidates(
  page = 1,
  pageSize = 20,
  status: 'pending' | 'all' = 'pending'
): Promise<MatchCandidatePage> {
  if (MOCK_MODE) return mockApi.getMatchCandidates(page, pageSize, status);
  const client = await createReadClient();
  const statusFilter = status === 'all' ? '' : `WHERE mc.status = 'pending'`;
  const offset = (page - 1) * pageSize;

  const { data } = await client.get('/query', {
    params: {
      q: `
        SELECT
          mc.pair_id       AS pairId,
          mc.hk_left       AS hkLeft,
          mc.hk_right      AS hkRight,
          mc.match_score   AS matchScore,
          mc.match_type    AS matchType,
          mc.name_score    AS nameScore,
          mc.city_match    AS cityMatch,
          mc.zip_match     AS zipMatch,
          mc.geo_score     AS geoScore,
          mc.status,
          mc.created_at    AS createdAt,
          mc.reviewed_by   AS reviewedBy,
          mc.reviewed_at   AS reviewedAt,
          -- Left satellite attributes
          COALESCE(ls_l.name, ys_l.name, ms_l.restaurant_name) AS leftName,
          COALESCE(ls_l.country, ys_l.country_code, ms_l.country) AS leftCountry,
          COALESCE(ls_l.city_std, ys_l.city, ms_l.city) AS leftCity,
          -- Right satellite attributes
          COALESCE(ls_r.name, ys_r.name, ms_r.restaurant_name) AS rightName,
          COALESCE(ls_r.country, ys_r.country_code, ms_r.country) AS rightCountry,
          COALESCE(ls_r.city_std, ys_r.city, ms_r.city) AS rightCity
        FROM silver_dv.bv_location_match_candidates mc
        LEFT JOIN silver_dv.sat_location_lightspeed ls_l
          ON mc.hk_left = ls_l.location_hk AND ls_l.load_end_date IS NULL
        LEFT JOIN silver_dv.sat_location_yext ys_l
          ON mc.hk_left = ys_l.location_hk AND ys_l.load_end_date IS NULL
        LEFT JOIN silver_dv.sat_location_mcwin ms_l
          ON mc.hk_left = ms_l.location_hk AND ms_l.load_end_date IS NULL
        LEFT JOIN silver_dv.sat_location_lightspeed ls_r
          ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
        LEFT JOIN silver_dv.sat_location_yext ys_r
          ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
        LEFT JOIN silver_dv.sat_location_mcwin ms_r
          ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
        ${statusFilter}
        ORDER BY mc.match_score DESC
        LIMIT ${pageSize} OFFSET ${offset}
      `
    }
  });

  const countResult = await client.get('/query', {
    params: { q: `SELECT COUNT(*) AS total FROM silver_dv.bv_location_match_candidates ${statusFilter}` }
  });

  return { items: data, total: countResult.data[0].total, page, pageSize };
}

export async function getGoldenLocation(locationHk: string): Promise<GoldenLocation> {
  if (MOCK_MODE) return mockApi.getGoldenLocation(locationHk);
  const client = await createReadClient();
  const { data } = await client.get('/query', {
    params: {
      q: `SELECT * FROM gold.dim_location WHERE location_hk = X'${locationHk}' AND is_current = true LIMIT 1`
    }
  });
  return data[0];
}

export async function getStewardshipLog(locationHk: string): Promise<StewardshipLogEntry[]> {
  if (MOCK_MODE) return mockApi.getStewardshipLog(locationHk);
  const client = await createReadClient();
  const { data } = await client.get('/query', {
    params: {
      q: `
        SELECT * FROM silver_dv.stewardship_log
        WHERE canonical_hk = X'${locationHk}'
        ORDER BY changed_at DESC
        LIMIT 100
      `
    }
  });
  return data;
}

// ---------- Write: Pair Review (through Azure Function proxy) ----------

export async function submitPairReview(action: PairReviewAction): Promise<void> {
  if (MOCK_MODE) { await mockApi.submitPairReview(action); return; }
  const client = await createWriteClient();
  await client.post('/api/mdm/location/review', action);
  // Azure Function wykona:
  // 1. UPDATE bv_location_match_candidates SET status = action.action, reviewed_by = user, reviewed_at = now()
  // 2. Jeśli accept: INSERT INTO bv_location_key_resolution (source_hk, canonical_hk, ...)
  // 3. INSERT INTO stewardship_log
}

export async function overrideField(
  locationHk: string,
  fieldName: string,
  newValue: string,
  reason: string
): Promise<void> {
  if (MOCK_MODE) { await mockApi.overrideField(locationHk, fieldName, newValue, reason); return; }
  const client = await createWriteClient();
  await client.post('/api/mdm/location/override', { locationHk, fieldName, newValue, reason });
}

// ---------- Config ----------

export async function getEntityConfig(entityId = 'business_location'): Promise<EntityConfig> {
  const client = await createReadClient();
  const { data } = await client.get('/query', {
    params: { q: `SELECT * FROM mdm_config.entity_config WHERE entity_id = '${entityId}' LIMIT 1` }
  });
  return data[0];
}
