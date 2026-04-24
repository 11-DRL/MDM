// Azure Function v4 - MDM API proxy for Fabric Lakehouse
// Read + write traffic goes through this API. DB connectivity uses Managed Identity.

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import { DefaultAzureCredential, type AccessToken } from '@azure/identity';
import * as crypto from 'crypto';
import * as sql from 'tedious';

type MatchSource = 'lightspeed' | 'yext' | 'mcwin' | 'gopos' | 'manual';
type MatchStatus = 'pending' | 'accepted' | 'rejected' | 'auto_accepted';
type QueryRows = Record<string, unknown>[];

const HEX_32_RE = /^[0-9a-f]{64}$/i;
const SAFE_ENTITY_ID_RE = /^[a-z0-9_]+$/i;

// ---------- Auth ----------

function validateBearerToken(req: HttpRequest): { ok: true; email: string } | { ok: false; status: 401 | 403; error: string } {
  const authHeader = req.headers.get('authorization') ?? req.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) {
    return { ok: false, status: 401, error: 'Missing or invalid Authorization header' };
  }

  const token = authHeader.slice(7);
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return { ok: false, status: 401, error: 'Malformed JWT' };

    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf-8'));
    const expectedTenant = process.env.AZURE_TENANT_ID;
    if (expectedTenant && payload.tid && payload.tid !== expectedTenant) {
      return { ok: false, status: 403, error: 'Token tenant mismatch' };
    }

    const email: string = payload.preferred_username ?? payload.upn ?? payload.email ?? payload.unique_name ?? 'unknown';
    return { ok: true, email };
  } catch {
    return { ok: false, status: 401, error: 'Failed to decode JWT' };
  }
}

function requireAuth(req: HttpRequest): { ok: true; email: string } | HttpResponseInit {
  const result = validateBearerToken(req);
  if (!result.ok) return { status: result.status, jsonBody: { error: result.error } };
  return result;
}

// ---------- DB ----------

const SQL_SCOPE = 'https://database.windows.net/.default';
const credential = new DefaultAzureCredential();
let cachedToken: AccessToken | null = null;

async function getSqlAccessToken(): Promise<string> {
  const now = Date.now();
  // Refresh 5 minutes before expiry
  if (cachedToken && cachedToken.expiresOnTimestamp - now > 5 * 60 * 1000) {
    return cachedToken.token;
  }
  const token = await credential.getToken(SQL_SCOPE);
  if (!token) throw new Error('Failed to acquire Azure AD token for Fabric SQL');
  cachedToken = token;
  return token.token;
}

async function getConnection(): Promise<sql.Connection> {
  const token = await getSqlAccessToken();
  const config: sql.ConnectionConfiguration = {
    server: process.env.FABRIC_SQL_SERVER!,
    authentication: { type: 'azure-active-directory-access-token', options: { token } },
    options: {
      database: process.env.FABRIC_DATABASE ?? 'lh_mdm',
      encrypt: true,
      port: 1433,
      trustServerCertificate: false,
      connectTimeout: 30_000,
      requestTimeout: 60_000,
    },
  };
  return new sql.Connection(config);
}

function sqlTypeForValue(value: unknown) {
  if (typeof value === 'bigint') return sql.TYPES.BigInt;
  if (typeof value === 'number') {
    if (!Number.isInteger(value)) return sql.TYPES.Float;
    return (value > 2147483647 || value < -2147483648) ? sql.TYPES.BigInt : sql.TYPES.Int;
  }
  if (typeof value === 'boolean') return sql.TYPES.Bit;
  if (value instanceof Date) return sql.TYPES.DateTime2;
  if (Buffer.isBuffer(value)) return sql.TYPES.VarBinary;
  return sql.TYPES.NVarChar;
}

function addSqlParameters(req: sql.Request, params: Record<string, unknown>) {
  for (const [name, value] of Object.entries(params)) {
    req.addParameter(name, sqlTypeForValue(value), value as never);
  }
}

async function withRetry<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastError: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (i < attempts - 1) {
        await new Promise(resolve => setTimeout(resolve, 200 * Math.pow(2, i)));
      }
    }
  }
  throw lastError;
}

async function execSql(query: string, params: Record<string, unknown> = {}): Promise<void> {
  return withRetry(() => new Promise(async (resolve, reject) => {
    let conn: sql.Connection;
    try {
      conn = await getConnection();
    } catch (err) {
      return reject(err);
    }
    conn.on('connect', (err) => {
      if (err) return reject(err);

      const request = new sql.Request(query, (requestErr) => {
        conn.close();
        if (requestErr) reject(requestErr);
        else resolve();
      });

      addSqlParameters(request, params);
      conn.execSql(request);
    });
    conn.connect();
  }));
}

async function querySql<T = Record<string, unknown>>(query: string, params: Record<string, unknown> = {}): Promise<T[]> {
  return withRetry(() => new Promise(async (resolve, reject) => {
    let conn: sql.Connection;
    try {
      conn = await getConnection();
    } catch (err) {
      return reject(err);
    }
    const rows: T[] = [];

    conn.on('connect', (err) => {
      if (err) return reject(err);

      const request = new sql.Request(query, (requestErr) => {
        conn.close();
        if (requestErr) reject(requestErr);
        else resolve(rows);
      });

      request.on('row', (columns: Array<{ metadata: { colName: string }; value: unknown }>) => {
        const row: Record<string, unknown> = {};
        for (const col of columns) {
          row[col.metadata.colName] = col.value;
        }
        rows.push(row as T);
      });

      addSqlParameters(request, params);
      conn.execSql(request);
    });
    conn.connect();
  }));
}

// ---------- Helpers ----------

function asString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'string') return value;
  return String(value);
}

function asNumber(value: unknown, fallback = 0): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asBoolean(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') return value;
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const norm = value.trim().toLowerCase();
    if (norm === 'true' || norm === '1') return true;
    if (norm === 'false' || norm === '0') return false;
  }
  return undefined;
}

function asIso(value: unknown): string | undefined {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string' && value.trim().length > 0) return value;
  return undefined;
}

function sanitizeHex32(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!HEX_32_RE.test(trimmed)) return null;
  return trimmed.toLowerCase();
}

function parsePositiveInt(rawValue: string | null, defaultValue: number, min: number, max: number): number {
  const parsed = Number(rawValue ?? defaultValue);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

function parseStatus(raw: string | null): 'pending' | 'all' {
  return raw?.toLowerCase() === 'all' ? 'all' : 'pending';
}

function sanitizeEntityId(raw: string | null): string {
  const value = raw?.trim() || 'business_location';
  if (!SAFE_ENTITY_ID_RE.test(value)) {
    throw new Error('Invalid entityId');
  }
  return value;
}

function toMatchSource(value: unknown): MatchSource {
  const raw = String(value ?? '').toLowerCase();
  if (raw === 'lightspeed' || raw === 'yext' || raw === 'mcwin' || raw === 'gopos' || raw === 'manual') {
    return raw;
  }
  return 'lightspeed';
}

// ---------- Read: queue stats ----------

async function getQueueStats(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        COALESCE(SUM(CASE WHEN status = 'pending'       THEN 1 ELSE 0 END), 0) AS pendingCount,
        COALESCE(SUM(CASE WHEN status = 'auto_accepted' THEN 1 ELSE 0 END), 0) AS autoAcceptedCount,
        COALESCE(SUM(CASE WHEN status = 'accepted'      THEN 1 ELSE 0 END), 0) AS acceptedCount,
        COALESCE(SUM(CASE WHEN status = 'rejected'      THEN 1 ELSE 0 END), 0) AS rejectedCount,
        (SELECT COUNT(*) FROM gold.dim_location WHERE is_current = 1)           AS totalGoldenRecords,
        COALESCE(CAST((SELECT AVG(completeness_score) FROM gold.dim_location_quality) AS FLOAT), 0.0) AS avgCompletenessScore
      FROM silver_dv.bv_location_match_candidates
    `);

    const row = rows[0] ?? {};
    return {
      status: 200,
      jsonBody: {
        pendingCount: asNumber(row.pendingCount),
        autoAcceptedCount: asNumber(row.autoAcceptedCount),
        acceptedCount: asNumber(row.acceptedCount),
        rejectedCount: asNumber(row.rejectedCount),
        totalGoldenRecords: asNumber(row.totalGoldenRecords),
        avgCompletenessScore: asNumber(row.avgCompletenessScore, 0),
      },
    };
  } catch (err) {
    ctx.error('getQueueStats failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Read: match candidates ----------

async function getMatchCandidates(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const page = parsePositiveInt(req.query.get('page'), 1, 1, 10_000);
  const pageSize = parsePositiveInt(req.query.get('pageSize'), 25, 1, 200);
  const status = parseStatus(req.query.get('status'));
  const offset = (page - 1) * pageSize;
  const whereClause = status === 'pending' ? `WHERE mc.status = 'pending'` : '';

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        mc.pair_id       AS pairId,
        CONVERT(VARCHAR(64), mc.hk_left, 2)  AS hkLeft,
        CONVERT(VARCHAR(64), mc.hk_right, 2) AS hkRight,
        CAST(mc.match_score AS FLOAT) AS matchScore,
        mc.match_type    AS matchType,
        CAST(mc.name_score AS FLOAT) AS nameScore,
        mc.city_match    AS cityMatch,
        mc.zip_match     AS zipMatch,
        CAST(mc.geo_score AS FLOAT) AS geoScore,
        mc.status,
        mc.created_at    AS createdAt,
        mc.reviewed_by   AS reviewedBy,
        mc.reviewed_at   AS reviewedAt,
        mc.review_note   AS reviewNote,
        COALESCE(ls_l.name, ys_l.name, ms_l.restaurant_name, gs_l.location_name, man_l.name) AS leftName,
        COALESCE(ls_l.country, ys_l.country_code, ms_l.country, gs_l.country, man_l.country)  AS leftCountry,
        COALESCE(ls_l.city_std, ys_l.city, ms_l.city, gs_l.city, man_l.city)                   AS leftCity,
        COALESCE(ls_r.name, ys_r.name, ms_r.restaurant_name, gs_r.location_name, man_r.name) AS rightName,
        COALESCE(ls_r.country, ys_r.country_code, ms_r.country, gs_r.country, man_r.country)  AS rightCountry,
        COALESCE(ls_r.city_std, ys_r.city, ms_r.city, gs_r.city, man_r.city)                   AS rightCity
      FROM silver_dv.bv_location_match_candidates mc
      LEFT JOIN silver_dv.sat_location_lightspeed ls_l ON mc.hk_left = ls_l.location_hk AND ls_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_l ON mc.hk_left = ys_l.location_hk AND ys_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_l ON mc.hk_left = ms_l.location_hk AND ms_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_l ON mc.hk_left = gs_l.location_hk AND gs_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_manual     man_l ON mc.hk_left = man_l.location_hk AND man_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_lightspeed ls_r ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_r ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_r ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_r ON mc.hk_right = gs_r.location_hk AND gs_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_manual     man_r ON mc.hk_right = man_r.location_hk AND man_r.load_end_date IS NULL
      ${whereClause}
      ORDER BY mc.match_score DESC
      OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY
    `);

    const countRows = await querySql<Record<string, unknown>>(`
      SELECT COUNT(*) AS total
      FROM silver_dv.bv_location_match_candidates mc
      ${whereClause}
    `);

    const items = rows.map(row => ({
      pairId: asString(row.pairId) ?? '',
      hkLeft: asString(row.hkLeft) ?? '',
      hkRight: asString(row.hkRight) ?? '',
      matchScore: asNumber(row.matchScore),
      matchType: asString(row.matchType) ?? 'composite',
      nameScore: row.nameScore == null ? undefined : asNumber(row.nameScore),
      cityMatch: asBoolean(row.cityMatch),
      zipMatch: asBoolean(row.zipMatch),
      geoScore: row.geoScore == null ? undefined : asNumber(row.geoScore),
      status: asString(row.status) as MatchStatus,
      createdAt: asIso(row.createdAt),
      reviewedBy: asString(row.reviewedBy),
      reviewedAt: asIso(row.reviewedAt),
      reviewNote: asString(row.reviewNote),
      leftName: asString(row.leftName),
      leftCountry: asString(row.leftCountry),
      leftCity: asString(row.leftCity),
      rightName: asString(row.rightName),
      rightCountry: asString(row.rightCountry),
      rightCity: asString(row.rightCity),
    }));

    return {
      status: 200,
      jsonBody: {
        items,
        total: asNumber(countRows[0]?.total),
        page,
        pageSize,
      },
    };
  } catch (err) {
    ctx.error('getMatchCandidates failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Read: pair detail ----------

async function getPair(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const pairId = req.params.pairId;
  if (!pairId) return { status: 400, jsonBody: { error: 'pairId required' } };

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        mc.pair_id       AS pairId,
        CONVERT(VARCHAR(64), mc.hk_left, 2)  AS hkLeft,
        CONVERT(VARCHAR(64), mc.hk_right, 2) AS hkRight,
        CAST(mc.match_score AS FLOAT) AS matchScore,
        mc.match_type    AS matchType,
        CAST(mc.name_score AS FLOAT) AS nameScore,
        mc.city_match    AS cityMatch,
        mc.zip_match     AS zipMatch,
        CAST(mc.geo_score AS FLOAT) AS geoScore,
        mc.status,
        mc.created_at    AS createdAt,
        mc.reviewed_by   AS reviewedBy,
        mc.reviewed_at   AS reviewedAt,
        mc.review_note   AS reviewNote,

        COALESCE(ls_l.name, ys_l.name, ms_l.restaurant_name, gs_l.location_name, man_l.name) AS leftName,
        COALESCE(ls_l.country, ys_l.country_code, ms_l.country, gs_l.country, man_l.country)  AS leftCountry,
        COALESCE(ls_l.city_std, ys_l.city, ms_l.city, gs_l.city, man_l.city)                   AS leftCity,
        COALESCE(ys_l.postal_code, ms_l.zip_code, gs_l.zip_code, man_l.zip_code)               AS leftZipCode,
        COALESCE(ys_l.address_line1, ms_l.address, gs_l.address, man_l.address)                AS leftAddress,
        COALESCE(ys_l.phone, gs_l.phone, man_l.phone)                                           AS leftPhone,
        COALESCE(ys_l.website_url, man_l.website_url)                                           AS leftWebsiteUrl,
        COALESCE(ys_l.latitude, man_l.latitude)                                                 AS leftLatitude,
        COALESCE(ys_l.longitude, man_l.longitude)                                               AS leftLongitude,
        ys_l.avg_rating                                                                         AS leftAvgRating,
        ys_l.review_count                                                                       AS leftReviewCount,
        COALESCE(ms_l.cost_center, man_l.cost_center)                                           AS leftCostCenter,
        COALESCE(ms_l.region, man_l.region)                                                     AS leftRegion,
        CASE
          WHEN man_l.location_hk IS NOT NULL THEN 'manual'
          WHEN ls_l.location_hk IS NOT NULL THEN 'lightspeed'
          WHEN ys_l.location_hk IS NOT NULL THEN 'yext'
          WHEN ms_l.location_hk IS NOT NULL THEN 'mcwin'
          WHEN gs_l.location_hk IS NOT NULL THEN 'gopos'
          ELSE 'lightspeed'
        END AS leftSource,

        COALESCE(ls_r.name, ys_r.name, ms_r.restaurant_name, gs_r.location_name, man_r.name) AS rightName,
        COALESCE(ls_r.country, ys_r.country_code, ms_r.country, gs_r.country, man_r.country)  AS rightCountry,
        COALESCE(ls_r.city_std, ys_r.city, ms_r.city, gs_r.city, man_r.city)                   AS rightCity,
        COALESCE(ys_r.postal_code, ms_r.zip_code, gs_r.zip_code, man_r.zip_code)               AS rightZipCode,
        COALESCE(ys_r.address_line1, ms_r.address, gs_r.address, man_r.address)                AS rightAddress,
        COALESCE(ys_r.phone, gs_r.phone, man_r.phone)                                           AS rightPhone,
        COALESCE(ys_r.website_url, man_r.website_url)                                           AS rightWebsiteUrl,
        COALESCE(ys_r.latitude, man_r.latitude)                                                 AS rightLatitude,
        COALESCE(ys_r.longitude, man_r.longitude)                                               AS rightLongitude,
        ys_r.avg_rating                                                                         AS rightAvgRating,
        ys_r.review_count                                                                       AS rightReviewCount,
        COALESCE(ms_r.cost_center, man_r.cost_center)                                           AS rightCostCenter,
        COALESCE(ms_r.region, man_r.region)                                                     AS rightRegion,
        CASE
          WHEN man_r.location_hk IS NOT NULL THEN 'manual'
          WHEN ls_r.location_hk IS NOT NULL THEN 'lightspeed'
          WHEN ys_r.location_hk IS NOT NULL THEN 'yext'
          WHEN ms_r.location_hk IS NOT NULL THEN 'mcwin'
          WHEN gs_r.location_hk IS NOT NULL THEN 'gopos'
          ELSE 'lightspeed'
        END AS rightSource
      FROM silver_dv.bv_location_match_candidates mc
      LEFT JOIN silver_dv.sat_location_lightspeed ls_l ON mc.hk_left  = ls_l.location_hk AND ls_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_l ON mc.hk_left  = ys_l.location_hk AND ys_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_l ON mc.hk_left  = ms_l.location_hk AND ms_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_l ON mc.hk_left  = gs_l.location_hk AND gs_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_manual     man_l ON mc.hk_left = man_l.location_hk AND man_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_lightspeed ls_r ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_r ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_r ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_r ON mc.hk_right = gs_r.location_hk AND gs_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_manual     man_r ON mc.hk_right = man_r.location_hk AND man_r.load_end_date IS NULL
      WHERE mc.pair_id = @pairId
    `, { pairId });

    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Pair not found' } };

    const row = rows[0];
    const hkLeft = asString(row.hkLeft) ?? '';
    const hkRight = asString(row.hkRight) ?? '';
    const createdAt = asIso(row.createdAt);

    const leftSource = toMatchSource(row.leftSource);
    const rightSource = toMatchSource(row.rightSource);

    return {
      status: 200,
      jsonBody: {
        pairId: asString(row.pairId),
        hkLeft,
        hkRight,
        matchScore: asNumber(row.matchScore),
        matchType: asString(row.matchType),
        nameScore: row.nameScore == null ? undefined : asNumber(row.nameScore),
        cityMatch: asBoolean(row.cityMatch),
        zipMatch: asBoolean(row.zipMatch),
        geoScore: row.geoScore == null ? undefined : asNumber(row.geoScore),
        status: asString(row.status),
        createdAt,
        reviewedBy: asString(row.reviewedBy),
        reviewedAt: asIso(row.reviewedAt),
        reviewNote: asString(row.reviewNote),
        leftSource,
        rightSource,
        leftAttributes: {
          locationHk: hkLeft,
          loadDate: createdAt,
          recordSource: leftSource,
          name: asString(row.leftName),
          country: asString(row.leftCountry),
          city: asString(row.leftCity),
          zipCode: asString(row.leftZipCode),
          address: asString(row.leftAddress),
          phone: asString(row.leftPhone),
          websiteUrl: asString(row.leftWebsiteUrl),
          latitude: row.leftLatitude == null ? undefined : asNumber(row.leftLatitude),
          longitude: row.leftLongitude == null ? undefined : asNumber(row.leftLongitude),
          avgRating: row.leftAvgRating == null ? undefined : asNumber(row.leftAvgRating),
          reviewCount: row.leftReviewCount == null ? undefined : asNumber(row.leftReviewCount),
          costCenter: asString(row.leftCostCenter),
          region: asString(row.leftRegion),
        },
        rightAttributes: {
          locationHk: hkRight,
          loadDate: createdAt,
          recordSource: rightSource,
          name: asString(row.rightName),
          country: asString(row.rightCountry),
          city: asString(row.rightCity),
          zipCode: asString(row.rightZipCode),
          address: asString(row.rightAddress),
          phone: asString(row.rightPhone),
          websiteUrl: asString(row.rightWebsiteUrl),
          latitude: row.rightLatitude == null ? undefined : asNumber(row.rightLatitude),
          longitude: row.rightLongitude == null ? undefined : asNumber(row.rightLongitude),
          avgRating: row.rightAvgRating == null ? undefined : asNumber(row.rightAvgRating),
          reviewCount: row.rightReviewCount == null ? undefined : asNumber(row.rightReviewCount),
          costCenter: asString(row.rightCostCenter),
          region: asString(row.rightRegion),
        },
      },
    };
  } catch (err) {
    ctx.error('getPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Read: golden locations ----------

async function getGoldenLocations(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const page = parsePositiveInt(req.query.get('page'), 1, 1, 10_000);
  const pageSize = parsePositiveInt(req.query.get('pageSize'), 25, 1, 200);
  const offset = (page - 1) * pageSize;

  try {
    const rows = await querySql<Record<string, unknown>>(`
      WITH quality_latest AS (
        SELECT
          location_hk,
          sources_count,
          completeness_score,
          ROW_NUMBER() OVER (PARTITION BY location_hk ORDER BY snapshot_date DESC) AS rn
        FROM gold.dim_location_quality
      )
      SELECT
        CONVERT(VARCHAR(64), g.location_hk, 2) AS locationHk,
        g.name,
        g.country,
        g.city,
        g.zip_code AS zipCode,
        g.phone,
        g.website_url AS websiteUrl,
        g.cost_center AS costCenter,
        g.region,
        g.valid_from AS validFrom,
        g.valid_to AS validTo,
        g.is_current AS isCurrent,
        g.name_source AS nameSource,
        g.country_source AS countrySource,
        g.city_source AS citySource,
        g.lightspeed_bl_id AS lightspeedBlId,
        g.yext_id AS yextId,
        g.mcwin_restaurant_id AS mcwinRestaurantId,
        g.gopos_location_id AS goposLocationId,
        CAST(q.completeness_score AS FLOAT) AS completenessScore,
        q.sources_count AS sourcesCount
      FROM gold.dim_location g
      LEFT JOIN quality_latest q
        ON g.location_hk = q.location_hk AND q.rn = 1
      WHERE g.is_current = 1
      ORDER BY g.country, g.city, g.name
      OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY
    `);

    const countRows = await querySql<Record<string, unknown>>(`
      SELECT COUNT(*) AS total FROM gold.dim_location WHERE is_current = 1
    `);

    return {
      status: 200,
      jsonBody: {
        items: rows.map(row => ({
          locationHk: asString(row.locationHk),
          name: asString(row.name),
          country: asString(row.country),
          city: asString(row.city),
          zipCode: asString(row.zipCode),
          phone: asString(row.phone),
          websiteUrl: asString(row.websiteUrl),
          costCenter: asString(row.costCenter),
          region: asString(row.region),
          validFrom: asIso(row.validFrom),
          validTo: asIso(row.validTo),
          isCurrent: asBoolean(row.isCurrent) ?? true,
          nameSource: asString(row.nameSource),
          countrySource: asString(row.countrySource),
          citySource: asString(row.citySource),
          lightspeedBlId: row.lightspeedBlId == null ? undefined : asNumber(row.lightspeedBlId),
          yextId: asString(row.yextId),
          mcwinRestaurantId: asString(row.mcwinRestaurantId),
          goposLocationId: asString(row.goposLocationId),
          completenessScore: row.completenessScore == null ? undefined : asNumber(row.completenessScore),
          sourcesCount: row.sourcesCount == null ? undefined : asNumber(row.sourcesCount),
        })),
        total: asNumber(countRows[0]?.total),
        page,
        pageSize,
      },
    };
  } catch (err) {
    ctx.error('getGoldenLocations failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

async function getGoldenLocation(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const locationHk = sanitizeHex32(req.params.locationHk);
  if (!locationHk) return { status: 400, jsonBody: { error: 'Invalid locationHk (expected 64-char hex)' } };

  try {
    const rows = await querySql<Record<string, unknown>>(`
      WITH quality_latest AS (
        SELECT
          location_hk,
          sources_count,
          completeness_score,
          ROW_NUMBER() OVER (PARTITION BY location_hk ORDER BY snapshot_date DESC) AS rn
        FROM gold.dim_location_quality
      )
      SELECT
        CONVERT(VARCHAR(64), g.location_hk, 2) AS locationHk,
        g.name,
        g.country,
        g.city,
        g.zip_code AS zipCode,
        g.address,
        g.phone,
        g.latitude,
        g.longitude,
        g.website_url AS websiteUrl,
        g.timezone,
        g.currency_code AS currencyCode,
        g.avg_rating AS avgRating,
        g.review_count AS reviewCount,
        g.cost_center AS costCenter,
        g.region,
        g.valid_from AS validFrom,
        g.valid_to AS validTo,
        g.is_current AS isCurrent,
        g.name_source AS nameSource,
        g.country_source AS countrySource,
        g.city_source AS citySource,
        g.lightspeed_bl_id AS lightspeedBlId,
        g.yext_id AS yextId,
        g.mcwin_restaurant_id AS mcwinRestaurantId,
        g.gopos_location_id AS goposLocationId,
        CAST(q.completeness_score AS FLOAT) AS completenessScore,
        q.sources_count AS sourcesCount
      FROM gold.dim_location g
      LEFT JOIN quality_latest q
        ON g.location_hk = q.location_hk AND q.rn = 1
      WHERE g.location_hk = CONVERT(VARBINARY(32), @locationHk, 2)
        AND g.is_current = 1
    `, { locationHk });

    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Golden location not found' } };
    const row = rows[0];

    return {
      status: 200,
      jsonBody: {
        locationHk: asString(row.locationHk),
        name: asString(row.name),
        country: asString(row.country),
        city: asString(row.city),
        zipCode: asString(row.zipCode),
        address: asString(row.address),
        phone: asString(row.phone),
        latitude: row.latitude == null ? undefined : asNumber(row.latitude),
        longitude: row.longitude == null ? undefined : asNumber(row.longitude),
        websiteUrl: asString(row.websiteUrl),
        timezone: asString(row.timezone),
        currencyCode: asString(row.currencyCode),
        avgRating: row.avgRating == null ? undefined : asNumber(row.avgRating),
        reviewCount: row.reviewCount == null ? undefined : asNumber(row.reviewCount),
        costCenter: asString(row.costCenter),
        region: asString(row.region),
        validFrom: asIso(row.validFrom),
        validTo: asIso(row.validTo),
        isCurrent: asBoolean(row.isCurrent) ?? true,
        nameSource: asString(row.nameSource),
        countrySource: asString(row.countrySource),
        citySource: asString(row.citySource),
        lightspeedBlId: row.lightspeedBlId == null ? undefined : asNumber(row.lightspeedBlId),
        yextId: asString(row.yextId),
        mcwinRestaurantId: asString(row.mcwinRestaurantId),
        goposLocationId: asString(row.goposLocationId),
        completenessScore: row.completenessScore == null ? undefined : asNumber(row.completenessScore),
        sourcesCount: row.sourcesCount == null ? undefined : asNumber(row.sourcesCount),
      },
    };
  } catch (err) {
    ctx.error('getGoldenLocation failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

async function getStewardshipLog(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const locationHk = sanitizeHex32(req.params.locationHk);
  if (!locationHk) return { status: 400, jsonBody: { error: 'Invalid locationHk (expected 64-char hex)' } };

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        log_id AS logId,
        CONVERT(VARCHAR(64), canonical_hk, 2) AS canonicalHk,
        action,
        field_name AS fieldName,
        old_value AS oldValue,
        new_value AS newValue,
        changed_by AS changedBy,
        changed_at AS changedAt,
        pair_id AS pairId,
        reason
      FROM silver_dv.stewardship_log
      WHERE canonical_hk = CONVERT(VARBINARY(32), @locationHk, 2)
      ORDER BY changed_at DESC
      LIMIT 100
    `, { locationHk });

    return {
      status: 200,
      jsonBody: rows.map(row => ({
        logId: asString(row.logId),
        canonicalHk: asString(row.canonicalHk),
        action: asString(row.action),
        fieldName: asString(row.fieldName),
        oldValue: asString(row.oldValue),
        newValue: asString(row.newValue),
        changedBy: asString(row.changedBy),
        changedAt: asIso(row.changedAt),
        pairId: asString(row.pairId),
        reason: asString(row.reason),
      })),
    };
  } catch (err) {
    ctx.error('getStewardshipLog failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Read: config ----------

async function getEntityConfig(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  let entityId: string;
  try {
    entityId = sanitizeEntityId(req.query.get('entityId'));
  } catch {
    return { status: 400, jsonBody: { error: 'Invalid entityId' } };
  }

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        entity_id AS entityId,
        entity_name AS entityName,
        hub_table AS hubTable,
        is_active AS isActive,
        CAST(match_threshold AS FLOAT) AS matchThreshold,
        CAST(auto_accept_threshold AS FLOAT) AS autoAcceptThreshold
      FROM mdm_config.entity_config
      WHERE entity_id = @entityId
      LIMIT 1
    `, { entityId });

    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Entity config not found' } };
    const row = rows[0];

    return {
      status: 200,
      jsonBody: {
        entityId: asString(row.entityId),
        entityName: asString(row.entityName),
        hubTable: asString(row.hubTable),
        isActive: asBoolean(row.isActive) ?? false,
        matchThreshold: asNumber(row.matchThreshold),
        autoAcceptThreshold: asNumber(row.autoAcceptThreshold),
      },
    };
  } catch (err) {
    ctx.error('getEntityConfig failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

async function getFieldConfigs(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  let entityId: string;
  try {
    entityId = sanitizeEntityId(req.query.get('entityId'));
  } catch {
    return { status: 400, jsonBody: { error: 'Invalid entityId' } };
  }

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        entity_id AS entityId,
        field_name AS fieldName,
        CAST(match_weight AS FLOAT) AS matchWeight,
        is_blocking_key AS isBlockingKey,
        standardizer,
        is_active AS isActive
      FROM mdm_config.field_config
      WHERE entity_id = @entityId
      ORDER BY field_name
    `, { entityId });

    return {
      status: 200,
      jsonBody: rows.map(row => ({
        entityId: asString(row.entityId),
        fieldName: asString(row.fieldName),
        matchWeight: asNumber(row.matchWeight, 0),
        isBlockingKey: asBoolean(row.isBlockingKey) ?? false,
        standardizer: asString(row.standardizer),
        isActive: asBoolean(row.isActive) ?? false,
      })),
    };
  } catch (err) {
    ctx.error('getFieldConfigs failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

async function getSourcePriorities(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  let entityId: string;
  try {
    entityId = sanitizeEntityId(req.query.get('entityId'));
  } catch {
    return { status: 400, jsonBody: { error: 'Invalid entityId' } };
  }

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        entity_id AS entityId,
        source_system AS sourceSystem,
        field_name AS fieldName,
        priority
      FROM mdm_config.source_priority
      WHERE entity_id = @entityId
      ORDER BY field_name, priority
    `, { entityId });

    return {
      status: 200,
      jsonBody: rows.map(row => ({
        entityId: asString(row.entityId),
        sourceSystem: asString(row.sourceSystem),
        fieldName: asString(row.fieldName),
        priority: asNumber(row.priority, 0),
      })),
    };
  } catch (err) {
    ctx.error('getSourcePriorities failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Write: review pair ----------

async function reviewPair(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as {
    pairId: string;
    action: 'accept' | 'reject';
    canonicalHk?: string;
    reason?: string;
  };

  if (!body.pairId || !['accept', 'reject'].includes(body.action)) {
    return { status: 400, jsonBody: { error: 'Invalid request: pairId and action required' } };
  }

  try {
    const pairRows = await querySql<Record<string, unknown>>(`
      SELECT
        CONVERT(VARCHAR(64), hk_left, 2) AS hkLeft,
        CONVERT(VARCHAR(64), hk_right, 2) AS hkRight
      FROM silver_dv.bv_location_match_candidates
      WHERE pair_id = @pairId
    `, { pairId: body.pairId });

    if (pairRows.length === 0) return { status: 404, jsonBody: { error: 'Pair not found' } };

    const hkLeft = asString(pairRows[0].hkLeft)?.toLowerCase();
    const hkRight = asString(pairRows[0].hkRight)?.toLowerCase();
    if (!hkLeft || !hkRight) return { status: 500, jsonBody: { error: 'Pair hash keys are invalid' } };

    const requestedCanonical = sanitizeHex32(body.canonicalHk);
    const canonicalHk = requestedCanonical ?? hkLeft;
    const sourceHk = canonicalHk === hkLeft ? hkRight : hkLeft;
    const status = body.action === 'accept' ? 'accepted' : 'rejected';

    await execSql(`
      UPDATE silver_dv.bv_location_match_candidates
      SET status = @status, reviewed_by = @caller, reviewed_at = GETUTCDATE(), review_note = @reason
      WHERE pair_id = @pairId
    `, { status, caller, reason: body.reason ?? '', pairId: body.pairId });

    if (body.action === 'accept') {
      await execSql(`
        INSERT INTO silver_dv.bv_location_key_resolution
          (source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type)
        SELECT
          CONVERT(VARBINARY(32), @sourceHk, 2),
          CONVERT(VARBINARY(32), @canonicalHk, 2),
          @caller,
          GETUTCDATE(),
          @pairId,
          'manual'
        WHERE NOT EXISTS (
          SELECT 1
          FROM silver_dv.bv_location_key_resolution r
          WHERE r.source_hk = CONVERT(VARBINARY(32), @sourceHk, 2)
        )
      `, { sourceHk, canonicalHk, caller, pairId: body.pairId });
    }

    await execSql(`
      INSERT INTO silver_dv.stewardship_log (log_id, canonical_hk, action, changed_by, changed_at, pair_id, reason)
      VALUES (
        @logId,
        CONVERT(VARBINARY(32), @canonicalHk, 2),
        @action,
        @caller,
        GETUTCDATE(),
        @pairId,
        @reason
      )
    `, {
      logId: crypto.randomUUID(),
      canonicalHk,
      action: body.action === 'accept' ? 'accept_match' : 'reject_match',
      caller,
      pairId: body.pairId,
      reason: body.reason ?? '',
    });

    ctx.log(`Pair ${body.pairId} ${status} by ${caller}`);
    return { status: 200, jsonBody: { ok: true, pairId: body.pairId, status } };
  } catch (err) {
    ctx.error('reviewPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Write: override field ----------

async function overrideField(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as {
    locationHk: string;
    fieldName: string;
    newValue: string;
    reason: string;
  };

  const locationHk = sanitizeHex32(body.locationHk);
  if (!locationHk || !body.fieldName || !body.reason) {
    return { status: 400, jsonBody: { error: 'locationHk (64-hex), fieldName, reason required' } };
  }

  const allowedFields = new Set([
    'name',
    'city',
    'zip_code',
    'country',
    'phone',
    'website_url',
    'timezone',
    'currency_code',
    'cost_center',
    'region',
  ]);

  if (!allowedFields.has(body.fieldName)) {
    return { status: 400, jsonBody: { error: `Field '${body.fieldName}' not allowed` } };
  }

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT ${body.fieldName} AS fieldValue
      FROM gold.dim_location
      WHERE location_hk = CONVERT(VARBINARY(32), @locationHk, 2) AND is_current = 1
    `, { locationHk });

    const oldValue = asString(rows[0]?.fieldValue) ?? '';
    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (log_id, canonical_hk, action, field_name, old_value, new_value, changed_by, changed_at, reason)
      VALUES (
        @logId,
        CONVERT(VARBINARY(32), @locationHk, 2),
        'override_field',
        @fieldName,
        @oldValue,
        @newValue,
        @caller,
        GETUTCDATE(),
        @reason
      )
    `, {
      logId: crypto.randomUUID(),
      locationHk,
      fieldName: body.fieldName,
      oldValue,
      newValue: body.newValue ?? '',
      caller,
      reason: body.reason,
    });

    return { status: 200, jsonBody: { ok: true } };
  } catch (err) {
    ctx.error('overrideField failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Write: create location ----------

interface CreateLocationBody {
  name: string;
  country: string;
  city: string;
  zipCode?: string;
  address?: string;
  phone?: string;
  websiteUrl?: string;
  latitude?: number;
  longitude?: number;
  timezone?: string;
  currencyCode?: string;
  costCenter?: string;
  region?: string;
  notes?: string;
}

function calcCompleteness(body: CreateLocationBody): number {
  const fields = [
    body.name,
    body.country,
    body.city,
    body.zipCode,
    body.address,
    body.phone,
    body.websiteUrl,
    body.timezone,
    body.currencyCode,
    body.costCenter,
  ];
  const filled = fields.filter(v => !!v && String(v).trim().length > 0).length;
  return Math.round((filled / fields.length) * 100) / 100;
}

async function createLocation(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as CreateLocationBody;
  if (!body.name?.trim() || !body.country?.trim() || !body.city?.trim()) {
    return { status: 400, jsonBody: { error: 'name, country, city are required' } };
  }

  const uuid = crypto.randomUUID();
  const businessKey = `manual|${uuid}`;
  const locationHk = crypto.createHash('sha256').update(businessKey).digest('hex');

  const nameStd = body.name.trim().toUpperCase();
  const countryStd = body.country.trim().toUpperCase();
  const cityStd = body.city.trim().toUpperCase();

  const hashDiffInput = JSON.stringify({
    name: body.name,
    country: body.country,
    city: body.city,
    zipCode: body.zipCode,
    address: body.address,
    phone: body.phone,
    websiteUrl: body.websiteUrl,
    lat: body.latitude,
    lon: body.longitude,
    timezone: body.timezone,
    currency: body.currencyCode,
    costCenter: body.costCenter,
    region: body.region,
    notes: body.notes,
  });
  const hashDiff = crypto.createHash('sha256').update(hashDiffInput).digest('hex');

  try {
    await execSql(`
      INSERT INTO silver_dv.hub_location (location_hk, business_key, load_date, record_source)
      VALUES (CONVERT(VARBINARY(32), @locationHk, 2), @businessKey, GETUTCDATE(), 'manual')
    `, { locationHk, businessKey });

    await execSql(`
      INSERT INTO silver_dv.sat_location_manual (
        location_hk, load_date, hash_diff, record_source,
        name, country, city, zip_code, address, phone, website_url,
        latitude, longitude, timezone, currency_code, cost_center, region, notes,
        name_std, country_std, city_std, created_by, created_at
      ) VALUES (
        CONVERT(VARBINARY(32), @locationHk, 2), GETUTCDATE(), CONVERT(VARBINARY(32), @hashDiff, 2), 'manual',
        @name, @country, @city, @zipCode, @address, @phone, @websiteUrl,
        @latitude, @longitude, @timezone, @currencyCode, @costCenter, @region, @notes,
        @nameStd, @countryStd, @cityStd, @caller, GETUTCDATE()
      )
    `, {
      locationHk,
      hashDiff,
      name: body.name,
      country: body.country,
      city: body.city,
      zipCode: body.zipCode ?? '',
      address: body.address ?? '',
      phone: body.phone ?? '',
      websiteUrl: body.websiteUrl ?? '',
      latitude: body.latitude ?? null,
      longitude: body.longitude ?? null,
      timezone: body.timezone ?? '',
      currencyCode: body.currencyCode ?? '',
      costCenter: body.costCenter ?? '',
      region: body.region ?? '',
      notes: body.notes ?? '',
      nameStd,
      countryStd,
      cityStd,
      caller,
    });

    await execSql(`
      INSERT INTO gold.dim_location (
        location_sk, location_hk, valid_from, is_current,
        name, country, city, zip_code, address, phone,
        latitude, longitude, website_url, timezone, currency_code,
        cost_center, region, name_source, country_source, city_source,
        created_at, updated_at
      ) VALUES (
        @locationSk, CONVERT(VARBINARY(32), @locationHk, 2), GETUTCDATE(), 1,
        @name, @country, @city, @zipCode, @address, @phone,
        @latitude, @longitude, @websiteUrl, @timezone, @currencyCode,
        @costCenter, @region, 'manual', 'manual', 'manual',
        GETUTCDATE(), GETUTCDATE()
      )
    `, {
      locationSk: Date.now(),
      locationHk,
      name: body.name,
      country: body.country,
      city: body.city,
      zipCode: body.zipCode ?? '',
      address: body.address ?? '',
      phone: body.phone ?? '',
      latitude: body.latitude ?? null,
      longitude: body.longitude ?? null,
      websiteUrl: body.websiteUrl ?? '',
      timezone: body.timezone ?? '',
      currencyCode: body.currencyCode ?? '',
      costCenter: body.costCenter ?? '',
      region: body.region ?? '',
    });

    await execSql(`
      INSERT INTO gold.dim_location_quality
        (location_hk, snapshot_date, sources_count, completeness_score, has_lightspeed, has_yext, has_mcwin, has_gopos)
      VALUES (
        CONVERT(VARBINARY(32), @locationHk, 2), GETUTCDATE(), 1,
        @completeness, 0, 0, 0, 0
      )
    `, {
      locationHk,
      completeness: calcCompleteness(body),
    });

    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (log_id, canonical_hk, action, changed_by, changed_at, reason)
      VALUES (
        @logId,
        CONVERT(VARBINARY(32), @locationHk, 2),
        'manual_create',
        @caller,
        GETUTCDATE(),
        @reason
      )
    `, {
      logId: crypto.randomUUID(),
      locationHk,
      caller,
      reason: `Manual create: ${body.name}`,
    });

    ctx.log(`Location created: ${locationHk} (${body.name}) by ${caller}`);
    return { status: 201, jsonBody: { ok: true, locationHk, businessKey } };
  } catch (err) {
    ctx.error('createLocation failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Routes ----------

app.http('getQueueStats', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/queue/stats',
  handler: getQueueStats,
});

app.http('getMatchCandidates', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/location/candidates',
  handler: getMatchCandidates,
});

app.http('getPair', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/location/pair/{pairId}',
  handler: getPair,
});

app.http('getGoldenLocations', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/location/golden',
  handler: getGoldenLocations,
});

app.http('getGoldenLocation', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/location/golden/{locationHk}',
  handler: getGoldenLocation,
});

app.http('getStewardshipLog', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/location/log/{locationHk}',
  handler: getStewardshipLog,
});

app.http('getEntityConfig', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/config/entity',
  handler: getEntityConfig,
});

app.http('getFieldConfigs', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/config/field-config',
  handler: getFieldConfigs,
});

app.http('getSourcePriorities', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'mdm/config/source-priority',
  handler: getSourcePriorities,
});

app.http('reviewPair', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'mdm/location/review',
  handler: reviewPair,
});

app.http('overrideField', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'mdm/location/override',
  handler: overrideField,
});

app.http('createLocation', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'mdm/location/create',
  handler: createLocation,
});

app.http('health', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'health',
  handler: async () => ({ status: 200, jsonBody: { status: 'ok' } }),
});
