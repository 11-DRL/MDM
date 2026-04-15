// Azure Function v4 — MDM Write Proxy
// Chroni Fabric Lakehouse przed bezpośrednim zapisem z UI
// Auth: weryfikuje Bearer token Azure AD (ten sam tenant co UI)

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import * as sql from 'tedious';
import * as crypto from 'crypto';

// ---------- Auth: Bearer token validation ----------
// Weryfikuje że request pochodzi od zalogowanego użytkownika Azure AD.
// Dekoduje JWT (bez weryfikacji podpisu — podpis weryfikuje Azure AD przed SWA).
// Jeśli funkcja jest dostępna tylko przez Azure SWA, token i tak był już zwalidowany.
// Dodatkowa weryfikacja: aud i iss muszą pasować do naszego tenantu.
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

    // Weryfikuj tenant (iss) — opcjonalnie, jeśli zmienna środowiskowa ustawiona
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

// ---------- DB connection ----------
function getConnection(): sql.Connection {
  const config: sql.ConnectionConfiguration = {
    server: process.env.FABRIC_SQL_SERVER!,
    authentication: { type: 'azure-active-directory-msi-app-service', options: {} },
    options: {
      database: process.env.FABRIC_DATABASE ?? 'lh_mdm',
      encrypt: true,
      port: 1433,
    },
  };
  return new sql.Connection(config);
}

// Retry z exponential backoff — 3 próby: 200ms → 400ms → 800ms
async function withRetry<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1) await new Promise(r => setTimeout(r, 200 * Math.pow(2, i)));
    }
  }
  throw lastErr;
}

async function execSql(query: string, params: Record<string, unknown> = {}): Promise<void> {
  return withRetry(() => new Promise((resolve, reject) => {
    const conn = getConnection();
    conn.on('connect', (err) => {
      if (err) return reject(err);
      const req = new sql.Request(query, (err2) => {
        conn.close();
        if (err2) reject(err2); else resolve();
      });
      for (const [name, value] of Object.entries(params)) {
        req.addParameter(name, sql.TYPES.NVarChar, String(value ?? ''));
      }
      conn.execSql(req);
    });
    conn.connect();
  }));
}

async function querySql<T = Record<string, unknown>>(
  query: string,
  params: Record<string, unknown> = {}
): Promise<T[]> {
  return withRetry(() => new Promise((resolve, reject) => {
    const conn = getConnection();
    const rows: T[] = [];
    conn.on('connect', (err) => {
      if (err) return reject(err);
      const req = new sql.Request(query, (err2) => {
        conn.close();
        if (err2) reject(err2); else resolve(rows);
      });
      req.on('row', (columns: Array<{ metadata: { colName: string }; value: unknown }>) => {
        const row: Record<string, unknown> = {};
        columns.forEach(col => { row[col.metadata.colName] = col.value; });
        rows.push(row as T);
      });
      for (const [name, value] of Object.entries(params)) {
        req.addParameter(name, sql.TYPES.NVarChar, String(value ?? ''));
      }
      conn.execSql(req);
    });
    conn.connect();
  }));
}

// ---------- POST /api/mdm/location/review ----------
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
  const status = body.action === 'accept' ? 'accepted' : 'rejected';
  try {
    await execSql(`
      UPDATE silver_dv.bv_location_match_candidates
      SET status = @status, reviewed_by = @caller, reviewed_at = GETUTCDATE(), review_note = @reason
      WHERE pair_id = @pairId
    `, { status, caller, reason: body.reason ?? '', pairId: body.pairId });

    if (body.action === 'accept' && body.canonicalHk) {
      await execSql(`
        INSERT INTO silver_dv.bv_location_key_resolution
          (source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type)
        SELECT hk_right, hk_left, @caller, GETUTCDATE(), @pairId, 'manual'
        FROM silver_dv.bv_location_match_candidates
        WHERE pair_id = @pairId
          AND NOT EXISTS (
            SELECT 1 FROM silver_dv.bv_location_key_resolution r
            WHERE r.source_hk = bv_location_match_candidates.hk_right
          )
      `, { caller, pairId: body.pairId });
    }

    await execSql(`
      INSERT INTO silver_dv.stewardship_log (canonical_hk, action, changed_by, changed_at, pair_id, reason)
      SELECT hk_left, @action, @caller, GETUTCDATE(), @pairId, @reason
      FROM silver_dv.bv_location_match_candidates WHERE pair_id = @pairId
    `, { action: body.action === 'accept' ? 'accept_match' : 'reject_match', caller, pairId: body.pairId, reason: body.reason ?? '' });

    ctx.log(`Pair ${body.pairId} ${status} by ${caller}`);
    return { status: 200, jsonBody: { ok: true, pairId: body.pairId, status } };
  } catch (err) {
    ctx.error('reviewPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- POST /api/mdm/location/override ----------
async function overrideField(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as {
    locationHk: string; fieldName: string; newValue: string; reason: string;
  };
  if (!body.locationHk || !body.fieldName || !body.reason) {
    return { status: 400, jsonBody: { error: 'locationHk, fieldName, reason required' } };
  }
  const ALLOWED_FIELDS = new Set([
    'name', 'city', 'zip_code', 'country', 'phone',
    'website_url', 'timezone', 'currency_code', 'cost_center', 'region',
  ]);
  if (!ALLOWED_FIELDS.has(body.fieldName)) {
    return { status: 400, jsonBody: { error: `Field '${body.fieldName}' not allowed` } };
  }
  try {
    const rows = await querySql<{ fieldValue: string }>(
      `SELECT ${body.fieldName} AS fieldValue FROM gold.dim_location
       WHERE location_hk = CONVERT(BINARY(32), @hk, 2) AND is_current = 1`,
      { hk: body.locationHk }
    );
    const oldValue = rows[0]?.fieldValue ?? '';

    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (canonical_hk, action, field_name, old_value, new_value, changed_by, changed_at, reason)
      VALUES (CONVERT(BINARY(32), @hk, 2), 'override_field', @fieldName, @oldValue, @newValue, @caller, GETUTCDATE(), @reason)
    `, { hk: body.locationHk, fieldName: body.fieldName, oldValue, newValue: body.newValue, caller, reason: body.reason });
    return { status: 200, jsonBody: { ok: true } };
  } catch (err) {
    ctx.error('overrideField failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- POST /api/mdm/location/create ----------
// Ręczne tworzenie nowej lokalizacji z UI — omija pipeline, trafia prosto do Hub + Sat + Gold
interface CreateLocationBody {
  // Podstawowe (wymagane)
  name:          string;
  country:       string;
  city:          string;
  // Kontakt
  zipCode?:      string;
  address?:      string;
  phone?:        string;
  websiteUrl?:   string;
  // Geo
  latitude?:     number;
  longitude?:    number;
  timezone?:     string;
  // Biznesowe
  currencyCode?: string;
  costCenter?:   string;
  region?:       string;
  notes?:        string;
}

async function createLocation(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as CreateLocationBody;

  if (!body.name?.trim() || !body.country?.trim() || !body.city?.trim()) {
    return { status: 400, jsonBody: { error: 'name, country, city are required' } };
  }

  const uuid    = crypto.randomUUID();
  const bizKey  = `manual|${uuid}`;
  // SHA256 hash key (hex) — identyczny wzorzec co nb_load_raw_vault_location.py
  const locationHk = crypto.createHash('sha256').update(bizKey).digest('hex');

  // Standaryzacja (uppercase trim — wzorzec z notebooków)
  const nameStd    = body.name.trim().toUpperCase();
  const countryStd = body.country.trim().toUpperCase();
  const cityStd    = body.city.trim().toUpperCase();

  // hash_diff na atrybuty (dla historyzacji Satellite)
  const hashDiffInput = JSON.stringify({
    name: body.name, country: body.country, city: body.city,
    zipCode: body.zipCode, address: body.address, phone: body.phone,
    websiteUrl: body.websiteUrl, lat: body.latitude, lon: body.longitude,
    timezone: body.timezone, currency: body.currencyCode,
    costCenter: body.costCenter, region: body.region,
  });
  const hashDiff = crypto.createHash('sha256').update(hashDiffInput).digest('hex');

  ctx.log(`Creating location: ${body.name} (${body.city}, ${body.country}) by ${caller}`);

  try {
    // 1. Hub
    await execSql(`
      INSERT INTO silver_dv.hub_location (location_hk, business_key, load_date, record_source)
      VALUES (CONVERT(BINARY(32), @hk, 2), @bizKey, GETUTCDATE(), 'manual')
    `, { hk: locationHk, bizKey });

    // 2. Satellite manual
    await execSql(`
      INSERT INTO silver_dv.sat_location_manual (
        location_hk, load_date, hash_diff, record_source,
        name, country, city, zip_code, address, phone, website_url,
        latitude, longitude, timezone, currency_code, cost_center, region, notes,
        name_std, country_std, city_std, created_by, created_at
      ) VALUES (
        CONVERT(BINARY(32), @hk, 2), GETUTCDATE(), CONVERT(BINARY(32), @hashDiff, 2), 'manual',
        @name, @country, @city, @zipCode, @address, @phone, @websiteUrl,
        @latitude, @longitude, @timezone, @currencyCode, @costCenter, @region, @notes,
        @nameStd, @countryStd, @cityStd, @caller, GETUTCDATE()
      )
    `, {
      hk: locationHk, hashDiff,
      name: body.name, country: body.country, city: body.city,
      zipCode: body.zipCode ?? '', address: body.address ?? '',
      phone: body.phone ?? '', websiteUrl: body.websiteUrl ?? '',
      latitude: String(body.latitude ?? ''), longitude: String(body.longitude ?? ''),
      timezone: body.timezone ?? '', currencyCode: body.currencyCode ?? '',
      costCenter: body.costCenter ?? '', region: body.region ?? '',
      notes: body.notes ?? '', nameStd, countryStd, cityStd, caller,
    });

    // 3. Gold — bezpośrednio (nie czeka na pipeline)
    await execSql(`
      INSERT INTO gold.dim_location (
        location_hk, valid_from, is_current,
        name, country, city, zip_code, address, phone,
        latitude, longitude, website_url, timezone, currency_code,
        cost_center, region, name_source, country_source, city_source,
        created_at, updated_at
      ) VALUES (
        CONVERT(BINARY(32), @hk, 2), GETUTCDATE(), 1,
        @name, @country, @city, @zipCode, @address, @phone,
        @latitude, @longitude, @websiteUrl, @timezone, @currencyCode,
        @costCenter, @region, 'manual', 'manual', 'manual',
        GETUTCDATE(), GETUTCDATE()
      )
    `, {
      hk: locationHk,
      name: body.name, country: body.country, city: body.city,
      zipCode: body.zipCode ?? '', address: body.address ?? '',
      phone: body.phone ?? '',
      latitude: String(body.latitude ?? ''), longitude: String(body.longitude ?? ''),
      websiteUrl: body.websiteUrl ?? '', timezone: body.timezone ?? '',
      currencyCode: body.currencyCode ?? '', costCenter: body.costCenter ?? '',
      region: body.region ?? '',
    });

    // 4. Quality metrics
    await execSql(`
      INSERT INTO gold.dim_location_quality
        (location_hk, snapshot_date, sources_count, completeness_score, has_lightspeed, has_yext, has_mcwin, has_gopos)
      VALUES (
        CONVERT(BINARY(32), @hk, 2), GETUTCDATE(), 1,
        @completeness, 0, 0, 0, 0
      )
    `, {
      hk: locationHk,
      completeness: String(calcCompleteness(body)),
    });

    // 5. Audit log
    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (canonical_hk, action, changed_by, changed_at, reason)
      VALUES (CONVERT(BINARY(32), @hk, 2), 'manual_create', @caller, GETUTCDATE(), @name)
    `, { hk: locationHk, caller, name: body.name });

    ctx.log(`Location created: ${locationHk} — ${body.name}`);
    return {
      status: 201,
      jsonBody: { ok: true, locationHk, bizKey },
    };
  } catch (err) {
    ctx.error('createLocation failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

function calcCompleteness(b: CreateLocationBody): number {
  const fields = [b.name, b.country, b.city, b.zipCode, b.address, b.phone, b.websiteUrl, b.timezone, b.currencyCode, b.costCenter];
  const filled = fields.filter(v => v && String(v).trim() !== '').length;
  return Math.round((filled / fields.length) * 100) / 100;
}

// ---------- GET /api/mdm/location/pair/:pairId ----------
// Pobiera pojedynczą parę do recenzji — zastępuje N+1 load 1000 rekordów w PairDetail
async function getPair(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = requireAuth(req);
  if (!('email' in auth)) return auth;

  const pairId = req.params.pairId;
  if (!pairId) return { status: 400, jsonBody: { error: 'pairId required' } };

  try {
    const rows = await querySql<Record<string, unknown>>(`
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
        -- Left record attributes
        COALESCE(ls_l.name, ys_l.name, ms_l.restaurant_name, gs_l.location_name)    AS leftName,
        COALESCE(ls_l.country, ys_l.country_code, ms_l.country, gs_l.country)       AS leftCountry,
        COALESCE(ls_l.city_std, ys_l.city, ms_l.city, gs_l.city)                    AS leftCity,
        COALESCE(ms_l.zip_code, ys_l.postal_code, gs_l.zip_code)                    AS leftZipCode,
        COALESCE(ys_l.phone, gs_l.phone)                                             AS leftPhone,
        ys_l.website_url                                                             AS leftWebsiteUrl,
        ys_l.avg_rating                                                              AS leftAvgRating,
        ys_l.review_count                                                            AS leftReviewCount,
        ms_l.cost_center                                                             AS leftCostCenter,
        ms_l.region                                                                  AS leftRegion,
        CASE WHEN ls_l.location_hk IS NOT NULL THEN 'lightspeed'
             WHEN ys_l.location_hk IS NOT NULL THEN 'yext'
             WHEN ms_l.location_hk IS NOT NULL THEN 'mcwin'
             ELSE 'gopos' END                                                        AS leftSource,
        -- Right record attributes
        COALESCE(ls_r.name, ys_r.name, ms_r.restaurant_name, gs_r.location_name)    AS rightName,
        COALESCE(ls_r.country, ys_r.country_code, ms_r.country, gs_r.country)       AS rightCountry,
        COALESCE(ls_r.city_std, ys_r.city, ms_r.city, gs_r.city)                    AS rightCity,
        COALESCE(ms_r.zip_code, ys_r.postal_code, gs_r.zip_code)                    AS rightZipCode,
        COALESCE(ys_r.phone, gs_r.phone)                                             AS rightPhone,
        ys_r.website_url                                                             AS rightWebsiteUrl,
        ys_r.avg_rating                                                              AS rightAvgRating,
        ys_r.review_count                                                            AS rightReviewCount,
        ms_r.cost_center                                                             AS rightCostCenter,
        ms_r.region                                                                  AS rightRegion,
        CASE WHEN ls_r.location_hk IS NOT NULL THEN 'lightspeed'
             WHEN ys_r.location_hk IS NOT NULL THEN 'yext'
             WHEN ms_r.location_hk IS NOT NULL THEN 'mcwin'
             ELSE 'gopos' END                                                        AS rightSource
      FROM silver_dv.bv_location_match_candidates mc
      LEFT JOIN silver_dv.sat_location_lightspeed ls_l ON mc.hk_left  = ls_l.location_hk AND ls_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_l ON mc.hk_left  = ys_l.location_hk AND ys_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_l ON mc.hk_left  = ms_l.location_hk AND ms_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_l ON mc.hk_left  = gs_l.location_hk AND gs_l.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_lightspeed ls_r ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_yext       ys_r ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_mcwin      ms_r ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
      LEFT JOIN silver_dv.sat_location_gopos      gs_r ON mc.hk_right = gs_r.location_hk AND gs_r.load_end_date IS NULL
      WHERE mc.pair_id = @pairId
    `, { pairId });

    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Pair not found' } };
    return { status: 200, jsonBody: rows[0] };
  } catch (err) {
    ctx.error('getPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Register routes ----------
app.http('reviewPair', {
  methods: ['POST'], authLevel: 'anonymous',
  route: 'mdm/location/review', handler: reviewPair,
});
app.http('overrideField', {
  methods: ['POST'], authLevel: 'anonymous',
  route: 'mdm/location/override', handler: overrideField,
});
app.http('createLocation', {
  methods: ['POST'], authLevel: 'anonymous',
  route: 'mdm/location/create', handler: createLocation,
});
app.http('getPair', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'mdm/location/pair/{pairId}', handler: getPair,
});
app.http('health', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'health',
  handler: async () => ({ status: 200, jsonBody: { status: 'ok' } }),
});
