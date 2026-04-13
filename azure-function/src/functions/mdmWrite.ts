// Azure Function v4 — MDM Write Proxy
// Chroni Fabric Lakehouse przed bezpośrednim zapisem z UI
// Auth: weryfikuje Bearer token Azure AD (ten sam tenant co UI)

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import * as sql from 'tedious';
import * as crypto from 'crypto';

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

async function execSql(query: string, params: Record<string, unknown> = {}): Promise<void> {
  return new Promise((resolve, reject) => {
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
  });
}

async function querySql<T = Record<string, unknown>>(
  query: string,
  params: Record<string, unknown> = {}
): Promise<T[]> {
  return new Promise((resolve, reject) => {
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
  });
}

function getCallerEmail(req: HttpRequest): string {
  const auth = req.headers.get('x-ms-client-principal');
  if (!auth) return 'unknown';
  try {
    const decoded = JSON.parse(Buffer.from(auth, 'base64').toString('utf-8'));
    return decoded.userDetails ?? 'unknown';
  } catch { return 'unknown'; }
}

// ---------- POST /api/mdm/location/review ----------
async function reviewPair(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const body = await req.json() as {
    pairId: string;
    action: 'accept' | 'reject';
    canonicalHk?: string;
    reason?: string;
  };
  if (!body.pairId || !['accept', 'reject'].includes(body.action)) {
    return { status: 400, jsonBody: { error: 'Invalid request: pairId and action required' } };
  }
  const caller = getCallerEmail(req);
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
  const caller = getCallerEmail(req);
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
  const body = await req.json() as CreateLocationBody;

  if (!body.name?.trim() || !body.country?.trim() || !body.city?.trim()) {
    return { status: 400, jsonBody: { error: 'name, country, city are required' } };
  }

  const caller  = getCallerEmail(req);
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
app.http('health', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'health',
  handler: async () => ({ status: 200, jsonBody: { status: 'ok' } }),
});



