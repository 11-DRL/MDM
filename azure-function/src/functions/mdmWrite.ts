// Azure Function v4 — MDM Write Proxy
// Chroni Fabric Lakehouse przed bezpośrednim zapisem z UI
// Auth: weryfikuje Bearer token Azure AD (ten sam tenant co UI)

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import { DefaultAzureCredential } from '@azure/identity';
import * as sql from 'tedious';

// ---------- DB connection (Fabric SQL Endpoint przez tedious) ----------
function getConnection(): sql.Connection {
  const config: sql.ConnectionConfiguration = {
    server: process.env.FABRIC_SQL_SERVER!,  // {workspace}.{region}.pbidedicated.windows.net
    authentication: {
      type: 'azure-active-directory-msi-app-service',
      options: {}
    },
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

// ---------- Helper: get caller identity from token ----------
function getCallerEmail(req: HttpRequest): string {
  const auth = req.headers.get('x-ms-client-principal');
  if (!auth) return 'unknown';
  try {
    const decoded = JSON.parse(Buffer.from(auth, 'base64').toString('utf-8'));
    return decoded.userDetails ?? 'unknown';
  } catch {
    return 'unknown';
  }
}

// ---------- POST /api/mdm/location/review ----------
// Body: { pairId, action: 'accept'|'reject', canonicalHk?, reason? }
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
    // 1. Zaktualizuj status pary
    await execSql(`
      UPDATE silver_dv.bv_location_match_candidates
      SET status = @status, reviewed_by = @caller, reviewed_at = GETUTCDATE(), review_note = @reason
      WHERE pair_id = @pairId
    `, { status, caller, reason: body.reason ?? '', pairId: body.pairId });

    // 2. Jeśli accept: zapisz key resolution
    if (body.action === 'accept' && body.canonicalHk) {
      // Pobierz hk_right (source do zastąpienia)
      // W uproszczeniu: canonical = hk_left (wyższy priorytet), source = hk_right
      await execSql(`
        INSERT INTO silver_dv.bv_location_key_resolution
          (source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type)
        SELECT
          hk_right,
          hk_left,
          @caller,
          GETUTCDATE(),
          @pairId,
          'manual'
        FROM silver_dv.bv_location_match_candidates
        WHERE pair_id = @pairId
          AND NOT EXISTS (
            SELECT 1 FROM silver_dv.bv_location_key_resolution r
            WHERE r.source_hk = bv_location_match_candidates.hk_right
          )
      `, { caller, pairId: body.pairId });
    }

    // 3. Audit log
    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (canonical_hk, action, changed_by, changed_at, pair_id, reason)
      SELECT
        hk_left,
        @action,
        @caller,
        GETUTCDATE(),
        @pairId,
        @reason
      FROM silver_dv.bv_location_match_candidates
      WHERE pair_id = @pairId
    `, { action: body.action === 'accept' ? 'accept_match' : 'reject_match', caller, pairId: body.pairId, reason: body.reason ?? '' });

    ctx.log(`Pair ${body.pairId} ${status} by ${caller}`);
    return { status: 200, jsonBody: { ok: true, pairId: body.pairId, status } };

  } catch (err) {
    ctx.error('reviewPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- POST /api/mdm/location/override ----------
// Body: { locationHk, fieldName, newValue, reason }
async function overrideField(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const body = await req.json() as {
    locationHk: string;
    fieldName: string;
    newValue: string;
    reason: string;
  };

  if (!body.locationHk || !body.fieldName || !body.reason) {
    return { status: 400, jsonBody: { error: 'locationHk, fieldName, reason required' } };
  }

  // Whitelist dopuszczalnych pól (zapobiega SQL injection przez field name)
  const ALLOWED_FIELDS = new Set([
    'name', 'city', 'zip_code', 'country', 'phone',
    'website_url', 'timezone', 'currency_code', 'cost_center', 'region'
  ]);
  if (!ALLOWED_FIELDS.has(body.fieldName)) {
    return { status: 400, jsonBody: { error: `Field '${body.fieldName}' not allowed` } };
  }

  const caller = getCallerEmail(req);

  try {
    // Pobierz starą wartość dla audit log
    // Zapisz override do audit log (golden record jest re-derivowany przez notebook)
    await execSql(`
      INSERT INTO silver_dv.stewardship_log
        (canonical_hk, action, field_name, new_value, changed_by, changed_at, reason)
      VALUES
        (CONVERT(BINARY(32), @hk, 2), 'override_field', @fieldName, @newValue, @caller, GETUTCDATE(), @reason)
    `, {
      hk: body.locationHk,
      fieldName: body.fieldName,
      newValue: body.newValue,
      caller,
      reason: body.reason,
    });

    ctx.log(`Field override: ${body.fieldName} on ${body.locationHk} by ${caller}`);
    return { status: 200, jsonBody: { ok: true } };

  } catch (err) {
    ctx.error('overrideField failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Register routes ----------
app.http('reviewPair', {
  methods: ['POST'],
  authLevel: 'anonymous',   // auth przez Azure AD Easy Auth (SWA)
  route: 'mdm/location/review',
  handler: reviewPair,
});

app.http('overrideField', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'mdm/location/override',
  handler: overrideField,
});

// Health check
app.http('health', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'health',
  handler: async () => ({ status: 200, jsonBody: { status: 'ok' } }),
});
