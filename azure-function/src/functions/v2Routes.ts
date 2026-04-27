// V2 API — Generic entity-aware endpoints.
// All routes live under /api/v2/entities/{entityId}/...
// Old v1 routes in mdmWrite.ts continue to work (backward compat for business_location).

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import * as crypto from 'crypto';
import { S } from '../lib/schemas';
import { requireAuth } from '../lib/auth';
import { querySql, execSql } from '../lib/sqlHelpers';
import {
  asString, asNumber, asBoolean, asIso,
  sanitizeHex32, parsePositiveInt, validateEntityId,
} from '../lib/helpers';
import {
  getEntityMeta,
  getFieldMetas,
  getSourceMetas,
  getAllEntities,
  type EntityMeta,
  type FieldMeta,
  type SourceMeta,
} from '../lib/entityMeta';

// =====================================================================
// V2 Endpoints
// =====================================================================

// ---------- GET /api/v2/entities — list all active entities ----------

async function v2ListEntities(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;
  try {
    const entities = await getAllEntities(querySql);
    return {
      status: 200,
      jsonBody: entities.map(e => ({
        entityId: e.entityId,
        entityName: e.entityName,
        displayLabelPl: e.displayLabelPl,
        displayLabelEn: e.displayLabelEn,
        icon: e.icon,
        displayOrder: e.displayOrder,
        matchEngine: e.matchEngine,
        hasMatching: e.matchEngine !== 'none',
      })),
    };
  } catch (err) {
    ctx.error('v2ListEntities failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- GET /api/v2/entities/{entityId}/schema — field config for UI rendering ----------

async function v2GetEntitySchema(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };

  try {
    const [meta, fields, sources] = await Promise.all([
      getEntityMeta(entityId, querySql),
      getFieldMetas(entityId, querySql),
      getSourceMetas(entityId, querySql),
    ]);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    return {
      status: 200,
      jsonBody: {
        entity: {
          entityId: meta.entityId,
          entityName: meta.entityName,
          displayLabelPl: meta.displayLabelPl,
          displayLabelEn: meta.displayLabelEn,
          icon: meta.icon,
          matchEngine: meta.matchEngine,
          hasMatching: meta.matchEngine !== 'none',
        },
        fields: fields.map(f => ({
          fieldName: f.fieldName,
          displayNamePl: f.displayNamePl,
          displayNameEn: f.displayNameEn,
          displayOrder: f.displayOrder,
          uiWidget: f.uiWidget,
          isOverridable: f.isOverridable,
          isRequired: f.isRequired,
          validators: f.validatorsJson ? JSON.parse(f.validatorsJson) : [],
          lookupEntity: f.lookupEntity,
          lookupField: f.lookupField,
          isGoldenField: f.isGoldenField,
          groupName: f.groupName,
        })),
        sources: sources.map(s => s.sourceSystem),
      },
    };
  } catch (err) {
    ctx.error('v2GetEntitySchema failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- GET /api/v2/entities/{entityId}/golden — list golden records ----------

async function v2GetGoldenRecords(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };

  const page = parsePositiveInt(req.query.get('page'), 1, 1, 10_000);
  const pageSize = parsePositiveInt(req.query.get('pageSize'), 25, 1, 200);
  const offset = (page - 1) * pageSize;

  try {
    const meta = await getEntityMeta(entityId, querySql);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    const fields = await getFieldMetas(entityId, querySql);
    const goldenFields = fields.filter(f => f.isGoldenField);
    const goldTable = `${S.gold}.${meta.goldTable}`;
    const hk = meta.hkColumn;

    // Build SELECT columns from golden fields
    const selectCols = goldenFields.map(f => f.fieldName).join(', ');

    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        CONVERT(VARCHAR(64), ${hk}, 2) AS hk,
        ${selectCols},
        valid_from, valid_to, is_current, created_at, updated_at
      FROM ${goldTable}
      WHERE is_current = 1
      ORDER BY created_at DESC
      OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY
    `);

    const countRows = await querySql<Record<string, unknown>>(
      `SELECT COUNT(*) AS total FROM ${goldTable} WHERE is_current = 1`
    );

    const items = rows.map(row => {
      const attrs: Record<string, unknown> = {};
      for (const f of goldenFields) {
        attrs[f.fieldName] = row[f.fieldName] ?? null;
      }
      return {
        hk: asString(row.hk),
        attributes: attrs,
        validFrom: asIso(row.valid_from),
        validTo: asIso(row.valid_to),
        isCurrent: asBoolean(row.is_current) ?? true,
      };
    });

    return {
      status: 200,
      jsonBody: { items, total: asNumber(countRows[0]?.total), page, pageSize },
    };
  } catch (err) {
    ctx.error('v2GetGoldenRecords failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- GET /api/v2/entities/{entityId}/golden/{hk} — single golden record ----------

async function v2GetGoldenRecord(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };
  const hk = sanitizeHex32(req.params.hk);
  if (!hk) return { status: 400, jsonBody: { error: 'Invalid hk (64-char hex)' } };

  try {
    const meta = await getEntityMeta(entityId, querySql);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    const fields = await getFieldMetas(entityId, querySql);
    const goldenFields = fields.filter(f => f.isGoldenField);
    const goldTable = `${S.gold}.${meta.goldTable}`;
    const hkCol = meta.hkColumn;
    const selectCols = goldenFields.map(f => f.fieldName).join(', ');

    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        CONVERT(VARCHAR(64), ${hkCol}, 2) AS hk,
        ${selectCols},
        valid_from, valid_to, is_current, created_at, updated_at
      FROM ${goldTable}
      WHERE ${hkCol} = CONVERT(VARBINARY(32), @hk, 2) AND is_current = 1
    `, { hk });

    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Record not found' } };
    const row = rows[0];

    const attrs: Record<string, unknown> = {};
    for (const f of goldenFields) attrs[f.fieldName] = row[f.fieldName] ?? null;

    return {
      status: 200,
      jsonBody: {
        hk: asString(row.hk),
        entityId,
        attributes: attrs,
        validFrom: asIso(row.valid_from),
        validTo: asIso(row.valid_to),
        isCurrent: true,
      },
    };
  } catch (err) {
    ctx.error('v2GetGoldenRecord failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- GET /api/v2/entities/{entityId}/queue/stats ----------

async function v2GetQueueStats(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };

  try {
    const meta = await getEntityMeta(entityId, querySql);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    if (!meta.bvMatchTable) {
      // Entity has no matching — return zero stats
      const goldCount = await querySql<Record<string, unknown>>(
        `SELECT COUNT(*) AS total FROM ${S.gold}.${meta.goldTable} WHERE is_current = 1`
      );
      return {
        status: 200,
        jsonBody: {
          pendingCount: 0, autoAcceptedCount: 0, acceptedCount: 0, rejectedCount: 0,
          totalGoldenRecords: asNumber(goldCount[0]?.total),
          avgCompletenessScore: 0,
        },
      };
    }

    const bvTable = `${S.silver}.${meta.bvMatchTable}`;
    const goldTable = `${S.gold}.${meta.goldTable}`;
    const qualityTable = `${S.gold}.${meta.qualityTable}`;

    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        COALESCE(SUM(CASE WHEN status = 'pending'       THEN 1 ELSE 0 END), 0) AS pendingCount,
        COALESCE(SUM(CASE WHEN status = 'auto_accepted' THEN 1 ELSE 0 END), 0) AS autoAcceptedCount,
        COALESCE(SUM(CASE WHEN status = 'accepted'      THEN 1 ELSE 0 END), 0) AS acceptedCount,
        COALESCE(SUM(CASE WHEN status = 'rejected'      THEN 1 ELSE 0 END), 0) AS rejectedCount,
        (SELECT COUNT(*) FROM ${goldTable} WHERE is_current = 1) AS totalGoldenRecords,
        COALESCE(CAST((SELECT AVG(completeness_score) FROM ${qualityTable}) AS FLOAT), 0.0) AS avgCompletenessScore
      FROM ${bvTable}
      WHERE entity_id = @entityId
    `, { entityId });

    const row = rows[0] ?? {};
    return {
      status: 200,
      jsonBody: {
        pendingCount: asNumber(row.pendingCount),
        autoAcceptedCount: asNumber(row.autoAcceptedCount),
        acceptedCount: asNumber(row.acceptedCount),
        rejectedCount: asNumber(row.rejectedCount),
        totalGoldenRecords: asNumber(row.totalGoldenRecords),
        avgCompletenessScore: asNumber(row.avgCompletenessScore),
      },
    };
  } catch (err) {
    ctx.error('v2GetQueueStats failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- GET /api/v2/entities/{entityId}/log/{hk} — stewardship log ----------

async function v2GetLog(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };
  const hk = sanitizeHex32(req.params.hk);
  if (!hk) return { status: 400, jsonBody: { error: 'Invalid hk' } };

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT TOP 100
        log_id AS logId,
        CONVERT(VARCHAR(64), canonical_hk, 2) AS canonicalHk,
        action, field_name AS fieldName,
        old_value AS oldValue, new_value AS newValue,
        changed_by AS changedBy, changed_at AS changedAt,
        pair_id AS pairId, reason
      FROM ${S.silver}.stewardship_log
      WHERE canonical_hk = CONVERT(VARBINARY(32), @hk, 2)
        AND entity_id = @entityId
      ORDER BY changed_at DESC
    `, { hk, entityId });

    return {
      status: 200,
      jsonBody: rows.map(r => ({
        logId: asString(r.logId), canonicalHk: asString(r.canonicalHk),
        action: asString(r.action), fieldName: asString(r.fieldName),
        oldValue: asString(r.oldValue), newValue: asString(r.newValue),
        changedBy: asString(r.changedBy), changedAt: asIso(r.changedAt),
        pairId: asString(r.pairId), reason: asString(r.reason),
      })),
    };
  } catch (err) {
    ctx.error('v2GetLog failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- POST /api/v2/entities/{entityId}/override — generic field override ----------

async function v2OverrideField(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };

  const body = await req.json() as {
    hk: string; fieldName: string; newValue: string; reason: string; expectedOldValue?: string | null;
  };
  const hk = sanitizeHex32(body.hk);
  if (!hk || !body.fieldName || !body.reason) {
    return { status: 400, jsonBody: { error: 'hk, fieldName, reason required' } };
  }

  try {
    const meta = await getEntityMeta(entityId, querySql);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    // Load allowed overridable fields from config
    const fields = await getFieldMetas(entityId, querySql);
    const overridableFields = new Set(fields.filter(f => f.isOverridable).map(f => f.fieldName));
    if (!overridableFields.has(body.fieldName)) {
      return { status: 400, jsonBody: { error: `Field '${body.fieldName}' not overridable for entity '${entityId}'` } };
    }

    const goldTable = `${S.gold}.${meta.goldTable}`;
    const hkCol = meta.hkColumn;

    // Read current value
    const rows = await querySql<Record<string, unknown>>(
      `SELECT ${body.fieldName} AS fieldValue FROM ${goldTable} WHERE ${hkCol} = CONVERT(VARBINARY(32), @hk, 2) AND is_current = 1`,
      { hk }
    );
    if (rows.length === 0) return { status: 404, jsonBody: { error: 'Record not found' } };

    const oldValue = asString(rows[0]?.fieldValue) ?? '';

    // Optimistic concurrency check
    if (body.expectedOldValue !== undefined && body.expectedOldValue !== null) {
      if (oldValue !== String(body.expectedOldValue)) {
        return { status: 412, jsonBody: { error: 'Value changed', currentValue: oldValue, expectedValue: body.expectedOldValue } };
      }
    }

    // Update gold + log
    await execSql(`
      UPDATE ${goldTable}
      SET ${body.fieldName} = @newValue, updated_at = GETUTCDATE()
      WHERE ${hkCol} = CONVERT(VARBINARY(32), @hk, 2) AND is_current = 1;

      INSERT INTO ${S.silver}.stewardship_log
        (log_id, canonical_hk, entity_id, action, field_name, old_value, new_value, changed_by, changed_at, reason)
      VALUES (@logId, CONVERT(VARBINARY(32), @hk, 2), @entityId, 'override_field', @fieldName, @oldValue, @newValue, @caller, GETUTCDATE(), @reason);
    `, {
      newValue: body.newValue ?? '',
      hk,
      entityId,
      logId: crypto.randomUUID(),
      fieldName: body.fieldName,
      oldValue,
      caller,
      reason: body.reason,
    });

    return { status: 200, jsonBody: { ok: true } };
  } catch (err) {
    ctx.error('v2OverrideField failed', err);
    return { status: 500, jsonBody: { error: 'Internal error' } };
  }
}

// ---------- POST /api/v2/entities/{entityId}/create — generic entity record creation ----------

async function v2CreateRecord(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const entityId = validateEntityId(req.params.entityId);
  if (!entityId) return { status: 400, jsonBody: { error: 'Invalid entityId' } };

  const body = await req.json() as { attributes: Record<string, unknown> };
  if (!body.attributes || typeof body.attributes !== 'object') {
    return { status: 400, jsonBody: { error: 'attributes object required' } };
  }

  try {
    const meta = await getEntityMeta(entityId, querySql);
    if (!meta) return { status: 404, jsonBody: { error: 'Entity not found' } };

    const fields = await getFieldMetas(entityId, querySql);
    const goldenFields = fields.filter(f => f.isGoldenField);

    // Validate required fields
    for (const f of goldenFields) {
      if (f.isRequired && !body.attributes[f.fieldName]) {
        return { status: 400, jsonBody: { error: `Field '${f.fieldName}' is required` } };
      }
    }

    const uuid = crypto.randomUUID();
    const businessKey = `manual|${uuid}`;
    const hkHex = crypto.createHash('sha256').update(businessKey).digest('hex');

    // Build hash_diff from all golden field values
    const hashInput = goldenFields.map(f => String(body.attributes[f.fieldName] ?? '')).join('|');
    const hashDiff = crypto.createHash('sha256').update(hashInput).digest('hex');

    // Build dynamic SQL for Hub + Satellite + Gold + Quality + Log
    const hubTable = `${S.silver}.${meta.hubTable}`;
    const hkCol = meta.hkColumn;
    const satTable = `${S.silver}.sat_${entityId}_manual`;
    const goldTable = `${S.gold}.${meta.goldTable}`;
    const qualityTable = `${S.gold}.${meta.qualityTable}`;

    // Satellite columns & values
    const satCols = goldenFields.map(f => f.fieldName);
    const satColsSql = satCols.join(', ');
    const satValsSql = satCols.map(c => `@attr_${c}`).join(', ');

    // Gold columns & values (same as sat + SCD2 control)
    const goldColsSql = satCols.join(', ');
    const goldValsSql = satCols.map(c => `@attr_${c}`).join(', ');

    // Completeness = % of non-null golden fields
    const filledCount = goldenFields.filter(f => {
      const v = body.attributes[f.fieldName];
      return v !== null && v !== undefined && String(v).trim().length > 0;
    }).length;
    const completeness = Math.round((filledCount / (goldenFields.length || 1)) * 100) / 100;

    const params: Record<string, unknown> = {
      hk: hkHex,
      businessKey,
      hashDiff,
      entityId,
      caller,
      sk: Date.now(),
      completeness,
      logId: crypto.randomUUID(),
    };

    // Add attribute params
    for (const f of goldenFields) {
      params[`attr_${f.fieldName}`] = body.attributes[f.fieldName] ?? null;
    }

    await execSql(`
      SET XACT_ABORT ON;
      BEGIN TRANSACTION;

      INSERT INTO ${hubTable} (${hkCol}, business_key, entity_id, load_date, record_source)
      VALUES (CONVERT(VARBINARY(32), @hk, 2), @businessKey, @entityId, GETUTCDATE(), 'manual');

      INSERT INTO ${satTable} (${hkCol}, load_date, hash_diff, record_source, ${satColsSql}, created_by, created_at)
      VALUES (CONVERT(VARBINARY(32), @hk, 2), GETUTCDATE(), CONVERT(VARBINARY(32), @hashDiff, 2), 'manual', ${satValsSql}, @caller, GETUTCDATE());

      INSERT INTO ${goldTable} (${hkCol}, valid_from, is_current, ${goldColsSql}, created_at, updated_at)
      VALUES (CONVERT(VARBINARY(32), @hk, 2), GETUTCDATE(), 1, ${goldValsSql}, GETUTCDATE(), GETUTCDATE());

      INSERT INTO ${qualityTable} (${hkCol}, snapshot_date, sources_count, completeness_score)
      VALUES (CONVERT(VARBINARY(32), @hk, 2), GETUTCDATE(), 1, @completeness);

      INSERT INTO ${S.silver}.stewardship_log (log_id, canonical_hk, entity_id, action, changed_by, changed_at, reason)
      VALUES (@logId, CONVERT(VARBINARY(32), @hk, 2), @entityId, 'manual_create', @caller, GETUTCDATE(), 'Manual create');

      COMMIT TRANSACTION;
    `, params);

    ctx.log(`v2 Created ${entityId} record: ${hkHex} by ${caller}`);
    return { status: 201, jsonBody: { ok: true, hk: hkHex, businessKey } };
  } catch (err) {
    ctx.error('v2CreateRecord failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// =====================================================================
// V2 Route registrations
// =====================================================================

app.http('v2ListEntities', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities',
  handler: v2ListEntities,
});

app.http('v2GetEntitySchema', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/schema',
  handler: v2GetEntitySchema,
});

app.http('v2GetGoldenRecords', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/golden',
  handler: v2GetGoldenRecords,
});

app.http('v2GetGoldenRecord', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/golden/{hk}',
  handler: v2GetGoldenRecord,
});

app.http('v2GetQueueStats', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/queue/stats',
  handler: v2GetQueueStats,
});

app.http('v2GetLog', {
  methods: ['GET'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/log/{hk}',
  handler: v2GetLog,
});

app.http('v2OverrideField', {
  methods: ['POST'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/override',
  handler: v2OverrideField,
});

app.http('v2CreateRecord', {
  methods: ['POST'], authLevel: 'anonymous',
  route: 'v2/entities/{entityId}/create',
  handler: v2CreateRecord,
});
