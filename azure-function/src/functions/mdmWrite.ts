// Azure Function v4 - MDM API proxy for Fabric Lakehouse
// Read + write traffic goes through this API. DB connectivity uses Managed Identity.

import { app, HttpRequest, HttpResponseInit, InvocationContext } from '@azure/functions';
import * as crypto from 'crypto';
import { S } from '../lib/schemas';
import { requireAuth, validateBearerToken, type AuthResult } from '../lib/auth';
import {
  getSqlAccessToken, getConnection, sqlTypeForValue, addSqlParameters,
  withRetry, execSql, execSqlWithRowCount, querySql,
} from '../lib/sqlHelpers';
import {
  asString, asNumber, asBoolean, asIso,
  sanitizeHex32, parsePositiveInt, parseStatus,
  sanitizeEntityId, toMatchSource,
  type MatchSource, type MatchStatus,
} from '../lib/helpers';

// Re-export for tests that import from mdmWrite
export { sanitizeHex32, parsePositiveInt, parseStatus, sanitizeEntityId, toMatchSource, validateBearerToken };

type QueryRows = Record<string, unknown>[];

// ---------- Read: queue stats ----------

async function getQueueStats(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;

  try {
    const rows = await querySql<Record<string, unknown>>(`
      SELECT
        COALESCE(SUM(CASE WHEN status = 'pending'       THEN 1 ELSE 0 END), 0) AS pendingCount,
        COALESCE(SUM(CASE WHEN status = 'auto_accepted' THEN 1 ELSE 0 END), 0) AS autoAcceptedCount,
        COALESCE(SUM(CASE WHEN status = 'accepted'      THEN 1 ELSE 0 END), 0) AS acceptedCount,
        COALESCE(SUM(CASE WHEN status = 'rejected'      THEN 1 ELSE 0 END), 0) AS rejectedCount,
        (SELECT COUNT(*) FROM ${S.gold}.dim_location WHERE is_current = 1)           AS totalGoldenRecords,
        COALESCE(CAST((SELECT AVG(completeness_score) FROM ${S.gold}.dim_location_quality) AS FLOAT), 0.0) AS avgCompletenessScore
      FROM ${S.silver}.bv_location_match_candidates
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
  const auth = await requireAuth(req);
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
      FROM ${S.silver}.bv_location_match_candidates mc
      LEFT JOIN ${S.silver}.sat_location_lightspeed ls_l ON mc.hk_left = ls_l.location_hk AND ls_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_yext       ys_l ON mc.hk_left = ys_l.location_hk AND ys_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_mcwin      ms_l ON mc.hk_left = ms_l.location_hk AND ms_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_gopos      gs_l ON mc.hk_left = gs_l.location_hk AND gs_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_manual     man_l ON mc.hk_left = man_l.location_hk AND man_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_lightspeed ls_r ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_yext       ys_r ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_mcwin      ms_r ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_gopos      gs_r ON mc.hk_right = gs_r.location_hk AND gs_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_manual     man_r ON mc.hk_right = man_r.location_hk AND man_r.load_end_date IS NULL
      ${whereClause}
      ORDER BY mc.match_score DESC
      OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY
    `);

    const countRows = await querySql<Record<string, unknown>>(`
      SELECT COUNT(*) AS total
      FROM ${S.silver}.bv_location_match_candidates mc
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
  const auth = await requireAuth(req);
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
      FROM ${S.silver}.bv_location_match_candidates mc
      LEFT JOIN ${S.silver}.sat_location_lightspeed ls_l ON mc.hk_left  = ls_l.location_hk AND ls_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_yext       ys_l ON mc.hk_left  = ys_l.location_hk AND ys_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_mcwin      ms_l ON mc.hk_left  = ms_l.location_hk AND ms_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_gopos      gs_l ON mc.hk_left  = gs_l.location_hk AND gs_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_manual     man_l ON mc.hk_left = man_l.location_hk AND man_l.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_lightspeed ls_r ON mc.hk_right = ls_r.location_hk AND ls_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_yext       ys_r ON mc.hk_right = ys_r.location_hk AND ys_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_mcwin      ms_r ON mc.hk_right = ms_r.location_hk AND ms_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_gopos      gs_r ON mc.hk_right = gs_r.location_hk AND gs_r.load_end_date IS NULL
      LEFT JOIN ${S.silver}.sat_location_manual     man_r ON mc.hk_right = man_r.location_hk AND man_r.load_end_date IS NULL
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
  const auth = await requireAuth(req);
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
        FROM ${S.gold}.dim_location_quality
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
      FROM ${S.gold}.dim_location g
      LEFT JOIN quality_latest q
        ON g.location_hk = q.location_hk AND q.rn = 1
      WHERE g.is_current = 1
      ORDER BY g.country, g.city, g.name
      OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY
    `);

    const countRows = await querySql<Record<string, unknown>>(`
      SELECT COUNT(*) AS total FROM ${S.gold}.dim_location WHERE is_current = 1
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
  const auth = await requireAuth(req);
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
        FROM ${S.gold}.dim_location_quality
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
      FROM ${S.gold}.dim_location g
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
  const auth = await requireAuth(req);
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
      FROM ${S.silver}.stewardship_log
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
  const auth = await requireAuth(req);
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
      FROM ${S.config}.entity_config
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
  const auth = await requireAuth(req);
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
      FROM ${S.config}.field_config
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
  const auth = await requireAuth(req);
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
      FROM ${S.config}.source_priority
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
  const auth = await requireAuth(req);
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
        CONVERT(VARCHAR(64), hk_right, 2) AS hkRight,
        status,
        reviewed_by  AS reviewedBy,
        reviewed_at  AS reviewedAt
      FROM ${S.silver}.bv_location_match_candidates
      WHERE pair_id = @pairId
    `, { pairId: body.pairId });

    if (pairRows.length === 0) return { status: 404, jsonBody: { error: 'Pair not found' } };

    const currentStatus = asString(pairRows[0].status);
    if (currentStatus !== 'pending') {
      // Optimistic concurrency: ktoś inny zdążył zrecenzować tę parę.
      return {
        status: 409,
        jsonBody: {
          error: 'Pair already reviewed',
          currentStatus,
          reviewedBy: asString(pairRows[0].reviewedBy),
          reviewedAt: asIso(pairRows[0].reviewedAt),
        },
      };
    }

    const hkLeft = asString(pairRows[0].hkLeft)?.toLowerCase();
    const hkRight = asString(pairRows[0].hkRight)?.toLowerCase();
    if (!hkLeft || !hkRight) return { status: 500, jsonBody: { error: 'Pair hash keys are invalid' } };

    const requestedCanonical = sanitizeHex32(body.canonicalHk);
    const canonicalHk = requestedCanonical ?? hkLeft;
    const sourceHk = canonicalHk === hkLeft ? hkRight : hkLeft;
    const status = body.action === 'accept' ? 'accepted' : 'rejected';
    const logId = crypto.randomUUID();
    const logAction = body.action === 'accept' ? 'accept_match' : 'reject_match';
    const isAccept = body.action === 'accept' ? 1 : 0;

    // Atomowy batch: UPDATE (guarded) + warunkowy INSERT resolution + INSERT log.
    // XACT_ABORT ON gwarantuje rollback przy dowolnym błędzie (np. constraint violation).
    // Własny RAISERROR 50409 sygnalizuje wyścig (rowCount=0 po UPDATE).
    try {
      const affected = await execSqlWithRowCount(`
        SET XACT_ABORT ON;
        BEGIN TRANSACTION;

        UPDATE ${S.silver}.bv_location_match_candidates
        SET status = @status, reviewed_by = @caller, reviewed_at = GETUTCDATE(), review_note = @reason
        WHERE pair_id = @pairId AND status = 'pending';

        IF @@ROWCOUNT = 0
        BEGIN
          ROLLBACK TRANSACTION;
          RAISERROR('CONCURRENCY_CONFLICT', 16, 1);
          RETURN;
        END

        IF @isAccept = 1
        BEGIN
          INSERT INTO ${S.silver}.bv_location_key_resolution
            (source_hk, canonical_hk, resolved_by, resolved_at, pair_id, resolution_type)
          SELECT
            CONVERT(VARBINARY(32), @sourceHk, 2),
            CONVERT(VARBINARY(32), @canonicalHk, 2),
            @caller,
            GETUTCDATE(),
            @pairId,
            'manual'
          WHERE NOT EXISTS (
            SELECT 1 FROM ${S.silver}.bv_location_key_resolution r
            WHERE r.source_hk = CONVERT(VARBINARY(32), @sourceHk, 2)
          );
        END

        INSERT INTO ${S.silver}.stewardship_log (log_id, canonical_hk, action, changed_by, changed_at, pair_id, reason)
        VALUES (
          @logId,
          CONVERT(VARBINARY(32), @canonicalHk, 2),
          @logAction,
          @caller,
          GETUTCDATE(),
          @pairId,
          @reason
        );

        COMMIT TRANSACTION;
      `, {
        status,
        caller,
        reason: body.reason ?? '',
        pairId: body.pairId,
        isAccept,
        sourceHk,
        canonicalHk,
        logId,
        logAction,
      });

      // Guard: upewnij się że transakcja faktycznie coś zmieniła (ochrona przed zwróceniem 200 gdyby batch nic nie wykonał)
      if (affected === 0) {
        ctx.warn(`reviewPair: batch reported rowCount=0 for pair ${body.pairId}`);
      }
    } catch (err) {
      const msg = String((err as Error)?.message ?? err);
      if (msg.includes('CONCURRENCY_CONFLICT')) {
        const latest = await querySql<Record<string, unknown>>(`
          SELECT status, reviewed_by AS reviewedBy, reviewed_at AS reviewedAt
          FROM ${S.silver}.bv_location_match_candidates
          WHERE pair_id = @pairId
        `, { pairId: body.pairId });
        return {
          status: 409,
          jsonBody: {
            error: 'Pair already reviewed',
            currentStatus: asString(latest[0]?.status),
            reviewedBy: asString(latest[0]?.reviewedBy),
            reviewedAt: asIso(latest[0]?.reviewedAt),
          },
        };
      }
      throw err;
    }

    ctx.log(`Pair ${body.pairId} ${status} by ${caller}`);
    return { status: 200, jsonBody: { ok: true, pairId: body.pairId, status } };
  } catch (err) {
    ctx.error('reviewPair failed', err);
    return { status: 500, jsonBody: { error: 'Internal error', detail: String(err) } };
  }
}

// ---------- Write: override field ----------

async function overrideField(req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> {
  const auth = await requireAuth(req);
  if (!('email' in auth)) return auth;
  const caller = auth.email;

  const body = await req.json() as {
    locationHk: string;
    fieldName: string;
    newValue: string;
    reason: string;
    expectedOldValue?: string | null;
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
      FROM ${S.gold}.dim_location
      WHERE location_hk = CONVERT(VARBINARY(32), @locationHk, 2) AND is_current = 1
    `, { locationHk });

    const oldValue = asString(rows[0]?.fieldValue) ?? '';

    // Optimistic concurrency: jeśli UI dostarczył expectedOldValue, sprawdź czy nic się nie zmieniło.
    // Null/undefined w expectedOldValue = klient rezygnuje ze sprawdzenia (backward-compatible).
    if (body.expectedOldValue !== undefined && body.expectedOldValue !== null) {
      const expected = String(body.expectedOldValue);
      if (oldValue !== expected) {
        return {
          status: 412,
          jsonBody: {
            error: 'Field value changed since last read',
            currentValue: oldValue,
            expectedValue: expected,
          },
        };
      }
    }
    await execSql(`
      INSERT INTO ${S.silver}.stewardship_log
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
  const auth = await requireAuth(req);
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

  // Atomowy batch: 5 INSERTów w jednej transakcji.
  // XACT_ABORT ON gwarantuje rollback przy dowolnym błędzie (np. duplikat HK, FK, constraint).
  // Zapobiega osieroconym rekordom w hub_location/sat_location_manual jeśli któryś krok zawiedzie.
  try {
    await execSql(`
      SET XACT_ABORT ON;
      BEGIN TRANSACTION;

      INSERT INTO ${S.silver}.hub_location (location_hk, business_key, load_date, record_source)
      VALUES (CONVERT(VARBINARY(32), @locationHk, 2), @businessKey, GETUTCDATE(), 'manual');

      INSERT INTO ${S.silver}.sat_location_manual (
        location_hk, load_date, hash_diff, record_source,
        name, country, city, zip_code, address, phone, website_url,
        latitude, longitude, timezone, currency_code, cost_center, region, notes,
        name_std, country_std, city_std, created_by, created_at
      ) VALUES (
        CONVERT(VARBINARY(32), @locationHk, 2), GETUTCDATE(), CONVERT(VARBINARY(32), @hashDiff, 2), 'manual',
        @name, @country, @city, @zipCode, @address, @phone, @websiteUrl,
        @latitude, @longitude, @timezone, @currencyCode, @costCenter, @region, @notes,
        @nameStd, @countryStd, @cityStd, @caller, GETUTCDATE()
      );

      INSERT INTO ${S.gold}.dim_location (
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
      );

      INSERT INTO ${S.gold}.dim_location_quality
        (location_hk, snapshot_date, sources_count, completeness_score, has_lightspeed, has_yext, has_mcwin, has_gopos)
      VALUES (
        CONVERT(VARBINARY(32), @locationHk, 2), GETUTCDATE(), 1,
        @completeness, 0, 0, 0, 0
      );

      INSERT INTO ${S.silver}.stewardship_log
        (log_id, canonical_hk, action, changed_by, changed_at, reason)
      VALUES (
        @logId,
        CONVERT(VARBINARY(32), @locationHk, 2),
        'manual_create',
        @caller,
        GETUTCDATE(),
        @logReason
      );

      COMMIT TRANSACTION;
    `, {
      locationHk,
      businessKey,
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
      locationSk: Date.now(),
      completeness: calcCompleteness(body),
      logId: crypto.randomUUID(),
      logReason: `Manual create: ${body.name}`,
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
