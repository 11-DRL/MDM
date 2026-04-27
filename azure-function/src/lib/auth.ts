// Shared auth helpers — JWT verification for Azure AD tokens.
// Used by mdmWrite (v1) and v2Routes.

import { HttpRequest, HttpResponseInit } from '@azure/functions';
import { createRemoteJWKSet, jwtVerify, type JWTPayload } from 'jose';

export type AuthResult = { ok: true; email: string } | { ok: false; status: 401 | 403 | 500; error: string };

let cachedJwks: ReturnType<typeof createRemoteJWKSet> | null = null;
let cachedJwksTenant: string | null = null;

function getJwks(tenantId: string) {
  if (!cachedJwks || cachedJwksTenant !== tenantId) {
    cachedJwks = createRemoteJWKSet(
      new URL(`https://login.microsoftonline.com/${tenantId}/discovery/v2.0/keys`),
    );
    cachedJwksTenant = tenantId;
  }
  return cachedJwks;
}

function extractEmail(payload: JWTPayload): string {
  const p = payload as Record<string, unknown>;
  return (
    (p.preferred_username as string | undefined) ??
    (p.upn as string | undefined) ??
    (p.email as string | undefined) ??
    (p.unique_name as string | undefined) ??
    'unknown'
  );
}

export async function validateBearerToken(req: HttpRequest): Promise<AuthResult> {
  const authHeader = req.headers.get('authorization') ?? req.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) {
    return { ok: false, status: 401, error: 'Missing or invalid Authorization header' };
  }

  const token = authHeader.slice(7);
  const tenantId = process.env.AZURE_TENANT_ID;
  const expectedAudience = process.env.EXPECTED_AUDIENCE;

  if (!tenantId) {
    return { ok: false, status: 500, error: 'AZURE_TENANT_ID not configured on server' };
  }

  if (!expectedAudience) {
    return { ok: false, status: 500, error: 'EXPECTED_AUDIENCE not configured on server' };
  }

  try {
    const v1Issuer = `https://sts.windows.net/${tenantId}/`;
    const v2Issuer = `https://login.microsoftonline.com/${tenantId}/v2.0`;

    const { payload } = await jwtVerify(token, getJwks(tenantId), {
      issuer: [v1Issuer, v2Issuer],
      audience: expectedAudience,
      clockTolerance: 5,
    });

    if (payload.tid && payload.tid !== tenantId) {
      return { ok: false, status: 403, error: 'Token tenant mismatch' };
    }

    return { ok: true, email: extractEmail(payload) };
  } catch (err) {
    const code = (err as { code?: string })?.code ?? '';
    const message = (err as Error)?.message ?? 'unknown';

    if (code === 'ERR_JWT_EXPIRED') return { ok: false, status: 401, error: 'Token expired' };
    if (code === 'ERR_JWS_SIGNATURE_VERIFICATION_FAILED') return { ok: false, status: 401, error: 'Invalid token signature' };
    if (code === 'ERR_JWT_CLAIM_VALIDATION_FAILED') return { ok: false, status: 401, error: `Invalid token claim: ${message}` };
    if (code === 'ERR_JWS_INVALID' || code === 'ERR_JWT_INVALID') return { ok: false, status: 401, error: 'Malformed JWT' };

    return { ok: false, status: 401, error: `Token validation failed: ${message}` };
  }
}

export async function requireAuth(req: HttpRequest): Promise<{ ok: true; email: string } | HttpResponseInit> {
  const result = await validateBearerToken(req);
  if (!result.ok) return { status: result.status, jsonBody: { error: result.error } };
  return result;
}
