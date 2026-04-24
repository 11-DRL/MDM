import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { validateBearerToken } from '../src/functions/mdmWrite';
import type { HttpRequest } from '@azure/functions';

function mockRequest(headers: Record<string, string | undefined>): HttpRequest {
  const h = new Map(
    Object.entries(headers).filter(([, v]) => v !== undefined) as [string, string][],
  );
  return {
    headers: {
      get: (key: string) => h.get(key.toLowerCase()) ?? h.get(key) ?? null,
    },
  } as unknown as HttpRequest;
}

describe('validateBearerToken', () => {
  const originalTenant = process.env.AZURE_TENANT_ID;

  beforeEach(() => {
    process.env.AZURE_TENANT_ID = '5d842dfd-009e-4f2f-bb85-78670fa303bb';
  });

  afterEach(() => {
    if (originalTenant === undefined) delete process.env.AZURE_TENANT_ID;
    else process.env.AZURE_TENANT_ID = originalTenant;
  });

  it('rejects request without Authorization header', async () => {
    const result = await validateBearerToken(mockRequest({}));
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.status).toBe(401);
      expect(result.error).toMatch(/Authorization/i);
    }
  });

  it('rejects request with non-Bearer header', async () => {
    const result = await validateBearerToken(mockRequest({ authorization: 'Basic dXNlcjpwYXNz' }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.status).toBe(401);
  });

  it('rejects malformed JWT (not 3 segments)', async () => {
    const result = await validateBearerToken(mockRequest({ authorization: 'Bearer not.a.valid.jwt.token' }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.status).toBe(401);
  });

  it('rejects unsigned/forged JWT (signature verification fails)', async () => {
    // Valid JWT structure but not signed by Azure AD JWKS → signature check fails.
    const header = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT', kid: 'fake' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify({
      iss: 'https://login.microsoftonline.com/5d842dfd-009e-4f2f-bb85-78670fa303bb/v2.0',
      tid: '5d842dfd-009e-4f2f-bb85-78670fa303bb',
      preferred_username: 'attacker@evil.com',
      exp: Math.floor(Date.now() / 1000) + 3600,
    })).toString('base64url');
    const forged = `${header}.${payload}.${Buffer.from('fake-signature').toString('base64url')}`;

    const result = await validateBearerToken(mockRequest({ authorization: `Bearer ${forged}` }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.status).toBe(401);
  }, 15_000);

  it('returns 500 when AZURE_TENANT_ID not configured', async () => {
    delete process.env.AZURE_TENANT_ID;
    const result = await validateBearerToken(mockRequest({ authorization: 'Bearer header.payload.sig' }));
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.status).toBe(500);
      expect(result.error).toMatch(/AZURE_TENANT_ID/);
    }
  });
});
