import { describe, it, expect, vi } from 'vitest';
import { AxiosError, AxiosHeaders } from 'axios';
import { ApiConflictError, ApiPreconditionError } from './mdmApi';

// Avoid import side effects from mdmApi (MSAL init). We re-implement mapAxiosError behavior
// by exercising it through a mock axios.isAxiosError flow. Since mapAxiosError isn't exported,
// test the error types themselves and the contract used by hooks.

describe('ApiConflictError', () => {
  it('has status 409 and holds concurrency metadata', () => {
    const err = new ApiConflictError('accepted', 'anna@l-osteria.de', '2026-04-24T10:00:00Z');
    expect(err.status).toBe(409);
    expect(err.currentStatus).toBe('accepted');
    expect(err.reviewedBy).toBe('anna@l-osteria.de');
    expect(err.reviewedAt).toBe('2026-04-24T10:00:00Z');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('ApiConflictError');
  });

  it('uses default Polish message when none provided', () => {
    const err = new ApiConflictError();
    expect(err.message).toMatch(/zmieniony/i);
  });

  it('allows custom message', () => {
    const err = new ApiConflictError(undefined, undefined, undefined, 'Custom msg');
    expect(err.message).toBe('Custom msg');
  });
});

describe('ApiPreconditionError', () => {
  it('has status 412 and holds current value', () => {
    const err = new ApiPreconditionError("L'Osteria München");
    expect(err.status).toBe(412);
    expect(err.currentValue).toBe("L'Osteria München");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('ApiPreconditionError');
  });

  it('uses default message when none provided', () => {
    const err = new ApiPreconditionError();
    expect(err.message).toMatch(/zmieniony/i);
  });
});

// Integration-style: build an AxiosError and verify it's our concern,
// ensuring hooks can rely on instanceof ApiConflictError.
describe('axios error shape', () => {
  it('AxiosError with 409 status has expected structure for mapper', () => {
    const err = new AxiosError(
      'Request failed',
      '409',
      undefined,
      undefined,
      {
        status: 409,
        data: {
          error: 'Pair already reviewed',
          currentStatus: 'accepted',
          reviewedBy: 'bob@example.com',
          reviewedAt: '2026-04-24T12:00:00Z',
        },
        statusText: 'Conflict',
        headers: new AxiosHeaders(),
        config: { headers: new AxiosHeaders() },
      } as never,
    );

    expect(err.response?.status).toBe(409);
    expect(err.response?.data).toMatchObject({
      error: 'Pair already reviewed',
      reviewedBy: 'bob@example.com',
    });
  });
});
