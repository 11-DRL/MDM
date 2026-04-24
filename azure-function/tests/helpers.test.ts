import { describe, it, expect } from 'vitest';
import {
  sanitizeHex32,
  parsePositiveInt,
  sanitizeEntityId,
  toMatchSource,
  parseStatus,
} from '../src/functions/mdmWrite';

describe('sanitizeHex32', () => {
  it('accepts lowercase 64-char hex', () => {
    const hex = 'a'.repeat(64);
    expect(sanitizeHex32(hex)).toBe(hex);
  });

  it('lowercases uppercase hex', () => {
    const hex = 'A'.repeat(64);
    expect(sanitizeHex32(hex)).toBe('a'.repeat(64));
  });

  it('rejects wrong length', () => {
    expect(sanitizeHex32('abc')).toBeNull();
    expect(sanitizeHex32('a'.repeat(63))).toBeNull();
    expect(sanitizeHex32('a'.repeat(65))).toBeNull();
  });

  it('rejects non-hex characters', () => {
    expect(sanitizeHex32('g'.repeat(64))).toBeNull();
    expect(sanitizeHex32('z'.repeat(64))).toBeNull();
  });

  it('returns null for empty/null/undefined', () => {
    expect(sanitizeHex32(null)).toBeNull();
    expect(sanitizeHex32(undefined)).toBeNull();
    expect(sanitizeHex32('')).toBeNull();
  });

  it('trims whitespace', () => {
    const hex = 'f'.repeat(64);
    expect(sanitizeHex32(`  ${hex}  `)).toBe(hex);
  });
});

describe('parsePositiveInt', () => {
  it('returns parsed value when within bounds', () => {
    expect(parsePositiveInt('25', 10, 1, 100)).toBe(25);
  });

  it('clamps to max', () => {
    expect(parsePositiveInt('9999', 10, 1, 100)).toBe(100);
  });

  it('clamps to min', () => {
    expect(parsePositiveInt('0', 10, 1, 100)).toBe(1);
  });

  it('returns default when null', () => {
    expect(parsePositiveInt(null, 10, 1, 100)).toBe(10);
  });

  it('returns default when NaN', () => {
    expect(parsePositiveInt('abc', 10, 1, 100)).toBe(10);
  });

  it('truncates decimals', () => {
    expect(parsePositiveInt('25.9', 10, 1, 100)).toBe(25);
  });
});

describe('sanitizeEntityId', () => {
  it('accepts alphanumeric with underscores', () => {
    expect(sanitizeEntityId('business_location')).toBe('business_location');
    expect(sanitizeEntityId('Entity_123')).toBe('Entity_123');
  });

  it('returns default when null/empty', () => {
    expect(sanitizeEntityId(null)).toBe('business_location');
    expect(sanitizeEntityId('')).toBe('business_location');
    expect(sanitizeEntityId('   ')).toBe('business_location');
  });

  it('throws on SQL injection attempts', () => {
    expect(() => sanitizeEntityId("'; DROP TABLE--")).toThrow('Invalid entityId');
    expect(() => sanitizeEntityId('table-name')).toThrow('Invalid entityId');
    expect(() => sanitizeEntityId('table name')).toThrow('Invalid entityId');
  });
});

describe('toMatchSource', () => {
  it('accepts all valid sources', () => {
    expect(toMatchSource('lightspeed')).toBe('lightspeed');
    expect(toMatchSource('yext')).toBe('yext');
    expect(toMatchSource('mcwin')).toBe('mcwin');
    expect(toMatchSource('gopos')).toBe('gopos');
    expect(toMatchSource('manual')).toBe('manual');
  });

  it('lowercases uppercase input', () => {
    expect(toMatchSource('LIGHTSPEED')).toBe('lightspeed');
  });

  it('falls back to lightspeed for unknown', () => {
    expect(toMatchSource('unknown')).toBe('lightspeed');
    expect(toMatchSource(null)).toBe('lightspeed');
    expect(toMatchSource(undefined)).toBe('lightspeed');
    expect(toMatchSource(42)).toBe('lightspeed');
  });
});

describe('parseStatus', () => {
  it('returns "all" for "all" (any case)', () => {
    expect(parseStatus('all')).toBe('all');
    expect(parseStatus('ALL')).toBe('all');
    expect(parseStatus('All')).toBe('all');
  });

  it('returns "pending" for anything else', () => {
    expect(parseStatus('pending')).toBe('pending');
    expect(parseStatus(null)).toBe('pending');
    expect(parseStatus('accepted')).toBe('pending');
    expect(parseStatus('')).toBe('pending');
  });
});
