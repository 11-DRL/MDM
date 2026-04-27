// Shared value-parsing helpers used by mdmWrite (v1) and v2Routes.

const HEX_32_RE = /^[0-9a-f]{64}$/i;
const SAFE_ENTITY_ID_RE = /^[a-z][a-z0-9_]{0,49}$/;

export function asString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'string') return value;
  return String(value);
}

export function asNumber(value: unknown, fallback = 0): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function asBoolean(value: unknown): boolean | undefined {
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

export function asIso(value: unknown): string | undefined {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string' && value.trim().length > 0) return value;
  return undefined;
}

export function sanitizeHex32(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!HEX_32_RE.test(trimmed)) return null;
  return trimmed.toLowerCase();
}

export function parsePositiveInt(rawValue: string | null, defaultValue: number, min: number, max: number): number {
  const parsed = Number(rawValue ?? defaultValue);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

export function validateEntityId(raw: string | undefined): string | null {
  if (!raw) return null;
  const v = raw.trim();
  return SAFE_ENTITY_ID_RE.test(v) ? v : null;
}

export type MatchSource = 'lightspeed' | 'yext' | 'mcwin' | 'gopos' | 'manual';
export type MatchStatus = 'pending' | 'accepted' | 'rejected' | 'auto_accepted';

export function toMatchSource(value: unknown): MatchSource {
  const raw = String(value ?? '').toLowerCase();
  if (raw === 'lightspeed' || raw === 'yext' || raw === 'mcwin' || raw === 'gopos' || raw === 'manual') {
    return raw;
  }
  return 'lightspeed';
}

export function parseStatus(raw: string | null): 'pending' | 'all' {
  return raw?.toLowerCase() === 'all' ? 'all' : 'pending';
}

export function sanitizeEntityId(raw: string | null): string {
  const value = raw?.trim() || 'business_location';
  if (!/^[a-z0-9_]+$/i.test(value)) {
    throw new Error('Invalid entityId');
  }
  return value;
}
