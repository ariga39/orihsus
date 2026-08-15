import { setTimeout as sleep } from 'node:timers/promises';

export function integer(value, fallback, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) return fallback;
  return parsed;
}

export function number(value, fallback, { min = 0, max = Number.MAX_VALUE } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) return fallback;
  return parsed;
}

export function bool(value, fallback = false) {
  if (value === undefined) return fallback;
  return value === true || value === 'true' || value === '1';
}

export function monotonicMs() {
  return Number(process.hrtime.bigint()) / 1e6;
}

export async function sleepUntil(deadline, signal) {
  while (!signal?.aborted) {
    const remaining = deadline - monotonicMs();
    if (remaining <= 0) return;
    await sleep(remaining, undefined, { signal });
  }
}

export function quantile(values, q) {
  if (!values.length) return 0;
  const ordered = [...values].sort((a, b) => a - b);
  return ordered[Math.min(ordered.length - 1, Math.floor(q * ordered.length))];
}

export function redactAuthorization(value = '') {
  if (!value) return 'none';
  return `sha256-not-recorded:${value.length}`;
}

export function seededRandom(seed) {
  let state = (Number(seed) || 1) >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

export function parseJsonHeader(value) {
  if (!value) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  try {
    if (raw.startsWith('{')) return JSON.parse(raw);
    return JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
  } catch (error) {
    throw new Error(`invalid x-mock-script: ${error.message}`);
  }
}

export function deepMerge(base, override) {
  if (!override || typeof override !== 'object' || Array.isArray(override)) return base;
  const result = { ...base };
  for (const [key, value] of Object.entries(override)) {
    result[key] = value && typeof value === 'object' && !Array.isArray(value)
      ? deepMerge(base?.[key] ?? {}, value)
      : value;
  }
  return result;
}
