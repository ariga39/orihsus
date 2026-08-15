import { createHash } from 'node:crypto';
import { deepMerge, integer, monotonicMs, number, seededRandom, sleepUntil } from './util.js';

export const DEFAULT_SCRIPT = Object.freeze({
  status: 200,
  headers: { 'content-type': 'application/json' },
  headerDelayMs: 0,
  bodyStartDelayMs: 0,
  body: { text: JSON.stringify({ id: 'mock', object: 'chat.completion', choices: [{ message: { role: 'assistant', content: 'ok' } }] }) },
});

export function normalizeScript(input = {}) {
  const script = deepMerge(DEFAULT_SCRIPT, input);
  script.status = integer(script.status, 200, { min: 100, max: 599 });
  script.headerDelayMs = delay(script.headerDelayMs);
  script.bodyStartDelayMs = delay(script.bodyStartDelayMs);
  if (script.sse) {
    script.headers = { ...script.headers, 'content-type': 'text/event-stream', 'cache-control': 'no-cache' };
    script.sse.count = script.sse.infinite ? Infinity : integer(script.sse.count, 1, { min: 0 });
  }
  return script;
}

function delay(value) { return value === 'infinite' || value === null ? Infinity : number(value, 0); }

export function fingerprint(authorization = '') {
  return createHash('sha256').update(authorization).digest('hex').slice(0, 12);
}

export class ScriptRegistry {
  constructor() { this.scenarios = new Map(); this.rules = []; this.attempts = new Map(); }
  setScenarios(entries) { this.scenarios = new Map(Object.entries(entries || {})); }
  setRules(rules) { this.rules = Array.isArray(rules) ? rules : []; this.attempts.clear(); }
  resolve({ inline, scenario, authorization, requestId }) {
    const key = `${requestId}\0${authorization}`;
    const attempt = (this.attempts.get(key) || 0) + 1;
    this.attempts.set(key, attempt);
    const rule = this.rules.find((candidate) =>
      (!candidate.match?.authorization || candidate.match.authorization === authorization) &&
      (!candidate.match?.keyFingerprint || candidate.match.keyFingerprint === fingerprint(authorization)) &&
      (!candidate.match?.requestId || candidate.match.requestId === requestId) &&
      (!candidate.match?.attempt || candidate.match.attempt === attempt));
    const named = scenario ? this.scenarios.get(scenario) : undefined;
    return { script: normalizeScript(inline ?? rule?.script ?? named ?? {}), attempt, rule: rule?.name ?? null };
  }
  reset() { this.attempts.clear(); }
}

async function plannedWait(ms, phase, context) {
  if (ms === Infinity) return new Promise((_, reject) => context.signal.addEventListener('abort', () => reject(context.signal.reason), { once: true }));
  const planned = monotonicMs() + ms;
  await sleepUntil(planned, context.signal);
  const actual = monotonicMs();
  context.metrics.observe(context.metrics.timingErrorMs, actual - planned);
  context.timeline.push({ phase, planned_ms: ms, actual_ms: actual - context.started, error_ms: actual - planned });
}

async function barrier(phase, definition, context) {
  if (!definition) return;
  const value = typeof definition === 'string' ? { name: definition } : definition;
  await context.barriers.wait(value.name, phase, context.requestId, integer(value.target, 0), context.signal);
  context.timeline.push({ phase: `barrier:${phase}`, actual_ms: monotonicMs() - context.started });
}

function write(response, data, context) {
  const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data));
  context.metrics.inc('bytes_sent', buffer.length); context.metrics.inc('chunks_sent');
  return new Promise((resolve, reject) => {
    if (response.write(buffer)) return resolve();
    const cleanup = () => {
      response.off('drain', drain);
      response.off('error', error);
      context.signal.removeEventListener('abort', abort);
    };
    const drain = () => { cleanup(); resolve(); }; const error = (err) => { cleanup(); reject(err); };
    const abort = () => { cleanup(); reject(context.signal.reason ?? new Error('aborted')); };
    response.once('drain', drain); response.once('error', error);
    context.signal.addEventListener('abort', abort, { once: true });
    if (context.signal.aborted) abort();
  });
}

function end(response) {
  return new Promise((resolve, reject) => {
    if (response.writableFinished) return resolve();
    const cleanup = () => { response.off('finish', finish); response.off('error', error); };
    const finish = () => { cleanup(); resolve(); };
    const error = (cause) => { cleanup(); reject(cause); };
    response.once('finish', finish); response.once('error', error);
    response.end();
  });
}

function bodyBuffer(body) {
  if (body?.base64) return Buffer.from(body.base64, 'base64');
  if (body?.invalidJson) return Buffer.from('{invalid-json');
  if (body?.usageLimit) {
    const u = body.usageLimit;
    return Buffer.from(JSON.stringify({ error: { type: 'GoUsageLimitError', message: u.message ?? 'Resets in 2 seconds', metadata: { limitName: u.limitName ?? '5h' } } }));
  }
  if (body?.size) return Buffer.alloc(integer(body.size, 0), body.byte ?? 'x');
  return Buffer.from(body?.text ?? '');
}

async function pacedBody(body, response, context) {
  const data = bodyBuffer(body);
  const chunkSize = body.bytewise ? 1 : integer(body.chunkSize, data.length || 1, { min: 1 });
  const rate = number(body.bytesPerSecond, 0);
  const interval = number(body.intervalMs, 0);
  const burst = integer(body.burstBytes, chunkSize, { min: 1 });
  let offset = 0; const start = monotonicMs();
  while (!context.signal.aborted && (offset < data.length || body.infinite)) {
    const source = data.length ? data : Buffer.from('x');
    const remaining = body.infinite ? chunkSize : Math.min(chunkSize, data.length - offset);
    const pieces = [];
    for (let filled = 0; filled < remaining;) {
      const take = Math.min(remaining - filled, source.length - (offset % source.length));
      pieces.push(source.subarray(offset % source.length, offset % source.length + take)); offset += take; filled += take;
    }
    await write(response, Buffer.concat(pieces), context);
    if (body.stallAfterBytes && offset >= body.stallAfterBytes) await plannedWait(Infinity, 'body_stall', context);
    const sentForRate = Math.ceil(offset / burst) * burst;
    if (rate) await sleepUntil(start + sentForRate / rate * 1000, context.signal);
    else if (interval) await plannedWait(interval, 'body_interval', context);
  }
}

async function sse(script, response, context) {
  const config = script.sse; const random = seededRandom(config.seed);
  const intervals = Array.isArray(config.intervalsMs) ? config.intervalsMs : null;
  let sent = 0;
  while (!context.signal.aborted && sent < config.count) {
    if (sent > 0) {
      let interval = intervals ? number(intervals[(sent - 1) % intervals.length], 0) : number(config.intervalMs, 0);
      if (config.jitterMs) interval += (random() * 2 - 1) * number(config.jitterMs, 0);
      await plannedWait(Math.max(0, interval), 'sse_interval', context);
    }
    const payload = config.eventBytes
      ? `data: ${'x'.repeat(Math.max(0, integer(config.eventBytes, 16) - 8))}\n\n`
      : `data: ${JSON.stringify({ id: sent, choices: [{ delta: { content: config.content ?? 'x' } }] })}\n\n`;
    await write(response, payload, context); sent++;
    if (config.disconnectAfter === sent) { response.destroy(); return; }
    if (config.silentAfter === sent) await plannedWait(Infinity, 'sse_silent', context);
    await barrier('chunk', script.barriers?.chunk, context);
  }
  if (config.done) await write(response, 'data: [DONE]\n\n', context);
  if (config.silentAfterSend) await plannedWait(Infinity, 'sse_silent', context);
}

export async function executeScript(script, response, context) {
  await plannedWait(script.headerDelayMs, 'headers_delay', context);
  await barrier('headers', script.barriers?.headers, context);
  const headers = { ...script.headers, 'x-mock-request-id': context.requestId };
  if (script.body?.contentLength) headers['content-length'] = bodyBuffer(script.body).length;
  response.writeHead(script.status, headers);
  response.flushHeaders?.();
  context.timeline.push({ phase: 'headers_sent', actual_ms: monotonicMs() - context.started });
  await plannedWait(script.bodyStartDelayMs, 'body_start_delay', context);
  await barrier('body', script.barriers?.body, context);
  if (script.sse) await sse(script, response, context);
  else await pacedBody(script.body ?? {}, response, context);
  await barrier('eof', script.barriers?.eof, context);
  if (!script.body?.infinite && !script.sse?.infinite && !response.destroyed) await end(response);
}
