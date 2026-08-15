#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { createSecureServer } from 'node:http2';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Capacity } from './lib/capacity.js';
import { Barriers } from './lib/barriers.js';
import { Metrics } from './lib/metrics.js';
import { executeScript, fingerprint, ScriptRegistry } from './lib/scripts.js';
import { bool, integer, monotonicMs, parseJsonHeader } from './lib/util.js';

export function parseArgs(argv = process.argv.slice(2), env = process.env) {
  const values = {};
  for (let i = 0; i < argv.length; i++) {
    if (!argv[i].startsWith('--')) throw new Error(`unexpected argument: ${argv[i]}`);
    const [raw, inline] = argv[i].slice(2).split('=', 2);
    values[raw] = inline ?? argv[++i];
  }
  if (Object.hasOwn(values, 'help')) return { help: true };
  return {
    host: values.host ?? env.MOCK_HOST ?? '127.0.0.1',
    port: integer(values.port ?? env.MOCK_PORT, 8443, { min: 0, max: 65535 }),
    cert: values.cert ?? env.MOCK_TLS_CERT,
    key: values.key ?? env.MOCK_TLS_KEY,
    controlToken: values['control-token'] ?? env.MOCK_CONTROL_TOKEN ?? '',
    maxConcurrency: integer(values['max-concurrency'] ?? env.MOCK_MAX_CONCURRENCY, 400, { min: 1 }),
    maxWaiting: integer(values['max-waiting'] ?? env.MOCK_MAX_WAITING, 0, { min: 0 }),
    maxConnections: integer(values['max-connections'] ?? env.MOCK_MAX_CONNECTIONS, 1024, { min: 1 }),
    waitTimeoutMs: integer(values['wait-timeout-ms'] ?? env.MOCK_WAIT_TIMEOUT_MS, 30000, { min: 1 }),
    eventBufferSize: integer(values['event-buffer-size'] ?? env.MOCK_EVENT_BUFFER_SIZE, 10000, { min: 0 }),
    detailedEvents: bool(values['detailed-events'] ?? env.MOCK_DETAILED_EVENTS, false),
    maxRequestBodyBytes: integer(values['max-request-body-bytes'] ?? env.MOCK_MAX_REQUEST_BODY_BYTES, 1024 * 1024, { min: 1 }),
  };
}

function help() {
  return `Usage: node server.js --cert FILE --key FILE [options]\n\n` +
    `  --host HOST                    listen host (127.0.0.1)\n` +
    `  --port PORT                    listen port (8443; 0 selects a free port)\n` +
    `  --max-concurrency N            active request cap (400)\n` +
    `  --max-waiting N                bounded request queue (0)\n` +
    `  --max-connections N            accepted TCP connection cap (1024)\n` +
    `  --wait-timeout-ms MS           capacity queue timeout (30000)\n` +
    `  --control-token TOKEN          require bearer token on /control\n` +
    `  --detailed-events true|false   bounded per-request event ring (false)\n` +
    `Environment equivalents use MOCK_* names; see README.md.\n`;
}

async function readBody(request, maximum) {
  const chunks = []; let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maximum) throw Object.assign(new Error('request body too large'), { statusCode: 413 });
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function json(response, status, value, headers = {}) {
  if (response.destroyed) return;
  const body = Buffer.from(JSON.stringify(value));
  response.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length, ...headers });
  response.end(body);
}

function controlAllowed(request, config) {
  return !config.controlToken || request.headers.authorization === `Bearer ${config.controlToken}`;
}

export function shouldCancelResponseClose(response) {
  return !response.writableFinished;
}

export function createMockServer(config) {
  if (!config.cert || !config.key) throw new Error('--cert and --key (or MOCK_TLS_CERT/MOCK_TLS_KEY) are required');
  const metrics = new Metrics(config); const barriers = new Barriers(); const registry = new ScriptRegistry();
  const capacity = new Capacity(config, metrics); const controllers = new Map();
  const server = createSecureServer({ cert: readFileSync(config.cert), key: readFileSync(config.key), allowHTTP1: true,
    settings: { initialWindowSize: 16 * 1024 * 1024, maxConcurrentStreams: Math.max(1000, config.maxConcurrency + config.maxWaiting) } });

  const sockets = new Set();
  server.on('connection', (socket) => {
    if (sockets.size >= config.maxConnections) {
      metrics.inc('connection_limit_hits'); metrics.inc('connections_rejected'); socket.destroy(); return;
    }
    sockets.add(socket); metrics.gauge('connections_active', 1);
    socket.once('close', () => { if (sockets.delete(socket)) metrics.gauge('connections_active', -1); });
  });
  server.on('sessionError', () => metrics.inc('errors'));
  server.on('tlsClientError', () => metrics.inc('errors'));

  server.on('request', async (request, response) => {
    const path = new URL(request.url, 'https://mock.invalid').pathname;
    try {
      if (path === '/metrics' && request.method === 'GET') return json(response, 200, metrics.snapshot({ barriers: barriers.snapshot() }));
      if (path === '/healthz' && request.method === 'GET') return json(response, 200, { ok: true });
      if (path.startsWith('/control/')) return await handleControl(path, request, response, { config, metrics, barriers, registry, controllers });
      if (path !== '/v1/chat/completions') return json(response, 404, { error: 'not found' });
      await handleCompletion(request, response, { config, metrics, barriers, registry, capacity, controllers });
    } catch (error) {
      metrics.inc('errors');
      if (!response.headersSent) json(response, error.statusCode ?? 500, { error: error.message }, error.capacity ? { 'retry-after': '1' } : {});
      else response.destroy(error);
    }
  });

  server.mock = { config, metrics, barriers, registry, controllers };
  return server;
}

async function handleControl(path, request, response, state) {
  if (!controlAllowed(request, state.config)) return json(response, 401, { error: 'unauthorized' });
  let payload = {};
  if (request.method === 'POST') {
    const raw = await readBody(request, state.config.maxRequestBodyBytes);
    payload = raw.length ? JSON.parse(raw) : {};
  }
  if (path === '/control/state' && request.method === 'GET') return json(response, 200, state.metrics.snapshot({ barriers: state.barriers.snapshot() }));
  if (path === '/control/scenarios' && request.method === 'POST') { state.registry.setScenarios(payload); return json(response, 200, { ok: true, count: Object.keys(payload).length }); }
  if (path === '/control/rules' && request.method === 'POST') { state.registry.setRules(payload.rules ?? payload); return json(response, 200, { ok: true }); }
  if (path === '/control/reset' && request.method === 'POST') {
    state.barriers.reset(); state.registry.reset(); state.metrics.reset(); return json(response, 200, { ok: true });
  }
  if (path === '/control/cancel' && request.method === 'POST') {
    let count = 0;
    for (const [id, set] of state.controllers) if (!payload.requestId || payload.requestId === id) for (const controller of set) { controller.abort(new Error('cancelled by control plane')); count++; }
    return json(response, 200, { ok: true, cancelled: count });
  }
  const match = path.match(/^\/control\/barriers\/([^/]+)\/(release|cancel)$/);
  if (match && request.method === 'POST') {
    const name = decodeURIComponent(match[1]);
    const count = match[2] === 'release' ? state.barriers.release(name, integer(payload.count, Infinity), payload.phase) : state.barriers.cancel(name);
    return json(response, 200, { ok: true, affected: count });
  }
  return json(response, 404, { error: 'control endpoint not found' });
}

async function handleCompletion(request, response, state) {
  const started = monotonicMs(); const requestId = String(request.headers['x-request-id'] ?? randomUUID());
  const authorization = String(request.headers.authorization ?? ''); const controller = new AbortController();
  let set = state.controllers.get(requestId); if (!set) state.controllers.set(requestId, set = new Set()); set.add(controller);
  let release; let handlerCompleted = false;
  const abortRequest = () => { if (!handlerCompleted) controller.abort(new Error('client disconnected')); };
  const abortPrematureResponseClose = () => {
    // `close` is emitted for both normal HTTP/2 stream completion and an early
    // peer disconnect. `writableFinished` distinguishes the two protocol states.
    if (!handlerCompleted && shouldCancelResponseClose(response)) controller.abort(new Error('client disconnected'));
  };
  request.once('aborted', abortRequest); response.once('close', abortPrematureResponseClose);
  state.metrics.inc('requests_total');
  try {
    const rawBody = await readBody(request, state.config.maxRequestBodyBytes);
    let bodyScript;
    if (rawBody.length) {
      try { bodyScript = JSON.parse(rawBody).mock; } catch { /* upstream payload need not be JSON */ }
    }
    const inline = parseJsonHeader(request.headers['x-mock-script']) ?? bodyScript;
    const selected = state.registry.resolve({ inline, scenario: request.headers['x-mock-scenario'], authorization, requestId });
    release = await state.capacity.acquire(controller.signal);
    const context = { requestId, signal: controller.signal, barriers: state.barriers, metrics: state.metrics, started, timeline: [] };
    state.metrics.status[selected.script.status] = (state.metrics.status[selected.script.status] || 0) + 1;
    await executeScript(selected.script, response, context);
    handlerCompleted = true; state.metrics.inc('completed');
    state.metrics.event({ request_id: requestId, key_fingerprint: fingerprint(authorization), attempt: selected.attempt,
      rule: selected.rule, status: selected.script.status, outcome: 'completed', script: selected.script, timeline: context.timeline });
  } catch (error) {
    const cancelled = controller.signal.aborted;
    if (cancelled) state.metrics.inc('cancelled'); else state.metrics.inc('errors');
    if (!response.headersSent && !response.destroyed) json(response, error.statusCode ?? (cancelled ? 499 : 500), { error: error.message }, error.capacity ? { 'retry-after': '1' } : {});
    else if (cancelled && !response.destroyed) response.destroy();
    state.metrics.event({ request_id: requestId, key_fingerprint: fingerprint(authorization), outcome: cancelled ? 'cancelled' : 'error', error: error.message });
  } finally {
    handlerCompleted = true; release?.(); set.delete(controller); if (!set.size) state.controllers.delete(requestId);
    request.off('aborted', abortRequest); response.off('close', abortPrematureResponseClose);
  }
}

export async function start(config) {
  const server = createMockServer(config);
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(config.port, config.host, resolve); });
  return server;
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  try {
    const config = parseArgs();
    if (config.help) { process.stdout.write(help()); process.exit(0); }
    const server = await start(config); const address = server.address();
    process.stdout.write(`${JSON.stringify({ event: 'listening', host: address.address, port: address.port, protocol: 'https', pid: process.pid })}\n`);
    const shutdown = () => { for (const set of server.mock.controllers.values()) for (const controller of set) controller.abort(); server.close(() => process.exit(0)); };
    process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);
  } catch (error) { process.stderr.write(`${error.stack ?? error}\n`); process.exit(1); }
}
