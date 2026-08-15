import test, { after, before } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import https from 'node:https';
import http2 from 'node:http2';
import { shouldCancelResponseClose, start } from '../server.js';

let server; let origin; let testKey; let testCert;

before(async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mock-upstream-test-'));
  testKey = join(directory, 'key.pem'); testCert = join(directory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-keyout', testKey, '-out', testCert, '-subj', '/CN=localhost'], { stdio: 'ignore' });
  server = await start({ host: '127.0.0.1', port: 0, key: testKey, cert: testCert, controlToken: '', maxConcurrency: 1,
    maxWaiting: 1, maxConnections: 20, waitTimeoutMs: 100, eventBufferSize: 100, detailedEvents: true, maxRequestBodyBytes: 1024 * 1024 });
  origin = `https://127.0.0.1:${server.address().port}`;
});

after(async () => {
  if (!server) return;
  for (const set of server.mock.controllers.values()) for (const controller of set) controller.abort();
  await new Promise((resolve) => server.close(resolve));
});

function request(path, { method = 'GET', headers = {}, body } = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request(`${origin}${path}`, { method, headers, rejectUnauthorized: false }, (res) => {
      const chunks = []; res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject); if (body) req.end(body); else req.end();
  });
}

function completion(script, id = `r-${Math.random()}`) {
  return request('/v1/chat/completions', { method: 'POST', headers: { 'x-request-id': id,
    'x-mock-script': Buffer.from(JSON.stringify(script)).toString('base64url') }, body: '{}' });
}

test('response close cancellation follows protocol completion state', () => {
  assert.equal(shouldCancelResponseClose({ writableFinished: true }), false);
  assert.equal(shouldCancelResponseClose({ writableFinished: false }), true);
});

test('serves HTTPS HTTP/1.1 default JSON and metrics', async () => {
  const result = await completion({});
  assert.equal(result.status, 200); assert.match(result.body, /chat\.completion/);
  const metrics = await request('/metrics');
  assert.equal(metrics.status, 200); assert.equal(JSON.parse(metrics.body).schema_version, 1);
});

test('supports HTTP/2', async () => {
  const client = http2.connect(origin, { rejectUnauthorized: false });
  const result = await new Promise((resolve, reject) => {
    const req = client.request({ ':path': '/healthz' }); const chunks = [];
    req.on('response', (headers) => req.status = headers[':status']); req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve({ status: req.status, body: Buffer.concat(chunks).toString() })); req.on('error', reject); req.end();
  });
  client.close(); assert.equal(result.status, 200);
});

test('separates header delay, body delay and paced body', async () => {
  const started = performance.now();
  const result = await completion({ headerDelayMs: 20, bodyStartDelayMs: 20, body: { text: 'abcd', chunkSize: 1, intervalMs: 5 } });
  assert.equal(result.body, 'abcd'); assert.ok(performance.now() - started >= 50);
});

test('barrier release controls headers', async () => {
  const pending = completion({ barriers: { headers: 'gate' }, body: { text: 'released' } }, 'barrier');
  await new Promise((resolve) => setTimeout(resolve, 20));
  const state = JSON.parse((await request('/control/state')).body);
  assert.equal(state.barriers.gate.waiting, 1);
  await request('/control/barriers/gate/release', { method: 'POST', body: '{}' });
  assert.equal((await pending).body, 'released');
});

test('capacity queues one and rejects excess with counters', async () => {
  const first = completion({ barriers: { eof: 'capacity' }, body: { text: 'one' } }, 'one');
  await new Promise((resolve) => setTimeout(resolve, 20));
  const second = completion({ body: { text: 'two' } }, 'two');
  await new Promise((resolve) => setTimeout(resolve, 10));
  const third = await completion({ body: { text: 'three' } }, 'three');
  assert.equal(third.status, 503); assert.equal(third.headers['retry-after'], '1');
  await request('/control/barriers/capacity/release', { method: 'POST', body: '{}' });
  assert.equal((await first).status, 200); assert.equal((await second).body, 'two');
  const metrics = JSON.parse((await request('/metrics')).body);
  assert.ok(metrics.counters.limit_hits >= 2); assert.ok(metrics.counters.rejected >= 1);
});

test('emits finite SSE with DONE', async () => {
  const result = await completion({ sse: { count: 2, intervalMs: 1, done: true } });
  assert.equal(result.status, 200); assert.match(result.headers['content-type'], /text\/event-stream/);
  assert.equal((result.body.match(/^data:/gm) || []).length, 3); assert.match(result.body, /\[DONE\]/);
});

test('rules select errors by key and attempt without recording secrets', async () => {
  await request('/control/rules', { method: 'POST', body: JSON.stringify({ rules: [{ name: 'usage', match: { authorization: 'Bearer K1', attempt: 1 },
    script: { status: 429, headers: { 'retry-after': '2' }, body: { usageLimit: { limitName: '5h', message: 'Resets in 2 seconds' } } } }] }) });
  const headers = { authorization: 'Bearer K1', 'x-request-id': 'attempt', 'content-type': 'application/json' };
  const first = await request('/v1/chat/completions', { method: 'POST', headers, body: '{}' });
  const second = await request('/v1/chat/completions', { method: 'POST', headers, body: '{}' });
  assert.equal(first.status, 429); assert.match(first.body, /GoUsageLimitError/); assert.equal(second.status, 200);
  const metrics = JSON.parse((await request('/metrics')).body);
  assert.doesNotMatch(JSON.stringify(metrics.events), /Bearer K1/);
});

test('normal immediate and delayed response completion never counts as cancellation', async () => {
  const stress = await start({ host: '127.0.0.1', port: 0, key: testKey, cert: testCert, controlToken: '', maxConcurrency: 100,
    maxWaiting: 0, maxConnections: 20, waitTimeoutMs: 100, eventBufferSize: 100, detailedEvents: true, maxRequestBodyBytes: 1024 * 1024 });
  const stressOrigin = `https://127.0.0.1:${stress.address().port}`;
  const count = 80; const size = 1024 * 1024;
  const client = http2.connect(stressOrigin, { rejectUnauthorized: false });
  try {
    const results = await Promise.all(Array.from({ length: count }, (_, index) => new Promise((resolve, reject) => {
      const req = client.request({ ':method': 'POST', ':path': '/v1/chat/completions',
        'x-request-id': `normal-completion-${index}`, 'content-type': 'application/json' });
      const chunks = []; let status;
      req.on('response', (headers) => { status = headers[':status']; });
      req.on('data', (chunk) => chunks.push(chunk)); req.on('end', () => resolve({ status, body: Buffer.concat(chunks) }));
      req.on('error', reject);
      req.end(JSON.stringify({ mock: { bodyStartDelayMs: index % 2 ? 5 : 0,
        body: { size, byte: 'z', chunkSize: index % 3 === 0 ? 257 : 65536 } } }));
    })));
    for (const result of results) {
      assert.equal(result.status, 200); assert.equal(result.body.length, size);
      assert.equal(result.body[0], 122); assert.equal(result.body.at(-1), 122);
    }
    const metrics = stress.mock.metrics.snapshot();
    assert.equal(metrics.counters.completed, count);
    assert.equal(metrics.counters.cancelled, 0);
    assert.equal(metrics.counters.errors, 0);
  } finally {
    client.close();
    for (const set of stress.mock.controllers.values()) for (const controller of set) controller.abort();
    await new Promise((resolve) => stress.close(resolve));
  }
});

test('an actual client stream cancellation is still counted as cancelled', async () => {
  await request('/control/reset', { method: 'POST', body: '{}' });
  const client = http2.connect(origin, { rejectUnauthorized: false });
  await new Promise((resolve, reject) => {
    const req = client.request({ ':method': 'POST', ':path': '/v1/chat/completions',
      'x-request-id': 'real-client-cancel',
      'x-mock-script': Buffer.from(JSON.stringify({ sse: { infinite: true, intervalMs: 1 } })).toString('base64url') });
    req.once('data', () => { req.close(http2.constants.NGHTTP2_CANCEL); resolve(); });
    req.once('error', (error) => { if (error.code !== 'ERR_HTTP2_STREAM_CANCEL') reject(error); });
    req.end('{}');
  });
  for (let attempt = 0; attempt < 50 && server.mock.controllers.size; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  client.close();
  const metrics = server.mock.metrics.snapshot();
  assert.equal(metrics.counters.completed, 0);
  assert.equal(metrics.counters.cancelled, 1);
});

test('control cancel interrupts an infinite SSE blocked by real HTTP/2 backpressure', async () => {
  await request('/control/reset', { method: 'POST', body: '{}' });
  const client = http2.connect(origin, { rejectUnauthorized: false });
  const req = client.request({ ':method': 'POST', ':path': '/v1/chat/completions',
    'x-request-id': 'backpressured-control-cancel',
    'x-mock-script': Buffer.from(JSON.stringify({ sse: { infinite: true, intervalMs: 0, eventBytes: 65536 } })).toString('base64url') });
  try {
    await new Promise((resolve, reject) => {
      req.once('response', resolve);
      req.once('error', reject);
      req.end('{}');
    });
    // Deliberately leave the response unread. The HTTP/2 stream window fills,
    // response.write() returns false, and the server waits for `drain`.
    for (let attempt = 0; attempt < 100 && server.mock.metrics.counters.bytes_sent < 65536; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(server.mock.metrics.counters.active, 1);
    const cancelled = await request('/control/cancel', { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ requestId: 'backpressured-control-cancel' }) });
    assert.equal(JSON.parse(cancelled.body).cancelled, 1);
    for (let attempt = 0; attempt < 100 && server.mock.metrics.counters.active !== 0; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(server.mock.controllers.size, 0);
    assert.equal(server.mock.metrics.counters.active, 0);
    assert.equal(server.mock.metrics.counters.cancelled, 1);
    assert.equal(server.mock.metrics.counters.completed, 0);
  } finally {
    req.close(http2.constants.NGHTTP2_CANCEL);
    client.destroy();
  }
});
