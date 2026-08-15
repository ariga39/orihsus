# mock-upstream

A programmable HTTPS upstream for `docs/loadtest-plan.md`. It uses only the Node.js standard library and runs as one asynchronous event-loop process. HTTP/1.1 and HTTP/2 are negotiated with ALPN. Node.js 20+ is required; OpenSSL is needed only to generate test certificates.

## Start

```sh
cd tools/loadtest/mock-upstream
./generate-cert.sh ./certs
node server.js \
  --cert ./certs/server-cert.pem \
  --key ./certs/server-key.pem \
  --host 127.0.0.1 --port 8443 \
  --max-concurrency 400 --max-waiting 0 \
  --max-connections 1024
```

Generated certificates are test-only, valid for 30 days, and contain `localhost` and `127.0.0.1` SANs. Trust `certs/server-cert.pem` in the test client and gateway, or use `curl --cacert certs/server-cert.pem`. Never reuse these certificates or keys in production.

Every CLI option has an environment equivalent: `MOCK_HOST`, `MOCK_PORT`, `MOCK_TLS_CERT`, `MOCK_TLS_KEY`, `MOCK_MAX_CONCURRENCY`, `MOCK_MAX_WAITING`, `MOCK_MAX_CONNECTIONS`, `MOCK_WAIT_TIMEOUT_MS`, `MOCK_CONTROL_TOKEN`, `MOCK_DETAILED_EVENTS`, `MOCK_EVENT_BUFFER_SIZE`, and `MOCK_MAX_REQUEST_BODY_BYTES`. Run `node server.js --help` for a summary.

Formal local tests normally use `--max-concurrency 400 --max-waiting 0`. Invalidate any run in which `waiting`, `rejected`, or `limit_hits` is nonzero. Startup emits one JSON line containing the actual listen port.

## Request scripts

The data endpoint is `POST /v1/chat/completions`. A request selects its script in this priority order:

1. `x-mock-script`, containing JSON or base64url-encoded JSON. Encoding is recommended to avoid header escaping.
2. The request JSON `mock` field.
3. An `x-mock-scenario` registered through the control API.
4. A control rule matching key, request ID, or attempt.
5. The default immediate chat-completion JSON response.

`x-request-id` is echoed as `x-mock-request-id`. Detailed events contain only a 12-character SHA-256 authorization fingerprint, never the original value.

Complete script example:

```json
{
  "status": 200,
  "headers": { "content-type": "application/json", "x-test": "yes" },
  "headerDelayMs": 100,
  "bodyStartDelayMs": 250,
  "barriers": {
    "headers": { "name": "all-arrived", "target": 200 },
    "body": "start-body",
    "chunk": "next-event",
    "eof": "finish"
  },
  "body": {
    "text": "response bytes",
    "chunkSize": 1024,
    "intervalMs": 10,
    "bytewise": false,
    "bytesPerSecond": 65536,
    "burstBytes": 4096,
    "contentLength": false,
    "infinite": false,
    "stallAfterBytes": 0
  }
}
```

- `headerDelayMs` and `bodyStartDelayMs` are independent. `"infinite"` or `null` remains silent until cancelled through the control API.
- Build a body with `text`, `base64`, or `size` plus `byte`. `bytewise` forces one-byte chunks. `bytesPerSecond` uses accumulated monotonic deadlines and `burstBytes` controls token bursts; otherwise `chunkSize` and `intervalMs` apply.
- `contentLength: true` sends a known length. The default is HTTP/1.1 chunking or HTTP/2 DATA frames. Infinite bodies generate small chunks on demand and do not buffer future output. `stallAfterBytes` stops permanently at the selected offset.
- Barriers may apply before headers, the first body byte, each SSE chunk, or EOF. A barrier releases automatically when its target arrives; omit the target for manual release.

SSE example:

```json
{
  "bodyStartDelayMs": 50,
  "sse": {
    "count": 100,
    "infinite": false,
    "intervalMs": 100,
    "intervalsMs": [0, 50, 200],
    "jitterMs": 10,
    "seed": 42,
    "eventBytes": 1024,
    "done": true,
    "disconnectAfter": 0,
    "silentAfter": 0,
    "silentAfterSend": false
  }
}
```

`intervalsMs` overrides and cycles instead of the fixed interval. `count: 0` with `silentAfterSend: true` sends headers and then remains silent. `infinite: true` ignores count. `disconnectAfter: N` closes after event N; `silentAfter: N` keeps the connection open without further output.

Error example:

```json
{
  "status": 429,
  "headers": { "retry-after": "2" },
  "body": {
    "usageLimit": { "limitName": "5h", "message": "Resets in 2 seconds" },
    "bytewise": true,
    "intervalMs": 1,
    "stallAfterBytes": 0
  }
}
```

Any `status` can produce 401, 403, or 5xx. `body.invalidJson` emits malformed JSON and `body.size: 70000` exceeds the gateway's 64 KiB classification prefix. Pacing and stalls also apply to error bodies.

Encode and send an inline script:

```sh
SCRIPT=$(node -e 'process.stdout.write(Buffer.from(JSON.stringify({sse:{count:3,intervalMs:100,done:true}})).toString("base64url"))')
curl --cacert certs/server-cert.pem -N \
  -H 'x-request-id: demo-1' -H "x-mock-script: $SCRIPT" \
  -d '{}' https://localhost:8443/v1/chat/completions
```

## Control API and barriers

The control API uses HTTPS JSON. When `--control-token TOKEN` is configured, send `Authorization: Bearer TOKEN`.

```sh
# Register named scenarios.
curl --cacert certs/server-cert.pem -H 'content-type: application/json' \
  -d '{"silent":{"sse":{"count":0,"silentAfterSend":true}}}' \
  https://localhost:8443/control/scenarios

# Release ten requests waiting at the headers phase; omit count to release all.
curl --cacert certs/server-cert.pem -H 'content-type: application/json' \
  -d '{"count":10,"phase":"headers"}' \
  https://localhost:8443/control/barriers/gate/release

# Cancel a barrier or permanently silent request.
curl --cacert certs/server-cert.pem -d '{}' https://localhost:8443/control/barriers/gate/cancel
curl --cacert certs/server-cert.pem -H 'content-type: application/json' \
  -d '{"requestId":"demo-1"}' https://localhost:8443/control/cancel
```

`POST /control/reset` clears barriers, attempt state, and accumulated metrics. It does not forcibly cancel active requests; call `/control/cancel` first between runs.

Rules are matched in order and can select raw authorization for test setup or the safer `keyFingerprint`, plus request ID and per-credential attempt number:

```json
{
  "rules": [
    {
      "name": "k1-first-attempt",
      "match": { "authorization": "Bearer K1", "attempt": 1 },
      "script": { "status": 429, "body": { "usageLimit": { "limitName": "5h" } } }
    },
    {
      "name": "specific-request",
      "match": { "requestId": "case-401" },
      "script": { "status": 401, "body": { "text": "unauthorized" } }
    }
  ]
}
```

POST the object to `/control/rules`. Attempts are counted by `(x-request-id, Authorization)` and reset by `/control/reset`.

## Structured metrics

`GET /metrics` requires no control token for easy collection. It and `GET /control/state` expose the same core schema:

- active/peak, waiting/peak, rejection, and limit-hit counters;
- active/peak connections and connection rejection/limit counters;
- completion, cancellation, internal error, status, byte/chunk, and observation-loss counters;
- handler capacity-wait and actual-minus-planned deadline percentiles;
- event-loop lag, RSS, process model, configured caps, and barrier state.

High-cardinality attempt events are stored only with `--detailed-events true`. Their in-memory ring is bounded; overflow increments `observation_dropped` instead of synchronously logging or writing. Disable them for throughput tests. When enabled for state-machine tests, require zero drops. Collect CPU, NIC, and descriptor data externally.

## Tests

```sh
npm test
```

Tests generate temporary self-signed certificates and exercise real HTTPS HTTP/1.1 and HTTP/2, independent timing, paced bodies, barriers, capacity waiting/rejection, SSE `[DONE]`, and key/attempt usage-limit rules. They install no packages and require no network access.
