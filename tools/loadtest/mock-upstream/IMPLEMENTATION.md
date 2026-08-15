# mock-upstream implementation record

Date: 2026-08-14 UTC

## Delivered

- Zero external npm dependencies; Node.js 20+ standard library only.
- TLS server using `http2.createSecureServer({ allowHTTP1: true })`, supporting HTTPS HTTP/1.1 and HTTP/2 on the same port.
- Explicit request concurrency, bounded waiting queue, queue timeout, and TCP connection caps. Structured counters include current/peak `active`, `waiting`, `rejected`, `limit_hits`, active/peak/rejected connections and connection limit hits.
- Per-request independent response-header and response-body-start delay, including control-plane-cancellable permanent silence.
- SSE fixed/list intervals, deterministic seeded jitter, finite/infinite event count, optional `[DONE]`, mid-stream disconnect, and silence after an event or after headers.
- Body chunk pacing, bytewise mode, interval mode, monotonic-deadline bytes/sec rate with burst, known Content-Length, HTTP/1.1 chunked / HTTP/2 DATA, infinite generated body, and mid-body stall.
- Arbitrary status/headers/body including 429 `GoUsageLimitError`, malformed or >64 KiB error bodies, 401/403/5xx, paced errors and stalled errors.
- Named and inline scripts plus ordered rules matching Authorization/key fingerprint, request ID, and attempt number.
- Header/body/chunk/EOF barriers with target-based automatic release or control-plane release of N waiters.
- HTTPS control endpoints for scenario/rule setup, barrier release/cancel, permanent-request cancellation, reset and state inspection.
- Structured JSON metrics for limits, request/status/byte/chunk counts, capacity queue time, planned-vs-actual timer error, event-loop lag, RSS, barriers and bounded optional attempt events. Authorization values are never emitted.
- Self-signed test certificate generation script and operational/CLI examples in `README.md`.

## Verification

Static syntax checks:

```text
node --check server.js
node --check lib/scripts.js
node --check lib/capacity.js
node --check lib/barriers.js
node --check lib/metrics.js
```

Result: all exited 0 with no output.

CLI smoke check:

```text
node server.js --help
```

Result: exited 0 and printed the option summary without requiring a certificate.

Automated integration tests:

```text
npm test
```

Final result: **7 tests passed, 0 failed** (925 ms reported by Node's test runner). Tests generate an ephemeral self-signed certificate and bind a local HTTPS port. Covered behavior:

1. HTTPS HTTP/1.1 default completion and metrics schema.
2. HTTP/2 ALPN/request handling.
3. Independent header/body-start delays and paced chunks.
4. Control-plane barrier inspection and release.
5. Active cap, bounded waiting, immediate 503 rejection, `Retry-After`, and counters.
6. Finite SSE event sequence and `[DONE]`.
7. Authorization/attempt rule selection for `GoUsageLimitError` and secret-redaction assertion.

The managed filesystem sandbox disallows binding localhost (`listen EPERM`), so the two successful integration runs were executed with the approved local-network test permission. No external network access or npm installation was used.

## Scope audit

All created or modified implementation files are under `tools/loadtest/mock-upstream/`. No load generator file was read or changed. Existing unrelated working-tree changes were left untouched.

## Operational boundary

This verification is functional, not a bypass capacity calibration. Before using results to evaluate orihsus, run the direct-to-mock staircase/long-stream/bandwidth/timer calibration described in `docs/loadtest-plan.md`, and require mock `waiting`, `rejected`, `limit_hits`, observation drops and unintended errors to remain zero for the formal round.

## 2026-08-14 response-close race fix

Integration load testing reported normal immediate/delayed bodies being classified as cancelled and occasionally arriving empty or truncated. The faulty lifecycle rule used the handler-local `finished` flag in the response `close` callback. Node emits `close` for a normally completed HTTP/2 stream as well as for a premature peer disconnect, and this event can race the async handler continuation after `response.end()`.

The fix makes protocol state authoritative:

- a response `close` cancels work only while `response.writableFinished` is false;
- finite responses await the response `finish` event after `response.end()` before incrementing `completed` or releasing request capacity;
- request `aborted` remains an unconditional cancellation while the handler is active.

The regression suite now includes:

- a minimal assertion that normal protocol-complete close is not cancellable while premature close is;
- 80 concurrent HTTP/2 responses over one reused session, each 1 MiB, mixing immediate/5 ms body starts and 257-byte/64 KiB chunks; every response is checked byte-for-byte at its boundaries and metrics must report `completed=80`, `cancelled=0`, `errors=0`;
- an actual HTTP/2 client `RST_STREAM CANCEL` against an infinite SSE response, which must report `completed=0`, `cancelled=1`.

Final verification after the fix:

```text
npm test
```

Result: **10 tests passed, 0 failed**. The preceding 9-test run also passed, including both high-volume normal completion and real-client-cancel integration cases. As before, localhost binding required the approved local test permission; no external network or external npm package was used.

## 2026-08-14 abortable backpressure fix

Integration testing found that `/control/cancel` could not clean up infinite large-event SSE streams while `response.write()` was backpressured. The server's write Promise waited only for `drain` or `error`; aborting the request controller therefore could not resume the handler, so its capacity lease and controller registration remained live. This presented as `active=50`, then `active=60` after ten new requests.

The write boundary now races three events:

- `drain` resolves the write;
- response `error` rejects it;
- `context.signal.abort` rejects it immediately with the signal reason.

Whichever event wins removes all three listeners. An already-aborted signal is checked after listener registration to close the registration race. When cancellation happens after response headers have been sent, the handler also destroys the response stream without injecting another error; its existing `finally` then releases the capacity lease and removes the controller registration.

The regression test uses real HTTP/2 flow control rather than a stub: it starts an infinite SSE with 64 KiB events, deliberately never reads the client response so the stream window fills and `response.write()` waits for `drain`, then calls `/control/cancel` from a separate HTTPS connection.

Pre-fix reproduction:

```text
node --test --test-name-pattern='control cancel interrupts' test/mock.test.js
```

Result: failed after 563 ms; control reported one cancellation request, but `active` and controller count remained 1 after the 500 ms cleanup budget. Destroying the client stream during test cleanup was the only way to release it.

Post-fix targeted result: passed in 44 ms, with `controllers.size=0`, `active=0`, `cancelled=1`, and `completed=0`.

Final verification:

```text
node --check server.js
node --check lib/scripts.js
npm test
```

Result: static checks passed; **11 tests passed, 0 failed**. The full suite includes the earlier normal-close race regressions, true client cancellation, and the new control-plane cancellation under real backpressure. Localhost binding used the approved local test permission; no external network or dependency installation was used.
