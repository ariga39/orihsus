# orihsus Design Decisions

This document records the decisions that define the gateway contract.

## Language and scope

- Implement the service in stable Rust with Tokio, Axum, Reqwest, Rustls, Serde, and Tracing.
- Ship one native binary with no database, container requirement, administrative UI, or metrics endpoint.
- Expose only `POST /v1/chat/completions`, `GET /v1/models`, `/healthz`, and `/readyz`. Return explicit 404 or 405 responses for everything else.
- Preserve successful and error response semantics from the upstream whenever the gateway can safely do so. Stream both SSE and ordinary successful bodies instead of buffering them completely.

## Key rotation

- Use a fill-first pool rather than round-robin selection. Keep using a healthy key until it becomes unavailable.
- Take one request-level candidate snapshot so a hot reload cannot mix generations during a retry sequence.
- Attempt at most two distinct keys per request.
- Treat exact OpenCode `GoUsageLimitError` payloads as quota cooldowns. Recognized dimensions are `weekly`, `monthly`, and `5h`; parsed reset durations override dimension defaults.
- Treat ordinary 429 responses with `Retry-After` when valid, otherwise exponential backoff. Handle 401/403 as key failures and retry eligible 5xx/network failures without exposing credentials.
- When every key is cooling down, wait within a fixed pool budget, then return 503 with a conservative `Retry-After` derived from the earliest recovery.
- Poll the OpenCode usage API proactively. A key at or above the configured utilization threshold is cooled until the reported reset time; polling failures do not remove otherwise healthy keys.

## Logging and accounting

- Emit bounded, non-blocking JSONL audit records from a dedicated writer thread.
- Record request ID, route, status, latency, token counts when available, outcome, and a truncated SHA-256 key fingerprint.
- Never log raw API keys, gateway tokens, authorization headers, or request/response bodies.
- Allow audit records to be dropped when the bounded queue is full; expose a one-time sanitized warning and counters instead of blocking requests.
- Reopen the audit file on demand for log rotation, with a bounded acknowledgement wait.

## Security

- Require HTTPS upstream URLs and serve TLS directly.
- Follow redirects only within the same origin. Never forward the selected authorization header across an origin, port, or scheme boundary.
- Require bearer authentication before admission control and body buffering.
- Require configuration mode `0600`, reject duplicate or blank secrets, and redact secrets from formatting and errors.
- Refuse to run as root and use a dedicated system account with a restrictive systemd sandbox.

## Capacity and lifecycle

- Bound active requests, queued requests, total accepted connections, request-body memory, error-body classification, and audit buffering.
- Reject a full admission queue immediately with `503` and `Retry-After: 1`; apply a finite queue wait deadline.
- Reserve body-budget permits before buffering a request body and hold them through upstream request construction.
- Apply deadlines independently to TLS/header reads, request bodies, upstream response headers, upstream error-body reads, and downstream writes.
- Hold an admission permit until a streamed response reaches EOF, fails, or is cancelled.
- Reap completed connection tasks continuously and bound graceful shutdown even if an audit writer is blocked.

## Deployment and testing

- Deploy with systemd, a dedicated account, explicit file ownership, restart policy, and bounded stop time.
- Hot-reload only the gateway token, upstream base URL, keys, and model list. Capacity, TLS, server, audit, and proactive-usage scheduling changes require restart.
- Test state machines deterministically and use real sockets/files for lifecycle, timeout, reload, TLS, and streaming boundaries.
- Treat formatter, Clippy with warnings denied, unit/integration tests, release build, and load-test tooling checks as release gates.

## Accepted trade-offs

- In-memory key state resets on process restart.
- A bounded audit queue may lose records under sustained writer failure.
- Streaming prevents replay after response headers have been committed.
- The fixed-per-request body reservation is simple and safe but can reduce small-body concurrency when `max_body_bytes` is large.
