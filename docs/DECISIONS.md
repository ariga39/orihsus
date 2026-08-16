# orihsus Design Decisions

This document records the decisions that define the gateway contract.

## Language and scope

- Implement the service in stable Rust with Tokio, Axum, Reqwest, Rustls, Serde, and Tracing.
- Ship one native binary with no database, container requirement, administrative UI, or metrics endpoint.
- Expose `POST /v1/chat/completions`, `POST /v1/messages`, `POST /v1/responses`, `GET /v1/models`, `/healthz`, and `/readyz`. Return explicit 404 or 405 responses for everything else. Proxy each request in its native protocol without OpenAI/Anthropic/Responses conversion.
- Stream SSE and ordinary successful bodies without buffering them completely. Sanitize final upstream 401/403/429/5xx bodies into bounded OpenAI-compatible errors; preserve only a validated `Retry-After` on 429.

## Key rotation

- Use a fill-first pool rather than round-robin selection. Keep using a healthy key until it becomes unavailable.
- Take one request-level candidate snapshot so a hot reload cannot mix generations during a retry sequence.
- Attempt at most two distinct keys per request.
- Treat exact OpenCode `GoUsageLimitError` payloads as quota cooldowns. Recognized dimensions are `weekly`, `monthly`, and `5h`; parsed reset durations override dimension defaults.
- Treat ordinary 429 responses with `Retry-After` when valid, otherwise exponential backoff. Handle 401/403 as key failures and retry eligible 5xx/network failures without exposing credentials.
- Once an upstream error response has been committed, retry only another key that is available immediately. A different key's existing cooldown must never delay or replace a saved 5xx response with the gateway-level all-keys-cooling 429.
- When every key is cooling down, wait within a fixed pool budget, then return 429 with a conservative `Retry-After` derived from the earliest recovery. Reserve 503 for gateway capacity, timeout, shutdown, or upstream-transport failures.
- Poll the OpenCode usage API proactively. A key at or above the configured utilization threshold is cooled until the reported reset time; polling failures do not remove otherwise healthy keys.

## Logging and accounting

- Emit bounded, non-blocking JSONL audit records from a dedicated writer thread.
- Record request ID, raw bounded OpenCode session/project/request IDs, status, latency, token counts when available, final downstream outcome, and a truncated SHA-256 key fingerprint.
- Keep at most two attempt summaries per audit line: key fingerprint, header/first-byte/first-event latency, upstream byte/chunk/event activity, commit state, terminal reason, and failover target. Bound each OpenCode correlation header to 256 bytes; omit an oversized value.
- Never log raw API keys, gateway tokens, authorization headers, or request/response bodies.
- Allow audit records to be dropped when the bounded queue is full; expose a one-time sanitized warning and counters instead of blocking requests.
- Reopen the audit file on demand for log rotation, with a bounded acknowledgement wait.
- Do not enqueue health/readiness probes. Install the bounded daily logrotate policy in `deploy/orihsus.logrotate` (100 MiB maximum, 14 compressed generations, SIGHUP reopen).

## Security

- Fix the upstream service root to `https://opencode.ai/zen/go/`; reject every configurable `upstream` or `base_url` field. Construct credential-bearing requests only for the built-in `/v1/chat/completions`, `/v1/messages`, `/v1/responses`, and `/v1/usage` paths; serve `/v1/models` locally. This removes the configuration SSRF and key-exfiltration surface, including private, loopback, metadata, query, fragment, and custom-path targets.
- Serve plaintext HTTP only on loopback and place nginx at the public boundary for TLS, HTTP/2, rate limiting, access logging, and fail2ban.
- Never follow upstream redirects. Return 3xx responses unchanged so a selected key cannot escape the fixed API-path allowlist, even within the trusted origin.
- Require bearer authentication before admission control and body buffering on OpenAI-format endpoints. Accept the same gateway token as `x-api-key` on the Anthropic-format messages endpoint, replacing it with the selected upstream key.
- Revalidate endpoint-appropriate authentication after admission so a queued request cannot survive token rotation.
- Keep YAML as the operator format but parse it with maintained `yaml_serde`; do not depend on unmaintained `serde_yaml` or unsound `serde_yml`.
- Reject release builds that enable `loadtest-insecure-upstream`; its debug-only client is pinned to the loopback mock and requires synthetic credentials.
- On Unix, open configuration with `O_NOFOLLOW`, then verify the open descriptor is a regular file owned by the effective process user with mode `0600`, and read that same descriptor. Reject duplicate or blank secrets and redact secrets from formatting and errors.
- Preserve OpenCode client semantics with a request-header allowlist derived from the installed SDK and CLI bundle. The Go-provider request path sends `x-opencode-project` (when available), `x-opencode-session`, `x-opencode-request`, `x-opencode-client`, and `User-Agent: opencode/<version>`; the SDK client also defines `x-opencode-directory` and v2 `x-opencode-workspace`. Forward the `x-opencode-*` namespace for compatibility plus known-safe `Content-Type`, `Accept`, and `User-Agent`; for `/v1/messages`, also forward `anthropic-version` and `anthropic-beta`. Deny rules take precedence over the prefix rule and remove cookies, authorization/API-key variants, hop-by-hop and `Connection`-named headers, forwarding identity, and tracing baggage. Replace client credentials with the selected upstream key and overwrite `x-request-id` with the gateway's sanitized value.
- Refuse to run as root and use a dedicated system account with a restrictive systemd sandbox.

## Capacity and lifecycle

- Bound active requests, queued requests, total accepted connections, request-body memory, model values (256 bytes and configured allowlist), error-body classification, audit buffering, and SSE streams.
- Reject a full admission queue immediately with `503` and `Retry-After: 1`; apply a finite queue wait deadline.
- Reserve body-budget permits before buffering a request body and hold them through upstream request construction.
- Apply deadlines independently to HTTP header reads, request bodies, upstream response headers, the first complete SSE event, gaps between committed SSE events, upstream error-body reads, and downstream writes. nginx independently bounds public TLS and client behavior.
- Treat the first complete SSE `data:` event as the downstream commit point. Preserve every prefetched byte verbatim under a 256 KiB cap; before commit a silent/broken attempt may fail over, while after commit a silent stream is terminated without changing keys or stitching streams.
- Track no-first-event and committed event-idle failures by `(key, model)`, so one model's liveness does not cool unrelated models. A client cancellation before the first-event deadline is not a key failure.
- Hold an admission permit until a streamed response reaches EOF, fails, or is cancelled.
- Give SSE responses a separate cap of one quarter of `max_concurrency` (at least one); reject excess streams with 503 and `Retry-After: 1`.
- Reap completed connection tasks continuously and bound graceful shutdown even if an audit writer is blocked.

## Deployment and testing

- Deploy orihsus with systemd as a dedicated account and nginx as the only public listener. Keep certificate lifecycle and public edge policy outside the application.
- Hot-reload only the gateway token, keys, and model list. Listener, capacity, server, audit, and proactive-usage scheduling changes require restart; the upstream origin and path allowlist are compiled in.
- Test state machines deterministically and use real sockets/files for lifecycle, timeout, reload, and streaming boundaries.
- Treat formatter, Clippy with warnings denied, unit/integration tests, release build, and load-test tooling checks as release gates.

## Accepted trade-offs

- In-memory key state resets on process restart.
- A bounded audit queue may lose records under sustained writer failure.
- Streaming prevents replay after the first complete SSE event has been committed.
- The fixed-per-request body reservation is simple and safe but can reduce small-body concurrency when `max_body_bytes` is large.
