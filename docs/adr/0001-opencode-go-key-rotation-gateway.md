# ADR-0001: OpenCode Go Multi-Key Rotation Gateway

- Status: Accepted
- Date: 2026-08-13

## Context

OpenCode Go subscription keys have independent usage limits and recovery times. Clients expect an OpenAI-compatible endpoint, including incremental SSE delivery, while operators need bounded resource use, safe key failover, auditable outcomes, and a deployable single service.

## Decision drivers

- Never leak an upstream key to clients, redirects, diagnostics, or audit logs.
- Continue serving when one key reaches a quota or fails.
- Preserve streaming latency without exposing upstream error bodies or metadata.
- Bound memory, connections, concurrency, queueing, retries, and shutdown.
- Keep deployment and state management simple.

## Decision

### System boundary

Implement one Rust process that authenticates gateway clients, exposes a small OpenAI-compatible route set on loopback HTTP, forwards to an HTTPS OpenCode upstream, rotates keys, and writes JSONL audit records. nginx is the only public listener and terminates TLS/HTTP2. Do not add a database, management UI, or arbitrary proxying.

### Components

Use separate modules for configuration/runtime snapshots, key-pool state, admission and body budgets, gateway protocol handling, audit output, file watching, proactive usage polling, and the HTTP server lifecycle. Keep public seams narrow enough for deterministic tests.

### Key state machine

Use fill-first selection. A request receives a snapshot of distinct candidates and can try at most two. Exact usage-limit payloads cool the selected key until the parsed or default reset; ordinary 429, 401/403, 5xx, and network failures follow their own bounded transitions. Concurrent results are ordered so stale completions cannot erase newer failure state.

When every candidate is unavailable, wait only within the configured pool deadline. On exhaustion, return 429 with a conservative `Retry-After`; reserve 503 for service and transport failures. Proactive usage polling may cool keys but cannot expose credentials or invalidate a newer key generation.

### Capacity control

Use semaphores for active requests, queued waiters, accepted connections, total buffered request-body bytes, and SSE streams. Cap SSE at one quarter of active-request capacity (at least one). Reject overflow immediately and place deadlines around every externally controlled wait. Use bounded channels for response pumping and audit work.

### Streaming boundary

Forward SSE and ordinary successful bodies incrementally. Retries are allowed only before downstream headers are committed. After commitment, upstream errors terminate the stream and update pool/audit state; downstream cancellation or write timeout cancels upstream work and releases admission.

### Configuration

Parse and validate a mode-`0600` YAML file with maintained `yaml_serde`. Require a loopback listen address. Publish one immutable runtime snapshot containing the hot-reloadable token, keys, and model allowlist; each model is limited to 256 UTF-8 bytes. Changes to listener, capacity, audit, or polling schedules require restart. The upstream URL is not configurable.

### Network and security

Fix the upstream service root to `https://opencode.ai/zen/go/` and allow credential-bearing requests only to the built-in chat-completions and usage paths. Reject all upstream URL configuration, preventing SSRF and key disclosure through private, loopback, metadata, query, fragment, or custom-path targets. Never follow redirects, including same-origin redirects, because they could escape the path allowlist. Forward only content negotiation/type and a sanitized request ID from the client boundary; drop cookies, alternate API keys, forwarding identity, tracing baggage, and extension headers. Reject root execution, bind loopback HTTP only, limit header size/read time, authenticate before admission/body allocation, and authenticate again after a queued request is admitted. On Unix, open config with no-follow and validate owner/type/mode on the same descriptor that is read. A debug-only load-test feature pins its mock to loopback; enabling its TLS bypass in a release build is a compile error. nginx owns public TLS, HTTP/2, connection policy, rate limiting, and fail2ban-visible access logs.

### Audit

Send sanitized, size-bounded records to a bounded non-blocking queue consumed by a writer thread. Record fingerprints and outcomes, not bodies or secrets, and omit health/readiness probes. Support bounded reopen and shutdown operations; ship logrotate policy and prefer a dropped audit record plus warning over blocking request traffic.

### Deployment

Ship a hardened systemd unit for a dedicated user. Graceful shutdown stops acceptance, drains bounded server work, stops background tasks, and attempts a bounded audit flush.

## Acceptance contract

- Only documented routes and methods are accepted.
- Credentials never appear in external responses or logs.
- Admission is exactly bounded by configured active and queued capacities.
- Request bodies and error classification cannot grow without limit.
- Streaming permits are released on EOF, upstream failure, downstream cancellation, or timeout.
- Hot reload cannot combine key generations within a request.
- Tests cover state transitions, real-socket timeouts, streaming, rotation, audit lifecycle, and shutdown.

## Consequences

The design is operationally simple, memory-bounded, and resilient to individual key failures. It also makes several trade-offs: key health is in-memory, audit delivery is best effort under writer failure, a committed stream cannot be retried, and conservative body reservation can limit small-body concurrency.

## Rejected alternatives

- Per-request round robin: wastes healthy quota and increases behavioral variance.
- SQLite or external state: unnecessary operational complexity for ephemeral cooldown state.
- Unbounded concurrency or waits: permits trivial resource exhaustion.
- Buffering complete SSE responses: destroys streaming latency and increases memory use.
- An unauthenticated public endpoint: exposes paid upstream capacity.
- Built-in dashboards or a management API: expands the attack surface beyond the required gateway.
