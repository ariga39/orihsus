# ADR-0001: OpenCode Go Multi-Key Rotation Gateway

- Status: Accepted
- Date: 2026-08-13

## Context

OpenCode Go subscription keys have independent usage limits and recovery times. Clients expect an OpenAI-compatible endpoint, including incremental SSE delivery, while operators need bounded resource use, safe key failover, auditable outcomes, and a deployable single service.

## Decision drivers

- Never leak an upstream key to clients, redirects, diagnostics, or audit logs.
- Continue serving when one key reaches a quota or fails.
- Preserve streaming latency and upstream error semantics.
- Bound memory, connections, concurrency, queueing, retries, and shutdown.
- Keep deployment and state management simple.

## Decision

### System boundary

Implement one Rust process that terminates TLS, authenticates gateway clients, exposes a small OpenAI-compatible route set, forwards to an HTTPS OpenCode upstream, rotates keys, and writes JSONL audit records. Do not add a database, management UI, or arbitrary proxying.

### Components

Use separate modules for configuration/runtime snapshots, key-pool state, admission and body budgets, gateway protocol handling, audit output, file watching, proactive usage polling, and the TLS server lifecycle. Keep public seams narrow enough for deterministic tests.

### Key state machine

Use fill-first selection. A request receives a snapshot of distinct candidates and can try at most two. Exact usage-limit payloads cool the selected key until the parsed or default reset; ordinary 429, 401/403, 5xx, and network failures follow their own bounded transitions. Concurrent results are ordered so stale completions cannot erase newer failure state.

When every candidate is unavailable, wait only within the configured pool deadline. On exhaustion, return 503 with a conservative `Retry-After`. Proactive usage polling may cool keys but cannot expose credentials or invalidate a newer key generation.

### Capacity control

Use semaphores for active requests, queued waiters, accepted connections, and total buffered request-body bytes. Reject overflow immediately and place deadlines around every externally controlled wait. Use bounded channels for response pumping and audit work.

### Streaming boundary

Forward SSE and ordinary successful bodies incrementally. Retries are allowed only before downstream headers are committed. After commitment, upstream errors terminate the stream and update pool/audit state; downstream cancellation or write timeout cancels upstream work and releases admission.

### Configuration

Parse and validate a mode-`0600` YAML file. Publish one immutable runtime snapshot containing the hot-reloadable token, upstream URL, keys, and model list. Changes to listener, TLS, capacity, audit, or polling schedules require restart.

### Network and security

Require upstream HTTPS and follow redirects only within the same origin. Serve TLS directly, reject root execution, limit header size/read time, and apply dedicated TLS/HTTP watchdogs. Authenticate before admission or body allocation.

### Audit

Send sanitized records to a bounded non-blocking queue consumed by a writer thread. Record fingerprints and outcomes, not bodies or secrets. Support bounded reopen and shutdown operations; prefer a dropped audit record plus warning over blocking request traffic.

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
