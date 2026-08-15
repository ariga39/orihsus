# WORKLOG — orihsus

This log summarizes implementation work and verification. It intentionally records shipped behavior rather than development-process artifacts.

## 2026-08-13 — Core gateway

### Configuration

- Added typed YAML parsing, cross-field validation, mode-`0600` enforcement, secret redaction, and immutable runtime snapshots.
- Classified fields into hot-reloadable runtime data and restart-required server data.
- Added tests for missing/duplicate keys, invalid URLs and bounds, unsafe permissions, redaction, and atomic snapshot replacement.

### Audit

- Added bounded non-blocking JSONL audit delivery to a dedicated writer thread.
- Records include request identity, route/status, latency, token usage, outcome, and truncated SHA-256 key fingerprints.
- Added rotation reopen, dropped/write-failure counters, one-time sanitized warnings, and bounded shutdown.
- Verified JSON validity, redaction, queue overflow behavior, file reopen, blocked-writer handling, and flush semantics with real temporary files.

### Key pool

- Implemented fill-first selection with distinct per-request candidates, quota cooldowns, ordinary rate-limit backoff, authentication failure handling, and a network circuit breaker.
- Added strict `GoUsageLimitError` parsing for `weekly`, `monthly`, and `5h` dimensions and safe reset-duration parsing.
- Protected state against stale concurrent completions and bounded time arithmetic.
- Added a finite wait when all keys are cooling down and conservative `Retry-After` calculation.

### Admission and body budget

- Added separate active and queue semaphores with FIFO waiting, immediate queue-full rejection, and a finite queue deadline.
- Added a global body-memory semaphore. A request reserves `max_body_bytes` before buffering and holds it until the upstream request owns the body.
- Verified cancellation and timeout paths release all permits.

### Gateway

- Added bearer authentication, strict route/method behavior, OpenAI-shaped local errors, model listing, and upstream forwarding.
- Added bounded two-key failover and final-error preservation.
- Streamed SSE and ordinary successful responses through a bounded channel.
- Added request-body, upstream-header, error-body idle, and downstream-write deadlines.
- Added outcome reporting for completed streams, upstream failures, and client cancellation.
- Ensured every authenticated client route is audited, including queue rejection and health/readiness method errors; unauthenticated traffic is not audited.

### Hot reload

- Added filesystem notification with debounce and atomic runtime publication.
- Key-pool generation changes are coordinated with runtime snapshots so an in-flight request never mixes generations.
- Reload failures retain the last valid configuration and emit a sanitized warning.

### TLS server and process lifecycle

- Added direct Rustls serving, TLS/header watchdogs, a total accepted-connection limit, HTTP/2 first-request and keepalive protection, and one-request HTTP/1 connections.
- Reaped completed connection tasks continuously and made transient accept failures retryable.
- Added signal handling and server-first lifecycle error propagation.
- Graceful shutdown stops acceptance, bounds connection draining, stops background tasks, and attempts bounded audit flushing.
- Added the systemd unit, example configuration, deployment documentation, and a refusal to run as root.

## 2026-08-14 — Hardening

### Streaming and failure semantics

- Required complete error bodies for classification; oversized bodies are streamed without exposing a partial classified body.
- Added idle timeouts to continued oversized-error reads and ordinary successful upstream reads.
- Prevented retries after downstream headers are committed.
- Correctly reported mid-stream upstream failures to the key pool and audit subsystem.
- Cancelled upstream pumping when a downstream client stopped consuming for longer than the write budget.

### Resource lifecycle

- Extended body-budget ownership across every phase that retains complete request bytes.
- Added connection-level protection for unauthenticated, TLS-handshake, header-slowloris, and silent HTTP/2 clients.
- Made audit reopen and shutdown bounded even when storage is blocked.
- Removed blocking joins from `AuditWriter::drop` and retained explicit bounded flush on main failure paths.
- Disabled HTTP/1 keep-alive after the first request so idle connections cannot retain connection permits.

### Configuration and correctness

- Rejected unknown configuration fields.
- Made the model list configurable and hot-reloadable.
- Added bounded jitter arithmetic and safe time calculations.
- Parsed both delta-seconds and HTTP-date forms of `Retry-After`.
- Aligned the systemd `TimeoutStopSec` value with the application drain budget.
- Formatted readiness failures as OpenAI error objects with `Retry-After`.

### Proactive usage rotation

- Added an authenticated OpenCode usage client and periodic polling worker.
- Added configurable threshold and interval validation.
- Usage states at or over threshold cool a key until the reported reset time; stale poll results cannot affect a newer key generation.
- Poll errors are sanitized and do not make healthy keys unavailable.

### Verification

- Formatter, Clippy with warnings denied, unit/integration tests, and release builds passed at each completed slice.
- Real sockets covered TLS stalls, HTTP/1 header stalls, HTTP/2 silence, connection caps, stream cancellation, and shutdown ordering.
- Temporary files and controlled writers covered configuration reloads, audit rotation, queue saturation, write failure, and blocked shutdown.

## 2026-08-15 — Load testing and allocator work

- Added a programmable HTTPS mock upstream and a Rust load generator under `tools/loadtest/`.
- Verified the 200-active/500-queued admission boundary, 30-second queue timeout, usage-limit failover, slow-client release, 1,024-connection slowloris protection, body-budget behavior, and audit completeness.
- Confirmed that the system allocator retained a large RSS high-water mark after large payload tests even though permits, connections, and request objects were released.
- Switched production allocation to jemalloc after comparison runs showed materially better idle RSS reclamation without changing gateway semantics.
- Captured the reproducible plan and measurements in `docs/loadtest-plan.md`, `docs/loadtest-results.md`, and `docs/rss-investigation.md`.

## 2026-08-15 — nginx edge migration

- Removed direct TLS certificate/key configuration and Rustls server acceptance from orihsus.
- Changed the default listener to `127.0.0.1:8080` and rejected non-loopback listener addresses.
- Assigned public TLS, HTTP/2, rate limiting, access logging, and fail2ban integration to nginx while preserving HTTPS for the OpenCode upstream.
- Updated systemd, configuration, deployment guidance, and server/configuration tests for the loopback HTTP boundary.
