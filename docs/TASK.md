# orihsus — OpenCode Go Key-Rotation Gateway

## Objective

Build a production-oriented Rust gateway that exposes native OpenAI Chat, Anthropic Messages, and OpenAI Responses endpoints, forwards requests to OpenCode Go without format conversion, rotates subscription API keys safely, and remains bounded under slow or hostile clients.

## Required behavior

- Serve `POST /v1/chat/completions`, `POST /v1/messages`, `POST /v1/responses`, `GET /v1/models`, `/healthz`, and `/readyz` as loopback HTTP behind nginx.
- Authenticate gateway clients before admission or body allocation: Bearer for OpenAI endpoints and Bearer or `x-api-key` for Anthropic Messages.
- Select keys from a fill-first pool and fail over at most once per request.
- Classify OpenCode usage-limit responses, ordinary rate limits, authentication failures, upstream failures, and network errors without leaking secrets.
- Preserve SSE and non-SSE streaming and stop upstream work when the downstream disappears or exceeds its write deadline.
- Hot-reload the safe runtime subset atomically.
- Emit bounded JSONL audit records with token counts and key fingerprints.
- Provide explicit bounds for connections, active work, queueing, body memory, timeouts, retries, and shutdown.
- Include a hardened systemd deployment and documented operational procedure.

## Technical constraints

- Stable Rust; Tokio/Axum/Reqwest/Rustls.
- One process and no persistent database.
- Fixed HTTPS OpenCode Go upstream and no redirect following.
- Secrets must be absent from logs, formatting, errors, and audit output.
- Production behavior must remain testable through narrow public seams and deterministic state-machine tests.

## Module map

- `config`: parsing, validation, redaction, and reloadable runtime snapshots.
- `pool`: key selection, cooldowns, circuit breaking, and request-level candidates.
- `queue`: bounded admission and global request-body budget.
- `gateway`: routing, authentication, retries, error classification, and streaming.
- `audit`: bounded JSONL writer, rotation, counters, and shutdown.
- `hot_reload`: filesystem watching and atomic runtime updates.
- `server`: loopback HTTP, connection limits, protocol watchdogs, and graceful shutdown.
- `usage`: proactive usage polling and key cooldown updates.
- `main`: process validation, component assembly, signals, and lifecycle ordering.

## Completion criteria

- `cargo fmt --check`, Clippy with warnings denied, all tests, and release build pass.
- Configuration and deployment examples match the implemented schema.
- Capacity and timeout boundaries are exercised with real sockets and the load-test tools.
- Documentation is in English and accurately describes restart versus hot-reload behavior.
