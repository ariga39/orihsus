# orihsus

A lightweight OpenAI-compatible gateway for OpenCode Go that rotates multiple subscription API keys. It is a single Rust binary built with Tokio and Axum, served as loopback HTTP behind nginx; it requires neither Docker nor a database.

## Features

- Transparently proxies `POST /v1/chat/completions`, Anthropic-format `POST /v1/messages`, and Responses-format `POST /v1/responses`, with no protocol conversion. `GET /v1/models` remains local; other routes return 404 or 405.
- Uses a fill-first key pool. `GoUsageLimitError` cools a key by quota dimension, while ordinary 429 responses use `Retry-After` or exponential backoff.
- Tries at most two keys per request. Retryable upstream errors become sanitized OpenAI-compatible JSON; upstream bodies and metadata are never exposed.
- Enforces gateway bearer authentication, header-read timeouts, and a maximum header size; nginx terminates public TLS and HTTP/2.
- Writes JSONL audit records with OpenCode session/project/request IDs, token counts, and bounded per-attempt streaming telemetry. Keys are represented only by the first 12 hexadecimal characters of a SHA-256 fingerprint.
- Records each successful usage poll as redacted daily JSONL history for capacity planning; disk failures never block polling or cooldown decisions.
- Hot-reloads tokens, keys, and models. When no manual model list is configured, the allowlist is refreshed from OpenCode Go at startup and hourly thereafter.
- Bounds concurrency, queued requests, request bodies, model names, SSE streams, error classification, and the audit queue.

## Quick start

```bash
cargo build --release
sudo install -m 0755 target/release/orihsus /usr/local/bin/orihsus
sudo install -m 0600 config.example.yaml /etc/orihsus/config.yaml
orihsus --config /etc/orihsus/config.yaml
```

Print the package version and the seven-character commit captured at build time with `orihsus --version` (or `-V`). Builds without an available Git checkout report `commit unknown`.

Production setup, including nginx TLS termination, the dedicated user, systemd sandbox, and audit permissions, is documented in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). The unit file is [deploy/orihsus.service](deploy/orihsus.service).

## Configuration

Run `orihsus --config <path>`; the default path is `/etc/orihsus/config.yaml`, and the file must have mode `0600`. The [example configuration](config.example.yaml) stays short: it contains required values plus the model list that operators commonly decide. The table below is the complete schema.

Configuration values are scalar strings or numbers. Durations are integer seconds and use a `_seconds` suffix; byte capacities are integer bytes and use a `_bytes` suffix. URLs and filesystem paths remain strings. Unknown fields and the old composite-string schema are rejected.

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `gateway_token` | string | yes | — | Gateway client token: Bearer authentication on OpenAI endpoints, or `x-api-key` on `/v1/messages`; must not be blank. Hot-reloadable. |
| `keys` | list of strings | yes | — | Non-empty, unique upstream key pool. Hot-reloadable. |
| `listen.host` | string (IP) | no | `127.0.0.1` | Loopback IP address. Non-loopback addresses are rejected. |
| `listen.port` | integer | no | `8080` | Local HTTP port, from 0 through 65535. |
| `models` | list of strings | no | synchronized | Manual non-empty, unique allowlist; each UTF-8 value is at most 256 bytes. When present it overrides and disables automatic synchronization. Hot-reloadable. |
| `model_sync.enabled` | boolean | no | `true` | Synchronize the allowlist from the public OpenCode Go `/v1/models` endpoint when `models` is absent. A failed or invalid refresh keeps the last-known-good list. Hot-reloadable. |
| `model_sync.interval_seconds` | integer | no | `3600` | Model refresh interval; at least `30`. The first refresh runs immediately at startup. Hot-reloadable. |
| `limits.max_concurrency` | integer | no | `200` | Maximum requests actively handled. |
| `limits.max_queue` | integer | no | `500` | Maximum requests waiting for admission; zero disables waiting. |
| `limits.queue_wait_timeout_seconds` | integer | no | `30` | Maximum time an admitted-to-queue request may wait; must be positive. |
| `limits.max_body_bytes` | integer | no | `10485760` | Maximum bytes in one request body. |
| `limits.max_inflight_body_bytes` | integer | no | `268435456` | Global resident request-body budget; at least `max_body_bytes`. |
| `key_failure_handling.backoff_initial_seconds` | integer | no | `5` | Initial cooldown after an ordinary key failure. |
| `key_failure_handling.backoff_max_seconds` | integer | no | `60` | Maximum exponential-backoff cooldown; at least the initial value and at most `7776000` (90 days). |
| `key_failure_handling.breaker_threshold` | integer | no | `5` | Consecutive failures that open a key's circuit breaker. |
| `key_failure_handling.breaker_cooldown_seconds` | integer | no | `60` | How long an opened key circuit remains unavailable. |
| `key_failure_handling.max_attempts` | integer | no | `2` | Total attempts across keys for one request; `1` or `2`. |
| `usage.soft_threshold_percent` | number | no | `80` | Usage percentage above which a key is proactively cooled; in `(0, 100]`. |
| `usage.poll_interval_seconds` | integer | no | `300` | OpenCode Go usage polling interval; at least `30`. |
| `usage_history_dir` | string (path) | no | `/var/log/orihsus/usage` | Directory for UTC-daily `YYYY-MM-DD.jsonl` usage snapshots. Changes require restart. |
| `audit.path` | string (path) | no | `/var/log/orihsus/audit.jsonl` | JSONL audit file. |
| `audit.queue_capacity` | integer | no | `4096` | In-memory audit queue capacity; must be positive. |
| `server.read_header_timeout_seconds` | integer | no | `5` | Deadline for reading request headers. |
| `server.max_header_bytes` | integer | no | `32768` | Maximum request-header bytes; at least `8192`. |
| `server.max_connections` | integer | no | `1024` | Simultaneous accepted TCP connections; at most `65536`. |
| `server.body_read_timeout_seconds` | integer | no | `30` | Deadline for reading an entire client request body. |
| `server.upstream_response_header_timeout_seconds` | integer | no | `60` | Deadline from sending upstream request to receiving response headers. |
| `server.first_event_timeout_seconds` | integer | no | `60` | Deadline from upstream headers to the first complete SSE `data:` event. Before this event the response is uncommitted and may fail over. |
| `server.inter_event_timeout_seconds` | integer | no | `90` | Maximum silence between complete SSE events after commit; expiry terminates the stream without failover. |
| `server.model_event_timeouts.<model>.first_event_timeout_seconds` | integer | no | global value | Per-model first-event override; omitted fields inherit the global default. Model names are non-blank and at most 256 bytes. |
| `server.model_event_timeouts.<model>.inter_event_timeout_seconds` | integer | no | global value | Per-model inter-event override; omitted fields inherit the global default. |
| `server.upstream_error_body_timeout_seconds` | integer | no | `5` | Deadline for reading a retryable error body for classification. |
| `server.response_write_timeout_seconds` | integer | no | `30` | Per-chunk deadline when forwarding a response to a slow client. |

`key_failure_handling` configures what happens when the currently selected upstream key fails. Ordinary failures use exponential backoff; repeated failures open that key's circuit breaker; `max_attempts` controls whether the same client request may fail over to one other key. It does not configure scheduled key replacement.

Only `gateway_token` and `keys` are required. Omitted optional sections use the defaults above. Before the first successful automatic refresh, the service retains `deepseek-chat` as a fail-safe allowlist. Listener, limits, key-failure handling, usage, audit, and server changes require restart. The upstream is fixed at `https://opencode.ai/zen/go/`; any `upstream` or `base_url` configuration is rejected. Only the fixed `/v1/chat/completions`, `/v1/messages`, `/v1/responses`, and authenticated `/v1/usage` upstream paths can receive subscription keys; model synchronization uses unauthenticated `GET /v1/models`, while the gateway's own `/v1/models` remains local. Request and successful response bodies are forwarded unchanged; clients must choose the protocol endpoint matching their model.

Each successful per-key usage response appends one record to `<usage_history_dir>/YYYY-MM-DD.jsonl`, selected by the poll's UTC timestamp. Records contain only the key fingerprint plus `rolling`, `weekly`, and `monthly` status, percent, and reset time; raw keys and monetary estimates are never written. Missing API fields are represented as `null`. Files are not merged or rotated by orihsus. Summarize the last seven UTC days with:

```bash
node tools/usage-summary.mjs --dir /var/log/orihsus/usage --days 7
```

The summary emits one row per day and fingerprint with sample count and each window's latest, maximum, and average percentage and latest `resetsAt`.

For SSE, orihsus buffers raw upstream bytes only until the first complete `data:` event (bounded by 256 KiB). A timeout, connection failure, or EOF in that precommit window may use the second configured attempt; the client sees only the winning attempt. After the first event is committed, orihsus never splices streams or changes keys. Only a complete SSE event containing a `data:` field resets the inter-event deadline: comments, keepalives, and incomplete fragments are forwarded transparently but do not count as model activity. An inter-event timeout ends that stream and records a key-and-model-scoped liveness failure.

```yaml
server:
  first_event_timeout_seconds: 60
  inter_event_timeout_seconds: 90
  model_event_timeouts:
    deepseek-reasoner:
      first_event_timeout_seconds: 120
      inter_event_timeout_seconds: 180
```

## Testing

```bash
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Tests use real files and sockets with temporary directories, filesystem notifications, and local mock upstreams. Internal modules are not mocked.

Install the repository's commit hooks with:

```bash
pipx install pre-commit
pre-commit install
pre-commit run --all-files
```

The hooks run `cargo fmt --check` and Clippy with warnings denied. Rust filename conventions are maintained through normal code review.

## Security

- The process refuses to run as root. Production uses a dedicated account and a systemd sandbox with `NoNewPrivileges`, `ProtectSystem=strict`, and no capabilities.
- On Unix, configuration must be a non-symlink regular file owned by the process user with mode `0600`; validation and reading use the same open descriptor. Keys and tokens are never emitted through `Debug`, `Display`, or logs.
- nginx is the only public listener and owns TLS, HTTP/2, edge rate limiting, and fail2ban-compatible access logs. orihsus rejects non-loopback listen addresses.
- The OpenCode upstream is a built-in HTTPS origin and redirects are never followed, so the selected `Authorization` header cannot escape the API-path allowlist.
- Credential-bearing upstream requests preserve the `x-opencode-*` namespace used by OpenCode. Its Go-provider request path currently emits `project`, `session`, `request`, and `client`; the installed SDK also defines `directory` and `workspace`. Known-safe protocol headers (`Content-Type`, `Accept`, `User-Agent`, plus Anthropic version/beta headers where applicable) are preserved too. The gateway overwrites `x-request-id` with its sanitized value and upstream authorization with the selected key. Client cookies, authorization/API-key variants (including names under an allowed prefix), hop-by-hop headers, forwarding identity, tracing baggage, and unknown extensions are dropped.
- Only the documented routes are exposed; the service cannot act as an arbitrary proxy.
- Production builds cannot enable the certificate-verification bypass used by the isolated loopback load-test harness.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
