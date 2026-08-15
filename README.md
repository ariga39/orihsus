# orihsus

A lightweight OpenAI-compatible gateway for OpenCode Go that rotates multiple subscription API keys. It is a single Rust binary built with Tokio and Axum, served as loopback HTTP behind nginx; it requires neither Docker nor a database.

## Features

- Supports `POST /v1/chat/completions` with transparent SSE streaming and `GET /v1/models`; other routes return 404 or 405.
- Uses a fill-first key pool. `GoUsageLimitError` cools a key by quota dimension, while ordinary 429 responses use `Retry-After` or exponential backoff.
- Tries at most two keys per request: the initially selected key and one failover key. The final upstream error is forwarded unchanged.
- Enforces gateway bearer authentication, header-read timeouts, and a maximum header size; nginx terminates public TLS and HTTP/2.
- Writes JSONL audit records with token counts. Keys are represented only by the first 12 hexadecimal characters of a SHA-256 fingerprint.
- Hot-reloads tokens, the upstream base URL, keys, and models. Other configuration changes require a restart.
- Bounds concurrency, queued requests, request bodies, error bodies, and the audit queue.

## Quick start

```bash
cargo build --release
sudo install -m 0755 target/release/orihsus /usr/local/bin/orihsus
sudo install -m 0600 config.example.yaml /etc/orihsus/config.yaml
orihsus --config /etc/orihsus/config.yaml
```

Production setup, including nginx TLS termination, the dedicated user, systemd sandbox, and audit permissions, is documented in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). The unit file is [deploy/orihsus.service](deploy/orihsus.service).

## Configuration

Run `orihsus --config <path>`; the default path is `/etc/orihsus/config.yaml`, and the file must have mode `0600`. The [example configuration](config.example.yaml) stays short: it contains required values plus the model list that operators commonly decide. The table below is the complete schema.

Configuration values are scalar strings or numbers. Durations are integer seconds and use a `_seconds` suffix; byte capacities are integer bytes and use a `_bytes` suffix. URLs and filesystem paths remain strings. Unknown fields and the old composite-string schema are rejected.

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `gateway_token` | string | yes | — | Bearer token required from gateway clients; must not be blank. Hot-reloadable. |
| `upstream.base_url` | string (URL) | yes | — | HTTPS OpenCode Go base URL without query or fragment. Hot-reloadable. |
| `keys` | list of strings | yes | — | Non-empty, unique upstream key pool. Hot-reloadable. |
| `listen.host` | string (IP) | no | `127.0.0.1` | Loopback IP address. Non-loopback addresses are rejected. |
| `listen.port` | integer | no | `8080` | Local HTTP port, from 0 through 65535. |
| `models` | list of strings | no | `["deepseek-chat"]` | Non-empty, unique values returned by `GET /v1/models`. Hot-reloadable. |
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
| `audit.path` | string (path) | no | `/var/log/orihsus/audit.jsonl` | JSONL audit file. |
| `audit.queue_capacity` | integer | no | `4096` | In-memory audit queue capacity; must be positive. |
| `server.read_header_timeout_seconds` | integer | no | `5` | Deadline for reading request headers. |
| `server.max_header_bytes` | integer | no | `32768` | Maximum request-header bytes; at least `8192`. |
| `server.max_connections` | integer | no | `1024` | Simultaneous accepted TCP connections; at most `65536`. |
| `server.body_read_timeout_seconds` | integer | no | `30` | Deadline for reading an entire client request body. |
| `server.upstream_response_header_timeout_seconds` | integer | no | `60` | Deadline from sending upstream request to receiving response headers. |
| `server.upstream_error_body_timeout_seconds` | integer | no | `5` | Deadline for reading a retryable error body for classification. |
| `server.response_write_timeout_seconds` | integer | no | `30` | Per-chunk deadline when forwarding a response to a slow client. |

`key_failure_handling` configures what happens when the currently selected upstream key fails. Ordinary failures use exponential backoff; repeated failures open that key's circuit breaker; `max_attempts` controls whether the same client request may fail over to one other key. It does not configure scheduled key replacement.

Only `gateway_token`, `upstream.base_url`, and `keys` are required. Omitted optional sections use the defaults above. Listener, limits, key-failure handling, usage, audit, and server changes require restart.

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
- Configuration must be mode `0600`; keys and tokens are never emitted through `Debug`, `Display`, or logs.
- nginx is the only public listener and owns TLS, HTTP/2, edge rate limiting, and fail2ban-compatible access logs. orihsus rejects non-loopback listen addresses.
- The OpenCode upstream must use HTTPS. Redirects are followed only when scheme, host, and effective port are unchanged, so the selected `Authorization` header cannot reach another origin.
- Only the documented routes are exposed; the service cannot act as an arbitrary proxy.
