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

Run `orihsus --config <path>`; the default is `/etc/orihsus/config.yaml`. The configuration file must have mode `0600`. See [config.example.yaml](config.example.yaml) for every field.

| Section | Purpose | Default |
| --- | --- | --- |
| `listen` | Loopback HTTP address; non-loopback values are rejected | `127.0.0.1:8080` |
| `gateway_token` | Gateway bearer token | required |
| `upstream.base_url` | HTTPS upstream URL | required |
| `keys` | Non-empty, unique key list | required |
| `models` | Non-empty unique static `/v1/models` entries; hot-reloadable | `["deepseek-chat"]` |
| `limits` | Concurrency, queue, timeout, and body limits | `200/500/30s/10MiB` |
| `rotation` | Backoff, circuit breaker, and attempt limits | `5s/60s/5/60s/2` |
| `usage` | Proactive usage threshold and polling interval; restart required | `80%/5m` |
| `audit.path`, `audit.queue_capacity` | JSONL audit output | `/var/log/orihsus/audit.jsonl`, `4096` |
| `server.read_header_timeout` | Header-read timeout | `5s` |
| `server.max_header_bytes` | Maximum header size | `32KiB` |
| `server.body_read_timeout` | Request-body deadline | `30s` |
| `server.upstream_response_header_timeout` | Upstream send-to-headers timeout | `60s` |
| `server.upstream_error_body_timeout` | Complete error-body classification timeout | `5s` |

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
