# Deploying orihsus

## 1. Create the service account and directories

```bash
sudo useradd --system --home /var/lib/orihsus --shell /usr/sbin/nologin orihsus
sudo install -d -o root -g orihsus -m 0750 /etc/orihsus
sudo install -d -o orihsus -g orihsus -m 0750 /var/log/orihsus
sudo install -d -o orihsus -g orihsus -m 0750 /var/lib/orihsus
```

The service deliberately runs without root privileges. Use a high listen port such as 8443 and terminate or forward port 443 at a trusted front end if necessary.

## 2. Install the binary

```bash
cargo build --release
sudo install -o root -g root -m 0755 target/release/orihsus /usr/local/bin/orihsus
```

## 3. Install configuration and certificates

```bash
sudo install -o orihsus -g orihsus -m 0600 config.example.yaml /etc/orihsus/config.yaml
sudo install -o orihsus -g orihsus -m 0600 cert.pem /etc/orihsus/cert.pem
sudo install -o orihsus -g orihsus -m 0600 key.pem /etc/orihsus/key.pem
sudo chown orihsus:orihsus /etc/orihsus/config.yaml
sudo chown orihsus:orihsus /etc/orihsus/cert.pem /etc/orihsus/key.pem
sudo chmod 600 /etc/orihsus/config.yaml /etc/orihsus/cert.pem /etc/orihsus/key.pem
```

Edit the configuration before starting the service. Use distinct, high-entropy gateway and upstream credentials. The upstream URL must be HTTPS. Configuration validation rejects insecure modes, missing files, duplicate keys, invalid bounds, and unsupported combinations.

## 4. Install and start systemd service

```bash
sudo install -o root -g root -m 0644 deploy/orihsus.service /etc/systemd/system/orihsus.service
sudo systemctl daemon-reload
sudo systemctl enable --now orihsus
sudo systemctl status orihsus
```

The supplied unit uses a dedicated account, `NoNewPrivileges`, an empty capability set, filesystem protection, private temporary storage, restart-on-failure, file-descriptor limits, and `TimeoutStopSec=45`. The 45-second stop budget covers a ≤30s connection drain, a 5s audit flush, and scheduling margin. Keep the unit and configuration limits aligned when changing capacity.

## 5. Health checks

```bash
curl --cacert /etc/orihsus/cert.pem https://127.0.0.1:8443/healthz
curl --cacert /etc/orihsus/cert.pem https://127.0.0.1:8443/readyz
```

`/healthz` reports process liveness. `/readyz` returns an OpenAI-shaped 503 with `Retry-After` when no key can serve traffic. Method mismatches and authenticated client routes are audited according to the gateway contract.

## 6. Client configuration

Point an OpenAI-compatible client at `https://<gateway-host>:8443/v1`, trust the gateway certificate chain, and send `Authorization: Bearer <gateway_token>`. The gateway token is not an upstream key.

## 7. Logs and audit rotation

Read service diagnostics with:

```bash
journalctl -u orihsus -f
```

Audit records are JSONL at the configured path. They contain metadata and key fingerprints, never secrets or bodies. After an external rotation, signal the process so the audit writer reopens the path:

```bash
sudo systemctl kill -s HUP orihsus
```

Keep audit ownership writable by `orihsus`. A full queue or writer failure produces a sanitized one-time warning; request handling remains non-blocking.

## 8. Reload and restart boundaries

Changes to the gateway token, upstream base URL, key set, or model list are hot-reloadable through the configured file watcher. TLS material, listen/server settings, capacity limits, audit settings, and usage-poll scheduling require a restart.

Validate edits before replacing the live file, preserve mode `0600`, and use an atomic rename so the watcher never observes a partially written configuration.

## 9. Resource discipline

- Ensure `LimitNOFILE` comfortably exceeds `server.max_connections` plus upstream and audit descriptors.
- Size `max_inflight_body_bytes` deliberately. Each admitted body currently reserves `max_body_bytes`, so effective simultaneous body buffering is the quotient of those values.
- Monitor RSS high-water behavior, open descriptors, rejected/queued requests, upstream latency, audit warnings, and restart counts.
- Long SSE streams retain admission permits until completion or cancellation.

## 10. Upgrade and rollback

```bash
sudo systemctl stop orihsus
sudo install -o root -g root -m 0755 target/release/orihsus /usr/local/bin/orihsus
sudo systemctl start orihsus
```

Retain the previous binary and configuration until health, readiness, a small authenticated request, and audit output have been verified. Roll back both binary and schema-dependent configuration together.
