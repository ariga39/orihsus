# Deploying orihsus Behind nginx

orihsus is an internal HTTP service. It binds to `127.0.0.1:8080`; nginx is the only public listener and owns TLS, HTTP/2, public rate limiting, and the access logs used by fail2ban.

## 1. Create the service account and directories

```bash
sudo useradd --system --home /var/lib/orihsus --shell /usr/sbin/nologin orihsus
sudo install -d -o root -g orihsus -m 0750 /etc/orihsus
sudo install -d -o orihsus -g orihsus -m 0750 /var/log/orihsus
sudo install -d -o orihsus -g orihsus -m 0750 /var/lib/orihsus
```

## 2. Install the binary and configuration

```bash
cargo build --release
sudo install -o root -g root -m 0755 target/release/orihsus /usr/local/bin/orihsus
sudo install -o orihsus -g orihsus -m 0600 config.example.yaml /etc/orihsus/config.yaml
sudo chown orihsus:orihsus /etc/orihsus/config.yaml
sudo chmod 600 /etc/orihsus/config.yaml
```

Replace every placeholder secret. The listener defaults to loopback port 8080. To change it, use separate scalar values such as `listen: { host: "127.0.0.1", port: 8081 }`; validation rejects non-loopback hosts. The upstream URL must remain HTTPS.

## 3. Install the systemd service

```bash
sudo install -o root -g root -m 0644 deploy/orihsus.service /etc/systemd/system/orihsus.service
sudo systemctl daemon-reload
sudo systemctl enable --now orihsus
sudo systemctl status orihsus
```

The unit runs without root or capabilities and grants write access only to the audit directory. `TimeoutStopSec=45` covers the ≤30s connection drain, 5s audit flush, and scheduling margin.

Verify the private HTTP listener locally:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

Do not expose port 8080 through a firewall, container port mapping, or public interface.

## 4. Reverse proxy recommendation

nginx is the recommended and supported public edge. A single mature edge layer handles certificate issuance/renewal, TLS policy, HTTP/2, request limiting, client-IP logging, and fail2ban integration, while orihsus remains focused on authentication, key rotation, bounded admission, upstream retries, streaming, and audit records. This separation reduces the application's attack surface and gives every public service a consistent operational control point.

Direct public HTTPS from orihsus is not recommended or supported. Embedding certificate loading and renewal in the application complicates ownership, reloads, expiry monitoring, and incident response. It also fragments authentication-failure logs, preventing fail2ban and edge policy from using one authoritative access log. An nginx boundary adds a well-tested layer before the application and allows public connection/TLS abuse to be rejected without consuming orihsus resources.

### nginx configuration

Define the shared request-rate zone in the nginx `http` context, for example `/etc/nginx/conf.d/orihsus-rate-limit.conf`:

```nginx
limit_req_zone $binary_remote_addr zone=orihsus_per_ip:10m rate=10r/s;
```

Create a server such as `/etc/nginx/sites-available/orihsus`:

```nginx
server {
    listen 80;
    server_name api.example.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    http2 on;
    server_name api.example.com;

    ssl_certificate     /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    access_log /var/log/nginx/orihsus-access.log;
    error_log  /var/log/nginx/orihsus-error.log warn;

    client_max_body_size 10m;
    limit_req zone=orihsus_per_ip burst=20 nodelay;
    limit_req_status 429;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Authorization $http_authorization;
        proxy_set_header X-Request-Id $request_id;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Connection "";

        # Preserve incremental OpenAI/SSE delivery.
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_cache off;
        proxy_read_timeout 1h;
        proxy_send_timeout 60s;
    }
}
```

Validate and reload:

```bash
sudo ln -s /etc/nginx/sites-available/orihsus /etc/nginx/sites-enabled/orihsus
sudo nginx -t
sudo systemctl reload nginx
```

Adjust `client_max_body_size` together with orihsus `limits.max_body_bytes` (an integer number of bytes). Keep buffering disabled for SSE. If the installed nginx predates the `http2 on;` directive, use its supported `listen 443 ssl http2;` syntax instead.

### fail2ban for repeated 401 responses

The nginx access log is the authoritative public-client log because nginx sees the real peer address. Configure fail2ban to match repeated `401` responses in `/var/log/nginx/orihsus-access.log`; do not use the loopback peer observed by orihsus. A minimal filter is:

```ini
# /etc/fail2ban/filter.d/nginx-orihsus-auth.conf
[Definition]
failregex = ^<HOST> .* "(?:GET|POST) .*" 401 .*$
ignoreregex =
```

```ini
# /etc/fail2ban/jail.d/nginx-orihsus-auth.local
[nginx-orihsus-auth]
enabled = true
filter = nginx-orihsus-auth
logpath = /var/log/nginx/orihsus-access.log
findtime = 10m
maxretry = 10
bantime = 1h
```

Confirm the filter against the actual configured nginx log format with `fail2ban-regex` before enabling it. Exclude trusted monitoring addresses where appropriate.

## 5. Client configuration

Point OpenAI-compatible clients at `https://api.example.com/v1` and send `Authorization: Bearer <gateway_token>`. Clients trust the public nginx certificate; orihsus has no certificate or private-key configuration.

## 6. Logs and rotation

Use `journalctl -u orihsus -f` for process diagnostics and nginx logs for public connection/authentication policy. orihsus audit records are JSONL at the configured path and contain metadata and key fingerprints, never raw credentials or bodies.

After rotating the audit file, signal orihsus so its writer reopens the path:

```bash
sudo systemctl kill -s HUP orihsus
```

## 7. Reload and restart boundaries

The gateway token, upstream base URL, key set, and model list are hot-reloadable. Listener/server settings, capacity, key-failure handling, audit, and usage-poll scheduling require an orihsus restart. nginx certificates and edge policy are reloaded independently with `nginx -t && systemctl reload nginx`.

## 8. Resource discipline

- Ensure `LimitNOFILE` exceeds the orihsus connection cap plus upstream/audit descriptors.
- Monitor both nginx public connection/rate-limit metrics and orihsus admission, latency, RSS, descriptors, audit warnings, and restart counts.
- Long SSE responses retain orihsus admission permits until completion or cancellation.
- Size the global body budget deliberately: each request currently reserves the full per-request body maximum.

## 9. Upgrade and rollback

Stop orihsus, replace the binary, start it, then verify private health/readiness and one authenticated request through nginx. Keep nginx configuration stable during an application rollback unless the public contract also changed.
