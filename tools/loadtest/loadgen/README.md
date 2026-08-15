# loadgen

`loadgen` is the stateful client for `docs/loadtest-plan.md`. It launches the initial wave behind a Tokio barrier, then starts a replacement only after one request finishes. Consequently `--concurrency N` means at most (and, while work remains, exactly) N unfinished requests rather than N queued tasks.

It supports HTTPS HTTP/1.1 and HTTP/2, a test CA or an explicit insecure mode, JSON validation, incremental SSE framing, paced reads, completely stopped reads, per-request JSONL, aggregate JSON, and raw TCP/TLS slowloris stages. TTFB is measured when response headers arrive; `first_event_ms` is the first complete SSE event; completion includes the complete response body (or the configured stopped-read hold).

`--write-bytes-per-sec N` streams the configured request body in 64KiB chunks against a monotonic-clock deadline. This is intended for body-budget and slow-upload tests; it does not pre-generate future chunks while the HTTP stack is backpressured.

## Build and examples

```sh
cargo build --release -p loadgen

# 200 HTTP/2 requests become eligible at one barrier; keep 200 in flight.
cargo run --release -p loadgen -- run \
  --url https://127.0.0.1:8443/v1/chat/completions \
  --protocol http2 --ca test-ca.pem -c 200 -n 700 \
  -H 'Authorization: Bearer test-only' --body '{"stream":false}' --mode json --jsonl

# Force a fresh HTTP/1 connection for every request during protocol baselining.
cargo run --release -p loadgen -- run --url https://localhost:8443/readyz \
  --protocol http1 --ca test-ca.pem -c 25 -n 1000 --method GET --no-keepalive

# SSE at 1 KiB/s. Summary JSON goes to stdout; request JSONL goes to stderr.
cargo run --release -p loadgen -- run --url https://localhost:8443/v1/chat/completions \
  --protocol http1 --ca test-ca.pem -c 50 --duration 120s --mode sse \
  --body '{"stream":true}' --read-bytes-per-sec 1024 --jsonl

# Read until one second has elapsed, stop polling the body, hold it for 60 s.
cargo run --release -p loadgen -- run --url https://localhost:8443/v1/chat/completions \
  --protocol http2 -k -c 200 -n 200 --mode sse \
  --stop-read-after 1s --hold-after-stop 60s

# Connect TCP only and send no ClientHello (TLS-handshake slowloris in the plan).
cargo run --release -p loadgen -- slowloris --target 127.0.0.1:8443 \
  --stage tcp -c 1024 --hold 10s

# Complete TLS, then drip an incomplete HTTP/1 header one byte per second.
cargo run --release -p loadgen -- slowloris --target localhost:8443 \
  --stage header --ca test-ca.pem -c 1024 --interval 1s --hold 10s

# Complete TLS and HTTP headers, then drip the request body.
cargo run --release -p loadgen -- slowloris --target localhost:8443 \
  --stage body -k -c 200 --interval 10s --hold 40s
```

`--insecure`/`-k` is intentionally opt-in and is only for isolated test systems. Prefer `--ca`. HTTP/1 normally permits pooling; `--no-keepalive` disables the idle pool when the baseline requires a new connection per request. HTTP/2 uses prior knowledge/ALPN and one shared client pool.

## Output

The stdout document has a stable `schema_version`, exact started/finished and peak-in-flight counts, maps for statuses, `Retry-After`, outcomes and classified errors, byte/event totals, and p50/p95/p99/max for TTFB, first SSE event, and completion latency. `--jsonl` emits a record per request to stderr so stdout remains directly machine-readable.

Slowloris output includes a record for every attempted connection. Stages are: `tcp` (TCP only), `tls` (TLS complete), `header` (TLS plus byte-paced incomplete HTTP/1 header), `body` (complete header plus byte-paced body), and `h2-preface` (TLS, HTTP/2 preface and SETTINGS, no request).
