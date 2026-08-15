# loadgen

`loadgen` is the stateful client for `docs/loadtest-plan.md`. It launches the initial wave behind a Tokio barrier, then starts a replacement only after one request finishes. Consequently `--concurrency N` means at most (and, while work remains, exactly) N unfinished requests rather than N queued tasks.

It supports HTTP and HTTPS over HTTP/1.1 and HTTP/2, a test CA or an explicit insecure mode, JSON validation, incremental SSE framing, paced reads, completely stopped reads, per-request JSONL, aggregate JSON, and raw TCP/TLS slowloris stages. Use HTTP against the private orihsus listener and HTTPS/TLS stages against the nginx public edge.

`--write-bytes-per-sec N` streams the configured request body in 64KiB chunks against a monotonic-clock deadline. This is intended for body-budget and slow-upload tests; it does not pre-generate future chunks while the HTTP stack is backpressured.

## Build and examples

```sh
cargo build --release -p loadgen

# Exercise the private orihsus HTTP listener directly.
cargo run --release -p loadgen -- run \
  --url http://127.0.0.1:18444/v1/chat/completions \
  --protocol http1 -c 200 -n 700 \
  -H 'Authorization: Bearer test-only' --body '{"stream":false}' --mode json --jsonl

# Force a fresh HTTP/1 connection for every request during protocol baselining.
cargo run --release -p loadgen -- run --url http://127.0.0.1:18444/readyz \
  --protocol http1 -c 25 -n 1000 --method GET --no-keepalive

# SSE at 1 KiB/s. Summary JSON goes to stdout; request JSONL goes to stderr.
cargo run --release -p loadgen -- run --url http://127.0.0.1:18444/v1/chat/completions \
  --protocol http1 -c 50 --duration 120s --mode sse \
  --body '{"stream":true}' --read-bytes-per-sec 1024 --jsonl

# Read until one second has elapsed, stop polling the body, hold it for 60 s.
cargo run --release -p loadgen -- run --url http://127.0.0.1:18444/v1/chat/completions \
  --protocol http1 -c 200 -n 200 --mode sse \
  --stop-read-after 1s --hold-after-stop 60s

# Hold private TCP connections without completing HTTP headers.
cargo run --release -p loadgen -- slowloris --target 127.0.0.1:18444 \
  --stage tcp -c 1024 --hold 10s

# Test nginx TLS/header handling at the public edge.
cargo run --release -p loadgen -- slowloris --target api.example.com:443 \
  --stage header -c 1024 --interval 1s --hold 10s

# Complete TLS and HTTP headers, then drip the request body.
cargo run --release -p loadgen -- slowloris --target api.example.com:443 \
  --stage body -c 200 --interval 10s --hold 40s
```

`--insecure`/`-k` is intentionally opt-in and is only for isolated test systems. Prefer `--ca`. HTTP/1 normally permits pooling; `--no-keepalive` disables the idle pool when the baseline requires a new connection per request. HTTP/2 uses prior knowledge/ALPN and one shared client pool.

## Output

The stdout document has a stable `schema_version`, exact started/finished and peak-in-flight counts, maps for statuses, `Retry-After`, outcomes and classified errors, byte/event totals, and p50/p95/p99/max for TTFB, first SSE event, and completion latency. `--jsonl` emits a record per request to stderr so stdout remains directly machine-readable.

Slowloris output includes a record for every attempted connection. Stages are: `tcp` (TCP only), `tls` (TLS complete), `header` (TLS plus byte-paced incomplete HTTP/1 header), `body` (complete header plus byte-paced body), and `h2-preface` (TLS, HTTP/2 preface and SETTINGS, no request).
