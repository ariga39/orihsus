# loadgen implementation and verification

Date: 2026-08-14 (UTC)

## Implemented scope

- `run` mode with HTTP/1.1 or HTTP/2 selection, HTTPS test-CA loading, and explicit `--insecure` certificate-verification bypass.
- An initial Tokio barrier plus one request loop per concurrency slot. A replacement is started only after that slot's previous request reaches a terminal state, so `--concurrency N` maintains exactly N unfinished requests while work remains. Finite request counts smaller than N are handled without dummy tasks.
- Fixed request-count or duration-based runs, configurable method/headers/body, request timeout, generated `x-request-id`, optional HTTP/1 idle-pool disabling, JSON response validation, and incremental SSE parsing (including CRLF, split lines, multi-line `data`, and `[DONE]`).
- Configurable cumulative monotonic-clock read pacing and stopped-body polling with a configurable hold time. Stopping also works while waiting for a silent next chunk.
- Per-request structured JSONL with status, `Retry-After`, header TTFB, first SSE event, completion latency, bytes/events, outcome, and classified error. Aggregate structured JSON includes counts, peak in-flight, status/Retry-After/outcome/error maps, totals, and nearest-rank p50/p95/p99/max latency data.
- `slowloris` mode for TCP-only connections, completed TLS, byte-paced incomplete HTTP/1 headers, byte-paced bodies, and HTTP/2 preface/SETTINGS without a first request. Raw TLS supports the same test CA / opt-in insecure policy.
- CLI usage and runnable examples in `README.md`.

## Automated coverage

Five loadgen tests cover CLI parsing, SSE framing across chunk boundaries, percentile calculation, actual concurrent in-flight replacement behavior through a local HTTP server, and SSE `[DONE]` plus `Retry-After` collection through a local HTTP server.

## Verification performed

All commands completed successfully from the repository root:

```text
cargo fmt --all -- --check
cargo test --workspace
  loadgen: 5 passed, 0 failed
  complete workspace: 254 passed, 0 failed
cargo clippy -p loadgen --all-targets -- -D warnings
cargo build --release -p loadgen
cargo run -q -p loadgen -- --help
```

The two HTTP end-to-end tests were run outside the filesystem/network sandbox solely because they bind an ephemeral `127.0.0.1` listener. They do not contact an external service. No mock implementation or file was changed.
