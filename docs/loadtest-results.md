# orihsus Local Load-Test Results

## Summary

Local implementation tests passed the exact admission boundary, queue timeout, usage-limit failover, connection-limit recovery, body-budget behavior, large-body completion, SSE byte/event integrity, and normal-disk audit completeness.

Slow-client cleanup was partially demonstrated: 7 of 10 stopped readers hit the approximately 30-second downstream-write timeout, while 3 continued making progress through shared HTTP/2 buffering until the client dropped. Application resources were released in every case. RSS permits and connections recovered, but the system allocator retained a large resident high-water mark; a jemalloc comparison resolved most idle retention and motivated the allocator change.

These were short local acceptance runs, not production SLO measurements. The planned five-minute repeated throughput runs, 60-minute mixed endurance run, and real slow-disk injection remain future work.

## Environment and tools

The gateway, mock upstream, and load generator ran on the same host with CPU affinity used for the large-payload cases. Traffic used local HTTPS and synthetic credentials.

`tools/loadtest/mock-upstream` has no external npm dependencies and supports HTTPS HTTP/1.1 and HTTP/2, scripted header/body/SSE timing, silence, pacing, barriers, fault responses, configurable caps, and structured metrics. Its final regression suite reported 11 passed tests, including HTTP/2 large-body integrity, normal-close races, real `RST_STREAM CANCEL`, and cancellation under HTTP/2 flow-control backpressure.

`tools/loadtest/loadgen` is a Rust program supporting HTTP/1.1 and HTTP/2, start barriers, exact unfinished-request counts, incremental JSON/SSE parsing, slow/stopped reads, structured percentiles, and multiple slowloris stages. Its five tests, Clippy with warnings denied, and release build passed. The workspace test run reported 254 passing tests at the time of measurement.

## Results

### Mock bypass calibration

Direct HTTP/2 traffic to the mock used concurrency 400 for 5,000 immediate JSON responses:

- 5,000/5,000 returned 200 with no errors;
- elapsed time was 970.677 ms, approximately 5,151 requests/s;
- event-loop lag p99 was 13.050 ms;
- the configured concurrency cap was not reached and waiting, rejection, limit-hit, and observation-drop counters remained zero.

This established adequate capacity for these local gateway runs, not the mock's absolute limit.

### Short-request baseline and recovery

HTTP/2 with concurrency 200 completed 5,000 non-streaming requests:

- 5,000/5,000 returned 200;
- elapsed time was 900.262 ms, approximately 5,554 requests/s;
- mock active peak was 165 of 400 with no capacity or observation failures.

Three runs after the slowloris test had median throughput of approximately 6,955 requests/s and completion p99 of 70.326 ms. The short local runs varied enough that these figures should be treated only as smoke/regression data.

### Admission boundary

Using independent HTTP/1 connections, 701 requests arrived together while the mock held SSE responses for five seconds.

- statuses were exactly 700 x 200 and 1 x 503;
- the rejected request had `Retry-After: 1`;
- mock active peak was exactly 200 of 400;
- all 700 admitted/queued requests eventually completed;
- elapsed time was 20.322 seconds, consistent with four release waves.

The boundary passed and the mock showed no internal capacity pressure.

### Queue full and 30-second timeout

The mock sent SSE headers to 200 requests and then remained silent. Another 500 waited and request 701 overflowed.

- statuses were 200 x 200 and 501 x 503;
- every 503 had `Retry-After: 1`;
- TTFB p50/p95/p99 was 30.331/30.396/30.408 seconds;
- the earliest overflow response arrived at approximately 72.6 ms;
- the mock received only 200 requests and peaked at 200 of 400;
- the 200 silent clients were cleaned up by their 45-second client deadline.

The queue boundary and timeout passed.

### Usage-limit failover

K1 returned an exact 429 `GoUsageLimitError` for the `5h` dimension with `Resets in 2 seconds`; K2 returned 200. The client received 200 at 42.701 ms TTFB and 42.712 ms completion. The mock recorded one correlated K1 attempt followed by one K2 attempt, with status totals 429 x 1 and 200 x 1 and no raw key values. This passed.

### Stopped readers and SSE memory

For 50 clients receiving infinite 64 KiB SSE events and stopping reads after one second:

- all received 200 and first-event p99 was 71.291 ms;
- clients dropped after approximately 41 seconds;
- audit output contained 50 `client_cancel` outcomes;
- gateway RSS moved from approximately 40,160 KiB during the run to 38,036 KiB afterward.

For 10 clients held for 55 seconds, seven audit latencies were 31.033–31.043 seconds, demonstrating the 30-second per-send timeout after channel saturation. Three terminated at client drop around 56.015 seconds because shared HTTP/2 flow control and buffering allowed some sends to keep completing within the rolling timeout. All ten ended as `client_cancel` and released admission.

The mock initially had a cancellation-control bug while blocked on response backpressure. The implementation was fixed to race drain/error against an abort signal and to destroy already-started responses. A real HTTP/2 flow-control regression then returned active count to zero in about 44 ms, and the full 11-test suite passed. Formal slow-reader measurements were not repeated after that tool fix.

### Slowloris and connection recovery

The load generator opened 1,024 TCP connections and sent no TLS ClientHello for ten seconds.

- all 1,024 connections were established;
- an extra HTTPS health probe at two seconds timed out, confirming the cap;
- a health probe at eight seconds returned 200, confirming the five-second handshake watchdog released slots;
- gateway-side established connections returned to zero;
- a subsequent short-request baseline succeeded;
- one sanitized accept-retry warning appeared, with no panic, exit, or log storm.

This passed.

### Audit and resource release

The final normal-disk audit file contained 17,868 JSONL records, equal to the gateway client requests plus three health probes. Every line parsed successfully, and stderr contained no audit-drop or write-failure warning.

Gateway RSS changed from approximately 37,876 KiB before slowloris cleanup to 38,036 KiB afterward. Slow SSE peaked around 40,160 KiB and returned to approximately 38,036 KiB. These cases showed no persistent growth.

## Large payloads

The large-payload configuration used `max_body_bytes=10MiB` and `max_inflight_body_bytes=256MiB`. Because each request reserves the full maximum, the correct body-buffer concurrency is `floor(256/10) = 25` for both 5 MiB and 9.5 MiB bodies.

### 5 MiB bodies

Sixty simultaneous HTTP/1 requests used an eight-second upstream header delay:

- 60/60 returned 200;
- elapsed time was 25.489 seconds;
- TTFB p50/p95/p99 was 17.266/25.436/25.455 seconds, showing 25 + 25 + 10 waves;
- mock active peak was exactly 25 and returned to zero;
- all mock capacity/error counters remained zero.

### 9.5 MiB and exact 10 MiB bodies

Thirty simultaneous 9.5 MiB HTTP/1 requests used a five-second upstream header delay:

- 30/30 returned 200 in 11.320 seconds;
- TTFB p50/p95/p99 was 6.208/11.268/11.283 seconds, showing 25 + 5 waves;
- mock active peak was 25 and returned to zero.

One exact 10 MiB request returned 200 in 90.857 ms, confirming that the configured boundary is inclusive. This does not imply that a 10 MiB limit is safe for a nominal 10 MiB model payload because JSON, Unicode, tools, schemas, and metadata require headroom.

### Slow large uploads

Two 30-request runs with paced 5 MiB and 9.5 MiB uploads completed successfully. The first 25 released permits and the remaining five reached upstream, proving body-budget and upstream-resource release. Kernel and protocol receive buffers may still accept bytes while application-level budget permits are unavailable.

### Large SSE responses

The mock emitted 32 64 KiB data events plus `[DONE]` per request, approximately 2 MiB per response, for 30 requests with 5 MiB request bodies.

Both normal and 128 KiB/s client reads completed 30/30 responses, parsed 990/990 events, and transferred 62,914,980 bytes. The mock peaked at 30 and returned to zero with no capacity failure or observation loss. This also confirmed that body permits are released after the upstream request is accepted rather than held through the response stream.

### RSS result

Application permits and connections were released, but gateway RSS remained approximately 392 MiB after 20 seconds idle versus an earlier approximately 38 MiB baseline. The evidence at that point could not distinguish allocator retention from unreachable live objects. Follow-up allocator experiments documented in `docs/rss-investigation.md` showed system-allocator retention and materially better jemalloc reclamation.

## Payload sizing guidance

| Target | Estimated request body | Suggested `max_body_bytes` | Suggested global budget for at least 25 bodies |
| --- | ---: | ---: | ---: |
| Current 1M context | about 5 MiB | 10 MiB | 256 MiB |
| Future 2M context | about 9–10 MiB | at least 16 MiB | at least 400 MiB; 512 MiB preferred when available |
| Future 10M context | about 45–50 MiB | at least 64 MiB | at least 1.6 GiB |

These values include rough serialization headroom and must be validated against real payload distributions. Reserving 64 MiB for every one of 200 active requests would imply a 12.5 GiB worst case, so admission count and body-memory concurrency should not be conflated.

## Protocol finding

Sixty 5 MiB requests multiplexed over one HTTP/2 connection all timed out before reaching the mock. The first 25 streams held body permits while other streams consumed the shared connection flow-control window, preventing permit holders from completing their bodies. This invalid run exposed connection-level head-of-line blocking. Large-payload clients should limit streams per connection or use a connection pool, and the gateway should retain a regression test for this combination.

## Verification commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo build --release
node --test tools/loadtest/mock-upstream/test/*.test.js
cargo test -p loadgen
cargo clippy -p loadgen --all-targets -- -D warnings
cargo build -p loadgen --release
```

## Follow-up work

1. Repeat the stopped-reader run with one connection per slow client.
2. Add server-close timing detection to slowloris output.
3. Run the full repeated baseline, mixed endurance workload, and controlled slow-disk injection on isolated hosts.
4. Preserve the HTTP/2 large-body flow-control case as a regression test and evaluate incremental body-budget accounting.
