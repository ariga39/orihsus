# orihsus Load-Test Plan

## Purpose

Validate gateway capacity boundaries, key rotation, streaming backpressure, slow-client cleanup, slowloris protection, audit behavior, and memory recovery. A generic HTTP benchmark is insufficient because critical behavior depends on precisely scripted upstream timing and response types.

Never run these tests with production keys, production upstreams, or a production audit directory.

## Capacity model

With the example configuration:

- at most 200 admitted requests hold execution permits;
- another 500 requests may wait in the admission queue;
- the 701st simultaneous request is rejected immediately with `503` and `Retry-After: 1`;
- queued requests time out after 30 seconds with the same response;
- an execution permit remains held until the downstream body completes or is cancelled.

The body budget is separate. Each request reserves the full `max_body_bytes`, not its content length. With 256 MiB global capacity and a 10 MiB per-request maximum, only 25 requests can buffer bodies simultaneously.

## Test topology

Run the load generator, orihsus, and mock upstream on separate hosts or isolated CPU groups. If they share a machine, record CPU for all three and treat throughput as a local regression baseline rather than a production SLO.

Use a release build, fixed CPU and memory allocation, stable file-descriptor limits, fixed nginx TLS/protocol settings, a dedicated JSONL path, synthetic keys, and unique request IDs. Run application-capacity cases directly against loopback HTTP and repeat public-edge cases through nginx.

- per-request connection, TTFB, first-event, completion, status, response bytes, and termination cause;
- gateway RSS/high-water mark, CPU, thread/task indicators, descriptors, network, disk, stderr, and audit output;
- mock-upstream concurrency, per-key attempts, cancellation, timing error, rejections, and observation loss.

## Tools

### Programmable mock upstream

`tools/loadtest/mock-upstream` must support HTTPS over HTTP/1.1 and HTTP/2 and independently script:

- response-header and body-start delay, including permanent silence;
- finite or infinite SSE, fixed/list/jittered intervals, `[DONE]`, mid-stream close, and silence;
- chunk size, byte pacing, burst behavior, known length, and stalled bodies;
- barriers that release an exact number of requests together;
- status/header/body rules by key, attempt, and request ID;
- exact and malformed `GoUsageLimitError`, ordinary 429, 401/403, and 5xx responses.

Its connection/request caps must be configurable and visible. Hot-path observation must be bounded and non-blocking. A formal test run is invalid if the mock reaches a cap, queues internally, rejects unexpectedly, drops observations, saturates CPU/NIC/FDs, or misses the configured timing tolerance.

### Load generator

`tools/loadtest/loadgen` must support HTTP/1.1 and HTTP/2, a start barrier, exactly N unfinished requests, incremental JSON/SSE parsing, slow or stopped reads, structured latency output, and TCP/TLS/header/body/HTTP2-preface slowloris modes. TLS and HTTP/2 edge tests target nginx; private orihsus tests use loopback HTTP/1.1.

Generic tools such as `wrk` or `hey` may establish a short-response baseline but cannot validate stateful capacity and streaming cases.

## Calibration

Before and after formal tests, bypass the gateway and drive the mock directly with the same TLS, protocol, response size, pacing, and connection reuse.

- Step short-response load beyond the intended gateway peak and locate the mock's capacity knee.
- Hold more long-lived streams than the formal scenario for at least ten minutes.
- Validate maximum planned response bandwidth.
- Sample configured delays and intervals and compare planned with actual monotonic timestamps.
- Require zero mock waiting, rejection, limit hits, and observation loss.

## Scenarios

### 0. Short-response baseline

Return approximately 1 KiB JSON immediately. Sweep concurrency through 1, 25, 50, 100, 150, 190, 200, and 220 for HTTP/1.1 and representative HTTP/2 connection counts. Record throughput and latency percentiles after warm-up.

### 1. Active limit

Hold 200 upstream requests at a barrier. Send more requests and verify that exactly 200 reach the mock until permits are released. Confirm long SSE streams retain permits and that completion admits queued work in FIFO order.

### 2. Queue limit and deadline

Keep 200 requests active, queue 500, then send one more. Require one immediate `503` with `Retry-After: 1`. Keep active requests blocked past 30 seconds and require all 500 queued requests to time out without reaching upstream.

### 3. Streaming and slow downstreams

Test normal paced SSE, permanently silent SSE, slow readers, and readers that stop completely. Verify incremental delivery, bounded buffering, cancellation propagation, audit outcomes, and admission release. A slow reader that continues to make progress may legitimately retain an SSE stream indefinitely; a fully blocked channel must hit the downstream-write budget.

### 4. Key rotation

- Return an exact usage-limit response for K1 and success for K2; require one final success and two correlated attempts.
- Exercise malformed JSON, unknown limit dimensions, invalid reset text, ordinary 429 with both `Retry-After` forms, 401/403, 5xx, and network failure.
- Cool every key, verify the bounded pool wait, then require 429 with an earliest-recovery `Retry-After`.
- Run concurrent failures to confirm stale results cannot erase newer cooldown or breaker state.

### 5. Audit behavior

Under normal storage, require one valid JSONL record for every auditable request, no secret/body content, and correct terminal outcomes. Under an intentionally blocked writer, verify request latency remains bounded, drop/failure counters advance, warnings are sanitized and not repeated, reopen is bounded, and shutdown respects its deadline.

### 6. Slowloris and request bodies

Test TLS and HTTP/2 slowloris behavior at nginx. Separately fill the orihsus loopback connection cap with clients that stop during HTTP/1 headers; confirm excess connections are closed and watchdogs restore capacity. Send slow request bodies through both paths and require body deadlines and budget release.

### 7. Large payloads

Use 5 MiB, 9.5 MiB, and exact-limit 10 MiB bodies over independent HTTP/1 connections. Verify the expected 25-request body gate. Stream 64 KiB SSE events to approximately 2 MiB per response with normal and throttled readers. Track complete bytes/events and memory recovery.

### 8. Endurance and recovery

Run a mixed workload for at least 60 minutes, combining short JSON, SSE, rate limits, slow readers, and churn. After fault removal, repeat the baseline and compare throughput, p99 latency, RSS, descriptors, and task counts.

## Pass criteria

Functional gates are exact: capacity counts, response codes/headers, key attempts, cancellation, permit reuse, and audit syntax must match the contract. Hardware-dependent performance has no invented absolute SLO. Establish the first qualified run as baseline; suggested regression thresholds are no more than 10% throughput loss, 20% p99 increase, or 10% steady-state RSS increase on identical hardware.

Run formal scenarios three times after warm-up and report the median plus the worst run. Stop and invalidate a run on mock/generator saturation, observation loss, swap/OOM, unexpected server errors, or an uncontrolled environmental change.
