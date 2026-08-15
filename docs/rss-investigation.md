# RSS Retention After Large Payloads

## Conclusion

The observed post-load RSS plateau was allocator retention, not evidence of a live-object leak. Request permits, upstream connections, response pumps, and audit records all returned to their idle state, while the system allocator kept dirty arenas after bursts of large request and SSE buffers. Comparison runs with jemalloc reclaimed substantially more resident memory, so production builds now use jemalloc.

This conclusion is bounded: RSS alone cannot prove the absence of every leak. Repeated-load slope, allocator statistics, task counts, descriptors, and application-level permit release are the relevant signals.

## Reproduction

The strongest case used concurrent 9.5 MiB request bodies and approximately 2 MiB SSE responses. After all requests completed:

- the mock upstream reported `active=0`;
- all client requests had terminal outcomes;
- admission and body-budget permits were reusable;
- open connection/task counts returned to baseline;
- RSS remained far above the earlier short-request baseline under the system allocator.

Idle time alone did not return RSS to baseline. Subsequent small-request runs succeeded, demonstrating that logical capacity had been released.

## Discriminating experiments

### `malloc_trim`

Invoking allocator trimming after the workload caused a substantial RSS decrease. This is strong evidence that free heap pages remained owned by allocator arenas rather than reachable application objects.

### Arena limits

Reducing glibc arena proliferation improved the retained high-water mark but did not make reclamation as predictable as desired. This also pointed to fragmentation and arena retention rather than a growing collection.

### jemalloc comparison

The same workload with jemalloc showed materially lower idle RSS after the burst and stable repeated-cycle behavior. Functional results and request accounting were unchanged. The project therefore uses jemalloc for the production binary while retaining tests that validate release at the application boundary.

## Code audit

### Request bodies

`gateway.rs` acquires a global body-budget permit before collecting bytes. Ownership extends through construction of the upstream request and is dropped after the body is handed off. Every rejection, timeout, and cancellation path releases the permit.

### Response pumping

Successful bodies are streamed from Reqwest into a bounded Tokio channel. The pump terminates on upstream EOF/error, downstream cancellation, or write timeout. It does not retain an unbounded list of future chunks.

### Audit

`audit.rs` sends fixed-size metadata through a bounded channel. Bodies and raw credentials are never retained. Rotation and shutdown have bounded acknowledgement waits.

### Admission and server tasks

`queue.rs` uses RAII semaphore permits. `server.rs` continuously reaps completed connection tasks, bounds total accepted connections, and applies protocol watchdogs.

No audited path intentionally retains complete request or response bodies after a request terminates.

## Operational guidance

- Alert on a sustained upward slope across repeated identical load/idle cycles, not on a single RSS high-water jump.
- Correlate RSS with allocator active/resident bytes, request/task counts, descriptors, mock-upstream active requests, and permit availability.
- Keep request-body and global body-budget limits proportional; raising only `max_body_bytes` reduces effective concurrent buffering.
- Validate allocator or decay changes against tail latency before deployment.
- For future very large context windows, consider incremental body-budget accounting rather than one full-limit reservation per request.

## Confidence limits

The local experiments were not a substitute for long-duration production profiling. A definitive regression investigation should run repeated large-payload cycles in a fresh process, capture jemalloc statistics or heap profiles, and distinguish a one-time plateau from linear per-cycle growth.
