use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use orihsus::audit::{
    fingerprint, AuditError, AuditRecord, AuditWriter, Outcome, AUDIT_REOPEN_TIMEOUT,
    AUDIT_SHUTDOWN_TIMEOUT,
};
use orihsus::gateway::AuditSink;
use tempfile::TempDir;

fn ts() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-08-13T12:34:56Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn record(id: &str, key: &str, input: Option<u64>, output: Option<u64>) -> AuditRecord {
    AuditRecord {
        timestamp: ts(),
        request_id: id.to_string(),
        model: Some("deepseek-chat".to_string()),
        key_fingerprint: Some(fingerprint(key)),
        input_tokens: input,
        output_tokens: output,
        status: 200,
        outcome: None,
        latency: Duration::from_millis(150),
    }
}

#[test]
fn fingerprint_is_first_12_hex_chars_of_sha256() {
    assert_eq!(fingerprint("key-1"), "be2974546978");
    let f = fingerprint("key-1");
    assert_eq!(f.len(), 12);
    assert!(f.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn writes_a_single_valid_json_line_with_all_fields() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 16).unwrap();

    assert_eq!(
        writer.try_record(record("req-1", "sk-secret-1", Some(12), Some(34))),
        Outcome::Accepted
    );
    writer.shutdown().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let mut lines = content.lines();
    let line = lines.next().expect("exactly one line");
    assert_eq!(lines.next(), None, "exactly one line");

    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["timestamp"], "2026-08-13T12:34:56.000Z");
    assert_eq!(v["request_id"], "req-1");
    assert_eq!(v["model"], "deepseek-chat");
    assert_eq!(v["key_fingerprint"], fingerprint("sk-secret-1"));
    assert_eq!(v["input_tokens"], 12);
    assert_eq!(v["output_tokens"], 34);
    assert_eq!(v["status"], 200);
    assert_eq!(v["latency_ms"], 150);
    assert!(
        !content.contains("sk-secret-1"),
        "raw key must not appear in the audit file"
    );
}

#[test]
fn missing_tokens_serialize_as_null() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 8).unwrap();

    assert_eq!(
        writer.try_record(record("req-null", "sk-secret-1", None, None)),
        Outcome::Accepted
    );
    writer.shutdown().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let line = content.lines().next().unwrap();
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert!(
        v["input_tokens"].is_null(),
        "missing input_tokens must write null"
    );
    assert!(
        v["output_tokens"].is_null(),
        "missing output_tokens must write null"
    );
}

#[test]
fn missing_model_and_key_serialize_as_null() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 8).unwrap();

    let rec = AuditRecord {
        timestamp: ts(),
        request_id: "req-nomodel".to_string(),
        model: None,
        key_fingerprint: None,
        input_tokens: None,
        output_tokens: None,
        status: 503,
        outcome: None,
        latency: Duration::from_millis(5),
    };
    assert_eq!(writer.try_record(rec), Outcome::Accepted);
    writer.shutdown().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let line = content.lines().next().unwrap();
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert!(
        v["model"].is_null(),
        "unknown model must write JSON null, not an empty string: {line}"
    );
    assert!(
        v["key_fingerprint"].is_null(),
        "unselected key must write JSON null, not an empty string: {line}"
    );
}

#[test]
fn outcome_serializes_as_optional_snake_case_string() {
    use orihsus::audit::AuditOutcome;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 8).unwrap();

    let mut with = record("req-out", "sk-secret-1", None, None);
    with.outcome = Some(AuditOutcome::UpstreamError);
    assert_eq!(writer.try_record(with), Outcome::Accepted);
    assert_eq!(
        writer.try_record(record("req-default", "sk-secret-1", None, None)),
        Outcome::Accepted
    );
    writer.shutdown().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let mut lines = content.lines();
    let v: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(v["outcome"], "upstream_error");
    let v: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert!(
        v["outcome"].is_null(),
        "the outcome field is backward compatible: unset must write null"
    );
}

/// Releasable gate: `write` blocks until released. Lets tests suspend the
/// background writer deterministically (filesystem seam mock).
#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn block(&self) {
        *self.0 .0.lock().unwrap() = false;
    }

    fn release(&self) {
        let (m, cv) = &*self.0;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }

    fn wait_open(&self) {
        let (m, cv) = &*self.0;
        let mut open = m.lock().unwrap();
        while !*open {
            open = cv.wait(open).unwrap();
        }
    }

    fn guard(&self) -> GateGuard {
        GateGuard(self.clone())
    }
}

/// Releases the gate on drop, including during a panic unwind. Tests hold one
/// for as long as the writer is deliberately blocked so a failing assert can
/// never strand the writer thread on the gate and deadlock the `AuditWriter`
/// Drop join.
struct GateGuard(Gate);

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct BlockingSink {
    gate: Gate,
    started: Arc<(Mutex<bool>, Condvar)>,
    written: Arc<Mutex<Vec<u8>>>,
}

impl Write for BlockingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (m, cv) = &*self.started;
        *m.lock().unwrap() = true;
        cv.notify_all();
        self.gate.wait_open();
        self.written.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn wait_started(started: &Arc<(Mutex<bool>, Condvar)>) {
    let (m, cv) = &**started;
    let mut v = m.lock().unwrap();
    while !*v {
        v = cv.wait(v).unwrap();
    }
}

#[test]
fn full_queue_drops_and_counts() {
    let gate = Gate::default();
    gate.block();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = BlockingSink {
        gate: gate.clone(),
        started: started.clone(),
        written: written.clone(),
    };
    let writer = AuditWriter::start_with_sink(Box::new(sink), 2).unwrap();
    let guard = gate.guard();

    assert_eq!(
        writer.try_record(record("a", "k1", None, None)),
        Outcome::Accepted
    );
    wait_started(&started);

    assert_eq!(
        writer.try_record(record("b", "k2", None, None)),
        Outcome::Accepted
    );
    assert_eq!(
        writer.try_record(record("c", "k3", None, None)),
        Outcome::Accepted
    );
    assert_eq!(
        writer.try_record(record("d", "k4", None, None)),
        Outcome::Dropped
    );
    assert_eq!(
        writer.try_record(record("e", "k5", None, None)),
        Outcome::Dropped
    );
    assert_eq!(writer.dropped(), 2);

    drop(guard);
    writer.shutdown().unwrap();

    let out = written.lock().unwrap().clone();
    let out = String::from_utf8(out).unwrap();
    let ids: Vec<String> = out
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["request_id"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[test]
fn shutdown_flushes_all_accepted_records_in_order() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 8).unwrap();

    for i in 0..3 {
        assert_eq!(
            writer.try_record(record(&format!("req-{i}"), "sk-secret-1", Some(1), Some(2))),
            Outcome::Accepted
        );
    }
    writer.shutdown().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let ids: Vec<String> = content
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["request_id"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(ids, vec!["req-0", "req-1", "req-2"]);
}

struct FailingSink;

impl Write for FailingSink {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("write failed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("flush failed"))
    }
}

#[test]
fn write_failures_surface_on_shutdown_without_panicking() {
    let writer = AuditWriter::start_with_sink(Box::new(FailingSink), 8).unwrap();

    assert_eq!(
        writer.try_record(record("req-fail", "sk-secret-1", None, None)),
        Outcome::Accepted
    );
    let result = writer.shutdown();
    assert!(
        result.is_err(),
        "shutdown must surface the write failure, got {result:?}"
    );
}

#[test]
fn write_failures_are_observable_at_runtime_without_shutdown() {
    let writer = AuditWriter::start_with_sink(Box::new(FailingSink), 8).unwrap();
    assert_eq!(
        writer.try_record(record("req-wf", "sk-secret-1", None, None)),
        Outcome::Accepted
    );

    // The background writer thread bumps the shared counter as soon as it
    // processes the failing record — observable before any shutdown.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while writer.write_failures() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "write_failures must become > 0 once the writer processes the failing record"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = writer.shutdown();
}

#[test]
fn write_failures_count_is_stable_and_the_writer_does_not_panic() {
    let writer = AuditWriter::start_with_sink(Box::new(FailingSink), 8).unwrap();
    for i in 0..3 {
        assert_eq!(
            writer.try_record(record(&format!("wf-{i}"), "sk-secret-1", None, None)),
            Outcome::Accepted
        );
    }

    // One failed write_all per record — exactly 3, no more, no fewer.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if writer.write_failures() == 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected 3 write failures (one per record), got {}",
            writer.write_failures()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        writer.write_failures(),
        3,
        "the failure count must stay stable while the writer is idle"
    );

    // Shutdown's flush is a distinct operation (counted once) and the first
    // write error still surfaces without panicking the writer thread.
    let result = writer.shutdown();
    assert!(
        result.is_err(),
        "shutdown must surface the write failure, got {result:?}"
    );
}

#[test]
fn audit_writer_can_be_used_as_a_gateway_audit_sink() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = Arc::new(AuditWriter::start(&path, 8).unwrap());

    let sink: Arc<dyn AuditSink> = writer.clone();
    assert_eq!(
        sink.record(record("req-1", "key-a", Some(1), Some(2))),
        Outcome::Accepted
    );

    drop(sink);
    // Dropping the sink is non-blocking (never joins); flush deterministically
    // via the surviving handle before reading.
    let writer = match Arc::try_unwrap(writer) {
        Ok(w) => w,
        Err(_) => panic!("sink erased only one reference"),
    };
    writer.shutdown().unwrap();
    let bytes = fs::read(&path).unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("req-1"));
}

fn ids_in(path: &std::path::Path) -> Vec<String> {
    let content = fs::read_to_string(path).unwrap();
    content
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["request_id"].as_str().unwrap().to_string()
        })
        .collect()
}

#[tokio::test]
async fn reopen_redirects_new_records_to_the_new_file_and_old_inode_does_not_grow() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = Arc::new(AuditWriter::start(&path, 16).unwrap());
    let sink: Arc<dyn AuditSink> = writer.clone();

    assert_eq!(
        sink.record(record("old", "sk-secret-1", Some(1), Some(2))),
        Outcome::Accepted
    );

    // Logrotate-style rotation: rename the live file away, create a fresh one
    // at the original path, then ask the writer to reopen onto it.
    #[cfg(unix)]
    let old_inode = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&path).unwrap().ino()
    };
    let rotated = dir.path().join("audit.jsonl.1");
    fs::rename(&path, &rotated).unwrap();
    fs::write(&path, "").unwrap();

    sink.reopen(&path).await.unwrap();

    assert_eq!(
        sink.record(record("new", "sk-secret-1", Some(3), Some(4))),
        Outcome::Accepted
    );
    drop(sink);
    // Dropping the sink never joins; flush deterministically before reading.
    let writer = match Arc::try_unwrap(writer) {
        Ok(w) => w,
        Err(_) => panic!("sink erased only one reference"),
    };
    writer.shutdown().unwrap();

    assert_eq!(
        ids_in(&rotated),
        vec!["old"],
        "the rotated file must not grow after reopen"
    );
    assert_eq!(
        ids_in(&path),
        vec!["new"],
        "records after reopen must land in the new file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            fs::metadata(&path).unwrap().ino(),
            old_inode,
            "reopen must open a fresh inode at the original path"
        );
    }
}

#[tokio::test]
async fn reopen_open_failure_returns_err_and_the_old_writer_keeps_working() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = Arc::new(AuditWriter::start(&path, 16).unwrap());
    let sink: Arc<dyn AuditSink> = writer.clone();

    assert_eq!(
        sink.record(record("before", "sk-secret-1", None, None)),
        Outcome::Accepted
    );

    let rotated = dir.path().join("audit.jsonl.1");
    fs::rename(&path, &rotated).unwrap();
    // A directory at the target path makes the reopen open fail.
    fs::create_dir(&path).unwrap();

    let result = sink.reopen(&path).await;
    assert!(
        result.is_err(),
        "reopen must surface the open failure, got {result:?}"
    );

    assert_eq!(
        sink.record(record("after", "sk-secret-1", None, None)),
        Outcome::Accepted,
        "the old writer must keep accepting records after a failed reopen"
    );
    drop(sink);
    // Dropping the sink never joins; flush deterministically before reading.
    let writer = match Arc::try_unwrap(writer) {
        Ok(w) => w,
        Err(_) => panic!("sink erased only one reference"),
    };
    writer.shutdown().unwrap();

    assert_eq!(
        ids_in(&rotated),
        vec!["before", "after"],
        "a failed reopen must leave the old writer writing to its previous file"
    );
    assert!(
        fs::metadata(&path).unwrap().is_dir(),
        "the failed reopen target must not have been clobbered"
    );
}

#[tokio::test]
async fn reopen_returns_promptly_when_the_audit_queue_is_full() {
    // GateWriter fixture: the blocking sink signals `started` first, then waits
    // on a Condvar gate, so `wait_started` deterministically guarantees the
    // writer is stuck mid-write (no race on "is the queue full?").
    let gate = Gate::default();
    gate.block();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = BlockingSink {
        gate: gate.clone(),
        started: started.clone(),
        written: written.clone(),
    };
    let writer = AuditWriter::start_with_sink(Box::new(sink), 1).unwrap();
    // Held for the whole body so a panicking assert unwinds and releases the
    // gate before the writer's Drop join runs; explicit `drop(guard)` below
    // releases before shutdown/asserts on the success path.
    let guard = gate.guard();

    // Record 1 blocks the writer (it is being written); record 2 then fills the
    // single remaining queue slot.
    assert_eq!(
        writer.try_record(record("a", "k", None, None)),
        Outcome::Accepted
    );
    wait_started(&started);
    assert_eq!(
        writer.try_record(record("b", "k", None, None)),
        Outcome::Accepted
    );

    // The Reopen command cannot even be enqueued: try_send must fail fast with
    // QueueFull instead of a blocking send on a Tokio worker.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let result = writer.reopen(&path).await;

    drop(guard);
    assert!(
        matches!(result, Err(AuditError::ReopenQueueFull)),
        "expected ReopenQueueFull, got {result:?}"
    );
    writer.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn reopen_times_out_when_the_writer_is_blocked() {
    // Writer is stuck on I/O (blocking sink) but the capacity-2 queue has room,
    // so the Reopen command is enqueued; the reply is awaited under the 5s
    // budget and must time out instead of hanging the task.
    let gate = Gate::default();
    gate.block();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = BlockingSink {
        gate: gate.clone(),
        started: started.clone(),
        written: written.clone(),
    };
    let writer = Arc::new(AuditWriter::start_with_sink(Box::new(sink), 2).unwrap());
    let guard = gate.guard();

    assert_eq!(
        writer.try_record(record("a", "k", None, None)),
        Outcome::Accepted
    );
    wait_started(&started);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let w = Arc::clone(&writer);
    let task = tokio::spawn(async move { w.reopen(&path).await });
    tokio::task::yield_now().await;

    tokio::time::advance(AUDIT_REOPEN_TIMEOUT).await;
    tokio::task::yield_now().await;
    let result = task
        .await
        .expect("the reopen future must finish once its 5s budget elapses");

    drop(guard);
    assert!(
        matches!(result, Err(AuditError::ReopenTimeout)),
        "expected ReopenTimeout, got {result:?}"
    );
}

#[tokio::test]
async fn shutdown_bounded_flushes_all_accepted_records_in_order() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer = AuditWriter::start(&path, 8).unwrap();

    for i in 0..3 {
        assert_eq!(
            writer.try_record(record(
                &format!("bounded-{i}"),
                "sk-secret-1",
                Some(1),
                Some(2)
            )),
            Outcome::Accepted
        );
    }
    writer.shutdown_bounded().await.unwrap();

    assert_eq!(
        ids_in(&path),
        vec!["bounded-0", "bounded-1", "bounded-2"],
        "the bounded shutdown must flush accepted records in order like shutdown"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_bounded_times_out_when_the_writer_is_stuck_and_never_blocks_the_runtime() {
    // GateWriter stuck mid-write: a naive synchronous join would hang the
    // current-thread worker forever. The bounded shutdown must return
    // ShutdownTimeout after AUDIT_SHUTDOWN_TIMEOUT without ever blocking the
    // worker, and releasing the gate must let the writer drain cleanly.
    let gate = Gate::default();
    gate.block();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = BlockingSink {
        gate: gate.clone(),
        started: started.clone(),
        written: written.clone(),
    };
    let writer = AuditWriter::start_with_sink(Box::new(sink), 1).unwrap();
    // Declared after `writer` so a panic unwind releases the gate before any
    // writer Drop join could deadlock.
    let guard = gate.guard();

    assert_eq!(
        writer.try_record(record("stuck", "k", None, None)),
        Outcome::Accepted
    );
    wait_started(&started);

    // A heartbeat on the same current-thread runtime must keep ticking while
    // the bounded shutdown waits: the worker is never blocked by the join.
    let beats = Arc::new(AtomicUsize::new(0));
    let beats2 = beats.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.tick().await;
        loop {
            interval.tick().await;
            beats2.fetch_add(1, Ordering::SeqCst);
        }
    });

    let task = tokio::spawn(async move { writer.shutdown_bounded().await });
    tokio::task::yield_now().await;

    tokio::time::advance(AUDIT_SHUTDOWN_TIMEOUT).await;
    tokio::task::yield_now().await;
    let result = task
        .await
        .expect("the bounded shutdown must resolve, not hang the worker");
    assert!(
        matches!(result, Err(AuditError::ShutdownTimeout)),
        "expected ShutdownTimeout, got {result:?}"
    );
    assert!(
        beats.load(Ordering::SeqCst) > 0,
        "the current-thread worker must keep running the heartbeat while bounded shutdown waits"
    );
    heartbeat.abort();

    // Release the gate: the stuck writer drains its accepted record and the
    // detached supervisor's join completes — clean, nothing hangs.
    drop(guard);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while written.lock().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the writer must flush the stuck record once the gate is released"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        String::from_utf8_lossy(&written.lock().unwrap()).contains("stuck"),
        "the released writer must have drained its accepted record"
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_a_stuck_writer_never_blocks_the_current_thread() {
    // GateWriter stuck mid-write: dropping the writer must NOT join (a naive
    // Drop join would hang the current-thread worker forever). The drop returns
    // immediately, the heartbeat keeps ticking, and once the gate is released
    // the detached writer thread drains and exits on its own.
    let gate = Gate::default();
    gate.block();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = BlockingSink {
        gate: gate.clone(),
        started: started.clone(),
        written: written.clone(),
    };
    let writer = AuditWriter::start_with_sink(Box::new(sink), 1).unwrap();
    let guard = gate.guard();

    assert_eq!(
        writer.try_record(record("detach", "k", None, None)),
        Outcome::Accepted
    );
    wait_started(&started);

    // A heartbeat on the same current-thread runtime must keep ticking after
    // the drop: the worker is never blocked by a Drop-side join.
    let beats = Arc::new(AtomicUsize::new(0));
    let beats2 = beats.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.tick().await;
        loop {
            interval.tick().await;
            beats2.fetch_add(1, Ordering::SeqCst);
        }
    });

    drop(writer);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;
    assert!(
        beats.load(Ordering::SeqCst) > 0,
        "dropping a stuck writer must never block the current-thread worker"
    );
    heartbeat.abort();

    // Release the gate: the detached writer thread drains its accepted record
    // and exits on its own (no hang, no leak).
    drop(guard);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while written.lock().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the detached writer must drain its record once the gate is released"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        String::from_utf8_lossy(&written.lock().unwrap()).contains("detach"),
        "the detached writer must still write its accepted record"
    );
}
