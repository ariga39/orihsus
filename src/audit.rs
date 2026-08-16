use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::ser::SerializeStruct;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// SHA-256 hex digest of `secret` truncated to its first 12 characters.
///
/// The original secret is never returned or stored; only this derived
/// fingerprint leaves the module.
pub fn fingerprint(secret: impl AsRef<str>) -> String {
    let digest = Sha256::digest(secret.as_ref().as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    hex[..12].to_string()
}

/// Result of offering a record to the audit writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The record was accepted into the bounded queue; it will be written.
    Accepted,
    /// The record was dropped: the queue was full or the writer is gone.
    Dropped,
}

/// Terminal state of a streamed upstream response body, recorded as an optional
/// audit field. `None` (serialized as JSON `null`) for requests that never
/// streamed a body to the client — rejections, buffered error passthrough,
/// oversized-error streams — so the field is backward compatible with existing
/// lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// The upstream body reached EOF and the response streamed to completion.
    Completed,
    /// The upstream body stream errored after response headers were committed.
    /// The client already saw a response, so there is no retry.
    UpstreamError,
    /// The client dropped the response body before the upstream finished.
    ClientCancel,
    /// A committed SSE stream stopped producing complete events for its
    /// model-specific liveness window. The stream was terminated, never retried.
    EventIdleTimeout,
}

pub const MAX_AUDIT_ATTEMPTS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptTerminalReason {
    Completed,
    ResponseHeaderTimeout,
    NetworkError,
    RetryableResponse,
    Forwarded,
    UpstreamError,
    ClientCancel,
    NoFirstEvent,
    EventIdleTimeout,
    EndBeforeFirstEvent,
}

#[derive(Debug, Clone)]
pub struct AttemptSummary {
    pub attempt_number: u8,
    pub key_fingerprint: String,
    pub response_header_latency: Option<Duration>,
    pub first_byte_latency: Option<Duration>,
    pub first_event_latency: Option<Duration>,
    pub upstream_bytes: u64,
    pub upstream_chunks: u64,
    pub upstream_events: u64,
    pub last_activity_offset: Option<Duration>,
    pub precommit: bool,
    pub committed: bool,
    pub terminal_reason: AttemptTerminalReason,
    pub failover_target: Option<String>,
}

impl Serialize for AttemptSummary {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut st = serializer.serialize_struct("AttemptSummary", 13)?;
        st.serialize_field("attempt_number", &self.attempt_number)?;
        st.serialize_field("key_fingerprint", &self.key_fingerprint)?;
        st.serialize_field(
            "response_header_latency_ms",
            &self.response_header_latency.map(|value| value.as_millis()),
        )?;
        st.serialize_field(
            "first_byte_latency_ms",
            &self.first_byte_latency.map(|value| value.as_millis()),
        )?;
        st.serialize_field(
            "first_event_latency_ms",
            &self.first_event_latency.map(|value| value.as_millis()),
        )?;
        st.serialize_field("upstream_bytes", &self.upstream_bytes)?;
        st.serialize_field("upstream_chunks", &self.upstream_chunks)?;
        st.serialize_field("upstream_events", &self.upstream_events)?;
        st.serialize_field(
            "last_activity_offset_ms",
            &self.last_activity_offset.map(|value| value.as_millis()),
        )?;
        st.serialize_field("precommit", &self.precommit)?;
        st.serialize_field("committed", &self.committed)?;
        st.serialize_field("terminal_reason", &self.terminal_reason)?;
        st.serialize_field("failover_target", &self.failover_target)?;
        st.end()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(transparent)]
pub struct AttemptSummaries(Vec<AttemptSummary>);

impl AttemptSummaries {
    pub fn push(&mut self, summary: AttemptSummary) {
        if self.0.len() < MAX_AUDIT_ATTEMPTS {
            self.0.push(summary);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &AttemptSummary> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn last_mut(&mut self) -> Option<&mut AttemptSummary> {
        self.0.last_mut()
    }
}

/// One audit line. All fields are public; the caller supplies the timestamp.
/// `model` and `key_fingerprint` are `None` when the request never resolved a
/// model or a key (e.g. admission/body/pool failures before any selection) and
/// serialize as JSON `null` — never a forged empty string.
#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    pub model: Option<String>,
    pub key_fingerprint: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub status: u16,
    pub outcome: Option<AuditOutcome>,
    pub latency: Duration,
    pub opencode_session_id: Option<String>,
    pub opencode_project_id: Option<String>,
    pub opencode_request_id: Option<String>,
    pub attempts: AttemptSummaries,
}

impl Serialize for AuditRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut st = serializer.serialize_struct("AuditRecord", 13)?;
        st.serialize_field(
            "timestamp",
            &self
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )?;
        st.serialize_field("request_id", &self.request_id)?;
        st.serialize_field("model", &self.model)?;
        st.serialize_field("key_fingerprint", &self.key_fingerprint)?;
        st.serialize_field("input_tokens", &self.input_tokens)?;
        st.serialize_field("output_tokens", &self.output_tokens)?;
        st.serialize_field("status", &self.status)?;
        st.serialize_field("outcome", &self.outcome)?;
        st.serialize_field("latency_ms", &self.latency.as_millis())?;
        st.serialize_field("opencode_session_id", &self.opencode_session_id)?;
        st.serialize_field("opencode_project_id", &self.opencode_project_id)?;
        st.serialize_field("opencode_request_id", &self.opencode_request_id)?;
        st.serialize_field("attempts", &self.attempts)?;
        st.end()
    }
}

/// Fixed, short bound on awaiting a `Reopen` reply. The writer thread may be
/// stuck on slow/failed disk I/O, so waiting unboundedly would let a reopen
/// future hang a Tokio task (and a single-core runtime's only worker) forever.
/// After this elapses `reopen` returns `Err(ReopenTimeout)`; the caller treats
/// a failed reopen as best-effort and keeps the previous file active.
pub const AUDIT_REOPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Fixed, short bound on [`AuditWriter::shutdown_bounded`] awaiting the writer
/// thread. A writer stuck on slow/failed disk I/O would otherwise make a
/// synchronous join hang the caller (e.g. main's graceful shutdown) forever.
/// After this elapses `shutdown_bounded` returns `Err(ShutdownTimeout)`; the
/// writer and its detached supervisor thread may still be alive until the I/O
/// recovers or the process exits, and accepted-but-unflushed records may be
/// lost.
pub const AUDIT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors surfaced by the audit writer.
#[derive(Debug)]
pub enum AuditError {
    Io(std::io::Error),
    InvalidCapacity(usize),
    WriterPanicked,
    /// The bounded queue is full and the writer is busy, so a `Reopen` command
    /// could not even be enqueued. The current file stays active.
    ReopenQueueFull,
    /// The writer did not answer the `Reopen` within [`AUDIT_REOPEN_TIMEOUT`]
    /// (it is likely stalled on I/O). The command, if it was enqueued, may
    /// still be processed later; until then the previous file stays active.
    ReopenTimeout,
    /// The bounded [`AuditWriter::shutdown_bounded`] deadline elapsed before
    /// the writer thread (stuck on I/O) could be joined. The writer and its
    /// detached supervisor thread may still be alive until the I/O recovers or
    /// the process exits; records accepted but not yet flushed may be lost.
    ShutdownTimeout,
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditError::Io(e) => write!(f, "audit write failed: {e}"),
            AuditError::InvalidCapacity(c) => {
                write!(f, "audit queue capacity must be at least 1, got {c}")
            }
            AuditError::WriterPanicked => write!(f, "audit writer thread panicked"),
            AuditError::ReopenQueueFull => {
                write!(
                    f,
                    "audit reopen skipped: the audit queue is full; keeping the current file"
                )
            }
            AuditError::ReopenTimeout => write!(
                f,
                "audit reopen timed out ({}s); the writer may be stalled",
                AUDIT_REOPEN_TIMEOUT.as_secs()
            ),
            AuditError::ShutdownTimeout => write!(
                f,
                "audit shutdown timed out ({}s); the writer may be stuck on I/O and unflushed records may be lost",
                AUDIT_SHUTDOWN_TIMEOUT.as_secs()
            ),
        }
    }
}

impl std::error::Error for AuditError {}

/// A message the background writer thread handles. `Record` is the request hot
/// path (bounded, non-blocking via `try_send`); `Reopen` asks the writer to
/// open a fresh file and swap to it only once the open succeeds, replying over
/// a one-shot so the caller can await the swap.
enum Command {
    Record(AuditRecord),
    Reopen {
        path: PathBuf,
        reply: tokio::sync::oneshot::Sender<Result<(), AuditError>>,
    },
}

/// Handle to an asynchronous JSONL audit writer.
///
/// `try_record` never blocks on file IO. Accepted records are written to the
/// file by a single background thread; `shutdown`/`shutdown_bounded` flush and
/// join deterministically. Dropping the writer WITHOUT an explicit shutdown
/// never joins the writer thread (which could hang on stuck I/O): it drops the
/// command sender and detaches the thread, which drains what it has and exits
/// best-effort — unflushed records may be lost.
pub struct AuditWriter {
    tx: Option<SyncSender<Command>>,
    dropped: Arc<AtomicU64>,
    write_failures: Arc<AtomicU64>,
    drop_warned: Arc<AtomicBool>,
    detach_warned: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<(), AuditError>>>,
}

/// Static, desensitized one-shot warning printed to stderr on the first audit
/// write failure. No error detail, path or value is ever echoed.
const WRITE_WARNING: &str = "orihsus: audit write failed; the audit log may be incomplete \
(further failures are only counted)";

/// Static, desensitized one-shot warning printed to stderr on the first
/// dropped audit record. No request detail or value is ever echoed.
const DROP_WARNING: &str = "orihsus: audit record dropped; the queue is full or the writer is \
gone (further drops are only counted)";

/// Static, desensitized one-shot warning printed when the writer is dropped
/// without an explicit shutdown and its thread is detached mid-flight. No path
/// or value is ever echoed.
const DETACH_WARNING: &str = "orihsus: audit writer detached without a final flush; unflushed \
records may be lost";

/// One-shot stderr warning guard: prints `message` on the first call, stays
/// silent afterwards so a failing writer never floods stderr. Returns whether
/// this call performed the print.
fn warn_once(flag: &AtomicBool, message: &str) -> bool {
    if flag.swap(true, Ordering::Relaxed) {
        return false;
    }
    eprintln!("{message}");
    true
}

impl AuditWriter {
    /// Start a writer that appends JSONL lines to the file at `path`.
    pub fn start(path: impl AsRef<Path>, capacity: usize) -> Result<AuditWriter, AuditError> {
        let file = open_append(path.as_ref()).map_err(AuditError::Io)?;
        spawn(Box::new(file), capacity)
    }

    /// Start a writer over an arbitrary sink. This is the filesystem seam:
    /// production uses a real file via [`AuditWriter::start`]; tests may inject
    /// a blocking or failing sink. Hidden from docs but public for tests.
    #[doc(hidden)]
    pub fn start_with_sink(
        sink: Box<dyn Write + Send>,
        capacity: usize,
    ) -> Result<AuditWriter, AuditError> {
        spawn(sink, capacity)
    }

    /// Offer a record to the writer. Never blocks.
    pub fn try_record(&self, record: AuditRecord) -> Outcome {
        let Some(tx) = &self.tx else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            warn_once(&self.drop_warned, DROP_WARNING);
            return Outcome::Dropped;
        };
        match tx.try_send(Command::Record(record)) {
            Ok(()) => Outcome::Accepted,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                warn_once(&self.drop_warned, DROP_WARNING);
                Outcome::Dropped
            }
        }
    }

    /// Reopen the log file at `path` and swap the background writer onto it.
    ///
    /// The writer opens the new file itself and only swaps once the open
    /// succeeds: on `Err` the previous file stays active and keeps receiving
    /// records. On success, awaiting completion guarantees that every record
    /// offered afterwards lands in the reopened file.
    ///
    /// This call never blocks a Tokio worker. The `Reopen` command is offered
    /// with `try_send`: if the bounded queue is full (writer busy/stalled), it
    /// returns `Err(ReopenQueueFull)` immediately and the current file stays
    /// active. The writer's reply is awaited with the short
    /// [`AUDIT_REOPEN_TIMEOUT`]: if the writer is stalled on I/O the call
    /// returns `Err(ReopenTimeout)` instead of hanging the task. A command
    /// already enqueued when the timeout fires may still be processed later by
    /// the writer (swapping once it drains) — that is acceptable, since until
    /// then the previous file keeps receiving records and the caller treats a
    /// failed reopen as best-effort.
    pub async fn reopen(&self, path: impl AsRef<Path>) -> Result<(), AuditError> {
        let Some(tx) = &self.tx else {
            return Err(AuditError::WriterPanicked);
        };
        let (reply, reply_rx) = tokio::sync::oneshot::channel();
        match tx.try_send(Command::Reopen {
            path: path.as_ref().to_path_buf(),
            reply,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(AuditError::ReopenQueueFull),
            Err(TrySendError::Disconnected(_)) => return Err(AuditError::WriterPanicked),
        }
        match tokio::time::timeout(AUDIT_REOPEN_TIMEOUT, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(AuditError::WriterPanicked),
            Err(_) => Err(AuditError::ReopenTimeout),
        }
    }

    /// Number of records dropped because the queue was full (or the writer
    /// was gone), accumulated atomically across all callers.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of failed write/flush operations, accumulated by the background
    /// writer thread. Each failed `write_all` call and each failed `flush` call
    /// increments this once (a `write_all` is a single operation regardless of
    /// how many internal `write` calls it made), so a permanently failing sink
    /// with N accepted records counts N write failures plus one on shutdown's
    /// flush. Readable at any time without stopping the writer.
    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    /// Flush all accepted records and stop the background writer, returning
    /// the first write failure encountered (if any).
    pub fn shutdown(mut self) -> Result<(), AuditError> {
        self.shutdown_inner()
    }

    /// Bounded variant of [`AuditWriter::shutdown`] for async contexts (e.g.
    /// main's graceful shutdown). Drops the command sender first so no new
    /// records are accepted, then awaits the writer thread under a hard
    /// [`AUDIT_SHUTDOWN_TIMEOUT`]. The join runs on an ordinary detached
    /// `std::thread` supervisor that reports over a one-shot — never
    /// `spawn_blocking`, which the tokio runtime shutdown would wait for.
    ///
    /// On success returns the same result as [`AuditWriter::shutdown`]. If the
    /// writer is stuck on I/O the deadline yields [`AuditError::ShutdownTimeout`]
    /// instead of hanging the caller: the writer and supervisor may still be
    /// alive until the I/O recovers or the process exits, and accepted records
    /// that were not yet flushed may be lost.
    pub async fn shutdown_bounded(mut self) -> Result<(), AuditError> {
        if let Some(tx) = self.tx.take() {
            drop(tx);
        }
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let result = match handle.join() {
                Ok(inner) => inner,
                Err(_) => Err(AuditError::WriterPanicked),
            };
            let _ = done_tx.send(result);
        });
        match tokio::time::timeout(AUDIT_SHUTDOWN_TIMEOUT, done_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(AuditError::WriterPanicked),
            Err(_) => Err(AuditError::ShutdownTimeout),
        }
    }

    fn shutdown_inner(&mut self) -> Result<(), AuditError> {
        if let Some(tx) = self.tx.take() {
            drop(tx);
        }
        match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(result) => result,
                Err(_) => Err(AuditError::WriterPanicked),
            },
            None => Ok(()),
        }
    }
}

impl Drop for AuditWriter {
    fn drop(&mut self) {
        // Never join the writer thread here: a writer stuck on I/O must not
        // hang whatever drops the last handle (e.g. main's cleanup on a failed
        // lifecycle, or a reference race). Drop the command sender so the
        // writer drains what it has and then exits on its own, and detach the
        // thread — best-effort, no blocking. Explicit shutdown()/shutdown_bounded()
        // are the only paths that flush-and-join deterministically.
        if let Some(tx) = self.tx.take() {
            drop(tx);
        }
        if self.handle.take().is_some() {
            warn_once(&self.detach_warned, DETACH_WARNING);
        }
    }
}

#[async_trait]
impl crate::gateway::AuditSink for AuditWriter {
    fn record(&self, record: AuditRecord) -> Outcome {
        self.try_record(record)
    }

    async fn reopen(&self, path: &Path) -> Result<(), AuditError> {
        self.reopen(path).await
    }
}

fn open_append(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn spawn(sink: Box<dyn Write + Send>, capacity: usize) -> Result<AuditWriter, AuditError> {
    if capacity == 0 {
        return Err(AuditError::InvalidCapacity(capacity));
    }
    let (tx, rx) = sync_channel(capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    let write_failures = Arc::new(AtomicU64::new(0));
    let drop_warned = Arc::new(AtomicBool::new(false));
    let detach_warned = Arc::new(AtomicBool::new(false));
    let write_warned = Arc::new(AtomicBool::new(false));
    let wf = Arc::clone(&write_failures);
    let ww = Arc::clone(&write_warned);
    let handle = thread::spawn(move || write_loop(sink, rx, wf, ww));
    Ok(AuditWriter {
        tx: Some(tx),
        dropped,
        write_failures,
        drop_warned,
        detach_warned,
        handle: Some(handle),
    })
}

fn write_loop(
    mut sink: Box<dyn Write + Send>,
    rx: Receiver<Command>,
    write_failures: Arc<AtomicU64>,
    write_warned: Arc<AtomicBool>,
) -> Result<(), AuditError> {
    let mut first_error: Option<std::io::Error> = None;
    while let Ok(command) = rx.recv() {
        match command {
            Command::Record(record) => {
                let mut line = match serde_json::to_vec(&record) {
                    Ok(line) => line,
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(std::io::Error::other(format!(
                                "serialize audit record: {e}"
                            )));
                        }
                        continue;
                    }
                };
                line.push(b'\n');
                if let Err(e) = sink.write_all(&line) {
                    write_failures.fetch_add(1, Ordering::Relaxed);
                    warn_once(&write_warned, WRITE_WARNING);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
            Command::Reopen { path, reply } => {
                // Open first, swap only on success: a failed open keeps the
                // previous file active so the writer never loses its sink.
                match open_append(&path) {
                    Ok(file) => {
                        sink = Box::new(file);
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(AuditError::Io(e)));
                    }
                }
            }
        }
    }
    if let Err(e) = sink.flush() {
        write_failures.fetch_add(1, Ordering::Relaxed);
        warn_once(&write_warned, WRITE_WARNING);
        if first_error.is_none() {
            first_error = Some(e);
        }
    }
    match first_error {
        Some(e) => Err(AuditError::Io(e)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warn_once_prints_only_on_the_first_call() {
        let flag = AtomicBool::new(false);
        assert!(
            warn_once(&flag, "first warning"),
            "the first call must warn"
        );
        assert!(
            !warn_once(&flag, "second warning"),
            "later calls must stay silent"
        );
        assert!(!warn_once(&flag, "third warning"));
    }

    #[test]
    fn warn_once_is_one_shot_even_across_failing_flags() {
        let a = AtomicBool::new(false);
        let b = AtomicBool::new(false);
        assert!(warn_once(&a, "a"));
        assert!(warn_once(&b, "b"), "independent flags each warn once");
        assert!(!warn_once(&a, "a again"));
        assert!(!warn_once(&b, "b again"));
    }
}
