//! Fail-open proactive polling of the OpenCode Go usage endpoint.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;

use crate::audit::fingerprint;
use crate::config::{Secret, Usage};
use crate::pool::KeyPool;

const USAGE_HISTORY_QUEUE_CAPACITY: usize = 4096;
const HISTORY_WARNING: &str =
    "orihsus: usage history write failed or record dropped; polling remains active";

#[derive(Debug, Clone, serde::Serialize)]
struct UsageWindowRecord {
    status: Option<String>,
    percent: Option<f64>,
    #[serde(rename = "resetsAt")]
    resets_at: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct UsageHistoryRecord {
    timestamp: String,
    key_fingerprint: String,
    rolling: UsageWindowRecord,
    weekly: UsageWindowRecord,
    monthly: UsageWindowRecord,
}

#[derive(Clone)]
pub struct UsageHistorySink {
    tx: SyncSender<UsageHistoryRecord>,
    dropped: Arc<AtomicU64>,
    warned: Arc<AtomicBool>,
}

impl UsageHistorySink {
    fn try_record(&self, record: UsageHistoryRecord) {
        if matches!(
            self.tx.try_send(record),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_))
        ) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            if !self.warned.swap(true, Ordering::Relaxed) {
                eprintln!("{HISTORY_WARNING}");
            }
        }
    }

    pub fn dropped_records(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Bounded, non-blocking writer for per-key usage snapshots.
pub struct UsageHistoryWriter {
    sink: Option<UsageHistorySink>,
    worker: Option<JoinHandle<()>>,
    write_failures: Arc<AtomicU64>,
}

impl UsageHistoryWriter {
    pub fn start(directory: impl Into<PathBuf>) -> Self {
        Self::start_with_capacity(directory, USAGE_HISTORY_QUEUE_CAPACITY)
    }

    #[doc(hidden)]
    pub fn start_with_capacity(directory: impl Into<PathBuf>, capacity: usize) -> Self {
        let directory = directory.into();
        let (tx, rx) = sync_channel(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let write_failures = Arc::new(AtomicU64::new(0));
        let failures = write_failures.clone();
        let warned = Arc::new(AtomicBool::new(false));
        let worker_warned = warned.clone();
        let worker = thread::spawn(move || {
            while let Ok(record) = rx.recv() {
                if write_history_record(&directory, &record).is_err() {
                    failures.fetch_add(1, Ordering::Relaxed);
                    if !worker_warned.swap(true, Ordering::Relaxed) {
                        eprintln!("{HISTORY_WARNING}");
                    }
                }
            }
        });
        Self {
            sink: Some(UsageHistorySink {
                tx,
                dropped,
                warned,
            }),
            worker: Some(worker),
            write_failures,
        }
    }

    pub fn sink(&self) -> UsageHistorySink {
        self.sink.as_ref().expect("writer is active").clone()
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    pub fn shutdown(&mut self) {
        self.sink.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for UsageHistoryWriter {
    fn drop(&mut self) {
        self.sink.take();
        // Never block shutdown on a stuck filesystem. Explicit shutdown joins
        // after the polling worker has released its sink; Drop detaches.
        self.worker.take();
    }
}

fn write_history_record(directory: &Path, record: &UsageHistoryRecord) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    let path = directory.join(format!("{}.jsonl", &record.timestamp[..10]));
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer(&mut file, record).map_err(std::io::Error::other)?;
    file.write_all(b"\n")
}

fn parse_history_record(
    body: &[u8],
    timestamp: DateTime<Utc>,
    key: &Secret,
) -> Option<UsageHistoryRecord> {
    let root: serde_json::Value = serde_json::from_slice(body).ok()?;
    let usage = root.get("usage").unwrap_or(&root);
    let window = |name: &str| {
        let value = usage.get(name);
        UsageWindowRecord {
            status: value
                .and_then(|v| v.get("status"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            percent: value
                .and_then(|v| v.get("percent").or_else(|| v.get("usagePercent")))
                .and_then(serde_json::Value::as_f64),
            resets_at: value
                .and_then(|v| v.get("resetsAt"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        }
    };
    Some(UsageHistoryRecord {
        timestamp: timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        key_fingerprint: fingerprint(key.as_str()),
        rolling: window("rolling"),
        weekly: window("weekly"),
        monthly: window("monthly"),
    })
}

/// Wall-clock seam used only to translate an absolute reset into a duration.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Static, secret-free usage fetch failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageFetchError {
    Timeout,
    HttpStatus,
    BodyTooLarge,
    Transport,
}

#[async_trait]
pub trait UsageFetcher: Send + Sync {
    async fn fetch(&self, key: &Secret) -> Result<Vec<u8>, UsageFetchError>;
}

pub struct HttpUsageFetcher {
    client: reqwest::Client,
    endpoint: url::Url,
}

impl std::fmt::Debug for HttpUsageFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpUsageFetcher")
    }
}

impl HttpUsageFetcher {
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            endpoint: Self::endpoint(),
        })
    }

    pub fn endpoint() -> url::Url {
        let base = url::Url::parse(crate::config::OPENCODE_GO_BASE_URL)
            .expect("built-in OpenCode Go base URL is valid");
        crate::config::upstream_api_url(&base, crate::config::UpstreamApi::Usage)
    }

    #[doc(hidden)]
    pub fn for_test_endpoint(client: reqwest::Client, endpoint: url::Url) -> Self {
        Self { client, endpoint }
    }
}

#[async_trait]
impl UsageFetcher for HttpUsageFetcher {
    async fn fetch(&self, key: &Secret) -> Result<Vec<u8>, UsageFetchError> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let response = self
                .client
                .get(self.endpoint.clone())
                .bearer_auth(key.as_str())
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await
                .map_err(|_| UsageFetchError::Transport)?;
            if !response.status().is_success() {
                return Err(UsageFetchError::HttpStatus);
            }
            const BODY_LIMIT: usize = 64 * 1024;
            if response
                .content_length()
                .is_some_and(|len| len > BODY_LIMIT as u64)
            {
                return Err(UsageFetchError::BodyTooLarge);
            }
            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| UsageFetchError::Transport)?;
                if body.len().saturating_add(chunk.len()) > BODY_LIMIT {
                    return Err(UsageFetchError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        })
        .await
        .map_err(|_| UsageFetchError::Timeout)?
    }
}

pub struct UsageMonitor {
    keys: tokio::sync::watch::Sender<Vec<Secret>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    history: Option<UsageHistoryWriter>,
}

#[derive(Clone)]
pub struct UsageKeysHandle(tokio::sync::watch::Sender<Vec<Secret>>);

impl UsageKeysHandle {
    pub fn replace_keys(&self, keys: Vec<Secret>) {
        self.0.send_replace(keys);
    }
}

impl UsageMonitor {
    pub fn start(
        config: Usage,
        history_dir: PathBuf,
        keys: Vec<Secret>,
        pool: Arc<KeyPool>,
    ) -> Result<UsageMonitor, reqwest::Error> {
        Ok(Self::start_with_history(
            config,
            keys,
            pool,
            Arc::new(HttpUsageFetcher::new()?),
            Arc::new(SystemClock),
            Some(UsageHistoryWriter::start(history_dir)),
        ))
    }

    #[doc(hidden)]
    pub fn start_with(
        config: Usage,
        keys: Vec<Secret>,
        pool: Arc<KeyPool>,
        fetcher: Arc<dyn UsageFetcher>,
        clock: Arc<dyn Clock>,
    ) -> UsageMonitor {
        Self::start_with_history(config, keys, pool, fetcher, clock, None)
    }

    #[doc(hidden)]
    pub fn start_with_history(
        config: Usage,
        keys: Vec<Secret>,
        pool: Arc<KeyPool>,
        fetcher: Arc<dyn UsageFetcher>,
        clock: Arc<dyn Clock>,
        history: Option<UsageHistoryWriter>,
    ) -> UsageMonitor {
        let (keys_tx, mut keys_rx) = tokio::sync::watch::channel(keys);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let history_sink = history.as_ref().map(UsageHistoryWriter::sink);
        let worker = tokio::spawn(async move {
            loop {
                let keys = keys_rx.borrow().clone();
                Self::poll_once_with_history(
                    fetcher.as_ref(),
                    clock.as_ref(),
                    config.soft_threshold_percent,
                    &keys,
                    &pool,
                    history_sink.as_ref(),
                )
                .await;
                tokio::select! {
                    _ = tokio::time::sleep(config.poll_interval) => {},
                    changed = keys_rx.changed() => if changed.is_err() { return; },
                    _ = &mut stop_rx => return,
                }
            }
        });
        UsageMonitor {
            keys: keys_tx,
            stop: Some(stop_tx),
            worker: Some(worker),
            history,
        }
    }

    pub fn replace_keys(&self, keys: Vec<Secret>) {
        self.keys.send_replace(keys);
    }

    pub fn keys_handle(&self) -> UsageKeysHandle {
        UsageKeysHandle(self.keys.clone())
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        if let Some(mut history) = self.history.take() {
            history.shutdown();
        }
    }

    /// Execute one non-overlapping round. Individual key failures are fail-open.
    #[doc(hidden)]
    pub async fn poll_once(
        fetcher: &dyn UsageFetcher,
        clock: &dyn Clock,
        soft_threshold_percent: f64,
        keys: &[Secret],
        pool: &Arc<KeyPool>,
    ) {
        Self::poll_once_with_history(fetcher, clock, soft_threshold_percent, keys, pool, None)
            .await;
    }

    #[doc(hidden)]
    pub async fn poll_once_with_history(
        fetcher: &dyn UsageFetcher,
        clock: &dyn Clock,
        soft_threshold_percent: f64,
        keys: &[Secret],
        pool: &Arc<KeyPool>,
        history: Option<&UsageHistorySink>,
    ) {
        let now = clock.now();
        let results = futures_util::future::join_all(
            keys.iter()
                .map(|key| async move { (key, fetcher.fetch(key).await) }),
        )
        .await;
        for (key, result) in results {
            let Ok(body) = result else { continue };
            if let (Some(history), Some(record)) = (history, parse_history_record(&body, now, key))
            {
                history.try_record(record);
            }
            let Some(cooldown) = evaluate(&body, now, soft_threshold_percent) else {
                continue;
            };
            pool.report_proactive_cooldown(&fingerprint(key.as_str()), cooldown);
        }
    }
}

impl Drop for UsageMonitor {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

fn evaluate(body: &[u8], now: DateTime<Utc>, threshold: f64) -> Option<Duration> {
    let root: serde_json::Value = serde_json::from_slice(body).ok()?;
    let usage = root.get("usage").unwrap_or(&root);
    let top_limited = root.get("status").and_then(|v| v.as_str()) == Some("rate-limited");
    let has_limited_window = ["rolling", "weekly", "monthly"].iter().any(|name| {
        usage
            .get(name)
            .and_then(|w| w.get("status"))
            .and_then(|v| v.as_str())
            == Some("rate-limited")
    });
    let mut latest = None;
    for name in ["rolling", "weekly", "monthly"] {
        let Some(window) = usage.get(name) else {
            continue;
        };
        let percent = window
            .get("percent")
            .or_else(|| window.get("usagePercent"))
            .and_then(serde_json::Value::as_f64);
        let limited = window.get("status").and_then(|v| v.as_str()) == Some("rate-limited");
        let top_applies = top_limited && (!has_limited_window || limited);
        if !top_applies
            && !limited
            && !percent
                .is_some_and(|p| p.is_finite() && (0.0..=100.0).contains(&p) && p >= threshold)
        {
            continue;
        }
        let Some(reset) = window
            .get("resetsAt")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        else {
            continue;
        };
        if reset <= now {
            continue;
        }
        latest = Some(latest.map_or(reset, |old: DateTime<Utc>| old.max(reset)));
    }
    let reset = latest?;
    (reset - now).to_std().ok()
}
