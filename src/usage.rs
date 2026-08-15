//! Fail-open proactive polling of the OpenCode Go usage endpoint.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;

use crate::audit::fingerprint;
use crate::config::{Secret, Usage};
use crate::pool::KeyPool;

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
        keys: Vec<Secret>,
        pool: Arc<KeyPool>,
    ) -> Result<UsageMonitor, reqwest::Error> {
        Ok(Self::start_with(
            config,
            keys,
            pool,
            Arc::new(HttpUsageFetcher::new()?),
            Arc::new(SystemClock),
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
        let (keys_tx, mut keys_rx) = tokio::sync::watch::channel(keys);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            loop {
                let keys = keys_rx.borrow().clone();
                Self::poll_once(
                    fetcher.as_ref(),
                    clock.as_ref(),
                    config.soft_threshold_percent,
                    &keys,
                    &pool,
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
        let now = clock.now();
        let results = futures_util::future::join_all(
            keys.iter()
                .map(|key| async move { (key, fetcher.fetch(key).await) }),
        )
        .await;
        for (key, result) in results {
            let Ok(body) = result else { continue };
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
