//! Fail-safe synchronization of the public OpenCode Go model list.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use crate::config::{ModelSync, MAX_MODEL_BYTES};
use crate::gateway::RuntimeStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelFetchError {
    Timeout,
    HttpStatus,
    BodyTooLarge,
    Transport,
}

#[async_trait]
pub trait ModelFetcher: Send + Sync {
    async fn fetch(&self) -> Result<Vec<u8>, ModelFetchError>;
}

pub struct HttpModelFetcher {
    client: reqwest::Client,
    endpoint: url::Url,
}

impl std::fmt::Debug for HttpModelFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpModelFetcher")
    }
}

impl HttpModelFetcher {
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: Self::endpoint(),
        })
    }

    pub fn endpoint() -> url::Url {
        let base = url::Url::parse(crate::config::OPENCODE_GO_BASE_URL)
            .expect("built-in OpenCode Go base URL is valid");
        crate::config::upstream_api_url(&base, crate::config::UpstreamApi::Models)
    }

    #[doc(hidden)]
    pub fn for_test_endpoint(client: reqwest::Client, endpoint: url::Url) -> Self {
        Self { client, endpoint }
    }
}

#[async_trait]
impl ModelFetcher for HttpModelFetcher {
    async fn fetch(&self) -> Result<Vec<u8>, ModelFetchError> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let response = self
                .client
                .get(self.endpoint.clone())
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await
                .map_err(|_| ModelFetchError::Transport)?;
            if !response.status().is_success() {
                return Err(ModelFetchError::HttpStatus);
            }
            const BODY_LIMIT: usize = 1024 * 1024;
            if response
                .content_length()
                .is_some_and(|length| length > BODY_LIMIT as u64)
            {
                return Err(ModelFetchError::BodyTooLarge);
            }
            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| ModelFetchError::Transport)?;
                if body.len().saturating_add(chunk.len()) > BODY_LIMIT {
                    return Err(ModelFetchError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        })
        .await
        .map_err(|_| ModelFetchError::Timeout)?
    }
}

pub struct ModelMonitor {
    config: tokio::sync::watch::Sender<ModelSync>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ModelSyncHandle(tokio::sync::watch::Sender<ModelSync>);

impl ModelSyncHandle {
    pub fn replace(&self, config: ModelSync) -> ModelSync {
        self.0.send_replace(config)
    }
}

impl ModelMonitor {
    pub fn start(config: ModelSync, runtime: RuntimeStore) -> Result<Self, reqwest::Error> {
        Ok(Self::start_with(
            config,
            runtime,
            Arc::new(HttpModelFetcher::new()?),
        ))
    }

    #[doc(hidden)]
    pub fn start_with(
        config: ModelSync,
        runtime: RuntimeStore,
        fetcher: Arc<dyn ModelFetcher>,
    ) -> Self {
        let (config_tx, mut config_rx) = tokio::sync::watch::channel(config);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            loop {
                let generation = config_rx.borrow().clone();
                if !generation.enabled {
                    tokio::select! {
                        changed = config_rx.changed() => if changed.is_err() { return; },
                        _ = &mut stop_rx => return,
                    }
                    continue;
                }
                let fetched = fetcher.fetch().await;
                if *config_rx.borrow() == generation {
                    if let Ok(body) = fetched {
                        if let Some(models) = parse_models(&body) {
                            runtime.update_models(models);
                        }
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(generation.interval) => {},
                    changed = config_rx.changed() => if changed.is_err() { return; },
                    _ = &mut stop_rx => return,
                }
            }
        });
        Self {
            config: config_tx,
            stop: Some(stop_tx),
            worker: Some(worker),
        }
    }

    pub fn config_handle(&self) -> ModelSyncHandle {
        ModelSyncHandle(self.config.clone())
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
    }

    #[doc(hidden)]
    pub async fn poll_once(fetcher: &dyn ModelFetcher, runtime: &RuntimeStore) {
        let Ok(body) = fetcher.fetch().await else {
            return;
        };
        let Some(models) = parse_models(&body) else {
            return;
        };
        runtime.update_models(models);
    }
}

impl Drop for ModelMonitor {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

fn parse_models(body: &[u8]) -> Option<Vec<String>> {
    let root: serde_json::Value = serde_json::from_slice(body).ok()?;
    let data = root.get("data")?.as_array()?;
    if data.is_empty() {
        return None;
    }
    let mut models = Vec::with_capacity(data.len());
    let mut seen = std::collections::HashSet::with_capacity(data.len());
    for item in data {
        let id = item.get("id")?.as_str()?;
        if id.trim().is_empty() || id.len() > MAX_MODEL_BYTES || !seen.insert(id) {
            return None;
        }
        models.push(id.to_string());
    }
    Some(models)
}
