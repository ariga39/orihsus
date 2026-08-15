use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orihsus::config::{ModelSync, Secret};
use orihsus::gateway::{RuntimeState, RuntimeStore};
use orihsus::models::{ModelFetchError, ModelFetcher, ModelMonitor};

fn store(models: &[&str]) -> RuntimeStore {
    RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("gateway-token"),
        base_url: "https://example.test/".parse().unwrap(),
        max_body_bytes: 1024,
        models: models.iter().map(|value| (*value).to_string()).collect(),
    })
}

struct StaticFetcher(Result<Vec<u8>, ModelFetchError>);

#[async_trait]
impl ModelFetcher for StaticFetcher {
    async fn fetch(&self) -> Result<Vec<u8>, ModelFetchError> {
        self.0.clone()
    }
}

#[tokio::test]
async fn successful_sync_atomically_replaces_the_runtime_allowlist() {
    let runtime = store(&["old-model"]);
    ModelMonitor::poll_once(
        &StaticFetcher(Ok(
            br#"{"object":"list","data":[{"id":"new-chat"},{"id":"new-reasoner"}]}"#.to_vec(),
        )),
        &runtime,
    )
    .await;

    assert_eq!(runtime.snapshot().models, vec!["new-chat", "new-reasoner"]);
    assert_eq!(runtime.snapshot().gateway_token.as_str(), "gateway-token");
}

#[tokio::test]
async fn failed_or_invalid_sync_keeps_the_last_known_good_allowlist() {
    for result in [
        Err(ModelFetchError::Transport),
        Ok(br#"{"object":"list","data":[]}"#.to_vec()),
        Ok(br#"{"object":"list","data":[{"id":"same"},{"id":"same"}]}"#.to_vec()),
        Ok(br#"not-json"#.to_vec()),
    ] {
        let runtime = store(&["last-known-good"]);
        ModelMonitor::poll_once(&StaticFetcher(result), &runtime).await;
        assert_eq!(runtime.snapshot().models, vec!["last-known-good"]);
    }
}

struct RecordingFetcher(tokio::sync::mpsc::UnboundedSender<()>);

#[async_trait]
impl ModelFetcher for RecordingFetcher {
    async fn fetch(&self) -> Result<Vec<u8>, ModelFetchError> {
        self.0.send(()).unwrap();
        Ok(br#"{"data":[{"id":"synced"}]}"#.to_vec())
    }
}

#[tokio::test(start_paused = true)]
async fn monitor_syncs_on_startup_and_then_at_the_configured_interval() {
    let runtime = store(&["fallback"]);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let monitor = ModelMonitor::start_with(
        ModelSync {
            enabled: true,
            interval: Duration::from_secs(3600),
        },
        runtime.clone(),
        Arc::new(RecordingFetcher(tx)),
    );

    tokio::task::yield_now().await;
    rx.try_recv().expect("startup must fetch immediately");
    assert_eq!(runtime.snapshot().models, vec!["synced"]);
    tokio::time::advance(Duration::from_secs(3599)).await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    rx.try_recv()
        .expect("configured interval must trigger a refresh");
    monitor.shutdown().await;
}

#[tokio::test]
async fn public_http_adapter_uses_the_models_endpoint_without_authentication() {
    use axum::http::HeaderMap;
    use axum::routing::get;

    let app = axum::Router::new().route(
        "/zen/go/v1/models",
        get(|headers: HeaderMap| async move {
            assert!(headers.get("authorization").is_none());
            assert_eq!(headers.get("accept").unwrap(), "application/json");
            br#"{"data":[{"id":"deepseek-chat"}]}"#.as_slice()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let fetcher = orihsus::models::HttpModelFetcher::for_test_endpoint(
        reqwest::Client::new(),
        format!("http://{addr}/zen/go/v1/models").parse().unwrap(),
    );

    assert_eq!(
        fetcher.fetch().await.unwrap(),
        br#"{"data":[{"id":"deepseek-chat"}]}"#
    );
    assert_eq!(
        orihsus::models::HttpModelFetcher::endpoint().as_str(),
        "https://opencode.ai/zen/go/v1/models"
    );
}
