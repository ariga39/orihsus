use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orihsus::audit::fingerprint;
use orihsus::config::Secret;
use orihsus::pool::{AttemptResult, KeyPool, NoJitter, PoolPolicy};
use orihsus::usage::{Clock, UsageFetchError, UsageFetcher, UsageMonitor};

fn pool() -> Arc<KeyPool> {
    Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("key-a"), Secret::new("key-b")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 5,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(30),
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    )
}

struct FixedClock(DateTime<Utc>);
impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct StaticFetcher(&'static [u8]);
#[async_trait]
impl UsageFetcher for StaticFetcher {
    async fn fetch(&self, _key: &Secret) -> Result<Vec<u8>, UsageFetchError> {
        Ok(self.0.to_vec())
    }
}

struct ErrorFetcher;
#[async_trait]
impl UsageFetcher for ErrorFetcher {
    async fn fetch(&self, _key: &Secret) -> Result<Vec<u8>, UsageFetchError> {
        Err(UsageFetchError::Timeout)
    }
}

#[tokio::test(start_paused = true)]
async fn rolling_percent_at_threshold_cools_the_key_until_its_reset() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher =
        StaticFetcher(br#"{"usage":{"rolling":{"percent":80,"resetsAt":"2026-08-14T12:10:00Z"}}}"#);

    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;

    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(selected) => selected,
        other => panic!("expected selected key, got {other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-b"));
}

#[tokio::test(start_paused = true)]
async fn multiple_triggered_windows_cool_until_the_latest_reset() {
    let pool = Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("key-a")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 5,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(30),
                max_attempts: 1,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    );
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher = StaticFetcher(br#"{"usage":{"rolling":{"percent":85,"resetsAt":"2026-08-14T12:00:10Z"},"weekly":{"percent":90,"resetsAt":"2026-08-14T12:00:20Z"}}}"#);
    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(19)).await;
    assert!(
        !handle.is_finished(),
        "earlier rolling reset must not recover key"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn top_level_rate_limited_uses_the_latest_valid_window_reset() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher = StaticFetcher(br#"{"status":"rate-limited","usage":{"rolling":{"percent":10,"resetsAt":"2026-08-14T12:05:00Z"},"weekly":{"percent":20,"resetsAt":"2026-08-14T12:20:00Z"}}}"#);
    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-b"));
}

#[tokio::test(start_paused = true)]
async fn percentages_below_threshold_leave_the_pool_unchanged() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher = StaticFetcher(br#"{"usage":{"rolling":{"percent":79.9,"resetsAt":"2026-08-14T13:00:00Z"},"weekly":{"percent":20,"resetsAt":"2026-08-20T12:00:00Z"}}}"#);
    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-a"));
}

#[tokio::test(start_paused = true)]
async fn past_or_invalid_resets_fail_open() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher = StaticFetcher(br#"{"usage":{"rolling":{"percent":99,"resetsAt":"not-a-date"},"weekly":{"percent":99,"resetsAt":"2026-08-14T11:59:59Z"}}}"#);
    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-a"));
}

#[tokio::test(start_paused = true)]
async fn fetch_and_json_errors_fail_open() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    UsageMonitor::poll_once(
        &ErrorFetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    UsageMonitor::poll_once(
        &StaticFetcher(b"not-json"),
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-a"));
}

#[tokio::test(start_paused = true)]
async fn window_rate_limited_cools_without_a_percentage() {
    let pool = pool();
    let now = "2026-08-14T12:00:00Z".parse().unwrap();
    let fetcher = StaticFetcher(
        br#"{"usage":{"monthly":{"status":"rate-limited","resetsAt":"2026-09-01T00:00:00Z"}}}"#,
    );
    UsageMonitor::poll_once(
        &fetcher,
        &FixedClock(now),
        80.0,
        &[Secret::new("key-a")],
        &pool,
    )
    .await;
    let mut req = pool.request();
    let selected = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-b"));
}

async fn start_https(app: axum::Router) -> (std::net::SocketAddr, tempfile::TempDir) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.key_pair.serialize_pem()).unwrap();
    let tls = orihsus::server::load_server_config(&cert, &key).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(orihsus::server::serve(
        listener,
        app,
        Some(tokio_rustls::TlsAcceptor::from(Arc::new(tls))),
        orihsus::server::Http1Limits {
            header_read_timeout: Duration::from_secs(5),
            max_header_bytes: 32 * 1024,
        },
        Arc::new(tokio::sync::Semaphore::new(16)),
        Duration::from_secs(5),
        std::future::pending(),
    ));
    (addr, dir)
}

#[tokio::test]
async fn http_adapter_uses_fixed_get_endpoint_and_bearer_header() {
    use axum::http::{HeaderMap, Method};
    use axum::routing::get;
    let app = axum::Router::new().route(
        "/zen/go/v1/usage",
        get(|method: Method, headers: HeaderMap| async move {
            assert_eq!(method, Method::GET);
            assert_eq!(
                headers.get("authorization").unwrap(),
                "Bearer adapter-secret"
            );
            assert_eq!(headers.get("accept").unwrap(), "application/json");
            br#"{"usage":{}}"#.as_slice()
        }),
    );
    let (addr, _cert) = start_https(app).await;
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let fetcher = orihsus::usage::HttpUsageFetcher::for_test_endpoint(
        client,
        format!("https://127.0.0.1:{}/zen/go/v1/usage", addr.port())
            .parse()
            .unwrap(),
    );
    assert_eq!(
        orihsus::usage::HttpUsageFetcher::endpoint().as_str(),
        "https://opencode.ai/zen/go/v1/usage"
    );
    assert_eq!(
        fetcher.fetch(&Secret::new("adapter-secret")).await.unwrap(),
        br#"{"usage":{}}"#
    );
}

#[tokio::test]
async fn http_adapter_classifies_non_success_without_exposing_key_or_body() {
    use axum::http::StatusCode;
    use axum::routing::get;
    let app = axum::Router::new().route(
        "/zen/go/v1/usage",
        get(|| async { (StatusCode::UNAUTHORIZED, "response-body-must-not-leak") }),
    );
    let (addr, _cert) = start_https(app).await;
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let fetcher = orihsus::usage::HttpUsageFetcher::for_test_endpoint(
        client,
        format!("https://127.0.0.1:{}/zen/go/v1/usage", addr.port())
            .parse()
            .unwrap(),
    );
    let err = fetcher
        .fetch(&Secret::new("raw-key-must-not-leak"))
        .await
        .unwrap_err();
    let rendered = format!("{err:?} {fetcher:?}");
    assert_eq!(err, UsageFetchError::HttpStatus);
    assert!(!rendered.contains("raw-key-must-not-leak"));
    assert!(!rendered.contains("response-body-must-not-leak"));
}

#[tokio::test]
async fn http_adapter_rejects_response_bodies_over_sixty_four_kibibytes() {
    use axum::routing::get;
    let body = vec![b'x'; 64 * 1024 + 1];
    let app = axum::Router::new().route(
        "/zen/go/v1/usage",
        get(move || {
            let body = body.clone();
            async move { body }
        }),
    );
    let (addr, _cert) = start_https(app).await;
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let fetcher = orihsus::usage::HttpUsageFetcher::for_test_endpoint(
        client,
        format!("https://127.0.0.1:{}/zen/go/v1/usage", addr.port())
            .parse()
            .unwrap(),
    );
    assert_eq!(
        fetcher.fetch(&Secret::new("key")).await.unwrap_err(),
        UsageFetchError::BodyTooLarge
    );
}

#[tokio::test(start_paused = true)]
async fn http_adapter_times_out_the_whole_request_after_ten_seconds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let fetcher = orihsus::usage::HttpUsageFetcher::for_test_endpoint(
        client,
        format!("https://127.0.0.1:{}/zen/go/v1/usage", addr.port())
            .parse()
            .unwrap(),
    );
    let handle = tokio::spawn(async move { fetcher.fetch(&Secret::new("timeout-key")).await });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(
        !handle.is_finished(),
        "must allow the request for nine seconds"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(handle.await.unwrap().unwrap_err(), UsageFetchError::Timeout);
}

struct RecordingFetcher(tokio::sync::mpsc::UnboundedSender<String>);
#[async_trait]
impl UsageFetcher for RecordingFetcher {
    async fn fetch(&self, key: &Secret) -> Result<Vec<u8>, UsageFetchError> {
        self.0.send(key.as_str().to_string()).unwrap();
        Ok(br#"{}"#.to_vec())
    }
}

#[tokio::test(start_paused = true)]
async fn monitor_polls_immediately_on_startup() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let monitor = UsageMonitor::start_with(
        orihsus::config::Usage {
            soft_threshold_percent: 80.0,
            poll_interval: Duration::from_secs(300),
        },
        vec![Secret::new("key-a")],
        pool(),
        Arc::new(RecordingFetcher(tx)),
        Arc::new(FixedClock("2026-08-14T12:00:00Z".parse().unwrap())),
    );
    tokio::task::yield_now().await;
    assert_eq!(rx.try_recv().unwrap(), "key-a");
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn replacing_keys_immediately_polls_only_the_new_generation() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let monitor = UsageMonitor::start_with(
        orihsus::config::Usage {
            soft_threshold_percent: 80.0,
            poll_interval: Duration::from_secs(300),
        },
        vec![Secret::new("old")],
        pool(),
        Arc::new(RecordingFetcher(tx)),
        Arc::new(FixedClock("2026-08-14T12:00:00Z".parse().unwrap())),
    );
    tokio::task::yield_now().await;
    assert_eq!(rx.try_recv().unwrap(), "old");
    monitor.replace_keys(vec![Secret::new("new")]);
    tokio::task::yield_now().await;
    assert_eq!(rx.try_recv().unwrap(), "new");
    assert!(rx.try_recv().is_err());
    monitor.shutdown().await;
}

struct GatedFetcher {
    seen: tokio::sync::mpsc::UnboundedSender<String>,
    release_first: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl UsageFetcher for GatedFetcher {
    async fn fetch(&self, key: &Secret) -> Result<Vec<u8>, UsageFetchError> {
        self.seen.send(key.as_str().to_string()).unwrap();
        if key.as_str() == "first" {
            self.release_first.notified().await;
        }
        Ok(br#"{}"#.to_vec())
    }
}

#[tokio::test(start_paused = true)]
async fn one_slow_key_does_not_block_other_keys_in_the_same_round() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let monitor = UsageMonitor::start_with(
        orihsus::config::Usage {
            soft_threshold_percent: 80.0,
            poll_interval: Duration::from_secs(300),
        },
        vec![Secret::new("first"), Secret::new("second")],
        pool(),
        Arc::new(GatedFetcher {
            seen: tx,
            release_first: release.clone(),
        }),
        Arc::new(FixedClock("2026-08-14T12:00:00Z".parse().unwrap())),
    );
    tokio::task::yield_now().await;
    assert_eq!(rx.try_recv().unwrap(), "first");
    assert_eq!(
        rx.try_recv().unwrap(),
        "second",
        "slow first key must not serialize the round"
    );
    release.notify_waiters();
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn rounds_never_overlap_and_shutdown_joins_the_worker() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let monitor = UsageMonitor::start_with(
        orihsus::config::Usage {
            soft_threshold_percent: 80.0,
            poll_interval: Duration::from_secs(30),
        },
        vec![Secret::new("first")],
        pool(),
        Arc::new(GatedFetcher {
            seen: tx,
            release_first: release.clone(),
        }),
        Arc::new(FixedClock("2026-08-14T12:00:00Z".parse().unwrap())),
    );
    tokio::task::yield_now().await;
    assert_eq!(rx.try_recv().unwrap(), "first");
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert!(
        rx.try_recv().is_err(),
        "no second round while first is in flight"
    );
    release.notify_waiters();
    monitor.shutdown().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert!(
        rx.try_recv().is_err(),
        "joined worker cannot poll after shutdown"
    );
}
