use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use orihsus::audit::{fingerprint, AuditError, AuditOutcome, AuditRecord, AuditWriter, Outcome};
use orihsus::config::Secret;
use orihsus::gateway::{
    build_router, AuditSink, BodyBudget, GatewayState, IoTimeouts, RuntimeState, RuntimeStore,
};
use orihsus::pool::{KeyPool, NoJitter, PoolPolicy};
use orihsus::queue::AdmissionQueue;
use tempfile::TempDir;
use tower::ServiceExt;
use url::Url;

#[derive(Clone, Default)]
struct MockControl {
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    sse: Arc<Mutex<Option<SseControl>>>,
}

struct SseControl {
    event1: Vec<u8>,
    event2: Vec<u8>,
    event2b: Option<Vec<u8>>,
    gate2: tokio::sync::mpsc::Receiver<()>,
    cancelled: Arc<AtomicBool>,
    cancel_notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct MockResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    extra_headers: Vec<(String, String)>,
}

impl MockResponse {
    fn json(status: u16, body: &'static [u8]) -> MockResponse {
        MockResponse {
            status,
            content_type: "application/json",
            body: body.to_vec(),
            extra_headers: Vec::new(),
        }
    }
}

impl Default for MockResponse {
    fn default() -> Self {
        MockResponse::json(
            200,
            br#"{"id":"cmpl-0","object":"chat.completion","choices":[]}"#,
        )
    }
}

struct CapturedRequest {
    method: String,
    path: String,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

async fn mock_chat(
    axum::extract::State(control): axum::extract::State<Arc<MockControl>>,
    req: axum::http::Request<axum::body::Body>,
) -> axum::response::Response {
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    control.requests.lock().unwrap().push(CapturedRequest {
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        headers: parts.headers,
        body: body_bytes.to_vec(),
    });
    if let Some(sse) = control.sse.lock().unwrap().take() {
        return sse_response(sse);
    }
    let resp = control
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_default();
    let mut rb = axum::response::Response::builder()
        .status(resp.status)
        .header("content-type", resp.content_type)
        .header("x-upstream-marker", "yes");
    for (k, v) in &resp.extra_headers {
        rb = rb.header(k, v);
    }
    rb.body(axum::body::Body::from(resp.body)).unwrap()
}

fn sse_response(sse: SseControl) -> axum::response::Response {
    use futures_util::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;
    let (tx, rx) = tokio::sync::mpsc::channel::<axum::body::Bytes>(4);
    let cancelled = sse.cancelled.clone();
    let notify = sse.cancel_notify.clone();
    tokio::spawn(async move {
        let mut sse = sse;
        if tx.send(axum::body::Bytes::from(sse.event1)).await.is_err() {
            return;
        }
        let _ = sse.gate2.recv().await;
        if tx.send(axum::body::Bytes::from(sse.event2)).await.is_err() {
            cancelled.store(true, Ordering::SeqCst);
            notify.notify_one();
            return;
        }
        if let Some(part2) = sse.event2b {
            if tx.send(axum::body::Bytes::from(part2)).await.is_err() {
                cancelled.store(true, Ordering::SeqCst);
                notify.notify_one();
            }
        }
    });
    axum::response::Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("x-upstream-marker", "yes")
        .body(axum::body::Body::from_stream(
            ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>),
        ))
        .unwrap()
}

async fn start_mock(control: Arc<MockControl>) -> std::net::SocketAddr {
    use axum::routing::post;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route("/v1/chat/completions", post(mock_chat))
        .route("/v1/messages", post(mock_chat))
        .route("/v1/responses", post(mock_chat))
        .with_state(control);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

#[tokio::test(start_paused = true)]
async fn silent_first_sse_attempt_fails_over_before_client_commit() {
    let control = Arc::new(MockControl::default());
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    control.sse.lock().unwrap().replace(SseControl {
        event1: Vec::new(),
        event2: b"data: {\"never\":true}\n\n".to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 200,
        content_type: "text/event-stream",
        body: b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"
            .to_vec(),
        extra_headers: vec![("x-attempt".to_string(), "second".to_string())],
    });
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        IoTimeouts {
            first_event: Duration::from_secs(5),
            ..IoTimeouts::default()
        },
    ));

    let task = tokio::spawn(async move { send(&app, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let resp = task.await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-attempt").unwrap(), "second");
    let body = body_string(resp).await;
    assert!(body.contains("\"content\":\"ok\""));
    assert!(!body.contains("never"));

    let requests = control.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].headers.get("authorization").unwrap(),
        "Bearer key-1"
    );
    assert_eq!(
        requests[1].headers.get("authorization").unwrap(),
        "Bearer key-2"
    );
    drop(requests);
    drop(gate_tx);

    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1);
    let attempts: Vec<_> = records[0].attempts.iter().collect();
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].terminal_reason,
        orihsus::audit::AttemptTerminalReason::NoFirstEvent
    );
    assert_eq!(
        attempts[0].failover_target.as_deref(),
        Some(fingerprint("key-2").as_str())
    );
    assert_eq!(
        attempts[1].terminal_reason,
        orihsus::audit::AttemptTerminalReason::Completed
    );
}

#[tokio::test(start_paused = true)]
async fn active_reasoning_events_reset_the_inter_event_watchdog() {
    let control = Arc::new(MockControl::default());
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    control.sse.lock().unwrap().replace(SseControl {
        event1: b"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n"
            .to_vec(),
        event2: b"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"still thinking\"}}]}\n\ndata: [DONE]\n\n"
            .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    let addr = start_mock(control).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with_timeouts(
        &pool(&["key-1"]),
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        IoTimeouts {
            first_event: Duration::from_secs(5),
            inter_event: Duration::from_secs(5),
            ..IoTimeouts::default()
        },
    ));

    let task = tokio::spawn(async move { send(&app, chat_req()).await });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let resp = task.await.unwrap();
    let mut body = resp.into_body();
    let first = next_data_frame(&mut body).await.unwrap();
    assert!(String::from_utf8_lossy(&first).contains("thinking"));
    tokio::time::advance(Duration::from_secs(4)).await;
    gate_tx.send(()).await.unwrap();
    let mut rest = Vec::new();
    while let Some(frame) = next_data_frame(&mut body).await {
        rest.extend_from_slice(&frame);
    }
    assert!(String::from_utf8_lossy(&rest).contains("still thinking"));
    assert_eq!(q.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn cancel_before_first_event_deadline_does_not_cool_the_key_model_pair() {
    let control = Arc::new(MockControl::default());
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    control.sse.lock().unwrap().replace(SseControl {
        event1: Vec::new(),
        event2: b"data: [DONE]\n\n".to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"after-cancel","choices":[]}"#,
        ));
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        IoTimeouts {
            first_event: Duration::from_secs(5),
            ..IoTimeouts::default()
        },
    ));

    let app_first = app.clone();
    let first = tokio::spawn(async move { send(&app_first, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    first.abort();
    let _ = first.await;
    drop(gate_tx);

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let requests = control.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].headers.get("authorization").unwrap(),
        "Bearer key-1",
        "a pre-deadline user cancel must not add liveness cooldown"
    );
}

#[derive(Clone, Default)]
struct TestSink(Arc<Mutex<Vec<AuditRecord>>>);

#[async_trait]
impl AuditSink for TestSink {
    fn record(&self, record: AuditRecord) -> Outcome {
        self.0.lock().unwrap().push(record);
        Outcome::Accepted
    }

    async fn reopen(&self, _path: &Path) -> Result<(), AuditError> {
        Ok(())
    }
}

fn pool(keys: &[&str]) -> Arc<KeyPool> {
    pool_with_timeout(keys, Duration::from_secs(2))
}

/// A roomy body budget that never throttles the sequential tiny-body tests here:
/// 256MiB total, each request reserving `max_body_bytes`.
fn budget_for(max_body_bytes: usize) -> BodyBudget {
    BodyBudget::new(256 * 1024 * 1024, max_body_bytes as u32)
}

fn pool_with_timeout(keys: &[&str], wait_timeout: Duration) -> Arc<KeyPool> {
    Arc::new(
        KeyPool::with_jitter(
            keys.iter().map(|k| Secret::new(*k)).collect(),
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 5,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout,
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    )
}

/// A pool whose consecutive-failure circuit breaker trips after `threshold`
/// network failures (the other helpers hardcode 5), so a test can reach a trip
/// in a handful of requests.
fn pool_with_breaker(keys: &[&str], threshold: u32) -> Arc<KeyPool> {
    pool_with_timeout_breaker(keys, threshold, Duration::from_secs(2))
}

fn pool_with_timeout_breaker(
    keys: &[&str],
    threshold: u32,
    wait_timeout: Duration,
) -> Arc<KeyPool> {
    Arc::new(
        KeyPool::with_jitter(
            keys.iter().map(|k| Secret::new(*k)).collect(),
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: threshold,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout,
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    )
}

fn queue(max_concurrency: usize, max_queue: usize, wait_timeout: Duration) -> Arc<AdmissionQueue> {
    Arc::new(AdmissionQueue::new(
        max_concurrency,
        max_queue,
        wait_timeout,
    ))
}

fn state(
    upstream: &str,
    keys: &[&str],
    queue: Arc<AdmissionQueue>,
    sink: TestSink,
) -> GatewayState {
    state_with(&pool(keys), &queue, upstream, sink)
}

fn state_with(
    pool: &Arc<KeyPool>,
    queue: &Arc<AdmissionQueue>,
    upstream: &str,
    sink: TestSink,
) -> GatewayState {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    GatewayState::new(
        http,
        Url::parse(upstream).unwrap(),
        Arc::clone(pool),
        Arc::clone(queue),
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::new(sink),
        1 << 20,
        budget_for(1 << 20),
        IoTimeouts::default(),
    )
}

fn state_limited(
    upstream: &str,
    keys: &[&str],
    queue: Arc<AdmissionQueue>,
    sink: TestSink,
    max_body_bytes: usize,
) -> GatewayState {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    GatewayState::new(
        http,
        Url::parse(upstream).unwrap(),
        pool(keys),
        queue,
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::new(sink),
        max_body_bytes,
        budget_for(max_body_bytes),
        IoTimeouts::default(),
    )
}

fn state_with_timeouts(
    pool: &Arc<KeyPool>,
    queue: &Arc<AdmissionQueue>,
    upstream: &str,
    sink: TestSink,
    timeouts: IoTimeouts,
) -> GatewayState {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    GatewayState::new(
        http,
        Url::parse(upstream).unwrap(),
        Arc::clone(pool),
        Arc::clone(queue),
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::new(sink),
        1 << 20,
        budget_for(1 << 20),
        timeouts,
    )
}

/// Build a gateway with an explicit body budget, byte limit and I/O timeouts
/// for the budget-behaviour tests.
fn state_with_budget(
    pool: &Arc<KeyPool>,
    queue: &Arc<AdmissionQueue>,
    upstream: &str,
    sink: TestSink,
    budget: BodyBudget,
    max_body_bytes: usize,
    timeouts: IoTimeouts,
) -> GatewayState {
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    GatewayState::new(
        http,
        Url::parse(upstream).unwrap(),
        Arc::clone(pool),
        Arc::clone(queue),
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::new(sink),
        max_body_bytes,
        budget,
        timeouts,
    )
}

#[tokio::test]
async fn healthz_is_always_ok() {
    let sink = TestSink::default();
    let app = build_router(state(
        "http://127.0.0.1:1",
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink,
    ));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn health_checks_never_enter_the_audit_queue() {
    let sink = TestSink::default();
    let app = build_router(state(
        "http://127.0.0.1:1",
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));
    for path in ["/healthz", "/readyz"] {
        let resp = send(
            &app,
            Request::builder().uri(path).body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert!(sink.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn every_client_route_writes_exactly_one_redacted_audit_record() {
    // The per-request audit contract covers non-health client-facing routes:
    // /v1/models (authed + auth failure), wrong-method fallbacks and unknown paths write
    // exactly one redacted record echoing the validated request id and the
    // final status, with model/key/usage null.
    let sink = TestSink::default();
    let q = queue(2, 2, Duration::from_secs(30));
    let p = pool(&["a"]);
    let app = build_router(state_with(&p, &q, "http://127.0.0.1:1", sink.clone()));

    fn req(method: &str, uri: &str, rid: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("x-request-id", rid)
            .body(Body::empty())
            .unwrap()
    }

    let cases: &[(&str, &str, &str, u16, bool)] = &[
        ("POST", "/healthz", "rid-healthz-405", 405, false),
        ("POST", "/readyz", "rid-readyz-405", 405, false),
        ("GET", "/v1/models", "rid-models-ok", 200, true),
        ("GET", "/v1/models", "rid-models-401", 401, false),
        ("POST", "/v1/models", "rid-models-405", 405, false),
        ("GET", "/v1/chat/completions", "rid-chat-405", 405, false),
        ("GET", "/v1/messages", "rid-messages-405", 405, false),
        ("GET", "/v1/responses", "rid-responses-405", 405, false),
        ("GET", "/unknown", "rid-not-found", 404, false),
    ];

    for (i, &(method, uri, rid, expected, authorized)) in cases.iter().enumerate() {
        let mut r = req(method, uri, rid);
        if authorized {
            r.headers_mut().insert(
                "authorization",
                axum::http::HeaderValue::from_static("Bearer gway-token"),
            );
        }
        let resp = send(&app, r).await;
        assert_eq!(
            resp.status().as_u16(),
            expected,
            "response for {method} {uri} ({rid})"
        );
        if expected == 405 {
            let v: serde_json::Value = serde_json::from_str(&body_string(resp).await)
                .expect("{rid}: a 405 must carry the gateway's OpenAI error body");
            assert_eq!(
                v["error"]["type"], "invalid_request_error",
                "{rid}: 405 uses the OpenAI-style error object"
            );
        }
        let records = sink.0.lock().unwrap();
        assert_eq!(
            records.len(),
            i + 1,
            "{method} {uri} ({rid}): exactly one audit record per request"
        );
        let rec = &records[i];
        assert_eq!(rec.request_id, rid, "{rid}: request id echoed/validated");
        assert_eq!(rec.status, expected, "{rid}: final status audited");
        assert_eq!(rec.model, None, "{rid}: model must be null");
        assert_eq!(rec.key_fingerprint, None, "{rid}: key must be null");
        assert_eq!(rec.input_tokens, None, "{rid}: usage must be null");
        assert_eq!(rec.output_tokens, None, "{rid}: usage must be null");
    }
}

async fn send(app: &axum::Router, req: Request<Body>) -> axum::response::Response {
    app.clone().oneshot(req).await.unwrap()
}

/// Send a request under a paused clock (`start_paused`) where the real upstream
/// mock must round-trip. A pending `tokio::time` timer (e.g. the upstream header
/// timeout) would otherwise be auto-advanced before the real-IO response lands,
/// so drive the request on a spawned task and yield to let the mock respond.
async fn send_io(app: &axum::Router, req: Request<Body>) -> axum::response::Response {
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, req).await });
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    handle.await.unwrap()
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn next_data_frame(body: &mut axum::body::Body) -> Option<axum::body::Bytes> {
    loop {
        let frame = body.frame().await?;
        match frame {
            Ok(f) => match f.into_data() {
                Ok(d) => return Some(d),
                Err(_) => continue,
            },
            Err(_) => return None,
        }
    }
}

fn models_req() -> Request<Body> {
    Request::builder()
        .uri("/v1/models")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn models_requires_bearer_token_and_returns_static_list() {
    let sink = TestSink::default();
    let app = build_router(state(
        "http://127.0.0.1:1",
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink,
    ));

    // missing token -> 401 OpenAI error
    let resp = send(&app, models_req()).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"]["type"], "authentication_error");
    assert_eq!(v["error"]["code"], "invalid_api_key");

    // wrong format -> 401
    let resp = send(
        &app,
        Request::builder()
            .uri("/v1/models")
            .header("authorization", "Basic abc")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // wrong token -> 401
    let resp = send(
        &app,
        Request::builder()
            .uri("/v1/models")
            .header("authorization", "Bearer wrong-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // valid token -> static model list
    let resp = send(
        &app,
        Request::builder()
            .uri("/v1/models")
            .header("authorization", "Bearer gway-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["object"], "list");
    assert_eq!(v["data"][0]["id"], "deepseek-chat");
    assert_eq!(v["data"][0]["object"], "model");
}

#[tokio::test]
async fn routing_returns_404_and_405_with_openai_error() {
    let sink = TestSink::default();
    let app = build_router(state(
        "http://127.0.0.1:1",
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink,
    ));

    let resp = send(
        &app,
        Request::builder()
            .uri("/unknown")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "unknown path -> 404");
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "invalid_request_error");

    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "models POST -> 405"
    );

    let resp = send(
        &app,
        Request::builder()
            .method("GET")
            .uri("/v1/chat/completions")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "chat GET -> 405"
    );
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "invalid_request_error");

    let resp = send(
        &app,
        Request::builder()
            .method("PUT")
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "models PUT -> 405"
    );
}

#[tokio::test]
async fn all_proxy_endpoints_reject_models_outside_the_configured_bounded_allowlist() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));

    for endpoint in ["/v1/chat/completions", "/v1/messages", "/v1/responses"] {
        for model in ["not-configured".to_string(), "m".repeat(257)] {
            let body = serde_json::json!({"model": model, "messages": []}).to_string();
            let resp = send(
                &app,
                Request::builder()
                    .method("POST")
                    .uri(endpoint)
                    .header("authorization", "Bearer gway-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{endpoint}");
        }
    }
    assert!(
        control.requests.lock().unwrap().is_empty(),
        "invalid models must never reach the upstream"
    );
}

#[tokio::test]
async fn readyz_reflects_pool_and_queue_readiness() {
    use orihsus::pool::Failure;

    let p = pool(&["a"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        "http://127.0.0.1:1",
        TestSink::default(),
    ));

    fn readyz_req(rid: &str) -> Request<Body> {
        Request::builder()
            .uri("/readyz")
            .header("x-request-id", rid)
            .body(Body::empty())
            .unwrap()
    }

    // healthy -> 200
    let resp = send(&app, readyz_req("rid-readyz-ok")).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "healthy pool + open queue -> 200"
    );

    // All keys cooling: return the client-visible rate-limit contract.
    // error carrying Retry-After.
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(&sel, Failure::Unavailable { retry_after: None });
    let resp = send(&app, readyz_req("rid-readyz-429-cooling")).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "all keys cooling -> 429"
    );
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json",
        "readiness failure must be an OpenAI-formatted JSON gateway error"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "readiness failure must carry Retry-After"
    );
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "error": {
                "message": "All upstream keys are temporarily rate limited",
                "type": "rate_limit_error",
                "param": null,
                "code": "upstream_keys_unavailable"
            }
        }),
        "readiness failure uses the standard OpenAI error object"
    );

    // Closed queue: same OpenAI 503 contract.
    q.close();
    let resp = send(&app, readyz_req("rid-readyz-503-closed")).await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "closed queue -> 503"
    );
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json",
        "closed-queue readiness failure must be a JSON gateway error"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "closed-queue readiness failure must carry Retry-After"
    );
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "error": {
                "message": "Service Unavailable",
                "type": "service_unavailable",
                "param": null,
                "code": null
            }
        }),
        "closed-queue readiness failure uses the standard OpenAI error object"
    );
}

#[tokio::test]
async fn chat_non_streaming_passthrough_headers_and_audit() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    control.responses.lock().unwrap().push_back(MockResponse::json(
        200,
        br#"{"id":"cmpl-1","object":"chat.completion","model":"deepseek-chat","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":20}}"#,
    ));

    let payload = serde_json::json!({
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": "hi"}],
        "unknown_field": 123,
        "tools": [{"type": "function", "function": {"name": "f"}}],
    });
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", "opencode/1.2.3")
            .header("x-request-id", "my-req-1")
            .header("x-opencode-project", "project-42")
            .header("x-opencode-session", "session-7")
            .header("x-opencode-request", "request-9")
            .header("x-opencode-client", "desktop")
            .header("x-opencode-directory", "%2Fworkspace")
            .header("x-opencode-workspace", "workspace-3")
            .header("cookie", "session=client-secret")
            .header("x-api-key", "client-api-secret")
            .header("x-opencode-api-key", "prefixed-client-secret")
            .header("proxy-authorization", "Basic client-secret")
            .header("x-forwarded-for", "203.0.113.9")
            .header("traceparent", "00-secret-trace-01")
            .header("tracestate", "vendor=secret")
            .header("baggage", "account=secret")
            .header("x-custom", "drop-me")
            .header("connection", "x-dyn")
            .header("x-dyn", "strip-me")
            .header("keep-alive", "timeout=5")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("x-request-id").unwrap(),
        "my-req-1",
        "request id echoed"
    );
    assert_eq!(
        resp.headers().get("x-upstream-marker").unwrap(),
        "yes",
        "end-to-end header forwarded"
    );
    assert!(
        !resp.headers().contains_key("keep-alive"),
        "hop-by-hop header stripped"
    );

    let body = body_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["id"], "cmpl-1");
    assert_eq!(v["choices"][0]["message"]["content"], "hi");
    assert_eq!(
        v["usage"]["prompt_tokens"], 10,
        "body passed through unchanged"
    );

    let requests = control.requests.lock().unwrap();
    let upstream_headers = &requests[0].headers;
    assert_eq!(
        upstream_headers.get("content-type").unwrap(),
        "application/json"
    );
    assert_eq!(upstream_headers.get("accept").unwrap(), "application/json");
    assert_eq!(
        upstream_headers.get("user-agent").unwrap(),
        "opencode/1.2.3"
    );
    assert_eq!(upstream_headers.get("x-request-id").unwrap(), "my-req-1");
    for (name, expected) in [
        ("x-opencode-project", "project-42"),
        ("x-opencode-session", "session-7"),
        ("x-opencode-request", "request-9"),
        ("x-opencode-client", "desktop"),
        ("x-opencode-directory", "%2Fworkspace"),
        ("x-opencode-workspace", "workspace-3"),
    ] {
        assert_eq!(upstream_headers.get(name).unwrap(), expected);
    }
    assert_eq!(
        upstream_headers.get("authorization").unwrap(),
        "Bearer key-1"
    );
    for rejected in [
        "cookie",
        "x-api-key",
        "x-opencode-api-key",
        "proxy-authorization",
        "x-forwarded-for",
        "traceparent",
        "tracestate",
        "baggage",
        "x-custom",
        "connection",
        "x-dyn",
        "keep-alive",
    ] {
        assert!(
            !upstream_headers.contains_key(rejected),
            "client header {rejected} reached the credential-bearing upstream"
        );
    }
    drop(requests);

    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1, "one audit record per request");
    let r = &records[0];
    assert_eq!(r.request_id, "my-req-1");
    assert_eq!(r.model.as_deref(), Some("deepseek-chat"));
    assert_eq!(
        r.key_fingerprint.as_deref(),
        Some(fingerprint("key-1").as_str())
    );
    assert_eq!(r.input_tokens, Some(10));
    assert_eq!(r.output_tokens, Some(20));
    assert_eq!(r.status, 200);
    assert_eq!(r.opencode_session_id.as_deref(), Some("session-7"));
    assert_eq!(r.opencode_project_id.as_deref(), Some("project-42"));
    assert_eq!(r.opencode_request_id.as_deref(), Some("request-9"));
    let attempts: Vec<_> = r.attempts.iter().collect();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt_number, 1);
    assert_eq!(attempts[0].key_fingerprint, fingerprint("key-1"));
    assert!(attempts[0].response_header_latency.is_some());
    assert!(attempts[0].first_byte_latency.is_some());
    assert!(attempts[0].first_event_latency.is_none());
    assert!(attempts[0].upstream_bytes > 0);
    assert!(attempts[0].upstream_chunks > 0);
    assert_eq!(attempts[0].upstream_events, 0);
    assert!(attempts[0].last_activity_offset.is_some());
    assert!(!attempts[0].precommit);
    assert!(attempts[0].committed);
    assert_eq!(
        attempts[0].terminal_reason,
        orihsus::audit::AttemptTerminalReason::Completed
    );
    assert!(attempts[0].failover_target.is_none());
    drop(records);

    let captured = control.requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let cr = &captured[0];
    assert_eq!(cr.method, "POST");
    assert_eq!(cr.path, "/v1/chat/completions");
    assert_eq!(
        cr.headers.get("authorization").unwrap(),
        "Bearer key-1",
        "auth replaced with selected key"
    );
    assert!(
        !cr.headers.contains_key("x-dyn"),
        "Connection-declared header stripped"
    );
    assert!(!cr.headers.contains_key("connection"));
    assert!(
        !cr.headers.contains_key("keep-alive"),
        "hop-by-hop stripped"
    );
    assert_eq!(
        cr.headers.get("host").unwrap(),
        &format!("{addr}"),
        "Host rebuilt from the upstream URL, never the client's"
    );
    assert!(!cr.headers.contains_key("x-custom"));
    let fwd: serde_json::Value = serde_json::from_slice(&cr.body).unwrap();
    assert_eq!(
        fwd["unknown_field"], 123,
        "unknown fields forwarded byte-for-byte"
    );
    assert!(fwd["tools"].is_array(), "tools forwarded");
}

#[tokio::test]
async fn messages_endpoint_transparently_uses_anthropic_transport() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));
    let upstream_body =
        br#"{"id":"msg_1","type":"message","content":[{"type":"text","text":"hi"}]}"#;
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(200, upstream_body));
    let request_body = br#"{"model":"deepseek-chat","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}"#;

    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header("x-api-key", "gway-token")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "prompt-caching-2024-07-31")
            .header("content-type", "application/json")
            .body(Body::from(request_body.as_slice()))
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await.as_bytes(), upstream_body);
    let requests = control.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/messages");
    assert_eq!(requests[0].body, request_body);
    assert_eq!(requests[0].headers.get("x-api-key").unwrap(), "key-1");
    assert!(!requests[0].headers.contains_key("authorization"));
    assert_eq!(
        requests[0].headers.get("anthropic-version").unwrap(),
        "2023-06-01"
    );
    assert_eq!(
        requests[0].headers.get("anthropic-beta").unwrap(),
        "prompt-caching-2024-07-31"
    );
}

#[tokio::test]
async fn responses_endpoint_transparently_uses_openai_transport() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));
    let upstream_body = br#"{"id":"resp_1","object":"response","status":"completed","output":[]}"#;
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(200, upstream_body));
    let request_body = br#"{"model":"deepseek-chat","input":"hello","store":false}"#;

    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("authorization", "Bearer gway-token")
            .header("content-type", "application/json")
            .body(Body::from(request_body.as_slice()))
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await.as_bytes(), upstream_body);
    let requests = control.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/responses");
    assert_eq!(requests[0].body, request_body);
    assert_eq!(
        requests[0].headers.get("authorization").unwrap(),
        "Bearer key-1"
    );
    assert!(!requests[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn oversized_body_returns_413_openai_error() {
    let sink = TestSink::default();
    let app = build_router(state_limited(
        "http://127.0.0.1:1",
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink,
        16,
    ));

    let big = vec![b'x'; 100];
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .body(Body::from(big))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "invalid_request_error");
}

/// Assert the last `TestSink` record is a body-read/selection rejection: exactly
/// one record appended per request, with the given status and null model, key
/// and usage.
fn assert_rejected_audit(sink: &TestSink, expected_len: usize, status: u16) {
    let records = sink.0.lock().unwrap();
    assert_eq!(
        records.len(),
        expected_len,
        "exactly one record per authenticated request"
    );
    let r = &records[expected_len - 1];
    assert_eq!(r.status, status);
    assert_eq!(r.model, None);
    assert_eq!(r.key_fingerprint, None);
    assert_eq!(r.input_tokens, None);
    assert_eq!(r.output_tokens, None);
}

#[tokio::test(start_paused = true)]
async fn body_read_rejections_are_audited_exactly_once_with_correct_status() {
    use futures_util::StreamExt;
    use std::convert::Infallible;

    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let app = build_router(GatewayState::new(
        http,
        Url::parse("http://127.0.0.1:1").unwrap(),
        pool(&["a"]),
        q.clone(),
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::new(sink.clone()),
        16,
        budget_for(16),
        IoTimeouts {
            body_read: Duration::from_secs(10),
            ..IoTimeouts::default()
        },
    ));

    // Unauthenticated POST -> 401, audited exactly once with null
    // model/key/usage: the body is never read (only headers are inspected).
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .body(Body::from(vec![b'x'; 100]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_rejected_audit(&sink, 1, 401);
    {
        let records = sink.0.lock().unwrap();
        assert!(
            !records[0].request_id.is_empty()
                && records[0]
                    .request_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
            "the 401 audit must carry a generated/validated request id"
        );
    }

    // The 401 path must never touch the request body: a stalled body returns
    // 401 immediately instead of waiting out the body_read deadline.
    let stall = futures_util::stream::pending::<Result<Bytes, Infallible>>();
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .body(Body::from_stream(stall))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_rejected_audit(&sink, 2, 401);

    // Body too large -> 413, audited once with null model/key/usage.
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .body(Body::from(vec![b'x'; 100]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_rejected_audit(&sink, 3, 413);

    // A body whose stream errors mid-read (invalid body) -> 400, audited once.
    let err_body = Body::from_stream(futures_util::stream::iter([Err::<Bytes, _>(
        std::io::Error::other("broken"),
    )]));
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .body(err_body)
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_rejected_audit(&sink, 4, 400);

    // A stalled body upload -> 503, audited once; permit released.
    let partial =
        futures_util::stream::iter([Ok::<Bytes, Infallible>(Bytes::from("{\"model\":\"x\""))]);
    let stall = futures_util::stream::pending::<Result<Bytes, Infallible>>();
    let body = Body::from_stream(partial.chain(stall));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    let handle = tokio::spawn(app.clone().oneshot(req));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap().unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_rejected_audit(&sink, 5, 503);
    assert_eq!(
        q.snapshot().active,
        0,
        "permit released after the body timeout"
    );
}

fn chat_req() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"deepseek-chat","messages":[{"role":"user","content":"hi"}]}"#.to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn retries_on_429_with_a_different_key_and_parses_retry_after() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink.clone()));

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: br#"{"error":{"message":"rate limited"}}"#.to_vec(),
        extra_headers: vec![("retry-after".to_string(), "7".to_string())],
    });
    control.responses.lock().unwrap().push_back(MockResponse::json(
        200,
        br#"{"id":"cmpl-ok","object":"chat.completion","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}"#,
    ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "failover to the second key succeeds"
    );
    let body = body_string(resp).await;
    assert!(body.contains("cmpl-ok"));

    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(captured.len(), 2, "two distinct keys attempted");
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-1"
        );
        assert_eq!(
            captured[1].headers.get("authorization").unwrap(),
            "Bearer key-2"
        );
    }

    {
        let records = sink.0.lock().unwrap();
        assert_eq!(
            records.len(),
            1,
            "one audit record for the successful request"
        );
        assert_eq!(
            records[0].key_fingerprint.as_deref(),
            Some(fingerprint("key-2").as_str())
        );
        assert_eq!(records[0].status, 200);
    }

    // key-1 was cooled by the 429 with Retry-After: 7 honored; next request uses key-2
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-next","object":"chat.completion"}"#,
        ));
    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(
            captured[2].headers.get("authorization").unwrap(),
            "Bearer key-2",
            "cooled key-1 no longer selected"
        );
    }
}

#[tokio::test]
async fn retries_on_5xx_without_disabling_the_key() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            502,
            br#"{"error":{"message":"upstream exploded"}}"#,
        ));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK, "5xx failover succeeds");

    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-1"
        );
        assert_eq!(
            captured[1].headers.get("authorization").unwrap(),
            "Bearer key-2"
        );
    }

    // 5xx must NOT cool/switch key-1: a fresh request goes back to key-1
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-next","object":"chat.completion"}"#,
        ));
    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(
            captured[2].headers.get("authorization").unwrap(),
            "Bearer key-1",
            "5xx does not disable the key"
        );
    }
}

#[tokio::test]
async fn upstream_down_returns_503_and_does_not_disable_keys() {
    let sink = TestSink::default();
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(&p, &q, "http://127.0.0.1:1", sink));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "network errors exhaust attempts -> 503"
    );
    assert!(resp.headers().contains_key("retry-after"));

    let resp = send(
        &app,
        Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "network failures do not disable keys"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_delta_seconds_is_honored_by_the_pool() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink));

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: b"{}".to_vec(),
        extra_headers: vec![("retry-after".to_string(), "8".to_string())],
    });
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "single key: the upstream 429 is passed through after exhaustion"
    );

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "Retry-After of 8s must be honored, not the 5s default backoff"
    );
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "key recovered after the honored Retry-After"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_http_date_is_honored_as_cooldown() {
    use chrono::{Duration as ChronoDuration, Utc};

    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink));

    // The upstream asks for a retry 30s from now in RFC 9110 IMF-fixdate
    // form. The gateway resolves it against its own wall clock, so the literal
    // must be computed at test time rather than pinned to a fixed date.
    let retry_at = Utc::now() + ChronoDuration::seconds(30);
    let http_date = retry_at.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: b"{}".to_vec(),
        extra_headers: vec![("retry-after".to_string(), http_date)],
    });
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "single key: the upstream 429 is passed through after exhaustion"
    );

    // The honored HTTP-date Retry-After (30s) must win over the 5s default
    // backoff, and the key must recover exactly after the 30s.
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    // Drain the mock round trip so the assertion can only pass when the
    // request is genuinely parked on the cooldown, not mid-flight over IO.
    for _ in 0..200 {
        tokio::task::yield_now().await;
        if handle.is_finished() {
            break;
        }
    }
    assert!(
        !handle.is_finished(),
        "HTTP-date Retry-After of 30s must be honored, not the 5s default backoff"
    );
    tokio::time::advance(Duration::from_secs(25)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "key recovered after the honored HTTP-date Retry-After"
    );
}

fn usage_limit_body(message: &str) -> Vec<u8> {
    format!(
        r#"{{"type":"error","error":{{"type":"GoUsageLimitError","message":"{message}"}},"metadata":{{"workspace":"wrk_x","limitName":"weekly"}}}}"#
    )
    .into_bytes()
}

#[tokio::test]
async fn huge_retry_after_delta_seconds_fails_over_without_panic() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink));

    // Retry-After: u64::MAX is a constructible Duration whose addition to an
    // Instant overflows and would panic/abort the process. It must instead be
    // clamped and the request must fail over to the next key.
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: b"{}".to_vec(),
        extra_headers: vec![("retry-after".to_string(), u64::MAX.to_string())],
    });
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}"#,
        ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "failover to the second key succeeds despite a huge Retry-After"
    );
    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(captured.len(), 2, "two distinct keys attempted");
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-1"
        );
        assert_eq!(
            captured[1].headers.get("authorization").unwrap(),
            "Bearer key-2"
        );
    }

    // key-1 must stay cooled for the clamped ceiling (not forever, not overflow):
    // a fresh request skips it.
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-next","object":"chat.completion"}"#,
        ));
    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(
            captured[2].headers.get("authorization").unwrap(),
            "Bearer key-2",
            "the huge Retry-After must clamp (not panic) and keep key-1 cooling"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn huge_usage_limit_resets_message_returns_bounded_retry_after_without_panic() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(2));
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink));

    // "Resets in 106751991167301 days" -> 9223372036854806400s: constructible as
    // a Duration but pushes `Instant + Duration` past its representable range.
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: usage_limit_body("Weekly usage limit reached. Resets in 106751991167301 days."),
        extra_headers: Vec::new(),
    });

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "single key: the upstream 429 is passed through after exhaustion, no panic"
    );

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let resp = handle
        .await
        .expect("the huge usage-limit cooldown must be clamped, never panic");
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "all keys cooling for the clamped ceiling -> self-produced 429"
    );
    let ra = resp
        .headers()
        .get("retry-after")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(
        ra,
        orihsus::pool::MAX_COOLDOWN.as_secs() - 2,
        "the returned Retry-After must match the clamped internal cooldown (finite, serializable)"
    );
}

#[tokio::test]
async fn body_budget_prevents_second_body_buffering_until_first_releases() {
    use futures_util::TryStreamExt;
    use std::convert::Infallible;

    // Budget = exactly one body: capacity equals the per-request reservation.
    // Both A and B pass admission (max_concurrency generous); the single-body
    // budget alone creates the contention.
    let budget = BodyBudget::new(1 << 20, 1 << 20);
    let p = pool(&["a"]);
    let q = queue(4, 4, Duration::from_secs(30));
    let sink = TestSink::default();
    let (addr, control) = start_gated_header_mock(
        br#"data: {"id":"a","choices":[]}

"#
        .to_vec(),
    )
    .await;
    let app = build_router(state_with_budget(
        &p,
        &q,
        &format!("http://{addr}"),
        sink,
        budget,
        1 << 20,
        IoTimeouts::default(),
    ));

    // A: a gated body that signals `started` once the gateway is mid-read
    // (i.e. holding the budget) and then waits for `release` before EOF.
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel::<()>(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let body_a = {
        let release = release.clone();
        Body::from_stream(futures_util::stream::unfold(true, move |first| {
            let started_tx = started_tx.clone();
            let release = release.clone();
            async move {
                if first {
                    let _ = started_tx.try_send(());
                    release.notified().await;
                    Some((
                        Ok::<Bytes, Infallible>(Bytes::from_static(
                            br#"{"model":"deepseek-chat"}"#,
                        )),
                        false,
                    ))
                } else {
                    None
                }
            }
        }))
    };
    let req_a = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(body_a)
        .unwrap();
    let app_a = app.clone();
    let handle_a = tokio::spawn(async move { send(&app_a, req_a).await });
    started_rx
        .recv()
        .await
        .expect("request A must reach the body read and hold the budget");

    // B: an immediate body that counts every chunk the gateway pulls. While A
    // holds the whole budget, B must not buffer a single byte.
    let bcount = Arc::new(AtomicUsize::new(0));
    let count = bcount.clone();
    let payload_b = Bytes::from_static(br#"{"model":"deepseek-chat","messages":[]}"#);
    let expected_b = payload_b.len();
    let stream_b =
        futures_util::stream::iter([Ok::<Bytes, Infallible>(payload_b)]).inspect_ok(move |chunk| {
            count.fetch_add(chunk.len(), Ordering::SeqCst);
        });
    let req_b = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from_stream(stream_b))
        .unwrap();
    let app_b = app.clone();
    let handle_b = tokio::spawn(async move { send(&app_b, req_b).await });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        bcount.load(Ordering::SeqCst),
        0,
        "the second body must not start accumulating memory while the first is being read"
    );

    // A's body completes but A still holds the complete `body_bytes`: it is
    // now parked on the gated upstream headers. B must STILL not read — the
    // budget covers the upstream-wait phase, not just the read phase.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), control.request_received.notified())
        .await
        .expect("the mock must receive A's full request");
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        bcount.load(Ordering::SeqCst),
        0,
        "B must stay blocked while A holds the budget waiting for upstream headers"
    );

    // A's upstream returns headers (and its response stream stays open); only
    // now is the budget released and B allowed to read.
    control.release.notify_one();
    let resp_a = handle_a.await.unwrap();
    assert_eq!(
        resp_a.status(),
        StatusCode::OK,
        "A completes once its upstream headers return"
    );
    let resp_b = handle_b.await.unwrap();
    assert_eq!(
        resp_b.status(),
        StatusCode::OK,
        "B proceeds once the budget is freed"
    );
    assert_eq!(
        bcount.load(Ordering::SeqCst),
        expected_b,
        "B's body is fully buffered after the budget freed"
    );
}

#[tokio::test]
async fn body_budget_covers_the_upstream_wait_but_not_the_response_stream() {
    use futures_util::TryStreamExt;
    use std::convert::Infallible;

    // A reads its whole body, then the upstream deliberately withholds response
    // headers. A still holds the complete `body_bytes` in memory, so with a
    // single-body budget B must not read a single byte while A waits. Only once
    // A's upstream returns headers — even while A's response stream is still
    // open — is the budget released and B allowed to read: the hard cap covers
    // the upstream-wait phase, not the downstream response phase.
    let budget = BodyBudget::new(1 << 20, 1 << 20);
    let p = pool(&["a"]);
    // Generous admission so A and B both pass; only the single-body budget
    // creates the contention.
    let q = queue(4, 4, Duration::from_secs(30));
    let sink = TestSink::default();
    let (addr, control) = start_gated_header_mock(
        br#"data: {"id":"a","choices":[]}

"#
        .to_vec(),
    )
    .await;
    let app = build_router(state_with_budget(
        &p,
        &q,
        &format!("http://{addr}"),
        sink,
        budget,
        1 << 20,
        IoTimeouts::default(),
    ));

    // A: a small, complete body that is fully read immediately.
    let req_a = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from(
            br#"{"model":"deepseek-chat","messages":[{"role":"user","content":"a"}]}"#.to_vec(),
        ))
        .unwrap();
    let app_a = app.clone();
    let handle_a = tokio::spawn(async move { send(&app_a, req_a).await });

    // Wait until the mock has A's full request: A's body is read and A is now
    // parked on the gated response headers (still holding the budget).
    tokio::time::timeout(Duration::from_secs(5), control.request_received.notified())
        .await
        .expect("the mock must receive A's full request");
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    // B: an immediate body counting every chunk the gateway pulls. While A is
    // waiting for upstream headers, B must not buffer a single byte.
    let bcount = Arc::new(AtomicUsize::new(0));
    let count = bcount.clone();
    let payload_b = Bytes::from_static(br#"{"model":"deepseek-chat","messages":[]}"#);
    let expected_b = payload_b.len();
    let stream_b =
        futures_util::stream::iter([Ok::<Bytes, Infallible>(payload_b)]).inspect_ok(move |chunk| {
            count.fetch_add(chunk.len(), Ordering::SeqCst);
        });
    let req_b = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from_stream(stream_b))
        .unwrap();
    let app_b = app.clone();
    let handle_b = tokio::spawn(async move { send(&app_b, req_b).await });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        bcount.load(Ordering::SeqCst),
        0,
        "B must not buffer while A holds the budget through the upstream header wait"
    );

    // A's upstream now returns headers + one SSE chunk, then stalls: A's
    // response stream is live. The budget must be released as soon as the
    // request is done (before the response streams), so B proceeds.
    control.release.notify_one();
    let resp_a = handle_a.await.unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK, "A's headers commit");
    let mut body_a = resp_a.into_body();
    let _e1 = next_data_frame(&mut body_a)
        .await
        .expect("A streams its first event");

    let resp_b = handle_b.await.unwrap();
    assert_eq!(
        resp_b.status(),
        StatusCode::OK,
        "B proceeds once the budget frees after A's headers"
    );
    assert_eq!(
        bcount.load(Ordering::SeqCst),
        expected_b,
        "B's body is fully buffered while A's response stream is still open"
    );
}

#[tokio::test(start_paused = true)]
async fn body_budget_is_released_on_too_large_body() {
    let budget = BodyBudget::new(1 << 20, 16);
    let p = pool(&["a"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with_budget(
        &p,
        &q,
        "http://127.0.0.1:1",
        sink,
        budget.clone(),
        16,
        IoTimeouts::default(),
    ));

    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .body(Body::from(vec![b'x'; 100]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        budget.available(),
        budget.capacity(),
        "the byte reservation must be released after a 413"
    );
}

#[tokio::test(start_paused = true)]
async fn body_budget_is_released_on_body_read_timeout() {
    use std::convert::Infallible;

    let budget = BodyBudget::new(1 << 20, 16);
    let p = pool(&["a"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let timeouts = IoTimeouts {
        body_read: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_budget(
        &p,
        &q,
        "http://127.0.0.1:1",
        sink,
        budget.clone(),
        16,
        timeouts,
    ));

    let stall = futures_util::stream::pending::<Result<Bytes, Infallible>>();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from_stream(stall))
        .unwrap();
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, req).await });
    for _ in 0..200 {
        tokio::task::yield_now().await;
        if budget.available() == (1 << 20) - 16 {
            break;
        }
    }
    assert_eq!(
        budget.available(),
        (1 << 20) - 16,
        "the stalled body must hold its byte reservation while reading"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "stalled body -> self-produced 503 under the shared body_read deadline"
    );
    assert_eq!(
        budget.available(),
        budget.capacity(),
        "the byte reservation must be released after the body-read timeout"
    );
}

#[tokio::test(start_paused = true)]
async fn body_budget_is_released_when_request_cancelled_mid_body() {
    use std::convert::Infallible;

    let budget = BodyBudget::new(1 << 20, 16);
    let p = pool(&["a"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with_budget(
        &p,
        &q,
        "http://127.0.0.1:1",
        sink,
        budget.clone(),
        16,
        IoTimeouts::default(),
    ));

    let stall = futures_util::stream::pending::<Result<Bytes, Infallible>>();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(Body::from_stream(stall))
        .unwrap();
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, req).await });
    for _ in 0..200 {
        tokio::task::yield_now().await;
        if budget.available() == (1 << 20) - 16 {
            break;
        }
    }
    assert_eq!(
        budget.available(),
        (1 << 20) - 16,
        "the in-flight body must hold its byte reservation"
    );
    handle.abort();
    let _ = handle.await;
    assert_eq!(
        budget.available(),
        budget.capacity(),
        "cancelling the request mid-body must release the byte reservation (RAII)"
    );
}

#[tokio::test]
async fn queue_full_maps_to_503() {
    let q = queue(1, 0, Duration::from_secs(30));
    let _hold = q.acquire().await.unwrap();
    let app = build_router(state("http://127.0.0.1:1", &["a"], q, TestSink::default()));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "queue Full -> 503"
    );
    assert!(resp.headers().contains_key("retry-after"));
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "service_unavailable");
}

#[tokio::test]
async fn queued_request_must_reauthenticate_after_token_rotation() {
    let q = queue(1, 1, Duration::from_secs(30));
    let hold = q.acquire().await.unwrap();
    let p = pool(&["key-1"]);
    let runtime = RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("token-old"),
        base_url: Url::parse("http://127.0.0.1:1").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });
    let app = build_router(GatewayState::with_runtime(
        upstream_client(),
        runtime.clone(),
        p,
        q.clone(),
        Arc::new(TestSink::default()),
        budget_for(1 << 20),
        IoTimeouts::default(),
    ));
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer token-old")
        .body(Body::from(r#"{"model":"deepseek-chat","messages":[]}"#))
        .unwrap();
    let handle = tokio::spawn(async move { send(&app, request).await });
    while q.snapshot().queued != 1 {
        tokio::task::yield_now().await;
    }

    runtime.update(RuntimeState {
        gateway_token: Secret::new("token-new"),
        base_url: Url::parse("http://127.0.0.1:1").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });
    drop(hold);

    assert_eq!(handle.await.unwrap().status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn queue_full_and_unauthenticated_are_each_audited_once() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer: Arc<AuditWriter> = Arc::new(AuditWriter::start(&path, 16).unwrap());

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let q = queue(1, 0, Duration::from_secs(30));
    let _hold = q.acquire().await.unwrap();
    let app = build_router(GatewayState::new(
        http,
        Url::parse("http://127.0.0.1:1").unwrap(),
        pool(&["a"]),
        q,
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::clone(&writer) as Arc<dyn AuditSink>,
        1 << 20,
        budget_for(1 << 20),
        IoTimeouts::default(),
    ));

    // Unauthenticated POST -> 401, audited exactly once with null model/key/usage.
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .body(Body::from(r#"{"model":"deepseek-chat","messages":[]}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Authenticated POST while the admission queue is full -> self-produced 503.
    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    drop(app);
    let writer = match Arc::try_unwrap(writer) {
        Ok(w) => w,
        Err(_) => panic!("router must be the only other sink holder"),
    };
    writer.shutdown().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let mut lines = content.lines();
    let unauth = lines
        .next()
        .expect("the unauthenticated 401 must be audited exactly once");
    let queue_full = lines
        .next()
        .expect("the authenticated queue-full request must be audited exactly once");
    assert_eq!(lines.next(), None, "exactly two audit lines");

    let v: serde_json::Value = serde_json::from_str(unauth).unwrap();
    assert_eq!(v["status"], 401);
    assert!(
        v["request_id"].is_string() && !v["request_id"].as_str().unwrap().is_empty(),
        "the 401 audit must carry a generated/validated request id: {unauth}"
    );
    assert!(
        v["model"].is_null(),
        "unknown model must be JSON null, not an empty string: {unauth}"
    );
    assert!(
        v["key_fingerprint"].is_null(),
        "unselected key must be JSON null, not an empty string: {unauth}"
    );
    assert!(
        v["input_tokens"].is_null(),
        "unknown usage must be null: {unauth}"
    );
    assert!(
        v["output_tokens"].is_null(),
        "unknown usage must be null: {unauth}"
    );

    let v: serde_json::Value = serde_json::from_str(queue_full).unwrap();
    assert_eq!(v["status"], 503);
    assert!(
        v["model"].is_null(),
        "unknown model must be JSON null, not an empty string: {queue_full}"
    );
    assert!(
        v["key_fingerprint"].is_null(),
        "unselected key must be JSON null, not an empty string: {queue_full}"
    );
    assert!(
        v["input_tokens"].is_null(),
        "unknown usage must be null: {queue_full}"
    );
    assert!(
        v["output_tokens"].is_null(),
        "unknown usage must be null: {queue_full}"
    );
    assert!(
        !content.contains("gway-token") && !content.contains("deepseek-chat"),
        "the audit lines must never contain the gateway token or the request body"
    );
}

#[tokio::test(start_paused = true)]
async fn queue_timeout_maps_to_503() {
    let q = queue(1, 1, Duration::from_secs(10));
    let _hold = q.acquire().await.unwrap();
    let app = build_router(state("http://127.0.0.1:1", &["a"], q, TestSink::default()));
    let app2 = app.clone();

    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "queue Timeout -> 503"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "every gateway-produced 503 must carry Retry-After"
    );
}

#[tokio::test]
async fn queue_closed_maps_to_503() {
    let q = queue(1, 1, Duration::from_secs(30));
    q.close();
    let app = build_router(state("http://127.0.0.1:1", &["a"], q, TestSink::default()));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "queue Closed -> 503"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "every gateway-produced 503 (including queue Closed) must carry Retry-After"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_request_body_times_out_and_releases_the_permit() {
    use futures_util::StreamExt;
    use std::convert::Infallible;

    let q = queue(2, 2, Duration::from_secs(30));
    let p = pool(&["a"]);
    let timeouts = IoTimeouts {
        body_read: Duration::from_secs(10),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        "http://127.0.0.1:1",
        TestSink::default(),
        timeouts,
    ));

    // An authenticated request whose body yields one partial chunk and then
    // stalls forever.
    let partial =
        futures_util::stream::iter([Ok::<Bytes, Infallible>(Bytes::from("{\"model\":\"x\""))]);
    let stall = futures_util::stream::pending::<Result<Bytes, Infallible>>();
    let body = Body::from_stream(partial.chain(stall));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer gway-token")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();

    let handle = tokio::spawn(app.clone().oneshot(req));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "the handler must respond within the configured body read timeout"
    );

    let resp = handle.await.unwrap().unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "stalled body -> self-produced 503"
    );
    assert!(resp.headers().contains_key("retry-after"));
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "service_unavailable");
    assert_eq!(
        q.snapshot().active,
        0,
        "permit must be released after the timeout"
    );
}

/// Mock upstream that accepts every connection but never writes a single byte:
/// the client is left waiting for response headers forever. Each accepted
/// connection is observed until the gateway cancels it (read hits EOF/error).
struct StallControl {
    accepts: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    all_closed: Arc<tokio::sync::Notify>,
}

async fn start_stalling_mock() -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            acceptor.accepts.fetch_add(1, Ordering::SeqCst);
            let per_conn = acceptor.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                per_conn.closed.fetch_add(1, Ordering::SeqCst);
                per_conn.all_closed.notify_one();
            });
        }
    });
    (addr, control)
}

/// Control for [`start_gated_header_mock`]: `request_received` fires once the
/// mock has read the first (A) request's full body, and `release` unblocks A's
/// response headers.
struct GatedHeaderControl {
    release: Arc<tokio::sync::Notify>,
    request_received: Arc<tokio::sync::Notify>,
}

/// Mock upstream that accepts any number of connections and reads each full
/// request (headers + `content-length` body). The FIRST connection — request A —
/// withholds response headers until `release`, then sends a 200 whose body is a
/// single SSE chunk followed by a permanent stall, so A's response stream stays
/// open while the upstream keeps the connection alive. Every later connection
/// is served an immediate 200 JSON. This pins request A in the "request body
/// read, still waiting for upstream headers" phase deterministically while
/// letting other requests complete.
async fn start_gated_header_mock(
    first_chunk: Vec<u8>,
) -> (std::net::SocketAddr, Arc<GatedHeaderControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(GatedHeaderControl {
        release: Arc::new(tokio::sync::Notify::new()),
        request_received: Arc::new(tokio::sync::Notify::new()),
    });
    let ctrl = control.clone();
    let first = std::sync::atomic::AtomicBool::new(true);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => return,
            };
            let is_first = first.swap(false, Ordering::SeqCst);
            let first_chunk = first_chunk.clone();
            let ctrl = ctrl.clone();
            tokio::spawn(async move {
                // Large enough to hold any test request body (16KiB) whole.
                let mut buf = [0u8; 65536];
                let mut n = 0;
                loop {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        return;
                    }
                    n += m;
                    if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                        break;
                    }
                }
                // Read the declared request body so reqwest's send completes.
                let headers_end = buf[..n]
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .map(|p| p + 4)
                    .unwrap_or(n);
                let head = std::str::from_utf8(&buf[..headers_end]).unwrap_or("");
                let content_length = head
                    .lines()
                    .find_map(|l| {
                        let mut it = l.splitn(2, ':');
                        let key = it.next()?.trim().to_ascii_lowercase();
                        let value = it.next()?.trim();
                        (key == "content-length")
                            .then(|| value.parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                while n < headers_end + content_length {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        return;
                    }
                    n += m;
                    if n >= buf.len() {
                        return;
                    }
                }
                if is_first {
                    // A: gate the response headers until the test releases it.
                    ctrl.request_received.notify_one();
                    ctrl.release.notified().await;
                    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
                    socket.write_all(head.as_bytes()).await.ok();
                    let size = first_chunk.len();
                    let mut piece = format!("{size:x}\r\n").into_bytes();
                    piece.extend_from_slice(&first_chunk);
                    piece.extend_from_slice(b"\r\n");
                    socket.write_all(&piece).await.ok();
                    socket.flush().await.ok();
                    // stall forever (A's response stream stays open).
                    let mut sink = [0u8; 1024];
                    loop {
                        match socket.read(&mut sink).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => continue,
                        }
                    }
                } else {
                    // Later requests (B): immediate 200 JSON.
                    let head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 28\r\n\r\n{\"id\":\"ok\",\"object\":\"chat.completion\"}";
                    socket.write_all(head.as_bytes()).await.ok();
                    socket.flush().await.ok();
                }
            });
        }
    });
    (addr, control)
}

async fn wait_until_closed(control: &Arc<StallControl>, n: usize) {
    for _ in 0..200 {
        if control.closed.load(Ordering::SeqCst) >= n {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("upstream connection(s) were never closed by the gateway");
}

#[tokio::test(start_paused = true)]
async fn upstream_header_timeout_cancels_attempt_and_fails_over_to_next_key() {
    let (addr, control) = start_stalling_mock().await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_header: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        timeouts,
    ));

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    // first attempt stalls on headers -> +5s timeout cancels it and fails over
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    // second attempt (key-2) also stalls -> +5s more -> exhausted
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "the handler must respond within the configured upstream header timeout"
    );

    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "both attempts exhausted -> self-produced 503"
    );
    assert!(resp.headers().contains_key("retry-after"));
    let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(v["error"]["type"], "service_unavailable");
    assert_eq!(q.snapshot().active, 0, "permit released after exhaustion");

    assert_eq!(
        control.accepts.load(Ordering::SeqCst),
        2,
        "both keys must be attempted: each cancelled attempt is a new upstream connection"
    );
    wait_until_closed(&control, 2).await;
}

#[tokio::test(start_paused = true)]
async fn upstream_timeouts_exhausted_are_audited_exactly_once_with_status_503() {
    // Both keys stall on response headers; the header timeout cancels each
    // attempt (a Network failure) and exhaustion yields a self-produced 503.
    // That 503 must be audited exactly once with null model/key/usage.
    let (addr, control) = start_stalling_mock().await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_header: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let sink = TestSink::default();
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        timeouts,
    ));

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "both attempts timed out -> handler responds"
    );

    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "network exhaustion -> self-produced 503"
    );
    assert!(resp.headers().contains_key("retry-after"));
    assert_eq!(q.snapshot().active, 0, "permit released after exhaustion");

    assert_rejected_audit(&sink, 1, 503);
    assert_eq!(
        control.accepts.load(Ordering::SeqCst),
        2,
        "both keys must be attempted"
    );
    wait_until_closed(&control, 2).await;
}

/// Mock upstream that, for every connection, reads the request, writes a
/// retryable HTTP error (`status`) with only `partial` of a `declared_len`
/// body (partial < 64KiB) and then stalls forever: the remaining bytes and
/// the EOF never arrive. Each connection is observed until the gateway
/// cancels it.
async fn start_partial_error_mock(
    status: u16,
    partial: Vec<u8>,
    declared_len: usize,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    start_partial_error_mock_with_retry_after(status, partial, declared_len, Some("15")).await
}

/// [`start_partial_error_mock`] with an explicit `Retry-After` header value
/// (`None` omits the header entirely), so tests can distinguish an honored
/// Retry-After from the default backoff.
async fn start_partial_error_mock_with_retry_after(
    status: u16,
    partial: Vec<u8>,
    declared_len: usize,
    retry_after: Option<&str>,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    let ra = retry_after
        .map(|r| format!("retry-after: {r}\r\n"))
        .unwrap_or_default();
    let reason = match status {
        429 => "Too Many Requests",
        401 => "Unauthorized",
        403 => "Forbidden",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {declared_len}\r\n{ra}\r\n"
    );
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            acceptor.accepts.fetch_add(1, Ordering::SeqCst);
            let per_conn = acceptor.clone();
            let partial = partial.clone();
            let head = head.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let mut n = 0;
                loop {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        break;
                    }
                    n += m;
                    if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                        break;
                    }
                }
                socket.write_all(head.as_bytes()).await.ok();
                socket.write_all(&partial).await.ok();
                let mut sink = [0u8; 1024];
                loop {
                    match socket.read(&mut sink).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                per_conn.closed.fetch_add(1, Ordering::SeqCst);
                per_conn.all_closed.notify_one();
            });
        }
    });
    (addr, control)
}

/// Mock upstream that, for every connection, reads the request, writes a
/// retryable HTTP error (`status`) with `Retry-After` (`None` omits it), sends
/// only `partial` of a `declared_len` body and then closes the connection. The
/// HTTP/1.1 client sees a body that ends before its declared length, so the
/// classification body stream yields an error rather than a clean EOF or a
/// stall.
async fn start_partial_error_close_mock(
    status: u16,
    partial: Vec<u8>,
    declared_len: usize,
    retry_after: Option<&str>,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    let ra = retry_after
        .map(|r| format!("retry-after: {r}\r\n"))
        .unwrap_or_default();
    let reason = match status {
        429 => "Too Many Requests",
        401 => "Unauthorized",
        403 => "Forbidden",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {declared_len}\r\n{ra}\r\n"
    );
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            acceptor.accepts.fetch_add(1, Ordering::SeqCst);
            let per_conn = acceptor.clone();
            let partial = partial.clone();
            let head = head.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let mut n = 0;
                loop {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        break;
                    }
                    n += m;
                    if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                        break;
                    }
                }
                socket.write_all(head.as_bytes()).await.ok();
                socket.write_all(&partial).await.ok();
                socket.flush().await.ok();
                // Close the write half: the body ends before its declared
                // length, so the client observes a read error, not clean EOF.
                socket.shutdown().await.ok();
                per_conn.closed.fetch_add(1, Ordering::SeqCst);
                per_conn.all_closed.notify_one();
            });
        }
    });
    (addr, control)
}

/// Mock upstream that, for every connection, writes a 200 with
/// `content-length: declared_len`, sends only `partial` bytes, then closes the
/// connection. The HTTP/1.1 client sees a body that ends before its declared
/// length, so its body stream yields an error — while the gateway has already
/// committed the 200 to the client.
async fn start_partial_then_close_mock(
    partial: Vec<u8>,
    declared_len: usize,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            acceptor.accepts.fetch_add(1, Ordering::SeqCst);
            let per_conn = acceptor.clone();
            let partial = partial.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let mut n = 0;
                loop {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        break;
                    }
                    n += m;
                    if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {declared_len}\r\n\r\n"
                );
                socket.write_all(head.as_bytes()).await.ok();
                socket.write_all(&partial).await.ok();
                socket.flush().await.ok();
                // Close the write half: the body ends before its declared
                // length, so the client observes a read error, not clean EOF.
                socket.shutdown().await.ok();
                per_conn.closed.fetch_add(1, Ordering::SeqCst);
                per_conn.all_closed.notify_one();
            });
        }
    });
    (addr, control)
}
/// A single-connection non-streaming (200) mock upstream whose body is split
/// into exactly two chunks: the first is sent immediately, the second is held
/// back until the test fires `control.release`. Until the release the upstream
/// has NOT finished, so a client can observe the first chunk before the body
/// ever ends. After the release the second chunk and the terminating chunked
/// EOF are sent; `finished` flips once the full body has been written.
struct TwoChunkControl {
    release: tokio::sync::mpsc::Sender<()>,
    finished: Arc<AtomicBool>,
}

async fn start_two_chunk_mock(
    first_chunk: Vec<u8>,
    second_chunk: Vec<u8>,
) -> (std::net::SocketAddr, Arc<TwoChunkControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (release, mut release_rx) = tokio::sync::mpsc::channel(1);
    let control = Arc::new(TwoChunkControl {
        release,
        finished: Arc::new(AtomicBool::new(false)),
    });
    let finished = control.finished.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8192];
        let mut n = 0;
        loop {
            let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
            if m == 0 {
                break;
            }
            n += m;
            if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                break;
            }
        }
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n",
            first_chunk.len()
        );
        socket.write_all(head.as_bytes()).await.ok();
        socket.write_all(&first_chunk).await.ok();
        socket.write_all(b"\r\n").await.ok();
        // gate the second chunk: the upstream has not finished until released
        let _ = release_rx.recv().await;
        let mut tail = format!("{:x}\r\n", second_chunk.len()).into_bytes();
        tail.extend_from_slice(&second_chunk);
        tail.extend_from_slice(b"\r\n0\r\n\r\n");
        socket.write_all(&tail).await.ok();
        socket.flush().await.ok();
        finished.store(true, Ordering::SeqCst);
    });
    (addr, control)
}

/// Mock upstream that, for every connection, writes a 200 whose body uses
/// `Transfer-Encoding: chunked`, sends exactly `first_chunk` as its first
/// chunk and then stalls forever: the terminating 0-chunk never arrives. The
/// response body is thus chunked AND non-terminating — a client can observe
/// the first chunk before the upstream ever ends. Each connection is observed
/// until the gateway cancels it.
async fn start_chunked_stalling_mock(
    first_chunk: Vec<u8>,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            acceptor.accepts.fetch_add(1, Ordering::SeqCst);
            let per_conn = acceptor.clone();
            let first_chunk = first_chunk.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let mut n = 0;
                loop {
                    let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
                    if m == 0 {
                        break;
                    }
                    n += m;
                    if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n",
                    first_chunk.len()
                );
                socket.write_all(head.as_bytes()).await.ok();
                socket.write_all(&first_chunk).await.ok();
                socket.write_all(b"\r\n").await.ok();
                // stall forever (the terminating 0-chunk never arrives) and
                // observe when the gateway closes the connection.
                let mut sink = [0u8; 1024];
                loop {
                    match socket.read(&mut sink).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                per_conn.closed.fetch_add(1, Ordering::SeqCst);
                per_conn.all_closed.notify_one();
            });
        }
    });
    (addr, control)
}

/// Mock upstream that, for every connection, writes a 200 whose body is
/// `n_chunks` chunked chunks (each large enough that the gateway forwards them
/// as separate sends) and then stalls forever: the terminating 0-chunk never
/// arrives. A client that stops reading lets the gateway's 16-slot response
/// channel fill up, parking the pump on `tx.send`. Every connection is observed
/// until the gateway cancels it.
async fn start_backpressure_mock(
    n_chunks: usize,
    content_type: &'static str,
) -> (std::net::SocketAddr, Arc<StallControl>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(StallControl {
        accepts: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        all_closed: Arc::new(tokio::sync::Notify::new()),
    });
    let acceptor = control.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        acceptor.accepts.fetch_add(1, Ordering::SeqCst);
        let per_conn = acceptor.clone();
        let mut buf = [0u8; 8192];
        let mut n = 0;
        loop {
            let m = socket.read(&mut buf[n..]).await.unwrap_or(0);
            if m == 0 {
                break;
            }
            n += m;
            if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || n >= buf.len() {
                break;
            }
        }
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ntransfer-encoding: chunked\r\n\r\n"
        );
        socket.write_all(head.as_bytes()).await.ok();
        let chunk = if content_type == "text/event-stream" {
            let mut event = b"data: ".to_vec();
            event.extend(std::iter::repeat_n(b'x', 8192 - event.len() - 2));
            event.extend_from_slice(b"\n\n");
            event
        } else {
            vec![b'x'; 8192]
        };
        for _ in 0..n_chunks {
            let mut piece = format!("{:x}\r\n", chunk.len()).into_bytes();
            piece.extend_from_slice(&chunk);
            piece.extend_from_slice(b"\r\n");
            socket.write_all(&piece).await.ok();
        }
        socket.flush().await.ok();
        // stall forever (the terminating 0-chunk never arrives) and observe
        // when the gateway closes the connection.
        let mut sink = [0u8; 1024];
        loop {
            match socket.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        per_conn.closed.fetch_add(1, Ordering::SeqCst);
        per_conn.all_closed.notify_one();
    });
    (addr, control)
}

#[tokio::test(start_paused = true)]
async fn stalled_error_body_times_out_and_degrades_without_leaking() {
    // A retryable upstream error whose body embeds a secret marker, declared
    // longer than the bytes actually sent: headers + a <64KiB partial body,
    // then the upstream stalls. The classification buffering must be bounded
    // so the permit is released, the partial body never leaks, and both keys
    // are tried per the existing max-2 strategy.
    let partial =
        br#"{"error":{"type":"GoUsageLimitError","message":"Weekly usage limit reached. Resets in 3 days.","secret":"LEAKME"}}"#
            .to_vec();
    for status in [429u16, 401, 403, 500] {
        let (addr, control) =
            start_partial_error_mock(status, partial.clone(), partial.len() + 512).await;
        let p = pool(&["key-1", "key-2"]);
        let q = queue(2, 2, Duration::from_secs(30));
        let timeouts = IoTimeouts {
            upstream_error_body: Duration::from_secs(5),
            ..IoTimeouts::default()
        };
        let app = build_router(state_with_timeouts(
            &p,
            &q,
            &format!("http://{addr}"),
            TestSink::default(),
            timeouts,
        ));

        let app2 = app.clone();
        let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
        // Drive the handler under a paused clock: each attempt needs the mock's
        // headers + partial body to land over the loopback socket (real IO)
        // before its error-body timeout can be armed and fire. Interleave
        // yields with small clock advances until the handler converges.
        let mut elapsed = Duration::ZERO;
        while !handle.is_finished() && elapsed < Duration::from_secs(30) {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(500)).await;
            elapsed += Duration::from_millis(500);
        }
        assert!(
            handle.is_finished(),
            "{status}: handler must respond within the configured error body timeout"
        );

        let resp = handle.await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{status}: stalled error bodies -> self-produced 503"
        );
        assert_eq!(
            resp.headers().get("retry-after").unwrap(),
            "1",
            "{status}: self-produced 503 carries its own Retry-After, not the upstream's 15"
        );
        let body = body_string(resp).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"]["type"], "service_unavailable");
        assert!(
            !body.contains("LEAKME"),
            "{status}: a partial error body must never leak"
        );
        assert_eq!(q.snapshot().active, 0, "{status}: permit released");
        assert_eq!(
            control.accepts.load(Ordering::SeqCst),
            2,
            "{status}: both keys must be attempted"
        );
        wait_until_closed(&control, 2).await;
    }
}

#[tokio::test(start_paused = true)]
async fn committed_5xx_body_stall_does_not_advance_the_circuit_breaker() {
    // The upstream commits an HTTP 500 (the response head is delivered) and then
    // stalls mid-error-body. The head is already committed, so a body-read
    // failure is NOT a pre-status network failure: it must not count toward the
    // consecutive-failure circuit breaker, even past the threshold. Each request
    // may retry key-1 then key-2, but the breaker stays closed and key-1 stays
    // selectable afterwards.
    let partial = br#"{"error":{"message":"committed then stalled"}}"#.to_vec();
    let (addr, control) = start_partial_error_mock(500, partial.clone(), partial.len() + 512).await;
    let p = pool_with_breaker(&["key-1", "key-2"], 2);
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_error_body: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        timeouts,
    ));

    // Two requests x two keys = two committed-5xx body stalls per key: past
    // breaker_threshold 2. A pre-status network failure would trip by now.
    for _ in 0..2 {
        let app2 = app.clone();
        let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
        let mut elapsed = Duration::ZERO;
        while !handle.is_finished() && elapsed < Duration::from_secs(30) {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(500)).await;
            elapsed += Duration::from_millis(500);
        }
        assert!(
            handle.is_finished(),
            "the handler must respond within the configured error body timeout"
        );
        let resp = handle.await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "stalled error bodies on every key -> self-produced 503"
        );
    }
    wait_until_closed(&control, 4).await;

    assert!(
        p.has_available_key(),
        "committed 5xx body stalls must not trip the circuit breaker even past the threshold"
    );

    // A fresh request against a healthy upstream must still start with key-1
    // (fill-first, never cooled).
    let healthy = Arc::new(MockControl::default());
    let addr2 = start_mock(healthy.clone()).await;
    healthy
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));
    let app2 = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr2}"),
        TestSink::default(),
    ));
    let resp = send_io(&app2, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the fresh request to the healthy upstream must be served"
    );
    let captured = healthy.requests.lock().unwrap();
    assert_eq!(
        captured[0].headers.get("authorization").unwrap(),
        "Bearer key-1",
        "a committed 5xx body stall must not cool or disable key-1"
    );
}

#[tokio::test(start_paused = true)]
async fn pre_status_header_stall_still_advances_the_circuit_breaker() {
    // A failure BEFORE any status is committed (response headers never arrive)
    // is a genuine network failure: it must still trip the circuit breaker past
    // the threshold. Guards against reclassifying pre-status failures alongside
    // committed-5xx body stalls.
    let (addr, control) = start_stalling_mock().await;
    let p = pool_with_breaker(&["key-1", "key-2"], 2);
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_header: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        timeouts,
    ));

    for _ in 0..2 {
        let app2 = app.clone();
        let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(
            handle.is_finished(),
            "the handler must respond within the configured upstream header timeout"
        );
        let resp = handle.await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "header-stalled attempts exhaust -> self-produced 503"
        );
    }
    wait_until_closed(&control, 4).await;

    assert!(
        !p.has_available_key(),
        "pre-status network failures must still trip the circuit breaker past the threshold"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_429_body_is_rate_limited_with_honored_retry_after_and_no_breaker() {
    // The upstream commits a 429 and then stalls mid-error-body. The status is
    // already committed, so the classification body stall is NOT a pre-status
    // network failure: it must cool the key as RateLimited honoring the parsed
    // Retry-After (15s), never advancing the consecutive-failure breaker
    // (threshold 1 would trip on Network).
    let partial = br#"{"error":{"message":"stalled 429"}}"#.to_vec();
    let (addr, control) = start_partial_error_mock_with_retry_after(
        429,
        partial.clone(),
        partial.len() + 512,
        Some("15"),
    )
    .await;
    let p = pool_with_timeout_breaker(&["a"], 1, Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_error_body: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        timeouts,
    ));

    // Drive the handler under a paused clock: the mock's headers + partial
    // body must land over the loopback socket (real IO) before the 5s error
    // body timeout is armed and fires.
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let mut elapsed = Duration::ZERO;
    while !handle.is_finished() && elapsed < Duration::from_secs(30) {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        elapsed += Duration::from_millis(500);
    }
    assert!(
        handle.is_finished(),
        "the handler must respond within the configured error body timeout"
    );
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "single key: the stalled 429 exhausts attempts -> self-produced 503"
    );
    wait_until_closed(&control, 1).await;

    // RateLimited with retry_after=15: the key cools for 15s, not the 5s
    // default backoff, and recovers cleanly — the breaker was never touched.
    let mut req = p.request();
    let probe = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        !probe.is_finished(),
        "the honored 15s Retry-After must win over the 5s default backoff"
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let result = probe.await.unwrap();
    assert!(
        matches!(result, orihsus::pool::AttemptResult::Selected(_)),
        "the key recovers after the honored Retry-After"
    );
    assert!(
        p.has_available_key(),
        "a stalled 429 error body must cool as RateLimited, never trip the breaker"
    );
}

#[tokio::test(start_paused = true)]
async fn errored_429_body_is_rate_limited_with_normal_backoff_and_no_breaker() {
    // The upstream commits a 429 (no Retry-After) whose error body ends before
    // its declared length and the connection closes: the classification body
    // stream yields an ERROR, not clean EOF. The committed status still makes
    // this a rate-limit — the key cools for the normal 5s backoff and never
    // advances the breaker (threshold 1 would trip on Network).
    let partial = br#"{"error":{"message":"truncated 429"}}"#.to_vec();
    let (addr, _control) =
        start_partial_error_close_mock(429, partial.clone(), partial.len() + 512, None).await;
    let p = pool_with_timeout_breaker(&["a"], 1, Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let timeouts = IoTimeouts {
        upstream_error_body: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
        timeouts,
    ));

    // The body error is immediate (the mock closes the connection), so the
    // handler converges with yields alone; a spare clock advance guards a
    // race where the timer is armed before the error lands.
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let mut elapsed = Duration::ZERO;
    while !handle.is_finished() && elapsed < Duration::from_secs(30) {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
    }
    assert!(
        handle.is_finished(),
        "the handler must respond once the truncated error body is read"
    );
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "single key: the errored 429 exhausts attempts -> self-produced 503"
    );

    // RateLimited with no Retry-After: normal exponential backoff (5s at step 0).
    let mut req = p.request();
    let probe = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        !probe.is_finished(),
        "still cooling at 4s: the normal 5s backoff must apply"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    let result = probe.await.unwrap();
    assert!(
        matches!(result, orihsus::pool::AttemptResult::Selected(_)),
        "the key recovers after the normal backoff"
    );
    assert!(
        p.has_available_key(),
        "a committed 429 body error must cool as RateLimited, never trip the breaker"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_auth_error_body_is_unavailable_with_no_breaker() {
    // A committed 401/403 whose error body stalls mid-read is an Unavailable
    // key, never a pre-status network failure: the key cools for the honored
    // Retry-After (15s) and the breaker (threshold 1) must stay closed.
    let partial = br#"{"error":{"message":"stalled auth"}}"#.to_vec();
    for status in [401u16, 403] {
        let (addr, control) =
            start_partial_error_mock(status, partial.clone(), partial.len() + 512).await;
        let p = pool_with_timeout_breaker(&["a"], 1, Duration::from_secs(30));
        let q = queue(2, 2, Duration::from_secs(30));
        let timeouts = IoTimeouts {
            upstream_error_body: Duration::from_secs(5),
            ..IoTimeouts::default()
        };
        let app = build_router(state_with_timeouts(
            &p,
            &q,
            &format!("http://{addr}"),
            TestSink::default(),
            timeouts,
        ));

        let app2 = app.clone();
        let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
        let mut elapsed = Duration::ZERO;
        while !handle.is_finished() && elapsed < Duration::from_secs(30) {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(500)).await;
            elapsed += Duration::from_millis(500);
        }
        assert!(
            handle.is_finished(),
            "{status}: the handler must respond within the error body timeout"
        );
        let resp = handle.await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{status}: single key -> self-produced 503"
        );
        wait_until_closed(&control, 1).await;

        // Unavailable honoring the 15s Retry-After; breaker untouched.
        let mut req = p.request();
        let probe = tokio::spawn(async move { req.next().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(
            !probe.is_finished(),
            "{status}: the honored 15s Retry-After must win over the 5s default backoff"
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        let result = probe.await.unwrap();
        assert!(
            matches!(result, orihsus::pool::AttemptResult::Selected(_)),
            "{status}: the key recovers after the honored Retry-After"
        );
        assert!(
            p.has_available_key(),
            "{status}: a stalled auth error body must cool as Unavailable, never trip the breaker"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pool_unavailable_maps_to_429_with_retry_after() {
    use orihsus::pool::Failure;

    let p = pool_with_timeout(&["a"], Duration::from_secs(2));
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );

    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        "http://127.0.0.1:1",
        TestSink::default(),
    ));
    let app2 = app.clone();

    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "pool Unavailable -> 429"
    );
    assert_eq!(
        resp.headers().get("retry-after").unwrap(),
        "43",
        "Retry-After = 45s cooldown minus the 2s already waited, ceil"
    );
}

#[tokio::test(start_paused = true)]
async fn pool_unavailable_is_audited_exactly_once_with_status_429() {
    use orihsus::pool::Failure;

    let p = pool_with_timeout(&["a"], Duration::from_secs(2));
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );

    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, "http://127.0.0.1:1", sink.clone()));
    let app2 = app.clone();

    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let resp = handle.await.unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        resp.headers().get("retry-after").unwrap(),
        "43",
        "Retry-After = 45s cooldown minus the 2s already waited, ceil"
    );

    assert_rejected_audit(&sink, 1, 429);
    assert_eq!(
        q.snapshot().active,
        0,
        "permit released on pool Unavailable"
    );
}

#[tokio::test]
async fn sse_streams_events_before_upstream_ends_and_records_usage() {
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    let control = Arc::new(MockControl::default());
    control.sse.lock().unwrap().replace(SseControl {
        event1: br#"data: {"id":"1","choices":[{"delta":{"content":"a"}}]}

"#
        .to_vec(),
        event2: br#"data: {"id":"1","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":7}}

data: [DONE]

"#
        .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let mut body = resp.into_body();
    let mut acc = String::new();
    loop {
        let f = next_data_frame(&mut body)
            .await
            .expect("stream must yield the first event");
        acc.push_str(&String::from_utf8_lossy(&f));
        if acc.contains("\"content\":\"a\"") {
            break;
        }
    }
    assert!(acc.contains("data:"), "SSE framing preserved: {acc:?}");
    assert!(acc.contains("\"delta\""));

    gate_tx.send(()).await.unwrap();
    let mut acc2 = String::new();
    loop {
        let f = next_data_frame(&mut body)
            .await
            .expect("stream must yield the final event");
        acc2.push_str(&String::from_utf8_lossy(&f));
        if acc2.contains("[DONE]") {
            break;
        }
    }
    assert!(
        acc2.contains("\"usage\""),
        "final usage event forwarded: {acc2:?}"
    );
    assert!(
        next_data_frame(&mut body).await.is_none(),
        "stream ends cleanly"
    );

    tokio::task::yield_now().await;
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].input_tokens,
        Some(5),
        "usage extracted from the SSE stream"
    );
    assert_eq!(records[0].output_tokens, Some(7));
    assert_eq!(records[0].status, 200);
}

#[tokio::test]
async fn sse_streams_have_a_separate_concurrency_limit() {
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    let control = Arc::new(MockControl::default());
    control.sse.lock().unwrap().replace(SseControl {
        event1: b"data: first\n\n".to_vec(),
        event2: b"data: [DONE]\n\n".to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 200,
        content_type: "text/event-stream",
        body: b"data: second\n\n".to_vec(),
        extra_headers: vec![],
    });
    let addr = start_mock(control).await;
    // The streaming limit is one quarter of total admission, with a floor of 1.
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1"],
        queue(4, 4, Duration::from_secs(30)),
        TestSink::default(),
    ));

    let first = send(&app, chat_req()).await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = send(&app, chat_req()).await;
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(second.headers().get("retry-after").unwrap(), "1");

    drop(first);
    let _ = gate_tx.send(()).await;
}

#[tokio::test]
async fn client_dropping_the_body_cancels_upstream_and_releases_permit() {
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_notify = Arc::new(tokio::sync::Notify::new());
    let control = Arc::new(MockControl::default());
    control.sse.lock().unwrap().replace(SseControl {
        event1: br#"data: {"id":"1","choices":[{"delta":{"content":"a"}}]}

"#
        .to_vec(),
        event2: br#"data: {"id":"1","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}

data: [DONE]

"#
        .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: cancelled.clone(),
        cancel_notify: cancel_notify.clone(),
    });
    let addr = start_mock(control.clone()).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink,
    ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _f1 = next_data_frame(&mut body).await.expect("first event");
    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the stream is live"
    );

    drop(body);
    tokio::task::yield_now().await;

    gate_tx.send(()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), cancel_notify.notified())
        .await
        .expect("upstream must observe the connection being cancelled");
    assert!(
        cancelled.load(Ordering::SeqCst),
        "mock saw the connection close"
    );

    assert_eq!(
        q.snapshot().active,
        0,
        "queue permit released when the client disconnects"
    );
}

#[tokio::test]
async fn committed_200_with_upstream_body_error_fails_the_key_and_audits_upstream_error() {
    use orihsus::pool::{KeyPool, NoJitter, PoolPolicy};
    // breaker_threshold=1: a single report_failure(Network) trips the breaker,
    // leaving the key unavailable. The buggy early-commit path called
    // report_success, which would leave it selectable.
    let p = Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("a")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 1,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(30),
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    );
    assert!(p.has_available_key(), "key starts selectable");

    // A 200 whose body ends before its declared length: the gateway commits
    // the 200/partial to the client, then the upstream body stream errors.
    let partial = br#"{"id":"cmpl-partial","object":"chat.completion","choices":[]"#.to_vec();
    let (addr, control) = start_partial_then_close_mock(partial.clone(), partial.len() + 512).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink.clone()));

    let resp = tokio::time::timeout(Duration::from_secs(2), send(&app, chat_req()))
        .await
        .expect("the 200 response must be committed promptly, before the body error is known");
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_body();
    let first = tokio::time::timeout(Duration::from_secs(2), next_data_frame(&mut body))
        .await
        .expect("the committed partial body must be forwarded")
        .expect("a data frame");
    assert_eq!(first.as_ref(), partial.as_slice(), "partial forwarded");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), next_data_frame(&mut body))
            .await
            .expect("the stream must end, not hang")
            .is_none(),
        "the client sees committed 200/partial then EOF — headers were committed, no retry"
    );

    tokio::task::yield_now().await;
    assert_eq!(
        q.snapshot().active,
        0,
        "permit released after the body error"
    );
    assert_eq!(
        control.accepts.load(Ordering::SeqCst),
        1,
        "no retry: headers/body were committed, a second key must not be attempted"
    );

    // The upstream body error must be reported as a network failure, NOT as a
    // success: the breaker (threshold 1) leaves the key unavailable.
    assert!(
        !p.has_available_key(),
        "the key must be breaker-cooled by report_failure(Network), not reset by report_success"
    );

    // Exactly one audit line: status stays the actual 200, outcome=upstream_error,
    // usage empty (the stream was truncated).
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1, "exactly one audit record");
    assert_eq!(records[0].status, 200);
    assert_eq!(records[0].outcome, Some(AuditOutcome::UpstreamError));
    assert_eq!(
        records[0].input_tokens, None,
        "usage must be empty for an upstream body error"
    );
    assert_eq!(records[0].output_tokens, None);
}

#[tokio::test(start_paused = true)]
async fn client_cancelled_success_audits_client_cancel_without_pool_feedback() {
    use orihsus::pool::Failure;
    // breaker_threshold=1: any (wrong) report_failure(Network) would trip the
    // breaker; a (wrong) report_success would reset the escalated backoff.
    let p = Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("a")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 1,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(30),
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    );
    let (_gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    let control = Arc::new(MockControl::default());
    control.sse.lock().unwrap().replace(SseControl {
        event1: br#"data: {"id":"1","choices":[{"delta":{"content":"a"}}]}

"#
        .to_vec(),
        event2: br#"data: {"id":"1","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}

data: [DONE]

"#
        .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    let addr = start_mock(control.clone()).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink.clone()));

    // Escalate the backoff to step 1 so a (wrong) report_success would be
    // observable: a client cancel must leave the step untouched.
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(&sel, Failure::RateLimited { retry_after: None });
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(p.has_available_key(), "key recovered after the backoff");

    // The client reads the first SSE event, then drops the response body while
    // the upstream is still streaming (the second event is gated).
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _e1 = next_data_frame(&mut body).await.expect("first SSE event");
    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the stream is live"
    );
    drop(body);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert_eq!(q.snapshot().active, 0, "permit released on client drop");
    assert!(
        p.has_available_key(),
        "client cancel must NOT trip the breaker: no report_failure was made"
    );

    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1, "exactly one audit record");
        assert_eq!(records[0].status, 200);
        assert_eq!(records[0].outcome, Some(AuditOutcome::ClientCancel));
        assert_eq!(
            records[0].input_tokens, None,
            "usage must be empty for a cancelled stream"
        );
        assert_eq!(records[0].output_tokens, None);
    }

    // And the backoff step must survive (no report_success): a fresh generic
    // 429 now cools for backoff_at(step=1) = 10s, not the reset 5s.
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(&sel, Failure::RateLimited { retry_after: None });
    let p2 = Arc::clone(&p);
    let probe = tokio::spawn(async move {
        let mut fresh = p2.request();
        fresh.next().await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        !probe.is_finished(),
        "client cancel must NOT reset the backoff step: cooldown is 10s, not 5s"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        probe.is_finished(),
        "the preserved 10s cooldown ends at +10s"
    );
}

#[tokio::test(start_paused = true)]
async fn completed_success_audits_completed_and_resets_pool_backoff() {
    use orihsus::pool::Failure;

    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(&p, &q, &format!("http://{addr}"), sink.clone()));

    // Escalate the backoff to step 1 so the completed success must RESET it.
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(&sel, Failure::RateLimited { retry_after: None });
    tokio::time::advance(Duration::from_secs(5)).await;

    control.responses.lock().unwrap().push_back(MockResponse::json(
        200,
        br#"{"id":"cmpl-ok","object":"chat.completion","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}"#,
    ));
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    while next_data_frame(&mut body).await.is_some() {}
    tokio::task::yield_now().await;

    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1, "exactly one audit record");
        assert_eq!(records[0].status, 200);
        assert_eq!(records[0].outcome, Some(AuditOutcome::Completed));
        assert_eq!(records[0].input_tokens, Some(3));
        assert_eq!(records[0].output_tokens, Some(4));
    }

    // Only report_success was called: a fresh generic 429 cools for the
    // initial 5s, not the escalated 10s.
    let mut req = p.request();
    let sel = match req.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    p.report_failure(&sel, Failure::RateLimited { retry_after: None });
    let p2 = Arc::clone(&p);
    let probe = tokio::spawn(async move {
        let mut fresh = p2.request();
        fresh.next().await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        probe.is_finished(),
        "completed success must reset the backoff: cooldown is 5s, not the escalated 10s"
    );
}

#[tokio::test]
async fn sse_usage_with_crlf_split_across_chunks_is_extracted() {
    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    let control = Arc::new(MockControl::default());
    control.sse.lock().unwrap().replace(SseControl {
        event1: b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\r\n\r\n".to_vec(),
        // the usage JSON is split mid-token across two chunks, using CRLF delimiters
        event2: br#"data: {"id":"1","usage":{"prompt_tokens":9,"completion_tokens":3"#.to_vec(),
        event2b: Some(b"}}\r\n\r\ndata: [DONE]\r\n\r\n".to_vec()),
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _e1 = next_data_frame(&mut body).await;
    gate_tx.send(()).await.unwrap();
    while next_data_frame(&mut body).await.is_some() {}
    tokio::task::yield_now().await;

    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].input_tokens,
        Some(9),
        "usage must be extracted from a CRLF event split across chunks"
    );
    assert_eq!(records[0].output_tokens, Some(3));
}

#[tokio::test(start_paused = true)]
async fn fractional_retry_after_is_treated_as_absent() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: b"{}".to_vec(),
        extra_headers: vec![("retry-after".to_string(), "1.5".to_string())],
    });

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    // delta-seconds must be a non-negative integer; "1.5" is invalid -> backoff (5s)
    let mut req = p.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(1500)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "fractional Retry-After (1.5) must be invalid -> 5s backoff, not 1.5s"
    );
    tokio::time::advance(Duration::from_millis(3500)).await;
    tokio::task::yield_now().await;
    assert!(handle.is_finished(), "recovered after the 5s backoff");
    assert!(
        matches!(
            handle.await.unwrap(),
            orihsus::pool::AttemptResult::Selected(_)
        ),
        "key selectable again"
    );
}

#[tokio::test]
async fn redirects_are_passed_through_not_followed() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 302,
        content_type: "text/plain",
        body: b"redirecting".to_vec(),
        extra_headers: vec![(
            "location".to_string(),
            "https://evil.example.com/x".to_string(),
        )],
    });

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::FOUND,
        "3xx passed through, never followed"
    );
    assert_eq!(
        resp.headers().get("location").unwrap(),
        "https://evil.example.com/x",
        "Location header forwarded as-is"
    );
    assert_eq!(body_string(resp).await, "redirecting");

    assert_eq!(sink.0.lock().unwrap().len(), 1, "redirect still audited");
}

#[tokio::test]
async fn gateway_errors_never_leak_tokens_or_keys() {
    let app = build_router(state(
        "http://127.0.0.1:1",
        &["sk-upstream-super-secret"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));

    let resp = send(
        &app,
        Request::builder()
            .uri("/v1/models")
            .header("authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(resp).await;
    assert!(
        !body.contains("gway-token"),
        "gateway token must not appear in the 401 body"
    );
    assert!(
        !body.contains("sk-upstream-super-secret"),
        "upstream key must not appear in the 401 body"
    );

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(
        !body.contains("gway-token"),
        "gateway token must not appear in the 503 body"
    );
    assert!(
        !body.contains("sk-upstream-super-secret"),
        "upstream key must not appear in the 503 body"
    );
}

#[tokio::test]
async fn invalid_request_id_is_replaced_and_never_injected() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"x","object":"chat.completion"}"#,
        ));

    let evil = "bad id with \u{2000} space";
    let resp = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("authorization", "Bearer gway-token")
            .header("x-request-id", evil)
            .body(Body::from(r#"{"model":"deepseek-chat","messages":[]}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let echoed = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_ne!(echoed, evil, "invalid request id must be replaced");
    assert!(
        echoed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
        "generated id uses a safe charset: {echoed:?}"
    );

    // The audit record is now written by the streaming task once the body is
    // consumed, so drain the body to EOF before asserting on the sink.
    let mut body = resp.into_body();
    while next_data_frame(&mut body).await.is_some() {}
    for _ in 0..200 {
        if !sink.0.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    let records = sink.0.lock().unwrap();
    assert_eq!(
        records[0].request_id, echoed,
        "audit uses the same request id as the response"
    );
}

#[tokio::test]
async fn final_upstream_error_is_sanitized_without_leaking_metadata() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1", "key-2"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: br#"{"error":{"message":"first-429"}}"#.to_vec(),
        extra_headers: vec![("x-first".to_string(), "1".to_string())],
    });
    control.responses.lock().unwrap().push_back(MockResponse {
        status: 429,
        content_type: "application/json",
        body: br#"{"error":{"message":"second-429"},"metadata":{"workspace":"wrk_SECRET"}}"#
            .to_vec(),
        extra_headers: vec![("x-second".to_string(), "2".to_string())],
    });

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "final upstream 429 keeps its rate-limit status"
    );
    assert!(
        !resp.headers().contains_key("x-first") && !resp.headers().contains_key("x-second"),
        "untrusted upstream headers must not pass through"
    );
    let body = body_string(resp).await;
    assert!(
        !body.contains("second-429") && !body.contains("wrk_SECRET"),
        "upstream error body and workspace metadata must not leak"
    );
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["error"]["type"], "rate_limit_error");

    {
        let captured = control.requests.lock().unwrap();
        assert_eq!(captured.len(), 2, "two distinct keys attempted");
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-1"
        );
        assert_eq!(
            captured[1].headers.get("authorization").unwrap(),
            "Bearer key-2"
        );
    }
    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1, "final error is still audited");
        assert_eq!(records[0].input_tokens, None, "usage null for errors");
        assert_eq!(records[0].output_tokens, None);
        assert_eq!(records[0].status, 429);
        assert_eq!(
            records[0].key_fingerprint.as_deref(),
            Some(fingerprint("key-2").as_str())
        );
    }
}

#[tokio::test]
async fn final_server_error_is_sanitized_when_both_keys_fail() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1", "key-2"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            500,
            br#"{"error":{"message":"boom-1"}}"#,
        ));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            500,
            br#"{"error":{"message":"boom-2"}}"#,
        ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "final upstream status is preserved"
    );
    let body = body_string(resp).await;
    assert!(!body.contains("boom-1") && !body.contains("boom-2"));
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["error"]["type"], "upstream_error");
}

#[tokio::test]
async fn server_error_is_returned_when_the_only_other_key_is_already_cooling() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["key-1", "key-2"], Duration::from_millis(20));
    let mut cooling = p.request();
    let key1 = match cooling.next().await {
        orihsus::pool::AttemptResult::Selected(selection) => selection,
        other => panic!("expected key1 selection, got {other:?}"),
    };
    p.report_failure(
        &key1,
        orihsus::pool::Failure::UsageLimit {
            dimension: orihsus::pool::UsageDimension::Weekly,
            cooldown: Duration::from_secs(60),
        },
    );

    let sink = TestSink::default();
    let app = build_router(state_with(
        &p,
        &queue(2, 2, Duration::from_secs(30)),
        &format!("http://{addr}"),
        sink.clone(),
    ));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            503,
            br#"{"error":{"message":"Endpoint unavailable"}}"#,
        ));

    let resp = send(&app, chat_req()).await;

    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a committed upstream 503 must win over an unrelated key cooldown"
    );
    let body = body_string(resp).await;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["error"]["type"], "upstream_error");
    assert!(!body.contains("Endpoint unavailable"));
    {
        let requests = control.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers.get("authorization").unwrap(),
            "Bearer key-2"
        );
    }
    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, 503);
    }

    let mut probe = p.request();
    let selected = match probe.next().await {
        orihsus::pool::AttemptResult::Selected(selection) => selection,
        other => panic!("503 must leave key2 selectable, got {other:?}"),
    };
    assert_eq!(selected.fingerprint(), fingerprint("key-2"));
}

#[tokio::test(start_paused = true)]
async fn non_streaming_success_resets_pool_backoff() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));

    let push = |status: u16| {
        control
            .responses
            .lock()
            .unwrap()
            .push_back(MockResponse::json(status, br#"{}"#))
    };

    push(429);
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "1st 429 (backoff 5s)"
    );
    tokio::time::advance(Duration::from_secs(5)).await;

    push(429);
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "2nd 429 (backoff escalates to 10s)"
    );
    tokio::time::advance(Duration::from_secs(10)).await;

    push(200);
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "success must reset the backoff"
    );

    push(429);
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS, "3rd 429");

    // The key must now cool for the RESET initial backoff (5s), not the escalated one.
    // Probe with the pool's public behavior only (no new probe API).
    let mut req = p.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "still cooling at 4s: backoff must be back to the initial 5s after success"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "must recover at the reset 5s backoff (not the escalated one)"
    );
    assert!(
        matches!(
            handle.await.unwrap(),
            orihsus::pool::AttemptResult::Selected(_)
        ),
        "key selectable again after the reset backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn sse_success_resets_pool_backoff() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, br#"{}"#));
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    tokio::time::advance(Duration::from_secs(5)).await;

    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    control.sse.lock().unwrap().replace(SseControl {
        event1: br#"data: {"choices":[{"delta":{"content":"a"}}]}

"#
        .to_vec(),
        event2: br#"data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}

data: [DONE]

"#
        .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
    let mut body = resp.into_body();
    let _e1 = next_data_frame(&mut body).await;
    gate_tx.send(()).await.unwrap();
    while next_data_frame(&mut body).await.is_some() {}
    tokio::task::yield_now().await;

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, br#"{}"#));
    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    let mut req = p.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(!handle.is_finished(), "still cooling at 4s");
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "SSE success must reset the backoff back to the initial 5s"
    );
    assert!(
        matches!(
            handle.await.unwrap(),
            orihsus::pool::AttemptResult::Selected(_)
        ),
        "key selectable again after the reset backoff"
    );
}

const USAGE_429_WEEKLY: &[u8] = br#"{"type":"error","error":{"type":"GoUsageLimitError","message":"Weekly usage limit reached. Resets in 3 days."},"metadata":{"workspace":"wrk_LEAKSECRET","limitName":"weekly"}}"#;

#[tokio::test(start_paused = true)]
async fn usage_limit_error_body_sets_dimension_cooldown_not_capped() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool_with_timeout(&["a"], Duration::from_secs(30));
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, USAGE_429_WEEKLY));

    let resp = send_io(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    // 3 days parsed from the message; the pool waits up to 30s then returns
    // the remaining cooldown (NOT capped at backoff_max=60s).
    let mut req = p.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    let remaining = 3 * 24 * 3600 - 30;
    match handle.await.unwrap() {
        orihsus::pool::AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                Duration::from_secs(remaining),
                "usage cooldown is the parsed reset duration, uncapped"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn usage_limit_fails_over_to_second_key_with_different_authorization() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, USAGE_429_WEEKLY));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK, "failover to key-2 succeeds");

    let captured = control.requests.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(
        captured[0].headers.get("authorization").unwrap(),
        "Bearer key-1"
    );
    assert_eq!(
        captured[1].headers.get("authorization").unwrap(),
        "Bearer key-2",
        "usage-limited key-1 must be skipped on the next attempt"
    );
}

#[tokio::test]
async fn unavailable_proactive_usage_check_leaves_passive_429_failover_intact() {
    struct Unavailable;
    #[async_trait::async_trait]
    impl orihsus::usage::UsageFetcher for Unavailable {
        async fn fetch(
            &self,
            _key: &orihsus::config::Secret,
        ) -> Result<Vec<u8>, orihsus::usage::UsageFetchError> {
            Err(orihsus::usage::UsageFetchError::Transport)
        }
    }
    struct Now;
    impl orihsus::usage::Clock for Now {
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            "2026-08-14T12:00:00Z".parse().unwrap()
        }
    }
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let p = pool(&["key-1", "key-2"]);
    orihsus::usage::UsageMonitor::poll_once(
        &Unavailable,
        &Now,
        80.0,
        &[orihsus::config::Secret::new("key-1")],
        &p,
    )
    .await;
    let q = queue(2, 2, Duration::from_secs(30));
    let app = build_router(state_with(
        &p,
        &q,
        &format!("http://{addr}"),
        TestSink::default(),
    ));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, USAGE_429_WEEKLY));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(200, br#"{"id":"ok","choices":[]}"#));
    assert_eq!(send(&app, chat_req()).await.status(), StatusCode::OK);
    let captured = control.requests.lock().unwrap();
    assert_eq!(
        captured[0].headers.get("authorization").unwrap(),
        "Bearer key-1"
    );
    assert_eq!(
        captured[1].headers.get("authorization").unwrap(),
        "Bearer key-2"
    );
}

#[tokio::test]
async fn usage_payload_workspace_and_message_never_leak_into_success() {
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let app = build_router(state(
        &format!("http://{addr}"),
        &["key-1", "key-2"],
        queue(2, 2, Duration::from_secs(30)),
        TestSink::default(),
    ));

    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(429, USAGE_429_WEEKLY));
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(
        !body.contains("wrk_LEAKSECRET") && !body.contains("Weekly usage limit reached"),
        "the failed key's workspace/message must never leak: {body}"
    );
}

fn upstream_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

#[tokio::test]
async fn models_endpoint_reflects_the_hot_reloaded_list() {
    use orihsus::gateway::{GatewayState, RuntimeState, RuntimeStore};

    // /v1/models must serve the models of the CURRENT runtime snapshot, so a hot
    // reload of the configured list is visible to new requests immediately while
    // nothing about the snapshot mutates in place.
    let pool = pool(&["a"]);
    let runtime = RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("gway-token"),
        base_url: Url::parse("http://127.0.0.1:1").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });
    let queue = queue(2, 2, Duration::from_secs(30));
    let state = GatewayState::with_runtime(
        upstream_client(),
        runtime.clone(),
        pool.clone(),
        queue.clone(),
        Arc::new(TestSink::default()),
        budget_for(1 << 20),
        IoTimeouts::default(),
    );
    let app = build_router(state);

    async fn models_ids(app: &axum::Router) -> Vec<String> {
        let req = Request::builder()
            .uri("/v1/models")
            .header("authorization", "Bearer gway-token")
            .body(Body::empty())
            .unwrap();
        let resp = send(app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
        v["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    }
    assert_eq!(models_ids(&app).await, vec!["deepseek-chat"]);

    runtime.update(RuntimeState {
        gateway_token: Secret::new("gway-token"),
        base_url: Url::parse("http://127.0.0.1:1").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string(), "deepseek-reasoner".to_string()],
    });

    assert_eq!(
        models_ids(&app).await,
        vec!["deepseek-chat", "deepseek-reasoner"],
        "a hot reload of the models list must be served to new /v1/models requests"
    );
}

#[tokio::test]
async fn hot_reload_updates_new_requests_but_inflight_keeps_old_snapshot() {
    use orihsus::gateway::{GatewayState, RuntimeState, RuntimeStore};

    let control_old = Arc::new(MockControl::default());
    let addr_old = start_mock(control_old.clone()).await;
    let control_new = Arc::new(MockControl::default());
    let addr_new = start_mock(control_new.clone()).await;

    let pool = Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("key-old")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 5,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(2),
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    );
    let runtime = RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("token-old"),
        base_url: Url::parse(&format!("http://{addr_old}")).unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });
    let queue = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let state = GatewayState::with_runtime(
        upstream_client(),
        runtime.clone(),
        pool.clone(),
        queue.clone(),
        Arc::new(sink.clone()),
        budget_for(1 << 20),
        IoTimeouts::default(),
    );
    let app = build_router(state);

    let (gate_tx, gate_rx) = tokio::sync::mpsc::channel(1);
    control_old.sse.lock().unwrap().replace(SseControl {
        event1: br#"data: {"choices":[{"delta":{"content":"a"}}]}

"#
        .to_vec(),
        event2: br#"data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}

data: [DONE]

"#
        .to_vec(),
        event2b: None,
        gate2: gate_rx,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancel_notify: Arc::new(tokio::sync::Notify::new()),
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer token-old")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"deepseek-chat","messages":[]}"#.to_string(),
        ))
        .unwrap();
    let resp = send(&app, req).await;
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream",
        "inflight SSE starts on the old snapshot"
    );
    let mut body = resp.into_body();
    let _e1 = next_data_frame(&mut body).await;
    {
        let captured = control_old.requests.lock().unwrap();
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-old",
            "inflight SSE forwards with the old key"
        );
    }

    runtime
        .update_with_keys(
            &pool,
            vec![Secret::new("key-new")],
            RuntimeState {
                gateway_token: Secret::new("token-new"),
                base_url: Url::parse(&format!("http://{addr_new}")).unwrap(),
                max_body_bytes: 1 << 20,
                models: vec!["deepseek-chat".to_string()],
            },
        )
        .unwrap();

    gate_tx.send(()).await.unwrap();
    while next_data_frame(&mut body).await.is_some() {}
    tokio::task::yield_now().await;
    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].key_fingerprint.as_deref(),
            Some(fingerprint("key-old").as_str()),
            "inflight SSE audits the key it was selected with"
        );
    }

    control_new
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer token-new")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"deepseek-chat","messages":[]}"#.to_string(),
        ))
        .unwrap();
    let resp = send(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK, "new token authenticates");
    {
        let captured = control_new.requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].headers.get("authorization").unwrap(),
            "Bearer key-new",
            "new request forwards to the new base_url with the new key"
        );
    }

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer token-old")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"deepseek-chat","messages":[]}"#.to_string(),
        ))
        .unwrap();
    let resp = send(&app, req).await;
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "the old token is no longer valid"
    );
}

#[test]
fn update_with_keys_is_atomic_with_snapshot_and_request() {
    use orihsus::gateway::{RuntimeState, RuntimeStore};
    use std::sync::mpsc;

    let policy = PoolPolicy {
        backoff_initial: Duration::from_secs(5),
        backoff_max: Duration::from_secs(60),
        breaker_threshold: 5,
        breaker_cooldown: Duration::from_secs(60),
        wait_timeout: Duration::from_secs(2),
        max_attempts: 2,
    };
    let pool = Arc::new(KeyPool::new(vec![Secret::new("key-1")], policy).unwrap());
    let store = RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("token-A"),
        base_url: Url::parse("https://old.example").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });

    let (replaced_tx, replaced_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let w_store = store.clone();
    let w_pool = pool.clone();
    let writer = std::thread::spawn(move || {
        w_store
            .update_with_keys_holding(
                &w_pool,
                vec![Secret::new("key-2")],
                RuntimeState {
                    gateway_token: Secret::new("token-B"),
                    base_url: Url::parse("https://new.example").unwrap(),
                    max_body_bytes: 1 << 20,
                    models: vec!["deepseek-chat".to_string()],
                },
                replaced_tx,
                release_rx,
            )
            .unwrap();
    });
    // Writer has replaced keys but not yet published: pool=key-2, store=token-A/old.
    replaced_rx.recv().unwrap();

    let s_store = store.clone();
    let s_pool = pool.clone();
    let reader = std::thread::spawn(move || {
        let (rt, _attempts) = s_store.snapshot_and_request(&s_pool);
        (
            rt.gateway_token.as_str().to_string(),
            rt.base_url.as_str().to_string(),
        )
    });

    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !reader.is_finished(),
        "a request starting mid-update must block; it must never observe key-2 with token-A/base_url-old"
    );

    release_tx.send(()).unwrap();
    let (token, base) = reader.join().unwrap();
    assert_eq!(
        (token.as_str(), base.as_str()),
        ("token-B", "https://new.example/"),
        "the request must observe the fully-published (all-new) state, never a mix"
    );

    writer.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_between_snapshot_and_first_next_keeps_runtime_and_held_lease_only() {
    use orihsus::gateway::{RuntimeState, RuntimeStore};

    let pool = pool(&["key-1", "key-2"]);
    let store = RuntimeStore::new(RuntimeState {
        gateway_token: Secret::new("token-A"),
        base_url: Url::parse("https://old.example").unwrap(),
        max_body_bytes: 1 << 20,
        models: vec!["deepseek-chat".to_string()],
    });

    // One in-flight request selects `key-1` before the reload...
    let (rt_old, mut attempts_old) = store.snapshot_and_request(&pool);
    let held = match attempts_old.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("expected Selected, got {other:?}"),
    };
    assert_eq!(held.key().as_str(), "key-1");

    // ...and a second request is paused BEFORE its first selection, carrying
    // the same old (token, base_url, models) snapshot and old key candidates.
    let (rt_paused, mut attempts_paused) = store.snapshot_and_request(&pool);

    // An atomic hot apply lands in between: it removes both old keys.
    store
        .update_with_keys(
            &pool,
            vec![Secret::new("key-new-1")],
            RuntimeState {
                gateway_token: Secret::new("token-B"),
                base_url: Url::parse("https://new.example").unwrap(),
                max_body_bytes: 1 << 20,
                models: vec!["deepseek-chat".to_string()],
            },
        )
        .unwrap();

    // The already-selected lease stays valid: an in-flight request may keep a
    // key that reload has since removed, and its snapshot is immutable.
    assert_eq!(held.key().as_str(), "key-1", "held lease must stay valid");
    assert_eq!(
        rt_old.gateway_token.as_str(),
        "token-A",
        "the in-flight request's snapshot is atomic: token stays old"
    );

    // The paused request must NOT select a key removed by reload: its snapshot
    // is still atomic (fully old) but its keys are gone, so the first selection
    // reports Unavailable rather than selecting a stale key.
    match attempts_paused.next().await {
        orihsus::pool::AttemptResult::Unavailable { .. } => {}
        other => panic!("expected Unavailable, got {other:?}"),
    }
    assert_eq!(
        rt_paused.gateway_token.as_str(),
        "token-A",
        "the paused request's snapshot is atomic: token stays old"
    );
    assert_eq!(
        rt_paused.base_url.as_str(),
        "https://old.example/",
        "the paused request's snapshot is atomic: base_url stays old"
    );
    assert_eq!(
        rt_paused.models,
        vec!["deepseek-chat".to_string()],
        "the paused request's snapshot is atomic: models stay old"
    );

    // A fresh request after the apply sees the new generation entirely.
    let (rt_new, mut attempts_new) = store.snapshot_and_request(&pool);
    let sel = match attempts_new.next().await {
        orihsus::pool::AttemptResult::Selected(s) => s,
        other => panic!("expected Selected, got {other:?}"),
    };
    assert_eq!(sel.key().as_str(), "key-new-1");
    assert_eq!(rt_new.gateway_token.as_str(), "token-B");
    assert_eq!(rt_new.base_url.as_str(), "https://new.example/");
    assert_eq!(rt_new.models, vec!["deepseek-chat".to_string()]);
}

#[tokio::test]
async fn non_streaming_200_two_chunks_second_gated_first_reaches_client_before_upstream_finishes() {
    // A 200 (non-SSE) response body split into exactly two chunks. The first
    // chunk must reach the client while the upstream is still open — i.e.
    // BEFORE the second chunk is released and the body ends — so the gateway
    // cannot be buffering the whole body (resp.bytes) waiting for EOF. The
    // first chunk is an incomplete JSON object, so usage stays null (the
    // bounded observer for a complete body is a later slice).
    let first_chunk: Vec<u8> = br#"{"id":"cmpl-gated","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"first"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}"#
        .to_vec();
    let second_chunk: Vec<u8> = b"}\n".to_vec();
    let (addr, control) = start_two_chunk_mock(first_chunk.clone(), second_chunk.clone()).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink.clone(),
    ));

    let resp = tokio::time::timeout(Duration::from_secs(2), send(&app, chat_req()))
        .await
        .expect("the gateway must return response headers promptly");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );

    let mut body = resp.into_body();
    let first = tokio::time::timeout(Duration::from_secs(2), next_data_frame(&mut body))
        .await
        .expect("the first chunk must arrive while the upstream body is still open")
        .expect("a data frame");
    assert_eq!(
        first.as_ref(),
        first_chunk.as_slice(),
        "the first chunk passes through untouched"
    );
    assert!(
        !control.finished.load(Ordering::SeqCst),
        "the upstream must not have finished yet: the second chunk is still gated"
    );
    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the body is live, before EOF"
    );

    // Release the second chunk: the upstream now finishes (EOF) and the
    // permit must be released once the body is fully consumed.
    control.release.send(()).await.unwrap();

    let second = tokio::time::timeout(Duration::from_secs(2), next_data_frame(&mut body))
        .await
        .expect("the second chunk must arrive after the release")
        .expect("a data frame");
    assert_eq!(
        second.as_ref(),
        second_chunk.as_slice(),
        "the second chunk passes through untouched"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), async {
            while next_data_frame(&mut body).await.is_some() {}
        })
        .await
        .is_ok(),
        "the body must reach EOF after the upstream finishes"
    );

    for _ in 0..200 {
        if q.snapshot().active == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        q.snapshot().active,
        0,
        "permit released once the body reaches EOF"
    );
    assert!(
        control.finished.load(Ordering::SeqCst),
        "the upstream must have completed the full body"
    );

    tokio::task::yield_now().await;
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1, "audited once");
    assert_eq!(records[0].status, 200);
}

#[tokio::test]
async fn non_streaming_200_streams_the_first_chunk_before_upstream_ends() {
    // A 200 (non-SSE) response body that is chunked and never terminates: the
    // upstream sends one chunk and then keeps the connection open forever. The
    // gateway must forward the first chunk to the client BEFORE the upstream
    // body ends — it must not buffer the whole body (resp.bytes) and wait for
    // EOF. The body is an incomplete JSON object (its closing brace and the
    // terminating 0-chunk never arrive), so usage must stay null.
    let first_chunk: Vec<u8> = br#"{"id":"cmpl-chunked","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"first"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}"#
        .to_vec();
    let (addr, control) = start_chunked_stalling_mock(first_chunk.clone()).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let app = build_router(state_with(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink.clone(),
    ));

    let resp = tokio::time::timeout(Duration::from_secs(2), send(&app, chat_req()))
        .await
        .expect("the gateway must return response headers promptly");
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_body();
    let chunk = tokio::time::timeout(Duration::from_secs(2), next_data_frame(&mut body))
        .await
        .expect("the first chunk must arrive while the upstream body is still open")
        .expect("a data frame");
    assert_eq!(
        chunk.as_ref(),
        first_chunk.as_slice(),
        "body bytes pass through untouched"
    );

    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the body is live, before EOF"
    );

    drop(body);
    wait_until_closed(&control, 1).await;
    tokio::task::yield_now().await;
    assert_eq!(
        q.snapshot().active,
        0,
        "client drop cancels upstream and releases the permit"
    );

    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1, "audited once");
    assert_eq!(
        records[0].input_tokens, None,
        "a body that never reaches EOF must not yield usage"
    );
    assert_eq!(records[0].output_tokens, None);
    assert_eq!(records[0].status, 200);
}

#[tokio::test]
async fn huge_non_streaming_body_passes_through_but_usage_is_null() {
    // A complete 200 JSON body beyond the fixed small usage cap: it must still
    // pass through byte-for-byte, but usage must be recorded as null (the
    // gateway must not accumulate the whole body to extract usage).
    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    let sink = TestSink::default();
    let app = build_router(state(
        &format!("http://{addr}"),
        &["a"],
        queue(2, 2, Duration::from_secs(30)),
        sink.clone(),
    ));

    let mut big = br#"{"id":"big","object":"chat.completion","choices":[],"usage":{"prompt_tokens":99,"completion_tokens":88},"pad":""#
        .to_vec();
    big.extend(std::iter::repeat_n(b'a', 80 * 1024));
    big.extend_from_slice(b"\"}");
    assert!(
        big.len() > 64 * 1024,
        "body must exceed the fixed JSON usage cap"
    );

    control.responses.lock().unwrap().push_back(MockResponse {
        status: 200,
        content_type: "application/json",
        body: big.clone(),
        extra_headers: Vec::new(),
    });

    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert_eq!(
        body.as_bytes(),
        big.as_slice(),
        "the body must still pass through byte-for-byte"
    );

    tokio::task::yield_now().await;
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].input_tokens, None,
        "usage must be null for a body beyond the fixed small cap"
    );
    assert_eq!(records[0].output_tokens, None);
    assert_eq!(records[0].status, 200);
}

#[tokio::test(start_paused = true)]
async fn slow_client_never_reading_is_cancelled_after_response_write_and_releases_permit() {
    // A client that reads the response headers and one chunk, then stops
    // reading (but keeps the body open) while the upstream keeps filling the
    // gateway's 16-slot channel: the pump parks on `tx.send`, and once the
    // per-chunk `response_write` budget (30s) elapses the stream must end, the
    // upstream connection must be cancelled and the admission permit released.
    let (addr, control) = start_backpressure_mock(32, "text/event-stream").await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let timeouts = IoTimeouts {
        response_write: Duration::from_secs(30),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        timeouts,
    ));

    // Drive the request with yields only (real I/O, virtual clock parked at 0)
    // so no timer can fire before the client stops reading.
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let resp = loop {
        tokio::task::yield_now().await;
        if handle.is_finished() {
            break handle.await.unwrap();
        }
    };
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _first = next_data_frame(&mut body)
        .await
        .expect("the first chunk streams");
    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the client stops reading"
    );

    // Cross the write budget while the client never reads again: the pump's
    // blocked send must time out, cancelling the upstream and ending the body.
    let mut elapsed = Duration::ZERO;
    loop {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
        assert!(
            elapsed < Duration::from_secs(40),
            "the write budget must fire, not hang forever"
        );
        if control.closed.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }

    assert_eq!(
        q.snapshot().active,
        0,
        "permit released once the write budget fires"
    );
    // Chunks already handed to the client before the budget fired are still
    // buffered in the response channel; the body must drain them and then end.
    while next_data_frame(&mut body).await.is_some() {}
    tokio::task::yield_now().await;
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1, "audited once");
    assert_eq!(records[0].status, 200);
    assert_eq!(
        records[0].outcome,
        Some(AuditOutcome::ClientCancel),
        "a downstream write timeout reuses client_cancel (no schema expansion)"
    );
}

#[tokio::test(start_paused = true)]
async fn slow_client_consuming_within_response_write_resets_the_budget() {
    // A slow-but-consuming client: after parking the pump on a full channel it
    // reads one chunk just before the 30s budget elapses (t=29s). That completed
    // send arms a fresh per-chunk budget, so the stream must survive a further
    // 29s of silence and only be abandoned a full 30s after the last consume.
    let (addr, control) = start_backpressure_mock(32, "text/event-stream").await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let timeouts = IoTimeouts {
        response_write: Duration::from_secs(30),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        timeouts,
    ));

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let resp = loop {
        tokio::task::yield_now().await;
        if handle.is_finished() {
            break handle.await.unwrap();
        }
    };
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _first = next_data_frame(&mut body)
        .await
        .expect("the first chunk streams");
    assert_eq!(q.snapshot().active, 1);

    // Park the pump on a full channel (real I/O; virtual clock stays at 0).
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    // Cross to just under the budget, then consume one chunk.
    tokio::time::advance(Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    let _consumed = next_data_frame(&mut body)
        .await
        .expect("a chunk consumed at t=29s");
    // The completed send lets the pump enqueue one more chunk and park again
    // with a fresh budget armed at t=29s.
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }

    // A further 29s of silence must NOT end the stream: the budget was reset.
    tokio::time::advance(Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        q.snapshot().active,
        1,
        "permit still held: the 29s consume reset the write budget"
    );
    assert_eq!(
        control.closed.load(Ordering::SeqCst),
        0,
        "upstream not cancelled: the 29s consume reset the write budget"
    );

    // Only a full 30s without any consumption abandons the stream.
    let mut elapsed = Duration::ZERO;
    loop {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
        assert!(
            elapsed < Duration::from_secs(10),
            "the reset budget must fire ~30s after the last consume"
        );
        if control.closed.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }
    assert_eq!(
        q.snapshot().active,
        0,
        "permit released once the reset budget fires"
    );
}

#[tokio::test(start_paused = true)]
async fn quiet_sse_ends_at_the_inter_event_deadline_without_failover() {
    // A quiet upstream: one SSE event, then total silence. Once response bytes
    // are committed the gateway may only terminate this stream; it must never
    // splice in another key's generation.
    let (addr, control) = start_backpressure_mock(1, "text/event-stream").await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let timeouts = IoTimeouts {
        response_write: Duration::from_secs(30),
        inter_event: Duration::from_secs(60),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &pool(&["a"]),
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        timeouts,
    ));

    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let resp = loop {
        tokio::task::yield_now().await;
        if handle.is_finished() {
            break handle.await.unwrap();
        }
    };
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let _e1 = next_data_frame(&mut body)
        .await
        .expect("the single SSE event streams");
    assert_eq!(q.snapshot().active, 1);

    // Silent SSE: the write budget is irrelevant, but the event-idle deadline
    // terminates the committed stream.
    for _ in 0..7 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
    }
    wait_until_closed(&control, 1).await;
    assert_eq!(q.snapshot().active, 0, "permit released on event idle");
    while next_data_frame(&mut body).await.is_some() {}
    let records = sink.0.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, Some(AuditOutcome::EventIdleTimeout));
    assert_eq!(
        records[0].attempts.iter().count(),
        1,
        "no post-commit retry"
    );
}

#[tokio::test(start_paused = true)]
async fn stalled_non_streaming_success_idle_times_out_ends_response_and_releases_permit() {
    use orihsus::pool::{KeyPool, NoJitter, PoolPolicy};
    // A committed 200 (non-SSE) whose body is one partial JSON chunk followed by
    // a permanent upstream stall. The client keeps reading, so the pump parks on
    // an idle `upstream.next()` (not on backpressure). The non-SSE success read
    // must be idle-bounded (reusing upstream_error_body) so the body ends, the
    // admission permit is released, the stalled upstream is cancelled and the
    // key is failed (network), never reported as a success.
    let p = Arc::new(
        KeyPool::with_jitter(
            vec![Secret::new("a")],
            PoolPolicy {
                backoff_initial: Duration::from_secs(5),
                backoff_max: Duration::from_secs(60),
                breaker_threshold: 1,
                breaker_cooldown: Duration::from_secs(60),
                wait_timeout: Duration::from_secs(30),
                max_attempts: 2,
            },
            Arc::new(NoJitter),
        )
        .unwrap(),
    );
    assert!(p.has_available_key(), "key starts selectable");

    let first_chunk: Vec<u8> =
        br#"{"id":"cmpl-stalled","object":"chat.completion","choices":[]"#.to_vec();
    let (addr, control) = start_chunked_stalling_mock(first_chunk.clone()).await;
    let q = queue(2, 2, Duration::from_secs(30));
    let sink = TestSink::default();
    let timeouts = IoTimeouts {
        upstream_error_body: Duration::from_secs(5),
        ..IoTimeouts::default()
    };
    let app = build_router(state_with_timeouts(
        &p,
        &q,
        &format!("http://{addr}"),
        sink.clone(),
        timeouts,
    ));

    // Drive until the 200 response headers arrive (real loopback IO first, so
    // the idle timer is armed only after the commit).
    let app2 = app.clone();
    let handle = tokio::spawn(async move { send(&app2, chat_req()).await });
    let resp = loop {
        tokio::task::yield_now().await;
        if handle.is_finished() {
            break handle.await.unwrap();
        }
    };
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/json"
    );
    let mut body = resp.into_body();
    let first = next_data_frame(&mut body)
        .await
        .expect("the partial JSON chunk must arrive while the upstream is still open");
    assert_eq!(first.as_ref(), first_chunk.as_slice());
    assert_eq!(
        q.snapshot().active,
        1,
        "permit held while the non-SSE body is live, before the idle bound"
    );

    // The client keeps reading to completion: the stalled read must idle-time
    // out (5s), end the body and release the permit — never a permanent hang.
    let q2 = q.clone();
    let drain = tokio::spawn(async move {
        while next_data_frame(&mut body).await.is_some() {}
        q2.snapshot().active
    });
    let mut elapsed = Duration::ZERO;
    let active = loop {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
        assert!(
            elapsed < Duration::from_secs(10),
            "the stalled non-SSE body must end after the idle bound, not hang forever"
        );
        if drain.is_finished() {
            break drain.await.unwrap();
        }
    };
    assert_eq!(
        active, 0,
        "admission permit released once the idle bound ends the non-SSE body"
    );
    assert!(
        !p.has_available_key(),
        "the idle timeout must fail the key (network), never report a success"
    );
    {
        let records = sink.0.lock().unwrap();
        assert_eq!(records.len(), 1, "audited exactly once");
        assert_eq!(records[0].status, 200);
        assert_eq!(
            records[0].outcome,
            Some(AuditOutcome::UpstreamError),
            "a stalled non-SSE body audits as an upstream error, not a completion"
        );
        assert_eq!(records[0].input_tokens, None);
    }

    // The idle timeout must also cancel the stalled upstream connection.
    let mut elapsed = Duration::ZERO;
    loop {
        if control.closed.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
        assert!(
            elapsed < Duration::from_secs(10),
            "the stalled upstream must be cancelled once the idle bound fires"
        );
    }
}

#[tokio::test]
async fn normal_stream_completion_releases_the_pump_arc_for_audit_flush() {
    // M3 residual proof (see WORKLOG round-7). `flush_audit_at_shutdown`
    // (main.rs) only flushes when `Arc::try_unwrap(writer)` succeeds, which
    // requires every stream pump task to have dropped its `Arc<GatewayState>`
    // (and hence its `Arc<dyn AuditSink>` pointing at the writer). This is the
    // NORMAL drained-shutdown case, not the forced-drain race: the in-flight
    // stream COMPLETES, so the pump must have released its Arc deterministically
    // by the time the client observes EOF — EOF on the response body is only
    // observable once the pump dropped its send half at task end. `try_unwrap`
    // (the exact seam `flush_audit_at_shutdown` uses) must therefore succeed and
    // the completed stream's audit record must flush.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("audit.jsonl");
    let writer: Arc<AuditWriter> = Arc::new(AuditWriter::start(&path, 16).unwrap());

    let control = Arc::new(MockControl::default());
    let addr = start_mock(control.clone()).await;
    control
        .responses
        .lock()
        .unwrap()
        .push_back(MockResponse::json(
            200,
            br#"{"id":"cmpl-ok","object":"chat.completion","choices":[]}"#,
        ));
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let app = build_router(GatewayState::new(
        http,
        Url::parse(&format!("http://{addr}")).unwrap(),
        pool(&["key-1"]),
        queue(2, 2, Duration::from_secs(30)),
        Secret::new("gway-token"),
        vec!["deepseek-chat".to_string()],
        Arc::clone(&writer) as Arc<dyn AuditSink>,
        1 << 20,
        budget_for(1 << 20),
        IoTimeouts::default(),
    ));

    // Serve one request and read the streamed body to EOF: a completed stream.
    let resp = send(&app, chat_req()).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("cmpl-ok"), "body: {body}");

    // Normal drained shutdown: drop the router after the last in-flight stream
    // completed. The pump task must have already dropped its Arc, so the exact
    // `flush_audit_at_shutdown` seam (`Arc::try_unwrap`) succeeds and the writer
    // is shut down cleanly, flushing the accepted record.
    drop(app);
    let writer = match Arc::try_unwrap(writer) {
        Ok(w) => w,
        Err(_) => panic!(
            "a completed stream's pump must release its GatewayState Arc, so a normal drained shutdown can flush"
        ),
    };
    writer.shutdown().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert!(
        content.contains("\"status\":200"),
        "the completed stream's audit record must be flushed on a normal drained shutdown: {content}"
    );
}
