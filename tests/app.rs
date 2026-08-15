use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use orihsus::app::{assemble, build_upstream_client, AppRuntime, BootstrapError};
use tempfile::TempDir;
use tower::ServiceExt;

fn write_config(dir: &Path, contents: &str) -> std::path::PathBuf {
    let path = dir.join("config.yaml");
    fs::write(&path, contents).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

const MINIMAL: &str = r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
audit:
  path: "/tmp/orihsus-app-test-audit.jsonl"
"#;

const FIVE_MIN_QUEUE_WAIT: &str = r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
limits:
  queue_wait_timeout: "5m"
audit:
  path: "__AUDIT_PATH__"
"#;

#[tokio::test]
async fn assemble_builds_router_and_healthz_is_ok() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = MINIMAL.replace(
        "/tmp/orihsus-app-test-audit.jsonl",
        audit_path.to_str().unwrap(),
    );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();

    let (runtime, router) = assemble(&cfg).unwrap();
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    drop(router);
    shutdown_audit(runtime);
}

#[tokio::test]
async fn assemble_starts_and_retains_the_usage_monitor_guard() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = MINIMAL.replace(
        "/tmp/orihsus-app-test-audit.jsonl",
        audit_path.to_str().unwrap(),
    );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();
    let (mut runtime, router) = assemble(&cfg).unwrap();
    assert!(runtime.usage_monitor.is_some());
    runtime.usage_monitor.take().unwrap().shutdown().await;
    drop(router);
    shutdown_audit(runtime);
}

#[tokio::test]
async fn assemble_wires_the_body_budget_from_config() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = MINIMAL.replace(
        "/tmp/orihsus-app-test-audit.jsonl",
        audit_path.to_str().unwrap(),
    );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();

    let (runtime, router) = assemble(&cfg).unwrap();
    // Default 200×10MiB would allow ~2GiB of resident bodies; the assembled
    // budget must cap the theoretical resident body memory at 256MiB.
    assert_eq!(runtime.body_budget.capacity(), 256 * 1024 * 1024);
    assert!(
        runtime.body_budget.capacity() < 200 * 10 * 1024 * 1024,
        "the inflight budget must cap the default 200×10MiB combo below ~2GiB"
    );

    drop(router);
    shutdown_audit(runtime);
}

#[tokio::test]
async fn assemble_wires_a_custom_body_budget() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = FIVE_MIN_QUEUE_WAIT
        .replace("__AUDIT_PATH__", audit_path.to_str().unwrap())
        .replace(
            "limits:\n  queue_wait_timeout: \"5m\"",
            "limits:\n  queue_wait_timeout: \"5m\"\n  max_inflight_body_bytes: \"64MiB\"",
        );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();

    let (runtime, router) = assemble(&cfg).unwrap();
    assert_eq!(runtime.body_budget.capacity(), 64 * 1024 * 1024);

    drop(router);
    shutdown_audit(runtime);
}

#[tokio::test]
async fn assemble_serves_the_configured_models_list() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = MINIMAL
        .replace(
            "/tmp/orihsus-app-test-audit.jsonl",
            audit_path.to_str().unwrap(),
        )
        .replace(
            "keys:\n  - \"key-1\"",
            "keys:\n  - \"key-1\"\nmodels:\n  - \"deepseek-chat\"\n  - \"deepseek-reasoner\"\n",
        );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(
        cfg.models,
        vec!["deepseek-chat".to_string(), "deepseek-reasoner".to_string()]
    );

    let (runtime, router) = assemble(&cfg).unwrap();
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header("authorization", "Bearer gway-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let ids: Vec<&str> = v["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec!["deepseek-chat", "deepseek-reasoner"],
        "/v1/models must serve exactly the configured model list"
    );

    drop(router);
    shutdown_audit(runtime);
}

fn shutdown_audit(runtime: AppRuntime) {
    let audit = runtime.audit;
    if let Ok(writer) = Arc::try_unwrap(audit) {
        writer.shutdown().unwrap();
    }
}

#[tokio::test]
async fn upstream_client_does_not_follow_cross_origin_redirects() {
    use axum::routing::get;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/",
        get(|| async {
            let mut resp = axum::response::Response::new(Body::empty());
            *resp.status_mut() = StatusCode::MOVED_PERMANENTLY;
            resp.headers_mut()
                .insert("location", "http://127.0.0.1:1/elsewhere".parse().unwrap());
            resp
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = build_upstream_client().unwrap();
    let resp = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::MOVED_PERMANENTLY,
        "a redirect to a different effective port must not be followed"
    );
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "http://127.0.0.1:1/elsewhere"
    );
}

#[tokio::test]
async fn upstream_client_follows_same_origin_redirects_and_keeps_authorization() {
    use axum::routing::get;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route(
            "/rel",
            get(|| async {
                let mut resp = axum::response::Response::new(Body::empty());
                *resp.status_mut() = StatusCode::FOUND;
                resp.headers_mut()
                    .insert("location", "/target".parse().unwrap());
                resp
            }),
        )
        .route(
            "/abs",
            get(move || async move {
                let mut resp = axum::response::Response::new(Body::empty());
                *resp.status_mut() = StatusCode::FOUND;
                resp.headers_mut()
                    .insert("location", format!("http://{addr}/target").parse().unwrap());
                resp
            }),
        )
        .route(
            "/target",
            get(|headers: axum::http::HeaderMap| async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                axum::response::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(auth))
                    .unwrap()
            }),
        );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // A relative and an absolute Location on the same origin must both be
    // followed, and the selected Authorization must be forwarded on the follow.
    let client = build_upstream_client().unwrap();
    for path in ["/rel", "/abs"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .header("authorization", "Bearer selected-key")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path}: same-origin follow");
        let body = resp.text().await.unwrap();
        assert_eq!(
            body, "Bearer selected-key",
            "{path}: the selected Authorization must be forwarded on a same-origin redirect"
        );
    }
}

#[tokio::test]
async fn upstream_client_stops_scheme_and_cross_host_redirects() {
    use axum::routing::get;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route(
            "/scheme",
            get(|| async {
                let mut resp = axum::response::Response::new(Body::empty());
                *resp.status_mut() = StatusCode::FOUND;
                resp.headers_mut()
                    .insert("location", "https://example.com/elsewhere".parse().unwrap());
                resp
            }),
        )
        .route(
            "/host",
            get(|| async {
                let mut resp = axum::response::Response::new(Body::empty());
                *resp.status_mut() = StatusCode::FOUND;
                resp.headers_mut()
                    .insert("location", "http://example.com/elsewhere".parse().unwrap());
                resp
            }),
        );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Scheme-changing and cross-host redirects must be stopped: the 3xx is
    // returned untouched and no request is ever sent to the redirect target, so
    // the selected Authorization cannot leak there.
    let client = build_upstream_client().unwrap();
    for path in ["/scheme", "/host"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FOUND,
            "{path}: a cross-origin/scheme redirect must not be followed"
        );
        assert_eq!(
            resp.headers().get("location").unwrap().to_str().unwrap(),
            if path == "/scheme" {
                "https://example.com/elsewhere"
            } else {
                "http://example.com/elsewhere"
            }
        );
    }
}

#[tokio::test]
async fn upstream_client_caps_a_same_origin_redirect_loop() {
    use axum::routing::get;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    // A misbehaving upstream that always redirects to itself must not hang the
    // request: the custom policy caps the same-origin chain and returns the 3xx.
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/loop",
        get(move || async move {
            hits2.fetch_add(1, Ordering::SeqCst);
            let mut resp = axum::response::Response::new(Body::empty());
            *resp.status_mut() = StatusCode::FOUND;
            resp.headers_mut()
                .insert("location", "/loop".parse().unwrap());
            resp
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = build_upstream_client().unwrap();
    let resp = tokio::time::timeout(
        Duration::from_secs(5),
        client.get(format!("http://{addr}/loop")).send(),
    )
    .await
    .expect("a same-origin redirect loop must be capped, not hang")
    .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FOUND,
        "the loop must be stopped with the 3xx response"
    );
    assert!(
        hits.load(Ordering::SeqCst) <= 11,
        "the redirect chain must be capped (got {} hits)",
        hits.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn assemble_fails_cleanly_when_audit_path_is_unwritable() {
    let dir = TempDir::new().unwrap();
    let cfg_text = MINIMAL.replace(
        "/tmp/orihsus-app-test-audit.jsonl",
        &dir.path()
            .join("no-such-dir")
            .join("audit.jsonl")
            .to_string_lossy(),
    );
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();

    match assemble(&cfg) {
        Err(e) => assert!(
            matches!(e, BootstrapError::Audit(_)),
            "unwritable audit path must fail at assembly: {e:?}"
        ),
        Ok(_) => panic!("expected audit failure"),
    }
}

#[tokio::test(start_paused = true)]
async fn pool_wait_budget_is_independent_of_queue_wait_timeout() {
    use orihsus::pool::{AttemptResult, Failure};

    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_text = FIVE_MIN_QUEUE_WAIT.replace("__AUDIT_PATH__", audit_path.to_str().unwrap());
    let path = write_config(dir.path(), &cfg_text);
    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.limits.queue_wait_timeout, Duration::from_secs(300));

    let (runtime, router) = assemble(&cfg).unwrap();
    let pool = runtime.pool.clone();

    // Single key; cool it for longer than the pool's own hard 30s wait budget.
    let mut req = pool.request();
    let sel = match req.next().await {
        AttemptResult::Selected(s) => s,
        other => panic!("{other:?}"),
    };
    pool.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(120)),
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "pool wait budget must be its own 30s hard cap, not the 5m queue timeout"
    );
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                Duration::from_secs(90),
                "120s cooldown minus the 30s already waited"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }

    drop(router);
    shutdown_audit(runtime);
}
