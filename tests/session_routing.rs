use std::sync::Arc;
use std::time::Duration;

use orihsus::audit::fingerprint;
use orihsus::config::Secret;
use orihsus::pool::{KeyPool, NoJitter, PoolPolicy};
use orihsus::routing::{RebindOutcome, RouteDecision, SessionRouting};
use orihsus::usage::UsageSnapshotStore;
use tempfile::TempDir;

fn pool() -> KeyPool {
    pool_with_keys(&["key-1"])
}

fn pool_with_keys(keys: &[&str]) -> KeyPool {
    KeyPool::with_jitter(
        keys.iter().map(|key| Secret::new(*key)).collect(),
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
    .unwrap()
}

fn record_monthly(snapshots: &UsageSnapshotStore, key: &str, reset: &str) {
    let now = chrono::Utc::now();
    let body = serde_json::json!({
        "usage": {
            "rolling": {"percent": 0, "resetsAt": (now + chrono::Duration::hours(5)).to_rfc3339()},
            "weekly": {"percent": 0, "resetsAt": (now + chrono::Duration::days(7)).to_rfc3339()},
            "monthly": {"percent": 0, "resetsAt": reset},
        }
    });
    assert!(snapshots.record_response(body.to_string().as_bytes(), now, &Secret::new(key)));
}

#[test]
fn monthly_reset_order_known_before_unknown_then_pinned_order() {
    let dir = TempDir::new().unwrap();
    let usage = UsageSnapshotStore::default();
    let routing = SessionRouting::open(dir.path().join("sessions.sqlite3"), usage.clone()).unwrap();
    let pool = pool_with_keys(&["key-1", "key-2"]);
    let candidates = vec![fingerprint("key-1"), fingerprint("key-2")];
    let model = "deepseek-chat";
    let now = chrono::Utc::now();
    let at = |days: i64| (now + chrono::Duration::days(days)).to_rfc3339();

    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        candidates,
        "all unknown keeps pinned pool order"
    );

    record_monthly(&usage, "key-2", &at(10));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        vec![fingerprint("key-2"), fingerprint("key-1")],
        "a known future reset outranks an unknown later candidate"
    );

    record_monthly(&usage, "key-1", &at(20));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        vec![fingerprint("key-2"), fingerprint("key-1")]
    );

    record_monthly(&usage, "key-1", &at(5));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        vec![fingerprint("key-1"), fingerprint("key-2")],
        "earliest known future reset wins"
    );

    record_monthly(&usage, "key-1", &at(-1));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        vec![fingerprint("key-2"), fingerprint("key-1")],
        "a past reset is unknown, not stale-snapshot age"
    );

    record_monthly(&usage, "key-1", &at(7));
    record_monthly(&usage, "key-2", &at(7));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        candidates,
        "equal known dates keep pinned order"
    );

    record_monthly(&usage, "key-1", "invalid");
    record_monthly(&usage, "key-2", &at(-2));
    assert_eq!(
        routing
            .new_session_order(&candidates, model, &pool)
            .unwrap(),
        candidates,
        "all invalid/past resets keep pinned order"
    );
}

#[tokio::test]
async fn tombstones_never_reuse_binding_generations() {
    let dir = TempDir::new().unwrap();
    let routing = SessionRouting::open(
        dir.path().join("sessions.sqlite3"),
        UsageSnapshotStore::default(),
    )
    .unwrap();
    let pool = pool();
    let candidates = vec![fingerprint("key-1")];
    let model = "deepseek-chat";

    let RouteDecision::Bound(first) = routing
        .decide(b"session-aba", &candidates, model, &pool)
        .await
        .unwrap()
    else {
        panic!("expected initial binding");
    };
    assert_eq!(first.generation, 1);

    let outcome = routing
        .rebind_after_usage_limit(
            b"session-aba",
            &first.fingerprint,
            first.generation,
            &candidates,
            model,
            &pool,
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(outcome, RebindOutcome::Rebound(None)));

    // The tombstone keeps generation 2. Binding the same key again must use
    // generation 3, not restart at 1.
    let RouteDecision::Bound(second) = routing
        .decide(b"session-aba", &candidates, model, &pool)
        .await
        .unwrap()
    else {
        panic!("expected binding after tombstone");
    };
    assert_eq!(second.fingerprint, first.fingerprint);
    assert_eq!(second.generation, 3);

    // A stale exact-exhaustion report from generation 1 must not overwrite
    // the newer generation-3 binding.
    let stale = routing
        .rebind_after_usage_limit(
            b"session-aba",
            &first.fingerprint,
            first.generation,
            &candidates,
            model,
            &pool,
            &[],
        )
        .await
        .unwrap();
    match stale {
        RebindOutcome::Current(Some(current)) => assert_eq!(current, second),
        other => panic!("stale report must observe the current binding, got {other:?}"),
    }
}
