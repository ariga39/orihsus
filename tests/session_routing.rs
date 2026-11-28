use std::sync::Arc;
use std::time::Duration;

use orihsus::audit::fingerprint;
use orihsus::config::Secret;
use orihsus::pool::{KeyPool, NoJitter, PoolPolicy};
use orihsus::routing::{RebindOutcome, RouteDecision, SessionRouting};
use orihsus::usage::UsageSnapshotStore;
use tempfile::TempDir;

fn pool() -> KeyPool {
    KeyPool::with_jitter(
        vec![Secret::new("key-1")],
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
