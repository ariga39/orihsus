use std::sync::Arc;
use std::time::Duration;

use orihsus::audit::fingerprint;
use orihsus::config::Secret;
use orihsus::pool::{
    AttemptResult, Failure, Jitter, KeyPool, NoJitter, PoolError, PoolPolicy, Selection,
    UniformJitter, UsageDimension,
};

fn policy() -> PoolPolicy {
    PoolPolicy {
        backoff_initial: Duration::from_secs(5),
        backoff_max: Duration::from_secs(60),
        breaker_threshold: 5,
        breaker_cooldown: Duration::from_secs(60),
        wait_timeout: Duration::from_secs(30),
        max_attempts: 2,
    }
}

fn pool(keys: &[&str]) -> KeyPool {
    pool_with(keys, policy())
}

fn pool_with(keys: &[&str], policy: PoolPolicy) -> KeyPool {
    KeyPool::with_jitter(
        keys.iter().map(|k| Secret::new(*k)).collect(),
        policy,
        Arc::new(NoJitter),
    )
    .unwrap()
}

fn expect_selected(result: AttemptResult) -> Selection {
    match result {
        AttemptResult::Selected(sel) => sel,
        other => panic!("expected Selected, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn empty_key_list_is_rejected() {
    let result = KeyPool::new(Vec::new(), policy());
    assert!(matches!(result, Err(PoolError::EmptyKeys)));
}

#[tokio::test(start_paused = true)]
async fn invalid_policy_is_rejected() {
    let keys = vec![Secret::new("a")];

    let mut bad_max_attempts_zero = policy();
    bad_max_attempts_zero.max_attempts = 0;
    let mut bad_max_attempts_three = policy();
    bad_max_attempts_three.max_attempts = 3;
    let mut bad_backoff_order = policy();
    bad_backoff_order.backoff_max = Duration::from_secs(5);
    bad_backoff_order.backoff_initial = Duration::from_secs(60);
    let mut bad_breaker_zero = policy();
    bad_breaker_zero.breaker_threshold = 0;
    let mut bad_wait_zero = policy();
    bad_wait_zero.wait_timeout = Duration::ZERO;

    for (name, p) in [
        ("max_attempts=0", bad_max_attempts_zero),
        ("max_attempts=3", bad_max_attempts_three),
        ("backoff_max<initial", bad_backoff_order),
        ("breaker_threshold=0", bad_breaker_zero),
        ("wait_timeout=0", bad_wait_zero),
    ] {
        let result = KeyPool::new(keys.clone(), p);
        assert!(
            matches!(result, Err(PoolError::InvalidPolicy(_))),
            "policy {name} must be rejected"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn fill_first_selects_the_current_key_for_each_request() {
    let pool = pool(&["a", "b"]);

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(sel.fingerprint(), fingerprint("a"));
    assert_eq!(sel.key().as_str(), "a");

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "fill-first keeps the current key"
    );

    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "selection is fixed after select"
    );
    assert_eq!(sel.key().as_str(), "a");
}

#[tokio::test(start_paused = true)]
// Superseded 2026-08-13: quota/soft-threshold strategy removed. Success never
// switches the fill-first key; only failures do.
async fn success_never_switches_the_fill_first_key() {
    let pool = pool(&["a", "b"]);

    let mut req = pool.request();
    let sel_a = expect_selected(req.next().await);
    pool.report_success(&sel_a);

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "success keeps the current fill-first key"
    );

    pool.report_success(&sel);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "repeated success never advances the cursor"
    );
}

#[tokio::test(start_paused = true)]
async fn unavailable_failure_switches_to_next_healthy_key() {
    let pool = pool(&["a", "b"]);

    let mut req = pool.request();
    let sel_a = expect_selected(req.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    pool.report_failure(&sel_a, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("b"),
        "current key unusable -> switch"
    );
}

#[tokio::test(start_paused = true)]
async fn proactive_cooldown_immediately_switches_to_the_next_key() {
    let pool = pool(&["a", "b"]);
    let mut req = pool.request();
    let selected = expect_selected(req.next().await);

    pool.report_proactive_cooldown(selected.fingerprint(), Duration::from_secs(60));

    let mut next = pool.request();
    assert_eq!(
        expect_selected(next.next().await).fingerprint(),
        fingerprint("b")
    );
}

#[tokio::test(start_paused = true)]
async fn proactively_cooled_key_recovers_exactly_at_reset() {
    let pool = pool(&["a"]);
    pool.report_proactive_cooldown(&fingerprint("a"), Duration::from_secs(10));
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(
        !handle.is_finished(),
        "key must remain unavailable before reset"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        expect_selected(handle.await.unwrap()).fingerprint(),
        fingerprint("a")
    );
}

#[tokio::test(start_paused = true)]
async fn shorter_proactive_report_never_shortens_existing_cooldown() {
    let pool = pool(&["a"]);
    let fp = fingerprint("a");
    pool.report_proactive_cooldown(&fp, Duration::from_secs(20));
    pool.report_proactive_cooldown(&fp, Duration::from_secs(5));
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(19)).await;
    assert!(
        !handle.is_finished(),
        "short report must not shorten cooldown"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn older_inflight_success_cannot_clear_a_proactive_cooldown() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let old = expect_selected(req.next().await);
    pool.report_proactive_cooldown(old.fingerprint(), Duration::from_secs(10));
    pool.report_success(&old);
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(!handle.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

async fn assert_unavailable_recovery(pool: &KeyPool, expected: Duration) {
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(expected - Duration::from_millis(1)).await;
    assert!(
        !handle.is_finished(),
        "must not recover before {expected:?}"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        matches!(handle.await.unwrap(), AttemptResult::Selected(_)),
        "must recover after {expected:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn unavailable_failure_backs_off_then_passive_probe_recovers() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !handle.is_finished(),
        "key must stay cooling for its 5s backoff"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    let sel = expect_selected(handle.await.unwrap());
    assert_eq!(sel.fingerprint(), fingerprint("a"));
}

#[tokio::test(start_paused = true)]
async fn backoff_escalates_exponentially_up_to_the_cap() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(120);
    let pool = pool_with(&["a"], p);
    for expected in [
        Duration::from_secs(5),
        Duration::from_secs(10),
        Duration::from_secs(20),
        Duration::from_secs(40),
        Duration::from_secs(60),
        Duration::from_secs(60),
    ] {
        assert_unavailable_recovery(&pool, expected).await;
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_is_honored_as_is_without_cap() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
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
        "must time out after wait_timeout (30s) even though the key stays down"
    );
    let res = handle.await.unwrap();
    match res {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                Duration::from_secs(90),
                "Retry-After must be honored as-is: 120s cooldown, 30s already waited"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn zero_retry_after_falls_back_to_exponential_backoff() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::ZERO),
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !handle.is_finished(),
        "zero retry-after is treated as absent (5s backoff)"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn success_resets_backoff_state() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });
    pool.report_success(&sel);

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        handle.is_finished(),
        "success must clear the key's cooldown immediately"
    );
    let sel = expect_selected(handle.await.unwrap());

    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !handle.is_finished(),
        "backoff must reset to the initial 5s after success"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn server_failure_does_not_disable_key_but_failover_uses_distinct_key() {
    let pool = pool(&["a", "b"]);
    let mut req = pool.request();
    let sel_a = expect_selected(req.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    pool.report_failure(&sel_a, Failure::Server);

    let sel_b = expect_selected(req.next().await);
    assert_eq!(
        sel_b.fingerprint(),
        fingerprint("b"),
        "failover must use a different key"
    );

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "5xx must not cool or switch the current key"
    );
}

#[tokio::test(start_paused = true)]
async fn request_never_repeats_a_key_and_exhausts_at_max_attempts() {
    let single = pool(&["a"]);
    let mut req = single.request();
    let sel_a = expect_selected(req.next().await);
    single.report_failure(&sel_a, Failure::Server);
    assert!(
        matches!(req.next().await, AttemptResult::Exhausted),
        "no distinct key remains after using the only key"
    );

    let two = pool(&["a", "b"]);
    let mut req = two.request();
    let s1 = expect_selected(req.next().await);
    two.report_failure(&s1, Failure::Server);
    let s2 = expect_selected(req.next().await);
    assert_ne!(
        s2.fingerprint(),
        s1.fingerprint(),
        "second attempt must be a distinct key"
    );
    two.report_failure(&s2, Failure::Server);
    assert!(
        matches!(req.next().await, AttemptResult::Exhausted),
        "third attempt must exceed max_attempts (2)"
    );
}

#[tokio::test(start_paused = true)]
async fn network_failures_count_toward_circuit_breaker() {
    let pool = pool(&["a"]);
    for _ in 0..4 {
        let mut req = pool.request();
        let sel = expect_selected(req.next().await);
        pool.report_failure(&sel, Failure::Network);
    }

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("a"),
        "below breaker threshold the key stays selectable"
    );

    pool.report_failure(&sel, Failure::Network);
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(!handle.is_finished(), "breaker must cool the key for 60s");
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn all_keys_cooling_waits_until_earliest_recovery_within_budget() {
    let pool = pool(&["a", "b"]);
    let mut req = pool.request();
    let sel_a = expect_selected(req.next().await);
    pool.report_failure(&sel_a, Failure::Unavailable { retry_after: None });
    let mut req = pool.request();
    let sel_b = expect_selected(req.next().await);
    pool.report_failure(&sel_b, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(!handle.is_finished(), "all keys cooling: must wait");
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn times_out_with_retry_after_when_recovery_beyond_budget() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "must time out after wait_timeout even when recovery is later"
    );
    let res = handle.await.unwrap();
    match res {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                Duration::from_secs(15),
                "Retry-After = time remaining until the earliest recovery"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn wait_budget_is_cumulative_across_next_calls() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(30);
    let pool = pool_with(&["a", "b", "c"], p);

    // Warm `a` and `b` into long cooldowns so the request's second attempt has
    // no healthy key to fall back to and must wait on the recovery timer.
    let mut warmer = pool.request();
    let s = expect_selected(warmer.next().await);
    assert_eq!(s.fingerprint(), fingerprint("a"));
    pool.report_failure(
        &s,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );
    let s = expect_selected(warmer.next().await);
    assert_eq!(s.fingerprint(), fingerprint("b"));
    pool.report_failure(
        &s,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );

    // Request under test: the 30s wait budget starts at request() creation.
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(sel.fingerprint(), fingerprint("c"));
    pool.report_failure(
        &sel,
        Failure::Unavailable {
            retry_after: Some(Duration::from_secs(45)),
        },
    );

    // Real upstream round-trip time passes between attempts.
    tokio::time::advance(Duration::from_secs(20)).await;

    // All three keys are cooling until +45s. The cumulative budget from
    // request() creation expires at +30s, so this next() must NOT get a fresh
    // 30s budget that would let it sleep all the way to the +45s recovery.
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "the wait budget must be cumulative across next() calls, not reset per call"
    );
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                Duration::from_secs(15),
                "budget expired at +30s while the earliest recovery is +45s"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

struct FixedJitter(Duration);

impl Jitter for FixedJitter {
    fn jitter(&self, base: Duration) -> Duration {
        base + self.0
    }
}

#[tokio::test(start_paused = true)]
async fn backoff_applies_the_injected_jitter() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(120);
    let pool = KeyPool::with_jitter(
        vec![Secret::new("a")],
        p,
        Arc::new(FixedJitter(Duration::from_millis(500))),
    )
    .unwrap();

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "cooldown must include the jitter offset"
    );
    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn backoff_plus_jitter_is_capped_at_backoff_max() {
    let mut p = policy();
    p.backoff_initial = Duration::from_secs(60);
    p.backoff_max = Duration::from_secs(60);
    p.wait_timeout = Duration::from_secs(120);
    let pool = KeyPool::with_jitter(
        vec![Secret::new("a")],
        p,
        Arc::new(FixedJitter(Duration::from_secs(30))),
    )
    .unwrap();

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert!(!handle.is_finished(), "cooldown base is 60s (capped)");
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    if handle.is_finished() {
        let res = handle.await.unwrap();
        assert!(
            matches!(res, AttemptResult::Selected(_)),
            "must recover exactly at backoff_max (60s)"
        );
    } else {
        handle.abort();
        let _ = handle.await;
        panic!("jitter (base + 30s = 90s) must still be capped at backoff_max (60s)");
    }
}

#[test]
fn uniform_jitter_never_overflows_and_stays_within_spread() {
    // An extreme but constructible base (u64::MAX seconds — far beyond what the
    // config cap will accept, but reachable by a directly-built PoolPolicy) must
    // not overflow the jitter addition and panic the process (`panic = "abort"`
    // in release). The result must clamp, never wrap or panic.
    let jitter = UniformJitter::default();
    let extreme = Duration::from_secs(u64::MAX);
    let out = jitter.jitter(extreme);
    assert!(out >= extreme, "jitter never shrinks the base: {out:?}");
    assert_eq!(
        out.checked_add(Duration::ZERO),
        Some(out),
        "the jittered value must remain a valid Duration"
    );

    // `Duration::MAX` is the constructible value that saturates the intermediate
    // `half_nanos` to u64::MAX exactly (`as_nanos()/2 ≡ 2^64 − 1 mod 2^64`), so
    // `half_nanos + 1` would overflow and the `% (half_nanos + 1)` would divide
    // by zero. It too must stay panic-free and clamp.
    let out = jitter.jitter(Duration::MAX);
    assert_eq!(
        out,
        Duration::MAX,
        "jitter(Duration::MAX) must saturate to MAX"
    );

    // Sane bases keep the documented spread: [base, base + base/2].
    let base = Duration::from_secs(60);
    for _ in 0..100 {
        let out = jitter.jitter(base);
        assert!(out >= base, "jitter never shrinks the base: {out:?}");
        assert!(
            out <= base + Duration::from_secs(30),
            "jitter must stay within [base, base + base/2]: {out:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn extreme_backoff_max_report_failure_never_panics_and_clamps() {
    // The config layer caps backoff_max at MAX_COOLDOWN, but a PoolPolicy built
    // directly (bypassing config) can still carry an extreme backoff_max. A 429
    // without Retry-After jitters on `backoff_at(...)`: the jittered value must
    // saturate (not panic) and the resulting cooldown must clamp to the ops
    // ceiling exactly like a huge Retry-After does.
    let mut p = policy();
    p.backoff_initial = Duration::from_secs(u64::MAX);
    p.backoff_max = Duration::from_secs(u64::MAX);
    p.wait_timeout = Duration::from_secs(30);
    let pool = KeyPool::with_jitter(
        vec![Secret::new("a")],
        p,
        Arc::new(UniformJitter::default()),
    )
    .unwrap();

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                orihsus::pool::MAX_COOLDOWN - Duration::from_secs(30),
                "an extreme backoff+jitter must clamp to the ops ceiling, never panic"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn request_created_before_replace_keys_never_selects_a_removed_key() {
    let pool = pool(&["a", "b"]);

    // A selection returned before the removal can still report once the key is
    // gone: both reports are no-ops on the live pool.
    let mut warmer = pool.request();
    let sel_a = expect_selected(warmer.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    // A request created before the removal is pinned to its old-generation
    // candidates, but a candidate that no longer exists in the pool is
    // unavailable — never selectable.
    let mut req = pool.request();
    pool.replace_keys(vec![Secret::new("b")]).unwrap();

    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("b"),
        "a request must never select a key removed by replace_keys"
    );

    pool.report_success(&sel_a);
    pool.report_failure(&sel_a, Failure::Network);

    // A request created after replace_keys uses the new generation.
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("b"),
        "a request created after replace_keys uses the new generation"
    );

    // If every candidate was removed, the request reports Unavailable rather
    // than selecting a removed key.
    let mut req = pool.request();
    pool.replace_keys(vec![Secret::new("c")]).unwrap();
    assert!(
        matches!(req.next().await, AttemptResult::Unavailable { .. }),
        "a request whose candidates were all removed must be Unavailable"
    );
}

#[tokio::test(start_paused = true)]
async fn replace_keys_preserves_state_and_keeps_leases_valid() {
    let pool = pool(&["a"]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });
    pool.replace_keys(vec![Secret::new("a")]).unwrap();

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "cooling state must survive replace_keys"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(&sel, Failure::Unavailable { retry_after: None });
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "backoff_step must survive replace_keys (10s not 5s)"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn replace_keys_removes_adds_and_never_invalidates_held_lease() {
    let pool = pool(&["a", "b"]);
    let mut req = pool.request();
    let sel_a = expect_selected(req.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    pool.replace_keys(vec![Secret::new("b"), Secret::new("c")])
        .unwrap();

    assert_eq!(sel_a.key().as_str(), "a", "held lease must stay valid");
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    let mut req = pool.request();
    let sel_b = expect_selected(req.next().await);
    assert_eq!(sel_b.fingerprint(), fingerprint("b"));
    pool.report_failure(&sel_b, Failure::Unavailable { retry_after: None });

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("c"),
        "new key must be selectable"
    );

    assert!(matches!(
        pool.replace_keys(Vec::new()),
        Err(PoolError::EmptyKeys)
    ));
}

#[tokio::test(start_paused = true)]
async fn usage_limit_cooldown_is_not_capped_and_returns_remaining() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(30);
    let pool = pool_with(&["a"], p);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    let cooldown = Duration::from_secs(7 * 24 * 3600);
    pool.report_failure(
        &sel,
        Failure::UsageLimit {
            dimension: UsageDimension::Weekly,
            cooldown,
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    assert!(
        handle.is_finished(),
        "must time out after wait_timeout while the key stays usage-limited"
    );
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                cooldown - Duration::from_secs(30),
                "usage cooldown must NOT be capped at backoff_max (60s)"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn short_generic_429_must_not_shorten_a_usage_cooldown() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(6 * 3600);
    let pool = pool_with(&["a"], p);

    // Request B starts first and grabs `a`...
    let mut req_b = pool.request();
    let sel_b = expect_selected(req_b.next().await);
    assert_eq!(sel_b.fingerprint(), fingerprint("a"));

    // ...then request A also uses `a` and reports a long usage cooldown.
    let mut req_a = pool.request();
    let sel_a = expect_selected(req_a.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));
    pool.report_failure(
        &sel_a,
        Failure::UsageLimit {
            dimension: UsageDimension::Weekly,
            cooldown: Duration::from_secs(5 * 3600),
        },
    );

    // B's generic 429 with a short Retry-After must not shorten the cooldown.
    pool.report_failure(
        &sel_b,
        Failure::RateLimited {
            retry_after: Some(Duration::from_secs(5)),
        },
    );

    let mut req_c = pool.request();
    let handle = tokio::spawn(async move { req_c.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4 * 3600)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "a short generic 429 must not shorten a longer usage cooldown"
    );
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
    assert!(
        matches!(handle.await.unwrap(), AttemptResult::Selected(_)),
        "key must recover at the end of its usage cooldown"
    );
}

#[tokio::test(start_paused = true)]
async fn usage_limit_failure_does_not_escalate_rate_limit_backoff() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(6 * 3600);
    let pool = pool_with(&["a"], p);

    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(
        &sel,
        Failure::UsageLimit {
            dimension: UsageDimension::FiveHour,
            cooldown: Duration::from_secs(5 * 3600),
        },
    );
    // A generic 429 while the usage cooldown is active is absorbed: it neither
    // shortens the cooldown nor escalates the backoff step.
    pool.report_failure(&sel, Failure::RateLimited { retry_after: None });

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4 * 3600)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "the generic 429 must not shorten the 5h usage cooldown"
    );
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
    let sel = expect_selected(handle.await.unwrap());

    // After the usage cooldown elapses, a fresh generic 429 gets the initial
    // 5s backoff, not an escalated one.
    pool.report_failure(&sel, Failure::RateLimited { retry_after: None });
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(
        !handle.is_finished(),
        "rate-limit backoff must be the initial 5s, not escalated by the usage-limit failure"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn huge_usage_limit_cooldown_is_clamped_not_overflow() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(30);
    let pool = pool_with(&["a"], p);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    // A GoUsageLimitError "Resets in N days" whose Duration is constructible but
    // pushes `Instant + Duration` past its representable range must not panic;
    // the deadline must be clamped to the ops ceiling.
    pool.report_failure(
        &sel,
        Failure::UsageLimit {
            dimension: UsageDimension::Weekly,
            cooldown: Duration::from_secs(9_223_372_036_854_775_807),
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                orihsus::pool::MAX_COOLDOWN - Duration::from_secs(30),
                "a cooldown beyond the ops ceiling must be clamped so the returned wait stays finite"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn huge_retry_after_is_clamped_not_overflow() {
    let mut p = policy();
    p.wait_timeout = Duration::from_secs(30);
    let pool = pool_with(&["a"], p);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);
    pool.report_failure(
        &sel,
        Failure::RateLimited {
            retry_after: Some(Duration::from_secs(u64::MAX)),
        },
    );

    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    match handle.await.unwrap() {
        AttemptResult::Unavailable { retry_after } => {
            assert_eq!(
                retry_after,
                orihsus::pool::MAX_COOLDOWN - Duration::from_secs(30),
                "a Retry-After beyond the ops ceiling must be clamped so the returned wait stays finite"
            );
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn rate_limited_honors_retry_after_and_switches_key() {
    let pool_ab = pool(&["a", "b"]);
    let mut req = pool_ab.request();
    let sel_a = expect_selected(req.next().await);
    pool_ab.report_failure(
        &sel_a,
        Failure::RateLimited {
            retry_after: Some(Duration::from_secs(9)),
        },
    );

    let mut req = pool_ab.request();
    let sel = expect_selected(req.next().await);
    assert_eq!(
        sel.fingerprint(),
        fingerprint("b"),
        "generic 429 switches to the next key"
    );

    let pool_c = pool(&["c"]);
    let mut req = pool_c.request();
    let sel = expect_selected(req.next().await);
    pool_c.report_failure(
        &sel,
        Failure::RateLimited {
            retry_after: Some(Duration::from_secs(9)),
        },
    );
    let mut req = pool_c.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(8)).await;
    assert!(
        !handle.is_finished(),
        "Retry-After of 9s must be honored (still cooling at 8s)"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn usage_limit_debug_never_leaks_body_or_workspace() {
    let workspace = "wrk_super-secret-workspace";
    let failure = Failure::UsageLimit {
        dimension: UsageDimension::Weekly,
        cooldown: Duration::from_secs(7 * 24 * 3600),
    };
    let debug = format!("{failure:?}");
    assert!(
        !debug.contains(workspace) && !debug.contains("usage limit reached"),
        "UsageLimit Debug must not leak body/workspace: {debug}"
    );
}

#[tokio::test(start_paused = true)]
async fn debug_and_errors_never_leak_raw_keys() {
    let secret = "sk-super-secret-key";
    let pool = pool(&[secret]);
    let mut req = pool.request();
    let sel = expect_selected(req.next().await);

    assert!(
        !format!("{sel:?}").contains(secret),
        "Selection Debug must not leak keys"
    );
    assert!(
        !format!("{pool:?}").contains(secret),
        "KeyPool Debug must not leak keys"
    );

    let err = KeyPool::new(Vec::new(), policy()).unwrap_err();
    assert!(
        !format!("{err:?}").contains(secret),
        "error Debug must not leak keys"
    );
    assert!(
        !format!("{err}").contains(secret),
        "error Display must not leak keys"
    );
}

#[tokio::test(start_paused = true)]
async fn request_created_before_a_failure_report_skips_the_now_cooled_key() {
    for failure in [
        Failure::UsageLimit {
            dimension: UsageDimension::Weekly,
            cooldown: Duration::from_secs(7 * 24 * 3600),
        },
        Failure::RateLimited {
            retry_after: Some(Duration::from_secs(9)),
        },
        Failure::Unavailable { retry_after: None },
    ] {
        let pool = pool(&["a", "b"]);

        // Request B is created while both keys are healthy...
        let mut req_b = pool.request();

        // ...then request A selects `a` and reports the failure on it.
        let mut req_a = pool.request();
        let sel_a = expect_selected(req_a.next().await);
        assert_eq!(sel_a.fingerprint(), fingerprint("a"));
        pool.report_failure(&sel_a, failure);

        // B's candidates were captured before the report, but the cooldown is a
        // live fact: B must skip the already-cooled key and pick `b`.
        let sel_b = expect_selected(req_b.next().await);
        assert_eq!(
            sel_b.fingerprint(),
            fingerprint("b"),
            "a request created before the report must skip the key cooled in the meantime"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn stale_success_must_not_clear_a_newer_cooldown() {
    for (failure, cooldown) in [
        (
            Failure::UsageLimit {
                dimension: UsageDimension::Weekly,
                cooldown: Duration::from_secs(7 * 24 * 3600),
            },
            Duration::from_secs(7 * 24 * 3600),
        ),
        (
            Failure::Unavailable {
                retry_after: Some(Duration::from_secs(300)),
            },
            Duration::from_secs(300),
        ),
    ] {
        let mut p = policy();
        p.wait_timeout = cooldown + Duration::from_secs(3600);
        let pool = pool_with(&["a"], p);

        // Request B starts first and grabs `a`...
        let mut req_b = pool.request();
        let sel_b = expect_selected(req_b.next().await);
        assert_eq!(sel_b.fingerprint(), fingerprint("a"));

        // ...then request A also uses `a` and reports a long cooldown on it.
        let mut req_a = pool.request();
        let sel_a = expect_selected(req_a.next().await);
        assert_eq!(sel_a.fingerprint(), fingerprint("a"));
        pool.report_failure(&sel_a, failure);

        // B's success is stale relative to the newer cooldown: it must not
        // clear the cooldown another request applied after B selected `a`.
        pool.report_success(&sel_b);

        let mut req_c = pool.request();
        let handle = tokio::spawn(async move { req_c.next().await });
        tokio::task::yield_now().await;
        tokio::time::advance(cooldown / 2).await;
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "an older request's success must not clear a newer cooldown"
        );
        tokio::time::advance(cooldown / 2).await;
        tokio::task::yield_now().await;
        assert!(
            matches!(handle.await.unwrap(), AttemptResult::Selected(_)),
            "key must still be selectable once its own cooldown elapses"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn stale_success_does_not_reset_newer_cooldown_consecutive_failures() {
    let pool = pool(&["a"]);

    // Request A (older) holds a selection on `a`...
    let mut req_a = pool.request();
    let sel_a = expect_selected(req_a.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    // ...then request B (newer) also selects `a` and accumulates state on it:
    // three network failures, then an unavailable failure that opens a cooldown
    // with `consecutive_failures = 3`.
    let mut req_b = pool.request();
    let sel_b = expect_selected(req_b.next().await);
    assert_eq!(sel_b.fingerprint(), fingerprint("a"));
    for _ in 0..3 {
        pool.report_failure(&sel_b, Failure::Network);
    }
    pool.report_failure(&sel_b, Failure::Unavailable { retry_after: None });

    // A's stale success must leave B's cooldown counters untouched.
    pool.report_success(&sel_a);

    // Let the cooldown elapse, then count toward the breaker: the preserved
    // consecutive_failures of 3 plus two more network failures (5) must trip
    // it. A reset to 0 would leave the key selectable after two.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let mut req_c = pool.request();
    let sel_c = expect_selected(req_c.next().await);
    pool.report_failure(&sel_c, Failure::Network);
    pool.report_failure(&sel_c, Failure::Network);
    assert!(
        !pool.has_available_key(),
        "a stale success must not reset the newer cooldown's consecutive_failures"
    );
}

#[tokio::test(start_paused = true)]
async fn stale_success_does_not_reset_newer_cooldown_backoff_step() {
    let pool = pool(&["a"]);

    // Request A (older) holds a selection on `a`...
    let mut req_a = pool.request();
    let sel_a = expect_selected(req_a.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    // ...then request B (newer) opens a 5s cooldown that escalates backoff_step
    // to 1.
    let mut req_b = pool.request();
    let sel_b = expect_selected(req_b.next().await);
    assert_eq!(sel_b.fingerprint(), fingerprint("a"));
    pool.report_failure(&sel_b, Failure::Unavailable { retry_after: None });

    // A's stale success must leave the escalated backoff_step untouched.
    pool.report_success(&sel_a);

    // Let the cooldown elapse; the next backoff must be the escalated 10s, not
    // a reset 5s.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let mut req_c = pool.request();
    let sel_c = expect_selected(req_c.next().await);
    pool.report_failure(&sel_c, Failure::Unavailable { retry_after: None });

    let mut req_d = pool.request();
    let handle = tokio::spawn(async move { req_d.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "a stale success must not reset the newer cooldown's backoff_step (10s, not 5s)"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(start_paused = true)]
async fn stale_success_does_not_reset_newer_network_failures_below_threshold() {
    let pool = pool(&["a"]);

    // Request A (older) holds a selection on `a`...
    let mut req_a = pool.request();
    let sel_a = expect_selected(req_a.next().await);
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    // ...then request B (newer) also selects `a` and reports four network
    // failures below the breaker threshold (4 < 5): no cooldown is opened, so
    // the count is the only thing protecting the breaker.
    let mut req_b = pool.request();
    let sel_b = expect_selected(req_b.next().await);
    assert_eq!(sel_b.fingerprint(), fingerprint("a"));
    for _ in 0..4 {
        pool.report_failure(&sel_b, Failure::Network);
    }
    assert!(
        pool.has_available_key(),
        "4 failures below threshold 5 must not cool the key"
    );

    // A's stale success must not wipe B's failures even though no cooldown is
    // active.
    pool.report_success(&sel_a);

    // One more network failure trips the breaker at the configured threshold
    // (4 retained + 1 = 5): a reset to 0 would leave the key selectable.
    let mut req_c = pool.request();
    let sel_c = expect_selected(req_c.next().await);
    pool.report_failure(&sel_c, Failure::Network);
    assert!(
        !pool.has_available_key(),
        "a stale success must not reset newer network failures: breaker must trip at threshold"
    );
}

#[tokio::test(start_paused = true)]
async fn success_still_resets_failure_and_backoff_state() {
    // The selection that owns the current cooldown resets everything on its own
    // success: the cooldown is cleared immediately.
    let owned = pool(&["a"]);
    let mut req = owned.request();
    let sel = expect_selected(req.next().await);
    for _ in 0..3 {
        owned.report_failure(&sel, Failure::Network);
    }
    owned.report_failure(&sel, Failure::Unavailable { retry_after: None });
    owned.report_success(&sel);

    // Cooldown cleared immediately and consecutive_failures reset: two network
    // failures (2 < threshold 5) leave the key selectable despite the three
    // accumulated before the success.
    let mut req = owned.request();
    let sel = expect_selected(req.next().await);
    owned.report_failure(&sel, Failure::Network);
    owned.report_failure(&sel, Failure::Network);
    assert!(
        owned.has_available_key(),
        "owning-selection success must reset consecutive_failures"
    );

    // backoff_step reset: the next unavailable failure backs off 5s, not the
    // escalated 10s.
    owned.report_failure(&sel, Failure::Unavailable { retry_after: None });
    let mut req = owned.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "owning-selection success must reset backoff_step to the initial 5s"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));

    // A success with no cooldown active resets the counters just the same.
    let plain = pool(&["a"]);
    let mut req = plain.request();
    let sel = expect_selected(req.next().await);
    for _ in 0..3 {
        plain.report_failure(&sel, Failure::Network);
    }
    plain.report_success(&sel);
    let mut req = plain.request();
    let sel = expect_selected(req.next().await);
    plain.report_failure(&sel, Failure::Network);
    plain.report_failure(&sel, Failure::Network);
    assert!(
        plain.has_available_key(),
        "a success with no cooldown active must reset consecutive_failures"
    );
}

#[tokio::test(start_paused = true)]
async fn breaker_half_open_allows_only_one_concurrent_probe() {
    let mut p = policy();
    p.breaker_threshold = 1;
    p.breaker_cooldown = Duration::from_secs(5);
    let pool = pool_with(&["a"], p);

    // One network failure trips the breaker: `a` cools for breaker_cooldown and
    // then must be re-probed by a single request.
    let mut warmer = pool.request();
    let sel = expect_selected(warmer.next().await);
    pool.report_failure(&sel, Failure::Network);

    // Three concurrent requests all wait on the recovering key.
    let mut handles = Vec::new();
    for _ in 0..3 {
        let mut req = pool.request();
        handles.push(tokio::spawn(async move { req.next().await }));
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    // Exactly one request probes the recovering key; the others keep waiting.
    let mut probes = Vec::new();
    let mut waiting = Vec::new();
    for h in handles {
        if h.is_finished() {
            probes.push(h.await.unwrap());
        } else {
            waiting.push(h);
        }
    }
    assert_eq!(
        probes.len(),
        1,
        "at most one concurrent request may probe the recovering key"
    );
    assert_eq!(
        waiting.len(),
        2,
        "the other requests must wait for the probe"
    );
    let probe = expect_selected(probes.pop().unwrap());
    assert_eq!(probe.fingerprint(), fingerprint("a"));

    // The probe resolves with success: the key is confirmed healthy and the
    // waiters proceed once the claim deadline elapses.
    pool.report_success(&probe);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    for h in waiting {
        let sel = expect_selected(h.await.unwrap());
        assert_eq!(sel.fingerprint(), fingerprint("a"));
    }
}

#[tokio::test(start_paused = true)]
async fn breaker_half_open_skips_to_healthy_key_under_concurrency() {
    let mut p = policy();
    p.breaker_threshold = 1;
    p.breaker_cooldown = Duration::from_secs(5);
    let pool = pool_with(&["a", "b"], p);

    // Trip `a`; the cursor moves to the healthy `b`.
    let mut warmer = pool.request();
    let sel = expect_selected(warmer.next().await);
    assert_eq!(sel.fingerprint(), fingerprint("a"));
    pool.report_failure(&sel, Failure::Network);

    // Concurrent requests prefer the healthy key: none selects the recovering
    // `a`.
    let mut handles = Vec::new();
    for _ in 0..3 {
        let mut req = pool.request();
        handles.push(tokio::spawn(async move { req.next().await }));
    }
    for h in handles {
        let sel = expect_selected(h.await.unwrap());
        assert_eq!(
            sel.fingerprint(),
            fingerprint("b"),
            "a concurrent request must skip the recovering key and use the healthy one"
        );
    }

    // Requests that already consumed `b` fall back to `a`; only one may probe
    // it once the breaker cooldown elapses.
    let mut fallbacks = Vec::new();
    for _ in 0..2 {
        let mut req = pool.request();
        let consumed = expect_selected(req.next().await);
        assert_eq!(consumed.fingerprint(), fingerprint("b"));
        fallbacks.push(tokio::spawn(async move { req.next().await }));
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    let mut probes = Vec::new();
    let mut waiting = Vec::new();
    for h in fallbacks {
        if h.is_finished() {
            probes.push(h.await.unwrap());
        } else {
            waiting.push(h);
        }
    }
    assert_eq!(
        probes.len(),
        1,
        "at most one fallback request may probe `a`"
    );
    assert_eq!(waiting.len(), 1);
    let probe = expect_selected(probes.pop().unwrap());
    assert_eq!(probe.fingerprint(), fingerprint("a"));

    pool.report_success(&probe);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    for h in waiting {
        let sel = expect_selected(h.await.unwrap());
        assert_eq!(sel.fingerprint(), fingerprint("a"));
    }
}

#[tokio::test(start_paused = true)]
async fn dropped_half_open_probe_does_not_wedge_the_key() {
    let mut p = policy();
    p.breaker_threshold = 1;
    p.breaker_cooldown = Duration::from_secs(5);
    let pool = pool_with(&["a"], p);

    for _ in 0..2 {
        // Trip the breaker: one network failure cools `a` for breaker_cooldown.
        let mut warmer = pool.request();
        let sel = expect_selected(warmer.next().await);
        pool.report_failure(&sel, Failure::Network);

        // A single request probes the recovering key, then drops the selection
        // without reporting.
        let mut prober = pool.request();
        let handle = tokio::spawn(async move { prober.next().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        let _ = expect_selected(handle.await.unwrap());

        // The claim is just a fresh breaker_cooldown deadline: an unreported
        // probe reopens the key on its own instead of wedging it forever.
        let mut req = pool.request();
        let handle = tokio::spawn(async move { req.next().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        let sel = expect_selected(handle.await.unwrap());
        assert_eq!(
            sel.fingerprint(),
            fingerprint("a"),
            "a dropped probe must not wedge the key forever"
        );

        // Resolve the new probe cleanly so the next cycle starts from healthy.
        pool.report_success(&sel);
    }
}

#[tokio::test(start_paused = true)]
async fn late_success_from_a_dropped_probe_does_not_clear_a_newer_probe_claim() {
    let mut p = policy();
    p.breaker_threshold = 1;
    p.breaker_cooldown = Duration::from_secs(5);
    let pool = pool_with(&["a"], p);

    // Trip the breaker: `a` cools for breaker_cooldown and enters half-open.
    let mut warmer = pool.request();
    let sel = expect_selected(warmer.next().await);
    pool.report_failure(&sel, Failure::Network);

    // Probe A claims the recovering key...
    let mut prober_a = pool.request();
    let handle = tokio::spawn(async move { prober_a.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let sel_a = expect_selected(handle.await.unwrap());
    assert_eq!(sel_a.fingerprint(), fingerprint("a"));

    // ...but its claim expires without a report.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    // Probe B claims the key immediately once A's claim lapses; its fresh
    // claim deadline now owns the key.
    let mut prober_b = pool.request();
    let handle = tokio::spawn(async move { prober_b.next().await });
    tokio::task::yield_now().await;
    let sel_b = expect_selected(handle.await.unwrap());
    assert_eq!(sel_b.fingerprint(), fingerprint("a"));

    // A's late success must not clear B's claim: the key stays locked to the
    // single probe B.
    pool.report_success(&sel_a);
    assert!(
        !pool.has_available_key(),
        "a late success from a dropped probe must not clear the newer probe's claim"
    );

    // Concurrent selectors still cannot select the key while B's claim holds.
    let mut req = pool.request();
    let handle = tokio::spawn(async move { req.next().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert!(
        !handle.is_finished(),
        "the key must remain unselectable while B's claim is active"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(handle.await.unwrap(), AttemptResult::Selected(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reporting_is_safe() {
    let mut p = policy();
    p.breaker_threshold = 10_000;
    let pool = Arc::new(pool_with(&["a", "b"], p));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            for _ in 0..3 {
                let mut req = pool.request();
                if let AttemptResult::Selected(sel) = req.next().await {
                    pool.report_failure(&sel, Failure::Network);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let mut req = pool.request();
    assert!(
        matches!(req.next().await, AttemptResult::Selected(_)),
        "pool must stay functional after concurrent use"
    );
}
