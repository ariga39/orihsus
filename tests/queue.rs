use std::time::Duration;

use orihsus::queue::{AdmissionError, AdmissionQueue};

fn q(max_concurrency: usize, max_queue: usize, wait_timeout: Duration) -> AdmissionQueue {
    AdmissionQueue::new(max_concurrency, max_queue, wait_timeout)
}

#[tokio::test(start_paused = true)]
async fn immediate_acquire_and_raii_release() {
    let queue = q(2, 1, Duration::from_secs(30));

    let p1 = queue.acquire().await.unwrap();
    let snap = queue.snapshot();
    assert_eq!(snap.active, 1, "one in-flight");
    assert_eq!(snap.queued, 0, "immediate acquire does not queue");
    assert_eq!(snap.max_concurrency, 2);
    assert_eq!(snap.max_queue, 1);
    assert_eq!(snap.wait_timeout, Duration::from_secs(30));

    let p2 = queue.acquire().await.unwrap();
    assert_eq!(queue.snapshot().active, 2);

    drop(p1);
    assert_eq!(
        queue.snapshot().active,
        1,
        "drop must release the concurrency slot"
    );
    drop(p2);
    assert_eq!(queue.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn full_concurrency_queues_up_to_max_queue_then_full() {
    let queue = q(1, 2, Duration::from_secs(30));
    let p1 = queue.acquire().await.unwrap();
    assert_eq!(queue.snapshot().active, 1);

    let h2 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    let h3 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 2, "waiters occupy the queue");
    assert_eq!(queue.snapshot().active, 1);

    assert!(
        matches!(queue.acquire().await, Err(AdmissionError::Full)),
        "queue full -> Full"
    );

    drop(p1);
    let p2 = h2.await.unwrap().unwrap();
    assert_eq!(queue.snapshot().active, 1);
    assert_eq!(queue.snapshot().queued, 1, "one waiter still queued");

    drop(p2);
    let p3 = h3.await.unwrap().unwrap();
    assert_eq!(queue.snapshot().active, 1);
    assert_eq!(queue.snapshot().queued, 0);
    drop(p3);
    assert_eq!(queue.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn zero_queue_means_immediate_reject_when_full() {
    let queue = q(1, 0, Duration::from_secs(30));
    let p1 = queue.acquire().await.unwrap();

    assert!(
        matches!(queue.acquire().await, Err(AdmissionError::Full)),
        "max_queue=0 -> no queueing, reject immediately"
    );
    assert_eq!(queue.snapshot().queued, 0);

    drop(p1);
    assert!(
        queue.acquire().await.is_ok(),
        "slot freed -> acquire succeeds"
    );
}

#[tokio::test(start_paused = true)]
async fn queued_waiter_times_out_after_wait_timeout() {
    let queue = q(1, 1, Duration::from_secs(10));
    let _p1 = queue.acquire().await.unwrap();
    let h2 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 1);

    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert!(
        h2.is_finished(),
        "queued waiter must time out after wait_timeout"
    );
    let res = h2.await.unwrap();
    assert!(matches!(res, Err(AdmissionError::Timeout)));
    assert_eq!(
        queue.snapshot().queued,
        0,
        "timeout must return the queue slot"
    );
}

#[tokio::test(start_paused = true)]
async fn canceling_a_queued_acquire_returns_the_slot() {
    let queue = q(1, 1, Duration::from_secs(30));
    let p1 = queue.acquire().await.unwrap();

    let h2 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 1);

    h2.abort();
    tokio::task::yield_now().await;
    assert_eq!(
        queue.snapshot().queued,
        0,
        "cancel must return the queue slot"
    );

    let h3 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(
        queue.snapshot().queued,
        1,
        "slot must be reusable after cancel"
    );

    drop(p1);
    let p3 = h3.await.unwrap().unwrap();
    assert_eq!(queue.snapshot().queued, 0);
    assert_eq!(queue.snapshot().active, 1);
    drop(p3);
    assert_eq!(queue.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn close_rejects_new_acquisitions_and_queued_waiters() {
    let queue = q(1, 1, Duration::from_secs(30));
    let p1 = queue.acquire().await.unwrap();
    let h2 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 1);

    queue.close();

    assert!(
        matches!(queue.acquire().await, Err(AdmissionError::Closed)),
        "new acquire after close -> Closed"
    );
    tokio::task::yield_now().await;
    assert!(h2.is_finished(), "queued waiter must resolve to Closed");
    assert!(matches!(h2.await.unwrap(), Err(AdmissionError::Closed)));
    assert_eq!(queue.snapshot().queued, 0);

    drop(p1);
    assert_eq!(
        queue.snapshot().active,
        0,
        "held permit remains valid and releasable"
    );
}

#[tokio::test(start_paused = true)]
async fn snapshot_counts_stay_accurate_under_cancellation() {
    let queue = q(2, 2, Duration::from_secs(30));
    let p1 = queue.acquire().await.unwrap();
    let p2 = queue.acquire().await.unwrap();
    assert_eq!(queue.snapshot().active, 2);

    let h3 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    let h4 = tokio::spawn({
        let q = queue.clone();
        async move { q.acquire().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 2);
    assert_eq!(queue.snapshot().active, 2);

    h4.abort();
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().queued, 1, "cancel returns the queue slot");
    assert_eq!(queue.snapshot().active, 2);

    drop(p2);
    let p3 = h3.await.unwrap().unwrap();
    assert_eq!(queue.snapshot().active, 2);
    assert_eq!(queue.snapshot().queued, 0);

    drop(p1);
    drop(p3);
    assert_eq!(queue.snapshot().active, 0);
    assert_eq!(queue.snapshot().queued, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_acquires_never_underflow_counts() {
    use std::sync::Arc;
    let queue = Arc::new(q(2, 4, Duration::from_secs(5)));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let queue = Arc::clone(&queue);
        handles.push(tokio::spawn(async move {
            let _ = queue.acquire().await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let s = queue.snapshot();
    assert_eq!(s.active, 0, "all acquired permits must be returned");
    assert_eq!(s.queued, 0, "all queue slots must be returned");
    assert_eq!(s.max_concurrency, 2);
    assert_eq!(s.max_queue, 4);

    let p = queue.acquire().await.unwrap();
    drop(p);
}
