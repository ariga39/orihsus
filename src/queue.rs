use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Outcome of an [`AdmissionQueue::acquire`] that did not get a permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// Concurrency is full and the queue is full (`max_queue` waiters).
    Full,
    /// The caller waited longer than `wait_timeout` in the queue.
    Timeout,
    /// The queue has been closed.
    Closed,
}

/// RAII guard: holds one in-flight concurrency slot while alive.
#[derive(Debug)]
pub struct Permit {
    #[allow(dead_code)] // held solely for RAII; the slot lives as long as this field
    permit: OwnedSemaphorePermit,
}

/// Read-only view of the queue state for health/readiness checks.
#[derive(Debug, Clone, Copy)]
pub struct QueueSnapshot {
    pub active: usize,
    pub queued: usize,
    pub max_concurrency: usize,
    pub max_queue: usize,
    pub wait_timeout: Duration,
}

/// Bounded admission control: `max_concurrency` in-flight slots plus a bounded
/// FIFO wait queue (`max_queue`), with per-waiter `wait_timeout`.
#[derive(Clone)]
pub struct AdmissionQueue {
    concurrency: std::sync::Arc<Semaphore>,
    queue_slots: std::sync::Arc<Semaphore>,
    max_concurrency: usize,
    max_queue: usize,
    wait_timeout: Duration,
}

impl AdmissionQueue {
    /// Build an admission queue.
    ///
    /// Preconditions: `max_concurrency > 0`, `wait_timeout > 0`.
    /// `max_queue == 0` means "no queueing, reject when concurrency is full".
    pub fn new(max_concurrency: usize, max_queue: usize, wait_timeout: Duration) -> AdmissionQueue {
        assert!(max_concurrency > 0, "max_concurrency must be > 0");
        assert!(wait_timeout > Duration::ZERO, "wait_timeout must be > 0");
        AdmissionQueue {
            concurrency: std::sync::Arc::new(Semaphore::new(max_concurrency)),
            queue_slots: std::sync::Arc::new(Semaphore::new(max_queue)),
            max_concurrency,
            max_queue,
            wait_timeout,
        }
    }

    /// Acquire an in-flight slot. Returns a [`Permit`] to be held (RAII) for
    /// the duration of the request.
    pub async fn acquire(&self) -> Result<Permit, AdmissionError> {
        match self.concurrency.clone().try_acquire_owned() {
            Ok(permit) => Ok(Permit { permit }),
            Err(TryAcquireError::NoPermits) => self.enqueue().await,
            Err(TryAcquireError::Closed) => Err(AdmissionError::Closed),
        }
    }

    async fn enqueue(&self) -> Result<Permit, AdmissionError> {
        let slot = match self.queue_slots.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(TryAcquireError::NoPermits) => return Err(AdmissionError::Full),
            Err(TryAcquireError::Closed) => return Err(AdmissionError::Closed),
        };
        let permit =
            match tokio::time::timeout(self.wait_timeout, self.concurrency.clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err(AdmissionError::Closed),
                Err(_) => return Err(AdmissionError::Timeout),
            };
        drop(slot);
        Ok(Permit { permit })
    }

    /// Close the queue: new acquisitions and queued waiters resolve to
    /// `Closed`. Already-held permits remain valid.
    pub fn close(&self) {
        self.concurrency.close();
        self.queue_slots.close();
    }

    /// True after [`AdmissionQueue::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.concurrency.is_closed()
    }

    /// Read-only state snapshot.
    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            active: self.max_concurrency - self.concurrency.available_permits(),
            queued: self.max_queue - self.queue_slots.available_permits(),
            max_concurrency: self.max_concurrency,
            max_queue: self.max_queue,
            wait_timeout: self.wait_timeout,
        }
    }
}
