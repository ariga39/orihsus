use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use crate::audit::fingerprint;
use crate::config::Secret;

/// System seam for backoff jitter. Tests inject a deterministic
/// implementation; production uses [`UniformJitter`].
pub trait Jitter: Send + Sync {
    /// Return a jittered duration derived from `base`.
    fn jitter(&self, base: Duration) -> Duration;
}

/// Deterministic jitter: returns `base` unchanged. Used in tests.
pub struct NoJitter;

impl Jitter for NoJitter {
    fn jitter(&self, base: Duration) -> Duration {
        base
    }
}

/// Default production jitter: `base + uniform(0, base/2]`, spread by a small
/// seedable xorshift RNG.
pub struct UniformJitter {
    rng: std::sync::Mutex<XorShift64>,
}

impl Default for UniformJitter {
    fn default() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9e37_79b9_7f4a_7c15);
        UniformJitter {
            rng: std::sync::Mutex::new(XorShift64::new(seed ^ 0x51_7c_c1_b7_27_22_0a_95)),
        }
    }
}

impl Jitter for UniformJitter {
    fn jitter(&self, base: Duration) -> Duration {
        let half_nanos = (base.as_nanos() / 2) as u64;
        let mut rng = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
        // Saturating, never `+`: for `Duration::MAX` the truncating cast yields
        // `half_nanos == u64::MAX`, so `half_nanos + 1` would overflow and the
        // following modulo would divide by zero. `saturating_add` keeps the
        // bound at u64::MAX, so the offset stays in `[0, u64::MAX)`, never
        // panicking. The outer add below is also saturating: an extreme but
        // constructible base (e.g. a directly-built PoolPolicy with an
        // unbounded backoff_max) would otherwise overflow `Duration + Duration`
        // and panic — and `panic = "abort"` in release would kill the whole
        // gateway. The caller clamps to `backoff_max` and `MAX_COOLDOWN`
        // afterwards.
        let offset = rng.next_u64() % half_nanos.saturating_add(1);
        base.saturating_add(Duration::from_nanos(offset))
    }
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        XorShift64(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Independent hard cap on how long a single request may wait for a recovering
/// key across all of its attempts. Deliberately decoupled from the queue's
/// `queue_wait_timeout`: a 5m queue wait must not become a 5m pool wait.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Ops ceiling on any cooldown deadline. A cooldown (external Retry-After or a
/// `GoUsageLimitError` reset duration) beyond this is clamped here before it is
/// added to an `Instant`, so an enormous but constructible `Duration` can never
/// overflow `Instant + Duration` (which panics, and `panic = "abort"` in
/// release would kill the whole process). Far above every legitimate horizon
/// (weekly 7d, monthly 31d, 5h) so normal semantics are untouched; the value
/// returned to a client as Retry-After is then always finite and serializable.
pub const MAX_COOLDOWN: Duration = Duration::from_secs(90 * 24 * 3600);

/// Tuning knobs for the key pool.
#[derive(Debug, Clone)]
pub struct PoolPolicy {
    /// Initial exponential backoff when no Retry-After is given.
    pub backoff_initial: Duration,
    /// Cap on the exponential backoff (including jitter) used when no
    /// Retry-After is given. A valid Retry-After is honored as-is.
    pub backoff_max: Duration,
    /// Consecutive network/other failures before the circuit breaker trips.
    pub breaker_threshold: u32,
    /// Circuit breaker cooldown after it trips.
    pub breaker_cooldown: Duration,
    /// How long `select` waits for the earliest recovering key. This is the
    /// per-request budget: a single deadline is captured at `request()` time
    /// and shared by all `next()` calls of that request. Production uses
    /// [`WAIT_TIMEOUT`]; tests tune it directly.
    pub wait_timeout: Duration,
    /// Max distinct keys a single request may try (1 or 2).
    pub max_attempts: usize,
}

impl PoolPolicy {
    fn validate(&self) -> Result<(), PoolError> {
        if self.backoff_initial.is_zero() {
            return Err(PoolError::InvalidPolicy(
                "backoff_initial must be greater than zero".into(),
            ));
        }
        if self.backoff_max < self.backoff_initial {
            return Err(PoolError::InvalidPolicy(
                "backoff_max must be >= backoff_initial".into(),
            ));
        }
        if self.breaker_threshold == 0 {
            return Err(PoolError::InvalidPolicy(
                "breaker_threshold must be at least 1".into(),
            ));
        }
        if self.breaker_cooldown.is_zero() {
            return Err(PoolError::InvalidPolicy(
                "breaker_cooldown must be greater than zero".into(),
            ));
        }
        if self.wait_timeout.is_zero() {
            return Err(PoolError::InvalidPolicy(
                "wait_timeout must be greater than zero".into(),
            ));
        }
        if !(1..=2).contains(&self.max_attempts) {
            return Err(PoolError::InvalidPolicy(
                "max_attempts must be 1 or 2".into(),
            ));
        }
        Ok(())
    }
}

/// Construction and reporting errors. Never contains key material.
#[derive(Debug)]
pub enum PoolError {
    EmptyKeys,
    InvalidPolicy(String),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::EmptyKeys => write!(f, "key pool requires at least one key"),
            PoolError::InvalidPolicy(msg) => write!(f, "invalid pool policy: {msg}"),
        }
    }
}

impl std::error::Error for PoolError {}

/// A concurrent, fill-first rotating pool of upstream keys.
pub struct KeyPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    state: RwLock<PoolState>,
    policy: PoolPolicy,
    jitter: Arc<dyn Jitter>,
    next_id: std::sync::atomic::AtomicU64,
}

struct PoolState {
    entries: Vec<KeyEntry>,
    cursor: usize,
}

struct KeyEntry {
    key: Secret,
    fingerprint: String,
    cooling_until: Option<tokio::time::Instant>,
    /// Selection id that applied the current `cooling_until`, if any. A success
    /// may only clear a cooldown it applied itself; a stale success from an
    /// older selection must leave a newer cooldown untouched.
    cooling_by: Option<u64>,
    consecutive_failures: u32,
    backoff_step: u32,
    /// Selection id of the most recent report or half-open claim that actually
    /// changed this key's failure, backoff, or cooldown state. A success from
    /// an older selection (smaller id) is a total no-op — it can never un-do a
    /// newer report's state change, even when no cooldown is currently active
    /// (e.g. network failures counted below the breaker threshold). Only a
    /// later or current success resets the state and clears this marker.
    last_failure_by: Option<u64>,
    /// True while this key is recovering from a circuit-breaker trip and has
    /// not yet been confirmed healthy by a probe. A recovering key is
    /// selectable by exactly one request: selecting it claims the key by
    /// assigning a fresh `breaker_cooldown` deadline owned by that selection's
    /// id, so every other selector skips it. A successful probe clears the
    /// flag; a failed probe re-cools the key; an unreported (dropped) probe
    /// lets the claim deadline elapse and the key become eligible again — it
    /// can never wedge forever.
    half_open: bool,
    liveness_cooling: HashMap<String, tokio::time::Instant>,
}

impl KeyPool {
    /// Build a pool from a non-empty key list. Keys with identical contents
    /// must have been rejected upstream; they are matched by fingerprint.
    pub fn new(keys: Vec<Secret>, policy: PoolPolicy) -> Result<KeyPool, PoolError> {
        KeyPool::with_jitter(keys, policy, Arc::new(UniformJitter::default()))
    }

    /// Same as [`KeyPool::new`] with an explicit jitter seam.
    pub fn with_jitter(
        keys: Vec<Secret>,
        policy: PoolPolicy,
        jitter: Arc<dyn Jitter>,
    ) -> Result<KeyPool, PoolError> {
        policy.validate()?;
        if keys.is_empty() {
            return Err(PoolError::EmptyKeys);
        }
        let entries = keys
            .into_iter()
            .map(|key| KeyEntry {
                fingerprint: fingerprint(key.as_str()),
                key,
                cooling_until: None,
                cooling_by: None,
                consecutive_failures: 0,
                backoff_step: 0,
                last_failure_by: None,
                half_open: false,
                liveness_cooling: HashMap::new(),
            })
            .collect();
        Ok(KeyPool {
            inner: Arc::new(PoolInner {
                state: RwLock::new(PoolState { entries, cursor: 0 }),
                policy,
                jitter,
                next_id: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }

    /// Atomically swap the candidate key set (hot reload). State of keys that
    /// are still present is preserved; removed keys are no longer selected;
    /// new keys start fresh. Already-held leases remain valid.
    pub fn replace_keys(&self, keys: Vec<Secret>) -> Result<(), PoolError> {
        if keys.is_empty() {
            return Err(PoolError::EmptyKeys);
        }
        let mut state = write_state(&self.inner);
        let old = &state.entries;
        let entries: Vec<KeyEntry> = keys
            .into_iter()
            .map(|key| {
                let fingerprint = fingerprint(key.as_str());
                let prev = old.iter().find(|e| e.fingerprint == fingerprint);
                KeyEntry {
                    fingerprint,
                    key,
                    cooling_until: prev.and_then(|e| e.cooling_until),
                    cooling_by: prev.and_then(|e| e.cooling_by),
                    consecutive_failures: prev.map_or(0, |e| e.consecutive_failures),
                    backoff_step: prev.map_or(0, |e| e.backoff_step),
                    last_failure_by: prev.and_then(|e| e.last_failure_by),
                    half_open: prev.is_some_and(|e| e.half_open),
                    liveness_cooling: prev.map(|e| e.liveness_cooling.clone()).unwrap_or_default(),
                }
            })
            .collect();
        state.cursor = state.cursor.min(entries.len().saturating_sub(1));
        state.entries = entries;
        Ok(())
    }
}

impl Clone for KeyPool {
    fn clone(&self) -> Self {
        KeyPool {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for KeyPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = read_state(&self.inner);
        let fingerprints: Vec<&str> = state
            .entries
            .iter()
            .map(|e| e.fingerprint.as_str())
            .collect();
        f.debug_struct("KeyPool")
            .field("keys", &fingerprints)
            .field("cursor", &state.cursor)
            .finish()
    }
}

fn read_state(inner: &PoolInner) -> std::sync::RwLockReadGuard<'_, PoolState> {
    inner.state.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_state(inner: &PoolInner) -> std::sync::RwLockWriteGuard<'_, PoolState> {
    inner.state.write().unwrap_or_else(PoisonError::into_inner)
}

/// A key lease bound to a single request. The key is fixed once selected and
/// gives controlled read-only access; the raw secret is never exposed via
/// `Debug`. The `id` is a unique per-selection sequence number so a report can
/// tell which request applied a given cooldown.
#[derive(Debug, Clone)]
pub struct Selection {
    fingerprint: String,
    key: Secret,
    id: u64,
}

impl Selection {
    /// Key fingerprint (SHA-256 hex prefix), the stable identity.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Controlled read-only access to the selected key.
    pub fn key(&self) -> &Secret {
        &self.key
    }
}

/// Result of an attempt to acquire a key for a request.
#[derive(Debug)]
pub enum AttemptResult {
    Selected(Selection),
    Unavailable { retry_after: Duration },
    Exhausted,
}

/// Dimension of a Go usage limit (derived from the upstream error payload).
/// Carries no message/workspace content; `Debug` is always safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageDimension {
    Weekly,
    Monthly,
    FiveHour,
}

/// Classification of an upstream failure reported for a selection.
#[derive(Debug, Clone, Copy)]
pub enum Failure {
    /// 429 with a recognized GoUsageLimitError: the key is usage-limited for
    /// the whole `cooldown`. The short Retry-After header is ignored and the
    /// cooldown is the usage-reset duration, NOT capped by `backoff_max`.
    UsageLimit {
        dimension: UsageDimension,
        cooldown: Duration,
    },
    /// 429 without a recognized usage-limit payload: `retry_after` is honored
    /// when present and positive, otherwise exponential backoff + jitter
    /// applies (capped at `backoff_max`).
    RateLimited { retry_after: Option<Duration> },
    /// 401 / 403: the key is unusable now. `retry_after` is honored when
    /// present and positive, otherwise exponential backoff applies.
    Unavailable { retry_after: Option<Duration> },
    /// 5xx: the request may fail over to another key, but the key itself is
    /// neither cooled nor circuit-broken.
    Server,
    /// Network/other key failure: counts toward the consecutive-failure
    /// circuit breaker. A trip puts the key into a half-open state: after the
    /// cooldown, exactly one request may probe it before it is usable again.
    Network,
}

/// A key candidate captured when the request started. The candidate set is
/// pinned to the key generation that existed at construction (hot reload never
/// leaks a newly-added key into an old request), but cooldown deadlines are
/// always read LIVE from the pool: a key cooled after this request was created
/// is skipped, even though the candidate itself is old.
struct RequestCandidate {
    fingerprint: String,
    key: Secret,
}

/// Request-scoped attempt tracker: at most `max_attempts` distinct keys per
/// request, never repeating a key already used. Selection happens only within
/// the candidate snapshot captured at construction — requests are pinned to the
/// key generation that existed when they started — but health (cooldown) is
/// always read from the live pool. The wait budget is a SINGLE deadline fixed
/// at construction, so all `next()` calls share one cumulative timeout.
pub struct RequestAttempts {
    pool: Arc<PoolInner>,
    candidates: Vec<RequestCandidate>,
    used: Vec<String>,
    max: usize,
    deadline: tokio::time::Instant,
    model: Option<String>,
}

impl KeyPool {
    /// Begin a request-scoped attempt series against this pool. The candidates
    /// (keys + their cooldown deadlines, in cursor order) are captured NOW:
    /// a request is pinned to the generation that existed when it started, so a
    /// concurrent `replace_keys` can never leak a newly-added key into it.
    pub fn request(&self) -> RequestAttempts {
        self.request_with_model(None)
    }

    pub fn request_for_model(&self, model: impl Into<String>) -> RequestAttempts {
        self.request_with_model(Some(model.into()))
    }

    fn request_with_model(&self, model: Option<String>) -> RequestAttempts {
        let state = read_state(&self.inner);
        let n = state.entries.len();
        let mut candidates = Vec::with_capacity(n);
        for off in 0..n {
            let idx = (state.cursor + off) % n;
            let e = &state.entries[idx];
            candidates.push(RequestCandidate {
                fingerprint: e.fingerprint.clone(),
                key: e.key.clone(),
            });
        }
        RequestAttempts {
            pool: Arc::clone(&self.inner),
            candidates,
            used: Vec::new(),
            max: self.inner.policy.max_attempts,
            deadline: deadline_after(tokio::time::Instant::now(), self.inner.policy.wait_timeout),
            model,
        }
    }

    pub fn report_liveness_failure(&self, selection: &Selection, model: &str) {
        let mut state = write_state(&self.inner);
        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|entry| entry.fingerprint == selection.fingerprint)
        else {
            return;
        };
        entry.liveness_cooling.insert(
            model.to_string(),
            deadline_after(
                tokio::time::Instant::now(),
                self.inner.policy.backoff_initial,
            ),
        );
    }

    /// True if at least one key is currently selectable (not cooling).
    pub fn has_available_key(&self) -> bool {
        let state = read_state(&self.inner);
        let now = tokio::time::Instant::now();
        state
            .entries
            .iter()
            .any(|e| !e.cooling_until.is_some_and(|d| d > now))
    }

    /// Report a successful request for `selection`: reset failure/backoff state.
    /// Fill-first keeps the current key; only failures advance the cursor. A
    /// success from an older selection never touches state that a newer report
    /// applied to the key — the cooldown, its `consecutive_failures`, its
    /// `backoff_step`, and the half-open marker are all left intact, even when
    /// no cooldown is currently active. Only a later or current success (whose
    /// selection id is not older than `last_failure_by`) resets the key's state
    /// normally and clears the marker. A success on a half-open probe confirms
    /// the key healthy and clears the half-open marker.
    pub fn report_success(&self, selection: &Selection) {
        let mut state = write_state(&self.inner);
        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|e| e.fingerprint == selection.fingerprint)
        else {
            return;
        };
        if entry.last_failure_by.is_some_and(|id| id > selection.id) {
            return;
        }
        entry.backoff_step = 0;
        entry.consecutive_failures = 0;
        entry.cooling_until = None;
        entry.cooling_by = None;
        entry.half_open = false;
        entry.last_failure_by = None;
    }

    /// Cool a live key based on a proactive usage observation. The report
    /// participates in the same monotonic report ordering as request results.
    pub fn report_proactive_cooldown(&self, fingerprint: &str, cooldown: Duration) {
        let now = tokio::time::Instant::now();
        let deadline = deadline_after(now, cooldown);
        let mut state = write_state(&self.inner);
        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|entry| entry.fingerprint == fingerprint)
        else {
            return;
        };
        // Allocate while holding the same state lock used by selection. This
        // keeps id order aligned with state-commit order, so a selection that
        // truly predates this report can never look newer merely because the
        // reporter waited for the lock after reserving its id.
        let report_id = self
            .inner
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !entry
            .cooling_until
            .is_some_and(|existing| existing > deadline)
        {
            entry.cooling_until = Some(deadline);
            entry.cooling_by = Some(report_id);
            entry.last_failure_by = Some(report_id);
            advance_cursor(&mut state, now);
        }
    }

    /// Report a failed request for `selection`.
    pub fn report_failure(&self, selection: &Selection, failure: Failure) {
        let mut state = write_state(&self.inner);
        let now = tokio::time::Instant::now();
        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|e| e.fingerprint == selection.fingerprint)
        else {
            return;
        };
        match failure {
            Failure::UsageLimit { cooldown, .. } => {
                let new_deadline = deadline_after(now, cooldown);
                if !entry.cooling_until.is_some_and(|d| d > new_deadline) {
                    entry.cooling_until = Some(new_deadline);
                    entry.cooling_by = Some(selection.id);
                    entry.last_failure_by = Some(selection.id);
                    advance_cursor(&mut state, now);
                }
            }
            Failure::RateLimited { retry_after } | Failure::Unavailable { retry_after } => {
                let (cooldown, backed_off) = match retry_after {
                    Some(ra) if ra > Duration::ZERO => (ra, false),
                    _ => {
                        let base = backoff_at(&self.inner.policy, entry.backoff_step);
                        (
                            self.inner
                                .jitter
                                .jitter(base)
                                .min(self.inner.policy.backoff_max),
                            true,
                        )
                    }
                };
                let new_deadline = deadline_after(now, cooldown);
                // A report never shortens an existing cooldown: a short generic
                // 429 must not shrink a longer usage cooldown (and an absorbed
                // report does not advance the backoff step either).
                if !entry.cooling_until.is_some_and(|d| d > new_deadline) {
                    if backed_off {
                        entry.backoff_step += 1;
                    }
                    entry.cooling_until = Some(new_deadline);
                    entry.cooling_by = Some(selection.id);
                    entry.last_failure_by = Some(selection.id);
                    advance_cursor(&mut state, now);
                }
            }
            Failure::Server => {}
            Failure::Network => {
                entry.consecutive_failures += 1;
                entry.last_failure_by = Some(selection.id);
                if entry.consecutive_failures >= self.inner.policy.breaker_threshold {
                    let new_deadline = deadline_after(now, self.inner.policy.breaker_cooldown);
                    if !entry.cooling_until.is_some_and(|d| d > new_deadline) {
                        entry.cooling_until = Some(new_deadline);
                        entry.cooling_by = Some(selection.id);
                        entry.consecutive_failures = 0;
                        entry.half_open = true;
                        advance_cursor(&mut state, now);
                    }
                }
            }
        }
    }
}

fn advance_cursor(state: &mut PoolState, now: tokio::time::Instant) {
    let n = state.entries.len();
    if n == 0 {
        return;
    }
    for off in 1..=n {
        let idx = (state.cursor + off) % n;
        if !state.entries[idx].cooling_until.is_some_and(|d| d > now) {
            state.cursor = idx;
            return;
        }
    }
}

/// Convert a cooldown duration into a deadline, clamped to [`MAX_COOLDOWN`]
/// first so the addition can never overflow `Instant`. A bare `now + duration`
/// panics on an enormous (but constructible) external Retry-After or
/// `GoUsageLimitError` reset; with `panic = "abort"` in release that would
/// terminate the whole gateway. `MAX_COOLDOWN` is far below the representable
/// range, so `checked_add` after the clamp is guaranteed to succeed, and every
/// deadline derived here yields a finite, serializable Retry-After downstream.
fn deadline_after(now: tokio::time::Instant, cooldown: Duration) -> tokio::time::Instant {
    now.checked_add(cooldown.min(MAX_COOLDOWN))
        .expect("MAX_COOLDOWN is always addable to a real Instant")
}

fn backoff_at(policy: &PoolPolicy, step: u32) -> Duration {
    let mut d = policy.backoff_initial;
    for _ in 0..step.min(20) {
        if d >= policy.backoff_max {
            break;
        }
        d = d.saturating_mul(2).min(policy.backoff_max);
    }
    d.min(policy.backoff_max)
}

impl RequestAttempts {
    pub(crate) fn set_model(&mut self, model: impl Into<String>) {
        self.model = Some(model.into());
    }

    /// Select another distinct key only if one is available now. Once the
    /// gateway has a committed upstream response, an unrelated key cooldown
    /// must not delay or replace that response.
    pub(crate) fn next_immediate(&mut self) -> AttemptResult {
        if self.used.len() >= self.max {
            return AttemptResult::Exhausted;
        }
        match self.try_select_unused() {
            Some(sel) => {
                self.used.push(sel.fingerprint.clone());
                AttemptResult::Selected(sel)
            }
            None => AttemptResult::Exhausted,
        }
    }

    /// Select the next distinct key for this request, or report that none is
    /// currently available (`Unavailable`) or that the attempt budget for
    /// this request is used up (`Exhausted`).
    pub async fn next(&mut self) -> AttemptResult {
        if self.used.len() >= self.max {
            return AttemptResult::Exhausted;
        }
        let deadline = self.deadline;
        loop {
            if let Some(sel) = self.try_select_unused() {
                self.used.push(sel.fingerprint.clone());
                return AttemptResult::Selected(sel);
            }
            if !self.has_unused() {
                return AttemptResult::Exhausted;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                let earliest = self.earliest_recovery().unwrap_or(now);
                return AttemptResult::Unavailable {
                    retry_after: earliest.saturating_duration_since(now),
                };
            }
            let Some(earliest) = self.earliest_recovery() else {
                return AttemptResult::Unavailable {
                    retry_after: Duration::ZERO,
                };
            };
            if earliest <= now {
                continue;
            }
            tokio::time::sleep_until(earliest.min(deadline)).await;
        }
    }

    /// Select the first unused, non-cooling candidate in the captured snapshot.
    /// Cooldown is read LIVE from the pool: a key cooled after this request was
    /// created is skipped even though the candidate itself is old, and a key
    /// removed by `replace_keys` is unavailable — never selectable. A key
    /// recovering from the circuit breaker (half-open) is claimed atomically
    /// under the write lock by the single request that selects it: the claim
    /// assigns that selection's id and a fresh `breaker_cooldown` deadline, so
    /// every other selector skips it until the probe reports — or the deadline
    /// elapses on its own if the probe is dropped. The claim also records
    /// `last_failure_by`, so a late success from an earlier, dropped probe can
    /// never clear a newer probe's claim and reopen the key to concurrent
    /// selectors.
    fn try_select_unused(&self) -> Option<Selection> {
        let mut state = write_state(&self.pool);
        let now = tokio::time::Instant::now();
        let cand_idx = self.candidates.iter().position(|c| {
            if self.used.contains(&c.fingerprint) {
                return false;
            }
            match state
                .entries
                .iter()
                .find(|e| e.fingerprint == c.fingerprint)
            {
                Some(e) => {
                    !e.cooling_until.is_some_and(|d| d > now)
                        && !self.model.as_ref().is_some_and(|model| {
                            e.liveness_cooling
                                .get(model)
                                .is_some_and(|deadline| *deadline > now)
                        })
                }
                None => false,
            }
        })?;
        let cand = &self.candidates[cand_idx];
        let sel = Selection {
            fingerprint: cand.fingerprint.clone(),
            key: cand.key.clone(),
            id: self
                .pool
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };
        if let Some(entry) = state
            .entries
            .iter_mut()
            .find(|e| e.fingerprint == cand.fingerprint)
        {
            if entry.half_open {
                entry.cooling_until = Some(deadline_after(now, self.pool.policy.breaker_cooldown));
                entry.cooling_by = Some(sel.id);
                // The claim is a cooldown-state change owned by this selection:
                // record it so a late success from an older probe (whose claim
                // lapsed unreported) can never clear a newer probe's claim.
                entry.last_failure_by = Some(sel.id);
            }
        }
        Some(sel)
    }

    fn has_unused(&self) -> bool {
        self.candidates
            .iter()
            .any(|c| !self.used.contains(&c.fingerprint))
    }

    fn earliest_recovery(&self) -> Option<tokio::time::Instant> {
        let state = read_state(&self.pool);
        let now = tokio::time::Instant::now();
        self.candidates
            .iter()
            .filter_map(|c| {
                let entry = state
                    .entries
                    .iter()
                    .find(|e| e.fingerprint == c.fingerprint)?;
                let model_deadline = self
                    .model
                    .as_ref()
                    .and_then(|model| entry.liveness_cooling.get(model).copied());
                match (entry.cooling_until, model_deadline) {
                    (Some(global), Some(model)) => Some(global.max(model)),
                    (Some(global), None) => Some(global),
                    (None, Some(model)) => Some(model),
                    (None, None) => None,
                }
            })
            .filter(|d| *d > now)
            .min()
    }
}
