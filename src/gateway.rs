use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONNECTION, CONTENT_TYPE, RETRY_AFTER,
};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use subtle::ConstantTimeEq;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

use crate::audit::{
    AttemptSummaries, AttemptSummary, AttemptTerminalReason, AuditError, AuditOutcome, AuditRecord,
    Outcome,
};
use crate::config::{
    upstream_api_url, EventTimeouts, GatewayKey, Secret, UpstreamApi, MAX_MODEL_BYTES,
};
use crate::pool::{AttemptResult, Failure, KeyPool, UsageDimension};
use crate::queue::{AdmissionError, AdmissionQueue, Permit};
use crate::usage::UsageSnapshotStore;

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Maximum bytes of an error body read for 429 classification. A body whose
/// EOF lands at or before this cap is buffered whole and passed through
/// byte-for-byte; once more than this is pending without EOF, classification
/// degrades to a generic rate-limit and the final response forwards the
/// buffered prefix followed by the still-unread upstream body stream (never
/// buffered whole).
const ERROR_CLASSIFY_CAP: usize = 64 * 1024;

/// Result of classifying an upstream 429 error body.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RateLimitKind {
    /// A recognized GoUsageLimitError with a concrete dimension + cooldown.
    UsageLimit {
        dimension: UsageDimension,
        cooldown: Duration,
    },
    /// Any other rate limit: fall back to Retry-After / exponential backoff.
    Generic,
}

/// Classify an upstream 429 error body. Only a body that is fully read (not
/// overflowed), valid UTF-8, parses as JSON, carries `error.type ==
/// "GoUsageLimitError"` exactly, and a `metadata.limitName` of
/// weekly/monthly/5h is treated as a usage limit. Anything else is `Generic`.
fn classify_429(body: &[u8], overflowed: bool) -> RateLimitKind {
    if overflowed {
        return RateLimitKind::Generic;
    }
    let text = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(_) => return RateLimitKind::Generic,
    };
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return RateLimitKind::Generic,
    };
    if value["error"]["type"].as_str() != Some("GoUsageLimitError") {
        return RateLimitKind::Generic;
    }
    let dimension = match value["metadata"]["limitName"].as_str() {
        Some("weekly") => UsageDimension::Weekly,
        Some("monthly") => UsageDimension::Monthly,
        Some("5h") => UsageDimension::FiveHour,
        _ => return RateLimitKind::Generic,
    };
    let message = value["error"]["message"].as_str().unwrap_or_default();
    let cooldown = parse_resets_in(message).unwrap_or_else(|| match dimension {
        UsageDimension::Weekly => Duration::from_secs(7 * 24 * 3600),
        UsageDimension::Monthly => Duration::from_secs(31 * 24 * 3600),
        UsageDimension::FiveHour => Duration::from_secs(5 * 3600),
    });
    RateLimitKind::UsageLimit {
        dimension,
        cooldown,
    }
}

/// Strictly parse "Resets in N unit(s)" from the usage-limit message.
/// Case-insensitive integer `seconds|minutes|hours|days`; zero/negative,
/// fractional, overflow, unknown units or a missing phrase all return `None`.
/// URLs/workspace tokens are never interpreted.
fn parse_resets_in(message: &str) -> Option<Duration> {
    let lower = message.to_ascii_lowercase();
    let idx = lower.find("resets in")?;
    let mut rest = &lower[idx + "resets in".len()..];
    rest = rest.trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    let count: u64 = digits.parse().ok()?;
    if count == 0 {
        return None;
    }
    rest = rest[digits.len()..].trim_start();
    let unit: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    let unit = unit.trim_end_matches('s');
    let secs = match unit {
        "second" => count.checked_mul(1)?,
        "minute" => count.checked_mul(60)?,
        "hour" => count.checked_mul(3600)?,
        "day" => count.checked_mul(86_400)?,
        _ => return None,
    };
    Some(Duration::from_secs(secs))
}

/// Where the gateway writes one audit record per proxied request.
#[async_trait]
pub trait AuditSink: Send + Sync {
    fn record(&self, record: AuditRecord) -> Outcome;

    /// Reopen the underlying audit log at `path`. A failed open returns `Err`
    /// and keeps the previous destination active. Awaiting guarantees records
    /// offered afterwards land in the reopened file.
    async fn reopen(&self, path: &Path) -> Result<(), AuditError>;
}

/// Bounded I/O timeouts for the non-streaming phases of a request. Read once
/// at construction: these are non-hot config, so changing them requires a
/// restart. After upstream response headers arrive, SSE reads stay unwrapped —
/// SSE streams run as long as the upstream keeps them open, the only active
/// bound being [`IoTimeouts::response_write`], a per-chunk budget on delivering
/// bytes to a slow client. A finite non-SSE success body is instead idle-bounded
/// per read by [`IoTimeouts::upstream_error_body`], so a stalled partial JSON
/// body cannot hold the admission permit and its upstream connection forever.
#[derive(Debug, Clone, Copy)]
pub struct IoTimeouts {
    /// Bound on reading a client request body; a stalled upload is rejected
    /// and its permit released after this long.
    pub body_read: Duration,
    /// Bound on waiting for upstream response headers after sending.
    pub upstream_header: Duration,
    /// Bound from upstream response headers to the first complete SSE data
    /// event. The response remains uncommitted during this phase so another
    /// key may be attempted safely.
    pub first_event: Duration,
    /// Per-event idle bound after the first SSE event has been committed.
    /// Expiry terminates the stream but never splices in another attempt.
    pub inter_event: Duration,
    /// Independent bound on reading a retryable/final error body for
    /// classification. The classification prefix must reach the cap or EOF
    /// within this bound; a prefix that stalls is a network failure, never
    /// served as a partial upstream error. A body that overruns the cap
    /// degrades to a generic classification and is forwarded as the buffered
    /// prefix plus the live remainder stream. Also reused as the per-read idle
    /// bound on a committed non-SSE success body: a stalled partial JSON body
    /// is a network failure that ends the response and releases its permit.
    /// Never applied to SSE reads; SSE uses the first/inter-event deadlines.
    pub upstream_error_body: Duration,
    /// Per-chunk bound on forwarding a response chunk to a client that has
    /// stopped consuming it. The bounded 16-slot channel lets a slow (or
    /// stalled) client fill up; once full the pump parks on `tx.send`, and
    /// after this budget the send is abandoned: the stream ends, the upstream
    /// is cancelled and the admission permit released. A client that consumes
    /// at least once per budget keeps the stream alive — each completed send
    /// arms a fresh budget. Never applied to an SSE `upstream.next()`; non-SSE
    /// success reads are idle-bounded instead by
    /// [`IoTimeouts::upstream_error_body`].
    pub response_write: Duration,
}

impl Default for IoTimeouts {
    fn default() -> Self {
        IoTimeouts {
            body_read: Duration::from_secs(30),
            upstream_header: Duration::from_secs(60),
            first_event: Duration::from_secs(60),
            inter_event: Duration::from_secs(90),
            upstream_error_body: Duration::from_secs(5),
            response_write: Duration::from_secs(30),
        }
    }
}

/// The hot-reloadable runtime fields, captured as one consistent snapshot per
/// request. Fields here can be swapped atomically while the gateway runs.
#[derive(Debug, Clone)]
pub struct RuntimeState {
    pub gateway_keys: Vec<GatewayKey>,
    pub base_url: Url,
    pub max_body_bytes: usize,
    pub key_aliases: BTreeMap<String, String>,
    /// Current model list served by `GET /v1/models`, hot-reloadable.
    pub models: Vec<String>,
}

/// Single seam holding the current [`RuntimeState`] as an immutable snapshot.
/// The internal lock coordinates reads (one snapshot + pool request per
/// request) with atomic hot applies (key replacement + publish under one write
/// lock), so a request never observes a mix of old and new generations.
#[derive(Clone)]
pub struct RuntimeStore {
    inner: Arc<std::sync::RwLock<Arc<RuntimeState>>>,
}

impl RuntimeStore {
    pub fn new(state: RuntimeState) -> RuntimeStore {
        RuntimeStore {
            inner: Arc::new(std::sync::RwLock::new(Arc::new(state))),
        }
    }

    /// Snapshot of the current runtime state (token publish is also guarded by
    /// the write lock, so readers always see a fully-old or fully-new token).
    pub fn snapshot(&self) -> Arc<RuntimeState> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Grab the runtime snapshot AND build the request's key-attempt tracker in
    /// one read critical section, so they are consistent with the same pool
    /// generation. Synchronous only: no await while the lock is held.
    pub fn snapshot_and_request(
        &self,
        pool: &KeyPool,
    ) -> (Arc<RuntimeState>, crate::pool::RequestAttempts) {
        let guard = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (guard.clone(), pool.request())
    }

    /// Atomically publish a new runtime state (no key change).
    pub fn update(&self, state: RuntimeState) {
        *self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(state);
    }

    /// Atomically replace only the model allowlist, preserving the latest
    /// concurrently hot-reloaded token, URL and body limit.
    pub fn update_models(&self, models: Vec<String>) {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = (**guard).clone();
        state.models = models;
        *guard = Arc::new(state);
    }

    /// Atomically apply a hot reload across both the key pool and the runtime
    /// state: within one write critical section the pool keys are replaced
    /// first (a failure leaves everything untouched) and only then is the new
    /// state published. A concurrent [`RuntimeStore::snapshot_and_request`]
    /// therefore sees all-old or all-new, never a mix.
    pub fn update_with_keys(
        &self,
        pool: &KeyPool,
        keys: Vec<Secret>,
        state: RuntimeState,
    ) -> Result<(), crate::pool::PoolError> {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pool.replace_keys(keys)?;
        *guard = Arc::new(state);
        Ok(())
    }

    /// Test-only seam: like [`RuntimeStore::update_with_keys`] but blocks
    /// holding the write lock after the pool keys are replaced and before the
    /// state is published, so tests can deterministically probe the mid-update
    /// window. Not for production use.
    #[doc(hidden)]
    pub fn update_with_keys_holding(
        &self,
        pool: &KeyPool,
        keys: Vec<Secret>,
        state: RuntimeState,
        replaced: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<(), crate::pool::PoolError> {
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pool.replace_keys(keys)?;
        let _ = replaced.send(());
        let _ = release.recv();
        *guard = Arc::new(state);
        Ok(())
    }
}

/// Global budget on bytes of request bodies buffered in flight at once.
///
/// A `tokio` semaphore is sized in byte-units with capacity
/// `max_inflight_body_bytes`; every request reserves `permits_per_request`
/// (`limits.max_body_bytes`) before it reads its body and holds the bytes for
/// as long as the request still carries the complete body — through every
/// upstream attempt — releasing only once the request body has been handed to
/// the upstream. So 200 concurrent 10MiB bodies can no longer hold ~2GiB of
/// body memory — only `max_inflight_body_bytes` (default 256MiB) at most, and a
/// request is never counted while merely streaming the downstream response.
/// Config validation guarantees `permits_per_request <= capacity` and that both
/// fit `u32` (`acquire_many`'s permit unit).
#[derive(Clone)]
pub struct BodyBudget {
    sem: Arc<tokio::sync::Semaphore>,
    permits_per_request: u32,
    capacity: usize,
}

impl BodyBudget {
    /// Build a budget of `capacity` bytes where each request reserves
    /// `permits_per_request` bytes. Both must be non-zero and
    /// `permits_per_request <= capacity` (enforced by config validation).
    pub fn new(capacity: usize, permits_per_request: u32) -> BodyBudget {
        assert!(capacity > 0, "body budget capacity must be > 0");
        assert!(
            permits_per_request as usize <= capacity,
            "permits_per_request must not exceed the body budget capacity"
        );
        BodyBudget {
            sem: Arc::new(tokio::sync::Semaphore::new(capacity)),
            permits_per_request,
            capacity,
        }
    }

    /// Reserve this request's body bytes, waiting while the global budget is
    /// exhausted. The returned RAII permit releases the bytes when dropped.
    /// The caller wraps this wait in the same deadline as the body read, so
    /// waiting for the budget and reading the body share one absolute deadline.
    pub async fn acquire(&self) -> BodyPermit {
        let permit = self
            .sem
            .clone()
            .acquire_many_owned(self.permits_per_request)
            .await
            .expect("body budget semaphore is never closed");
        BodyPermit { _permit: permit }
    }

    /// Total budget capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Bytes still available to reserve right now.
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }
}

/// RAII byte reservation on a [`BodyBudget`]. Dropping it releases the bytes.
#[must_use]
pub struct BodyPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Runtime dependencies for the gateway routes.
pub struct GatewayState {
    pub(crate) http: reqwest::Client,
    pub(crate) pool: Arc<KeyPool>,
    pub(crate) queue: Arc<AdmissionQueue>,
    pub(crate) audit: Arc<dyn AuditSink>,
    pub(crate) runtime: RuntimeStore,
    pub(crate) body_budget: BodyBudget,
    pub(crate) stream_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) timeouts: IoTimeouts,
    pub(crate) model_event_timeouts: BTreeMap<String, EventTimeouts>,
    pub(crate) usage_snapshots: UsageSnapshotStore,
    pub(crate) started_at: tokio::time::Instant,
}

impl GatewayState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        http: reqwest::Client,
        base_url: Url,
        pool: Arc<KeyPool>,
        queue: Arc<AdmissionQueue>,
        gateway_token: Secret,
        models: Vec<String>,
        audit: Arc<dyn AuditSink>,
        max_body_bytes: usize,
        body_budget: BodyBudget,
        timeouts: IoTimeouts,
    ) -> GatewayState {
        GatewayState::with_runtime(
            http,
            RuntimeStore::new(RuntimeState {
                gateway_keys: vec![GatewayKey {
                    name: "legacy".into(),
                    token: gateway_token,
                }],
                base_url,
                max_body_bytes,
                key_aliases: BTreeMap::new(),
                models,
            }),
            pool,
            queue,
            audit,
            body_budget,
            timeouts,
        )
    }

    /// Same as [`GatewayState::new`] with an externally owned runtime store so
    /// tests and the hot-reload target can publish new snapshots.
    #[allow(clippy::too_many_arguments)]
    pub fn with_runtime(
        http: reqwest::Client,
        runtime: RuntimeStore,
        pool: Arc<KeyPool>,
        queue: Arc<AdmissionQueue>,
        audit: Arc<dyn AuditSink>,
        body_budget: BodyBudget,
        timeouts: IoTimeouts,
    ) -> GatewayState {
        let max_streams = (queue.snapshot().max_concurrency / 4).max(1);
        GatewayState {
            http,
            pool,
            queue,
            audit,
            runtime,
            body_budget,
            stream_slots: Arc::new(tokio::sync::Semaphore::new(max_streams)),
            timeouts,
            model_event_timeouts: BTreeMap::new(),
            usage_snapshots: UsageSnapshotStore::default(),
            started_at: tokio::time::Instant::now(),
        }
    }

    pub fn with_model_event_timeouts(
        mut self,
        model_event_timeouts: BTreeMap<String, EventTimeouts>,
    ) -> Self {
        self.model_event_timeouts = model_event_timeouts;
        self
    }

    pub fn with_usage_snapshots(mut self, snapshots: UsageSnapshotStore) -> Self {
        self.usage_snapshots = snapshots;
        self
    }

    fn event_timeouts_for(&self, model: &str) -> EventTimeouts {
        self.model_event_timeouts
            .get(model)
            .copied()
            .unwrap_or(EventTimeouts {
                first_event_timeout: self.timeouts.first_event,
                inter_event_timeout: self.timeouts.inter_event,
            })
    }
}

/// Build the gateway router.
pub fn build_router(state: GatewayState) -> Router {
    Router::new()
        .route("/healthz", get(healthz).fallback(method_not_allowed))
        .route("/readyz", get(readyz).fallback(method_not_allowed))
        .route("/v1/models", get(models).fallback(method_not_allowed))
        .route("/v1/status", get(status).fallback(method_not_allowed))
        .route(
            "/v1/chat/completions",
            post(chat_completions).fallback(method_not_allowed),
        )
        .route("/v1/messages", post(messages).fallback(method_not_allowed))
        .route(
            "/v1/responses",
            post(responses).fallback(method_not_allowed),
        )
        .fallback(not_found)
        .with_state(Arc::new(state))
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(state): State<Arc<GatewayState>>) -> Response {
    if state.queue.is_closed() {
        service_unavailable(Some(1))
    } else if !state.pool.has_available_key() {
        rate_limited(1)
    } else {
        StatusCode::OK.into_response()
    }
}

fn openai_error(
    status: StatusCode,
    message: &str,
    error_type: &str,
    code: Option<&str>,
) -> Response {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": error_type,
            "param": serde_json::Value::Null,
            "code": code,
        }
    });
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn service_unavailable(retry_after_secs: Option<u64>) -> Response {
    let mut resp = openai_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "Service Unavailable",
        "service_unavailable",
        None,
    );
    if let Some(secs) = retry_after_secs {
        resp.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            HeaderValue::from_str(&secs.to_string()).unwrap(),
        );
    }
    resp
}

fn rate_limited(retry_after_secs: u64) -> Response {
    let mut resp = openai_error(
        StatusCode::TOO_MANY_REQUESTS,
        "All upstream keys are temporarily rate limited",
        "rate_limit_error",
        Some("upstream_keys_unavailable"),
    );
    resp.headers_mut().insert(
        RETRY_AFTER,
        HeaderValue::from_str(&retry_after_secs.to_string()).unwrap(),
    );
    resp
}

fn unauthorized() -> Response {
    openai_error(
        StatusCode::UNAUTHORIZED,
        "Incorrect API key provided",
        "authentication_error",
        Some("invalid_api_key"),
    )
}

async fn method_not_allowed(
    State(state): State<Arc<GatewayState>>,
    req: Request<Body>,
) -> Response {
    let start = tokio::time::Instant::now();
    let request_id = request_id_for(req.headers());
    record_audit_rejected(
        &state,
        &request_id,
        StatusCode::METHOD_NOT_ALLOWED.as_u16(),
        start,
    );
    openai_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "Method Not Allowed",
        "invalid_request_error",
        None,
    )
}

async fn not_found(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    let start = tokio::time::Instant::now();
    let request_id = request_id_for(req.headers());
    record_audit_rejected(&state, &request_id, StatusCode::NOT_FOUND.as_u16(), start);
    openai_error(
        StatusCode::NOT_FOUND,
        "Not Found",
        "invalid_request_error",
        None,
    )
}

#[allow(clippy::result_large_err)]
fn check_auth(state: &GatewayState, headers: &HeaderMap) -> Result<String, Response> {
    let rt = state.runtime.snapshot();
    let header = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
    let given = header
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or_default();
    authenticate_gateway_key(&rt.gateway_keys, given).ok_or_else(unauthorized)
}

/// Compare every configured credential before returning, so key ordering does
/// not create an early-exit timing signal between identities.
fn authenticate_gateway_key(keys: &[GatewayKey], given: &str) -> Option<String> {
    let mut matched = None;
    for key in keys {
        if bool::from(given.as_bytes().ct_eq(key.token.as_str().as_bytes())) {
            matched = Some(key.name.clone());
        }
    }
    matched
}

#[allow(clippy::result_large_err)]
fn check_proxy_auth(
    state: &GatewayState,
    headers: &HeaderMap,
    api: UpstreamApi,
) -> Result<String, Response> {
    if let Ok(name) = check_auth(state, headers) {
        return Ok(name);
    }
    if matches!(api, UpstreamApi::Messages) {
        let rt = state.runtime.snapshot();
        let given = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if let Some(name) = authenticate_gateway_key(&rt.gateway_keys, given) {
            return Ok(name);
        }
    }
    Err(unauthorized())
}

async fn models(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    let start = tokio::time::Instant::now();
    let request_id = request_id_for(req.headers());
    if let Err(resp) = check_auth(&state, req.headers()) {
        record_audit_rejected(
            &state,
            &request_id,
            StatusCode::UNAUTHORIZED.as_u16(),
            start,
        );
        return resp;
    }
    // Read the current snapshot: a hot reload of the configured model list is
    // served to new requests immediately; nothing mutates in place.
    let rt = state.runtime.snapshot();
    let data: Vec<serde_json::Value> = rt
        .models
        .iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "object": "model",
                "created": 0,
                "owned_by": "orihsus",
            })
        })
        .collect();
    let body = serde_json::json!({ "object": "list", "data": data });
    record_audit_rejected(&state, &request_id, StatusCode::OK.as_u16(), start);
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn status(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    let start = tokio::time::Instant::now();
    let request_id = request_id_for(req.headers());
    if let Err(resp) = check_auth(&state, req.headers()) {
        record_audit_rejected(
            &state,
            &request_id,
            StatusCode::UNAUTHORIZED.as_u16(),
            start,
        );
        return resp;
    }

    let rt = state.runtime.snapshot();
    let wall_now = Utc::now();
    let keys: Vec<_> = state
        .pool
        .status_snapshot()
        .into_iter()
        .map(|key| {
            let usage = state.usage_snapshots.get(&key.usage_fingerprint);
            let cooling_until = key.cooling_remaining.and_then(|remaining| {
                chrono::Duration::from_std(remaining)
                    .ok()
                    .map(|duration| (wall_now + duration).to_rfc3339())
            });
            serde_json::json!({
                "id": key.id,
                "name": rt.key_aliases.get(&key.usage_fingerprint),
                "health": key.health,
                "cooling_reason": key.cooling_reason,
                "cooling_until": cooling_until,
                "usage_updated_at": usage.as_ref().map(|value| &value.timestamp),
                "usage": usage.as_ref().map(|value| serde_json::json!({
                    "rolling": value.rolling,
                    "weekly": value.weekly,
                    "monthly": value.monthly,
                })),
            })
        })
        .collect();
    let mut models = rt.models.clone();
    models.sort();
    let body = serde_json::json!({
        "keys": { "count": keys.len(), "items": keys },
        "models": { "count": models.len(), "allowlist": models },
        "service": {
            "version": env!("CARGO_PKG_VERSION"),
            "commit": env!("ORIHSUS_COMMIT_HASH"),
            "uptime_seconds": state.started_at.elapsed().as_secs(),
        },
    });
    record_audit_rejected(&state, &request_id, StatusCode::OK.as_u16(), start);
    (StatusCode::OK, axum::Json(body)).into_response()
}

enum BodyReadError {
    TooLarge,
    Invalid,
}

async fn read_limited(body: Body, limit: usize) -> Result<Bytes, BodyReadError> {
    let limited = http_body_util::Limited::new(body, limit);
    match http_body_util::BodyExt::collect(limited).await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(e) => {
            if e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                Err(BodyReadError::TooLarge)
            } else {
                Err(BodyReadError::Invalid)
            }
        }
    }
}

fn request_id_for(headers: &HeaderMap) -> String {
    let candidate = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim);
    let valid = candidate.is_some_and(|v| {
        !v.is_empty()
            && v.len() <= 128
            && v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    });
    match valid {
        true => candidate.unwrap().to_string(),
        false => generate_request_id(),
    }
}

fn generate_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{t:016x}{n:016x}")
}

fn extract_model(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["model"].as_str().map(|s| s.to_string()))
}

fn connection_named_headers(headers: &HeaderMap) -> HashSet<String> {
    let mut set = HashSet::new();
    for v in headers.get_all(CONNECTION) {
        if let Ok(s) = v.to_str() {
            for part in s.split(',') {
                set.insert(part.trim().to_ascii_lowercase());
            }
        }
    }
    set
}

fn is_hop_by_hop(lowercased: &str) -> bool {
    HOP_BY_HOP.contains(&lowercased)
}

fn is_sensitive_request_header(lowercased: &str) -> bool {
    matches!(
        lowercased,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "cookie2"
            | "x-api-key"
            | "api-key"
            | "x-real-ip"
            | "forwarded"
            | "x-forwarded-for"
            | "x-forwarded-host"
            | "x-forwarded-proto"
            | "x-forwarded-port"
            | "baggage"
            | "traceparent"
            | "tracestate"
            | "b3"
            | "x-ot-span-context"
            | "grpc-trace-bin"
    ) || lowercased.ends_with("-authorization")
        || lowercased.ends_with("-api-key")
        || lowercased.starts_with("x-forwarded-")
        || lowercased.starts_with("x-b3-")
}

fn should_forward_request_header(
    lowercased: &str,
    connection: &HashSet<String>,
    api: UpstreamApi,
) -> bool {
    if is_hop_by_hop(lowercased)
        || connection.contains(lowercased)
        || is_sensitive_request_header(lowercased)
    {
        return false;
    }

    // OpenCode's Go-provider request path currently emits client/project/
    // request/session; @opencode-ai/sdk also emits directory/workspace. Keep
    // the namespace open for compatible additions while the deny rules above
    // prevent credential-shaped names from riding the prefix rule.
    lowercased.starts_with("x-opencode-")
        || matches!(lowercased, "content-type" | "accept" | "user-agent")
        || (matches!(api, UpstreamApi::Messages)
            && matches!(lowercased, "anthropic-version" | "anthropic-beta"))
}

async fn forward_request(
    state: &GatewayState,
    sel: &crate::pool::Selection,
    headers: &HeaderMap,
    body: &Bytes,
    base_url: &Url,
    request_id: &str,
    api: UpstreamApi,
) -> Result<reqwest::Response, reqwest::Error> {
    let url = upstream_api_url(base_url, api);
    let mut rb = state.http.post(url).body(body.clone());
    // Preserve OpenCode client semantics through an explicit allowlist and
    // prefix rule. Sensitive classes take precedence over prefix allowance.
    let connection = connection_named_headers(headers);
    for (name, value) in headers {
        let lowercased = name.as_str().to_ascii_lowercase();
        if should_forward_request_header(&lowercased, &connection, api) {
            rb = rb.header(name, value);
        }
    }
    if matches!(api, UpstreamApi::Messages) {
        rb = rb.header("x-api-key", sel.key().as_str());
    } else {
        rb = rb.header(AUTHORIZATION, format!("Bearer {}", sel.key().as_str()));
    }
    rb = rb.header("x-request-id", request_id);
    rb.send().await
}

fn filter_response_headers(headers: &HeaderMap) -> HeaderMap {
    let connection = connection_named_headers(headers);
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let lname = name.as_str().to_ascii_lowercase();
        if is_hop_by_hop(&lname) || connection.contains(&lname) {
            continue;
        }
        if lname == "content-length" || lname == "transfer-encoding" {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn is_auth_unavailable(status: StatusCode) -> bool {
    matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
}

/// Parse an RFC 9110 `Retry-After` header value into a duration.
///
/// `delta-seconds` is honored as-is. An HTTP-date (`IMF-fixdate`) is resolved
/// against an explicit `now` — the production caller passes `SystemTime::now`,
/// and tests pin a fixed instant — and clamped to a non-negative duration, so
/// a past date degrades safely to a zero wait instead of a negative or
/// absurdly large one. Anything unparseable returns `None` and the caller
/// falls back to the default exponential backoff.
fn parse_retry_after(v: &HeaderValue, now: SystemTime) -> Option<Duration> {
    let v = v.to_str().ok()?.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let retry_at = parse_http_date(v)?;
    Some(
        retry_at
            .signed_duration_since(DateTime::<Utc>::from(now))
            .to_std()
            .unwrap_or(Duration::ZERO),
    )
}

/// Parse an RFC 9110 HTTP-date (`IMF-fixdate`, the RFC 5322 shape such as
/// `Fri, 14 Aug 2026 12:00:30 GMT`) into a UTC instant. Anything else — an
/// `obs-date`, garbage, an offset without a named zone — is rejected.
fn parse_http_date(v: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(v)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RequestUsage {
    input_tokens: u64,
    cached_tokens: u64,
    uncached_tokens: u64,
    cache_write_tokens: Option<u64>,
    output_tokens: u64,
    reasoning_tokens: Option<u64>,
}

fn extract_usage(body: &[u8]) -> Option<RequestUsage> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return None;
    };
    let usage = &v["usage"];
    let input_tokens = usage["prompt_tokens"]
        .as_u64()
        .or_else(|| usage["input_tokens"].as_u64())?;
    let output_tokens = usage["completion_tokens"]
        .as_u64()
        .or_else(|| usage["output_tokens"].as_u64())?;
    let cached_tokens = usage["prompt_tokens_details"]["cached_tokens"]
        .as_u64()
        .or_else(|| usage["prompt_cache_hit_tokens"].as_u64())
        .or_else(|| usage["cache_read_input_tokens"].as_u64())
        .unwrap_or(0)
        .min(input_tokens);
    let cache_write_tokens = usage["prompt_tokens_details"]["cache_write_tokens"]
        .as_u64()
        .or_else(|| usage["cache_write_tokens"].as_u64())
        .or_else(|| usage["cache_creation_input_tokens"].as_u64());
    let reasoning_tokens = usage["completion_tokens_details"]["reasoning_tokens"]
        .as_u64()
        .or_else(|| usage["output_tokens_details"]["reasoning_tokens"].as_u64())
        .or_else(|| usage["reasoning_tokens"].as_u64());
    Some(RequestUsage {
        input_tokens,
        cached_tokens,
        uncached_tokens: input_tokens - cached_tokens,
        cache_write_tokens,
        output_tokens,
        reasoning_tokens,
    })
}

const MAX_AUDIT_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Default)]
struct RequestAudit {
    gateway_key: Option<String>,
    opencode_session_id: Option<String>,
    opencode_project_id: Option<String>,
    opencode_request_id: Option<String>,
    attempts: AttemptSummaries,
}

impl RequestAudit {
    fn from_headers(headers: &HeaderMap) -> Self {
        fn bounded(headers: &HeaderMap, name: &str) -> Option<String> {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .filter(|value| value.len() <= MAX_AUDIT_ID_BYTES)
                .map(str::to_owned)
        }
        Self {
            gateway_key: None,
            opencode_session_id: bounded(headers, "x-opencode-session"),
            opencode_project_id: bounded(headers, "x-opencode-project"),
            opencode_request_id: bounded(headers, "x-opencode-request"),
            attempts: AttemptSummaries::default(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn record_audit(
    state: &GatewayState,
    request_id: &str,
    model: &str,
    key_fingerprint: &str,
    usage: Option<RequestUsage>,
    status: u16,
    start: tokio::time::Instant,
    outcome: Option<AuditOutcome>,
    audit: RequestAudit,
) {
    let record = AuditRecord {
        timestamp: Utc::now(),
        request_id: request_id.to_string(),
        model: Some(model.to_string()),
        key_fingerprint: Some(key_fingerprint.to_string()),
        gateway_key: audit.gateway_key.clone(),
        input_tokens: usage.map(|value| value.input_tokens),
        cached_tokens: usage.map(|value| value.cached_tokens),
        uncached_tokens: usage.map(|value| value.uncached_tokens),
        cache_write_tokens: usage.and_then(|value| value.cache_write_tokens),
        output_tokens: usage.map(|value| value.output_tokens),
        reasoning_tokens: usage.and_then(|value| value.reasoning_tokens),
        status,
        outcome,
        latency: start.elapsed(),
        opencode_session_id: audit.opencode_session_id,
        opencode_project_id: audit.opencode_project_id,
        opencode_request_id: audit.opencode_request_id,
        attempts: audit.attempts,
    };
    let _ = state.audit.record(record);
}

/// Audit a request rejected before any key or model was resolved (e.g. the
/// admission queue was full, or the request body was unreadable). `model`,
/// `key_fingerprint` and usage are unknown here and are recorded as JSON
/// `null` — never a forged empty string.
fn record_audit_rejected(
    state: &GatewayState,
    request_id: &str,
    status: u16,
    start: tokio::time::Instant,
) {
    record_audit_rejected_with_context(state, request_id, status, start, RequestAudit::default());
}

fn record_audit_rejected_with_context(
    state: &GatewayState,
    request_id: &str,
    status: u16,
    start: tokio::time::Instant,
    audit: RequestAudit,
) {
    let record = AuditRecord {
        timestamp: Utc::now(),
        request_id: request_id.to_string(),
        model: None,
        key_fingerprint: None,
        gateway_key: audit.gateway_key.clone(),
        input_tokens: None,
        cached_tokens: None,
        uncached_tokens: None,
        cache_write_tokens: None,
        output_tokens: None,
        reasoning_tokens: None,
        status,
        outcome: None,
        latency: start.elapsed(),
        opencode_session_id: audit.opencode_session_id,
        opencode_project_id: audit.opencode_project_id,
        opencode_request_id: audit.opencode_request_id,
        attempts: audit.attempts,
    };
    let _ = state.audit.record(record);
}

async fn chat_completions(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    proxy_request(state, req, UpstreamApi::ChatCompletions).await
}

async fn messages(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    proxy_request(state, req, UpstreamApi::Messages).await
}

async fn responses(State(state): State<Arc<GatewayState>>, req: Request<Body>) -> Response {
    proxy_request(state, req, UpstreamApi::Responses).await
}

async fn proxy_request(state: Arc<GatewayState>, req: Request<Body>, api: UpstreamApi) -> Response {
    let start = tokio::time::Instant::now();
    let mut request_audit = RequestAudit::from_headers(req.headers());
    if let Err(resp) = check_proxy_auth(&state, req.headers(), api) {
        // A rejected request is still audited exactly once. Only headers are
        // inspected for auth — the request body is never read — so model, key
        // and usage are unknown and recorded as JSON `null`; no secret or body
        // content can leak into the line.
        let request_id = request_id_for(req.headers());
        record_audit_rejected_with_context(
            &state,
            &request_id,
            StatusCode::UNAUTHORIZED.as_u16(),
            start,
            request_audit,
        );
        return resp;
    }
    let request_id = request_id_for(req.headers());
    let permit = match state.queue.acquire().await {
        Ok(p) => p,
        Err(e) => {
            // The request was rejected before any key/model was resolved
            // (e.g. admission queue full): still audit it, with model, key and
            // usage unknown (null).
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                start,
                request_audit,
            );
            return queue_error_response(e);
        }
    };
    // A queued request may have passed the first check before a hot token
    // rotation. Revalidate after admission, immediately before taking the
    // runtime/key snapshot and reading the body.
    match check_proxy_auth(&state, req.headers(), api) {
        Ok(name) => request_audit.gateway_key = Some(name),
        Err(resp) => {
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::UNAUTHORIZED.as_u16(),
                start,
                request_audit,
            );
            return resp;
        }
    }
    // One consistent (snapshot, key-pool) pair for this whole request, grabbed
    // atomically; in-flight SSE streams keep the pair they started with.
    let (rt, mut attempts) = state.runtime.snapshot_and_request(&state.pool);
    let (parts, body) = req.into_parts();
    // Reserve this request's body bytes and stream the body under ONE body_read
    // deadline: waiting for the global budget and reading the body share a
    // single absolute deadline, so a budget wait can never double the wait. The
    // RAII byte permit is returned together with the buffered body and held in
    // this handler for as long as the request still needs `body_bytes` — i.e.
    // through every upstream attempt — so the budget bounds all requests that
    // hold a complete body, not just the read phase. It is dropped the moment
    // `handle_upstream` yields a terminal outcome (the request body has been
    // handed to the upstream) and never follows the downstream response.
    let (body_bytes, body_permit) = match tokio::time::timeout(state.timeouts.body_read, async {
        let budget = state.body_budget.acquire().await;
        let bytes = read_limited(body, rt.max_body_bytes).await?;
        Ok::<(Bytes, BodyPermit), BodyReadError>((bytes, budget))
    })
    .await
    {
        Ok(Ok(ok)) => ok,
        Ok(Err(BodyReadError::TooLarge)) => {
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                start,
                request_audit,
            );
            return openai_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body too large",
                "invalid_request_error",
                None,
            );
        }
        Ok(Err(BodyReadError::Invalid)) => {
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::BAD_REQUEST.as_u16(),
                start,
                request_audit,
            );
            return openai_error(
                StatusCode::BAD_REQUEST,
                "Invalid request body",
                "invalid_request_error",
                None,
            );
        }
        Err(_) => {
            // Stalled client upload: bound the read, release the permit (dropped
            // on return) and tell the client to retry later.
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                start,
                request_audit,
            );
            return service_unavailable(Some(1));
        }
    };
    let Some(model) = extract_model(&body_bytes) else {
        record_audit_rejected_with_context(
            &state,
            &request_id,
            StatusCode::BAD_REQUEST.as_u16(),
            start,
            request_audit,
        );
        return openai_error(
            StatusCode::BAD_REQUEST,
            "Model must be one of the configured models",
            "invalid_request_error",
            Some("invalid_model"),
        );
    };
    if model.len() > MAX_MODEL_BYTES || !rt.models.iter().any(|allowed| allowed == &model) {
        record_audit_rejected_with_context(
            &state,
            &request_id,
            StatusCode::BAD_REQUEST.as_u16(),
            start,
            request_audit,
        );
        return openai_error(
            StatusCode::BAD_REQUEST,
            "Model must be one of the configured models",
            "invalid_request_error",
            Some("invalid_model"),
        );
    }
    attempts.set_model(model.clone());

    let mut last_response: Option<(ConsumedResponse, String)> = None;
    let final_outcome = loop {
        // Before any upstream response exists, waiting for a cooling candidate
        // can still produce service. After a committed error exists, retry only
        // a key available now: a different key's cooldown must not delay or
        // replace the response already received.
        let next_attempt = if last_response.is_some() {
            attempts.next_immediate()
        } else {
            attempts.next().await
        };
        let sel = match next_attempt {
            AttemptResult::Selected(s) => s,
            AttemptResult::Unavailable { retry_after } => {
                break FinalOutcome::Unavailable(retry_after)
            }
            AttemptResult::Exhausted => break FinalOutcome::LastResponse(last_response),
        };
        let fingerprint = sel.fingerprint().to_string();
        if let Some(previous) = request_audit.attempts.last_mut() {
            if previous.precommit && previous.failover_target.is_none() {
                previous.failover_target = Some(fingerprint.clone());
            }
        }
        let attempt_number = u8::try_from(request_audit.attempts.len() + 1)
            .expect("the pool permits at most two attempts");
        let attempt_start = tokio::time::Instant::now();
        // Bound only the send-to-headers phase. The timeout future is dropped
        // when it elapses, cancelling the attempt; the response body (SSE) is
        // never bounded here, so a long stream is not affected.
        let resp = match tokio::time::timeout(
            state.timeouts.upstream_header,
            forward_request(
                &state,
                &sel,
                &parts.headers,
                &body_bytes,
                &rt.base_url,
                &request_id,
                api,
            ),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => {
                request_audit.attempts.push(new_attempt_summary(
                    attempt_number,
                    fingerprint.clone(),
                    None,
                    AttemptTerminalReason::ResponseHeaderTimeout,
                ));
                // The upstream accepted the connection but never produced
                // response headers within the bound: classify as a network
                // failure and let the pool fail over to another key.
                state.pool.report_failure(&sel, Failure::Network);
                continue;
            }
        };
        match resp {
            Ok(resp) => {
                request_audit.attempts.push(new_attempt_summary(
                    attempt_number,
                    fingerprint.clone(),
                    Some(attempt_start.elapsed()),
                    AttemptTerminalReason::RetryableResponse,
                ));
                let status = resp.status();
                if status == StatusCode::TOO_MANY_REQUESTS {
                    // The status is already committed, so however the error body
                    // resolves this is a rate-limit, never a pre-status network
                    // failure. The Retry-After header is parsed up front so a
                    // failed body read can still honor it.
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| parse_retry_after(v, SystemTime::now()));
                    let consumed = match consume_classify_body(
                        resp,
                        state.timeouts.upstream_error_body,
                        attempt_start,
                        start,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(()) => {
                            // The error body stalled or errored before
                            // classification could complete: discard the
                            // partial body (never passed to the client),
                            // cool the key as RateLimited with the parsed
                            // Retry-After (else normal backoff) and fail
                            // over. The circuit breaker is untouched.
                            state
                                .pool
                                .report_failure(&sel, Failure::RateLimited { retry_after });
                            continue;
                        }
                    };
                    observe_consumed_error(request_audit.attempts.last_mut(), &consumed);
                    let failure = match classify_error_body(&consumed.body) {
                        RateLimitKind::UsageLimit {
                            dimension,
                            cooldown,
                        } => Failure::UsageLimit {
                            dimension,
                            cooldown,
                        },
                        RateLimitKind::Generic => Failure::RateLimited { retry_after },
                    };
                    state.pool.report_failure(&sel, failure);
                    last_response = Some((consumed, fingerprint));
                    continue;
                }
                if is_auth_unavailable(status) {
                    // Same principle as 429: a committed 401/403 is an
                    // Unavailable key regardless of how the error body resolves,
                    // so a stalled/errored body is never a pre-status network
                    // failure. The Retry-After header is parsed up front so a
                    // failed body read can still honor it.
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| parse_retry_after(v, SystemTime::now()));
                    let consumed = match consume_classify_body(
                        resp,
                        state.timeouts.upstream_error_body,
                        attempt_start,
                        start,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(()) => {
                            state
                                .pool
                                .report_failure(&sel, Failure::Unavailable { retry_after });
                            continue;
                        }
                    };
                    observe_consumed_error(request_audit.attempts.last_mut(), &consumed);
                    state
                        .pool
                        .report_failure(&sel, Failure::Unavailable { retry_after });
                    last_response = Some((consumed, fingerprint));
                    continue;
                }
                if status.is_server_error() {
                    let consumed = match consume_classify_body(
                        resp,
                        state.timeouts.upstream_error_body,
                        attempt_start,
                        start,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(()) => {
                            // The upstream already committed a 5xx response head:
                            // a stalled/errored error-body read is NOT a pre-status
                            // network failure, so the key is neither cooled nor
                            // circuit-broken (a retry may still fail over).
                            state.pool.report_failure(&sel, Failure::Server);
                            continue;
                        }
                    };
                    observe_consumed_error(request_audit.attempts.last_mut(), &consumed);
                    state.pool.report_failure(&sel, Failure::Server);
                    last_response = Some((consumed, fingerprint));
                    continue;
                }
                let prepared = match prepare_response(
                    resp,
                    state.event_timeouts_for(&model),
                    SSE_EVENT_CAP,
                    attempt_start,
                )
                .await
                {
                    Ok(prepared) => prepared,
                    Err(failure) => {
                        if let Some(summary) = request_audit.attempts.last_mut() {
                            summary.first_byte_latency = failure.first_byte_latency;
                            summary.upstream_bytes = failure.upstream_bytes;
                            summary.upstream_chunks = failure.upstream_chunks;
                            summary.last_activity_offset = failure.last_activity_offset;
                            summary.terminal_reason = failure.reason;
                        }
                        match failure.reason {
                            AttemptTerminalReason::NetworkError => {
                                state.pool.report_failure(&sel, Failure::Network)
                            }
                            AttemptTerminalReason::NoFirstEvent
                            | AttemptTerminalReason::EndBeforeFirstEvent => {
                                state.pool.report_liveness_failure(&sel, &model)
                            }
                            _ => {}
                        }
                        continue;
                    }
                };
                if let Some(summary) = request_audit.attempts.last_mut() {
                    summary.first_byte_latency = prepared.first_byte_latency;
                    summary.first_event_latency = prepared.first_event_latency;
                    summary.precommit = false;
                    summary.committed = true;
                    summary.terminal_reason = AttemptTerminalReason::Forwarded;
                }
                break FinalOutcome::Forward {
                    prepared,
                    fingerprint,
                    sel,
                    attempt_start,
                };
            }
            Err(_) => {
                request_audit.attempts.push(new_attempt_summary(
                    attempt_number,
                    fingerprint.clone(),
                    Some(attempt_start.elapsed()),
                    AttemptTerminalReason::NetworkError,
                ));
                state.pool.report_failure(&sel, Failure::Network);
                continue;
            }
        }
    };

    // The request body has been handed to the upstream and locally the buffered
    // body is no longer needed: release the byte budget before constructing or
    // streaming the response. The permit never follows a downstream SSE — only
    // requests still holding a complete body_bytes count against the budget.
    drop(body_permit);
    drop(body_bytes);

    match final_outcome {
        FinalOutcome::Unavailable(retry_after) => {
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::TOO_MANY_REQUESTS.as_u16(),
                start,
                request_audit,
            );
            rate_limited(retry_after_secs(retry_after))
        }
        // exhausted with no upstream response at all (only network errors)
        FinalOutcome::LastResponse(None) => {
            record_audit_rejected_with_context(
                &state,
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                start,
                request_audit,
            );
            service_unavailable(Some(1))
        }
        FinalOutcome::LastResponse(Some((consumed, fingerprint))) => {
            finalize_consumed_error(
                &state,
                consumed,
                permit,
                request_id,
                model,
                fingerprint,
                start,
                request_audit,
            )
            .await
        }
        FinalOutcome::Forward {
            prepared,
            fingerprint,
            sel,
            attempt_start,
        } => {
            finalize_response(
                &state,
                prepared,
                Some(sel),
                permit,
                request_id,
                model,
                fingerprint,
                start,
                request_audit,
                attempt_start,
            )
            .await
        }
    }
}

fn new_attempt_summary(
    attempt_number: u8,
    key_fingerprint: String,
    response_header_latency: Option<Duration>,
    terminal_reason: AttemptTerminalReason,
) -> AttemptSummary {
    AttemptSummary {
        attempt_number,
        key_fingerprint,
        response_header_latency,
        first_byte_latency: None,
        first_event_latency: None,
        upstream_bytes: 0,
        upstream_chunks: 0,
        upstream_events: 0,
        last_activity_offset: None,
        precommit: true,
        committed: false,
        terminal_reason,
        failover_target: None,
    }
}

enum FinalOutcome {
    Unavailable(Duration),
    LastResponse(Option<(ConsumedResponse, String)>),
    Forward {
        prepared: PreparedResponse,
        fingerprint: String,
        sel: crate::pool::Selection,
        attempt_start: tokio::time::Instant,
    },
}

fn is_streaming(resp: &reqwest::Response) -> bool {
    resp.headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase().starts_with("text/event-stream"))
        .unwrap_or(false)
}

type UpstreamByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>;

struct PreparedResponse {
    status: StatusCode,
    headers: HeaderMap,
    streaming: bool,
    upstream: UpstreamByteStream,
    prefetched: Vec<Bytes>,
    first_byte_latency: Option<Duration>,
    first_event_latency: Option<Duration>,
    inter_event_timeout: Duration,
}

struct PrefetchFailure {
    reason: AttemptTerminalReason,
    first_byte_latency: Option<Duration>,
    upstream_bytes: u64,
    upstream_chunks: u64,
    last_activity_offset: Option<Duration>,
}

async fn prepare_response(
    resp: reqwest::Response,
    event_timeouts: EventTimeouts,
    buffer_cap: usize,
    attempt_start: tokio::time::Instant,
) -> Result<PreparedResponse, PrefetchFailure> {
    let status = resp.status();
    let headers = filter_response_headers(resp.headers());
    let streaming = is_streaming(&resp);
    let mut upstream: UpstreamByteStream = Box::pin(resp.bytes_stream());
    if !streaming {
        return Ok(PreparedResponse {
            status,
            headers,
            streaming,
            upstream,
            prefetched: Vec::new(),
            first_byte_latency: None,
            first_event_latency: None,
            inter_event_timeout: event_timeouts.inter_event_timeout,
        });
    }

    let mut detector = SseUsageParser::new(buffer_cap);
    let mut prefetched = Vec::new();
    let mut total = 0usize;
    let mut chunks = 0u64;
    let mut first_byte_latency = None;
    let read = async {
        loop {
            match upstream.next().await {
                Some(Ok(bytes)) => {
                    let elapsed = attempt_start.elapsed();
                    first_byte_latency.get_or_insert(elapsed);
                    total = total.saturating_add(bytes.len());
                    chunks = chunks.saturating_add(1);
                    if total > buffer_cap {
                        return Err(AttemptTerminalReason::EndBeforeFirstEvent);
                    }
                    let events = detector.push(&bytes);
                    prefetched.push(bytes);
                    if events > 0 {
                        return Ok(elapsed);
                    }
                }
                Some(Err(_)) => return Err(AttemptTerminalReason::NetworkError),
                None => return Err(AttemptTerminalReason::EndBeforeFirstEvent),
            }
        }
    };
    match tokio::time::timeout(event_timeouts.first_event_timeout, read).await {
        Ok(Ok(first_event_latency)) => Ok(PreparedResponse {
            status,
            headers,
            streaming,
            upstream,
            prefetched,
            first_byte_latency,
            first_event_latency: Some(first_event_latency),
            inter_event_timeout: event_timeouts.inter_event_timeout,
        }),
        Ok(Err(reason)) => Err(PrefetchFailure {
            reason,
            first_byte_latency,
            upstream_bytes: u64::try_from(total).unwrap_or(u64::MAX),
            upstream_chunks: chunks,
            last_activity_offset: first_byte_latency,
        }),
        Err(_) => Err(PrefetchFailure {
            reason: AttemptTerminalReason::NoFirstEvent,
            first_byte_latency,
            upstream_bytes: u64::try_from(total).unwrap_or(u64::MAX),
            upstream_chunks: chunks,
            last_activity_offset: first_byte_latency,
        }),
    }
}

/// A retryable/final upstream error response whose body was consumed during
/// classification. Status, headers and body are all reconstructable so the
/// final error is passed through untouched.
struct ConsumedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: ErrorBody,
    first_byte_latency: Option<Duration>,
    upstream_bytes: u64,
    upstream_chunks: u64,
    last_activity_offset: Option<Duration>,
}

fn observe_consumed_error(summary: Option<&mut AttemptSummary>, consumed: &ConsumedResponse) {
    if let Some(summary) = summary {
        summary.first_byte_latency = consumed.first_byte_latency;
        summary.upstream_bytes = consumed.upstream_bytes;
        summary.upstream_chunks = consumed.upstream_chunks;
        summary.last_activity_offset = consumed.last_activity_offset;
    }
}

/// Result of a bounded classification read of an upstream error body.
enum ErrorBody {
    /// EOF reached at or before the classification cap: the complete body is
    /// buffered and can be passed through byte-for-byte.
    Buffered(Bytes),
    /// More than the cap was pending without EOF. The body is discarded after
    /// classification and never exposed to the client.
    Oversized,
}

/// Bounded classification read of an upstream error response's body. Reads at
/// most [`ERROR_CLASSIFY_CAP`] bytes of prefix: a body whose EOF lands within
/// the cap is buffered whole; a body that overruns the cap keeps the unread
/// remainder is discarded. `Err(())` means a mid-body connection failure; the
/// partial bytes are also discarded.
async fn consume_error_response(
    resp: reqwest::Response,
    attempt_start: tokio::time::Instant,
    request_start: tokio::time::Instant,
) -> Result<ConsumedResponse, ()> {
    let status = resp.status();
    let headers = filter_response_headers(resp.headers());
    let mut stream = resp.bytes_stream();
    let mut prefix: Vec<u8> = Vec::with_capacity(ERROR_CLASSIFY_CAP.min(4096));
    let mut first_byte_latency = None;
    let mut upstream_bytes = 0u64;
    let mut upstream_chunks = 0u64;
    let mut last_activity_offset = None;
    loop {
        let chunk = match stream.next().await {
            Some(Ok(c)) => c,
            Some(Err(_)) => return Err(()),
            None => {
                return Ok(ConsumedResponse {
                    status,
                    headers,
                    body: ErrorBody::Buffered(Bytes::from(prefix)),
                    first_byte_latency,
                    upstream_bytes,
                    upstream_chunks,
                    last_activity_offset,
                })
            }
        };
        let now = tokio::time::Instant::now();
        first_byte_latency.get_or_insert_with(|| now.duration_since(attempt_start));
        upstream_bytes =
            upstream_bytes.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        upstream_chunks = upstream_chunks.saturating_add(1);
        last_activity_offset = Some(now.duration_since(request_start));
        let remaining = ERROR_CLASSIFY_CAP - prefix.len();
        if chunk.len() <= remaining {
            prefix.extend_from_slice(&chunk);
        } else {
            prefix.extend_from_slice(&chunk[..remaining]);
            return Ok(ConsumedResponse {
                status,
                headers,
                body: ErrorBody::Oversized,
                first_byte_latency,
                upstream_bytes,
                upstream_chunks,
                last_activity_offset,
            });
        }
    }
}

/// Classify the body of a 429 response held by a [`ConsumedResponse`]. Only a
/// fully buffered (EOF within the cap) body can be a usage limit; an oversized
/// body always degrades to `Generic`.
fn classify_error_body(body: &ErrorBody) -> RateLimitKind {
    match body {
        ErrorBody::Buffered(bytes) => classify_429(bytes, false),
        ErrorBody::Oversized => RateLimitKind::Generic,
    }
}

/// Bounded classification read of a retryable/final upstream error body
/// (429/401/403/5xx). Reads at most [`ERROR_CLASSIFY_CAP`] bytes within
/// `timeout`: a prefix that stalls — never reaching the cap or EOF — times out
/// as `Err(())`, discarding the partial body and letting the caller cancel the
/// attempt. A body that reaches EOF within the cap is buffered whole; a body
/// that overruns the cap returns its buffered prefix plus the unread remainder
/// stream so the final response can forward it byte-for-byte.
async fn consume_classify_body(
    resp: reqwest::Response,
    timeout: Duration,
    attempt_start: tokio::time::Instant,
    request_start: tokio::time::Instant,
) -> Result<ConsumedResponse, ()> {
    match tokio::time::timeout(
        timeout,
        consume_error_response(resp, attempt_start, request_start),
    )
    .await
    {
        Ok(Ok(consumed)) => Ok(consumed),
        Ok(Err(())) | Err(_) => Err(()),
    }
}

/// Finalize a retryable upstream error that was consumed for classification:
/// pass status, filtered headers and the full body through, and audit it. A
/// fully buffered body is passed through byte-for-byte; an oversized body is
/// forwarded as the buffered prefix plus the live remainder stream (never
/// buffered whole), holding the permit until EOF or client drop.
#[allow(clippy::too_many_arguments)]
async fn finalize_consumed_error(
    state: &Arc<GatewayState>,
    consumed: ConsumedResponse,
    permit: Permit,
    request_id: String,
    model: String,
    fingerprint: String,
    start: tokio::time::Instant,
    audit: RequestAudit,
) -> Response {
    let status = consumed.status;
    record_audit(
        state,
        &request_id,
        &model,
        &fingerprint,
        None,
        status.as_u16(),
        start,
        None,
        audit,
    );
    drop(permit);
    let (message, error_type, code) = if status == StatusCode::TOO_MANY_REQUESTS {
        (
            "Upstream rate limit exceeded",
            "rate_limit_error",
            "rate_limit",
        )
    } else if is_auth_unavailable(status) {
        (
            "Upstream authentication failed",
            "authentication_error",
            "upstream_authentication",
        )
    } else {
        ("Upstream service error", "upstream_error", "upstream_error")
    };
    let mut response = openai_error(status, message, error_type, Some(code));
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).expect("validated request ID"),
    );
    if status == StatusCode::TOO_MANY_REQUESTS {
        if let Some(value) = consumed.headers.get(RETRY_AFTER) {
            response.headers_mut().insert(RETRY_AFTER, value.clone());
        }
    }
    response
}

/// Forward a final upstream response by streaming its body to the client. Both
/// SSE and non-streaming responses take the same path: bytes flow to the client
/// as they arrive (never buffered whole) while a bounded side-band parser
/// extracts usage. The one-time pool feedback (success on clean EOF, network
/// failure on a mid-stream upstream error, nothing on client cancel) and the
/// audit line are both decided in the streaming task once the body reaches its
/// terminal state — never optimistically at response-acceptance time, because
/// an upstream body error after the headers are committed must not be recorded
/// as a success.
#[allow(clippy::too_many_arguments)]
async fn finalize_response(
    state: &Arc<GatewayState>,
    prepared: PreparedResponse,
    sel: Option<crate::pool::Selection>,
    permit: Permit,
    request_id: String,
    model: String,
    fingerprint: String,
    start: tokio::time::Instant,
    audit: RequestAudit,
    attempt_start: tokio::time::Instant,
) -> Response {
    let streaming = prepared.streaming;
    let stream_permit = if streaming {
        match state.stream_slots.clone().try_acquire_owned() {
            Ok(slot) => Some(slot),
            Err(_) => {
                record_audit(
                    state,
                    &request_id,
                    &model,
                    &fingerprint,
                    None,
                    StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    start,
                    None,
                    audit,
                );
                drop(permit);
                return service_unavailable(Some(1));
            }
        }
    } else {
        None
    };
    let parser = if streaming {
        StreamUsageParser::Sse(SseUsageParser::new(SSE_EVENT_CAP))
    } else {
        StreamUsageParser::Json(JsonUsageParser::new(JSON_USAGE_CAP))
    };
    stream_response(
        state.clone(),
        prepared,
        sel,
        permit,
        request_id,
        model,
        fingerprint,
        start,
        parser,
        stream_permit,
        audit,
        attempt_start,
    )
    .await
}

fn retry_after_secs(duration: Duration) -> u64 {
    duration.as_secs_f64().ceil().max(1.0) as u64
}

fn queue_error_response(e: AdmissionError) -> Response {
    match e {
        AdmissionError::Full => service_unavailable(Some(1)),
        AdmissionError::Timeout => service_unavailable(Some(1)),
        // A self-produced 503 always carries Retry-After; closed admission is
        // no different.
        AdmissionError::Closed => service_unavailable(Some(1)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_response(
    state: Arc<GatewayState>,
    prepared: PreparedResponse,
    sel: Option<crate::pool::Selection>,
    permit: Permit,
    request_id: String,
    model: String,
    fingerprint: String,
    start: tokio::time::Instant,
    mut parser: StreamUsageParser,
    stream_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    mut audit: RequestAudit,
    attempt_start: tokio::time::Instant,
) -> Response {
    let status = prepared.status;
    let filtered = prepared.headers;
    let streaming = prepared.streaming;
    let upstream = prepared.upstream;
    let prefetched = prepared.prefetched;
    let initial_first_byte_latency = prepared.first_byte_latency;
    let initial_first_event_latency = prepared.first_event_latency;
    let inter_event_timeout = prepared.inter_event_timeout;

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let client_gone = Arc::new(tokio::sync::Notify::new());
    let task_gone = Arc::clone(&client_gone);
    let task_request_id = request_id.clone();
    let status_u16 = status.as_u16();

    tokio::spawn(async move {
        let mut upstream = upstream;
        let idle = if streaming {
            Some(inter_event_timeout)
        } else {
            Some(state.timeouts.upstream_error_body)
        };
        let result = tokio::select! {
            _ = task_gone.notified() => PumpResult::cancelled(),
            result = pump_upstream(
                &mut upstream,
                &tx,
                &mut parser,
                PumpPlan {
                    idle,
                    write_budget: state.timeouts.response_write,
                    attempt_start,
                    request_start: start,
                    prefetched,
                    initial_first_byte_latency,
                    initial_first_event_latency,
                    streaming,
                },
            ) => result,
        };
        let outcome = result.outcome;
        if let Some(summary) = audit.attempts.last_mut() {
            summary.first_byte_latency = result.metrics.first_byte_latency;
            summary.first_event_latency = result.metrics.first_event_latency;
            summary.upstream_bytes = result.metrics.upstream_bytes;
            summary.upstream_chunks = result.metrics.upstream_chunks;
            summary.upstream_events = result.metrics.upstream_events;
            summary.last_activity_offset = result.metrics.last_activity_offset;
            summary.terminal_reason = match outcome {
                PumpOutcome::Completed => AttemptTerminalReason::Completed,
                PumpOutcome::UpstreamError => AttemptTerminalReason::UpstreamError,
                PumpOutcome::ClientCancel => AttemptTerminalReason::ClientCancel,
                PumpOutcome::EventIdleTimeout => AttemptTerminalReason::EventIdleTimeout,
            };
        }
        // One-time pool feedback decided by the terminal state of the stream:
        // a mid-stream upstream error is a network failure (never a success —
        // the headers are already committed, so there is no retry); a client
        // cancel reports nothing.
        if let Some(sel) = &sel {
            match outcome {
                PumpOutcome::Completed => state.pool.report_success(sel),
                PumpOutcome::UpstreamError => state.pool.report_failure(sel, Failure::Network),
                PumpOutcome::ClientCancel => {}
                PumpOutcome::EventIdleTimeout => state.pool.report_liveness_failure(sel, &model),
            }
        }
        let usage = match outcome {
            PumpOutcome::Completed => parser.usage(),
            PumpOutcome::UpstreamError
            | PumpOutcome::ClientCancel
            | PumpOutcome::EventIdleTimeout => None,
        };
        let audit_outcome = match outcome {
            PumpOutcome::Completed => Some(AuditOutcome::Completed),
            PumpOutcome::UpstreamError => Some(AuditOutcome::UpstreamError),
            PumpOutcome::ClientCancel => Some(AuditOutcome::ClientCancel),
            PumpOutcome::EventIdleTimeout => Some(AuditOutcome::EventIdleTimeout),
        };
        record_audit(
            &state,
            &task_request_id,
            &model,
            &fingerprint,
            usage,
            status_u16,
            start,
            audit_outcome,
            audit,
        );
        drop(stream_permit);
        drop(permit);
        drop(upstream);
    });

    let stream = ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>);
    let body = Body::from_stream(DropNotifyStream::new(stream, client_gone));
    let mut rb = Response::builder().status(status);
    let mut last_name: Option<HeaderName> = None;
    for (name, value) in filtered {
        let name = match name {
            Some(n) => {
                last_name = Some(n.clone());
                n
            }
            None => last_name
                .clone()
                .expect("only the first header entry may lack a name"),
        };
        rb = rb.header(name, value);
    }
    rb = rb.header("x-request-id", &request_id);
    rb.body(body).unwrap()
}

/// Terminal state of the upstream body pump for a forwarded response. Drives
/// the one-time pool feedback and the audit outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpOutcome {
    /// The upstream body reached EOF: forward as a clean success.
    Completed,
    /// The upstream body stream errored after response headers were committed.
    /// The client already saw a response, so there is no retry; the key is a
    /// network failure, never a success.
    UpstreamError,
    /// The client dropped the response body — or stopped consuming it long
    /// enough for the per-chunk write budget to elapse (indistinguishable from
    /// the audit schema's perspective, so the same `client_cancel` terminal
    /// state is reused): no pool feedback either way.
    ClientCancel,
    /// A complete event was already committed, then no next event arrived in
    /// the configured window. The stream ends without failover.
    EventIdleTimeout,
}

#[derive(Debug, Default)]
struct PumpMetrics {
    first_byte_latency: Option<Duration>,
    first_event_latency: Option<Duration>,
    upstream_bytes: u64,
    upstream_chunks: u64,
    upstream_events: u64,
    last_activity_offset: Option<Duration>,
}

#[derive(Debug)]
struct PumpResult {
    outcome: PumpOutcome,
    metrics: PumpMetrics,
}

impl PumpResult {
    fn cancelled() -> Self {
        Self {
            outcome: PumpOutcome::ClientCancel,
            metrics: PumpMetrics::default(),
        }
    }
}

/// Offer one response chunk to the client channel under [`IoTimeouts::response_write`].
/// Returns `true` when the chunk was handed off (the client consumed a slot
/// within the budget, or the channel still had room), `false` when the client
/// dropped the body or stopped reading for the whole budget. Only the send is
/// bounded — never the upstream read — so a quiet SSE upstream is unaffected.
async fn send_with_timeout(
    tx: &tokio::sync::mpsc::Sender<Bytes>,
    bytes: Bytes,
    budget: Duration,
) -> bool {
    matches!(
        tokio::time::timeout(budget, tx.send(bytes)).await,
        Ok(Ok(()))
    )
}

struct PumpPlan {
    idle: Option<Duration>,
    write_budget: Duration,
    attempt_start: tokio::time::Instant,
    request_start: tokio::time::Instant,
    prefetched: Vec<Bytes>,
    initial_first_byte_latency: Option<Duration>,
    initial_first_event_latency: Option<Duration>,
    streaming: bool,
}

async fn pump_upstream(
    upstream: &mut (impl futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin),
    tx: &tokio::sync::mpsc::Sender<Bytes>,
    parser: &mut StreamUsageParser,
    plan: PumpPlan,
) -> PumpResult {
    let PumpPlan {
        idle,
        write_budget,
        attempt_start,
        request_start,
        prefetched,
        initial_first_byte_latency,
        initial_first_event_latency,
        streaming,
    } = plan;
    let mut metrics = PumpMetrics {
        first_byte_latency: initial_first_byte_latency,
        first_event_latency: initial_first_event_latency,
        ..PumpMetrics::default()
    };
    for bytes in prefetched {
        metrics.upstream_bytes = metrics
            .upstream_bytes
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        metrics.upstream_chunks = metrics.upstream_chunks.saturating_add(1);
        metrics.last_activity_offset = Some(request_start.elapsed());
        metrics.upstream_events = metrics.upstream_events.saturating_add(parser.push(&bytes));
        if !send_with_timeout(tx, bytes, write_budget).await {
            return PumpResult {
                outcome: PumpOutcome::ClientCancel,
                metrics,
            };
        }
    }
    let mut event_deadline = if streaming {
        idle.map(|idle| tokio::time::Instant::now() + idle)
    } else {
        None
    };
    loop {
        let next = match (event_deadline, idle) {
            (Some(deadline), _) => match tokio::time::timeout_at(deadline, upstream.next()).await {
                Err(_) => {
                    return PumpResult {
                        outcome: PumpOutcome::EventIdleTimeout,
                        metrics,
                    }
                }
                Ok(r) => r,
            },
            (None, Some(idle)) => match tokio::time::timeout(idle, upstream.next()).await {
                // A finite, content-length-delimited non-SSE body that stalls
                // mid-body is a network failure, never a success: the body can
                // never complete, so the key is failed and the stream ends.
                Err(_) => {
                    return PumpResult {
                        outcome: PumpOutcome::UpstreamError,
                        metrics,
                    }
                }
                Ok(r) => r,
            },
            (None, None) => upstream.next().await,
        };
        match next {
            Some(Ok(bytes)) => {
                let now = tokio::time::Instant::now();
                metrics
                    .first_byte_latency
                    .get_or_insert_with(|| now.duration_since(attempt_start));
                metrics.upstream_bytes = metrics
                    .upstream_bytes
                    .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                metrics.upstream_chunks = metrics.upstream_chunks.saturating_add(1);
                metrics.last_activity_offset = Some(now.duration_since(request_start));
                let events = parser.push(&bytes);
                metrics.upstream_events = metrics.upstream_events.saturating_add(events);
                if events > 0 && metrics.first_event_latency.is_none() {
                    metrics.first_event_latency = Some(now.duration_since(attempt_start));
                }
                if events > 0 {
                    event_deadline = idle.map(|idle| now + idle);
                }
                if !send_with_timeout(tx, bytes, write_budget).await {
                    return PumpResult {
                        outcome: PumpOutcome::ClientCancel,
                        metrics,
                    };
                }
            }
            Some(Err(_)) => {
                return PumpResult {
                    outcome: PumpOutcome::UpstreamError,
                    metrics,
                }
            }
            None => break,
        }
    }
    PumpResult {
        outcome: PumpOutcome::Completed,
        metrics,
    }
}

/// Maximum bytes of one SSE event buffered for usage extraction. An event
/// larger than this is marked for discard until its delimiter arrives.
const SSE_EVENT_CAP: usize = 256 * 1024;

/// Fixed small cap on the buffered bytes of a non-streaming JSON response body
/// used to extract usage. A body larger than this (or one that never reaches
/// EOF) is streamed through untouched but records null usage — the gateway
/// never accumulates the whole body.
const JSON_USAGE_CAP: usize = 64 * 1024;

/// Side-band usage parsers run alongside the forwarded body stream. Both are
/// bounded: they never retain more than a fixed cap of the streamed bytes.
enum StreamUsageParser {
    /// `text/event-stream`: incremental SSE parser extracting the final usage
    /// event.
    Sse(SseUsageParser),
    /// Non-streaming JSON: buffers at most [`JSON_USAGE_CAP`] bytes; usage is
    /// recorded only when the buffered body is complete JSON within the cap.
    Json(JsonUsageParser),
}

impl StreamUsageParser {
    fn push(&mut self, chunk: &[u8]) -> u64 {
        match self {
            StreamUsageParser::Sse(p) => p.push(chunk),
            StreamUsageParser::Json(p) => {
                p.push(chunk);
                0
            }
        }
    }

    fn usage(&self) -> Option<RequestUsage> {
        match self {
            StreamUsageParser::Sse(p) => p.usage(),
            StreamUsageParser::Json(p) => p.usage(),
        }
    }
}

/// Bounded side-band parser for a non-streaming (`application/json`) response
/// body. Buffers at most `cap` bytes; once exceeded the body is marked
/// overflowed and never accumulated. `usage()` returns the token counts only
/// when the buffered body is complete JSON (EOF reached) within the cap — a
/// huge or truncated body yields `None`.
struct JsonUsageParser {
    buf: Vec<u8>,
    cap: usize,
    overflowed: bool,
}

impl JsonUsageParser {
    fn new(cap: usize) -> Self {
        JsonUsageParser {
            buf: Vec::with_capacity(cap.min(4096)),
            cap,
            overflowed: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if self.overflowed {
            return;
        }
        let remaining = self.cap - self.buf.len();
        if chunk.len() <= remaining {
            self.buf.extend_from_slice(chunk);
        } else {
            self.buf.extend_from_slice(&chunk[..remaining]);
            self.buf.clear();
            self.overflowed = true;
        }
    }

    fn usage(&self) -> Option<RequestUsage> {
        if self.overflowed {
            return None;
        }
        extract_usage(&self.buf)
    }
}

/// Bounded incremental SSE parser: extracts the final usage event without
/// accumulating the whole stream. Handles LF (`\n\n`) and CRLF (`\r\n\r\n`)
/// delimiters split across arbitrary chunk boundaries. An event larger than
/// `event_cap` is marked for discard until its delimiter arrives, so a
/// truncated event is never mis-parsed.
struct SseUsageParser {
    event_buf: Vec<u8>,
    event_cap: usize,
    tail: [u8; 4],
    tail_len: usize,
    discarding: bool,
    usage: Option<RequestUsage>,
    event_count: u64,
}

impl SseUsageParser {
    fn new(event_cap: usize) -> Self {
        SseUsageParser {
            event_buf: Vec::with_capacity(event_cap.min(4096)),
            event_cap,
            tail: [0; 4],
            tail_len: 0,
            discarding: false,
            usage: None,
            event_count: 0,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> u64 {
        let before = self.event_count;
        for &b in chunk {
            if self.tail_len == 4 {
                self.tail.copy_within(1..4, 0);
                self.tail[3] = b;
            } else {
                self.tail[self.tail_len] = b;
                self.tail_len += 1;
            }
            let tail = &self.tail[..self.tail_len];
            if tail.ends_with(b"\n\n") || tail.ends_with(b"\r\n\r\n") {
                let event = std::mem::take(&mut self.event_buf);
                if !self.discarding && self.consume_event(&event) {
                    self.event_count = self.event_count.saturating_add(1);
                }
                self.discarding = false;
                self.tail_len = 0;
            } else if !self.discarding {
                if self.event_buf.len() < self.event_cap {
                    self.event_buf.push(b);
                } else {
                    self.discarding = true;
                }
            }
        }
        self.event_count - before
    }

    fn consume_event(&mut self, event: &[u8]) -> bool {
        let text = String::from_utf8_lossy(event);
        let data: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(str::trim)
            .collect();
        if data.is_empty() {
            return false;
        }
        let payload = data.join("\n");
        if payload == "[DONE]" {
            return true;
        }
        if serde_json::from_str::<serde_json::Value>(&payload).is_ok() {
            self.usage = extract_usage(payload.as_bytes()).or(self.usage);
        }
        true
    }

    fn usage(&self) -> Option<RequestUsage> {
        self.usage
    }
}

/// Fires `notify` once when the underlying stream is dropped (client gone).
struct DropNotifyStream<S> {
    inner: S,
    notify: Arc<tokio::sync::Notify>,
}

impl<S> DropNotifyStream<S> {
    fn new(inner: S, notify: Arc<tokio::sync::Notify>) -> Self {
        DropNotifyStream { inner, notify }
    }
}

impl<S> Drop for DropNotifyStream<S> {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl<S: futures_util::Stream + Unpin> futures_util::Stream for DropNotifyStream<S> {
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<S::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::UsageDimension;

    fn usage_body(limit: &str, message: &str) -> Vec<u8> {
        format!(
            r#"{{"type":"error","error":{{"type":"GoUsageLimitError","message":"{message}"}},"metadata":{{"workspace":"wrk_x","limitName":"{limit}"}}}}"#
        )
        .into_bytes()
    }

    fn usage(kind: RateLimitKind) -> bool {
        matches!(kind, RateLimitKind::UsageLimit { .. })
    }

    #[test]
    fn classify_detects_usage_limit_and_parses_resets_message() {
        let cases: &[(&str, &str, u64)] = &[
            (
                "weekly",
                "Weekly usage limit reached. Resets in 3 days.",
                3 * 24 * 3600,
            ),
            (
                "monthly",
                "Monthly quota exceeded. Resets in 5 hours.",
                5 * 3600,
            ),
            ("5h", "Limit hit. Resets in 45 minutes.", 45 * 60),
            ("weekly", "Limit hit. Resets in 90 seconds.", 90),
        ];
        for (limit, message, secs) in cases {
            let body = usage_body(limit, message);
            let kind = classify_429(&body, false);
            match kind {
                RateLimitKind::UsageLimit { cooldown, .. } => {
                    assert_eq!(cooldown, Duration::from_secs(*secs), "{message}")
                }
                _ => panic!("expected UsageLimit for {message}"),
            }
        }
    }

    #[test]
    fn classify_accepts_case_variations_and_units() {
        let body = usage_body("weekly", "Resets in 3 DAYS");
        match classify_429(&body, false) {
            RateLimitKind::UsageLimit { cooldown, .. } => {
                assert_eq!(cooldown, Duration::from_secs(3 * 24 * 3600))
            }
            _ => panic!("uppercase unit must parse"),
        }
    }

    #[test]
    fn classify_falls_back_to_dimension_duration_when_message_has_no_resets() {
        let weekly = classify_429(&usage_body("weekly", "Weekly usage limit reached."), false);
        assert_eq!(
            weekly,
            RateLimitKind::UsageLimit {
                dimension: UsageDimension::Weekly,
                cooldown: Duration::from_secs(7 * 24 * 3600)
            }
        );
        let monthly = classify_429(
            &usage_body("monthly", "Monthly usage limit reached."),
            false,
        );
        assert_eq!(
            monthly,
            RateLimitKind::UsageLimit {
                dimension: UsageDimension::Monthly,
                cooldown: Duration::from_secs(31 * 24 * 3600)
            }
        );
        let five_hour = classify_429(&usage_body("5h", "5h usage limit reached."), false);
        assert_eq!(
            five_hour,
            RateLimitKind::UsageLimit {
                dimension: UsageDimension::FiveHour,
                cooldown: Duration::from_secs(5 * 3600)
            }
        );
    }

    #[test]
    fn classify_degrades_to_generic_on_malformed_or_unknown_payloads() {
        assert!(!usage(classify_429(b"{", false)));
        assert!(!usage(classify_429(b"", false)));
        assert!(!usage(classify_429(b"[]", false)));
        let non_usage = br#"{"error":{"type":"OtherError","message":"Resets in 3 days."}}"#;
        assert!(!usage(classify_429(non_usage, false)));
        let unknown_dim = usage_body("quarterly", "Resets in 3 days.");
        assert!(!usage(classify_429(&unknown_dim, false)));
        let missing_type = br#"{"error":{"message":"Resets in 3 days.","type":null}}"#;
        assert!(!usage(classify_429(missing_type, false)));
    }

    #[test]
    fn classify_degrades_on_illegal_utf8_and_overflow() {
        let bad_utf8 = vec![0xff, 0xfe, 0x00, 0x01];
        assert!(!usage(classify_429(&bad_utf8, false)));
        // even a perfectly valid usage body is generic once the body overflowed the cap
        let body = usage_body("weekly", "Resets in 3 days.");
        assert!(!usage(classify_429(&body, true)));
    }

    #[test]
    fn parse_resets_in_never_reads_urls_or_workspace() {
        assert_eq!(
            parse_resets_in("Weekly usage limit reached. Resets in 3 days. Extra text."),
            Some(Duration::from_secs(3 * 24 * 3600))
        );
        // a URL containing digits and "resets" must not be parsed
        assert_eq!(
            parse_resets_in("See https://api.example.com/reset?id=3 for details."),
            None
        );
        assert_eq!(parse_resets_in("Workspace wrk_3 was reset."), None);
        assert_eq!(parse_resets_in("No reset phrase here."), None);
        assert_eq!(parse_resets_in("Resets in 0 days."), None);
        assert_eq!(parse_resets_in("Resets in 3.5 hours."), None);
        assert_eq!(parse_resets_in("Resets in -3 days."), None);
        assert_eq!(parse_resets_in("Resets in 3 fortnight."), None);
        assert_eq!(
            parse_resets_in("Resets in 99999999999999999999 days."),
            None
        );
    }

    fn rfc2822_utc(s: &str) -> SystemTime {
        DateTime::parse_from_rfc2822(s)
            .unwrap()
            .with_timezone(&Utc)
            .into()
    }

    #[test]
    fn retry_after_delta_seconds_is_parsed_as_is() {
        let now = rfc2822_utc("Fri, 14 Aug 2026 12:00:00 GMT");
        assert_eq!(
            parse_retry_after(&HeaderValue::from_static("30"), now),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after(&HeaderValue::from_static(" 30 "), now),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn retry_after_http_date_is_resolved_against_explicit_now() {
        let now = rfc2822_utc("Fri, 14 Aug 2026 12:00:00 GMT");
        let retry_at = HeaderValue::from_static("Fri, 14 Aug 2026 12:00:30 GMT");
        assert_eq!(
            parse_retry_after(&retry_at, now),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn retry_after_past_http_date_is_no_valid_wait() {
        let now = rfc2822_utc("Fri, 14 Aug 2026 12:00:00 GMT");
        let past = HeaderValue::from_static("Fri, 14 Aug 2026 11:59:30 GMT");
        assert_eq!(parse_retry_after(&past, now), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_unparseable_falls_back_to_default_backoff() {
        let now = rfc2822_utc("Fri, 14 Aug 2026 12:00:00 GMT");
        for bad in ["banana", "", "1.5", "-3", "14 Aug 2026"] {
            assert_eq!(
                parse_retry_after(&HeaderValue::from_static(bad), now),
                None,
                "{bad:?} must fall back to the default backoff"
            );
        }
    }
}
