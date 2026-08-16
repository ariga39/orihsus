//! Assembles the long-lived runtime from a validated config.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;

use crate::audit::AuditWriter;
use crate::config::Config;
use crate::gateway::{build_router, BodyBudget, GatewayState, RuntimeState, RuntimeStore};
use crate::models::ModelMonitor;
use crate::pool::{KeyPool, PoolPolicy};
use crate::queue::AdmissionQueue;
use crate::usage::UsageMonitor;

/// Startup errors. Never echoes keys or tokens.
#[derive(Debug)]
pub enum BootstrapError {
    Audit(crate::audit::AuditError),
    Pool(crate::pool::PoolError),
    Client(reqwest::Error),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Audit(e) => write!(f, "cannot start audit logging: {e}"),
            BootstrapError::Pool(e) => write!(f, "cannot build the key pool: {e}"),
            BootstrapError::Client(e) => write!(f, "cannot build the upstream client: {e}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// The long-lived runtime pieces assembled from the config.
pub struct AppRuntime {
    pub pool: Arc<KeyPool>,
    pub queue: Arc<AdmissionQueue>,
    pub runtime: RuntimeStore,
    pub audit: Arc<AuditWriter>,
    pub body_budget: BodyBudget,
    pub usage_monitor: Option<UsageMonitor>,
    pub model_monitor: Option<ModelMonitor>,
}

/// Assemble everything the gateway needs from a validated config. Any failure
/// here aborts startup (no partial runtime).
pub fn assemble(cfg: &Config) -> Result<(AppRuntime, Router), BootstrapError> {
    let http = build_upstream_client().map_err(BootstrapError::Client)?;
    let policy = PoolPolicy {
        backoff_initial: cfg.key_failure_handling.backoff_initial,
        backoff_max: cfg.key_failure_handling.backoff_max,
        breaker_threshold: u32::try_from(cfg.key_failure_handling.breaker_threshold)
            .expect("config validation caps breaker_threshold at u32::MAX"),
        breaker_cooldown: cfg.key_failure_handling.breaker_cooldown,
        // The pool has its own hard 30s wait budget, independent of the queue's
        // queue_wait_timeout (which only bounds admission waiting).
        wait_timeout: crate::pool::WAIT_TIMEOUT,
        max_attempts: cfg.key_failure_handling.max_attempts,
    };
    let pool = Arc::new(KeyPool::new(cfg.keys.clone(), policy).map_err(BootstrapError::Pool)?);
    let queue = Arc::new(AdmissionQueue::new(
        cfg.limits.max_concurrency,
        cfg.limits.max_queue,
        cfg.limits.queue_wait_timeout,
    ));
    let runtime = RuntimeStore::new(RuntimeState {
        gateway_token: cfg.gateway_token.clone(),
        base_url: url::Url::parse(crate::config::OPENCODE_GO_BASE_URL)
            .expect("built-in OpenCode Go base URL is valid"),
        max_body_bytes: cfg.limits.max_body_bytes,
        models: cfg.models.clone(),
    });
    let audit = Arc::new(
        AuditWriter::start(&cfg.audit.path, cfg.audit.queue_capacity)
            .map_err(BootstrapError::Audit)?,
    );
    let timeouts = crate::gateway::IoTimeouts {
        body_read: cfg.server.body_read_timeout,
        upstream_header: cfg.server.upstream_response_header_timeout,
        first_event: cfg.server.first_event_timeout,
        inter_event: cfg.server.inter_event_timeout,
        upstream_error_body: cfg.server.upstream_error_body_timeout,
        response_write: cfg.server.response_write_timeout,
    };
    let body_budget = BodyBudget::new(
        cfg.limits.max_inflight_body_bytes,
        u32::try_from(cfg.limits.max_body_bytes)
            .expect("config validation caps max_body_bytes at u32::MAX"),
    );
    let state = GatewayState::with_runtime(
        http,
        runtime.clone(),
        pool.clone(),
        queue.clone(),
        audit.clone(),
        body_budget.clone(),
        timeouts,
    );
    let router = build_router(state);
    let usage_monitor = UsageMonitor::start(cfg.usage.clone(), cfg.keys.clone(), pool.clone())
        .map_err(BootstrapError::Client)?;
    let model_monitor = ModelMonitor::start(cfg.model_sync.clone(), runtime.clone())
        .map_err(BootstrapError::Client)?;
    Ok((
        AppRuntime {
            pool,
            queue,
            runtime,
            audit,
            body_budget,
            usage_monitor: Some(usage_monitor),
            model_monitor: Some(model_monitor),
        },
        router,
    ))
}

/// Upstream client: rustls-verified HTTPS to the built-in upstream, a
/// connect timeout only — no overall request timeout so SSE streams can run for
/// as long as the upstream keeps them open. Redirects are never followed: even
/// a same-origin redirect could escape the credential-bearing path allowlist.
pub fn build_upstream_client() -> Result<reqwest::Client, reqwest::Error> {
    let builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90));
    #[cfg(feature = "loadtest-insecure-upstream")]
    let builder = builder.danger_accept_invalid_certs(true);
    builder.build()
}
