//! Assembles the long-lived runtime from a validated config.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use url::Url;

use crate::audit::AuditWriter;
use crate::config::Config;
use crate::gateway::{build_router, BodyBudget, GatewayState, RuntimeState, RuntimeStore};
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
}

/// Assemble everything the gateway needs from a validated config. Any failure
/// here aborts startup (no partial runtime).
pub fn assemble(cfg: &Config) -> Result<(AppRuntime, Router), BootstrapError> {
    let http = build_upstream_client().map_err(BootstrapError::Client)?;
    let policy = PoolPolicy {
        backoff_initial: cfg.rotation.backoff_initial,
        backoff_max: cfg.rotation.backoff_max,
        breaker_threshold: u32::try_from(cfg.rotation.breaker_threshold)
            .expect("config validation caps breaker_threshold at u32::MAX"),
        breaker_cooldown: cfg.rotation.breaker_cooldown,
        // The pool has its own hard 30s wait budget, independent of the queue's
        // queue_wait_timeout (which only bounds admission waiting).
        wait_timeout: crate::pool::WAIT_TIMEOUT,
        max_attempts: cfg.rotation.max_attempts,
    };
    let pool = Arc::new(KeyPool::new(cfg.keys.clone(), policy).map_err(BootstrapError::Pool)?);
    let queue = Arc::new(AdmissionQueue::new(
        cfg.limits.max_concurrency,
        cfg.limits.max_queue,
        cfg.limits.queue_wait_timeout,
    ));
    let runtime = RuntimeStore::new(RuntimeState {
        gateway_token: cfg.gateway_token.clone(),
        base_url: cfg.upstream.base_url.clone(),
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
    Ok((
        AppRuntime {
            pool,
            queue,
            runtime,
            audit,
            body_budget,
            usage_monitor: Some(usage_monitor),
        },
        router,
    ))
}

/// How many redirect hops a single upstream request may follow before the
/// gateway stops (mirrors reqwest's default). The custom policy must bound the
/// chain itself: an unbounded same-origin redirect loop would hang the request
/// until the upstream header timeout.
const MAX_REDIRECT_HOPS: usize = 10;

/// Upstream client: rustls-verified HTTPS (base_url is https by config), a
/// connect timeout only — no overall request timeout so SSE streams can run for
/// as long as the upstream keeps them open. Redirects are followed only when
/// scheme, host and effective port are unchanged; a cross-origin/scheme/port
/// redirect is returned to the gateway, so the selected key's Authorization is
/// never forwarded to the redirect target.
pub fn build_upstream_client() -> Result<reqwest::Client, reqwest::Error> {
    let builder = reqwest::Client::builder()
        .redirect(same_origin_redirect_policy())
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90));
    #[cfg(feature = "loadtest-insecure-upstream")]
    let builder = builder.danger_accept_invalid_certs(true);
    builder.build()
}

/// Same-origin redirect policy: follow only redirects whose scheme, host and
/// effective port are unchanged from the original request (`previous[0]`), so a
/// relative or absolute same-origin `Location` is followed but a cross-origin,
/// scheme-changing or port-changing target is stopped before any request is
/// sent to it.
fn same_origin_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        let origin = attempt.previous().first().unwrap_or(attempt.url());
        if attempt.previous().len() > MAX_REDIRECT_HOPS {
            attempt.stop()
        } else if same_origin(origin, attempt.url()) {
            attempt.follow()
        } else {
            attempt.stop()
        }
    })
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && effective_port(a) == effective_port(b)
}

/// The port a URL actually connects on, using the scheme default when none is
/// explicit, so `https://host` and `https://host:443` are the same origin.
fn effective_port(url: &Url) -> u16 {
    url.port().unwrap_or_else(|| match url.scheme() {
        "http" => 80,
        "https" => 443,
        _ => 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn same(a: &str, b: &str) -> bool {
        same_origin(&Url::parse(a).unwrap(), &Url::parse(b).unwrap())
    }

    #[test]
    fn origin_matches_on_scheme_host_and_effective_port() {
        assert!(same("https://api.opencode.go", "https://api.opencode.go/"));
        assert!(same(
            "https://api.opencode.go",
            "https://api.opencode.go:443"
        ));
        assert!(same("http://api.opencode.go", "http://api.opencode.go:80"));
        assert!(same(
            "https://api.opencode.go:8443",
            "https://api.opencode.go:8443"
        ));
        assert!(!same(
            "https://api.opencode.go",
            "https://api.opencode.go:8443"
        ));
        assert!(!same("https://api.opencode.go", "http://api.opencode.go"));
        assert!(!same("https://api.opencode.go", "https://other.example"));
    }
}
