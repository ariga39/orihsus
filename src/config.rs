use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use url::Url;

use crate::pool::MAX_COOLDOWN;

const DEFAULT_LISTEN: &str = "0.0.0.0:8443";
const DEFAULT_MAX_CONCURRENCY: usize = 200;
const DEFAULT_MAX_QUEUE: usize = 500;
const DEFAULT_QUEUE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
/// Global cap on bytes of request bodies that may be resident (buffered) at
/// once. 200 × 10MiB would otherwise allow ~2GiB of bodies in flight; the
/// gateway instead holds a body budget and admits bodies one at a time up to
/// this ceiling. Must fit `u32` (the semaphore permit unit) and be at least
/// `max_body_bytes`.
const DEFAULT_MAX_INFLIGHT_BODY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_BACKOFF_INITIAL: Duration = Duration::from_secs(5);
const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(60);
const DEFAULT_MODELS: [&str; 1] = ["deepseek-chat"];
const DEFAULT_BREAKER_THRESHOLD: usize = 5;
const DEFAULT_BREAKER_COOLDOWN: Duration = Duration::from_secs(60);
const DEFAULT_MAX_ATTEMPTS: usize = 2;
const DEFAULT_USAGE_SOFT_THRESHOLD_PERCENT: f64 = 80.0;
const DEFAULT_USAGE_POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_AUDIT_PATH: &str = "/var/log/orihsus/audit.jsonl";
const DEFAULT_AUDIT_QUEUE_CAPACITY: usize = 4096;
const DEFAULT_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_HEADER_BYTES: usize = 32 * 1024;
/// Hard cap on simultaneous TCP connections accepted by the server, across all
/// TLS/handshake/header phases. Tied to the systemd `LimitNOFILE=65536` FD
/// budget: beyond it a connection cannot be served anyway.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
const MAX_CONNECTIONS_CEILING: usize = 65_536;
/// How long a client may take to send the whole request body.
const DEFAULT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the upstream may take to produce response headers.
const DEFAULT_UPSTREAM_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
/// Independent bound on reading a retryable error body for classification.
const DEFAULT_UPSTREAM_ERROR_BODY_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-chunk bound on delivering a response to a slow/non-reading client.
const DEFAULT_RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Load and validate the full runtime configuration from a YAML file.
pub fn load(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    check_permissions(path)?;
    let contents = std::fs::read_to_string(path).map_err(|e| ConfigError {
        kind: ErrorKind::Io {
            path: path.to_owned(),
            source: e,
        },
    })?;
    let raw: RawConfig = serde_yaml::from_str(&contents).map_err(|e| {
        let (line, column) = match e.location() {
            Some(loc) => (loc.line(), loc.column()),
            None => (0, 0),
        };
        ConfigError {
            kind: ErrorKind::Parse {
                path: path.to_owned(),
                line,
                column,
            },
        }
    })?;
    Config::try_from(raw)
}

#[cfg(unix)]
fn check_permissions(path: &Path) -> Result<(), ConfigError> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).map_err(|e| ConfigError {
        kind: ErrorKind::Io {
            path: path.to_owned(),
            source: e,
        },
    })?;
    if meta.permissions().mode() & 0o777 != 0o600 {
        return Err(ConfigError {
            kind: ErrorKind::Permission(path.to_owned()),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(_path: &Path) -> Result<(), ConfigError> {
    Ok(())
}

/// Fully validated runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub tls: TlsPaths,
    pub gateway_token: Secret,
    pub upstream: Upstream,
    pub keys: Vec<Secret>,
    /// Static model list advertised by `GET /v1/models` and hot-reloadable.
    pub models: Vec<String>,
    pub limits: Limits,
    pub rotation: Rotation,
    pub usage: Usage,
    pub audit: Audit,
    pub server: Server,
}

/// Proactive OpenCode Go usage polling policy. Changes require a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct Usage {
    pub soft_threshold_percent: f64,
    pub poll_interval: Duration,
}

/// TLS certificate and private key paths.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// Upstream OpenAI-compatible endpoint.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub base_url: Url,
}

/// Capacity and admission bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_concurrency: usize,
    pub max_queue: usize,
    pub queue_wait_timeout: Duration,
    pub max_body_bytes: usize,
    /// Global cap on bytes of request bodies resident at once (see
    /// [`crate::gateway::BodyBudget`]). Must be >= `max_body_bytes` and fit
    /// `u32` (the semaphore permit unit).
    pub max_inflight_body_bytes: usize,
}

/// Key rotation and failure-handling bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rotation {
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub breaker_threshold: usize,
    pub breaker_cooldown: Duration,
    pub max_attempts: usize,
}

/// Audit JSONL logging configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Audit {
    pub path: PathBuf,
    pub queue_capacity: usize,
}

/// Local HTTP server hardening and bounded I/O timeouts. Changing any of these
/// requires a restart (they are not hot-reloadable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Server {
    pub read_header_timeout: Duration,
    pub max_header_bytes: usize,
    /// Bound on reading a client request body; a stalled upload after this
    /// many seconds is rejected.
    pub body_read_timeout: Duration,
    /// Bound on waiting for upstream response headers after the request is
    /// sent; once headers arrive no overall timeout is applied (SSE streams
    /// run as long as the upstream keeps them open).
    pub upstream_response_header_timeout: Duration,
    /// Independent bound on reading a retryable error body (up to the 64KiB
    /// classification cap) so a stalled upstream error cannot hang a request.
    pub upstream_error_body_timeout: Duration,
    /// Per-chunk bound on forwarding a response chunk to a client that has
    /// stopped consuming it. A client that never reads fills the gateway's
    /// response channel and after this many seconds the stream is abandoned:
    /// the upstream is cancelled and the admission permit released. A client
    /// that consumes at least once per window keeps the stream alive, and the
    /// bound never applies to a quiet SSE upstream, which can idle forever.
    pub response_write_timeout: Duration,
    /// Hard cap on simultaneous TCP connections (TLS handshake, header read
    /// and serving alike). A connection past the cap is closed immediately at
    /// accept, so slowloris clients that never complete TLS/headers cannot
    /// grow the task/FD set.
    pub max_connections: usize,
}

/// A secret value whose `Debug` never reveals its contents.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[derive(Debug)]
pub struct ConfigError {
    kind: ErrorKind,
}

#[derive(Debug)]
enum ErrorKind {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        line: usize,
        column: usize,
    },
    Permission(PathBuf),
    Validation(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Io { path, source } => {
                write!(f, "cannot read config {}: {}", path.display(), source)
            }
            ErrorKind::Parse { path, line, column } => {
                write!(
                    f,
                    "invalid YAML in {} at line {}, column {}",
                    path.display(),
                    line,
                    column
                )
            }
            ErrorKind::Permission(path) => {
                write!(
                    f,
                    "config file {} must have Unix permissions 0600",
                    path.display()
                )
            }
            ErrorKind::Validation(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    fn try_from(raw: RawConfig) -> Result<Config, ConfigError> {
        let validation = |msg: String| ConfigError {
            kind: ErrorKind::Validation(msg),
        };

        let gateway_token = match raw.gateway_token {
            Some(t) if !t.trim().is_empty() => Secret::new(t),
            _ => return Err(validation("gateway token must be set and non-empty".into())),
        };

        let listen = match raw.listen {
            Some(addr) => addr.parse::<SocketAddr>().map_err(|_| {
                validation(format!(
                    "listen must be a valid socket address, got {addr:?}"
                ))
            })?,
            None => DEFAULT_LISTEN.parse::<SocketAddr>().unwrap(),
        };

        let tls = raw
            .tls
            .ok_or_else(|| validation("tls section is required".into()))?;
        let cert = non_empty(tls.cert_path, "tls.cert_path is required", &validation)?;
        let key = non_empty(tls.key_path, "tls.key_path is required", &validation)?;

        let upstream = raw
            .upstream
            .ok_or_else(|| validation("upstream section is required".into()))?;
        let base_url = non_empty_string(
            upstream.base_url,
            "upstream.base_url is required",
            &validation,
        )?;
        let base_url = Url::parse(&base_url)
            .map_err(|_| validation("upstream.base_url must be a valid URL".into()))?;
        if base_url.scheme() != "https" {
            return Err(validation("upstream.base_url must use https".into()));
        }
        if base_url.query().is_some() || base_url.fragment().is_some() {
            return Err(validation(
                "upstream.base_url must not contain a query or fragment".into(),
            ));
        }
        // Normalize a path prefix to a trailing slash so `/openai` and
        // `/openai/` resolve identically: `Url::join` drops the last path
        // segment when the base path has no trailing slash, which would
        // silently discard a configured path prefix when forwarding.
        let base_url = normalize_base_url_path(base_url);

        let keys = raw.keys.unwrap_or_default();
        if keys.is_empty() {
            return Err(validation("at least one upstream key is required".into()));
        }
        for k in &keys {
            if k.trim().is_empty() {
                return Err(validation("upstream keys must be non-empty".into()));
            }
        }
        let mut seen = std::collections::HashSet::with_capacity(keys.len());
        for k in &keys {
            if !seen.insert(k) {
                return Err(validation(
                    "upstream keys must not contain duplicates".into(),
                ));
            }
        }
        let keys = keys.into_iter().map(Secret::new).collect();

        let models = match raw.models {
            Some(models) => {
                if models.is_empty() {
                    return Err(validation("models must not be empty".into()));
                }
                let mut seen = std::collections::HashSet::with_capacity(models.len());
                for m in &models {
                    if m.trim().is_empty() {
                        return Err(validation(
                            "models must contain only non-blank model names".into(),
                        ));
                    }
                    if !seen.insert(m) {
                        return Err(validation("models must not contain duplicates".into()));
                    }
                }
                models
            }
            None => DEFAULT_MODELS.iter().map(|s| s.to_string()).collect(),
        };

        let limits = raw.limits.unwrap_or_default();
        let max_concurrency = limits.max_concurrency.unwrap_or(DEFAULT_MAX_CONCURRENCY);
        if max_concurrency == 0 {
            return Err(validation(
                "limits.max_concurrency must be at least 1".into(),
            ));
        }
        if max_concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(validation(format!(
                "limits.max_concurrency must be at most {} (tokio semaphore permits)",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        let max_queue = limits.max_queue.unwrap_or(DEFAULT_MAX_QUEUE);
        if max_queue > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(validation(format!(
                "limits.max_queue must be at most {} (tokio semaphore permits)",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        let queue_wait_timeout = match limits.queue_wait_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_QUEUE_WAIT_TIMEOUT,
        };
        if queue_wait_timeout.is_zero() {
            return Err(validation(
                "limits.queue_wait_timeout must be greater than zero".into(),
            ));
        }
        let max_body_bytes = match limits.max_body_bytes {
            Some(raw) => parse_bytes(&raw).map_err(validation)?,
            None => DEFAULT_MAX_BODY_BYTES,
        };
        if max_body_bytes == 0 {
            return Err(validation(
                "limits.max_body_bytes must be at least 1 byte".into(),
            ));
        }
        if max_body_bytes > u32::MAX as usize {
            return Err(validation(
                "limits.max_body_bytes must fit u32 (body reservations are u32 semaphore permits)"
                    .into(),
            ));
        }
        let max_inflight_body_bytes = match limits.max_inflight_body_bytes {
            Some(raw) => parse_bytes(&raw).map_err(validation)?,
            None => DEFAULT_MAX_INFLIGHT_BODY_BYTES,
        };
        if max_inflight_body_bytes == 0 {
            return Err(validation(
                "limits.max_inflight_body_bytes must be at least 1 byte".into(),
            ));
        }
        if max_inflight_body_bytes > u32::MAX as usize {
            return Err(validation(
                "limits.max_inflight_body_bytes must fit u32 (semaphore permits are u32)".into(),
            ));
        }
        if max_inflight_body_bytes < max_body_bytes {
            return Err(validation(
                "limits.max_inflight_body_bytes must be >= limits.max_body_bytes".into(),
            ));
        }

        let rotation = raw.rotation.unwrap_or_default();
        if rotation.soft_threshold.is_some() {
            return Err(validation(
                "rotation.soft_threshold is no longer supported; remove it from the config".into(),
            ));
        }
        let backoff_initial = match rotation.backoff_initial {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_BACKOFF_INITIAL,
        };
        if backoff_initial.is_zero() {
            return Err(validation(
                "rotation.backoff_initial must be greater than zero".into(),
            ));
        }
        let backoff_max = match rotation.backoff_max {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_BACKOFF_MAX,
        };
        if backoff_max.is_zero() {
            return Err(validation(
                "rotation.backoff_max must be greater than zero".into(),
            ));
        }
        if backoff_max < backoff_initial {
            return Err(validation(
                "rotation.backoff_max must be >= rotation.backoff_initial".into(),
            ));
        }
        if backoff_max > MAX_COOLDOWN {
            return Err(validation(
                "rotation.backoff_max must be at most the ops cooldown ceiling (90d): a larger value would overflow the jittered backoff"
                    .into(),
            ));
        }
        let breaker_threshold = rotation
            .breaker_threshold
            .unwrap_or(DEFAULT_BREAKER_THRESHOLD);
        if breaker_threshold == 0 {
            return Err(validation(
                "rotation.breaker_threshold must be at least 1".into(),
            ));
        }
        if breaker_threshold > u32::MAX as usize {
            return Err(validation(
                "rotation.breaker_threshold must fit u32 (the key pool breaker counts failures in u32)"
                    .into(),
            ));
        }
        let breaker_cooldown = match rotation.breaker_cooldown {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_BREAKER_COOLDOWN,
        };
        if breaker_cooldown.is_zero() {
            return Err(validation(
                "rotation.breaker_cooldown must be greater than zero".into(),
            ));
        }
        let max_attempts = rotation.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if max_attempts == 0 || max_attempts > 2 {
            return Err(validation(
                "rotation.max_attempts must be between 1 and 2".into(),
            ));
        }

        let usage = raw.usage.unwrap_or_default();
        let soft_threshold_percent = usage
            .soft_threshold_percent
            .unwrap_or(DEFAULT_USAGE_SOFT_THRESHOLD_PERCENT);
        if !soft_threshold_percent.is_finite()
            || soft_threshold_percent <= 0.0
            || soft_threshold_percent > 100.0
        {
            return Err(validation(
                "usage.soft_threshold_percent must be finite and in (0, 100]".into(),
            ));
        }
        let poll_interval = match usage.poll_interval {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_USAGE_POLL_INTERVAL,
        };
        if poll_interval < Duration::from_secs(30) {
            return Err(validation(
                "usage.poll_interval must be at least 30 seconds".into(),
            ));
        }

        let audit = raw.audit.unwrap_or_default();
        let audit_path = match audit.path {
            Some(p) if !p.trim().is_empty() => PathBuf::from(p),
            _ => PathBuf::from(DEFAULT_AUDIT_PATH),
        };
        let audit_queue_capacity = audit.queue_capacity.unwrap_or(DEFAULT_AUDIT_QUEUE_CAPACITY);
        if audit_queue_capacity == 0 {
            return Err(validation("audit.queue_capacity must be at least 1".into()));
        }

        let server = raw.server.unwrap_or_default();
        let read_header_timeout = match server.read_header_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_READ_HEADER_TIMEOUT,
        };
        if read_header_timeout.is_zero() {
            return Err(validation(
                "server.read_header_timeout must be greater than zero".into(),
            ));
        }
        let max_header_bytes = match server.max_header_bytes {
            Some(raw) => parse_bytes(&raw).map_err(validation)?,
            None => DEFAULT_MAX_HEADER_BYTES,
        };
        if max_header_bytes < 8192 {
            return Err(validation(
                "server.max_header_bytes must be at least 8192 bytes (HTTP/1 minimum)".into(),
            ));
        }
        if max_header_bytes > u32::MAX as usize {
            return Err(validation(
                "server.max_header_bytes must fit u32 (hyper's HTTP/2 header-list cap is u32)"
                    .into(),
            ));
        }
        let body_read_timeout = match server.body_read_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_BODY_READ_TIMEOUT,
        };
        if body_read_timeout.is_zero() {
            return Err(validation(
                "server.body_read_timeout must be greater than zero".into(),
            ));
        }
        let upstream_response_header_timeout = match server.upstream_response_header_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_UPSTREAM_RESPONSE_HEADER_TIMEOUT,
        };
        if upstream_response_header_timeout.is_zero() {
            return Err(validation(
                "server.upstream_response_header_timeout must be greater than zero".into(),
            ));
        }
        let upstream_error_body_timeout = match server.upstream_error_body_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_UPSTREAM_ERROR_BODY_TIMEOUT,
        };
        if upstream_error_body_timeout.is_zero() {
            return Err(validation(
                "server.upstream_error_body_timeout must be greater than zero".into(),
            ));
        }
        let response_write_timeout = match server.response_write_timeout {
            Some(raw) => parse_duration(&raw).map_err(validation)?,
            None => DEFAULT_RESPONSE_WRITE_TIMEOUT,
        };
        if response_write_timeout.is_zero() {
            return Err(validation(
                "server.response_write_timeout must be greater than zero".into(),
            ));
        }
        let max_connections = server.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
        if max_connections == 0 {
            return Err(validation(
                "server.max_connections must be at least 1".into(),
            ));
        }
        if max_connections > MAX_CONNECTIONS_CEILING {
            return Err(validation(
                format!(
                    "server.max_connections must be at most {MAX_CONNECTIONS_CEILING} (the systemd LimitNOFILE FD budget)"
                )
            ));
        }

        Ok(Config {
            listen,
            tls: TlsPaths { cert, key },
            gateway_token,
            upstream: Upstream { base_url },
            keys,
            models,
            limits: Limits {
                max_concurrency,
                max_queue,
                queue_wait_timeout,
                max_body_bytes,
                max_inflight_body_bytes,
            },
            rotation: Rotation {
                backoff_initial,
                backoff_max,
                breaker_threshold,
                breaker_cooldown,
                max_attempts,
            },
            usage: Usage {
                soft_threshold_percent,
                poll_interval,
            },
            audit: Audit {
                path: audit_path,
                queue_capacity: audit_queue_capacity,
            },
            server: Server {
                read_header_timeout,
                max_header_bytes,
                body_read_timeout,
                upstream_response_header_timeout,
                upstream_error_body_timeout,
                response_write_timeout,
                max_connections,
            },
        })
    }
}

fn non_empty(
    value: Option<String>,
    message: &'static str,
    validation: &impl Fn(String) -> ConfigError,
) -> Result<PathBuf, ConfigError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(PathBuf::from(v)),
        _ => Err(validation(message.into())),
    }
}

/// Normalize `base_url`'s path to a trailing slash (a root path is left
/// alone), so a configured path prefix like `/openai` and `/openai/` both
/// resolve to `/openai/` and `Url::join("v1/chat/completions")` keeps it.
fn normalize_base_url_path(mut url: Url) -> Url {
    let path = url.path();
    if !path.is_empty() && !path.ends_with('/') {
        url.set_path(&format!("{path}/"));
    }
    url
}

fn non_empty_string(
    value: Option<String>,
    message: &'static str,
    validation: &impl Fn(String) -> ConfigError,
) -> Result<String, ConfigError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(validation(message.into())),
    }
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    humantime::parse_duration(raw).map_err(|_| format!("invalid duration {raw:?}"))
}

fn parse_bytes(raw: &str) -> Result<usize, String> {
    let raw = raw.trim();
    let split = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let (num, suffix) = raw.split_at(split);
    let value: u64 = num
        .parse()
        .map_err(|_| format!("invalid byte size {raw:?}"))?;
    let multiplier: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => return Err(format!("invalid byte size suffix {other:?}")),
    };
    let bytes = value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte size too large: {raw:?}"))?;
    usize::try_from(bytes).map_err(|_| format!("byte size too large: {raw:?}"))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawConfig {
    gateway_token: Option<String>,
    listen: Option<String>,
    tls: Option<RawTls>,
    upstream: Option<RawUpstream>,
    keys: Option<Vec<String>>,
    models: Option<Vec<String>>,
    limits: Option<RawLimits>,
    rotation: Option<RawRotation>,
    usage: Option<RawUsage>,
    audit: Option<RawAudit>,
    server: Option<RawServer>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawUsage {
    soft_threshold_percent: Option<f64>,
    poll_interval: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawTls {
    cert_path: Option<String>,
    key_path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawUpstream {
    base_url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawLimits {
    max_concurrency: Option<usize>,
    max_queue: Option<usize>,
    queue_wait_timeout: Option<String>,
    max_body_bytes: Option<String>,
    max_inflight_body_bytes: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawRotation {
    /// Deprecated since the soft-threshold/quota strategy was removed. Kept
    /// only to detect and reject a leftover field; the value is never echoed
    /// (a catch-all type avoids any parse/type error that could leak it).
    soft_threshold: Option<serde_yaml::Value>,
    backoff_initial: Option<String>,
    backoff_max: Option<String>,
    breaker_threshold: Option<usize>,
    breaker_cooldown: Option<String>,
    max_attempts: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawAudit {
    path: Option<String>,
    queue_capacity: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawServer {
    read_header_timeout: Option<String>,
    max_header_bytes: Option<String>,
    body_read_timeout: Option<String>,
    upstream_response_header_timeout: Option<String>,
    upstream_error_body_timeout: Option<String>,
    response_write_timeout: Option<String>,
    max_connections: Option<usize>,
}
