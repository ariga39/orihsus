use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::pool::MAX_COOLDOWN;
use serde::Deserialize;

#[cfg(not(feature = "loadtest-insecure-upstream"))]
pub const OPENCODE_GO_BASE_URL: &str = "https://opencode.ai/zen/go/";
#[cfg(feature = "loadtest-insecure-upstream")]
pub const OPENCODE_GO_BASE_URL: &str = "https://127.0.0.1:18443/";
pub const MAX_MODEL_BYTES: usize = 256;

/// Closed set of upstream APIs that may receive an OpenCode Go key.
#[derive(Debug, Clone, Copy)]
pub(crate) enum UpstreamApi {
    ChatCompletions,
    Messages,
    Responses,
    Usage,
    Models,
}

pub(crate) fn upstream_api_url(base: &url::Url, api: UpstreamApi) -> url::Url {
    let path = match api {
        UpstreamApi::ChatCompletions => "v1/chat/completions",
        UpstreamApi::Messages => "v1/messages",
        UpstreamApi::Responses => "v1/responses",
        UpstreamApi::Usage => "v1/usage",
        UpstreamApi::Models => "v1/models",
    };
    base.join(path).expect("fixed upstream API path is valid")
}

const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";
const DEFAULT_LISTEN_PORT: u16 = 8080;
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
const DEFAULT_MODEL_SYNC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_BREAKER_THRESHOLD: usize = 5;
const DEFAULT_BREAKER_COOLDOWN: Duration = Duration::from_secs(60);
const DEFAULT_MAX_ATTEMPTS: usize = 2;
const DEFAULT_USAGE_SOFT_THRESHOLD_PERCENT: f64 = 80.0;
const DEFAULT_USAGE_POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_USAGE_HISTORY_DIR: &str = "/var/log/orihsus/usage";
const DEFAULT_AUDIT_PATH: &str = "/var/log/orihsus/audit.jsonl";
const DEFAULT_AUDIT_QUEUE_CAPACITY: usize = 4096;
const DEFAULT_READ_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_HEADER_BYTES: usize = 32 * 1024;
/// Hard cap on simultaneous TCP connections accepted by the server, across all
/// connection/header phases. Tied to the systemd `LimitNOFILE=65536` FD
/// budget: beyond it a connection cannot be served anyway.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;
const MAX_CONNECTIONS_CEILING: usize = 65_536;
/// How long a client may take to send the whole request body.
const DEFAULT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the upstream may take to produce response headers.
const DEFAULT_UPSTREAM_RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_FIRST_EVENT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_INTER_EVENT_TIMEOUT: Duration = Duration::from_secs(90);
/// Independent bound on reading a retryable error body for classification.
const DEFAULT_UPSTREAM_ERROR_BODY_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-chunk bound on delivering a response to a slow/non-reading client.
const DEFAULT_RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Load and validate the full runtime configuration from a YAML file.
pub fn load(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    let mut file = open_config(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| ConfigError {
            kind: ErrorKind::Io {
                path: path.to_owned(),
                source: e,
            },
        })?;
    let raw: RawConfig = yaml_serde::from_str(&contents).map_err(|e| {
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
fn open_config(path: &Path) -> Result<File, ConfigError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|e| ConfigError {
            kind: ErrorKind::Io {
                path: path.to_owned(),
                source: e,
            },
        })?;
    let meta = file.metadata().map_err(|e| ConfigError {
        kind: ErrorKind::Io {
            path: path.to_owned(),
            source: e,
        },
    })?;
    // Validate and read the same open file description. This closes both
    // symlink traversal and path-replacement TOCTOU windows.
    // SAFETY: geteuid takes no arguments, has no preconditions, and cannot fail.
    let effective_uid = unsafe { libc::geteuid() };
    if !secure_config_metadata(
        meta.is_file(),
        meta.permissions().mode() & 0o777,
        meta.uid(),
        effective_uid,
    ) {
        return Err(ConfigError {
            kind: ErrorKind::Permission(path.to_owned()),
        });
    }
    Ok(file)
}

#[cfg(unix)]
fn secure_config_metadata(is_file: bool, mode: u32, owner: u32, effective_uid: u32) -> bool {
    is_file && mode == 0o600 && owner == effective_uid
}

#[cfg(not(unix))]
fn open_config(path: &Path) -> Result<File, ConfigError> {
    File::open(path).map_err(|e| ConfigError {
        kind: ErrorKind::Io {
            path: path.to_owned(),
            source: e,
        },
    })
}

/// Fully validated runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub gateway_keys: Vec<GatewayKey>,
    pub keys: Vec<Secret>,
    /// Optional display aliases keyed internally by the audit-compatible fingerprint.
    pub key_aliases: BTreeMap<String, String>,
    /// Current startup/fallback model list, or an explicit manual override.
    pub models: Vec<String>,
    pub model_sync: ModelSync,
    pub limits: Limits,
    pub key_failure_handling: KeyFailureHandling,
    pub usage: Usage,
    pub usage_history_dir: PathBuf,
    pub audit: Audit,
    pub server: Server,
}

/// A client credential and its non-secret audit identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayKey {
    pub name: String,
    pub token: Secret,
}

/// Public OpenCode Go model-list synchronization policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSync {
    pub enabled: bool,
    pub interval: Duration,
}

/// Proactive OpenCode Go usage polling policy. Changes require a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct Usage {
    pub soft_threshold_percent: f64,
    pub poll_interval: Duration,
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

/// Backoff, circuit-breaker, and cross-key retry policy after an upstream key
/// fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyFailureHandling {
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
    /// sent. SSE liveness uses the separate first/inter-event deadlines below.
    pub upstream_response_header_timeout: Duration,
    /// Bound from upstream response headers to the first complete SSE event.
    pub first_event_timeout: Duration,
    /// Maximum silence between complete SSE events after downstream commit.
    pub inter_event_timeout: Duration,
    /// Model-specific event liveness policies, with omitted fields inherited
    /// from the two global defaults above.
    pub model_event_timeouts: BTreeMap<String, EventTimeouts>,
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
    /// Hard cap on simultaneous TCP connections (header read and serving
    /// alike). A connection past the cap is closed immediately at accept, so
    /// slowloris clients that never complete headers cannot
    /// grow the task/FD set.
    pub max_connections: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTimeouts {
    pub first_event_timeout: Duration,
    pub inter_event_timeout: Duration,
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
                    "config file {} must be a regular file owned by the process user with Unix permissions 0600",
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

        if raw.gateway_token.is_some() && raw.gateway_keys.is_some() {
            return Err(validation(
                "set either gateway_token or gateway_keys, not both".into(),
            ));
        }
        let gateway_keys = if let Some(token) = raw.gateway_token {
            if token.trim().is_empty() {
                return Err(validation("gateway token must be non-empty".into()));
            }
            vec![GatewayKey {
                name: "legacy".into(),
                token: Secret::new(token),
            }]
        } else if let Some(entries) = raw.gateway_keys {
            if entries.is_empty() {
                return Err(validation("at least one gateway key is required".into()));
            }
            let mut names = std::collections::HashSet::with_capacity(entries.len());
            let mut tokens = std::collections::HashSet::with_capacity(entries.len());
            let mut keys = Vec::with_capacity(entries.len());
            for entry in entries {
                if entry.name.trim().is_empty() || entry.name.len() > 128 {
                    return Err(validation(
                        "gateway key names must be non-blank and at most 128 bytes".into(),
                    ));
                }
                if entry.token.trim().is_empty() {
                    return Err(validation("gateway key tokens must be non-empty".into()));
                }
                if !names.insert(entry.name.clone()) {
                    return Err(validation("gateway key names must be unique".into()));
                }
                if !tokens.insert(entry.token.clone()) {
                    return Err(validation("gateway key tokens must be unique".into()));
                }
                keys.push(GatewayKey {
                    name: entry.name,
                    token: Secret::new(entry.token),
                });
            }
            keys
        } else {
            return Err(validation(
                "gateway_token or gateway_keys must be set".into(),
            ));
        };

        let listen = raw.listen.unwrap_or_default();
        let listen_host = listen.host.as_deref().unwrap_or(DEFAULT_LISTEN_HOST);
        let listen_host = listen_host
            .parse::<IpAddr>()
            .map_err(|_| validation("listen.host must be a valid IP address".into()))?;
        let listen = SocketAddr::new(listen_host, listen.port.unwrap_or(DEFAULT_LISTEN_PORT));
        if !listen.ip().is_loopback() {
            return Err(validation(
                "listen.host must be a loopback address; expose the service through nginx".into(),
            ));
        }

        let raw_keys = raw.keys.unwrap_or_default();
        if raw_keys.is_empty() {
            return Err(validation("at least one upstream key is required".into()));
        }
        let mut keys = Vec::with_capacity(raw_keys.len());
        let mut key_aliases = BTreeMap::new();
        for raw_key in raw_keys {
            let (key, name) = match raw_key {
                RawKey::Plain(key) => (key, None),
                RawKey::Named { key, name } => (key, name),
            };
            if key.trim().is_empty() {
                return Err(validation("upstream keys must be non-empty".into()));
            }
            if let Some(name) = &name {
                if name.trim().is_empty() || name.len() > 128 {
                    return Err(validation(
                        "upstream key names must be non-blank and at most 128 bytes".into(),
                    ));
                }
                key_aliases.insert(crate::audit::fingerprint(&key), name.clone());
            }
            keys.push(key);
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

        let has_manual_models = raw.models.is_some();
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
                    if m.len() > MAX_MODEL_BYTES {
                        return Err(validation(
                            "models must not exceed 256 bytes per value".into(),
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
        let model_sync = raw.model_sync.unwrap_or_default();
        let model_sync_enabled = !has_manual_models && model_sync.enabled.unwrap_or(true);
        let model_sync_interval = Duration::from_secs(
            model_sync
                .interval_seconds
                .unwrap_or(DEFAULT_MODEL_SYNC_INTERVAL.as_secs()),
        );
        if model_sync_interval < Duration::from_secs(30) {
            return Err(validation(
                "model_sync.interval_seconds must be at least 30".into(),
            ));
        }

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
        let queue_wait_timeout = match limits.queue_wait_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_QUEUE_WAIT_TIMEOUT,
        };
        if queue_wait_timeout.is_zero() {
            return Err(validation(
                "limits.queue_wait_timeout_seconds must be greater than zero".into(),
            ));
        }
        let max_body_bytes = limits.max_body_bytes.unwrap_or(DEFAULT_MAX_BODY_BYTES);
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
        let max_inflight_body_bytes = limits
            .max_inflight_body_bytes
            .unwrap_or(DEFAULT_MAX_INFLIGHT_BODY_BYTES);
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

        let key_failure = raw.key_failure_handling.unwrap_or_default();
        if key_failure.soft_threshold.is_some() {
            return Err(validation(
                "key_failure_handling.soft_threshold is no longer supported; remove it from the config"
                    .into(),
            ));
        }
        let backoff_initial = match key_failure.backoff_initial_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_BACKOFF_INITIAL,
        };
        if backoff_initial.is_zero() {
            return Err(validation(
                "key_failure_handling.backoff_initial_seconds must be greater than zero".into(),
            ));
        }
        let backoff_max = match key_failure.backoff_max_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_BACKOFF_MAX,
        };
        if backoff_max.is_zero() {
            return Err(validation(
                "key_failure_handling.backoff_max_seconds must be greater than zero".into(),
            ));
        }
        if backoff_max < backoff_initial {
            return Err(validation(
                "key_failure_handling.backoff_max_seconds must be >= key_failure_handling.backoff_initial_seconds".into(),
            ));
        }
        if backoff_max > MAX_COOLDOWN {
            return Err(validation(
                "key_failure_handling.backoff_max_seconds must be at most the ops cooldown ceiling (7776000 seconds)"
                    .into(),
            ));
        }
        let breaker_threshold = key_failure
            .breaker_threshold
            .unwrap_or(DEFAULT_BREAKER_THRESHOLD);
        if breaker_threshold == 0 {
            return Err(validation(
                "key_failure_handling.breaker_threshold must be at least 1".into(),
            ));
        }
        if breaker_threshold > u32::MAX as usize {
            return Err(validation(
                "key_failure_handling.breaker_threshold must fit u32 (the key pool breaker counts failures in u32)"
                    .into(),
            ));
        }
        let breaker_cooldown = match key_failure.breaker_cooldown_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_BREAKER_COOLDOWN,
        };
        if breaker_cooldown.is_zero() {
            return Err(validation(
                "key_failure_handling.breaker_cooldown_seconds must be greater than zero".into(),
            ));
        }
        let max_attempts = key_failure.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if max_attempts == 0 || max_attempts > 2 {
            return Err(validation(
                "key_failure_handling.max_attempts must be between 1 and 2".into(),
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
        let poll_interval = match usage.poll_interval_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_USAGE_POLL_INTERVAL,
        };
        if poll_interval < Duration::from_secs(30) {
            return Err(validation(
                "usage.poll_interval_seconds must be at least 30".into(),
            ));
        }
        let usage_history_dir = raw
            .usage_history_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_USAGE_HISTORY_DIR));
        if usage_history_dir.as_os_str().is_empty() {
            return Err(validation("usage_history_dir must not be empty".into()));
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
        let read_header_timeout = match server.read_header_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_READ_HEADER_TIMEOUT,
        };
        if read_header_timeout.is_zero() {
            return Err(validation(
                "server.read_header_timeout_seconds must be greater than zero".into(),
            ));
        }
        let max_header_bytes = server.max_header_bytes.unwrap_or(DEFAULT_MAX_HEADER_BYTES);
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
        let body_read_timeout = match server.body_read_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_BODY_READ_TIMEOUT,
        };
        if body_read_timeout.is_zero() {
            return Err(validation(
                "server.body_read_timeout_seconds must be greater than zero".into(),
            ));
        }
        let upstream_response_header_timeout = match server.upstream_response_header_timeout_seconds
        {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_UPSTREAM_RESPONSE_HEADER_TIMEOUT,
        };
        if upstream_response_header_timeout.is_zero() {
            return Err(validation(
                "server.upstream_response_header_timeout_seconds must be greater than zero".into(),
            ));
        }
        let first_event_timeout = match server.first_event_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_FIRST_EVENT_TIMEOUT,
        };
        if first_event_timeout.is_zero() {
            return Err(validation(
                "server.first_event_timeout_seconds must be greater than zero".into(),
            ));
        }
        let inter_event_timeout = match server.inter_event_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_INTER_EVENT_TIMEOUT,
        };
        if inter_event_timeout.is_zero() {
            return Err(validation(
                "server.inter_event_timeout_seconds must be greater than zero".into(),
            ));
        }
        let mut model_event_timeouts = BTreeMap::new();
        for (model, policy) in server.model_event_timeouts {
            if model.trim().is_empty() || model.len() > MAX_MODEL_BYTES {
                return Err(validation(
                    "server.model_event_timeouts model names must be non-blank and at most 256 bytes"
                        .into(),
                ));
            }
            let model_first = Duration::from_secs(
                policy
                    .first_event_timeout_seconds
                    .unwrap_or(first_event_timeout.as_secs()),
            );
            let model_inter = Duration::from_secs(
                policy
                    .inter_event_timeout_seconds
                    .unwrap_or(inter_event_timeout.as_secs()),
            );
            if model_first.is_zero() || model_inter.is_zero() {
                return Err(validation(
                    "server.model_event_timeouts values must be greater than zero".into(),
                ));
            }
            model_event_timeouts.insert(
                model,
                EventTimeouts {
                    first_event_timeout: model_first,
                    inter_event_timeout: model_inter,
                },
            );
        }
        let upstream_error_body_timeout = match server.upstream_error_body_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_UPSTREAM_ERROR_BODY_TIMEOUT,
        };
        if upstream_error_body_timeout.is_zero() {
            return Err(validation(
                "server.upstream_error_body_timeout_seconds must be greater than zero".into(),
            ));
        }
        let response_write_timeout = match server.response_write_timeout_seconds {
            Some(seconds) => Duration::from_secs(seconds),
            None => DEFAULT_RESPONSE_WRITE_TIMEOUT,
        };
        if response_write_timeout.is_zero() {
            return Err(validation(
                "server.response_write_timeout_seconds must be greater than zero".into(),
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
            gateway_keys,
            keys,
            key_aliases,
            models,
            model_sync: ModelSync {
                enabled: model_sync_enabled,
                interval: model_sync_interval,
            },
            limits: Limits {
                max_concurrency,
                max_queue,
                queue_wait_timeout,
                max_body_bytes,
                max_inflight_body_bytes,
            },
            key_failure_handling: KeyFailureHandling {
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
            usage_history_dir,
            audit: Audit {
                path: audit_path,
                queue_capacity: audit_queue_capacity,
            },
            server: Server {
                read_header_timeout,
                max_header_bytes,
                body_read_timeout,
                upstream_response_header_timeout,
                first_event_timeout,
                inter_event_timeout,
                model_event_timeouts,
                upstream_error_body_timeout,
                response_write_timeout,
                max_connections,
            },
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawConfig {
    gateway_token: Option<String>,
    gateway_keys: Option<Vec<RawGatewayKey>>,
    listen: Option<RawListen>,
    keys: Option<Vec<RawKey>>,
    models: Option<Vec<String>>,
    model_sync: Option<RawModelSync>,
    limits: Option<RawLimits>,
    key_failure_handling: Option<RawKeyFailureHandling>,
    usage: Option<RawUsage>,
    usage_history_dir: Option<String>,
    audit: Option<RawAudit>,
    server: Option<RawServer>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGatewayKey {
    name: String,
    token: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawKey {
    Plain(String),
    Named { key: String, name: Option<String> },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawModelSync {
    enabled: Option<bool>,
    interval_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawUsage {
    soft_threshold_percent: Option<f64>,
    poll_interval_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawListen {
    host: Option<String>,
    port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawLimits {
    max_concurrency: Option<usize>,
    max_queue: Option<usize>,
    queue_wait_timeout_seconds: Option<u64>,
    max_body_bytes: Option<usize>,
    max_inflight_body_bytes: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawKeyFailureHandling {
    /// Retained only to provide a redacted migration error for an old field.
    soft_threshold: Option<yaml_serde::Value>,
    backoff_initial_seconds: Option<u64>,
    backoff_max_seconds: Option<u64>,
    breaker_threshold: Option<usize>,
    breaker_cooldown_seconds: Option<u64>,
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
    read_header_timeout_seconds: Option<u64>,
    max_header_bytes: Option<usize>,
    body_read_timeout_seconds: Option<u64>,
    upstream_response_header_timeout_seconds: Option<u64>,
    first_event_timeout_seconds: Option<u64>,
    inter_event_timeout_seconds: Option<u64>,
    #[serde(default)]
    model_event_timeouts: BTreeMap<String, RawEventTimeouts>,
    upstream_error_body_timeout_seconds: Option<u64>,
    response_write_timeout_seconds: Option<u64>,
    max_connections: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
struct RawEventTimeouts {
    first_event_timeout_seconds: Option<u64>,
    inter_event_timeout_seconds: Option<u64>,
}

#[cfg(test)]
mod upstream_allowlist_tests {
    use super::*;

    #[test]
    fn built_in_upstream_has_one_https_origin_without_query_or_fragment() {
        let base = url::Url::parse(OPENCODE_GO_BASE_URL).unwrap();
        assert_eq!(base.scheme(), "https");
        #[cfg(not(feature = "loadtest-insecure-upstream"))]
        assert_eq!(base.host_str(), Some("opencode.ai"));
        #[cfg(not(feature = "loadtest-insecure-upstream"))]
        assert_eq!(base.path(), "/zen/go/");
        #[cfg(feature = "loadtest-insecure-upstream")]
        assert_eq!(base.host_str(), Some("127.0.0.1"));
        #[cfg(feature = "loadtest-insecure-upstream")]
        assert_eq!(base.path(), "/");
        assert!(base.query().is_none());
        assert!(base.fragment().is_none());
    }

    #[test]
    fn upstream_api_allowlist_builds_only_fixed_paths() {
        let base = url::Url::parse(OPENCODE_GO_BASE_URL).unwrap();
        #[cfg(not(feature = "loadtest-insecure-upstream"))]
        let expected_prefix = "/zen/go";
        #[cfg(feature = "loadtest-insecure-upstream")]
        let expected_prefix = "";
        assert_eq!(
            upstream_api_url(&base, UpstreamApi::ChatCompletions).path(),
            format!("{expected_prefix}/v1/chat/completions")
        );
        assert_eq!(
            upstream_api_url(&base, UpstreamApi::Messages).path(),
            format!("{expected_prefix}/v1/messages")
        );
        assert_eq!(
            upstream_api_url(&base, UpstreamApi::Responses).path(),
            format!("{expected_prefix}/v1/responses")
        );
        assert_eq!(
            upstream_api_url(&base, UpstreamApi::Usage).path(),
            format!("{expected_prefix}/v1/usage")
        );
    }
}

#[cfg(all(test, unix))]
mod config_file_security_tests {
    use super::secure_config_metadata;

    #[test]
    fn owner_mismatch_is_rejected_even_for_a_regular_mode_0600_file() {
        assert!(!secure_config_metadata(true, 0o600, 1001, 1000));
    }
}
