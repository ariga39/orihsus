//! Hot configuration reload: watch the config file's parent directory with
//! `notify` (so editor temp+rename and atomic replacement are supported),
//! debounce event storms through a small bounded channel, re-load and fully
//! validate the config, and hand the hot-variable fields to a target as one
//! consistent snapshot. Invalid or partially-written configs keep the
//! last-known-good configuration active.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use notify::{RecursiveMode, Watcher};

use crate::config::{self, Config, Secret};

/// Capacity of the bridge from the notify watcher to the async worker. Bounded
/// so an event storm can never accumulate without limit; a dropped event only
/// means "re-read the file on the next wake-up".
const EVENT_BUFFER: usize = 16;

/// Opaque marker that an [`ApplyFn`] callback refused the reload. It carries no
/// data at all, so an apply failure can never smuggle secret material into the
/// status snapshot; the module records a single static message instead.
#[derive(Debug)]
pub struct ApplyError;

/// Message recorded when an apply callback fails.
const APPLY_FAILED_MSG: &str = "config apply failed";

/// Static, desensitized stderr summaries emitted when a reload fails. They
/// never contain the `ConfigError` display, a filesystem path or a config
/// value — operators get the fact plus the static reason, and detailed (still
/// redacted) state stays in [`StatusSnapshot`]. Success emits nothing.
const RELOAD_FAILED_SUMMARY: &str =
    "orihsus: config reload failed; keeping the previous configuration";
const RELOAD_REFUSED_SUMMARY: &str = "orihsus: config reload refused: a restart is required";
const RELOAD_APPLY_SUMMARY: &str =
    "orihsus: config apply failed; keeping the previous configuration";

/// A consistent snapshot of the fields that may be hot-swapped. The listener is
/// intentionally absent: they are not hot-reloadable in this version and are
/// guarded separately.
#[derive(Debug, Clone)]
pub struct RuntimeConfigSnapshot {
    pub gateway_token: Secret,
    pub keys: Vec<Secret>,
    pub models: Vec<String>,
    pub model_sync: config::ModelSync,
    pub limits: config::Limits,
    pub key_failure_handling: config::KeyFailureHandling,
}

impl RuntimeConfigSnapshot {
    pub fn from_config(cfg: &Config) -> RuntimeConfigSnapshot {
        RuntimeConfigSnapshot {
            gateway_token: cfg.gateway_token.clone(),
            keys: cfg.keys.clone(),
            models: cfg.models.clone(),
            model_sync: cfg.model_sync.clone(),
            limits: cfg.limits.clone(),
            key_failure_handling: cfg.key_failure_handling.clone(),
        }
    }
}

/// The apply callback type: receives a consistent snapshot of the hot fields
/// and atomically applies them. Returning `Err` keeps the last-known-good
/// configuration active and is recorded as a failed reload. A `ReloadTarget`
/// struct can be passed by wrapping it: `move |snap| target.apply(snap)`.
type ApplyFn = dyn Fn(&RuntimeConfigSnapshot) -> Result<(), ApplyError> + Send + Sync;

/// Observable reload state. `last_error` is always redacted.
#[derive(Debug, Clone)]
pub struct StatusSnapshot {
    pub successful_reloads: u64,
    pub failed_reloads: u64,
    pub last_error: Option<String>,
    pub last_loaded_at: Option<SystemTime>,
    pub needs_restart: bool,
}

/// Errors from [`HotReloader::start`]. Contains only paths and OS errors,
/// never configuration values.
#[derive(Debug)]
pub enum HotReloadError {
    /// Could not register a watcher on the config's parent directory.
    Watch {
        path: PathBuf,
        source: notify::Error,
    },
    /// Could not spawn the forwarding thread.
    ThreadSpawn(std::io::Error),
}

impl fmt::Display for HotReloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HotReloadError::Watch { path, source } => {
                write!(
                    f,
                    "cannot watch config directory {}: {}",
                    path.display(),
                    source
                )
            }
            HotReloadError::ThreadSpawn(e) => write!(f, "cannot spawn config watcher thread: {e}"),
        }
    }
}

impl std::error::Error for HotReloadError {}

/// Handle to a running config reloader. Dropping it stops the watcher and the
/// worker task.
pub struct HotReloader {
    watcher: Option<notify::RecommendedWatcher>,
    watcher_thread: Option<std::thread::JoinHandle<()>>,
    worker: tokio::task::JoinHandle<()>,
    status: Arc<StatusInner>,
    trigger: tokio::sync::mpsc::Sender<()>,
}

/// A cheap handle that can force the reloader's worker to re-read and apply
/// the config on demand (the SIGHUP path uses it in addition to the file-watch
/// triggers). Best-effort: a full bridge channel only means the worker is
/// already awake and will re-read the file on its current debounce window.
#[derive(Clone)]
pub struct ReloadTrigger(tokio::sync::mpsc::Sender<()>);

impl ReloadTrigger {
    /// Wake the worker now, never blocking.
    pub fn fire(&self) {
        let _ = self.0.try_send(());
    }
}

#[derive(Default)]
struct StatusInner {
    state: std::sync::Mutex<StatusState>,
}

impl StatusInner {
    fn record_success(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.successful_reloads += 1;
        state.last_error = None;
        state.last_loaded_at = Some(SystemTime::now());
        state.needs_restart = false;
    }

    fn record_failure(&self, msg: String) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed_reloads += 1;
        state.last_error = Some(msg);
    }

    fn record_needs_restart(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.failed_reloads += 1;
        state.last_error = Some(NEEDS_RESTART_MSG.to_string());
        state.needs_restart = true;
    }
}

/// Message recorded when a valid config changes a non-hot field.
const NEEDS_RESTART_MSG: &str =
    "config requires a restart: static settings changed; reload refused";

#[derive(Default)]
struct StatusState {
    successful_reloads: u64,
    failed_reloads: u64,
    last_error: Option<String>,
    last_loaded_at: Option<SystemTime>,
    needs_restart: bool,
}

impl HotReloader {
    /// Watch `config_path`'s parent directory and react to changes of the
    /// config file only. The initial configuration is `initial` (already loaded
    /// by the caller); the reloader never applies anything at startup and only
    /// reacts to later changes. Must be called from within a tokio runtime.
    pub fn start(
        config_path: impl AsRef<Path>,
        debounce: Duration,
        initial: Config,
        apply: impl Fn(&RuntimeConfigSnapshot) -> Result<(), ApplyError> + Send + Sync + 'static,
    ) -> Result<HotReloader, HotReloadError> {
        assert!(!debounce.is_zero(), "debounce must be non-zero");
        // Resolve lexically against the cwd so a relative config path compares
        // equal to the (root.join(name)) paths notify reports.
        let config_path = std::path::absolute(config_path.as_ref())
            .unwrap_or_else(|_| config_path.as_ref().to_path_buf());
        let parent = match config_path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };

        let (bridge_tx, bridge_rx) = tokio::sync::mpsc::channel(EVENT_BUFFER);
        let trigger = bridge_tx.clone();
        let (watcher, watcher_thread) = build_watcher(&parent, config_path.clone(), bridge_tx)?;
        let status = Arc::new(StatusInner::default());
        let apply: Arc<ApplyFn> = Arc::new(apply);
        let worker = tokio::spawn(worker(
            bridge_rx,
            config_path,
            debounce,
            initial,
            apply,
            Arc::clone(&status),
        ));

        Ok(HotReloader {
            watcher: Some(watcher),
            watcher_thread: Some(watcher_thread),
            worker,
            status,
            trigger,
        })
    }

    /// A handle that forces a config reload on demand, independent of the
    /// file watcher. The SIGHUP path calls [`ReloadTrigger::fire`] after
    /// reopening the audit log so a traditional reload-on-HUP still happens.
    pub fn reload_trigger(&self) -> ReloadTrigger {
        ReloadTrigger(self.trigger.clone())
    }

    /// Read-only snapshot of reload state.
    pub fn status(&self) -> StatusSnapshot {
        let state = self
            .status
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        StatusSnapshot {
            successful_reloads: state.successful_reloads,
            failed_reloads: state.failed_reloads,
            last_error: state.last_error.clone(),
            last_loaded_at: state.last_loaded_at,
            needs_restart: state.needs_restart,
        }
    }

    /// Stop the watcher and the worker (equivalent to dropping the handle).
    pub fn shutdown(self) {}
}

impl Drop for HotReloader {
    fn drop(&mut self) {
        self.worker.abort();
        self.watcher = None;
        if let Some(thread) = self.watcher_thread.take() {
            let _ = thread.join();
        }
    }
}

impl fmt::Debug for HotReloader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotReloader")
            .field("status", &self.status())
            .finish()
    }
}

fn build_watcher(
    parent: &Path,
    target: PathBuf,
    tx: tokio::sync::mpsc::Sender<()>,
) -> Result<(notify::RecommendedWatcher, std::thread::JoinHandle<()>), HotReloadError> {
    let (ntx, nrx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(ntx).map_err(|source| HotReloadError::Watch {
        path: parent.to_path_buf(),
        source,
    })?;
    watcher
        .watch(parent, RecursiveMode::NonRecursive)
        .map_err(|source| HotReloadError::Watch {
            path: parent.to_path_buf(),
            source,
        })?;
    let handle = std::thread::Builder::new()
        .name("orihsus-config-watch".into())
        .spawn(move || {
            for event in nrx {
                let Ok(event) = event else { continue };
                if event_targets(&event, &target) {
                    let _ = tx.try_send(());
                }
            }
        })
        .map_err(HotReloadError::ThreadSpawn)?;
    Ok((watcher, handle))
}

/// True when the event mutates `target` itself (create/modify/remove, or a move
/// into/out of that path). Read-only `Access`/`Other` events are ignored: they
/// are produced by our own re-reads and would otherwise cause endless
/// self-triggering.
fn event_targets(event: &notify::Event, target: &Path) -> bool {
    is_relevant_kind(event.kind) && event.paths.iter().any(|p| p == target)
}

fn is_relevant_kind(kind: notify::EventKind) -> bool {
    matches!(
        kind,
        notify::EventKind::Create(_) | notify::EventKind::Modify(_) | notify::EventKind::Remove(_)
    )
}

async fn worker(
    mut rx: tokio::sync::mpsc::Receiver<()>,
    config_path: PathBuf,
    debounce: Duration,
    initial: Config,
    apply: Arc<ApplyFn>,
    status: Arc<StatusInner>,
) {
    loop {
        if rx.recv().await.is_none() {
            return;
        }
        // Debounce: wait for a quiet window of `debounce` with no further
        // events before re-reading. Rapid bursts collapse into one reload.
        loop {
            tokio::time::sleep(debounce).await;
            while rx.try_recv().is_ok() {}
            if rx.is_empty() {
                break;
            }
        }
        let cfg = match config::load(&config_path) {
            Ok(cfg) => cfg,
            Err(e) => {
                status.record_failure(format!("cannot reload config: {e}"));
                eprintln!("{RELOAD_FAILED_SUMMARY}");
                continue;
            }
        };
        if non_hot_changed(&cfg, &initial) {
            status.record_needs_restart();
            eprintln!("{RELOAD_REFUSED_SUMMARY}");
            continue;
        }
        let snapshot = RuntimeConfigSnapshot::from_config(&cfg);
        match apply(&snapshot) {
            Ok(()) => status.record_success(),
            Err(_) => {
                status.record_failure(APPLY_FAILED_MSG.to_string());
                eprintln!("{RELOAD_APPLY_SUMMARY}");
            }
        }
    }
}

/// True when the loaded config changes a field that cannot be hot-swapped
/// (listen, limits/queue shape, key-failure policy, audit and HTTP server
/// hardening). Such a reload is refused wholesale: applying only the hot
/// fields would leave a half-applied configuration.
fn non_hot_changed(cfg: &Config, baseline: &Config) -> bool {
    cfg.listen != baseline.listen
        || cfg.limits != baseline.limits
        || cfg.key_failure_handling != baseline.key_failure_handling
        || cfg.usage != baseline.usage
        || cfg.usage_history_dir != baseline.usage_history_dir
        || cfg.audit != baseline.audit
        || cfg.server != baseline.server
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{
        AccessKind, AccessMode, CreateKind, DataChange, EventKind, ModifyKind, RemoveKind,
        RenameMode,
    };

    fn target() -> PathBuf {
        PathBuf::from("/etc/orihsus/config.yaml")
    }

    #[test]
    fn access_and_other_events_never_trigger() {
        let t = target();
        for kind in [
            EventKind::Access(AccessKind::Read),
            EventKind::Access(AccessKind::Close(AccessMode::Write)),
            EventKind::Other,
        ] {
            let event = notify::Event::new(kind).add_path(t.clone());
            assert!(!event_targets(&event, &t), "kind {kind:?} must not trigger");
        }
    }

    #[test]
    fn mutations_of_the_target_trigger() {
        let t = target();
        for kind in [
            EventKind::Create(CreateKind::Any),
            EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            EventKind::Remove(RemoveKind::Any),
        ] {
            let event = notify::Event::new(kind).add_path(t.clone());
            assert!(event_targets(&event, &t), "kind {kind:?} must trigger");
        }
    }

    #[test]
    fn sibling_paths_never_trigger() {
        let t = target();
        let sibling = PathBuf::from("/etc/orihsus/other.yaml");
        let event = notify::Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)))
            .add_path(sibling.clone());
        assert!(!event_targets(&event, &t));
    }
}
