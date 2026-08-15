//! orihsus — OpenCode Go key rotation gateway (single binary entry point).
//!
//! Startup order: CLI parse → root guard → config load → assemble runtime →
//! bind loopback HTTP → serve with graceful shutdown → hot reload → flush
//! audit. Any startup failure prints a redacted message to stderr and exits
//! non-zero; secrets never reach the logs.

use std::future::Future;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use orihsus::app::assemble;
use orihsus::config::{self, Config};
use orihsus::gateway::RuntimeState;
use orihsus::hot_reload::{ApplyError, HotReloader};
use orihsus::server::{serve, Http1Limits, ServerSettings};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const DEFAULT_CONFIG_PATH: &str = "/etc/orihsus/config.yaml";
/// Debounce for the config watcher.
const RELOAD_DEBOUNCE: Duration = Duration::from_secs(1);
/// How long to drain in-flight connections during graceful shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// CLI argument errors. Static messages only; argument values are never echoed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliError {
    Help,
    Version,
    UnknownOption,
    MissingValue,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Help => Ok(()),
            CliError::Version => Ok(()),
            CliError::UnknownOption => write!(f, "unknown option (see --help)"),
            CliError::MissingValue => write!(f, "--config requires a path argument"),
        }
    }
}

fn usage() -> &'static str {
    "orihsus — OpenCode Go key rotation gateway\n\
     \n\
     USAGE:\n\
     \x20   orihsus [--config <PATH>]\n\
     \n\
     OPTIONS:\n\
     \x20   --config <PATH>    path to the YAML config (default: /etc/orihsus/config.yaml)\n\
     \x20   -h, --help         print this help\n\
     \x20   -V, --version      print version and build commit"
}

fn parse_args(args: &[String]) -> Result<PathBuf, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "-h" || arg == "--help" {
            return Err(CliError::Help);
        }
        if arg == "-V" || arg == "--version" {
            return Err(CliError::Version);
        }
        if arg == "--config" {
            let value = iter.next().ok_or(CliError::MissingValue)?;
            config = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = arg.strip_prefix("--config=") {
            if value.is_empty() {
                return Err(CliError::MissingValue);
            }
            config = Some(PathBuf::from(value));
            continue;
        }
        return Err(CliError::UnknownOption);
    }
    Ok(config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH)))
}

/// Pure seam over the effective user id: reject running as root.
pub fn reject_root_for(euid: u32) -> Result<(), MainError> {
    if euid == 0 {
        Err(MainError::Root)
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn current_euid() -> u32 {
    // SAFETY: geteuid takes no arguments and always succeeds.
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn current_euid() -> u32 {
    1000
}

fn reject_root() -> Result<(), MainError> {
    reject_root_for(current_euid())
}

/// Top-level startup/shutdown errors. Never echoes keys or tokens.
#[derive(Debug)]
pub enum MainError {
    Root,
    Config(config::ConfigError),
    Bootstrap(orihsus::app::BootstrapError),
    HotReload(orihsus::hot_reload::HotReloadError),
    Io(std::io::Error),
    ServerTask(tokio::task::JoinError),
}

impl std::fmt::Display for MainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MainError::Root => write!(f, "refusing to run as root; use a dedicated user"),
            MainError::Config(e) => write!(f, "{e}"),
            MainError::Bootstrap(e) => write!(f, "{e}"),
            MainError::HotReload(e) => write!(f, "{e}"),
            MainError::Io(e) => write!(f, "I/O error: {e}"),
            MainError::ServerTask(e) => write!(f, "server task failed: {e}"),
        }
    }
}

impl std::error::Error for MainError {}

impl From<config::ConfigError> for MainError {
    fn from(e: config::ConfigError) -> Self {
        MainError::Config(e)
    }
}

impl From<orihsus::app::BootstrapError> for MainError {
    fn from(e: orihsus::app::BootstrapError) -> Self {
        MainError::Bootstrap(e)
    }
}

impl From<orihsus::hot_reload::HotReloadError> for MainError {
    fn from(e: orihsus::hot_reload::HotReloadError) -> Self {
        MainError::HotReload(e)
    }
}

impl From<std::io::Error> for MainError {
    fn from(e: std::io::Error) -> Self {
        MainError::Io(e)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config_path = match parse_args(&args) {
        Ok(p) => p,
        Err(CliError::Help) => {
            println!("{}", usage());
            return ExitCode::SUCCESS;
        }
        Err(CliError::Version) => {
            println!(
                "orihsus {} commit {}",
                env!("CARGO_PKG_VERSION"),
                env!("ORIHSUS_COMMIT_HASH")
            );
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("orihsus: {e}");
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
    };

    let cfg = match load_config(&config_path).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("orihsus: {e}");
            return ExitCode::FAILURE;
        }
    };

    match run(cfg, config_path).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("orihsus: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Root guard + config load (kept separate so tests can exercise each seam).
async fn load_config(config_path: &PathBuf) -> Result<Config, MainError> {
    reject_root()?;
    Ok(config::load(config_path)?)
}

async fn run(cfg: Config, config_path: PathBuf) -> Result<ExitCode, MainError> {
    let (mut runtime, router) = assemble(&cfg)?;

    let listener = tokio::net::TcpListener::bind(cfg.listen)
        .await
        .map_err(MainError::Io)?;

    let settings = ServerSettings::from_config(&cfg.server);
    let limits = Http1Limits::from_settings(&settings);

    let reloader = {
        let initial = cfg.clone();
        let pool = runtime.pool.clone();
        let store = runtime.runtime.clone();
        let usage_keys = runtime
            .usage_monitor
            .as_ref()
            .expect("assemble always starts usage monitor")
            .keys_handle();
        // Only hot fields are applied atomically: keys + token/max_body/
        // models are swapped under one lock, so a failure never half-applies and
        // a request never observes mixed generations. Non-hot changes
        // (limits/key-failure handling/audit/server/listen) are refused by the reloader
        // before this callback ever runs.
        HotReloader::start(&config_path, RELOAD_DEBOUNCE, initial, move |snap| {
            store
                .update_with_keys(
                    &pool,
                    snap.keys.clone(),
                    RuntimeState {
                        gateway_token: snap.gateway_token.clone(),
                        base_url: url::Url::parse(orihsus::config::OPENCODE_GO_BASE_URL)
                            .expect("built-in OpenCode Go base URL is valid"),
                        max_body_bytes: snap.limits.max_body_bytes,
                        models: snap.models.clone(),
                    },
                )
                .map_err(|_| ApplyError)?;
            usage_keys.replace_keys(snap.keys.clone());
            Ok(())
        })?
    };

    // SIGHUP = reopen the audit log (logrotate) and force a config reload.
    // The reopen is best-effort: on failure we log once and keep writing to
    // the current file, but the config reload still runs either way. The task
    // handle is saved so cleanup below can stop it (no hanging reference race).
    let mut sighup_handle = {
        #[cfg(unix)]
        {
            let audit = Arc::downgrade(&runtime.audit);
            let audit_path = cfg.audit.path.clone();
            let reload = reloader.reload_trigger();
            Some(tokio::spawn(async move {
                let mut hangup =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                        Ok(sig) => sig,
                        Err(e) => {
                            eprintln!("orihsus: cannot install SIGHUP handler: {e}");
                            return;
                        }
                    };
                loop {
                    hangup.recv().await;
                    match audit.upgrade() {
                        Some(audit) => {
                            if let Err(e) = on_sighup(&audit, &audit_path, &reload).await {
                                eprintln!(
                                    "orihsus: audit reopen failed, keeping the current file: {e}"
                                );
                            }
                        }
                        None => reload.fire(),
                    }
                }
            }))
        }
        #[cfg(not(unix))]
        {
            None
        }
    };

    eprintln!(
        "orihsus: serving HTTP on {} behind nginx with {} key(s); watching {}",
        cfg.listen,
        cfg.keys.len(),
        config_path.display()
    );

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let connection_cap = Arc::new(tokio::sync::Semaphore::new(cfg.server.max_connections));
    let server = tokio::spawn(serve(
        listener,
        router,
        limits,
        connection_cap,
        DRAIN_TIMEOUT,
        async move {
            let _ = stop_rx.await;
        },
    ));

    // Never return early before cleanup: a server error or panic must still
    // stop the SIGHUP task and flush the audit (bounded) before the process
    // exits; the original lifecycle result is returned afterwards.
    let lifecycle_result = run_lifecycle(
        shutdown_signal(),
        &runtime.queue,
        move || reloader.shutdown(),
        server,
        stop_tx,
    )
    .await;

    // Stop the SIGHUP task so it can no longer race cleanup (a hangup arriving
    // mid-shutdown must not hold the writer or block the exit).
    if let Some(handle) = sighup_handle.take() {
        handle.abort();
        let _ = handle.await;
    }

    if let Some(monitor) = runtime.usage_monitor.take() {
        monitor.shutdown().await;
    }

    // Audit flush now that the router/server are fully dropped. Bounded: a
    // writer stuck on disk I/O must not hang the graceful shutdown — after
    // AUDIT_SHUTDOWN_TIMEOUT the process still exits, warning that records
    // accepted but not yet flushed may be lost.
    flush_audit_at_shutdown(runtime.audit).await;

    lifecycle_result?;
    Ok(ExitCode::SUCCESS)
}

/// Post-lifecycle audit flush seam, run unconditionally — on a clean signal
/// AND on a server error — so accepted records are flushed (bounded) before
/// the process exits. If the writer is still referenced (a race), dropping it
/// is non-blocking (`AuditWriter`'s `Drop` never joins) and unflushed records
/// may be lost.
async fn flush_audit_at_shutdown(audit: Arc<orihsus::audit::AuditWriter>) {
    if let Ok(writer) = Arc::try_unwrap(audit) {
        if let Err(e) = writer.shutdown_bounded().await {
            eprintln!("orihsus: audit flush failed: {e}");
        }
    } else {
        eprintln!(
            "orihsus: audit writer still referenced at shutdown; dropping is non-blocking and unflushed records may be lost"
        );
    }
}

/// SIGHUP handling seam: reopen the audit log at `path` (best-effort — a
/// failed open keeps the previous file active) and then always force a config
/// reload. Returns the reopen result so the caller can log it once without the
/// failure ever blocking the reload.
async fn on_sighup(
    audit: &Arc<orihsus::audit::AuditWriter>,
    path: &std::path::Path,
    reload: &orihsus::hot_reload::ReloadTrigger,
) -> Result<(), orihsus::audit::AuditError> {
    let result = audit.reopen(path).await;
    reload.fire();
    result
}

/// Coordinate a single server lifecycle. The shutdown signal and the server
/// task race: on a signal, stop admission and config reload FIRST, then ask the
/// server to stop accepting and await its (internally draining) completion. If
/// the server instead dies (I/O error or panic) before any signal, end the
/// lifecycle immediately — stop new work and propagate the error — without
/// waiting for a shutdown that may never arrive. Server JoinError / serve I/O
/// errors map to a redacted `MainError` so main exits non-zero instead of
/// swallowing them.
async fn run_lifecycle(
    shutdown: impl Future<Output = ()>,
    queue: &orihsus::queue::AdmissionQueue,
    close_reloader: impl FnOnce(),
    mut server: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    stop: tokio::sync::oneshot::Sender<()>,
) -> Result<(), MainError> {
    let ended = tokio::select! {
        _ = shutdown => None,
        result = &mut server => Some(map_server_result(result)),
    };
    let mut close_reloader = Some(close_reloader);
    match ended {
        Some(result) => {
            // Server died before the signal: stop new work now and propagate,
            // never waiting for a signal that may not come.
            queue.close();
            close_reloader.take().unwrap()();
            result
        }
        None => {
            // Signal first: normal graceful drain. Stop new work before the
            // server stops accepting; serve drains in-flight connections.
            queue.close();
            close_reloader.take().unwrap()();
            let _ = stop.send(());
            map_server_result(server.await)
        }
    }
}

fn map_server_result(
    result: Result<Result<(), std::io::Error>, tokio::task::JoinError>,
) -> Result<(), MainError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(MainError::Io(e)),
        Err(join) => Err(MainError::ServerTask(join)),
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_flag_and_default_path() {
        assert_eq!(
            parse_args(&[]).unwrap(),
            PathBuf::from("/etc/orihsus/config.yaml")
        );
        assert_eq!(
            parse_args(&["--config".to_string(), "/tmp/x.yaml".to_string()]).unwrap(),
            PathBuf::from("/tmp/x.yaml")
        );
        assert_eq!(
            parse_args(&["--config=/tmp/y.yaml".to_string()]).unwrap(),
            PathBuf::from("/tmp/y.yaml")
        );
    }

    #[test]
    fn unknown_or_malformed_args_are_rejected() {
        assert!(matches!(
            parse_args(&["--bogus".to_string()]),
            Err(CliError::UnknownOption)
        ));
        assert!(matches!(
            parse_args(&["--config".to_string()]),
            Err(CliError::MissingValue)
        ));
        assert!(matches!(
            parse_args(&["--config=".to_string()]),
            Err(CliError::MissingValue)
        ));
    }

    #[test]
    fn help_requests_usage() {
        assert!(matches!(
            parse_args(&["-h".to_string()]),
            Err(CliError::Help)
        ));
        assert!(matches!(
            parse_args(&["--help".to_string()]),
            Err(CliError::Help)
        ));
    }

    #[test]
    fn version_flags_request_version_output() {
        assert!(matches!(
            parse_args(&["-V".to_string()]),
            Err(CliError::Version)
        ));
        assert!(matches!(
            parse_args(&["--version".to_string()]),
            Err(CliError::Version)
        ));
    }

    #[test]
    fn root_guard_rejects_root_and_accepts_others() {
        assert!(reject_root_for(0).is_err());
        assert!(reject_root_for(1000).is_ok());
        assert!(reject_root_for(65534).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_stops_admission_and_reloader_before_the_server() {
        use std::sync::Arc;

        let queue = Arc::new(orihsus::queue::AdmissionQueue::new(
            2,
            2,
            Duration::from_secs(30),
        ));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let q2 = queue.clone();
        let ev_server = events.clone();
        let server = tokio::spawn(async move {
            let _ = stop_rx.await;
            ev_server.lock().unwrap().push("server-stopped");
            assert!(
                q2.is_closed(),
                "the queue must be closed before the server stops accepting"
            );
            Ok::<(), std::io::Error>(())
        });

        let (sig_tx, sig_rx) = tokio::sync::oneshot::channel();
        let ev_reloader = events.clone();
        let result = run_lifecycle(
            async move {
                let _ = sig_rx.await;
            },
            &queue,
            move || ev_reloader.lock().unwrap().push("reloader-stopped"),
            server,
            stop_tx,
        );
        sig_tx.send(()).unwrap();
        result.await.unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["reloader-stopped", "server-stopped"],
            "queue.close and reloader.shutdown must happen-before the server stop"
        );
        assert!(queue.is_closed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_propagates_io_errors_from_the_server() {
        let queue = Arc::new(orihsus::queue::AdmissionQueue::new(
            2,
            2,
            Duration::from_secs(30),
        ));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let _ = stop_rx.await;
            Err::<(), std::io::Error>(std::io::Error::other("server accept exploded"))
        });
        let (sig_tx, sig_rx) = tokio::sync::oneshot::channel();
        let result = run_lifecycle(
            async move {
                let _ = sig_rx.await;
            },
            &queue,
            || {},
            server,
            stop_tx,
        );
        sig_tx.send(()).unwrap();
        match result.await {
            Err(MainError::Io(e)) => {
                assert!(
                    e.to_string().contains("server accept exploded"),
                    "server I/O error must surface: {e}"
                )
            }
            other => panic!("expected MainError::Io, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_propagates_server_panics() {
        let queue = Arc::new(orihsus::queue::AdmissionQueue::new(
            2,
            2,
            Duration::from_secs(30),
        ));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let _ = stop_rx.await;
            panic!("server task panicked");
        });
        let (sig_tx, sig_rx) = tokio::sync::oneshot::channel();
        let result = run_lifecycle(
            async move {
                let _ = sig_rx.await;
            },
            &queue,
            || {},
            server,
            stop_tx,
        );
        sig_tx.send(()).unwrap();
        match result.await {
            Err(MainError::ServerTask(_)) => {}
            other => panic!("expected MainError::ServerTask, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_ends_when_the_server_dies_before_any_signal() {
        let queue = Arc::new(orihsus::queue::AdmissionQueue::new(
            2,
            2,
            Duration::from_secs(30),
        ));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
        let ev_reloader = events.clone();
        // The server dies immediately, before any shutdown signal ever fires.
        let server = tokio::spawn(async move {
            Err::<(), std::io::Error>(std::io::Error::other("server died early"))
        });

        // The shutdown signal never arrives; a lifecycle that waits for it
        // would hang forever, so bound the whole thing with a timeout.
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_lifecycle(
                std::future::pending(),
                &queue,
                move || ev_reloader.lock().unwrap().push("reloader-stopped"),
                server,
                stop_tx,
            ),
        )
        .await
        .expect("must not wait for a shutdown signal that never arrives");

        match result {
            Err(MainError::Io(e)) => assert!(
                e.to_string().contains("server died early"),
                "the server error must propagate: {e}"
            ),
            other => panic!("expected MainError::Io, got {other:?}"),
        }
        assert!(
            queue.is_closed(),
            "admission must be closed when the server dies"
        );
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["reloader-stopped"],
            "reloader must be stopped when the server dies"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_ends_when_the_server_panics_before_any_signal() {
        let queue = Arc::new(orihsus::queue::AdmissionQueue::new(
            2,
            2,
            Duration::from_secs(30),
        ));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (stop_tx, _stop_rx) = tokio::sync::oneshot::channel();
        let ev_reloader = events.clone();
        let server = tokio::spawn(async move {
            panic!("server task panicked early");
        });

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_lifecycle(
                std::future::pending(),
                &queue,
                move || ev_reloader.lock().unwrap().push("reloader-stopped"),
                server,
                stop_tx,
            ),
        )
        .await
        .expect("must not wait for a shutdown signal that never arrives");

        match result {
            Err(MainError::ServerTask(_)) => {}
            other => panic!("expected MainError::ServerTask, got {other:?}"),
        }
        assert!(
            queue.is_closed(),
            "admission must be closed when the server dies"
        );
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["reloader-stopped"],
            "reloader must be stopped when the server dies"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sighup_reopens_the_audit_file_and_always_triggers_a_config_reload() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::TempDir::new().unwrap();
        let audit_path = dir.path().join("audit.jsonl");
        let config = format!(
            "gateway_token: \"gway-secret\"\n\
             keys:\n  - \"key-1\"\n\
             audit:\n  path: \"{}\"\n",
            audit_path.display()
        );
        let cfg_path = dir.path().join("config.yaml");
        fs::write(&cfg_path, config).unwrap();
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).unwrap();
        let cfg = orihsus::config::load(&cfg_path).unwrap();

        let reloads = Arc::new(AtomicUsize::new(0));
        let reloads_apply = reloads.clone();
        let reloader = orihsus::hot_reload::HotReloader::start(
            &cfg_path,
            Duration::from_millis(150),
            cfg.clone(),
            move |_| {
                reloads_apply.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        let trigger = reloader.reload_trigger();
        let writer = Arc::new(orihsus::audit::AuditWriter::start(&audit_path, 8).unwrap());

        let record = |id: &str| orihsus::audit::AuditRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-08-14T12:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            request_id: id.to_string(),
            model: Some("deepseek-chat".to_string()),
            key_fingerprint: Some("f".to_string()),
            input_tokens: Some(1),
            output_tokens: Some(2),
            status: 200,
            outcome: None,
            latency: Duration::from_millis(5),
        };

        // Success: rotate away the live file and reopen onto the fresh path.
        let rotated = dir.path().join("audit.1.jsonl");
        fs::rename(&audit_path, &rotated).unwrap();
        fs::write(&audit_path, "").unwrap();
        assert!(
            on_sighup(&writer, &audit_path, &trigger).await.is_ok(),
            "a healthy reopen must succeed"
        );
        assert_eq!(
            writer.try_record(record("post-reopen")),
            orihsus::audit::Outcome::Accepted
        );

        // The worker debounces bursts, so wait out the first reload before
        // firing the next one; then a failed reopen must still reload.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while reloads.load(Ordering::SeqCst) < 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the first config reload must fire, got {}",
                reloads.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let blocked = dir.path().join("blocked.jsonl");
        fs::create_dir(&blocked).unwrap();
        assert!(
            on_sighup(&writer, &blocked, &trigger).await.is_err(),
            "a failed reopen must return Err"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while reloads.load(Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the config reload must still fire after a failed reopen, got {}",
                reloads.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let writer = match Arc::try_unwrap(writer) {
            Ok(writer) => writer,
            Err(_) => panic!("audit writer still referenced"),
        };
        writer.shutdown().unwrap();
        reloader.shutdown();

        let new_content = fs::read_to_string(&audit_path).unwrap();
        assert!(
            new_content.contains("post-reopen"),
            "records after a successful reopen must land in the new file"
        );
        let rotated_content = fs::read_to_string(&rotated).unwrap();
        assert!(
            !rotated_content.contains("post-reopen"),
            "the rotated file must not grow after reopen"
        );
    }

    /// Sink that signals `started` as soon as the writer begins writing a
    /// record, then blocks the writer thread until `gate` is dropped. `started`
    /// lets the test deterministically wait for the writer to be mid-write
    /// (its queue slot drained) before filling the queue again.
    struct BlockingSink {
        gate: std::sync::mpsc::Receiver<()>,
        started: std::sync::mpsc::Sender<()>,
    }

    impl std::io::Write for BlockingSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let _ = self.started.send(());
            let _ = self.gate.recv();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Releases the blocking gate on drop, including during a panic unwind.
    /// Declared after the writer so a failing assert drops it before the
    /// writer's `AuditWriter` Drop join can deadlock on a blocked thread.
    struct BlockingGate(Option<std::sync::mpsc::Sender<()>>);

    impl Drop for BlockingGate {
        fn drop(&mut self) {
            self.0.take();
        }
    }

    #[tokio::test]
    async fn on_sighup_fires_reload_even_when_reopen_skips_a_full_audit_queue() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // The writer blocks in `write` until the gate sender is dropped; the
        // `started` signal proves it is mid-write on the first record, so the
        // single queue slot is known-free and exactly one more record fills it.
        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let writer = Arc::new(
            orihsus::audit::AuditWriter::start_with_sink(
                Box::new(BlockingSink {
                    gate: gate_rx,
                    started: started_tx,
                }),
                1,
            )
            .unwrap(),
        );
        let gate = BlockingGate(Some(gate_tx));
        let record = || orihsus::audit::AuditRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-08-14T12:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            request_id: "q".to_string(),
            model: None,
            key_fingerprint: None,
            input_tokens: None,
            output_tokens: None,
            status: 503,
            outcome: None,
            latency: Duration::from_millis(5),
        };
        assert_eq!(
            writer.try_record(record()),
            orihsus::audit::Outcome::Accepted
        );
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the writer must be mid-write on its first record");
        assert_eq!(
            writer.try_record(record()),
            orihsus::audit::Outcome::Accepted
        );

        let dir = tempfile::TempDir::new().unwrap();
        let audit_path = dir.path().join("audit.jsonl");
        let cfg_path = dir.path().join("config.yaml");
        let config = format!(
            "gateway_token: \"gway-secret\"\n\
             keys:\n  - \"key-1\"\n\
             audit:\n  path: \"{}\"\n",
            audit_path.display()
        );
        fs::write(&cfg_path, config).unwrap();
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).unwrap();
        let cfg = orihsus::config::load(&cfg_path).unwrap();

        let reloads = Arc::new(AtomicUsize::new(0));
        let reloads_apply = reloads.clone();
        let reloader = orihsus::hot_reload::HotReloader::start(
            &cfg_path,
            Duration::from_millis(150),
            cfg.clone(),
            move |_| {
                reloads_apply.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        let trigger = reloader.reload_trigger();

        let result = on_sighup(&writer, &audit_path, &trigger).await;
        // Release the writer before any assert: a failing assert must not strand
        // the blocked writer and deadlock the AuditWriter Drop join.
        drop(gate);
        assert!(
            result.is_err(),
            "a full audit queue must make reopen fail fast: {result:?}"
        );

        // The config reload must fire unconditionally, even though reopen failed.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while reloads.load(Ordering::SeqCst) < 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the config reload must still fire after a failed reopen, got {}",
                reloads.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(writer);
        reloader.shutdown();
    }

    #[tokio::test]
    async fn audit_flush_seam_runs_and_flushes_after_a_lifecycle_error() {
        use std::fs;
        // The cleanup seam that main runs after `run_lifecycle` — even on a
        // server error — must actually flush the accepted records. The same
        // call runs unconditionally in `run()`, so a failed lifecycle no longer
        // skips the bounded audit flush.
        let dir = tempfile::TempDir::new().unwrap();
        let audit_path = dir.path().join("audit.jsonl");
        let writer = Arc::new(orihsus::audit::AuditWriter::start(&audit_path, 8).unwrap());
        let record = || orihsus::audit::AuditRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-08-14T12:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            request_id: "cleanup".to_string(),
            model: None,
            key_fingerprint: None,
            input_tokens: None,
            output_tokens: None,
            status: 200,
            outcome: None,
            latency: Duration::from_millis(5),
        };
        assert_eq!(
            writer.try_record(record()),
            orihsus::audit::Outcome::Accepted
        );

        flush_audit_at_shutdown(writer).await;

        let content = fs::read_to_string(&audit_path).unwrap();
        assert!(
            content.contains("cleanup"),
            "the cleanup seam must flush accepted records even on the error path"
        );
    }

    #[tokio::test]
    async fn aborting_the_sighup_task_returns_promptly() {
        // main's cleanup stops the SIGHUP task with abort()+await: a hangup
        // racing the shutdown must not block the exit. A task parked forever on
        // a channel recv (the same shape as the SIGHUP loop) must be cancelled
        // and awaited promptly.
        let (_tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let handle = tokio::spawn(async move {
            loop {
                let _ = rx.recv().await;
            }
        });
        tokio::task::yield_now().await;

        handle.abort();
        let joined = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            joined.is_ok(),
            "aborting + awaiting the SIGHUP-shaped task must return promptly, not hang"
        );
    }
}
