use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orihsus::config::Secret;
use orihsus::hot_reload::{
    ApplyError, HotReloadError, HotReloader, RuntimeConfigSnapshot, StatusSnapshot,
};
use tempfile::TempDir;

const DEBOUNCE: Duration = Duration::from_millis(150);

const MINIMAL: &str = r#"
gateway_token: "gway-secret"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
"#;

fn write_config(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

fn with_keys(keys: &[&str]) -> String {
    let mut out = MINIMAL.replace("keys:\n  - \"key-1\"", "keys:");
    for k in keys {
        out.push_str(&format!("  - \"{k}\"\n"));
    }
    out
}

#[derive(Default)]
struct Recorder {
    applied: Mutex<Vec<Vec<Secret>>>,
    count: AtomicUsize,
    fail: AtomicBool,
}

impl Recorder {
    fn apply(&self, snapshot: &RuntimeConfigSnapshot) -> Result<(), ApplyError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(ApplyError);
        }
        self.applied.lock().unwrap().push(snapshot.keys.clone());
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn last_applied(&self) -> Option<Vec<Secret>> {
        self.applied.lock().unwrap().last().cloned()
    }
}

async fn wait_for(cond: impl Fn() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starts_with_zero_status_and_applies_nothing_at_start() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let s: StatusSnapshot = reloader.status();
    assert_eq!(s.successful_reloads, 0);
    assert_eq!(s.failed_reloads, 0);
    assert!(s.last_error.is_none());
    assert!(s.last_loaded_at.is_none());
    assert!(!s.needs_restart);

    tokio::time::sleep(2 * DEBOUNCE).await;
    assert_eq!(recorder.count(), 0, "startup must not apply anything");

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modify_triggers_reload_and_apply() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let updated = with_keys(&["key-2"]);
    fs::write(&path, &updated).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

    assert!(
        wait_for(|| recorder.count() >= 1, Duration::from_secs(10)).await,
        "a modify of the config must trigger a reload"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-2")]));
    assert!(
        reloader.status().successful_reloads >= 1,
        "successful reload must be recorded"
    );

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_writes_are_debounced_into_one_apply_of_final_version() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    for i in 0..5 {
        let version = with_keys(&[&format!("key-{i}")]);
        fs::write(&path, &version).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        wait_for(|| recorder.count() >= 1, Duration::from_secs(10)).await,
        "the burst must eventually be applied"
    );
    assert_eq!(
        recorder.last_applied(),
        Some(vec![Secret::new("key-4")]),
        "the final stable version must win"
    );
    tokio::time::sleep(2 * DEBOUNCE).await;
    assert_eq!(
        recorder.count(),
        1,
        "the whole burst must collapse into a single apply"
    );

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_to_sibling_files_are_ignored() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let sibling = dir.path().join("other.yaml");
    for i in 0..4 {
        fs::write(&sibling, format!("gateway_token: sibling-{i}\n")).unwrap();
        fs::set_permissions(&sibling, fs::Permissions::from_mode(0o600)).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    tokio::time::sleep(3 * DEBOUNCE).await;
    assert_eq!(
        recorder.count(),
        0,
        "changes to files other than the config must be ignored"
    );
    assert_eq!(reloader.status().successful_reloads, 0);

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_config_keeps_last_known_good_and_recovers() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);
    assert_eq!(reloader.status().successful_reloads, 1);

    fs::write(&path, "gateway_token: [unclosed\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "an invalid write must surface as a failed reload"
    );
    assert_eq!(recorder.count(), 1, "last-known-good must be preserved");
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-a")]));
    assert!(
        reloader.status().last_error.is_some(),
        "the failure must be observable"
    );
    assert_eq!(reloader.status().successful_reloads, 1);

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 2, Duration::from_secs(10)).await,
        "the worker must keep listening after a failed reload"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-b")]));

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn models_change_is_hot_applied_not_refused() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    assert_eq!(
        initial.models,
        vec!["deepseek-chat".to_string()],
        "precondition: the default models list"
    );
    let applied: Arc<Mutex<Option<Vec<String>>>> = Arc::new(Mutex::new(None));
    let applied2 = applied.clone();
    let reloader = HotReloader::start(&path, DEBOUNCE, initial, move |snap| {
        *applied2.lock().unwrap() = Some(snap.models.clone());
        Ok(())
    })
    .unwrap();

    let with_models = format!("{MINIMAL}models:\n  - \"deepseek-reasoner\"\n");
    fs::write(&path, &with_models).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || applied.lock().unwrap().is_some(),
            Duration::from_secs(10)
        )
        .await,
        "a models change must be hot-applied, not refused as needs-restart"
    );
    assert_eq!(
        *applied.lock().unwrap(),
        Some(vec!["deepseek-reasoner".to_string()]),
        "the applied snapshot must carry the new model list"
    );
    assert!(!reloader.status().needs_restart);
    assert!(reloader.status().successful_reloads >= 1);

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_field_typo_keeps_last_known_good_and_recovers() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);

    // A typo'd hardening key must be rejected as an invalid config (deny_unknown_fields),
    // never silently accepted with the default for the intended field.
    let typo = format!("{MINIMAL}server:\n  max_connection: 8\n");
    fs::write(&path, &typo).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "a config with an unknown field must surface as a failed reload"
    );
    assert_eq!(recorder.count(), 1, "last-known-good must be preserved");
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-a")]));
    assert_eq!(reloader.status().successful_reloads, 1);

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 2, Duration::from_secs(10)).await,
        "the worker must keep listening after the rejected typo"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-b")]));

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permission_error_keeps_last_known_good_and_recovers() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "a permission error must be surfaced as a failed reload"
    );
    assert_eq!(recorder.count(), 1, "last-known-good must be preserved");
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-a")]));

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 2, Duration::from_secs(10)).await,
        "after restoring 0600 the reloader must recover"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-b")]));

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_hot_changes_require_restart_and_are_not_applied() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let with_listen = MINIMAL.replace(
        "gateway_token: \"gway-secret\"",
        "listen: \"127.0.0.1:8443\"\ngateway_token: \"gway-secret\"",
    );
    fs::write(&path, &with_listen).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| reloader.status().needs_restart, Duration::from_secs(10)).await,
        "a listen change must flag needs_restart"
    );
    assert_eq!(recorder.count(), 0, "a non-hot change must not be applied");
    assert_eq!(reloader.status().successful_reloads, 0);
    assert_eq!(reloader.status().failed_reloads, 1);
    assert!(reloader.status().last_error.is_some());

    let with_server_change = MINIMAL.replace(
        "gateway_token: \"gway-secret\"",
        "server:\n  max_header_bytes: \"16KiB\"\ngateway_token: \"gway-secret\"",
    );
    fs::write(&path, &with_server_change).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 2,
            Duration::from_secs(10)
        )
        .await,
        "a server change must also be refused"
    );
    assert!(reloader.status().needs_restart);
    assert_eq!(recorder.count(), 0);

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await,
        "a hot-only change must be applied and clear needs_restart"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-b")]));
    assert!(!reloader.status().needs_restart);
    assert_eq!(reloader.status().successful_reloads, 1);

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_dynamic_config_changes_require_restart_and_are_not_applied() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    // limits / rotation / server changes cannot be applied online: they must
    // be refused wholesale (needs_restart), never half-applied.
    let cases: &[(&str, &str)] = &[
        ("limits", "limits:\n  max_concurrency: 300\n"),
        ("rotation", "rotation:\n  backoff_initial: \"9s\"\n"),
        ("server", "server:\n  read_header_timeout: \"3s\"\n"),
        ("server timeout", "server:\n  body_read_timeout: \"40s\"\n"),
        (
            "server upstream header timeout",
            "server:\n  upstream_response_header_timeout: \"40s\"\n",
        ),
        (
            "audit",
            "audit:\n  path: \"/var/log/orihsus/other.jsonl\"\n",
        ),
    ];
    for (name, section) in cases {
        let text = format!("{MINIMAL}\n{section}");
        fs::write(&path, &text).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            wait_for(|| reloader.status().needs_restart, Duration::from_secs(10)).await,
            "{name} change must flag needs_restart"
        );
        assert_eq!(
            recorder.count(),
            0,
            "{name} change must never reach the apply callback"
        );
    }

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_policy_changes_require_restart_and_are_not_applied() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();
    fs::write(
        &path,
        format!("{MINIMAL}\nusage:\n  soft_threshold_percent: 70\n"),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| reloader.status().needs_restart, Duration::from_secs(10)).await);
    assert_eq!(recorder.count(), 0);
    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_failure_keeps_last_known_good_and_is_recorded() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);

    recorder.fail.store(true, Ordering::SeqCst);
    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "an apply failure must be recorded as a failed reload"
    );
    assert_eq!(
        recorder.count(),
        1,
        "the rejected snapshot must not replace last-known-good"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-a")]));
    assert_eq!(reloader.status().successful_reloads, 1);
    assert_eq!(
        reloader.status().last_error.as_deref(),
        Some("config apply failed"),
        "an apply failure must be observable via the static message"
    );

    recorder.fail.store(false, Ordering::SeqCst);
    let v3 = with_keys(&["key-c"]);
    fs::write(&path, &v3).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 2, Duration::from_secs(10)).await,
        "the worker must recover after the target starts accepting again"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-c")]));

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_errors_never_leak_secrets_into_status() {
    let dir = TempDir::new().unwrap();
    let token_secret = "GWAY-TOKEN-SECRET-77";
    let key_secret = "RAW-KEY-SECRET-77";
    let secret_cfg = format!(
        "gateway_token: \"{token_secret}\"\n\
         upstream:\n  base_url: \"https://api.opencode.go\"\n\
         keys:\n  - \"{key_secret}\"\n"
    );
    let path = write_config(dir.path(), "config.yaml", &secret_cfg);
    let initial = orihsus::config::load(&path).unwrap();
    let reloader = HotReloader::start(&path, DEBOUNCE, initial, |_snap| Err(ApplyError)).unwrap();

    fs::write(&path, &secret_cfg).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "the apply failure must be observed"
    );

    let status = reloader.status();
    assert_eq!(
        status.last_error.as_deref(),
        Some("config apply failed"),
        "apply failures surface one static, non-leaking message"
    );
    assert!(
        !format!("{status:?}").contains(token_secret)
            && !format!("{status:?}").contains(key_secret),
        "status Debug leaked secrets: {status:?}"
    );
    let last_error = status.last_error.clone().unwrap_or_default();
    assert!(
        !last_error.contains(token_secret) && !last_error.contains(key_secret),
        "last_error leaked secrets: {last_error}"
    );
    assert!(
        !format!("{reloader:?}").contains(token_secret)
            && !format!("{reloader:?}").contains(key_secret),
        "reloader Debug leaked secrets"
    );

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_stops_the_watcher_and_worker() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);

    drop(reloader);

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    tokio::time::sleep(3 * DEBOUNCE).await;
    assert_eq!(
        recorder.count(),
        1,
        "after drop no further reloads may be triggered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_then_recreate_recovers() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let v1 = with_keys(&["key-a"]);
    fs::write(&path, &v1).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() == 1, Duration::from_secs(10)).await);

    fs::remove_file(&path).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "removing the config must surface a failed reload"
    );

    let v2 = with_keys(&["key-b"]);
    fs::write(&path, &v2).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(|| recorder.count() == 2, Duration::from_secs(10)).await,
        "recreating the config must restore reloads"
    );
    assert_eq!(recorder.last_applied(), Some(vec![Secret::new("key-b")]));

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_and_snapshots_never_leak_secrets() {
    let dir = TempDir::new().unwrap();
    let secret = "SUPER-SECRET-KEY-42";
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();

    let malformed = format!("gateway_token: \"{secret}\"\nupstream: [unclosed\n");
    fs::write(&path, &malformed).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "the malformed write must be observed"
    );

    let status = reloader.status();
    let status_debug = format!("{status:?}");
    assert!(
        !status_debug.contains(secret),
        "status debug leaked secret: {status_debug}"
    );
    assert!(
        !status
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains(secret),
        "last_error leaked secret: {:?}",
        status.last_error
    );
    assert!(
        !format!("{reloader:?}").contains(secret),
        "reloader debug leaked secret"
    );

    let v = with_keys(&["key-secret-77"]);
    fs::write(&path, &v).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(wait_for(|| recorder.count() >= 1, Duration::from_secs(10)).await);

    let cfg = orihsus::config::load(&path).unwrap();
    let snapshot = RuntimeConfigSnapshot::from_config(&cfg);
    assert_eq!(
        snapshot.rotation.backoff_initial, cfg.rotation.backoff_initial,
        "the hot snapshot must carry the current Rotation type"
    );
    let snap_debug = format!("{snapshot:?}");
    assert!(
        !snap_debug.contains("key-secret-77"),
        "snapshot debug leaked key: {snap_debug}"
    );
    assert!(
        !snap_debug.contains("gway-secret"),
        "snapshot debug leaked token: {snap_debug}"
    );

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_fails_when_the_parent_directory_does_not_exist() {
    let dir = TempDir::new().unwrap();
    let valid = write_config(dir.path(), "ok.yaml", MINIMAL);
    let initial = orihsus::config::load(&valid).unwrap();

    let missing = dir.path().join("no-such-dir").join("config.yaml");
    let err = HotReloader::start(&missing, DEBOUNCE, initial, |_| Ok(())).unwrap_err();
    assert!(
        matches!(err, HotReloadError::Watch { .. }),
        "a missing parent directory must yield Watch, got: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_trigger_forces_a_reload_without_touching_the_file() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();
    let trigger = reloader.reload_trigger();

    // No file change: only the explicit trigger must cause a reload.
    assert_eq!(recorder.count(), 0);
    trigger.fire();
    assert!(
        wait_for(|| recorder.count() >= 1, Duration::from_secs(10)).await,
        "an explicit reload trigger must re-read and apply the config"
    );

    // Firing again (e.g. a second SIGHUP) triggers another reload.
    trigger.fire();
    assert!(
        wait_for(|| recorder.count() >= 2, Duration::from_secs(10)).await,
        "the trigger must be repeatable"
    );

    reloader.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn triggered_reload_failure_is_recorded_as_desensitized_status() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);
    let initial = orihsus::config::load(&path).unwrap();
    let recorder = Arc::new(Recorder::default());
    let rec = recorder.clone();
    let reloader =
        HotReloader::start(&path, DEBOUNCE, initial, move |snap| rec.apply(snap)).unwrap();
    let trigger = reloader.reload_trigger();

    // A trigger-driven reload whose apply fails: the worker's failure branch
    // (which also emits its desensitized stderr summary) must record the failed
    // status, never count the rejected snapshot as applied.
    recorder.fail.store(true, Ordering::SeqCst);
    trigger.fire();
    assert!(
        wait_for(
            || reloader.status().failed_reloads >= 1,
            Duration::from_secs(10)
        )
        .await,
        "a triggered apply failure must be recorded as a failed reload"
    );
    assert_eq!(
        reloader.status().last_error.as_deref(),
        Some("config apply failed"),
        "the failure surfaces one static, non-leaking message"
    );
    assert_eq!(recorder.count(), 0, "a failed apply must not be applied");
    assert!(
        !format!("{:?}", reloader.status()).contains("gway-secret"),
        "status debug must stay desensitized"
    );

    reloader.shutdown();
}
