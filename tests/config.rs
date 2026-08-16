use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orihsus::config::Secret;
use tempfile::TempDir;

fn write_config(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

const MINIMAL: &str = r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
"#;

#[test]
fn minimal_valid_config_yields_defaults() {
    let dir = TempDir::new().unwrap();
    let path = write_config(dir.path(), "config.yaml", MINIMAL);

    let cfg = orihsus::config::load(&path).unwrap();

    assert_eq!(cfg.gateway_token.as_str(), "gway-secret");
    assert_eq!(cfg.keys, vec![Secret::new("key-1")]);
    assert_eq!(cfg.listen.ip().to_string(), "127.0.0.1");
    assert_eq!(cfg.listen.port(), 8080);
    assert_eq!(cfg.limits.max_concurrency, 200);
    assert_eq!(cfg.limits.max_queue, 500);
    assert_eq!(cfg.limits.queue_wait_timeout, Duration::from_secs(30));
    assert_eq!(cfg.limits.max_body_bytes, 10 * 1024 * 1024);
    assert_eq!(
        cfg.limits.max_inflight_body_bytes,
        256 * 1024 * 1024,
        "default inflight body budget is 256MiB, not 200×10MiB"
    );

    assert_eq!(
        cfg.key_failure_handling.backoff_initial,
        Duration::from_secs(5)
    );
    assert_eq!(
        cfg.key_failure_handling.backoff_max,
        Duration::from_secs(60)
    );
    assert_eq!(cfg.key_failure_handling.breaker_threshold, 5);
    assert_eq!(
        cfg.key_failure_handling.breaker_cooldown,
        Duration::from_secs(60)
    );
    assert_eq!(cfg.key_failure_handling.max_attempts, 2);

    assert_eq!(cfg.usage.soft_threshold_percent, 80.0);
    assert_eq!(cfg.usage.poll_interval, Duration::from_secs(5 * 60));
    assert_eq!(
        cfg.usage_history_dir,
        PathBuf::from("/var/log/orihsus/usage")
    );

    assert_eq!(
        cfg.audit.path,
        PathBuf::from("/var/log/orihsus/audit.jsonl")
    );
    assert_eq!(cfg.audit.queue_capacity, 4096);
    assert_eq!(cfg.server.read_header_timeout, Duration::from_secs(5));
    assert_eq!(cfg.server.max_header_bytes, 32 * 1024);
    assert_eq!(cfg.server.body_read_timeout, Duration::from_secs(30));
    assert_eq!(
        cfg.server.upstream_response_header_timeout,
        Duration::from_secs(60)
    );
    assert_eq!(cfg.server.first_event_timeout, Duration::from_secs(60));
    assert_eq!(cfg.server.inter_event_timeout, Duration::from_secs(90));
    assert_eq!(
        cfg.server.upstream_error_body_timeout,
        Duration::from_secs(5)
    );
    assert_eq!(
        cfg.server.response_write_timeout,
        Duration::from_secs(30),
        "default per-chunk response write timeout"
    );
    assert_eq!(cfg.server.max_connections, 1024);
}

#[test]
fn non_loopback_listener_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "public-listener.yaml",
        &format!("listen:\n  host: \"0.0.0.0\"\n  port: 8080\n{MINIMAL}"),
    );
    let err = orihsus::config::load(&path).unwrap_err();
    let rendered = format!("{err}");
    assert!(rendered.contains("loopback"), "got: {rendered}");
    assert!(rendered.contains("nginx"), "got: {rendered}");
}

#[test]
fn obsolete_tls_section_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "obsolete-tls.yaml",
        &format!("tls:\n  cert_path: /tmp/cert.pem\n  key_path: /tmp/key.pem\n{MINIMAL}"),
    );
    let err = orihsus::config::load(&path).unwrap_err();
    assert!(format!("{err}").contains("invalid YAML"), "got: {err}");
}

#[test]
fn composite_string_schema_is_rejected() {
    let dir = TempDir::new().unwrap();
    for (name, prefix) in [
        ("listen.yaml", "listen: \"127.0.0.1:8080\"\n"),
        ("duration.yaml", "usage:\n  poll_interval_seconds: \"5m\"\n"),
        ("bytes.yaml", "limits:\n  max_body_bytes: \"10MiB\"\n"),
    ] {
        let path = write_config(dir.path(), name, &format!("{prefix}{MINIMAL}"));
        let err = orihsus::config::load(path).unwrap_err();
        assert!(format!("{err}").contains("invalid YAML"), "{name}: {err}");
    }
}

#[test]
fn explicit_usage_values_are_parsed() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "usage.yaml",
        &MINIMAL.replace(
            "keys:\n  - \"key-1\"",
            "keys:\n  - \"key-1\"\nusage_history_dir: /srv/orihsus/usage-history\nusage:\n  soft_threshold_percent: 73.5\n  poll_interval_seconds: 45",
        ),
    );

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.usage.soft_threshold_percent, 73.5);
    assert_eq!(cfg.usage.poll_interval, Duration::from_secs(45));
    assert_eq!(
        cfg.usage_history_dir,
        PathBuf::from("/srv/orihsus/usage-history")
    );
}

#[test]
fn empty_usage_history_directory_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "usage-history.yaml",
        &MINIMAL.replace(
            "keys:\n  - \"key-1\"",
            "keys:\n  - \"key-1\"\nusage_history_dir: \"\"",
        ),
    );
    let error = orihsus::config::load(path).unwrap_err();
    assert!(format!("{error}").contains("usage_history_dir"));
}

#[test]
fn invalid_usage_thresholds_are_rejected_without_leaking_secrets() {
    let dir = TempDir::new().unwrap();
    for (name, value) in [("nan", ".nan"), ("zero", "0"), ("over", "100.1")] {
        let secret = format!("key-{name}-must-not-leak");
        let yaml = MINIMAL.replace(
            "keys:\n  - \"key-1\"",
            &format!("keys:\n  - \"{secret}\"\nusage:\n  soft_threshold_percent: {value}"),
        );
        let path = write_config(dir.path(), &format!("{name}.yaml"), &yaml);
        let err = orihsus::config::load(path).unwrap_err();
        let rendered = format!("{err:?} {err}");
        assert!(rendered.contains("soft_threshold_percent"), "{rendered}");
        assert!(!rendered.contains(&secret), "{rendered}");
    }
}

#[test]
fn usage_poll_interval_shorter_than_thirty_seconds_is_rejected() {
    let dir = TempDir::new().unwrap();
    for (name, value) in [("zero", 0), ("short", 29)] {
        let path = write_config(
            dir.path(),
            &format!("interval-{name}.yaml"),
            &MINIMAL.replace(
                "keys:\n  - \"key-1\"",
                &format!("keys:\n  - \"key-1\"\nusage:\n  poll_interval_seconds: {value}"),
            ),
        );
        let err = orihsus::config::load(path).unwrap_err();
        assert!(format!("{err}").contains("poll_interval_seconds"), "{err}");
    }
}

#[test]
fn custom_audit_and_server_values_are_parsed() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
audit:
  path: "/srv/orihsus/audit.log"
  queue_capacity: 128
server:
  read_header_timeout_seconds: 2
  max_header_bytes: 8192
  body_read_timeout_seconds: 20
  upstream_response_header_timeout_seconds: 45
  first_event_timeout_seconds: 70
  inter_event_timeout_seconds: 110
  model_event_timeouts:
    deepseek-reasoner:
      first_event_timeout_seconds: 140
    deepseek-chat:
      inter_event_timeout_seconds: 220
  upstream_error_body_timeout_seconds: 7
  response_write_timeout_seconds: 45
  max_connections: 2048
"#,
    );

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.audit.path, PathBuf::from("/srv/orihsus/audit.log"));
    assert_eq!(cfg.audit.queue_capacity, 128);
    assert_eq!(cfg.server.read_header_timeout, Duration::from_secs(2));
    assert_eq!(cfg.server.max_header_bytes, 8 * 1024);
    assert_eq!(cfg.server.body_read_timeout, Duration::from_secs(20));
    assert_eq!(
        cfg.server.upstream_response_header_timeout,
        Duration::from_secs(45)
    );
    assert_eq!(cfg.server.first_event_timeout, Duration::from_secs(70));
    assert_eq!(cfg.server.inter_event_timeout, Duration::from_secs(110));
    assert_eq!(
        cfg.server
            .model_event_timeouts
            .get("deepseek-reasoner")
            .unwrap()
            .first_event_timeout,
        Duration::from_secs(140)
    );
    assert_eq!(
        cfg.server
            .model_event_timeouts
            .get("deepseek-reasoner")
            .unwrap()
            .inter_event_timeout,
        Duration::from_secs(110),
        "missing model field inherits the global default"
    );
    assert_eq!(
        cfg.server
            .model_event_timeouts
            .get("deepseek-chat")
            .unwrap()
            .first_event_timeout,
        Duration::from_secs(70),
        "missing model field inherits the global default"
    );
    assert_eq!(
        cfg.server
            .model_event_timeouts
            .get("deepseek-chat")
            .unwrap()
            .inter_event_timeout,
        Duration::from_secs(220)
    );
    assert_eq!(
        cfg.server.upstream_error_body_timeout,
        Duration::from_secs(7)
    );
    assert_eq!(cfg.server.response_write_timeout, Duration::from_secs(45));
    assert_eq!(cfg.server.max_connections, 2048);
}

#[test]
fn invalid_audit_and_server_values_are_rejected() {
    let dir = TempDir::new().unwrap();
    let base = |extra: &str| {
        format!(
            r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
{extra}
"#
        )
    };

    let zero_capacity = write_config(
        dir.path(),
        "cap.yaml",
        &base("audit:\n  path: \"/x/a.log\"\n  queue_capacity: 0\n"),
    );
    let err = orihsus::config::load(&zero_capacity).unwrap_err();
    assert!(format!("{err}").contains("queue_capacity"), "got: {err}");

    let zero_timeout = write_config(
        dir.path(),
        "timeout.yaml",
        &base("server:\n  read_header_timeout_seconds: 0\n"),
    );
    let err = orihsus::config::load(&zero_timeout).unwrap_err();
    assert!(
        format!("{err}").contains("read_header_timeout_seconds"),
        "got: {err}"
    );

    let zero_headers = write_config(
        dir.path(),
        "headers.yaml",
        &base("server:\n  max_header_bytes: 0\n"),
    );
    let err = orihsus::config::load(&zero_headers).unwrap_err();
    assert!(format!("{err}").contains("max_header_bytes"), "got: {err}");

    let zero_body_read = write_config(
        dir.path(),
        "body_read.yaml",
        &base("server:\n  body_read_timeout_seconds: 0\n"),
    );
    let err = orihsus::config::load(&zero_body_read).unwrap_err();
    assert!(
        format!("{err}").contains("body_read_timeout_seconds"),
        "got: {err}"
    );

    let zero_upstream_header = write_config(
        dir.path(),
        "upstream_header.yaml",
        &base("server:\n  upstream_response_header_timeout_seconds: 0\n"),
    );
    let err = orihsus::config::load(&zero_upstream_header).unwrap_err();
    assert!(
        format!("{err}").contains("upstream_response_header_timeout_seconds"),
        "got: {err}"
    );

    for (name, field) in [
        ("first_event", "first_event_timeout_seconds"),
        ("inter_event", "inter_event_timeout_seconds"),
    ] {
        let path = write_config(
            dir.path(),
            &format!("{name}.yaml"),
            &base(&format!(
                "server:\n  model_event_timeouts:\n    deepseek-chat:\n      {field}: 0\n"
            )),
        );
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(
            format!("{err}").contains("model_event_timeouts"),
            "got: {err}"
        );
    }

    let zero_error_body = write_config(
        dir.path(),
        "error_body.yaml",
        &base("server:\n  upstream_error_body_timeout_seconds: 0\n"),
    );
    let err = orihsus::config::load(&zero_error_body).unwrap_err();
    assert!(
        format!("{err}").contains("upstream_error_body_timeout_seconds"),
        "got: {err}"
    );

    let zero_response_write = write_config(
        dir.path(),
        "response_write.yaml",
        &base("server:\n  response_write_timeout_seconds: 0\n"),
    );
    let err = orihsus::config::load(&zero_response_write).unwrap_err();
    assert!(
        format!("{err}").contains("response_write_timeout_seconds"),
        "got: {err}"
    );

    let zero_connections = write_config(
        dir.path(),
        "connections_zero.yaml",
        &base("server:\n  max_connections: 0\n"),
    );
    let err = orihsus::config::load(&zero_connections).unwrap_err();
    assert!(format!("{err}").contains("max_connections"), "got: {err}");

    let huge_connections = write_config(
        dir.path(),
        "connections_huge.yaml",
        &base("server:\n  max_connections: 70000\n"),
    );
    let err = orihsus::config::load(&huge_connections).unwrap_err();
    assert!(format!("{err}").contains("max_connections"), "got: {err}");
}

#[test]
fn missing_gateway_token_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
keys:
  - "key-1"
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();

    assert!(format!("{err}").contains("gateway token"), "got: {err}");
}

#[test]
fn configurable_upstream_url_is_rejected() {
    let dir = TempDir::new().unwrap();
    for (name, candidate) in [
        ("metadata", "http://169.254.169.254/latest/meta-data"),
        ("foreign-host", "https://evil.example/zen/go"),
        ("query", "https://opencode.ai/zen/go?target=evil"),
        ("fragment", "https://opencode.ai/zen/go#override"),
        ("custom-path", "https://opencode.ai/not/allowed"),
        ("official", "https://opencode.ai/zen/go"),
    ] {
        let path = write_config(
            dir.path(),
            &format!("{name}.yaml"),
            &format!(
                r#"
gateway_token: "gway-secret"
upstream:
  base_url: "{candidate}"
keys:
  - "key-1"
"#
            ),
        );
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(
            format!("{err}").contains("invalid YAML"),
            "{name}: got {err}"
        );
    }
}

#[test]
fn empty_or_duplicate_keys_are_rejected() {
    let dir = TempDir::new().unwrap();
    let base = |keys: &str| {
        format!(
            r#"
gateway_token: "gway-secret"
keys:
{keys}
"#
        )
    };

    let no_keys = write_config(dir.path(), "no-keys.yaml", &base(""));
    let err = orihsus::config::load(&no_keys).unwrap_err();
    assert!(format!("{err}").contains("key"), "got: {err}");

    let empty_key = write_config(
        dir.path(),
        "empty-key.yaml",
        &base(
            r#"  - ""
"#,
        ),
    );
    let err = orihsus::config::load(&empty_key).unwrap_err();
    assert!(format!("{err}").contains("non-empty"), "got: {err}");

    let duplicate = write_config(
        dir.path(),
        "duplicate.yaml",
        &base(
            r#"  - "key-1"
  - "key-1"
"#,
        ),
    );
    let err = orihsus::config::load(&duplicate).unwrap_err();
    assert!(format!("{err}").contains("duplicate"), "got: {err}");
}

#[test]
// Superseded 2026-08-13: the soft-threshold/quota strategy was removed
// entirely (see WORKLOG migration section). A leftover field must FAIL to load
// with a static, value-free hint rather than being silently ignored.
fn deprecated_soft_threshold_is_rejected_with_a_static_message() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
  soft_threshold: 0.8
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("soft_threshold"),
        "the error must name the removed field: {msg}"
    );
    assert!(
        msg.contains("remove") || msg.contains("deleted") || msg.contains("no longer supported"),
        "the error must say the field was removed: {msg}"
    );
    assert!(
        !msg.contains("0.8"),
        "the error must not echo the value: {msg}"
    );
}

#[test]
// The value is never echoed, so even a secret-shaped value cannot leak.
fn deprecated_soft_threshold_error_never_leaks_value_or_secrets() {
    let dir = TempDir::new().unwrap();
    let secret = "sk-super-secret-soft-value";
    let path = write_config(
        dir.path(),
        "config.yaml",
        &format!(
            r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
  soft_threshold: "{secret}"
"#
        ),
    );

    let err = orihsus::config::load(&path).unwrap_err();
    assert!(
        !format!("{err}").contains(secret),
        "Display leaked the soft_threshold value: {err}"
    );
    assert!(
        !format!("{err:?}").contains(secret),
        "Debug leaked the soft_threshold value: {err:?}"
    );
}

#[test]
fn zero_or_out_of_range_limit_values_are_rejected() {
    let dir = TempDir::new().unwrap();
    let with_limits = |name: &str, limits: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
limits:
{limits}
"#
            ),
        )
    };

    let cases: &[(&str, &str, &str)] = &[
        (
            "max_concurrency_zero.yaml",
            "  max_concurrency: 0\n",
            "max_concurrency",
        ),
        (
            "queue_timeout_zero.yaml",
            "  queue_wait_timeout_seconds: 0\n",
            "queue_wait_timeout_seconds",
        ),
        (
            "body_bytes_zero.yaml",
            "  max_body_bytes: 0\n",
            "max_body_bytes",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = with_limits(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains(field), "{field}: got: {err}");
    }
}

#[test]
fn max_queue_zero_is_allowed() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
limits:
  max_queue: 0
"#,
    );

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(
        cfg.limits.max_queue, 0,
        "max_queue=0 means no queueing, must be accepted"
    );
}

#[test]
fn max_concurrency_and_max_queue_above_semaphore_max_permits_are_rejected() {
    let dir = TempDir::new().unwrap();
    let with_limits = |name: &str, limits: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
limits:
{limits}
"#
            ),
        )
    };

    // AdmissionQueue builds a tokio Semaphore from each value; a value above
    // Semaphore::MAX_PERMITS would panic in the semaphore constructor (and
    // release builds panic="abort"), so the config layer must reject it.
    let too_many = tokio::sync::Semaphore::MAX_PERMITS + 1;
    let cases: &[(&str, &str, &str)] = &[
        (
            "concurrency_above_max_permits.yaml",
            &format!("  max_concurrency: {too_many}\n"),
            "max_concurrency",
        ),
        (
            "queue_above_max_permits.yaml",
            &format!("  max_queue: {too_many}\n"),
            "max_queue",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = with_limits(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains(field), "{field}: got: {err}");
    }
}

#[test]
fn breaker_threshold_above_u32_max_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
  breaker_threshold: 4294967296
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();
    assert!(format!("{err}").contains("breaker_threshold"), "got: {err}");
}

#[test]
fn max_header_bytes_above_u32_max_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
server:
  max_header_bytes: 4294967296
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();
    assert!(format!("{err}").contains("max_header_bytes"), "got: {err}");
}

#[test]
fn custom_max_inflight_body_bytes_is_parsed() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
limits:
  max_body_bytes: 20971520
  max_inflight_body_bytes: 67108864
"#,
    );

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.limits.max_body_bytes, 20 * 1024 * 1024);
    assert_eq!(cfg.limits.max_inflight_body_bytes, 64 * 1024 * 1024);
}

#[test]
fn invalid_max_inflight_body_bytes_is_rejected() {
    let dir = TempDir::new().unwrap();
    let with_limits = |name: &str, limits: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
limits:
{limits}
"#
            ),
        )
    };

    let cases: &[(&str, &str, &str)] = &[
        (
            "inflight_zero.yaml",
            "  max_inflight_body_bytes: 0\n",
            "max_inflight_body_bytes",
        ),
        (
            "inflight_lt_body.yaml",
            "  max_body_bytes: 10485760\n  max_inflight_body_bytes: 1048576\n",
            "max_inflight_body_bytes",
        ),
        (
            "inflight_exceeds_u32.yaml",
            "  max_inflight_body_bytes: 5368709120\n",
            "max_inflight_body_bytes",
        ),
        (
            "body_exceeds_u32.yaml",
            "  max_body_bytes: 5368709120\n",
            "max_body_bytes",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = with_limits(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains(field), "{field}: got: {err}");
    }
}

#[test]
fn key_failure_handling_bounds_are_enforced() {
    let dir = TempDir::new().unwrap();
    let with_key_failure_handling = |name: &str, key_failure_handling: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
{key_failure_handling}
"#
            ),
        )
    };

    let cases: &[(&str, &str, &str)] = &[
        (
            "backoff_initial_zero.yaml",
            "  backoff_initial_seconds: 0\n",
            "backoff_initial_seconds",
        ),
        (
            "backoff_max_zero.yaml",
            "  backoff_max_seconds: 0\n",
            "backoff_max_seconds",
        ),
        (
            "backoff_max_lt_initial.yaml",
            "  backoff_initial_seconds: 60\n  backoff_max_seconds: 5\n",
            "backoff_max_seconds",
        ),
        (
            "breaker_threshold_zero.yaml",
            "  breaker_threshold: 0\n",
            "breaker_threshold",
        ),
        (
            "breaker_cooldown_zero.yaml",
            "  breaker_cooldown_seconds: 0\n",
            "breaker_cooldown_seconds",
        ),
        (
            "backoff_max_extreme.yaml",
            "  backoff_max_seconds: 18446744073709551615\n",
            "backoff_max_seconds",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = with_key_failure_handling(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains(field), "{field}: got: {err}");
    }
}

#[test]
fn backoff_max_is_capped_at_the_ops_cooldown_ceiling() {
    let dir = TempDir::new().unwrap();
    let with_backoff = |name: &str, max: u64| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
  backoff_max_seconds: {max}
"#
            ),
        )
    };

    // A huge numeric duration would overflow the jitter addition
    // (process abort under panic="abort"); the config layer must reject it as
    // invalid instead of accepting it.
    let extreme = with_backoff("extreme.yaml", 18446744073709551615);
    let err = orihsus::config::load(&extreme).unwrap_err();
    assert!(
        format!("{err}").contains("backoff_max_seconds"),
        "an extreme backoff_max_seconds must be rejected: {err}"
    );

    // The ops ceiling (MAX_COOLDOWN = 90 days) is the sane upper bound: at the
    // ceiling the config still loads, just past it the load fails.
    let at_ceiling = with_backoff("ceiling.yaml", 90 * 24 * 60 * 60);
    assert!(
        orihsus::config::load(&at_ceiling).is_ok(),
        "backoff_max_seconds at the 90d ops ceiling must be accepted"
    );
    let past_ceiling = with_backoff("past.yaml", 91 * 24 * 60 * 60);
    let err = orihsus::config::load(&past_ceiling).unwrap_err();
    assert!(
        format!("{err}").contains("backoff_max_seconds"),
        "backoff_max_seconds past the 90d ops ceiling must be rejected: {err}"
    );
}

#[test]
fn max_attempts_must_be_within_one_and_two() {
    let dir = TempDir::new().unwrap();
    let with_attempts = |name: &str, value: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
key_failure_handling:
  max_attempts: {value}
"#
            ),
        )
    };

    for (name, value) in [("zero.yaml", "0"), ("three.yaml", "3")] {
        let path = with_attempts(name, value);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(
            format!("{err}").contains("max_attempts"),
            "value {value}: got: {err}"
        );
    }
}

#[test]
fn unknown_top_level_fields_are_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "unknown-top.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
max_concurrency: 8
"#,
    );
    let err = orihsus::config::load(&path).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("invalid YAML"),
        "an unknown top-level key must be rejected as a parse error (line/column only): {msg}"
    );
}

#[test]
fn unknown_nested_fields_are_rejected() {
    let dir = TempDir::new().unwrap();
    let base = |name: &str, extra: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
{extra}
"#
            ),
        )
    };

    // A typo'd hardening/capacity key must fail to load instead of silently
    // selecting the default for the intended field.
    let cases: &[(&str, &str, &str)] = &[
        (
            "limits-inflight-typo.yaml",
            "limits:\n  max_inflight_body_byte: \"16MiB\"\n",
            "max_inflight_body_bytes",
        ),
        (
            "server-connections-typo.yaml",
            "server:\n  max_connection: 8\n",
            "max_connections",
        ),
        (
            "server-header-typo.yaml",
            "server:\n  max_header_byte: \"8KiB\"\n",
            "max_header_bytes",
        ),
        (
            "key-failure-handling-typo.yaml",
            "key_failure_handling:\n  backoff_maximum: \"60s\"\n",
            "backoff_max_seconds",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = base(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(
            format!("{err}").contains("invalid YAML"),
            "{name}: an unknown nested field ({field:?}) must be rejected as a parse error: {err}"
        );
    }
}

#[test]
fn models_default_and_custom_values() {
    let dir = TempDir::new().unwrap();
    let minimal = write_config(dir.path(), "minimal.yaml", MINIMAL);
    let cfg = orihsus::config::load(&minimal).unwrap();
    assert_eq!(
        cfg.models,
        vec!["deepseek-chat".to_string()],
        "the last-resort list remains available until the startup sync succeeds"
    );
    assert!(
        cfg.model_sync.enabled,
        "absent models must enable upstream synchronization"
    );
    assert_eq!(cfg.model_sync.interval, Duration::from_secs(60 * 60));

    let custom = write_config(
        dir.path(),
        "custom.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
models:
  - "deepseek-chat"
  - "deepseek-reasoner"
"#,
    );
    let cfg = orihsus::config::load(&custom).unwrap();
    assert_eq!(
        cfg.models,
        vec!["deepseek-chat".to_string(), "deepseek-reasoner".to_string()]
    );
    assert!(
        !cfg.model_sync.enabled,
        "an explicit list is a manual override"
    );
}

#[test]
fn model_sync_can_be_disabled_or_given_a_custom_interval() {
    let dir = TempDir::new().unwrap();
    let disabled = write_config(
        dir.path(),
        "disabled-model-sync.yaml",
        "gateway_token: gway-secret\nkeys:\n  - key-1\nmodel_sync:\n  enabled: false\n",
    );
    let cfg = orihsus::config::load(&disabled).unwrap();
    assert!(!cfg.model_sync.enabled);

    let custom = write_config(
        dir.path(),
        "custom-model-sync.yaml",
        "gateway_token: gway-secret\nkeys:\n  - key-1\nmodel_sync:\n  interval_seconds: 7200\n",
    );
    let cfg = orihsus::config::load(&custom).unwrap();
    assert!(cfg.model_sync.enabled);
    assert_eq!(cfg.model_sync.interval, Duration::from_secs(7200));
}

#[test]
fn model_sync_interval_shorter_than_thirty_seconds_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "short-model-sync.yaml",
        "gateway_token: gway-secret\nkeys:\n  - key-1\nmodel_sync:\n  interval_seconds: 29\n",
    );
    let err = orihsus::config::load(&path).unwrap_err();
    assert!(
        format!("{err}").contains("model_sync.interval_seconds"),
        "{err}"
    );
}

#[test]
fn empty_blank_or_duplicate_models_are_rejected() {
    let dir = TempDir::new().unwrap();
    let with_models = |name: &str, models: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
models:
{models}
"#
            ),
        )
    };

    let cases: &[(&str, &str, &str)] = &[
        ("empty.yaml", "  []\n", "models"),
        ("blank.yaml", "  - \"\"\n", "models"),
        ("whitespace.yaml", "  - \"   \"\n", "models"),
        (
            "duplicate.yaml",
            "  - \"deepseek-chat\"\n  - \"deepseek-chat\"\n",
            "models",
        ),
    ];
    for (name, models, field) in cases {
        let path = with_models(name, models);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(
            format!("{err}").contains(field),
            "{name}: {field} must be rejected: {err}"
        );
    }
}

#[test]
fn configured_model_names_are_bounded_to_256_bytes() {
    let dir = TempDir::new().unwrap();
    let oversized = "m".repeat(257);
    let path = write_config(
        dir.path(),
        "oversized-model.yaml",
        &format!("gateway_token: gway-secret\nkeys:\n  - key-1\nmodels:\n  - {oversized}\n"),
    );
    let err = orihsus::config::load(&path).unwrap_err();
    assert!(format!("{err}").contains("256 bytes"), "got: {err}");
}

#[test]
fn config_file_must_have_0600_permissions() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
keys:
  - "key-1"
"#,
    );

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let err = orihsus::config::load(&path).unwrap_err();
    assert!(format!("{err}").contains("0600"), "got: {err}");

    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(orihsus::config::load(&path).is_ok());
}

#[cfg(unix)]
#[test]
fn config_loader_rejects_symlinks_even_when_target_is_mode_0600() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let target = write_config(dir.path(), "real.yaml", MINIMAL);
    let link = dir.path().join("config.yaml");
    symlink(&target, &link).unwrap();

    let err = orihsus::config::load(&link).unwrap_err();
    assert!(
        format!("{err}").contains("cannot read config"),
        "symlink must be rejected at open: {err}"
    );
}

#[test]
fn errors_and_debug_never_leak_secrets() {
    let dir = TempDir::new().unwrap();
    let secret_token = "TOKEN-SECRET-12345";
    let secret_key = "KEY-SECRET-12345";

    let duplicate = write_config(
        dir.path(),
        "duplicate.yaml",
        &format!(
            r#"
gateway_token: "{secret_token}"
keys:
  - "{secret_key}"
  - "{secret_key}"
"#
        ),
    );
    let err = orihsus::config::load(&duplicate).unwrap_err();
    assert!(
        !format!("{err}").contains(secret_token),
        "validation leaked token: {err}"
    );
    assert!(
        !format!("{err}").contains(secret_key),
        "validation leaked key: {err}"
    );
    assert!(
        !format!("{err:?}").contains(secret_token),
        "debug validation leaked token: {err:?}"
    );
    assert!(
        !format!("{err:?}").contains(secret_key),
        "debug validation leaked key: {err:?}"
    );

    let malformed = write_config(
        dir.path(),
        "malformed.yaml",
        &format!(
            r#"
gateway_token: "{secret_token}"
upstream: [unclosed
key_failure_handling:
  base_url: "https://api.opencode.go"
keys:
  - "{secret_key}"
"#
        ),
    );
    let err = orihsus::config::load(&malformed).unwrap_err();
    assert!(
        !format!("{err}").contains(secret_token),
        "parse leaked token: {err}"
    );
    assert!(
        !format!("{err}").contains(secret_key),
        "parse leaked key: {err}"
    );

    let ok = write_config(
        dir.path(),
        "ok.yaml",
        &format!(
            r#"
gateway_token: "{secret_token}"
keys:
  - "{secret_key}"
"#
        ),
    );
    let cfg = orihsus::config::load(&ok).unwrap();
    assert!(
        !format!("{cfg:?}").contains(secret_token),
        "config debug leaked token: {cfg:?}"
    );
    assert!(
        !format!("{cfg:?}").contains(secret_key),
        "config debug leaked key: {cfg:?}"
    );
}
