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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
    assert_eq!(cfg.listen.ip().to_string(), "0.0.0.0");
    assert_eq!(cfg.tls.cert, PathBuf::from("/etc/orihsus/cert.pem"));
    assert_eq!(cfg.tls.key, PathBuf::from("/etc/orihsus/key.pem"));
    assert_eq!(cfg.upstream.base_url.as_str(), "https://api.opencode.go/");

    assert_eq!(cfg.limits.max_concurrency, 200);
    assert_eq!(cfg.limits.max_queue, 500);
    assert_eq!(cfg.limits.queue_wait_timeout, Duration::from_secs(30));
    assert_eq!(cfg.limits.max_body_bytes, 10 * 1024 * 1024);
    assert_eq!(
        cfg.limits.max_inflight_body_bytes,
        256 * 1024 * 1024,
        "default inflight body budget is 256MiB, not 200×10MiB"
    );

    assert_eq!(cfg.rotation.backoff_initial, Duration::from_secs(5));
    assert_eq!(cfg.rotation.backoff_max, Duration::from_secs(60));
    assert_eq!(cfg.rotation.breaker_threshold, 5);
    assert_eq!(cfg.rotation.breaker_cooldown, Duration::from_secs(60));
    assert_eq!(cfg.rotation.max_attempts, 2);

    assert_eq!(cfg.usage.soft_threshold_percent, 80.0);
    assert_eq!(cfg.usage.poll_interval, Duration::from_secs(5 * 60));

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
fn explicit_usage_values_are_parsed() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "usage.yaml",
        &MINIMAL.replace(
            "keys:\n  - \"key-1\"",
            "keys:\n  - \"key-1\"\nusage:\n  soft_threshold_percent: 73.5\n  poll_interval: \"45s\"",
        ),
    );

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.usage.soft_threshold_percent, 73.5);
    assert_eq!(cfg.usage.poll_interval, Duration::from_secs(45));
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
    for (name, value) in [("zero", "0s"), ("short", "29s")] {
        let path = write_config(
            dir.path(),
            &format!("interval-{name}.yaml"),
            &MINIMAL.replace(
                "keys:\n  - \"key-1\"",
                &format!("keys:\n  - \"key-1\"\nusage:\n  poll_interval: \"{value}\""),
            ),
        );
        let err = orihsus::config::load(path).unwrap_err();
        assert!(format!("{err}").contains("poll_interval"), "{err}");
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
audit:
  path: "/srv/orihsus/audit.log"
  queue_capacity: 128
server:
  read_header_timeout: "2s"
  max_header_bytes: "8KiB"
  body_read_timeout: "20s"
  upstream_response_header_timeout: "45s"
  upstream_error_body_timeout: "7s"
  response_write_timeout: "45s"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
        &base("server:\n  read_header_timeout: \"0s\"\n"),
    );
    let err = orihsus::config::load(&zero_timeout).unwrap_err();
    assert!(
        format!("{err}").contains("read_header_timeout"),
        "got: {err}"
    );

    let zero_headers = write_config(
        dir.path(),
        "headers.yaml",
        &base("server:\n  max_header_bytes: \"0KiB\"\n"),
    );
    let err = orihsus::config::load(&zero_headers).unwrap_err();
    assert!(format!("{err}").contains("max_header_bytes"), "got: {err}");

    let zero_body_read = write_config(
        dir.path(),
        "body_read.yaml",
        &base("server:\n  body_read_timeout: \"0s\"\n"),
    );
    let err = orihsus::config::load(&zero_body_read).unwrap_err();
    assert!(format!("{err}").contains("body_read_timeout"), "got: {err}");

    let zero_upstream_header = write_config(
        dir.path(),
        "upstream_header.yaml",
        &base("server:\n  upstream_response_header_timeout: \"0s\"\n"),
    );
    let err = orihsus::config::load(&zero_upstream_header).unwrap_err();
    assert!(
        format!("{err}").contains("upstream_response_header_timeout"),
        "got: {err}"
    );

    let zero_error_body = write_config(
        dir.path(),
        "error_body.yaml",
        &base("server:\n  upstream_error_body_timeout: \"0s\"\n"),
    );
    let err = orihsus::config::load(&zero_error_body).unwrap_err();
    assert!(
        format!("{err}").contains("upstream_error_body_timeout"),
        "got: {err}"
    );

    let zero_response_write = write_config(
        dir.path(),
        "response_write.yaml",
        &base("server:\n  response_write_timeout: \"0s\"\n"),
    );
    let err = orihsus::config::load(&zero_response_write).unwrap_err();
    assert!(
        format!("{err}").contains("response_write_timeout"),
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();

    assert!(format!("{err}").contains("gateway token"), "got: {err}");
}

#[test]
fn missing_tls_paths_are_rejected() {
    let dir = TempDir::new().unwrap();

    let missing_cert = write_config(
        dir.path(),
        "no-cert.yaml",
        r#"
gateway_token: "gway-secret"
tls:
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
"#,
    );
    let err = orihsus::config::load(&missing_cert).unwrap_err();
    assert!(format!("{err}").contains("cert_path"), "got: {err}");

    let missing_key = write_config(
        dir.path(),
        "no-key.yaml",
        r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
"#,
    );
    let err = orihsus::config::load(&missing_key).unwrap_err();
    assert!(format!("{err}").contains("key_path"), "got: {err}");
}

#[test]
fn non_https_upstream_is_rejected() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "http://api.opencode.go"
keys:
  - "key-1"
"#,
    );

    let err = orihsus::config::load(&path).unwrap_err();

    assert!(format!("{err}").contains("https"), "got: {err}");
}

#[test]
fn base_url_path_prefix_normalizes_to_a_trailing_slash() {
    // /openai and /openai/ must resolve identically: the path prefix is
    // normalized to a trailing slash so forward_request's join keeps it (a bare
    // /openai would let Url::join drop the last path segment).
    for (input, expected) in [
        (
            "https://api.opencode.go/openai",
            "https://api.opencode.go/openai/",
        ),
        (
            "https://api.opencode.go/openai/",
            "https://api.opencode.go/openai/",
        ),
        (
            "https://api.opencode.go/a/b",
            "https://api.opencode.go/a/b/",
        ),
        ("https://api.opencode.go", "https://api.opencode.go/"),
    ] {
        let dir = TempDir::new().unwrap();
        let path = write_config(
            dir.path(),
            "config.yaml",
            &format!(
                r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "{input}"
keys:
  - "key-1"
"#
            ),
        );
        let cfg = orihsus::config::load(&path).unwrap();
        assert_eq!(cfg.upstream.base_url.as_str(), expected, "input {input}");
        assert_eq!(
            cfg.upstream
                .base_url
                .join("v1/chat/completions")
                .unwrap()
                .as_str(),
            format!("{expected}v1/chat/completions"),
            "input {input}: the forwarded join must keep the path prefix"
        );
    }
}

#[test]
fn base_url_with_query_or_fragment_is_rejected() {
    let dir = TempDir::new().unwrap();
    for (name, bad) in [
        (
            "query.yaml",
            "https://api.opencode.go?api-version=2026-01-01",
        ),
        ("fragment.yaml", "https://api.opencode.go/openai#frag"),
        ("query-and-path.yaml", "https://api.opencode.go/openai?x=1"),
    ] {
        let path = write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "{bad}"
keys:
  - "key-1"
"#
            ),
        );
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains("base_url"), "{name}: got: {err}");
    }
}

#[test]
fn empty_or_duplicate_keys_are_rejected() {
    let dir = TempDir::new().unwrap();
    let base = |keys: &str| {
        format!(
            r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
            "  queue_wait_timeout: \"0s\"\n",
            "queue_wait_timeout",
        ),
        (
            "body_bytes_zero.yaml",
            "  max_body_bytes: \"0MiB\"\n",
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
server:
  max_header_bytes: "4294967296"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
limits:
  max_body_bytes: "20MiB"
  max_inflight_body_bytes: "64MiB"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
            "  max_inflight_body_bytes: \"0MiB\"\n",
            "max_inflight_body_bytes",
        ),
        (
            "inflight_lt_body.yaml",
            "  max_body_bytes: \"10MiB\"\n  max_inflight_body_bytes: \"1MiB\"\n",
            "max_inflight_body_bytes",
        ),
        (
            "inflight_exceeds_u32.yaml",
            "  max_inflight_body_bytes: \"5GiB\"\n",
            "max_inflight_body_bytes",
        ),
        (
            "body_exceeds_u32.yaml",
            "  max_body_bytes: \"5GiB\"\n",
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
fn rotation_bounds_are_enforced() {
    let dir = TempDir::new().unwrap();
    let with_rotation = |name: &str, rotation: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
{rotation}
"#
            ),
        )
    };

    let cases: &[(&str, &str, &str)] = &[
        (
            "backoff_initial_zero.yaml",
            "  backoff_initial: \"0s\"\n",
            "backoff_initial",
        ),
        (
            "backoff_max_zero.yaml",
            "  backoff_max: \"0s\"\n",
            "backoff_max",
        ),
        (
            "backoff_max_lt_initial.yaml",
            "  backoff_initial: \"60s\"\n  backoff_max: \"5s\"\n",
            "backoff_max",
        ),
        (
            "breaker_threshold_zero.yaml",
            "  breaker_threshold: 0\n",
            "breaker_threshold",
        ),
        (
            "breaker_cooldown_zero.yaml",
            "  breaker_cooldown: \"0s\"\n",
            "breaker_cooldown",
        ),
        (
            "backoff_max_extreme.yaml",
            "  backoff_max: \"18446744073709551615s\"\n",
            "backoff_max",
        ),
    ];
    for (name, snippet, field) in cases {
        let path = with_rotation(name, snippet);
        let err = orihsus::config::load(&path).unwrap_err();
        assert!(format!("{err}").contains(field), "{field}: got: {err}");
    }
}

#[test]
fn backoff_max_is_capped_at_the_ops_cooldown_ceiling() {
    let dir = TempDir::new().unwrap();
    let with_backoff = |name: &str, max: &str| {
        write_config(
            dir.path(),
            name,
            &format!(
                r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
  backoff_max: "{max}"
"#
            ),
        )
    };

    // A huge-but-parseable humantime value would overflow the jitter addition
    // (process abort under panic="abort"); the config layer must reject it as
    // invalid instead of accepting it.
    let extreme = with_backoff("extreme.yaml", "18446744073709551615s");
    let err = orihsus::config::load(&extreme).unwrap_err();
    assert!(
        format!("{err}").contains("backoff_max"),
        "an extreme backoff_max must be rejected: {err}"
    );

    // The ops ceiling (MAX_COOLDOWN = 90 days) is the sane upper bound: at the
    // ceiling the config still loads, just past it the load fails.
    let at_ceiling = with_backoff("ceiling.yaml", "90d");
    assert!(
        orihsus::config::load(&at_ceiling).is_ok(),
        "backoff_max at the 90d ops ceiling must be accepted"
    );
    let past_ceiling = with_backoff("past.yaml", "91d");
    let err = orihsus::config::load(&past_ceiling).unwrap_err();
    assert!(
        format!("{err}").contains("backoff_max"),
        "backoff_max past the 90d ops ceiling must be rejected: {err}"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
keys:
  - "key-1"
rotation:
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
            "rotation-typo.yaml",
            "rotation:\n  backoff_maximum: \"60s\"\n",
            "backoff_max",
        ),
        (
            "tls-typo.yaml",
            "tls:\n  certpath: \"/etc/orihsus/cert.pem\"\n",
            "cert_path",
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
        "absent models must default to the backwards-compatible deepseek-chat list"
    );

    let custom = write_config(
        dir.path(),
        "custom.yaml",
        r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
fn config_file_must_have_0600_permissions() {
    let dir = TempDir::new().unwrap();
    let path = write_config(
        dir.path(),
        "config.yaml",
        r#"
gateway_token: "gway-secret"
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
tls: [unclosed
upstream:
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
tls:
  cert_path: "/etc/orihsus/cert.pem"
  key_path: "/etc/orihsus/key.pem"
upstream:
  base_url: "https://api.opencode.go"
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
