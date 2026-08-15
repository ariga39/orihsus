use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[test]
fn deployment_assets_exist() {
    for path in [
        "src/main.rs",
        "deploy/orihsus.service",
        "config.example.yaml",
        "docs/DEPLOYMENT.md",
        "README.md",
    ] {
        assert!(Path::new(path).exists(), "deployment asset missing: {path}");
    }
}

#[test]
fn systemd_unit_contains_the_required_safety_directives() {
    let text = fs::read_to_string("deploy/orihsus.service").unwrap();
    for required in [
        "User=orihsus",
        "Group=orihsus",
        "ExecStart=",
        "--config /etc/orihsus/config.yaml",
        "Restart=on-failure",
        "NoNewPrivileges=true",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "PrivateTmp=true",
        "PrivateDevices=true",
        "ProtectKernelTunables=true",
        "ProtectKernelModules=true",
        "ProtectControlGroups=true",
        "RestrictSUIDSGID=true",
        "LockPersonality=true",
        "MemoryDenyWriteExecute=true",
        "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
        "ReadOnlyPaths=",
        "ReadWritePaths=",
        "CapabilityBoundingSet=",
        "AmbientCapabilities=",
    ] {
        assert!(
            text.contains(required),
            "systemd unit missing required directive: {required}"
        );
    }
    // capability lists must be empty (non-root, cannot bind low ports)
    let cap_line = text
        .lines()
        .find(|l| l.starts_with("CapabilityBoundingSet="))
        .unwrap();
    assert_eq!(cap_line, "CapabilityBoundingSet=");
    let ambient_line = text
        .lines()
        .find(|l| l.starts_with("AmbientCapabilities="))
        .unwrap();
    assert_eq!(ambient_line, "AmbientCapabilities=");
}

#[test]
fn systemd_stop_timeout_covers_the_full_shutdown_budget() {
    // The process drains in-flight connections for up to 30s (DRAIN_TIMEOUT)
    // and then flushes the audit writer with a 5s bound (AUDIT_SHUTDOWN_TIMEOUT).
    // TimeoutStopSec must cover both plus margin, or systemd SIGKILLs a
    // legitimately-draining process and truncates already-accepted audit records.
    let text = fs::read_to_string("deploy/orihsus.service").unwrap();
    let line = text
        .lines()
        .find(|l| l.starts_with("TimeoutStopSec="))
        .expect("unit must set TimeoutStopSec");
    let secs: u64 = line
        .trim_start_matches("TimeoutStopSec=")
        .parse()
        .expect("TimeoutStopSec must be a plain integer number of seconds");
    assert_eq!(
        secs, 45,
        "TimeoutStopSec must be the chosen 45s (30s drain + 5s audit flush + margin)"
    );
    assert!(
        !text.contains("drain ≤ 5s"),
        "the unit comment must not understate the drain budget"
    );
    assert!(
        text.contains("30s drain") && text.contains("5s audit"),
        "the unit comment must document the 30s drain + 5s audit flush it covers"
    );
}

#[test]
fn deployment_docs_agree_on_the_shutdown_budget() {
    let docs = fs::read_to_string("docs/DEPLOYMENT.md").unwrap();
    assert!(
        !docs.contains("TimeoutStopSec=10"),
        "DEPLOYMENT.md must not still cite the old 10s TimeoutStopSec"
    );
    assert!(
        docs.contains("TimeoutStopSec=45"),
        "DEPLOYMENT.md must document the chosen 45s TimeoutStopSec"
    );
    assert!(
        docs.contains("≤30s") || docs.contains("30 秒"),
        "DEPLOYMENT.md must keep documenting the ≤30s connection drain"
    );
}

#[test]
fn example_config_loads_when_copied_and_chmod_600() {
    let dir = tempfile::TempDir::new().unwrap();
    let src = fs::read_to_string("config.example.yaml").unwrap();
    let path = dir.path().join("config.yaml");
    fs::write(&path, &src).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

    let cfg = orihsus::config::load(&path).unwrap();
    assert_eq!(cfg.listen.port(), 8443);
    assert_eq!(cfg.server.max_header_bytes, 32 * 1024);
    assert_eq!(cfg.server.max_connections, 1024);
    assert_eq!(cfg.audit.queue_capacity, 4096);
    assert_eq!(cfg.keys.len(), 2);
}

#[test]
fn docs_and_example_recommend_service_user_readable_ownership() {
    let unit = fs::read_to_string("deploy/orihsus.service").unwrap();
    assert!(
        unit.contains("User=orihsus"),
        "precondition: the service runs as the orihsus user"
    );

    for asset in ["docs/DEPLOYMENT.md", "config.example.yaml"] {
        let text = fs::read_to_string(asset).unwrap();
        assert!(
            !text.contains("root:orihsus"),
            "{asset} must not recommend root:orihsus: a 0600 root-owned file is unreadable by the orihsus service user"
        );
        assert!(
            text.contains("orihsus:orihsus"),
            "{asset} must recommend orihsus:orihsus ownership so the service user can read the file"
        );
        assert!(
            text.contains("600"),
            "{asset} must keep the 0600 plaintext-key requirement"
        );
    }
}

#[test]
fn docs_apply_the_readable_owner_to_config_and_tls_key() {
    let deployment = fs::read_to_string("docs/DEPLOYMENT.md").unwrap();
    assert!(
        deployment.contains("chown orihsus:orihsus /etc/orihsus/config.yaml"),
        "config ownership must be orihsus:orihsus"
    );
    assert!(
        deployment.contains("orihsus:orihsus /etc/orihsus/cert.pem /etc/orihsus/key.pem"),
        "TLS cert/key ownership must be orihsus:orihsus"
    );
    assert!(
        deployment.contains("chmod 600"),
        "config and TLS files must stay 0600"
    );
}

const MINIMAL_NO_LISTEN: &str = r#"
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
fn default_and_example_listen_are_bindable_by_nonroot_without_capabilities() {
    let dir = tempfile::TempDir::new().unwrap();

    let example = dir.path().join("example.yaml");
    fs::write(&example, fs::read_to_string("config.example.yaml").unwrap()).unwrap();
    fs::set_permissions(&example, fs::Permissions::from_mode(0o600)).unwrap();
    let cfg = orihsus::config::load(&example).unwrap();
    assert!(
        cfg.listen.port() >= 1024,
        "example listen port {} must be a high port bindable by non-root",
        cfg.listen.port()
    );

    let minimal = dir.path().join("minimal.yaml");
    fs::write(&minimal, MINIMAL_NO_LISTEN).unwrap();
    fs::set_permissions(&minimal, fs::Permissions::from_mode(0o600)).unwrap();
    let cfg = orihsus::config::load(&minimal).unwrap();
    assert!(
        cfg.listen.port() >= 1024,
        "default listen port {} must be a high port bindable by non-root",
        cfg.listen.port()
    );
}
