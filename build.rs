use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    track_current_ref();

    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|hash| hash.trim().to_owned())
        .filter(|hash| hash.len() >= 7 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(|hash| hash[..7].to_owned())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=ORIHSUS_COMMIT_HASH={commit}");
}

fn track_current_ref() {
    let Ok(head) = std::fs::read_to_string(".git/HEAD") else {
        return;
    };
    let Some(reference) = head.trim().strip_prefix("ref: ") else {
        return;
    };
    let path = Path::new(".git").join(reference);
    println!("cargo:rerun-if-changed={}", path.display());
}
