use std::process::Command;

#[test]
fn version_prints_package_version_and_build_commit() {
    let expected_commit = Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git is available for the repository test")
        .stdout;
    let expected_commit = String::from_utf8(expected_commit)
        .unwrap()
        .trim()
        .to_owned();

    for flag in ["-V", "--version"] {
        let output = Command::new(env!("CARGO_BIN_EXE_orihsus"))
            .arg(flag)
            .output()
            .expect("run orihsus version output");

        assert!(output.status.success(), "{flag}");
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("orihsus 0.1.0 commit {expected_commit}\n"),
            "{flag}"
        );
        assert!(output.stderr.is_empty(), "{flag}");
    }
}
