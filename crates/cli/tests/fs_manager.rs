//! The CLI's `fs create`, `doctor`, `fs list` and `export` over the
//! filesystem manager (plan 31 §4): their output, and the rules the CLI
//! took from it when its implementation and the control service's became
//! one ("Plan 31 closing: one Manager for the CLI and the control
//! service"). The tests of a changed rule name what the CLI did before.
//! Local-directory backends; registry, config, data and runtime dirs in a
//! temp dir.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// nextest remaps binary paths when a test runs from an extracted archive;
/// see `mount_state_dir.rs`.
fn bin() -> String {
    std::env::var("NEXTEST_BIN_EXE_constellation")
        .or_else(|_| std::env::var("CARGO_BIN_EXE_constellation"))
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_constellation").to_string())
}

fn constellation(dir: &Path, args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .env("CONSTELLATION_REGISTRY", dir.join("config/registry.toml"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_RUNTIME_DIR", dir.join("run"))
        .env("CONSTELLATION_P2P", "off")
        .env("CONSTELLATION_CONTROL_TIMEOUT_MS", "2000")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

fn p(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn text(out: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// An empty directory to make a filesystem in.
fn backend(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join("s3").join(name);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn registry(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("config/registry.toml")).unwrap_or_default()
}

#[test]
fn create_doctor_and_list_print_what_they_did() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = backend(dir.path(), "fs");
    let out = constellation(
        dir.path(),
        &[
            "fs",
            "create",
            "myfs",
            "--s3",
            p(&s3),
            "--chunk-size",
            "2097152",
            "--compression",
            "raw",
            "--max-size",
            "1G",
        ],
    );
    let (stdout, stderr) = text(&out);
    assert!(out.status.success(), "{stdout}{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines[0].starts_with("created filesystem ") && lines[0].ends_with(p(&s3)));
    assert_eq!(
        lines[1..],
        [
            "  name:        myfs",
            "  chunk_size:  2097152",
            "  compression: raw",
            "  e2e:         false",
            "  max_size:    1073741824",
        ]
    );
    // The preflight report, on stderr.
    assert!(stderr.contains("write object (PUT)"), "{stderr}");

    let out = constellation(dir.path(), &["doctor", "myfs"]);
    let (stdout, stderr) = text(&out);
    assert!(out.status.success(), "{stdout}{stderr}");
    let labels = [
        "backend ...........................",
        "create-if-absent (If-None-Match) ... ok",
        "etag CAS (If-Match) ...............",
        "conditional-write semantics (what this provider answers):",
        "bucket versioning .................",
        "filesystem at prefix .............. ok (",
    ];
    for label in labels {
        assert!(
            stdout.lines().any(|l| l.starts_with(label)),
            "{label:?} missing:\n{stdout}"
        );
    }

    let out = constellation(dir.path(), &["fs", "list"]);
    let (stdout, stderr) = text(&out);
    assert!(out.status.success(), "{stdout}{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines[0].starts_with("NAME") && lines[0].ends_with("MOUNTED?"));
    assert!(
        lines[1].starts_with("myfs ") && lines[1].contains(p(&s3)),
        "{stdout}"
    );
    assert!(lines[1].ends_with(" no"), "{stdout}");
}

/// Was: the preflight ran and printed, (for `--e2e`) the passphrase was
/// asked for, and only the conditional create then failed.
#[test]
fn create_refuses_an_existing_filesystem_before_the_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = backend(dir.path(), "fs");
    assert!(
        constellation(dir.path(), &["fs", "create", "one", "--s3", p(&s3)])
            .status
            .success()
    );
    let out = constellation(dir.path(), &["fs", "create", "two", "--s3", p(&s3)]);
    let (stdout, stderr) = text(&out);
    assert!(!out.status.success(), "{stdout}{stderr}");
    assert!(stderr.contains("already exists"), "{stderr}");
    assert!(!stderr.contains("write object (PUT)"), "{stderr}");
    assert!(!registry(dir.path()).contains("[two]"));
}

/// The test suites' fixed names (`tests`, `harness`): a name registered
/// by `fs create` and never mounted is pointed at the next one.
#[test]
fn create_reuses_a_name_that_was_never_mounted() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (backend(dir.path(), "a"), backend(dir.path(), "b"));
    for s3 in [&a, &b] {
        let out = constellation(dir.path(), &["fs", "create", "tests", "--s3", p(s3)]);
        assert!(out.status.success(), "{out:?}");
    }
    assert!(
        registry(dir.path()).contains(&format!("s3 = \"{}\"", p(&b))),
        "{}",
        registry(dir.path())
    );
}

/// Was: the name was repointed at the new filesystem, and its state dir's
/// replica of the old one would have served it.
#[test]
fn create_does_not_repoint_a_name_in_use() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (backend(dir.path(), "a"), backend(dir.path(), "b"));
    let out = constellation(dir.path(), &["fs", "create", "myfs", "--s3", p(&a)]);
    assert!(out.status.success(), "{out:?}");
    std::fs::create_dir_all(dir.path().join("data/constellation/myfs/meta.db")).unwrap();
    let out = constellation(dir.path(), &["fs", "create", "myfs", "--s3", p(&b)]);
    let (stdout, stderr) = text(&out);
    assert!(!out.status.success(), "{stdout}{stderr}");
    assert!(stderr.contains("is in use"), "{stderr}");
    assert!(!b.join("meta.json").exists(), "created at {b:?}");
    assert!(registry(dir.path()).contains(&format!("s3 = \"{}\"", p(&a))));
}

/// Was: registered, with `<data dir>/..` as its state dir, which
/// `export ..` then deleted with every other name's state dir in it.
#[test]
fn create_refuses_a_name_that_is_not_one_path_component() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = backend(dir.path(), "fs");
    for name in ["..", "a:b"] {
        let out = constellation(dir.path(), &["fs", "create", name, "--s3", p(&s3)]);
        let (stdout, stderr) = text(&out);
        assert!(!out.status.success(), "{name}: {stdout}{stderr}");
        assert!(stderr.contains("invalid filesystem name"), "{stderr}");
        assert!(!s3.join("meta.json").exists(), "{name}: created");
    }
}

/// Was: a row whose state dir holds others' (written before names were
/// checked) was exported by deleting that dir, others' state dirs with it.
#[test]
fn export_does_not_delete_a_dir_holding_another_names_state_dir() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = backend(dir.path(), "fs");
    assert!(
        constellation(dir.path(), &["fs", "create", "inner", "--s3", p(&s3)])
            .status
            .success()
    );
    let inner = dir.path().join("data/constellation/inner");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::write(inner.join("daemon.log"), b"x").unwrap();
    let mut rows = registry(dir.path());
    rows.push_str(&format!(
        "\n[outer]\ns3 = \"/nowhere\"\nstate_dir = \"{}\"\nread_only_member = false\n\
         web_ui = 0\nmounts = []\n",
        p(&dir.path().join("data"))
    ));
    std::fs::write(dir.path().join("config/registry.toml"), rows).unwrap();
    let out = constellation(dir.path(), &["export", "outer"]);
    let (stdout, stderr) = text(&out);
    assert!(!out.status.success(), "{stdout}{stderr}");
    assert!(stderr.contains("refusing to delete it"), "{stderr}");
    assert!(inner.join("daemon.log").exists());

    // The name's own state dir goes, with its row.
    let out = constellation(dir.path(), &["export", "inner"]);
    assert!(out.status.success(), "{out:?}");
    assert!(!inner.exists());
    assert!(!registry(dir.path()).contains("[inner]"));
}

/// `mount NAME` holds its state dir from the registry write on: a create
/// of NAME elsewhere while the mount is still reading `meta.json` (or at
/// the passphrase prompt, or forking) is refused. It was not: the state dir
/// was free until the daemon took `daemon.lock`, so the row was repointed
/// and the daemon then filled a replica of the old URL under it. The read
/// is held open here with a FIFO as `meta.json`.
#[test]
fn a_mount_holds_its_state_dir_from_its_registration() {
    let dir = tempfile::tempdir().unwrap();
    let (slow, other) = (backend(dir.path(), "slow"), backend(dir.path(), "other"));
    let fifo = slow.join("meta.json");
    let made = Command::new("mkfifo").arg(&fifo).status().unwrap();
    assert!(made.success());
    let mnt = dir.path().join("no/such/mnt");
    let mut mount = Command::new(bin())
        .args(["mount", "myfs", p(&mnt), "--s3", p(&slow), "--foreground"])
        .env(
            "CONSTELLATION_REGISTRY",
            dir.path().join("config/registry.toml"),
        )
        .env("XDG_CONFIG_HOME", dir.path().join("config"))
        .env("XDG_DATA_HOME", dir.path().join("data"))
        .env("XDG_RUNTIME_DIR", dir.path().join("run"))
        .env("CONSTELLATION_P2P", "off")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Registered, and blocked opening the FIFO.
    let t = std::time::Instant::now();
    while !registry(dir.path()).contains("[myfs]") {
        assert!(t.elapsed().as_secs() < 60, "mount never registered myfs");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    std::thread::sleep(std::time::Duration::from_millis(500));
    let out = constellation(dir.path(), &["fs", "create", "myfs", "--s3", p(&other)]);
    let _ = mount.kill();
    let _ = mount.wait();
    let (stdout, stderr) = text(&out);
    assert!(
        !out.status.success(),
        "repointed under a mount: {stdout}{stderr}"
    );
    assert!(stderr.contains("is in use"), "{stderr}");
    assert!(!other.join("meta.json").exists());
    assert!(registry(dir.path()).contains(&format!("s3 = \"{}\"", p(&slow))));
}
