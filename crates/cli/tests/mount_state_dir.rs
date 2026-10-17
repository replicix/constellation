//! `mount NAME MOUNTPOINT --state-dir DIR` serves the name from DIR.
//!
//! The flag used to be accepted and ignored for a name: the daemon took
//! the name's registered state dir (`~/.local/share/constellation/NAME`
//! after `fs create`), while `status NAME --state-dir DIR` and the
//! caller's cleanup looked in DIR. Each mount here fails at its
//! mountpoint (a path that does not exist), so no FUSE is needed: by then
//! the daemon has taken the state dir it serves (`daemon.lock`).

use constellation_platform::drain::{small_stdio_pipes, DrainedChild};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

/// nextest remaps binary paths when a test runs from an extracted archive
/// (`--archive-file`/`--extract-to`): it exposes the remapped path as
/// `NEXTEST_BIN_EXE_constellation` and also rewrites `CARGO_BIN_EXE_constellation`
/// in the environment, so read those at runtime instead of trusting the
/// path `env!` baked in at compile time.
fn bin() -> &'static str {
    static BIN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        std::env::var("NEXTEST_BIN_EXE_constellation")
            .or_else(|_| std::env::var("CARGO_BIN_EXE_constellation"))
            .unwrap_or_else(|_| env!("CARGO_BIN_EXE_constellation").to_string())
    })
}

/// A `constellation` with its registry, config, data and runtime dirs in
/// `dir`, and no P2P endpoint.
fn constellation(dir: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(bin());
    // Deterministically what a loaded host gives every new pipe (8 KiB
    // past `pipe-user-pages-soft`): `mount` writes ~19 KiB to stderr, and
    // a parent that waits for the exit before it reads the pipe hangs.
    small_stdio_pipes(&mut cmd, 4096);
    cmd.args(args)
        .env("CONSTELLATION_REGISTRY", dir.join("config/registry.toml"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_RUNTIME_DIR", dir.join("run"))
        .env("CONSTELLATION_P2P", "off")
        .stdin(Stdio::null());
    // A mount that unexpectedly succeeds serves in the foreground for good.
    let done = DrainedChild::spawn(&mut cmd)
        .unwrap()
        .finish(Duration::from_secs(120))
        .unwrap();
    assert!(
        !done.timed_out,
        "constellation {args:?} still running after 120 s; stderr:\n{}",
        String::from_utf8_lossy(&done.output.stderr)
    );
    done.output
}

fn p(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// A registered (`fs create`), never mounted `myfs` on the local file
/// backend in `dir`; its default state dir, and a mountpoint that does
/// not exist.
fn created(dir: &Path) -> (PathBuf, PathBuf) {
    let s3 = dir.join("s3");
    std::fs::create_dir_all(&s3).unwrap();
    let out = constellation(dir, &["fs", "create", "myfs", "--s3", p(&s3)]);
    assert!(out.status.success(), "fs create: {out:?}");
    (dir.join("data/constellation/myfs"), dir.join("no/such/mnt"))
}

#[test]
fn a_named_mount_uses_the_state_dir_it_is_given() {
    let dir = tempfile::tempdir().unwrap();
    let (default, mnt) = created(dir.path());
    let state = dir.path().join("state");
    let out = constellation(
        dir.path(),
        &[
            "mount",
            "myfs",
            p(&mnt),
            "--state-dir",
            p(&state),
            "--foreground",
        ],
    );
    assert!(!out.status.success(), "mounted on a missing path: {out:?}");
    assert!(
        state.join("daemon.lock").exists(),
        "the daemon did not take --state-dir {state:?}: {out:?}"
    );
    assert!(
        !default.join("daemon.lock").exists(),
        "the daemon took the registered default {default:?} instead"
    );
    // Stored: `status myfs`, `umount myfs` and `export myfs` find it.
    let registry = std::fs::read_to_string(dir.path().join("config/registry.toml")).unwrap();
    assert!(
        registry.contains(&format!("state_dir = \"{}\"", p(&state))),
        "{registry}"
    );
}

#[test]
fn a_named_mount_does_not_move_a_populated_state_dir() {
    let dir = tempfile::tempdir().unwrap();
    let (default, mnt) = created(dir.path());
    std::fs::create_dir_all(default.join("meta.db")).unwrap();
    let elsewhere = dir.path().join("elsewhere");
    let out = constellation(
        dir.path(),
        &[
            "mount",
            "myfs",
            p(&mnt),
            "--state-dir",
            p(&elsewhere),
            "--foreground",
        ],
    );
    assert!(!out.status.success(), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("which is in use"), "{stderr}");
    assert!(!elsewhere.exists(), "{elsewhere:?} was created");
    assert!(!default.join("daemon.lock").exists());
}

/// `cd ~; constellation mount myfs MNT --state-dir .` must not make $HOME
/// the name's state dir: `export myfs` deletes it with everything in it.
#[test]
fn a_named_mount_refuses_a_state_dir_that_is_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let (default, mnt) = created(dir.path());
    let home = dir.path().join("home");
    std::fs::create_dir_all(home.join("documents")).unwrap();
    let out = constellation(
        dir.path(),
        &[
            "mount",
            "myfs",
            p(&mnt),
            "--state-dir",
            p(&home),
            "--foreground",
        ],
    );
    assert!(!out.status.success(), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("is not an empty directory"), "{stderr}");
    let registry = std::fs::read_to_string(dir.path().join("config/registry.toml")).unwrap();
    assert!(!registry.contains(p(&home)), "{registry}");
    assert!(!home.join("daemon.lock").exists() && !default.join("daemon.lock").exists());
}
