//! `harness smoke`: the end-to-end smoke test, ported from `tests/smoke.sh`
//! (which is now a thin wrapper that runs this). Same steps, same
//! assertions, same knobs:
//!
//! - `CONSTELLATION_BIN`: the binary under test (default
//!   `${CARGO_TARGET_DIR:-target}/debug/constellation`, like `tests/lib.sh`);
//! - the optional positional backend: a local directory (default, a fresh
//!   temp dir) or `s3://bucket/prefix`, whose endpoint and credentials come
//!   from the `AWS_*` environment as before (the command inherits it; unlike
//!   [`crate::client::Client`], nothing is overridden here).
//!
//! The steps: create a fs (a second create must be refused), mount, POSIX
//! namespace ops, a 3.5 MiB multi-chunk file, partial in-place edit,
//! truncate, append, unlink-while-open, rm/rmdir, unmount, remount and
//! verify persistence, remount with a cold chunk cache, `status`.

use crate::client::is_mountpoint;
use anyhow::{bail, ensure, Context, Result};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// `tests/lib.sh`'s `say`.
fn say(msg: &str) {
    println!("== {msg}");
}

fn bin() -> PathBuf {
    std::env::var_os("CONSTELLATION_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target"));
            target.join("debug/constellation")
        })
}

/// The mount under test, driven like `fs_mount`/`fs_unmount` in `lib.sh`:
/// a `--foreground` child whose log is appended to `mount.log`.
struct Mount {
    bin: PathBuf,
    backend: String,
    mnt: PathBuf,
    state: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

impl Mount {
    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(&self.bin);
        c.args(args);
        c
    }

    fn mount(&mut self) -> Result<()> {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        let child = self
            .cmd(&["mount", "/"])
            .arg(&self.mnt)
            .args(["--s3", &self.backend, "--state-dir"])
            .arg(&self.state)
            .arg("--foreground")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("spawning {}", self.bin.display()))?;
        self.child = Some(child);
        for _ in 0..100 {
            if is_mountpoint(&self.mnt) {
                return Ok(());
            }
            if let Some(st) = self.child.as_mut().unwrap().try_wait()? {
                self.child = None;
                bail!("FAIL: mount process died ({st})");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        bail!("FAIL: mount did not appear")
    }

    fn unmount(&mut self) {
        let ok = |prog: &str, mnt: &Path| {
            Command::new(prog)
                .arg("-u")
                .arg(mnt)
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        let _ = ok("fusermount3", &self.mnt) || ok("fusermount", &self.mnt);
        if let Some(mut child) = self.child.take() {
            // `wait "$MOUNT_PID"` in lib.sh, but bounded.
            let deadline = Instant::now() + Duration::from_secs(120);
            while Instant::now() < deadline {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `fs_cleanup`: unmount, dump the mount log if the run failed, remove the
/// work dir.
struct Cleanup {
    mount: Mount,
    work: PathBuf,
    passed: bool,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.mount.unmount();
        if !self.passed {
            if let Ok(log) = fs::read_to_string(&self.mount.log) {
                if !log.is_empty() {
                    println!("--- mount log ---");
                    print!("{log}");
                }
            }
        }
        let _ = fs::remove_dir_all(&self.work);
    }
}

fn cmp(a: &Path, b: &Path) -> Result<()> {
    let (x, y) = (fs::read(a)?, fs::read(b)?);
    ensure!(
        x == y,
        "FAIL: {} and {} differ (sizes {} vs {})",
        a.display(),
        b.display(),
        x.len(),
        y.len()
    );
    Ok(())
}

/// `dd if=/dev/urandom` for `len` bytes.
fn random_bytes(len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(buf)
}

/// `printf XYZ | dd of=f bs=1 seek=N conv=notrunc`.
fn overwrite_at(path: &Path, offset: u64, data: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .write(true)
        .open(path)?
        .write_all_at(data, offset)?;
    Ok(())
}

/// `echo tail >> f`.
fn append(path: &Path, data: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .append(true)
        .open(path)?
        .write_all(data)?;
    Ok(())
}

/// `$(cat f)`: content with trailing newlines stripped.
fn cat_trimmed(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)?.trim_end_matches('\n').to_string())
}

pub fn run(backend: Option<String>) -> Result<()> {
    // `fs_setup`: mktemp -d /tmp/constellation-test.XXXXXX.
    let base = if Path::new("/tmp").is_dir() {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    };
    let work = tempfile::Builder::new()
        .prefix("constellation-test.")
        .tempdir_in(base)?
        .keep();
    let backend = backend
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| work.join("backend").display().to_string());
    let mnt = work.join("mnt");
    fs::create_dir_all(&mnt)?;
    let mut cleanup = Cleanup {
        mount: Mount {
            bin: bin(),
            backend,
            mnt: mnt.clone(),
            state: work.join("state"),
            log: work.join("mount.log"),
            child: None,
        },
        work: work.clone(),
        passed: false,
    };
    smoke(&mut cleanup.mount, &work)?;
    cleanup.passed = true;
    println!("SMOKE TEST PASSED");
    Ok(())
}

fn smoke(m: &mut Mount, work: &Path) -> Result<()> {
    let mnt = m.mnt.clone();
    let backend = m.backend.clone();
    ensure!(
        m.bin.is_file(),
        "constellation binary not found at {} (build it, or set CONSTELLATION_BIN)",
        m.bin.display()
    );

    say(&format!("fs create + doctor ({backend})"));
    let out = m
        .cmd(&[
            "fs",
            "create",
            "tests",
            "--s3",
            &backend,
            "--chunk-size",
            "1048576",
            "--compression",
            "zstd:3",
        ])
        .status()?;
    ensure!(out.success(), "fs create failed ({out})");
    let out = m.cmd(&["doctor", "tests", "--s3", &backend]).status()?;
    ensure!(out.success(), "doctor failed ({out})");
    let second = m
        .cmd(&["fs", "create", "tests", "--s3", &backend])
        .stderr(Stdio::null())
        .status()?;
    ensure!(!second.success(), "FAIL: double create succeeded");

    say("mount");
    m.mount()?;

    say("basic namespace ops");
    fs::create_dir_all(mnt.join("dir/sub"))?;
    fs::write(mnt.join("dir/hello.txt"), "hello constellation\n")?;
    ensure!(cat_trimmed(&mnt.join("dir/hello.txt"))? == "hello constellation");
    std::os::unix::fs::symlink("hello.txt", mnt.join("dir/link"))?;
    ensure!(fs::read_link(mnt.join("dir/link"))? == Path::new("hello.txt"));
    ensure!(cat_trimmed(&mnt.join("dir/link"))? == "hello constellation");
    fs::rename(mnt.join("dir/hello.txt"), mnt.join("dir/sub/renamed.txt"))?;
    ensure!(cat_trimmed(&mnt.join("dir/sub/renamed.txt"))? == "hello constellation");
    let names: Vec<_> = fs::read_dir(mnt.join("dir"))?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<std::io::Result<_>>()?;
    ensure!(
        !names.iter().any(|n| n == "hello.txt"),
        "FAIL: old name still present"
    );

    say("multi-chunk file (3.5 MiB across 1 MiB chunks)");
    let reference = work.join("random.bin");
    let mut data = random_bytes(3 << 20)?;
    data.extend(random_bytes(512 << 10)?);
    fs::write(&reference, &data)?;
    fs::copy(&reference, mnt.join("dir/random.bin"))?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("partial in-place edit");
    overwrite_at(&mnt.join("dir/random.bin"), 2_000_000, b"XYZ")?;
    overwrite_at(&reference, 2_000_000, b"XYZ")?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("truncate");
    fs::OpenOptions::new()
        .write(true)
        .open(mnt.join("dir/random.bin"))?
        .set_len(1_500_000)?;
    fs::OpenOptions::new()
        .write(true)
        .open(&reference)?
        .set_len(1_500_000)?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("append");
    append(&mnt.join("dir/random.bin"), b"tail\n")?;
    append(&reference, b"tail\n")?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("unlink while open");
    fs::write(mnt.join("orphan.txt"), "orphan data\n")?;
    let mut held = fs::File::open(mnt.join("orphan.txt"))?;
    fs::remove_file(mnt.join("orphan.txt"))?;
    ensure!(!mnt.join("orphan.txt").exists());
    let mut text = String::new();
    held.seek(SeekFrom::Start(0))?;
    held.read_to_string(&mut text)?;
    ensure!(
        text.trim_end_matches('\n') == "orphan data",
        "FAIL: unlinked-but-open file read back {text:?}"
    );
    drop(held);

    say("rm/rmdir");
    fs::remove_file(mnt.join("dir/link"))?;
    fs::remove_file(mnt.join("dir/sub/renamed.txt"))?;
    fs::remove_dir(mnt.join("dir/sub"))?;
    ensure!(
        fs::remove_dir(mnt.join("dir")).is_err(),
        "FAIL: rmdir non-empty succeeded"
    );

    say("unmount");
    m.unmount();

    say("remount and verify persistence");
    m.mount()?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;
    ensure!(mnt.join("dir").is_dir());
    ensure!(!mnt.join("orphan.txt").exists());

    say("cold cache read (fresh chunk cache, data pulled from backend)");
    m.unmount();
    let _ = fs::remove_dir_all(m.state.join("cache"));
    m.mount()?;
    cmp(&reference, &mnt.join("dir/random.bin"))?;

    say("status");
    let status = m.cmd(&["status", "--s3", &backend]).output()?;
    ensure!(
        status.status.success(),
        "status failed ({}): {}",
        status.status,
        String::from_utf8_lossy(&status.stderr)
    );
    ensure!(
        String::from_utf8_lossy(&status.stdout).contains("uuid"),
        "FAIL: status output has no uuid"
    );

    m.unmount();
    Ok(())
}
