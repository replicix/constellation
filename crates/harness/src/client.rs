//! A constellation client node: its own state dir + mountpoint, driving
//! the real `constellation` binary. Supports clean unmount, hard kill
//! (SIGKILL, simulating a crash), and remount.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct Client {
    pub name: String,
    pub backend: String,
    pub endpoint: String,
    #[allow(dead_code)]
    pub work: PathBuf,
    pub mnt: PathBuf,
    state: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

fn bin() -> PathBuf {
    std::env::var_os("CONSTELLATION_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // target/release relative to the workspace root.
            let target = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target"));
            target.join("release/constellation")
        })
}

impl Client {
    /// Prepare a client working under `root` (not yet mounted).
    pub fn new(root: &Path, name: &str, endpoint: &str, backend: &str) -> Result<Self> {
        let work = root.join(name);
        let mnt = work.join("mnt");
        let state = work.join("state");
        std::fs::create_dir_all(&mnt)?;
        std::fs::create_dir_all(&state)?;
        Ok(Self {
            name: name.to_string(),
            backend: backend.to_string(),
            endpoint: endpoint.to_string(),
            log: work.join("mount.log"),
            work,
            mnt,
            state,
            child: None,
        })
    }

    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(bin());
        c.args(args)
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT", &self.endpoint)
            .env("AWS_ALLOW_HTTP", "true");
        c
    }

    pub fn fs_create(&self) -> Result<()> {
        let out = self
            .cmd(&[
                "fs",
                "create",
                "--s3",
                &self.backend,
                "--chunk-size",
                "1048576",
            ])
            .output()?;
        if !out.status.success() {
            bail!("fs create failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        Ok(())
    }

    pub fn mount(&mut self) -> Result<()> {
        if self.child.is_some() {
            bail!("{} already mounted", self.name);
        }
        let logf = std::fs::File::create(&self.log)?;
        let child = self
            .cmd(&[
                "mount",
                "--s3",
                &self.backend,
                self.mnt.to_str().unwrap(),
                "--state-dir",
                self.state.to_str().unwrap(),
            ])
            .stdout(Stdio::from(logf.try_clone()?))
            .stderr(Stdio::from(logf))
            .spawn()
            .context("spawning mount")?;
        self.child = Some(child);
        for _ in 0..100 {
            if is_mountpoint(&self.mnt) {
                return Ok(());
            }
            if let Some(st) = self.child.as_mut().unwrap().try_wait()? {
                bail!(
                    "{} mount died at startup ({st}): {}",
                    self.name,
                    self.tail_log()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        bail!("{} mount did not appear", self.name)
    }

    /// Clean unmount (flushes, exits the daemon).
    pub fn unmount(&mut self) -> Result<()> {
        let _ = Command::new("fusermount3")
            .args(["-u"])
            .arg(&self.mnt)
            .status();
        if let Some(mut child) = self.child.take() {
            for _ in 0..100 {
                if child.try_wait()?.is_some() {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            child.kill().ok();
            bail!("{} daemon did not exit after unmount", self.name);
        }
        Ok(())
    }

    /// Crash: SIGKILL the daemon, then clean up the dead mountpoint.
    pub fn kill9(&mut self) -> Result<()> {
        let mut child = self.child.take().context("not mounted")?;
        child.kill().context("SIGKILL")?;
        child.wait()?;
        // The kernel keeps a dead FUSE mount around; detach it.
        let _ = Command::new("fusermount3")
            .args(["-u", "-z"])
            .arg(&self.mnt)
            .status();
        Ok(())
    }

    #[allow(dead_code)]
    pub fn is_mounted(&self) -> bool {
        self.child.is_some() && is_mountpoint(&self.mnt)
    }

    pub fn tail_log(&self) -> String {
        std::fs::read_to_string(&self.log)
            .map(|s| {
                let lines: Vec<&str> = s.lines().rev().take(15).collect();
                lines.into_iter().rev().collect::<Vec<_>>().join("\n")
            })
            .unwrap_or_default()
    }

    /// Wipe the local chunk cache (keeps metadata) — forces S3 reads.
    pub fn drop_cache(&self) -> Result<()> {
        let cache = self.state.join("cache");
        if cache.exists() {
            std::fs::remove_dir_all(&cache)?;
        }
        Ok(())
    }

    /// Query the daemon's control socket (`status`).
    pub fn control_status(&self) -> Result<serde_json::Value> {
        use std::io::{BufRead, BufReader, Write};
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)
            .with_context(|| format!("connecting to {}", sock.display()))?;
        stream.write_all(b"{\"cmd\":\"status\"}\n")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        let resp: serde_json::Value = serde_json::from_str(&line)?;
        anyhow::ensure!(
            resp["resp"] == "status",
            "unexpected control response: {resp}"
        );
        Ok(resp)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("fusermount3")
                .args(["-u"])
                .arg(&self.mnt)
                .status();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn is_mountpoint(p: &Path) -> bool {
    Command::new("mountpoint")
        .arg("-q")
        .arg(p)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
