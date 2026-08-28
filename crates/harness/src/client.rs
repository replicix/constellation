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
    /// Extra environment for the mount process (lease tuning etc.).
    env: Vec<(String, String)>,
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
            env: Vec::new(),
        })
    }

    /// Extra env for this client's mount (e.g. a short lease TTL).
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.to_string(), value.to_string()));
        self
    }

    fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(bin());
        c.args(args)
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT", &self.endpoint)
            .env("AWS_ALLOW_HTTP", "true")
            // Snappy cross-node propagation for multi-client scenarios.
            .env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
            // Fail fast when toxiproxy cuts S3 so FUSE threads are not
            // stuck in object_store's default 180s retry budget.
            .env("CONSTELLATION_S3_MAX_RETRIES", "2")
            .env("CONSTELLATION_S3_RETRY_TIMEOUT_MS", "2000");
        for (k, v) in &self.env {
            c.env(k, v);
        }
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

    /// Freeze the daemon (SIGSTOP): the process stays alive with its
    /// mount and unshipped journal intact but stops renewing its lease —
    /// the "unreachable holder" case from DESIGN.md §4.
    pub fn pause(&self) -> Result<()> {
        self.signal(libc::SIGSTOP)
    }

    /// Thaw a paused daemon (SIGCONT).
    pub fn resume(&self) -> Result<()> {
        self.signal(libc::SIGCONT)
    }

    fn signal(&self, sig: i32) -> Result<()> {
        let child = self.child.as_ref().context("not mounted")?;
        let pid = child.id() as libc::pid_t;
        if unsafe { libc::kill(pid, sig) } != 0 {
            bail!(
                "kill({pid}, {sig}) failed: {}",
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn is_mounted(&self) -> bool {
        self.child.is_some() && is_mountpoint(&self.mnt)
    }

    pub fn tail_log(&self) -> String {
        self.tail_log_n(15)
    }

    pub fn tail_log_n(&self, n: usize) -> String {
        std::fs::read_to_string(&self.log)
            .map(|s| {
                let lines: Vec<&str> = s.lines().rev().take(n).collect();
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

    pub fn control_status(&self) -> Result<serde_json::Value> {
        use std::io::{BufRead, BufReader, Write};
        use std::time::Duration;
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)
            .with_context(|| format!("connecting to {}", sock.display()))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
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

    pub fn reintegrate(&self) -> Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::time::Duration;
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)
            .with_context(|| format!("connecting to {}", sock.display()))?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.write_all(b"{\"cmd\":\"reintegrate\"}\n")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        let resp: serde_json::Value = serde_json::from_str(&line)?;
        anyhow::ensure!(
            resp["resp"] == "ok",
            "reintegration failed: {resp}; log:\n{}",
            self.tail_log_n(80)
        );
        Ok(())
    }

    /// Permanently leave the cluster (self), or admin-retire `node_id`.
    pub fn leave(&mut self, node_id: Option<u64>, force: bool) -> Result<()> {
        let mut args = vec![
            "leave".to_string(),
            "--state-dir".into(),
            self.state.display().to_string(),
        ];
        if let Some(id) = node_id {
            args.push("--node-id".into());
            args.push(id.to_string());
        }
        if force {
            args.push("--force".into());
        }
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let out = self.cmd(&arg_refs).output()?;
        if !out.status.success() {
            bail!(
                "leave failed: {}\n{}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        // Self-leave triggers fusermount; wait for the daemon to exit.
        if node_id.is_none() {
            if let Some(mut child) = self.child.take() {
                for _ in 0..100 {
                    if child.try_wait()?.is_some() {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                child.kill().ok();
                bail!("{} daemon did not exit after leave", self.name);
            }
        }
        Ok(())
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
