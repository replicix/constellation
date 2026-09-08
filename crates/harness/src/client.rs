//! A constellation client node: its own state dir + mountpoint, driving
//! the real `constellation` binary. Supports clean unmount, hard kill
//! (SIGKILL, simulating a crash), and remount.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long `mount_view` polls for the mountpoint to appear, and `unmount`/
/// `leave` poll for the daemon to exit, before giving up. Both loops return
/// as soon as the condition is met, so a generous ceiling costs nothing for
/// the common case of small, fast fault-injection scenarios — it only
/// raises the bound for genuinely slow or hung cases. Full-corpus
/// perf-regression runs (120k+ files, GB-scale write-back drains) routinely
/// need well over the 10s this used to allow: draining that many pending
/// uploads, shipping the journal tail, and writing a checkpoint on a large
/// SQLite replica are all legitimately slow, not hung, and the old bound
/// turned "still finishing" into a flaky "daemon did not exit after
/// unmount"/"mount did not appear" failure. Override with
/// `CONSTELLATION_HARNESS_MOUNT_TIMEOUT_S` for scenario-specific tuning.
fn client_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_HARNESS_MOUNT_TIMEOUT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(120),
    )
}

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
    /// `--cache-size` override; `None` keeps the binary's default.
    cache_size: Option<u64>,
    write_mode: Option<String>,
    e2e: bool,
    web_ui_port: Option<u16>,
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
            cache_size: None,
            write_mode: None,
            e2e: false,
            web_ui_port: None,
        })
    }

    /// Give this client its own P2P identity, stored in its state dir.
    ///
    /// The binary's default key path is per-user
    /// (~/.config/constellation/node.key), so N clients on one host
    /// share one iroh key unless a scenario overrides it: every dial
    /// between them then fails with "Connecting to ourself is not
    /// supported" and the whole P2P fast path (forwards, handoffs,
    /// coop fetch) is silently dead. Real fleets have one node per
    /// machine and never hit this. Scenarios that exercise live
    /// multi-node P2P must opt in; scenarios written against the
    /// S3-only slow path deliberately keep the shared key (or set
    /// CONSTELLATION_P2P=off).
    pub fn with_own_node_key(self) -> Self {
        let key = self.state.join("node.key").display().to_string();
        self.with_env("CONSTELLATION_NODE_KEY", &key)
    }

    /// Extra env for this client's mount (e.g. a short lease TTL).
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.to_string(), value.to_string()));
        self
    }

    /// Override `--cache-size` (bytes) at mount time.
    pub fn with_cache_size(mut self, bytes: u64) -> Self {
        self.cache_size = Some(bytes);
        self
    }

    pub fn with_write_mode(mut self, mode: &str) -> Self {
        self.write_mode = Some(mode.to_string());
        self
    }

    pub fn with_e2e(mut self) -> Self {
        self.e2e = true;
        self.env.push((
            "CONSTELLATION_PASSPHRASE".into(),
            "harness-correct-passphrase".into(),
        ));
        self
    }

    pub fn with_web_ui(mut self, port: u16) -> Self {
        self.web_ui_port = Some(port);
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
        let mut args = vec![
            "fs",
            "create",
            "--s3",
            &self.backend,
            "--chunk-size",
            "1048576",
        ];
        if self.e2e {
            args.push("--e2e");
        }
        let out = self.cmd(&args).output()?;
        if !out.status.success() {
            bail!("fs create failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        Ok(())
    }

    pub fn assert_wrong_passphrase_rejected(&self) -> Result<()> {
        let wrong_state = self.work.join("wrong-state");
        let wrong_mnt = self.work.join("wrong-mnt");
        std::fs::create_dir_all(&wrong_state)?;
        std::fs::create_dir_all(&wrong_mnt)?;
        let output = self
            .cmd(&[
                "mount",
                "--s3",
                &self.backend,
                wrong_mnt.to_str().unwrap(),
                "--state-dir",
                wrong_state.to_str().unwrap(),
            ])
            .env("CONSTELLATION_PASSPHRASE", "definitely-wrong")
            .output()?;
        if output.status.success() {
            bail!("mount unexpectedly accepted a wrong E2E passphrase");
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("unlocking E2E keyring") {
            bail!("wrong-passphrase failure was not clean: {stderr}");
        }
        Ok(())
    }

    pub fn mount(&mut self) -> Result<()> {
        self.mount_view(None, &[])
    }

    pub fn mount_view(&mut self, inner: Option<&str>, extra: &[&str]) -> Result<()> {
        if self.child.is_some() {
            bail!("{} already mounted", self.name);
        }
        let logf = std::fs::File::create(&self.log)?;
        let mut args = vec![
            "mount".to_string(),
            "--s3".to_string(),
            self.backend.clone(),
        ];
        if let Some(inner) = inner {
            args.push(inner.to_string());
        }
        args.push(self.mnt.to_str().unwrap().to_string());
        args.push("--state-dir".to_string());
        args.push(self.state.to_str().unwrap().to_string());
        args.extend(extra.iter().map(|arg| (*arg).to_string()));
        if let Some(bytes) = self.cache_size {
            args.push("--cache-size".to_string());
            args.push(bytes.to_string());
        }
        if let Some(mode) = &self.write_mode {
            args.push("--write-mode".to_string());
            args.push(mode.clone());
        }
        if let Some(port) = self.web_ui_port {
            args.push("--web-ui".to_string());
            args.push(port.to_string());
        }
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let child = self
            .cmd(&arg_refs)
            .stdout(Stdio::from(logf.try_clone()?))
            .stderr(Stdio::from(logf))
            .spawn()
            .context("spawning mount")?;
        self.child = Some(child);
        let deadline = Instant::now() + client_timeout();
        while Instant::now() < deadline {
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
        bail!(
            "{} mount did not appear within {:?}: {}",
            self.name,
            client_timeout(),
            self.tail_log()
        )
    }

    pub fn snapshot_create(&self, selector: &str) -> Result<()> {
        self.control_command(&["snapshot", "create", selector])
    }

    pub fn snapshot_delete(&self, selector: &str) -> Result<()> {
        self.control_command(&["snapshot", "delete", selector])
    }

    pub fn clone_snapshot(&self, selector: &str, destination: &str) -> Result<()> {
        self.control_command(&["clone", selector, destination])
    }

    pub fn replica_db(&self) -> PathBuf {
        self.state.join("meta.db")
    }

    pub fn gc_process(&self, orphans: bool) -> Result<Child> {
        let mut args = vec![
            "gc",
            "run",
            "--s3",
            &self.backend,
            "--state-dir",
            self.state.to_str().unwrap(),
        ];
        if orphans {
            args.push("--orphans");
        }
        Ok(self
            .cmd(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?)
    }

    pub fn gc_run(&self, orphans: bool) -> Result<std::process::Output> {
        Ok(self.gc_process(orphans)?.wait_with_output()?)
    }

    pub fn fsck(&self, repair: bool) -> Result<std::process::Output> {
        let mut args = vec![
            "fsck",
            "--s3",
            &self.backend,
            "--state-dir",
            self.state.to_str().unwrap(),
        ];
        if repair {
            args.push("--repair");
        }
        Ok(self.cmd(&args).output()?)
    }

    fn control_command(&self, prefix: &[&str]) -> Result<()> {
        let mut args: Vec<String> = prefix.iter().map(|arg| (*arg).to_string()).collect();
        args.push("--state-dir".into());
        args.push(self.state.display().to_string());
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let output = self.cmd(&refs).output()?;
        if !output.status.success() {
            bail!(
                "{} failed: {}{}",
                prefix.join(" "),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    /// Clean unmount (flushes, exits the daemon).
    pub fn unmount(&mut self) -> Result<()> {
        let _ = Command::new("fusermount3")
            .args(["-u"])
            .arg(&self.mnt)
            .status();
        if let Some(mut child) = self.child.take() {
            let deadline = Instant::now() + client_timeout();
            while Instant::now() < deadline {
                if child.try_wait()?.is_some() {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            child.kill().ok();
            bail!(
                "{} daemon did not exit after unmount within {:?}: {}",
                self.name,
                client_timeout(),
                self.tail_log()
            );
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

    /// Current resident set size of the daemon process, read from
    /// `/proc/<pid>/status` (`VmRSS`). Used by `big-file-write` to prove
    /// RSS stays flat regardless of the size of the file being written
    /// (plan 05a's exit criterion).
    pub fn rss_bytes(&self) -> Result<u64> {
        let pid = self.child.as_ref().context("not mounted")?.id();
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
            .with_context(|| format!("reading /proc/{pid}/status"))?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse()
                    .with_context(|| format!("parsing VmRSS line {line:?}"))?;
                return Ok(kb * 1024);
            }
        }
        bail!("no VmRSS line in /proc/{pid}/status")
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

    pub fn set_write_mode(&self, mode: &str) -> Result<()> {
        use std::io::{BufRead, BufReader, Write};
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)?;
        stream.set_read_timeout(Some(Duration::from_secs(60)))?;
        writeln!(stream, "{{\"cmd\":\"set_write_mode\",\"mode\":\"{mode}\"}}")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        let response: serde_json::Value = serde_json::from_str(&line)?;
        anyhow::ensure!(
            response["resp"] == "ok",
            "write-mode switch failed: {response}"
        );
        Ok(())
    }

    pub fn set_quota(&self, max_bytes: Option<u64>) -> Result<()> {
        use std::io::{BufRead, BufReader, Write};
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)?;
        stream.set_read_timeout(Some(Duration::from_secs(60)))?;
        let body = match max_bytes {
            Some(n) => format!(r#"{{"cmd":"set_quota","max_bytes":{n}}}"#),
            None => r#"{"cmd":"set_quota","max_bytes":null}"#.to_string(),
        };
        writeln!(stream, "{body}")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        let response: serde_json::Value = serde_json::from_str(&line)?;
        anyhow::ensure!(response["resp"] == "ok", "set_quota failed: {response}");
        Ok(())
    }

    pub fn get_quota(&self) -> Result<(Option<u64>, u64)> {
        use std::io::{BufRead, BufReader, Write};
        let sock = self.state.join("control.sock");
        let mut stream = std::os::unix::net::UnixStream::connect(&sock)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.write_all(b"{\"cmd\":\"get_quota\"}\n")?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        let response: serde_json::Value = serde_json::from_str(&line)?;
        anyhow::ensure!(
            response["resp"] == "quota",
            "get_quota failed: {response}"
        );
        let max = response["max_bytes"].as_u64();
        let used = response["used_bytes"].as_u64().unwrap_or(0);
        Ok((max, used))
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
                let deadline = Instant::now() + client_timeout();
                while Instant::now() < deadline {
                    if child.try_wait()?.is_some() {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                child.kill().ok();
                bail!(
                    "{} daemon did not exit after leave within {:?}",
                    self.name,
                    client_timeout()
                );
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
