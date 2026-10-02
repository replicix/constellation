//! A constellation client node: its own state dir + mountpoint, driving
//! the real `constellation` binary. Supports clean unmount, hard kill
//! (SIGKILL, simulating a crash), and remount.

use anyhow::{bail, Context, Result};
use constellation_platform::mounts::UnmountMode;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A control call on the daemon of `state_dir` (see [`Client::control_call`]).
pub fn control_call_at(
    state_dir: &Path,
    method: &str,
    params: serde_json::Value,
    within: Duration,
) -> Result<serde_json::Value> {
    let sock = constellation_control::transport::locate_socket(state_dir).with_context(|| {
        format!(
            "no daemon has recorded a control socket in {}",
            state_dir.display()
        )
    })?;
    control_runtime().block_on(async {
        let call = async {
            let client = constellation_control::Client::connect_unix(&sock).await?;
            client.call_json(method, params).await
        };
        match tokio::time::timeout(within, call).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(anyhow::anyhow!("{method}: {}", e.message)),
            Err(_) => bail!(
                "{method}: no answer from {} within {within:?}",
                sock.display()
            ),
        }
    })
}

/// The runtime every control call of the (synchronous) harness runs on.
fn control_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("the harness's control runtime")
    })
}

/// How long `mount_view` polls for the mountpoint to appear, and `unmount`/
/// `leave` poll for the daemon to exit, before giving up. Both loops return
/// as soon as the condition is met, so a generous ceiling costs nothing for
/// the common case of small, fast fault-injection scenarios — it only
/// raises the bound for genuinely slow or hung cases. Full-corpus
/// perf-regression runs (120k+ files, GB-scale write-back drains) routinely
/// need well over the 10s this used to allow: draining that many pending
/// uploads, shipping the journal tail, and publishing a metadata commit
/// are all legitimately slow, not hung, and the old bound
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
    /// Plan 38 Z2a: the daemon runs with `io_uring_setup(2)` refused by a
    /// seccomp filter (`crate::sandbox`).
    deny_io_uring: bool,
    /// Plan 38 Z2a: the daemon's `RLIMIT_AS`.
    address_space: Option<u64>,
}

/// The `constellation` binary under test.
pub fn constellation_bin() -> PathBuf {
    bin()
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

/// [`Client::without_env`]'s marker value.
const UNSET: &str = "\u{0}unset";

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
            deny_io_uring: false,
            address_space: None,
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

    /// [`Self::with_env`] on a client already in place (takes effect on
    /// the next spawned command; a later value for the same key wins).
    pub fn set_env(&mut self, key: &str, value: &str) {
        self.env.push((key.to_string(), value.to_string()));
    }

    /// Undo [`Self::set_env`]: the next spawned command does not see
    /// `key` at all.
    pub fn unset_env(&mut self, key: &str) {
        self.env.retain(|(k, _)| k != key);
        self.env.push((key.to_string(), UNSET.to_string()));
    }

    /// Remove a variable the harness sets for every client by default
    /// (e.g. its short S3 retry budget), so the mount runs with the
    /// product default instead.
    pub fn without_env(self, key: &str) -> Self {
        self.with_env(key, UNSET)
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

    /// Mount with `io_uring_setup(2)` refused (`EPERM`) by a seccomp
    /// filter on the daemon, as a container runtime's default profile
    /// refuses it; the daemon mounts through `crate::sandbox`'s
    /// `fusermount3` relay, since the filter needs `no_new_privs`.
    pub fn with_io_uring_denied(mut self) -> Self {
        self.deny_io_uring = true;
        self
    }

    /// Mount with the daemon's address space limited to `bytes`
    /// (`RLIMIT_AS`); `None` lifts it again.
    pub fn set_address_space_limit(&mut self, bytes: Option<u64>) {
        self.address_space = bytes;
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
            if v == UNSET {
                c.env_remove(k);
            } else {
                c.env(k, v);
            }
        }
        c
    }

    pub fn fs_create(&self) -> Result<()> {
        // Plan 21: `fs create` now takes a mandatory name positional. The
        // harness always drives mounts through explicit `--state-dir`/
        // `--s3` (never a registered name), so this registry row is
        // never read back — a fixed throwaway name is fine; it just
        // avoids letting the shared per-user registry file grow one row
        // per scenario run.
        let mut args = vec![
            "fs",
            "create",
            "harness",
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

    /// Plan 30 §M10: `fs set epoch-slack` on this client's backend.
    pub fn fs_set_epoch_slack(&self, slack: u32) -> Result<()> {
        let slack = slack.to_string();
        let out = self
            .cmd(&[
                "fs",
                "set",
                "epoch-slack",
                "harness",
                &slack,
                "--s3",
                &self.backend,
            ])
            .output()?;
        if !out.status.success() {
            bail!(
                "fs set epoch-slack failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    /// Change the E2E passphrase via `fs passwd`, without touching any
    /// mounted node. The target positional is a throwaway — `--s3` selects
    /// the backend directly (as everywhere else in the harness).
    pub fn passwd(&self, old: &str, new: &str) -> Result<()> {
        let out = self
            .cmd(&["fs", "passwd", "harness", "--s3", &self.backend])
            .env("CONSTELLATION_PASSPHRASE", old)
            .env("CONSTELLATION_NEW_PASSPHRASE", new)
            .output()?;
        if !out.status.success() {
            bail!("fs passwd failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        Ok(())
    }

    pub fn assert_wrong_passphrase_rejected(&self) -> Result<()> {
        self.assert_passphrase_rejected("definitely-wrong")
    }

    /// A cold mount with `passphrase` must fail cleanly at keyring unlock.
    /// Used after `fs passwd` to prove the old passphrase no longer opens
    /// the filesystem.
    pub fn assert_passphrase_rejected(&self, passphrase: &str) -> Result<()> {
        let wrong_state = self.work.join(format!("rej-state-{passphrase}"));
        let wrong_mnt = self.work.join(format!("rej-mnt-{passphrase}"));
        std::fs::create_dir_all(&wrong_state)?;
        std::fs::create_dir_all(&wrong_mnt)?;
        let output = self
            .cmd(&[
                "mount",
                "/",
                wrong_mnt.to_str().unwrap(),
                "--s3",
                &self.backend,
                "--state-dir",
                wrong_state.to_str().unwrap(),
                "--foreground",
            ])
            .env("CONSTELLATION_PASSPHRASE", passphrase)
            .output()?;
        if output.status.success() {
            bail!("mount unexpectedly accepted passphrase {passphrase:?}");
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("unlocking E2E keyring") {
            bail!("passphrase rejection was not clean: {stderr}");
        }
        Ok(())
    }

    pub fn mount(&mut self) -> Result<()> {
        self.mount_view(None, &[])
    }

    /// [`Self::mount`] that must have its mountpoint up within `within`
    /// (instead of `CONSTELLATION_HARNESS_MOUNT_TIMEOUT_S`). A mount that
    /// exits first fails with `mount died at startup` and the log tail
    /// (and the client is unmounted again); one still running at the
    /// deadline is killed and fails with `did not appear within`.
    pub fn mount_within(&mut self, within: Duration) -> Result<()> {
        self.mount_view_within(None, &[], within)
    }

    /// The daemon's pid, if mounted.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }

    /// `constellation status / --state-dir <state>` as a child process:
    /// the CLI path (not the socket shortcut of [`Self::control_status`]),
    /// for scenarios that test the CLI's own bounds.
    pub fn status_cli(&self) -> Result<std::process::Output> {
        Ok(self
            .cmd(&["status", "/", "--state-dir", self.state.to_str().unwrap()])
            .output()?)
    }

    pub fn mount_view(&mut self, inner: Option<&str>, extra: &[&str]) -> Result<()> {
        self.mount_view_within(inner, extra, client_timeout())
    }

    fn mount_view_within(
        &mut self,
        inner: Option<&str>,
        extra: &[&str],
        within: Duration,
    ) -> Result<()> {
        if self.child.is_some() {
            bail!("{} already mounted", self.name);
        }
        // A previous incarnation's log is kept (`mount.log.<n>`): what a
        // daemon logged before a `kill -9` is the evidence of what it was
        // doing (a stalled FUSE request and its backtrace, EC2 campaign 7
        // B-2). `log_text` stays the current incarnation's.
        if self.log.metadata().is_ok_and(|m| m.len() > 0) {
            let n = self.log_files().len();
            let _ = std::fs::rename(&self.log, self.log.with_extension(format!("log.{n}")));
        }
        let logf = std::fs::File::create(&self.log)?;
        // Plan 21: `mount` now takes TARGET MOUNTPOINT as two positionals
        // (TARGET is a registry name, "name:/sub", or — as here, since
        // the harness always mounts ad-hoc via --state-dir/--s3 — a
        // literal inner path). The old single-positional "just the
        // mountpoint, root implied" shorthand is gone: root must be
        // spelled out as "/" explicitly.
        let mut args = vec![
            "mount".to_string(),
            inner.unwrap_or("/").to_string(),
            self.mnt.to_str().unwrap().to_string(),
            "--s3".to_string(),
            self.backend.clone(),
            "--state-dir".to_string(),
            self.state.to_str().unwrap().to_string(),
            // Keep direct process-lifetime control: `Client` owns the
            // child's pid, waits on it, signals it, etc. Daemonizing by
            // default (plan 21, step 5) would fork away from that pid.
            "--foreground".to_string(),
        ];
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
        let mut cmd = self.cmd(&arg_refs);
        self.sandbox(&mut cmd)?;
        let child = cmd
            .stdout(Stdio::from(logf.try_clone()?))
            .stderr(Stdio::from(logf))
            .spawn()
            .context("spawning mount")?;
        self.child = Some(child);
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if is_mountpoint(&self.mnt) {
                return Ok(());
            }
            if let Some(st) = self.child.as_mut().unwrap().try_wait()? {
                self.child = None;
                bail!(
                    "{} mount died at startup ({st}): {}",
                    self.name,
                    self.tail_log()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(mut child) = self.child.take() {
            child.kill().ok();
            let _ = child.wait();
        }
        bail!(
            "{} mount did not appear within {within:?}: {}",
            self.name,
            self.tail_log()
        )
    }

    /// Applies [`Self::with_io_uring_denied`] and
    /// [`Self::set_address_space_limit`] to the daemon about to be spawned.
    fn sandbox(&self, cmd: &mut Command) -> Result<()> {
        use std::os::unix::process::CommandExt;
        if self.deny_io_uring {
            for (k, v) in crate::sandbox::Broker::get()?.env() {
                cmd.env(k, v);
            }
        }
        let (deny, limit) = (self.deny_io_uring, self.address_space);
        if deny || limit.is_some() {
            // SAFETY: both hooks make only async-signal-safe syscalls.
            unsafe {
                cmd.pre_exec(move || {
                    if let Some(bytes) = limit {
                        crate::sandbox::limit_address_space(bytes)?;
                    }
                    if deny {
                        crate::sandbox::deny_io_uring_setup()?;
                    }
                    Ok(())
                });
            }
        }
        Ok(())
    }

    /// The daemon's exit status, once it exits by itself (no unmount is
    /// made): within `within`, or the daemon is killed and this fails.
    pub fn wait_exit(&mut self, within: Duration) -> Result<std::process::ExitStatus> {
        let mut child = self.child.take().context("not mounted")?;
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        child.kill().ok();
        let _ = child.wait();
        bail!(
            "{} daemon did not exit within {within:?}: {}",
            self.name,
            self.tail_log()
        )
    }

    /// The FUSE connection number of this client's mount
    /// (`/sys/fs/fuse/connections/<n>`).
    pub fn fuse_connection(&self) -> Option<u32> {
        fuse_connection_of(&self.mnt)
    }

    /// Detach the dead mount a daemon left behind (after an abort or a
    /// crash), bounded.
    pub fn detach_dead_mount(&self) {
        detach_bounded(&self.mnt, Duration::from_secs(10));
    }

    pub fn snapshot_create(&self, selector: &str) -> Result<()> {
        self.control_command(&["snapshot", "create", selector])
    }

    pub fn snapshot_delete(&self, selector: &str) -> Result<()> {
        self.control_command(&["snapshot", "delete", selector])
    }

    /// `snapshot hold` (or `release`) of a selector or id, no owner.
    pub fn snapshot_hold(&self, selector: &str, held: bool) -> Result<()> {
        self.control_command(&["snapshot", if held { "hold" } else { "release" }, selector])
    }

    pub fn clone_snapshot(&self, selector: &str, destination: &str) -> Result<()> {
        self.control_command(&["clone", selector, destination])
    }

    /// How many snapshots the live daemon reports, via the control
    /// socket (`constellation snapshot ls`) rather than opening the
    /// metadata store's own file/directory from a second process —
    /// `fjall` (unlike SQLite/WAL) enforces single-process access with a
    /// lock file, so a harness-side read of a still-mounted node's
    /// replica must go through the daemon, not around it.
    pub fn snapshot_count(&self) -> Result<usize> {
        let output = self
            .cmd(&[
                "snapshot",
                "ls",
                "/",
                // Plan 32 §0.4 made the default a table; `--json` is the
                // shape this parses.
                "--json",
                "--state-dir",
                self.state.to_str().unwrap(),
            ])
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "snapshot ls failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value =
            serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "parsing snapshot ls output: {}",
                    String::from_utf8_lossy(&output.stdout)
                )
            })?;
        Ok(value.as_array().map(Vec::len).unwrap_or(0))
    }

    pub fn replica_db(&self) -> PathBuf {
        self.state.join("meta.db")
    }

    pub fn gc_process(&self) -> Result<Child> {
        // "/" is a placeholder TARGET positional (plan 21): unregistered,
        // so `--s3`/`--state-dir` below are what actually get used.
        let args = vec![
            "gc",
            "run",
            "/",
            "--s3",
            &self.backend,
            "--state-dir",
            self.state.to_str().unwrap(),
        ];
        Ok(self
            .cmd(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?)
    }

    pub fn gc_run(&self) -> Result<std::process::Output> {
        Ok(self.gc_process()?.wait_with_output()?)
    }

    /// Run one GC round inside this client's daemon over its control
    /// socket (what `constellation gc run` does when a daemon holds the
    /// state dir), without spawning a process. A scenario that holds a
    /// file open on a *frozen* (SIGSTOP'd) mount must use this: forking
    /// duplicates that FUSE fd and exec's close-on-exec sends the frozen
    /// daemon a `flush` the child then waits on, uninterruptibly.
    pub fn gc_run_control(&self) -> Result<serde_json::Value> {
        let result = self
            .control_call(
                "gc.run",
                serde_json::json!({"verify_only": false}),
                Duration::from_secs(300),
            )
            .map_err(|e| anyhow::anyhow!("gc run failed: {e:#}; log:\n{}", self.tail_log_n(60)))?;
        Ok(result["report"].clone())
    }

    pub fn fsck(&self, repair: bool) -> Result<std::process::Output> {
        let mut args = vec![
            "fsck",
            "/",
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
        let _ = unmount(&self.mnt, UnmountMode::Normal);
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

    /// Unmount and return the daemon's exit status, which must arrive
    /// within `within` (the daemon is SIGKILLed and this fails otherwise).
    /// Unlike [`Self::unmount`], a daemon that exits non-zero is not an
    /// error here: the caller asserts on the status.
    pub fn unmount_exit(&mut self, within: Duration) -> Result<std::process::ExitStatus> {
        let _ = unmount(&self.mnt, UnmountMode::Normal);
        let mut child = self.child.take().context("not mounted")?;
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        child.kill().ok();
        let _ = child.wait();
        let _ = unmount(&self.mnt, UnmountMode::Lazy);
        bail!(
            "{} daemon did not exit within {within:?} of the unmount: {}",
            self.name,
            self.tail_log()
        )
    }

    /// Crash: SIGKILL the daemon, then clean up the dead mountpoint.
    pub fn kill9(&mut self) -> Result<()> {
        let mut child = self.child.take().context("not mounted")?;
        kill9(&mut child).context("SIGKILL")?;
        child.wait()?;
        // The kernel keeps a dead FUSE mount around; detach it.
        let _ = unmount(&self.mnt, UnmountMode::Lazy);
        Ok(())
    }

    /// [`Self::kill9`] that requires the process to be gone within
    /// `within`: how long it took. A `kill -9`ed daemon whose last thread
    /// is wedged in the kernel (EC2 campaign 6 B-1: inside a FUSE
    /// reverse-invalidation write, behind a request it can no longer
    /// answer) stays a zombie instead; that fails with the zombie's
    /// thread states and `wchan`s — after aborting its FUSE connection so
    /// the mount, the zombie and the lock are cleaned up all the same.
    pub fn kill9_within(&mut self, within: Duration) -> Result<Duration> {
        let mut child = self.child.take().context("not mounted")?;
        let pid = child.id();
        kill9(&mut child).context("SIGKILL")?;
        let t = Instant::now();
        let mut exited = false;
        while t.elapsed() < within {
            if child.try_wait()?.is_some() {
                exited = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let took = t.elapsed();
        let diagnosis = if exited {
            None
        } else {
            Some(zombie_diagnosis(pid))
        };
        if !exited {
            // Release it: abort the connection of its mount, which ends
            // the requests its wedged thread waits behind.
            if let Some(n) = fuse_connection_of(&self.mnt) {
                let _ = constellation_platform::native().mounts.abort_fuse(n);
            }
            let t2 = Instant::now();
            while t2.elapsed() < Duration::from_secs(10) && child.try_wait()?.is_none() {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        // The kernel keeps a dead FUSE mount around; detach it (bounded:
        // a detach of a wedged mount would itself hang).
        detach_bounded(&self.mnt, Duration::from_secs(10));
        match diagnosis {
            None => Ok(took),
            Some(d) => bail!(
                "{} (pid {pid}) did not exit within {within:?} of kill -9: {d}",
                self.name
            ),
        }
    }

    /// Freeze the daemon (SIGSTOP): the process stays alive with its
    /// mount and unshipped journal intact but stops renewing its lease —
    /// the "unreachable holder" case from DESIGN.md §4.
    pub fn pause(&self) -> Result<()> {
        let pid = self.pid().context("not mounted")?;
        constellation_platform::native()
            .process
            .suspend(pid)
            .with_context(|| format!("suspending {pid}"))
    }

    /// Thaw a paused daemon (SIGCONT).
    pub fn resume(&self) -> Result<()> {
        let pid = self.pid().context("not mounted")?;
        constellation_platform::native()
            .process
            .resume(pid)
            .with_context(|| format!("resuming {pid}"))
    }

    #[allow(dead_code)]
    pub fn is_mounted(&self) -> bool {
        self.child.is_some() && is_mountpoint(&self.mnt)
    }

    /// Current resident set size of the daemon process, read from
    /// `/proc/<pid>/status` (`VmRSS`). Used by `big-file-write` to prove
    /// RSS stays flat regardless of the size of the file being written
    /// (plan 07's exit criterion).
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

    /// The whole mount log (every mount of this client appends to it).
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Every incarnation's log, oldest first (`mount.log.1`, ... then
    /// `mount.log`, the current one).
    pub fn log_files(&self) -> Vec<PathBuf> {
        let Some(dir) = self.log.parent() else {
            return Vec::new();
        };
        let mut rotated: Vec<(usize, PathBuf)> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        let n = name.strip_prefix("mount.log.")?.parse::<usize>().ok()?;
                        Some((n, e.path()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        rotated.sort();
        let mut out: Vec<PathBuf> = rotated.into_iter().map(|(_, p)| p).collect();
        if self.log.exists() {
            out.push(self.log.clone());
        }
        out
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

    /// This client's state directory (plan 30 §M4: the poison scenario
    /// removes a pending chunk from `<state>/cache`).
    pub fn state_dir(&self) -> &Path {
        &self.state
    }

    /// Call one control method (plan 31 C5) on this client's daemon, found
    /// through its state dir's `control.path`, with raw JSON params; the
    /// method's result, or the daemon's refusal as the error (its message).
    pub fn control_call(
        &self,
        method: &str,
        params: serde_json::Value,
        within: Duration,
    ) -> Result<serde_json::Value> {
        control_call_at(&self.state, method, params, within)
    }

    /// [`Self::control_call`] with the default 60 s bound.
    pub fn control(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        self.control_call(method, params, Duration::from_secs(60))
    }

    /// `node.status`: the status report.
    pub fn control_status(&self) -> Result<serde_json::Value> {
        self.control_call(
            "node.status",
            serde_json::json!({}),
            Duration::from_secs(10),
        )
    }

    /// Trigger one prune pass via the control socket (plan 22).
    pub fn prune_run(&self, dry_run: bool) -> Result<serde_json::Value> {
        self.control_call(
            "prune.run",
            serde_json::json!({"path": null, "dry_run": dry_run}),
            Duration::from_secs(30),
        )
        .map_err(|e| anyhow::anyhow!("prune run failed: {e:#}; log:\n{}", self.tail_log_n(80)))
    }

    pub fn reintegrate(&self) -> Result<()> {
        self.control_call(
            "node.reintegrate",
            serde_json::json!({}),
            Duration::from_secs(30),
        )
        .map_err(|e| {
            anyhow::anyhow!("reintegration failed: {e:#}; log:\n{}", self.tail_log_n(80))
        })?;
        Ok(())
    }

    pub fn set_write_mode(&self, mode: &str) -> Result<()> {
        self.control("node.set_write_mode", serde_json::json!({"mode": mode}))
            .context("write-mode switch failed")?;
        Ok(())
    }

    pub fn set_quota(&self, max_bytes: Option<u64>) -> Result<()> {
        self.control("quota.set", serde_json::json!({"max_bytes": max_bytes}))
            .context("set_quota failed")?;
        Ok(())
    }

    pub fn get_quota(&self) -> Result<(Option<u64>, u64)> {
        let q = self
            .control_call("quota.get", serde_json::json!({}), Duration::from_secs(10))
            .context("get_quota failed")?;
        Ok((
            q["max_bytes"].as_u64(),
            q["used_bytes"].as_u64().unwrap_or(0),
        ))
    }

    /// Permanently leave the cluster (self), or admin-retire `node_id`.
    pub fn leave(&mut self, node_id: Option<u64>, force: bool) -> Result<()> {
        let mut args = vec![
            "leave".to_string(),
            "/".to_string(), // placeholder TARGET (plan 21); --state-dir below wins
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
            let _ = unmount(&self.mnt, UnmountMode::Normal);
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// What `/proc` says about a process that did not exit after `kill -9`:
/// its state and thread count, and every thread's state and `wchan`
/// (readable without root, unlike `stack`).
fn zombie_diagnosis(pid: u32) -> String {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let mut out = String::new();
    for key in ["State:", "Threads:", "SigPnd:", "ShdPnd:"] {
        if let Some(line) = status.lines().find(|l| l.starts_with(key)) {
            out.push_str(
                line.split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .as_str(),
            );
            out.push_str("; ");
        }
    }
    if out.is_empty() {
        return format!("/proc/{pid} is gone (reaped by someone else?)");
    }
    // Only `status`, `comm` and `wchan`: reading a task's `stat` can
    // block behind the very thing being diagnosed (`do_task_stat` takes
    // locks a thread stuck in the kernel may hold).
    if let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) {
        for task in tasks.flatten() {
            let tid = task.file_name().to_string_lossy().into_owned();
            let state = std::fs::read_to_string(task.path().join("status"))
                .ok()
                .and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with("State:"))
                        .map(|l| l.trim_start_matches("State:").trim().to_string())
                })
                .unwrap_or_else(|| "?".into());
            let comm = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
            let wchan = std::fs::read_to_string(task.path().join("wchan")).unwrap_or_default();
            out.push_str(&format!(
                "thread {tid} ({}): {state} in {}; ",
                comm.trim(),
                wchan.trim()
            ));
        }
    }
    out
}

/// The FUSE connection number of the mount at `mountpoint` (Linux:
/// `/sys/fs/fuse/connections/<n>`), from the host's mount table.
fn fuse_connection_of(mountpoint: &Path) -> Option<u32> {
    constellation_platform::native()
        .mounts
        .list()
        .ok()?
        .into_iter()
        .filter(|m| m.mountpoint == mountpoint)
        .find_map(|m| m.fuse_connection())
}

/// Unmount `mnt` through the host's mount service (Linux: the setuid
/// `fusermount3`, as the harness always ran it).
fn unmount(mnt: &Path, mode: UnmountMode) -> std::io::Result<()> {
    constellation_platform::native().mounts.unmount(mnt, mode)
}

/// A lazy detach of `mnt` that gives up after `bound` (a detach of a
/// wedged mount can itself hang). The helper is a child process, killed at
/// the bound: a detach left running (or a thread falling through to the
/// mount service's other fallbacks) could complete later and detach the
/// *next* mount at the same path.
fn detach_bounded(mnt: &Path, bound: Duration) {
    let Ok(mut fm) = Command::new("fusermount3")
        .args(["-u", "-z"])
        .arg(mnt)
        .spawn()
    else {
        return;
    };
    let t = Instant::now();
    while t.elapsed() < bound && matches!(fm.try_wait(), Ok(None)) {
        std::thread::sleep(Duration::from_millis(50));
    }
    if matches!(fm.try_wait(), Ok(None)) {
        let _ = fm.kill();
        let _ = fm.wait();
    }
}

/// SIGKILL the daemon, through the host's process service (a daemon that
/// has already exited is already dead: `Ok`, as `Child::kill` says).
fn kill9(child: &mut Child) -> std::io::Result<()> {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return Ok(());
    }
    constellation_platform::native().process.kill(child.id())
}

/// Whether `p` is a mountpoint whose filesystem answers, as `mountpoint
/// -q` has always said: it `stat(2)`s the path first, so a dead FUSE mount
/// (`ENOTCONN`) is not one, and a mount whose daemon has not answered
/// `FUSE_INIT` yet is waited for, not reported up early.
pub(crate) fn is_mountpoint(p: &Path) -> bool {
    std::fs::metadata(p).is_ok()
        && constellation_platform::native()
            .mounts
            .is_mountpoint(p)
            .unwrap_or(false)
}
