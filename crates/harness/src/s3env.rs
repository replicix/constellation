//! Test environment: an S3 server plus toxiproxy. Constellation clients
//! talk to S3 *through* the proxy, so every scenario can inject faults on
//! the S3 path. Two interchangeable backends ([`S3Backend`]), selected
//! process-wide with `--s3-backend` / `CONSTELLATION_HARNESS_S3_BACKEND`
//! (see [`select_backend`]) and identical as seen through [`S3Env`]:
//!
//! - `docker` (default): floci + toxiproxy containers on a private docker
//!   network.
//!
//!   client (host) -> 127.0.0.1:<toxi-port> -> toxiproxy -> floci:4566
//!
//! - `process`: a native `versitygw` (posix backend on a temp dir) and a
//!   native `toxiproxy-server`, both on 127.0.0.1 with free ports, for hosts
//!   without docker (macOS/Windows CI). Binaries come from
//!   `CONSTELLATION_VERSITYGW_BIN` / `CONSTELLATION_TOXIPROXY_BIN`, else
//!   `PATH`, else `~/.local/bin`; `tests/ci/install-native-s3.sh` installs
//!   the pinned versions.
//!
//!   client (host) -> 127.0.0.1:<proxy-port> -> toxiproxy -> versitygw
//!
//! versitygw requires SigV4 where floci accepts anonymous requests; the
//! harness's own raw requests go through [`crate::s3auth`] for that.

use crate::docker::{Container, Network};
use crate::reqlog::CountingProxy;
use crate::toxiproxy::{Proxy, Toxiproxy};
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

pub const FLOCI_IMAGE: &str = "floci/floci:1.7.0-compat";
pub const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy:2.12.0";
pub const BUCKET: &str = "constellation-harness";

/// Prefix for the docker container and network names this environment
/// owns, from `CONSTELLATION_HARNESS_DOCKER_PREFIX` (default
/// `constellation-harness`).
///
/// Startup force-removes leftovers under its own prefix, because a
/// crashed run holds the network open. That makes two harness processes
/// on one host mutually destructive: the second one's cleanup deletes the
/// first one's live S3 out from under it, and the first one then fails
/// with connection-refused somewhere unrelated. Giving each run a
/// distinct prefix is the way to run two at once (e.g. a long matrix in
/// one terminal and a single scenario in another).
fn docker_prefix() -> String {
    std::env::var("CONSTELLATION_HARNESS_DOCKER_PREFIX")
        .unwrap_or_else(|_| "constellation-harness".to_string())
}

/// Host-wide guard for one docker prefix, so two harness processes
/// cannot quietly destroy each other's environment.
///
/// Startup force-removes every container under its prefix (a crashed run
/// holds the network open, so it has to), which is exactly what makes a
/// second concurrent run fatal to the first: its cleanup deletes the
/// live S3 the first one is using, and the first then fails somewhere
/// unrelated with a connection error, a "name is already in use"
/// conflict, or a missing network. Rather than leave that to whoever
/// remembers, take an advisory lock on the prefix and refuse up front.
/// The lock file is never removed; only the `flock` matters, and it is
/// released with the fd (including on a crash or a kill).
struct PrefixLock {
    _file: std::fs::File,
}

impl PrefixLock {
    fn acquire(prefix: &str) -> Result<PrefixLock> {
        use std::os::fd::AsRawFd;
        let path = std::env::temp_dir().join(format!(".{prefix}.lock"));
        // A file another user left (a `sudo` run of the same prefix) may
        // not be opened with `O_CREAT` in a sticky world-writable /tmp
        // (`fs.protected_regular`, on by default on Fedora) nor for
        // writing (it is 0644): `flock` needs neither, so open it
        // read-only then.
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .or_else(|e| match e.kind() {
                std::io::ErrorKind::PermissionDenied => std::fs::File::open(&path),
                _ => Err(e),
            })
            .with_context(|| format!("opening the harness lock file {}", path.display()))?;
        // SAFETY: a plain `flock(2)` on a file descriptor we own.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                anyhow::bail!(
                    "another harness run is already using the docker prefix `{prefix}` \
                     (lock: {}). Harness runs share their containers, network and bucket, \
                     so a second run would tear the first one's environment down. Wait for \
                     it to finish, or give this run its own environment with \
                     CONSTELLATION_HARNESS_DOCKER_PREFIX=<other-name>.",
                    path.display()
                );
            }
            return Err(
                anyhow::Error::new(err).context(format!("locking the harness prefix {prefix}"))
            );
        }
        Ok(PrefixLock { _file: file })
    }
}

/// Which S3 server + toxiproxy the harness runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum S3Backend {
    /// floci + toxiproxy containers (needs docker). The default.
    Docker,
    /// Native versitygw + toxiproxy-server processes (no docker).
    Process,
}

impl S3Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            S3Backend::Docker => "docker",
            S3Backend::Process => "process",
        }
    }

    pub fn parse(s: &str) -> Result<S3Backend> {
        match s.trim() {
            "docker" => Ok(S3Backend::Docker),
            "process" => Ok(S3Backend::Process),
            other => bail!("unknown S3 backend {other:?} (want `docker` or `process`)"),
        }
    }

    /// Whether the harness's raw requests must carry a SigV4 signature.
    pub fn needs_auth(self) -> bool {
        self == S3Backend::Process
    }
}

impl std::fmt::Display for S3Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Environment alternative to `--s3-backend`.
pub const BACKEND_ENV: &str = "CONSTELLATION_HARNESS_S3_BACKEND";

/// 0 = not selected yet (fall back to [`BACKEND_ENV`], then docker).
static SELECTED: AtomicU8 = AtomicU8::new(0);

/// Select the backend every later [`S3Env::start`] uses. `flag` is the
/// `--s3-backend` value; without it [`BACKEND_ENV`] is consulted (an invalid
/// value is an error, not a silent fallback), then `docker`. Called once
/// from `main` before any scenario runs.
pub fn select_backend(flag: Option<S3Backend>) -> Result<S3Backend> {
    let b = match flag {
        Some(b) => b,
        None => match std::env::var(BACKEND_ENV) {
            Ok(v) if !v.trim().is_empty() => {
                S3Backend::parse(&v).with_context(|| format!("{BACKEND_ENV}={v:?}"))?
            }
            _ => S3Backend::Docker,
        },
    };
    set_backend(b);
    Ok(b)
}

/// Set the process-wide backend (last call wins; tests and `main` only).
pub fn set_backend(b: S3Backend) {
    SELECTED.store(
        match b {
            S3Backend::Docker => 1,
            S3Backend::Process => 2,
        },
        Ordering::SeqCst,
    );
}

/// The process-wide backend: what [`select_backend`] chose, else the
/// environment variable (lenient here: an invalid value is `select_backend`'s
/// job to reject), else docker.
pub fn backend() -> S3Backend {
    match SELECTED.load(Ordering::SeqCst) {
        1 => S3Backend::Docker,
        2 => S3Backend::Process,
        _ => std::env::var(BACKEND_ENV)
            .ok()
            .and_then(|v| S3Backend::parse(&v).ok())
            .unwrap_or(S3Backend::Docker),
    }
}

pub struct S3Env {
    pub toxiproxy: Toxiproxy,
    /// What the `s3` toxiproxy proxy listens on and forwards to, as
    /// toxiproxy itself sees them (container names inside docker, loopback
    /// ports for the process backend).
    proxy_listen: String,
    proxy_upstream: String,
    /// S3 endpoint (through the proxy) for constellation clients.
    pub endpoint: String,
    /// S3 endpoint bypassing the proxy (for harness-side checks).
    #[allow(dead_code)]
    pub direct_endpoint: String,
    backend: S3Backend,
    /// Owns the containers / processes; dropped last, tearing them down.
    _guard: Guard,
}

/// The backend-specific resources of an [`S3Env`]. Each variant's fields
/// drop in declaration order, which is the teardown order.
#[allow(dead_code)]
enum Guard {
    Docker(DockerGuard),
    Process(ProcessGuard),
}

// Drop order matters: containers -> network -> lock.
#[allow(dead_code)]
struct DockerGuard {
    _toxi: Container,
    _floci: Container,
    _net: Network,
    _lock: PrefixLock,
}

// Drop order matters: proxy -> S3 server -> temp dir.
#[allow(dead_code)]
struct ProcessGuard {
    _toxi: ChildGuard,
    _s3: ChildGuard,
    _dir: tempfile::TempDir,
}

impl S3Env {
    /// Start the environment of the process-wide [`backend`].
    pub fn start() -> Result<S3Env> {
        Self::start_with(backend())
    }

    pub fn start_with(backend: S3Backend) -> Result<S3Env> {
        match backend {
            S3Backend::Docker => Self::start_docker(),
            S3Backend::Process => Self::start_process(),
        }
    }

    pub fn backend(&self) -> S3Backend {
        self.backend
    }

    fn start_docker() -> Result<S3Env> {
        let prefix = docker_prefix();
        let lock = PrefixLock::acquire(&prefix)?;
        let floci_name = format!("{prefix}-floci");
        let toxi_name = format!("{prefix}-toxiproxy");
        // Leftovers from a crashed run would hold the network open.
        let _ = crate::docker::docker(&["rm", "-f", &floci_name, &toxi_name]);
        let net = Network::create(&prefix)?;
        let floci = Container::run(
            &floci_name,
            FLOCI_IMAGE,
            &[
                "--network",
                &net.name,
                "-p",
                "127.0.0.1::4566",
                "-e",
                "FLOCI_STORAGE_MODE=memory",
            ],
        )?;
        let toxi = Container::run(
            &toxi_name,
            TOXIPROXY_IMAGE,
            &[
                "--network",
                &net.name,
                "-p",
                "127.0.0.1::8474",
                "-p",
                "127.0.0.1::4567",
            ],
        )?;
        let toxi_api = toxi.host_port(8474)?;
        let s3_via_proxy = toxi.host_port(4567)?;
        let s3_direct = floci.host_port(4566)?;

        let toxiproxy = Toxiproxy::new(toxi_api);
        toxiproxy.wait_ready(30)?;

        let direct_endpoint = format!("http://127.0.0.1:{s3_direct}");
        wait_s3_ready(&direct_endpoint, 60)?;
        create_bucket(&direct_endpoint, BUCKET, false)?;

        Ok(S3Env {
            toxiproxy,
            proxy_listen: "0.0.0.0:4567".to_string(),
            proxy_upstream: format!("{floci_name}:4566"),
            endpoint: format!("http://127.0.0.1:{s3_via_proxy}"),
            direct_endpoint,
            backend: S3Backend::Docker,
            _guard: Guard::Docker(DockerGuard {
                _toxi: toxi,
                _floci: floci,
                _net: net,
                _lock: lock,
            }),
        })
    }

    fn start_process() -> Result<S3Env> {
        let versitygw = find_bin("CONSTELLATION_VERSITYGW_BIN", "versitygw")?;
        let toxiproxy_bin = find_bin("CONSTELLATION_TOXIPROXY_BIN", "toxiproxy-server")?;
        let dir = tempfile::Builder::new()
            .prefix("constellation-harness-s3-")
            .tempdir()
            .context("creating the process-backend temp dir")?;
        let data = dir.path().join("data");
        std::fs::create_dir(&data)?;
        let ports = free_ports(3)?;
        let (s3_port, api_port, proxy_port) = (ports[0], ports[1], ports[2]);

        let mut s3 = ChildGuard::spawn(
            "versitygw",
            Command::new(&versitygw)
                .args(["--access", crate::s3auth::ACCESS_KEY])
                .args(["--secret", crate::s3auth::SECRET_KEY])
                .args(["--region", crate::s3auth::REGION])
                .args(["--port", &format!("127.0.0.1:{s3_port}")])
                .arg("--quiet")
                .arg("posix")
                .arg(&data),
            &dir.path().join("versitygw.log"),
        )?;
        let mut toxi = ChildGuard::spawn(
            "toxiproxy-server",
            Command::new(&toxiproxy_bin)
                .args(["-host", "127.0.0.1"])
                .args(["-port", &api_port.to_string()]),
            &dir.path().join("toxiproxy.log"),
        )?;

        let toxiproxy = Toxiproxy::new(api_port);
        wait_for("toxiproxy API", 30, &mut toxi, || toxiproxy.is_ready())?;
        let direct_endpoint = format!("http://127.0.0.1:{s3_port}");
        wait_for("versitygw", 60, &mut s3, || {
            crate::s3auth::signed("GET", &format!("{direct_endpoint}/"))
                .call()
                .is_ok()
        })?;
        create_bucket(&direct_endpoint, BUCKET, true)?;

        Ok(S3Env {
            toxiproxy,
            proxy_listen: format!("127.0.0.1:{proxy_port}"),
            proxy_upstream: format!("127.0.0.1:{s3_port}"),
            endpoint: format!("http://127.0.0.1:{proxy_port}"),
            direct_endpoint,
            backend: S3Backend::Process,
            _guard: Guard::Process(ProcessGuard {
                _toxi: toxi,
                _s3: s3,
                _dir: dir,
            }),
        })
    }

    /// The S3 proxy all constellation clients go through.
    pub fn s3_proxy(&self) -> Result<Proxy<'_>> {
        self.toxiproxy
            .create_proxy("s3", &self.proxy_listen, &self.proxy_upstream)
    }

    /// The S3 proxy [`Self::s3_proxy`] created (for toxics on it later).
    pub fn existing_s3_proxy(&self) -> Proxy<'_> {
        self.toxiproxy.proxy("s3")
    }

    /// A request-counting relay chained *in front of* toxiproxy, for
    /// scenarios that assert on S3 request classes (plan 26). Clients
    /// given `counter.endpoint()` still go through every toxic:
    ///
    ///   client -> counter (host) -> toxiproxy -> floci
    pub fn counting_proxy(&self) -> Result<CountingProxy> {
        let upstream = self
            .endpoint
            .strip_prefix("http://")
            .context("S3 endpoint is not http://")?;
        CountingProxy::start(upstream)
    }
}

fn wait_s3_ready(endpoint: &str, timeout_s: u64) -> Result<()> {
    for _ in 0..timeout_s * 10 {
        if let Ok(resp) = ureq::get(&format!("{endpoint}/_floci/health")).call() {
            if resp.status() == 200 {
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    anyhow::bail!("floci not ready after {timeout_s}s")
}

/// Path-style CreateBucket. Anonymous against floci (which does not enforce
/// SigV4 by default); `sign` for versitygw, which always does.
fn create_bucket(endpoint: &str, bucket: &str, sign: bool) -> Result<()> {
    let url = format!("{endpoint}/{bucket}");
    let req = if sign {
        crate::s3auth::signed("PUT", &url)
    } else {
        ureq::put(&url)
    };
    match req.call() {
        Ok(_) => Ok(()),
        // 409 = already exists (rerun); anything else is fatal.
        Err(ureq::Error::Status(409, _)) => Ok(()),
        Err(e) => Err(e).context("creating harness bucket"),
    }
}

/// A native server process, killed and reaped on drop, logging to a file.
struct ChildGuard {
    name: &'static str,
    child: Child,
    log: PathBuf,
}

impl ChildGuard {
    fn spawn(name: &'static str, cmd: &mut Command, log: &Path) -> Result<ChildGuard> {
        let out = std::fs::File::create(log)?;
        let child = cmd
            .stdin(Stdio::null())
            .stdout(out.try_clone()?)
            .stderr(out)
            .spawn()
            .with_context(|| format!("starting {name}"))?;
        Ok(ChildGuard {
            name,
            child,
            log: log.to_path_buf(),
        })
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(20)..].join("\n")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // SIGTERM first so versitygw can close cleanly, SIGKILL if it lingers.
        // SAFETY: plain kill(2) on a child we own and have not reaped yet.
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Poll `ready` until it holds, failing early (with the process's log) if the
/// process exits first.
fn wait_for(
    what: &str,
    timeout_s: u64,
    proc: &mut ChildGuard,
    mut ready: impl FnMut() -> bool,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_s);
    loop {
        if ready() {
            return Ok(());
        }
        if let Some(status) = proc.child.try_wait()? {
            bail!(
                "{} exited during startup ({status}):\n{}",
                proc.name,
                proc.log_tail()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "{what} not ready after {timeout_s}s; {} log:\n{}",
                proc.name,
                proc.log_tail()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `n` distinct free loopback TCP ports. They are released before use, so a
/// port can in principle be taken in between; the readiness checks turn that
/// into a clear startup error rather than a wrong-server connection.
fn free_ports(n: usize) -> Result<Vec<u16>> {
    let listeners = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()
        .context("finding free ports")?;
    listeners
        .iter()
        .map(|l| Ok(l.local_addr()?.port()))
        .collect()
}

const INSTALL_HINT: &str = "install the pinned versions with tests/ci/install-native-s3.sh \
     (then put its target dir on PATH, or point the variable at the binary)";

/// A native helper binary: `$var`, else `PATH`, else `~/.local/bin` (the
/// install script's default target).
fn find_bin(var: &str, name: &str) -> Result<PathBuf> {
    if let Some(p) = std::env::var_os(var).filter(|p| !p.is_empty()) {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Ok(p);
        }
        bail!("{var}={} is not a file; {INSTALL_HINT}", p.display());
    }
    let mut dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    if let Some(home) = std::env::var_os("HOME") {
        dirs.push(Path::new(&home).join(".local/bin"));
    }
    for d in dirs {
        let p = d.join(name);
        if p.is_file() {
            return Ok(p);
        }
    }
    bail!(
        "{name} not found (set {var}, or put it on PATH): the `process` S3 backend needs \
         native versitygw and toxiproxy-server; {INSTALL_HINT}"
    )
}
