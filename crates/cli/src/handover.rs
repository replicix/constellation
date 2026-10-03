//! `constellation daemon --upgrade`: replace the running daemon's binary
//! while every view stays mounted (plan 31 C4b, §6.11).
//!
//! # The shape: `exec` in place, not a second process
//!
//! The daemon `exec`s the new binary with the connections as inherited
//! descriptors. The process — its pid, its `daemon.lock` (an `flock` on an
//! open file the new image inherits, never released), its `daemon.pid`,
//! the zombie reaper watching it, whatever supervises it (a shell, a
//! service manager, the harness's `Client`) — is the same before and
//! after, so nothing that locates or guards a daemon sees a gap: there is
//! no window in which `daemon.lock` is free for a `mount` to take and then
//! `abort_stale_mounts` on connections that are merely changing hands.
//! The control socket's *listener* is inherited too, so a `status` or a
//! `mount` connecting during the handover waits in its backlog and is
//! answered by the new image. (The alternative, a second process receiving
//! the descriptors over `SCM_RIGHTS`, has to move `daemon.lock` between
//! processes — `/proc/locks` would then name a dead pid as its holder,
//! which the takeover logic reads — and it loses the supervisor's pid.)
//!
//! # The sequence (old image, then new)
//!
//! 1. **Preflight** (nothing changed yet; refusals answer the request): a
//!    handover is not already running; the new binary runs and speaks this
//!    handover's version (`daemon --handover-abi`); an E2E filesystem's
//!    passphrase is in the environment (the new image unlocks the keyring
//!    again, with no terminal); no view holds a cluster lock and no
//!    blocking lock wait is in flight (neither survives a restart of the
//!    node: `View::handover_blockers`, `SessionControl::deferred_replies`).
//! 2. **Detach every session** (`SessionControl::detach`, whose module doc
//!    has the steps): the notification gate closes while the session still
//!    serves; the fuser workers stop before their next read (requests the
//!    kernel queues from now on wait for the new image); the view's pending
//!    writes are published (`sync_view`); the view's spec and handle table
//!    are exported. If any detach fails, the ones already detached are
//!    resumed in place on the same descriptors and the upgrade is refused:
//!    the daemon carries on as before.
//! 3. The request is answered ("handing over"), and the rest runs on a
//!    thread of its own:
//! 4. **Durability barrier**: each view is closed for the handover
//!    (`Engine::close_view_for_handover`: unregistered, its ephemeral clone
//!    and open-orphan claims left in place), then the node shuts down
//!    exactly as a clean unmount does — dirty chunks uploaded, the journal
//!    shipped, a metadata commit published, the lease released
//!    (`Engine::shutdown_for_handover`) — and the replica is synced to
//!    disk. A drain that cannot finish (S3 down) leaves its journal and
//!    pending rows on disk, which the new image ships, exactly as a
//!    remount's; nothing is lost, since the state dir is the same.
//! 5. **`exec`** the new binary, `constellation daemon --resume-from <fd>`,
//!    `<fd>` a memfd holding [`DaemonHandoff`] (JSON). The FUSE
//!    descriptors, the lock, the control listener and a descriptor of the
//!    *current* executable are inherited (their close-on-exec flag
//!    cleared). If the `exec` fails, the current executable is `exec`ed
//!    instead, with the same handoff: the rollback is itself a handover.
//! 6. **New image** ([`resume_main`]): every inherited descriptor goes back
//!    to close-on-exec; the node starts on the same state dir as any
//!    restart does (incarnation bump, registry, the lease re-acquired on
//!    the first write); if it cannot start and this is not already a
//!    rollback, it `exec`s the previous binary with the handoff. Then each
//!    view reopens with its handle table (`Engine::open_view_resumed`), and
//!    its session resumes on the inherited descriptor without `FUSE_INIT`
//!    (`FuseSession::resume`): the requests queued since step 2 are served
//!    now. The data of every open file is invalidated once (the gate
//!    dropped the engine's invalidations during the gap), the old mount
//!    records are replaced by new ones, and the control API is served from
//!    the inherited listener. `status` reports
//!    `handover.generation` one higher.
//!
//! What an application sees: a stall for the length of steps 2-6 (every
//! syscall on the mount blocks in the kernel's queue), never `ENOTCONN` or
//! `EIO`; its open descriptors, `flock`/POSIX locks under `--locks local`
//! (the kernel's), and its cwd inside the mount all carry on.
//!
//! # Refusals and limits
//!
//! - **Lock waits and cluster locks** refuse the upgrade (step 1, and again
//!   on the session thread once it stopped, for a wait that began in
//!   between). A blocking `F_SETLKW`/`flock` has read its request and must
//!   answer it on the same descriptor from this process; fuser 0.18 cannot
//!   interrupt it, and draining it could take forever. A granted cluster
//!   lock lives in this node's memory and its grant, neither of which
//!   survives the node's restart. Retry once they are released. (Under
//!   `--locks local` the kernel holds every lock: nothing to refuse.)
//! - **A pending write that cannot be published** refuses it (the
//!   `sync_view` of step 2 fails; the session resumes in place). Today
//!   that includes a file written, unlinked and still open: its `fsync`
//!   and `close` fail with `ENOENT` as well — an engine bug that predates
//!   the handover.
//! - **Invalidations** the engine produces during the gap are dropped
//!   (the gate); entries and attributes are TTL-bounded, and every open
//!   file's pages are invalidated on resume.
//! - **An E2E filesystem** needs `CONSTELLATION_PASSPHRASE` in the daemon's
//!   environment (the new image unlocks the keyring without a terminal).
//! - **After the `exec`, failures cannot be undone in place**: a node that
//!   does not start rolls back to the previous binary (once); a view that
//!   cannot be reopened ends its mount (its descriptor closes), as a crash
//!   would. `add_mount`/`remove_mount` answer [`crate::node_runtime::UPGRADING`]
//!   meanwhile (an attaching `mount` retries).
//! - The request (`node.handoff` with `HandoffTarget::Exec`, plan 31 C5)
//!   is honoured on the unix socket only (it executes a binary); the web
//!   UI's HTTP adapter refuses it (`constellation_control::web::HTTP_REFUSED`).

use crate::node_runtime::{MountId, NodeRuntime, SessionInfo};
use anyhow::{bail, Context, Result};
use constellation_engine::{EngineConfig, ViewHandoff};
use constellation_frontend_fuse::{FuseHandoff, FuseSession, MountOptions, NegotiatedInit};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, Write};
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The handoff's format: an old and a new binary must agree on it
/// (`daemon --handover-abi`). Bump it on any change to what crosses:
/// 2 — plan 32 M0c, the `.constellation` synthetic nodes carry the
/// directory's inode instead of its path. 3 — plan 39 §3.7, a view's
/// file handles are its own per-open numbers (`handles`), and the node's
/// discard error events cross (`errors`): a version-2 image could neither
/// parse this handoff nor serve the handles the kernel holds, so the ABI
/// probe refuses the mix up front. 4 — plan 37 K3a, a mount carries
/// `foreign` (made by someone else, `view.mount{PreopenedFd}`).
/// 5 — plan 38 Z3b, passthrough table + write-intent counts + passthrough
/// chunk hashes. 6 — plan 38 Z2c, `fuse_transport` may be `uring`, and an
/// absent `fuse_uring_queue_depth` means "chosen per mount" (8, or 32 for
/// a cluster-lock mount on `uring`) where a version-5 image always sent
/// the depth it used: across the mix a version-5 image would refuse the
/// policy and a version-6 one would pin every ring mount to 8.
pub const HANDOVER_VERSION: u32 = 6;

/// `daemon.lock`'s descriptor (held for the process's life; handed on).
static LOCK_FD: AtomicI32 = AtomicI32::new(-1);
/// The zombie reaper this process started (a child to wait for).
static REAPER_PID: AtomicU32 = AtomicU32::new(0);

pub fn set_lock_fd(fd: RawFd) {
    LOCK_FD.store(fd, Ordering::SeqCst);
}

pub fn set_reaper_pid(pid: u32) {
    REAPER_PID.store(pid, Ordering::SeqCst);
}

/// A daemon's handover bookkeeping.
pub struct HandoverState {
    generation: u32,
    upgrading: AtomicBool,
    last_error: Mutex<Option<String>>,
    /// A clone of the control socket's listener, to hand on.
    pub(crate) control: Mutex<Option<std::os::unix::net::UnixListener>>,
    /// The executable this image runs, as it was when it started (a
    /// package upgrade replaces the file: this path then names the new
    /// binary, `/proc/self/exe` the deleted old one).
    exe: Option<PathBuf>,
    /// Plan 37 §8: a handoff to another process (`crate::handoff_socket`).
    pub(crate) socket: crate::handoff_socket::SenderState,
}

impl HandoverState {
    pub fn new(generation: u32) -> Self {
        Self {
            generation,
            upgrading: AtomicBool::new(false),
            last_error: Mutex::new(None),
            control: Mutex::new(None),
            exe: std::env::current_exe().ok(),
            socket: Default::default(),
        }
    }

    /// This image's place in its chain of handovers.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Mark a handover under way; `false` when one already is.
    pub(crate) fn begin(&self) -> bool {
        !self.upgrading.swap(true, Ordering::SeqCst)
    }

    /// The handover under way failed (and nothing changed): recorded for
    /// `status`, and no longer under way.
    pub(crate) fn fail_with(&self, why: String) -> String {
        self.fail(why)
    }

    /// The handover under way ended without handing anything over
    /// (`why`, if it is worth reporting).
    pub(crate) fn finish(&self, why: Option<String>) {
        if let Some(why) = why {
            *self.last_error.lock().unwrap() = Some(why);
        }
        self.upgrading.store(false, Ordering::SeqCst);
    }

    /// A handover is under way (the daemon must not exit when its session
    /// threads end).
    pub fn in_progress(&self) -> bool {
        self.upgrading.load(Ordering::SeqCst)
    }

    pub fn status(&self) -> constellation_control::proto::types::HandoverStatus {
        constellation_control::proto::types::HandoverStatus {
            generation: self.generation,
            pid: std::process::id(),
            upgrading: self.in_progress(),
            last_error: self.last_error.lock().unwrap().clone(),
        }
    }

    fn fail(&self, why: String) -> String {
        *self.last_error.lock().unwrap() = Some(why.clone());
        self.upgrading.store(false, Ordering::SeqCst);
        why
    }
}

/// The node's settings, as the next image starts it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHandoff {
    backend: String,
    state_dir: Option<PathBuf>,
    cache_size: u64,
    staging_budget: Option<u64>,
    fsync_s3: bool,
    /// Plan 39: `--fsync-timeout` in ms; `0` an explicit `hard`, absent
    /// no flag (`CONSTELLATION_FSYNC_TIMEOUT`, else wait until durable).
    #[serde(default)]
    fsync_timeout_ms: Option<u64>,
    cto_strict: bool,
    locks: Option<bool>,
    write_mode: String,
    read_only_member: bool,
    atime: String,
    /// `"admit"`/`"always"`, or absent in an older image's handoff (the
    /// resumed node then falls back to `CONSTELLATION_CACHE_VERIFY` and
    /// the default, as a fresh mount would).
    #[serde(default)]
    cache_verify: Option<String>,
    /// Plan 38 Z1b: `--fuse-transport` (`auto`/`uring`/`dev-fuse`). The
    /// *resumed* mounts are `/dev/fuse` by construction (a ring session
    /// cannot be handed over at all, §3(e)); this is for the views added
    /// to the new image afterwards, which would otherwise lose a flag the
    /// environment did not also set.
    #[serde(default)]
    fuse_transport: Option<String>,
    #[serde(default)]
    fuse_uring_queue_depth: Option<usize>,
    pin_target: Option<constellation_engine::e2e_pin::PinTarget>,
    web_ui: u16,
}

impl NodeHandoff {
    pub fn of(
        cfg: &EngineConfig,
        web_ui: u16,
        fuse_transport: constellation_frontend_fuse::TransportConfig,
    ) -> Self {
        Self {
            backend: cfg.backend.clone(),
            state_dir: cfg.state_dir.clone(),
            cache_size: cfg.cache_size,
            staging_budget: cfg.staging_budget,
            fsync_s3: cfg.fsync_s3,
            fsync_timeout_ms: cfg
                .fsync_timeout
                .map(|t| t.map_or(0, |t| (t.as_millis() as u64).max(1))),
            cto_strict: cfg.cto_strict,
            locks: cfg.locks,
            write_mode: cfg.initial_write_mode.as_str().to_string(),
            read_only_member: cfg.read_only_member,
            atime: cfg.atime_mode.as_str().to_string(),
            cache_verify: cfg.cache_verify.map(|v| v.as_str().to_string()),
            fuse_transport: Some(fuse_transport.policy.as_str().to_string()),
            fuse_uring_queue_depth: fuse_transport.uring_queue_depth,
            pin_target: cfg.pin_target.clone(),
            web_ui,
        }
    }

    /// What the previous image's `--fuse-transport` /
    /// `--fuse-uring-queue-depth` were, as flags for
    /// `TransportConfig::resolve` to apply the environment over (the new
    /// image resolves them again, as a fresh `mount` does). An
    /// unparseable stored value is an error: it can only have come from a
    /// `--fuse-transport` this binary already accepted.
    #[allow(clippy::type_complexity)]
    fn fuse_transport_flags(
        &self,
    ) -> Result<(
        Option<constellation_frontend_fuse::TransportPolicy>,
        Option<usize>,
    )> {
        let policy = match &self.fuse_transport {
            Some(raw) => Some(
                constellation_frontend_fuse::TransportPolicy::parse(raw)
                    .with_context(|| format!("the handoff's fuse transport {raw:?}"))?,
            ),
            None => None,
        };
        Ok((policy, self.fuse_uring_queue_depth))
    }

    /// [`Self::fuse_transport_flags`] with the environment applied over it.
    fn fuse_transport(&self) -> Result<constellation_frontend_fuse::TransportConfig> {
        let (policy, depth) = self.fuse_transport_flags()?;
        constellation_frontend_fuse::TransportConfig::resolve(
            policy,
            depth,
            crate::node_runtime::profile_transport()?,
        )
        .map_err(anyhow::Error::msg)
    }

    fn engine_config(&self) -> Result<EngineConfig> {
        let mut cfg = EngineConfig::new(self.backend.clone());
        cfg.state_dir = self.state_dir.clone();
        cfg.cache_size = self.cache_size;
        cfg.staging_budget = self.staging_budget;
        cfg.fsync_s3 = self.fsync_s3;
        cfg.fsync_timeout = self
            .fsync_timeout_ms
            .map(|ms| (ms > 0).then(|| std::time::Duration::from_millis(ms)));
        cfg.cto_strict = self.cto_strict;
        cfg.locks = self.locks;
        cfg.initial_write_mode = self.write_mode.parse().map_err(anyhow::Error::msg)?;
        cfg.read_only_member = self.read_only_member;
        cfg.atime_mode = constellation_engine::atime::AtimeMode::parse(&self.atime)
            .context("the handoff's atime mode")?;
        cfg.cache_verify = match &self.cache_verify {
            Some(raw) => Some(
                constellation_fs_core::cache::CacheVerify::parse(raw)
                    .context("the handoff's cache-verify mode")?,
            ),
            None => None,
        };
        cfg.pin_target = self.pin_target.clone();
        // No terminal: an E2E filesystem's passphrase comes from the
        // environment (the preflight made sure it is there).
        cfg.passphrase = constellation_engine::PassphraseSource::Ask(Box::new(|| {
            std::env::var("CONSTELLATION_PASSPHRASE")
                .map(zeroize::Zeroizing::new)
                .context("CONSTELLATION_PASSPHRASE is not set")
        }));
        cfg.version = env!("CONSTELLATION_VERSION").to_string();
        Ok(cfg)
    }
}

/// One view, handed over.
#[derive(Debug, Serialize, Deserialize)]
pub struct MountHandoff {
    /// Its mount record in the previous image (replaced by the new one's).
    pub(crate) old_id: u64,
    pub(crate) subtree: String,
    pub(crate) mountpoint: PathBuf,
    pub(crate) fs_name: String,
    pub(crate) allow_other: bool,
    pub(crate) read_only: bool,
    pub(crate) fuse_threads: usize,
    pub(crate) fuse_fd: RawFd,
    pub(crate) init: NegotiatedInit,
    /// Somebody else made the mount (`view.mount{PreopenedFd}`): the next
    /// image ends the session rather than unmounting `mountpoint`, which
    /// is only its name (`FuseHandoff::foreign`).
    pub(crate) foreign: bool,
    /// Plan 38 Z3b: the session's passthrough table, its backing ids
    /// still registered (`FuseHandoff::passthrough`).
    pub(crate) passthrough: constellation_frontend_fuse::PassthroughHandoff,
    pub(crate) view: ViewHandoff,
}

/// Everything the next image receives (JSON in a memfd).
#[derive(Debug, Serialize, Deserialize)]
pub struct DaemonHandoff {
    version: u32,
    /// The generation the receiving image serves as.
    generation: u32,
    /// The binary that wrote it (for the log).
    from_version: String,
    lock_fd: Option<RawFd>,
    control_fd: Option<RawFd>,
    reaper_pid: Option<u32>,
    /// The previous image's executable, to roll back to.
    rollback_exe_fd: Option<RawFd>,
    /// This `exec` is the rollback: never roll back again.
    is_rollback: bool,
    node: NodeHandoff,
    mounts: Vec<MountHandoff>,
}

impl DaemonHandoff {
    fn fds(&self) -> Vec<RawFd> {
        let mut fds: Vec<RawFd> = [self.lock_fd, self.control_fd, self.rollback_exe_fd]
            .into_iter()
            .flatten()
            .collect();
        fds.extend(self.mounts.iter().map(|m| m.fuse_fd));
        fds
    }
}

/// What `daemon --handover-abi` prints: what the preflight checks.
pub fn handover_abi() -> String {
    serde_json::json!({
        "handover": HANDOVER_VERSION,
        "version": env!("CONSTELLATION_VERSION"),
    })
    .to_string()
}

fn set_cloexec(fd: RawFd, on: bool) -> std::io::Result<()> {
    // SAFETY: F_GETFD/F_SETFD on a descriptor number; a bad one is EBADF.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let flags = if on {
            flags | libc::FD_CLOEXEC
        } else {
            flags & !libc::FD_CLOEXEC
        };
        if libc::fcntl(fd, libc::F_SETFD, flags) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// A memfd (inherited across `exec`) holding `handoff`.
fn write_handoff(handoff: &DaemonHandoff) -> std::io::Result<RawFd> {
    let fd = memfd()?;
    // SAFETY: a descriptor we just created and own.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.write_all(&serde_json::to_vec(handoff).map_err(std::io::Error::other)?)?;
    file.flush()?;
    Ok(file.into_raw_fd())
}

#[cfg(target_os = "linux")]
fn memfd() -> std::io::Result<RawFd> {
    // SAFETY: a NUL-terminated name and no flags (no close-on-exec: the
    // next image reads it).
    let fd = unsafe { libc::memfd_create(c"constellation-handoff".as_ptr(), 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

/// In-place upgrade is Linux-only (`upgrade` refuses elsewhere); other
/// hosts only type-check this module.
#[cfg(not(target_os = "linux"))]
fn memfd() -> std::io::Result<RawFd> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "in-place upgrade is Linux-only",
    ))
}

fn read_handoff(fd: RawFd) -> Result<DaemonHandoff> {
    // SAFETY: the descriptor the previous image passed us, ours now.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.rewind().context("the handoff memfd")?;
    let mut text = String::new();
    file.read_to_string(&mut text)
        .context("reading the handoff")?;
    let handoff: DaemonHandoff = serde_json::from_str(&text).context("parsing the handoff")?;
    if handoff.version != HANDOVER_VERSION {
        bail!(
            "handoff version {} (this binary speaks {HANDOVER_VERSION})",
            handoff.version
        );
    }
    Ok(handoff)
}

/// `exec` `binary` (argv[0] `constellation`) with the handoff; returns
/// only on failure.
fn exec_with(binary: &Path, handoff: &DaemonHandoff) -> std::io::Error {
    for fd in handoff.fds() {
        if let Err(e) = set_cloexec(fd, false) {
            return e;
        }
    }
    let memfd = match write_handoff(handoff) {
        Ok(fd) => fd,
        Err(e) => return e,
    };
    let err = std::process::Command::new(binary)
        .arg0("constellation")
        .args(["daemon", "--resume-from", &memfd.to_string()])
        .exec();
    // SAFETY: ours; the exec did not happen.
    drop(unsafe { OwnedFd::from_raw_fd(memfd) });
    err
}

/// The previous image's executable, if the handoff carries it: `exec` it
/// with the handoff marked as a rollback. Never returns; exits when even
/// that fails (the mounts then end as with a crash).
fn roll_back(mut handoff: DaemonHandoff, why: &str) -> ! {
    match handoff.rollback_exe_fd.filter(|_| !handoff.is_rollback) {
        Some(fd) => {
            tracing::error!(why, "handover failed; rolling back to the previous binary");
            handoff.is_rollback = true;
            let exe = PathBuf::from(format!("/proc/self/fd/{fd}"));
            let err = exec_with(&exe, &handoff);
            tracing::error!(error = %err, "the rollback exec failed; exiting (the mounts end)");
        }
        None => tracing::error!(why, "handover failed and cannot roll back; exiting"),
    }
    std::process::exit(1)
}

/// Run `binary --handover-abi` (bounded) and check it speaks this
/// handover.
fn preflight(binary: &Path) -> Result<(), String> {
    let mut child = std::process::Command::new(binary)
        .args(["daemon", "--handover-abi"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("{} does not run: {e}", binary.display()))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{} --handover-abi did not answer",
                    binary.display()
                ));
            }
        }
    }
    let mut out = String::new();
    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_string(&mut out);
    }
    let abi: serde_json::Value = serde_json::from_str(out.trim()).map_err(|_| {
        format!(
            "{} does not support in-place upgrades (no --handover-abi)",
            binary.display()
        )
    })?;
    match abi.get("handover").and_then(|v| v.as_u64()) {
        Some(v) if v == u64::from(HANDOVER_VERSION) => Ok(()),
        other => Err(format!(
            "{} speaks handover version {other:?}, this daemon {HANDOVER_VERSION}",
            binary.display()
        )),
    }
}

/// One mounted view, as the upgrade handles it.
pub(crate) struct Target {
    pub(crate) id: MountId,
    pub(crate) info: SessionInfoParts,
    pub(crate) control: constellation_frontend_fuse::SessionControl,
    pub(crate) view: Arc<constellation_engine::View>,
}

#[derive(Clone)]
pub(crate) struct SessionInfoParts {
    pub(crate) subtree: String,
    pub(crate) mountpoint: PathBuf,
    pub(crate) fs_name: String,
    pub(crate) allow_other: bool,
    pub(crate) read_only: bool,
    pub(crate) fuse_threads: usize,
    pub(crate) sink: constellation_frontend_fuse::FuseNotifySink,
    pub(crate) caps: constellation_vfs::FrontendCaps,
    pub(crate) qos: constellation_engine::ViewQos,
    pub(crate) confine_links: bool,
}

impl SessionInfoParts {
    fn info(&self) -> SessionInfo {
        SessionInfo {
            subtree: self.subtree.clone(),
            mountpoint: self.mountpoint.clone(),
            fs_name: self.fs_name.clone(),
            allow_other: self.allow_other,
            read_only: self.read_only,
            fuse_threads: self.fuse_threads,
            sink: self.sink.clone(),
            caps: self.caps.clone(),
            qos: self.qos,
            confine_links: self.confine_links,
        }
    }

    fn options(&self, cfg: constellation_frontend_fuse::TransportConfig) -> MountOptions {
        // These options only ever resume a detached session
        // (`resume_in_place`).
        handover_options(
            self.fs_name.clone(),
            self.fuse_threads,
            self.allow_other,
            self.read_only,
            cfg,
        )
    }
}

pub(crate) type Detached = (
    Target,
    constellation_frontend_fuse::SessionHandoff<Result<ViewHandoff>>,
);

/// `node.handoff` (`HandoffTarget::Exec`): steps 1-3 of the module doc here; 4-5 on a thread
/// of their own once this answers.
pub fn upgrade(node: &Arc<NodeRuntime>, binary: Option<&Path>) -> Result<String, String> {
    if !cfg!(target_os = "linux") {
        return Err("in-place upgrade is Linux-only".into());
    }
    let state = &node.handover;
    if state.upgrading.swap(true, Ordering::SeqCst) {
        return Err("a handover is already under way".into());
    }
    let binary = match binary.map(Path::to_path_buf).or_else(|| state.exe.clone()) {
        Some(binary) => binary,
        None => return Err(state.fail("the daemon's executable is unknown".into())),
    };
    let detached = match prepare(node, &binary) {
        Ok(detached) => detached,
        Err(why) => return Err(state.fail(why)),
    };
    let views = detached.len();
    let next = state.generation + 1;
    let message = format!(
        "handing {views} view(s) over to {} (generation {next}, pid {})",
        binary.display(),
        std::process::id()
    );
    let handing = node.clone();
    let spawned = std::thread::Builder::new()
        .name("handover".into())
        .spawn(move || hand_over(handing, binary, detached));
    if let Err(e) = spawned {
        // The detached sessions went with the closure: they end (and the
        // mounts with them) when it drops. Nothing else can be done.
        return Err(state.fail(format!("could not start the handover thread: {e}")));
    }
    tracing::info!("{message}");
    Ok(message)
}

/// Steps 1-2: preflight, then detach every view (undoing on failure).
fn prepare(node: &Arc<NodeRuntime>, binary: &Path) -> Result<Vec<Detached>, String> {
    preflight(binary)?;
    let engine = node.engine();
    if engine.fsmeta().e2e && std::env::var_os("CONSTELLATION_PASSPHRASE").is_none() {
        return Err(
            "an E2E filesystem's daemon can be upgraded only with CONSTELLATION_PASSPHRASE \
                    in its environment (the new image unlocks the keyring again)"
                .into(),
        );
    }
    let targets = targets(node, &[])?;
    check_targets(&targets, "upgrade")?;
    detach_targets(node, targets, None)
}

/// The mounted views to hand over: `ids`, or every one when empty.
pub(crate) fn targets(node: &Arc<NodeRuntime>, ids: &[u64]) -> Result<Vec<Target>, String> {
    let mounts = node.mounts.lock().unwrap();
    if let Some(missing) = ids
        .iter()
        .find(|id| !mounts.keys().any(|m| m.as_u64() == **id))
    {
        return Err(format!("no view {missing} is mounted"));
    }
    let targets: Vec<Target> = mounts
        .iter()
        .filter(|(id, _)| ids.is_empty() || ids.contains(&id.as_u64()))
        .map(|(id, m)| Target {
            id: *id,
            info: SessionInfoParts {
                subtree: m.subtree.clone(),
                mountpoint: m.mountpoint.clone(),
                fs_name: m.fs_name.clone(),
                allow_other: m.allow_other,
                read_only: m.read_only,
                fuse_threads: m.fuse_threads,
                sink: m.sink.clone(),
                caps: m.caps.clone(),
                qos: m.qos,
                confine_links: m.confine_links,
            },
            control: m.control.clone(),
            view: m.view.clone(),
        })
        .collect();
    if targets.is_empty() {
        return Err("no view is mounted".into());
    }
    Ok(targets)
}

/// What refuses a handover of `targets` before anything is detached: a
/// session served over a ring, a cluster lock, a blocking lock wait.
/// `what` names the handover in the refusal.
pub(crate) fn check_targets(targets: &[Target], what: &str) -> Result<(), String> {
    // Plan 38 §3(e): a session served over a ring can be handed over
    // **never**, not "once something is released" -- so it is refused
    // here, by name, before anything is detached, rather than as a
    // `detach` failure part way through a multi-view handover (which
    // would resume the views already detached in place for nothing).
    // `SessionControl::detach` refuses it too; this is the preflight that
    // tells the operator which mount and what to do about it.
    let ring: Vec<String> = targets
        .iter()
        .filter(|t| !t.control.transport().is_dev_fuse())
        .map(|t| {
            format!(
                "{} (served over {})",
                t.info.mountpoint.display(),
                t.control.transport()
            )
        })
        .collect();
    if !ring.is_empty() {
        return Err(format!(
            "refusing the {what}: a FUSE session served over io_uring cannot be handed to \
             another process image, on any kernel through 7.3 (plan 38 §3(e)) -- unmount and \
             remount these views, or mount them with --fuse-transport dev-fuse to keep them \
             upgradable in place: {}",
            ring.join("; ")
        ));
    }
    let mut blockers = Vec::new();
    for t in targets {
        for b in t.view.handover_blockers() {
            blockers.push(format!("{}: {b}", t.info.mountpoint.display()));
        }
        let waits = t.control.deferred_replies();
        if waits > 0 {
            blockers.push(format!(
                "{}: {waits} blocking lock wait(s) in flight",
                t.info.mountpoint.display()
            ));
        }
    }
    if !blockers.is_empty() {
        return Err(format!(
            "refusing the {what} (retry once these are released): {}",
            blockers.join("; ")
        ));
    }
    Ok(())
}

/// How many views [`detach_targets`] detaches at once.
const DETACH_PARALLEL: usize = 8;

/// Detach every target (its session stops reading, drains — bounded by
/// `drain`, else the session's own default — publishes and exports); on
/// any failure the ones already detached are resumed in place and nothing
/// is handed over.
///
/// The views are detached concurrently ([`DETACH_PARALLEL`] at a time):
/// each one's session, drain, barrier and export are its own, so a view's
/// callers wait for its own drain and publication, not for every other
/// view's as well (with N views, serially, the first view stopped would
/// wait for all N). Every view stopped is still served again only once
/// all have been tried, and only on a failure.
pub(crate) fn detach_targets(
    node: &Arc<NodeRuntime>,
    targets: Vec<Target>,
    drain: Option<Duration>,
) -> Result<Vec<Detached>, String> {
    let engine = node.engine();
    let mut results = Vec::with_capacity(targets.len());
    let mut targets = targets.into_iter().peekable();
    while targets.peek().is_some() {
        let batch: Vec<Target> = targets.by_ref().take(DETACH_PARALLEL).collect();
        let detach = |t: Target| {
            let view = t.view.clone();
            let result = t.control.detach_within(drain, || engine.export_view(&view));
            (t, result)
        };
        if batch.len() == 1 {
            results.extend(batch.into_iter().map(detach));
            continue;
        }
        std::thread::scope(|scope| {
            let running: Vec<_> = batch
                .into_iter()
                .map(|t| scope.spawn(|| detach(t)))
                .collect();
            for handle in running {
                results.push(handle.join().expect("a detach thread panicked"));
            }
        });
    }
    let mut done: Vec<Detached> = Vec::new();
    let mut failure = None;
    for (t, result) in results {
        match result {
            Ok(handoff) if handoff.view.is_ok() => done.push((t, handoff)),
            other => {
                let why = match &other {
                    Err(e) => e.to_string(),
                    Ok(h) => format!("{:#}", h.view.as_ref().expect_err("an export error")),
                };
                failure.get_or_insert(format!("detaching a view failed: {why}"));
                if let Ok(handoff) = other {
                    done.push((t, handoff));
                }
            }
        }
    }
    if let Some(why) = failure {
        for (t, handoff) in done {
            resume_in_place(node, t, handoff.fuse);
        }
        return Err(why);
    }
    Ok(done)
}

/// Serve a detached session again in this image (an abandoned upgrade).
pub(crate) fn resume_in_place(node: &Arc<NodeRuntime>, t: Target, fuse: FuseHandoff) {
    let mountpoint = t.info.mountpoint.clone();
    match FuseSession::resume(
        fuse,
        t.view.clone(),
        &t.info.options(node.fuse_transport),
        t.info.caps.clone(),
        Some(&t.info.sink),
    ) {
        Ok(session) => {
            node.serve_session(session, t.view, t.info.info());
        }
        Err(e) => tracing::error!(
            ?mountpoint,
            error = %e,
            id = t.id.as_u64(),
            "could not resume a detached view in place; the mount ends"
        ),
    }
}

/// Steps 4-5 (never returns when the `exec` happens).
fn hand_over(node: Arc<NodeRuntime>, binary: PathBuf, detached: Vec<Detached>) {
    let engine = node.engine().clone();
    for (t, _) in &detached {
        engine.close_view_for_handover(&t.view);
    }
    if let Err(e) = engine.shutdown_for_handover() {
        // The journal and pending rows stay in the state dir; the next
        // image ships them (as a remount would).
        tracing::warn!(error = %format!("{e:#}"), "the pre-handover drain left work for the next image");
    }
    let mut mounts = Vec::new();
    // Kept until the `exec`: a view's passthrough pins keep the chunks the
    // kernel serves handed-over handles from un-evictable (plan 38 Z3b),
    // and the next image re-pins them from the snapshot.
    let mut views = Vec::new();
    for (t, handoff) in detached {
        views.push(t.view.clone());
        let view = match handoff.view {
            Ok(view) => view,
            Err(_) => unreachable!("prepare keeps only exported views"),
        };
        mounts.push(MountHandoff {
            old_id: t.id.as_u64(),
            subtree: t.info.subtree,
            mountpoint: t.info.mountpoint,
            fs_name: t.info.fs_name,
            allow_other: t.info.allow_other,
            read_only: t.info.read_only,
            fuse_threads: t.info.fuse_threads,
            foreign: handoff.fuse.foreign,
            fuse_fd: handoff.fuse.fuse_fd.into_raw_fd(),
            init: handoff.fuse.init,
            passthrough: handoff.fuse.passthrough,
            view,
        });
    }
    let control_fd = node
        .handover
        .control
        .lock()
        .unwrap()
        .take()
        .map(IntoRawFd::into_raw_fd);
    let rollback_exe_fd = std::fs::File::open("/proc/self/exe")
        .ok()
        .map(IntoRawFd::into_raw_fd);
    let lock_fd = Some(LOCK_FD.load(Ordering::SeqCst)).filter(|fd| *fd >= 0);
    let reaper_pid = Some(REAPER_PID.load(Ordering::SeqCst)).filter(|p| *p != 0);
    let handoff = DaemonHandoff {
        version: HANDOVER_VERSION,
        generation: node.handover.generation + 1,
        from_version: env!("CONSTELLATION_VERSION").to_string(),
        lock_fd,
        control_fd,
        reaper_pid,
        rollback_exe_fd,
        is_rollback: false,
        node: node.handoff_config.clone(),
        mounts,
    };
    tracing::info!(binary = %binary.display(), views = handoff.mounts.len(), "exec'ing the new binary");
    let err = exec_with(&binary, &handoff);
    roll_back(handoff, &format!("exec {}: {err}", binary.display()));
}

/// `constellation daemon --resume-from <fd>`: step 6 of the module doc.
pub fn resume_main(
    handoff_fd: RawFd,
    threads: crate::parallelism::ThreadPlan,
    log_buffer: crate::log_buffer::LogBuffer,
) -> Result<()> {
    let handoff = read_handoff(handoff_fd)?;
    for fd in handoff.fds() {
        let _ = set_cloexec(fd, true);
    }
    if let Some(fd) = handoff.lock_fd {
        set_lock_fd(fd);
    }
    if let Some(pid) = handoff.reaper_pid {
        set_reaper_pid(pid);
        // The previous image's waiter for its child went with it.
        let _ = std::thread::Builder::new()
            .name("reaper-wait".into())
            .spawn(move || {
                let mut status = 0;
                // SAFETY: waiting for our own child.
                unsafe { libc::waitpid(pid as libc::pid_t, &mut status, 0) };
            });
    }
    tracing::info!(
        generation = handoff.generation,
        from = %handoff.from_version,
        rollback = handoff.is_rollback,
        views = handoff.mounts.len(),
        "resuming a handed-over daemon"
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.tokio)
        .max_blocking_threads(threads.blocking)
        .enable_all()
        .build()?;
    // A duplicate: the original stays open for a rollback until the node
    // is up.
    let control = handoff.control_fd.and_then(|fd| {
        // SAFETY: borrowing an inherited descriptor to duplicate it.
        let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
        borrowed
            .try_clone_to_owned()
            .ok()
            .map(std::os::unix::net::UnixListener::from)
    });
    let engine = match handoff.node.engine_config() {
        Ok(engine) => engine,
        Err(e) => roll_back(handoff, &format!("{e:#}")),
    };
    // Plan 38 Z1b: the knob the previous image ran with, re-resolved so
    // the environment still wins. Only the views added *after* this
    // resume can act on it; the resumed ones are `/dev/fuse` for good.
    let fuse_transport = match handoff.node.fuse_transport() {
        Ok(cfg) => cfg,
        Err(e) => roll_back(handoff, &format!("{e:#}")),
    };
    let state_dir = handoff
        .node
        .state_dir
        .clone()
        .context("the handoff names no state dir")?;
    let node = match NodeRuntime::start(
        crate::node_runtime::NodeConfig {
            fs_id: constellation_engine::FsId::new(state_dir.display().to_string()),
            engine,
            web_ui: handoff.node.web_ui,
            log_buffer,
            resumed: Some(crate::node_runtime::Resumed {
                generation: handoff.generation,
                control,
            }),
            fuse_transport,
            // `daemon --upgrade` hands over FUSE-serving daemons; a
            // headless `serve` node is replaced by restarting its pod.
            control_socket: None,
            persistent: false,
            signals: None,
        },
        rt.handle().clone(),
    ) {
        Ok(node) => node,
        Err(e) => roll_back(handoff, &format!("starting the node: {e:#}")),
    };
    // Up: the rollback's copies go.
    for fd in [handoff.control_fd, handoff.rollback_exe_fd]
        .into_iter()
        .flatten()
    {
        // SAFETY: inherited descriptors nothing else refers to.
        drop(unsafe { OwnedFd::from_raw_fd(fd) });
    }
    // The previous image's mount records go first, all of them: the new
    // image's view ids restart and may reuse an old one.
    for m in &handoff.mounts {
        crate::daemon_lock::forget_mount(&state_dir, m.old_id);
    }
    let mut served = 0;
    for m in handoff.mounts {
        let mountpoint = m.mountpoint.clone();
        match resume_mount(&node, m) {
            Ok(_) => served += 1,
            Err(e) => tracing::error!(
                ?mountpoint,
                error = %format!("{e:#}"),
                "a handed-over view could not be resumed; its mount ends"
            ),
        }
    }
    crate::startup::done("handover complete: serving");
    tracing::info!(
        generation = handoff.generation,
        served,
        "handover complete: serving"
    );
    node.wait_all();
    let failed = node.shutdown_error();
    drop(node);
    rt.shutdown_timeout(Duration::from_secs(10));
    match failed {
        Some(message) => bail!(message),
        None => Ok(()),
    }
}

/// The options of every session `daemon --upgrade` resumes (the new
/// image's `resume_mount`, and the old one's `resume_in_place` after an
/// abandoned detach). Handover-capable by construction: a resumed
/// connection is `/dev/fuse` (`FuseHandoff::transport`; a ring session
/// cannot be handed over at all, plan 38 §3(e)), and the marker says so at
/// the call site rather than leaving it to a comment, whatever `cfg` (the
/// node's knob) asks for. `cfg` still goes in: the session records what
/// was asked, so a resumed mount on a ring-capable host reports its
/// `handover_capable` fallback.
fn handover_options(
    fs_name: String,
    fuse_threads: usize,
    allow_other: bool,
    read_only: bool,
    cfg: constellation_frontend_fuse::TransportConfig,
) -> MountOptions {
    let mut opts = MountOptions::handover_capable(
        fs_name,
        fuse_threads,
        constellation_frontend_fuse::KernelTuning::for_workers(
            crate::parallelism::thread_plan().fuse,
        ),
        cfg,
        constellation_frontend_fuse::HandoverCapable,
    );
    opts.allow_other = allow_other;
    opts.read_only = read_only;
    opts
}

/// Reopen one view and resume its session on the inherited descriptor.
pub(crate) fn resume_mount(node: &Arc<NodeRuntime>, m: MountHandoff) -> Result<MountId> {
    // SAFETY: the inherited connection, ours from here (closing it on an
    // error below ends the mount, as there is nobody else to serve it).
    let fuse_fd = unsafe { OwnedFd::from_raw_fd(m.fuse_fd) };
    let engine = node.engine().clone();
    let caps = constellation_frontend_fuse::caps(engine.locks_cluster() && !m.read_only);
    let events = constellation_engine::DeferredEvents::new();
    let open: Vec<constellation_vfs::Ino> = m.view.handles.open_inos().collect();
    let (qos, confine_links) = (m.view.spec.qos, m.view.spec.confine_links);
    let view =
        engine.open_view_resumed(m.view.spec, m.view.handles, caps.clone(), events.clone())?;
    node.ensure_status(&view);
    let options = handover_options(
        m.fs_name.clone(),
        m.fuse_threads,
        m.allow_other,
        m.read_only,
        node.fuse_transport,
    );
    let session = match FuseSession::resume(
        FuseHandoff {
            fuse_fd,
            init: m.init,
            mountpoint: Some(m.mountpoint.clone()),
            foreign: m.foreign,
            passthrough: m.passthrough.clone(),
        },
        view.clone(),
        &options,
        caps.clone(),
        None,
    ) {
        Ok(session) => session,
        Err(e) => {
            engine.close_view(&view);
            return Err(e).context("resuming the FUSE session");
        }
    };
    let sink = session.notifier();
    events.set(Arc::new(sink.clone()));
    let root = view.view_root();
    let id = node.serve_session(
        session,
        view,
        SessionInfo {
            subtree: m.subtree,
            mountpoint: m.mountpoint,
            fs_name: m.fs_name,
            allow_other: m.allow_other,
            read_only: m.read_only,
            fuse_threads: m.fuse_threads,
            sink: sink.clone(),
            caps,
            qos,
            confine_links,
        },
    );
    // The gate dropped the engine's invalidations during the handover:
    // drop the kernel's pages of every open file once (entries and
    // attributes expire within their TTL anyway). On a thread: a
    // notification may wait for a request in flight, which is served now.
    if !open.is_empty() {
        let _ = std::thread::Builder::new()
            .name("handover-inval".into())
            .spawn(move || {
                let batch: Vec<constellation_vfs::Invalidation> = open
                    .into_iter()
                    .map(|ino| constellation_vfs::Invalidation::Data {
                        ino: if ino == root {
                            constellation_vfs::ROOT_INO
                        } else {
                            ino
                        },
                        range: None,
                    })
                    .collect();
                constellation_vfs::FrontendEvents::invalidate(&sink, &batch);
            });
    }
    Ok(id)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn handoff() -> DaemonHandoff {
        let mut engine = EngineConfig::new("s3://bucket/prefix");
        engine.state_dir = Some(PathBuf::from("/var/lib/constellation/x"));
        engine.cache_size = 123 << 20;
        engine.cto_strict = true;
        engine.locks = Some(false);
        engine.initial_write_mode = "back".parse().unwrap();
        engine.pin_target = Some(constellation_engine::e2e_pin::PinTarget::named(
            "myfs",
            "s3://bucket/prefix",
        ));
        DaemonHandoff {
            version: HANDOVER_VERSION,
            generation: 3,
            from_version: "test".into(),
            lock_fd: Some(7),
            control_fd: None,
            reaper_pid: Some(42),
            rollback_exe_fd: Some(9),
            is_rollback: false,
            node: NodeHandoff::of(
                &engine,
                8080,
                constellation_frontend_fuse::TransportConfig {
                    policy: constellation_frontend_fuse::TransportPolicy::Auto,
                    uring_queue_depth: Some(4),
                    ..Default::default()
                },
            ),
            mounts: Vec::new(),
        }
    }

    /// The handoff crosses `exec` in a memfd, and restarts the node with
    /// the settings it ran with.
    #[test]
    fn the_handoff_round_trips_through_a_memfd() {
        let sent = handoff();
        let fd = write_handoff(&sent).unwrap();
        let got = read_handoff(fd).unwrap();
        assert_eq!(got.generation, 3);
        assert_eq!(got.fds(), vec![7, 9]);
        assert_eq!(got.reaper_pid, Some(42));
        let cfg = got.node.engine_config().unwrap();
        assert_eq!(cfg.backend, "s3://bucket/prefix");
        assert_eq!(cfg.cache_size, 123 << 20);
        assert!(cfg.cto_strict);
        assert_eq!(cfg.locks, Some(false));
        assert_eq!(cfg.initial_write_mode.as_str(), "back");
        assert_eq!(
            cfg.state_dir.as_deref(),
            Some(Path::new("/var/lib/constellation/x"))
        );
        // Plan 38 Z1b: the transport knob survives the `exec` for the
        // views the new image mounts afterwards. Asserted on the stored
        // flags, not on `fuse_transport()`, which the ambient
        // `CONSTELLATION_FUSE_TRANSPORT` is entitled to override.
        let (policy, depth) = got.node.fuse_transport_flags().unwrap();
        assert_eq!(
            policy,
            Some(constellation_frontend_fuse::TransportPolicy::Auto)
        );
        assert_eq!(depth, Some(4));
        assert!(cfg.pin_target.is_some());
        assert_eq!(got.node.web_ui, 8080);
    }

    /// Plan 38 Z2c: the sessions `daemon --upgrade` resumes are
    /// `/dev/fuse` whatever the node's knob says — the shipped `auto`, the
    /// cluster-lock opt-in `uring`, any depth — while the views the new
    /// image mounts afterwards (plain ones) take the knob.
    #[test]
    fn an_upgrade_target_session_is_pinned_to_dev_fuse() {
        use constellation_frontend_fuse::{TransportConfig, TransportPolicy};
        for policy in [
            TransportPolicy::Auto,
            TransportPolicy::Uring,
            TransportPolicy::DevFuse,
        ] {
            let cfg = TransportConfig {
                policy,
                uring_queue_depth: Some(16),
                ..Default::default()
            };
            let opts = handover_options("v".into(), 2, true, false, cfg);
            assert_eq!(opts.transport(), TransportPolicy::DevFuse, "{policy}");
            assert!(opts.is_handover_capable());
            assert!(opts.allow_other && !opts.read_only);
        }
        assert_eq!(
            TransportConfig::default().policy,
            TransportPolicy::Auto,
            "plain mounts default to the ladder"
        );
    }

    #[test]
    fn a_handoff_of_another_version_is_refused() {
        let mut sent = handoff();
        sent.version = HANDOVER_VERSION + 1;
        let fd = write_handoff(&sent).unwrap();
        assert!(read_handoff(fd).is_err());
    }

    #[test]
    fn a_handoff_from_before_foreign_mounts_is_refused() {
        // Version 2 had no `MountHandoff::foreign`: no default fills it in,
        // the version (and so `--handover-abi`'s preflight) refuses it.
        let mut sent = handoff();
        sent.version = 2;
        let fd = write_handoff(&sent).unwrap();
        assert!(read_handoff(fd).is_err());
        assert_ne!(HANDOVER_VERSION, 2);
    }

    #[test]
    fn cloexec_is_toggled_on_inherited_descriptors() {
        let file = std::fs::File::open("/proc/self/exe").unwrap();
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        let get = || unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC;
        assert_ne!(get(), 0, "std opens close-on-exec");
        set_cloexec(fd, false).unwrap();
        assert_eq!(get(), 0);
        set_cloexec(fd, true).unwrap();
        assert_ne!(get(), 0);
    }

    #[test]
    fn the_abi_probe_names_this_handover_version() {
        let abi: serde_json::Value = serde_json::from_str(&handover_abi()).unwrap();
        assert_eq!(abi["handover"], HANDOVER_VERSION);
    }
}
