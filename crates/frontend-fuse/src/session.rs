//! Mounting a view ([`mount`], [`mount_source`]), the [`FuseSession`] it
//! returns, and the session handover (plan 31 §6.11):
//! [`SessionControl::detach`] and [`FuseSession::resume`].
//!
//! # Where a session's connection comes from: [`MountSource`]
//!
//! - `Path`: this process mounts. As root on Linux it calls `mount(2)`
//!   itself (`constellation_platform::linux::fuse_mount_fd`, the same
//!   primitive a CSI node plugin uses) and handshakes on the descriptor —
//!   no `fusermount3`; otherwise fuser mounts (through `fusermount3`).
//! - `PreopenedFd`: somebody else called `mount(2)` and hands over the
//!   descriptor with `FUSE_INIT` still pending; the session handshakes on
//!   it.
//! - A handed-over, already initialised connection is not a mount source
//!   but a [`FuseHandoff`], served by [`FuseSession::resume`] without a
//!   handshake (the kernel sends `FUSE_INIT` once per connection).
//!
//! # Detach
//!
//! [`SessionControl::detach`] hands a serving session's connection out
//! without the kernel ever seeing an unmount:
//!
//! 1. **Refuse outright** a session whose transport is not
//!    [`Transport::DevFuse`] (plan 38 §3(e)): a connection whose ring
//!    queues became ready can never be served over `/dev/fuse` again, so
//!    there is nothing to hand over. This comes before everything below —
//!    nothing is quiesced, and the session goes on serving.
//! 2. **Refuse early** while a request waits for a deferred reply (a
//!    blocking lock wait: its reply must be written on the descriptor it
//!    was read from, by a process that still serves it; fuser 0.18 cannot
//!    interrupt the wait).
//! 3. **Close the notification gate** (`notify`'s module doc) while the
//!    session still serves, and wait — bounded — for the notification
//!    writes under way. From here the engine's invalidations are dropped.
//! 4. **Stop reading** (the vendored fuser's `SessionDetacher`): each
//!    worker finishes the request it is dispatching — every op but a lock
//!    wait answers inline — and returns before its next read. Requests the
//!    kernel queues from now on stay queued, for the next server.
//! 5. On the session's own thread, once every worker has returned:
//!    **re-check** the deferred replies (a lock wait may have begun between
//!    2 and 4), **drain the deferred reads** (a cold read the engine answers
//!    from its completion pool: bounded, so waited for — up to
//!    `CONSTELLATION_HANDOVER_READ_DRAIN_MS` — rather than refused), then
//!    **publish every pending write** (`Vfs::sync_view`, the whole-view
//!    barrier). Either failing aborts the detach: the session
//!    resumes serving the same descriptor, in place, and the caller gets
//!    the reason.
//! 6. The caller receives the descriptor (a duplicate: same open file,
//!    same connection) and the `FUSE_INIT` it agreed, and exports the
//!    view's state; the session thread returns [`SessionExit::Detached`]
//!    without unmounting or destroying anything.
//!
//! Queued requests, `FORGET`s included, are served by whoever resumes the
//! descriptor; the kernel's lookup counts need nothing from us (inode
//! numbers are the replica's, the view keeps no lookup table).

use crate::adapter::{Deferred, FuseFs, KernelTuning};
use crate::notify::{FuseNotifySink, NotifyGate};
use crate::stats::SessionStats;
use constellation_types::Code;
use constellation_vfs::{Blocking, Caller, FrontendCaps, Observer, OpCtx, OpKind, Vfs};
use fuser::{NegotiatedInit, Transport};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::Duration;

/// Which transport a mount asks the kernel to serve it over (plan 38
/// §3(e)). A *policy*, not an outcome: what the connection ended up on is
/// [`FuseSession::transport`], runtime-negotiated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransportPolicy {
    /// Run plan 38 §2.4's ladder: FUSE-over-io_uring when this build
    /// carries the `io-uring` feature and the kernel and the process both
    /// grant it, `/dev/fuse` `writev` whenever anything in that chain
    /// says no. Every refusal is a logged, observable downgrade, never a
    /// mount failure.
    Auto,
    /// `/dev/fuse` `read`/`writev`, on every kernel and every platform.
    /// The default for **every** mount at plan 38 Z1 (Z2c is what flips
    /// plain mounts to [`Self::Auto`]), and permanently the only policy a
    /// handover-capable session may have ([`MountOptions::handover_capable`]).
    #[default]
    DevFuse,
}

impl TransportPolicy {
    /// `auto` / `dev-fuse`, as the knob and the CLI flag spell them.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "dev-fuse" | "dev_fuse" => Some(Self::DevFuse),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::DevFuse => "dev-fuse",
        }
    }
}

impl std::fmt::Display for TransportPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Env override of `--fuse-transport` ([`TransportConfig::resolve`]).
pub const TRANSPORT_ENV: &str = "CONSTELLATION_FUSE_TRANSPORT";
/// Env override of `--fuse-uring-queue-depth` ([`TransportConfig::resolve`]).
pub const URING_QUEUE_DEPTH_ENV: &str = "CONSTELLATION_FUSE_URING_QUEUE_DEPTH";

/// Ring entries per kernel queue, when the ring is what a mount gets.
/// The Skory fork's default, and libfuse's; plan 38 §4 works the ring's
/// memory budget out as `queues x depth x payload`, so this is one of the
/// two numbers an operator who has measured their own trade-off turns.
pub const DEFAULT_URING_QUEUE_DEPTH: usize = 8;

/// The transport knob, resolved once (plan 38 §4): what a *plain* mount
/// of this host asks for. A handover-capable session ignores it
/// ([`MountOptions::handover_capable`]).
///
/// [`Default`] is the shipped default — `dev-fuse`, depth 8 — and reads
/// no environment, so a test or a library embedder gets today's transport
/// without a knob in sight. The daemon calls [`Self::resolve`] once,
/// before it forks, and carries the answer to every mount it makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub policy: TransportPolicy,
    /// `io_uring_queue_depth`: ignored unless `policy` resolves to a ring.
    pub uring_queue_depth: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            policy: TransportPolicy::default(),
            uring_queue_depth: DEFAULT_URING_QUEUE_DEPTH,
        }
    }
}

impl TransportConfig {
    /// [`CONSTELLATION_FUSE_TRANSPORT`](TRANSPORT_ENV) /
    /// [`CONSTELLATION_FUSE_URING_QUEUE_DEPTH`](URING_QUEUE_DEPTH_ENV),
    /// else the `--fuse-transport` / `--fuse-uring-queue-depth` flags,
    /// else the default.
    ///
    /// The env wins over the flag, as `CONSTELLATION_PROFILE` wins over
    /// the profile its caller passes and `CONSTELLATION_CACHE_VERIFY`
    /// over `--cache-verify`: an operator must be able to put one host
    /// on one transport without editing what starts the daemon.
    ///
    /// Unlike `--cache-verify`, an unparseable value is an **error**
    /// rather than a warned-about fall-through, following
    /// `CONSTELLATION_PROFILE` ("an unknown value is an error, not a
    /// silent default"): a transport is not a safety setting whose
    /// stricter reading must survive a typo, and `dev-fuse` is what an
    /// ignored `auto` would silently give — the opposite of what the
    /// operator asked for, with the kernel's whole fast path quietly off.
    pub fn resolve(
        policy: Option<TransportPolicy>,
        uring_queue_depth: Option<usize>,
    ) -> Result<Self, String> {
        Self::resolve_from(|key| std::env::var(key).ok(), policy, uring_queue_depth)
    }

    /// [`Self::resolve`] over any variable source (tests).
    pub fn resolve_from(
        var: impl Fn(&str) -> Option<String>,
        policy: Option<TransportPolicy>,
        uring_queue_depth: Option<usize>,
    ) -> Result<Self, String> {
        let get = |key: &str| {
            var(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let policy = match get(TRANSPORT_ENV) {
            Some(raw) => TransportPolicy::parse(&raw)
                .ok_or_else(|| format!("{TRANSPORT_ENV}={raw}: expected auto or dev-fuse"))?,
            None => policy.unwrap_or_default(),
        };
        let depth = match get(URING_QUEUE_DEPTH_ENV) {
            Some(raw) => raw
                .parse::<usize>()
                .ok()
                .filter(|d| *d > 0)
                .ok_or_else(|| {
                    format!("{URING_QUEUE_DEPTH_ENV}={raw}: expected a positive integer")
                })?,
            None => uring_queue_depth.unwrap_or(DEFAULT_URING_QUEUE_DEPTH),
        };
        Ok(Self {
            policy,
            uring_queue_depth: depth,
        })
    }
}

/// The marker [`MountOptions::handover_capable`] takes: proof at the call
/// site that this session is one `SessionControl::detach` may be asked to
/// hand to another process image — `constellation daemon --upgrade`'s own
/// target mounts, and plan 37's CSI engine-pod
/// [`MountSource::PreopenedFd`] sessions.
///
/// It exists so that "a handover-capable session is `/dev/fuse`" (plan 38
/// §3(e)) is a thing the type system asks about rather than a convention
/// a future caller can forget: `MountOptions`'s transport fields are
/// private, the only way to a `TransportPolicy::Auto` mount is
/// [`MountOptions::new`], and the only way to a handover-capable one is
/// this constructor, which pins [`TransportPolicy::DevFuse`] whatever the
/// knob says.
#[derive(Debug, Clone, Copy)]
pub struct HandoverCapable;

/// How a view is mounted.
#[derive(Debug, Clone)]
pub struct MountOptions {
    /// The source name mount tools report.
    pub fs_name: String,
    /// Other users may access the mount (`SessionACL::All`; else the
    /// mounting user only).
    pub allow_other: bool,
    /// Mounted read-only (a frozen snapshot view).
    pub read_only: bool,
    /// fuser worker threads dispatching this mount's requests.
    pub n_threads: usize,
    /// The kernel request queue `FUSE_INIT` negotiates.
    pub tuning: KernelTuning,
    /// What transport this mount asks for (plan 38 §2.4's ladder).
    /// Private, and `DevFuse` for a [`Self::handover_capable`] session
    /// whatever the knob said — see [`HandoverCapable`].
    transport: TransportPolicy,
    /// What the transport knob asked for before any handover pin: a
    /// pinned session that was asked for `Auto` took a fallback, and
    /// `node.status` says so (plan 38 §2.4, [`crate::stats`]).
    asked: TransportPolicy,
    /// `io_uring_queue_depth`, read only when `transport` resolves to a
    /// ring.
    uring_queue_depth: usize,
    /// This session may be detached and resumed in another process image,
    /// so `transport` is pinned.
    handover: bool,
}

impl MountOptions {
    /// Options for a plain mount — one nothing will ever hand to another
    /// process image — with `cfg`'s transport policy.
    ///
    /// `allow_other` and `read_only` default to `false`; set them on the
    /// returned value (they are what the mount *is*, not what it may be).
    pub fn new(
        fs_name: impl Into<String>,
        n_threads: usize,
        tuning: KernelTuning,
        cfg: TransportConfig,
    ) -> Self {
        Self {
            fs_name: fs_name.into(),
            allow_other: false,
            read_only: false,
            n_threads,
            tuning,
            transport: cfg.policy,
            asked: cfg.policy,
            uring_queue_depth: cfg.uring_queue_depth,
            handover: false,
        }
    }

    /// Options for a session that may later be detached and resumed in
    /// another process image: [`TransportPolicy::DevFuse`], permanently
    /// and whatever `cfg` says (plan 38 §3(e)/Z0a — a connection whose
    /// ring queues became ready can never be served over `/dev/fuse`
    /// again, so `detach` refuses it outright and there is nothing to
    /// hand over).
    ///
    /// A `cfg` asking for [`TransportPolicy::Auto`] is logged at info, not
    /// refused: the knob is host-wide, and a host that wants the ring for
    /// its plain mounts must not fail to mount the ones it can upgrade.
    pub fn handover_capable(
        fs_name: impl Into<String>,
        n_threads: usize,
        tuning: KernelTuning,
        cfg: TransportConfig,
        _: HandoverCapable,
    ) -> Self {
        let fs_name = fs_name.into();
        if cfg.policy != TransportPolicy::DevFuse {
            tracing::info!(
                fs_name = %fs_name,
                asked = %cfg.policy,
                env = TRANSPORT_ENV,
                "this session can be handed to another process image, so it is served over \
                 /dev/fuse whatever the transport knob asks for (a ring session cannot be \
                 detached)"
            );
        }
        Self {
            transport: TransportPolicy::DevFuse,
            handover: true,
            ..Self::new(fs_name, n_threads, tuning, cfg)
        }
    }

    /// Override the transport of a plain mount. A no-op (logged) on a
    /// [`Self::handover_capable`] one: the pin is the point.
    pub fn with_transport(mut self, policy: TransportPolicy) -> Self {
        if self.handover {
            tracing::info!(
                fs_name = %self.fs_name,
                asked = %policy,
                "a handover-capable session stays on /dev/fuse"
            );
            return self;
        }
        self.transport = policy;
        self.asked = policy;
        self
    }

    /// What transport this mount asks for.
    pub fn transport(&self) -> TransportPolicy {
        self.transport
    }

    /// Ring entries per kernel queue, when the ring is what it gets.
    pub fn uring_queue_depth(&self) -> usize {
        self.uring_queue_depth
    }

    /// Whether this session may be handed to another process image.
    pub fn is_handover_capable(&self) -> bool {
        self.handover
    }

    /// The kernel mount these options describe, at `mountpoint`.
    pub fn source(&self, mountpoint: &Path) -> MountSource {
        let mut opts = constellation_platform::MountOpts::new(self.fs_name.clone());
        opts.allow_other = self.allow_other;
        opts.read_only = self.read_only;
        MountSource::Path(mountpoint.to_path_buf(), opts)
    }

    fn config(&self) -> fuser::Config {
        let mut config = fuser::Config::default();
        config.acl = if self.allow_other {
            fuser::SessionACL::All
        } else {
            fuser::SessionACL::Owner
        };
        config.n_threads = Some(self.n_threads.max(1));
        config.clone_fd = cfg!(target_os = "linux") && config.n_threads != Some(1);
        config.io_uring_queue_depth = self.uring_queue_depth.clamp(1, u32::MAX as usize) as u32;
        // One ring per worker thread, the kernel's per-CPU queues
        // partitioned across them (plan 38 §3(a)/§4): fuser sizes the set
        // from `n_threads`, not from the CPU count the kernel would
        // otherwise give a ring each. `clone_fd` is ignored when the ring
        // is active -- the ring's own per-worker queues are what it
        // exists for -- and fuser logs that once.
        if self.transport == TransportPolicy::Auto {
            if cfg!(feature = "io-uring") {
                config.io_uring = true;
            } else {
                // A ladder that degrades: asking for a transport this
                // build cannot speak is a downgrade, not a mount failure
                // (fuser's own `Config::io_uring` would refuse the mount).
                // Logged once per mount, as every other rung's refusal is
                // (plan 38 §2.4).
                tracing::warn!(
                    fs_name = %self.fs_name,
                    "transport auto, but this build has no io-uring feature; using /dev/fuse"
                );
            }
        }
        config
    }
}

/// Where a session's connection comes from (see the module doc).
#[derive(Debug)]
pub enum MountSource {
    /// Mount at this path, with these kernel options.
    Path(PathBuf, constellation_platform::MountOpts),
    /// A `/dev/fuse` descriptor mounted elsewhere, `FUSE_INIT` pending
    /// (`constellation_platform::linux::fuse_mount_fd`).
    PreopenedFd(OwnedFd),
}

/// A handed-over connection: what [`FuseSession::resume`] serves.
#[derive(Debug)]
pub struct FuseHandoff {
    /// The connection's `/dev/fuse` descriptor.
    pub fuse_fd: OwnedFd,
    /// What its `FUSE_INIT` agreed.
    pub init: NegotiatedInit,
    /// Where it is mounted, when known (for unmounting it later).
    pub mountpoint: Option<PathBuf>,
}

impl FuseHandoff {
    /// The transport the connection is served over. Always
    /// [`Transport::DevFuse`]: [`SessionControl::detach`] refuses to
    /// produce a handoff for any other, and
    /// [`NegotiatedInit::check_resumable`] -- which
    /// [`FuseSession::resume`] runs -- refuses to serve one (plan 38
    /// §3(e)). So a resumed session is `DevFuse` by construction, in
    /// builds with and without the `io-uring` feature alike.
    pub fn transport(&self) -> Transport {
        self.init.transport
    }
}

/// Everything the next server of a session needs (plan 31 §6.11's
/// `SessionHandoff`): the connection, and the view's own state `S` —
/// its spec and open-handle table, which the host exports from the view
/// (this crate does not name engine types; the daemon's `S` is
/// `constellation_engine::ViewHandoff`).
#[derive(Debug)]
pub struct SessionHandoff<S> {
    pub fuse: FuseHandoff,
    pub view: S,
}

/// Why a detach did not happen. The session keeps serving in every case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachError {
    pub code: Code,
    pub reason: String,
}

impl std::fmt::Display for DetachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.reason, self.code)
    }
}

impl std::error::Error for DetachError {}

fn refused(code: Code, reason: impl Into<String>) -> DetachError {
    DetachError {
        code,
        reason: reason.into(),
    }
}

/// How [`FuseSession::run`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionExit {
    /// The mount is gone (unmounted here or outside, or aborted).
    Unmounted,
    /// Handed out through [`SessionControl::detach`]: still mounted, the
    /// view is the caller's to export and close.
    Detached,
}

/// How long a detach waits for the notification writes under way
/// (`CONSTELLATION_HANDOVER_NOTIFY_WAIT_MS`, default 5000).
fn notify_wait() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_HANDOVER_NOTIFY_WAIT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000),
    )
}

/// How long a detach waits for the reads the engine is still answering
/// from its completion pool (`CONSTELLATION_HANDOVER_READ_DRAIN_MS`,
/// default 30 s): each is a bounded fetch.
fn read_drain_wait() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_HANDOVER_READ_DRAIN_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30_000),
    )
}

/// Wait, up to `within`, for every deferred read to be answered.
fn drain_reads(deferred: &Deferred, within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    while deferred.bounded() > 0 {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    true
}

type DetachReply = mpsc::SyncSender<Result<(OwnedFd, NegotiatedInit), DetachError>>;

#[derive(Default)]
struct Pending {
    reply: Option<DetachReply>,
    ended: bool,
}

/// State shared by a session, its thread and its [`SessionControl`]s.
struct Shared {
    /// The transport `FUSE_INIT` settled on, fixed for the life of the
    /// connection. Only a `DevFuse` session can be handed over, so this is
    /// what [`SessionControl::detach`] refuses by (plan 38 §3(e)).
    transport: Transport,
    /// What this session reports about its transport (plan 38 §5).
    stats: Arc<SessionStats>,
    /// `None` for a session that was never armed, which is every session
    /// whose `transport` is not `DevFuse`.
    detacher: Mutex<Option<fuser::SessionDetacher>>,
    /// fuser's unmounter while fuser owns the mount (`Path`, not root);
    /// after a detach, or for a mount made here or elsewhere, the mount is
    /// unmounted by path.
    fuser_unmounter: Mutex<Option<fuser::SessionUnmounter>>,
    mountpoint: Mutex<Option<PathBuf>>,
    gate: Arc<NotifyGate>,
    deferred: Arc<Deferred>,
    /// The detach waiting for the session thread's answer, and whether
    /// that thread is gone (nobody would answer).
    pending: Mutex<Pending>,
    /// One detach at a time.
    detaching: Mutex<()>,
}

/// A mounted FUSE session, not yet serving. The daemon runs it on a
/// dedicated OS thread ([`FuseSession::run`]), keeping a
/// [`SessionControl`] to unmount or detach it from elsewhere and a
/// [`FuseNotifySink`] for the engine's cache invalidations.
pub struct FuseSession<V: Vfs> {
    session: fuser::Session<FuseFs<V>>,
    vfs: Arc<V>,
    config: fuser::Config,
    shared: Arc<Shared>,
}

/// Mount `view` at `mountpoint` ([`mount_source`] with
/// [`MountOptions::source`]).
pub fn mount<V: Vfs>(
    view: Arc<V>,
    mountpoint: &Path,
    opts: &MountOptions,
    caps: FrontendCaps,
) -> std::io::Result<FuseSession<V>> {
    mount_source(view, opts.source(mountpoint), opts, caps)
}

/// Whether this process may call `mount(2)` itself.
fn privileged() -> bool {
    // SAFETY: geteuid has no preconditions.
    cfg!(target_os = "linux") && unsafe { libc::geteuid() } == 0
}

/// Mount `view` from `source` (see the module doc): the requested ACL,
/// `n_threads` workers each with its own `/dev/fuse` descriptor on Linux
/// (`clone_fd`), and the `FUSE_INIT` negotiation `caps` and
/// `opts.tuning` ask for.
pub fn mount_source<V: Vfs>(
    view: Arc<V>,
    source: MountSource,
    opts: &MountOptions,
    caps: FrontendCaps,
) -> std::io::Result<FuseSession<V>> {
    let config = opts.config();
    let fs = FuseFs::new(view.clone(), caps, opts.tuning);
    let deferred = fs.deferred().clone();
    let observer = fs.observer_slot();
    match source {
        MountSource::PreopenedFd(fd) => {
            // Somebody else holds this connection and may want it handed
            // back (plan 37's engine pod; plan 38 §3(e)): `/dev/fuse`
            // whatever `opts` asked for, so the pin holds even for a
            // caller that built plain options by mistake.
            debug_assert!(
                opts.is_handover_capable(),
                "a PreopenedFd session's options must be handover-capable"
            );
            let mut config = config;
            config.io_uring = false;
            let session = fuser::Session::from_fd(fs, fd, config.acl, config.clone())?;
            FuseSession::new(session, deferred, observer, opts, view, config, None, None)
        }
        MountSource::Path(mountpoint, kernel) if privileged() => {
            let fd = mount_fd(&mountpoint, &kernel)?;
            let session = match fuser::Session::from_fd(fs, fd, config.acl, config.clone()) {
                Ok(session) => session,
                Err(error) => {
                    // The mount exists; a failed handshake must not leave it.
                    let _ = unmount_path(&mountpoint, true);
                    return Err(error);
                }
            };
            FuseSession::new(
                session,
                deferred,
                observer,
                opts,
                view,
                config,
                Some(mountpoint),
                None,
            )
        }
        MountSource::Path(mountpoint, kernel) => {
            let mut options = vec![fuser::MountOption::FSName(kernel.fsname.clone())];
            if kernel.default_permissions {
                options.push(fuser::MountOption::DefaultPermissions);
            }
            if kernel.read_only {
                options.push(fuser::MountOption::RO);
            }
            let mut config = config;
            config.mount_options = options;
            if kernel.allow_other {
                config.acl = fuser::SessionACL::All;
            }
            let mut session = fuser::Session::new(fs, &mountpoint, &config)?;
            let unmounter = session.unmount_callable();
            FuseSession::new(
                session,
                deferred,
                observer,
                opts,
                view,
                config,
                Some(mountpoint),
                Some(unmounter),
            )
        }
    }
}

#[cfg(target_os = "linux")]
fn mount_fd(
    mountpoint: &Path,
    opts: &constellation_platform::MountOpts,
) -> std::io::Result<OwnedFd> {
    constellation_platform::linux::fuse_mount_fd(mountpoint, opts)
}

#[cfg(not(target_os = "linux"))]
fn mount_fd(_: &Path, _: &constellation_platform::MountOpts) -> std::io::Result<OwnedFd> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "a direct FUSE mount is Linux-only",
    ))
}

/// Unmount the FUSE mount at `path` without fuser (a mount made here, by
/// another process, or handed over): `umount2` as root, else
/// `fusermount3 -u`.
fn unmount_path(path: &Path, lazy: bool) -> std::io::Result<()> {
    let mode = if lazy {
        constellation_platform::UnmountMode::Lazy
    } else {
        constellation_platform::UnmountMode::Normal
    };
    constellation_platform::native().mounts.unmount(path, mode)
}

impl<V: Vfs> FuseSession<V> {
    /// `observer`: the filesystem's [`FuseFs::observer_slot`], filled here
    /// now that the handshake has settled the transport its ops and spans
    /// are labelled with — before [`Self::run`] serves any request.
    #[allow(clippy::too_many_arguments)]
    fn new(
        mut session: fuser::Session<FuseFs<V>>,
        deferred: Arc<Deferred>,
        observer: Arc<OnceLock<Observer>>,
        opts: &MountOptions,
        vfs: Arc<V>,
        config: fuser::Config,
        mountpoint: Option<PathBuf>,
        fuser_unmounter: Option<fuser::SessionUnmounter>,
    ) -> std::io::Result<Self> {
        // A ring session is not detachable: the kernel can neither hand
        // its queues to another process nor route its requests back to
        // `/dev/fuse` (plan 38 §3(e), Z0a). fuser refuses to arm one, so
        // ask it only for the transport that can be.
        let transport = session.transport();
        let stats = Arc::new(SessionStats::at_handshake(
            opts.asked,
            opts.handover,
            session.negotiated_init().as_ref(),
            transport,
            opts.uring_queue_depth,
        ));
        let _ = observer.set(Observer::new(
            crate::adapter::FRONTEND,
            &vfs.identity(),
            transport.name(),
        ));
        let detacher = if transport.is_dev_fuse() {
            Some(session.detacher()?)
        } else {
            tracing::info!(
                transport = transport.name(),
                "FUSE session is not handover-capable: this transport cannot be detached"
            );
            None
        };
        let gate = NotifyGate::new(session.notifier());
        Ok(Self {
            session,
            vfs,
            config,
            shared: Arc::new(Shared {
                transport,
                stats,
                detacher: Mutex::new(detacher),
                fuser_unmounter: Mutex::new(fuser_unmounter),
                mountpoint: Mutex::new(mountpoint),
                gate,
                deferred,
                pending: Mutex::new(Pending::default()),
                detaching: Mutex::new(()),
            }),
        })
    }

    /// Serve a handed-over connection (plan 31 §6.11) without re-running
    /// `FUSE_INIT`: the kernel answered it once, for the previous server.
    /// `view` must be the view the handoff's state was exported from,
    /// reopened ([`SessionHandoff::view`]). `sink`: a sink to reuse (its
    /// gate reopens on this session's channel) — an in-process resume
    /// after an aborted handover, where the engine already holds the sink;
    /// `None` makes a new one ([`Self::notifier`]).
    pub fn resume(
        handoff: FuseHandoff,
        view: Arc<V>,
        opts: &MountOptions,
        caps: FrontendCaps,
        sink: Option<&FuseNotifySink>,
    ) -> std::io::Result<FuseSession<V>> {
        let mut config = opts.config();
        // A resumed connection is `/dev/fuse` -- `check_resumable` refuses
        // anything else -- and there is no handshake here to create rings
        // in, so asking for the ring would be silently ignored. Clear it
        // rather than carry a request nothing can honor. Belt and braces:
        // every caller builds these options with
        // [`MountOptions::handover_capable`], which pins the policy
        // already, and `CONSTELLATION_FUSE_TRANSPORT=auto` is therefore
        // safe to leave set across a `daemon --upgrade`.
        debug_assert!(
            opts.transport() == TransportPolicy::DevFuse,
            "a resumed session's options must be handover-capable"
        );
        config.io_uring = false;
        let fs = FuseFs::new(view.clone(), caps, opts.tuning);
        let deferred = fs.deferred().clone();
        let observer = fs.observer_slot();
        let session = fuser::Session::from_fd_resumed(
            fs,
            handoff.fuse_fd,
            config.acl,
            config.clone(),
            handoff.init,
        )?;
        let mut resumed = FuseSession::new(
            session,
            deferred,
            observer,
            opts,
            view,
            config,
            handoff.mountpoint,
            None,
        )?;
        if let Some(sink) = sink {
            sink.gate().reopen(Some(resumed.session.notifier()));
            Arc::get_mut(&mut resumed.shared)
                .expect("a new session's state is not shared yet")
                .gate = sink.gate().clone();
        }
        Ok(resumed)
    }

    /// What this session's `FUSE_INIT` agreed.
    pub fn negotiated_init(&self) -> Option<NegotiatedInit> {
        self.session.negotiated_init()
    }

    /// The transport this session serves its connection over: what
    /// [`MountOptions::transport`] asked for if the kernel, the build and
    /// the process's capabilities all granted it, and
    /// [`Transport::DevFuse`] otherwise (the reason was logged once, by
    /// fuser, during the handshake). Fixed for the life of the connection.
    pub fn transport(&self) -> Transport {
        self.shared.transport
    }

    /// What this session reports about its transport (plan 38 §5): the
    /// negotiated transport, ring queue depth, the fallback its handshake
    /// took and its zero-copy read count.
    pub fn stats(&self) -> Arc<SessionStats> {
        self.shared.stats.clone()
    }

    /// The handle that unmounts or detaches this session from any thread.
    pub fn control(&self) -> SessionControl {
        SessionControl {
            shared: self.shared.clone(),
        }
    }

    /// A handle that unmounts this session from any thread (the daemon's
    /// `remove_mount`, a signal).
    pub fn unmounter(&mut self) -> FuseUnmounter {
        FuseUnmounter(self.control())
    }

    /// This mount's kernel cache, for the engine's invalidations.
    pub fn notifier(&self) -> FuseNotifySink {
        FuseNotifySink::gated(self.shared.gate.clone())
    }

    /// Where it is mounted, when known.
    pub fn set_mountpoint(&mut self, mountpoint: PathBuf) {
        *self.shared.mountpoint.lock().unwrap() = Some(mountpoint);
    }

    /// Serve requests until the mount ends or is detached; blocks the
    /// calling thread. A detach that has to be abandoned (module doc, step
    /// 4) resumes serving the same descriptor here, in place.
    pub fn run(self) -> std::io::Result<SessionExit> {
        let shared = self.shared.clone();
        let exit = self.serve();
        // Whatever ended the loop, no later detach may wait for it, and
        // one waiting now is answered.
        let mut pending = shared.pending.lock().unwrap();
        pending.ended = true;
        if let Some(reply) = pending.reply.take() {
            let _ = reply.send(Err(refused(
                Code::NotConnected,
                "the session ended during the detach",
            )));
        }
        exit
    }

    fn serve(self) -> std::io::Result<SessionExit> {
        let FuseSession {
            mut session,
            vfs,
            config,
            shared,
        } = self;
        loop {
            let detached = match session.run_detachable()? {
                // A detach racing the unmount is answered by `run`.
                fuser::SessionEnd::Ended => return Ok(SessionExit::Unmounted),
                fuser::SessionEnd::Detached(detached) => detached,
            };
            // fuser has given the mount up; from here it is unmounted by
            // path whatever happens.
            shared.fuser_unmounter.lock().unwrap().take();
            let reply = shared.pending.lock().unwrap().reply.take();
            let verdict = if shared.deferred.count() > 0 {
                Err(refused(
                    Code::Busy,
                    format!(
                        "{} blocking lock wait(s) began while the session stopped",
                        shared.deferred.count()
                    ),
                ))
            } else if !drain_reads(&shared.deferred, read_drain_wait()) {
                Err(refused(
                    Code::Busy,
                    format!(
                        "{} deferred read(s) still unanswered after {:?}",
                        shared.deferred.bounded(),
                        read_drain_wait()
                    ),
                ))
            } else {
                sync_view(&*vfs)
            };
            match (verdict, reply) {
                (Ok(()), Some(reply)) => {
                    let _ = reply.send(Ok((detached.fd, detached.init)));
                    return Ok(SessionExit::Detached);
                }
                (verdict, reply) => {
                    let why = verdict
                        .err()
                        .unwrap_or_else(|| refused(Code::Io, "nobody was waiting for the detach"));
                    tracing::warn!(reason = %why, "session detach abandoned; resuming in place");
                    // Serve the same connection again, in place.
                    let mut resumed = fuser::Session::from_fd_resumed(
                        detached.filesystem,
                        detached.fd,
                        config.acl,
                        config.clone(),
                        detached.init,
                    )?;
                    *shared.detacher.lock().unwrap() = Some(resumed.detacher()?);
                    shared.gate.reopen(Some(resumed.notifier()));
                    session = resumed;
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(why));
                    }
                }
            }
        }
    }
}

/// The whole-view barrier a detach ends with: every pending write of the
/// view published (`Vfs::sync_view`).
fn sync_view<V: Vfs>(vfs: &V) -> Result<(), DetachError> {
    let caller = Caller::new(0, 0, None);
    Blocking::run(|r| vfs.sync_view(&OpCtx::new(OpKind::SyncView, &caller), r))
        .map_err(|e| refused(e.code(), "publishing the view's pending writes failed"))
}

/// Unmounts or detaches a [`FuseSession`] from another thread.
#[derive(Clone)]
pub struct SessionControl {
    shared: Arc<Shared>,
}

impl SessionControl {
    /// Unmount (the session's thread then returns
    /// [`SessionExit::Unmounted`]).
    pub fn unmount(&self) -> std::io::Result<()> {
        if let Some(unmounter) = self.shared.fuser_unmounter.lock().unwrap().as_mut() {
            return unmounter.unmount();
        }
        let path = self.shared.mountpoint.lock().unwrap().clone();
        match path {
            Some(path) => unmount_path(&path, false),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "the mountpoint of a preopened FUSE session is unknown",
            )),
        }
    }

    /// The transport this session's connection is served over. A
    /// non-[`Transport::DevFuse`] one cannot be handed to another process
    /// image at all (plan 38 §3(e)), so `daemon --upgrade`'s preflight
    /// reads this and refuses by name before it detaches anything.
    pub fn transport(&self) -> Transport {
        self.shared.transport
    }

    /// Requests waiting for a deferred reply right now.
    pub fn deferred_replies(&self) -> usize {
        self.shared.deferred.count()
    }

    /// Hand the session out (the module doc's steps): once the session
    /// has stopped reading and published its pending writes, `export`
    /// runs (the view is quiescent: nothing reaches it through this
    /// session) and its result travels with the connection. On any
    /// refusal the session keeps serving and nothing was exported.
    pub fn detach<S>(&self, export: impl FnOnce() -> S) -> Result<SessionHandoff<S>, DetachError> {
        // First, before anything is quiesced: a session served over a ring
        // can never be handed over. Its entries belong to this process's
        // io_uring instance and die with it; the kernel does not route the
        // connection's requests back to `/dev/fuse`, and re-registering
        // from the next process loses every request that sat in an old
        // entry -- leaving its caller unkillable until a fusectl abort
        // (plan 38 §3(e), measured by Z0a). Tearing the mount down and
        // remounting is the only upgrade path for such a session.
        if !self.shared.transport.is_dev_fuse() {
            return Err(refused(
                Code::NotSupported,
                format!(
                    "this session is served over the {} transport, which cannot be handed over \
                     (only dev_fuse can); unmount and remount instead",
                    self.shared.transport
                ),
            ));
        }
        let _one = self.shared.detaching.lock().unwrap();
        let deferred = self.shared.deferred.count();
        if deferred > 0 {
            return Err(refused(
                Code::Busy,
                format!(
                    "{deferred} blocking lock wait(s) in flight (a lock wait cannot be handed \
                     over; retry once it is granted)"
                ),
            ));
        }
        if !self.shared.gate.close_and_wait(notify_wait()) {
            self.shared.gate.reopen(None);
            return Err(refused(
                Code::Busy,
                "a kernel cache notification stayed blocked in the kernel",
            ));
        }
        let (tx, rx) = mpsc::sync_channel(1);
        {
            let mut pending = self.shared.pending.lock().unwrap();
            if pending.ended {
                return Err(refused(Code::NotConnected, "the session has ended"));
            }
            pending.reply = Some(tx);
        }
        match self.shared.detacher.lock().unwrap().as_ref() {
            Some(detacher) => detacher.detach(),
            None => {
                self.shared.pending.lock().unwrap().reply.take();
                self.shared.gate.reopen(None);
                return Err(refused(Code::Io, "the session cannot be detached"));
            }
        }
        let answer = rx.recv().unwrap_or_else(|_| {
            Err(refused(
                Code::NotConnected,
                "the session thread ended during the detach",
            ))
        });
        let (fuse_fd, init) = answer?;
        let mountpoint = self.shared.mountpoint.lock().unwrap().clone();
        Ok(SessionHandoff {
            fuse: FuseHandoff {
                fuse_fd,
                init,
                mountpoint,
            },
            view: export(),
        })
    }
}

/// Ends a [`FuseSession`] from another thread.
pub struct FuseUnmounter(SessionControl);

impl FuseUnmounter {
    pub fn unmount(&mut self) -> std::io::Result<()> {
        self.0.unmount()
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    //! The handover against a real kernel mount (root and `/dev/fuse`
    //! only; skipped, loudly, elsewhere): detach with nothing in flight,
    //! a detach that waits for an op in flight, and a resume that serves
    //! a request the kernel queued while nobody was reading.

    use super::*;
    use constellation_vfs::mock::{MockVfs, Script};
    use constellation_vfs::StatFs;
    use std::os::unix::fs::MetadataExt;
    use std::time::Instant;

    // ---------------------------------------------- the transport knob

    /// No kernel needed: the knob's precedence, spellings and refusals.
    #[test]
    fn the_transport_knob_wins_over_the_flag() {
        let none = |_: &str| None;
        let var = |pairs: &'static [(&'static str, &'static str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())
            }
        };
        // Nothing set anywhere: the shipped default (plan 38 Z1 leaves
        // every mount on `/dev/fuse`; Z2c is what flips plain mounts).
        let cfg = TransportConfig::resolve_from(none, None, None).unwrap();
        assert_eq!(cfg.policy, TransportPolicy::DevFuse);
        assert_eq!(cfg.uring_queue_depth, DEFAULT_URING_QUEUE_DEPTH);
        // The flag alone.
        let cfg =
            TransportConfig::resolve_from(none, Some(TransportPolicy::Auto), Some(4)).unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Auto);
        assert_eq!(cfg.uring_queue_depth, 4);
        // The env over the flag, either way round, and tolerant of
        // whitespace and case as the other knobs' parsers are.
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, " AUTO "), (URING_QUEUE_DEPTH_ENV, "16")]),
            Some(TransportPolicy::DevFuse),
            Some(4),
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Auto);
        assert_eq!(cfg.uring_queue_depth, 16);
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, "dev_fuse")]),
            Some(TransportPolicy::Auto),
            None,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::DevFuse);
        // An empty value is "unset", not a parse error: an exported but
        // empty variable is what a shell leaves behind.
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, "  ")]),
            Some(TransportPolicy::Auto),
            None,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Auto);
        // Unparseable is an error, not a silent `dev-fuse` (see
        // `TransportConfig::resolve`'s doc for why this knob differs from
        // `--cache-verify`).
        let err = TransportConfig::resolve_from(var(&[(TRANSPORT_ENV, "uring")]), None, None)
            .expect_err("an unknown transport must be refused");
        assert!(
            err.contains(TRANSPORT_ENV) && err.contains("uring"),
            "{err}"
        );
        for bad in [
            &[(URING_QUEUE_DEPTH_ENV, "0")],
            &[(URING_QUEUE_DEPTH_ENV, "-1")],
            &[(URING_QUEUE_DEPTH_ENV, "lots")],
        ] {
            let err = TransportConfig::resolve_from(var(bad), None, None)
                .expect_err("a non-positive queue depth must be refused");
            assert!(err.contains(URING_QUEUE_DEPTH_ENV), "{err}");
        }
    }

    /// Plan 38 §3(e)'s hard rule, as a type-level fact rather than a
    /// convention: the only route to a handover-capable `MountOptions` is
    /// the constructor that takes the marker, and it pins `dev-fuse`.
    #[test]
    fn a_handover_capable_session_ignores_the_knob() {
        let asked = TransportConfig {
            policy: TransportPolicy::Auto,
            uring_queue_depth: 4,
        };
        let plain = MountOptions::new("plain", 2, KernelTuning::for_workers(2), asked);
        assert_eq!(plain.transport(), TransportPolicy::Auto);
        assert!(!plain.is_handover_capable());
        assert_eq!(plain.uring_queue_depth(), 4);

        let pinned = MountOptions::handover_capable(
            "pinned",
            2,
            KernelTuning::for_workers(2),
            asked,
            HandoverCapable,
        );
        assert_eq!(pinned.transport(), TransportPolicy::DevFuse);
        assert!(pinned.is_handover_capable());
        // Not even an explicit override gets through it.
        let still = pinned.clone().with_transport(TransportPolicy::Auto);
        assert_eq!(still.transport(), TransportPolicy::DevFuse);
        // A plain mount's override does.
        assert_eq!(
            plain.with_transport(TransportPolicy::DevFuse).transport(),
            TransportPolicy::DevFuse
        );
    }

    /// What `MountOptions` asks fuser for. `Auto` without the feature is
    /// a downgrade (the ladder's step 2->3), not a mount failure --
    /// `fuser::Config::io_uring` would refuse the mount outright.
    #[test]
    fn auto_asks_fuser_for_the_ring_only_where_the_build_can_serve_it() {
        let auto = MountOptions::new(
            "auto",
            2,
            KernelTuning::for_workers(2),
            TransportConfig {
                policy: TransportPolicy::Auto,
                uring_queue_depth: 3,
            },
        );
        let config = auto.config();
        assert_eq!(config.io_uring, cfg!(feature = "io-uring"));
        assert_eq!(config.io_uring_queue_depth, 3);
        let dev = auto.with_transport(TransportPolicy::DevFuse).config();
        assert!(!dev.io_uring);
    }

    fn kernel_available() -> bool {
        // SAFETY: no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        if !root || !Path::new("/dev/fuse").exists() {
            eprintln!("skipping: needs root and /dev/fuse");
            return false;
        }
        true
    }

    /// A handover-capable session's options: these tests are the detach
    /// protocol's own, and `HandoverCapable` is what pins them to
    /// `/dev/fuse` however the ambient `CONSTELLATION_FUSE_TRANSPORT` is
    /// set (so a ring-on harness leg does not break them).
    fn options() -> MountOptions {
        let mut opts = MountOptions::handover_capable(
            "constellation-handover-test",
            2,
            KernelTuning::for_workers(2),
            TransportConfig::default(),
            HandoverCapable,
        );
        opts.allow_other = true;
        opts
    }

    /// Plan 38 Z1b: a plain mount asking for the ladder
    /// (`TransportPolicy::Auto`), with `allow_other` off so the mount goes
    /// through `fusermount3` where the tests run unprivileged. Two
    /// workers, so the ring set is two rings sharing the kernel's per-CPU
    /// queues round-robin (§3(a): one ring per worker, not per CPU).
    fn uring_options() -> MountOptions {
        MountOptions::new(
            "constellation-uring-test",
            2,
            KernelTuning::for_workers(2),
            TransportConfig {
                policy: TransportPolicy::Auto,
                ..TransportConfig::default()
            },
        )
    }

    /// The transport this host can actually grant, probed the way the
    /// session probes it (`fuser::uring_unavailable`): the feature has to
    /// be built in, `fuse.enable_uring` has to be `Y` (kernel 6.14+) *and*
    /// `io_uring_setup(2)` has to be permitted here -- a seccomp policy
    /// that denies it (plan 38 §8, and plan 37's CSI pods) leaves
    /// `enable_uring=Y` saying nothing. Anything short of all three is a
    /// downgrade the session must take silently, which is exactly the
    /// ladder's step 2->3 (plan 38 §2.4).
    fn transport_this_host_grants() -> Transport {
        #[cfg(feature = "io-uring")]
        {
            match fuser::uring_unavailable() {
                None => Transport::Uring,
                Some(why) => {
                    eprintln!("this host cannot grant the ring: {why}");
                    Transport::DevFuse
                }
            }
        }
        #[cfg(not(feature = "io-uring"))]
        Transport::DevFuse
    }

    fn caps() -> FrontendCaps {
        crate::caps(false)
    }

    struct Mounted {
        dir: tempfile::TempDir,
        control: SessionControl,
        thread: std::thread::JoinHandle<std::io::Result<SessionExit>>,
    }

    fn serve(
        session: FuseSession<MockVfs>,
    ) -> (
        SessionControl,
        std::thread::JoinHandle<std::io::Result<SessionExit>>,
    ) {
        let control = session.control();
        (control, std::thread::spawn(move || session.run()))
    }

    fn mount_mock(vfs: &MockVfs) -> Option<Mounted> {
        if !kernel_available() {
            return None;
        }
        let dir = tempfile::tempdir().unwrap();
        let session = match mount(Arc::new(vfs.clone()), dir.path(), &options(), caps()) {
            Ok(session) => session,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping: mount(2) refused here ({e})");
                return None;
            }
            Err(e) => panic!("mount: {e}"),
        };
        let (control, thread) = serve(session);
        Some(Mounted {
            dir,
            control,
            thread,
        })
    }

    fn resume(
        vfs: &MockVfs,
        handoff: FuseHandoff,
    ) -> (
        SessionControl,
        std::thread::JoinHandle<std::io::Result<SessionExit>>,
    ) {
        let session = FuseSession::resume(handoff, Arc::new(vfs.clone()), &options(), caps(), None)
            .expect("resume");
        serve(session)
    }

    fn statfs() -> StatFs {
        StatFs {
            blocks: 1000,
            bfree: 500,
            bavail: 500,
            files: 10,
            ffree: 5,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        }
    }

    fn statvfs(path: &Path) -> std::io::Result<u64> {
        let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: a valid path and an out-struct the call fills.
        let mut out: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c.as_ptr(), &mut out) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(out.f_blocks as u64)
    }

    fn end(control: SessionControl, thread: std::thread::JoinHandle<std::io::Result<SessionExit>>) {
        control.unmount().expect("unmount");
        assert_eq!(thread.join().unwrap().unwrap(), SessionExit::Unmounted);
    }

    #[test]
    fn detach_with_nothing_in_flight_hands_out_a_live_connection() {
        let vfs = MockVfs::reference(caps());
        let Some(m) = mount_mock(&vfs) else { return };
        let file = m.dir.path().join("f");
        std::fs::write(&file, b"kept").unwrap();
        let dev = std::fs::metadata(m.dir.path()).unwrap().dev();
        let exported = std::sync::atomic::AtomicBool::new(false);
        let handoff = m
            .control
            .detach(|| exported.store(true, std::sync::atomic::Ordering::SeqCst))
            .expect("detach");
        assert!(exported.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            m.thread.join().unwrap().unwrap(),
            SessionExit::Detached,
            "the session thread hands the connection out"
        );
        let init = handoff.fuse.init;
        assert_eq!(init.proto_major, 7);
        assert!(init.max_write > 0 && init.kernel_minor > 0);
        init.check_resumable().unwrap();
        // The mount is still there (nobody unmounted it; the kernel lists
        // it) while nobody serves it.
        let listed = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
        assert!(listed.contains("constellation-handover-test"));
        let (control, thread) = resume(&vfs, handoff.fuse);
        assert_eq!(std::fs::read(&file).unwrap(), b"kept");
        assert_eq!(std::fs::metadata(m.dir.path()).unwrap().dev(), dev);
        std::fs::write(m.dir.path().join("g"), b"after").unwrap();
        assert_eq!(std::fs::read(m.dir.path().join("g")).unwrap(), b"after");
        end(control, thread);
    }

    #[test]
    fn detach_waits_for_an_op_in_flight() {
        let vfs = MockVfs::reference(caps());
        let Some(m) = mount_mock(&vfs) else { return };
        vfs.on_statfs(Script::With(Arc::new(|_| {
            std::thread::sleep(Duration::from_millis(800));
            Ok(statfs())
        })));
        let path = m.dir.path().to_path_buf();
        let slow = std::thread::spawn(move || statvfs(&path));
        std::thread::sleep(Duration::from_millis(200));
        let started = Instant::now();
        let handoff = m.control.detach(|| ()).expect("detach");
        let waited = started.elapsed();
        assert!(
            slow.is_finished(),
            "the detach returned before the op in flight was answered"
        );
        assert_eq!(slow.join().unwrap().expect("the op in flight"), 1000);
        assert!(waited >= Duration::from_millis(400), "{waited:?}");
        assert_eq!(m.thread.join().unwrap().unwrap(), SessionExit::Detached);
        let (control, thread) = resume(&vfs, handoff.fuse);
        end(control, thread);
    }

    #[test]
    fn a_resumed_session_serves_what_the_kernel_queued_meanwhile() {
        let vfs = MockVfs::reference(caps());
        let Some(m) = mount_mock(&vfs) else { return };
        std::fs::write(m.dir.path().join("f"), b"0123456789").unwrap();
        let handoff = m.control.detach(|| ()).expect("detach");
        assert_eq!(m.thread.join().unwrap().unwrap(), SessionExit::Detached);
        // Nobody reads the connection now: these wait in the kernel.
        let dir = m.dir.path().to_path_buf();
        let queued = std::thread::spawn(move || {
            let size = std::fs::metadata(dir.join("f"))?.len();
            std::fs::write(dir.join("new"), b"queued")?;
            Ok::<_, std::io::Error>(size)
        });
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !queued.is_finished(),
            "a request was answered with nobody serving"
        );
        let (control, thread) = resume(&vfs, handoff.fuse);
        assert_eq!(queued.join().unwrap().expect("the queued requests"), 10);
        assert_eq!(std::fs::read(m.dir.path().join("new")).unwrap(), b"queued");
        end(control, thread);
    }

    #[test]
    fn a_detach_refuses_while_a_lock_wait_is_in_flight() {
        let vfs = MockVfs::reference(caps());
        let Some(m) = mount_mock(&vfs) else { return };
        let fs = FuseFs::new(Arc::new(vfs.clone()), caps(), KernelTuning::for_workers(1));
        // A counted deferred reply, as a blocking `setlk` makes one.
        let deferred = fs.deferred().clone();
        let (responder, _wait) = Blocking::<()>::pair();
        let tracked = crate::adapter::track_for_test(&deferred, responder);
        assert_eq!(deferred.count(), 1);
        drop(tracked);
        assert_eq!(
            deferred.count(),
            0,
            "a dropped deferred reply is counted out"
        );
        // The live session's own counter: nothing deferred, then the
        // refusal path by hand.
        assert_eq!(m.control.deferred_replies(), 0);
        m.control.shared.deferred.track_raw();
        let refused = m.control.detach(|| ()).expect_err("a lock wait blocks it");
        assert_eq!(refused.code, Code::Busy);
        m.control.shared.deferred.untrack_raw();
        // Still serving.
        std::fs::write(m.dir.path().join("still"), b"up").unwrap();
        end(m.control, m.thread);
    }

    #[test]
    fn negotiated_init_round_trips() {
        let init = NegotiatedInit {
            kernel_major: 7,
            kernel_minor: 41,
            proto_major: 7,
            proto_minor: 40,
            kernel_flags: 0x1_2345_6789,
            flags: fuser::InitFlags::FUSE_ASYNC_READ.bits(),
            max_readahead: 131072,
            max_write: 1 << 20,
            max_background: 96,
            congestion_threshold: 72,
            time_gran_ns: 1,
            max_pages: 256,
            max_stack_depth: 0,
            transport: Transport::DevFuse,
        };
        let json = serde_json::to_string(&init).unwrap();
        assert_eq!(serde_json::from_str::<NegotiatedInit>(&json).unwrap(), init);
        let bytes = postcard::to_allocvec(&init).unwrap();
        assert_eq!(
            postcard::from_bytes::<NegotiatedInit>(&bytes).unwrap(),
            init
        );
        init.check_resumable().unwrap();
        let mut unknown = init;
        unknown.flags |= 1 << 62;
        assert!(unknown.check_resumable().is_err());
        let mut major = init;
        major.proto_major = 8;
        assert!(major.check_resumable().is_err());
        let mut big = init;
        big.max_write = 64 << 20;
        assert!(big.check_resumable().is_err());
        // Plan 38 §3(e): a ring connection is not resumable at all, in
        // every build -- the refusal is not behind the `io-uring` feature,
        // because a build without it must refuse a handoff a build with it
        // produced rather than read `/dev/fuse` and hang.
        for transport in [Transport::Uring, Transport::UringZeroCopy] {
            let mut ring = init;
            ring.transport = transport;
            let err = ring
                .check_resumable()
                .expect_err("a ring connection is not resumable");
            assert!(
                err.to_string().contains(transport.name()),
                "the refusal names the transport: {err}"
            );
            assert!(err.to_string().contains("cannot be handed over"), "{err}");
        }
    }

    /// Plan 38 Z1a. `daemon --upgrade` detaches every session *before* the
    /// new image parses the handoff, so a handoff a pre-Z1a binary wrote --
    /// which has no `transport` field at all -- must still parse, or the
    /// upgrade loses every mount with no rollback. The literal below is
    /// what a `main` binary's `MountHandoff.init` serializes to.
    #[test]
    fn a_pre_z1a_handoff_parses_as_dev_fuse() {
        let old = r#"{"kernel_major":7,"kernel_minor":41,"proto_major":7,"proto_minor":40,
            "kernel_flags":5033163263,"flags":1108347323,"max_readahead":131072,
            "max_write":1048576,"max_background":96,"congestion_threshold":72,
            "time_gran_ns":1,"max_pages":256,"max_stack_depth":0}"#;
        let init: NegotiatedInit = serde_json::from_str(old).expect("a pre-Z1a handoff parses");
        // Nothing before this patch could negotiate anything but /dev/fuse.
        assert_eq!(init.transport, Transport::DevFuse);
        init.check_resumable().expect("and it resumes");
    }

    #[test]
    fn a_detach_drains_deferred_reads_and_gives_up_past_its_wait() {
        let deferred = Arc::new(Deferred::default());
        assert!(drain_reads(&deferred, Duration::ZERO), "nothing to drain");
        deferred.track_bounded_raw();
        deferred.track_bounded_raw();
        assert_eq!((deferred.count(), deferred.bounded()), (0, 2));
        // Unanswered past the wait: the detach gives up (and resumes).
        let started = std::time::Instant::now();
        assert!(!drain_reads(&deferred, Duration::from_millis(50)));
        assert!(started.elapsed() >= Duration::from_millis(50));
        // Answered meanwhile (from the engine's pool): drained.
        let answering = deferred.clone();
        let pool = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            answering.untrack_bounded_raw();
            std::thread::sleep(Duration::from_millis(30));
            answering.untrack_bounded_raw();
        });
        assert!(drain_reads(&deferred, Duration::from_secs(10)));
        assert_eq!(deferred.bounded(), 0);
        pool.join().unwrap();
    }

    /// Plan 38 Z1a: a mount that asks for the ring gets whatever the
    /// running kernel and this build actually offer, and serves either
    /// way. On the dev host (`fuse.enable_uring=N`) that is the ladder's
    /// fallback to `/dev/fuse` -- the whole point of the runtime
    /// negotiation; on `RING_BOX` with the feature it is the ring.
    #[test]
    fn a_mount_that_asks_for_the_ring_serves_on_whatever_it_gets() {
        if !Path::new("/dev/fuse").exists() {
            eprintln!("skipping: needs /dev/fuse");
            return;
        }
        let vfs = MockVfs::reference(caps());
        let dir = tempfile::tempdir().unwrap();
        let session = match mount(Arc::new(vfs.clone()), dir.path(), &uring_options(), caps()) {
            Ok(session) => session,
            Err(e) => {
                eprintln!("skipping: cannot mount here ({e})");
                return;
            }
        };
        let expected = transport_this_host_grants();
        eprintln!(
            "mounted with transport auto; this host grants {expected}, the session reports {}",
            session.transport()
        );
        assert_eq!(session.transport(), expected);
        assert_eq!(
            session.negotiated_init().expect("negotiated").transport,
            expected,
            "the negotiated record and the session agree"
        );
        // Plan 38 §2.4/§5: the session's stats say what it got, and a
        // fallback is recorded exactly where the host could not grant it.
        let stats = session.stats();
        assert_eq!(stats.transport(), expected);
        match stats.last_fallback() {
            None => assert!(!expected.is_dev_fuse(), "a fallback went unrecorded"),
            Some(fallback) => {
                assert!(expected.is_dev_fuse(), "{fallback:?} on a ring session");
                assert_eq!((fallback.from, fallback.to), (Transport::Uring, expected));
                assert_ne!(fallback.reason, crate::FallbackReason::HandoverCapable);
                eprintln!("recorded fallback: {fallback:?}");
            }
        }
        assert_eq!(
            stats.uring_queue_depth() > 0,
            !expected.is_dev_fuse(),
            "a queue depth exactly when there are ring queues"
        );
        let handover_capable = session.transport().is_dev_fuse();
        let (control, thread) = serve(session);

        // It serves: a `statfs` and a `readdir` through the mount, and --
        // where this process owns the mount's root, which it does not when
        // the tests run unprivileged against `MockVfs::reference`'s
        // root-owned tree -- a write and a read back.
        vfs.on_statfs(Script::ok(statfs()));
        assert_eq!(statvfs(dir.path()).unwrap(), 1000);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        let file = dir.path().join("f");
        if std::fs::write(&file, b"over the ring or not").is_ok() {
            assert_eq!(std::fs::read(&file).unwrap(), b"over the ring or not");
        } else {
            eprintln!("not writing: this process does not own the mock root");
        }

        // A ring session refuses the handover, and refuses it *first*:
        // before the notification gate closes or anything else is
        // quiesced, so it is still serving afterwards (plan 38 §3(e); the
        // `/dev/fuse` case is the three detach tests above).
        if !handover_capable {
            let err = control
                .detach(|| ())
                .expect_err("a ring session cannot be handed over");
            assert_eq!(err.code, Code::NotSupported);
            assert!(err.reason.contains(expected.name()), "{err}");
            assert!(err.reason.contains("cannot be handed over"), "{err}");
            vfs.on_statfs(Script::ok(statfs()));
            assert_eq!(statvfs(dir.path()).unwrap(), 1000);
        }
        end(control, thread);
    }

    /// Plan 38 §3(e): whatever a handoff claims, `resume` refuses to serve
    /// a connection that is not `/dev/fuse`.
    #[test]
    fn resume_refuses_a_ring_handoff() {
        if !Path::new("/dev/fuse").exists() {
            eprintln!("skipping: needs /dev/fuse");
            return;
        }
        let vfs = MockVfs::reference(caps());
        let dir = tempfile::tempdir().unwrap();
        // `allow_other` off (what `uring_options` differs in, besides the
        // fs name), so `fusermount3` mounts this where tests run unprivileged
        // and without `user_allow_other`: the refusal is then a gate on an
        // ordinary host too, not only on a box that can grant the ring.
        let opts = uring_options().with_transport(TransportPolicy::DevFuse);
        let session = match mount(Arc::new(vfs.clone()), dir.path(), &opts, caps()) {
            Ok(session) => session,
            Err(e) => {
                eprintln!("skipping: cannot mount here ({e})");
                return;
            }
        };
        let (control, thread) = serve(session);
        let handoff = control.detach(|| ()).expect("detach");
        assert_eq!(thread.join().unwrap().unwrap(), SessionExit::Detached);
        let mut fuse = handoff.fuse;
        assert_eq!(fuse.transport(), Transport::DevFuse);
        // Claim the ring on an ordinary connection: `resume` must refuse
        // it rather than serve a transport it cannot have registered.
        fuse.init.transport = Transport::Uring;
        let mountpoint = fuse.mountpoint.clone();
        let err = match FuseSession::resume(fuse, Arc::new(vfs.clone()), &opts, caps(), None) {
            Err(err) => err,
            Ok(_) => panic!("a ring handoff is not resumable"),
        };
        assert!(err.to_string().contains("uring"), "{err}");
        // Nothing serves the mount now; take it out of the mount table.
        if let Some(path) = mountpoint {
            let _ = unmount_path(&path, true);
        }
    }
}
