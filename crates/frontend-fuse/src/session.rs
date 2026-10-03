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
//!    2 and 4), **drain the deferred reads and `fsync`s** (a cold read the
//!    engine answers from its completion pool, an `fsync` from its `fsync`
//!    pool, plan 39: waited for — up to
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
use crate::passthrough::{
    reason, PassthroughHandoff, PassthroughPolicy, PassthroughState, PassthroughWish,
    PASSTHROUGH_ENV,
};
use crate::stats::{Handshake, LockWaitCounter, SessionStats};
use constellation_types::Code;
use constellation_vfs::{Blocking, Caller, FrontendCaps, Observer, OpCtx, OpKind, Vfs};
use fuser::{NegotiatedInit, Transport};
use std::os::fd::{AsFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::Duration;

/// Which transport a mount asks the kernel to serve it over (plan 38
/// §3(e)). A *policy*, not an outcome: what the connection ended up on is
/// [`FuseSession::transport`], runtime-negotiated.
///
/// # Cluster locks and the ring (plan 38 Z2c)
///
/// Over a ring every request holds its queue entry until it is answered,
/// and the kernel queues a CPU's requests behind that CPU's queue only. A
/// blocking lock request (`F_SETLKW`, a blocking `flock`) handed to the
/// view's `lock-wait` thread therefore holds an entry for as long as the
/// lock is contended, which `/dev/fuse` never does. The vendored fuser
/// keeps one entry of every queue free by serving a blocking lock request
/// past `depth - 1` waiters on one queue as a non-blocking one — granted
/// if free, `ENOLCK` if contended (`RingCommit::reserve_lock_wait`) —
/// which removes the deadlock and replaces it with an error that
/// `/dev/fuse` mounts never return. The maintainer's decision
/// (2026-10-02): [`Self::Auto`] keeps a mount whose frontend forwards
/// locks to the cluster ([`FrontendCaps::cluster_locks`], the default
/// with P2P) on `/dev/fuse`, recorded as a `cluster_locks` fallback;
/// [`Self::Uring`] is the explicit opt-in that puts such a mount on the
/// ring anyway, with a deeper queue ([`CLUSTER_LOCKS_URING_QUEUE_DEPTH`])
/// and every downgrade counted (`lock_wait_downgrades`). To be revisited
/// after plan 38 Z4's zero-copy numbers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransportPolicy {
    /// Run plan 38 §2.4's ladder: FUSE-over-io_uring when this build
    /// carries the `io-uring` feature and the kernel and the process both
    /// grant it, `/dev/fuse` `writev` whenever anything in that chain
    /// says no. Every refusal is a logged, observable downgrade, never a
    /// mount failure. A mount with cluster locks stays on `/dev/fuse`
    /// (see the type's doc). The default for a plain mount since plan 38
    /// Z2c.
    #[default]
    Auto,
    /// [`Self::Auto`], and a mount with cluster locks takes the ring too,
    /// accepting the per-queue lock-wait budget: at most `depth - 1`
    /// blocking lock requests of one CPU wait at a time, and a further
    /// contended one is answered `ENOLCK`.
    Uring,
    /// `/dev/fuse` `read`/`writev`, on every kernel and every platform:
    /// the only policy a handover-capable session may have
    /// ([`MountOptions::handover_capable`]), and the mobile profile's
    /// default.
    DevFuse,
}

impl TransportPolicy {
    /// `auto` / `uring` / `dev-fuse`, as the knob and the CLI flag spell
    /// them.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "uring" => Some(Self::Uring),
            "dev-fuse" | "dev_fuse" => Some(Self::DevFuse),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Uring => "uring",
            Self::DevFuse => "dev-fuse",
        }
    }

    /// Whether this policy asks for the ring at all (for some mounts).
    pub fn asks_for_ring(self) -> bool {
        self != Self::DevFuse
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

/// Plan 38 Z4: whether a mount that gets the ring tries io_uring
/// zero-copy queues ([`UringZeroCopy`], resolved with the rest of the
/// transport knob by [`TransportConfig::resolve`]):
///
/// - `auto` (the default): zero-copy queues wherever the kernel offers
///   buffer pools (`FUSE_HAS_IO_URING_BUFPOOL`, 7.3+) and the daemon has
///   `CAP_SYS_ADMIN`, their buffer pools handed to the kernel unregistered:
///   a pool page becomes resident when a request first uses it and is never
///   given back, so a busy mount converges on the whole pool -- possible
///   CPUs x queue depth x one request's payload (1 MiB): 256 MiB at 32
///   CPUs and depth 8, 1 GiB at depth 32. The session negotiates `uring_zc`
///   then; anything short of that is plain `uring`, logged once by fuser.
/// - `pinned`: the same, with the pools registered as an io_uring fixed
///   buffer: all of it resident and pinned from the mount on, charged to
///   `RLIMIT_MEMLOCK` without `CAP_IPC_LOCK`, in exchange for the kernel
///   not importing a request's buffer per request. Opt-in.
/// - `off`: never; the ring stays `uring` with an entry buffer each.
///
/// Zero-copy only changes how replies reach the reader when a file is
/// opened for it, which nothing in the adapter does yet (plan 38 Z4b);
/// until then a `uring_zc` session serves exactly as a `uring` one, its
/// requests' payloads travelling in pool buffers. An unknown value is an
/// error, as for the other transport knobs.
pub const URING_ZERO_COPY_ENV: &str = "CONSTELLATION_FUSE_URING_ZERO_COPY";

/// Plan 38 Z4: [`URING_ZERO_COPY_ENV`]'s three answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UringZeroCopy {
    /// Zero-copy queues where the kernel and the capability allow, their
    /// pools unregistered (resident as requests touch them).
    #[default]
    Auto,
    /// The same, the pools registered (pinned, all resident from the start).
    Pinned,
    /// No zero-copy queues.
    Off,
}

impl UringZeroCopy {
    /// `auto` / `pinned` / `off`.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "pinned" => Some(Self::Pinned),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    /// fuser's `(io_uring_zero_copy, io_uring_register_pool)`.
    fn fuser(self) -> (bool, bool) {
        match self {
            Self::Auto => (true, false),
            Self::Pinned => (true, true),
            Self::Off => (false, false),
        }
    }
}

/// Ring entries per kernel queue, when the ring is what a mount gets.
/// The Skory fork's default, and libfuse's; plan 38 §4 works the ring's
/// memory budget out as `queues x depth x payload`, so this is one of the
/// two numbers an operator who has measured their own trade-off turns.
pub const DEFAULT_URING_QUEUE_DEPTH: usize = 8;

/// Ring entries per kernel queue for a mount with cluster locks that
/// [`TransportPolicy::Uring`] put on the ring (plan 38 Z2c), unless
/// `--fuse-uring-queue-depth` says otherwise. The lock-wait budget is
/// `depth - 1` blocked lock waiters per CPU, so 32 lets 31 processes of
/// one CPU wait for contended locks at once before the next is answered
/// `ENOLCK`, where 8 lets 7. The price is reserved address space —
/// `possible CPUs x depth x (max_write + a page)`, `MAP_NORESERVE` and
/// resident only as far as traffic touches it (Z1's measurement: 0 MiB
/// idle, 3.2 MiB after a workload, at depth 8) — so four times the
/// default's reservation and no resident memory of its own; Z2c's
/// measurement is in `docs/plans/v1/PROGRESS.md`, "Plan 38 Z2".
pub const CLUSTER_LOCKS_URING_QUEUE_DEPTH: usize = 32;

/// The transport knob, resolved once (plan 38 §4): what a *plain* mount
/// of this host asks for. A handover-capable session ignores it
/// ([`MountOptions::handover_capable`]).
///
/// [`Default`] is the shipped default — `auto`, the queue depth chosen
/// per mount — and reads no environment. The daemon calls
/// [`Self::resolve`] once, before it forks, and carries the answer to
/// every mount it makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub policy: TransportPolicy,
    /// `io_uring_queue_depth`, read only when a mount resolves to a ring.
    /// `None`: [`DEFAULT_URING_QUEUE_DEPTH`], or
    /// [`CLUSTER_LOCKS_URING_QUEUE_DEPTH`] for a mount with cluster locks
    /// that [`TransportPolicy::Uring`] put on the ring.
    pub uring_queue_depth: Option<usize>,
    /// Plan 38 Z3b: which mounts ask for FUSE passthrough — by default the
    /// read-only ones ([`PassthroughPolicy`]; [`PASSTHROUGH_ENV`];
    /// [`Self::with_cache_verify_always`]). Independent of `policy`:
    /// passthrough needs no ring (plan 38 §2.4).
    pub passthrough: PassthroughPolicy,
    /// Plan 38 Z4: whether a mount that gets the ring tries zero-copy
    /// queues ([`URING_ZERO_COPY_ENV`]).
    pub uring_zero_copy: UringZeroCopy,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            policy: TransportPolicy::default(),
            uring_queue_depth: None,
            passthrough: PassthroughPolicy::platform_default(),
            uring_zero_copy: UringZeroCopy::default(),
        }
    }
}

impl TransportConfig {
    /// [`CONSTELLATION_FUSE_TRANSPORT`](TRANSPORT_ENV) /
    /// [`CONSTELLATION_FUSE_URING_QUEUE_DEPTH`](URING_QUEUE_DEPTH_ENV),
    /// else the `--fuse-transport` / `--fuse-uring-queue-depth` flags,
    /// else `default` (the engine profile's: `auto`, or `dev-fuse` under
    /// the mobile profile) and the per-mount depth.
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
        default: TransportPolicy,
    ) -> Result<Self, String> {
        Self::resolve_from(
            |key| std::env::var(key).ok(),
            policy,
            uring_queue_depth,
            default,
        )
    }

    /// [`Self::resolve`] over any variable source (tests).
    pub fn resolve_from(
        var: impl Fn(&str) -> Option<String>,
        policy: Option<TransportPolicy>,
        uring_queue_depth: Option<usize>,
        default: TransportPolicy,
    ) -> Result<Self, String> {
        let get = |key: &str| {
            var(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let policy = match get(TRANSPORT_ENV) {
            Some(raw) => TransportPolicy::parse(&raw).ok_or_else(|| {
                format!("{TRANSPORT_ENV}={raw}: expected auto, uring or dev-fuse")
            })?,
            None => policy.unwrap_or(default),
        };
        let depth =
            match get(URING_QUEUE_DEPTH_ENV) {
                Some(raw) => Some(raw.parse::<usize>().ok().filter(|d| *d > 0).ok_or_else(
                    || format!("{URING_QUEUE_DEPTH_ENV}={raw}: expected a positive integer"),
                )?),
                None => uring_queue_depth,
            };
        // `CONSTELLATION_FUSE_PASSTHROUGH`: unset is the default (read-only
        // mounts only); `1` opts writable mounts in too, `ETXTBSY` caveat
        // and all (the `passthrough` module doc); `0` turns it off. An
        // unknown value is an error, as for the transport above — an
        // operator who typed it wanted one answer or the other, and either
        // silent default is the opposite of one of them.
        let uring_zero_copy = match get(URING_ZERO_COPY_ENV) {
            Some(raw) => UringZeroCopy::parse(&raw).ok_or_else(|| {
                format!("{URING_ZERO_COPY_ENV}={raw}: expected auto, pinned or off")
            })?,
            None => UringZeroCopy::default(),
        };
        let passthrough = match get(PASSTHROUGH_ENV) {
            Some(raw) => match raw.to_ascii_lowercase().as_str() {
                "1" | "on" | "true" | "yes" => PassthroughPolicy::On,
                "0" | "off" | "false" | "no" => PassthroughPolicy::Off(reason::DISABLED),
                _ => return Err(format!("{PASSTHROUGH_ENV}={raw}: expected 1 or 0")),
            },
            None => PassthroughPolicy::platform_default(),
        };
        Ok(Self {
            policy,
            uring_queue_depth: depth,
            passthrough,
            uring_zero_copy,
        })
    }

    /// `--cache-verify always`: no passthrough, whatever
    /// [`PASSTHROUGH_ENV`] says. Every byte served must be hashed on the
    /// read that serves it, and a passthrough handle's reads never reach
    /// the daemon (plan 38 §2.3). The engine refuses to offer a backing
    /// file under `always` too; this keeps the mount from even asking the
    /// kernel, and makes the reason the one `node.status` reports.
    pub fn with_cache_verify_always(mut self) -> Self {
        self.passthrough = PassthroughPolicy::Off(reason::CACHE_VERIFY_ALWAYS);
        self
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
    /// `io_uring_queue_depth` when set explicitly, read only when
    /// `transport` resolves to a ring ([`TransportConfig::uring_queue_depth`]).
    uring_queue_depth: Option<usize>,
    /// This session may be detached and resumed in another process image,
    /// so `transport` is pinned.
    handover: bool,
    /// Plan 38 Z3b: whether to ask for FUSE passthrough at `FUSE_INIT`
    /// (single-chunk read-only opens served by the kernel from the chunk
    /// file). From [`TransportConfig::passthrough`]: by default
    /// ([`PassthroughPolicy::ReadOnlyMounts`]) only a [`Self::read_only`]
    /// mount asks; [`PassthroughPolicy::On`] is the opt-in for writable
    /// mounts too, with the `ETXTBSY` caveat the `passthrough` module doc
    /// describes. Whether the session *gets* it is up to the process's
    /// `CAP_SYS_ADMIN` and the kernel (the same doc), reported per mount
    /// in `node.status`.
    ///
    /// A handover-capable session asks too: the backing ids it registers
    /// cross a handover with the session (`FuseHandoff::passthrough`).
    pub passthrough: PassthroughPolicy,
    /// Plan 38 Z4: whether a ring mount tries zero-copy queues, from
    /// [`TransportConfig::uring_zero_copy`].
    uring_zero_copy: UringZeroCopy,
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
            passthrough: cfg.passthrough,
            uring_zero_copy: cfg.uring_zero_copy,
        }
    }

    /// What this mount asks the kernel for, passthrough-wise: its
    /// [`Self::passthrough`] policy applied to [`Self::read_only`].
    pub fn passthrough_wish(&self) -> PassthroughWish {
        self.passthrough.wish(self.read_only)
    }

    /// Options for a session that may later be detached and resumed in
    /// another process image: [`TransportPolicy::DevFuse`], permanently
    /// and whatever `cfg` says (plan 38 §3(e)/Z0a — a connection whose
    /// ring queues became ready can never be served over `/dev/fuse`
    /// again, so `detach` refuses it outright and there is nothing to
    /// hand over).
    ///
    /// A `cfg` asking for the ring is not refused: the knob is host-wide,
    /// and a host that wants the ring for its plain mounts must not fail
    /// to mount the ones it can upgrade. The session records the pin as a
    /// `handover_capable` fallback where the ring would otherwise have
    /// been granted ([`crate::stats`]).
    pub fn handover_capable(
        fs_name: impl Into<String>,
        n_threads: usize,
        tuning: KernelTuning,
        cfg: TransportConfig,
        _: HandoverCapable,
    ) -> Self {
        // Not logged here: the mount records the pin as its fallback
        // (`handover_capable`, `crate::stats`) and the daemon logs that
        // record once per mount, so a line here would be a second report of
        // the same downgrade.
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

    /// Ring entries per kernel queue an explicit setting asked for
    /// (`None`: chosen per mount, [`TransportConfig::uring_queue_depth`]).
    pub fn uring_queue_depth(&self) -> Option<usize> {
        self.uring_queue_depth
    }

    /// What a mount with these options and `caps` asks fuser for (plan 38
    /// §2.4 and Z2c's cluster-lock rule, [`TransportPolicy`]'s doc). Pure:
    /// the build rung is `feature`, so every case is testable on any host.
    fn plan(&self, caps: &FrontendCaps, feature: bool) -> TransportPlan {
        let cluster_locks = caps.cluster_locks;
        let wanted = match self.transport {
            TransportPolicy::DevFuse => false,
            TransportPolicy::Auto => !cluster_locks,
            TransportPolicy::Uring => true,
        };
        let depth = self.uring_queue_depth.unwrap_or(
            if cluster_locks && self.transport == TransportPolicy::Uring {
                CLUSTER_LOCKS_URING_QUEUE_DEPTH
            } else {
                DEFAULT_URING_QUEUE_DEPTH
            },
        );
        TransportPlan {
            ring: wanted && feature,
            depth,
            held_back_for_locks: self.asked == TransportPolicy::Auto && cluster_locks,
        }
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

    /// The fuser configuration a mount with `plan` (from [`Self::plan`])
    /// starts with; `lock_waits` counts the ring's lock-wait downgrades.
    fn config(&self, plan: TransportPlan, lock_waits: &LockWaitCounter) -> fuser::Config {
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        let _ = lock_waits;
        let mut config = fuser::Config::default();
        config.acl = if self.allow_other {
            fuser::SessionACL::All
        } else {
            fuser::SessionACL::Owner
        };
        config.n_threads = Some(self.n_threads.max(1));
        config.clone_fd = cfg!(target_os = "linux") && config.n_threads != Some(1);
        config.io_uring_queue_depth = plan.depth.clamp(1, u32::MAX as usize) as u32;
        // One ring per worker thread, the kernel's per-CPU queues
        // partitioned across them (plan 38 §3(a)/§4): fuser sizes the set
        // from `n_threads`, not from the CPU count the kernel would
        // otherwise give a ring each. `clone_fd` is ignored when the ring
        // is active -- the ring's own per-worker queues are what it
        // exists for -- and fuser logs that once.
        if plan.ring {
            config.io_uring = true;
            (config.io_uring_zero_copy, config.io_uring_register_pool) =
                self.uring_zero_copy.fuser();
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            {
                config.io_uring_malformed_register = uring_fault_malformed_register();
                config.io_uring_lock_wait_downgrades = Some(lock_waits.hook());
            }
        } else if !cfg!(feature = "io-uring")
            && self.transport.asks_for_ring()
            && !plan.held_back_for_locks
        {
            // A ladder that degrades: asking for a transport this build
            // cannot speak is a downgrade, not a mount failure (fuser's own
            // `Config::io_uring` would refuse the mount). Logged once per
            // mount, as every other rung's refusal is (plan 38 §2.4).
            tracing::warn!(
                fs_name = %self.fs_name,
                transport = %self.transport,
                "the ring was asked for, but this build has no io-uring feature; using /dev/fuse"
            );
        }
        config
    }
}

/// What a mount asks fuser for ([`MountOptions::plan`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransportPlan {
    /// Ask for the ring (the build has it and the policy wants it for
    /// this mount).
    ring: bool,
    /// `io_uring_queue_depth`, when `ring`.
    depth: usize,
    /// [`TransportPolicy::Auto`] kept this mount off the ring because its
    /// frontend has cluster locks: the session's `cluster_locks` fallback.
    held_back_for_locks: bool,
}

/// `CONSTELLATION_FUSE_URING_FAULT=malformed-register`: **fault injection,
/// for the harness's `transport-refused-registration` scenario only.** A
/// mount that asks for the ring makes every ring REGISTER malformed, so a
/// kernel that offered the ring refuses it after the `FUSE_INIT` reply —
/// the one downgrade of plan 38 §2.4 no host setting can provoke — and the
/// mount falls back to `/dev/fuse` ([`mount_source`]). Unset (the default)
/// or any other value: no fault.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn uring_fault_malformed_register() -> bool {
    std::env::var("CONSTELLATION_FUSE_URING_FAULT")
        .is_ok_and(|v| v.trim().eq_ignore_ascii_case("malformed-register"))
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
    /// Somebody else made the mount (a [`MountSource::PreopenedFd`]
    /// session, and every session resumed from one): `mountpoint` is only
    /// its name, possibly in another mount namespace, and ending the
    /// session closes the connection instead of unmounting by path.
    pub foreign: bool,
    /// Plan 38 Z3b: which inodes the kernel holds open in which mode, and
    /// the backing ids — still registered — of those in passthrough mode
    /// ([`PassthroughHandoff`]).
    pub passthrough: PassthroughHandoff,
}

/// Whether a connection's `FUSE_INIT` turned passthrough on.
fn negotiated_passthrough(init: &NegotiatedInit) -> bool {
    init.max_stack_depth > 0 && init.flags & fuser::InitFlags::FUSE_PASSTHROUGH.bits() != 0
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

/// How long a detach waits for the reads and `fsync`s the engine is still
/// answering from its pools (`CONSTELLATION_HANDOVER_READ_DRAIN_MS`,
/// default 30 s): a read is a bounded fetch, an `fsync` its barrier —
/// unless S3 is away, and then the detach refuses after this long.
fn read_drain_wait() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_HANDOVER_READ_DRAIN_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30_000),
    )
}

/// Wait, up to `within`, for every deferred read and `fsync` to be answered.
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
    /// The detach under way bounds its read drain by this, not by
    /// [`read_drain_wait`] ([`SessionControl::detach_within`]).
    drain: Option<Duration>,
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
    /// Somebody else made the mount ([`FuseHandoff::foreign`]): never
    /// unmounted by path from here.
    foreign: bool,
    /// [`SessionControl::unmount`] asked a session it cannot unmount to
    /// end: the next detach the session thread sees closes the
    /// connection instead of handing it out or resuming it.
    ending: std::sync::atomic::AtomicBool,
    gate: Arc<NotifyGate>,
    deferred: Arc<Deferred>,
    /// Plan 38 Z3b: the session's passthrough state (its table crosses a
    /// handover in [`FuseHandoff::passthrough`]).
    passthrough: Arc<PassthroughState>,
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

/// Clear `O_NONBLOCK` on `fd`'s open file description.
fn set_blocking(fd: &OwnedFd) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let raw = fd.as_raw_fd();
    // SAFETY: F_GETFL/F_SETFL on a descriptor we own.
    unsafe {
        let flags = libc::fcntl(raw, libc::F_GETFL);
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if flags & libc::O_NONBLOCK != 0
            && libc::fcntl(raw, libc::F_SETFL, flags & !libc::O_NONBLOCK) < 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// How long a [`MountSource::PreopenedFd`] connection may take to show its
/// `FUSE_INIT` (`CONSTELLATION_PREOPENED_INIT_WAIT_MS`, default 10000).
fn preopened_init_wait() -> Duration {
    Duration::from_millis(
        std::env::var("CONSTELLATION_PREOPENED_INIT_WAIT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000),
    )
}

/// A sanity check before the `FUSE_INIT` handshake (fuser's stock
/// `from_fd`) on a pre-opened descriptor — not the guard. A connection
/// somebody just mounted has its `FUSE_INIT` queued by `mount(2)` itself,
/// so it is readable at once and nobody has set it non-blocking. A
/// *handed-over* one (plan 37 §8) never sends `FUSE_INIT` again: the
/// handshake would wait forever on it — spinning a core if it is still
/// non-blocking (K0 question 1), or answer `EIO` to a real request it
/// mistook for the handshake. Such a descriptor must go to
/// [`FuseSession::resume`], and what keeps it from coming here is the
/// type split: the production paths carry a handed-over connection as a
/// `FuseHandoff`, which only `resume` takes, never as a
/// `MountSource::PreopenedFd`. This check cannot tell the two apart in
/// general (a handed-over connection with a request queued is readable
/// and, since `detach` clears `O_NONBLOCK`, blocking too); it turns the
/// cases it can see — non-blocking, or nothing to read within `wait` —
/// into a refusal instead of a hang.
fn fresh_connection(fd: &OwnedFd, wait: Duration) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let raw = fd.as_raw_fd();
    // SAFETY: F_GETFL on a descriptor we own.
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if flags & libc::O_NONBLOCK != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing a non-blocking FUSE descriptor as a new mount: it belongs to a running \
             session (a handed-over connection is resumed, never re-initialised)",
        ));
    }
    let mut pfd = libc::pollfd {
        fd: raw,
        events: libc::POLLIN,
        revents: 0,
    };
    let deadline = std::time::Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        let ms = left.as_millis().min(i32::MAX as u128) as i32;
        // SAFETY: one live pollfd for the duration of the call.
        let n = unsafe { libc::poll(&mut pfd, 1, ms) };
        if n > 0 {
            return Ok(());
        }
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "no FUSE_INIT on the preopened descriptor within {wait:?}: not a fresh \
                 connection (a handed-over one is resumed, never re-initialised)"
            ),
        ));
    }
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
    let plan = opts.plan(&caps, cfg!(feature = "io-uring"));
    let lock_waits = LockWaitCounter::default();
    let config = opts.config(plan, &lock_waits);
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
            let fs = FuseFs::new(view.clone(), caps.clone(), opts.tuning)
                .with_passthrough(opts.passthrough_wish());
            let deferred = fs.deferred().clone();
            let observer = fs.observer_slot();
            let passthrough = fs.passthrough().clone();
            let mut config = config;
            config.io_uring = false;
            share_fd_without_a_device(&mut config);
            fresh_connection(&fd, preopened_init_wait())?;
            let session = fuser::Session::from_fd(fs, fd, config.acl, config.clone())?;
            FuseSession::new(
                session,
                deferred,
                observer,
                passthrough,
                &caps,
                opts,
                plan,
                lock_waits,
                view,
                config,
                None,
                true,
                None,
            )
        }
        MountSource::Path(mountpoint, kernel) => {
            match mount_path(
                &view,
                &mountpoint,
                &kernel,
                opts,
                plan,
                &lock_waits,
                caps.clone(),
                config.clone(),
            ) {
                // Plan 38 §2.4, the "refused ring registration mid-INIT"
                // rung: the INIT reply had already committed the
                // connection to rings, which the kernel then would not
                // register, and such a connection can neither be served
                // nor moved back to `/dev/fuse` (Z0a). fuser's constructor
                // failed and its drop took the mount down with it; this is
                // a new mount, on `/dev/fuse`. Logged once, here: the
                // refusal itself is not logged anywhere else.
                Err(error) if config.io_uring && registration_refused(&error) => {
                    tracing::warn!(
                        %error,
                        mountpoint = %mountpoint.display(),
                        "io_uring requested but the kernel refused to register the rings; \
                         mounting again over /dev/fuse"
                    );
                    let mut config = config;
                    config.io_uring = false;
                    mount_path(
                        &view,
                        &mountpoint,
                        &kernel,
                        opts,
                        plan,
                        &lock_waits,
                        caps,
                        config,
                    )
                }
                other => other,
            }
        }
    }
}

/// The ring registration refusal [`mount_source`] falls back from.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn registration_refused(error: &std::io::Error) -> bool {
    fuser::RegistrationRefused::is(error)
}

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
fn registration_refused(_: &std::io::Error) -> bool {
    false
}

/// A session on a descriptor somebody else opened may run where
/// `/dev/fuse` cannot be opened — plan 37's unprivileged engine pod has no
/// device at all — and `clone_fd` (one cloned descriptor per worker, which
/// fuser makes by opening `/dev/fuse` and `FUSE_DEV_IOC_CLONE`) would then
/// fail the session as it starts. Its workers share the one descriptor
/// instead, which is what fuser does on every other platform.
fn share_fd_without_a_device(config: &mut fuser::Config) {
    if !config.clone_fd {
        return;
    }
    let openable = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_ok();
    if !openable {
        tracing::info!(
            "/dev/fuse cannot be opened here: this session's workers share the handed-in \
             descriptor instead of cloning one each"
        );
        config.clone_fd = false;
    }
}

/// One attempt at mounting `view` at `mountpoint` with `config`.
#[allow(clippy::too_many_arguments)]
fn mount_path<V: Vfs>(
    view: &Arc<V>,
    mountpoint: &Path,
    kernel: &constellation_platform::MountOpts,
    opts: &MountOptions,
    plan: TransportPlan,
    lock_waits: &LockWaitCounter,
    caps: FrontendCaps,
    config: fuser::Config,
) -> std::io::Result<FuseSession<V>> {
    let declared = caps.clone();
    let fs = FuseFs::new(view.clone(), caps, opts.tuning).with_passthrough(opts.passthrough_wish());
    let deferred = fs.deferred().clone();
    let observer = fs.observer_slot();
    let passthrough = fs.passthrough().clone();
    if privileged() {
        let fd = mount_fd(mountpoint, kernel)?;
        let session = match fuser::Session::from_fd(fs, fd, config.acl, config.clone()) {
            Ok(session) => session,
            Err(error) => {
                // The mount exists; a failed handshake must not leave it.
                let _ = unmount_path(mountpoint, true);
                return Err(error);
            }
        };
        return FuseSession::new(
            session,
            deferred,
            observer,
            passthrough,
            &declared,
            opts,
            plan,
            lock_waits.clone(),
            view.clone(),
            config,
            Some(mountpoint.to_path_buf()),
            false,
            None,
        );
    }
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
    let mut session = fuser::Session::new(fs, mountpoint, &config)?;
    let unmounter = session.unmount_callable();
    FuseSession::new(
        session,
        deferred,
        observer,
        passthrough,
        &declared,
        opts,
        plan,
        lock_waits.clone(),
        view.clone(),
        config,
        Some(mountpoint.to_path_buf()),
        false,
        Some(unmounter),
    )
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
        passthrough: Arc<PassthroughState>,
        caps: &FrontendCaps,
        opts: &MountOptions,
        plan: TransportPlan,
        lock_waits: LockWaitCounter,
        vfs: Arc<V>,
        config: fuser::Config,
        mountpoint: Option<PathBuf>,
        foreign: bool,
        fuser_unmounter: Option<fuser::SessionUnmounter>,
    ) -> std::io::Result<Self> {
        // A ring session is not detachable: the kernel can neither hand
        // its queues to another process nor route its requests back to
        // `/dev/fuse` (plan 38 §3(e), Z0a). fuser refuses to arm one, so
        // ask it only for the transport that can be.
        let transport = session.transport();
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
        // Recorded (and a fallback counted) only once nothing here can fail
        // any more: a session that never serves took no fallback.
        let stats = Arc::new(SessionStats::at_handshake(
            Handshake {
                asked: opts.asked,
                pinned: opts.handover,
                held_back_for_locks: plan.held_back_for_locks,
                init: session.negotiated_init().as_ref(),
                negotiated: transport,
                uring_queue_depth: plan.depth,
            },
            lock_waits,
            passthrough.clone(),
        ));
        let gate = NotifyGate::new(session.notifier());
        // The backing-id ioctls go to the connection; a duplicate of the
        // session's descriptor is the same connection, usable from any
        // worker and by `release` long after the open that registered
        // the id.
        passthrough.set_transport(transport.name());
        if passthrough.enabled() || !passthrough.export().inodes.is_empty() {
            passthrough.set_device(session.as_fd().try_clone_to_owned()?);
        }
        // `CapEff` and the kernel's offer are not proof the kernel will
        // register a backing file for this process (a user namespace, a
        // cache on overlayfs): one probe, before anything is served, and
        // the view stops offering backing files if it failed.
        if passthrough.enabled() {
            if let Some(file) = vfs.passthrough_probe() {
                if !passthrough.probe(file) {
                    let mut caps = caps.clone();
                    caps.passthrough = false;
                    vfs.frontend_negotiated(&caps);
                }
            }
        }
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
                foreign,
                ending: std::sync::atomic::AtomicBool::new(false),
                gate,
                deferred,
                passthrough,
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
        let plan = opts.plan(&caps, cfg!(feature = "io-uring"));
        let lock_waits = LockWaitCounter::default();
        let mut config = opts.config(plan, &lock_waits);
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
        share_fd_without_a_device(&mut config);
        let mut caps = caps;
        let fs = FuseFs::new(view.clone(), caps.clone(), opts.tuning)
            .with_passthrough(opts.passthrough_wish());
        let deferred = fs.deferred().clone();
        let observer = fs.observer_slot();
        let passthrough = fs.passthrough().clone();
        // Plan 38 Z3b: no `FUSE_INIT` here, so what the first server
        // agreed decides, and the handed-over table says which inodes are
        // in which mode. Its backing ids are still registered: the kernel
        // goes on serving their handles, every later open of those inodes
        // must reuse them, and this process closes them at their last
        // release — whether or not *it* may start new ones.
        passthrough.import(&handoff.passthrough);
        let outcome = passthrough.want_from_kernel().and_then(|()| {
            if negotiated_passthrough(&handoff.init) {
                Ok(())
            } else {
                Err(reason::KERNEL)
            }
        });
        passthrough.settle(outcome);
        caps.passthrough = passthrough.enabled();
        view.frontend_negotiated(&caps);
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
            passthrough,
            &caps,
            opts,
            plan,
            lock_waits,
            view,
            config,
            handoff.mountpoint,
            handoff.foreign,
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

    /// This session's passthrough state: `node.status`'s
    /// `fuse.passthrough` reads [`PassthroughState::status`] from it.
    pub fn passthrough(&self) -> Arc<PassthroughState> {
        self.shared.passthrough.clone()
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
            let (reply, drain) = {
                let mut pending = shared.pending.lock().unwrap();
                (pending.reply.take(), pending.drain.take())
            };
            let drain = drain.unwrap_or_else(read_drain_wait);
            if shared.ending.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(end_detached(detached, &shared, &*vfs, reply));
            }
            let verdict = if shared.deferred.count() > 0 {
                Err(refused(
                    Code::Busy,
                    format!(
                        "{} blocking lock wait(s) began while the session stopped",
                        shared.deferred.count()
                    ),
                ))
            } else if !drain_reads(&shared.deferred, drain) {
                Err(refused(
                    Code::Busy,
                    format!(
                        "{} deferred read(s) or fsync(s) still unanswered after {:?}",
                        shared.deferred.bounded(),
                        drain
                    ),
                ))
            } else {
                sync_view(&*vfs)
            };
            // Plan 37 K0 (gap 4): an armed session's channel is
            // non-blocking, and the flag lives on the open file
            // description, which travels with the descriptor through
            // `SCM_RIGHTS`. Handed out like that, any reader but
            // `from_fd_resumed` (which `poll`s first) spins on `EAGAIN` —
            // a full core, measured. Blocking again before it leaves, so a
            // misuse is at worst a parked thread; the resuming session sets
            // the mode it wants itself.
            let verdict = verdict.and_then(|()| {
                set_blocking(&detached.fd).map_err(|e| {
                    refused(
                        Code::Io,
                        format!("clearing O_NONBLOCK on the handed-out descriptor: {e}"),
                    )
                })
            });
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

/// [`SessionControl::end`]'s second half, on the session thread: the
/// stopped session's deferred reads drained (bounded) and the view's
/// pending writes published, both best effort — nothing is resumed from
/// here, so a failure is logged, not refused — then the connection
/// closed. A detach that was waiting is told the session ended.
fn end_detached<V: Vfs>(
    detached: fuser::DetachedSession<FuseFs<V>>,
    shared: &Shared,
    vfs: &V,
    reply: Option<DetachReply>,
) -> SessionExit {
    if let Some(reply) = reply {
        let _ = reply.send(Err(refused(
            Code::NotConnected,
            "the session was ended during the detach",
        )));
    }
    if !drain_reads(&shared.deferred, read_drain_wait()) {
        tracing::warn!(
            reads = shared.deferred.bounded(),
            "ending the session with deferred reads unanswered"
        );
    }
    if let Err(e) = sync_view(vfs) {
        tracing::warn!(reason = %e, "ending the session: publishing pending writes failed");
    }
    // Every descriptor of the connection goes: the notifier's (the
    // gate's), then the session's own. The last one closing is what
    // aborts the connection in the kernel.
    shared.gate.retire(notify_wait());
    let fuser::DetachedSession { filesystem, fd, .. } = detached;
    drop(fd);
    drop(filesystem);
    let mountpoint = shared.mountpoint.lock().unwrap().clone();
    tracing::info!(?mountpoint, "FUSE session ended; its connection is closed");
    SessionExit::Unmounted
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
    ///
    /// A mount this process made is unmounted. One somebody else made (a
    /// [`MountSource::PreopenedFd`] session: plan 37's CSI node plugin
    /// mounts in its own mount namespace, which this unprivileged process
    /// can neither see nor unmount in) is **ended** instead ([`Self::end`]):
    /// the session stops, publishes the view's pending writes and closes
    /// the connection; the kernel then answers the mount `ENOTCONN` until
    /// its maker unmounts it. Either way the session thread returns, so a
    /// caller that joins it (the daemon's `view.unmount`) never waits for
    /// an unmount nobody will do.
    pub fn unmount(&self) -> std::io::Result<()> {
        if let Some(unmounter) = self.shared.fuser_unmounter.lock().unwrap().as_mut() {
            return unmounter.unmount();
        }
        let path = self.shared.mountpoint.lock().unwrap().clone();
        match path {
            Some(path) if !self.shared.foreign => unmount_path(&path, false),
            _ => self.end(),
        }
    }

    /// Whether somebody else made this session's mount
    /// ([`FuseHandoff::foreign`]), so it is ended rather than unmounted.
    pub fn is_foreign(&self) -> bool {
        self.shared.foreign
    }

    /// End the session without unmounting: stop reading (the detach's
    /// step 4), let the in-flight requests finish, drain the deferred
    /// reads (bounded) and publish the view's pending writes, then close
    /// the connection. Requests still queued in the kernel fail when the
    /// last descriptor closes (`ECONNABORTED`), so this is for a mount
    /// nobody uses any more — plan 37's `NodeUnstageVolume`, which kubelet
    /// sends only once every pod publishing the volume is gone.
    ///
    /// Returns once the session has been asked to stop; its thread returns
    /// [`SessionExit::Unmounted`] when it has. A lock wait still deferred
    /// keeps its reply channel, and with it the connection, open until it
    /// is answered — unmounting the mount (its maker's job) ends that too.
    pub fn end(&self) -> std::io::Result<()> {
        if self.shared.pending.lock().unwrap().ended {
            return Ok(());
        }
        self.shared
            .ending
            .store(true, std::sync::atomic::Ordering::SeqCst);
        match self.shared.detacher.lock().unwrap().as_ref() {
            Some(detacher) => {
                detacher.detach();
                Ok(())
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!(
                    "a {} session somebody else mounted can be neither unmounted nor ended \
                     from here",
                    self.shared.transport
                ),
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
        self.detach_within(None, export)
    }

    /// [`Self::detach`], with the wait for the reads and `fsync`s the
    /// engine is still answering bounded by `drain` (`None`: the default,
    /// `CONSTELLATION_HANDOVER_READ_DRAIN_MS`). Plan 37 §8's
    /// `--handoff-drain-timeout`: past it the detach refuses and the
    /// session goes on serving, so a slow drain costs a retry, never an
    /// abandoned request.
    pub fn detach_within<S>(
        &self,
        drain: Option<Duration>,
        export: impl FnOnce() -> S,
    ) -> Result<SessionHandoff<S>, DetachError> {
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
            pending.drain = drain;
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
                foreign: self.shared.foreign,
                // Taken once every worker stopped: nothing changes it now,
                // and its backing ids stay registered for the next server.
                passthrough: self.shared.passthrough.export(),
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
        let auto = TransportPolicy::Auto;
        // Nothing set anywhere: the profile's default (plan 38 Z2c: `auto`
        // for a plain mount, `dev-fuse` under the mobile profile), and the
        // queue depth left to each mount.
        let cfg = TransportConfig::resolve_from(none, None, None, auto).unwrap();
        assert_eq!(cfg, TransportConfig::default());
        assert_eq!(cfg.policy, TransportPolicy::Auto);
        assert_eq!(cfg.uring_queue_depth, None);
        let cfg =
            TransportConfig::resolve_from(none, None, None, TransportPolicy::DevFuse).unwrap();
        assert_eq!(cfg.policy, TransportPolicy::DevFuse);
        // The flag alone, over the profile's default.
        let cfg = TransportConfig::resolve_from(
            none,
            Some(TransportPolicy::Uring),
            Some(4),
            TransportPolicy::DevFuse,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Uring);
        assert_eq!(cfg.uring_queue_depth, Some(4));
        // The env over the flag, either way round, and tolerant of
        // whitespace and case as the other knobs' parsers are.
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, " AUTO "), (URING_QUEUE_DEPTH_ENV, "16")]),
            Some(TransportPolicy::DevFuse),
            Some(4),
            TransportPolicy::DevFuse,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Auto);
        assert_eq!(cfg.uring_queue_depth, Some(16));
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, "dev_fuse")]),
            Some(TransportPolicy::Auto),
            None,
            auto,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::DevFuse);
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, "Uring")]),
            None,
            None,
            TransportPolicy::DevFuse,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::Uring);
        // An empty value is "unset", not a parse error: an exported but
        // empty variable is what a shell leaves behind.
        let cfg = TransportConfig::resolve_from(
            var(&[(TRANSPORT_ENV, "  ")]),
            Some(TransportPolicy::DevFuse),
            None,
            auto,
        )
        .unwrap();
        assert_eq!(cfg.policy, TransportPolicy::DevFuse);
        // Unparseable is an error, not a silent `dev-fuse` (see
        // `TransportConfig::resolve`'s doc for why this knob differs from
        // `--cache-verify`).
        let err = TransportConfig::resolve_from(var(&[(TRANSPORT_ENV, "ring")]), None, None, auto)
            .expect_err("an unknown transport must be refused");
        assert!(err.contains(TRANSPORT_ENV) && err.contains("ring"), "{err}");
        for bad in [
            &[(URING_QUEUE_DEPTH_ENV, "0")],
            &[(URING_QUEUE_DEPTH_ENV, "-1")],
            &[(URING_QUEUE_DEPTH_ENV, "lots")],
        ] {
            let err = TransportConfig::resolve_from(var(bad), None, None, auto)
                .expect_err("a non-positive queue depth must be refused");
            assert!(err.contains(URING_QUEUE_DEPTH_ENV), "{err}");
        }
    }

    /// Plan 38 Z3b: `CONSTELLATION_FUSE_PASSTHROUGH` and
    /// `--cache-verify always`, as `MountOptions` and the adapter see them.
    #[test]
    fn the_passthrough_knob_and_cache_verify_always() {
        let none = |_: &str| None;
        let var = |pairs: &'static [(&'static str, &'static str)]| {
            move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())
            }
        };
        let tuning = KernelTuning::for_workers(2);
        // By default (Linux) only a read-only mount asks — plain or
        // handover-capable alike; a writable one says why it does not
        // (review 38-z3b must-fix 1: `open(O_RDWR)` would meet ETXTBSY).
        let cfg =
            TransportConfig::resolve_from(none, None, None, TransportPolicy::default()).unwrap();
        assert_eq!(cfg.passthrough, PassthroughPolicy::ReadOnlyMounts);
        let mut plain = MountOptions::new("p", 2, tuning, cfg);
        assert_eq!(
            plain.passthrough_wish(),
            PassthroughWish::Off(reason::WRITABLE_MOUNT)
        );
        plain.read_only = true;
        assert_eq!(plain.passthrough_wish(), PassthroughWish::On);
        let mut pinned = MountOptions::handover_capable("h", 2, tuning, cfg, HandoverCapable);
        assert_eq!(
            pinned.passthrough_wish(),
            PassthroughWish::Off(reason::WRITABLE_MOUNT)
        );
        pinned.read_only = true;
        assert_eq!(pinned.passthrough_wish(), PassthroughWish::On);
        // `1` opts writable mounts in; the mount option says the same.
        let cfg = TransportConfig::resolve_from(
            var(&[(PASSTHROUGH_ENV, "1")]),
            None,
            None,
            TransportPolicy::default(),
        )
        .unwrap();
        assert_eq!(cfg.passthrough, PassthroughPolicy::On);
        let opted = MountOptions::new("p", 2, tuning, cfg);
        assert_eq!(opted.passthrough_wish(), PassthroughWish::On);
        let mut by_option = MountOptions::new(
            "p",
            2,
            tuning,
            TransportConfig::resolve_from(none, None, None, TransportPolicy::default()).unwrap(),
        );
        by_option.passthrough = PassthroughPolicy::On;
        assert_eq!(by_option.passthrough_wish(), PassthroughWish::On);
        // The env turns it off, or back on; anything else is an error.
        for off in ["0", "off", " FALSE "] {
            let pairs: &'static [(&'static str, &'static str)] =
                Box::leak(Box::new([(PASSTHROUGH_ENV, off)]));
            let cfg =
                TransportConfig::resolve_from(var(pairs), None, None, TransportPolicy::default())
                    .unwrap();
            assert_eq!(
                cfg.passthrough,
                PassthroughPolicy::Off(reason::DISABLED),
                "{off}"
            );
            let mut opts = MountOptions::new("p", 2, tuning, cfg);
            opts.read_only = true;
            assert_eq!(
                opts.passthrough_wish(),
                PassthroughWish::Off(reason::DISABLED)
            );
        }
        let err = TransportConfig::resolve_from(
            var(&[(PASSTHROUGH_ENV, "maybe")]),
            None,
            None,
            TransportPolicy::default(),
        )
        .expect_err("an unknown value must be refused");
        assert!(err.contains(PASSTHROUGH_ENV), "{err}");
        // `--cache-verify always` forces it off, whatever the env said,
        // and says why.
        let cfg = TransportConfig::resolve_from(
            var(&[(PASSTHROUGH_ENV, "1")]),
            None,
            None,
            TransportPolicy::default(),
        )
        .unwrap()
        .with_cache_verify_always();
        let mut opts = MountOptions::new("p", 2, tuning, cfg);
        opts.read_only = true;
        assert_eq!(
            opts.passthrough_wish(),
            PassthroughWish::Off(reason::CACHE_VERIFY_ALWAYS)
        );
        // ... and the adapter built from those options never asks the
        // kernel, so `node.status` reports exactly that reason.
        let fs = FuseFs::new(
            Arc::new(MockVfs::new()),
            FrontendCaps::linux_fuse(false),
            tuning,
        )
        .with_passthrough(opts.passthrough_wish());
        assert_eq!(
            fs.passthrough().want_from_kernel(),
            Err(reason::CACHE_VERIFY_ALWAYS)
        );
        fs.passthrough().settle(Err(reason::CACHE_VERIFY_ALWAYS));
        let status = fs.passthrough().status();
        assert!(!status.enabled);
        assert_eq!(
            status.unavailable_reason.as_deref(),
            Some(reason::CACHE_VERIFY_ALWAYS)
        );
        assert!(
            status.fallbacks.is_empty(),
            "an operator's choice, not a downgrade"
        );
    }

    /// Plan 38 §3(e)'s hard rule, as a type-level fact rather than a
    /// convention: the only route to a handover-capable `MountOptions` is
    /// the constructor that takes the marker, and it pins `dev-fuse`.
    #[test]
    fn a_handover_capable_session_ignores_the_knob() {
        for policy in [TransportPolicy::Auto, TransportPolicy::Uring] {
            let asked = TransportConfig {
                policy,
                uring_queue_depth: Some(4),
                ..TransportConfig::default()
            };
            let pinned = MountOptions::handover_capable(
                "pinned",
                2,
                KernelTuning::for_workers(2),
                asked,
                HandoverCapable,
            );
            assert_eq!(pinned.transport(), TransportPolicy::DevFuse);
            for caps in [crate::caps(false), crate::caps(true)] {
                assert!(!pinned.plan(&caps, true).ring, "{policy} {caps:?}");
            }
        }
        let asked = TransportConfig {
            policy: TransportPolicy::Auto,
            uring_queue_depth: Some(4),
            ..TransportConfig::default()
        };
        let plain = MountOptions::new("plain", 2, KernelTuning::for_workers(2), asked);
        assert_eq!(plain.transport(), TransportPolicy::Auto);
        assert!(!plain.is_handover_capable());
        assert_eq!(plain.uring_queue_depth(), Some(4));

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
                uring_queue_depth: Some(3),
                ..TransportConfig::default()
            },
        );
        let plan = auto.plan(&crate::caps(false), cfg!(feature = "io-uring"));
        let config = auto.config(plan, &LockWaitCounter::default());
        assert_eq!(config.io_uring, cfg!(feature = "io-uring"));
        assert_eq!(config.io_uring_queue_depth, 3);
        let dev = auto.with_transport(TransportPolicy::DevFuse);
        let plan = dev.plan(&crate::caps(false), cfg!(feature = "io-uring"));
        assert!(!dev.config(plan, &LockWaitCounter::default()).io_uring);
    }

    /// Plan 38 Z4: `CONSTELLATION_FUSE_URING_ZERO_COPY` resolves with the
    /// rest of the transport knob -- `auto` by default, an unknown value
    /// refused -- and reaches fuser as its two zero-copy switches.
    #[test]
    fn the_zero_copy_knob_reaches_fuser() {
        let resolve = |pairs: Vec<(&'static str, String)>| {
            let get = move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.clone())
            };
            TransportConfig::resolve_from(get, None, None, TransportPolicy::Uring)
        };
        assert_eq!(
            resolve(vec![]).unwrap().uring_zero_copy,
            UringZeroCopy::Auto
        );
        assert_eq!(
            TransportConfig::default().uring_zero_copy,
            UringZeroCopy::Auto
        );
        assert!(
            !fuser::Config::default().io_uring_register_pool,
            "fuser's default is the unregistered pool too"
        );
        for (raw, knob, fuser) in [
            ("auto", UringZeroCopy::Auto, (true, false)),
            (" Pinned ", UringZeroCopy::Pinned, (true, true)),
            ("off", UringZeroCopy::Off, (false, false)),
        ] {
            let cfg = resolve(vec![(URING_ZERO_COPY_ENV, raw.to_owned())]).unwrap();
            assert_eq!(cfg.uring_zero_copy, knob, "{raw}");
            let opts = MountOptions::new("zc", 2, KernelTuning::for_workers(2), cfg);
            let plan = opts.plan(&crate::caps(false), true);
            let config = opts.config(plan, &LockWaitCounter::default());
            assert_eq!(
                (config.io_uring_zero_copy, config.io_uring_register_pool),
                fuser,
                "{raw}"
            );
        }
        for old in ["unpinned", "yes"] {
            let err =
                resolve(vec![(URING_ZERO_COPY_ENV, old.to_owned())]).expect_err("an unknown value");
            assert!(err.contains(URING_ZERO_COPY_ENV), "{err}");
        }
    }

    /// Plan 38 Z2c, the maintainer's decision: under `auto` a mount with
    /// cluster locks stays on `/dev/fuse` (and says so as its fallback);
    /// `uring` puts it on the ring with the deeper queue; an explicit
    /// depth wins over both defaults; a mount with local locks gets the
    /// ring under either policy at the ordinary depth.
    #[test]
    fn auto_keeps_cluster_lock_mounts_on_dev_fuse_and_uring_opts_in() {
        let opts = |policy, depth| {
            MountOptions::new(
                "z2c",
                2,
                KernelTuning::for_workers(2),
                TransportConfig {
                    policy,
                    uring_queue_depth: depth,
                    ..TransportConfig::default()
                },
            )
        };
        let (local, cluster) = (crate::caps(false), crate::caps(true));
        let plan = |o: &MountOptions, caps| o.plan(caps, true);
        let p = |ring, depth, held| TransportPlan {
            ring,
            depth,
            held_back_for_locks: held,
        };
        let d = DEFAULT_URING_QUEUE_DEPTH;
        let deep = CLUSTER_LOCKS_URING_QUEUE_DEPTH;
        let auto = opts(TransportPolicy::Auto, None);
        assert_eq!(plan(&auto, &local), p(true, d, false));
        assert_eq!(plan(&auto, &cluster), p(false, d, true));
        let uring = opts(TransportPolicy::Uring, None);
        assert_eq!(plan(&uring, &local), p(true, d, false));
        assert_eq!(plan(&uring, &cluster), p(true, deep, false));
        let explicit = opts(TransportPolicy::Uring, Some(4));
        assert_eq!(plan(&explicit, &cluster), p(true, 4, false));
        let dev = opts(TransportPolicy::DevFuse, None);
        assert_eq!(plan(&dev, &cluster), p(false, d, false));
        // The build rung comes after the policy: no ring without the
        // feature, and the cluster-lock rule still recorded under `auto`.
        assert_eq!(auto.plan(&cluster, false), p(false, d, true));
        assert_eq!(uring.plan(&cluster, false), p(false, deep, false));
        assert!(deep > d, "the deeper queue is deeper");
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

    /// A test's mountpoint. Dropped — a failed assertion's unwinding
    /// included — it is lazily unmounted before the directory is removed:
    /// a test that fails with a detached, unserved connection then fails,
    /// instead of hanging in the removal's `statx` on the mount (37-k5a's
    /// review, `detach_waits_for_an_op_in_flight` under host load).
    struct MountDir(Option<tempfile::TempDir>);

    impl MountDir {
        fn new() -> MountDir {
            MountDir(Some(tempfile::tempdir().unwrap()))
        }

        fn path(&self) -> &Path {
            self.0.as_ref().expect("until dropped").path()
        }
    }

    impl Drop for MountDir {
        fn drop(&mut self) {
            if let Some(dir) = self.0.take() {
                // Not mounted any more (the test ended its session): a
                // harmless refusal.
                let _ = unmount_path(dir.path(), true);
                drop(dir);
            }
        }
    }

    struct Mounted {
        dir: MountDir,
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
        let dir = MountDir::new();
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

    /// The guard on the stock handshake (`fresh_connection`), without a
    /// kernel: a non-blocking descriptor is refused at once, a blocking one
    /// with nothing to read once the wait is over, a readable one passes.
    #[test]
    fn the_stock_handshake_takes_only_a_fresh_connection() {
        let (r, w) = std::io::pipe().unwrap();
        let r: OwnedFd = r.into();
        let started = std::time::Instant::now();
        let err = fresh_connection(&r, Duration::from_millis(150)).unwrap_err();
        assert!(err.to_string().contains("no FUSE_INIT"), "{err}");
        assert!(started.elapsed() >= Duration::from_millis(140));
        let mut w = std::fs::File::from(OwnedFd::from(w));
        std::io::Write::write_all(&mut w, b"x").unwrap();
        fresh_connection(&r, Duration::from_millis(150)).expect("readable: a queued FUSE_INIT");
        // SAFETY: F_GETFL/F_SETFL on a descriptor we own.
        unsafe {
            let raw = std::os::fd::AsRawFd::as_raw_fd(&r);
            let flags = libc::fcntl(raw, libc::F_GETFL);
            libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let started = std::time::Instant::now();
        let err = fresh_connection(&r, Duration::from_secs(30)).unwrap_err();
        assert!(err.to_string().contains("non-blocking"), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "refused at once"
        );
        set_blocking(&r).unwrap();
        fresh_connection(&r, Duration::from_millis(150)).expect("blocking again");
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
        // Handed out blocking (K0 gap 4): whatever reads it next cannot
        // spin on EAGAIN.
        // SAFETY: F_GETFL on a live descriptor.
        let flags = unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&handoff.fuse.fuse_fd),
                libc::F_GETFL,
            )
        };
        assert_eq!(
            flags & libc::O_NONBLOCK,
            0,
            "the handed-out descriptor blocks"
        );
        // And the stock handshake refuses it rather than waiting for a
        // FUSE_INIT that never comes (or spinning) — unless a request is
        // queued on it right now (the kernel's own, say), which makes it
        // readable: the guard's known limit (`fresh_connection`'s doc).
        // Only then may it pass, and only because the poll saw one.
        let dup = handoff.fuse.fuse_fd.try_clone().unwrap();
        match fresh_connection(&dup, Duration::ZERO) {
            Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{err}"),
            Ok(()) => {
                let mut pfd = libc::pollfd {
                    fd: std::os::fd::AsRawFd::as_raw_fd(&dup),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: one live pollfd for the duration of the call.
                let ready = unsafe { libc::poll(&mut pfd, 1, 0) };
                assert_eq!(ready, 1, "passed the guard with nothing queued");
            }
        }
        drop(dup);
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

    /// Plan 37's K0 gap 3: a session on a descriptor somebody else mounted
    /// (the CSI node plugin's staging mount, served by an engine pod that
    /// cannot see or unmount it) is *ended* by `unmount` — its thread
    /// returns and the connection closes — instead of being refused,
    /// which left the daemon's `view.unmount` joined forever on a thread
    /// nothing would end. The mount itself stays until its maker unmounts
    /// it, answering `ENOTCONN` meanwhile.
    #[test]
    fn unmounting_a_preopened_session_ends_it_without_unmounting() {
        let vfs = MockVfs::reference(caps());
        if !kernel_available() {
            return;
        }
        let dir = MountDir::new();
        let mut kernel = constellation_platform::MountOpts::new("constellation-preopened-test");
        kernel.allow_other = true;
        let fd = match mount_fd(dir.path(), &kernel) {
            Ok(fd) => fd,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping: mount(2) refused here ({e})");
                return;
            }
            Err(e) => panic!("mount: {e}"),
        };
        let session = mount_source(
            Arc::new(vfs.clone()),
            MountSource::PreopenedFd(fd),
            &options(),
            caps(),
        )
        .expect("serving the preopened descriptor");
        let (control, thread) = serve(session);
        assert!(control.is_foreign());
        std::fs::write(dir.path().join("f"), b"served").unwrap();
        assert_eq!(std::fs::read(dir.path().join("f")).unwrap(), b"served");

        control.unmount().expect("a foreign session is ended");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(thread.join());
        });
        let exit = rx
            .recv_timeout(Duration::from_secs(20))
            .expect("the session thread must return once ended")
            .unwrap()
            .unwrap();
        assert_eq!(exit, SessionExit::Unmounted);
        // Still mounted (this process never unmounted it), but served by
        // nobody: the connection was closed.
        let listed = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
        assert!(listed.contains("constellation-preopened-test"));
        let err = std::fs::metadata(dir.path().join("f")).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN), "{err}");
        // Ending an ended session is a no-op; the maker's unmount works.
        control.unmount().expect("a second end");
        unmount_path(dir.path(), false).expect("the maker unmounts");
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
        let dir = MountDir::new();
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
        // Plan 38 Z4: a ring is `uring_zc` exactly where the kernel offered
        // buffer pools (7.3+) and the process has `CAP_SYS_ADMIN` as the
        // kernel checks it (in the initial user namespace) -- a test run as
        // root on such a kernel -- and `uring` anywhere else.
        let negotiated = session.negotiated_init().expect("negotiated");
        let bufpool = fuser::InitFlags::from_bits_retain(negotiated.kernel_flags)
            .contains(fuser::InitFlags::FUSE_HAS_IO_URING_BUFPOOL);
        let init_userns = std::fs::read_to_string("/proc/self/uid_map")
            .is_ok_and(|m| m.split_whitespace().collect::<Vec<_>>() == ["0", "0", "4294967295"]);
        let expected = match expected {
            Transport::Uring if bufpool && crate::has_cap_sys_admin() && init_userns => {
                Transport::UringZeroCopy
            }
            other => other,
        };
        eprintln!("buffer pools offered: {bufpool}; expecting {expected}");
        assert_eq!(session.transport(), expected);
        assert_eq!(
            negotiated.transport, expected,
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
        let dir = MountDir::new();
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
