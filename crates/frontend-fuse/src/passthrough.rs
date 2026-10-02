//! FUSE passthrough (plan 38 §3(b)/§3(c), Z3b): answering an open with
//! `FOPEN_PASSTHROUGH` and a backing id, so the kernel reads the chunk
//! file the engine handed over ([`constellation_vfs::Opened::backing`])
//! itself and the handle's reads never reach the daemon.
//!
//! # Whether a session may at all
//!
//! Four things, all checked once per session and never per open: the
//! mount asked for it ([`crate::MountOptions::passthrough`]: by default
//! only a read-only mount does, see below; `CONSTELLATION_FUSE_PASSTHROUGH`
//! and `--cache-verify always` change that); the process has
//! `CAP_SYS_ADMIN` (the kernel refuses `FUSE_DEV_IOC_BACKING_OPEN` without
//! it — `fs/fuse/backing.c`); the kernel offered `FUSE_PASSTHROUGH` at
//! `FUSE_INIT` (6.9+, built with `CONFIG_FUSE_PASSTHROUGH`) — only then
//! does `init` set `max_stack_depth = 1`, which is what makes the kernel
//! turn passthrough on for the connection; and, once the session has its
//! `/dev/fuse` descriptor and before it serves anything, one probe
//! registration of a file on the cache's filesystem succeeds
//! ([`PassthroughState::probe`]). `CapEff` alone is not proof: in a user
//! namespace it shows a `CAP_SYS_ADMIN` the kernel's `capable()` rejects
//! (`EPERM`), and a cache on overlayfs is refused as stacked (`ELOOP`).
//! A session that wanted passthrough and cannot have it logs the reason
//! **once** and reports it in `node.status`
//! ([`PassthroughStatus::unavailable_reason`]); its opens are answered
//! the ordinary way, and the view is told so
//! ([`constellation_vfs::Vfs::frontend_negotiated`]) and stops offering
//! backing files.
//!
//! # Why only read-only mounts by default
//!
//! The kernel rule below makes `open(O_RDWR)` of a file some other
//! process holds open through passthrough fail with `ETXTBSY` — a legal
//! POSIX call answered with an errno no local filesystem gives, whose
//! timing depends on what else happens to have the file open
//! (`fopen(f, "r+")`, Java's `RandomAccessFile(f, "rw")`, Python's
//! `open(f, "r+b")`, SQLite opening a database read-write while a
//! read-only connection holds it). On a mount the kernel mounted `ro`,
//! `open(O_RDWR)` is `EROFS` before FUSE sees it and the conflict cannot
//! arise, so passthrough is on there by default
//! ([`PassthroughPolicy::ReadOnlyMounts`]); a writable mount gets it only
//! when the operator opts in (`CONSTELLATION_FUSE_PASSTHROUGH=1`,
//! [`PassthroughPolicy::On`]) and accepts that caveat (plan 38 §3(c)).
//! (Today's read-only mounts are snapshot views; the engine offers their
//! frozen one-chunk files as backing files since plan 38 Z3c — like a
//! live file's, only when the chunk is verified on disk and not held in
//! the daemon's memory tier, whose hit beats the kernel's read.)
//!
//! # The kernel's per-inode rule, which the engine cannot see
//!
//! The kernel keeps one I/O mode per *inode* (`fs/fuse/iomode.c`): while
//! any handle of an inode is open with `FOPEN_PASSTHROUGH`, every other
//! open of it must be too, on the **same** backing file
//! (`fuse_file_io_open`: anything else is `EIO` to the opener), and while
//! any is open the ordinary, page-cached way, a passthrough open is
//! refused (`ETXTBSY`, again `EIO` to the opener). The engine decides
//! eligibility per open; this module reconciles that with the kernel's
//! rule, per inode, before every reply:
//!
//! - **No handle open in passthrough mode**: an eligible read-only open
//!   gets a backing id only when no ordinary handle of the inode is open
//!   or about to be (a write-intent open registers itself before it calls
//!   the view, so a passthrough cannot slip in between its call and its
//!   reply). Otherwise it is answered the ordinary way — counted as a
//!   `cached_handle_open` fallback.
//! - **Some handle open in passthrough mode**: every open of the inode
//!   reuses that handle's backing id. A read-only open the engine offered
//!   the very same chunk is plain passthrough. Any other — the engine
//!   offered nothing (a write landed, so the file is no longer that
//!   chunk), offered a different chunk (`backing_busy`), or a write-only
//!   open — is answered `FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO`: its reads
//!   and writes go to the daemon (direct I/O), so it sees the current
//!   bytes, and only an `mmap` of it would use the backing file. A
//!   write-only handle cannot be `mmap`ed at all (`mmap` needs read
//!   access), and a read-only one maps the backing file read-only.
//! - **A read-write open of an inode open in passthrough mode** is
//!   refused with `ETXTBSY`, until the passthrough handles close. It is
//!   the one open the kernel's rule leaves no safe answer for: an
//!   ordinary reply is `EIO`, and a passthrough one opens the chunk file
//!   read-write in the kernel (`backing_file_open` uses the opener's
//!   flags), where a shared writable `mmap` would write into the
//!   content-addressed cache. `ETXTBSY` is what Linux already answers an
//!   open for writing of a file it is executing, and it names the cause.
//!   The refusal is made at the reply, after the view answered: an open
//!   the view refuses itself (`EROFS` on a frozen view, `ENOENT`, `EACCES`)
//!   gets the view's errno, and one it accepted is released again before
//!   the `ETXTBSY` goes out. A view's open has no side effect a release
//!   does not undo (`O_TRUNC` is applied by the kernel after a successful
//!   open — this adapter does not negotiate `FUSE_ATOMIC_O_TRUNC`).
//!
//! So a passthrough handle opened *before* a writer keeps reading the
//! chunk it was opened on until it is closed — close-to-open, as for a
//! writer on another node (plan 38 §3(c)) — while every open *after* the
//! writer sees the writer's bytes through `read(2)`. An `mmap` of such a
//! later handle is the exception the kernel leaves: a handle answered
//! `FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO` reads and writes through the
//! daemon, but `fuse_file_mmap` maps the inode's *backing* file whenever
//! the handle has one, so its `mmap` shows the chunk the first
//! passthrough handle was opened on — the old bytes — until every
//! passthrough handle of the inode is closed. Nothing in userspace can
//! change that (there is no way to revoke a backing file).
//!
//! # Backing ids
//!
//! An id is registered (`FUSE_DEV_IOC_BACKING_OPEN`) for the first
//! passthrough handle of an inode and closed (`FUSE_DEV_IOC_BACKING_CLOSE`)
//! when its last one is released, exactly once. Closing it any earlier is
//! not safe: the kernel looks the id up when the opener's task processes
//! the reply, after it was written (fuser's `BackingId` doc), and every
//! later open of the inode needs the same one. The kernel's own references
//! — one on the backing file per id, one per passthrough handle, one per
//! inode in passthrough mode — keep what it serves from alive however the
//! ids are closed, which is also why the ids are kept raw here rather
//! than as fuser's `BackingId`, whose `Drop` closes them: a session that
//! is handed over must leave them registered, and the next process closes
//! them by number ([`PassthroughHandoff`]).

use crate::adapter::open_flags_write_intent;
use constellation_types::Code;
use constellation_vfs::{Ino, OpenFlags, PassthroughChunk};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Env override of a mount's passthrough wish: `1`/`0` (also `on`/`off`,
/// `true`/`false`). Read by [`crate::TransportConfig::resolve`].
pub const PASSTHROUGH_ENV: &str = "CONSTELLATION_FUSE_PASSTHROUGH";

/// Why a session does not use passthrough (`node.status`'s
/// `fuse.passthrough.unavailable_reason`, the fallback metric's `reason`).
pub mod reason {
    /// `CONSTELLATION_FUSE_PASSTHROUGH=0`, or options that never asked.
    pub const DISABLED: &str = "disabled";
    /// A writable mount without the opt-in (the module doc's "Why only
    /// read-only mounts by default").
    pub const WRITABLE_MOUNT: &str = "writable_mount";
    /// `--cache-verify always`: every byte served is hashed on the read
    /// that serves it, which a kernel-served read cannot be (plan 38 §2.3).
    pub const CACHE_VERIFY_ALWAYS: &str = "cache_verify_always";
    /// The process lacks `CAP_SYS_ADMIN`.
    pub const NO_CAP_SYS_ADMIN: &str = "no_cap_sys_admin";
    /// The kernel did not offer `FUSE_PASSTHROUGH` at `FUSE_INIT`.
    pub const KERNEL: &str = "kernel";
    /// Not Linux.
    pub const PLATFORM: &str = "platform";
    /// The backing-id ioctl failed: for the whole session when the probe
    /// at its start did ([`super::PassthroughState::probe`]), else per open.
    pub const BACKING_OPEN: &str = "backing_open";
    /// Per open: an ordinary handle of the inode is open, so the kernel
    /// would refuse a passthrough one.
    pub const CACHED_HANDLE_OPEN: &str = "cached_handle_open";
    /// Per open: the engine offered a chunk other than the one the inode's
    /// open passthrough handles read; the open is served by the daemon.
    pub const BACKING_BUSY: &str = "backing_busy";
}

/// Explanation for the once-per-session log line.
fn explain(reason: &str) -> &'static str {
    match reason {
        reason::CACHE_VERIFY_ALWAYS => {
            "--cache-verify always hashes every byte served, which a kernel-served read cannot"
        }
        reason::NO_CAP_SYS_ADMIN => {
            "the process lacks CAP_SYS_ADMIN, which registering a backing file needs"
        }
        reason::KERNEL => {
            "the kernel did not offer FUSE_PASSTHROUGH (needs Linux 6.9+ built with \
             CONFIG_FUSE_PASSTHROUGH)"
        }
        reason::PLATFORM => "passthrough is Linux-only",
        reason::WRITABLE_MOUNT => {
            "a writable mount uses it only when CONSTELLATION_FUSE_PASSTHROUGH=1"
        }
        reason::BACKING_OPEN => {
            "the kernel refused to register a file of the cache directory as a backing file \
             (EPERM: CAP_SYS_ADMIN only in a user namespace; ELOOP: the cache is on a stacked \
             filesystem such as overlayfs)"
        }
        _ => "disabled by the mount's options",
    }
}

/// The mount-level wish: on, or off and why ([`crate::TransportConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassthroughWish {
    On,
    Off(&'static str),
}

impl PassthroughWish {
    pub fn is_on(self) -> bool {
        self == Self::On
    }
}

/// The passthrough knob ([`crate::TransportConfig::passthrough`],
/// [`crate::MountOptions::passthrough`]): which mounts ask for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassthroughPolicy {
    /// The default: read-only mounts ask, writable ones do not (reason
    /// [`reason::WRITABLE_MOUNT`]) — see the module doc.
    ReadOnlyMounts,
    /// `CONSTELLATION_FUSE_PASSTHROUGH=1`: every mount asks, writable ones
    /// with the `ETXTBSY` caveat the module doc describes.
    On,
    /// No mount asks, for this reason.
    Off(&'static str),
}

impl PassthroughPolicy {
    /// The shipped default: read-only mounts, on Linux.
    pub fn platform_default() -> Self {
        if cfg!(target_os = "linux") {
            Self::ReadOnlyMounts
        } else {
            Self::Off(reason::PLATFORM)
        }
    }

    /// What a mount that is (or is not) `read_only` asks the kernel for.
    pub fn wish(self, read_only: bool) -> PassthroughWish {
        match self {
            _ if !cfg!(target_os = "linux") => PassthroughWish::Off(reason::PLATFORM),
            Self::On => PassthroughWish::On,
            Self::ReadOnlyMounts if read_only => PassthroughWish::On,
            Self::ReadOnlyMounts => PassthroughWish::Off(reason::WRITABLE_MOUNT),
            Self::Off(why) => PassthroughWish::Off(why),
        }
    }
}

/// Whether the effective capability set holds `CAP_SYS_ADMIN`
/// (`/proc/self/status`'s `CapEff`). The kernel checks `capable()`
/// against the initial user namespace; a container whose `CAP_SYS_ADMIN`
/// is only its own namespace's passes here and fails at the session's
/// probe ([`PassthroughState::probe`]), which turns passthrough off for
/// the session (logged once) rather than letting every open fall back.
pub fn has_cap_sys_admin() -> bool {
    const CAP_SYS_ADMIN: u32 = 21;
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return false;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
        .is_some_and(|caps| caps & (1 << CAP_SYS_ADMIN) != 0)
}

/// Registering and closing backing ids. The real one is the two
/// `/dev/fuse` ioctls; the wire tests substitute their own, because a
/// socket pair standing in for `/dev/fuse` has no ioctls.
#[doc(hidden)]
pub trait BackingOps: Send + Sync + 'static {
    /// `FUSE_DEV_IOC_BACKING_OPEN` of `fd` on `dev`.
    fn open(&self, dev: Option<BorrowedFd<'_>>, fd: BorrowedFd<'_>) -> std::io::Result<u32>;
    /// `FUSE_DEV_IOC_BACKING_CLOSE` of `id` on `dev`.
    fn close(&self, dev: Option<BorrowedFd<'_>>, id: u32) -> std::io::Result<()>;
}

/// The ioctls on the session's `/dev/fuse` descriptor.
pub(crate) struct DevFuseBacking;

fn no_device() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "no /dev/fuse descriptor to register backing files on",
    )
}

impl BackingOps for DevFuseBacking {
    fn open(&self, dev: Option<BorrowedFd<'_>>, fd: BorrowedFd<'_>) -> std::io::Result<u32> {
        fuser::BackingId::create_raw(dev.ok_or_else(no_device)?, fd)
    }

    #[cfg(target_os = "linux")]
    fn close(&self, dev: Option<BorrowedFd<'_>>, id: u32) -> std::io::Result<()> {
        // `FUSE_DEV_IOC_BACKING_CLOSE`: `_IOW(229, 2, uint32_t)`
        // (`include/uapi/linux/fuse.h`).
        const REQUEST: libc::Ioctl = libc::_IOW::<u32>(229, 2);
        let dev = dev.ok_or_else(no_device)?;
        // SAFETY: the request reads one `u32` through the pointer, which
        // points at a live local for the duration of the call.
        let rc = unsafe { libc::ioctl(dev.as_raw_fd(), REQUEST, &id as *const u32) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn close(&self, _: Option<BorrowedFd<'_>>, _: u32) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "backing files are Linux-only",
        ))
    }
}

/// One inode's handles on this session, as the kernel's I/O mode sees
/// them (see the module doc).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct InodeIo {
    /// Ordinary (page-cached) handles open.
    cached: u32,
    /// Write-intent opens between their pre-check and their reply.
    pending: u32,
    /// The backing every passthrough handle of the inode uses.
    backing: Option<Backing>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Backing {
    id: u32,
    hash: [u8; 32],
    /// Handles open with `FOPEN_PASSTHROUGH` (with or without
    /// `FOPEN_DIRECT_IO`).
    handles: u32,
}

impl InodeIo {
    fn idle(&self) -> bool {
        self.cached == 0 && self.pending == 0 && self.backing.is_none()
    }
}

/// What an open did before it called the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreOpen {
    /// Nothing registered: a read-only open, or a create (which learns
    /// its inode only from the reply). Decided entirely at the reply.
    Unregistered,
    /// A write-intent open registered as pending.
    Pending,
    /// A write-intent open of an inode in passthrough mode (no
    /// registration: it shares the backing, or — read-write — is refused
    /// at the reply, once the view had its say).
    Busy,
}

/// How to answer an open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenAnswer {
    /// The ordinary reply.
    Plain,
    /// `FOPEN_PASSTHROUGH` on backing `id`; `direct_io` adds
    /// `FOPEN_DIRECT_IO` (the daemon serves reads and writes).
    Passthrough { id: u32, direct_io: bool },
    /// Refuse the open (after undoing the view's side of it).
    Refuse(Code),
}

/// A session's passthrough state: the wish, the outcome of the
/// negotiation, the per-inode table and the counters `node.status` reads.
/// Shared by the adapter, its responders and the session.
pub struct PassthroughState {
    wish: Mutex<PassthroughWish>,
    enabled: AtomicBool,
    unavailable: Mutex<Option<&'static str>>,
    device: OnceLock<OwnedFd>,
    ops: Box<dyn BackingOps>,
    /// Assume `CAP_SYS_ADMIN` (tests).
    assume_cap: bool,
    table: Mutex<HashMap<Ino, InodeIo>>,
    opens: AtomicU64,
    /// Every open answered with a backing id, ever (`opens` is the live
    /// count).
    opens_total: AtomicU64,
    refused: AtomicU64,
    fallbacks: Mutex<BTreeMap<&'static str, u64>>,
    /// The transport this session's reads use when passthrough does not
    /// serve them (the fallback metric's `to`).
    transport: Mutex<&'static str>,
    /// The session's transport is not known yet (the handshake is still
    /// running): process-wide counts wait in `deferred` until it is, so
    /// their `to` label is the real one.
    transport_known: AtomicBool,
    deferred: Mutex<Vec<&'static str>>,
    logged_backing_error: AtomicBool,
}

impl std::fmt::Debug for PassthroughState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PassthroughState")
            .field("enabled", &self.enabled.load(Ordering::Relaxed))
            .field("unavailable", &*self.unavailable.lock().unwrap())
            .field("opens", &self.opens.load(Ordering::Relaxed))
            .finish()
    }
}

/// What `node.status` reports per mount (`fuse.passthrough`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassthroughStatus {
    pub enabled: bool,
    /// Handles currently open with a backing id.
    pub opens: u64,
    /// Opens answered with a backing id since the session started.
    pub opens_total: u64,
    pub unavailable_reason: Option<String>,
    /// Read-write opens refused with `ETXTBSY` (module doc).
    pub refused_opens: u64,
    /// `(from, to, reason, count)`: the downgrades counted on this session.
    pub fallbacks: Vec<(String, String, String, u64)>,
}

/// The passthrough state a session handover carries (plan 38 Z3b): every
/// inode with a handle open, with its ordinary-handle count and, when it
/// is in passthrough mode, its backing id — still registered with the
/// kernel, which goes on serving the handed-over handles from it — and
/// the chunk it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassthroughHandoff {
    /// Whether the next process may *start* backings is its own
    /// negotiation's answer (the first server's `FUSE_INIT`, its own wish,
    /// capability and probe), so it is not carried; these inodes are
    /// honoured either way.
    pub inodes: Vec<InodeHandoff>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeHandoff {
    pub ino: Ino,
    pub cached: u32,
    /// `(backing id, chunk hash, passthrough handles)`.
    pub backing: Option<(u32, [u8; 32], u32)>,
}

impl PassthroughState {
    pub(crate) fn new(wish: PassthroughWish) -> Arc<Self> {
        Self::with_ops(wish, Box::new(DevFuseBacking), false)
    }

    pub(crate) fn with_ops(
        wish: PassthroughWish,
        ops: Box<dyn BackingOps>,
        assume_cap: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            wish: Mutex::new(wish),
            enabled: AtomicBool::new(false),
            unavailable: Mutex::new(match wish {
                PassthroughWish::On => None,
                PassthroughWish::Off(why) => Some(why),
            }),
            device: OnceLock::new(),
            ops,
            assume_cap,
            table: Mutex::new(HashMap::new()),
            opens: AtomicU64::new(0),
            opens_total: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            fallbacks: Mutex::new(BTreeMap::new()),
            transport: Mutex::new("dev_fuse"),
            transport_known: AtomicBool::new(false),
            deferred: Mutex::new(Vec::new()),
            logged_backing_error: AtomicBool::new(false),
        })
    }

    pub(crate) fn set_wish(&self, wish: PassthroughWish) {
        *self.wish.lock().unwrap() = wish;
        if let PassthroughWish::Off(why) = wish {
            *self.unavailable.lock().unwrap() = Some(why);
        }
    }

    /// The session's transport, once its handshake settled it: the `to`
    /// of every fallback counted process-wide, including the ones the
    /// handshake itself took (counted now).
    pub(crate) fn set_transport(&self, name: &'static str) {
        *self.transport.lock().unwrap() = name;
        self.transport_known.store(true, Ordering::Relaxed);
        for why in std::mem::take(&mut *self.deferred.lock().unwrap()) {
            crate::stats::count_fallback("passthrough", name, why);
        }
    }

    /// The `/dev/fuse` descriptor the ioctls go to (a duplicate of the
    /// session's: same connection).
    pub(crate) fn set_device(&self, fd: OwnedFd) {
        let _ = self.device.set(fd);
    }

    /// Settle, once the session has its descriptor and before it serves,
    /// whether the kernel will really register a backing file for it:
    /// register `file` — a regular file on the filesystem the view's
    /// backing files live on ([`constellation_vfs::Vfs::passthrough_probe`])
    /// — and close the id at once. A failure (`EPERM` where `CAP_SYS_ADMIN`
    /// is only a user namespace's, `ELOOP` for a cache on overlayfs) turns
    /// passthrough off for the session with reason
    /// [`reason::BACKING_OPEN`], logged once, instead of a session that
    /// says `enabled` while every open falls back. Returns whether
    /// passthrough is still on; a session that had it off is left alone.
    pub(crate) fn probe(&self, file: std::io::Result<std::fs::File>) -> bool {
        if !self.enabled() {
            return false;
        }
        match file.and_then(|f| self.ops.open(self.device(), f.as_fd())) {
            Ok(id) => {
                if let Err(error) = self.ops.close(self.device(), id) {
                    tracing::warn!(id, %error, "passthrough: closing the probe's backing id failed");
                }
                true
            }
            Err(error) => {
                self.enabled.store(false, Ordering::Relaxed);
                self.unavailable_with(reason::BACKING_OPEN, Some(&error));
                false
            }
        }
    }

    fn device(&self) -> Option<BorrowedFd<'_>> {
        self.device.get().map(|fd| fd.as_fd())
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Whether this session should ask the kernel for passthrough, given
    /// what the kernel offers: the wish and the capability. `Err`: why not
    /// (already recorded; logged once if the mount wanted it).
    pub(crate) fn want_from_kernel(&self) -> Result<(), &'static str> {
        if let PassthroughWish::Off(why) = *self.wish.lock().unwrap() {
            return Err(why);
        }
        if !(self.assume_cap || has_cap_sys_admin()) {
            self.unavailable(reason::NO_CAP_SYS_ADMIN);
            return Err(reason::NO_CAP_SYS_ADMIN);
        }
        Ok(())
    }

    /// The negotiation's outcome: on (the kernel agreed), or off for
    /// `why`, logged once when the mount wanted it.
    pub(crate) fn settle(&self, outcome: Result<(), &'static str>) {
        match outcome {
            Ok(()) => {
                *self.unavailable.lock().unwrap() = None;
                self.enabled.store(true, Ordering::Relaxed);
                tracing::info!("FUSE passthrough enabled for single-chunk read-only opens");
            }
            Err(why) => {
                self.enabled.store(false, Ordering::Relaxed);
                if self.wish.lock().unwrap().is_on() {
                    self.unavailable(why);
                } else {
                    *self.unavailable.lock().unwrap() = Some(why);
                }
            }
        }
    }

    /// A mount that wanted passthrough cannot have it: logged once (each
    /// session settles once), counted, reported.
    fn unavailable(&self, why: &'static str) {
        self.unavailable_with(why, None);
    }

    /// [`Self::unavailable`], naming the error that decided it.
    fn unavailable_with(&self, why: &'static str, error: Option<&std::io::Error>) {
        let mut slot = self.unavailable.lock().unwrap();
        if *slot == Some(why) {
            return;
        }
        *slot = Some(why);
        drop(slot);
        match error {
            Some(error) => tracing::warn!(
                reason = why,
                %error,
                "passthrough unavailable: {why}: {}",
                explain(why)
            ),
            None => tracing::warn!(
                reason = why,
                "passthrough unavailable: {why}: {}",
                explain(why)
            ),
        }
        self.count_fallback(why);
    }

    /// Counted here (this session's, for its own status) and process-wide
    /// (`constellation_fuse_transport_fallbacks_total`, which must not step
    /// back when the mount goes away).
    fn count_fallback(&self, why: &'static str) {
        *self.fallbacks.lock().unwrap().entry(why).or_insert(0) += 1;
        if self.transport_known.load(Ordering::Relaxed) {
            crate::stats::count_fallback("passthrough", *self.transport.lock().unwrap(), why);
        } else {
            self.deferred.lock().unwrap().push(why);
        }
    }

    /// Before the view is asked to open `ino` with `flags`. Never
    /// refuses: a refusal waits for the reply ([`Self::on_open_reply`]),
    /// so the view's own errno wins over it.
    pub(crate) fn before_open(&self, ino: Ino, flags: OpenFlags) -> PreOpen {
        if !open_flags_write_intent(flags) {
            return PreOpen::Unregistered;
        }
        let mut table = self.table.lock().unwrap();
        let io = table.entry(ino).or_default();
        if io.backing.is_some() {
            return PreOpen::Busy;
        }
        io.pending += 1;
        PreOpen::Pending
    }

    fn refuse_rdwr(&self, ino: Ino) {
        self.refused.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            ino,
            "passthrough: a read-write open of an inode open in passthrough mode is refused \
             (ETXTBSY) until those handles close"
        );
    }

    /// The view answered the open of `ino` with `backing`: how to reply.
    /// Updates the table as if the reply was sent (it is, right after,
    /// on this thread), so a `release` can never find it missing.
    pub(crate) fn on_open_reply(
        &self,
        ino: Ino,
        flags: OpenFlags,
        pre: PreOpen,
        backing: Option<&PassthroughChunk>,
    ) -> OpenAnswer {
        let write_intent = open_flags_write_intent(flags);
        let mut table = self.table.lock().unwrap();
        let io = table.entry(ino).or_default();
        if pre == PreOpen::Pending {
            io.pending = io.pending.saturating_sub(1);
        }
        if let Some(b) = io.backing.as_mut() {
            if flags.contains(OpenFlags::READ | OpenFlags::WRITE) {
                // See the module doc for why this is refused, and why
                // here rather than before the view was asked. The inode
                // has a backing, so its entry stays.
                drop(table);
                self.refuse_rdwr(ino);
                return OpenAnswer::Refuse(Code::TextBusy);
            }
            b.handles += 1;
            self.opens.fetch_add(1, Ordering::Relaxed);
            self.opens_total.fetch_add(1, Ordering::Relaxed);
            let same = backing.is_some_and(|c| c.hash == b.hash) && !write_intent;
            let id = b.id;
            if backing.is_some() && !same && !write_intent {
                drop(table);
                self.count_fallback(reason::BACKING_BUSY);
            }
            return OpenAnswer::Passthrough {
                id,
                direct_io: !same,
            };
        }
        let eligible = backing.filter(|_| !write_intent && self.enabled());
        let Some(chunk) = eligible else {
            io.cached += 1;
            return OpenAnswer::Plain;
        };
        if io.cached > 0 || io.pending > 0 {
            io.cached += 1;
            drop(table);
            self.count_fallback(reason::CACHED_HANDLE_OPEN);
            return OpenAnswer::Plain;
        }
        match self.ops.open(self.device(), chunk.fd.as_fd()) {
            Ok(id) => {
                io.backing = Some(Backing {
                    id,
                    hash: chunk.hash,
                    handles: 1,
                });
                self.opens.fetch_add(1, Ordering::Relaxed);
                self.opens_total.fetch_add(1, Ordering::Relaxed);
                OpenAnswer::Passthrough {
                    id,
                    direct_io: false,
                }
            }
            Err(error) => {
                io.cached += 1;
                drop(table);
                // Once per session at warn (it is most likely the same
                // cause every time: a cache directory on a stacked
                // filesystem, `ELOOP`, or a namespace-only
                // `CAP_SYS_ADMIN`, `EPERM`); every one is counted.
                if !self.logged_backing_error.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        ino,
                        %error,
                        "passthrough: registering a backing file failed; serving such opens \
                         through the daemon (logged once per mount)"
                    );
                } else {
                    tracing::debug!(ino, %error, "passthrough: registering a backing file failed");
                }
                self.count_fallback(reason::BACKING_OPEN);
                OpenAnswer::Plain
            }
        }
    }

    /// A write-intent open that registered as pending never reached its
    /// reply (the view refused it).
    pub(crate) fn open_failed(&self, ino: Ino, pre: PreOpen) {
        if pre != PreOpen::Pending {
            return;
        }
        let mut table = self.table.lock().unwrap();
        if let Some(io) = table.get_mut(&ino) {
            io.pending = io.pending.saturating_sub(1);
            if io.idle() {
                table.remove(&ino);
            }
        }
    }

    /// A handle of `ino` was released. Within one inode every open handle
    /// is in the same mode (the module doc's rule, which this table
    /// enforces), so the inode's mode says which count it was.
    pub(crate) fn release(&self, ino: Ino) {
        let close = {
            let mut table = self.table.lock().unwrap();
            let Some(io) = table.get_mut(&ino) else {
                return;
            };
            let mut close = None;
            match io.backing.as_mut() {
                Some(b) => {
                    b.handles = b.handles.saturating_sub(1);
                    self.opens.fetch_sub(1, Ordering::Relaxed);
                    if b.handles == 0 {
                        close = Some(b.id);
                        io.backing = None;
                    }
                }
                None => io.cached = io.cached.saturating_sub(1),
            }
            if io.idle() {
                table.remove(&ino);
            }
            close
        };
        if let Some(id) = close {
            if let Err(error) = self.ops.close(self.device(), id) {
                // The kernel frees the id with the connection either way.
                tracing::warn!(ino, id, %error, "passthrough: closing a backing id failed");
            }
        }
    }

    pub fn status(&self) -> PassthroughStatus {
        let to = *self.transport.lock().unwrap();
        PassthroughStatus {
            enabled: self.enabled(),
            opens: self.opens.load(Ordering::Relaxed),
            opens_total: self.opens_total.load(Ordering::Relaxed),
            unavailable_reason: self.unavailable.lock().unwrap().map(str::to_string),
            refused_opens: self.refused.load(Ordering::Relaxed),
            fallbacks: self
                .fallbacks
                .lock()
                .unwrap()
                .iter()
                .map(|(why, n)| {
                    (
                        "passthrough".to_string(),
                        to.to_string(),
                        why.to_string(),
                        *n,
                    )
                })
                .collect(),
        }
    }

    /// The table, for a handover (taken once the workers stopped).
    pub(crate) fn export(&self) -> PassthroughHandoff {
        let mut inodes: Vec<InodeHandoff> = self
            .table
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, io)| io.cached > 0 || io.backing.is_some())
            .map(|(ino, io)| InodeHandoff {
                ino: *ino,
                cached: io.cached,
                backing: io.backing.map(|b| (b.id, b.hash, b.handles)),
            })
            .collect();
        inodes.sort_unstable_by_key(|i| i.ino);
        PassthroughHandoff { inodes }
    }

    /// Adopt a handed-over table (a resumed session, before it serves).
    pub(crate) fn import(&self, handoff: &PassthroughHandoff) {
        let mut table = self.table.lock().unwrap();
        let mut opens = 0;
        for i in &handoff.inodes {
            let backing = i.backing.map(|(id, hash, handles)| {
                opens += u64::from(handles);
                Backing { id, hash, handles }
            });
            table.insert(
                i.ino,
                InodeIo {
                    cached: i.cached,
                    pending: 0,
                    backing,
                },
            );
        }
        self.opens.fetch_add(opens, Ordering::Relaxed);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[derive(Default)]
    struct Fake {
        next: AtomicU32,
        closed: Mutex<Vec<u32>>,
        fail: AtomicBool,
    }

    impl BackingOps for Arc<Fake> {
        fn open(&self, _: Option<BorrowedFd<'_>>, _: BorrowedFd<'_>) -> std::io::Result<u32> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(std::io::Error::from_raw_os_error(libc::ELOOP));
            }
            Ok(self.next.fetch_add(1, Ordering::SeqCst) + 1)
        }
        fn close(&self, _: Option<BorrowedFd<'_>>, id: u32) -> std::io::Result<()> {
            self.closed.lock().unwrap().push(id);
            Ok(())
        }
    }

    fn state() -> (Arc<PassthroughState>, Arc<Fake>) {
        let fake = Arc::new(Fake::default());
        let st = PassthroughState::with_ops(PassthroughWish::On, Box::new(fake.clone()), true);
        st.want_from_kernel().unwrap();
        st.settle(Ok(()));
        (st, fake)
    }

    fn chunk(byte: u8) -> PassthroughChunk {
        PassthroughChunk {
            fd: Arc::new(std::fs::File::open("/dev/null").unwrap()),
            len: 1,
            hash: [byte; 32],
        }
    }

    const RO: OpenFlags = OpenFlags::READ;
    const WO: OpenFlags = OpenFlags::WRITE;
    const RW: OpenFlags = OpenFlags::READ.union(OpenFlags::WRITE);

    fn open(
        st: &PassthroughState,
        ino: Ino,
        flags: OpenFlags,
        c: Option<&PassthroughChunk>,
    ) -> OpenAnswer {
        let pre = st.before_open(ino, flags);
        st.on_open_reply(ino, flags, pre, c)
    }

    #[test]
    fn one_backing_per_inode_closed_exactly_once_at_the_last_release() {
        let (st, fake) = state();
        let a = chunk(1);
        assert_eq!(
            open(&st, 5, RO, Some(&a)),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: false
            }
        );
        // A second eligible open of the same chunk reuses the id.
        assert_eq!(
            open(&st, 5, RO, Some(&a)),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: false
            }
        );
        assert_eq!(st.status().opens, 2);
        st.release(5);
        assert!(
            fake.closed.lock().unwrap().is_empty(),
            "one handle still open"
        );
        st.release(5);
        assert_eq!(*fake.closed.lock().unwrap(), vec![1]);
        assert_eq!(st.status().opens, 0);
        // A stray release (nothing open) closes nothing more.
        st.release(5);
        assert_eq!(*fake.closed.lock().unwrap(), vec![1]);
    }

    #[test]
    fn opens_after_a_passthrough_one_share_its_backing_or_are_refused() {
        let (st, _fake) = state();
        let a = chunk(1);
        let b = chunk(2);
        open(&st, 5, RO, Some(&a));
        // The engine offers nothing (a writer wrote) or another chunk (a
        // remote write landed): served by the daemon on the same backing.
        assert_eq!(
            open(&st, 5, RO, None),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: true
            }
        );
        assert_eq!(
            open(&st, 5, RO, Some(&b)),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: true
            }
        );
        // Write-only: the same, and it cannot be mmapped at all.
        assert_eq!(
            open(&st, 5, WO, None),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: true
            }
        );
        // Read-write: ETXTBSY.
        assert_eq!(open(&st, 5, RW, None), OpenAnswer::Refuse(Code::TextBusy));
        let status = st.status();
        assert_eq!((status.opens, status.refused_opens), (4, 1));
        assert!(status
            .fallbacks
            .iter()
            .any(|(_, _, why, n)| why == reason::BACKING_BUSY && *n == 1));
    }

    /// Review 38-z3b must-fix 2: the read-write refusal is made at the
    /// reply, so a view that refuses the open itself is heard first.
    #[test]
    fn a_read_write_open_is_refused_only_after_the_view_accepted_it() {
        let (st, fake) = state();
        open(&st, 5, RO, Some(&chunk(1)));
        // The view refuses (EROFS, ENOENT, EACCES): its answer stands, no
        // ETXTBSY is counted, and the inode keeps its backing.
        let pre = st.before_open(5, RW);
        assert_eq!(pre, PreOpen::Busy);
        st.open_failed(5, pre);
        assert_eq!(st.status().refused_opens, 0);
        assert_eq!(st.status().opens, 1);
        // The view accepts: ETXTBSY at the reply (the adapter releases
        // the view's handle), and the inode is left as it was.
        let pre = st.before_open(5, RW);
        assert_eq!(
            st.on_open_reply(5, RW, pre, None),
            OpenAnswer::Refuse(Code::TextBusy)
        );
        let status = st.status();
        assert_eq!((status.opens, status.refused_opens), (1, 1));
        st.release(5);
        assert_eq!(*fake.closed.lock().unwrap(), vec![1]);
        assert!(st.export().inodes.is_empty(), "nothing left behind");
    }

    #[test]
    fn the_probe_settles_a_session_whose_kernel_refuses_backing_files() {
        // Registered and closed at once: passthrough stays on.
        let (st, fake) = state();
        assert!(st.probe(std::fs::File::open("/dev/null")));
        assert!(st.enabled());
        assert_eq!(*fake.closed.lock().unwrap(), vec![1]);
        assert!(st.status().fallbacks.is_empty());
        // Refused (a user namespace's CAP_SYS_ADMIN, a cache on
        // overlayfs): off for the session, with the reason, counted once.
        let (st, fake) = state();
        fake.fail.store(true, Ordering::SeqCst);
        assert!(!st.probe(std::fs::File::open("/dev/null")));
        assert!(!st.enabled());
        let status = st.status();
        assert_eq!(
            status.unavailable_reason.as_deref(),
            Some(reason::BACKING_OPEN)
        );
        assert_eq!(status.fallbacks.len(), 1);
        // Its opens are plain from now on, and nothing more is counted.
        fake.fail.store(false, Ordering::SeqCst);
        assert_eq!(open(&st, 2, RO, Some(&chunk(1))), OpenAnswer::Plain);
        assert_eq!(st.status().fallbacks, status.fallbacks);
        // No probe file at all (the cache directory refused one) is the
        // same answer.
        let (st, _) = state();
        assert!(!st.probe(Err(std::io::Error::from_raw_os_error(libc::EACCES))));
        assert_eq!(
            st.status().unavailable_reason.as_deref(),
            Some(reason::BACKING_OPEN)
        );
        // A session that never had it is not probed.
        let fake = Arc::new(Fake::default());
        let off = PassthroughState::with_ops(
            PassthroughWish::Off(reason::WRITABLE_MOUNT),
            Box::new(fake.clone()),
            true,
        );
        off.settle(Err(reason::WRITABLE_MOUNT));
        assert!(!off.probe(std::fs::File::open("/dev/null")));
        assert_eq!(fake.next.load(Ordering::SeqCst), 0);
        assert_eq!(
            off.status().unavailable_reason.as_deref(),
            Some(reason::WRITABLE_MOUNT)
        );
    }

    /// Review 38-z3b must-fix 1: on by default only for read-only mounts.
    #[test]
    fn the_default_asks_only_for_read_only_mounts() {
        let d = PassthroughPolicy::platform_default();
        assert_eq!(d, PassthroughPolicy::ReadOnlyMounts);
        assert_eq!(d.wish(true), PassthroughWish::On);
        assert_eq!(d.wish(false), PassthroughWish::Off(reason::WRITABLE_MOUNT));
        assert_eq!(PassthroughPolicy::On.wish(false), PassthroughWish::On);
        assert_eq!(PassthroughPolicy::On.wish(true), PassthroughWish::On);
        let off = PassthroughPolicy::Off(reason::CACHE_VERIFY_ALWAYS);
        assert_eq!(
            off.wish(true),
            PassthroughWish::Off(reason::CACHE_VERIFY_ALWAYS)
        );
    }

    #[test]
    fn an_ordinary_handle_keeps_the_inode_out_of_passthrough() {
        let (st, fake) = state();
        let a = chunk(1);
        assert_eq!(open(&st, 9, RW, None), OpenAnswer::Plain);
        assert_eq!(open(&st, 9, RO, Some(&a)), OpenAnswer::Plain);
        st.release(9);
        st.release(9);
        assert_eq!(
            open(&st, 9, RO, Some(&a)),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: false
            },
            "all closed: passthrough again"
        );
        assert!(fake.closed.lock().unwrap().is_empty());
    }

    #[test]
    fn a_pending_writer_blocks_a_passthrough_open_racing_it() {
        let (st, _fake) = state();
        let a = chunk(1);
        let pre = st.before_open(3, RW);
        assert_eq!(pre, PreOpen::Pending);
        // The reader's reply lands between the writer's call and its reply.
        assert_eq!(open(&st, 3, RO, Some(&a)), OpenAnswer::Plain);
        assert_eq!(st.on_open_reply(3, RW, pre, None), OpenAnswer::Plain);
        // A refused write-intent open leaves nothing pending.
        let pre = st.before_open(4, WO);
        st.open_failed(4, pre);
        assert_eq!(
            open(&st, 4, RO, Some(&a)),
            OpenAnswer::Passthrough {
                id: 1,
                direct_io: false
            }
        );
    }

    #[test]
    fn a_failed_registration_falls_back_and_counts() {
        let (st, fake) = state();
        fake.fail.store(true, Ordering::SeqCst);
        assert_eq!(open(&st, 2, RO, Some(&chunk(1))), OpenAnswer::Plain);
        let status = st.status();
        assert_eq!(status.opens, 0);
        assert_eq!(
            status.fallbacks,
            vec![(
                "passthrough".to_string(),
                "dev_fuse".to_string(),
                reason::BACKING_OPEN.to_string(),
                1
            )]
        );
    }

    #[test]
    fn a_disabled_session_ignores_backing_but_honours_handed_over_inodes() {
        let fake = Arc::new(Fake::default());
        let st = PassthroughState::with_ops(
            PassthroughWish::Off(reason::DISABLED),
            Box::new(fake.clone()),
            true,
        );
        assert_eq!(st.want_from_kernel(), Err(reason::DISABLED));
        st.settle(Err(reason::DISABLED));
        assert_eq!(open(&st, 1, RO, Some(&chunk(1))), OpenAnswer::Plain);
        // Not a downgrade: nothing counted.
        assert!(st.status().fallbacks.is_empty());
        assert_eq!(
            st.status().unavailable_reason.as_deref(),
            Some(reason::DISABLED)
        );
        // An inode a previous process left in passthrough mode still gets
        // its backing (the kernel would answer anything else EIO), and its
        // id is closed by this process at the last release.
        st.import(&PassthroughHandoff {
            inodes: vec![InodeHandoff {
                ino: 7,
                cached: 0,
                backing: Some((42, [3; 32], 1)),
            }],
        });
        assert_eq!(st.status().opens, 1);
        assert_eq!(
            open(&st, 7, RO, Some(&chunk(3))),
            OpenAnswer::Passthrough {
                id: 42,
                direct_io: false
            }
        );
        st.release(7);
        st.release(7);
        assert_eq!(*fake.closed.lock().unwrap(), vec![42]);
    }

    #[test]
    fn the_table_round_trips_a_handover() {
        let (st, _) = state();
        open(&st, 5, RO, Some(&chunk(1)));
        open(&st, 6, RW, None);
        let out = st.export();
        assert_eq!(
            out.inodes,
            vec![
                InodeHandoff {
                    ino: 5,
                    cached: 0,
                    backing: Some((1, [1; 32], 1))
                },
                InodeHandoff {
                    ino: 6,
                    cached: 1,
                    backing: None
                },
            ]
        );
        let json = serde_json::to_string(&out).unwrap();
        assert_eq!(
            serde_json::from_str::<PassthroughHandoff>(&json).unwrap(),
            out
        );
        let (next, fake) = state();
        next.import(&out);
        assert_eq!(next.export(), out);
        // The handed-over ordinary handle still keeps inode 6 out of
        // passthrough in the next process.
        assert_eq!(open(&next, 6, RO, Some(&chunk(2))), OpenAnswer::Plain);
        next.release(5);
        assert_eq!(*fake.closed.lock().unwrap(), vec![1]);
    }

    #[test]
    fn cap_sys_admin_is_read_from_the_effective_set() {
        // Whatever this process has, the answer agrees with `CapEff`.
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let eff = status
            .lines()
            .find_map(|l| l.strip_prefix("CapEff:"))
            .map(|h| u64::from_str_radix(h.trim(), 16).unwrap())
            .unwrap();
        assert_eq!(has_cap_sys_admin(), eff & (1 << 21) != 0);
    }
}
