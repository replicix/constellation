//! Who is asking, and about which op: [`OpCtx`], [`Caller`].

use smallvec::SmallVec;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Instant;

/// One op's identity, unique within the process (for tracing and the
/// watchdog).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OpId(pub u64);

impl OpId {
    /// The next id (one relaxed atomic increment).
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

macro_rules! op_kinds {
    ($($kind:ident => $name:literal,)*) => {
        /// Which op of the [`crate::Vfs`] trait.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum OpKind {
            $($kind,)*
        }

        impl OpKind {
            /// Every op, in trait order.
            pub const ALL: &'static [OpKind] = &[$(OpKind::$kind,)*];

            /// The op's name, as logs and the watchdog spell it.
            pub const fn name(self) -> &'static str {
                match self {
                    $(OpKind::$kind => $name,)*
                }
            }
        }
    };
}

op_kinds! {
    Lookup => "lookup",
    Getattr => "getattr",
    Setattr => "setattr",
    Readlink => "readlink",
    Mknod => "mknod",
    Mkdir => "mkdir",
    Symlink => "symlink",
    Link => "link",
    Unlink => "unlink",
    Rmdir => "rmdir",
    Rename => "rename",
    Open => "open",
    Create => "create",
    Read => "read",
    Write => "write",
    Flush => "flush",
    Release => "release",
    Fsync => "fsync",
    Readdir => "readdir",
    Statfs => "statfs",
    Fallocate => "fallocate",
    Seek => "lseek",
    Getxattr => "getxattr",
    Setxattr => "setxattr",
    Listxattr => "listxattr",
    Removexattr => "removexattr",
    LockTest => "getlk",
    LockAcquire => "setlk",
    LockRelease => "setlk",
    SyncView => "sync_view",
}

/// A set of [`OpKind`]s (e.g. the ops a frontend can answer from another
/// thread, [`crate::FrontendCaps::deferrable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OpKindSet(u64);

impl OpKindSet {
    pub const EMPTY: OpKindSet = OpKindSet(0);
    pub const ALL: OpKindSet = {
        let mut bits = 0u64;
        let mut i = 0;
        while i < OpKind::ALL.len() {
            bits |= 1 << OpKind::ALL[i] as u8;
            i += 1;
        }
        OpKindSet(bits)
    };

    pub const fn of(kinds: &[OpKind]) -> Self {
        let mut set = Self::EMPTY;
        let mut i = 0;
        while i < kinds.len() {
            set = set.with(kinds[i]);
            i += 1;
        }
        set
    }

    pub const fn with(self, kind: OpKind) -> Self {
        Self(self.0 | 1 << kind as u8)
    }

    pub const fn contains(self, kind: OpKind) -> bool {
        self.0 & (1 << kind as u8) != 0
    }

    pub fn iter(self) -> impl Iterator<Item = OpKind> {
        OpKind::ALL
            .iter()
            .copied()
            .filter(move |k| self.contains(*k))
    }
}

/// The caller's identity in its platform's terms, for an [`IdentityMap`]
/// to map to POSIX. Only POSIX today; Windows SIDs (plan 35) and Android
/// app signatures (plan 36) join later.
///
/// [`IdentityMap`]: crate::IdentityMap
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Principal {
    Posix { uid: u32, gid: u32 },
}

/// Who is asking: a POSIX identity (uid, primary gid, supplementary gids)
/// plus the pid when the frontend knows it.
///
/// **The supplementary groups are resolved lazily, once.** Plan 31 §6.1
/// sketches `gids` gathered eagerly at the frontend edge; for FUSE that
/// would read `/proc/<pid>/status` on every request, while today only a
/// permission check that needs a group (a create that found its name
/// existing and opens it, `View::create_or_open`) reads it. So a FUSE
/// caller carries uid + primary gid + pid, and [`Caller::in_group`] asks
/// the host (`constellation_platform`'s `Process::supplementary_groups`)
/// the first time a group beyond the primary one matters, caching the
/// answer for the rest of the op. A frontend that is handed the full list
/// up front (NFS `AUTH_SYS`, a Windows token) uses
/// [`Caller::with_groups`] and nothing is read.
#[derive(Debug)]
pub struct Caller {
    pub uid: u32,
    pub gid: u32,
    /// `None`: unknown (FUSE sends 0 for a kernel-internal request).
    pub pid: Option<u32>,
    pub principal: Principal,
    groups: OnceLock<SmallVec<[u32; 16]>>,
}

impl Caller {
    /// A caller whose supplementary groups are read from the host (by
    /// `pid`) if and when a check needs them; none without a pid.
    pub fn new(uid: u32, gid: u32, pid: Option<u32>) -> Self {
        Self {
            uid,
            gid,
            pid,
            principal: Principal::Posix { uid, gid },
            groups: OnceLock::new(),
        }
    }

    /// A caller whose supplementary groups the frontend already knows.
    pub fn with_groups(uid: u32, gid: u32, gids: &[u32]) -> Self {
        let caller = Self::new(uid, gid, None);
        let _ = caller.groups.set(SmallVec::from_slice(gids));
        caller
    }

    /// uid 0, gid 0, no groups (tests, engine-internal ops).
    pub fn root() -> Self {
        Self::with_groups(0, 0, &[])
    }

    /// The supplementary groups (read from the host once, on first use;
    /// empty when they cannot be read).
    pub fn supplementary(&self) -> &[u32] {
        self.groups.get_or_init(|| match self.pid {
            None => SmallVec::new(),
            Some(pid) => constellation_platform::native()
                .process
                .supplementary_groups(pid)
                .map(SmallVec::from_vec)
                .unwrap_or_default(),
        })
    }

    /// Whether the caller is in `gid`: its primary group, or one of its
    /// supplementary groups.
    pub fn in_group(&self, gid: u32) -> bool {
        gid == self.gid || self.supplementary().contains(&gid)
    }
}

/// Cancellation of an op in flight: set by the frontend (a WinFsp
/// cancel, an NFS disconnect, a control client's `Cancel` frame), checked
/// by the engine at its wait points.
///
/// **Linux FUSE sets it for `fsync`/`fsyncdir` and `O_SYNC`/`O_DSYNC`
/// writes only for a dying caller** (plan 39 §3.3: the vendored fuser
/// delivers `FUSE_INTERRUPT`; the adapter cancels the request it names
/// once the calling thread has a fatal signal pending — killable, as NFS
/// `hard`, not interruptible by a handled signal), **and for a blocked
/// `F_SETLKW`/`flock` on any signal** (interruptible, as POSIX has it: the
/// wait answers `EINTR`).
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// One op's context: its id and kind, who asks, and the bounds on it.
///
/// Threading contract (plan 31 §6.4): an op may be called from any thread
/// that is not a tokio runtime worker — the engine parks the calling
/// thread on its bounded waits (and its `Runtime::block_on` refuses a
/// runtime thread outright). Frontends own their dispatch threads.
pub struct OpCtx<'a> {
    pub op: OpId,
    pub kind: OpKind,
    pub caller: &'a Caller,
    /// The kernel's lock owner of the request, where it says (FUSE: a
    /// direct-I/O read or write; the POSIX owner, the caller's file
    /// table): an `fcntl` lock owner whose cluster-lock grant lapsed is
    /// fenced by it even when the pid does not tell (plan 30 §M14). Never
    /// a `flock` owner (the open file).
    pub lock_owner: Option<u64>,
    pub deadline: Option<Instant>,
    pub cancel: Option<&'a CancelToken>,
    pub span: &'a tracing::Span,
}

static NO_SPAN: LazyLock<tracing::Span> = LazyLock::new(tracing::Span::none);

impl<'a> OpCtx<'a> {
    /// A fresh op: a new [`OpId`], no deadline, no cancellation, no span.
    pub fn new(kind: OpKind, caller: &'a Caller) -> Self {
        Self {
            op: OpId::next(),
            kind,
            caller,
            lock_owner: None,
            deadline: None,
            cancel: None,
            span: &NO_SPAN,
        }
    }

    pub fn with_lock_owner(mut self, owner: Option<u64>) -> Self {
        self.lock_owner = owner;
        self
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_cancel(mut self, cancel: &'a CancelToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    pub fn with_span(mut self, span: &'a tracing::Span) -> Self {
        self.span = span;
        self
    }

    /// Whether the frontend cancelled this op.
    pub fn cancelled(&self) -> bool {
        self.cancel.is_some_and(CancelToken::is_cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_kind_sets_hold_exactly_what_was_put_in() {
        let set = OpKindSet::of(&[OpKind::Read, OpKind::Write]);
        assert!(set.contains(OpKind::Read) && set.contains(OpKind::Write));
        assert!(!set.contains(OpKind::Readdir));
        assert_eq!(set.iter().count(), 2);
        assert_eq!(OpKindSet::ALL.iter().count(), OpKind::ALL.len());
        assert!(OpKind::ALL.len() <= 64);
    }

    #[test]
    fn a_caller_with_known_groups_never_asks_the_host() {
        let c = Caller::with_groups(1000, 100, &[7, 8]);
        assert!(c.in_group(100));
        assert!(c.in_group(8));
        assert!(!c.in_group(9));
        let none = Caller::new(1000, 100, None);
        assert!(none.in_group(100));
        assert!(!none.in_group(7));
    }

    #[test]
    fn ops_get_distinct_ids_and_cancellation_is_seen() {
        let c = Caller::root();
        let token = CancelToken::new();
        let a = OpCtx::new(OpKind::Read, &c);
        let b = OpCtx::new(OpKind::Read, &c).with_cancel(&token);
        assert_ne!(a.op, b.op);
        assert!(!b.cancelled());
        token.cancel();
        assert!(b.cancelled());
        assert!(!a.cancelled());
    }
}
