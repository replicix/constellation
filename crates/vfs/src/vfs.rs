//! The [`Vfs`] trait: the ops a frontend drives.

use crate::ctx::OpCtx;
use crate::name::{Name, XattrName, XattrNameBuf};
use crate::observe::ViewIdentity;
use crate::responder::{DirSink, Responder};
use crate::types::{
    Attr, Durability, Entry, FallocateMode, Fh, Ino, LockOwner, LockRange, LockSpec, LockStatus,
    OpenFlags, OpenOwner, Opened, ReadData, RenameFlags, SeekWhence, SetAttr, SetXattrFlags,
    StatFs, WriteData,
};
use constellation_types::Rdev;

/// One mounted view of a filesystem, as every frontend sees it.
///
/// **Completion.** Every op completes exactly once, through its responder
/// `r` (see [`crate::responder`] for the exactly-once and drop rules). A
/// fast path completes it inline on the calling thread; the responder is
/// a generic parameter, so an inline completion is monomorphised per
/// frontend — no allocation, no dynamic dispatch. An op that waits on
/// something unbounded moves `r` to another thread and returns at once:
/// today a blocking lock (`lock_acquire` with `sleep`), and only for
/// frontends whose [`crate::FrontendCaps::deferrable`] allows it; every
/// other wait (a lease acquisition, a chunk fetch, the core's reply)
/// parks the calling thread, bounded, exactly as before plan 31 — their
/// conversion to deferral is incremental (plan 31 §6.3, C7).
///
/// **Everything is decided beneath this trait.** The inode renumbering of
/// a subtree or snapshot view, the synthetic `.constellation` tree, the
/// scratch directories, name and xattr policy, the pending-write size
/// overlay, the lease gate, `cto=strict`'s read waits, lock fencing, the
/// per-inode op ordering — none of it is a frontend's to repeat. A
/// frontend decodes its protocol into these arguments and encodes the
/// results; nothing else.
///
/// **Addressing.** Inodes are the view's numbering ([`crate::ROOT_INO`] is
/// the view's root). The file ops take the inode *and* the handle `open`
/// gave, as FUSE delivers them; the view addresses by the inode, as it
/// always has, and the handle is carried for the handle table a session
/// handover exports (plan 31 §6.11).
pub trait Vfs: Send + Sync + 'static {
    /// Who this view is, for the frontend's metrics and tracing spans
    /// ([`crate::Observer`]): its number and its allowlisted metric
    /// label. Anonymous by default (a mock, a test double).
    fn identity(&self) -> ViewIdentity {
        ViewIdentity::default()
    }

    /// The frontend serving this view has settled with its kernel what it
    /// can actually do, which may be less (or more) than the
    /// [`crate::FrontendCaps`] it declared before it existed. Only
    /// [`crate::FrontendCaps::passthrough`] is read today: Linux FUSE
    /// learns at `FUSE_INIT` whether it can register backing files (plan
    /// 38 Z3b), after the view was opened. Called before the frontend
    /// serves anything, and again by a resumed session. Ignored by
    /// default.
    fn frontend_negotiated(&self, caps: &crate::FrontendCaps) {
        let _ = caps;
    }

    /// The frontend's platform no longer references `ino` (Linux FUSE:
    /// `FORGET`, sent when the kernel evicts the inode). What a view
    /// kept to answer for an inode it removed while still referenced (an
    /// `O_PATH` descriptor, a removed directory that is a process's
    /// working directory) can go. Ignored by default.
    fn forget(&self, ino: Ino) {
        let _ = ino;
    }

    /// A regular file on the filesystem this view's backing files
    /// ([`crate::Opened::backing`]) live on, for a frontend to check once,
    /// before it serves, that its kernel will really accept one (Linux
    /// FUSE registers it as a passthrough backing file and closes it at
    /// once, plan 38 Z3b: a user namespace's `CAP_SYS_ADMIN` or a stacked
    /// filesystem is refused there and nowhere earlier). `None`: this view
    /// never hands out backing files, so there is nothing to check.
    fn passthrough_probe(&self) -> Option<std::io::Result<std::fs::File>> {
        None
    }

    /// Resolve `name` in `parent`.
    fn lookup<R: Responder<Entry>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R);

    fn getattr<R: Responder<Attr>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Option<Fh>, r: R);

    fn setattr<R: Responder<Attr>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Option<Fh>,
        set: &SetAttr,
        r: R,
    );

    /// A symlink's target.
    fn readlink<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R);

    /// Create a node; `mode` carries the POSIX file-type bits
    /// ([`crate::types::mode`]) and the permissions.
    #[allow(clippy::too_many_arguments)]
    fn mknod<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        rdev: Rdev,
        r: R,
    );

    fn mkdir<R: Responder<Entry>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, mode: u32, r: R);

    fn symlink<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        target: &[u8],
        r: R,
    );

    /// A second name for `ino`.
    fn link<R: Responder<Entry>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        new_parent: Ino,
        new_name: &Name,
        r: R,
    );

    fn unlink<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R);

    fn rmdir<R: Responder<()>>(&self, cx: &OpCtx<'_>, parent: Ino, name: &Name, r: R);

    #[allow(clippy::too_many_arguments)]
    fn rename<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        new_parent: Ino,
        new_name: &Name,
        flags: RenameFlags,
        r: R,
    );

    /// Open an existing inode.
    ///
    /// The answer may carry a backing file ([`Opened::backing`]) the
    /// frontend's kernel is allowed to read this handle's data from
    /// directly; a frontend that cannot do that ignores it. It is decided
    /// once, here, and lives until this handle's [`Vfs::release`].
    fn open<R: Responder<Opened>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        flags: OpenFlags,
        owner: OpenOwner,
        r: R,
    );

    /// `open(O_CREAT)`: create `name`, or open what is there without
    /// `EXCL` (a race another node won).
    #[allow(clippy::too_many_arguments)]
    fn create<R: Responder<(Entry, Opened)>>(
        &self,
        cx: &OpCtx<'_>,
        parent: Ino,
        name: &Name,
        mode: u32,
        flags: OpenFlags,
        owner: OpenOwner,
        r: R,
    );

    /// Up to `len` bytes at `off` (fewer at the end of the file).
    #[allow(clippy::too_many_arguments)]
    fn read<R: Responder<ReadData>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u32,
        r: R,
    );

    /// Write `data` at `off`; `flags` are the handle's open flags (`SYNC`
    /// publishes before answering). Answers the bytes written.
    #[allow(clippy::too_many_arguments)]
    fn write<R: Responder<u32>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        data: WriteData<'_>,
        flags: OpenFlags,
        r: R,
    );

    /// The close-time flush fence (the close-to-open publication point,
    /// plan 30 `cto=strict`), per `close(2)`; drops `owner`'s locks.
    fn flush<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, owner: LockOwner, r: R);

    /// The last close of a handle (`owner`: its `flock` owner). Ends the
    /// lifetime of whatever [`Opened::backing`] the open handed out.
    #[allow(clippy::too_many_arguments)]
    fn release<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        flags: OpenFlags,
        owner: Option<LockOwner>,
        r: R,
    );

    /// The durability barrier.
    fn fsync<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, fh: Fh, level: Durability, r: R);

    /// List `ino` from `cookie` on (0: the start; each entry carries the
    /// cookie to resume after it) into `r` until it is full, then answer.
    #[allow(clippy::too_many_arguments)]
    fn readdir<R: DirSink + Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        cookie: u64,
        plus: bool,
        r: R,
    );

    fn statfs<R: Responder<StatFs>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R);

    #[allow(clippy::too_many_arguments)]
    fn fallocate<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        len: u64,
        mode: FallocateMode,
        r: R,
    );

    /// `SEEK_DATA`/`SEEK_HOLE` (`Set`/`Cur`/`End` are the frontend's own:
    /// `Invalid` here).
    #[allow(clippy::too_many_arguments)]
    fn seek<R: Responder<u64>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        off: u64,
        whence: SeekWhence,
        r: R,
    );

    /// An xattr's whole value (a frontend's size probe is its own).
    fn getxattr<R: Responder<Vec<u8>>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R);

    fn setxattr<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        name: &XattrName,
        value: &[u8],
        flags: SetXattrFlags,
        r: R,
    );

    /// The xattr names, sorted (the virtual ones per the view's
    /// [`crate::XattrPolicy`]).
    fn listxattr<R: Responder<Vec<XattrNameBuf>>>(&self, cx: &OpCtx<'_>, ino: Ino, r: R);

    fn removexattr<R: Responder<()>>(&self, cx: &OpCtx<'_>, ino: Ino, name: &XattrName, r: R);

    /// `F_GETLK`. Only asked when the frontend forwards locks
    /// ([`crate::FrontendCaps::cluster_locks`]): `NotImplemented` otherwise.
    fn lock_test<R: Responder<LockStatus>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        r: R,
    );

    /// `F_SETLK` (`sleep: false`: `Again` on a conflict) or
    /// `F_SETLKW`/`flock` (`sleep: true`: waits, on a thread of its own —
    /// the one op that defers today).
    #[allow(clippy::too_many_arguments)]
    fn lock_acquire<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        lock: LockSpec,
        sleep: bool,
        r: R,
    );

    /// `F_UNLCK`: drop `owner`'s locks in `range`.
    #[allow(clippy::too_many_arguments)]
    fn lock_release<R: Responder<()>>(
        &self,
        cx: &OpCtx<'_>,
        ino: Ino,
        fh: Fh,
        owner: LockOwner,
        range: LockRange,
        r: R,
    );

    /// Whole-view barrier (unmount, suspend, snapshot): every pending
    /// write of the view published, and durable as `fsync` would make it.
    fn sync_view<R: Responder<()>>(&self, cx: &OpCtx<'_>, r: R);
}
