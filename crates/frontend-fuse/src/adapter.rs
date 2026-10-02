//! `impl fuser::Filesystem for FuseFs<V>`: each callback decoded into an
//! [`OpCtx`](constellation_vfs::OpCtx) and a [`Vfs`] call, its reply wrapped
//! as the op's responder.
//!
//! Each op is begun by the session's [`Observer`] (plan 31 §6.10): it gets
//! its id and a `vfs.op` span, entered for the call, and its reply is
//! wrapped to count the op — outcome and latency — when it completes,
//! wherever that happens.
//!
//! No filesystem policy lives here — that is the view's, beneath the
//! trait. What does: the FUSE protocol. The kernel's flag words (`O_*`,
//! `FALLOC_FL_*`, `SEEK_*`, `XATTR_*`, `RENAME_*`, `F_*LCK`) become the
//! contract's decoded types, the kernel's device-number encoding becomes
//! [`Rdev`], a `SystemTime` becomes nanoseconds, and the `FUSE_INIT`
//! negotiation (the kernel queue sizing, parallel directory ops, and the
//! lock capabilities `--locks cluster` needs) happens here.
//!
//! Every op answers inline on the calling fuser worker, as it always did,
//! except a blocking lock, which the view completes from its `lock-wait`
//! thread, and a cold read (a chunk in no local cache), which the engine
//! completes from its completion pool (`FrontendCaps::linux_fuse` declares
//! every op deferrable; locks and cold reads defer so far — plan 31 §6.3,
//! C7b). Both are counted ([`Deferred`]) for a session handover.

use crate::reply::{
    AttrReply, BytesReply, CreateReply, DirReply, EmptyReply, EntryReply, LockReply, LseekReply,
    OpenReply, ReadReply, StatfsReply, WriteReply, XattrListReply, XattrReply, F_RDLCK, F_UNLCK,
    F_WRLCK,
};
use constellation_types::{Code, Rdev};
use constellation_vfs::{
    Caller, Durability, FallocateMode, Fh, FrontendCaps, LockKind, LockOwner, LockRange, LockSpec,
    Name, Observer, OpKind, OpenFlags, OpenOwner, RenameFlags, Responder, SeekWhence, SetAttr,
    SetXattrFlags, TimeSet, Vfs, WriteData, XattrName,
};
use fuser::{
    BsdFileFlags, FileHandle, Filesystem, INodeNo, InitFlags, KernelConfig, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen, ReplyWrite, ReplyXattr, Request,
    TimeOrNow, WriteFlags,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// The `frontend` label of this crate's op metrics and spans.
pub const FRONTEND: &str = "fuse";

/// What `FUSE_INIT` negotiates besides the capabilities: the kernel's
/// request queue, sized for the dispatcher's workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTuning {
    pub max_background: u16,
    pub congestion_threshold: u16,
    /// `FUSE_PARALLEL_DIROPS`: more than one worker.
    pub parallel_dirops: bool,
}

impl KernelTuning {
    /// The tuning for a dispatcher of `workers` threads
    /// ([`crate::threads`]).
    pub fn for_workers(workers: usize) -> Self {
        Self {
            max_background: crate::threads::max_background(workers),
            congestion_threshold: crate::threads::congestion_threshold(workers),
            parallel_dirops: workers > 1,
        }
    }
}

/// The FUSE session's filesystem: a shared [`Vfs`] (the daemon keeps its
/// own handle to the view, for the lock path's flushes and the hold
/// writer), the frontend's capabilities, and the `FUSE_INIT` tuning.
pub struct FuseFs<V: Vfs> {
    vfs: Arc<V>,
    caps: FrontendCaps,
    tuning: KernelTuning,
    /// Op ids, the `vfs.op` span and the op metrics (plan 31 §6.10), for
    /// this session's view, labelled with the transport the session
    /// negotiated (plan 38 §5). That is only known once the handshake has
    /// run, after this filesystem was handed to fuser, so the session
    /// fills the slot right after `FUSE_INIT` ([`Self::observer_slot`]),
    /// before any request is served.
    obs: Arc<OnceLock<Observer>>,
    /// Requests answered from another thread and not answered yet (plan
    /// 31 §6.11: a handover drains them, or refuses).
    deferred: Arc<Deferred>,
}

impl<V: Vfs> FuseFs<V> {
    pub fn new(vfs: Arc<V>, caps: FrontendCaps, tuning: KernelTuning) -> Self {
        Self {
            vfs,
            caps,
            tuning,
            obs: Arc::default(),
            deferred: Arc::default(),
        }
    }

    pub(crate) fn deferred(&self) -> &Arc<Deferred> {
        &self.deferred
    }

    /// Where the session puts this filesystem's [`Observer`] once it knows
    /// the negotiated transport.
    pub(crate) fn observer_slot(&self) -> Arc<OnceLock<Observer>> {
        self.obs.clone()
    }

    /// The session's observer. A filesystem served without a
    /// [`crate::FuseSession`] (the wire tests drive one over a socket
    /// pair) never has its slot filled and counts as `/dev/fuse`, which
    /// is what such a connection is.
    #[inline]
    fn obs(&self) -> &Observer {
        self.obs.get_or_init(|| {
            Observer::new(
                FRONTEND,
                &self.vfs.identity(),
                fuser::Transport::DevFuse.name(),
            )
        })
    }
}

/// The requests whose reply may come from another thread after the fuser
/// worker that read them moved on, in two counts:
///
/// - **blocking**: the blocking lock waits (`FUSE_SETLKW`/`flock`), which
///   may wait forever. A detach refuses while any is in flight: a reply
///   must be written on the descriptor its request was read from, by a
///   process that still serves it.
/// - **bounded**: reads, which the engine may answer from its completion
///   pool when they are cold (plan 31 C7b). Their waits end on their own
///   (a fetch succeeds or fails), so a detach drains them instead of
///   refusing.
///
/// Every other op answers inline, on the worker, before it reads again, so
/// stopping the workers drains those by itself.
#[derive(Default)]
pub(crate) struct Deferred {
    blocking: std::sync::atomic::AtomicUsize,
    bounded: std::sync::atomic::AtomicUsize,
}

impl Deferred {
    /// Blocking lock waits in flight.
    pub(crate) fn count(&self) -> usize {
        self.blocking.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Reads that may still be answered from the engine's pool.
    pub(crate) fn bounded(&self) -> usize {
        self.bounded.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn counter(&self, bounded: bool) -> &std::sync::atomic::AtomicUsize {
        if bounded {
            &self.bounded
        } else {
            &self.blocking
        }
    }

    fn track<R>(self: &Arc<Self>, reply: R) -> Tracked<R> {
        self.track_as(reply, false)
    }

    fn track_bounded<R>(self: &Arc<Self>, reply: R) -> Tracked<R> {
        self.track_as(reply, true)
    }

    fn track_as<R>(self: &Arc<Self>, reply: R, bounded: bool) -> Tracked<R> {
        self.counter(bounded)
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Tracked {
            reply: Some(reply),
            deferred: self.clone(),
            bounded,
        }
    }
}

#[cfg(test)]
impl Deferred {
    pub(crate) fn track_raw(&self) {
        self.blocking
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn untrack_raw(&self) {
        self.blocking
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn track_bounded_raw(&self) {
        self.bounded
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn untrack_bounded_raw(&self) {
        self.bounded
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
pub(crate) fn track_for_test<R: constellation_vfs::Responder<()>>(
    deferred: &Arc<Deferred>,
    reply: R,
) -> impl constellation_vfs::Responder<()> {
    deferred.track(reply)
}

/// A responder counted in [`Deferred`] until it has answered (or was
/// dropped, which answers `EIO`).
struct Tracked<R> {
    reply: Option<R>,
    deferred: Arc<Deferred>,
    bounded: bool,
}

impl<T, R: constellation_vfs::Responder<T>> constellation_vfs::Responder<T> for Tracked<R> {
    fn done(mut self, result: constellation_vfs::VfsResult<T>) {
        if let Some(reply) = self.reply.take() {
            reply.done(result);
        }
    }
}

impl<R> Drop for Tracked<R> {
    fn drop(&mut self) {
        // The reply (if `done` never ran) answers from its own drop first.
        drop(self.reply.take());
        self.deferred
            .counter(self.bounded)
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Who is asking: FUSE sends uid, gid and pid (0 for a request the
/// kernel makes on its own); the supplementary groups are read from the
/// host only if a check needs them (`constellation_vfs::Caller`).
fn caller(req: &Request) -> Caller {
    let pid = req.pid();
    Caller::new(req.uid(), req.gid(), (pid != 0).then_some(pid))
}

fn name(name: &OsStr) -> &Name {
    Name::new(name.as_bytes())
}

fn xattr_name(name: &OsStr) -> &XattrName {
    XattrName::new(name.as_bytes())
}

/// An open's flag word, decoded. The access mode reads as the kernel's
/// `O_ACCMODE` check does: read-only, write-only, or anything else as
/// both.
pub(crate) fn open_flags(raw: i32) -> OpenFlags {
    let mut flags = match raw & libc::O_ACCMODE {
        libc::O_RDONLY => OpenFlags::READ,
        libc::O_WRONLY => OpenFlags::WRITE,
        _ => OpenFlags::READ | OpenFlags::WRITE,
    };
    for (bit, flag) in [
        (libc::O_CREAT, OpenFlags::CREATE),
        (libc::O_EXCL, OpenFlags::EXCL),
        (libc::O_TRUNC, OpenFlags::TRUNC),
        (libc::O_APPEND, OpenFlags::APPEND),
        // `O_SYNC` includes `O_DSYNC`'s bit: either one publishes each
        // write before it returns.
        (libc::O_SYNC | libc::O_DSYNC, OpenFlags::SYNC),
    ] {
        if raw & bit != 0 {
            flags |= flag;
        }
    }
    flags
}

/// `fallocate`'s mode word, decoded; any bit the contract does not name
/// is kept as `UNSUPPORTED` (the view refuses it where it always did).
#[cfg(target_os = "linux")]
pub(crate) fn fallocate_mode(raw: i32) -> FallocateMode {
    let mut mode = FallocateMode::empty();
    let mut rest = raw;
    for (bit, flag) in [
        (libc::FALLOC_FL_KEEP_SIZE, FallocateMode::KEEP_SIZE),
        (libc::FALLOC_FL_PUNCH_HOLE, FallocateMode::PUNCH_HOLE),
        (libc::FALLOC_FL_ZERO_RANGE, FallocateMode::ZERO_RANGE),
    ] {
        if raw & bit != 0 {
            mode |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        mode |= FallocateMode::UNSUPPORTED;
    }
    mode
}

/// Without Linux's `FALLOC_FL_*`: only a plain allocation is nameable.
#[cfg(not(target_os = "linux"))]
pub(crate) fn fallocate_mode(raw: i32) -> FallocateMode {
    if raw == 0 {
        FallocateMode::empty()
    } else {
        FallocateMode::UNSUPPORTED
    }
}

/// `setxattr`'s flag word, decoded (both bits at once, or any other, is
/// the view's `EINVAL`).
pub(crate) fn setxattr_flags(raw: i32) -> SetXattrFlags {
    let mut flags = SetXattrFlags::empty();
    let mut rest = raw;
    for (bit, flag) in [
        (libc::XATTR_CREATE, SetXattrFlags::CREATE),
        (libc::XATTR_REPLACE, SetXattrFlags::REPLACE),
    ] {
        if raw & bit != 0 {
            flags |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        flags |= SetXattrFlags::UNSUPPORTED;
    }
    flags
}

/// `renameat2`'s flags, decoded (the view accepts and ignores them, as
/// the adapter always did).
pub(crate) fn rename_flags(raw: u32) -> RenameFlags {
    let mut flags = RenameFlags::empty();
    let mut rest = raw;
    #[cfg(target_os = "linux")]
    for (bit, flag) in [
        (libc::RENAME_NOREPLACE, RenameFlags::NOREPLACE),
        (libc::RENAME_EXCHANGE, RenameFlags::EXCHANGE),
        (libc::RENAME_WHITEOUT, RenameFlags::WHITEOUT),
    ] {
        if raw & bit != 0 {
            flags |= flag;
            rest &= !bit;
        }
    }
    if rest != 0 {
        flags |= RenameFlags::UNSUPPORTED;
    }
    flags
}

/// `lseek`'s whence. The kernel forwards only `SEEK_DATA`/`SEEK_HOLE`
/// (it answers the rest itself, and refuses a whence past `SEEK_MAX`).
pub(crate) fn seek_whence(raw: i32) -> Option<SeekWhence> {
    match raw {
        libc::SEEK_SET => Some(SeekWhence::Set),
        libc::SEEK_CUR => Some(SeekWhence::Cur),
        libc::SEEK_END => Some(SeekWhence::End),
        libc::SEEK_DATA => Some(SeekWhence::Data),
        libc::SEEK_HOLE => Some(SeekWhence::Hole),
        _ => None,
    }
}

/// A `setattr` time.
fn time_set(t: TimeOrNow) -> TimeSet {
    match t {
        TimeOrNow::Now => TimeSet::Now,
        TimeOrNow::SpecificTime(st) => TimeSet::At(
            st.duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        ),
    }
}

fn lock_spec(owner: fuser::LockOwner, start: u64, end: u64, kind: LockKind, pid: u32) -> LockSpec {
    LockSpec {
        owner: LockOwner(owner.0),
        range: LockRange { start, end },
        kind,
        pid,
    }
}

impl<V: Vfs> Filesystem for FuseFs<V> {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.set_max_background(self.tuning.max_background);
        let _ = config.set_congestion_threshold(self.tuning.congestion_threshold);
        if self.tuning.parallel_dirops {
            // Older kernels may not advertise this capability. Multi-reader
            // dispatch still works; only directory operations remain ordered.
            let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        }
        // Plan 30 §M14: under `--locks cluster` the kernel hands POSIX and
        // `flock` locks to `getlk`/`setlk`; without the capabilities (and
        // on a frozen snapshot view, where nothing can be written — the
        // daemon declares no `cluster_locks` for one) it keeps them
        // node-local, as it always did.
        if self.caps.cluster_locks {
            if let Err(missing) =
                config.add_capabilities(InitFlags::FUSE_POSIX_LOCKS | InitFlags::FUSE_FLOCK_LOCKS)
            {
                tracing::warn!(
                    ?missing,
                    "the kernel does not offer FUSE lock forwarding; locks on this mount stay node-local"
                );
            }
        }
        Ok(())
    }

    fn lookup(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEntry) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Lookup, parent.0);
        let _in = op.enter();
        self.vfs.lookup(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EntryReply(reply)),
        );
    }

    fn getattr(&self, req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Getattr, ino.0);
        let _in = op.enter();
        self.vfs.getattr(
            &op.ctx(&caller),
            ino.0,
            fh.map(|fh| Fh(fh.0)),
            op.responder(AttrReply(reply)),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let caller = caller(req);
        let set = SetAttr {
            mode,
            uid,
            gid,
            size,
            atime: atime.map(time_set),
            mtime: mtime.map(time_set),
        };
        let op = self.obs().begin(OpKind::Setattr, ino.0);
        let _in = op.enter();
        self.vfs.setattr(
            &op.ctx(&caller),
            ino.0,
            fh.map(|fh| Fh(fh.0)),
            &set,
            op.responder(AttrReply(reply)),
        );
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Readlink, ino.0);
        let _in = op.enter();
        self.vfs
            .readlink(&op.ctx(&caller), ino.0, op.responder(BytesReply(reply)));
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Mkdir, parent.0);
        let _in = op.enter();
        self.vfs.mkdir(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            op.responder(EntryReply(reply)),
        );
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        // FUSE carries the kernel's 32-bit `new_encode_dev`; the contract
        // (and the journal) carry the portable pair (plan 31 §7).
        let rdev: Rdev = constellation_platform::from_linux_fuse_rdev(rdev);
        let op = self.obs().begin(OpKind::Mknod, parent.0);
        let _in = op.enter();
        self.vfs.mknod(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            rdev,
            op.responder(EntryReply(reply)),
        );
    }

    fn link(
        &self,
        req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Link, ino.0);
        let _in = op.enter();
        self.vfs.link(
            &op.ctx(&caller),
            ino.0,
            newparent.0,
            name(newname),
            op.responder(EntryReply(reply)),
        );
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Create, parent.0);
        let _in = op.enter();
        self.vfs.create(
            &op.ctx(&caller),
            parent.0,
            name(n),
            mode,
            open_flags(flags),
            OpenOwner::NONE,
            op.responder(CreateReply(reply)),
        );
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Symlink, parent.0);
        let _in = op.enter();
        self.vfs.symlink(
            &op.ctx(&caller),
            parent.0,
            name(link_name),
            target.as_os_str().as_bytes(),
            op.responder(EntryReply(reply)),
        );
    }

    fn unlink(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Unlink, parent.0);
        let _in = op.enter();
        self.vfs.unlink(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Rmdir, parent.0);
        let _in = op.enter();
        self.vfs.rmdir(
            &op.ctx(&caller),
            parent.0,
            name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        n: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Rename, parent.0);
        let _in = op.enter();
        self.vfs.rename(
            &op.ctx(&caller),
            parent.0,
            name(n),
            newparent.0,
            name(newname),
            rename_flags(flags.bits()),
            op.responder(EmptyReply(reply)),
        );
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: ReplyOpen) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Open, ino.0);
        let _in = op.enter();
        self.vfs.open(
            &op.ctx(&caller),
            ino.0,
            open_flags(flags.0),
            OpenOwner::NONE,
            op.responder(OpenReply(reply)),
        );
    }

    fn read(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Read, ino.0);
        let _in = op.enter();
        self.vfs.read(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            size,
            // A cold read may be answered from the engine's completion
            // pool: counted until it is, for a detach to drain.
            op.responder(self.deferred.track_bounded(ReadReply { reply, size })),
        );
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Write, ino.0);
        let _in = op.enter();
        // Borrowed from wherever fuser received the request: the worker's
        // `/dev/fuse` buffer, or, over io_uring, the ring entry's own payload
        // buffer the kernel copied the bytes into — no copy into a worker
        // buffer either way (plan 38 §3(b)). The borrow ends when this call
        // returns, before the entry can be re-armed: fuser keeps the entry
        // fetched until the dispatch that received `data` is over
        // (`HeldRequest` in `vendor/fuser/src/uring/ring.rs`), and a view
        // that keeps the bytes past the call copies them into
        // `WriteData::Shared` itself.
        self.vfs.write(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            WriteData::Borrowed(data),
            open_flags(flags.0),
            op.responder(WriteReply(reply)),
        );
    }

    fn flush(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Flush, ino.0);
        let _in = op.enter();
        self.vfs.flush(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            LockOwner(lock_owner.0),
            op.responder(EmptyReply(reply)),
        );
    }

    fn fsync(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Fsync, ino.0);
        let _in = op.enter();
        self.vfs.fsync(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            Durability::Configured,
            op.responder(EmptyReply(reply)),
        );
    }

    fn release(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: fuser::OpenFlags,
        lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Release, ino.0);
        let _in = op.enter();
        self.vfs.release(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            open_flags(flags.0),
            lock_owner.map(|owner| LockOwner(owner.0)),
            op.responder(EmptyReply(reply)),
        );
    }

    fn readdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Readdir, ino.0);
        let _in = op.enter();
        self.vfs.readdir(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            false,
            op.responder(DirReply(reply)),
        );
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        n: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        let op = self.obs().begin(OpKind::Setxattr, ino.0);
        if position != 0 {
            // macOS's resource-fork offset; a Linux kernel always sends 0.
            // Refused here, and counted like any other refusal.
            op.responder(EmptyReply(reply))
                .done(Err(Code::Invalid.into()));
            return;
        }
        let caller = caller(req);
        let _in = op.enter();
        self.vfs.setxattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            value,
            setxattr_flags(flags),
            op.responder(EmptyReply(reply)),
        );
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, n: &OsStr, size: u32, reply: ReplyXattr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Getxattr, ino.0);
        let _in = op.enter();
        self.vfs.getxattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            op.responder(XattrReply { reply, size }),
        );
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Listxattr, ino.0);
        let _in = op.enter();
        self.vfs.listxattr(
            &op.ctx(&caller),
            ino.0,
            op.responder(XattrListReply { reply, size }),
        );
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, n: &OsStr, reply: ReplyEmpty) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Removexattr, ino.0);
        let _in = op.enter();
        self.vfs.removexattr(
            &op.ctx(&caller),
            ino.0,
            xattr_name(n),
            op.responder(EmptyReply(reply)),
        );
    }

    fn statfs(&self, req: &Request, ino: INodeNo, reply: fuser::ReplyStatfs) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Statfs, ino.0);
        let _in = op.enter();
        self.vfs
            .statfs(&op.ctx(&caller), ino.0, op.responder(StatfsReply(reply)));
    }

    fn fallocate(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let op = self.obs().begin(OpKind::Fallocate, ino.0);
        let _in = op.enter();
        self.vfs.fallocate(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset,
            length,
            fallocate_mode(mode),
            op.responder(EmptyReply(reply)),
        );
    }

    fn lseek(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        // Refused here or not, the op is counted.
        let op = self.obs().begin(OpKind::Seek, ino.0);
        if offset < 0 {
            op.responder(LseekReply(reply))
                .done(Err(Code::NoDeviceOrAddress.into()));
            return;
        }
        let Some(whence) = seek_whence(whence) else {
            op.responder(LseekReply(reply))
                .done(Err(Code::Invalid.into()));
            return;
        };
        let caller = caller(req);
        let _in = op.enter();
        self.vfs.seek(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            offset as u64,
            whence,
            op.responder(LseekReply(reply)),
        );
    }

    /// Plan 30 §M14: `F_GETLK` under `--locks cluster`. Under `--locks
    /// local` the kernel never asks.
    fn getlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        let caller = caller(req);
        // A write test asks about any lock; anything else (a read test,
        // `F_UNLCK`) about write locks only.
        let kind = if typ == F_WRLCK {
            LockKind::Write
        } else {
            LockKind::Read
        };
        let op = self.obs().begin(OpKind::LockTest, ino.0);
        let _in = op.enter();
        self.vfs.lock_test(
            &op.ctx(&caller),
            ino.0,
            Fh(fh.0),
            lock_spec(lock_owner, start, end, kind, pid),
            op.responder(LockReply(reply)),
        );
    }

    /// Plan 30 §M14: `F_SETLK`/`F_SETLKW`/`flock` under `--locks
    /// cluster`. Non-blocking requests are answered on this worker; the
    /// view answers a blocking one (`sleep`) from a thread of its own, so
    /// a contended lock never pins a FUSE worker. fuser 0.18 delivers no
    /// interrupts: a blocked wait cannot be cancelled by a signal.
    fn setlk(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: fuser::LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let caller = caller(req);
        let kind = match typ {
            F_UNLCK => {
                let op = self.obs().begin(OpKind::LockRelease, ino.0);
                let _in = op.enter();
                self.vfs.lock_release(
                    &op.ctx(&caller),
                    ino.0,
                    Fh(fh.0),
                    LockOwner(lock_owner.0),
                    LockRange { start, end },
                    op.responder(EmptyReply(reply)),
                );
                return;
            }
            F_RDLCK => LockKind::Read,
            F_WRLCK => LockKind::Write,
            // Not reachable from the kernel, which validates `l_type`
            // (and builds `flock`'s itself) before asking.
            _ => {
                self.obs()
                    .begin(OpKind::LockAcquire, ino.0)
                    .responder(EmptyReply(reply))
                    .done(Err(Code::Invalid.into()));
                return;
            }
        };
        let op = self.obs().begin(OpKind::LockAcquire, ino.0);
        let _in = op.enter();
        let cx = op.ctx(&caller);
        let spec = lock_spec(lock_owner, start, end, kind, pid);
        if sleep {
            // May answer from the view's `lock-wait` thread: counted, and
            // counted as an op when that thread answers.
            let reply = op.responder(self.deferred.track(EmptyReply(reply)));
            self.vfs
                .lock_acquire(&cx, ino.0, Fh(fh.0), spec, true, reply);
        } else {
            self.vfs.lock_acquire(
                &cx,
                ino.0,
                Fh(fh.0),
                spec,
                false,
                op.responder(EmptyReply(reply)),
            );
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use constellation_vfs::types::mode;

    #[test]
    fn open_flags_decode_the_access_mode_as_the_kernel_does() {
        let rw = OpenFlags::READ | OpenFlags::WRITE;
        assert_eq!(open_flags(libc::O_RDONLY), OpenFlags::READ);
        assert_eq!(open_flags(libc::O_WRONLY), OpenFlags::WRITE);
        assert_eq!(open_flags(libc::O_RDWR), rw);
        assert_eq!(
            open_flags(libc::O_ACCMODE),
            rw,
            "an invalid mode reads as both"
        );
        assert_eq!(
            open_flags(libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC),
            rw | OpenFlags::CREATE | OpenFlags::EXCL | OpenFlags::TRUNC
        );
        for sync in [libc::O_SYNC, libc::O_DSYNC] {
            assert!(open_flags(libc::O_WRONLY | sync).contains(OpenFlags::SYNC));
        }
        assert!(!open_flags(libc::O_WRONLY | libc::O_APPEND).contains(OpenFlags::SYNC));
        assert!(open_flags(libc::O_WRONLY | libc::O_APPEND).contains(OpenFlags::APPEND));
    }

    #[test]
    fn fallocate_setxattr_and_rename_flags_keep_what_they_cannot_name() {
        assert_eq!(fallocate_mode(0), FallocateMode::empty());
        assert_eq!(
            fallocate_mode(libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE),
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE
        );
        assert_eq!(
            fallocate_mode(libc::FALLOC_FL_ZERO_RANGE),
            FallocateMode::ZERO_RANGE
        );
        assert!(fallocate_mode(libc::FALLOC_FL_COLLAPSE_RANGE).contains(FallocateMode::UNSUPPORTED));
        assert_eq!(setxattr_flags(0), SetXattrFlags::empty());
        assert_eq!(setxattr_flags(libc::XATTR_CREATE), SetXattrFlags::CREATE);
        assert_eq!(setxattr_flags(libc::XATTR_REPLACE), SetXattrFlags::REPLACE);
        assert!(setxattr_flags(libc::XATTR_CREATE | libc::XATTR_REPLACE)
            .mode()
            .is_err());
        assert!(setxattr_flags(8).contains(SetXattrFlags::UNSUPPORTED));
        assert!(setxattr_flags(8).mode().is_err());
        assert_eq!(rename_flags(0), RenameFlags::empty());
        assert_eq!(rename_flags(libc::RENAME_NOREPLACE), RenameFlags::NOREPLACE);
        assert!(rename_flags(1 << 20).contains(RenameFlags::UNSUPPORTED));
    }

    #[test]
    fn whence_and_lock_types_and_mode_bits_match_the_kernels() {
        assert_eq!(seek_whence(libc::SEEK_DATA), Some(SeekWhence::Data));
        assert_eq!(seek_whence(libc::SEEK_HOLE), Some(SeekWhence::Hole));
        assert_eq!(seek_whence(libc::SEEK_SET), Some(SeekWhence::Set));
        assert_eq!(seek_whence(99), None);
        assert_ne!(F_RDLCK, F_WRLCK);
        assert_ne!(F_WRLCK, F_UNLCK);
        // The contract's POSIX type bits are the kernel's.
        assert_eq!(mode::S_IFMT, libc::S_IFMT);
        assert_eq!(mode::S_IFREG, libc::S_IFREG);
        assert_eq!(mode::S_IFDIR, libc::S_IFDIR);
        assert_eq!(mode::S_IFLNK, libc::S_IFLNK);
        assert_eq!(mode::S_IFIFO, libc::S_IFIFO);
        assert_eq!(mode::S_IFSOCK, libc::S_IFSOCK);
        assert_eq!(mode::S_IFBLK, libc::S_IFBLK);
        assert_eq!(mode::S_IFCHR, libc::S_IFCHR);
    }

    #[test]
    fn setattr_times_are_nanoseconds_or_the_engines_now() {
        assert_eq!(time_set(TimeOrNow::Now), TimeSet::Now);
        let t = UNIX_EPOCH + std::time::Duration::from_nanos(1_234_567_891);
        assert_eq!(
            time_set(TimeOrNow::SpecificTime(t)),
            TimeSet::At(1_234_567_891)
        );
        let before = UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert_eq!(time_set(TimeOrNow::SpecificTime(before)), TimeSet::At(0));
    }

    #[test]
    fn kernel_tuning_scales_with_the_workers() {
        let one = KernelTuning::for_workers(1);
        assert_eq!(
            (
                one.max_background,
                one.congestion_threshold,
                one.parallel_dirops
            ),
            (16, 12, false)
        );
        let twelve = KernelTuning::for_workers(12);
        assert_eq!((twelve.max_background, twelve.parallel_dirops), (96, true));
    }
}
