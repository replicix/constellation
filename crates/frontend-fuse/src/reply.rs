//! Responders: each `constellation_vfs` completion turned into its fuser
//! reply, and the portable [`Code`] into the Linux errno.
//!
//! Each responder is a newtype around the fuser `Reply*` it completes —
//! no allocation, and `done(self)` consumes it, so a request is answered
//! at most once. **Drop fail-safe:** a responder dropped without `done`
//! drops its fuser reply, whose own `Drop` (`fuser::ReplyRaw`) answers
//! `EIO` and logs a warning — so a request is answered at least once too,
//! on every path, including a `lock-wait` thread that could not be
//! started (`constellation_engine::locks::ClusterLocks::lock`).

use crate::attrwatch::{Arrival, Replying};
use crate::dentries::{EntryGuard, KernelEntries, RenameGuard};
use crate::notify::NotifyGate;
use crate::pagecache::{Claim, Mark, OpenFacts, PageCacheTable};
use crate::passthrough::{OpenAnswer, PassthroughState, PreOpen};
use crate::repair::RepairLane;
use crate::stats::ZeroCopyCounter;
use constellation_types::Code;
use constellation_vfs::{
    Attr, DirSink, Entry, Fh, FileKind, Ino, LockKind, LockStatus, OpenFlags, Opened, ReadData,
    Responder, StatFs, VfsResult, XattrNameBuf, ZeroCopySource, ROOT_INO,
};
use fuser::{
    Errno, FileHandle, FileType, FopenFlags, Generation, INodeNo, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek,
    ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Transport,
};
use std::ffi::OsStr;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

/// Plan 42 §5.3.1 (W3b): before a namespace operation's reply is written,
/// an attribute-only `FUSE_NOTIFY_INVAL_INODE` for every directory it
/// changed and every inode it moved. The kernel's `fuse_dir_changed`
/// invalidates the parent's attributes after the reply without bumping
/// its `attr_version`, so a `GETATTR`/`LOOKUP`/`READDIRPLUS` reply the
/// daemon generated before the change but the kernel processes after it
/// passes the version check, stores the old attributes and clears the
/// invalidation — the old `st_nlink` cached for the attribute TTL (race
/// 2b); `fuse_rename_common` does the same to the moved inode's `ctime`.
/// The notification sets the inode's `attr_version` to a fresh value
/// (`fuse_reverse_inval_inode`), above the snapshot every such request
/// took before it was sent, so their replies are rejected whenever they
/// arrive; a request sent after it is answered after the change. Written
/// from the replying thread (`NotifyGate::notifier_now` says why that is
/// safe for an attribute-only one); errors mean the kernel holds no such
/// inode, so nothing to protect.
/// The notice invalidates the inode's whole attribute mask, so each costs
/// the next path walk through a directory a `GETATTR` (plan 42 F21): under
/// `--cto strict` every operation writes it (no attribute lifetime, no
/// request), in bounded mode only for the inodes an attribute reply may
/// still be on its way for (`crate::attrwatch`).
#[derive(Clone)]
pub(crate) struct DirNotice {
    pub gate: Arc<OnceLock<Arc<NotifyGate>>>,
    pub pc: Arc<PageCacheTable>,
}

impl DirNotice {
    /// `dirs` were changed and `moved` were moved by the namespace
    /// operation being answered: notices for those the mode calls for
    /// (module doc above; F21 for the narrowed trigger). A kernel with 0003
    /// needs none for `dirs`, still one for `moved` (F16;
    /// [`crate::pagecache::PageCacheConfig::dir_notice_modes`]).
    fn tell(&self, dirs: &[Option<Ino>], moved: &[Option<Ino>]) {
        use crate::pagecache::NoticeMode;
        let modes = self.pc.config().dir_notice_modes();
        let mut inos: [Option<Ino>; 4] = [None; 4];
        let mut n = 0;
        for (class, mode) in [(dirs, modes.dirs), (moved, modes.moved)] {
            if mode == NoticeMode::Off {
                continue;
            }
            for ino in class {
                if n < inos.len() {
                    inos[n] = *ino;
                    n += 1;
                }
            }
        }
        if n == 0 {
            return;
        }
        let inos = &inos[..n];
        let mode = if modes.narrowed() {
            NoticeMode::Narrowed
        } else {
            NoticeMode::Always
        };
        let Some(notifier) = self.gate.get().and_then(|g| g.notifier_now()) else {
            return;
        };
        let mut distinct: [Ino; 4] = [0; 4];
        let mut n = 0;
        for ino in inos.iter().flatten() {
            if distinct[..n].contains(ino) {
                continue;
            }
            if n < distinct.len() {
                distinct[n] = *ino;
                n += 1;
            }
        }
        if mode == NoticeMode::Always {
            for ino in &distinct[..n] {
                let _ = notifier.inval_inode(INodeNo(*ino), -1, 0);
                self.pc.count_dir_notice();
            }
            return;
        }
        let (needed, marks) = self.pc.attr_watch().changed(&distinct[..n]);
        self.pc.count_dir_notices_skipped(n - needed.len());
        for ino in needed {
            let _ = notifier.inval_inode(INodeNo(ino), -1, 0);
            self.pc.count_dir_notice();
        }
        // Only now: until the notices are written, a concurrent change of
        // these inodes must still find the marks (`attrwatch`).
        self.pc.attr_watch().release(marks);
    }

    /// A reply carrying the attributes of `ino` is about to be written
    /// (narrowed mode): if a namespace operation changed `ino` while its
    /// request was being served, that operation did not see this reply
    /// coming, so the notice is written here, before it.
    fn replying(&self, seen: Arrival, ino: Ino) {
        let Some(marks) = self.pc.attr_watch().replying(seen, ino) else {
            return;
        };
        if let Some(notifier) = self.gate.get().and_then(|g| g.notifier_now()) {
            let _ = notifier.inval_inode(INodeNo(ino), -1, 0);
            self.pc.count_dir_notice();
        }
        self.pc.attr_watch().release(marks);
    }

    /// [`Self::replying`] for a reply of several inodes (`READDIRPLUS`):
    /// `ino` joins the set of marks the request has made so far, and the
    /// notice is written first if a change of it was recorded since the
    /// request arrived. `false`: the table of marks is full, nothing was
    /// marked and nothing written — the caller leaves `ino`'s attributes
    /// out of the reply (a notice per entry would be a storm, and would
    /// make the kernel reject the page's attributes, `attrwatch`).
    fn replying_each(&self, seen: Arrival, ino: Ino) -> bool {
        match self.pc.attr_watch().replying_each(seen, ino) {
            Replying::Clear => true,
            Replying::Full => false,
            Replying::Notice(marks) => {
                if let Some(notifier) = self.gate.get().and_then(|g| g.notifier_now()) {
                    let _ = notifier.inval_inode(INodeNo(ino), -1, 0);
                    self.pc.count_dir_notice();
                }
                self.pc.attr_watch().release(marks);
                true
            }
        }
    }
}

/// Plan 42 §5.3.2 (W3a): a directory's attributes are answered with an
/// attribute TTL of 0 and the entry's TTL kept (`zero`), so the kernel
/// never answers a directory's `stat` from a cache an install may be
/// half-way through updating (race 2a: `fuse_change_attributes_common`
/// clears the invalidation and sets `i_time` before it stores the fields,
/// and `fuse_update_get_attr` reads both without `fi->lock`); with an
/// attribute TTL of 0 `i_time` is 0 and every reader refreshes. `false`:
/// the view's TTL, as before plan 42.
fn attr_ttl(attr: &Attr, dir_zero: bool) -> Duration {
    if dir_zero && attr.kind == FileKind::Dir {
        Duration::ZERO
    } else {
        attr.ttl
    }
}

/// The Linux FUSE boundary (plan 31 §7): the one place a portable [`Code`]
/// becomes the kernel's errno number. Everything below the reply carries
/// `Code`; nothing here ever holds a raw errno.
pub fn reply_code(code: Code) -> Errno {
    Errno::from_i32(code.to_linux_errno())
}

/// The kernel's attribute struct for `attr`.
pub(crate) fn fuse_attr(a: &Attr) -> fuser::FileAttr {
    // Before 1970 too: a time `utimes` set there is stored as is.
    let ts = |ns: i64| {
        if ns >= 0 {
            UNIX_EPOCH + Duration::from_nanos(ns as u64)
        } else {
            UNIX_EPOCH - Duration::from_nanos(ns.unsigned_abs())
        }
    };
    fuser::FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.blocks,
        atime: ts(a.atime_ns),
        mtime: ts(a.mtime_ns),
        ctime: ts(a.ctime_ns),
        crtime: ts(a.ctime_ns),
        kind: file_type(a.kind),
        perm: a.mode as u16,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        // The kernel's 32-bit `new_encode_dev`, the inverse of what
        // `mknod` unpacked (`adapter.rs`).
        rdev: constellation_platform::to_linux_fuse_rdev(a.rdev),
        blksize: a.blksize,
        flags: 0,
    }
}

pub(crate) fn file_type(kind: FileKind) -> FileType {
    match kind {
        FileKind::File => FileType::RegularFile,
        FileKind::Dir => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
        FileKind::Fifo => FileType::NamedPipe,
        FileKind::Socket => FileType::Socket,
        FileKind::BlockDev => FileType::BlockDevice,
        FileKind::CharDev => FileType::CharDevice,
    }
}

/// An entry reply, and the name it hands the kernel (`dentries`; the
/// node id it hands it is counted there too, `refs`).
pub(crate) struct EntryReply {
    // Before `reply`: fields drop in declaration order, and a responder
    // dropped without `done` must end its guard before fuser's Drop answers
    // EIO.
    pub name: EntryGuard,
    pub reply: ReplyEntry,
    /// Plan 42 §5.2.1 (A3): the size a file's reply hands the kernel.
    pub pc: Arc<PageCacheTable>,
    /// Plan 42 §5.3.1: the directory the operation changed (`None` for a
    /// `lookup`), told before the reply.
    pub changed: Option<Ino>,
    pub notice: DirNotice,
    /// Plan 42 F21: what the request saw when it arrived.
    pub seen: Arrival,
    /// Plan 42 §5.3.2: a directory's attributes get no kernel lifetime.
    pub dir_zero: bool,
}

impl Responder<Entry> for EntryReply {
    fn done(self, r: VfsResult<Entry>) {
        let EntryReply {
            name,
            reply,
            pc,
            changed,
            notice,
            seen,
            dir_zero,
        } = self;
        match r {
            Ok(e) => {
                if e.attr.kind == FileKind::File {
                    pc.size_seen(e.attr.ino, e.attr.size);
                }
                if changed.is_some() {
                    notice.tell(&[changed], &[]);
                }
                notice.replying(seen, e.attr.ino);
                // Before the bytes go: the kernel may forget this lookup as
                // soon as it has the reply, on another worker.
                name.hands_out(e.attr.ino);
                reply.entry_with_ttls(
                    &attr_ttl(&e.attr, dir_zero),
                    &e.attr.ttl,
                    &fuse_attr(&e.attr),
                    Generation(e.generation),
                );
                name.replied(e.attr.ttl);
            }
            Err(e) => name.failed(|| reply.error(reply_code(e.code()))),
        }
    }

    /// A parked op's reply holds one of its ring queue's lock-wait entries
    /// (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// An attribute reply (`GETATTR`, `SETATTR`). Plan 42 §5.2.1 (A3): a
/// size other than the one last handed to the kernel makes the requester
/// drop the file's pages when it installs the reply.
pub(crate) struct AttrReply {
    pub reply: ReplyAttr,
    pub pc: Arc<PageCacheTable>,
    /// Plan 42 F21: the narrowed W3b notice, and what the request saw when
    /// it arrived.
    pub notice: DirNotice,
    pub seen: Arrival,
    /// Plan 42 §5.3.2: a directory's attributes get no kernel lifetime.
    pub dir_zero: bool,
}

impl Responder<Attr> for AttrReply {
    fn done(self, r: VfsResult<Attr>) {
        match r {
            Ok(a) => {
                if a.kind == FileKind::File {
                    self.pc.size_seen(a.ino, a.size);
                }
                self.notice.replying(self.seen, a.ino);
                self.reply
                    .attr(&attr_ttl(&a, self.dir_zero), &fuse_attr(&a))
            }
            Err(e) => self.reply.error(reply_code(e.code())),
        }
    }

    /// A parked op's reply holds one of its ring queue's lock-wait entries
    /// (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// `readlink`'s target.
pub(crate) struct BytesReply(pub ReplyData);

impl Responder<Vec<u8>> for BytesReply {
    fn done(self, r: VfsResult<Vec<u8>>) {
        match r {
            Ok(bytes) => self.0.data(&bytes),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// A read's reply, and the `size` the kernel asked for.
///
/// Over `/dev/fuse` the bytes go out exactly as they always did: joined
/// (`ReadData::contiguous`, a copy only when there is more than one
/// segment) and written with one `writev(2)`. Over an io_uring entry (plan
/// 38 §3(b)) each segment is copied straight into the entry's payload
/// buffer in order (`ReplyData::gather`): one copy, no syscall, and no
/// join first even for a multi-segment read. Either way the reply may come
/// from any thread — the engine's completion pool answers cold reads.
///
/// A short read (EOF, a hole at the end) is just fewer bytes, and an empty
/// one an empty reply. More bytes than were asked for is a bug beneath the
/// adapter: the kernel sized the request — and over a ring, the entry's
/// payload buffer — from the negotiated `max_write`/`max_pages`, so the
/// excess would not fit, and cutting it off would hand the application a
/// silently different file. Debug builds stop on it; release builds answer
/// `EIO`, never a truncated read.
///
/// A zero-copy answer (plan 38 §3(d): a range of a chunk file,
/// [`ReadData::as_zero_copy`]) goes out as one `ReplyData::read_fixed`:
/// on a request the kernel zero-copied, the ring's thread reads the file
/// straight into the reader's pages and the read is counted
/// (`constellation_fuse_zero_copy_reads_total`); on any other request
/// fuser reads the range into the reply buffer itself (`pread(2)`), so
/// the bytes are right whatever the request turned out to be. The reply
/// owns a share of the chunk file and its disk-cache pin until the read is
/// done, on whatever thread finishes it.
pub(crate) struct ReadReply {
    pub reply: ReplyData,
    pub size: u32,
    pub zero_copy: ZeroCopyCounter,
    /// Plan 42 §5.2.3: counted in flight on its inode until answered.
    pub _counted: Option<InFlightOp>,
}

/// A request counted in flight on an inode
/// ([`PageCacheTable::read_begin`]/`write_begin`) until this drops —
/// after its reply was written, on every path.
pub(crate) struct InFlightOp {
    pc: Arc<PageCacheTable>,
    ino: Ino,
}

impl InFlightOp {
    pub(crate) fn new(pc: &Arc<PageCacheTable>, ino: Ino) -> Self {
        Self {
            pc: pc.clone(),
            ino,
        }
    }
}

impl Drop for InFlightOp {
    fn drop(&mut self) {
        self.pc.op_done(self.ino);
    }
}

/// A zero-copy read's chunk file, for `read_fixed` (which wants an owned
/// `AsFd`): the share keeps the file open and its chunk pinned.
struct ZeroCopyFd(Arc<ZeroCopySource>);

impl AsFd for ZeroCopyFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.file().as_fd()
    }
}

impl Responder<ReadData> for ReadReply {
    fn done(self, r: VfsResult<ReadData>) {
        let data = match r {
            Ok(data) => data,
            Err(e) => return self.reply.error(reply_code(e.code())),
        };
        if data.len() > self.size as usize {
            debug_assert!(
                false,
                "a read of {} bytes answered with {}",
                self.size,
                data.len()
            );
            tracing::error!(
                asked = self.size,
                got = data.len(),
                "a read was answered with more bytes than it asked for; replying EIO"
            );
            return self.reply.error(reply_code(Code::Io));
        }
        if data.is_empty() {
            return self.reply.data(&[]);
        }
        if let Some(zc) = data.as_zero_copy() {
            let zero_copied = self.reply.zero_copy();
            self.reply
                .read_fixed(ZeroCopyFd(Arc::clone(&zc.source)), zc.offset, zc.len);
            if zero_copied {
                self.zero_copy.count();
            }
            return;
        }
        match self.reply.transport() {
            Transport::DevFuse => match data.contiguous() {
                Ok(bytes) => self.reply.data(&bytes),
                Err(error) => self.reply.error(reply_code(error.code())),
            },
            _ => self.reply.gather(data.segments()),
        }
    }

    /// A parked read's reply holds one of its ring queue's lock-wait
    /// entries (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// Plan 43 C7-F F-c review: whether a reply type can reserve one of its
/// ring queue's `depth - 1` lock-wait entries when its op waits
/// (`hold_for_wait`, which the vendored fuser has on every reply type a
/// parked op answers with). A parked op whose reply cannot is refused over a ring
/// (`adapter::Parkable`): holding its entry outside the budget let parked
/// reads and writes fill a CPU's queue and stall every request on it.
pub(crate) trait RingBudget {
    const RING_BUDGETED: bool;
}

macro_rules! ring_budget {
    ($($t:ty => $b:expr),* $(,)?) => {
        $(impl RingBudget for $t { const RING_BUDGETED: bool = $b; })*
    };
}

ring_budget! {
    EmptyReply => true,
    DirChangeReply => true,
    RenameReply => true,
    AttrReply => true,
    EntryReply => true,
    CreateReply => true,
    OpenReply => true,
    ReadReply => true,
    WriteReply => true,
}

/// What a reply that waits over a ring answers to `begin_wait`: one of its
/// queue's lock-wait entries taken (`hold_for_wait`), or none left.
fn ring_wait(held: bool) -> constellation_vfs::WaitStart {
    if held {
        constellation_vfs::WaitStart::Held(None)
    } else {
        constellation_vfs::WaitStart::Refused
    }
}

pub(crate) struct EmptyReply(pub ReplyEmpty);

/// `unlink`/`rmdir`'s reply, and the directory it changed (plan 42
/// §5.3.1), told before the reply.
pub(crate) struct DirChangeReply {
    pub reply: ReplyEmpty,
    pub parent: Ino,
    pub notice: DirNotice,
}

impl Responder<()> for DirChangeReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => {
                self.notice.tell(&[Some(self.parent)], &[]);
                self.reply.ok()
            }
            Err(e) => self.reply.error(reply_code(e.code())),
        }
    }

    /// As [`EmptyReply`]'s.
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

impl Responder<()> for EmptyReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => self.0.ok(),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }

    /// Over a ring the waiting request holds its entry: one of the
    /// queue's `depth - 1` that lock waits may hold
    /// ([`fuser::ReplyEmpty::hold_for_wait`]).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.0.hold_for_wait())
    }
}

/// A rename's reply, and the dentries it moves in the kernel
/// (`dentries`).
pub(crate) struct RenameReply {
    // Before `reply`: fields drop in declaration order, and a responder
    // dropped without `done` must end its guard before fuser's Drop answers
    // EIO.
    pub names: RenameGuard,
    pub exchange: bool,
    /// Plan 42 §5.3.1: the two directories, told before the reply with
    /// the inode(s) the rename moved.
    pub parents: (Ino, Ino),
    pub notice: DirNotice,
    pub reply: ReplyEmpty,
}

impl Responder<constellation_vfs::Renamed> for RenameReply {
    fn done(self, r: VfsResult<constellation_vfs::Renamed>) {
        match r {
            Ok(moved) => {
                self.notice.tell(
                    &[Some(self.parents.0), Some(self.parents.1)],
                    &[moved.moved, moved.exchanged],
                );
                self.reply.ok();
                self.names.renamed(self.exchange);
            }
            Err(e) => {
                // Nothing moved: the names stop counting as in flight
                // before the kernel has the error (`EntryGuard::failed`).
                let RenameReply { reply, names, .. } = self;
                drop(names);
                reply.error(reply_code(e.code()));
            }
        }
    }

    /// As [`EmptyReply`]'s: a parked rename holds one of the queue's
    /// lock-wait entries over a ring.
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// Gives back the view's side of an open that was answered but must be
/// refused after all (`passthrough`'s module doc: a read-write open of an
/// inode in passthrough mode): a `release` of the handle the view counted.
pub(crate) type Undo = Box<dyn FnOnce(Ino, Fh) + Send>;

/// How an open's reply is shaped by the session's passthrough state
/// (plan 38 §3(b)): the inode, the decoded flags and what the open
/// registered before it called the view.
pub(crate) struct OpenCtx {
    pub ino: Ino,
    pub flags: OpenFlags,
    pub pre: PreOpen,
    pub pt: Arc<PassthroughState>,
    pub undo: Undo,
    /// Plan 42 §5.1.2: decides whether the reply keeps the pages, and
    /// records the drop if it does not.
    pub pc: Arc<PageCacheTable>,
}

impl OpenCtx {
    /// Plan 42 §5.1: whether this open of `ino`, answered `fh` with
    /// `stamp` and the `FOPEN_*` word `flags` (`passthrough`: with a
    /// backing file), keeps the kernel's pages — decided and recorded
    /// before the reply's bytes are written.
    fn keeps(
        &self,
        ino: Ino,
        fh: Fh,
        stamp: Option<constellation_vfs::CacheStamp>,
        flags: FopenFlags,
        passthrough: bool,
        create: bool,
    ) -> bool {
        self.pc.open_reply(OpenFacts {
            ino,
            fh: fh.0,
            stamp,
            plain: !passthrough,
            direct_io: flags.contains(FopenFlags::FOPEN_DIRECT_IO),
            create_trunc: create && self.flags.contains(OpenFlags::TRUNC),
            write_access: crate::adapter::open_flags_write_access(self.flags),
        })
    }
}

/// `flags`, keeping the kernel's pages when `keep`.
fn with_keep(flags: FopenFlags, keep: bool) -> FopenFlags {
    if keep {
        flags | FopenFlags::FOPEN_KEEP_CACHE
    } else {
        flags
    }
}

fn answer_is_ok(answer: &OpenAnswer) -> bool {
    !matches!(answer, OpenAnswer::Refuse(_))
}

/// The `FOPEN_*` word and backing id of an answer that is not a refusal.
fn fopen(answer: OpenAnswer) -> Option<(FopenFlags, Option<u32>)> {
    match answer {
        OpenAnswer::Plain => Some((FopenFlags::empty(), None)),
        OpenAnswer::Passthrough { id, direct_io } => Some((
            if direct_io {
                FopenFlags::FOPEN_DIRECT_IO
            } else {
                FopenFlags::empty()
            },
            Some(id),
        )),
        OpenAnswer::Refuse(_) => None,
    }
}

pub(crate) struct OpenReply {
    pub reply: ReplyOpen,
    pub cx: OpenCtx,
}

impl Responder<Opened> for OpenReply {
    fn done(self, r: VfsResult<Opened>) {
        let OpenReply { reply, cx } = self;
        match r {
            Ok(o) => {
                // Without passthrough on the session `o.backing` is simply
                // dropped here; the engine's pin on it goes at `release`.
                let answer = cx
                    .pt
                    .on_open_reply(cx.ino, cx.flags, cx.pre, o.backing.as_ref());
                let fh = FileHandle(o.fh.0);
                match fopen(answer) {
                    // Plan 38 §3(d): the engine marked the open for
                    // zero-copy reads (only a read-only one, on a session
                    // that negotiated them); passthrough, where the
                    // session gave it, wins above — the kernel then serves
                    // the reads with no daemon involvement at all. Plan 42
                    // §5.1: either may keep the pages.
                    Some((flags, None)) => {
                        let keep = cx.keeps(cx.ino, o.fh, o.cache, flags, false, false);
                        let flags = with_keep(flags, keep);
                        if o.zero_copy {
                            reply.opened_zero_copy(fh, flags)
                        } else {
                            reply.opened(fh, flags)
                        }
                    }
                    Some((flags, Some(id))) => {
                        // Never keeps; recorded as a drop (and S4 for a
                        // direct-I/O one).
                        cx.keeps(cx.ino, o.fh, o.cache, flags, true, false);
                        // SAFETY: `id` is registered on this connection and
                        // stays so until the inode's last passthrough handle
                        // is released (`PassthroughState::release`), which
                        // cannot precede this reply; `into_raw` below keeps
                        // the wrapper from closing it.
                        let backing = unsafe { reply.wrap_backing(id) };
                        reply.opened_passthrough(fh, flags, &backing);
                        let _ = backing.into_raw();
                    }
                    None => {
                        (cx.undo)(cx.ino, o.fh);
                        if let OpenAnswer::Refuse(code) = answer {
                            reply.error(reply_code(code));
                        }
                    }
                }
            }
            Err(e) => {
                cx.pt.open_failed(cx.ino, cx.pre);
                reply.error(reply_code(e.code()))
            }
        }
    }

    /// A parked op's reply holds one of its ring queue's lock-wait entries
    /// (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

pub(crate) struct CreateReply {
    /// The name it hands the kernel (`dentries`), and where the node id
    /// it hands it is counted (`refs`). Before `reply`: fields drop in
    /// declaration order, and a responder dropped without `done` must end
    /// its guard before fuser's Drop answers EIO.
    pub name: EntryGuard,
    /// `ino` is learnt from the reply; `pre` is always `Unregistered`.
    pub cx: OpenCtx,
    /// Plan 42 §5.3.1: the directory the create changed, told before the
    /// reply (also when it opened an existing file: the kernel runs
    /// `fuse_dir_changed` only for a new entry, and the notice is
    /// harmless).
    pub parent: Ino,
    pub notice: DirNotice,
    /// Plan 42 F21: what the request saw when it arrived.
    pub seen: Arrival,
    pub reply: ReplyCreate,
}

impl Responder<(Entry, Opened)> for CreateReply {
    fn done(self, r: VfsResult<(Entry, Opened)>) {
        let CreateReply {
            reply,
            cx,
            name,
            parent,
            notice,
            seen,
        } = self;
        match r {
            Ok((e, o)) => {
                // A create can open an existing inode (no `O_EXCL`), and
                // that inode may be in passthrough mode: the kernel's
                // per-inode rule applies to it exactly as to an open.
                //
                // A `Refuse` here cannot be undone in full: the view's
                // `create` has already run `O_TRUNC` on the existing file
                // (a create, unlike an open, carries it to the view), and
                // the release below gives back only the handle. It is
                // reachable only when the kernel had no positive dentry
                // for a file that exists — another node created it since
                // this one last looked — because otherwise the kernel
                // sends `FUSE_OPEN`, not `FUSE_CREATE`; and only on a
                // writable mount that opted in to passthrough. The caller
                // gets `ETXTBSY` with the truncate kept, as a writer on
                // another node truncating the file would have left it.
                let answer = cx
                    .pt
                    .on_open_reply(e.attr.ino, cx.flags, cx.pre, o.backing.as_ref());
                let fh = FileHandle(o.fh.0);
                let attr = fuse_attr(&e.attr);
                let generation = Generation(e.generation);
                // A3 (plan 42 §5.2.1) and W3b (§5.3.1); a refused open
                // below hands nothing and changed nothing the kernel knows.
                if answer_is_ok(&answer) {
                    cx.pc.size_seen(e.attr.ino, e.attr.size);
                    notice.tell(&[Some(parent)], &[]);
                    notice.replying(seen, e.attr.ino);
                }
                match fopen(answer) {
                    Some((flags, None)) => {
                        let keep = cx.keeps(e.attr.ino, o.fh, o.cache, flags, false, true);
                        let flags = with_keep(flags, keep);
                        // Counted before the bytes go, as an entry reply's.
                        name.hands_out(e.attr.ino);
                        reply.created(&e.attr.ttl, &attr, generation, fh, flags);
                        name.replied(e.attr.ttl);
                    }
                    Some((flags, Some(id))) => {
                        cx.keeps(e.attr.ino, o.fh, o.cache, flags, true, true);
                        // SAFETY: as in `OpenReply`.
                        let backing = unsafe { reply.wrap_backing(id) };
                        name.hands_out(e.attr.ino);
                        reply.created_passthrough(
                            &e.attr.ttl,
                            &attr,
                            generation,
                            fh,
                            flags,
                            &backing,
                        );
                        let _ = backing.into_raw();
                        name.replied(e.attr.ttl);
                    }
                    None => {
                        (cx.undo)(e.attr.ino, o.fh);
                        name.failed(|| {
                            if let OpenAnswer::Refuse(code) = answer {
                                reply.error(reply_code(code));
                            }
                        });
                    }
                }
            }
            Err(e) => name.failed(|| reply.error(reply_code(e.code()))),
        }
    }

    /// A parked op's reply holds one of its ring queue's lock-wait entries
    /// (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// A write's reply. Plan 42 §5.1.3 (S3): a `WRITE` answered with an
/// error or a short count leaves the kernel's folios holding bytes the
/// daemon did not take (the kernel turns a short count into `EIO` and
/// keeps the folios' contents, 1a061c554253), so the file's pages are no
/// longer known current.
///
/// Plan 42 §5.2.3: a write-through write's claim — the reads recorded on
/// its pages — decides whether its reply waits for a repair, which the
/// session's repair lane makes and then answers (`crate::repair`); the
/// worker does not wait.
pub(crate) struct WriteReply {
    pub reply: ReplyWrite,
    pub ino: Ino,
    pub offset: u64,
    /// The bytes the request carried.
    pub len: u32,
    pub lane: Arc<RepairLane>,
    /// The write's registration and in-flight count, taken when the
    /// request arrived (`PageCacheTable::write_begin`).
    pub reg: WriteReg,
}

/// A write's registration, given up if its reply is dropped unanswered
/// (`PageCacheTable::abandon`: a responder dropped without `done`, which
/// fuser answers `EIO`), and its in-flight count, which ends with it.
/// Made by the adapter before the request is handed on, so every way the
/// reply can end passes through it.
pub(crate) struct WriteReg {
    pc: Arc<PageCacheTable>,
    unique: u64,
    ino: Ino,
    claimed: bool,
    /// Dropped with the registration: after the reply, or at the hand-off.
    _counted: Option<InFlightOp>,
}

impl WriteReg {
    /// `counted`: what `PageCacheTable::write_begin` answered.
    pub(crate) fn new(pc: &Arc<PageCacheTable>, ino: Ino, unique: u64, counted: bool) -> Self {
        Self {
            pc: pc.clone(),
            unique,
            ino,
            claimed: false,
            _counted: counted.then(|| InFlightOp::new(pc, ino)),
        }
    }
}

impl Drop for WriteReg {
    fn drop(&mut self) {
        if !self.claimed {
            self.pc.abandon(self.ino, self.unique);
        }
    }
}

impl Responder<u32> for WriteReply {
    fn done(self, r: VfsResult<u32>) {
        let WriteReply {
            reply,
            ino,
            offset,
            len,
            lane,
            mut reg,
        } = self;
        let ok = matches!(r, Ok(n) if n >= len);
        if !ok {
            reg.pc.mark(ino, Mark::S3);
        }
        reg.claimed = true;
        let claim = reg.pc.claim(ino, reg.unique, offset, u64::from(len), ok);
        match (claim, r) {
            (Claim::Repair { ranges, tail }, Ok(n)) => {
                // Handed to the lane: no longer in flight here (the
                // repair's clock runs only while nothing else is).
                drop(reg);
                lane.submit(ino, ranges, tail, Box::new(move || reply.written(n)));
            }
            (_, Ok(n)) => reply.written(n),
            (_, Err(e)) => reply.error(reply_code(e.code())),
        }
    }

    /// A parked op's reply holds one of its ring queue's lock-wait entries
    /// (plan 43 C7-F F-c).
    fn begin_wait(&mut self) -> constellation_vfs::WaitStart {
        ring_wait(self.reply.hold_for_wait())
    }
}

/// `FUSE_COPY_FILE_RANGE`'s answer: the bytes copied, as a write's
/// (`fuse_write_out`). The view copies at most `COPY_MAX` per call, which
/// fits; a count that would not is a short copy of what fits, never a
/// wrapped one (the kernel answers `EIO` to a count above what it asked).
///
/// `EOPNOTSUPP` and `EXDEV` are never sent: either makes the kernel copy
/// this request itself with a splice that reads the source through its
/// page cache (`fuse_copy_file_range`), the path this op exists to close
/// (the engine's `view::copy`). Neither is a code a copy refuses with; if
/// one ever surfaced from beneath, it is answered `EIO`. `ENOSYS` (a view
/// that does not copy) is sent as such: the kernel then stops asking.
pub(crate) struct CopyReply(pub ReplyWrite);

impl Responder<u64> for CopyReply {
    fn done(self, r: VfsResult<u64>) {
        match r {
            Ok(n) => self.0.written(u32::try_from(n).unwrap_or(!0xfff_u32)),
            Err(e) => {
                let code = match e.code() {
                    code @ (Code::NotSupported | Code::CrossDevice) => {
                        tracing::warn!(%code, "copy_file_range failed with a code that would make the kernel splice-copy; answering EIO");
                        Code::Io
                    }
                    code => code,
                };
                self.0.error(reply_code(code))
            }
        }
    }
}

pub(crate) struct StatfsReply(pub ReplyStatfs);

impl Responder<StatFs> for StatfsReply {
    fn done(self, r: VfsResult<StatFs>) {
        match r {
            Ok(s) => self.0.statfs(
                s.blocks, s.bfree, s.bavail, s.files, s.ffree, s.bsize, s.namelen, s.frsize,
            ),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

pub(crate) struct LseekReply(pub ReplyLseek);

impl Responder<u64> for LseekReply {
    fn done(self, r: VfsResult<u64>) {
        match r {
            Ok(offset) => self.0.offset(offset as i64),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// What an xattr reply of `len` bytes answers a request for `size`
/// bytes: the size probe (`size == 0`), `ERANGE` when the buffer is too
/// small, the bytes otherwise.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum XattrAnswer {
    Size(u32),
    TooSmall,
    Data,
}

pub(crate) fn xattr_answer(len: usize, size: u32) -> XattrAnswer {
    if size == 0 {
        XattrAnswer::Size(len as u32)
    } else if (size as usize) < len {
        XattrAnswer::TooSmall
    } else {
        XattrAnswer::Data
    }
}

fn reply_xattr(value: &[u8], size: u32, reply: ReplyXattr) {
    match xattr_answer(value.len(), size) {
        XattrAnswer::Size(n) => reply.size(n),
        XattrAnswer::TooSmall => reply.error(reply_code(Code::Range)),
        XattrAnswer::Data => reply.data(value),
    }
}

/// `getxattr`'s value, under the request's size-probe semantics.
pub(crate) struct XattrReply {
    pub reply: ReplyXattr,
    pub size: u32,
}

impl Responder<Vec<u8>> for XattrReply {
    fn done(self, r: VfsResult<Vec<u8>>) {
        match r {
            Ok(value) => reply_xattr(&value, self.size, self.reply),
            Err(e) => self.reply.error(reply_code(e.code())),
        }
    }
}

/// The names as `listxattr(2)` wants them: each NUL-terminated.
pub(crate) fn encode_xattr_names(names: &[XattrNameBuf]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for name in names {
        encoded.extend_from_slice(name.as_bytes());
        encoded.push(0);
    }
    encoded
}

/// `listxattr`'s names, under the request's size-probe semantics.
pub(crate) struct XattrListReply {
    pub reply: ReplyXattr,
    pub size: u32,
}

impl Responder<Vec<XattrNameBuf>> for XattrListReply {
    fn done(self, r: VfsResult<Vec<XattrNameBuf>>) {
        match r {
            Ok(names) => reply_xattr(&encode_xattr_names(&names), self.size, self.reply),
            Err(e) => self.reply.error(reply_code(e.code())),
        }
    }
}

// `F_RDLCK`/`F_WRLCK`/`F_UNLCK` are `c_int` on Linux (`c_short` on the
// BSDs), and `ReplyLock` speaks `i32`.
#[allow(clippy::unnecessary_cast)]
pub(crate) const F_RDLCK: i32 = libc::F_RDLCK as i32;
#[allow(clippy::unnecessary_cast)]
pub(crate) const F_WRLCK: i32 = libc::F_WRLCK as i32;
#[allow(clippy::unnecessary_cast)]
pub(crate) const F_UNLCK: i32 = libc::F_UNLCK as i32;

pub(crate) struct LockReply(pub ReplyLock);

impl Responder<LockStatus> for LockReply {
    fn done(self, r: VfsResult<LockStatus>) {
        match r {
            Ok(LockStatus::Unlocked) => self.0.locked(0, 0, F_UNLCK, 0),
            Ok(LockStatus::Locked { range, kind, pid }) => {
                let typ = match kind {
                    LockKind::Write => F_WRLCK,
                    LockKind::Read => F_RDLCK,
                };
                self.0.locked(range.start, range.end, typ, pid)
            }
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// `opendir`'s reply: the view's stream id for the directory (plan 40
/// §4.9), and no open flags (the kernel does not cache the directory:
/// `FOPEN_CACHE_DIR` is a follow-up).
pub(crate) struct OpenDirReply(pub ReplyOpen);

impl Responder<Fh> for OpenDirReply {
    fn done(self, r: VfsResult<Fh>) {
        match r {
            Ok(fh) => self.0.opened(FileHandle(fh.0), FopenFlags::empty()),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// `readdir`'s buffer and completion in one (`R: DirSink + Responder<()>`).
pub(crate) struct DirReply(pub ReplyDirectory);

impl DirSink for DirReply {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        self.0
            .add(INodeNo(ino), next, file_type(kind), OsStr::from_bytes(name))
    }
}

impl Responder<()> for DirReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => self.0.ok(),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// The bytes of a `fuse_direntplus` ahead of its name: a `fuse_entry_out`
/// (128) and a `fuse_dirent` (24).
const DIRENTPLUS_HEADER: usize = 152;

/// What a `READDIRPLUS` record of `name` takes in the reply buffer (a name
/// only entry takes the same): fuser's `ReplyDirectoryPlus` refuses a
/// record that does not fit, and writes nothing.
fn direntplus_len(name: &[u8]) -> usize {
    (DIRENTPLUS_HEADER + name.len()).next_multiple_of(8)
}

/// One entry of a `READDIRPLUS` page that handed the kernel a node id.
struct Handed {
    ino: Ino,
    /// The entry's name is `names[prev.name_end..name_end]`.
    name_end: usize,
    /// The entry TTL the dentry was installed with.
    ttl: Duration,
    /// A regular file's size, which the page handed the kernel (A3).
    size: Option<u64>,
}

/// The entries a `READDIRPLUS` reply buffered with a node id, in order,
/// and their names (one buffer, not one allocation per name).
#[derive(Default)]
pub(crate) struct HandedList {
    items: Vec<Handed>,
    names: Vec<u8>,
}

impl HandedList {
    fn push(&mut self, ino: Ino, name: &[u8], ttl: Duration, size: Option<u64>) {
        self.names.extend_from_slice(name);
        self.items.push(Handed {
            ino,
            name_end: self.names.len(),
            ttl,
            size,
        });
    }

    fn iter(&self) -> impl Iterator<Item = (&Handed, &[u8])> {
        let mut start = 0;
        self.items.iter().map(move |h| {
            let name = &self.names[start..h.name_end];
            start = h.name_end;
            (h, name)
        })
    }
}

/// `readdirplus`'s buffer and completion in one (`R: DirSink +
/// Responder<()>`; plan 40 §4.7).
///
/// An entry with attributes is what a `LOOKUP` of its name answers, and
/// the kernel takes it as one: a dentry, an inode and one more lookup of
/// the node id, to be given back by `FORGET`. So this is, per entry, what
/// [`EntryReply`] does, with two differences in *when*:
///
/// - **Only the entries the buffer took are accounted.** A page's entries
///   are offered one at a time and the buffer says no from some entry on
///   (the kernel's request size, `ReplyDirectoryPlus`); that entry, and
///   the ones the view then does not offer, reach the kernel as nothing.
///   Counting one would leave the inode held for good, and a mark for one
///   a notice and a table slot for nothing. So nothing is marked for an
///   entry before the buffer is known to have room for it
///   ([`ReplyDirectoryPlus::remaining_capacity`]).
/// - **Accounting is in [`Responder::done`], on success.** The view
///   answers no error after it added an entry (`Vfs::readdir`), so a page
///   either succeeds with everything it added or hands the kernel an
///   error and nothing. `done(Ok)` takes the references, arms A3 and
///   records the names, all before the bytes are written (a `FORGET` can
///   follow the reply at once, on another worker, and an `Unknown` one is
///   kept from the view for good); an error or a dropped responder
///   accounts nothing.
///
/// What cannot wait for `done` is the attribute reply mark: the reply is
/// on its way to the kernel as soon as it is written, a change of an inode
/// recorded after the request arrived must find the mark, and the entry
/// is not marked after its bytes are in the buffer. So the mark (or the
/// notice that replaces it, `DirNotice::replying_each`) is made when the
/// entry is offered, after the room check and before the bytes are
/// buffered.
pub(crate) struct DirPlusReply {
    pub reply: ReplyDirectoryPlus,
    /// The listed directory (the names are recorded under it).
    pub parent: Ino,
    pub entries: Arc<KernelEntries>,
    /// Plan 42 §5.2.1 (A3): the size a file's entry hands the kernel.
    pub pc: Arc<PageCacheTable>,
    pub notice: DirNotice,
    /// Plan 42 F21: what the request saw when it arrived.
    pub seen: Arrival,
    /// Plan 42 §5.3.2: a directory's attributes get no kernel lifetime
    /// (its entry keeps its TTL: `add_with_ttls`).
    pub dir_zero: bool,
    pub handed: HandedList,
}

impl DirPlusReply {
    fn add_name_only(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        self.reply
            .add_name(INodeNo(ino), next, file_type(kind), OsStr::from_bytes(name))
    }
}

impl DirSink for DirPlusReply {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        self.add_name_only(ino, next, kind, name)
    }

    fn add_entry(
        &mut self,
        ino: Ino,
        next: u64,
        kind: FileKind,
        name: &[u8],
        entry: Option<&Entry>,
    ) -> bool {
        // The root's node id is refused by the kernel (`-EIO`, and a
        // forget of it); no view lists it as a child. Defence in depth.
        let usable = |e: &&Entry| e.attr.ino != ROOT_INO && e.attr.ino != 0;
        let Some(e) = entry.filter(usable) else {
            return self.add_name_only(ino, next, kind, name);
        };
        debug_assert_eq!(e.attr.ino, ino, "an entry's node id is its inode");
        if self.reply.remaining_capacity() < direntplus_len(name) {
            return true;
        }
        if !self.notice.replying_each(self.seen, e.attr.ino) {
            // The table of marks is full (a pid-0 lister of a huge
            // directory): the kernel looks this name up when it needs it.
            return self.add_name_only(ino, next, kind, name);
        }
        let full = self.reply.add_with_ttls(
            INodeNo(e.attr.ino),
            next,
            OsStr::from_bytes(name),
            &e.attr.ttl,
            &attr_ttl(&e.attr, self.dir_zero),
            &fuse_attr(&e.attr),
            Generation(e.generation),
        );
        if !full {
            let size = (e.attr.kind == FileKind::File).then_some(e.attr.size);
            self.handed.push(e.attr.ino, name, e.attr.ttl, size);
        }
        full
    }
}

impl Responder<()> for DirPlusReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => {
                for (h, name) in self.handed.iter() {
                    if let Some(size) = h.size {
                        self.pc.size_seen(h.ino, size);
                    }
                    // Before the bytes go, as `EntryGuard::hands_out`.
                    self.entries.refs().took(h.ino);
                    self.entries.listed(self.parent, name, h.ttl);
                }
                self.reply.ok();
            }
            Err(e) => {
                // The contract: no error after an entry was added. (A
                // dropped responder, which fuser answers `EIO`, never
                // gets here.)
                debug_assert!(
                    self.handed.items.is_empty(),
                    "a readdir page failed ({e:?}) after it added entries"
                );
                self.reply.error(reply_code(e.code()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_types::Rdev;

    /// A write's reply dropped without an answer (fuser then answers
    /// `EIO`) gives its registration up: the pending write goes, the inode
    /// is marked S5 and its in-flight count returns to 0 — else every later
    /// claim of the inode would see a concurrent write and the repair clock
    /// would never run again. (The registration used to be made inside
    /// `done`, so a dropped reply left both behind.)
    #[test]
    fn a_write_reply_dropped_unanswered_gives_its_registration_up() {
        let pc = PageCacheTable::with_page_size(Default::default(), 4096);
        let counted = pc.write_begin(7, 11, false);
        assert!(counted);
        let reg = WriteReg::new(&pc, 7, 11, counted);
        assert_eq!(pc.ops_in_flight(7), 1);
        pc.with_entry(7, |e| assert_eq!(e.unwrap().pending.len(), 1));
        drop(reg);
        assert_eq!(pc.ops_in_flight(7), 0);
        pc.with_entry(7, |e| {
            let e = e.unwrap();
            assert!(e.pending.is_empty());
            assert_ne!(e.marks, 0, "S5");
        });
    }

    /// The FUSE boundary answers every errno the adapter produced before
    /// plan 31 with exactly the same number: each code the workspace uses
    /// against `fuser`'s own (libc-derived) constant.
    #[test]
    fn every_code_the_adapter_uses_leaves_as_its_linux_errno() {
        let expect = [
            (Code::NotFound, Errno::ENOENT),
            (Code::Exists, Errno::EEXIST),
            (Code::NotEmpty, Errno::ENOTEMPTY),
            (Code::Stale, Errno::ESTALE),
            (Code::NoData, Errno::ENODATA),
            (Code::NoData, Errno::NO_XATTR),
            (Code::NameTooLong, Errno::ENAMETOOLONG),
            (Code::NoLock, Errno::ENOLCK),
            (Code::NotSupported, Errno::EOPNOTSUPP),
            (Code::NotSupported, Errno::ENOTSUP),
            (Code::Again, Errno::EAGAIN),
            (Code::Again, Errno::EWOULDBLOCK),
            (Code::Intr, Errno::EINTR),
            (Code::TimedOut, Errno::ETIMEDOUT),
            (Code::Io, Errno::EIO),
            (Code::NotDir, Errno::ENOTDIR),
            (Code::IsDir, Errno::EISDIR),
            (Code::Invalid, Errno::EINVAL),
            (Code::ReadOnly, Errno::EROFS),
            (Code::NoSpace, Errno::ENOSPC),
            (Code::Access, Errno::EACCES),
            (Code::Perm, Errno::EPERM),
            (Code::CrossDevice, Errno::EXDEV),
            (Code::NoDeviceOrAddress, Errno::ENXIO),
            (Code::Range, Errno::ERANGE),
            (Code::NotImplemented, Errno::ENOSYS),
            (Code::FileTooBig, Errno::EFBIG),
            (Code::NotConnected, Errno::ENOTCONN),
            (Code::Busy, Errno::EBUSY),
            (Code::TooBig, Errno::E2BIG),
        ];
        for (code, errno) in expect {
            assert_eq!(reply_code(code), errno, "{code:?}");
        }
        // And no code leaves as something `fuser` would coerce (a
        // non-positive number becomes EIO there).
        for &code in Code::ALL {
            assert_eq!(reply_code(code).code(), code.to_linux_errno(), "{code:?}");
        }
    }

    #[test]
    fn the_kernel_attr_is_what_the_adapter_always_sent() {
        let a = Attr {
            ino: 42,
            kind: FileKind::CharDev,
            size: 1000,
            blocks: 2,
            mode: 0o644,
            nlink: 1,
            uid: 7,
            gid: 8,
            rdev: Rdev { major: 4, minor: 5 },
            atime_ns: -5,
            mtime_ns: 1_500_000_000,
            ctime_ns: 2_000_000_000,
            blksize: 131072,
            ttl: Duration::from_secs(1),
        };
        let f = fuse_attr(&a);
        assert_eq!(f.ino, INodeNo(42));
        assert_eq!((f.size, f.blocks, f.blksize), (1000, 2, 131072));
        assert_eq!(f.kind, FileType::CharDevice);
        assert_eq!(f.perm, 0o644);
        assert_eq!(
            f.atime,
            UNIX_EPOCH - Duration::from_nanos(5),
            "before the epoch"
        );
        assert_eq!(f.mtime, UNIX_EPOCH + Duration::from_millis(1500));
        assert_eq!(f.crtime, f.ctime);
        assert_eq!(f.rdev, (4 << 8) | 5);
    }

    #[test]
    fn xattr_replies_follow_the_size_probe_protocol() {
        assert_eq!(xattr_answer(5, 0), XattrAnswer::Size(5));
        assert_eq!(xattr_answer(5, 4), XattrAnswer::TooSmall);
        assert_eq!(xattr_answer(5, 5), XattrAnswer::Data);
        assert_eq!(xattr_answer(0, 0), XattrAnswer::Size(0));
        let names: Vec<XattrNameBuf> = vec!["user.a".into(), "user.bb".into()];
        assert_eq!(encode_xattr_names(&names), b"user.a\0user.bb\0");
        assert!(encode_xattr_names(&[]).is_empty());
    }
}
