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

use crate::dentries::{EntryGuard, RenameGuard};
use crate::passthrough::{OpenAnswer, PassthroughState, PreOpen};
use constellation_types::Code;
use constellation_vfs::{
    Attr, DirSink, Entry, Fh, FileKind, Ino, LockKind, LockStatus, OpenFlags, Opened, ReadData,
    Responder, StatFs, VfsResult, XattrNameBuf,
};
use fuser::{
    Errno, FileHandle, FileType, FopenFlags, Generation, INodeNo, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Transport,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// The Linux FUSE boundary (plan 31 §7): the one place a portable [`Code`]
/// becomes the kernel's errno number. Everything below the reply carries
/// `Code`; nothing here ever holds a raw errno.
pub fn reply_code(code: Code) -> Errno {
    Errno::from_i32(code.to_linux_errno())
}

/// The kernel's attribute struct for `attr`.
pub(crate) fn fuse_attr(a: &Attr) -> fuser::FileAttr {
    let ts = |ns: i64| {
        if ns >= 0 {
            UNIX_EPOCH + Duration::from_nanos(ns as u64)
        } else {
            UNIX_EPOCH
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

/// An entry reply, and the name it hands the kernel (`dentries`).
pub(crate) struct EntryReply {
    pub reply: ReplyEntry,
    pub name: EntryGuard,
}

impl Responder<Entry> for EntryReply {
    fn done(self, r: VfsResult<Entry>) {
        match r {
            Ok(e) => {
                self.reply
                    .entry(&e.attr.ttl, &fuse_attr(&e.attr), Generation(e.generation));
                self.name.replied(e.attr.ttl);
            }
            Err(e) => self.reply.error(reply_code(e.code())),
        }
    }
}

pub(crate) struct AttrReply(pub ReplyAttr);

impl Responder<Attr> for AttrReply {
    fn done(self, r: VfsResult<Attr>) {
        match r {
            Ok(a) => self.0.attr(&a.ttl, &fuse_attr(&a)),
            Err(e) => self.0.error(reply_code(e.code())),
        }
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
pub(crate) struct ReadReply {
    pub reply: ReplyData,
    pub size: u32,
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
        match self.reply.transport() {
            Transport::DevFuse => self.reply.data(&data.contiguous()),
            _ => self.reply.gather(data.segments()),
        }
    }
}

pub(crate) struct EmptyReply(pub ReplyEmpty);

impl Responder<()> for EmptyReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => self.0.ok(),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

/// A rename's reply, and the dentries it moves in the kernel
/// (`dentries`).
pub(crate) struct RenameReply {
    pub reply: ReplyEmpty,
    pub names: RenameGuard,
    pub exchange: bool,
}

impl Responder<()> for RenameReply {
    fn done(self, r: VfsResult<()>) {
        match r {
            Ok(()) => {
                self.reply.ok();
                self.names.renamed(self.exchange);
            }
            Err(e) => self.reply.error(reply_code(e.code())),
        }
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
                    Some((flags, None)) => reply.opened(fh, flags),
                    Some((flags, Some(id))) => {
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
}

pub(crate) struct CreateReply {
    pub reply: ReplyCreate,
    /// `ino` is learnt from the reply; `pre` is always `Unregistered`.
    pub cx: OpenCtx,
    /// The name it hands the kernel (`dentries`).
    pub name: EntryGuard,
}

impl Responder<(Entry, Opened)> for CreateReply {
    fn done(self, r: VfsResult<(Entry, Opened)>) {
        let CreateReply { reply, cx, name } = self;
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
                match fopen(answer) {
                    Some((flags, None)) => {
                        reply.created(&e.attr.ttl, &attr, generation, fh, flags);
                        name.replied(e.attr.ttl);
                    }
                    Some((flags, Some(id))) => {
                        // SAFETY: as in `OpenReply`.
                        let backing = unsafe { reply.wrap_backing(id) };
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
                        if let OpenAnswer::Refuse(code) = answer {
                            reply.error(reply_code(code));
                        }
                    }
                }
            }
            Err(e) => reply.error(reply_code(e.code())),
        }
    }
}

pub(crate) struct WriteReply(pub ReplyWrite);

impl Responder<u32> for WriteReply {
    fn done(self, r: VfsResult<u32>) {
        match r {
            Ok(n) => self.0.written(n),
            Err(e) => self.0.error(reply_code(e.code())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_types::Rdev;

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
        assert_eq!(f.atime, UNIX_EPOCH, "before the epoch reads as the epoch");
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
