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

use constellation_types::Code;
use constellation_vfs::{
    Attr, DirSink, Entry, FileKind, Ino, LockKind, LockStatus, Opened, ReadData, Responder, StatFs,
    VfsResult, XattrNameBuf,
};
use fuser::{
    Errno, FileHandle, FileType, FopenFlags, Generation, INodeNo, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyLseek, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
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

pub(crate) struct EntryReply(pub ReplyEntry);

impl Responder<Entry> for EntryReply {
    fn done(self, r: VfsResult<Entry>) {
        match r {
            Ok(e) => self
                .0
                .entry(&e.attr.ttl, &fuse_attr(&e.attr), Generation(e.generation)),
            Err(e) => self.0.error(reply_code(e.code())),
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

pub(crate) struct ReadReply(pub ReplyData);

impl Responder<ReadData> for ReadReply {
    fn done(self, r: VfsResult<ReadData>) {
        match r {
            Ok(data) => self.0.data(&data.contiguous()),
            Err(e) => self.0.error(reply_code(e.code())),
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

pub(crate) struct OpenReply(pub ReplyOpen);

impl Responder<Opened> for OpenReply {
    fn done(self, r: VfsResult<Opened>) {
        match r {
            Ok(o) => self.0.opened(FileHandle(o.fh.0), FopenFlags::empty()),
            Err(e) => self.0.error(reply_code(e.code())),
        }
    }
}

pub(crate) struct CreateReply(pub ReplyCreate);

impl Responder<(Entry, Opened)> for CreateReply {
    fn done(self, r: VfsResult<(Entry, Opened)>) {
        match r {
            Ok((e, o)) => self.0.created(
                &e.attr.ttl,
                &fuse_attr(&e.attr),
                Generation(e.generation),
                FileHandle(o.fh.0),
                FopenFlags::empty(),
            ),
            Err(e) => self.0.error(reply_code(e.code())),
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
