//! The ops' argument and result types.
//!
//! Every type here is the portable, decoded form of what a frontend
//! received: the FUSE adapter turns `O_*` open flags, `FALLOC_FL_*`,
//! `SEEK_*`, `XATTR_*`, `RENAME_*` and `F_*LCK` into these at its edge, and
//! turns [`Attr`]/[`Entry`]/[`StatFs`] back into its kernel's structs.
//! A bit a frontend received that this contract does not name is kept as
//! the `UNSUPPORTED` bit of the set it belongs to, so the engine refuses it
//! exactly where it always did (never earlier, at the frontend), keeping
//! the order in which an op's checks answer.

use crate::{VfsError, VfsResult};
use bytes::Bytes;
use constellation_types::{Code, Rdev};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// An inode number, in the numbering the view shows its frontend (the
/// view's root is [`ROOT_INO`], whatever inode it is underneath).
pub type Ino = u64;

/// The root of every view.
pub const ROOT_INO: Ino = 1;

/// A handle a frontend got from `open`/`create` and passes back on the
/// file ops. Opaque to the frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fh(pub u64);

/// The kind of an inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileKind {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    BlockDev,
    CharDev,
}

/// POSIX `st_mode` file-type bits, as `mknod` receives them. The values
/// are the traditional ones every Unix shares; a frontend of a platform
/// without them (Windows) synthesizes them.
pub mod mode {
    pub const S_IFMT: u32 = 0o170_000;
    pub const S_IFSOCK: u32 = 0o140_000;
    pub const S_IFLNK: u32 = 0o120_000;
    pub const S_IFREG: u32 = 0o100_000;
    pub const S_IFBLK: u32 = 0o060_000;
    pub const S_IFDIR: u32 = 0o040_000;
    pub const S_IFCHR: u32 = 0o020_000;
    pub const S_IFIFO: u32 = 0o010_000;
}

/// An inode's attributes, and how long the frontend may cache them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr {
    pub ino: Ino,
    pub kind: FileKind,
    pub size: u64,
    /// 512-byte blocks.
    pub blocks: u64,
    /// Permission bits (as stored; may carry type bits too).
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: Rdev,
    /// Nanoseconds since the unix epoch.
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    /// The preferred I/O size.
    pub blksize: u32,
    /// How long the frontend may answer from its cache of these
    /// attributes (and of the entry that led to them) without asking.
    /// Zero under `--cto strict` (plan 30 §M8).
    pub ttl: Duration,
}

/// A name's resolution: the inode it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub attr: Attr,
    pub generation: u64,
}

/// A time to set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSet {
    /// The engine's clock (the node's HLC): never below a stamp it applied.
    Now,
    /// Nanoseconds since the unix epoch.
    At(i64),
}

/// What a `setattr` changes (`None`: left as it is).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<TimeSet>,
    pub mtime: Option<TimeSet>,
}

/// An `open`/`create`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opened {
    pub fh: Fh,
    /// The chunk file this open may be served from directly, when the
    /// engine found the whole file to be one cached, verified chunk
    /// (plan 38 §3(c)). `None` — always, for every frontend but Linux
    /// FUSE, and for most opens even there — means "answer this handle's
    /// reads the ordinary way", which is what every frontend that does
    /// not know about [`PassthroughChunk`] does by simply dropping it.
    pub backing: Option<PassthroughChunk>,
    /// This handle's reads may be answered from chunk files
    /// ([`ReadData::zero_copy`], plan 38 §3(d)): set only for a read-only
    /// open of a frontend that negotiated zero-copy reads
    /// ([`crate::FrontendCaps::zero_copy`]), which then marks the open so
    /// its kernel hands it the reader's pages (Linux FUSE:
    /// `FOPEN_IO_URING_ZERO_COPY`). Which reads actually are zero-copy is
    /// decided per read; the rest of them come back as bytes.
    pub zero_copy: bool,
}

impl Opened {
    /// A handle answered the ordinary way (no backing file).
    pub fn new(fh: Fh) -> Self {
        Self {
            fh,
            backing: None,
            zero_copy: false,
        }
    }
}

/// The one chunk file a passthrough-eligible open is allowed to be read
/// from directly (plan 38 §3(c)): the whole of the file's committed
/// content is this chunk, it is resident in the local disk cache, and
/// this process has verified it.
///
/// `fd` is that file, opened read-only by the engine — handed over open
/// so the frontend needs no second `open(2)` and cannot be given a path
/// that was unlinked in between. It is a [`std::fs::File`] rather than a
/// `std::os::fd::OwnedFd` because this crate stays portable (`std::os::fd`
/// does not exist on Windows, where `constellation-vfs` still
/// type-checks — `tools/check-cross-known-failures.txt`); on Unix `&File`
/// is exactly the `impl AsFd` a FUSE frontend hands to `fuser`'s
/// `open_backing`. It is shared ([`Arc`]) because the engine keeps the
/// same open file description alive next to the disk-cache pin that keeps
/// the chunk from being evicted, for as long as the handle lives.
#[derive(Debug, Clone)]
pub struct PassthroughChunk {
    pub fd: Arc<std::fs::File>,
    /// The chunk file's length, which is the file's whole committed length.
    pub len: u64,
    /// The chunk's content hash (`constellation_fs_core::ChunkHash`'s
    /// bytes), carried for diagnostics and for the frontend's own
    /// per-backing bookkeeping.
    pub hash: [u8; 32],
}

/// Two backings are the same backing when they are the same open file
/// description for the same chunk. `std::fs::File` has no equality of its
/// own, and comparing descriptor numbers would call a reopened file equal
/// to the one it replaced.
impl PartialEq for PassthroughChunk {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.fd, &other.fd) && self.len == other.len && self.hash == other.hash
    }
}

impl Eq for PassthroughChunk {}

/// The frontend's identity for an opener (NFSv4's open-owner). FUSE has
/// none ([`OpenOwner::NONE`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenOwner(pub u64);

impl OpenOwner {
    pub const NONE: OpenOwner = OpenOwner(0);
}

/// A lock owner (POSIX: the process's file table; `flock`: the open file).
/// `flush`/`release` drop its locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockOwner(pub u64);

macro_rules! bit_set {
    ($(#[$doc:meta])* $name:ident { $($(#[$fdoc:meta])* $flag:ident = $bit:expr,)* }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub struct $name(u32);

        impl $name {
            $($(#[$fdoc])* pub const $flag: $name = $name($bit);)*

            pub const fn empty() -> Self {
                Self(0)
            }

            pub const fn bits(self) -> u32 {
                self.0
            }

            pub const fn from_bits(bits: u32) -> Self {
                Self(bits)
            }

            /// Whether every bit of `other` is set.
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }

            /// Whether any bit of `other` is set.
            pub const fn intersects(self, other: Self) -> bool {
                self.0 & other.0 != 0
            }

            pub const fn is_empty(self) -> bool {
                self.0 == 0
            }

            pub const fn union(self, other: Self) -> Self {
                Self(self.0 | other.0)
            }
        }

        impl std::ops::BitOr for $name {
            type Output = Self;
            fn bitor(self, other: Self) -> Self {
                self.union(other)
            }
        }

        impl std::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, other: Self) {
                *self = self.union(other);
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut set = f.debug_set();
                $(if self.contains(Self::$flag) { set.entry(&stringify!($flag)); })*
                set.finish()
            }
        }
    };
}

bit_set!(
    /// An open's flags, decoded. The access mode is `READ`, `WRITE` or
    /// both (a frontend maps an access mode it cannot name to both, as
    /// the kernel's `O_ACCMODE` check does).
    OpenFlags {
        READ = 1,
        WRITE = 1 << 1,
        CREATE = 1 << 2,
        EXCL = 1 << 3,
        TRUNC = 1 << 4,
        APPEND = 1 << 5,
        /// `O_SYNC` or `O_DSYNC`: every write publishes before it returns.
        SYNC = 1 << 6,
    }
);

bit_set!(
    /// `fallocate`'s mode, decoded.
    FallocateMode {
        KEEP_SIZE = 1,
        PUNCH_HOLE = 1 << 1,
        ZERO_RANGE = 1 << 2,
        /// A mode bit the contract does not name: refused (`NotSupported`).
        UNSUPPORTED = 1 << 31,
    }
);

bit_set!(
    /// `setxattr`'s flags, decoded (see [`SetXattrFlags::mode`]).
    SetXattrFlags {
        CREATE = 1,
        REPLACE = 1 << 1,
        /// A flag bit the contract does not name: refused (`Invalid`).
        UNSUPPORTED = 1 << 31,
    }
);

bit_set!(
    /// `rename`'s flags, decoded.
    RenameFlags {
        NOREPLACE = 1,
        EXCHANGE = 1 << 1,
        WHITEOUT = 1 << 2,
        UNSUPPORTED = 1 << 31,
    }
);

/// What a `setxattr` may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetXattrMode {
    /// Create or replace.
    Set,
    /// Only create (`EEXIST` if present).
    Create,
    /// Only replace (`ENODATA` if absent).
    Replace,
}

impl SetXattrFlags {
    /// The mode these flags ask for; `Invalid` for both at once or any
    /// other bit.
    pub fn mode(self) -> Result<SetXattrMode, Code> {
        match self {
            f if f == Self::empty() => Ok(SetXattrMode::Set),
            f if f == Self::CREATE => Ok(SetXattrMode::Create),
            f if f == Self::REPLACE => Ok(SetXattrMode::Replace),
            _ => Err(Code::Invalid),
        }
    }
}

/// `lseek`'s `whence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekWhence {
    Set,
    Cur,
    End,
    /// The next offset at or after the given one holding data.
    Data,
    /// The next hole at or after the given one (end of file counts).
    Hole,
}

/// How durable an `fsync` makes the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Durability {
    /// What the view is configured for (`--fsync-mode`): what `fsync(2)`
    /// asks for through FUSE.
    Configured,
    /// On this node's disk (the journal and the staged data synced).
    Local,
    /// In the shared log (S3): what `--fsync-mode s3` makes every fsync.
    Durable,
}

/// A byte range a lock covers: `start..=end` (FUSE's `end` for "to the
/// end of the file" is `i64::MAX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockRange {
    pub start: u64,
    pub end: u64,
}

/// A lock's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    Read,
    Write,
}

/// One lock request: whose, where, what (and, for POSIX locks, the pid a
/// conflicting `F_GETLK` reports).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockSpec {
    pub owner: LockOwner,
    pub range: LockRange,
    pub kind: LockKind,
    pub pid: u32,
}

/// A lock test's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockStatus {
    /// Nothing conflicts.
    Unlocked,
    /// This lock conflicts (pid 0: another node's).
    Locked {
        range: LockRange,
        kind: LockKind,
        pid: u32,
    },
}

/// `statfs`'s answer, in `bsize` blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatFs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

/// A file a read is answered from without its bytes passing through this
/// process (plan 38 §3(d)): a verified chunk file of the local disk cache,
/// opened read-only, and whatever keeps it from being evicted while a read
/// of it is in flight. The frontend's kernel reads it straight into the
/// reader's pages (Linux FUSE over a zero-copy io_uring queue:
/// `IORING_OP_READ_FIXED`).
///
/// `hold` is opaque here (the engine's disk-cache pin): this crate stays
/// independent of the cache, and nothing but its `Drop` matters to a
/// frontend. A [`ZeroCopyRead`] shares the source ([`Arc`]), so the hold
/// lasts until the last read that names it is done — on whatever thread
/// the frontend finishes it — and not merely until the engine answers.
pub struct ZeroCopySource {
    file: std::fs::File,
    _hold: Box<dyn std::any::Any + Send + Sync>,
}

impl ZeroCopySource {
    pub fn new(file: std::fs::File, hold: impl std::any::Any + Send + Sync) -> Self {
        Self {
            file,
            _hold: Box::new(hold),
        }
    }

    /// The file, open read-only.
    pub fn file(&self) -> &std::fs::File {
        &self.file
    }

    /// `len` bytes from `offset`, read by this process: what a frontend
    /// that cannot hand the file to its kernel answers with instead
    /// ([`ReadData::contiguous`]). All of them or an error: the engine
    /// names only a range the file holds, so a file that ends before it
    /// is `UnexpectedEof`, never a short answer.
    pub fn read_range(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut done = 0;
        while done < len {
            #[cfg(unix)]
            let n = std::os::unix::fs::FileExt::read_at(
                &self.file,
                &mut buf[done..],
                offset + done as u64,
            )?;
            #[cfg(windows)]
            let n = std::os::windows::fs::FileExt::seek_read(
                &self.file,
                &mut buf[done..],
                offset + done as u64,
            )?;
            #[cfg(not(any(unix, windows)))]
            let n = {
                let _ = offset;
                0
            };
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the file ends {done} bytes into a {len}-byte zero-copy range"),
                ));
            }
            done += n;
        }
        Ok(buf)
    }
}

impl fmt::Debug for ZeroCopySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZeroCopySource")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

/// A read answered as `len` bytes of `source` from `offset`
/// ([`ReadData::zero_copy`]).
#[derive(Debug, Clone)]
pub struct ZeroCopyRead {
    pub source: Arc<ZeroCopySource>,
    pub offset: u64,
    pub len: usize,
}

/// The same read of the same open source.
impl PartialEq for ZeroCopyRead {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source, &other.source)
            && self.offset == other.offset
            && self.len == other.len
    }
}

impl Eq for ZeroCopyRead {}

/// A read's bytes: a scatter list of shared buffers, so the engine can
/// answer from its cache without a copy. Almost always one segment.
///
/// Or, on a handle opened with [`Opened::zero_copy`], a range of a file
/// the frontend's kernel is to read itself ([`Self::zero_copy`]): then
/// there are no segments. The engine answers that way only to a frontend
/// that negotiated it ([`crate::FrontendCaps::zero_copy`]); any other
/// still gets the right bytes from [`Self::contiguous`], read here (or
/// `EIO` if they cannot be read).
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ReadData {
    segments: SmallVec<[Bytes; 4]>,
    zero_copy: Option<ZeroCopyRead>,
}

impl ReadData {
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(bytes))
    }

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut segments = SmallVec::new();
        segments.push(bytes);
        Self {
            segments,
            zero_copy: None,
        }
    }

    /// `len` bytes of `source` from `offset`, for the frontend's kernel to
    /// read (plan 38 §3(d)).
    pub fn zero_copy(source: Arc<ZeroCopySource>, offset: u64, len: usize) -> Self {
        Self {
            segments: SmallVec::new(),
            zero_copy: Some(ZeroCopyRead {
                source,
                offset,
                len,
            }),
        }
    }

    /// The file range this read is answered from, if it is a zero-copy one.
    pub fn as_zero_copy(&self) -> Option<&ZeroCopyRead> {
        self.zero_copy.as_ref()
    }

    pub fn push(&mut self, bytes: Bytes) {
        debug_assert!(
            self.zero_copy.is_none(),
            "bytes pushed onto a zero-copy read"
        );
        self.segments.push(bytes);
    }

    /// The bytes, in order; empty for a zero-copy read.
    pub fn segments(&self) -> &[Bytes] {
        &self.segments
    }

    pub fn len(&self) -> usize {
        match &self.zero_copy {
            Some(zc) => zc.len,
            None => self.segments.iter().map(Bytes::len).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes in one slice: borrowed when there is one segment (the
    /// common case, no copy), gathered otherwise. A zero-copy read is read
    /// from its file here (for a reader that is not the frontend's kernel:
    /// the control plane's `browse`, a frontend that never negotiated
    /// zero-copy): all of its bytes, or `Code::Io` if the file cannot give
    /// them, never a short answer. Byte reads cannot fail.
    pub fn contiguous(&self) -> VfsResult<Cow<'_, [u8]>> {
        if let Some(zc) = &self.zero_copy {
            return match zc.source.read_range(zc.offset, zc.len) {
                Ok(bytes) => Ok(Cow::Owned(bytes)),
                Err(error) => {
                    tracing::error!(%error, "reading a zero-copy read's file failed; answering EIO");
                    Err(VfsError::new(Code::Io))
                }
            };
        }
        Ok(match self.segments.as_slice() {
            [] => Cow::Borrowed(&[]),
            [one] => Cow::Borrowed(one),
            many => Cow::Owned(many.iter().flat_map(|b| b.iter().copied()).collect()),
        })
    }
}

impl fmt::Debug for ReadData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadData")
            .field("segments", &self.segments.len())
            .field("len", &self.len())
            .field("zero_copy", &self.zero_copy.is_some())
            .finish()
    }
}

/// A write's bytes: borrowed from the frontend's request buffer when the
/// write completes inline, shared when it must outlive the call.
#[derive(Clone, Debug)]
pub enum WriteData<'a> {
    Borrowed(&'a [u8]),
    Shared(Bytes),
}

impl WriteData<'_> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            WriteData::Borrowed(bytes) => bytes,
            WriteData::Shared(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setxattr_flags_name_one_mode_each_and_refuse_the_rest() {
        assert_eq!(SetXattrFlags::empty().mode(), Ok(SetXattrMode::Set));
        assert_eq!(SetXattrFlags::CREATE.mode(), Ok(SetXattrMode::Create));
        assert_eq!(SetXattrFlags::REPLACE.mode(), Ok(SetXattrMode::Replace));
        assert_eq!(
            (SetXattrFlags::CREATE | SetXattrFlags::REPLACE).mode(),
            Err(Code::Invalid)
        );
        assert_eq!(SetXattrFlags::UNSUPPORTED.mode(), Err(Code::Invalid));
    }

    #[test]
    fn read_data_is_contiguous_without_a_copy_when_it_is_one_segment() {
        let data = ReadData::from_vec(b"abc".to_vec());
        let seg_ptr = data.segments()[0].as_ptr();
        match data.contiguous().unwrap() {
            Cow::Borrowed(b) => assert_eq!(b.as_ptr(), seg_ptr),
            Cow::Owned(_) => panic!("one segment was copied"),
        }
        let mut two = data.clone();
        two.push(Bytes::from_static(b"de"));
        assert_eq!(&*two.contiguous().unwrap(), b"abcde");
        assert_eq!(two.len(), 5);
        assert!(ReadData::default().is_empty());
    }

    #[test]
    fn bit_sets_combine_and_debug_by_name() {
        let f = OpenFlags::READ | OpenFlags::TRUNC;
        assert!(f.contains(OpenFlags::READ));
        assert!(!f.contains(OpenFlags::WRITE));
        assert!(f.intersects(OpenFlags::TRUNC | OpenFlags::SYNC));
        assert_eq!(format!("{f:?}"), "{\"READ\", \"TRUNC\"}");
    }
}
