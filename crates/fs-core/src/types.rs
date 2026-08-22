//! Core filesystem types shared across crates.

use serde::{Deserialize, Serialize};

/// Inode number. 1 is the filesystem root (FUSE convention).
pub type Ino = u64;

pub const ROOT_INO: Ino = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    BlockDev,
    CharDev,
}

impl InodeKind {
    pub fn as_u8(self) -> u8 {
        match self {
            InodeKind::File => 0,
            InodeKind::Dir => 1,
            InodeKind::Symlink => 2,
            InodeKind::Fifo => 3,
            InodeKind::Socket => 4,
            InodeKind::BlockDev => 5,
            InodeKind::CharDev => 6,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(InodeKind::File),
            1 => Some(InodeKind::Dir),
            2 => Some(InodeKind::Symlink),
            3 => Some(InodeKind::Fifo),
            4 => Some(InodeKind::Socket),
            5 => Some(InodeKind::BlockDev),
            6 => Some(InodeKind::CharDev),
            _ => None,
        }
    }

    /// Kinds created by mknod: no data plane, metadata only (the kernel
    /// implements FIFO/socket I/O; device access is governed by `nodev`).
    pub fn is_special(self) -> bool {
        matches!(
            self,
            InodeKind::Fifo | InodeKind::Socket | InodeKind::BlockDev | InodeKind::CharDev
        )
    }
}

/// File attributes as stored in the metadata replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileAttr {
    pub ino: Ino,
    pub kind: InodeKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    /// Access time, nanoseconds since the unix epoch (noatime semantics:
    /// set by utimensat/touch, not implicitly by reads).
    #[serde(default)]
    pub atime_ns: i64,
    /// Modification time, nanoseconds since the unix epoch.
    pub mtime_ns: i64,
    /// Change (attribute) time, nanoseconds since the unix epoch.
    pub ctime_ns: i64,
    /// Device number for block/char device nodes, 0 otherwise.
    #[serde(default)]
    pub rdev: u64,
}

impl FileAttr {
    pub fn new_dir(ino: Ino, mode: u32, uid: u32, gid: u32, now_ns: i64) -> Self {
        Self {
            ino,
            kind: InodeKind::Dir,
            size: 0,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink: 2,
            atime_ns: now_ns,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
            rdev: 0,
        }
    }

    pub fn new_file(ino: Ino, mode: u32, uid: u32, gid: u32, now_ns: i64) -> Self {
        Self {
            ino,
            kind: InodeKind::File,
            size: 0,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink: 1,
            atime_ns: now_ns,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
            rdev: 0,
        }
    }

    pub fn new_symlink(ino: Ino, uid: u32, gid: u32, now_ns: i64, target_len: u64) -> Self {
        Self {
            ino,
            kind: InodeKind::Symlink,
            size: target_len,
            mode: 0o777,
            uid,
            gid,
            nlink: 1,
            atime_ns: now_ns,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
            rdev: 0,
        }
    }

    /// Special node (fifo/socket/device) created by mknod.
    pub fn new_special(
        ino: Ino,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
        now_ns: i64,
    ) -> Self {
        debug_assert!(kind.is_special());
        Self {
            ino,
            kind,
            size: 0,
            mode: mode & 0o7777,
            uid,
            gid,
            nlink: 1,
            atime_ns: now_ns,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
            rdev,
        }
    }
}

/// Current time as nanoseconds since the unix epoch.
pub fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
