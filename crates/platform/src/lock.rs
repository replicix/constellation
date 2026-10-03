//! Advisory whole-file locks: `daemon.lock` (one daemon per state dir),
//! the takeover mutex beside it, and the writer locks of the registry and
//! the E2E pins.
//!
//! On Linux and macOS these are `flock(2)` locks: owned by the open file
//! description, released when its last descriptor closes — including by
//! the kernel at process exit, which is what makes a leaked
//! [`LockGuard`] a "held for the rest of this process's life" lock. Plan
//! 35 maps the same shape onto `LockFileEx`.

use std::fs::File;
use std::io;
use std::path::Path;

/// A held lock: the locked file, released when this is dropped (the
/// descriptor closes). `std::mem::forget` keeps it for the process's
/// lifetime.
#[derive(Debug)]
pub struct LockGuard {
    file: File,
}

impl LockGuard {
    /// Only [`FileLock`] implementations make guards, after locking.
    pub fn new(file: File) -> LockGuard {
        LockGuard { file }
    }

    pub fn file(&self) -> &File {
        &self.file
    }
}

pub trait FileLock: Send + Sync {
    /// Take an exclusive lock on `file`, waiting for as long as another
    /// holder has it.
    fn lock(&self, file: File) -> io::Result<LockGuard>;

    /// Take an exclusive lock on `file` if nobody holds one: `Ok(None)`
    /// when another open file holds it (the OS said "would block",
    /// [`constellation_types::Code::Again`]), an error for anything else.
    fn try_lock(&self, file: File) -> io::Result<Option<LockGuard>>;

    /// The pids that may hold the lock on `path`, when the host can tell
    /// (Linux: `/proc/locks`). Empty when nobody holds it, or the host
    /// cannot say. More than the holder may show: `/proc/locks` names a
    /// file only by device and inode number, which other files can share
    /// (btrfs numbers every subvolume's inodes from 257 under one
    /// device), so a pid here holds *a* lock under the same name — confirm
    /// it with [`FileLock::opened_by`] before acting on it.
    fn holder_pids(&self, path: &Path) -> Vec<u32>;

    /// Whether process `pid` has `path`'s very file (the same device and
    /// inode as `stat(path)` reports) open, as far as the host can tell:
    /// `false` when it does not, or when that cannot be seen (no `/proc`,
    /// another user's process).
    fn opened_by(&self, pid: u32, path: &Path) -> bool;
}

/// Open (creating, never truncating) the file a lock is taken on.
pub fn open_lock_file(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
}
