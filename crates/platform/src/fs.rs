//! The filesystem capabilities that differ between hosts (plan 31 §2
//! point 2): hole punching, preallocation, fsync strength and page-cache
//! advice. Everything else the engine does to local files is plain
//! `std::fs`.

use std::fs::File;
use std::io;

pub trait FsPrimitives: Send + Sync {
    /// Deallocate `len` bytes at `offset` without changing the file's
    /// length: reads there return zeros, and the blocks go back to the
    /// filesystem (Linux `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`,
    /// macOS `F_PUNCHHOLE`). A filesystem that cannot punch holes answers
    /// an error the caller may ignore when the bytes are only an
    /// optimisation to free (staging's are: they are already durable in
    /// the chunk cache).
    fn punch_hole(&self, file: &File, offset: u64, len: u64) -> io::Result<()>;

    /// Allocate blocks for `len` bytes at `offset`, so later writes there
    /// cannot fail for lack of space. With `keep_size` the file's length
    /// does not change; without it the file grows to cover the range.
    fn preallocate(&self, file: &File, offset: u64, len: u64, keep_size: bool) -> io::Result<()>;

    /// Flush `file` to stable storage, through the drive's own cache
    /// where the host distinguishes (macOS `F_FULLFSYNC`; on Linux
    /// `fsync` already asks the device to flush).
    fn full_fsync(&self, file: &File) -> io::Result<()>;

    /// Advise the host to drop `file`'s cached pages for `len` bytes at
    /// `offset` (`len == 0`: to the end), so the next read comes from the
    /// filesystem (Linux `POSIX_FADV_DONTNEED`). Advisory: an error only
    /// means the pages may stay.
    fn drop_cache(&self, file: &File, offset: u64, len: u64) -> io::Result<()>;
}
