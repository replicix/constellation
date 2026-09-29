//! What the engine tells the frontend serving a view (plan 31 C3 seam;
//! C4 grows it into `constellation-vfs`'s `FrontendEvents`, §6.5).
//!
//! In C3 the one event is a cluster lock grant's: the file's cached pages
//! and attributes in the kernel may predate what the previous holder
//! wrote, so the frontend drops them (`locks::ClusterLocks`). The FUSE
//! adapter implements it with its kernel invalidation thread
//! (`cli/src/kernel_inval.rs`, `InodeInvalidator`), whose module doc
//! explains why a notification is never sent from a request handler
//! directly and why a wait for one is bounded.

use constellation_fs_core::Ino;
use std::time::Duration;

/// The frontend's side of the engine's events (see the module doc).
pub trait FrontendEvents: Send + Sync {
    /// Drop the frontend's cached pages and attributes of `ino`; not
    /// waited for (callers are request handlers, which the notification
    /// may be queued behind).
    fn invalidate_inode(&self, ino: Ino);

    /// As [`Self::invalidate_inode`], waiting up to `timeout` for it to
    /// be done; `false` if it was not by then.
    fn invalidate_inode_and_wait(&self, ino: Ino, timeout: Duration) -> bool;
}
