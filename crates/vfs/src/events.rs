//! What the engine tells a frontend: [`FrontendEvents`] (plan 31 §6.5).
//!
//! Today's one event is a cache invalidation: another node's write (or a
//! lock grant) made something the frontend's kernel may have cached
//! stale — a name, an inode's attributes, a file's pages. The engine's
//! delivery machinery (`constellation_engine::kernel_inval`, whose module
//! doc is the design note for this contract, with the zombie-daemon
//! incident behind it) calls [`FrontendEvents::invalidate`] from one
//! dedicated notifier thread, never from an op, never while holding an
//! engine lock, holds a notification back while an op on the same inode
//! is in flight, and drops one older than the attribute TTL.

use crate::name::NameBuf;
use crate::types::Ino;

/// One thing a frontend's cache must forget, in the view's inode numbering.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Invalidation {
    /// A name in a directory (created, removed, renamed).
    Entry { parent: Ino, name: NameBuf },
    /// An inode's attributes only.
    Attr { ino: Ino },
    /// An inode's attributes and its cached pages: all of them
    /// (`range: None`), or `(offset, len)`.
    Data { ino: Ino, range: Option<(u64, u64)> },
    /// A name removed, and the inode it named (not produced yet).
    Deleted {
        parent: Ino,
        name: NameBuf,
        ino: Ino,
    },
    /// I/O on the inode is fenced (not produced yet).
    Fenced { ino: Ino },
    /// The view is closing (not produced yet).
    ViewClosing,
}

/// The frontend's side of the engine's events.
pub trait FrontendEvents: Send + Sync + 'static {
    /// Deliver `batch` to the frontend's cache. Called from one dedicated
    /// notifier thread, never while the engine holds a lock: a frontend's
    /// delivery mechanism (a `write(2)` on `/dev/fuse`, a WinFsp notify
    /// call, an Android `ContentResolver` call) may itself block on
    /// kernel state that an op in flight holds, so nothing that could be
    /// waiting on this call may ever be waited on by it. Failures (the
    /// usual one: nothing was cached) are the frontend's to ignore.
    fn invalidate(&self, batch: &[Invalidation]);
}
