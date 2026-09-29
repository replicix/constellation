//! [`FuseNotifySink`]: the engine's cache invalidations, as
//! `FUSE_NOTIFY_INVAL_*` writes on `/dev/fuse`.
//!
//! A 1:1 rename of the adapter's former `NotifySink` impl on
//! `fuser::Notifier` (plan 31 §6.5). The delivery rules — one dedicated
//! thread, never while an engine lock is held, held back while an op on
//! the inode is in flight, dropped past the TTL — are the engine's
//! (`constellation_engine::kernel_inval`, whose module doc tells the
//! zombie-daemon incident behind them); this only performs the writes.

use constellation_vfs::{FrontendEvents, Invalidation};
use fuser::{INodeNo, Notifier};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

/// One mount's kernel cache, as the engine's invalidations reach it.
pub struct FuseNotifySink(pub Notifier);

impl FrontendEvents for FuseNotifySink {
    fn invalidate(&self, batch: &[Invalidation]) {
        for inv in batch {
            // ENOENT (nothing cached) is the common answer; every error
            // only means there was nothing to drop.
            let _ = match inv {
                Invalidation::Entry { parent, name } => self
                    .0
                    .inval_entry(INodeNo(*parent), OsStr::from_bytes(name.as_bytes())),
                // Offset -1: the attributes only.
                Invalidation::Attr { ino } => self.0.inval_inode(INodeNo(*ino), -1, 0),
                // Offset 0, length 0: the attributes and every page.
                Invalidation::Data { ino, range: None } | Invalidation::Fenced { ino } => {
                    self.0.inval_inode(INodeNo(*ino), 0, 0)
                }
                Invalidation::Data {
                    ino,
                    range: Some((offset, len)),
                } => self
                    .0
                    .inval_inode(INodeNo(*ino), *offset as i64, *len as i64),
                Invalidation::Deleted { parent, name, ino } => self.0.delete(
                    INodeNo(*parent),
                    INodeNo(*ino),
                    OsStr::from_bytes(name.as_bytes()),
                ),
                _ => Ok(()),
            };
        }
    }
}
