//! Plan 42 §5.1: what the view tells a frontend whose kernel keeps a
//! file's pages across opens (Linux FUSE with `FOPEN_KEEP_CACHE`).
//!
//! Two things, both fail-safe towards "drop":
//!
//! - **The cache stamp** an open answers ([`View::open_stamp`],
//!   `Opened::cache`): the content identity of the inode's committed row,
//!   read in the same record read the open makes anyway
//!   (`Meta::getattr_stamped`). The frontend may keep the pages it cached
//!   since an open that dropped them was answered the same stamp; no stamp
//!   (`None`) means drop, and is what an open answers under `--cto strict`
//!   (an open there is the close-to-open point, and its drop is what makes
//!   the pages follow the replica, independent of how and when the
//!   notifications reach the kernel), with kernel invalidation off (the
//!   kernel then never hears of another node's change), for anything but a
//!   regular file, and while a data notification for the inode is still
//!   on its way (`InFlight::data_pending`: queued, held back for a
//!   request in flight, or being written — the change it reports is
//!   committed, but the kernel does not know yet).
//! - **The events that change content under a kernel without changing the
//!   stamp or reaching that kernel** (§5.1.5), each pushed as a data
//!   invalidation through the node's invalidator, which the frontend
//!   delivers (and then stops trusting the file's pages) or reports
//!   undelivered (and stops trusting them too):
//!   - a write session that ends without becoming the row
//!     ([`Retired::Uncommitted`], and `drop_writes`): the kernel's pages
//!     hold its bytes, the view will never answer them, and the row — so
//!     the stamp — is what it was;
//!   - a commit through this view ([`Retired::Committed`]): every *other*
//!     view of the node is told (plan 42 Q3), since their kernels may hold
//!     pages of the row it replaced and no foreign apply will report it;
//!     this view's own kernel holds what it wrote;
//!   - a stranding rollback (`Meta::set_rollback_hook`, wired by the node).

use super::*;
use constellation_vfs::CacheStamp;

/// How a write session left the view ([`View::retire_writes`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Retired {
    /// Its manifest was committed: the row is what it wrote.
    Committed,
    /// It wrote nothing (`WriteState::changed_nothing`).
    NothingChanged,
    /// It is gone without a commit: the kernel's pages hold bytes the view
    /// will never answer.
    Uncommitted,
}

impl View {
    /// Retire `ino`'s detached write session, telling the kernels what
    /// `why` changed under them (module doc). Every retirement goes
    /// through here, so a new path must say which it is.
    pub(super) fn retire_writes(&self, ino: Ino, why: Retired) {
        self.writes.retire(ino);
        match why {
            Retired::Committed => self.inflight.invalidate_others(self.id, ino),
            Retired::NothingChanged => {}
            Retired::Uncommitted => self.inflight.invalidate_uncommitted(ino),
        }
    }

    /// The cache stamp an open of `ino` (replica numbering) answers, given
    /// the stamp of the record it read (`None` for anything but a regular
    /// file, or a node-local scratch file). Checked after the record read:
    /// a change committed before it shows in `stamp`, one whose
    /// notification is still on its way shows as pending.
    pub(super) fn open_stamp(&self, ino: Ino, stamp: Option<u128>) -> Option<CacheStamp> {
        if self.sync.as_ref().is_some_and(|h| h.cto_strict) || !self.inflight.enabled() {
            return None;
        }
        let stamp = stamp?;
        if self.inflight.data_pending(ino) {
            return None;
        }
        Some(CacheStamp(stamp))
    }

    /// The stamp of a frozen file (a snapshot's): its content never
    /// changes and nothing writes it, so its synthetic node id names one
    /// content for the view's life — a constant per node id.
    pub(super) fn frozen_stamp(&self, ino: Ino) -> Option<CacheStamp> {
        if self.sync.as_ref().is_some_and(|h| h.cto_strict) {
            return None;
        }
        let digest = blake3::hash(&[b"frozen".as_slice(), &ino.to_le_bytes()].concat());
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest.as_bytes()[..16]);
        Some(CacheStamp(u128::from_le_bytes(out)))
    }
}
