//! [`PageCacheTable`]: what this connection's kernel holds in its page
//! cache, as far as the adapter can know it — so that an open may keep a
//! file's pages (`FOPEN_KEEP_CACHE`) only when they are known current
//! (plan 42 §5.1, W1).
//!
//! # Why every open dropped the pages, and what keeps them now
//!
//! Until plan 42 no open reply carried `FOPEN_KEEP_CACHE`, so the kernel
//! dropped a file's pages at every open (`fuse_open` →
//! `invalidate_inode_pages2`). That drop was what made close-to-open hold
//! for pages: whatever the kernel cached — a page another node changed
//! since, one whose notification was dropped, one a write-through write
//! left stale (race 1, `crate::repair`) — the next open threw it away.
//! An open now keeps the pages iff **all** hold when its reply is written:
//!
//! 1. the engine answered a cache stamp (`Opened::cache`: bounded mode,
//!    invalidation on, a regular file, no data notification for it on its
//!    way — `constellation_engine`'s `view::kernel_cache`);
//! 2. the last open that **dropped** the pages was answered the same stamp
//!    ([`PcEntry::dropped_at`]): every page present now entered the cache
//!    after that drop, filled by a read of the view or a write through
//!    this kernel, and a committed change since would have changed the
//!    stamp;
//! 3. nothing seen since that drop could have left a page that differs
//!    from what a read of the view answers ([`PcEntry::suspect`], the
//!    marks below);
//! 4. that drop is known to be complete ([`PcEntry::open_arms`] empty):
//!    the kernel drops the pages in the opener's context after it read the
//!    reply, at a time the daemon does not see, and another open of the
//!    same inode may be answered meanwhile — keeping, it would let its
//!    caller read the pages the first open is about to drop, which may be
//!    the very stale pages that made the first one drop (plan 42 F17,
//!    found implementing §5.1). The drop is known complete at the first
//!    request that carries the dropping open's file handle: the kernel
//!    installs the descriptor only after `fuse_finish_open`'s drop;
//! 5. the open is not a `create` carrying `O_TRUNC` (an `open(O_TRUNC)`
//!    reaches the daemon as a `SETATTR size=0` first: the adapter does not
//!    negotiate `FUSE_ATOMIC_O_TRUNC`, and the kernel follows that reply
//!    with its own drop), its answer is a plain one (a passthrough open's
//!    reads never reach the page cache's daemon side), and
//!    `CONSTELLATION_FUSE_KEEP_CACHE` is not `off`.
//!
//! Otherwise the reply carries no `FOPEN_KEEP_CACHE` and, *before* its
//! bytes are written, the entry records the drop: the stamp (or none),
//! `suspect` cleared, the open's handle among the drops not yet complete.
//! Every rule fails towards dropping; an inode with no entry is not
//! current.
//!
//! # Suspect marks
//!
//! Set (on an entry that exists: an absent one is not current anyway) by
//! anything after which a page may differ from what the view answers:
//!
//! - **S1** every data invalidation the notification sink writes — a
//!   foreign change, a lock grant, the handover resume's, the engine's
//!   uncommitted-session, other-view and rollback events: the kernel may
//!   have failed to drop a page (`invalidate_inode_pages2` returns `EBUSY`
//!   for one it could not, and the kernel ignores it), and a row that
//!   changed and changed back would repeat a stamp;
//! - **S2** every data invalidation **not** delivered: dropped by the
//!   engine past its bound (`FrontendEvents::undelivered`), refused by the
//!   closed handover gate, or a write that failed with anything but
//!   `ENOENT` — pages have no lifetime;
//! - **S3** a `WRITE` answered with an error or a short count: the folio
//!   keeps the bytes the daemon did not take;
//! - **S4** an open answered `FOPEN_DIRECT_IO` (a read-write passthrough
//!   open): its writes bypass the page cache other handles hold;
//! - **S5** the write-race bookkeeping could not rule out a stale page, or
//!   a repair was late or skipped (plan 42 K4); until that bookkeeping
//!   lands, every `WRITE`, when it arrives and when it is answered.
//!
//! A dropping open clears the mark; nothing else does.
//!
//! # Lifetime
//!
//! One table per session. An entry is created by an open reply (and by the
//! write-race bookkeeping); it is removed when the kernel gives back the
//! inode's last lookup (`Forgot::Last`) — it evicted the inode with its
//! pages — or more lookups than were counted (`Forgot::Underflow`: in
//! doubt, absence is the safe state). Inode numbers are never reused, so a
//! later lookup of the same number is a fresh kernel inode with no pages
//! and finds no entry. A resumed session starts with an empty table (the
//! table is not exported across a handover: the first open of each inode
//! after a resume drops once), as does a remount.

use crate::refs::Mix;
use constellation_vfs::{CacheStamp, Ino};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

/// Shards: every worker's opens, reads and writes consult the table.
const SHARDS: usize = 64;

/// Handle arms kept per entry; past that, one overflow arm that completes
/// when the inode's last handle is released.
const MAX_OPEN_ARMS: usize = 4;

/// `CONSTELLATION_FUSE_KEEP_CACHE` (plan 42 §5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepCache {
    /// §5.1's rules decide (bounded mode; the engine answers no stamp
    /// under `--cto strict`).
    Auto,
    /// Every open drops the pages, as before plan 42 (diagnosis).
    Off,
}

impl KeepCache {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Some(Self::Auto),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Off => "off",
        }
    }
}

/// Why an open dropped the pages (`dropping_opens` by reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The engine answered no stamp.
    NoStamp,
    /// The stamp is not the one the last drop recorded.
    StampChanged,
    /// A mark says a page may be stale.
    Suspect,
    /// Nothing is known of the inode's pages.
    NoEntry,
    /// A `create` with `O_TRUNC`.
    Trunc,
    /// `CONSTELLATION_FUSE_KEEP_CACHE=off`.
    Off,
    /// The last drop is not known to be complete yet.
    DropInFlight,
    /// A passthrough answer.
    Passthrough,
}

impl DropReason {
    pub const ALL: [DropReason; 8] = [
        Self::NoStamp,
        Self::StampChanged,
        Self::Suspect,
        Self::NoEntry,
        Self::Trunc,
        Self::Off,
        Self::DropInFlight,
        Self::Passthrough,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoStamp => "no_stamp",
            Self::StampChanged => "stamp_changed",
            Self::Suspect => "suspect",
            Self::NoEntry => "no_entry",
            Self::Trunc => "trunc",
            Self::Off => "off",
            Self::DropInFlight => "drop_in_flight",
            Self::Passthrough => "passthrough",
        }
    }
}

/// A suspect mark's source (`suspect_marks` by mark).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    /// A data invalidation written.
    S1,
    /// A data invalidation not delivered.
    S2,
    /// A write answered with an error or a short count.
    S3,
    /// A direct-I/O open.
    S4,
    /// The write-race bookkeeping could not rule a stale page out.
    S5,
}

impl Mark {
    pub const ALL: [Mark; 5] = [Self::S1, Self::S2, Self::S3, Self::S4, Self::S5];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::S1 => "s1",
            Self::S2 => "s2",
            Self::S3 => "s3",
            Self::S4 => "s4",
            Self::S5 => "s5",
        }
    }

    fn index(self) -> usize {
        self as usize
    }

    fn bit(self) -> u8 {
        1 << self.index()
    }
}

/// A drop of an inode's pages announced and not yet known complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arm {
    /// A1: an open answered without `FOPEN_KEEP_CACHE`, by its handle.
    Open(u64),
    /// A1 past [`MAX_OPEN_ARMS`]: complete when no handle is left.
    OpenOverflow,
}

/// What the table knows of one inode's pages.
#[derive(Debug, Default)]
pub(crate) struct PcEntry {
    /// The stamp of the last open that dropped the pages (`None`: it had
    /// none).
    pub(crate) dropped_at: Option<CacheStamp>,
    /// The marks since that drop (module doc), one bit per [`Mark`].
    pub(crate) marks: u8,
    /// Drops announced and not yet known complete.
    pub(crate) arms: Vec<Arm>,
    /// Open handles of this session (any access), and those with write
    /// access.
    pub(crate) handles: u32,
    pub(crate) write_handles: u32,
}

impl PcEntry {
    /// A mark since the last drop.
    #[cfg(test)]
    pub(crate) fn suspect(&self) -> bool {
        self.marks != 0
    }

    /// An open's drop is still to happen in the kernel (rule 4).
    fn open_arms(&self) -> bool {
        self.arms
            .iter()
            .any(|a| matches!(a, Arm::Open(_) | Arm::OpenOverflow))
    }
}

/// What an open was answered, for [`PageCacheTable::open_reply`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct OpenFacts {
    pub(crate) ino: Ino,
    pub(crate) fh: u64,
    pub(crate) stamp: Option<CacheStamp>,
    /// A plain (or zero-copy) answer, not a passthrough one.
    pub(crate) plain: bool,
    /// A passthrough answer with `FOPEN_DIRECT_IO`.
    pub(crate) direct_io: bool,
    /// A `create` that carried `O_TRUNC`.
    pub(crate) create_trunc: bool,
    pub(crate) write_access: bool,
}

/// The table's counters (`node.status`'s `fuse.mounts[].kernel_cache`).
#[derive(Default)]
pub struct PageCacheStats {
    pub(crate) keep_cache_opens: AtomicU64,
    pub(crate) dropping_opens: [AtomicU64; 8],
    pub(crate) suspect_marks: [AtomicU64; 5],
}

/// A snapshot of [`PageCacheStats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageCacheCounts {
    pub keep_cache_opens: u64,
    /// By [`DropReason::as_str`].
    pub dropping_opens: Vec<(&'static str, u64)>,
    /// By [`Mark::as_str`].
    pub suspect_marks: Vec<(&'static str, u64)>,
}

/// The settings the table works under (`CONSTELLATION_FUSE_KEEP_CACHE`,
/// and the fault-only knob of §7.2).
#[derive(Debug, Clone, Copy)]
pub struct PageCacheConfig {
    pub keep_cache: KeepCache,
    /// `CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT=1`: the decision
    /// ignores S5 — the naive keep-cache plan 42 F1 guards against, for a
    /// scenario's fail-before only.
    pub ignore_s5: bool,
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            keep_cache: KeepCache::Auto,
            ignore_s5: false,
        }
    }
}

type Shard = Mutex<HashMap<Ino, PcEntry, BuildHasherDefault<Mix>>>;

/// One session's [`PcEntry`]s (see the module doc).
pub struct PageCacheTable {
    shards: Box<[Shard]>,
    config: PageCacheConfig,
    stats: PageCacheStats,
}

impl std::fmt::Debug for PageCacheTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCacheTable")
            .field("config", &self.config)
            .field("counts", &self.counts())
            .finish()
    }
}

impl PageCacheTable {
    pub fn new(config: PageCacheConfig) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            config,
            stats: PageCacheStats::default(),
        })
    }

    pub fn config(&self) -> PageCacheConfig {
        self.config
    }

    fn shard(&self, ino: Ino) -> &Shard {
        &self.shards[(ino as usize) % SHARDS]
    }

    /// Decide an open's reply (module doc) and record it: `true` keeps the
    /// pages (`FOPEN_KEEP_CACHE`). Called before the reply is written.
    pub(crate) fn open_reply(&self, o: OpenFacts) -> bool {
        let mut shard = self.shard(o.ino).lock().unwrap();
        let entry = shard.get(&o.ino);
        let ignored = if self.config.ignore_s5 {
            Mark::S5.bit()
        } else {
            0
        };
        let verdict = if self.config.keep_cache == KeepCache::Off {
            Err(DropReason::Off)
        } else if !o.plain {
            Err(DropReason::Passthrough)
        } else if o.create_trunc {
            Err(DropReason::Trunc)
        } else {
            match (o.stamp, entry) {
                (None, _) => Err(DropReason::NoStamp),
                (_, None) => Err(DropReason::NoEntry),
                (Some(_), Some(e)) if e.marks & !ignored != 0 => Err(DropReason::Suspect),
                (Some(s), Some(e)) if e.dropped_at != Some(s) => Err(DropReason::StampChanged),
                (Some(_), Some(e)) if e.open_arms() => Err(DropReason::DropInFlight),
                (Some(_), Some(_)) => Ok(()),
            }
        };
        let e = shard.entry(o.ino).or_default();
        e.handles += 1;
        if o.write_access {
            e.write_handles += 1;
        }
        match verdict {
            Ok(()) => {
                self.stats.keep_cache_opens.fetch_add(1, Relaxed);
                true
            }
            Err(reason) => {
                self.stats.dropping_opens[reason as usize].fetch_add(1, Relaxed);
                e.dropped_at = o.stamp;
                e.marks = 0;
                let opens = e.arms.iter().filter(|a| matches!(a, Arm::Open(_))).count();
                if opens < MAX_OPEN_ARMS {
                    e.arms.push(Arm::Open(o.fh));
                } else if !e.arms.contains(&Arm::OpenOverflow) {
                    e.arms.push(Arm::OpenOverflow);
                }
                if o.direct_io {
                    e.marks |= Mark::S4.bit();
                    self.stats.suspect_marks[Mark::S4.index()].fetch_add(1, Relaxed);
                }
                false
            }
        }
    }

    /// Mark `ino`'s pages as possibly stale (module doc). An absent entry
    /// is not current already, and is not created.
    pub(crate) fn mark(&self, ino: Ino, mark: Mark) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            e.marks |= mark.bit();
        }
        drop(shard);
        self.stats.suspect_marks[mark.index()].fetch_add(1, Relaxed);
    }

    /// A request carrying file handle `fh` of `ino` reached the adapter:
    /// the open that returned `fh` has been installed by the kernel, so its
    /// drop (if it dropped) is complete (rule 4).
    pub(crate) fn handle_used(&self, ino: Ino, fh: u64) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            if let Some(i) = e.arms.iter().position(|a| *a == Arm::Open(fh)) {
                e.arms.swap_remove(i);
            }
        }
    }

    /// `RELEASE` of `fh`: one handle fewer, and its drop complete.
    pub(crate) fn released(&self, ino: Ino, fh: u64, write_access: bool) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            // Its arm still live: no request ever carried `fh`, not even
            // the FLUSH of a close. The kernel's open can fail after the
            // reply (`fuse_finish_open`'s `fuse_file_io_open`: a passthrough
            // backing-open error, an iomode clash) and then releases the
            // handle without the drop the reply asked for, so the drop is
            // as good as lost (S2): the open's reply cleared the marks.
            if let Some(i) = e.arms.iter().position(|a| *a == Arm::Open(fh)) {
                e.arms.swap_remove(i);
                e.marks |= Mark::S2.bit();
                self.stats.suspect_marks[Mark::S2.index()].fetch_add(1, Relaxed);
            }
            e.handles = e.handles.saturating_sub(1);
            if write_access {
                e.write_handles = e.write_handles.saturating_sub(1);
            }
            if e.handles == 0 {
                e.arms.retain(|a| *a != Arm::OpenOverflow);
            }
        }
    }

    /// The kernel evicted `ino` (its last lookup given back), or the count
    /// is in doubt: what is known of its pages goes.
    pub(crate) fn forget(&self, ino: Ino) {
        self.shard(ino).lock().unwrap().remove(&ino);
    }

    /// The counters.
    pub fn counts(&self) -> PageCacheCounts {
        PageCacheCounts {
            keep_cache_opens: self.stats.keep_cache_opens.load(Relaxed),
            dropping_opens: DropReason::ALL
                .iter()
                .map(|r| {
                    (
                        r.as_str(),
                        self.stats.dropping_opens[*r as usize].load(Relaxed),
                    )
                })
                .collect(),
            suspect_marks: Mark::ALL
                .iter()
                .map(|m| {
                    (
                        m.as_str(),
                        self.stats.suspect_marks[m.index()].load(Relaxed),
                    )
                })
                .collect(),
        }
    }

    /// Entries held (tests, and a bound on the table's memory).
    pub fn entries(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().len()).sum()
    }

    #[cfg(test)]
    pub(crate) fn with_entry<T>(&self, ino: Ino, f: impl FnOnce(Option<&PcEntry>) -> T) -> T {
        f(self.shard(ino).lock().unwrap().get(&ino))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Option<CacheStamp> = Some(CacheStamp(7));

    fn facts(fh: u64, stamp: Option<CacheStamp>) -> OpenFacts {
        OpenFacts {
            ino: 42,
            fh,
            stamp,
            plain: true,
            direct_io: false,
            create_trunc: false,
            write_access: false,
        }
    }

    /// A table whose inode 42 was dropped once at stamp `S`, that drop
    /// complete (its handle used): the next open with `S` keeps.
    fn current() -> std::sync::Arc<PageCacheTable> {
        let t = PageCacheTable::new(PageCacheConfig::default());
        assert!(!t.open_reply(facts(1, S)), "no entry: drop");
        t.handle_used(42, 1);
        t
    }

    #[test]
    fn an_open_keeps_only_after_a_complete_drop_at_the_same_stamp() {
        let t = current();
        assert!(t.open_reply(facts(2, S)), "same stamp, nothing marked");
        assert!(t.open_reply(facts(3, S)));
        assert_eq!(t.counts().keep_cache_opens, 2);
    }

    /// Each of rules 1–5 alone flips keep to drop (each checked by
    /// removing the rule: the open keeps).
    #[test]
    fn each_rule_alone_drops() {
        // 1: no stamp.
        let t = current();
        assert!(!t.open_reply(facts(2, None)));
        // ... and that drop recorded no stamp: the next open drops too.
        t.handle_used(42, 2);
        assert!(!t.open_reply(facts(3, S)), "dropped_at is None now");
        // 2: another stamp.
        let t = current();
        assert!(!t.open_reply(facts(2, Some(CacheStamp(8)))));
        // 3: a mark.
        for mark in Mark::ALL {
            let t = current();
            t.mark(42, mark);
            assert!(!t.open_reply(facts(2, S)), "{mark:?}");
        }
        // 4: the last drop not known complete.
        let t = PageCacheTable::new(PageCacheConfig::default());
        assert!(!t.open_reply(facts(1, S)));
        assert!(
            !t.open_reply(facts(2, S)),
            "the first open's drop may not have run"
        );
        t.handle_used(42, 1);
        assert!(!t.open_reply(facts(3, S)), "the second's neither");
        t.handle_used(42, 2);
        t.handle_used(42, 3);
        assert!(t.open_reply(facts(4, S)));
        // 5: a truncating create, a passthrough answer, the knob.
        let t = current();
        assert!(!t.open_reply(OpenFacts {
            create_trunc: true,
            ..facts(2, S)
        }));
        let t = current();
        assert!(!t.open_reply(OpenFacts {
            plain: false,
            ..facts(2, S)
        }));
        let t = PageCacheTable::new(PageCacheConfig {
            keep_cache: KeepCache::Off,
            ..Default::default()
        });
        assert!(!t.open_reply(facts(1, S)));
        t.handle_used(42, 1);
        assert!(!t.open_reply(facts(2, S)), "off");
    }

    /// A dropping open records its stamp and clears the mark; a keeping
    /// one changes nothing.
    #[test]
    fn a_dropping_open_records_and_clears() {
        let t = current();
        t.mark(42, Mark::S1);
        assert!(!t.open_reply(facts(2, S)));
        t.with_entry(42, |e| {
            let e = e.unwrap();
            assert!(!e.suspect());
            assert_eq!(e.dropped_at, S);
            assert_eq!(e.arms, vec![Arm::Open(2)]);
        });
        // A close: its FLUSH carries the handle, then the RELEASE.
        t.handle_used(42, 2);
        t.released(42, 2, false);
        assert!(t.open_reply(facts(3, S)), "current again");
        let drops: u64 = t.counts().dropping_opens.iter().map(|(_, n)| n).sum();
        assert_eq!(drops, 2);
    }

    /// A mark creates no entry; a direct-I/O open marks its own entry.
    #[test]
    fn marks_and_direct_io() {
        let t = PageCacheTable::new(PageCacheConfig::default());
        t.mark(42, Mark::S1);
        assert_eq!(t.entries(), 0);
        assert!(!t.open_reply(OpenFacts {
            plain: false,
            direct_io: true,
            ..facts(1, S)
        }));
        t.with_entry(42, |e| assert!(e.unwrap().suspect(), "S4"));
    }

    /// `Forgot::Last` removes the entry: the inode is not current.
    #[test]
    fn a_forgotten_inode_is_not_current() {
        let t = current();
        t.forget(42);
        assert_eq!(t.entries(), 0);
        assert!(!t.open_reply(facts(2, S)), "no entry");
    }

    /// Past four handle arms one overflow arm completes with the last
    /// handle.
    #[test]
    fn handle_arms_overflow_until_the_last_release() {
        let t = PageCacheTable::new(PageCacheConfig::default());
        for fh in 1..=6 {
            assert!(!t.open_reply(facts(fh, S)));
        }
        t.with_entry(42, |e| {
            let e = e.unwrap();
            assert_eq!(e.arms.len(), 5);
            assert!(e.arms.contains(&Arm::OpenOverflow));
        });
        for fh in 1..=5 {
            t.handle_used(42, fh);
            t.released(42, fh, false);
        }
        assert!(!t.open_reply(facts(7, S)), "the overflow arm lives");
        t.released(42, 6, false);
        t.handle_used(42, 7);
        t.released(42, 7, false);
        assert!(t.open_reply(facts(8, S)));
    }

    /// The fault knob ignores an S5 mark and only that.
    #[test]
    fn the_naive_knob_ignores_s5_only() {
        let t = PageCacheTable::new(PageCacheConfig {
            ignore_s5: true,
            ..Default::default()
        });
        assert!(!t.open_reply(facts(1, S)));
        t.handle_used(42, 1);
        t.mark(42, Mark::S5);
        assert!(t.open_reply(facts(2, S)), "S5 ignored");
        t.mark(42, Mark::S1);
        assert!(!t.open_reply(facts(3, S)), "S1 is not");
    }

    #[test]
    fn keep_cache_parses_its_two_values() {
        assert_eq!(KeepCache::parse("auto"), Some(KeepCache::Auto));
        assert_eq!(KeepCache::parse(" OFF "), Some(KeepCache::Off));
        assert_eq!(KeepCache::parse(""), Some(KeepCache::Auto));
        assert_eq!(KeepCache::parse("on"), None);
    }
}
