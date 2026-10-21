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
//!    installs the descriptor only after `fuse_finish_open`'s drop. (Its
//!    `RELEASE` completes it too. The one exception: when
//!    `fuse_finish_open` fails, `fuse_open` releases the handle and skips
//!    the drop, so the entry records a drop that did not run — reachable
//!    only through an io-mode conflict, which the passthrough answers
//!    never create: `PassthroughState::on_open_reply`);
//! 5. the open is not a `create` carrying `O_TRUNC` (an `open(O_TRUNC)`
//!    reaches the daemon as an `OPEN` and then a `SETATTR size=0` carrying
//!    the new handle — the adapter does not negotiate
//!    `FUSE_ATOMIC_O_TRUNC` — after the open's own drop, and the kernel
//!    truncates the pages itself), its answer is a plain one (a passthrough open's
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
//!   a repair was late or skipped (`crate::repair`).
//!
//! A dropping open clears the mark; nothing else does.
//!
//! # Write-race bookkeeping (W2, plan 42 §5.2)
//!
//! Race 1: a write-through `write(2)` copies into an uptodate folio and
//! unlocks it before its `FUSE_WRITE` reaches the daemon; a drop of the
//! file's pages in that window lets a read fetch the page from the daemon
//! before the write is applied, and the kernel then serves the pre-write
//! bytes for an acknowledged write. The table follows every drop the
//! adapter causes or can see coming and every read made while one may
//! have happened, so that the write in the window can be identified — by
//! the kernel's own serialization, not a clock — and the pages those reads
//! fetched invalidated before that write is answered (`crate::repair`).
//!
//! - **Arming.** An inode is *armed* while a drop of its pages has been
//!   announced and no write has been claimed since that drop was known to
//!   be complete: `!arms.is_empty() || drop_done`. Each drop adds an
//!   [`Arm`] *before* the reply or notification that causes it: A1 an
//!   open answered without `FOPEN_KEEP_CACHE` (complete at the first
//!   request carrying its handle: the kernel installs the descriptor
//!   only after `fuse_finish_open`'s drop); A2 a data notification
//!   (complete when the sink's write returns: the kernel drops inside
//!   it); A3 an attribute-bearing reply for a regular file whose size
//!   differs from the size last handed to the kernel (complete at once:
//!   nothing tells the daemon when the requester installs the reply; for
//!   a truncating `SETATTR` the kernel drops under `inode_lock`, so no
//!   write-through write can be in flight across it); A4 a repair.
//! - **Recording.** A `WRITE` without `FUSE_WRITE_CACHE` (a write-through
//!   one; page writeback keeps its folios under writeback for the round
//!   trip, so nothing drops them) is registered as pending. A `READ` of an
//!   inode that is armed, has a write pending or a handle with write
//!   access records its page range, `Pending` (a write was pending),
//!   `Armed`, or `Plain`, before the engine reads the data, under the
//!   shard lock the claim takes: a read is either recorded before the
//!   claim or reads after the claimed write was applied.
//! - **Claim.** When a registered write is answered (after the engine
//!   applied it), the reads recorded on its pages are its *hits*:
//!   `Pending`/`Armed` ones are repaired before the reply, `Plain` ones
//!   (a read-modify-write of a cold page, or a drop the daemon cannot see)
//!   mark the inode S5, the records are cleared and so is `drop_done`.
//!
//! Three facts a later reader would otherwise "fix":
//!
//! - **A claim clears every record, also of pages it does not cover.**
//!   Any later write of such a page is copied after this claim's reply
//!   (one write-through write per inode at a time: `fuse_cache_write_iter`
//!   holds `inode_lock` from the copy to the reply), and a read in flight
//!   on a page blocks a writer's `__filemap_get_folio(FGP_WRITEBEGIN)` on
//!   it, so a recorded read's fetch always precedes any later copy into
//!   the same page.
//! - **A multi-request `write(2)`** (larger than `max_write`) is several
//!   successive candidates, each filled after the previous one's reply
//!   and claimed in turn: one is pending at a time.
//! - **A request the kernel aborts** (a fatal signal) after a worker read
//!   it is still answered by the daemon (the kernel waits for its answer
//!   once the request left its pending list), so a claim always comes; a
//!   connection abort ends the session and its table.
//!
//! What the bookkeeping cannot see — a drop by reclaim, `fadvise`,
//! `drop_caches` — leaves at most a `Plain` hit, so S5: the next open
//! drops, today's bound. (Measured on Linux 7.3: those paths do not drop
//! a folio the writer holds a reference on — `mapping_evict_folio` skips
//! one with an elevated count — and the writer holds its request's
//! folios until the reply, so they did not reproduce race 1; the
//! invalidations of `invalidate_inode_pages2` do.)
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
    /// A1: an open answered without `FOPEN_KEEP_CACHE`, by its handle,
    /// and the read generation it was announced at.
    Open { fh: u64, gen: u64 },
    /// A1 past [`MAX_OPEN_ARMS`]: complete when no handle is left.
    OpenOverflow,
    /// A2: a data notification being written (its sequence number), and,
    /// for one of the whole file, the read generation it was announced at.
    Notify { seq: u64, gen: Option<u64> },
    /// A4: a repair being written.
    Repair(u64),
}

impl Arm {
    /// The read generation a whole-file drop was announced at: once it is
    /// complete, every read recorded before it fetched a folio that drop
    /// removed (`invalidate_inode_pages2` waits for a folio a read is
    /// filling, and drops it), so those records rule nothing in any more.
    fn supersedes(&self) -> Option<u64> {
        match self {
            Arm::Open { gen, .. } => Some(*gen),
            Arm::Notify { gen, .. } => *gen,
            Arm::OpenOverflow | Arm::Repair(_) => None,
        }
    }
}

/// What a recorded read may have fetched (the stronger wins a merge).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReadKind {
    /// No drop known, no write pending: a read-modify-write, or a drop
    /// the daemon cannot see.
    Plain,
    /// After a known drop.
    Armed,
    /// While a write was pending.
    Pending,
}

/// The pages of reads recorded since the last claim: at most
/// [`ReadSet::MAX`] disjoint ranges of page indices `[start, end)`, each
/// with its kind and the read generation it was recorded at.
/// Overlapping or adjacent ranges merge (the stronger kind and the later
/// generation win); past the bound the two closest ranges merge — a wider
/// range repairs more, never less, and a later generation keeps a record
/// longer, never shorter.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ReadSet(Vec<(u64, u64, ReadKind, u64)>);

impl ReadSet {
    const MAX: usize = 16;

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    pub(crate) fn add(&mut self, start: u64, end: u64, kind: ReadKind, gen: u64) {
        if start >= end {
            return;
        }
        let (mut start, mut end, mut kind, mut gen) = (start, end, kind, gen);
        // Absorb every range that touches the new one.
        self.0.retain(|&(s, e, k, g)| {
            if s <= end && start <= e {
                start = start.min(s);
                end = end.max(e);
                kind = kind.max(k);
                gen = gen.max(g);
                false
            } else {
                true
            }
        });
        let at = self.0.partition_point(|&(s, _, _, _)| s < start);
        self.0.insert(at, (start, end, kind, gen));
        while self.0.len() > Self::MAX {
            // The two closest neighbours.
            let i = (0..self.0.len() - 1)
                .min_by_key(|&i| self.0[i + 1].0 - self.0[i].1)
                .expect("at least two ranges");
            let (s, _, k1, g1) = self.0[i];
            let (_, e, k2, g2) = self.0.remove(i + 1);
            self.0[i] = (s, e, k1.max(k2), g1.max(g2));
        }
    }

    /// Forget the records made before read generation `gen` (a whole-file
    /// drop announced at `gen` has completed: [`Arm::supersedes`]).
    pub(crate) fn supersede(&mut self, gen: u64) {
        self.0.retain(|&(_, _, _, g)| g >= gen);
    }

    /// The parts of the recorded ranges inside `[start, end)`.
    pub(crate) fn hits(&self, start: u64, end: u64) -> Vec<(u64, u64, ReadKind)> {
        self.0
            .iter()
            .filter(|&&(s, e, _, _)| s < end && start < e)
            .map(|&(s, e, k, _)| (s.max(start), e.min(end), k))
            .collect()
    }
}

/// A registered write in progress: `(unique, first page, end page)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingWrite {
    pub(crate) unique: u64,
}

/// What a claim asks of the write's reply ([`PageCacheTable::claim`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Claim {
    /// Answer it now.
    Reply,
    /// Invalidate these page ranges first (`crate::repair`), and then
    /// `tail`, the write's last page when the write does not cover it
    /// whole: that page may be the writer's own folio, which a write-through
    /// writer keeps locked until its reply when the copy left it not
    /// uptodate — then the repair waits for the reply it holds, and the
    /// page is not stale (a read can neither fill a folio the writer has
    /// locked nor be sent while it is) — `crate::repair` answers the write
    /// early on that one page.
    Repair {
        ranges: Vec<(u64, u64)>,
        tail: Option<u64>,
    },
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
    /// W2: a drop completed and no write has been claimed since.
    pub(crate) drop_done: bool,
    /// W2: registered (write-through) writes in progress.
    pub(crate) pending: Vec<PendingWrite>,
    /// W2: the pages reads fetched since the last claim.
    pub(crate) reads: ReadSet,
    /// W2: the size last handed to the kernel (A3).
    pub(crate) last_size: Option<u64>,
    /// W2: bumped at every whole-file drop announced (A1, A2): what a
    /// read is recorded at, and what a completed drop supersedes.
    pub(crate) read_gen: u64,
    /// W2: requests on the inode in progress whose answer a repair may be
    /// waiting for (`READ`s, `WRITE`s; a write handed to the repair lane
    /// no longer counts): the repair's clock runs only while there are
    /// none (`crate::repair`).
    pub(crate) ops_in_flight: u32,
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
            .any(|a| matches!(a, Arm::Open { .. } | Arm::OpenOverflow))
    }

    /// Nothing an entry created by a drop alone would lose: no handle, no
    /// arm, no request, no record, no stamp and no size handed to the
    /// kernel (an entry with `last_size` leaves at FORGET). An open of the
    /// inode drops with or without it (`dropped_at` is `None`).
    fn idle(&self) -> bool {
        self.handles == 0
            && self.write_handles == 0
            && self.arms.is_empty()
            && self.pending.is_empty()
            && self.reads.0.is_empty()
            && self.ops_in_flight == 0
            && self.dropped_at.is_none()
            && self.marks == 0
            && self.last_size.is_none()
    }

    /// A whole-file drop is announced: the generation it supersedes once
    /// complete.
    fn announce(&mut self) -> u64 {
        self.read_gen += 1;
        self.read_gen
    }

    /// W2: a drop announced, or completed with no claim since.
    fn armed(&self) -> bool {
        !self.arms.is_empty() || self.drop_done
    }

    /// Complete the first live arm `is` matches: the kernel has finished
    /// that drop. A whole-file drop also supersedes the reads recorded
    /// before it was announced.
    fn complete(&mut self, is: impl Fn(&Arm) -> bool) -> bool {
        match self.arms.iter().position(is) {
            Some(i) => {
                let arm = self.arms.swap_remove(i);
                self.drop_done = true;
                if let Some(gen) = arm.supersedes() {
                    self.reads.supersede(gen);
                }
                true
            }
            None => false,
        }
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
    /// W2: writes whose reply waited for a repair, and the pages they
    /// invalidated.
    pub(crate) repairs: AtomicU64,
    pub(crate) repair_pages: AtomicU64,
    /// W2: repairs that came too late (the reply had been answered at the
    /// bound) or were skipped (the gate was closed).
    pub(crate) repairs_late: AtomicU64,
    /// W2: the time repaired writes' replies waited, in all, and the
    /// longest.
    pub(crate) repair_wait_ms: AtomicU64,
    pub(crate) repair_wait_max_ms: AtomicU64,
    /// W2: repairs whose write was answered early, blocked on its own
    /// last page (`crate::repair`'s `TAIL_GRACE`).
    pub(crate) repairs_released: AtomicU64,
    /// W2: claims with a `Plain` hit (S5 instead of a repair).
    pub(crate) ambiguous_writes: AtomicU64,
    /// W2: claims while another registered write was pending (not
    /// expected: one write-through write per inode at a time).
    pub(crate) concurrent_writes: AtomicU64,
    /// W3b: attribute-only notifications for changed directories and moved
    /// inodes (`crate::reply`).
    pub(crate) dir_attr_notices: AtomicU64,
    /// W3b, narrowed: changed inodes no reply was on its way for, so
    /// without a notice.
    pub(crate) dir_attr_notices_skipped: AtomicU64,
}

/// A snapshot of [`PageCacheStats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageCacheCounts {
    pub keep_cache_opens: u64,
    /// By [`DropReason::as_str`].
    pub dropping_opens: Vec<(&'static str, u64)>,
    /// By [`Mark::as_str`].
    pub suspect_marks: Vec<(&'static str, u64)>,
    pub repairs: u64,
    pub repair_pages: u64,
    pub repairs_late: u64,
    pub repairs_released: u64,
    pub repair_wait_ms: u64,
    pub repair_wait_max_ms: u64,
    pub ambiguous_writes: u64,
    pub concurrent_writes: u64,
    pub dir_attr_notices: u64,
    pub dir_attr_notices_skipped: u64,
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
    /// W2's repair (`CONSTELLATION_FUSE_WRITE_REPAIR`): `auto` follows
    /// the kernel's verdict ([`crate::kernel_fixes`]); off, every repair
    /// hit is an S5 instead.
    pub write_repair: crate::Switch,
    /// W3b (`CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY`), likewise.
    pub dir_change_notify: crate::Switch,
    /// W3a (`CONSTELLATION_FUSE_DIR_ATTR_TTL`), and whether the view
    /// serves `--cto strict` (what `auto` follows).
    pub dir_attr_ttl: crate::DirAttrTtl,
    pub strict: bool,
}

impl PageCacheConfig {
    /// Whether W2 repairs now.
    pub fn repair_on(&self) -> bool {
        on(
            self.write_repair,
            crate::kernel_fixes::KernelFix::WriteThroughWriteback,
        )
    }

    /// Whether W3b writes notices now, and for which namespace operations
    /// ([`NoticeMode`]).
    pub fn dir_notice_mode(&self) -> NoticeMode {
        match self.dir_change_notify {
            crate::Switch::On => NoticeMode::Always,
            crate::Switch::Off => NoticeMode::Off,
            crate::Switch::Auto => {
                if crate::kernel_fixes::fixed(crate::kernel_fixes::KernelFix::DirChangeBumpsVersion)
                {
                    NoticeMode::Off
                } else if self.strict {
                    NoticeMode::Always
                } else {
                    NoticeMode::Narrowed
                }
            }
        }
    }

    /// Whether W3b writes its notices now.
    pub fn dir_notice_on(&self) -> bool {
        self.dir_notice_mode() != NoticeMode::Off
    }

    /// Whether W3a gives directories' attributes no kernel lifetime now:
    /// `auto` under strict, unless the kernel carries the fix; in bounded
    /// mode `auto` is `entry` (plan 42 Q1, `crate::DirAttrTtl::Auto`).
    pub fn dir_attr_zero(&self) -> bool {
        match self.dir_attr_ttl {
            crate::DirAttrTtl::Zero => true,
            crate::DirAttrTtl::Entry => false,
            crate::DirAttrTtl::Auto => {
                self.strict
                    && !crate::kernel_fixes::fixed(
                        crate::kernel_fixes::KernelFix::AttrsValidAfterStore,
                    )
            }
        }
    }

    /// What `node.status` reports of the workarounds (plan 42 §5.4).
    pub fn status(&self) -> KernelWorkaroundsStatus {
        use crate::kernel_fixes::{verdict, KernelFix, Source};
        let overridden = |fix: KernelFix| match fix {
            KernelFix::WriteThroughWriteback => self.write_repair != crate::Switch::Auto,
            KernelFix::DirChangeBumpsVersion => self.dir_change_notify != crate::Switch::Auto,
            KernelFix::AttrsValidAfterStore => self.dir_attr_ttl != crate::DirAttrTtl::Auto,
        };
        KernelWorkaroundsStatus {
            keep_cache: self.keep_cache.as_str(),
            write_repair: self.write_repair.as_str(),
            write_repair_on: self.repair_on(),
            dir_attr_ttl: self.dir_attr_ttl.as_str(),
            dir_attr_ttl_zero: self.dir_attr_zero(),
            dir_change_notify: self.dir_change_notify.as_str(),
            dir_change_notify_on: self.dir_notice_on(),
            verdicts: KernelFix::ALL
                .iter()
                .map(|&fix| {
                    let (v, source) = verdict(fix);
                    let source = if overridden(fix) {
                        Source::Override
                    } else {
                        source
                    };
                    (fix.as_str(), v.as_str(), source.as_str())
                })
                .collect(),
        }
    }
}

/// A snapshot of a session's workaround settings and the kernel's verdicts
/// ([`PageCacheConfig::status`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KernelWorkaroundsStatus {
    pub keep_cache: &'static str,
    pub write_repair: &'static str,
    pub write_repair_on: bool,
    pub dir_attr_ttl: &'static str,
    pub dir_attr_ttl_zero: bool,
    pub dir_change_notify: &'static str,
    pub dir_change_notify_on: bool,
    /// `(fix, verdict, source)`.
    pub verdicts: Vec<(&'static str, &'static str, &'static str)>,
}

/// When W3b writes its notice for a namespace operation (plan 42 §5.3.1,
/// F21).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeMode {
    /// Never.
    Off,
    /// For every operation: `on`, and `auto` under `--cto strict`, where a
    /// directory's attributes have no kernel lifetime anyway (W3a), so the
    /// notice costs no request.
    Always,
    /// `auto` in bounded mode: only for an inode an attribute reply may
    /// still be on its way to the kernel for (`crate::attrwatch`). The
    /// notice invalidates the inode's whole attribute mask, so each one
    /// costs the next path walk through a directory a `GETATTR`.
    Narrowed,
}

/// A workaround's switch against its fix's verdict: `auto` is on unless
/// the kernel is known to carry the fix.
fn on(switch: crate::Switch, fix: crate::kernel_fixes::KernelFix) -> bool {
    match switch {
        crate::Switch::On => true,
        crate::Switch::Off => false,
        crate::Switch::Auto => !crate::kernel_fixes::fixed(fix),
    }
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            keep_cache: KeepCache::Auto,
            ignore_s5: false,
            write_repair: crate::Switch::Auto,
            dir_change_notify: crate::Switch::Auto,
            dir_attr_ttl: crate::DirAttrTtl::Auto,
            strict: false,
        }
    }
}

type Shard = Mutex<HashMap<Ino, PcEntry, BuildHasherDefault<Mix>>>;

/// One session's [`PcEntry`]s (see the module doc).
pub struct PageCacheTable {
    shards: Box<[Shard]>,
    config: std::sync::RwLock<PageCacheConfig>,
    stats: PageCacheStats,
    /// `log2` of the kernel's page size.
    page_shift: u32,
    /// Sequence numbers of A2/A4 arms.
    next_arm: AtomicU64,
    /// Plan 42 F21: attribute replies in flight, for W3b's narrowed notice.
    watch: crate::attrwatch::AttrWatch,
}

impl std::fmt::Debug for PageCacheTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCacheTable")
            .field("config", &self.config())
            .field("counts", &self.counts())
            .finish()
    }
}

impl PageCacheTable {
    pub fn new(config: PageCacheConfig) -> std::sync::Arc<Self> {
        Self::with_page_size(config, crate::workarounds::page_size())
    }

    /// [`Self::new`] for a kernel page of `page` bytes (a power of two).
    pub(crate) fn with_page_size(config: PageCacheConfig, page: u64) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            config: std::sync::RwLock::new(config),
            stats: PageCacheStats::default(),
            page_shift: page.trailing_zeros(),
            next_arm: AtomicU64::new(1),
            watch: Default::default(),
        })
    }

    pub fn config(&self) -> PageCacheConfig {
        *self.config.read().unwrap()
    }

    /// The kernel's page size.
    pub(crate) fn page_size(&self) -> u64 {
        1 << self.page_shift
    }

    /// The pages `[off, off + len)` covers.
    fn pages(&self, off: u64, len: u64) -> (u64, u64) {
        let p = self.page_size();
        (off >> self.page_shift, off.saturating_add(len).div_ceil(p))
    }

    fn shard(&self, ino: Ino) -> &Shard {
        &self.shards[(ino as usize) % SHARDS]
    }

    /// Decide an open's reply (module doc) and record it: `true` keeps the
    /// pages (`FOPEN_KEEP_CACHE`). Called before the reply is written.
    pub(crate) fn open_reply(&self, o: OpenFacts) -> bool {
        let mut shard = self.shard(o.ino).lock().unwrap();
        let entry = shard.get(&o.ino);
        let config = self.config();
        let ignored = if config.ignore_s5 { Mark::S5.bit() } else { 0 };
        let verdict = if config.keep_cache == KeepCache::Off {
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
                let opens = e
                    .arms
                    .iter()
                    .filter(|a| matches!(a, Arm::Open { .. }))
                    .count();
                if opens < MAX_OPEN_ARMS {
                    let gen = e.announce();
                    e.arms.push(Arm::Open { fh: o.fh, gen });
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
            e.complete(|a| matches!(a, Arm::Open { fh: f, .. } if *f == fh));
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
            if e.complete(|a| matches!(a, Arm::Open { fh: f, .. } if *f == fh)) {
                e.marks |= Mark::S2.bit();
                self.stats.suspect_marks[Mark::S2.index()].fetch_add(1, Relaxed);
            }
            e.handles = e.handles.saturating_sub(1);
            if write_access {
                e.write_handles = e.write_handles.saturating_sub(1);
            }
            if e.handles == 0 {
                e.complete(|a| *a == Arm::OpenOverflow);
            }
        }
    }

    /// The kernel evicted `ino` (its last lookup given back), or the count
    /// is in doubt: what is known of its pages goes.
    pub(crate) fn forget(&self, ino: Ino) {
        self.shard(ino).lock().unwrap().remove(&ino);
    }

    /// A resumed session's handles (plan 42 §5.1.2): `ino` has `handles`
    /// open, `writers` of them with write access, from before the
    /// handover. Seeded before the session reads its first request.
    pub(crate) fn seed_handles(&self, ino: Ino, handles: u32, writers: u32) {
        let mut shard = self.shard(ino).lock().unwrap();
        let e = shard.entry(ino).or_default();
        e.handles += handles;
        e.write_handles += writers;
    }

    /// A2/A4: a drop of `ino`'s pages is about to be written (a data
    /// notification — `whole`: of every page —, a repair): the inode is
    /// armed until it returns ([`Self::drop_written`]). Creates the entry.
    pub(crate) fn arm_drop(&self, ino: Ino, repair: bool, whole: bool) -> u64 {
        let seq = self.next_arm.fetch_add(1, Relaxed);
        let mut shard = self.shard(ino).lock().unwrap();
        // Created when absent, so that an open answered while this drop is
        // still to be written inherits its arm; removed again when the drop
        // is written if nothing else came of it (`drop_written`), since the
        // engine names every inode a log record touched, most never looked
        // up by this kernel, and only a FORGET would remove it otherwise.
        let e = shard.entry(ino).or_default();
        let arm = if repair {
            Arm::Repair(seq)
        } else {
            let gen = whole.then(|| e.announce());
            Arm::Notify { seq, gen }
        };
        e.arms.push(arm);
        seq
    }

    /// The drop armed as `seq` has been written: the kernel has finished
    /// it.
    pub(crate) fn drop_written(&self, ino: Ino, seq: u64) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            e.complete(|a| {
                matches!(a, Arm::Notify { seq: s, .. } if *s == seq) || *a == Arm::Repair(seq)
            });
            if e.idle() {
                shard.remove(&ino);
            }
        }
    }

    /// A4 written while its write's reply was still held: no write of the
    /// inode can have been in the window since (the held write is the
    /// only one between copy and reply, and the engine has applied it), so
    /// the drop arms nothing past it. A repair that comes late — after the
    /// watchdog answered the write — is completed with
    /// [`Self::drop_written`] instead: the next write may have overlapped
    /// it.
    pub(crate) fn repair_written_in_time(&self, ino: Ino, seq: u64) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            if let Some(i) = e.arms.iter().position(|a| *a == Arm::Repair(seq)) {
                e.arms.swap_remove(i);
            }
        }
    }

    /// A3: an attribute-bearing reply hands the kernel `size` for regular
    /// file `ino`; a size other than the last one handed makes the
    /// requester drop the file's pages when it installs the reply
    /// (`fuse_change_attributes_i`), at a time the daemon does not see:
    /// the drop is taken as complete at once (module doc). An inode
    /// without an entry has no pages this session knows of.
    pub(crate) fn size_seen(&self, ino: Ino, size: u64) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            if e.last_size != Some(size) {
                e.last_size = Some(size);
                e.drop_done = true;
            }
        }
    }

    /// A `READ` of `[off, off + len)` reached the adapter: recorded if the
    /// inode is armed, has a write pending or a handle that can write, and
    /// counted in flight while the entry exists (`true`: call
    /// [`Self::op_done`] when it is answered).
    pub(crate) fn read_begin(&self, ino: Ino, off: u64, len: u64) -> bool {
        let mut shard = self.shard(ino).lock().unwrap();
        let Some(e) = shard.get_mut(&ino) else {
            return false;
        };
        e.ops_in_flight += 1;
        let kind = if !e.pending.is_empty() {
            Some(ReadKind::Pending)
        } else if e.armed() {
            Some(ReadKind::Armed)
        } else if e.write_handles > 0 {
            Some(ReadKind::Plain)
        } else {
            None
        };
        if let Some(kind) = kind {
            let (start, end) = self.pages(off, len);
            e.reads.add(start, end, kind, e.read_gen);
        }
        true
    }

    /// A request counted in flight was answered (or handed to the repair
    /// lane, for a write).
    pub(crate) fn op_done(&self, ino: Ino) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            e.ops_in_flight = e.ops_in_flight.saturating_sub(1);
        }
    }

    /// Requests on `ino` in progress whose answer a repair may wait for.
    pub(crate) fn ops_in_flight(&self, ino: Ino) -> u32 {
        self.shard(ino)
            .lock()
            .unwrap()
            .get(&ino)
            .map_or(0, |e| e.ops_in_flight)
    }

    /// A `WRITE` reached the adapter, counted in flight; a write-through
    /// one (`!writeback`) is registered as the inode's candidate (module
    /// doc). Creates the entry for a write-through one; `true`: counted
    /// (call [`Self::op_done`]).
    pub(crate) fn write_begin(&self, ino: Ino, unique: u64, writeback: bool) -> bool {
        let mut shard = self.shard(ino).lock().unwrap();
        let e = if writeback {
            match shard.get_mut(&ino) {
                Some(e) => e,
                None => return false,
            }
        } else {
            let e = shard.entry(ino).or_default();
            e.pending.push(PendingWrite { unique });
            e
        };
        e.ops_in_flight += 1;
        true
    }

    /// Claim the registered write `unique` of `[off, off + len)` as it is
    /// answered `ok` (module doc): what its reply must wait for.
    pub(crate) fn claim(&self, ino: Ino, unique: u64, off: u64, len: u64, ok: bool) -> Claim {
        let mut shard = self.shard(ino).lock().unwrap();
        let Some(e) = shard.get_mut(&ino) else {
            return Claim::Reply;
        };
        let Some(i) = e.pending.iter().position(|w| w.unique == unique) else {
            // Not registered: a writeback write (its folios are under
            // writeback for the round trip), or one the table forgot.
            return Claim::Reply;
        };
        e.pending.swap_remove(i);
        if !e.pending.is_empty() {
            // Not expected (one write-through write per inode at a time):
            // the records stay for the other write's claim, and nothing
            // can be ruled out for this one.
            e.marks |= Mark::S5.bit();
            drop(shard);
            self.stats.suspect_marks[Mark::S5.index()].fetch_add(1, Relaxed);
            if self.stats.concurrent_writes.fetch_add(1, Relaxed) == 0 {
                tracing::warn!(
                    ino,
                    "two write-through WRITEs of one inode in progress at once; its pages are \
                     taken as not current (logged once)"
                );
            }
            return Claim::Reply;
        }
        let (start, end) = self.pages(off, len);
        // Clears every record, also of pages it does not cover (module
        // doc): a later write of those pages is copied after this reply.
        let hits = e.reads.hits(start, end);
        e.reads.clear();
        e.drop_done = false;
        if !ok {
            return Claim::Reply;
        }
        e.last_size = Some(e.last_size.unwrap_or(0).max(off.saturating_add(len)));
        let plain = hits.iter().any(|h| h.2 == ReadKind::Plain);
        let repair: Vec<(u64, u64)> = hits
            .iter()
            .filter(|h| h.2 != ReadKind::Plain)
            .map(|h| (h.0, h.1))
            .collect();
        let write_repair = self.config().repair_on();
        if plain || (!repair.is_empty() && !write_repair) {
            e.marks |= Mark::S5.bit();
        }
        drop(shard);
        if plain {
            self.stats.ambiguous_writes.fetch_add(1, Relaxed);
        }
        if plain || (!repair.is_empty() && !write_repair) {
            self.stats.suspect_marks[Mark::S5.index()].fetch_add(1, Relaxed);
        }
        if repair.is_empty() || !write_repair {
            return Claim::Reply;
        }
        // The page the writer may hold locked (KF2): the last of its
        // request, when the write does not cover it whole. (A read sent
        // while the write was registered cannot have hit a page the writer
        // holds locked — the reader waits for the lock first — but records
        // merge, so the kind of a hit says nothing about one page.)
        let end_byte = off.saturating_add(len);
        let last = end_byte.saturating_sub(1) >> self.page_shift;
        let partial = !end_byte.is_multiple_of(self.page_size()) || off > last << self.page_shift;
        let tail = (partial && hits.iter().any(|h| h.0 <= last && last < h.1)).then_some(last);
        let ranges = merge(repair)
            .into_iter()
            .flat_map(|(s, e)| match tail {
                Some(t) if s <= t && t < e => vec![(s, t), (t + 1, e)],
                _ => vec![(s, e)],
            })
            .filter(|(s, e)| s < e)
            .collect();
        Claim::Repair { ranges, tail }
    }

    /// A registered write whose reply was dropped unanswered (fuser then
    /// answers `EIO`): no claim will come, so its registration goes, and
    /// nothing can be ruled out for its pages (S5).
    pub(crate) fn abandon(&self, ino: Ino, unique: u64) {
        let mut shard = self.shard(ino).lock().unwrap();
        if let Some(e) = shard.get_mut(&ino) {
            if let Some(i) = e.pending.iter().position(|w| w.unique == unique) {
                e.pending.swap_remove(i);
                e.marks |= Mark::S5.bit();
                drop(shard);
                self.stats.suspect_marks[Mark::S5.index()].fetch_add(1, Relaxed);
            }
        }
    }

    /// Inodes with a handle that can write (the unseen-drop fault).
    pub(crate) fn writable_inodes(&self) -> Vec<Ino> {
        self.shards
            .iter()
            .flat_map(|s| {
                s.lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, e)| e.write_handles > 0)
                    .map(|(ino, _)| *ino)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// One W3b notification written.
    pub(crate) fn count_dir_notice(&self) {
        self.stats.dir_attr_notices.fetch_add(1, Relaxed);
    }

    /// `n` changed inodes W3b did not need a notice for.
    pub(crate) fn count_dir_notices_skipped(&self, n: usize) {
        self.stats
            .dir_attr_notices_skipped
            .fetch_add(n as u64, Relaxed);
    }

    /// Plan 42 F21: a request that carries attributes, or changes the
    /// namespace, arrives from task `tid` (the attribute watch's
    /// [`crate::attrwatch::AttrWatch::arrive`]; untracked unless W3b is
    /// narrowed).
    pub(crate) fn attr_arrive(&self, tid: u32) -> crate::attrwatch::Arrival {
        if self.config().dir_notice_mode() == NoticeMode::Narrowed {
            self.watch.arrive(tid)
        } else {
            crate::attrwatch::Arrival::untracked()
        }
    }

    pub(crate) fn attr_watch(&self) -> &crate::attrwatch::AttrWatch {
        &self.watch
    }

    pub(crate) fn stats(&self) -> &PageCacheStats {
        &self.stats
    }

    /// The counters.
    pub fn counts(&self) -> PageCacheCounts {
        let get = |a: &AtomicU64| a.load(Relaxed);
        PageCacheCounts {
            repairs: get(&self.stats.repairs),
            repair_pages: get(&self.stats.repair_pages),
            repairs_late: get(&self.stats.repairs_late),
            repairs_released: get(&self.stats.repairs_released),
            repair_wait_ms: get(&self.stats.repair_wait_ms),
            repair_wait_max_ms: get(&self.stats.repair_wait_max_ms),
            ambiguous_writes: get(&self.stats.ambiguous_writes),
            concurrent_writes: get(&self.stats.concurrent_writes),
            dir_attr_notices: get(&self.stats.dir_attr_notices),
            dir_attr_notices_skipped: get(&self.stats.dir_attr_notices_skipped),
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

/// Page ranges sorted and merged where they touch.
fn merge(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match out.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Option<CacheStamp> = Some(CacheStamp(7));

    /// Plan 42 F21: `auto` writes W3b's notice for every operation under
    /// `--cto strict` (unchanged by the narrowing) and narrowed in bounded
    /// mode; `on` and `off` override both (the shipped table fixes
    /// nothing).
    #[test]
    fn the_notice_mode_follows_the_switch_and_the_mode() {
        use crate::Switch::{Auto, Off, On};
        for (switch, strict, mode) in [
            (Auto, true, NoticeMode::Always),
            (Auto, false, NoticeMode::Narrowed),
            (On, true, NoticeMode::Always),
            (On, false, NoticeMode::Always),
            (Off, true, NoticeMode::Off),
            (Off, false, NoticeMode::Off),
        ] {
            let config = PageCacheConfig {
                dir_change_notify: switch,
                strict,
                ..Default::default()
            };
            assert_eq!(config.dir_notice_mode(), mode, "{switch:?} strict={strict}");
            let t = PageCacheTable::new(config);
            // Only the narrowed mode tracks attribute replies.
            let a = t.attr_arrive(7);
            assert_eq!(
                t.attr_watch().replying(a, 1).is_some() || t.attr_watch().changed(&[1]).0 == [1],
                mode == NoticeMode::Narrowed,
                "{switch:?} strict={strict}"
            );
        }
    }

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
            assert!(matches!(e.arms[..], [Arm::Open { fh: 2, .. }]));
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

    const P: u64 = 4096;

    fn table() -> std::sync::Arc<PageCacheTable> {
        PageCacheTable::with_page_size(PageCacheConfig::default(), P)
    }

    #[test]
    fn a_read_set_merges_keeps_the_stronger_kind_and_stays_bounded() {
        let mut r = ReadSet::default();
        r.add(3, 5, ReadKind::Plain, 1);
        r.add(5, 6, ReadKind::Armed, 0);
        assert_eq!(
            r.0,
            vec![(3, 6, ReadKind::Armed, 1)],
            "adjacent: merged, stronger kind, later generation"
        );
        r.add(10, 11, ReadKind::Pending, 2);
        r.add(4, 4, ReadKind::Pending, 2);
        assert_eq!(r.0.len(), 2, "an empty range adds nothing");
        assert_eq!(
            r.hits(5, 11),
            vec![(5, 6, ReadKind::Armed), (10, 11, ReadKind::Pending)]
        );
        assert!(r.hits(6, 10).is_empty());
        let mut r = ReadSet::default();
        for i in 0..40 {
            r.add(i * 10, i * 10 + 1, ReadKind::Plain, 0);
        }
        assert!(r.0.len() <= ReadSet::MAX);
        // Every page recorded is still covered.
        for i in 0..40 {
            assert_eq!(r.hits(i * 10, i * 10 + 1).len(), 1, "page {}", i * 10);
        }
    }

    /// An inode with a write handle and no drop: a read is `Plain`; after
    /// an announced drop it is `Armed`; while a write is pending,
    /// `Pending`. Without a handle that can write, no drop and no write,
    /// nothing is recorded.
    #[test]
    fn reads_are_recorded_with_what_may_have_happened() {
        let t = table();
        assert!(!t.read_begin(42, 0, P), "no entry: not even counted");
        assert!(!t.open_reply(facts(1, S)));
        t.handle_used(42, 1); // the open's drop is complete: armed
        assert!(t.read_begin(42, 3 * P, P));
        t.with_entry(42, |e| {
            assert_eq!(e.unwrap().reads.0, vec![(3, 4, ReadKind::Armed, 1)])
        });
        assert!(t.write_begin(42, 7, false));
        assert!(t.read_begin(42, 9 * P, 10));
        t.with_entry(42, |e| {
            assert!(e.unwrap().reads.0.contains(&(9, 10, ReadKind::Pending, 1)));
        });
        t.claim(42, 7, 0, P, true);
        // Not armed, nothing pending, no handle that can write.
        assert!(t.read_begin(42, 5 * P, P));
        t.with_entry(42, |e| assert!(e.unwrap().reads.0.is_empty()));
        t.seed_handles(42, 1, 1);
        assert!(t.read_begin(42, 5 * P, P));
        t.with_entry(42, |e| {
            assert_eq!(e.unwrap().reads.0, vec![(5, 6, ReadKind::Plain, 1)])
        });
        for _ in 0..5 {
            t.op_done(42);
        }
        assert_eq!(t.ops_in_flight(42), 0);
    }

    /// The claim (§5.2.3): `Armed`/`Pending` hits are repaired, `Plain`
    /// ones mark S5; every record is cleared, also outside the write; a
    /// write never registered (page writeback) claims nothing; a failed
    /// write clears and repairs nothing. (Checked to fail with the Plain
    /// rule and the clear each removed.)
    #[test]
    fn a_claim_repairs_what_a_known_drop_may_have_fetched() {
        let t = table();
        t.open_reply(OpenFacts {
            write_access: true,
            ..facts(1, S)
        });
        t.handle_used(42, 1); // the drop is complete: armed
        t.read_begin(42, 3 * P, P);
        t.read_begin(42, 20 * P, P);
        assert!(t.write_begin(42, 7, false));
        assert_eq!(
            t.claim(42, 7, 3 * P, 2 * P, true),
            Claim::Repair {
                ranges: vec![(3, 4)],
                tail: None
            },
            "page 3 only: page 4 was never read (and the write covers it whole)"
        );
        t.with_entry(42, |e| {
            let e = e.unwrap();
            assert!(e.reads.0.is_empty(), "page 20's record goes too");
            assert!(!e.drop_done);
            assert_eq!(e.last_size, Some(5 * P));
        });
        // Not armed any more: a read is Plain, and its hit only marks.
        t.read_begin(42, 3 * P, P);
        assert!(t.write_begin(42, 8, false));
        assert_eq!(t.claim(42, 8, 3 * P, P, true), Claim::Reply);
        t.with_entry(42, |e| assert_ne!(e.unwrap().marks & Mark::S5.bit(), 0));
        assert_eq!(t.counts().ambiguous_writes, 1);
        // Page writeback is never registered, never claimed.
        assert!(t.write_begin(42, 9, true));
        assert_eq!(t.claim(42, 9, 0, P, true), Claim::Reply);
        // A failed write repairs nothing and clears the records.
        let seq = t.arm_drop(42, false, false);
        t.drop_written(42, seq);
        t.read_begin(42, 0, P);
        assert!(t.write_begin(42, 10, false));
        assert_eq!(t.claim(42, 10, 0, P, false), Claim::Reply);
        t.with_entry(42, |e| assert!(e.unwrap().reads.0.is_empty()));
    }

    /// A RELEASE of a handle no request ever carried: the kernel may have
    /// failed the open after the reply, skipping its drop, so the next open
    /// at the same stamp drops; a used handle's release marks nothing.
    #[test]
    fn a_release_of_an_unused_dropping_handle_marks_the_drop_lost() {
        let t = current();
        assert!(
            !t.open_reply(facts(2, Some(CacheStamp(8)))),
            "stamp moved: drop"
        );
        t.released(42, 2, false);
        t.with_entry(42, |e| assert_ne!(e.unwrap().marks & Mark::S2.bit(), 0));
        assert!(
            !t.open_reply(facts(3, Some(CacheStamp(8)))),
            "its drop may not have run"
        );
        t.handle_used(42, 3);
        t.released(42, 3, false);
        assert!(
            t.open_reply(facts(4, Some(CacheStamp(8)))),
            "a used handle's drop ran"
        );
    }

    /// A drop of an inode the table holds no entry for (a data
    /// notification for an inode this kernel never opened) leaves no entry
    /// once written; an open answered while it is still to be written
    /// inherits its arm and keeps the entry.
    #[test]
    fn a_written_drop_of_an_unknown_inode_leaves_no_entry() {
        let t = table();
        let seq = t.arm_drop(7, false, true);
        t.with_entry(7, |e| assert!(e.unwrap().armed()));
        t.drop_written(7, seq);
        let seq = t.arm_drop(8, true, false);
        t.drop_written(8, seq);
        assert_eq!(t.entries(), 0, "a FORGET never comes for these");
        // Armed, then opened before the drop is written: the entry stays.
        let seq = t.arm_drop(42, false, true);
        t.open_reply(facts(1, S));
        t.drop_written(42, seq);
        t.with_entry(42, |e| assert!(e.unwrap().handles == 1));
    }

    /// A write that ends inside a page a read hit: that page is the
    /// repair's last, on its own; a write that covers its last page whole
    /// has none.
    #[test]
    fn the_writers_possibly_locked_last_page_is_repaired_last() {
        let t = table();
        t.open_reply(facts(1, S));
        t.handle_used(42, 1);
        t.read_begin(42, 0, 4 * P);
        t.write_begin(42, 1, false);
        assert_eq!(
            t.claim(42, 1, P, 2 * P + 10, true),
            Claim::Repair {
                ranges: vec![(1, 3)],
                tail: Some(3)
            }
        );
        let seq = t.arm_drop(42, false, true);
        t.drop_written(42, seq);
        t.read_begin(42, 0, 4 * P);
        t.write_begin(42, 2, false);
        assert_eq!(
            t.claim(42, 2, P, 3 * P, true),
            Claim::Repair {
                ranges: vec![(1, 4)],
                tail: None
            },
            "the write covers its last page whole"
        );
    }

    /// A completed whole-file drop supersedes the reads recorded before it
    /// was announced (their folios are gone), not those after: the
    /// writer's own tail folio, re-created after such a drop and locked
    /// until its reply, is then not repaired — a repair of it would wait
    /// for that very reply. (Checked to fail with the supersession
    /// removed: the claim repairs page 3.)
    #[test]
    fn a_completed_drop_supersedes_the_reads_before_it() {
        let t = table();
        t.open_reply(OpenFacts {
            write_access: true,
            ..facts(1, S)
        });
        t.handle_used(42, 1);
        t.read_begin(42, 3 * P, P);
        // Another open's drop (the content changed), announced, then
        // complete.
        assert!(!t.open_reply(facts(2, Some(CacheStamp(8)))));
        t.read_begin(42, 7 * P, P); // between announcement and completion
        t.handle_used(42, 2);
        t.with_entry(42, |e| {
            let pages: Vec<u64> = e.unwrap().reads.0.iter().map(|r| r.0).collect();
            assert_eq!(pages, vec![7], "page 3's record went with the drop");
        });
        t.write_begin(42, 9, false);
        assert_eq!(
            t.claim(42, 9, 3 * P, 5 * P, true),
            Claim::Repair {
                ranges: vec![(7, 8)],
                tail: None
            }
        );
        // A partial notification supersedes nothing.
        t.read_begin(42, 3 * P, P);
        let seq = t.arm_drop(42, false, false);
        t.drop_written(42, seq);
        t.with_entry(42, |e| assert_eq!(e.unwrap().reads.0.len(), 1));
    }

    /// Two registered writes at once (not expected): the records stay for
    /// the other's claim, and the inode is marked.
    #[test]
    fn a_claim_beside_another_pending_write_keeps_the_records() {
        let t = table();
        t.open_reply(OpenFacts {
            write_access: true,
            ..facts(1, S)
        });
        t.handle_used(42, 1);
        t.write_begin(42, 1, false);
        t.read_begin(42, 0, P);
        t.write_begin(42, 2, false);
        assert_eq!(t.claim(42, 1, 0, P, true), Claim::Reply);
        t.with_entry(42, |e| {
            let e = e.unwrap();
            assert!(!e.reads.0.is_empty());
            assert_ne!(e.marks & Mark::S5.bit(), 0);
        });
        assert_eq!(
            t.claim(42, 2, 0, P, true),
            Claim::Repair {
                ranges: vec![(0, 1)],
                tail: None
            },
            "the other write's claim repairs"
        );
    }

    /// W2 off (a kernel with the fix): a repair hit marks S5 instead.
    #[test]
    fn with_the_repair_off_a_hit_marks_instead() {
        let t = PageCacheTable::with_page_size(
            PageCacheConfig {
                write_repair: crate::Switch::Off,
                ..Default::default()
            },
            P,
        );
        t.open_reply(facts(1, S));
        t.handle_used(42, 1);
        t.read_begin(42, 0, P);
        t.write_begin(42, 1, false);
        assert_eq!(t.claim(42, 1, 0, P, true), Claim::Reply);
        t.with_entry(42, |e| assert_ne!(e.unwrap().marks & Mark::S5.bit(), 0));
    }

    /// A3: a size other than the last one handed arms; an inode without
    /// an entry is not created.
    #[test]
    fn a_size_change_in_a_reply_arms() {
        let t = table();
        t.size_seen(42, 10);
        assert_eq!(t.entries(), 0);
        t.open_reply(facts(1, S));
        t.with_entry(42, |e| assert!(!e.unwrap().drop_done));
        t.size_seen(42, 10);
        t.with_entry(42, |e| assert!(e.unwrap().drop_done, "unknown size: armed"));
        t.write_begin(42, 1, false);
        t.claim(42, 1, 0, 10, true);
        t.size_seen(42, 10);
        t.with_entry(42, |e| assert!(!e.unwrap().drop_done, "the same size"));
        t.size_seen(42, 11);
        t.with_entry(42, |e| assert!(e.unwrap().drop_done));
    }

    /// A write whose reply was dropped unanswered gives its registration
    /// up and marks the inode.
    #[test]
    fn an_abandoned_write_is_unregistered_and_marked() {
        let t = table();
        t.write_begin(42, 1, false);
        t.abandon(42, 1);
        t.with_entry(42, |e| {
            let e = e.unwrap();
            assert!(e.pending.is_empty());
            assert_ne!(e.marks & Mark::S5.bit(), 0);
        });
    }

    #[test]
    fn keep_cache_parses_its_two_values() {
        assert_eq!(KeepCache::parse("auto"), Some(KeepCache::Auto));
        assert_eq!(KeepCache::parse(" OFF "), Some(KeepCache::Off));
        assert_eq!(KeepCache::parse(""), Some(KeepCache::Auto));
        assert_eq!(KeepCache::parse("on"), None);
    }
}
