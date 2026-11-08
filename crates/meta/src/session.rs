//! Plan 30 §M6: positions and the per-node session guarantees
//! (read-your-writes, monotonic reads) for local FUSE reads.
//!
//! # Positions
//!
//! A holder evaluates a forwarded op against its replica: every log
//! segment it has shipped or applied (through log sequence `seq`) plus its
//! own unshipped journal (through journal sequence `jseq`, under lease
//! `epoch`). [`Position`] names that state: `seq`, and the unshipped part
//! as a [`JournalPos`] (absent when nothing was unshipped). Segments carry
//! the journal position they ship through (the segment envelope's
//! `through`), so a replica that has applied a segment of epoch `e`
//! shipped through `t` has everything of `e`'s journal up to `t`
//! ([`SessionState::applied`]). `JournalPos` orders epoch-first: a
//! position from a tenure that ended before shipping it is below every
//! position of the next epoch, so an observer of stranded work stops
//! waiting at the takeover's epoch marker — it is not owed those effects
//! (plan 30 M13's `Tentative` rule seen by a third party).
//!
//! # The watermark
//!
//! `observed` is the highest position this node's client has seen through
//! a reply **whose effects are not installed here** (a refusal, an
//! `Exists` without a hint, an op that waited for the log): only those
//! can make a local read go backwards. A forwarded op accepted and
//! installed as a shadow does not raise it (its keys are covered by the
//! shadow, and the base rule guarantees the shadow sits on everything the
//! holder had for them), so `touch a; ls; stat .` on a non-holder never
//! waits. Volatile: a restart is a new FUSE session.
//!
//! # The read wait
//!
//! [`crate::Meta::session_wait`], called by every FUSE read path before
//! it reads:
//! 1. a queued replay (a stranded op of this node, rolled back until the
//!    replay lands) touching the keys → wait: that is the session's own
//!    acknowledged write missing from the replica;
//! 2. applied position ≥ `observed` → go (the fast path: two loads under
//!    one mutex, plus one counter read for step 1);
//! 3. every key covered by speculation (a shadow or hint) whose position
//!    is at least `observed` → go;
//! 4. otherwise wait for the applied position to advance, bounded by
//!    `CONSTELLATION_SESSION_WAIT_MS` (default 2000, 0 disables); on
//!    timeout answer anyway (degraded, not an error), warn once.
//!
//! # A watermark nobody reaches
//!
//! `observed` names positions of other nodes' state; two of its parts
//! can name what this replica will never hold (EC2 campaign 7, finding
//! B-2: a `git add` and a `cat` that "hung" for good on a node whose
//! every read was paying the whole budget, 288 timeouts of 292 reads):
//!
//! - a delegation stream generation that ended before this incarnation
//!   started. A lock's floor is the join of every releaser's frontier
//!   since the lock was first taken, so it keeps naming generations long
//!   after their `DelegateNodeEnded`; a replica that applied the `DelegateNodeEnded` while
//!   running voids the generation (`void_stream`), one that restarted
//!   since has no memory of it — `voided` and `streams` are volatile —
//!   and never reaches the grant's position. [`crate::Meta::session_wait_at`]
//!   consults the persisted delegation table before a wait with such a
//!   dependency: a generation the table once delegated (at or below its
//!   `max_gen`) and no longer lists has ended, and is voided;
//! - anything else (a journal position of a tenure this replica's
//!   `applied` never covers, a generation the table cannot account for):
//!   a watermark still unreached `CONSTELLATION_SESSION_WATERMARK_TTL_MS`
//!   (default 10 s; 0 disables the rule) after it was raised is dropped
//!   to the applied position, with a warning. The guarantee it carried is
//!   already gone (every read since answered degraded); keeping it would
//!   only make every later read wait the budget for nothing.

use crate::mutate::MutateOp;
use crate::record::LogRecord;
use crate::replay::TouchSet;
use crate::store::local::RowCommit;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// A point in one tenure's journal: `(epoch, journal seq)`, ordered
/// epoch-first.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct JournalPos {
    pub epoch: u64,
    pub jseq: u64,
}

/// Plan 30 §M11: how many delegation streams a position can name. An
/// op whose requester observed more goes to the root, which reads its
/// own log and needs no tracking (`deps_overflow_to_root`).
pub const STREAMS_CAP: usize = 8;

/// Plan 30 §M11: the per-stream pending part of a [`Position`] — for
/// each delegation generation, the highest stream index observed (a
/// delegate's acknowledgement carries its own; the root's carries every
/// generation's appended cursor). `(0, 0)` entries are empty; the used
/// entries are sorted by generation and come first. Fixed-size so a
/// position stays `Copy` and small on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Streams(pub [(u64, u64); STREAMS_CAP]);

impl Streams {
    pub const NONE: Streams = Streams([(0, 0); STREAMS_CAP]);

    pub fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.0.iter().copied().take_while(|(g, _)| *g != 0)
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.0[0].0 == 0
    }

    pub fn is_full(&self) -> bool {
        self.0[STREAMS_CAP - 1].0 != 0
    }

    pub fn get(&self, gen: u64) -> Option<u64> {
        self.iter().find(|(g, _)| *g == gen).map(|(_, i)| i)
    }

    /// Raise generation `gen` to at least `idx`. `false` when the table
    /// is full and `gen` is not in it (the caller overflows to the root).
    pub fn raise(&mut self, gen: u64, idx: u64) -> bool {
        if gen == 0 {
            return true;
        }
        for e in self.0.iter_mut() {
            if e.0 == gen {
                e.1 = e.1.max(idx);
                return true;
            }
            if e.0 == 0 {
                *e = (gen, idx);
                self.0
                    .sort_by_key(|(g, _)| if *g == 0 { u64::MAX } else { *g });
                return true;
            }
        }
        false
    }

    /// Lower generation `gen` to at most `cut` (M6's void rule: the part
    /// of an observation a stream never appended is void once the stream
    /// ended); a cut of 0 drops it.
    pub fn lower(&mut self, gen: u64, cut: u64) {
        let mut v: Vec<(u64, u64)> = self.iter().collect();
        for e in v.iter_mut() {
            if e.0 == gen {
                e.1 = e.1.min(cut);
            }
        }
        v.retain(|(_, i)| *i > 0);
        *self = Streams::NONE;
        for (k, e) in v.into_iter().enumerate() {
            self.0[k] = e;
        }
    }

    pub fn dominates(&self, other: &Streams) -> bool {
        other
            .iter()
            .all(|(g, i)| self.get(g).is_some_and(|mine| mine >= i))
    }

    /// The component-wise maximum; `None` when it would not fit.
    pub fn join(&self, other: &Streams) -> Option<Streams> {
        let mut s = *self;
        for (g, i) in other.iter() {
            if !s.raise(g, i) {
                return None;
            }
        }
        Some(s)
    }
}

/// The state a holder evaluated an op against (see the module doc).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    /// Every log segment through this sequence.
    pub seq: u64,
    /// Plus its unshipped journal through here, if it had any.
    pub pending: Option<JournalPos>,
    /// Plan 30 §M11: plus, per delegation stream, what it held ahead of
    /// the log (see [`Streams`]).
    pub streams: Streams,
}

impl Position {
    pub const ZERO: Position = Position {
        seq: 0,
        pending: None,
        streams: Streams::NONE,
    };

    /// Whether a replica at `self` has everything `other` names
    /// (component-wise; `None < Some`).
    pub fn dominates(&self, other: &Position) -> bool {
        self.seq >= other.seq
            && self.pending >= other.pending
            && self.streams.dominates(&other.streams)
    }

    /// The component-wise maximum. A stream table that would overflow
    /// keeps `self`'s streams and sets [`Position::overflowed`]'s
    /// condition (the caller checks `streams.is_full()`); see
    /// [`Streams::join`].
    pub fn join(&self, other: &Position) -> Position {
        Position {
            seq: self.seq.max(other.seq),
            pending: self.pending.max(other.pending),
            streams: self.streams.join(&other.streams).unwrap_or(self.streams),
        }
    }

    /// Whether joining `other` would exceed the stream cap.
    pub fn overflows_with(&self, other: &Position) -> bool {
        self.streams.join(&other.streams).is_none()
    }

    /// The wire form of the streams part (`(gen, idx)` pairs).
    pub fn streams_wire(&self) -> Vec<(u64, u64)> {
        self.streams.iter().collect()
    }

    pub fn with_streams_wire(mut self, wire: &[(u64, u64)]) -> Position {
        for (g, i) in wire {
            if !self.streams.raise(*g, *i) {
                break;
            }
        }
        self
    }

    pub fn to_postcard(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    pub fn from_postcard(bytes: &[u8]) -> Position {
        postcard::from_bytes(bytes).unwrap_or(Position::ZERO)
    }

    /// The lowest log sequence at which a delete of what this position
    /// observed could ship: its `seq` when nothing was unshipped, else
    /// the next segment (plan 29 M6's `ship_floor`, now exact for an idle
    /// holder). A replica below it may install an `Exists` hint read at
    /// this position. Not where the hint retires: the next segment need
    /// not carry the entry (it may have been cut before the entry was
    /// journaled) — that is the whole position (`store::spec`'s
    /// `hint_reached`).
    pub fn hint_floor(&self) -> u64 {
        self.seq + u64::from(self.pending.is_some())
    }
}

/// A key a FUSE read reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ReadKey {
    /// `lookup(parent, name)`.
    Dentry(u64, String),
    /// `getattr`/`open`/`readlink`/`getxattr`/`listxattr` of an inode.
    Ino(u64),
    /// `readdir` of a directory: every entry in it. Never covered by
    /// speculation; only the applied position satisfies it.
    Dir(u64),
}

/// The keys a speculative entry makes current (its dentries and inodes),
/// or that a queued replay will change (those, plus the parent directories
/// whose attributes — times, link count — it changes).
#[derive(Clone, Debug, Default)]
pub struct KeySet {
    pub dentries: Vec<(u64, String)>,
    pub inos: Vec<u64>,
}

impl KeySet {
    ///
    /// For *covering* (speculation installed from a reply) this is exactly
    /// the records' `TouchSet` — the dentries and inodes the holder's
    /// `base` was computed over (plan 30 M5 round 3: the last shipped
    /// touch of the op's keys). The parent directory's inode is *not*
    /// covered: its attributes may have been changed by later shipped
    /// creates in the same directory that the base does not look at, so
    /// after a refusal raised `observed`, `getattr(parent)` waits for the
    /// applied position. (Without a raise — the common case — it is on the
    /// fast path anyway, and the shadow's records do update the parent's
    /// times at once.)
    pub fn from_records(records: &[LogRecord]) -> KeySet {
        let t = TouchSet::from_records(records.iter());
        KeySet {
            dentries: t.dentries.into_iter().collect(),
            inos: t.inos.into_iter().collect(),
        }
    }

    /// A placeholder's keys (`AwaitRetry`): the dentries and exclusive
    /// inodes the dropped row touched.
    pub fn from_touch_set(t: &TouchSet) -> KeySet {
        // The shared directories are the parents of the names: a listing of
        // one is waited for through the names (`touches` of `Dir`), but a
        // read of another name in it, or of its own attributes, does not
        // wait for a row that changes them only by a commutative merge.
        KeySet {
            dentries: t.dentries.iter().cloned().collect(),
            inos: t.inos.iter().copied().collect(),
        }
    }

    /// The keys `op` will touch (for a queued replay: no records yet).
    pub fn from_op(op: &MutateOp) -> KeySet {
        let mut k = KeySet::default();
        match op {
            MutateOp::Mkdir {
                parent, name, ino, ..
            }
            | MutateOp::Create {
                parent, name, ino, ..
            }
            | MutateOp::Symlink {
                parent, name, ino, ..
            }
            | MutateOp::Mknod {
                parent, name, ino, ..
            }
            | MutateOp::Link { ino, parent, name }
            | MutateOp::Publish {
                ino, parent, name, ..
            } => {
                k.dentry(*parent, name);
                k.inos.push(*ino);
            }
            MutateOp::Unlink { parent, name } | MutateOp::Rmdir { parent, name } => {
                k.dentry(*parent, name)
            }
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            }
            | MutateOp::Exchange {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                k.dentry(*parent, name);
                k.dentry(*new_parent, new_name);
            }
            MutateOp::Setattr { ino, .. }
            | MutateOp::SetManifest { ino, .. }
            | MutateOp::SetXattr { ino, .. }
            | MutateOp::RemoveXattr { ino, .. } => k.inos.push(*ino),
            MutateOp::AtimeBatch { .. } => {}
            MutateOp::Records { records } => return KeySet::from_records(records),
        }
        k
    }

    fn dentry(&mut self, parent: u64, name: &str) {
        self.dentries.push((parent, name.to_string()));
        self.inos.push(parent);
    }

    /// Whether this set makes `key` current. `Dir` only through an entry
    /// in it or the directory inode itself.
    pub fn covers(&self, key: &ReadKey) -> bool {
        match key {
            ReadKey::Dentry(p, n) => self.dentries.iter().any(|(dp, dn)| dp == p && dn == n),
            ReadKey::Ino(i) => self.inos.contains(i),
            ReadKey::Dir(_) => false,
        }
    }

    /// Whether this set changes anything `key` reads.
    pub fn touches(&self, key: &ReadKey) -> bool {
        match key {
            ReadKey::Dentry(p, n) => self.dentries.iter().any(|(dp, dn)| dp == p && dn == n),
            ReadKey::Ino(i) => self.inos.contains(i),
            ReadKey::Dir(d) => self.inos.contains(d) || self.dentries.iter().any(|(p, _)| p == d),
        }
    }
}

/// How one read's wait ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWait {
    /// The applied position already covered `observed`.
    Fast,
    /// Speculation at least as new as `observed` covers every key.
    Covered,
    /// Waited this long, then satisfied.
    Waited(Duration),
    /// Waited the whole budget; answered from the replica anyway.
    TimedOut(Duration),
    /// The wait is disabled (`CONSTELLATION_SESSION_WAIT_MS=0`).
    Disabled,
}

/// Log2 wait-time buckets: `[0]` < 1 ms, `[i]` < 2^i ms, the last one
/// everything from 2^(N-2) ms up.
pub const WAIT_BUCKETS: usize = 14;

/// Counters for `status` (plan 30 §M6's read-latency measurement).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStats {
    /// Reads that went through the session check.
    pub reads: u64,
    pub fast: u64,
    pub covered: u64,
    /// Reads that waited (and were then satisfied), and those whose wait
    /// began because a queued replay touched their keys.
    pub waited: u64,
    pub replay_blocked: u64,
    /// Reads answered degraded after the whole budget.
    pub timeouts: u64,
    /// The subset of `timeouts` while M4 held-back journal rows existed on
    /// the answering holder or here (the coordinator's decision 1: a held
    /// row stalls the shipped-through position).
    pub degraded_held: u64,
    /// `waits_ms[i]`: waits (satisfied or timed out) in log2 bucket `i`
    /// (see [`WAIT_BUCKETS`]).
    pub waits_ms: [u64; WAIT_BUCKETS],
    /// Total milliseconds spent waiting.
    pub wait_ms_total: u64,
    /// Times `observed` was raised (a reply whose effects were not
    /// installed here).
    pub raised: u64,
    /// Watermarks dropped after `CONSTELLATION_SESSION_WATERMARK_TTL_MS`
    /// unreached, and stream dependencies voided because the persisted
    /// delegation table showed their generation ended (module doc, "A
    /// watermark nobody reaches").
    pub abandoned: u64,
    pub voided_ended: u64,
    /// Plan 30 §M9: reads on a holder under a `Backup`/`S3` acknowledgement
    /// policy whose wait began because the unshipped journal touched their
    /// keys and was not yet durable (on every backup, or in the log).
    pub durability_blocked: u64,
    /// Plan 30 §M9: fast-path acknowledgements that waited for
    /// durability, their total wait, and those left in doubt (the lease
    /// was lost meanwhile; resubmitted by rid).
    pub fast_acks_waited: u64,
    pub fast_ack_wait_us_total: u64,
    pub fast_acks_in_doubt: u64,
    /// Plan 43 C1-2 (D4): fast-path answers given with the gate off whose
    /// rows were re-tagged `Lone`.
    pub fast_acks_downgraded: u64,
    /// Plan 43 C2 (§5.4, D5): fast-path waits that found the gate off on a
    /// node that is neither lone nor holding an epoch
    /// (`DurableWait::Unexpected`), resubmitted in doubt
    /// (`status.ack.ungated_unexpected`).
    pub ungated_unexpected: u64,
    /// Plan 43 C2 (§5.3 item 5): client mutations answered `EIO` because
    /// they were settled (`MutateOutcome::Abandoned`: refused for good by
    /// the sequencer in force, nothing of them ever executes;
    /// `status.ack.settled_eio`), as opposed to an unsettled in-doubt
    /// `EIO`.
    pub settled_eio: u64,
}

#[derive(Default)]
struct Inner {
    /// The applied log sequence as last reported (refreshed from the
    /// store on the slow path).
    applied_seq: u64,
    /// The journal position of the last applied (or shipped) segment.
    applied: Option<JournalPos>,
    /// The same, as persisted by the previous incarnation (`Meta::open`
    /// seeds it): what this replica holds, for what a read waits on —
    /// but not what its clients have seen (`deps`, `frontier`): a fresh
    /// FUSE session has seen nothing yet, and advertising the seed there
    /// held a restarted node's first writes at a holder that could not
    /// reach it (`git-under-flock-faults`, 120 s in doubt after a
    /// whole-cluster kill).
    seeded: Option<JournalPos>,
    /// Plan 30 §M9 × §M11: the holder's journal position the pre-S3
    /// stream installed here contiguously from the applied log (backup-
    /// acknowledged rows, in the holder's order). A dependency on the
    /// holder's journal (`Position::pending`) is reached through it as
    /// through `applied`: this replica holds those rows, and a takeover
    /// re-ships exactly them. Cleared whenever streamed speculation may
    /// have been rolled back ([`SessionState::clear_streamed`]).
    streamed: Option<JournalPos>,
    observed: Position,
    /// When `observed` was last raised, while it stays unreached (the
    /// watermark TTL's clock; see the module doc).
    observed_since: Option<Instant>,
    /// Speculation installed with a position, until the applied position
    /// dominates it.
    covering: Vec<(KeySet, Position)>,
    /// Plan 30 §M11: per delegation generation, the stream index this
    /// replica holds (applied, executed or appended).
    streams: std::collections::BTreeMap<u64, u64>,
    /// Plan 30 §M11: generations whose `DelegateNodeEnded` this replica applied:
    /// any dependency on them is satisfied (void past the cut).
    voided: std::collections::BTreeSet<u64>,
    /// The log's cut of each voided generation: the stream index its
    /// root appended before the `DelegateNodeEnded` (what this replica holds of it
    /// *from the log*, never a delegate's own executed-but-unappended
    /// rows). A dependency past it is *lost*: a tentative
    /// acknowledgement, stranded and replayed by rid (see
    /// [`SessionState::deps_lost`]).
    void_cuts: std::collections::BTreeMap<u64, u64>,
    /// What the table said when the store was opened ([`SessionState::
    /// seed_ended_before_open`]): its highest generation and the live ones.
    /// A generation at or below the first and not among the second ended
    /// before this incarnation began: the only ones [`SessionState::
    /// ended_before_open`] lets a check void lazily (an end applied since,
    /// from a streamed-ahead install, may still strand: only the durable
    /// log voids those).
    open_table: Option<(u64, std::collections::BTreeSet<u64>)>,
    /// Plan 30 §M11: the delegation stream positions this node's
    /// clients were answered with (a shadow or hint installed from a
    /// delegate's reply does not raise `observed`, M6's rule, but a
    /// later write of the same client must order after it wherever it
    /// executes): the `deps` of the next write carry them.
    frontier: std::collections::BTreeMap<u64, u64>,
    /// Plan 30 §M11: the log part of the same (the root's replies, whose
    /// shadows do not raise `observed` either).
    frontier_log: Position,
    /// Plan 30 §M9 × §M6: the latest applied epoch marker that announced
    /// a re-shipped predecessor tail (`LogRecord::TailFollows`), as the
    /// journal position it reached. Until the applied position moves
    /// past it, a target of an older epoch is not reached: the effects
    /// it names are acknowledged rows of the predecessor that the
    /// successor ships next (see [`SessionState::owe`]).
    owed: Option<JournalPos>,
    /// Chunk metered-own-rows: rows of `applied`'s tenure applied here
    /// above its journal seq — shipped ahead of a row still deferred on
    /// the holder, which keeps every segment's `through` below it (the
    /// segments' `rows`). With `excused`, a watermark is reached past a
    /// deferred row of this node's own.
    ahead: Option<(u64, RowRanges)>,
    /// Chunk metered-own-rows: rows of `observed`'s tenure that it does
    /// not owe — this node's own transactions, unshipped on the holder,
    /// whose effects this replica carries (`OwnRows::settle`). Every
    /// other row through it is owed as before.
    excused: std::collections::BTreeSet<u64>,
    /// Plan 43 C2d: per generation this node is the delegate of, what the
    /// core says of the answers to this node's own clients
    /// ([`SessionState::set_deleg_durable`]).
    deleg_durable: std::collections::BTreeMap<u64, DelegDurable>,
    /// Plan 43 C2d: per generation, the highest `through` ever published
    /// for it. It outlives the generation's entry in `deleg_durable` (a
    /// withdrawal) so that a row the group once covered stays `Durable`
    /// for a waiter that comes or wakes after it; the withdrawn
    /// generations' share is bounded by [`WITHDRAWN_COVERED_CAP`].
    deleg_covered: std::collections::BTreeMap<u64, u64>,
}

/// How many withdrawn generations keep their high-water `through`
/// ([`SessionState::set_deleg_durable`]). A waiter is on one generation for
/// at most one `DURABLE_WAIT_BUDGET`; the oldest tombstone is dropped first
/// (generations only grow), and a waiter that outlives its tombstone is
/// answered `Lost`, which is in doubt, resubmitted by rid: slower, never
/// wrong.
const WITHDRAWN_COVERED_CAP: usize = 16;

/// Disjoint inclusive journal seq ranges, `start -> end`.
#[derive(Debug, Default, Clone)]
struct RowRanges(std::collections::BTreeMap<u64, u64>);

/// Most ranges [`Inner::ahead`] keeps; past it the lowest are dropped
/// (a read then waits for the log, as without them).
const AHEAD_RANGES_CAP: usize = 4096;
/// Most rows [`Inner::excused`] keeps; past it none are (likewise).
const EXCUSED_CAP: usize = 4096;

impl RowRanges {
    fn insert(&mut self, row: u64) {
        if self.end_covering(row).is_some() {
            return;
        }
        let mut start = row;
        let mut end = row;
        if let Some((&s, &e)) = self.0.range(..row).next_back() {
            if e + 1 == row {
                start = s;
            }
        }
        if let Some(&e) = self.0.get(&(row + 1)) {
            self.0.remove(&(row + 1));
            end = e;
        }
        self.0.insert(start, end);
        while self.0.len() > AHEAD_RANGES_CAP {
            if let Some((s, e)) = self.0.pop_first() {
                tracing::debug!(
                    first = s,
                    last = e,
                    "rows applied ahead past the cap: forgotten (a read waits for them to ship)"
                );
            }
        }
    }

    /// The end of the range holding `row`, if any.
    fn end_covering(&self, row: u64) -> Option<u64> {
        self.0
            .range(..=row)
            .next_back()
            .filter(|(_, e)| **e >= row)
            .map(|(_, e)| *e)
    }

    /// Forget everything at or below `row`.
    fn prune_through(&mut self, row: u64) {
        let keep = self.0.split_off(&(row + 1));
        let straddling = self.0.iter().next_back().filter(|(_, e)| **e > row);
        let mut rest = keep;
        if let Some((_, e)) = straddling {
            rest.insert(row + 1, *e);
        }
        self.0 = rest;
    }
}

impl Inner {
    /// `a` joined with `b`, for a watermark: stream entries this replica
    /// already reaches (applied that far, or voided) are dropped first —
    /// they wait for nothing — so the cap is spent on what is still
    /// owed. `None`: what is still owed does not fit the cap. The plain
    /// [`Position::join`] kept `a`'s streams and dropped *all* of `b`'s on
    /// an overflow: a lock grant's floor naming a ninth generation was
    /// then not waited for (sim `locks-delegated-writes` seed 200981, a
    /// stale read under the lock).
    fn join_owed(&self, a: &Position, b: &Position) -> Option<Position> {
        let mut owed: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for (g, i) in a.streams.iter().chain(b.streams.iter()) {
            if i == 0 || self.voided.contains(&g) || self.streams.get(&g).is_some_and(|m| *m >= i) {
                continue;
            }
            let e = owed.entry(g).or_insert(0);
            *e = (*e).max(i);
        }
        let mut streams = Streams::NONE;
        for (g, i) in owed {
            if !streams.raise(g, i) {
                return None;
            }
        }
        Some(Position {
            seq: a.seq.max(b.seq),
            pending: a.pending.max(b.pending),
            streams,
        })
    }

    fn applied_position(&self) -> Position {
        let mut streams = Streams::NONE;
        for (g, i) in &self.streams {
            if !streams.raise(*g, *i) {
                break;
            }
        }
        Position {
            seq: self.applied_seq,
            pending: self.applied.max(self.seeded),
            streams,
        }
    }

    /// `applied_position().dominates(target)` with the void rule: a
    /// dependency on an ended generation counts as satisfied.
    fn reaches(&self, target: &Position) -> bool {
        let owed = match (self.owed, target.pending) {
            (Some(marker), Some(p)) => p.epoch < marker.epoch && self.applied <= Some(marker),
            _ => false,
        };
        !owed
            && self.applied_seq >= target.seq
            && self.applied.max(self.seeded).max(self.streamed) >= target.pending
            && target.streams.iter().all(|(g, i)| {
                // Plan 30 §M14: a stream with nothing appended yet (a fresh
                // generation's `(gen, 0)`) is reached by everyone.
                i == 0 || self.voided.contains(&g) || self.streams.get(&g).is_some_and(|m| *m >= i)
            })
    }

    /// Whether `observed` is reached: [`Self::reaches`], or, with rows
    /// excused, every row of its tenure through it that is not excused is
    /// here — at or below the applied (or streamed) journal seq, or
    /// applied out of order above it (`ahead`) — with its log and stream
    /// parts reached as ever.
    fn reaches_observed(&self) -> bool {
        let target = self.observed;
        if self.reaches(&target) {
            return true;
        }
        let Some(p) = target.pending else {
            return false;
        };
        if self.excused.is_empty() {
            return false;
        }
        let owed = self
            .owed
            .is_some_and(|marker| p.epoch < marker.epoch && self.applied <= Some(marker));
        if owed
            || !self.reaches(&Position {
                pending: None,
                ..target
            })
        {
            return false;
        }
        let Some(base) = self.applied.max(self.seeded).max(self.streamed) else {
            return false;
        };
        if base.epoch != p.epoch {
            return false;
        }
        let ahead = match &self.ahead {
            Some((e, rows)) if *e == p.epoch => Some(rows),
            _ => None,
        };
        let mut next = base.jseq + 1;
        while next <= p.jseq {
            if self.excused.contains(&next) {
                next += 1;
            } else if let Some(end) = ahead.and_then(|a| a.end_covering(next)) {
                next = end + 1;
            } else {
                return false;
            }
        }
        true
    }
}

/// The session state `Meta` carries.
/// Plan 30 §M9: the outcome of [`SessionState::wait_durable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableWait {
    /// The watermark covers the row (how long it took).
    Durable(Duration),
    /// The gate is off and the lease is still this node's.
    Ungated,
    /// The gate is off because the lease was lost.
    Lost,
    TimedOut,
    /// Plan 43 C2 (§5.4, D5): the gate is off while the lease is still this
    /// node's, but the node is neither lone nor holding a continuation
    /// epoch (a release or a handoff after the fast-path write, a tenure
    /// that is not usable): nothing says the row is committed and nothing
    /// lets this node answer on its disk alone. In doubt: the op is
    /// resubmitted by rid and the core answers from the row's commit.
    Unexpected,
}

/// Plan 43 C2d (C2 spec §4), plan 46 M1.1 step 4: per generation this node
/// is the delegate of, what the core knows of the delegation's group, for
/// the FUSE fast path: the commit rule its own clients' rows are journaled
/// with — the rule's variant and the configuration's version it was decided
/// under — and how far the group has committed the generation's stream.
///
/// The fast path stamps its row with `commit` and nothing else: it does not
/// decide a class. Under the writer lock it compares the version published
/// here with the one the store records (`SessionState::deleg_commit_at_journal`,
/// `Meta::finish_local`): a change between the publication and the journal
/// (a `DelegateNodeGroup` record the store applied since) sends the op to the core,
/// which stamps the row from the configuration it reads then.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegDurable {
    /// The commit rule of a row the fast path journals under the generation
    /// now (`Core::deleg_rule`): `Final` answers at once, `Group` waits.
    pub commit: RowCommit,
    /// The highest stream index whose prefix is committed: the log carries
    /// it, or every member of the group (and of a requested change) holds
    /// it (`Core::deleg_committed_through`).
    pub through: u64,
    /// The generation is stopped, lapsed or lost its backup: nothing more
    /// will commit under it from here, the op's answer is in doubt.
    pub lost: bool,
}

pub struct SessionState {
    inner: Mutex<Inner>,
    cv: Condvar,
    budget_ms: AtomicU64,
    /// `CONSTELLATION_SESSION_WATERMARK_TTL_MS` (0: never dropped).
    watermark_ttl_ms: AtomicU64,
    warned: AtomicBool,
    stats: Mutex<SessionStats>,
    /// Plan 30 §M9: this node holds under a `Backup`/`S3` acknowledgement
    /// policy, so its own reads must not observe unshipped journal rows
    /// that are not yet durable (the "observers of tentative effects"
    /// rule applied to the holder's own clients).
    durable_gate: AtomicBool,
    /// Plan 30 §M9: the gate went off because the lease was lost (a
    /// deposition), not because the policy went back to `Local`: a
    /// mutation waiting on it is in doubt.
    durable_lost: AtomicBool,
    /// The highest journal seq that is durable under that policy.
    durable_jseq: AtomicU64,
    /// Plan 43 C1-2: the class of the answers the core gives its own
    /// node's clients (`Core::root_commit`), published with the gate:
    /// the class a FUSE fast-path row is journaled with
    /// (`Meta::finish_local`). Stored before the gate so a reader that sees
    /// the gate has seen the class.
    durable_class: AtomicU64,
    /// Plan 43 C2 (§5.4, D5): with the gate off, this node may answer its
    /// own clients on its disk alone — it is a lone root, or holds a
    /// continuation epoch (`Core::root_lone`, `epoch_held`). Published with
    /// the gate, stored before it.
    durable_lone_ok: AtomicBool,
    /// I1 class 2: the driver's veto on `durable_lone_ok`, read at the
    /// moment of the check, not published by a step: `true` while this
    /// node has a lapse of its own the core has not been handed (a frozen
    /// process wakes its FUSE threads before its driver). See
    /// [`SessionState::set_lone_guard`].
    lone_guard: OnceLock<LoneGuard>,
}

/// See [`SessionState::set_lone_guard`].
pub type LoneGuard = Arc<dyn Fn() -> bool + Send + Sync>;

fn budget_default() -> u64 {
    std::env::var("CONSTELLATION_SESSION_WAIT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

fn watermark_ttl_default() -> u64 {
    std::env::var("CONSTELLATION_SESSION_WATERMARK_TTL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}

impl Default for SessionState {
    fn default() -> Self {
        SessionState {
            inner: Mutex::new(Inner::default()),
            cv: Condvar::new(),
            budget_ms: AtomicU64::new(budget_default()),
            watermark_ttl_ms: AtomicU64::new(watermark_ttl_default()),
            warned: AtomicBool::new(false),
            stats: Mutex::new(SessionStats::default()),
            durable_gate: AtomicBool::new(false),
            durable_lost: AtomicBool::new(false),
            durable_jseq: AtomicU64::new(0),
            durable_class: AtomicU64::new(RowCommit::LONE.to_bits()),
            // (Nothing published yet: a replica no core drives answers as
            // before. The core publishes `false` at its start, before the
            // mount serves, and the truth at every step after.)
            durable_lone_ok: AtomicBool::new(true),
            lone_guard: OnceLock::new(),
        }
    }
}

fn bucket(d: Duration) -> usize {
    let ms = d.as_millis() as u64;
    if ms == 0 {
        return 0;
    }
    ((64 - ms.leading_zeros()) as usize).min(WAIT_BUCKETS - 1)
}

impl SessionState {
    pub fn budget(&self) -> Duration {
        Duration::from_millis(self.budget_ms.load(Ordering::Relaxed))
    }

    pub fn set_budget_ms(&self, ms: u64) {
        self.budget_ms.store(ms, Ordering::Relaxed);
    }

    /// How long an unreached watermark is kept (module doc, "A watermark
    /// nobody reaches"); zero: for good.
    pub fn watermark_ttl(&self) -> Duration {
        Duration::from_millis(self.watermark_ttl_ms.load(Ordering::Relaxed))
    }

    pub fn set_watermark_ttl_ms(&self, ms: u64) {
        self.watermark_ttl_ms.store(ms, Ordering::Relaxed);
    }

    /// The generations `observed` depends on that this replica neither
    /// holds far enough nor knows as void (what a wait would block on).
    pub fn unreached_streams(&self) -> Vec<u64> {
        let g = self.inner.lock().unwrap();
        g.observed
            .streams
            .iter()
            .filter(|(gen, i)| {
                *i > 0 && !g.voided.contains(gen) && !g.streams.get(gen).is_some_and(|m| *m >= *i)
            })
            .map(|(gen, _)| gen)
            .collect()
    }

    /// The generations `deps` names (beyond index 0) that this replica
    /// neither holds far enough nor knows as void.
    pub fn unreached_deps(&self, deps: &Position) -> Vec<u64> {
        let g = self.inner.lock().unwrap();
        deps.streams
            .iter()
            .filter(|(gen, i)| {
                *i > 0 && !g.voided.contains(gen) && !g.streams.get(gen).is_some_and(|m| *m >= *i)
            })
            .map(|(gen, _)| gen)
            .collect()
    }

    /// Void every unreached generation `observed` names for which
    /// `ended` says the generation is over (the persisted delegation
    /// table once delegated it and no longer lists it): the rule
    /// `void_stream` applies at a `DelegateNodeEnded`, for a replica that applied
    /// the record in an earlier incarnation. Returns how many.
    pub fn void_ended(&self, ended: impl Fn(u64) -> bool) -> usize {
        self.void_ended_among(self.unreached_streams(), ended)
    }

    /// [`Self::void_ended`] over `gens` (the unreached ones, as
    /// [`Self::unreached_streams`] or [`Self::unreached_deps`] list them).
    pub fn void_ended_among(&self, gens: Vec<u64>, ended: impl Fn(u64) -> bool) -> usize {
        let mut n = 0;
        for gen in gens {
            if !ended(gen) {
                continue;
            }
            let cut = self.stream_applied(gen);
            self.void_stream(gen, cut);
            n += 1;
        }
        if n > 0 {
            self.stats.lock().unwrap().voided_ended += n as u64;
        }
        n
    }

    /// Drop `observed` to the applied position when it has stayed
    /// unreached for the watermark TTL (module doc). `true` if dropped.
    fn abandon_stale_watermark(&self) -> bool {
        let ttl = self.watermark_ttl();
        if ttl.is_zero() {
            return false;
        }
        let mut g = self.inner.lock().unwrap();
        let Some(since) = g.observed_since else {
            return false;
        };
        let observed = g.observed;
        if g.reaches_observed() {
            g.observed_since = None;
            return false;
        }
        let age = since.elapsed();
        if age < ttl {
            return false;
        }
        let applied = g.applied_position();
        g.observed = applied;
        g.observed_since = None;
        g.excused.clear();
        drop(g);
        self.stats.lock().unwrap().abandoned += 1;
        tracing::warn!(
            dropped = ?observed,
            ?applied,
            ?age,
            "dropping a session watermark this replica has not reached for the watermark TTL: \
             every read since has answered degraded, and nothing it names is coming (a \
             generation or tenure that ended before this incarnation started)"
        );
        true
    }

    pub fn stats(&self) -> SessionStats {
        *self.stats.lock().unwrap()
    }

    pub fn count_fast_ack(&self, waited: Duration) {
        let mut s = self.stats.lock().unwrap();
        s.fast_acks_waited += 1;
        s.fast_ack_wait_us_total += waited.as_micros() as u64;
    }

    pub fn count_fast_ack_downgraded(&self) {
        self.stats.lock().unwrap().fast_acks_downgraded += 1;
    }

    /// Plan 43 C2 (D5): a fast-path wait found the gate off on a node that
    /// may not answer on its disk alone.
    pub fn count_ungated_unexpected(&self) {
        self.stats.lock().unwrap().ungated_unexpected += 1;
    }

    /// Plan 43 C2 (§5.3 item 5): a client mutation was answered `EIO` as
    /// settled (`MutateOutcome::Abandoned`).
    pub fn count_settled_eio(&self) {
        self.stats.lock().unwrap().settled_eio += 1;
    }

    pub fn count_fast_ack_in_doubt(&self) {
        self.stats.lock().unwrap().fast_acks_in_doubt += 1;
    }

    pub fn observed(&self) -> Position {
        self.inner.lock().unwrap().observed
    }

    /// Plan 30 §M11: a delegate answered one of this node's clients at
    /// stream position `(gen, idx)`: the next write's `deps` carry it
    /// (see `deps`).
    pub fn note_frontier(&self, pos: &Position) {
        let mut g = self.inner.lock().unwrap();
        if pos.seq > g.frontier_log.seq {
            g.frontier_log.seq = pos.seq;
        }
        if pos.pending > g.frontier_log.pending {
            g.frontier_log.pending = pos.pending;
        }
        for (gen, idx) in pos.streams.iter() {
            let cur = g.frontier.entry(gen).or_insert(0);
            if idx > *cur {
                *cur = idx;
            }
        }
    }

    /// Plan 30 §M11: the position a write submitted here depends on —
    /// `observed`, with every delegation stream position this node
    /// holds or was answered with (its own delegate executions, the
    /// streams it applied, the replies it installed). `None` when they
    /// do not fit `Streams` (the write goes to the root, which orders
    /// after everything).
    pub fn deps(&self) -> Option<Position> {
        let g = self.inner.lock().unwrap();
        let mut streams = g.observed.streams;
        for (gen, idx) in g.frontier.iter().chain(g.streams.iter()) {
            if g.voided.contains(gen) {
                continue;
            }
            if !streams.raise(*gen, *idx) {
                return None;
            }
        }
        // A voided generation is depended on only up to its cut: past it
        // nothing will ever be appended (this node's own tentative rows
        // are replayed by rid, and its writes wait for those replays), and
        // a dependency there would never be met (`deps_lost`). `observed`
        // can name one past the cut (a reply that arrived after the
        // `DelegateNodeEnded` applied here: long-delegated seed 70176).
        for gen in g.voided.iter() {
            if streams.get(*gen).is_some() {
                let cut = g
                    .void_cuts
                    .get(gen)
                    .copied()
                    .unwrap_or_else(|| g.streams.get(gen).copied().unwrap_or(0));
                streams.lower(*gen, cut);
            }
        }
        Some(Position {
            seq: g.observed.seq.max(g.frontier_log.seq).max(g.applied_seq),
            pending: g
                .observed
                .pending
                .max(g.frontier_log.pending)
                .max(g.applied),
            streams,
        })
    }

    /// Everything this node's clients have seen or been acknowledged:
    /// [`Self::deps`], except that a stream table that does not fit
    /// keeps the generations that do (the highest ones: the older a
    /// generation, the likelier it is applied or void everywhere).
    ///
    /// A cluster lock's release carries it (EC2 campaign 4 B-1): the
    /// next holder waits for it, so every file the releaser wrote under
    /// the lock — not only the locked one — reads as the releaser left
    /// it.
    pub fn frontier(&self) -> Position {
        if let Some(p) = self.deps() {
            return p;
        }
        let g = self.inner.lock().unwrap();
        let mut all: std::collections::BTreeMap<u64, u64> = g.observed.streams.iter().collect();
        for (gen, idx) in g.frontier.iter().chain(g.streams.iter()) {
            if g.voided.contains(gen) {
                continue;
            }
            let e = all.entry(*gen).or_insert(0);
            *e = (*e).max(*idx);
        }
        let mut streams = Streams::NONE;
        for (gen, idx) in all.into_iter().rev().take(STREAMS_CAP) {
            streams.raise(gen, idx);
        }
        Position {
            seq: g.observed.seq.max(g.frontier_log.seq).max(g.applied_seq),
            pending: g
                .observed
                .pending
                .max(g.frontier_log.pending)
                .max(g.applied),
            streams,
        }
    }

    /// Plan 30 §M11: this replica holds delegation stream `gen` through
    /// `idx` — applied from a segment carrying the row's origin, executed
    /// here as the delegate, or appended here as the root.
    pub fn note_stream(&self, gen: u64, idx: u64) {
        if gen == 0 {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let cur = g.streams.entry(gen).or_insert(0);
        if idx > *cur {
            *cur = idx;
        }
        let applied = g.applied_position();
        g.covering.retain(|(_, p)| !applied.dominates(p));
        drop(g);
        self.cv.notify_all();
    }

    /// The inverse of [`Self::note_stream`] for a root's journal append
    /// that left the journal unshipped (a deposed root's rebuild, a
    /// recall strand): stream `gen` goes back to `to`. Only lowered, and
    /// only from what the store counted (`local::retract_log_idx_tx`);
    /// no wake-up, lowering satisfies no wait.
    pub fn retract_stream(&self, gen: u64, to: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(cur) = g.streams.get_mut(&gen) {
            if *cur > to {
                *cur = to;
            }
        }
    }

    /// Plan 30 §M11: generation `gen` ended at `cut` (a `DelegateNodeEnded` record
    /// applied): every dependency on it past the cut is void. The
    /// watermark is lowered to the cut; from here on a wait on the stream
    /// is satisfied at once.
    pub fn void_stream(&self, gen: u64, cut: u64) {
        let mut g = self.inner.lock().unwrap();
        g.voided.insert(gen);
        g.observed.streams.lower(gen, cut);
        drop(g);
        self.cv.notify_all();
    }

    /// The end of `gen` this root journaled (`DelegateNodeEnded`, voiding it here
    /// at once) left the journal unshipped with its transaction (a deposed
    /// root's rollback or rebuild), and the table, as the log has it, still
    /// lists the generation: it is live again here too. Left voided, a
    /// dependency on its rows past what the log holds passed at once,
    /// though those rows had been rolled back with the end and were
    /// appended again (or replayed by rid) only later, behind the op that
    /// depended on them (`long-delegated-late-answers` seed 259: a deposed
    /// root ended a generation through its delegate's rows 4 and 5 and
    /// lost them with the end; re-elected, it executed the delegate's next
    /// op, whose deps named row 5, before the delegate re-streamed them,
    /// and the log broke the delegate's program order). No wake-up:
    /// lifting a void satisfies no wait.
    pub fn unvoid(&self, gen: u64) {
        let mut g = self.inner.lock().unwrap();
        g.voided.remove(&gen);
        g.void_cuts.remove(&gen);
    }

    /// What a restart forgets (EC2 campaign 7, the `git-under-flock-
    /// causal` reader): the stream indices and the voided generations
    /// live here in memory, and a replica that opens its store again
    /// re-applies nothing below its applied position — so a generation
    /// whose `DelegateNodeEnded` it applied before the restart is neither held nor
    /// voided afterwards, and a watermark naming it (a lock grant's
    /// floor: the releaser's frontier, which still names the stream it
    /// was answered from) is never reached: every read under it waits
    /// the whole session budget, for the rest of the process. Seed from
    /// what the store persists: every generation in `gens` (the live ones
    /// and every one the store keeps a log index or a void cut of;
    /// numbers are sparse, ADR-43: `epoch << 32 | n`, so there is no
    /// range to walk) is held through the log's index of it, and one the
    /// table no longer lists is voided at that cut, if the table has
    /// reached it (`gen <= max_gen`, the table's highest). A generation the
    /// table named and ended before this replica applied any row or end
    /// of it has no key and is not seeded: a check of a dependency that
    /// names it voids it lazily ([`Self::ended_before_open`], with
    /// [`Self::seed_ended_before_open`]; `Meta::reaches_deps`), at cut 0
    /// (what the store holds of it), as the eager seeding gave it.
    ///
    /// Plan 43 C2: the cut of a generation the table no longer lists is
    /// the one its `DelegateNodeEnded` record carried (`void_cut`, persisted when
    /// the record was applied from the log), else — a generation a `DelegateNodeEnded`
    /// of an older log ended — what the log holds of it (`log_idx`).
    pub fn seed_generations(
        &self,
        max_gen: u64,
        gens: &std::collections::BTreeSet<u64>,
        live: &std::collections::BTreeSet<u64>,
        log_idx: &dyn Fn(u64) -> u64,
        void_cut: &dyn Fn(u64) -> Option<u64>,
    ) {
        let mut g = self.inner.lock().unwrap();
        for &gen in gens {
            let idx = log_idx(gen);
            let cur = g.streams.entry(gen).or_insert(0);
            if idx > *cur {
                *cur = idx;
            }
            // Only a number the table has reached can have ended: a key
            // above its highest (a deposed root's stranded appends leave
            // `deleg_log_idx` behind, `retract_log_idx_tx`, while the strand
            // rolls the table back below it) names a grant the log may yet
            // carry, whose rows must not meet a void
            // (`ended_before_open`'s rule).
            // A kept cut is an end the log applied (or, ADR-43, a marker
            // proved the grant never reaches it: `orphaned_tx`,
            // a number the table never listed), so it voids above the
            // highest too; a deposed root's own stranded end leaves no cut
            // (`retract_void_cut_tx`).
            let kept = void_cut(gen);
            if (gen <= max_gen || kept.is_some()) && !live.contains(&gen) {
                let cut = kept.unwrap_or(idx);
                g.voided.insert(gen);
                g.observed.streams.lower(gen, cut);
                // Set, not raised: the store's cut is the log's. After an
                // in-process rebuild a deposed root's own cut of the
                // generation (its journaled `DelegateNodeEnded`, stranded since) may
                // sit above the log's, and keeping it would count a
                // dependency between the two as not lost. At an open the
                // map is empty, and set is raise.
                g.void_cuts.insert(gen, cut);
            }
        }
        drop(g);
        self.cv.notify_all();
    }

    /// Remember the table as the store was opened with it, or as an in-process
    /// rebuild replaced it (`max_gen`, the `live` generations): see
    /// [`Self::ended_before_open`].
    pub fn seed_ended_before_open(&self, max_gen: u64, live: &std::collections::BTreeSet<u64>) {
        self.inner.lock().unwrap().open_table = Some((max_gen, live.clone()));
    }

    /// Whether `gen` ended before this incarnation began: the table named
    /// it (at or below its highest generation) and did not list it when
    /// the store was opened. The seeding voids the ones it holds a key of;
    /// this is the rest, which a check of a dependency may void lazily
    /// (cut 0, what the store holds of it): a generation whose end this
    /// incarnation applied, or only saw streamed ahead, is the log's to
    /// void.
    pub fn ended_before_open(&self, gen: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .open_table
            .as_ref()
            .is_some_and(|(max, live)| gen <= *max && !live.contains(&gen))
    }

    /// The log's cut of voided generation `gen` (see `Inner::void_cuts`).
    /// Only raised: rows of the generation the log carries after its
    /// `DelegateNodeEnded` (a root that was itself the delegate ships its own rows
    /// after ending the generation) are not lost.
    pub fn note_void_cut(&self, gen: u64, cut: u64) {
        let mut g = self.inner.lock().unwrap();
        let e = g.void_cuts.entry(gen).or_insert(0);
        *e = (*e).max(cut);
    }

    /// The cut of voided generation `gen` as this replica knows it (see
    /// `Inner::void_cuts`): `None` while it is not voided here.
    pub fn void_cut(&self, gen: u64) -> Option<u64> {
        let g = self.inner.lock().unwrap();
        if !g.voided.contains(&gen) {
            return None;
        }
        Some(
            g.void_cuts
                .get(&gen)
                .copied()
                .unwrap_or_else(|| g.streams.get(&gen).copied().unwrap_or(0)),
        )
    }

    /// Whether generation `gen` is voided here.
    pub fn is_voided(&self, gen: u64) -> bool {
        self.inner.lock().unwrap().voided.contains(&gen)
    }

    /// Whether `deps` names a transaction the log will never carry: a
    /// generation that ended below the index it depends on. The void rule
    /// would count it as satisfied, but the effect it names was a
    /// tentative acknowledgement, stranded and replayed by rid *after*
    /// the generation ended — executing the dependent op now would put
    /// it in the log ahead of its cause (sim `marker-order`,
    /// long-delegated seeds 74189, 76967, long-delegated-backup 77901).
    /// Such an op is not executed: the executor answers `Held` (or leaves
    /// an inbox op in the inbox), and the requester re-sends it with
    /// fresh `deps` once its own replays have settled.
    pub fn deps_lost(&self, deps: &Position) -> bool {
        let g = self.inner.lock().unwrap();
        deps.streams.iter().any(|(gen, i)| {
            i > 0
                && g.voided.contains(&gen)
                && i > g
                    .void_cuts
                    .get(&gen)
                    .copied()
                    .unwrap_or_else(|| g.streams.get(&gen).copied().unwrap_or(0))
        })
    }

    /// Whether this replica has everything `deps` names, with the void
    /// rule (a dependency on a recalled generation is satisfied).
    pub fn reaches(&self, deps: &Position) -> bool {
        self.inner.lock().unwrap().reaches(deps)
    }

    /// The streams part of [`Self::reaches`] alone: what the *root*
    /// checks before executing (its own tenure's journal positions are
    /// its own; an older tenure's are applied or void).
    pub fn reaches_streams(&self, deps: &Position) -> bool {
        let g = self.inner.lock().unwrap();
        deps.streams.iter().all(|(gen, i)| {
            i == 0 || g.voided.contains(&gen) || g.streams.get(&gen).is_some_and(|m| *m >= i)
        })
    }

    /// The stream index this replica holds of `gen` (0: none).
    pub fn stream_applied(&self, gen: u64) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .streams
            .get(&gen)
            .copied()
            .unwrap_or(0)
    }

    pub fn applied(&self) -> Position {
        self.inner.lock().unwrap().applied_position()
    }

    /// Plan 30 §M9 × §M6: the segment just applied is a sealed backup's
    /// takeover marker at `marker` (its epoch and `through`) whose
    /// successor re-ships the predecessor's acknowledged tail next. Until
    /// the applied journal position passes `marker`, a target position
    /// of an older epoch is not reached (backup-crash-slow seed 600066
    /// and six more: a refusal observed the old holder's acknowledged,
    /// unshipped rename; a read after the marker, before the re-shipped
    /// tail, went back to the name's older state).
    pub fn owe(&self, marker: JournalPos) {
        let mut g = self.inner.lock().unwrap();
        if g.owed.is_none_or(|o| o < marker) {
            g.owed = Some(marker);
        }
    }

    /// What the previous incarnation persisted (`Meta::open`): the
    /// applied log sequence and journal position this replica holds.
    /// Reached by reads, not advertised by writes (see `Inner::seeded`).
    pub fn seed_applied(&self, seq: u64, pos: Option<JournalPos>) {
        let mut g = self.inner.lock().unwrap();
        g.applied_seq = g.applied_seq.max(seq);
        if pos > g.seeded {
            g.seeded = pos;
        }
    }

    /// The replica applied (or shipped) a segment at `seq` shipped through
    /// `through` (a fenced segment passes `None`: its journal position
    /// took no effect).
    pub fn advance(&self, seq: u64, through: Option<JournalPos>) {
        self.advance_rows(seq, through, &[]);
    }

    /// [`Self::advance`] for a segment carrying journal `rows` of
    /// `through`'s tenure (chunk metered-own-rows: those above the applied
    /// journal seq are kept, see `Inner::ahead`).
    pub fn advance_rows(&self, seq: u64, through: Option<JournalPos>, rows: &[u64]) {
        let mut g = self.inner.lock().unwrap();
        g.applied_seq = g.applied_seq.max(seq);
        if through > g.applied {
            g.applied = through;
        }
        if let Some(a) = g.applied {
            if g.ahead.as_ref().is_none_or(|(e, _)| *e != a.epoch) {
                g.ahead = Some((a.epoch, RowRanges::default()));
            }
            let (_, ahead) = g.ahead.as_mut().expect("set above");
            if through.is_some_and(|t| t.epoch == a.epoch) {
                for row in rows.iter().filter(|r| **r > a.jseq) {
                    ahead.insert(*row);
                }
            }
            ahead.prune_through(a.jseq);
        }
        let applied = g.applied_position();
        g.covering.retain(|(_, p)| !applied.dominates(p));
        drop(g);
        self.cv.notify_all();
    }

    /// Plan 43 C1-4: whether row `jseq` of `epoch`'s tenure is in the log this
    /// replica applied: at or below the applied journal position of that
    /// epoch, or applied ahead of it (a segment named it while an earlier row
    /// was deferred). The pre-S3 stream delivers a transaction a millisecond
    /// after the segment carrying it now and then; installed after the fact,
    /// a committed entry would wait for a segment that already went by.
    pub fn row_in_log(&self, epoch: u64, jseq: u64) -> bool {
        let g = self.inner.lock().unwrap();
        let Some(a) = g.applied.filter(|a| a.epoch == epoch) else {
            return false;
        };
        jseq <= a.jseq
            || g.ahead
                .as_ref()
                .is_some_and(|(e, rows)| *e == epoch && rows.end_covering(jseq).is_some())
    }

    /// Plan 30 §M9 × §M11: the pre-S3 stream installed the holder's
    /// journal here through `pos` (see `Inner::streamed`).
    pub fn note_streamed(&self, pos: JournalPos) {
        let mut g = self.inner.lock().unwrap();
        if Some(pos) > g.streamed {
            g.streamed = Some(pos);
            drop(g);
            self.cv.notify_all();
        }
    }

    /// Streamed speculation may have been rolled back (a strand, a
    /// takeover, a newer epoch): only the applied log counts again.
    pub fn clear_streamed(&self) {
        self.inner.lock().unwrap().streamed = None;
    }

    /// A reply whose effects are not installed here observed `pos`.
    pub fn raise_observed(&self, pos: Position) {
        self.raise_observed_excusing(pos, &[]);
    }

    /// [`Self::raise_observed`], except that the journal rows in
    /// `excused` (inclusive ranges of `pos`'s tenure) are not owed: this
    /// node's own transactions, still unshipped on the holder, whose
    /// effects this replica carries (chunk metered-own-rows,
    /// `OwnRows::settle`). Joined with what `observed` already excuses: a
    /// row stays excused only if every observation whose position covers
    /// it excuses it.
    pub fn raise_observed_excusing(&self, pos: Position, excused: &[(u64, u64)]) {
        let mut g = self.inner.lock().unwrap();
        // Past the cap even after dropping what is reached (nine
        // generations owed at once), the newest are kept: the oldest
        // generations' streams are the likeliest in the log already.
        let next = match g.join_owed(&g.observed, &pos) {
            Some(p) => p,
            None => {
                let mut all: std::collections::BTreeMap<u64, u64> =
                    g.observed.streams.iter().collect();
                for (gen, i) in pos.streams.iter() {
                    let e = all.entry(gen).or_insert(0);
                    *e = (*e).max(i);
                }
                let kept: Vec<(u64, u64)> = all.into_iter().rev().take(STREAMS_CAP).collect();
                Position {
                    seq: g.observed.seq.max(pos.seq),
                    pending: g.observed.pending.max(pos.pending),
                    streams: Streams::NONE,
                }
                .with_streams_wire(&kept)
            }
        };
        let fresh: std::collections::BTreeSet<u64> = excused
            .iter()
            .flat_map(|(first, last)| *first..=*last)
            .take(EXCUSED_CAP + 1)
            .collect();
        let old = std::mem::take(&mut g.excused);
        let observations = [(g.observed.pending, &old), (pos.pending, &fresh)];
        let mut joined = std::collections::BTreeSet::new();
        if let Some(e) = next.pending.map(|p| p.epoch) {
            // A row not excused by an observation that covers it is owed.
            let excuses = |row: u64| {
                observations.iter().all(|(p, x)| match p {
                    Some(p) if p.epoch == e => row > p.jseq || x.contains(&row),
                    _ => true,
                })
            };
            for (p, x) in &observations {
                if p.is_some_and(|p| p.epoch == e) {
                    joined.extend(x.iter().copied().filter(|r| excuses(*r)));
                }
            }
        }
        if joined.len() <= EXCUSED_CAP {
            g.excused = joined;
        }
        if next != g.observed {
            g.observed = next;
            g.observed_since = Some(Instant::now());
            drop(g);
            self.stats.lock().unwrap().raised += 1;
        }
    }

    /// Whether this replica reaches the `observed` watermark (with its
    /// excused rows): a read waits for nothing it owes.
    pub fn reaches_observed(&self) -> bool {
        self.inner.lock().unwrap().reaches_observed()
    }

    /// Speculation installed for `keys` from a reply at `pos`.
    pub fn note_covering(&self, keys: KeySet, pos: Position) {
        let mut g = self.inner.lock().unwrap();
        if !g.applied_position().dominates(&pos) {
            g.covering.push((keys, pos));
        }
        drop(g);
        self.cv.notify_all();
    }

    /// Wake waiters (a replay resolved).
    pub fn notify(&self) {
        self.cv.notify_all();
    }

    /// Plan 30 §M9: the core's durability watermark for this holder's
    /// journal (`gate`: a non-`Local` policy is in force). Every advance
    /// wakes the waiters. `lone_ok` (plan 43 C2, §5.4): with the gate off,
    /// the node may answer its own clients on its disk alone (it is lone,
    /// or holds an epoch).
    pub fn set_durable(&self, gate: bool, jseq: u64, lost: bool, class: RowCommit, lone_ok: bool) {
        self.durable_lost.store(lost, Ordering::Relaxed);
        // Release, read with Acquire after the fast path's journal write
        // (`durable_lone_ok`, `wait_durable`): a writer that reads the gate
        // or this flag after its row is journaled sees the core's latest
        // "may not answer alone" (plan 43 C2f follow-up).
        self.durable_lone_ok.store(lone_ok, Ordering::Release);
        self.durable_class.store(class.to_bits(), Ordering::Release);
        self.durable_gate.store(gate, Ordering::Release);
        let before = self.durable_jseq.swap(jseq, Ordering::Relaxed);
        if !gate || jseq > before {
            self.cv.notify_all();
        }
    }

    /// Plan 30 §M9: the FUSE fast path's acknowledgement wait under a
    /// durability gate. The op is journaled at or below `jseq`; return
    /// once the core's durable watermark covers it, or once the gate is
    /// off — because the lease was lost (`Lost`: in doubt, resubmitted by
    /// rid), because the node is lone or holds an epoch (`Ungated`: the
    /// row is this node's, answered on its disk; plan 43 C2 §5.4: never a
    /// wait for the archive in an epoch), or for any other reason
    /// (`Unexpected`: in doubt, counted and resubmitted, D5).
    pub fn wait_durable(&self, jseq: u64, budget: Duration) -> DurableWait {
        let started = Instant::now();
        loop {
            if !self.durable_gate.load(Ordering::Acquire) {
                return if self.durable_lost.load(Ordering::Relaxed) {
                    DurableWait::Lost
                } else if self.durable_lone_ok() {
                    DurableWait::Ungated
                } else {
                    DurableWait::Unexpected
                };
            }
            if self.durable_jseq.load(Ordering::Relaxed) >= jseq {
                return DurableWait::Durable(started.elapsed());
            }
            let waited = started.elapsed();
            if waited >= budget {
                return DurableWait::TimedOut;
            }
            let slice = (budget - waited).min(Duration::from_millis(20));
            let g = self.inner.lock().unwrap();
            let _ = self.cv.wait_timeout(g, slice).unwrap();
        }
    }

    /// Plan 43 C2d: what the core knows of the delegation `gen` held here
    /// (`None`: not held). Every change wakes the waiters.
    pub fn set_deleg_durable(&self, gen: u64, d: Option<DelegDurable>) {
        let mut g = self.inner.lock().unwrap();
        let changed = match d {
            Some(d) => {
                let covered = g.deleg_covered.entry(gen).or_insert(0);
                *covered = (*covered).max(d.through);
                g.deleg_durable.insert(gen, d) != Some(d)
            }
            None => {
                let was_held = g.deleg_durable.remove(&gen).is_some();
                // Nothing was ever covered: no tombstone to keep.
                if g.deleg_covered.get(&gen) == Some(&0) {
                    g.deleg_covered.remove(&gen);
                }
                // Bound the tombstones: the oldest withdrawn generation
                // goes first.
                loop {
                    let withdrawn: Vec<u64> = g
                        .deleg_covered
                        .keys()
                        .filter(|k| !g.deleg_durable.contains_key(k))
                        .copied()
                        .collect();
                    if withdrawn.len() <= WITHDRAWN_COVERED_CAP {
                        break;
                    }
                    g.deleg_covered.remove(&withdrawn[0]);
                }
                was_held
            }
        };
        drop(g);
        if changed {
            self.cv.notify_all();
        }
    }

    /// Plan 43 C2d: the commit rule a delegate's fast-path row of `gen` is
    /// journaled with now; `None`: the core has not published the
    /// generation (the op goes through the core).
    pub fn deleg_row_class(&self, gen: u64) -> Option<RowCommit> {
        self.inner
            .lock()
            .unwrap()
            .deleg_durable
            .get(&gen)
            .map(|d| d.commit)
    }

    /// Plan 46 M1.1 step 4: the rule the fast path stamps its row of `gen`
    /// with at the journaling, under the store's writer lock, given the
    /// version of the generation's configuration the store records there
    /// (`store_version`: the latest `DelegateNodeGroup` record applied, 0 when none).
    /// `None`: the core has not published the generation, or the state it
    /// published its rule on is not the store's now: the rule was decided
    /// under another version (a record applied since). The op is not
    /// journaled here, it goes through the core, which decides from the
    /// state it reads then. The row is never classed again by its place in
    /// the stream.
    ///
    /// Only a delegate generation's own group (`Group` on the delegate)
    /// names the applied version: the archive's rule (`ack=s3`) and a
    /// designation's `Final` have none to compare.
    pub fn deleg_commit_at_journal(&self, gen: u64, store_version: u64) -> Option<RowCommit> {
        let commit = {
            let g = self.inner.lock().unwrap();
            g.deleg_durable.get(&gen)?.commit
        };
        let names_applied = matches!(
            commit,
            RowCommit::Group {
                on: crate::store::local::GroupOn::Delegate,
                ..
            }
        );
        if names_applied && commit.config_version() != Some(store_version) {
            return None;
        }
        Some(commit)
    }

    /// Plan 43 C2d: the delegate fast path's wait for its group. The op
    /// journaled stream index `idx` of generation `gen`: `Durable` once the
    /// group has committed the prefix through it (the log, or every member
    /// of the group; a row that is committed is answered whatever became of
    /// the delegation since, its withdrawal included: the generation's
    /// high-water `through` outlives it, within [`WITHDRAWN_COVERED_CAP`]
    /// withdrawn generations); `Lost` for a row not covered if the
    /// generation is not held, is
    /// stopped, lapsed or lost its backup (the answer is in doubt: the core
    /// dedups the retry by rid and answers from the row's commit);
    /// `TimedOut` at `budget`. Never `Ungated`: a delegate's row waits for
    /// its commit (the root's append, or the backup), as the core's own
    /// answer does.
    pub fn wait_deleg_durable(&self, gen: u64, idx: u64, budget: Duration) -> DurableWait {
        let started = Instant::now();
        let mut g = self.inner.lock().unwrap();
        loop {
            // Covered once, covered for good: the high-water outlives the
            // generation's withdrawal and its `lost` flag.
            if g.deleg_covered.get(&gen).is_some_and(|c| *c >= idx) {
                return DurableWait::Durable(started.elapsed());
            }
            match g.deleg_durable.get(&gen) {
                Some(d) if !d.lost => {}
                _ => return DurableWait::Lost,
            }
            let waited = started.elapsed();
            if waited >= budget {
                return DurableWait::TimedOut;
            }
            // (Every change notifies; the slice only bounds a missed wake.)
            let slice = (budget - waited).min(Duration::from_millis(20));
            g = self.cv.wait_timeout(g, slice).unwrap().0;
        }
    }

    /// Plan 43 C1-2: the class the core published with the gate. The
    /// fast path's rows are journaled with it and its wait follows it.
    pub fn published_class(&self) -> RowCommit {
        RowCommit::from_bits(self.durable_class.load(Ordering::Acquire))
    }

    /// Plan 43 C2 (§5.4, D5): with the gate off, the core said this node may
    /// answer its own clients alone (it is lone, or holds an epoch).
    ///
    /// I1 class 2: not while the guard ([`Self::set_lone_guard`]) says this
    /// node has a lapse its core has not been handed: what the core
    /// published was judged on a directory older than the lapse.
    pub fn durable_lone_ok(&self) -> bool {
        self.durable_lone_ok.load(Ordering::Acquire)
            && !self.lone_guard.get().is_some_and(|guard| guard())
    }

    /// Install the driver's veto on [`Self::durable_lone_ok`]: `guard`
    /// returns `true` while this node has a lapse of its own (a process
    /// freeze, a held runtime) that its core has not been handed yet. A
    /// root resumed from a freeze is judged lone on the directory it held
    /// before it; its FUSE threads run before its driver can hand the lapse
    /// and a newer directory, and must not answer on the disk alone then. A
    /// row journaled `Lone` meanwhile waits like a gated one and takes the
    /// in-doubt path through the core (D5).
    ///
    /// Set once (the first guard stays), read on every fast-path operation
    /// without a lock; the guard must not take one either
    /// (`OffCore::lapse_unhanded` reads atomics and a clock).
    pub fn set_lone_guard(&self, guard: LoneGuard) {
        let _ = self.lone_guard.set(guard);
    }

    /// Whether the durability gate is on (one atomic load: the fast
    /// path's only cost when it is off).
    pub fn durable_gated(&self) -> bool {
        self.durable_gate.load(Ordering::Relaxed)
    }

    pub fn durable_jseq(&self) -> u64 {
        self.durable_jseq.load(Ordering::Relaxed)
    }

    /// A new FUSE session (restart): forget what the last one observed.
    pub fn reset(&self) {
        let mut g = self.inner.lock().unwrap();
        g.observed = Position::ZERO;
        g.observed_since = None;
        g.excused.clear();
        g.covering.clear();
    }

    /// The decision for one check, with `applied_seq` refreshed by the
    /// caller when it is stale. `None` = keep waiting.
    ///
    /// `floor` is a per-read target on top of `observed` (plan 30 §M8: a
    /// `cto=strict` open waits for the position its ReadIndex answer, or
    /// its delegation, carried — without raising the node-wide watermark,
    /// so unrelated reads never wait for it).
    fn check(
        &self,
        keys: &[ReadKey],
        replay_touches: bool,
        floor: &Position,
    ) -> Option<SessionWait> {
        if replay_touches {
            return None;
        }
        let g = self.inner.lock().unwrap();
        // Both watermarks, each on its own (never one dropped: see
        // `join_owed`); `observed` with the rows it excuses. A shadow
        // must dominate what each still owes: the streams the replica
        // already reaches are stripped (a root reply's position names
        // none), unless what is owed overflows the cap.
        let targets: Vec<Position> = [
            (g.reaches_observed(), g.observed),
            (g.reaches(floor), *floor),
        ]
        .into_iter()
        .filter(|(reached, _)| !reached)
        .map(|(_, t)| g.join_owed(&t, &Position::ZERO).unwrap_or(t))
        .collect();
        if targets.is_empty() {
            return Some(SessionWait::Fast);
        }
        let covered = keys.iter().all(|k| {
            g.covering
                .iter()
                .filter(|(ks, _)| ks.covers(k))
                .any(|(_, p)| targets.iter().all(|t| p.dominates(t)))
        });
        covered.then_some(SessionWait::Covered)
    }

    /// One non-blocking check (a caller that cannot block a thread —
    /// the simulation's single-threaded runtime — polls this instead).
    pub fn ready(
        &self,
        keys: &[ReadKey],
        applied_seq: u64,
        replay_touches: bool,
        floor: &Position,
    ) -> bool {
        {
            let mut g = self.inner.lock().unwrap();
            if applied_seq > g.applied_seq {
                g.applied_seq = applied_seq;
            }
        }
        self.check(keys, replay_touches, floor).is_some()
    }

    /// The wait loop; `refresh` returns the store's applied sequence and
    /// whether a queued replay touches `keys`, `held` whether M4 held
    /// rows exist.
    pub(crate) fn wait(
        &self,
        keys: &[ReadKey],
        floor: &Position,
        mut refresh: impl FnMut() -> (u64, bool, bool),
        held: impl Fn() -> bool,
    ) -> SessionWait {
        let budget = self.budget();
        if budget.is_zero() {
            return SessionWait::Disabled;
        }
        let started = Instant::now();
        let mut replay_blocked = false;
        let mut durability_blocked = false;
        let mut slept = false;
        let result = loop {
            let (seq, replay_touches, durability_pending) = refresh();
            replay_blocked |= replay_touches;
            durability_blocked |= durability_pending;
            let replay_touches = replay_touches || durability_pending;
            {
                let mut g = self.inner.lock().unwrap();
                if seq > g.applied_seq {
                    g.applied_seq = seq;
                }
            }
            if let Some(ok) = self.check(keys, replay_touches, floor) {
                break if slept {
                    SessionWait::Waited(started.elapsed())
                } else {
                    ok
                };
            }
            // A watermark past its TTL is dropped here, and the check
            // repeats (the read then waits, if at all, for its own floor).
            if self.abandon_stale_watermark() {
                continue;
            }
            let waited = started.elapsed();
            if waited >= budget {
                break SessionWait::TimedOut(waited);
            }
            let slice = (budget - waited).min(Duration::from_millis(20));
            let g = self.inner.lock().unwrap();
            let _ = self.cv.wait_timeout(g, slice).unwrap();
            slept = true;
        };
        let mut s = self.stats.lock().unwrap();
        s.reads += 1;
        if replay_blocked {
            s.replay_blocked += 1;
        }
        if durability_blocked {
            s.durability_blocked += 1;
        }
        match result {
            SessionWait::Fast => s.fast += 1,
            SessionWait::Covered => s.covered += 1,
            SessionWait::Waited(d) => {
                s.waited += 1;
                s.waits_ms[bucket(d)] += 1;
                s.wait_ms_total += d.as_millis() as u64;
            }
            SessionWait::TimedOut(d) => {
                s.timeouts += 1;
                s.waits_ms[bucket(d)] += 1;
                s.wait_ms_total += d.as_millis() as u64;
                let held = held();
                if held {
                    s.degraded_held += 1;
                }
                drop(s);
                if !self.warned.swap(true, Ordering::Relaxed) {
                    let g = self.inner.lock().unwrap();
                    tracing::warn!(
                        ?keys,
                        observed = ?g.observed,
                        applied = ?g.applied_position(),
                        held,
                        "session wait timed out; answering from the local replica \
                         (degraded, not an error; later timeouts are only counted)"
                    );
                }
            }
            SessionWait::Disabled => {}
        }
        result
    }
}

// ------------------------------------------------------------ Meta API

impl crate::store::Meta {
    /// This node's session state (plan 30 §M6).
    pub fn session(&self) -> &SessionState {
        &self.session
    }

    /// The FUSE read paths' session wait for `keys` (see the module doc).
    /// Never fails: a store error only skips the replay check.
    pub fn session_wait(&self, keys: &[ReadKey]) -> SessionWait {
        self.session_wait_at(keys, &Position::ZERO)
    }

    /// [`Self::session_wait`] with a per-read position `floor` on top of
    /// the watermark (plan 30 §M8's strict open).
    pub fn session_wait_at(&self, keys: &[ReadKey], floor: &Position) -> SessionWait {
        self.void_ended_generations();
        self.session.wait(
            keys,
            floor,
            || {
                let seq = self.applied_seq().unwrap_or(0);
                (
                    seq,
                    self.replay_touches(keys),
                    self.durability_pending(keys),
                )
            },
            || self.held_any.load(Ordering::Relaxed),
        )
    }

    /// Plan 30 §M9: this holder acknowledges only what is durable on its
    /// backups (or in the log), and its own reads observe nothing less:
    /// `true` while a row of the unshipped journal that touched `keys` is
    /// past the durable watermark — the rows the read would observe, not
    /// every row (an unrelated client's row in flight to the backup does
    /// not hold this read). Rows journaled while the gate was off are
    /// untracked and compared by the journal tip, as before. One atomic
    /// load when the gate is off.
    pub fn durability_pending(&self, keys: &[ReadKey]) -> bool {
        if !self.session.durable_gated() {
            return false;
        }
        let need = match self.unshipped_seq_for(keys) {
            None => return false,
            Some(crate::store::UNTRACKED) => self.journal_tip().unwrap_or(0),
            Some(seq) => seq,
        };
        need > self.session.durable_jseq()
    }

    /// The module doc's "A watermark nobody reaches": a stream generation
    /// the watermark depends on that this replica neither holds nor knows
    /// as void is looked up in the persisted delegation table (one point
    /// read, only when such a dependency is outstanding — never on the
    /// fast path): once delegated and no longer live, it has ended, and
    /// the dependency is void, as a `DelegateNodeEnded` applied in this incarnation
    /// would have made it.
    fn void_ended_generations(&self) {
        self.void_ended_among(self.session.unreached_streams());
    }

    /// The same for the generations `deps` names, called when a check of
    /// `deps` against the session says no ([`Self::reaches_deps`],
    /// [`Self::reaches_streams_deps`]): a generation the table ended
    /// before this incarnation began and this replica held no row or end
    /// of has no persisted key, so the restart's seeding left it alone
    /// (ADR-43: numbers are sparse, there is no range to void eagerly), and
    /// a client's dependency on it would wait for the rest of the process.
    /// Slow path only, and only for those generations
    /// ([`SessionState::ended_before_open`]): an end this incarnation saw
    /// (a streamed-ahead install of a `DelegateNodeEnded` the successor may strand)
    /// is the durable log's to void, not a check's. The cut is what the
    /// session holds of the generation (0 after a restart), as the seeding
    /// gave it.
    fn void_ended_deps(&self, deps: &Position) {
        let gens: Vec<u64> = self
            .session
            .unreached_deps(deps)
            .into_iter()
            .filter(|gen| self.session.ended_before_open(*gen))
            .collect();
        self.void_ended_among(gens);
    }

    fn void_ended_among(&self, gens: Vec<u64>) {
        if gens.is_empty() {
            return;
        }
        // ADR-43: an end only outstanding speculation carries is the
        // durable log's to void (`Meta::speculatively_ended_generations`),
        // read in the snapshot the table is read in.
        let (table, speculative) = {
            let r = self.db.read_tx();
            let table = crate::delegation::read_table_tx(&r, self).unwrap_or_default();
            // An unreadable set is not an empty one: voiding on it would void
            // the speculative ends it exists to keep live. Void nothing; the
            // next read tries again.
            let speculative = match self.speculatively_ended_generations_at(&r) {
                Ok(set) => set,
                Err(e) => {
                    tracing::warn!(error = %e, "could not read the speculatively ended generations; voiding nothing");
                    return;
                }
            };
            (table, speculative)
        };
        let max_gen = table.max_gen();
        let voided = self.session.void_ended_among(gens, |gen| {
            gen <= max_gen && !table.has_gen(gen) && !speculative.contains(&gen)
        });
        if voided > 0 {
            tracing::info!(
                voided,
                max_gen = %crate::delegation::GenLabel::of(max_gen),
                "voided session dependencies on delegation generations that ended before this \
                 incarnation (the delegation table no longer lists them)"
            );
        }
    }

    /// [`SessionState::reaches`] for `deps`, voiding the generations it
    /// names that the table ended (see [`Self::void_ended_deps`]) when the
    /// session says no.
    pub fn reaches_deps(&self, deps: &Position) -> bool {
        if self.session.reaches(deps) {
            return true;
        }
        self.void_ended_deps(deps);
        self.session.reaches(deps)
    }

    /// [`SessionState::reaches_streams`] for `deps`, as [`Self::reaches_deps`].
    pub fn reaches_streams_deps(&self, deps: &Position) -> bool {
        if self.session.reaches_streams(deps) {
            return true;
        }
        self.void_ended_deps(deps);
        self.session.reaches_streams(deps)
    }

    /// [`SessionState::deps_lost`] for `deps`, after the same lazy void
    /// ([`Self::void_ended_deps`]). The executors ask `deps_lost` first and
    /// `reaches_streams` after it (`holder.rs` `mutate_request`, the
    /// delegate stream's append and drain): were only the second to void,
    /// the first arrival of an op whose dependency is past the cut of a
    /// generation that ended before the open would pass `deps_lost`, be
    /// reached by the void, and execute ahead of its cause, where the
    /// eager seeding refused it.
    pub fn deps_lost_deps(&self, deps: &Position) -> bool {
        self.void_ended_deps(deps);
        self.session.deps_lost(deps)
    }

    /// [`SessionState::ready`] against this store: whether a read of
    /// `keys` may run right now without waiting.
    pub fn session_ready(&self, keys: &[ReadKey]) -> bool {
        self.session_ready_at(keys, &Position::ZERO)
    }

    /// [`Self::session_ready`] with a per-read position floor.
    pub fn session_ready_at(&self, keys: &[ReadKey], floor: &Position) -> bool {
        let seq = self.applied_seq().unwrap_or(0);
        self.session.ready(
            keys,
            seq,
            self.replay_touches(keys) || self.durability_pending(keys),
            floor,
        )
    }

    /// Whether a queued replay (a stranded op rolled back until it lands)
    /// touches any of `keys`, or a member copy row does (a committed row
    /// rolled back until the successor's re-ship lands:
    /// [`Meta::member_copy_touches`]; bounded by the session wait, after which
    /// the read degrades and shows the gap, and by the entry's own cap,
    /// `HELD_COPY_MAX_AGE_MS`). One counter read and one atomic load when
    /// neither is held.
    pub fn replay_touches(&self, keys: &[ReadKey]) -> bool {
        if self.member_copy_touches(keys) {
            return true;
        }
        if !self.has_pending_replays() {
            return false;
        }
        self.pending_replays().is_ok_and(|queue| {
            queue.iter().any(|q| {
                // A placeholder (plan 46 M0.2) has no op: the keys of the
                // row it stands for, which its requester's retry will
                // write again.
                let ks = match &q.await_retry {
                    Some(a) => KeySet::from_touch_set(&a.keys),
                    None => KeySet::from_op(&q.op),
                };
                keys.iter().any(|k| ks.touches(k))
            })
        })
    }

    /// The acked watermark a ship of journal rows `seqs` will leave
    /// (`store::journal::ack_rows_at`'s rule, read-only: just below the
    /// oldest row left, or every seq handed out when none is;
    /// `journal::watermark_after`). What a segment's `through` says.
    pub fn journal_through_after(&self, seqs: &[u64]) -> Result<u64, crate::MetaError> {
        let r = self.db.read_tx();
        if seqs.is_empty() {
            return crate::store::journal::acked_watermark(&r, &self.local);
        }
        let shipping: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        crate::store::journal::watermark_after(&r, &self.journal_ks, &self.local, |s| {
            shipping.contains(&s)
        })
    }

    /// The journal position a holder at `epoch` evaluates against right
    /// now: its shipped-through log sequence is the caller's, this is the
    /// unshipped part (`None` when every row has shipped).
    pub fn journal_position(&self, epoch: u64) -> Option<JournalPos> {
        // The highest row still in the journal, not `next − 1`: a
        // stranding deletes rows without moving the acked watermark, and
        // a position past every remaining row would wait for a ship that
        // has nothing to ship (plan 30 §M9's durability parks found it).
        let r = self.db.read_tx();
        let last = crate::store::journal::max_seq(&r, &self.journal_ks, &self.local).ok()?;
        let acked = self.journal_acked_seq().ok()?;
        (last > acked).then_some(JournalPos { epoch, jseq: last })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jp(epoch: u64, jseq: u64) -> Option<JournalPos> {
        Some(JournalPos { epoch, jseq })
    }

    fn at(seq: u64, epoch: u64, jseq: u64) -> Position {
        Position {
            seq,
            pending: jp(epoch, jseq),
            streams: Streams::NONE,
        }
    }

    /// Plan 43 C2d: the delegate fast path's wait follows the commit point
    /// the core publishes per generation: a covered row is durable (whatever
    /// became of the delegation since), an uncovered one waits, and a
    /// generation lost, withdrawn or never published is in doubt; a raise
    /// wakes a waiter.
    #[test]
    fn a_delegates_wait_follows_the_published_commit_point() {
        let s = std::sync::Arc::new(SessionState::default());
        let published = |commit, through, lost| DelegDurable {
            commit,
            through,
            lost,
        };
        let wait = |gen, idx| s.wait_deleg_durable(gen, idx, Duration::ZERO);
        assert_eq!(wait(7, 1), DurableWait::Lost, "never published");
        assert_eq!(s.deleg_row_class(7), None);
        s.set_deleg_durable(7, Some(published(RowCommit::DELEG_GROUP, 0, false)));
        assert_eq!(s.deleg_row_class(7), Some(RowCommit::DELEG_GROUP));
        assert_eq!(wait(7, 1), DurableWait::TimedOut);
        assert_eq!(wait(8, 1), DurableWait::Lost, "another generation");
        // A waiter is woken by the raise (and not by waiting out its budget).
        let waiter = {
            let s = s.clone();
            std::thread::spawn(move || s.wait_deleg_durable(7, 2, Duration::from_secs(30)))
        };
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        s.set_deleg_durable(7, Some(published(RowCommit::DELEG_GROUP, 2, false)));
        assert!(matches!(waiter.join().unwrap(), DurableWait::Durable(_)));
        assert!(started.elapsed() < Duration::from_secs(5));
        // Committed stays committed when the delegation is lost; the rest
        // is in doubt.
        s.set_deleg_durable(7, Some(published(RowCommit::DELEG_GROUP, 2, true)));
        assert!(matches!(wait(7, 2), DurableWait::Durable(_)));
        assert_eq!(wait(7, 3), DurableWait::Lost);
        // Withdrawn while a waiter sleeps: it is told, not left to its budget.
        s.set_deleg_durable(7, Some(published(RowCommit::DELEG_GROUP, 2, false)));
        let waiter = {
            let s = s.clone();
            std::thread::spawn(move || s.wait_deleg_durable(7, 3, Duration::from_secs(30)))
        };
        std::thread::sleep(Duration::from_millis(50));
        s.set_deleg_durable(7, None);
        assert_eq!(waiter.join().unwrap(), DurableWait::Lost);
        // What the group covered before the withdrawal stays durable; the
        // rest of the withdrawn generation is in doubt.
        assert!(matches!(wait(7, 2), DurableWait::Durable(_)));
        assert_eq!(wait(7, 3), DurableWait::Lost);
    }

    /// The tombstones of withdrawn generations are bounded: the oldest goes
    /// first, a generation that covered nothing leaves none, a held one is
    /// never dropped.
    #[test]
    fn withdrawn_generations_keep_a_bounded_high_water() {
        let s = SessionState::default();
        let published = |through| DelegDurable {
            commit: RowCommit::DELEG_GROUP,
            through,
            lost: false,
        };
        let wait = |gen, idx| s.wait_deleg_durable(gen, idx, Duration::ZERO);
        // A held generation, and one that covered nothing before its withdrawal.
        s.set_deleg_durable(1000, Some(published(1)));
        s.set_deleg_durable(999, Some(published(0)));
        s.set_deleg_durable(999, None);
        assert_eq!(wait(999, 1), DurableWait::Lost);
        let total = WITHDRAWN_COVERED_CAP as u64 + 4;
        for gen in 1..=total {
            s.set_deleg_durable(gen, Some(published(5)));
            s.set_deleg_durable(gen, None);
        }
        assert_eq!(wait(1, 5), DurableWait::Lost, "the oldest was dropped");
        assert!(matches!(wait(total, 5), DurableWait::Durable(_)));
        assert!(matches!(wait(1000, 1), DurableWait::Durable(_)));
        assert!(s.inner.lock().unwrap().deleg_covered.len() <= WITHDRAWN_COVERED_CAP + 1);
    }

    /// Review claim 3 (fail-before): a row the published watermark covers
    /// is durable "whatever became of the delegation since"
    /// (`wait_deleg_durable`'s doc), the withdrawal of the generation
    /// included. `a_delegates_wait_follows_the_published_commit_point` pins
    /// the opposite (`Lost` for a covered idx after the withdrawal).
    #[test]
    fn a_covered_row_stays_durable_after_the_generation_is_withdrawn() {
        let s = SessionState::default();
        s.set_deleg_durable(
            7,
            Some(DelegDurable {
                commit: RowCommit::DELEG_GROUP,
                through: 2,
                lost: false,
            }),
        );
        s.set_deleg_durable(7, None);
        let got = s.wait_deleg_durable(7, 1, Duration::ZERO);
        assert!(
            matches!(got, DurableWait::Durable(_)),
            "idx 1 was covered (through 2) before the generation was withdrawn: {got:?}"
        );
    }

    /// Plan 46 M1.1 step 4: the fast path stamps the rule the core published
    /// and nothing else, as long as the version it was decided under is the
    /// one the store records under the writer lock; a different one (a
    /// `DelegateNodeGroup` record applied since) is no rule to stamp: the op goes
    /// through the core. A rule that names no applied version (the archive's,
    /// a designation's) is stamped as published.
    #[test]
    fn the_published_rule_is_stamped_only_at_the_version_the_store_records() {
        let s = SessionState::default();
        let publish = |commit| {
            s.set_deleg_durable(
                7,
                Some(DelegDurable {
                    commit,
                    through: 10,
                    lost: false,
                }),
            );
        };
        assert_eq!(s.deleg_commit_at_journal(7, 0), None, "not published");
        let group = |v| RowCommit::group(v, crate::store::local::GroupOn::Delegate);
        publish(group(3));
        assert_eq!(s.deleg_commit_at_journal(7, 3), Some(group(3)));
        assert_eq!(
            s.deleg_commit_at_journal(7, 4),
            None,
            "a record applied since"
        );
        assert_eq!(s.deleg_commit_at_journal(7, 2), None, "the store is behind");
        publish(RowCommit::FINAL);
        assert_eq!(s.deleg_commit_at_journal(7, 9), Some(RowCommit::FINAL));
        publish(RowCommit::ARCHIVE);
        assert_eq!(s.deleg_commit_at_journal(7, 9), Some(RowCommit::ARCHIVE));
    }

    /// Plan 43 C1-2: the class the core publishes with the gate is what the
    /// fast path's rows are journaled with and its wait follows; a lone node
    /// publishes `Lone`.
    #[test]
    fn the_session_publishes_the_class_with_the_gate() {
        let s = SessionState::default();
        assert_eq!(s.published_class(), RowCommit::LONE, "nothing published");
        s.set_durable(true, 7, false, RowCommit::ROOT_GROUP, false);
        assert_eq!(s.published_class(), RowCommit::ROOT_GROUP);
        assert!(s.durable_gated());
        s.set_durable(true, 7, false, RowCommit::ARCHIVE, false);
        assert_eq!(s.published_class(), RowCommit::ARCHIVE);
        // The gate goes off: the class the core publishes with it is the
        // lone node's, and a waiter finds the gate off.
        s.set_durable(false, u64::MAX, false, RowCommit::LONE, true);
        assert_eq!(s.published_class(), RowCommit::LONE);
        assert_eq!(
            s.wait_durable(9, Duration::from_millis(1)),
            DurableWait::Ungated
        );
    }

    /// Plan 43 C2 (§5.4, D5): with the gate off, a fast-path wait is
    /// answered on this disk only where the core said the node may (lone,
    /// or an epoch's hold: at once, never a wait for the archive); the gate
    /// off anywhere else (a release or a handoff after the write) is in
    /// doubt, and a lost lease stays `Lost`. Fails on the parent: every gate
    /// off with the lease kept was `Ungated`.
    #[test]
    fn an_ungated_wait_is_answered_only_where_the_node_may_answer_alone() {
        let s = SessionState::default();
        s.set_durable(true, 3, false, RowCommit::ROOT_GROUP, false);
        // An epoch's hold: the gate is off and the node may answer — at
        // once, whatever the budget.
        s.set_durable(false, u64::MAX, false, RowCommit::FINAL, true);
        let started = Instant::now();
        assert_eq!(
            s.wait_durable(9, Duration::from_secs(30)),
            DurableWait::Ungated
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        // Released (or handed off): the gate is off, the node not lone.
        s.set_durable(false, u64::MAX, false, RowCommit::LONE, false);
        assert_eq!(
            s.wait_durable(9, Duration::from_secs(30)),
            DurableWait::Unexpected
        );
        // Lost wins.
        s.set_durable(false, u64::MAX, true, RowCommit::LONE, false);
        assert_eq!(
            s.wait_durable(9, Duration::from_millis(1)),
            DurableWait::Lost
        );
    }

    /// Chunk metered-own-rows: this node's own close at rows 12–13 is
    /// deferred on the holder (its chunk held here on a metered network),
    /// so every segment's `through` stops at 11; rows 14–18 ship ahead of
    /// it. A refusal observed the holder at 18. With 12–13 excused (the
    /// close's effect is here), the watermark is reached once 14–18 are
    /// applied, not before; without the excuse, only once 12–13 ship.
    #[test]
    fn an_observation_excusing_own_rows_is_reached_by_the_rows_shipped_around_them() {
        let s = SessionState::default();
        s.advance_rows(5, jp(1, 11), &[10, 11]);
        s.raise_observed_excusing(at(5, 1, 18), &[(12, 13)]);
        assert!(!s.reaches_observed(), "14-18 not applied yet");
        s.advance_rows(6, jp(1, 11), &[14, 15]);
        assert!(!s.reaches_observed(), "16-18 missing");
        s.advance_rows(7, jp(1, 11), &[16, 17, 18]);
        assert!(s.reaches_observed(), "everything but this node's own rows");
        // A plain observation of the same position owes 12–13 again.
        s.raise_observed(at(7, 1, 18));
        assert!(!s.reaches_observed(), "12-13 are owed now");
        s.advance_rows(8, jp(1, 18), &[12, 13]);
        assert!(s.reaches_observed());
    }

    /// The join: a row stays excused only while every observation
    /// covering it excuses it; one past the earlier position keeps the
    /// later excuse; a newer tenure's position drops the older excuses.
    #[test]
    fn excused_rows_join_per_covering_observation() {
        let s = SessionState::default();
        s.advance_rows(5, jp(1, 11), &[]);
        s.raise_observed_excusing(at(5, 1, 14), &[(12, 13)]);
        s.raise_observed_excusing(at(5, 1, 20), &[(12, 13), (19, 20)]);
        s.advance_rows(6, jp(1, 11), &[14, 15, 16, 17, 18]);
        assert!(s.reaches_observed(), "12-13 and 19-20 excused by both");
        // An observation at 16 that excuses nothing owes 12-13; 19-20
        // lie past it and stay excused.
        s.raise_observed(at(6, 1, 16));
        assert!(!s.reaches_observed());
        s.advance_rows(7, jp(1, 18), &[12, 13]);
        assert!(s.reaches_observed(), "19-20 still excused");
        // Another tenure: its position is what counts.
        s.raise_observed_excusing(at(7, 2, 3), &[(1, 3)]);
        assert!(!s.reaches_observed(), "nothing of tenure 2 applied");
        s.advance_rows(8, jp(2, 0), &[]);
        assert!(s.reaches_observed(), "tenure 2's rows 1-3 are all excused");
    }

    /// Rows applied out of order are kept as ranges and forgotten once
    /// `through` passes them; a newer tenure starts afresh.
    #[test]
    fn rows_ahead_of_through_are_kept_as_ranges() {
        let mut r = RowRanges::default();
        for row in [5, 7, 6, 9, 10, 3] {
            r.insert(row);
        }
        assert_eq!(
            r.0.iter().map(|(a, b)| (*a, *b)).collect::<Vec<_>>(),
            vec![(3, 3), (5, 7), (9, 10)]
        );
        assert_eq!(r.end_covering(6), Some(7));
        assert_eq!(r.end_covering(8), None);
        r.prune_through(6);
        assert_eq!(
            r.0.iter().map(|(a, b)| (*a, *b)).collect::<Vec<_>>(),
            vec![(7, 7), (9, 10)]
        );
        r.insert(8);
        assert_eq!(
            r.0.iter().map(|(a, b)| (*a, *b)).collect::<Vec<_>>(),
            vec![(7, 10)]
        );
    }

    /// EC2 campaign 7 (`git-under-flock-causal`, reader `b` after its
    /// remount): a lock grant's floor named generation 1 at index 816,
    /// ended before the restart; the fresh session state knew neither
    /// the generation nor its end, so every read waited the budget. A
    /// seeded state reaches the floor at once (the generation is void),
    /// still holds live generation 2 at what the log has of it, and a
    /// dependency past the ended generation's cut is lost.
    #[test]
    fn a_reopened_session_reaches_a_floor_on_a_generation_that_ended_before_the_restart() {
        let floor = |g: u64, i: u64| {
            let mut streams = Streams::NONE;
            assert!(streams.raise(g, i));
            Position {
                seq: 0,
                pending: None,
                streams,
            }
        };
        let s = SessionState::default();
        assert!(!s.reaches(&floor(1, 816)), "unknown before the seed");
        let live: std::collections::BTreeSet<u64> = [2].into_iter().collect();
        let log_idx = |g: u64| match g {
            1 => 816,
            2 => 370,
            _ => 0,
        };
        s.seed_generations(2, &[1, 2].into(), &live, &log_idx, &|_| None);
        assert!(s.reaches(&floor(1, 816)), "the ended generation is void");
        assert!(s.reaches(&floor(1, 900)), "void past its cut too");
        assert!(
            s.deps_lost(&floor(1, 900)),
            "but a write's dependency past the cut is lost"
        );
        assert!(!s.deps_lost(&floor(1, 816)));
        assert!(
            s.reaches(&floor(2, 370)),
            "the live generation is held through the log's index"
        );
        assert!(!s.reaches(&floor(2, 371)), "and no further");
    }

    /// ADR-43: generation numbers are sparse (`epoch << 32 | n`): the seeding
    /// walks the numbers the store keeps, not a range up to the highest, and
    /// a number of epoch 6 does not make five billion of epoch 5's.
    #[test]
    fn a_reopened_session_seeds_sparse_generations() {
        let floor = |g: u64, i: u64| {
            let mut streams = Streams::NONE;
            assert!(streams.raise(g, i));
            Position {
                seq: 0,
                pending: None,
                streams,
            }
        };
        let (old, new) = ((5u64 << 32) | 3, (6u64 << 32) | 1);
        let live: std::collections::BTreeSet<u64> = [new].into_iter().collect();
        let log_idx = |g: u64| match g {
            g if g == old => 7,
            g if g == new => 2,
            _ => 0,
        };
        let s = SessionState::default();
        s.seed_generations(new, &[old, new].into(), &live, &log_idx, &|g| {
            (g == old).then_some(6)
        });
        // The ended one is void at the record's cut (6), not at the log's (7).
        assert!(s.is_voided(old));
        assert!(!s.deps_lost(&floor(old, 6)));
        assert!(s.deps_lost(&floor(old, 7)));
        assert!(s.reaches(&floor(old, 7)), "void: reached");
        // The live one is held through the log's index of it, no further.
        assert!(!s.is_voided(new));
        assert!(s.reaches(&floor(new, 2)));
        assert!(!s.reaches(&floor(new, 3)));
        // A number nobody keeps is not touched.
        assert!(!s.is_voided(old - 1));
        assert!(!s.reaches(&floor(old - 1, 1)));
    }

    /// ADR-43 (review S-3): a key above the table's highest generation (a
    /// deposed root's stranded appends leave `deleg_log_idx` behind while
    /// the strand rolls the table back below it) is held, not voided: the
    /// table has not reached the number, so it has not ended, and the log
    /// may yet carry its grant and rows.
    #[test]
    fn a_key_above_the_tables_highest_generation_is_held_not_voided() {
        let floor = |g: u64, i: u64| {
            let mut streams = Streams::NONE;
            assert!(streams.raise(g, i));
            Position {
                seq: 0,
                pending: None,
                streams,
            }
        };
        let (ended, stranded) = ((5u64 << 32) | 3, (7u64 << 32) | 1);
        let log_idx = |g: u64| if g == stranded { 4 } else { 0 };
        let s = SessionState::default();
        s.seed_generations(
            ended,
            &[ended, stranded].into(),
            &Default::default(),
            &log_idx,
            &|_| None,
        );
        assert!(s.is_voided(ended));
        assert!(!s.is_voided(stranded), "above the table's highest");
        assert!(s.reaches(&floor(stranded, 4)), "held through its key");
        assert!(!s.reaches(&floor(stranded, 5)), "and no further");
        assert!(!s.deps_lost(&floor(stranded, 5)));
    }

    /// ADR-43 (review M2): only a generation that ended before this
    /// incarnation began may be voided lazily by a check; one the table
    /// lists at open, one above its highest, and one that ends later (a
    /// streamed-ahead end may strand) may not.
    #[test]
    fn only_a_generation_that_ended_before_the_open_is_voided_lazily() {
        let s = SessionState::default();
        assert!(!s.ended_before_open(1), "nothing seeded: nothing ended");
        let (ended, live, ahead) = ((5u64 << 32) | 3, (6u64 << 32) | 1, (6u64 << 32) | 2);
        s.seed_ended_before_open(live, &[live].into());
        assert!(s.ended_before_open(ended));
        assert!(s.ended_before_open(1));
        assert!(!s.ended_before_open(live));
        assert!(!s.ended_before_open(ahead));
    }

    /// Plan 43 C2: the cut of a generation that ended before the restart is
    /// the one its `DelegateNodeEnded` record carried (persisted when the record
    /// was applied), not what the log happens to hold of it — a log
    /// written before the record has only the latter.
    #[test]
    fn a_reopened_session_takes_the_cut_from_the_record_when_it_has_one() {
        let floor = |g: u64, i: u64| {
            let mut streams = Streams::NONE;
            assert!(streams.raise(g, i));
            Position {
                seq: 0,
                pending: None,
                streams,
            }
        };
        let live = std::collections::BTreeSet::new();
        let log_idx = |g: u64| if g == 1 { 816 } else { 0 };
        // The record said the stream was cut at 800; the log holds 816
        // (rows past the cut that were applied before the record's own
        // segment) — the record's cut wins.
        let s = SessionState::default();
        s.seed_generations(1, &[1].into(), &live, &log_idx, &|g| {
            (g == 1).then_some(800)
        });
        assert_eq!(s.void_cut(1), Some(800));
        assert!(!s.deps_lost(&floor(1, 800)));
        assert!(s.deps_lost(&floor(1, 801)));
        // No record cut (an orphaned grant's end): the log's index.
        let s = SessionState::default();
        s.seed_generations(1, &[1].into(), &live, &log_idx, &|_| None);
        assert_eq!(s.void_cut(1), Some(816));
        assert!(s.deps_lost(&floor(1, 817)));
        assert_eq!(s.void_cut(2), None, "not voided here");
    }

    /// Plan 30 §M9 × §M6 (backup seed 607661, backup-crash-slow 600066):
    /// a refusal observed the old holder's acknowledged, unshipped state
    /// at `(1, 53)`; the sealed backup took over and its marker announced
    /// the tail it re-ships (`TailFollows`). The marker alone does not
    /// reach the observation; the successor's next segment does. A TTL
    /// takeover's marker (no announcement) still ends the wait, as M6
    /// wants: that tenure's unshipped work is not coming back.
    #[test]
    fn an_announced_tail_keeps_older_observations_waiting_past_the_marker() {
        let s = SessionState::default();
        let observed = Position {
            seq: 18,
            pending: jp(1, 53),
            streams: Default::default(),
        };
        s.owe(JournalPos { epoch: 2, jseq: 4 });
        s.advance(19, jp(2, 4));
        assert!(!s.reaches(&observed), "reached at the marker");
        s.advance(20, jp(2, 9));
        assert!(s.reaches(&observed), "the tail landed");
        // Without the announcement the marker ends the wait.
        let t = SessionState::default();
        t.advance(19, jp(2, 4));
        assert!(t.reaches(&observed));
        // A position of the new tenure itself is unaffected.
        let s = SessionState::default();
        s.owe(JournalPos { epoch: 2, jseq: 4 });
        s.advance(19, jp(2, 4));
        assert!(s.reaches(&Position {
            seq: 19,
            pending: jp(2, 4),
            streams: Default::default(),
        }));
    }

    /// A dependency on the holder's journal is reached through the pre-S3
    /// stream's watermark as through the applied log; clearing it (a
    /// strand) leaves only the applied log; the `owed` rule still holds.
    #[test]
    fn the_streamed_watermark_reaches_a_journal_dependency_until_cleared() {
        let s = SessionState::default();
        s.advance(10, jp(1, 40));
        let deps = Position {
            seq: 10,
            pending: jp(1, 45),
            streams: Default::default(),
        };
        assert!(!s.reaches(&deps));
        s.note_streamed(JournalPos { epoch: 1, jseq: 44 });
        assert!(!s.reaches(&deps), "short of the dependency");
        s.note_streamed(JournalPos { epoch: 1, jseq: 45 });
        assert!(s.reaches(&deps));
        // Never lowered by an older report.
        s.note_streamed(JournalPos { epoch: 1, jseq: 41 });
        assert!(s.reaches(&deps));
        s.clear_streamed();
        assert!(!s.reaches(&deps), "a strand leaves the applied log only");
        // The applied log's seq is still required.
        s.note_streamed(JournalPos { epoch: 1, jseq: 50 });
        assert!(!s.reaches(&Position { seq: 11, ..deps }));
        // An announced tail is not reached through the old tenure's stream.
        let t = SessionState::default();
        t.owe(JournalPos { epoch: 2, jseq: 4 });
        t.advance(19, jp(2, 4));
        t.note_streamed(JournalPos { epoch: 1, jseq: 60 });
        assert!(!t.reaches(&Position {
            seq: 18,
            pending: jp(1, 53),
            streams: Default::default(),
        }));
    }

    #[test]
    fn positions_order_epoch_first_and_join_componentwise() {
        let a = Position {
            seq: 5,
            pending: jp(1, 100),
            streams: Default::default(),
        };
        let b = Position {
            seq: 6,
            pending: jp(2, 3),
            streams: Default::default(),
        };
        assert!(b.dominates(&a));
        assert!(!a.dominates(&b));
        let c = Position {
            seq: 9,
            pending: None,
            streams: Default::default(),
        };
        assert!(!c.dominates(&a), "an unshipped row is not in any seq");
        assert_eq!(
            a.join(&c),
            Position {
                seq: 9,
                pending: jp(1, 100),
                streams: Default::default()
            }
        );
        assert_eq!(a.hint_floor(), 6);
        assert_eq!(c.hint_floor(), 9);
    }

    /// A lock grant's floor naming streams the observed watermark does
    /// not, past the cap together: the floor must still be waited for,
    /// and a shadow covering the key at the observed watermark alone must
    /// not answer (sim `locks-delegated-writes` seed 200981: the join
    /// dropped the floor's streams and a covered read returned the old
    /// turn under the new holder's lock).
    #[test]
    fn a_floor_past_the_streams_cap_is_still_waited_for() {
        let s = SessionState::default();
        let mut observed = Position::ZERO;
        for g in 1..=6 {
            assert!(observed.streams.raise(g, 1));
        }
        s.raise_observed(observed);
        let mut floor = Position::ZERO;
        for g in 7..=14 {
            assert!(floor.streams.raise(g, 1));
        }
        let k = [ReadKey::Ino(9)];
        // A shadow of an older write of the key, at the observed state.
        s.note_covering(
            KeySet {
                dentries: Vec::new(),
                inos: vec![9],
            },
            observed,
        );
        assert!(!s.ready(&k, 0, false, &floor), "the floor was dropped");
        // Once the replica has what is owed, it is ready.
        for g in 1..=14 {
            s.note_stream(g, 1);
        }
        assert!(s.ready(&k, 0, false, &floor));
    }

    /// Review of chunk metered-own-rows: `observed` keeps a stream entry
    /// (a delegate's refusal) the replica has since reached, and a root
    /// refusal raised its pending part past the applied rows. A shadow
    /// from a root reply (its position names no streams) at a later
    /// position covers the key: the stream it does not name is reached
    /// and owed no more, so the read is covered, not made to wait.
    #[test]
    fn a_covered_read_ignores_streams_the_replica_already_reaches() {
        let s = SessionState::default();
        let mut delegated = Position::ZERO;
        assert!(delegated.streams.raise(4, 3));
        s.raise_observed(delegated);
        s.raise_observed(at(2, 1, 7));
        s.note_stream(4, 3);
        let k = [ReadKey::Ino(9)];
        s.note_covering(
            KeySet {
                dentries: Vec::new(),
                inos: vec![9],
            },
            at(2, 1, 8),
        );
        assert!(s.ready(&k, 2, false, &Position::ZERO), "covered");
        // An uncovered key still waits for the pending rows.
        assert!(!s.ready(&[ReadKey::Ino(10)], 2, false, &Position::ZERO));
    }

    #[test]
    fn fast_path_covered_and_timeout() {
        let s = SessionState::default();
        s.set_budget_ms(30);
        let k = [ReadKey::Dentry(1, "a".into())];
        assert_eq!(
            s.wait(&k, &Position::ZERO, || (0, false, false), || false),
            SessionWait::Fast
        );
        let obs = Position {
            seq: 3,
            pending: jp(1, 7),
            streams: Default::default(),
        };
        s.raise_observed(obs);
        // Covered by a shadow at least as new.
        s.note_covering(
            KeySet {
                dentries: vec![(1, "a".into())],
                inos: vec![1],
            },
            Position {
                seq: 3,
                pending: jp(1, 8),
                streams: Default::default(),
            },
        );
        assert_eq!(
            s.wait(&k, &Position::ZERO, || (3, false, false), || false),
            SessionWait::Covered
        );
        // The directory is not covered: it waits, then times out.
        assert!(matches!(
            s.wait(
                &[ReadKey::Dir(1)],
                &Position::ZERO,
                || (3, false, false),
                || true
            ),
            SessionWait::TimedOut(_)
        ));
        assert_eq!(s.stats().degraded_held, 1);
        // Applying the segment shipped through the position releases it.
        s.advance(4, jp(1, 9));
        assert_eq!(
            s.wait(
                &[ReadKey::Dir(1)],
                &Position::ZERO,
                || (4, false, false),
                || false
            ),
            SessionWait::Fast
        );
        // A queued replay touching the key blocks even the fast path.
        assert!(matches!(
            s.wait(&k, &Position::ZERO, || (4, true, false), || false),
            SessionWait::TimedOut(_)
        ));
    }

    /// EC2 campaign 7 B-2: a lock grant's floor named a delegation
    /// generation that ended before this incarnation (its `DelegateNodeEnded` was
    /// applied by an earlier one; `voided` is volatile). The persisted
    /// table says it ended: the dependency is voided and the read goes
    /// through at once, instead of every read paying the whole budget.
    #[test]
    fn a_dependency_on_a_generation_the_table_shows_ended_is_voided() {
        let s = SessionState::default();
        s.set_budget_ms(2_000);
        s.advance(10, jp(3, 7));
        let mut observed = Position {
            seq: 10,
            pending: jp(3, 7),
            streams: Default::default(),
        };
        assert!(observed.streams.raise(5, 1649));
        assert!(observed.streams.raise(6, 6990));
        s.raise_observed(observed);
        assert_eq!(s.unreached_streams(), vec![5, 6]);
        // Generation 6 is still live in the table: only 5 is voided.
        assert_eq!(s.void_ended(|gen| gen == 5), 1);
        assert_eq!(s.unreached_streams(), vec![6]);
        assert!(!s.reaches(&observed));
        assert_eq!(s.void_ended(|_| true), 1);
        assert!(s.unreached_streams().is_empty());
        let r = s.wait(
            &[ReadKey::Ino(1)],
            &Position::ZERO,
            || (10, false, false),
            || false,
        );
        assert_eq!(r, SessionWait::Fast);
        assert_eq!(s.stats().voided_ended, 2);
        assert_eq!(s.stats().timeouts, 0);
    }

    /// The safety net for everything else: a watermark unreached for
    /// the TTL is dropped to the applied position, once; the reads that
    /// timed out before that are counted, the ones after go through.
    #[test]
    fn a_watermark_nobody_reaches_is_dropped_after_its_ttl() {
        let s = SessionState::default();
        s.set_budget_ms(50);
        s.set_watermark_ttl_ms(120);
        s.advance(10, None);
        s.raise_observed(Position {
            seq: 10,
            pending: jp(2, 36),
            streams: Default::default(),
        });
        let read = || {
            s.wait(
                &[ReadKey::Ino(1)],
                &Position::ZERO,
                || (10, false, false),
                || false,
            )
        };
        assert!(matches!(read(), SessionWait::TimedOut(_)));
        assert_eq!(s.stats().abandoned, 0);
        std::thread::sleep(Duration::from_millis(130));
        // Past the TTL: dropped at the start of this wait, no timeout.
        assert_eq!(read(), SessionWait::Fast);
        assert_eq!(s.stats().abandoned, 1);
        assert_eq!(s.stats().timeouts, 1);
        assert_eq!(s.observed(), s.applied());
        // A watermark that is reached keeps nothing to drop.
        s.raise_observed(Position {
            seq: 10,
            pending: None,
            streams: Default::default(),
        });
        std::thread::sleep(Duration::from_millis(130));
        assert_eq!(read(), SessionWait::Fast);
        assert_eq!(s.stats().abandoned, 1);
        // TTL 0: kept for good (the old behaviour).
        s.set_watermark_ttl_ms(0);
        s.raise_observed(Position {
            seq: 10,
            pending: jp(2, 40),
            streams: Default::default(),
        });
        std::thread::sleep(Duration::from_millis(130));
        assert!(matches!(read(), SessionWait::TimedOut(_)));
        assert_eq!(s.stats().abandoned, 1);
    }

    #[test]
    fn a_waiter_is_released_by_advance() {
        // Generous, because it is only this test's patience: the
        // assertions below never consume it (see the handshake).
        const BUDGET: Duration = Duration::from_secs(10);
        let s = std::sync::Arc::new(SessionState::default());
        s.set_budget_ms(BUDGET.as_secs() * 1_000);
        // Never drop the watermark: that takes abandonment — the wait
        // loop's third way out — off the table, so the returned variant
        // alone says what released the waiter.
        s.set_watermark_ttl_ms(0);
        s.raise_observed(Position {
            seq: 2,
            pending: None,
            streams: Default::default(),
        });
        // `wait` calls `refresh` once per pass round the loop, so the
        // count is this test's window into the loop: `refresh` runs,
        // the check fails, the thread sleeps on the condvar (setting
        // `slept`), and only then does `refresh` run again. Seeing the
        // second call therefore means the waiter is genuinely parked
        // and any later success must come back as `Waited`, not `Fast`.
        // A plain sleep here instead raced: on a loaded host the main
        // thread's `advance` could land before the spawned thread's
        // first `refresh`, the first check then passed outright and the
        // wait returned `Fast` without ever having waited (2 of 200
        // runs at host load ~30).
        let passes = std::sync::Arc::new(AtomicU64::new(0));
        let s2 = s.clone();
        let p2 = passes.clone();
        let t = std::thread::spawn(move || {
            s2.wait(
                &[ReadKey::Ino(1)],
                &Position::ZERO,
                || {
                    p2.fetch_add(1, Ordering::Release);
                    (s2.applied().seq, false, false)
                },
                || false,
            )
        });
        let deadline = Instant::now() + BUDGET;
        while passes.load(Ordering::Acquire) < 2 {
            assert!(
                Instant::now() < deadline,
                "the waiter never parked: {} refresh passes",
                passes.load(Ordering::Acquire)
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        s.advance(2, None);
        let r = t.join().unwrap();
        // `check` can only pass once `applied_seq` reaches the observed
        // seq 2 (nothing covers the key: `covering` is empty), and only
        // `advance` raises it — so `Waited` *is* "released by the
        // advance". `TimedOut` would mean the advance never released it,
        // and `Fast`/`Covered` that the waiter never parked, both of
        // which the handshake and this assertion separate cleanly
        // without any wall-clock bound: the old `Waited(d) if d <
        // 1_000ms` form read a loaded host's scheduling delay as a
        // failure (seen once at host load ~50) while adding nothing —
        // the loop re-checks every 20 ms regardless of the notification,
        // so no duration this test could assert distinguishes being
        // woken by `advance` from polling just after it.
        assert!(matches!(r, SessionWait::Waited(_)), "{r:?}");
        let st = s.stats();
        assert_eq!(
            (st.waited, st.timeouts, st.abandoned),
            (1, 0, 0),
            "released by something other than the advance: {st:?}"
        );
    }

    #[test]
    fn keys_of_a_create_cover_its_own_keys_not_the_parent() {
        let recs = vec![LogRecord::Create {
            parent: 1,
            name: "a".into(),
            ino: 7,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }];
        let k = KeySet::from_records(&recs);
        assert!(k.covers(&ReadKey::Dentry(1, "a".into())));
        assert!(k.covers(&ReadKey::Ino(7)));
        assert!(
            !k.covers(&ReadKey::Ino(1)),
            "the parent is outside the per-key base (M5 round 3)"
        );
        assert!(!k.covers(&ReadKey::Dir(1)));
        assert!(k.touches(&ReadKey::Dir(1)));
        // A queued replay of the create blocks the parent's attributes.
        let op = MutateOp::Create {
            parent: 1,
            name: "a".into(),
            ino: 7,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let q = KeySet::from_op(&op);
        assert!(q.touches(&ReadKey::Ino(1)) && q.touches(&ReadKey::Ino(7)));
    }

    /// Plan 30 §M9, per row: a gated holder's read waits only while a
    /// row it would observe is past the durable watermark — not while
    /// some other key's row is (the coarse rule it replaces: any
    /// unshipped touch of the keys plus any non-durable row). Rows
    /// journaled with the gate off are untracked and fall back to the
    /// journal tip.
    #[test]
    fn a_gated_read_waits_only_for_the_rows_it_would_observe() {
        let meta = crate::store::Meta::open_in_memory().unwrap();
        meta.set_node_prefix(1).unwrap();
        let root = constellation_fs_core::types::ROOT_INO;
        let create = |name: &str| MutateOp::Create {
            parent: root,
            name: name.into(),
            ino: meta.allocate_ino(root).unwrap(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let dentry = |name: &str| [ReadKey::Dentry(root, name.into())];
        let s = meta.session();
        s.set_durable(true, 0, false, RowCommit::ROOT_GROUP, false);
        crate::mutate::execute(&meta, &create("a"), None).unwrap();
        assert!(meta.durability_pending(&dentry("a")));
        s.set_durable(
            true,
            meta.journal_tip().unwrap(),
            false,
            RowCommit::ROOT_GROUP,
            false,
        );
        crate::mutate::execute(&meta, &create("b"), None).unwrap();
        // `a` is durable although `b`'s row (a different key) is not.
        assert!(!meta.durability_pending(&dentry("a")));
        assert!(meta.durability_pending(&dentry("b")));
        assert!(!meta.durability_pending(&dentry("zzz")), "untouched");
        assert!(
            meta.durability_pending(&[ReadKey::Dir(root)]),
            "`ls` sees b"
        );
        s.set_durable(
            true,
            meta.journal_tip().unwrap(),
            false,
            RowCommit::ROOT_GROUP,
            false,
        );
        assert!(!meta.durability_pending(&[ReadKey::Dir(root)]));

        // Gate off: `c` is journaled untracked; with the gate back on it
        // is compared against the tip (and `a` stays exact).
        s.set_durable(false, u64::MAX, false, RowCommit::LONE, true);
        let before = meta.journal_tip().unwrap();
        crate::mutate::execute(&meta, &create("c"), None).unwrap();
        s.set_durable(true, before, false, RowCommit::ROOT_GROUP, false);
        assert!(meta.durability_pending(&dentry("c")));
        assert!(!meta.durability_pending(&dentry("a")));
        assert!(
            meta.durability_pending(&[ReadKey::Dir(root)]),
            "the directory's bound became untracked with c"
        );
        s.set_durable(
            true,
            meta.journal_tip().unwrap(),
            false,
            RowCommit::ROOT_GROUP,
            false,
        );
        assert!(!meta.durability_pending(&dentry("c")));
        assert!(!meta.durability_pending(&[ReadKey::Dir(root)]));
    }
}
