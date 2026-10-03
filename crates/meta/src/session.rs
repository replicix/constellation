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
//!   after their `Recall`; a replica that applied the `Recall` while
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
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
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
    #[serde(default)]
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
    #[serde(default)]
    pub abandoned: u64,
    #[serde(default)]
    pub voided_ended: u64,
    /// Plan 30 §M9: reads on a holder under a `Backup`/`S3` acknowledgement
    /// policy whose wait began because the unshipped journal touched their
    /// keys and was not yet durable (on every backup, or in the log).
    #[serde(default)]
    pub durability_blocked: u64,
    /// Plan 30 §M9: fast-path acknowledgements that waited for
    /// durability, their total wait, and those left in doubt (the lease
    /// was lost meanwhile; resubmitted by rid).
    pub fast_acks_waited: u64,
    pub fast_ack_wait_us_total: u64,
    pub fast_acks_in_doubt: u64,
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
    /// Plan 30 §M11: generations whose `Recall` this replica applied:
    /// any dependency on them is satisfied (void past the cut).
    voided: std::collections::BTreeSet<u64>,
    /// The log's cut of each voided generation: the stream index its
    /// root appended before the `Recall` (what this replica holds of it
    /// *from the log*, never a delegate's own executed-but-unappended
    /// rows). A dependency past it is *lost*: a tentative
    /// acknowledgement, stranded and replayed by rid (see
    /// [`SessionState::deps_lost`]).
    void_cuts: std::collections::BTreeMap<u64, u64>,
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
}

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

    /// Void every unreached generation `observed` names for which
    /// `ended` says the generation is over (the persisted delegation
    /// table once delegated it and no longer lists it): the rule
    /// `void_stream` applies at a `Recall`, for a replica that applied
    /// the record in an earlier incarnation. Returns how many.
    pub fn void_ended(&self, ended: impl Fn(u64) -> bool) -> usize {
        let gens = self.unreached_streams();
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
        if g.reaches(&observed) {
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
        // `Recall` applied here: long-delegated seed 70176).
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

    /// Plan 30 §M11: generation `gen` ended at `cut` (a `Recall` record
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

    /// What a restart forgets (EC2 campaign 7, the `git-under-flock-
    /// causal` reader): the stream indices and the voided generations
    /// live here in memory, and a replica that opens its store again
    /// re-applies nothing below its applied position — so a generation
    /// whose `Recall` it applied before the restart is neither held nor
    /// voided afterwards, and a watermark naming it (a lock grant's
    /// floor: the releaser's frontier, which still names the stream it
    /// was answered from) is never reached: every read under it waits
    /// the whole session budget, for the rest of the process. Seed from
    /// what the store persists: every generation up to the table's
    /// `max_gen` is held through the log's index of it, and one the
    /// table no longer lists is voided at that cut.
    pub fn seed_generations(
        &self,
        max_gen: u64,
        live: &std::collections::BTreeSet<u64>,
        log_idx: &dyn Fn(u64) -> u64,
    ) {
        let mut g = self.inner.lock().unwrap();
        for gen in 1..=max_gen {
            let idx = log_idx(gen);
            let cur = g.streams.entry(gen).or_insert(0);
            if idx > *cur {
                *cur = idx;
            }
            if !live.contains(&gen) {
                g.voided.insert(gen);
                g.observed.streams.lower(gen, idx);
                let e = g.void_cuts.entry(gen).or_insert(0);
                *e = (*e).max(idx);
            }
        }
        drop(g);
        self.cv.notify_all();
    }

    /// The log's cut of voided generation `gen` (see `Inner::void_cuts`).
    /// Only raised: rows of the generation the log carries after its
    /// `Recall` (a root that was itself the delegate ships its own rows
    /// after ending the generation) are not lost.
    pub fn note_void_cut(&self, gen: u64, cut: u64) {
        let mut g = self.inner.lock().unwrap();
        let e = g.void_cuts.entry(gen).or_insert(0);
        *e = (*e).max(cut);
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
        let mut g = self.inner.lock().unwrap();
        g.applied_seq = g.applied_seq.max(seq);
        if through > g.applied {
            g.applied = through;
        }
        let applied = g.applied_position();
        g.covering.retain(|(_, p)| !applied.dominates(p));
        drop(g);
        self.cv.notify_all();
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
        if next != g.observed {
            g.observed = next;
            g.observed_since = Some(Instant::now());
            drop(g);
            self.stats.lock().unwrap().raised += 1;
        }
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
    /// wakes the waiters.
    pub fn set_durable(&self, gate: bool, jseq: u64, lost: bool) {
        self.durable_lost.store(lost, Ordering::Relaxed);
        self.durable_gate.store(gate, Ordering::Relaxed);
        let before = self.durable_jseq.swap(jseq, Ordering::Relaxed);
        if !gate || jseq > before {
            self.cv.notify_all();
        }
    }

    /// Plan 30 §M9: the FUSE fast path's acknowledgement wait under a
    /// durability gate. The op is journaled at or below `jseq`; return
    /// once the core's durable watermark covers it, or once the gate is
    /// off — because the policy is `Local` again (`Ungated`: the row is
    /// this node's, acknowledged as under `Local`), or because the lease
    /// was lost (`Lost`: in doubt, resubmitted by rid).
    pub fn wait_durable(&self, jseq: u64, budget: Duration) -> DurableWait {
        let started = Instant::now();
        loop {
            if !self.durable_gate.load(Ordering::Relaxed) {
                return if self.durable_lost.load(Ordering::Relaxed) {
                    DurableWait::Lost
                } else {
                    DurableWait::Ungated
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
        // Both watermarks, joined when what they still owe fits the cap,
        // else each on its own (never one dropped: see `join_owed`).
        let targets: Vec<Position> = match g.join_owed(&g.observed, floor) {
            Some(t) => vec![t],
            None => vec![g.observed, *floor],
        };
        if targets.iter().all(|t| g.reaches(t)) {
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
    /// the dependency is void, as a `Recall` applied in this incarnation
    /// would have made it.
    fn void_ended_generations(&self) {
        if self.session.unreached_streams().is_empty() {
            return;
        }
        let table = self.delegation_table();
        let max_gen = table.max_gen();
        let voided = self
            .session
            .void_ended(|gen| gen <= max_gen && !table.iter().any(|d| d.gen == gen));
        if voided > 0 {
            tracing::info!(
                voided,
                max_gen,
                "voided session dependencies on delegation generations that ended before this \
                 incarnation (the delegation table no longer lists them)"
            );
        }
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
    /// touches any of `keys`. One counter read when the queue is empty.
    pub fn replay_touches(&self, keys: &[ReadKey]) -> bool {
        if !self.has_pending_replays() {
            return false;
        }
        self.pending_replays().is_ok_and(|queue| {
            queue.iter().any(|q| {
                let ks = KeySet::from_op(&q.op);
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
        s.seed_generations(2, &live, &log_idx);
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
    /// generation that ended before this incarnation (its `Recall` was
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
        s.set_durable(true, 0, false);
        crate::mutate::execute(&meta, &create("a"), None).unwrap();
        assert!(meta.durability_pending(&dentry("a")));
        s.set_durable(true, meta.journal_tip().unwrap(), false);
        crate::mutate::execute(&meta, &create("b"), None).unwrap();
        // `a` is durable although `b`'s row (a different key) is not.
        assert!(!meta.durability_pending(&dentry("a")));
        assert!(meta.durability_pending(&dentry("b")));
        assert!(!meta.durability_pending(&dentry("zzz")), "untouched");
        assert!(
            meta.durability_pending(&[ReadKey::Dir(root)]),
            "`ls` sees b"
        );
        s.set_durable(true, meta.journal_tip().unwrap(), false);
        assert!(!meta.durability_pending(&[ReadKey::Dir(root)]));

        // Gate off: `c` is journaled untracked; with the gate back on it
        // is compared against the tip (and `a` stays exact).
        s.set_durable(false, u64::MAX, false);
        let before = meta.journal_tip().unwrap();
        crate::mutate::execute(&meta, &create("c"), None).unwrap();
        s.set_durable(true, before, false);
        assert!(meta.durability_pending(&dentry("c")));
        assert!(!meta.durability_pending(&dentry("a")));
        assert!(
            meta.durability_pending(&[ReadKey::Dir(root)]),
            "the directory's bound became untracked with c"
        );
        s.set_durable(true, meta.journal_tip().unwrap(), false);
        assert!(!meta.durability_pending(&dentry("c")));
        assert!(!meta.durability_pending(&[ReadKey::Dir(root)]));
    }
}
