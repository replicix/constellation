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
    /// the next segment (what the `Exists` hint retires at; plan 29 M6's
    /// `ship_floor`, now exact for an idle holder).
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
    observed: Position,
    /// Speculation installed with a position, until the applied position
    /// dominates it.
    covering: Vec<(KeySet, Position)>,
    /// Plan 30 §M11: per delegation generation, the stream index this
    /// replica holds (applied, executed or appended).
    streams: std::collections::BTreeMap<u64, u64>,
    /// Plan 30 §M11: generations whose `Recall` this replica applied:
    /// any dependency on them is satisfied (void past the cut).
    voided: std::collections::BTreeSet<u64>,
    /// Plan 30 §M11: the delegation stream positions this node's
    /// clients were answered with (a shadow or hint installed from a
    /// delegate's reply does not raise `observed`, M6's rule, but a
    /// later write of the same client must order after it wherever it
    /// executes): the `deps` of the next write carry them.
    frontier: std::collections::BTreeMap<u64, u64>,
    /// Plan 30 §M11: the log part of the same (the root's replies, whose
    /// shadows do not raise `observed` either).
    frontier_log: Position,
}

impl Inner {
    fn applied_position(&self) -> Position {
        let mut streams = Streams::NONE;
        for (g, i) in &self.streams {
            if !streams.raise(*g, *i) {
                break;
            }
        }
        Position {
            seq: self.applied_seq,
            pending: self.applied,
            streams,
        }
    }

    /// `applied_position().dominates(target)` with the void rule: a
    /// dependency on an ended generation counts as satisfied.
    fn reaches(&self, target: &Position) -> bool {
        self.applied_seq >= target.seq
            && self.applied >= target.pending
            && target.streams.iter().all(|(g, i)| {
                self.voided.contains(&g) || self.streams.get(&g).is_some_and(|m| *m >= i)
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

impl Default for SessionState {
    fn default() -> Self {
        SessionState {
            inner: Mutex::new(Inner::default()),
            cv: Condvar::new(),
            budget_ms: AtomicU64::new(budget_default()),
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
        deps.streams
            .iter()
            .all(|(gen, i)| g.voided.contains(&gen) || g.streams.get(&gen).is_some_and(|m| *m >= i))
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

    /// A reply whose effects are not installed here observed `pos`.
    pub fn raise_observed(&self, pos: Position) {
        let mut g = self.inner.lock().unwrap();
        let next = g.observed.join(&pos);
        if next != g.observed {
            g.observed = next;
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
        let target = g.observed.join(floor);
        if g.reaches(&target) {
            return Some(SessionWait::Fast);
        }
        let covered = keys.iter().all(|k| {
            g.covering
                .iter()
                .filter(|(ks, _)| ks.covers(k))
                .any(|(_, p)| p.dominates(&target))
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
    /// `true` while the unshipped journal touched `keys` and reaches past
    /// the durable watermark. One atomic load when the gate is off.
    pub fn durability_pending(&self, keys: &[ReadKey]) -> bool {
        if !self.session.durable_gated() {
            return false;
        }
        if !self.unshipped_touches_keys(keys) {
            return false;
        }
        self.journal_tip().unwrap_or(0) > self.session.durable_jseq()
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
    /// (`store::journal::ack_rows_at`'s rule, read-only): the highest of
    /// them, unless a row still in the journal (held back, M4) lies below
    /// it — then just below that row. What a segment's `through` says.
    pub fn journal_through_after(&self, seqs: &[u64]) -> Result<u64, crate::MetaError> {
        let acked = self.journal_acked_seq()?;
        let Some(&upto) = seqs.iter().max() else {
            return Ok(acked);
        };
        if !self.held_any.load(Ordering::Relaxed) {
            return Ok(upto.max(acked));
        }
        let shipping: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        let first_left = self
            .journal_seqs_between(acked + 1, upto)?
            .into_iter()
            .find(|s| !shipping.contains(s));
        Ok(match first_left {
            Some(s) => s.saturating_sub(1).max(acked),
            None => upto.max(acked),
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

    #[test]
    fn a_waiter_is_released_by_advance() {
        let s = std::sync::Arc::new(SessionState::default());
        s.set_budget_ms(2_000);
        s.raise_observed(Position {
            seq: 2,
            pending: None,
            streams: Default::default(),
        });
        let s2 = s.clone();
        let t = std::thread::spawn(move || {
            s2.wait(
                &[ReadKey::Ino(1)],
                &Position::ZERO,
                || (s2.applied().seq, false, false),
                || false,
            )
        });
        std::thread::sleep(Duration::from_millis(30));
        s.advance(2, None);
        let r = t.join().unwrap();
        assert!(
            matches!(r, SessionWait::Waited(d) if d < Duration::from_millis(1_000)),
            "{r:?}"
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
}
