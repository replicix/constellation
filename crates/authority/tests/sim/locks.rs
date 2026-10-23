//! Plan 30 §M14 in the simulation: cross-node `flock`/`fcntl`.
//!
//! A lock step of a client thread is what the FUSE layer does for a
//! whole-file `setlk`, over the node's real `Meta::locks()` tables and
//! its real core:
//!
//! - `local_set`; on `NeedGrant(mode)` a `Control::Lock` (blocking, or
//!   non-blocking for a `F_SETLK`), and on `Granted { position }` the
//!   session wait at `position` (the replica must have what the previous
//!   holder wrote), then `local_set` again; `Conflict` (another local
//!   owner) waits locally;
//! - the critical section: a few I/O steps, each asking
//!   `LockTables::fenced` first, and the owner fence
//!   (`LockTables::fenced_owners`: the turn write goes to *another* file,
//!   which only the owner fence covers) — a fenced step is refused
//!   (`EIO`) and the client gives the lock up;
//! - `local_unlock`; when that left the inode idle under a recalled
//!   grant, `Control::LockIdle` (the core flushes, then releases).
//!
//! The mutual-exclusion ghost ([`LockGhost`]) sees, per inode, who is
//! *performing I/O* under a lock — from its first non-fenced I/O step to
//! its unlock or its first fenced step — in true simulated time (not a
//! node's possibly skewed clock). Two holders in conflicting modes at
//! once is a violation, whether on two nodes (the grants failed) or on
//! one (the local table failed).
//!
//! A crashed node's lock state is the process's: [`reset_node`] empties
//! its tables (the sim keeps the `Meta` across a restart for the
//! journal, which production reopens from disk with empty lock tables).

use super::node::NodeHandle;
use super::run::Cluster;
use constellation_authority::action::{ControlOk, LockAnswer};
use constellation_authority::{Control, NodeId};
use constellation_meta::locks::{Fence, LocalLock, LocalOutcome, LockMode};
use constellation_meta::{Meta, ReadKey};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One lock step of a client: a whole-file lock on `lock_files[file]`,
/// `ios` I/O steps `io_ms` apart, then the unlock.
#[derive(Clone, Debug)]
pub struct LockStep {
    pub file: usize,
    pub mode: LockMode,
    pub blocking: bool,
    pub ios: u32,
    pub io_ms: u64,
    /// Write a turn number into the lock file's mtime under the lock
    /// (`SimConfig::lock_writes`).
    pub write: bool,
}

/// A lock acquisition that has not completed within this much simulated
/// time fails the seed (a hang: the client-level cap is far longer).
pub const LOCK_WAIT_CAP_MS: u64 = 60_000;

/// What the lock clients saw, summed over the run.
#[derive(Debug, Default, Clone, Copy)]
pub struct LockCounters {
    /// Lock steps started, and those that got their lock.
    pub steps: u64,
    pub acquired: u64,
    /// I/O steps performed, and refused because the grant had lapsed.
    pub ios: u64,
    pub fenced_ios: u64,
    /// Of those, refused by the owner fence alone (the lock file's own
    /// fence had lifted: a new grant arrived after the lapse).
    pub owner_fenced_ios: u64,
    /// `local_set` found another local owner in the way.
    pub local_conflicts: u64,
    /// `Control::Lock` answers.
    pub granted: u64,
    pub would_block: u64,
    /// Plan 43 F-b: `Wait` answers (a non-blocking request the owner
    /// could not grant yet, nothing in conflict), honoured by asking
    /// again after `retry_ms`.
    pub grace_waits: u64,
    /// A non-blocking request that waited past the engine's bound
    /// (`Config::lock_owner_wait_max_ms`) and gave up, `EAGAIN` in the
    /// engine.
    pub wait_gave_up: u64,
    pub unavailable: u64,
    /// A `Granted` after which `local_set` still needed a grant (it had
    /// lapsed or been taken back meanwhile).
    pub regrants: u64,
    /// `Control::LockIdle` sent (the unlock left a recalled grant idle).
    pub idle_sent: u64,
    /// The session wait at a grant's position ran out of budget.
    pub position_timeouts: u64,
    /// The node died under the step.
    pub abandoned: u64,
    /// The longest acquisition (simulated ms).
    pub max_wait_ms: u64,
    /// ... of a client of node 1, the lock files' owner (the root), and of
    /// the clients of the other nodes: the owner's own clients share a
    /// table with the owner, which the others' do not.
    pub max_wait_owner_ms: u64,
    pub max_wait_rest_ms: u64,
    /// Plan 30 §M14 (lock-to-unlock coherence): grants whose holder was
    /// checked against the frontier the previous exclusive holder on
    /// another node had at its unlock, and those whose replica did not
    /// reach it once the grant's session wait was over.
    pub visibility_checks: u64,
    pub visibility_gaps: u64,
    /// `lock_writes`: turns written and acknowledged under a lock, reads
    /// by the next holder on another node, and reads that found an older
    /// turn (a stale read under the lock: a failure).
    pub turns_written: u64,
    pub turn_reads: u64,
    pub stale_turn_reads: u64,
    /// Reads that found an unacknowledged older turn landed late (plan
    /// 30 §M14 phase 2: none, now that a write carries its fencing token).
    pub late_unacked_turns: u64,
    /// Turn writes the executor refused for a lapsed grant (`LockLapsed`).
    pub token_refused: u64,
    /// Fairness: acquisitions made while a client on *another* node had
    /// been waiting for the same lock for more than [`LOCK_FAIR_MS`]
    /// longer (it asked first, by a whole grant window and more, and was
    /// overtaken). Faults reorder legitimately (a paused or partitioned
    /// waiter is skipped); a fault-free run should count none.
    pub overtaken: u64,
    /// The longest idle time a blocking waiter waited for a lock after the
    /// last critical section that conflicted with it had ended: `acquired
    /// at − max(its wait began, the last conflicting section's end)` (none
    /// for the first acquisition of a lock). It
    /// does not depend on the queue ahead of it or on the sections' length:
    /// what a handoff costs when the lock is free and the waiter is next
    /// (an exclusive waiter conflicts with any section, a shared one with
    /// exclusive ones). The ask-again rule's measure: a stale push, an
    /// outwait or a 9.5 s re-send shows here.
    pub max_handoff_gap_ms: u64,
    /// Plan 43 C7-F F-c: I/O steps (a client's first, or a later one) that
    /// found their grant lapsing and *parked* (waited for the owner's
    /// verdict, as the engine's parked ops do) instead of being refused;
    /// those that resumed (the owner admitted a late renewal), and those
    /// refused in the end (`EIO`, as a lapse always was). Lock requests
    /// that waited for a lapsing grant's verdict (`LocalOutcome::Parked`).
    pub parked_ios: u64,
    pub resumed_ios: u64,
    pub park_refused_ios: u64,
    pub parked_requests: u64,
    /// The longest park (simulated ms).
    pub max_park_ms: u64,
}

/// A park that has not ended within this much simulated time fails the seed:
/// the engine's bound (`lock_owner_wait_max_ms`) plus slack.
pub const PARK_CAP_SLACK_MS: u64 = 15_000;

/// See [`LockCounters::overtaken`]: `2 × (ttl + margin)` of the sim's
/// default lock configuration.
pub const LOCK_FAIR_MS: u64 = 12_000;

struct InIo {
    node: NodeId,
    thread: u64,
    mode: LockMode,
    since_ms: u64,
}

#[derive(Default)]
struct GhostInner {
    seed: u64,
    files: Vec<String>,
    inos: Vec<u64>,
    /// `SimConfig::lock_unlink_ratio`: the clients lock the inodes set at
    /// setup, not what the names resolve to (they may be unlinked).
    by_ino: bool,
    in_io: BTreeMap<u64, Vec<InIo>>,
    violations: Vec<String>,
    counters: LockCounters,
    /// The last lock events, for a violation's report.
    trace: VecDeque<String>,
    /// Per inode, the last exclusive holder's unlock: its node and its
    /// session frontier then (everything its node had been acknowledged).
    last_release: BTreeMap<u64, (NodeId, u64, constellation_meta::Position)>,
    visibility_gaps: Vec<String>,
    /// `lock_writes`: the next turn number, and per inode the last turn
    /// acknowledged under an exclusive lock `(turn, node, at)`.
    next_turn: i64,
    last_turn: BTreeMap<u64, (i64, NodeId, u64, constellation_meta::Rid)>,
    /// Per lock file (by index), the data file its turns go to (empty:
    /// the lock file itself).
    data: Vec<u64>,
    /// Every turn written, by its rid.
    turn_rids: BTreeMap<i64, constellation_meta::Rid>,
    /// Turns whose write was acknowledged under the lock.
    turns_acked: std::collections::BTreeSet<i64>,
    /// F1 (`SimConfig::strict_lock_turns`): turns whose write was answered
    /// `Accepted`, whether or not the client still honoured its grant
    /// then, and whether such a turn is never excused as unacknowledged.
    turns_accepted: std::collections::BTreeSet<i64>,
    strict_turns: bool,
    /// See `SimConfig::lock_write_wait_ms`.
    write_wait_ms: Option<u64>,
    /// F1: every critical section, `(ino, node, thread, mode, from, until)`
    /// (`None`: still open), and per turn write the lock it was written
    /// under `(lock ino, node)`: what a held write's landing is checked
    /// against (`LockGhost::check_landings`).
    io_history: Vec<(u64, NodeId, u64, LockMode, u64, Option<u64>)>,
    turn_locks: BTreeMap<constellation_meta::Rid, (u64, NodeId, u64, i64)>,
    /// Every turn acknowledged under its lock, `(data ino, turn, node,
    /// at)`.
    acked_history: Vec<(u64, i64, NodeId, u64)>,
    /// `(the missed turn's rid, the read turn's rid, the report)`.
    #[allow(clippy::type_complexity)]
    stale_reads: Vec<(
        constellation_meta::Rid,
        Option<constellation_meta::Rid>,
        String,
    )>,
    /// Fairness: per inode, the clients waiting for a grant `(node,
    /// thread, since, mode)`.
    waiting: BTreeMap<u64, Vec<(NodeId, u64, u64, LockMode)>>,
    overtakes: Vec<String>,
    /// Per inode, when the last critical section ended `(any, exclusive)`
    /// ([`LockCounters::max_handoff_gap_ms`]).
    section_ends: BTreeMap<u64, (u64, u64)>,
    /// Plan 43 C7-F F-c: per inode, every grant a client was told of
    /// `(node, mode, simulated ms)`: what a parked client that resumes is
    /// checked against (`check_resume`).
    grants_seen: BTreeMap<u64, Vec<(NodeId, LockMode, u64)>>,
}

/// The mutual-exclusion ghost and the lock clients' counters.
#[derive(Default)]
pub struct LockGhost {
    inner: Mutex<GhostInner>,
}

const TRACE_LEN: usize = 300;

impl LockGhost {
    pub fn new(seed: u64) -> Self {
        let g = Self::default();
        g.inner.lock().unwrap().seed = seed;
        g
    }

    fn with<R>(&self, f: impl FnOnce(&mut GhostInner) -> R) -> R {
        f(&mut self.inner.lock().unwrap())
    }

    pub fn set_files(&self, files: Vec<(String, u64)>) {
        self.with(|g| {
            g.files = files.iter().map(|(n, _)| n.clone()).collect();
            g.inos = files.iter().map(|(_, i)| *i).collect();
        });
    }

    pub fn set_by_ino(&self, by_ino: bool) {
        self.with(|g| g.by_ino = by_ino);
    }

    /// See `SimConfig::strict_lock_turns`.
    pub fn set_strict_turns(&self, on: bool) {
        self.with(|g| g.strict_turns = on);
    }

    /// How long a client waits for its turn write's answer before it
    /// gives up on it (in doubt): 3 s; under `strict_lock_turns` as long
    /// as an application waits for its FUSE write (a held write is
    /// answered when it lands), unless `SimConfig::lock_write_wait_ms`
    /// says otherwise.
    fn write_wait_ms(&self) -> u64 {
        self.with(|g| match g.write_wait_ms {
            Some(ms) => ms,
            None if g.strict_turns => 30_000,
            None => 3_000,
        })
    }

    /// See `SimConfig::lock_write_wait_ms`.
    pub fn set_write_wait_ms(&self, ms: Option<u64>) {
        self.with(|g| g.write_wait_ms = ms);
    }

    /// F1: the turn write `rid` (turn `turn` into `data`) is issued under
    /// `lock_ino`'s lock by `node`.
    fn turn_lock(
        &self,
        rid: constellation_meta::Rid,
        lock_ino: u64,
        node: NodeId,
        data: u64,
        turn: i64,
    ) {
        self.with(|g| {
            g.turn_locks.insert(rid, (lock_ino, node, data, turn));
        });
    }

    /// F1: held writes that landed (`(rid, simulated ms)`, the sim
    /// driver's `Shared::landed`) must not have landed inside another
    /// node's conflicting critical section on the lock they were written
    /// under: the next holder's floor then lacked them, and an
    /// acknowledged or in-doubt write overwrote what it wrote under its
    /// own lock (the lost update the token at the write and the settle
    /// prevent). Nor after another node's newer turn into the same file
    /// was acknowledged: it overwrote that acknowledged write. `skip`: the
    /// writes rolled back since (tentative), whose effect is gone.
    pub fn check_landings(
        &self,
        landed: &[(constellation_meta::Rid, u64)],
        skip: &std::collections::HashSet<constellation_meta::Rid>,
    ) {
        self.with(|g| {
            let seed = g.seed;
            let mut found = Vec::new();
            for (rid, at) in landed {
                if skip.contains(rid) {
                    continue;
                }
                let Some((ino, node, data, turn)) = g.turn_locks.get(rid).copied() else {
                    continue;
                };
                for (d, t, n, acked_at) in &g.acked_history {
                    if *d == data && *n != node && *t > turn && *acked_at < *at {
                        found.push(format!(
                            "seed {seed}: ino {data:#x}: node {node}'s held write {rid:?} (turn \
                             {turn}) landed at t={at}, after node {n}'s turn {t} was \
                             acknowledged there (t={acked_at}): an acknowledged write lost"
                        ));
                    }
                }
                for (i, n, _, mode, from, until) in &g.io_history {
                    if *i == ino
                        && *n != node
                        && *mode == LockMode::Exclusive
                        && *from <= *at
                        && until.is_none_or(|u| *at < u)
                    {
                        found.push(format!(
                            "seed {seed}: ino {ino:#x}: node {node}'s held write {rid:?} under its \
                             lock landed at t={at}, inside node {n}'s critical section \
                             (t={from}..{until:?})"
                        ));
                    }
                }
            }
            g.violations.extend(found);
        });
    }

    /// F1: a turn's write was answered `Accepted`: its application was
    /// told it succeeded under the lock.
    fn turn_accepted(&self, turn: i64) {
        self.with(|g| {
            g.turns_accepted.insert(turn);
        });
    }

    /// The inode lock file `i` had at setup, if the clients lock that
    /// whatever its name resolves to now ([`Self::set_by_ino`]).
    fn setup_ino(&self, i: usize) -> Option<u64> {
        self.with(|g| (g.by_ino && !g.inos.is_empty()).then(|| g.inos[i % g.inos.len()]))
    }

    pub fn file(&self, i: usize) -> Option<String> {
        self.with(|g| {
            if g.files.is_empty() {
                None
            } else {
                Some(g.files[i % g.files.len()].clone())
            }
        })
    }

    pub fn set_data(&self, data: Vec<u64>) {
        self.with(|g| g.data = data);
    }

    /// Where the turns under lock file `file` are written.
    fn data_ino(&self, file: usize, lock_ino: u64) -> u64 {
        self.with(|g| {
            if g.data.is_empty() {
                lock_ino
            } else {
                g.data[file % g.data.len()]
            }
        })
    }

    pub fn inos(&self) -> Vec<u64> {
        self.with(|g| g.inos.clone())
    }

    pub fn count(&self, f: impl FnOnce(&mut LockCounters)) {
        self.with(|g| f(&mut g.counters));
    }

    pub fn counters(&self) -> LockCounters {
        self.with(|g| g.counters)
    }

    pub fn violations(&self) -> Vec<String> {
        self.with(|g| g.violations.clone())
    }

    pub fn visibility_gaps(&self) -> Vec<String> {
        self.with(|g| g.visibility_gaps.clone())
    }

    #[allow(clippy::type_complexity)]
    pub fn stale_reads(
        &self,
    ) -> Vec<(
        constellation_meta::Rid,
        Option<constellation_meta::Rid>,
        String,
    )> {
        self.with(|g| g.stale_reads.clone())
    }

    fn take_turn(&self, rid: constellation_meta::Rid) -> i64 {
        self.with(|g| {
            g.next_turn += 1;
            // Well past any real clock's ns, so no other write of the
            // file's mtime can look like a turn.
            let turn = 4_000_000_000_000_000_000 + g.next_turn;
            g.turn_rids.insert(turn, rid);
            turn
        })
    }

    fn turn_acked(
        &self,
        ino: u64,
        turn: i64,
        node: NodeId,
        at_ms: u64,
        rid: constellation_meta::Rid,
    ) {
        self.with(|g| {
            g.counters.turns_written += 1;
            g.turns_acked.insert(turn);
            g.acked_history.push((ino, turn, node, at_ms));
            let e = g.last_turn.entry(ino).or_insert((0, node, at_ms, rid));
            if turn > e.0 {
                *e = (turn, node, at_ms, rid);
            }
        });
    }

    /// A holder on `node` with a fresh grant read the file's mtime: it
    /// must be the last turn another node wrote under the lock, or later.
    fn turn_read(&self, ino: u64, node: NodeId, at_ms: u64, read: Option<i64>) {
        self.with(|g| {
            let Some((turn, by, at, rid)) = g.last_turn.get(&ino).copied() else {
                return;
            };
            if by == node {
                return;
            }
            g.counters.turn_reads += 1;
            if read.is_some_and(|m| m >= turn) {
                return;
            }
            // An older turn whose write was never acknowledged under its
            // lock (the client gave up on it: in doubt, still in an inbox
            // or a retry) landed late over the newer ones: the write
            // outlived its lock, which no grant can order (PROGRESS.md,
            // the fence's flush-start limit).
            // F1: not one its application was told had succeeded (the
            // lock's lost update: an acknowledged write landed over the
            // next holder's).
            if read.is_some_and(|m| {
                g.turn_rids.contains_key(&m)
                    && !g.turns_acked.contains(&m)
                    && !(g.strict_turns && g.turns_accepted.contains(&m))
            }) {
                g.counters.late_unacked_turns += 1;
                return;
            }
            g.counters.stale_turn_reads += 1;
            let seed = g.seed;
            let read_rid = read.and_then(|m| g.turn_rids.get(&m).copied());
            g.stale_reads.push((
                rid,
                read_rid,
                format!(
                "seed {seed}: ino {ino:#x}: node {node} read turn {read:?} at t={at_ms} under a \
                 fresh grant; node {by} had written turn {turn} under its lock (acked t={at}, \
                 rid {rid:?})"
            ),
            ));
        });
    }

    /// An exclusive holder on `node` unlocked `ino` with its node's
    /// session at `frontier`.
    pub fn released(
        &self,
        ino: u64,
        node: NodeId,
        at_ms: u64,
        frontier: constellation_meta::Position,
    ) {
        self.with(|g| {
            g.last_release.insert(ino, (node, at_ms, frontier));
        });
    }

    /// `node` enters a critical section on `ino` with a grant it was just
    /// given (and waited for): its replica must have what the previous
    /// exclusive holder on another node had been acknowledged when it
    /// unlocked — the grant's position carries that ("lock-to-unlock
    /// coherence", PROGRESS.md "Fix: git-under-flock divergence").
    /// `own_epoch`: the root lease epoch `node` holds, if it does — its
    /// own tenure's journal is in its replica (it executed it), though
    /// the session counts only shipped journal as reached (the core's
    /// grant to itself leaves that journal out for the same reason).
    pub fn check_visible(
        &self,
        ino: u64,
        node: NodeId,
        at_ms: u64,
        meta: &Meta,
        own_epoch: Option<u64>,
    ) {
        self.with(|g| {
            let Some((prev, released_at, mut frontier)) = g.last_release.get(&ino).copied() else {
                return;
            };
            if prev == node {
                return;
            }
            if frontier
                .pending
                .is_some_and(|p| Some(p.epoch) == own_epoch)
            {
                frontier.pending = None;
            }
            g.counters.visibility_checks += 1;
            if meta.session().reaches(&frontier) {
                return;
            }
            g.counters.visibility_gaps += 1;
            let seed = g.seed;
            g.visibility_gaps.push(format!(
                "seed {seed}: ino {ino:#x}: node {node} entered at t={at_ms} without what node                  {prev} had at its unlock (t={released_at}): {frontier:?}; applied {:?}",
                meta.applied_seq().ok()
            ));
        });
    }

    pub fn trace(&self) -> Vec<String> {
        self.with(|g| g.trace.iter().cloned().collect())
    }

    pub fn note(&self, at_ms: u64, what: String) {
        tracing::debug!(t = at_ms, "lock ghost: {what}");
        self.with(|g| {
            if g.trace.len() == TRACE_LEN {
                g.trace.pop_front();
            }
            g.trace.push_back(format!("t={at_ms} {what}"));
        });
    }

    /// `thread` on `node` starts performing I/O on `ino` under a lock in
    /// `mode`: every holder already there must be compatible.
    pub fn enter(&self, ino: u64, node: NodeId, thread: u64, mode: LockMode, at_ms: u64) {
        self.note(
            at_ms,
            format!("node {node} t{thread} enters I/O on {ino:#x} {mode:?}"),
        );
        self.with(|g| {
            let seed = g.seed;
            let mut found = Vec::new();
            let v = g.in_io.entry(ino).or_default();
            for e in v.iter() {
                if e.mode.conflicts(mode) {
                    found.push(format!(
                        "seed {seed}: ino {ino:#x}: node {} (t{}, {:?}, in I/O since t={}) and \
                         node {node} (t{thread}, {mode:?}) at t={at_ms}{}",
                        e.node,
                        e.thread,
                        e.mode,
                        e.since_ms,
                        if e.node == node {
                            " [same node: the local table]"
                        } else {
                            ""
                        }
                    ));
                }
            }
            v.push(InIo {
                node,
                thread,
                mode,
                since_ms: at_ms,
            });
            g.violations.extend(found);
            g.io_history.push((ino, node, thread, mode, at_ms, None));
        });
    }

    /// A client of `node` was told it holds a grant in `mode` on `ino`.
    pub fn grant_seen(&self, ino: u64, node: NodeId, mode: LockMode, at_ms: u64) {
        self.with(|g| {
            g.grants_seen
                .entry(ino)
                .or_default()
                .push((node, mode, at_ms))
        });
    }

    /// Plan 43 C7-F F-c: `thread` on `node` parked at `from_ms` (its grant
    /// lapsed: it performs no I/O while parked, so it left its critical
    /// section then) and resumes at `to_ms`, the owner having admitted a late
    /// renewal of the same grant. Safe only if nobody was granted a
    /// conflicting lock meanwhile: the property of the model, in the sim's
    /// terms (a grant another node's client was told of in between, or a
    /// holder in a conflicting section now).
    pub fn check_resume(
        &self,
        ino: u64,
        node: NodeId,
        thread: u64,
        mode: LockMode,
        from_ms: u64,
        to_ms: u64,
    ) {
        self.note(
            to_ms,
            format!(
                "node {node} t{thread} resumes {mode:?} on {ino:#x} (parked since t={from_ms})"
            ),
        );
        self.with(|g| {
            let seed = g.seed;
            let mut found = Vec::new();
            if let Some(v) = g.grants_seen.get(&ino) {
                for (n, m, at) in v {
                    if *n != node && m.conflicts(mode) && *at > from_ms && *at <= to_ms {
                        found.push(format!(
                            "seed {seed}: ino {ino:#x}: node {node} (t{thread}, {mode:?}) resumed at \
                             t={to_ms} after a late renewal, although node {n} had been granted \
                             {m:?} at t={at}, inside its park (since t={from_ms})"
                        ));
                    }
                }
            }
            if let Some(v) = g.in_io.get(&ino) {
                for e in v {
                    if e.node != node && e.mode.conflicts(mode) {
                        found.push(format!(
                            "seed {seed}: ino {ino:#x}: node {node} (t{thread}, {mode:?}) resumed at \
                             t={to_ms} while node {} (t{}, {:?}) is in I/O since t={}",
                            e.node, e.thread, e.mode, e.since_ms
                        ));
                    }
                }
            }
            g.violations.extend(found);
        });
    }

    pub fn leave(&self, ino: u64, node: NodeId, thread: u64, at_ms: u64, why: &str) {
        self.note(
            at_ms,
            format!("node {node} t{thread} leaves I/O on {ino:#x} ({why})"),
        );
        self.with(|g| {
            if let Some(v) = g.in_io.get_mut(&ino) {
                if let Some(e) = v.iter().find(|e| e.node == node && e.thread == thread) {
                    let ends = g.section_ends.entry(ino).or_default();
                    ends.0 = ends.0.max(at_ms);
                    if e.mode == LockMode::Exclusive {
                        ends.1 = ends.1.max(at_ms);
                    }
                }
                v.retain(|e| !(e.node == node && e.thread == thread));
            }
            if let Some(e) = g
                .io_history
                .iter_mut()
                .rev()
                .find(|e| e.0 == ino && e.1 == node && e.2 == thread && e.5.is_none())
            {
                e.5 = Some(at_ms);
            }
        });
    }

    /// The process on `node` died: its applications' locks went with it.
    pub fn drop_node(&self, node: NodeId, at_ms: u64) {
        self.note(at_ms, format!("node {node} crashed"));
        self.with(|g| {
            for (ino, v) in g.in_io.iter_mut() {
                for e in v.iter().filter(|e| e.node == node) {
                    let ends = g.section_ends.entry(*ino).or_default();
                    ends.0 = ends.0.max(at_ms);
                    if e.mode == LockMode::Exclusive {
                        ends.1 = ends.1.max(at_ms);
                    }
                }
                v.retain(|e| e.node != node);
            }
            for e in g.io_history.iter_mut() {
                if e.1 == node && e.5.is_none() {
                    e.5 = Some(at_ms);
                }
            }
            for v in g.waiting.values_mut() {
                v.retain(|e| e.0 != node);
            }
        });
    }

    /// `thread` on `node` starts waiting for a grant on `ino` (its first
    /// `NeedGrant`).
    pub fn wait_begin(&self, ino: u64, node: NodeId, thread: u64, mode: LockMode, at_ms: u64) {
        self.with(|g| {
            let v = g.waiting.entry(ino).or_default();
            if !v.iter().any(|e| e.0 == node && e.1 == thread) {
                v.push((node, thread, at_ms, mode));
            }
        });
    }

    /// The wait ended (acquired, or given up): counts an overtake when
    /// a client of another node has been waiting [`LOCK_FAIR_MS`] longer.
    pub fn wait_end(&self, ino: u64, node: NodeId, thread: u64, at_ms: u64, acquired: bool) {
        self.with(|g| {
            let seed = g.seed;
            let ends = g.section_ends.get(&ino).copied().unwrap_or((0, 0));
            let Some(v) = g.waiting.get_mut(&ino) else {
                return;
            };
            let mine = v
                .iter()
                .find(|e| e.0 == node && e.1 == thread)
                .map(|e| (e.2, e.3));
            v.retain(|e| !(e.0 == node && e.1 == thread));
            let (Some((since, mode)), true) = (mine, acquired) else {
                return;
            };
            let conflicting_end = if mode == LockMode::Exclusive {
                ends.0
            } else {
                ends.1
            };
            let gap = at_ms.saturating_sub(since.max(conflicting_end));
            // (The first acquisition of a lock is nobody's handoff: its wait
            // is the cluster's start-up, the delegation's installation.)
            if conflicting_end > 0 {
                g.counters.max_handoff_gap_ms = g.counters.max_handoff_gap_ms.max(gap);
            }
            if conflicting_end > 0 && gap > 3_000 {
                tracing::debug!(
                    t = at_ms,
                    "lock ghost: SLOW HANDOFF node {node} t{thread} {mode:?} on {ino:#x}: waiting \
                     since t={since}, last conflicting section ended t={conflicting_end}, acquired \
                     at t={at_ms}: idle {gap} ms"
                );
            }
            let overtaken: Vec<String> = v
                .iter()
                .filter(|e| e.0 != node && e.2 + LOCK_FAIR_MS < since)
                .map(|e| format!("node {} t{} (waiting since t={})", e.0, e.1, e.2))
                .collect();
            if !overtaken.is_empty() {
                g.counters.overtaken += 1;
                g.overtakes.push(format!(
                    "seed {seed}: ino {ino:#x}: node {node} t{thread} (asked t={since}) acquired at t={at_ms} ahead of {}",
                    overtaken.join(", ")
                ));
            }
        });
    }

    pub fn overtakes(&self) -> Vec<String> {
        self.with(|g| g.overtakes.clone())
    }

    /// Nodes with a client inside a critical section now.
    pub fn nodes_in_io(&self) -> Vec<NodeId> {
        self.with(|g| {
            let mut v: Vec<NodeId> = g.in_io.values().flatten().map(|e| e.node).collect();
            v.sort_unstable();
            v.dedup();
            v
        })
    }
}

/// Empty `meta`'s lock tables: the process died (see the module doc).
pub fn reset_node(meta: &Meta, inos: &[u64], now: constellation_authority::Now) {
    let t = meta.locks();
    t.clear_grants();
    for (ino, _) in t.held_all() {
        t.drop_held_any(ino);
    }
    for ino in inos {
        for l in t.local_locks(*ino) {
            t.local_release_owner(*ino, l.owner, now);
        }
    }
}

/// The node's current handle while it is the incarnation `inc`, alive.
fn same(cluster: &Cluster, node: NodeId, inc: u32) -> Option<Arc<NodeHandle>> {
    let h = cluster.get(node);
    (h.alive() && h.shared.incarnation.load(Ordering::SeqCst) == inc).then_some(h)
}

enum Awaited {
    Answer(Result<ControlOk, String>),
    /// The node died meanwhile (its answer channels are dropped with it,
    /// or never answered by a stale handle).
    Died,
    /// Not answered by `deadline_ms` (simulated, true time).
    Late,
}

async fn await_control(
    cluster: &Cluster,
    node: NodeId,
    inc: u32,
    deadline_ms: u64,
    mut rx: tokio::sync::oneshot::Receiver<Result<ControlOk, String>>,
) -> Awaited {
    loop {
        match tokio::time::timeout(Duration::from_millis(100), &mut rx).await {
            Ok(Ok(r)) => return Awaited::Answer(r),
            Ok(Err(_)) => return Awaited::Died,
            Err(_) => {
                if same(cluster, node, inc).is_none() {
                    return Awaited::Died;
                }
                if cluster.env.clock.elapsed_ms() > deadline_ms {
                    return Awaited::Late;
                }
            }
        }
    }
}

/// A wait registered with the fairness ghost; a step that gives up
/// (died, would block, unavailable, a failure) withdraws it on drop.
struct WaitGuard {
    ghost: Arc<LockGhost>,
    ino: u64,
    node: NodeId,
    thread: u64,
    clock: super::clock::Clock,
    done: bool,
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        if !self.done {
            self.ghost.wait_end(
                self.ino,
                self.node,
                self.thread,
                self.clock.elapsed_ms(),
                false,
            );
        }
    }
}

/// One lock step (see the module doc).
pub async fn client_lock(
    cluster: Arc<Cluster>,
    node: NodeId,
    thread: u64,
    step: LockStep,
    wait_budget_ms: u64,
    ignore_fence: bool,
    failures: Arc<Mutex<Vec<String>>>,
) {
    let ghost = cluster.locks.clone();
    let clock = cluster.env.clock.clone();
    let handle = cluster.get(node);
    if !handle.alive() {
        return;
    }
    let inc = handle.shared.incarnation.load(Ordering::SeqCst);
    let Some(name) = ghost.file(step.file) else {
        return;
    };
    ghost.count(|c| c.steps += 1);
    // The name on this node's replica (created at setup; a restarted
    // node has it in its journal).
    // Or the inode it had at setup, kept open (`set_by_ino`).
    let mut ino = ghost.setup_ino(step.file);
    for _ in 0..200 {
        if ino.is_some() {
            break;
        }
        let Some(h) = same(&cluster, node, inc) else {
            ghost.count(|c| c.abandoned += 1);
            return;
        };
        let (parent, leaf) = super::run::split_name(&h.meta, &name);
        if let Some(i) = h.meta.child_ino(parent, &leaf).ok().flatten() {
            ino = Some(i);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let Some(ino) = ino else {
        failures.lock().unwrap().push(format!(
            "lock client t{thread}: {name} never appeared on node {node}"
        ));
        return;
    };
    let lock = LocalLock {
        owner: thread,
        pid: thread as u32,
        pid_start: 0,
        write: step.mode == LockMode::Exclusive,
        start: 0,
        end: u64::MAX,
    };
    let jitter = thread % 7;
    let t0 = clock.elapsed_ms();
    let deadline = t0 + LOCK_WAIT_CAP_MS;
    // A non-blocking request told to wait gives up at the engine's bound
    // (`ClusterLocks::owner_wait_max`), inside the cap.
    let nb_bound = (cluster.env.config)(node, inc)
        .lock_owner_wait_max_ms()
        .min(LOCK_WAIT_CAP_MS - 1_000);
    let stuck = |h: &NodeHandle| {
        format!(
            "lock client t{thread} on node {node}: {:?} lock on {name} ({ino:#x}) not \
             acquired within {LOCK_WAIT_CAP_MS}ms of simulated time (from t={t0}); held here: \
             {:?}\n  lock trace (last {TRACE_LEN}):\n    {}",
            step.mode,
            h.meta.locks().held(ino),
            ghost.trace().join("\n    ")
        )
    };
    let mut after_grant = false;
    // The fairness ghost's record of this wait (see `WaitGuard`).
    let mut wait: Option<WaitGuard> = None;
    // The step got a grant and its session wait completed.
    let mut fresh_grant = false;
    let mut grant_position = constellation_meta::Position::ZERO;
    let mut regrants = 0u32;
    loop {
        let Some(h) = same(&cluster, node, inc) else {
            ghost.count(|c| c.abandoned += 1);
            return;
        };
        if !step.blocking && clock.elapsed_ms() - t0 > nb_bound {
            ghost.count(|c| c.wait_gave_up += 1);
            return;
        }
        if clock.elapsed_ms() > deadline {
            failures.lock().unwrap().push(stuck(&h));
            return;
        }
        let now = h.clock.now();
        match h.meta.locks().local_set(ino, lock, now) {
            LocalOutcome::Done => break,
            // Plan 43 C7-F F-c: the grant held is lapsing and its owner has
            // not said yet: the request waits for the verdict (the engine's
            // `LocalOutcome::Parked`), it does not ask for a fresh grant.
            LocalOutcome::Parked => {
                ghost.count(|c| c.parked_requests += 1);
                after_grant = false;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            LocalOutcome::Conflict(_) | LocalOutcome::Predecessor => {
                ghost.count(|c| c.local_conflicts += 1);
                after_grant = false;
                if !step.blocking {
                    ghost.count(|c| c.would_block += 1);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10 + jitter)).await;
            }
            LocalOutcome::NeedGrant(mode) => {
                if wait.is_none() && step.blocking {
                    // (A non-blocking request holds no place in a queue:
                    // the owner does not park it, and it may be
                    // overtaken while it waits for a grace to end.)
                    ghost.wait_begin(ino, node, thread, step.mode, t0);
                    wait = Some(WaitGuard {
                        ghost: ghost.clone(),
                        ino,
                        node,
                        thread,
                        clock: clock.clone(),
                        done: false,
                    });
                }
                if after_grant {
                    // `Granted`, yet the grant does not admit the lock: it
                    // lapsed or was recalled meanwhile — or the core
                    // answers `Granted` for a grant `local_set` refuses,
                    // which would spin at no simulated time.
                    ghost.count(|c| c.regrants += 1);
                    regrants += 1;
                    if regrants >= 200 {
                        failures.lock().unwrap().push(format!(
                            "{} [{regrants} Granted answers whose grant local_set refused; \
                             held: {:?}]",
                            stuck(&h),
                            h.meta.locks().held(ino)
                        ));
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1 + jitter)).await;
                }
                after_grant = false;
                // The application's wait so far travels with every round
                // of one call (as the engine's lock call does), so a loser
                // keeps its place in the owner's queue.
                let waited_ms = if step.blocking {
                    clock.elapsed_ms().saturating_sub(t0)
                } else {
                    0
                };
                let rx = h.control(Control::Lock {
                    ino,
                    mode,
                    blocking: step.blocking,
                    waited_ms,
                });
                let answer = await_control(&cluster, node, inc, deadline, rx).await;
                let answer = match answer {
                    Awaited::Answer(a) => a,
                    Awaited::Died => {
                        ghost.count(|c| c.abandoned += 1);
                        return;
                    }
                    Awaited::Late => {
                        failures.lock().unwrap().push(format!(
                            "{} [Control::Lock {{ {mode:?}, blocking: {} }} unanswered]",
                            stuck(&h),
                            step.blocking
                        ));
                        return;
                    }
                };
                match answer {
                    Ok(ControlOk::Lock(LockAnswer::Granted { position })) => {
                        ghost.count(|c| c.granted += 1);
                        ghost.grant_seen(ino, node, mode, clock.elapsed_ms());
                        ghost.note(
                            clock.elapsed_ms(),
                            format!("node {node} t{thread} granted {mode:?} on {ino:#x}"),
                        );
                        // The session wait at the grant's position (polled:
                        // this runtime is single-threaded).
                        // As the FUSE layer does (`cli::locks::granted`):
                        // the grant's position is the session's watermark
                        // for every later read on this node.
                        h.meta.session().raise_observed(position);
                        grant_position = position;
                        let keys = [ReadKey::Ino(ino)];
                        let mut spent = 0;
                        fresh_grant = true;
                        while !h.meta.session_ready_at(&keys, &position) {
                            if spent >= wait_budget_ms || !h.alive() {
                                fresh_grant = false;
                                ghost.count(|c| c.position_timeouts += 1);
                                ghost.note(
                                    clock.elapsed_ms(),
                                    format!(
                                        "node {node} t{thread} gave up waiting for {position:?} \
                                         on {ino:#x} (applied {:?}, observed {:?}, streams \
                                         reached {})",
                                        h.meta.applied_seq().ok(),
                                        h.meta.session().observed(),
                                        h.meta.session().reaches_streams(&position)
                                    ),
                                );
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            spent += 5;
                        }
                        after_grant = true;
                    }
                    Ok(ControlOk::Lock(LockAnswer::WouldBlock)) => {
                        ghost.count(|c| c.would_block += 1);
                        if !step.blocking {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(50 + jitter)).await;
                    }
                    Ok(ControlOk::Lock(LockAnswer::Wait { retry_ms })) => {
                        // The engine's wait (`ClusterLocks::lock_nb_deferred`),
                        // bounded here by the step's cap.
                        ghost.count(|c| c.grace_waits += 1);
                        if step.blocking {
                            failures.lock().unwrap().push(format!(
                                "lock client t{thread} on node {node}: a blocking request \
                                 was told to wait for a grace by the engine's answer"
                            ));
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(retry_ms)).await;
                    }
                    Ok(ControlOk::Lock(LockAnswer::Unavailable)) => {
                        ghost.count(|c| c.unavailable += 1);
                        if !step.blocking {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(200 + jitter)).await;
                    }
                    other => {
                        failures.lock().unwrap().push(format!(
                            "lock client t{thread} on node {node}: unexpected answer to \
                             Control::Lock: {other:?}"
                        ));
                        return;
                    }
                }
            }
        }
    }
    let waited = clock.elapsed_ms() - t0;
    if let Some(mut w) = wait.take() {
        w.done = true;
        ghost.wait_end(ino, node, thread, clock.elapsed_ms(), true);
    }
    ghost.count(|c| {
        c.acquired += 1;
        c.max_wait_ms = c.max_wait_ms.max(waited);
        if node == 1 {
            c.max_wait_owner_ms = c.max_wait_owner_ms.max(waited);
        } else {
            c.max_wait_rest_ms = c.max_wait_rest_ms.max(waited);
        }
    });
    // The critical section.
    let mut entered = false;
    let mut fenced = false;
    let mut wrote = false;
    // A re-entry after a park does not read the previous holder's turn again.
    let mut resumed_once = false;
    for _ in 0..step.ios {
        // Plan 43 C7-F F-c: a grant that is lapsing parks the client (it
        // performs no I/O, so it leaves its critical section at the park's
        // start) until the owner admits a late renewal (it re-enters, after
        // the resume check) or refuses it (fenced, as a lapse always was).
        let mut park_start: Option<u64> = None;
        let (h, ino_fenced, owner_fenced) = loop {
            let Some(h) = same(&cluster, node, inc) else {
                if entered {
                    ghost.leave(ino, node, thread, clock.elapsed_ms(), "node died");
                }
                ghost.count(|c| c.abandoned += 1);
                return;
            };
            // Plan 43 C7b (review of spec commit 7): no thread of a suspended
            // host runs. The client performs no I/O while it is suspended
            // (it leaves its critical section where it notices), and on
            // resume it judges its grant by the clocks it reads then: an
            // honour clock that under-counted the suspend lets it re-enter
            // under a grant its owner has outwaited.
            if h.suspended() {
                if entered {
                    ghost.leave(ino, node, thread, clock.elapsed_ms(), "suspended");
                    entered = false;
                    resumed_once = true;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            let now = h.clock.now();
            let ino_fence = h.meta.locks().fenced(ino, now);
            let owner_fenced = h
                .meta
                .locks()
                .fenced_owners(now)
                .iter()
                .any(|f| f.owner == thread);
            let parked = !ignore_fence
                && !owner_fenced
                && ino_fence != Fence::Fenced
                && (ino_fence == Fence::Parked
                    || h.meta
                        .locks()
                        .parked_owners(now)
                        .iter()
                        .any(|f| f.owner == thread));
            if !parked {
                if let Some(since) = park_start {
                    let end = clock.elapsed_ms();
                    ghost.count(|c| c.max_park_ms = c.max_park_ms.max(end - since));
                    if ino_fence == Fence::Ok && !owner_fenced {
                        ghost.count(|c| c.resumed_ios += 1);
                        ghost.check_resume(ino, node, thread, step.mode, since, end);
                        resumed_once = true;
                    } else {
                        ghost.count(|c| c.park_refused_ios += 1);
                    }
                }
                break (h, ino_fence == Fence::Fenced, owner_fenced);
            }
            let started = *park_start.get_or_insert_with(|| clock.elapsed_ms());
            if started == clock.elapsed_ms() {
                ghost.count(|c| c.parked_ios += 1);
                if entered {
                    ghost.leave(ino, node, thread, started, "parked");
                    entered = false;
                } else {
                    ghost.note(
                        started,
                        format!("node {node} t{thread} parked on {ino:#x} before any I/O"),
                    );
                }
            }
            let cap = (cluster.env.config)(node, inc).lock_owner_wait_max_ms() + PARK_CAP_SLACK_MS;
            if clock.elapsed_ms() > started + cap {
                failures.lock().unwrap().push(format!(
                    "lock client t{thread} on node {node}: parked on {ino:#x} since t={started} \
                     past the bound ({cap} ms); held: {:?}",
                    h.meta.locks().held(ino)
                ));
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        if (ino_fenced || owner_fenced) && !ignore_fence {
            // `AUTHORITY_SIM_LANE_DEBUG=1`: when and where a stall fenced I/O.
            if std::env::var_os("AUTHORITY_SIM_LANE_DEBUG").is_some() {
                eprintln!(
                    "fenced n{node} t{thread} ino={ino:#x} at {} ino_fenced={ino_fenced} \
                     held={:?}",
                    clock.elapsed_ms(),
                    h.meta.locks().held(ino)
                );
            }
            ghost.count(|c| {
                c.fenced_ios += 1;
                if !ino_fenced {
                    c.owner_fenced_ios += 1;
                }
            });
            if entered {
                ghost.leave(ino, node, thread, clock.elapsed_ms(), "fenced");
            } else {
                ghost.note(
                    clock.elapsed_ms(),
                    format!("node {node} t{thread} fenced on {ino:#x} before any I/O"),
                );
            }
            fenced = true;
            break;
        }
        if !entered {
            ghost.enter(ino, node, thread, step.mode, clock.elapsed_ms());
            if fresh_grant && !wrote && !resumed_once {
                let own_epoch = h.view().s3_held.map(|(e, _, _)| e);
                ghost.check_visible(ino, node, clock.elapsed_ms(), &h.meta, own_epoch);
                let data = ghost.data_ino(step.file, ino);
                // A read of the data file waits for the watermark, as a
                // FUSE read would (`session_wait_at` on its keys).
                let keys = [ReadKey::Ino(data)];
                let mut spent = 0;
                let mut ready = true;
                while !h.meta.session_ready_at(&keys, &grant_position) {
                    if spent >= wait_budget_ms || !h.alive() {
                        ready = false;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    spent += 5;
                }
                if ready {
                    let read = constellation_meta::MetaStore::getattr(&*h.meta, data)
                        .ok()
                        .flatten()
                        .map(|a| a.mtime_ns);
                    ghost.turn_read(data, node, clock.elapsed_ms(), read);
                } else {
                    ghost.count(|c| c.position_timeouts += 1);
                }
            }
            entered = true;
            if step.write && !wrote {
                wrote = true;
                // The turn, written under the lock and awaited (as an
                // application's write + fsync would be).
                let data = ghost.data_ino(step.file, ino);
                let rid = h.next_rid();
                // Plan 30 §M14 phase 2: the write carries the fencing
                // token of the client's locks, as a FUSE thread's does,
                // and counts in flight for their release.
                let tag = match h
                    .meta
                    .locks()
                    .owner_tag(&[thread], h.clock.now(), h.clock.id())
                {
                    Ok(tag) => tag,
                    Err(_) => {
                        // Lapsed since the fence check above: fenced.
                        ghost.count(|c| c.fenced_ios += 1);
                        ghost.leave(ino, node, thread, clock.elapsed_ms(), "fenced");
                        entered = false;
                        fenced = true;
                        break;
                    }
                };
                h.meta.locks().tag_begin(&tag, h.clock.now());
                let turn = ghost.take_turn(rid);
                ghost.turn_lock(rid, ino, node, data, turn);
                // The holder is in I/O while the write is awaited, for as
                // long as its grant is honoured; once it lapses the write
                // may land after another node's grant (the fence's
                // flush-start limit: the turn check exempts it as
                // unacknowledged), so it leaves then.
                let mut rx = h.submit_tagged(
                    rid,
                    constellation_meta::MutateOp::Setattr {
                        ino: data,
                        mode: None,
                        uid: None,
                        gid: None,
                        size: None,
                        atime_ns: None,
                        mtime_ns: Some(turn),
                    },
                    tag.clone(),
                );
                let t_write = clock.elapsed_ms();
                let under = h.meta.locks().honoured(ino, h.clock.now()).map(|g| g.id);
                let mut lapsed_once = false;
                let reply = loop {
                    match tokio::time::timeout(Duration::from_millis(5), &mut rx).await {
                        Ok(r) => break Some(r),
                        Err(_) => {
                            let lapsed = same(&cluster, node, inc).is_none_or(|h| {
                                h.meta.locks().fenced(ino, h.clock.now())
                                    != constellation_meta::locks::Fence::Ok
                            });
                            lapsed_once |= lapsed;
                            if lapsed && entered {
                                ghost.leave(
                                    ino,
                                    node,
                                    thread,
                                    clock.elapsed_ms(),
                                    "lapsed during a write",
                                );
                                entered = false;
                            }
                            if clock.elapsed_ms() > t_write + ghost.write_wait_ms() {
                                break None;
                            }
                        }
                    }
                };
                let acked = matches!(
                    reply,
                    Some(Ok(constellation_authority::action::ClientReply::Outcome(
                        constellation_meta::MutateOutcome::Accepted { .. }
                    )))
                );
                if acked {
                    ghost.turn_accepted(turn);
                }
                // Answered definitively (executed or refused), or left in
                // doubt: then its grants wait for its token's window.
                let settled = matches!(
                    reply,
                    Some(Ok(constellation_authority::action::ClientReply::Outcome(
                        constellation_meta::MutateOutcome::Accepted { .. }
                            | constellation_meta::MutateOutcome::Errno(_)
                            | constellation_meta::MutateOutcome::Exists { .. }
                            | constellation_meta::MutateOutcome::Conflict { .. }
                            | constellation_meta::MutateOutcome::LockLapsed
                    )))
                );
                if matches!(
                    reply,
                    Some(Ok(constellation_authority::action::ClientReply::Outcome(
                        constellation_meta::MutateOutcome::LockLapsed
                    )))
                ) {
                    ghost.count(|c| c.token_refused += 1);
                }
                if let Some(h) = same(&cluster, node, inc) {
                    h.meta.locks().tag_end(&tag, !settled, h.clock.now());
                }
                // Acknowledged while the grant it was written under is
                // still honoured (not another thread's later one): the
                // write is the holder's under its lock.
                if acked
                    && !lapsed_once
                    && under.is_some()
                    && same(&cluster, node, inc).is_some_and(|h| {
                        h.meta.locks().honoured(ino, h.clock.now()).map(|g| g.id) == under
                    })
                {
                    ghost.turn_acked(data, turn, node, clock.elapsed_ms(), rid);
                }
                continue;
            }
        }
        ghost.count(|c| c.ios += 1);
        tokio::time::sleep(Duration::from_millis(step.io_ms)).await;
    }
    let Some(h) = same(&cluster, node, inc) else {
        if entered && !fenced {
            ghost.leave(ino, node, thread, clock.elapsed_ms(), "node died");
        }
        ghost.count(|c| c.abandoned += 1);
        return;
    };
    if entered && !fenced {
        ghost.leave(ino, node, thread, clock.elapsed_ms(), "unlock");
        if step.mode == LockMode::Exclusive {
            ghost.released(ino, node, clock.elapsed_ms(), h.meta.session().frontier());
        }
    }
    let idle = h
        .meta
        .locks()
        .local_unlock(ino, thread, 0, u64::MAX, h.clock.now());
    if idle && h.meta.locks().held(ino).is_some_and(|g| g.recalled) {
        ghost.count(|c| c.idle_sent += 1);
        let rx = h.control(Control::LockIdle { ino });
        let deadline = clock.elapsed_ms() + 10_000;
        if let Awaited::Late = await_control(&cluster, node, inc, deadline, rx).await {
            failures.lock().unwrap().push(format!(
                "lock client t{thread} on node {node}: Control::LockIdle on {ino:#x} unanswered"
            ));
        }
    }
}
