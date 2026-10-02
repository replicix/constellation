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
use constellation_meta::locks::{LocalLock, LocalOutcome, LockMode};
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
    /// Reads that found an unacknowledged older turn landed late.
    pub late_unacked_turns: u64,
    /// Fairness: acquisitions made while a client on *another* node had
    /// been waiting for the same lock for more than [`LOCK_FAIR_MS`]
    /// longer (it asked first, by a whole grant window and more, and was
    /// overtaken). Faults reorder legitimately (a paused or partitioned
    /// waiter is skipped); a fault-free run should count none.
    pub overtaken: u64,
}

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
    /// `(the missed turn's rid, the read turn's rid, the report)`.
    #[allow(clippy::type_complexity)]
    stale_reads: Vec<(
        constellation_meta::Rid,
        Option<constellation_meta::Rid>,
        String,
    )>,
    /// Fairness: per inode, the clients waiting for a grant `(node,
    /// thread, since)`.
    waiting: BTreeMap<u64, Vec<(NodeId, u64, u64)>>,
    overtakes: Vec<String>,
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
            if read.is_some_and(|m| g.turn_rids.contains_key(&m) && !g.turns_acked.contains(&m)) {
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
        });
    }

    pub fn leave(&self, ino: u64, node: NodeId, thread: u64, at_ms: u64, why: &str) {
        self.note(
            at_ms,
            format!("node {node} t{thread} leaves I/O on {ino:#x} ({why})"),
        );
        self.with(|g| {
            if let Some(v) = g.in_io.get_mut(&ino) {
                v.retain(|e| !(e.node == node && e.thread == thread));
            }
        });
    }

    /// The process on `node` died: its applications' locks went with it.
    pub fn drop_node(&self, node: NodeId, at_ms: u64) {
        self.note(at_ms, format!("node {node} crashed"));
        self.with(|g| {
            for v in g.in_io.values_mut() {
                v.retain(|e| e.node != node);
            }
            for v in g.waiting.values_mut() {
                v.retain(|e| e.0 != node);
            }
        });
    }

    /// `thread` on `node` starts waiting for a grant on `ino` (its first
    /// `NeedGrant`).
    pub fn wait_begin(&self, ino: u64, node: NodeId, thread: u64, at_ms: u64) {
        self.with(|g| {
            let v = g.waiting.entry(ino).or_default();
            if !v.iter().any(|e| e.0 == node && e.1 == thread) {
                v.push((node, thread, at_ms));
            }
        });
    }

    /// The wait ended (acquired, or given up): counts an overtake when
    /// a client of another node has been waiting [`LOCK_FAIR_MS`] longer.
    pub fn wait_end(&self, ino: u64, node: NodeId, thread: u64, at_ms: u64, acquired: bool) {
        self.with(|g| {
            let seed = g.seed;
            let Some(v) = g.waiting.get_mut(&ino) else {
                return;
            };
            let mine = v
                .iter()
                .find(|e| e.0 == node && e.1 == thread)
                .map(|e| e.2);
            v.retain(|e| !(e.0 == node && e.1 == thread));
            let (Some(since), true) = (mine, acquired) else {
                return;
            };
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
pub fn reset_node(meta: &Meta, inos: &[u64], now_ms: i64) {
    let t = meta.locks();
    t.clear_grants();
    for (ino, _) in t.held_all() {
        t.drop_held_any(ino);
    }
    for ino in inos {
        for l in t.local_locks(*ino) {
            t.local_release_owner(*ino, l.owner, now_ms);
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
    let clock = cluster.env.clock;
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
    let mut ino = None;
    for _ in 0..200 {
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
        if clock.elapsed_ms() > deadline {
            failures.lock().unwrap().push(stuck(&h));
            return;
        }
        let now = h.clock.now().0;
        match h.meta.locks().local_set(ino, lock, now) {
            LocalOutcome::Done => break,
            LocalOutcome::Conflict(_) => {
                ghost.count(|c| c.local_conflicts += 1);
                after_grant = false;
                if !step.blocking {
                    ghost.count(|c| c.would_block += 1);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10 + jitter)).await;
            }
            LocalOutcome::NeedGrant(mode) => {
                if wait.is_none() {
                    ghost.wait_begin(ino, node, thread, t0);
                    wait = Some(WaitGuard {
                        ghost: ghost.clone(),
                        ino,
                        node,
                        thread,
                        clock,
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
                let rx = h.control(Control::Lock {
                    ino,
                    mode,
                    blocking: step.blocking,
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
    });
    // The critical section.
    let mut entered = false;
    let mut fenced = false;
    let mut wrote = false;
    for _ in 0..step.ios {
        let Some(h) = same(&cluster, node, inc) else {
            if entered {
                ghost.leave(ino, node, thread, clock.elapsed_ms(), "node died");
            }
            ghost.count(|c| c.abandoned += 1);
            return;
        };
        let now = h.clock.now().0;
        let ino_fenced = h.meta.locks().fenced(ino, now);
        let owner_fenced = h
            .meta
            .locks()
            .fenced_owners(now)
            .iter()
            .any(|f| f.owner == thread);
        if (ino_fenced || owner_fenced) && !ignore_fence {
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
            if fresh_grant && !wrote {
                let own_epoch = h.view().s3_held.map(|(e, _)| e);
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
                let turn = ghost.take_turn(rid);
                // The holder is in I/O while the write is awaited, for as
                // long as its grant is honoured; once it lapses the write
                // may land after another node's grant (the fence's
                // flush-start limit: the turn check exempts it as
                // unacknowledged), so it leaves then.
                let mut rx = h.submit(
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
                );
                let t_write = clock.elapsed_ms();
                let under = h.meta.locks().honoured(ino, h.clock.now().0).map(|g| g.id);
                let mut lapsed_once = false;
                let reply = loop {
                    match tokio::time::timeout(Duration::from_millis(5), &mut rx).await {
                        Ok(r) => break Some(r),
                        Err(_) => {
                            let lapsed = same(&cluster, node, inc)
                                .is_none_or(|h| h.meta.locks().fenced(ino, h.clock.now().0));
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
                            if clock.elapsed_ms() > t_write + 3_000 {
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
                // Acknowledged while the grant it was written under is
                // still honoured (not another thread's later one): the
                // write is the holder's under its lock.
                if acked
                    && !lapsed_once
                    && under.is_some()
                    && same(&cluster, node, inc).is_some_and(|h| {
                        h.meta.locks().honoured(ino, h.clock.now().0).map(|g| g.id) == under
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
        .local_unlock(ino, thread, 0, u64::MAX, h.clock.now().0);
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
