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
//!   `LockTables::fenced` first — a fenced step is refused (`EIO`) and
//!   the client gives the lock up;
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
}

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
        });
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
                        let keys = [ReadKey::Ino(ino)];
                        let mut spent = 0;
                        while !h.meta.session_ready_at(&keys, &position) {
                            if spent >= wait_budget_ms || !h.alive() {
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
    ghost.count(|c| {
        c.acquired += 1;
        c.max_wait_ms = c.max_wait_ms.max(waited);
    });
    // The critical section.
    let mut entered = false;
    let mut fenced = false;
    for _ in 0..step.ios {
        let Some(h) = same(&cluster, node, inc) else {
            if entered {
                ghost.leave(ino, node, thread, clock.elapsed_ms(), "node died");
            }
            ghost.count(|c| c.abandoned += 1);
            return;
        };
        if h.meta.locks().fenced(ino, h.clock.now().0) && !ignore_fence {
            ghost.count(|c| c.fenced_ios += 1);
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
            entered = true;
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
