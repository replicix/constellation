//! Client histories and the sequential specification they are checked
//! against — the same shape as `constellation-model`'s `namespace.rs`
//! (one flat directory of file names; `create_excl`, `unlink` and, new
//! here, `rename`, `link` and `exchange`), with `stateright`'s
//! `LinearizabilityTester` used as a library.
//!
//! Plan 30 M13 round 3a's tentative-acknowledgement rule is applied at
//! check time: an op whose effect was rolled back at any point (its rid
//! turned up in a replay queue — a stranded shadow, or a deposed holder's
//! own unshipped transaction) is fed to the checker as an operation still
//! in flight on a synthetic thread of its own, so the tester may
//! linearize it wherever its replay lands, or leave it out (a conflict
//! copy), and never requires it to be visible in between.

use constellation_meta::Rid;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NsOp {
    Create(String),
    Unlink(String),
    Rename(String, String),
    /// Plan 45 follow-up: `link(a, b)`. The client resolves `a` on its own
    /// replica and links that inode (`MutateOp::Link` names an inode, as
    /// the kernel's `link` does), so the holder may find it under `a`,
    /// under another name only (a rename it has and the replica lacks),
    /// or gone. The spec therefore speaks for `b` alone: `Ok` makes `b`
    /// present and needs it absent, `EEXIST` needs it present, `ENOENT`
    /// (the resolved inode is gone) is always possible. `a` is kept for
    /// the reports.
    Link(String, String),
    /// Plan 45 follow-up: `renameat2(RENAME_EXCHANGE)` of `a` and `b`
    /// (by name, on the holder): `Ok` needs both present and changes no
    /// name's presence; otherwise `ENOENT`.
    Exchange(String, String),
    /// Plan 45 follow-up: a `Rename(a, b)` the log shows took no effect —
    /// `a` and `b` were names of one inode (`run::same_inode_renames`).
    /// `Ok` needs both present and changes nothing. Never issued by a
    /// client.
    Same(String, String),
    /// Plan 30 §M11: the destination half of a rename in a per-directory
    /// projection (`project_dir`): sets the name present, always `Ok`.
    /// Never issued by a client.
    Put(String),
}

/// Plan 30 §M11: the directory a name lives in (`"d1/x"` → `"d1"`; a
/// bare name lives in the root directory, `""`).
pub fn dir_of(name: &str) -> &str {
    match name.rfind('/') {
        Some(i) => &name[..i],
        None => "",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NsRet {
    Ok,
    Eexist,
    Enoent,
    /// Plan 30 §M11 phase 2b: refused because a designation was
    /// involved (`EROFS` under it from a non-designee reaching the root,
    /// `EXDEV` across its boundary): no effect, explained by the table,
    /// not by the namespace.
    Erofs,
    /// Plan 43 C2 (§5.3 item 5): settled at its deadline — refused for
    /// good, no effect ever; no namespace precondition explains it.
    Abandoned,
}

/// The reference object: the set of names present in the root directory.
#[derive(Clone, Debug, Default)]
pub struct NsSpec {
    pub present: BTreeSet<String>,
}

impl SequentialSpec for NsSpec {
    type Op = NsOp;
    type Ret = NsRet;

    fn invoke(&mut self, op: &NsOp) -> NsRet {
        match op {
            NsOp::Create(n) => {
                if self.present.contains(n) {
                    NsRet::Eexist
                } else {
                    self.present.insert(n.clone());
                    NsRet::Ok
                }
            }
            NsOp::Unlink(n) => {
                if self.present.remove(n) {
                    NsRet::Ok
                } else {
                    NsRet::Enoent
                }
            }
            NsOp::Rename(a, b) => {
                if !self.present.remove(a) {
                    NsRet::Enoent
                } else {
                    self.present.insert(b.clone());
                    NsRet::Ok
                }
            }
            NsOp::Put(n) => {
                self.present.insert(n.clone());
                NsRet::Ok
            }
            // The deterministic choice (in-flight ops, the log's own
            // successes): the link took effect if it could.
            NsOp::Link(_, b) => {
                if self.present.contains(b) {
                    NsRet::Eexist
                } else {
                    self.present.insert(b.clone());
                    NsRet::Ok
                }
            }
            NsOp::Exchange(a, b) | NsOp::Same(a, b) => {
                if self.present.contains(a) && self.present.contains(b) {
                    NsRet::Ok
                } else {
                    NsRet::Enoent
                }
            }
        }
    }

    /// `Link` is the one op with more than one valid answer
    /// ([`NsOp::Link`]): `ENOENT` for an inode the holder no longer has.
    fn is_valid_step(&mut self, op: &NsOp, ret: &NsRet) -> bool {
        match (op, ret) {
            (NsOp::Link(..), NsRet::Enoent) => true,
            _ => self.invoke(op) == *ret,
        }
    }
}

/// Plan 30 §M11: the history restricted to directory `dir`, for the
/// per-key checks under delegation (the log orders keys of different
/// owners independently; only ops sharing a directory are ordered by
/// real time). An op wholly in another directory disappears; a rename
/// across directories becomes its `Unlink` half here (with the rename's
/// return) or its `Put` half (present only when the rename returned
/// `Ok`, or has not returned yet).
pub fn project_dir(events: &[HistEvt], dir: &str) -> Vec<HistEvt> {
    use std::collections::HashMap;
    let mut returns: HashMap<Rid, NsRet> = HashMap::new();
    for e in events {
        if let HistEvt::Return { rid, ret, .. } = e {
            returns.insert(*rid, *ret);
        }
    }
    let mut out = Vec::new();
    // Which projected op each rid became (so its return maps the same way).
    let mut projected: HashMap<Rid, NsOp> = HashMap::new();
    for e in events {
        match e {
            HistEvt::Invoke { thread, rid, op } => {
                let p = match op {
                    NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) if dir_of(n) == dir => {
                        Some(op.clone())
                    }
                    NsOp::Rename(a, b) if dir_of(a) == dir && dir_of(b) == dir => Some(op.clone()),
                    NsOp::Rename(a, _) if dir_of(a) == dir => Some(NsOp::Unlink(a.clone())),
                    NsOp::Rename(_, b) if dir_of(b) == dir => {
                        if returns.get(rid).is_some_and(|r| *r != NsRet::Ok) {
                            None
                        } else {
                            Some(NsOp::Put(b.clone()))
                        }
                    }
                    // The spec reads `b` alone.
                    NsOp::Link(_, b) if dir_of(b) == dir => Some(op.clone()),
                    // Changes no presence: kept whole or not at all.
                    NsOp::Exchange(a, b) | NsOp::Same(a, b)
                        if dir_of(a) == dir && dir_of(b) == dir =>
                    {
                        Some(op.clone())
                    }
                    _ => None,
                };
                if let Some(p) = p {
                    projected.insert(*rid, p.clone());
                    out.push(HistEvt::Invoke {
                        thread: *thread,
                        rid: *rid,
                        op: p,
                    });
                }
            }
            HistEvt::Return { thread, rid, ret } => {
                if let Some(p) = projected.get(rid) {
                    let ret = match p {
                        NsOp::Put(_) => NsRet::Ok,
                        _ => *ret,
                    };
                    out.push(HistEvt::Return {
                        thread: *thread,
                        rid: *rid,
                        ret,
                    });
                }
            }
        }
    }
    out
}

/// Every directory the history touches.
pub fn dirs_of(events: &[HistEvt]) -> Vec<String> {
    let mut dirs: Vec<String> = Vec::new();
    for e in events {
        if let HistEvt::Invoke { op, .. } = e {
            let names: Vec<&str> = match op {
                NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => vec![n],
                NsOp::Rename(a, b) | NsOp::Link(a, b) | NsOp::Exchange(a, b) | NsOp::Same(a, b) => {
                    vec![a, b]
                }
            };
            for n in names {
                let d = dir_of(n).to_string();
                if !dirs.contains(&d) {
                    dirs.push(d);
                }
            }
        }
    }
    dirs.sort();
    dirs
}

pub type ThreadId = u64;

#[derive(Clone, Debug)]
pub enum HistEvt {
    Invoke {
        thread: ThreadId,
        rid: Rid,
        op: NsOp,
    },
    Return {
        thread: ThreadId,
        rid: Rid,
        ret: NsRet,
    },
}

/// Plan 30 §M6: one client read of a name from the local replica.
#[derive(Clone, Debug)]
pub struct ReadEvt {
    pub node: u64,
    pub incarnation: u32,
    pub thread: ThreadId,
    pub name: String,
    pub present: bool,
    /// Ticks (the history's real-time order) at invoke and return.
    pub inv: u64,
    pub ret: u64,
    /// The session wait ran out and the read answered degraded.
    pub timed_out: bool,
    /// ADR-57: a pending acknowledged row (a hold) touched the read's keys
    /// at some point of the read.
    pub held: bool,
    /// ... and the read was answered while it still did, or after its hold
    /// ran out without a release: the answer lacks an acknowledged row.
    pub held_served: bool,
    /// ADR-57: the read waited the read hold span and was refused (`EIO`):
    /// it answered nothing, and is left out of the histories.
    pub unserved: bool,
    /// How the read's floor was obtained: a read delegation the node held,
    /// a ReadIndex answer, a degraded one, or none (a bounded mount).
    pub path: &'static str,
}

#[derive(Default)]
pub struct History {
    events: Mutex<Vec<HistEvt>>,
    /// Plan 30 §M6: the tick of each event in `events` (same index), on
    /// the clock reads share.
    ticks: Mutex<Vec<u64>>,
    reads: Mutex<Vec<ReadEvt>>,
    /// Plan 46 M1.3 step 13: strict reads served under a stream holder's read
    /// delegation `(node, gen, true elapsed ms)`.
    deleg_reads: Mutex<Vec<(u64, u64, u64)>>,
    clock: std::sync::atomic::AtomicU64,
}

impl History {
    fn push(&self, e: HistEvt) {
        let tick = self.tick();
        let mut events = self.events.lock().unwrap();
        events.push(e);
        self.ticks.lock().unwrap().push(tick);
    }

    /// A strict read on `node` was served under a read delegation of stream
    /// `gen`'s holder at `ms`.
    pub fn deleg_read(&self, node: u64, gen: u64, ms: u64) {
        self.deleg_reads.lock().unwrap().push((node, gen, ms));
    }

    pub fn deleg_reads(&self) -> Vec<(u64, u64, u64)> {
        self.deleg_reads.lock().unwrap().clone()
    }

    /// The next tick of the shared real-time order.
    pub fn tick(&self) -> u64 {
        self.clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn invoke(&self, thread: ThreadId, rid: Rid, op: NsOp) {
        self.push(HistEvt::Invoke { thread, rid, op });
    }

    pub fn ret(&self, thread: ThreadId, rid: Rid, ret: NsRet) {
        self.push(HistEvt::Return { thread, rid, ret });
    }

    pub fn read(&self, r: ReadEvt) {
        self.reads.lock().unwrap().push(r);
    }

    pub fn reads(&self) -> Vec<ReadEvt> {
        self.reads.lock().unwrap().clone()
    }

    pub fn ticks(&self) -> Vec<u64> {
        self.ticks.lock().unwrap().clone()
    }

    pub fn events(&self) -> Vec<HistEvt> {
        self.events.lock().unwrap().clone()
    }

    /// Ops that returned, with their result.
    pub fn returned(&self) -> Vec<(Rid, NsRet)> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                HistEvt::Return { rid, ret, .. } => Some((rid, ret)),
                _ => None,
            })
            .collect()
    }

    pub fn invoked(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| matches!(e, HistEvt::Invoke { .. }))
            .count()
    }
}

/// Thread ids above this are synthetic (one per tentative op).
const TENTATIVE_THREAD_BASE: ThreadId = 1 << 40;

/// Check the history for linearizability against [`NsSpec`], treating
/// `tentative` rids per the module doc. Returns the reason on failure.
///
/// Stateright's `LinearizabilityTester` validates the history (a thread
/// invokes only after its previous op returned); the search is
/// [`linearizable`]'s, which memoizes what it explored. The tester's own
/// search (`is_consistent`) keeps no memory of the states it has seen and
/// is exponential in the ops that overlap: `far` seed 1498 with the
/// lease-reads knob (72 events, 11 ops open at once, 6 in flight forever)
/// did not finish in 20 minutes. Both decide the same question
/// (`the_memoized_search_agrees_with_stateright`).
pub fn check_linearizable(events: &[HistEvt], tentative: &HashSet<Rid>) -> Result<(), String> {
    check_linearizable_with(events, tentative, false)
}

fn check_linearizable_with(
    events: &[HistEvt],
    tentative: &HashSet<Rid>,
    stateright_search: bool,
) -> Result<(), String> {
    // Plan 43 C2: a settled op had no effect and never will (the
    // exactly-once check fails one the log completes): left out.
    let abandoned: HashSet<Rid> = events
        .iter()
        .filter_map(|e| match e {
            HistEvt::Return {
                rid,
                ret: NsRet::Abandoned,
                ..
            } => Some(*rid),
            _ => None,
        })
        .collect();
    let mut tester = LinearizabilityTester::<ThreadId, NsSpec>::new(NsSpec::default());
    // The same history for the search: each op, its invocation's index,
    // and its return (index, value), `None` while in flight (forever, for
    // a tentative op).
    let mut ops: Vec<LinOp> = Vec::new();
    let mut open: std::collections::HashMap<ThreadId, usize> = std::collections::HashMap::new();
    for (i, evt) in events.iter().enumerate() {
        let r = match evt {
            HistEvt::Invoke { rid, .. } | HistEvt::Return { rid, .. }
                if abandoned.contains(rid) =>
            {
                continue
            }
            HistEvt::Invoke { thread, rid, op } => {
                let thread = if tentative.contains(rid) {
                    TENTATIVE_THREAD_BASE + i as u64
                } else {
                    *thread
                };
                open.insert(thread, ops.len());
                ops.push(LinOp {
                    op: op.clone(),
                    invoked: i,
                    ret: None,
                });
                tester.on_invoke(thread, op.clone()).map(|_| ())
            }
            HistEvt::Return { thread, rid, ret } => {
                if tentative.contains(rid) {
                    // Left in flight forever: may linearize anywhere after
                    // its invocation, or not at all.
                    Ok(())
                } else {
                    if let Some(k) = open.remove(thread) {
                        ops[k].ret = Some((i, *ret));
                    }
                    tester.on_return(*thread, *ret).map(|_| ())
                }
            }
        };
        if let Err(e) = r {
            return Err(format!(
                "history is not a valid concurrent history at event {i}: {e}"
            ));
        }
    }
    let consistent = if stateright_search {
        tester.is_consistent()
    } else {
        linearizable(&ops)?
    };
    if consistent {
        Ok(())
    } else {
        Err("history is not linearizable".to_string())
    }
}

/// One op of a history, for [`linearizable`].
struct LinOp {
    op: NsOp,
    /// The index of its invocation in the history.
    invoked: usize,
    /// The index of its return and the value returned; `None`: in flight
    /// (it may take effect anywhere after its invocation, or never).
    ret: Option<(usize, NsRet)>,
}

/// The Wing and Gong search with Lowe's memoization: an order of the ops
/// that respects real time (an op goes next only if no op still to place
/// returned before it was invoked) in which every returned op's value is
/// the spec's answer, and every op in flight either takes effect (the
/// spec's own answer, as stateright applies one) or never does. A
/// configuration (the ops placed, the spec's state) is explored once.
fn linearizable(ops: &[LinOp]) -> Result<bool, String> {
    if ops.len() > 128 {
        return Err(format!(
            "{} ops: more than the linearizability search's bound of 128",
            ops.len()
        ));
    }
    let returned: u128 = ops
        .iter()
        .enumerate()
        .filter(|(_, o)| o.ret.is_some())
        .fold(0, |m, (k, _)| m | (1u128 << k));
    let mut seen: HashSet<(u128, BTreeSet<String>)> = HashSet::new();
    fn go(
        ops: &[LinOp],
        returned: u128,
        placed: u128,
        state: &NsSpec,
        seen: &mut HashSet<(u128, BTreeSet<String>)>,
    ) -> bool {
        if placed & returned == returned {
            return true;
        }
        // The first return of an op still to place: nothing invoked after
        // it can go before it.
        let bound = ops
            .iter()
            .enumerate()
            .filter(|(k, _)| placed & (1u128 << k) == 0)
            .filter_map(|(_, o)| o.ret.map(|(at, _)| at))
            .min()
            .unwrap_or(usize::MAX);
        for (k, o) in ops.iter().enumerate() {
            if placed & (1u128 << k) != 0 || o.invoked > bound {
                continue;
            }
            let mut next = state.clone();
            match &o.ret {
                Some((_, ret)) => {
                    if !next.is_valid_step(&o.op, ret) {
                        continue;
                    }
                }
                None => {
                    next.invoke(&o.op);
                }
            }
            let placed = placed | (1u128 << k);
            if !seen.insert((placed, next.present.clone())) {
                continue;
            }
            if go(ops, returned, placed, &next, seen) {
                return true;
            }
        }
        false
    }
    Ok(go(ops, returned, 0, &NsSpec::default(), &mut seen))
}

/// Plan 45 §12 O2: each completed rid's journal row, as the shipping
/// tenure numbered it: `(epoch, node, journal seq)` (`run::journal_order`).
pub type JournalOrder = std::collections::HashMap<Rid, (u64, u64, u64)>;

/// The successes' linearization order: the log's, except that one
/// tenure's run of rows is taken in the order it journaled them. The ship
/// plan sends a tenure's transactions out of journal order only around
/// one deferred or held on a pending chunk (`store::held`, plan 30
/// §M4/§M7), and what it sends ahead touches none of the deferred keys:
/// the two commute, and the journal order is the holder's execution
/// order, the one its answers were given in. That is write-back's
/// documented relaxation ("unrelated changes the same node makes
/// afterwards can become visible first", durability-and-failover): a
/// later op, of the same node or invoked after the deferred one
/// returned, may precede it in the log. Within one directory every
/// entry change also writes the directory's inode, so it takes a link or
/// a rename into another directory to show (plan 45 §12 O2: `link f3
/// x/f0` deferred behind `f3`'s pending write, `create f0` shipped
/// first, `flex+links` seed 9). Rids without a journal row
/// ([`JournalOrder`]) keep their log position and end a run.
pub fn witness_order(
    completed_at: &std::collections::HashMap<Rid, (u64, usize)>,
    journal: &JournalOrder,
) -> Vec<Rid> {
    let mut by_log: Vec<(Rid, (u64, usize))> = completed_at.iter().map(|(r, p)| (*r, *p)).collect();
    by_log.sort_by_key(|(_, p)| *p);
    let mut out: Vec<Rid> = Vec::with_capacity(by_log.len());
    let mut run: Vec<(u64, Rid)> = Vec::new();
    let mut tenure: Option<(u64, u64)> = None;
    let flush = |run: &mut Vec<(u64, Rid)>, out: &mut Vec<Rid>| {
        run.sort_by_key(|(j, _)| *j);
        out.extend(run.drain(..).map(|(_, r)| r));
    };
    for (rid, _) in by_log {
        match journal.get(&rid) {
            Some((e, n, j)) => {
                if tenure != Some((*e, *n)) {
                    flush(&mut run, &mut out);
                    tenure = Some((*e, *n));
                }
                run.push((*j, rid));
            }
            None => {
                flush(&mut run, &mut out);
                tenure = None;
                out.push(rid);
            }
        }
    }
    flush(&mut run, &mut out);
    out
}

/// The most tentative ops (invoked before a refusal returned) the witnessed
/// check tries every subset of when it explains a refusal by tentative
/// effects.
const TENTATIVE_SUBSET_MAX: usize = 14;

/// Linearizability with the log as the witness. Every op that took effect
/// has a `Completed { rid }` at one log position, and the log is the
/// holder's execution order, so the successes' linearization order is
/// known; refusals (`EEXIST`/`ENOENT`) are pure reads and only need *some*
/// point in their real-time window at which the spec refuses the same
/// way. That makes the check exact for this spec and polynomial, where
/// the generic tester's search is exponential in overlapping ops (a
/// crash-heavy run leaves many tentative ops in flight forever, and the
/// tester never finishes). Checks:
/// 1. real time: a success that returned before another was invoked
///    precedes it in the log (tentative ops only need to follow what
///    returned before their invocation), in [`witness_order`];
/// 2. values: running the spec in log order gives every non-tentative
///    success `Ok`;
/// 3. refusals: an op refused with `ret` (and absent from the log) has a
///    spec state in its window — after every success that returned before
///    it was invoked, before every success invoked after it returned —
///    that returns `ret`. A refusal with no such state is re-checked with
///    the tentative ops invoked before it returned applied on top of the
///    window's states: a holder that later died may have evaluated it
///    against an effect it had acked but not shipped, which its requester
///    replayed later (or gave up on). That is plan 30 §1.2's L2 window
///    seen by a third party, which M9 closes; it is counted as
///    [`Witness::observed_tentative`], not failed.
pub fn check_linearizable_witnessed(
    events: &[HistEvt],
    tentative: &HashSet<Rid>,
    dropped: &HashSet<Rid>,
    completed_at: &std::collections::HashMap<Rid, (u64, usize)>,
    journal: &JournalOrder,
) -> Result<Witness, String> {
    use std::collections::HashMap;
    let mut invoke_at: HashMap<Rid, (usize, NsOp)> = HashMap::new();
    let mut return_at: HashMap<Rid, (usize, NsRet)> = HashMap::new();
    for (i, e) in events.iter().enumerate() {
        match e {
            HistEvt::Invoke { rid, op, .. } => {
                invoke_at.insert(*rid, (i, op.clone()));
            }
            HistEvt::Return { rid, ret, .. } => {
                return_at.insert(*rid, (i, *ret));
            }
        }
    }
    // The successes in log order (a tenure's run in journal order).
    let successes: Vec<Rid> = witness_order(completed_at, journal)
        .into_iter()
        .filter(|rid| invoke_at.contains_key(rid))
        .collect();
    let index_of: HashMap<Rid, usize> = successes
        .iter()
        .enumerate()
        .map(|(i, rid)| (*rid, i))
        .collect();
    // 1. Real-time order among successes, in log order: a failing run
    // names the same pair every time (`index_of` is a `HashMap`, whose
    // order is the process's).
    for (ia, a) in successes.iter().enumerate() {
        let Some((ret_a, _)) = return_at.get(a) else {
            continue;
        };
        if tentative.contains(a) {
            continue;
        }
        for b in &successes[..ia] {
            let (inv_b, _) = &invoke_at[b];
            if ret_a < inv_b {
                return Err(format!(
                    "rid {a:?} returned before rid {b:?} was invoked but follows it in the log"
                ));
            }
        }
    }
    // 2. Values along the log order; the spec states after each success.
    let mut spec = NsSpec::default();
    let mut states: Vec<NsSpec> = vec![spec.clone()];
    let mut trace: Vec<String> = Vec::new();
    for rid in &successes {
        let (_, op) = &invoke_at[rid];
        let before = spec.present.clone();
        let ret = spec.invoke(op);
        trace.push(format!(
            "{rid:?} {op:?}{} -> {ret:?}",
            if tentative.contains(rid) { "*" } else { "" }
        ));
        if ret != NsRet::Ok && !tentative.contains(rid) {
            return Err(format!(
                "rid {rid:?} ({op:?}) took effect in the log but the spec refuses it there: {ret:?} \
                 (present: {before:?}; the log's successes before it: {})",
                trace.join("; ")
            ));
        }
        states.push(spec.clone());
    }
    // 3. Refusals: some state in the window returns the same refusal.
    let mut witness = Witness::default();
    for (rid, (ret_i, ret)) in &return_at {
        if *ret == NsRet::Ok
            || *ret == NsRet::Erofs
            || *ret == NsRet::Abandoned
            || tentative.contains(rid)
            || index_of.contains_key(rid)
        {
            continue;
        }
        let (inv_i, op) = &invoke_at[rid];
        // After every success that returned before this op was invoked
        // (a tentative op's return is not binding: it may have landed
        // through its replay, later in the log).
        let lo = index_of
            .iter()
            .filter(|(b, _)| {
                !tentative.contains(*b) && return_at.get(*b).is_some_and(|(r, _)| r < inv_i)
            })
            .map(|(_, ib)| ib + 1)
            .max()
            .unwrap_or(0);
        // Before every success invoked after this op returned.
        let hi = index_of
            .iter()
            .filter(|(b, _)| invoke_at[*b].0 > *ret_i)
            .map(|(_, ib)| *ib)
            .min()
            .unwrap_or(states.len() - 1);
        let hi = hi.min(states.len() - 1);
        let feasible = (lo..=hi).any(|i| states[i].clone().is_valid_step(op, ret));
        if feasible {
            continue;
        }
        // Tentative effects the holder may have shown this op before they
        // were rolled back: every tentative op invoked before it returned,
        // in invocation order, alone or together; and the ones an
        // operator's drop rolled back also as any subset of them (a chain:
        // an op acked on top of another's unshipped effect, and later ones
        // the holder had not executed yet when it refused).
        // (A `Same` is a rename the log shows took no effect, because by the
        // time it was replayed its two names were of one inode; what the
        // refusing holder saw was its first run, which moved the name.)
        let first_runs: HashMap<Rid, NsOp> = invoke_at
            .iter()
            .filter_map(|(t, (_, op_t))| match op_t {
                NsOp::Same(a, b) => Some((*t, NsOp::Rename(a.clone(), b.clone()))),
                _ => None,
            })
            .collect();
        let mut early: Vec<(usize, &NsOp, bool)> = invoke_at
            .iter()
            .filter(|(t, (inv_t, _))| tentative.contains(*t) && inv_t < ret_i)
            .map(|(t, (inv_t, op_t))| {
                (
                    *inv_t,
                    first_runs.get(t).unwrap_or(op_t),
                    dropped.contains(t),
                )
            })
            .collect();
        early.sort_by_key(|(i, _, _)| *i);
        // The ones `repair drop-held` rolled back, and the other tentative
        // ones (a deposition's), each in invocation order.
        let by_drop: Vec<(usize, &NsOp)> = early
            .iter()
            .filter(|(_, _, d)| *d)
            .map(|(i, t, _)| (*i, *t))
            .collect();
        let others: Vec<(usize, &NsOp)> = early
            .iter()
            .filter(|(_, _, d)| !*d)
            .map(|(i, t, _)| (*i, *t))
            .collect();
        let explained = (lo..=hi).any(|i| {
            let all = {
                let mut st = states[i].clone();
                for (_, t, _) in &early {
                    st.invoke(t);
                }
                st.is_valid_step(op, ret)
            };
            // A subset of the ops an operator's drop rolled back
            // (`dropped`), with the other tentative ones (a deposition's)
            // either all applied or none, in any order when few (the
            // sequencer executed them in the order they reached it, not
            // the order they were invoked in): the holder executed the
            // ones it had when it refused, and not the ones that reached
            // it later — a chain the drop rolls back whole and replays by
            // rid, each in its own turn. The other tentative ops are tried
            // all together or one alone, never as a subset of their own: a
            // long list of them would explain anything.
            let subset = || {
                by_drop.len() <= TENTATIVE_SUBSET_MAX
                    && (1u32..(1u32 << by_drop.len())).any(|mask| {
                        [false, true].into_iter().any(|with_others| {
                            let chain: Vec<&NsOp> = by_drop
                                .iter()
                                .enumerate()
                                .filter(|(k, _)| mask & (1 << k) != 0)
                                .map(|(_, (_, t))| *t)
                                .collect();
                            // (The other tentative ones, if any, are
                            // in force first, in invocation order.)
                            let mut base = states[i].clone();
                            if with_others {
                                for (_, t) in &others {
                                    base.invoke(t);
                                }
                            }
                            any_order(&chain, &base, op, ret)
                        })
                    })
            };
            all || early.iter().any(|(_, t, _)| {
                let mut st = states[i].clone();
                st.invoke(t);
                st.is_valid_step(op, ret)
            }) || subset()
        });
        if explained {
            witness.observed_tentative += 1;
            witness.observers.insert(*rid);
            // Whether the ops an operator's drop rolled back explain it
            // alone (none of a deposition's): the operator's action, not
            // the protocol's, took the effect it saw away.
            let by_drop_alone = !by_drop.is_empty()
                && by_drop.len() <= TENTATIVE_SUBSET_MAX
                && (lo..=hi).any(|i| {
                    (1u32..(1u32 << by_drop.len())).any(|mask| {
                        let chain: Vec<&NsOp> = by_drop
                            .iter()
                            .enumerate()
                            .filter(|(k, _)| mask & (1 << k) != 0)
                            .map(|(_, (_, t))| *t)
                            .collect();
                        any_order(&chain, &states[i], op, ret)
                    })
                });
            if by_drop_alone {
                witness.observed_dropped += 1;
            }
            continue;
        }
        let at = |i: usize| -> String {
            index_of
                .iter()
                .find(|(_, ib)| **ib == i)
                .map(|(b, _)| format!("{b:?} {:?}", invoke_at[b].1))
                .unwrap_or_default()
        };
        return Err(format!(
            "rid {rid:?} ({op:?}) returned {ret:?} but no state in its window \
             (log positions {lo}..={hi}; before: {}; after: {}) refuses it that way, \
             even with tentative effects applied (window states: {}; tentative ops \
             invoked before it returned: {})",
            at(lo.saturating_sub(1)),
            at(hi),
            (lo..=hi)
                .map(|i| format!("{:?}", states[i].present))
                .collect::<Vec<_>>()
                .join(" "),
            early
                .iter()
                .map(|(_, t, d)| format!("{t:?}{}", if *d { "*" } else { "" }))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(witness)
}

/// Whether `ops` applied to `base` in some order (every order when there
/// are at most [`TENTATIVE_ORDER_MAX`] of them, else the given one and its
/// reverse) leave a state in which `op` returns `ret`.
fn any_order(ops: &[&NsOp], base: &NsSpec, op: &NsOp, ret: &NsRet) -> bool {
    fn go(left: &mut Vec<&NsOp>, st: &NsSpec, op: &NsOp, ret: &NsRet) -> bool {
        if st.clone().is_valid_step(op, ret) {
            return true;
        }
        for k in 0..left.len() {
            let t = left.remove(k);
            let mut next = st.clone();
            next.invoke(t);
            let found = go(left, &next, op, ret);
            left.insert(k, t);
            if found {
                return true;
            }
        }
        false
    }
    if ops.len() <= TENTATIVE_ORDER_MAX {
        // (Every prefix of every order is tried too: a state reached on the
        // way is a subset of its own.)
        return go(&mut ops.to_vec(), base, op, ret);
    }
    let apply = |seq: &mut dyn Iterator<Item = &&NsOp>| {
        let mut st = base.clone();
        for t in seq {
            st.invoke(t);
        }
        st.is_valid_step(op, ret)
    };
    apply(&mut ops.iter()) || apply(&mut ops.iter().rev())
}

/// The most tentative ops the witnessed check tries in every order.
const TENTATIVE_ORDER_MAX: usize = 5;

/// What the witnessed check saw besides a pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Witness {
    /// Refusals explained only by an acked-then-rolled-back effect the
    /// refusing holder still had at the time (the L2 window).
    pub observed_tentative: usize,
    /// Of them, the refusals the ops `repair drop-held` rolled back explain
    /// on their own: the operator's action took the effect they saw away
    /// (never acknowledged by the protocol's rule), so `strict_durability`
    /// does not count them.
    pub observed_dropped: usize,
    /// Their rids. The generic tester ([`check_linearizable`]) must treat
    /// them as tentative too: under `Local` their answer depended on an
    /// effect the log placed later (or never), exactly like the tentative
    /// op's own acknowledgement — without this the two checkers enforced
    /// different contracts, and a small history (the only kind the
    /// generic tester runs on) with one such refusal failed as "not
    /// linearizable" (`locks-faults` seed 195356).
    pub observers: HashSet<Rid>,
}

/// Plan 30 §M6/§M11 (EC2 campaign 7, finding B-1): the log keeps every
/// node's program order across directories and owners. A success that
/// returned to a node before the same node invoked another op precedes
/// it in the log, whichever sequencer executed each: a delegate waits
/// for the requester's `deps` before it executes, and the root parks an
/// execution until the delegate streams its `deps` name are appended.
/// So a replica that tails the log — and whatever is installed ahead of
/// it as a contiguous prefix — never shows the later effect without the
/// earlier one (git: a ref or reflog line without the objects it
/// names). `check_linearizable_witnessed` checks real time within a
/// directory only (different owners order their keys independently);
/// this is the cross-directory rule that still holds: one node's own
/// order. Tentative ops (rolled back, replayed by rid) are exempt. The
/// order is [`witness_order`]'s: a tenure's rows as it journaled them.
pub fn check_session_order(
    events: &[HistEvt],
    tentative: &HashSet<Rid>,
    completed_at: &std::collections::HashMap<Rid, (u64, usize)>,
    journal: &JournalOrder,
) -> Result<(), String> {
    use std::collections::HashMap;
    let mut invoke_at: HashMap<Rid, usize> = HashMap::new();
    let mut return_at: HashMap<Rid, usize> = HashMap::new();
    for (i, e) in events.iter().enumerate() {
        match e {
            HistEvt::Invoke { rid, .. } => {
                invoke_at.insert(*rid, i);
            }
            HistEvt::Return { rid, .. } => {
                return_at.insert(*rid, i);
            }
        }
    }
    // Per session (node incarnation), its non-tentative successes:
    // (return index, invoke index, witness position, rid).
    type Op = (usize, usize, (u64, usize), Rid);
    let order: HashMap<Rid, (u64, usize)> = witness_order(completed_at, journal)
        .into_iter()
        .enumerate()
        .map(|(i, rid)| (rid, (i as u64, 0)))
        .collect();
    // In session order, for the same reason as the real-time check's.
    let mut sessions: std::collections::BTreeMap<(u64, u32), Vec<Op>> =
        std::collections::BTreeMap::new();
    for (rid, pos) in &order {
        if tentative.contains(rid) {
            continue;
        }
        let (Some(inv), Some(ret)) = (invoke_at.get(rid), return_at.get(rid)) else {
            continue;
        };
        sessions
            .entry((rid.node, rid.incarnation))
            .or_default()
            .push((*ret, *inv, *pos, *rid));
    }
    for ops in sessions.values_mut() {
        ops.sort_by_key(|(ret, ..)| *ret);
        let mut by_invoke = ops.clone();
        by_invoke.sort_by_key(|(_, inv, ..)| *inv);
        // Of the ops returned before the current one was invoked, the
        // one furthest along the log.
        let mut furthest: Option<((u64, usize), Rid)> = None;
        let mut returned = 0;
        for (_, inv, pos, rid) in &by_invoke {
            while returned < ops.len() && ops[returned].0 < *inv {
                let (_, _, p, r) = ops[returned];
                if furthest.is_none_or(|(fp, _)| p > fp) {
                    furthest = Some((p, r));
                }
                returned += 1;
            }
            if let Some((fp, fr)) = furthest {
                if fp > *pos {
                    return Err(format!(
                        "rid {fr:?} returned to node {} before it invoked rid {rid:?}, but follows it in the log ({fp:?} after {pos:?}): the log breaks the node's program order",
                        rid.node
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Plan 46 M0.3: one overwrite a client attempted (`Step::TruncCommit`): the
/// size-only `Setattr` of an `O_TRUNC` open (`truncated`: it was
/// acknowledged), then the `SetManifest` of the close (`manifest`: the one it
/// named, `None` when it was not attempted; `committed`: it was
/// acknowledged).
#[derive(Debug, Clone)]
pub struct TruncRec {
    pub name: String,
    pub truncated: bool,
    pub committed: bool,
    pub manifest: Option<Vec<u8>>,
}

/// One op of an overwrite: whether it is the commit, whether it was
/// acknowledged, and the manifest the commit named.
type OverwriteOp<'a> = (bool, bool, Option<&'a Vec<u8>>);

/// A file as the converged replicas hold it: its size and its manifest.
pub type FinalFile = (u64, Option<Vec<u8>>);

/// The 2026-10-16 loss's check (`tloss-two-node`, plan 43 C2): each file's
/// final size and manifest are those of the last acknowledged op of its
/// overwrites, or of an op attempted after it. (A file only ever created
/// and overwritten by one client, in sequence: the truncate leaves size 0,
/// the commit leaves `chunk_size` bytes and the manifest it named. An
/// acknowledged commit that the run then ends without, a file left empty
/// by a truncate that replayed after its commit, is the loss.) Returns the
/// number of files checked.
pub fn check_trunc_commits(
    recs: &[TruncRec],
    finals: &std::collections::BTreeMap<String, Option<FinalFile>>,
    chunk_size: u64,
) -> Result<usize, String> {
    let mut by_name: std::collections::BTreeMap<&str, Vec<OverwriteOp>> = Default::default();
    for r in recs {
        let ops = by_name.entry(r.name.as_str()).or_default();
        ops.push((false, r.truncated, None));
        if r.manifest.is_some() {
            ops.push((true, r.committed, r.manifest.as_ref()));
        }
    }
    for (name, ops) in &by_name {
        let Some(fin) = finals.get(*name) else {
            continue;
        };
        let Some((size, manifest)) = fin else {
            if ops.iter().any(|o| o.1) {
                return Err(format!(
                    "{name}: an acknowledged overwrite, and the file is gone"
                ));
            }
            continue;
        };
        // The states the file may end in: after the last acknowledged op or
        // any op attempted after it (or, with none acknowledged, as created).
        let from = ops.iter().rposition(|o| o.1).unwrap_or(0);
        let ok = ops[from..].iter().any(|(commit, _, m)| {
            if *commit {
                *size == chunk_size && manifest.as_ref() == *m
            } else {
                *size == 0
            }
        }) || (ops.iter().all(|o| !o.1) && *size == 0);
        if !ok {
            let last = ops.iter().rposition(|o| o.1);
            let hash = |m: &Option<Vec<u8>>| {
                m.as_ref().map_or("none".to_string(), |m| {
                    format!("{:02x?}", &m[..m.len().min(6)])
                })
            };
            return Err(format!(
                "TruncCommit: {name} ends with size {size} and manifest {}, but its last \
                 acknowledged op is {} (op {last:?} of {}); the states an op after it allows \
                 do not include this one",
                hash(manifest),
                last.map_or("none", |i| if ops[i].0 { "a commit" } else { "a truncate" }),
                ops.len()
            ));
        }
    }
    Ok(by_name.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 1,
            incarnation: 1,
            seq,
        }
    }

    /// Node 1 creates `d1/a`, and once that returned, `d2/b`. The log
    /// must hold them in that order, whichever owner appended each; a
    /// tentative `d1/a` (rolled back, replayed later) is exempt.
    #[test]
    fn the_log_keeps_a_nodes_program_order_across_directories() {
        let events = vec![
            HistEvt::Invoke {
                thread: 1,
                rid: rid(1),
                op: NsOp::Create("d1/a".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 1,
                rid: rid(2),
                op: NsOp::Create("d2/b".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(2),
                ret: NsRet::Ok,
            },
        ];
        let none = HashSet::new();
        let in_order = [(rid(1), (1, 0)), (rid(2), (1, 1))].into_iter().collect();
        assert!(check_session_order(&events, &none, &in_order, &JournalOrder::new()).is_ok());
        let reversed = [(rid(1), (2, 0)), (rid(2), (1, 0))].into_iter().collect();
        let err = check_session_order(&events, &none, &reversed, &JournalOrder::new()).unwrap_err();
        assert!(err.contains("program order"), "{err}");
        let tentative: HashSet<Rid> = [rid(1)].into_iter().collect();
        assert!(check_session_order(&events, &tentative, &reversed, &JournalOrder::new()).is_ok());
        // Two ops in flight together are unordered.
        let overlapping = vec![
            events[0].clone(),
            events[2].clone(),
            events[1].clone(),
            events[3].clone(),
        ];
        assert!(check_session_order(&overlapping, &none, &reversed, &JournalOrder::new()).is_ok());
        // Plan 45 §12 O2: one tenure journaled rid 1 first and shipped
        // rid 2 ahead of it (rid 1 deferred on a pending chunk): the
        // write-back relaxation, in both checks. Journaled the other way
        // round, the log broke the order.
        let deferred: JournalOrder = [(rid(1), (1, 1, 5)), (rid(2), (1, 1, 6))]
            .into_iter()
            .collect();
        assert!(check_session_order(&events, &none, &reversed, &deferred).is_ok());
        assert!(check_linearizable_witnessed(&events, &none, &none, &reversed, &deferred).is_ok());
        let journaled_reversed: JournalOrder = [(rid(1), (1, 1, 6)), (rid(2), (1, 1, 5))]
            .into_iter()
            .collect();
        assert!(check_session_order(&events, &none, &reversed, &journaled_reversed).is_err());
        assert!(check_linearizable_witnessed(
            &events,
            &none,
            &none,
            &reversed,
            &journaled_reversed
        )
        .is_err());
        let other_tenure: JournalOrder = [(rid(1), (1, 1, 5)), (rid(2), (2, 2, 6))]
            .into_iter()
            .collect();
        assert!(check_session_order(&events, &none, &reversed, &other_tenure).is_err());
    }

    #[test]
    fn a_double_create_that_both_succeed_is_not_linearizable() {
        let events = vec![
            HistEvt::Invoke {
                thread: 1,
                rid: rid(1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 2,
                rid: rid(2),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 2,
                rid: rid(2),
                ret: NsRet::Ok,
            },
        ];
        assert!(check_linearizable(&events, &HashSet::new()).is_err());
        // Marking the first tentative lets the checker drop it.
        let mut tentative = HashSet::new();
        tentative.insert(rid(1));
        assert!(check_linearizable(&events, &tentative).is_ok());
        // The witnessed check agrees: both in the log is a double create;
        // only the second in the log (the first dropped) is fine.
        let both = [(rid(1), (1, 0)), (rid(2), (2, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(
            &events,
            &HashSet::new(),
            &HashSet::new(),
            &both,
            &JournalOrder::new()
        )
        .is_err());
        let second_only = [(rid(2), (2, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(
            &events,
            &tentative,
            &HashSet::new(),
            &second_only,
            &JournalOrder::new()
        )
        .is_ok());
    }

    /// `locks-faults` seed 195356's shape (the L2 window seen by a third
    /// party): t1's create of `f0` is acknowledged, rolled back with its
    /// crashed holder and replayed after t2's next op; t2's own create of
    /// `f0` was refused on the strength of it. The witnessed check counts
    /// the refusal as an observer of a tentative effect; the generic
    /// tester must then treat it as in flight too (it failed the history
    /// before, while the witnessed check passed it).
    #[test]
    fn a_refusal_that_observed_a_tentative_effect_is_in_flight_for_both_checkers() {
        let inv = |thread, r, op| HistEvt::Invoke {
            thread,
            rid: rid(r),
            op,
        };
        let ret = |thread, r, ret| HistEvt::Return {
            thread,
            rid: rid(r),
            ret,
        };
        let events = vec![
            inv(1, 1, NsOp::Create("f0".into())),
            ret(1, 1, NsRet::Ok),
            inv(2, 2, NsOp::Create("f0".into())),
            ret(2, 2, NsRet::Eexist),
            // In the log before t1's replay: nothing removed `f0` between
            // the refusal and this create but t1's rolled-back effect.
            inv(2, 3, NsOp::Create("f0".into())),
            ret(2, 3, NsRet::Ok),
        ];
        let tentative: HashSet<Rid> = [rid(1)].into_iter().collect();
        let log = [(rid(3), (2, 0)), (rid(1), (3, 0))].into_iter().collect();
        let w = check_linearizable_witnessed(
            &events,
            &tentative,
            &HashSet::new(),
            &log,
            &JournalOrder::new(),
        )
        .expect("witnessed");
        assert_eq!(w.observed_tentative, 1);
        // (Rolled back by a deposition, not an operator's drop: the
        // protocol's, so `strict_durability` counts it.)
        assert_eq!(w.observed_dropped, 0);
        assert!(w.observers.contains(&rid(2)));
        // The same history with the effect rolled back by `repair
        // drop-held`: the operator's action, not counted.
        let dropped: HashSet<Rid> = [rid(1)].into_iter().collect();
        let w =
            check_linearizable_witnessed(&events, &tentative, &dropped, &log, &JournalOrder::new())
                .expect("witnessed");
        assert_eq!((w.observed_tentative, w.observed_dropped), (1, 1));
        assert!(check_linearizable(&events, &tentative).is_err());
        let in_flight: HashSet<Rid> = tentative.union(&w.observers).copied().collect();
        assert!(check_linearizable(&events, &in_flight).is_ok());
    }

    /// A chain of tentative effects (plan 46 M0.2's `drop-held-chain`
    /// seeds 66 and 993): t1's create of `f3` is acked and rolled back,
    /// t2's rename of it to `f2` is acked on top of it and rolled back too,
    /// and a rename of `f2` back (t3) had not run yet when t4's create of
    /// `f2` was refused on the strength of the first two. Neither all of
    /// them together nor any one alone shows `f2`; the first two do. And
    /// an unlink of `f3` invoked between them (t5, a forward the holder
    /// executed late) is left out as well: a subset of the invocation order.
    #[test]
    fn a_refusal_that_observed_a_chain_of_tentative_effects_is_explained_by_a_subset() {
        let inv = |thread, r, op| HistEvt::Invoke {
            thread,
            rid: rid(r),
            op,
        };
        let ret = |thread, r, ret| HistEvt::Return {
            thread,
            rid: rid(r),
            ret,
        };
        let events = vec![
            inv(1, 1, NsOp::Create("f3".into())),
            inv(4, 5, NsOp::Unlink("f3".into())),
            inv(2, 2, NsOp::Rename("f3".into(), "f2".into())),
            ret(2, 2, NsRet::Ok),
            inv(1, 3, NsOp::Rename("f2".into(), "f3".into())),
            inv(3, 4, NsOp::Create("f2".into())),
            ret(3, 4, NsRet::Eexist),
        ];
        let tentative: HashSet<Rid> = [rid(1), rid(2), rid(3), rid(5)].into_iter().collect();
        let none = std::collections::HashMap::new();
        // Rolled back by an operator's drop: explained by the first two.
        let w = check_linearizable_witnessed(
            &events,
            &tentative,
            &tentative,
            &none,
            &JournalOrder::new(),
        )
        .expect("explained by the first two tentative effects");
        assert_eq!((w.observed_tentative, w.observed_dropped), (1, 1));
        // The same chain rolled back by anything else is not: a subset of
        // tentative ops explains a refusal only for the dropped ones, so a
        // refusal no state, no single tentative effect and not all of them
        // together shows is a violation.
        let err = check_linearizable_witnessed(
            &events,
            &tentative,
            &HashSet::new(),
            &none,
            &JournalOrder::new(),
        )
        .unwrap_err();
        assert!(err.contains("no state in its window"), "{err}");
        // Only the ops that were dropped count: the unlink alone not.
        let some: HashSet<Rid> = [rid(5)].into_iter().collect();
        assert!(check_linearizable_witnessed(
            &events,
            &tentative,
            &some,
            &none,
            &JournalOrder::new()
        )
        .is_err());
    }

    /// A chain the sequencer ran in the order its ops reached it, not the
    /// order they were invoked in (`drop-held-chain-backed` seed 364): t2's
    /// rename of `f3` was invoked before t3's rename of `f2` into `f3`, and
    /// ran after it; t4's create of `f0` was refused on the strength of both.
    #[test]
    fn a_chain_run_in_another_order_than_invoked_explains_a_refusal() {
        let inv = |thread, r, op| HistEvt::Invoke {
            thread,
            rid: rid(r),
            op,
        };
        let ret = |thread, r, ret| HistEvt::Return {
            thread,
            rid: rid(r),
            ret,
        };
        let events = vec![
            inv(1, 1, NsOp::Create("f2".into())),
            ret(1, 1, NsRet::Ok),
            inv(2, 2, NsOp::Rename("f3".into(), "f0".into())),
            inv(3, 3, NsOp::Rename("f2".into(), "f3".into())),
            inv(4, 4, NsOp::Create("f0".into())),
            ret(4, 4, NsRet::Eexist),
        ];
        let tentative: HashSet<Rid> = [rid(2), rid(3)].into_iter().collect();
        let log = [(rid(1), (1, 0))].into_iter().collect();
        let w = check_linearizable_witnessed(
            &events,
            &tentative,
            &tentative,
            &log,
            &JournalOrder::new(),
        )
        .expect("explained by running t3 then t2");
        assert_eq!(w.observed_tentative, 1);
        // Not dropped by an operator: no order of a subset of them counts.
        assert!(check_linearizable_witnessed(
            &events,
            &tentative,
            &HashSet::new(),
            &log,
            &JournalOrder::new()
        )
        .is_err());
    }

    #[test]
    fn the_witnessed_check_places_refusals_in_their_window() {
        // t1: create a → Ok (log 1). t2: create a → Eexist, invoked after
        // t1 returned: feasible. t3: unlink b → Enoent, concurrent with
        // everything: feasible. t4: create a → Ok returned, absent from the
        // log: caught by the value rule (returned success never landed).
        let events = vec![
            HistEvt::Invoke {
                thread: 1,
                rid: rid(1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 1,
                rid: rid(1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 2,
                rid: rid(2),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Invoke {
                thread: 3,
                rid: rid(3),
                op: NsOp::Unlink("b".into()),
            },
            HistEvt::Return {
                thread: 2,
                rid: rid(2),
                ret: NsRet::Eexist,
            },
            HistEvt::Return {
                thread: 3,
                rid: rid(3),
                ret: NsRet::Enoent,
            },
        ];
        let log = [(rid(1), (1, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(
            &events,
            &HashSet::new(),
            &HashSet::new(),
            &log,
            &JournalOrder::new()
        )
        .is_ok());
        // A refusal that no state in its window explains.
        let mut bad = events.clone();
        bad[4] = HistEvt::Return {
            thread: 2,
            rid: rid(2),
            ret: NsRet::Enoent,
        };
        assert!(check_linearizable_witnessed(
            &bad,
            &HashSet::new(),
            &HashSet::new(),
            &log,
            &JournalOrder::new()
        )
        .is_err());
        // A tentative success whose original `Ok` came before t2's
        // invocation but whose replay landed late does not bound t2's
        // window: t2's `Eexist` needs a state with `a` present, and the
        // log has it at position 0 — before the tentative op landed.
        let mut tentative = HashSet::new();
        tentative.insert(rid(1));
        let with_a = vec![
            HistEvt::Invoke {
                thread: 9,
                rid: rid(9),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 9,
                rid: rid(9),
                ret: NsRet::Ok,
            },
        ];
        let mut events2 = with_a;
        events2.extend(events.clone());
        let log2 = [(rid(9), (1, 0)), (rid(1), (5, 0))].into_iter().collect();
        assert!(check_linearizable_witnessed(
            &events2,
            &tentative,
            &HashSet::new(),
            &log2,
            &JournalOrder::new()
        )
        .is_ok());
    }

    #[test]
    fn a_file_ends_in_its_last_acknowledged_commit_or_a_later_attempt() {
        let rec = |t: bool, c: Option<bool>, m: u8| TruncRec {
            name: "d1/f0".into(),
            truncated: t,
            committed: c == Some(true),
            manifest: c.map(|_| vec![m]),
        };
        let fin = |size: u64, m: Option<u8>| {
            [("d1/f0".to_string(), Some((size, m.map(|m| vec![m]))))]
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        // Two overwrites, both acknowledged: the second commit stands.
        let recs = [rec(true, Some(true), 1), rec(true, Some(true), 2)];
        assert!(check_trunc_commits(&recs, &fin(4096, Some(2)), 4096).is_ok());
        // The loss: the first commit or an empty file after two
        // acknowledged commits.
        assert!(check_trunc_commits(&recs, &fin(4096, Some(1)), 4096).is_err());
        assert!(check_trunc_commits(&recs, &fin(0, None), 4096).is_err());
        // The second commit unacknowledged, its truncate acknowledged: the
        // file is empty or holds the second commit.
        let recs = [rec(true, Some(true), 1), rec(true, Some(false), 2)];
        assert!(check_trunc_commits(&recs, &fin(4096, Some(2)), 4096).is_ok());
        assert!(check_trunc_commits(&recs, &fin(0, None), 4096).is_ok());
        assert!(check_trunc_commits(&recs, &fin(4096, Some(1)), 4096).is_err());
        // The last acknowledged op is a truncate: the commit before it may
        // not come back.
        let recs = [rec(true, Some(true), 1), rec(true, None, 0)];
        assert!(check_trunc_commits(&recs, &fin(0, None), 4096).is_ok());
        assert!(check_trunc_commits(&recs, &fin(4096, Some(1)), 4096).is_err());
        // Nothing acknowledged: as created.
        let recs = [rec(false, Some(false), 1)];
        assert!(check_trunc_commits(&recs, &fin(0, None), 4096).is_ok());
    }

    /// The memoized search ([`linearizable`]) and stateright's own
    /// (`is_consistent`) decide the same on random small histories: threads
    /// whose ops take effect at a random instant between invocation and
    /// return (the answer the spec gives then, now and again replaced by
    /// another value), some left in flight, some marked tentative, some
    /// settled. Both verdicts occur.
    #[test]
    fn the_memoized_search_agrees_with_stateright() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let names = ["a", "b", "c"];
        let rets = [NsRet::Ok, NsRet::Eexist, NsRet::Enoent];
        let (mut yes, mut no) = (0, 0);
        for case in 0..3_000u64 {
            let mut rng = StdRng::seed_from_u64(case);
            let mut spec = NsSpec::default();
            let mut events = Vec::new();
            // Per thread: (rid, op, answer once it took effect).
            let mut busy: std::collections::BTreeMap<ThreadId, (Rid, NsOp, Option<NsRet>)> =
                std::collections::BTreeMap::new();
            let mut seq = 0;
            let ops = rng.random_range(3..10);
            let threads = rng.random_range(2..5u64);
            let mut tentative = HashSet::new();
            while seq < ops || !busy.is_empty() {
                let t = rng.random_range(1..=threads);
                match busy.get_mut(&t) {
                    None if seq < ops => {
                        seq += 1;
                        let n = |rng: &mut StdRng| names[rng.random_range(0..3)].to_string();
                        let op = match rng.random_range(0..7) {
                            0 => NsOp::Create(n(&mut rng)),
                            1 => NsOp::Unlink(n(&mut rng)),
                            2 => NsOp::Rename(n(&mut rng), n(&mut rng)),
                            3 => NsOp::Put(n(&mut rng)),
                            4 => NsOp::Link(n(&mut rng), n(&mut rng)),
                            5 => NsOp::Exchange(n(&mut rng), n(&mut rng)),
                            _ => NsOp::Same(n(&mut rng), n(&mut rng)),
                        };
                        let r = rid(seq);
                        if rng.random_bool(0.1) {
                            tentative.insert(r);
                        }
                        events.push(HistEvt::Invoke {
                            thread: t,
                            rid: r,
                            op: op.clone(),
                        });
                        busy.insert(t, (r, op, None));
                    }
                    None => {}
                    Some((_, op, done @ None)) => *done = Some(spec.invoke(op)),
                    Some((r, _, Some(ret))) => {
                        let r = *r;
                        let ret = match rng.random_range(0..12) {
                            0 => rets[rng.random_range(0..3)],
                            1 => NsRet::Abandoned,
                            _ => *ret,
                        };
                        busy.remove(&t);
                        // Some ops never return (in flight at the end).
                        if seq < ops || !rng.random_bool(0.15) {
                            events.push(HistEvt::Return {
                                thread: t,
                                rid: r,
                                ret,
                            });
                        }
                    }
                }
            }
            let memo = check_linearizable_with(&events, &tentative, false);
            let sr = check_linearizable_with(&events, &tentative, true);
            assert_eq!(memo, sr, "case {case}: {events:?} tentative {tentative:?}");
            if memo.is_ok() {
                yes += 1;
            } else {
                no += 1;
            }
        }
        assert!(yes > 300 && no > 300, "linearizable {yes}, not {no}");
    }
}
