//! Plan 43 C0: the commit-rule oracles' plumbing (the invariants I1, I2, I4
//! and I5 of the plan's §4), judged by the simulation and the models only —
//! the core carries no assert (C0 changes no behaviour).
//!
//! * I1 `acked_implies_committed`: every definitive answer is classified,
//!   **at the answering sequencer's emission**, by reading every node's
//!   store and the bucket's lease and log at that instant, independently of
//!   what the core claims ([`committed_at_answer`]). What a transaction
//!   carries must be the answer's own outcome (a completion for an
//!   acceptance, a refusal of that code for a refusal), and a requester's
//!   reply passed on is not judged but verified at the end of the run
//!   against the answer it passes on (plan 43 C2a).
//! * I2 `one_identity_per_op`: (b) no rid queued for execution on two nodes
//!   at once ([`IdentityLedger`]); (c) verbatim, at the end of the run
//!   (`run.rs`).
//! * I4 `unacked_fails_never_lands_late` and I5 `speculation_only_lone`: at
//!   the end of the run, from the logs this module keeps.
//!
//! Each oracle always counts; its strict flag (`SimConfig::strict_*`) only
//! decides whether a count fails the run.

use super::bus::Bus;
use super::clock::Clock;
use super::node::Shared;
use super::store::Bucket;
use constellation_authority::segment::{self, Segment};
use constellation_authority::{NodeId, Replica, Seq};
use constellation_meta::RowCommit;
use constellation_meta::{BackupTx, LogRecord, Meta, MutateOutcome, Rid};
use constellation_store_s3::{AckPolicy, Lease, LogStore};
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// `(epoch, backups, epoch hold)` of the tenure that appended a delegate's row.
pub type AppendTenure = (u64, Vec<NodeId>, bool);

/// Everything the oracles share in one run (`NodeEnv::oracles`).
pub struct Oracles {
    pub bucket: Arc<Bucket>,
    /// Node id -> the node's current `Meta`, updated by every
    /// `NodeHandle::start_with` (a restart, with or without its journal).
    pub stores: Mutex<BTreeMap<NodeId, Arc<Meta>>>,
    /// Node id -> the core's backup RTT budget and link-stability window
    /// (ms), for the lone check.
    pub budgets: Mutex<BTreeMap<NodeId, (u64, u64)>>,
    /// Node id -> since when (on its wait clock) each peer link has been up
    /// without a break, as the driver's directory last told its core
    /// (`Event::Peers`): the stability window of a backup candidate.
    pub directory: Mutex<BTreeMap<NodeId, BTreeMap<NodeId, i64>>>,
    /// Every answer, in emission order (the `AckLog`).
    pub acks: Mutex<Vec<AckEntry>>,
    pub identity: Mutex<IdentityLedger>,
    /// The first submission of each rid, with its tick.
    pub invokes: Mutex<Vec<(Rid, u64)>>,
    invoked: Mutex<HashSet<Rid>>,
    /// One global, strictly increasing event counter: every emission
    /// judged, every in-doubt answer and every client invocation takes the
    /// next value, so "after" is exact on the single-threaded runtime.
    pub tick: AtomicU64,
    /// Answers whose requester-side mapping `(sequencer, req) -> rid` was
    /// missing (must be 0).
    pub acks_unmapped: AtomicU64,
    /// Descriptions of I1 violations, in order (judged at emission, failed
    /// at the end of the run).
    pub i1_violations: Mutex<Vec<String>>,
    /// ADR-47, check 1: the root's refusals of ops under a non-stealable
    /// grant, the executions of non-stealable generations and every client
    /// submission, each with the tick it happened at.
    pub fence_audit: Mutex<FenceAudit>,
    /// Plan 43 C1-2: answers whose claim of a commit the judgement or the
    /// answering row's own class does not bear out (`check_claim`).
    pub claim_violations: Mutex<Vec<String>>,
    /// Outcomes the core answered a client with that no sequencer decided
    /// (`AnswerSource::None`: a node's own error): not judged.
    pub acks_unsourced: AtomicU64,
    /// Plan 43 C7b, I6: the lease-claim oracle (see [`LeaseClaims`]).
    pub leases: Arc<LeaseClaims>,
    /// Plan 46 M1.1: the tenure each delegate row `(gen, idx)` was appended
    /// in at a root, `(epoch, backups, epoch hold in force)`, recorded when
    /// the root appended it (the latest append wins: a successor's re-append
    /// of an unretired row is its own tenure's). A row off a delegate's
    /// stream or drained from a sealed tail is the appending tenure's; a row
    /// journaled by any other event is a re-journal and keeps its record.
    pub appended: Mutex<HashMap<(u64, u64), AppendTenure>>,
    /// The latest lease configuration landed in each epoch (`epoch ->
    /// backups`): a lone root that adds a backup later in its epoch commits
    /// the rows it appended by that backup's copy (the core counts the
    /// configuration as it is, not as it was at the append).
    pub epoch_backups: Arc<Mutex<BTreeMap<u64, Vec<NodeId>>>>,
    /// Plan 46 M1.1 step 5: the backup set of every lease version that
    /// landed (`config_version -> backups`; a version names one landing:
    /// every tenure starts above its predecessor's). A row keeps the rule of
    /// the configuration it was journaled in, so a `Group{Root}` row is
    /// judged by the set of its own version, whatever the lease lists now.
    pub version_backups: Arc<Mutex<BTreeMap<u64, Vec<NodeId>>>>,
    /// Nodes whose core holds a continuation epoch right now: a delegate's
    /// row their journal holds counts once a member holds it (plan 46 M1.4).
    pub epoch_holds: Mutex<BTreeSet<NodeId>>,
    /// Plan 46 M1.4 step 3: the highest journal seq of a continuation epoch
    /// each node's store has held as streamed speculation, ever seen: a
    /// member's copy that committed a delegate's row stays what committed it
    /// after the epoch ended and the copy was retired or stranded.
    pub streamed_seen: Mutex<BTreeMap<(NodeId, u64), u64>>,
    /// Plan 46 M1.4 step 4: the members of each continuation epoch's hold,
    /// as its owner's core named them (`Core::hold_roster`: the epoch's
    /// members and the nodes that acknowledged its journal). Only a member's
    /// store is a copy of the hold (the group is the owner and a member).
    pub hold_members: Mutex<BTreeMap<u64, BTreeSet<NodeId>>>,
    /// Plan 46 M1.1 step 3: the row-version oracle (see [`RowVersions`]).
    pub row_versions: Arc<Mutex<RowVersions>>,
    /// Plan 46 M0.3: the run's reachability history (see `reach.rs`), the
    /// op each rid submitted (to find the delegate of its keys), and every
    /// unsettled `EIO` with what the history says of it.
    pub reach: Arc<super::reach::Reach>,
    pub ops: Mutex<HashMap<Rid, constellation_meta::MutateOp>>,
    pub unsettled: Mutex<Vec<Unsettled>>,
    /// The holder's release after its drain, judged from the root's side
    /// (`Oracles::note_release_rows`), independent of the core's own counter.
    pub release_drain: Mutex<ReleaseDrain>,
    /// The causal cut (plan 30 M11): a delegate waited for an op's deps
    /// before executing, so a root refuses a stream batch for lacking them
    /// only when it was deposed without knowing it. Each refusal at the
    /// *current* root (the one holding the highest lease epoch written) is
    /// a delegate that executed ahead of its cause.
    pub causal_cut: Mutex<Vec<String>>,
    /// Plan 46 M1.3 step 4: the generations whose holder sent the root a
    /// release (a note on its stream: a row, or the cut a rowless release
    /// ends at; the latter leaves nothing in the log).
    pub sticky_released: Mutex<BTreeMap<u64, super::sticky::SentRelease>>,
    /// The delegation table as the lease holder had it, each time it changed
    /// (`(at_ms, holder, table)`): the generation history (install, recall,
    /// end) that decides who owns an op's keys at each instant.
    pub tables: Mutex<
        Vec<(
            u64,
            NodeId,
            Arc<constellation_meta::delegation::DelegationTable>,
        )>,
    >,
}

/// The holder's release rows seen, and those journaled before the root had
/// appended the rows before them (`Oracles::note_release_rows`).
#[derive(Default)]
pub struct ReleaseDrain {
    pub checked: BTreeMap<(NodeId, u64), u64>,
    pub rows: u64,
    pub violations: Vec<String>,
}

/// An unsettled `EIO` (the client was told in doubt after its settle went
/// unanswered), and whether its requester was isolated (plan 46 M0.3).
#[derive(Debug, Clone)]
pub struct Unsettled {
    pub node: NodeId,
    pub rid: Rid,
    pub at_ms: u64,
    pub window_ms: u64,
    pub verdict: super::reach::Verdict,
    /// A non-stealable holder's refusal was held back for the op (it may
    /// have run there): I4's exception for an unreachable non-stealable
    /// holder, counted apart from the reachability classes and never a
    /// violation (`Stats::unsettled_nonstealable_holder`).
    pub designation_held: bool,
    /// Sync-on-silence C1: for an EIO of a generation tail the root could
    /// not reach, whether a strict member was silent before the delegate
    /// went down (`Reach::silence_class`).
    pub silence: Option<String>,
}

impl Unsettled {
    /// A key for counting the causes: `isolated`, or how a sequencer was
    /// reachable and what its core was busy with.
    pub fn cause(&self) -> String {
        match &self.verdict {
            super::reach::Verdict::Isolated { why } if why.starts_with("requester") => {
                "isolated/requester stopped".into()
            }
            super::reach::Verdict::Isolated { .. } => "isolated".into(),
            super::reach::Verdict::NoSequencerInTime { why } => {
                if why.contains("holds nothing") {
                    "no-sequencer-in-time/lease of a holder with nothing".into()
                } else if why.contains("no member") {
                    "no-sequencer-in-time/group tail".into()
                } else if why.contains("keys across generations") {
                    "no-sequencer-in-time/generation outwait, keys across generations".into()
                } else if why.contains("ends the generation") {
                    "no-sequencer-in-time/generation outwait".into()
                } else {
                    "no-sequencer-in-time/claim".into()
                }
            }
            super::reach::Verdict::GenerationTailUnreachable { .. } => {
                "generation-tail-unreachable".into()
            }
            super::reach::Verdict::Reachable { via, busy, .. } => {
                let via = match via {
                    super::reach::Via::Itself => "itself",
                    super::reach::Via::Root(_) => "root",
                    super::reach::Via::Delegate(_) => "delegate",
                    super::reach::Via::LeasePath => "lease-path",
                };
                format!("{via}/{busy}")
            }
        }
    }

    pub fn describe(&self) -> String {
        let span = format!(
            "[{}, {}] ms",
            self.at_ms.saturating_sub(self.window_ms),
            self.at_ms
        );
        match &self.verdict {
            super::reach::Verdict::Isolated { why } => format!(
                "I4: rid {:?} told EIO unsettled by node {} at t={} ms; isolated over {span} ({why})",
                self.rid, self.node, self.at_ms
            ),
            super::reach::Verdict::NoSequencerInTime { why } => format!(
                "I4: rid {:?} told EIO unsettled by node {} at t={} ms; no sequencer in time over {span} ({why})",
                self.rid, self.node, self.at_ms
            ),
            super::reach::Verdict::GenerationTailUnreachable { why } => format!(
                "I4: rid {:?} told EIO unsettled by node {} at t={} ms; the root could not end the generation over {span} ({why})",
                self.rid, self.node, self.at_ms
            ),
            super::reach::Verdict::Reachable {
                via,
                from_ms,
                to_ms,
                busy,
            } => format!(
                "I4: rid {:?} told EIO unsettled by node {} at t={} ms although a sequencer was \
                 reachable over {span}: {via:?} during [{from_ms}, {to_ms}] ms (its core: {busy})",
                self.rid, self.node, self.at_ms
            ),
        }
    }
}

impl Oracles {
    /// `node` refused delegate batches for lacking their deps (the rise of
    /// `Stats::deleg_deps_refused` over one step), holding lease epoch
    /// `epoch` (`None`: no lease). A violation when that is the highest
    /// epoch written; a deposed root's refusal is not.
    pub fn note_deps_refused(&self, node: NodeId, epoch: Option<u64>, refused: u64, now_ms: i64) {
        let highest = self.leases.max_epoch.load(Ordering::SeqCst);
        if refused > 0 && epoch.is_some_and(|e| e == highest) {
            self.causal_cut.lock().unwrap().push(format!(
                "node {node}, the current root (lease epoch {highest}), refused {refused} delegate \
                 batch(es) for lacking their deps at {now_ms} ms: a delegate executed ahead of its cause"
            ));
        }
    }

    pub fn new(bucket: Arc<Bucket>) -> Arc<Self> {
        let leases = Arc::new(LeaseClaims::default());
        let reach = bucket.reach();
        let epoch_backups: Arc<Mutex<BTreeMap<u64, Vec<NodeId>>>> = Arc::default();
        let epoch_backups_hook = epoch_backups.clone();
        let version_backups: Arc<Mutex<BTreeMap<u64, Vec<NodeId>>>> = Arc::default();
        let version_backups_hook = version_backups.clone();
        let row_versions: Arc<Mutex<RowVersions>> = Arc::default();
        {
            let row_versions = row_versions.clone();
            let leases = leases.clone();
            let reach = reach.clone();
            bucket.set_lease_hook(Arc::new(move |node, prev, new| {
                epoch_backups_hook
                    .lock()
                    .unwrap()
                    .insert(new.epoch, new.backups.clone());
                version_backups_hook
                    .lock()
                    .unwrap()
                    .insert(new.config_version, new.backups.clone());
                row_versions
                    .lock()
                    .unwrap()
                    .lease_landed(new.epoch, new.config_version);
                reach.lease_landed(
                    new.holder,
                    new.released,
                    new.backups.clone(),
                    new.inherited_tail
                        .as_ref()
                        .map(|t| t.members.clone())
                        .unwrap_or_default(),
                    new.epoch,
                    new.inherited_tail.as_ref().map_or(0, |t| t.epoch),
                );
                leases.landed(node, prev, new)
            }));
        }
        Arc::new(Self {
            leases,
            reach,
            ops: Mutex::new(HashMap::new()),
            unsettled: Mutex::new(Vec::new()),
            release_drain: Mutex::new(ReleaseDrain::default()),
            causal_cut: Mutex::new(Vec::new()),
            sticky_released: Mutex::new(BTreeMap::new()),
            tables: Mutex::new(Vec::new()),
            bucket,
            stores: Mutex::new(BTreeMap::new()),
            budgets: Mutex::new(BTreeMap::new()),
            directory: Mutex::new(BTreeMap::new()),
            acks: Mutex::new(Vec::new()),
            identity: Mutex::new(IdentityLedger::default()),
            invokes: Mutex::new(Vec::new()),
            invoked: Mutex::new(HashSet::new()),
            tick: AtomicU64::new(1),
            acks_unmapped: AtomicU64::new(0),
            i1_violations: Mutex::new(Vec::new()),
            fence_audit: Mutex::new(FenceAudit::default()),
            claim_violations: Mutex::new(Vec::new()),
            appended: Mutex::new(HashMap::new()),
            epoch_backups,
            version_backups,
            epoch_holds: Mutex::new(BTreeSet::new()),
            streamed_seen: Mutex::new(BTreeMap::new()),
            hold_members: Mutex::new(BTreeMap::new()),
            row_versions,
            acks_unsourced: AtomicU64::new(0),
        })
    }

    pub fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::SeqCst)
    }

    /// ADR-47: the core's audit of one step (`Core::take_final_nonholder_refusals`,
    /// `Core::take_nonstealable_executions`), stamped with the step's tick.
    pub fn note_fence_audit(
        &self,
        node: NodeId,
        refusals: Vec<(Rid, Code, bool)>,
        execs: Vec<(Rid, u64)>,
    ) {
        if refusals.is_empty() && execs.is_empty() {
            return;
        }
        let tick = self.next_tick();
        let mut a = self.fence_audit.lock().unwrap();
        a.refusals
            .extend(refusals.into_iter().map(|(r, c, d)| (r, c, d, tick, node)));
        a.execs
            .extend(execs.into_iter().map(|(r, g)| (r, g, tick, node)));
    }

    /// ADR-47: a client submitted `rid` (the first time or again).
    pub fn note_submit(&self, rid: Rid) {
        let tick = self.next_tick();
        self.fence_audit.lock().unwrap().submits.push((rid, tick));
    }

    /// A client's first submission of `rid` (a resubmission adds nothing).
    pub fn note_invoke(&self, rid: Rid) {
        if self.invoked.lock().unwrap().insert(rid) {
            let t = self.next_tick();
            self.invokes.lock().unwrap().push((rid, t));
        }
    }

    /// The op a client submitted under `rid` (plan 46 M0.3: its keys name
    /// the delegate that would settle it).
    pub fn note_op(&self, rid: Rid, op: &constellation_meta::MutateOp) {
        self.ops.lock().unwrap().insert(rid, op.clone());
    }

    /// A lease holder's delegation table, as of now: kept when it differs
    /// from the last one kept (an empty one counts as none).
    pub fn note_table(
        &self,
        holder: NodeId,
        table: constellation_meta::delegation::DelegationTable,
    ) {
        self.note_table_at(self.reach.now_ms(), holder, table);
    }

    fn note_table_at(
        &self,
        at: u64,
        holder: NodeId,
        table: constellation_meta::delegation::DelegationTable,
    ) {
        let mut t = self.tables.lock().unwrap();
        if t.last()
            .map_or(table.is_empty(), |(_, _, last)| **last == table)
        {
            return;
        }
        t.push((at, holder, Arc::new(table)));
    }

    /// Whether the last table kept names a delegation.
    pub fn tables_nonempty(&self) -> bool {
        self.tables
            .lock()
            .unwrap()
            .last()
            .is_some_and(|(_, _, t)| !t.is_empty())
    }

    /// Who owned `rid`'s keys at each instant of `[from, to]`: the holder's
    /// table at that instant resolved over the op's keys (the generation
    /// history decides, not the table at the `EIO`), and when each generation
    /// of the history first showed in a table (the start of the root's window
    /// of a generation its delegate never installed).
    fn owner_timeline(
        &self,
        node: NodeId,
        rid: Rid,
        from: u64,
        to: u64,
    ) -> (Vec<(u64, super::reach::Owner)>, BTreeMap<u64, u64>) {
        use super::reach::Owner;
        let root = vec![(0, Owner::Root)];
        let Some(op) = self.ops.lock().unwrap().get(&rid).cloned() else {
            return (root, BTreeMap::new());
        };
        let Some(meta) = self.store(node) else {
            return (root, BTreeMap::new());
        };
        let keys = constellation_meta::TouchSet::from_op_in_names(
            &op,
            &|p, n| Replica::lookup_ino(&*meta, p, n),
            &|i| Replica::names_of_ino(&*meta, i),
        );
        let tables = self.tables.lock().unwrap().clone();
        let mut born: BTreeMap<u64, u64> = BTreeMap::new();
        for (at, _, table) in &tables {
            for d in table.iter() {
                born.entry(d.gen).or_insert(*at);
            }
        }
        let mut instants = vec![from];
        instants.extend(tables.iter().map(|t| t.0).filter(|t| *t > from && *t <= to));
        let mut out: Vec<(u64, Owner)> = Vec::new();
        for t in instants {
            let holder = self.reach.holder_at(t);
            let table = tables
                .iter()
                .rev()
                .filter(|(at, _, _)| *at <= t)
                .find(|(_, h, _)| Some(*h) == holder)
                .or_else(|| tables.iter().rev().find(|(at, _, _)| *at <= t));
            let owner = match table {
                None => Owner::Root,
                Some((_, _, tb)) => match tb.resolve(&*meta, &keys) {
                    constellation_meta::delegation::Ownership::Root => Owner::Root,
                    constellation_meta::delegation::Ownership::Delegated(d) => Owner::Delegate {
                        node: d.node,
                        gen: d.gen,
                    },
                    constellation_meta::delegation::Ownership::CrossSubtree {
                        involved, ..
                    } => Owner::Blocked(involved.iter().map(|d| (d.node, d.gen)).collect()),
                },
            };
            if out.last().map(|(_, o)| o) != Some(&owner) {
                out.push((t, owner));
            }
        }
        (out, born)
    }

    /// Plan 46 M0.3: `node`'s core told a client `EIO`, in doubt, at the
    /// end of its settle window (`window_ms`, the core's `settle_window`).
    /// Classified from the reachability history, never from a node's log:
    /// isolated (no sequencer reachable in the whole window) or not.
    pub fn note_unsettled(
        &self,
        node: NodeId,
        rid: Rid,
        window_ms: u64,
        started: Option<u64>,
        designation_held: bool,
        accepted_by_delegate: bool,
    ) {
        let at = self.reach.now_ms();
        // (A settle lasts at least a window; one whose deadline was extended
        // lasts from the arming of its second deadline.)
        let from_ms = started.unwrap_or(at).min(at.saturating_sub(window_ms));
        let (owners, born) = if accepted_by_delegate {
            // A delegate executed the op and answered it: what the requester
            // still lacked was the commit of that delegate's rows (it awaited
            // the log), which only a root gives. The sequencer to reach is a
            // root (or a claimant the lease path yields), not the delegate it
            // had heard: a restarted root that holds nothing until its lease
            // runs out leaves none (`sticky-ttl` with slow lease reads, seed
            // 907).
            (vec![(0, super::reach::Owner::Root)], Default::default())
        } else {
            self.owner_timeline(node, rid, from_ms, at)
        };
        let question = super::reach::Question {
            requester: node,
            from_ms,
            to_ms: at,
            owners,
            born,
        };
        let verdict = self.reach.classify(question.clone());
        let silence = self.reach.silence_class(&question, &verdict);
        self.unsettled.lock().unwrap().push(Unsettled {
            node,
            rid,
            at_ms: at,
            window_ms: window_ms.max(at.saturating_sub(started.unwrap_or(at))),
            verdict,
            designation_held,
            silence,
        });
    }

    /// Plan 46 M1.3 step 5: a holder journals the release row of a sticky
    /// subtree by its own command (`judge`: this step journaled one) only once the root has appended every row of the stream
    /// before it (the drain), or an acknowledged op of the subtree would be
    /// unstreamed when the root takes the subtree back. Judged from the
    /// stores, after each event of `node`: a new `SubtreeReleased` /
    /// `StreamReleased` row of one of its streams at index `R` while no
    /// store holds the stream appended through `R - 1` from the log is a
    /// finding. Independent of the core's counter of the same defect
    /// (`Stats::deleg_sticky_release_undrained`, which only the knob's own
    /// branch increments).
    pub fn note_release_rows(&self, node: NodeId, meta: &Meta, judge: bool) {
        let table = meta.delegation_table();
        let gens: Vec<u64> = table
            .streams()
            .filter(|s| s.node == node)
            .map(|s| s.gen)
            .collect();
        for gen in gens {
            let top = Replica::delegate_idx(meta, gen);
            let from = {
                let seen = self.release_drain.lock().unwrap();
                seen.checked.get(&(node, gen)).copied().unwrap_or(0)
            };
            if top <= from {
                continue;
            }
            let mut releases = Vec::new();
            for tx in Replica::delegate_txs_from(meta, gen, from + 1, usize::MAX) {
                if tx.records.iter().any(|r| {
                    matches!(
                        r,
                        LogRecord::SubtreeReleased { node: n, .. }
                            | LogRecord::StreamReleased { node: n, .. } if *n == node
                    )
                }) {
                    releases.push(tx.idx);
                }
            }
            let appended = self
                .stores
                .lock()
                .unwrap()
                .values()
                .map(|m| Replica::log_stream_idx(&**m, gen))
                .max()
                .unwrap_or(0);
            let mut seen = self.release_drain.lock().unwrap();
            seen.checked.insert((node, gen), top);
            for idx in releases.into_iter().filter(|_| judge) {
                seen.rows += 1;
                if appended + 1 < idx {
                    seen.violations.push(format!(
                        "node {node} journaled the release row {idx} of stream {gen} while the \
                         root had appended it only through {appended}"
                    ));
                }
            }
        }
    }

    pub fn register(&self, id: NodeId, meta: Arc<Meta>, budget_ms: u64, stable_ms: u64) {
        self.stores.lock().unwrap().insert(id, meta);
        self.budgets
            .lock()
            .unwrap()
            .insert(id, (budget_ms, stable_ms));
    }

    /// `node`'s store after an event: note the tip of each continuation
    /// epoch it holds as streamed speculation (`streamed_seen`).
    pub fn note_streamed(&self, node: NodeId, meta: &Meta) {
        self.note_streamed_to(node, meta, self.reach.max_epoch());
    }

    fn note_streamed_to(&self, node: NodeId, meta: &Meta, max_epoch: u64) {
        let mut seen = self.streamed_seen.lock().unwrap();
        for epoch in 1..=max_epoch {
            if let Ok(Some(tip)) = Replica::streamed_tip(meta, epoch) {
                let t = seen.entry((node, epoch)).or_insert(0);
                *t = (*t).max(tip);
            }
        }
    }

    /// The members of the hold `epoch`'s owner reports (`hold_members`).
    pub fn note_hold_members(&self, epoch: u64, members: &[NodeId]) {
        self.hold_members
            .lock()
            .unwrap()
            .entry(epoch)
            .or_default()
            .extend(members.iter().copied());
    }

    /// The links `id`'s directory reported up, with their `since` (wait ms).
    pub fn note_directory(&self, id: NodeId, up: BTreeMap<NodeId, i64>) {
        self.directory.lock().unwrap().insert(id, up);
    }

    fn store(&self, id: NodeId) -> Option<Arc<Meta>> {
        self.stores.lock().unwrap().get(&id).cloned()
    }
}

// ---------------------------------------------------------------------
// The archive index.
// ---------------------------------------------------------------------

/// A rid with both a completion and a refusal in the log: the rid, then the
/// first outcome's and the contradicting one's segment and code.
type Conflict = (Rid, (Seq, Option<Code>), (Seq, Option<Code>));

/// What the bucket's log carries, kept synchronously: `Bucket` notes each
/// segment as it lands (`NodeStore::put_opts`), and the index answers over
/// the contiguous prefix from 1 with epoch fencing exactly as
/// `check::replay_log` applies it (a segment with `0 < epoch < max_epoch
/// so far` is fenced and carries nothing).
pub struct ArchiveIndex {
    log: LogStore,
    inner: Mutex<ArchiveInner>,
}

#[derive(Default)]
struct ArchiveInner {
    segments: BTreeMap<Seq, Arc<Segment>>,
    /// The contiguous prefix from 1 that has been judged.
    prefix: Seq,
    max_epoch: u64,
    fenced: BTreeSet<Seq>,
    /// The first outcome (`Completed` or `Refused`) of each rid in an
    /// applied, unfenced segment: (seq, record index, the outcome's code:
    /// `None` for `Completed`, `Some(code)` for `Refused`).
    outcomes: HashMap<Rid, (Seq, usize, Option<Code>)>,
    /// Rids that have both a `Completed` and a `Refused` in applied, unfenced
    /// segments: (rid, the first outcome's seq and code, the contradicting
    /// one's seq and code).
    conflicts: Vec<Conflict>,
    /// How many segments landed, their decompressed bytes and their bytes as
    /// stored (each seq once): the size of the log the run wrote, for the
    /// sweeps' comparison of two builds (`SWEEP-SEGMENTS`).
    landed: (u64, u64, u64),
}

impl Default for ArchiveIndex {
    fn default() -> Self {
        Self {
            log: LogStore::new(Arc::new(object_store::memory::InMemory::new())),
            inner: Mutex::new(ArchiveInner::default()),
        }
    }
}

impl ArchiveIndex {
    /// Segment `seq` landed with this (sealed, compressed) body.
    pub fn note(&self, seq: Seq, body: &[u8]) {
        let Ok(plain) = self.log.open_segment(seq, body) else {
            return;
        };
        let Ok(seg) = segment::decode(&plain) else {
            return;
        };
        {
            let mut g = self.inner.lock().unwrap();
            if !g.segments.contains_key(&seq) {
                g.landed.0 += 1;
                g.landed.1 += plain.len() as u64;
                g.landed.2 += body.len() as u64;
            }
        }
        self.note_segment(seq, seg);
    }

    /// The segments that landed, their decompressed bytes and their bytes
    /// as stored.
    pub fn landed(&self) -> (u64, u64, u64) {
        self.inner.lock().unwrap().landed
    }

    /// As [`Self::note`], for a decoded segment (the unit tests').
    pub fn note_segment(&self, seq: Seq, seg: Segment) {
        let mut g = self.inner.lock().unwrap();
        g.segments.entry(seq).or_insert_with(|| Arc::new(seg));
        // Tolerate out-of-order arrival: only extend the prefix while the
        // next segment is here.
        while let Some(seg) = g.segments.get(&(g.prefix + 1)).cloned() {
            let seq = g.prefix + 1;
            if seg.epoch > 0 && seg.epoch < g.max_epoch {
                g.fenced.insert(seq);
            } else {
                for (i, rec) in seg.records.iter().enumerate() {
                    if let Some((rid, code)) = record_outcome(rec) {
                        match g.outcomes.get(&rid).copied() {
                            None => {
                                g.outcomes.insert(rid, (seq, i, code));
                            }
                            Some((s0, _, c0)) if c0.is_some() != code.is_some() => {
                                if !g.conflicts.iter().any(|(r, _, _)| *r == rid) {
                                    g.conflicts.push((rid, (s0, c0), (seq, code)));
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
                g.max_epoch = g.max_epoch.max(seg.epoch);
            }
            g.prefix = seq;
        }
    }

    /// Rids the applied log carries both a refusal and a completion of
    /// (plan 46 M1.3 step 5: a refusal journaled for an op the sticky holder
    /// ran): one description each.
    pub fn conflicting_outcomes(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .conflicts
            .iter()
            .map(|(rid, (s0, c0), (s1, c1))| {
                format!(
                    "refused_and_completed: rid {rid:?} has both a refusal and a completion in the log: {} in segment {s0}, then {} in segment {s1}",
                    c0.map_or("Completed".to_string(), |c| format!("Refused({c:?})")),
                    c1.map_or("Completed".to_string(), |c| format!("Refused({c:?})")),
                )
            })
            .collect()
    }

    /// Where the rid's outcome (`Completed { rid }` or `Refused { rid, .. }`)
    /// is in the applied unfenced prefix: (seq, record index, the segment).
    pub fn outcome_of(&self, rid: Rid) -> Option<(Seq, usize, Arc<Segment>)> {
        let g = self.inner.lock().unwrap();
        let (seq, i, _) = *g.outcomes.get(&rid)?;
        Some((seq, i, g.segments.get(&seq)?.clone()))
    }

    /// [`Self::outcome_of`] when the outcome is `want` (`None`: a
    /// completion; `Some(code)`: a refusal of that code).
    pub fn outcome_as(&self, rid: Rid, want: Option<Code>) -> Option<(Seq, usize, Arc<Segment>)> {
        let g = self.inner.lock().unwrap();
        let (seq, i, code) = *g.outcomes.get(&rid)?;
        (code == want).then_some(())?;
        Some((seq, i, g.segments.get(&seq)?.clone()))
    }

    /// The archive's first outcome of `rid`: its segment and code (`None`:
    /// a completion).
    pub fn outcome_code(&self, rid: Rid) -> Option<(Seq, Option<Code>)> {
        let g = self.inner.lock().unwrap();
        g.outcomes.get(&rid).map(|(seq, _, code)| (*seq, *code))
    }

    /// Whether segment `seq` is in the applied prefix and was fenced.
    pub fn is_fenced(&self, seq: Seq) -> bool {
        self.inner.lock().unwrap().fenced.contains(&seq)
    }
}

/// A record that is a rid's outcome: its rid, and `None` for a completion or
/// `Some(code)` for a refusal.
fn record_outcome(rec: &LogRecord) -> Option<(Rid, Option<Code>)> {
    match rec {
        LogRecord::Completed { rid } => Some((*rid, None)),
        LogRecord::Refused { rid, code } => Some((*rid, Some(*code))),
        _ => None,
    }
}

/// Whether `records` carry `rid`'s outcome as `want` (`None`: completed;
/// `Some(code)`: refused with that code).
fn carries_as(records: &[LogRecord], rid: Rid, want: Option<Code>) -> bool {
    records
        .iter()
        .any(|r| record_outcome(r) == Some((rid, want)))
}

/// The first outcome of `rid` in `records` that is not `want`, if any.
fn carries_other(records: &[LogRecord], rid: Rid, want: Option<Code>) -> Option<Option<Code>> {
    records
        .iter()
        .filter_map(record_outcome)
        .find(|(r, code)| *r == rid && *code != want)
        .map(|(_, code)| code)
}

/// How an outcome in the log reads in a violation's text.
fn describe_code(code: Option<Code>) -> String {
    match code {
        None => "a completion".into(),
        Some(c) => format!("a refusal ({})", c.posix_name()),
    }
}

// ---------------------------------------------------------------------
// I1.
// ---------------------------------------------------------------------

/// Plan 46 M1.1 step 7: the rule of the configuration an answer's commit
/// point belongs to, as the oracle finds it from the cluster's own state
/// (the archive, the lease object, the journals, the delegation table): the
/// core's claim (`RowCommit`) is compared with it, never taken from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Judged {
    /// A configuration with members: the holder and each of them hold the
    /// transaction.
    Group,
    /// A configuration with no member: an applied, unfenced log segment
    /// carries it.
    Archive,
    /// A non-stealable configuration (a designee, an epoch's hold owner).
    Final,
    /// A singleton: its own disk.
    Singleton,
}

impl Judged {
    pub fn name(&self) -> &'static str {
        match self {
            Judged::Group => "group",
            Judged::Archive => "archive_waits",
            Judged::Final => "final",
            Judged::Singleton => "singleton",
        }
    }
}

/// What an answer's judgement expected, from the answerer's state (plan §5.1's
/// table), for the per-rule counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    Class(Judged),
    /// No transaction of the rid anywhere (a refusal that journaled no
    /// `Refused`, or an acceptance with nothing behind it).
    Unjournaled,
}

impl Expected {
    pub fn name(&self) -> &'static str {
        match self {
            Expected::Class(c) => c.name(),
            Expected::Unjournaled => "unjournaled",
        }
    }
}

/// The transaction `T` carrying the rid's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRef {
    pub node: NodeId,
    pub first: u64,
    pub last: u64,
    pub origin: (u64, u64),
    pub epoch: u64,
    pub records: Vec<LogRecord>,
}

#[derive(Debug, Clone)]
pub struct Judgement {
    /// The first class that holds.
    pub class: Option<Judged>,
    pub expected: Expected,
    pub tx: Option<TxRef>,
    /// What each class check saw (for failures).
    pub detail: String,
    /// Plan 43 C2a: a transaction (the archive's, the answerer's journal,
    /// the lease holder's journal) carries the rid with another outcome
    /// than the answer's — judged whether or not a class holds.
    pub mismatch: Option<String>,
}

/// What the answering node's core says about itself at the answer: the
/// parts of the judgement that are the answerer's own state.
#[derive(Debug, Clone, Default)]
pub struct AnswererState {
    /// The lease epoch the answerer holds (an epoch hold's included).
    pub epoch: Option<u64>,
    /// A continuation epoch's hold is in force.
    pub epoch_held: bool,
    /// Per generation this node holds as a delegate: the backups its own
    /// store's table records for it (`DelegateNodeGroup`, plan 43 C2; empty: no
    /// record, the group is the delegate and the root). Read from the
    /// answerer's store, not from the core — the delegate counts a backup
    /// only once its own table names it, and the oracle must not take the
    /// core's word for it.
    pub deleg_groups: Vec<(u64, Vec<NodeId>)>,
    /// The generations this node holds as a delegate.
    pub holds_gens: Vec<u64>,
    /// The generations of the delegation table that are non-stealable
    /// (sticky grants and designations) of this node: never stolen by the
    /// root.
    pub non_stealable_gens: Vec<u64>,
}

impl AnswererState {
    pub fn of(core: &constellation_authority::Core, meta: &Meta, id: NodeId) -> Self {
        let view = core.deleg_view();
        Self {
            epoch: core.lease().epoch(),
            epoch_held: core.lease().epoch_held(),
            deleg_groups: recorded_groups(meta, view.mine.iter().map(|m| m.1)),
            holds_gens: view.mine.iter().map(|m| m.1).collect(),
            non_stealable_gens: meta
                .delegation_table()
                .iter()
                .filter(|d| d.node == id && d.non_stealable())
                .map(|d| d.gen)
                .collect(),
        }
    }
}

/// The groups `meta`'s table records for generations `gens` (plan 43 C2).
pub fn recorded_groups(meta: &Meta, gens: impl Iterator<Item = u64>) -> Vec<(u64, Vec<NodeId>)> {
    gens.map(|g| {
        let backups = meta
            .deleg_group(g)
            .ok()
            .flatten()
            .map(|r| r.backups)
            .unwrap_or_default();
        (g, backups)
    })
    .collect()
}

/// How an answer reads in the report: `ok`, `refused:<code>`, `exists`, ….
pub fn outcome_kind(outcome: &MutateOutcome) -> Option<OutcomeKind> {
    match outcome {
        MutateOutcome::Accepted { .. } => Some(OutcomeKind::Ok),
        MutateOutcome::Errno(c) => Some(OutcomeKind::Refused(*c)),
        MutateOutcome::Exists { .. } => Some(OutcomeKind::Exists),
        MutateOutcome::Conflict { .. } => Some(OutcomeKind::Conflict),
        MutateOutcome::LockLapsed => Some(OutcomeKind::LockLapsed),
        MutateOutcome::Abandoned => Some(OutcomeKind::Abandoned),
        // `Busy`, `NotHolder`, `Held`: not an answer.
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    Ok,
    Refused(Code),
    Exists,
    Conflict,
    LockLapsed,
    /// Plan 43 C2 (§5.3 item 5): settled — refused for good
    /// (`Refused { rid, ABANDONED_CODE }`); the client hears `EIO`.
    Abandoned,
}

impl OutcomeKind {
    pub fn name(&self) -> String {
        match self {
            OutcomeKind::Ok => "ok".into(),
            OutcomeKind::Refused(c) => format!("refused:{}", c.posix_name()),
            OutcomeKind::Exists => "exists".into(),
            OutcomeKind::Conflict => "conflict".into(),
            OutcomeKind::LockLapsed => "lock-lapsed".into(),
            OutcomeKind::Abandoned => "abandoned".into(),
        }
    }

    /// The outcome record the sequencer journals for this answer: `None`
    /// for a completion (`Completed`), `Some(code)` for a refusal
    /// (`Refused { code }`; an `Exists` hint is `EEXIST`, a conflict
    /// `ESTALE`, a lapsed grant `ENOLCK`: `core::client::LAPSED_CODE`, a
    /// settled op `ECANCELED`: `ABANDONED_CODE`).
    pub fn record_code(&self) -> Option<Code> {
        match self {
            OutcomeKind::Ok => None,
            OutcomeKind::Refused(c) => Some(*c),
            OutcomeKind::Exists => Some(Code::Exists),
            OutcomeKind::Conflict => Some(Code::Stale),
            OutcomeKind::LockLapsed => Some(Code::NoLock),
            OutcomeKind::Abandoned => Some(constellation_authority::ABANDONED_CODE),
        }
    }

    /// A refusal journals a `Refused` row, or none (D6).
    pub fn is_refusal(&self) -> bool {
        !matches!(self, OutcomeKind::Ok)
    }

    /// The code a refusal is counted under (`unjournaled_refusals`).
    pub fn refusal_name(&self) -> String {
        match self {
            OutcomeKind::Ok => "ok".into(),
            OutcomeKind::Refused(c) => c.posix_name().to_string(),
            OutcomeKind::Exists => "exists-hint".into(),
            OutcomeKind::Conflict => "conflict".into(),
            OutcomeKind::LockLapsed => "lock-lapsed".into(),
            OutcomeKind::Abandoned => "abandoned".into(),
        }
    }
}

/// How an answer's source is treated by the oracle (plan 43 C2a): a
/// sequencer's answer (a `MutateReply`, which carries no source, or an
/// `AnswerSource::Sequenced`) is judged; a relay is verified at the end of
/// the run; a node's own error is neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Treatment {
    pub judge: bool,
    pub relay_from: Option<NodeId>,
    pub claimed: Option<RowCommit>,
    pub unsourced: bool,
}

pub fn answer_treatment(source: Option<constellation_authority::AnswerSource>) -> Treatment {
    use constellation_authority::AnswerSource;
    let (judge, relay_from, claimed, unsourced) = match source {
        None => (true, None, None, false),
        Some(AnswerSource::Sequenced(class)) => (true, None, Some(class), false),
        Some(AnswerSource::Relayed { from }) => (false, Some(from), None, false),
        Some(AnswerSource::None) => (false, None, None, true),
    };
    Treatment {
        judge,
        relay_from,
        claimed,
        unsourced,
    }
}

/// Plan 43 C1-2: an answer that says it waited for a group or the archive
/// (`claim`: the class the core named, or `committed` on a forward's reply)
/// must be met by a class that holds (`Lone`, or an epoch hold's
/// non-stealability, does not meet it), and the answering row, still in the
/// answerer's journal, must carry the class the answer names (a gated row
/// answered with the gate off was re-tagged `Lone` and is answered `Lone`:
/// the core says so, D4). `rows`: the classes of the answerer's unshipped
/// transactions of the rid; `delegated`: the row is a delegate's or the
/// root's copy of one (`Lone` until C2/C3 move them: not compared).
/// Whether the row's rule is the answer class `c`'s (the version aside).
fn row_is_class(row: constellation_meta::RowCommit, c: RowCommit) -> bool {
    use constellation_authority::core::GroupOn;
    match c {
        RowCommit::Singleton { .. } => row.is_singleton(),
        RowCommit::Group {
            on: GroupOn::Archive,
            ..
        } => row.is_archive(),
        RowCommit::Group {
            on: GroupOn::Root, ..
        } => matches!(
            row,
            RowCommit::Group {
                on: GroupOn::Root,
                ..
            }
        ),
        RowCommit::Group {
            on: GroupOn::Delegate,
            ..
        } => matches!(
            row,
            RowCommit::Group {
                on: GroupOn::Delegate,
                ..
            }
        ),
        RowCommit::Group {
            on: GroupOn::Hold, ..
        } => row.is_hold(),
        RowCommit::Final => row.is_final(),
    }
}

pub fn check_claim(
    claim: Option<RowCommit>,
    committed: Option<bool>,
    judged: Option<Judged>,
    rows: &[constellation_meta::RowCommit],
    delegated: bool,
    unjournaled_refusal: bool,
) -> Option<String> {
    use constellation_authority::core::{commit_name, GroupOn};
    // A refusal no journal carries (a designation's `EROFS`/`EXDEV`, a
    // frozen epoch's `EROFS`, a fast path's `LockLapsed` at the write) is no
    // row's answer: its claim is the nominal rule of nothing journaled
    // (version 0, plan 46 M1.1 step 7), and `Singleton` judges it only
    // vacuously (a lone answerer's disk "holds" a row that is not there).
    // I1 exempts it the same way (`uncommitted`); whether it should have
    // been journaled is I4's (`unjournaled_then_landed`).
    if unjournaled_refusal {
        return None;
    }
    let gated = match (claim, committed) {
        (Some(c), _) => c.gated(),
        (None, Some(c)) => c,
        (None, None) => return None,
    };
    if gated && matches!(judged, Some(Judged::Singleton) | Some(Judged::Final)) {
        return Some(format!(
            "the answer claims a commit ({}) but only {} judges it",
            claim.map_or("committed", commit_name),
            judged.map_or("none", |c| c.name())
        ));
    }
    if rows.is_empty() || delegated {
        return None;
    }
    let ok = match claim {
        Some(RowCommit::Group {
            on: GroupOn::Delegate,
            ..
        }) => true,
        Some(c) => rows.iter().any(|r| row_is_class(*r, c)),
        None => rows.iter().any(|r| r.gated() == gated),
    };
    (!ok).then(|| {
        format!(
            "the answer says {} but the journal row of the rid is tagged {:?}",
            claim.map_or(if gated { "committed" } else { "uncommitted" }, commit_name),
            rows
        )
    })
}

#[derive(Debug, Clone)]
pub struct AckEntry {
    pub node: NodeId,
    pub rid: Rid,
    pub tick: u64,
    pub at_ms: u64,
    pub kind: OutcomeKind,
    /// The answer left the node as a `MutateReply` to a requester (the
    /// sequencer's answer), as opposed to its own client's reply.
    pub reply: bool,
    /// The requester passing on the answer of this sequencer (the core
    /// said so: `AnswerSource::Relayed`): not judged, verified at the end
    /// of the run against the sequencer's own judged answer.
    pub relay_from: Option<NodeId>,
    /// The class the core said the answer waited for
    /// (`AnswerSource::Sequenced`; none for a `MutateReply`, which carries
    /// no source).
    pub claimed: Option<RowCommit>,
    pub judged: Option<Judgement>,
}

impl AckEntry {
    pub fn is_relay(&self) -> bool {
        self.relay_from.is_some()
    }
}

/// Plan 43 C1-3, the C1-2/C1-3 review: the rids whose rollback the strict
/// durability check excuses — those the oracle itself judged answered as a
/// lone node's ([`committed_at_answer`]: no stronger class held, the
/// answerer's group was itself by the lease object it saw, no peer within
/// its budget), every judged answer of the rid that gave the outcome its
/// client heard. What the core *claims* is not consulted: a core that
/// wrongly claims `Lone` (the class-claim check judges that claim) must
/// not also hide the loss of a committed op.
///
/// An answer of another outcome is not the one rolled back: the rid run
/// again after the rollback, by rid, against a namespace without its
/// effect (`long-backup` seed 1063: a lone root's acknowledged unlink,
/// rolled back by the takeover of its successor, which refused the replay
/// `ENOENT`, the name gone meanwhile — a committed refusal the client
/// never heard). Without a client's answer (it gave up), every judged
/// answer counts.
pub fn lone_answered(acks: &[AckEntry]) -> HashSet<Rid> {
    let mut heard: BTreeMap<Rid, Vec<OutcomeKind>> = BTreeMap::new();
    for a in acks.iter().filter(|a| !a.reply) {
        heard.entry(a.rid).or_default().push(a.kind);
    }
    let mut lone: BTreeMap<Rid, bool> = BTreeMap::new();
    for a in acks.iter().filter(|a| !a.is_relay()) {
        let Some(j) = &a.judged else { continue };
        if heard.get(&a.rid).is_some_and(|k| !k.contains(&a.kind)) {
            continue;
        }
        *lone.entry(a.rid).or_insert(true) &= j.class == Some(Judged::Singleton);
    }
    lone.into_iter()
        .filter(|(_, l)| *l)
        .map(|(r, _)| r)
        .collect()
}

/// The transaction in `txs` carrying `rid`'s outcome as `want`.
fn find_tx(txs: &[BackupTx], rid: Rid, want: Option<Code>) -> Option<&BackupTx> {
    txs.iter().find(|t| carries_as(&t.records, rid, want))
}

fn journal_of(meta: &Meta) -> Vec<BackupTx> {
    Replica::journal_txs_from(meta, 0, usize::MAX)
}

/// The class an answer by `answerer` (in state `st`) of `rid`'s `outcome`
/// belongs to, judged against every node's store, the bucket's lease and
/// log **as they are now**, independently of the core's claim.
pub fn committed_at_answer(
    o: &Oracles,
    bus: &Bus,
    answerer: NodeId,
    st: &AnswererState,
    rid: Rid,
    outcome: &MutateOutcome,
) -> Judgement {
    // What the transaction must carry: the answer's own outcome (a
    // completion for an acceptance, a refusal of the answer's code for a
    // refusal). Whatever else carries the rid is a mismatch.
    let want = outcome_kind(outcome).and_then(|k| k.record_code());
    let mut detail: Vec<String> = Vec::new();
    let lease = o.bucket.peek_lease();
    let own = o.store(answerer);
    let own_txs = own.as_deref().map(journal_of).unwrap_or_default();
    // The lease holder's journal (a successor that re-shipped the row, or
    // the root that appended a delegate's row).
    let holder = lease.as_ref().map(|l| l.holder).filter(|h| *h != 0);
    let holder_txs: Vec<BackupTx> = match holder {
        Some(h) if h == answerer => own_txs.clone(),
        Some(h) => o.store(h).as_deref().map(journal_of).unwrap_or_default(),
        None => Vec::new(),
    };
    // T: the answerer's own transaction, else the holder's.
    let tx: Option<TxRef> = if let Some(t) = find_tx(&own_txs, rid, want) {
        Some(TxRef {
            node: answerer,
            first: t.first,
            last: t.last,
            origin: t.origin,
            epoch: st.epoch.unwrap_or(0),
            records: t.records.clone(),
        })
    } else {
        find_tx(&holder_txs, rid, want).map(|t| TxRef {
            node: holder.unwrap_or(0),
            first: t.first,
            last: t.last,
            origin: t.origin,
            epoch: lease.as_ref().map_or(0, |l| l.epoch),
            records: t.records.clone(),
        })
    };

    // Plan 46 M1.1 step 5: the group a root row commits by is the one of
    // the version it was journaled in.
    let row_backups = {
        let version_of = |node: NodeId| -> Option<u64> {
            o.store(node)?
                .journal_classes_of_rid(rid)
                .ok()?
                .into_iter()
                .find_map(|c| match c {
                    constellation_meta::RowCommit::Group {
                        config_version,
                        on: constellation_meta::GroupOn::Root,
                    } => Some(config_version),
                    _ => None,
                })
        };
        version_of(answerer)
            .or_else(|| holder.and_then(version_of))
            .and_then(|v| o.version_backups.lock().unwrap().get(&v).cloned())
    };
    let class = classify(
        o,
        bus,
        answerer,
        st,
        (rid, want),
        lease.as_ref(),
        row_backups.as_deref(),
        tx.as_ref(),
        &own_txs,
        &holder_txs,
        &mut detail,
    );
    // The same rid carried with another outcome anywhere an answer's
    // commit is read from.
    let mut others: Vec<String> = Vec::new();
    if let Some((seq, code)) = o.bucket.archive.outcome_code(rid) {
        if code != want {
            others.push(format!(
                "the archive (segment {seq}) carries {}",
                describe_code(code)
            ));
        }
    }
    let other_in = |txs: &[BackupTx]| {
        txs.iter()
            .find_map(|t| carries_other(&t.records, rid, want))
    };
    if let Some(code) = other_in(&own_txs) {
        others.push(format!(
            "the answerer's journal carries {}",
            describe_code(code)
        ));
    }
    if holder.is_some_and(|h| h != answerer) {
        if let Some(code) = other_in(&holder_txs) {
            others.push(format!(
                "the lease holder {}'s journal carries {}",
                holder.unwrap_or(0),
                describe_code(code)
            ));
        }
    }
    let mismatch = (!others.is_empty()).then(|| others.join(" and "));
    let expected = if tx.is_none() && class.is_none() {
        Expected::Unjournaled
    } else {
        expected_class(o, bus, answerer, st, rid, lease.as_ref(), tx.as_ref())
    };
    Judgement {
        class,
        expected,
        tx,
        detail: detail.join("; "),
        mismatch,
    }
}

#[allow(clippy::too_many_arguments)]
fn classify(
    o: &Oracles,
    bus: &Bus,
    answerer: NodeId,
    st: &AnswererState,
    (rid, want): (Rid, Option<Code>),
    lease: Option<&Lease>,
    row_backups: Option<&[NodeId]>,
    tx: Option<&TxRef>,
    own_txs: &[BackupTx],
    holder_txs: &[BackupTx],
    detail: &mut Vec<String>,
) -> Option<Judged> {
    // 1. Archive.
    if let Some((seq, _, _)) = o.bucket.archive.outcome_as(rid, want) {
        detail.push(format!("archive: in applied segment {seq}"));
        return Some(Judged::Archive);
    }
    detail.push("archive: not in an applied unfenced segment".into());

    // 2. RootGroup: the lease object's holder and every listed backup.
    // (A version that lists nobody is the configuration of a row stamped for
    // a listing that was out when it was journaled: the set the lease lists
    // now is the group it rests on.)
    let group = row_backups
        .filter(|g| !g.is_empty())
        .or(lease.map(|l| l.backups.as_slice()));
    match (lease, tx) {
        (Some(l), Some(t))
            if t.origin == (0, 0) && group.is_some_and(|g| !g.is_empty()) =>
        {
            match root_group(o, l, group.unwrap_or(&[]), (rid, want), holder_txs) {
                Ok(()) => {
                    detail.push("root-group: holder and every listed backup hold it".into());
                    return Some(Judged::Group);
                }
                Err(why) => detail.push(format!("root-group: {why}")),
            }
        }
        (Some(l), Some(t)) if t.origin == (0, 0) => detail.push(format!(
            "root-group: the lease at epoch {} (holder {}) lists no backup (the row's version lists {row_backups:?})",
            l.epoch, l.holder
        )),
        (None, _) => detail.push("root-group: no lease object".into()),
        (_, None) => detail.push("root-group: no transaction of the rid".into()),
        _ => detail.push("root-group: not a root row".into()),
    }

    // 3. DelegateNodeGroup.
    if let Some(t) = tx {
        let (g, i) = t.origin;
        if g != 0 {
            match deleg_group(o, answerer, st, (rid, want), g, i, t, lease, holder_txs) {
                Ok(why) => {
                    detail.push(format!("deleg-group: {why}"));
                    return Some(Judged::Group);
                }
                Err(why) => detail.push(format!("deleg-group: {why}")),
            }
        }
    }

    // 4. NonStealable: a designee, or an epoch's hold owner on a root row.
    if let Some(t) = tx {
        let (g, _) = t.origin;
        if g != 0 && st.non_stealable_gens.contains(&g) {
            detail.push(format!("non-stealable: holder of generation {g}"));
            return Some(Judged::Final);
        }
        if g == 0 && st.epoch_held {
            // Plan 46 M1.4 step 2: the hold is a group of the owner and a
            // member: committed once a member's store holds the row (as
            // streamed speculation of the hold's epoch, through the row's
            // last seq), not on the owner's disk alone.
            let epoch = st.epoch.unwrap_or(0);
            let member = o.stores.lock().unwrap().iter().find_map(|(n, m)| {
                (*n != answerer
                    && Replica::streamed_tip(&**m, epoch)
                        .ok()
                        .flatten()
                        .is_some_and(|tip| tip >= t.last))
                .then_some(*n)
            });
            if let Some(n) = member {
                detail.push(format!(
                    "hold-group: node {n}'s store holds the hold's journal through {}",
                    t.last
                ));
                return Some(Judged::Group);
            }
            detail.push("non-stealable: epoch hold owner, no member holds it".into());
            return Some(Judged::Final);
        }
    }

    // 5. Lone.
    if rid.node != answerer {
        detail.push(format!(
            "lone: rid is node {}'s, not the answerer's",
            rid.node
        ));
        return None;
    }
    let group_is_itself = match tx {
        // Plan 46 M1.2 step 13: no delegate is a singleton. The holder of the
        // generation has the root in its group at least; the singleton is
        // a lone root's alone (answering its own node's client for a row of
        // a delegate's stream it holds).
        Some(t) if t.origin.0 != 0 => {
            let holder = st.deleg_groups.iter().any(|(g, _)| *g == t.origin.0);
            if holder {
                detail.push(format!(
                    "lone: this node holds generation {} as its delegate; its group is never itself",
                    t.origin.0
                ));
            }
            !holder
        }
        Some(_) | None => match lease {
            Some(l) if l.holder == answerer && l.backups.is_empty() => true,
            Some(l) => {
                detail.push(format!(
                    "lone: lease holder {} lists backups {:?}",
                    l.holder, l.backups
                ));
                false
            }
            None => {
                detail.push("lone: no lease object".into());
                false
            }
        },
    };
    if !group_is_itself {
        return None;
    }
    if let Some(n) = peer_in_budget(o, bus, answerer) {
        detail.push(format!("lone: peer {n} is linked within the budget"));
        return None;
    }
    // (`own_txs` only feeds T; a Lone answer needs no copy elsewhere.)
    let _ = own_txs;
    detail.push("lone: its group is itself and no peer is within the budget".into());
    Some(Judged::Singleton)
}

/// A live node `answerer` is linked to within its RTT budget, if any, as
/// the node can know: judged by the peer directory the core was actually
/// given, and with no stability term. A link that has only just come up is
/// already a peer a backup could be had from: the core waits for the archive
/// meanwhile and never answers alone with a peer linked (i1-592), so an
/// answer given `Lone` within the stability window of a link coming up is a
/// violation.
///
/// The core's roster and links come from the driver's last poll, and the bus
/// is ahead of them: just after a partition healed or the other nodes
/// restarted the bus has the peer linked while the core was told roster
/// `[self]` or the link down. A core that was told of nobody is lone by its
/// own information (plan 43 Layer A: lone is no *known* peer in budget; the
/// rollback risk equals a genuine lone answer's, and the roster singleton of
/// plan 46 M2.3 removes the case). So an answerer with a directory entry
/// counts only the peers that entry reports up; one with no entry at all was
/// told nothing and counts every bus link (a core that was told nothing must
/// not judge itself lone).
fn peer_in_budget(o: &Oracles, bus: &Bus, answerer: NodeId) -> Option<NodeId> {
    let (budget, _) = o
        .budgets
        .lock()
        .unwrap()
        .get(&answerer)
        .copied()
        .unwrap_or((0, 0));
    let directory = o.directory.lock().unwrap();
    let up = directory.get(&answerer);
    bus.nodes().into_iter().find(|n| {
        *n != answerer
            && bus.linked(answerer, *n)
            && bus.rtt(answerer, *n) <= budget
            && up.is_none_or(|m| m.contains_key(n))
    })
}

/// RootGroup: the lease object's holder's journal and every listed
/// backup's tail hold the transaction (`Ok`), or why not.
fn root_group(
    o: &Oracles,
    l: &Lease,
    backups: &[NodeId],
    (rid, want): (Rid, Option<Code>),
    holder_txs: &[BackupTx],
) -> Result<(), String> {
    let Some(ht) = find_tx(holder_txs, rid, want) else {
        return Err(format!(
            "the lease holder {} (epoch {}) has no journal row of the rid",
            l.holder, l.epoch
        ));
    };
    backups_hold(o, l.epoch, backups, ht, Some((l.holder, holder_txs)))
}

/// Every backup in `backups` holds `ht` in its tail of `epoch` (or of the
/// epoch it now serves), or, being the lease holder, in its journal.
fn backups_hold(
    o: &Oracles,
    epoch: u64,
    backups: &[NodeId],
    ht: &BackupTx,
    holder: Option<(NodeId, &[BackupTx])>,
) -> Result<(), String> {
    for b in backups {
        if let Some((h, txs)) = holder {
            if h == *b && txs.iter().any(|t| t.records == ht.records) {
                continue;
            }
        }
        let Some(store) = o.store(*b) else {
            return Err(format!("listed backup {b} has no registered store"));
        };
        let mut epochs = vec![epoch];
        if let Ok(Some(role)) = store.backup_role() {
            if role.epoch != epoch {
                epochs.insert(0, role.epoch);
            }
        }
        let held = epochs.iter().any(|e| {
            store
                .backup_tail(*e)
                .map(|tail| tail.iter().any(|t| t.records == ht.records))
                .unwrap_or(false)
        });
        if !held {
            return Err(format!(
                "listed backup {b}'s tail (epochs {epochs:?}) lacks the row"
            ));
        }
    }
    Ok(())
}

/// A member (other than the hold's owner) of the hold of one of `epochs`
/// whose store held the hold's journal (its streamed copy) through `last` at
/// some event (`Oracles::note_streamed`). A node outside the hold's epoch
/// that streamed from the owner (a subscriber) is no member of its group.
fn hold_member_streamed(
    o: &Oracles,
    holder: Option<NodeId>,
    epochs: &[u64],
    last: u64,
) -> Option<NodeId> {
    let members = o.hold_members.lock().unwrap();
    o.streamed_seen
        .lock()
        .unwrap()
        .iter()
        .find_map(|((n, ep), tip)| {
            (Some(*n) != holder
                && epochs.contains(ep)
                && *tip >= last
                && members.get(ep).is_some_and(|m| m.contains(n)))
            .then_some(*n)
        })
}

/// DelegateNodeGroup: the delegate holds `(g, i)`; each recorded backup's delegate
/// tail holds it, or, with none recorded, the root's journal does and the
/// root's own configuration committed that copy: every backup the lease
/// object lists holds it (plan 46 §11, "M1.0 answers": a lone root's
/// journal is no second copy; with no backup the root commits by the
/// archive, judged before this).
#[allow(clippy::too_many_arguments)]
fn deleg_group(
    o: &Oracles,
    answerer: NodeId,
    st: &AnswererState,
    (rid, want): (Rid, Option<Code>),
    g: u64,
    i: u64,
    t: &TxRef,
    lease: Option<&Lease>,
    holder_txs: &[BackupTx],
) -> Result<String, String> {
    // The delegate's journal holds it — or the root appended it and the
    // delegate retired its row by that (`t` was found at the root then).
    let root_has = holder_txs
        .iter()
        .any(|x| x.origin == (g, i) && carries_as(&x.records, rid, want));
    if t.node != answerer && !root_has {
        return Err(format!("the row ({g},{i}) is not the answerer's"));
    }
    if t.node == answerer && !st.holds_gens.contains(&g) && !root_has {
        return Err(format!("the answerer holds no generation {g}"));
    }
    let backups: Vec<NodeId> = st
        .deleg_groups
        .iter()
        .filter(|(gen, _)| *gen == g)
        .flat_map(|(_, b)| b.iter().copied())
        .collect();
    if !backups.is_empty() {
        for b in &backups {
            let Some(store) = o.store(*b) else {
                return Err(format!("recorded backup {b} has no registered store"));
            };
            let held = store
                .deleg_backup_tail(g)
                .map(|tail| {
                    tail.iter()
                        .any(|d| d.idx == i && carries_as(&d.records, rid, want))
                })
                .unwrap_or(false);
            if !held {
                return Err(format!(
                    "recorded backup {b}'s delegate tail of generation {g} lacks index {i}"
                ));
            }
        }
        return Ok(format!("every recorded backup {backups:?} holds ({g},{i})"));
    }
    if root_has {
        // The root's copy counts when the TENURE THAT APPENDED the row
        // committed it, not by the lease at answer time: a row committed
        // under epoch 1's root and backup stays committed after a successor
        // sealed and re-shipped it (its lease may list no backup).
        let ht = holder_txs
            .iter()
            .find(|x| x.origin == (g, i) && carries_as(&x.records, rid, want))
            .expect("root_has");
        // Judge the tenure that appended the row, as recorded then.
        let Some((e, at_append, held)) = o.appended.lock().unwrap().get(&(g, i)).cloned() else {
            return Err(format!(
                "no backup recorded; the root journal holds ({g},{i}) but no append of it \
                 was recorded"
            ));
        };
        // The configuration the appending epoch ended with (or has now): a
        // backup added later in the epoch commits the row by its copy.
        let bs = o
            .epoch_backups
            .lock()
            .unwrap()
            .get(&e)
            .cloned()
            .unwrap_or(at_append);
        // Plan 46 M1.4 step 3: an epoch hold is a group of the owner and a
        // member, so the root's copy counts in a hold once a member's store
        // holds the row: as streamed speculation of the hold's epoch through
        // the row's last seq (the member-acked seq the core counts by,
        // `Core::config_committed_jseq`), or, for the delegate that is the
        // member, as its own journal row (the stream delivers it back and
        // the member's store already holds it). The owner's journal alone
        // does not count. The core counts by the acks while the hold is in
        // force when it computes the ack, so a hold that began after the
        // append counts too.
        let hold_now = lease.is_some_and(|l| o.epoch_holds.lock().unwrap().contains(&l.holder));
        let holder = lease.map(|l| l.holder);
        let mut epochs = vec![e];
        if let Some(le) = lease.map(|l| l.epoch).filter(|le| *le != e) {
            epochs.push(le);
        }
        // (Streamed speculation exists only in a hold: no gate on the
        // tenure's record, which may be noted after the hold ended.)
        if let Some(n) = hold_member_streamed(o, holder, &epochs, ht.last) {
            return Ok(format!(
                "no backup recorded; the root journal holds ({g},{i}) in an epoch hold \
                 and node {n}'s store held the hold's journal through {}",
                ht.last
            ));
        }
        // (The group of a hold is the owner and one member: two copies. The
        // delegate's own journal is the second one only if it is a member of
        // the hold's epoch; any other delegate's row is the root's alone.)
        let delegate_member = {
            let members = o.hold_members.lock().unwrap();
            epochs
                .iter()
                .any(|ep| members.get(ep).is_some_and(|m| m.contains(&answerer)))
        };
        if (held || hold_now) && t.node == answerer && Some(answerer) != holder && delegate_member {
            return Ok(format!(
                "no backup recorded; the root journal holds ({g},{i}) in an epoch hold \
                 and the delegate {answerer}, a member, holds the row in its own journal"
            ));
        }
        if bs.is_empty() {
            return Err(format!(
                "no backup recorded; the root journal holds ({g},{i}) but the tenure of \
                 epoch {e} that appended it listed no backup (only the archive commits a lone \
                 root's copy; a lone root's journal is no second copy)"
            ));
        }
        // The holder's own journal stands in only for a listed backup that
        // inherited the row by sealing; the appending tenure's backups must
        // hold it.
        match backups_hold(o, e, &bs, ht, lease.map(|l| (l.holder, holder_txs))) {
            Ok(()) => Ok(format!(
                "no backup recorded; the root journal holds ({g},{i}) and the tenure of \
                 epoch {e} committed it: every backup {bs:?} holds it"
            )),
            Err(w) => Err(format!(
                "no backup recorded; the root journal holds ({g},{i}) but the tenure of \
                 epoch {e} did not commit it: {w}"
            )),
        }
    } else {
        Err(format!(
            "no backup recorded and the root's journal lacks the appended row ({g},{i})"
        ))
    }
}

/// Plan 46 M1.1 steps 3 and 4: `row_version_matches_config`, the sim's form
/// of the model property. A journal row carries the version of the
/// configuration in force where it was journaled, and which configuration
/// that is follows from the rule (`Core::root_rule`, `Core::deleg_rule`), so
/// the oracle judges each row against the newest version of the namespace its
/// rule is stamped from:
///
/// - `Group{on: Delegate}`: the generation's `DelegateNodeGroup` configuration as
///   the journaling node's replica has applied it (`DelegateState::
///   group_version`; 0 while no record is applied).
/// - `Group{on: Root}`: the root's lease configuration (`config_version` of
///   the lease object) of the row's epoch. A row journaled while no lease is
///   held (epoch 0) carries the version of the lease the node last held, 0
///   if it never held one, "no configuration yet"
///   (`Core::root_config_version`). A takeover's re-journaled tail of the
///   predecessor carries the successor's configuration (the one in force in
///   the journal the rows are in: `Core::apply_backup_tail`), and so does a
///   fast-path row journaled in the window after `clear_holder_epoch` (the
///   version last published): no version-0 placeholder is accepted under a
///   lease.
/// - `Final` carries no version and is not judged.
/// - `Group{on: Archive}` and `Singleton`: both namespaces stamp them (the
///   root's rule: the lease's; a delegate's: its group's, and an archive row
///   of a delegate that has no group (`ack=s3`, or the generation ended) is
///   version 0, "no group"). A row of a delegation stream executed on a node
///   that held no lease (epoch 0) is the delegate's: its group's. A
///   root-journal row (no stream) is the lease's. A stream row journaled
///   under a lease (a delegate that is also the root, or the root's append)
///   may be either: a violation only when neither holds.
///
/// "In force" is the configuration the journaling node has applied: the lease
/// its core holds (a landing the holder has not read yet is not in force for
/// it: add-then-record journals the old version until the holder learns the
/// change, and the record commits under both), the `DelegateNodeGroup` record its
/// replica has applied. A delegate's group is judged by journal position
/// (`Meta::deleg_group_at`: the version applied when the row was journaled,
/// exactly); the lease's version by event (below; the lease has no per-position history to judge by,
/// and the event's lag is exactly the fast path's: it stamps the rule the core published at the
/// end of its previous event, so a root row's version is the lease's at that event or at this one). The version the node had applied at its previous
/// event is as good as the one at this event (a published rule the fast path
/// stamped, a record the store applied after the core's last read, which the
/// fast path refuses). An older version, one that existed but was
/// superseded before the previous event, is a violation: the row would wait
/// for a rule no longer in force. A row stamped from anything else (the
/// `per_row_class` control) is in neither.
#[derive(Debug, Default)]
pub struct RowVersions {
    /// The newest `config_version` a lease object carried, per epoch, as the
    /// bucket saw them land.
    lease_top: BTreeMap<u64, u64>,
    /// The lease version each node's core had applied at its previous event
    /// (`tick`).
    lease_seen: BTreeMap<NodeId, u64>,
    /// The `DelegateNodeGroup` version each node's replica had applied at its
    /// previous event, per generation it has a row of.
    group_seen: BTreeMap<(NodeId, u64), u64>,
    /// The highest `DelegateNodeGroup` version any store has held of a generation:
    /// the one in force when the generation ended within an event (its
    /// group row goes with it), which a row journaled before the end carries.
    group_top: BTreeMap<u64, u64>,
    /// The highest journal seq judged per node.
    judged_to: BTreeMap<NodeId, u64>,
    /// The rule each judged row (node, first seq) carried when it was
    /// judged: a row seen again under another rule was re-tagged.
    judged_class: BTreeMap<(NodeId, u64), constellation_meta::RowCommit>,
    /// Re-tags judged (plan 46 M1.1 review, SHOULD-1).
    pub retags: usize,
    /// Plan 46 M1.4 step 2: the continuation epochs a node's core has held
    /// as a hold. A hold's rows carry the epoch's number as their version
    /// (the epoch is the configuration; no lease CAS moves a hold's
    /// membership), so the hold's configuration is "an epoch some node held".
    hold_epochs: std::collections::BTreeSet<u64>,
    pub checked: usize,
    pub unstamped: usize,
    pub violations: Vec<String>,
}

impl RowVersions {
    /// `node`'s core holds the hold of continuation epoch `epoch`.
    pub fn hold_held(&mut self, epoch: u64) {
        self.hold_epochs.insert(epoch);
    }

    pub fn lease_landed(&mut self, epoch: u64, version: u64) {
        self.lease_top.insert(epoch, version);
    }

    /// `node`'s journal was wiped: its seqs start again.
    pub fn forget(&mut self, node: NodeId) {
        self.judged_to.remove(&node);
        self.judged_class.retain(|(n, _), _| *n != node);
    }

    /// An event of `node` is over: what its core can have read by now is the
    /// lag the next event's rows are judged with.
    pub fn tick(&mut self, node: NodeId, lease_now: u64, group_now: &dyn Fn(u64) -> u64) {
        self.lease_seen.insert(node, lease_now);
        let gens: Vec<(NodeId, u64)> = self
            .group_seen
            .keys()
            .filter(|(n, _)| *n == node)
            .copied()
            .collect();
        for (n, g) in gens {
            self.group_seen.insert((n, g), group_now(g));
        }
    }

    /// Judge the rows of `node`'s journal that are new since the last call
    /// (`rows`: `Meta::journal_rows_for_audit`), `group_now(gen)` being the
    /// `DelegateNodeGroup` version `node`'s replica has applied for `gen`.
    pub fn judge(
        &mut self,
        node: NodeId,
        rows: &[constellation_meta::AuditRow],
        lease_now: u64,
        group_now: &dyn Fn(u64) -> u64,
        group_max: &dyn Fn(u64) -> u64,
        group_at: &dyn Fn(u64, u64) -> Option<u64>,
    ) {
        let from = self.judged_to.get(&node).copied().unwrap_or(0);
        let lease_prev = self.lease_seen.get(&node).copied();
        // The group versions the node had applied at its previous event,
        // read once for the whole call: a generation's first row seeds its
        // entry below with the version applied *now*, which is no previous
        // event's for the rows of the same event after it (two rows of a new
        // generation journaled before its first `DelegateNodeGroup` record was
        // installed in that event: the second was judged against the record,
        // long-delegated-backup seed 634).
        let group_before: BTreeMap<u64, u64> = self
            .group_seen
            .iter()
            .filter(|((n, _), _)| *n == node)
            .map(|((_, g), v)| (*g, *v))
            .collect();
        // A row judged before and carrying another rule now was re-tagged
        // (`Meta::reclass_journal_tx`): the configuration it was journaled
        // in does not change with the re-tag, so it keeps its version (never
        // the placeholder 0); one that had none (`Final`) is judged as a new
        // row. Never judged again would let a re-tag drop the version
        // unseen (plan 46 M1.1 review, SHOULD-1).
        let mut rejudge: Vec<u64> = Vec::new();
        for row in rows.iter().filter(|r| r.first <= from) {
            let Some(before) = self.judged_class.get(&(node, row.first)).copied() else {
                continue;
            };
            if before == row.class {
                continue;
            }
            self.retags += 1;
            self.judged_class.insert((node, row.first), row.class);
            match (before.config_version(), row.class.config_version()) {
                // Plan 46 M1.4 step 2: a row answered in a hold is re-tagged
                // the hold's (an older configuration's row answered by the
                // hold's rule), and a hold's row answered after it ended is
                // re-tagged by the node's rule then: the hold's epoch is a
                // configuration of its own.
                (Some(_), Some(_)) if before.is_hold() => {}
                (Some(_), Some(_)) if row.class.is_hold() => rejudge.push(row.first),
                (None, _) => rejudge.push(row.first),
                (Some(was), Some(now)) if was == now => {}
                (Some(_), None) => {}
                (Some(was), Some(now)) => self.violations.push(format!(
                    "row_version_matches_config: node {node} journal seq {} was re-tagged \
                     {before:?} -> {:?}: a re-tag keeps the row's version ({was}), not {now}",
                    row.first, row.class
                )),
            }
        }
        let present: std::collections::BTreeSet<u64> = rows.iter().map(|r| r.first).collect();
        self.judged_class
            .retain(|(n, s), _| *n != node || present.contains(s));
        for row in rows
            .iter()
            .filter(|r| r.first > from || rejudge.contains(&r.first))
        {
            let (seq, epoch, gen, idx, class) = (row.first, row.epoch, row.gen, row.idx, row.class);
            if seq > from {
                self.judged_to.insert(node, seq);
            }
            self.judged_class.insert((node, seq), class);
            if gen != 0 {
                let top = self.group_top.entry(gen).or_default();
                *top = (*top).max(group_max(gen));
            }
            let Some(v) = class.config_version() else {
                // `Final`: no configuration to carry.
                self.unstamped += 1;
                continue;
            };
            self.checked += 1;
            // The lease the core holds, else the one it last held
            // (`Core::root_config_version`), whatever the row's epoch: a row
            // journaled at epoch 0 after the lease was lost (the window after
            // `clear_holder_epoch`, an inbox drain after a deposition) belongs
            // to the last tenure's configuration, and a node that never held
            // one has version 0, "no configuration yet".
            let in_lease = v == lease_now || Some(v) == lease_prev;
            let group_prev = group_before.get(&gen).copied().unwrap_or(0);
            // The version the node's replica had applied at the row's journal
            // position (`Meta::deleg_group_at`), exactly: a stamp one record
            // behind is caught even when the record was applied in the same
            // event. Without a recorded history (a replica rebuilt from the
            // log) the node's applied version now or at its previous event.
            let ended = group_now(gen) == 0 && v == self.group_top.get(&gen).copied().unwrap_or(0);
            let in_group = gen != 0
                && match group_at(gen, seq) {
                    Some(in_force) => v == in_force,
                    None => v == group_now(gen) || v == group_prev || ended,
                };
            let ok = match class {
                constellation_meta::RowCommit::Group {
                    on: constellation_meta::GroupOn::Delegate,
                    ..
                } => in_group,
                constellation_meta::RowCommit::Group {
                    on: constellation_meta::GroupOn::Root,
                    ..
                } => in_lease,
                // The hold's configuration is its epoch's number.
                constellation_meta::RowCommit::Group {
                    on: constellation_meta::GroupOn::Hold,
                    ..
                } => self.hold_epochs.contains(&v),
                // A delegate's archive row with no group: version 0.
                constellation_meta::RowCommit::Group {
                    on: constellation_meta::GroupOn::Archive,
                    ..
                } if gen != 0 && v == 0 => true,
                _ if gen == 0 => in_lease,
                _ if epoch == 0 => in_group,
                _ => in_lease || in_group,
            };
            if gen != 0 {
                self.group_seen.entry((node, gen)).or_insert(group_now(gen));
            }
            if !ok {
                self.violations.push(format!(
                    "row_version_matches_config: node {node} journal seq {seq} (epoch {epoch}, \
                     stream ({gen},{idx})) {class:?}: version {v} is not the configuration in \
                     force (the node's lease: {lease_now}, at its previous event {lease_prev:?}, \
                     the newest landed in the epoch {:?}; the generation's group on this node: \
                     {}, before {group_prev})",
                    self.lease_top.get(&epoch),
                    group_now(gen)
                ));
            }
        }
    }
}

/// The class the core would be expected to give the answer (plan §5.1's
/// table), from the answerer's state.
fn expected_class(
    o: &Oracles,
    bus: &Bus,
    answerer: NodeId,
    st: &AnswererState,
    rid: Rid,
    lease: Option<&Lease>,
    tx: Option<&TxRef>,
) -> Expected {
    let g = tx.map_or(0, |t| t.origin.0);
    if g != 0 {
        // Plan 46 M1.2 step 13: no delegate answers from its disk alone;
        // every answer waits for the group (the root's append, when no
        // backup is recorded), a designation's or sticky grant's is final.
        let holder = st.deleg_groups.iter().any(|(gen, _)| *gen == g);
        return Expected::Class(if st.non_stealable_gens.contains(&g) {
            Judged::Final
        } else if !holder && rid.node == answerer && peer_in_budget(o, bus, answerer).is_none() {
            // (A lone root answering its own node's client for a row of a
            // delegate's stream it holds.)
            Judged::Singleton
        } else {
            Judged::Group
        });
    }
    if st.epoch_held {
        return Expected::Class(Judged::Final);
    }
    let Some(l) = lease else {
        return Expected::Class(Judged::Archive);
    };
    if l.ack_policy == AckPolicy::S3 {
        return Expected::Class(Judged::Archive);
    }
    if !l.backups.is_empty() {
        return Expected::Class(Judged::Group);
    }
    if peer_in_budget(o, bus, answerer).is_some() {
        // The bring-up rule: while a backup could be had, wait for the
        // archive.
        return Expected::Class(Judged::Archive);
    }
    if rid.node == answerer {
        Expected::Class(Judged::Singleton)
    } else {
        // Plan 43 C2's rule: a lone root answers another node's forward
        // from the archive (today such an answer is a violation).
        Expected::Class(Judged::Archive)
    }
}

// ---------------------------------------------------------------------
// I2 (b): one copy of a rid in the cluster.
// ---------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct NodeCopies {
    pub queued: BTreeSet<Rid>,
    /// The queued rids whose entry stands for a committed answer (plan 43
    /// C1-4: the class is gated).
    pub queued_gated: BTreeSet<Rid>,
    pub executing: BTreeSet<Rid>,
}

/// Where each rid is queued for execution, per node, and the rids that were
/// ever queued on two nodes at once.
#[derive(Debug, Default)]
pub struct IdentityLedger {
    per_node: BTreeMap<NodeId, NodeCopies>,
    /// Plan 43 C1-4: a rid queued for execution on two nodes at once where a
    /// queued copy stands for a committed answer (what I2 (b) judges from C1).
    pub violations: BTreeMap<Rid, String>,
    /// The same pairs where no copy stands for a committed answer: the lone
    /// branch's and the delegations' (C2/C3 close them); counted, not judged.
    pub ungated: BTreeMap<Rid, String>,
    /// Plan 46 M0.2 review: rids `repair drop-held` rolled back and left
    /// queued with their op (a row of the group classes whose answer may
    /// have left: the entry is the only copy that keeps an acknowledged
    /// effect). Its requester, whose answer may never have arrived, retries
    /// by rid: gated behind the entry where it is queued, or answered from
    /// the entry's execution by the rid's outcome anywhere else — one
    /// execution either way, which the exactly-once check of the log
    /// judges (the requester's own queue, if its shadow was rolled back
    /// too, replays by the same rid). So a pair that includes such a rid is
    /// counted with the ungated ones, not judged.
    pub kept: BTreeSet<Rid>,
    /// L2: rids another node queued for execution with a gated class (a
    /// committed answer's entry), once each, with where and when. Counted,
    /// never judged: a deposed root's rebuild used to queue the gated copy
    /// of a delegate's row it had appended, which no oracle pair caught
    /// where no re-ask overlapped it.
    pub foreign_gated: BTreeMap<Rid, String>,
}

impl IdentityLedger {
    /// Node `node`'s replay queue and executing set after an event.
    pub fn update(
        &mut self,
        node: NodeId,
        queued: BTreeSet<Rid>,
        queued_gated: BTreeSet<Rid>,
        executing: BTreeSet<Rid>,
        tick: u64,
    ) {
        let copies = self.per_node.entry(node).or_default();
        if copies.queued == queued
            && copies.queued_gated == queued_gated
            && copies.executing == executing
        {
            return;
        }
        copies.queued = queued;
        copies.queued_gated = queued_gated;
        copies.executing = executing;
        let mine = copies.clone();
        for rid in &mine.queued_gated {
            if rid.node != node && !self.kept.contains(rid) {
                self.foreign_gated.entry(*rid).or_insert_with(|| {
                    format!("rid {rid:?} queued gated on {node} at tick {tick}")
                });
            }
        }
        let rids: BTreeSet<Rid> = mine.queued.union(&mine.executing).copied().collect();
        for rid in rids {
            if self.violations.contains_key(&rid) {
                continue;
            }
            for (other, c) in &self.per_node {
                if *other == node || !(c.queued.contains(&rid) || c.executing.contains(&rid)) {
                    continue;
                }
                let kinds = |c: &NodeCopies| {
                    let mut k = Vec::new();
                    if c.queued.contains(&rid) {
                        k.push("queued");
                    }
                    if c.executing.contains(&rid) {
                        k.push("executing");
                    }
                    k.join("+")
                };
                let text = format!(
                    "I2 one_identity_per_op: rid {rid:?} queued on {node} ({}) and {other} ({}) at tick {tick}",
                    kinds(&mine),
                    kinds(c)
                );
                if self.kept.contains(&rid) {
                    // (Counted, not judged: see `kept`.)
                    self.ungated.entry(rid).or_insert(text);
                } else if mine.queued_gated.contains(&rid) || c.queued_gated.contains(&rid) {
                    self.violations.insert(rid, text);
                } else {
                    self.ungated.entry(rid).or_insert(text);
                }
                break;
            }
        }
    }

    /// The operator's drop kept `rids` queued with their op ([`Self::kept`]).
    /// The ledger saw the queue change before the drop's answer reached the
    /// operator: whatever it judged of these rids in between is counted
    /// instead.
    pub fn keep(&mut self, rids: impl IntoIterator<Item = Rid>) {
        for rid in rids {
            self.kept.insert(rid);
            if let Some(text) = self.violations.remove(&rid) {
                self.ungated.entry(rid).or_insert(text);
            }
        }
    }

    /// The process died: what it was executing is gone; its persisted
    /// queue runs again at restart.
    pub fn crash(&mut self, node: NodeId) {
        if let Some(c) = self.per_node.get_mut(&node) {
            c.executing.clear();
        }
    }

    /// A restart without the journal: nothing is kept.
    pub fn wipe(&mut self, node: NodeId) {
        self.per_node.remove(&node);
    }
}

// ---------------------------------------------------------------------
// The end of a run: what the oracles counted.
// ---------------------------------------------------------------------

/// What the oracles found in one run (the strict flags decide which of
/// them fail it).
#[derive(Debug, Default)]
pub struct Findings {
    pub acks_by_class: BTreeMap<String, usize>,
    pub uncommitted_acks: BTreeMap<String, usize>,
    /// I2 (c) answers the operator's `abandon-group` gave up on purpose (the
    /// rid is in the operator-lost set): counted here, not judged.
    pub i2c_operator_lost: usize,
    pub unjournaled_refusals: BTreeMap<String, usize>,
    /// Plan 43 C2a, I1: answers whose rid a transaction carries with
    /// another outcome (by answer kind), and relays with no judged answer
    /// of the sequencer they name behind them.
    pub mismatched_acks: BTreeMap<String, usize>,
    pub unverified_relays: usize,
    /// Plan 43 C2a, D6 of C0: an unjournaled refusal answered to a client
    /// whose rid the log later completes (a failure under `strict_settle`).
    pub unjournaled_landed: Vec<String>,
    /// I1: the descriptions, in emission order.
    pub i1: Vec<String>,
    /// Plan 43 C1-2: answers whose class claim is not borne out
    /// (`strict_commit` fails the run on one).
    pub class_claim: Vec<String>,
    /// Plan 46 M1.1 step 3: rows whose version is not their configuration's
    /// (`strict_commit` fails the run on one).
    pub row_version: Vec<String>,
    /// I2 (b): one description per rid queued on two nodes (a copy standing
    /// for a committed answer).
    pub i2: Vec<String>,
    /// I2 (b) where no copy stands for a committed answer (counted).
    pub i2_ungated: Vec<String>,
    /// L2: another node's rid queued with a gated class (counted).
    pub i2_foreign_gated: Vec<String>,
    /// I2 (c): one description per rid not completed verbatim.
    pub i2c: Vec<String>,
    pub i4: Vec<String>,
    pub i5: Vec<String>,
    /// Plan 46 M1.3 step 13: I5's operator branch. A rolled-back row of a
    /// stream the operator abandoned, past the cut the log gave it: the cost
    /// the abandon says it has (counted, not a violation).
    pub i5_operator: Vec<String>,
    /// ADR-47, check 1 (`nonholder_refused_a_designee_ran`): a rid the root
    /// refused for good under a non-stealable grant that a non-stealable
    /// generation executed and the log completes, with no client submission
    /// of it after the refusal.
    pub fence: Vec<String>,
    /// The same rows when their answer was given after the abandon was
    /// answered (a holder that came back and had not learned the end): not
    /// the cost the operator accepted, counted apart from `i5_operator` (the
    /// holder's `Final` rows are queued for replay by rid, never dropped).
    pub i5_after_end: Vec<String>,
    /// I6 (plan 43 C7b): TTL claims of a live lease.
    pub i6: Vec<String>,
    /// Plan 46 M1.2 step 3, `acked_at_or_below_cut` (`cut.rs`): answers for a
    /// row past its stream's cut; the rows judged and the abandon-excused.
    pub cut: Vec<String>,
    pub cut_judged: usize,
    pub cut_abandoned: usize,
    pub cut_refusals: usize,
    /// Plan 46 M1.3 step 4, `sticky_ends_only_by_holder_or_operator`
    /// (`sticky.rs`): sticky streams that ended otherwise, and the counts.
    pub sticky: Vec<String>,
    pub sticky_granted: usize,
    pub sticky_released: usize,
    pub sticky_abandoned: usize,
    pub sticky_claimed: usize,
}

impl Findings {
    /// The first five violations of each oracle.
    pub fn examples(&self) -> Vec<String> {
        [
            &self.i1,
            &self.class_claim,
            &self.row_version,
            &self.i2,
            &self.i2c,
            &self.i4,
            &self.unjournaled_landed,
            &self.i5,
            &self.i6,
            &self.cut,
            &self.sticky,
        ]
        .into_iter()
        .flat_map(|v| v.iter().take(5).cloned())
        .collect()
    }
}

/// The rids of `dropped` (those `repair drop-held` rolled back) whose effect
/// survives: the log has an outcome of the rid — it completes it, or carries
/// its refusal, the drop's own (`Io`, the dropped write, whose conflict copy
/// keeps what it wrote) or its replay's (the state it was replayed on no
/// longer admits it: replays refused are the existing contract, and a
/// content-bearing one makes a conflict copy). A dropped rid the log has no
/// outcome of is an acknowledged effect lost, and is judged as before.
pub fn survivors_of_drop(log: &super::check::Oracle, dropped: &BTreeSet<Rid>) -> BTreeSet<Rid> {
    let refused: HashSet<Rid> = log
        .unfenced
        .values()
        .flat_map(|seg| seg.records.iter())
        .filter_map(|rec| match rec {
            LogRecord::Refused { rid, .. } => Some(*rid),
            _ => None,
        })
        .collect();
    dropped
        .iter()
        .filter(|rid| log.completed_at.contains_key(rid) || refused.contains(rid))
        .copied()
        .collect()
}

/// The rids a run's operator abandon gave up beyond those a client saw
/// return: OK answers under a group class whose transaction is a delegate's
/// stream row (`origin` other than the holder's own `(0, 0)`) that the log
/// never completed. Only a dead delegate group held such a row; a row a
/// live group committed is in the log, and the holder's own rows are never
/// in this set.
pub fn unlogged_delegate_rows(o: &Oracles, log: &super::check::Oracle) -> BTreeSet<Rid> {
    o.acks
        .lock()
        .unwrap()
        .iter()
        .filter(|a| a.kind == OutcomeKind::Ok && !a.is_relay())
        .filter(|a| {
            a.judged.as_ref().is_some_and(|j| {
                j.class == Some(Judged::Group) && j.tx.as_ref().is_some_and(|t| t.origin.0 != 0)
            })
        })
        .filter(|a| !log.completed_at.contains_key(&a.rid))
        .map(|a| a.rid)
        .collect()
}

/// Tally the run's answers and run the end-of-run oracles: I2 (c) over the
/// answers' transactions and the log, I4 over the in-doubt answers and the
/// invocations, I5 over the rolled-back set (each rid with the tick of its
/// latest rollback: only an answer given before it was rolled back).
/// `operator_lost`: the rids the operator's `abandon-group` gave up (exempt
/// from I2 (c), counted in `i2c_operator_lost`).
/// `converged`: the run reached quiescence, so a group-answered rid the log
/// never completed is absent for good.
#[allow(clippy::too_many_arguments)]
pub fn end_of_run(
    o: &Oracles,
    log: &super::check::Oracle,
    in_doubt: &[(NodeId, Rid, u64, u64)],
    rolled_back: &HashMap<Rid, u64>,
    dropped: &BTreeSet<Rid>,
    operator_lost: &BTreeSet<Rid>,
    abandoned_at: &BTreeMap<u64, u64>,
    converged: bool,
) -> Findings {
    // `repair drop-held` is the operator's action: it rolls back what
    // depended on a lost write and replays it by rid after the write's
    // conflict copy, so a dropped rid is neither verbatim (I2c) nor
    // spared a rollback (I5) — but only if its effect survives
    // ([`survivors_of_drop`]).
    let survivors = survivors_of_drop(log, dropped);
    let survives = |rid: &Rid| survivors.contains(rid);
    let mut f = Findings {
        i1: o.i1_violations.lock().unwrap().clone(),
        class_claim: o.claim_violations.lock().unwrap().clone(),
        row_version: o.row_versions.lock().unwrap().violations.clone(),
        i6: o.leases.violations.lock().unwrap().clone(),
        ..Findings::default()
    };
    {
        // (One guard: two in the literal above would deadlock.)
        let ledger = o.identity.lock().unwrap();
        f.i2 = ledger.violations.values().cloned().collect();
        f.i2_ungated = ledger.ungated.values().cloned().collect();
        f.i2_foreign_gated = ledger.foreign_gated.values().cloned().collect();
    }
    let acks = o.acks.lock().unwrap();
    let audit = o.fence_audit.lock().unwrap();
    f.fence = fence_findings(&audit, &acks, log);
    // Each rid's first OK answer judged other than `Lone` (its tick).
    let mut non_lone_ok: BTreeMap<Rid, u64> = BTreeMap::new();
    let mut verbatim: BTreeMap<Rid, String> = BTreeMap::new();
    let mut verbatim_seen: HashSet<Rid> = HashSet::new();
    for a in acks.iter().filter(|a| !a.is_relay()) {
        let Some(j) = &a.judged else { continue };
        let class = j.class.map_or("none", |c| c.name());
        *f.acks_by_class.entry(class.to_string()).or_default() += 1;
        if j.mismatch.is_some() {
            *f.mismatched_acks.entry(a.kind.name()).or_default() += 1;
        }
        if j.class.is_none() {
            if j.expected == Expected::Unjournaled && a.kind.is_refusal() {
                *f.unjournaled_refusals
                    .entry(a.kind.refusal_name())
                    .or_default() += 1;
                // `EROFS` (a frozen epoch's refusal) is not final: the
                // caller retries the op, the sim's client under the same
                // rid, and the op then runs.
                // (ADR-47: a designation's refusal that reached the client
                // is final, unless the client submitted the rid again; a
                // forward's reply the requester may hold back is not.)
                let retried = a.kind == OutcomeKind::Refused(Code::ReadOnly)
                    && (a.reply
                        || !audit.refusals.iter().any(|r| r.0 == a.rid && !r.2)
                        || audit.submits.iter().any(|s| s.0 == a.rid && s.1 > a.tick));
                if let Some((seq, i)) = log.completed_at.get(&a.rid).filter(|_| !retried) {
                    f.unjournaled_landed.push(format!(
                        "I4 unjournaled_then_landed: rid {:?} was refused ({}) by node {} at t={} ms \
                         (tick {}) with no refusal in any journal, and the log completes it at \
                         segment {seq} record {i} (answered {}; what the judgement saw: {})",
                        a.rid,
                        a.kind.name(),
                        a.node,
                        a.at_ms,
                        a.tick,
                        if a.reply {
                            "as a forward's reply"
                        } else {
                            "to its own client"
                        },
                        j.detail
                    ));
                }
            } else {
                *f.uncommitted_acks
                    .entry(format!("{}/{}", j.expected.name(), a.kind.name()))
                    .or_default() += 1;
            }
        }
        if a.kind == OutcomeKind::Ok && j.class != Some(Judged::Singleton) {
            let t = non_lone_ok.entry(a.rid).or_insert(a.tick);
            *t = (*t).min(a.tick);
        }
        // I2 (c): an OK answered under a group class names a transaction
        // the log must carry as it was committed.
        if a.kind == OutcomeKind::Ok && j.class == Some(Judged::Group) {
            if let Some(t) = &j.tx {
                if operator_lost.contains(&a.rid) {
                    // Given up on purpose by the operator's `abandon-group`
                    // (only that group held the row): exactly these rids.
                    if verbatim_seen.insert(a.rid) {
                        f.i2c_operator_lost += 1;
                    }
                } else if let Some(why) =
                    not_verbatim(log, a.rid, t, converged).filter(|_| !survives(&a.rid))
                {
                    verbatim.entry(a.rid).or_insert_with(|| {
                        format!(
                            "I2c verbatim: rid {:?} answered OK by node {} at t={} ms (tick {}) \
                             under {} (T = node {} epoch {} rows {}..={} origin {:?}): {why}",
                            a.rid,
                            a.node,
                            a.at_ms,
                            a.tick,
                            class,
                            t.node,
                            t.epoch,
                            t.first,
                            t.last,
                            t.origin
                        )
                    });
                }
            }
        }
    }
    f.i2c = verbatim.into_values().collect();
    // Relays (plan 43 C2a): a requester passing an answer on must have a
    // sequencer's judged answer of the same outcome behind it, emitted
    // earlier — else the client heard an answer nobody gave.
    for a in acks.iter().filter(|a| a.is_relay()) {
        let from = a.relay_from.unwrap_or(0);
        let behind = acks.iter().any(|e| {
            e.node == from
                && e.rid == a.rid
                && e.reply
                && e.tick < a.tick
                && e.judged.is_some()
                && e.kind.record_code() == a.kind.record_code()
        });
        if !behind {
            f.unverified_relays += 1;
            let others: Vec<String> = acks
                .iter()
                .filter(|e| e.rid == a.rid)
                .map(|e| {
                    format!(
                        "node {} tick {} {} reply={} relay_from={:?} judged={}",
                        e.node,
                        e.tick,
                        e.kind.name(),
                        e.reply,
                        e.relay_from,
                        e.judged.is_some()
                    )
                })
                .collect();
            f.i1.push(format!(
                "I1 acked_implies_committed: relay of rid {:?} ({}) from node {from} by node {} \
                 at t={} ms (tick {}) with no judged answer of node {from} behind it \
                 (every answer of the rid: {})",
                a.rid,
                a.kind.name(),
                a.node,
                a.at_ms,
                a.tick,
                others.join("; ")
            ));
        }
    }
    // I4.
    let invokes = o.invokes.lock().unwrap().clone();
    f.i4 = super::check::no_late_landing(in_doubt, &invokes, log).1;
    // Plan 43 C2 (§7.1 item 6): a settled answer promises the op never
    // takes effect: no completion of the rid in the log, wherever it lands.
    f.i4.extend(super::check::no_settled_landing(&acks, log));
    // I5: acknowledged OK, not as a Lone answer, and rolled back after.
    // (A rollback before the answer is not of it: a lone node's row rolled
    // back by its deposition, then its replay answered by the group.)
    let cuts = super::cut::StreamCuts::of_log(log);
    let refused_rids: HashSet<Rid> = log
        .unfenced
        .values()
        .flat_map(|seg| seg.records.iter())
        .filter_map(|rec| match rec {
            LogRecord::Refused { rid, .. } => Some(*rid),
            _ => None,
        })
        .collect();
    for (rid, at) in rolled_back {
        if non_lone_ok.get(rid).is_some_and(|t| t < at) {
            let Some(a) = acks.iter().rev().find(|a| {
                a.rid == *rid
                    && a.tick < *at
                    && !a.is_relay()
                    && a.kind == OutcomeKind::Ok
                    && a.judged
                        .as_ref()
                        .is_some_and(|j| j.class != Some(Judged::Singleton))
            }) else {
                continue;
            };
            if survives(rid) {
                continue;
            }
            // Plan 46 M1.3 step 13 (the operator's I5): a row of a stream
            // the operator abandoned, past the cut the log gave it, is what
            // the abandon costs; its holder's replay may be refused (a
            // conflict copy). Counted, not judged.
            if let Some((gen, idx)) = a
                .judged
                .as_ref()
                .and_then(|j| j.tx.as_ref())
                .map(|t| t.origin)
            {
                if gen != 0
                    && cuts.abandoned.contains(&gen)
                    && cuts.cut.get(&gen).is_some_and(|c| idx > *c)
                {
                    if abandoned_at.get(&gen).is_some_and(|t0| a.at_ms > *t0) {
                        // A returned holder's row the log completed by its
                        // rid was replayed, as the contract says: not lost.
                        if a.judged
                            .as_ref()
                            .is_some_and(|j| j.class == Some(Judged::Final))
                            && log.completed_at.contains_key(rid)
                        {
                            continue;
                        }
                        // No record of the rid at all: the row vanished,
                        // neither replayed nor refused.
                        if !log.completed_at.contains_key(rid) && !refused_rids.contains(rid) {
                            f.i5.push(format!(
                                "I5 after the end: rid {rid:?} (row {idx} of abandoned generation {gen}, past its cut) \
                                 answered at t={} ms, after the abandon was answered at t={} ms, rolled back, \
                                 and the log has neither its completion nor a refusal of it",
                                a.at_ms, abandoned_at[&gen]
                            ));
                            continue;
                        }
                        f.i5_after_end.push(format!(
                            "I5 after the end: rid {rid:?} (row {idx} of abandoned generation {gen}, past its cut) \
                             answered at t={} ms, after the abandon was answered at t={} ms, rolled back",
                            a.at_ms, abandoned_at[&gen]
                        ));
                        continue;
                    }
                    f.i5_operator.push(format!(
                        "I5 operator: rid {rid:?} (row {idx} of abandoned generation {gen}, past its cut) \
                         rolled back"
                    ));
                    continue;
                }
            }
            // Plan 46 M0.2 (R7, variant 3 (i)): a non-stealable sequencer's
            // OK (an epoch hold's) is uncommitted by design, and its owner's
            // queue is the rid's one copy: the requester's shadow of it is
            // rolled back when the owner's replay lands and retires it, not
            // lost. The rollback is a violation only if the log never
            // completes the rid (its replay refused, or nobody's).
            if a.judged
                .as_ref()
                .is_some_and(|j| j.class == Some(Judged::Final))
                && log.completed_at.contains_key(rid)
            {
                continue;
            }
            let class = a
                .judged
                .as_ref()
                .map_or("none", |j| j.class.map_or("none", |c| c.name()));
            f.i5.push(format!(
                "I5 speculation_only_lone: rid {rid:?} was acknowledged by node {} at t={} ms \
                 (tick {}, {}; class {class}) and rolled back",
                a.node,
                a.at_ms,
                a.tick,
                if a.reply {
                    "a forward's reply"
                } else {
                    "its own client's"
                },
            ));
        }
    }
    f.i5.sort();
    f.i5_operator.sort();
    f.i5_after_end.sort();
    // Plan 46 M1.2 step 3: `acked_at_or_below_cut`.
    let cut = super::cut::judge(&acks, &cuts);
    f.cut = cut.past_cut;
    f.cut_judged = cut.judged;
    f.cut_abandoned = cut.abandoned;
    f.cut_refusals = cut.refusals;
    let sticky = super::sticky::StickyEnds::of_log(log).judge(&o.sticky_released.lock().unwrap());
    f.sticky_granted = sticky.granted;
    f.sticky_released = sticky.ended_released;
    f.sticky_abandoned = sticky.ended_abandoned;
    f.sticky_claimed = sticky.ended_claimed;
    f.sticky = sticky.violations;
    f
}

/// The rids of replay-queue entries `queue` (`(rid, queue_seq)`) that were
/// not queued when the queue was last read (`seen`, which becomes `queue`):
/// the rollbacks I5 dates now. Keyed by the entry, not the rid: a rid whose
/// replay left the queue and that was queued again (a new entry) between two
/// reads is rolled back again.
pub fn newly_queued(seen: &mut BTreeSet<(Rid, u64)>, queue: &[(Rid, u64)]) -> Vec<Rid> {
    let new: Vec<Rid> = queue
        .iter()
        .filter(|e| !seen.contains(e))
        .map(|(rid, _)| *rid)
        .collect();
    *seen = queue.iter().copied().collect();
    new
}

/// Why the log does not carry `t` as it was committed (`None`: it does).
///
/// Plan 43 C1-4 (D9): a transaction a takeover re-ships carries its identity
/// as its first record (`TailOf { epoch, jseq }`: the journal that first held
/// it, kept through any number of re-ships). The transaction `t` is judged by
/// that identity — its own `TailOf` if it is a re-shipped one, else its place
/// in its epoch's journal — against the log's completing transaction's, and
/// its records must be the log's, ignoring the `TailOf` records of either.
fn not_verbatim(
    log: &super::check::Oracle,
    rid: Rid,
    t: &TxRef,
    converged: bool,
) -> Option<String> {
    use constellation_meta::LogRecord;
    let Some((seq, i)) = log.completed_at.get(&rid).copied() else {
        return converged.then(|| "absent: the log never completed it".to_string());
    };
    let seg = log.unfenced.get(&seq)?;
    let plain = |r: &&LogRecord| !matches!(r, LogRecord::TailOf { .. });
    let want: Vec<&LogRecord> = t.records.iter().filter(plain).collect();
    // The run of the log's records that is the completing transaction:
    // ends at the completing record, carries `want` (ignoring `TailOf`), and
    // may begin with the `TailOf` that names it.
    let mut run: Option<(usize, usize)> = None;
    if !want.is_empty() {
        // (The completion is not always the transaction's last record: an
        // inbox position's acknowledgement rides after it.)
        'search: for st in (0..=i).rev() {
            if !plain(&&seg.records[st]) {
                continue;
            }
            for end in i..seg.records.len() {
                let got: Vec<&LogRecord> = seg.records[st..=end].iter().filter(plain).collect();
                if got.len() > want.len() {
                    break;
                }
                if got == want {
                    run = Some((st, end));
                    break 'search;
                }
            }
            if seg.records[st..=i].iter().filter(plain).count() > want.len() {
                break;
            }
        }
    }
    let log_ident = run.and_then(|(st, _)| match st.checked_sub(1).map(|p| &seg.records[p]) {
        Some(LogRecord::TailOf { epoch, jseq }) => Some((*epoch, *jseq)),
        _ => None,
    });
    let t_ident = constellation_meta::tail_identity(&t.records).unwrap_or((t.epoch, t.first));
    let identity = if t.origin.0 == 0 {
        match log_ident {
            Some(id) => id == t_ident,
            None => {
                seg.epoch == t.epoch
                    && seg.node == t.node
                    && seg.rows.contains(&t.first)
                    && t_ident == (t.epoch, t.first)
            }
        }
    } else {
        seg.origins.contains(&t.origin)
    };
    if !identity {
        return Some(format!(
            "its completion is in segment {seq} (epoch {}, node {}, rows {:?}, origins {:?}, \
             identity {log_ident:?}): not under the identity it was committed with {t_ident:?}",
            seg.epoch, seg.node, seg.rows, seg.origins
        ));
    }
    if !want.is_empty() && run.is_none() {
        let from = i.saturating_sub(want.len() + 1);
        return Some(format!(
            "its completion is in segment {seq} at record {i}, which does not carry the \
             {} record(s) it was committed with as one run (committed with {:?}; the log's \
             records {from}..={i}: {:?})",
            want.len(),
            t.records,
            &seg.records[from..=i]
        ));
    }
    None
}

// ---------------------------------------------------------------------
// I6 (plan 43 C7b): no TTL claim of a live lease.
// ---------------------------------------------------------------------

/// A claimant's `takeover_permit`: the epoch and holder it may take over.
type Permit = Option<(u64, NodeId)>;

/// What one node's latest incarnation lets the lease-claim oracle see.
struct ClaimNode {
    clock: Clock,
    shared: Arc<Shared>,
    /// The node's `expiry_margin_ms`: its holder honours its lease only
    /// until `holder_expires_ms - margin`.
    margin_ms: u64,
    /// The node's `backup_takeover_ms`: a seal of a holder that heard from
    /// the sealing backup within it is a seal of a live holder.
    window_ms: u64,
}

/// The direct two-lease-holders oracle (plan 43 C7b, commit 1; invariant
/// I6): at every lease CAS the bucket accepts that replaces another node's
/// unreleased, unfenced lease **without a permit** (a seal or an `ack=s3`
/// takeover, which fence the old holder by another route), the
/// predecessor's own honour end for that body is looked up on the
/// predecessor's honour clock (`Lease::holder_expires_ms` on the clock
/// `holder_clock` names, less its margin: the window in which its holder
/// admits work under the body) and compared with the predecessor's honour
/// reading *now* (`ProtocolClock::peek_honour`, which moves no clock). A
/// claim before that end is a violation: two nodes may act as the holder.
///
/// It reads true state only the harness has (the predecessor's clock), and
/// judges nothing the core claims. A predecessor that is dead, or that has
/// restarted since (another `holder_clock`), honours nothing: not judged.
///
/// Counted always, in every configuration; a count fails the run only under
/// `SimConfig::strict_lease_claims`.
#[derive(Default)]
pub struct LeaseClaims {
    nodes: Mutex<BTreeMap<NodeId, ClaimNode>>,
    bus: Mutex<Option<Arc<Bus>>>,
    /// `(claimant, epoch it writes)` -> the claimant's `takeover_permit`
    /// when it sent the CAS (the epoch and holder it holds a permit for).
    intents: Mutex<HashMap<(NodeId, u64), Permit>>,
    /// Claims of another node's unreleased, unfenced lease with no permit.
    pub ttl_claims: AtomicU64,
    /// Of those, the ones that landed inside the predecessor's honour
    /// window: the violations.
    pub live_claims: AtomicU64,
    /// Permitted claims of a `Backup`-policy lease (a seal's takeover)
    /// whose holder still honoured its lease and had heard from the sealing
    /// backup within the takeover window: count only, not a violation (the
    /// seal fences the holder by the segment epoch).
    pub seals_of_a_live_holder: AtomicU64,
    /// All permitted claims of an unreleased foreign lease.
    pub permitted_claims: AtomicU64,
    pub violations: Mutex<Vec<String>>,
    /// The highest lease epoch any CAS has written so far: a node whose
    /// core holds this epoch is the current root, any other one is deposed
    /// (or never was).
    pub max_epoch: AtomicU64,
}

impl LeaseClaims {
    /// `node`'s current incarnation (its clock, liveness and margin).
    pub fn register(
        &self,
        node: NodeId,
        clock: Clock,
        shared: Arc<Shared>,
        margin_ms: u64,
        window_ms: u64,
    ) {
        self.nodes.lock().unwrap().insert(
            node,
            ClaimNode {
                clock,
                shared,
                margin_ms,
                window_ms,
            },
        );
    }

    /// The run's bus (what the holder heard from whom, and when).
    pub fn set_bus(&self, bus: Arc<Bus>) {
        *self.bus.lock().unwrap() = Some(bus);
    }

    /// `node` is about to send a lease CAS writing `epoch`, holding this
    /// takeover permit.
    pub fn note_attempt(&self, node: NodeId, epoch: u64, permit: Option<(u64, NodeId)>) {
        self.intents.lock().unwrap().insert((node, epoch), permit);
    }

    /// A lease CAS landed in the bucket (called with the body it replaced).
    pub fn landed(&self, node: NodeId, prev: Option<Lease>, new: Lease) {
        self.max_epoch.fetch_max(new.epoch, Ordering::SeqCst);
        let Some(prev) = prev else { return };
        // A claim: another node, the next epoch, on a lease that was held
        // and neither released nor fenced.
        if new.holder != node
            || prev.holder == 0
            || prev.holder == node
            || prev.released
            || new.epoch != prev.epoch + 1
            || prev.retired.contains(&prev.holder)
        {
            return;
        }
        let permit = self
            .intents
            .lock()
            .unwrap()
            .get(&(node, new.epoch))
            .copied()
            .flatten();
        let permitted = permit == Some((prev.epoch, prev.holder))
            && match prev.ack_policy {
                AckPolicy::Backup => prev.backups.contains(&node),
                AckPolicy::S3 => true,
                AckPolicy::Local => false,
            };
        let nodes = self.nodes.lock().unwrap();
        // How much longer the predecessor's own process honours the body,
        // if it does at all.
        let honoured = nodes.get(&prev.holder).and_then(|n| {
            if !n.shared.alive.load(Ordering::SeqCst)
                || prev.holder_clock == 0
                || prev.holder_clock != n.clock.id()
                || prev.holder_expires_ms == 0
            {
                return None;
            }
            let end = prev.holder_expires_ms - n.margin_ms as i64;
            let left = end - n.clock.peek_honour().0;
            (left > 0).then_some((left, n.clock.elapsed_ms()))
        });
        if permitted {
            self.permitted_claims.fetch_add(1, Ordering::Relaxed);
            // A seal of a live holder: its own process still honours the
            // lease and has heard from the sealing backup within the
            // takeover window (a holder cut off from its backup is what a
            // seal is for).
            let heard = nodes.get(&prev.holder).is_some_and(|h| {
                self.bus
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|b| b.since_delivery(node, prev.holder))
                    .is_some_and(|d| d.as_millis() as u64 <= h.window_ms)
            });
            if prev.ack_policy == AckPolicy::Backup && honoured.is_some() && heard {
                self.seals_of_a_live_holder.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        self.ttl_claims.fetch_add(1, Ordering::Relaxed);
        if let Some((left, at_ms)) = honoured {
            self.live_claims.fetch_add(1, Ordering::Relaxed);
            self.violations.lock().unwrap().push(format!(
                "TTL claim of a live lease: node {node} took epoch {} of node {} at true {at_ms} ms, \
                 which honours it for another {left} ms",
                new.epoch, prev.holder
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::types::ROOT_INO;
    use constellation_meta::{DelegateTx, MutateOp, Position};
    use constellation_store_s3::{LeaseMode, LeaseStore};
    use constellation_types::clock::Now;

    fn rid(n: u64, seq: u64) -> Rid {
        Rid {
            node: n,
            incarnation: 1,
            seq,
        }
    }

    /// A refusal for lacking deps is a violation at the root that holds the
    /// highest lease epoch written, and none at a deposed one (seed 4947).
    #[test]
    fn a_deps_refusal_is_judged_only_at_the_current_root() {
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        o.leases.max_epoch.store(5, Ordering::SeqCst);
        // Deposed (epoch 4 of 5), no lease, no refusal: not judged.
        o.note_deps_refused(1, Some(4), 2, 100);
        o.note_deps_refused(2, None, 2, 100);
        o.note_deps_refused(3, Some(5), 0, 100);
        assert!(o.causal_cut.lock().unwrap().is_empty());
        // The current root: judged.
        o.note_deps_refused(3, Some(5), 1, 200);
        assert_eq!(o.causal_cut.lock().unwrap().len(), 1);
    }

    fn seg(epoch: u64, rids: &[Rid]) -> Segment {
        Segment {
            node: 1,
            epoch,
            records: rids
                .iter()
                .map(|r| LogRecord::Completed { rid: *r })
                .collect(),
            through: 0,
            rows: Vec::new(),
            origins: Vec::new(),
        }
    }

    fn refused_seg(epoch: u64, rid: Rid, code: Code) -> Segment {
        Segment {
            records: vec![LogRecord::Refused { rid, code }],
            ..seg(epoch, &[])
        }
    }

    /// A small world: stores for nodes 1..=3 (all with a 50 ms budget and
    /// no stability window), an empty bus and bucket.
    struct World {
        o: Arc<Oracles>,
        bus: Arc<Bus>,
        metas: BTreeMap<NodeId, Arc<Meta>>,
    }

    fn world() -> World {
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        let bus = Bus::new(1, (1, 15), 0.0);
        let mut metas = BTreeMap::new();
        for id in 1..=3u64 {
            let m = Meta::open_in_memory().expect("meta");
            m.set_node_prefix(id).expect("prefix");
            let m = Arc::new(m);
            o.register(id, m.clone(), 50, 0);
            metas.insert(id, m);
        }
        World { o, bus, metas }
    }

    impl World {
        fn lease(&self, holder: u64, epoch: u64, backups: &[u64]) {
            let l = Lease::granted("p0", holder, epoch, 60_000)
                .reconfigured(backups.to_vec(), AckPolicy::Backup);
            let leases = LeaseStore::new(self.o.bucket.raw(), "p0", LeaseMode::Cas);
            futures::executor::block_on(leases.try_create(&l)).expect("lease");
        }

        /// `node` executes a create of `name` under `rid` into its journal.
        fn create(&self, node: u64, rid: Rid, name: &str) -> BackupTx {
            let m = &self.metas[&node];
            let ino = m.allocate_ino(ROOT_INO).expect("ino");
            let op = MutateOp::Create {
                parent: ROOT_INO,
                name: name.into(),
                ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
            };
            constellation_meta::execute_mutate(m, &op, Some(rid)).expect("execute");
            Replica::journal_txs_from(&**m, 0, usize::MAX)
                .into_iter()
                .find(|t| carries_as(&t.records, rid, None))
                .expect("journaled")
        }

        fn judge(&self, answerer: u64, st: &AnswererState, rid: Rid) -> Judgement {
            let outcome = MutateOutcome::Accepted {
                epoch: 1,
                records: Vec::new(),
            };
            self.judge_as(answerer, st, rid, &outcome)
        }

        fn judge_as(
            &self,
            answerer: u64,
            st: &AnswererState,
            rid: Rid,
            outcome: &MutateOutcome,
        ) -> Judgement {
            committed_at_answer(&self.o, &self.bus, answerer, st, rid, outcome)
        }
    }

    fn table(rows: &[(u64, u64, u64)]) -> constellation_meta::delegation::DelegationTable {
        use constellation_meta::delegation::DelegationRecord;
        let mut t = constellation_meta::delegation::DelegationTable::default();
        for (dir, node, gen) in rows {
            t.apply(DelegationRecord::DelegateNode {
                node: *node,
                gen: *gen,
                dirs: vec![constellation_meta::SubtreeGrant {
                    dir: *dir,
                    kind: constellation_meta::SubtreeKind::Placed,
                }],
            });
        }
        t
    }

    #[test]
    fn the_owner_timeline_follows_the_holders_table_history() {
        use super::super::reach::Owner;
        let w = world();
        // Node 1 holds the lease from 0; node 2 held another's, in the past.
        w.o.reach.lease_landed_at(0, 1, 1_000_000, false);
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "a".into(),
            ino: 4_000,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let r1 = rid(1, 1);
        w.o.note_op(r1, &op);
        // The root directory is delegated to node 2 (generation 7) at 1 000,
        // ended at 3 000 (an empty table), delegated to node 3 (generation 8)
        // at 5 000; a table the holder's, from 100, with no delegation, is
        // none.
        w.o.note_table_at(1_000, 1, table(&[(ROOT_INO, 2, 7)]));
        w.o.note_table_at(3_000, 1, table(&[]));
        w.o.note_table_at(5_000, 1, table(&[(ROOT_INO, 3, 8)]));
        let (owners, born) = w.o.owner_timeline(1, r1, 500, 6_000);
        assert_eq!(
            owners,
            vec![
                (500, Owner::Root),
                (1_000, Owner::Delegate { node: 2, gen: 7 }),
                (3_000, Owner::Root),
                (5_000, Owner::Delegate { node: 3, gen: 8 }),
            ]
        );
        // The generations' births are the first table that lists them, also
        // the ones before the window.
        assert_eq!(born, BTreeMap::from([(7, 1_000), (8, 5_000)]));
        // A window that begins inside a generation starts with its owner.
        let (owners, _) = w.o.owner_timeline(1, r1, 2_000, 4_000);
        assert_eq!(
            owners,
            vec![
                (2_000, Owner::Delegate { node: 2, gen: 7 }),
                (3_000, Owner::Root),
            ]
        );
        // An op unknown to the oracle is the root's.
        let (owners, born) = w.o.owner_timeline(1, rid(1, 9), 500, 6_000);
        assert_eq!(owners, vec![(0, Owner::Root)]);
        assert!(born.is_empty());
        // Keys under two generations are blocked on both.
        let w = world();
        w.o.reach.lease_landed_at(0, 1, 1_000_000, false);
        let rename = MutateOp::Rename {
            parent: 100,
            name: "a".into(),
            new_parent: 200,
            new_name: "b".into(),
            noreplace: false,
        };
        let r2 = rid(1, 2);
        w.o.note_op(r2, &rename);
        w.o.note_table_at(1_000, 1, table(&[(100, 2, 7), (200, 3, 8)]));
        let (owners, _) = w.o.owner_timeline(1, r2, 1_500, 2_000);
        assert_eq!(owners.len(), 1);
        let Owner::Blocked(mut involved) = owners[0].1.clone() else {
            panic!("{owners:?}")
        };
        involved.sort_unstable();
        assert_eq!(involved, vec![(2, 7), (3, 8)]);
        // A table of a node that is not the holder is used only when the
        // holder has none: node 2's table is ignored while node 1's counts.
        w.o.note_table_at(1_800, 2, table(&[]));
        let (owners, _) = w.o.owner_timeline(1, r2, 1_500, 2_000);
        assert!(matches!(owners[0].1, Owner::Blocked(_)), "{owners:?}");
    }

    fn holder_state(epoch: u64) -> AnswererState {
        AnswererState {
            epoch: Some(epoch),
            ..Default::default()
        }
    }

    #[test]
    fn i1_an_ok_answer_with_a_refusal_in_the_log_is_a_violation() {
        let w = world();
        let r = rid(3, 1);
        w.lease(1, 1, &[]);
        // The archive carries the rid *refused*; the answer said OK. The
        // C0 oracle took "some outcome of the rid is archived" for a
        // commit of what was answered.
        w.o.bucket
            .archive
            .note_segment(1, refused_seg(1, r, Code::Exists));
        let st = holder_state(1);
        let j = w.judge(1, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        let m = j.mismatch.expect("the archive carries another outcome");
        assert!(
            m.contains("the archive (segment 1) carries a refusal (EEXIST)"),
            "{m}"
        );
        // The same answer kind as the log's: committed, no mismatch.
        let j = w.judge_as(1, &st, r, &MutateOutcome::Errno(Code::Exists));
        assert_eq!(j.class, Some(Judged::Archive), "{}", j.detail);
        assert_eq!(j.mismatch, None);
        // A refusal of another code is another outcome too.
        let j = w.judge_as(1, &st, r, &MutateOutcome::Errno(Code::NotFound));
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.mismatch.is_some());
        // An `Exists` hint and a conflict are refusals of EEXIST / ESTALE.
        let j = w.judge_as(
            1,
            &st,
            r,
            &MutateOutcome::Exists {
                records: Vec::new(),
                epoch: 1,
            },
        );
        assert_eq!(j.class, Some(Judged::Archive), "{}", j.detail);
        let j = w.judge_as(1, &st, r, &MutateOutcome::Conflict { manifest: None });
        assert!(j.mismatch.is_some(), "{}", j.detail);
    }

    #[test]
    fn i1_a_refusal_answer_with_a_completion_in_the_journal_is_a_violation() {
        let w = world();
        let r = rid(1, 1);
        // The answerer's journal completed the rid (it executed it).
        w.create(1, r, "a");
        w.lease(1, 1, &[]);
        let st = holder_state(1);
        let j = w.judge(1, &st, r);
        assert_eq!(j.class, Some(Judged::Singleton), "{}", j.detail);
        assert_eq!(j.mismatch, None);
        // Answered refused: no transaction carries a refusal, a completion
        // does. (A lone node's answer to its own client needs no
        // transaction, so a class still holds: the mismatch is judged
        // whether or not one does.)
        let refused = MutateOutcome::Errno(Code::Exists);
        let j = w.judge_as(1, &st, r, &refused);
        assert!(j.tx.is_none(), "no transaction carries the refusal");
        assert_eq!(
            j.mismatch.as_deref(),
            Some("the answerer's journal carries a completion")
        );
        // The lease holder's journal is read too (a delegate's answer, the
        // root's row).
        let w2 = world();
        let r2 = rid(3, 1);
        w2.create(1, r2, "b");
        w2.lease(1, 1, &[]);
        let j = w2.judge_as(2, &holder_state(1), r2, &refused);
        assert_eq!(
            j.mismatch.as_deref(),
            Some("the lease holder 1's journal carries a completion"),
            "{}",
            j.detail
        );
    }

    /// The `Findings` of `acks` over a log that completes `completed`.
    fn findings(acks: Vec<AckEntry>, completed: &[(Rid, (u64, usize))]) -> Findings {
        findings_rolled_back(acks, completed, &HashMap::new())
    }

    /// The same, with `rolled_back` (each rid's latest rollback's tick).
    fn findings_rolled_back(
        acks: Vec<AckEntry>,
        completed: &[(Rid, (u64, usize))],
        rolled_back: &HashMap<Rid, u64>,
    ) -> Findings {
        findings_dropped(acks, completed, rolled_back, &BTreeSet::new())
    }

    /// The same, with the rids `repair drop-held` rolled back.
    fn findings_dropped(
        acks: Vec<AckEntry>,
        completed: &[(Rid, (u64, usize))],
        rolled_back: &HashMap<Rid, u64>,
        dropped: &BTreeSet<Rid>,
    ) -> Findings {
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        *o.acks.lock().unwrap() = acks;
        end_of_run(
            &o,
            &log_with(seg(1, &[]), completed),
            &[],
            rolled_back,
            dropped,
            &BTreeSet::new(),
            &BTreeMap::new(),
            true,
        )
    }

    /// ADR-47, check 1: findings of an audit over a log that completes `r`.
    fn fence_of(audit: FenceAudit, acks: Vec<AckEntry>, r: Rid) -> Vec<String> {
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        *o.acks.lock().unwrap() = acks;
        *o.fence_audit.lock().unwrap() = audit;
        end_of_run(
            &o,
            &log_with(seg(1, &[]), &[(r, (1, 0))]),
            &[],
            &HashMap::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeMap::new(),
            true,
        )
        .fence
    }

    /// A rid the designee ran before the root's refusal reached the client
    /// is I1; one the client submitted again after hearing the refusal, or
    /// whose refusal the requester held back, is not.
    #[test]
    fn check_1_judges_a_refusal_of_an_op_a_designee_ran() {
        let r = rid(3, 1);
        let refused = |tick| ack(3, r, tick, OutcomeKind::Refused(Code::ReadOnly), false);
        let audit = |exec_tick: u64, submits: &[u64]| FenceAudit {
            refusals: vec![(r, Code::ReadOnly, false, 5, 2)],
            execs: vec![(r, 7, exec_tick, 1)],
            submits: submits.iter().map(|t| (r, *t)).collect(),
        };
        // The designee ran it first.
        let f = fence_of(audit(3, &[1]), vec![refused(6)], r);
        assert_eq!(f.len(), 1, "{f:?}");
        assert!(f[0].contains("nonholder_refused_a_designee_ran"), "{f:?}");
        // It ran after the refusal with no new submission: still I1.
        assert_eq!(fence_of(audit(9, &[1]), vec![refused(6)], r).len(), 1);
        // The client submitted again after hearing the refusal.
        assert!(fence_of(audit(9, &[1, 8]), vec![refused(6)], r).is_empty());
        // The requester held the root's refusal back: no client heard it.
        let reply = ack(2, r, 6, OutcomeKind::Refused(Code::ReadOnly), true);
        assert!(fence_of(audit(3, &[1]), vec![reply], r).is_empty());
    }

    /// A rollback is dated by the queue entry: a rid queued, answered, gone
    /// from the queue unread (no rollback counter moved) and queued again as
    /// a new entry is a new rollback. (Keyed by the rid, as before, the third
    /// read found nothing new: the second rollback kept the first one's tick,
    /// and the answer between them was never judged.)
    #[test]
    fn i5_dates_a_second_rollback_of_a_rid_by_its_new_entry() {
        let r = rid(1, 1);
        let mut seen = BTreeSet::new();
        assert_eq!(newly_queued(&mut seen, &[(r, 3)]), vec![r]);
        assert!(
            newly_queued(&mut seen, &[(r, 3)]).is_empty(),
            "the same entry"
        );
        assert_eq!(newly_queued(&mut seen, &[(r, 9)]), vec![r], "a new entry");
        assert!(newly_queued(&mut seen, &[]).is_empty());
    }

    /// I5 judges the answers given before the rollback (placement-hot seed
    /// 68): a lone root answered its own client, its deposition rolled the
    /// row back and queued it, and the replay was answered by the next
    /// root's group. That answer came after the rollback; nothing it
    /// acknowledged was rolled back. A group answer before the rollback is
    /// still a violation. (The parent's rule judged every OK answer of a
    /// rolled-back rid: the first case was a violation.)
    #[test]
    fn i5_judges_only_the_answers_before_the_rollback() {
        let group = |a: AckEntry| AckEntry {
            judged: Some(Judgement {
                class: Some(Judged::Group),
                expected: Expected::Class(Judged::Group),
                tx: None,
                detail: String::new(),
                mismatch: None,
            }),
            ..a
        };
        let r = rid(1, 1);
        let acks = vec![
            ack(1, r, 1, OutcomeKind::Ok, false),
            group(ack(2, r, 5, OutcomeKind::Ok, true)),
        ];
        let f = findings_rolled_back(acks.clone(), &[], &HashMap::from([(r, 3)]));
        assert!(f.i5.is_empty(), "{:?}", f.i5);
        // Rolled back again after the group's answer: a violation.
        let f = findings_rolled_back(acks.clone(), &[], &HashMap::from([(r, 6)]));
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5[0].contains("by node 2"), "{:?}", f.i5);
        // A rid whose rollback the queue never dated is judged against
        // every answer.
        let f = findings_rolled_back(acks, &[], &HashMap::from([(r, u64::MAX)]));
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
    }

    /// Plan 46 M1.3 step 13: a rolled-back row of an abandoned stream past
    /// its cut is the operator's I5 (counted), not a violation; the same row
    /// at or below the cut, or of a stream nobody abandoned, still is one.
    #[test]
    fn i5_counts_a_rollback_past_an_abandoned_streams_cut_as_the_operators() {
        let r = rid(1, 1);
        let row_of = |idx: u64, class: Judged| AckEntry {
            judged: Some(Judgement {
                class: Some(class),
                expected: Expected::Class(class),
                tx: Some(TxRef {
                    node: 2,
                    first: idx,
                    last: idx,
                    origin: (7, idx),
                    epoch: 1,
                    records: Vec::new(),
                }),
                detail: String::new(),
                mismatch: None,
            }),
            ..ack(2, r, 5, OutcomeKind::Ok, true)
        };
        let row = |idx: u64| row_of(idx, Judged::Group);
        let o = |records: Vec<LogRecord>| {
            let mut s = seg(1, &[]);
            s.records = records;
            let oracles = Oracles::new(Bucket::new(1, (0, 0)));
            (oracles, s)
        };
        let ended = LogRecord::DelegateNodeEnded {
            node: 2,
            gen: 7,
            through: 3,
        };
        let abandoned = LogRecord::GroupAbandoned {
            epoch: 0,
            gen: 7,
            through_known: 3,
        };
        let run = |records: Vec<LogRecord>, idx: u64, at: BTreeMap<u64, u64>| {
            let (oracles, s) = o(records);
            *oracles.acks.lock().unwrap() = vec![row(idx)];
            end_of_run(
                &oracles,
                &log_with(s, &[]),
                &[],
                &HashMap::from([(r, 6)]),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &at,
                true,
            )
        };
        // Past the cut of an abandoned stream: the operator's.
        let f = run(vec![ended.clone(), abandoned.clone()], 4, BTreeMap::new());
        assert!(f.i5.is_empty(), "{:?}", f.i5);
        assert_eq!(f.i5_operator.len(), 1, "{:?}", f.i5_operator);
        // At the cut: a violation.
        let f = run(vec![ended.clone(), abandoned.clone()], 3, BTreeMap::new());
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5_operator.is_empty());
        // The stream ended but nobody abandoned it: a violation.
        let f = run(vec![ended.clone()], 4, BTreeMap::new());
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5_operator.is_empty());
        // A row answered after the abandon was answered (the row is acked at
        // t=5): not the cost the operator accepted, counted apart. One
        // answered at the same instant or before is the operator's.
        let refusal = LogRecord::Refused {
            rid: r,
            code: constellation_authority::ABANDONED_CODE,
        };
        let f = run(
            vec![ended.clone(), abandoned.clone(), refusal],
            4,
            BTreeMap::from([(7, 4)]),
        );
        assert!(f.i5.is_empty() && f.i5_operator.is_empty(), "{:?}", f.i5);
        assert_eq!(f.i5_after_end.len(), 1, "{:?}", f.i5_after_end);
        // A returned holder's row the log completed by its rid (class
        // Final) was replayed: counted nowhere.
        let (oracles, s) = o(vec![ended.clone(), abandoned.clone()]);
        *oracles.acks.lock().unwrap() = vec![row_of(4, Judged::Final)];
        let f = end_of_run(
            &oracles,
            &log_with(s, &[(r, (1, 0))]),
            &[],
            &HashMap::from([(r, 6)]),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeMap::from([(7, 4)]),
            true,
        );
        assert!(
            f.i5.is_empty() && f.i5_after_end.is_empty() && f.i5_operator.is_empty(),
            "{:?}",
            f.i5
        );
        // No record of the rid at all: the row vanished, a violation.
        let f = run(
            vec![ended.clone(), abandoned.clone()],
            4,
            BTreeMap::from([(7, 4)]),
        );
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5_after_end.is_empty() && f.i5_operator.is_empty());
        let f = run(vec![ended, abandoned], 4, BTreeMap::from([(7, 5)]));
        assert_eq!(f.i5_operator.len(), 1);
        assert!(f.i5_after_end.is_empty());
    }

    /// Plan 46 M0.2 (R7): a non-stealable sequencer's OK (an epoch hold's)
    /// that is rolled back is a violation only if the log never completes
    /// the rid: its owner's replay landing is what retires the requester's
    /// shadow. (A group's OK rolled back stays a violation either way.)
    #[test]
    fn i5_excuses_a_non_stealable_ok_whose_rid_the_log_completes() {
        let judged = |class| {
            Some(Judgement {
                class: Some(class),
                expected: Expected::Class(class),
                tx: None,
                detail: String::new(),
                mismatch: None,
            })
        };
        let r = rid(1, 1);
        let acks = |class| {
            vec![AckEntry {
                judged: judged(class),
                ..ack(2, r, 5, OutcomeKind::Ok, true)
            }]
        };
        let rolled = HashMap::from([(r, 6)]);
        // Never completed in the log (its replay refused, or nobody's): a
        // violation.
        let f = findings_rolled_back(acks(Judged::Final), &[], &rolled);
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        // Completed by the owner's replay: not.
        let f = findings_rolled_back(acks(Judged::Final), &[(r, (3, 0))], &rolled);
        assert!(f.i5.is_empty(), "{:?}", f.i5);
        // A group's OK is a violation whatever the log says.
        let f = findings_rolled_back(acks(Judged::Group), &[(r, (3, 0))], &rolled);
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
    }

    /// Plan 46 M0.2 review: a rid `repair drop-held` rolled back is the
    /// operator's action, excused from I5 only if its effect survives (the
    /// log completes the rid: a completion, or the refusal that carries
    /// its conflict copy). The same rid not dropped, or dropped and never
    /// completed, is a rolled-back acknowledgement as before.
    #[test]
    fn i5_excuses_a_dropped_rid_only_if_its_effect_survives() {
        let judged = Some(Judgement {
            class: Some(Judged::Group),
            expected: Expected::Class(Judged::Group),
            tx: None,
            detail: String::new(),
            mismatch: None,
        });
        let (r, other) = (rid(1, 1), rid(1, 2));
        let acks = vec![
            AckEntry {
                judged: judged.clone(),
                ..ack(2, r, 5, OutcomeKind::Ok, true)
            },
            AckEntry {
                judged,
                ..ack(2, other, 5, OutcomeKind::Ok, true)
            },
        ];
        let rolled = HashMap::from([(r, 6), (other, 6)]);
        let dropped = BTreeSet::from([r]);
        let log = [(r, (3, 0)), (other, (3, 1))];
        // The dropped rid, completed in the log: excused; the other, not
        // dropped: still a violation.
        let f = findings_dropped(acks.clone(), &log, &rolled, &dropped);
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5[0].contains(&format!("{other:?}")), "{:?}", f.i5);
        // Dropped but never completed: its acknowledged effect is lost.
        let f = findings_dropped(acks.clone(), &[(other, (3, 1))], &rolled, &dropped);
        assert_eq!(f.i5.len(), 2, "{:?}", f.i5);
        // Dropped and refused by the drop (the conflict copy keeps the
        // write): survives.
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        *o.acks.lock().unwrap() = acks;
        let log = log_with(refused_seg(1, r, Code::Io), &[(other, (3, 1))]);
        let f = end_of_run(
            &o,
            &log,
            &[],
            &rolled,
            &dropped,
            &BTreeSet::new(),
            &BTreeMap::new(),
            true,
        );
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        // Its replay refused: the outcome is the log's.
        let log = log_with(refused_seg(1, r, Code::NotFound), &[(other, (3, 1))]);
        let f = end_of_run(
            &o,
            &log,
            &[],
            &rolled,
            &dropped,
            &BTreeSet::new(),
            &BTreeMap::new(),
            true,
        );
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        // A refusal of a rid the drop did not roll back excuses nothing.
        let log = log_with(refused_seg(1, other, Code::NotFound), &[(r, (3, 0))]);
        let f = end_of_run(
            &o,
            &log,
            &[],
            &rolled,
            &dropped,
            &BTreeSet::new(),
            &BTreeMap::new(),
            true,
        );
        assert_eq!(f.i5.len(), 1, "{:?}", f.i5);
        assert!(f.i5[0].contains(&format!("{other:?}")), "{:?}", f.i5);
    }

    fn ack(node: u64, r: Rid, tick: u64, kind: OutcomeKind, reply: bool) -> AckEntry {
        AckEntry {
            node,
            rid: r,
            tick,
            at_ms: tick,
            kind,
            reply,
            relay_from: None,
            claimed: None,
            judged: Some(Judgement {
                class: Some(Judged::Singleton),
                expected: Expected::Class(Judged::Singleton),
                tx: None,
                detail: String::new(),
                mismatch: None,
            }),
        }
    }

    fn relay(node: u64, from: u64, r: Rid, tick: u64, kind: OutcomeKind) -> AckEntry {
        AckEntry {
            relay_from: Some(from),
            judged: None,
            ..ack(node, r, tick, kind, false)
        }
    }

    /// The strict durability check excuses the rollback of an answer the
    /// oracle judged `Lone`, not of one the core claimed `Lone`: a core
    /// that claims `Lone` for a committed answer would hide its loss.
    /// Fails on the parent (the claim decided it).
    #[test]
    fn only_a_rid_judged_lone_is_excused() {
        let judged = |class: Option<Judged>| Judgement {
            class,
            expected: Expected::Class(Judged::Group),
            tx: None,
            detail: String::new(),
            mismatch: None,
        };
        let (a, b, c, d) = (rid(1, 1), rid(1, 2), rid(1, 3), rid(1, 4));
        let (e, f, g) = (rid(1, 5), rid(1, 6), rid(1, 7));
        let acks = vec![
            // Claimed and judged `Lone`: excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                ..ack(1, a, 1, OutcomeKind::Ok, false)
            },
            // Claimed `Lone`, judged a group's: not excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                judged: Some(judged(Some(Judged::Group))),
                ..ack(1, b, 2, OutcomeKind::Ok, false)
            },
            // Claimed `Lone`, no class held: not excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                judged: Some(judged(None)),
                ..ack(1, c, 3, OutcomeKind::Ok, false)
            },
            // Judged `Lone` once, a group's at a second answer: not excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                ..ack(1, d, 4, OutcomeKind::Ok, false)
            },
            AckEntry {
                claimed: Some(RowCommit::LONE),
                judged: Some(judged(Some(Judged::Group))),
                ..ack(1, d, 5, OutcomeKind::Ok, false)
            },
            // Judged `Lone` at its client's answer; the replay after the
            // rollback refused by the next root, a committed refusal its
            // client never heard (long-backup seed 1063): excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                ..ack(1, e, 6, OutcomeKind::Ok, false)
            },
            AckEntry {
                judged: Some(judged(Some(Judged::Group))),
                ..ack(3, e, 7, OutcomeKind::Refused(Code::NotFound), true)
            },
            // The same, the second answer of the outcome its client heard:
            // not excused.
            AckEntry {
                claimed: Some(RowCommit::LONE),
                ..ack(1, f, 8, OutcomeKind::Ok, false)
            },
            AckEntry {
                judged: Some(judged(Some(Judged::Group))),
                ..ack(3, f, 9, OutcomeKind::Ok, true)
            },
            // A relayed answer: judged at the sequencer's reply of the
            // outcome relayed, not at another reply of the rid.
            AckEntry {
                judged: Some(judged(Some(Judged::Group))),
                ..ack(3, g, 10, OutcomeKind::Ok, true)
            },
            relay(1, 3, g, 11, OutcomeKind::Ok),
            ack(3, g, 12, OutcomeKind::Refused(Code::Exists), true),
        ];
        assert_eq!(lone_answered(&acks), HashSet::from([a, e]));
    }

    #[test]
    fn i1_a_relay_without_a_judged_answer_is_a_violation() {
        let r = rid(3, 1);
        // Node 3 passes on node 2's OK, which node 2 emitted (judged) first.
        let f = findings(
            vec![
                ack(2, r, 1, OutcomeKind::Ok, true),
                relay(3, 2, r, 2, OutcomeKind::Ok),
            ],
            &[],
        );
        assert_eq!(f.unverified_relays, 0, "{:?}", f.i1);
        // The sequencer's answer is of another kind, is not a reply, is
        // later, or is another node's: the client heard an answer nobody
        // gave.
        for acks in [
            vec![
                ack(2, r, 1, OutcomeKind::Refused(Code::Exists), true),
                relay(3, 2, r, 2, OutcomeKind::Ok),
            ],
            vec![
                ack(2, r, 1, OutcomeKind::Ok, false),
                relay(3, 2, r, 2, OutcomeKind::Ok),
            ],
            vec![
                relay(3, 2, r, 1, OutcomeKind::Ok),
                ack(2, r, 2, OutcomeKind::Ok, true),
            ],
            vec![
                ack(1, r, 1, OutcomeKind::Ok, true),
                relay(3, 2, r, 2, OutcomeKind::Ok),
            ],
            vec![relay(3, 2, r, 2, OutcomeKind::Ok)],
        ] {
            let f = findings(acks, &[]);
            assert_eq!(f.unverified_relays, 1, "{:?}", f.i1);
            assert!(
                f.i1[0].contains("relay of rid") && f.i1[0].contains("from node 2"),
                "{:?}",
                f.i1
            );
        }
        // An unjudged sequencer answer (no store: not judged) does not
        // back a relay.
        let mut unjudged = ack(2, r, 1, OutcomeKind::Ok, true);
        unjudged.judged = None;
        let f = findings(vec![unjudged, relay(3, 2, r, 2, OutcomeKind::Ok)], &[]);
        assert_eq!(f.unverified_relays, 1);
    }

    #[test]
    fn i1_a_requester_that_re_executes_as_holder_is_judged() {
        // Node 2 replied OK to node 3's forward; node 3 then took the lease
        // and answered the same rid OK to its own client by executing it
        // (`Sequenced`). The C0 oracle took that for a relay of node 2's
        // reply (same rid, same kind, another node) and never judged it.
        use constellation_authority::AnswerSource;
        let treat = |s| super::answer_treatment(s);
        let t = treat(Some(AnswerSource::Sequenced(RowCommit::LONE)));
        assert!(t.judge && t.relay_from.is_none());
        assert_eq!(t.claimed, Some(RowCommit::LONE));
        // A `MutateReply` (no source) is a sequencer's answer: judged.
        let t = treat(None);
        assert!(t.judge && t.relay_from.is_none() && t.claimed.is_none());
        // Only an explicit relay is not judged, and it names its sender.
        let t = treat(Some(AnswerSource::Relayed { from: 2 }));
        assert!(!t.judge);
        assert_eq!(t.relay_from, Some(2));
        // A node's own error is not a sequencer's answer: not judged, not a
        // relay, counted apart.
        let t = treat(Some(AnswerSource::None));
        assert!(!t.judge && t.relay_from.is_none() && t.unsourced);
    }

    #[test]
    fn i4_an_unjournaled_refusal_whose_rid_completes_is_reported() {
        let r = rid(1, 1);
        let mut a = ack(1, r, 1, OutcomeKind::Refused(Code::Again), false);
        if let Some(j) = a.judged.as_mut() {
            j.class = None;
            j.expected = Expected::Unjournaled;
        }
        let f = findings(vec![a.clone()], &[]);
        assert!(f.unjournaled_landed.is_empty());
        assert_eq!(f.unjournaled_refusals.values().sum::<usize>(), 1);
        let f = findings(vec![a.clone()], &[(r, (1, 0))]);
        assert_eq!(f.unjournaled_landed.len(), 1, "{:?}", f.unjournaled_landed);
        assert!(f.unjournaled_landed[0].contains("unjournaled_then_landed"));
        // `EROFS` is not a final answer (the caller retries the op, and it
        // runs): not a refusal that landed.
        a.kind = OutcomeKind::Refused(Code::ReadOnly);
        let f = findings(vec![a], &[(r, (1, 0))]);
        assert!(
            f.unjournaled_landed.is_empty(),
            "{:?}",
            f.unjournaled_landed
        );
    }

    /// lki1 triage (`delegated-designated+commit` seed 69000): a lone root's
    /// `EROFS` under an unreachable designation journals nothing; its claim
    /// (the nominal rule of nothing journaled) is not compared with the
    /// vacuous `Singleton` judgement. A journaled refusal still is.
    #[test]
    fn an_unjournaled_refusals_claim_is_not_judged() {
        let archive = Some(RowCommit::ARCHIVE);
        let lone = Some(Judged::Singleton);
        assert!(check_claim(archive, None, lone, &[], false, false).is_some());
        assert_eq!(check_claim(archive, None, lone, &[], false, true), None);
        let rows = [RowCommit::ARCHIVE];
        assert!(check_claim(archive, None, lone, &rows, false, false).is_some());
    }

    #[test]
    fn archive_index_fences_a_superseded_epoch() {
        let a = ArchiveIndex::default();
        a.note_segment(1, seg(2, &[rid(1, 1)]));
        // A lower-epoch segment after a higher one is fenced: not archived.
        a.note_segment(2, seg(1, &[rid(1, 2)]));
        a.note_segment(3, seg(2, &[rid(1, 3)]));
        assert!(a.outcome_of(rid(1, 1)).is_some());
        assert!(
            a.outcome_of(rid(1, 2)).is_none(),
            "a fenced segment carries nothing"
        );
        assert!(a.is_fenced(2));
        assert_eq!(a.outcome_of(rid(1, 3)).map(|(s, _, _)| s), Some(3));
    }

    #[test]
    fn archive_index_waits_for_a_gap() {
        let a = ArchiveIndex::default();
        a.note_segment(2, seg(1, &[rid(1, 2)]));
        assert!(
            a.outcome_of(rid(1, 2)).is_none(),
            "past a gap the prefix has not reached it"
        );
        a.note_segment(1, seg(1, &[rid(1, 1)]));
        assert!(a.outcome_of(rid(1, 2)).is_some());
    }

    #[test]
    fn i1_root_group_needs_every_listed_backup() {
        let w = world();
        let r = rid(3, 1);
        let tx = w.create(1, r, "a");
        w.lease(1, 1, &[2]);
        let st = holder_state(1);
        // The holder's journal holds it, the listed backup's tail does not.
        let j = w.judge(1, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("listed backup 2"), "{}", j.detail);
        assert_eq!(j.expected, Expected::Class(Judged::Group));
        // The backup holds it (as the row was shipped to it).
        w.metas[&2]
            .backup_append(1, std::slice::from_ref(&tx))
            .unwrap();
        let j = w.judge(1, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
        // Records that differ are not the same row.
        let other = BackupTx {
            records: vec![LogRecord::Completed { rid: r }],
            ..tx.clone()
        };
        w.metas[&2].backup_trim(1, u64::MAX, &[]).unwrap();
        w.metas[&2].backup_append(1, &[other]).unwrap();
        assert_eq!(w.judge(1, &st, r).class, None);
        // The tail is trimmed once the segment lands: the archive holds it.
        w.metas[&2].backup_trim(1, u64::MAX, &[]).unwrap();
        w.o.bucket.archive.note_segment(1, seg(1, &[r]));
        assert_eq!(w.judge(1, &st, r).class, Some(Judged::Archive));
    }

    /// Plan 46 M1.1: with no delegate backup recorded, the root's appended
    /// copy counts once the root's own configuration committed it: every
    /// backup the lease lists holds the append.
    #[test]
    fn i1_deleg_group_counts_the_root_append_once_the_roots_backups_hold_it() {
        let w = world();
        let r = rid(3, 1);
        let m = &w.metas[&2];
        let ino = m.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let (records, _) = m
            .delegate_execute(
                &op,
                Some(r),
                1,
                Position::ZERO,
                &constellation_meta::locks::LockTag::NONE,
                Now::same(0),
            )
            .unwrap();
        w.lease(1, 1, &[3]);
        let st = AnswererState {
            holds_gens: vec![1],
            ..Default::default()
        };
        w.metas[&1]
            .apply_delegate_tx(&records, Some(r), 1, 1, Position::ZERO)
            .unwrap();
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![3], false));
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("listed backup 3"), "{}", j.detail);
        // The root's backup holds the append: the root's configuration
        // committed its copy.
        let txs = Replica::journal_txs_from(&*w.metas[&1], 0, usize::MAX);
        w.metas[&3].backup_append(1, &txs).unwrap();
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
    }

    /// Node 2's delegate row `(1, 1)` of rid `(3, 1)`, appended to node 1's
    /// (the root's) journal; returns the root's journal.
    fn root_appended_row(w: &World) -> Vec<BackupTx> {
        let r = rid(3, 1);
        let m = &w.metas[&2];
        let ino = m.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let (records, _) = m
            .delegate_execute(
                &op,
                Some(r),
                1,
                Position::ZERO,
                &constellation_meta::locks::LockTag::NONE,
                Now::same(0),
            )
            .unwrap();
        w.metas[&1]
            .apply_delegate_tx(&records, Some(r), 1, 1, Position::ZERO)
            .unwrap();
        Replica::journal_txs_from(&*w.metas[&1], 0, usize::MAX)
    }

    /// Plan 46 M1.1 review: the root's copy counts against the tenure that
    /// appended the row. Epoch 1's root (1) and backup (3) committed it; a
    /// successor's lease (epoch 2) lists no backup, and the row is still
    /// committed.
    #[test]
    fn i1_deleg_group_counts_a_row_committed_under_an_earlier_tenure() {
        let w = world();
        let r = rid(3, 1);
        let st = AnswererState {
            holds_gens: vec![1],
            ..Default::default()
        };
        let txs = root_appended_row(&w);
        w.lease(1, 2, &[]);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("no append"), "{}", j.detail);
        // Appended in epoch 1, which listed backup 3 (it lacks the row).
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![3], false));
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("epoch 1"), "{}", j.detail);
        w.metas[&3].backup_append(1, &txs).unwrap();
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
        assert!(j.detail.contains("tenure of epoch 1"), "{}", j.detail);
    }

    /// Plan 46 M1.1 review probe: node 1 was epoch 1's backup, sealed, and
    /// holds epoch 2 alone (no backup). Delegate 2's row is appended in
    /// epoch 2 to node 1's journal only: nothing but the archive commits
    /// it, and node 1's old backup role does not.
    #[test]
    fn i1_deleg_group_lone_successor_row_is_not_committed_by_its_old_backup_role() {
        let w = world();
        let r = rid(3, 1);
        let st = AnswererState {
            holds_gens: vec![1],
            ..Default::default()
        };
        root_appended_row(&w);
        w.lease(1, 2, &[]);
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (2, vec![], false));
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("no backup"), "{}", j.detail);
    }

    /// Plan 46 M1.1 steps 3 and 4: a row's version must be the configuration
    /// in force, in the namespace its rule is stamped from: the lease's newest
    /// version for a root row, the node's applied `DelegateNodeGroup` version for a
    /// `Group{Delegate}` row, with the lag of the node's previous event. A
    /// version that existed but was superseded (before that event), a
    /// version-0 placeholder under a lease, and the other namespace's version
    /// are violations.
    #[test]
    fn row_versions_must_be_the_configuration_in_force() {
        use constellation_meta::{GroupOn, RowCommit};
        let mut rv = RowVersions::default();
        let group = std::cell::Cell::new(1u64);
        let group_now = |gen: u64| if gen == 7 { group.get() } else { 0 };
        // The node's previous event had applied lease version 5 and the
        // group at 1.
        rv.group_seen.insert((1, 7), 1);
        rv.tick(1, 5, &group_now);
        // Then it applies lease version 6 and group version 2.
        group.set(2);
        let row = |first, epoch, gen, idx, class| constellation_meta::AuditRow {
            first,
            epoch,
            gen,
            idx,
            class,
        };
        let rows = vec![
            // ok: the newest lease version / the one the node had read.
            row(1, 3, 0, 0, RowCommit::group(6, GroupOn::Root)),
            row(2, 3, 0, 0, RowCommit::Singleton { config_version: 5 }),
            // ok: a delegate row at the applied group / the one before it.
            row(3, 0, 7, 1, RowCommit::group(2, GroupOn::Delegate)),
            row(4, 0, 7, 2, RowCommit::group(1, GroupOn::Delegate)),
            // ok: a root row at epoch 0 (no lease held now) carrying the
            // lease the node holds or last held, and a delegate archive row
            // with no group.
            row(5, 0, 0, 0, RowCommit::Singleton { config_version: 6 }),
            row(6, 0, 7, 3, RowCommit::ARCHIVE),
            // ok: Final is not judged.
            row(7, 0, 7, 4, RowCommit::Final),
            // 8: version 0 under a lease: the adopted-tail placeholder.
            row(8, 3, 0, 0, RowCommit::group(0, GroupOn::Root)),
            // 9: the archive row journaled after `clear_holder_epoch` at 0.
            row(9, 3, 0, 0, RowCommit::ARCHIVE),
            // 10: a group version 3 nobody has applied; 11: a lease version
            // on a delegate-group row.
            row(10, 0, 7, 5, RowCommit::group(3, GroupOn::Delegate)),
            row(11, 0, 7, 6, RowCommit::group(5, GroupOn::Delegate)),
            // 12: a delegate-group version on a root-group row.
            row(12, 3, 0, 0, RowCommit::group(2, GroupOn::Root)),
        ];
        rv.judge(1, &rows, 6, &group_now, &|g| group_now(g), &|_, _| None);
        let seqs: Vec<_> = rv
            .violations
            .iter()
            .map(|v| v.split("seq ").nth(1).unwrap().split(' ').next().unwrap())
            .collect();
        assert_eq!(seqs, ["8", "9", "10", "11", "12"], "{:?}", rv.violations);
        assert_eq!((rv.checked, rv.unstamped), (11, 1));
        rv.judge(1, &rows, 6, &group_now, &|g| group_now(g), &|_, _| None);
        assert_eq!(rv.violations.len(), 5, "judged once");
    }

    /// Plan 46 M1.1 review (SHOULD-1): a row judged once and seen again
    /// under another rule was re-tagged, and the re-tag is judged: it keeps
    /// the version the row was journaled in (a placeholder 0 is a
    /// violation, the same version under another rule is not), and a
    /// `Final` row that gets a rule is judged as a new row. Fails on the
    /// parent: a judged seq was never looked at again.
    #[test]
    fn a_retag_is_judged_again() {
        use constellation_meta::{GroupOn, RowCommit};
        let mut rv = RowVersions::default();
        let group_now = |_: u64| 0;
        rv.tick(1, 6, &group_now);
        let row = |first, class| constellation_meta::AuditRow {
            first,
            epoch: 3,
            gen: 0,
            idx: 0,
            class,
        };
        let judge = |rv: &mut RowVersions, rows: &[constellation_meta::AuditRow]| {
            rv.judge(1, rows, 6, &group_now, &|g| group_now(g), &|_, _| None)
        };
        let journaled = vec![
            row(1, RowCommit::group(6, GroupOn::Root)),
            row(2, RowCommit::group(6, GroupOn::Root)),
            row(3, RowCommit::Final),
            row(4, RowCommit::Final),
        ];
        judge(&mut rv, &journaled);
        assert!(rv.violations.is_empty(), "{:?}", rv.violations);
        let retagged = vec![
            // Same version, another rule: fine.
            row(1, RowCommit::Singleton { config_version: 6 }),
            // The placeholder version: a violation.
            row(2, RowCommit::LONE),
            // A Final row given the configuration in force: fine; one given
            // a version that is not in force: a violation.
            row(3, RowCommit::Singleton { config_version: 6 }),
            row(4, RowCommit::Singleton { config_version: 2 }),
        ];
        judge(&mut rv, &retagged);
        let seqs: Vec<_> = rv
            .violations
            .iter()
            .map(|v| v.split("seq ").nth(1).unwrap().split(' ').next().unwrap())
            .collect();
        assert_eq!(seqs, ["2", "4"], "{:?}", rv.violations);
        assert_eq!(rv.retags, 4);
        judge(&mut rv, &retagged);
        assert_eq!(rv.violations.len(), 2, "judged once");
    }

    /// Step 4: a version that existed but was superseded before the node's
    /// previous event is caught, which step 3's "in some history" let by
    /// (the negative control `per_row_class` stamps one in no history; this
    /// is the stale one: a row journaled by a path that does not follow the
    /// configuration).
    #[test]
    fn a_stale_but_existing_version_is_caught() {
        use constellation_meta::{GroupOn, RowCommit};
        let mut rv = RowVersions::default();
        // The node applied 6, then 7 at its previous event, and 8 now.
        rv.tick(1, 6, &|_| 0);
        rv.tick(1, 7, &|_| 0);
        let rows = vec![constellation_meta::AuditRow {
            first: 1,
            epoch: 3,
            gen: 0,
            idx: 0,
            class: RowCommit::group(6, GroupOn::Root),
        }];
        rv.judge(1, &rows, 8, &|_| 0, &|_| 0, &|_, _| None);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
        // The delegate's group: applied at 3 two events ago, now 5.
        let mut rv = RowVersions::default();
        rv.group_seen.insert((1, 7), 4);
        let rows = vec![constellation_meta::AuditRow {
            first: 1,
            epoch: 0,
            gen: 7,
            idx: 1,
            class: RowCommit::group(3, GroupOn::Delegate),
        }];
        rv.judge(1, &rows, 0, &|_| 5, &|_| 5, &|_, _| None);
        // By journal position: version 1 was applied while the journal's
        // next seq was 1, version 2 at 5 (both in this one event): a row at
        // seq 7 carrying 1 is one record behind and is caught, though the
        // node's previous event had applied 1; the row at seq 3 carrying 1
        // was journaled before version 2 and is right.
        let mut rv = RowVersions::default();
        rv.group_seen.insert((1, 7), 1);
        let row = |first, v| constellation_meta::AuditRow {
            first,
            epoch: 0,
            gen: 7,
            idx: first,
            class: RowCommit::group(v, GroupOn::Delegate),
        };
        let at = |_gen: u64, first: u64| Some(if first >= 5 { 2 } else { 1 });
        rv.judge(1, &[row(3, 1), row(7, 1)], 0, &|_| 2, &|_| 2, &at);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
        assert!(rv.violations[0].contains("seq 7"), "{:?}", rv.violations);
        rv.judge(1, &[row(9, 2)], 0, &|_| 2, &|_| 2, &at);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
    }

    /// Plan 46 M1.1 step 5 triage (long-delegated-backup seed 634): two rows
    /// of a generation new to the oracle, journaled in one event before the
    /// generation's first `DelegateNodeGroup` record was installed (later in that
    /// event: no recorded version at or below their position), both carry
    /// version 0. The first row seeds the generation's "previous event"
    /// entry with the version applied by the end of the event; the second
    /// is judged against the same previous event as the first, not that.
    #[test]
    fn rows_of_a_new_generation_in_one_event_share_its_previous_event() {
        use constellation_meta::{GroupOn, RowCommit};
        let mut rv = RowVersions::default();
        let row = |first, idx| constellation_meta::AuditRow {
            first,
            epoch: 0,
            gen: 7,
            idx,
            class: RowCommit::group(0, GroupOn::Delegate),
        };
        rv.judge(1, &[row(17, 1), row(18, 2)], 0, &|_| 1, &|_| 1, &|_, _| {
            None
        });
        assert!(rv.violations.is_empty(), "{:?}", rv.violations);
        // The record is in force from the next event on: a row stamped 0
        // two events later is one behind.
        rv.tick(1, 0, &|_| 1);
        rv.tick(1, 0, &|_| 1);
        rv.judge(1, &[row(19, 3)], 0, &|_| 1, &|_| 1, &|_, _| None);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
    }

    /// A root row journaled at epoch 0 carries the lease the node last held
    /// (`Core::root_config_version`; `long-delegated-late-answers` seed 1793
    /// under slow lease reads: a deposed holder drained its inbox at epoch 0
    /// and stamped the archive rows with its tenure's version 4, which the
    /// oracle's "epoch 0 is version 0" called a violation), 0 when it never
    /// held one; and a delegate's group row journaled before the generation's
    /// first `DelegateNodeGroup` record is version 0 by position (`Meta::
    /// deleg_group_at` is `Some(0)`; seed 1938 of `long-delegated-backup`).
    #[test]
    fn an_epoch_0_row_carries_the_last_held_lease() {
        use constellation_meta::{GroupOn, RowCommit};
        let row = |first, gen, class| constellation_meta::AuditRow {
            first,
            epoch: 0,
            gen,
            idx: first,
            class,
        };
        // The node last held a lease of version 4.
        let mut rv = RowVersions::default();
        rv.tick(1, 4, &|_| 0);
        let rows = [
            row(1, 0, RowCommit::group(4, GroupOn::Archive)),
            // Version 0 after a tenure: a placeholder, not the configuration.
            row(2, 0, RowCommit::group(0, GroupOn::Archive)),
            row(3, 0, RowCommit::group(3, GroupOn::Archive)),
        ];
        rv.judge(1, &rows, 4, &|_| 0, &|_| 0, &|_, _| None);
        let seqs: Vec<_> = rv
            .violations
            .iter()
            .map(|v| v.split("seq ").nth(1).unwrap().split(' ').next().unwrap())
            .collect();
        assert_eq!(seqs, ["2", "3"], "{:?}", rv.violations);
        // A node that never held a lease: version 0.
        let mut rv = RowVersions::default();
        rv.judge(
            2,
            &[row(1, 0, RowCommit::LONE)],
            0,
            &|_| 0,
            &|_| 0,
            &|_, _| None,
        );
        assert!(rv.violations.is_empty(), "{:?}", rv.violations);
        // A delegate row before the first record, judged after the record
        // was applied in the same event: version 0 by position.
        let mut rv = RowVersions::default();
        let at = |_gen: u64, first: u64| Some(if first >= 5 { 1 } else { 0 });
        let rows = [
            row(3, 7, RowCommit::group(0, GroupOn::Delegate)),
            row(6, 7, RowCommit::group(1, GroupOn::Delegate)),
            row(7, 7, RowCommit::group(0, GroupOn::Delegate)),
        ];
        rv.judge(3, &rows, 0, &|_| 1, &|_| 1, &at);
        assert_eq!(rv.violations.len(), 1, "{:?}", rv.violations);
        assert!(rv.violations[0].contains("seq 7"), "{:?}", rv.violations);
    }

    /// Plan 46 M1.4 step 3: an epoch hold's journal counts as the root's
    /// copy once a member's store holds the row (`Core::config_committed_jseq`
    /// counts by the member-acked seq): the delegate's own journal row, or a
    /// streamed copy of the hold's epoch through the row. The owner's
    /// journal alone does not count, and a lone root's does not either.
    #[test]
    fn i1_deleg_group_counts_a_hold_by_a_member_not_the_owners_journal() {
        let w = world();
        let r = rid(3, 1);
        let st = AnswererState {
            holds_gens: vec![1],
            ..Default::default()
        };
        let txs = root_appended_row(&w);
        w.lease(1, 1, &[]);
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![], false));
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("lone root's"), "{}", j.detail);
        // Appended in a hold: the delegate that is a member holds its own
        // row (owner + member: two copies); one that is no member of the
        // hold's epoch does not.
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![], true));
        w.o.note_hold_members(1, &[1, 3]);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "a non-member delegate: {}", j.detail);
        w.o.note_hold_members(1, &[2]);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
        assert!(j.detail.contains("own journal"), "{}", j.detail);
        w.o.hold_members.lock().unwrap().clear();
        // The owner's journal alone is no member's copy, a streamed copy of
        // another epoch is none either, one of the hold's epoch through the
        // row is.
        let t = txs.iter().find(|t| t.origin == (1, 1)).expect("row");
        assert_eq!(hold_member_streamed(&w.o, Some(1), &[1], t.last), None);
        w.metas[&3]
            .install_streamed(9, 1, t.first, t.last, &t.records)
            .unwrap();
        w.o.note_streamed_to(3, &w.metas[&3], 9);
        assert_eq!(hold_member_streamed(&w.o, Some(1), &[1], t.last), None);
        w.metas[&3]
            .install_streamed(1, 1, t.first, t.last, &t.records)
            .unwrap();
        w.o.note_streamed_to(3, &w.metas[&3], 9);
        // A subscriber of the owner's stream that is no member of the hold
        // is not a copy of the group.
        assert_eq!(hold_member_streamed(&w.o, Some(1), &[1], t.last), None);
        w.o.note_hold_members(1, &[1, 3]);
        assert_eq!(hold_member_streamed(&w.o, Some(1), &[1], t.last), Some(3));
        assert_eq!(hold_member_streamed(&w.o, Some(3), &[1], t.last), None);
        // A hold that began after the append counts as well.
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![], false));
        w.o.epoch_holds.lock().unwrap().insert(1);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
    }

    #[test]
    fn i1_deleg_group_needs_the_root_append_when_no_backup() {
        let w = world();
        let r = rid(3, 1);
        // Node 2 is the delegate of generation 1: its own journal row (1, 1).
        let m = &w.metas[&2];
        let ino = m.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let (records, idx) = m
            .delegate_execute(
                &op,
                Some(r),
                1,
                Position::ZERO,
                &constellation_meta::locks::LockTag::NONE,
                Now::same(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
        w.lease(1, 1, &[]);
        let st = AnswererState {
            holds_gens: vec![1],
            ..Default::default()
        };
        let j = w.judge(2, &st, r);
        assert_eq!(j.tx.as_ref().map(|t| t.origin), Some((1, 1)));
        assert_eq!(j.class, None, "{}", j.detail);
        assert_eq!(j.expected, Expected::Class(Judged::Group));
        // The root appends the row (origin (1, 1)) to its journal. A lone
        // root's journal is no second copy (plan 46 M1.1): with no backup
        // listed only the archive commits the root's copy.
        w.metas[&1]
            .apply_delegate_tx(&records, Some(r), 1, 1, Position::ZERO)
            .unwrap();
        w.o.appended
            .lock()
            .unwrap()
            .insert((1, 1), (1, vec![], false));
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("no backup"), "{}", j.detail);
        // With a recorded backup it is the backup's tail that must hold it.
        let st = AnswererState {
            holds_gens: vec![1],
            deleg_groups: vec![(1, vec![3])],
            ..Default::default()
        };
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        let dtx = DelegateTx {
            idx: 1,
            rid: Some(r),
            records,
            deps: Position::ZERO,
        };
        w.metas[&3].deleg_backup_append(1, 0, &[dtx]).unwrap();
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
    }

    /// Plan 43 C2 (§7.1 item 3): the delegate's group is the record its own
    /// table holds, not the backup the delegate appends to. A backup that
    /// holds the row but is in no applied record is not the group: the
    /// root's journal must (D4: a configuration counts once its master has
    /// it durably). Once the delegate's table records the backup, the
    /// backup's tail is the check.
    #[test]
    fn i1_deleg_group_reads_the_record_not_the_append_target() {
        let w = world();
        let r = rid(3, 1);
        let m = &w.metas[&2];
        let ino = m.allocate_ino(ROOT_INO).unwrap();
        let op = MutateOp::Create {
            parent: ROOT_INO,
            name: "x".into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let (records, _) = m
            .delegate_execute(
                &op,
                Some(r),
                1,
                Position::ZERO,
                &constellation_meta::locks::LockTag::NONE,
                Now::same(0),
            )
            .unwrap();
        w.lease(1, 1, &[]);
        // Node 3 holds the row (the delegate appended to it) ...
        let dtx = DelegateTx {
            idx: 1,
            rid: Some(r),
            records,
            deps: Position::ZERO,
        };
        w.metas[&3].deleg_backup_append(1, 0, &[dtx]).unwrap();
        // ... but the delegate's table records no group: not committed.
        let st = AnswererState {
            holds_gens: vec![1],
            deleg_groups: recorded_groups(&w.metas[&2], [1].into_iter()),
            ..Default::default()
        };
        assert_eq!(st.deleg_groups, vec![(1, vec![])]);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, None, "{}", j.detail);
        // The record reaches the delegate's table: the backup is the group.
        w.metas[&2]
            .apply_segment(
                2,
                1,
                &[LogRecord::DelegateNodeGroup {
                    node: 3,
                    gen: 1,
                    version: 1,
                    backups: vec![3],
                }],
                &Default::default(),
            )
            .unwrap();
        let st = AnswererState {
            holds_gens: vec![1],
            deleg_groups: recorded_groups(&w.metas[&2], [1].into_iter()),
            ..Default::default()
        };
        assert_eq!(st.deleg_groups, vec![(1, vec![3])]);
        let j = w.judge(2, &st, r);
        assert_eq!(j.class, Some(Judged::Group), "{}", j.detail);
    }

    #[test]
    fn i1_lone_needs_an_empty_group_and_no_peer_in_budget() {
        let w = world();
        let own = rid(1, 1);
        w.create(1, own, "a");
        w.lease(1, 1, &[]);
        let st = holder_state(1);
        // Its own client, a group of itself, nobody near: Lone.
        let j = w.judge(1, &st, own);
        assert_eq!(j.class, Some(Judged::Singleton), "{}", j.detail);
        // Another node's client is not answered Lone.
        let remote = rid(3, 1);
        w.create(1, remote, "b");
        let j = w.judge(1, &st, remote);
        assert_eq!(j.class, None, "{}", j.detail);
        assert_eq!(j.expected, Expected::Class(Judged::Archive));
        // A peer linked within the budget (the node could have had a backup).
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        w.bus.attach(2, tx);
        w.bus.set_rtt(1, 2, 5);
        let j = w.judge(1, &st, own);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("peer 2 is linked"), "{}", j.detail);
        // A link just up is a peer too (no stability term): an answer given
        // `Lone` within the window of a link coming up is a violation.
        w.o.note_directory(1, [(2u64, 9_990i64)].into());
        w.o.budgets.lock().unwrap().insert(1, (50, 200));
        let j = w.judge(1, &st, own);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("peer 2 is linked"), "{}", j.detail);
        // ... unless it is out of budget, or the link is cut.
        w.o.budgets.lock().unwrap().insert(1, (50, 0));
        w.bus.set_rtt(1, 2, 120);
        assert_eq!(w.judge(1, &st, own).class, Some(Judged::Singleton));
        w.bus.set_rtt(1, 2, 5);
        w.bus.set_partition(1, 2, true);
        assert_eq!(w.judge(1, &st, own).class, Some(Judged::Singleton));
        w.bus.set_partition(1, 2, false);
        // ... or the directory its core was given does not report the peer
        // up (the bus is ahead of the driver's last poll: the core was told
        // of nobody): lone by its own information. A core with no directory
        // entry at all was told nothing and counts every link.
        w.o.note_directory(1, BTreeMap::new());
        assert_eq!(w.judge(1, &st, own).class, Some(Judged::Singleton));
        w.o.directory.lock().unwrap().remove(&1);
        assert_eq!(w.judge(1, &st, own).class, None);
        // A listed backup (even one nobody can reach) makes it a group of two.
        let w2 = world();
        w2.create(1, own, "a");
        w2.lease(1, 1, &[2]);
        let j = w2.judge(1, &st, own);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("listed backup 2"), "{}", j.detail);
    }

    #[test]
    fn i1_non_stealable_is_a_designee_or_an_epoch_hold() {
        let w = world();
        let r = rid(3, 1);
        w.create(1, r, "a");
        w.lease(2, 3, &[]);
        let hold = AnswererState {
            epoch: Some(3),
            epoch_held: true,
            ..Default::default()
        };
        let j = w.judge(1, &hold, r);
        assert_eq!(j.class, Some(Judged::Final), "{}", j.detail);
    }

    /// A log with one applied segment whose completion of `r` is at record
    /// `i`, for the verbatim and late-landing checks.
    fn log_with(seg: Segment, completed: &[(Rid, (u64, usize))]) -> super::super::check::Oracle {
        let mut unfenced = BTreeMap::new();
        unfenced.insert(1, Arc::new(seg));
        super::super::check::Oracle {
            head: 1,
            final_dump: Vec::new(),
            at: BTreeMap::new(),
            completed: completed.iter().map(|(r, _)| (*r, 1)).collect(),
            completed_at: completed.iter().copied().collect(),
            refused_at: HashMap::new(),
            segments: 1,
            fenced: 0,
            unfenced,
            describe: Vec::new(),
        }
    }

    /// The operator's `abandon-group` gives up the rows only the dead group
    /// held: an OK answered under a group class whose rid the log never
    /// completed is exempt from I2 (c) exactly when it is in the
    /// operator-lost set (counted, not silently dropped); the same lost row
    /// outside the set still fails.
    #[test]
    fn i2c_exempts_exactly_the_operator_lost_rids() {
        let (r, other) = (rid(1, 1), rid(1, 2));
        let tx = |first: u64| TxRef {
            node: 1,
            first,
            last: first,
            origin: (0, 0),
            epoch: 1,
            records: vec![LogRecord::Completed { rid: r }],
        };
        let judged = |first: u64| {
            Some(Judgement {
                class: Some(Judged::Group),
                expected: Expected::Class(Judged::Group),
                tx: Some(tx(first)),
                detail: String::new(),
                mismatch: None,
            })
        };
        let acks = vec![
            AckEntry {
                judged: judged(5),
                ..ack(2, r, 5, OutcomeKind::Ok, true)
            },
            AckEntry {
                judged: judged(6),
                ..ack(2, other, 5, OutcomeKind::Ok, true)
            },
        ];
        let run = |lost: &BTreeSet<Rid>| {
            let o = Oracles::new(Bucket::new(1, (0, 0)));
            *o.acks.lock().unwrap() = acks.clone();
            let log = log_with(seg(1, &[]), &[]);
            end_of_run(
                &o,
                &log,
                &[],
                &HashMap::new(),
                &BTreeSet::new(),
                lost,
                &BTreeMap::new(),
                true,
            )
        };
        // Neither is in the set: both are absent.
        let f = run(&BTreeSet::new());
        assert_eq!(f.i2c.len(), 2, "{:?}", f.i2c);
        assert_eq!(f.i2c_operator_lost, 0);
        // Only `r` was given up: `other` (a lost row the operator did not
        // abandon) still fails.
        let f = run(&BTreeSet::from([r]));
        assert_eq!(f.i2c.len(), 1, "{:?}", f.i2c);
        assert!(f.i2c[0].contains(&format!("{other:?}")), "{:?}", f.i2c);
        assert_eq!(f.i2c_operator_lost, 1);
        // The unlogged-delegate-row set: the holder's own row `(0, 0)` and a
        // completed delegate row are not in it.
        let mut acks = acks;
        for a in &mut acks {
            a.judged.as_mut().unwrap().tx.as_mut().unwrap().origin =
                if a.rid == r { (2, 7) } else { (0, 0) };
        }
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        *o.acks.lock().unwrap() = acks;
        let lost = unlogged_delegate_rows(&o, &log_with(seg(1, &[]), &[]));
        assert_eq!(lost, BTreeSet::from([r]));
        let lost = unlogged_delegate_rows(&o, &log_with(seg(1, &[]), &[(r, (1, 0))]));
        assert!(lost.is_empty(), "{lost:?}");
    }

    #[test]
    fn i2c_an_adopted_tail_is_not_verbatim() {
        let r = rid(1, 1);
        let rec = vec![LogRecord::Completed { rid: r }];
        let committed = TxRef {
            node: 1,
            first: 5,
            last: 5,
            origin: (0, 0),
            epoch: 1,
            records: rec.clone(),
        };
        let shipped = |epoch: u64, node: u64, rows: Vec<u64>| Segment {
            node,
            epoch,
            records: rec.clone(),
            through: 5,
            rows,
            origins: vec![(0, 0)],
        };
        let log = log_with(shipped(1, 1, vec![5]), &[(r, (1, 0))]);
        assert_eq!(not_verbatim(&log, r, &committed, true), None);
        // The successor re-journaled it under its own epoch (and jseq).
        let log = log_with(shipped(2, 2, vec![1]), &[(r, (1, 0))]);
        let why = not_verbatim(&log, r, &committed, true).expect("not verbatim");
        assert!(why.contains("not under the identity"), "{why}");
        // Never completed while the run converged: absent.
        let log = log_with(shipped(1, 1, vec![5]), &[]);
        assert!(not_verbatim(&log, r, &committed, true)
            .unwrap()
            .contains("absent"));
        assert_eq!(not_verbatim(&log, r, &committed, false), None);
        // A delegate's row is verbatim by its origin.
        let deleg = TxRef {
            origin: (2, 7),
            ..committed.clone()
        };
        let mut s = shipped(1, 1, vec![5]);
        s.origins = vec![(2, 7)];
        let log = log_with(s, &[(r, (1, 0))]);
        assert_eq!(not_verbatim(&log, r, &deleg, true), None);
        let mut s = shipped(1, 1, vec![5]);
        s.origins = vec![(2, 8)];
        let log = log_with(s, &[(r, (1, 0))]);
        assert!(not_verbatim(&log, r, &deleg, true).is_some());
    }

    #[test]
    fn i4_an_in_doubt_rid_landing_after_a_later_op_is_late() {
        let (r1, r2) = (rid(1, 1), rid(1, 2));
        let in_doubt = [(1u64, r1, 0u64, 10u64), (1, r1, 0, 30)];
        let invokes = [(r1, 5u64), (r2, 11)];
        // r1 (answered in doubt at tick 10) lands at (5, 0); r2, invoked
        // afterwards by the same node, landed first at (3, 0): late.
        let log = log_with(seg(1, &[]), &[(r1, (5, 0)), (r2, (3, 0))]);
        let (n, why) = super::super::check::no_late_landing(&in_doubt, &invokes, &log);
        assert_eq!(n, 1, "{why:?}");
        assert!(
            why[0].contains("I4 unacked_fails_never_lands_late"),
            "{why:?}"
        );
        // r1 landing before the later op is clean.
        let log = log_with(seg(1, &[]), &[(r1, (2, 0)), (r2, (3, 0))]);
        assert_eq!(
            super::super::check::no_late_landing(&in_doubt, &invokes, &log).0,
            0
        );
        // An op another node invoked is not this node's client's.
        let invokes = [(r1, 5u64), (rid(2, 1), 11)];
        let log = log_with(seg(1, &[]), &[(r1, (5, 0)), (rid(2, 1), (3, 0))]);
        assert_eq!(
            super::super::check::no_late_landing(&in_doubt, &invokes, &log).0,
            0
        );
    }

    /// Plan 43 C2 (§7.1 item 6): a settled answer (`Abandoned`, to a client)
    /// whose rid the log completes — anywhere, not just after a later op —
    /// is an I4 violation; a sequencer's `Abandoned` reply to a requester is
    /// not the client's answer. Fails on the C0 oracle: no such check.
    #[test]
    fn i4_a_settled_eio_whose_rid_completes_is_a_violation() {
        let (r1, r2) = (rid(1, 1), rid(1, 2));
        let entry = |rid: Rid, reply: bool| AckEntry {
            node: 1,
            rid,
            tick: 3,
            at_ms: 9_000,
            kind: OutcomeKind::Abandoned,
            reply,
            relay_from: None,
            claimed: None,
            judged: None,
        };
        let acks = vec![entry(r1, false), entry(r2, true)];
        let log = log_with(seg(1, &[]), &[(r1, (5, 0)), (r2, (3, 0))]);
        let why = super::super::check::no_settled_landing(&acks, &log);
        assert_eq!(why.len(), 1, "{why:?}");
        assert!(
            why[0].contains("I4: rid") && why[0].contains("answered Abandoned"),
            "{why:?}"
        );
        // Not in the log: settled indeed.
        let log = log_with(seg(1, &[]), &[(r2, (3, 0))]);
        assert!(super::super::check::no_settled_landing(&acks, &log).is_empty());
    }

    #[test]
    fn i2_two_nodes_queue_one_rid() {
        let mut l = IdentityLedger::default();
        let r = rid(2, 5);
        l.update(1, [r].into(), [r].into(), BTreeSet::new(), 1);
        assert!(l.violations.is_empty(), "one copy is fine");
        // The same node holding the rid queued and executing is one copy
        // (the client's retry merged with its queue entry).
        l.update(1, [r].into(), [r].into(), [r].into(), 2);
        assert!(l.violations.is_empty());
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r].into(), 3);
        assert_eq!(l.violations.len(), 1, "{:?}", l.violations);
        let text = &l.violations[&r];
        assert!(text.contains("queued on 2 (executing) and 1"), "{text}");
        // A crash drops the executing copy but keeps the queue.
        let mut l = IdentityLedger::default();
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r].into(), 1);
        l.crash(2);
        l.update(1, [r].into(), [r].into(), BTreeSet::new(), 2);
        assert!(l.violations.is_empty());
        let mut l = IdentityLedger::default();
        l.update(1, [r].into(), [r].into(), BTreeSet::new(), 1);
        l.crash(1);
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r].into(), 2);
        assert_eq!(l.violations.len(), 1, "the queue survives a crash");
        let mut l = IdentityLedger::default();
        l.update(1, [r].into(), [r].into(), BTreeSet::new(), 1);
        l.wipe(1);
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r].into(), 2);
        assert!(l.violations.is_empty(), "a restart without the journal");
        // Plan 43 C1-4: a pair none of whose queued copies stands for a
        // committed answer is the lone branch's: counted apart.
        let mut l = IdentityLedger::default();
        l.update(1, [r].into(), BTreeSet::new(), BTreeSet::new(), 1);
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r].into(), 2);
        assert!(l.violations.is_empty());
        assert_eq!(l.ungated.len(), 1);
    }

    /// Plan 46 M0.2 review: a rid the operator's drop kept queued with its
    /// op is counted with the ungated pairs (a requester still waiting, the
    /// requester's own queue), not judged; the same pairs for a rid the
    /// drop did not keep are still violations.
    #[test]
    fn i2_a_rid_the_drop_kept_is_counted_not_judged() {
        let (r, other) = (rid(1, 1), rid(1, 2));
        let mut l = IdentityLedger::default();
        l.update(1, [r, other].into(), [r, other].into(), BTreeSet::new(), 1);
        l.update(2, BTreeSet::new(), BTreeSet::new(), [r, other].into(), 2);
        assert_eq!(l.violations.len(), 2, "{:?}", l.violations);
        // The drop's answer reaches the operator after the queue changed.
        l.keep([r]);
        assert_eq!(l.violations.len(), 1, "{:?}", l.violations);
        assert!(l.violations.contains_key(&other));
        assert!(l.ungated.contains_key(&r), "counted");
        l.update(3, [r].into(), [r].into(), BTreeSet::new(), 3);
        assert!(!l.violations.contains_key(&r), "the requester's own queue");
    }
}

/// ADR-47's audit: what the root decided to refuse for good under a
/// non-stealable grant, what its generations executed, and the clients'
/// submissions, with ticks.
#[derive(Default)]
pub struct FenceAudit {
    /// `(rid, code, by the inbox drain, tick, node)`.
    pub refusals: Vec<(Rid, Code, bool, u64, NodeId)>,
    /// `(rid, generation, tick, node)`.
    pub execs: Vec<(Rid, u64, u64, NodeId)>,
    /// `(rid, tick)`.
    pub submits: Vec<(Rid, u64)>,
}

/// Check 1, `nonholder_refused_a_designee_ran` (I1, non-stealable arm): a
/// rid the root decided to refuse for good and a non-stealable generation
/// executed, whose `Completed` the log carries. The refusal is final when a
/// client heard it (an `EROFS`/`EXDEV` answered to its own client, not a
/// forward's reply, which the requester may hold) or
/// the inbox drain journaled it; a root's refusal the requester held back
/// is not. A client that submitted the rid again after the refusal may run
/// it legitimately.
fn fence_findings(
    audit: &FenceAudit,
    acks: &[AckEntry],
    log: &super::check::Oracle,
) -> Vec<String> {
    let mut out = Vec::new();
    for &(rid, code, drain, t_label, node) in &audit.refusals {
        let heard = acks
            .iter()
            .filter(|a| {
                !a.is_relay()
                    && !a.reply
                    && a.rid == rid
                    && matches!(
                        a.kind,
                        OutcomeKind::Refused(Code::ReadOnly | Code::CrossDevice)
                    )
            })
            .map(|a| a.tick)
            .min();
        let Some(t_ref) = heard.or(drain.then_some(t_label)) else {
            continue;
        };
        if !log.completed_at.contains_key(&rid) {
            continue;
        }
        for &(r, gen, t_exec, exec_node) in audit.execs.iter().filter(|e| e.0 == rid) {
            let resubmitted = audit
                .submits
                .iter()
                .any(|&(s, t)| s == rid && t > t_ref && t < t_exec);
            if t_exec < t_ref || !resubmitted {
                out.push(format!(
                    "nonholder_refused_a_designee_ran: rid {r:?} was refused for good ({code:?}, \
                     {}) by node {node} at tick {t_ref}, and node {exec_node} executed it under \
                     non-stealable generation {gen} at tick {t_exec}; the log completes it \
                     (the client submitted it at ticks {:?}; answers at {:?})",
                    if drain { "the inbox drain" } else { "the root" },
                    audit
                        .submits
                        .iter()
                        .filter(|s| s.0 == rid)
                        .map(|s| s.1)
                        .collect::<Vec<_>>(),
                    acks.iter()
                        .filter(|a| !a.is_relay() && a.rid == rid)
                        .map(|a| (a.tick, a.kind.name()))
                        .collect::<Vec<_>>(),
                ));
            }
        }
    }
    out
}
