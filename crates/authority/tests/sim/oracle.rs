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
use constellation_authority::core::AckClass;
use constellation_authority::segment::{self, Segment};
use constellation_authority::{NodeId, Replica, Seq};
use constellation_meta::{BackupTx, LogRecord, Meta, MutateOutcome, Rid};
use constellation_store_s3::{AckPolicy, Lease, LogStore};
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    /// Plan 43 C1-2: answers whose claim of a commit the judgement or the
    /// answering row's own class does not bear out (`check_claim`).
    pub claim_violations: Mutex<Vec<String>>,
    /// Outcomes the core answered a client with that no sequencer decided
    /// (`AnswerSource::None`: a node's own error): not judged.
    pub acks_unsourced: AtomicU64,
    /// Plan 43 C7b, I6: the lease-claim oracle (see [`LeaseClaims`]).
    pub leases: Arc<LeaseClaims>,
    /// Review follow-ups of the lone lead: own `Lone` rows of a lone
    /// delegate past the lead, or behind an uncommitted gated row of its own
    /// (see [`LoneLead`]).
    pub lone_lead: Mutex<LoneLead>,
}

/// The lone delegate's lead oracle (`Core::deleg_lone_held`): a row of its
/// own node's clients is `Lone` only when its journal is fewer transactions
/// past the committed prefix than the lead, and when no gated row of its
/// own node's clients is uncommitted before it.
///
/// A row is judged once, the step it first shows in the journal, against the
/// prefix the delegate knew committed then: the prefix at the journaling
/// (the class is decided under the writer lock, and the step that journaled
/// a row moved the prefix no further). A later look would weaken both
/// rules: the prefix only grows, and a gated row commits.
#[derive(Default)]
pub struct LoneLead {
    /// `(node, gen, idx)` of own `Lone` rows more than the lead past the
    /// committed prefix.
    pub past_lead: BTreeSet<(NodeId, u64, u64)>,
    /// `(node, gen, idx)` of own `Lone` rows behind an uncommitted own gated
    /// row.
    pub behind_gated: BTreeSet<(NodeId, u64, u64)>,
    /// The first violations, described.
    pub examples: Vec<String>,
    /// Rows judged (distinct `(node, gen, idx)` of own `Lone` rows seen),
    /// so a run that never exercised the rule shows.
    pub lone_rows_seen: BTreeSet<(NodeId, u64, u64)>,
    /// Own gated rows seen the same way.
    pub gated_rows_seen: BTreeSet<(NodeId, u64, u64)>,
    /// Non-vacuity of the gated mark: decisions of a lone delegate's own
    /// op that an uncommitted gated row of its own alone held back from
    /// `Lone` (the lead had room), and `Lone` decisions the store made
    /// `DelegGroup` where it journaled the row (past the lead, or behind
    /// such a gated row). A run that never held a row for the mark never
    /// exercised the hysteresis, whatever the rows say.
    pub held_by_gate: u64,
    pub demoted_at_journal: u64,
    /// The highest prefix each delegate has known committed: the group's
    /// own count can dip while it changes (a requested backup holds
    /// nothing yet), the commit point of a row already journaled does not.
    high_water: BTreeMap<(NodeId, u64), u64>,
}

impl LoneLead {
    /// Judge the rows of `node`'s generation `gen`: `rows` are its
    /// unretired journal rows `(idx, class, rid)`, `committed` the prefix
    /// the delegate knows committed, `lead` the lead in transactions.
    pub fn judge(
        &mut self,
        node: NodeId,
        gen: u64,
        rows: &[(u64, constellation_meta::RowClass, Option<Rid>)],
        committed: u64,
        lead: u64,
    ) {
        let hw = self.high_water.entry((node, gen)).or_default();
        *hw = (*hw).max(committed);
        let committed = *hw;
        let own = |rid: &Option<Rid>| rid.is_some_and(|r| r.node == node);
        let mut open_gated: Option<u64> = None;
        for (idx, class, rid) in rows {
            if !own(rid) {
                continue;
            }
            let key = (node, gen, *idx);
            if class.gated() {
                self.gated_rows_seen.insert(key);
                if *idx > committed && open_gated.is_none() {
                    open_gated = Some(*idx);
                }
            } else if *class == constellation_meta::RowClass::Lone {
                // Judged the once, when it first shows.
                if !self.lone_rows_seen.insert(key) {
                    continue;
                }
                if *idx > committed.saturating_add(lead) && self.past_lead.insert(key) {
                    self.note(format!(
                        "node {node} gen {gen}: own Lone row {idx} is more than the lead \
                         ({lead}) past the committed prefix ({committed})"
                    ));
                }
                if let Some(g) = open_gated {
                    if self.behind_gated.insert(key) {
                        self.note(format!(
                            "node {node} gen {gen}: own Lone row {idx} follows own gated row {g}, \
                             not committed (committed prefix {committed})"
                        ));
                    }
                }
            }
        }
    }

    fn note(&mut self, what: String) {
        if self.examples.len() < 5 {
            self.examples.push(what);
        }
    }
}

impl Oracles {
    pub fn new(bucket: Arc<Bucket>) -> Arc<Self> {
        let leases = Arc::new(LeaseClaims::default());
        {
            let leases = leases.clone();
            bucket.set_lease_hook(Arc::new(move |node, prev, new| {
                leases.landed(node, prev, new)
            }));
        }
        Arc::new(Self {
            leases,
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
            claim_violations: Mutex::new(Vec::new()),
            lone_lead: Mutex::new(LoneLead::default()),
            acks_unsourced: AtomicU64::new(0),
        })
    }

    pub fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::SeqCst)
    }

    /// A client's first submission of `rid` (a resubmission adds nothing).
    pub fn note_invoke(&self, rid: Rid) {
        if self.invoked.lock().unwrap().insert(rid) {
            let t = self.next_tick();
            self.invokes.lock().unwrap().push((rid, t));
        }
    }

    pub fn register(&self, id: NodeId, meta: Arc<Meta>, budget_ms: u64, stable_ms: u64) {
        self.stores.lock().unwrap().insert(id, meta);
        self.budgets
            .lock()
            .unwrap()
            .insert(id, (budget_ms, stable_ms));
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
        self.note_segment(seq, seg);
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
                        g.outcomes.entry(rid).or_insert((seq, i, code));
                    }
                }
                g.max_epoch = g.max_epoch.max(seg.epoch);
            }
            g.prefix = seq;
        }
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

/// What an answer's judgement expected, from the answerer's state (plan §5.1's
/// table), for the per-class counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    Class(AckClass),
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
    pub class: Option<AckClass>,
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
    /// store's table records for it (`DelegGroup`, plan 43 C2; empty: no
    /// record, the group is the delegate and the root). Read from the
    /// answerer's store, not from the core — the delegate counts a backup
    /// only once its own table names it, and the oracle must not take the
    /// core's word for it.
    pub deleg_groups: Vec<(u64, Vec<NodeId>)>,
    /// The generations this node holds as a delegate.
    pub holds_gens: Vec<u64>,
    /// The generations of the delegation table that are designations of
    /// this node (never stolen by the root).
    pub designated_gens: Vec<u64>,
    /// The answerer's wait clock at the answer (ms): a link counts as a
    /// candidate once it has been up for the stability window.
    pub now_wait: i64,
}

impl AnswererState {
    pub fn of(
        core: &constellation_authority::Core,
        meta: &Meta,
        id: NodeId,
        now_wait: i64,
    ) -> Self {
        let view = core.deleg_view();
        Self {
            epoch: core.lease().epoch(),
            epoch_held: core.lease().epoch_held(),
            deleg_groups: recorded_groups(meta, view.mine.iter().map(|m| m.1)),
            holds_gens: view.mine.iter().map(|m| m.1).collect(),
            designated_gens: meta
                .delegation_table()
                .iter()
                .filter(|d| d.node == id && d.designated)
                .map(|d| d.gen)
                .collect(),
            now_wait,
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
}

impl OutcomeKind {
    pub fn name(&self) -> String {
        match self {
            OutcomeKind::Ok => "ok".into(),
            OutcomeKind::Refused(c) => format!("refused:{}", c.posix_name()),
            OutcomeKind::Exists => "exists".into(),
            OutcomeKind::Conflict => "conflict".into(),
            OutcomeKind::LockLapsed => "lock-lapsed".into(),
        }
    }

    /// The outcome record the sequencer journals for this answer: `None`
    /// for a completion (`Completed`), `Some(code)` for a refusal
    /// (`Refused { code }`; an `Exists` hint is `EEXIST`, a conflict
    /// `ESTALE`, a lapsed grant `ENOLCK`: `core::client::LAPSED_CODE`).
    pub fn record_code(&self) -> Option<Code> {
        match self {
            OutcomeKind::Ok => None,
            OutcomeKind::Refused(c) => Some(*c),
            OutcomeKind::Exists => Some(Code::Exists),
            OutcomeKind::Conflict => Some(Code::Stale),
            OutcomeKind::LockLapsed => Some(Code::NoLock),
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
    pub claimed: Option<AckClass>,
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
pub fn check_claim(
    claim: Option<AckClass>,
    committed: Option<bool>,
    judged: Option<AckClass>,
    rows: &[constellation_meta::RowClass],
    delegated: bool,
) -> Option<String> {
    use constellation_meta::RowClass;
    let gated = match (claim, committed) {
        (Some(c), _) => c.gated(),
        (None, Some(c)) => c,
        (None, None) => return None,
    };
    if gated && matches!(judged, Some(AckClass::Lone) | Some(AckClass::NonStealable)) {
        return Some(format!(
            "the answer claims a commit ({}) but only {} judges it",
            claim.map_or("committed".to_string(), |c| c.name().to_string()),
            judged.map_or("none", |c| c.name())
        ));
    }
    if rows.is_empty() || delegated {
        return None;
    }
    let ok = match claim {
        Some(AckClass::DelegGroup { .. }) => true,
        Some(c) => rows.contains(&RowClass::from(c)),
        None => rows.iter().any(|r| r.gated() == gated),
    };
    (!ok).then(|| {
        format!(
            "the answer says {} but the journal row of the rid is tagged {:?}",
            claim.map_or(if gated { "committed" } else { "uncommitted" }, |c| c
                .name()),
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
    pub claimed: Option<AckClass>,
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
/// its budget), every judged answer of the rid. What the core *claims* is
/// not consulted: a core that wrongly claims `Lone` (the class-claim check
/// judges that claim) must not also hide the loss of a committed op.
pub fn lone_answered(acks: &[AckEntry]) -> HashSet<Rid> {
    let mut lone: BTreeMap<Rid, bool> = BTreeMap::new();
    for a in acks.iter().filter(|a| !a.is_relay()) {
        let Some(j) = &a.judged else { continue };
        *lone.entry(a.rid).or_insert(true) &= j.class == Some(AckClass::Lone);
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

    let class = classify(
        o,
        bus,
        answerer,
        st,
        (rid, want),
        lease.as_ref(),
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
    tx: Option<&TxRef>,
    own_txs: &[BackupTx],
    holder_txs: &[BackupTx],
    detail: &mut Vec<String>,
) -> Option<AckClass> {
    // 1. Archive.
    if let Some((seq, _, _)) = o.bucket.archive.outcome_as(rid, want) {
        detail.push(format!("archive: in applied segment {seq}"));
        return Some(AckClass::Archive);
    }
    detail.push("archive: not in an applied unfenced segment".into());

    // 2. RootGroup: the lease object's holder and every listed backup.
    match (lease, tx) {
        (Some(l), Some(t)) if t.origin == (0, 0) && !l.backups.is_empty() => {
            match root_group(o, l, (rid, want), holder_txs) {
                Ok(()) => {
                    detail.push("root-group: holder and every listed backup hold it".into());
                    return Some(AckClass::RootGroup);
                }
                Err(why) => detail.push(format!("root-group: {why}")),
            }
        }
        (Some(l), Some(t)) if t.origin == (0, 0) => detail.push(format!(
            "root-group: the lease at epoch {} (holder {}) lists no backup",
            l.epoch, l.holder
        )),
        (None, _) => detail.push("root-group: no lease object".into()),
        (_, None) => detail.push("root-group: no transaction of the rid".into()),
        _ => detail.push("root-group: not a root row".into()),
    }

    // 3. DelegGroup.
    if let Some(t) = tx {
        let (g, i) = t.origin;
        if g != 0 {
            match deleg_group(o, answerer, st, (rid, want), g, i, t, holder_txs) {
                Ok(why) => {
                    detail.push(format!("deleg-group: {why}"));
                    return Some(AckClass::DelegGroup { gen: g });
                }
                Err(why) => detail.push(format!("deleg-group: {why}")),
            }
        }
    }

    // 4. NonStealable: a designee, or an epoch's hold owner on a root row.
    if let Some(t) = tx {
        let (g, _) = t.origin;
        if g != 0 && st.designated_gens.contains(&g) {
            detail.push(format!("non-stealable: designee of generation {g}"));
            return Some(AckClass::NonStealable);
        }
        if g == 0 && st.epoch_held {
            detail.push("non-stealable: epoch hold owner".into());
            return Some(AckClass::NonStealable);
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
        Some(t) if t.origin.0 != 0 => {
            let backed = st
                .deleg_groups
                .iter()
                .any(|(g, b)| *g == t.origin.0 && !b.is_empty());
            if backed {
                detail.push(format!("lone: generation {} records a backup", t.origin.0));
            }
            !backed
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
    if let Some(n) = peer_in_budget(o, bus, answerer, st.now_wait) {
        detail.push(format!("lone: peer {n} is linked within the budget"));
        return None;
    }
    // (`own_txs` only feeds T; a Lone answer needs no copy elsewhere.)
    let _ = own_txs;
    detail.push("lone: its group is itself and no peer is within the budget".into());
    Some(AckClass::Lone)
}

/// A live node `answerer` is linked to within its RTT budget, if any — a
/// link that has been up for the stability window, as the core's candidate
/// selection requires (`Core::backup_candidates`): a peer that has only just
/// come up is not yet one a backup could be had from, and until it is the
/// node is alone in the plan's sense.
fn peer_in_budget(o: &Oracles, bus: &Bus, answerer: NodeId, now_wait: i64) -> Option<NodeId> {
    let (budget, stable) = o
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
            && up
                .and_then(|m| m.get(n))
                .is_none_or(|since| now_wait - *since >= stable as i64)
    })
}

/// RootGroup: the lease object's holder's journal and every listed
/// backup's tail hold the transaction (`Ok`), or why not.
fn root_group(
    o: &Oracles,
    l: &Lease,
    (rid, want): (Rid, Option<Code>),
    holder_txs: &[BackupTx],
) -> Result<(), String> {
    let Some(ht) = find_tx(holder_txs, rid, want) else {
        return Err(format!(
            "the lease holder {} (epoch {}) has no journal row of the rid",
            l.holder, l.epoch
        ));
    };
    for b in &l.backups {
        let Some(store) = o.store(*b) else {
            return Err(format!("listed backup {b} has no registered store"));
        };
        let mut epochs = vec![l.epoch];
        if let Ok(Some(role)) = store.backup_role() {
            if role.epoch != l.epoch {
                epochs.insert(0, role.epoch);
            }
        }
        let held = epochs.iter().any(|e| {
            store
                .backup_tail(*e)
                .map(|tail| {
                    tail.iter()
                        .any(|t| carries_as(&t.records, rid, want) && t.records == ht.records)
                })
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

/// DelegGroup: the delegate holds `(g, i)`; each recorded backup's delegate
/// tail holds it, or, with none recorded, the root's journal does.
#[allow(clippy::too_many_arguments)]
fn deleg_group(
    o: &Oracles,
    answerer: NodeId,
    st: &AnswererState,
    (rid, want): (Rid, Option<Code>),
    g: u64,
    i: u64,
    t: &TxRef,
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
        Ok(format!(
            "no backup recorded; the root journal holds ({g},{i})"
        ))
    } else {
        Err(format!(
            "no backup recorded and the root's journal lacks the appended row ({g},{i})"
        ))
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
        // Plan 43 C2 (§2.1): a lone delegate — its table records no backup
        // and no peer, the root included, is within the budget — answers its
        // own node's client from its disk; every other answer waits for the
        // group (the root's append, when no backup is recorded).
        let backed = st
            .deleg_groups
            .iter()
            .any(|(gen, b)| *gen == g && !b.is_empty());
        return Expected::Class(if st.designated_gens.contains(&g) {
            AckClass::NonStealable
        } else if rid.node == answerer
            && !backed
            && peer_in_budget(o, bus, answerer, st.now_wait).is_none()
        {
            AckClass::Lone
        } else {
            AckClass::DelegGroup { gen: g }
        });
    }
    if st.epoch_held {
        return Expected::Class(AckClass::NonStealable);
    }
    let Some(l) = lease else {
        return Expected::Class(AckClass::Archive);
    };
    if l.ack_policy == AckPolicy::S3 {
        return Expected::Class(AckClass::Archive);
    }
    if !l.backups.is_empty() {
        return Expected::Class(AckClass::RootGroup);
    }
    if peer_in_budget(o, bus, answerer, st.now_wait).is_some() {
        // The bring-up rule: while a backup could be had, wait for the
        // archive.
        return Expected::Class(AckClass::Archive);
    }
    if rid.node == answerer {
        Expected::Class(AckClass::Lone)
    } else {
        // Plan 43 C2's rule: a lone root answers another node's forward
        // from the archive (today such an answer is a violation).
        Expected::Class(AckClass::Archive)
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
                if mine.queued_gated.contains(&rid) || c.queued_gated.contains(&rid) {
                    self.violations.insert(rid, text);
                } else {
                    self.ungated.entry(rid).or_insert(text);
                }
                break;
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
    pub unjournaled_refusals: BTreeMap<String, usize>,
    /// Plan 43 C2a, I1: answers whose rid a transaction carries with
    /// another outcome (by answer kind), and relays with no judged answer
    /// of the sequencer they name behind them.
    pub mismatched_acks: BTreeMap<String, usize>,
    pub unverified_relays: usize,
    /// Answers whose claimed class (`AnswerSource::Sequenced`) is not the
    /// class the judgement found (informational: the first class that
    /// holds may be stronger than the one the answer waited for).
    pub class_mismatch: usize,
    /// Plan 43 C2a, D6 of C0: an unjournaled refusal answered to a client
    /// whose rid the log later completes (a failure under `strict_settle`).
    pub unjournaled_landed: Vec<String>,
    /// I1: the descriptions, in emission order.
    pub i1: Vec<String>,
    /// Plan 43 C1-2: answers whose class claim is not borne out
    /// (`strict_commit` fails the run on one).
    pub class_claim: Vec<String>,
    /// I2 (b): one description per rid queued on two nodes (a copy standing
    /// for a committed answer).
    pub i2: Vec<String>,
    /// I2 (b) where no copy stands for a committed answer (counted).
    pub i2_ungated: Vec<String>,
    /// I2 (c): one description per rid not completed verbatim.
    pub i2c: Vec<String>,
    pub i4: Vec<String>,
    pub i5: Vec<String>,
    /// I6 (plan 43 C7b): TTL claims of a live lease.
    pub i6: Vec<String>,
}

impl Findings {
    /// The first five violations of each oracle.
    pub fn examples(&self) -> Vec<String> {
        [
            &self.i1,
            &self.class_claim,
            &self.i2,
            &self.i2c,
            &self.i4,
            &self.unjournaled_landed,
            &self.i5,
            &self.i6,
        ]
        .into_iter()
        .flat_map(|v| v.iter().take(5).cloned())
        .collect()
    }
}

/// Tally the run's answers and run the end-of-run oracles: I2 (c) over the
/// answers' transactions and the log, I4 over the in-doubt answers and the
/// invocations, I5 over the rolled-back set. `converged`: the run reached
/// quiescence, so a group-answered rid the log never completed is absent
/// for good.
pub fn end_of_run(
    o: &Oracles,
    log: &super::check::Oracle,
    in_doubt: &[(NodeId, Rid, u64, u64)],
    rolled_back: &HashSet<Rid>,
    converged: bool,
) -> Findings {
    let mut f = Findings {
        i1: o.i1_violations.lock().unwrap().clone(),
        class_claim: o.claim_violations.lock().unwrap().clone(),
        i6: o.leases.violations.lock().unwrap().clone(),
        ..Findings::default()
    };
    {
        // (One guard: two in the literal above would deadlock.)
        let ledger = o.identity.lock().unwrap();
        f.i2 = ledger.violations.values().cloned().collect();
        f.i2_ungated = ledger.ungated.values().cloned().collect();
    }
    let acks = o.acks.lock().unwrap();
    let mut non_lone_ok: BTreeSet<Rid> = BTreeSet::new();
    let mut verbatim: BTreeMap<Rid, String> = BTreeMap::new();
    for a in acks.iter().filter(|a| !a.is_relay()) {
        let Some(j) = &a.judged else { continue };
        let class = j.class.map_or("none", |c| c.name());
        *f.acks_by_class.entry(class.to_string()).or_default() += 1;
        if j.mismatch.is_some() {
            *f.mismatched_acks.entry(a.kind.name()).or_default() += 1;
        }
        if a.claimed
            .is_some_and(|c| Some(c.index()) != j.class.map(|c| c.index()))
        {
            f.class_mismatch += 1;
        }
        if j.class.is_none() {
            if j.expected == Expected::Unjournaled && a.kind.is_refusal() {
                *f.unjournaled_refusals
                    .entry(a.kind.refusal_name())
                    .or_default() += 1;
                // `EROFS` (a frozen epoch's refusal) is not final: the
                // caller retries the op, the sim's client under the same
                // rid, and the op then runs.
                let retried = a.kind == OutcomeKind::Refused(Code::ReadOnly);
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
        if a.kind == OutcomeKind::Ok && j.class != Some(AckClass::Lone) {
            non_lone_ok.insert(a.rid);
        }
        // I2 (c): an OK answered under a group class names a transaction
        // the log must carry as it was committed.
        if a.kind == OutcomeKind::Ok
            && matches!(
                j.class,
                Some(AckClass::RootGroup) | Some(AckClass::DelegGroup { .. })
            )
        {
            if let Some(t) = &j.tx {
                if let Some(why) = not_verbatim(log, a.rid, t, converged) {
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
    // I5: acknowledged OK, rolled back, and not a Lone answer.
    for rid in rolled_back {
        if non_lone_ok.contains(rid) {
            let Some(a) = acks
                .iter()
                .rev()
                .find(|a| a.rid == *rid && a.judged.is_some() && a.kind == OutcomeKind::Ok)
            else {
                continue;
            };
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
    f
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
        assert_eq!(j.class, Some(AckClass::Archive), "{}", j.detail);
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
        assert_eq!(j.class, Some(AckClass::Archive), "{}", j.detail);
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
        assert_eq!(j.class, Some(AckClass::Lone), "{}", j.detail);
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
        let o = Oracles::new(Bucket::new(1, (0, 0)));
        *o.acks.lock().unwrap() = acks;
        end_of_run(
            &o,
            &log_with(seg(1, &[]), completed),
            &[],
            &HashSet::new(),
            true,
        )
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
                class: Some(AckClass::Lone),
                expected: Expected::Class(AckClass::Lone),
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
        let judged = |class: Option<AckClass>| Judgement {
            class,
            expected: Expected::Class(AckClass::RootGroup),
            tx: None,
            detail: String::new(),
            mismatch: None,
        };
        let (a, b, c, d) = (rid(1, 1), rid(1, 2), rid(1, 3), rid(1, 4));
        let acks = vec![
            // Claimed and judged `Lone`: excused.
            AckEntry {
                claimed: Some(AckClass::Lone),
                ..ack(1, a, 1, OutcomeKind::Ok, false)
            },
            // Claimed `Lone`, judged a group's: not excused.
            AckEntry {
                claimed: Some(AckClass::Lone),
                judged: Some(judged(Some(AckClass::RootGroup))),
                ..ack(1, b, 2, OutcomeKind::Ok, false)
            },
            // Claimed `Lone`, no class held: not excused.
            AckEntry {
                claimed: Some(AckClass::Lone),
                judged: Some(judged(None)),
                ..ack(1, c, 3, OutcomeKind::Ok, false)
            },
            // Judged `Lone` once, a group's at a second answer: not excused.
            AckEntry {
                claimed: Some(AckClass::Lone),
                ..ack(1, d, 4, OutcomeKind::Ok, false)
            },
            AckEntry {
                claimed: Some(AckClass::Lone),
                judged: Some(judged(Some(AckClass::RootGroup))),
                ..ack(1, d, 5, OutcomeKind::Ok, false)
            },
        ];
        assert_eq!(lone_answered(&acks), HashSet::from([a]));
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
        let t = treat(Some(AnswerSource::Sequenced(AckClass::Lone)));
        assert!(t.judge && t.relay_from.is_none());
        assert_eq!(t.claimed, Some(AckClass::Lone));
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
        assert_eq!(j.expected, Expected::Class(AckClass::RootGroup));
        // The backup holds it (as the row was shipped to it).
        w.metas[&2]
            .backup_append(1, std::slice::from_ref(&tx))
            .unwrap();
        let j = w.judge(1, &st, r);
        assert_eq!(j.class, Some(AckClass::RootGroup), "{}", j.detail);
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
        assert_eq!(w.judge(1, &st, r).class, Some(AckClass::Archive));
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
        assert_eq!(j.expected, Expected::Class(AckClass::DelegGroup { gen: 1 }));
        // The root appends the row (origin (1, 1)) to its journal.
        w.metas[&1]
            .apply_delegate_tx(&records, Some(r), 1, 1, Position::ZERO)
            .unwrap();
        let j = w.judge(2, &st, r);
        assert_eq!(
            j.class,
            Some(AckClass::DelegGroup { gen: 1 }),
            "{}",
            j.detail
        );
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
        assert_eq!(
            j.class,
            Some(AckClass::DelegGroup { gen: 1 }),
            "{}",
            j.detail
        );
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
                &[LogRecord::DelegGroup {
                    dir: ROOT_INO,
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
        assert_eq!(
            j.class,
            Some(AckClass::DelegGroup { gen: 1 }),
            "{}",
            j.detail
        );
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
        assert_eq!(j.class, Some(AckClass::Lone), "{}", j.detail);
        // Another node's client is not answered Lone.
        let remote = rid(3, 1);
        w.create(1, remote, "b");
        let j = w.judge(1, &st, remote);
        assert_eq!(j.class, None, "{}", j.detail);
        assert_eq!(j.expected, Expected::Class(AckClass::Archive));
        // A peer linked within the budget (the node could have had a backup).
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        w.bus.attach(2, tx);
        w.bus.set_rtt(1, 2, 5);
        let j = w.judge(1, &st, own);
        assert_eq!(j.class, None, "{}", j.detail);
        assert!(j.detail.contains("peer 2 is linked"), "{}", j.detail);
        // ... unless its link is too young to be a candidate (the stability
        // window), or it is out of budget, or the link is cut.
        w.o.note_directory(1, [(2u64, 9_990i64)].into());
        w.o.budgets.lock().unwrap().insert(1, (50, 200));
        let young = AnswererState {
            now_wait: 10_000,
            ..holder_state(1)
        };
        assert_eq!(w.judge(1, &young, own).class, Some(AckClass::Lone));
        w.o.budgets.lock().unwrap().insert(1, (50, 0));
        w.bus.set_rtt(1, 2, 120);
        assert_eq!(w.judge(1, &st, own).class, Some(AckClass::Lone));
        w.bus.set_rtt(1, 2, 5);
        w.bus.set_partition(1, 2, true);
        assert_eq!(w.judge(1, &st, own).class, Some(AckClass::Lone));
        w.bus.set_partition(1, 2, false);
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
        assert_eq!(j.class, Some(AckClass::NonStealable), "{}", j.detail);
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
            segments: 1,
            fenced: 0,
            unfenced,
            describe: Vec::new(),
        }
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
}
