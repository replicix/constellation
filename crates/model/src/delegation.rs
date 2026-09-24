//! Plan 30 §M11: delegated sub-sequencers over one log — the
//! `Delegation` model.
//!
//! # Why a model of its own
//!
//! The authority model ([`crate::protocol`]) checks one sequencer: the
//! lease, the log, forwarding, stranding, replay and positions. M11 adds
//! a second kind of executor — a *delegate* that sequences one subtree at
//! local speed and streams its records to the root, which appends them
//! without re-validating — and every new risk is about *two* executors
//! disagreeing on who owns a key at an instant: a stale delegation
//! honoured past its recall, a recall that does not drain, a stream
//! appended under the wrong generation, a record whose causal
//! predecessors are missing from the replica it is read on. Whether the
//! time-bounded part is safe depends on clocks, which the authority model
//! leaves out. So, like M8's [`crate::cto`], M9's `backup` and M10's
//! `flex`, this is a focused `stateright::Model` over the same
//! abstractions, with a namespace just rich enough to have subtrees.
//!
//! # What is modeled
//!
//! - **Namespace.** A fixed directory tree ([`DelegModel::parents`]: dir 0
//!   is the root directory, e.g. `[NONE, 0, 0, 1]` for `/D1`, `/D2`,
//!   `/D1/D3`) with [`NAMES`] names per directory. A key is `(dir, name)`
//!   ([`key`]); its state is one presence bit. Ops: `Create`, `Unlink`
//!   (single key) and `Move(src, dst)` (two keys: `ENOENT` when the source
//!   is absent, else the source is removed and the destination set — the
//!   shape of a rename across subtrees), plus reads.
//! - **The log.** One sequence of [`Entry`]s: takeover markers,
//!   `Delegate{dir,node,gen}` / `Recall{dir,gen}` records (the replicated
//!   delegation table), and writes. Every entry carries its
//!   [`Origin`]: the *stream* it came from (the root's journal in epoch
//!   `e`, or delegation generation `g`) and its index in that stream — the
//!   real segment envelope's `rows` (M9) generalized to delegate batches —
//!   and its `deps` ([`Pos`]).
//! - **Ownership.** A key belongs to the innermost live delegation whose
//!   directory is an ancestor of (or is) the key's directory, else to the
//!   root; an op whose keys have two owners, or one delegated key and one
//!   root-owned key, is *cross-subtree* and goes to the root. Every node
//!   resolves ownership from the table in *its own applied prefix* (a
//!   stale view earns a `NotOwner` reply and a retry once the view
//!   changes).
//! - **Delegate execution.** A delegate validates against its replica
//!   (applied prefix plus its own unappended records — authoritative for
//!   its subtree, because every write there goes through it), keeps the
//!   record as speculation, streams it to the root as
//!   `Stream{gen, idx, rid, op, deps}`, and acknowledges. Under a backup
//!   ([`DelegModel::backups`]) it acknowledges only after the backup
//!   persisted the record (M9's `Backup` policy, per delegate).
//! - **The root's append.** Stream records are appended in index order
//!   once the generation is live (not ended) — the *generation check* —
//!   and, as an assertion, once their `deps` are in. The root re-validates
//!   nothing.
//! - **`deps`.** A requester's `observed` position is a small vector
//!   clock: the log length it has applied plus, per stream, the highest
//!   index an acknowledgement carried (M6's `Position { seq, pending }`
//!   with `pending` per stream instead of per tenure). A forward carries
//!   it as `deps`. A delegate executes an op only once its replica
//!   satisfies the op's `deps` (its own stream is trivially satisfied);
//!   the root executes an op it owns only once its appended cursors
//!   satisfy them. That is the causal-cut rule: a replica never contains
//!   a record whose `deps` it lacks, and a reader never sees a marker
//!   without the data it was written after.
//! - **Recall.** The root recalls a delegation before executing anything
//!   that touches its subtree: `Recall{gen}` to the delegate, which stops
//!   acknowledging, and answers `Recalled{gen, through}`; the root ends
//!   the generation (a `Recall` record) once its cursor reached
//!   `through`, then executes. An unreachable delegate is *outwaited*:
//!   the root ends the generation once its own clock passes the grant's
//!   `until` (`granted + ttl + root_margin`); the delegate honours a
//!   grant only until `sent + ttl − deleg_margin` in its clock, measured
//!   from when it sent the renewal request (M8's discipline). Records
//!   the root did not append before ending the generation are refused
//!   from then on; the delegate retracts them when it tails the `Recall`,
//!   and their requesters (Layer A: they keep what they were acknowledged)
//!   replay them by rid through the new owner, deduplicated by the log.
//! - **Root failover.** The root lease is a register with expiry in the
//!   holder's clock; a takeover at `local ≥ expires` tails to head,
//!   appends a marker and learns the delegation table from the log. The
//!   old root's unshipped journal (when [`DelegModel::journal`] is on)
//!   is stranded like any holder's (M3b); delegates re-stream their
//!   unretired records to the new root. Grants are capped by the granting
//!   root's lease (`cap_by_lease`), so at a takeover every inherited
//!   grant has expired in the delegate's clock and the new root may
//!   recall at once; without the cap it must wait the horizon
//!   (`takeover_horizon`). M9's seal-based takeover is the same
//!   transition with an earlier trigger and is not modeled separately.
//! - **Backups and drains.** A delegate's backup keeps the records it
//!   was sent; it may *seal* (answering every later append `sealed`) and
//!   send its tail to the root as `Drain{gen, records}`, which the root
//!   appends (they were validated by the delegate, and the seal
//!   guarantees no later acknowledgement) before ending the generation.
//! - **Time and faults.** A global real time `t` ([`Action::Tick`]),
//!   per-node clocks `t + off[i]` with `|off| ≤ max_offset` that may
//!   step ([`Action::Jump`]); a lossy reordering network; fail-stop
//!   crashes.
//!
//! # Model action → code (phase 2; nothing exists yet)
//!
//! | Model | Code |
//! |---|---|
//! | `Start`/`RetryWrite` resolution | `Core::submit` → `delegation::owner_of(keys)` (an ancestor walk over the replica) → local delegate execution, `Send(MutateRequest{deps})`, or the root |
//! | `Deliver(Fwd)` at a delegate | a per-subtree sequencer slot inside the core: validate on `Replica`, `install_delegate_spec`, `Send(DelegateStream)`, ack (parked for a backup) |
//! | `Deliver(Stream)` at the root | `Core::on_delegate_stream`: gen + ownership check, `apply_records_journaled` in stream order, no validation |
//! | `Deliver(Fwd)` at the root, cross-subtree | `recall_needed` generalized to write delegations → `Recall{dir,gen}`, `park_reply` until `Recalled` or the timer |
//! | `RecallTimeout` | `Timer::DelegationExpiry` → `end_generation` (a `Recall` record journaled) |
//! | `Deliver(Recall)` at a delegate | stop, `DelegateStream` drain, `Recalled{through}` |
//! | `Tail` of `Recall`/`Delegate`/marker | `Replica::apply_segment`: retire delegate speculation by `(gen, idx)`, retract past the cut, queue replays by rid |
//! | `RenewReq`/`Renewed` | the read-delegation renewal path, capped by the root lease |
//! | `Seal`/`Drain` | M9's backup seal, addressed to the root instead of the lease |
//! | `Takeover` | the M5 acquisition; the table is read from the replica |
//!
//! # Properties
//!
//! - `per_key_linearizable` (always): each key's history — `Create`,
//!   `Unlink`, and a `Move`'s two halves — is linearizable against a
//!   boolean register, with M13 round 3a's treatment of tentative
//!   acknowledgements (an op acknowledged by an executor whose generation
//!   or tenure ended without the record in the log is fed as still in
//!   flight on a thread of its own).
//! - `causal_cut` (always): every live replica (applied prefix, a
//!   delegate's speculation, the root's journal) satisfies the `deps` of
//!   every record it contains.
//! - `marker_order` (always): a `ReadPair{marker, data}` never sees the
//!   marker without the data, when the data's write was acknowledged and
//!   not retracted.
//! - `recall_safety` (always): no delegate acknowledges under a
//!   generation the root has ended.
//! - `log_records_valid` (always): every write in the log satisfied its
//!   precondition at its position — what "append without re-validating"
//!   relies on.
//! - `exactly_once` (always): no rid appears twice in the log.
//! - `converged_at_quiescence`, `read_your_writes`,
//!   `stable_without_faults` (no acknowledgement is retracted in a run
//!   with no crash, no drop, no clock step and no timeout — a recall must
//!   drain) and `durable_acks_stand` (M9's `acked_never_lost` per
//!   delegate: a backup-acknowledged op is in the log or held by a live
//!   node; a single crash never loses it).
//!
//! # Rules the model found (beyond the plan's list)
//!
//! Each was a violation in a design configuration before it became a
//! rule; `tests/delegation.rs` keeps the configurations:
//! 1. the delegate waits for `deps` too (its own readers see its
//!    speculation), not only the root;
//! 2. a dependency on an ended stream past its cut is void, and an
//!    executor *normalizes* the `deps` it records ([`State::normalize_deps`]);
//! 3. an acknowledgement from a stream that ended past its cut is
//!    tentative wherever it is — in a requester's memory, in flight, or
//!    arriving after the requester tailed the `Recall`;
//! 4. a generation with a live backup ends through the backup's seal
//!    and drain, never by TTL alone, and no renewal is granted once a
//!    recall or a reclaim began;
//! 5. a deferred (backup-gated) acknowledgement re-checks the grant;
//! 6. the root reclaims an expired, unrenewed grant on its own;
//! 7. the initial grant is capped like every renewal, which makes every
//!    inherited grant dead at a takeover;
//! 8. an outwaited recall and an unrenewed root lease count as faults
//!    for `stable_without_faults`;
//! 9. `durable_acks_stand` is "never lost" (M9's shape), not "never
//!    retracted".
//!
//! # The drift margin
//!
//! Exactly M8's: with every clock within `D` of real time, a delegate
//! honours a grant until real time `≤ s + ttl − deleg_margin + 2D` and
//! the root outwaits it from `≥ g + ttl + root_margin − 2D` with `g ≥
//! s`; safe when `deleg_margin + root_margin > 4D`. The cap by the root
//! lease (`ttl ≤ expires − lease_margin − now`) makes every inherited
//! grant dead by the time a successor can take over when `lease_margin +
//! deleg_margin > 4D`. With all margins equal to the lease's `M`, both
//! are the lease's own `M > 2D`.

use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use stateright::{Model, Property};

/// "No node" / "no directory".
pub const NONE: u8 = u8::MAX;
/// Names per directory.
pub const NAMES: u8 = 2;
/// Stream ids: root epochs are `1..GEN_BASE`, generation `g` is
/// `GEN_BASE + g`.
pub const GEN_BASE: u8 = 8;
/// Generations `1..MAX_GENS`.
pub const MAX_GENS: usize = 6;

pub type Key = u8;

pub const fn key(dir: u8, name: u8) -> Key {
    dir * NAMES + name
}

pub const fn dir_of(k: Key) -> u8 {
    k / NAMES
}

pub const fn gen_stream(gen: u8) -> u8 {
    GEN_BASE + gen
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WriteOp {
    Create(Key),
    Unlink(Key),
    Move(Key, Key),
}

impl WriteOp {
    pub fn keys(self) -> Vec<Key> {
        match self {
            WriteOp::Create(k) | WriteOp::Unlink(k) => vec![k],
            WriteOp::Move(a, b) => vec![a, b],
        }
    }
}

/// One step of a node's client script.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Write(WriteOp),
    /// A lookup of one key on this node's replica (after the M6 session
    /// wait when `session_wait` is on).
    Read(Key),
    /// A causal probe: both keys read from one replica snapshot, no
    /// session wait (a fresh reader). Violates `marker_order` when the
    /// marker is present, the data absent, and the data's write was
    /// acknowledged and not retracted.
    ReadPair {
        marker: Key,
        data: Key,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rid {
    pub node: u8,
    pub seq: u8,
}

/// Where a log entry came from: `stream` is a root epoch or
/// `gen_stream(gen)`; `idx` its index in that stream (1-based).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Origin {
    pub stream: u8,
    pub idx: u8,
}

/// A position: the log length, plus per stream the highest index
/// observed (M6's `Position` with a per-stream pending part). Sorted by
/// stream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pos {
    pub seq: u8,
    pub pend: Vec<(u8, u8)>,
}

impl Pos {
    pub fn join(&self, o: &Pos) -> Pos {
        let mut p = self.clone();
        p.seq = p.seq.max(o.seq);
        for &(s, i) in &o.pend {
            p.raise(s, i);
        }
        p
    }

    pub fn raise(&mut self, stream: u8, idx: u8) {
        match self.pend.iter_mut().find(|(s, _)| *s == stream) {
            Some(e) => e.1 = e.1.max(idx),
            None => {
                self.pend.push((stream, idx));
                self.pend.sort();
            }
        }
    }

    pub fn dominates(&self, o: &Pos) -> bool {
        self.seq >= o.seq
            && o.pend
                .iter()
                .all(|&(s, i)| self.pend.iter().any(|&(s2, i2)| s2 == s && i2 >= i))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Rec {
    /// The takeover marker of the epoch in `Origin::stream`.
    Marker,
    Delegate {
        dir: u8,
        node: u8,
        gen: u8,
    },
    Recall {
        dir: u8,
        gen: u8,
    },
    Write {
        rid: Rid,
        op: WriteOp,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub origin: Origin,
    pub rec: Rec,
    pub deps: Pos,
}

// ------------------------------------------------------------ per-key spec

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyOp {
    Create,
    Unlink,
    /// A `Move`'s destination half: set present, always `Ok`.
    Put,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Ret {
    Ok,
    Eexist,
    Enoent,
    /// Acknowledged by an executor whose generation or tenure ended with
    /// the record not in the log (M13 round 3a): in flight forever for
    /// the checker, its replay by rid may land it later.
    Tentative,
    /// A tentative op whose replay was refused (a conflict copy).
    Conflicted,
    /// A `Move`'s destination half when the move returned `ENOENT`: no
    /// operation on that key happened.
    Skipped,
}

#[derive(Clone, Debug, Default)]
pub struct KeySpec {
    pub present: bool,
}

impl SequentialSpec for KeySpec {
    type Op = KeyOp;
    type Ret = Ret;

    fn invoke(&mut self, op: &KeyOp) -> Ret {
        match op {
            KeyOp::Create => {
                if self.present {
                    Ret::Eexist
                } else {
                    self.present = true;
                    Ret::Ok
                }
            }
            KeyOp::Unlink => {
                if self.present {
                    self.present = false;
                    Ret::Ok
                } else {
                    Ret::Enoent
                }
            }
            KeyOp::Put => {
                self.present = true;
                Ret::Ok
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HEvt {
    Invoke {
        node: u8,
        key: Key,
        op: KeyOp,
        rid: Rid,
    },
    Return {
        node: u8,
        key: Key,
        ret: Ret,
        rid: Rid,
    },
}

/// Presence bits over all keys.
pub type View = u16;

pub fn present(v: View, k: Key) -> bool {
    v & (1 << k) != 0
}

fn set(v: View, k: Key, on: bool) -> View {
    if on {
        v | (1 << k)
    } else {
        v & !(1 << k)
    }
}

/// Validate and apply `op` (the sequencer's `execute`).
pub fn eval(v: View, op: WriteOp) -> (Ret, View) {
    match op {
        WriteOp::Create(k) => {
            if present(v, k) {
                (Ret::Eexist, v)
            } else {
                (Ret::Ok, set(v, k, true))
            }
        }
        WriteOp::Unlink(k) => {
            if present(v, k) {
                (Ret::Ok, set(v, k, false))
            } else {
                (Ret::Enoent, v)
            }
        }
        WriteOp::Move(a, b) => {
            if present(v, a) {
                (Ret::Ok, set(set(v, a, false), b, true))
            } else {
                (Ret::Enoent, v)
            }
        }
    }
}

/// Apply a logged write unconditionally (log replay).
pub fn force(v: View, op: WriteOp) -> View {
    match op {
        WriteOp::Create(k) => set(v, k, true),
        WriteOp::Unlink(k) => set(v, k, false),
        WriteOp::Move(a, b) => set(set(v, a, false), b, true),
    }
}

fn fold(mut v: View, entries: &[Entry]) -> View {
    for e in entries {
        if let Rec::Write { op, .. } = e.rec {
            v = force(v, op);
        }
    }
    v
}

// --------------------------------------------------------- the delegation table

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Deleg {
    pub dir: u8,
    pub node: u8,
    pub gen: u8,
}

/// The live delegations after `entries` (a `Recall` ends its generation).
pub fn live_table(entries: &[Entry]) -> Vec<Deleg> {
    let mut t: Vec<Deleg> = Vec::new();
    for e in entries {
        match e.rec {
            Rec::Delegate { dir, node, gen } => t.push(Deleg { dir, node, gen }),
            Rec::Recall { gen, .. } => t.retain(|d| d.gen != gen),
            _ => {}
        }
    }
    t
}

/// The delegation owning `dir`: the innermost live delegation on `dir` or
/// an ancestor (the ancestor walk).
pub fn owner_of_dir(table: &[Deleg], parents: &[u8], mut dir: u8) -> Option<Deleg> {
    loop {
        if let Some(d) = table.iter().find(|d| d.dir == dir) {
            return Some(*d);
        }
        if dir == 0 || parents[dir as usize] == NONE {
            return None;
        }
        dir = parents[dir as usize];
    }
}

/// Who executes `op`: one delegation (every key under it), or the root
/// (`None`: a root-owned key, or keys under two owners).
pub fn owner_of_op(table: &[Deleg], parents: &[u8], op: WriteOp) -> Option<Deleg> {
    let mut owner: Option<Option<Deleg>> = None;
    for k in op.keys() {
        let o = owner_of_dir(table, parents, dir_of(k));
        match owner {
            None => owner = Some(o),
            Some(prev) if prev == o => {}
            Some(_) => return None,
        }
    }
    owner.flatten()
}

/// The live delegations `op`'s keys fall under (the recall set of a
/// cross-subtree op).
pub fn delegations_touched(table: &[Deleg], parents: &[u8], op: WriteOp) -> Vec<Deleg> {
    let mut v: Vec<Deleg> = Vec::new();
    for k in op.keys() {
        if let Some(d) = owner_of_dir(table, parents, dir_of(k)) {
            if !v.contains(&d) {
                v.push(d);
            }
        }
    }
    v
}

// ------------------------------------------------------------------ config

#[derive(Clone, Debug)]
pub struct DelegModel {
    /// `parents[dir]`; `parents[0] == NONE`.
    pub parents: Vec<u8>,
    pub scripts: Vec<Vec<Op>>,
    /// Keys present at the start.
    pub initial_present: Vec<Key>,
    /// `(dir, node)`: delegations in the initial log, generations `1..`,
    /// renewed at `t = 0`.
    pub initial_delegations: Vec<(u8, u8)>,
    /// `(delegate, backup)`: the delegate acknowledges only after the
    /// backup persisted the record.
    pub backups: Vec<(u8, u8)>,
    /// `(dir, node)` the root may delegate (or re-delegate) at any time.
    pub delegatable: Vec<(u8, u8)>,
    // ---- the rules, each a knob so the checker shows what it prevents ----
    /// The root executes an op it owns only once its `deps` are appended.
    pub deps_at_root: bool,
    /// A delegate executes an op only once its replica satisfies `deps`.
    pub deps_at_delegate: bool,
    /// The root waits for the delegate's drain (or the timeout) before
    /// ending the generation and executing.
    pub drain_before_execute: bool,
    /// The root refuses stream records of an ended generation.
    pub gen_check: bool,
    /// A grant's ttl is capped by the root lease's usable end.
    pub cap_by_lease: bool,
    /// A new root waits `deleg_ttl + root_margin` before outwaiting an
    /// inherited grant (needed only without the cap).
    pub takeover_horizon: bool,
    /// Reads wait for the M6 session position.
    pub session_wait: bool,
    /// The root's appends go to a journal shipped by `Ship` (else straight
    /// to the log).
    pub journal: bool,
    pub deleg_margin: i16,
    pub root_margin: i16,
    pub lease_margin: i16,
    pub deleg_ttl: i16,
    pub lease_ttl: i16,
    /// Delegates renew (`RenewReq`) when their grant is about to expire.
    pub allow_renew: bool,
    /// The root ends a generation whose grant expired unrenewed, with no
    /// recall pending (a crashed or partitioned delegate).
    pub reclaim_expired: bool,
    /// A backup may seal only once its delegate crashed (`false`: any
    /// time, M9's nondeterministic silence detection).
    pub seal_only_after_crash: bool,
    pub max_offset: i16,
    /// The initial clock offsets (default all zero).
    pub init_offsets: Option<Vec<i16>>,
    pub max_jumps: u8,
    pub max_drops: u8,
    pub max_crashes: u8,
    pub max_tick: u8,
    pub max_epoch: u8,
    pub max_history: usize,
}

impl DelegModel {
    /// The design, with honest clocks: node 0 roots, the rules on.
    pub fn design(parents: Vec<u8>, scripts: Vec<Vec<Op>>) -> Self {
        DelegModel {
            parents,
            scripts,
            initial_present: Vec::new(),
            initial_delegations: Vec::new(),
            backups: Vec::new(),
            delegatable: Vec::new(),
            deps_at_root: true,
            deps_at_delegate: true,
            drain_before_execute: true,
            gen_check: true,
            cap_by_lease: true,
            takeover_horizon: false,
            session_wait: true,
            journal: false,
            deleg_margin: 1,
            root_margin: 1,
            lease_margin: 1,
            deleg_ttl: 4,
            lease_ttl: 30,
            allow_renew: false,
            reclaim_expired: false,
            seal_only_after_crash: true,
            max_offset: 0,
            init_offsets: None,
            max_jumps: 0,
            max_drops: 0,
            max_crashes: 0,
            max_tick: 6,
            max_epoch: 1,
            max_history: 40,
        }
    }

    fn n(&self) -> usize {
        self.scripts.len()
    }
}

// ------------------------------------------------------------------- state

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    pub expires: i16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecallPhase {
    None,
    Sent,
    /// `Recalled{through}` received.
    Drained(u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GenState {
    pub live: bool,
    pub ended: bool,
    pub dir: u8,
    pub node: u8,
    /// Appended through this stream index.
    pub cursor: u8,
    /// Outwait the delegate once the root's clock reaches this.
    pub until: i16,
    pub recall: RecallPhase,
    /// The root asked the delegate's backup to seal and drain.
    pub seal_asked: bool,
}

impl GenState {
    const DEAD: GenState = GenState {
        live: false,
        ended: false,
        dir: NONE,
        node: NONE,
        cursor: 0,
        until: i16::MIN,
        recall: RecallPhase::None,
        seal_asked: false,
    };
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamRec {
    pub gen: u8,
    pub idx: u8,
    pub rid: Rid,
    pub op: WriteOp,
    pub deps: Pos,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HeldOp {
    pub from: u8,
    pub rid: Rid,
    pub op: WriteOp,
    pub deps: Pos,
    pub replay: bool,
    pub waiting: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Root {
    pub epoch: u8,
    pub expires: i16,
    pub journal: Vec<Entry>,
    pub jseq: u8,
    pub gens: Vec<GenState>,
    /// Stream records ahead of their generation's cursor.
    pub buffer: Vec<StreamRec>,
    pub held: Vec<HeldOp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Parked {
    pub from: u8,
    pub rid: Rid,
    pub op: WriteOp,
    pub deps: Pos,
    pub replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Awaiting {
    pub idx: u8,
    pub from: u8,
    pub rid: Rid,
    pub replay: bool,
    pub ret: Ret,
    pub pos: Pos,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DelegState {
    pub dir: u8,
    pub gen: u8,
    /// Honoured while the delegate's clock is below this.
    pub until: i16,
    pub stopped: bool,
    /// Executed, not yet retired by the log (in index order).
    pub spec: Vec<StreamRec>,
    pub next_idx: u8,
    /// The root epoch the speculation was last streamed to.
    pub streamed_epoch: u8,
    pub parked: Vec<Parked>,
    /// Acknowledgements waiting for the backup.
    pub awaiting: Vec<Awaiting>,
    /// The delegate's clock when it sent its pending `RenewReq`.
    pub renew_sent: Option<i16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Acked {
    pub rid: Rid,
    pub op: WriteOp,
    pub origin: Origin,
    pub durable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NodeOp {
    Idle,
    Writing {
        rid: Rid,
        op: WriteOp,
        /// Where it was sent (`NONE`: waiting locally), the owner
        /// generation the view named, and the applied prefix then.
        to: u8,
        gen: u8,
        applied_at: u8,
    },
    ReadStart {
        key: Key,
    },
    ReadPair {
        marker: Key,
        data: Key,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub alive: bool,
    pub applied: u8,
    pub epoch_seen: u8,
    pub pc: u8,
    pub next_seq: u8,
    pub op: NodeOp,
    pub observed: Pos,
    pub acked: Vec<Acked>,
    pub replays: Vec<(Rid, WriteOp)>,
    pub replay_inflight: bool,
    /// Read-your-writes ghost: this node's own last effective write per
    /// key.
    pub own_last: Vec<Option<bool>>,
    pub root: Option<Root>,
    pub deleg: Option<DelegState>,
    /// Backup side.
    pub tail: Vec<StreamRec>,
    pub sealed: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Fwd {
        from: u8,
        to: u8,
        rid: Rid,
        op: WriteOp,
        deps: Pos,
        replay: bool,
    },
    Ack {
        to: u8,
        rid: Rid,
        ret: Ret,
        pos: Pos,
        origin: Option<Origin>,
        replay: bool,
        durable: bool,
    },
    NotOwner {
        to: u8,
        rid: Rid,
        replay: bool,
    },
    Stream {
        to: u8,
        rec: StreamRec,
    },
    RenewReq {
        from: u8,
        to: u8,
        gen: u8,
    },
    Renewed {
        to: u8,
        gen: u8,
        ttl: i16,
    },
    Recall {
        to: u8,
        gen: u8,
    },
    Recalled {
        to: u8,
        gen: u8,
        through: u8,
    },
    Append {
        from: u8,
        to: u8,
        rec: StreamRec,
    },
    AppendAck {
        to: u8,
        gen: u8,
        idx: u8,
        sealed: bool,
    },
    Drain {
        to: u8,
        gen: u8,
        recs: Vec<StreamRec>,
    },
    /// The root asks a backup to seal its delegate's generation.
    SealReq {
        to: u8,
        gen: u8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Violation {
    /// `(node, marker, data)`.
    MarkerOrder(u8, Key, Key),
    /// `(node, key)`.
    ReadYourWrites(u8, Key),
    /// A delegate acknowledged under an ended generation.
    RecallSafety(u8, u8),
}

pub const SAW_LOCAL_DELEG_WRITE: u16 = 1;
pub const SAW_FORWARD_TO_DELEG: u16 = 2;
pub const SAW_CROSS_SUBTREE: u16 = 4;
pub const SAW_RECALL_DRAINED: u16 = 8;
pub const SAW_RECALL_TIMEOUT: u16 = 16;
pub const SAW_REPLAY_LANDED: u16 = 32;
pub const SAW_TAKEOVER: u16 = 64;
pub const SAW_BACKUP_DRAIN: u16 = 128;
pub const SAW_RESTREAM: u16 = 256;
pub const SAW_DEPS_WAIT: u16 = 512;
pub const SAW_REDELEGATED: u16 = 1024;
pub const SAW_TENTATIVE: u16 = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: u8,
    pub off: Vec<i16>,
    pub jumps: u8,
    pub drops: u8,
    pub crashes: u8,
    pub reg: Reg,
    pub log: Vec<Entry>,
    pub nodes: Vec<Node>,
    pub net: Vec<Msg>,
    pub history: Vec<HEvt>,
    pub next_gen: u8,
    /// Ghost: generations some root has ended.
    pub ended_gens: u16,
    /// Ghost: keys with a non-tentative acknowledged `Create`.
    pub closed: u16,
    /// Ghost: rids acknowledged after a backup persisted them.
    pub durable_rids: Vec<Rid>,
    pub violation: Option<Violation>,
    pub saw: u16,
}

// ------------------------------------------------------------ state helpers

impl State {
    fn local(&self, i: usize) -> i16 {
        self.t as i16 + self.off[i]
    }

    fn usable_root(&self, cfg: &DelegModel, i: usize) -> bool {
        let n = &self.nodes[i];
        n.alive
            && n.root.as_ref().is_some_and(|r| {
                r.epoch == self.reg.epoch
                    && self.reg.holder == i as u8
                    && self.local(i) < r.expires - cfg.lease_margin
            })
    }

    fn send(&mut self, m: Msg) {
        self.net.push(m);
        self.net.sort();
    }

    /// The entries node `i` has: its applied prefix, its journal if it
    /// roots, its speculation if it delegates (in that order).
    fn entries_of(&self, i: usize) -> Vec<Entry> {
        let n = &self.nodes[i];
        let mut v: Vec<Entry> = self.log[..n.applied as usize].to_vec();
        if let Some(r) = &n.root {
            v.extend(r.journal.iter().cloned());
        }
        if let Some(d) = &n.deleg {
            for s in &d.spec {
                v.push(Entry {
                    origin: Origin {
                        stream: gen_stream(s.gen),
                        idx: s.idx,
                    },
                    rec: Rec::Write {
                        rid: s.rid,
                        op: s.op,
                    },
                    deps: s.deps.clone(),
                });
            }
        }
        v
    }

    fn view_of(&self, cfg: &DelegModel, i: usize) -> View {
        fold(initial_view(cfg), &self.entries_of(i))
    }

    /// The delegation table as node `i` sees it.
    fn table_of(&self, i: usize) -> Vec<Deleg> {
        live_table(&self.entries_of(i))
    }

    /// Highest index of `stream` among `entries`.
    fn applied_of(entries: &[Entry], stream: u8) -> u8 {
        entries
            .iter()
            .filter(|e| e.origin.stream == stream)
            .map(|e| e.origin.idx)
            .max()
            .unwrap_or(0)
    }

    /// Whether node `i`'s replica has everything `pos` names. A
    /// dependency on a stream the replica has seen end (a `Recall` of
    /// its generation, a marker above its epoch) past what the log
    /// appended of it is void: it can never be satisfied, and the
    /// acknowledgement it came from is tentative.
    fn satisfied(&self, i: usize, pos: &Pos) -> bool {
        let n = &self.nodes[i];
        if n.applied < pos.seq {
            return false;
        }
        let entries = self.entries_of(i);
        pos.pend.iter().all(|&(s, idx)| {
            Self::applied_of(&entries, s) >= idx || Self::stream_ended(&entries, s)
        })
    }

    /// The `deps` an executor records for a record: what the requester
    /// observed, with every dependency on a stream this replica has seen
    /// end lowered to that stream's cut (M6's void rule) — so a record
    /// never names a position no replica can ever hold.
    fn normalize_deps(entries: &[Entry], deps: &Pos) -> Pos {
        let mut p = deps.clone();
        for e in p.pend.iter_mut() {
            if Self::stream_ended(entries, e.0) {
                e.1 = e.1.min(Self::applied_of(entries, e.0));
            }
        }
        p.pend.retain(|e| e.1 > 0);
        p
    }

    fn stream_ended(entries: &[Entry], stream: u8) -> bool {
        entries.iter().any(|e| match e.rec {
            Rec::Recall { gen, .. } => gen_stream(gen) == stream,
            Rec::Marker => stream < GEN_BASE && e.origin.stream > stream,
            _ => false,
        })
    }

    fn completed(&self, i: usize, rid: Rid) -> bool {
        self.entries_of(i)
            .iter()
            .any(|e| matches!(e.rec, Rec::Write { rid: r, .. } if r == rid))
    }

    fn root_id(&self) -> Option<usize> {
        let h = self.reg.holder;
        (h != NONE).then_some(h as usize)
    }

    /// The position node `i` answers with: what its replica holds.
    fn pos_of(&self, i: usize) -> Pos {
        let n = &self.nodes[i];
        let mut p = Pos {
            seq: n.applied,
            pend: Vec::new(),
        };
        if let Some(r) = &n.root {
            if r.jseq > 0 && !r.journal.is_empty() {
                p.raise(r.epoch, r.jseq);
            }
            for g in 1..MAX_GENS as u8 {
                let gs = &r.gens[g as usize];
                if gs.cursor > 0 {
                    p.raise(gen_stream(g), gs.cursor);
                }
            }
        }
        if let Some(d) = &n.deleg {
            if d.next_idx > 1 {
                p.raise(gen_stream(d.gen), d.next_idx - 1);
            }
        }
        p
    }

    fn violate(&mut self, v: Violation) {
        if self.violation.is_none() {
            self.violation = Some(v);
        }
    }

    // ---- history

    fn invoke(&mut self, node: u8, rid: Rid, op: WriteOp) {
        match op {
            WriteOp::Create(k) => self.history.push(HEvt::Invoke {
                node,
                key: k,
                op: KeyOp::Create,
                rid,
            }),
            WriteOp::Unlink(k) => self.history.push(HEvt::Invoke {
                node,
                key: k,
                op: KeyOp::Unlink,
                rid,
            }),
            WriteOp::Move(a, b) => {
                self.history.push(HEvt::Invoke {
                    node,
                    key: a,
                    op: KeyOp::Unlink,
                    rid,
                });
                self.history.push(HEvt::Invoke {
                    node,
                    key: b,
                    op: KeyOp::Put,
                    rid,
                });
            }
        }
    }

    fn returned(&mut self, node: u8, rid: Rid, op: WriteOp, ret: Ret) {
        match op {
            WriteOp::Create(k) | WriteOp::Unlink(k) => {
                self.history.push(HEvt::Return {
                    node,
                    key: k,
                    ret,
                    rid,
                });
            }
            WriteOp::Move(a, b) => {
                self.history.push(HEvt::Return {
                    node,
                    key: a,
                    ret,
                    rid,
                });
                self.history.push(HEvt::Return {
                    node,
                    key: b,
                    ret: if ret == Ret::Ok {
                        Ret::Ok
                    } else {
                        Ret::Skipped
                    },
                    rid,
                });
            }
        }
    }

    fn mark(&mut self, rid: Rid, to: Ret) {
        for e in self.history.iter_mut() {
            if let HEvt::Return {
                ret, rid: r, key, ..
            } = e
            {
                if *r == rid && matches!(ret, Ret::Ok | Ret::Tentative) {
                    *ret = to;
                    self.closed &= !(1 << *key);
                }
            }
        }
        self.saw |= SAW_TENTATIVE;
    }
}

fn initial_view(cfg: &DelegModel) -> View {
    cfg.initial_present.iter().fold(0, |v, k| set(v, *k, true))
}

// ----------------------------------------------------------------- actions

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Jump(u8, i16),
    Drop(usize),
    Deliver(usize),
    Crash(u8),
    /// Apply the next log entry at node `i`.
    Tail(u8),
    /// The root ships its journal as one slot.
    Ship(u8),
    Takeover(u8),
    Start(u8),
    /// Re-resolve an op whose target changed (or waited locally).
    RetryWrite(u8),
    FinishRead(u8),
    /// A delegate re-checks its parked ops.
    DelegRetry(u8),
    /// A delegate renews its grant.
    RenewReq(u8),
    /// A delegate re-streams its speculation to a new root.
    Restream(u8),
    /// The root ends a generation whose recall the delegate did not
    /// answer in time.
    RecallTimeout(u8),
    /// The root delegates `dir` to `node`.
    Delegate(u8, u8),
    /// A backup seals and drains its tail to the root.
    Seal(u8),
    /// A requester replays a retracted op by rid.
    SendReplay(u8),
}

impl Model for DelegModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        let off = self.init_offsets.clone().unwrap_or(vec![0i16; n]);
        let mut log = Vec::new();
        let expires = off[0] + self.lease_ttl;
        log.push(Entry {
            origin: Origin { stream: 1, idx: 0 },
            rec: Rec::Marker,
            deps: Pos::default(),
        });
        let mut nodes: Vec<Node> = (0..n)
            .map(|_| Node {
                alive: true,
                applied: 0,
                epoch_seen: 1,
                pc: 0,
                next_seq: 0,
                op: NodeOp::Idle,
                observed: Pos::default(),
                acked: Vec::new(),
                replays: Vec::new(),
                replay_inflight: false,
                own_last: vec![None; (self.parents.len() as u8 * NAMES) as usize],
                root: None,
                deleg: None,
                tail: Vec::new(),
                sealed: None,
            })
            .collect();
        let mut root = Root {
            epoch: 1,
            expires,
            journal: Vec::new(),
            jseq: 0,
            gens: vec![GenState::DEAD; MAX_GENS],
            buffer: Vec::new(),
            held: Vec::new(),
        };
        let mut next_gen = 1;
        // The initial grants, given at `t = 0` and capped by the root
        // lease like every renewal.
        let ttl = if self.cap_by_lease {
            self.deleg_ttl.min(self.lease_ttl - self.lease_margin)
        } else {
            self.deleg_ttl
        };
        for &(dir, node) in &self.initial_delegations {
            let gen = next_gen;
            next_gen += 1;
            root.jseq += 1;
            log.push(Entry {
                origin: Origin {
                    stream: 1,
                    idx: root.jseq,
                },
                rec: Rec::Delegate { dir, node, gen },
                deps: Pos::default(),
            });
            root.gens[gen as usize] = GenState {
                live: true,
                ended: false,
                dir,
                node,
                cursor: 0,
                until: off[0] + ttl + self.root_margin,
                recall: RecallPhase::None,
                seal_asked: false,
            };
            nodes[node as usize].deleg = Some(DelegState {
                dir,
                gen,
                until: off[node as usize] + ttl - self.deleg_margin,
                stopped: false,
                spec: Vec::new(),
                next_idx: 1,
                streamed_epoch: 1,
                parked: Vec::new(),
                awaiting: Vec::new(),
                renew_sent: None,
            });
        }
        let len = log.len() as u8;
        for node in nodes.iter_mut() {
            node.applied = len;
        }
        nodes[0].root = Some(root);
        vec![State {
            t: 0,
            off,
            jumps: 0,
            drops: 0,
            crashes: 0,
            reg: Reg {
                holder: 0,
                epoch: 1,
                expires,
            },
            log,
            nodes,
            net: Vec::new(),
            history: Vec::new(),
            next_gen,
            ended_gens: 0,
            closed: 0,
            durable_rids: Vec::new(),
            violation: None,
            saw: 0,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.violation.is_some() {
            return;
        }
        let n = self.n();
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        if s.jumps < self.max_jumps {
            for i in 0..n {
                for d in -self.max_offset..=self.max_offset {
                    if d != s.off[i] {
                        actions.push(Action::Jump(i as u8, d));
                    }
                }
            }
        }
        for k in 0..s.net.len() {
            if k == 0 || s.net[k] != s.net[k - 1] {
                if s.drops < self.max_drops {
                    actions.push(Action::Drop(k));
                }
                actions.push(Action::Deliver(k));
            }
        }
        for i in 0..n {
            let node = &s.nodes[i];
            let iu = i as u8;
            if !node.alive {
                continue;
            }
            if s.crashes < self.max_crashes {
                actions.push(Action::Crash(iu));
            }
            if (node.applied as usize) < s.log.len() {
                actions.push(Action::Tail(iu));
            }
            if let Some(r) = &node.root {
                if !r.journal.is_empty() && s.usable_root(self, i) {
                    actions.push(Action::Ship(iu));
                }
                if s.usable_root(self, i) {
                    for g in 1..MAX_GENS {
                        let gs = &r.gens[g];
                        // A recall the delegate did not answer in time, or
                        // (`reclaim_expired`) a grant nobody renewed: the
                        // delegate honours neither any more.
                        if gs.live
                            && !gs.ended
                            && (gs.recall != RecallPhase::None || self.reclaim_expired)
                        {
                            let done =
                                matches!(gs.recall, RecallPhase::Drained(th) if gs.cursor >= th);
                            // With a seal asked of the backup, wait for
                            // its drain unless the request or the drain
                            // was lost (nothing in flight for it).
                            let seal_in_flight = gs.seal_asked
                                && s.net.iter().any(|m| {
                                    matches!(m, Msg::SealReq { gen, .. } | Msg::Drain { gen, .. } if *gen == g as u8)
                                });
                            if !done && !seal_in_flight && s.local(i) >= gs.until {
                                actions.push(Action::RecallTimeout(g as u8));
                            }
                        }
                    }
                    if s.next_gen < MAX_GENS as u8 {
                        let table = s.table_of(i);
                        for &(dir, node) in &self.delegatable {
                            let target = &s.nodes[node as usize];
                            if node != iu
                                && target.alive
                                && target.deleg.is_none()
                                && owner_of_dir(&table, &self.parents, dir).is_none()
                            {
                                actions.push(Action::Delegate(dir, node));
                            }
                        }
                    }
                }
            }
            let claimable = s.reg.holder == NONE || s.local(i) >= s.reg.expires;
            if claimable
                && node.deleg.is_none()
                && node.root.is_none()
                && s.reg.epoch < self.max_epoch
                && s.reg.holder != iu
            {
                actions.push(Action::Takeover(iu));
            }
            if let Some(d) = &node.deleg {
                if !d.parked.is_empty() && !d.stopped {
                    let p = &d.parked[0];
                    if s.local(i) < d.until && (!self.deps_at_delegate || s.satisfied(i, &p.deps)) {
                        actions.push(Action::DelegRetry(iu));
                    }
                }
                if self.allow_renew
                    && !d.stopped
                    && d.renew_sent.is_none()
                    && d.until.saturating_sub(s.local(i)) <= 1
                    && s.reg.holder != NONE
                {
                    actions.push(Action::RenewReq(iu));
                }
                if !d.spec.is_empty() && d.streamed_epoch < node.epoch_seen && s.reg.holder != NONE
                {
                    actions.push(Action::Restream(iu));
                }
            }
            if node.sealed.is_none() && !node.tail.is_empty() {
                let d = self.backups.iter().find(|(_, b)| *b == iu).map(|(d, _)| *d);
                if let Some(d) = d {
                    if !self.seal_only_after_crash || !s.nodes[d as usize].alive {
                        actions.push(Action::Seal(iu));
                    }
                }
            }
            if !node.replays.is_empty() && !node.replay_inflight && s.reg.holder != NONE {
                actions.push(Action::SendReplay(iu));
            }
            match &node.op {
                NodeOp::Idle => {
                    if (node.pc as usize) < self.scripts[i].len()
                        && s.history.len() + 2 <= self.max_history
                    {
                        actions.push(Action::Start(iu));
                    }
                }
                NodeOp::Writing {
                    rid,
                    op,
                    to,
                    gen,
                    applied_at,
                } => {
                    let in_flight = s.net.iter().any(|m| match m {
                        Msg::Fwd { from, rid: r, .. } => *from == iu && r == rid,
                        Msg::Ack { to, rid: r, .. } | Msg::NotOwner { to, rid: r, .. } => {
                            *to == iu && r == rid
                        }
                        _ => false,
                    }) || s.nodes.iter().any(|x| {
                        x.root
                            .as_ref()
                            .is_some_and(|r| r.held.iter().any(|h| h.rid == *rid))
                            || x.deleg.as_ref().is_some_and(|d| {
                                d.parked.iter().any(|p| p.rid == *rid)
                                    || d.awaiting.iter().any(|a| a.rid == *rid)
                            })
                    });
                    if !in_flight {
                        let (t2, g2) = resolve(self, s, i, *op);
                        let changed = t2 != *to || g2 != *gen || node.applied != *applied_at;
                        if *to == NONE || changed {
                            actions.push(Action::RetryWrite(iu));
                        }
                    }
                }
                NodeOp::ReadStart { .. } => {
                    if !self.session_wait || s.satisfied(i, &node.observed) {
                        actions.push(Action::FinishRead(iu));
                    }
                }
                NodeOp::ReadPair { .. } => actions.push(Action::FinishRead(iu)),
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Tick => s.t += 1,
            Action::Jump(i, d) => {
                s.off[i as usize] = d;
                s.jumps += 1;
            }
            Action::Drop(k) => {
                s.net.remove(k);
                s.drops += 1;
            }
            Action::Deliver(k) => {
                let m = s.net.remove(k);
                deliver(self, &mut s, m);
            }
            Action::Crash(i) => {
                s.nodes[i as usize].alive = false;
                s.crashes += 1;
            }
            Action::Tail(i) => tail(self, &mut s, i as usize),
            Action::Ship(i) => {
                let i = i as usize;
                let r = s.nodes[i].root.as_mut().expect("root");
                let j = std::mem::take(&mut r.journal);
                s.log.extend(j);
                s.nodes[i].applied = s.log.len() as u8;
            }
            Action::Takeover(i) => takeover(self, &mut s, i as usize),
            Action::Start(i) => {
                let i = i as usize;
                let op = self.scripts[i][s.nodes[i].pc as usize];
                match op {
                    Op::Read(key) => s.nodes[i].op = NodeOp::ReadStart { key },
                    Op::ReadPair { marker, data } => {
                        s.nodes[i].op = NodeOp::ReadPair { marker, data }
                    }
                    Op::Write(w) => {
                        let seq = s.nodes[i].next_seq;
                        s.nodes[i].next_seq += 1;
                        let rid = Rid { node: i as u8, seq };
                        s.invoke(i as u8, rid, w);
                        s.nodes[i].op = NodeOp::Writing {
                            rid,
                            op: w,
                            to: NONE,
                            gen: NONE,
                            applied_at: s.nodes[i].applied,
                        };
                        start_write(self, &mut s, i);
                    }
                }
            }
            Action::RetryWrite(i) => start_write(self, &mut s, i as usize),
            Action::FinishRead(i) => finish_read(self, &mut s, i as usize),
            Action::DelegRetry(i) => {
                let i = i as usize;
                let p = s.nodes[i]
                    .deleg
                    .as_mut()
                    .expect("delegate")
                    .parked
                    .remove(0);
                delegate_handle(self, &mut s, i, p.from, p.rid, p.op, p.deps, p.replay);
            }
            Action::RenewReq(i) => {
                let i = i as usize;
                let now = s.local(i);
                let d = s.nodes[i].deleg.as_mut().expect("delegate");
                d.renew_sent = Some(now);
                let gen = d.gen;
                let to = s.reg.holder;
                s.send(Msg::RenewReq {
                    from: i as u8,
                    to,
                    gen,
                });
            }
            Action::Restream(i) => {
                let i = i as usize;
                let to = s.reg.holder;
                let seen = s.nodes[i].epoch_seen;
                let d = s.nodes[i].deleg.as_mut().expect("delegate");
                d.streamed_epoch = seen;
                let recs: Vec<StreamRec> = s.nodes[i].deleg.as_ref().unwrap().spec.clone();
                for rec in recs {
                    s.send(Msg::Stream { to, rec });
                }
                s.saw |= SAW_RESTREAM;
            }
            Action::RecallTimeout(g) => {
                let r = s.root_id().expect("root");
                // A delegation with a live, unsealed backup is ended
                // through the backup (M9's failover shape): the drain
                // brings every backup-acknowledged record. Only a
                // delegation with no such backup is cut by time.
                let node = s.nodes[r].root.as_ref().unwrap().gens[g as usize].node;
                let backup = self
                    .backups
                    .iter()
                    .find(|(d, _)| *d == node)
                    .map(|(_, b)| *b)
                    .filter(|b| s.nodes[*b as usize].alive);
                match backup {
                    // Ask (again: the request or the drain may have been
                    // lost) and wait for the drain.
                    Some(b) => {
                        s.nodes[r].root.as_mut().unwrap().gens[g as usize].seal_asked = true;
                        s.send(Msg::SealReq { to: b, gen: g });
                    }
                    None => {
                        end_gen(self, &mut s, r, g);
                        s.saw |= SAW_RECALL_TIMEOUT;
                        root_progress(self, &mut s, r);
                    }
                }
            }
            Action::Delegate(dir, node) => {
                let r = s.root_id().expect("root");
                let gen = s.next_gen;
                s.next_gen += 1;
                let now = s.local(r);
                let root = s.nodes[r].root.as_mut().expect("root");
                root.gens[gen as usize] = GenState {
                    live: true,
                    ended: false,
                    dir,
                    node,
                    cursor: 0,
                    // Live from the record on; the delegate honours
                    // nothing until it renews.
                    until: now,
                    recall: RecallPhase::None,
                    seal_asked: false,
                };
                root_append(
                    self,
                    &mut s,
                    r,
                    Rec::Delegate { dir, node, gen },
                    Pos::default(),
                );
                s.saw |= SAW_REDELEGATED;
            }
            Action::Seal(b) => seal(&mut s, b as usize, None),
            Action::SendReplay(i) => {
                let i = i as usize;
                let (rid, op) = s.nodes[i].replays[0];
                let (to, _) = resolve(self, &s, i, op);
                s.nodes[i].replay_inflight = true;
                let deps = s.nodes[i].observed.clone();
                if to == i as u8 {
                    execute_here(self, &mut s, i, i as u8, rid, op, deps, true);
                } else {
                    s.send(Msg::Fwd {
                        from: i as u8,
                        to,
                        rid,
                        op,
                        deps,
                        replay: true,
                    });
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("per_key_linearizable", |m: &DelegModel, s: &State| {
                prop_per_key_linearizable(m, s)
            }),
            Property::always("causal_cut", |m: &DelegModel, s: &State| {
                prop_causal_cut(m, s)
            }),
            Property::always("marker_order", |_, s: &State| {
                !matches!(s.violation, Some(Violation::MarkerOrder(..)))
            }),
            Property::always("recall_safety", |_, s: &State| {
                !matches!(s.violation, Some(Violation::RecallSafety(..)))
            }),
            Property::always("log_records_valid", |m: &DelegModel, s: &State| {
                prop_log_valid(m, s)
            }),
            Property::always("exactly_once", |_, s: &State| prop_exactly_once(s)),
            Property::always("converged_at_quiescence", |m: &DelegModel, s: &State| {
                prop_converged(m, s)
            }),
            Property::always("read_your_writes", |_, s: &State| {
                !matches!(s.violation, Some(Violation::ReadYourWrites(..)))
            }),
            // A recall the root outwaited counts as a fault: a reply
            // delayed past the grant's ttl is indistinguishable from a
            // dead delegate, and outwaiting it is the design.
            Property::always("stable_without_faults", |_, s: &State| {
                !(s.crashes == 0
                    && s.drops == 0
                    && s.jumps == 0
                    && s.saw & (SAW_RECALL_TIMEOUT | SAW_TAKEOVER) == 0
                    && s.saw & SAW_TENTATIVE != 0)
            }),
            // M9's `acked_never_lost`, per delegate: an op acknowledged
            // after its backup persisted it is in the log, or held by a
            // live node — the delegate's speculation, the root's journal
            // or buffer, a listed backup's tail, or a requester's Layer-A
            // memory and replay queue. A crash of one node never loses it.
            Property::always("durable_acks_stand", |_, s: &State| {
                prop_durable_never_lost(s)
            }),
            Property::sometimes("all_ops_done", |m: &DelegModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| !n.alive || n.pc as usize == sc.len())
                    && quiescent(s)
            }),
        ];
        macro_rules! witness {
            ($name:literal, $bit:expr) => {
                props.push(Property::sometimes($name, |_, s: &State| s.saw & $bit != 0));
            };
        }
        witness!("delegate_wrote_locally", SAW_LOCAL_DELEG_WRITE);
        witness!("forwarded_to_delegate", SAW_FORWARD_TO_DELEG);
        witness!("cross_subtree_recall", SAW_CROSS_SUBTREE);
        witness!("recall_drained", SAW_RECALL_DRAINED);
        witness!("recall_timed_out", SAW_RECALL_TIMEOUT);
        witness!("replay_landed", SAW_REPLAY_LANDED);
        witness!("root_takeover", SAW_TAKEOVER);
        witness!("backup_drained", SAW_BACKUP_DRAIN);
        witness!("restreamed", SAW_RESTREAM);
        witness!("deps_waited", SAW_DEPS_WAIT);
        witness!("redelegated", SAW_REDELEGATED);
        props
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick && s.history.len() <= self.max_history
    }
}

// ------------------------------------------------------------ the protocol

/// Where node `i` sends `op` per its own view: `(target, gen)`; the
/// target is the root for cross-subtree ops and root-owned keys.
fn resolve(cfg: &DelegModel, s: &State, i: usize, op: WriteOp) -> (u8, u8) {
    let table = s.table_of(i);
    match owner_of_op(&table, &cfg.parents, op) {
        Some(d) => (d.node, d.gen),
        None => (s.reg.holder, NONE),
    }
}

/// Start (or retry) node `i`'s write.
fn start_write(cfg: &DelegModel, s: &mut State, i: usize) {
    let NodeOp::Writing { rid, op, .. } = s.nodes[i].op.clone() else {
        return;
    };
    let (to, gen) = resolve(cfg, s, i, op);
    s.nodes[i].op = NodeOp::Writing {
        rid,
        op,
        to,
        gen,
        applied_at: s.nodes[i].applied,
    };
    if to == NONE {
        return;
    }
    let deps = s.nodes[i].observed.clone();
    if to == i as u8 {
        execute_here(cfg, s, i, i as u8, rid, op, deps, false);
    } else {
        if gen != NONE {
            s.saw |= SAW_FORWARD_TO_DELEG;
        }
        s.send(Msg::Fwd {
            from: i as u8,
            to,
            rid,
            op,
            deps,
            replay: false,
        });
    }
}

/// Node `i` was asked to execute `op` (locally or by `from`): as a
/// delegate or as the root.
#[allow(clippy::too_many_arguments)]
fn execute_here(
    cfg: &DelegModel,
    s: &mut State,
    i: usize,
    from: u8,
    rid: Rid,
    op: WriteOp,
    deps: Pos,
    replay: bool,
) {
    if s.nodes[i].deleg.is_some() {
        delegate_handle(cfg, s, i, from, rid, op, deps, replay);
    } else if s.nodes[i].root.is_some() {
        root_handle(cfg, s, i, from, rid, op, deps, replay);
    } else {
        ack(
            cfg,
            s,
            i,
            Msg::NotOwner {
                to: from,
                rid,
                replay,
            },
        );
    }
}

/// An acknowledgement to the node that executed the op needs no
/// network: the FUSE reply is local.
fn ack(cfg: &DelegModel, s: &mut State, executor: usize, m: Msg) {
    let to = match &m {
        Msg::Ack { to, .. } | Msg::NotOwner { to, .. } => *to as usize,
        _ => unreachable!(),
    };
    if to == executor {
        deliver(cfg, s, m);
    } else {
        s.send(m);
    }
}

/// A delegate's side of `Fwd` (or its own client's op).
#[allow(clippy::too_many_arguments)]
fn delegate_handle(
    cfg: &DelegModel,
    s: &mut State,
    i: usize,
    from: u8,
    rid: Rid,
    op: WriteOp,
    deps: Pos,
    replay: bool,
) {
    let table = s.table_of(i);
    let owner = owner_of_op(&table, &cfg.parents, op);
    let Some(d) = s.nodes[i].deleg.clone() else {
        ack(
            cfg,
            s,
            i,
            Msg::NotOwner {
                to: from,
                rid,
                replay,
            },
        );
        return;
    };
    if d.stopped || owner.is_none_or(|o| o.node != i as u8 || o.gen != d.gen) {
        ack(
            cfg,
            s,
            i,
            Msg::NotOwner {
                to: from,
                rid,
                replay,
            },
        );
        return;
    }
    if s.completed(i, rid) {
        let pos = s.pos_of(i);
        ack(
            cfg,
            s,
            i,
            Msg::Ack {
                to: from,
                rid,
                ret: Ret::Ok,
                pos,
                origin: None,
                replay,
                durable: false,
            },
        );
        return;
    }
    let expired = s.local(i) >= d.until;
    let deps_missing = cfg.deps_at_delegate && !s.satisfied(i, &deps);
    if expired || deps_missing {
        if deps_missing {
            s.saw |= SAW_DEPS_WAIT;
        }
        s.nodes[i].deleg.as_mut().unwrap().parked.push(Parked {
            from,
            rid,
            op,
            deps,
            replay,
        });
        return;
    }
    if s.ended_gens & (1 << d.gen) != 0 {
        s.violate(Violation::RecallSafety(i as u8, d.gen));
    }
    if from == i as u8 {
        s.saw |= SAW_LOCAL_DELEG_WRITE;
    }
    let view = s.view_of(cfg, i);
    let (ret, _) = eval(view, op);
    let mut origin = None;
    if ret == Ret::Ok {
        let idx = d.next_idx;
        let rec = StreamRec {
            gen: d.gen,
            idx,
            rid,
            op,
            deps: State::normalize_deps(&s.entries_of(i), &deps),
        };
        origin = Some(Origin {
            stream: gen_stream(d.gen),
            idx,
        });
        let dd = s.nodes[i].deleg.as_mut().unwrap();
        dd.next_idx += 1;
        dd.spec.push(rec.clone());
        let to = s.reg.holder;
        s.send(Msg::Stream {
            to,
            rec: rec.clone(),
        });
        if let Some(&(_, b)) = cfg.backups.iter().find(|(dn, _)| *dn == i as u8) {
            let pos = s.pos_of(i);
            s.nodes[i].deleg.as_mut().unwrap().awaiting.push(Awaiting {
                idx,
                from,
                rid,
                replay,
                ret,
                pos,
            });
            s.send(Msg::Append {
                from: i as u8,
                to: b,
                rec,
            });
            return;
        }
    }
    let pos = s.pos_of(i);
    ack(
        cfg,
        s,
        i,
        Msg::Ack {
            to: from,
            rid,
            ret,
            pos,
            origin,
            replay,
            durable: false,
        },
    );
}

/// The root's side of `Fwd` (or its own client's op).
#[allow(clippy::too_many_arguments)]
fn root_handle(
    cfg: &DelegModel,
    s: &mut State,
    r: usize,
    from: u8,
    rid: Rid,
    op: WriteOp,
    deps: Pos,
    replay: bool,
) {
    if !s.usable_root(cfg, r) {
        ack(
            cfg,
            s,
            r,
            Msg::NotOwner {
                to: from,
                rid,
                replay,
            },
        );
        return;
    }
    if s.completed(r, rid) {
        let pos = s.pos_of(r);
        ack(
            cfg,
            s,
            r,
            Msg::Ack {
                to: from,
                rid,
                ret: Ret::Ok,
                pos,
                origin: None,
                replay,
                durable: true,
            },
        );
        return;
    }
    let table = s.table_of(r);
    let touched = delegations_touched(&table, &cfg.parents, op);
    if owner_of_op(&table, &cfg.parents, op).is_some() {
        // Wholly under one delegation: not the root's.
        ack(
            cfg,
            s,
            r,
            Msg::NotOwner {
                to: from,
                rid,
                replay,
            },
        );
        return;
    }
    let mut waiting = Vec::new();
    if !touched.is_empty() {
        s.saw |= SAW_CROSS_SUBTREE;
    }
    for d in touched {
        start_recall(cfg, s, r, d.gen);
        if cfg.drain_before_execute {
            waiting.push(d.gen);
        }
    }
    s.nodes[r].root.as_mut().unwrap().held.push(HeldOp {
        from,
        rid,
        op,
        deps,
        replay,
        waiting,
    });
    root_progress(cfg, s, r);
}

fn start_recall(cfg: &DelegModel, s: &mut State, r: usize, gen: u8) {
    let root = s.nodes[r].root.as_mut().unwrap();
    let gs = &mut root.gens[gen as usize];
    if gs.recall != RecallPhase::None || gs.ended {
        return;
    }
    gs.recall = RecallPhase::Sent;
    let to = gs.node;
    s.send(Msg::Recall { to, gen });
    if !cfg.drain_before_execute {
        end_gen(cfg, s, r, gen);
    }
}

/// End generation `gen` at its current cursor: the `Recall` record.
fn end_gen(cfg: &DelegModel, s: &mut State, r: usize, gen: u8) {
    let root = s.nodes[r].root.as_mut().unwrap();
    let gs = &mut root.gens[gen as usize];
    if gs.ended {
        return;
    }
    gs.ended = true;
    let dir = gs.dir;
    root.buffer.retain(|b| b.gen != gen);
    for h in root.held.iter_mut() {
        h.waiting.retain(|g| *g != gen);
    }
    s.ended_gens |= 1 << gen;
    let cut = s.nodes[r].root.as_ref().unwrap().gens[gen as usize].cursor;
    strand_stream(s, gen_stream(gen), cut);
    root_append(cfg, s, r, Rec::Recall { dir, gen }, Pos::default());
}

/// Ghost: stream `stream` ended with everything past `cut` unappended.
/// Every acknowledgement past it is tentative from this instant (the
/// requesters learn it when they tail the record); a backup-acknowledged
/// one was promised durable.
fn strand_stream(s: &mut State, stream: u8, cut: u8) {
    let lost: Vec<(Rid, bool)> = s
        .nodes
        .iter()
        .flat_map(|n| n.acked.iter())
        .filter(|a| a.origin.stream == stream && a.origin.idx > cut)
        .map(|a| (a.rid, a.durable))
        .collect();
    let _ = &lost;
    for (rid, _) in lost {
        s.mark(rid, Ret::Tentative);
    }
    // An acknowledgement still in flight is from an ended stream too: it
    // arrives as a tentative one (M3b's reply-racing-takeover rule).
    for m in s.net.iter_mut() {
        if let Msg::Ack { ret, origin, .. } = m {
            if *ret == Ret::Ok && origin.is_some_and(|o| o.stream == stream && o.idx > cut) {
                *ret = Ret::Tentative;
            }
        }
    }
}

fn root_append(cfg: &DelegModel, s: &mut State, r: usize, rec: Rec, deps: Pos) -> Origin {
    let root = s.nodes[r].root.as_mut().unwrap();
    root.jseq += 1;
    let origin = Origin {
        stream: root.epoch,
        idx: root.jseq,
    };
    let e = Entry { origin, rec, deps };
    if cfg.journal {
        root.journal.push(e);
    } else {
        s.log.push(e);
        s.nodes[r].applied = s.log.len() as u8;
    }
    origin
}

/// The root's stream cursors satisfy `pos`.
fn root_satisfied(s: &State, r: usize, pos: &Pos) -> bool {
    let root = s.nodes[r].root.as_ref().unwrap();
    pos.pend.iter().all(|&(st, idx)| {
        if st >= GEN_BASE {
            let gs = &root.gens[(st - GEN_BASE) as usize];
            gs.cursor >= idx || gs.ended
        } else if st == root.epoch {
            root.jseq >= idx
        } else {
            // An older epoch: what it shipped is in the log; the rest
            // is void.
            true
        }
    })
}

/// Everything the root can do now: append buffered stream records in
/// order, complete drained recalls, execute held ops.
fn root_progress(cfg: &DelegModel, s: &mut State, r: usize) {
    loop {
        let mut changed = false;
        // Appends.
        for g in 1..MAX_GENS {
            loop {
                let root = s.nodes[r].root.as_ref().unwrap();
                let gs = &root.gens[g];
                if !gs.live || (cfg.gen_check && gs.ended) {
                    break;
                }
                let next = gs.cursor + 1;
                let Some(k) = root
                    .buffer
                    .iter()
                    .position(|b| b.gen == g as u8 && b.idx == next)
                else {
                    break;
                };
                let rec = root.buffer[k].clone();
                if cfg.deps_at_root && !root_satisfied(s, r, &rec.deps) {
                    break;
                }
                let root = s.nodes[r].root.as_mut().unwrap();
                root.buffer.remove(k);
                root.gens[g].cursor = next;
                let stream = gen_stream(g as u8);
                let e = Entry {
                    origin: Origin { stream, idx: next },
                    rec: Rec::Write {
                        rid: rec.rid,
                        op: rec.op,
                    },
                    deps: rec.deps,
                };
                if cfg.journal {
                    root.journal.push(e);
                } else {
                    s.log.push(e);
                    s.nodes[r].applied = s.log.len() as u8;
                }
                changed = true;
            }
        }
        // Drained recalls.
        for g in 1..MAX_GENS {
            let gs = s.nodes[r].root.as_ref().unwrap().gens[g];
            if gs.live && !gs.ended {
                if let RecallPhase::Drained(th) = gs.recall {
                    if gs.cursor >= th {
                        end_gen(cfg, s, r, g as u8);
                        s.saw |= SAW_RECALL_DRAINED;
                        changed = true;
                    }
                }
            }
        }
        // Held ops.
        let root = s.nodes[r].root.as_ref().unwrap();
        let ready = root.held.iter().position(|h| {
            h.waiting.is_empty() && (!cfg.deps_at_root || root_satisfied(s, r, &h.deps))
        });
        if let Some(k) = ready {
            let h = s.nodes[r].root.as_mut().unwrap().held.remove(k);
            root_execute(cfg, s, r, h);
            changed = true;
        } else if let Some(root) = s.nodes[r].root.as_ref() {
            if root.held.iter().any(|h| h.waiting.is_empty()) {
                s.saw |= SAW_DEPS_WAIT;
            }
        }
        if !changed {
            return;
        }
    }
}

fn root_execute(cfg: &DelegModel, s: &mut State, r: usize, h: HeldOp) {
    let view = s.view_of(cfg, r);
    let (ret, _) = eval(view, h.op);
    let mut origin = None;
    if ret == Ret::Ok {
        let deps = State::normalize_deps(&s.entries_of(r), &h.deps);
        origin = Some(root_append(
            cfg,
            s,
            r,
            Rec::Write {
                rid: h.rid,
                op: h.op,
            },
            deps,
        ));
    }
    let pos = s.pos_of(r);
    ack(
        cfg,
        s,
        r,
        Msg::Ack {
            to: h.from,
            rid: h.rid,
            ret,
            pos,
            origin,
            replay: h.replay,
            durable: !cfg.journal,
        },
    );
}

fn finish_read(cfg: &DelegModel, s: &mut State, i: usize) {
    let view = s.view_of(cfg, i);
    match s.nodes[i].op.clone() {
        NodeOp::ReadStart { key } => {
            if let Some(want) = s.nodes[i].own_last[key as usize] {
                if present(view, key) != want {
                    s.violate(Violation::ReadYourWrites(i as u8, key));
                }
            }
        }
        NodeOp::ReadPair { marker, data } => {
            if present(view, marker) && !present(view, data) && s.closed & (1 << data) != 0 {
                s.violate(Violation::MarkerOrder(i as u8, marker, data));
            }
        }
        _ => return,
    }
    s.nodes[i].op = NodeOp::Idle;
    s.nodes[i].pc += 1;
}

fn takeover(cfg: &DelegModel, s: &mut State, i: usize) {
    let epoch = s.reg.epoch + 1;
    let expires = s.local(i) + cfg.lease_ttl;
    s.reg = Reg {
        holder: i as u8,
        epoch,
        expires,
    };
    while (s.nodes[i].applied as usize) < s.log.len() {
        tail(cfg, s, i);
    }
    for old in 1..epoch {
        let cut = State::applied_of(&s.log, old);
        strand_stream(s, old, cut);
    }
    s.log.push(Entry {
        origin: Origin {
            stream: epoch,
            idx: 0,
        },
        rec: Rec::Marker,
        deps: Pos::default(),
    });
    s.nodes[i].applied = s.log.len() as u8;
    s.nodes[i].epoch_seen = epoch;
    let now = s.local(i);
    let mut gens = vec![GenState::DEAD; MAX_GENS];
    for d in live_table(&s.log) {
        gens[d.gen as usize] = GenState {
            live: true,
            ended: false,
            dir: d.dir,
            node: d.node,
            cursor: State::applied_of(&s.log, gen_stream(d.gen)),
            until: if cfg.takeover_horizon {
                now + cfg.deleg_ttl + cfg.root_margin
            } else {
                now
            },
            recall: RecallPhase::None,
            seal_asked: false,
        };
    }
    for e in &s.log {
        if let Rec::Recall { gen, .. } = e.rec {
            gens[gen as usize].ended = true;
        }
    }
    s.nodes[i].root = Some(Root {
        epoch,
        expires,
        journal: Vec::new(),
        jseq: 0,
        gens,
        buffer: Vec::new(),
        held: Vec::new(),
    });
    s.saw |= SAW_TAKEOVER;
}

/// Apply the next log entry at node `i`: retire or retract speculation
/// and acknowledgements, install a delegation, learn an epoch, depose.
fn tail(cfg: &DelegModel, s: &mut State, i: usize) {
    let e = s.log[s.nodes[i].applied as usize].clone();
    s.nodes[i].applied += 1;
    let applied_prefix: Vec<Entry> = s.log[..s.nodes[i].applied as usize].to_vec();
    // Streams this entry ends.
    let mut ended: Vec<u8> = Vec::new();
    match e.rec {
        Rec::Marker => {
            let epoch = e.origin.stream;
            s.nodes[i].epoch_seen = epoch;
            for st in 1..epoch {
                ended.push(st);
            }
            // A deposed root: its journal is stranded; its own
            // acknowledgements follow the stream rule below.
            if s.nodes[i].root.as_ref().is_some_and(|r| r.epoch < epoch) {
                s.nodes[i].root = None;
            }
        }
        Rec::Recall { gen, .. } => ended.push(gen_stream(gen)),
        Rec::Delegate { dir, node, gen } => {
            if let Some(d) = &s.nodes[i].deleg {
                if d.dir == dir && d.gen < gen {
                    ended.push(gen_stream(d.gen));
                }
            }
            if node as usize == i && s.nodes[i].deleg.is_none() {
                s.nodes[i].deleg = Some(DelegState {
                    dir,
                    gen,
                    until: i16::MIN,
                    stopped: false,
                    spec: Vec::new(),
                    next_idx: 1,
                    streamed_epoch: s.nodes[i].epoch_seen,
                    parked: Vec::new(),
                    awaiting: Vec::new(),
                    renew_sent: None,
                });
            }
        }
        Rec::Write { .. } => {}
    }
    // Retire: the log carries this record.
    if let Some(d) = s.nodes[i].deleg.as_mut() {
        if e.origin.stream == gen_stream(d.gen) {
            d.spec.retain(|r| r.idx > e.origin.idx);
        }
    }
    s.nodes[i].acked.retain(|a| a.origin != e.origin);
    // Retract past the cut of an ended stream.
    for st in ended {
        let cut = State::applied_of(&applied_prefix, st);
        let lost: Vec<Acked> = s.nodes[i]
            .acked
            .iter()
            .filter(|a| a.origin.stream == st && a.origin.idx > cut)
            .cloned()
            .collect();
        s.nodes[i]
            .acked
            .retain(|a| !(a.origin.stream == st && a.origin.idx > cut));
        // M6's rule: the part of an observation its stream never
        // appended is void once the stream ended.
        for e in s.nodes[i].observed.pend.iter_mut() {
            if e.0 == st && e.1 > cut {
                e.1 = cut;
            }
        }
        s.nodes[i].observed.pend.retain(|e| e.1 > 0);
        for a in lost {
            for k in a.op.keys() {
                s.nodes[i].own_last[k as usize] = None;
            }
            s.nodes[i].replays.push((a.rid, a.op));
        }
        if let Some(d) = s.nodes[i].deleg.clone() {
            if gen_stream(d.gen) == st {
                s.nodes[i].deleg = None;
                for p in d.parked {
                    ack(
                        cfg,
                        s,
                        i,
                        Msg::NotOwner {
                            to: p.from,
                            rid: p.rid,
                            replay: p.replay,
                        },
                    );
                }
            }
        }
    }
}

fn deliver(cfg: &DelegModel, s: &mut State, m: Msg) {
    match m {
        Msg::Fwd {
            from,
            to,
            rid,
            op,
            deps,
            replay,
        } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            execute_here(cfg, s, i, from, rid, op, deps, replay);
        }
        Msg::Ack {
            to,
            rid,
            ret,
            pos,
            origin,
            replay,
            durable,
        } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            s.nodes[i].observed = s.nodes[i].observed.join(&pos);
            if replay {
                if s.nodes[i].replays.first().is_some_and(|(r, _)| *r == rid) {
                    s.nodes[i].replays.remove(0);
                    s.nodes[i].replay_inflight = false;
                    if ret == Ret::Ok {
                        s.saw |= SAW_REPLAY_LANDED;
                        if let Some(o) = origin {
                            // Track the landed replay like an acknowledgement, so
                            // a later stranding of *it* is seen too.
                            if let Some(w) = op_of(s, rid) {
                                s.nodes[i].acked.push(Acked {
                                    rid,
                                    op: w,
                                    origin: o,
                                    durable,
                                });
                            }
                        }
                    } else {
                        s.mark(rid, Ret::Conflicted);
                    }
                }
                return;
            }
            let NodeOp::Writing { rid: r, op, .. } = s.nodes[i].op.clone() else {
                return;
            };
            if r != rid {
                return;
            }
            // M3's reply-racing-takeover rule: an acknowledgement from a
            // stream this node has already seen end, past the cut, is
            // tentative on arrival and queued for replay, never installed
            // as a clean success.
            let stale = ret == Ret::Tentative
                || origin.is_some_and(|o| {
                    let prefix = &s.log[..s.nodes[i].applied as usize];
                    State::stream_ended(prefix, o.stream)
                        && State::applied_of(prefix, o.stream) < o.idx
                });
            if stale && matches!(ret, Ret::Ok | Ret::Tentative) {
                s.returned(i as u8, rid, op, Ret::Tentative);
                s.saw |= SAW_TENTATIVE;
                s.nodes[i].replays.push((rid, op));
                s.nodes[i].op = NodeOp::Idle;
                s.nodes[i].pc += 1;
                return;
            }
            s.returned(i as u8, rid, op, ret);
            if ret == Ret::Ok && durable && !s.durable_rids.contains(&rid) {
                s.durable_rids.push(rid);
            }
            if ret == Ret::Ok {
                for k in op.keys() {
                    let on = !matches!(op, WriteOp::Unlink(_))
                        && !matches!(op, WriteOp::Move(a, _) if a == k);
                    s.nodes[i].own_last[k as usize] = Some(on);
                    if on {
                        s.closed |= 1 << k;
                    }
                }
                if let Some(o) = origin {
                    s.nodes[i].acked.push(Acked {
                        rid,
                        op,
                        origin: o,
                        durable,
                    });
                }
            }
            s.nodes[i].op = NodeOp::Idle;
            s.nodes[i].pc += 1;
        }
        Msg::NotOwner { to, rid, replay } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            if replay {
                s.nodes[i].replay_inflight = false;
                return;
            }
            if let NodeOp::Writing {
                rid: r, to: sent, ..
            } = &mut s.nodes[i].op
            {
                if *r == rid {
                    *sent = NONE;
                }
            }
        }
        Msg::Stream { to, rec } => {
            let r = to as usize;
            if !s.usable_root(cfg, r) {
                return;
            }
            let root = s.nodes[r].root.as_mut().unwrap();
            let gs = &root.gens[rec.gen as usize];
            if !gs.live || rec.idx <= gs.cursor || root.buffer.contains(&rec) {
                return;
            }
            if cfg.gen_check && gs.ended {
                return;
            }
            root.buffer.push(rec);
            root.buffer.sort_by_key(|b| (b.gen, b.idx));
            root_progress(cfg, s, r);
        }
        Msg::RenewReq { from, to, gen } => {
            let r = to as usize;
            let mut ttl = 0;
            if s.usable_root(cfg, r) {
                let now = s.local(r);
                let root = s.nodes[r].root.as_mut().unwrap();
                let gs = &mut root.gens[gen as usize];
                // No renewal once a recall or a reclaim has begun: the
                // generation is ending.
                if gs.live
                    && !gs.ended
                    && gs.recall == RecallPhase::None
                    && !gs.seal_asked
                    && gs.node == from
                {
                    ttl = cfg.deleg_ttl;
                    if cfg.cap_by_lease {
                        ttl = ttl.min(root.expires - cfg.lease_margin - now);
                    }
                    if ttl > 0 {
                        gs.until = gs.until.max(now + ttl + cfg.root_margin);
                    }
                }
            }
            s.send(Msg::Renewed { to: from, gen, ttl });
        }
        Msg::Renewed { to, gen, ttl } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            if let Some(d) = s.nodes[i].deleg.as_mut() {
                if d.gen == gen {
                    if let Some(sent) = d.renew_sent.take() {
                        if ttl > 0 {
                            // Measured from the request's send (M8): the
                            // root granted at or after that instant.
                            d.until = d.until.max(sent + ttl - cfg.deleg_margin);
                        }
                    }
                }
            }
        }
        Msg::Recall { to, gen } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            let Some(d) = s.nodes[i].deleg.clone() else {
                return;
            };
            if d.gen != gen || d.stopped {
                return;
            }
            let dd = s.nodes[i].deleg.as_mut().unwrap();
            dd.stopped = true;
            let parked = std::mem::take(&mut dd.parked);
            let through = dd.next_idx - 1;
            let root = s.reg.holder;
            s.send(Msg::Recalled {
                to: root,
                gen,
                through,
            });
            for p in parked {
                ack(
                    cfg,
                    s,
                    i,
                    Msg::NotOwner {
                        to: p.from,
                        rid: p.rid,
                        replay: p.replay,
                    },
                );
            }
        }
        Msg::Recalled { to, gen, through } => {
            let r = to as usize;
            if !s.usable_root(cfg, r) {
                return;
            }
            let root = s.nodes[r].root.as_mut().unwrap();
            let gs = &mut root.gens[gen as usize];
            if gs.recall == RecallPhase::Sent {
                gs.recall = RecallPhase::Drained(through);
            }
            root_progress(cfg, s, r);
        }
        Msg::Append { from, to, rec } => {
            let b = to as usize;
            if !s.nodes[b].alive {
                return;
            }
            let sealed = s.nodes[b].sealed.is_some();
            if !sealed {
                // The append stream is ordered: a record ahead of the
                // tail's end is not the next one and is left unanswered.
                let expected = s.nodes[b].tail.last().map_or(1, |r| r.idx + 1);
                if rec.idx != expected || s.nodes[b].tail.first().is_some_and(|r| r.gen != rec.gen)
                {
                    return;
                }
                s.nodes[b].tail.push(rec.clone());
            }
            s.send(Msg::AppendAck {
                to: from,
                gen: rec.gen,
                idx: rec.idx,
                sealed,
            });
        }
        Msg::AppendAck {
            to,
            gen,
            idx,
            sealed,
        } => {
            let i = to as usize;
            if !s.nodes[i].alive {
                return;
            }
            let now = s.local(i);
            let Some(d) = s.nodes[i].deleg.as_mut() else {
                return;
            };
            if d.gen != gen {
                return;
            }
            if sealed {
                d.stopped = true;
                d.awaiting.clear();
                return;
            }
            let Some(k) = d.awaiting.iter().position(|a| a.idx == idx) else {
                return;
            };
            let a = d.awaiting.remove(k);
            // M9's rule for a parked reply: it leaves only while the
            // grant is still honoured (the requester retries otherwise).
            if now >= d.until || d.stopped {
                return;
            }
            let appended = s.log.iter().any(|e| {
                e.origin
                    == Origin {
                        stream: gen_stream(gen),
                        idx,
                    }
            });
            if s.ended_gens & (1 << gen) != 0 && !appended {
                s.violate(Violation::RecallSafety(i as u8, gen));
            }
            ack(
                cfg,
                s,
                i,
                Msg::Ack {
                    to: a.from,
                    rid: a.rid,
                    ret: a.ret,
                    pos: a.pos,
                    origin: Some(Origin {
                        stream: gen_stream(gen),
                        idx,
                    }),
                    replay: a.replay,
                    durable: true,
                },
            );
        }
        Msg::SealReq { to, gen } => {
            let b = to as usize;
            if s.nodes[b].alive {
                seal(s, b, Some(gen));
            }
        }
        Msg::Drain { to, gen, recs } => {
            let r = to as usize;
            if !s.usable_root(cfg, r) {
                return;
            }
            let root = s.nodes[r].root.as_mut().unwrap();
            if !root.gens[gen as usize].live || root.gens[gen as usize].ended {
                return;
            }
            for rec in recs {
                if rec.idx > root.gens[gen as usize].cursor && !root.buffer.contains(&rec) {
                    root.buffer.push(rec);
                }
            }
            root.buffer.sort_by_key(|b| (b.gen, b.idx));
            root_progress(cfg, s, r);
            end_gen(cfg, s, r, gen);
            s.saw |= SAW_BACKUP_DRAIN;
            root_progress(cfg, s, r);
        }
    }
}

/// Backup `b` seals (`gen`, or whatever its tail holds) and drains its
/// tail to the root.
fn seal(s: &mut State, b: usize, gen: Option<u8>) {
    let gen = gen.or_else(|| s.nodes[b].tail.first().map(|r| r.gen));
    let Some(gen) = gen else {
        return;
    };
    // Already sealed: answer with the drain again (the first may have
    // been lost).
    s.nodes[b].sealed = Some(gen);
    let recs = s.nodes[b].tail.clone();
    let to = s.reg.holder;
    s.send(Msg::Drain { to, gen, recs });
}

fn op_of(s: &State, rid: Rid) -> Option<WriteOp> {
    // Reconstruct from the history's per-key ops.
    let mut keys: Vec<(Key, KeyOp)> = Vec::new();
    for e in &s.history {
        if let HEvt::Invoke {
            rid: r, key, op, ..
        } = e
        {
            if *r == rid {
                keys.push((*key, *op));
            }
        }
    }
    match keys.as_slice() {
        [(k, KeyOp::Create)] => Some(WriteOp::Create(*k)),
        [(k, KeyOp::Unlink)] => Some(WriteOp::Unlink(*k)),
        [(a, KeyOp::Unlink), (b, KeyOp::Put)] => Some(WriteOp::Move(*a, *b)),
        _ => None,
    }
}

// -------------------------------------------------------------- properties

/// Thread ids for tentative ops: above any node id, plus the history
/// index.
const TENTATIVE_THREAD_BASE: usize = 64;

fn prop_per_key_linearizable(m: &DelegModel, s: &State) -> bool {
    let tentative: Vec<Rid> = s
        .history
        .iter()
        .filter_map(|e| match e {
            HEvt::Return {
                ret: Ret::Tentative | Ret::Conflicted,
                rid,
                ..
            } => Some(*rid),
            _ => None,
        })
        .collect();
    let skipped: Vec<(Key, Rid)> = s
        .history
        .iter()
        .filter_map(|e| match e {
            HEvt::Return {
                ret: Ret::Skipped,
                rid,
                key,
                ..
            } => Some((*key, *rid)),
            _ => None,
        })
        .collect();
    let mut keys: Vec<Key> = s
        .history
        .iter()
        .map(|e| match e {
            HEvt::Invoke { key, .. } | HEvt::Return { key, .. } => *key,
        })
        .collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        let mut tester = LinearizabilityTester::<usize, KeySpec>::new(KeySpec {
            present: m.initial_present.contains(&k),
        });
        for (idx, evt) in s.history.iter().enumerate() {
            let r = match evt {
                HEvt::Invoke { key, .. } | HEvt::Return { key, .. } if *key != k => continue,
                HEvt::Invoke { rid, .. } if skipped.contains(&(k, *rid)) => continue,
                HEvt::Return {
                    ret: Ret::Skipped, ..
                } => continue,
                HEvt::Invoke { op, rid, .. } if tentative.contains(rid) => {
                    tester.on_invoke(TENTATIVE_THREAD_BASE + idx, *op)
                }
                HEvt::Invoke { node, op, .. } => tester.on_invoke(*node as usize, *op),
                HEvt::Return {
                    ret: Ret::Tentative | Ret::Conflicted,
                    ..
                } => continue,
                HEvt::Return { node, ret, .. } => tester.on_return(*node as usize, *ret),
            };
            if r.is_err() {
                return false;
            }
        }
        if !tester.is_consistent() {
            return false;
        }
    }
    true
}

/// Every live replica satisfies the `deps` of every record it holds.
fn prop_causal_cut(_m: &DelegModel, s: &State) -> bool {
    for (i, n) in s.nodes.iter().enumerate() {
        if !n.alive {
            continue;
        }
        let entries = s.entries_of(i);
        for e in &entries {
            if e.deps.seq > n.applied {
                return false;
            }
            for &(st, idx) in &e.deps.pend {
                if State::applied_of(&entries, st) < idx {
                    return false;
                }
            }
        }
    }
    true
}

/// Every logged write satisfied its precondition at its position.
fn prop_log_valid(m: &DelegModel, s: &State) -> bool {
    let mut v = initial_view(m);
    for e in &s.log {
        if let Rec::Write { op, .. } = e.rec {
            let (ret, next) = eval(v, op);
            if ret != Ret::Ok {
                return false;
            }
            v = next;
        }
    }
    true
}

fn prop_exactly_once(s: &State) -> bool {
    let mut rids: Vec<Rid> = s
        .log
        .iter()
        .filter_map(|e| match e.rec {
            Rec::Write { rid, .. } => Some(rid),
            _ => None,
        })
        .collect();
    let n = rids.len();
    rids.sort();
    rids.dedup();
    rids.len() == n
}

fn prop_durable_never_lost(s: &State) -> bool {
    s.durable_rids.iter().all(|rid| {
        s.log
            .iter()
            .any(|e| matches!(e.rec, Rec::Write { rid: r, .. } if r == *rid))
            || s.nodes.iter().any(|n| {
                n.alive
                    && (n.acked.iter().any(|a| a.rid == *rid)
                        || n.replays.iter().any(|(r, _)| r == rid)
                        || n.tail.iter().any(|t| t.rid == *rid)
                        || n.deleg
                            .as_ref()
                            .is_some_and(|d| d.spec.iter().any(|t| t.rid == *rid))
                        || n.root.as_ref().is_some_and(|r| {
                            r.buffer.iter().any(|t| t.rid == *rid)
                                || r.journal.iter().any(
                                    |e| matches!(e.rec, Rec::Write { rid: x, .. } if x == *rid),
                                )
                        }))
            })
    })
}

fn quiescent(s: &State) -> bool {
    s.net.is_empty()
        && s.nodes.iter().all(|n| {
            !n.alive
                || (matches!(n.op, NodeOp::Idle)
                    && n.replays.is_empty()
                    && n.applied as usize == s.log.len()
                    && n.root.as_ref().is_none_or(|r| {
                        r.journal.is_empty() && r.buffer.is_empty() && r.held.is_empty()
                    })
                    && n.deleg
                        .as_ref()
                        .is_none_or(|d| d.spec.is_empty() && d.parked.is_empty()))
        })
}

fn prop_converged(m: &DelegModel, s: &State) -> bool {
    if !quiescent(s) {
        return true;
    }
    let log_view = fold(initial_view(m), &s.log);
    s.nodes
        .iter()
        .enumerate()
        .all(|(i, n)| !n.alive || s.view_of(m, i) == log_view)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ancestor_walk_finds_the_innermost_delegation() {
        let parents = vec![NONE, 0, 0, 1];
        let table = vec![
            Deleg {
                dir: 1,
                node: 1,
                gen: 1,
            },
            Deleg {
                dir: 2,
                node: 2,
                gen: 2,
            },
        ];
        assert_eq!(owner_of_dir(&table, &parents, 3).map(|d| d.gen), Some(1));
        assert_eq!(owner_of_dir(&table, &parents, 0), None);
        // A move inside one subtree has one owner; across two, none.
        assert!(owner_of_op(&table, &parents, WriteOp::Move(key(1, 0), key(3, 0))).is_some());
        assert!(owner_of_op(&table, &parents, WriteOp::Move(key(1, 0), key(2, 0))).is_none());
        assert!(owner_of_op(&table, &parents, WriteOp::Move(key(1, 0), key(0, 0))).is_none());
        assert_eq!(
            delegations_touched(&table, &parents, WriteOp::Move(key(1, 0), key(2, 0))).len(),
            2
        );
    }

    #[test]
    fn positions_join_and_dominate_per_stream() {
        let mut a = Pos {
            seq: 2,
            pend: vec![(9, 1)],
        };
        let b = Pos {
            seq: 1,
            pend: vec![(9, 3), (10, 1)],
        };
        let j = a.join(&b);
        assert_eq!(j.seq, 2);
        assert_eq!(j.pend, vec![(9, 3), (10, 1)]);
        assert!(j.dominates(&a) && j.dominates(&b) && !a.dominates(&b));
        a.raise(10, 2);
        assert!(!j.dominates(&a));
    }
}
