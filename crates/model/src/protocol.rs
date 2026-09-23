//! The `stateright::Model` implementation itself: state, actions,
//! transitions and properties for the authority protocol variants of plan
//! 30 (`Today` from §M1, `ExactlyOnce` from §M2, `Recovery` from §M3a
//! and §M3b). See the crate-level docs for the action → code mapping
//! table and the list of deliberate simplifications.

use crate::namespace::{
    eval, force_apply, present, with_presence, DirState, Errno, NamespaceSpec, NsOp, NsRet, Record,
};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stateright::{Model, Property};

pub type NodeId = u8;
pub type Epoch = u8;
pub type Seq = u8;
pub type Tick = u8;
pub type MsgId = u32;
pub type Incarnation = u8;

/// A request identity (plan 30 §M2, `Rid { node, incarnation, seq }`).
/// `incarnation` is bumped on every `Restart` (the volatile in-memory
/// `seq` counter is lost across a crash, so the persisted incarnation is
/// what keeps a post-crash rid from ever colliding with a pre-crash one);
/// `seq` is per-incarnation and monotonic, allocated once per client op
/// and kept across every retry of that op (forward, handoff, or lease
/// path) — never reallocated except for a genuinely new op (a
/// `SetManifest` rebase is a new op with a new rid, per the plan text).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rid {
    pub node: NodeId,
    pub incarnation: Incarnation,
    pub seq: Seq,
}

/// A logged record together with the rid of the op that produced it, if
/// any (`LogRecord::Completed { rid }` shipping in the same transaction
/// as the op's own records — modeled as riding along with the record
/// itself rather than as a separate log entry, since nothing here needs
/// to address a `Completed` record independently of the record it
/// completes). `Today` always sets this (rid allocation is unconditional,
/// per `mutate_op_rebasable`), but only `ExactlyOnce` ever reads it back.
pub type Logged = (Option<Rid>, Record);

/// One unshipped journal row: the record a holder executed, plus what
/// plan 30 §M3b's holder-side capture stores for it
/// (`Meta::begin_local`/`finish_local`, a `spec` entry of kind
/// `SpecKind::Local`).
///
/// `epoch` and `before` are only captured under `Protocol::Recovery`
/// (both stay `0`/`false` under `Today`/`ExactlyOnce`, which have no
/// holder-side capture, so their reachable state spaces are unchanged):
/// - `epoch` is the tenure the entry executed under. A tailed segment at
///   a higher epoch, or a `Renew` that finds the lease moved, proves that
///   tenure is over (the entry is stranded: rolled back and queued for
///   replay by rid).
/// - `before` is the entry's before-image: whether `rec`'s one name was
///   present in the node's replica view immediately before this entry
///   applied (every modeled record touches exactly one name, so one bit
///   is the whole `Option<value>` of the real `before: [(key,
///   Option<value>)]`). Captured at execute time from `node_replica`, and
///   re-captured (`recapture_before_images`) whenever something beneath
///   the journal changes — a tailed segment, or a shadow retired or
///   stranded — the model's form of the real rollback-apply-redo, which
///   re-captures every redone entry's before-image. `Publish` substitutes
///   the earliest one per name (`log_prefix_view`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JournalEntry {
    pub rid: Option<Rid>,
    pub rec: Record,
    pub epoch: Epoch,
    pub before: bool,
}

/// A shipped log segment: `seq -> (epoch, records)` in the crate doc's
/// terms. Corresponds to the envelope `shipper.rs::encode`/`decode`
/// produce and to one `LogStore::put_segment` slot.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Segment {
    pub node: NodeId,
    pub epoch: Epoch,
    pub records: Vec<Logged>,
}

/// The S3 lease register (`leases/p0.json`, `store-s3::Lease`), modeled
/// with etag-CAS semantics collapsed into "claimable" logic rather than
/// an explicit ETag value: any node attempting `AcquireLease` when the
/// register is claimable is exactly the CAS race the real object's ETag
/// arbitrates, so nothing is lost by not carrying a literal tag.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseReg {
    pub holder: Option<NodeId>,
    pub epoch: Epoch,
    pub expires_at: Tick,
    pub released: bool,
}

/// Where a node's own in-flight client op currently stands. Exactly one
/// `Invoke`/`Return` pair is recorded per op regardless of how many of
/// these phases it passes through: the retries below are invisible
/// machinery inside a single FUSE syscall (`mutate_op_rebasable` calls
/// forward *once*, then falls back to lease acquisition once), matching
/// what a real caller observes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Sent to `holder` as `MutateRequest{corr}`, awaiting `MutateReply`.
    WaitingReply { corr: MsgId, holder: NodeId },
    /// Forward failed/timed out (or there was no holder to ask): must
    /// acquire the lease, then execute locally.
    NeedsLease,
    /// Asked the (still-believed-live) holder for a fast handoff.
    WaitingHandoff { corr: MsgId, holder: NodeId },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientOp {
    pub op: NsOp,
    pub phase: Phase,
    /// Allocated once, at `ClientInvoke`, and kept unchanged across every
    /// retry this op goes through (plan 30 §M2).
    pub rid: Rid,
    /// How many times `RetryForward` has fired for this op. Plan 30 §M2
    /// caps same-rid forward retries at "three attempts or ~2s" before
    /// falling to the lease path; modeling that count (rather than
    /// letting the checker retry indefinitely up to the generic
    /// `within_boundary` cap) keeps the state space a small, fixed
    /// multiple of `Today`'s instead of growing with every extra tick the
    /// checker is willing to spend retrying the same op.
    pub attempts: u8,
}

/// Plan 30 §M2: "the same holder, with backoff (three attempts or 2s)".
const MAX_FORWARD_RETRIES: u8 = 3;

/// What a forwarded mutation request resolves to
/// (`forward.rs::MutateOutcome`, reduced: `Busy` and `NotHolder` are the
/// same fallback for the caller — see `fusefs.rs`'s `Busy | NotHolder`
/// arm — so this model only has `NotHolder`).
///
/// `Accepted` carries the epoch the answering holder executed (or first
/// executed) the op under (`MutateOutcome::Accepted { epoch, .. }`): a
/// `Recovery` shadow remembers it, so a later segment or takeover at a
/// higher epoch recognizes the shadow as stranded (`ShadowEntry::epoch`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Accepted(Record, Epoch),
    Errno(Errno),
    NotHolder,
}

/// One outstanding requester shadow (plan 30 §M3a's `spec` entry of kind
/// `Shadow { rid, epoch }`, reduced to what the model's rules read): the
/// op's rid (retirement and replay are by rid under `Recovery`), the
/// epoch that accepted it (stranding compares it against applied
/// segments and takeovers), and the record it installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShadowEntry {
    pub rid: Rid,
    pub epoch: Epoch,
    pub rec: Record,
}

/// `Recovery` only: a stranded op queued for replay by rid
/// (`Meta::pending_replays`), in original order. `inflight` is the `corr`
/// of the replay request currently awaiting a reply, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplayEntry {
    pub rid: Rid,
    pub op: NsOp,
    pub inflight: Option<MsgId>,
    /// How many replay requests this entry has sent. Capped at
    /// [`MAX_REPLAY_ATTEMPTS`] for the same reason `ClientOp::attempts`
    /// is: the real drain retries forever, but every retry mints a fresh
    /// message id, so an uncapped retry loop only grows the state space
    /// without reaching a new kind of state.
    pub attempts: u8,
}

/// Replay requests one stranded op may send before the model stops
/// offering `ReplayStranded` for it (see [`ReplayEntry::attempts`]).
const MAX_REPLAY_ATTEMPTS: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MsgBody {
    MutateReq {
        from: NodeId,
        to: NodeId,
        corr: MsgId,
        op: NsOp,
        rid: Rid,
    },
    MutateRep {
        to: NodeId,
        corr: MsgId,
        outcome: Outcome,
    },
    HandoffReq {
        from: NodeId,
        to: NodeId,
        corr: MsgId,
    },
    HandoffRep {
        to: NodeId,
        corr: MsgId,
        ok: bool,
    },
}

/// One P2P message in flight. `id` is the transport-level identity used
/// to target this exact message for delivery or loss (the lossy,
/// reordering network); `corr`, carried inside the body, is the logical
/// request id a reply is matched against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Envelope {
    pub id: MsgId,
    pub body: MsgBody,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub alive: bool,
    pub paused: bool,
    /// This node's cached belief that it holds authority, at this epoch
    /// (`LeaseView`/`LeaseKeeper::ship_epoch`). Only ever invalidated by
    /// `Renew` noticing a mismatch against the shared register — a live
    /// node trusts its cache between renewals, exactly like the FUSE fast
    /// path trusts `LeaseView::usable()` without going to S3.
    pub held_epoch: Option<Epoch>,
    /// Highest log seq this node has tailed (`Meta::applied_seq`).
    pub applied_seq: Seq,
    /// Unshipped local records, in order (the fjall journal), each with
    /// its `Recovery` capture (see [`JournalEntry`]).
    pub journal: Vec<JournalEntry>,
    /// Outstanding shadows (`forward.rs::apply_accepted`), oldest first.
    /// `Today`/`ExactlyOnce` never hold more than one at a time (one
    /// client op per node); `Recovery` can briefly hold a replayed op's
    /// shadow next to a new client op's.
    pub shadows: Vec<ShadowEntry>,
    /// `Recovery` only: stranded ops awaiting replay by rid, oldest first
    /// (`Meta::pending_replays`). Durable, like the `spec` keyspace.
    pub replays: Vec<ReplayEntry>,
    pub client_op: Option<ClientOp>,
    /// Ops this node's (single, serialized) FUSE-calling client will
    /// still issue, oldest first.
    pub pending_ops: Vec<NsOp>,
    /// This incarnation's persisted counter (`local` keyspace), bumped on
    /// `Restart` before the node serves again (plan 30 §M2).
    pub incarnation: Incarnation,
    /// The next `seq` to allocate within the current incarnation. Reset
    /// to 0 on `Restart` — it is the *incarnation* bump, not a surviving
    /// counter, that keeps a post-restart rid unique.
    pub next_seq: Seq,
    /// The holder's in-memory "recent outcomes" map (plan 30 §M2): rids
    /// this node has executed as holder but not yet shipped, so a retry
    /// against the same still-live holder gets an identical reply without
    /// re-executing. Volatile: lost on `Restart`, unlike the durable,
    /// log-derived `completed` table (see `rid_completed_record`). The
    /// epoch is the one the op executed under, answered back on a retry.
    /// Under `Recovery` a Local entry rolled back on deposition takes its
    /// `recent` outcome with it (`strand_local`).
    pub recent: Vec<(Rid, Record, Epoch)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HistEvt {
    Invoke(NodeId, NsOp),
    Return(NodeId, NsRet),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub tick: Tick,
    pub lease: LeaseReg,
    /// Slots `1..=max_seq`; index 0 is unused padding so `Seq` can serve
    /// directly as an index.
    pub log: Vec<Option<Segment>>,
    /// The last-published commit: `(published_state, claimed applied
    /// position)` (`mtree_publish::Commit`).
    pub commit: Option<(DirState, Seq)>,
    pub nodes: Vec<Node>,
    pub network: Vec<Envelope>,
    pub history: Vec<HistEvt>,
    pub next_id: MsgId,
    pub crashes_used: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    ClientInvoke(NodeId),
    DeliverForwardRequest(MsgId),
    DeliverForwardReply(MsgId),
    ForwardTimeout(NodeId),
    /// `ExactlyOnce` only: retry the same rid to whoever `state.lease`
    /// currently names as holder — the same holder if it has not
    /// changed, or the redirected one if it has (plan 30 §M2's "retry
    /// the same rid... 1. the same holder... 2. a redirected holder").
    RetryForward(NodeId),
    RequestHandoff(NodeId),
    DeliverHandoffRequest(MsgId),
    DeliverHandoffReply(MsgId),
    /// `Recovery` only: replay the oldest stranded op by rid — locally if
    /// this node holds a usable lease, else to whoever `state.lease`
    /// names (`recovery::drain_pending_replays`).
    ReplayStranded(NodeId),
    Tail(NodeId),
    Ship(NodeId),
    Renew(NodeId),
    AcquireLease(NodeId),
    Publish(NodeId),
    Crash(NodeId),
    Restart(NodeId),
    Pause(NodeId),
    Resume(NodeId),
    DropMessage(MsgId),
}

/// Selects the modeled protocol variant. `Today` (M1) and `ExactlyOnce`
/// (M2) exist so far; later milestones (per plan 30 §4) add siblings here
/// rather than replacing them, each changing a specific, documented
/// subset of the transitions below:
/// - `ExactlyOnce` (M2): every rid is checked against the log-derived
///   `completed` table plus the holder's in-memory `recent` map at the
///   top of `DeliverForwardRequest`, and against `completed` alone (the
///   coverage rule: `AcquireLease` is only offered once tailed to head)
///   before `AcquireLease` would otherwise re-execute a `NeedsLease` op.
///   `RetryForward` is also only ever offered under this variant — see
///   the comments at all three call sites.
/// - `Recovery` (M3a, the requester side of plan 30 §M3), layered on
///   `ExactlyOnce` (replay by rid is only exactly-once because of M2's
///   dedup):
///   - a shadow carries the epoch that accepted it and retires by rid
///     (`Completed { rid }` in an applied segment), not by record
///     equality;
///   - applying a segment whose epoch is higher than an outstanding
///     shadow's strands the shadow: it is rolled back (dropped from the
///     overlay) and queued for replay by rid (`tail_one`);
///   - `ReplayStranded` replays the oldest queued op through whoever
///     holds the lease, or executes it locally when this node does;
///   - the takeover gate: `AcquireLease` strands every shadow below the
///     new epoch and executes every queued replay locally, in order,
///     before anything validates against the replica;
///   - `Publish` is not offered while a shadow is outstanding;
///   - a requester never installs a shadow for a rid its applied log
///     already completed (the reply raced the requester's own tail);
///   - authority is time-bounded (`authority`): a holder whose lease
///     expired or moved may not execute or ship, as
///     `LeaseView::usable`/`LeaseKeeper::ship_epoch` enforce. `Today` and
///     `ExactlyOnce` keep the looser "trust `held_epoch` until `Renew`"
///     abstraction M1 shipped with.
///
///   M3b adds the holder side on the same variant:
///   - holder-side capture: every journal entry records the epoch it
///     executed under and its before-image ([`JournalEntry`]); an entry
///     retires when it ships, and a tailed segment re-captures the
///     before-images of the entries that survive it (rollback, apply,
///     redo);
///   - `Publish` is offered even while the journal is non-empty, and
///     publishes the node's replica with every name an unshipped entry
///     touches replaced by that name's earliest before-image
///     (`log_prefix_view`), at `applied_seq`. Still not offered while a
///     shadow is outstanding. `AuthorityModel::raw_holder_publish` is a
///     non-vacuity knob that publishes the raw replica instead;
///   - deposition rolls back, then replays by rid: a `Renew` that finds
///     the lease moved, or a tailed (non-fenced) segment at a higher
///     epoch than an entry's, removes those entries from the journal (and
///     their `recent` outcomes) and queues their ops, in journal order, on
///     the same `replays` queue stranded shadows use (`strand_local`). A
///     tail that proves the node's own tenure over also clears
///     `held_epoch`, as `recover_deposed` marks the lease lost;
///   - the takeover epoch marker: a takeover `AcquireLease` ships an
///     empty segment at its new epoch at the next log slot before the
///     gate runs (`ship_epoch_marker`), fencing late segments of the old
///     epoch and stranding third nodes' old-epoch shadows as soon as they
///     tail it, even if the new holder's own op is refused and ships
///     nothing. The gate also strands the node's own Local entries below
///     the new epoch;
///   - a forward reply at an epoch below the one this node holds is not
///     installed as a shadow (`reply_superseded`); its op is queued for
///     replay by rid, unless the applied log already completed it;
///   - dedup (`rid_completed_record`) also consults the node's own
///     unshipped journal, which writes `completed` in the op's own
///     transaction in the real code (`holder_execute`'s
///     `completed_position` check), so a deposed holder's replay of an op
///     the new holder already re-executed is answered, not re-run.
///
///   The continuation-epoch path (`adopt_epoch_hold`) is not modeled.
/// - `Positions`/`Backup`/`FlexEpochs`/`Delegation` (M6/M9/M10/M11) each
///   extend `State`/`Action` further; none are added speculatively here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    Today,
    ExactlyOnce,
    Recovery,
}

impl Protocol {
    /// Rid-keyed dedup (plan 30 §M2): `ExactlyOnce`, and `Recovery` on
    /// top of it.
    fn dedups(self) -> bool {
        matches!(self, Protocol::ExactlyOnce | Protocol::Recovery)
    }
}

/// A model instance: which protocol variant, how many nodes, the bounds
/// that keep the state space finite, and the fixed client workload.
#[derive(Clone, Debug)]
pub struct AuthorityModel {
    pub protocol: Protocol,
    pub n_nodes: u8,
    pub max_tick: Tick,
    pub max_seq: Seq,
    pub lease_ttl: Tick,
    pub max_crashes: u8,
    pub allow_restart: bool,
    pub allow_pause: bool,
    pub allow_lossy: bool,
    /// Same-rid forward retries offered per client op (`RetryForward`,
    /// dedup variants only). Defaults to plan 30 §M2's three; a config
    /// that is about something else (bug B's stranding shape) may lower
    /// it to keep exhaustive exploration inside the test budget.
    pub max_forward_retries: u8,
    pub initial_holder: Option<NodeId>,
    pub initial_expiry: Tick,
    pub workload: Vec<(NodeId, NsOp)>,
    /// `within_boundary`'s message-id backstop (see its doc comment).
    /// Defaults to 200, generous enough for `Today`/`ExactlyOnce`'s
    /// smaller action set. `Recovery` adds a whole extra dimension
    /// (shadows/replays and `ReplayStranded`) that multiplies branching
    /// per state; a config that needs exhaustive `Recovery` exploration
    /// inside the test budget lowers this explicitly rather than relying
    /// on the generous default meant for the other two variants.
    pub max_next_id: MsgId,
    /// `Recovery` only, a non-vacuity knob (default `false`): publish the
    /// node's raw replica, speculative unshipped journal included, instead
    /// of the before-image-substituted log-prefix view. With it on, a
    /// holder that publishes between executing and shipping violates
    /// `commits_are_log_prefixes`, which is how a test shows that property
    /// actually constrains M3b's holder publish (it is not true merely by
    /// construction).
    pub raw_holder_publish: bool,
}

impl AuthorityModel {
    pub fn new(n_nodes: u8) -> Self {
        Self {
            protocol: Protocol::Today,
            n_nodes,
            max_tick: 4,
            max_seq: 3,
            lease_ttl: 2,
            max_crashes: 0,
            allow_restart: false,
            allow_pause: false,
            allow_lossy: true,
            max_forward_retries: MAX_FORWARD_RETRIES,
            initial_holder: None,
            initial_expiry: 0,
            workload: Vec::new(),
            max_next_id: 200,
            raw_holder_publish: false,
        }
    }

    pub fn with_protocol(mut self, p: Protocol) -> Self {
        self.protocol = p;
        self
    }

    pub fn with_initial_holder(mut self, id: NodeId, expiry: Tick) -> Self {
        self.initial_holder = Some(id);
        self.initial_expiry = expiry;
        self
    }

    pub fn with_max_tick(mut self, t: Tick) -> Self {
        self.max_tick = t;
        self
    }

    pub fn with_max_seq(mut self, s: Seq) -> Self {
        self.max_seq = s;
        self
    }

    pub fn with_lease_ttl(mut self, t: Tick) -> Self {
        self.lease_ttl = t;
        self
    }

    pub fn with_max_crashes(mut self, c: u8) -> Self {
        self.max_crashes = c;
        self
    }

    pub fn with_restart(mut self, b: bool) -> Self {
        self.allow_restart = b;
        self
    }

    pub fn with_pause(mut self, b: bool) -> Self {
        self.allow_pause = b;
        self
    }

    pub fn with_lossy(mut self, b: bool) -> Self {
        self.allow_lossy = b;
        self
    }

    pub fn with_forward_retries(mut self, n: u8) -> Self {
        self.max_forward_retries = n;
        self
    }

    pub fn with_op(mut self, node: NodeId, op: NsOp) -> Self {
        self.workload.push((node, op));
        self
    }

    pub fn with_max_next_id(mut self, n: MsgId) -> Self {
        self.max_next_id = n;
        self
    }

    pub fn with_raw_holder_publish(mut self, b: bool) -> Self {
        self.raw_holder_publish = b;
        self
    }
}

impl AuthorityModel {
    /// `forward.rs::apply_accepted` → `Meta::install_shadow`: apply an
    /// accepted op ahead of the log. Under `Recovery` the install is
    /// skipped when this replica's applied log already completed the rid
    /// (the reply lost a race with the requester's own tail), since the
    /// effect is then already part of the log prefix and re-applying it
    /// on top of later records would move the replica backwards.
    fn install_shadow(&self, s: &mut State, id: NodeId, rid: Rid, epoch: Epoch, rec: Record) {
        if self.protocol == Protocol::Recovery && rid_in_applied_log(s, id, rid) {
            return;
        }
        s.nodes[id as usize]
            .shadows
            .push(ShadowEntry { rid, epoch, rec });
    }

    /// Execute a stranded op locally, by rid, as the holder
    /// (`recovery::replay_locally`): skipped if the rid already took
    /// effect (M2 dedup), journaled if it still validates, and otherwise
    /// refused — the real code materializes that refusal as a conflict
    /// copy; the model just drops it.
    fn replay_locally(&self, s: &mut State, id: NodeId, rid: Rid, op: NsOp) {
        if rid_completed_record(s, id, rid, self.protocol).is_some() {
            return;
        }
        let base = node_replica(s, id);
        let (ret, _) = eval(base, op);
        if ret == NsRet::Ok {
            let epoch = s.nodes[id as usize].held_epoch.unwrap_or(0);
            self.journal_push(s, id, rid, op, epoch);
            s.nodes[id as usize].recent.push((rid, op, epoch));
        }
    }

    /// Journal one record `id` just executed under `epoch`. Under
    /// `Recovery` this is also plan 30 §M3b's holder-side capture
    /// (`Meta::begin_local`/`finish_local`): the entry's before-image is
    /// read from the node's replica view as it stands immediately before
    /// the entry applies, which already includes every earlier journal
    /// entry. The other variants capture nothing (see [`JournalEntry`]).
    fn journal_push(&self, s: &mut State, id: NodeId, rid: Rid, rec: Record, epoch: Epoch) {
        let entry = if self.protocol == Protocol::Recovery {
            let view = node_replica(s, id);
            JournalEntry {
                rid: Some(rid),
                rec,
                epoch,
                before: present(view, rec.name()),
            }
        } else {
            JournalEntry {
                rid: Some(rid),
                rec,
                epoch: 0,
                before: false,
            }
        };
        s.nodes[id as usize].journal.push(entry);
    }

    /// Plan 30 §M3b, `Recovery` only: an `Accepted` reply executed under
    /// an epoch lower than the one `id` holds (`held_epoch`, the model's
    /// `Meta::holder_epoch`) is already stranded — a higher epoch exists,
    /// this node's own — so `Meta::install_shadow` refuses it rather than
    /// letting the new holder validate against an older epoch's effect
    /// until the next drain tick strands it. The caller queues the op for
    /// replay by rid instead. A stale `held_epoch` (deposition not yet
    /// noticed) only ever errs towards replaying, which dedup makes safe.
    fn reply_superseded(&self, s: &State, id: NodeId, epoch: Epoch) -> bool {
        self.protocol == Protocol::Recovery
            && s.nodes[id as usize].held_epoch.is_some_and(|h| h > epoch)
    }
}

fn is_live(state: &State, id: NodeId) -> bool {
    let n = &state.nodes[id as usize];
    n.alive && !n.paused
}

/// Replay `log[1..=upto]` applying epoch fencing the way
/// `shipper.rs::apply_decoded_segment` does: a segment whose epoch is
/// lower than the highest epoch already applied is skipped (but still
/// counts toward that running maximum — a no-op in practice since it was
/// already below it). Returns the resulting directory state, the running
/// maximum epoch (the latter is what a node's own tailer would have in
/// `self.parts[part].max_epoch`), and every rid completed by an applied
/// (non-fenced) segment along the way — the log-derived half of plan 30
/// §M2's `completed` keyspace (`replay.rs::apply_one`'s `Completed`
/// arm, folded into the same pass since nothing here ever needs the two
/// separately).
#[allow(clippy::type_complexity)]
fn log_fold(log: &[Option<Segment>], upto: Seq) -> (DirState, Epoch, Vec<(Rid, Record, Epoch)>) {
    let mut dir: DirState = 0;
    let mut max_epoch: Epoch = 0;
    let mut completions = Vec::new();
    for seq in 1..=upto {
        if let Some(seg) = &log[seq as usize] {
            let fenced = seg.epoch > 0 && seg.epoch < max_epoch;
            if !fenced {
                for (rid, r) in &seg.records {
                    dir = force_apply(dir, *r);
                    if let Some(rid) = rid {
                        completions.push((*rid, *r, seg.epoch));
                    }
                }
                max_epoch = max_epoch.max(seg.epoch);
            }
        }
    }
    (dir, max_epoch, completions)
}

fn log_state_and_epoch(log: &[Option<Segment>], upto: Seq) -> (DirState, Epoch) {
    let (dir, max_epoch, _) = log_fold(log, upto);
    (dir, max_epoch)
}

/// Whether `rid` has already completed as far as `id` can tell: either a
/// segment it has tailed carries the completion (durable, survives
/// `Restart` — the `completed` keyspace), or `id` itself executed it as
/// holder and still has it in the volatile `recent` map (not yet
/// shipped). Only ever consulted by the dedup variants. Returns the
/// record the rid produced and the epoch it took effect under (the
/// segment's, or the executing tenure's), so the caller can answer with
/// an identical outcome rather than deriving one afresh — and so a
/// `Recovery` shadow installed from a dedup answer is never stranded by
/// a segment that precedes the one completing it.
///
/// Under `Recovery` (plan 30 §M3b) the node's own unshipped journal
/// counts too: the real journal writes `completed` in the op's own
/// transaction, so `holder_execute`'s `completed_position` check sees a
/// rid this tenure executed through any path — not only the forwarded
/// ones `recent` remembers, but also the holder's own client ops and the
/// takeover path's re-execution. That is what keeps a deposed holder's
/// replay of an op the new holder already re-executed (the M2 coverage
/// gap: the old holder's journal was never visible to the new one) from
/// running twice. A rolled-back entry leaves the journal, and with it
/// this answer.
fn rid_completed_record(
    state: &State,
    id: NodeId,
    rid: Rid,
    protocol: Protocol,
) -> Option<(Record, Epoch)> {
    let node = &state.nodes[id as usize];
    let (_, _, completions) = log_fold(&state.log, node.applied_seq);
    completions
        .into_iter()
        .find(|(r, _, _)| *r == rid)
        .map(|(_, rec, epoch)| (rec, epoch))
        .or_else(|| {
            if protocol != Protocol::Recovery {
                return None;
            }
            node.journal
                .iter()
                .find(|e| e.rid == Some(rid))
                .map(|e| (e.rec, e.epoch))
        })
        .or_else(|| {
            node.recent
                .iter()
                .find(|(r, _, _)| *r == rid)
                .map(|(_, rec, epoch)| (*rec, *epoch))
        })
}

/// Whether `id`'s applied log prefix already carries `rid`'s completion
/// (the `completed` keyspace alone, without the holder's `recent` map).
/// `Recovery`'s requester consults it before installing a shadow: a
/// reply that arrives after the requester already tailed the segment
/// completing it must not be re-applied on top of later records
/// (`Meta::install_shadow`).
fn rid_in_applied_log(state: &State, id: NodeId, rid: Rid) -> bool {
    let node = &state.nodes[id as usize];
    let (_, _, completions) = log_fold(&state.log, node.applied_seq);
    completions.iter().any(|(r, _, _)| *r == rid)
}

/// The epoch `id` may execute and ship under right now, if any.
///
/// `Today`/`ExactlyOnce` trust the node's cached `held_epoch` until a
/// `Renew` notices deposition (M1's abstraction). `Recovery` also models
/// the time bound the real `LeaseView::usable`/`LeaseKeeper::ship_epoch`
/// enforce: the lease register must still name this node at this epoch,
/// unreleased and unexpired. Without it a holder whose lease expired
/// could still execute forwarded ops and ship them after a takeover,
/// which the real code forbids and which would make `Recovery`'s
/// takeover-time replay look like a double execution.
fn authority(state: &State, id: NodeId, protocol: Protocol) -> Option<Epoch> {
    let held = state.nodes[id as usize].held_epoch?;
    if protocol != Protocol::Recovery {
        return Some(held);
    }
    let lease = &state.lease;
    (lease.holder == Some(id)
        && lease.epoch == held
        && !lease.released
        && lease.expires_at > state.tick)
        .then_some(held)
}

/// The highest occupied log slot. Always contiguous from 1 (see the
/// crate doc: every successful `put_segment` targets the writer's own
/// `applied_seq + 1`, and a collision is absorbed by tailing instead of
/// writing elsewhere), so this doubles as "how many slots exist".
fn log_head(log: &[Option<Segment>]) -> Seq {
    log.iter()
        .rposition(|s| s.is_some())
        .map(|i| i as Seq)
        .unwrap_or(0)
}

/// What a node's replica currently shows: the log-derived state at its
/// own `applied_seq`, overlaid with its outstanding shadows (forwarded
/// ops' replies, applied ahead of the log — `forward.rs::apply_accepted`)
/// and its own unshipped journal (a holder's local writes not yet
/// shipped).
fn node_replica(state: &State, id: NodeId) -> DirState {
    let mut dir = replica_below_journal(state, id);
    for e in &state.nodes[id as usize].journal {
        dir = force_apply(dir, e.rec);
    }
    dir
}

/// `node_replica` without the node's own journal: the base its first
/// journal entry applies on top of.
fn replica_below_journal(state: &State, id: NodeId) -> DirState {
    let node = &state.nodes[id as usize];
    let (mut dir, _) = log_state_and_epoch(&state.log, node.applied_seq);
    for sh in &node.shadows {
        dir = force_apply(dir, sh.rec);
    }
    dir
}

/// Plan 30 §M3b's redo step, `Recovery` only: re-capture every journal
/// entry's before-image against what now lies beneath it. The real code
/// gets here by rolling the Local entries back, applying whatever changed
/// beneath them (a tailed segment, a retired or stranded shadow) and
/// redoing them, which captures each afresh; the model's replica is a
/// fold computed on demand, so only the captured bits need refreshing.
///
/// Only called where the real code rolls back and redoes (`tail_one`,
/// the takeover gate), never before `Publish`: a stale before-image at
/// publish time is exactly what `commits_are_log_prefixes` would catch.
/// The model folds shadows *beneath* the journal regardless of arrival
/// order (the real `spec` log orders by `spec_seq`), so a shadow
/// installed on top of Local entries leaves their bits describing a
/// different base than `node_replica` folds; that is harmless because
/// `Publish` is never offered while a shadow is outstanding, and every
/// path that removes a shadow re-captures here.
fn recapture_before_images(s: &mut State, id: NodeId) {
    let mut view = replica_below_journal(s, id);
    for e in &mut s.nodes[id as usize].journal {
        e.before = present(view, e.rec.name());
        view = force_apply(view, e.rec);
    }
}

/// Plan 30 §M3b holder publish (`mtree_publish::plan_from_dirty` over
/// `Meta::publish_basis_at`): the node's replica with every name an
/// unshipped journal entry touches replaced by that name's *earliest*
/// captured before-image. With no shadow outstanding (`Publish`'s
/// precondition) that is exactly the log-prefix state at `applied_seq`
/// — but it is computed here from what the replica shows plus the
/// captured bits, never by folding the log, so `commits_are_log_prefixes`
/// genuinely checks the capture and re-capture rules.
fn log_prefix_view(state: &State, id: NodeId) -> DirState {
    let mut dir = node_replica(state, id);
    let mut substituted: DirState = 0;
    for e in &state.nodes[id as usize].journal {
        let n = e.rec.name();
        if !present(substituted, n) {
            substituted = with_presence(substituted, n, true);
            dir = with_presence(dir, n, e.before);
        }
    }
    dir
}

/// Plan 30 §M3b, `Recovery` only: roll back every journal (Local) entry
/// whose epoch is below `below` — its tenure is over — and queue its op
/// for replay by rid, in original journal order, behind whatever is
/// already queued (`recovery::recover_deposed` → `Meta::strand_local`
/// feeding `pending_replays`). Its `recent` outcome goes too: a retry
/// must not be answered from an effect that no longer exists. Rolling
/// back is simply removing the entry, since `node_replica` folds the
/// journal live; the survivors (never an earlier entry: epochs along the
/// journal are non-decreasing) are re-captured by the caller.
fn strand_local(s: &mut State, id: NodeId, below: Epoch) {
    let node = &mut s.nodes[id as usize];
    let (stranded, kept): (Vec<JournalEntry>, Vec<JournalEntry>) =
        node.journal.iter().partition(|e| e.epoch < below);
    node.journal = kept;
    for e in stranded {
        if let Some(rid) = e.rid {
            node.recent.retain(|(r, _, _)| *r != rid);
            node.replays.push(ReplayEntry {
                rid,
                op: e.rec,
                inflight: None,
                attempts: 0,
            });
        }
    }
}

/// Apply exactly the next log slot to `id`'s tailer state
/// (`shipper.rs::apply_decoded_segment` → `Meta::apply_segment`): advance
/// `applied_seq` and, if the segment was not fenced out, retire the
/// shadows it confirms. `Today`/`ExactlyOnce` match by record equality
/// (the pre-M3a `shadow_retire_matching`); `Recovery` matches by rid and
/// then strands every remaining shadow whose epoch this segment
/// supersedes — rolled back (dropped from the overlay, which
/// `node_replica` reads live) and queued for replay by rid.
///
/// Plan 30 §M3b adds the holder side under `Recovery`: every journal
/// (Local) entry whose epoch the segment supersedes is stranded too
/// (`strand_local`, queued behind the shadows stranded by the same
/// segment, matching the model's shadows-beneath-journal fold order),
/// a `held_epoch` below the segment's is dropped (the tenure is provably
/// over: `recover_deposed` marks the lease lost), and the surviving
/// entries' before-images are re-captured over the new base
/// (`recapture_before_images`: the real code rolls the Local entries
/// back, applies the segment, and redoes them, since a tailed segment
/// always precedes the holder's unshipped work in log order).
///
/// Panics if there is no next slot to apply; callers only invoke this
/// when `log_head > applied_seq`.
fn tail_one(s: &mut State, id: NodeId, protocol: Protocol) {
    let node = &s.nodes[id as usize];
    let (_, max_before) = log_state_and_epoch(&s.log, node.applied_seq);
    let target = node.applied_seq + 1;
    let seg = s.log[target as usize]
        .clone()
        .expect("caller checked log_head > applied_seq");
    let fenced = seg.epoch > 0 && seg.epoch < max_before;
    let node = &mut s.nodes[id as usize];
    node.applied_seq = target;
    if fenced {
        return;
    }
    if protocol == Protocol::Recovery {
        node.shadows
            .retain(|sh| !seg.records.iter().any(|(rid, _)| *rid == Some(sh.rid)));
        let (stranded, kept): (Vec<ShadowEntry>, Vec<ShadowEntry>) =
            node.shadows.iter().partition(|sh| sh.epoch < seg.epoch);
        node.shadows = kept;
        for sh in stranded {
            node.replays.push(ReplayEntry {
                rid: sh.rid,
                op: sh.rec,
                inflight: None,
                attempts: 0,
            });
        }
        if node.held_epoch.is_some_and(|h| h < seg.epoch) {
            node.held_epoch = None;
        }
        strand_local(s, id, seg.epoch);
        recapture_before_images(s, id);
    } else {
        node.shadows
            .retain(|sh| !seg.records.iter().any(|(_, r)| *r == sh.rec));
    }
}

/// Attempt to ship `id`'s whole journal as one segment
/// (`shipper.rs::ship_part`'s create-if-absent `put_segment`). A
/// collision (someone else already filled the target slot — a deposed
/// holder racing a new one) is absorbed by tailing that slot instead,
/// exactly once, matching `Err(AlreadyExists) => { self.tail_part(...); }`.
/// Returns whether the journal ended up empty (shipped, or already was).
fn try_ship_once(s: &mut State, id: NodeId, max_seq: Seq, protocol: Protocol) -> bool {
    let node = &s.nodes[id as usize];
    if node.journal.is_empty() {
        return true;
    }
    let target = node.applied_seq + 1;
    if target > max_seq {
        return false;
    }
    let Some(epoch) = authority(s, id, protocol) else {
        // `ship_epoch()` is `None`: nothing ships until a renewal.
        return false;
    };
    if s.log[target as usize].is_none() {
        // Shipping retires every Local entry (plan 30 §M3b: "a local
        // entry retires when its record ships"), before-images and all.
        let journal = std::mem::take(&mut s.nodes[id as usize].journal);
        let records = journal.iter().map(|e| (e.rid, e.rec)).collect();
        s.log[target as usize] = Some(Segment {
            node: id,
            epoch,
            records,
        });
        s.nodes[id as usize].applied_seq = target;
        true
    } else {
        tail_one(s, id, protocol);
        s.nodes[id as usize].journal.is_empty()
    }
}

/// A handoff's inline flush: keep absorbing collisions and re-attempting
/// the ship until the journal is empty or the log is full
/// (`node_runtime.rs`'s `HandOff` arm: `ship.sync_one` then `release()`).
fn flush_for_handoff(s: &mut State, id: NodeId, max_seq: Seq, protocol: Protocol) -> bool {
    loop {
        if s.nodes[id as usize].journal.is_empty() {
            return true;
        }
        let before = s.nodes[id as usize].applied_seq;
        if !try_ship_once(s, id, max_seq, protocol) && s.nodes[id as usize].applied_seq == before {
            // Log is full and no progress was made: decline the handoff,
            // keeping the lease ("declining is always safe").
            return false;
        }
    }
}

/// Plan 30 §M3b takeover epoch marker, `Recovery` only
/// (`Shipper::ship_epoch_marker`, called from `shipper::acquire_lease_for`
/// before `recovery::takeover_gate`): an empty segment at the new epoch
/// at the next free slot. If a late segment already took that slot, tail
/// it and retry at the next one, as `ship_part`'s collision absorption
/// does. Returns `false` if the log is full, in which case the takeover
/// cannot complete (`actions()` never offers it then).
///
/// Everything downstream of the marker follows from the ordinary rules:
/// `log_fold`'s fencing skips any later segment of an older epoch, and a
/// third node tailing the marker strands its older-epoch shadows (and a
/// deposed holder its Local entries) in `tail_one`, without waiting for
/// the new holder to ship an op of its own.
fn ship_epoch_marker(
    s: &mut State,
    id: NodeId,
    epoch: Epoch,
    max_seq: Seq,
    protocol: Protocol,
) -> bool {
    loop {
        let target = s.nodes[id as usize].applied_seq + 1;
        if target > max_seq {
            return false;
        }
        if s.log[target as usize].is_none() {
            s.log[target as usize] = Some(Segment {
                node: id,
                epoch,
                records: Vec::new(),
            });
            s.nodes[id as usize].applied_seq = target;
            return true;
        }
        tail_one(s, id, protocol);
    }
}

/// Whether `id` may claim the lease right now, and if so whether it is a
/// genuine takeover requiring tail-to-head first
/// (`LeaseKeeper::classify`'s `Plan::Claim{needs_tail}` /
/// `TailedToHead`). `None` means not claimable at all (someone else holds
/// an unexpired, unreleased lease).
fn claim_kind(state: &State, id: NodeId) -> Option<bool> {
    match state.lease.holder {
        None => Some(false),
        Some(h) if h == id && !state.lease.released => Some(false),
        Some(_) => {
            if state.lease.released || state.lease.expires_at <= state.tick {
                Some(true)
            } else {
                None
            }
        }
    }
}

impl Model for AuthorityModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let mut nodes = Vec::new();
        for _ in 0..self.n_nodes {
            nodes.push(Node {
                alive: true,
                paused: false,
                held_epoch: None,
                applied_seq: 0,
                journal: Vec::new(),
                shadows: Vec::new(),
                replays: Vec::new(),
                client_op: None,
                pending_ops: Vec::new(),
                incarnation: 0,
                next_seq: 0,
                recent: Vec::new(),
            });
        }
        for (n, op) in &self.workload {
            nodes[*n as usize].pending_ops.push(*op);
        }
        let lease = if let Some(h) = self.initial_holder {
            nodes[h as usize].held_epoch = Some(1);
            LeaseReg {
                holder: Some(h),
                epoch: 1,
                expires_at: self.initial_expiry,
                released: false,
            }
        } else {
            LeaseReg {
                holder: None,
                epoch: 0,
                expires_at: 0,
                released: false,
            }
        };
        vec![State {
            tick: 0,
            lease,
            log: vec![None; self.max_seq as usize + 1],
            commit: None,
            nodes,
            network: Vec::new(),
            history: Vec::new(),
            next_id: 0,
            crashes_used: 0,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.tick < self.max_tick {
            actions.push(Action::Tick);
        }
        for (i, node) in state.nodes.iter().enumerate() {
            let id = i as NodeId;
            if !node.alive {
                if self.allow_restart {
                    actions.push(Action::Restart(id));
                }
                continue;
            }
            if node.paused {
                actions.push(Action::Resume(id));
                continue;
            }
            if node.client_op.is_none() && !node.pending_ops.is_empty() {
                actions.push(Action::ClientInvoke(id));
            }
            if let Some(cop) = &node.client_op {
                match &cop.phase {
                    Phase::WaitingReply { .. } => actions.push(Action::ForwardTimeout(id)),
                    Phase::NeedsLease => {
                        match claim_kind(state, id) {
                            Some(needs_tail) => {
                                // Plan 30 §M3b: under `Recovery` a
                                // takeover ships its epoch marker into
                                // the next slot, so, like `Ship` at
                                // capacity, it is not offered once the
                                // log is full (`ship_epoch_marker`).
                                let marker_fits = !needs_tail
                                    || self.protocol != Protocol::Recovery
                                    || log_head(&state.log) < self.max_seq;
                                if (!needs_tail || node.applied_seq == log_head(&state.log))
                                    && marker_fits
                                {
                                    actions.push(Action::AcquireLease(id));
                                }
                            }
                            // Genuinely busy (someone else holds an
                            // unexpired, unreleased lease): the fast
                            // handoff path is only ever worth asking for
                            // here (`node_runtime.rs`: handoff is
                            // requested precisely when the plain acquire
                            // attempt came back `Busy`). Offering it
                            // whenever a holder merely *exists* — even
                            // one already released/expired and hence
                            // directly claimable — would let a declined
                            // or pointless handoff round be requested
                            // forever, since each round mints a fresh
                            // message id and so a fresh, never-before-
                            // seen state; gating on genuine business
                            // keeps the reachable state space finite.
                            None => {
                                if let Some(h) = state.lease.holder {
                                    if h != id {
                                        actions.push(Action::RequestHandoff(id));
                                        // Plan 30 §M2: retry the same rid
                                        // before falling back to the
                                        // lease path at all — only
                                        // meaningful once there is
                                        // dedup on the other end to
                                        // retry safely against.
                                        if self.protocol.dedups()
                                            && cop.attempts < self.max_forward_retries
                                        {
                                            actions.push(Action::RetryForward(id));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Phase::WaitingHandoff { .. } => {}
                }
            }
            if authority(state, id, self.protocol).is_some()
                && !node.journal.is_empty()
                && node.applied_seq < self.max_seq
            {
                actions.push(Action::Ship(id));
            }
            if node.held_epoch.is_some() {
                actions.push(Action::Renew(id));
            }
            if log_head(&state.log) > node.applied_seq {
                actions.push(Action::Tail(id));
            }
            // Plan 30 §M3a: replay the oldest stranded op, in order — only
            // once its previous request (if any) has been answered, and
            // only when there is somewhere to send it: this node itself
            // (usable authority) or a holder the lease names elsewhere.
            if let Some(head) = node.replays.first() {
                let target_exists = authority(state, id, self.protocol).is_some()
                    || state.lease.holder.is_some_and(|h| h != id);
                if head.inflight.is_none() && head.attempts < MAX_REPLAY_ATTEMPTS && target_exists {
                    actions.push(Action::ReplayStranded(id));
                }
            }
            // `mtree_publish` runs on the same per-partition sync cycle
            // that ships first (`node_runtime.rs`'s round: `ship_all`,
            // then, on its own idle cadence, `publish`), so by the time
            // an honest, still-acting node publishes, its own journal
            // has already been drained. Gating on an empty journal here
            // approximates that ordering rather than treating `Publish`
            // as fully independent of it — without this, *any* node
            // (even a lone, never-forwarded-to holder) could publish the
            // instant after a local `execute_mutate`, before ever
            // shipping, which is a real defect (plan 30 §1.1: "the
            // holder side has the same shape") but not the one
            // `single_writer_is_clean` exists to rule out. A lingering
            // *shadow* (the actual bug B shape) is untouched by this
            // gate and still reaches `Publish`.
            // Plan 30 §M3a publish rule, `Recovery` only: a node with an
            // outstanding shadow does not publish (`TreePublisher::publish`
            // defers while `Meta::has_outstanding_speculation`). Queued
            // replays do not block it: stranded effects are already rolled
            // back, so the replica is a log prefix plus live shadows (and
            // the node's own journal, substituted away at publish).
            // Plan 30 §M3b, `Recovery` only: the empty-journal gate above
            // (simplification 9) is gone — a node publishes while its
            // journal is non-empty, substituting before-images for the
            // unshipped entries (`log_prefix_view`, see `Publish`). The
            // shadow deferral stays.
            let speculating = self.protocol == Protocol::Recovery && !node.shadows.is_empty();
            let journal_gate = self.protocol != Protocol::Recovery && !node.journal.is_empty();
            if !journal_gate && !speculating {
                actions.push(Action::Publish(id));
            }
            if self.max_crashes > state.crashes_used {
                actions.push(Action::Crash(id));
            }
            if self.allow_pause {
                actions.push(Action::Pause(id));
            }
        }
        for env in &state.network {
            let deliverable = match env.body {
                MsgBody::MutateReq { to, .. } => is_live(state, to),
                MsgBody::MutateRep { to, .. } => is_live(state, to),
                MsgBody::HandoffReq { to, .. } => is_live(state, to),
                MsgBody::HandoffRep { to, .. } => is_live(state, to),
            };
            if deliverable {
                let a = match env.body {
                    MsgBody::MutateReq { .. } => Action::DeliverForwardRequest(env.id),
                    MsgBody::MutateRep { .. } => Action::DeliverForwardReply(env.id),
                    MsgBody::HandoffReq { .. } => Action::DeliverHandoffRequest(env.id),
                    MsgBody::HandoffRep { .. } => Action::DeliverHandoffReply(env.id),
                };
                actions.push(a);
            }
            if self.allow_lossy {
                actions.push(Action::DropMessage(env.id));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut s = state.clone();
        match action {
            Action::Tick => {
                s.tick += 1;
            }
            Action::ClientInvoke(id) => {
                let op = s.nodes[id as usize].pending_ops.remove(0);
                s.history.push(HistEvt::Invoke(id, op));
                // Rid allocated at the top, unconditionally, and kept
                // across every retry this op goes through (plan 30 §M2:
                // "every `MutateOp` a FUSE call issues gets its rid at
                // the top of `mutate_op_rebasable`").
                let seq = s.nodes[id as usize].next_seq;
                s.nodes[id as usize].next_seq += 1;
                let rid = Rid {
                    node: id,
                    incarnation: s.nodes[id as usize].incarnation,
                    seq,
                };
                let held = authority(&s, id, self.protocol);
                if let Some(epoch) = held {
                    // Fast path: `fusefs.rs::mutate_op_rebasable`'s
                    // `view.open_for_new_mutation()` branch — a
                    // synchronous local `execute_mutate`, no yield point.
                    let base = node_replica(&s, id);
                    let (ret, _) = eval(base, op);
                    if ret == NsRet::Ok {
                        self.journal_push(&mut s, id, rid, op, epoch);
                    }
                    s.history.push(HistEvt::Return(id, ret));
                } else if let Some(h) = s.lease.holder {
                    // `forward.rs::request_mutate_with`: send once, block
                    // on the reply (modeled as entering `WaitingReply`).
                    let corr = s.next_id;
                    s.next_id += 1;
                    s.network.push(Envelope {
                        id: corr,
                        body: MsgBody::MutateReq {
                            from: id,
                            to: h,
                            corr,
                            op,
                            rid,
                        },
                    });
                    s.nodes[id as usize].client_op = Some(ClientOp {
                        op,
                        phase: Phase::WaitingReply { corr, holder: h },
                        rid,
                        attempts: 0,
                    });
                } else {
                    // No known holder to forward to at all: straight to
                    // lease acquisition.
                    s.nodes[id as usize].client_op = Some(ClientOp {
                        op,
                        phase: Phase::NeedsLease,
                        rid,
                        attempts: 0,
                    });
                }
            }
            Action::DeliverForwardRequest(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (from, to, op, rid, corr) = match env.body {
                    MsgBody::MutateReq {
                        from,
                        to,
                        op,
                        rid,
                        corr,
                    } => (from, to, op, rid, corr),
                    _ => return None,
                };
                // Plan 30 §M2 holder dedup: a retried rid (same holder or
                // redirected) is answered from the recorded outcome
                // instead of being re-executed
                // (`forward.rs::holder_execute`'s "an executed rid is
                // never executed again"). Refusals are never recorded —
                // only `Ok` outcomes reach `completed`/`recent` — so a
                // retried refused op is re-evaluated at the retry.
                let dedup = self.protocol.dedups();
                let held = authority(&s, to, self.protocol);
                // `holder_execute` answers `NotHolder` before it looks at
                // anything else; `Today`/`ExactlyOnce` keep M1/M2's order
                // (dedup first), which only matters for a node that is no
                // longer holder but still remembers the rid.
                let cached = if dedup && (held.is_some() || self.protocol != Protocol::Recovery) {
                    rid_completed_record(&s, to, rid, self.protocol)
                } else {
                    None
                };
                let outcome = if let Some((rec, epoch)) = cached {
                    Outcome::Accepted(rec, epoch)
                } else if let Some(epoch) = held {
                    let base = node_replica(&s, to);
                    let (ret, _) = eval(base, op);
                    match ret {
                        NsRet::Ok => {
                            self.journal_push(&mut s, to, rid, op, epoch);
                            if dedup {
                                s.nodes[to as usize].recent.push((rid, op, epoch));
                            }
                            Outcome::Accepted(op, epoch)
                        }
                        NsRet::Err(e) => Outcome::Errno(e),
                    }
                } else {
                    Outcome::NotHolder
                };
                let rep_id = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: rep_id,
                    body: MsgBody::MutateRep {
                        to: from,
                        corr,
                        outcome,
                    },
                });
            }
            Action::DeliverForwardReply(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (to, corr, outcome) = match env.body {
                    MsgBody::MutateRep { to, corr, outcome } => (to, corr, outcome),
                    _ => return None,
                };
                let mut completed = None;
                if let Some(cop) = s.nodes[to as usize].client_op.clone() {
                    if let Phase::WaitingReply { corr: waiting, .. } = cop.phase {
                        if waiting == corr {
                            match outcome {
                                Outcome::Accepted(rec, epoch) => {
                                    // `forward.rs::apply_accepted`: shadow
                                    // insert + apply immediately, ahead of
                                    // the log — unless (plan 30 §M3b) this
                                    // node already holds a higher epoch,
                                    // in which case the op goes straight
                                    // to the replay queue (skipped if the
                                    // applied log already completed it).
                                    // The client was accepted either way.
                                    if self.reply_superseded(&s, to, epoch) {
                                        if !rid_in_applied_log(&s, to, cop.rid) {
                                            s.nodes[to as usize].replays.push(ReplayEntry {
                                                rid: cop.rid,
                                                op: rec,
                                                inflight: None,
                                                attempts: 0,
                                            });
                                        }
                                    } else {
                                        self.install_shadow(&mut s, to, cop.rid, epoch, rec);
                                    }
                                    completed = Some(NsRet::Ok);
                                }
                                Outcome::Errno(e) => completed = Some(NsRet::Err(e)),
                                Outcome::NotHolder => {
                                    s.nodes[to as usize].client_op.as_mut().unwrap().phase =
                                        Phase::NeedsLease;
                                }
                            }
                        }
                    }
                }
                if let Some(ret) = completed {
                    s.history.push(HistEvt::Return(to, ret));
                    s.nodes[to as usize].client_op = None;
                }
                // Plan 30 §M3a: or the reply to a replay-by-rid request.
                // Client forwards and replays each mint their own `corr`,
                // so at most one of the two ever matches.
                let replay = s.nodes[to as usize]
                    .replays
                    .iter()
                    .position(|r| r.inflight == Some(corr));
                if let Some(i) = replay {
                    let entry = s.nodes[to as usize].replays[i];
                    match outcome {
                        Outcome::Accepted(rec, epoch) => {
                            if self.reply_superseded(&s, to, epoch) {
                                // Plan 30 §M3b: answered under an epoch
                                // this node has already superseded. Not
                                // installed; the entry stays queued (in
                                // place, keeping its order) for another
                                // replay by rid, unless the applied log
                                // already completed it.
                                if rid_in_applied_log(&s, to, entry.rid) {
                                    s.nodes[to as usize].replays.remove(i);
                                } else {
                                    s.nodes[to as usize].replays[i].inflight = None;
                                }
                            } else {
                                // Replayed: a fresh shadow under the new
                                // holder's epoch, retiring like any other.
                                s.nodes[to as usize].replays.remove(i);
                                self.install_shadow(&mut s, to, entry.rid, epoch, rec);
                            }
                        }
                        Outcome::Errno(_) => {
                            // Refused: the real code materializes a
                            // `.constellation-conflict/` copy and counts
                            // it; the model has no conflict files, so the
                            // op is simply resolved.
                            s.nodes[to as usize].replays.remove(i);
                        }
                        Outcome::NotHolder => {
                            s.nodes[to as usize].replays[i].inflight = None;
                        }
                    }
                }
            }
            Action::ForwardTimeout(id) => {
                // `forward.rs::request_mutate_with`'s `tokio::time::timeout`
                // firing before the reply arrives: the pending `MutateReq`/
                // `MutateRep` in `s.network` is simply left to be delivered
                // (and ignored, since `client_op` has moved on) or dropped.
                if let Some(cop) = &mut s.nodes[id as usize].client_op {
                    if matches!(cop.phase, Phase::WaitingReply { .. }) {
                        cop.phase = Phase::NeedsLease;
                    }
                }
            }
            Action::RetryForward(id) => {
                // Plan 30 §M2: "the requester retries the same rid... 1.
                // the same holder... 2. a redirected holder" — modeled as
                // one action that always targets whoever `state.lease`
                // currently names, which is the same holder if it has
                // not changed and the redirected one if it has.
                let cop = s.nodes[id as usize].client_op.clone()?;
                let h = s.lease.holder?;
                if h == id {
                    return None;
                }
                let corr = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: corr,
                    body: MsgBody::MutateReq {
                        from: id,
                        to: h,
                        corr,
                        op: cop.op,
                        rid: cop.rid,
                    },
                });
                let cop_mut = s.nodes[id as usize].client_op.as_mut().unwrap();
                cop_mut.phase = Phase::WaitingReply { corr, holder: h };
                cop_mut.attempts += 1;
            }
            Action::RequestHandoff(id) => {
                let h = s.lease.holder?;
                let corr = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: corr,
                    body: MsgBody::HandoffReq {
                        from: id,
                        to: h,
                        corr,
                    },
                });
                if let Some(cop) = &mut s.nodes[id as usize].client_op {
                    cop.phase = Phase::WaitingHandoff { corr, holder: h };
                }
            }
            Action::DeliverHandoffRequest(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (from, to, corr) = match env.body {
                    MsgBody::HandoffReq { from, to, corr } => (from, to, corr),
                    _ => return None,
                };
                let ok = if authority(&s, to, self.protocol).is_some() {
                    if flush_for_handoff(&mut s, to, self.max_seq, self.protocol) {
                        s.nodes[to as usize].held_epoch = None;
                        s.lease.released = true;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                let rep_id = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: rep_id,
                    body: MsgBody::HandoffRep { to: from, corr, ok },
                });
            }
            Action::DeliverHandoffReply(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (to, corr) = match env.body {
                    MsgBody::HandoffRep { to, corr, .. } => (to, corr),
                    _ => return None,
                };
                if let Some(cop) = &mut s.nodes[to as usize].client_op {
                    if let Phase::WaitingHandoff { corr: waiting, .. } = cop.phase {
                        if waiting == corr {
                            // Whether the handoff succeeded or was
                            // declined, the requester falls back to the
                            // ordinary CAS-based acquire
                            // (`shipper::acquire_lease_for`); a decline
                            // just means it may have to wait out the TTL.
                            cop.phase = Phase::NeedsLease;
                        }
                    }
                }
            }
            Action::Tail(id) => {
                tail_one(&mut s, id, self.protocol);
            }
            Action::Ship(id) => {
                try_ship_once(&mut s, id, self.max_seq, self.protocol);
            }
            Action::Renew(id) => {
                let epoch = s.nodes[id as usize]
                    .held_epoch
                    .expect("Renew requires holding");
                let still_ours =
                    s.lease.holder == Some(id) && s.lease.epoch == epoch && !s.lease.released;
                if still_ours {
                    s.lease.expires_at = s.tick + self.lease_ttl;
                } else {
                    // `diagnose_lost_renew` -> `mark_lost`: deposition is
                    // terminal, the journal is stranded.
                    s.nodes[id as usize].held_epoch = None;
                    if self.protocol == Protocol::Recovery {
                        // Plan 30 §M3b (`recovery::recover_deposed`):
                        // every Local entry belongs to the lost tenure, so
                        // all of them roll back and queue for replay by
                        // rid, in journal order. Nothing is left to
                        // re-capture.
                        strand_local(&mut s, id, Epoch::MAX);
                    }
                }
            }
            Action::AcquireLease(id) => {
                // Plan 30 §M3b: a takeover (the model's `needs_tail`
                // claim — the register last named another holder, or
                // this node after releasing) ships an epoch marker under
                // `Recovery`. Decided before the CAS below rewrites the
                // register `claim_kind` reads.
                let takeover = claim_kind(&s, id) == Some(true);
                let new_epoch = match s.lease.holder {
                    None => 1,
                    Some(h) if h == id && !s.lease.released => s.lease.epoch.max(1),
                    _ => s.lease.epoch + 1,
                };
                s.lease = LeaseReg {
                    holder: Some(id),
                    epoch: new_epoch,
                    expires_at: s.tick + self.lease_ttl,
                    released: false,
                };
                s.nodes[id as usize].held_epoch = Some(new_epoch);
                if self.protocol == Protocol::Recovery {
                    // Plan 30 §M3a takeover gate
                    // (`shipper::acquire_lease_for` → `LeaseKeeper::
                    // commit_gated`): after tailing to head and winning
                    // the CAS, but before this node validates anything
                    // against its replica, strand every shadow the new
                    // epoch supersedes and execute every queued replay
                    // locally, in original order. The client op below
                    // therefore never sees phantom state, and a stranded
                    // op lands before any op that arrives after the
                    // takeover.
                    //
                    // Plan 30 §M3b: a takeover first ships its epoch
                    // marker (`ship_epoch_marker`), before the gate's
                    // local replays and before anything else executes.
                    // Modeled as part of this one atomic step rather than
                    // as a separate action: in the real code the window
                    // between the CAS and the marker landing has the view
                    // still closed, so this node executes, ships and
                    // answers nothing (a forwarded request arriving
                    // meanwhile is refused or waits for the view, which
                    // the model expresses by delivering it before or after
                    // this step); in this model the old holder cannot ship
                    // into the window either, because `authority` ends its
                    // tenure at the expiry or release this CAS required;
                    // and third nodes only observe the marker once it
                    // exists. So no other node's action
                    // could interleave with a distinguishable effect, and
                    // a separate step would only add interleavings (and a
                    // "marker pending" flag gating every action) without
                    // new reachable outcomes. The retry loop for a slot a
                    // late segment took is kept for fidelity, though
                    // tail-to-head plus exclusive authority make it
                    // unreachable here.
                    if takeover
                        && !ship_epoch_marker(&mut s, id, new_epoch, self.max_seq, self.protocol)
                    {
                        // Log full: the takeover cannot complete.
                        // `actions()` never offers this, so unreachable.
                        return None;
                    }
                    let node = &mut s.nodes[id as usize];
                    let (stranded, kept): (Vec<ShadowEntry>, Vec<ShadowEntry>) =
                        node.shadows.iter().partition(|sh| sh.epoch < new_epoch);
                    node.shadows = kept;
                    for sh in stranded {
                        node.replays.push(ReplayEntry {
                            rid: sh.rid,
                            op: sh.rec,
                            inflight: None,
                            attempts: 0,
                        });
                    }
                    // Plan 30 §M3b: this node's own Local entries from an
                    // earlier tenure are stranded the same way (behind
                    // its shadows, as in `tail_one`), and whatever
                    // survives is re-captured over the new base. Tail-to-
                    // head over the previous holder's marker has normally
                    // already done this; the gate keeps it unconditional,
                    // as `takeover_gate` requires no stranded speculation.
                    strand_local(&mut s, id, new_epoch);
                    recapture_before_images(&mut s, id);
                    // In-flight ones too: their target's authority ended
                    // with this CAS, so it can only answer `NotHolder`,
                    // and that late answer then matches nothing.
                    let queued = std::mem::take(&mut s.nodes[id as usize].replays);
                    for entry in queued {
                        self.replay_locally(&mut s, id, entry.rid, entry.op);
                    }
                }
                // `mutate_op_rebasable`: `require_lease_for` succeeding
                // falls straight into `execute_mutate`, synchronously —
                // but under `ExactlyOnce`, only after checking whether
                // this rid was already completed by a stale forward
                // (plan 30 §M2's coverage rule: `actions()` only offers
                // `AcquireLease` for a genuine takeover once this node
                // has tailed to head, so `rid_completed_record` here sees
                // every completion up to the point the takeover
                // happened).
                if let Some(cop) = s.nodes[id as usize].client_op.clone() {
                    let dedup = self.protocol.dedups();
                    let cached = if dedup {
                        rid_completed_record(&s, id, cop.rid, self.protocol)
                    } else {
                        None
                    };
                    let ret = if cached.is_some() {
                        NsRet::Ok
                    } else {
                        let base = node_replica(&s, id);
                        let (ret, _) = eval(base, cop.op);
                        if ret == NsRet::Ok {
                            self.journal_push(&mut s, id, cop.rid, cop.op, new_epoch);
                        }
                        ret
                    };
                    s.history.push(HistEvt::Return(id, ret));
                    s.nodes[id as usize].client_op = None;
                }
            }
            Action::ReplayStranded(id) => {
                // Plan 30 §M3a recovery step 3: "replay the stranded ops,
                // by rid and in original order, to the current sequencer
                // (or execute them locally if this node is now the
                // holder)". No `Invoke`/`Return` is recorded: the client
                // already got its answer when the op was first accepted;
                // replay only makes the log agree with that answer.
                let entry = *s.nodes[id as usize].replays.first()?;
                if entry.inflight.is_some() {
                    return None;
                }
                if authority(&s, id, self.protocol).is_some() {
                    s.nodes[id as usize].replays.remove(0);
                    self.replay_locally(&mut s, id, entry.rid, entry.op);
                } else {
                    let h = s.lease.holder.filter(|h| *h != id)?;
                    let corr = s.next_id;
                    s.next_id += 1;
                    s.network.push(Envelope {
                        id: corr,
                        body: MsgBody::MutateReq {
                            from: id,
                            to: h,
                            corr,
                            op: entry.op,
                            rid: entry.rid,
                        },
                    });
                    let head = &mut s.nodes[id as usize].replays[0];
                    head.inflight = Some(corr);
                    head.attempts += 1;
                }
            }
            Action::Publish(id) => {
                // `mtree_publish::TreePublisher::publish`: commits from
                // the current replica (dirty-key derived in the real
                // code; here, the full node_replica, which — faithfully —
                // includes any lingering shadow, since mtree_publish has
                // "no notion of shadows").
                //
                // Plan 30 §M3b, `Recovery`: `plan_from_dirty` over
                // `Meta::publish_basis_at` — the replica with every
                // name an unshipped journal entry touches replaced by its
                // earliest before-image (`log_prefix_view`), claimed at
                // `applied_seq`. `raw_holder_publish` (a test-only knob)
                // publishes the raw replica instead, journal included.
                let dir = if self.protocol == Protocol::Recovery && !self.raw_holder_publish {
                    log_prefix_view(&s, id)
                } else {
                    node_replica(&s, id)
                };
                let applied = s.nodes[id as usize].applied_seq;
                s.commit = Some((dir, applied));
            }
            Action::Crash(id) => {
                s.nodes[id as usize].alive = false;
                s.crashes_used += 1;
            }
            Action::Restart(id) => {
                // Durable journal/applied_seq/shadow survive; authority
                // does not (`bootstrap()` starts a fresh `LeaseKeeper`).
                // The persisted incarnation is bumped before serving
                // again, and the volatile per-incarnation `seq` counter
                // and holder-side `recent` outcome cache both reset to
                // empty (plan 30 §M2: "incarnation is persisted... and
                // bumped at every mount" is exactly what keeps a rid
                // allocated after this restart from ever colliding with
                // one from before it, even though `next_seq` itself
                // restarts at 0).
                s.nodes[id as usize].alive = true;
                s.nodes[id as usize].paused = false;
                s.nodes[id as usize].held_epoch = None;
                s.nodes[id as usize].incarnation += 1;
                s.nodes[id as usize].next_seq = 0;
                s.nodes[id as usize].recent.clear();
                // The replay queue is durable (`pending_replay`); a
                // request that was in flight is simply re-sent later.
                for entry in &mut s.nodes[id as usize].replays {
                    entry.inflight = None;
                }
            }
            Action::Pause(id) => {
                s.nodes[id as usize].paused = true;
            }
            Action::Resume(id) => {
                s.nodes[id as usize].paused = false;
            }
            Action::DropMessage(mid) => {
                s.network.retain(|e| e.id != mid);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("linearizable", prop_linearizable),
            Property::always("converged_at_quiescence", prop_converged_at_quiescence),
            Property::always("commits_are_log_prefixes", prop_commits_are_log_prefixes),
            Property::sometimes("progress", prop_progress),
        ]
    }

    /// A defensive backstop, not a load-bearing part of the model: every
    /// field the transitions above touch is already bounded by a config
    /// value (workload size, `max_tick`, `max_seq`, `max_crashes`), so
    /// the reachable state space is finite without this. It exists so
    /// that a future protocol variant with a genuine "keep retrying
    /// forever" edge (e.g. a handoff that always declines because the
    /// log is full) fails a bounded exploration loudly instead of
    /// hanging the checker.
    ///
    /// `max_next_id` (default 200) is `Recovery`'s escape valve: its
    /// added shadow/replay dimension multiplies branching per state far
    /// beyond what `Today`/`ExactlyOnce` reach at the same message-id
    /// budget, so a `Recovery` config that needs exhaustive coverage
    /// inside the test time/memory budget lowers it explicitly.
    fn within_boundary(&self, state: &State) -> bool {
        state.next_id < self.max_next_id && state.history.len() < 64
    }
}

fn prop_linearizable(_m: &AuthorityModel, s: &State) -> bool {
    let mut tester = LinearizabilityTester::<NodeId, NamespaceSpec>::new(NamespaceSpec::default());
    for evt in &s.history {
        let r = match evt {
            HistEvt::Invoke(n, op) => tester.on_invoke(*n, *op),
            HistEvt::Return(n, ret) => tester.on_return(*n, *ret),
        };
        if r.is_err() {
            return false;
        }
    }
    tester.is_consistent()
}

/// No client op is in flight anywhere, and (`Recovery`) no live node
/// still has a stranded op waiting to be replayed: a queued replay is
/// outstanding work exactly like an in-flight client op.
fn no_client_op_in_flight(s: &State) -> bool {
    s.nodes
        .iter()
        .all(|n| n.client_op.is_none() && (!n.alive || n.replays.is_empty()))
}

/// `Recovery` only: the lease still names a node that has crashed, so
/// nobody has failed over yet. Stranding is detected only by a segment
/// or a takeover at a higher epoch (a requester can never tell a dead
/// holder from a slow one — plan 30 §2 constraint 5), so until some node
/// takes over, a shadow accepted by the dead holder is still legitimately
/// in doubt rather than stranded: the real cluster resolves it at the
/// next write anyone makes, which is what a takeover is. This mirrors the
/// dead holder's own unshipped journal, which
/// `every_durable_slot_applied_everywhere` already exempts.
fn failover_pending(s: &State) -> bool {
    s.lease.holder.is_some_and(|h| !s.nodes[h as usize].alive)
}

/// "Every durable slot is applied on every live node" — but a live node
/// whose *own* journal still has unshipped records is not quiescent
/// either (it will ship them momentarily; that is ordinary operation,
/// not the stranding bug B is about). A crashed holder's stranded
/// journal is exactly what should be allowed to keep this false forever
/// — which is why the check is skipped for nodes that are not alive.
fn every_durable_slot_applied_everywhere(s: &State) -> bool {
    let head = log_head(&s.log);
    s.nodes
        .iter()
        .all(|n| !n.alive || (n.applied_seq == head && n.journal.is_empty()))
}

fn prop_converged_at_quiescence(m: &AuthorityModel, s: &State) -> bool {
    if !no_client_op_in_flight(s) || !every_durable_slot_applied_everywhere(s) {
        return true;
    }
    if m.protocol == Protocol::Recovery && failover_pending(s) {
        return true;
    }
    let head = log_head(&s.log);
    let (log_dir, _) = log_state_and_epoch(&s.log, head);
    s.nodes
        .iter()
        .enumerate()
        .all(|(i, n)| !n.alive || node_replica(s, i as NodeId) == log_dir)
}

fn prop_commits_are_log_prefixes(_m: &AuthorityModel, s: &State) -> bool {
    match &s.commit {
        None => true,
        Some((published, applied)) => {
            let (derived, _) = log_state_and_epoch(&s.log, *applied);
            *published == derived
        }
    }
}

fn prop_progress(m: &AuthorityModel, s: &State) -> bool {
    let total_ops = m.workload.len();
    let completed = s
        .history
        .iter()
        .filter(|e| matches!(e, HistEvt::Return(..)))
        .count();
    completed == total_ops
        && s.nodes
            .iter()
            .all(|n| n.client_op.is_none() && n.pending_ops.is_empty())
}
