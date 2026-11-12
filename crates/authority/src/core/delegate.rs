//! Plan 30 §M11: delegated sub-sequencers over one log.
//!
//! Two sides live here. **A delegate** (a node the log's
//! `Delegate{dir, node, gen}` record names) executes every mutation whose
//! keys fall under `dir` on its own replica — authoritative for the
//! subtree, because every write there goes through it — journals the
//! transaction with the generation's next stream index and the
//! requester's `deps`, acknowledges, and streams the transactions to the
//! root in stream order (`DelegateStream`, one batch in flight per
//! generation). It honours its grant only while its clock is below
//! `sent + ttl − margin` measured from its own renewal request (M8's
//! discipline), renews at half the ttl, and stops the moment the root
//! recalls the generation. Its journal rows are speculation like a
//! holder's: `Meta` retires them when a segment carries their origin
//! and strands them when the log recalls the generation (rolled back,
//! replayed by rid through the new owner).
//!
//! **The root** (the lease holder) appends a delegate's stream in index
//! order once the generation is live and the batch's `deps` are in its
//! replica — no re-validation — and journals it with the delegate's
//! origin, so the segment carries it and every replica (the delegate
//! included) learns its per-generation applied index. Before it executes
//! anything whose keys fall under a live delegation (a cross-subtree
//! rename, an op forwarded by a node with a stale table, an
//! `Undelegate`) it recalls the generation: `DelegRecall`, then the
//! delegate's `DelegRecalled{through}` and the stream catching up to
//! `through`, or the grant's expiry on the root's clock (granted at
//! `g`: `g + ttl + margin`) — after which the generation *ends*: a
//! `Recall{dir, gen}` record journaled, every later stream record of it
//! refused. An unrenewed grant is reclaimed the same way (a crashed
//! delegate is never recalled by an op alone). No renewal is granted
//! once a recall or a reclaim began.
//!
//! The rules the model made load-bearing (`crates/model/src/delegation.rs`,
//! "Rules the model found") are each a line below: the delegate waits
//! for `deps` too (its own readers see its speculation); a dependency on
//! an ended generation is void (`Replica::reaches`); an acknowledgement
//! from an ended stream is tentative (`Meta` strands the row; the
//! requester replays by rid); the initial grant is capped like every
//! renewal by the root lease, so an inherited grant is dead at a takeover
//! and the successor may reclaim at once; the root reclaims an unrenewed
//! grant on its own.
//!
//! Positions: a delegate's acknowledgement carries `(gen, idx)` in the
//! position's `streams` (M6's watermark with a per-stream pending part);
//! the root's carries every generation's cursor. A requester whose
//! observed streams are full sends its op to the root instead
//! (`deps_overflow_to_root`, coordinator decision 4).
//!
//! Continuation epochs (M10): no delegation inside an epoch. When the
//! driver reports an active epoch the root recalls every live generation
//! (over P2P, which an epoch has by definition) and refuses `Delegate`;
//! a delegate that learns of an epoch stops executing. Delegation resumes
//! by an operator's `delegate` after the epoch closed.

use super::backup::{FreshHold, FreshVerdict};
use super::{Asker, CommitRule, Core, GroupOn, Timer};
use crate::action::{Action, AnswerSource, ControlOk};
use crate::event::{PeerMsg, ReleaseWhy};
use crate::ids::{HonourMs, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Ownership;
use constellation_meta::locks::LockTag;
use constellation_meta::{
    LogRecord, MetaError, MutateOp, MutateOutcome, OwnChunks, Position, RecallNeed, Rid,
    SubtreeKind, TouchSet,
};
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet};

/// Parked-wait ids for generations (above every read-delegation grant
/// id; `Parked::waiting` holds both kinds).
pub(crate) const DELEG_WAIT_BASE: u64 = 1 << 62;

pub(crate) fn wait_id(gen: u64) -> u64 {
    DELEG_WAIT_BASE + gen
}

/// Why the root recalled a stream (plan 46 M1.3 step 4 review: a sticky
/// grant is recalled by the operator's `undelegate` and, as the one
/// exception of §5.4, by a continuation epoch; any other cause is the
/// root taking a non-stealable grant back).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallCause {
    /// The operator's `undelegate`.
    Undelegate,
    /// A continuation epoch opened: the delegates stop and the hold
    /// sequences (`recall_all`).
    Epoch,
    /// Placement, a cross-subtree op, a designation, a stalled partial
    /// release: causes that never apply to a non-stealable grant.
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallPhase {
    None,
    Sent,
    /// `DelegRecalled { through }` received: ends once the cursor is there.
    Drained(u64),
    /// Phase 2b: the delegate is silent; its backup was asked to seal
    /// and drain.
    Sealing,
}

/// Who asks for a grant (plan 46 M1.3 step 9, ADR-55): the root's own
/// sticky grant is made only by the placement or a handover, never by an
/// operator's `delegate` (open question 6 of the step 9 spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GrantSource {
    Operator,
    Placement,
    #[allow(dead_code)] // step 8 C4: the handover's grant
    Handover,
}

/// Why a generation exists (phase 2b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegKind {
    /// `constellation delegate`: until `undelegate` or a cross-subtree op.
    Manual,
    /// The placement (`core::placement`): recalled by it too.
    Placed,
    /// An offline designation (plans 03–05): recalled only by `online`;
    /// never expires; a cross-subtree op touching it is refused.
    Designated,
    /// Plan 46 M1.3 step 4 (`constellation delegate --sticky`): a grant the
    /// root does not take back by itself. No TTL end, no placement recall,
    /// no recall by a cross-subtree op; it ends by the holder's release or
    /// an operator action.
    Sticky,
}

impl DelegKind {
    /// The one test of the family: the root cannot take the grant back by
    /// itself (no TTL end, no placement recall, `EROFS` at the root and
    /// `EXDEV` across the boundary while the holder is out of reach,
    /// locks that outlive the holder's silence). A stream is `Final` only
    /// when every grant in it is.
    pub(crate) fn non_stealable(self) -> bool {
        matches!(self, DelegKind::Designated | DelegKind::Sticky)
    }

    /// Whether a grant of kind `other` may join a stream holding `self`:
    /// a stream never mixes the families (stealable, sticky, designated).
    pub(crate) fn joins(self, other: DelegKind) -> bool {
        if self.non_stealable() || other.non_stealable() {
            self == other
        } else {
            true
        }
    }

    /// The kind the record carries.
    pub(crate) fn subtree_kind(self) -> SubtreeKind {
        match self {
            DelegKind::Designated => SubtreeKind::Designated,
            DelegKind::Sticky => SubtreeKind::Sticky,
            DelegKind::Manual | DelegKind::Placed => SubtreeKind::Placed,
        }
    }
}

/// Step 10: a request for some subtrees of a stream is sent this many times
/// again (L3) before it becomes a recall of the whole stream: the delegate
/// may be waiting for the read delegations it granted under them, and a
/// recall closes the stream's window.
pub(crate) const PARTIAL_RESENDS_BEFORE_RECALL: u32 = 3;

/// The root's view of one generation.
#[derive(Debug, Clone)]
pub(crate) struct GenState {
    /// The subtrees the stream holds and why each was granted (phase 2b:
    /// an operator, the placement, a designation). Plan 46 M1.2 step 9: one
    /// stream per node, grown in place by a later grant (`DelegateNode`
    /// again, the same generation).
    pub dirs: BTreeMap<Ino, DelegKind>,
    pub node: NodeId,
    /// Plan 43 C2: the generation's configuration — the latest
    /// `DelegateNodeGroup` record this root journaled (seeded from its table
    /// for an inherited generation). The root drains its backup (seal)
    /// when the delegate dies.
    pub group: GroupState,
    /// Phase 2b: subtrees a cross-subtree op released (or ended with the
    /// stream): delegated to the same node again once the op executed.
    pub redelegate_dirs: BTreeMap<Ino, DelegKind>,
    /// Plan 46 M1.2 step 10: the subtrees a release request names and the
    /// delegate's `SubtreeReleased` row has not yet reached the root's
    /// log (a recall of the stream: all of its subtrees). While any is left
    /// the request is sent again (L3).
    pub releasing: BTreeSet<Ino>,
    /// When the root granted it (placement's dwell counts from here; the
    /// cut's designation-online check), on the wait clock.
    pub granted: WaitMs,
    /// Plan 46 M0.3 (S3): inherited from a predecessor root, never granted
    /// by this tenure (`granted` is when it was learned).
    pub inherited: bool,
    /// When the root last grew the stream by a subtree (step 9): a forward
    /// from the delegate for the new subtree is "sent before its table
    /// carried the grant" for [`Core::grant_in_flight_ms`], as for a fresh
    /// generation.
    pub grown_at: Option<WaitMs>,
    /// ADR-44: the recall was started inside a shippable takeover gate, on
    /// demand (an op or a settle that needs this generation ended,
    /// [`Core::gate_recall_on_demand`]). Only such a recall is resent, has
    /// its drain taken and is ended (`DelegateNodeEnded`) before the gate opens.
    pub gate_recalled: bool,
    /// Placement: since when the delegate's share has been below the
    /// leave threshold (`None`: it is not).
    pub below_since: Option<WaitMs>,
    /// Plan 43 C2e (§3.7): the seals asked of the group's members, by request
    /// id: `(member, strict)`. A *strict* member is one of the latest
    /// `DelegateNodeGroup` version (the end waits for one of them); a best-effort
    /// one is named only by an older record (sealed once, its tail drained
    /// when it comes while the generation is still ending, never waited
    /// for). A late answer to any earlier request is as good as the latest's.
    pub seal_reqs: BTreeMap<OpId, (NodeId, bool)>,
    /// Plan 43 C2e (Q1, D8): a strict member's sealed tail the root has not
    /// drained to its end (a row whose dependencies are not here yet, a row
    /// of a newer tenure, an append error, a root that may not end the
    /// recall now): kept, and drained again after every event, until the
    /// cursor reaches the end. The generation stays `Sealing` meanwhile.
    pub sealed_tail: Option<std::sync::Arc<SealedTail>>,
    /// Plan 43 C2 (Q1): the generation cannot end: its latest `DelegateNodeGroup`
    /// record names a backup, which may hold acknowledged rows the root
    /// does not, and none has answered its seal with a tail drained to its
    /// end. Asked again every window until one does (counted in
    /// `Stats::deleg_seal_waits`), `status.delegation.gens[].waiting_for_group`;
    /// `constellation repair abandon --gen` ends it at the cursor.
    pub waiting_for_group: bool,
    /// The last WARN about the wait (once a minute).
    pub waiting_warned: Option<WaitMs>,
    /// Since when the generation has been `waiting_for_group` (status).
    pub waiting_since: Option<WaitMs>,
    /// What this root knows of each member it asked to seal (status: the
    /// operator weighs `abandon` by it).
    pub member_notes: BTreeMap<NodeId, MemberNote>,
    /// Plan 46 M1.2 step 7 (c3-spec D6): the previous roots this root still
    /// has to seal, for a generation it inherited with no recorded backup (its
    /// group is the delegate and a root: a root that appended rows the delegate
    /// was answered through has them only as a member copy, and the root's
    /// cursor does not cover them). The generation does not end while any is
    /// left (`end_generation`); each leaves by its answer drained to its end
    /// ("holds nothing" included), or all together by a renewal of the delegate
    /// that proves this root has every row it executed. Persisted
    /// (`Replica::set_deleg_prev_roots`).
    pub prev_roots: BTreeSet<NodeId>,
    /// The requests of those seals (a late answer to any is as good).
    pub prev_reqs: BTreeMap<OpId, NodeId>,
    /// A previous root's sealed tail the drain has not reached the end of.
    pub prev_tails: BTreeMap<NodeId, std::sync::Arc<SealedTail>>,
    /// The end of the generation was asked for and waits for `prev_roots`.
    pub prev_blocked: bool,
    /// Previous roots settled in this tenure (a transitive answer naming one
    /// again does not ask it twice).
    pub prev_done: BTreeSet<NodeId>,
    /// When this root last heard from the delegate about this generation (a
    /// renewal, a stream batch, a recall's answer), on its wait clock.
    pub delegate_heard: Option<WaitMs>,
    /// Appended through this stream index.
    pub cursor: u64,
    /// The round and request of the delegate's last stream batch: what a
    /// pushed [`PeerMsg::DelegateStreamAck`] carries
    /// ([`Core::root_stream_push`]).
    pub last_batch: Option<(u64, OpId)>,
    /// The `committed` of the last acknowledgement sent for this
    /// generation (plan 43 Q16, ADR-41): the push goes out only when the
    /// root's committed prefix rises above it. Also where
    /// [`Core::root_append_cap`]'s scan starts (past it: every row through it
    /// was committed when it was sent); a new round of the stream resets it
    /// to 0.
    pub committed_sent: u64,
    // The window (`until`: the root outwaits the delegate once its **wait**
    // clock reaches it; plan 43 C7-H1) lives in `Core::dw`, the root's
    // shared window table (plan 41 R1, `DelegWindows`).
    pub recall: RecallPhase,
    /// The phase a seal of the backup replaced: what an unanswered seal
    /// goes back to.
    pub recall_before_seal: RecallPhase,
    /// Plan 46 M1.2 step 14 (L2b): the delegate's rowless release ended its
    /// stream at this cut (`ReleaseNote::end_at`), and the latest record
    /// names backups: the root seals the group before it ends the generation
    /// (rows the delegate was answered through may sit on the backups above
    /// the cut: the delegate-turned-root's unshipped appends of a re-ship).
    /// Not persisted: a later root inherits the generation, recalls it again
    /// and is answered again.
    pub release_cut: Option<u64>,
    /// The generation is `Sealing` because of that release: the seal is not
    /// given up for the phase it replaced (a strict member's transport
    /// failure, a renewal), the end waits for the cursor to reach
    /// `release_cut` as well, and the stream is not re-delegated to the
    /// backup (the cause of the recall stands).
    pub release_seal: bool,
    /// The retry of that wait (`Timer::DelegReleaseSeal`): `arm_gen_expiry`
    /// arms nothing for a sticky or designated grant.
    pub release_seal_timer: Option<TimerId>,
    /// A strict member answered "holds nothing" (or a drained tail) before
    /// the cursor reached `release_cut`: the end is tried again after every
    /// event, as rows arrive.
    pub release_answered: Option<NodeId>,
    /// A strict member's answer (a tail, or "holds nothing") was taken for
    /// the release (`end_generation` asserts it).
    pub release_group_answered: bool,
    /// The wait for the cursor to reach `release_cut` is over: a strict
    /// member answered, and a whole window (`Timer::DelegReleaseSeal`, armed
    /// when the answer came: `release_answered_at`) later the stream still
    /// lacks rows up to the cut. The delegate may have let
    /// go of its stream (it answered the release and dropped its state), and
    /// a row of it that the group does not hold was never acknowledged: the
    /// generation ends at the cursor.
    pub release_cut_waived: bool,
    /// When that answer first came (the waiver is a whole window later).
    pub release_answered_at: Option<WaitMs>,
    pub ended: bool,
    pub expiry: Option<TimerId>,
    pub recall_req: Option<OpId>,
    /// Every attempt of the recall still registered in `dl.by_req` (an answer
    /// to any of them counts; `recall_req` is the newest).
    pub recall_tries: Vec<OpId>,
    /// When the recall started (the stall `deleg_recall_ms_*` measures).
    pub recall_started: Option<WaitMs>,
    /// Why the root recalled the stream (`None`: it did not; the holder's
    /// own release ended it).
    pub recall_cause: Option<RecallCause>,
    /// Step 10: how often the request for some subtrees only was sent again
    /// (a few resends before it becomes a recall of the stream).
    pub release_resends: u32,
    /// The timer that sends the recall again while it is unanswered
    /// (`Core::on_release_resend`), and its current back-off.
    pub recall_resend: Option<TimerId>,
    pub recall_backoff_ms: u64,
    /// `Undelegate` controls answered when the generation ends.
    pub controls: Vec<OpId>,
    /// The read delegations (M8) this root granted on the subtree before
    /// it delegated it, being recalled (`Core::delegate_dir`): the
    /// delegate's writes recall only its own table's, so no renewal is
    /// granted while one of these is live. Only a stream's first subtree has
    /// any: a subtree that grows a live stream is journaled once they are
    /// gone (`Core::delegate_dir`, [`DelegationState::growing`]).
    pub read_recalls: BTreeSet<u64>,
    /// The delegate's renewal that waits for `read_recalls`, answered
    /// when the last of them is gone.
    pub renew_waiting: Option<(NodeId, OpId, u64, RenewFrom)>,
}

/// The streams an op touches and the subtrees of each, with the kind each was
/// granted as.
type Involved = Vec<(u64, BTreeMap<Ino, DelegKind>)>;

/// An ended stream to grant again: its generation, subtrees and delegate.
type Redelegation = (u64, Vec<(Ino, DelegKind)>, NodeId);

/// One live stream of the table, as the sync reads it (the node, the
/// generation and the subtrees it holds).
pub(crate) struct StreamView {
    pub gen: u64,
    pub node: NodeId,
    pub dirs: Vec<(Ino, SubtreeKind)>,
}

impl StreamView {
    pub(crate) fn of(s: &constellation_meta::delegation::NodeStream) -> Self {
        StreamView {
            gen: s.gen,
            node: s.node,
            dirs: s.subtrees.clone(),
        }
    }

    /// Every grant of the stream is non-stealable.
    pub(crate) fn non_stealable(&self) -> bool {
        !self.dirs.is_empty() && self.dirs.iter().all(|(_, k)| k.non_stealable())
    }

    /// Every grant of the stream is sticky.
    pub(crate) fn sticky(&self) -> bool {
        !self.dirs.is_empty() && self.dirs.iter().all(|(_, k)| *k == SubtreeKind::Sticky)
    }

    pub(crate) fn dir_list(&self) -> Vec<Ino> {
        self.dirs.iter().map(|(d, _)| *d).collect()
    }
}

impl GenState {
    /// Every grant of the stream is non-stealable (a sticky grant or a
    /// designation): the one rule of the stream is `Final`, and the TTL, the
    /// placement and a recall by a cross-subtree op leave it alone. A stream
    /// with any stealable grant has the group rule and is reclaimed whole.
    pub(crate) fn non_stealable(&self) -> bool {
        !self.dirs.is_empty() && self.dirs.values().all(|k| k.non_stealable())
    }

    /// Every grant of the stream is an offline designation (what only
    /// `online` releases).
    pub(crate) fn designated(&self) -> bool {
        !self.dirs.is_empty() && self.dirs.values().all(|k| *k == DelegKind::Designated)
    }

    /// The stream's kind: `Designated` or `Sticky` when every grant is,
    /// `Manual` when an operator granted any of its subtrees (the placement
    /// does not recall it), otherwise `Placed`.
    pub(crate) fn kind(&self) -> DelegKind {
        if self.designated() {
            DelegKind::Designated
        } else if !self.dirs.is_empty() && self.dirs.values().all(|k| *k == DelegKind::Sticky) {
            DelegKind::Sticky
        } else if self.dirs.values().any(|k| *k == DelegKind::Manual) || self.dirs.is_empty() {
            DelegKind::Manual
        } else {
            DelegKind::Placed
        }
    }

    pub(crate) fn dir_list(&self) -> Vec<Ino> {
        self.dirs.keys().copied().collect()
    }
}

/// When the root last asked, lost the transport to, and heard from one
/// member of a generation's group (wait clock).
#[derive(Debug, Clone, Default)]
pub(crate) struct MemberNote {
    pub asked: Option<WaitMs>,
    pub failed: Option<WaitMs>,
    pub answered: Option<WaitMs>,
}

/// A sealed tail waiting to be drained (see [`GenState::sealed_tail`]).
#[derive(Debug, Clone)]
pub(crate) struct SealedTail {
    /// The member that sealed and sent it.
    pub from: NodeId,
    pub txs: Vec<constellation_meta::DelegateTx>,
    /// Where the tail runs to without a gap, counted from the cursor at
    /// the time it arrived: what the drain must reach.
    pub end: u64,
}

/// Plan 43 C2: a generation's configuration as the root knows it (the
/// latest `DelegateNodeGroup` record journaled for it; version 0 and no backups:
/// the group is the delegate and the root).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GroupState {
    pub version: u64,
    pub backups: Vec<NodeId>,
    /// Every backup any record of the generation named (C2e's seal set).
    pub named: BTreeSet<NodeId>,
}

impl GroupState {
    pub(crate) fn of(row: Option<constellation_meta::delegation::NodeGroupRow>) -> Self {
        row.map(|r| GroupState {
            version: r.version,
            backups: r.backups,
            named: r.named.into_iter().collect(),
        })
        .unwrap_or_default()
    }

    /// The backup to seal (delegates keep at most one).
    pub(crate) fn backup(&self) -> Option<NodeId> {
        self.backups.first().copied()
    }

    /// Plan 43 C2e (D3): the members the generation's end waits for — the
    /// latest version's backups.
    pub(crate) fn strict(&self) -> &[NodeId] {
        &self.backups
    }

    /// Plan 43 C2e (D3): the backups only an older record named: sealed
    /// once at the end, never waited for.
    pub(crate) fn best_effort(&self) -> Vec<NodeId> {
        self.named
            .iter()
            .copied()
            .filter(|n| !self.backups.contains(n))
            .collect()
    }
}

/// Plan 43 C2 (D7): where a delegate's stream row stands for the
/// acknowledgement parked on it (`Core::deleg_stream_state_with`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamState {
    Committed,
    Waiting,
    Dropped,
}

/// What `complete_ready` reads once for all the parked acknowledgements:
/// the log's index of each generation, and whether each delegate row
/// asked about is still in the journal (a seek each: the set of every
/// outstanding row cost a walk of the whole stream per event).
#[derive(Default)]
pub(crate) struct StreamCache {
    log_idx: BTreeMap<u64, u64>,
    pending: std::collections::HashMap<(u64, u64), bool>,
}

/// What [`Core::holder_append_cap`] of a generation was computed from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HolderCapStamp {
    tip: u64,
    shipped: u64,
    out_of_order: (usize, u64),
    durable: u64,
    holds: bool,
}

/// An op a delegate holds until its `deps` are here or its grant is
/// renewed.
#[derive(Debug, Clone)]
pub(crate) struct ParkedDeleg {
    /// `0`: this node's own client op (its `ClientOp` is in `clients`).
    pub from: NodeId,
    pub req: Option<OpId>,
    pub rid: Rid,
    pub op: MutateOp,
    pub deps: Position,
    /// The requester's M2 receipt, forwarded on when the op re-enters
    /// the holder path (`answer_not_owner` → the requester re-sends).
    #[allow(dead_code)]
    pub acked_through: u64,
    /// Plan 30 §M14 phase 2: the op's fencing token.
    pub tag: LockTag,
    /// Plan 43 C2: a settle of the rid, not an execution.
    pub settle: bool,
    /// ADR-47: the forward's fence (checked at admission; kept so a
    /// re-entry through `answer_not_owner` carries it).
    #[allow(dead_code)]
    pub fence: super::holder::Fence,
    /// Plan 46 M1.3 step 5: parked because a release of the subtree is
    /// draining (`Core::sticky_holds`): it runs again when the hold lifts,
    /// and is sent to the root when the release row is journaled.
    pub held: bool,
    /// Late-landing S2b: when it arrived, on this node's wait clock. A
    /// forwarded attempt (`from != 0`, not a settle) older than
    /// `Deadlines::inbox_read_fresh` is not executed when its wait ends.
    pub arrived: WaitMs,
}

/// A generation installed while the read grants a previous process of this
/// node made may still be honoured (ADR-46): its writes and read grants wait
/// for `until`.
#[derive(Debug, Clone)]
pub(crate) struct RestartQuarantine {
    /// When the persisted delegate read-grant horizon ends, on the wait
    /// clock.
    pub until: WaitMs,
    /// The timer that ends it (`Timer::DelegRestartQuarantine`); `None`
    /// once it has fired.
    pub timer: Option<TimerId>,
    /// The void requests of the early exit not answered yet: req -> reader.
    pub asked: BTreeMap<OpId, NodeId>,
    /// The void round went out (every audience node was asked).
    pub round_sent: bool,
    /// Every node answered the void: no reader honours a grant of the
    /// previous process, and the quarantine is over before `until`.
    pub voided: bool,
}

/// Plan 46 M1.2 step 10: a release this delegate journaled (or could not,
/// `ReleaseNote::end_at`) and has not seen acknowledged: its note rides
/// every batch until the root's acknowledgement covers `row`.
#[derive(Debug, Clone)]
pub(crate) struct PendingRelease {
    pub note: constellation_meta::locks::ReleaseNote,
    /// The stream index of the release row (`None`: none was journaled).
    pub row: Option<u64>,
    /// The first release row this note covers (a later release merged into
    /// a note that was not acknowledged yet keeps the earlier row): what a
    /// hold keeps back starts here.
    pub first_row: Option<u64>,
    /// The row and everything after it are held back from the stream until
    /// the read delegations this node granted under the released subtrees
    /// are recalled (phase 2b).
    pub held: bool,
    /// How many recalls the hold waits for (one per release merged in):
    /// the hold is let go when the last is done.
    pub holds: u32,
    /// When the note's lock `carry` was measured (wait clock): each send
    /// takes the time since off what is left of every record.
    pub left_at: WaitMs,
    /// The root that asked: a release that journaled no row answers it
    /// directly.
    pub to: NodeId,
    /// The release gives the whole stream up (or ends it at a cut): once
    /// the root acknowledges it, the stream is released, not only a part.
    pub ends: bool,
}

impl PendingRelease {
    /// A later release joins this one, which the root has not acknowledged
    /// yet: one note for both, never one dropped for the other. The subtrees
    /// and the lock grants are the union (each grant once; what is left of
    /// each is measured again), the floors join, and the hold stays until
    /// every recall it waits for is done. The acknowledgement that covers the
    /// later row covers the earlier.
    pub(crate) fn merge(mut self, later: PendingRelease, now: WaitMs) -> PendingRelease {
        let mut note = self.note_at(now);
        let new = later.note_at(now);
        for dir in new.dirs {
            if !note.dirs.contains(&dir) {
                note.dirs.push(dir);
            }
        }
        note.dirs.sort_unstable();
        note.end_at = match (note.end_at, new.end_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        // Each grant once, as the later release handed it back: a grant in
        // both was handed back, granted to the stream again and handed back
        // once more (plan 46 M1.2 step 10), and the earlier copy is stale
        // (released or replaced since, at the delegate).
        for g in new.locks.grants {
            note.locks.grants.retain(|x| x.id != g.id);
            note.locks.grants.push(g);
        }
        for (id, left) in new.locks.carry {
            note.locks.carry.retain(|(x, _)| *x != id);
            note.locks.carry.push((id, left));
        }
        for (id, at) in new.locks.rows {
            note.locks.rows.retain(|(x, _)| *x != id);
            note.locks.rows.push((id, at));
        }
        note.locks.floor =
            constellation_meta::locks::floor_join(&note.locks.floor, &new.locks.floor);
        note.locks.barrier = note.locks.barrier.max(new.locks.barrier);
        note.leaving |= new.leaving;
        note.seal |= new.seal;
        note.no_stream &= new.no_stream;
        self.note = note;
        // A release that journaled no row ends the stream at its cut: the
        // note is acknowledged by the batch that carries it.
        self.row = if later.row.is_none() {
            None
        } else {
            self.row.max(later.row)
        };
        self.first_row = match (self.first_row, later.first_row) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.held |= later.held;
        self.ends |= later.ends;
        self.holds += later.holds;
        self.left_at = now;
        self.to = later.to;
        self
    }

    /// The note as it goes out at `now`: what is left of each handed-back
    /// record is measured again.
    pub(crate) fn note_at(&self, now: WaitMs) -> constellation_meta::locks::ReleaseNote {
        let gone = now.since(self.left_at).max(0) as u64;
        let mut note = self.note.clone();
        for (_, left) in note.locks.carry.iter_mut() {
            *left = left.saturating_sub(gone);
        }
        note
    }
}

/// This node as the delegate of one generation.
#[derive(Debug, Clone)]
pub(crate) struct DelegateState {
    /// The subtrees this node honours under the generation.
    pub dirs: BTreeSet<Ino>,
    /// Plan 46 M1.2 step 9: subtrees a later `DelegateNode` of the live
    /// stream added that this node learned but does not honour yet: each
    /// waits for its record to be committed (`None`: not yet; step 8b's rule
    /// for a growth) and then for a granting answer to a renewal sent after
    /// that (`Some(after)`: a granting answer to a renewal sent after `after`
    /// counts; it carries the lock handoff and has waited for the read
    /// delegations under the new subtree).
    pub grown: BTreeMap<Ino, Option<HonourMs>>,
    /// Plan 46 M1.2 step 10: this node released subtrees on the root's
    /// request and the root has not acknowledged the release row yet.
    pub release: Option<PendingRelease>,
    /// Plan 46 M1.2 step 11: the root acknowledged the release of the whole
    /// stream (a leave counts only these as released; the others wait for
    /// the log to end them).
    pub release_acked: bool,
    /// Installed again after a restart for the release row of a stream that
    /// released every subtree, and nothing else (no subtree is left to tell
    /// its kind by, so `non_stealable` is false): the state a sync must keep
    /// while the row is unacknowledged (`Core::delegation_sync`).
    pub remnant: bool,
    /// Plan 46 M1.2 step 10: the subtrees this node released from the stream
    /// (a repeated or late request for one is not news: it is not a subtree
    /// the node has yet to learn). A subtree granted to the stream again
    /// leaves the set once the node has learned it.
    pub released_dirs: BTreeSet<Ino>,
    /// Plan 46 M1.2 step 11: the latest answer to a renewal this node
    /// received, as `(root, when that root sent it, on its clock)`
    /// ([`PeerMsg::DelegRenewed::sent_at`]); echoed in the next renewal.
    pub root_answer: Option<(NodeId, i64)>,
    /// Plan 46 M1.2 step 10: subtrees the root asked this node to release
    /// that this node has not learned yet (the `DelegateNode` record that
    /// grew the stream is in a segment it has not applied): released once it
    /// has, never before: a release journaled first would be undone by that
    /// record when it is applied, and the delegate would hold what the log
    /// gave back. With the root that asked.
    pub release_pending: (BTreeSet<Ino>, NodeId),
    /// The send time of the renewal whose granting answer the root's core
    /// just gave (in time or late): the answer that carries the lock
    /// handoff, grace and floor. Taken by the message arm that installs
    /// them, which then lets the grown subtrees it covers in.
    pub core_answer_sent: Option<HonourMs>,
    pub gen: u64,
    /// Honoured while this node's honour clock is below this (`HonourMs(0)`:
    /// not renewed yet; plan 43 C7-H1).
    pub until: HonourMs,
    pub stopped: bool,
    /// Installed on a boot after the one that held the stream (it never
    /// re-streams): its rowless release may sit below rows the group
    /// acknowledged (`ReleaseNote::seal`).
    pub rebooted: bool,
    /// Installed while the previous process's read grants may be live: no
    /// write executes and no read delegation is granted until `until`
    /// ([`Core::deleg_restart_quarantined`]). Set for every kind of
    /// generation.
    pub restart_q: Option<RestartQuarantine>,
    /// The stream's next attempt is not before this (a failed send backs
    /// off; the root unreachable must not mean a send per event).
    pub stream_after: Option<WaitMs>,
    /// The current backoff, doubled per failure up to a bound.
    pub stream_backoff_ms: u64,
    /// The current back-off of consecutive refusals (the root answered: it is
    /// reachable, in its takeover gate or without the table yet), apart from
    /// `stream_backoff_ms`, which the transport failures of a root that was
    /// away raised to its bound: a root that comes back and refuses is asked
    /// again after a few ticks, not after the bound (`far-root-writes+unsettled`
    /// seed 1390: a settle ran out inside that wait).
    pub refusal_backoff_ms: u64,
    /// The root refused the stream (the generation is ending or unknown
    /// to it): nothing more is streamed until the log says.
    pub refused: bool,
    /// The root acknowledged the stream through this index (appended: it
    /// drives the flow control, not the commit).
    pub streamed_through: u64,
    /// The root told this delegate its own tenure has committed its
    /// appends of the stream through this index (`DelegateStreamAck.committed`,
    /// at most `streamed_through`): the root's copy a delegate without a
    /// recorded backup counts for its group (plan 43 Q16, ADR-41).
    pub root_committed: u64,
    /// The root's last word on this stream said its commit moves only at its
    /// next ship (`DelegateStreamAck.archive_bound`, plan 46 M1.2): the
    /// delegate waits for the root's push and asks only at the ship interval.
    pub archive_bound: bool,
    /// The next time this delegate may ask the root for its commit with an
    /// empty batch (`deleg_poke_root`): one stream tick after the last ask.
    pub poke_after: Option<WaitMs>,
    /// The ask for the commit in flight: its request id and when it was
    /// sent. Apart from `inflight`: a batch of rows never waits for it.
    pub ask: Option<(OpId, WaitMs)>,
    /// The current back-off of the ask (a refusal, a failure, a timeout),
    /// doubled up to a bound as the stream's; 0 after an answer.
    pub poke_backoff_ms: u64,
    /// The batch in flight: its request id and last index.
    pub inflight: Option<(OpId, u64)>,
    /// A renewal in flight: its request id and this node's honour clock
    /// when it was sent.
    pub renew: Option<(OpId, HonourMs)>,
    /// When `renew` was sent, on the wait clock: its give-up timeout is
    /// liveness, and a forward step must not fire it early.
    pub renew_sent_wait: WaitMs,
    /// Plan 41 R2: the renewal in flight is itself an early resend (or was
    /// already resent): it waits for the ordinary timeout. `renew_resend_next`:
    /// the one that just expired early, so the next one is marked.
    pub renew_resent: bool,
    pub renew_resend_next: bool,
    pub renew_timer: Option<TimerId>,
    /// When this node installed the generation: a grant it never got
    /// renewed lapses counting from here (`deleg_lapse_at`); honour clock.
    pub installed: HonourMs,
    /// The latest renewal of this generation known to have gone out: one
    /// of the core's that was answered (its send time), or one registered
    /// from the off-core lane ([`Core::on_deleg_renew_sent_off_core`]);
    /// `HonourMs(0)`: none yet. Not a renewal the core only decided on: the
    /// action that sends it may still wait behind a blocking one in the
    /// same step (the driver notes the dispatched ones, `OffCore`). The
    /// lane's next renewal is due a period after the later of the two.
    pub last_renew_sent: HonourMs,
    /// The send time of the latest renewal of this generation that was
    /// *answered* with a grant (the core's, or a lane's counted as a late
    /// answer): `WaitMs(0)`: none. The renewal after it falls due a period on,
    /// and that is the due time a loss is judged from
    /// ([`Core::deleg_loss_due`]); `last_renew_sent` also moves with the
    /// lane's unanswered registrations, which postpone only the lane.
    pub answered_sent: HonourMs,
    /// The period the core renews at (`on_deleg_renewed` arms its next
    /// timer so: a quarter of the ttl while the generation has lock
    /// grants out, else half); `0` before the first answer, when the
    /// request timeout is the period.
    pub renew_period_ms: u64,
    /// The lapse watch (`on_deleg_lapse`), re-armed by every renewal.
    pub lapse_timer: Option<TimerId>,
    /// The grant lapsed unrenewed (`on_deleg_lapse`): nothing executes
    /// or parks here, but renewals go on — a granted one ends the lapse.
    pub lapsed: bool,
    pub parked: Vec<ParkedDeleg>,
    /// Executed under this generation here (for `status`).
    pub executed: u64,
    /// ADR-56: when an op of this node's own last ran in each subtree (the
    /// core's executions and the engine fast path's, reported by
    /// [`Core::deleg_note_local`]), on the wait clock. A forwarded op is no
    /// activity of the holder's.
    pub last_own_op: BTreeMap<Ino, WaitMs>,
    /// Phase 2b: every grant is non-stealable (a designation or a sticky
    /// grant): honoured whatever the clock says.
    pub non_stealable: bool,
    /// Phase 2b: the root the stream last went to; a different one
    /// (a failover) gets every unretired transaction again.
    pub last_root: Option<NodeId>,
    /// The last root the stream went to that was not `None` (`last_root`
    /// forgets it while no root is in view): a root found after a spell
    /// with none is a failover when it is another node than this one.
    pub known_root: Option<NodeId>,
    /// Phase 2b: the backup peer this delegate appends to before it
    /// acknowledges, and what it has acknowledged.
    pub backup: Option<NodeId>,
    pub backup_acked: u64,
    pub backup_sent_through: u64,
    pub backup_inflight: Option<(OpId, u64)>,
    pub backup_sealed: bool,
    pub backup_failures: u32,
    /// The next append is not before this: the backup answered short of
    /// rows this delegate has already retired (they are in the log, the
    /// backup has not applied them yet), so a re-send now would be
    /// answered the same.
    pub backup_after: Option<WaitMs>,
    /// The backup the in-flight renewal named (the root must learn a
    /// newly chosen backup before the next timer).
    pub renew_backup: Option<NodeId>,
    /// When the in-flight stream batch / backup append was sent: one
    /// lost in a partition (no transport failure), or answered late, is
    /// re-sent after `deleg_request_timeout_ms` (and a backoff), not
    /// waited for forever.
    pub inflight_at: WaitMs,
    /// (The backup append's send, on the honour clock: a late answer is
    /// kept as a [`LateReq`], whose `sent` is.)
    pub backup_inflight_at: HonourMs,
    /// `backup_inflight_at` on the wait clock (the give-up timeout).
    pub backup_inflight_wait: WaitMs,
    /// The stream round: this node's incarnation and a fresh id at the
    /// install and at every re-stream to a new root
    /// ([`Core::deleg_new_round`]). Every batch and renewal carries it and
    /// the root echoes it; an answer counts only in the round it
    /// answers. A root that changed and came back (R → R2 → R) is the
    /// same node, not the same stream: the re-stream to R2 restarted from
    /// what the log carries, and R's answer from its first tenure would
    /// claim rows R2's tenure never got (long-delegated-backup seed
    /// 75504's class); and an answer to this node's previous incarnation
    /// names request ids this one reuses.
    pub stream_round: u64,
    /// The round of the latest granting answer the root's core sent for this
    /// generation (`on_deleg_renewed`): the lock content of the first
    /// answer (handoff, grace, floor, cut) has been installed in that round.
    /// An answer off the core (plan 41 R2) extends only a generation whose
    /// current round it names here — "never the first grant" holds per
    /// round, so a root that changed leaves it to its core to grant first.
    pub core_granted_round: Option<u64>,
    /// The last batch sent carried this generation's grants on these
    /// inodes out of the subtree (`Core::deleg_leaving_grants`); its last
    /// index. Dropped from this table once the root acknowledged it.
    pub leaving: Option<(u64, Vec<Ino>)>,
    /// Plan 43 C2: the generation's group as this replica's table records it
    /// (version 0 and no backups: no record, the group is the delegate and
    /// the root). Learned only from the replica — the log, or the committed
    /// pre-S3 stream — never from a message (D4): a configuration counts
    /// once its master holds it durably.
    pub group_version: u64,
    pub group_backups: Vec<NodeId>,
    /// A change this delegate asked for (`DelegRenew.backup`) that its table
    /// does not record yet: until it does, an acknowledgement needs the
    /// members of the applied group and of this one (D4, the union rule) —
    /// the root may end the generation by either version.
    pub group_requested: Option<Vec<NodeId>>,
    /// Plan 46 M1.1 step 5: the groups this generation has had that its
    /// rows are not all committed under yet (oldest first). A row keeps the
    /// rule of the group it was journaled in, and a change of the group
    /// commits under old and new: nothing past a closed group's end is
    /// answered before that group's own members hold its end (or the log
    /// carries it).
    pub group_spans: Vec<GroupSpan>,
    /// The highest index answered under a group (`removal_safe`'s bound).
    pub acked_max: u64,
    /// Plan 43 C2e follow-up: this process recorded, synced, that it holds a
    /// grant of the generation on this boot (`Replica::deleg_note_held`).
    pub held_noted: bool,
    /// A designee: the stream index through which this process synced its
    /// journal; its stream sends nothing past it (`deleg_stream`).
    pub synced_through: u64,
    /// A designee: the index a sync in flight covers
    /// (`Action::SyncDelegStream`).
    pub sync_inflight: Option<u64>,
    /// The first index of this stream whose `deps` name a row its
    /// generation ended without (`Replica::deps_lost`): never streamed, the
    /// generation stopped before it (`deleg_stream`).
    pub lost_from: Option<u64>,
}

/// Plan 46 M1.1 step 5: a closed group of a delegate generation
/// ([`DelegateState::group_spans`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpan {
    /// The group's members (none: the group is the delegate and the root).
    pub backups: Vec<NodeId>,
    /// What the backup of that group held when the group closed (it may
    /// hold more since, while it is the generation's backup still).
    pub acked: u64,
    /// The generation's stream index when the record that closed the group
    /// was applied.
    pub end: u64,
}

/// A renewal or backup append [`Core::deleg_expire_inflight`] gave up
/// waiting for. The timeout re-sends what may have been lost (a partition
/// drops a message with no transport failure); it says nothing about the
/// answer to the first send, which a loaded peer gives late, not never.
/// Thrown away, an answer later than the timeout never counted (under
/// `stress-ng-fs-nodes` a delegate whose renewal answers all came late
/// lapsed). A late answer is used for what it can only add: a backup's
/// acknowledgement from the backup still appended to, a granted renewal
/// (honoured from its own send). Stream batches need no such record:
/// the root's acknowledgement is cumulative, and counts whenever it
/// comes in the round it answers ([`Core::on_delegate_stream_ack`]).
#[derive(Debug, Clone, Copy)]
struct LateReq {
    gen: u64,
    kind: ReqKind,
    /// The root or backup it went to.
    to: Option<NodeId>,
    /// This node's honour clock at the send.
    sent: HonourMs,
    /// The request was completed by the root's off-core answer
    /// ([`Core::on_deleg_renewed_off_core`]): the core's own answer, when
    /// it comes, is still its verdict on it, a refusal included.
    off_core: bool,
}

/// What the off-core renewal lane (`crate::deleg_lane`) needs to renew one
/// generation of this node's while the core is stuck in a step: published
/// after every step ([`Core::deleg_renew_certs`]), so it is the state at
/// the end of the last finished step, never newer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegRenewCert {
    pub gen: u64,
    /// The root the generation renews with, and the stream round it is in.
    pub root: NodeId,
    pub round: u64,
    /// When the next renewal is due: a period after the latest one known
    /// to have gone out and been answered or registered (the install,
    /// before the first). The driver adds the core's dispatched sends.
    pub next_due: HonourMs,
    pub period_ms: u64,
}

/// Who sent a renewal the root is answering ([`Core::on_deleg_renew`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenewFrom {
    /// The delegate's core: its backup, whether no renewal of the
    /// generation was granted to it yet, and (noted by the caller before
    /// this) the stream head it executed by the time it sent.
    Core { backup: Option<NodeId>, first: bool },
    /// The delegate's off-core lane (`crate::deleg_lane`): the core was
    /// stuck, and the lane knows neither its head nor its current backup.
    /// The root extends the grant, answers as for any renewal, and learns
    /// nothing from it about the delegate's stream or backup.
    OffCore,
}

/// How long a late answer is still used ([`LateReq`]).
const LATE_ANSWER_MS: i64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReqKind {
    Stream,
    /// A delegate's empty batch asking the root for its commit
    /// (`deleg_poke_root`).
    Poke,
    Renew,
    Recall,
    /// Phase 2b: a delegate's append to its backup.
    BackupAppend,
    /// Phase 2b: the root's seal request to a dead delegate's backup.
    Seal,
    /// Plan 46 M1.2 step 7 (c3-spec D6): the next root's seal request to the
    /// previous root of a generation without a recorded backup.
    SealPrevRoot,
}

impl DelegationState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("dl_gens", self.gens.len()),
            ("dl_mine", self.mine.len()),
            ("dl_by_req", self.by_req.len()),
            ("dl_late", self.late.len()),
            ("dl_pending_exec", self.pending_exec.len()),
            (
                "dl_parked",
                self.mine.values().map(|d| d.parked.len()).sum(),
            ),
            ("dl_backing", self.backing.len()),
            ("dl_sealed", self.sealed.len()),
            ("dl_designating", self.designating.len()),
            ("dl_growing", self.growing.len()),
            ("dl_install_waiting", self.install_waiting.len()),
            ("dl_commit_waiting", self.commit_waiting.len()),
        ]
    }
}

/// How much of the root's work this node can do now, as far as the
/// generations its table names go ([`Core::root_deleg_level`]): ordered,
/// so a resync is due when the level rises.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RootSync {
    /// Not a root that serves generations (no lease, a gate whose journal
    /// work is not done, a release in its final section).
    #[default]
    None,
    /// Plan 46 M0.3 (S5, ADR-42): a takeover gate whose journal work is
    /// done (`PendingGate::shippable`). The generations of other nodes
    /// are taken on and served; none is recalled, sealed or reclaimed.
    Gated,
    /// The lease is usable: every root duty.
    Usable,
}

/// How long a recall answered for a generation this node's table has
/// not named yet is remembered ([`DelegationState::handed_back`]).
const HANDED_BACK_UNSEEN_MS: i64 = 60_000;

/// How many freezes [`DelegationState::freezes`] keeps.
const FREEZES_KEPT: usize = 8;

/// Plan 46 M1.3 step 5: one `Control::StickyRelease` in progress. Per
/// stream it names, the subtrees it releases; each part is released when the
/// holder has drained (no op of the subtrees in flight here, the root has
/// appended through the last row of the stream), and the command is done
/// when the root has appended the release rows. Until a part's row is
/// journaled (and at most [`STICKY_HOLD_MAX_MS`] from the start) new ops
/// under its subtrees are held, so that the drain does not chase a stream
/// that never stops.
#[derive(Debug)]
pub(crate) struct StickyReleaseCmd {
    /// The control to answer once done (`wait`).
    pub op: Option<OpId>,
    pub started: WaitMs,
    pub deadline: WaitMs,
    /// Generation -> the subtrees to release.
    pub parts: BTreeMap<u64, BTreeSet<Ino>>,
    /// ADR-56: a handover's release (the root asked, the holder found itself
    /// quiet); counted, and put back as a wait if it gives up.
    pub handover: bool,
    /// The root that asked (a handover's; `0` for a release by hand).
    pub root: NodeId,
    /// The core step that made the command ([`DelegationState::step`]). The
    /// engine's fast path is told to leave the command's subtrees by the
    /// view the driver mirrors after that step; the release goes only once a
    /// sample the driver took after that mirror (`Core::deleg_note_local`)
    /// shows no fast-path op in flight there.
    pub made_at: u64,
}

/// ADR-56: the root asked this node, the holder of sticky subtrees of stream
/// `gen`, to hand `dirs` over to a node that dominates them. The holder
/// decides when ([`Core::handover_progress`]): at once when none of its own
/// ops is in flight there, else once its own ops there have been idle for
/// `handover_grace_ms`. Forwarded ops never count.
#[derive(Debug, Clone)]
pub(crate) struct HandoverWait {
    pub gen: u64,
    pub dirs: BTreeSet<Ino>,
    /// The root that asked (the release row's note goes to it).
    pub root: NodeId,
    /// When the first request of this wait arrived.
    pub asked: WaitMs,
    /// When the root last asked (a repeated request refreshes it): the wait
    /// expires [`Core::handover_patience_ms`] after it, as the root's
    /// request does.
    pub last_asked: WaitMs,
}

/// How long a draining release holds the new ops of its subtrees at most
/// (a stalled drain, a root that does not answer, must not stop the
/// holder's clients: past it the ops run and the release goes on waiting).
pub(crate) const STICKY_HOLD_MAX_MS: i64 = 5_000;

/// How often the holder's releases in progress look again with no event.
const STICKY_RELEASE_TICK_MS: i64 = 200;

/// Plan 46 M1.2 step 11 (maintainer decision 2026-11-06, "Leave ends grants
/// first"): a node leaves the roster only once the log has ended every grant
/// it holds. The holder that is alive releases them (`StreamReleased` rows,
/// the root then journals `DelegateNodeEnded`: [`Core::on_control_release_grants`]);
/// for a node that is gone, the root recalls them and ends them at the TTL
/// ([`Core::on_control_end_grants_of`]). A holder's own leave is answered when
/// every stream it held is stopped and the root's acknowledgement covers its
/// release row (its own table is no witness: its journal applies the release
/// at once); the root's, when no stream of the node is live in its table and
/// its generations; either at the deadline with an error.
#[derive(Debug)]
pub(crate) struct LeaveWait {
    pub op: OpId,
    /// The leaving node (this one for a holder's own leave).
    pub who: NodeId,
    pub deadline: WaitMs,
    /// The streams already asked to release (holder side), and when.
    pub asked: BTreeMap<u64, WaitMs>,
}

/// Plan 46 M1.3 step 13: which streams `repair abandon --gen/--node` names.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AbandonSel {
    Gen(u64),
    Node(NodeId, Option<u64>),
}

/// What a validated abandon will do (all of it checked before any is started).
#[derive(Debug, Default)]
pub(crate) struct AbandonPlan {
    /// Placed generations waiting for their group: ended at once.
    pub placed: Vec<u64>,
    /// Placed generations not waiting: left to their TTL (`--node` only).
    pub placed_left: Vec<u64>,
    /// Non-stealable streams: end after their read delegations are done.
    pub streams: Vec<u64>,
    /// `--force` overrode a heard-lately refusal: who was heard, and when.
    pub forced: BTreeMap<u64, (NodeId, &'static str, u64)>,
}

/// Plan 46 M1.3 step 13: one `repair abandon` of non-stealable streams
/// waiting for the read delegations their holders granted (C11: recall,
/// else expire).
#[derive(Debug)]
pub(crate) struct AbandonWait {
    pub op: OpId,
    /// The streams; each ends (`end_generation(.., Some(known))`) once its
    /// reads are done.
    pub gens: BTreeSet<u64>,
    /// Void requests not answered yet: req -> (reader, gen).
    pub asked: BTreeMap<OpId, (NodeId, u64)>,
    /// How many void requests each stream's broadcast sent.
    pub sent: BTreeMap<u64, usize>,
    /// `command time + read_delegation_ttl_ms + expiry_margin_ms` (the root's wait
    /// clock): a holder's read grant ran `ttl` from the reader's send, and
    /// the holder, gone at the command, granted none after it.
    pub deadline: WaitMs,
    /// `--force` overrode a heard-lately refusal, per stream (the audit).
    pub forced: BTreeMap<u64, (NodeId, &'static str, u64)>,
    /// What the answer reports.
    pub lines: Vec<String>,
    /// The lease epoch this root held at the command: a different one, or a
    /// lost lease, means the root was deposed and the command is refused;
    /// a lease that is merely not usable for a moment is waited out.
    pub epoch: Option<crate::Epoch>,
    /// The peers were not known when the void went out (no roster or no
    /// peer snapshot yet): nothing was asked, and the deadline decides.
    pub audience_unknown: bool,
}

/// How soon an abandon whose deadline passed looks again when the root is
/// not usable (the end is journaled once it is).
const ABANDON_RETRY_MS: u64 = 200;

/// The generation number of a [`PeerMsg::DelegateStream`] that is no stream:
/// a node tells the root it is leaving (or no longer is) with a batch of no
/// rows and a [`constellation_meta::locks::ReleaseNote`] saying so. The
/// root answers with a `DelegateStreamAck` for the same generation. Real
/// generations are `epoch << 32 | n`, and never 0.
pub(crate) const LEAVE_NOTE_GEN: u64 = 0;

/// Why a node is marked leaving at its root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaveMark {
    /// A mark the node's own note made lapses (the node may have stayed up,
    /// or come back under the same id, and nobody takes the mark back); one
    /// an operator's leave made (`None`) stays until that leave fails.
    pub until: Option<WaitMs>,
    /// The request id of the note that made it: a take-back older than the
    /// mark is no take-back.
    pub req: u64,
}

/// A note from a leaving node to its root, until acknowledged.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaveTell {
    /// Leaving (`true`), or no longer (a leave that failed).
    pub leaving: bool,
    pub to: NodeId,
    pub req: OpId,
    /// When it was sent (`None`: due now, a send that failed).
    pub sent: Option<WaitMs>,
    /// A take-back is not sent after this.
    pub until: WaitMs,
}

/// ADR-55 (R-H): the root's own sticky stream, until it is honoured.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnGrant {
    /// The journal seq of the stream's last `DelegateNode` record
    /// (`u64::MAX`: not known; the stream is never honoured on it).
    pub record: u64,
    /// The root rows at or below this journal seq are the ones R-H (b)
    /// looks at: `record`, or, for a stream found again after a restart
    /// with its record in the log already, the root rows journaled so far.
    pub scan_to: u64,
    /// The decision: see [`DelegationState::own_grants`].
    pub honoured: bool,
    /// When the record was journaled: what `Stats::root_own_honour_wait_ms_max`
    /// and `root_own_commit_wait_ms_max` measure from (a growth that follows
    /// before the archive carries the first record does not restart it).
    pub since: WaitMs,
    /// When this core first saw the archive carry the record, and so when
    /// an unshipped root row of the stream's keys started to be waited for
    /// (the liveness bound counts from here).
    pub covered_at: Option<WaitMs>,
}

#[derive(Debug, Default)]
pub(crate) struct DelegationState {
    /// The `(gen, idx)` rows `deleg_drain_tail` appended since the last
    /// [`Core::take_drained_rows`]: the simulation's oracle judges a row
    /// drained from a sealed tail by the tenure that drained it.
    pub(crate) drained_rows: Vec<(u64, u64)>,
    /// Plan 41 L0: this tenure took an unexpired lease over (a seal, or
    /// `ack=s3`; `note_fast_takeover`), so a predecessor's last delegation
    /// renewal may have been decided up to `backup_takeover_ms` after its
    /// last proof of liveness, which came before this tenure's takeover
    /// CAS: a generation inherited from it is outwaited that much longer
    /// (`delegation_sync`). 0 after any other acquisition.
    pub(crate) inherit_slack_ms: u64,
    /// The root's generations (this node holds the lease): the live ones,
    /// and the ended ones until [`Core::deleg_forget_ended`] forgets them.
    pub gens: BTreeMap<u64, GenState>,
    /// The generations this tenure ended, by when they may be forgotten
    /// ([`Core::deleg_forget_ended`]).
    forget_at: BTreeSet<(WaitMs, u64)>,
    /// This tenure forgot an ended generation: `gens` may be empty though
    /// it knew generations (see [`Core::deleg_known`]).
    forgot: bool,
    /// Plan 41 R2: the generations this tenure sent a granting answer for,
    /// and the lock floor the latest carried (`Core::deleg_settled`).
    pub(crate) answered_granting: BTreeSet<u64>,
    floor_sent: BTreeMap<u64, Position>,
    /// Designations not journaled yet because this root's read
    /// delegations under the directory were live (`Core::delegate_dir`):
    /// directory → designee. No read delegation is granted under one
    /// meanwhile, so the next sync can journal it.
    pub designating: BTreeMap<Ino, NodeId>,
    /// A grant that would grow a live stream, not journaled yet because this
    /// root's read delegations under the directory were live: directory →
    /// (delegate, until when the recalled grants have expired in any case).
    /// No read delegation is granted under one meanwhile; the caller's next
    /// attempt journals it. A stream's renewals never wait for them (step 9).
    pub growing: BTreeMap<Ino, (NodeId, WaitMs)>,
    /// Whether a certificate is published (`Core::deleg_publish_root`).
    cert_set: bool,
    /// The generations this node is the delegate of.
    pub mine: BTreeMap<u64, DelegateState>,
    /// Plan 43 C3: per generation, the highest index below this node's
    /// first holder append its tenure has not committed, with what it was
    /// computed from (`Core::holder_append_cap`): recomputed only when one
    /// of those changed.
    pub holder_caps: BTreeMap<u64, (HolderCapStamp, u64)>,
    /// Plan 43 Q16 (ADR-41): the same for [`Core::root_append_cap`], the
    /// root's own committed prefix of its appends of a delegate's stream
    /// (kept apart: `holder_append_cap` excludes a lone tenure's appends,
    /// this one counts them): `(stamp, scan start, cap)`.
    pub root_caps: BTreeMap<u64, (HolderCapStamp, u64, u64)>,
    /// Plan 43 C2d: what [`Core::report_deleg_durable`] last told the
    /// replica's session, per generation.
    pub reported: BTreeMap<u64, constellation_meta::DelegDurable>,
    /// Phase 2b: the generations this node backs, `gen -> (delegate,
    /// acked)`, and the ones it sealed.
    pub backing: BTreeMap<u64, (NodeId, u64)>,
    pub sealed: BTreeSet<u64>,
    pub(crate) by_req: BTreeMap<OpId, (u64, ReqKind)>,
    /// Backup appends and renewals that `deleg_expire_inflight` stopped
    /// waiting for: their answer may still come, and is used when it does
    /// (`LateReq`).
    late: BTreeMap<OpId, LateReq>,
    /// The latest freezes of the heartbeat thread handed over
    /// (`Event::OwnFreeze`), oldest first, at most [`FREEZES_KEPT`].
    freezes: Vec<(WaitMs, WaitMs)>,
    /// Local ops whose execution waits for a recall or for `deps` (their
    /// `finish` is deferred to the parked continuation).
    pub pending_exec: BTreeSet<Rid>,
    pub(crate) stream_timer: Option<TimerId>,
    /// Path B: the generations of this node the table lists that are not
    /// installed yet because an ended generation's rows wait for its end
    /// (`delegation_sync`), by generation: since when, the ended
    /// generations (`None`: the journal could not be read), and when the
    /// last warning went out.
    pub(crate) install_waiting: BTreeMap<u64, InstallWait>,
    /// Plan 46 M1.2 step 8b (M1.0 answer 4): the generations of this node
    /// the table lists whose `DelegateNode` record the root's configuration
    /// has not committed as far as this replica can tell (it holds the
    /// record only as an uncommitted streamed entry), by generation: since
    /// when. Not installed, so nothing executes and no renewal goes out,
    /// until the log carries the record (the archive, for a lone root).
    pub(crate) commit_waiting: BTreeMap<u64, WaitMs>,
    /// Plan 46 M1.3 step 9 (ADR-55, R-H): the root's own sticky streams by
    /// generation, with the journal seq of the last `DelegateNode` record of
    /// each and whether the stream is honoured: the archive carries that
    /// record and no root row at or below it is unshipped whose keys resolve
    /// to the stream. Decided in [`Core::deleg_after_event`]; once true it
    /// stays true (no root row under a live own generation enters the
    /// journal, R-I4) until the next record (a growth) of the stream.
    pub(crate) own_grants: BTreeMap<u64, OwnGrant>,
    /// The streams this node granted itself while it was the root (a deposed
    /// root keeps streaming them: `Stats::root_own_resumed`).
    pub(crate) own_minted: BTreeSet<u64>,
    /// ADR-55: this process installed a stream under a restart quarantine
    /// (it held delegations before it restarted): a stream of its own it holds
    /// waits for its claim of the lease instead of executing in the gate.
    pub(crate) restarted_holder: bool,
    /// Tests only: honour a grant the root has not committed, as before step
    /// 8b, to pin what the orphan rule does for a delegate that executed
    /// under one (the rule stays as the defence behind the honour rule).
    #[cfg(test)]
    pub(crate) test_honour_uncommitted: bool,
    /// The timer that runs `delegation_sync` again while an install waits
    /// (or a group could not be read): a segment's records are not the
    /// only way the wait ends.
    pub(crate) install_retry: Option<TimerId>,
    /// When a delegate that knows no root may read the lease for it again
    /// ([`Core::deleg_learn_root`]): once a stream tick.
    root_read_after: Option<WaitMs>,
    /// What the last table sync found this node as a root (and so which
    /// generations it learned from the table): see
    /// [`Core::deleg_root_sync_due`].
    pub(crate) root_synced: RootSync,
    /// Plan 30 §M10: an active continuation epoch — no delegation.
    pub(crate) epoch_active: bool,
    /// Generations this node's table still delegates to it whose recall
    /// it answered, with the root that recalled them: the subtree's lock
    /// grants went back there (`on_deleg_recall`), so lock requests and
    /// renewals for it are sent there (`NotOwner { root }`) rather than
    /// refused with `NotOwner { 0 }` until the `DelegateNodeEnded` record reaches
    /// this table — under an S3 cut, only after it. Also for a
    /// generation never installed here (an epoch was active when the
    /// table named it). `(root, recalled at, row seen)`: dropped once the
    /// generation's table row was seen here and is gone, or — a row
    /// never seen (the recall overtook the record naming this node) —
    /// after [`HANDED_BACK_UNSEEN_MS`].
    pub handed_back: BTreeMap<u64, (NodeId, WaitMs, bool)>,
    /// Plan 46 M1.2 step 11: `Leave` ends grants first. The leaves waiting
    /// for the grants of a node to end in the log (see [`LeaveWait`]).
    pub leave_waits: Vec<LeaveWait>,
    pub leave_timer: Option<TimerId>,
    /// Plan 46 M1.3 step 5: the holder's releases of sticky subtrees in
    /// progress (see [`StickyReleaseCmd`]).
    pub sticky_cmds: Vec<StickyReleaseCmd>,
    pub sticky_timer: Option<TimerId>,
    /// ADR-56: the handover requests this holder has not answered (by its
    /// own release row) yet.
    pub handovers: Vec<HandoverWait>,
    /// ADR-56: ops the engine's fast path had admitted and not yet
    /// journaled when it last reported ([`Core::deleg_note_local`]), per
    /// (stream, subtree).
    pub fast_inflight: BTreeMap<(u64, Ino), u64>,
    /// Steps `Core::handle` has run (see [`StickyReleaseCmd::made_at`]).
    pub step: u64,
    /// The step the engine's last report followed.
    pub sampled_at: u64,
    /// Plan 46 M1.3 step 5: the generations this process installed and the
    /// table still lists. A stream of this node that released every subtree
    /// and is not among them lost its state with a restart
    /// (`delegation_sync` installs a remnant to ship the release row).
    pub installed_gens: BTreeSet<u64>,
    /// Plan 46 M1.3 step 13: the `repair abandon` commands waiting for read
    /// delegations (see [`AbandonWait`]), and the timer of their earliest
    /// deadline.
    pub abandons: Vec<AbandonWait>,
    pub abandon_timer: Option<TimerId>,
    /// ADR-46: where the read grants the previous process's delegates made
    /// may last to, loaded at start (`Core::deleg_restart_start`); `None`:
    /// nothing is left of them.
    pub restart_horizon: Option<WaitMs>,
    /// The process-level timer at `restart_horizon` (a
    /// `Timer::DelegRestartQuarantine(0)`): it records that the previous
    /// process's grants are gone whether or not a generation was installed.
    pub restart_settle_timer: Option<TimerId>,
    /// The nodes leaving (this node is their root): nothing is granted to
    /// them from the moment their leave starts, so no grant is made between
    /// the end of their grants and the retirement of their registry entry.
    pub leaving: BTreeMap<NodeId, LeaveMark>,
    /// The latest leave sequence (the request id of the note) of each node
    /// that took its leave back: a leaving note of that sequence or an
    /// older one (a delayed duplicate) marks nothing.
    pub leave_taken_back: BTreeMap<NodeId, u64>,
    /// This node is leaving: its release notes say so to the root, and it
    /// installs no grant from here on.
    pub leave_self: bool,
    /// The root that acknowledged this node's leave note (`LEAVE_NOTE_GEN`):
    /// the leave counts only once the current root is that one.
    pub leave_told: Option<NodeId>,
    /// The note to the root that has not been acknowledged (a leave note,
    /// or the note taking it back after a failed leave).
    pub leave_tell: Option<LeaveTell>,
    /// The generations a release request reached this node for at least once
    /// (the simulation's loss of the first one, L3).
    pub release_first: BTreeSet<u64>,
    /// A lease release waiting for the generations whose recall was in
    /// flight when it was issued (`Core::release_recall_waits`).
    pub(crate) release_wait: Option<ReleaseWait>,
}

/// A lease release's wait for generations being recalled: their wait ids
/// are in the release's park, the bound's timer is armed.
#[derive(Debug, Clone)]
pub(crate) struct ReleaseWait {
    pub gens: BTreeSet<u64>,
    pub timer: TimerId,
    /// The bound passed with a generation not ended: the release goes
    /// without waiting for it (as before the wait).
    pub bound_passed: bool,
}

/// One generation that waits for its group (plan 43 C2e, `status`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitingGenView {
    pub gen: u64,
    pub dirs: Vec<Ino>,
    pub delegate: NodeId,
    pub waited_ms: u64,
    /// When this root last heard from the delegate, ms ago: a renewal, a
    /// stream batch, or any exchange on its link (`None`: never).
    pub delegate_heard_ms: Option<u64>,
    /// A strict member's tail is in hand but not appended to its end:
    /// `(member, cursor, end)`.
    pub tail: Option<(NodeId, u64, u64)>,
    pub members: Vec<WaitingMemberView>,
}

/// What the root knows of one member it asked to seal, in ms ago on its
/// wait clock (`None`: never).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitingMemberView {
    pub node: NodeId,
    /// Of the latest group version (the end waits for it).
    pub strict: bool,
    /// A previous root this root seals (c3-spec D6); the end waits for it.
    pub previous_root: bool,
    pub asked_ms: Option<u64>,
    pub failed_ms: Option<u64>,
    pub answered_ms: Option<u64>,
    /// When this root last heard from it at all: its seal's answer, or any
    /// exchange on its link.
    pub heard_ms: Option<u64>,
}

/// Plan 30 §M11's view for `status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegView {
    /// `(dirs, gen, until, stopped, streamed_through, executed, parked,
    /// class)`: one row per stream held (plan 46 M1.2 step 9: a node holds one
    /// stream, whatever its subtrees), `dirs` the subtrees it honours;
    /// `until` a reading of this node's honour clock (plan 43
    /// C7-H1; the engine's `DelegView` judges it on that clock); `class`
    /// the name of the class this node's own clients' answers wait for
    /// (plan 43 C2e: `commit_name`).
    #[allow(clippy::type_complexity)]
    pub mine: Vec<(Vec<Ino>, u64, i64, bool, u64, u64, usize, String)>,
    /// `(dirs, node, gen, cursor, until_ms (wall, for status), recall, ended,
    /// waiting_for_group)`: the last says the generation cannot end because
    /// no member of its group has handed over the tail (plan 43 C2e).
    #[allow(clippy::type_complexity)]
    pub gens: Vec<(Vec<Ino>, NodeId, u64, u64, i64, String, bool, bool)>,
    /// Plan 43 C2e: the generations that cannot end for want of their
    /// group's tail, with what the root knows of each member (what
    /// `abandon --gen` is weighed by).
    pub waiting: Vec<WaitingGenView>,
    /// Path B: `(dirs, gen, waited_ms, ended)` for each generation of this
    /// node the table lists that is not installed yet, because an ended
    /// generation's rows still wait for its end in the log (`ended`
    /// empty: the journal could not be read).
    pub install_waiting: Vec<(Vec<Ino>, u64, u64, Vec<u64>)>,
    pub pending_exec: usize,
    /// Phase 2b: per generation held here, `(gen, backup, backup_acked)`.
    pub backups: Vec<(u64, NodeId, u64)>,
    /// Phase 2b: per generation on the root, `(gen, kind, backup)`.
    pub kinds: Vec<(u64, String, NodeId)>,
}

/// Path B: an install of a generation that waits for an ended
/// generation's end (`DelegationState::install_waiting`).
#[derive(Debug, Clone)]
pub(crate) struct InstallWait {
    pub dirs: Vec<Ino>,
    pub since: WaitMs,
    pub ended: Option<Vec<u64>>,
    pub warned: Option<WaitMs>,
}

/// How often `delegation_sync` runs again while an install waits.
const INSTALL_RETRY_MS: u64 = 1_000;
/// A waiting install is warned about once it waited this long, and then
/// at most this often.
const INSTALL_WAIT_WARN_MS: i64 = 5_000;
const INSTALL_WAIT_WARN_EVERY_MS: i64 = 60_000;

/// The keys `op` touches, as ownership is resolved over them.
impl Core {
    pub fn deleg_view(&self) -> DelegView {
        let mut view = DelegView {
            gens: self
                .dl
                .gens
                .iter()
                .map(|(gen, g)| {
                    (
                        g.dir_list(),
                        g.node,
                        *gen,
                        g.cursor,
                        // Status: a wall stamp, from the wait-clock end.
                        self.clk.wall_of_wait(self.gen_until(*gen)).0,
                        format!("{:?}", g.recall),
                        g.ended,
                        g.waiting_for_group && !g.ended,
                    )
                })
                .collect(),
            kinds: self
                .dl
                .gens
                .iter()
                .map(|(g, s)| (*g, format!("{:?}", s.kind()), s.group.backup().unwrap_or(0)))
                .collect(),
            waiting: self
                .dl
                .gens
                .iter()
                .filter(|(_, g)| g.waiting_for_group && !g.ended)
                .map(|(gen, g)| {
                    let ago = |t: Option<WaitMs>| t.map(|t| self.clk.wait.since(t).max(0) as u64);
                    let members = g
                        .group
                        .strict()
                        .iter()
                        .map(|n| (*n, true, false))
                        .chain(g.group.best_effort().into_iter().map(|n| (n, false, false)))
                        .chain(
                            g.prev_roots
                                .iter()
                                .filter(|n| !g.group.strict().contains(n))
                                .map(|n| (*n, true, true)),
                        )
                        .map(|(node, strict, previous_root)| {
                            let note = g.member_notes.get(&node).cloned().unwrap_or_default();
                            WaitingMemberView {
                                node,
                                strict,
                                previous_root,
                                asked_ms: ago(note.asked),
                                failed_ms: ago(note.failed),
                                answered_ms: ago(note.answered),
                                heard_ms: ago(self.deleg_member_heard(g, node)),
                            }
                        })
                        .collect();
                    WaitingGenView {
                        gen: *gen,
                        dirs: g.dir_list(),
                        delegate: g.node,
                        waited_ms: ago(g.waiting_since).unwrap_or(0),
                        delegate_heard_ms: ago(self.deleg_delegate_heard(g)),
                        tail: g.sealed_tail.as_ref().map(|t| (t.from, g.cursor, t.end)),
                        members,
                    }
                })
                .collect(),
            install_waiting: self
                .dl
                .install_waiting
                .iter()
                .map(|(gen, w)| {
                    (
                        w.dirs.clone(),
                        *gen,
                        self.clk.wait.since(w.since).max(0) as u64,
                        w.ended.clone().unwrap_or_default(),
                    )
                })
                .collect(),
            ..DelegView::default()
        };
        self.deleg_view_local(&mut view);
        view
    }

    /// [`Self::deleg_view`]'s parts about the generations this node is the
    /// delegate of (`mine`, `backups`, `pending_exec`), refreshed in
    /// `view`: what the engine's fast path reads after every step. The
    /// root's rows (`gens`, `kinds`) walk every generation it knows, the
    /// ended ones its delegates may still take for live included
    /// ([`Self::deleg_forget_ended`]), and are for `status` alone.
    pub fn deleg_view_local(&self, view: &mut DelegView) {
        view.mine = self
            .dl
            .mine
            .values()
            .map(|d| {
                // ADR-56: the subtrees a holder's release is draining leave
                // the fast path's view (its ops go to the core, which holds
                // them while the hold lasts, `sticky_holds`, and runs them
                // through the core after) for as long as the command lasts,
                // so that no fast-path op lands behind the release row.
                let draining: Vec<&BTreeSet<Ino>> = self
                    .dl
                    .sticky_cmds
                    .iter()
                    .filter_map(|c| c.parts.get(&d.gen))
                    .collect();
                (
                    d.dirs
                        .iter()
                        .copied()
                        .filter(|x| !draining.iter().any(|p| p.contains(x)))
                        .collect(),
                    d.gen,
                    d.until.0,
                    // A generation inside its restart quarantine is not
                    // admitted by the engine's fast path: the op goes to
                    // the core and parks.
                    d.stopped
                        || !self.own_honoured(d.gen)
                        || d.restart_q
                            .as_ref()
                            .is_some_and(|q| q.timer.is_some() && !q.voided),
                    d.streamed_through,
                    d.executed,
                    d.parked.len(),
                    // The class published at the last event
                    // ([`Self::report_deleg_durable`]), not decided again
                    // for every step's view.
                    self.dl
                        .reported
                        .get(&d.gen)
                        .map_or("", |r| super::commit_name(r.commit))
                        .to_string(),
                )
            })
            .collect();
        view.pending_exec = self.dl.pending_exec.len();
        view.backups = self
            .dl
            .mine
            .values()
            .filter_map(|d| d.backup.map(|b| (d.gen, b, d.backup_acked)))
            .collect();
    }

    /// How many generations the root knows, ended ones included (the
    /// rows of [`Self::deleg_view`]).
    pub fn deleg_known_generations(&self) -> usize {
        self.dl.gens.len()
    }

    /// Phase 2b: the placement's busiest subtrees (`status`).
    pub fn placement_top(&self) -> Vec<(Ino, NodeId, u64, u64)> {
        self.pl.top.clone()
    }

    fn me(&self) -> NodeId {
        self.cfg.node_id
    }

    /// Generation `gen`'s window end on the root's wait clock, from the
    /// shared window table (plan 41 R1); `WaitMs(0)` for a generation the
    /// table does not know.
    pub(crate) fn gen_until(&self, gen: u64) -> WaitMs {
        self.dw.until(gen).unwrap_or(WaitMs(0))
    }

    /// Whether this tenure has known a generation, ended ones included:
    /// what `!dl.gens.is_empty()` answered before ended generations were
    /// forgotten ([`Self::deleg_forget_ended`]), for the checks that
    /// read it so (the lock lane stays off for the rest of the tenure
    /// once it delegated, as before).
    pub(crate) fn deleg_known(&self) -> bool {
        !self.dl.gens.is_empty() || self.dl.forgot
    }

    /// When ended generation `gen` may be forgotten: once its window
    /// has passed by the expiry margin. By then its delegate's grant has
    /// lapsed, and every lock grant it made or renewed with it (capped by
    /// the window); an answer of its delegate still in flight finds it
    /// unknown, as it would at a successor, and is dropped as one for an
    /// ended generation is (`lock_install_returned`: no grants back; the
    /// floor and barrier a drained answer carried were noted with it, an
    /// outwait noted its own).
    fn deleg_forget_due(&self, now: Now, gen: u64) -> WaitMs {
        now.wait
            .max(self.gen_until(gen))
            .plus(self.cfg.expiry_margin_ms)
    }

    /// Forget the ended generations nothing can need any more
    /// ([`Self::deleg_forget_due`]), unless a re-delegation still waits
    /// on one or the table still names it (a row it names is learned again
    /// by the next sync). Kept for good, every recall and re-delegation
    /// left one more entry that every step walked — the `status` view
    /// (`deleg_view`), the window publication, the re-delegation scan,
    /// `root_progress`: after 1,900 recalls in one directory a root's
    /// own creates went from 2,100 to 260 a second for as long as it ran,
    /// 70 % of its core in `deleg_view`. A step with nothing due costs one
    /// comparison.
    pub(crate) fn deleg_forget_ended(&mut self, now: Now, replica: &dyn Replica) {
        if !self
            .dl
            .forget_at
            .first()
            .is_some_and(|(at, _)| now.wait >= *at)
        {
            return;
        }
        let table = replica.delegation_table();
        let named: BTreeSet<u64> = table.gens().collect();
        let mut again = Vec::new();
        while let Some(&(at, gen)) = self.dl.forget_at.first() {
            if now.wait < at {
                break;
            }
            self.dl.forget_at.pop_first();
            let Some(g) = self.dl.gens.get(&gen) else {
                continue;
            };
            if !g.ended {
                continue;
            }
            if !g.redelegate_dirs.is_empty()
                || named.contains(&gen)
                || now.wait < self.gen_until(gen)
            {
                again.push((self.deleg_forget_due(now, gen), gen));
                continue;
            }
            self.dl.gens.remove(&gen);
            self.dl.root_caps.remove(&gen);
            self.dl.holder_caps.remove(&gen);
            self.dw.remove(gen);
            self.lock_forget_generation(gen);
            self.dl.forgot = true;
            self.stats.deleg_forgotten += 1;
        }
        self.dl.forget_at.extend(again);
    }

    /// Generations this root has not ended.
    pub(crate) fn deleg_live_generations(&self) -> usize {
        self.dl.gens.values().filter(|g| !g.ended).count()
    }

    /// The root this node streams to and renews with: the lease holder
    /// as last learned.
    pub(crate) fn root_node(&self) -> Option<NodeId> {
        self.lease
            .cached_holder
            .or(self.lease.last_seen.as_ref().map(|l| l.holder))
            .filter(|h| *h != 0 && *h != self.cfg.node_id)
    }

    /// The node a holder streams to and renews with: [`Self::root_node`], or
    /// this node itself while it holds the lease and a sticky stream of its
    /// own is live in its tenure (ADR-55: the root is the holder of its own
    /// stream; its messages to itself are delivered inline).
    pub(crate) fn holder_root(&self) -> Option<NodeId> {
        self.root_node().or_else(|| {
            let me = self.cfg.node_id;
            (self.lease.held.is_some()
                && !self.lease.lost
                && self
                    .dl
                    .gens
                    .values()
                    .any(|g| !g.ended && g.node == me && g.non_stealable()))
            .then_some(me)
        })
    }

    /// ADR-55: whether this node's own row `idx` of stream `gen` has left its
    /// journal (the segment that carried it was acknowledged by the archive).
    fn own_row_shipped(replica: &dyn Replica, gen: u64, idx: u64) -> bool {
        replica
            .delegate_txs_from(gen, idx, 1)
            .first()
            .is_none_or(|t| t.idx > idx)
    }

    /// ADR-55: a release of the root's own stream completes once its row has
    /// shipped (see `on_delegate_stream_ack`); checked on every event while one
    /// is pending.
    fn own_release_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let me = self.cfg.node_id;
        let pending: Vec<u64> = self
            .dl
            .mine
            .iter()
            .filter(|(g, d)| {
                d.release
                    .as_ref()
                    .is_some_and(|r| !r.held && r.row.is_some())
                    && (self.dl.gens.get(g).is_some_and(|s| s.node == me)
                        || self.dl.own_minted.contains(g))
            })
            .map(|(g, _)| *g)
            .collect();
        let mut done = false;
        for gen in pending {
            let row = self
                .dl
                .mine
                .get(&gen)
                .and_then(|d| d.release.as_ref())
                .and_then(|r| r.row)
                .expect("pending");
            let streamed = self.dl.mine[&gen].streamed_through;
            if streamed >= row && Self::own_row_shipped(replica, gen, row) {
                let d = self.dl.mine.get_mut(&gen).expect("present");
                d.release_acked |= d.release.as_ref().is_some_and(|r| r.ends);
                d.release = None;
                done = true;
            }
        }
        if done {
            self.delegation_sync(now, replica, out);
        }
    }

    /// ADR-55 (R-I4), audited in the simulation only (`track_fence_audit`):
    /// the root's own path is about to execute `op`; none of its keys may be
    /// under the root's own live sticky stream.
    pub(crate) fn audit_root_path_execution(&mut self, op: &MutateOp, replica: &dyn Replica) {
        if !self.track_fence_audit {
            return;
        }
        if let Ownership::Delegated(d) =
            replica.resolve_ownership(&super::holder::keys_of_op_in(op, replica))
        {
            if self
                .dl
                .gens
                .get(&d.gen)
                .is_some_and(|g| !g.ended && g.node == self.cfg.node_id && g.non_stealable())
            {
                self.stats.root_path_own_stream_executions += 1;
            }
        }
    }

    /// ADR-55: this node is about to be the root again and holds a stream of its
    /// own: it is inside its takeover gate, or it has restarted and the lease
    /// object still names it (its previous process's tenure, which its first
    /// forward re-adopts). The claim ends its own streams when it opens;
    /// whatever it executed as their holder meanwhile would be stranded with
    /// the rest of its tail.
    pub(crate) fn own_claim_pending(&self) -> bool {
        // (Only a process that restarted holding delegations: a node that is
        // the designee of a stream and claims the lease by a handoff executes
        // in its gate as it always did.)
        self.dl.restarted_holder
            && (self.lease.gate.is_some()
                || (self.lease.held.is_none()
                // (A deposed node has learned the new holder: it is a holder,
                // not a claimant.)
                && self.root_node().is_none()
                && self
                    .lease
                    .last_seen
                    .as_ref()
                    .is_some_and(|l| l.holder == self.cfg.node_id && !l.released)))
    }

    /// Whether `deps` are reached for an op the holder of stream `gen`
    /// executes (R-X). A far delegate waits for the log to carry the whole
    /// position. The root's own stream (ADR-55) is a root that holds its own
    /// journal while it holds the tenure, and its session's `applied` moves by segments only
    /// ([`Replica::reaches_streams`]): the root rows an op rests on are
    /// [`Self::own_honoured`]'s premise (R-H), the stream's own rows the
    /// replica's, and a premise lost with its generation is not reached.
    pub(crate) fn stream_deps_reached(
        &self,
        gen: u64,
        deps: &Position,
        replica: &dyn Replica,
    ) -> bool {
        if self.is_own_stream(gen) && self.root_tenure() {
            replica.reaches_streams(deps) && !replica.deps_lost(deps)
        } else {
            replica.reaches(deps)
        }
    }

    /// This node holds a root tenure: the lease, or the hold of a
    /// continuation epoch. A deposed root keeps streaming its own grant as
    /// a holder, and then waits for the log to carry its dependencies like
    /// any far holder: the root rows its deposition rolled back are held
    /// read-side until they are back (ADR-57), and a stream row must not
    /// execute on state without them.
    fn root_tenure(&self) -> bool {
        self.lease.held.is_some() || self.lease.epoch_hold().is_some()
    }

    /// Whether `gen` is a stream of the root's own (ADR-55): one this node
    /// granted itself, a deposed or restarted node's included.
    fn is_own_stream(&self, gen: u64) -> bool {
        self.dl.own_grants.contains_key(&gen)
            || self.dl.own_minted.contains(&gen)
            || self
                .dl
                .gens
                .get(&gen)
                .is_some_and(|g| !g.ended && g.node == self.cfg.node_id)
    }

    /// R-X's grant test: the holder of `gen` may execute now: its grant is
    /// live (`until`) and not stopped, and for the root's own stream
    /// [`Self::own_honoured`].
    pub(crate) fn stream_honoured(&self, now: Now, gen: u64) -> bool {
        self.dl
            .mine
            .get(&gen)
            .is_some_and(|d| now.honour < d.until && !d.stopped)
            && self.own_honoured(gen)
    }

    /// R-H: whether the root's own stream `gen` is honoured. A stream that
    /// is not one of the root's own always is. Its ops park (the stream's
    /// `commit_waiting`), the fast path does not admit under it and nothing
    /// is journaled in it until then.
    pub(crate) fn own_honoured(&self, gen: u64) -> bool {
        self.dl.own_grants.get(&gen).is_none_or(|g| g.honoured)
    }

    /// Whether a root row touching `keys` is one the own stream `gen` would
    /// diverge from if it were rolled back and replayed after a row of the
    /// stream: its keys resolve to the stream.
    fn keys_resolve_to_stream(replica: &dyn Replica, keys: &TouchSet, gen: u64) -> bool {
        match replica.resolve_ownership(keys) {
            Ownership::Root => false,
            Ownership::Delegated(d) => d.gen == gen,
            Ownership::CrossSubtree { involved, .. } => involved.iter().any(|d| d.gen == gen),
        }
    }

    /// R-H (b): whether a root row of this node's journal at or below
    /// `record` is not in the log yet and touches the keys of own stream
    /// `gen`. Rows on other keys commute with the stream's (the ship plan's
    /// own argument, `meta::store::held`).
    fn own_root_rows_unshipped(
        &self,
        gen: u64,
        record: u64,
        acked: u64,
        replica: &dyn Replica,
    ) -> bool {
        if replica.journal_root_bound() <= self.ack.shipped_through.max(acked) {
            return false;
        }
        match replica.journal_root_rows_through(record) {
            Ok(rows) => rows.iter().any(|(first, keys)| {
                !self.archive_covers(*first, acked)
                    && Self::keys_resolve_to_stream(replica, keys, gen)
            }),
            // (The journal could not be read: not decided.)
            Err(_) => true,
        }
    }

    /// R-H, once per event: decide the own streams not honoured yet, and
    /// release one whose honour has waited for a root row of its keys longer
    /// than the bound (a held, poisoned row under its directories): the
    /// root, as the holder, releases the grant with nothing executed
    /// (`Stats::root_own_released_unhonoured`); the quarantine keeps the
    /// placement from granting it again at once.
    fn own_honour_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.dl.own_grants.is_empty() {
            return;
        }
        let acked = replica.journal_acked_seq().unwrap_or(0);
        let gens: Vec<u64> = self.dl.own_grants.keys().copied().collect();
        let mut release: Vec<u64> = Vec::new();
        for g in gens {
            if !(self.dl.mine.contains_key(&g) || self.dl.gens.contains_key(&g)) {
                self.dl.own_grants.remove(&g);
                continue;
            }
            let og = self.dl.own_grants[&g];
            if og.honoured {
                continue;
            }
            // A record the archive carries stays committed (a lease loss
            // resets what this core counts as shipped; the log does not
            // forget).
            if og.record == u64::MAX || !self.archive_covers(og.record, acked) {
                continue;
            }
            if og.covered_at.is_none() {
                let ms = now.wait.since(og.since).max(0) as u64;
                self.stats.root_own_commit_waits += 1;
                self.stats.root_own_commit_wait_ms_max =
                    self.stats.root_own_commit_wait_ms_max.max(ms);
            }
            let covered_at = og.covered_at.unwrap_or(now.wait);
            if self.own_root_rows_unshipped(g, og.scan_to, acked, replica) {
                if now.wait.since(covered_at) >= self.dl_t().own_first_op_wait as i64 {
                    release.push(g);
                }
                self.dl.own_grants.get_mut(&g).expect("present").covered_at = Some(covered_at);
                continue;
            }
            let ms = now.wait.since(og.since).max(0) as u64;
            self.stats.root_own_honour_wait_ms_max = self.stats.root_own_honour_wait_ms_max.max(ms);
            let og = self.dl.own_grants.get_mut(&g).expect("present");
            og.honoured = true;
            og.covered_at = Some(covered_at);
        }
        for g in release {
            let dirs = self.sticky_held(g, replica);
            if self.own_sticky_release(now, g, dirs, replica, out) {
                self.stats.root_own_released_unhonoured += 1;
                // (Counted once: the release is under way, the grant ends.)
                if let Some(og) = self.dl.own_grants.get_mut(&g) {
                    og.covered_at = Some(now.wait.plus(u64::MAX / 4));
                }
            }
        }
    }

    /// This node holds the root lease usably (may append, recall, grant).
    pub(crate) fn root_usable(&self, now: Now) -> bool {
        self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.fenced()
    }

    /// Plan 46 M0.3 (S5, ADR-42): this node holds the lease in a takeover
    /// gate whose journal work is done (`PendingGate::shippable`: the
    /// marker landed, the predecessor's tail and this node's own rows are
    /// journaled), and is not releasing. The gate waits only for the
    /// read-delegation quarantine and the kernel drain, which protect
    /// keys a previous tenure may have read-delegated; a live generation
    /// of another node owns none of them. `journal_ship_epoch` also
    /// covers `lost` and the lease's time.
    pub(crate) fn root_gated_shippable(&self, now: Now) -> bool {
        !self.lease.releasing
            && self.lease.gate.as_ref().is_some_and(|g| g.shippable)
            && self.lease.journal_ship_epoch(now, &self.cfg).is_some()
    }

    /// A shippable gate serves generation `gen` (ADR-42): one it took on
    /// from the table (`GenState::inherited`; a generation of this tenure's
    /// own, kept across a gate, is the usable root's to end), not ended and
    /// not recalled.
    pub(crate) fn gated_serves(&self, now: Now, gen: u64) -> bool {
        self.root_gated_shippable(now)
            && self
                .dl
                .gens
                .get(&gen)
                .is_some_and(|g| g.inherited && !g.ended && g.recall == RecallPhase::None)
    }

    /// ADR-44: a shippable gate takes `gen`'s stream batches: a generation
    /// it serves ([`Self::gated_serves`]), or one it is recalling on demand
    /// (`GenState::gate_recalled`): the drain to the recall's `through` is
    /// what lets it end the generation before the gate opens.
    pub(crate) fn gated_takes_stream(&self, now: Now, gen: u64) -> bool {
        self.gated_serves(now, gen)
            || (self.root_gated_shippable(now)
                && self.dl.gens.get(&gen).is_some_and(|g| {
                    g.inherited
                        && g.gate_recalled
                        && !g.ended
                        && matches!(g.recall, RecallPhase::Sent | RecallPhase::Drained(_))
                }))
    }

    /// ADR-44 (amends ADR-42): what a takeover gate whose journal work is
    /// done recalls for an op on `keys` it cannot execute before it opens:
    /// exactly the generations an open root would recall for that op, and
    /// nothing else. An inherited generation of another node, not ended, not
    /// a designation (never recalled), that the op needs ended: every one a
    /// cross-subtree op involves, or the one a single-generation op falls
    /// under when an open root would not redirect it there (`redirect_from`
    /// names the requester of a forward or a settle, as `on_forward`'s S3
    /// test: a live, reachable generation not being recalled, and not the
    /// delegate's own op; `None`: the caller never redirects, the inbox).
    /// The recall is the ordinary one (`start_recall`); the gate takes the
    /// drain and ends the generation at its cut (`root_progress`), so a
    /// settle on the keys becomes the gate's own (S1) once the cut is
    /// journaled. It seals, reclaims and outwaits nothing, and starts no
    /// recall on its own initiative. `true`: a recall the op needs is in
    /// flight.
    pub(crate) fn gate_recall_on_demand(
        &mut self,
        now: Now,
        redirect_from: Option<NodeId>,
        keys: &TouchSet,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.delegation
            || !self.deleg_known()
            || self.lease.gate.is_none()
            || !self.root_gated_shippable(now)
        {
            return false;
        }
        let me = self.me();
        let (involved, cross): (Involved, bool) = match replica.resolve_ownership(keys) {
            Ownership::Root => return false,
            Ownership::Delegated(d) => {
                if d.node == me {
                    return false;
                }
                if let Some(from) = redirect_from {
                    let live = self.dl.gens.get(&d.gen).is_some_and(|g| {
                        !g.ended
                            && g.recall == RecallPhase::None
                            && now.wait < self.gen_until(d.gen)
                            && self
                                .roster
                                .links()
                                .get(&d.node)
                                .is_some_and(|l| l.connected)
                    });
                    let disowned = from == d.node && !self.grant_in_flight(now, d.gen);
                    if live && !disowned {
                        return false;
                    }
                }
                (
                    vec![(d.gen, BTreeMap::from([(d.dir, kind_of(d.kind))]))],
                    false,
                )
            }
            Ownership::CrossSubtree { involved, .. } => (
                Self::group_by_gen(involved.iter().filter(|d| d.node != me)),
                true,
            ),
        };
        let mut pending = false;
        for (gen, dirs) in involved {
            let Some(g) = self.dl.gens.get_mut(&gen) else {
                continue;
            };
            if g.ended || !g.inherited || g.node == me || g.non_stealable() {
                continue;
            }
            if g.recall == RecallPhase::None {
                if cross {
                    // As `deleg_recall_plan`: delegated again once the op
                    // ran (`deleg_redelegate_after_cross`, a usable root's).
                    for (d, k) in &dirs {
                        let k = g.dirs.get(d).copied().unwrap_or(*k);
                        g.redelegate_dirs.insert(*d, k);
                    }
                }
                self.start_recall(now, gen, out);
                if let Some(g) = self.dl.gens.get(&gen) {
                    if g.recall != RecallPhase::None {
                        self.stats.deleg_gate_recalls += 1;
                    }
                }
            }
            pending |= self
                .dl
                .gens
                .get(&gen)
                .is_some_and(|g| !g.ended && g.gate_recalled && g.recall != RecallPhase::None);
        }
        pending
    }

    /// The subtrees `delegations` name, by the stream that holds them, with
    /// the kind each was granted as.
    fn group_by_gen<'a>(
        delegations: impl Iterator<Item = &'a constellation_meta::delegation::Delegation>,
    ) -> Involved {
        let mut by: BTreeMap<u64, BTreeMap<Ino, DelegKind>> = BTreeMap::new();
        for d in delegations {
            by.entry(d.gen).or_default().insert(d.dir, kind_of(d.kind));
        }
        by.into_iter().collect()
    }

    /// What this node does as a root now, for the table's generations.
    pub(crate) fn root_deleg_level(&self, now: Now) -> RootSync {
        if self.root_usable(now) {
            RootSync::Usable
        } else if self.root_gated_shippable(now) {
            RootSync::Gated
        } else {
            RootSync::None
        }
    }

    /// This node takes the generations of other nodes as a root: usable,
    /// or in a shippable gate (ADR-42). Only a usable root recalls, seals
    /// or reclaims them.
    pub(crate) fn root_serves_generations(&self, now: Now) -> bool {
        self.root_deleg_level(now) != RootSync::None
    }

    // ----------------------------------------------------- the table

    /// The table changed (a segment applied, a record journaled here, the
    /// lease changed hands): install the delegations that name this node,
    /// drop the ones the log ended, and — as the root — learn the
    /// generations a predecessor left live.
    pub(crate) fn delegation_sync(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.delegation {
            return;
        }
        let table = replica.delegation_table();
        let me = self.me();
        // The delegate side.
        let live_mine: BTreeSet<u64> = table
            .iter()
            .filter(|d| d.node == me)
            .map(|d| d.gen)
            .collect();
        // A stream whose every subtree this node released is no entry of
        // `table.iter()` (its own journal applied the release row), but it
        // is live in the table until the root ends it. Until the root has
        // appended the release row the state of a non-stealable stream stays:
        // the row is shipped by it (plan 46 M1.3 step 5; a sync between the
        // row and the root's acknowledgement dropped the state, and the row
        // was never streamed: a sticky holder's answers are final, so its
        // rows are the root's only copy). A stealable stream's recall is
        // outwaited by the root's expiry whatever its row did, and is left
        // as it was. The remnant a restart installed again for the row
        // (`DelegateState::remnant`) stays whatever its kind: it has no
        // subtree to tell it by, and a sticky stream's row that never ships
        // is never ended by the root's expiry either (sim `sticky-idle-release`
        // seed 217: the sync after the install dropped it, the root listed
        // the stream for good and the row stayed in the journal).
        let streams_mine: BTreeSet<u64> = table
            .streams()
            .filter(|s| s.node == me)
            .map(|s| s.gen)
            .collect();
        let ended: Vec<u64> = self
            .dl
            .mine
            .iter()
            .filter(|(g, _)| !live_mine.contains(g))
            .filter(|(g, d)| {
                // A release row the log holds already (this node shipped
                // it in its own segment, or a takeover re-appended it) is
                // nothing to wait for: the stream never sends it, so it
                // would never be acknowledged either.
                let unlogged = d
                    .release
                    .as_ref()
                    .is_some_and(|r| r.row.is_none_or(|row| replica.log_stream_idx(**g) < row));
                // ADR-55: the root's own stream ended at its inline append
                // (the table forgot it) but its release row has not shipped:
                // the state stays to stream it to the next root.
                !((d.non_stealable || d.remnant)
                    && unlogged
                    && (streams_mine.contains(g) || self.dl.own_minted.contains(g)))
            })
            .map(|(g, _)| *g)
            .collect();
        for gen in ended {
            self.drop_delegate_state(now, gen, replica, out);
        }
        self.dl.handed_back.retain(|gen, (_, at, seen)| {
            if live_mine.contains(gen) {
                *seen = true;
                return true;
            }
            !*seen && now.wait.since(*at) < HANDED_BACK_UNSEEN_MS
        });
        let live: BTreeSet<u64> = table.gens().collect();
        self.dl.release_first.retain(|gen| live.contains(gen));
        self.dl.installed_gens.retain(|gen| live.contains(gen));
        // Path B: the table here can be ahead of the log (the root's stream
        // installs its records speculatively), and say that a generation of
        // this node ended and a new one began before the segment that ends
        // the old one is applied. The old one's rows past its cut leave the
        // journal only then (the strand, replayed by rid), and until then
        // they are in this replica's state: an op of the new generation
        // executed on that state (a chmod of a file the stranded row
        // created) depends on a row the log never carries, and the root,
        // which counts a dependency on the ended generation as void,
        // appends it. So no generation is installed here while an ended
        // generation's rows are still in the journal, its end not yet
        // applied from the log; the segment that applies it syncs again.
        let mut awaiting_end: Option<Option<Vec<u64>>> = None;
        let mut waiting_now: BTreeSet<u64> = BTreeSet::new();
        let mut uncommitted: Option<Result<BTreeSet<u64>, String>> = None;
        #[cfg(test)]
        if self.dl.test_honour_uncommitted {
            uncommitted = Some(Ok(BTreeSet::new()));
        }
        let mut commit_now: BTreeSet<u64> = BTreeSet::new();
        let mut retry = false;
        for st in table.streams().filter(|s| s.node == me) {
            // A stream that released every subtree holds nothing to install,
            // unless its release row is still to ship: a restart between the
            // row and the root's append (plan 46 M1.3 step 5) left it in the
            // journal, and nothing else would send it. The remnant is
            // installed stopped, holding the release.
            // Only after a restart: a stream this process installed and
            // dropped is not one whose state was lost (a recall's answer is
            // outwaited by the root whatever its row did).
            let remnant = st.subtrees.is_empty();
            if remnant
                && (self.dl.installed_gens.contains(&st.gen)
                    || replica.delegate_idx(st.gen) <= replica.log_stream_idx(st.gen))
            {
                continue;
            }
            let d = StreamView::of(&st);
            // (A leaving node installs nothing: a grant that reached it
            // before the root heard it leaves is released unheld.)
            if self.dl.mine.contains_key(&d.gen) || self.dl.epoch_active || self.dl.leave_self {
                continue;
            }
            // A generation this node already answered a recall for (the
            // grant was parked, or never installed, when the root took it
            // back) is never installed afterwards: the root ended it in its
            // own journal and refuses what would run under it. Its end may
            // sit in a later segment than its `DelegateNode`; the sync that
            // applies that one drops the `handed_back` entry.
            if self.dl.handed_back.contains_key(&d.gen) {
                continue;
            }
            let awaiting = awaiting_end.get_or_insert_with(|| {
                let live: BTreeSet<u64> = table.gens().collect();
                replica.deleg_gens_awaiting_end(&live)
            });
            if awaiting.as_ref().is_none_or(|gens| !gens.is_empty()) {
                let ended = awaiting.clone();
                self.deleg_note_install_wait(now, d.dir_list(), d.gen, ended);
                waiting_now.insert(d.gen);
                retry = true;
                continue;
            }
            // Plan 46 M1.2 step 8b (M1.0 answer 4): a grant is honoured once
            // the root's configuration has committed its `DelegateNode`
            // record: the archive for a lone root, its group otherwise. The
            // root streams a row its group holds as committed
            // (`StreamAhead.committed`) and a lone root's journal not at
            // all, so a record this replica holds only as an uncommitted
            // streamed entry is learned but not honoured: the delegate
            // neither installs it nor renews it, and its ops forward to the
            // root as for any generation it does not hold. The segment that
            // carries the row retires the entry and syncs again (the log's
            // `DelegateNode`); the retry timer covers a lost one. No wait is
            // unbounded: the root, which hears no renewal, reclaims the
            // generation at its first-renewal horizon, and a root that is
            // gone leaves the number to ADR-43's orphan rule.
            let uncommitted = uncommitted.get_or_insert_with(|| replica.deleg_grants_uncommitted());
            match uncommitted {
                Ok(set) if !set.contains(&d.gen) => {}
                other => {
                    if other.is_ok() && !self.dl.commit_waiting.contains_key(&d.gen) {
                        self.stats.deleg_install_waits_commit += 1;
                        tracing::debug!(
                            node = me,
                            dirs = ?d.dir_list(),
                            gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                            "delegation not honoured yet: its DelegateNode record is not committed (waits for the log)"
                        );
                    }
                    self.dl.commit_waiting.entry(d.gen).or_insert(now.wait);
                    commit_now.insert(d.gen);
                    retry = true;
                    continue;
                }
            }
            // A group that cannot be read is not "no record": installed at
            // a later sync (plan 43 C2e review), which the retry timer
            // brings if no segment does.
            let Ok(group) = replica.deleg_group(d.gen) else {
                retry = true;
                continue;
            };
            let stream_round = self.deleg_new_round();
            tracing::info!(node = me, dirs = ?d.dir_list(), gen = %constellation_meta::delegation::GenLabel::of(d.gen), "delegation installed");
            self.stats.deleg_installed += 1;
            // Plan 43 C2e follow-up: a process that held this generation
            // before a reboot may have lost the unsynced end of its journal
            // (commits reach the OS, not the disk), rows it acknowledged
            // through its group or that the root's cursor covers. Granted
            // again, its next rows would take their indices: the backup's
            // acknowledged rows overwritten, the root skipping them as
            // appended. It does not take the generation up again: it never
            // renews, and the root ends the generation by its backup's
            // sealed tail, or at the cursor. A restart on the same boot finds
            // the journal as it was and goes on (`delegate-handoff-renewal`);
            // a designee is exempt: it renews nothing, and its stream
            // sends only rows a sync put on its disk (`deleg_stream`).
            let rebooted =
                !d.non_stealable() && replica.deleg_held_on_another_boot(d.gen, now.boot.id);
            if rebooted {
                self.stats.deleg_rebooted_holder += 1;
                tracing::warn!(
                    node = me,
                    dirs = ?d.dir_list(),
                    gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                    "this node held the delegation before a reboot and may have lost rows it acknowledged: \
                     not taken up again; the root ends it by its group"
                );
            }
            // A delegate's reply base is the last applied segment that
            // touched the op's keys (`reply_base`), and the window only
            // records segments applied while this node delegates: one
            // applied before this grant — the root's earlier writes in
            // the subtree — would be missing from it, and a reply naming
            // a lower base would let the requester install it on a
            // replica without them (chaos-soak-4 seed 42, `wf293`). The
            // floor covers everything applied so far.
            self.shipped_floor = self.shipped_floor.max(replica.applied_seq().unwrap_or(0));
            self.dl.installed_gens.insert(d.gen);
            let restart_q = self
                .dl
                .restart_horizon
                .filter(|until| *until > now.wait && !self.cfg.restart_skips_write_quarantine)
                .map(|until| {
                    let timer =
                        self.set_timer_wait(until, Timer::DelegRestartQuarantine(d.gen), out);
                    self.stats.deleg_restart_quarantines += 1;
                    tracing::info!(
                        node = me,
                        dirs = ?d.dir_list(),
                        gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                        wait_ms = until.since(now.wait),
                        "this node held delegations before a restart; its writes wait until \
                         the read delegations its previous process granted have expired"
                    );
                    RestartQuarantine {
                        until,
                        timer: Some(timer),
                        asked: BTreeMap::new(),
                        round_sent: false,
                        voided: false,
                    }
                });
            if restart_q.is_some() {
                self.dl.restarted_holder = true;
            }
            self.dl.mine.insert(
                d.gen,
                DelegateState {
                    dirs: d.dir_list().into_iter().collect(),
                    grown: BTreeMap::new(),
                    release: remnant.then(|| PendingRelease {
                        note: constellation_meta::locks::ReleaseNote {
                            leaving: self.dl.leave_self,
                            ..Default::default()
                        },
                        row: Some(replica.delegate_idx(d.gen)),
                        first_row: Some(replica.delegate_idx(d.gen)),
                        held: false,
                        holds: 0,
                        left_at: now.wait,
                        to: self.holder_root().unwrap_or(0),
                        ends: true,
                    }),
                    release_acked: false,
                    release_pending: (BTreeSet::new(), 0),
                    released_dirs: BTreeSet::new(),
                    root_answer: None,
                    core_answer_sent: None,
                    gen: d.gen,
                    until: HonourMs(0),
                    // This node's takeover gate ended the generation
                    // (`Meta::strand_for_takeover`): it ends at what the
                    // log holds of it; nothing more executes here.
                    stopped: replica.deleg_ended_at_gate(d.gen) || rebooted || remnant,
                    rebooted,
                    restart_q,
                    stream_after: None,
                    stream_backoff_ms: 0,
                    refusal_backoff_ms: 0,
                    refused: false,
                    streamed_through: replica.log_stream_idx(d.gen),
                    root_committed: replica.log_stream_idx(d.gen),
                    archive_bound: false,
                    poke_after: None,
                    ask: None,
                    poke_backoff_ms: 0,
                    inflight: None,
                    renew: None,
                    renew_sent_wait: WaitMs(0),
                    renew_resent: false,
                    renew_resend_next: false,
                    renew_timer: None,
                    installed: now.honour,
                    last_renew_sent: HonourMs(0),
                    answered_sent: HonourMs(0),
                    renew_period_ms: 0,
                    lapse_timer: None,
                    lapsed: false,
                    parked: Vec::new(),
                    executed: 0,
                    last_own_op: BTreeMap::new(),
                    non_stealable: d.non_stealable(),
                    remnant,
                    last_root: self.holder_root(),
                    known_root: self.holder_root(),
                    backup: None,
                    backup_acked: 0,
                    backup_sent_through: 0,
                    backup_inflight: None,
                    backup_sealed: false,
                    backup_failures: 0,
                    backup_after: None,
                    renew_backup: None,
                    inflight_at: WaitMs(0),
                    backup_inflight_at: HonourMs(0),
                    backup_inflight_wait: WaitMs(0),
                    stream_round,
                    core_granted_round: None,
                    leaving: None,
                    group_version: group.as_ref().map_or(0, |g| g.version),
                    group_backups: group.map(|g| g.backups).unwrap_or_default(),
                    group_requested: None,
                    group_spans: Vec::new(),
                    acked_max: 0,
                    held_noted: false,
                    synced_through: 0,
                    sync_inflight: None,
                    lost_from: None,
                },
            );
            if d.node == me {
                self.recover_own_grant_row(d.gen, replica);
            }
            if remnant {
                self.arm_stream_tick(now, out);
                continue;
            }
            if d.non_stealable() {
                // Honoured whatever the root says or the clock does
                // (DESIGN.md §5.2: the designee writes while isolated).
                self.dl.mine.get_mut(&d.gen).expect("present").until = HonourMs(i64::MAX / 2);
            }
            self.deleg_renew_now(now, d.gen, out);
            self.arm_deleg_lapse(now, d.gen, out);
        }
        // Plan 46 M1.2 step 9: a stream this node already holds may have
        // grown (a later `DelegateNode` of the same generation).
        if !self.dl.epoch_active {
            let mut grown_streams: Vec<(u64, Vec<(Ino, SubtreeKind)>)> = Vec::new();
            for st in table.streams().filter(|s| s.node == me) {
                let differs = self.dl.mine.get(&st.gen).is_some_and(|d| {
                    st.subtrees
                        .iter()
                        .any(|(dir, _)| !d.dirs.contains(dir) && !d.grown.contains_key(dir))
                        || d.dirs
                            .iter()
                            .chain(d.grown.keys())
                            .any(|dir| !st.subtrees.iter().any(|(x, _)| x == dir))
                        || d.grown.values().any(|v| v.is_none())
                });
                if differs {
                    grown_streams.push((st.gen, st.subtrees));
                }
            }
            for (gen, subtrees) in grown_streams {
                let uncommitted =
                    uncommitted.get_or_insert_with(|| replica.deleg_grants_uncommitted());
                let committed = match uncommitted {
                    Ok(set) => !set.contains(&gen),
                    Err(_) => false,
                };
                let before: Vec<Ino> = self
                    .dl
                    .mine
                    .get(&gen)
                    .map(|d| d.dirs.iter().copied().collect())
                    .unwrap_or_default();
                let synced = self.deleg_sync_growth(now, gen, &subtrees, committed, replica, out);
                // Plan 46 M1.2 step 10: handoff grants that came before their
                // subtree's row are installed before anything here serves it
                // (a non-stealable stream honours a grown subtree right here).
                let promoted: Vec<Ino> = self
                    .dl
                    .mine
                    .get(&gen)
                    .map(|d| {
                        d.dirs
                            .iter()
                            .copied()
                            .filter(|x| !before.contains(x))
                            .collect()
                    })
                    .unwrap_or_default();
                self.lock_install_deferred(now, gen, &promoted, replica);
                if !synced {
                    self.dl.commit_waiting.entry(gen).or_insert(now.wait);
                    commit_now.insert(gen);
                    retry = true;
                }
            }
        }
        // Plan 46 M1.2 step 10: subtrees the root asked for that this node
        // had not learned: released now that their records are here.
        if !self.dl.epoch_active {
            let ready: Vec<(u64, NodeId, Vec<Ino>)> = self
                .dl
                .mine
                .iter_mut()
                .filter(|(_, d)| !d.release_pending.0.is_empty())
                .filter_map(|(gen, d)| {
                    let known: Vec<Ino> = d
                        .release_pending
                        .0
                        .iter()
                        .copied()
                        .filter(|x| d.dirs.contains(x) || d.grown.contains_key(x))
                        .collect();
                    for x in &known {
                        d.release_pending.0.remove(x);
                    }
                    (!known.is_empty()).then_some((*gen, d.release_pending.1, known))
                })
                .collect();
            for (gen, to, dirs) in ready {
                self.on_release_request(
                    now,
                    to,
                    OpId(0),
                    gen,
                    dirs,
                    ReleaseWhy::Recall,
                    replica,
                    out,
                );
            }
        }
        // With an epoch active nothing was looked at: the waits stand.
        if !self.dl.epoch_active {
            self.deleg_install_waits_settle(now, &waiting_now);
        }
        if !self.dl.epoch_active {
            // Plan 46 M1.3 step 7: the first-op wait of a grant whose record
            // is committed now and which this node installed: the time the
            // holder's ops were parked for the archive.
            let mut waited = Vec::new();
            self.dl.commit_waiting.retain(|g, since| {
                let keep = commit_now.contains(g);
                if !keep {
                    waited.push((*g, now.wait.since(*since).max(0) as u64));
                }
                keep
            });
            for (g, ms) in waited {
                if self.dl.mine.contains_key(&g) {
                    self.stats.deleg_commit_waits += 1;
                    self.stats.deleg_commit_wait_ms_sum += ms;
                    self.stats.deleg_commit_wait_ms_max =
                        self.stats.deleg_commit_wait_ms_max.max(ms);
                    tracing::debug!(
                        node = self.me(),
                        gen = %constellation_meta::delegation::GenLabel::of(g),
                        ms,
                        "a delegation's first ops waited for its record's archive"
                    );
                }
            }
        }
        if retry && self.dl.install_retry.is_none() {
            let t = self.set_timer_wait(
                now.wait.plus(INSTALL_RETRY_MS),
                Timer::DelegInstallRetry,
                out,
            );
            self.dl.install_retry = Some(t);
        }
        // Plan 43 C2 (D4): the group each held generation's table records.
        self.deleg_group_refresh(replica);
        // The root side.
        let level = self.root_deleg_level(now);
        self.dl.root_synced = level;
        if level != RootSync::None {
            let live: BTreeSet<u64> = table.gens().collect();
            // The generations this sync looks at: `table` is the snapshot
            // taken at its start. A generation granted while it runs (the
            // re-delegation after a cross-subtree op, which an ended
            // generation's waiters trigger) is in the replica's table but
            // not in this snapshot, and is not one a record ended.
            let known: BTreeSet<u64> = self.dl.gens.keys().copied().collect();
            for st in table.streams() {
                let d = StreamView::of(&st);
                if let Some(g) = self.dl.gens.get_mut(&d.gen) {
                    // A growth the log carries that this tenure did not
                    // journal (a predecessor's, applied late): the stream
                    // holds it.
                    for (dir, kind) in &d.dirs {
                        g.dirs.entry(*dir).or_insert(kind_of(*kind));
                    }
                    // And a release it did not append (a predecessor's row,
                    // applied late): the stream no longer holds the subtree.
                    if !g.ended && !d.dirs.is_empty() {
                        g.dirs.retain(|dir, _| d.dirs.iter().any(|(x, _)| x == dir));
                        g.releasing.retain(|dir| g.dirs.contains_key(dir));
                    }
                    continue;
                }
                // A stream that released every subtree has none to inherit
                // (step 10 gives it an end).
                if d.dirs.is_empty() {
                    continue;
                }
                // Plan 46 M0.3 (S5, ADR-42): a gate whose journal work is
                // done takes on the generations of other nodes. Its own
                // are taken on and ended when the gate opens (the open
                // syncs again as `Usable`), and its S1 answers their
                // settles until then.
                if level == RootSync::Gated && d.node == me {
                    continue;
                }
                // Its group unreadable: inherited at a later sync (an empty
                // group would end it at the cursor without its backup).
                let Ok(group_row) = replica.deleg_group(d.gen) else {
                    continue;
                };
                // Inherited from a predecessor root. Its grant was capped
                // by that lease (the model's `cap_by_lease`), which a TTL
                // takeover has outwaited; a seal-based takeover of an
                // unexpired lease (M9) has not, so the conservative
                // horizon is waited before a reclaim — a renewal from the
                // delegate ends the wait sooner.
                // After such a fast takeover the horizon counts from the
                // predecessor's last renewal, which it decided within
                // `backup_takeover_ms` of a proof of liveness older than
                // this tenure (plan 41 L0, `on_deleg_renew`), so that
                // window is added (`inherit_slack_ms`), as the lock
                // quarantine of `note_marker_landed` adds it. Without it
                // the bound needed `backup_takeover_ms + 2D ≤ 2M`, which
                // the defaults meet only for D ≤ 250 ms and a 3 s lease's
                // margin (750 ms) not at all.
                // The log's index of the stream, never a shadow's (a
                // forwarded op's reply raised `stream_applied` past what
                // the predecessor appended; the delegate re-streams the
                // rest from here).
                let mut cursor = replica.log_stream_idx(d.gen);
                // ADR-55: a stream this node holds itself (it was the root and
                // its own holder, and claims the lease again): every row it
                // assigned is in its own journal, which is the root's, and
                // ships in its segments. The cut it ends the stream at
                // is what it assigned, not the log's index alone: a lower one
                // would void answers it gave `Final`.
                if d.node == me {
                    cursor = cursor.max(replica.delegate_idx(d.gen));
                }
                tracing::info!(
                    node = me,
                    gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                    dirs = ?d.dir_list(),
                    delegate = d.node,
                    cursor,
                    stream_applied = replica.stream_applied(d.gen),
                    "inherited a delegation from a predecessor root"
                );
                let inherited_until = now
                    .wait
                    .plus(self.reclaim_horizon_ms() + self.dl.inherit_slack_ms);
                self.dw.insert(
                    d.gen,
                    d.node,
                    inherited_until,
                    inherited_until,
                    !(d.non_stealable() && !(self.cfg.ttl_reclaims_sticky && d.sticky())),
                );
                self.dl.gens.insert(
                    d.gen,
                    GenState {
                        dirs: d.dirs.iter().map(|(dir, k)| (*dir, kind_of(*k))).collect(),
                        node: d.node,
                        group: GroupState::of(group_row),
                        redelegate_dirs: BTreeMap::new(),
                        releasing: BTreeSet::new(),
                        granted: now.wait,
                        inherited: true,
                        grown_at: None,
                        gate_recalled: false,
                        below_since: None,
                        seal_reqs: BTreeMap::new(),
                        sealed_tail: None,
                        waiting_for_group: false,
                        waiting_warned: None,
                        waiting_since: None,
                        member_notes: BTreeMap::new(),
                        prev_roots: BTreeSet::new(),
                        prev_reqs: BTreeMap::new(),
                        prev_tails: BTreeMap::new(),
                        prev_blocked: false,
                        prev_done: BTreeSet::new(),
                        delegate_heard: None,
                        cursor,
                        last_batch: None,
                        committed_sent: 0,
                        recall: RecallPhase::None,
                        recall_before_seal: RecallPhase::None,
                        release_cut: None,
                        release_seal: false,
                        release_seal_timer: None,
                        release_answered: None,
                        release_group_answered: false,
                        release_cut_waived: false,
                        release_answered_at: None,
                        ended: false,
                        expiry: None,
                        recall_req: None,
                        recall_tries: Vec::new(),
                        recall_started: None,
                        recall_cause: None,
                        release_resends: 0,
                        recall_resend: None,
                        recall_backoff_ms: 0,
                        controls: Vec::new(),
                        read_recalls: BTreeSet::new(),
                        renew_waiting: None,
                    },
                );
                self.stats.deleg_inherited += 1;
                if level == RootSync::Gated {
                    self.stats.deleg_gated_inherited += 1;
                }
                if d.node != me {
                    self.deleg_inherit_prev_roots(now, d.gen, replica, out);
                }
                if d.node == me {
                    // A predecessor delegated this subtree to the node
                    // that is now the root: the root executes it itself.
                    self.end_generation(now, d.gen, None, replica, out);
                    continue;
                }
                self.arm_gen_expiry(now, d.gen, out);
            }
            let gone: Vec<u64> = self
                .dl
                .gens
                .iter()
                .filter(|(g, s)| !s.ended && !live.contains(g) && known.contains(g))
                .map(|(g, _)| *g)
                .collect();
            for gen in gone {
                // Ended by a record this node did not write (a
                // predecessor's, applied from the log).
                self.mark_ended(now, gen, replica, out);
            }
            // The gate recalls and ends nothing (ADR-42); the open does what
            // it kept: a recall drained while the gate was pending ends now.
            if level == RootSync::Usable {
                if self.dl.epoch_active {
                    self.recall_all(now, out);
                }
                self.root_progress(now, replica, out);
            }
        } else if self.deleg_known() && (self.lease.held.is_none() || self.lease.lost) {
            // Only a lease that is gone ends this root's generations. One
            // held but unusable for the moment (a renewal or a gate in
            // flight, S3 away) keeps them, as `on_deleg_expiry` does: the
            // state dropped there was never learned again while the lease
            // stayed this node's, and with no generation known the root
            // executed ops under a live delegation itself — beside the
            // delegate (long-delegated seed 75808: an `EEXIST` evaluated
            // on the root's replica, which lacked the delegate's rename).
            self.deleg_on_lease_gone(now, replica, out);
        }
        self.deleg_after_event(now, replica, out);
    }

    /// Path B: the install of `gen` waits for the end of `ended` (`None`:
    /// the journal could not be read): counted, kept for `status`, and
    /// warned about once it waited [`INSTALL_WAIT_WARN_MS`], then every
    /// [`INSTALL_WAIT_WARN_EVERY_MS`].
    fn deleg_note_install_wait(
        &mut self,
        now: Now,
        dirs: Vec<Ino>,
        gen: u64,
        ended: Option<Vec<u64>>,
    ) {
        let me = self.me();
        let w = self.dl.install_waiting.entry(gen).or_insert_with(|| {
            self.stats.deleg_install_waits_end += 1;
            tracing::debug!(
                node = me,
                dirs = ?dirs,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ended = ?ended,
                "delegation not installed yet: an ended generation's rows wait for its end in the log"
            );
            InstallWait {
                dirs: dirs.clone(),
                since: now.wait,
                ended: ended.clone(),
                warned: None,
            }
        });
        w.ended = ended;
        let waited = now.wait.since(w.since);
        if waited >= INSTALL_WAIT_WARN_MS
            && w.warned
                .is_none_or(|t| now.wait.since(t) >= INSTALL_WAIT_WARN_EVERY_MS)
        {
            w.warned = Some(now.wait);
            tracing::warn!(
                node = me,
                dirs = ?dirs,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                waited_ms = waited,
                ended = ?w.ended,
                "a delegation still waits to be installed: an ended generation's rows wait for its end in the log \
                 (unreadable journal: ended None)"
            );
        }
    }

    /// The installs that waited and wait no more (installed, or gone from
    /// the table): how long they waited.
    fn deleg_install_waits_settle(&mut self, now: Now, waiting_now: &BTreeSet<u64>) {
        let done: Vec<u64> = self
            .dl
            .install_waiting
            .keys()
            .filter(|g| !waiting_now.contains(g))
            .copied()
            .collect();
        for gen in done {
            if let Some(w) = self.dl.install_waiting.remove(&gen) {
                let ms = now.wait.since(w.since).max(0) as u64;
                self.stats.deleg_install_wait_ms += ms;
                self.stats.deleg_install_wait_max_ms = self.stats.deleg_install_wait_max_ms.max(ms);
            }
        }
    }

    /// The retry timer of a waiting install (or of a group that could not
    /// be read): sync again.
    pub(crate) fn on_deleg_install_retry(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.install_retry = None;
        self.delegation_sync(now, replica, out);
    }

    /// Plan 46 M1.2 step 9: the table lists stream `gen` of this node with
    /// `subtrees`; bring the stream's honoured set (`dirs`) and its waiting
    /// growth (`grown`) in line. A subtree the table no longer lists leaves
    /// both. A new one waits for its `DelegateNode` record to be committed
    /// (step 8b's rule: `committed` is false while this replica holds the
    /// record only as an uncommitted streamed entry), and then for a
    /// granting answer to a renewal sent after that
    /// ([`Self::deleg_unpark_grown`]): the root's answer carries the lock
    /// handoff of the new subtree and has waited for the read delegations
    /// under it. An all-designated stream is honoured without any renewal,
    /// so a designated subtree added to it is honoured once committed.
    /// Returns false while a growth waits for its commit.
    fn deleg_sync_growth(
        &mut self,
        now: Now,
        gen: u64,
        subtrees: &[(Ino, SubtreeKind)],
        committed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return true;
        };
        let listed = |dir: &Ino| subtrees.iter().any(|(x, _)| x == dir);
        d.dirs.retain(|dir| listed(dir));
        d.grown.retain(|dir, _| listed(dir));
        let mut learned = 0u64;
        for (dir, _) in subtrees {
            if !d.dirs.contains(dir) && !d.grown.contains_key(dir) {
                d.grown.insert(*dir, None);
                // A subtree the root grants this stream again is one to
                // release when it is asked for again: the release this node
                // journaled of it earlier is not one a later request repeats
                // (step 10 review).
                if d.released_dirs.remove(dir) {
                    // Plan 46 M1.2 step 10: the root applied the release
                    // before it granted the subtree again, so the lock grants
                    // a note not acknowledged yet hands back under it are the
                    // root's already, and handed again with the subtree. A
                    // copy here would ride the next release of it (merged into
                    // this note) as stale as it is now.
                    if let Some(r) = d.release.as_mut() {
                        let ns = replica.namespace();
                        let locks = &mut r.note.locks;
                        let gone: Vec<constellation_meta::locks::GrantId> = locks
                            .grants
                            .iter()
                            .filter(|g| is_under(ns, g.ino, *dir))
                            .map(|g| g.id)
                            .collect();
                        locks.grants.retain(|g| !gone.contains(&g.id));
                        locks.carry.retain(|(id, _)| !gone.contains(id));
                        locks.rows.retain(|(id, _)| !gone.contains(id));
                    }
                }
                learned += 1;
            }
        }
        self.stats.deleg_grows_learned += learned;
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return true;
        };
        if d.grown.is_empty() {
            return true;
        }
        if !committed {
            if d.grown.values().any(|v| v.is_none()) && learned > 0 {
                self.stats.deleg_install_waits_commit += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    "a grown subtree is not honoured yet: its DelegateNode record is not committed (waits for the log)"
                );
            }
            return false;
        }
        let non_stealable_stream = d.non_stealable;
        let mut promoted = 0u64;
        // Only a renewal sent after this point counts: not one in flight,
        // nor one answered or sent by the lane already.
        let after = d
            .last_renew_sent
            .max(d.renew.map_or(HonourMs(-1), |(_, sent)| sent));
        let dirs: Vec<Ino> = d.grown.keys().copied().collect();
        for dir in dirs {
            if d.grown.get(&dir) == Some(&None) {
                d.grown.insert(dir, Some(after));
            }
            // A stream never mixes grant kinds: a designated stream grows
            // by designations only, honoured once committed.
            if non_stealable_stream {
                d.grown.remove(&dir);
                d.dirs.insert(dir);
                promoted += 1;
            }
        }
        self.stats.deleg_grows_honoured += promoted;
        if self.dl.mine.get(&gen).is_some_and(|d| !d.grown.is_empty()) {
            // The renewal that unlocks it: sent after this point.
            self.deleg_renew_now(now, gen, out);
        }
        true
    }

    /// Plan 46 M1.2 step 9: the root's core answered a renewal of `gen` that
    /// this node sent at `sent`, and its lock content is installed: every
    /// grown subtree whose commit was learned before that send is honoured
    /// from now on (returned). One still waiting asks again at once. Only
    /// the core's answer counts: an answer without it carries no handoff,
    /// grace or floor for the new subtree.
    pub(crate) fn deleg_unpark_grown(
        &mut self,
        now: Now,
        gen: u64,
        sent: HonourMs,
        out: &mut Vec<Action>,
    ) -> Vec<Ino> {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return Vec::new();
        };
        if d.grown.is_empty() {
            return Vec::new();
        }
        let ready: Vec<Ino> = d
            .grown
            .iter()
            .filter(|(_, v)| v.is_some_and(|t| t < sent))
            .map(|(dir, _)| *dir)
            .collect();
        for dir in &ready {
            d.grown.remove(dir);
            d.dirs.insert(*dir);
        }
        self.stats.deleg_grows_honoured += ready.len() as u64;
        let waiting = d.grown.values().any(|v| v.is_some());
        if waiting {
            self.deleg_renew_now(now, gen, out);
        }
        ready
    }

    pub(crate) fn drop_delegate_state(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // The log ended the generation (the root's reclaim, a recall's record)
        // before this node's own lapse watch or a refusal did: a grant that
        // had already run out is a grant lost, counted as one in a freeze
        // if one explains it. A recalled or refused generation (`stopped`),
        // and one already counted at its lapse (`lapsed`), are not.
        if self.dl.mine.get(&gen).is_some_and(|d| {
            !d.lapsed && !d.stopped && !d.non_stealable && now.honour >= self.deleg_lapse_at(d)
        }) {
            self.deleg_note_loss(gen, true);
        }
        let Some(d) = self.dl.mine.remove(&gen) else {
            return;
        };
        tracing::info!(node = self.me(), dirs = ?d.dirs, gen = %constellation_meta::delegation::GenLabel::of(gen), "delegation ended here");
        // Plan 43 C2 (D7): what waited for a row past the generation's cut
        // is never acknowledged (no cut known: every row of it).
        let through = replica.void_cut(gen).unwrap_or(0);
        self.deleg_abort_stream_parks_past(now, gen, through, replica, out);
        for t in [d.renew_timer, d.lapse_timer].into_iter().flatten() {
            self.cancel_timer(t, out);
        }
        if let Some((req, _)) = d.inflight {
            self.dl.by_req.remove(&req);
        }
        if let Some((req, _)) = d.renew {
            self.dl.by_req.remove(&req);
        }
        if let Some((req, _)) = d.ask {
            self.dl.by_req.remove(&req);
        }
        let root = self.holder_root().unwrap_or(0);
        for p in d.parked {
            self.answer_not_owner(now, p, root, replica, out);
        }
        // Plan 30 §M14: whatever lock grants of the subtree are still
        // here (the log ended the generation before the recall message
        // did, or without one) are dropped — their holders keep
        // honouring them: the root reinstated its copies of what it
        // handed, and an outwaited generation's own grants were capped
        // by its tenure. Unless this node is the root: then those on
        // inodes that are the root's now (the subtree's, or unlinked
        // under their lock) are this table's own, as a delegate's batch
        // carries them ([`Core::lock_install_leaving`]). Dropped, nobody
        // had them, and no grace covers an unlinked inode (sim
        // `locks-unlinked-delegated-hcrash-backup` seed 41: the root's
        // backup and `d1`'s delegate took the root over, ended its own
        // inherited generation, and granted itself an unlinked inode
        // node 3 still held exclusively).
        let mut dropped = self.lock_take_generation(gen, replica);
        if self.lease.held.is_some() && !self.lease.lost {
            let (kept, rest): (Vec<_>, Vec<_>) = dropped.into_iter().partition(|g| {
                g.until > now.wait
                    && matches!(
                        replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                        constellation_meta::delegation::Ownership::Root
                    )
            });
            dropped = rest;
            if !kept.is_empty() {
                let kept = kept.iter().map(|g| g.to_wire(now)).collect();
                self.lock_install_leaving(now, gen, kept, replica);
            }
        }
        if !dropped.is_empty() {
            tracing::info!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                n = dropped.len(),
                "delegation ended by the log with lock grants still here; dropped"
            );
        }
        self.lock_reserve_all(now, replica, out);
    }

    /// A parked op cannot execute here any more: the requester
    /// re-resolves (a redirect to the root, which recalls if it must).
    fn answer_not_owner(
        &mut self,
        now: Now,
        p: ParkedDeleg,
        holder: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.deleg_not_owner += 1;
        if p.from == 0 {
            // Own client op: back through the ordinary route.
            if self.clients.contains_key(&p.rid) {
                self.route(now, p.rid, replica, out);
            }
            return;
        }
        if let Some(req) = p.req {
            out.push(Action::Send {
                to: p.from,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome: MutateOutcome::NotHolder { holder },
                    base: None,
                    position: Position::ZERO,
                    gen: 0,
                    own_chunks: OwnChunks::None,
                    own_rows: None,
                    committed: false,
                    non_stealable: false,
                },
            });
        }
    }

    // ----------------------------------------------------- the delegate

    /// Plan 46 M1.2 step 9: some key lies in a subtree stream `gen` (this
    /// node's) grew by that this node does not honour yet. Judged against the
    /// table, not against what the last sync noted. The core and the lock
    /// service leave such a key to the root, whatever the stream's renewal
    /// says: the renewal covers the honoured subtrees only.
    pub(crate) fn deleg_parked_for(
        &self,
        keys: &TouchSet,
        gen: u64,
        replica: &dyn Replica,
    ) -> bool {
        let Some(s) = self.dl.mine.get(&gen) else {
            return false;
        };
        let honoured: Vec<Ino> = s.dirs.iter().copied().collect();
        replica
            .delegation_table()
            .touches_unhonoured(replica.namespace(), keys, gen, &honoured)
    }

    /// Whether `op` is this node's to execute as a delegate right now:
    /// the generation that owns every key, if it is one of this node's,
    /// not stopped, and its grant honoured.
    fn my_generation_for(&self, keys: &TouchSet, replica: &dyn Replica) -> Option<u64> {
        if self.dl.mine.is_empty() {
            return None;
        }
        match replica.resolve_ownership(keys) {
            Ownership::Delegated(d) if d.node == self.me() => {
                // ADR-55: a node inside its own takeover gate is claiming the
                // lease; the gate ends the streams it holds when it opens
                // (the claim path), and a row journaled in one meanwhile would
                // be stranded with the rest of its tail, an answer it gave
                // (a refusal included) void. Its ops wait for the gate
                // (`Held` to a forward, the gate's queue to its own).
                if self.own_claim_pending() {
                    return None;
                }
                let s = self.dl.mine.get(&d.gen)?;
                // Plan 46 M1.2 step 9: a subtree the stream grew by and this
                // node does not honour yet (its record not committed, or the
                // root's answer not here, or the sync that learns it not run)
                // is the root's still: judged against the table, not against
                // what the last sync noted.
                if self.deleg_parked_for(keys, d.gen, replica) {
                    return None;
                }
                // `stopped` follows the store's mark at the next event; a
                // generation a takeover's strand ended is refused at the
                // write already (`Meta::begin_local`), so the core never
                // executes under it, nor journals a refusal into it.
                (!s.stopped && !s.lapsed && !replica.deleg_ended_at_gate(d.gen)).then_some(d.gen)
            }
            _ => None,
        }
    }

    /// Plan 30 §M9's journaled refusal, as a delegate: a transaction of
    /// the stream (the root appends it; a retry by rid anywhere finds it).
    pub(crate) fn record_delegate_refusal(
        &mut self,
        rid: Rid,
        code: Code,
        gen: u64,
        deps: Position,
        replica: &dyn Replica,
    ) -> bool {
        // R-J: a refusal is a row of the stream, journaled only through the
        // holder's gate: for the root's own stream, honoured.
        debug_assert!(
            self.own_honoured(gen),
            "a refusal journaled in the root's own stream {gen} before it is honoured"
        );
        constellation_meta::clear_last_journaled();
        if let Err(error) = replica.delegate_refusal(rid, code, gen, deps) {
            tracing::warn!(node = self.me(), ?rid, %code, %error, "could not journal a delegate refusal");
            return false;
        }
        self.stats.refusals_journaled += 1;
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.executed += 1;
        }
        true
    }

    /// This node's own op `rid`, parked as a delegate before it executed
    /// (`delegate_try_execute`), reached its client deadline: the park is
    /// dropped (`true`), as a root's local park is
    /// (`abort_exec_local_park`).
    pub(crate) fn abort_deleg_own_park(&mut self, rid: Rid) -> bool {
        for d in self.dl.mine.values_mut() {
            if let Some(i) = d.parked.iter().position(|p| p.from == 0 && p.rid == rid) {
                d.parked.remove(i);
                return true;
            }
        }
        false
    }

    /// Execute `op` here as a delegate if it is this node's to execute
    /// (`true`), parking it while its `deps` are missing or the grant is
    /// not honoured. `from == 0` is this node's own client op.
    ///
    /// `settle` (plan 43 C2, §5.3 item 5): settle the rid instead — the same
    /// ownership and grant, never a wait for `deps` or a read recall (a
    /// refusal has no effect to order, and changes nothing a reader sees);
    /// an unexecuted park of the rid's earlier attempt here is dropped
    /// first (left parked it would run the op after the settle).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delegate_try_execute(
        &mut self,
        now: Now,
        from: NodeId,
        req: Option<OpId>,
        rid: Rid,
        op: &MutateOp,
        deps: Position,
        acked_through: u64,
        tag: &LockTag,
        settle: bool,
        fence: super::holder::Fence,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let keys = super::holder::keys_of_op_in(op, replica);
        let Some(gen) = self.my_generation_for(&keys, replica) else {
            return false;
        };
        // The fault `root_executes_under_own_grant` (ADR-55, R-I4): the root's
        // own path runs this node's own ops beside the stream that runs the
        // forwards routed by it.
        if from == 0
            && self.cfg.root_executes_under_own_grant
            && self
                .dl
                .gens
                .get(&gen)
                .is_some_and(|g| !g.ended && g.node == self.cfg.node_id)
        {
            return false;
        }
        // ADR-47 (the inbox rule): a rid whose batch is still in the holder's
        // inbox is not run here. Its batches are withdrawn first (the
        // tombstone; liveness): a drain read that predates it is halted, not
        // refused, because this node holds the grant (`inbox_halts_touched`).
        // The op is offered again once the withdrawal landed.
        if from == 0
            && self
                .clients
                .get(&rid)
                .is_some_and(|c| c.inbox_ever && !c.inbox_keys.is_empty())
        {
            self.stats.inbox_local_withdrawals += 1;
            self.send_forward(now, rid, 0, 0, replica, out);
            return true;
        }
        // Fence C9.2: an inbox-ever forward under a non-stealable generation
        // is refused in this generation's stream, whatever its route.
        let refuse_in_stream = self.refuses_in_stream(from, gen, fence, settle);
        let settle_like = settle || refuse_in_stream;
        if !refuse_in_stream && self.fence_forward(now, from, req, gen, (fence, settle), out) {
            return true;
        }
        // ADR-47: this node's own op, offered to its generation, is touched
        // by it from here on (parked or executed), before anything else can
        // send the rid on.
        if from == 0 {
            if let Some(c) = self.clients.get_mut(&rid) {
                c.touched_gen = c.touched_gen.max(gen);
                c.touched_local = true;
            }
        }
        if settle_like {
            for d in self.dl.mine.values_mut() {
                let before = d.parked.len();
                d.parked.retain(|p| p.rid != rid || p.settle);
                self.stats.settle_dropped_parks += (before - d.parked.len()) as u64;
            }
        }
        let honoured = self
            .dl
            .mine
            .get(&gen)
            .is_some_and(|d| now.honour < d.until || d.non_stealable)
            && self.own_honoured(gen);
        // ADR-46: a restarted holder executes nothing while the read grants
        // its previous process made may be live. A settle passes: its
        // refusal has no effect a reader sees.
        let quarantined = honoured && !settle_like && self.deleg_restart_quarantined(now, gen);
        if quarantined && from != 0 {
            if let Some(req) = req {
                // Not parked: a retry would park a duplicate, and a silent
                // park makes the requester spend its attempts. `Held` costs
                // it none (DESIGN §6).
                let left = self
                    .dl
                    .mine
                    .get(&gen)
                    .and_then(|d| d.restart_q.as_ref())
                    .map_or(0, |q| q.until.since(now.wait).max(0) as u64);
                self.stats.deleg_restart_q_held += 1;
                out.push(Action::Send {
                    to: from,
                    msg: PeerMsg::MutateReply {
                        req,
                        outcome: MutateOutcome::Held {
                            retry_ms: left.min(self.cfg.forward_timeout_ms).max(1),
                            outwait_ms: None,
                            lost_chunk: false,
                        },
                        base: None,
                        position: Position::ZERO,
                        // A delegate's: no lease holder to cache from it.
                        gen,
                        own_chunks: OwnChunks::None,
                        own_rows: None,
                        committed: false,
                        non_stealable: false,
                    },
                });
                return true;
            }
        }
        // The model's rule 1: the delegate waits for `deps` too.
        let reaches = settle_like
            || self.cfg.skip_deleg_dep_wait
            || self.stream_deps_reached(gen, &deps, replica);
        // Phase 2b (M8): the read delegations this delegate granted on
        // what the op touches are recalled first.
        let recalling = !settle_like
            && honoured
            && reaches
            && !quarantined
            && self.deleg_read_recall_pending(now, from, op, replica, out);
        // Plan 46 M1.3 step 5: a release of the subtree is draining; the op
        // waits for its row (it goes to the root then) or for the hold to
        // lift.
        let holding = !settle_like && self.sticky_holds(now, gen, &keys, replica);
        // Plan 46 M0.2 (review): behind the replay queue on its keys, as at
        // the root (R3); under `ack=s3` a delegate's rows for other nodes
        // are `Archive` and become placeholders too. The parked op is
        // released by the stream tick when the queue lets go.
        let queued = !settle_like && self.gated_by_queue(from, rid, op, replica);
        if queued {
            self.stats.ops_parked_behind_queue += 1;
        }
        if !honoured || !reaches || recalling || queued || holding || quarantined {
            tracing::debug!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?rid,
                honoured,
                reaches,
                recalling,
                ?deps,
                applied = ?replica.applied_position(),
                "delegate parks an op"
            );
            if !reaches {
                self.stats.deleg_deps_waits += 1;
            } else if !honoured {
                self.stats.deleg_parked_expired += 1;
            } else if holding {
                self.stats.deleg_sticky_release_held += 1;
            } else if quarantined {
                self.stats.deleg_restart_q_parks += 1;
            } else {
                self.stats.recall_waits += 1;
            }
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.parked.push(ParkedDeleg {
                from,
                req,
                rid,
                op: op.clone(),
                deps,
                acked_through,
                tag: tag.clone(),
                settle,
                fence,
                held: holding,
                arrived: now.wait,
            });
            if from == 0 {
                if let Some(c) = self.clients.get_mut(&rid) {
                    c.phase = super::client::Phase::Recalling;
                }
            }
            if !honoured {
                self.deleg_renew_now(now, gen, out);
            }
            return true;
        }
        self.delegate_execute_now(
            now,
            gen,
            from,
            req,
            rid,
            op,
            deps,
            (tag, settle, fence),
            replica,
            out,
        );
        true
    }

    /// Fence C9.2: whether a forward is refused in generation `gen`'s
    /// stream instead of run: it was ever in an inbox, this node holds its
    /// keys under `gen`, a non-stealable generation, and the forward is not
    /// a settle or this node's own op. Under a stealable generation nobody
    /// refuses for good (the root recalls), so the fence rules there.
    fn refuses_in_stream(
        &self,
        from: NodeId,
        gen: u64,
        fence: super::holder::Fence,
        settle: bool,
    ) -> bool {
        from != 0
            && fence.inbox_ever
            && !settle
            && !self.cfg.holder_runs_inbox_ever
            && self.dl.mine.get(&gen).is_some_and(|m| m.non_stealable)
    }

    /// ADR-47, the delegate fence: whether a forward from a peer is
    /// answered `Fenced` instead of being run or parked under `gen`, the
    /// generation this node holds its keys under, because it was not routed
    /// by `gen`. Nothing is parked or journaled. The one place the rule is
    /// decided; it runs before every other gate of `delegate_try_execute`.
    /// Any node, the root and a placed generation included.
    fn fence_forward(
        &mut self,
        now: Now,
        from: NodeId,
        req: Option<OpId>,
        gen: u64,
        (fence, settle): (super::holder::Fence, bool),
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.forward_is_fenced(now, from, gen, fence, settle) {
            return false;
        }
        self.stats.deleg_fenced_forwards += 1;
        if fence.inbox_ever && !settle {
            self.stats.inbox_ever_fenced += 1;
        }
        if let Some(req) = req.filter(|r| *r != OpId(0)) {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome: MutateOutcome::Fenced { gen },
                    base: None,
                    position: Position::ZERO,
                    gen,
                    own_chunks: OwnChunks::None,
                    own_rows: None,
                    committed: false,
                    non_stealable: false,
                },
            });
        }
        true
    }

    /// Whether `delegate_try_execute` would fence this forward: its keys are
    /// this node's under a generation the forward was not routed by.
    pub(crate) fn fenced_at_delegate(
        &self,
        now: Now,
        from: NodeId,
        op: &MutateOp,
        (fence, settle): (super::holder::Fence, bool),
        replica: &dyn Replica,
    ) -> bool {
        self.cfg.delegation
            && self
                .my_generation_for(&super::holder::keys_of_op_in(op, replica), replica)
                .is_some_and(|gen| self.forward_is_fenced(now, from, gen, fence, settle))
    }

    /// Whether a forward from `from` is not for this node's generation `gen`
    /// to run. Two rules, one per kind of rid:
    /// - an ordinary rid must have been routed by `gen` (`fence.routed_gen`);
    /// - a rid that was ever in an inbox (`fence.inbox_ever`) is run only
    ///   through the lease holder (its drain decides such a rid, and a
    ///   batch it still holds can be refused after a designee ran it). A
    ///   designee of a non-stealable generation never runs it (it refuses
    ///   it in the generation's stream, `refuses_in_stream`); of a stealable
    ///   one it answers `Fenced` whatever the route, except the lease
    ///   holder, which runs it as the designee of its own placed
    ///   generation (nobody refuses under a stealable grant for good).
    ///
    /// The node's own ops (`from == 0`) are never fenced.
    fn forward_is_fenced(
        &self,
        now: Now,
        from: NodeId,
        gen: u64,
        fence: super::holder::Fence,
        settle: bool,
    ) -> bool {
        if from == 0 || self.cfg.fence_accepts_unrouted {
            return false;
        }
        // (A settle executes nothing: it keeps the route rule.)
        if fence.inbox_ever && !settle {
            // Non-stealable: never run here (`refuses_in_stream` refuses it
            // in the stream). Stealable: the lease holder may still run it;
            // nobody refuses under a stealable grant for good, so no clock
            // carries I1 there.
            return (!self.cfg.holder_runs_inbox_ever
                && self.dl.mine.get(&gen).is_some_and(|m| m.non_stealable))
                || !self.lease.holds_unexpired(now, &self.cfg);
        }
        fence.routed_gen != gen
    }

    /// Phase 2b (M8 under M11): a write this delegate executes must not
    /// leave a read delegation it granted on what the write touches
    /// live (the holder's rule, `recall_needed`, for the delegate's
    /// subtree; the requester's own grant excepted). `true` while a
    /// recall is in flight (started here, once).
    fn deleg_read_recall_pending(
        &mut self,
        now: Now,
        from: NodeId,
        op: &MutateOp,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.cfg.read_delegations {
            return false;
        }
        let inos = replica.recall_inos_of_op(op);
        if inos.is_empty() {
            return false;
        }
        let except = Some(if from == 0 { self.me() } else { from });
        self.recall_needed(now, &inos, except, replica, out)
            .is_some()
    }

    #[allow(clippy::too_many_arguments)]
    fn delegate_execute_now(
        &mut self,
        now: Now,
        gen: u64,
        from: NodeId,
        req: Option<OpId>,
        rid: Rid,
        op: &MutateOp,
        deps: Position,
        (tag, settle, fence): (&LockTag, bool, super::holder::Fence),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let refuse_in_stream = self.refuses_in_stream(from, gen, fence, settle);
        let settle_like = settle || refuse_in_stream;
        // A dependency its generation ended without (a tentative
        // acknowledgement, replayed by rid after the end): executing now
        // would put this op in the log ahead of its cause (`marker-order`,
        // long-delegated seed 74189). Not a retry of an op already done.
        // (Nor a settle: its refusal has no effect to order.)
        if !settle_like
            && replica.deps_lost(&deps)
            && replica.recent_outcome(rid).is_none()
            && replica.completed_outcome(rid).ok().flatten().is_none()
        {
            if from == 0 {
                if self.hold_for_lost_deps(now, rid, replica, out) {
                    return;
                }
                let fresh = self.clients.get(&rid).map(|c| c.deps);
                if let Some(fresh) = fresh.filter(|d| !replica.deps_lost(d)) {
                    self.delegate_try_execute(
                        now,
                        0,
                        None,
                        rid,
                        op,
                        fresh,
                        0,
                        tag,
                        false,
                        super::holder::Fence::default(),
                        replica,
                        out,
                    );
                }
                return;
            }
            self.stats.deps_lost_refused += 1;
            tracing::debug!(
                node = self.cfg.node_id,
                ?rid,
                ?deps,
                "an execution whose deps were lost with their generation is refused"
            );
            if let Some(req) = req {
                out.push(Action::Send {
                    to: from,
                    msg: PeerMsg::MutateReply {
                        req,
                        outcome: MutateOutcome::Held {
                            retry_ms: self.cfg.delegation_stream_tick_ms.max(10),
                            outwait_ms: None,
                            lost_chunk: false,
                        },
                        base: None,
                        position: Position::ZERO,
                        // A delegate's: no lease holder to cache from it.
                        gen,
                        own_chunks: OwnChunks::None,
                        own_rows: None,
                        committed: false,
                        non_stealable: false,
                    },
                });
            }
            return;
        }
        // chaos-soak-4 seed 42 (`wf293`): what this replica holds ahead of
        // its applied log (the root's pre-S3 stream, a shadow of its own)
        // is nowhere in the log the requester's `base` check reads, so
        // an op touching it is answered without a base: the requester
        // waits for the log instead of installing the reply on a replica
        // that may lack the earlier write (see
        // `a_delegate_reply_base_covers_what_it_applied_before_the_grant_and_streamed_state`).
        let base = if replica.speculation_touches(&super::holder::keys_of_op(op)) {
            None
        } else {
            self.reply_base(op, replica)
        };
        // The requester installs an accepted reply as a shadow under
        // this epoch: the root's, as this delegate knows it, so that the
        // shadow strands at a root takeover (conservative: the record
        // re-streams and the replay dedups) and never before.
        let epoch = self.ship.max_epoch.max(1);
        let mut exec_idx: Option<u64> = None;
        // The stream whose row the answer waits for: this generation's, or
        // (a dedup) the generation that journaled the rid's row.
        let mut wait_gen = gen;
        // Plan 43 C2 (§2.1): what an answer of the row this delegate journals
        // now waits for, by who is asked; set for the journaling below (the
        // row's class is fixed there) and for the answer after it.
        let asker = if from == 0 { Asker::Own } else { Asker::Remote };
        // Plan 46 M1.1 step 4: the row is stamped with the configuration in
        // force when it is journaled: the version this replica has applied
        // now, not the one the last refresh read (a record applied by an
        // earlier step of this event loop since).
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            Self::deleg_group_refresh_one(d, replica);
        }
        // (A settle's refusal is never `Lone`: it waits for the group, also
        // for this node's own client — see `Core::settle_refusal`.)
        let class_asker = if settle_like { Asker::Remote } else { asker };
        let rule = self.deleg_rule(gen, class_asker);
        let mut class: constellation_meta::RowCommit = rule.into();
        let dedup = replica.recent_outcome(rid).is_some()
            || replica.completed_outcome(rid).ok().flatten().is_some();
        if dedup {
            // Plan 43 C2 (D6): an answer from the rid's earlier execution
            // waits for that row's commit as if it had just executed it —
            // the row may be this delegate's, still in its own uncommitted
            // journal, and the retry is no reason to leave it.
            let wait;
            (class, wait) = self.deleg_dedup_wait(class, gen, rid, from, replica);
            if let Some((g, idx)) = wait {
                (wait_gen, exec_idx) = (g, Some(idx));
            }
        }
        // The row is stamped as decided here, from the configuration read
        // now (the store never classes it again).
        let _row_class = (!dedup)
            .then(|| constellation_meta::PendingClass::set(self.deleg_row_commit(gen, rule)));
        constellation_meta::clear_last_journaled();
        let outcome = if let Some(records) = replica.recent_outcome(rid) {
            self.stats.forward_dedup_hits += 1;
            MutateOutcome::Accepted { epoch, records }
        } else if let Some(o) = replica.completed_outcome(rid).ok().flatten() {
            self.stats.forward_dedup_hits += 1;
            match o {
                constellation_meta::CompletedOutcome::Executed { .. } => MutateOutcome::Accepted {
                    epoch,
                    records: Vec::new(),
                },
                // As every other sequencer answers it: a stale base is the
                // conflict a client rebases on (never `ESTALE`), an ended
                // grant `LockLapsed`.
                constellation_meta::CompletedOutcome::Refused { code } => {
                    super::client::refused_as_outcome(code)
                }
            }
        } else if settle_like {
            // Plan 43 C2 (§5.3 item 5): no outcome here; refused for good,
            // a row of the stream like any refusal (its class set above),
            // answered once its group holds it (below).
            tracing::debug!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?rid,
                "settle: no outcome here; refused for good"
            );
            // No `deps`: the refusal has no effect to order after anything
            // (the settle did not wait for the op's), and a root appends a
            // stream row only once it holds what the row names.
            let none = Position::ZERO;
            // Fence C9.2: an inbox-ever forward is refused `EROFS`, as the
            // root's row 8 refuses one.
            let code = if refuse_in_stream {
                Code::ReadOnly
            } else {
                super::client::settle_code(rid, Some(op))
            };
            if self.record_delegate_refusal(rid, code, gen, none, replica) {
                if refuse_in_stream {
                    self.stats.holder_inbox_refusals += 1;
                    // ADR-55 (Q4): counted apart when the stream is the
                    // root's own.
                    if self
                        .dl
                        .gens
                        .get(&gen)
                        .is_some_and(|g| g.node == self.cfg.node_id)
                    {
                        self.stats.holder_inbox_refusals_own_root += 1;
                    }
                } else {
                    self.stats.settles_refused += 1;
                }
                exec_idx = Some(replica.delegate_idx(gen));
                super::client::refused_as_outcome(code)
            } else {
                MutateOutcome::Busy
            }
        } else {
            match replica.delegate_execute(op, Some(rid), gen, deps, tag, now) {
                Ok((records, idx)) => {
                    replica.remember_outcome(rid, &records);
                    if rid.node != self.cfg.node_id {
                        replica.note_foreign_executed(&records);
                    }
                    self.stats.deleg_executed += 1;
                    if self
                        .dl
                        .gens
                        .get(&gen)
                        .is_some_and(|g| g.node == self.cfg.node_id)
                    {
                        self.stats.root_own_answers += 1;
                        // (The root's own op the core ran in its own stream:
                        // the self-append does not count it.)
                        if from == 0 && self.cfg.placement {
                            let keys = TouchSet::from_records(records.iter());
                            let dirs = Core::dirs_of_keys(&keys, replica);
                            self.place_note(self.cfg.node_id, dirs);
                        }
                    }
                    if from == 0 {
                        self.handover_note_own_op(now, gen, op, replica);
                    }
                    if let Some(d) = self.dl.mine.get_mut(&gen) {
                        d.executed += 1;
                        if self.track_fence_audit && d.non_stealable {
                            self.nonstealable_executions.push((rid, gen));
                        }
                    }
                    exec_idx = Some(idx);
                    MutateOutcome::Accepted { epoch, records }
                }
                // A stale base or an ended grant: the rid's outcome,
                // journaled into the stream like any refusal (see
                // `holder_execute`), so no `Records` copy of the op by the
                // same rid runs it later.
                Err(MetaError::Conflict) => match op {
                    MutateOp::SetManifest { ino, .. } => {
                        self.record_delegate_refusal(rid, Code::Stale, gen, deps, replica);
                        exec_idx = Some(replica.delegate_idx(gen));
                        MutateOutcome::Conflict {
                            manifest: replica.manifest(*ino),
                        }
                    }
                    _ => MutateOutcome::Errno(Code::Again),
                },
                // Plan 30 §M14 phase 2: a grant the op was issued under
                // ended. Nothing ran; the refusal is the rid's outcome.
                Err(MetaError::LockLapsed) => {
                    self.stats.lock_lapsed_refusals += 1;
                    self.record_delegate_refusal(
                        rid,
                        super::client::LAPSED_CODE,
                        gen,
                        deps,
                        replica,
                    );
                    exec_idx = Some(replica.delegate_idx(gen));
                    MutateOutcome::LockLapsed
                }
                Err(MetaError::Exists) => {
                    let code = Code::Exists;
                    self.record_delegate_refusal(rid, code, gen, deps, replica);
                    exec_idx = Some(replica.delegate_idx(gen));
                    match super::client::named_child(op) {
                        Some((parent, name)) => match replica.entry_as_record(parent, name) {
                            Some(record) => MutateOutcome::Exists {
                                records: vec![record],
                                epoch,
                            },
                            None => MutateOutcome::Errno(code),
                        },
                        None => MutateOutcome::Errno(code),
                    }
                }
                Err(e) => {
                    let code = e.code();
                    self.record_delegate_refusal(rid, code, gen, deps, replica);
                    exec_idx = Some(replica.delegate_idx(gen));
                    MutateOutcome::Errno(code)
                }
            }
        };
        let position = Position {
            seq: replica.applied_seq().unwrap_or(0),
            pending: None,
            streams: {
                let mut s = constellation_meta::Streams::NONE;
                s.raise(gen, replica.stream_applied(gen));
                s
            },
        };
        self.arm_stream_tick(now, out);
        // Phase 2b: under a backup (or `ack=s3`) the acknowledgement
        // waits until the transaction is on the backup (or in a segment
        // this replica applied); the row streams meanwhile.
        let own_chunks = if from == 0 {
            OwnChunks::None
        } else {
            self.own_chunks_for(from, rid, &outcome, (gen, &position), replica)
        };
        // Plan 43 C2 (§3.4): `class` is the row's, fixed when it was
        // journaled. A row whose class waits (`DelegateNodeGroup`, `Archive`) is
        // answered once its group holds it: the root's append of the stream
        // that its own tenure committed, or the recorded backup's
        // acknowledgement, or the log.
        if let Some(idx) = exec_idx {
            if class.gated() && !self.deleg_stream_durable(wait_gen, idx, replica) {
                self.stats.deleg_acks_parked += 1;
                let what = if from == 0 {
                    if let Some(c) = self.clients.get_mut(&rid) {
                        c.phase = super::client::Phase::Recalling;
                    }
                    super::readindex::ParkedWhat::Finish {
                        rid,
                        outcome,
                        class,
                    }
                } else {
                    super::readindex::ParkedWhat::Reply {
                        to: from,
                        req,
                        rid,
                        outcome,
                        base,
                        position,
                        gen,
                        held_timer: None,
                        blockers: None,
                        completed: None,
                        class,
                    }
                };
                self.park_stream_need(now, wait_gen, idx, what);
                if matches!(own_chunks, OwnChunks::Upload(_)) {
                    // Neither this delegate's backup nor its stream to
                    // the root carries the transaction before the
                    // requester's chunks are up.
                    self.held_for_upload(rid, own_chunks, out);
                }
                self.deleg_backup_stream(now, replica, out);
                // The root's committed append is what a delegate without a
                // recorded backup waits for: send the row now, not on the
                // tick.
                self.deleg_stream(now, replica, out);
                return;
            }
            if class.gated() {
                if let Some(d) = self.dl.mine.get_mut(&wait_gen) {
                    d.acked_max = d.acked_max.max(idx);
                }
            }
        }
        if from == 0 {
            self.finish_as(
                now,
                rid,
                outcome,
                AnswerSource::Sequenced(class),
                replica,
                out,
            );
            return;
        }
        if let Some(req) = req {
            self.send_answer(
                now,
                rid,
                super::backup::EmissionPath::DelegExec,
                from,
                PeerMsg::MutateReply {
                    req,
                    outcome,
                    base,
                    position,
                    gen,
                    own_chunks,
                    // A delegate's own rows are not worked out
                    // (`Meta::remote_blockers`): the requester waits for
                    // everything through the position.
                    own_rows: None,
                    committed: false,
                    // ADR-55: a non-stealable grant's answer is final by
                    // design; the requester may not list the grant yet.
                    non_stealable: self.dl.mine.get(&gen).is_some_and(|d| d.non_stealable),
                },
                class,
                replica,
                out,
            );
        }
    }

    /// The rule stored with a row of generation `gen` this delegate
    /// journals (or publishes for its fast path): its configuration's, in
    /// the version of the group in force. With `Config::per_row_class` (the
    /// negative control) a row journaled while the delegate records no
    /// backup carries a wrong version (`RowCommit::PER_ROW_SKEW` past its
    /// group's).
    fn deleg_row_commit(&self, gen: u64, rule: CommitRule) -> constellation_meta::RowCommit {
        let commit = constellation_meta::RowCommit::from(rule);
        match (self.cfg.per_row_class, self.dl.mine.get(&gen), commit) {
            (true, Some(d), constellation_meta::RowCommit::Group { on, .. })
                if d.group_backups.is_empty() =>
            {
                // A version close to the real one and in no history: the
                // group's applied version plus a skew no run reaches.
                constellation_meta::RowCommit::group(
                    d.group_version + constellation_meta::RowCommit::PER_ROW_SKEW,
                    on,
                )
            }
            _ => commit,
        }
    }

    /// Plan 46 M1.1 step 2: the commit rule of this node's delegate
    /// generation `gen`, for an answer to `asker`. Under `ack=s3`, or once the generation ended here
    /// (what is left of it is in the log), the group has no member and the
    /// archive carries it; a designation (or a sticky grant) is `Final`;
    /// otherwise the generation's group at its applied `DelegateNodeGroup`
    /// version. Plan 46 M1.2 step 13: no delegate answers on its disk alone
    /// (the root grants only to a node whose group is within budget, or a
    /// non-stealable grant); only the negative control
    /// (`Config::lone_delegate`) does.
    pub fn deleg_rule(&self, gen: u64, asker: Asker) -> CommitRule {
        let archive = CommitRule::Group {
            version: 0,
            members: 0,
            on: GroupOn::Archive,
        };
        if self.cfg.ack_s3 {
            return archive;
        }
        match self.dl.mine.get(&gen) {
            Some(d) if d.non_stealable => CommitRule::Final,
            Some(d)
                if self.cfg.lone_delegate
                    && asker == Asker::Own
                    && d.group_backups.is_empty()
                    && d.group_requested.is_none()
                    && d.backup.is_none()
                    && !self.backup_could_be_had() =>
            {
                CommitRule::Singleton {
                    version: d.group_version,
                }
            }
            Some(d) => CommitRule::Group {
                version: d.group_version,
                members: d.group_backups.len(),
                on: GroupOn::Delegate,
            },
            None => archive,
        }
    }

    /// Plan 43 C2 (D6): the class and wait of the answer to a retry of `rid`
    /// that this delegate answers from the rid's earlier execution (`class`:
    /// what a new row would be given). The earlier row may still be in this
    /// delegate's own journal and uncommitted; the retry's answer then
    /// waits for it exactly as the first execution's did. A row of this
    /// generation that waits for its group (gated), or any row asked for by
    /// another node, parks under `DelegateNodeGroup`: prefix commit makes
    /// the row committed once the root's append (or the backup) holds a
    /// later index. This node's own client, retrying a singleton row (only
    /// the negative control `Config::lone_delegate` journals one), is
    /// answered at once as such. The row gone from the journal (the log carries it, or it was
    /// shipped) is answered from the log. The row may be of another
    /// generation of this node than `gen` (the rid's keys moved to it: the
    /// directory delegated here again before the first generation's
    /// strand): the wait is then for that generation's
    /// row — `(generation, index)` — and a generation that ended here
    /// commits only through the log, or drops the row and aborts the wait.
    fn deleg_dedup_wait(
        &self,
        class: constellation_meta::RowCommit,
        gen: u64,
        rid: Rid,
        from: NodeId,
        replica: &dyn Replica,
    ) -> (constellation_meta::RowCommit, Option<(u64, u64)>) {
        let committed_class = |row: constellation_meta::RowCommit| {
            constellation_meta::RowCommit::group(
                row.config_version().unwrap_or(0),
                if self.cfg.ack_s3 {
                    GroupOn::Archive
                } else {
                    GroupOn::Delegate
                },
            )
        };
        match replica.outcome_row(rid) {
            Some(r) if r.gen != 0 => {
                if class.is_final() {
                    (class, None)
                } else if r.class.gated() || from != 0 || r.gen != gen {
                    (committed_class(r.class), Some((r.gen, r.idx)))
                } else {
                    (r.class.as_singleton(0), None)
                }
            }
            _ if class.is_final() => (class, None),
            _ => (class.as_archive(0), None),
        }
    }

    /// The committed prefix of `gen`'s stream as this delegate knows it
    /// (`None`: not held here), for the simulation's oracles.
    pub fn deleg_committed_for_audit(&self, gen: u64, replica: &dyn Replica) -> Option<u64> {
        self.dl
            .mine
            .get(&gen)
            .map(|_| self.deleg_committed_capped(gen, replica))
    }

    /// The backup this delegate tells the root it wants (`DelegRenew.backup`,
    /// the *request* for a `DelegateNodeGroup` record): the change it asked for and
    /// the table does not record yet, else the group it applied.
    pub(crate) fn deleg_requested_backup(d: &DelegateState) -> Option<NodeId> {
        match &d.group_requested {
            Some(v) => v.first().copied(),
            None => d.group_backups.first().copied(),
        }
    }

    /// A row of `txs` (rows of `gen` read for a batch) whose dependency was
    /// lost with its generation (`Replica::deps_lost`) stops the generation
    /// before it, before anything holds it ([`Self::deleg_note_lost`]).
    fn deleg_note_lost_in(
        &mut self,
        gen: u64,
        txs: &[constellation_meta::DelegateTx],
        replica: &dyn Replica,
    ) {
        if let Some(t) = txs.iter().find(|t| replica.deps_lost(&t.deps)) {
            self.deleg_note_lost(gen, t.idx);
        }
    }

    /// What `d`'s generation has committed for its `DelegateNodeGroup`
    /// acknowledgements: the prefix the log carries (`log`), or its group
    /// holds; `ack=s3` commits only through the log. Nothing at or past a
    /// row whose dependency was lost is appended: the generation ends
    /// before it ([`DelegateState::lost_from`]). One stream per node: a row
    /// depends on no uncommitted row of another stream of its node, so
    /// nothing else caps the prefix.
    fn deleg_committed_prefix(&self, d: &DelegateState, log: u64) -> u64 {
        let committed = self.deleg_committed_through(d, log);
        match d.lost_from {
            Some(l) => committed.min((l - 1).max(log)),
            None => committed,
        }
    }

    /// Plan 43 C2 (§3.4): transaction `(gen, idx)` is committed — the answer
    /// of a row of the class `DelegateNodeGroup` may leave — from what this
    /// delegate knows: the log carries it, or every member of the group
    /// holds it. See [`Self::deleg_stream_state_with`].
    pub(crate) fn deleg_stream_durable(&self, gen: u64, idx: u64, replica: &dyn Replica) -> bool {
        self.deleg_stream_state_with(gen, idx, || replica.log_stream_idx(gen), || true)
            == StreamState::Committed
            && !self.holder_append_uncommitted(gen, idx, replica)
    }

    /// Plan 43 C3: whether this node's own unshipped journal holds row
    /// `(gen, idx)` as a lease holder's append that its root tenure has not
    /// committed (shipped, or held by every committed backup). Such an
    /// append — a takeover gate's re-append of this node's own rows as the
    /// delegate, or its re-applied predecessor's tail — raised
    /// `log_stream_idx` here, which [`Self::deleg_stream_state_with`]
    /// counts as the log or the root's copy; on this node it is neither,
    /// only this disk. A row so held is not committed yet, whoever waits
    /// for it: a park, a retry's dedup answer (`deleg_dedup_wait`), a
    /// `completed` row answered at a deadline (`completed_row_settled`), the
    /// fast path's published watermark (`report_deleg_durable`), a recall's
    /// answer. Before, a delegate that took the root over answered such a
    /// row at the gate on its own disk alone (`delegated-holder-cut
    /// +commit` seeds 124, 214, 348: I1). A deposed (or released) tenure
    /// counts only what shipped; a lone one (nothing gates its own rows,
    /// `durable_jseq` is `u64::MAX`) too: its answer would be a `Lone` one,
    /// which no reader of this re-tags, so it waits for the archive. A
    /// store error counts as not committed (the check runs again at the
    /// next event).
    pub(crate) fn holder_append_uncommitted(
        &self,
        gen: u64,
        idx: u64,
        replica: &dyn Replica,
    ) -> bool {
        // One atomic load for a node that never appended as a holder.
        let tip = replica.holder_append_tip();
        if tip == 0 {
            return false;
        }
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        if tip <= shipped {
            return false;
        }
        match replica.holder_append_of(gen, idx) {
            Ok(None) => false,
            Ok(Some(last)) => !self.holder_append_committed(last, shipped),
            Err(_) => true,
        }
    }

    /// Whether this tenure committed its journal row ending at `last`
    /// (shipped through `shipped`, or out of order, or on every committed
    /// backup of a tenure that still holds and is not lone).
    fn holder_append_committed(&self, last: u64, shipped: u64) -> bool {
        let holds = self.lease.held.is_some() && !self.lease.lost;
        let durable = self.durable_jseq();
        last <= shipped
            || holds
                && (self.ack.shipped_rows.contains(&last)
                    || (durable != u64::MAX && last <= durable))
    }

    /// The highest index of generation `gen` below this node's first holder
    /// append of it, in index order, that its tenure has not committed
    /// (`u64::MAX`: none). Not a walk down from a prefix: rows ship out of
    /// order (`ack.shipped_rows`), so a higher row may be in the log while a
    /// lower one is held (a chunk upload it waits for); the scan goes up
    /// from the log's floor. Cached per generation and recomputed only when
    /// what it is computed from changed (the appends' tip, the shipped
    /// watermark and out-of-order set, the durable watermark, the lease):
    /// the fast path's report asks it every event for a generation a
    /// takeover stopped. A store error caps at 0 for this call, uncached.
    pub(crate) fn holder_append_cap(&mut self, gen: u64, replica: &dyn Replica) -> u64 {
        let tip = replica.holder_append_tip();
        if tip == 0 {
            return u64::MAX;
        }
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        if tip <= shipped {
            self.dl.holder_caps.remove(&gen);
            return u64::MAX;
        }
        let stamp = HolderCapStamp {
            tip,
            shipped,
            out_of_order: (
                self.ack.shipped_rows.len(),
                self.ack.shipped_rows.last().copied().unwrap_or(0),
            ),
            durable: self.durable_jseq(),
            holds: self.lease.held.is_some() && !self.lease.lost,
        };
        if let Some((s, cap)) = self.dl.holder_caps.get(&gen) {
            if *s == stamp {
                return *cap;
            }
        }
        let Ok(appends) = replica.holder_appends(gen) else {
            return 0;
        };
        let cap = appends
            .iter()
            .find(|(_, last)| !self.holder_append_committed(*last, shipped))
            .map_or(u64::MAX, |(idx, _)| idx.saturating_sub(1));
        self.dl.holder_caps.insert(gen, (stamp, cap));
        cap
    }

    /// Plan 43 Q16 (ADR-41): the highest index of delegate generation `gen`
    /// below the first row this node, as its root, appended and its tenure
    /// has not committed (`u64::MAX`: none) — what its
    /// `DelegateStreamAck.committed` is capped by. The group `{delegate,
    /// root}` counts the root's copy of a row only once it survives the
    /// root's deposition: the row is shipped (`journal_acked_seq`, or out of
    /// order), or this tenure holds the lease, is not lost, and every
    /// backup it gates on holds it ([`Self::config_committed_jseq`]). A root
    /// with backups therefore acknowledges only once they hold the append:
    /// its successor is a backup, or has sealed one, and holds the row. A
    /// lone root's journal counts for nothing (plan 46 §11, "M1.0 answers":
    /// ADR-41's lone-journal branch removed): a lone root paused past its
    /// expiry is taken over from the archive alone, and a row the delegate
    /// answered on that journal is lost when the delegate then fails; with
    /// no backup the root's configuration commits by the archive. (Model:
    /// `node_stream`'s knob `root_journal_counts` violates
    /// `acked_never_lost`.) An epoch hold's journal still counts (plan 46
    /// M1.4 makes the hold a group). Cached like [`Self::holder_append_cap`],
    /// in `dl.root_caps` (keyed also by where the scan starts, the committed
    /// prefix last sent); a store error caps at 0, uncached.
    pub(crate) fn root_append_cap(&mut self, gen: u64, replica: &dyn Replica) -> u64 {
        let tip = replica.holder_append_tip();
        if tip == 0 {
            return u64::MAX;
        }
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        if tip <= shipped {
            self.dl.root_caps.remove(&gen);
            return u64::MAX;
        }
        let holds = self.lease.held.is_some() && !self.lease.lost;
        let durable = self.config_committed_jseq();
        let stamp = HolderCapStamp {
            tip,
            shipped,
            out_of_order: (
                self.ack.shipped_rows.len(),
                self.ack.shipped_rows.last().copied().unwrap_or(0),
            ),
            durable,
            holds,
        };
        // The scan starts past the committed prefix last sent (G1 follow-up:
        // one seek, where a walk from 0 read every deleted `journal_origin`
        // key of the generation's shipped history, a cost that grew with its
        // length on both of the delegate's critical-path calls). It rests on
        // two things. (a) `committed_sent` lives within one root tenure:
        // `GenState` goes with the tenure (`deleg_on_lease_gone`), and a new
        // round of the stream starts it again at 0 (`on_delegate_stream`),
        // so the prefix this tenure sent is never one the delegate has since
        // reset (it resets `root_committed` when its root changes). (b) A row
        // this tenure committed stays committed: it is shipped, or on every
        // backup this tenure gated on, which a successor holds or has
        // sealed. A full scan lowers the cap after a send only through
        // `config_committed_jseq` falling while the lease is held (a backup
        // newly listed with nothing counted, a listing out or in doubt, an
        // `AckPolicy` change, an epoch hold ending); the narrowed scan never claims less than the
        // prefix already sent there, which (b) justifies, and the cap is the
        // full scan's wherever that is not below it. `committed_sent` is
        // assigned, not raised, by a batch's acknowledgement: it may go back,
        // which only widens the scan.
        let from = self
            .dl
            .gens
            .get(&gen)
            .map_or(0, |g| g.committed_sent)
            .saturating_add(1);
        if let Some((s, f, cap)) = self.dl.root_caps.get(&gen) {
            if *s == stamp && *f == from {
                return *cap;
            }
        }
        let Ok(appends) = replica.holder_appends_from(gen, from) else {
            return 0;
        };
        let cap = appends
            .iter()
            .find(|(_, last)| {
                !(*last <= shipped
                    || holds && (self.ack.shipped_rows.contains(last) || *last <= durable))
            })
            .map_or(u64::MAX, |(idx, _)| idx.saturating_sub(1));
        self.dl.root_caps.insert(gen, (stamp, from, cap));
        cap
    }

    /// Plan 43 Q16 (ADR-41): as the root, tell each delegate its rows' commit
    /// as it rises. The acknowledgement of a batch carries what the root's
    /// tenure has committed then; the backups' acknowledgements, a ship, a
    /// lease change come later, and a delegate that waits for them would
    /// otherwise wait for its next batch. Every generation not ended whose
    /// appended cursor is above the last `committed` sent is recomputed; one
    /// that rose is sent an acknowledgement of its own last batch (its round
    /// and request: the delegate drops no acknowledgement of its current
    /// round, and raises `root_committed` before it looks at the request).
    pub(crate) fn root_stream_push(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let due: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.last_batch.is_some() && g.cursor > g.committed_sent)
            .map(|(gen, _)| *gen)
            .collect();
        if due.is_empty() || !self.root_serves_generations(now) {
            return;
        }
        self.dl
            .root_caps
            .retain(|g, _| self.dl.gens.get(g).is_some_and(|g| !g.ended));
        for gen in due {
            let cap = self.root_append_cap(gen, replica);
            let g = self.dl.gens.get_mut(&gen).expect("present");
            let committed = g.cursor.min(cap);
            if committed <= g.committed_sent {
                continue;
            }
            let Some((round, req)) = g.last_batch else {
                continue;
            };
            g.committed_sent = committed;
            let (to, through) = (g.node, g.cursor);
            let archive_bound = self.config_archive_bound();
            let every = self.cfg.delegation_push_drop_every;
            let nth =
                self.stats.deleg_stream_acks_pushed + self.stats.deleg_stream_acks_push_dropped + 1;
            if every > 0 && nth.is_multiple_of(every) {
                // (A push lost on the way: `committed_sent` has risen, and it
                // is counted apart from the pushes sent.)
                self.stats.deleg_stream_acks_push_dropped += 1;
                continue;
            }
            self.stats.deleg_stream_acks_pushed += 1;
            out.push(Action::Send {
                to,
                msg: PeerMsg::DelegateStreamAck {
                    req,
                    gen,
                    round,
                    through,
                    committed,
                    refused: false,
                    archive_bound: archive_bound && committed < through,
                },
            });
        }
    }

    /// [`Self::deleg_stream_state_with`] with what it reads cached across
    /// calls (`cache`): `complete_ready` checks every parked acknowledgement
    /// on every event, and a read of the store per check was most of a
    /// loaded delegate's core time while its backup lagged
    /// (`stress-ng-fs-nodes`: 73% of the core thread's samples, steps of
    /// seconds back to back).
    pub(crate) fn deleg_stream_state_cached(
        &self,
        gen: u64,
        idx: u64,
        replica: &dyn Replica,
        cache: &mut StreamCache,
    ) -> StreamState {
        let StreamCache {
            log_idx, pending, ..
        } = cache;
        let state = self.deleg_stream_state_with(
            gen,
            idx,
            || {
                *log_idx
                    .entry(gen)
                    .or_insert_with(|| replica.log_stream_idx(gen))
            },
            || {
                *pending
                    .entry((gen, idx))
                    .or_insert_with(|| replica.delegate_tx_pending(gen, idx).unwrap_or(true))
            },
        );
        // (Read only for a row about to be released: one seek.)
        if state == StreamState::Committed && self.holder_append_uncommitted(gen, idx, replica) {
            StreamState::Waiting
        } else {
            state
        }
    }

    /// Plan 43 C2 (§3.4): the highest index of `d`'s stream that every
    /// member of its group holds — of the applied version and, while a
    /// change is pending, of the requested one (D4): a recorded backup by
    /// its acknowledgement of this delegate's appends (a backup that is
    /// not the one appended to holds nothing for it), the root (no backup
    /// recorded: the group is the delegate and the root, Q16) by the
    /// prefix of its appends of the stream that its own tenure has committed
    /// (`root_committed`: shipped, or held by its backups; the appended
    /// cursor `streamed_through` drives the flow control and is no second
    /// copy, plan 43 Q16, ADR-41). What the log carries is not counted here.
    pub(crate) fn deleg_group_through(d: &DelegateState) -> u64 {
        let held = |members: &[NodeId]| {
            members
                .iter()
                .map(|b| {
                    if d.backup == Some(*b) {
                        d.backup_acked
                    } else {
                        0
                    }
                })
                .min()
                .unwrap_or(d.root_committed)
        };
        let current =
            held(&d.group_backups).min(d.group_requested.as_deref().map_or(u64::MAX, held));
        d.group_spans
            .iter()
            .filter_map(|s| Self::group_span_cap(d, s))
            .fold(current, u64::min)
    }

    /// The index through which a closed group's own members hold the
    /// stream, while they do not hold its end (`None`: they do).
    fn group_span_cap(d: &DelegateState, s: &GroupSpan) -> Option<u64> {
        let through = match s.backups.as_slice() {
            [] => d.root_committed,
            members => members
                .iter()
                .map(|b| {
                    if d.backup == Some(*b) {
                        s.acked.max(d.backup_acked)
                    } else {
                        s.acked
                    }
                })
                .min()
                .unwrap_or(0),
        };
        (through < s.end).then_some(through)
    }

    /// The committed prefix of `gen`'s stream ([`Self::deleg_committed_prefix`]):
    /// what a `DelegateNodeGroup` answer waits for.
    fn deleg_committed_capped(&self, gen: u64, replica: &dyn Replica) -> u64 {
        let log = replica.log_stream_idx(gen);
        match self.dl.mine.get(&gen) {
            Some(d) => self.deleg_committed_prefix(d, log),
            None => log,
        }
    }

    /// Plan 43 C2d (§4): the highest index of generation `d`'s stream whose
    /// prefix the log carries (`log_idx`) or its group holds. `ack=s3`
    /// commits only through the log. What an acknowledgement waits for is
    /// this, stopped at a lost dependency ([`Self::deleg_committed_prefix`]).
    fn deleg_committed_through(&self, d: &DelegateState, log_idx: u64) -> u64 {
        if self.cfg.ack_s3 {
            log_idx
        } else {
            Self::deleg_group_through(d).max(log_idx)
        }
    }

    /// Plan 43 C2d (§4): tell the replica's session, for each generation
    /// this node is the delegate of, how its own clients' rows are classed
    /// and how far the group has committed the stream — the FUSE fast path
    /// journals its row with the class and answers once the watermark
    /// covers it. Published on change; a generation no longer held is
    /// withdrawn, which ends a wait on it (`Lost`).
    pub(crate) fn report_deleg_durable(&mut self, replica: &dyn Replica) {
        if self.dl.mine.is_empty() && self.dl.reported.is_empty() {
            return;
        }
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        self.dl.holder_caps.retain(|g, _| gens.contains(g));
        let holder_caps: BTreeMap<u64, u64> = gens
            .into_iter()
            .map(|g| (g, self.holder_append_cap(g, replica)))
            .collect();
        let mut current = BTreeMap::new();
        let mut raised = Vec::new();
        for d in self.dl.mine.values() {
            let rule = self.deleg_rule(d.gen, Asker::Own);
            let through = self.deleg_committed_prefix(d, replica.log_stream_idx(d.gen));
            // The prefix is floored at the log's index, which this node's own
            // holder appends raise (a takeover gate's re-append): the fast
            // path's waiter, whose high-water (`SessionState::deleg_covered`)
            // outlives `lost` and the generation's withdrawal, would take such a row
            // on this disk alone for committed, now or after the report
            // that withdraws the generation.
            let through = through.min(holder_caps.get(&d.gen).copied().unwrap_or(u64::MAX));
            // The fast path answers its own node's clients from the FUSE
            // thread, where the core does not see it: every index committed
            // may have been answered, so `deleg_group_maintain` lets go of
            // none of them before the group that is left holds it
            // (`acked_max`). With no backup recorded `through` is at most
            // the root's committed prefix (`root_committed`) or the log's
            // index, so the raise holds back an added backup only until it
            // has caught up with what the root has committed and never
            // blocks a removal (`acked_max <= root_committed.max(logged)`,
            // ADR-41).
            if through > d.acked_max {
                raised.push((d.gen, through));
            }
            current.insert(
                d.gen,
                constellation_meta::DelegDurable {
                    commit: self.deleg_row_commit(d.gen, rule),
                    through,
                    lost: d.stopped || d.lapsed || d.backup_sealed,
                },
            );
        }
        for (gen, through) in raised {
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                d.acked_max = d.acked_max.max(through);
            }
        }
        for gen in self.dl.reported.keys() {
            if !current.contains_key(gen) {
                replica.set_deleg_durable(*gen, None);
            }
        }
        for (gen, d) in &current {
            if self.dl.reported.get(gen) != Some(d) {
                replica.set_deleg_durable(*gen, Some(*d));
            }
        }
        self.dl.reported = current;
    }

    /// Plan 43 C2 (§3.4, D7): where row `(gen, idx)` of a delegate's stream
    /// stands. Committed: the log carries it (`idx` at or below the log's
    /// index of the generation, never "the row left this journal"), or every
    /// member of the applied group — and of a requested one, while a change
    /// is pending (D4: the root may end the generation by either version) —
    /// holds it: a recorded backup by its acknowledgement of this delegate's
    /// appends, the root (no backup recorded: the group is the delegate and
    /// the root, Q16) by the prefix of its appends of the stream that its own
    /// tenure has committed (`DelegateStreamAck.committed`, `root_committed`;
    /// the appended cursor `through` is the flow control's, and a deposed
    /// root's journal holds a row no successor does: ADR-41). A generation no
    /// longer held here, and `ack=s3`, commit only through the log. Not
    /// committed and still in the journal (`pending`): waiting. Not
    /// committed and gone from the journal: the row was dropped — a
    /// recall's or a takeover's strand, a rebuild — and what its
    /// acknowledgement waited for will never come: the acknowledgement is
    /// aborted (the requester, or this node's own client, retries by rid),
    /// not released as if the log carried a write it never will.
    fn deleg_stream_state_with(
        &self,
        gen: u64,
        idx: u64,
        log_idx: impl FnOnce() -> u64,
        pending: impl FnOnce() -> bool,
    ) -> StreamState {
        let d = self.dl.mine.get(&gen);
        let log = log_idx();
        let committed = match d {
            Some(d) => idx <= self.deleg_committed_prefix(d, log),
            None => idx <= log,
        };
        if committed {
            StreamState::Committed
        } else if d.and_then(|d| d.lost_from).is_some_and(|l| idx >= l) {
            // A row at or past one whose dependency was lost is never
            // appended: the generation ends before it.
            StreamState::Dropped
        } else if pending() {
            StreamState::Waiting
        } else {
            StreamState::Dropped
        }
    }

    /// Plan 43 C2 (§3.3): request a change of the generation's group when
    /// one is due — the transposition of the root lease's add-then-record,
    /// remove-when-safe rules (`backup_promote`, `removal_safe`). The
    /// delegate *requests* it in its next renewal (`DelegRenew.backup`); the
    /// root journals the `DelegateNodeGroup` record; the delegate counts the new
    /// member only once its own table records it, and an acknowledgement
    /// needs the members of both versions meanwhile.
    ///
    /// - Adding or replacing: the backup appended to holds every index
    ///   answered so far (`backup_acked >= acked_max`), or the log does.
    /// - Removing (the backup is gone: dropped, sealed): every index
    ///   answered is at or below the root's journaled cursor or in the log
    ///   (`removal_safe`), so no acknowledged row is left only on a backup
    ///   the record stops naming.
    fn deleg_group_maintain(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            if d.stopped || d.group_requested.is_some() {
                continue;
            }
            let want: Vec<NodeId> = d.backup.into_iter().collect();
            if want == d.group_backups {
                continue;
            }
            let adding = want.iter().any(|b| !d.group_backups.contains(b));
            let (acked_max, backup_acked, root_held) =
                (d.acked_max, d.backup_acked, d.root_committed);
            let safe = acked_max == 0 || {
                let logged = replica.log_stream_idx(gen);
                if adding {
                    backup_acked >= acked_max || acked_max <= logged
                } else {
                    acked_max <= root_held.max(logged)
                }
            };
            if !safe {
                continue;
            }
            tracing::info!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?want,
                applied = ?self.dl.mine[&gen].group_backups,
                "delegate asks the root to record a group change"
            );
            self.dl.mine.get_mut(&gen).expect("present").group_requested = Some(want);
            self.deleg_renew_now(now, gen, out);
        }
    }

    /// Plan 43 C2 (D4): the delegate learns its group from its replica's
    /// table, nowhere else — a record applied from the log (or the
    /// committed pre-S3 stream). A pending request ends when the table
    /// records it.
    pub(crate) fn deleg_group_refresh(&mut self, replica: &dyn Replica) {
        for d in self.dl.mine.values_mut() {
            Self::deleg_group_refresh_one(d, replica);
        }
    }

    /// [`Self::deleg_group_refresh`] for one generation.
    fn deleg_group_refresh_one(d: &mut DelegateState, replica: &dyn Replica) {
        // A read that failed is not "no record": the group known stays
        // (plan 43 C2e review: an emptied group made the delegate lone).
        let Ok(row) = replica.deleg_group(d.gen) else {
            return;
        };
        let (version, backups) = row.map_or((0, Vec::new()), |r| (r.version, r.backups));
        if version != d.group_version || backups != d.group_backups {
            if backups != d.group_backups {
                // The record closes the group it replaces at the stream's
                // index now: rows before keep that group's rule.
                let acked = if d.group_backups.contains(&d.backup.unwrap_or(0)) {
                    d.backup_acked
                } else {
                    0
                };
                d.group_spans.push(GroupSpan {
                    backups: std::mem::take(&mut d.group_backups),
                    acked,
                    end: replica.delegate_idx(d.gen),
                });
            }
            d.group_version = version;
            d.group_backups = backups;
        }
        if !d.group_spans.is_empty() {
            let log = replica.log_stream_idx(d.gen);
            let spans = std::mem::take(&mut d.group_spans);
            d.group_spans = spans
                .into_iter()
                .filter(|s| Self::group_span_cap(d, s).is_some_and(|t| t.max(log) < s.end))
                .collect();
        }
        if d.group_requested.as_ref() == Some(&d.group_backups) {
            d.group_requested = None;
        }
    }

    /// Phase 2b: pick a backup for each generation held here (M9's
    /// candidate rule: a write-eligible peer within the RTT budget,
    /// connected the longest), never the root itself.
    fn deleg_backup_select(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.cfg.backup_rtt_budget_ms == 0 {
            return;
        }
        let root = self.root_node();
        let candidates = self.backup_candidates(now);
        let mut chosen = Vec::new();
        let mut backed = self.dl.mine.values().filter(|d| d.backup.is_some()).count();
        for d in self.dl.mine.values_mut() {
            if d.backup.is_some() || d.backup_sealed || d.stopped {
                continue;
            }
            if backed >= self.cfg.delegation_backed_gens_max {
                continue;
            }
            if let Some(b) = candidates.iter().find(|n| Some(**n) != root).copied() {
                backed += 1;
                d.backup = Some(b);
                d.backup_acked = 0;
                d.backup_sent_through = 0;
                d.backup_after = None;
                chosen.push(d.gen);
                tracing::info!(
                    node = self.cfg.node_id,
                    gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                    backup = b,
                    "delegate chose a backup"
                );
            }
        }
        if !chosen.is_empty() {
            // A late answer from an earlier stint of the same peer as this
            // generation's backup counts nothing for this one, which
            // starts from 0 (as a late stream acknowledgement across a
            // root change, `deleg_after_event`).
            self.dl
                .late
                .retain(|_, l| !(l.kind == ReqKind::BackupAppend && chosen.contains(&l.gen)));
        }
        for gen in chosen {
            // The root learns the backup from a renewal (it seals it when
            // this delegate falls silent): send one now.
            self.deleg_renew_now(now, gen, out);
        }
    }

    /// Phase 2b: append what the backup lacks, one batch in flight.
    pub(crate) fn deleg_backup_stream(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            let Some(backup) = d.backup else { continue };
            if d.backup_inflight.is_some() || d.backup_after.is_some_and(|t| now.wait < t) {
                continue;
            }
            let from = d.backup_sent_through + 1;
            let mut txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            if txs.is_empty() {
                continue;
            }
            self.deleg_note_lost_in(gen, &txs, replica);
            // Nothing at or past a row whose dependency was lost goes to
            // the backup either.
            if let Some(l) = self.dl.mine.get(&gen).and_then(|d| d.lost_from) {
                txs.retain(|t| t.idx < l);
                if txs.is_empty() {
                    continue;
                }
            }
            let last = txs.last().map(|t| t.idx).unwrap_or(from);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::BackupAppend));
            self.stats.deleg_backup_appends += 1;
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.backup_inflight = Some((req, last));
            d.backup_inflight_at = now.honour;
            d.backup_inflight_wait = now.wait;
            out.push(Action::Send {
                to: backup,
                msg: PeerMsg::DelegBackupAppend { req, gen, txs },
            });
        }
        let _ = now;
    }

    /// Phase 2b: the backup answered an append.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_backup_ack(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        acked: u64,
        sealed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.dl.by_req.contains_key(&req) {
            // A late answer (`LateReq`): what the backup holds, if it is
            // still the backup appended to.
            if sealed
                || self
                    .deleg_take_late(req, ReqKind::BackupAppend, gen, Some(from))
                    .is_none()
            {
                return;
            }
            let Some(d) = self.dl.mine.get_mut(&gen) else {
                return;
            };
            if d.backup != Some(from) || acked <= d.backup_acked {
                return;
            }
            d.backup_acked = acked;
            d.backup_sent_through = d.backup_sent_through.max(acked);
            self.complete_ready(now, replica, out);
            self.deleg_backup_stream(now, replica, out);
            return;
        }
        let Some((g, ReqKind::BackupAppend)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.backup != Some(from) {
            return;
        }
        self.stats.deleg_backup_acks += 1;
        let inflight = d.backup_inflight.take();
        if sealed {
            // The backup sealed the generation: the root is draining it
            // and will end it. Nothing more is acknowledged here; what
            // waited retries by rid (the root has it, or refuses it).
            tracing::warn!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                backup = from,
                "the backup sealed this delegation"
            );
            d.backup_sealed = true;
            d.backup = None;
            d.stopped = true;
            self.deleg_abort_stream_parks(now, gen, replica, out);
            return;
        }
        d.backup_failures = 0;
        d.backup_acked = d.backup_acked.max(acked);
        d.backup_after = None;
        let mut paced = false;
        if let Some((_, last)) = inflight {
            if acked < last {
                // Short: resend from what it holds.
                d.backup_sent_through = acked;
                // Unless the gap is below what this delegate still holds:
                // those rows are in the log (retired here), the backup
                // has not applied that segment yet, and a re-send now
                // is answered the same — a message loop at the round
                // trip (long-delegated-backup seed 70232: 22k appends).
                // Again on the stream tick; the backup catches up on the
                // log meanwhile.
                let first = replica
                    .delegate_txs_from(gen, acked + 1, 1)
                    .first()
                    .map(|t| t.idx);
                if first.is_some_and(|f| f > acked + 1) {
                    let wait = self.cfg.delegation_stream_tick_ms.max(1) * 4;
                    let d = self.dl.mine.get_mut(&gen).expect("present");
                    d.backup_after = Some(now.wait.plus(wait));
                    paced = true;
                    tracing::debug!(
                        node = self.cfg.node_id,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        backup = from,
                        acked,
                        first,
                        "delegate backup behind the log; appending again on the tick"
                    );
                }
            } else {
                d.backup_sent_through = d.backup_sent_through.max(last);
            }
        }
        if paced {
            self.arm_stream_tick(now, out);
        }
        self.deleg_group_maintain(now, replica, out);
        self.complete_ready(now, replica, out);
        self.deleg_backup_stream(now, replica, out);
    }

    /// Phase 2b: what waited for the backup of `gen` cannot be
    /// acknowledged here any more: the requester (or this node's own
    /// client) retries by rid, which the root answers from the drain.
    fn deleg_abort_stream_parks(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let ids = self.stream_parks_of(gen);
        for id in ids {
            self.abort_park_busy(now, id, replica, out);
        }
    }

    /// Plan 43 C2 (D7): generation `gen` ended here with the cut `through`:
    /// what waited for a row past it will never be in the log (the rows
    /// past the cut are void: replayed by rid, or dropped), so the
    /// acknowledgement is not given — the requester, or this node's own
    /// client, retries by rid at the generation's new owner. Parks of rows
    /// at or below the cut stay: they release through the archive
    /// (`deleg_stream_durable`).
    fn deleg_abort_stream_parks_past(
        &mut self,
        now: Now,
        gen: u64,
        through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let ids = self.stream_parks_past(gen, through);
        for id in ids {
            self.abort_park_busy(now, id, replica, out);
        }
    }

    // ----------------------------------------------------- the backup

    /// The highest index of `gen`'s stream below this node's own unshipped
    /// append of it, when it is the generation's root as well as its backup
    /// (`u64::MAX`: no such append). What a backup's acknowledgement counts
    /// as "in the log" (`Meta::deleg_backup_acked`) stops there: a row the
    /// root appended to its journal and nothing has shipped is no copy the
    /// backup's tail holds, and an acknowledgement of an earlier append
    /// that claimed it let the delegate answer before the backup's own
    /// append of it arrived (I1, `delegated-holder-cut` seed 802 inside a
    /// takeover gate, plan 46 M0.3 S5). Not only while this node is the
    /// generation's root: a deposed root (its `GenState` dropped by
    /// `deleg_on_lease_gone`) keeps its unshipped appends in the journal,
    /// and in `log_stream_idx`, until its recovery strands them, and the
    /// delegate may still send it the backup appends above them.
    fn root_log_floor(&self, gen: u64, replica: &dyn Replica) -> u64 {
        // The tip only rises, and a strand deletes journal rows without
        // advancing the watermark: a recovered, deposed holder would scan on
        // every backup append for good. Every unshipped append has its last
        // row in the journal above the watermark, so the journal's highest
        // seq bounds the tip exactly. A journal that cannot be read bounds
        // nothing.
        let tip = replica
            .holder_append_tip()
            .min(replica.max_journal_seq().unwrap_or(u64::MAX));
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        if tip == 0 || tip <= shipped {
            return u64::MAX;
        }
        let Ok(appends) = replica.holder_appends(gen) else {
            return 0;
        };
        appends
            .iter()
            .find(|(_, last)| *last > shipped && !self.ack.shipped_rows.contains(last))
            .map_or(u64::MAX, |(idx, _)| idx.saturating_sub(1))
    }

    /// Phase 2b: a delegate's append (this node is its backup).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_backup_append(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        // A seal flag that cannot be read is not "not sealed": after a
        // restart only the store knows the seal, and an append taken over it
        // is acknowledged to the old delegate as held by its group while the
        // end of the generation at the sealed tail never carries it. No
        // answer; the delegate sends again (plan 43 C2e).
        let sealed = if self.dl.sealed.contains(&gen) {
            true
        } else {
            match replica.deleg_backup_sealed(gen) {
                Some(sealed) => sealed,
                None => return,
            }
        };
        let mut acked = self.dl.backing.get(&gen).map(|(_, a)| *a).unwrap_or(0);
        if !sealed && !txs.is_empty() {
            // Not persisted, or not read back: no answer (it used to answer
            // `acked: 0` and reset what this node backs of the generation).
            let floor = self.root_log_floor(gen, replica);
            let Some(a) = replica.deleg_backup_append(gen, &txs, floor) else {
                return;
            };
            acked = a;
            self.stats.deleg_backup_persisted += txs.len() as u64;
        }
        if !sealed {
            self.dl.backing.insert(gen, (from, acked));
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegBackupAck {
                req,
                gen,
                acked,
                sealed,
            },
        });
    }

    /// Phase 2b: the root asks this backup to seal `gen` and hand over
    /// its tail (the delegate is silent).
    pub(crate) fn on_deleg_seal(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        // A store that cannot be read is not a store that holds nothing:
        // "I hold nothing" (`sealed: false`) ends the generation at the
        // root's cursor, and an empty tail drains to nothing. Neither is
        // said; no answer, and the root asks again (plan 43 C2e).
        let backing = if self.dl.backing.contains_key(&gen) {
            true
        } else {
            match replica.deleg_backup_holds_any(gen) {
                Some(held) => held,
                None => return,
            }
        };
        // Plan 43 C2: sealed whether or not it holds anything, persisted
        // (and synced) before it is answered. "I hold nothing" (`sealed:
        // false`) ends the generation at the root's cursor, so this node
        // must not take the old delegate's appends afterwards — a batch
        // still in flight, or one from a delegate that outlived its grant —
        // and acknowledge rows the log will never carry. Not persisted: no
        // answer; the root asks again while the record names this node.
        if !replica.deleg_backup_seal(gen) {
            return;
        }
        self.dl.sealed.insert(gen);
        // The previous roots this node itself still owed (it was deposed and
        // succeeded before its own predecessor answered): the root seals them
        // too (c3-spec D6, transitive). Unreadable: no answer.
        let Some(prev_roots) = replica.deleg_prev_roots(gen) else {
            return;
        };
        let txs = if backing {
            let Some(tail) = replica.deleg_backup_tail(gen) else {
                return;
            };
            self.stats.deleg_backup_seals += 1;
            tail
        } else {
            Vec::new()
        };
        tracing::info!(
            node = self.cfg.node_id,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            root = from,
            backing,
            rows = txs.len(),
            "sealed a delegation for the root"
        );
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegSealed {
                req,
                gen,
                sealed: backing,
                txs,
                prev_roots,
            },
        });
    }

    /// Phase 2b: a member's answer to a seal: append its tail, end the
    /// generation, delegate the directory to the backup.
    ///
    /// Plan 43 C2e (§3.7, D3, D8): a *strict* member (of the latest
    /// `DelegateNodeGroup` version) ends the generation once its tail is drained to
    /// its end — the rows acknowledged through the group are on every
    /// member, so one member's tail is the group's — and until then the
    /// generation stays `Sealing` ([`Self::deleg_end_sealed`]). A
    /// best-effort member (named only by an older record) is drained when it
    /// answers in time and never holds anything.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_sealed(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        sealed: bool,
        txs: Vec<constellation_meta::DelegateTx>,
        prev_roots: Vec<NodeId>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if matches!(self.dl.by_req.get(&req), Some((g, ReqKind::SealPrevRoot)) if *g == gen) {
            // (Kept in `by_req` until the set is settled: a late answer to an
            // earlier request is as good.)
            self.on_prev_root_sealed(now, from, req, gen, sealed, txs, prev_roots, replica, out);
            return;
        }
        let Some((g, ReqKind::Seal)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(gs) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        let Some(&(member, asked_strict)) = gs.seal_reqs.get(&req) else {
            return;
        };
        if asked_strict {
            gs.seal_reqs.remove(&req);
        }
        if gs.ended || member != from {
            return;
        }
        gs.member_notes.entry(from).or_default().answered = Some(now.wait);
        // Strict only if it still is: an answer to a request of an earlier
        // seal (the transport failed and the generation went on, a group
        // record named other backups since) is a best-effort member's. It
        // ends nothing: the rows acknowledged through the group now in
        // force are on that group's members, not necessarily on this one.
        let strict =
            asked_strict && gs.recall == RecallPhase::Sealing && gs.group.strict().contains(&from);
        if !strict {
            if gs.recall != RecallPhase::Sealing {
                return;
            }
            // Its rows only add to what the root appends; a gap or a
            // missing dependency stops the drain and nothing waits.
            if sealed && self.root_ends_recall(now, gen) {
                let _ = self.deleg_drain_tail(now, gen, &txs, true, replica, out);
            }
            return;
        }
        if !sealed {
            // The member holds nothing of the generation (and is sealed
            // against later appends): no row was acknowledged through it,
            // so the root's cursor covers what the group acknowledged. The
            // spec asks to keep waiting; a backup chosen before the
            // delegate appended anything holds nothing, and waiting for it
            // would wedge an idle generation (C2c).
            self.deleg_end_sealed(now, gen, from, true, replica, out);
            return;
        }
        // Where the tail runs to without a gap from the cursor: what the
        // drain must reach before the generation may end.
        let end = txs.iter().fold(
            gs.cursor,
            |end, tx| {
                if tx.idx == end + 1 {
                    tx.idx
                } else {
                    end
                }
            },
        );
        if gs.sealed_tail.as_ref().is_none_or(|t| end > t.end) {
            gs.sealed_tail = Some(std::sync::Arc::new(SealedTail { from, txs, end }));
        }
        self.deleg_end_sealed(now, gen, from, true, replica, out);
    }

    /// Plan 43 C2e (D8): end `gen` if the strict member `from` answered
    /// ("holds nothing", or its sealed tail, kept in `sealed_tail`, drained
    /// to its end) and this root may end the recall. Otherwise the
    /// generation stays `Sealing`: rows past the drain's break (a
    /// dependency not here yet, a row of a newer tenure, an append error)
    /// — or the whole tail, when this root may not end the recall now — may
    /// have been answered through the backup, and the recall that ends the
    /// generation drops them on the delegate rather than replaying them.
    /// The drain is retried after every event ([`Self::deleg_after_event`])
    /// and at the window's end; the seal is not asked again.
    fn deleg_end_sealed(
        &mut self,
        now: Now,
        gen: u64,
        from: NodeId,
        first: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(gs) = self.dl.gens.get(&gen) else {
            return;
        };
        if gs.ended {
            return;
        }
        let (dirs, non_stealable) = (gs.dirs.clone(), gs.non_stealable());
        let (release_seal, release_cut, cut_waived) =
            (gs.release_seal, gs.release_cut, gs.release_cut_waived);
        let tail = gs.sealed_tail.clone();
        let ends_recall = self.root_ends_recall(now, gen);
        if release_seal {
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                g.release_group_answered = true;
            }
        }
        let mut sealed = false;
        if let Some(tail) = tail {
            sealed = true;
            let mut drained_to = self.dl.gens[&gen].cursor;
            let mut lost = false;
            if ends_recall {
                (drained_to, lost) =
                    self.deleg_drain_tail(now, gen, &tail.txs, first, replica, out);
            }
            // The drain stopped at a row whose dependency was lost with its
            // generation: no row from there on was acknowledged (its
            // acknowledgement waits for the row's commit, which a lost
            // dependency never gets), so the generation ends before it; the rows strand and are
            // replayed by rid (or dropped, unanswered) on the delegate.
            if lost && drained_to < tail.end {
                tracing::info!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    cursor = drained_to,
                    tail_end = tail.end,
                    "a sealed tail's row depends on one lost with its generation: \
                     the generation ends before it"
                );
            }
            // A tail the root's cursor already covers has nothing to append,
            // so a generation the delegate released (plan 46 M1.2 step 14)
            // does not wait for a root that cannot append yet (a takeover
            // gate waiting out the read-delegation quarantine): ending it
            // appends nothing, as an unsealed release ends at once, and the
            // delegate's parked ops would otherwise wait the quarantine out.
            let nothing_to_append = release_seal && tail.end <= drained_to;
            if (!ends_recall && !nothing_to_append) || (drained_to < tail.end && !lost) {
                if first {
                    self.stats.deleg_seal_waits += 1;
                }
                tracing::debug!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    backup = from,
                    cursor = drained_to,
                    tail_end = tail.end,
                    ends_recall,
                    "a sealed delegation's tail is not drained to its end: the generation is not ended yet"
                );
                return;
            }
        }
        // Plan 46 M1.2 step 14 (L2b): a generation the delegate released
        // ends once the root's cursor has reached the release's cut as well:
        // "holds nothing" with rows still in flight on the stream would drop
        // them. The rows come with the stream (or a tail), and the end is
        // tried again after every event. A window later (the retry timer) the
        // wait is waived: the rows the group does not hold were not
        // acknowledged, and a delegate that let go of its stream sends no
        // more.
        if release_seal {
            let cursor = self.dl.gens.get(&gen).map_or(0, |g| g.cursor);
            // The answer is in hand from now on: the end is tried again after
            // every event (rows come, previous roots settle), and the wait
            // for the stream is waived a whole window from here.
            let fresh = self.dl.gens.get_mut(&gen).is_some_and(|g| {
                g.release_answered = Some(from);
                let fresh = g.release_answered_at.is_none();
                g.release_answered_at.get_or_insert(now.wait);
                fresh
            });
            if cursor < release_cut.unwrap_or(0) && !cut_waived {
                if fresh {
                    let at = now.wait.plus(self.release_stream_window());
                    self.arm_release_seal_timer(gen, at, out);
                }
                if first {
                    self.stats.deleg_seal_waits += 1;
                }
                tracing::debug!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    member = from,
                    cursor,
                    cut = ?release_cut,
                    "a released delegation's group has answered but the stream is not at the release's cut: the generation is not ended yet"
                );
                return;
            }
        }
        self.end_generation(now, gen, None, replica, out);
        // (Not a generation the delegate released: the cause of the recall
        // stands, and the backup is no new delegate for it.)
        if sealed && !non_stealable && !release_seal {
            // Every subtree of the stream, to the backup: the first grant
            // opens its stream, the rest grow it.
            for (dir, kind) in dirs {
                match self.delegate_dir(now, dir, from, kind, replica, out) {
                    Ok(new_gen) => {
                        tracing::info!(
                            node = self.me(),
                            dir,
                            delegate = from,
                            gen = %constellation_meta::delegation::GenLabel::of(gen),
                            new_gen = %constellation_meta::delegation::GenLabel::of(new_gen),
                            "re-delegated to the backup"
                        );
                    }
                    Err(why) => {
                        tracing::debug!(
                            node = self.me(),
                            dir,
                            why,
                            "not re-delegated to the backup"
                        )
                    }
                }
            }
        }
    }

    /// Append the contiguous rows of a sealed tail (`txs`) that follow the
    /// generation's cursor and whose dependencies the root holds; returns the
    /// cursor, and whether the drain stopped at a row whose dependency was
    /// lost with its generation (`Replica::deps_lost`: the void rule would
    /// count it as met, and the row would land ahead of its cause, or
    /// without it). `count`: the stop reasons and the drain are counted (a
    /// retry of the same tail is not).
    fn deleg_drain_tail(
        &mut self,
        now: Now,
        gen: u64,
        txs: &[constellation_meta::DelegateTx],
        count: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> (u64, bool) {
        let Some(gs) = self.dl.gens.get(&gen) else {
            return (0, false);
        };
        let mut cursor = gs.cursor;
        let mut drained = 0u64;
        let mut lost = false;
        for tx in txs {
            if tx.idx <= cursor {
                continue;
            }
            if tx.idx != cursor + 1 {
                break;
            }
            if self.deps_from_a_newer_tenure(&tx.deps) {
                if count {
                    self.stats.deleg_append_newer_tenure += 1;
                }
                break;
            }
            if replica.deps_lost(&tx.deps) {
                if count {
                    self.stats.deleg_append_deps_lost += 1;
                }
                lost = true;
                break;
            }
            if !replica.reaches_streams(&tx.deps) {
                // A sealed tail can carry a row that followed a row of the
                // dead delegate's other generation the root does not hold
                // yet (the backup acknowledged the appends, nothing waited
                // for the root): the drain waits for that generation's
                // end, or its rows. Not the live stream's causal cut, which
                // never sends such a row (`acked_here`).
                if count {
                    self.stats.deleg_drain_deps_waits += 1;
                }
                break;
            }
            let applied = replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps);
            self.stats.deleg_append_skipped += replica.take_deleg_append_skipped();
            self.stats.deleg_append_uncompleted += replica.take_deleg_append_uncompleted();
            match applied {
                Ok(_) => {
                    replica.note_foreign_executed(&tx.records);
                    cursor = tx.idx;
                    drained += 1;
                    self.dl.drained_rows.push((gen, tx.idx));
                }
                Err(error) => {
                    tracing::warn!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), idx = tx.idx, %error, "could not append a sealed transaction");
                    break;
                }
            }
        }
        if drained == 0 {
            return (cursor, lost);
        }
        if let Some(gs) = self.dl.gens.get_mut(&gen) {
            gs.cursor = cursor;
        }
        self.stats.deleg_sealed_drained += drained;
        self.answer_awaiting_log(now, replica, out);
        self.release_exec_parks_completed(now, replica, out);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            drained,
            cursor,
            "drained a sealed delegation"
        );
        (cursor, lost)
    }

    /// The root learns the generations its table names when the table
    /// changes and when it acquires the lease — usable then. A lease that
    /// becomes usable only later (acquired inside its expiry margin, its
    /// first renewal making it usable) learned nothing: the root executed
    /// ops under a live delegation as if it were its own, and refused the
    /// delegate's stream for good, its acknowledged writes never appended
    /// (sim `long-delegated-late-answers` seed 45: a takeover whose lease
    /// PUT was slow). So the table is synced again whenever this node is
    /// a usable root and the last sync did not find it one — before the
    /// event, after it, and before the root executes anything itself.
    pub(crate) fn deleg_root_sync_due(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // (A level that changed either way: a gate installed over a tenure
        // that was usable syncs again at `Gated`, and the open at `Usable`.)
        let level = self.root_deleg_level(now);
        if self.cfg.delegation && level != RootSync::None && level != self.dl.root_synced {
            self.stats.deleg_root_resyncs += 1;
            self.delegation_sync(now, replica, out);
        }
    }

    /// Retry every parked op whose wait is over, then stream and renew.
    pub(crate) fn deleg_after_event(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // ADR-55: the restart's wait for the claim is over when the claim
        // landed (the lease is held and its gate is open: the claim ended the
        // streams of its own), or when this process holds no stream any
        // more; a later gate, as a designee of a handoff, executes as it
        // always did.
        if self.dl.restarted_holder
            && (self.dl.mine.is_empty() || (self.lease.held.is_some() && self.lease.gate.is_none()))
        {
            self.dl.restarted_holder = false;
        }
        // Phase 2b: the placement runs on the root, delegations or not.
        if self.root_usable(now) {
            self.place_arm(now, out);
            // M12: a re-delegation held back (the op still parked, the
            // delegate away) is tried again on the next event.
            if self.dl.gens.values().any(|g| !g.redelegate_dirs.is_empty()) {
                self.deleg_redelegate_after_cross(now, replica, out);
            }
        }
        // ADR-44: a recall the gate drained while it was not shippable (a
        // gate paused past its lease: no further batch or answer comes to
        // end it) is ended now that it is shippable again. One scan per
        // event, in a gate only.
        if self.lease.gate.is_some()
            && self.dl.gens.values().any(|g| {
                g.gate_recalled
                    && g.inherited
                    && !g.ended
                    && matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th)
            })
        {
            self.root_progress(now, replica, out);
        }
        self.deleg_forget_ended(now, replica);
        self.own_honour_progress(now, replica, out);
        self.deleg_leave_progress(now, replica, out);
        self.handover_progress(now, replica, out);
        self.sticky_release_progress(now, replica, out);
        self.own_release_progress(now, replica, out);
        self.abandon_progress(now, replica, out);
        self.deleg_restart_void_round(now, replica, out);
        // Plan 43 C2e (D8): a strict member's sealed tail that the drain
        // stopped short of (a dependency the log brings, a root that could
        // not end the recall) is drained again; the generation ends once the
        // cursor reaches the tail's end.
        let stalled: Vec<(u64, NodeId)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended)
            .filter_map(|(gen, g)| {
                g.sealed_tail
                    .as_ref()
                    .map(|t| t.from)
                    .or(g.release_answered)
                    .map(|from| (*gen, from))
            })
            .collect();
        for (gen, from) in stalled {
            self.deleg_end_sealed(now, gen, from, false, replica, out);
        }
        // Plan 46 M1.2 step 7 (D6): the previous roots' seals.
        if self
            .dl
            .gens
            .values()
            .any(|g| !g.ended && !g.prev_roots.is_empty())
        {
            self.deleg_prev_roots_step(now, replica, out);
        }
        if self.dl.mine.is_empty() && !self.deleg_known() {
            return;
        }
        // Phase 2b: a new root (a failover) gets every unretired
        // transaction again; it deduplicates by cursor and rid. What the
        // log carries counts as acknowledged, as at install: the new
        // root replayed the log before it serves, and those rows are
        // retired here — no batch can carry them, so no answer would
        // ever acknowledge them. From 0, a generation whose first
        // unretired row depends on another generation of this node waits
        // for that one's acknowledgement (`deleg_stream`), and two that
        // depend on each other never send at all (git-under-flock-faults
        // fault #14: rows 19.. of gen 4 behind (6, 2), row 2 of gen 6
        // behind (4, 18); every op parked on the new root for 120 s).
        let root = self.holder_root();
        let mut stale_reqs = Vec::new();
        let mut renew_again = Vec::new();
        let rounds: BTreeMap<u64, u64> = self
            .dl
            .mine
            .values()
            .filter(|d| d.last_root != root)
            .map(|d| d.gen)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|g| (g, self.deleg_new_round()))
            .collect();
        for d in self.dl.mine.values_mut() {
            if let Some(round) = rounds.get(&d.gen) {
                // The old root's answers — to the batch in flight, to one
                // given up on, to a renewal — must not count for the new
                // one, nor for the old root if it comes back
                // (`DelegateState::stream_round`). Every change counts, a
                // root lost from view and found again too: the gap may
                // hide another tenure (this node's own: `root_node`
                // leaves it out), and an answer counts by root and round
                // alone. The batch in flight is answered in the old round,
                // so stale: the next one goes now, not after its timeout.
                d.stream_round = *round;
                if let Some((req, _)) = d.inflight.take() {
                    stale_reqs.push(req);
                    d.stream_after = None;
                    d.stream_backoff_ms = 0;
                    d.refusal_backoff_ms = 0;
                }
                // So is the renewal in flight: its answer is dropped as
                // stale (`on_deleg_renewed`), and while `renew` named it
                // no renewal went out until it timed out
                // (`deleg_expire_inflight`, the forward timeout). A
                // delegate whose cached root was cleared and found again
                // right after a re-delegation to itself sat out that
                // timeout with its new generation never renewed, answering
                // every lock renewal in the subtree `NotOwner { 0 }` while
                // the holders' windows ran out (sim
                // `locks-delegated-production-ttl-heavy`, R2-3b). It goes
                // again now, in the new round.
                if let Some((req, _)) = d.renew.take() {
                    stale_reqs.push(req);
                    if !d.stopped {
                        renew_again.push(d.gen);
                    }
                }
                // (So is an ask for the commit in flight: it was made in the
                // old round, and the next goes in the new one.)
                if let Some((req, _)) = d.ask.take() {
                    stale_reqs.push(req);
                }
                d.poke_backoff_ms = 0;
                d.poke_after = None;
                d.archive_bound = false;
                // (A root found after a spell with none in view is a failover
                // when it is another node than the last one: this node held
                // the lease itself meanwhile, or the holder was unknown. The
                // old root's acknowledgements say nothing of the new one's
                // cursor, and a batch past its gap is never taken.)
                if root.is_some()
                    && (d.last_root.is_some() || d.known_root.is_some_and(|k| Some(k) != root))
                {
                    d.streamed_through = replica.log_stream_idx(d.gen);
                    d.root_committed = d.streamed_through;
                    d.refused = false;
                    d.stream_after = None;
                    d.stream_backoff_ms = 0;
                    d.refusal_backoff_ms = 0;
                    self.stats.deleg_restreams += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        gen = %constellation_meta::delegation::GenLabel::of(d.gen),
                        ?root,
                        "re-streaming to a new root"
                    );
                }
                d.last_root = root;
                if root.is_some() {
                    d.known_root = root;
                }
            }
        }
        for req in stale_reqs {
            self.dl.by_req.remove(&req);
        }
        for gen in renew_again {
            self.deleg_renew_now(now, gen, out);
        }
        // Parked delegate ops.
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let Some(d) = self.dl.mine.get(&gen) else {
                continue;
            };
            if d.parked.is_empty() {
                continue;
            }
            let honoured = self.stream_honoured(now, gen);
            if !honoured {
                continue;
            }
            // ADR-46: only settles run inside the restart quarantine.
            let quarantined = self.deleg_restart_quarantined(now, gen);
            let candidates: Vec<(usize, NodeId, MutateOp, bool)> = d
                .parked
                .iter()
                .enumerate()
                .filter(|(_, p)| {
                    p.settle
                        || (!quarantined
                            && self.stream_deps_reached(gen, &p.deps, replica)
                            && !self.gated_by_queue(p.from, p.rid, &p.op, replica)
                            && !(p.held
                                && self.sticky_holds(
                                    now,
                                    gen,
                                    &super::holder::keys_of_op_in(&p.op, replica),
                                    replica,
                                )))
                })
                .map(|(i, p)| (i, p.from, p.op.clone(), p.settle))
                .collect();
            let mut ready = Vec::new();
            for (i, from, op, settle) in candidates {
                if settle || !self.deleg_read_recall_pending(now, from, &op, replica, out) {
                    ready.push(i);
                }
            }
            if ready.is_empty() {
                continue;
            }
            let mut taken = Vec::new();
            let d = self.dl.mine.get_mut(&gen).expect("present");
            for i in ready.into_iter().rev() {
                taken.push(d.parked.remove(i));
            }
            taken.reverse();
            for p in taken {
                if p.from == 0 && !self.clients.contains_key(&p.rid) {
                    continue;
                }
                // Late-landing S2b: an attempt that waited longer than the
                // bound is not executed (its requester sends it again if it
                // is still waiting; that arrival executes).
                if p.from != 0
                    && !p.settle
                    && now.wait.since(p.arrived) >= self.dl_t().inbox_read_fresh as i64
                {
                    self.stats.stale_parks_dropped += 1;
                    if let Some(req) = p.req {
                        out.push(Action::Send {
                            to: p.from,
                            msg: PeerMsg::MutateReply {
                                req,
                                outcome: MutateOutcome::Held {
                                    retry_ms: self.cfg.recall_hold_ms.max(50),
                                    outwait_ms: None,
                                    lost_chunk: false,
                                },
                                base: None,
                                position: Position::ZERO,
                                gen,
                                own_chunks: OwnChunks::None,
                                own_rows: None,
                                committed: false,
                                non_stealable: false,
                            },
                        });
                    }
                    continue;
                }
                self.delegate_execute_now(
                    now,
                    gen,
                    p.from,
                    p.req,
                    p.rid,
                    &p.op,
                    p.deps,
                    (&p.tag, p.settle, p.fence),
                    replica,
                    out,
                );
            }
        }
        self.deleg_stream(now, replica, out);
        // Phase 2b: the delegate's backup.
        self.deleg_backup_select(now, out);
        self.deleg_group_maintain(now, replica, out);
        self.deleg_backup_stream(now, replica, out);
        // Root-side parked executions waiting for `deps` (and, phase 2b,
        // a delegate's acknowledgements waiting for a segment).
        if self.has_deps_parks() || self.has_stream_parks() {
            self.complete_ready(now, replica, out);
        }
    }

    fn arm_stream_tick(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.dl.stream_timer.is_none() {
            let t = self.set_timer_wait(
                now.wait.plus(self.cfg.delegation_stream_tick_ms),
                Timer::DelegStream,
                out,
            );
            self.dl.stream_timer = Some(t);
        }
    }

    pub(crate) fn on_deleg_stream_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.stream_timer = None;
        self.deleg_stream(now, replica, out);
    }

    /// A fresh stream round ([`DelegateState::stream_round`]): unique
    /// within this incarnation (an op id, below 2^40) and across
    /// incarnations (the incarnation in the high 24 bits: two
    /// incarnations share them only 2^24 restarts apart).
    fn deleg_new_round(&mut self) -> u64 {
        const OP_BITS: u32 = 40;
        // (An op id carries the incarnation in its high bits already: the low
        // bits are the counter.)
        let op = self.op_id().0 & ((1 << OP_BITS) - 1);
        debug_assert!(
            u64::from(self.cfg.incarnation) < 1 << (64 - OP_BITS),
            "incarnation {} overflows a round",
            self.cfg.incarnation
        );
        (u64::from(self.cfg.incarnation) << OP_BITS) | (op & ((1 << OP_BITS) - 1))
    }

    /// Plan 41 R2: how long a renewal waits for the root before it is sent
    /// once more: the root's off-core answerer acts after its core has been
    /// stuck for `stuck_threshold_ms` (a quarter of a grant period), and a
    /// request that arrived before that went to the core.
    pub(crate) fn deleg_resend_ms(&self) -> i64 {
        self.dl_t().deleg_resend as i64
    }

    /// How long a stream batch, backup append or renewal waits for its
    /// answer before it is sent again (a partition drops it silently).
    pub(crate) fn deleg_request_timeout_ms(&self) -> u64 {
        self.dl_t().deleg_request_timeout
    }

    /// Stop waiting for the in-flight requests whose answers are
    /// overdue (a renewal's or backup append's answer is still used when
    /// it comes: [`LateReq`]; a stream acknowledgement always is, in its
    /// round); the generations whose renewal was dropped (re-sent by the
    /// caller). A stream batch is re-sent from the acknowledged cursor
    /// after a backoff, as after a transport failure: the root is slow,
    /// and the same batch every timeout only adds to what it is slow with.
    fn deleg_expire_inflight(&mut self, now: Now) -> Vec<u64> {
        let timeout = self.deleg_request_timeout_ms() as i64;
        let resend = self.deleg_resend_ms().min(timeout);
        let tick = self.cfg.delegation_stream_tick_ms.max(1);
        let root = self.holder_root();
        let mut late = Vec::new();
        let mut given_up = Vec::new();
        let mut asks_given_up = Vec::new();
        let mut renew_again = Vec::new();
        for d in self.dl.mine.values_mut() {
            // An ask for the commit not answered in time is given up: no
            // stream timeout, no back-off of the rows' stream, only its own.
            if let Some((req, sent)) = d.ask {
                if now.wait.since(sent) >= timeout {
                    d.ask = None;
                    asks_given_up.push((d.gen, req));
                }
            }
            if let Some((req, _)) = d.inflight {
                if now.wait.since(d.inflight_at) >= timeout {
                    given_up.push(req);
                    d.inflight = None;
                    d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
                    d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
                    self.stats.deleg_stream_timeouts += 1;
                }
            }
            if let Some((req, _)) = d.backup_inflight {
                if now.wait.since(d.backup_inflight_wait) >= timeout {
                    late.push((req, d.backup, d.backup_inflight_at));
                    d.backup_inflight = None;
                    self.stats.deleg_stream_timeouts += 1;
                }
            }
            if let Some((req, sent)) = d.renew {
                let age = now.wait.since(d.renew_sent_wait);
                // Plan 41 R2: a renewal the root has not answered within what
                // its off-core answerer needs to see its core stuck
                // ([`Core::deleg_resend_ms`]) is sent once more, the first one
                // kept for a late answer. A request that reached the root
                // just before its core got stuck was queued to that core
                // (nothing was stuck yet) and waits for it; only a request
                // made after the stall began is answered off the core.
                let early = !d.renew_resent && !d.stopped && !d.non_stealable && age >= resend;
                if age >= timeout || early {
                    d.renew_resend_next = early;
                    late.push((req, root, sent));
                    d.renew = None;
                    if d.renew_timer.is_none() && !d.stopped {
                        renew_again.push(d.gen);
                    }
                }
            }
        }
        self.dl
            .late
            .retain(|_, l| now.honour.since(l.sent) < LATE_ANSWER_MS);
        for req in given_up {
            self.dl.by_req.remove(&req);
        }
        for (gen, req) in asks_given_up {
            self.dl.by_req.remove(&req);
            self.deleg_poke_backoff(now, gen);
        }
        for (req, to, sent) in late {
            if let Some((gen, kind)) = self.dl.by_req.remove(&req) {
                self.dl.late.insert(
                    req,
                    LateReq {
                        gen,
                        kind,
                        to,
                        sent,
                        off_core: false,
                    },
                );
            }
        }
        renew_again
    }

    /// A delegate that knows no root reads the lease for one, at most once
    /// a stream tick: what retries (the stream tick, the renewal timer)
    /// comes back at that pace. The streaming pass runs after every event,
    /// the lease read's answer included, and a read issued from each was a
    /// chain of reads that never ended while no answer named a root — one
    /// more chain per event: a delegate that took the root lease over (its
    /// own lease names it, the takeover's gate waits) read the lease
    /// without pause, its reads in flight growing past 250,000 in 11
    /// simulated seconds (long-delegated-backup seed 1682).
    fn deleg_learn_root(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.dl.root_read_after.is_some_and(|t| now.wait < t) {
            return;
        }
        self.dl.root_read_after = Some(now.wait.plus(self.cfg.delegation_stream_tick_ms.max(1)));
        self.issue_s3(
            crate::action::S3Op::LeaseGet,
            super::S3For::RefreshHolder,
            out,
        );
    }

    /// The late answer to `req` ([`LateReq`]), if it is one, of `kind`
    /// for `gen` from `from`.
    fn deleg_take_late(
        &mut self,
        req: OpId,
        kind: ReqKind,
        gen: u64,
        from: Option<NodeId>,
    ) -> Option<LateReq> {
        let l = *self.dl.late.get(&req)?;
        if l.kind != kind || l.gen != gen || (from.is_some() && l.to != from) {
            return None;
        }
        self.dl.late.remove(&req)
    }

    /// `Action::SyncDelegStream { gen, through }` finished: a designee's
    /// rows through `through` are on its disk and may stream; a failed
    /// sync backs the stream off and is asked again on the tick.
    pub(crate) fn on_deleg_stream_synced(
        &mut self,
        now: Now,
        gen: u64,
        through: u64,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.sync_inflight != Some(through) {
            return;
        }
        d.sync_inflight = None;
        if ok {
            d.synced_through = d.synced_through.max(through);
        } else {
            self.stats.deleg_designee_sync_failures += 1;
            tracing::warn!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                through,
                "a designee's journal sync failed: its stream waits and asks again"
            );
            let tick = self.cfg.delegation_stream_tick_ms.max(1);
            d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
            d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
            self.arm_stream_tick(now, out);
            return;
        }
        self.deleg_stream(now, replica, out);
    }

    /// Row `idx` of `gen` depends on a row its generation ended without
    /// (`Replica::deps_lost`): the generation stops before it
    /// (`DelegateState::lost_from`, see `deleg_stream`).
    fn deleg_note_lost(&mut self, gen: u64, idx: u64) {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.lost_from.is_some_and(|l| l <= idx) {
            return;
        }
        d.lost_from = Some(idx);
        d.stopped = true;
        self.stats.deleg_lost_deps_stopped += 1;
        tracing::warn!(
            node = self.cfg.node_id,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            idx,
            "a row depends on one its generation ended without: \
             the generation stops before it, the row is replayed by rid"
        );
    }

    /// The first row of `gen` not yet streamed whose dependency was lost
    /// with another generation: a recall answers before it. A row read for
    /// a batch had its lost dependency seen then (`deleg_note_lost_in`);
    /// only the rows no batch has read yet (a batch in flight, past the
    /// window) are read from the journal.
    fn deleg_find_lost(&mut self, gen: u64, replica: &dyn Replica) -> Option<u64> {
        let d = self.dl.mine.get(&gen)?;
        if let Some(l) = d.lost_from {
            return Some(l);
        }
        let from = d.streamed_through.max(replica.log_stream_idx(gen)) + 1;
        let idx = replica
            .delegate_deps_from(gen, from)?
            .into_iter()
            .find(|(_, deps)| replica.deps_lost(deps))
            .map(|(idx, _)| idx)?;
        self.deleg_note_lost(gen, idx);
        Some(idx)
    }

    /// Whether `d`'s group counts the root's copy: no backup recorded, or none
    /// in the applied or the requested version (`deleg_group_through`).
    fn deleg_counts_root(d: &DelegateState) -> bool {
        d.group_backups.is_empty()
            || d.group_requested
                .as_deref()
                .is_some_and(<[NodeId]>::is_empty)
    }

    /// Plan 43 Q16 (ADR-41): the generations whose commit an answer parked
    /// here waits on, each with since when the oldest such answer has waited:
    /// a generation whose root has appended rows it has not told this delegate
    /// it committed (`root_committed < streamed_through`; a stopped or lapsed
    /// delegate is not honoured, what it holds is the holder's to settle),
    /// and one of
    /// - an answer of its own on a row above `root_committed`, its group
    ///   counting the root's copy;
    /// - an answer of its own while the recorded backup is gone and the
    ///   change of the group waits for the root's commit to cover what was
    ///   answered (`deleg_group_maintain`).
    ///
    /// Empty (without a scan of the parks) while no generation has appended
    /// rows left uncommitted.
    ///
    /// A stopped generation is not asked, so a restarted root whose delegate
    /// generations are stopped does not re-adopt its lease on its own. Its
    /// parked own answers are settled at their first client deadline
    /// (`client_deadline`, 42bcf265), and that settle is the demand that
    /// re-adopts a restarted root's lease or claims a released one; the op is
    /// answered from the log, after about one deadline. Measured shapes:
    /// `long-delegated` seed 1576 (slow lease reads) and `delegated-epoch`
    /// seed 1355. A proactive re-claim was evaluated and dropped: latency
    /// only, no invariant at stake.
    fn deleg_awaited_commits(&self, replica: &dyn Replica) -> BTreeMap<u64, WaitMs> {
        let open =
            |d: &DelegateState| !d.stopped && !d.lapsed && d.root_committed < d.streamed_through;
        let mut awaited: BTreeMap<u64, WaitMs> = BTreeMap::new();
        if !self.dl.mine.values().any(open) {
            return awaited;
        }
        let mut note = |gen: u64, since: WaitMs| {
            let e = awaited.entry(gen).or_insert(since);
            *e = (*e).min(since);
        };
        // The log's index of a generation, read from the store at most once
        // per call, and only when the cheap tests in memory leave it open
        // (this runs on every event: a read per dependency row per park cost
        // a loaded delegate most of its core thread once, `StreamCache`).
        let mut logs: BTreeMap<u64, u64> = BTreeMap::new();
        let mut log_of = |g: u64| *logs.entry(g).or_insert_with(|| replica.log_stream_idx(g));
        for (gen, idx, since) in self.stream_parks() {
            let Some(d) = self.dl.mine.get(&gen) else {
                continue;
            };
            if open(d) {
                if Self::deleg_counts_root(d) && idx > d.root_committed && idx <= d.streamed_through
                {
                    note(gen, since);
                }
                if d.group_requested.is_none()
                    && d.backup.is_none()
                    && !d.group_backups.is_empty()
                    && d.acked_max > d.root_committed
                    && log_of(gen) < d.acked_max
                {
                    note(gen, since);
                }
            }
        }
        awaited
    }

    /// Plan 43 Q16 (ADR-41): ask the root for its commit of `gen`'s stream
    /// with an empty batch, once an answer has waited on it for a stream
    /// tick (`since`, from [`Self::deleg_awaited_commits`]), and again every
    /// tick while it waits. The root's own push (`root_stream_push`) comes
    /// first on every healthy path (one round trip after the batch's
    /// acknowledgement), and is the only word the root sends unasked: a push
    /// that was lost, one a root restarted since the batch (its generation's
    /// `last_batch` is empty) never sent, and one on the round before a
    /// change of this delegate's (dropped as stale) leave a quiet delegate
    /// with nothing else to send, and its answer parked until its deadline
    /// and the settle. The root answers an empty batch with its cursor and
    /// its commit (`on_delegate_stream`), on this delegate's round, and
    /// counts the generation's last batch from it, so its pushes go on.
    ///
    /// The ask is tracked apart from the batch in flight
    /// (`DelegateState::ask`): a row executed meanwhile streams at once, and
    /// the ask's answer, failure or timeout touches nothing of a batch of
    /// rows. A refusal, a failure or a timeout of the ask backs the asking
    /// off as the stream's back-off does (`poke_backoff_ms`), and nothing
    /// else.
    ///
    /// A stopped generation is never poked, even for a root restarted since
    /// its batch: its parked own answers are settled at their first client
    /// deadline (`client_deadline`, 42bcf265), which supplies the demand that
    /// re-adopts the root's lease (see [`Self::deleg_awaited_commits`]).
    fn deleg_poke_root(
        &mut self,
        now: Now,
        gen: u64,
        root: NodeId,
        since: WaitMs,
        out: &mut Vec<Action>,
    ) {
        let tick = self.cfg.delegation_stream_tick_ms.max(1);
        let ship = self.deleg_ship_interval_ms();
        let d = self.dl.mine.get_mut(&gen).expect("present");
        // An archive-bound commit moves when the root ships, and the root's
        // push says so: the ask is only the fallback for a push that was
        // lost, at the ship interval, not a tick.
        let archive_bound = d.archive_bound;
        let gap = if archive_bound { ship } else { tick };
        if d.ask.is_some()
            || d.poke_after.is_some_and(|t| now.wait < t)
            || now.wait.since(since) < gap as i64
        {
            return;
        }
        d.poke_after = Some(now.wait.plus(gap));
        let round = d.stream_round;
        let req = self.op_id();
        self.dl.by_req.insert(req, (gen, ReqKind::Poke));
        self.stats.deleg_root_pokes += 1;
        self.stats.deleg_root_pokes_archive_bound += u64::from(archive_bound);
        self.dl.mine.get_mut(&gen).expect("present").ask = Some((req, now.wait));
        out.push(Action::Send {
            to: root,
            msg: PeerMsg::DelegateStream {
                req,
                gen,
                round,
                txs: Vec::new(),
                leaving: Vec::new(),
                leaving_barriers: Vec::new(),
                released: None,
            },
        });
    }

    /// The cadence of the root's ship (`sync_interval_ms`), and so of an
    /// archive-bound commit's rise: the delegate's fallback ask. Capped at
    /// the stream back-off's ceiling (2 s): `sync_interval_ms` is only the
    /// floor of the ship cadence (close and fsync nudge a ship sooner), and a
    /// lost push must not park an answer for a long configured interval.
    fn deleg_ship_interval_ms(&self) -> u64 {
        let tick = self.cfg.delegation_stream_tick_ms.max(1);
        self.cfg.sync_interval_ms.clamp(tick, 2_000.max(tick))
    }

    /// An ask for the commit was refused, failed or timed out: the next one
    /// waits as long as a stream's back-off (a root inside its takeover gate,
    /// a partition, must not be asked every tick).
    fn deleg_poke_backoff(&mut self, now: Now, gen: u64) {
        let tick = self.cfg.delegation_stream_tick_ms.max(1);
        let ship = self.deleg_ship_interval_ms();
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.poke_backoff_ms = (d.poke_backoff_ms * 2).clamp(tick * 4, 2_000);
            // Archive-bound: the next ask is the fallback's, one ship interval
            // on, and no sooner than the back-off (a partitioned root is not
            // asked more often than without the flag).
            let wait = if d.archive_bound {
                ship.max(d.poke_backoff_ms)
            } else {
                d.poke_backoff_ms
            };
            d.poke_after = Some(now.wait.plus(wait));
        }
    }

    /// Send the next batch of every generation without one in flight.
    fn deleg_stream(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.dl.mine.is_empty() {
            return;
        }
        // A renewal whose answer was lost (a dropped message: no
        // transport failure, no reply) is sent again, or its timer never
        // comes back (long-delegated seed 70067: reclaimed unrenewed).
        for gen in self.deleg_expire_inflight(now) {
            self.deleg_renew_now(now, gen, out);
        }
        let Some(root) = self.holder_root() else {
            // Learn the holder; the tick streams.
            self.deleg_learn_root(now, out);
            self.arm_stream_tick(now, out);
            return;
        };
        let awaited = self.deleg_awaited_commits(replica);
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            if d.inflight.is_some() || d.refused || d.stream_after.is_some_and(|t| now.wait < t) {
                continue;
            }
            let from = d.streamed_through + 1;
            let mut txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            self.deleg_note_lost_in(gen, &txs, replica);
            if let Some(l) = self.dl.mine.get(&gen).and_then(|d| d.lost_from) {
                txs.retain(|t| t.idx < l);
            }
            // Plan 30 §M12: a node holds one stream (plan 46 M1.2 step 9),
            // but its journal can still hold rows of an earlier one (the
            // directory delegated here again before the first generation's
            // strand); a transaction may depend on this node's own
            // execution under it (it reached it locally). The root must
            // hold that first: the batch stops before a transaction whose
            // own-generation deps the root has not acknowledged, and goes
            // out once it has.
            // A row the log carries is the root's whatever it answered
            // (an acknowledgement lost, then the row retired here).
            let acked_here = |h: u64, i: u64, mine: &BTreeMap<u64, DelegateState>| -> bool {
                h == gen
                    || mine.get(&h).is_none_or(|o| o.streamed_through >= i)
                    || replica.log_stream_idx(h) >= i
            };
            if let Some(stop) = txs.iter().position(|t| {
                t.deps
                    .streams
                    .iter()
                    .any(|(h, i)| !acked_here(h, i, &self.dl.mine))
            }) {
                txs.truncate(stop);
            }
            // A row whose `deps` name a row its generation ended without
            // (another generation of this node reclaimed below a row this
            // one followed, that row stranded and replayed by rid after the
            // end) is not the log's to carry before its cause: appended, it
            // would land ahead of the replay. The root does not append it
            // either (`on_delegate_stream`). This generation stops before it:
            // nothing more executes here, its recall is answered at the row
            // before, and the root ends it there, so the row strands with
            // the rest of the stream and is replayed by rid after its cause.
            if let Some(lost) = txs.iter().position(|t| replica.deps_lost(&t.deps)) {
                let idx = txs[lost].idx;
                txs.truncate(lost);
                self.deleg_note_lost(gen, idx);
            }
            // Plan 46 M1.2 step 10: a release whose read delegations are
            // still being recalled keeps its row, and what follows it, back.
            if let Some(PendingRelease {
                held: true,
                first_row: Some(row),
                ..
            }) = self.dl.mine.get(&gen).and_then(|d| d.release.as_ref())
            {
                let row = *row;
                txs.retain(|t| t.idx < row);
            }
            let d = self.dl.mine.get(&gen).expect("present");
            if txs.is_empty() {
                if let Some(since) = awaited.get(&gen) {
                    self.deleg_poke_root(now, gen, root, *since, out);
                }
                continue;
            }
            let mut last = txs.last().map(|t| t.idx).unwrap_or(from);
            // A designee answers its rows at once and its commits reach
            // the OS, not the disk: a power loss can drop the end of its
            // journal. Nothing ends a designation then (it renews nothing,
            // the root never reclaims it), and the designee journals on
            // from what its disk kept, so its next rows take the lost
            // indices. Had the root appended the lost rows, it would skip
            // the new ones as appended (`on_delegate_stream`: at or below
            // its cursor) and the log and the designee would part. So a
            // designee's rows leave in its stream only once they are on its
            // disk, and the root's cursor never passes what a restart finds.
            // The sync is a full flush of the store: off the core
            // (`Action::SyncDelegStream`), the rows it covers sent once it
            // is done (`Core::on_deleg_stream_synced`).
            // (The root's own stream is appended to the journal the rows are
            // in: there is no second copy to keep ahead of a power loss.)
            if d.non_stealable && root != self.cfg.node_id {
                if last > d.synced_through && d.sync_inflight.is_none() {
                    let through = replica.delegate_idx(gen);
                    self.stats.deleg_designee_syncs += 1;
                    self.dl.mine.get_mut(&gen).expect("present").sync_inflight = Some(through);
                    out.push(Action::SyncDelegStream { gen, through });
                }
                let synced = self.dl.mine[&gen].synced_through;
                let keep = txs.iter().take_while(|t| t.idx <= synced).count();
                txs.truncate(keep);
                let Some(l) = txs.last().map(|t| t.idx) else {
                    continue;
                };
                last = l;
            }
            let leaving = self.deleg_leaving_grants(now, gen, replica);
            let leaving_barriers = self.deleg_leaving_barriers(now, gen, &leaving, replica);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::Stream));
            self.stats.deleg_streamed_txs += txs.len() as u64;
            // ADR-55: rows of a stream this node was granted as the root,
            // streamed to a root that is another node (it was deposed and
            // lives on as the holder): the witness of breakdown Q4.
            if root != self.cfg.node_id && gen >> 32 != 0 && self.minted_by_me(gen) {
                self.stats.root_own_resumed += txs.len() as u64;
            }
            let d = self.dl.mine.get_mut(&gen).expect("present");
            let round = d.stream_round;
            d.inflight = Some((req, last));
            d.inflight_at = now.wait;
            let released = d
                .release
                .as_ref()
                .filter(|r| !r.held)
                .map(|r| r.note_at(now.wait));
            d.leaving = (!leaving.is_empty() || !leaving_barriers.is_empty()).then(|| {
                let mut inos: Vec<Ino> = leaving.iter().map(|g| g.ino).collect();
                inos.extend(leaving_barriers.iter().map(|(ino, _)| *ino));
                (last, inos)
            });
            tracing::debug!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?req,
                from,
                last,
                root,
                leaving = leaving.len(),
                "delegate stream batch"
            );
            out.push(Action::Send {
                to: root,
                msg: PeerMsg::DelegateStream {
                    req,
                    gen,
                    round,
                    txs,
                    leaving,
                    leaving_barriers,
                    released,
                },
            });
        }
        // A lost ack (the transport reports it), a batch that could not
        // be sent, or a generation (or its backup append) backing off:
        // the tick retries.
        // An answer parked on the root's word (`deleg_poke_root`) is asked
        // for on the tick too.
        if self.dl.mine.values().any(|d| {
            d.inflight.is_some()
                || d.stream_after.is_some_and(|t| now.wait < t)
                || d.backup_after.is_some_and(|t| now.wait < t)
                || (!d.stopped && replica.delegate_idx(d.gen) > d.streamed_through)
                || d.ask.is_some()
                || awaited.contains_key(&d.gen)
        }) {
            self.arm_stream_tick(now, out);
        }
    }

    /// The root's acknowledgement of this node's stream of `gen`,
    /// cumulative: the root holds the stream through `through`, whichever
    /// batch `req` names. It counts whenever it comes — an answer to a
    /// batch given up on and re-sent, one overtaken by a later answer, a
    /// duplicate — provided it is from the root this generation streams
    /// to, in the round it streams in: a new root re-streams from what the
    /// log carries, and must not be credited with what an old one (or an
    /// earlier tenure of the same node, or an answer to this node's
    /// previous incarnation) holds (long-delegated-backup seed 75504: the
    /// old root's ack through 7 arrived after the re-stream to its
    /// successor, whose cursor was 6; the delegate's last row never
    /// reached the log). Only the rows the root applied are ever credited:
    /// `through` is its cursor after the batch. A refusal counts only
    /// for the batch in flight (the next one is refused, or not, anew).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_delegate_stream_ack(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        through: u64,
        committed: u64,
        refused: bool,
        archive_bound: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let root = self.holder_root();
        if gen == LEAVE_NOTE_GEN {
            self.on_leave_note_ack(from, req, refused);
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let stale = root != Some(from) || d.last_root != Some(from) || round != d.stream_round;
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?req,
            through,
            committed,
            refused,
            was = d.streamed_through,
            was_committed = d.root_committed,
            inflight = ?d.inflight,
            stale,
            "delegate stream acknowledgement"
        );
        if stale {
            self.stats.deleg_stream_acks_stale += 1;
            return;
        }
        let answers_inflight = d.inflight.is_some_and(|(r, _)| r == req);
        // The answer to an ask for the commit (`deleg_poke_root`), apart from
        // any batch of rows in flight: it is no answer to that.
        // (Its answer is cumulative like any: through the end of a batch of rows
        // sent after it, it covers that batch, and the batch's own answer then
        // counts as no progress and backs the rows off once, as a duplicate
        // answer does.)
        let is_ask = d.ask.is_some_and(|(r, _)| r == req);
        if is_ask {
            d.ask = None;
            self.dl.by_req.remove(&req);
        }
        // An acknowledgement through the in-flight batch's last row (one
        // for a given-up batch overtaken by its re-send) answers that
        // batch too: the next one goes out now, not after the re-send's
        // own answer.
        let covers_inflight = !refused && d.inflight.is_some_and(|(_, last)| last <= through);
        if answers_inflight || covers_inflight {
            if let Some((r, _)) = d.inflight.take() {
                self.dl.by_req.remove(&r);
            }
        }
        let d = self.dl.mine.get_mut(&gen).expect("present");
        if refused {
            if !answers_inflight {
                if is_ask {
                    self.deleg_poke_backoff(now, gen);
                }
                return;
            }
            // The generation is ending, or the root has not learned it
            // (a successor before its table, a holder inside its takeover
            // gate): the log ends a generation, a refusal does not — back
            // off and try again on the tick.
            self.stats.deleg_stream_refused += 1;
            let tick = self.cfg.delegation_stream_tick_ms.max(1);
            d.refusal_backoff_ms = (d.refusal_backoff_ms * 2).clamp(tick * 4, 2_000);
            d.stream_backoff_ms = d.refusal_backoff_ms;
            d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
            self.arm_stream_tick(now, out);
            return;
        }
        // Plan 43 Q16 (ADR-41): what the root's tenure has committed of its
        // appends is its copy for this delegate's group. Counted whenever it
        // comes, before the checks below drop an acknowledgement that
        // appended nothing new: the root's push (`root_stream_push`) is such
        // one. Cumulative, and never past what the root appended.
        let committed = committed.min(through);
        let committed_rose = committed > d.root_committed;
        // The root's word on its commit's cadence (an answer older than what
        // the delegate already counted says nothing new).
        if committed >= d.root_committed {
            d.archive_bound = archive_bound;
        }
        if is_ask {
            if committed_rose {
                d.poke_backoff_ms = 0;
            } else {
                // The root had nothing more committed (a lone root's commit
                // rises when it ships): ask again after the back-off, not on
                // every tick, or the ask costs one round trip per tick for
                // the whole wait of the root's ship cadence.
                self.deleg_poke_backoff(now, gen);
            }
        }
        let d = self.dl.mine.get_mut(&gen).expect("present");
        if committed_rose {
            d.root_committed = committed;
            self.stats.deleg_root_committed_raised += 1;
            self.stats.deleg_root_asks_raised += u64::from(is_ask);
            if self.has_stream_parks() {
                self.complete_ready(now, replica, out);
            }
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let progressed = through > d.streamed_through;
        // Plan 46 M1.2 step 10: the root appended the release row (its batch
        // carried the note, which it handled before the rows), or
        // acknowledged the batch of a release that journaled none.
        // ADR-55: the release row's append to the root's own journal (this
        // node is the root) is no copy beyond this disk until the segment that
        // carries it is acknowledged: a root deposed before that, with the
        // stream dropped here, would leave the row in its journal for good and
        // the new root holding the stream live. The state stays until the row
        // has shipped.
        let own_row_pending = from == self.cfg.node_id
            && d.release.as_ref().is_some_and(|r| {
                r.row
                    .is_some_and(|row| !Self::own_row_shipped(replica, gen, row))
            });
        let d = self.dl.mine.get_mut(&gen).expect("present");
        if !own_row_pending
            && d.release.as_ref().is_some_and(|r| {
                !r.held
                    && match r.row {
                        Some(row) => through >= row,
                        None => answers_inflight,
                    }
            })
        {
            // (Only a release of the whole stream counts as the stream
            // released: a stream a refused renewal, a backup's seal, lost
            // dependencies, a held note that failed or an epoch stopped is
            // not released, nor ended in the log.)
            d.release_acked |= d.release.as_ref().is_some_and(|r| r.ends);
            d.release = None;
        }
        if !progressed && !answers_inflight {
            return;
        }
        if !progressed {
            // The root took none of the batch: a dependency it does not
            // hold yet, or one lost with its generation
            // (`deleg_append_deps_lost`). Back off, as for a refusal: the
            // next event re-sent the same batch at once, and the two
            // nodes looped at the round trip until the dependency came
            // (or, lost, until this node applied the end of its
            // generation).
            self.stats.deleg_stream_no_progress += 1;
            // A fixed short delay, not the refusal's doubling: nothing
            // wakes the stream when the missing dependency arrives, so a
            // long back-off would be the dependency's latency.
            let tick = self.cfg.delegation_stream_tick_ms.max(1);
            d.stream_after = Some(now.wait.plus(tick * 4));
            self.arm_stream_tick(now, out);
            return;
        }
        if progressed && !answers_inflight {
            tracing::debug!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                through,
                was = d.streamed_through,
                "a stream acknowledgement for an earlier batch counted"
            );
        }
        d.streamed_through = d.streamed_through.max(through);
        if d.inflight.is_none() {
            d.stream_after = None;
            d.stream_backoff_ms = 0;
            d.refusal_backoff_ms = 0;
        }
        if progressed && !answers_inflight {
            self.stats.deleg_stream_acks_unmatched += 1;
        }
        self.deleg_drop_left(gen, replica);
        self.deleg_stream(now, replica, out);
    }

    /// A stream, renewal or recall request failed at the transport: let
    /// the tick retry it.
    pub(crate) fn on_deleg_request_failed(
        &mut self,
        now: Now,
        req: OpId,
        out: &mut Vec<Action>,
    ) -> bool {
        // The off-core lane's request failed at the transport (the
        // heartbeat thread tells the core on the urgent lane, behind the
        // lane's own registration): it is not pending any more, and a loss
        // is not to be put down to a root that never answered it.
        if req.0 & crate::deleg_lane::LANE_REQ_BIT != 0 {
            return self.dl.late.remove(&req).is_some();
        }
        let Some((gen, kind)) = self.dl.by_req.remove(&req) else {
            return false;
        };
        match kind {
            ReqKind::Stream => {
                let tick = self.cfg.delegation_stream_tick_ms.max(1);
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.inflight = None;
                    // Back off: the root is unreachable or slow, and the
                    // stream is retried on the tick, not per event
                    // (harness `delegate-partition`: 440k sends in a cut).
                    d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
                    d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
                }
                self.arm_stream_tick(now, out);
            }
            ReqKind::Poke => {
                // An ask for the commit failed at the transport: it is asked
                // again after the back-off. A batch of rows in flight is
                // none of its.
                if self
                    .dl
                    .mine
                    .get(&gen)
                    .is_some_and(|d| d.ask.is_some_and(|(r, _)| r == req))
                {
                    self.dl.mine.get_mut(&gen).expect("present").ask = None;
                    self.deleg_poke_backoff(now, gen);
                }
                self.arm_stream_tick(now, out);
            }
            ReqKind::Renew => {
                // (A lapsed generation retries on its own cadence: the
                // root is gone or cut off, not slow.)
                let retry = match self.dl.mine.get(&gen) {
                    Some(d) if d.lapsed => self.deleg_request_timeout_ms(),
                    _ => self.cfg.delegation_stream_tick_ms,
                };
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.renew = None;
                    if d.renew_timer.is_none() {
                        let t = self.set_timer_honour(
                            now.honour.plus(retry),
                            Timer::DelegRenew(gen),
                            out,
                        );
                        self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
                    }
                }
            }
            ReqKind::Recall => {
                // One attempt failed: the resend timer sends the next one,
                // and the expiry outwaits them all.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.recall_tries.retain(|r| *r != req);
                    if g.recall_req == Some(req) {
                        g.recall_req = None;
                    }
                }
            }
            ReqKind::BackupAppend => {
                // Phase 2b: the backup is unreachable; the tick resends,
                // and after three failures the delegate goes on without
                // it (M9: no peer in budget means the local policy).
                let mut dropped = None;
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.backup_inflight = None;
                    d.backup_failures += 1;
                    if d.backup_failures >= 3 {
                        dropped = d.backup.take();
                        d.backup_failures = 0;
                    }
                }
                if let Some(b) = dropped {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        backup = b,
                        "delegate dropped an unreachable backup"
                    );
                }
                self.arm_stream_tick(now, out);
            }
            ReqKind::SealPrevRoot => {
                // The previous root is unreachable: the next round of
                // `deleg_prev_roots_step` asks again (the request stays known: a
                // late answer is as good).
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    if let Some(&m) = g.prev_reqs.get(&req) {
                        g.member_notes.entry(m).or_default().failed = Some(now.wait);
                    }
                }
            }
            ReqKind::Seal => {
                // Phase 2b: the backup did not answer the seal: the
                // grant's expiry asks again (a recorded backup's tail is
                // never given up, C2e) or ends the generation without it.
                // Back to the phase the seal replaced: a recall under way
                // stays one, so no renewal revives the grant (long-
                // delegated-backup seed 71251: the backup had crashed,
                // the phase went back to `None`, the restarted delegate's
                // renewals were granted again and again, and the root's
                // own op parked on the recall waited past its deadline —
                // `EIO` for a write the log carried).
                //
                // Only a strict member's failure, and only while no tail is
                // in hand: a best-effort member's changes nothing.
                let mut granting = false;
                let mut warn = false;
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    // (A best-effort member stays asked: once, not per window.)
                    // (A request no longer known — a group record or the end of
                    // the seal forgot it — changes nothing.)
                    let member = g.seal_reqs.get(&req).map(|&(m, _)| m);
                    if let Some(m) = member {
                        g.member_notes.entry(m).or_default().failed = Some(now.wait);
                    }
                    let strict_failed = match g.seal_reqs.get(&req) {
                        Some(&(_, false)) | None => false,
                        Some(&(_, true)) => {
                            g.seal_reqs.remove(&req);
                            true
                        }
                    };
                    if strict_failed && g.sealed_tail.is_none() && g.recall == RecallPhase::Sealing
                    {
                        // Plan 46 M1.2 step 14: a seal asked for a delegate's
                        // release goes back to nothing: the delegate stopped
                        // at a cut below rows its group may hold, and the
                        // phase before it was not the end of the stream.
                        if !g.release_seal {
                            g.recall = match g.recall_before_seal {
                                RecallPhase::Sealing => RecallPhase::None,
                                before => before,
                            };
                        }
                        granting = g.recall == RecallPhase::None && !g.ended;
                        // The member is unreachable (down, or cut from this
                        // root): the generation cannot end without its tail,
                        // and says so, as when the seal goes unanswered.
                        // The seals asked of the other strict members are of
                        // the seal that is over.
                        if !g.release_seal {
                            g.seal_reqs.retain(|_, (_, strict)| !*strict);
                        }
                        if !g.ended {
                            g.waiting_for_group = true;
                            g.waiting_since.get_or_insert(now.wait);
                            warn = g
                                .waiting_warned
                                .is_none_or(|at| now.wait.since(at) >= 60_000);
                            if warn {
                                g.waiting_warned = Some(now.wait);
                            }
                        }
                    }
                }
                if warn {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        "a delegation's recorded backup cannot be reached for its seal: the generation is not ended without its tail \
                         (`constellation repair abandon --gen {gen}` ends it at the root's cursor; rows only the group holds are lost)"
                    );
                }
                if granting && !self.dl.epoch_active {
                    // The core grants again (and so may the answerer).
                    self.dw.reopen(gen);
                }
                self.arm_gen_expiry(now, gen, out);
            }
        }
        true
    }

    // ----------------------------------------------------- renewals

    /// An accepted answer to a renewal of `gen` from `root`, which the root
    /// sent at `sent_at` on its clock: the next renewal echoes it
    /// ([`PeerMsg::DelegRenew::stream_head_at`]). Only a later one replaces
    /// it (the same root's clock), a different root's always does.
    /// When this node sent renewal `req` of `gen` (honour clock): the one
    /// in flight, or one whose answer comes late (`LateReq`).
    pub(crate) fn deleg_renew_sent(&self, gen: u64, req: OpId) -> Option<HonourMs> {
        self.dl
            .mine
            .get(&gen)
            .and_then(|d| d.renew)
            .filter(|(r, _)| *r == req)
            .map(|(_, sent)| sent)
            .or_else(|| {
                self.dl
                    .late
                    .get(&req)
                    .filter(|l| l.gen == gen && l.kind == ReqKind::Renew)
                    .map(|l| l.sent)
            })
    }

    pub(crate) fn deleg_note_root_answer(&mut self, gen: u64, root: NodeId, sent_at: i64) {
        if sent_at <= 0 {
            return;
        }
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            match d.root_answer {
                Some((r, at)) if r == root && at >= sent_at => {}
                _ => d.root_answer = Some((root, sent_at)),
            }
        }
    }

    pub(crate) fn deleg_renew_now(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(root) = self.holder_root() else {
            // Learn the holder, and try again after a tick: the first
            // renewal of a grant installed from an S3 tail (no P2P
            // exchange with the root yet) must not be the last
            // (long-delegated seed 70067: never renewed, reclaimed).
            self.deleg_learn_root(now, out);
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                if d.renew_timer.is_none() && !d.stopped {
                    let t = self.set_timer_honour(
                        now.honour.plus(self.cfg.delegation_stream_tick_ms.max(1)),
                        Timer::DelegRenew(gen),
                        out,
                    );
                    self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
                }
            }
            return;
        };
        let _ = self.deleg_expire_inflight(now);
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.renew.is_some() || d.stopped {
            return;
        }
        let req = self.op_id();
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.renew = Some((req, now.honour));
        d.renew_sent_wait = now.wait;
        d.renew_resent = std::mem::take(&mut d.renew_resend_next);
        let first = !d.renew_resent;
        if let Some(t) = d.renew_timer.take() {
            self.cancel_timer(t, out);
        }
        self.dl.by_req.insert(req, (gen, ReqKind::Renew));
        let d = self.dl.mine.get_mut(&gen).expect("present");
        // Plan 43 C2: the renewal carries the *request* for a group change
        // (the root journals a `DelegateNodeGroup` record for it), or the group the
        // delegate applied.
        let backup = Self::deleg_requested_backup(d);
        d.renew_backup = backup;
        let round = d.stream_round;
        // Honoured until 0: never granted (a grant sets it).
        let ungranted = d.until == HonourMs(0);
        tracing::debug!(
            node = self.cfg.node_id,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?req,
            root,
            "delegation renewal sent"
        );
        out.push(Action::Send {
            to: root,
            msg: PeerMsg::DelegRenew {
                req,
                gen,
                round,
                backup,
                stream_head: 0,
                // Plan 46 M1.2 step 11 (step 10 review): a moment on the
                // root's clock that precedes this send, not this node's.
                stream_head_at: if self.cfg.deleg_head_at_own_clock {
                    now.wall.0
                } else {
                    self.dl
                        .mine
                        .get(&gen)
                        .and_then(|d| d.root_answer)
                        .filter(|(from, _)| *from == root)
                        .map_or(i64::MIN, |(_, at)| at)
                },
                first: ungranted,
            },
        });
        // The stream step that sends the early resend (`deleg_expire_inflight`)
        // runs on events and on the stream tick: make sure one comes.
        if first && self.dl.stream_timer.is_none() {
            let at = now.wait.plus(self.deleg_resend_ms().max(1) as u64);
            let t = self.set_timer_wait(at, Timer::DelegStream, out);
            self.dl.stream_timer = Some(t);
        }
    }

    /// Plan 43 C2 (§3.3): the root journals the generation's configuration
    /// as asked by its delegate's renewal: `DelegateNodeGroup { version: latest + 1,
    /// backups }`. Only while the generation lives and is not ending (the
    /// end uses the latest record). The record is a root row like any other
    /// (its class is the root's now); no answer waits for it, and the
    /// delegate counts the new member only once its own table records it,
    /// i.e. once the record is committed (D4).
    fn deleg_journal_group(&mut self, gen: u64, backups: Vec<NodeId>, replica: &dyn Replica) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        let version = g.group.version + 1;
        let _class = self.journal_class();
        let rec = LogRecord::DelegateNodeGroup {
            node: g.node,
            gen,
            version,
            backups: backups.clone(),
        };
        if let Err(error) = replica.apply_records_journaled(&[rec], None) {
            tracing::warn!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), %error, "could not journal a delegation group record");
            return;
        }
        self.stats.deleg_group_records += 1;
        tracing::info!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            version,
            ?backups,
            "delegation group recorded"
        );
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.group.named.extend(backups.iter().copied());
            g.group.version = version;
            g.group.backups = backups;
            // The seals asked for the group that was are of no use to this
            // one: an answer to one must not end the generation by a member
            // the rows are no longer acknowledged through.
            g.seal_reqs.retain(|_, (_, strict)| !*strict);
        }
    }

    pub(crate) fn on_deleg_renew_timer(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.renew_timer = None;
        }
        self.deleg_renew_now(now, gen, out);
        self.deleg_lapsed_retry(now, gen, out);
    }

    /// Root side: grant (or refuse) a renewal. `backup`: the delegate's
    /// backup as its core names it ([`RenewFrom::Core`]), or the off-core
    /// lane's renewal ([`RenewFrom::OffCore`]), which carries neither a
    /// backup nor a stream head and so extends the grant and nothing else.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_renew(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        source: RenewFrom,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A delegate is another node: a root that still counts itself
        // alone (no peer or roster event named it yet) starts the kernel
        // drain before it grants anything (the first renewal waits on it).
        if from != self.me() {
            self.note_foreign(now, replica, out);
        }
        if let Some(g) = self.dl.gens.get_mut(&gen).filter(|g| g.node == from) {
            g.delegate_heard = Some(now.wait);
        }
        if self.deleg_renew_waits_for_reads(now, from, req, gen, round, source, replica, out) {
            return;
        }
        let mut ttl_ms = 0u64;
        let mut hold = None;
        let mut ends = false;
        let mut group_change: Option<Vec<NodeId>> = None;
        // Plan 46 M0.3 (S5, ADR-42): a takeover gate whose journal work is
        // done answers the renewals of a generation it has taken on
        // (`delegation_sync` at `RootSync::Gated`): its keys are not among
        // the ones the read quarantine protects. A first renewal waited for
        // that quarantine above (`deleg_renew_waits_for_reads`) and is
        // answered only after it.
        let gated_serves = self.gated_serves(now, gen);
        if (self.root_usable(now) || gated_serves) && !self.dl.epoch_active {
            ends = true;
            // Plan 41 L0: as a lock renewal (`lock_renew_one`), the
            // root extends a delegation only as far as a fresh owner may:
            // under a tenure that can be taken over before its lease
            // expires, with fresh S3 liveness and a marked lease, or the
            // answer waits for the lease read that proves the tenure. The
            // successor waits a delegation renewal out only if it was
            // made within `backup_takeover_ms` of the old root's last
            // proof (the `deleg` term of `note_marker_landed`'s floor,
            // the reclaim horizon of an inherited generation); with only
            // `root_usable` (no liveness), a root sealed by its backups
            // renewed for as long as its lease ran, and its delegate kept
            // granting locks in the subtree the successor reclaimed.
            hold = self.renew_hold(now);
            let cap = self.grant_cap_ms(now);
            let extra = self.reclaim_horizon_ms() - self.cfg.delegation_ttl_ms;
            // Plan 43 C2e: a delegate that renews while the root waits for its
            // group's tail is alive, and holds every row it acknowledged on
            // its own disk: the seal that waits is given up (as when its
            // transport fails) and the grant goes on. A renewal says so: the
            // process that sends it held the generation all along, or took it
            // up after a restart on the same boot, whose journal is as the
            // last process left it; one that held it before a reboot (a power
            // loss can drop the unsynced end of the journal) never renews it
            // again (`delegation_sync`), and the wait goes on for the backup. Its backup may have
            // been sealed meanwhile; the delegate then loses it and stops,
            // and the next expiry asks again. Not while a tail is in hand.
            let revive = self.dl.gens.get(&gen).is_some_and(|g| {
                g.node == from
                    && !g.ended
                    && g.recall == RecallPhase::Sealing
                    && g.waiting_for_group
                    && g.sealed_tail.is_none()
                    && !g.release_seal
            });
            if revive {
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.recall = match g.recall_before_seal {
                        RecallPhase::Sealing => RecallPhase::None,
                        before => before,
                    };
                    g.seal_reqs.retain(|_, (_, strict)| !*strict);
                    if g.recall == RecallPhase::None {
                        self.dw.reopen(gen);
                    }
                }
            }
            // Plan 46 M1.3 step 13: an abandon in its wait has no holder
            // grant anything after the command (the deadline's premise): the
            // renewal is refused, and the holder stops.
            let abandoning = self.dl.abandons.iter().any(|w| w.gens.contains(&gen));
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                if g.node == from && !g.ended && g.recall == RecallPhase::None && !abandoning {
                    ttl_ms = self.cfg.delegation_ttl_ms.min(cap);
                    if ttl_ms > 0 {
                        // The delegate is alive after all: nothing waits
                        // for its group's tail any more.
                        g.waiting_for_group = false;
                        g.waiting_since = None;
                        // (A `max` in the shared window table: it commutes
                        // with any extension made off the core.)
                        self.dw
                            .extend_core(gen, now.wait.plus(ttl_ms + extra), true);
                    }
                    // Plan 43 C2: the delegate's request for its group is
                    // the core's to make: the lane's certificate can be a
                    // step old, and a record journaled for a stale `None` (or
                    // the previous backup) would name a member clients are
                    // no longer acknowledged by. A request that differs from
                    // the generation's latest record is journaled as the
                    // next version, below.
                    if let RenewFrom::Core { backup, .. } = source {
                        let want: Vec<NodeId> = backup.into_iter().collect();
                        if want != g.group.backups {
                            group_change = Some(want);
                        }
                    }
                }
            }
            if let Some(want) = group_change {
                self.deleg_journal_group(gen, want, replica);
            }
            tracing::debug!(
                node = self.me(),
                from,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?req,
                ttl_ms,
                "delegation renewal"
            );
            if ttl_ms > 0 {
                self.stats.deleg_renewals += 1;
                if gated_serves {
                    self.stats.deleg_renewals_gated += 1;
                }
                self.lease.touch(now);
                self.arm_gen_expiry(now, gen, out);
                // Capped by the lease below what the delegate needs to
                // grant a lock (it honours `ttl − margin` and caps its
                // grants a margin before that): the lease is renewed now.
                // At half its ttl, a short lease ended the chain of caps
                // (its own margin, the delegation's, the delegate's grant
                // cap, the holder's) before its renewal: the delegate's
                // renewals came back no longer, and a held lock lapsed
                // with no fault (sim `locks-delegated-production-ttl-heavy`
                // seed 10: a 6 s lease, 1 s margins).
                if ttl_ms < self.cfg.delegation_ttl_ms
                    && (ttl_ms as i64) < self.lock_min_grant_ms() + 2 * self.lock_margin_ms()
                {
                    self.lease_renew_for_grants(now, out);
                }
            } else {
                self.stats.deleg_renewals_refused += 1;
            }
        } else if !self.root_usable(now) && self.deleg_renewal_awaits_own_lease(now, replica, out) {
            // Left unanswered: the delegate re-sends it when it times out,
            // and the re-adopted root, which inherits the generation,
            // grants it.
            tracing::debug!(
                node = self.me(),
                from,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?req,
                "delegation renewal left unanswered: this node may be the root again"
            );
            return;
        } else if self.lease.held.is_some()
            && !self.lease.lost
            && !self.lease.releasing
            && self.lease.gate.is_some()
        {
            // Plan 46 M0.3 (S2): the takeover gate is pending (not yet
            // shippable, or the generation is one it does not serve: ADR-42
            // answers the others), and this node is the root as soon as it
            // opens. A refusal (`ttl_ms` 0) stops
            // the generation at its delegate for good (`deleg_renewal_
            // refused`), and the gate then inherits it as live
            // (`delegation_sync`) and outwaits it for a TTL, the delegate's
            // own ops held and other nodes' bounced meanwhile. Unanswered,
            // the delegate sends it again on its timeout: the opened root
            // grants it, or the delegate lapses (`on_deleg_lapse`), which is
            // always safe. A refusal is for a root that decided the
            // generation ends: one that recalls it, or whose lease is lost
            // or released (a releasing gated root will never be the root).
            tracing::debug!(
                node = self.me(),
                from,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?req,
                "delegation renewal left unanswered: the takeover gate is pending"
            );
            return;
        } else {
            self.stats.deleg_renewals_refused += 1;
        }
        // Plan 30 §M14: the lock grants under the subtree travel with the
        // first renewal that grants, and again with every later one while
        // they may be live (the reply can be lost).
        // So does what is left of a lock grace here that covers it.
        let (locks, lock_carry, lock_grace_ms) = if ttl_ms > 0 {
            let (locks, carry) = self.lock_take_handoff(now, gen);
            (
                locks,
                carry,
                self.lock_grace_for_generation(now, gen, replica),
            )
        } else {
            (Vec::new(), Vec::new(), 0)
        };
        // The cut first: a barrier it settles raises the floor.
        let (lock_cut_at, lock_cut, lock_barrier) = if ttl_ms > 0 {
            let (at, cut, barrier) = self.lock_cut_for_generation(now, gen, replica);
            (at, Box::new(cut), barrier)
        } else {
            (0, Box::default(), 0)
        };
        let lock_floor = if ttl_ms > 0 {
            self.lock_floor_for_generation(gen)
        } else {
            Default::default()
        };
        let msg = PeerMsg::DelegRenewed {
            req,
            gen,
            round,
            ttl_ms,
            locks,
            lock_grace_ms,
            lock_floor,
            lock_cut_at,
            lock_cut,
            lock_barrier,
            lock_carry,
            sent_at: now.wall.0,
            ends: ends && ttl_ms == 0,
        };
        self.deleg_answer_renewal(now, from, hold.filter(|_| ttl_ms > 0), msg, out);
    }

    /// A delegation renewal reached this node, which holds no usable
    /// lease: whether it may be the root again in a moment, so that the
    /// renewal is left unanswered rather than refused. A refusal
    /// (`ttl_ms` 0) stops the generation at its delegate for good
    /// (`on_deleg_renewed`), and nothing asked for the lease again: the
    /// subtree's lock waiters were answered `NotOwner { 0 }` by the
    /// delegate and relearned a lease naming this node until some
    /// mutation took it (sim `locks-delegated+crash1@4507:620` seed 222: a
    /// waiter unanswered for 60 s). So a restarted root whose previous
    /// incarnation's lease the object still names re-adopts it, as it
    /// does for a forward (EC2 campaign 8 A-1); and one that has not read
    /// the lease since it started (the start's read in flight, or failed)
    /// reads it now and decides at the delegate's next renewal.
    fn deleg_renewal_awaits_own_lease(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if self.readopting() || self.readopt_own_lease_for_forward(now, replica, out) {
            self.stats.readopt_for_forward += 1;
            return true;
        }
        if self.cfg.p2p
            && self.lease.last_seen.is_none()
            && self.lease.cached_holder.is_none()
            && self.lease.held.is_none()
            && !self.lease.lost
            && !self.retired()
        {
            self.issue_s3(
                crate::action::S3Op::LeaseGet,
                super::S3For::RefreshHolder,
                out,
            );
            return true;
        }
        false
    }

    /// A granting answer (`ttl_ms > 0`) to a delegate's renewal goes out
    /// from this tenure: the delegate has the lock content it carried (the
    /// first handoff, the grace, the floor of its subtree).
    fn note_granting_answer(&mut self, msg: &PeerMsg) {
        if let PeerMsg::DelegRenewed {
            gen,
            ttl_ms,
            lock_floor,
            ..
        } = msg
        {
            if *ttl_ms > 0 {
                self.dl.answered_granting.insert(*gen);
                self.dl.floor_sent.insert(*gen, *lock_floor);
            }
        }
    }

    /// Plan 41 R2: the lock side has nothing left for generation `gen`'s
    /// delegate that an answer without lock content would leave behind: this
    /// tenure sent it a granting answer, and since then no handoff is
    /// waiting, no barrier is out and the subtree's floor is what that
    /// answer carried ([`Core::lock_deleg_quiet`]).
    pub(crate) fn deleg_settled(&self, gen: u64, replica: &dyn Replica) -> bool {
        let Some(g) = self.dl.gens.get(&gen) else {
            return false;
        };
        g.read_recalls.is_empty()
            && self.dl.answered_granting.contains(&gen)
            && self.lock_deleg_quiet(gen, &g.dir_list(), replica)
            && self.lock_floor_for_generation(gen)
                == self.dl.floor_sent.get(&gen).copied().unwrap_or_default()
    }

    /// Plan 41 R2: the facts the root's off-core answerer rests on (the lease
    /// as this tenure holds it, the freshness rule of `renew_hold`), or
    /// `None` while this node is not a usable root: no delegation, no
    /// lease, a release or takeover gate, a continuation epoch.
    pub(crate) fn deleg_root_lease(&self) -> Option<super::RootLease> {
        if !self.cfg.delegation || !self.cfg.p2p || self.dl.epoch_active {
            return None;
        }
        let l = &self.lease;
        if l.lost || l.fenced() || l.epoch_held() {
            return None;
        }
        let (held, _) = l.held.as_ref()?;
        Some(super::RootLease {
            epoch: held.epoch,
            honour_until: l.honour_until(&self.cfg)?,
            margin_ms: self.cfg.expiry_margin_ms,
            delegation_ttl_ms: self.cfg.delegation_ttl_ms,
            extra_ms: self.reclaim_horizon_ms() - self.cfg.delegation_ttl_ms,
            fast: self.may_be_taken_over_early(),
            last_s3_fresh: self.last_s3_fresh(),
            window_ms: self.cfg.backup_takeover_ms as i64,
            marked: self.tenure_marked(),
        })
    }

    /// Plan 41 R2: publish what the off-core answerer may rest on, as of the
    /// end of the step that just ran (`Core::handle`'s last act, so before
    /// the driver dispatches the step's actions: a release's CAS or an
    /// epoch's recall goes out only after the certificate that forbids
    /// answering has been published).
    pub(crate) fn deleg_publish_root(&mut self, replica: &dyn Replica) {
        if !self.deleg_known() {
            if self.dl.cert_set {
                self.dw.publish(None, &[]);
                self.dl.cert_set = false;
            }
            return;
        }
        let lease = self.deleg_root_lease();
        let settled: Vec<u64> = if lease.is_some() {
            self.dl
                .gens
                .iter()
                .filter(|(gen, g)| {
                    !g.ended
                        && g.recall == RecallPhase::None
                        && !g.non_stealable()
                        && self.deleg_settled(**gen, replica)
                })
                .map(|(gen, _)| *gen)
                .collect()
        } else {
            Vec::new()
        };
        self.dw.publish(lease, &settled);
        self.dl.cert_set = true;
    }

    /// The root's P2P service extended delegate `from`'s window of `gen` off
    /// the core by a grant of `ttl_ms` ([`Event::DelegRenewAnsweredOffCore`]):
    /// the table is extended already; this is the bookkeeping the answer
    /// skipped. The request itself is handled in full by
    /// [`Core::on_deleg_renew`] (it was queued ahead of this event).
    pub(crate) fn on_deleg_renew_answered_off_core(
        &mut self,
        now: Now,
        gen: u64,
        from: NodeId,
        ttl_ms: u64,
        out: &mut Vec<Action>,
    ) {
        if !self.dl.gens.get(&gen).is_some_and(|g| g.node == from) {
            return;
        }
        self.stats.deleg_root_answered_off_core += 1;
        self.lease.touch(now);
        // The expiry watch reads the (longer) window again.
        self.arm_gen_expiry(now, gen, out);
        // As a short grant of the core's own: the lease is renewed now, for
        // the delegate to be able to grant locks (see `on_deleg_renew`).
        if ttl_ms < self.cfg.delegation_ttl_ms
            && (ttl_ms as i64) < self.lock_min_grant_ms() + 2 * self.lock_margin_ms()
        {
            self.lease_renew_for_grants(now, out);
        }
    }

    /// Plan 41 L0: send a delegation renewal's answer, or hold it
    /// (`hold`) until the tenure is proven live. Answers to one delegate
    /// keep their order: one made behind a held one waits with it.
    fn deleg_answer_renewal(
        &mut self,
        now: Now,
        to: NodeId,
        hold: Option<FreshHold>,
        msg: PeerMsg,
        out: &mut Vec<Action>,
    ) {
        self.deleg_flush_held(now, out);
        let behind = self
            .ack
            .renew_held_deleg
            .iter()
            .rev()
            .find(|(_, n, _)| *n == to)
            .map(|(f, ..)| *f);
        match (hold, behind) {
            (None, None) => {
                self.note_granting_answer(&msg);
                out.push(Action::Send { to, msg })
            }
            (Some(f), _) => {
                self.stats.renew_held += 1;
                self.ack.renew_held_deleg.push((f, to, msg));
                self.renew_kick(&[f], out);
            }
            (None, Some(f)) => self.ack.renew_held_deleg.push((f, to, msg)),
        }
    }

    /// The held delegation renewal answers that may go now, in order per
    /// delegate ([`Core::renew_verdict`]). One whose tenure is gone is
    /// dropped, not refused: a refusal (`ttl_ms: 0`) ends the generation
    /// at its delegate, while a delegate whose renewal goes unanswered
    /// asks again when it times out — at whichever root is then the
    /// holder.
    pub(crate) fn deleg_flush_held(&mut self, now: Now, out: &mut Vec<Action>) {
        let held = std::mem::take(&mut self.ack.renew_held_deleg);
        let mut keep: Vec<(FreshHold, NodeId, PeerMsg)> = Vec::new();
        for (f, to, msg) in held {
            let verdict = if keep.iter().any(|(_, n, _)| *n == to) {
                FreshVerdict::Wait
            } else {
                self.renew_verdict(now, f)
            };
            match verdict {
                FreshVerdict::Ready => {
                    self.note_granting_answer(&msg);
                    out.push(Action::Send { to, msg })
                }
                FreshVerdict::Wait => keep.push((f, to, msg)),
                FreshVerdict::Refuse => self.stats.renew_refused += 1,
            }
        }
        self.ack.renew_held_deleg = keep;
    }

    /// When this node stops counting on its grant of a generation it
    /// cannot get renewed. This node honours a grant until
    /// `sent + ttl − margin` from its request (`until`), the root until
    /// `granted + ttl + margin` from the renewal it answered, and
    /// `granted ≥ sent`; so the lapse point `until + 2 × margin` is the
    /// root's earliest reclaim of a grant not renewed since — the
    /// delegate lapses first, by the request's one-way delay. A grant never renewed here
    /// (installed from the log, or re-adopted at a restart) lapses at
    /// the reclaim horizon from the install.
    fn deleg_lapse_at(&self, d: &DelegateState) -> HonourMs {
        HonourMs(self.dl_t().deleg_lapse_at(d.until.0, d.installed.0))
    }

    fn arm_deleg_lapse(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let old = d.lapse_timer.take();
        if let Some(t) = old {
            self.cancel_timer(t, out);
        }
        let d = self.dl.mine.get(&gen).expect("present");
        if d.non_stealable || d.stopped || d.lapsed {
            return;
        }
        let at = self.deleg_lapse_at(d).max(now.honour.plus(1));
        let t = self.set_timer_honour(at, Timer::DelegLapse(gen), out);
        self.dl.mine.get_mut(&gen).expect("present").lapse_timer = Some(t);
    }

    /// The lapse watch fired: a grant nobody renewed by the root's
    /// earliest reclaim is not counted on any more. A live, reachable
    /// root answers a renewal within a round trip, so the root is dead or
    /// cut off, and the ops parked on the renewal would wait for an
    /// answer that may never come — for minutes on kind, where the root's
    /// engine pod was force-deleted and this node's `fsync`s never
    /// returned. Lapsed, the generation executes and parks nothing more:
    /// its ops take the ordinary route, to a live root (which recalls the
    /// generation first), else the inbox and the lease path, where this
    /// node takes the root over once the lease allows and ends the
    /// generation itself (its own rows are then stranded and replayed by
    /// rid, after the predecessor's tail). It still streams what it holds
    /// and keeps asking for a renewal: a link that recovers before the
    /// root reclaims gets the grant back (a granted renewal ends the
    /// lapse); only a refusal or the log ends it. Lapsing is always safe:
    /// it only gives up execution while the grant is not honoured anyway.
    pub(crate) fn on_deleg_lapse(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        d.lapse_timer = None;
        if d.non_stealable || d.stopped {
            return;
        }
        if d.lapsed {
            // The watch armed for the parked ops' give-up point.
            self.deleg_give_up_parks(now, gen, replica, out);
            return;
        }
        let d = self.dl.mine.get(&gen).expect("present");
        if now.honour < d.until || now.honour < self.deleg_lapse_at(d) {
            self.arm_deleg_lapse(now, gen, out);
            return;
        }
        let (dirs, until_ms) = (d.dirs.clone(), d.until.0);
        let (frozen, root_silent) = self.deleg_note_loss(gen, true);
        tracing::warn!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            dirs = ?dirs,
            root = self.holder_root().unwrap_or(0),
            until_ms,
            frozen,
            root_silent,
            "delegation lapsed unrenewed past the root's earliest reclaim: \
             releasing its parked ops, still renewing"
        );
        self.stats.deleg_lapsed += 1;
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.lapsed = true;
        let parked = std::mem::take(&mut d.parked);
        let root = self.holder_root().unwrap_or(0);
        for p in parked {
            self.answer_not_owner(now, p, root, replica, out);
        }
        // Plan 46 M1.2 step 13: no delegate answers on its disk alone, so an
        // op of this node's own clients waits for the root's append, and
        // the lapse does not take it away: a root alive behind a slow link
        // outwaits the generation until `deleg_end_due`, fetches the row
        // with the tail and commits it, and an op sent down the lease path
        // meanwhile (the root refuses it until the generation has ended)
        // would hear `EIO` before its row landed (I4). A root that renews
        // appends the row and releases the park. The give-up is at the
        // root's end of the generation: if the generation is still here
        // and nothing ended it, the root is dead, and the client takes the
        // lease path under its rid, which takes a dead root over by TTL.
        if !self.stream_parks_of(gen).is_empty() {
            self.arm_deleg_give_up(now, gen, out);
        }
        self.deleg_renew_now(now, gen, out);
        self.deleg_lapsed_retry(now, gen, out);
        self.deleg_after_event(now, replica, out);
    }

    /// When a root alive behind the link has ended generation `d` for
    /// want of a renewal: the root's earliest reclaim
    /// ([`Core::deleg_lapse_at`]) and the recall that fetches the tail
    /// (a few round trips of the link to the root, at most one request
    /// timeout; a link not measured, the whole timeout).
    pub(crate) fn deleg_end_due(&self, d: &DelegateState) -> HonourMs {
        let rtt = self
            .holder_root()
            .and_then(|r| self.roster.links().get(&r))
            .and_then(|l| l.rtt_ms);
        let lapse = self.deleg_lapse_at(d).0;
        HonourMs(self.dl_t().deleg_end_due(lapse, rtt))
    }

    fn arm_deleg_give_up(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return;
        };
        let at = self.deleg_end_due(d).max(now.honour.plus(1));
        let old = self
            .dl
            .mine
            .get_mut(&gen)
            .expect("present")
            .lapse_timer
            .take();
        if let Some(t) = old {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer_honour(at, Timer::DelegLapse(gen), out);
        self.dl.mine.get_mut(&gen).expect("present").lapse_timer = Some(t);
    }

    /// The give-up point of a lapsed generation's parked ops: the root's
    /// end of the generation has passed and nothing ended it here, so the
    /// root is silent for good ([`Core::on_deleg_lapse`]).
    fn deleg_give_up_parks(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return;
        };
        if now.honour < self.deleg_end_due(d) {
            self.arm_deleg_give_up(now, gen, out);
            return;
        }
        tracing::warn!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            "the lapsed delegation outlived the root's reclaim: its parked ops take the lease path"
        );
        self.deleg_abort_stream_parks(now, gen, replica, out);
    }

    /// A lapsed generation asks for a renewal again every request
    /// timeout: an unanswered one (a dead root never answers, nor fails
    /// at the transport) has nothing else to bring it back.
    fn deleg_lapsed_retry(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return;
        };
        if !d.lapsed || d.stopped || d.renew_timer.is_some() {
            return;
        }
        let t = self.set_timer_honour(
            now.honour.plus(self.deleg_request_timeout_ms()),
            Timer::DelegRenew(gen),
            out,
        );
        self.dl.mine.get_mut(&gen).expect("present").renew_timer = Some(t);
    }

    /// What the off-core lane may renew while the core is stuck: one
    /// certificate per generation of this node's that executes and renews
    /// (not stopped, not a designation, granted a renewal before: the
    /// first grant is the core's to ask for, since the root may hold it,
    /// for the read delegations under the subtree, and a lane renewal it
    /// answers would extend an honour end of 0) with a root that is known
    /// and is not this node. The lane only *sends* from them
    /// ([`Core::on_deleg_renew_sent_off_core`]); nothing here is the grant,
    /// the backup or the stream head: the lane's renewal carries none of
    /// them ([`RenewFrom::OffCore`]).
    pub fn deleg_renew_certs(&self) -> Vec<DelegRenewCert> {
        self.deleg_renew_cert_iter().collect()
    }

    /// Whether [`Core::deleg_renew_certs`] would differ from `published`:
    /// compared in place, nothing allocated (the driver asks after every
    /// step, and a delegate's certificates change a few times a minute).
    pub fn deleg_renew_certs_differ(&self, published: &[DelegRenewCert]) -> bool {
        let mut now = self.deleg_renew_cert_iter();
        for p in published {
            if now.next().as_ref() != Some(p) {
                return true;
            }
        }
        now.next().is_some()
    }

    fn deleg_renew_cert_iter(&self) -> impl Iterator<Item = DelegRenewCert> + '_ {
        let root = self.holder_root();
        let timeout = self.deleg_request_timeout_ms();
        self.dl
            .mine
            .values()
            .filter(move |d| root.is_some() && !d.stopped && !d.non_stealable && d.until.0 > 0)
            .map(move |d| {
                let (next_due, period_ms) = Self::deleg_next_renewal(d, timeout);
                DelegRenewCert {
                    gen: d.gen,
                    root: root.expect("filtered"),
                    round: d.stream_round,
                    next_due,
                    period_ms,
                }
            })
    }

    /// When generation `d`'s next renewal falls due, and the period: a
    /// period after the latest renewal known to have gone out (the install,
    /// before the first), the period the core renews at (the request
    /// timeout before the first answer).
    fn deleg_next_renewal(d: &DelegateState, request_timeout_ms: u64) -> (HonourMs, u64) {
        let period = if d.renew_period_ms > 0 {
            d.renew_period_ms
        } else {
            request_timeout_ms
        };
        let base = if d.last_renew_sent.0 > 0 {
            d.last_renew_sent
        } else {
            d.installed
        };
        (base.plus(period), period)
    }

    /// The heartbeat thread was frozen from `from` to `until`
    /// ([`Event::OwnFreeze`]); the same freeze, grown, replaces the last.
    pub(crate) fn on_own_freeze(&mut self, from: WaitMs, until: WaitMs) {
        if until <= from {
            return;
        }
        let f = &mut self.dl.freezes;
        match f.last_mut() {
            Some((a, b)) if *a == from => *b = (*b).max(until),
            _ => f.push((from, until)),
        }
        if f.len() > FREEZES_KEPT {
            f.remove(0);
        }
    }

    /// When the renewal that would have saved generation `d`'s grant fell
    /// due: a period after the latest renewal *answered* (the install,
    /// before the first). Not [`Core::deleg_next_renewal`]'s, which the lane's
    /// unanswered registrations push out.
    fn deleg_loss_due(d: &DelegateState, request_timeout_ms: u64) -> HonourMs {
        let period = if d.renew_period_ms > 0 {
            d.renew_period_ms
        } else {
            request_timeout_ms
        };
        let base = if d.answered_sent.0 > 0 {
            d.answered_sent
        } else {
            d.installed
        };
        base.plus(period)
    }

    /// This node gave up its grant of `gen` (a lapse, a refusal or the log's
    /// end of the generation after the grant ran out): count the loss, and
    /// say what explains it, apart from the plain counters.
    ///
    /// - `lapses_during_freeze` if a freeze of the heartbeat thread covers
    ///   the time from the renewal's due time to the point the grant ran out
    ///   (`deleg_lapse_at`, the root's earliest reclaim): nothing in the
    ///   process could renew, so the loss is neither the lane's nor the
    ///   core's;
    /// - else, if `silent_allowed`, `lapses_root_silent` if a renewal sent
    ///   by the time the lane would have acted (a due time plus
    ///   [`FREEZE_REACTION_MS`](crate::deleg_lane::FREEZE_REACTION_MS), the
    ///   freeze's own rule) was never answered: the root, which answers
    ///   renewals from its core, did not answer within the grant. A renewal
    ///   sent later than that, or none, is the lane's or the core's failure
    ///   and is counted in neither.
    ///
    /// `(frozen, silent)`.
    fn deleg_note_loss(&mut self, gen: u64, silent_allowed: bool) -> (bool, bool) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return (false, false);
        };
        self.stats.deleg_grants_lost += 1;
        let due = Self::deleg_loss_due(d, self.deleg_request_timeout_ms());
        let lapse_point = self.deleg_lapse_at(d);
        // The freezes are stamps of the driver's wake clock (the wait
        // clock): the honour stamps here are read on it through the last
        // step's reading, for the statistic.
        if let Some(len) = crate::deleg_lane::freeze_explains(
            &self.dl.freezes,
            self.clk.wait_of_honour(due),
            self.clk.wait_of_honour(lapse_point),
        ) {
            self.stats.deleg_lapses_frozen += 1;
            self.stats.deleg_last_freeze_ms = len as u64;
            self.stats.deleg_last_freeze_gen = gen;
            return (true, false);
        }
        if !silent_allowed {
            return (false, false);
        }
        let horizon = self.reclaim_horizon_ms() as i64;
        let root = self.holder_root();
        let pending = d.renew.map(|(_, sent)| sent).into_iter().chain(
            self.dl
                .late
                .values()
                .filter(|l| l.gen == gen && l.kind == ReqKind::Renew && l.to == root)
                .map(|l| l.sent),
        );
        let in_time = |sent: HonourMs| {
            sent.0 <= due.0 + crate::deleg_lane::FREEZE_REACTION_MS
                && sent.0 + horizon >= lapse_point.0
        };
        if !pending.into_iter().any(in_time) {
            return (false, false);
        }
        self.stats.deleg_lapses_root_silent += 1;
        self.stats.deleg_last_silent_gen = gen;
        (false, true)
    }

    /// The off-core lane sent renewal `req` of `gen` (stream round `round`)
    /// to `to` at `sent` (this node's clock, read before the request was
    /// handed to the transport): its answer counts as a late answer does
    /// ([`LateReq`]), honoured from that send, and nothing else changes.
    ///
    /// The lane exists for a core stuck in a step (a priority metadata
    /// write waiting for the store's writer lock behind a slow journal
    /// write, a blocking log write) for longer than the renewal's slack:
    /// no renewal went out, and the root reclaimed a delegation that was
    /// alive. Integrity, the same inequality as an in-time renewal:
    ///
    /// 1. The request is registered here before it can be answered (the
    ///    lane sends this event on the urgent lane first, and the answer
    ///    comes in on it too), and `sent` is read before the send. The root
    ///    grants from its receipt (`granted ≥ sent`) and reclaims no
    ///    earlier than `granted + ttl + margin`; this node honours
    ///    `sent + ttl − margin` ([`Core::deleg_late_renewed`]): it stops at
    ///    least two margins before the root's earliest reclaim.
    /// 2. Only the core decides. `until`, the lapse, the parked ops, the
    ///    execution, the lock handoff the answer carries all go through
    ///    `on_deleg_renewed` → `deleg_late_renewed`, with its root and round
    ///    checks. A refusal on a lane request is dropped there, as any
    ///    late refusal: the log ends the generation.
    /// 3. The root's rules for granting are the same: no grant once a
    ///    recall or a reclaim began, its usability, its freshness hold. What
    ///    it *learns* from the request is narrower: the lane's renewal is
    ///    its own message ([`PeerMsg::DelegRenewOffCore`]) with no stream
    ///    head and no backup, and the root extends the grant and notes
    ///    neither. A head needs the core (`settle_tagged_writes`, and the
    ///    fast path executes rows while the core is stuck): the as-of time
    ///    of the root's lock cut and a new tenure's wait for the delegate's
    ///    head stay as the core's last renewal left them, so a lock barrier
    ///    under the subtree does not settle on a position that lacks rows
    ///    the delegate executed since (liveness only: it settles when the
    ///    core renews). The backup is the core's to name: a certificate a
    ///    step old must not unname the backup the core just chose.
    /// 4. The certificate may be older than the generation (a step that
    ///    stops it — a refusal, a strand — is running): at most one more
    ///    renewal per period reaches the root for a generation this node no
    ///    longer executes in. That costs liveness only; the generation
    ///    still ends through the log or a recall.
    ///
    /// Ignored unless the generation is still this node's, executing (not
    /// stopped, not a designation), in the round the lane read, and
    /// renewed with `to`.
    pub(crate) fn on_deleg_renew_sent_off_core(
        &mut self,
        gen: u64,
        round: u64,
        to: NodeId,
        req: OpId,
        sent: HonourMs,
    ) {
        if self.holder_root() != Some(to) {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.stopped || d.non_stealable || d.stream_round != round || d.until.0 == 0 {
            return;
        }
        d.last_renew_sent = d.last_renew_sent.max(sent);
        self.dl.late.insert(
            req,
            LateReq {
                gen,
                kind: ReqKind::Renew,
                to: Some(to),
                sent,
                off_core: false,
            },
        );
        self.stats.deleg_renewals_off_core += 1;
    }

    /// The requests whose late answers are still used (tests).
    #[cfg(test)]
    pub(crate) fn deleg_late_reqs(&self) -> Vec<OpId> {
        self.dl.late.keys().copied().collect()
    }

    /// Plan 30 §M14: this node's honoured end of generation `gen` as its
    /// delegate (`None`: not held, stopped, or never renewed).
    pub(crate) fn deleg_mine_until(&self, gen: u64) -> Option<HonourMs> {
        self.dl
            .mine
            .get(&gen)
            .filter(|d| !d.stopped && d.until.0 > 0)
            .map(|d| d.until)
    }

    /// The root a recall handed generation `gen`'s lock grants back to,
    /// while this node is still its delegate in the table.
    pub(crate) fn deleg_handed_back_to(&self, gen: u64) -> Option<NodeId> {
        self.dl
            .handed_back
            .get(&gen)
            .map(|(root, _, _)| *root)
            .filter(|n| *n != 0 && *n != self.cfg.node_id)
    }

    /// A grant never outlives the root lease's usable end (the cap).
    pub(crate) fn grant_cap_ms(&self, now: Now) -> u64 {
        if self.lease.held.is_none() {
            return 0;
        }
        if self.lease.epoch_held() {
            let ttl = self.cfg.delegation_ttl_ms;
            self.ack.note_hold_grant(now.wait.plus(ttl));
            return ttl;
        }
        // Plan 43 C7-H1 (R6): the lease's end on the honour clock; no
        // honour end (a body of an earlier incarnation): no cap.
        self.lease.honour_left_ms(now, &self.cfg).map_or(0, |left| {
            (left - self.cfg.expiry_margin_ms as i64).max(0) as u64
        })
    }

    /// The root's answer to a renewal. `false`: it is not from the root
    /// this generation renews with, in its round
    /// ([`DelegateState::stream_round`]), or the generation is not this
    /// node's — another root's grant, one answering this node's previous
    /// incarnation (whose request ids this one reuses), or a late one for
    /// an ended generation says nothing about this grant, and neither
    /// does the lock handoff it carries.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_renewed(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        ttl_ms: u64,
        ends: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let root = self.holder_root();
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?req,
            ttl_ms,
            in_flight = self.dl.by_req.contains_key(&req),
            "delegation renewal answer"
        );
        match self.dl.mine.get(&gen) {
            Some(d) if root == Some(from) && d.stream_round == round => {}
            // Not this node's any more (ended, or never installed here):
            // nothing to renew, and its lock handoff — the cut above all,
            // which would replace a newer root's — says nothing either.
            _ => {
                self.stats.deleg_renewed_stale += 1;
                return false;
            }
        }
        if ttl_ms > 0 {
            // The lock content of this answer is installed by the caller;
            // the generation's first grant in this round has been the core's
            // ([`DelegateState::core_granted_round`]).
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                d.core_granted_round = Some(round);
            }
        }
        if !self.dl.by_req.contains_key(&req) {
            // The core's verdict on a request the root's off-core answer
            // completed: a refusal still ends the generation here, as it
            // would have in time (the answer off the core is a grant of
            // what the lease allowed, not a change of the root's mind).
            if ttl_ms == 0
                && self
                    .dl
                    .late
                    .get(&req)
                    .is_some_and(|l| l.off_core && l.gen == gen && l.kind == ReqKind::Renew)
            {
                self.dl.late.remove(&req);
                if self.dl.mine.get(&gen).is_some_and(|d| !d.stopped) {
                    self.deleg_renewal_refused(now, gen, ends, replica, out);
                }
                return true;
            }
            self.deleg_late_renewed(now, req, gen, ttl_ms, replica, out);
            return true;
        }
        let Some((g, ReqKind::Renew)) = self.dl.by_req.remove(&req) else {
            return true;
        };
        if g != gen {
            return true;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return true;
        };
        let Some((_, sent)) = d.renew.take() else {
            return true;
        };
        if ttl_ms > 0 {
            d.core_answer_sent = Some(sent);
        }
        if let Some(t) = d.renew_timer.take() {
            // (A lapsed generation's retry.)
            self.cancel_timer(t, out);
        }
        if !self.dl.mine.contains_key(&gen) {
            return true;
        }
        if ttl_ms == 0 {
            self.deleg_renewal_refused(now, gen, ends, replica, out);
            return true;
        }
        self.deleg_granted(now, gen, sent, ttl_ms, replica, out);
        true
    }

    /// The root's core refused a renewal of `gen`: the generation is ending;
    /// stop executing (the log ends it), keep the parked ops for `NotHolder`
    /// then. A root that reclaimed a grant this node's frozen process could
    /// not renew refuses the renewal it sends on thawing, before the lapse
    /// watch is handled: counted as a loss in a freeze too.
    fn deleg_renewal_refused(
        &mut self,
        now: Now,
        gen: u64,
        ends: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return;
        };
        self.stats.deleg_renewals_refused += 1;
        // A non-stealable grant is not ended by a renewal's answer that only
        // says the root cannot grant now (it is not usable, its window is
        // closed: `ends` false): the root never takes it back by itself (a
        // sticky grant ends by its holder's release, plan 46 §5.4). The
        // holder goes on; a stream it stopped here was never released, and
        // its subtree stayed the root's to refuse for good (plan 46 M1.3
        // step 5). A usable root that says the generation is not served
        // (`ends`: an operator's abandon, a recall, the end of the stream)
        // stops it, as for any other.
        if d.non_stealable && !ends {
            return;
        }
        let ran_out = !d.lapsed && now.honour >= self.deleg_lapse_at(d);
        if ran_out {
            // (The root answered: it cannot have been silent.)
            self.deleg_note_loss(gen, false);
        }
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.stopped = true;
        let parked = std::mem::take(&mut d.parked);
        let root = self.holder_root().unwrap_or(0);
        for p in parked {
            self.answer_not_owner(now, p, root, replica, out);
        }
    }

    /// A granting answer to the renewal of `gen` that this node sent at
    /// `sent`: honour the grant from the send, renew again at half the ttl
    /// (a quarter with lock grants out), re-arm the lapse watch. The root's
    /// core's answer, or its P2P service's off-core one
    /// ([`Core::on_deleg_renewed_off_core`]).
    fn deleg_granted(
        &mut self,
        now: Now,
        gen: u64,
        sent: HonourMs,
        ttl_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.deleg_note_held_here(now, gen, replica) {
            return;
        }
        let d = self.dl.mine.get_mut(&gen).expect("present");
        // Measured from the send (M8's discipline): honoured until
        // `sent + ttl − margin` on this clock.
        let until = sent.plus(ttl_ms.saturating_sub(self.cfg.expiry_margin_ms));
        if until > d.until {
            d.until = until;
        }
        d.last_renew_sent = d.last_renew_sent.max(sent);
        d.answered_sent = d.answered_sent.max(sent);
        if d.lapsed && now.honour < d.until {
            // The link came back before the root reclaimed: the grant is
            // honoured again.
            tracing::info!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                "a lapsed delegation was renewed"
            );
            d.lapsed = false;
        }
        // Renew at half the ttl — at a quarter while this generation has
        // lock grants out: they are capped by what is left of it, and a
        // grant needs at least `2 × margin` of it (`lock_min_grant_ms`).
        let at = if replica.locks().has_grants_of_gen(gen) {
            sent.plus(ttl_ms / 4)
        } else {
            sent.plus(ttl_ms / 2)
        };
        let t = self.set_timer_honour(at.max(now.honour.plus(1)), Timer::DelegRenew(gen), out);
        {
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.renew_timer = Some(t);
            d.renew_period_ms = at.since(sent).max(1) as u64;
        }
        self.arm_deleg_lapse(now, gen, out);
        let d = self.dl.mine.get_mut(&gen).expect("present");
        // The root granted the renewal: it serves this generation now. A
        // stream refusal just before it (a root restarted or re-adopting
        // after a slow lease read, not yet knowing the generation) left the
        // stream backing off for up to two seconds, so rows the root could
        // take now waited for the back-off, and an op awaiting their commit
        // ran out its settle first (`sticky-ttl` with slow lease reads,
        // seeds 532, 1705). The batch goes now.
        let resume_stream = d.inflight.is_none() && d.stream_after.is_some();
        if resume_stream {
            d.stream_after = None;
            d.stream_backoff_ms = 0;
            d.refusal_backoff_ms = 0;
        }
        if d.group_requested.is_some() && d.renew_backup != Self::deleg_requested_backup(d) {
            // Plan 43 C2: the root journals the group change when it learns
            // the request — tell it now, not at the next timer.
            self.deleg_renew_now(now, gen, out);
        }
        if resume_stream {
            self.deleg_stream(now, replica, out);
        }
        self.deleg_after_event(now, replica, out);
    }

    /// Plan 43 C2e follow-up: before the first grant of `gen` this process
    /// uses, it records (synced) that it holds the generation on this boot,
    /// so that after a reboot it knows its journal may lack rows it
    /// acknowledged (`delegation_sync`). `false`: not recorded; the grant is
    /// not used and the generation stops here (the root ends it by its
    /// group). A designee needs none (it renews nothing).
    fn deleg_note_held_here(&mut self, now: Now, gen: u64, replica: &dyn Replica) -> bool {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return false;
        };
        if d.held_noted || d.non_stealable {
            return true;
        }
        if replica.deleg_note_held(gen, now.boot.id) {
            d.held_noted = true;
            return true;
        }
        d.stopped = true;
        tracing::warn!(
            node = self.cfg.node_id,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            "could not record the delegation's grant: not used, the generation stops here"
        );
        false
    }

    /// The root's P2P service answered renewal `req` of `gen` off its core
    /// (plan 41 R2, [`PeerMsg::DelegRenewedOffCore`]): a grant of `ttl_ms`
    /// with no lock content. Applied only as an extension of a grant this
    /// node already serves — the honoured end from the request's own send,
    /// as a late answer is ([`LateReq`]) — and never as a first grant: no
    /// lock install, cut, floor, grace or barrier (the root's core sends
    /// them with its own answer, which still comes and is counted as a late
    /// one here). Ignored unless the generation is still this node's,
    /// executing and serving (the root's core granted it in this round:
    /// [`DelegateState::core_granted_round`], that answer carried the
    /// handoff), in the round the answer names, from the root it
    /// renews with, and the request is one this node sent (its own in
    /// flight, or a lane's registered one).
    ///
    /// The core's own request that this answers is then complete as far as
    /// the schedule goes: the next renewal is armed from its send as for
    /// any granting answer (a root stuck for longer than the grant would
    /// otherwise have this node wait for the full answer, and lapse, before
    /// it asked again). The full answer, when the root's core runs, is a
    /// late answer: its extension is a no-op and its lock content is
    /// installed as ever.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_deleg_renewed_off_core(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        ttl_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let root = self.holder_root();
        let serving = self.deleg_mine_until(gen).is_some();
        let sent = match self.dl.mine.get(&gen) {
            Some(d)
                if ttl_ms > 0
                    && serving
                    && !d.non_stealable
                    && root == Some(from)
                    && d.stream_round == round
                    && d.core_granted_round == Some(round) =>
            {
                match d.renew {
                    Some((r, sent)) if r == req => Some((sent, true)),
                    _ => self
                        .dl
                        .late
                        .get(&req)
                        .filter(|l| l.gen == gen && l.kind == ReqKind::Renew && l.to == Some(from))
                        .map(|l| (l.sent, false)),
                }
            }
            _ => None,
        };
        let Some((sent, in_flight)) = sent else {
            self.stats.deleg_renewed_stale += 1;
            return;
        };
        self.stats.deleg_off_core_answers_applied += 1;
        if !in_flight {
            // A lane's request, or one the core gave up waiting for: a late
            // answer, extension only; its full answer, if it comes, is
            // counted as late too.
            self.deleg_extend_late(now, gen, sent, ttl_ms, replica, out);
            return;
        }
        // The core's own request in flight: complete for the schedule, its
        // full answer is a late one from here.
        self.dl.by_req.remove(&req);
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.renew = None;
        if let Some(t) = d.renew_timer.take() {
            self.cancel_timer(t, out);
        }
        self.dl.late.insert(
            req,
            LateReq {
                gen,
                kind: ReqKind::Renew,
                to: Some(from),
                sent,
                off_core: true,
            },
        );
        self.deleg_granted(now, gen, sent, ttl_ms, replica, out);
    }

    /// The answer to a renewal [`Core::deleg_expire_inflight`] stopped
    /// waiting for ([`LateReq`]): granted, the grant is honoured from that
    /// renewal's send, as an answer in time would have been (the root
    /// counts from its receipt, which is later). A refusal is left to the
    /// renewal in flight, or the next: the log ends a generation.
    fn deleg_late_renewed(
        &mut self,
        now: Now,
        req: OpId,
        gen: u64,
        ttl_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let root = self.holder_root();
        let Some(late) = self.deleg_take_late(req, ReqKind::Renew, gen, None) else {
            return;
        };
        if ttl_ms == 0 || late.to.is_none() || late.to != root {
            return;
        }
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.core_answer_sent = Some(late.sent);
        }
        self.deleg_extend_late(now, gen, late.sent, ttl_ms, replica, out);
    }

    /// A granted renewal sent at `sent` that did not count in time (a late
    /// answer, the lane's, an off-core one): the grant is honoured from that
    /// send, as an answer in time would have been. Extension only.
    fn deleg_extend_late(
        &mut self,
        now: Now,
        gen: u64,
        sent: HonourMs,
        ttl_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let margin = self.cfg.expiry_margin_ms;
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.stopped {
            return;
        }
        let until = sent.plus(ttl_ms.saturating_sub(margin));
        if until <= d.until {
            return;
        }
        if !self.deleg_note_held_here(now, gen, replica) {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        tracing::debug!(
            node = self.cfg.node_id,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            late_ms = now.honour.since(sent),
            "a late delegation renewal counted"
        );
        d.until = until;
        d.last_renew_sent = d.last_renew_sent.max(sent);
        d.answered_sent = d.answered_sent.max(sent);
        if d.lapsed && now.honour < d.until {
            tracing::info!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                "a lapsed delegation was renewed"
            );
            d.lapsed = false;
        }
        self.arm_deleg_lapse(now, gen, out);
        self.deleg_after_event(now, replica, out);
    }

    // ----------------------------------------------------- recalls (delegate)

    /// Plan 46 M1.2 step 10: the root asks this node to release `dirs` of
    /// stream `gen` (every subtree it holds: a recall of the stream). This
    /// node stops executing under them, hands their lock grants back, and
    /// journals the release as the next row of its stream:
    /// `SubtreeReleased { dir, at }` per subtree, or one `StreamReleased`
    /// when nothing is left of its set. The row is behind everything
    /// executed under the subtrees (`at` is the stream's index then), and
    /// the root sequences them only after it has appended it, so no row of
    /// the subtree is in the log after the root's own. The other subtrees of
    /// the stream stay live. Idempotent: the root sends the request again
    /// while it has not seen the row (L3).
    ///
    /// A stream that cannot release by a row (one of its rows depends on a
    /// row its generation lost, `deleg_find_lost`; the stream is past what
    /// this node's own holder tenure has committed, `holder_append_cap`; the
    /// generation was never installed here) answers with the cut it ends at
    /// instead (`ReleaseNote::end_at`): a recall is always safe.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_release_request(
        &mut self,
        now: Now,
        from: NodeId,
        _req: OpId,
        gen: u64,
        dirs: Vec<Ino>,
        why: ReleaseWhy,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Simulation (L3): the first release request of every Nth
        // generation is lost on the way in; the root's repeated request is
        // answered.
        if self.dl.release_first.insert(gen) {
            self.stats.deleg_recalls_first += 1;
            let every = self.cfg.delegation_recalled_drop_every;
            if every > 0 && self.stats.deleg_recalls_first.is_multiple_of(every) {
                self.stats.deleg_recalleds_dropped += 1;
                return;
            }
        }
        // ADR-56: a handover is a request the holder answers when it is
        // quiet; what it does not cover (a placed grant) is a recall.
        let dirs = if why == ReleaseWhy::Handover {
            match self.on_handover_request(now, from, gen, dirs, replica, out) {
                Some(rest) if !rest.is_empty() => rest,
                _ => return,
            }
        } else {
            dirs
        };
        self.release_subtrees(now, from, gen, dirs, replica, out);
    }

    /// The release itself (see [`Self::on_release_request`]): `from` is the
    /// root the note goes to. Also the holder's own, once it has drained
    /// ([`Self::sticky_release_progress`]).
    #[allow(clippy::too_many_arguments)]
    fn release_subtrees(
        &mut self,
        now: Now,
        from: NodeId,
        gen: u64,
        dirs: Vec<Ino>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.me();
        let asked: BTreeSet<Ino> = dirs.into_iter().collect();
        let Some(d) = self.dl.mine.get(&gen) else {
            // Never installed here (the grant was parked, or not committed
            // when the root took it back): never installed afterwards. The
            // stream is the root's to end at the rows this node holds of it.
            let seen = replica.delegation_table().stream(me, gen).is_some();
            if self
                .dl
                .handed_back
                .insert(gen, (from, now.wait, seen))
                .is_none()
            {
                self.stats.deleg_release_rowless_unseen += 1;
            }
            let idx = replica.delegate_idx(gen);
            let cut = idx.min(self.holder_append_cap(gen, replica));
            // A stream this node never took up (on this boot) will not be
            // streamed by it: the rows it holds of it sit at its journal's end
            // and never reach the root's cursor, and the group may hold more
            // than the journal does. So the cut is exact only when this node
            // holds no row of the generation at all (`idx == 0`) and never
            // executed under it on another boot (`deleg_note_held` syncs the
            // record before the first row, so no record means no row was
            // executed). Anything else asks for the seal.
            let seal = idx > 0 || replica.deleg_held_on_another_boot(gen, now.boot.id);
            if seal {
                self.stats.deleg_release_rowless_unseen_sealed += 1;
            }
            let note = constellation_meta::locks::ReleaseNote {
                dirs: asked.iter().copied().collect(),
                end_at: Some(cut),
                locks: Default::default(),
                leaving: self.dl.leave_self,
                seal,
                no_stream: seal,
            };
            self.send_release_cut(gen, from, note, out);
            self.lock_reserve_all(now, replica, out);
            return;
        };
        // What this node holds under the stream: honoured and parked
        // subtrees alike, and any the table lists for it that it has not
        // learned yet.
        let mut held: BTreeSet<Ino> = d.dirs.iter().chain(d.grown.keys()).copied().collect();
        held.extend(
            replica
                .delegation_table()
                .iter()
                .filter(|t| t.gen == gen && t.node == me)
                .map(|t| t.dir),
        );
        let asked: BTreeSet<Ino> = if asked.is_empty() {
            held.clone()
        } else {
            asked
        };
        // A subtree this node has not learned yet waits for its record
        // (`release_pending`); the rest is released now.
        // A subtree this node released is not one it has yet to learn (the
        // request for it is repeated, or came late): unless the stream was
        // granted it again, which shows once the node holds it.
        {
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.released_dirs.retain(|x| !held.contains(x));
        }
        let d = self.dl.mine.get(&gen).expect("present");
        let unknown: BTreeSet<Ino> = asked
            .difference(&held)
            .filter(|x| !d.released_dirs.contains(x))
            .copied()
            .collect();
        let asked: BTreeSet<Ino> = asked.intersection(&held).copied().collect();
        // A repeated request for what is released already: the batch that
        // carries the note goes out again.
        let repeated = d.release.as_ref().and_then(|r| {
            asked
                .iter()
                .all(|x| r.note.dirs.contains(x))
                .then(|| (r.row.is_none(), r.held, r.note_at(now.wait)))
        });
        if !unknown.is_empty() {
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.release_pending.0.extend(unknown);
            d.release_pending.1 = from;
        }
        if let Some((rowless, held, note)) = repeated {
            {
                // (The request is sent again because the root has not seen
                // the row: a batch in flight is taken as lost.)
                if rowless {
                    if !held {
                        self.send_release_cut(gen, from, note, out);
                    }
                    return;
                }
                let d = self.dl.mine.get_mut(&gen).expect("present");
                if let Some((r, _)) = d.inflight.take() {
                    self.dl.by_req.remove(&r);
                }
                let d = self.dl.mine.get_mut(&gen).expect("present");
                d.stream_after = None;
                d.stream_backoff_ms = 0;
                d.refusal_backoff_ms = 0;
                self.deleg_stream(now, replica, out);
                return;
            }
        }
        if asked.is_empty() {
            return;
        }
        let whole = held.iter().all(|x| asked.contains(x));
        // The stream's counter is what its rows are: a generation this
        // node's takeover gate ended has the log's index of it (the rows
        // the gate re-appended counted, the others replayed by rid as root
        // rows, `Meta::end_own_streams_at_log`): no hole. Not past this
        // node's own holder appends its tenure has not committed (a
        // takeover gate's re-append, unshipped, perhaps never: a node
        // deposed before its gate's segment ships): the next root's log
        // would lack a row the cut covers. Not past a row whose dependency
        // was lost (`deleg_stream`, or the journal read here): it is never
        // streamed, and the root ends the generation before it.
        let idx = replica.delegate_idx(gen);
        let mut cut = idx.min(self.holder_append_cap(gen, replica));
        let capped = cut < idx;
        if let Some(lost) = self.deleg_find_lost(gen, replica) {
            cut = cut.min(lost - 1);
        }
        let rebooted = self.dl.mine.get(&gen).is_some_and(|d| d.rebooted);
        let rowless = cut < idx || self.dl.mine.get(&gen).is_some_and(|d| d.stopped);
        tracing::debug!(
            node = me,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?asked,
            ?held,
            whole,
            rowless,
            idx,
            cut,
            "releasing subtrees"
        );
        // Why the release is rowless, and whether its cut needs the group's
        // seal (plan 46 M1.2 step 14): rows above a cut capped below the
        // delegate's holder appends, or above what a stream it restarted
        // without re-sent, may be acknowledged by the group. A lost
        // dependency or a plain stop cuts exactly.
        let seal = rowless && (capped || rebooted);
        if rowless {
            if capped {
                self.stats.deleg_release_rowless_capped += 1;
            } else if rebooted {
                self.stats.deleg_release_rowless_rebooted += 1;
            } else if cut < idx {
                self.stats.deleg_release_rowless_lost_dep += 1;
            } else {
                self.stats.deleg_release_rowless_plain += 1;
            }
        }
        // Without a row the stream ends at the cut, every subtree with it:
        // the whole stream is handed back, its locks too (the others'
        // holders keep honouring them until the root has them).
        if rowless && !whole {
            self.stats.deleg_release_rowless_partial += 1;
        }
        let (asked, whole) = if rowless {
            (held.clone(), true)
        } else {
            (asked, whole)
        };
        let mut row = None;
        if !rowless {
            let records: Vec<LogRecord> = if whole {
                vec![LogRecord::StreamReleased {
                    node: me,
                    gen,
                    through: idx,
                }]
            } else {
                asked
                    .iter()
                    .map(|dir| LogRecord::SubtreeReleased {
                        node: me,
                        gen,
                        dir: *dir,
                        at: idx,
                    })
                    .collect()
            };
            // The row is stamped with the configuration in force when it is
            // journaled (the version this replica has applied now); nobody is
            // answered by it, so it is the stream's rule for a remote asker.
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                Self::deleg_group_refresh_one(d, replica);
            }
            let rule = self.deleg_rule(gen, Asker::Remote);
            let _row_class =
                constellation_meta::PendingClass::set(self.deleg_row_commit(gen, rule));
            match replica.delegate_release(&records, gen, Position::ZERO) {
                Ok(i) => row = Some(i),
                Err(error) => {
                    // The root's repeated request tries again.
                    tracing::warn!(node = me, gen = %constellation_meta::delegation::GenLabel::of(gen), %error, "could not journal a release");
                    return;
                }
            }
        }
        self.stats.deleg_recalls_received += 1;
        // The subtrees' lock grants go back with the note; their waiters are
        // told to ask the root.
        let asked_list: Vec<Ino> = asked.iter().copied().collect();
        let mut locks = self.lock_hand_back(now, gen, &asked_list, whole, replica);
        // Each grant under the row that hands it back (the cut without one):
        // the root tells a later release of a subtree granted to the stream
        // again from a re-sent copy of this note by it.
        let at = row.unwrap_or(cut);
        locks.rows = locks.grants.iter().map(|g| (g.id, at)).collect();
        if whole {
            self.dl.handed_back.insert(
                gen,
                (
                    from,
                    now.wait,
                    replica.delegation_table().stream(me, gen).is_some(),
                ),
            );
        }
        let note = constellation_meta::locks::ReleaseNote {
            dirs: asked_list.clone(),
            end_at: rowless.then_some(cut),
            locks,
            leaving: self.dl.leave_self,
            seal,
            no_stream: false,
        };
        let parked = {
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.dirs.retain(|x| !asked.contains(x));
            d.grown.retain(|x, _| !asked.contains(x));
            if whole || rowless {
                d.stopped = true;
                d.stream_after = None;
                d.stream_backoff_ms = 0;
                d.refusal_backoff_ms = 0;
            }
            d.released_dirs.extend(asked.iter().copied());
            let release = PendingRelease {
                note,
                row,
                first_row: row,
                held: false,
                holds: 0,
                left_at: now.wait,
                to: from,
                ends: whole || rowless,
            };
            // A note the root has not acknowledged is never dropped for
            // this one: the two are one.
            d.release = Some(match d.release.take() {
                Some(old) => {
                    self.stats.deleg_release_merged += 1;
                    old.merge(release, now.wait)
                }
                None => release,
            });
            std::mem::take(&mut d.parked)
        };
        // Ops parked here for a subtree that is no longer this node's go to
        // the root; the others stay parked.
        let mut keep = Vec::new();
        for p in parked {
            let keys = super::holder::keys_of_op_in(&p.op, replica);
            let mine = !whole
                && matches!(
                    replica.resolve_ownership(&keys),
                    Ownership::Delegated(t) if t.gen == gen && t.node == me
                );
            if mine {
                keep.push(p);
            } else {
                self.answer_not_owner(now, p, from, replica, out);
            }
        }
        // The release row goes out at once: a batch in flight (its answer may
        // be late, or lost) is taken as given up, the root's request is
        // waiting for the row.
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.parked = keep;
            if let Some((r, _)) = d.inflight.take() {
                self.dl.by_req.remove(&r);
            }
            let d = self.dl.mine.get_mut(&gen).expect("present");
            d.stream_after = None;
            d.stream_backoff_ms = 0;
            d.refusal_backoff_ms = 0;
            d.refused = false;
        }
        // Phase 2b (M8): the read delegations this delegate granted under
        // the released subtrees are recalled first; the root executes under
        // them only once they are gone (or, past the horizon, expired).
        // The release row and what follows it are held back from the
        // stream until then.
        if self.cfg.read_delegations {
            let mut need = replica.read_delegations().all_live(now.wait);
            if !whole {
                let ns = replica.namespace();
                need.grants
                    .retain(|g| asked.iter().any(|dir| is_under(ns, g.ino, *dir)));
            }
            // ADR-46: the read grants of a previous process are not in the
            // table; the release row waits for them to expire too.
            if let Some(q) = self
                .dl
                .mine
                .get(&gen)
                .and_then(|d| d.restart_q.as_ref())
                .filter(|q| q.timer.is_some() && !q.voided && q.until > now.wait)
            {
                need.quarantine_until =
                    Some(need.quarantine_until.map_or(q.until, |u| u.max(q.until)));
            }
            if let Some(wait) = self.start_recalls(now, need, out) {
                if let Some(r) = self.dl.mine.get_mut(&gen).and_then(|d| d.release.as_mut()) {
                    r.held = true;
                    r.holds += 1;
                }
                self.park(
                    now,
                    wait,
                    None,
                    super::readindex::ParkedWhat::ReleaseHeld { gen },
                );
            }
        }
        if rowless {
            let note = self
                .dl
                .mine
                .get(&gen)
                .and_then(|d| d.release.as_ref())
                .filter(|r| !r.held)
                .map(|r| r.note_at(now.wait));
            if let Some(note) = note {
                self.send_release_cut(gen, from, note, out);
            }
        }
        self.deleg_stream(now, replica, out);
        self.lock_reserve_all(now, replica, out);
    }

    /// A release that journaled no row (`ReleaseNote::end_at`) says so to the
    /// root that asked, in a stream batch of no rows: the stream ends at the
    /// cut, which is the root's to journal (`DelegateNodeEnded`).
    fn send_release_cut(
        &mut self,
        gen: u64,
        to: NodeId,
        note: constellation_meta::locks::ReleaseNote,
        out: &mut Vec<Action>,
    ) {
        let req = self.op_id();
        let round = self.dl.mine.get(&gen).map_or(0, |d| d.stream_round);
        out.push(Action::Send {
            to,
            msg: PeerMsg::DelegateStream {
                req,
                gen,
                round,
                txs: Vec::new(),
                leaving: Vec::new(),
                leaving_barriers: Vec::new(),
                released: Some(note),
            },
        });
    }

    /// The read delegations a release waited for are gone: its row streams.
    pub(crate) fn deleg_release_unheld(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let cut = self
            .dl
            .mine
            .get_mut(&gen)
            .and_then(|d| d.release.as_mut())
            .and_then(|r| {
                r.holds = r.holds.saturating_sub(1);
                if r.holds > 0 {
                    return None;
                }
                r.held = false;
                r.row.is_none().then(|| (r.to, r.note_at(now.wait)))
            });
        if let Some((to, note)) = cut {
            self.send_release_cut(gen, to, note, out);
        }
        self.deleg_stream(now, replica, out);
    }

    // ----------------------------------------------------- reads (M8)

    /// Phase 2b: the keys a read of `ino` (and `name` under it) touches,
    /// for ownership.
    /// The keys a strict read touches, keyed like the writes that change
    /// them (`keys_of_op`): a lookup is its dentry (the directory's own
    /// inode resolves by *its* parent, which would make every lookup in
    /// a delegated directory cross-subtree); an inode read is the inode.
    pub(crate) fn read_keys(ino: Ino, name: Option<&str>) -> TouchSet {
        let mut keys = TouchSet::default();
        match name {
            Some(n) => {
                keys.dentries.insert((ino, n.to_string()));
            }
            None => {
                keys.inos.insert(ino);
            }
        }
        keys
    }

    /// At start: where the read grants the previous process's delegates made
    /// may last to (ADR-46). A missing or unreadable key is a full span.
    ///
    /// The key is settled (`none`) as soon as nothing of the previous
    /// process's grants is left, installed generation or not: at once when
    /// the load found none, from a timer at the horizon otherwise. A node
    /// that rarely grants would else keep a passed stamp (or no key), and
    /// its next crash and reboot would pay a full span for grants nobody
    /// made.
    pub(crate) fn deleg_restart_start(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        use constellation_meta::readdeleg::DelegHorizon;
        let span = self.dl_t().read_span as i64;
        let horizon = match replica.load_deleg_grant_quarantine(now, span) {
            DelegHorizon::None => {
                replica.settle_deleg_grant_horizon(now);
                None
            }
            DelegHorizon::Until(t) => Some(t).filter(|t| *t > now.wait),
            DelegHorizon::Unknown => {
                tracing::warn!(
                    node = self.cfg.node_id,
                    wait_ms = span,
                    "the delegate read-grant horizon is missing or unreadable; \
                     waiting a full read TTL before executing"
                );
                Some(now.wait.plus(span.max(0) as u64))
            }
        };
        self.dl.restart_horizon = horizon;
        if let Some(t) = horizon {
            let id = self.set_timer_wait(t, Timer::DelegRestartQuarantine(0), out);
            self.dl.restart_settle_timer = Some(id);
        }
    }

    /// Whether `gen` is inside its restart quarantine: installed while the
    /// previous process's read grants may be live, and `until` has not
    /// passed (ADR-46).
    pub(crate) fn deleg_restart_quarantined(&self, now: Now, gen: u64) -> bool {
        self.dl
            .mine
            .get(&gen)
            .and_then(|d| d.restart_q.as_ref())
            .is_some_and(|q| !q.voided && now.wait < q.until)
    }

    /// The nodes that may hold a read delegation of this node's delegates,
    /// or `None` while that set is not known. The early exit of a
    /// restart quarantine and the abandon of a stream end a wait because
    /// every node of this set has dropped its grants, so the set is a
    /// safety boundary: it must contain every node that can hold a grant
    /// of the previous process, read-only members included.
    ///
    /// Invariant relied on: the peer directory behind `links` was filled
    /// by a registry scan that read every record without an error
    /// (`Event::Peers::registry_complete`), so it lists every live node,
    /// read-only ones too, and a node enrolled later cannot hold a grant
    /// of a process that is already gone. The roster (write-eligible
    /// nodes) alone never names read-only members. The set is the union
    /// of the roster and the links seen since that first complete
    /// snapshot: a later scan that lost a record cannot shrink it. A
    /// delegate grants a read delegation only to a member of this set
    /// (`deleg_read_index`), so every holder of a grant is asked, and a
    /// reader the set does not yet name reads through. Until the roster and
    /// such a snapshot have arrived the set is not known (and an empty
    /// one would read as "nobody holds any"); then the horizon decides.
    /// This is the one place that builds the audience.
    pub(crate) fn void_audience(&self) -> Option<BTreeSet<NodeId>> {
        self.roster.audience(self.cfg.node_id)
    }

    /// The early exit of the restart quarantine (ADR-46, D6): every
    /// quarantined generation asks every node that may hold a read
    /// delegation of its previous process to drop them
    /// ([`PeerMsg::DelegReadVoid`], scope `Restart`). Once all answered, no
    /// reader honours a grant of the dead process, and the quarantine is
    /// over before its horizon. A node that does not answer (down, cut off)
    /// leaves the horizon to decide, as before. Runs at install and at the
    /// first event that completes the audience; a generation is asked once.
    pub(crate) fn deleg_restart_void_round(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.read_delegations {
            return;
        }
        let todo: Vec<u64> = self
            .dl
            .mine
            .iter()
            .filter(|(g, d)| {
                d.restart_q
                    .as_ref()
                    .is_some_and(|q| !q.round_sent && !q.voided)
                    && self.deleg_restart_quarantined(now, **g)
            })
            .map(|(g, _)| *g)
            .collect();
        if todo.is_empty() {
            return;
        }
        let Some(audience) = self.void_audience() else {
            return;
        };
        for gen in todo {
            tracing::info!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                nodes = ?audience,
                "asking every node to drop the read delegations this node's previous process granted"
            );
            let mut asked = BTreeMap::new();
            for p in &audience {
                let req = self.op_id();
                asked.insert(req, *p);
                out.push(Action::Send {
                    to: *p,
                    msg: PeerMsg::DelegReadVoid {
                        req,
                        gen,
                        scope: crate::event::VoidScope::Restart,
                    },
                });
            }
            let done = asked.is_empty();
            if let Some(q) = self
                .dl
                .mine
                .get_mut(&gen)
                .and_then(|d| d.restart_q.as_mut())
            {
                q.asked = asked;
                q.round_sent = true;
            }
            if done {
                self.deleg_restart_voided(now, gen, replica, out);
            }
        }
    }

    /// `req` was a void request of the early exit: it is answered. Whether
    /// it was.
    pub(crate) fn deleg_restart_void_acked(&mut self, req: OpId) -> bool {
        let Some(gen) = self.dl.mine.iter_mut().find_map(|(g, d)| {
            d.restart_q
                .as_mut()
                .and_then(|q| q.asked.remove(&req))
                .map(|_| *g)
        }) else {
            return false;
        };
        let all = self
            .dl
            .mine
            .get(&gen)
            .and_then(|d| d.restart_q.as_ref())
            .is_some_and(|q| q.round_sent && q.asked.is_empty());
        if all {
            self.deleg_restart_voided_flag(gen);
        }
        true
    }

    fn deleg_restart_voided_flag(&mut self, gen: u64) {
        if let Some(q) = self
            .dl
            .mine
            .get_mut(&gen)
            .and_then(|d| d.restart_q.as_mut())
            .filter(|q| !q.voided)
        {
            q.voided = true;
            self.stats.deleg_restart_q_voided += 1;
            tracing::info!(
                node = self.cfg.node_id,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                "every node dropped the read delegations this node's previous process \
                 granted; its writes run"
            );
        }
    }

    fn deleg_restart_voided(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.deleg_restart_voided_flag(gen);
        self.complete_ready(now, replica, out);
    }

    /// A restart quarantine timer fired. `gen` 0 is the process-level one:
    /// it settles the persisted key at the horizon. Otherwise the generation's
    /// quarantine is over and what it parked runs. A timer that fires before
    /// its time (the event carries a stale clock, M1.3 step 11) arms itself
    /// again: the generation must not stay stopped for the rest of its life.
    pub(crate) fn on_deleg_restart_quarantine(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if gen == 0 {
            self.dl.restart_settle_timer = None;
            if let Some(h) = self.dl.restart_horizon {
                if now.wait < h {
                    let id = self.set_timer_wait(h, Timer::DelegRestartQuarantine(0), out);
                    self.dl.restart_settle_timer = Some(id);
                } else {
                    replica.settle_deleg_grant_horizon(now);
                }
            }
            return;
        }
        let early = self
            .dl
            .mine
            .get(&gen)
            .and_then(|d| d.restart_q.as_ref())
            .filter(|q| q.timer.is_some() && !q.voided && now.wait < q.until)
            .map(|q| q.until);
        if let Some(until) = early {
            let id = self.set_timer_wait(until, Timer::DelegRestartQuarantine(gen), out);
            if let Some(q) = self
                .dl
                .mine
                .get_mut(&gen)
                .and_then(|d| d.restart_q.as_mut())
            {
                q.timer = Some(id);
            }
            return;
        }
        if let Some(q) = self
            .dl
            .mine
            .get_mut(&gen)
            .and_then(|d| d.restart_q.as_mut())
            .filter(|q| q.timer.is_some() && now.wait >= q.until)
        {
            q.timer = None;
            if !q.voided {
                self.stats.deleg_restart_q_waited_out += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    "the read delegations this node's previous process granted have expired; \
                     its writes run"
                );
            }
        }
        if self.dl.restart_horizon.is_some_and(|h| now.wait >= h) {
            replica.settle_deleg_grant_horizon(now);
        }
        // The parked ops run from `deleg_after_event`; a release row held
        // for the quarantine is completed here.
        self.complete_ready(now, replica, out);
    }

    /// Phase 2b: answer a strict read for this delegate's subtree, if it
    /// is: the position is this replica's plus the stream index, and a
    /// read delegation capped by the grant.
    pub(crate) fn deleg_read_index(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        name: Option<&str>,
        replica: &dyn Replica,
    ) -> Option<crate::event::ReadIndexOutcome> {
        if !self.cfg.delegation || self.dl.mine.is_empty() {
            return None;
        }
        let keys = Self::read_keys(ino, name);
        let gen = self.my_generation_for(&keys, replica)?;
        let d = self.dl.mine.get(&gen)?;
        if now.honour >= d.until {
            return None;
        }
        // Plan 46 M1.3 step 13 (C11): the log ended this stream (the
        // root's abandon, say); `mine` still holds it until the table
        // sync drops it. It answers no strict read, so grants no read
        // delegation.
        if replica.void_cut(gen).is_some() {
            return None;
        }
        let epoch = super::DELEG_READ_EPOCH_BASE + gen;
        // ADR-46: the position is current (no write executes meanwhile),
        // but a grant now would join the previous process's, which this
        // node's table has forgotten.
        let quarantined = self.deleg_restart_quarantined(now, gen);
        // Only a reader the stream's void reaches (`void_audience`) gets a
        // grant: one the directory has not shown this core yet reads
        // through, and gets one on a later read, once it is known.
        let grant = if self.cfg.read_delegations
            && from != self.cfg.node_id
            && self.void_audience().is_some_and(|a| a.contains(&from))
        {
            let margin = self.cfg.expiry_margin_ms as i64;
            // A delegate's read grant is capped by its own honour end.
            let ttl = self.dl_t().read_grant_ttl(d.until.since(now.honour));
            // A non-stealable stream is honoured from its install (whatever
            // the root says), so its read grants need the root's word: one
            // renewal sent after the install was granted. A holder that came
            // back with its journal during an operator's abandon is not
            // renewed (the root refuses it), and grants no read meanwhile.
            let renewed = !d.non_stealable || d.answered_sent >= d.installed;
            if ttl > 0
                && renewed
                // ADR-57: no grant over keys a pending acknowledged row touches.
                && !replica.read_hold_touches_ino(ino)
                && !self
                    .rd
                    .blocked
                    .get(&ino)
                    .is_some_and(|until| *until > now.wait)
            {
                let until = now.wait.plus((ttl + margin) as u64);
                if quarantined {
                    self.stats.deleg_read_grants_quarantined += 1;
                    None
                } else if !self.grant_horizon_covers(
                    constellation_meta::readdeleg::GrantHorizon::Deleg,
                    now,
                    until,
                    replica,
                ) {
                    // Not durable yet: not granted (as the root's); the
                    // read is answered with its position, and the horizon
                    // is being written ahead.
                    None
                } else {
                    let id = replica.read_delegations().grant(from, ino, until);
                    self.stats.deleg_read_grants += 1;
                    Some(crate::event::ReadGrantMsg {
                        id,
                        ttl_ms: ttl as u64,
                        epoch,
                    })
                }
            } else {
                None
            }
        } else {
            None
        };
        self.stats.deleg_read_index_served += 1;
        Some(crate::event::ReadIndexOutcome::Ok {
            position: Position {
                seq: replica.applied_seq().unwrap_or(0),
                pending: None,
                streams: {
                    let mut s = constellation_meta::Streams::NONE;
                    s.raise(gen, replica.stream_applied(gen));
                    s
                },
            },
            grant,
            ahead: None,
        })
    }

    /// Phase 2b: where a strict read of `ino` goes — the delegate owning
    /// it (`Some(node)`), this node itself as that delegate (`Some(me)`),
    /// or the holder (`None`).
    pub(crate) fn deleg_read_owner(
        &self,
        now: Now,
        ino: Ino,
        name: Option<&str>,
        replica: &dyn Replica,
    ) -> Option<NodeId> {
        self.deleg_read_owner_gen(now, ino, name, replica)
            .map(|(node, _)| node)
    }

    /// [`Self::deleg_read_owner`] with the generation of the delegation
    /// that owns the keys, which a redirect names (a designee is no lease
    /// holder: the requester must not cache it).
    pub(crate) fn deleg_read_owner_gen(
        &self,
        now: Now,
        ino: Ino,
        name: Option<&str>,
        replica: &dyn Replica,
    ) -> Option<(NodeId, u64)> {
        if !self.cfg.delegation || replica.delegation_table().is_empty() {
            return None;
        }
        let keys = Self::read_keys(ino, name);
        match replica.resolve_ownership(&keys) {
            Ownership::Delegated(d) => {
                if d.node == self.cfg.node_id {
                    let honoured = self
                        .dl
                        .mine
                        .get(&d.gen)
                        .is_some_and(|m| now.honour < m.until && !m.stopped);
                    honoured.then_some((d.node, d.gen))
                } else {
                    Some((d.node, d.gen))
                }
            }
            _ => None,
        }
    }

    // ----------------------------------------------------- the root

    /// The generations `keys` fall under that this root must end before
    /// it executes: started (recalled) here; the caller parks on their
    /// wait ids. `None`: nothing to recall.
    /// What executing an op touching `keys` on the root needs first
    /// (phase 2b: a designation is never recalled — the op is refused).
    /// `own`: the op is this node's own client's (else a forward or an inbox
    /// batch of another node's).
    pub(crate) fn deleg_recall_plan(
        &mut self,
        now: Now,
        own: bool,
        keys: &TouchSet,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> RecallPlan {
        if !self.cfg.delegation || !self.deleg_known() {
            return RecallPlan::None;
        }
        let (involved, cross): (Involved, bool) = match replica.resolve_ownership(keys) {
            Ownership::Root => return RecallPlan::None,
            Ownership::Delegated(d) => (
                vec![(d.gen, BTreeMap::from([(d.dir, kind_of(d.kind))]))],
                false,
            ),
            Ownership::CrossSubtree { involved, .. } => {
                self.stats.deleg_cross_subtree += 1;
                (Self::group_by_gen(involved.iter()), true)
            }
        };
        // The gate never recalls (ADR-42): a generation it took on is the
        // delegate's until the gate opens, and a recall sent inside it would
        // end the generation (`DelegateNodeEnded`) in a view that is closed.
        if self.lease.gate.is_some()
            && involved
                .iter()
                .any(|(g, _)| self.dl.gens.get(g).is_some_and(|s| !s.ended))
        {
            return RecallPlan::Gated;
        }
        // ADR-55 (R-I4): the root's own sticky stream is neither executed
        // under by the root's own path nor refused by it.
        let me = self.me();
        if !cross {
            if let Some((gen, _)) = involved.first() {
                if self
                    .dl
                    .gens
                    .get(gen)
                    .is_some_and(|s| !s.ended && s.node == me && s.non_stealable())
                {
                    if self.cfg.root_executes_under_own_grant {
                        return RecallPlan::None;
                    }
                    self.stats.root_own_holds += 1;
                    return RecallPlan::Hold(*gen);
                }
            }
        }
        // ADR-55 (Q3): an op across the boundary of the root's own sticky
        // subtree releases it, as the holder (the root's own decision, never a
        // recall), and waits for the release row, instead of `EXDEV`. Other
        // nodes' non-stealable boundaries stay `EXDEV`.
        let is_own = |core: &Self, g: &u64| {
            core.dl
                .gens
                .get(g)
                .is_some_and(|s| !s.ended && s.node == me && s.kind() == DelegKind::Sticky)
        };
        let non_stealable = involved.iter().any(|(g, _)| {
            self.dl
                .gens
                .get(g)
                .is_some_and(|s| !s.ended && s.non_stealable() && !is_own(self, g))
        });
        let mut own_waiting = BTreeSet::new();
        if !non_stealable && cross && !self.cfg.root_executes_under_own_grant {
            for (gen, dirs) in &involved {
                if is_own(self, gen) {
                    let asked: BTreeSet<Ino> = dirs.keys().copied().collect();
                    if !self.own_sticky_release(now, *gen, asked, replica, out) {
                        // Nothing of the asked subtrees is held any more (the
                        // release took them already): no release to wait for;
                        // the stream's executor runs the op.
                        self.stats.root_own_holds += 1;
                        return RecallPlan::Hold(*gen);
                    }
                    if own {
                        self.stats.root_own_released_for_cross_own += 1;
                    } else {
                        self.stats.root_own_released_for_cross_forwarded += 1;
                    }
                    own_waiting.insert(wait_id(*gen));
                }
            }
        }
        if non_stealable {
            // Plans 03–05: only the designee writes under its path
            // (`EROFS` for anyone else once the designee is unreachable),
            // and nothing moves across its boundary (`EXDEV`).
            self.stats.deleg_refused_designated += 1;
            tracing::info!(
                node = self.me(),
                cross,
                "refusing an op under an unreachable designation (plans 03-05)"
            );
            return RecallPlan::Refuse(if cross {
                Code::CrossDevice
            } else {
                Code::ReadOnly
            });
        }
        let mut waiting = own_waiting;
        for (gen, dirs) in involved {
            if self.dl.gens.get(&gen).is_some_and(|g| g.ended) || is_own(self, &gen) {
                continue;
            }
            if cross {
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    // Released by an op, not a decision: delegate the
                    // subtrees to the same node again once the op ran.
                    for (d, k) in &dirs {
                        let k = g.dirs.get(d).copied().unwrap_or(*k);
                        g.redelegate_dirs.insert(*d, k);
                    }
                }
            }
            // Plan 46 M1.2 step 10: only the subtrees the op touches are
            // asked for; the rest of the stream stays live.
            let asked: BTreeSet<Ino> = dirs.keys().copied().collect();
            self.request_release(now, gen, &asked, out);
            waiting.insert(wait_id(gen));
        }
        if waiting.is_empty() {
            RecallPlan::None
        } else {
            RecallPlan::Wait(waiting)
        }
    }

    /// A delegate transaction whose `deps` name a journal position of a
    /// newer root tenure than this one: this root has been superseded
    /// without knowing it yet (paused, cut off), and must not append it —
    /// its replica would hold the transaction without its cause (long-
    /// delegated-backup seed 74035: a deposed root drained a sealed
    /// backup's marker whose data only its successor had; the rows strand
    /// at the deposition, but the replica showed the marker meanwhile).
    pub(crate) fn deps_from_a_newer_tenure(&self, deps: &Position) -> bool {
        deps.pending
            .is_some_and(|p| self.lease.epoch().is_none_or(|mine| p.epoch > mine))
    }

    /// Phase 2b: after a cross-subtree op executed, the generations it
    /// ended are granted again to their nodes (a new generation each).
    pub(crate) fn deleg_redelegate_after_cross(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.root_usable(now) {
            return;
        }
        // The subtrees an op released (or ended with its stream) are granted
        // to the same node again; a stream still being recalled is waited
        // for, and a subtree its stream still holds (the op did not need it
        // after all) is dropped from the list.
        let again: Vec<Redelegation> = self
            .dl
            .gens
            .iter_mut()
            .filter(|(_, g)| {
                !g.redelegate_dirs.is_empty() && (g.ended || g.recall == RecallPhase::None)
            })
            .map(|(gen, g)| {
                let held = |d: &Ino| !g.ended && g.dirs.contains_key(d);
                g.redelegate_dirs
                    .retain(|d, _| !held(d) || g.releasing.contains(d));
                let ready: Vec<(Ino, DelegKind)> = g
                    .redelegate_dirs
                    .iter()
                    .filter(|(d, _)| !held(d))
                    .map(|(d, k)| (*d, *k))
                    .collect();
                for (d, _) in &ready {
                    g.redelegate_dirs.remove(d);
                }
                (*gen, ready, g.node)
            })
            .filter(|(_, ready, _)| !ready.is_empty())
            .collect();
        for (gen, dirs, node) in again {
            let gate_recalled = self.dl.gens.get(&gen).is_some_and(|g| g.gate_recalled);
            let later = |core: &mut Self| {
                if let Some(g) = core.dl.gens.get_mut(&gen) {
                    g.redelegate_dirs.extend(dirs.iter().copied());
                }
            };
            if !self.dl.pending_exec.is_empty()
                || self.has_exec_parks()
                || self.rd_has_recall_wait(gen)
                || self.inbox_holds_redelegation(now)
            {
                // The op that ended it has not run yet: next time.
                later(self);
                continue;
            }
            if !self.roster.links().get(&node).is_some_and(|l| l.connected) {
                // The delegate is away for the moment (reconnecting
                // mid-recall): once it is back.
                later(self);
                continue;
            }
            // Every subtree again: the first grant opens the new stream and
            // the rest grow it.
            let mut results = Vec::new();
            for (dir, kind) in &dirs {
                results.push((
                    *dir,
                    self.delegate_dir(now, *dir, node, *kind, replica, out),
                ));
            }
            for (dir, result) in results {
                match result {
                    Ok(new_gen) => {
                        self.stats.deleg_redelegated += 1;
                        // ADR-44: the subtree of a generation the gate recalled on
                        // demand, re-granted at the open (the op's retry may
                        // recall it once more).
                        if gate_recalled {
                            self.stats.deleg_gate_redelegated += 1;
                        }
                        tracing::info!(
                            node = self.me(),
                            dir,
                            delegate = node,
                            gen = %constellation_meta::delegation::GenLabel::of(gen),
                            new_gen = %constellation_meta::delegation::GenLabel::of(new_gen),
                            "re-delegated after a cross-subtree op"
                        );
                    }
                    Err(why) => tracing::debug!(
                        node = self.me(),
                        dir,
                        delegate = node,
                        why,
                        "not re-delegated"
                    ),
                }
            }
        }
    }

    /// Plan 46 M0.3 (S3): whether `gen`'s delegate may not have its grant
    /// yet, so that an op it forwards to this root is one "sent before its
    /// table carried the grant": this tenure granted the generation, not
    /// longer ago than [`Core::grant_in_flight_ms`], and has not heard from the
    /// delegate since. Otherwise the delegate forwarding its own op has
    /// disowned the generation (stopped by a recall, a refused renewal or a
    /// restart): it answers every op of it `NotHolder`, and the root must end
    /// it, not wait for it. An inherited generation was never granted here.
    pub(crate) fn grant_in_flight(&self, now: Now, gen: u64) -> bool {
        self.dl.gens.get(&gen).is_some_and(|g| {
            (!g.inherited
                && g.delegate_heard.is_none()
                && now.wait.since(g.granted) < self.grant_in_flight_ms())
                || g.grown_at
                    .is_some_and(|t| now.wait.since(t) < self.grant_in_flight_ms())
        })
    }

    /// Plan 46 M0.3 (S3): how long after the root granted a generation its
    /// delegate's own op forwarded to the root may still mean "my table has not
    /// carried the grant yet". The delegate learns the grant from the
    /// log's `DelegateNode` record (one stream tick) and from its first
    /// renewal's answer; each is a P2P round trip the product waits
    /// [`Core::deleg_request_timeout_ms`] for before it counts the request
    /// dropped and sends it again. So: two stream ticks plus two such round
    /// trips (the spec's `2 x delegation_stream_tick_ms + 2 x max P2P RTT`,
    /// the request timeout being the product's bound on a P2P RTT).
    ///
    /// The product has no bound on an answer's *delivery*: one that comes
    /// after the request timeout is still counted (`LATE_ANSWER_MS`), and the
    /// simulation's `deleg_answer_delay` delivers some 1.75 s late. A delegate
    /// whose grant arrives later than this window is taken as having disowned
    /// the generation and its generation is recalled: that costs liveness
    /// only, a recall is always safe.
    pub(crate) fn grant_in_flight_ms(&self) -> i64 {
        self.dl_t().grant_in_flight as i64
    }

    pub(crate) fn start_recall(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        self.start_recall_for(now, gen, RecallCause::Other, out);
    }

    pub(crate) fn start_recall_for(
        &mut self,
        now: Now,
        gen: u64,
        cause: RecallCause,
        out: &mut Vec<Action>,
    ) {
        // Plan 46 M0.3 (S5, ADR-42), amended by ADR-44: a takeover gate
        // recalls only on demand (`gate_recall_on_demand`), once its journal
        // work is done, and only a generation of another node it took on.
        let gated = self.lease.gate.is_some();
        if gated && !self.root_gated_shippable(now) {
            return;
        }
        let me = self.me();
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended || g.recall != RecallPhase::None {
            return;
        }
        if gated && (!g.inherited || g.node == me) {
            return;
        }
        g.recall = RecallPhase::Sent;
        g.recall_cause = Some(cause);
        g.gate_recalled = gated;
        if g.kind() == DelegKind::Sticky {
            match cause {
                RecallCause::Undelegate => {}
                RecallCause::Epoch => self.stats.deleg_sticky_recalled_epoch += 1,
                RecallCause::Other => self.stats.deleg_sticky_recalled_other += 1,
            }
        }
        // Every subtree of the stream (plan 46 M1.2 step 10: a recall of the
        // generation is a request for all of them).
        g.releasing = g.dirs.keys().copied().collect();
        // The slot closes at the transition, in this step (plan 41 R1,
        // rule 2): no renewal is granted from here, whoever answers it.
        self.dw.close_for_recall(gen);
        self.send_release_request(now, gen, out);
    }

    /// Plan 46 M1.2 step 10: ask the holder of stream `gen` to release the
    /// subtrees `dirs` only. The rest of the stream stays live: its slot
    /// stays open and its renewals are granted. A stream asked for all of
    /// its subtrees is recalled whole ([`Self::start_recall`]); one already
    /// being recalled is not asked again, the caller waits for it.
    pub(crate) fn request_release(
        &mut self,
        now: Now,
        gen: u64,
        dirs: &BTreeSet<Ino>,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended || g.recall != RecallPhase::None {
            return;
        }
        if g.dirs.keys().all(|d| dirs.contains(d)) {
            self.start_recall(now, gen, out);
            return;
        }
        // A takeover gate recalls whole generations only.
        if self.lease.gate.is_some() {
            return;
        }
        if g.kind() == DelegKind::Sticky {
            self.stats.deleg_sticky_recalled_other += 1;
        }
        let outstanding = g.recall_resend.is_some();
        let new = dirs.iter().any(|d| g.releasing.insert(*d));
        if outstanding && !new {
            return;
        }
        self.send_release_request(now, gen, out);
    }

    /// Send `gen`'s release request for its `releasing` subtrees: a new
    /// attempt under a new request id, with its resend timer (L3).
    fn send_release_request(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        let (to, dirs) = (g.node, g.releasing.iter().copied().collect::<Vec<_>>());
        let had_timer = g.recall_resend.take();
        let req = self.op_id();
        let back_off = self.deleg_request_timeout_ms();
        if let Some(t) = had_timer {
            self.cancel_timer(t, out);
        }
        let timer = self.set_timer_wait(now.wait.plus(back_off), Timer::ReleaseResend(gen), out);
        let gs = self.dl.gens.get_mut(&gen).expect("present");
        gs.recall_req = Some(req);
        gs.recall_tries.push(req);
        gs.recall_started.get_or_insert(now.wait);
        gs.recall_backoff_ms = back_off;
        gs.recall_resend = Some(timer);
        self.dl.by_req.insert(req, (gen, ReqKind::Recall));
        self.stats.deleg_recalls_sent += 1;
        let every = self.cfg.delegation_recall_drop_every;
        if every > 0 && self.stats.deleg_recalls_sent.is_multiple_of(every) {
            // Simulation: the request is lost on the way (the loss a
            // request's timeout stands for in production).
            self.stats.deleg_recalls_dropped += 1;
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        tracing::info!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?dirs,
            delegate = to,
            "asking a delegate to release subtrees"
        );
        out.push(Action::Send {
            to,
            msg: PeerMsg::ReleaseRequest {
                req,
                gen,
                dirs,
                why: ReleaseWhy::Recall,
            },
        });
        self.arm_gen_expiry(now, gen, out);
    }

    /// The release request's resend timer: a request nobody answered (no
    /// release row reached this log) is sent again (L3), each time under a
    /// new request id, with the delay doubling from a request's timeout to
    /// 2 s, until the row comes or the expiry outwaits the generation.
    /// Nothing goes out in a takeover gate (ADR-42) unless the gate started
    /// the recall on demand (ADR-44), or while this root is unusable; the
    /// timer is armed again and the next firing tries once more. A request
    /// for some subtrees only that a request's timeout did not get a row for
    /// becomes a recall of the whole stream: the delegate is slow, cut off or
    /// down, and the recall closes the stream's window, so the grant's expiry
    /// bounds the wait as it always did.
    pub(crate) fn on_release_resend(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let patience = self.deleg_request_timeout_ms();
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.recall_resend = None;
        if g.ended
            || g.releasing.is_empty()
            || matches!(g.recall, RecallPhase::Drained(_) | RecallPhase::Sealing)
        {
            return;
        }
        if g.recall == RecallPhase::None
            && g.release_resends >= PARTIAL_RESENDS_BEFORE_RECALL
            && g.recall_started
                .is_some_and(|at| now.wait.since(at) >= patience as i64)
        {
            self.start_recall(now, gen, out);
            if self
                .dl
                .gens
                .get(&gen)
                .is_some_and(|g| g.recall != RecallPhase::None)
            {
                return;
            }
        }
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        let (to, dirs, mut back_off, gate_recalled) = (
            g.node,
            g.releasing.iter().copied().collect::<Vec<_>>(),
            g.recall_backoff_ms,
            g.gate_recalled,
        );
        // ADR-44: a recall the gate started on demand is sent again inside
        // the gate too (L3); any other waits for the open.
        let gate_ok = gate_recalled && self.root_gated_shippable(now);
        if (self.lease.gate.is_none() && self.root_usable(now)) || gate_ok {
            back_off = self.dl_t().next_recall_backoff(back_off);
            let req = self.op_id();
            let g = self.dl.gens.get_mut(&gen).expect("present");
            g.recall_req = Some(req);
            g.recall_tries.push(req);
            if g.recall == RecallPhase::None {
                g.release_resends += 1;
                self.stats.deleg_release_partial_resent += 1;
            }
            self.dl.by_req.insert(req, (gen, ReqKind::Recall));
            self.stats.deleg_recalls_resent += 1;
            tracing::info!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                ?dirs,
                delegate = to,
                "sending an unanswered release request again"
            );
            out.push(Action::Send {
                to,
                msg: PeerMsg::ReleaseRequest {
                    req,
                    gen,
                    dirs,
                    why: ReleaseWhy::Recall,
                },
            });
        } else {
            tracing::debug!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                gate_recalled,
                gated = self.lease.gate.is_some(),
                "an unanswered release request is not sent again now (this root is not usable, or its gate is not shippable)"
            );
        }
        let t = self.set_timer_wait(now.wait.plus(back_off), Timer::ReleaseResend(gen), out);
        let g = self.dl.gens.get_mut(&gen).expect("present");
        g.recall_backoff_ms = back_off;
        g.recall_resend = Some(t);
    }

    /// Plan 46 M1.2 step 10: the delegate's release rows (or the cut a
    /// stream that could not journal one ends at) reached this log. Each
    /// released subtree leaves the stream's set and the request for it is
    /// done; a stream released whole (`StreamReleased`, or a set that is
    /// empty) has drained to the row and ends now. The ops that waited for
    /// the subtrees run (the table of this replica forgot them with the
    /// rows).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn root_release_appended(
        &mut self,
        now: Now,
        gen: u64,
        dirs: &[Ino],
        whole: bool,
        cut: Option<(u64, bool, bool)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Plan 46 M1.3 step 7 (D-e): the holder's own release of a sticky
        // subtree (a release row, no recall under way) keeps the placement
        // from granting it straight back to that node on the window's old
        // counts. (`kind()` is read before the dirs leave the stream.) A
        // release that races a recall under way for the same stream records
        // nothing: the recall decides.
        if let Some(g) = self.dl.gens.get(&gen) {
            if !g.ended
                && cut.is_none()
                && g.recall == RecallPhase::None
                && g.kind() == DelegKind::Sticky
            {
                let until = now.wait.plus(self.cfg.placement_window_ms);
                // A stream released whole (an unmount, a suspension, a
                // release of everything) carries no dirs in its row: it
                // gives up all it still holds.
                let mut held = dirs.to_vec();
                if whole {
                    held.extend(g.dir_list());
                }
                for dir in held {
                    self.pl.released.insert(dir, (g.node, until));
                }
            }
        }
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended {
            return;
        }
        let (cut, seal, no_stream) = match cut {
            Some((cut, seal, no_stream)) => (Some(cut), seal, no_stream),
            None => (None, false, false),
        };
        for dir in dirs {
            g.dirs.remove(dir);
            g.releasing.remove(dir);
        }
        let cursor = g.cursor;
        // (The knob `partial_release_ends_stream`: a sticky stream ends at
        // any release row of its holder.)
        let knob = self.cfg.partial_release_ends_stream
            && g.kind() == DelegKind::Sticky
            && !dirs.is_empty();
        let end = cut.or_else(|| (whole || g.dirs.is_empty() || knob).then_some(cursor));
        // Plan 46 M1.2 step 14 (L2b): a rowless release ends the stream at a
        // cut the delegate capped below its own unshipped holder appends
        // (`holder_append_cap`). The premise that rows above the cap were
        // never acknowledged holds for a lone tenure's appends, not for a
        // C3 re-append: its acknowledgement was the delegate group's. So when
        // the latest record names backups the group is sealed before the
        // generation ends, as the model's `End` does (the recorded members'
        // verbatim tail is appended first). Only a release that asks for it
        // (`ReleaseNote::seal`: capped, or from a stream never installed or
        // restarted without); a cut that is exact ends the stream as before.
        // The note rides every batch until acknowledged: a repeat of one that
        // began the seal only lowers the cut.
        let wants_seal = cut.is_some() && seal && !g.group.strict().is_empty();
        let mut release_seal = false;
        if let Some(th) = end {
            g.releasing.clear();
            if wants_seal && g.release_seal {
                g.release_cut = g.release_cut.map(|c| c.min(th)).or(Some(th));
            } else if wants_seal && !matches!(g.recall, RecallPhase::Drained(_)) {
                release_seal = true;
                self.stats.deleg_release_seals += 1;
                g.release_cut = Some(th);
                g.release_seal = true;
                // A releasing node with no stream sends no row up to the
                // cut: nothing to wait for after the group's answer.
                g.release_cut_waived |= no_stream;
                if g.recall != RecallPhase::Sealing {
                    g.recall_before_seal = g.recall;
                    g.recall = RecallPhase::Sealing;
                }
                self.dw.close_for_recall(gen);
            } else if matches!(g.recall, RecallPhase::None | RecallPhase::Sent) {
                g.recall = RecallPhase::Drained(th);
                // The slot closes: a stream released whole renews no more.
                self.dw.close_for_recall(gen);
            }
        }
        let g = self.dl.gens.get_mut(&gen).expect("present");
        if g.releasing.is_empty() {
            let tries = std::mem::take(&mut g.recall_tries);
            g.recall_req = None;
            let timer = g.recall_resend.take();
            // (A stream released whole is timed when it ends.)
            if end.is_none() {
                g.release_resends = 0;
                if let Some(at) = g.recall_started.take() {
                    let ms = now.wait.since(at).max(0) as u64;
                    self.stats.deleg_recalls_timed += 1;
                    self.stats.deleg_recall_ms_sum += ms;
                    self.stats.deleg_recall_ms_max = self.stats.deleg_recall_ms_max.max(ms);
                    if ms > 1_000 {
                        self.stats.deleg_recall_over_1s += 1;
                    }
                }
            }
            for r in tries {
                self.dl.by_req.remove(&r);
            }
            if let Some(t) = timer {
                self.cancel_timer(t, out);
            }
        }
        self.stats.deleg_releases_appended += 1;
        if release_seal {
            self.deleg_release_seal_round(now, gen, true, replica, out);
        }
        self.deleg_wait_done(now, wait_id(gen), replica, out);
        self.root_progress(now, replica, out);
    }

    /// Plan 46 M1.2 step 14 (L2b): one round of the wait a rowless release
    /// of a recorded generation enters. `first`: the release has just been
    /// appended (the seals go out to the strict members and, once, to the
    /// best-effort ones); otherwise the retry (`Timer::DelegReleaseSeal`):
    /// a tail in hand is drained again, else the strict members are asked
    /// again, the wait is counted and shown (`waiting_for_group`), and the
    /// timer re-armed. Liveness is C2e's: any one live strict member ends
    /// the wait, and none reachable is the operator's `abandon-group`.
    fn deleg_release_seal_round(
        &mut self,
        now: Now,
        gen: u64,
        first: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended || !g.release_seal {
            return;
        }
        if g.prev_blocked {
            // The end waits for the previous roots (`deleg_end_waits_for_prev_roots`
            // arms nothing for a sticky or designated grant): keep this
            // wait's own clock running, and ask the group when they settle
            // (`deleg_prev_roots_cleared`).
            let at = now.wait.plus(self.release_seal_window());
            self.arm_release_seal_timer(gen, at, out);
            return;
        }
        let window = self.release_seal_window();
        let stream_window = self.release_stream_window();
        let strict: Vec<NodeId> = g.group.strict().to_vec();
        let best_effort = g.group.best_effort();
        let in_hand = g
            .sealed_tail
            .as_ref()
            .map(|t| t.from)
            .or(g.release_answered);
        if let Some(from) = in_hand {
            if !first {
                // A whole window after the answer, not after the release.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    let due = g
                        .release_answered_at
                        .is_some_and(|at| now.wait.since(at) >= stream_window as i64);
                    g.release_cut_waived |= g.release_answered.is_some() && due;
                }
            }
            // A tail in hand (or an answer waiting for the cursor): drained
            // again, its rows or the stream's may have come.
            self.deleg_end_sealed(now, gen, from, false, replica, out);
            if self.dl.gens.get(&gen).is_none_or(|g| g.ended) {
                return;
            }
        } else {
            if first {
                for member in best_effort {
                    if member == self.me() {
                        continue;
                    }
                    self.deleg_seal_member(now, gen, member, false, replica, out);
                }
            }
            for member in strict {
                self.deleg_seal_member(now, gen, member, true, replica, out);
            }
            if self.dl.gens.get(&gen).is_none_or(|g| g.ended) {
                return;
            }
        }
        if !first {
            self.stats.deleg_seal_waits += 1;
            let warn = {
                let gs = self.dl.gens.get_mut(&gen).expect("present");
                gs.waiting_for_group = true;
                gs.waiting_since.get_or_insert(now.wait);
                let due = gs
                    .waiting_warned
                    .is_none_or(|at| now.wait.since(at) >= 60_000);
                if due {
                    gs.waiting_warned = Some(now.wait);
                }
                due
            };
            if warn {
                tracing::warn!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    "a delegate released its stream below rows its recorded backup may hold: the generation is not ended without the backup's tail \
                     (`constellation repair abandon-group --gen {gen}` ends it at the root's cursor; rows only the group holds are lost)"
                );
            }
        }
        // The waiver of the wait for the stream is a window after the
        // group's answer: the timer is at that time when an answer is in hand.
        let mut again = now.wait.plus(window);
        if let Some(g) = self.dl.gens.get(&gen) {
            if let (Some(at), false) = (g.release_answered_at, g.release_cut_waived) {
                again = again.min(at.plus(stream_window)).max(now.wait);
            }
        }
        self.arm_release_seal_timer(gen, again, out);
    }

    /// The window of the release seal's retry.
    fn release_seal_window(&self) -> u64 {
        self.dl_t().release_seal_window
    }

    /// The wait for the delegate's stream to reach a released cut after the
    /// group has answered: the rows in flight on the stream are one round
    /// trip away.
    fn release_stream_window(&self) -> u64 {
        self.dl_t().release_stream_window
    }

    /// (Re)arm `Timer::DelegReleaseSeal(gen)` at `at`.
    fn arm_release_seal_timer(&mut self, gen: u64, at: WaitMs, out: &mut Vec<Action>) {
        let old = self
            .dl
            .gens
            .get_mut(&gen)
            .and_then(|g| g.release_seal_timer.take());
        if let Some(t) = old {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer_wait(at, Timer::DelegReleaseSeal(gen), out);
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.release_seal_timer = Some(t);
        }
    }

    /// The retry of [`Self::deleg_release_seal_round`].
    pub(crate) fn on_deleg_release_seal(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.release_seal_timer = None;
        }
        self.deleg_release_seal_round(now, gen, false, replica, out);
    }

    /// [`Self::deleg_send_seal`], and a member that is this node itself (a
    /// root that is its generation's backup) is served inline: the daemon
    /// does not send a message to itself.
    fn deleg_seal_member(
        &mut self,
        now: Now,
        gen: u64,
        member: NodeId,
        strict: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.me();
        if member != me {
            self.deleg_send_seal(gen, member, strict, out);
            return;
        }
        let before = out.len();
        self.deleg_send_seal(gen, member, strict, out);
        let sent: Vec<Action> = out.drain(before..).collect();
        for action in sent {
            let Action::Send {
                msg: PeerMsg::DelegSeal { req, gen: g },
                ..
            } = action
            else {
                out.push(action);
                continue;
            };
            let mut answer = Vec::new();
            self.on_deleg_seal(now, me, req, g, replica, &mut answer);
            for a in answer {
                match a {
                    Action::Send {
                        msg:
                            PeerMsg::DelegSealed {
                                req,
                                gen,
                                sealed,
                                txs,
                                prev_roots,
                            },
                        ..
                    } => self
                        .on_deleg_sealed(now, me, req, gen, sealed, txs, prev_roots, replica, out),
                    a => out.push(a),
                }
            }
        }
    }

    /// A release lets another node take the lease over at once. A
    /// generation this root is recalling would then end nowhere: the
    /// lease's end drops it with its recall (`deleg_on_lease_gone`), the
    /// delegate's answer finds no request to match (`on_deleg_recalled`)
    /// and the lock grants it hands back are dropped
    /// (`lock_install_returned`), and no `DelegateNodeEnded` is journaled, which
    /// only a root does (`end_generation`). The table kept the generation
    /// delegated while the delegate had handed it back here
    /// (`handed_back`): lock requests went round the two
    /// (`locks-unlinked-delegated-blips` seed 230, an epoch's flush
    /// releasing the lease of a root that had just recalled `d1`), and the
    /// handed-back grants' renewals were refused until they lapsed under
    /// their holders' I/O.
    ///
    /// So a release waits for every generation whose recall is in flight
    /// to end here: drained, or outwaited by its expiry. The wait ids for
    /// the release's park, empty when none is (the common case: no wait,
    /// no timer). Bounded by the recall's own outwait: the latest window
    /// of them (capped at a renewal's horizon from now), a seal of a
    /// silent delegate's backup after it, and the expiry's retry tick.
    /// Past the bound (`on_release_recall_bound`) the release goes as it
    /// did before the wait.
    pub(crate) fn release_recall_waits(
        &mut self,
        now: Now,
        out: &mut Vec<Action>,
    ) -> BTreeSet<u64> {
        if !self.cfg.delegation
            || self
                .dl
                .release_wait
                .as_ref()
                .is_some_and(|w| w.bound_passed)
        {
            return BTreeSet::new();
        }
        let gens: BTreeSet<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.recall != RecallPhase::None)
            .map(|(g, _)| *g)
            .collect();
        if gens.is_empty() {
            return BTreeSet::new();
        }
        let horizon = now.wait.plus(self.reclaim_horizon_ms());
        let until = gens
            .iter()
            .map(|g| self.gen_until(*g))
            .max()
            .unwrap_or(now.wait)
            .clamp(now.wait, horizon);
        let bound = until.plus(self.dl_t().release_recall_bound(0));
        if let Some(w) = self.dl.release_wait.take() {
            self.cancel_timer(w.timer, out);
        }
        let timer = self.set_timer_wait(bound, Timer::ReleaseRecallBound, out);
        self.stats.deleg_release_waits += 1;
        tracing::info!(
            node = self.me(),
            generations = ?gens,
            bound = bound.0,
            "the release waits for the generations being recalled to end"
        );
        let ids = gens.iter().map(|g| wait_id(*g)).collect();
        self.dl.release_wait = Some(ReleaseWait {
            gens,
            timer,
            bound_passed: false,
        });
        ids
    }

    /// `root_usable`, or a usable lease whose release waits for `gen`'s
    /// recall (`release_recall_waits`): the release fenced the lease (no
    /// new op, no renewal of anything), but the recall it waits for still
    /// ends here, by the delegate's stream reaching its drained answer or
    /// by the expiry outwaiting it. Fenced, neither happened: the wait ran
    /// to its bound.
    fn root_ends_recall(&self, now: Now, gen: u64) -> bool {
        self.root_usable(now)
            || (self
                .dl
                .release_wait
                .as_ref()
                .is_some_and(|w| !w.bound_passed && w.gens.contains(&gen))
                && self.lease.ship_epoch(now, &self.cfg).is_some())
    }

    pub(crate) fn recall_all(&mut self, now: Now, out: &mut Vec<Action>) {
        let gens: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.recall == RecallPhase::None)
            .map(|(g, _)| *g)
            .collect();
        for gen in gens {
            self.start_recall_for(now, gen, RecallCause::Epoch, out);
        }
    }

    /// Whether the TTL leaves a grant of this kind alone: every non-stealable
    /// kind, but for the simulation's negative control
    /// (`Config::ttl_reclaims_sticky`: the code without this step's rule).
    fn ttl_exempt_kind(&self, k: DelegKind) -> bool {
        k.non_stealable() && !(self.cfg.ttl_reclaims_sticky && k == DelegKind::Sticky)
    }

    fn ttl_exempt(&self, g: &GenState) -> bool {
        g.non_stealable() && self.ttl_exempt_kind(g.kind())
    }

    fn arm_gen_expiry(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended || self.ttl_exempt(g) {
            // A non-stealable grant never expires: `online` ends it (an
            // unreachable designee's stays until it returns, DESIGN.md
            // §5.2).
            return;
        }
        let at = self.gen_until(gen).max(now.wait);
        if let Some(t) = g.expiry {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer_wait(at, Timer::DelegExpiry(gen), out);
        self.dl.gens.get_mut(&gen).expect("present").expiry = Some(t);
    }

    /// Plan 46 M1.2 step 7 (c3-spec D6): this root has just inherited `gen`
    /// from a predecessor. When its latest `DelegateNodeGroup` records no backup (the
    /// group is the delegate and a root) the rows the delegate was answered
    /// through may sit only on the delegate and on whichever root appended
    /// them: the previous holder of the lease this node claimed, whose member
    /// copy (`backup_tail[DELEG_BACKUP_BASE + gen]`, what its deposition kept
    /// of its appends) holds the ones the log never got. It is sealed and its
    /// answer appended; the generation does not end before
    /// (`end_generation`). The set persists, with whatever an earlier
    /// tenure of this node still owed, until each has answered.
    fn deleg_inherit_prev_roots(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.me();
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.node == me || !g.group.strict().is_empty() {
            return;
        }
        let mut set: BTreeSet<NodeId> = replica
            .deleg_prev_roots(gen)
            .unwrap_or_default()
            .into_iter()
            .collect();
        if let Some(prev) = self.lease.prev_holder {
            set.insert(prev);
        }
        // This node's own member copy of `gen` (what an earlier deposition of
        // it kept of the rows it appended, c3-spec §6.1) is one the generation
        // must be drained from before it ends, like any previous root's: a
        // node that claims again after its successor died holds rows the log
        // may not (and its cursor says nothing of them). It is "asked" with
        // no message: the tail is read here (`deleg_load_own_copy`, retried
        // by every round of the step). A copy that holds nothing is no debt; one
        // that cannot be read leaves the set as it was.
        match replica.deleg_backup_holds_any(gen) {
            Some(true) => {
                set.insert(me);
            }
            Some(false) => {
                set.remove(&me);
            }
            None => {}
        }
        if set.is_empty() {
            return;
        }
        let nodes: Vec<NodeId> = set.iter().copied().collect();
        self.persist_prev_roots(gen, &nodes, replica);
        tracing::info!(
            node = me,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            prev_roots = ?nodes,
            "inherited a generation without a recorded backup: sealing the previous root(s)"
        );
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.prev_roots = set;
        }
        self.deleg_prev_roots_step(now, replica, out);
    }

    /// Persist the previous roots still to seal; a failure is logged and
    /// counted (the set is then only in memory: a restart before it settles
    /// seals what the claim names, not what an earlier tenure owed).
    fn persist_prev_roots(&mut self, gen: u64, nodes: &[NodeId], replica: &dyn Replica) {
        if !replica.set_deleg_prev_roots(gen, nodes) {
            self.stats.deleg_prev_roots_persist_failed += 1;
            tracing::warn!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                "the previous roots to seal could not be persisted"
            );
        }
    }

    /// The previous roots' seals (D6), after every event: ask each that has
    /// not answered (again every `backup_takeover_ms`), drain the tails that
    /// came, and settle the ones drained to their end.
    fn deleg_prev_roots_step(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let gens: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && !g.prev_roots.is_empty())
            .map(|(gen, _)| *gen)
            .collect();
        for gen in gens {
            if self.dl.gens[&gen].prev_roots.contains(&self.me())
                && !self.dl.gens[&gen].prev_tails.contains_key(&self.me())
            {
                self.deleg_load_own_copy(gen, replica);
            }
            let tails: Vec<(NodeId, std::sync::Arc<SealedTail>)> = self.dl.gens[&gen]
                .prev_tails
                .iter()
                .map(|(n, t)| (*n, t.clone()))
                .collect();
            for (node, tail) in tails {
                let mut drained_to = self.dl.gens[&gen].cursor;
                let mut lost = false;
                if self.root_ends_recall(now, gen) {
                    (drained_to, lost) =
                        self.deleg_drain_tail(now, gen, &tail.txs, false, replica, out);
                }
                // A row whose dependency was lost with its generation was
                // never acknowledged (as for a backup's tail): the drain
                // stops there and the previous root has nothing more to give.
                if drained_to >= tail.end || lost {
                    self.deleg_prev_root_done(now, gen, node, replica, out);
                }
            }
            let Some(g) = self.dl.gens.get(&gen) else {
                continue;
            };
            let me = self.me();
            let due: Vec<NodeId> = g
                .prev_roots
                .iter()
                .copied()
                .filter(|n| *n != me && !g.prev_tails.contains_key(n))
                .filter(|n| {
                    g.member_notes
                        .get(n)
                        .and_then(|m| m.asked)
                        .is_none_or(|at| now.wait.since(at) >= self.cfg.backup_takeover_ms as i64)
                })
                .collect();
            for node in due {
                self.deleg_send_prev_root_seal(gen, node, out);
            }
        }
    }

    fn deleg_send_prev_root_seal(&mut self, gen: u64, node: NodeId, out: &mut Vec<Action>) {
        let req = self.op_id();
        self.dl.by_req.insert(req, (gen, ReqKind::SealPrevRoot));
        let now = self.clk.wait;
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.prev_reqs.insert(req, node);
            g.member_notes.entry(node).or_default().asked = Some(now);
        }
        self.stats.deleg_prev_root_seals += 1;
        out.push(Action::Send {
            to: node,
            msg: PeerMsg::DelegSeal { req, gen },
        });
    }

    /// `node` is settled: its tail drained to its end, or it held nothing.
    fn deleg_prev_root_done(
        &mut self,
        now: Now,
        gen: u64,
        node: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.prev_roots.remove(&node);
        g.prev_tails.remove(&node);
        g.prev_done.insert(node);
        if g.prev_roots.is_empty() {
            self.deleg_prev_roots_cleared(now, gen, true, replica, out);
        } else {
            let nodes: Vec<NodeId> = g.prev_roots.iter().copied().collect();
            self.persist_prev_roots(gen, &nodes, replica);
        }
    }

    /// Every previous root of `gen` is settled (or the delegate showed this
    /// root has every row it executed, or the operator abandoned the group):
    /// forget the requests and the persisted set, and let the end that waited
    /// go on.
    fn deleg_prev_roots_cleared(
        &mut self,
        now: Now,
        gen: u64,
        progress: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.persist_prev_roots(gen, &[], replica);
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.prev_roots.clear();
        g.prev_tails.clear();
        let reqs = std::mem::take(&mut g.prev_reqs);
        for req in reqs.keys() {
            self.dl.by_req.remove(req);
        }
        let g = self.dl.gens.get_mut(&gen).expect("present");
        if std::mem::take(&mut g.prev_blocked) {
            g.waiting_for_group = false;
            g.waiting_since = None;
            // The end that waited is tried now, not at the next window.
            self.dw.close(gen);
            self.dw.set_until_closed(gen, now.wait);
            self.arm_gen_expiry(now, gen, out);
        }
        // A release that waits for its group asks it now (the retry timer
        // ran on while the end was blocked, but a window is long).
        if self
            .dl
            .gens
            .get(&gen)
            .is_some_and(|g| g.release_seal && !g.ended)
        {
            self.deleg_release_seal_round(now, gen, true, replica, out);
        }
        // A recall that drained meanwhile ends the generation now
        // (`root_progress` skips a generation with previous roots left: it
        // never asked for the end, so nothing is marked blocked).
        if progress {
            self.root_progress(now, replica, out);
        }
    }

    /// A previous root's answer to a seal (D6): append its tail from the
    /// cursor as a backup's is appended, and settle it once the cursor
    /// reaches the end. Never ends the generation, never re-delegates it:
    /// the previous root is no delegate.
    #[allow(clippy::too_many_arguments)]
    fn on_prev_root_sealed(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        sealed: bool,
        txs: Vec<constellation_meta::DelegateTx>,
        prev_roots: Vec<NodeId>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(gs) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if gs.ended || gs.prev_reqs.get(&req) != Some(&from) || !gs.prev_roots.contains(&from) {
            return;
        }
        gs.member_notes.entry(from).or_default().answered = Some(now.wait);
        // Transitive (D6): the roots the answering node itself still owed a
        // seal are sealed too (before it is settled, so the set is never empty
        // between them).
        let mut added = false;
        for n in prev_roots {
            if n != from && !gs.prev_done.contains(&n) && gs.prev_roots.insert(n) {
                added = true;
            }
        }
        if added {
            let nodes: Vec<NodeId> = gs.prev_roots.iter().copied().collect();
            self.persist_prev_roots(gen, &nodes, replica);
        }
        let Some(gs) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if !sealed {
            // It holds nothing of the generation (and is sealed against more).
            self.deleg_prev_root_done(now, gen, from, replica, out);
            return;
        }
        let end = txs.iter().fold(
            gs.cursor,
            |end, tx| {
                if tx.idx == end + 1 {
                    tx.idx
                } else {
                    end
                }
            },
        );
        self.stats.deleg_prev_root_rows += txs.iter().filter(|t| t.idx > gs.cursor).count() as u64;
        gs.prev_tails
            .insert(from, std::sync::Arc::new(SealedTail { from, txs, end }));
        self.deleg_prev_roots_step(now, replica, out);
    }

    /// This node is among `gen`'s previous roots (a transitive answer named
    /// it): its own member copy is the tail it contributes, read locally. A
    /// copy that cannot be read is retried by the next round of the step.
    fn deleg_load_own_copy(&mut self, gen: u64, replica: &dyn Replica) {
        let me = self.me();
        let txs = match replica.deleg_backup_holds_any(gen) {
            Some(true) => replica.deleg_backup_tail(gen),
            // It holds nothing: nothing to drain (settled by the step).
            Some(false) => Some(Vec::new()),
            None => None,
        };
        if let (Some(txs), Some(g)) = (txs, self.dl.gens.get_mut(&gen)) {
            let end = txs.iter().fold(
                g.cursor,
                |end, tx| {
                    if tx.idx == end + 1 {
                        tx.idx
                    } else {
                        end
                    }
                },
            );
            g.prev_tails
                .insert(me, std::sync::Arc::new(SealedTail { from: me, txs, end }));
        }
    }

    /// The delegate's renewal says its stream head is `head`: when this root
    /// has appended through it, it holds every row the delegate executed, and
    /// so every row any previous root appended (the delegate keeps its rows
    /// until the log carries them): the previous roots need not be asked.
    pub(crate) fn deleg_prev_roots_renewed(
        &mut self,
        now: Now,
        from: NodeId,
        gen: u64,
        head: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let satisfied = self.dl.gens.get(&gen).is_some_and(|g| {
            !g.ended && g.node == from && !g.prev_roots.is_empty() && head <= g.cursor
        });
        if satisfied {
            self.stats.deleg_prev_root_renewals += 1;
            self.deleg_prev_roots_cleared(now, gen, true, replica, out);
        }
    }

    /// Plan 43 C2e: ask `member` to seal `gen`. `strict`: it is one of the
    /// latest version's members, whose tail the end waits for.
    fn deleg_send_seal(&mut self, gen: u64, member: NodeId, strict: bool, out: &mut Vec<Action>) {
        let req = self.op_id();
        self.dl.by_req.insert(req, (gen, ReqKind::Seal));
        let now = self.clk.wait;
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.seal_reqs.insert(req, (member, strict));
            g.member_notes.entry(member).or_default().asked = Some(now);
        }
        self.stats.deleg_seals_sent += 1;
        out.push(Action::Send {
            to: member,
            msg: PeerMsg::DelegSeal { req, gen },
        });
    }

    pub(crate) fn on_deleg_expiry(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        g.expiry = None;
        if g.ended {
            return;
        }
        if !self.root_ends_recall(now, gen) {
            // Phase 2b round 2: the root is unusable for the moment (a
            // renewal in flight, S3 away) but not gone (`deleg_on_lease_gone`
            // drops the state when it is): the expiry is tried again
            // shortly, or a recall in flight is never outwaited — an
            // execution parked on it waits forever (long-delegated seed
            // 70051: 85 000 simulated seconds of timers).
            let again = now.wait.plus(self.cfg.delegation_stream_tick_ms.max(100));
            let t = self.set_timer_wait(again, Timer::DelegExpiry(gen), out);
            self.dl.gens.get_mut(&gen).expect("present").expiry = Some(t);
            return;
        }
        // One critical section on the window table's own clock (plan 41 R1):
        // a window extended since this step took its `now` is re-armed, and
        // one that has run out is closed in the same section, so nothing
        // lengthens it between this decision and the seal or the reclaim.
        if self.dw.close_if_expired(gen, now).is_err() {
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        let g = self.dl.gens.get(&gen).expect("present").clone();
        let done = matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th);
        if done {
            return;
        }
        // Plan 30 §M14: an outwaited delegate — the lock grants moved to
        // it may still be honoured with this root's windows: a grace on
        // the subtree (the seal path re-delegates; the grace stands).
        let outwaited_dirs = (g.recall != RecallPhase::Sealing).then(|| g.dir_list());
        // Plan 43 C2e (Q1, D3): the seal set. A generation whose latest
        // record names backups (the strict members) is not ended without
        // one member's tail — rows acknowledged through the group may be
        // nowhere else — so a seal is asked of each until one answers with
        // a tail the root drains to its end, however many windows pass.
        // Without a record (the group is the delegate and the root) the
        // root's cursor covers everything acknowledged and nothing is
        // waited for. Backups only an older record named are sealed once,
        // without waiting (the union rule and `removal_safe` leave every row
        // acknowledged under an older version at or below the cursor or on
        // the later version's members: the belt keeps them from taking more
        // appends, and their tails are drained if they come in time).
        let strict: Vec<NodeId> = g.group.strict().to_vec();
        let recorded = !strict.is_empty();
        for dir in outwaited_dirs.into_iter().flatten() {
            self.lock_on_generation_outwaited(now, dir);
        }
        for member in g.group.best_effort() {
            if member == self.me() || g.seal_reqs.values().any(|(n, _)| *n == member) {
                continue;
            }
            self.deleg_send_seal(gen, member, false, out);
        }
        // Phase 2b: a silent delegate with a backup — ask its backups to
        // seal and hand over what they hold first.
        if recorded && g.recall != RecallPhase::Sealing {
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                g.recall_before_seal = g.recall;
                g.recall = RecallPhase::Sealing;
            }
            // Closed already (the expiry decision), and nothing reopens it
            // until the seal is answered or gives up: the seal's own window.
            self.dw.close(gen);
            self.dw
                .set_until_closed(gen, now.wait.plus(self.cfg.forward_timeout_ms * 2));
            for member in strict {
                tracing::info!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    backup = member,
                    "delegate silent: sealing its backup"
                );
                self.deleg_send_seal(gen, member, true, out);
            }
            self.arm_gen_expiry(now, gen, out);
            return;
        }
        // The grant is not honoured any more (the margin argument): a
        // pending recall is outwaited, an unrenewed grant reclaimed.
        match g.recall {
            RecallPhase::None => {
                if !self.cfg.delegation_reclaim_expired {
                    // Nothing ends here: the decision above shut the slot,
                    // and the core's own answers go on, so it opens again.
                    self.dw.reopen(gen);
                    return;
                }
                self.stats.deleg_reclaimed += 1;
                tracing::info!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    until = self.gen_until(gen).0,
                    now = now.wait.0,
                    "reclaiming an unrenewed delegation"
                );
            }
            RecallPhase::Sealing if recorded => {
                // Plan 43 C2 (Q1): no strict member has answered with a tail
                // drained to its end in time: the generation is not ended
                // without it. A tail in hand is drained again (its
                // dependencies may have come); otherwise the seals are asked
                // again (the earlier requests stay known: a late answer to
                // one is as good), and we wait.
                if let Some(from) = g.sealed_tail.as_ref().map(|t| t.from) {
                    self.deleg_end_sealed(now, gen, from, false, replica, out);
                    if self.dl.gens.get(&gen).is_none_or(|g| g.ended) {
                        return;
                    }
                } else {
                    for member in strict {
                        self.deleg_send_seal(gen, member, true, out);
                    }
                }
                self.stats.deleg_seal_waits += 1;
                let warn = {
                    let gs = self.dl.gens.get_mut(&gen).expect("present");
                    gs.waiting_for_group = true;
                    gs.waiting_since.get_or_insert(now.wait);
                    let due = gs
                        .waiting_warned
                        .is_none_or(|at| now.wait.since(at) >= 60_000);
                    if due {
                        gs.waiting_warned = Some(now.wait);
                    }
                    due
                };
                if warn {
                    tracing::warn!(
                        node = self.me(),
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        "a delegation's recorded backup has not answered its seal: the generation is not ended without its tail \
                         (`constellation repair abandon --gen {gen}` ends it at the root's cursor; rows only the group holds are lost)"
                    );
                }
                self.dw.close(gen);
                self.dw
                    .set_until_closed(gen, now.wait.plus(self.release_seal_window()));
                self.arm_gen_expiry(now, gen, out);
                return;
            }
            RecallPhase::Sealing => {
                // The seal was not answered in time.
                self.stats.deleg_reclaimed += 1;
            }
            _ => self.stats.deleg_recalls_expired += 1,
        }
        self.end_generation(now, gen, None, replica, out);
    }

    /// A drained recall whose stream caught up ends the generation.
    fn root_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        // Plan 46 M1.2 step 7 (D6): the delegate answered the recall with
        // `through`, its stream's index capped by its own holder appends its
        // tenure has not committed (`holder_append_cap`, `on_deleg_recall`), not
        // by its head: the rows above the cap were never acknowledged either
        // (`DelegateStreamAck.committed` is capped the same way). A root that has
        // appended through it holds every row acknowledged to a client, so no
        // previous root holds an acknowledged row it lacks (as for a renewal's
        // `stream_head`, `deleg_prev_roots_renewed`). A delegate that was
        // recalled renews no more, and a dead previous root answers nothing:
        // this is the proof that is left.
        let proven: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended
                    && !g.prev_roots.is_empty()
                    && matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th)
            })
            .map(|(gen, _)| *gen)
            .collect();
        for gen in proven {
            self.stats.deleg_prev_root_recalls += 1;
            self.deleg_prev_roots_cleared(now, gen, false, replica, out);
        }
        // Plan 46 M0.3 (S5, ADR-42): the takeover gate ends no generation (a
        // recall kept from a usable phase of this tenure is ended when the
        // gate opens: `delegation_sync`), except, under ADR-44, one it
        // recalled on demand once the recall drained: its cut is journaled
        // now, so a settle on its keys is the gate's own (S1).
        let gated = self.lease.gate.is_some();
        if gated && !self.root_gated_shippable(now) {
            return;
        }
        let done: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended
                    && g.prev_roots.is_empty()
                    && (!gated || (g.gate_recalled && g.inherited))
                    && matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th)
            })
            .map(|(g, _)| *g)
            .collect();
        for gen in done {
            self.stats.deleg_recalls_drained += 1;
            self.end_generation(now, gen, None, replica, out);
        }
    }

    /// Plan 43 C2e (Q1) and plan 46 M1.3 step 13 (Q5, C11): `constellation
    /// repair abandon --gen G | --node N [--gen G]`. The operator accepts
    /// that the holder (and, for a placed generation, the group) of the
    /// streams is gone for good; each stream is ended at the root's cursor
    /// with `LogRecord::GroupAbandoned` journaled with the cut (the audit),
    /// and the rows only the holder or its group held, acknowledged to their
    /// clients, are lost to the log (a holder that returns replays its
    /// copies by rid; refused ones become conflict copies).
    ///
    /// * A **placed** generation is abandoned only while it waits for its
    ///   group (`waiting_for_group`), i.e. after the root outwaited its
    ///   recall or TTL end; its delegate's read grants are capped by its own
    ///   `until - margin`, which that outwait covered. It ends at once, with
    ///   no read wait (plan 46 step 13 D4).
    /// * A **non-stealable** stream (sticky, designated) is never TTL-ended
    ///   and its holder's read grants are not capped by its `until`: C11,
    ///   "abandon = recall, else expire". The root voids its own copy of the
    ///   grants, asks every other node to void theirs
    ///   ([`PeerMsg::DelegReadVoid`]) and ends the stream once they answered,
    ///   or `read_delegation_ttl_ms + expiry_margin_ms` after the command (the
    ///   holder is gone, so every grant was sent before it): no read is
    ///   served under the stream after its end is journaled. During the wait
    ///   the stream stays live (its subtree still blocks), and a forced,
    ///   still-streaming holder's rows keep being appended: the cut is the
    ///   cursor when the end is journaled.
    ///
    /// All the streams are validated before any is started.
    pub(crate) fn on_control_abandon_streams(
        &mut self,
        now: Now,
        op: OpId,
        sel: AbandonSel,
        force: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A repeated request for streams a wait already covers (the operator
        // gave up waiting and asks again) takes over that wait: the answer
        // goes to the new command, the old one is told it was superseded.
        let asked: Vec<u64> = match sel {
            AbandonSel::Gen(g) | AbandonSel::Node(_, Some(g)) => vec![g],
            AbandonSel::Node(n, None) => self
                .dl
                .gens
                .iter()
                .filter(|(_, g)| g.node == n && !g.ended)
                .map(|(g, _)| *g)
                .collect(),
        };
        let covering = if asked.is_empty() {
            None
        } else {
            self.dl
                .abandons
                .iter_mut()
                .find(|w| asked.iter().all(|g| w.gens.contains(g)))
        };
        if let Some(w) = covering {
            let old = std::mem::replace(&mut w.op, op);
            out.push(Action::ControlDone {
                op: old,
                result: Err("taken over by a repeated command for the same streams".to_string()),
            });
            return;
        }
        match self.abandon_validate(now, sel, force) {
            Err(error) => out.push(Action::ControlDone {
                op,
                result: Err(error),
            }),
            Ok(plan) => self.abandon_start(now, op, plan, replica, out),
        }
    }

    fn abandon_validate(
        &self,
        now: Now,
        sel: AbandonSel,
        force: bool,
    ) -> Result<AbandonPlan, String> {
        let by_node = match sel {
            AbandonSel::Node(n, _) => Some(n),
            AbandonSel::Gen(_) => None,
        };
        let gens: Vec<u64> = match sel {
            AbandonSel::Gen(g) => vec![g],
            AbandonSel::Node(n, _) if n == self.me() => {
                return Err(format!(
                    "node {n} is this node: release your own grants with `constellation release`"
                ));
            }
            AbandonSel::Node(n, Some(g)) => match self.dl.gens.get(&g) {
                Some(s) if s.node == n => vec![g],
                Some(s) => {
                    return Err(format!(
                        "generation {g} is node {}'s stream, not node {n}'s",
                        s.node
                    ));
                }
                None => {
                    return Err(format!(
                        "this node is not the root of generation {g} (run it on the node whose `status` lists it under delegation.gens)"
                    ));
                }
            },
            AbandonSel::Node(n, None) => {
                let gens: Vec<u64> = self
                    .dl
                    .gens
                    .iter()
                    .filter(|(_, g)| g.node == n && !g.ended)
                    .map(|(g, _)| *g)
                    .collect();
                if gens.is_empty() {
                    return Err(format!("this root lists no live stream of node {n}"));
                }
                gens
            }
        };
        let mut plan = AbandonPlan::default();
        for gen in gens {
            if self.dl.abandons.iter().any(|w| w.gens.contains(&gen)) {
                return Err(format!(
                    "an abandon of generation {gen} is already waiting for its read delegations"
                ));
            }
            let heard = self
                .dl
                .gens
                .get(&gen)
                .and_then(|g| self.deleg_group_heard_lately(now, g));
            let g = match self.dl.gens.get(&gen) {
                None => {
                    return Err(format!(
                        "this node is not the root of generation {gen} (run it on the node whose `status` lists it under delegation.gens)"
                    ));
                }
                Some(g) => g,
            };
            if g.ended {
                return Err(format!("generation {gen} has ended already"));
            }
            let stream = g.non_stealable();
            if !stream && !g.waiting_for_group {
                if by_node.is_some() {
                    plan.placed_left.push(gen);
                    continue;
                }
                return Err(format!(
                    "generation {gen} is not waiting for its group (delegation.gens[].waiting_for_group); \
                     a placed stream ends by its TTL, nothing to abandon"
                ));
            }
            // A member answered: its tail is here, the group is not lost.
            // The root appends it as its dependencies allow; ending the
            // generation now would drop the rows past the cursor that the
            // group acknowledged and that are in this very tail.
            if let Some(t) = g.sealed_tail.as_ref() {
                return Err(format!(
                    "node {}'s tail of generation {gen} is in hand (appended through {} of {}) and is appended as soon as its \
                     dependencies are here; this group is not lost, so it is not abandoned (see delegation.waiting)",
                    t.from, g.cursor, t.end
                ));
            }
            // Plan 46 M1.2 step 7 (c3-spec D6): a previous root whose tail is
            // in hand or not yet answered may hold rows its clients were told
            // are committed, that no other member has: not given up without
            // `--force` (which the operator gives after confirming it is gone).
            if !force && (!g.prev_roots.is_empty() || !g.prev_tails.is_empty()) {
                return Err(format!(
                    "generation {gen} still waits for the tail of previous root(s) {:?} ({} tail(s) in hand): rows only a previous \
                     root held, acknowledged to their clients, would be lost. Confirm from elsewhere that they are down, then run \
                     the command again with --force",
                    g.prev_roots,
                    g.prev_tails.len()
                ));
            }
            // The discriminator (C2e follow-up): a delegate or a strict
            // member this root heard from within a grant's ttl may be alive
            // and cut from it only; its tail, or the delegate's rows, are not
            // lost. The operator confirms from elsewhere that the nodes are
            // down, and forces.
            if let (false, Some((node, role, ago))) = (force, heard) {
                return Err(format!(
                    "this root heard from node {node} ({role} of generation {gen}) {ago} ms ago, within \
                     delegation_ttl_ms ({} ms): it may be alive and cut from this node only, holding rows its \
                     clients were told are committed. Confirm from elsewhere that the delegate and the members \
                     of its group are down, then run the command again with --force",
                    self.cfg.delegation_ttl_ms
                ));
            }
            if !self.root_ends_recall(now, gen) {
                return Err(format!(
                    "this node cannot end generation {gen} now (it is not a usable root)"
                ));
            }
            if let Some(h) = heard {
                plan.forced.insert(gen, h);
            }
            if stream {
                plan.streams.push(gen);
            } else {
                plan.placed.push(gen);
            }
        }
        if plan.streams.is_empty() && plan.placed.is_empty() {
            return Err(format!(
                "generations {:?} are placed streams that are not waiting for their group: nothing the cluster cannot end by itself \
                 (they end by their TTL)",
                plan.placed_left
            ));
        }
        Ok(plan)
    }

    fn abandon_start(
        &mut self,
        now: Now,
        op: OpId,
        plan: AbandonPlan,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut lines = Vec::new();
        let mut failed = None;
        for gen in &plan.placed {
            match self.abandon_end_now(now, *gen, plan.forced.get(gen).copied(), replica, out) {
                Ok(cursor) => lines.push(format!(
                    "generation {gen} (placed) ended at index {cursor}; rows past it that only its group held are lost"
                )),
                Err(e) => failed = Some(e),
            }
        }
        for gen in &plan.placed_left {
            lines.push(format!(
                "generation {gen} (placed) is not waiting for its group: left to its TTL"
            ));
        }
        if let Some(e) = failed {
            // What was ended before the failure is reported (it stays ended).
            let done: Vec<&String> = lines.iter().filter(|l| l.contains("ended at")).collect();
            if !done.is_empty() {
                self.stats.deleg_abandons += 1;
            }
            let result = if done.is_empty() {
                e
            } else {
                format!(
                    "{e} (already done: {})",
                    done.iter()
                        .map(|l| l.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            };
            out.push(Action::ControlDone {
                op,
                result: Err(result),
            });
            return;
        }
        self.stats.deleg_abandons += 1;
        if plan.streams.is_empty() {
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(lines.join("\n"))),
            });
            return;
        }
        let me = self.me();
        let mut w = AbandonWait {
            op,
            gens: plan.streams.iter().copied().collect(),
            asked: BTreeMap::new(),
            sent: BTreeMap::new(),
            deadline: now.wait.plus(self.dl_t().abandon_read_wait),
            forced: plan.forced.clone(),
            lines,
            epoch: self.lease.epoch(),
            audience_unknown: false,
        };
        let waits = self.cfg.read_delegations && !self.cfg.abandon_skips_read_recall;
        if waits {
            // This node's own copy of the grants, then every other node's.
            // Until the directory has shown the peers, nobody can be asked
            // and the deadline decides (an empty set would read as "every
            // reader answered").
            let known = self.void_audience();
            w.audience_unknown = known.is_none();
            let peers = known.unwrap_or_default();
            for gen in &plan.streams {
                replica
                    .read_delegations()
                    .void_gen_epoch(super::DELEG_READ_EPOCH_BASE + *gen);
                let holder = self.dl.gens.get(gen).map(|g| g.node);
                for p in peers.iter().filter(|p| Some(**p) != holder) {
                    let req = self.op_id();
                    w.asked.insert(req, (*p, *gen));
                    *w.sent.entry(*gen).or_default() += 1;
                    out.push(Action::Send {
                        to: *p,
                        msg: PeerMsg::DelegReadVoid {
                            req,
                            gen: *gen,
                            scope: crate::event::VoidScope::Ended,
                        },
                    });
                }
            }
            self.stats.deleg_abandon_read_waits += 1;
        }
        tracing::warn!(
            node = me,
            gens = ?plan.streams,
            deadline = w.deadline.0,
            waiting = waits,
            "the operator abandons non-stealable stream(s): recalling their read delegations, else waiting them out"
        );
        self.dl.abandons.push(w);
        self.abandon_progress(now, replica, out);
    }

    /// End `gen` at the root's cursor for an abandon (`GroupAbandoned`
    /// journaled), returning the cursor. `forced`: `--force` overrode the
    /// heard-lately refusal; who was heard, for the audit.
    fn abandon_end_now(
        &mut self,
        now: Now,
        gen: u64,
        forced: Option<(NodeId, &'static str, u64)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
        let Some(g) = self.dl.gens.get(&gen) else {
            return Err(format!("generation {gen} is gone"));
        };
        let known = g
            .sealed_tail
            .iter()
            .map(|t| t.end)
            .chain(g.prev_tails.values().map(|t| t.end))
            .fold(g.cursor, u64::max);
        let cursor = g.cursor;
        let (heard_node, heard_role, heard_ms) = forced.unwrap_or((0, "", 0));
        tracing::warn!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            cursor,
            known,
            forced = forced.is_some(),
            heard_node,
            heard_role,
            heard_ms,
            "the operator abandoned a delegation{}: ending it at the root's cursor; rows only its holder or group held are lost",
            if forced.is_some() {
                " with --force, although this root heard from that node within delegation_ttl_ms"
            } else {
                ""
            }
        );
        self.end_generation(now, gen, Some(known), replica, out);
        if self.dl.gens.get(&gen).is_some_and(|g| g.ended) {
            self.stats.groups_abandoned += 1;
            if forced.is_some() {
                self.stats.groups_abandoned_forced += 1;
            }
            Ok(cursor)
        } else {
            Err(format!(
                "could not journal the end of generation {gen}; try again"
            ))
        }
    }

    /// `req` was a void request of an abandon: forget it. Whether it was.
    pub(crate) fn deleg_abandon_acked(&mut self, req: OpId) -> bool {
        self.dl
            .abandons
            .iter_mut()
            .any(|w| w.asked.remove(&req).is_some())
    }

    /// Every event, every answer to a void request and the deadline: end the
    /// streams whose read delegations are recalled or waited out, answer the
    /// commands that are done.
    pub(crate) fn abandon_progress(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.abandons.is_empty() {
            return;
        }
        let mut waits = std::mem::take(&mut self.dl.abandons);
        let mut keep = Vec::new();
        for mut w in waits.drain(..) {
            let mut failed = None;
            for gen in w.gens.clone() {
                let ended = self.dl.gens.get(&gen).is_none_or(|g| g.ended);
                if ended {
                    // The holder came back and released, or the log ended
                    // the stream otherwise: not ours.
                    w.gens.remove(&gen);
                    w.asked.retain(|_, (_, g)| *g != gen);
                    self.stats.deleg_abandon_superseded += 1;
                    w.lines.push(format!(
                        "generation {gen} ended meanwhile (by its holder's release or otherwise); nothing abandoned"
                    ));
                    continue;
                }
                // Only a deposition ends the command: a lease renewal that is
                // late (the root is not usable for a moment) is waited out,
                // the end is journaled once the root is usable again.
                if self.lease.lost || self.lease.epoch() != w.epoch {
                    failed = Some(
                        "this node is no longer the root; run the command again on the root"
                            .to_string(),
                    );
                    break;
                }
                let pending = w.asked.values().filter(|(_, g)| *g == gen).count();
                let blind = w.audience_unknown;
                if (pending > 0 || blind) && now.wait < w.deadline {
                    continue;
                }
                // The wait for a usable root is bounded: once the lease this
                // root holds has expired on its own clock, another node may
                // be the root, and the operator is told so.
                if now.wait >= w.deadline
                    && !self.root_ends_recall(now, gen)
                    && !self.lease.holds_unexpired(now, &self.cfg)
                {
                    failed = Some(
                        "this node is no longer the root (its lease expired); run the command again on the root"
                            .to_string(),
                    );
                    break;
                }
                if !self.root_ends_recall(now, gen) {
                    continue;
                }
                if pending > 0 || blind {
                    self.stats.deleg_abandon_reads_expired += 1;
                } else {
                    self.stats.deleg_abandon_reads_recalled += 1;
                }
                let sent = w.sent.get(&gen).copied().unwrap_or(0);
                let label = match self.dl.gens.get(&gen).map(|g| (g.kind(), g.node)) {
                    Some((DelegKind::Designated, n)) => ("designated", n),
                    Some((_, n)) => ("sticky", n),
                    None => ("", 0),
                };
                match self.abandon_end_now(now, gen, w.forced.get(&gen).copied(), replica, out) {
                    Ok(cursor) => {
                        w.gens.remove(&gen);
                        w.asked.retain(|_, (_, g)| *g != gen);
                        w.lines.push(format!(
                            "generation {gen} ({}) of node {} ended at index {cursor}; rows past it that only node {} held are lost \
                             to the log; node {} replays them by rid when it returns, refused ones become conflict copies; \
                             read delegations: recalled from {} node(s), waited out for {pending}",
                            label.0,
                            label.1,
                            label.1,
                            label.1,
                            sent - pending
                        ));
                    }
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                let done = w.lines.join("; ");
                out.push(Action::ControlDone {
                    op: w.op,
                    result: Err(if done.is_empty() {
                        e
                    } else {
                        format!("{e} (already done: {done})")
                    }),
                });
            } else if w.gens.is_empty() {
                out.push(Action::ControlDone {
                    op: w.op,
                    result: Ok(ControlOk::Text(w.lines.join("\n"))),
                });
            } else {
                keep.push(w);
            }
        }
        keep.append(&mut self.dl.abandons);
        self.dl.abandons = keep;
        self.abandon_arm(now, out);
    }

    /// The timer of the earliest deadline of a wait; none when no wait is left.
    fn abandon_arm(&mut self, now: Now, out: &mut Vec<Action>) {
        let next = self.dl.abandons.iter().map(|w| w.deadline).min();
        match (next, self.dl.abandon_timer) {
            (None, Some(t)) => {
                self.cancel_timer(t, out);
                self.dl.abandon_timer = None;
            }
            (Some(at), None) => {
                // A deadline that has passed while the root is not usable:
                // look again soon, not at once.
                let at = if at > now.wait {
                    at
                } else {
                    now.wait.plus(ABANDON_RETRY_MS)
                };
                let t = self.set_timer_wait(at, Timer::AbandonWait, out);
                self.dl.abandon_timer = Some(t);
            }
            _ => {}
        }
    }

    pub(crate) fn on_abandon_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.abandon_timer = None;
        self.abandon_progress(now, replica, out);
    }

    /// When this root last heard from `node`: a message from it, or a
    /// successful exchange on its link (the driver's directory, which forgets
    /// it once the link drops; this table does not).
    fn link_heard(&self, node: NodeId) -> Option<WaitMs> {
        self.peer_heard
            .get(&node)
            .copied()
            .max(self.roster.links().get(&node).and_then(|l| l.last_seen))
    }

    /// `node` was heard from at `at` (wait clock).
    pub(crate) fn note_peer_heard(&mut self, node: NodeId, at: WaitMs) {
        let e = self.peer_heard.entry(node).or_insert(at);
        *e = (*e).max(at);
    }

    /// When this root last heard from `g`'s delegate: a renewal, a stream
    /// batch, a recall's answer, or any exchange on its link.
    pub(crate) fn deleg_delegate_heard(&self, g: &GenState) -> Option<WaitMs> {
        g.delegate_heard.max(self.link_heard(g.node))
    }

    /// When this root last heard from `member` of `g`'s group: its seal's
    /// answer, or any exchange on its link.
    pub(crate) fn deleg_member_heard(&self, g: &GenState, member: NodeId) -> Option<WaitMs> {
        g.member_notes
            .get(&member)
            .and_then(|n| n.answered)
            .max(self.link_heard(member))
    }

    /// Plan 43 C2e follow-up (`repair abandon` without `--force`): the
    /// node of `g`'s group this root heard from within `delegation_ttl_ms`
    /// most lately — the delegate, or a strict member — as `(node, role, ms
    /// ago)`.
    fn deleg_group_heard_lately(
        &self,
        now: Now,
        g: &GenState,
    ) -> Option<(NodeId, &'static str, u64)> {
        let ago = |t: Option<WaitMs>| t.map(|t| now.wait.since(t).max(0) as u64);
        std::iter::once((g.node, "the delegate", ago(self.deleg_delegate_heard(g))))
            .chain(g.group.strict().iter().map(|m| {
                (
                    *m,
                    "a member of its group",
                    ago(self.deleg_member_heard(g, *m)),
                )
            }))
            .chain(g.prev_roots.iter().map(|m| {
                (
                    *m,
                    "a previous root of its generation",
                    ago(self.deleg_member_heard(g, *m)),
                )
            }))
            .filter_map(|(node, role, ago)| ago.map(|a| (node, role, a)))
            .filter(|(_, _, a)| *a < self.dl_t().abandon_heard_window)
            .min_by_key(|(_, _, a)| *a)
    }

    /// End `gen` here and now: the `DelegateNodeEnded` record journaled (the table
    /// row changes with it), later stream records of it refused, every
    /// parked execution waiting on it released.
    ///
    /// `abandoned`: the operator gave the generation's group up
    /// (`constellation repair abandon --gen`): the end is journaled
    /// with a [`LogRecord::GroupAbandoned`] (`through_known`: the highest
    /// index of the group the root knew of), the log's permanent record that
    /// rows the group may have acknowledged past the cut are lost.
    fn end_generation(
        &mut self,
        now: Now,
        gen: u64,
        abandoned: Option<u64>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended {
            return;
        }
        // Plan 46 M1.2 step 7 (c3-spec D6): not while a previous root's tail
        // may hold rows past the cursor (`abandon` is the operator's
        // way out: `abandoned`).
        if abandoned.is_none() && !g.prev_roots.is_empty() {
            self.deleg_end_waits_for_prev_roots(now, gen, out);
            return;
        }
        // Plan 46 M1.2 step 14 (L2b): a generation the delegate released
        // below rows its group may hold ends after a strict member's answer
        // (drained, or "holds nothing") with the cursor at the release's
        // cut, or by an abandon.
        debug_assert!(
            abandoned.is_some()
                || !g.release_seal
                || g.group.strict().is_empty()
                || (g.release_group_answered
                    && (g.cursor >= g.release_cut.unwrap_or(0) || g.release_cut_waived)),
            "a released recorded generation ends without its group's answer at the cut"
        );
        if let Some(at) = g.recall_started {
            let ms = now.wait.since(at).max(0) as u64;
            self.stats.deleg_recalls_timed += 1;
            self.stats.deleg_recall_ms_sum += ms;
            self.stats.deleg_recall_ms_max = self.stats.deleg_recall_ms_max.max(ms);
            if ms > 1_000 {
                self.stats.deleg_recall_over_1s += 1;
            }
        }
        // Whatever ends a generation passed a closed slot (an expiry, a
        // recall); closed here too, for every other way in.
        self.dw.close(gen);
        let node = g.node;
        let dirs = g.dir_list();
        let cursor = g.cursor;
        let drained = matches!(g.recall, RecallPhase::Drained(_));
        // Plan 46 M1.3 step 4 (coverage): how a sticky stream ended.
        if g.kind() == DelegKind::Sticky {
            if abandoned.is_some() {
                self.stats.deleg_sticky_ended_abandoned += 1;
            } else if g.node == self.cfg.node_id {
                // The holder itself took the lease (`delegation_sync`): it
                // executes the subtree as the root, which is its own end of
                // the grant, a holder's.
                self.stats.deleg_sticky_ended_released += 1;
            } else if drained {
                match g.recall_cause {
                    None => self.stats.deleg_sticky_ended_released += 1,
                    Some(RecallCause::Undelegate) => self.stats.deleg_sticky_ended_operator += 1,
                    Some(_) => self.stats.deleg_sticky_ended_recalled += 1,
                }
            } else {
                self.stats.deleg_sticky_ended_other += 1;
            }
        }
        // Plan 43 C2: the generation's end carries its cut: the rows this
        // root appended (`cursor`) — or, when this root was the
        // generation's delegate itself (inherited at a takeover), further:
        // its own rows past the cursor stay in its journal and ship after
        // the record, so they are not lost. Unless its takeover stranded
        // them (`strand_for_takeover`, a predecessor's backup tail to
        // re-apply): then none of them stayed, they are replayed by rid as
        // root rows, and the cut is the cursor. A cut above it called a
        // dependency on a stranded row present here while the next root
        // refused it as lost: the dependent op `Held`, re-sent with the
        // same deps, forever (`delegated-holder-cut` seed 139 under the
        // clock mix). Worked out before the record is journaled; every
        // replica reads it from the record.
        let own = self.dl.mine.contains_key(&gen) && !replica.deleg_ended_at_gate(gen);
        let through = if own {
            cursor.max(replica.stream_applied(gen))
        } else {
            cursor
        };
        let _class = self.journal_class();
        let mut records = vec![LogRecord::DelegateNodeEnded { node, gen, through }];
        if let Some(through_known) = abandoned {
            records.push(LogRecord::GroupAbandoned {
                epoch: 0,
                gen,
                through_known,
            });
        }
        if let Err(error) = replica.apply_records_journaled(&records, None) {
            tracing::warn!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), %error, "could not journal the recall record");
            self.arm_gen_expiry(now.plus(500), gen, out);
            return;
        }
        replica.void_stream(gen, through);
        replica.note_void_cut(gen, through);
        // What this root holds of the generation's replies (its own
        // forwarded ops' shadows and hints) is stranded like on any
        // other replica applying the `DelegateNodeEnded` (long-delegated seed 72780).
        match replica.strand_recalled_speculation(gen) {
            Ok(s) if s.any() => {
                tracing::info!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    shadows = s.shadows,
                    hints = s.hints,
                    "the root's own speculation of an ended generation stranded; replaying"
                );
                self.stats.speculation_rolled_back += (s.shadows + s.hints) as u64;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), %error, "could not strand the root's speculation of an ended generation");
            }
        }
        self.lock_note_replay_pins(now, replica);
        self.lease.touch(now);
        self.nudge(now, out);
        // Plan 30 §M14: grants handed to this generation and not handed
        // back (still waiting for its first renewal — sim seed 96027 —,
        // or sent with a renewal reply its recall overtook or that was
        // lost — seeds 196252, 196102) come back to this table, where
        // the subtree's next delegation takes them along. An outwaited
        // generation also leaves a grace on the subtree (sim seed 96046).
        self.lock_on_generation_ended(now, gen, replica);
        if !drained {
            for dir in &dirs {
                self.lock_on_generation_outwaited(now, *dir);
            }
        }
        tracing::info!(
            node = self.me(),
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?dirs,
            cursor,
            "delegation generation ended"
        );
        self.mark_ended(now, gen, replica, out);
        // The record is journaled here, not applied from a segment: a
        // generation this node held itself (inherited by a successor
        // that was the delegate) ends on the delegate side too.
        // (ADR-55: not while the release of the root's own stream has not
        // shipped: this node, deposed, streams the row to the next root.)
        let own_release_unshipped = self.dl.own_minted.contains(&gen)
            && self.dl.mine.get(&gen).is_some_and(|d| d.release.is_some());
        if self.dl.mine.contains_key(&gen) && !own_release_unshipped {
            self.drop_delegate_state(now, gen, replica, out);
        }
    }

    /// The end of `gen` was asked for while previous roots are unsettled: it
    /// waits (the status says so, a WARN once a minute) and the expiry asks
    /// again.
    fn deleg_end_waits_for_prev_roots(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        self.stats.deleg_prev_root_waits += 1;
        let warn = {
            let Some(g) = self.dl.gens.get_mut(&gen) else {
                return;
            };
            g.prev_blocked = true;
            g.waiting_for_group = true;
            g.waiting_since.get_or_insert(now.wait);
            let due = g
                .waiting_warned
                .is_none_or(|at| now.wait.since(at) >= 60_000);
            if due {
                g.waiting_warned = Some(now.wait);
            }
            due
        };
        if warn {
            tracing::warn!(
                node = self.me(),
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                "a delegation's previous root has not answered its seal: the generation is not ended without its tail \
                 (`constellation repair abandon --gen {gen}` ends it at the root's cursor; rows only it held are lost)"
            );
        }
        self.dw.close(gen);
        self.dw
            .set_until_closed(gen, now.wait.plus(self.release_seal_window()));
        self.arm_gen_expiry(now, gen, out);
    }

    fn mark_ended(&mut self, now: Now, gen: u64, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        let first = !g.ended;
        if !g.prev_roots.is_empty() || !g.prev_reqs.is_empty() {
            self.persist_prev_roots(gen, &[], replica);
            let Some(g) = self.dl.gens.get_mut(&gen) else {
                return;
            };
            for req in std::mem::take(&mut g.prev_reqs).keys() {
                self.dl.by_req.remove(req);
            }
            let g = self.dl.gens.get_mut(&gen).expect("present");
            g.prev_roots.clear();
            g.prev_tails.clear();
            g.prev_blocked = false;
        }
        let g = self.dl.gens.get_mut(&gen).expect("present");
        g.ended = true;
        g.waiting_for_group = false;
        g.waiting_since = None;
        g.sealed_tail = None;
        g.seal_reqs.clear();
        g.release_answered = None;
        g.release_answered_at = None;
        let release_timer = g.release_seal_timer.take();
        self.dw.close(gen);
        if let Some(t) = release_timer {
            self.cancel_timer(t, out);
        }
        if first {
            let due = self.deleg_forget_due(now, gen);
            self.dl.forget_at.insert((due, gen));
        }
        self.dl.answered_granting.remove(&gen);
        self.dl.floor_sent.remove(&gen);
        let g = self.dl.gens.get_mut(&gen).expect("present");
        let expiry = g.expiry.take();
        let recall_tries = std::mem::take(&mut g.recall_tries);
        g.recall_req = None;
        let resend = g.recall_resend.take();
        let controls = std::mem::take(&mut g.controls);
        if let Some(t) = expiry {
            self.cancel_timer(t, out);
        }
        if let Some(t) = resend {
            self.cancel_timer(t, out);
        }
        for req in recall_tries {
            self.dl.by_req.remove(&req);
        }
        self.stats.deleg_ended += 1;
        for c in controls {
            out.push(Action::ControlDone {
                op: c,
                result: Ok(ControlOk::Text(format!("generation {gen} ended"))),
            });
        }
        // Parked executions waiting on this generation.
        self.deleg_wait_done(now, wait_id(gen), replica, out);
    }

    /// A batch from a generation's delegate is as good a sign of life as
    /// its renewal: the root does not outwait (nor seal through its
    /// backup) a delegate that is streaming to it. Under load the root
    /// handles renewals and batches late, not never; a delegate whose
    /// renewals queue behind (or whose answers came too late to count, as
    /// before the answers were one way) is still here, and sealing it at
    /// the cursor it is streaming from left its writes waiting for minutes
    /// (`stress-ng-fs-nodes`). Only the root's own wait moves: the
    /// delegate honours its grant from its renewals alone, so a later
    /// `until` here is never less safe. Not during a recall (no renewal is
    /// granted then either: the recall's outwait stands).
    fn deleg_live_by_stream(&mut self, now: Now, gen: u64) {
        let ttl = self.cfg.delegation_ttl_ms.min(self.grant_cap_ms(now));
        if ttl == 0 || self.dl.epoch_active {
            return;
        }
        let extra = self.reclaim_horizon_ms() - self.cfg.delegation_ttl_ms;
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended || g.recall != RecallPhase::None {
            return;
        }
        // (The expiry timer re-arms itself when it fires before `until`.)
        if self
            .dw
            .renewed_until(gen)
            .is_some_and(|renewed| now.wait >= renewed)
        {
            // The renewals alone would have let it be outwaited.
            self.stats.deleg_live_by_stream += 1;
        }
        self.dw.extend_core(gen, now.wait.plus(ttl + extra), false);
    }

    /// The root appends a delegate's batch, and acknowledges the stream
    /// through its cursor (cumulative: a batch it already holds, re-sent
    /// because the delegate gave up on an answer that was only late, is
    /// answered with the same cursor).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_delegate_stream(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        leaving: Vec<constellation_meta::locks::GrantWire>,
        leaving_barriers: Vec<(Ino, i64)>,
        released: Option<constellation_meta::locks::ReleaseNote>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if gen == LEAVE_NOTE_GEN {
            self.on_leave_note(now, from, req, released, out);
            return;
        }
        // Plan 46 M0.3 (S5, ADR-42): a takeover gate whose journal work is
        // done takes its inherited delegates' batches. `root_ends_recall`
        // stays as it was: it also gates `on_deleg_expiry`, and the gate
        // reclaims and seals nothing.
        let usable = (self.root_ends_recall(now, gen) || self.gated_takes_stream(now, gen))
            && self.cfg.delegation;
        // A root back from a restart that the lease object still names, and
        // that holds nothing yet (a restart with nothing of its own to ship
        // does not acquire on its own), refuses its delegate's batches for as
        // long as nobody asks it to be the root: the delegate backs off and
        // re-sends, the forwards that would re-adopt it go to the delegate
        // instead (`delegated-no-backup-root-near` seed 789: no root for
        // 538 s). As for a forward and a renewal, the batch re-adopts the
        // lease (`readopt_own_lease_for_forward` checks that it still names
        // this node); this one is refused, and the delegate's next re-send is
        // answered by the root it re-adopted.
        if !usable && self.cfg.delegation {
            self.readopt_own_lease_for_forward(now, replica, out);
        }
        let refuse = |out: &mut Vec<Action>, through: u64| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::DelegateStreamAck {
                    req,
                    gen,
                    round,
                    through,
                    committed: 0,
                    refused: true,
                    archive_bound: false,
                },
            });
        };
        let Some(g) = self.dl.gens.get(&gen) else {
            self.stats.deleg_stream_refusals += 1;
            refuse(out, 0);
            return;
        };
        if !usable || g.ended || g.node != from {
            self.stats.deleg_stream_refusals += 1;
            if g.ended
                && released
                    .as_ref()
                    .is_some_and(|n| !n.locks.grants.is_empty())
            {
                // A delegate's lock grants that come after the generation
                // ended: nobody honours them any more, and none is installed
                // (`lock_returned_after_end` counted the late answer of a
                // recall before the release rode the stream).
                self.stats.lock_returned_after_end +=
                    released.as_ref().map_or(0, |n| n.locks.grants.len() as u64);
            }
            refuse(out, g.cursor);
            return;
        }
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.delegate_heard = Some(now.wait);
        }
        if released.as_ref().is_some_and(|n| n.leaving) && !self.leave_note_stale(from, req) {
            self.leave_mark(now, from, Some(req));
        }
        self.deleg_live_by_stream(now, gen);
        let g = self.dl.gens.get(&gen).expect("present");
        tracing::debug!(
            node = self.me(),
            from,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?req,
            first = txs.first().map(|t| t.idx).unwrap_or(0),
            last = txs.last().map(|t| t.idx).unwrap_or(0),
            cursor = g.cursor,
            "delegate stream batch received"
        );
        let mut cursor = g.cursor;
        let mut appended = 0u64;
        let batch_last = txs.last().map(|t| t.idx).unwrap_or(0);
        // Plan 46 M1.2 step 10: the lock grants of the subtrees the delegate
        // released are back in this table before the root executes under
        // them (each grant once, whichever batch brought it).
        let mut released_rows: Vec<Ino> = Vec::new();
        let mut released_all = false;
        if let Some(note) = &released {
            let dirs: Vec<Ino> = note.dirs.clone();
            self.lock_install_returned(now, gen, note.locks.clone(), &dirs, replica);
        }
        for tx in txs {
            if tx.idx <= cursor {
                continue;
            }
            if tx.idx != cursor + 1 {
                // A gap: the delegate re-sends from the cursor.
                break;
            }
            // The model's causal-cut assertion at the root: a delegate
            // waited for these before executing, so they are in the log
            // (and in this journal) already. Not so: refuse the batch
            // and count it; the delegate re-sends after a tick.
            if self.deps_from_a_newer_tenure(&tx.deps) {
                self.stats.deleg_append_newer_tenure += 1;
                break;
            }
            // A dependency on a row its generation ended without (a
            // delegate's row past the cut, stranded and replayed by rid
            // after the end): the void rule would count it as satisfied,
            // and the row would land ahead of its cause. Not appended; the
            // delegate stops the generation before it (`deleg_stream`).
            if replica.deps_lost(&tx.deps) {
                self.stats.deleg_append_deps_lost += 1;
                tracing::debug!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    idx = tx.idx,
                    deps = ?tx.deps,
                    "delegate batch refused: a dependency was lost with its generation"
                );
                break;
            }
            if !replica.reaches_streams(&tx.deps) {
                // A refusal, not a causal cut: nothing was appended. A root
                // that was deposed without knowing it (the delegate
                // re-streams to the new root, whose segment it has not
                // applied) lacks deps the delegate did wait for.
                self.stats.deleg_deps_refused += 1;
                tracing::debug!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    idx = tx.idx,
                    deps = ?tx.deps,
                    applied = ?replica.applied_position(),
                    "delegate batch refused: the root lacks its deps"
                );
                break;
            }
            // ADR-55: this node's own stream: the holder's row is in this
            // journal already, with its origin `(gen, idx)`, and ships in
            // this node's segment (a release row has no rid to dedup by, so
            // nothing is applied a second time). The log's index of the
            // stream is the log's: it rises when a segment that carries the
            // row is applied, here or at the next root, which a re-stream
            // after a deposition starts from.
            let applied = if from == self.me() {
                Ok(false)
            } else {
                replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps)
            };
            self.stats.deleg_append_skipped += replica.take_deleg_append_skipped();
            self.stats.deleg_append_uncompleted += replica.take_deleg_append_uncompleted();
            match applied {
                Ok(_) => {
                    // (No causal-cut check here: `reaches_streams` held
                    // above and only rises, so a check after the append
                    // could never fire. The cut is judged on the refusals:
                    // see `deleg_deps_refused`.)
                    replica.note_foreign_executed(&tx.records);
                    cursor = tx.idx;
                    appended += 1;
                    for r in &tx.records {
                        match r {
                            LogRecord::SubtreeReleased { dir, .. } => released_rows.push(*dir),
                            LogRecord::StreamReleased { .. } => released_all = true,
                            _ => {}
                        }
                    }
                    // The op's origin, not the delegate that executed it: a
                    // forwarded op counts for its requester. (ADR-55: the
                    // root's own rows in its own stream are not counted here:
                    // the FUSE fast path that ran them counted them already
                    // (`place_note_local`), and the core counts the ones it
                    // ran itself when it ran them.)
                    let origin = tx.rid.map(|r| r.node).unwrap_or(from);
                    if self.cfg.placement && !(origin == self.me() && from == self.me()) {
                        let keys = TouchSet::from_records(tx.records.iter());
                        let dirs = Core::dirs_of_keys(&keys, replica);
                        self.place_note(origin, dirs);
                    }
                }
                Err(error) => {
                    tracing::warn!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), idx = tx.idx, %error, "could not append a delegate transaction");
                    break;
                }
            }
        }
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.cursor = cursor;
        }
        // The delegate's release rows are in this log: the subtrees are
        // sequenced here from now on (the table of this replica forgot them
        // with the rows), and the ops that waited for them run.
        let cut = released.as_ref().and_then(|n| n.end_at);
        let seal = released.as_ref().is_some_and(|n| n.seal);
        let no_stream = released.as_ref().is_some_and(|n| n.seal && n.no_stream);
        if !released_rows.is_empty() || released_all || cut.is_some() {
            self.root_release_appended(
                now,
                gen,
                &released_rows,
                released_all,
                cut.map(|c| (c, seal, no_stream)),
                replica,
                out,
            );
        }
        // Grants on inodes the batch takes out of the subtree: this
        // table's from the step its rows are applied in.
        if !leaving.is_empty() && cursor >= batch_last {
            self.lock_install_leaving(now, gen, leaving, replica);
        }
        if !leaving_barriers.is_empty() && cursor >= batch_last {
            self.lock_install_leaving_barriers(leaving_barriers, replica);
        }
        if appended > 0 {
            self.lock_take_back_left(now, gen, replica);
        }
        if appended > 0 {
            self.stats.deleg_appended_txs += appended;
            if self.root_gated_shippable(now) {
                self.stats.deleg_appended_gated += appended;
            }
            // The root's own ops forwarded to this delegate may await
            // the log for exactly these completions, or be parked on a
            // recall of it.
            self.answer_awaiting_log(now, replica, out);
            self.release_exec_parks_completed(now, replica, out);
            self.lease.touch(now);
            self.nudge(now, out);
        }
        // `through` is what the root appended (the delegate's flow control);
        // `committed` what its own tenure has committed of that, the root's
        // copy for the delegate's group (ADR-41). The rest follows by a
        // push when the commit rises (`root_stream_push`).
        // A new round (a new root of the delegate, a restarted stream) counts
        // from nothing: the delegate has reset what it counted of this
        // tenure's commit, and the cap scans the generation in full, once.
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            if g.last_batch.is_none_or(|(r, _)| r != round) {
                g.committed_sent = 0;
            }
        }
        let committed = cursor.min(self.root_append_cap(gen, replica));
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.last_batch = Some((round, req));
            g.committed_sent = committed;
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegateStreamAck {
                req,
                gen,
                round,
                through: cursor,
                committed,
                refused: false,
                archive_bound: self.config_archive_bound() && committed < cursor,
            },
        });
        self.root_progress(now, replica, out);
        // Executions parked on these rows' deps.
        if appended > 0 && self.has_deps_parks() {
            self.complete_ready(now, replica, out);
        }
    }

    // ----------------------------------------------------- controls

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_control_delegate(
        &mut self,
        now: Now,
        op: OpId,
        dir: Ino,
        node: NodeId,
        sticky: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let fail = |out: &mut Vec<Action>, msg: String| {
            out.push(Action::ControlDone {
                op,
                result: Err(msg),
            });
        };
        let kind = if sticky {
            DelegKind::Sticky
        } else {
            DelegKind::Manual
        };
        match self.delegate_dir(now, dir, node, kind, replica, out) {
            Ok(gen) => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(format!(
                    "delegated dir {dir} to node {node} (gen {gen})"
                ))),
            }),
            Err(msg) => fail(out, msg),
        }
    }

    /// Delegate `dir` to `node` as `kind`: the `DelegateNode` record and the
    /// root's generation state. The checks every path shares (an
    /// operator's control, the placement, a designation, a
    /// re-delegation after a cross-subtree op).
    pub(crate) fn delegate_dir(
        &mut self,
        now: Now,
        dir: Ino,
        node: NodeId,
        kind: DelegKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
        self.delegate_dir_from(GrantSource::Operator, now, dir, node, kind, replica, out)
    }

    /// [`Self::delegate_dir`] for a grant asked by `source`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delegate_dir_from(
        &mut self,
        source: GrantSource,
        now: Now,
        dir: Ino,
        node: NodeId,
        kind: DelegKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
        // ADR-55 R-I1: the root directory is the root sequencer's, whoever
        // asks and whatever the kind.
        if dir == ROOT_INO && !self.cfg.root_sticky_on_slash {
            return Err("the root directory is never delegated".into());
        }
        if !self.cfg.delegation {
            return Err("delegation is off (CONSTELLATION_DELEGATION)".into());
        }
        if !self.cfg.p2p {
            return Err("delegation needs P2P".into());
        }
        if !self.root_usable(now) {
            return Err("this node does not hold the lease".into());
        }
        if self.dl.epoch_active || self.epoch.open {
            return Err("no delegation while a continuation epoch is open".into());
        }
        // ADR-55: the root holds its own sticky grant (placement or
        // handover); never an operator's, and never another kind.
        let own = node == self.me();
        if own && (kind != DelegKind::Sticky || source == GrantSource::Operator) {
            return Err("cannot delegate to the root itself".into());
        }
        if !own && self.is_leaving(now, node) {
            return Err(format!("node {node} is leaving; nothing is granted to it"));
        }
        // Plan 46 M1.2 step 13: no lone delegate. A stealable grant goes
        // only to a node with a group within budget (a peer within
        // `CONSTELLATION_BACKUP_RTT_BUDGET_MS`, the root included); a sticky
        // or designated one is final and needs none. A far writer's ops are
        // forwarded to the root.
        if !kind.non_stealable() && !self.delegate_group_in_budget(node) {
            self.stats.delegate_refused_far += 1;
            return Err(format!(
                "node {node} has no peer within the backup RTT budget ({} ms), the root \
                 included; its ops are forwarded (a sticky grant answers there: \
                 `delegate --sticky`)",
                self.cfg.backup_rtt_budget_ms
            ));
        }
        // Plan 46 M1.2 step 9: a node holds one stream, and a stream never
        // mixes grant kinds (maintainer decision 2026-11-07): it holds
        // designations only (final, never reclaimed) or stealable grants
        // only (group-committed, reclaimed whole). A grant to a node whose
        // stream is live grows it (below); one to a node with none opens
        // its stream. Checked before anything is touched, so a refusal
        // costs nothing and the placement does not retry it every round.
        let live_stream = self
            .dl
            .gens
            .iter()
            .find(|(_, g)| g.node == node && !g.ended)
            .map(|(gen, _)| *gen);
        if let Some(gen) = live_stream {
            let g = &self.dl.gens[&gen];
            let held = g.kind();
            if !held.joins(kind) {
                if held.non_stealable() && kind.non_stealable() {
                    return Err(format!(
                        "node {node} holds {} directories only; the other kind of grant that can \
                         never be reclaimed waits until they are released",
                        if held == DelegKind::Sticky {
                            "sticky"
                        } else {
                            "designated"
                        }
                    ));
                }
                if held.non_stealable() {
                    return Err(format!(
                        "node {node} holds {} directories only; a grant that can be \
                         reclaimed waits until they are released",
                        if held == DelegKind::Sticky {
                            "sticky"
                        } else {
                            "designated"
                        }
                    ));
                }
                if kind == DelegKind::Sticky {
                    return Err(format!(
                        "node {node} holds delegations that can be reclaimed; \
                         `undelegate` them first (a stream never mixes grant kinds)"
                    ));
                }
                // A designation on a node with a stealable stream: that
                // stream is recalled first, then the designation opens its
                // own stream (the next designation sync). Nothing stealable
                // is granted to the node meanwhile (`designating`).
                self.dl.designating.insert(dir, node);
                if g.recall == RecallPhase::None && !g.waiting_for_group && !g.prev_blocked {
                    tracing::info!(
                        node = self.me(),
                        dir,
                        designee = node,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        "a designation recalls the node's stealable stream first (a stream never mixes grant kinds)"
                    );
                    self.start_recall(now, gen, out);
                }
                return Err(format!(
                    "node {node}'s stealable delegation is being recalled before the designation"
                ));
            }
            if g.recall != RecallPhase::None || g.waiting_for_group || g.prev_blocked {
                return Err(format!("node {node}'s delegation is being recalled"));
            }
        } else if kind != DelegKind::Designated && self.dl.designating.values().any(|n| *n == node)
        {
            // Neither a stealable grant nor a sticky one: the designation's
            // stream opens first (a stream never mixes grant kinds).
            return Err(format!(
                "a designation to node {node} is pending; no other kind of grant is made to it \
                 until the designation's stream is open"
            ));
        }
        // The delegate is another node: a root that still counts itself
        // alone starts the kernel drain now, so the first renewal (and a
        // designation) waits for it with the rest of the quarantine.
        if !own {
            self.note_foreign(now, replica, out);
        }
        let table = replica.delegation_table();
        if let Some(d) = table.covering(replica.namespace(), dir) {
            return Err(format!(
                "directory {dir} is under delegation {} (gen {})",
                d.dir, d.gen
            ));
        }
        // A delegation on a descendant of `dir` would be shadowed.
        if table
            .iter()
            .any(|d| d.dir != dir && is_under(replica.namespace(), d.dir, dir))
        {
            return Err(format!("a directory under {dir} is already delegated"));
        }
        // Plan 30 §M8 under M11: the read delegations this root granted
        // under `dir` must be gone before the delegate executes, since its
        // writes recall only the grants in its own table. A designee
        // executes from the moment it installs the record (no renewal), so
        // a designation is journaled only once there are none: they are
        // recalled, nothing more is granted under `dir`, and the next sync
        // delegates. Any other generation is journaled now (nothing more
        // is granted under it from here) and its first renewal waits.
        let read_grants = self.read_grants_under(now, dir, replica);
        // And the read delegations no table here lists (granted before a
        // restart, or by the tenure this one took over early, or cached by
        // a lone node's kernel): while the quarantine that bounds them
        // runs, the first renewal waits for it too, and a designation is
        // not journaled.
        let quarantine = self.first_renewal_quarantine(now, replica);
        self.dl.growing.retain(|_, (_, until)| *until > now.wait);
        if live_stream.is_some() && !kind.non_stealable() {
            // A grown subtree has no first renewal to wait out the quarantine
            // (or the read delegations) with, and the stream's renewals must
            // not wait for them (they renew the honoured subtrees): it is
            // granted once they are gone, as a designation is.
            if let Some(q) = quarantine {
                return Err(format!(
                    "read delegations granted before this tenure may be honoured for {} ms",
                    q.since(now.wait)
                ));
            }
            if !read_grants.grants.is_empty() {
                let n = read_grants.grants.len();
                let until = read_grants
                    .grants
                    .iter()
                    .map(|g| g.until)
                    .max()
                    .expect("non-empty");
                self.dl.growing.insert(dir, (node, until));
                self.start_recalls(now, read_grants, out);
                return Err(format!(
                    "{n} read delegations under directory {dir} are being recalled"
                ));
            }
            self.dl.growing.remove(&dir);
        }
        if kind.non_stealable() {
            // A non-stealable holder executes from the moment it installs the
            // record (no renewal), so the grant is journaled only once no
            // read delegation under `dir` is left. A designation's wait is
            // kept in `designating` (the designation sync retries it); a
            // sticky grant's is an error the operator retries.
            let designating = kind == DelegKind::Designated;
            if !read_grants.grants.is_empty() {
                if designating {
                    self.dl.designating.insert(dir, node);
                }
                let n = read_grants.grants.len();
                self.start_recalls(now, read_grants, out);
                return Err(format!(
                    "{n} read delegations under directory {dir} are being recalled"
                ));
            }
            if let Some(q) = quarantine {
                if designating {
                    self.dl.designating.insert(dir, node);
                }
                return Err(format!(
                    "read delegations granted before this tenure may be honoured for {} ms",
                    q.since(now.wait)
                ));
            }
            self.dl.designating.remove(&dir);
        }
        if let Some(gen) = live_stream {
            let grown = self.grow_generation(now, gen, dir, kind, replica, out);
            if own && grown.is_ok() {
                // The holder side learns its own growth at once.
                self.delegation_sync(now, replica, out);
            }
            return grown;
        }
        // ADR-43: a generation is a stream's identity, minted by one
        // tenure in its own epoch's block (`epoch << 32 | n`), the epoch of
        // the S3 lease this node holds (one holder per lease epoch, so two
        // tenures never mint the same number, even when one of them was
        // deposed without knowing it). Above every number the log named
        // as the floor of a lost counter; a number outside the block, from
        // any source, is refused.
        let epoch = self
            .lease
            .held_epoch()
            .ok_or("this node holds no S3 lease epoch to mint a generation in")?;
        let highest = table.max_gen();
        let gen = replica
            .next_delegation_gen(epoch, highest + 1)
            .map_err(|e| e.to_string())?;
        let _class = self.journal_class();
        constellation_meta::clear_last_journaled();
        replica
            .apply_records_journaled(
                &[LogRecord::delegate_one(node, gen, dir, kind.subtree_kind())],
                None,
            )
            .map_err(|e| e.to_string())?;
        if own {
            self.note_own_grant_row(gen);
            self.dl.own_minted.insert(gen);
        }
        let read_recalls = self
            .start_recalls(now, read_grants, out)
            .map(|(ids, _)| ids)
            .unwrap_or_default();
        // A first renewal that waits out the quarantine must not find the
        // generation reclaimed for its silence meanwhile.
        let first_until = quarantine
            .map_or(now.wait, |q| q.max(now.wait))
            .plus(self.reclaim_horizon_ms());
        self.dw.insert(
            gen,
            node,
            first_until,
            first_until,
            !self.ttl_exempt_kind(kind),
        );
        self.dl.gens.insert(
            gen,
            GenState {
                dirs: BTreeMap::from([(dir, kind)]),
                node,
                group: GroupState::default(),
                redelegate_dirs: BTreeMap::new(),
                releasing: BTreeSet::new(),
                granted: now.wait,
                inherited: false,
                grown_at: None,
                gate_recalled: false,
                below_since: None,
                seal_reqs: BTreeMap::new(),
                sealed_tail: None,
                waiting_for_group: false,
                waiting_warned: None,
                waiting_since: None,
                member_notes: BTreeMap::new(),
                prev_roots: BTreeSet::new(),
                prev_reqs: BTreeMap::new(),
                prev_tails: BTreeMap::new(),
                prev_blocked: false,
                prev_done: BTreeSet::new(),
                delegate_heard: None,
                cursor: 0,
                last_batch: None,
                committed_sent: 0,
                recall: RecallPhase::None,
                recall_before_seal: RecallPhase::None,
                release_cut: None,
                release_seal: false,
                release_seal_timer: None,
                release_answered: None,
                release_group_answered: false,
                release_cut_waived: false,
                release_answered_at: None,
                ended: false,
                expiry: None,
                recall_req: None,
                recall_tries: Vec::new(),
                recall_started: None,
                recall_cause: None,
                release_resends: 0,
                recall_resend: None,
                recall_backoff_ms: 0,
                controls: Vec::new(),
                read_recalls,
                renew_waiting: None,
            },
        );
        self.arm_gen_expiry(now, gen, out);
        // Plan 30 §M14: the subtree's lock grants go with it.
        self.lock_on_delegated(gen, replica);
        self.lock_reserve_all(now, replica, out);
        self.stats.deleg_delegated += 1;
        match kind {
            DelegKind::Placed => self.stats.place_delegated += 1,
            DelegKind::Designated => self.stats.deleg_designated += 1,
            DelegKind::Sticky => self.stats.deleg_sticky += 1,
            DelegKind::Manual => {}
        }
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            dir,
            delegate = node,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?kind,
            "delegated"
        );
        if own {
            // ADR-55: the holder side is this node: it installs the stream
            // now (honoured once the record is archived, R-I3).
            self.stats.place_root_own_granted += 1;
            self.delegation_sync(now, replica, out);
        }
        Ok(gen)
    }

    /// Whether stream `gen` is one this node's own tenure granted itself (its
    /// number is minted in an epoch block of a lease this node held): the
    /// node was the root when it was made.
    fn minted_by_me(&self, gen: u64) -> bool {
        self.dl.own_minted.contains(&gen)
    }

    /// ADR-55: remember the journal seq of the record `delegate_dir` just
    /// journaled for the root's own stream `gen`.
    fn note_own_grant_row(&mut self, gen: u64) {
        let record = match constellation_meta::take_last_journaled() {
            Some(row) => row.last,
            // (No row was reported: never honoured on it.)
            None => u64::MAX,
        };
        let since = self
            .dl
            .own_grants
            .get(&gen)
            .map_or(self.clk.wait, |g| g.since);
        self.dl.own_grants.insert(
            gen,
            OwnGrant {
                record,
                scan_to: record,
                honoured: false,
                since,
                covered_at: None,
            },
        );
    }

    /// ADR-55 (R-I3, M3): a process that restarted over its journal installs
    /// the stream it granted itself again, and its `DelegateNode` record may
    /// be in the unshipped journal still (the root was cut off from S3 when it
    /// made the grant, and rebooted before the archive carried it). The
    /// record's row is found there again, so the stream waits for the archive
    /// as before the restart: honoured meanwhile, its `Final` answers would
    /// rest on a grant the log never had (`far-root-writes-paused+durability`
    /// seeds 917, 1854).
    fn recover_own_grant_row(&mut self, gen: u64, replica: &dyn Replica) {
        if self.dl.own_grants.contains_key(&gen) || self.dl.own_minted.contains(&gen) {
            return;
        }
        // (A delegate's journal holds stream rows only; the root's own rows
        // are what can hold the record.)
        if !replica.journal_has_undelegated() {
            return;
        }
        let from = replica.journal_acked_seq().unwrap_or(0) + 1;
        let row = replica
            .journal_txs_from(from, usize::MAX)
            .into_iter()
            .filter(|tx| {
                tx.records
                    .iter()
                    .any(|r| matches!(r, LogRecord::DelegateNode { gen: g, .. } if *g == gen))
            })
            .map(|tx| tx.last)
            .max();
        // (R-H: the record may be in the log already while a root row of the
        // stream's keys is not: (b) then looks at the root rows journaled so
        // far.)
        let (record, scan_to) = match row {
            Some(last) => (last, last),
            None => (0, replica.journal_root_bound()),
        };
        self.dl.own_grants.insert(
            gen,
            OwnGrant {
                record,
                scan_to,
                honoured: false,
                since: self.clk.wait,
                covered_at: None,
            },
        );
    }

    /// Plan 46 M1.2 step 9: grant `dir` to the node that holds stream `gen`:
    /// the stream grows (`DelegateNode` again, the same generation; a window,
    /// a cut and a group exist already). The root's read delegations under
    /// `dir` are gone already (`delegate_dir` recalls them first and refuses
    /// meanwhile), so no renewal of the stream waits for any. The root stops
    /// sequencing `dir` at once. The delegate honours it once the record is
    /// committed and the root has answered a renewal sent after that
    /// ([`Self::deleg_unpark_grown`]): the answer carries the lock handoff of
    /// the new subtree.
    fn grow_generation(
        &mut self,
        now: Now,
        gen: u64,
        dir: Ino,
        kind: DelegKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
        let node = self.dl.gens[&gen].node;
        let _class = self.journal_class();
        constellation_meta::clear_last_journaled();
        replica
            .apply_records_journaled(
                &[LogRecord::delegate_one(node, gen, dir, kind.subtree_kind())],
                None,
            )
            .map_err(|e| e.to_string())?;
        if node == self.me() {
            self.note_own_grant_row(gen);
        }
        let g = self.dl.gens.get_mut(&gen).expect("present");
        g.dirs.insert(dir, kind);
        g.grown_at = Some(now.wait);
        g.below_since = None;
        self.dw.unsettle(gen);
        // Plan 30 §M14: the new subtree's lock grants go with it (handed
        // over with the next granting renewal).
        self.lock_on_delegated(gen, replica);
        self.lock_reserve_all(now, replica, out);
        self.stats.deleg_delegated += 1;
        self.stats.deleg_grows += 1;
        match kind {
            DelegKind::Placed => self.stats.place_delegated += 1,
            DelegKind::Designated => self.stats.deleg_designated += 1,
            DelegKind::Sticky => self.stats.deleg_sticky += 1,
            DelegKind::Manual => {}
        }
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            dir,
            delegate = node,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?kind,
            "grew a delegation's stream by a subtree"
        );
        Ok(gen)
    }

    /// How long the root honours a grant it cannot see renewed: the
    /// grant's ttl plus the margin. The read delegations a delegate
    /// grants under it (phase 2b, M8) need no extra term: each is capped
    /// by the delegate's own `until` less the margin, so every one of
    /// them lapses before the grant does.
    pub(crate) fn reclaim_horizon_ms(&self) -> u64 {
        self.dl_t().reclaim_horizon
    }

    /// The live read delegations (M8) this root granted on `dir` or on
    /// anything under it, to other nodes (a walk up from each grant's
    /// inode: bounded by the grants, not by the subtree).
    fn read_grants_under(&self, now: Now, dir: Ino, replica: &dyn Replica) -> RecallNeed {
        if !self.cfg.read_delegations || !self.cfg.recall_before_ack {
            return RecallNeed::default();
        }
        let ns = replica.namespace();
        let grants = replica
            .read_delegations()
            .all_live(now.wait)
            .grants
            .into_iter()
            .filter(|g| is_under(ns, g.ino, dir))
            .collect();
        RecallNeed {
            grants,
            quarantine_until: None,
        }
    }

    /// Whether `ino` is under a designation waiting for this root's read
    /// delegations to be recalled ([`DelegationState::designating`]): no
    /// read delegation is granted on it.
    pub(crate) fn under_pending_designation(
        &self,
        now: Now,
        ino: Ino,
        replica: &dyn Replica,
    ) -> bool {
        let ns = replica.namespace();
        self.dl
            .designating
            .keys()
            .any(|dir| is_under(ns, ino, *dir))
            || self
                .dl
                .growing
                .iter()
                .any(|(dir, (_, until))| *until > now.wait && is_under(ns, ino, *dir))
    }

    /// Until when this root's read-delegation quarantine runs, if it
    /// does (`ReadDelegations::quarantine_until`): the grants of a
    /// previous incarnation, of the tenure a fast takeover superseded, and
    /// what a lone node's kernel cached, none of them in any table a
    /// delegate's writes recall.
    fn first_renewal_quarantine(&self, now: Now, replica: &dyn Replica) -> Option<WaitMs> {
        if !self.cfg.recall_before_ack {
            return None;
        }
        let q = replica.read_delegations().quarantine_until();
        (q > now.wait).then_some(q)
    }

    /// A delegate's renewal of `gen` must wait for the root's read
    /// delegations on the subtree to be recalled
    /// ([`GenState::read_recalls`]), or, for a first renewal (the delegate
    /// was granted none yet, so it executed nothing), for this root's
    /// read-delegation quarantine to pass: a root does not know whether a
    /// generation it inherited was granted by its predecessor, whose
    /// grants under the subtree the quarantine bounds. `true` if it was
    /// set aside, to be answered when the wait is over.
    #[allow(clippy::too_many_arguments)]
    fn deleg_renew_waits_for_reads(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        round: u64,
        source: RenewFrom,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        // The lane's renewal of a generation this tenure granted nothing
        // yet carries no `first`: while either wait runs it is left
        // unanswered (the delegate's lane never sends one before its first
        // grant; this is the root's side of that rule), and it does not
        // take the place of the core's renewal that waits.
        let off_core_ungranted =
            source == RenewFrom::OffCore && !self.dl.answered_granting.contains(&gen);
        let quarantine = match source {
            RenewFrom::Core { first: true, .. } => self.first_renewal_quarantine(now, replica),
            RenewFrom::OffCore if off_core_ungranted => self.first_renewal_quarantine(now, replica),
            _ => None,
        };
        let horizon = self.reclaim_horizon_ms();
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return false;
        };
        if g.node != from || g.ended {
            return false;
        }
        if !g.read_recalls.is_empty() {
            let table = replica.read_delegations();
            g.read_recalls.retain(|id| table.is_live(*id, now.wait));
        }
        if g.read_recalls.is_empty() && quarantine.is_none() {
            return false;
        }
        if off_core_ungranted {
            return true;
        }
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            gen = %constellation_meta::delegation::GenLabel::of(gen),
            ?req,
            grants = g.read_recalls.len(),
            quarantine_ms = quarantine.map(|q| q.since(now.wait)),
            "delegation renewal waits for the subtree's read delegations"
        );
        g.renew_waiting = Some((from, req, round, source));
        self.stats.deleg_renew_read_waits += 1;
        if let Some(q) = quarantine {
            // Not reclaimed for the silence the wait causes (a generation
            // inherited before its first grant has the window of any
            // inherited one): the delegate, granted nothing, honours
            // nothing, so a later reclaim is safe.
            // (A recall, a seal or a lost lease may have closed the slot:
            // the wait lengthens the window and reopens nothing, rule 2.)
            self.dw.extend_until(gen, q.plus(horizon));
            self.arm_grant_quarantine(q, out);
        }
        true
    }

    /// Read delegation `id` was recalled or outwaited: a renewal that
    /// waited for it, and for nothing else, is answered.
    pub(crate) fn deleg_read_grant_gone(
        &mut self,
        now: Now,
        id: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut ready = Vec::new();
        for (gen, g) in self.dl.gens.iter_mut() {
            if g.read_recalls.remove(&id) && g.read_recalls.is_empty() {
                if let Some(w) = g.renew_waiting.take() {
                    ready.push((*gen, w));
                }
            }
        }
        for (gen, (from, req, round, source)) in ready {
            self.on_deleg_renew(now, from, req, gen, round, source, replica, out);
        }
    }

    /// The read-delegation quarantine passed: the renewals that waited
    /// for it, and for no recall, are answered.
    pub(crate) fn deleg_read_quarantine_over(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let ready: Vec<_> = self
            .dl
            .gens
            .iter_mut()
            .filter(|(_, g)| g.read_recalls.is_empty())
            .filter_map(|(gen, g)| g.renew_waiting.take().map(|w| (*gen, w)))
            .collect();
        for (gen, (from, req, round, source)) in ready {
            self.on_deleg_renew(now, from, req, gen, round, source, replica, out);
        }
    }

    /// Phase 2b: keep the table in step with the offline designations
    /// (plans 03–05): a live write designation becomes a designated
    /// generation to its designee (unless the designee is this root:
    /// then the root sequences it, which is the same thing); a released
    /// one is recalled (drained by the designee). A directory under
    /// another delegation waits for that generation to end first.
    pub(crate) fn on_control_sync_designations(
        &mut self,
        now: Now,
        entries: &[(Ino, NodeId)],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.delegation || !self.root_usable(now) {
            return;
        }
        // A designation waiting for read recalls that is no longer listed
        // (or names another designee) holds no grant back any more.
        self.dl
            .designating
            .retain(|dir, node| entries.contains(&(*dir, *node)));
        let table = replica.delegation_table();
        for &(dir, node) in entries {
            if node == self.me() {
                continue;
            }
            match table.get(dir) {
                Some(d) if d.designated() && d.node == node => continue,
                Some(d) if !d.non_stealable() => {
                    // A placed or manual delegation on the very directory:
                    // it ends, the designation follows on the next sync.
                    if self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                        self.start_recall(now, d.gen, out);
                    }
                    continue;
                }
                _ => {}
            }
            if let Some(d) = table.covering(replica.namespace(), dir) {
                if !d.non_stealable() && self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                    self.start_recall(now, d.gen, out);
                }
                continue;
            }
            if let Err(why) = self.delegate_dir(now, dir, node, DelegKind::Designated, replica, out)
            {
                tracing::debug!(
                    node = self.me(),
                    dir,
                    designee = node,
                    why,
                    "designation not delegated yet"
                );
            }
        }
        let released: Vec<u64> = table
            .iter()
            .filter(|d| {
                d.designated()
                    && !entries
                        .iter()
                        .any(|(dir, node)| *dir == d.dir && *node == d.node)
            })
            .map(|d| d.gen)
            .collect();
        for gen in released {
            if self.dl.gens.get(&gen).is_some_and(|g| !g.ended) {
                tracing::info!(node = self.me(), gen = %constellation_meta::delegation::GenLabel::of(gen), "designation released: recalling");
                self.start_recall(now, gen, out);
            }
        }
    }

    pub(crate) fn on_control_undelegate(
        &mut self,
        now: Now,
        op: OpId,
        dir: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.root_usable(now) {
            out.push(Action::ControlDone {
                op,
                result: Err("this node does not hold the lease".into()),
            });
            return;
        }
        let table = replica.delegation_table();
        let entries: Vec<_> = table.get(dir).into_iter().collect();
        // A designation is the operator's offline authority (plans
        // 03–05): only `online` releases it. Recalling it here would just
        // see it re-delegated by the next designation sync, after
        // bouncing the designee's in-flight ops.
        if entries.iter().any(|d| d.designated())
            || entries
                .iter()
                .any(|d| self.dl.gens.get(&d.gen).is_some_and(|g| g.designated()))
        {
            out.push(Action::ControlDone {
                op,
                result: Err(format!(
                    "directory {dir} is designated (an offline designation); \
                     release it with `constellation online`, not `undelegate`"
                )),
            });
            return;
        }
        // The directory's generation; the control is answered when it ended.
        let gens: Vec<u64> = entries
            .iter()
            .map(|d| d.gen)
            .filter(|g| self.dl.gens.get(g).is_some_and(|s| !s.ended))
            .collect();
        let Some(&gen) = gens.last() else {
            out.push(Action::ControlDone {
                op,
                result: Err(format!("directory {dir} is not delegated")),
            });
            return;
        };
        for g in gens.iter().filter(|g| **g != gen) {
            self.start_recall_for(now, *g, RecallCause::Undelegate, out);
        }
        self.dl
            .gens
            .get_mut(&gen)
            .expect("present")
            .controls
            .push(op);
        self.start_recall_for(now, gen, RecallCause::Undelegate, out);
    }

    // ------------------------------------------- Leave ends grants first

    /// How long a leave waits for the log to end the grants (the root's
    /// outwait of a holder that does not answer is one TTL, and the root's
    /// own recall of it a resend or two more).
    pub(crate) fn leave_wait_ms(&self) -> i64 {
        self.dl_t().leave_wait as i64
    }

    /// `node` is leaving: this root grants it nothing more, and a
    /// designation waiting to be journaled for it is dropped.
    ///
    /// `note` is the request of the node's own note. The leaving node sends
    /// it again every resend interval for as long as its leave lasts (also
    /// after the core answered it, and to a new root), so the mark lapses
    /// when no note has come for three resend intervals or
    /// twice the delegation TTL, whichever is longer (a node that stays up,
    /// or comes back under the same id, is granted again), not at a fixed
    /// time after the first one. `None` is an
    /// operator's leave, whose mark goes with the leave that fails.
    fn leave_mark(&mut self, now: Now, node: NodeId, note: Option<OpId>) {
        let mark = match (self.dl.leaving.get(&node), note) {
            // (An operator's mark is not weakened by the node's note.)
            (Some(m), Some(_)) if m.until.is_none() => *m,
            (_, Some(req)) => LeaveMark {
                until: Some(now.wait.plus(self.leave_note_lapse_ms())),
                req: req.0,
            },
            (_, None) => LeaveMark {
                until: None,
                req: 0,
            },
        };
        self.dl.leaving.insert(node, mark);
        self.dl.designating.retain(|_, n| *n != node);
        self.dl.growing.retain(|_, (n, _)| *n != node);
    }

    /// How often a leaving node sends its note again.
    fn leave_resend_ms(&self) -> i64 {
        self.dl_t().leave_resend as i64
    }

    /// How long the root's mark of a leaving node lasts without a note.
    fn leave_note_lapse_ms(&self) -> u64 {
        self.dl_t().leave_note_lapse
    }

    /// Whether a leaving note of `req` from `node` is a delayed duplicate
    /// of one a take-back came after.
    fn leave_note_stale(&self, node: NodeId, req: OpId) -> bool {
        self.dl
            .leave_taken_back
            .get(&node)
            .is_some_and(|t| req.0 <= *t)
    }

    /// `node` is leaving (see [`DelegationState::leaving`]).
    pub(crate) fn is_leaving(&self, now: Now, node: NodeId) -> bool {
        self.dl
            .leaving
            .get(&node)
            .is_some_and(|m| m.until.is_none_or(|t| now.wait < t))
    }

    /// The streams the table lists for `node`, with a live generation here
    /// (a root) or not.
    fn leave_streams_of(&self, node: NodeId, replica: &dyn Replica) -> BTreeSet<u64> {
        replica
            .delegation_table()
            .iter()
            .filter(|t| t.node == node)
            .map(|t| t.gen)
            .collect()
    }

    fn leave_wait_arm(&mut self, now: Now, out: &mut Vec<Action>) {
        // (A leaving node keeps telling its root for as long as it leaves.)
        let telling = self.dl.leave_tell.is_some()
            || (self.dl.leave_self
                && self.cfg.delegation
                && self.cfg.p2p
                && !self.root_usable(now));
        if self.dl.leave_timer.is_some() || (self.dl.leave_waits.is_empty() && !telling) {
            return;
        }
        let t = self.set_timer_wait(now.wait.plus(500), Timer::LeaveWait, out);
        self.dl.leave_timer = Some(t);
    }

    pub(crate) fn on_leave_wait_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.leave_timer = None;
        self.deleg_leave_progress(now, replica, out);
        self.leave_wait_arm(now, out);
    }

    /// `Control::ReleaseGrants` (a holder's own leave, before it flushes and
    /// retires its registry record): this node releases every stream it
    /// holds, as if its root had asked for all of them. The release row goes
    /// through the stream, the root journals `DelegateNodeEnded` once its
    /// journal shows it, and the control is answered when the table lists
    /// no stream of this node. A node that holds nothing is answered at once.
    pub(crate) fn on_control_release_grants(
        &mut self,
        now: Now,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.me();
        self.dl.leave_self = true;
        self.dl.leave_told = None;
        self.dl.leave_tell = None;
        self.dl.leave_waits.push(LeaveWait {
            op,
            who: me,
            deadline: now.wait.plus(self.leave_wait_ms() as u64),
            asked: BTreeMap::new(),
        });
        self.deleg_leave_progress(now, replica, out);
        self.leave_wait_arm(now, out);
    }

    // ------------------------------------------- the holder's own release

    /// The sticky subtrees of stream `gen` this node holds and honours.
    fn sticky_held(&self, gen: u64, replica: &dyn Replica) -> BTreeSet<Ino> {
        let me = self.me();
        let Some(d) = self.dl.mine.get(&gen) else {
            return BTreeSet::new();
        };
        let table = replica.delegation_table();
        d.dirs
            .iter()
            .copied()
            .filter(|dir| {
                table
                    .get(*dir)
                    .is_some_and(|t| t.node == me && t.gen == gen && t.kind == SubtreeKind::Sticky)
            })
            .collect()
    }

    /// Whether a release of this node's is draining the subtree the keys lie
    /// in (stream `gen`): the op waits (`ParkedDeleg::held`) for the release
    /// row, which sends it to the root, or for the hold to lift. A hold lasts
    /// [`STICKY_HOLD_MAX_MS`] at most.
    pub(crate) fn sticky_holds(
        &self,
        now: Now,
        gen: u64,
        keys: &TouchSet,
        replica: &dyn Replica,
    ) -> bool {
        if self.dl.sticky_cmds.is_empty() || self.cfg.holder_releases_before_drain {
            return false;
        }
        let mut live = self
            .dl
            .sticky_cmds
            .iter()
            .filter(|c| now.wait.since(c.started) < STICKY_HOLD_MAX_MS)
            .filter_map(|c| c.parts.get(&gen))
            .peekable();
        if live.peek().is_none() {
            return false;
        }
        match replica.resolve_ownership(keys) {
            Ownership::Delegated(t) if t.node == self.me() && t.gen == gen => {
                live.any(|dirs| dirs.contains(&t.dir))
            }
            _ => false,
        }
    }

    /// `Control::StickyRelease` (plan 46 M1.3 step 5): the holder releases
    /// sticky subtrees by its own decision (idle, unmount, suspend, by
    /// hand). The command is the request; [`Self::sticky_release_progress`]
    /// does the release once the holder has drained. The root's side is the
    /// one of a recall's release: it appends the row, then ends the stream
    /// (`DelegateNodeEnded`) when nothing is left of it, or lets it live on
    /// without the released subtrees (the same generation).
    pub(crate) fn on_control_sticky_release(
        &mut self,
        now: Now,
        op: OpId,
        dirs: Vec<Ino>,
        wait: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let asked: BTreeSet<Ino> = dirs.into_iter().collect();
        let mut parts: BTreeMap<u64, BTreeSet<Ino>> = BTreeMap::new();
        let mut found: BTreeSet<Ino> = BTreeSet::new();
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let mut pool = self.sticky_held(gen, replica);
            // (Subtrees a release in progress has named count: the command
            // is repeated, and waits for the same rows.)
            for c in &self.dl.sticky_cmds {
                if let Some(p) = c.parts.get(&gen) {
                    pool.extend(p.iter().copied());
                }
            }
            let pick: BTreeSet<Ino> = if asked.is_empty() {
                pool
            } else {
                pool.intersection(&asked).copied().collect()
            };
            found.extend(pick.iter().copied());
            if !pick.is_empty() {
                parts.insert(gen, pick);
            }
        }
        let unknown: Vec<Ino> = asked.difference(&found).copied().collect();
        if !unknown.is_empty() {
            out.push(Action::ControlDone {
                op,
                result: Err(format!(
                    "not a sticky grant of this node: {}",
                    unknown
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            });
            return;
        }
        if parts.is_empty() {
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text("this node holds no sticky grant".into())),
            });
            return;
        }
        let n: usize = parts.values().map(|p| p.len()).sum();
        if !wait {
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(format!("releasing {n} sticky subtree(s)"))),
            });
        }
        self.dl.sticky_cmds.push(StickyReleaseCmd {
            op: wait.then_some(op),
            started: now.wait,
            deadline: now.wait.plus(self.leave_wait_ms() as u64),
            parts,
            handover: false,
            root: 0,
            made_at: self.dl.step,
        });
        self.sticky_release_progress(now, replica, out);
    }

    /// ADR-55: the root, as the holder of its own stream `gen`, releases
    /// `dirs` by its own decision (the long dwell, a cross-subtree op): the
    /// command the holder's release is (`Control::StickyRelease`), drained
    /// and journaled by [`Self::sticky_release_progress`] and appended inline.
    pub(crate) fn own_sticky_release(
        &mut self,
        now: Now,
        gen: u64,
        dirs: BTreeSet<Ino>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let mut pool = self.sticky_held(gen, replica);
        for c in &self.dl.sticky_cmds {
            if let Some(p) = c.parts.get(&gen) {
                pool.extend(p.iter().copied());
            }
        }
        let pick: BTreeSet<Ino> = pool.intersection(&dirs).copied().collect();
        if pick.is_empty() {
            return false;
        }
        if self
            .dl
            .sticky_cmds
            .iter()
            .any(|c| c.parts.get(&gen).is_some_and(|p| pick.is_subset(p)))
        {
            // (A release of the same subtrees is under way: one wait for it.)
            return true;
        }
        self.stats.place_root_own_released += 1;
        self.dl.sticky_cmds.push(StickyReleaseCmd {
            op: None,
            started: now.wait,
            deadline: now.wait.plus(self.leave_wait_ms() as u64),
            parts: BTreeMap::from([(gen, pick)]),
            handover: false,
            root: 0,
            made_at: self.dl.step,
        });
        self.sticky_release_progress(now, replica, out);
        true
    }

    /// Every event and tick: release what has drained, answer the commands
    /// that are done, give up on the ones out of time.
    pub(crate) fn sticky_release_progress(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.sticky_cmds.is_empty() {
            return;
        }
        let mut cmds = std::mem::take(&mut self.dl.sticky_cmds);
        let mut keep = Vec::new();
        for mut c in cmds.drain(..) {
            let gens: Vec<u64> = c.parts.keys().copied().collect();
            for gen in gens {
                let mut dirs = c.parts.remove(&gen).expect("present");
                if !self.sticky_part_progress(now, gen, c.made_at, &mut dirs, replica, out) {
                    c.parts.insert(gen, dirs);
                }
            }
            if c.parts.is_empty() {
                if let Some(op) = c.op {
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Text("the sticky subtrees are released".into())),
                    });
                }
                continue;
            }
            if now.wait >= c.deadline {
                self.stats.deleg_sticky_releases_given_up += 1;
                if c.handover {
                    // The root asked and the holder agreed: the wait goes
                    // back (the subtree is the holder's until it releases),
                    // else the request is lost until the root asks again.
                    self.stats.deleg_handover_given_up +=
                        c.parts.values().map(|p| p.len() as u64).sum::<u64>();
                    for (gen, dirs) in std::mem::take(&mut c.parts) {
                        self.handover_wait(now, c.root, gen, dirs);
                    }
                }
                tracing::warn!(
                    node = self.me(),
                    "a release of sticky subtrees did not complete in time; the grants stay"
                );
                if let Some(op) = c.op {
                    out.push(Action::ControlDone {
                        op,
                        result: Err("the sticky subtrees are not released yet (the holder \
                                     has not drained, or the root has not appended the \
                                     release); run the release again"
                            .into()),
                    });
                }
                continue;
            }
            keep.push(c);
        }
        keep.append(&mut self.dl.sticky_cmds);
        self.dl.sticky_cmds = keep;
        self.sticky_release_arm(now, out);
    }

    /// One part of a command: stream `gen`'s subtrees `dirs`. `true`: done.
    fn sticky_part_progress(
        &mut self,
        now: Now,
        gen: u64,
        made_at: u64,
        dirs: &mut BTreeSet<Ino>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let me = self.me();
        // The log ended the stream: the root has the release (or ended it).
        let Some(d) = self.dl.mine.get(&gen) else {
            return true;
        };
        let still: Vec<Ino> = dirs
            .iter()
            .copied()
            .filter(|x| d.dirs.contains(x))
            .collect();
        // Released (by this command's row, or a recall's): done once the
        // root has appended the row (the note is not pending any more).
        if still.is_empty() {
            // ADR-55: the root's own stream is released when its row is appended
            // (inline); the state stays until the row has shipped (it
            // ships in the segment that follows, a flush at an unmount), so the
            // command does not wait for it. A delegate's is done once the
            // root has the row (the release note is not pending any more).
            return d.release.is_none() || self.dl.own_minted.contains(&gen);
        }
        // The ops of the subtrees that are in flight here: parked for their
        // dependencies or a read recall, not the ones this release holds.
        let busy = d.parked.iter().any(|p| {
            !p.held
                && matches!(
                    replica.resolve_ownership(&super::holder::keys_of_op_in(&p.op, replica)),
                    Ownership::Delegated(t)
                        if t.node == me && t.gen == gen && still.contains(&t.dir)
                )
        });
        let (inflight, stopped, refused, streamed) = (
            d.inflight.is_some(),
            d.stopped,
            d.refused,
            d.streamed_through,
        );
        let root = d.last_root.or_else(|| self.holder_root());
        let idx = replica.delegate_idx(gen);
        // A stream that cannot release by a row (a lost dependency, rows its
        // tenure has not committed) would end whole at a cut: never for a
        // release of some subtrees. It waits.
        let cap = self.holder_append_cap(gen, replica);
        let lost = self.deleg_find_lost(gen, replica);
        let Some(root) = root else {
            self.deleg_learn_root(now, out);
            return false;
        };
        if stopped || idx > cap || lost.is_some() {
            tracing::debug!(
                node = me,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                stopped,
                idx,
                cap,
                lost = ?lost,
                "a sticky release waits: the stream cannot release by a row now"
            );
            return false;
        }
        // (The engine's fast path reports what it still has in flight; its
        // subtrees left its view when the command was made.)
        let fast = !self.fast_path_clear(made_at, gen, &still);
        let drained = !busy && !inflight && !refused && !fast && streamed >= idx;
        if !drained {
            tracing::debug!(
                node = me,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                busy,
                inflight,
                refused,
                streamed,
                idx,
                "a sticky release waits for the drain"
            );
            if !self.cfg.holder_releases_before_drain {
                return false;
            }
            self.stats.deleg_sticky_release_undrained += 1;
        }
        self.release_subtrees(now, root, gen, still, replica, out);
        if let Some(d) = self.dl.mine.get(&gen) {
            if dirs.iter().all(|x| !d.dirs.contains(x)) {
                self.stats.deleg_sticky_releases += 1;
                if !d.stopped {
                    // (The stream lives on: MUST-1.)
                    self.stats.deleg_sticky_releases_partial += 1;
                }
            }
        }
        false
    }

    // ------------------------------------------------------- the handover

    /// Plan 46 M1.3 step 8 (ADR-56): the root asked this node, the holder of
    /// stream `gen`, to hand `dirs` over to a node that dominates them
    /// (`dirs` empty: every subtree of the stream). A sticky subtree waits
    /// for the holder to be quiet there ([`Self::handover_progress`]); a
    /// designated one is ignored (only `online` moves a designation). A
    /// placed one is not the holder's to keep: it is released as a recall
    /// would, but by the holder's own command, which takes the subtree out of
    /// the fast path's view and drains it first (the engine does not stop the
    /// fast path for a handover request, so the drain is the core's).
    /// `Some(dirs)`: the stream is not installed here; the caller answers as
    /// a recall does (rowless).
    fn on_handover_request(
        &mut self,
        now: Now,
        from: NodeId,
        gen: u64,
        dirs: Vec<Ino>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<Vec<Ino>> {
        let me = self.me();
        let Some(d) = self.dl.mine.get(&gen) else {
            // (Not installed here: a recall's rowless answer is the safe
            // one. The engine's fast path has no entry for it.)
            return Some(dirs);
        };
        let held: BTreeSet<Ino> = d.dirs.iter().copied().collect();
        let asked: BTreeSet<Ino> = if dirs.is_empty() {
            held.clone()
        } else {
            dirs.into_iter().collect()
        };
        let table = replica.delegation_table();
        let (mut wait, mut placed) = (BTreeSet::new(), BTreeSet::new());
        for dir in asked {
            let kind = table
                .get(dir)
                .filter(|t| t.node == me && t.gen == gen)
                .map(|t| t.kind);
            match kind {
                Some(SubtreeKind::Sticky) if held.contains(&dir) => {
                    wait.insert(dir);
                }
                Some(SubtreeKind::Designated) => {
                    self.stats.deleg_handover_ignored += 1;
                    tracing::debug!(
                        node = me,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        dir,
                        "a handover request for a designated subtree is ignored"
                    );
                }
                // Not a subtree this node holds now (released, or not learned
                // yet): nothing to hand over.
                None => {}
                _ if !held.contains(&dir) => {}
                _ => {
                    placed.insert(dir);
                }
            }
        }
        // What a release in progress has named is not named again (the
        // request is repeated until the row is seen).
        let draining: BTreeSet<Ino> = self
            .dl
            .sticky_cmds
            .iter()
            .filter_map(|c| c.parts.get(&gen))
            .flatten()
            .copied()
            .collect();
        wait.retain(|x| !draining.contains(x));
        placed.retain(|x| !draining.contains(x));
        // At the request: a subtree with no op of the holder's own in flight
        // goes at once; one with an op in flight waits for the grace of
        // idleness (a repeated request does not turn a wait into "at once").
        let waiting: BTreeSet<Ino> = self
            .dl
            .handovers
            .iter()
            .filter(|w| w.gen == gen)
            .flat_map(|w| w.dirs.iter().copied())
            .collect();
        let (wait, mut at_once): (BTreeSet<Ino>, BTreeSet<Ino>) = wait
            .into_iter()
            .partition(|x| waiting.contains(x) || self.own_in_flight(gen, *x, replica));
        // The wait starts with the busy op: the grace runs from it.
        for dir in wait.difference(&waiting) {
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                d.last_own_op.insert(*dir, now.wait);
            }
        }
        at_once.extend(placed);
        if !at_once.is_empty() {
            tracing::info!(
                node = me,
                gen = %constellation_meta::delegation::GenLabel::of(gen),
                dirs = ?at_once,
                root = from,
                "no op of the holder's own in flight in the subtrees the root asked for: releasing them"
            );
            self.handover_release(now, from, BTreeMap::from([(gen, at_once)]));
        }
        let fresh: BTreeSet<Ino> = wait.difference(&waiting).copied().collect();
        if !fresh.is_empty() {
            self.stats.deleg_handover_waits += 1;
        }
        // A wait is refreshed by a repeated request, fresh dirs or not.
        for w in self.dl.handovers.iter_mut().filter(|w| w.gen == gen) {
            w.last_asked = now.wait;
            w.root = from;
        }
        if !fresh.is_empty() {
            self.handover_wait(now, from, gen, fresh);
        }
        self.sticky_release_arm(now, out);
        None
    }

    /// Add `dirs` of stream `gen` to the holder's waits (merged per stream).
    fn handover_wait(&mut self, now: Now, root: NodeId, gen: u64, dirs: BTreeSet<Ino>) {
        match self.dl.handovers.iter_mut().find(|w| w.gen == gen) {
            Some(w) => {
                w.dirs.extend(dirs);
                w.root = root;
                w.last_asked = now.wait;
            }
            None => self.dl.handovers.push(HandoverWait {
                gen,
                dirs,
                root,
                asked: now.wait,
                last_asked: now.wait,
            }),
        }
    }

    /// How long a handover request stays alive at the holder after the root
    /// last asked: as long as the root keeps it (its patience is the grace
    /// and two forward timeouts, ADR-56 3.1). Past it the root has dropped
    /// the request, and a release now would leave the subtree to the root's
    /// keys (no `pl.handover` to grant from) for nothing.
    pub(crate) fn handover_patience_ms(&self) -> i64 {
        self.cfg.handover_grace_ms as i64 + 2 * self.cfg.forward_timeout_ms as i64
    }

    /// An op of this node's own ran in stream `gen` (the core's path): note
    /// the subtree for the holder's grace.
    fn handover_note_own_op(&mut self, now: Now, gen: u64, op: &MutateOp, replica: &dyn Replica) {
        let me = self.me();
        let keys = super::holder::keys_of_op_in(op, replica);
        if let Ownership::Delegated(t) = replica.resolve_ownership(&keys) {
            if t.node == me && t.gen == gen {
                if let Some(d) = self.dl.mine.get_mut(&gen) {
                    d.last_own_op.insert(t.dir, now.wait);
                }
            }
        }
    }

    /// The engine's fast path ran ops of this node's own as the delegate
    /// since it last reported (`notes`: stream, subtree) and has
    /// `inflight` (stream, subtree, count) admitted and not yet journaled
    /// (ADR-56, 3.4). Called by the driver after every step, with the mirror
    /// of this core's view for that step done: the report is a sample taken
    /// after the step ([`StickyReleaseCmd::made_at`]).
    pub fn deleg_note_local(
        &mut self,
        now: Now,
        notes: &[(u64, Ino)],
        inflight: &[(u64, Ino, u64)],
    ) {
        self.dl.sampled_at = self.dl.step;
        self.dl.fast_inflight = inflight
            .iter()
            .filter(|(_, _, n)| *n > 0)
            .map(|(g, d, n)| ((*g, *d), *n))
            .collect();
        for (gen, dir) in notes {
            if let Some(d) = self.dl.mine.get_mut(gen) {
                d.last_own_op.insert(*dir, now.wait);
            }
        }
    }

    /// Whether the engine's fast path holds no admitted op under `dirs` of
    /// stream `gen` that the core must wait for before it releases them, for
    /// a command made at step `made_at`: the sample the driver took after
    /// that step's mirror shows none. Without an engine fast path
    /// (`Config::fast_path_sampled` off: the simulation, the unit harness)
    /// there is nothing to wait for.
    pub(crate) fn fast_path_clear(&self, made_at: u64, gen: u64, dirs: &[Ino]) -> bool {
        if !self.cfg.fast_path_sampled {
            return true;
        }
        self.dl.sampled_at >= made_at
            && dirs
                .iter()
                .all(|d| !self.dl.fast_inflight.contains_key(&(gen, *d)))
    }

    /// Whether an op of this node's own is in flight in subtree `dir` of
    /// stream `gen`: parked for its dependencies or a read recall (the ops
    /// a release holds are not: they are the release's), or admitted by the
    /// engine's fast path under that subtree.
    pub(crate) fn own_in_flight(&self, gen: u64, dir: Ino, replica: &dyn Replica) -> bool {
        if self.dl.fast_inflight.contains_key(&(gen, dir)) {
            return true;
        }
        let me = self.me();
        let Some(d) = self.dl.mine.get(&gen) else {
            return false;
        };
        d.parked.iter().any(|p| {
            p.from == 0
                && !p.held
                && !p.settle
                && matches!(
                    replica.resolve_ownership(&super::holder::keys_of_op_in(&p.op, replica)),
                    Ownership::Delegated(t) if t.node == me && t.gen == gen && t.dir == dir
                )
        })
    }

    /// Every event and tick: a handover request whose subtrees the holder is
    /// quiet in (ADR-56, H-I5) becomes a release of the holder's own, which
    /// [`Self::sticky_release_progress`] drains and journals as step 5's.
    pub(crate) fn handover_progress(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.handovers.is_empty() {
            return;
        }
        let grace = self.cfg.handover_grace_ms as i64;
        let patience = self.handover_patience_ms();
        let waits = std::mem::take(&mut self.dl.handovers);
        let mut keep = Vec::new();
        let mut parts: BTreeMap<u64, BTreeSet<Ino>> = BTreeMap::new();
        let mut root = 0;
        for mut w in waits {
            if now.wait.since(w.last_asked) >= patience {
                // The root has dropped it.
                self.stats.deleg_handover_expired += w.dirs.len() as u64;
                continue;
            }
            let held = self.sticky_held(w.gen, replica);
            // Released by other means (a recall, an abandon, the holder's
            // own command), or the stream is gone: nothing to hand over.
            w.dirs.retain(|x| held.contains(x));
            let draining: BTreeSet<Ino> = self
                .dl
                .sticky_cmds
                .iter()
                .filter_map(|c| c.parts.get(&w.gen))
                .flatten()
                .copied()
                .collect();
            w.dirs.retain(|x| !draining.contains(x));
            // (It was busy at the request: it goes once none is in flight and
            // the holder's own ops there have been idle for the grace.)
            let quiet: Vec<Ino> = w
                .dirs
                .iter()
                .copied()
                .filter(|dir| {
                    !self.own_in_flight(w.gen, *dir, replica)
                        && self
                            .dl
                            .mine
                            .get(&w.gen)
                            .and_then(|d| d.last_own_op.get(dir))
                            .is_none_or(|t| now.wait.since(*t) >= grace)
                })
                .collect();
            for dir in &quiet {
                w.dirs.remove(dir);
            }
            if !quiet.is_empty() {
                tracing::info!(
                    node = self.me(),
                    gen = %constellation_meta::delegation::GenLabel::of(w.gen),
                    dirs = ?quiet,
                    waited_ms = now.wait.since(w.asked),
                    root = w.root,
                    "the holder is quiet in the subtrees the root asked for: releasing them"
                );
                root = w.root;
                parts.entry(w.gen).or_default().extend(quiet);
            }
            if !w.dirs.is_empty() {
                keep.push(w);
            }
        }
        keep.append(&mut self.dl.handovers);
        self.dl.handovers = keep;
        if !parts.is_empty() {
            self.handover_release(now, root, parts);
        }
        self.sticky_release_arm(now, out);
    }

    /// The holder's own release of `parts` (stream -> subtrees) for a
    /// handover: a command that leaves the fast path's view now and drains
    /// and journals the row once the engine has reported the fast path clear
    /// (see [`StickyReleaseCmd::made_at`]).
    fn handover_release(&mut self, now: Now, root: NodeId, parts: BTreeMap<u64, BTreeSet<Ino>>) {
        self.stats.deleg_handover_released += parts.values().map(|p| p.len() as u64).sum::<u64>();
        self.dl.sticky_cmds.push(StickyReleaseCmd {
            op: None,
            started: now.wait,
            deadline: now.wait.plus(self.leave_wait_ms() as u64),
            parts,
            handover: true,
            root,
            made_at: self.dl.step,
        });
    }

    fn sticky_release_arm(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.dl.sticky_timer.is_some()
            || (self.dl.sticky_cmds.is_empty() && self.dl.handovers.is_empty())
        {
            return;
        }
        let t = self.set_timer_wait(
            now.wait.plus(STICKY_RELEASE_TICK_MS as u64),
            Timer::StickyRelease,
            out,
        );
        self.dl.sticky_timer = Some(t);
    }

    pub(crate) fn on_sticky_release_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dl.sticky_timer = None;
        self.handover_progress(now, replica, out);
        self.sticky_release_progress(now, replica, out);
    }

    /// `Control::EndGrantsOf { node }` (the admin form of leave, for a node
    /// that cannot release its own grants, or any node retired by an
    /// operator): the root recalls every stream of `node` and ends it, by
    /// the holder's release if it answers and else at the generation's
    /// expiry (the root's TTL end), before the registry entry goes. Only the
    /// root can: another node never answers that `node` holds nothing (its
    /// table may lag the root's), and refuses.
    pub(crate) fn on_control_end_grants_of(
        &mut self,
        now: Now,
        op: OpId,
        node: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if node == self.me() {
            out.push(Action::ControlDone {
                op,
                result: Err("a node releases its own grants with a self-leave".into()),
            });
            return;
        }
        if !self.cfg.delegation || !self.cfg.p2p {
            // (No grant is made, so none is held.)
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(format!("node {node} holds no delegation"))),
            });
            return;
        }
        // A node that is not the root cannot tell whether `node` holds
        // anything (its table may lag the root's): it never answers that
        // the node holds nothing.
        if !self.root_usable(now) {
            // The node is the lease holder itself, as the lease object this
            // node read names it (a dead or cut-off root; inside an epoch,
            // the carrier): a root is no delegate (`on_prev_root_sealed`),
            // so it holds no grant to end, and what it granted is the
            // next tenure's to wait out, which the retirement's fencing of
            // its leases lets begin. Refusing here would make the one node
            // an operator must retire when the root is gone impossible to
            // retire.
            if self.root_node() == Some(node)
                && self
                    .lease
                    .last_seen
                    .as_ref()
                    .is_some_and(|l| l.holder == node)
            {
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Text(format!(
                        "node {node} is the root and holds no delegation of its own"
                    ))),
                });
                return;
            }
            out.push(Action::ControlDone {
                op,
                result: Err(format!(
                    "this node is not the root, which alone can end the delegations of \
                     node {node}; run the leave on the root{}",
                    self.root_node()
                        .map(|r| format!(" (node {r})"))
                        .unwrap_or_default()
                )),
            });
            return;
        }
        // Nothing more is granted to the node from here on.
        self.leave_mark(now, node, None);
        self.dl.leave_waits.push(LeaveWait {
            op,
            who: node,
            deadline: now.wait.plus(self.leave_wait_ms() as u64),
            asked: BTreeMap::new(),
        });
        self.deleg_leave_progress(now, replica, out);
        self.leave_wait_arm(now, out);
    }

    /// This node's leave is known to the root it has now: that root
    /// acknowledged the leave note, or this node holds the lease itself (or
    /// nobody can grant: delegation is off).
    pub(crate) fn leave_told_ok(&self, now: Now) -> bool {
        if !self.cfg.delegation || !self.cfg.p2p || self.root_usable(now) {
            return true;
        }
        // (No root to tell: the lease as this node read it is released. A
        // leaving node takes no grant, so nothing new is granted to it, and
        // whatever it holds is released or lapses, which the leave waits
        // for separately; an idle cluster's leave is not stuck on a root
        // that is nobody.)
        self.lease
            .last_seen
            .as_ref()
            .is_some_and(|l| l.released || l.holder == 0)
            || (self.dl.leave_told.is_some() && self.dl.leave_told == self.root_node())
    }

    /// A leave note to the root: sent when the leave starts and again every
    /// resend interval for as long as the leave lasts (the root's mark of
    /// the node lapses without them, and a new root has to hear of the
    /// leave), always to the root there is now; and the note taking it back
    /// after a failed leave, to the root there is now too, until that is
    /// acknowledged or out of time.
    fn leave_tell_progress(&mut self, now: Now, out: &mut Vec<Action>) {
        let resend = self.leave_resend_ms();
        // This node is the root (or nobody grants): no root to tell.
        if !self.cfg.delegation || !self.cfg.p2p || self.root_usable(now) {
            self.dl.leave_tell = None;
            return;
        }
        let tell = match self.dl.leave_tell {
            Some(t) if !t.leaving && now.wait >= t.until => {
                self.dl.leave_tell = None;
                return;
            }
            // (A leaving note of a leave that is over.)
            Some(t) if t.leaving && !self.dl.leave_self => {
                self.dl.leave_tell = None;
                return;
            }
            Some(t) => Some(t),
            None => None,
        };
        let leaving = match tell {
            Some(t) => t.leaving,
            None if self.dl.leave_self => true,
            None => return,
        };
        let to = match (self.root_node(), tell) {
            (Some(root), _) => root,
            (None, _) if leaving => {
                self.deleg_learn_root(now, out);
                return;
            }
            (None, Some(t)) => t.to,
            (None, None) => return,
        };
        if let Some(t) = tell {
            if t.to == to && t.sent.is_some_and(|at| now.wait.since(at) < resend) {
                return;
            }
        }
        let req = match tell {
            Some(t) if t.to == to => t.req,
            _ => self.op_id(),
        };
        let until = tell.map_or(now.wait.plus(self.leave_wait_ms() as u64), |t| t.until);
        self.dl.leave_tell = Some(LeaveTell {
            leaving,
            to,
            req,
            sent: Some(now.wait),
            until,
        });
        out.push(Action::Send {
            to,
            msg: PeerMsg::DelegateStream {
                req,
                gen: LEAVE_NOTE_GEN,
                round: 0,
                txs: Vec::new(),
                leaving: Vec::new(),
                leaving_barriers: Vec::new(),
                released: Some(constellation_meta::locks::ReleaseNote {
                    leaving,
                    ..Default::default()
                }),
            },
        });
    }

    /// The root's answer to a leave note.
    fn on_leave_note_ack(&mut self, from: NodeId, req: OpId, refused: bool) {
        let Some(t) = self.dl.leave_tell else {
            return;
        };
        if t.req != req || t.to != from || refused {
            return;
        }
        if t.leaving {
            // (The leave goes on: the note is sent again at the next
            // resend, to this root and to a new one.)
            if self.dl.leave_self {
                self.dl.leave_told = Some(from);
            }
        } else {
            self.dl.leave_tell = None;
        }
    }

    /// A send of the leave note failed: due again at the next look.
    pub(crate) fn on_leave_note_failed(&mut self, req: OpId) -> bool {
        match self.dl.leave_tell.as_mut() {
            Some(t) if t.req == req => {
                t.sent = None;
                true
            }
            _ => false,
        }
    }

    /// The root's side of a leave note: the node leaves (this root grants it
    /// nothing more, from before the answer), or it does not any more (a
    /// leave that failed; a note older than the mark it would take back is
    /// ignored, and an operator's mark stays).
    fn on_leave_note(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        note: Option<constellation_meta::locks::ReleaseNote>,
        out: &mut Vec<Action>,
    ) {
        let ok = self.cfg.delegation && self.cfg.p2p && self.root_usable(now) && note.is_some();
        if ok {
            if note.as_ref().is_some_and(|n| n.leaving) {
                if !self.leave_note_stale(from, req) {
                    self.leave_mark(now, from, Some(req));
                }
            } else {
                // (Remembered also when nothing is marked: a delayed
                // leaving note may still come.)
                let t = self.dl.leave_taken_back.entry(from).or_insert(0);
                *t = (*t).max(req.0);
                if self
                    .dl
                    .leaving
                    .get(&from)
                    .is_some_and(|m| m.until.is_some() && m.req < req.0)
                {
                    self.dl.leaving.remove(&from);
                }
            }
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegateStreamAck {
                req,
                gen: LEAVE_NOTE_GEN,
                round: 0,
                through: 0,
                committed: 0,
                refused: !ok,
                archive_bound: false,
            },
        });
    }

    /// This node's own leave failed (out of time): it is not leaving, takes
    /// grants again, and tells the root so.
    pub(crate) fn leave_self_abort(&mut self, now: Now, out: &mut Vec<Action>) {
        let told = self.dl.leave_told.take();
        let pending = self.dl.leave_tell.take();
        self.dl.leave_self = false;
        // The root there is now is told (the one that was told may have
        // lost the lease; its mark went with it); the last one told when
        // none is known.
        let Some(root) = self.root_node().or(told).or(pending.map(|t| t.to)) else {
            return;
        };
        let req = self.op_id();
        self.dl.leave_tell = Some(LeaveTell {
            leaving: false,
            to: root,
            req,
            sent: None,
            until: now.wait.plus(self.leave_wait_ms() as u64),
        });
        self.leave_tell_progress(now, out);
        self.leave_wait_arm(now, out);
    }

    /// Every event and tick: ask what has to be asked, answer the leaves
    /// that are over (the table lists no stream of the node, and none is
    /// live here) or out of time.
    pub(crate) fn deleg_leave_progress(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.leave_waits_progress(now, replica, out);
        self.leave_tell_progress(now, out);
    }

    fn leave_waits_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.dl.leave_waits.is_empty() {
            return;
        }
        let me = self.me();
        let resend = self.dl_t().leave_resend;
        let mut waits = std::mem::take(&mut self.dl.leave_waits);
        let mut keep = Vec::new();
        for mut w in waits.drain(..) {
            let streams = self.leave_streams_of(w.who, replica);
            let done = if w.who == me {
                // (This node's own journal applies its release rows to its
                // table at once, so the table is no witness: the root has
                // appended the release row, which the acknowledgement of
                // the batch that carried it says, and the stream is
                // stopped. A stream this node never installed is listed
                // until the root's end reaches the table.)
                // (Only the root's acknowledgement of the release of the whole
                // stream counts: a stream stopped by a refused renewal, a
                // backup's seal, lost dependencies, a held note or an epoch
                // is not ended in the log, and is the table's to drop.)
                // (A stream the log ended is dropped from `mine`.)
                // (And the root has heard that this node leaves, whether it
                // holds a stream or not: a node that holds none sends no
                // release note, and the root would grant it again.)
                self.leave_told_ok(now)
                    && streams.iter().all(|g| self.dl.mine.contains_key(g))
                    && self.dl.mine.values().all(|d| d.release_acked)
            } else {
                streams.is_empty() && !self.dl.gens.values().any(|g| g.node == w.who && !g.ended)
            };
            if done {
                out.push(Action::ControlDone {
                    op: w.op,
                    result: Ok(ControlOk::Text(format!(
                        "the delegations of node {} are ended",
                        w.who
                    ))),
                });
                continue;
            }
            if now.wait >= w.deadline {
                if w.who != me {
                    // (The retirement does not follow: the node is not
                    // leaving, and may be granted again.)
                    self.dl.leaving.remove(&w.who);
                } else {
                    self.leave_self_abort(now, out);
                }
                out.push(Action::ControlDone {
                    op: w.op,
                    result: Err(format!(
                        "the delegations of node {} are not ended yet (the log has not carried \
                         their end); run the leave again",
                        w.who
                    )),
                });
                continue;
            }
            if w.who == me {
                // Release what the table or this node's state lists, once
                // and again every resend interval (the batch with the note
                // goes out again; a lost request is the root's to resend, a
                // lost release is ours).
                let mut gens: BTreeSet<u64> = streams;
                gens.extend(self.dl.mine.keys().copied());
                for gen in gens {
                    if self.dl.mine.get(&gen).is_some_and(|d| d.release_acked) {
                        continue;
                    }
                    let due = w
                        .asked
                        .get(&gen)
                        .is_none_or(|at| now.wait.since(*at) >= resend as i64);
                    if !due {
                        continue;
                    }
                    let root = self
                        .dl
                        .mine
                        .get(&gen)
                        .and_then(|d| d.last_root)
                        .or_else(|| self.root_node());
                    let Some(root) = root else {
                        self.deleg_learn_root(now, out);
                        continue;
                    };
                    w.asked.insert(gen, now.wait);
                    let req = self.op_id();
                    self.on_release_request(
                        now,
                        root,
                        req,
                        gen,
                        Vec::new(),
                        ReleaseWhy::Recall,
                        replica,
                        out,
                    );
                }
            } else {
                let gens: Vec<u64> = self
                    .dl
                    .gens
                    .iter()
                    .filter(|(_, g)| g.node == w.who && !g.ended)
                    .map(|(gen, _)| *gen)
                    .collect();
                for gen in gens {
                    self.start_recall(now, gen, out);
                }
            }
            keep.push(w);
        }
        keep.append(&mut self.dl.leave_waits);
        self.dl.leave_waits = keep;
    }

    /// Plan 30 §M10: an epoch became active (or closed).
    pub(crate) fn deleg_on_epoch(
        &mut self,
        now: Now,
        active: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.dl.epoch_active == active {
            return;
        }
        self.dl.epoch_active = active;
        if !active {
            return;
        }
        // No renewal is granted inside an epoch: every slot closes at the
        // transition.
        self.dw.close_all();
        // Delegates stop; the root recalls everything.
        let mine: Vec<u64> = self.dl.mine.keys().copied().collect();
        let root = self.root_node().unwrap_or(0);
        for gen in mine {
            if let Some(d) = self.dl.mine.get_mut(&gen) {
                d.stopped = true;
                let parked = std::mem::take(&mut d.parked);
                for p in parked {
                    self.answer_not_owner(now, p, root, replica, out);
                }
            }
        }
        if self.root_usable(now) {
            self.recall_all(now, out);
        }
    }

    /// The lease was lost or released: no generation is this node's to
    /// end any more (a successor learns them from the log).
    pub(crate) fn deleg_on_lease_gone(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Phase 2b: what waited on these generations' recalls re-routes
        // (a local op to the new holder; a forwarded one is answered
        // `Busy` and re-sent).
        let ids: BTreeSet<u64> = self.dl.gens.keys().map(|g| wait_id(*g)).collect();
        // A release waiting for recalls (`release_recall_waits`) is not
        // an execution to abort: it stops waiting for these generations,
        // and its bound's timer, due now, lets it see the lease gone
        // (`release_after_recalls`) in a step of its own.
        if let Some(w) = self.dl.release_wait.as_mut() {
            w.bound_passed = true;
            let timer = w.timer;
            self.cancel_timer(timer, out);
            let timer = self.set_timer_wait(now.wait, Timer::ReleaseRecallBound, out);
            if let Some(w) = self.dl.release_wait.as_mut() {
                w.timer = timer;
            }
        }
        if !ids.is_empty() {
            tracing::info!(
                node = self.me(),
                generations = ids.len(),
                "the root lease is gone: its delegation state is dropped"
            );
            self.abort_parks_waiting_on(now, &ids, replica, out);
        }
        // No window is this node's any more: closed and gone in this step.
        self.dw.close_all();
        self.dw.clear();
        self.dl.answered_granting.clear();
        self.dl.floor_sent.clear();
        self.dl.designating.clear();
        self.dl.leaving.clear();
        self.dl.leave_taken_back.clear();
        // An operator's leave waits on this root's own state: a new root
        // does not know the node leaves, and the table here lags, so the
        // wait must not end as done. It is for the operator to run again on
        // the root.
        let me = self.cfg.node_id;
        let mut waits = std::mem::take(&mut self.dl.leave_waits);
        waits.retain(|w| {
            if w.who == me {
                return true;
            }
            out.push(Action::ControlDone {
                op: w.op,
                result: Err(format!(
                    "the lease was lost while the delegations of node {} were being ended; \
                     run the leave again on the root",
                    w.who
                )),
            });
            false
        });
        self.dl.leave_waits = waits;
        self.dl.growing.clear();
        self.dl.cert_set = false;
        self.dl.forget_at.clear();
        self.dl.forgot = false;
        self.dl.root_caps.clear();
        self.dl.holder_caps.clear();
        for (_, g) in std::mem::take(&mut self.dl.gens) {
            if let Some(t) = g.expiry {
                self.cancel_timer(t, out);
            }
            if let Some(t) = g.recall_resend {
                self.cancel_timer(t, out);
            }
            for req in g.recall_tries {
                self.dl.by_req.remove(&req);
            }
            for c in g.controls {
                out.push(Action::ControlDone {
                    op: c,
                    result: Err("the lease was lost".into()),
                });
            }
        }
        // Plan 30 §M14: the tenure's lock grants go with it.
        self.lock_on_lease_gone(now, replica, out);
    }
}

/// What the root must do before executing an op (phase 2b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecallPlan {
    None,
    /// Wait for these recalls (`wait_id`s).
    Wait(BTreeSet<u64>),
    /// Refuse with this code (a designation is involved).
    Refuse(Code),
    /// Plan 46 M0.3 (S5, ADR-42): the takeover gate is pending and a
    /// generation the op falls under is one it took on: the gate recalls
    /// nothing, so the op waits for the gate to open (a `Busy` retry, an
    /// inbox batch left for the poll after it).
    Gated,
    /// ADR-55: the keys are under the root's own live sticky stream and the
    /// core's executor did not take the op (the grant is not honoured yet, or
    /// the stream is stopped for a moment): the root never refuses its own
    /// stream, nor executes under it; the asker retries (`Held`).
    Hold(u64),
}

/// The kind a subtree was granted as, from the table's.
fn kind_of(k: SubtreeKind) -> DelegKind {
    match k {
        SubtreeKind::Designated => DelegKind::Designated,
        SubtreeKind::Sticky => DelegKind::Sticky,
        SubtreeKind::Placed => DelegKind::Manual,
    }
}

/// Whether `dir` is `ancestor` or under it.
pub(crate) fn is_under(
    ns: &dyn constellation_meta::delegation::Namespace,
    mut dir: Ino,
    ancestor: Ino,
) -> bool {
    for _ in 0..4096 {
        if dir == ancestor {
            return true;
        }
        match ns.parent_of(dir) {
            Some(p) => dir = p,
            None => return false,
        }
    }
    false
}
