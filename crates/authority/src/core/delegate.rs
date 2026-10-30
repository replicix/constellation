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
use super::{AckClass, Asker, Core, Timer};
use crate::action::{Action, AnswerSource, ControlOk};
use crate::event::PeerMsg;
use crate::ids::{Epoch, HonourMs, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::delegation::{Ownership, Range};
use constellation_meta::locks::LockTag;
use constellation_meta::{
    LogRecord, MetaError, MutateOp, MutateOutcome, OwnChunks, Position, RecallNeed, Rid, TouchSet,
};
use constellation_types::Code;
use std::collections::{BTreeMap, BTreeSet};

/// Parked-wait ids for generations (above every read-delegation grant
/// id; `Parked::waiting` holds both kinds).
pub(crate) const DELEG_WAIT_BASE: u64 = 1 << 62;

pub(crate) fn wait_id(gen: u64) -> u64 {
    DELEG_WAIT_BASE + gen
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
}

/// The root's view of one generation.
#[derive(Debug, Clone)]
pub(crate) struct GenState {
    pub dir: Ino,
    pub node: NodeId,
    /// Plan 30 §M12: the part of `dir` (a hash range, or the whole).
    pub range: Range,
    /// Why it exists (phase 2b): an operator, the placement, a
    /// designation.
    pub kind: DelegKind,
    /// Plan 43 C2: the generation's configuration — the latest
    /// `DelegGroup` record this root journaled (seeded from its table
    /// for an inherited generation). The root drains its backup (seal)
    /// when the delegate dies.
    pub group: GroupState,
    /// Phase 2b: ended by a cross-subtree op: delegate the directory to
    /// the same node again once the op executed.
    pub redelegate: bool,
    /// When the root granted it (placement's dwell counts from here; the
    /// cut's designation-online check), on the wait clock.
    pub granted: WaitMs,
    /// Placement: since when the delegate's share has been below the
    /// leave threshold (`None`: it is not).
    pub below_since: Option<WaitMs>,
    /// Plan 43 C2e (§3.7): the seals asked of the group's members, by request
    /// id: `(member, strict)`. A *strict* member is one of the latest
    /// `DelegGroup` version (the end waits for one of them); a best-effort
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
    /// Plan 43 C2 (Q1): the generation cannot end: its latest `DelegGroup`
    /// record names a backup, which may hold acknowledged rows the root
    /// does not, and none has answered its seal with a tail drained to its
    /// end. Asked again every window until one does (counted in
    /// `Stats::deleg_seal_waits`), `status.delegation.gens[].waiting_for_group`;
    /// `constellation repair abandon-group --gen` ends it at the cursor.
    pub waiting_for_group: bool,
    /// The last WARN about the wait (once a minute).
    pub waiting_warned: Option<WaitMs>,
    /// Since when the generation has been `waiting_for_group` (status).
    pub waiting_since: Option<WaitMs>,
    /// What this root knows of each member it asked to seal (status: the
    /// operator weighs `abandon-group` by it).
    pub member_notes: BTreeMap<NodeId, MemberNote>,
    /// When this root last heard from the delegate about this generation (a
    /// renewal, a stream batch, a recall's answer), on its wait clock.
    pub delegate_heard: Option<WaitMs>,
    /// Appended through this stream index.
    pub cursor: u64,
    // The window (`until`: the root outwaits the delegate once its **wait**
    // clock reaches it; plan 43 C7-H1) lives in `Core::dw`, the root's
    // shared window table (plan 41 R1, `DelegWindows`).
    pub recall: RecallPhase,
    /// The phase a seal of the backup replaced: what an unanswered seal
    /// goes back to.
    pub recall_before_seal: RecallPhase,
    pub ended: bool,
    pub expiry: Option<TimerId>,
    pub recall_req: Option<OpId>,
    /// `Undelegate` controls answered when the generation ends.
    pub controls: Vec<OpId>,
    /// The read delegations (M8) this root granted on the subtree before
    /// it delegated it, being recalled (`Core::delegate_dir`): the
    /// delegate's writes recall only its own table's, so no renewal is
    /// granted while one of these is live.
    pub read_recalls: BTreeSet<u64>,
    /// The delegate's renewal that waits for `read_recalls`, answered
    /// when the last of them is gone.
    pub renew_waiting: Option<(NodeId, OpId, u64, RenewFrom)>,
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
/// latest `DelegGroup` record journaled for it; version 0 and no backups:
/// the group is the delegate and the root).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GroupState {
    pub version: u64,
    pub backups: Vec<NodeId>,
    /// Every backup any record of the generation named (C2e's seal set).
    pub named: BTreeSet<NodeId>,
}

impl GroupState {
    pub(crate) fn of(row: Option<constellation_meta::delegation::DelegGroupRow>) -> Self {
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
}

/// This node as the delegate of one generation.
#[derive(Debug, Clone)]
pub(crate) struct DelegateState {
    pub dir: Ino,
    pub gen: u64,
    /// Honoured while this node's honour clock is below this (`HonourMs(0)`:
    /// not renewed yet; plan 43 C7-H1).
    pub until: HonourMs,
    pub stopped: bool,
    /// The stream's next attempt is not before this (a failed send backs
    /// off; the root unreachable must not mean a send per event).
    pub stream_after: Option<WaitMs>,
    /// The current backoff, doubled per failure up to a bound.
    pub stream_backoff_ms: u64,
    /// The root refused the stream (the generation is ending or unknown
    /// to it): nothing more is streamed until the log says.
    pub refused: bool,
    /// The root acknowledged the stream through this index.
    pub streamed_through: u64,
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
    /// Phase 2b: a designation — honoured whatever the clock says.
    pub designated: bool,
    /// Phase 2b: the root the stream last went to; a different one
    /// (a failover) gets every unretired transaction again.
    pub last_root: Option<NodeId>,
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
    /// The highest index answered under a group (`removal_safe`'s bound).
    pub acked_max: u64,
    /// Plan 43 C2e follow-up: this process recorded, synced, that it holds a
    /// grant of the generation on this boot (`Replica::deleg_note_held`).
    pub held_noted: bool,
    /// A designee: the stream index through which this process synced its
    /// journal; its stream sends nothing past it (`deleg_stream`).
    pub synced_through: u64,
    /// The first index of this stream whose `deps` name a row its
    /// generation ended without (`Replica::deps_lost`): never streamed, the
    /// generation stopped before it (`deleg_stream`).
    pub lost_from: Option<u64>,
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
    Renew,
    Recall,
    /// Phase 2b: a delegate's append to its backup.
    BackupAppend,
    /// Phase 2b: the root's seal request to a dead delegate's backup.
    Seal,
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
        ]
    }
}

/// How long a recall answered for a generation this node's table has
/// not named yet is remembered ([`DelegationState::handed_back`]).
const HANDED_BACK_UNSEEN_MS: i64 = 60_000;

/// How many freezes [`DelegationState::freezes`] keeps.
const FREEZES_KEPT: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct DelegationState {
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
    answered_granting: BTreeSet<u64>,
    floor_sent: BTreeMap<u64, Position>,
    /// Designations not journaled yet because this root's read
    /// delegations under the directory were live (`Core::delegate_dir`):
    /// directory → designee. No read delegation is granted under one
    /// meanwhile, so the next sync can journal it.
    pub designating: BTreeMap<Ino, NodeId>,
    /// Whether a certificate is published (`Core::deleg_publish_root`).
    cert_set: bool,
    /// The generations this node is the delegate of.
    pub mine: BTreeMap<u64, DelegateState>,
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
    stream_timer: Option<TimerId>,
    /// When a delegate that knows no root may read the lease for it again
    /// ([`Core::deleg_learn_root`]): once a stream tick.
    root_read_after: Option<WaitMs>,
    /// The last table sync found this node a usable root (and so learned
    /// the generations the table names): see [`Core::deleg_root_sync_due`].
    root_synced: bool,
    /// Plan 30 §M10: an active continuation epoch — no delegation.
    pub(crate) epoch_active: bool,
    /// Generations this node's table still delegates to it whose recall
    /// it answered, with the root that recalled them: the subtree's lock
    /// grants went back there (`on_deleg_recall`), so lock requests and
    /// renewals for it are sent there (`NotOwner { root }`) rather than
    /// refused with `NotOwner { 0 }` until the `Recall` record reaches
    /// this table — under an S3 cut, only after it. Also for a
    /// generation never installed here (an epoch was active when the
    /// table named it). `(root, recalled at, row seen)`: dropped once the
    /// generation's table row was seen here and is gone, or — a row
    /// never seen (the recall overtook the record naming this node) —
    /// after [`HANDED_BACK_UNSEEN_MS`].
    pub handed_back: BTreeMap<u64, (NodeId, WaitMs, bool)>,
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
    pub dir: Ino,
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
    /// `(dir, gen, until, stopped, streamed_through, executed, parked,
    /// class)`: `until` a reading of this node's honour clock (plan 43
    /// C7-H1; the engine's `DelegView` judges it on that clock); `class`
    /// the name of the class this node's own clients' answers wait for
    /// (plan 43 C2e: `AckClass::name`).
    #[allow(clippy::type_complexity)]
    pub mine: Vec<(Ino, u64, i64, bool, u64, u64, usize, String)>,
    /// `(dir, node, gen, cursor, until_ms (wall, for status), recall, ended,
    /// waiting_for_group)`: the last says the generation cannot end because
    /// no member of its group has handed over the tail (plan 43 C2e).
    #[allow(clippy::type_complexity)]
    pub gens: Vec<(Ino, NodeId, u64, u64, i64, String, bool, bool)>,
    /// Plan 43 C2e: the generations that cannot end for want of their
    /// group's tail, with what the root knows of each member (what
    /// `abandon-group --gen` is weighed by).
    pub waiting: Vec<WaitingGenView>,
    pub pending_exec: usize,
    /// Phase 2b: per generation held here, `(gen, backup, backup_acked)`.
    pub backups: Vec<(u64, NodeId, u64)>,
    /// Phase 2b: per generation on the root, `(gen, kind, backup)`.
    pub kinds: Vec<(u64, String, NodeId)>,
}

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
                        g.dir,
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
                .map(|(g, s)| (*g, format!("{:?}", s.kind), s.group.backup().unwrap_or(0)))
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
                        .map(|n| (*n, true))
                        .chain(g.group.best_effort().into_iter().map(|n| (n, false)))
                        .map(|(node, strict)| {
                            let note = g.member_notes.get(&node).cloned().unwrap_or_default();
                            WaitingMemberView {
                                node,
                                strict,
                                asked_ms: ago(note.asked),
                                failed_ms: ago(note.failed),
                                answered_ms: ago(note.answered),
                                heard_ms: ago(self.deleg_member_heard(g, node)),
                            }
                        })
                        .collect();
                    WaitingGenView {
                        gen: *gen,
                        dir: g.dir,
                        delegate: g.node,
                        waited_ms: ago(g.waiting_since).unwrap_or(0),
                        delegate_heard_ms: ago(self.deleg_delegate_heard(g)),
                        tail: g.sealed_tail.as_ref().map(|t| (t.from, g.cursor, t.end)),
                        members,
                    }
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
                (
                    d.dir,
                    d.gen,
                    d.until.0,
                    d.stopped,
                    d.streamed_through,
                    d.executed,
                    d.parked.len(),
                    // The class published at the last event
                    // ([`Self::report_deleg_durable`]), not decided again
                    // for every step's view.
                    match self.dl.reported.get(&d.gen).map(|r| r.class) {
                        Some(constellation_meta::RowClass::Lone) => "lone",
                        Some(constellation_meta::RowClass::Archive) => "archive",
                        Some(constellation_meta::RowClass::RootGroup) => "root-group",
                        Some(constellation_meta::RowClass::DelegGroup) => "deleg-group",
                        Some(constellation_meta::RowClass::NonStealable) => "non-stealable",
                        None => "",
                    }
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
    /// `root_progress`: after 1,900 recalls in a split directory a root's
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
        let named: BTreeSet<u64> = table.iter().map(|d| d.gen).collect();
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
            if g.redelegate || named.contains(&gen) || now.wait < self.gen_until(gen) {
                again.push((self.deleg_forget_due(now, gen), gen));
                continue;
            }
            self.dl.gens.remove(&gen);
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

    /// This node holds the root lease usably (may append, recall, grant).
    pub(crate) fn root_usable(&self, now: Now) -> bool {
        self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.fenced()
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
        let ended: Vec<u64> = self
            .dl
            .mine
            .keys()
            .copied()
            .filter(|g| !live_mine.contains(g))
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
        for d in table.iter().filter(|d| d.node == me) {
            if self.dl.mine.contains_key(&d.gen) || self.dl.epoch_active {
                continue;
            }
            let awaiting = awaiting_end.get_or_insert_with(|| {
                let live: BTreeSet<u64> = table.iter().map(|d| d.gen).collect();
                replica.deleg_gens_awaiting_end(&live)
            });
            if awaiting.as_ref().is_none_or(|gens| !gens.is_empty()) {
                self.stats.deleg_install_waits_end += 1;
                tracing::debug!(
                    node = me,
                    dir = d.dir,
                    gen = d.gen,
                    ended = ?awaiting,
                    "delegation not installed yet: an ended generation's rows wait for its end in the log"
                );
                continue;
            }
            // A group that cannot be read is not "no record": installed at
            // a later sync (plan 43 C2e review).
            let Ok(group) = replica.deleg_group(d.gen) else {
                continue;
            };
            let stream_round = self.deleg_new_round();
            tracing::info!(node = me, dir = d.dir, gen = d.gen, "delegation installed");
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
            let rebooted = !d.designated && replica.deleg_held_on_another_boot(d.gen, now.boot.id);
            if rebooted {
                self.stats.deleg_rebooted_holder += 1;
                tracing::warn!(
                    node = me,
                    dir = d.dir,
                    gen = d.gen,
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
            self.dl.mine.insert(
                d.gen,
                DelegateState {
                    dir: d.dir,
                    gen: d.gen,
                    until: HonourMs(0),
                    // A takeover's strand left a hole in this stream
                    // (`Meta::strand_for_takeover`): it ends at what the
                    // log holds of it; nothing more executes here.
                    stopped: replica.delegate_stranded(d.gen) || rebooted,
                    stream_after: None,
                    stream_backoff_ms: 0,
                    refused: false,
                    streamed_through: replica.log_stream_idx(d.gen),
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
                    designated: d.designated,
                    last_root: self.root_node(),
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
                    acked_max: 0,
                    held_noted: false,
                    synced_through: 0,
                    lost_from: None,
                },
            );
            if d.designated {
                // Honoured whatever the root says or the clock does
                // (DESIGN.md §5.2: the designee writes while isolated).
                self.dl.mine.get_mut(&d.gen).expect("present").until = HonourMs(i64::MAX / 2);
            }
            self.deleg_renew_now(now, d.gen, out);
            self.arm_deleg_lapse(now, d.gen, out);
        }
        // Plan 43 C2 (D4): the group each held generation's table records.
        self.deleg_group_refresh(replica);
        // The root side.
        self.dl.root_synced = self.root_usable(now);
        if self.dl.root_synced {
            let live: BTreeSet<u64> = table.iter().map(|d| d.gen).collect();
            for d in table.iter() {
                if self.dl.gens.contains_key(&d.gen) {
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
                let cursor = replica.log_stream_idx(d.gen);
                tracing::info!(
                    node = me,
                    gen = d.gen,
                    dir = d.dir,
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
                    !d.designated,
                );
                self.dl.gens.insert(
                    d.gen,
                    GenState {
                        dir: d.dir,
                        node: d.node,
                        range: d.range,
                        kind: if d.designated {
                            DelegKind::Designated
                        } else {
                            DelegKind::Manual
                        },
                        group: GroupState::of(group_row),
                        redelegate: false,
                        granted: now.wait,
                        below_since: None,
                        seal_reqs: BTreeMap::new(),
                        sealed_tail: None,
                        waiting_for_group: false,
                        waiting_warned: None,
                        waiting_since: None,
                        member_notes: BTreeMap::new(),
                        delegate_heard: None,
                        cursor,
                        recall: RecallPhase::None,
                        recall_before_seal: RecallPhase::None,
                        ended: false,
                        expiry: None,
                        recall_req: None,
                        controls: Vec::new(),
                        read_recalls: BTreeSet::new(),
                        renew_waiting: None,
                    },
                );
                self.stats.deleg_inherited += 1;
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
                .filter(|(g, s)| !s.ended && !live.contains(g))
                .map(|(g, _)| *g)
                .collect();
            for gen in gone {
                // Ended by a record this node did not write (a
                // predecessor's, applied from the log).
                self.mark_ended(now, gen, replica, out);
            }
            if self.dl.epoch_active {
                self.recall_all(now, out);
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
            !d.lapsed && !d.stopped && !d.designated && now.honour >= self.deleg_lapse_at(d)
        }) {
            self.deleg_note_loss(gen, true);
        }
        let Some(d) = self.dl.mine.remove(&gen) else {
            return;
        };
        tracing::info!(node = self.me(), dir = d.dir, gen, "delegation ended here");
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
        let root = self.root_node().unwrap_or(0);
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
                gen,
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
                },
            });
        }
    }

    // ----------------------------------------------------- the delegate

    /// Whether `op` is this node's to execute as a delegate right now:
    /// the generation that owns every key, if it is one of this node's,
    /// not stopped, and its grant honoured.
    fn my_generation_for(&self, keys: &TouchSet, replica: &dyn Replica) -> Option<u64> {
        if self.dl.mine.is_empty() {
            return None;
        }
        match replica.resolve_ownership(keys) {
            Ownership::Delegated(d) if d.node == self.me() => {
                let s = self.dl.mine.get(&d.gen)?;
                // `stopped` follows the store's mark at the next event; a
                // generation a takeover's strand ended is refused at the
                // write already (`Meta::begin_local`), so the core never
                // executes under it, nor journals a refusal into it.
                (!s.stopped && !s.lapsed && !replica.delegate_stranded(d.gen)).then_some(d.gen)
            }
            _ => None,
        }
    }

    /// Plan 30 §M9's journaled refusal, as a delegate: a transaction of
    /// the stream (the root appends it; a retry by rid anywhere finds it).
    fn record_delegate_refusal(
        &mut self,
        rid: Rid,
        code: Code,
        gen: u64,
        deps: Position,
        replica: &dyn Replica,
    ) {
        constellation_meta::clear_last_journaled();
        if let Err(error) = replica.delegate_refusal(rid, code, gen, deps) {
            tracing::warn!(node = self.me(), ?rid, %code, %error, "could not journal a delegate refusal");
            return;
        }
        self.stats.refusals_journaled += 1;
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.executed += 1;
        }
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
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let keys = super::holder::keys_of_op_in(op, replica);
        let Some(gen) = self.my_generation_for(&keys, replica) else {
            return false;
        };
        let honoured = self
            .dl
            .mine
            .get(&gen)
            .is_some_and(|d| now.honour < d.until || d.designated);
        // The model's rule 1: the delegate waits for `deps` too.
        let reaches = replica.reaches(&deps);
        // Phase 2b (M8): the read delegations this delegate granted on
        // what the op touches are recalled first.
        let recalling =
            honoured && reaches && self.deleg_read_recall_pending(now, from, op, replica, out);
        if !honoured || !reaches || recalling {
            tracing::debug!(
                node = self.me(),
                gen,
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
        self.delegate_execute_now(now, gen, from, req, rid, op, deps, tag, replica, out);
        true
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
        tag: &LockTag,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A dependency its generation ended without (a tentative
        // acknowledgement, replayed by rid after the end): executing now
        // would put this op in the log ahead of its cause (`marker-order`,
        // long-delegated seed 74189). Not a retry of an op already done.
        if replica.deps_lost(&deps)
            && replica.recent_outcome(rid).is_none()
            && replica.completed_outcome(rid).ok().flatten().is_none()
        {
            if from == 0 {
                if self.hold_for_lost_deps(now, rid, replica, out) {
                    return;
                }
                let fresh = self.clients.get(&rid).map(|c| c.deps);
                if let Some(fresh) = fresh.filter(|d| !replica.deps_lost(d)) {
                    self.delegate_try_execute(now, 0, None, rid, op, fresh, 0, tag, replica, out);
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
                        },
                        base: None,
                        position: Position::ZERO,
                        gen: 0,
                        own_chunks: OwnChunks::None,
                        own_rows: None,
                        committed: false,
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
        let mut class = self.deleg_ack_class(now, gen, asker, replica);
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
        // (Only a `Lone` decision can be undone at the journaling.)
        let limits = if class == AckClass::Lone {
            self.deleg_own_limits(gen, replica)
        } else {
            constellation_meta::OwnLimits {
                committed: u64::MAX,
                horizon: u64::MAX,
            }
        };
        let _row_class = (!dedup).then(|| {
            // The delegate's own node's rows are the ones its `Lone` rows
            // follow ([`Self::deleg_lone_held_by_gated`]), and the ones the
            // store classes again where it journals them: the fast path's
            // rows may go in between (see below).
            if asker == Asker::Own {
                constellation_meta::PendingClass::set_own(class.into(), limits)
            } else {
                constellation_meta::PendingClass::set(class.into())
            }
        });
        if !dedup
            && asker == Asker::Own
            && matches!(class, AckClass::DelegGroup { .. })
            && self.deleg_lone_held_by_gate_alone(now, gen, replica)
        {
            self.stats.deleg_lone_held_by_gate += 1;
        }
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
        } else {
            match replica.delegate_execute(op, Some(rid), gen, deps, tag, now) {
                Ok((records, idx)) => {
                    replica.remember_outcome(rid, &records);
                    if rid.node != self.cfg.node_id {
                        replica.note_foreign_executed(&records);
                    }
                    self.stats.deleg_executed += 1;
                    if let Some(d) = self.dl.mine.get_mut(&gen) {
                        d.executed += 1;
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
        // The class the row was journaled with is its class. The store
        // classed the row again where it journaled it, under the writer
        // lock (`Meta::finish_local`): the FUSE fast path's rows go in
        // while this core waits for it, and a `Lone` decision made before
        // that no longer holds for a row past the lead or behind a gated
        // row that has not committed. Only ever to the stronger class: the
        // row waits for its group (below) like a `DelegGroup` row the core
        // decided itself.
        if !dedup && exec_idx.is_some() && class == AckClass::Lone {
            if let Some(row) = constellation_meta::take_last_journaled() {
                if row.class.gated() {
                    class = AckClass::DelegGroup { gen };
                    self.stats.deleg_lone_demoted += 1;
                }
            }
        }
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
        // journaled. A row whose class waits (`DelegGroup`, `Archive`) is
        // answered once its group holds it: the root's journaled append of
        // the stream, or the recorded backup's acknowledgement, or the log.
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
                // The root's append is what a delegate without a recorded
                // backup waits for: send the row now, not on the tick.
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
                },
                class,
                out,
            );
        }
    }

    /// Plan 43 C2 (§2.1): what an answer of a row this delegate journals now
    /// waits for, by whom it is asked. A designation's rows are
    /// non-stealable; under `ack=s3` the archive; a lone delegate (below)
    /// answers its own node's client from its disk (`Lone`, the one
    /// speculation place) and every other node's forward from the group of
    /// the generation — the root, whose journaled append is the second copy
    /// (Q16); otherwise the group (a recorded backup, or the root).
    ///
    /// Decided at journal time and never again for that row: a delegate
    /// that turns lone after journaling a `DelegGroup` row still waits for
    /// that row's commit, and the row's strand behaviour follows it.
    ///
    /// A lone delegate's own rows are `Lone` only while its journal is
    /// within a bounded lead of what the root holds
    /// ([`Self::deleg_lone_lead_full`]) and no gated row of its own node's
    /// clients is uncommitted ahead of them
    /// ([`Self::deleg_lone_held_by_gated`]); otherwise they wait for the
    /// root too, like every other node's.
    pub fn deleg_ack_class(
        &self,
        now: Now,
        gen: u64,
        asker: Asker,
        replica: &dyn Replica,
    ) -> AckClass {
        if self.cfg.ack_s3 {
            return AckClass::Archive;
        }
        match self.dl.mine.get(&gen) {
            Some(d) if d.designated => AckClass::NonStealable,
            Some(d)
                if asker == Asker::Own
                    && self.deleg_lone(now, d)
                    && !self.deleg_lone_held(d, replica) =>
            {
                AckClass::Lone
            }
            Some(_) => AckClass::DelegGroup { gen },
            // The generation ended here: what is left of it is in the log.
            None => AckClass::Archive,
        }
    }

    /// Plan 43 C2 (D6): the class and wait of the answer to a retry of `rid`
    /// that this delegate answers from the rid's earlier execution (`class`:
    /// what a new row would be given). The earlier row may still be in this
    /// delegate's own journal and uncommitted; the retry's answer then
    /// waits for it exactly as the first execution's did. A row of this
    /// generation that waits for its group (gated), or any row asked for by
    /// another node, parks under `DelegGroup`: a remote must never be
    /// answered a `Lone` row, and prefix commit makes the row committed once
    /// the root's append (or the backup) holds a later index. This node's
    /// own client, retrying a `Lone` row, is answered at once as `Lone`.
    /// The row gone from the journal (the log carries it, or it was
    /// shipped) is answered from the log. The row may be of another
    /// generation of this node than `gen` (the rid's keys moved to it: a
    /// second delegation here, the directory delegated here again before the
    /// first generation's strand): the wait is then for that generation's
    /// row — `(generation, index)` — and a generation that ended here
    /// commits only through the log, or drops the row and aborts the wait.
    fn deleg_dedup_wait(
        &self,
        class: AckClass,
        gen: u64,
        rid: Rid,
        from: NodeId,
        replica: &dyn Replica,
    ) -> (AckClass, Option<(u64, u64)>) {
        let committed_class = |gen: u64| {
            if self.cfg.ack_s3 {
                AckClass::Archive
            } else {
                AckClass::DelegGroup { gen }
            }
        };
        match replica.outcome_row(rid) {
            Some(r) if r.gen != 0 => {
                if class == AckClass::NonStealable {
                    (class, None)
                } else if r.class.gated() || from != 0 || r.gen != gen {
                    (committed_class(r.gen), Some((r.gen, r.idx)))
                } else {
                    (AckClass::Lone, None)
                }
            }
            _ if class == AckClass::NonStealable => (class, None),
            _ => (AckClass::Archive, None),
        }
    }

    /// A lone delegate's journal of `d`'s generation is as many
    /// transactions past its committed prefix (the root's cursor, or the
    /// log) as a stream batch carries records (`delegation_stream_rows`:
    /// one batch of single-record transactions, two of creates). Its own
    /// node's rows, answered from its disk, are not held back by the
    /// stream, which carries one batch per round trip to the root; another
    /// node's forward is answered once the root holds its row, behind every
    /// row journaled before it. Under its own node's load (a create loop at
    /// 7,000 a second against a stream of 640, the root 60 ms away) the
    /// journal ran ahead of the stream without bound, and every forward
    /// waited for the backlog: 90,000 rows, minutes, until the client
    /// deadline answered `EIO`. Past the lead its own rows wait for the root
    /// too (`DelegGroup`), so its node's load is held to what the stream
    /// carries and a forward waits for a bounded few batches.
    fn deleg_lone_lead_full(
        &self,
        d: &DelegateState,
        replica: &dyn Replica,
        committed: u64,
    ) -> bool {
        self.cfg.delegation_lone_lead
            && replica.delegate_idx(d.gen).saturating_sub(committed)
                >= (self.cfg.delegation_stream_rows as u64).max(1)
    }

    /// A lone delegate's own row is not `Lone` now: its journal is a lead
    /// ahead of the committed prefix ([`Self::deleg_lone_lead_full`]), or a
    /// gated row of its own node's clients is uncommitted ahead of it
    /// ([`Self::deleg_lone_held_by_gated`]).
    pub(crate) fn deleg_lone_held(&self, d: &DelegateState, replica: &dyn Replica) -> bool {
        let committed = self.deleg_committed_through(d, replica.log_stream_idx(d.gen));
        self.deleg_lone_lead_full(d, replica, committed)
            || self.deleg_lone_held_by_gated(d, replica, committed)
    }

    /// The highest stream index a `Lone` row of `d`'s generation may take:
    /// the committed prefix plus the lead. The row that takes index `n` is
    /// within the lead exactly when `n <= horizon`
    /// ([`Self::deleg_lone_lead_full`] asked about the index before it).
    /// Published with the class ([`Self::report_deleg_durable`]) so the
    /// fast path classes each row by its own index, under the writer lock,
    /// instead of by the class the core published at the end of its last
    /// step: the journal moves between the steps of a busy core. The
    /// core's own ops carry it with the committed prefix
    /// ([`Self::deleg_own_limits`]): they wait for the writer lock after
    /// their class is decided.
    fn deleg_lone_horizon(&self, through: u64) -> u64 {
        if self.cfg.delegation_lone_lead {
            through.saturating_add((self.cfg.delegation_stream_rows as u64).max(1))
        } else {
            u64::MAX
        }
    }

    /// What the store needs to class an own row again where it journals it
    /// ([`constellation_meta::PendingClass::set_own`]): the committed prefix
    /// and the horizon as this core knows them now, which only lag the
    /// store's truth by the time the row waits for the writer lock. With the
    /// hysteresis off (the simulation's negative control) no gated row holds
    /// a `Lone` row back.
    fn deleg_own_limits(&self, gen: u64, replica: &dyn Replica) -> constellation_meta::OwnLimits {
        let Some(d) = self.dl.mine.get(&gen) else {
            return constellation_meta::OwnLimits {
                committed: u64::MAX,
                horizon: u64::MAX,
            };
        };
        let committed = self.deleg_committed_through(d, replica.log_stream_idx(gen));
        constellation_meta::OwnLimits {
            committed: if self.cfg.delegation_lone_hysteresis {
                committed
            } else {
                u64::MAX
            },
            horizon: self.deleg_lone_horizon(committed),
        }
    }

    /// The only thing that keeps a lone delegate's next own row from being
    /// `Lone` is a gated row of its own that has not committed (not the
    /// lead): for the counter that shows the rule was exercised.
    fn deleg_lone_held_by_gate_alone(&self, now: Now, gen: u64, replica: &dyn Replica) -> bool {
        let Some(d) = self.dl.mine.get(&gen) else {
            return false;
        };
        if self.cfg.ack_s3 || d.designated || !self.deleg_lone(now, d) {
            return false;
        }
        let committed = self.deleg_committed_through(d, replica.log_stream_idx(gen));
        !self.deleg_lone_lead_full(d, replica, committed)
            && self.deleg_lone_held_by_gated(d, replica, committed)
    }

    /// An own `Lone` row of `d`'s generation would follow a gated row of
    /// its own node's clients that has not committed. At a root crash the
    /// strand drops the uncommitted gated row (its client was not answered
    /// and retries by rid) and replays the `Lone` one after it, which then
    /// runs without the effect it was answered on top of (a create of `f`,
    /// then a rename of `f`: the rename fails). The lead flips a lone
    /// delegate's class back and forth, and a delegate that turned lone
    /// after journaling `DelegGroup` rows has such rows too: `Lone` returns
    /// only once every one of them has committed. Another node's forward is
    /// left out: I5 would allow rolling back a `Lone` answer behind it, and
    /// counting it would hold a delegate that serves remote nodes back for
    /// good, for cost.
    ///
    /// The rule spans this node's generations: a gated row of another of
    /// them that is open holds a `Lone` row of `d`'s back too
    /// ([`Replica::delegate_gated_open_elsewhere`]). One node can hold two
    /// generations whose keys meet (two ranges of a split directory, or a
    /// range and a whole subtree): an unlink of a file's primary link moves
    /// the file's own keys to the generation of its next link, with neither
    /// generation ending, and a takeover's strand ends every own generation
    /// together.
    fn deleg_lone_held_by_gated(
        &self,
        d: &DelegateState,
        replica: &dyn Replica,
        committed: u64,
    ) -> bool {
        self.cfg.delegation_lone_hysteresis
            && (replica.delegate_gated_idx(d.gen) > committed
                || replica.delegate_gated_open_elsewhere(d.gen))
    }

    /// The committed prefix of `gen`'s stream as this delegate knows it
    /// (`None`: not held here), for the simulation's oracles.
    pub fn deleg_committed_for_audit(&self, gen: u64, replica: &dyn Replica) -> Option<u64> {
        self.dl
            .mine
            .get(&gen)
            .map(|d| self.deleg_committed_through(d, replica.log_stream_idx(gen)))
    }

    /// Plan 43 C2 (§2.1): this delegate is lone for `d`'s generation: its
    /// replica records no backup for it and none is requested, it appends
    /// to none, and no peer — the root included — is a backup candidate
    /// (linked, within `backup_rtt_budget_ms`, up for `backup_stable_ms`:
    /// the rule `Core::backup_candidates` selects by, D10). The group is
    /// itself then, and its own node's client is answered from its disk.
    pub(crate) fn deleg_lone(&self, now: Now, d: &DelegateState) -> bool {
        d.group_backups.is_empty()
            && d.group_requested.is_none()
            && d.backup.is_none()
            && self.backup_candidates(now).is_empty()
    }

    /// The backup this delegate tells the root it wants (`DelegRenew.backup`,
    /// the *request* for a `DelegGroup` record): the change it asked for and
    /// the table does not record yet, else the group it applied.
    pub(crate) fn deleg_requested_backup(d: &DelegateState) -> Option<NodeId> {
        match &d.group_requested {
            Some(v) => v.first().copied(),
            None => d.group_backups.first().copied(),
        }
    }

    /// Plan 43 C2 (§3.4): transaction `(gen, idx)` is committed — the answer
    /// of a row of the class `DelegGroup` may leave — from what this
    /// delegate knows: the log carries it, or every member of the group
    /// holds it. See [`Self::deleg_stream_state_with`].
    pub(crate) fn deleg_stream_durable(&self, gen: u64, idx: u64, replica: &dyn Replica) -> bool {
        self.deleg_stream_state_with(gen, idx, || replica.log_stream_idx(gen), || true)
            == StreamState::Committed
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
        let StreamCache { log_idx, pending } = cache;
        self.deleg_stream_state_with(
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
        )
    }

    /// Plan 43 C2 (§3.4): the highest index of `d`'s stream that every
    /// member of its group holds — of the applied version and, while a
    /// change is pending, of the requested one (D4): a recorded backup by
    /// its acknowledgement of this delegate's appends (a backup that is
    /// not the one appended to holds nothing for it), the root (no backup
    /// recorded: the group is the delegate and the root, Q16) by the
    /// cursor of its journaled append of the stream (`streamed_through`).
    /// What the log carries is not counted here.
    fn deleg_group_through(d: &DelegateState) -> u64 {
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
                .unwrap_or(d.streamed_through)
        };
        held(&d.group_backups).min(d.group_requested.as_deref().map_or(u64::MAX, held))
    }

    /// Plan 43 C2d (§4): the highest index of generation `d`'s stream whose
    /// prefix is committed — the log carries it (`log_idx`), or its group
    /// holds it. `ack=s3` commits only through the log. What the delegate's
    /// FUSE fast path waits on ([`Self::report_deleg_durable`]), the same
    /// predicate the core path's parks wait on
    /// ([`Self::deleg_stream_state_with`]).
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
    pub(crate) fn report_deleg_durable(&mut self, now: Now, replica: &dyn Replica) {
        if self.dl.mine.is_empty() && self.dl.reported.is_empty() {
            return;
        }
        let mut current = BTreeMap::new();
        let mut raised = Vec::new();
        for d in self.dl.mine.values() {
            let class = self.deleg_ack_class(now, d.gen, Asker::Own, replica);
            let through = self.deleg_committed_through(d, replica.log_stream_idx(d.gen));
            // The fast path answers its own node's clients from the FUSE
            // thread, where the core does not see it: every index committed
            // may have been answered — as a `DelegGroup` row journaled
            // before the delegate turned lone is, whatever the class is
            // now — so `deleg_group_maintain` lets go of none of them
            // before the group that is left holds it (`acked_max`).
            // For a lone delegate `through` is at most the root's cursor
            // or the log's index, so the raise holds back an added backup
            // only until it has caught up with what is streamed and never
            // blocks a removal (`acked_max <= streamed.max(logged)`).
            if through > d.acked_max {
                raised.push((d.gen, through));
            }
            current.insert(
                d.gen,
                constellation_meta::DelegDurable {
                    class: class.into(),
                    through,
                    horizon: self.deleg_lone_horizon(through),
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
    /// the root, Q16) by the cursor of its journaled append of the stream
    /// (`DelegateStreamAck.through`, `streamed_through`). A generation no
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
        let committed = match self.dl.mine.get(&gen) {
            Some(d) if !self.cfg.ack_s3 => idx <= Self::deleg_group_through(d) || idx <= log_idx(),
            _ => idx <= log_idx(),
        };
        if committed {
            StreamState::Committed
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
    /// root journals the `DelegGroup` record; the delegate counts the new
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
            let (acked_max, backup_acked, streamed) =
                (d.acked_max, d.backup_acked, d.streamed_through);
            let safe = acked_max == 0 || {
                let logged = replica.log_stream_idx(gen);
                if adding {
                    backup_acked >= acked_max || acked_max <= logged
                } else {
                    acked_max <= streamed.max(logged)
                }
            };
            if !safe {
                continue;
            }
            tracing::info!(
                node = self.cfg.node_id,
                gen,
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
            // A read that failed is not "no record": the group known stays
            // (plan 43 C2e review: an emptied group made the delegate lone).
            let Ok(row) = replica.deleg_group(d.gen) else {
                continue;
            };
            let (version, backups) = row.map_or((0, Vec::new()), |r| (r.version, r.backups));
            if version != d.group_version || backups != d.group_backups {
                d.group_version = version;
                d.group_backups = backups;
            }
            if d.group_requested.as_ref() == Some(&d.group_backups) {
                d.group_requested = None;
            }
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
        for d in self.dl.mine.values_mut() {
            if d.backup.is_some() || d.backup_sealed || d.stopped {
                continue;
            }
            if let Some(b) = candidates.iter().find(|n| Some(**n) != root).copied() {
                d.backup = Some(b);
                d.backup_acked = 0;
                d.backup_sent_through = 0;
                d.backup_after = None;
                chosen.push(d.gen);
                tracing::info!(
                    node = self.cfg.node_id,
                    gen = d.gen,
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
            let txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            if txs.is_empty() {
                continue;
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
                gen,
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
                        gen,
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
            let Some(a) = replica.deleg_backup_append(gen, &txs) else {
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
            gen,
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
            },
        });
    }

    /// Phase 2b: a member's answer to a seal: append its tail, end the
    /// generation, delegate the directory to the backup.
    ///
    /// Plan 43 C2e (§3.7, D3, D8): a *strict* member (of the latest
    /// `DelegGroup` version) ends the generation once its tail is drained to
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
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
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
                self.deleg_drain_tail(now, gen, &txs, true, replica, out);
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
        let (dir, kind, range) = (gs.dir, gs.kind, gs.range);
        let tail = gs.sealed_tail.clone();
        let ends_recall = self.root_ends_recall(now, gen);
        let mut sealed = false;
        if let Some(tail) = tail {
            sealed = true;
            let mut drained_to = self.dl.gens[&gen].cursor;
            if ends_recall {
                drained_to = self.deleg_drain_tail(now, gen, &tail.txs, first, replica, out);
            }
            if !ends_recall || drained_to < tail.end {
                if first {
                    self.stats.deleg_seal_waits += 1;
                }
                tracing::debug!(
                    node = self.me(),
                    gen,
                    backup = from,
                    cursor = drained_to,
                    tail_end = tail.end,
                    ends_recall,
                    "a sealed delegation's tail is not drained to its end: the generation is not ended yet"
                );
                return;
            }
        }
        self.end_generation(now, gen, None, replica, out);
        if sealed && kind != DelegKind::Designated {
            match self.delegate_dir(now, dir, from, kind, range, replica, out) {
                Ok(new_gen) => {
                    tracing::info!(
                        node = self.me(),
                        dir,
                        delegate = from,
                        gen,
                        new_gen,
                        "re-delegated to the backup"
                    );
                }
                Err(why) => {
                    tracing::debug!(node = self.me(), dir, why, "not re-delegated to the backup")
                }
            }
        }
    }

    /// Append the contiguous rows of a sealed tail (`txs`) that follow the
    /// generation's cursor and whose dependencies the root holds; returns the
    /// cursor. `count`: the stop reasons and the drain are counted (a retry of
    /// the same tail is not).
    fn deleg_drain_tail(
        &mut self,
        now: Now,
        gen: u64,
        txs: &[constellation_meta::DelegateTx],
        count: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> u64 {
        let Some(gs) = self.dl.gens.get(&gen) else {
            return 0;
        };
        let mut cursor = gs.cursor;
        let mut drained = 0u64;
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
            if !replica.reaches_streams(&tx.deps) {
                if count {
                    self.stats.deleg_deps_unsatisfied_at_append += 1;
                }
                break;
            }
            match replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps) {
                Ok(_) => {
                    replica.note_foreign_executed(&tx.records);
                    cursor = tx.idx;
                    drained += 1;
                }
                Err(error) => {
                    tracing::warn!(node = self.me(), gen, idx = tx.idx, %error, "could not append a sealed transaction");
                    break;
                }
            }
        }
        if drained == 0 {
            return cursor;
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
            gen,
            drained,
            cursor,
            "drained a sealed delegation"
        );
        cursor
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
        if self.cfg.delegation && !self.dl.root_synced && self.root_usable(now) {
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
        // Phase 2b: the placement runs on the root, delegations or not.
        if self.root_usable(now) {
            self.place_arm(now, out);
            // M12: a re-delegation held back (the op still parked, the
            // delegate away) is tried again on the next event.
            if self.dl.gens.values().any(|g| g.ended && g.redelegate) {
                self.deleg_redelegate_after_cross(now, replica, out);
            }
        }
        self.deleg_forget_ended(now, replica);
        // Plan 43 C2e (D8): a strict member's sealed tail that the drain
        // stopped short of (a dependency the log brings, a root that could
        // not end the recall) is drained again; the generation ends once the
        // cursor reaches the tail's end.
        let stalled: Vec<(u64, NodeId)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended)
            .filter_map(|(gen, g)| g.sealed_tail.as_ref().map(|t| (*gen, t.from)))
            .collect();
        for (gen, from) in stalled {
            self.deleg_end_sealed(now, gen, from, false, replica, out);
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
        let root = self.root_node();
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
                if d.last_root.is_some() && root.is_some() {
                    d.streamed_through = replica.log_stream_idx(d.gen);
                    d.refused = false;
                    d.stream_after = None;
                    d.stream_backoff_ms = 0;
                    self.stats.deleg_restreams += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        gen = d.gen,
                        ?root,
                        "re-streaming to a new root"
                    );
                }
                d.last_root = root;
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
            let honoured = now.honour < d.until && !d.stopped;
            if !honoured {
                continue;
            }
            let candidates: Vec<(usize, NodeId, MutateOp)> = d
                .parked
                .iter()
                .enumerate()
                .filter(|(_, p)| replica.reaches(&p.deps))
                .map(|(i, p)| (i, p.from, p.op.clone()))
                .collect();
            let mut ready = Vec::new();
            for (i, from, op) in candidates {
                if !self.deleg_read_recall_pending(now, from, &op, replica, out) {
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
                self.delegate_execute_now(
                    now, gen, p.from, p.req, p.rid, &p.op, p.deps, &p.tag, replica, out,
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
        crate::deleg_lane::stuck_threshold_ms(self.cfg.delegation_ttl_ms / 2)
            + crate::deleg_lane::DELEG_RESEND_SLACK_MS
    }

    /// How long a stream batch, backup append or renewal waits for its
    /// answer before it is sent again (a partition drops it silently).
    pub(crate) fn deleg_request_timeout_ms(&self) -> u64 {
        self.cfg
            .forward_timeout_ms
            .max(self.cfg.delegation_stream_tick_ms * 4)
            .max(1)
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
        let root = self.root_node();
        let mut late = Vec::new();
        let mut given_up = Vec::new();
        let mut renew_again = Vec::new();
        for d in self.dl.mine.values_mut() {
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
                let early = !d.renew_resent && !d.stopped && !d.designated && age >= resend;
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
        let Some(root) = self.root_node() else {
            // Learn the holder; the tick streams.
            self.deleg_learn_root(now, out);
            self.arm_stream_tick(now, out);
            return;
        };
        let gens: Vec<u64> = self.dl.mine.keys().copied().collect();
        for gen in gens {
            let d = self.dl.mine.get(&gen).expect("present");
            if d.inflight.is_some() || d.refused || d.stream_after.is_some_and(|t| now.wait < t) {
                continue;
            }
            let from = d.streamed_through + 1;
            let mut txs = replica.delegate_txs_from(gen, from, self.cfg.delegation_stream_rows);
            // Plan 30 §M12: a node holds several generations (the ranges
            // of a directory, a subtree); a transaction of one may depend
            // on this node's own execution under another (it reached it
            // locally). The root must hold that first: the batch stops
            // before a transaction whose own-generation deps the root has
            // not acknowledged, and goes out once it has.
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
                let d = self.dl.mine.get_mut(&gen).expect("present");
                if d.lost_from.is_none_or(|l| idx < l) {
                    d.lost_from = Some(idx);
                    d.stopped = true;
                    self.stats.deleg_lost_deps_stopped += 1;
                    tracing::warn!(
                        node = self.cfg.node_id,
                        gen,
                        idx,
                        "a row depends on one its generation ended without: \
                         the generation stops before it, the row is replayed by rid"
                    );
                }
            }
            let d = self.dl.mine.get(&gen).expect("present");
            if txs.is_empty() {
                continue;
            }
            let last = txs.last().map(|t| t.idx).unwrap_or(from);
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
            if d.designated && last > d.synced_through {
                let through = replica.delegate_idx(gen);
                if !replica.sync_journal() {
                    let tick = self.cfg.delegation_stream_tick_ms.max(1);
                    let d = self.dl.mine.get_mut(&gen).expect("present");
                    d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
                    d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
                    continue;
                }
                self.stats.deleg_designee_syncs += 1;
                let d = self.dl.mine.get_mut(&gen).expect("present");
                d.synced_through = d.synced_through.max(through);
            }
            let leaving = self.deleg_leaving_grants(now, gen, replica);
            let leaving_barriers = self.deleg_leaving_barriers(now, gen, &leaving, replica);
            let req = self.op_id();
            self.dl.by_req.insert(req, (gen, ReqKind::Stream));
            self.stats.deleg_streamed_txs += txs.len() as u64;
            let d = self.dl.mine.get_mut(&gen).expect("present");
            let round = d.stream_round;
            d.inflight = Some((req, last));
            d.inflight_at = now.wait;
            d.leaving = (!leaving.is_empty() || !leaving_barriers.is_empty()).then(|| {
                let mut inos: Vec<Ino> = leaving.iter().map(|g| g.ino).collect();
                inos.extend(leaving_barriers.iter().map(|(ino, _)| *ino));
                (last, inos)
            });
            tracing::debug!(
                node = self.me(),
                gen,
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
                },
            });
        }
        // A lost ack (the transport reports it), a batch that could not
        // be sent, a generation (or its backup append) backing off, or
        // rows held back behind another generation's acknowledgement:
        // the tick retries.
        if self.dl.mine.values().any(|d| {
            d.inflight.is_some()
                || d.stream_after.is_some_and(|t| now.wait < t)
                || d.backup_after.is_some_and(|t| now.wait < t)
                || (!d.stopped && replica.delegate_idx(d.gen) > d.streamed_through)
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
        refused: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let root = self.root_node();
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let stale = root != Some(from) || d.last_root != Some(from) || round != d.stream_round;
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            gen,
            ?req,
            through,
            refused,
            was = d.streamed_through,
            inflight = ?d.inflight,
            stale,
            "delegate stream acknowledgement"
        );
        if stale {
            self.stats.deleg_stream_acks_stale += 1;
            return;
        }
        let answers_inflight = d.inflight.is_some_and(|(r, _)| r == req);
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
                return;
            }
            // The generation is ending, or the root has not learned it
            // (a successor before its table, a holder inside its takeover
            // gate): the log ends a generation, a refusal does not — back
            // off and try again on the tick.
            self.stats.deleg_stream_refused += 1;
            let tick = self.cfg.delegation_stream_tick_ms.max(1);
            d.stream_backoff_ms = (d.stream_backoff_ms * 2).clamp(tick * 4, 2_000);
            d.stream_after = Some(now.wait.plus(d.stream_backoff_ms));
            self.arm_stream_tick(now, out);
            return;
        }
        let progressed = through > d.streamed_through;
        if !progressed && !answers_inflight {
            return;
        }
        if progressed && !answers_inflight {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                through,
                was = d.streamed_through,
                "a stream acknowledgement for an earlier batch counted"
            );
        }
        d.streamed_through = d.streamed_through.max(through);
        if d.inflight.is_none() {
            d.stream_after = None;
            d.stream_backoff_ms = 0;
        }
        if progressed && !answers_inflight {
            self.stats.deleg_stream_acks_unmatched += 1;
        }
        self.deleg_drop_left(gen, replica);
        // Plan 43 C2 (§3.4): the root's journaled append is the second copy a
        // delegate without a recorded backup waits for: what waited for
        // these rows is answered now.
        if progressed && self.has_stream_parks() {
            self.complete_ready(now, replica, out);
        }
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
                // Outwaited by the expiry timer.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.recall_req = None;
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
                        gen,
                        backup = b,
                        "delegate dropped an unreachable backup"
                    );
                }
                self.arm_stream_tick(now, out);
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
                        g.recall = match g.recall_before_seal {
                            RecallPhase::Sealing => RecallPhase::None,
                            before => before,
                        };
                        granting = g.recall == RecallPhase::None && !g.ended;
                        // The member is unreachable (down, or cut from this
                        // root): the generation cannot end without its tail,
                        // and says so, as when the seal goes unanswered.
                        // The seals asked of the other strict members are of
                        // the seal that is over.
                        g.seal_reqs.retain(|_, (_, strict)| !*strict);
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
                        gen,
                        "a delegation's recorded backup cannot be reached for its seal: the generation is not ended without its tail \
                         (`constellation repair abandon-group --gen {gen}` ends it at the root's cursor; rows only the group holds are lost)"
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

    pub(crate) fn deleg_renew_now(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(root) = self.root_node() else {
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
        // (the root journals a `DelegGroup` record for it), or the group the
        // delegate applied.
        let backup = Self::deleg_requested_backup(d);
        d.renew_backup = backup;
        let round = d.stream_round;
        // Honoured until 0: never granted (a grant sets it).
        let ungranted = d.until == HonourMs(0);
        tracing::debug!(
            node = self.cfg.node_id,
            gen,
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
                // Row 9 (plan 43 Appendix B, C7b): the root compares it with
                // its own clock; kept apart from the renewal's honour send.
                stream_head_at: now.wall.0,
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
    /// as asked by its delegate's renewal: `DelegGroup { version: latest + 1,
    /// backups }`. Only while the generation lives and is not ending (the
    /// end uses the latest record). The record is a root row like any other
    /// (its class is the root's now); no answer waits for it, and the
    /// delegate counts the new member only once its own table records it,
    /// i.e. once the record is committed (D4).
    fn deleg_journal_group(
        &mut self,
        gen: u64,
        dir: Ino,
        backups: Vec<NodeId>,
        replica: &dyn Replica,
    ) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        let version = g.group.version + 1;
        let _class = self.journal_class();
        let rec = LogRecord::DelegGroup {
            dir,
            gen,
            version,
            backups: backups.clone(),
        };
        if let Err(error) = replica.apply_records_journaled(&[rec], None) {
            tracing::warn!(node = self.me(), gen, %error, "could not journal a delegation group record");
            return;
        }
        self.stats.deleg_group_records += 1;
        tracing::info!(
            node = self.me(),
            gen,
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
        let mut group_change: Option<(Ino, Vec<NodeId>)> = None;
        if self.root_usable(now) && !self.dl.epoch_active {
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
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                if g.node == from && !g.ended && g.recall == RecallPhase::None {
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
                            group_change = Some((g.dir, want));
                        }
                    }
                }
            }
            if let Some((dir, want)) = group_change {
                self.deleg_journal_group(gen, dir, want, replica);
            }
            tracing::debug!(
                node = self.me(),
                from,
                gen,
                ?req,
                ttl_ms,
                "delegation renewal"
            );
            if ttl_ms > 0 {
                self.stats.deleg_renewals += 1;
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
                gen,
                ?req,
                "delegation renewal left unanswered: this node may be the root again"
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
        let Some(dir) = self.dl.gens.get(&gen).map(|g| g.dir) else {
            return false;
        };
        self.dl.answered_granting.contains(&gen)
            && self.lock_deleg_quiet(gen, dir, replica)
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
                        && g.kind != DelegKind::Designated
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
        let renewed = if d.until.0 > 0 {
            d.until.plus(2 * self.cfg.expiry_margin_ms)
        } else {
            HonourMs(0)
        };
        renewed.max(d.installed.plus(self.reclaim_horizon_ms()))
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
        if d.designated || d.stopped || d.lapsed {
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
        if d.designated || d.stopped || d.lapsed {
            return;
        }
        let d = self.dl.mine.get(&gen).expect("present");
        if now.honour < d.until || now.honour < self.deleg_lapse_at(d) {
            self.arm_deleg_lapse(now, gen, out);
            return;
        }
        let (dir, until_ms) = (d.dir, d.until.0);
        let (frozen, root_silent) = self.deleg_note_loss(gen, true);
        tracing::warn!(
            node = self.me(),
            gen,
            dir,
            root = self.root_node().unwrap_or(0),
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
        let root = self.root_node().unwrap_or(0);
        for p in parked {
            self.answer_not_owner(now, p, root, replica, out);
        }
        self.deleg_renew_now(now, gen, out);
        self.deleg_lapsed_retry(now, gen, out);
        self.deleg_after_event(now, replica, out);
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
        let root = self.root_node();
        let timeout = self.deleg_request_timeout_ms();
        self.dl
            .mine
            .values()
            .filter(move |d| root.is_some() && !d.stopped && !d.designated && d.until.0 > 0)
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
        let root = self.root_node();
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
        if self.root_node() != Some(to) {
            return;
        }
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        if d.stopped || d.designated || d.stream_round != round || d.until.0 == 0 {
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
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let root = self.root_node();
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            gen,
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
                    self.deleg_renewal_refused(now, gen, replica, out);
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
        if let Some(t) = d.renew_timer.take() {
            // (A lapsed generation's retry.)
            self.cancel_timer(t, out);
        }
        if !self.dl.mine.contains_key(&gen) {
            return true;
        }
        if ttl_ms == 0 {
            self.deleg_renewal_refused(now, gen, replica, out);
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
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(d) = self.dl.mine.get(&gen) else {
            return;
        };
        self.stats.deleg_renewals_refused += 1;
        let ran_out = !d.lapsed && now.honour >= self.deleg_lapse_at(d);
        if ran_out {
            // (The root answered: it cannot have been silent.)
            self.deleg_note_loss(gen, false);
        }
        let d = self.dl.mine.get_mut(&gen).expect("present");
        d.stopped = true;
        let parked = std::mem::take(&mut d.parked);
        let root = self.root_node().unwrap_or(0);
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
                gen,
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
        if d.group_requested.is_some() && d.renew_backup != Self::deleg_requested_backup(d) {
            // Plan 43 C2: the root journals the group change when it learns
            // the request — tell it now, not at the next timer.
            self.deleg_renew_now(now, gen, out);
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
        if d.held_noted || d.designated {
            return true;
        }
        if replica.deleg_note_held(gen, now.boot.id) {
            d.held_noted = true;
            return true;
        }
        d.stopped = true;
        tracing::warn!(
            node = self.cfg.node_id,
            gen,
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
        let root = self.root_node();
        let serving = self.deleg_mine_until(gen).is_some();
        let sent = match self.dl.mine.get(&gen) {
            Some(d)
                if ttl_ms > 0
                    && serving
                    && !d.designated
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
        let root = self.root_node();
        let Some(late) = self.deleg_take_late(req, ReqKind::Renew, gen, None) else {
            return;
        };
        if ttl_ms == 0 || late.to.is_none() || late.to != root {
            return;
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
            gen,
            late_ms = now.honour.since(sent),
            "a late delegation renewal counted"
        );
        d.until = until;
        d.last_renew_sent = d.last_renew_sent.max(sent);
        d.answered_sent = d.answered_sent.max(sent);
        if d.lapsed && now.honour < d.until {
            tracing::info!(
                node = self.cfg.node_id,
                gen,
                "a lapsed delegation was renewed"
            );
            d.lapsed = false;
        }
        self.arm_deleg_lapse(now, gen, out);
        self.deleg_after_event(now, replica, out);
    }

    // ----------------------------------------------------- recalls (delegate)

    pub(crate) fn on_deleg_recall(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A stream a takeover's strand holed has nothing past what the
        // log holds of it: its counter's later indices were root rows'.
        let through = if replica.delegate_stranded(gen) {
            replica.log_stream_idx(gen)
        } else {
            replica.delegate_idx(gen)
        };
        // Plan 30 §M14: the subtree's lock grants go back with the answer;
        // its waiters are told to ask the root.
        let locks = self.lock_hand_back(now, gen, replica);
        let seen = replica
            .delegation_table()
            .iter()
            .any(|d| d.gen == gen && d.node == self.me());
        self.dl.handed_back.insert(gen, (from, now.wait, seen));
        if let Some(d) = self.dl.mine.get_mut(&gen) {
            d.stopped = true;
            self.stats.deleg_recalls_received += 1;
            let parked = std::mem::take(&mut d.parked);
            for p in parked {
                self.answer_not_owner(now, p, from, replica, out);
            }
            // Drain: everything executed streams as usual; the root
            // waits for the cursor to reach `through`.
            self.deleg_stream(now, replica, out);
        }
        // Not past a row whose dependency was lost (`deleg_stream`): it is
        // never streamed, and the root ends the generation before it.
        let through = match self.dl.mine.get(&gen).and_then(|d| d.lost_from) {
            Some(lost) => through.min(lost - 1),
            None => through,
        };
        // Phase 2b (M8): the read delegations this delegate granted are
        // recalled first; the root executes under the subtree only once
        // they are gone (or, past the horizon, expired).
        if self.cfg.read_delegations {
            let need = replica.read_delegations().all_live(now.wait);
            if let Some(wait) = self.start_recalls(now, need, out) {
                self.park(
                    now,
                    wait,
                    None,
                    super::readindex::ParkedWhat::DelegRecalled {
                        to: from,
                        req,
                        gen,
                        through,
                        locks,
                    },
                );
                self.lock_reserve_all(now, replica, out);
                return;
            }
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegRecalled {
                req,
                gen,
                through,
                locks,
            },
        });
        self.lock_reserve_all(now, replica, out);
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
        let epoch = super::DELEG_READ_EPOCH_BASE + gen;
        let grant = if self.cfg.read_delegations && from != self.cfg.node_id {
            let margin = self.cfg.expiry_margin_ms as i64;
            // A delegate's read grant is capped by its own honour end.
            let cap = d.until.since(now.honour) - margin;
            let ttl = (self.cfg.read_delegation_ttl_ms as i64).min(cap);
            if ttl > 0
                && !self
                    .rd
                    .blocked
                    .get(&ino)
                    .is_some_and(|until| *until > now.wait)
                && !self.split_directory(ino, replica)
            {
                let until = now.wait.plus((ttl + margin) as u64);
                let id = replica.read_delegations().grant(from, ino, until);
                self.stats.deleg_read_grants += 1;
                Some(crate::event::ReadGrantMsg {
                    id,
                    ttl_ms: ttl as u64,
                    epoch,
                })
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

    /// Plan 30 §M8 under §M12: whether `ino` is a directory split into
    /// name-hash ranges. Its entries have a sequencer per range (the root
    /// for the ranges nobody holds), and each one's writes recall only
    /// the read delegations in its own table, while a delegation on the
    /// directory covers all its entries. So none is granted on it, by the
    /// root or by a range's delegate: a lookup in it asks the name's
    /// sequencer every time.
    pub(crate) fn split_directory(&self, ino: Ino, replica: &dyn Replica) -> bool {
        self.cfg.delegation && replica.delegation_table().is_split(ino)
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
                    honoured.then_some(d.node)
                } else {
                    Some(d.node)
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
    pub(crate) fn deleg_recall_plan(
        &mut self,
        now: Now,
        keys: &TouchSet,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> RecallPlan {
        if !self.cfg.delegation || !self.deleg_known() {
            return RecallPlan::None;
        }
        let (involved, cross): (Vec<u64>, bool) = match replica.resolve_ownership(keys) {
            Ownership::Root => return RecallPlan::None,
            Ownership::Delegated(d) => (vec![d.gen], false),
            Ownership::CrossSubtree { involved, .. } => {
                self.stats.deleg_cross_subtree += 1;
                (involved.iter().map(|d| d.gen).collect(), true)
            }
        };
        let designated = involved.iter().any(|g| {
            self.dl
                .gens
                .get(g)
                .is_some_and(|s| !s.ended && s.kind == DelegKind::Designated)
        });
        if designated {
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
        let mut waiting = BTreeSet::new();
        for gen in involved {
            if self.dl.gens.get(&gen).is_some_and(|g| g.ended) {
                continue;
            }
            if cross {
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    // Ended by an op, not a decision: delegate it again
                    // to the same node once the op ran (a known 2a gap).
                    g.redelegate = true;
                }
            }
            self.start_recall(now, gen, out);
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
        let again: Vec<(u64, Ino, NodeId, DelegKind, Range)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| g.ended && g.redelegate)
            .map(|(gen, g)| (*gen, g.dir, g.node, g.kind, g.range))
            .collect();
        for (gen, dir, node, kind, range) in again {
            if let Some(g) = self.dl.gens.get_mut(&gen) {
                g.redelegate = false;
            }
            if !self.dl.pending_exec.is_empty()
                || self.has_exec_parks()
                || self.rd_has_recall_wait(gen)
                || self.inbox_holds_redelegation(now)
            {
                // The op that ended it has not run yet: next time.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.redelegate = true;
                }
                continue;
            }
            if !self.links.get(&node).is_some_and(|l| l.connected) {
                // The delegate is away for the moment (M12: a range's
                // delegate reconnecting mid-recall): once it is back.
                if let Some(g) = self.dl.gens.get_mut(&gen) {
                    g.redelegate = true;
                }
                continue;
            }
            match self.delegate_dir(now, dir, node, kind, range, replica, out) {
                Ok(new_gen) => {
                    self.stats.deleg_redelegated += 1;
                    tracing::info!(
                        node = self.me(),
                        dir,
                        delegate = node,
                        gen,
                        new_gen,
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

    pub(crate) fn start_recall(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        if g.ended || g.recall != RecallPhase::None {
            return;
        }
        g.recall = RecallPhase::Sent;
        // The slot closes at the transition, in this step (plan 41 R1,
        // rule 2): no renewal is granted from here, whoever answers it.
        self.dw.close_for_recall(gen);
        let (to, dir) = (g.node, g.dir);
        let req = self.op_id();
        self.dl.gens.get_mut(&gen).expect("present").recall_req = Some(req);
        self.dl.by_req.insert(req, (gen, ReqKind::Recall));
        self.stats.deleg_recalls_sent += 1;
        tracing::info!(
            node = self.me(),
            gen,
            dir,
            delegate = to,
            "recalling a delegation"
        );
        out.push(Action::Send {
            to,
            msg: PeerMsg::DelegRecall { req, dir, gen },
        });
        self.arm_gen_expiry(now, gen, out);
    }

    /// A release lets another node take the lease over at once. A
    /// generation this root is recalling would then end nowhere: the
    /// lease's end drops it with its recall (`deleg_on_lease_gone`), the
    /// delegate's answer finds no request to match (`on_deleg_recalled`)
    /// and the lock grants it hands back are dropped
    /// (`lock_install_returned`), and no `RecallCut` is journaled, which
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
        let bound = until
            .plus(2 * self.cfg.forward_timeout_ms + self.cfg.delegation_stream_tick_ms.max(100));
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

    fn recall_all(&mut self, now: Now, out: &mut Vec<Action>) {
        let gens: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.recall == RecallPhase::None)
            .map(|(g, _)| *g)
            .collect();
        for gen in gens {
            self.start_recall(now, gen, out);
        }
    }

    fn arm_gen_expiry(&mut self, now: Now, gen: u64, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get(&gen) else {
            return;
        };
        if g.ended || g.kind == DelegKind::Designated {
            // A designation never expires: `online` ends it (an
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
        let outwaited_dir = (g.recall != RecallPhase::Sealing).then_some(g.dir);
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
        if let Some(dir) = outwaited_dir {
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
                    gen,
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
                    gen,
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
                        gen,
                        "a delegation's recorded backup has not answered its seal: the generation is not ended without its tail \
                         (`constellation repair abandon-group --gen {gen}` ends it at the root's cursor; rows only the group holds are lost)"
                    );
                }
                self.dw.close(gen);
                self.dw.set_until_closed(
                    gen,
                    now.wait
                        .plus(self.cfg.backup_takeover_ms.max(self.cfg.forward_timeout_ms)),
                );
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

    pub(crate) fn on_deleg_recalled(
        &mut self,
        now: Now,
        req: OpId,
        gen: u64,
        through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((g, ReqKind::Recall)) = self.dl.by_req.remove(&req) else {
            return;
        };
        if g != gen {
            return;
        }
        let Some(gs) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        gs.recall_req = None;
        gs.delegate_heard = Some(now.wait);
        if gs.recall == RecallPhase::Sent {
            gs.recall = RecallPhase::Drained(through);
        }
        self.root_progress(now, replica, out);
    }

    /// A drained recall whose stream caught up ends the generation.
    fn root_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let done: Vec<u64> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended && matches!(g.recall, RecallPhase::Drained(th) if g.cursor >= th)
            })
            .map(|(g, _)| *g)
            .collect();
        for gen in done {
            self.stats.deleg_recalls_drained += 1;
            self.end_generation(now, gen, None, replica, out);
        }
    }

    /// Plan 43 C2e (Q1, `constellation repair abandon-group --gen G`): the
    /// operator accepts that the group of generation `gen` is gone for good.
    /// Valid on the root of a generation that waits for its group
    /// (`status.delegation.gens[].waiting_for_group`): it is ended at the
    /// cursor now, with `LogRecord::GroupAbandoned` journaled with the cut
    /// (the audit), and the rows only the group held — acknowledged to
    /// their clients — are lost (the delegate's, if it comes back, are
    /// dropped by the recall's strand).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_control_abandon_generation(
        &mut self,
        now: Now,
        op: OpId,
        epoch: Epoch,
        gen: u64,
        force: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let heard = self
            .dl
            .gens
            .get(&gen)
            .and_then(|g| self.deleg_group_heard_lately(now, g));
        let result = match self.dl.gens.get(&gen) {
            _ if epoch != 0 => Err(
                "a delegation's group is named by --gen alone (no --epoch)".to_string(),
            ),
            None => Err(format!(
                "this node is not the root of generation {gen} (run it on the node whose `status` lists it under delegation.gens)"
            )),
            Some(g) if g.ended => Err(format!("generation {gen} has ended already")),
            Some(g) if !g.waiting_for_group => Err(format!(
                "generation {gen} is not waiting for its group (delegation.gens[].waiting_for_group)"
            )),
            // A member answered: its tail is here, the group is not lost.
            // The root appends it as its dependencies allow; ending the
            // generation now would drop the rows past the cursor that the
            // group acknowledged and that are in this very tail.
            Some(g) if g.sealed_tail.is_some() => {
                let t = g.sealed_tail.as_ref().expect("checked");
                Err(format!(
                    "node {}'s tail of generation {gen} is in hand (appended through {} of {}) and is appended as soon as its \
                     dependencies are here; this group is not lost, so it is not abandoned (see delegation.waiting)",
                    t.from, g.cursor, t.end
                ))
            }
            // The discriminator (C2e follow-up): a delegate or a strict
            // member this root heard from within a grant's ttl may be alive
            // and cut from it only; its tail, or the delegate's rows, are not
            // lost. The operator confirms from elsewhere that the nodes are
            // down, and forces.
            Some(_) if !force && heard.is_some() => {
                let (node, role, ago) = heard.expect("checked");
                Err(format!(
                    "this root heard from node {node} ({role} of generation {gen}) {ago} ms ago, within \
                     delegation_ttl_ms ({} ms): it may be alive and cut from this node only, holding rows its \
                     clients were told are committed. Confirm from elsewhere that the delegate and the members \
                     of its group are down, then run the command again with --force",
                    self.cfg.delegation_ttl_ms
                ))
            }
            Some(_) if !self.root_ends_recall(now, gen) => Err(format!(
                "this node cannot end generation {gen} now (it is not a usable root)"
            )),
            Some(g) => {
                let known = g.sealed_tail.as_ref().map_or(g.cursor, |t| t.end.max(g.cursor));
                let cursor = g.cursor;
                let forced = force && heard.is_some();
                // The audit of `--force`: who was heard from, and when.
                let (heard_node, heard_role, heard_ms) = heard.unwrap_or((0, "", 0));
                tracing::warn!(
                    node = self.me(),
                    gen,
                    cursor,
                    known,
                    forced,
                    heard_node,
                    heard_role,
                    heard_ms,
                    "the operator abandoned the group of a delegation{}: ending it at the root's cursor; rows only its group held are lost",
                    if forced {
                        " with --force, although this root heard from that node within delegation_ttl_ms"
                    } else {
                        ""
                    }
                );
                self.end_generation(now, gen, Some(known), replica, out);
                if self.dl.gens.get(&gen).is_some_and(|g| g.ended) {
                    self.stats.groups_abandoned += 1;
                    if forced {
                        self.stats.groups_abandoned_forced += 1;
                    }
                    Ok(ControlOk::Text(format!(
                        "generation {gen} ended at index {cursor}; rows past it that only its group held are lost"
                    )))
                } else {
                    Err(format!(
                        "could not journal the end of generation {gen}; try again"
                    ))
                }
            }
        };
        out.push(Action::ControlDone { op, result });
    }

    /// When this root last heard from `node`: a message from it, or a
    /// successful exchange on its link (the driver's directory, which forgets
    /// it once the link drops; this table does not).
    fn link_heard(&self, node: NodeId) -> Option<WaitMs> {
        self.peer_heard
            .get(&node)
            .copied()
            .max(self.links.get(&node).and_then(|l| l.last_seen))
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

    /// Plan 43 C2e follow-up (`repair abandon-group` without `--force`): the
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
            .filter_map(|(node, role, ago)| ago.map(|a| (node, role, a)))
            .filter(|(_, _, a)| *a < self.cfg.delegation_ttl_ms)
            .min_by_key(|(_, _, a)| *a)
    }

    /// End `gen` here and now: the `RecallCut` record journaled (the table
    /// row changes with it), later stream records of it refused, every
    /// parked execution waiting on it released.
    ///
    /// `abandoned`: the operator gave the generation's group up
    /// (`constellation repair abandon-group --gen`): the end is journaled
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
        // Whatever ends a generation passed a closed slot (an expiry, a
        // recall); closed here too, for every other way in.
        self.dw.close(gen);
        let dir = g.dir;
        let cursor = g.cursor;
        let drained = matches!(g.recall, RecallPhase::Drained(_));
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
        let own = self.dl.mine.contains_key(&gen) && !replica.delegate_stranded(gen);
        let through = if own {
            cursor.max(replica.stream_applied(gen))
        } else {
            cursor
        };
        let _class = self.journal_class();
        let mut records = vec![LogRecord::RecallCut { dir, gen, through }];
        if let Some(through_known) = abandoned {
            records.push(LogRecord::GroupAbandoned {
                epoch: 0,
                gen,
                through_known,
            });
        }
        if let Err(error) = replica.apply_records_journaled(&records, None) {
            tracing::warn!(node = self.me(), gen, %error, "could not journal the recall record");
            self.arm_gen_expiry(now.plus(500), gen, out);
            return;
        }
        replica.void_stream(gen, through);
        replica.note_void_cut(gen, through);
        // What this root holds of the generation's replies (its own
        // forwarded ops' shadows and hints) is stranded like on any
        // other replica applying the `Recall` (long-delegated seed 72780).
        match replica.strand_recalled_speculation(gen) {
            Ok(s) if s.any() => {
                tracing::info!(
                    node = self.me(),
                    gen,
                    shadows = s.shadows,
                    hints = s.hints,
                    "the root's own speculation of an ended generation stranded; replaying"
                );
                self.stats.speculation_rolled_back += (s.shadows + s.hints) as u64;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(node = self.me(), gen, %error, "could not strand the root's speculation of an ended generation");
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
            self.lock_on_generation_outwaited(now, dir);
        }
        tracing::info!(
            node = self.me(),
            gen,
            dir,
            cursor,
            "delegation generation ended"
        );
        self.mark_ended(now, gen, replica, out);
        // The record is journaled here, not applied from a segment: a
        // generation this node held itself (inherited by a successor
        // that was the delegate) ends on the delegate side too.
        if self.dl.mine.contains_key(&gen) {
            self.drop_delegate_state(now, gen, replica, out);
        }
    }

    fn mark_ended(&mut self, now: Now, gen: u64, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(g) = self.dl.gens.get_mut(&gen) else {
            return;
        };
        let first = !g.ended;
        g.ended = true;
        g.waiting_for_group = false;
        g.waiting_since = None;
        g.sealed_tail = None;
        g.seal_reqs.clear();
        self.dw.close(gen);
        if first {
            let due = self.deleg_forget_due(now, gen);
            self.dl.forget_at.insert((due, gen));
        }
        self.dl.answered_granting.remove(&gen);
        self.dl.floor_sent.remove(&gen);
        let g = self.dl.gens.get_mut(&gen).expect("present");
        let expiry = g.expiry.take();
        let recall_req = g.recall_req.take();
        let controls = std::mem::take(&mut g.controls);
        if let Some(t) = expiry {
            self.cancel_timer(t, out);
        }
        if let Some(req) = recall_req {
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
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let usable = self.root_ends_recall(now, gen) && self.cfg.delegation;
        let refuse = |out: &mut Vec<Action>, through: u64| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::DelegateStreamAck {
                    req,
                    gen,
                    round,
                    through,
                    refused: true,
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
            refuse(out, g.cursor);
            return;
        }
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.delegate_heard = Some(now.wait);
        }
        self.deleg_live_by_stream(now, gen);
        let g = self.dl.gens.get(&gen).expect("present");
        tracing::debug!(
            node = self.me(),
            from,
            gen,
            ?req,
            first = txs.first().map(|t| t.idx).unwrap_or(0),
            last = txs.last().map(|t| t.idx).unwrap_or(0),
            cursor = g.cursor,
            "delegate stream batch received"
        );
        let mut cursor = g.cursor;
        let mut appended = 0u64;
        let batch_last = txs.last().map(|t| t.idx).unwrap_or(0);
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
                    gen,
                    idx = tx.idx,
                    deps = ?tx.deps,
                    "delegate batch refused: a dependency was lost with its generation"
                );
                break;
            }
            if !replica.reaches_streams(&tx.deps) {
                self.stats.deleg_deps_unsatisfied_at_append += 1;
                tracing::debug!(
                    node = self.me(),
                    gen,
                    idx = tx.idx,
                    deps = ?tx.deps,
                    applied = ?replica.applied_position(),
                    "delegate batch refused: the root lacks its deps"
                );
                break;
            }
            match replica.apply_delegate_tx(&tx.records, tx.rid, gen, tx.idx, tx.deps) {
                Ok(_) => {
                    replica.note_foreign_executed(&tx.records);
                    cursor = tx.idx;
                    appended += 1;
                    if self.cfg.placement {
                        // The op's origin, not the delegate that executed
                        // it: a forwarded op counts for its requester.
                        let origin = tx.rid.map(|r| r.node).unwrap_or(from);
                        let keys = TouchSet::from_records(tx.records.iter());
                        let dirs = Core::dirs_of_keys(&keys, replica);
                        self.place_note(origin, dirs);
                    }
                }
                Err(error) => {
                    tracing::warn!(node = self.me(), gen, idx = tx.idx, %error, "could not append a delegate transaction");
                    break;
                }
            }
        }
        if let Some(g) = self.dl.gens.get_mut(&gen) {
            g.cursor = cursor;
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
            // The root's own ops forwarded to this delegate may await
            // the log for exactly these completions, or be parked on a
            // recall of it.
            self.answer_awaiting_log(now, replica, out);
            self.release_exec_parks_completed(now, replica, out);
            self.lease.touch(now);
            self.nudge(now, out);
        }
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::DelegateStreamAck {
                req,
                gen,
                round,
                through: cursor,
                refused: false,
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
        range: Range,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let fail = |out: &mut Vec<Action>, msg: String| {
            out.push(Action::ControlDone {
                op,
                result: Err(msg),
            });
        };
        // ADR-38: a directory split into ranges has no strict listing (one
        // sequencer answers it, whose position does not cover the other
        // ranges' streams), so none is split under `cto=strict`.
        if range.bits > 0 && self.cfg.strict_mounts {
            return fail(
                out,
                "a directory cannot be split into hash ranges under --cto strict \
                 (delegate it whole instead)"
                    .into(),
            );
        }
        match self.delegate_dir(now, dir, node, DelegKind::Manual, range, replica, out) {
            Ok(gen) => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Text(format!(
                    "delegated dir {dir} to node {node} (gen {gen})"
                ))),
            }),
            Err(msg) => fail(out, msg),
        }
    }

    /// Delegate `dir` to `node` as `kind`: the `Delegate` record and the
    /// root's generation state. The checks every path shares (an
    /// operator's control, the placement, a designation, a
    /// re-delegation after a cross-subtree op).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delegate_dir(
        &mut self,
        now: Now,
        dir: Ino,
        node: NodeId,
        kind: DelegKind,
        range: Range,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<u64, String> {
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
        if node == self.me() {
            return Err("cannot delegate to the root itself".into());
        }
        // The delegate is another node: a root that still counts itself
        // alone starts the kernel drain now, so the first renewal (and a
        // designation) waits for it with the rest of the quarantine.
        self.note_foreign(now, replica, out);
        let table = replica.delegation_table();
        if let Some(d) = table.covering(replica.namespace(), dir) {
            return Err(format!(
                "directory {dir} is under delegation {} (gen {})",
                d.dir, d.gen
            ));
        }
        if range.is_whole() {
            if table.is_split(dir) {
                return Err(format!("directory {dir} is split into ranges"));
            }
            // A delegation on a descendant of `dir` would be shadowed.
            if table
                .iter()
                .any(|d| d.dir != dir && is_under(replica.namespace(), d.dir, dir))
            {
                return Err(format!("a directory under {dir} is already delegated"));
            }
            if kind != DelegKind::Designated
                && table
                    .iter()
                    .any(|d| d.node == node && !d.designated && d.range.is_whole())
            {
                return Err(format!("node {node} already holds a delegation"));
            }
        } else {
            // Plan 30 §M12: one split depth per directory; a range is
            // delegated once; never over a designation.
            if kind == DelegKind::Designated {
                return Err("a designation covers a whole directory".into());
            }
            let ranges = table.ranges_of(dir);
            if let Some(d) = ranges.iter().find(|d| d.range.is_whole()) {
                return Err(format!(
                    "directory {dir} is wholly delegated (gen {})",
                    d.gen
                ));
            }
            if let Some(d) = ranges.iter().find(|d| d.range.bits != range.bits) {
                return Err(format!(
                    "directory {dir} is split {} ways, not {}",
                    1u64 << d.range.bits,
                    1u64 << range.bits
                ));
            }
            if let Some(d) = ranges.iter().find(|d| d.range == range) {
                return Err(format!(
                    "range {} of {dir} is delegated (gen {})",
                    range.label(),
                    d.gen
                ));
            }
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
        if kind == DelegKind::Designated {
            if !read_grants.grants.is_empty() {
                self.dl.designating.insert(dir, node);
                let n = read_grants.grants.len();
                self.start_recalls(now, read_grants, out);
                return Err(format!(
                    "{n} read delegations under directory {dir} are being recalled"
                ));
            }
            if let Some(q) = quarantine {
                self.dl.designating.insert(dir, node);
                return Err(format!(
                    "read delegations granted before this tenure may be honoured for {} ms",
                    q.since(now.wait)
                ));
            }
            self.dl.designating.remove(&dir);
        }
        // Above every generation the log ever named (a predecessor's
        // ended ones included): a generation is a stream's identity.
        let highest = table.max_gen();
        let gen = replica
            .next_delegation_gen(highest + 1)
            .map_err(|e| e.to_string())?;
        let designated = kind == DelegKind::Designated;
        let _class = self.journal_class();
        replica
            .apply_records_journaled(
                &[LogRecord::Delegate {
                    dir,
                    node,
                    gen,
                    designated,
                    range: (range.bits, range.idx),
                }],
                None,
            )
            .map_err(|e| e.to_string())?;
        let read_recalls = self
            .start_recalls(now, read_grants, out)
            .map(|(ids, _)| ids)
            .unwrap_or_default();
        // A first renewal that waits out the quarantine must not find the
        // generation reclaimed for its silence meanwhile.
        let first_until = quarantine
            .map_or(now.wait, |q| q.max(now.wait))
            .plus(self.reclaim_horizon_ms());
        self.dw
            .insert(gen, node, first_until, first_until, !designated);
        self.dl.gens.insert(
            gen,
            GenState {
                dir,
                node,
                range,
                kind,
                group: GroupState::default(),
                redelegate: false,
                granted: now.wait,
                below_since: None,
                seal_reqs: BTreeMap::new(),
                sealed_tail: None,
                waiting_for_group: false,
                waiting_warned: None,
                waiting_since: None,
                member_notes: BTreeMap::new(),
                delegate_heard: None,
                cursor: 0,
                recall: RecallPhase::None,
                recall_before_seal: RecallPhase::None,
                ended: false,
                expiry: None,
                recall_req: None,
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
            DelegKind::Manual => {}
        }
        self.lease.touch(now);
        self.nudge(now, out);
        tracing::info!(
            node = self.me(),
            dir,
            delegate = node,
            gen,
            ?kind,
            "delegated"
        );
        Ok(gen)
    }

    /// How long the root honours a grant it cannot see renewed: the
    /// grant's ttl plus the margin. The read delegations a delegate
    /// grants under it (phase 2b, M8) need no extra term: each is capped
    /// by the delegate's own `until` less the margin, so every one of
    /// them lapses before the grant does.
    pub(crate) fn reclaim_horizon_ms(&self) -> u64 {
        self.cfg.delegation_ttl_ms + self.cfg.expiry_margin_ms
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
    pub(crate) fn under_pending_designation(&self, ino: Ino, replica: &dyn Replica) -> bool {
        !self.dl.designating.is_empty()
            && self
                .dl
                .designating
                .keys()
                .any(|dir| is_under(replica.namespace(), ino, *dir))
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
            gen,
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
                Some(d) if d.designated && d.node == node => continue,
                Some(d) if !d.designated => {
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
                if !d.designated && self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                    self.start_recall(now, d.gen, out);
                }
                continue;
            }
            if table.is_split(dir) {
                // Plan 30 §M12: the ranges end first; the designation
                // follows on the next sync.
                for d in table.ranges_of(dir) {
                    if self.dl.gens.get(&d.gen).is_some_and(|g| !g.ended) {
                        self.start_recall(now, d.gen, out);
                    }
                }
                continue;
            }
            if let Err(why) = self.delegate_dir(
                now,
                dir,
                node,
                DelegKind::Designated,
                Range::WHOLE,
                replica,
                out,
            ) {
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
                d.designated
                    && !entries
                        .iter()
                        .any(|(dir, node)| *dir == d.dir && *node == d.node)
            })
            .map(|d| d.gen)
            .collect();
        for gen in released {
            if self.dl.gens.get(&gen).is_some_and(|g| !g.ended) {
                tracing::info!(node = self.me(), gen, "designation released: recalling");
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
        let entries = table.ranges_of(dir);
        // A designation is the operator's offline authority (plans
        // 03–05): only `online` releases it. Recalling it here would just
        // see it re-delegated by the next designation sync, after
        // bouncing the designee's in-flight ops.
        if entries.iter().any(|d| d.designated)
            || entries.iter().any(|d| {
                self.dl
                    .gens
                    .get(&d.gen)
                    .is_some_and(|g| g.kind == DelegKind::Designated)
            })
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
        // Plan 30 §M12: every generation of the directory (one whole, or
        // its ranges); the control is answered when the last one ended.
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
            self.start_recall(now, *g, out);
        }
        self.dl
            .gens
            .get_mut(&gen)
            .expect("present")
            .controls
            .push(op);
        self.start_recall(now, gen, out);
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
        self.dl.cert_set = false;
        self.dl.forget_at.clear();
        self.dl.forgot = false;
        for (_, g) in std::mem::take(&mut self.dl.gens) {
            if let Some(t) = g.expiry {
                self.cancel_timer(t, out);
            }
            if let Some(req) = g.recall_req {
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
}

/// Whether `dir` is `ancestor` or under it.
fn is_under(
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
