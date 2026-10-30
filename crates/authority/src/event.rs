//! Everything that can happen *to* the core: the input side of
//! `Core::handle(now, event, replica) -> Vec<Action>`.
//!
//! An event is a fact the IO driver observed — a FUSE thread submitted an
//! op, a peer's message arrived, an S3 request completed, a timer fired —
//! never a request for the core to wait on something. Results correlate
//! with the action that asked for them through the [`OpId`] the core
//! minted (see [`crate::ids`]); the core keeps, per outstanding id, what
//! it was for, so a result that arrives after the core moved on (a round
//! it abandoned, a forward already answered another way) is recognised as
//! stale and dropped instead of being misread as the answer to something
//! else.

use crate::ids::{Epoch, HonourMs, NodeId, OpId, Seq, TimerId, WaitMs};
use constellation_fs_core::Ino;
use constellation_meta::locks::LockTag;
use constellation_meta::{BackupTx, DelegateTx, MutateOp, MutateOutcome, OwnChunks, Position, Rid};
use constellation_store_s3::heartbeat::Promise;
use constellation_store_s3::inbox::InboxBatch;
use constellation_store_s3::{Lease, LeaseTag};

/// One input to the core.
#[derive(Debug)]
pub enum Event {
    /// A local client (a FUSE thread, or a system op such as retention
    /// pruning) submitted a mutation. `rid` is allocated by the caller
    /// once and kept across every retry (plan 30 §M2), which is what
    /// makes the reply (`Action::Reply`) addressable without a channel.
    Submit {
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        /// Plan 30 §M14 phase 2: the fencing token — the cluster-lock
        /// grants the op's issuer held (empty: none). Carried on every
        /// forward, inbox batch and replay of the op; every executor
        /// refuses it once a named grant is no longer live.
        tag: LockTag,
    },
    /// `Submit` of a rid this node executed but never acknowledged: the
    /// FUSE fast path's acknowledgement wait ended with the lease lost
    /// (`DurableWait::Lost`) or ran out (`TimedOut`), and the write comes
    /// back through the core under its rid. The core marks the rid in
    /// doubt and submits it in one step, so the op can never run as one
    /// never sent: it reads `completed` before it executes, and a copy of
    /// the rid already live here (the deposition's queue entry) is the
    /// same op, not a second one.
    SubmitInDoubt {
        rid: Rid,
        op: MutateOp,
        policy: Policy,
        tag: LockTag,
    },
    /// A peer's message was delivered.
    Peer { from: NodeId, msg: PeerMsg },
    /// A peer request the core sent (`Action::Send` of a `PeerMsg`
    /// carrying a `req`) failed at the transport: no dial, connection
    /// lost, or the driver's own request timeout. The core treats it as
    /// it treats its own forward timeout: the request is in doubt, not
    /// refused. `outage` is M13's reachability evidence: the failure was
    /// a dial or transport error with no open connection to the peer
    /// left, not a slow reply on a live one.
    PeerFailed { req: OpId, to: NodeId, outage: bool },
    /// An S3 request completed (`Action::S3`).
    S3 { op: OpId, result: S3Result },
    /// A timer the core set fired.
    Timer { id: TimerId },
    /// `Action::UploadDirtyChunks` finished.
    UploadsDone { op: OpId, result: UploadResult },
    /// `Action::Publish` / `Action::FollowHead` finished (a commit
    /// landed, or it was deferred or failed). The core only clears its
    /// "publishing" flag on it.
    PublishDone { op: OpId, ok: bool },
    /// `Action::ConflictCopy` finished: the copy exists (`ok`), or could
    /// not be made yet (no holder took the steps).
    ConflictCopyDone { queue_seq: u64, ok: bool },
    /// `Action::RebuildReplica` finished (a deposition recovery with
    /// uncaptured journal rows).
    RebuildDone { op: OpId, ok: bool },
    /// Plan 30 §M14: `Action::LockFlush` finished (`ok`: the file's dirty
    /// data and its manifest are acknowledged).
    LockFlushed {
        ino: Ino,
        grant: constellation_meta::locks::GrantId,
        ok: bool,
    },
    /// Plan 30 §M14: `Action::PersistLockHorizon { until }` finished:
    /// `durable` is the horizon now on disk (at least `until`), `None`
    /// if the write failed; both are on this node's wait clock (plan 43
    /// C7a: internal, never serialised).
    LockHorizonPersisted {
        until: WaitMs,
        durable: Option<WaitMs>,
    },
    /// `Action::SyncDelegStream { gen, through }` finished: `ok` the sync
    /// succeeded (the rows through `through` are on the disk).
    DelegStreamSynced { gen: u64, through: u64, ok: bool },
    /// The membership poll read the write-eligible roster (M13's inbox
    /// polls it; M9 picks backups from it).
    Roster { write_eligible: Vec<NodeId> },
    /// Plan 30 §M10: the filesystem's `epoch_slack` as `meta.json` now
    /// says (the driver re-reads it with the roster).
    Slack { epoch_slack: u32 },
    /// The P2P directory's view of the peers (M13's reachability rule,
    /// the holder's inbox poll set, M9's backup liveness). Sent whole,
    /// whenever the driver refreshes it.
    Peers { links: Vec<PeerLink> },
    /// EC2 campaign 8 A-1: this node's own S3 path, as the driver sees it
    /// (sent with every `Peers` refresh). `stalled`: no S3 request of this
    /// node has completed for the driver's stall window
    /// (`CONSTELLATION_S3_STALL_MS`, default 6 s — longer than the 5 s
    /// registry poll, so a working path always shows a completion inside
    /// it). `peers_reach_s3`, asked of the live peers (`PingS3`) while
    /// stalled: `Some(true)` some peer reaches S3 (the outage is this
    /// node's own), `Some(false)` peers answered and none does (a bucket
    /// outage), `None` nobody answered (or not stalled).
    OwnS3 {
        stalled: bool,
        peers_reach_s3: Option<bool>,
    },
    /// M7: the driver dropped a subscriber of this holder's log stream
    /// (its bounded send buffer overflowed, or its connection went away):
    /// stop streaming to it. The subscriber falls back to S3 and
    /// resubscribes on its own.
    SubscriberGone { node: NodeId, req: OpId },
    /// The lease holder `from`'s off-core liveness heartbeat (the driver
    /// sends it while its core is responsive; `Payload::HolderAlive`),
    /// heard at `at` — its arrival on this node's wait clock, which may be
    /// well before this node's core gets to it. `listed: false`: the
    /// holder dropped this node's `candidacy`
    /// (`PeerMsg::BackupAppend::candidacy`) from its backups.
    /// `seq`, `sent`, `holder_expires` and `guard` are the beat's number,
    /// the holder's **wait** clock when it sent it, the end of the lease
    /// it beats for on that clock and what it waits out that its lease
    /// does not cap, on it too; `clock` is the id of that clock
    /// (`constellation_net::Beat`; plan 43 C7-H1: values of one holder
    /// clock, opaque on this node, never compared with its own readings:
    /// `Core::beat_cap` keeps them per clock id). The seal watch places the
    /// lease's end on this node's clock from them too (`Core::known_end`;
    /// plan 43 C7b, row 24: no wall expiry crosses nodes). `seq` 0: not a
    /// numbered beat (the driver handing the latest arrival over,
    /// `Driver::hand_heard`): `clock`, `sent` and `holder_expires` are then
    /// those of the latest beat or append it stands for (0: none).
    HolderAlive {
        from: NodeId,
        epoch: Epoch,
        candidacy: u64,
        listed: bool,
        at: WaitMs,
        seq: u64,
        clock: u64,
        sent: i64,
        holder_expires: i64,
        /// Plan 43 C7-F F-a: the latest instant on the holder's wait
        /// clock at which grants it waits out but its lease does not cap
        /// may still be honoured (its takeover floor, its lock and read
        /// quarantines, its subtree graces); 0: none, or an unnumbered
        /// arrival; `i64::MAX`: unknown (its takeover gate is pending),
        /// which disables the cap (`Core::beat_cap`).
        guard: i64,
    },
    /// This node's own runtime was held up from `from` until `until` (its
    /// lag probe woke at least an eighth of `backup_takeover_ms` late; a
    /// lapse still in progress is handed again as it grows, with the same
    /// `from`): what it stamped as an arrival meanwhile it stamped late,
    /// and what it has not stamped yet may already be here. A silence
    /// watch (the seal watch, `ack=s3`'s) counts silence only from the
    /// end of the latest lapse (`Core::attentive_since`).
    OwnLag { from: WaitMs, until: WaitMs },
    /// The off-core delegation renewal lane (`crate::deleg_lane`) sent a
    /// renewal of `gen` (stream round `round`) to root `to` as request `req`
    /// at `sent` (this node's honour clock, read before the send), because the
    /// core was stuck in a step. Sent on the urgent lane ahead of the
    /// answer, so the answer is known when it arrives
    /// ([`Core::on_deleg_renew_sent_off_core`]).
    DelegRenewSentOffCore {
        gen: u64,
        round: u64,
        to: NodeId,
        req: OpId,
        sent: HonourMs,
    },
    /// The root's P2P service answered delegate `from`'s renewal of `gen`
    /// off the core (plan 41 R2, `crate::deleg_lane::answer_root`): the
    /// shared window table is extended already, by a grant of `ttl_ms`. The
    /// core does the bookkeeping the answer skipped ([`Core::on_deleg_renew_answered_off_core`]):
    /// the statistics, the lease's activity stamp, the expiry timer, the
    /// lease renewal a short grant calls for. Sent on the urgent lane,
    /// after the request it answers (which the core still handles, and
    /// answers in full).
    DelegRenewAnsweredOffCore { gen: u64, from: NodeId, ttl_ms: u64 },
    /// Plan 41 D3: an off-core lane read the lease object for the tenure of
    /// `epoch` while this core was stuck, the request sent at `sent` (this
    /// node's honour clock, read before the send), and read `read`
    /// ([`crate::root_probe`]). The core treats it as a `LeaseGet` of its own
    /// ([`Core::on_root_probed`]): a tenure seen live is proven live as of
    /// `sent`, one seen taken over is deposed here. Urgent lane.
    RootProbed {
        epoch: Epoch,
        sent: HonourMs,
        read: crate::root_probe::LeaseRead,
    },
    /// Plan 41 L2, the lock lane extended `renewed` of this node's own
    /// grants at its own table while the core was busy
    /// ([`Core::on_lane_own`]): statistics, the mirror, the lease renewal a
    /// short window calls for. Urgent lane.
    LaneOwn { renewed: u32 },
    /// Plan 41 L4: the owner `from` answered the lane's `LockRenew` sent at
    /// `sent` (this node's honour clock): the lane applied every `Ok`
    /// already; the core applies every result as it applies its own
    /// renewal's (`Lost`, `NotOwner`, the release of a recalled grant)
    /// ([`Core::on_lane_renewed`]). Urgent lane.
    LaneRenewed {
        from: NodeId,
        sent: HonourMs,
        results: Vec<(
            constellation_fs_core::Ino,
            constellation_meta::locks::GrantId,
            LockRenewResult,
        )>,
    },
    /// The lane's `LockRenew` to `to` was not answered in time (or failed
    /// at the transport): the core learns the lock owner again, as after
    /// its own renewal's timeout ([`Core::on_lane_unanswered`]). Urgent
    /// lane.
    LaneUnanswered { to: NodeId },
    /// The off-core heartbeat thread did not run from `from` to `until`
    /// (this node's wait clock): the whole process was frozen, and nothing in it
    /// could renew a delegation (`Core::on_own_freeze`). The core uses it
    /// only to say, when it gives a grant up, whether the freeze alone
    /// explains the loss (`Stats::deleg_lapses_frozen`).
    OwnFreeze { from: WaitMs, until: WaitMs },
    /// The holder's side of the same heartbeat: backup `from` answered
    /// one sent at `at` (its process and link are up, whatever its core
    /// is doing). A backup known alive is given `backup_slow_max_ms`, not
    /// `backup_ack_timeout_ms`, to make acknowledgement progress.
    BackupAlive { from: NodeId, at: WaitMs },
    /// Local writes the driver's fast path executed without the core
    /// (the FUSE lease view's `admit`): the wait-clock time of the last one, for the
    /// idle-release clock, and their rid seqs, for the ack tracker
    /// (`acked_through`).
    Activity {
        last_write: WaitMs,
        acked_seqs: Vec<u64>,
    },
    /// A control-plane request from the daemon (unmount, `fsync`,
    /// snapshot publish, GC's tail-to-head, a placement claim offer, the
    /// continuation-epoch machinery).
    Control { op: OpId, req: Control },
}

/// What a submitted op may do to get executed (`Event::Submit`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// A FUSE thread's mutation: forward with retries and a redirect,
    /// M13's inbox when there is no P2P path, then the lease path until
    /// the deadline.
    Client,
    /// A system op (retention pruning, conflict-copy steps): one forward
    /// to the known holder, then at most one lease acquisition — never a
    /// wait for a live holder to let go — and no idle-clock touch.
    System,
    /// A best-effort batch (read-time atime): one forward to the cached
    /// holder if one is known, nothing else.
    BestEffort,
}

/// How an upload pass ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadResult {
    /// Everything shippable is in S3; `held` is M4's count of journal
    /// transactions the pass had to hold back behind an unrecoverable
    /// chunk (0 when nothing is held).
    Done { held: u64 },
    /// A real upload failure (S3 unreachable): the round fails and
    /// nothing ships.
    Failed(String),
    /// The driver decided this round must not run (a continuation epoch
    /// is frozen, a test-only fault point): finish it quietly.
    Skip,
}

/// One peer as the driver's directory knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerLink {
    pub node: NodeId,
    /// The last RPC to it succeeded, or gossip has it as a neighbor.
    pub connected: bool,
    /// When it was last heard from (any successful exchange), on this
    /// node's wait clock.
    pub last_seen: Option<WaitMs>,
    /// Plan 30 §M9: the measured round trip to it, when known. Backup
    /// candidates are chosen by this against the RTT budget, never by
    /// assuming a LAN.
    pub rtt_ms: Option<u64>,
    /// Plan 30 §M9: since when the link has been up without a break
    /// (the plan prefers the peer connected the longest).
    pub since: Option<WaitMs>,
}

/// Control-plane requests (`SyncRequest`'s non-mutation arms).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// Run a round soon (`SyncRequest::Nudge`).
    Nudge,
    /// Plan 30 §M9: the FUSE fast path journaled a row under a
    /// durability gate; the holder's backups get their append now (the
    /// event itself is a no-op: every event ends in
    /// `backup_after_event`).
    Journaled,
    /// `--fsync-mode s3` barrier: ship this node's journal, then answer.
    Barrier { ino: Option<Ino> },
    /// Ship everything and publish a commit now (snapshots).
    PublishNow,
    /// Tail to head without touching the lease (in-daemon GC).
    TailToHead,
    /// Take the lease if it is free (a snapshot barrier, a blocked FUSE
    /// gate): answered with `ControlOk::Lease`.
    Acquire,
    /// Placement offered this node the lease (`SyncRequest::ClaimOffer`).
    ClaimOffer { epoch: Epoch },
    /// Run the deposition recovery now, if this node was deposed, and
    /// report what it did (`reintegrate`).
    Reintegrate,
    /// Plan 43 C1-3 (§5.8, Q1): the operator accepts that the group of
    /// `epoch` (generation `gen` 0, the root group) is gone for good: the
    /// claim that waits for its tail proceeds without it; or, with `gen`
    /// != 0 (and `epoch` 0), that of a delegation generation: the root,
    /// which cannot end it without the tail (`waiting_for_group`), ends it
    /// at its cursor and journals `GroupAbandoned` (C2e)
    /// (`constellation repair abandon-group`). Answered with
    /// `ControlOk::Text`; refused unless a claim, or a generation, is
    /// waiting for that group. A generation's is also refused, unless
    /// `force`, while this root has heard from its delegate or a strict
    /// member of its group within `delegation_ttl_ms`: such a node may not be
    /// gone.
    AbandonGroup { epoch: Epoch, gen: u64, force: bool },
    /// Flush the journal, publish if the holder, release the lease, and
    /// answer — without stopping (`leave`).
    Flush,
    /// Final flush + release on unmount (`Shipper::shutdown_all`); the
    /// core stops afterwards.
    Shutdown,
    /// Plan 30 §M8: a `cto=strict` open or lookup on this node needs a
    /// ReadIndex (answered with `ControlOk::ReadIndex`).
    ReadIndex {
        ino: Ino,
        dir: bool,
        name: Option<String>,
        /// Read-ahead wanted with the answer.
        ahead: ReadAhead,
    },
    /// Plan 30 §M8: a FUSE write this node executed as the sequencer
    /// touched `inos`, which carry read delegations: recall them (or
    /// wait them out), then answer `Done` — the write returns only then.
    Recall { inos: Vec<Ino> },
    /// Plan 30 §M14: a local lock on `ino` needs a cross-node grant in
    /// `mode` (answered with `ControlOk::Lock`; a blocking request waits
    /// at the owner, a non-blocking one is answered `WouldBlock`).
    Lock {
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        blocking: bool,
        /// How long the application has been waiting for this lock (a
        /// duration: the engine's rounds of one lock call, the clocks of
        /// nodes never compared). The owner queues the request from there,
        /// so a client that lost a round keeps its place.
        waited_ms: u64,
    },
    /// Plan 30 §M14: the last local lock under a recalled grant on `ino`
    /// left; the grant is released (`Done` at once).
    LockIdle { ino: Ino },
    /// Plan 30 §M14 phase 2: the last tagged mutation in flight under
    /// `ino`'s recalled grant was answered; a release waiting for it
    /// goes on now (`Done` at once).
    LockReleaseWake { ino: Ino },
    /// Plan 30 §M14: `getlk` — is a conflicting grant held elsewhere?
    LockTest {
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
    },
    /// Continuation epochs (DESIGN.md §5.3): the driver's epoch machine
    /// changed state. `open`: a promise or active epoch blocks S3
    /// takeover; `active`: writes are local (no S3 CAS); `frozen`: a
    /// member is missing, writes refused; `flushing`: the epoch closed
    /// and its journal must reach S3 before the lease is let go; `base`:
    /// the log position the epoch started from.
    Epoch {
        open: bool,
        active: bool,
        frozen: bool,
        flushing: bool,
        base: Seq,
        /// Plan 30 §M10's claim rule (enforced since M9): the epoch's
        /// members, for whether a lease may be carried into it.
        members: Vec<NodeId>,
        /// Plan 30 §M10's claim resolution: the lease the epoch carries —
        /// the members' claim at the highest lease epoch, provided no
        /// member knows a later one and its policy may be carried — or
        /// none. Kept after the epoch closes (the flush re-claim of
        /// exactly this lease object needs no promise check).
        carrier: Option<Carrier>,
        /// Any member's lease claim below this epoch is stale: that
        /// member was taken over (it has not heard yet) and is deposed at
        /// activation.
        stale_below: Epoch,
    },
    /// Plan 30 §M10: this node's registry record is retired (admin
    /// `leave --node-id`): it never acquires, promises or acknowledges
    /// again.
    Retire,
    /// Plan 30 §M11: delegate `dir` to `node` (this node must hold the
    /// root lease; answered `Text`).
    Delegate {
        dir: Ino,
        node: NodeId,
        /// Plan 30 §M12: `(bits, idx)` — `(0, 0)` is the whole directory.
        range: (u8, u32),
    },
    /// Plan 30 §M11: recall the delegation on `dir` (drained, or outwaited
    /// by its grant's expiry; answered `Text` once the generation ended).
    Undelegate { dir: Ino },
    /// Plan 30 §M11 phase 2b: the live write designations `(dir,
    /// designee)`; the root delegates the new ones (designated) and
    /// recalls the released ones. Answered `Done` at once.
    SyncDesignations { entries: Vec<(Ino, NodeId)> },
    /// Plan 31 C8: how this node may take authority from now on — the
    /// engine profile's `LeaseMode::ForwardOnly` (`forward_only`), and a
    /// host suspension (`suspended`, from `Suspending` until `Resumed`).
    /// Answered `Done` at once; see [`crate::AuthorityMode`].
    Authority { forward_only: bool, suspended: bool },
}

/// Plan 30 §M10: the lease a continuation epoch carries, exactly as the
/// lease object read when the member claimed it (holder, epoch, expiry).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Carrier {
    pub node: NodeId,
    pub epoch: Epoch,
    pub expires_unix_ms: i64,
}

/// Messages between cores. The driver maps these to and from
/// `constellation_net::Payload`; the core never sees the wire format.
/// Variants for later milestones are declared now so their arrival needs
/// no interface change: the core answers them with nothing until the
/// milestone that implements them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMsg {
    /// Forward `op` to the holder (`Payload::MutateRequest`). `req` is the
    /// requester's correlation id, echoed by the reply; `rid` is the op's
    /// exactly-once identity, stable across retries.
    MutateRequest {
        req: OpId,
        rid: Rid,
        op: MutateOp,
        /// Plan 30 §M2 GC receipt: the requester has seen replies for
        /// every seq of its incarnation up to here.
        acked_through: u64,
        /// Plan 30 §M11: the position the requester has observed (its
        /// causal dependencies): an executor runs the op only once its
        /// replica holds everything it names.
        deps: Position,
        /// The log sequence the requester had applied when it sent the
        /// op (chunk close-stall-followup). `deps.seq` is no stand-in: it
        /// is what the requester *observed*, which can run ahead of what
        /// it applied. A reply whose `base` this covers is installed there
        /// at once, never `AwaitingLog`, so its `own_chunks` would go
        /// unread and the holder skips working them out
        /// (`Core::own_chunks_for`).
        applied: Seq,
        /// Plan 30 §M14 phase 2: the op's fencing token (`Event::Submit`).
        tag: LockTag,
    },
    /// The holder's answer. `base` is the first form of plan 30 §M6's
    /// position on replies: the log position the requester must have
    /// applied for the reply's records to be installed ahead of the log
    /// (the holder's last shipped sequence when it evaluated the op), or
    /// `None` when the holder's unshipped journal had already touched one
    /// of the op's keys, so that only the log can deliver the records in
    /// their order. The simulation found the case this guards: a shadow
    /// installed on a stale base, then the segment carrying both the
    /// earlier record and the op applied on top (`rename` makes it
    /// visible; the model's create/unlink-only namespace cannot).
    ///
    /// Plan 30 §M6: `position` is the state the holder evaluated the op
    /// against — its shipped-through log sequence plus its unshipped
    /// journal position (`constellation_meta::Position`) — on every
    /// outcome. `base` stays the install precondition for `Accepted`
    /// (and the hint of `Exists`); `position` is what the requester's
    /// `observed` watermark rises to when the reply's effects are not
    /// installed there, and what installed speculation covers up to.
    /// `Position::ZERO` on `Busy`/`NotHolder` (nothing observed).
    MutateReply {
        req: OpId,
        outcome: MutateOutcome,
        base: Option<Seq>,
        position: Position,
        /// Plan 30 §M11: the delegation generation that executed the op
        /// (0: the root). A shadow or hint installed from the reply
        /// strands when the log recalls it.
        gen: u64,
        /// Chunk close-stall-metered: whether the op's records wait for
        /// the requester's own pending chunks (a `back` close forwarded
        /// with them still uploading there), and whether anything but its
        /// upload brings them back to it (`Core::own_chunks_for`). Set on
        /// `Accepted`, and on a `Held` only while the acknowledgement
        /// itself waits for durability; `None` on every other answer.
        own_chunks: OwnChunks,
        /// Chunk metered-own-rows: which of the unshipped transactions
        /// through `position` are the requester's own, and what the rest
        /// wait for of its chunks (`constellation_meta::OwnRows`), so it
        /// waits, and uploads, only for what it lacks
        /// (`Core::settle_own_rows`). `None`: not worked out (nothing of
        /// the requester's pending there, a `Held`, a delegate's answer):
        /// everything through the position is owed.
        own_rows: Option<constellation_meta::OwnRows>,
        /// Plan 43 C1-2: the answer waited for a group or the archive
        /// (`AckClass::gated`): what it says is committed, so a shadow or
        /// hint the requester installs from it never rolls back at a
        /// takeover (C1-4). `false` on an answer that waited for nothing
        /// (a lone node's, or one released with the gate off) and on every
        /// non-answer (`Busy`, `NotHolder`, a `Held`).
        committed: bool,
    },
    /// "I want the lease" (`Payload::LeaseRequest`), sent to the holder.
    /// `epoch_applied`: `Some(applied)` asks for a continuation epoch's
    /// P2P-only hold transfer and carries the requester's applied
    /// sequence, which must be at the holder's head (nothing reaches S3
    /// during an epoch, so a successor behind the holder's log could
    /// never catch up before it executes: flex-crash seed 30702). `None`
    /// asks for the S3 handoff (flush, release, the requester claims).
    /// The holder declines a request for the other kind (seed 2236: an
    /// S3 release answered an epoch request, and the requester's local
    /// hold stood beside the next S3 holder).
    LeaseRequest {
        req: OpId,
        epoch_applied: Option<Seq>,
    },
    /// The holder's answer (`Payload::LeaseHandoff`): it flushed and
    /// released (`released: true`, and `head_seq` is its last shipped
    /// sequence, the requester's catch-up target), or it declined.
    LeaseHandoff {
        req: OpId,
        released: bool,
        epoch: Epoch,
        head_seq: Option<Seq>,
    },
    /// Gossip hint that a segment landed (`Payload::SegmentPublished`).
    /// Plan 30 §M7: a hint only — the log itself travels on direct
    /// streams ([`PeerMsg::LogStream`]) or through S3; gossip carries
    /// membership and digests.
    SegmentPublished {
        seq: Seq,
        epoch: Epoch,
    },
    /// M7: subscribe to the holder's log stream from `from` (the
    /// subscriber's next expected sequence). `req` names the subscription
    /// in every frame of it; the driver reports `Event::PeerFailed` with
    /// it when the stream cannot be opened or breaks.
    LogSubscribe {
        req: OpId,
        from: Seq,
    },
    /// M7: the subscriber lets subscription `req` go (it switched holders,
    /// took the lease itself, or found a gap).
    LogUnsubscribe {
        req: OpId,
    },
    /// M7: frame `n` (0, 1, 2, … per subscription) of subscription `req`:
    /// one segment the holder's cursor passed over — shipped by it, or
    /// read back from S3 — exactly the bytes of `log/<seq>` in S3, or a
    /// heartbeat (`segment: None`). `head` is the holder's highest
    /// applied-or-shipped sequence when it sent the frame, and `epoch` the
    /// epoch it holds. A subscriber applies segments through the tail
    /// path (`Core::apply_incoming`, fencing included) and never skips a
    /// sequence: a frame out of order breaks the stream, and a missing
    /// sequence is read from S3.
    LogStream {
        req: OpId,
        n: u64,
        epoch: Epoch,
        head: Seq,
        segment: Option<(Seq, Vec<u8>)>,
    },
    /// M7: the holder ends subscription `req`: it stopped holding
    /// (`refused: false`), or was never the holder (`refused: true`).
    LogStreamEnd {
        req: OpId,
        refused: bool,
    },
    /// Plan 30 §M8: a `cto=strict` open (`dir: false`) or lookup (`dir:
    /// true`, with the looked-up `name`) on a node that is not the
    /// sequencer asks it where the state it reads is: the answer's
    /// position covers every mutation of `ino` (its record; a
    /// directory's entries; the named entry and its target) acknowledged
    /// before the request arrived. The sequencer may grant a read
    /// delegation on `ino` with it.
    ReadIndex {
        req: OpId,
        ino: Ino,
        dir: bool,
        name: Option<String>,
        /// Read-ahead: a hint on this read, which only the sequencer
        /// answering for itself acts on.
        ahead: ReadAhead,
    },
    ReadIndexReply {
        req: OpId,
        outcome: ReadIndexOutcome,
    },
    /// Plan 30 §M8: the sequencer recalls read delegation `grant` on
    /// `ino` before acknowledging a mutation that touched it; the
    /// delegate stops honouring it, then acks.
    DelegationRecall {
        req: OpId,
        ino: Ino,
        grant: u64,
    },
    DelegationRecalled {
        req: OpId,
    },
    /// Plan 30 §M9: the holder streams its journal to a backup: whole
    /// transactions from journal seq `from` (the seq after what the
    /// backup last acknowledged), or none (a heartbeat). `through` is the
    /// holder's shipped-through journal seq (the backup may trim below
    /// it). `config_version` is the lease's as the holder knows it, so a
    /// backup can tell a stale holder's appends from the current one's.
    /// `candidacy` names the holder's bring-up of this backup (unique per
    /// holder and growing; a backup dropped and re-added gets a new one):
    /// a dismissal (`HolderAlive { listed: false }`) applies only to the
    /// candidacy it names. `sent_ms`, `expires_ms` and `clock` are what a
    /// beat carries (`constellation_net::Beat`): the holder's wait clock
    /// when it built the append, the end of the lease it holds at `epoch` on
    /// that clock (0: none) and the clock's id. A backup whose heartbeat lane
    /// is broken learns from them when to read the lease (`Core::known_end`;
    /// plan 43 C7b, row 24).
    BackupAppend {
        req: OpId,
        epoch: Epoch,
        holder: NodeId,
        config_version: u64,
        candidacy: u64,
        from: u64,
        txs: Vec<BackupTx>,
        through: u64,
        sent_ms: i64,
        expires_ms: i64,
        clock: u64,
    },
    /// Plan 30 §M9: the backup holds every row through `acked`
    /// (durably), or has `sealed` the epoch and will never acknowledge
    /// it again (the holder must reconfigure it out before it can
    /// acknowledge anything further — or is being taken over).
    BackupAck {
        req: OpId,
        epoch: Epoch,
        acked: u64,
        sealed: bool,
        /// Plan 43 C1-1: the backup's hold for this `(holder, epoch)` was
        /// created by this very append (its earlier tail, if any, was
        /// discarded or never held): `acked` is all it has, whatever the
        /// holder counted before. The holder restarts its count from it.
        fresh: bool,
    },
    /// Plan 30 §M9: pre-S3 streaming. The holder's backups hold `txs`
    /// (journal transactions of its tenure at `epoch`), evaluated against
    /// the log through `base`; a subscriber that has applied `base` may
    /// install them ahead of the log as `Streamed` speculation, which the
    /// segment carrying their rows retires. One-way; the log itself
    /// still arrives on the stream (`LogStream`) or from S3.
    StreamAhead {
        epoch: Epoch,
        base: Seq,
        txs: Vec<BackupTx>,
        /// Plan 43 C1-4: the rows are held by the holder's group (or the
        /// archive) — a holder streams only what its backups acknowledged —
        /// except an epoch hold's, which streams to its tip; a subscriber
        /// installs them as committed entries (not stranded by a takeover of
        /// their epoch) iff set. Appended field: mixed versions unsupported.
        committed: bool,
    },
    /// Plan 37 §8: the holder is about to be replaced by a successor on
    /// its own state dir (same node, lease and epoch): a backup of it at
    /// `epoch` counts no silence for `for_ms` (capped,
    /// `core::backup::BACKUP_HOLD_MAX_MS`). One-way.
    BackupHold {
        epoch: Epoch,
        for_ms: u64,
    },
    /// Plan 30 §M10: a would-be taker of the lease whose renewal key is
    /// `key` asks for a promise anchored to it ("I have watched that key
    /// run out; are you in no open epoch, and will you join none for a
    /// while?"; plan 43 C7b: sent only after the taker's own observation of
    /// the key ran out). The peer persists its gate, publishes a heartbeat
    /// naming the key (unless it is in an open epoch), and answers.
    PromiseRequest {
        req: OpId,
        key: constellation_store_s3::RenewalKey,
    },
    /// Plan 30 §M10: whether the promise was given (`granted: false`:
    /// refused — this node is in an open epoch, retired, or runs with no
    /// slack), and the epoch slack it runs with.
    PromiseReply {
        req: OpId,
        granted: bool,
        epoch_slack: u32,
    },
    /// Plan 30 §M11: a delegate streams its executed transactions of
    /// generation `gen` to the root, in stream order from index
    /// `from_idx` (`txs[0].idx`), one batch in flight per generation.
    /// Each carries the requester's `deps`. One way: the root answers
    /// with its own [`PeerMsg::DelegateStreamAck`] (the driver reports
    /// `PeerFailed` only for a send that did not reach the root).
    DelegateStream {
        req: OpId,
        gen: u64,
        /// The delegate's stream round (`DelegateState::stream_round`):
        /// echoed by every answer, which counts only in the round it
        /// answers.
        round: u64,
        txs: Vec<DelegateTx>,
        /// The delegate's live lock grants of `gen` on inodes no longer
        /// in its subtree (unlinked under their lock): the root installs
        /// them once it has applied the batch, which takes them out of
        /// the subtree in its replica too (`Core::deleg_leaving_grants`).
        leaving: Vec<constellation_meta::locks::GrantWire>,
        /// Plan 30 §M14: the delegate's outwait barriers on inodes no
        /// longer in its subtree, `(inode, as of)` on its clock: the root
        /// keeps them once it has applied the batch
        /// (`Core::deleg_leaving_barriers`).
        leaving_barriers: Vec<(Ino, i64)>,
    },
    /// Plan 30 §M11: the root appended the stream through `through`
    /// (cumulative: the delegate re-sends from there, whichever batch
    /// `req` names), or `refused` the batch `req` (the generation is
    /// unknown, ended, or not this node's): the delegate backs off.
    DelegateStreamAck {
        req: OpId,
        gen: u64,
        round: u64,
        through: u64,
        refused: bool,
    },
    /// Plan 30 §M11: a delegate renews its grant on `gen`; the root
    /// answers with the ttl (0: refused — the generation is ending or is
    /// not this delegate's). The delegate measures from its send. One
    /// way, as `DelegateStream`.
    DelegRenew {
        req: OpId,
        gen: u64,
        round: u64,
        /// Phase 2b: the delegate's backup peer, if it appends to one.
        backup: Option<NodeId>,
        /// Plan 30 §M14: the highest stream index the delegate executed
        /// under `gen` when it sent this (filled in after the event, see
        /// `Core::lock_fill_renew_heads`). A new root tenure floors its
        /// lock grants with every inherited generation's head: a release
        /// the previous tenure recorded may name a stream index the new
        /// root has not been re-streamed yet.
        stream_head: u64,
        /// When the delegate sent this, on its clock: `stream_head` is
        /// at least everything it executed before then. The root's lock
        /// cut is as of the earliest of these (`Core::lock_cut_here`).
        stream_head_at: i64,
        /// No renewal of `gen` has been granted to the delegate yet (it
        /// executes nothing until one is): a root that does not know
        /// whether its predecessor granted one, and may still be honouring
        /// that predecessor's read delegations under the subtree, answers
        /// it only once those are outwaited (`Core::on_deleg_renew`).
        first: bool,
    },
    /// A delegate's off-core lane renews its grant on `gen` while its
    /// core is stuck in a step (`crate::deleg_lane`). The root answers with
    /// the same `DelegRenewed`. It carries no backup and no stream head
    /// (the lane has neither: the core's are not known to be current, and
    /// a head needs the core's `settle_tagged_writes`), so the root
    /// extends the grant and learns nothing about the delegate's stream
    /// or backup from it.
    DelegRenewOffCore {
        req: OpId,
        gen: u64,
        round: u64,
    },
    /// The root's P2P service answered a delegation renewal off its core
    /// while the core was stuck (plan 41 R2): the RPC reply of `req`, as
    /// `Payload::DelegRenewedOffCore`. A grant of `ttl_ms` on `gen`, stream
    /// round `round`, with no lock content: the delegate's core applies it
    /// only as an extension of a grant it already serves, honoured from the
    /// request's own send (`Core::on_deleg_renewed_off_core`); the root's
    /// core still answers `req` in full, with `DelegRenewed`.
    DelegRenewedOffCore {
        req: OpId,
        gen: u64,
        round: u64,
        ttl_ms: u64,
    },
    DelegRenewed {
        req: OpId,
        gen: u64,
        round: u64,
        ttl_ms: u64,
        /// Plan 30 §M14: the root's lock grants under the subtree, handed
        /// over with the first renewal (restamped by the delegate).
        locks: Vec<constellation_meta::locks::GrantWire>,
        /// Plan 30 §M14: how much longer the root refuses new lock grants
        /// on the subtree (a grace: after a takeover of a released lease,
        /// an outwaited delegate, a restart quarantine) — grants of an
        /// earlier tenure it cannot hand over may still be honoured. A
        /// delegate that starts serving with this renewal refuses new
        /// grants (and accepts reclaims) as long, plus the margin.
        lock_grace_ms: u64,
        /// Plan 30 §M14: the lock floor of the subtree at the move (what
        /// its holders released at, as the root knew it): the delegate
        /// joins it into every grant under the subtree, so the move does
        /// not lose "the next holder reads what the previous one wrote".
        lock_floor: constellation_meta::Position,
        /// Plan 30 §M14: the root's cut of everything acknowledged
        /// anywhere, as of `lock_cut_at` on the root's clock (0: none) —
        /// what settles the delegate's outwait barriers
        /// (`Core::lock_take_cut`).
        lock_cut_at: i64,
        lock_cut: Box<constellation_meta::Position>,
        /// Plan 30 §M14: the latest outwait barrier the root has left
        /// under the subtree (0: none): the delegate's grants there wait
        /// for a cut as of it too.
        lock_barrier: i64,
        /// What is left, in ms on the root's clock at this send, of the
        /// record of each grant of `locks`: the delegate records
        /// `now + left + margin` from the receipt, never later than a
        /// restamp (`Core::lock_install_moved`); a grant not listed it
        /// restamps. A duration: no clock's absolute crosses nodes.
        lock_carry: Vec<(constellation_meta::locks::GrantId, u64)>,
    },
    /// Plan 30 §M11: the root recalls generation `gen` on `dir`; the
    /// delegate stops executing under it and answers with the highest
    /// stream index it executed (`through`), which the root waits for
    /// (or outwaits by the grant's expiry).
    DelegRecall {
        req: OpId,
        dir: Ino,
        gen: u64,
    },
    DelegRecalled {
        req: OpId,
        gen: u64,
        through: u64,
        /// Plan 30 §M14: the delegate's lock grants under the subtree,
        /// handed back (restamped by the root), and the subtree's lock
        /// floor as the delegate knew it.
        locks: constellation_meta::locks::LockHandback,
    },
    /// Phase 2b: a delegate appends its stream transactions to its
    /// backup before acknowledging them; the backup answers with what it
    /// holds contiguously, or `sealed`.
    DelegBackupAppend {
        req: OpId,
        gen: u64,
        txs: Vec<DelegateTx>,
    },
    DelegBackupAck {
        req: OpId,
        gen: u64,
        acked: u64,
        sealed: bool,
    },
    /// Phase 2b: the root asks a silent delegate's backup to seal the
    /// generation and hand over its tail.
    DelegSeal {
        req: OpId,
        gen: u64,
    },
    DelegSealed {
        req: OpId,
        gen: u64,
        /// Whether this node was the backup (else `txs` is empty).
        sealed: bool,
        txs: Vec<DelegateTx>,
    },
    /// Plan 30 §M14: a node asks the owning sequencer of `ino` for a
    /// lock grant in `mode`; `blocking` parks at the owner until the
    /// conflicting grants are recalled or outwaited.
    LockRequest {
        req: OpId,
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        blocking: bool,
        /// The requester's honour clock when it sent (plan 43 C7-H1):
        /// echoed in a `LockGranted` push, so the grant's window never
        /// starts later than the request it answers (a stale push after a
        /// pause found this). The owner never reads it as its own time.
        sent: HonourMs,
        /// The requester's incarnation: the owner drops what an earlier
        /// one left queued (`LockState::incarnations`).
        incarnation: u32,
        /// How long the application has been waiting for this lock, a
        /// duration (`Control::Lock::waited_ms`, plus time in this core).
        waited_ms: u64,
    },
    LockReply {
        req: OpId,
        outcome: LockOutcome,
    },
    /// Plan 30 §M14: a parked request answered `Waiting` gets its grant
    /// pushed; `sent` echoes the requester's honour clock at its last send
    /// (the grant's lifetime counts from there).
    LockGranted {
        ino: Ino,
        sent: HonourMs,
        outcome: LockOutcome,
    },
    /// The requester no longer waits for `ino` in the modes named: an ask
    /// to ask again found no op of it to re-send, or the op left without
    /// needing the owner (another op's grant covered it, it gave up). The
    /// owner drops its parked waiters of the node there, at once and
    /// without counting an unused turn.
    LockCancel {
        ino: Ino,
        exclusive: bool,
        shared: bool,
        /// The requester's honour clock when it decided: only waiters
        /// whose last request was sent before it (`LockRequest::sent`, a
        /// reading of the same clock) are dropped (a request of the node
        /// sent after the cancel, which a different stream may deliver
        /// first, stays). Opaque to the owner.
        upto: HonourMs,
    },
    /// Plan 30 §M14: the owner recalls `grant`; the holder acks receipt
    /// and releases (`LockReleased`) once no local lock is under it and
    /// its dirty data is flushed — or is outwaited by the grant's expiry.
    LockRecall {
        req: OpId,
        ino: Ino,
        grant: constellation_meta::locks::GrantId,
    },
    LockRecalled {
        req: OpId,
    },
    LockReleased {
        ino: Ino,
        grant: constellation_meta::locks::GrantId,
        /// Everything the releasing node's clients had seen or been
        /// acknowledged when it released (`Replica::frontier`): the next
        /// grant of the file carries it, so what the previous holder
        /// wrote to *other* files under the lock is visible under the
        /// next one too (EC2 campaign 4 B-1: git's refs and objects
        /// under an `flock` turn file).
        position: constellation_meta::Position,
    },
    /// Plan 30 §M14: renew the grants this node holds at the owner.
    LockRenew {
        req: OpId,
        entries: Vec<LockRenewEntry>,
        /// The holder's honour reading when it sent this, and the id of that
        /// clock: what a sequencer that installed these grants as copies
        /// confirms them from (`Grant::confirmed`; plan 43 C7b, row 29).
        /// Readings of the holder's own clock, opaque to the owner.
        sent: HonourMs,
        clock: u64,
    },
    LockRenewed {
        req: OpId,
        results: Vec<(Ino, constellation_meta::locks::GrantId, LockRenewResult)>,
    },
    /// Plan 30 §M14: the holder's lock table, mirrored to a backup (one
    /// way, asynchronous; the successor installs the last one it saw).
    LockMirror {
        ver: u64,
        grants: Vec<constellation_meta::locks::GrantWire>,
        /// Every lock floor the holder knows, joined: a fast successor
        /// puts it under the whole namespace.
        floor: constellation_meta::Position,
        /// Plan 43 C7-F F-c: `Some(key)`: the **final** mirror of the lease
        /// `key` names — the whole table as of the moment its holder could
        /// no longer add a record under it (`Core::lock_final_step`).
        final_for: Option<LeaseKey>,
    },
    /// Plan 30 §M14: `getlk` — whether another node holds a conflicting
    /// grant.
    LockTest {
        req: OpId,
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
    },
    LockTestReply {
        req: OpId,
        outcome: LockTestOutcome,
    },
    /// Plan 43 C1-3 (Q13): seal epoch `epoch` here and send what this node
    /// holds of it. The claimant of a lease by TTL asks every member of the
    /// old group (the holder and each listed backup, or the inherited
    /// group) before its CAS: the first member that answers `held` has,
    /// write-all, every committed row. `config_version`: the configuration
    /// of the lease the claimant read (a member accepts any version at or
    /// above the one it holds); `from`: the first journal seq wanted (0:
    /// the first batch).
    SealTail {
        req: OpId,
        epoch: Epoch,
        config_version: u64,
        from: u64,
    },
    /// The answer, in batches of `backup_batch_rows` (`next`: ask again
    /// from there). `held: false`: this node holds no tail of `epoch` (it
    /// was not a member, or is not any more); `through`: the last journal
    /// seq of the contiguous hold.
    SealedTail {
        req: OpId,
        epoch: Epoch,
        held: bool,
        txs: Vec<BackupTx>,
        through: u64,
        next: Option<u64>,
    },
}

impl PeerMsg {
    /// The variant's name, for the driver's trace and slow-step lines.
    pub fn kind(&self) -> &'static str {
        match self {
            PeerMsg::MutateRequest { .. } => "MutateRequest",
            PeerMsg::MutateReply { .. } => "MutateReply",
            PeerMsg::LeaseRequest { .. } => "LeaseRequest",
            PeerMsg::LeaseHandoff { .. } => "LeaseHandoff",
            PeerMsg::SegmentPublished { .. } => "SegmentPublished",
            PeerMsg::LogSubscribe { .. } => "LogSubscribe",
            PeerMsg::LogUnsubscribe { .. } => "LogUnsubscribe",
            PeerMsg::LogStream { .. } => "LogStream",
            PeerMsg::LogStreamEnd { .. } => "LogStreamEnd",
            PeerMsg::ReadIndex { .. } => "ReadIndex",
            PeerMsg::ReadIndexReply { .. } => "ReadIndexReply",
            PeerMsg::DelegationRecall { .. } => "DelegationRecall",
            PeerMsg::DelegationRecalled { .. } => "DelegationRecalled",
            PeerMsg::BackupAppend { .. } => "BackupAppend",
            PeerMsg::BackupAck { .. } => "BackupAck",
            PeerMsg::StreamAhead { .. } => "StreamAhead",
            PeerMsg::BackupHold { .. } => "BackupHold",
            PeerMsg::PromiseRequest { .. } => "PromiseRequest",
            PeerMsg::PromiseReply { .. } => "PromiseReply",
            PeerMsg::DelegateStream { .. } => "DelegateStream",
            PeerMsg::DelegateStreamAck { .. } => "DelegateStreamAck",
            PeerMsg::DelegRenew { .. } => "DelegRenew",
            PeerMsg::DelegRenewOffCore { .. } => "DelegRenewOffCore",
            PeerMsg::DelegRenewedOffCore { .. } => "DelegRenewedOffCore",
            PeerMsg::DelegRenewed { .. } => "DelegRenewed",
            PeerMsg::DelegRecall { .. } => "DelegRecall",
            PeerMsg::DelegRecalled { .. } => "DelegRecalled",
            PeerMsg::DelegBackupAppend { .. } => "DelegBackupAppend",
            PeerMsg::DelegBackupAck { .. } => "DelegBackupAck",
            PeerMsg::DelegSeal { .. } => "DelegSeal",
            PeerMsg::DelegSealed { .. } => "DelegSealed",
            PeerMsg::LockRequest { .. } => "LockRequest",
            PeerMsg::LockReply { .. } => "LockReply",
            PeerMsg::LockGranted { .. } => "LockGranted",
            PeerMsg::LockCancel { .. } => "LockCancel",
            PeerMsg::LockRecall { .. } => "LockRecall",
            PeerMsg::LockRecalled { .. } => "LockRecalled",
            PeerMsg::LockReleased { .. } => "LockReleased",
            PeerMsg::LockRenew { .. } => "LockRenew",
            PeerMsg::LockRenewed { .. } => "LockRenewed",
            PeerMsg::LockMirror { .. } => "LockMirror",
            PeerMsg::LockTest { .. } => "LockTest",
            PeerMsg::LockTestReply { .. } => "LockTestReply",
            PeerMsg::SealTail { .. } => "SealTail",
            PeerMsg::SealedTail { .. } => "SealedTail",
        }
    }

    /// The correlation id this message *answers*, when it is a reply.
    pub fn answers(&self) -> Option<OpId> {
        match self {
            PeerMsg::MutateReply { req, .. }
            | PeerMsg::LeaseHandoff { req, .. }
            | PeerMsg::ReadIndexReply { req, .. }
            | PeerMsg::DelegationRecalled { req }
            | PeerMsg::BackupAck { req, .. }
            | PeerMsg::PromiseReply { req, .. }
            | PeerMsg::DelegateStreamAck { req, .. }
            | PeerMsg::DelegRenewed { req, .. }
            | PeerMsg::DelegRenewedOffCore { req, .. }
            | PeerMsg::DelegRecalled { req, .. }
            | PeerMsg::DelegBackupAck { req, .. }
            | PeerMsg::DelegSealed { req, .. }
            | PeerMsg::LockReply { req, .. }
            | PeerMsg::LockRecalled { req }
            | PeerMsg::LockRenewed { req, .. }
            | PeerMsg::LockTestReply { req, .. }
            | PeerMsg::SealedTail { req, .. } => Some(*req),
            _ => None,
        }
    }

    /// The correlation id this message carries as a *request*, when it
    /// expects a reply (the driver reports `Event::PeerFailed` for it).
    pub fn requests(&self) -> Option<OpId> {
        match self {
            PeerMsg::MutateRequest { req, .. }
            | PeerMsg::LeaseRequest { req, .. }
            | PeerMsg::LogSubscribe { req, .. }
            | PeerMsg::ReadIndex { req, .. }
            | PeerMsg::DelegationRecall { req, .. }
            | PeerMsg::BackupAppend { req, .. }
            | PeerMsg::PromiseRequest { req, .. }
            | PeerMsg::DelegateStream { req, .. }
            | PeerMsg::DelegRenew { req, .. }
            | PeerMsg::DelegRenewOffCore { req, .. }
            | PeerMsg::DelegRecall { req, .. }
            | PeerMsg::DelegBackupAppend { req, .. }
            | PeerMsg::DelegSeal { req, .. }
            | PeerMsg::LockRequest { req, .. }
            | PeerMsg::LockRecall { req, .. }
            | PeerMsg::LockRenew { req, .. }
            | PeerMsg::LockTest { req, .. }
            | PeerMsg::SealTail { req, .. } => Some(*req),
            _ => None,
        }
    }
}

/// Plan 30 §M8: the sequencer's answer to a `ReadIndex`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadIndexOutcome {
    /// Wait until the replica reaches `position`, then read; with a read
    /// delegation on the inode when `grant` is set.
    Ok {
        position: Position,
        grant: Option<ReadGrantMsg>,
        /// Delegations on children of the directory (read-ahead), when
        /// the request asked for them and the sequencer made any.
        ahead: Option<Box<AheadGrants>>,
    },
    /// Not the holder (`holder`: whom it believes holds, 0 unknown).
    NotHolder { holder: NodeId },
    /// The holder, but fenced (its takeover gate, or a release in
    /// progress): ask again shortly.
    Busy,
}

/// What a `ReadIndex` asks for besides the read of its own inode: delegations
/// on children of a directory. A hint: only the sequencer answering for
/// itself acts on it (a delegate, or a node answering after a redirect,
/// ignores it), and everything else about the read, its redirects, grants,
/// position and the reader's install, is keyed on the request's `ino`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReadAhead {
    #[default]
    None,
    /// The request is a directory's own read (a listing's first chunk):
    /// delegations on its first children.
    Listing,
    /// The request is a plain read of a child inode looked up by `name` in
    /// directory `dir`: delegations on `dir`'s next children from `name`
    /// (a scan will look them up one by one).
    Siblings { dir: Ino, name: String },
}

/// Read delegations on children of a directory, granted with one
/// `ReadIndex` answer: one lifetime and epoch for all (they were made
/// together), each its own grant id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AheadGrants {
    pub ttl_ms: u64,
    pub epoch: Epoch,
    /// `(ino, grant id)`.
    pub grants: Vec<(Ino, u64)>,
}

/// A read delegation as granted on the wire: `ttl_ms` counts from when
/// the requester *sent* its request (it measures from there, minus the
/// margin); `epoch` is the grant's lease epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadGrantMsg {
    pub id: u64,
    pub ttl_ms: u64,
    pub epoch: Epoch,
}

/// The result of one `S3Op` (see `crate::action::S3Op`). Each variant
/// mirrors its request; the driver picks the variant by the request it
/// ran, never by inspecting the core.
#[derive(Debug)]
pub enum S3Result {
    /// `S3Op::LeaseGet`: the object and its tag, or none yet.
    LeaseGet(Result<Option<(Lease, LeaseTag)>, S3Failure>),
    /// `S3Op::LeaseCreate` / `S3Op::LeaseSwap`: the new tag, or why not.
    LeasePut(Result<LeaseTag, CasFailure>),
    /// `S3Op::SegmentPut`.
    SegmentPut(Result<(), CasFailure>),
    /// `S3Op::SegmentRun`: the contiguous run present from `from`.
    SegmentRun(Result<Vec<(Seq, Vec<u8>)>, S3Failure>),
    /// `S3Op::SegmentGap`: the lowest segment at or after `from`, if any.
    SegmentGap(Result<Option<Seq>, S3Failure>),
    /// `S3Op::InboxPut`: created (or found to be this very batch, already
    /// landed), or why not.
    InboxPut(Result<(), CasFailure>),
    /// `S3Op::InboxRun`: the contiguous run of one requester's batches.
    InboxRun(Result<Vec<InboxBatch>, S3Failure>),
    /// `S3Op::InboxDrain`: every batch below the epoch, in key order.
    InboxDrain(Result<Vec<InboxBatch>, S3Failure>),
    InboxDelete(Result<(), S3Failure>),
    /// `S3Op::InboxTombstone`: the batch now holds no ops.
    InboxTombstone(Result<(), S3Failure>),
    /// `S3Op::InboxLastN`: the highest batch number this node wrote
    /// under the epoch, if any (a previous incarnation's).
    InboxLastN(Result<Option<u64>, S3Failure>),
    /// Plan 30 §M10: `S3Op::HeartbeatRead` — every decodable heartbeat
    /// object, keyed by the node its key names.
    Heartbeats(Result<Vec<(NodeId, Promise)>, S3Failure>),
    /// Plan 30 §M10: `S3Op::HeartbeatPut`.
    HeartbeatPut(Result<(), S3Failure>),
}

/// Why a conditional PUT did not land. `Conflict` is a lost race (412,
/// or 409 on a create); M4's `cas::put_conditional` folds its 409 retries
/// and its own-write recognition in *before* the core sees a result, so
/// the core only ever distinguishes "lost" from "the store failed".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasFailure {
    Conflict,
    Failed(String),
}

/// A non-conditional request failed (timeout, 5xx, unreachable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Failure(pub String);

/// Plan 30 §M14: the owner's answer to a `LockRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockOutcome {
    /// Honour it until `sent + ttl − margin`; read `ino` only once the
    /// replica reaches `position`.
    Granted {
        id: constellation_meta::locks::GrantId,
        mode: constellation_meta::locks::LockMode,
        ttl_ms: u64,
        position: Position,
    },
    /// Parked (a blocking request): ask again in `retry_ms` (the grant
    /// may be pushed before that). `grace`: the owner may not grant yet
    /// but nothing conflicts (a grace, the tenure's floor, a barrier, a
    /// delegate's short authority; plan 43 F-b): a non-blocking request
    /// waits too, asking again at `retry_ms` without spending an
    /// attempt, instead of being refused.
    Waiting { retry_ms: u64, grace: bool },
    /// A conflicting grant is held elsewhere (recalled now); a
    /// non-blocking request.
    WouldBlock,
    /// Ask `owner` (0: unknown; read the lease).
    NotOwner { owner: NodeId },
    /// Try again shortly (the owner is between tenures, or probing).
    Busy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockRenewEntry {
    pub ino: Ino,
    pub grant: constellation_meta::locks::GrantId,
    pub mode: constellation_meta::locks::LockMode,
    /// Plan 43 C7-F F-c: a **late** renewal, sent for a grant whose window
    /// its holder no longer honours (it pins local locks or unpublished
    /// writes and asks to have the same grant back). The owner admits it
    /// only where it can prove no conflicting grant was issued since
    /// (`LockTables::admit_late`), never as a reclaim.
    pub late: bool,
}

/// Plan 43 C7-F F-c: the identity of a lease body as a final mirror and a
/// fast takeover compare it: `(holder, holder_clock, holder_wait_ms,
/// epoch)`. Equal means the very lease the old root held at its end.
pub type LeaseKey = (u64, u64, i64, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockRenewResult {
    /// Honoured until `sent + ttl − margin`; `recalled`: release it once
    /// free (a lost recall repaired by the renewal). `id`/`mode`: the
    /// grant the owner holds for this node on the inode — a newer one
    /// than asked about when its reply was lost (the node adopts it).
    Ok {
        ttl_ms: u64,
        recalled: bool,
        id: constellation_meta::locks::GrantId,
        mode: constellation_meta::locks::LockMode,
    },
    /// The owner does not know it (and no grace admits a reclaim): it is
    /// lost; I/O under it is fenced.
    Lost,
    NotOwner {
        owner: NodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockTestOutcome {
    Free,
    Held {
        node: NodeId,
        mode: constellation_meta::locks::LockMode,
    },
    NotOwner {
        owner: NodeId,
    },
}
