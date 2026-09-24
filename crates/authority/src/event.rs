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

use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
use constellation_fs_core::Ino;
use constellation_meta::{LogRecord, MutateOp, MutateOutcome, Position, Rid};
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
    /// The membership poll read the write-eligible roster (M13's inbox
    /// polls it; M9 picks backups from it).
    Roster { write_eligible: Vec<NodeId> },
    /// The P2P directory's view of the peers (M13's reachability rule,
    /// the holder's inbox poll set, M9's backup liveness). Sent whole,
    /// whenever the driver refreshes it.
    Peers { links: Vec<PeerLink> },
    /// M7: the driver dropped a subscriber of this holder's log stream
    /// (its bounded send buffer overflowed, or its connection went away):
    /// stop streaming to it. The subscriber falls back to S3 and
    /// resubscribes on its own.
    SubscriberGone { node: NodeId, req: OpId },
    /// Local writes the driver's fast path executed without the core
    /// (the FUSE lease view's `admit`): the time of the last one, for the
    /// idle-release clock, and their rid seqs, for the ack tracker
    /// (`acked_through`).
    Activity {
        last_write: Ms,
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
    /// When it was last heard from (any successful exchange).
    pub last_seen: Option<Ms>,
}

/// Control-plane requests (`SyncRequest`'s non-mutation arms).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// Run a round soon (`SyncRequest::Nudge`).
    Nudge,
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
    /// Flush the journal, publish if the holder, release the lease, and
    /// answer — without stopping (`leave`).
    Flush,
    /// Final flush + release on unmount (`Shipper::shutdown_all`); the
    /// core stops afterwards.
    Shutdown,
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
    },
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
    },
    /// "I want the lease" (`Payload::LeaseRequest`), sent to the holder.
    LeaseRequest {
        req: OpId,
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
    /// M8: `cto=strict` open(): ask the owning sequencer for the current
    /// position and inode record, and possibly a read delegation.
    ReadIndex {
        req: OpId,
        ino: Ino,
    },
    ReadIndexReply {
        req: OpId,
        position: Seq,
        record: Option<LogRecord>,
        delegation_ttl_ms: Option<u64>,
    },
    /// M8: the sequencer recalls a read delegation before acking a
    /// mutation on that inode; the delegate acks.
    DelegationRecall {
        req: OpId,
        ino: Ino,
    },
    DelegationRecalled {
        req: OpId,
    },
    /// M9: the holder streams a journal batch to its backups; a backup
    /// acks it; a backup seals an epoch before taking over.
    BackupAppend {
        epoch: Epoch,
        first_journal_seq: u64,
        records: Vec<LogRecord>,
    },
    BackupAck {
        epoch: Epoch,
        journal_seq: u64,
    },
    Sealed {
        epoch: Epoch,
    },
    /// M11: a delegate's ordered record stream to the root (`deps` is the
    /// highest position the delegate's requester observed).
    DelegateStream {
        gen: u64,
        rid: Rid,
        records: Vec<LogRecord>,
        deps: Seq,
    },
    /// M11: the root recalls a delegation; the delegate drains and acks.
    Recall {
        req: OpId,
        dir: Ino,
        gen: u64,
    },
    Recalled {
        req: OpId,
    },
}

impl PeerMsg {
    /// The correlation id this message *answers*, when it is a reply.
    pub fn answers(&self) -> Option<OpId> {
        match self {
            PeerMsg::MutateReply { req, .. }
            | PeerMsg::LeaseHandoff { req, .. }
            | PeerMsg::ReadIndexReply { req, .. }
            | PeerMsg::DelegationRecalled { req }
            | PeerMsg::Recalled { req } => Some(*req),
            _ => None,
        }
    }

    /// The correlation id this message carries as a *request*, when it
    /// expects a reply (the driver reports `Event::PeerFailed` for it).
    pub fn requests(&self) -> Option<OpId> {
        match self {
            PeerMsg::MutateRequest { req, .. }
            | PeerMsg::LeaseRequest { req }
            | PeerMsg::LogSubscribe { req, .. }
            | PeerMsg::ReadIndex { req, .. }
            | PeerMsg::DelegationRecall { req, .. }
            | PeerMsg::Recall { req, .. } => Some(*req),
            _ => None,
        }
    }
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
    /// `S3Op::InboxPut`: created (or found to be this very batch, already
    /// landed), or why not.
    InboxPut(Result<(), CasFailure>),
    /// `S3Op::InboxRun`: the contiguous run of one requester's batches.
    InboxRun(Result<Vec<InboxBatch>, S3Failure>),
    /// `S3Op::InboxDrain`: every batch below the epoch, in key order.
    InboxDrain(Result<Vec<InboxBatch>, S3Failure>),
    InboxDelete(Result<(), S3Failure>),
    /// `S3Op::InboxLastN`: the highest batch number this node wrote
    /// under the epoch, if any (a previous incarnation's).
    InboxLastN(Result<Option<u64>, S3Failure>),
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
