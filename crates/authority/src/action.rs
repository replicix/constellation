//! Everything the core can ask the IO driver to do: the output side of
//! `Core::handle`.
//!
//! The core never performs IO, never sleeps and never waits. Anything that
//! can block — an S3 round trip, a P2P send, the chunk upload pass, a
//! metadata publish, a delay — is an action the driver carries out
//! *after* `handle` returned, and whose outcome comes back as an
//! [`crate::event::Event`] with the same [`OpId`]. Replica writes (fjall)
//! are the one exception and are not actions at all: they are synchronous
//! local calls through [`crate::replica::Replica`], made from inside
//! `handle` with no await between the decision and the write — which is
//! the atomicity `node_runtime::dispatch_forward` relied on before the
//! extraction.
//!
//! That split is what makes plan 30 M2b's failure mode unrepresentable:
//! there is no lock a handler could await, and no in-flight round it could
//! wait on, because a round is just core state advanced by events.

use crate::event::PeerMsg;
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
use constellation_fs_core::Ino;
use constellation_meta::{MutateOp, MutateOutcome, Rid};
use constellation_store_s3::inbox::{InboxBatch, InboxKey};
use constellation_store_s3::{Lease, LeaseTag};

/// One thing for the driver to do.
#[derive(Debug)]
pub enum Action {
    /// Answer the local client that submitted `rid`.
    Reply { rid: Rid, reply: ClientReply },
    /// Send a message to a peer. For a message whose `PeerMsg::requests`
    /// is `Some`, the driver reports `Event::PeerFailed` if it cannot be
    /// delivered; the core's own timers bound the wait for a reply.
    Send { to: NodeId, msg: PeerMsg },
    /// Run one S3 request and report `Event::S3 { op, .. }`.
    S3 { op: OpId, req: S3Op },
    /// Fire `Event::Timer { id }` at `at` (logical ms). Setting an id that
    /// is already pending moves it.
    SetTimer {
        id: TimerId,
        at: Ms,
        kind: TimerKind,
    },
    /// The core no longer wants this timer (a stale fire is ignored
    /// anyway; this only saves the wake-up).
    CancelTimer { id: TimerId },
    /// Run the chunk upload pass (`upload_dirty_chunks`) — for `ino` only,
    /// or everything pending — and report `Event::UploadsDone`. `round`
    /// says this is a sync round's opening pass (the driver runs its
    /// continuation-epoch pre-round checks before it), as opposed to a
    /// flush's.
    UploadDirtyChunks {
        op: OpId,
        ino: Option<Ino>,
        round: bool,
    },
    /// Publish a metadata commit from the replica's log-prefix state
    /// (`TreePublisher::publish` with `Meta::publish_basis_at`) and report
    /// `Event::PublishDone`. The core has already checked it is the
    /// holder and that no shadow or hint is outstanding.
    Publish { op: OpId, epoch: Epoch },
    /// M4: a follower's idle-cadence counterpart of `Publish` — clear the
    /// dirty keys the head commit already covers
    /// (`TreePublisher::follow_head`) — and report `Event::PublishDone`.
    FollowHead { op: OpId },
    /// Gossip a shipped segment (`Shipper`'s `announce_segment`), and run
    /// the offline-designation flush-ack check for the records it
    /// carries. Best effort; peers converge by tailing regardless.
    Announce {
        seq: Seq,
        epoch: Epoch,
        payload: Vec<u8>,
    },
    /// A replay by rid was refused by the current namespace: materialize
    /// the stranded op as a `.constellation-conflict/` copy
    /// (`recovery::materialize_*`) and report `Event::ConflictCopyDone`.
    ConflictCopy {
        queue_seq: u64,
        rid: Rid,
        op: MutateOp,
        reason: String,
    },
    /// A deposition recovery found journal rows with no before-images
    /// (holder capture off): rebuild the namespace from the shared log
    /// (`Meta::replace_ns_from_rebuilt` over a bootstrapped side replica)
    /// and report `Event::RebuildDone`.
    RebuildReplica { op: OpId },
    /// A sync round finished (`failed` says how), for the driver's spool
    /// counters, pin refresh and continuation-epoch bookkeeping.
    RoundDone { failed: Option<String> },
    /// M13's holder started an inbox tenure: re-read the write-eligible
    /// roster now (`Event::Roster`) instead of at the driver's next
    /// periodic refresh, so requesters that mounted since the last read
    /// are polled at once rather than up to a refresh period later.
    RefreshRoster,
    /// S3 is back and this node may ship its continuation-epoch journal:
    /// close the epoch (`EpochManager::close`); the driver reports the
    /// new state with `Control::Epoch`.
    EpochClose,
    /// The continuation epoch's journal is fully in S3 and the lease has
    /// been let go: the driver's epoch machine may stop flushing.
    EpochFlushed,
    /// Answer a control-plane request.
    ControlDone {
        op: OpId,
        result: Result<ControlOk, String>,
    },
}

/// The answer to `Event::Submit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientReply {
    /// The op's outcome: accepted (with its records, or none when an
    /// earlier attempt already took effect and the log carries them), or
    /// a refusal with its errno.
    Outcome(MutateOutcome),
    /// The acquire deadline passed with the op neither executed here nor
    /// answered by a holder. It may still take effect (a forward that
    /// timed out, a batch in an inbox); the FUSE layer answers `EIO` and
    /// the op stays retryable by the same rid.
    InDoubt,
}

/// An S3 request the core wants run. Every variant is one portable S3
/// call: GET, conditional PUT (`If-None-Match: *` create or `If-Match`
/// swap), a GET-next run, or a LIST/DELETE for M13's inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Op {
    /// GET the lease object.
    LeaseGet,
    /// Create-if-absent the lease object.
    LeaseCreate { lease: Lease },
    /// Swap the lease object if it still has `tag`.
    LeaseSwap { lease: Lease, tag: LeaseTag },
    /// Create-if-absent segment `seq` with the encoded payload.
    SegmentPut { seq: Seq, payload: Vec<u8> },
    /// GET-next: the contiguous run of segments from `from`, `width` in
    /// flight.
    SegmentRun { from: Seq, width: usize },
    /// M13: CAS-create `inbox/<epoch>/<node>/<n>` (`InboxStore::put_batch`;
    /// a key already holding this very batch counts as landed).
    InboxPut { batch: InboxBatch },
    /// M13: GET-next a requester's batches from `from`.
    InboxRun {
        epoch: Epoch,
        node: NodeId,
        from: u64,
        width: usize,
    },
    /// M13: LIST and GET every batch below `epoch` (a takeover's drain).
    InboxDrain { below_epoch: Epoch },
    /// M13: DELETE one batch.
    InboxDelete { key: InboxKey },
    /// M13: LIST-last this node's batch numbering under `epoch`.
    InboxLastN { epoch: Epoch, node: NodeId },
}

/// What a timer is for (for the driver's logs; the core keeps its own
/// record keyed by `TimerId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerKind {
    /// The periodic sync round (with idle backoff).
    Poll,
    /// A forwarded op's reply deadline (`CONSTELLATION_FORWARD_TIMEOUT_MS`).
    ForwardTimeout,
    /// Backoff before the same-rid retry of a forward.
    ForwardBackoff,
    /// Backoff before a client op re-asks for the lease.
    AcquireRetry,
    /// A client op's overall deadline (`acquire_deadline`, 2×TTL).
    ClientDeadline,
    /// The stranded-op replay drain tick.
    ReplayDrain,
    /// A job's peer request (a `LeaseRequest`) went unanswered. Every
    /// request the core sends has a timer: a delivered request whose
    /// answerer died is the one loss the transport cannot report.
    JobRequestTimeout,
    /// M13: the holder's next inbox poll.
    InboxPoll,
    /// M13: a waiting requester's next lease re-read.
    InboxRecheck,
    /// M13: backoff before the submitter retries a batch PUT.
    InboxSubmitRetry,
    /// M13 round 3b: the escalator's tick while demand is sustained.
    EscalateTick,
}

/// Successful control-plane results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlOk {
    Done,
    /// `PublishNow`: the log position the published commit covers.
    Published {
        applied: Seq,
    },
    /// `Acquire` / `ClaimOffer`: whether this node holds the lease now
    /// and, when not, a best-effort snapshot of who does (0/0 unknown).
    Lease {
        acquired: bool,
        holder: NodeId,
        epoch: Epoch,
    },
    /// `Reintegrate`: what the recovery did.
    Text(String),
}
