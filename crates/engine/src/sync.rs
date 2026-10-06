//! The sync task's request channel: every message the daemon's callers
//! (the FUSE adapter, the control socket, the P2P bridge, background
//! tickers) send to the authority core's driver
//! ([`crate::authority_driver`]), plus the two outcome structs they carry.
//!
//! These lived in `cli/src/fusefs.rs` until plan 31 C3: an accident of
//! history (the sync task's handle, `SyncHandle`, lived with
//! `ConstellationFs`, now `View`). They name no FUSE type; they are
//! engine-internal driver traffic, and moving them here is what lets
//! `authority_driver`, `gc`, `locks`, `prune` and `recovery` live in the
//! engine crate.

use constellation_fs_core::{ChunkHash, Ino};

/// Outcome of a `SyncRequest::Acquire` attempt (see its doc).
#[derive(Debug, Clone, Copy, Default)]
pub struct AcquireProgress {
    pub acquired: bool,
    pub holder: u64,
    pub epoch: u64,
}

impl AcquireProgress {
    pub fn busy(holder: u64, epoch: u64) -> Self {
        Self {
            acquired: false,
            holder,
            epoch,
        }
    }
}

/// Outcome of a successful partition handoff (flush + lease release).
#[derive(Debug, Clone)]
pub struct HandoffResult {
    pub epoch: u64,
    pub etag: Option<String>,
    pub head_seq: Option<u64>,
}

/// Why a durability request (`Barrier`, `DrainInode`) failed, and whether
/// it is worth waiting out (plan 39: an `fsync` retries a
/// [`ErrorClass::Transient`] failure until the data is durable and
/// answers a [`ErrorClass::Permanent`] one with `EIO` at once).
#[derive(Debug, Clone)]
pub struct SyncFailure {
    pub class: ErrorClass,
    pub message: String,
}

pub use constellation_store_s3::ErrorClass;

impl SyncFailure {
    /// Classified from the error's chain: content this node lost, or a
    /// local metadata-store failure, is permanent; chunks another node
    /// still has to upload are transient (plan 39b,
    /// `crate::upload::DrainShortfall`); the rest is the store's table
    /// (`constellation_store_s3::classify`).
    pub fn from_error(error: &anyhow::Error) -> Self {
        let shortfall = error
            .chain()
            .find_map(|e| e.downcast_ref::<crate::upload::DrainShortfall>());
        let local = error
            .chain()
            .any(|e| e.downcast_ref::<constellation_meta::MetaError>().is_some());
        let class = if let Some(shortfall) = shortfall {
            match shortfall {
                crate::upload::DrainShortfall::Lost { .. } => ErrorClass::Permanent,
                crate::upload::DrainShortfall::AwaitingRemote { .. } => ErrorClass::Transient,
            }
        } else if local {
            ErrorClass::Permanent
        } else {
            constellation_store_s3::classify_chain(error.chain())
        };
        Self {
            class,
            message: format!("{error:#}"),
        }
    }

    /// A failure known only as text (a sync round's outcome crosses the
    /// authority core as a string).
    pub fn from_text(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            class: constellation_store_s3::classify_message(&message),
            message,
        }
    }
}

impl std::fmt::Display for SyncFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SyncFailure {}

impl From<SyncFailure> for String {
    fn from(failure: SyncFailure) -> String {
        failure.message
    }
}

/// A request to the daemon's sync task — the authority core's driver
/// (`crate::authority_driver`), which turns each into a core event.
pub enum SyncRequest {
    /// Run a sync round soon; the sender does not wait.
    Nudge,
    /// The write-eligible roster from the registry poll (M13: who the
    /// holder polls).
    Roster(Vec<u64>),
    /// The continuation-epoch machine changed state (the driver
    /// re-reports it to the core).
    EpochChanged,
    /// Ship everything this node can, then publish a plan 28 metadata
    /// commit and reply with `(seq, root)` — the tree a snapshot taken
    /// now retains. With `through: Some(applied)`, ship nothing first:
    /// publish a commit covering applied log position `applied` (a
    /// snapshot whose `Barrier` already shipped every row admitted
    /// before it; `TreePublisher::publish_through`).
    Publish {
        through: Option<u64>,
        reply: tokio::sync::oneshot::Sender<Result<(u64, constellation_mtree::NodeHash), String>>,
    },
    /// Upload `ino`'s chunks, then wait until every journal row admitted
    /// before this request has shipped (fsync, snapshot and clone
    /// barrier; `jobs::BarrierWait` in the core) — not for an empty
    /// journal, which a busy holder never has.
    Barrier {
        ino: Ino,
        reply: tokio::sync::oneshot::Sender<Result<(), SyncFailure>>,
    },
    /// Upload `ino`'s pending chunks (all of them for `ino == 0`). EC2
    /// finding 1: when this node's own uploads make no progress for
    /// `chunk_handoff_after`, the chunks are handed to a peer that can
    /// reach S3 (`ChunkHandoff`), and the drain succeeds once that peer
    /// has them in S3.
    ///
    /// `fsync` (the drain of an `fsync` or an `O_SYNC` write, plan 39b;
    /// `ino != 0`): the reply is a success only once nothing of `ino` is
    /// pending here —
    /// - the durable reports this file's chunks owe
    ///   (`meta::store::remote`) are delivered before replying, each send
    ///   bounded, also after a peer handoff — so once the `fsync` returns,
    ///   the sequencer a `back` close forwarded the manifest to knows its
    ///   chunks are up; other files' reports stay in the background;
    /// - a pending chunk of `ino` neither in the cache nor in S3 fails it
    ///   permanently (`crate::upload::DrainShortfall::Lost`), never `Ok`;
    /// - rows of `ino` another node forwarded as pending (on the
    ///   sequencer) are waited for, and still missing after a slice of
    ///   the wait they fail it transiently
    ///   (`DrainShortfall::AwaitingRemote`) so the `fsync`'s retry loop
    ///   keeps waiting under plan 39's policy.
    DrainInode {
        ino: Ino,
        fsync: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), SyncFailure>>,
    },
    /// EC2 finding 1: `requester` cannot reach S3 and hands us `hashes`
    /// to fetch from it and upload; `reply` is whether all are in S3.
    AcceptHandoff {
        requester: u64,
        hashes: Vec<ChunkHash>,
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    /// Tail to the log head without shipping or publishing anything
    /// (plan 29 M3a: in-daemon GC's liveness-freshness gate). Never
    /// touches a lease, so it is safe from a read-only member.
    TailToHead {
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Take the lease if it is free. `acquired: false` means a live
    /// foreign holder still owns it — `holder`/`epoch` are a best-effort
    /// snapshot of that holder (0/0 when unknown), letting a retrying
    /// caller tell forward progress from a genuinely stuck wait (plan 29
    /// M3c).
    Acquire {
        reply: tokio::sync::oneshot::Sender<Result<AcquireProgress, String>>,
    },
    /// A peer asked us to hand the lease over (M3.3 fast path): flush the
    /// journal to S3 and release. Replies with the epoch and last shipped
    /// seq we held, or `None` if we do not hold it or the flush failed —
    /// the requester then waits the lease out through S3.
    HandOff {
        requester: u64,
        /// `Payload::LeaseRequest::epoch_applied`.
        epoch_applied: Option<u64>,
        reply: tokio::sync::oneshot::Sender<Option<HandoffResult>>,
    },
    /// A peer forwarded a mutation to this node as (believed) holder.
    /// The reply carries plan 30 §M6's `base` (see
    /// `constellation_authority::PeerMsg::MutateReply`).
    Mutate {
        requester: u64,
        op: Vec<u8>,
        /// Plan 30 §M2: the op's exactly-once identity, for holder-side
        /// dedup.
        rid: constellation_meta::Rid,
        /// Plan 30 §M2 GC: prune `recent` outcomes for `requester`'s
        /// current incarnation up to this seq.
        acked_through: u64,
        /// Plan 30 §M11: postcard of the requester's observed position.
        deps: Vec<u8>,
        /// Chunks the op's manifest names that are still uploading on the
        /// requester (`meta::store::remote`): enrolled before it executes.
        pending: Vec<ChunkHash>,
        /// The log sequence the requester had applied when it sent the
        /// op (`PeerMsg::MutateRequest::applied`).
        applied: u64,
        /// Plan 30 §M14 phase 2: the op's fencing token.
        tag: constellation_meta::locks::LockTag,
        reply: tokio::sync::oneshot::Sender<crate::authority_driver::MutateReplyParts>,
    },
    /// `from` reports chunks it forwarded as pending durable in S3: ack
    /// the rows this node awaits for them (`meta::store::remote`).
    ChunksDurable {
        from: u64,
        hashes: Vec<ChunkHash>,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// This node's own mutation, when the FUSE fast path could not
    /// execute it locally: the core forwards it, submits it through the
    /// holder's inbox, or takes the lease, per `policy`.
    Submit {
        op: constellation_meta::MutateOp,
        /// Plan 30 §M2: allocated once by the caller and kept across every
        /// retry this op goes through.
        rid: constellation_meta::Rid,
        policy: constellation_authority::Policy,
        /// Plan 30 §M9: a resubmission of an op the fast path executed
        /// here but could not acknowledge (the lease was lost while its
        /// acknowledgement waited for durability): in doubt from the
        /// start, resolved against `completed` by rid.
        in_doubt: bool,
        /// Plan 30 §M14 phase 2: the fencing token — the cluster-lock
        /// grants the issuing process held (empty: none).
        tag: constellation_meta::locks::LockTag,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ClientReply>,
    },
    /// Plan 30 §M9: the fast path journaled a row under a durability
    /// gate; the core appends it to the backups now.
    Journaled,
    /// Plan 30 §M8: a strict open or lookup on this node needs to know
    /// how it may read (`constellation_authority::ReadAnswer`).
    ReadIndex {
        ino: Ino,
        dir: bool,
        name: Option<String>,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ReadAnswer>,
    },
    /// Plan 30 §M8: a write the FUSE fast path executed here as the
    /// sequencer touched inodes other nodes hold read delegations on:
    /// answered once they are recalled (or outwaited).
    Recall {
        inos: Vec<Ino>,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Plan 30 §M11: a delegate's stream batch (this node is the root);
    /// answered `(through, refused)`.
    PeerDelegateStream {
        from: u64,
        gen: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        reply: tokio::sync::oneshot::Sender<(u64, bool)>,
    },
    /// Plan 30 §M11: a delegate's renewal; answered with the ttl (0:
    /// refused).
    PeerDelegRenew {
        from: u64,
        gen: u64,
        /// Phase 2b: the delegate's backup peer (0: none).
        backup: u64,
        /// Plan 30 §M14: the delegate's executed stream head.
        stream_head: u64,
        /// Answered with the ttl and (§M14) the root's lock grants under
        /// the subtree, handed over with the first renewal, and the
        /// remaining lock grace on it (ms).
        reply: tokio::sync::oneshot::Sender<(
            u64,
            Vec<constellation_meta::locks::Grant>,
            u64,
            constellation_meta::Position,
        )>,
    },
    /// Plan 30 §M11: the root recalls a generation this node holds;
    /// answered with the highest stream index executed here (and, §M14,
    /// the lock grants handed back).
    PeerDelegRecall {
        root: u64,
        dir: Ino,
        gen: u64,
        reply: tokio::sync::oneshot::Sender<(u64, constellation_meta::locks::LockHandback)>,
    },
    /// Plan 30 §M11 phase 2b: a delegate's append to this backup;
    /// answered `(acked, sealed)`.
    PeerDelegBackupAppend {
        from: u64,
        gen: u64,
        txs: Vec<constellation_meta::DelegateTx>,
        reply: tokio::sync::oneshot::Sender<(u64, bool)>,
    },
    /// Plan 30 §M11 phase 2b: the root's seal request to this backup;
    /// answered `(sealed, tail)`.
    PeerDelegSeal {
        root: u64,
        gen: u64,
        reply: tokio::sync::oneshot::Sender<(bool, Vec<constellation_meta::DelegateTx>)>,
    },
    /// Plan 30 §M11 phase 2b: the live write designations `(dir,
    /// designee)`, after every refresh; the root keeps the table in step.
    SyncDesignations {
        entries: Vec<(Ino, u64)>,
    },
    /// Plan 30 §M11: operator controls (`constellation delegate` /
    /// `undelegate`).
    Delegate {
        dir: Ino,
        node: u64,
        /// Plan 30 §M12: `(bits, idx)`; `(0, 0)` is the whole directory.
        range: (u8, u32),
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
    Undelegate {
        dir: Ino,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
    /// Plan 30 §M8: a peer's ReadIndex, to answer as the sequencer.
    PeerReadIndex {
        requester: u64,
        ino: Ino,
        dir: bool,
        name: Option<String>,
        reply: tokio::sync::oneshot::Sender<constellation_authority::ReadIndexOutcome>,
    },
    /// Plan 30 §M8: the sequencer recalls a read delegation this node
    /// holds; answered once it is no longer honoured.
    PeerRecall {
        holder: u64,
        ino: Ino,
        grant: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Plan 30 §M9: the holder streams journal transactions to this
    /// node as its backup; answered with `(acked through, sealed)`.
    PeerBackupAppend {
        holder: u64,
        epoch: u64,
        config_version: u64,
        candidacy: u64,
        from: u64,
        txs: Vec<constellation_meta::BackupTx>,
        through: u64,
        reply: tokio::sync::oneshot::Sender<(u64, bool)>,
    },
    /// Plan 37 §8: the holder this node backs is being replaced on its
    /// own state dir (`Payload::BackupHold`).
    PeerBackupHold {
        holder: u64,
        epoch: u64,
        for_ms: u64,
    },
    /// Plan 37 §8: this node, the holder, is about to be replaced on its
    /// own state dir: ask each committed backup to hold its seal watch
    /// for `for_ms`; answered with the backups that confirmed.
    HoldBackups {
        for_ms: u64,
        reply: tokio::sync::oneshot::Sender<Vec<u64>>,
    },
    /// Plan 30 §M9: backup-acked transactions streamed ahead of S3 by
    /// the holder this node follows.
    PeerStreamAhead {
        from: u64,
        epoch: u64,
        base: u64,
        txs: Vec<constellation_meta::BackupTx>,
    },
    /// The lease holder's off-core liveness heartbeat, stamped with its
    /// arrival (unix ms): `Event::HolderAlive`.
    PeerHolderAlive {
        holder: u64,
        epoch: u64,
        candidacy: u64,
        listed: bool,
        at_unix_ms: i64,
    },
    /// Plan 30 §M10: a would-be taker asks this node for a heartbeat
    /// promise; answered with `(until, epoch_slack)` (`until: None`:
    /// refused).
    PeerPromiseRequest {
        requester: u64,
        expires_unix_ms: i64,
        reply: tokio::sync::oneshot::Sender<(Option<i64>, u32)>,
    },
    /// Plan 30 §M10: `meta.json`'s `epoch_slack` as last read.
    Slack(u32),
    /// Plan 30 §M10: this node's registry record is retired.
    Retired,
    /// A peer's gossip says segment `seq` landed (plan 30 §M7: a hint
    /// with no payload). The core tails now unless its log stream from
    /// the holder delivers it.
    SegmentHint {
        seq: u64,
        epoch: u64,
    },
    /// Plan 30 §M7: a peer subscribes to this node's log stream. The
    /// driver hands it to the core and writes whatever the core streams
    /// to `requester` into `sink` (bounded: a subscriber that falls
    /// behind is dropped back to S3 tailing).
    LogSubscribe {
        requester: u64,
        req: u64,
        from: u64,
        sink: tokio::sync::mpsc::Sender<constellation_net::LogEvent>,
        /// Segment bytes queued in `sink` and not yet taken by the stream
        /// writer (the driver adds, the writer's relay subtracts).
        queued_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    },
    ClaimOffer {
        epoch: u64,
    },
    /// Run the deposition recovery now (the control API, or automatically
    /// after mounting a persisted deposed state dir).
    Reintegrate(tokio::sync::oneshot::Sender<Result<String, String>>),
    /// Permanently leave the cluster (self). Flushes, tombstones the
    /// registry record, marks the state dir spent, then the caller
    /// unmounts.
    Leave {
        force: bool,
        reply: tokio::sync::oneshot::Sender<Result<String, String>>,
    },
    /// Plan 30 §M14: a local lock on `ino` needs a cross-node grant in
    /// `mode` (`Control::Lock`); `blocking` waits at the owner.
    Lock {
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        blocking: bool,
        reply: tokio::sync::oneshot::Sender<constellation_authority::LockAnswer>,
    },
    /// Plan 30 §M14: the last local lock under a recalled grant on `ino`
    /// left; the core releases the grant (nobody waits).
    LockIdle {
        ino: Ino,
    },
    /// Plan 30 §M14 phase 2: the last tagged mutation in flight under
    /// `ino`'s recalled grant was answered; a release waiting for it goes
    /// on now.
    LockReleaseWake {
        ino: Ino,
    },
    /// Plan 30 §M14: `getlk` — does another node hold a conflicting
    /// grant?
    LockTest {
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        reply: tokio::sync::oneshot::Sender<constellation_authority::LockTestAnswer>,
    },
    /// Plan 30 §M14: a peer's lock request, to answer as the owning
    /// sequencer.
    PeerLockRequest {
        requester: u64,
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        blocking: bool,
        /// The requester's clock when it sent (echoed in a push).
        sent: i64,
        incarnation: u32,
        reply: tokio::sync::oneshot::Sender<constellation_authority::LockOutcome>,
    },
    /// Plan 30 §M14: the owner recalls a grant this node holds; answered
    /// on receipt.
    PeerLockRecall {
        owner: u64,
        ino: Ino,
        grant: constellation_meta::locks::GrantId,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    /// Plan 30 §M14: a holder renews its grants at this node.
    PeerLockRenew {
        from: u64,
        entries: Vec<constellation_authority::LockRenewEntry>,
        reply: tokio::sync::oneshot::Sender<
            Vec<(
                Ino,
                constellation_meta::locks::GrantId,
                constellation_authority::LockRenewResult,
            )>,
        >,
    },
    /// Plan 30 §M14: a peer's `getlk`, to answer as the owner.
    PeerLockTest {
        requester: u64,
        ino: Ino,
        mode: constellation_meta::locks::LockMode,
        reply: tokio::sync::oneshot::Sender<constellation_authority::LockTestOutcome>,
    },
    /// Plan 30 §M14, one way: the owner pushed a parked request's grant.
    PeerLockGranted {
        from: u64,
        ino: Ino,
        sent: i64,
        outcome: constellation_authority::LockOutcome,
    },
    /// Plan 30 §M14, one way: a holder released a grant.
    PeerLockReleased {
        from: u64,
        ino: Ino,
        grant: constellation_meta::locks::GrantId,
        /// The releaser's frontier (`PeerMsg::LockReleased::position`).
        position: constellation_meta::Position,
    },
    /// Plan 30 §M14, one way: the holder's grant table (this node backs
    /// it up).
    PeerLockMirror {
        from: u64,
        ver: u64,
        grants: Vec<constellation_meta::locks::Grant>,
        floor: constellation_meta::Position,
    },
    /// Final flush + release on unmount; the core stops afterwards.
    Shutdown {
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Plan 31 C8: how this node may take write authority from now on
    /// (`constellation_authority::AuthorityMode`: the profile's
    /// `LeaseMode::ForwardOnly`, and a host suspension).
    Authority {
        forward_only: bool,
        suspended: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Plan 31 C8: a suspension's flush — every pending chunk up, the
    /// journal shipped, a commit published if the holder, the lease
    /// released — without stopping (`Control::Flush`, as `leave` uses).
    Flush {
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}
