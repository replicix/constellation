//! Wire messages for the P2P fast path (DESIGN.md §4, §12).
//!
//! Encoding is postcard. Gossip carries the bare signed envelope while
//! direct streams add a four-byte length prefix. Cache digests make JSON
//! materially expensive (base64 bloom bits and hex hashes), so every P2P
//! message uses one compact format rather than maintaining two codecs.
//!
//! Every message is **signed by the sender's node key and verified
//! before it is acted on**. Signing matters even though QUIC already
//! authenticates the peer: gossip messages are forwarded by third
//! parties, so the transport peer is not necessarily the author.
//!
//! Nothing here is load-bearing for correctness. Every message is an
//! accelerator for something the S3 path already does on a timer, so a
//! dropped, delayed, or rejected message can only cost latency.

use anyhow::{Context, Result};
use iroh::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};

/// ALPN for direct (non-gossip) requests. Versioned so a future
/// message-set change can be negotiated rather than mis-parsed.
///
/// `3`: `Payload::MutateRequest` carries `applied` (chunk
/// close-stall-followup). `2`: `Payload::MutateReply` carries
/// `own_chunks` (chunk close-stall-metered). Nodes of different versions
/// do not talk P2P (no compatibility shim): a cluster upgrades all its
/// nodes together.
pub const ALPN: &[u8] = b"constellation/3";

/// Earlier versions of [`ALPN`]. An endpoint accepts their handshakes only
/// to refuse the connection by name (closing it with a reason naming both
/// versions), and a dial the peer refused is retried with them once, to
/// learn its version: either way both sides log the mismatch explicitly
/// (`endpoint::log_version_mismatch`), where they used to log only
/// rustls' "peer doesn't support any known protocol" every few seconds.
pub const OLDER_ALPNS: &[&[u8]] = &[b"constellation/2", b"constellation/1"];

/// Largest accepted frame. Messages are small; the cap just stops a
/// malicious peer from making us allocate.
pub const MAX_FRAME: usize = 64 * 1024;
/// iroh-gossip's transport frame. Its default is only 4 KiB; every node
/// must configure this same value so a full bloom bucket is deliverable.
pub const GOSSIP_MAX_MESSAGE_SIZE: usize = 32 * 1024;
/// Conservative content budget beneath iroh-gossip's postcard protocol
/// envelope. `Signed::encode_bare` enforces this before enqueue, because
/// the gossip sender otherwise reports an oversized frame asynchronously.
pub const GOSSIP_CONTENT_LIMIT: usize = GOSSIP_MAX_MESSAGE_SIZE - 1024;
/// Largest log segment a stream frame may carry (plan 30 §M7). A shipped
/// segment is capped at 4 MiB of records; this leaves room for the
/// envelope and stops a peer from making us allocate.
pub const MAX_LOG_SEGMENT: u64 = 64 * 1024 * 1024;
/// Conservative delta batch under [`GOSSIP_CONTENT_LIMIT`]. Raw hashes
/// cost 32 bytes each; the remaining headroom covers payload/envelope
/// tags, counters, author, and signature.
pub const MAX_GOSSIP_DELTA_ADDS: usize = 900;

/// What a peer can say. Extensible on purpose: phase 5 adds cooperative
/// chunk serving over the same ALPN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// Gossiped after a segment PUT succeeds: recipients that do not
    /// follow the holder's log stream tail immediately instead of waiting
    /// for their next poll. Plan 30 §M7: a hint only — the log itself
    /// travels on direct log streams ([`Payload::LogSubscribe`]) or through
    /// S3; gossip carries membership and digests.
    SegmentPublished {
        part: String,
        seq: u64,
        epoch: u64,
    },
    /// Plan 30 §M7: subscribe to the holder's log stream from `from`.
    /// Sent directly to the holder, on a bidirectional stream that stays
    /// open: the holder answers with [`Payload::LogFrame`]s (each followed
    /// by its segment bytes) until one side lets go, or with one
    /// [`Payload::LogEnd`].
    LogSubscribe {
        part: String,
        requester: u64,
        req_id: u64,
        from: u64,
    },
    /// Plan 30 §M7: frame `n` of subscription `req_id`. `segment` names
    /// the log sequence whose S3 object bytes (`len` of them, `blake3`
    /// hash `hash`) follow the frame on the stream; `None` is a
    /// heartbeat. `head` is the holder's highest applied-or-shipped
    /// sequence, `epoch` the epoch it holds.
    LogFrame {
        req_id: u64,
        n: u64,
        epoch: u64,
        head: u64,
        segment: Option<(u64, u64, [u8; 32])>,
    },
    /// Plan 30 §M7: the holder ends subscription `req_id` (it stopped
    /// holding, or `refused`: it never held).
    LogEnd {
        req_id: u64,
        refused: bool,
    },
    /// GC has CAS-published `gc/condemned.json`. Writers still re-read S3;
    /// this is only a freshness nudge, never the authority.
    CondemnedPublished {
        epoch: u64,
    },
    /// "I want the lease for `part`." Sent directly to the holder.
    /// `epoch_applied`: `Some(applied)` asks for a continuation epoch's
    /// P2P-only hold transfer (nothing reaches S3 during an epoch, so the
    /// holder hands its hold only to a requester that has applied its
    /// whole log); `None` asks for the S3 handoff.
    LeaseRequest {
        part: String,
        requester: u64,
        epoch_applied: Option<u64>,
    },
    /// Holder's answer: it flushed and released, so the requester can
    /// CAS-claim now. `released: false` means it declined (still busy).
    /// `etag` / `head_seq` are optional accelerators so the requester
    /// can skip a classify GET.
    LeaseHandoff {
        part: String,
        epoch: u64,
        released: bool,
        etag: Option<String>,
        head_seq: Option<u64>,
    },
    /// Liveness for status UX only — never a safety input.
    Ping {
        node_id: u64,
    },
    /// A would-be proposer of a continuation epoch asks whether this node
    /// reaches S3: the recipient probes S3 now (one bounded lease GET,
    /// skipped when its own rounds are failing) and answers `Pong` with
    /// `s3_ok` (EC2 follow-up 3c; the epoch-member-lost fix: a stale
    /// last-success time is not evidence).
    PingS3 {
        node_id: u64,
    },
    Pong {
        node_id: u64,
        /// EC2 follow-up 3c: answering a [`Payload::PingS3`], the node's
        /// S3 probe just succeeded. A would-be proposer of a continuation
        /// epoch that hears it from a live member is not in a bucket
        /// outage, only its own S3 is gone: it does not propose. Always
        /// `false` for a plain `Ping`. Advisory only: `false` changes
        /// nothing.
        s3_ok: bool,
    },
    /// Propose a continuation epoch (DESIGN.md §5.3). Recipients persist
    /// the promise locally BEFORE replying; activation is a later
    /// [`Payload::EpochActivate`] once every member has acked.
    EpochPropose {
        epoch_id: String,
        members: Vec<u64>,
        /// Applied-seq vector at proposal time (`part` → seq).
        base: Vec<(String, u64)>,
        proposer: u64,
        /// Plan 30 §M10: the proposer's `epoch_slack`; `members` is at
        /// least `N − f` of the write-eligible roster (all of it at 0).
        epoch_slack: u32,
    },
    /// Signed ack of an [`Payload::EpochPropose`]. The ack is only sent
    /// after the promise is durable on the member's local disk.
    EpochAck {
        epoch_id: String,
        member: u64,
        accepted: bool,
        /// Plan 30 §M10's claim resolution: the lease this member holds
        /// usably (epoch, expiry, whether the claim rule lets an epoch of
        /// these members carry it), and the highest lease epoch it knows
        /// exists.
        claim: Option<EpochClaim>,
        known: u64,
    },
    /// All members have acked: the epoch is now the authority root.
    /// Redistributed by the proposer; also gossiped so a late joiner of
    /// the message stream still activates.
    EpochActivate {
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        /// Plan 30 §M10: the lease the epoch carries (`None`: none), and
        /// the epoch below which a member's claim is stale.
        carrier: Option<EpochCarrier>,
        stale_below: u64,
    },
    /// Plan 30 §M10: the proposer of `epoch_id` gave up on it (a member
    /// declined or did not answer) and will never activate it: a member
    /// that persisted its promise for it drops the promise (it would
    /// otherwise stay `Promised`, an open epoch, for good). Answered
    /// with `Ok`.
    EpochAbort {
        epoch_id: String,
        proposer: u64,
    },
    /// Plan 30 §M10: a would-be taker of the expired lease asks for a
    /// heartbeat promise past `expires_unix_ms`. Answered by
    /// [`Payload::PromiseReply`].
    PromiseRequest {
        requester: u64,
        req_id: u64,
        expires_unix_ms: i64,
    },
    /// Plan 30 §M10: the persisted promise (`None`: refused — in an open
    /// continuation epoch, or retired), and the answerer's slack.
    PromiseReply {
        req_id: u64,
        until: Option<i64>,
        epoch_slack: u32,
    },
    /// Gossiped bloom of one hash-prefix bucket of this node's
    /// clean/pinned chunk cache (DESIGN.md §7). Recipients consult it
    /// locally on a miss — zero per-request messages. Caches larger
    /// than one frame are split: `bucket` / `buckets` identify the
    /// slice. A node with a small cache sends `buckets = 1`.
    CacheDigest {
        node_id: u64,
        generation: u64,
        /// Packed bloom bit vector (see [`crate::bloom`]).
        bits: Vec<u8>,
        nbits: u64,
        k: u32,
        n: u64,
        /// Index of this slice.
        bucket: u32,
        /// How many slices this sender currently uses.
        buckets: u32,
    },
    /// Add-only delta between snapshots. Removals are not sent: they
    /// raise FPR until the next [`Payload::CacheDigest`] for that
    /// bucket. `adds` may span buckets; the receiver routes each hash
    /// with the sender's `buckets` count.
    CacheDigestDelta {
        node_id: u64,
        generation: u64,
        /// Raw blake3 hashes newly inserted since the last snapshot.
        adds: Vec<[u8; 32]>,
        buckets: u32,
    },
    /// Direct request for one cached chunk. The payload is tiny; the
    /// bytes follow on the same stream (length-prefixed) if `found`.
    ChunkRequest {
        hash: [u8; 32],
    },
    ChunkResponse {
        hash: [u8; 32],
        status: ChunkStatus,
    },
    /// Plan 30 §M15: exact cache-membership heartbeat, gossiped every
    /// digest interval. A receiver whose mirror of `node_id` does not
    /// match starts a reconciliation session with it.
    CacheSummary {
        node_id: u64,
        summary: crate::reconcile::Summary,
    },
    /// Plan 30 §M15: one publish tick's exact adds and removes, chained
    /// by `seq` and by before/after root fingerprints.
    CacheSetDelta {
        node_id: u64,
        delta: crate::reconcile::Delta,
    },
    /// Plan 30 §M15: one reconciliation round, sent directly to the
    /// owner of the set being mirrored.
    ReconcileRequest {
        queries: Vec<crate::reconcile::Query>,
    },
    ReconcileReply {
        reply: crate::reconcile::Reply,
    },
    /// Non-holder asks the lease holder to validate and journal `op`
    /// (postcard-encoded `MutateOp` from constellation-meta).
    MutateRequest {
        part: String,
        requester: u64,
        req_id: u64,
        epoch_seen: u64,
        /// Postcard bytes of `constellation_meta::MutateOp`.
        op: Vec<u8>,
        /// Plan 30 §M2: this op's exactly-once identity. Stable across
        /// every retry of the same op (unlike `req_id`, a fresh
        /// per-attempt correlation id every time) — the holder keys its
        /// dedup/`recent`-outcome lookup on this, not on `req_id`. A
        /// plain `(u64, u32, u64)` here rather than importing
        /// `constellation_meta::Rid`: this crate stays free of the
        /// metadata dependency (see the module doc).
        rid: (u64, u32, u64),
        /// Plan 30 §M2 GC: the highest contiguous `rid.seq` of this
        /// requester's current incarnation whose reply it has already
        /// received. The holder drops `recent` outcomes for that
        /// requester's incarnation up to this seq — a request carries
        /// its own future GC receipt.
        acked_through: u64,
        /// Plan 30 §M11: postcard bytes of the requester's observed
        /// `constellation_meta::Position` (its causal dependencies).
        #[serde(default)]
        deps: Vec<u8>,
        /// The chunks a `SetManifest` op names that are still uploading
        /// on the requester (a `--write-mode back` close). The recipient
        /// enrolls them as pending uploads of its own, awaited from the
        /// requester, before it executes the op, so nothing naming them
        /// leaves it before they are in S3 (`meta::store::remote`).
        #[serde(default)]
        pending: Vec<[u8; 32]>,
        /// The log sequence the requester had applied when it sent this
        /// (chunk close-stall-followup): a reply whose base it covers is
        /// installed there at once, so the holder skips working out what
        /// the op's records wait for (`Core::own_chunks_for`).
        applied: u64,
    },
    /// Holder's answer: postcard-encoded `MutateOutcome`.
    MutateReply {
        req_id: u64,
        /// Postcard bytes of `constellation_meta::MutateOutcome`.
        outcome: Vec<u8>,
        /// Plan 30 M5 (§M6's first form): the log position the requester
        /// must have applied before it may install the reply's records
        /// ahead of the log — the holder's last shipped sequence when it
        /// evaluated the op — or `None` when the holder's unshipped
        /// journal already touched one of the op's keys (then only the
        /// log delivers the records, in their order).
        #[serde(default)]
        base: Option<u64>,
        /// Plan 30 §M6: the state the holder evaluated the op against —
        /// its shipped-through log sequence, and its unshipped journal
        /// position `(epoch, journal seq)` if it had one
        /// (`constellation_meta::Position`).
        #[serde(default)]
        position_seq: u64,
        #[serde(default)]
        position_pending: Option<(u64, u64)>,
        /// Plan 30 §M11: the position's per-stream part (`(gen, idx)`).
        #[serde(default)]
        position_streams: Vec<(u64, u64)>,
        /// Plan 30 §M11: the generation that executed the op (0: root).
        #[serde(default)]
        gen: u64,
        /// Whether the op's transaction waits for the requester's own
        /// pending chunks (`constellation_meta::OwnChunks::to_wire`: 0
        /// no, 1 yes but streamed to it past them, 2 yes and only its
        /// upload releases them), and of which inodes (`own_inos`).
        own_chunks: u8,
        own_inos: Vec<u64>,
        /// Chunk metered-own-rows: `constellation_meta::OwnRows::to_wire`
        /// of the requester's own transactions unshipped through the
        /// position (empty: not worked out).
        own_rows: Vec<u8>,
    },
    /// Plan 30 §M8: a `cto=strict` reader asks the sequencer where the
    /// state of `ino` is (its record; with `dir`, its entries; with
    /// `name`, that entry and its target) — `constellation_authority`'s
    /// `PeerMsg::ReadIndex`.
    ReadIndex {
        requester: u64,
        req_id: u64,
        ino: u64,
        dir: bool,
        name: Option<String>,
    },
    /// The sequencer's answer: `status` 0 = the position (and maybe a
    /// read delegation `(id, ttl_ms, epoch)`), 1 = not the holder
    /// (`holder`: whom it believes holds, 0 unknown), 2 = busy (fenced).
    ReadIndexReply {
        req_id: u64,
        status: u8,
        holder: u64,
        position_seq: u64,
        position_pending: Option<(u64, u64)>,
        grant: Option<(u64, u64, u64)>,
        #[serde(default)]
        position_streams: Vec<(u64, u64)>,
    },
    /// Plan 30 §M11: a delegate streams its transactions of generation
    /// `gen` to the root (`txs`: postcard of
    /// `Vec<constellation_meta::DelegateTx>`). Answered by
    /// [`Payload::DelegateStreamAck`].
    DelegateStream {
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    },
    DelegateStreamAck {
        req_id: u64,
        gen: u64,
        through: u64,
        refused: bool,
    },
    /// Plan 30 §M11: a delegate renews its grant on `gen`; `ttl_ms` 0
    /// refuses.
    DelegRenew {
        from: u64,
        req_id: u64,
        gen: u64,
        /// Phase 2b: the delegate's backup peer (0: none).
        #[serde(default)]
        backup: u64,
        /// Plan 30 §M14: the delegate's executed stream head for `gen`.
        #[serde(default)]
        stream_head: u64,
    },
    DelegRenewed {
        req_id: u64,
        gen: u64,
        ttl_ms: u64,
        /// Plan 30 §M14: postcard of the root's lock grants under the
        /// subtree (`Vec<constellation_meta::locks::Grant>`), handed over
        /// with the first renewal; empty otherwise.
        #[serde(default)]
        locks: Vec<u8>,
        /// Plan 30 §M14: the remaining lock grace on the subtree (ms; 0:
        /// none) — see `PeerMsg::DelegRenewed::lock_grace_ms`.
        #[serde(default)]
        lock_grace_ms: u64,
        /// Plan 30 §M14: postcard of the subtree's lock floor (a
        /// `constellation_meta::Position`; empty: none).
        #[serde(default)]
        lock_floor: Vec<u8>,
    },
    /// Plan 30 §M11: the root recalls generation `gen` on `dir`; the
    /// delegate stops and answers the highest stream index it executed.
    DelegRecall {
        root: u64,
        req_id: u64,
        dir: u64,
        gen: u64,
    },
    DelegRecalled {
        req_id: u64,
        gen: u64,
        through: u64,
        /// Plan 30 §M14: postcard of the delegate's lock grants under the
        /// subtree, handed back to the root.
        #[serde(default)]
        locks: Vec<u8>,
        /// Plan 30 §M14: postcard of the subtree's lock floor.
        #[serde(default)]
        lock_floor: Vec<u8>,
    },
    /// Plan 30 §M11 phase 2b: a delegate's append to its backup (postcard
    /// `Vec<DelegateTx>`), and the backup's contiguous hold (or `sealed`).
    DelegBackupAppend {
        from: u64,
        req_id: u64,
        gen: u64,
        txs: Vec<u8>,
    },
    DelegBackupAck {
        req_id: u64,
        gen: u64,
        acked: u64,
        sealed: bool,
    },
    /// Plan 30 §M11 phase 2b: the root asks a delegate's backup to seal
    /// generation `gen` and hand its tail over.
    DelegSeal {
        root: u64,
        req_id: u64,
        gen: u64,
    },
    DelegSealed {
        req_id: u64,
        gen: u64,
        sealed: bool,
        txs: Vec<u8>,
    },
    /// Plan 30 §M8: the sequencer recalls read delegation `grant` on
    /// `ino`; the delegate stops honouring it, then answers
    /// [`Payload::ReadRecalled`].
    ReadRecall {
        holder: u64,
        req_id: u64,
        ino: u64,
        grant: u64,
    },
    ReadRecalled {
        req_id: u64,
    },
    /// Plan 30 §M9: the holder streams whole journal transactions to a
    /// backup (`constellation_authority`'s `PeerMsg::BackupAppend`).
    /// `txs` is postcard of `Vec<constellation_meta::BackupTx>`; `from`
    /// is the journal seq the batch starts at (the seq after what the
    /// backup last acknowledged), `through` the holder's shipped-through
    /// seq. Answered by [`Payload::BackupAck`].
    BackupAppend {
        holder: u64,
        req_id: u64,
        epoch: u64,
        config_version: u64,
        from: u64,
        txs: Vec<u8>,
        through: u64,
    },
    /// Plan 30 §M9: the backup holds every row through `acked`, or has
    /// `sealed` the epoch (it will never acknowledge it again).
    BackupAck {
        req_id: u64,
        epoch: u64,
        acked: u64,
        sealed: bool,
    },
    /// Plan 30 §M9: pre-S3 streaming — backup-acked transactions
    /// (postcard of `Vec<BackupTx>`) evaluated against the log through
    /// `base`, for a subscriber to install ahead of the log. Answered
    /// with [`Payload::Ok`] (nothing to say).
    StreamAhead {
        from: u64,
        epoch: u64,
        base: u64,
        txs: Vec<u8>,
    },
    /// A reply with nothing to say.
    Ok {
        req_id: u64,
    },
    /// `from` has put `hashes` in S3: chunks it named in a forwarded
    /// manifest while they were still uploading ([`Payload::MutateRequest`]'s
    /// `pending`). The recipient acks the rows it awaits for them.
    /// Answered with [`Payload::Ok`].
    ChunksDurable {
        from: u64,
        hashes: Vec<[u8; 32]>,
    },
    /// Gossiped RTT vector for holder-driven lease placement.
    PeerRtts {
        node_id: u64,
        /// `(peer_id, rtt_ms)` samples.
        rtts: Vec<(u64, u16)>,
    },
    /// Holder offers the lease to a better-placed writer (placement).
    LeaseOffer {
        part: String,
        epoch: u64,
    },
    // ---- Plan 30 §M14: cross-node `flock`/`fcntl` (appended: postcard
    // numbers variants by position). The authority core's `PeerMsg::Lock*`
    // one for one; a grant id travels as `(node, seq)`, a mode as
    // `exclusive`.
    /// A node asks the owning sequencer of `ino` for a lock grant.
    /// Answered by [`Payload::LockReply`].
    LockRequest {
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
        blocking: bool,
        /// The requester's clock (unix ms) when it sent; echoed in a
        /// `LockGranted` push.
        sent: i64,
    },
    LockReply {
        req_id: u64,
        outcome: LockOutcomeWire,
    },
    /// One way: a parked request's grant, pushed (`sent`: the requester's
    /// clock at its last send, echoed).
    LockGranted {
        from: u64,
        ino: u64,
        sent: i64,
        outcome: LockOutcomeWire,
    },
    /// The owner recalls `grant`; answered by [`Payload::LockRecalled`]
    /// on receipt (the release follows as [`Payload::LockReleased`]).
    LockRecall {
        owner: u64,
        req_id: u64,
        ino: u64,
        grant: (u64, u64),
    },
    LockRecalled {
        req_id: u64,
    },
    /// One way: the holder released `grant`. `position`: what its
    /// clients had seen or been acknowledged (a postcard
    /// `constellation_meta::Position`), which the next grant carries.
    LockReleased {
        from: u64,
        ino: u64,
        grant: (u64, u64),
        #[serde(default)]
        position: Vec<u8>,
    },
    /// Renew grants at their owner; answered by [`Payload::LockRenewed`].
    LockRenew {
        from: u64,
        req_id: u64,
        entries: Vec<LockRenewWire>,
    },
    LockRenewed {
        req_id: u64,
        results: Vec<(u64, (u64, u64), LockRenewResultWire)>,
    },
    /// One way: the holder's grant table mirrored to a backup (`grants`:
    /// postcard of `Vec<constellation_meta::locks::Grant>`).
    LockMirror {
        from: u64,
        ver: u64,
        grants: Vec<u8>,
        /// Postcard of every lock floor the holder knows, joined.
        #[serde(default)]
        floor: Vec<u8>,
    },
    /// `getlk`: is a conflicting grant held elsewhere? Answered by
    /// [`Payload::LockTestReply`].
    LockTest {
        requester: u64,
        req_id: u64,
        ino: u64,
        exclusive: bool,
    },
    LockTestReply {
        req_id: u64,
        outcome: LockTestOutcomeWire,
    },
    /// EC2 finding 1: `requester` cannot reach S3 but must make chunks
    /// durable there before its close may publish a manifest naming them
    /// (every chunk a segment references is in S3 first). The recipient,
    /// a peer that can, fetches each of `hashes` from the requester
    /// (an ordinary [`Payload::ChunkRequest`], which the requester serves
    /// although the chunks are still dirty there), verifies it, uploads
    /// it, and answers [`Payload::ChunkHandoffReply`] once every one is
    /// in S3.
    ChunkHandoff {
        requester: u64,
        req_id: u64,
        hashes: Vec<[u8; 32]>,
    },
    /// Answer to [`Payload::ChunkHandoff`]: whether every chunk is now
    /// durable in S3.
    ChunkHandoffReply {
        req_id: u64,
        uploaded: bool,
    },
    // ---- Plan 32 Step 0.1: snapshot rows at the root-lease holder
    // (appended: postcard encodes the variant index, so a new variant
    // anywhere but the end would renumber every one after it) ----
    /// A non-holder asks the root-lease holder to execute a snapshot
    /// batch: every snapshot row write (create, delete, hold) goes
    /// through here, so taking, deleting or holding a snapshot never
    /// moves the write lease. Answered by [`Payload::SnapshotBatchReply`].
    SnapshotBatchRequest {
        requester: u64,
        req_id: u64,
        /// The batch's exactly-once identity (`constellation_meta::Rid`
        /// as a tuple, as in [`Payload::MutateRequest`]), allocated once
        /// and kept across every retry: the holder answers a duplicate
        /// from the results it kept, without executing it again.
        rid: (u64, u32, u64),
        items: Vec<SnapshotItem>,
    },
    SnapshotBatchReply {
        req_id: u64,
        outcome: SnapshotBatchOutcome,
    },
    // ---- Plan 37 §8 (37-k5a): a holder replaced on its own state dir
    // (appended, as above) ----
    /// The lease holder `holder` is about to be replaced by a successor
    /// on its own state dir (an engine-pod handoff): the same node, lease
    /// and epoch, silent while the successor starts. A backup of `holder`
    /// at `epoch` counts no silence for `for_ms` (capped by the
    /// recipient) instead of sealing the epoch after its usual budget;
    /// the successor's first append ends the hold. Answered with
    /// [`Payload::Ok`].
    BackupHold {
        holder: u64,
        epoch: u64,
        for_ms: u64,
    },
}

/// Plan 30 §M14: `constellation_authority::LockOutcome` on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockOutcomeWire {
    Granted {
        grant: (u64, u64),
        exclusive: bool,
        ttl_ms: u64,
        position_seq: u64,
        position_pending: Option<(u64, u64)>,
        position_streams: Vec<(u64, u64)>,
    },
    Waiting {
        retry_ms: u64,
    },
    WouldBlock,
    NotOwner {
        owner: u64,
    },
    Busy,
}

/// Plan 30 §M14: one `constellation_authority::LockRenewEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockRenewWire {
    pub ino: u64,
    pub grant: (u64, u64),
    pub exclusive: bool,
}

/// Plan 30 §M14: `constellation_authority::LockRenewResult` on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockRenewResultWire {
    /// `id`/`exclusive`: the grant the owner holds for the node (a newer
    /// one than asked about when its reply was lost).
    Ok {
        ttl_ms: u64,
        recalled: bool,
        id: (u64, u64),
        exclusive: bool,
    },
    Lost,
    NotOwner {
        owner: u64,
    },
}

/// Plan 30 §M14: `constellation_authority::LockTestOutcome` on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockTestOutcomeWire {
    Free,
    Held { node: u64, exclusive: bool },
    NotOwner { owner: u64 },
}

/// Plan 30 §M10: a member's lease claim in an [`Payload::EpochAck`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochClaim {
    pub epoch: u64,
    pub expires_unix_ms: i64,
    pub may_carry: bool,
}

/// Plan 30 §M10: the lease a continuation epoch carries, as its holder
/// read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochCarrier {
    pub node: u64,
    pub epoch: u64,
    pub expires_unix_ms: i64,
}

/// Why a holder did not serve a requested chunk. The requester needs
/// the distinction to account precisely: `Absent` for a chunk it was
/// told the holder has is a false-positive peer fetch; a chunk the
/// holder dropped moments ago is only a propagation race.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkDecline {
    /// Serving budget exhausted, cooperative cache off, or a local read
    /// error: nothing to learn about membership.
    Busy,
    /// The holder does not have it and has not dropped it recently.
    Absent,
    /// The holder dropped it (evicted, or it became dirty) within its
    /// recent-removal window, so a requester's view may not have caught
    /// up yet.
    RecentlyRemoved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    /// The chunk body follows on the stream.
    Found,
    Declined(ChunkDecline),
}

/// Bytes a [`Signed`] envelope adds around a payload's postcard body:
/// author key, 64-byte signature and their length prefixes.
pub const SIGNED_ENVELOPE: usize = 32 + 1 + 64 + 3;

/// Approximate on-the-wire size of `payload` once signed, for the
/// digest-plane byte counters. Exact postcard body plus the fixed
/// envelope; gossip fan-out and QUIC framing are not included.
pub fn wire_len(payload: &Payload) -> usize {
    postcard::to_allocvec(payload).map(|v| v.len()).unwrap_or(0) + SIGNED_ENVELOPE
}

/// A payload plus its author and signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signed {
    /// Author's raw public key (also the iroh endpoint id).
    pub from: [u8; 32],
    /// Ed25519 signature over `body`.
    sig: Vec<u8>,
    /// Exact postcard bytes that were signed.
    body: Vec<u8>,
}

impl Signed {
    pub fn new(key: &SecretKey, payload: &Payload) -> Result<Self> {
        let body = postcard::to_allocvec(payload)?;
        let sig = key.sign(&body);
        Ok(Self {
            from: *key.public().as_bytes(),
            sig: sig.to_bytes().to_vec(),
            body,
        })
    }

    /// Verify the signature and return the payload. The author is
    /// [`Signed::from`]; the caller still has to decide whether that
    /// key is allowed (see [`crate::allowlist`]).
    pub fn verify(&self) -> Result<(PublicKey, Payload)> {
        let key = PublicKey::from_bytes(&self.from)?;
        let sig: [u8; 64] = self
            .sig
            .as_slice()
            .try_into()
            .context("signature must be 64 bytes")?;
        key.verify(&self.body, &iroh::Signature::from_bytes(&sig))
            .context("bad signature on peer message")?;
        Ok((key, postcard::from_bytes(&self.body)?))
    }

    /// Bare postcard envelope used by gossip and inside stream framing.
    pub fn encode_bare(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Length-prefixed frame: 4-byte big-endian length, then postcard.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bare = self.encode_bare()?;
        anyhow::ensure!(bare.len() <= MAX_FRAME, "message too large: {}", bare.len());
        let mut out = Vec::with_capacity(4 + bare.len());
        out.extend_from_slice(&(bare.len() as u32).to_be_bytes());
        out.extend_from_slice(&bare);
        Ok(out)
    }

    /// Decode exactly one bare message. Trailing bytes and a signature
    /// that is not Ed25519-sized are refused: `postcard::from_bytes`
    /// ignores trailing input, which let a length-prefixed frame decode
    /// as a bare message whenever the prefix happened to parse (about 1
    /// in 20 random keys) — the silent framing mismatch
    /// `stream_framing_and_bare_encoding_are_distinct` exists to catch.
    pub fn decode(frame: &[u8]) -> Result<Self> {
        let (signed, rest): (Signed, _) = postcard::take_from_bytes(frame)?;
        anyhow::ensure!(
            rest.is_empty(),
            "{} trailing bytes after a message",
            rest.len()
        );
        anyhow::ensure!(signed.sig.len() == 64, "signature must be 64 bytes");
        Ok(signed)
    }
}

/// Read one length-prefixed frame.
pub async fn read_frame<R>(r: &mut R) -> Result<Signed>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    anyhow::ensure!(len <= MAX_FRAME, "peer announced a {len}-byte frame");
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Signed::decode(&buf)
}

/// Write one length-prefixed frame.
pub async fn write_frame<W>(w: &mut W, msg: &Signed) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    w.write_all(&msg.encode()?).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SecretKey {
        SecretKey::generate()
    }

    #[test]
    fn sign_verify_roundtrip() {
        let k = key();
        let payload = Payload::SegmentPublished {
            part: "p0".into(),
            seq: 7,
            epoch: 3,
        };
        let signed = Signed::new(&k, &payload).unwrap();
        let (author, got) = signed.verify().unwrap();
        assert_eq!(author, k.public());
        assert_eq!(got, payload);
    }

    /// A tampered body must not verify: gossip is forwarded by third
    /// parties, so the transport peer is not necessarily the author.
    #[test]
    fn tampered_body_is_rejected() {
        let k = key();
        let mut signed = Signed::new(
            &k,
            &Payload::LeaseRequest {
                part: "p0".into(),
                requester: 1,
                epoch_applied: None,
            },
        )
        .unwrap();
        signed.body[0] ^= 1;
        assert!(signed.verify().is_err(), "modified body must not verify");
    }

    /// Re-signing someone else's payload under our own key changes the
    /// author, so a relay cannot impersonate the original sender.
    #[test]
    fn signature_from_another_key_is_rejected() {
        let (a, b) = (key(), key());
        let payload = Payload::Ping { node_id: 1 };
        let real = Signed::new(&a, &payload).unwrap();
        let forged = Signed {
            from: *b.public().as_bytes(),
            ..real
        };
        assert!(forged.verify().is_err(), "author/signature mismatch");
    }

    #[test]
    fn frame_roundtrip_over_a_duplex() {
        let k = key();
        let msg = Signed::new(
            &k,
            &Payload::Pong {
                node_id: 42,
                s3_ok: true,
            },
        )
        .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (mut a, mut b) = tokio::io::duplex(4096);
            write_frame(&mut a, &msg).await.unwrap();
            let got = read_frame(&mut b).await.unwrap();
            assert_eq!(
                got.verify().unwrap().1,
                Payload::Pong {
                    node_id: 42,
                    s3_ok: true,
                }
            );
        });
    }

    /// Gossip carries whole datagrams while streams need framing, so the
    /// two encodings differ. A prefixed frame must NOT decode as a bare
    /// gossip payload — the mismatch was silent and disabled push
    /// invalidation entirely (every peer fell back to polling).
    #[test]
    fn stream_framing_and_bare_encoding_are_distinct() {
        let k = key();
        let payload = Payload::SegmentPublished {
            part: "p0".into(),
            seq: 3,
            epoch: 1,
        };
        let signed = Signed::new(&k, &payload).unwrap();
        let bare = signed.encode_bare().unwrap();
        let framed = signed.encode().unwrap();
        assert_eq!(framed.len(), bare.len() + 4, "frame adds a length prefix");
        // The bare form is what gossip receivers decode.
        assert_eq!(Signed::decode(&bare).unwrap().verify().unwrap().1, payload);
        // The framed form must not be mistaken for it. What a receiver
        // does is decode *then verify*; decode alone refuses almost every
        // frame structurally, and the signature check closes the rest
        // (a shifted `from`/`sig` pair cannot verify).
        assert!(
            Signed::decode(&framed)
                .and_then(|signed| signed.verify())
                .is_err(),
            "a length-prefixed frame must not be accepted as a bare payload"
        );
    }

    /// Plan 32 Step 0.1: the snapshot batch variants are appended after
    /// every existing one (postcard encodes the variant index, so the
    /// earlier variants keep theirs), and both round-trip a signature.
    #[test]
    fn snapshot_batch_variants_are_appended_and_round_trip() {
        let tag = |payload: &Payload| postcard::to_allocvec(payload).unwrap()[0];
        let last_before = tag(&Payload::ChunkHandoffReply {
            req_id: 0,
            uploaded: false,
        });
        let request = Payload::SnapshotBatchRequest {
            requester: 2,
            req_id: 9,
            rid: (2, u32::MAX, 1 << 32 | 7),
            items: vec![
                SnapshotItem::Create {
                    path: "/vol".into(),
                    name: "auto-20260928T1405Z".into(),
                    origin: 1,
                    policy_ino: 42,
                    creator: 2,
                    held: false,
                    held_by: None,
                    skip_if_unchanged_since: Some("mtree:3:00:42".into()),
                },
                SnapshotItem::Delete {
                    id: "abc".into(),
                    force: false,
                },
                SnapshotItem::Hold {
                    id: "abc".into(),
                    held: true,
                    by: Some("user:op".into()),
                    force: false,
                },
            ],
        };
        let reply = Payload::SnapshotBatchReply {
            req_id: 9,
            outcome: SnapshotBatchOutcome::Done(vec![
                SnapshotItemResult::Created {
                    id: "abc".into(),
                    seq: 4,
                    root_hash: "mtree:4:00:42".into(),
                    row: SnapshotRowWire {
                        id: "abc".into(),
                        held_by: Some("csi:x".into()),
                        refer_bytes: Some(9),
                        ..Default::default()
                    },
                },
                SnapshotItemResult::Skipped,
                SnapshotItemResult::AlreadyExists { id: "abc".into() },
                SnapshotItemResult::Deleted,
                SnapshotItemResult::NotFound,
                SnapshotItemResult::HoldSet {
                    row: SnapshotRowWire::default(),
                },
                SnapshotItemResult::Refused {
                    reason: "held".into(),
                },
                SnapshotItemResult::DeletedObjectRemains {
                    reason: "503".into(),
                },
                SnapshotItemResult::Held {
                    reason: "held by csi:x".into(),
                },
            ]),
        };
        assert_eq!(tag(&request), last_before + 1);
        assert_eq!(tag(&reply), last_before + 2);
        let k = key();
        for payload in [request, reply] {
            let signed = Signed::new(&k, &payload).unwrap();
            assert_eq!(signed.verify().unwrap().1, payload);
        }
    }

    /// Plan 32 Step 7.6: the largest snapshot delete batch a requester
    /// sends, and the largest reply a holder sends for it (every item
    /// refused with a reason far past the clip), both fit a frame.
    #[test]
    fn maximum_snapshot_delete_batch_fits_a_frame() {
        let request = Payload::SnapshotBatchRequest {
            requester: u64::MAX,
            req_id: u64::MAX,
            rid: (u64::MAX, u32::MAX, u64::MAX),
            items: (0..MAX_SNAPSHOT_DELETES_PER_BATCH)
                .map(|_| SnapshotItem::Delete {
                    id: "f".repeat(64),
                    force: true,
                })
                .collect(),
        };
        let n = Signed::new(&key(), &request)
            .unwrap()
            .encode()
            .unwrap()
            .len();
        assert!(n <= MAX_FRAME, "{n}");

        let mut outcome = SnapshotBatchOutcome::Done(
            (0..MAX_SNAPSHOT_DELETES_PER_BATCH)
                .map(|i| {
                    // Multi-byte chars, so the clip must find a boundary.
                    let reason = "é/".repeat(4096);
                    match i % 2 {
                        0 => SnapshotItemResult::Refused { reason },
                        _ => SnapshotItemResult::Held { reason },
                    }
                })
                .collect(),
        );
        outcome.clip_reasons();
        let SnapshotBatchOutcome::Done(results) = &outcome else {
            unreachable!()
        };
        for result in results {
            let (SnapshotItemResult::Refused { reason } | SnapshotItemResult::Held { reason }) =
                result
            else {
                unreachable!()
            };
            assert!(
                reason.len() <= MAX_SNAPSHOT_REASON_BYTES,
                "{}",
                reason.len()
            );
            assert!(reason.ends_with('…'));
        }
        let reply = Payload::SnapshotBatchReply {
            req_id: u64::MAX,
            outcome,
        };
        let n = Signed::new(&key(), &reply).unwrap().encode().unwrap().len();
        assert!(n <= MAX_FRAME, "{n}");

        // A short reason is left alone.
        let mut short = SnapshotBatchOutcome::Failed("drain failed".into());
        short.clip_reasons();
        assert_eq!(short, SnapshotBatchOutcome::Failed("drain failed".into()));
    }

    #[test]
    fn oversized_frame_is_refused() {
        let k = key();
        let msg = Signed::new(
            &k,
            &Payload::SegmentPublished {
                part: "p".repeat(MAX_FRAME),
                seq: 1,
                epoch: 1,
            },
        )
        .unwrap();
        assert!(msg.encode().is_err(), "must refuse to send a huge frame");
    }

    #[test]
    fn a_full_bloom_bucket_fits_the_real_gossip_budget() {
        let bits = vec![0xa5; crate::bloom::MAX_BITS_BYTES];
        let msg = Signed::new(
            &key(),
            &Payload::CacheDigest {
                node_id: 1,
                generation: 2,
                nbits: (bits.len() * 8) as u64,
                k: crate::bloom::K,
                n: crate::bloom::ENTRIES_PER_BUCKET as u64,
                bits,
                bucket: 127,
                buckets: 128,
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "full digest is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    #[test]
    fn a_maximum_delta_batch_fits_the_real_gossip_budget() {
        let msg = Signed::new(
            &key(),
            &Payload::CacheDigestDelta {
                node_id: 1,
                generation: 2,
                adds: vec![[7; 32]; MAX_GOSSIP_DELTA_ADDS],
                buckets: 128,
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "maximum delta is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    /// A delta of [`crate::reconcile::MAX_DELTA_KEYS`] worst-case keys
    /// (10-byte varints) must fit one gossip frame.
    #[test]
    fn a_maximum_exact_delta_fits_the_real_gossip_budget() {
        use crate::reconcile::{Delta, MAX_DELTA_KEYS};
        // Real gaps between 2048 sorted random keys are ~8-byte varints;
        // charge every key the 10-byte maximum.
        let mut worst = Vec::with_capacity(MAX_DELTA_KEYS * 10);
        for _ in 0..MAX_DELTA_KEYS {
            worst.extend_from_slice(&[0xff; 9]);
            worst.push(0x01);
        }
        let msg = Signed::new(
            &key(),
            &Payload::CacheSetDelta {
                node_id: u64::MAX,
                delta: Delta {
                    incarnation: u64::MAX,
                    seq: u64::MAX,
                    base: [0xff; 16],
                    root: [0xff; 16],
                    adds: worst,
                    removes: Vec::new(),
                },
            },
        )
        .unwrap();
        let n = msg.encode_bare().unwrap().len();
        assert!(
            n <= GOSSIP_CONTENT_LIMIT,
            "maximum exact delta is {n} bytes, content limit is {GOSSIP_CONTENT_LIMIT}"
        );
    }

    /// The largest request and reply a session can produce must fit a
    /// direct-stream frame, or the round silently fails on the wire.
    #[test]
    fn maximum_reconcile_frames_fit_a_stream_frame() {
        use crate::reconcile::{
            respond, KeySet, Query, Range, Summary, MAX_QUERIES_PER_REQUEST, REPLY_BUDGET,
        };
        let queries: Vec<Query> = (0..MAX_QUERIES_PER_REQUEST as u64)
            .map(|i| Query {
                range: Range {
                    bits: 60,
                    path: (1u64 << 60) - 1 - i,
                },
                fp: [0xff; 16],
                count: u64::MAX,
            })
            .collect();
        let req = Signed::new(&key(), &Payload::ReconcileRequest { queries }).unwrap();
        assert!(req.encode().is_ok(), "maximum request exceeds MAX_FRAME");

        // A dense owner answering "send everything" pages at the budget.
        let owner =
            KeySet::from_keys((0..200_000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        let empty = KeySet::new();
        let queries: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = empty.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: 0,
                }
            })
            .collect();
        let summary = Summary {
            incarnation: u64::MAX,
            seq: u64::MAX,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let reply = respond(&owner, summary, &queries, REPLY_BUDGET);
        let msg = Signed::new(&key(), &Payload::ReconcileReply { reply }).unwrap();
        let n = msg.encode().expect("maximum reply exceeds MAX_FRAME").len();
        assert!(n <= MAX_FRAME, "{n}");

        // And a reply made only of child splits.
        let mirror = KeySet::from_keys((0..200_000u64).map(|i| i.wrapping_mul(31)));
        let queries: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = mirror.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: a.count.max(1),
                }
            })
            .collect();
        let reply = respond(&owner, summary, &queries, REPLY_BUDGET);
        let msg = Signed::new(&key(), &Payload::ReconcileReply { reply }).unwrap();
        assert!(
            msg.encode().is_ok(),
            "children-only reply exceeds MAX_FRAME"
        );
    }

    #[test]
    fn lock_payloads_round_trip() {
        let k = key();
        for payload in [
            Payload::LockRequest {
                requester: 1,
                req_id: 2,
                ino: 3,
                exclusive: true,
                blocking: false,
                sent: 7,
            },
            Payload::LockReply {
                req_id: 2,
                outcome: LockOutcomeWire::Granted {
                    grant: (4, 5),
                    exclusive: false,
                    ttl_ms: 5000,
                    position_seq: 6,
                    position_pending: Some((7, 8)),
                    position_streams: vec![(9, 10)],
                },
            },
            Payload::LockRenewed {
                req_id: 2,
                results: vec![(
                    3,
                    (4, 5),
                    LockRenewResultWire::Ok {
                        ttl_ms: 1,
                        recalled: true,
                        id: (4, 5),
                        exclusive: true,
                    },
                )],
            },
            Payload::LockTestReply {
                req_id: 2,
                outcome: LockTestOutcomeWire::Held {
                    node: 1,
                    exclusive: true,
                },
            },
            Payload::DelegRenewed {
                req_id: 1,
                gen: 2,
                ttl_ms: 3,
                locks: vec![1, 2, 3],
                lock_grace_ms: 4,
                lock_floor: vec![5],
            },
        ] {
            let signed = Signed::new(&k, &payload).unwrap();
            let frame = signed.encode().unwrap();
            let back = Signed::decode(&frame[4..]).unwrap();
            assert_eq!(back.verify().unwrap().1, payload);
        }
    }

    #[test]
    fn wire_len_tracks_the_signed_encoding() {
        let payload = Payload::Ping { node_id: 7 };
        let real = Signed::new(&key(), &payload)
            .unwrap()
            .encode_bare()
            .unwrap()
            .len();
        let est = wire_len(&payload);
        assert!(est.abs_diff(real) <= 8, "estimate {est} vs real {real}");
    }
}

/// Plan 32 Step 0.1: one snapshot row operation of a
/// [`Payload::SnapshotBatchRequest`]. Plain data, so the engine's
/// executor and the wire share one definition
/// (`constellation_engine::snapshot_batch` re-exports it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotItem {
    /// Take `path@name`. `origin`/`policy_ino`/`creator`/`held`/`held_by`
    /// are the row's plan 32 §0.4 fields; `creator` is the node that
    /// asked, not the holder that executes.
    Create {
        path: String,
        name: String,
        origin: u8,
        policy_ino: u64,
        creator: u64,
        held: bool,
        held_by: Option<String>,
        /// An encoded `SnapshotRoot` (`mtree:<seq>:<root>:<ino>`): skip
        /// the snapshot when the subtree has not changed since that one
        /// (plan 32 Step 3.4).
        skip_if_unchanged_since: Option<String>,
    },
    /// Delete snapshot `id`; a held one only with `force`.
    Delete { id: String, force: bool },
    /// Set or release snapshot `id`'s retention hold, under the owner
    /// rule of plan 32 §0.4 (`force` overrides it).
    Hold {
        id: String,
        held: bool,
        by: Option<String>,
        force: bool,
    },
}

/// Plan 32 Step 0.1: one item's result, in item order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotItemResult {
    /// The snapshot exists now: its `snaps/` object and its row. `row`
    /// is the row as recorded, so a requester need not wait for its
    /// replica to hear of it.
    Created {
        id: String,
        seq: u64,
        root_hash: String,
        row: SnapshotRowWire,
    },
    /// The subtree did not change since `skip_if_unchanged_since`:
    /// nothing written.
    Skipped,
    /// The `snaps/` object already exists (the name is taken — by this
    /// same batch on an earlier holder, when a retry crossed a holder
    /// change). Nothing written.
    AlreadyExists {
        id: String,
    },
    Deleted,
    NotFound,
    /// The hold row as now recorded.
    HoldSet {
        row: SnapshotRowWire,
    },
    /// Refused or failed; nothing (more) written for this item.
    Refused {
        reason: String,
    },
    /// A delete removed the row, but deleting its `snaps/` object
    /// failed: the snapshot is gone from every listing, and the object
    /// is an orphan (plan 32 §0.3's reconciliation removes it). The
    /// caller reports `reason` as an error, as before batches.
    DeletedObjectRemains {
        reason: String,
    },
    /// A delete without `force` refused because the snapshot is held
    /// (plan 32 Step 5); `reason` is the message for a person. Its own
    /// variant so a caller (plan 32 Step 4's expiry, which counts a hold
    /// that won the race rather than failing) never parses `reason`.
    Held {
        reason: String,
    },
}

/// `constellation_meta::SnapshotRow` on the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRowWire {
    pub id: String,
    pub path: String,
    pub name: String,
    pub root_hash: String,
    pub created_unix_ms: i64,
    pub origin: u8,
    pub policy_ino: u64,
    pub held: bool,
    pub creator: u64,
    pub held_by: Option<String>,
    pub refer_bytes: Option<u64>,
}

/// Plan 32 Step 0.1: how a holder answered a snapshot batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotBatchOutcome {
    /// Executed (now, or earlier under the same rid): per-item results.
    Done(Vec<SnapshotItemResult>),
    /// This node does not hold the root lease; nothing executed.
    NotHolder,
    /// The batch could not run (its drain or its publish failed);
    /// nothing executed. The requester may retry under the same rid.
    Failed(String),
}

/// The most snapshot deletes one [`Payload::SnapshotBatchRequest`]
/// carries. A delete item is a fixed 67 bytes on the wire (a 64-hex id),
/// so a `snapshot delete <range>` of thousands of snapshots would
/// overflow [`MAX_FRAME`] in one batch: a requester splits its deletes
/// into batches of at most this many (plan 32 Step 7.6). Together with
/// [`MAX_SNAPSHOT_REASON_BYTES`] it bounds the reply as well; the tests
/// pin both under the frame.
pub const MAX_SNAPSHOT_DELETES_PER_BATCH: usize = 256;

/// The longest reason a forwarded [`Payload::SnapshotBatchReply`]
/// carries per item. A refusal names the snapshot's path, which has no
/// useful bound, so a reply of 256 refusals could otherwise outgrow
/// [`MAX_FRAME`] after the holder had already executed the batch.
pub const MAX_SNAPSHOT_REASON_BYTES: usize = 192;

impl SnapshotBatchOutcome {
    /// Clip every reason to [`MAX_SNAPSHOT_REASON_BYTES`] (on a char
    /// boundary, marked with `…`) before the outcome goes on the wire.
    pub fn clip_reasons(&mut self) {
        match self {
            Self::Done(results) => {
                for result in results {
                    if let SnapshotItemResult::Refused { reason }
                    | SnapshotItemResult::DeletedObjectRemains { reason }
                    | SnapshotItemResult::Held { reason } = result
                    {
                        clip_reason(reason);
                    }
                }
            }
            Self::Failed(reason) => clip_reason(reason),
            Self::NotHolder => {}
        }
    }
}

fn clip_reason(reason: &mut String) {
    const MARK: &str = "…";
    if reason.len() <= MAX_SNAPSHOT_REASON_BYTES {
        return;
    }
    let mut end = MAX_SNAPSHOT_REASON_BYTES - MARK.len();
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason.truncate(end);
    reason.push_str(MARK);
}
