//! The daemon's IO driver for the sans-IO authority core (plan 30 M5
//! phase 2): what `node_runtime`'s sync loop, `lease.rs`'s keeper,
//! `forward.rs`, `recovery.rs`'s drain, `shipper.rs`'s round and
//! `inbox.rs`'s runtime used to be, minus every decision — those live in
//! `constellation_authority::Core` and are shared with the simulation
//! (`crates/authority/tests/sim`).
//!
//! The shape is the simulation's `tests/sim/node.rs`: one task owns the
//! core and the replica; every `SyncRequest` from FUSE, the control
//! socket, the P2P bridge or a background ticker becomes an
//! [`Event`]; every [`Action`] the core returns is carried out *after*
//! `Core::handle` returned, by spawning the IO (S3, P2P, uploads,
//! publishes) or by answering a channel. Nothing in this file awaits
//! while holding the core, and the core never awaits at all — the
//! "no await inside the sync loop" invariant of plan 30 M2b/M3a is now
//! the type of `Core::handle`.
//!
//! The one writer outside the core is the FUSE fast path
//! (`LeaseView::admit` + a local `execute_mutate`), kept for its cost
//! (no channel hop per local write). It is fenced by the view mirror the
//! driver refreshes after every event *before* dispatching the event's
//! actions, plus the driver's check before a release CAS
//! (`wait_quiescent` and a journal re-read under the mirrored releasing
//! flag): a write admitted before the flag is either shipped by the
//! flush or fails the release, never stranded behind it.

use crate::lease::LeaseView;
use crate::sync::{AcquireProgress, HandoffResult, SyncRequest};
use anyhow::{Context, Result};
use constellation_authority::action::ControlOk;
use constellation_authority::core::JobKind;
use constellation_authority::{
    Action, CasFailure, ClientReply, Config, Control, Core, EpochState, Event, HonourMs, InboxView,
    LockAnswer, LockOutcome, LockRenewResult, LockTestAnswer, LockTestOutcome, Ms, NodeId, Now,
    OpId, PeerLink, PeerMsg, Policy, ReadAnswer, ReadGrantMsg, ReadIndexOutcome, S3Failure, S3Op,
    S3Result, ShipState, Stats, TimerId, TimerKind, UploadResult, WaitMs,
};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::locks::GrantId;
use constellation_meta::{JournalPos, Meta, MutateOp, MutateOutcome, OwnChunks, Position, Rid};
use constellation_net::{LogEvent, Payload};
use constellation_store_s3::inbox::InboxStore;
use constellation_store_s3::log::PARTITION;
use constellation_store_s3::{
    ChunkStore, CompressionSetting, LeaseMode, LeaseStore, LogStore, StoreError,
};
use constellation_types::Code;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// A core step (handle + refresh + dispatch) longer than this is logged:
/// nothing else on the node's authority path — a holder's backup
/// heartbeats included — runs meanwhile (a third of the default 1.5 s
/// seal window).
const SLOW_STEP_US: u64 = 500_000;

/// What a forwarded mutation's reply carries back to the bridge: the
/// outcome, plan 30 §M6's `base`, the position and (§M11) the executing
/// delegation generation (0: the root).
pub type MutateReplyParts = (
    MutateOutcome,
    Option<u64>,
    Position,
    u64,
    (OwnChunks, Option<constellation_meta::OwnRows>),
);

/// How often [`StepProfile`] is logged.
const STEP_PROFILE_EVERY: Duration = Duration::from_secs(5);

/// The core's steps since the last report: per event kind, how many and
/// how long; and the deepest each lane got. Logged at debug (`core step
/// profile`): a node whose core falls behind shows here what it was busy
/// with, and which lane waited.
#[derive(Debug, Default)]
struct StepProfile {
    since: Option<Instant>,
    kinds: HashMap<&'static str, (u64, u64, u64)>,
    max_pending: (usize, usize, usize),
}

impl StepProfile {
    fn note(&mut self, kind: &'static str, us: u64, int: usize, sync: usize, urgent: usize) {
        let since = *self.since.get_or_insert_with(Instant::now);
        let k = self.kinds.entry(kind).or_default();
        k.0 += 1;
        k.1 += us;
        k.2 = k.2.max(us);
        let m = &mut self.max_pending;
        *m = (m.0.max(int), m.1.max(sync), m.2.max(urgent));
        if since.elapsed() < STEP_PROFILE_EVERY {
            return;
        }
        let mut kinds: Vec<_> = self.kinds.drain().collect();
        kinds.sort_by_key(|(_, (_, total, _))| std::cmp::Reverse(*total));
        let busy_ms: u64 = kinds.iter().map(|(_, (_, total, _))| total).sum::<u64>() / 1000;
        let top: Vec<String> = kinds
            .iter()
            .take(8)
            .map(|(kind, (n, total, max))| {
                format!("{kind}:{n}/{}ms/max{}ms", total / 1000, max / 1000)
            })
            .collect();
        tracing::debug!(
            window_ms = since.elapsed().as_millis() as u64,
            busy_ms,
            max_int_pending = m.0,
            max_sync_pending = m.1,
            max_urgent_pending = m.2,
            top = ?top,
            "core step profile"
        );
        *self = Self::default();
    }
}

/// Plan 30 §M14: an owner's answer to a `LockRenew`.
pub type LockRenewResults = Vec<(constellation_fs_core::Ino, GrantId, LockRenewResult)>;

/// The core's observable state, refreshed after every event, for
/// `status` and the background tickers (placement, atime, prune).
#[derive(Debug, Clone, Default)]
pub struct CoreStatus {
    pub stats: Stats,
    pub ship: Option<ShipState>,
    pub lease: constellation_control::proto::types::LeaseStatus,
    pub gate_pending: bool,
    pub job: Option<JobKind>,
    pub clients_in_flight: usize,
    pub inbox: InboxView,
    /// Refused replays whose conflict copy is pending / stalled.
    pub copies: (u64, u64),
    pub rounds_completed: u64,
    pub last_error: Option<String>,
    pub epoch: EpochState,
    /// Plan 30 §M7.
    pub stream: constellation_authority::core::StreamView,
    pub stream_enabled: bool,
    /// Plan 30 §M8.
    pub read: constellation_authority::core::ReadView,
    pub read_delegations: bool,
    /// Plan 30 §M9.
    pub ack: constellation_authority::core::AckView,
    pub ack_s3: bool,
    /// Plan 30 §M11.
    pub delegation: constellation_authority::core::DelegView,
    pub delegation_enabled: bool,
    /// Ops the FUSE fast path executed as the delegate (`DelegateView`).
    pub delegation_fast_path_executed: u64,
    /// Plan 30 §M12: ops the FUSE fast path sent through the core because
    /// a live delegation owned their keys.
    pub delegation_fast_path_routed: u64,
    /// Ops the delegate's FUSE fast path sent through the core because its
    /// generation ended at this node's takeover gate.
    pub delegation_fast_path_fenced: u64,
    /// Phase 2b: the placement's busiest subtrees, `(dir, node, node_ops,
    /// subtree_ops)`.
    pub placement_top: Vec<(u64, u64, u64, u64)>,
    /// Plan 30 §M14: `--locks cluster`, and the core's lock requests,
    /// parked waiters and recalls in flight.
    pub locks_cluster: bool,
    pub lock_requests_in_flight: usize,
    pub lock_waiters: usize,
    pub lock_recalls_in_flight: usize,
    /// EC2 campaign 8 A-1.
    pub own_s3: constellation_authority::core::OwnS3,
    /// The peers whose link the core sees connected (`startup-link-lag`).
    pub connected_links: Vec<u64>,
    /// Plan 31 C8: forward-only and suspended, as the core applies them.
    pub authority: constellation_authority::core::AuthorityMode,
    /// The longest core step since the driver started, and its event's
    /// kind; the longest within the last minute or two
    /// ([`StepTail`]): the tail a holder's stall bound
    /// (`CONSTELLATION_HOLDER_STALL_MS`) must stay above.
    pub longest_step_ms: u64,
    pub longest_step_kind: &'static str,
    pub recent_longest_step_ms: u64,
}

/// The core-step tail for `status` ([`CoreStatus::longest_step_ms`]):
/// the high-water mark, and the longest of the current and the previous
/// minute.
#[derive(Debug)]
struct StepTail {
    longest: (u64, &'static str),
    minute: std::time::Instant,
    this_minute: u64,
    last_minute: u64,
}

impl StepTail {
    fn new() -> Self {
        Self {
            longest: (0, ""),
            minute: std::time::Instant::now(),
            this_minute: 0,
            last_minute: 0,
        }
    }

    /// A step of `ms` (event `kind`) ended now.
    fn note(&mut self, ms: u64, kind: &'static str) {
        let elapsed = self.minute.elapsed();
        if elapsed >= Duration::from_secs(120) {
            self.last_minute = 0;
            self.this_minute = 0;
            self.minute = std::time::Instant::now();
        } else if elapsed >= Duration::from_secs(60) {
            self.last_minute = self.this_minute;
            self.this_minute = 0;
            self.minute = std::time::Instant::now();
        }
        self.this_minute = self.this_minute.max(ms);
        if ms > self.longest.0 {
            self.longest = (ms, kind);
        }
    }

    fn recent(&self) -> u64 {
        self.this_minute.max(self.last_minute)
    }
}

/// Short names for the trace line around every core step.
fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Submit { .. } => "Submit",
        Event::SubmitInDoubt { .. } => "SubmitInDoubt",
        Event::Peer { msg, .. } => msg.kind(),
        Event::PeerFailed { .. } => "PeerFailed",
        Event::Timer { .. } => "Timer",
        Event::UploadsDone { .. } => "UploadsDone",
        Event::PublishDone { .. } => "PublishDone",
        Event::ConflictCopyDone { .. } => "ConflictCopyDone",
        Event::RebuildDone { .. } => "RebuildDone",
        Event::LockFlushed { .. } => "LockFlushed",
        Event::LockHorizonPersisted { .. } => "LockHorizonPersisted",
        Event::Roster { .. } => "Roster",
        Event::Slack { .. } => "Slack",
        Event::Peers { .. } => "Peers",
        Event::OwnS3 { .. } => "OwnS3",
        Event::Activity { .. } => "Activity",
        Event::SubscriberGone { .. } => "SubscriberGone",
        Event::HolderAlive { .. } => "HolderAlive",
        Event::BackupAlive { .. } => "BackupAlive",
        Event::OwnLag { .. } => "OwnLag",
        Event::OwnFreeze { .. } => "OwnFreeze",
        Event::DelegRenewSentOffCore { .. } => "DelegRenewSentOffCore",
        Event::Control { req, .. } => match req {
            Control::Nudge => "Control(Nudge)",
            Control::Journaled => "Control(Journaled)",
            Control::PublishNow => "Control(PublishNow)",
            Control::Barrier { .. } => "Control(Barrier)",
            Control::TailToHead => "Control(TailToHead)",
            Control::Acquire => "Control(Acquire)",
            _ => "Control",
        },
        Event::S3 { result, .. } => match result {
            S3Result::LeaseGet(_) => "S3(LeaseGet)",
            S3Result::LeasePut(_) => "S3(LeasePut)",
            S3Result::SegmentPut(_) => "S3(SegmentPut)",
            S3Result::SegmentRun(_) => "S3(SegmentRun)",
            S3Result::SegmentGap(_) => "S3(SegmentGap)",
            _ => "S3",
        },
    }
}

fn action_kind(action: &Action) -> &'static str {
    match action {
        Action::Reply { .. } => "Reply",
        Action::Send { .. } => "Send",
        Action::SetTimer { kind, .. } => match kind {
            TimerKind::Poll => "SetTimer(Poll)",
            _ => "SetTimer",
        },
        Action::CancelTimer { .. } => "CancelTimer",
        Action::S3 { req, .. } => s3_kind(req),
        Action::UploadDirtyChunks { .. } => "UploadDirtyChunks",
        Action::UploadAwaited { .. } => "UploadAwaited",
        Action::Publish { .. } => "Publish",
        Action::FollowHead { .. } => "FollowHead",
        Action::Announce { .. } => "Announce",
        Action::ConflictCopy { .. } => "ConflictCopy",
        Action::RebuildReplica { .. } => "RebuildReplica",
        Action::LockFlush { .. } => "LockFlush",
        Action::PersistLockHorizon { .. } => "PersistLockHorizon",
        Action::RoundDone { .. } => "RoundDone",
        Action::RefreshRoster => "RefreshRoster",
        Action::EpochClose => "EpochClose",
        Action::EpochFlushed => "EpochFlushed",
        Action::ControlDone { .. } => "ControlDone",
    }
}

/// A short name for an S3 op, for the trace lines around its task.
fn s3_kind(op: &S3Op) -> &'static str {
    match op {
        S3Op::LeaseGet => "LeaseGet",
        S3Op::LeaseCreate { .. } => "LeaseCreate",
        S3Op::LeaseSwap { .. } => "LeaseSwap",
        S3Op::SegmentPut { .. } => "SegmentPut",
        S3Op::SegmentRun { .. } => "SegmentRun",
        S3Op::SegmentGap { .. } => "SegmentGap",
        S3Op::InboxPut { .. } => "InboxPut",
        S3Op::InboxRun { .. } => "InboxRun",
        S3Op::InboxDrain { .. } => "InboxDrain",
        S3Op::InboxDelete { .. } => "InboxDelete",
        S3Op::InboxTombstone { .. } => "InboxTombstone",
        S3Op::InboxLastN { .. } => "InboxLastN",
        S3Op::HeartbeatRead => "HeartbeatRead",
        S3Op::HeartbeatPut { .. } => "HeartbeatPut",
    }
}

/// Everything the driver needs from the daemon.
pub struct DriverDeps {
    pub meta: Arc<Meta>,
    pub store_inner: Arc<dyn object_store::ObjectStore>,
    pub log: LogStore,
    pub lease_mode: LeaseMode,
    pub e2e: Option<constellation_store_s3::SharedE2eKeys>,
    pub peers: constellation_net::Peers,
    pub view: Arc<LeaseView>,
    /// Plan 30 §M11: the FUSE fast path's delegate check.
    pub delegates: Arc<crate::lease::DelegateView>,
    pub epochs: Arc<crate::epoch::EpochManager>,
    pub designations: Arc<crate::designation::DesignationManager>,
    pub placement: Arc<crate::placement::Placement>,
    pub pins: Arc<crate::pin::PinManager>,
    pub publisher: Option<Arc<tokio::sync::Mutex<crate::mtree_publish::TreePublisher>>>,
    pub cache: Arc<DiskCache>,
    pub chunk_store: Arc<ChunkStore>,
    pub compression: CompressionSetting,
    pub upload: Arc<crate::upload::UploadRuntime>,
    pub forward: Arc<crate::forward::ForwardState>,
    pub reintegration: Arc<crate::reintegrate::ReintegrationState>,
    pub state_dir: PathBuf,
    pub last_sync_ms: Arc<AtomicU64>,
    /// Rid seqs the FUSE fast path completed (`SyncHandle::acked`),
    /// drained into the core's ack tracker.
    pub pending_acks: Arc<Mutex<Vec<u64>>>,
    pub status: Arc<Mutex<CoreStatus>>,
    pub config: Config,
    /// Plan 30 §M14: flush one inode through before a recalled lock
    /// grant is released (`Action::LockFlush`); blocking, run on the
    /// blocking pool. Every mounted view's write state
    /// (`crate::locks::LockFlushers`).
    pub lock_flush: crate::locks::LockFlushHook,
    /// Plan 30 §M14 phase 2: the mounted views' roots
    /// (`crate::locks::LockFlushers::view_roots`): a refused replay of an
    /// op issued under a lock keeps its conflict copy under the deepest
    /// one above its file (`Action::ConflictCopy`'s `locked`).
    pub view_roots: crate::locks::ViewRootsHook,
    /// Plan 30 M0's fault knob (`CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`):
    /// delay every forwarded-mutation reply this node sends, after the
    /// op executed (bug A's trigger). 0 in production.
    pub fault_reply_delay_ms: u64,
    /// The open-orphan hold writer (`crate::holds`), nudged after every
    /// applied segment so a foreign unlink of a locally open file is
    /// claimed within a round trip. `None` in tools and tests.
    pub holds: Option<Arc<crate::holds::Holds>>,
    /// What the peer service shares with this driver off the core's
    /// step ([`OffCore`]).
    pub off_core: Arc<OffCore>,
    /// The peer requests that must not queue behind the others
    /// (`SyncRequest`s the peer service sends on [`OffCore`]'s lane:
    /// renewals and the holder's heartbeat), served before everything
    /// else, the internal channel included ([`Driver::run`]). Taken by
    /// [`Driver::new`].
    pub urgent_rx: Option<mpsc::UnboundedReceiver<SyncRequest>>,
    /// The urgent lane's sender, for the answers to this node's own lock
    /// renewals ([`SyncRequest::LockRenewAnswered`]); `None` without a
    /// peer service (they go on the internal channel then).
    pub urgent_tx: Option<mpsc::UnboundedSender<SyncRequest>>,
}

/// Plan 30 §M7: frames queued for one log-stream subscriber, at most
/// `CONSTELLATION_LOG_STREAM_QUEUE` (default 1024) of them.
pub fn log_stream_queue() -> usize {
    std::env::var("CONSTELLATION_LOG_STREAM_QUEUE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1024)
}

/// Plan 30 §M7: bytes queued for one log-stream subscriber before the
/// holder drops it back to S3 tailing
/// (`CONSTELLATION_LOG_STREAM_BUFFER_BYTES`, default 32 MiB). This is what
/// keeps a slow WAN subscriber from slowing the holder: the holder never
/// waits on a subscriber, it only ever enqueues or drops.
/// The subscriber's frame backlog budget ([`Driver::log_permits`]): the
/// holder's per-subscriber budget, as a semaphore's permit count.
fn log_backlog_bytes() -> usize {
    log_stream_buffer_bytes().min(u32::MAX as usize)
}

fn log_stream_buffer_bytes() -> usize {
    std::env::var("CONSTELLATION_LOG_STREAM_BUFFER_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(32 << 20)
}

fn env_ms(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Like [`env_ms`], but `0` is a value, not "unset": for knobs where 0
/// means something (no split, no share floor, no rate floor).
fn env_u64_zero_ok(name: &str, default: u64) -> u64 {
    parse_u64_zero_ok(std::env::var(name).ok().as_deref(), default)
}

fn parse_u64_zero_ok(raw: Option<&str>, default: u64) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// One subscriber of this holder's log stream: the frames queued for its
/// stream writer (the P2P handler), and how many bytes of segment payload
/// sit in that queue.
struct SubscriberSink {
    req: OpId,
    tx: mpsc::Sender<LogEvent>,
    queued_bytes: Arc<AtomicU64>,
}

/// Plan 30 §M7: how long a sync round waits for the upload pass before
/// shipping what is already shippable (`CONSTELLATION_ROUND_UPLOAD_WAIT_MS`,
/// default 250). The ship defers every transaction whose manifest still
/// names a pending chunk (`Meta::take_journal_grouped`), so a round no
/// longer has to wait for a whole write-back backlog before anything else
/// can ship.
fn round_upload_wait() -> Duration {
    Duration::from_millis(env_ms("CONSTELLATION_ROUND_UPLOAD_WAIT_MS", 250))
}

/// Plan 30 §M7: the one full upload pass (every pending chunk) in flight,
/// shared by the rounds that wait on it. A round joins the running pass
/// (or starts one) and waits at most the round budget; the pass keeps
/// going in the background and nudges a round when it finishes, so the
/// transactions it unblocked ship at once.
#[derive(Clone, Default)]
struct BulkPass {
    running: Arc<Mutex<Option<PassDone>>>,
}

/// A bulk pass's outcome, `None` while it runs.
type PassDone = tokio::sync::watch::Receiver<Option<Result<(), String>>>;

impl BulkPass {
    /// The running pass, or a new one.
    fn join(
        &self,
        upload: Uploader,
        tx: mpsc::UnboundedSender<Internal>,
        budget: Duration,
    ) -> PassDone {
        let mut running = self.running.lock().unwrap();
        if let Some(rx) = running.as_ref() {
            if rx.borrow().is_none() {
                return rx.clone();
            }
        }
        let (done_tx, done_rx) = tokio::sync::watch::channel(None);
        *running = Some(done_rx.clone());
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let result = upload.run_background().await.map_err(|e| format!("{e:#}"));
            let _ = done_tx.send(Some(result));
            if started.elapsed() >= budget {
                // A round already moved on without this pass: ship what
                // it unblocked now, not at the next poll.
                let _ = tx.send(Internal::Control {
                    req: Control::Nudge,
                    reply: ControlReply::None,
                });
            }
        });
        done_rx
    }

    /// One round's (or flush's) upload step. `complete`: every chunk
    /// pending now must be up before it returns — the running pass is
    /// waited for, then a fresh full pass runs (it picks up what arrived
    /// after the running one took its snapshot).
    async fn run(
        &self,
        upload: Uploader,
        tx: mpsc::UnboundedSender<Internal>,
        complete: bool,
        budget: Duration,
    ) -> Result<(), String> {
        let mut rx = self.join(upload.clone(), tx, budget);
        if complete {
            let _ = rx.wait_for(|r| r.is_some()).await;
            return upload.run_complete().await.map_err(|e| format!("{e:#}"));
        }
        let finished = tokio::time::timeout(budget, rx.wait_for(|r| r.is_some()))
            .await
            .ok()
            .and_then(|r| r.ok().map(|r| r.clone()));
        match finished {
            Some(Some(result)) => result,
            // Still uploading (or the pass went away): ship what is
            // already shippable.
            _ => Ok(()),
        }
    }
}

/// What a control-plane request is waiting for.
enum ControlReply {
    None,
    Publish(oneshot::Sender<std::result::Result<(u64, constellation_mtree::NodeHash), String>>),
    Done(oneshot::Sender<std::result::Result<(), String>>),
    Acquire(oneshot::Sender<std::result::Result<AcquireProgress, String>>),
    ClaimOffer,
    Text(oneshot::Sender<std::result::Result<String, String>>),
    Leave(oneshot::Sender<std::result::Result<String, String>>),
    /// Plan 30 §M8.
    ReadIndex(oneshot::Sender<ReadAnswer>),
    Recall(oneshot::Sender<()>),
    /// Plan 30 §M14.
    Lock(oneshot::Sender<LockAnswer>),
    LockTest(oneshot::Sender<LockTestAnswer>),
}

/// What reaches the driver task from the IO it spawned.
// An event is moved through the channel once and matched on at once;
// boxing the large variant would cost an allocation per event for no
// benefit.
#[allow(clippy::large_enum_variant)]
enum Internal {
    Event(Event),
    Control {
        req: Control,
        reply: ControlReply,
    },
    /// A core timer fired (`deferred`: put back once behind the backlog,
    /// [`measures_silence`]).
    Timer {
        id: TimerId,
        kind: TimerKind,
        deferred: bool,
    },
    /// A log-stream frame (or its end), holding its bytes of the
    /// subscriber's backlog budget ([`Driver::log_permits`]) until the core
    /// has handled it.
    Frame(Event, tokio::sync::OwnedSemaphorePermit),
    /// The holder's heartbeat asking whether the driver loop still takes
    /// work ([`OffCore::echo_sent`]): answered by taking it, no core step.
    Echo,
}

/// Read the core's tunables from the environment (the `CONSTELLATION_*`
/// knobs the extracted code read), for `node_id` at `incarnation`.
#[allow(clippy::too_many_arguments)]
pub fn load_config(
    node_id: u64,
    incarnation: u32,
    lease_mode: LeaseMode,
    p2p: bool,
    read_only_member: bool,
    interval_ms: u64,
    idle_max_ms: u64,
    retention_s: u64,
) -> Config {
    let ttl_ms = crate::lease::lease_ttl_ms();
    let mut c = Config::defaults(node_id, incarnation);
    c.clock_id = constellation_platform::clock::protocol().id();
    c.clock_rate_budget_ppm = constellation_platform::clock::protocol().rate_budget_ppm();
    c.single_writer = lease_mode == LeaseMode::SingleWriter;
    c.ttl_ms = ttl_ms;
    c.expiry_margin_ms = crate::lease::expiry_margin_ms() as u64;
    c.idle_release_ms = crate::lease::idle_release_ms();
    // Overridable for the harness (plan 30 §M11's measurement scenario
    // keeps the lease on its root through a burst of forwarded writes).
    c.dwell_ms = env_ms(
        "CONSTELLATION_LEASE_DWELL_MS",
        crate::lease::LEASE_MIN_DWELL_MS,
    );
    c.wanted_grace_ms = env_ms(
        "CONSTELLATION_LEASE_WANTED_GRACE_MS",
        crate::lease::LEASE_WANTED_GRACE_MS,
    );
    c.handoff_pause_ms = crate::lease::HANDOFF_PAUSE_MS as u64;
    c.forwarding = crate::forward::forwarding_enabled();
    c.p2p = p2p;
    c.forward_timeout_ms = crate::forward::forward_timeout_ms();
    c.acquire_deadline_ms = 2 * ttl_ms;
    c.s3_less_deadline_ms = env_ms(
        "CONSTELLATION_S3_LESS_OP_DEADLINE_MS",
        c.s3_less_deadline_ms,
    );
    c.sync_interval_ms = interval_ms;
    c.idle_max_ms = idle_max_ms;
    c.publisher = !read_only_member;
    c.publish_idle_ms = crate::shipper::publish_idle_interval().as_millis() as u64;
    c.atime_ship_max_delay_ms = crate::atime::ship_max_delay().as_millis() as u64;
    let knobs = crate::inbox::knobs(interval_ms, ttl_ms, retention_s);
    c.inbox = knobs.enabled;
    c.inbox_warm_max_ms = knobs.warm_max_ms;
    c.inbox_cold_max_ms = knobs.cold_max_ms;
    c.inbox_hot_ms = knobs.hot_ms;
    c.inbox_hot_grace = knobs.hot_grace;
    c.inbox_poll_width = knobs.poll_width;
    c.inbox_recheck_ms = knobs.recheck_ms;
    c.inbox_deadline_ms = knobs.deadline_ms;
    c.inbox_p2p_grace_ms = knobs.p2p_grace_ms;
    c.inbox_tail_ms = knobs.tail_ms;
    c.escalation = knobs.escalation;
    c.escalate_window_ms = knobs.escalate_window_ms;
    c.escalate_ops = knobs.escalate_ops;
    c.escalate_wait_ms = knobs.escalate_wait_ms;
    c.escalate_retry_ms = knobs.escalate_retry_ms;
    // Plan 30 §M7: direct log streams (on unless
    // `CONSTELLATION_LOG_STREAMS` is 0/off/false; P2P off disables them
    // too).
    c.log_streams = !matches!(
        std::env::var("CONSTELLATION_LOG_STREAMS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false"
    );
    c.stream_heartbeat_ms = env_ms(
        "CONSTELLATION_LOG_STREAM_HEARTBEAT_MS",
        c.stream_heartbeat_ms,
    );
    c.stream_timeout_ms = env_ms("CONSTELLATION_LOG_STREAM_TIMEOUT_MS", c.stream_timeout_ms);
    c.stream_backstop_ms = env_ms("CONSTELLATION_LOG_STREAM_BACKSTOP_MS", c.stream_backstop_ms);
    c.gap_check_ms = env_ms("CONSTELLATION_LOG_GAP_CHECK_MS", c.gap_check_ms);
    c.gap_hint_check_ms = env_ms("CONSTELLATION_LOG_GAP_HINT_CHECK_MS", c.gap_hint_check_ms);
    // Plan 30 §M8: read delegations (granted to `cto=strict` readers
    // that ask; on unless `CONSTELLATION_READ_DELEGATIONS` is 0/off/false)
    // and their TTL. A forwarded reply held for recalls answers `Held`
    // at half the forward timeout, well inside the requester's RPC.
    c.read_delegations = p2p
        && !matches!(
            std::env::var("CONSTELLATION_READ_DELEGATIONS")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false"
        );
    c.read_delegation_ttl_ms = crate::cto::read_delegation_ttl_ms();
    c.recall_hold_ms = (c.forward_timeout_ms / 2).max(1);
    // How long a forwarded `back` close may wait on the holder for its
    // own record before its held chunks go up (`Action::UploadAwaited`;
    // 0: never).
    c.own_record_wait_ms =
        env_u64_zero_ok("CONSTELLATION_OWN_RECORD_WAIT_MS", c.own_record_wait_ms);
    // Plan 30 §M14: cluster locks follow P2P here; `node_runtime` applies
    // the mount's `--locks` (`crate::locks::cluster_effective`).
    c.locks = p2p;
    c.lock_ttl_ms = crate::locks::lock_ttl_ms(c.lock_ttl_ms);
    c.lock_cache_idle_ms = crate::locks::lock_cache_idle_ms(c.lock_cache_idle_ms);
    c.read_index_deadline_ms = crate::cto::read_index_budget_ms();
    // Plan 30 §M9: backups (peers within the RTT budget; `0` backups or
    // no peer in budget is today's behaviour), the ack timeout that
    // reconfigures a silent backup out, the silence after which a backup
    // seals and takes over, and pre-S3 streaming to followers.
    // (`0` is meaningful here — no peer is ever in budget — so not
    // `env_ms`, which treats 0 as unset.)
    // Plan 30 §M11: delegation (on unless `CONSTELLATION_DELEGATION` is
    // 0/off/false; P2P off disables it) and the grant ttl.
    c.delegation = p2p
        && !matches!(
            std::env::var("CONSTELLATION_DELEGATION")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false"
        );
    // Default: the lock grant TTL (at least the core's 5 s, which a short
    // test TTL must not shorten). A delegate's lock grants never outlive
    // its delegation (they are capped at what is left of it, less the
    // margin, and the holding node takes another margin off), so at the
    // core's 5 s a lock in a delegated subtree was honoured for at most
    // ~3 s and renewed every second or so: one slow step on a busy node
    // and it lapsed under the application's lock (`EIO`), and a delegate
    // whose renewal came a second late had its generation sealed and
    // drained by the root (`stress-ng-fs-nodes`). The price is the
    // reclaim of a dead delegate's subtree, after `ttl + margin`.
    // With renewals that no longer starve (overload-cascade-2), 5 s still
    // lapsed under `stress-ng-fs-nodes` (a delegate's own 0.5-0.8 s steps
    // behind compactions against ~2.3 s grants) and 10 s passed 2 of 2 at
    // load 35-65: a candidate for the default, kept at the lock TTL until
    // it is measured under heavier load.
    c.delegation_ttl_ms = env_ms(
        "CONSTELLATION_DELEGATION_TTL_MS",
        constellation_authority::Config::default_delegation_ttl_ms(c.lock_ttl_ms),
    );
    // Plan 30 §M11 phase 2b: the placement's knobs.
    // Plan 30 §M12: on by default (a dominant writer's subtree moves to
    // it, a hot shared directory splits into hash ranges);
    // `CONSTELLATION_DELEGATION_PLACEMENT=off` pins the single sequencer.
    c.placement = c.delegation
        && !matches!(
            std::env::var("CONSTELLATION_DELEGATION_PLACEMENT")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false"
        );
    // `0` is a value for the four share/rate knobs: SPLIT=0 turns hash
    // range splits off, LEAVE=0 never recalls a placed delegation for
    // its share (only for its rate), DOMINANCE=0 gives a subtree to its
    // top writer whatever its share, MIN_OPS=0 drops the rate floor.
    c.placement_split_pct =
        env_u64_zero_ok("CONSTELLATION_DELEGATION_SPLIT", c.placement_split_pct).min(100);
    c.placement_window_ms = env_ms("CONSTELLATION_DELEGATION_WINDOW_MS", c.placement_window_ms);
    c.placement_min_ops = env_u64_zero_ok("CONSTELLATION_DELEGATION_MIN_OPS", c.placement_min_ops);
    c.placement_dominance_pct = env_u64_zero_ok(
        "CONSTELLATION_DELEGATION_DOMINANCE",
        c.placement_dominance_pct,
    )
    .min(100);
    c.placement_leave_pct =
        env_u64_zero_ok("CONSTELLATION_DELEGATION_LEAVE", c.placement_leave_pct).min(100);
    c.placement_dwell_ms = env_ms("CONSTELLATION_DELEGATION_DWELL_MS", c.placement_dwell_ms);
    c.placement_cooldown_ms = env_ms(
        "CONSTELLATION_DELEGATION_COOLDOWN_MS",
        c.placement_cooldown_ms,
    );
    c.backup_rtt_budget_ms = std::env::var("CONSTELLATION_BACKUP_RTT_BUDGET_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(c.backup_rtt_budget_ms);
    c.backups_max = std::env::var("CONSTELLATION_BACKUPS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(c.backups_max);
    c.backup_ack_timeout_ms = env_ms(
        "CONSTELLATION_BACKUP_ACK_TIMEOUT_MS",
        c.backup_ack_timeout_ms,
    )
    .max(1);
    c.backup_slow_max_ms = env_ms("CONSTELLATION_BACKUP_SLOW_MAX_MS", c.backup_slow_max_ms)
        .max(c.backup_ack_timeout_ms);
    c.backup_takeover_ms = env_ms("CONSTELLATION_BACKUP_TAKEOVER_MS", c.backup_takeover_ms).max(1);
    c.backup_heartbeat_ms = env_ms("CONSTELLATION_BACKUP_HEARTBEAT_MS", c.backup_heartbeat_ms)
        .max(1)
        .min(c.backup_takeover_ms / 3)
        .max(1);
    c.pre_s3_streaming = p2p
        && !matches!(
            std::env::var("CONSTELLATION_PRE_S3_STREAMING")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false"
        );
    c
}

/// Plan 30 §M9: `fs create --ack-policy local|s3`, defaulting to
/// `CONSTELLATION_ACK`; `None` (neither set) stores no policy (`local`).
///
/// M16: the policy is the filesystem's, not a mount's. It is recorded on
/// every lease tenure (`Lease::ack_policy`) and what an acknowledgement
/// means is the tenure's, so a per-mount flag could only ever apply to
/// tenures its own mount acquired: a `--ack s3` requester forwarding to a
/// `local` holder got local acknowledgements. One policy for every mount
/// makes every tenure's policy the one each mount asked for.
pub fn ack_policy_flag(flag: Option<&str>) -> anyhow::Result<Option<String>> {
    let env = std::env::var("CONSTELLATION_ACK").ok();
    parse_ack_policy(flag.or(env.as_deref()))
}

fn parse_ack_policy(raw: Option<&str>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        p @ ("local" | "s3") => Ok(Some(p.to_string())),
        other => anyhow::bail!("invalid --ack-policy {other:?} (expected local or s3)"),
    }
}

/// Plan 30 §M9: whether the filesystem acknowledges on the shared log
/// (`ack_policy = s3` in `meta.json`).
pub fn ack_s3_of(fs_policy: Option<&str>) -> bool {
    fs_policy.is_some_and(|p| p.trim().eq_ignore_ascii_case("s3"))
}

fn now() -> Now {
    constellation_platform::clock::protocol().now()
}

/// This node's wait clock (`ProtocolClock::wait`) in ms: the clock of the
/// driver's wake and lag stamps, the beats' send stamps and every arrival
/// stamp the core compares with them (plan 43 C7-H1): none of them moves
/// with a wall step.
fn wait_now_ms() -> i64 {
    constellation_platform::clock::protocol().wait().0
}

fn cas_failure(e: StoreError) -> CasFailure {
    match e {
        StoreError::CasConflict | StoreError::AlreadyExists => CasFailure::Conflict,
        other => CasFailure::Failed(other.to_string()),
    }
}

fn s3_failure(e: StoreError) -> S3Failure {
    S3Failure(e.to_string())
}

pub struct Driver {
    core: Core,
    deps: DriverDeps,
    node_id: u64,
    leases: LeaseStore,
    inbox: InboxStore,
    int_tx: mpsc::UnboundedSender<Internal>,
    int_rx: mpsc::UnboundedReceiver<Internal>,
    /// The timers that keep a grant this node holds ([`prompt_timer`]),
    /// served before the internal channel.
    prompt_tx: mpsc::UnboundedSender<Internal>,
    prompt_rx: mpsc::UnboundedReceiver<Internal>,
    /// The bytes of the holder's log-stream frames this node's
    /// subscriptions may have queued for the core at once: the holder's
    /// budget for one subscriber's queue ([`log_stream_buffer_bytes`]).
    /// A core behind by that much stops its subscription reading; the
    /// holder's queue for it then fills and the holder drops it to S3
    /// tailing — as it would a subscriber that far behind anyway. Below
    /// it the frames are in arrival order with the replies and timers on
    /// the internal channel, as they always were.
    ///
    /// overload-cascade-2 tried two smaller bounds, each worse than
    /// none. A lane of 64 frames of its own, served after the internal
    /// channel, starved while that channel was never empty: the stream
    /// watchdog (a timer on it) found the stream silent with frames
    /// waiting and dropped it. 64 frames on the internal channel were
    /// seconds of lag at most: under a load spike both non-holders were
    /// dropped at once ("queue full"), tailed S3 in seconds-long
    /// `SegmentRun` steps, and each, the other's delegate backup, could
    /// not acknowledge appends whose deps its replica no longer reached —
    /// both delegates' writes waited 107 s (`stress-ng-fs-nodes`).
    log_permits: Arc<tokio::sync::Semaphore>,
    sync_tx: mpsc::UnboundedSender<SyncRequest>,
    sync_rx: mpsc::UnboundedReceiver<SyncRequest>,
    replies: HashMap<Rid, oneshot::Sender<ClientReply>>,
    mutate_replies: HashMap<OpId, oneshot::Sender<MutateReplyParts>>,
    handoff_replies: HashMap<OpId, oneshot::Sender<Option<HandoffResult>>>,
    /// Plan 30 §M8: peers' ReadIndex requests and recalls, by the id the
    /// driver minted for the core.
    read_index_replies: HashMap<OpId, oneshot::Sender<ReadIndexOutcome>>,
    recall_replies: HashMap<OpId, oneshot::Sender<()>>,
    /// Plan 30 §M11: peers' delegation recalls this node is answering
    /// (stream batches and renewals are one way, answered by the core's
    /// own messages).
    deleg_recall_replies:
        HashMap<OpId, oneshot::Sender<(u64, constellation_meta::locks::LockHandback)>>,
    /// Plan 30 §M14: peers' lock requests, recalls, renewals and tests
    /// this node is answering.
    lock_request_replies: HashMap<OpId, oneshot::Sender<LockOutcome>>,
    lock_recall_replies: HashMap<OpId, oneshot::Sender<()>>,
    lock_renew_replies: HashMap<OpId, oneshot::Sender<LockRenewResults>>,
    lock_test_replies: HashMap<OpId, oneshot::Sender<LockTestOutcome>>,
    deleg_backup_replies: HashMap<OpId, oneshot::Sender<(u64, bool)>>,
    deleg_seal_replies: HashMap<OpId, oneshot::Sender<(bool, Vec<constellation_meta::DelegateTx>)>>,
    /// Plan 30 §M10: peers' promise requests this node is answering.
    promise_replies: HashMap<OpId, oneshot::Sender<(Option<i64>, u32)>>,
    /// Plan 30 §M9: the holder's `BackupAppend` requests this node is
    /// answering, by the core's op id.
    backup_replies: HashMap<OpId, oneshot::Sender<(u64, bool)>>,
    controls: HashMap<OpId, ControlReply>,
    next_control: u64,
    /// Whether this node holds (from the last refresh), for the tickers.
    last_epoch_reported: Option<EpochState>,
    /// Plan 30 §M7, holder side: the subscribers of this node's log
    /// stream, by node.
    subscribers: HashMap<NodeId, SubscriberSink>,
    /// Plan 30 §M7, subscriber side: the task reading each subscription.
    subscriptions: HashMap<OpId, tokio::task::AbortHandle>,
    stream_buffer_bytes: u64,
    /// Plan 30 §M7: the background upload pass rounds share.
    bulk_pass: BulkPass,
    round_upload_wait: Duration,
    /// Who the holder's off-core heartbeat goes to (from the last
    /// refresh), and the driver loop's progress stamp: see
    /// [`holder_alive_loop`].
    alive: Arc<Mutex<Option<AliveTargets>>>,
    /// The urgent lane ([`DriverDeps::urgent_rx`]); `None` once closed
    /// (never while the driver holds [`DriverDeps::urgent_tx`]).
    urgent_rx: Option<mpsc::UnboundedReceiver<SyncRequest>>,
    /// The holders' liveness handed to the core so far
    /// ([`fresh_heard`]).
    heard_handed: HashMap<NodeId, Heard>,
    /// The end of the latest own lapse handed to the core
    /// ([`Driver::hand_lag`]).
    lag_handed: i64,
    /// The latest freeze of the heartbeat thread handed to the core
    /// ([`Driver::hand_freeze`]), `(from, until)`.
    freeze_handed: (i64, i64),
    /// The thread the driver loop runs on now (its tokio worker), for the
    /// stuck-driver backtrace.
    loop_thread: Arc<Mutex<constellation_platform::process::ThreadRef>>,
    alive_book: AliveBook,
    step_end: std::time::Instant,
    /// Where the core's time goes, per event kind, logged at debug every
    /// [`STEP_PROFILE_EVERY`] ([`StepProfile`]).
    profile: StepProfile,
    /// The step-time tail for `status` ([`StepTail`]).
    step_tail: StepTail,
    /// The core's job and the replica's next log sequence, and since
    /// when both have stayed the same (the stuck-job report,
    /// [`STUCK_JOB`]).
    job_since: Option<(JobKind, u64, std::time::Instant, bool)>,
    /// The protocol clock's step count already logged (`note_clock_steps`).
    clock_steps_logged: u64,
    /// When the step being handled took its `Now` (taken just before it):
    /// the anchor of the relative timers it arms. `SetTimer { after_ms }`
    /// counts from the reading, not from the dispatch, so a step that waited
    /// between its clock read and its actions (a store stall, a stopped
    /// process) still fires the timers it armed when the reading says, as
    /// the absolute deadlines it used to arm did.
    now_at: std::time::Instant,
}

/// A core job (a round, an acquisition, a flush, ...) in progress this
/// long with the replica's log cursor not moving is reported once, in
/// full: while a job holds the cursor the node applies nothing of the log.
const STUCK_JOB: Duration = Duration::from_secs(20);

/// The targets of the holder's off-core heartbeat ([`holder_alive_loop`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct AliveTargets {
    epoch: u64,
    /// The committed backups and the candidate being brought up, with
    /// their candidacies (`Core::backup_candidacies`).
    listed: Vec<(NodeId, u64)>,
    /// Candidacies the holder dropped lately (`listed: false`).
    dismissed: Vec<(NodeId, u64)>,
    /// The lease body's wall expiry, carried on every beat for the seal
    /// watch (row 24, C7b).
    until_unix_ms: i64,
    /// Plan 43 C7-H1: the end of the lease this holder honours, on its
    /// honour clock (`LeaseState::honour_until`; 0: none): no heartbeat at
    /// or past it (a backward wall step must not keep the holder beating
    /// past the lease it can honour).
    until_honour: i64,
    /// Plan 43 C7-H1 (R7): the same lease's end on the holder's wait
    /// clock (`LeaseState::wait_until`; 0: none), which the beats name:
    /// their `sent` is on that clock too, so a successor that seals this
    /// holder places the lease's end by a difference no wall step moves.
    until_wait: i64,
    /// Plan 43 C7-F F-a: what this holder waits out that its lease does
    /// not cap (`Core::beat_guard_wait_ms`), on its wait clock, as the beat
    /// names it, so a successor that seals this holder can end its own
    /// floor with the lease: 0 none, `i64::MAX` unknown.
    guard_wait: i64,
}

/// How long a dropped backup is told it was dropped (`HolderAlive` with
/// `listed: false`); a lost message leaves it sealing, harmlessly, as
/// before.
const DISMISS_FOR_MS: i64 = 10_000;

/// The driver's book of who [`holder_alive_loop`] beats to: the backups of
/// the lease this node holds, and the candidacies it dropped within
/// [`DISMISS_FOR_MS`]. A node selected again is no longer told it was
/// dropped (its new candidacy is listed); the backup applies a dismissal
/// only to the candidacy it names, so one still in flight is harmless.
#[derive(Debug, Default)]
struct AliveBook {
    /// node → (epoch, told until (wait-clock ms), candidacy).
    dismissed: HashMap<NodeId, (u64, i64, u64)>,
    /// The last targets' epoch and listed backups.
    prev: Option<(u64, Vec<(NodeId, u64)>)>,
}

impl AliveBook {
    /// The targets for this refresh: `held` is the epoch, the wall expiry,
    /// the honour end, the wait end and the guard of a lease this node
    /// holds (`None`: none, or only within an open continuation epoch),
    /// `listed` its backups.
    fn update(
        &mut self,
        now_wait_ms: i64,
        held: Option<(u64, i64, i64, i64, i64)>,
        listed: Vec<(NodeId, u64)>,
    ) -> Option<AliveTargets> {
        let Some((epoch, until_unix_ms, until_honour, until_wait, guard_wait)) = held else {
            self.dismissed.clear();
            self.prev = None;
            return None;
        };
        self.dismissed
            .retain(|_, (e, until, _)| *e == epoch && *until > now_wait_ms);
        for (n, _) in &listed {
            self.dismissed.remove(n);
        }
        if let Some((prev_epoch, prev)) = &self.prev {
            if *prev_epoch == epoch {
                for &(n, candidacy) in prev {
                    if candidacy != 0 && !listed.iter().any(|(l, _)| *l == n) {
                        self.dismissed
                            .insert(n, (epoch, now_wait_ms + DISMISS_FOR_MS, candidacy));
                    }
                }
            }
        }
        self.prev = Some((epoch, listed.clone()));
        let mut dismissed: Vec<(NodeId, u64)> = self
            .dismissed
            .iter()
            .map(|(n, (_, _, c))| (*n, *c))
            .collect();
        dismissed.sort_unstable();
        Some(AliveTargets {
            epoch,
            listed,
            dismissed,
            until_unix_ms,
            until_honour,
            until_wait,
            guard_wait,
        })
    }
}

/// `CONSTELLATION_HOLDER_STALL_MS` (default 15000): a driver loop that
/// has made no progress for this long (one core step, or anything else
/// it does between waits for work) stops the holder's off-core
/// heartbeat, so its backups seal it `CONSTELLATION_BACKUP_TAKEOVER_MS`
/// later. That long is a hung authority, not a busy one; a dead process,
/// a stopped driver or a cut link stops the heartbeat at once.
fn holder_stall_ms() -> i64 {
    env_ms("CONSTELLATION_HOLDER_STALL_MS", 15_000).max(1) as i64
}

/// What one tick of [`holder_alive_loop`] does.
#[derive(Debug, PartialEq, Eq)]
enum AliveTick {
    /// Not holding, or past the lease's expiry: nothing.
    Idle,
    /// The driver loop has not progressed for this long: no beats.
    Hung(i64),
    /// Beat these: `(node, candidacy, listed)`.
    Beat(Vec<(NodeId, u64, bool)>),
}

/// `busy_since`: the driver loop's last progress stamp while it works
/// (unix ms), 0 while it waits for work. `runtime_stalled`: how long the
/// shared runtime has not run its lag probe ([`OffCore::runtime_stalled_ms`]):
/// a runtime that runs nothing that long is a hung authority too, even
/// with the driver parked waiting for work (the beat no longer runs on
/// it, so it no longer stops with it by itself). `echo_sent`: when the
/// echo still unanswered went to the driver loop (0: none outstanding,
/// [`OffCore::echo_sent`]): a loop that has not taken a message for that
/// long serves nothing, however often its runtime gets a slice — the
/// runtime starved to a slice every few seconds, which neither rule
/// above sees, keeps beating and renewing while answering nobody.
fn alive_tick(
    t: Option<&AliveTargets>,
    (now, honour): (i64, HonourMs),
    (busy_since, runtime_stalled, echo_sent): (i64, i64, i64),
    stall: i64,
) -> AliveTick {
    let Some(t) = t else {
        return AliveTick::Idle;
    };
    // The lease ended for this holder on its honour clock (plan 43
    // C7-H1); the stall stamps below are the driver's wake clock (the wait
    // clock).
    if honour.0 >= t.until_honour {
        return AliveTick::Idle;
    }
    if busy_since != 0 && now - busy_since > stall {
        return AliveTick::Hung(now - busy_since);
    }
    if runtime_stalled > stall {
        return AliveTick::Hung(runtime_stalled);
    }
    if echo_sent != 0 && now - echo_sent > stall {
        return AliveTick::Hung(now - echo_sent);
    }
    AliveTick::Beat(
        t.listed
            .iter()
            .map(|(n, c)| (*n, *c, true))
            .chain(t.dismissed.iter().map(|(n, c)| (*n, *c, false)))
            .collect(),
    )
}

/// The holder's liveness for its backups, off the core's step (plan 30
/// §M9's seal watch). The core heartbeats its backups with appends, from
/// its own timers: a step that takes seconds (an overloaded node: CPU,
/// disk, metadata writes queued behind the mount's) silenced them, and the
/// backups sealed and deposed a live holder about 1.5 s in
/// (`stress-ng-fs-nodes`). This loop beats every backup heartbeat
/// interval while the node holds an unexpired lease and the driver loop
/// has progressed within [`holder_stall_ms`]; a backup counts a beat as
/// it counts an append (`Event::HolderAlive`), from its arrival
/// ([`OffCore`]). A backup that answers that its own driver is
/// progressing ([`OffCore::core_responsive`]) is alive: the core hears so
/// (`Event::BackupAlive`) and gives it longer to acknowledge; one whose
/// driver is stuck answers so, and gets the ordinary acknowledgement
/// timeout. Under `ack=s3` it beats the log stream's subscribers, which
/// read the holder's silence (`Core::liveness_targets`).
///
/// It runs on a thread of its own, with a runtime of its own
/// ([`AliveTask::start`]), and sends on the liveness lane
/// (`Peers::request_to_node_liveness`: a connection that runtime
/// dialed, so drives). As a task of the daemon's shared runtime — its
/// timer, and the pooled connection's driver — it beat only when that
/// runtime got to it, and a runtime held up for 1.5 s silenced the
/// holder: its backup sealed a live holder that had logged nothing for
/// 2.5 s, no slow step, no stuck driver (`snapsched-write-overhead`,
/// `silent_ms=1501`). What it reads of the driver is atomics and a
/// mutex the driver never holds across a wait. Each beat carries its
/// number and send time, so a backup tells beats sent late (this
/// process held up: the host) from beats heard late (`Core::silence_window`).
#[allow(clippy::too_many_arguments)]
async fn holder_alive_loop(
    peers: constellation_net::Peers,
    alive: Arc<Mutex<Option<AliveTargets>>>,
    off_core: Arc<OffCore>,
    (every, stall): (Duration, i64),
    node_id: NodeId,
    (core, prompt): (
        mpsc::UnboundedSender<Internal>,
        mpsc::UnboundedSender<Internal>,
    ),
    loop_thread: Arc<Mutex<constellation_platform::process::ThreadRef>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let backtraces = constellation_vfs::watch::backtraces_from_env();
    let mut stalled = false;
    let cap = beats_outstanding_max(every);
    // Beats still waiting for their answer, per peer.
    let pending: Arc<Mutex<HashMap<NodeId, usize>>> = Arc::default();
    let mut seq: u64 = 0;
    let mut lane = DelegLane::from_env();
    let mut due = tokio::time::Instant::now() + every;
    off_core.note_beat_due(wait_now_ms() + every.as_millis() as i64);
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep_until(due).await;
        // On cadence; a tick this thread itself took late (the host held
        // the process up) is not caught up with a burst.
        let woke = tokio::time::Instant::now();
        // How late this thread ran its tick: the whole process (or at
        // least this thread) was frozen that long. Recorded (one atomic
        // store) only when it matters to a grant; nothing is logged.
        let woke_ms = wait_now_ms();
        let late = woke.saturating_duration_since(due).as_millis() as i64;
        if late >= constellation_authority::deleg_lane::FREEZE_MIN_MS {
            off_core.note_beat_freeze(woke_ms - late, late);
        }
        due = (due + every).max(woke);
        off_core.note_beat_due(woke_ms + due.saturating_duration_since(woke).as_millis() as i64);
        let targets = alive.lock().unwrap().clone();
        let busy = off_core.busy_since();
        // One reading of the protocol clock per tick (plan 43 C7-H1): the
        // beats' send stamp and what is mapped onto it, the lease's end,
        // the lane's due times and the echo's answer stamp all come from it.
        let clk = constellation_platform::clock::protocol().now();
        let now = clk.wait.0;
        let runtime_stalled = off_core.runtime_stalled_ms(now);
        // One echo outstanding at a time, on the prompt lane (served
        // after the urgent one, ahead of the internal channel's backlog:
        // it asks whether the loop takes messages, not whether it has
        // caught up).
        let mut echo = off_core.echo_sent.load(Ordering::Relaxed);
        if echo == 0 {
            off_core.echo_sent.store(now, Ordering::Relaxed);
            if prompt.send(Internal::Echo).is_err() {
                return;
            }
            echo = now;
        }
        let tick = alive_tick(
            targets.as_ref(),
            (now, clk.honour),
            (busy, runtime_stalled, echo),
            stall,
        );
        // Whether or not this node holds a lease: a driver stuck this
        // long is reported (with the thread's backtrace under
        // `CONSTELLATION_FUSE_STALL_BACKTRACE=1`).
        let stuck_ms = if busy != 0 { now - busy } else { 0 }
            .max(runtime_stalled)
            .max(now - echo);
        let hung =
            (busy != 0 && now - busy > stall) || runtime_stalled > stall || now - echo > stall;
        // The delegation renewals the stuck core cannot send
        // ([`DelegLane`]); on any node with a delegation, holder or not.
        for (root, payload) in lane.tick(&off_core, node_id, clk.honour, stuck_ms, stall) {
            let peers = peers.clone();
            let off_core = off_core.clone();
            tokio::spawn(async move {
                let req_id = match &payload {
                    Payload::DelegRenewOffCore { req_id, .. } => *req_id,
                    _ => 0,
                };
                if peers
                    .request_to_node_liveness(root, &payload, BEAT_TIMEOUT)
                    .await
                    .is_err()
                {
                    off_core.lane_send_failed(root, req_id);
                }
            });
        }
        if hung != stalled {
            stalled = hung;
            if hung {
                let thread = *loop_thread.lock().unwrap();
                if targets.is_some() {
                    tracing::warn!(
                        node = node_id,
                        stuck_ms,
                        runtime_stalled_ms = runtime_stalled,
                        tid = thread.tid,
                        holding = true,
                        "the authority driver has made no progress (this holder sends no more \
                         liveness heartbeats: its backups will seal this tenure)"
                    );
                } else {
                    tracing::warn!(
                        node = node_id,
                        stuck_ms,
                        runtime_stalled_ms = runtime_stalled,
                        tid = thread.tid,
                        holding = false,
                        "the authority driver has made no progress"
                    );
                }
                if backtraces
                    && constellation_platform::native()
                        .process
                        .request_backtrace(thread)
                        .is_err()
                {
                    tracing::warn!(tid = thread.tid, "could not signal the driver's thread");
                }
            } else {
                tracing::info!(node = node_id, "the authority driver is progressing again");
            }
        }
        let (AliveTick::Beat(beats), Some(t)) = (tick, targets) else {
            continue;
        };
        seq += 1;
        for (to, candidacy, listed) in beats {
            if crate::fault::p2p_denied(to) || !beat_slot(&mut pending.lock().unwrap(), to, cap) {
                continue;
            }
            // The tick's one reading: an earlier stamp than the send only
            // lengthens `at − sent`, which lengthens the successor's cap.
            // The beat is on the holder's wait clock (plan 43 C7-H1, R7):
            // `constellation_authority::beat_sent_ms` is the one place that
            // says which reading it is.
            let at = clk.wait;
            let payload = Payload::HolderAlive {
                holder: node_id,
                epoch: t.epoch,
                candidacy,
                listed,
                beat: constellation_net::Beat {
                    seq,
                    sent_ms: constellation_authority::beat_sent_ms(clk),
                    expires_unix_ms: t.until_unix_ms,
                    guard_ms: t.guard_wait,
                    expires_ms: t.until_wait,
                    clock: constellation_platform::clock::protocol().id(),
                },
            };
            let peers = peers.clone();
            let core = core.clone();
            let pending = pending.clone();
            // Answered late is still answered (a loaded peer's runtime
            // takes its time): a request that times out marks the peer's
            // link down in the directory, and timing beats out at the
            // interval had the holder removing a live backup for "link
            // down". So a beat waits `BEAT_TIMEOUT` for its answer, and
            // the next ones go out meanwhile, every interval, up to
            // `beats_outstanding_max` (one beat in flight per peer, as
            // before, silenced the heartbeat for as long as one answer
            // took: up to 5 s against the backup's 1.5 s watch).
            tokio::spawn(async move {
                let answered = peers
                    .request_to_node_liveness(to, &payload, BEAT_TIMEOUT)
                    .await;
                beat_done(&mut pending.lock().unwrap(), to);
                let responsive = matches!(
                    answered,
                    Ok(Payload::HolderAliveAck {
                        core_responsive: true
                    })
                );
                if listed && responsive {
                    let _ = core.send(Internal::Event(Event::BackupAlive { from: to, at }));
                }
            });
        }
    }
}

/// The off-core delegation renewal lane's part of the heartbeat thread's
/// tick (`crate::authority_driver::holder_alive_loop`, the lane
/// `constellation_authority::deleg_lane` specifies).
///
/// A delegate's renewal is a step of its core. A core stuck in one step —
/// a priority metadata write waiting for the store's writer lock behind a
/// slow journal write, a blocking log write — for longer than the grant's
/// slack renewed nothing, and the root reclaimed a delegation whose node
/// was alive (`delegate-store-stall`: a 9 s store stall, 6 of 6 runs). While
/// the driver has been stuck for a quarter of the renewal period (at most
/// `DELEG_LANE_STUCK_MS`) and not longer than the hung bound
/// ([`holder_stall_ms`]), this sends each
/// renewal that is due from the certificates the core published after its
/// last step, on the liveness lane (a connection and a runtime of the
/// thread's own). Each send is registered with the core on the urgent lane
/// first, so the answer, which comes in on that lane too, is known when it
/// arrives: the core counts it as a late answer, honoured from the send
/// (`Core::on_deleg_renew_sent_off_core`). The lane only sends. It never
/// sets a delegation's honoured end, its lapse, its parked ops or its
/// execution, and logs nothing per send. `CONSTELLATION_DELEG_LANE=off`
/// turns it off (A/B runs, the fail-before of `delegate-store-stall`).
struct DelegLane {
    on: bool,
    seq: u64,
    /// The latest send per generation and stream round (honour clock).
    sent: HashMap<(u64, u64), HonourMs>,
}

impl DelegLane {
    fn new(on: bool) -> Self {
        Self {
            on,
            seq: 0,
            sent: HashMap::new(),
        }
    }

    fn from_env() -> Self {
        Self::new(!matches!(
            std::env::var("CONSTELLATION_DELEG_LANE")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false"
        ))
    }

    /// The renewals to send now, as `(root, payload)`; each is registered
    /// on the urgent lane already (`SyncRequest::DelegRenewSentOffCore`,
    /// with `now` as its send time: read before the send, so the grant
    /// is honoured no later than the root's receipt allows). One that
    /// cannot be registered (no driver) is not sent.
    fn tick(
        &mut self,
        off_core: &OffCore,
        node_id: NodeId,
        now: HonourMs,
        stuck_ms: i64,
        hung_ms: i64,
    ) -> Vec<(NodeId, Payload)> {
        // The common case, a driver that progresses: two compares.
        if !self.on
            || !(constellation_authority::deleg_lane::DELEG_LANE_STUCK_MIN_MS..=hung_ms)
                .contains(&stuck_ms)
        {
            return Vec::new();
        }
        let mut certs = off_core.deleg_certs.lock().unwrap().clone();
        {
            // The root's latest grant of each, as its answer reached the P2P
            // service: shorter than the core's period if the root capped it.
            let granted = off_core.granted_ttl.lock().unwrap();
            for c in certs.iter_mut() {
                // Only the root the certificate names: another node's
                // answer says nothing of this grant.
                if let Some((from, ttl)) = granted.get(&(c.gen, c.round)) {
                    if *from == c.root {
                        *c = constellation_authority::deleg_lane::with_granted_ttl(c, *ttl);
                    }
                }
            }
        }
        if certs.is_empty() {
            self.sent.clear();
            return Vec::new();
        }
        self.sent
            .retain(|k, _| certs.iter().any(|c| (c.gen, c.round) == *k));
        // What went out: the lane's own sends, and the core's that the
        // driver dispatched since the certificate was made.
        let mut went_out = self.sent.clone();
        for (k, at) in off_core.renewals_dispatched.lock().unwrap().iter() {
            let e = went_out.entry(*k).or_insert(HonourMs(*at));
            *e = (*e).max(HonourMs(*at));
        }
        let mut out = Vec::new();
        for c in constellation_authority::deleg_lane::due(&certs, now, stuck_ms, hung_ms, &went_out)
        {
            if crate::fault::p2p_denied(c.root) {
                continue;
            }
            self.seq += 1;
            let req_id = constellation_authority::deleg_lane::LANE_REQ_BIT | self.seq;
            let registered = off_core.urgent.lock().unwrap().as_ref().is_some_and(|tx| {
                tx.send(SyncRequest::DelegRenewSentOffCore {
                    gen: c.gen,
                    round: c.round,
                    to: c.root,
                    req_id,
                    sent: now.0,
                })
                .is_ok()
            });
            if !registered {
                continue;
            }
            self.sent.insert((c.gen, c.round), now);
            out.push((
                c.root,
                Payload::DelegRenewOffCore {
                    from: node_id,
                    req_id,
                    gen: c.gen,
                    round: c.round,
                },
            ));
        }
        out
    }
}

/// The shared runtime's lag probe: a task of the daemon's runtime that
/// wakes every [`PROBE_EVERY_MS`] and records how late it woke
/// ([`OffCore::note_probe`]). A lapse of at least the lag threshold is
/// this node's own: what its runtime stamped as an arrival meanwhile, it
/// stamped late, and the silence watches do not count it
/// (`Event::OwnLag`, [`Driver::hand_lag`]); a probe that stops ticking
/// for [`holder_stall_ms`] stops the holder's heartbeat
/// ([`alive_tick`]). About 20 wake-ups a second, each two atomic
/// stores.
pub(crate) async fn runtime_probe_task(off_core: Arc<OffCore>) {
    let every = Duration::from_millis(PROBE_EVERY_MS as u64);
    let mut due = tokio::time::Instant::now() + every;
    loop {
        tokio::time::sleep_until(due).await;
        let woke = tokio::time::Instant::now();
        let late = woke.saturating_duration_since(due).as_millis() as i64;
        off_core.note_probe(wait_now_ms(), late);
        due = woke + every;
    }
}

/// The lag probe's period ([`runtime_probe_task`]).
const PROBE_EVERY_MS: i64 = 50;

/// How long one beat of [`holder_alive_loop`] waits for its answer.
const BEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// The most beats of [`holder_alive_loop`] in flight to one peer: a new
/// one every interval for as long as one waits for its answer
/// ([`BEAT_TIMEOUT`]), so a slow answer never delays the next beat, and
/// no more (a peer that answers nothing costs a bounded number of
/// requests).
fn beats_outstanding_max(every: Duration) -> usize {
    let every = every.as_millis().max(1);
    (BEAT_TIMEOUT.as_millis().div_ceil(every) as usize).clamp(1, 64)
}

/// Take a beat slot for `to` if fewer than `cap` are in flight.
fn beat_slot(pending: &mut HashMap<NodeId, usize>, to: NodeId, cap: usize) -> bool {
    let n = pending.entry(to).or_default();
    if *n >= cap {
        return false;
    }
    *n += 1;
    true
}

/// A beat to `to` was answered (or timed out).
fn beat_done(pending: &mut HashMap<NodeId, usize>, to: NodeId) {
    if let Some(n) = pending.get_mut(&to) {
        *n = n.saturating_sub(1);
        if *n == 0 {
            pending.remove(&to);
        }
    }
}

/// [`holder_alive_loop`] (on its thread) and [`runtime_probe_task`] for
/// the life of [`Driver::run`]: dropped when `run` returns or its task
/// dies, it forgets the targets (no beat goes out after that) and stops
/// both; the thread ends within an interval.
struct AliveTask {
    stop: Arc<std::sync::atomic::AtomicBool>,
    off_core: Arc<OffCore>,
    probe: tokio::task::JoinHandle<()>,
    alive: Arc<Mutex<Option<AliveTargets>>>,
}

impl AliveTask {
    /// Start the beat thread (a current-thread runtime of its own) and
    /// the lag probe (on the caller's, the shared, runtime).
    #[allow(clippy::too_many_arguments)]
    fn start(
        peers: constellation_net::Peers,
        alive: Arc<Mutex<Option<AliveTargets>>>,
        off_core: Arc<OffCore>,
        (every, stall): (Duration, i64),
        node_id: NodeId,
        lanes: (
            mpsc::UnboundedSender<Internal>,
            mpsc::UnboundedSender<Internal>,
        ),
        loop_thread: Arc<Mutex<constellation_platform::process::ThreadRef>>,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = tokio::spawn(runtime_probe_task(off_core.clone()));
        let probe_owner = off_core.clone();
        let beat = {
            let (alive, stop) = (alive.clone(), stop.clone());
            move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        tracing::error!(%error, "no runtime for the holder's heartbeat");
                        return;
                    }
                };
                rt.block_on(holder_alive_loop(
                    peers,
                    alive,
                    off_core,
                    (every, stall),
                    node_id,
                    lanes,
                    loop_thread,
                    stop,
                ));
            }
        };
        if let Err(error) = std::thread::Builder::new()
            .name("cst-holder-beat".into())
            .spawn(beat)
        {
            tracing::error!(%error, "could not start the holder's heartbeat thread");
        }
        Self {
            stop,
            off_core: probe_owner,
            probe,
            alive,
        }
    }
}

impl Drop for AliveTask {
    fn drop(&mut self) {
        if let Ok(mut alive) = self.alive.lock() {
            *alive = None;
        }
        self.stop.store(true, Ordering::Relaxed);
        self.probe.abort();
        // A probe that no longer runs is not a stalled runtime
        // ([`OffCore::runtime_stalled`]).
        self.off_core.probe_stopped();
    }
}

/// What this node's peer service (`crate::p2p::P2pBridge`) and its
/// authority driver share outside the core's step, where a slow step
/// cannot delay it:
///
/// - **When each holder was last heard from**, as a backup: the arrival
///   of its latest heartbeat (`HolderAlive`) or append, by holder and
///   epoch, with the lease expiry that heartbeat named. The core's seal
///   watch measures the holder's silence and reads its lease's expiry
///   from these, however far behind its own steps are; fed
///   only by the events it handled, it measured this node's own backlog
///   too — a backup whose steps took seconds sealed a live holder that
///   was sending to it the whole time (`silent_ms=21872`). The driver
///   hands the latest arrivals to the core right before the watch is
///   evaluated ([`Driver::hand_heard`]).
/// - **The driver loop's progress stamp** (unix ms of its last wake-up or
///   step, 0 while it waits for work): the holder stops its heartbeat
///   when its own loop is stuck ([`holder_alive_loop`]), and a backup
///   answers a heartbeat with whether its loop is
///   ([`OffCore::core_responsive`]).
/// - **The shared runtime's lag** ([`runtime_probe_task`]): its probe's
///   latest tick and the end of its latest lapse of at least
///   `lag_threshold_ms`, for the silence watches ([`Driver::hand_lag`])
///   and the holder's heartbeat ([`alive_tick`]).
#[derive(Debug)]
pub struct OffCore {
    busy_since: std::sync::atomic::AtomicI64,
    /// holder → (its latest epoch heard, the latest arrival at it, the
    /// latest lease expiry a heartbeat of that epoch named; 0: none).
    heard: Mutex<HashMap<NodeId, Heard>>,
    responsive_ms: i64,
    /// The lag probe's latest tick (unix ms; 0: not started).
    probe_tick: std::sync::atomic::AtomicI64,
    /// The probe's latest lapse of at least `lag_threshold_ms`, `(from,
    /// until)` in unix ms (`(0, 0)`: none).
    lapse: Mutex<(i64, i64)>,
    /// What counts as a lapse: the core's lag threshold, an eighth of
    /// `backup_takeover_ms`.
    lag_threshold_ms: i64,
    /// When the holder's heartbeat sent the driver loop the echo it has
    /// not taken yet (`Internal::Echo`; unix ms, 0: none outstanding).
    pub(crate) echo_sent: std::sync::atomic::AtomicI64,
    /// The delegation renewal certificates, published after every step
    /// (`Core::deleg_renew_certs`), and the urgent lane's sender, for the
    /// off-core delegation renewal lane ([`holder_alive_loop`]).
    pub(crate) deleg_certs: Mutex<Vec<constellation_authority::DelegRenewCert>>,
    pub(crate) urgent: Mutex<Option<mpsc::UnboundedSender<SyncRequest>>>,
    /// When the driver dispatched the core's latest renewal of each
    /// generation and round (this node's honour clock, the certificates'
    /// own: plan 43 C7-H1), kept for the certificates' keys: a
    /// renewal the core decided on may wait behind a blocking action of the
    /// same step, and the lane counts only what went out.
    pub(crate) renewals_dispatched: Mutex<HashMap<(u64, u64), i64>>,
    /// When the heartbeat thread's next tick is due (unix ms; 0: it has not
    /// started), written at every tick: a thread that has not run since
    /// is frozen, and the driver, waking first, can say so before the
    /// thread can ([`deleg_lane::freezes`]).
    beat_due: std::sync::atomic::AtomicI64,
    /// The last freeze the thread saw end, `(from, length)` packed in one
    /// word ([`deleg_lane::pack_freeze`]: one atomic, no torn read).
    beat_freeze: std::sync::atomic::AtomicU64,
    /// The latest grant (ttl, ms) an answer of the root named, per
    /// generation and round, noted by the P2P service as it arrives
    /// (`note_granted_ttl`): the lane's period follows it
    /// ([`deleg_lane::with_granted_ttl`]), where the core's bookkeeping is
    /// behind a stuck step. The root caps a grant by its own lease.
    pub(crate) granted_ttl: Mutex<HashMap<(u64, u64), (NodeId, u64)>>,
}

impl OffCore {
    /// `responsive_ms`: the longest the driver loop may be inside one
    /// wake-up and still count as responsive ([`Self::core_responsive`]).
    /// The lag threshold is the default takeover window's (1.5 s / 8).
    pub fn new(responsive_ms: u64) -> Self {
        Self {
            busy_since: std::sync::atomic::AtomicI64::new(0),
            heard: Mutex::new(HashMap::new()),
            responsive_ms: responsive_ms.max(1) as i64,
            probe_tick: std::sync::atomic::AtomicI64::new(0),
            lapse: Mutex::new((0, 0)),
            lag_threshold_ms: 1_500 / 8,
            echo_sent: std::sync::atomic::AtomicI64::new(0),
            deleg_certs: Mutex::new(Vec::new()),
            urgent: Mutex::new(None),
            renewals_dispatched: Mutex::new(HashMap::new()),
            beat_due: std::sync::atomic::AtomicI64::new(0),
            beat_freeze: std::sync::atomic::AtomicU64::new(0),
            granted_ttl: Mutex::new(HashMap::new()),
        }
    }

    /// The core's renewal of `(gen, round)` was dispatched at `at_honour_ms`
    /// (a reading of the honour clock).
    pub(crate) fn note_renewal_dispatched(&self, gen: u64, round: u64, at_honour_ms: i64) {
        self.renewals_dispatched
            .lock()
            .unwrap()
            .insert((gen, round), at_honour_ms);
    }

    /// The heartbeat thread's next tick is due at `at_unix_ms`.
    pub(crate) fn note_beat_due(&self, at_unix_ms: i64) {
        self.beat_due.store(at_unix_ms, Ordering::Relaxed);
    }

    /// The heartbeat thread ran `len_ms` late: it did not run from
    /// `from_unix_ms` (and neither did anything else in the process).
    pub(crate) fn note_beat_freeze(&self, from_unix_ms: i64, len_ms: i64) {
        self.beat_freeze.store(
            constellation_authority::deleg_lane::pack_freeze(from_unix_ms, len_ms),
            Ordering::Relaxed,
        );
    }

    /// The freezes of the heartbeat thread as of `now_wait_ms`, oldest
    /// first (`deleg_lane::freezes`).
    pub(crate) fn beat_freezes(&self, now_wait_ms: i64) -> Vec<(i64, i64)> {
        constellation_authority::deleg_lane::freezes(
            self.beat_freeze.load(Ordering::Relaxed),
            self.beat_due.load(Ordering::Relaxed),
            now_wait_ms,
        )
    }

    /// An answer of `from` granted `ttl_ms` of generation `gen`, round
    /// `round` (the P2P service, off the core). The lane uses it only if
    /// `from` is the root its certificate names.
    pub(crate) fn note_granted_ttl(&self, from: NodeId, gen: u64, round: u64, ttl_ms: u64) {
        self.granted_ttl
            .lock()
            .unwrap()
            .insert((gen, round), (from, ttl_ms));
    }

    /// The lane's request `req_id` to `to` failed at the transport: the core
    /// is told on the urgent lane, after the lane's registration of it (one
    /// FIFO), so it drops the pending entry the registration made.
    pub(crate) fn lane_send_failed(&self, to: NodeId, req_id: u64) {
        if let Some(tx) = self.urgent.lock().unwrap().as_ref() {
            let _ = tx.send(SyncRequest::DelegRenewOffCoreFailed { to, req_id });
        }
    }

    /// The freezes of the heartbeat thread (`beat_freezes`) that were the
    /// whole process's: the shared runtime's lag probe missed the same
    /// interval ([`deleg_lane::whole_process`]). A heartbeat thread held up
    /// alone, in a log line or on a mutex, is not handed to the core as a
    /// freeze: the lane's thread failed, the loss is not excused.
    pub(crate) fn whole_process_freezes(&self, now_wait_ms: i64) -> Vec<(i64, i64)> {
        let freezes = self.beat_freezes(now_wait_ms);
        if freezes.is_empty() {
            return freezes;
        }
        let runtime = self.lapse(now_wait_ms);
        freezes
            .into_iter()
            .filter(|f| constellation_authority::deleg_lane::whole_process(*f, runtime))
            .collect()
    }

    /// The default for a node with `config`: half the holder's bound for
    /// a slow backup (`backup_slow_max_ms`, 10 s: 5 s). A backup whose
    /// steps take seconds (an overloaded host) is responsive and kept
    /// that long; one whose loop is stuck in one wake-up longer answers
    /// that it is not, and is removed at the ordinary acknowledgement
    /// timeout instead of being waited for. Lapses count from an eighth
    /// of `backup_takeover_ms` (the core's threshold).
    pub fn for_config(config: &Config) -> Self {
        Self {
            lag_threshold_ms: (config.backup_takeover_ms as i64 / 8).max(1),
            ..Self::new(config.backup_slow_max_ms / 2)
        }
    }

    /// One tick of the lag probe at `now_wait_ms`, `late_ms` after it
    /// was due.
    pub(crate) fn note_probe(&self, now_wait_ms: i64, late_ms: i64) {
        if late_ms >= self.lag_threshold_ms {
            *self.lapse.lock().unwrap() = (now_wait_ms - late_ms, now_wait_ms);
        }
        self.probe_tick.store(now_wait_ms, Ordering::Relaxed);
    }

    /// Only the latest lapse is kept: two the probe records while the
    /// driver is inside one long step reach the core as the second alone.
    /// That undercounts the node's lapses, which can only shorten
    /// `Core::attentive_since` — an earlier seal, towards the plain
    /// window, never a later one than the stated bound. A queue would need
    /// that bound re-derived.
    ///
    /// This node's latest lapse as of `now_wait_ms`, `(from, until)`
    /// (`(0, 0)`: none): the probe's latest, or the one in progress if
    /// the probe itself is overdue by the threshold — the runtime is
    /// being held up right now, and the probe cannot say so before it
    /// runs (it says so with the same start once it does).
    pub(crate) fn lapse(&self, now_wait_ms: i64) -> (i64, i64) {
        let tick = self.probe_tick.load(Ordering::Relaxed);
        if tick != 0 && now_wait_ms - tick >= PROBE_EVERY_MS + self.lag_threshold_ms {
            return (tick + PROBE_EVERY_MS, now_wait_ms);
        }
        *self.lapse.lock().unwrap()
    }

    /// How long past its period the lag probe has not ticked (0: it is
    /// ticking, or not started).
    pub(crate) fn runtime_stalled_ms(&self, now_wait_ms: i64) -> i64 {
        let tick = self.probe_tick.load(Ordering::Relaxed);
        if tick == 0 {
            return 0;
        }
        (now_wait_ms - tick - PROBE_EVERY_MS).max(0)
    }

    /// How long the lag probe has not ticked past its period: the shared
    /// runtime has run nothing for that long (zero while it ticks, and
    /// while no probe runs: before the driver starts, after it ends).
    /// `node.ping` fails past a bound of it
    /// ([`crate::control::PING_FAILS_AFTER`]).
    pub(crate) fn runtime_stalled(&self) -> Duration {
        Duration::from_millis(self.runtime_stalled_ms(wait_now_ms()) as u64)
    }

    /// The probe was stopped with the driver: not started, as far as
    /// [`Self::runtime_stalled`] is concerned.
    pub(crate) fn probe_stopped(&self) {
        self.probe_tick.store(0, Ordering::Relaxed);
    }

    pub(crate) fn set_busy(&self, at_unix_ms: i64) {
        self.busy_since.store(at_unix_ms, Ordering::Relaxed);
    }

    /// The driver loop woke at `at_unix_ms`, whatever lane woke it: it is
    /// busy from now, and it answers the heartbeat's echo by iterating at
    /// all — not only by taking the echo, which rides the prompt lane and
    /// waits behind the urgent one (a loop saturated by urgent traffic
    /// — lock renewals, other holders' beats — still iterates, and is not
    /// hung).
    pub(crate) fn loop_woke(&self, at_unix_ms: i64) {
        self.set_busy(at_unix_ms);
        self.echo_sent.store(0, Ordering::Relaxed);
    }

    pub(crate) fn busy_since(&self) -> i64 {
        self.busy_since.load(Ordering::Relaxed)
    }

    /// Whether the driver loop is waiting for work or has progressed
    /// within `responsive_ms`.
    pub(crate) fn core_responsive(&self, now_wait_ms: i64) -> bool {
        let busy = self.busy_since();
        busy == 0 || now_wait_ms - busy <= self.responsive_ms
    }

    /// The holder `holder` was heard from at `epoch` (a heartbeat or an
    /// append arrived) at `at_unix_ms`; both name the expiry of the
    /// holder's lease, `expires_unix_ms` (0: none held). An older
    /// epoch's is ignored.
    pub(crate) fn note_holder_heard(
        &self,
        holder: NodeId,
        epoch: u64,
        at_unix_ms: i64,
        expires_unix_ms: i64,
    ) {
        let mut heard = self.heard.lock().unwrap();
        let e = heard
            .entry(holder)
            .or_insert((epoch, at_unix_ms, expires_unix_ms));
        if epoch > e.0 {
            *e = (epoch, at_unix_ms, expires_unix_ms);
        } else if epoch == e.0 {
            e.1 = e.1.max(at_unix_ms);
            e.2 = e.2.max(expires_unix_ms);
        }
    }

    fn heard(&self) -> HashMap<NodeId, Heard> {
        self.heard.lock().unwrap().clone()
    }
}

/// A holder as the peer service heard it: `(epoch, arrival, expiry)`
/// (unix ms; the expiry 0: none named yet).
type Heard = (u64, i64, i64);

/// The holders heard from since what was handed to the core
/// (`handed`, updated): `(holder, epoch, arrival, expiry)`. A newer
/// expiry alone (an arrival that named a later one at the same stamp)
/// is fresh too.
fn fresh_heard(
    heard: HashMap<NodeId, Heard>,
    handed: &mut HashMap<NodeId, Heard>,
) -> Vec<(NodeId, u64, i64, i64)> {
    let mut fresh: Vec<(NodeId, u64, i64, i64)> = heard
        .into_iter()
        .filter(|(holder, now)| handed.get(holder).is_none_or(|was| now > was))
        .map(|(holder, (epoch, at, expires))| (holder, epoch, at, expires))
        .collect();
    fresh.sort_unstable();
    for &(holder, epoch, at, expires) in &fresh {
        handed.insert(holder, (epoch, at, expires));
    }
    fresh
}

/// An owner's expiry of a grant it handed out (a lock grant, a
/// delegation): it outwaits the holder's renewal, which reaches the
/// driver on the urgent lane ([`DriverDeps::urgent_rx`]); what was
/// queued there when it fired is handled first ([`Driver::run`]).
fn waits_for_renewals(kind: TimerKind) -> bool {
    matches!(kind, TimerKind::LockGrantExpiry | TimerKind::DelegExpiry)
}

/// The timers that keep a grant this node holds: a delegate's renewal
/// and lapse, and a lock holder's renewal tick. Delivered on a lane
/// served before the internal channel ([`Driver::run`]): a backlog of
/// frames there must not hold back a renewal (the owner outwaits a silent
/// holder) nor the lapse (the grant is not honoured past it). The lapse
/// still goes after the renewal answers queued on the urgent lane, which
/// is served first. A lock holder honours a grant from its renewal's
/// send and drops a lapsed one at the tick, so a tick taken earlier only
/// renews sooner; the renewal's timeout stays a silence timer.
fn prompt_timer(kind: TimerKind) -> bool {
    matches!(
        kind,
        TimerKind::DelegRenew | TimerKind::DelegLapse | TimerKind::LockRenewTick
    )
}

/// The core's event for the answer to a lock renewal: the owner's
/// `LockRenewed`, or `PeerFailed` when none came.
fn lock_renew_event(
    to: NodeId,
    req: OpId,
    results: Option<LockRenewResults>,
    outage: bool,
) -> Event {
    match results {
        Some(results) => Event::Peer {
            from: to,
            msg: PeerMsg::LockRenewed { req, results },
        },
        None => Event::PeerFailed { req, to, outage },
    }
}

/// Hand the driver the answer to this node's lock renewal `req`
/// ([`Driver::lock_renew_rpc`]): on the urgent lane when there is one
/// (a closed one is a driver that is gone: it holds the receiver until
/// it stops), else on the internal channel.
fn lock_renew_answer(
    urgent: Option<&mpsc::UnboundedSender<SyncRequest>>,
    int_tx: &mpsc::UnboundedSender<Internal>,
    to: NodeId,
    req: OpId,
    results: Option<LockRenewResults>,
    outage: bool,
) {
    match urgent {
        Some(urgent) => {
            let _ = urgent.send(SyncRequest::LockRenewAnswered {
                to,
                req_id: req.0,
                results,
                outage,
            });
        }
        None => {
            let _ = int_tx.send(Internal::Event(lock_renew_event(to, req, results, outage)));
        }
    }
}

/// A timer that measures a peer's silence: it is handled after the
/// requests and peer messages that reached this node before it fired
/// (the internal channel is served before them, so after a long step a
/// due timer overtook them), for at most [`SILENCE_DRAIN_BUDGET`]. Not the
/// backup's seal watch: it decides on the holder's arrival stamps
/// ([`Driver::hand_heard`]), which no backlog delays, and a drain only
/// made it later.
fn measures_silence(kind: TimerKind) -> bool {
    matches!(
        kind,
        TimerKind::ForwardTimeout
            | TimerKind::JobRequestTimeout
            | TimerKind::ReadIndexTimeout
            | TimerKind::LockRequestTimeout
            | TimerKind::LockRenewTimeout
            | TimerKind::LockTestTimeout
            | TimerKind::LockGrantExpiry
            | TimerKind::DelegExpiry
            | TimerKind::GrantExpiry
            | TimerKind::StreamWatchdog
    )
}

/// The most a silence timer's drain ([`measures_silence`]) spends on the
/// backlog before the timer is put back behind what else is due, once.
const SILENCE_DRAIN_BUDGET: Duration = Duration::from_millis(500);

/// How a drain before a silence timer ended.
#[derive(Debug, PartialEq, Eq)]
enum Drained {
    /// Everything queued before it was handled.
    All,
    /// The budget ran out with some still queued.
    OutOfTime,
    /// The core stopped.
    Stopped,
}

/// Handle up to `pending` queued items, for at most `budget`:
/// `handle_next` handles the next one (`None`: the queue is empty;
/// `Some(false)`: the core stopped).
fn drain_queued(
    pending: usize,
    budget: Duration,
    mut handle_next: impl FnMut() -> Option<bool>,
) -> Drained {
    let started = std::time::Instant::now();
    for _ in 0..pending {
        if started.elapsed() >= budget {
            return Drained::OutOfTime;
        }
        match handle_next() {
            None => break,
            Some(false) => return Drained::Stopped,
            Some(true) => {}
        }
    }
    Drained::All
}

impl Driver {
    /// Build the driver; `run` drives it. `sync_tx` is what the rest of
    /// the daemon sends `SyncRequest`s on.
    pub fn new(
        mut deps: DriverDeps,
        sync_tx: mpsc::UnboundedSender<SyncRequest>,
        sync_rx: mpsc::UnboundedReceiver<SyncRequest>,
    ) -> Self {
        let urgent_rx = deps.urgent_rx.take();
        let node_id = deps.config.node_id;
        let core = Core::new(deps.config.clone());
        let leases = LeaseStore::new(deps.store_inner.clone(), PARTITION, deps.lease_mode);
        let inbox = match &deps.e2e {
            Some(keys) => InboxStore::new_e2e(deps.store_inner.clone(), keys.clone()),
            None => InboxStore::new(deps.store_inner.clone()),
        };
        let (int_tx, int_rx) = mpsc::unbounded_channel();
        let (prompt_tx, prompt_rx) = mpsc::unbounded_channel();
        Self {
            core,
            deps,
            node_id,
            leases,
            inbox,
            int_tx,
            int_rx,
            prompt_tx,
            prompt_rx,
            log_permits: Arc::new(tokio::sync::Semaphore::new(log_backlog_bytes())),
            sync_tx,
            sync_rx,
            replies: HashMap::new(),
            mutate_replies: HashMap::new(),
            handoff_replies: HashMap::new(),
            read_index_replies: HashMap::new(),
            recall_replies: HashMap::new(),
            deleg_recall_replies: HashMap::new(),
            lock_request_replies: HashMap::new(),
            lock_recall_replies: HashMap::new(),
            lock_renew_replies: HashMap::new(),
            lock_test_replies: HashMap::new(),
            deleg_backup_replies: HashMap::new(),
            deleg_seal_replies: HashMap::new(),
            promise_replies: HashMap::new(),
            backup_replies: HashMap::new(),
            controls: HashMap::new(),
            next_control: 1 << 48,
            last_epoch_reported: None,
            subscribers: HashMap::new(),
            subscriptions: HashMap::new(),
            stream_buffer_bytes: log_stream_buffer_bytes() as u64,
            bulk_pass: BulkPass::default(),
            round_upload_wait: round_upload_wait(),
            alive: Arc::default(),
            urgent_rx,
            heard_handed: HashMap::new(),
            lag_handed: 0,
            freeze_handed: (0, 0),
            loop_thread: Arc::new(Mutex::new(
                constellation_platform::process::ThreadRef::unknown(),
            )),
            alive_book: AliveBook::default(),
            step_end: std::time::Instant::now(),
            profile: StepProfile::default(),
            step_tail: StepTail::new(),
            job_since: None,
            clock_steps_logged: 0,
            now_at: std::time::Instant::now(),
        }
    }

    /// Plan 43 C7-H1: say so when the node's wall clock stepped. The
    /// protocol clock counts the steps it sees (`types::clock`); nothing
    /// in the protocol acts on them: the honour clock follows a forward
    /// step once and neither it nor the wait clock moves for a backward
    /// one. The log line is the operator's trace of why a held window
    /// ended early.
    fn note_clock_steps(&mut self) {
        let clock = constellation_platform::clock::protocol();
        let seen = clock.step_count();
        if seen == self.clock_steps_logged {
            return;
        }
        self.clock_steps_logged = seen;
        let stats = clock.step_stats();
        let (at, delta) = stats.last_step.unwrap_or((0, 0));
        tracing::warn!(
            node = self.node_id,
            delta_ms = delta,
            wall_ms = at,
            steps_forward = stats.steps_forward,
            steps_backward = stats.steps_backward,
            "the wall clock stepped; honour and wait clocks unaffected (a forward step ends \
             held windows early; plan 43 C7-H1)"
        );
    }

    /// The driver's own event sender (tickers spawned by `run`).
    fn event_tx(&self) -> mpsc::UnboundedSender<Internal> {
        self.int_tx.clone()
    }

    pub async fn run(mut self) {
        let mut out = Vec::new();
        self.now_at = std::time::Instant::now();
        let start = now();
        self.core.start(start, &*self.deps.meta, &mut out);
        self.refresh();
        self.dispatch(out);
        self.report_epoch(true);
        // EC2 finding 1 × 4798008: chunks `back` closes forwarded as
        // pending are handed to a peer while this node's S3 path makes no
        // progress (checked every second; independent of the rounds,
        // which may be stuck on the same unreachable S3).
        {
            let handoff = self.chunk_handoff();
            if handoff.possible() {
                tokio::spawn(forwarded_handoff_watch(handoff));
            }
        }
        // The peer directory and the roster, on a ticker like the
        // daemon's registry poll; with them, this node's own S3 path
        // (EC2 campaign 8 A-1, `Event::OwnS3`). The links also go out as
        // soon as the directory changes (`startup-link-lag`).
        {
            let tx = self.event_tx();
            let peers = self.deps.peers.clone();
            let own_s3 = OwnS3Watch::new(self.deps.upload.clone(), peers.clone());
            tokio::spawn(peer_links_task(peers, own_s3, tx));
        }
        *self.deps.off_core.urgent.lock().unwrap() = self.deps.urgent_tx.clone();
        let _alive_task = self.core.config().p2p.then(|| {
            AliveTask::start(
                self.deps.peers.clone(),
                self.alive.clone(),
                self.deps.off_core.clone(),
                (
                    Duration::from_millis(self.core.config().backup_heartbeat_ms.max(1)),
                    holder_stall_ms(),
                ),
                self.node_id,
                (self.int_tx.clone(), self.prompt_tx.clone()),
                self.loop_thread.clone(),
            )
        });
        crate::fault::start_runtime_hog_file(self.node_id);
        crate::fault::start_clock_faults();
        self.step_end = std::time::Instant::now();
        enum Wake {
            Internal(Internal),
            Request(SyncRequest),
        }
        loop {
            // Waiting for work is not being stuck (`holder_alive_loop`).
            self.deps.off_core.set_busy(0);
            let urgent_open = self.urgent_rx.is_some();
            // The urgent lane first (with the answers to this node's own
            // lock renewals), then the timers that keep a grant held here
            // (a delegate's renewal and lapse, the lock renewal tick),
            // then the internal channel: a grant must not wait behind its
            // holder's own backlog. With the internal channel first, a
            // delegate whose log-stream frames queued 17 s deep sent its
            // renewal and took the root's answer that late, and lapsed
            // (`stress-ng-fs-nodes`: a flush parked 47 s). Nothing the
            // lanes carry is ordered against the internal channel: peer
            // messages are not ordered against each other in transit, and
            // a renewal answer or a heartbeat taken before an S3 result or
            // a frame is the same as one that arrived a moment earlier.
            let wake = tokio::select! {
                biased;
                req = async {
                    match self.urgent_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                }, if urgent_open => match req {
                    Some(req) => Wake::Request(req),
                    None => {
                        self.urgent_rx = None;
                        continue;
                    }
                },
                msg = self.prompt_rx.recv() => match msg {
                    Some(m) => Wake::Internal(m),
                    None => break,
                },
                msg = self.int_rx.recv() => match msg {
                    Some(m) => Wake::Internal(m),
                    None => break,
                },
                req = self.sync_rx.recv() => match req {
                    Some(req) => Wake::Request(req),
                    None => break,
                },
            };
            self.deps.off_core.loop_woke(wait_now_ms());
            self.note_clock_steps();
            *self.loop_thread.lock().unwrap() =
                constellation_platform::native().process.current_thread();
            self.hand_lag();
            self.hand_freeze();
            let internal = match wake {
                // Answered by the wake-up itself (`loop_woke`); the echo
                // is what wakes a loop that is waiting for work.
                Wake::Internal(Internal::Echo) => continue,
                Wake::Internal(internal) => internal,
                Wake::Request(req) => match self.on_request(req) {
                    Some(internal) => internal,
                    None => continue,
                },
            };
            if let Internal::Timer { id, kind, deferred } = &internal {
                let (id, kind, deferred) = (*id, *kind, *deferred);
                // A backup's seal watch decides on when the holder was last
                // heard from — its beats' and appends' arrival here, not
                // when this node's core got to them: a backup whose steps
                // took seconds sealed a live holder `silent_ms=21872` in
                // with its appends queued (`stress-ng-fs-nodes`).
                if kind == TimerKind::BackupWatch {
                    self.hand_heard();
                }
                // An owner's expiry of a grant it handed out goes after
                // the renewals that reached this node before it fired (the
                // urgent lane: a handful, cheap): handled first, they only
                // make the owner wait longer, and the holder honours its
                // grant from its own send, so a later expiry is never
                // unsafe (before, the owner dropped the grant and answered
                // the queued renewal `Lost`).
                if waits_for_renewals(kind) {
                    while let Some(req) = self.urgent_rx.as_mut().and_then(|rx| rx.try_recv().ok())
                    {
                        if let Some(earlier) = self.on_request(req) {
                            self.step(earlier);
                        }
                        if self.core.stopped() {
                            return;
                        }
                    }
                }
                // Other silence timers go after the ordinary requests
                // queued when they fired, as before (bounded: past
                // `SILENCE_DRAIN_BUDGET` the timer goes back behind
                // whatever else is due, once, so the proactive timers and
                // S3 completions on the internal channel never wait out
                // the backlog).
                if measures_silence(kind) {
                    let pending = self.sync_rx.len();
                    let drained = drain_queued(pending, SILENCE_DRAIN_BUDGET, || {
                        let req = self.sync_rx.try_recv().ok()?;
                        if let Some(earlier) = self.on_request(req) {
                            self.step(earlier);
                        }
                        Some(!self.core.stopped())
                    });
                    match drained {
                        Drained::Stopped => return,
                        Drained::OutOfTime if !deferred => {
                            let _ = self.int_tx.send(Internal::Timer {
                                id,
                                kind,
                                deferred: true,
                            });
                            continue;
                        }
                        _ => {}
                    }
                }
            }
            self.step(internal);
            if self.core.stopped() {
                break;
            }
        }
    }

    /// Hand the core this node's latest own lapse (`Event::OwnLag`,
    /// [`OffCore::lapse`]) when it ended materially later than the
    /// last one handed (by half the lag threshold): before every wake-up
    /// is handled, so a silence watch never measures across a lapse it
    /// was not told of. The probe's own staleness counts, so a wake-up
    /// right after the runtime was held up hands the lapse over even when
    /// the probe has not run yet. Costs a core step only per lapse.
    fn hand_lag(&mut self) {
        let off = &self.deps.off_core;
        let (from, until) = off.lapse(wait_now_ms());
        if until > self.lag_handed + off.lag_threshold_ms / 2 {
            self.lag_handed = until;
            self.step(Internal::Event(Event::OwnLag {
                from: WaitMs(from),
                until: WaitMs(until),
            }));
        }
    }

    /// Hand the core a freeze of the heartbeat thread it has not been told of
    /// (`Event::OwnFreeze`), before the wake-up is handled: a process that
    /// was frozen wakes its driver and the heartbeat thread together, and the
    /// driver reads the thread's missed tick (`OffCore::beat_freezes`) before
    /// the thread has run to record it. The core only uses it to say, when it
    /// gives a grant up, whether the freeze explains the loss. The same
    /// freeze, grown by more than 200 ms or finished, is handed again.
    fn hand_freeze(&mut self) {
        for (from, until) in self.deps.off_core.whole_process_freezes(wait_now_ms()) {
            let (hf, hu) = self.freeze_handed;
            if from != hf || until > hu + 200 {
                self.freeze_handed = (from, until);
                self.step(Internal::Event(Event::OwnFreeze {
                    from: WaitMs(from),
                    until: WaitMs(until),
                }));
            }
        }
    }

    /// Hand the core the holders heard from since the last hand-off
    /// ([`OffCore`]), as heartbeats at their arrival: the seal watch then
    /// measures the holder's silence, not this node's backlog.
    fn hand_heard(&mut self) {
        let heard = self.deps.off_core.heard();
        for (from, epoch, at, expires) in fresh_heard(heard, &mut self.heard_handed) {
            self.step(Internal::Event(Event::HolderAlive {
                from,
                epoch,
                candidacy: 0,
                listed: true,
                at: WaitMs(at),
                seq: 0,
                clock: 0,
                sent: 0,
                holder_expires: 0,
                guard: 0,
                expires: Ms(expires),
            }));
        }
    }

    /// One core step: the event, the mirror refresh, the actions.
    fn step(&mut self, internal: Internal) {
        let waited_us = self.step_end.elapsed().as_micros() as u64;
        // A frame's permit goes once the step is done.
        let mut _frame = None;
        let event = match internal {
            Internal::Event(event) => event,
            Internal::Frame(event, permit) => {
                _frame = Some(permit);
                event
            }
            Internal::Control { req, reply } => {
                let op = self.control_id();
                self.controls.insert(op, reply);
                Event::Control { op, req }
            }
            Internal::Timer { id, .. } => Event::Timer { id },
            // Taken by the loop itself (`Driver::run`), never stepped.
            Internal::Echo => return,
        };
        self.deps
            .last_sync_ms
            .store(crate::prune::wait_ms(), Ordering::Relaxed);
        let kind = event_kind(&event);
        // Progress (a wake-up may run several steps).
        self.deps.off_core.set_busy(wait_now_ms());
        let started = std::time::Instant::now();
        let int_pending = self.int_rx.len();
        let sync_pending = self.sync_rx.len();
        let applies_segments = matches!(
            &event,
            Event::S3 {
                result: S3Result::SegmentRun(Ok(run)),
                ..
            } if !run.is_empty()
        ) || matches!(
            &event,
            Event::Peer {
                msg: PeerMsg::LogStream {
                    segment: Some(_),
                    ..
                },
                ..
            }
        );
        // The step's metadata writes go ahead of the FUSE workers'
        // (`priority_writes`): behind them, under a busy mount, one
        // step took seconds, and everything the node's authority does
        // waits for the step.
        let ((action_kinds, handled_us, refreshed_us, dispatched_us), store) =
            constellation_meta::priority_writes(|| {
                self.now_at = std::time::Instant::now();
                let actions = self.core.handle(now(), event, &*self.deps.meta);
                if applies_segments {
                    if let Some(holds) = &self.deps.holds {
                        holds.nudge();
                    }
                }
                let handled_us = started.elapsed().as_micros() as u64;
                // The mirror first: a FUSE thread must see the releasing
                // flag before the release's IO starts.
                self.refresh();
                let refreshed_us = started.elapsed().as_micros() as u64 - handled_us;
                let action_kinds: Vec<&'static str> = actions.iter().map(action_kind).collect();
                // Load campaign: a stop between the decision and its
                // dispatch (`CONSTELLATION_FAULT_STEP_PAUSE_FILE`).
                crate::fault::step_pause();
                self.dispatch(actions);
                let dispatched_us =
                    started.elapsed().as_micros() as u64 - handled_us - refreshed_us;
                (action_kinds, handled_us, refreshed_us, dispatched_us)
            });
        tracing::trace!(
            event = kind,
            actions = ?action_kinds,
            job = ?self.core.job(),
            handled_us,
            refreshed_us,
            dispatched_us,
            int_pending,
            sync_pending,
            waited_us,
            "core step"
        );
        let step_us = handled_us + refreshed_us + dispatched_us;
        self.profile.note(
            kind,
            step_us,
            int_pending,
            sync_pending,
            self.urgent_rx.as_ref().map_or(0, |rx| rx.len()),
        );
        self.step_tail.note(step_us / 1_000, kind);
        {
            let mut status = self.deps.status.lock().unwrap();
            status.longest_step_ms = self.step_tail.longest.0;
            status.longest_step_kind = self.step_tail.longest.1;
            status.recent_longest_step_ms = self.step_tail.recent();
        }
        if step_us >= SLOW_STEP_US {
            // Everything the core does (backup heartbeats included)
            // waits for this step: a long one silences the node.
            tracing::warn!(
                event = kind,
                actions = ?action_kinds,
                handled_us,
                refreshed_us,
                dispatched_us,
                store_lock_waits = store.lock_waits,
                store_lock_wait_us = store.lock_wait_us,
                store_lock_wait_max_us = store.lock_wait_max_us,
                store_lock_wait_max_behind = ?store
                    .lock_wait_max_behind
                    .map(|(at, held_us)| format!("{at} (held {} ms)", held_us / 1000)),
                store_syncs = store.syncs,
                store_sync_us = store.sync_us,
                "slow core step"
            );
        }
        self.step_end = std::time::Instant::now();
    }

    fn control_id(&mut self) -> OpId {
        let id = OpId(self.next_control);
        self.next_control += 1;
        id
    }

    /// Mirror the core's lease state into the FUSE-facing view and the
    /// status snapshot.
    fn refresh(&mut self) {
        let now = now();
        self.publish_alive_targets(now.wait);
        {
            // Compared in place: nothing is allocated on a step that
            // changed no certificate (the lock and the walk over the
            // node's one delegation are the whole cost).
            let mut c = self.deps.off_core.deleg_certs.lock().unwrap();
            if self.core.deleg_renew_certs_differ(&c) {
                *c = self.core.deleg_renew_certs();
                let keep = |k: &(u64, u64)| c.iter().any(|x| (x.gen, x.round) == *k);
                let off = &self.deps.off_core;
                off.renewals_dispatched
                    .lock()
                    .unwrap()
                    .retain(|k, _| keep(k));
                off.granted_ttl.lock().unwrap().retain(|k, _| keep(k));
            }
        }
        // Plan 30 §M12: what the FUSE fast path executed since the last
        // event, for the placement.
        let notes = self.deps.delegates.take_fast_path_notes();
        if !notes.is_empty() {
            self.core.place_note_local(&notes, &*self.deps.meta);
        }
        // M16: the root lease's placement counts them too (it hears of
        // core-executed ops through `Action::Reply` only).
        let fast_ops = self.deps.delegates.take_fast_path_ops();
        self.deps.placement.note_own(self.node_id, fast_ops);
        let cfg = self.core.config();
        let lease = self.core.lease();
        self.deps
            .view
            .mirror(lease, now, cfg, self.core.ack_gated());
        self.deps
            .view
            .set_liveness(self.core.fast_tenure(), self.core.last_s3_fresh());
        self.deps.view.set_release_hold(
            self.core.takeover_floor_until(),
            self.core.fast_gate_pending(),
        );
        let mut status = self.deps.status.lock().unwrap();
        status.stats = self.core.stats;
        status.ship = Some(self.core.ship().clone());
        status.lease = self.deps.view.status();
        status.gate_pending = lease.gate.is_some();
        status.job = self.core.job();
        let next_seq = self.core.ship().next_seq;
        match (status.job, &mut self.job_since) {
            (None, since) => *since = None,
            (Some(job), Some((kind, seq, at, reported))) if *kind == job && *seq == next_seq => {
                if !*reported && at.elapsed() >= STUCK_JOB {
                    *reported = true;
                    tracing::warn!(
                        node = self.node_id,
                        secs = at.elapsed().as_secs(),
                        next_seq,
                        job = self.core.job_detail().unwrap_or_default(),
                        "a core job has been in progress for a long time, the log cursor not moving"
                    );
                }
            }
            (Some(job), since) => {
                *since = Some((job, next_seq, std::time::Instant::now(), false));
            }
        }
        status.clients_in_flight = self.core.clients().count();
        status.inbox = self.core.inbox_view(now);
        status.copies = self.core.replay_copies(now);
        status.rounds_completed = self.core.stats.rounds_completed;
        status.last_error = self.core.ship().last_error.clone();
        status.epoch = self.core.epoch_state();
        status.stream = self.core.stream_view();
        status.stream_enabled = cfg.log_streams && cfg.p2p;
        status.read = self.core.read_view();
        status.read_delegations = cfg.read_delegations;
        status.ack = self.core.ack_view();
        status.ack_s3 = cfg.ack_s3;
        status.delegation = self.core.deleg_view();
        status.delegation_enabled = cfg.delegation;
        status.delegation_fast_path_executed = self.deps.delegates.executed();
        status.delegation_fast_path_routed = self.deps.delegates.routed();
        status.delegation_fast_path_fenced = self.deps.delegates.fenced();
        self.deps.delegates.mirror(&status.delegation);
        self.deps
            .delegates
            .set_gated(self.core.deleg_fast_path_gated());
        status.placement_top = self.core.placement_top();
        status.locks_cluster = cfg.locks;
        let lv = self.core.lock_view();
        status.lock_requests_in_flight = lv.requests_in_flight;
        status.lock_waiters = lv.waiters;
        status.lock_recalls_in_flight = lv.recalls_in_flight;
        status.own_s3 = self.core.own_s3();
        status.connected_links = self.core.connected_links();
        status.authority = self.core.authority_mode();
        drop(status);
        self.deps
            .epochs
            .set_claim_view(self.core.epoch_claim_view(now));
    }

    /// What [`holder_alive_loop`] beats to: the lease this node holds,
    /// its committed backups and candidate, and the candidacies it
    /// dropped lately ([`AliveBook`]).
    fn publish_alive_targets(&mut self, now: WaitMs) {
        let lease = self.core.lease();
        let held = match &lease.held {
            Some((l, _)) if !lease.epoch_held() => Some((
                l.epoch,
                l.expires_unix_ms,
                lease.honour_until(self.core.config()).map_or(0, |h| h.0),
                lease.wait_until(self.core.config()).map_or(0, |w| w.0),
                self.core.beat_guard_wait_ms(&*self.deps.meta),
            )),
            _ => None,
        };
        let listed = if held.is_some() {
            self.core.liveness_targets()
        } else {
            Vec::new()
        };
        let targets = self.alive_book.update(now.0, held, listed);
        let mut alive = self.alive.lock().unwrap();
        if *alive != targets {
            *alive = targets;
        }
    }

    /// Tell the core the epoch machine's state when it changed (or
    /// `force`d, at start).
    fn report_epoch(&mut self, force: bool) {
        let e = &self.deps.epochs;
        let base = self.deps.meta.applied_seq().unwrap_or(0);
        let state = EpochState {
            open: e.is_open(),
            active: e.writes_ok(),
            frozen: e.is_frozen(),
            flushing: e.is_flushing(),
            base: self
                .last_epoch_reported
                .filter(|s| s.open)
                .map(|s| s.base)
                .unwrap_or(base),
        };
        if !force && self.last_epoch_reported == Some(state) {
            return;
        }
        self.last_epoch_reported = Some(state);
        let members = e.status().members;
        let (carrier, stale_below) = e.carrier();
        let _ = self.int_tx.send(Internal::Control {
            req: Control::Epoch {
                open: state.open,
                active: state.active,
                frozen: state.frozen,
                flushing: state.flushing,
                base: state.base,
                members,
                carrier: carrier.map(|c| constellation_authority::Carrier {
                    node: c.node,
                    epoch: c.epoch,
                    expires_unix_ms: c.expires_unix_ms,
                }),
                stale_below,
            },
            reply: ControlReply::None,
        });
    }

    fn drain_acks(&mut self) -> Vec<u64> {
        std::mem::take(&mut *self.deps.pending_acks.lock().unwrap())
    }

    // ---- requests → events ----

    fn on_request(&mut self, req: SyncRequest) -> Option<Internal> {
        let control = |req: Control, reply: ControlReply| Some(Internal::Control { req, reply });
        match req {
            SyncRequest::Nudge => control(Control::Nudge, ControlReply::None),
            SyncRequest::Journaled => control(Control::Journaled, ControlReply::None),
            SyncRequest::Roster(roster) => Some(Internal::Event(Event::Roster {
                write_eligible: roster,
            })),
            SyncRequest::EpochChanged => {
                self.report_epoch(false);
                None
            }
            SyncRequest::Publish {
                through: None,
                reply,
            } => control(Control::PublishNow, ControlReply::Publish(reply)),
            SyncRequest::Publish {
                through: Some(applied),
                reply,
            } => {
                // A snapshot whose barrier already shipped what it needs:
                // no round, no wait for an empty journal.
                let Some(publisher) = self.deps.publisher.clone() else {
                    let _ = reply.send(Err("this mount has no metadata tree publisher".into()));
                    return None;
                };
                let epoch = self.core.ship().last_ship_epoch;
                tokio::spawn(async move {
                    let r = publisher.lock().await.publish_through(epoch, applied).await;
                    let _ = reply.send(r.map_err(|e| format!("{e:#}")));
                });
                None
            }
            SyncRequest::Barrier { ino, reply } => {
                // Upload the inode's chunks first; the round ships the
                // journal.
                let tx = self.int_tx.clone();
                let upload = self.uploader();
                tokio::spawn(async move {
                    match upload.run(Some(ino)).await {
                        Ok(_) => {
                            // The round's outcome crosses the core as
                            // text: classified from it (plan 39).
                            let (round, outcome) = tokio::sync::oneshot::channel();
                            let _ = tx.send(Internal::Control {
                                req: Control::Barrier { ino: Some(ino) },
                                reply: ControlReply::Done(round),
                            });
                            let result = match outcome.await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(error)) => Err(crate::sync::SyncFailure::from_text(error)),
                                Err(_) => Err(crate::sync::SyncFailure {
                                    class: crate::sync::ErrorClass::Permanent,
                                    message: "the sync task stopped".into(),
                                }),
                            };
                            let _ = reply.send(result);
                        }
                        Err(e) => {
                            let _ = reply.send(Err(crate::sync::SyncFailure::from_error(&e)));
                        }
                    }
                });
                None
            }
            SyncRequest::ChunksDurable {
                from,
                hashes,
                reply,
            } => {
                self.deps.upload.note_reported(&hashes);
                match self.deps.meta.ack_remote_chunks(&hashes) {
                    Ok(acked) => {
                        for (hash, _) in &acked {
                            // Content this node also held dirty (it wrote the
                            // same bytes) is durable now: evictable again.
                            if !self.deps.meta.upload_pending_for_hash(hash).unwrap_or(true)
                                && self.deps.cache.state_of(hash)
                                    == Some(constellation_fs_core::cache::ChunkState::Dirty)
                            {
                                self.deps.cache.set_state(
                                    hash,
                                    constellation_fs_core::cache::ChunkState::Clean,
                                );
                            }
                            self.deps.upload.forget_remote_poll(hash);
                        }
                        tracing::debug!(
                            from,
                            reported = hashes.len(),
                            acked = acked.len(),
                            "forwarded chunks reported durable"
                        );
                        let _ = reply.send(());
                        if acked.is_empty() {
                            None
                        } else {
                            // Ship what they held back.
                            control(Control::Nudge, ControlReply::None)
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, from, "could not ack reported chunks");
                        let _ = reply.send(());
                        None
                    }
                }
            }
            SyncRequest::DrainInode { ino, fsync, reply } => {
                let upload = self.uploader();
                let handoff = (ino != 0).then(|| self.chunk_handoff());
                tokio::spawn(async move {
                    let r = drain_with_handoff(upload, handoff, ino, fsync && ino != 0).await;
                    let _ = reply.send(r.map_err(|e| crate::sync::SyncFailure::from_error(&e)));
                });
                None
            }
            SyncRequest::AcceptHandoff {
                requester,
                hashes,
                reply,
            } => {
                let handoff = self.chunk_handoff();
                tokio::spawn(async move {
                    let _ = reply.send(handoff.accept(requester, hashes).await);
                });
                None
            }
            SyncRequest::TailToHead { reply } => {
                control(Control::TailToHead, ControlReply::Done(reply))
            }
            SyncRequest::Acquire { reply } => {
                control(Control::Acquire, ControlReply::Acquire(reply))
            }
            SyncRequest::HandOff {
                requester,
                epoch_applied,
                reply,
            } => {
                if self.deps.placement.declines_claim(self.node_id, requester) {
                    // This node's own clients write more than the
                    // requester's right now: the offer it made is stale.
                    // Nothing is flushed or released, no S3 request.
                    tracing::info!(
                        requester,
                        "declining a claim of our lease offer: this node writes more now"
                    );
                    let _ = reply.send(None);
                    return None;
                }
                let req = self.control_id();
                self.handoff_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LeaseRequest { req, epoch_applied },
                }))
            }
            SyncRequest::Mutate {
                requester,
                op,
                rid,
                acked_through,
                deps,
                pending,
                applied,
                tag,
                reply,
            } => {
                let deps = Position::from_postcard(&deps);
                let op = match MutateOp::from_postcard(&op) {
                    Ok(op) => op,
                    Err(_) => {
                        let _ = reply.send((
                            MutateOutcome::Errno(Code::Invalid),
                            None,
                            Position::ZERO,
                            0,
                            (OwnChunks::None, None),
                        ));
                        return None;
                    }
                };
                // A `back` close forwarded before its chunks were in S3:
                // await them here — enrolled before the op can execute,
                // so no row of this node naming them is ever releasable
                // without them (`meta::store::remote`). An op that ends
                // up refused or executed elsewhere leaves rows the
                // requester's report (or this node's S3 check) acks.
                // A report that overtook this forward already covers
                // what it names.
                let pending = self.deps.upload.not_reported(pending);
                if !pending.is_empty() {
                    if let MutateOp::SetManifest { ino, .. } = &op {
                        if let Err(error) = self
                            .deps
                            .meta
                            .enroll_remote_chunks(*ino, &pending, requester)
                        {
                            tracing::warn!(%error, ino, "could not await a forwarded manifest's chunks");
                            let _ = reply.send((
                                MutateOutcome::Errno(Code::Io),
                                None,
                                Position::ZERO,
                                0,
                                (OwnChunks::None, None),
                            ));
                            return None;
                        }
                    }
                }
                let req = self.control_id();
                tracing::trace!(
                    target: "constellation::fwd",
                    rid = rid.seq,
                    rnode = rid.node,
                    req = req.0,
                    "mutate request received"
                );
                self.mutate_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::MutateRequest {
                        req,
                        rid,
                        op,
                        acked_through,
                        deps,
                        applied,
                        tag,
                    },
                }))
            }
            // One way: `req` is the sender's id, echoed in the answer.
            SyncRequest::PeerDelegateStream {
                from,
                req_id,
                gen,
                round,
                txs,
                leaving,
                leaving_barriers,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::DelegateStream {
                    req: OpId(req_id),
                    gen,
                    round,
                    txs,
                    leaving,
                    leaving_barriers,
                },
            })),
            SyncRequest::PeerDelegateStreamAck {
                from,
                req_id,
                gen,
                round,
                through,
                refused,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::DelegateStreamAck {
                    req: OpId(req_id),
                    gen,
                    round,
                    through,
                    refused,
                },
            })),
            SyncRequest::DelegRenewSentOffCore {
                gen,
                round,
                to,
                req_id,
                sent,
            } => Some(Internal::Event(Event::DelegRenewSentOffCore {
                gen,
                round,
                to,
                req: OpId(req_id),
                sent: HonourMs(sent),
            })),
            SyncRequest::PeerDelegRenew {
                from,
                req_id,
                gen,
                round,
                backup,
                stream_head,
                stream_head_at,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::DelegRenew {
                    req: OpId(req_id),
                    gen,
                    round,
                    backup: (backup != 0).then_some(backup),
                    stream_head,
                    stream_head_at,
                },
            })),
            SyncRequest::PeerDelegRenewOffCore {
                from,
                req_id,
                gen,
                round,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::DelegRenewOffCore {
                    req: OpId(req_id),
                    gen,
                    round,
                },
            })),
            SyncRequest::DelegRenewOffCoreFailed { to, req_id } => {
                Some(Internal::Event(Event::PeerFailed {
                    req: OpId(req_id),
                    to,
                    outage: false,
                }))
            }
            SyncRequest::PeerDelegRenewed {
                from,
                req_id,
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
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::DelegRenewed {
                    req: OpId(req_id),
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
                },
            })),
            SyncRequest::PeerDelegRecall {
                root,
                dir,
                gen,
                reply,
            } => {
                let req = self.control_id();
                self.deleg_recall_replies.insert(req, reply);
                // The fast path admits nothing more under `gen` from
                // here; the core answers the recall (with the stream
                // index it reads then) once every admitted op is
                // journaled, so nothing lands past the `through` the
                // root drains to (harness `cross-subtree-rename`: an op
                // admitted in that window was refused by the root and
                // rolled back here).
                let delegates = self.deps.delegates.clone();
                delegates.stop(gen);
                let tx = self.int_tx.clone();
                tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    while delegates.in_flight() > 0 && started.elapsed() < Duration::from_secs(5) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let _ = tx.send(Internal::Event(Event::Peer {
                        from: root,
                        msg: PeerMsg::DelegRecall { req, dir, gen },
                    }));
                });
                None
            }
            SyncRequest::PeerDelegBackupAppend {
                from,
                gen,
                txs,
                reply,
            } => {
                let req = self.control_id();
                self.deleg_backup_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from,
                    msg: PeerMsg::DelegBackupAppend { req, gen, txs },
                }))
            }
            SyncRequest::PeerDelegSeal { root, gen, reply } => {
                let req = self.control_id();
                self.deleg_seal_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: root,
                    msg: PeerMsg::DelegSeal { req, gen },
                }))
            }
            SyncRequest::SyncDesignations { entries } => {
                control(Control::SyncDesignations { entries }, ControlReply::None)
            }
            SyncRequest::Delegate {
                dir,
                node,
                range,
                reply,
            } => control(
                Control::Delegate { dir, node, range },
                ControlReply::Text(reply),
            ),
            SyncRequest::Undelegate { dir, reply } => {
                control(Control::Undelegate { dir }, ControlReply::Text(reply))
            }
            SyncRequest::Submit {
                op,
                rid,
                policy,
                in_doubt,
                tag,
                reply,
            } => {
                self.replies.insert(rid, reply);
                let acked = self.drain_acks();
                if !acked.is_empty() {
                    let _ = self.int_tx.send(Internal::Event(Event::Activity {
                        last_write: WaitMs(self.deps.view.last_write_ms()),
                        acked_seqs: acked,
                    }));
                }
                // One event carries the in-doubt mark with the op, so the
                // core can never see the op without its mark.
                Some(Internal::Event(if in_doubt {
                    Event::SubmitInDoubt {
                        rid,
                        op,
                        policy,
                        tag,
                    }
                } else {
                    Event::Submit {
                        rid,
                        op,
                        policy,
                        tag,
                    }
                }))
            }
            SyncRequest::ReadIndex {
                ino,
                dir,
                name,
                reply,
            } => control(
                Control::ReadIndex { ino, dir, name },
                ControlReply::ReadIndex(reply),
            ),
            SyncRequest::Recall { inos, reply } => {
                control(Control::Recall { inos }, ControlReply::Recall(reply))
            }
            SyncRequest::PeerReadIndex {
                requester,
                ino,
                dir,
                name,
                reply,
            } => {
                let req = self.control_id();
                self.read_index_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::ReadIndex {
                        req,
                        ino,
                        dir,
                        name,
                    },
                }))
            }
            SyncRequest::PeerRecall {
                holder,
                ino,
                grant,
                reply,
            } => {
                let req = self.control_id();
                self.recall_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: holder,
                    msg: PeerMsg::DelegationRecall { req, ino, grant },
                }))
            }
            SyncRequest::PeerBackupAppend {
                holder,
                epoch,
                config_version,
                candidacy,
                from,
                txs,
                through,
                expires_unix_ms,
                reply,
            } => {
                let req = self.control_id();
                tracing::trace!(target: "constellation::fwd", from, req = req.0, "append received");
                self.backup_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: holder,
                    msg: PeerMsg::BackupAppend {
                        req,
                        epoch,
                        holder,
                        config_version,
                        candidacy,
                        from,
                        txs,
                        through,
                        expires_unix_ms,
                    },
                }))
            }
            SyncRequest::PeerBackupHold {
                holder,
                epoch,
                for_ms,
            } => Some(Internal::Event(Event::Peer {
                from: holder,
                msg: PeerMsg::BackupHold { epoch, for_ms },
            })),
            SyncRequest::HoldBackups { for_ms, reply } => {
                // Plan 37 §8: only the committed backups can seal; a
                // candidate being brought up is not in the lease object.
                let epoch = self
                    .core
                    .lease()
                    .held
                    .as_ref()
                    .map(|(lease, _)| lease.epoch);
                let backups = self.core.ack_view().backups;
                let (Some(epoch), false) = (epoch, backups.is_empty()) else {
                    let _ = reply.send(Vec::new());
                    return None;
                };
                let peers = self.deps.peers.clone();
                let holder = self.node_id;
                let timeout = Duration::from_millis(self.core.config().backup_ack_timeout_ms);
                tokio::spawn(async move {
                    let payload = Payload::BackupHold {
                        holder,
                        epoch,
                        for_ms,
                    };
                    let asks = backups.into_iter().map(|backup| {
                        let (peers, payload) = (peers.clone(), payload.clone());
                        async move {
                            let answer = tokio::time::timeout(
                                timeout,
                                peers.request_to_node_timeout(backup, &payload, timeout),
                            )
                            .await;
                            matches!(answer, Ok(Ok(_))).then_some(backup)
                        }
                    });
                    let held = futures::future::join_all(asks)
                        .await
                        .into_iter()
                        .flatten()
                        .collect();
                    let _ = reply.send(held);
                });
                None
            }
            SyncRequest::PeerStreamAhead {
                from,
                epoch,
                base,
                txs,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::StreamAhead { epoch, base, txs },
            })),
            SyncRequest::PeerHolderAlive {
                holder,
                epoch,
                candidacy,
                listed,
                at_unix_ms,
                beat,
            } => Some(Internal::Event(Event::HolderAlive {
                from: holder,
                epoch,
                candidacy,
                listed,
                at: WaitMs(at_unix_ms),
                seq: beat.seq,
                clock: beat.clock,
                sent: beat.sent_ms,
                holder_expires: beat.expires_ms,
                guard: beat.guard_ms,
                expires: Ms(beat.expires_unix_ms),
            })),
            SyncRequest::PeerPromiseRequest {
                requester,
                expires_unix_ms,
                reply,
            } => {
                let req = self.control_id();
                self.promise_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::PromiseRequest {
                        req,
                        expires_unix_ms,
                    },
                }))
            }
            SyncRequest::Slack(epoch_slack) => {
                self.deps.epochs.set_slack(epoch_slack);
                Some(Internal::Event(Event::Slack { epoch_slack }))
            }
            SyncRequest::Retired => control(Control::Retire, ControlReply::None),
            SyncRequest::SegmentHint { seq, epoch } => Some(Internal::Event(Event::Peer {
                from: 0,
                msg: PeerMsg::SegmentPublished { seq, epoch },
            })),
            SyncRequest::LogSubscribe {
                requester,
                req,
                from,
                sink,
                queued_bytes,
            } => {
                let req = OpId(req);
                // A newer subscription from the same node replaces the
                // old one; dropping the old sink ends its stream.
                self.subscribers.insert(
                    requester,
                    SubscriberSink {
                        req,
                        tx: sink,
                        queued_bytes,
                    },
                );
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LogSubscribe { req, from },
                }))
            }
            SyncRequest::ClaimOffer { epoch } => {
                control(Control::ClaimOffer { epoch }, ControlReply::ClaimOffer)
            }
            SyncRequest::Reintegrate(reply) => {
                if self.deps.epochs.is_open() {
                    let _ = reply.send(Err(
                        "cannot reintegrate while a continuation epoch is open".into(),
                    ));
                    return None;
                }
                self.deps
                    .reintegration
                    .in_progress
                    .store(true, Ordering::Relaxed);
                control(Control::Reintegrate, ControlReply::Text(reply))
            }
            SyncRequest::Leave { force, reply } => {
                if let Err(e) = crate::leave::refuse_open_epoch(&self.deps.epochs) {
                    let _ = reply.send(Err(e.to_string()));
                    return None;
                }
                if let Err(e) = crate::leave::pre_leave_checks(
                    &self.deps.meta,
                    &self.deps.designations,
                    self.node_id,
                    force,
                ) {
                    let _ = reply.send(Err(e.to_string()));
                    return None;
                }
                // Upload dirty chunks before the journal flush so the
                // leave does not strand content that only exists here.
                let tx = self.int_tx.clone();
                let upload = self.uploader();
                tokio::spawn(async move {
                    match upload.run(None).await {
                        Ok(_) => {
                            let _ = tx.send(Internal::Control {
                                req: Control::Flush,
                                reply: ControlReply::Leave(reply),
                            });
                        }
                        Err(e) => {
                            let _ = reply.send(Err(format!(
                                "cannot upload dirty chunks before leave: {e:#}"
                            )));
                        }
                    }
                });
                None
            }
            SyncRequest::Lock {
                ino,
                mode,
                blocking,
                waited_ms,
                reply,
            } => control(
                Control::Lock {
                    ino,
                    mode,
                    blocking,
                    waited_ms,
                },
                ControlReply::Lock(reply),
            ),
            SyncRequest::LockIdle { ino } => control(Control::LockIdle { ino }, ControlReply::None),
            SyncRequest::LockReleaseWake { ino } => {
                control(Control::LockReleaseWake { ino }, ControlReply::None)
            }
            SyncRequest::LockTest { ino, mode, reply } => control(
                Control::LockTest { ino, mode },
                ControlReply::LockTest(reply),
            ),
            SyncRequest::PeerLockRequest {
                requester,
                ino,
                mode,
                blocking,
                sent,
                incarnation,
                waited_ms,
                reply,
            } => {
                let req = self.control_id();
                // A request the core never answers leaves its sender here
                // only until the P2P task waiting on it gives up
                // (`P2pBridge::lock_requested` bounds the wait).
                self.lock_request_replies.retain(|_, tx| !tx.is_closed());
                self.lock_request_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LockRequest {
                        req,
                        ino,
                        mode,
                        blocking,
                        sent: HonourMs(sent),
                        incarnation,
                        waited_ms,
                    },
                }))
            }
            SyncRequest::PeerLockRecall {
                owner,
                ino,
                grant,
                reply,
            } => {
                let req = self.control_id();
                self.lock_recall_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: owner,
                    msg: PeerMsg::LockRecall { req, ino, grant },
                }))
            }
            SyncRequest::PeerLockRenew {
                from,
                entries,
                reply,
            } => {
                let req = self.control_id();
                self.lock_renew_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from,
                    msg: PeerMsg::LockRenew { req, entries },
                }))
            }
            SyncRequest::LockRenewAnswered {
                to,
                req_id,
                results,
                outage,
            } => Some(Internal::Event(lock_renew_event(
                to,
                OpId(req_id),
                results,
                outage,
            ))),
            SyncRequest::PeerLockTest {
                requester,
                ino,
                mode,
                reply,
            } => {
                let req = self.control_id();
                self.lock_test_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LockTest { req, ino, mode },
                }))
            }
            SyncRequest::PeerLockGranted {
                from,
                ino,
                sent,
                outcome,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::LockGranted {
                    ino,
                    sent: HonourMs(sent),
                    outcome,
                },
            })),
            SyncRequest::PeerLockReleased {
                from,
                ino,
                grant,
                position,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::LockReleased {
                    ino,
                    grant,
                    position,
                },
            })),
            SyncRequest::PeerLockCancel {
                from,
                ino,
                exclusive,
                shared,
                upto,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::LockCancel {
                    ino,
                    exclusive,
                    shared,
                    upto: HonourMs(upto),
                },
            })),
            SyncRequest::PeerLockMirror {
                from,
                ver,
                grants,
                floor,
            } => Some(Internal::Event(Event::Peer {
                from,
                msg: PeerMsg::LockMirror { ver, grants, floor },
            })),
            SyncRequest::Shutdown { reply } => {
                control(Control::Shutdown, ControlReply::Done(reply))
            }
            SyncRequest::Authority {
                forward_only,
                suspended,
                reply,
            } => control(
                Control::Authority {
                    forward_only,
                    suspended,
                },
                ControlReply::Done(reply),
            ),
            SyncRequest::Flush { reply } => control(Control::Flush, ControlReply::Done(reply)),
        }
    }

    fn chunk_handoff(&self) -> ChunkHandoff {
        ChunkHandoff {
            node_id: self.node_id,
            meta: self.deps.meta.clone(),
            cache: self.deps.cache.clone(),
            store: self.deps.chunk_store.clone(),
            compression: self.deps.compression,
            upload: self.deps.upload.clone(),
            peers: self.deps.peers.clone(),
            view: self.deps.view.clone(),
            sync_tx: self.sync_tx.clone(),
        }
    }

    fn uploader(&self) -> Uploader {
        Uploader {
            cache: self.deps.cache.clone(),
            meta: self.deps.meta.clone(),
            store: self.deps.chunk_store.clone(),
            compression: self.deps.compression,
            upload: self.deps.upload.clone(),
            peers: Some(self.deps.peers.clone()),
            node_id: self.node_id,
        }
    }

    // ---- actions → IO ----

    fn dispatch(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Reply { rid, reply } => {
                    if let Some(tx) = self.replies.remove(&rid) {
                        if let ClientReply::Outcome(MutateOutcome::Accepted { .. }) = &reply {
                            self.deps.placement.note_local(self.node_id);
                        }
                        let _ = tx.send(reply);
                    }
                }
                Action::Send { to, msg } => self.send(to, msg),
                Action::S3 { op, req } => self.spawn_s3(op, req),
                Action::SetTimer { id, after_ms, kind } => {
                    let tx = if prompt_timer(kind) {
                        self.prompt_tx.clone()
                    } else {
                        self.int_tx.clone()
                    };
                    // From the step's reading (`now_at`), not from here.
                    let delay =
                        Duration::from_millis(after_ms).saturating_sub(self.now_at.elapsed());
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(Internal::Timer {
                            id,
                            kind,
                            deferred: false,
                        });
                    });
                }
                Action::CancelTimer { .. } => {}
                Action::UploadAwaited { inos } => {
                    // The inodes' passes, not the round's background one:
                    // the upload hold does not stop them (an `fsync`'s
                    // drain is the same pass). Their reports tell the
                    // holder, which then ships the waiting transaction. A
                    // repeat (the core's safety timer) after the chunks
                    // went up finds no row and costs one index lookup.
                    let inos: Vec<_> = inos
                        .into_iter()
                        .filter(|ino| {
                            !self
                                .deps
                                .meta
                                .pending_uploads_for_ino(*ino)
                                .is_ok_and(|rows| rows.is_empty())
                        })
                        .collect();
                    if inos.is_empty() {
                        continue;
                    }
                    let upload = self.uploader();
                    let held = self.deps.upload.hold.is_held();
                    tokio::spawn(async move {
                        for ino in inos {
                            if let Err(error) = upload.run(Some(ino)).await {
                                tracing::debug!(
                                    ino,
                                    error = %format!("{error:#}"),
                                    "upload for a forwarded op waiting on its own transaction failed"
                                );
                            } else {
                                tracing::debug!(
                                    ino,
                                    held,
                                    "uploaded the chunks a forwarded op waited on"
                                );
                            }
                        }
                    });
                }
                Action::UploadDirtyChunks {
                    op,
                    ino,
                    round,
                    complete,
                } => {
                    let tx = self.int_tx.clone();
                    let upload = self.uploader();
                    let epochs = self.deps.epochs.clone();
                    let acked = self.drain_acks();
                    let last_write = WaitMs(self.deps.view.last_write_ms());
                    if round {
                        let _ = tx.send(Internal::Event(Event::Activity {
                            last_write,
                            acked_seqs: acked,
                        }));
                    }
                    let sync_tx = self.sync_tx.clone();
                    let bulk = self.bulk_pass.clone();
                    let wait = self.round_upload_wait;
                    tokio::spawn(async move {
                        if round && crate::fault::sync_held() {
                            let _ = tx.send(Internal::Event(Event::UploadsDone {
                                op,
                                result: UploadResult::Skip,
                            }));
                            return;
                        }
                        if round && epochs.is_open() {
                            epochs.check_liveness().await;
                            let _ = sync_tx.send(SyncRequest::EpochChanged);
                            // Plan 30 §M10: frozen or not, the core runs
                            // an epoch round's upload only once its probe
                            // found S3 — the hold owner's to flush, a
                            // member's to upload its own chunks and to
                            // close once the carried lease has moved
                            // (`epoch_carrier_checked`). A frozen member's
                            // pass used to be skipped here, so it closed
                            // only once the missing member returned.
                        }
                        let result = if ino.is_none() {
                            bulk.run(upload, tx.clone(), complete, wait).await
                        } else {
                            upload.run(ino).await.map_err(|e| format!("{e:#}"))
                        };
                        let result = match result {
                            Ok(()) => UploadResult::Done { held: 0 },
                            Err(e) => UploadResult::Failed(e),
                        };
                        let _ = tx.send(Internal::Event(Event::UploadsDone { op, result }));
                    });
                }
                Action::Publish { op, epoch } => {
                    let tx = self.int_tx.clone();
                    let Some(publisher) = self.deps.publisher.clone() else {
                        let _ = tx.send(Internal::Event(Event::PublishDone { op, ok: false }));
                        continue;
                    };
                    tokio::spawn(async move {
                        let mut publisher = publisher.lock().await;
                        let ok = match publisher.publish(epoch).await {
                            Ok(_) => true,
                            Err(e) => {
                                tracing::warn!(error = %e, "metadata tree publish failed; retrying next round");
                                false
                            }
                        };
                        let _ = tx.send(Internal::Event(Event::PublishDone { op, ok }));
                    });
                }
                Action::FollowHead { op } => {
                    let tx = self.int_tx.clone();
                    let Some(publisher) = self.deps.publisher.clone() else {
                        let _ = tx.send(Internal::Event(Event::PublishDone { op, ok: false }));
                        continue;
                    };
                    tokio::spawn(async move {
                        let ok = match publisher.try_lock() {
                            Ok(mut p) => match p.follow_head().await {
                                Ok(_) => true,
                                Err(e) => {
                                    tracing::debug!(error = %e, "follower head check failed; retrying later");
                                    false
                                }
                            },
                            Err(_) => false,
                        };
                        let _ = tx.send(Internal::Event(Event::PublishDone { op, ok }));
                    });
                }
                Action::Announce {
                    seq,
                    epoch,
                    payload,
                } => {
                    let peers = self.deps.peers.clone();
                    let _ = &payload;
                    tokio::spawn(async move {
                        // Plan 30 §M7: a hint only. The segment reaches the
                        // holder's subscribers on their log streams (over
                        // QUIC between enrolled members, which hold the
                        // E2E key anyway), so gossip — relayed by third
                        // parties — never carries log content, sealed or
                        // not.
                        peers.announce_segment(PARTITION, seq, epoch).await;
                    });
                }
                Action::ConflictCopy {
                    queue_seq,
                    rid,
                    op,
                    reason,
                    locked,
                } => {
                    let roots = if locked {
                        (self.deps.view_roots)()
                    } else {
                        Vec::new()
                    };
                    let tx = self.int_tx.clone();
                    let meta = self.deps.meta.clone();
                    let sync_tx = self.sync_tx.clone();
                    let forward = self.deps.forward.clone();
                    let node_id = self.node_id;
                    tokio::spawn(async move {
                        let refusal = constellation_meta::Refusal {
                            reason,
                            ts_unix: constellation_fs_core::types::now_ns() / 1_000_000_000,
                        };
                        let ok = match crate::recovery::materialize_remote(
                            &meta, &sync_tx, &forward, node_id, rid, &op, &refusal, &roots,
                        )
                        .await
                        {
                            Ok(made) => made,
                            Err(error) => {
                                tracing::debug!(%error, ?rid, "conflict copy attempt failed");
                                false
                            }
                        };
                        let _ = tx.send(Internal::Event(Event::ConflictCopyDone { queue_seq, ok }));
                    });
                }
                Action::RebuildReplica { op } => {
                    let tx = self.int_tx.clone();
                    let meta = self.deps.meta.clone();
                    let log = self.deps.log.clone();
                    let state_dir = self.deps.state_dir.clone();
                    let holds = self.deps.holds.clone();
                    tokio::spawn(async move {
                        let keep = || {
                            holds
                                .as_ref()
                                .and_then(|h| h.open_orphans().ok())
                                .unwrap_or_default()
                        };
                        let ok = match rebuild_replica(&meta, &log, &state_dir, &keep).await {
                            Ok(()) => true,
                            Err(error) => {
                                tracing::warn!(error = %format!("{error:#}"), "rebuilding the replica failed");
                                false
                            }
                        };
                        // The kept orphans' claim is re-stamped from the
                        // rebuilt replica at once.
                        if let Some(h) = &holds {
                            h.nudge();
                        }
                        let _ = tx.send(Internal::Event(Event::RebuildDone { op, ok }));
                    });
                }
                Action::LockFlush { ino, grant } => {
                    // The flush goes through the FUSE views' write state
                    // and waits on this driver (manifest commit, upload,
                    // barrier): never on the driver loop.
                    let tx = self.int_tx.clone();
                    let flush = self.deps.lock_flush.clone();
                    tokio::task::spawn_blocking(move || {
                        let ok = flush(ino, grant);
                        let _ = tx.send(Internal::Event(Event::LockFlushed { ino, grant, ok }));
                    });
                }
                Action::PersistLockHorizon { until } => {
                    // A durable write (seconds on a loaded disk): on the
                    // blocking pool, never the driver loop. The core holds
                    // the grant answers that need it until the event.
                    let tx = self.int_tx.clone();
                    let meta = self.deps.meta.clone();
                    tokio::task::spawn_blocking(move || {
                        let durable = match meta.note_lock_grant_horizon(until) {
                            Ok(d) => Some(d),
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "persisting the lock-grant horizon failed; refusing the answers waiting for it"
                                );
                                None
                            }
                        };
                        let _ = tx.send(Internal::Event(Event::LockHorizonPersisted {
                            until,
                            durable,
                        }));
                    });
                }
                Action::RefreshRoster => {
                    let store = self.deps.store_inner.clone();
                    let sync_tx = self.sync_tx.clone();
                    tokio::spawn(async move {
                        if let Ok(roster) =
                            constellation_store_s3::write_eligible_roster(store).await
                        {
                            let _ = sync_tx.send(SyncRequest::Roster(roster));
                        }
                    });
                }
                Action::RoundDone { failed } => {
                    if self.deps.epochs.is_open() {
                        // A member that stopped answering freezes the
                        // epoch (EROFS) — checked every round, holder or
                        // not.
                        let epochs = self.deps.epochs.clone();
                        let sync_tx = self.sync_tx.clone();
                        tokio::spawn(async move {
                            epochs.check_liveness().await;
                            let _ = sync_tx.send(SyncRequest::EpochChanged);
                        });
                    }
                    if failed.is_none() {
                        self.deps.epochs.note_s3_success();
                        let pins = self.deps.pins.clone();
                        tokio::spawn(async move { pins.refresh_all().await });
                    } else {
                        // S3 is away: maybe open a continuation epoch.
                        let epochs = self.deps.epochs.clone();
                        let meta = self.deps.meta.clone();
                        let sync_tx = self.sync_tx.clone();
                        tokio::spawn(async move {
                            let base = std::collections::BTreeMap::from([(
                                PARTITION.to_string(),
                                meta.applied_seq().unwrap_or(0),
                            )]);
                            match epochs.maybe_propose(base).await {
                                Ok(true) => {
                                    let _ = sync_tx.send(SyncRequest::EpochChanged);
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    tracing::debug!(%error, "continuation epoch proposal failed")
                                }
                            }
                        });
                    }
                    self.report_epoch(false);
                }
                Action::EpochClose => {
                    self.deps.epochs.close();
                    self.report_epoch(false);
                }
                Action::EpochFlushed => {
                    self.deps.epochs.finish_flushing();
                    self.report_epoch(false);
                }
                Action::ControlDone { op, result } => self.control_done(op, result),
            }
        }
    }

    fn send(&mut self, to: NodeId, msg: PeerMsg) {
        match msg {
            PeerMsg::MutateReply {
                req,
                outcome,
                base,
                position,
                gen,
                own_chunks,
                own_rows,
            } => {
                tracing::trace!(target: "constellation::fwd", req = req.0, "mutate reply sent");
                if let Some(tx) = self.mutate_replies.remove(&req) {
                    if matches!(outcome, MutateOutcome::Accepted { .. }) {
                        self.deps.placement.note_forwarded(to);
                    }
                    let delay = self.deps.fault_reply_delay_ms;
                    if delay > 0 {
                        // Fault injection only: the op already executed;
                        // this delays only the reply the requester waits
                        // for.
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            let _ = tx.send((outcome, base, position, gen, (own_chunks, own_rows)));
                        });
                    } else {
                        let _ = tx.send((outcome, base, position, gen, (own_chunks, own_rows)));
                    }
                }
            }
            PeerMsg::ReadIndexReply { req, outcome } => {
                if let Some(tx) = self.read_index_replies.remove(&req) {
                    let _ = tx.send(outcome);
                }
            }
            PeerMsg::DelegateStreamAck {
                req,
                gen,
                round,
                through,
                refused,
            } => {
                let payload = Payload::DelegateStreamAck {
                    from: self.node_id,
                    req_id: req.0,
                    gen,
                    round,
                    through,
                    refused,
                };
                self.one_way(to, payload);
            }
            PeerMsg::DelegRenewed {
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
            } => {
                let payload = Payload::DelegRenewed {
                    from: self.node_id,
                    req_id: req.0,
                    gen,
                    round,
                    ttl_ms,
                    locks: crate::locks::grants_wire(&locks),
                    lock_grace_ms,
                    lock_floor: crate::locks::floor_wire(&lock_floor),
                    lock_cut_at,
                    lock_cut: crate::locks::floor_wire(&lock_cut),
                    lock_barrier,
                    lock_carry: crate::locks::carry_wire(&lock_carry),
                };
                self.one_way(to, payload);
            }
            PeerMsg::DelegRecalled {
                req,
                through,
                locks,
                ..
            } => {
                if let Some(tx) = self.deleg_recall_replies.remove(&req) {
                    let _ = tx.send((through, locks));
                }
            }
            PeerMsg::LockReply { req, outcome } => {
                if let Some(tx) = self.lock_request_replies.remove(&req) {
                    let _ = tx.send(outcome);
                }
            }
            PeerMsg::LockRecalled { req } => {
                if let Some(tx) = self.lock_recall_replies.remove(&req) {
                    let _ = tx.send(());
                }
            }
            PeerMsg::LockRenewed { req, results } => {
                if let Some(tx) = self.lock_renew_replies.remove(&req) {
                    let _ = tx.send(results);
                }
            }
            PeerMsg::LockTestReply { req, outcome } => {
                if let Some(tx) = self.lock_test_replies.remove(&req) {
                    let _ = tx.send(outcome);
                }
            }
            PeerMsg::LockRequest {
                req,
                ino,
                mode,
                blocking,
                sent,
                incarnation,
                waited_ms,
            } => {
                let payload = Payload::LockRequest {
                    requester: self.node_id,
                    req_id: req.0,
                    ino,
                    exclusive: crate::locks::exclusive(mode),
                    blocking,
                    sent: sent.0,
                    incarnation,
                    waited_ms,
                };
                self.lock_rpc(to, req, payload, |reply, req| match reply {
                    Payload::LockReply { req_id, outcome } if req_id == req.0 => {
                        Some(PeerMsg::LockReply {
                            req,
                            outcome: crate::locks::outcome_of(outcome),
                        })
                    }
                    _ => None,
                });
            }
            PeerMsg::LockRecall { req, ino, grant } => {
                let payload = Payload::LockRecall {
                    owner: self.node_id,
                    req_id: req.0,
                    ino,
                    grant: crate::locks::grant_wire(grant),
                };
                self.lock_rpc(to, req, payload, |reply, req| match reply {
                    Payload::LockRecalled { req_id } if req_id == req.0 => {
                        Some(PeerMsg::LockRecalled { req })
                    }
                    _ => None,
                });
            }
            PeerMsg::LockRenew { req, entries } => {
                let payload = Payload::LockRenew {
                    from: self.node_id,
                    req_id: req.0,
                    entries: crate::locks::renew_entries_wire(&entries),
                };
                self.lock_renew_rpc(to, req, payload);
            }
            PeerMsg::LockTest { req, ino, mode } => {
                let payload = Payload::LockTest {
                    requester: self.node_id,
                    req_id: req.0,
                    ino,
                    exclusive: crate::locks::exclusive(mode),
                };
                self.lock_rpc(to, req, payload, |reply, req| match reply {
                    Payload::LockTestReply { req_id, outcome } if req_id == req.0 => {
                        Some(PeerMsg::LockTestReply {
                            req,
                            outcome: crate::locks::test_outcome_of(outcome),
                        })
                    }
                    _ => None,
                });
            }
            PeerMsg::LockGranted { ino, sent, outcome } => {
                let payload = Payload::LockGranted {
                    from: self.node_id,
                    ino,
                    sent: sent.0,
                    outcome: crate::locks::outcome_wire(&outcome),
                };
                self.one_way(to, payload);
            }
            PeerMsg::LockCancel {
                ino,
                exclusive,
                shared,
                upto,
            } => {
                let payload = Payload::LockCancel {
                    from: self.node_id,
                    ino,
                    exclusive,
                    shared,
                    upto: upto.0,
                };
                self.one_way(to, payload);
            }
            PeerMsg::LockReleased {
                ino,
                grant,
                position,
            } => {
                let payload = Payload::LockReleased {
                    from: self.node_id,
                    ino,
                    grant: crate::locks::grant_wire(grant),
                    position: position.to_postcard(),
                };
                self.one_way(to, payload);
            }
            PeerMsg::LockMirror { ver, grants, floor } => {
                let payload = Payload::LockMirror {
                    from: self.node_id,
                    ver,
                    grants: crate::locks::grants_wire(&grants),
                    floor: crate::locks::floor_wire(&floor),
                };
                self.one_way(to, payload);
            }
            PeerMsg::DelegBackupAck {
                req, acked, sealed, ..
            } => {
                if let Some(tx) = self.deleg_backup_replies.remove(&req) {
                    let _ = tx.send((acked, sealed));
                }
            }
            PeerMsg::DelegSealed {
                req, sealed, txs, ..
            } => {
                if let Some(tx) = self.deleg_seal_replies.remove(&req) {
                    let _ = tx.send((sealed, txs));
                }
            }
            PeerMsg::DelegBackupAppend { req, gen, txs } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let from = self.node_id;
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms * 4);
                let bytes = postcard::to_allocvec(&txs).unwrap_or_default();
                if crate::fault::p2p_denied(to) {
                    let _ = tx.send(Internal::Event(Event::PeerFailed {
                        req,
                        to,
                        outage: true,
                    }));
                    return;
                }
                tokio::spawn(async move {
                    let payload = Payload::DelegBackupAppend {
                        from,
                        req_id: req.0,
                        gen,
                        txs: bytes,
                    };
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::DelegBackupAck {
                            req_id,
                            gen,
                            acked,
                            sealed,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegBackupAck {
                                    req,
                                    gen,
                                    acked,
                                    sealed,
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::DelegSeal { req, gen } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let root = self.node_id;
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms * 4);
                if crate::fault::p2p_denied(to) {
                    let _ = tx.send(Internal::Event(Event::PeerFailed {
                        req,
                        to,
                        outage: true,
                    }));
                    return;
                }
                tokio::spawn(async move {
                    let payload = Payload::DelegSeal {
                        root,
                        req_id: req.0,
                        gen,
                    };
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::DelegSealed {
                            req_id,
                            gen,
                            sealed,
                            txs,
                        })) if req_id == req.0 => {
                            let txs = postcard::from_bytes(&txs).unwrap_or_default();
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegSealed {
                                    req,
                                    gen,
                                    sealed,
                                    txs,
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            // One way: the root answers with its own `DelegateStreamAck` /
            // `DelegRenewed`, whenever its core gets to it; this request
            // only waits for the message to be queued there, and a send
            // that did not get there is `PeerFailed` (the core backs off).
            PeerMsg::DelegateStream {
                req,
                gen,
                round,
                txs,
                leaving,
                leaving_barriers,
            } => {
                let payload = Payload::DelegateStream {
                    from: self.node_id,
                    req_id: req.0,
                    gen,
                    round,
                    txs: postcard::to_allocvec(&txs).unwrap_or_default(),
                    leaving: postcard::to_allocvec(&leaving).unwrap_or_default(),
                    leaving_barriers: postcard::to_allocvec(&leaving_barriers).unwrap_or_default(),
                };
                self.one_way_reported(to, req, payload);
            }
            PeerMsg::DelegRenew {
                req,
                gen,
                round,
                backup,
                stream_head,
                stream_head_at,
            } => {
                // Dispatched: the lane's next renewal is a period from
                // here, not from when the core decided on this one (a
                // blocking action earlier in the step may have delayed it).
                self.deps.off_core.note_renewal_dispatched(
                    gen,
                    round,
                    constellation_platform::clock::protocol().honour().0,
                );
                let payload = Payload::DelegRenew {
                    from: self.node_id,
                    req_id: req.0,
                    gen,
                    round,
                    backup: backup.unwrap_or(0),
                    stream_head,
                    stream_head_at,
                };
                self.one_way_reported(to, req, payload);
            }
            PeerMsg::DelegRecall { req, dir, gen } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let root = self.node_id;
                // Unanswered: outwaited by the grant's expiry in the core.
                let timeout = Duration::from_millis(
                    self.core.config().delegation_ttl_ms + self.core.config().expiry_margin_ms,
                );
                if crate::fault::p2p_denied(to) {
                    // Dropped: the recall is outwaited like an unanswered one.
                    return;
                }
                tokio::spawn(async move {
                    let payload = Payload::DelegRecall {
                        root,
                        req_id: req.0,
                        dir,
                        gen,
                    };
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::DelegRecalled {
                            req_id,
                            gen,
                            through,
                            locks,
                            lock_floor,
                            lock_barrier,
                            lock_carry,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegRecalled {
                                    req,
                                    gen,
                                    through,
                                    locks: constellation_meta::locks::LockHandback {
                                        grants: crate::locks::grants_of(&locks),
                                        carry: crate::locks::carry_of(&lock_carry),
                                        floor: crate::locks::floor_of(&lock_floor),
                                        barrier: lock_barrier,
                                    },
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::DelegationRecalled { req } => {
                if let Some(tx) = self.recall_replies.remove(&req) {
                    let _ = tx.send(());
                }
            }
            PeerMsg::ReadIndex {
                req,
                ino,
                dir,
                name,
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let requester = self.node_id;
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
                let denied = crate::fault::p2p_denied(to);
                tokio::spawn(async move {
                    let payload = Payload::ReadIndex {
                        requester,
                        req_id: req.0,
                        ino,
                        dir,
                        name,
                    };
                    if denied {
                        // Fault injection: the link to this peer is cut —
                        // a connection failure, at once.
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        }));
                        return;
                    }
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::ReadIndexReply {
                            req_id,
                            status,
                            holder,
                            position_seq,
                            position_pending,
                            grant,
                            position_streams,
                        })) if req_id == req.0 => {
                            let outcome = match status {
                                0 => ReadIndexOutcome::Ok {
                                    position: Position {
                                        seq: position_seq,
                                        pending: position_pending
                                            .map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                                        streams: Default::default(),
                                    }
                                    .with_streams_wire(&position_streams),
                                    grant: grant.map(|(id, ttl_ms, epoch)| ReadGrantMsg {
                                        id,
                                        ttl_ms,
                                        epoch,
                                    }),
                                },
                                2 => ReadIndexOutcome::Busy,
                                _ => ReadIndexOutcome::NotHolder { holder },
                            };
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::ReadIndexReply { req, outcome },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::DelegationRecall { req, ino, grant } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let holder = self.node_id;
                // A recall that goes unanswered is outwaited by the
                // grant's expiry in the core; this only bounds the RPC.
                let timeout = Duration::from_millis(
                    self.core.config().read_delegation_ttl_ms + self.core.config().expiry_margin_ms,
                );
                let denied = crate::fault::p2p_denied(to);
                tokio::spawn(async move {
                    let payload = Payload::ReadRecall {
                        holder,
                        req_id: req.0,
                        ino,
                        grant,
                    };
                    if denied {
                        // Fault injection: the link to this peer is cut —
                        // a connection failure, at once.
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        }));
                        return;
                    }
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::ReadRecalled { req_id })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegationRecalled { req },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::LeaseHandoff {
                req,
                released,
                epoch,
                head_seq,
            } => {
                if let Some(tx) = self.handoff_replies.remove(&req) {
                    let _ = tx.send(released.then_some(HandoffResult {
                        epoch,
                        etag: None,
                        head_seq,
                    }));
                }
            }
            PeerMsg::MutateRequest {
                req,
                rid,
                op,
                acked_through,
                deps,
                applied,
                tag,
            } => {
                // A manifest naming chunks still uploading here (a `back`
                // close) says so; the recipient awaits them, and this
                // node reports them once they are up (`Uploader::run`).
                let pending =
                    crate::upload::forwarded_pending_chunks(&self.deps.meta, &self.deps.cache, &op);
                self.deps.upload.note_forwarded(&pending, to);
                // A pass that acked one of them between the two lines
                // above found no forward to report it to: owe it now.
                for hash in &pending {
                    if !self.deps.meta.upload_pending_for_hash(hash).unwrap_or(true) {
                        self.deps.upload.note_up(hash);
                    }
                }
                let pending: Vec<[u8; 32]> = pending.iter().map(|h| h.0).collect();
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let forward = self.deps.forward.clone();
                let deps_bytes = deps.to_postcard();
                let lock_tag = tag.to_wire();
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
                let requester = self.node_id;
                let denied = crate::fault::p2p_denied(to);
                let op_bytes = match op.to_postcard() {
                    Ok(b) => b,
                    Err(_) => {
                        let _ = tx.send(Internal::Event(Event::Peer {
                            from: to,
                            msg: PeerMsg::MutateReply {
                                req,
                                outcome: MutateOutcome::Errno(Code::Invalid),
                                base: None,
                                position: Position::ZERO,
                                gen: 0,
                                own_chunks: OwnChunks::None,
                                own_rows: None,
                            },
                        }));
                        return;
                    }
                };
                tokio::spawn(async move {
                    let payload = Payload::MutateRequest {
                        part: PARTITION.to_string(),
                        requester,
                        req_id: req.0,
                        epoch_seen: 0,
                        op: op_bytes,
                        rid: (rid.node, rid.incarnation, rid.seq),
                        acked_through,
                        deps: deps_bytes,
                        pending,
                        applied,
                        lock_tag,
                    };
                    let started = std::time::Instant::now();
                    if denied {
                        // Fault injection: the link to this peer is cut —
                        // a connection failure, at once.
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        }));
                        return;
                    }
                    let reply =
                        tokio::time::timeout(timeout, peers.request_to_node(to, &payload)).await;
                    tracing::trace!(
                        target: "constellation::fwd_rtt",
                        to,
                        us = started.elapsed().as_micros() as u64,
                        "forward round trip"
                    );
                    match reply {
                        Ok(Ok(Payload::MutateReply {
                            req_id,
                            outcome,
                            base,
                            position_seq,
                            position_pending,
                            position_streams,
                            gen,
                            own_chunks,
                            own_inos,
                            own_rows,
                        })) if req_id == req.0 => {
                            let position = Position {
                                seq: position_seq,
                                pending: position_pending
                                    .map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                                streams: Default::default(),
                            }
                            .with_streams_wire(&position_streams);
                            let outcome = if outcome.is_empty() {
                                MutateOutcome::Busy
                            } else {
                                MutateOutcome::from_postcard(&outcome)
                                    .unwrap_or(MutateOutcome::Busy)
                            };
                            tracing::trace!(
                                to,
                                rtt_us = started.elapsed().as_micros() as u64,
                                "forward round trip"
                            );
                            match &outcome {
                                MutateOutcome::Accepted { .. } => {
                                    forward.record_ok(started.elapsed())
                                }
                                _ => forward.record_err(),
                            }
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::MutateReply {
                                    req,
                                    outcome,
                                    base,
                                    position,
                                    gen,
                                    own_chunks: OwnChunks::from_wire(own_chunks, own_inos),
                                    own_rows: constellation_meta::OwnRows::from_wire(&own_rows),
                                },
                            }));
                        }
                        Ok(Ok(_)) => {
                            forward.record_err();
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::MutateReply {
                                    req,
                                    outcome: MutateOutcome::Busy,
                                    base: None,
                                    position: Position::ZERO,
                                    gen: 0,
                                    own_chunks: OwnChunks::None,
                                    own_rows: None,
                                },
                            }));
                        }
                        _ => {
                            forward.record_err();
                            // Round 3a/4's rule: an outage only with no
                            // open connection left to the holder.
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::LeaseRequest { req, epoch_applied } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let requester = self.node_id;
                let timeout = Duration::from_millis(self.core.config().handoff_request_timeout_ms);
                let denied = crate::fault::p2p_denied(to);
                tokio::spawn(async move {
                    let payload = Payload::LeaseRequest {
                        part: PARTITION.to_string(),
                        requester,
                        epoch_applied,
                    };
                    if denied {
                        // Fault injection: the link to this peer is cut —
                        // a connection failure, at once.
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        }));
                        return;
                    }
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::LeaseHandoff {
                            released,
                            epoch,
                            head_seq,
                            ..
                        })) => {
                            if released {
                                tracing::info!(holder = to, epoch, "peer handed the lease over");
                            }
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::LeaseHandoff {
                                    req,
                                    released,
                                    epoch,
                                    head_seq,
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::LogStream {
                req,
                n,
                epoch,
                head,
                segment,
            } => {
                let seq = segment.as_ref().map(|(seq, _)| *seq);
                let bytes = segment.as_ref().map(|(_, p)| p.len() as u64).unwrap_or(0);
                self.stream_to(
                    to,
                    req,
                    LogEvent::Frame {
                        n,
                        epoch,
                        head,
                        segment,
                    },
                    bytes,
                    seq,
                );
            }
            PeerMsg::LogStreamEnd { req, refused } => {
                if self.subscribers.get(&to).is_some_and(|s| s.req == req) {
                    let sink = self.subscribers.remove(&to).expect("present");
                    let _ = sink.tx.try_send(LogEvent::End { refused });
                } else if refused {
                    // A refusal the handler has not got a sink for (it
                    // raced a replacement): nothing to write to.
                }
            }
            PeerMsg::BackupAck {
                req,
                epoch,
                acked,
                sealed,
            } => {
                let _ = epoch;
                tracing::trace!(target: "constellation::fwd", req = req.0, "append reply sent");
                if let Some(tx) = self.backup_replies.remove(&req) {
                    let _ = tx.send((acked, sealed));
                }
            }
            PeerMsg::BackupAppend {
                req,
                epoch,
                holder,
                config_version,
                candidacy,
                from,
                txs,
                through,
                expires_unix_ms,
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                // A slow backup's answer still counts (`backup_slow_max_ms`,
                // `Core::backup_timeouts`): waiting only the ack timeout
                // for it threw away every acknowledgement a loaded backup
                // gave more than a second late.
                let timeout = Duration::from_millis(self.core.config().backup_slow_max_ms);
                let cut = Duration::from_millis(self.core.config().backup_ack_timeout_ms);
                let denied = crate::fault::p2p_denied(to);
                let txs = match postcard::to_allocvec(&txs) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "encoding a backup append; dropped");
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: false,
                        }));
                        return;
                    }
                };
                tokio::spawn(async move {
                    if denied {
                        // Fault injection: the link to this peer is cut.
                        tokio::time::sleep(cut).await;
                        let _ = tx.send(Internal::Event(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        }));
                        return;
                    }
                    let payload = Payload::BackupAppend {
                        holder,
                        req_id: req.0,
                        epoch,
                        config_version,
                        candidacy,
                        from,
                        txs,
                        through,
                        expires_unix_ms,
                    };
                    let started = std::time::Instant::now();
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    tracing::trace!(
                        target: "constellation::backup_rtt",
                        to,
                        from,
                        us = started.elapsed().as_micros() as u64,
                        "backup append round trip"
                    );
                    match reply {
                        Ok(Ok(Payload::BackupAck {
                            req_id,
                            epoch,
                            acked,
                            sealed,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::BackupAck {
                                    req,
                                    epoch,
                                    acked,
                                    sealed,
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::BackupHold { epoch, for_ms } => {
                // Sent by `SyncRequest::HoldBackups`, which waits for the
                // answers; one the core asked for is fire and forget.
                let peers = self.deps.peers.clone();
                let holder = self.node_id;
                let timeout = Duration::from_millis(self.core.config().backup_ack_timeout_ms);
                tokio::spawn(async move {
                    let payload = Payload::BackupHold {
                        holder,
                        epoch,
                        for_ms,
                    };
                    let _ = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                });
            }
            PeerMsg::StreamAhead { epoch, base, txs } => {
                let peers = self.deps.peers.clone();
                let from = self.node_id;
                let timeout = Duration::from_millis(self.core.config().backup_ack_timeout_ms);
                if crate::fault::p2p_denied(to) {
                    return;
                }
                let Ok(txs) = postcard::to_allocvec(&txs) else {
                    return;
                };
                tokio::spawn(async move {
                    let payload = Payload::StreamAhead {
                        from,
                        epoch,
                        base,
                        txs,
                    };
                    // Fire and forget: a follower that misses a batch
                    // gets the rows from the segment.
                    let _ = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                });
            }
            PeerMsg::PromiseReply {
                req,
                until,
                epoch_slack,
            } => {
                if let Some(tx) = self.promise_replies.remove(&req) {
                    let _ = tx.send((until, epoch_slack));
                }
            }
            PeerMsg::PromiseRequest {
                req,
                expires_unix_ms,
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let requester = self.node_id;
                let timeout = Duration::from_millis(self.core.config().promise_wait_ms);
                if crate::fault::p2p_denied(to) {
                    let _ = tx.send(Internal::Event(Event::PeerFailed {
                        req,
                        to,
                        outage: true,
                    }));
                    return;
                }
                tokio::spawn(async move {
                    let payload = Payload::PromiseRequest {
                        requester,
                        req_id: req.0,
                        expires_unix_ms,
                    };
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::PromiseReply {
                            req_id,
                            until,
                            epoch_slack,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::PromiseReply {
                                    req,
                                    until,
                                    epoch_slack,
                                },
                            }));
                        }
                        _ => {
                            let outage =
                                crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::LogSubscribe { req, from } => self.spawn_subscription(to, req, from),
            PeerMsg::LogUnsubscribe { req } => {
                // Dropping our end of the stream is the unsubscription:
                // the holder's writer fails and its driver drops us.
                if let Some(handle) = self.subscriptions.remove(&req) {
                    handle.abort();
                }
            }
            other => {
                tracing::debug!(to, ?other, "unsupported peer message; dropped");
            }
        }
    }

    /// Plan 30 §M14: one lock RPC to `to`, bounded by the forward
    /// timeout; `answer` turns the matching reply into the core's
    /// message. No answer (timeout, a cut link, a reply for another
    /// request) is the request's `PeerFailed`.
    fn lock_rpc(
        &self,
        to: NodeId,
        req: OpId,
        payload: Payload,
        answer: impl FnOnce(Payload, OpId) -> Option<PeerMsg> + Send + 'static,
    ) {
        let tx = self.int_tx.clone();
        let peers = self.deps.peers.clone();
        let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
        if crate::fault::p2p_denied(to) {
            // Fault injection: the link to this peer is cut — a
            // connection failure, at once.
            let _ = tx.send(Internal::Event(Event::PeerFailed {
                req,
                to,
                outage: true,
            }));
            return;
        }
        tokio::spawn(async move {
            let reply = tokio::time::timeout(
                timeout,
                peers.request_to_node_timeout(to, &payload, timeout),
            )
            .await;
            match reply.ok().and_then(|r| r.ok()).and_then(|r| answer(r, req)) {
                Some(msg) => {
                    let _ = tx.send(Internal::Event(Event::Peer { from: to, msg }));
                }
                None => {
                    let outage = crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                    let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                }
            }
        });
    }

    /// [`Driver::lock_rpc`] for this node's own lock renewal: the answer,
    /// or its failure, comes back on the urgent lane
    /// ([`SyncRequest::LockRenewAnswered`]), as the renewal was at the
    /// owner. On the internal channel it waited behind a backlog of
    /// frames there, and the grant lapsed under the application's lock
    /// all the same. The renewal's timeout stays on the internal channel:
    /// an answer queued when it fires is taken first.
    fn lock_renew_rpc(&self, to: NodeId, req: OpId, payload: Payload) {
        let urgent = self.deps.urgent_tx.clone();
        let int_tx = self.int_tx.clone();
        let answer = move |results: Option<LockRenewResults>, outage: bool| {
            lock_renew_answer(urgent.as_ref(), &int_tx, to, req, results, outage)
        };
        if crate::fault::p2p_denied(to) {
            // Fault injection: the link to this peer is cut — a
            // connection failure, at once.
            answer(None, true);
            return;
        }
        let peers = self.deps.peers.clone();
        let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
        tokio::spawn(async move {
            let reply = tokio::time::timeout(
                timeout,
                peers.request_to_node_timeout(to, &payload, timeout),
            )
            .await;
            match reply.ok().and_then(|r| r.ok()) {
                Some(Payload::LockRenewed { req_id, results }) if req_id == req.0 => {
                    answer(Some(crate::locks::renew_results_of(results)), false);
                }
                _ => {
                    let outage = crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                    answer(None, outage);
                }
            }
        });
    }

    /// [`Driver::one_way`] for a message the core tracks as `req`: one
    /// that did not reach `to` (refused by the fault switch, a dial or
    /// connection failure, not queued there in time) comes back as
    /// `PeerFailed`.
    fn one_way_reported(&self, to: NodeId, req: OpId, payload: Payload) {
        let tx = self.int_tx.clone();
        if crate::fault::p2p_denied(to) {
            let _ = tx.send(Internal::Event(Event::PeerFailed {
                req,
                to,
                outage: true,
            }));
            return;
        }
        let peers = self.deps.peers.clone();
        let timeout = Duration::from_millis(self.core.config().forward_timeout_ms * 4);
        tokio::spawn(async move {
            let reply = tokio::time::timeout(
                timeout,
                peers.request_to_node_timeout(to, &payload, timeout),
            )
            .await;
            if !matches!(reply, Ok(Ok(Payload::Ok { .. }))) {
                let outage = crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
            }
        });
    }

    /// Plan 30 §M14: a one-way message (fire and forget, like
    /// `StreamAhead`): the core's timers cover a lost one.
    fn one_way(&self, to: NodeId, payload: Payload) {
        if crate::fault::p2p_denied(to) {
            return;
        }
        let peers = self.deps.peers.clone();
        let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
        tokio::spawn(async move {
            let _ = tokio::time::timeout(
                timeout,
                peers.request_to_node_timeout(to, &payload, timeout),
            )
            .await;
        });
    }

    /// Plan 30 §M7, holder side: queue one frame for subscriber `to`. The
    /// holder never waits on a subscriber: a full queue, or one holding
    /// more than the byte budget, drops the subscriber (it falls back to
    /// S3 and resubscribes), and the core is told.
    fn stream_to(&mut self, to: NodeId, req: OpId, event: LogEvent, bytes: u64, seq: Option<u64>) {
        let Some(sink) = self.subscribers.get(&to) else {
            let _ = self
                .int_tx
                .send(Internal::Event(Event::SubscriberGone { node: to, req }));
            return;
        };
        if sink.req != req {
            return;
        }
        let over_budget = sink.queued_bytes.load(Ordering::Relaxed) + bytes
            > self.stream_buffer_bytes
            && bytes > 0;
        let sent = if over_budget {
            Err("byte budget")
        } else {
            sink.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
            match sink.tx.try_send(event) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err("queue full"),
                Err(mpsc::error::TrySendError::Closed(_)) => Err("gone"),
            }
        };
        match sent {
            Ok(()) => {
                if let Some(seq) = seq {
                    tracing::debug!(
                        target: "constellation::log_stream",
                        to,
                        seq,
                        bytes,
                        "stream send queued"
                    );
                }
            }
            Err(why) => {
                tracing::info!(
                    target: "constellation::log_stream",
                    subscriber = to,
                    why,
                    "dropping a log-stream subscriber; it falls back to S3"
                );
                self.subscribers.remove(&to);
                let _ = self
                    .int_tx
                    .send(Internal::Event(Event::SubscriberGone { node: to, req }));
            }
        }
    }

    /// Plan 30 §M7, subscriber side: open the stream and turn what it
    /// carries into core events; its end (without the holder's `End`) is
    /// the subscription's `PeerFailed`.
    fn spawn_subscription(&mut self, to: NodeId, req: OpId, from: u64) {
        self.subscriptions.retain(|_, h| !h.is_finished());
        let tx = self.int_tx.clone();
        let permits = self.log_permits.clone();
        let peers = self.deps.peers.clone();
        let handle = tokio::spawn(async move {
            let mut rx = match peers.subscribe_log(to, req.0, from).await {
                Ok(rx) => rx,
                Err(error) => {
                    tracing::debug!(
                        target: "constellation::log_stream",
                        holder = to,
                        %error,
                        "could not open the holder's log stream"
                    );
                    let outage = crate::fault::p2p_denied(to) || !peers.connection_alive(to).await;
                    let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                    return;
                }
            };
            let mut ended = false;
            let budget = log_backlog_bytes() as u32;
            while let Some(event) = rx.recv().await {
                // Its bytes of the backlog budget (at least one permit, at
                // most the whole budget).
                let cost = match &event {
                    LogEvent::Frame {
                        segment: Some((_, bytes)),
                        ..
                    } => (bytes.len().min(budget as usize) as u32).max(1),
                    _ => 1,
                };
                let msg = match event {
                    LogEvent::Frame {
                        n,
                        epoch,
                        head,
                        segment,
                    } => {
                        if let Some((seq, bytes)) = &segment {
                            tracing::debug!(
                                target: "constellation::log_stream",
                                holder = to,
                                seq,
                                n,
                                bytes = bytes.len(),
                                "stream receive"
                            );
                        }
                        PeerMsg::LogStream {
                            req,
                            n,
                            epoch,
                            head,
                            segment,
                        }
                    }
                    LogEvent::End { refused } => {
                        ended = true;
                        PeerMsg::LogStreamEnd { req, refused }
                    }
                };
                // Bounded ([`Driver::log_permits`]): a core that far behind
                // the stream stops this read, the holder's queue for this
                // node fills, and the holder drops it to S3 tailing.
                let Ok(permit) = permits.clone().acquire_many_owned(cost).await else {
                    return;
                };
                if tx
                    .send(Internal::Frame(Event::Peer { from: to, msg }, permit))
                    .is_err()
                {
                    return;
                }
            }
            if !ended {
                let _ = tx.send(Internal::Event(Event::PeerFailed {
                    req,
                    to,
                    outage: false,
                }));
            }
        });
        self.subscriptions.insert(req, handle.abort_handle());
    }

    fn spawn_s3(&mut self, op: OpId, req: S3Op) {
        let kind = s3_kind(&req);
        let started = std::time::Instant::now();
        tracing::trace!(?op, kind, "s3 op issued");
        let tx = self.int_tx.clone();
        let leases = self.leases.clone();
        let log = self.deps.log.clone();
        let inbox = self.inbox.clone();
        let view = self.deps.view.clone();
        let meta = self.deps.meta.clone();
        let heartbeats = constellation_store_s3::HeartbeatStore::new(self.deps.store_inner.clone());
        let uploader = self.uploader();
        tokio::spawn(async move {
            let result = match req {
                S3Op::LeaseGet => S3Result::LeaseGet(leases.get().await.map_err(s3_failure)),
                S3Op::LeaseCreate { lease } => {
                    S3Result::LeasePut(leases.try_create(&lease).await.map_err(cas_failure))
                }
                S3Op::LeaseSwap { lease, tag } => {
                    if lease.released {
                        // The FUSE fast path: a write admitted before the
                        // releasing flag went up finishes before the CAS,
                        // and one that landed after the flush fails the
                        // release rather than being stranded behind it.
                        view.wait_quiescent().await;
                        if constellation_meta::MetaStore::journal_len(&*meta).unwrap_or(0) > 0 {
                            let _ = tx.send(Internal::Event(Event::S3 {
                                op,
                                result: S3Result::LeasePut(Err(CasFailure::Failed(
                                    "a write landed before the release; retrying next round".into(),
                                ))),
                            }));
                            return;
                        }
                    }
                    S3Result::LeasePut(leases.try_swap(&lease, &tag).await.map_err(cas_failure))
                }
                S3Op::SegmentPut { seq, payload } => {
                    S3Result::SegmentPut(log.put_segment(seq, &payload).await.map_err(cas_failure))
                }
                S3Op::SegmentRun { from, width } => {
                    S3Result::SegmentRun(log.get_run(from, width).await.map_err(s3_failure))
                }
                S3Op::SegmentGap { from } => {
                    S3Result::SegmentGap(log.first_segment_from(from).await.map_err(s3_failure))
                }
                S3Op::InboxPut { batch } => {
                    // The inbox carries no pending-chunk list (a P2P forward
                    // does): a manifest goes through it only once its
                    // chunks are up, as every close's did before.
                    let mut drained = Ok(());
                    for ino in inbox_manifests_pending(&uploader, &batch) {
                        if let Err(e) = uploader.run(Some(ino)).await {
                            drained = Err(CasFailure::Failed(format!(
                                "uploading a manifest's chunks before the inbox: {e:#}"
                            )));
                            break;
                        }
                    }
                    S3Result::InboxPut(match drained {
                        Ok(()) => inbox
                            .put_batch(&batch)
                            .await
                            .map(|_| ())
                            .map_err(cas_failure),
                        Err(e) => Err(e),
                    })
                }
                S3Op::InboxRun { .. } if crate::fault::inbox_polls_paused() => {
                    S3Result::InboxRun(Ok(Vec::new()))
                }
                S3Op::InboxRun {
                    epoch,
                    node,
                    from,
                    width,
                } => S3Result::InboxRun(
                    inbox
                        .get_run(epoch, node, from, width)
                        .await
                        .map_err(s3_failure),
                ),
                S3Op::InboxDrain { below_epoch } => {
                    S3Result::InboxDrain(inbox.drain_below(below_epoch).await.map_err(s3_failure))
                }
                S3Op::InboxDelete { key } => {
                    S3Result::InboxDelete(inbox.delete(key).await.map_err(s3_failure))
                }
                S3Op::InboxTombstone { batch } => {
                    S3Result::InboxTombstone(inbox.put_tombstone(&batch).await.map_err(s3_failure))
                }
                S3Op::InboxLastN { epoch, node } => {
                    S3Result::InboxLastN(inbox.last_n(epoch, node).await.map_err(s3_failure))
                }
                S3Op::HeartbeatRead => {
                    S3Result::Heartbeats(heartbeats.read_all().await.map_err(s3_failure))
                }
                S3Op::HeartbeatPut { promise } => {
                    S3Result::HeartbeatPut(heartbeats.put(&promise).await.map_err(s3_failure))
                }
            };
            tracing::trace!(
                ?op,
                kind,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "s3 op done"
            );
            let _ = tx.send(Internal::Event(Event::S3 { op, result }));
        });
    }

    fn control_done(&mut self, op: OpId, result: std::result::Result<ControlOk, String>) {
        let Some(reply) = self.controls.remove(&op) else {
            return;
        };
        match reply {
            ControlReply::None => {}
            ControlReply::Done(tx) => {
                let _ = tx.send(result.map(|_| ()));
            }
            ControlReply::Text(tx) => {
                self.deps
                    .reintegration
                    .in_progress
                    .store(false, Ordering::Relaxed);
                let _ = tx.send(result.map(|ok| match ok {
                    ControlOk::Text(t) => t,
                    _ => "done".to_string(),
                }));
            }
            ControlReply::Acquire(tx) => {
                let _ = tx.send(result.map(|ok| match ok {
                    ControlOk::Lease {
                        acquired,
                        holder,
                        epoch,
                    } => AcquireProgress {
                        acquired,
                        holder,
                        epoch,
                    },
                    _ => AcquireProgress::busy(0, 0),
                }));
            }
            ControlReply::ClaimOffer => {
                if let Ok(ControlOk::Lease { acquired: true, .. }) = result {
                    self.deps.placement.mark_migrated();
                }
            }
            ControlReply::ReadIndex(tx) => {
                let _ = tx.send(match result {
                    Ok(ControlOk::ReadIndex(answer)) => answer,
                    // The S3 path (no live sequencer, or no P2P): a
                    // tail-to-head job answered it.
                    Ok(_) => ReadAnswer::Tailed,
                    Err(_) => ReadAnswer::Degraded,
                });
            }
            ControlReply::Recall(tx) => {
                let _ = tx.send(());
            }
            ControlReply::Lock(tx) => {
                let _ = tx.send(match result {
                    Ok(ControlOk::Lock(answer)) => answer,
                    // Refused or dropped: no grant (`ENOLCK`).
                    _ => LockAnswer::Unavailable,
                });
            }
            ControlReply::LockTest(tx) => {
                let _ = tx.send(match result {
                    Ok(ControlOk::LockTest(answer)) => answer,
                    _ => LockTestAnswer::Free,
                });
            }
            ControlReply::Publish(tx) => match result {
                Ok(_) => {
                    // The round shipped everything; publish the replica as
                    // it stands (what a snapshot retains).
                    let Some(publisher) = self.deps.publisher.clone() else {
                        let _ = tx.send(Err("this mount has no metadata tree publisher".into()));
                        return;
                    };
                    let epoch = self.core.ship().last_ship_epoch;
                    tokio::spawn(async move {
                        let r = publisher.lock().await.publish_now(epoch).await;
                        let _ = tx.send(r.map_err(|e| format!("{e:#}")));
                    });
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
            },
            ControlReply::Leave(tx) => match result {
                Ok(_) => {
                    let store = self.deps.store_inner.clone();
                    let meta = self.deps.meta.clone();
                    let node_id = self.node_id;
                    tokio::spawn(async move {
                        let r = crate::leave::finish_leave(store, &meta, node_id).await;
                        let _ = tx.send(
                            r.map(|_| {
                                format!("left cluster as node {node_id}; registry record retired")
                            })
                            .map_err(|e| e.to_string()),
                        );
                    });
                }
                Err(e) => {
                    let _ = tx.send(Err(format!(
                        "cannot flush before leave (is S3 reachable?): {e}"
                    )));
                }
            },
        }
    }
}

/// The inodes of the `SetManifest` ops in `batch` whose chunks are still
/// pending here.
fn inbox_manifests_pending(
    uploader: &Uploader,
    batch: &constellation_store_s3::InboxBatch,
) -> Vec<constellation_fs_core::Ino> {
    let mut inos = Vec::new();
    for op in &batch.ops {
        let Ok(op) = MutateOp::from_postcard(&op.op) else {
            continue;
        };
        if let MutateOp::SetManifest { ino, .. } = &op {
            if !crate::upload::forwarded_pending_chunks(&uploader.meta, &uploader.cache, &op)
                .is_empty()
                && !inos.contains(ino)
            {
                inos.push(*ino);
            }
        }
    }
    inos
}

/// The chunk upload pass, as the driver's spawned tasks run it.
#[derive(Clone)]
struct Uploader {
    cache: Arc<DiskCache>,
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    compression: CompressionSetting,
    upload: Arc<crate::upload::UploadRuntime>,
    /// For the durable reports this pass owes (`meta::store::remote`).
    peers: Option<constellation_net::Peers>,
    node_id: u64,
}

impl Uploader {
    async fn run(&self, only_ino: Option<constellation_fs_core::Ino>) -> Result<()> {
        self.run_report(only_ino).await.map(|_| ())
    }

    /// An `fsync`'s drain of `ino` (plan 39b): a pass over the inode's
    /// rows, then the durable reports *those* chunks owe, delivered before
    /// it returns rather than in the background (each send bounded, 2 s;
    /// one not delivered is retried at the next pass, as before) — so when
    /// the `fsync` returns, the sequencer a `back` close forwarded the
    /// file's manifest to has acked its chunks, and a reader there does not
    /// wait `CONSTELLATION_REMOTE_CHUNK_WAIT_S` for a report that died with
    /// this node. Other files' reports go out from the background passes:
    /// an unreachable peer costs this `fsync` nothing unless it awaits this
    /// file. A report a concurrent background pass already took (it
    /// uploaded the chunk this drain waited for) goes out from that pass's
    /// own task.
    ///
    /// Never `Ok` with the inode not durable, whatever the cause:
    /// - a pending chunk of `ino` that is neither in the cache nor in S3
    ///   (a torn disk) is [`crate::upload::DrainShortfall::Lost`],
    ///   permanent — `EIO` now and on every later `fsync` while the row
    ///   stays (a background pass records it unrecoverable, M4);
    /// - rows of `ino` another node forwarded as pending (this node is the
    ///   sequencer of that node's `back` close, `meta::store::remote`) are
    ///   waited for — their reports, or the S3 checks, ack them — as
    ///   [`Self::run_complete`] does for a barrier; still there after
    ///   [`FSYNC_REMOTE_SLICE`] (or `CONSTELLATION_REMOTE_CHUNK_WAIT_S`, if
    ///   shorter) the drain fails
    ///   [`crate::upload::DrainShortfall::AwaitingRemote`], transient, and
    ///   the `fsync`'s retry loop keeps waiting under plan 39's policy (hard
    ///   by default; `--fsync-timeout` and an interrupt bound it). A barrier
    ///   gives up on such chunks after `CONSTELLATION_REMOTE_CHUNK_WAIT_S`
    ///   and holds the records that need them; an `fsync` of the file never
    ///   turns that into a success.
    async fn run_fsync(&self, ino: constellation_fs_core::Ino) -> Result<()> {
        let started = std::time::Instant::now();
        let slice = crate::upload::remote_chunk_wait().min(FSYNC_REMOTE_SLICE);
        loop {
            let mine: std::collections::HashSet<_> = self
                .meta
                .pending_uploads_for_ino(ino)?
                .into_iter()
                .collect();
            let result = crate::upload::upload_dirty_chunks_report(
                &self.cache,
                &self.meta,
                &self.store,
                self.compression,
                &self.upload,
                Some(ino),
                None,
            )
            .await;
            self.send_durable_reports_of(Some(&mine)).await;
            let report = result?;
            let lost: Vec<_> = report.missing.iter().filter(|(_, i)| *i == ino).collect();
            if let Some((hash, _)) = lost.first() {
                return Err(crate::upload::DrainShortfall::Lost {
                    ino,
                    hash: *hash,
                    count: lost.len(),
                }
                .into());
            }
            if report.awaiting == 0 {
                return Ok(());
            }
            if started.elapsed() >= slice {
                return Err(crate::upload::DrainShortfall::AwaitingRemote {
                    ino,
                    awaiting: report.awaiting,
                    waited_s: started.elapsed().as_secs(),
                }
                .into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The round's opportunistic pass, which the upload hold (plan 31 C8,
    /// `UploadMode::UnmeteredOnly`) keeps from taking new chunks.
    async fn run_background(&self) -> Result<()> {
        let result = crate::upload::upload_dirty_chunks_background(
            &self.cache,
            &self.meta,
            &self.store,
            self.compression,
            &self.upload,
        )
        .await;
        let reporter = self.clone();
        tokio::spawn(async move { reporter.send_durable_reports().await });
        result.map(|_| ())
    }

    /// One pass, then the reports it owes: every node this node forwarded
    /// a manifest to while some of its chunks were still pending here
    /// learns which of them are up now, so it can let the manifest go.
    /// Sent in the background (a pass never waits on a peer); one that
    /// does not arrive is retried at the next passes, and the recipient
    /// checks S3 itself in the end.
    async fn run_report(
        &self,
        only_ino: Option<constellation_fs_core::Ino>,
    ) -> Result<crate::upload::UploadReport> {
        let result = crate::upload::upload_dirty_chunks_report(
            &self.cache,
            &self.meta,
            &self.store,
            self.compression,
            &self.upload,
            only_ino,
            None,
        )
        .await;
        let reporter = self.clone();
        tokio::spawn(async move { reporter.send_durable_reports().await });
        result
    }

    async fn send_durable_reports(&self) {
        self.send_durable_reports_of(None).await
    }

    /// [`Self::send_durable_reports`], limited to the reports `only`'s
    /// chunks owe (`None`: every report owed).
    async fn send_durable_reports_of(
        &self,
        only: Option<&std::collections::HashSet<constellation_fs_core::ChunkHash>>,
    ) {
        let Some(peers) = &self.peers else {
            return;
        };
        let mut reports = match only {
            None => self.upload.take_durable_reports(),
            Some(hashes) => self.upload.take_durable_reports_of(hashes),
        };
        if reports.is_empty() {
            return;
        }
        if let Some(all) = reports.remove(&crate::upload::REPORT_TO_ALL) {
            let known = peers.remote_snapshot();
            if known.is_empty() {
                // Right after a restart: nobody to tell yet.
                self.upload
                    .requeue_report(crate::upload::REPORT_TO_ALL, all);
            } else {
                self.upload.report_delivered(crate::upload::REPORT_TO_ALL);
                for peer in known {
                    reports.entry(peer.node_id).or_default().extend(&all);
                }
            }
        }
        let sends = reports.into_iter().map(|(node, hashes)| {
            let payload = Payload::ChunksDurable {
                from: self.node_id,
                hashes: hashes.iter().map(|h| h.0).collect(),
            };
            async move {
                let sent = tokio::time::timeout(
                    Duration::from_secs(2),
                    peers.request_to_node(node, &payload),
                )
                .await;
                if matches!(sent, Ok(Ok(_))) {
                    self.upload.report_delivered(node);
                } else {
                    tracing::debug!(
                        node,
                        chunks = hashes.len(),
                        "durable report not delivered; retried at the next pass"
                    );
                    self.upload.requeue_report(node, hashes);
                }
            }
        });
        futures::future::join_all(sends).await;
    }

    /// A pass that must leave nothing pending (a barrier, a forced
    /// publish, a final flush): chunks other nodes forwarded as pending
    /// are waited for — their reports ack them — up to
    /// `CONSTELLATION_REMOTE_CHUNK_WAIT_S`; past that the ship holds what
    /// needs them and the caller sees the journal not shipped.
    async fn run_complete(&self) -> Result<()> {
        let started = std::time::Instant::now();
        let limit = crate::upload::remote_chunk_wait();
        loop {
            let report = self.run_report(None).await?;
            if report.awaiting == 0 || started.elapsed() >= limit {
                if report.awaiting > 0 {
                    tracing::warn!(
                        awaiting = report.awaiting,
                        waited_s = started.elapsed().as_secs(),
                        "chunks other nodes forwarded as pending are still not in S3"
                    );
                }
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// EC2 campaign 8 A-1: how long this node's S3 path may complete nothing
/// while a request of this node waits on it (or failed unanswered) before
/// the core is told it is stalled (`Event::OwnS3`, `OwnS3Watch`;
/// `CONSTELLATION_S3_STALL_MS`, default 6000 — longer than the 5 s
/// registry poll, so a working path always shows a completion inside it;
/// 0 disables).
fn s3_stall_ms() -> u64 {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        std::env::var("CONSTELLATION_S3_STALL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6000)
    })
}

/// How often, while stalled, the live peers are asked whether they reach
/// S3 (`PingS3`: one bounded lease GET on each).
const OWN_S3_ASK_EVERY: Duration = Duration::from_secs(5);
/// How long an answer to that question may take.
const OWN_S3_ASK_TIMEOUT: Duration = Duration::from_secs(3);

/// How often the driver sends the core its peer links when nothing
/// changes.
const PEER_LINKS_TICK: Duration = Duration::from_millis(1_000);

/// [`PeerLink`]s from the peer directory. `since`: when each peer's link
/// came up, for the backup selection's stability requirement (plan 30
/// §M9).
fn peer_links(peers: &constellation_net::Peers, since: &mut HashMap<u64, WaitMs>) -> Vec<PeerLink> {
    let now = WaitMs(wait_now_ms());
    peers
        .snapshot()
        .into_iter()
        .map(|p| {
            // Fault injection: a denied peer is a link that is down (the
            // cut is a real one to the core: the inbox path, no redirects
            // to it).
            let connected = p.connected && !crate::fault::p2p_denied(p.node_id);
            let up_since = if connected {
                Some(*since.entry(p.node_id).or_insert(now))
            } else {
                since.remove(&p.node_id);
                None
            };
            PeerLink {
                node: p.node_id,
                connected,
                last_seen: if connected {
                    p.last_seen
                        .map(|at| WaitMs(wait_now_ms() - at.elapsed().as_millis() as i64))
                } else {
                    None
                },
                rtt_ms: p.rtt_ms,
                since: up_since,
            }
        })
        .collect()
}

/// Sends the core `Event::Peers` and `Event::OwnS3` every
/// [`PEER_LINKS_TICK`], and `Event::Peers` again as soon as a peer enrols
/// or leaves or its link goes up or down (`startup-link-lag`: with the
/// tick alone, a peer that had just enrolled was not in the core's links
/// for up to a second, so a write into its fresh delegation went to the
/// root, which recalled the delegation).
async fn peer_links_task(
    peers: constellation_net::Peers,
    mut own_s3: OwnS3Watch,
    tx: mpsc::UnboundedSender<Internal>,
) {
    let mut since: HashMap<u64, WaitMs> = HashMap::new();
    let mut changed = peers.links_changed();
    let mut tick = tokio::time::Instant::now();
    loop {
        let ticked = tokio::time::Instant::now() >= tick;
        if ticked {
            let (stalled, peers_reach_s3) = own_s3.tick();
            if tx
                .send(Internal::Event(Event::OwnS3 {
                    stalled,
                    peers_reach_s3,
                }))
                .is_err()
            {
                return;
            }
            tick = tokio::time::Instant::now() + PEER_LINKS_TICK;
        }
        // Read before the snapshot: a change after it wakes us again.
        if let Some(rx) = changed.as_mut() {
            rx.borrow_and_update();
        }
        let links = peer_links(&peers, &mut since);
        if tx.send(Internal::Event(Event::Peers { links })).is_err() {
            return;
        }
        match changed.as_mut() {
            Some(rx) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(tick) => {}
                    r = rx.changed() => {
                        if r.is_err() {
                            changed = None;
                        }
                    }
                }
            }
            None => tokio::time::sleep_until(tick).await,
        }
    }
}

/// A watch tick this long after the previous one (they are a second
/// apart) came late: the daemon itself did not run (its runtime's workers
/// all held up, by the metadata store's writer lock in finding 14), and
/// neither did the reading of answers that had arrived meanwhile. Counted
/// in `status.own_s3.late_ticks`.
const OWN_S3_LATE_TICK: Duration = Duration::from_secs(3);

/// Late watch ticks since the daemon started ([`OWN_S3_LATE_TICK`]).
static OWN_S3_LATE_TICKS: AtomicU64 = AtomicU64::new(0);

/// `status.own_s3.late_ticks`.
pub fn own_s3_late_ticks() -> u64 {
    OWN_S3_LATE_TICKS.load(Ordering::Relaxed)
}

/// EC2 campaign 8 A-1: this node's own S3 path for the core, once a
/// second. Stalled when no S3 request (and no chunk PUT) of this node
/// has completed for [`s3_stall_ms`] while one waited on S3 that long
/// (`UploadRuntime::uploads_stall_in`), and the watch saw it waiting
/// through as many consecutive ticks as the window has seconds; while
/// stalled, the live peers are asked every [`OWN_S3_ASK_EVERY`] whether
/// they reach S3, in the background (the tick never waits on them).
///
/// Counting ticks, not only time, keeps a stall of the daemon itself from
/// reading as S3's (finding 14): requests whose answers arrived while the
/// runtime was held up look outstanding all that time, but the stall is
/// one tick however long it lasted, so the watch waits a window's worth of
/// ticks after it before it says S3 is stalled. A runtime whose ticks are
/// all late still gets there, a tick at a time. A stall already seen
/// lasts until S3 answers or a chunk PUT lands.
struct OwnS3Watch {
    upload: Arc<crate::upload::UploadRuntime>,
    peers: constellation_net::Peers,
    asked: Option<std::time::Instant>,
    answer: Arc<std::sync::Mutex<Option<bool>>>,
    /// The previous tick: when (for lateness) and at which unix ms.
    last_tick: Option<(std::time::Instant, i64)>,
    /// Consecutive ticks through which S3 left this node waiting
    /// (`S3Waits::waited_since` the previous tick).
    waited_ticks: u64,
    stalled: bool,
}

impl OwnS3Watch {
    fn new(upload: Arc<crate::upload::UploadRuntime>, peers: constellation_net::Peers) -> Self {
        Self {
            upload,
            peers,
            asked: None,
            answer: Arc::new(std::sync::Mutex::new(None)),
            last_tick: None,
            waited_ticks: 0,
            stalled: false,
        }
    }

    fn tick(&mut self) -> (bool, Option<bool>) {
        self.tick_at(std::time::Instant::now(), wait_now_ms())
    }

    fn tick_at(&mut self, now: std::time::Instant, now_ms: i64) -> (bool, Option<bool>) {
        let ms = s3_stall_ms();
        let waits = self.upload.s3_waits();
        match self.last_tick {
            Some((at, at_ms)) => {
                if now.saturating_duration_since(at) >= OWN_S3_LATE_TICK {
                    OWN_S3_LATE_TICKS.fetch_add(1, Ordering::Relaxed);
                }
                if waits.waited_since(at_ms) {
                    self.waited_ticks += 1;
                } else {
                    self.waited_ticks = 0;
                }
            }
            None => self.waited_ticks = 0,
        }
        self.last_tick = Some((now, now_ms));
        let ticks = ms.div_ceil(PEER_LINKS_TICK.as_millis() as u64);
        let stalled = ms > 0
            && waits.stall_in(now_ms, ms as i64) == Some(0)
            && (self.stalled || self.waited_ticks >= ticks);
        self.stalled = stalled;
        if !stalled {
            self.asked = None;
            *self.answer.lock().unwrap() = None;
            return (false, None);
        }
        if self.asked.is_none_or(|at| at.elapsed() >= OWN_S3_ASK_EVERY) && self.peers.is_enabled() {
            self.asked = Some(std::time::Instant::now());
            let (peers, answer) = (self.peers.clone(), self.answer.clone());
            tokio::spawn(async move {
                let ids: Vec<u64> = peers.snapshot().iter().map(|p| p.node_id).collect();
                let answers = futures::future::join_all(ids.into_iter().map(|id| {
                    let peers = peers.clone();
                    async move {
                        tokio::time::timeout(OWN_S3_ASK_TIMEOUT, peers.ping_node_s3(id))
                            .await
                            .ok()
                            .flatten()
                    }
                }))
                .await;
                let reach = if answers.contains(&Some(true)) {
                    Some(true)
                } else if answers.contains(&Some(false)) {
                    Some(false)
                } else {
                    None
                };
                *answer.lock().unwrap() = reach;
            });
        }
        let answer = *self.answer.lock().unwrap();
        (true, answer)
    }
}

/// EC2 finding 1: how long a drain (a write-through close, a forwarded
/// manifest's pre-publication upload, an `fsync`) waits on this node's
/// S3 path making no progress — no upload and no other S3 request
/// completing, while one of its requests waits that long (or failed
/// unanswered; finding 14) — before it hands its chunks to a peer
/// (`CONSTELLATION_CHUNK_HANDOFF_AFTER_MS`, default 6000, longer than
/// the 5 s registry poll so a working link always shows a completion
/// inside it; 0 disables). The drain's first check comes `after` in and
/// rechecks when its first request will have waited that long
/// (`handoff_recheck`).
fn chunk_handoff_after_ms() -> u64 {
    static AFTER: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *AFTER.get_or_init(|| {
        std::env::var("CONSTELLATION_CHUNK_HANDOFF_AFTER_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6000)
    })
}

/// How long one attempt of an `fsync`'s drain waits for chunks another
/// node forwarded as pending before it answers
/// [`crate::upload::DrainShortfall::AwaitingRemote`] and the `fsync`'s
/// retry loop (its backoff, its "still trying" warning, its timeout)
/// takes over (`Uploader::run_fsync`).
const FSYNC_REMOTE_SLICE: Duration = Duration::from_secs(5);

/// How long a peer may take to fetch and upload a handoff's chunks.
const CHUNK_HANDOFF_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Hashes per `ChunkHandoff` message (a frame is at most 64 KiB).
const CHUNK_HANDOFF_BATCH: usize = 512;
/// After a drain needed a handoff, the next drains hand off at once for
/// this long while uploads still make no progress.
const CHUNK_HANDOFF_STICKY_MS: i64 = 30_000;

/// Upload `ino`'s pending chunks (every pending chunk for `ino == 0`),
/// handing them to a peer when this node's own S3 path makes no progress
/// (EC2 finding 1: a node that lost S3 but not its peers blocked every
/// close for the whole outage). Plan 08's invariant is untouched — a
/// forwarded manifest still names only chunks S3 has — it is only who
/// puts them there that changes. This node's own upload keeps running
/// in the background either way (a repeat PUT of content-addressed data
/// is harmless).
///
/// `fsync` (an `fsync`'s drain of `ino`, plan 39b): the pass is
/// [`Uploader::run_fsync`], and a drain that succeeds through a peer
/// delivers the durable reports of the chunks it handed off before it
/// returns, as the pass would have. A [`crate::upload::DrainShortfall`]
/// is not handed off: a lost chunk is lost to the peer too, and a chunk
/// another node still has is that node's to upload.
async fn drain_with_handoff(
    upload: Uploader,
    handoff: Option<ChunkHandoff>,
    ino: constellation_fs_core::Ino,
    fsync: bool,
) -> Result<()> {
    let (tx, mut done) = tokio::sync::oneshot::channel();
    let stats = upload.upload.clone();
    let reporter = fsync.then(|| upload.clone());
    tokio::spawn(async move {
        let result = if fsync {
            upload.run_fsync(ino).await
        } else {
            upload.run((ino != 0).then_some(ino)).await
        };
        let _ = tx.send(result);
    });
    // After a successful handoff: the reports its chunks owe (`note_up`
    // queued them), sent now on an `fsync`'s drain.
    let handed_off = |hashes: Vec<constellation_fs_core::ChunkHash>| {
        let reporter = reporter.clone();
        async move {
            if let Some(reporter) = reporter {
                let hashes = hashes.into_iter().collect();
                reporter.send_durable_reports_of(Some(&hashes)).await;
            }
            Ok(())
        }
    };
    let stopped = || anyhow::anyhow!("the drain task stopped");
    let after_ms = chunk_handoff_after_ms();
    let Some(handoff) = handoff.filter(|h| after_ms > 0 && h.possible()) else {
        return done.await.unwrap_or_else(|_| Err(stopped()));
    };
    let after = std::time::Duration::from_millis(after_ms);
    let now = wait_now_ms();
    let sticky = now
        - stats
            .handoff
            .last_needed_ms
            .load(std::sync::atomic::Ordering::Relaxed)
        < CHUNK_HANDOFF_STICKY_MS;
    let mut tried = false;
    let mut wait = if sticky && stats.uploads_stalled(after_ms as i64) {
        // S3 was just found unreachable and nothing has gone up since:
        // do not make this close wait the whole detection time again.
        std::time::Duration::from_millis(50)
    } else {
        after
    };
    loop {
        match tokio::time::timeout(wait, &mut done).await {
            Ok(result) => {
                let result = result.unwrap_or_else(|_| Err(stopped()));
                if let Err(error) = &result {
                    let shortfall = error
                        .downcast_ref::<crate::upload::DrainShortfall>()
                        .is_some();
                    if !tried && !shortfall {
                        tracing::info!(ino, %error, "drain failed; handing its chunks to a peer");
                        if let Some(hashes) = handoff.hand_off(ino).await {
                            return handed_off(hashes).await;
                        }
                    }
                }
                return result;
            }
            Err(_) if !tried => match handoff_recheck(&stats, after, wait_now_ms()) {
                None => {
                    tried = true;
                    tracing::info!(
                        ino,
                        waited_ms = wait.as_millis() as u64,
                        "no upload has completed on this node for a while: handing the drain's chunks to a peer"
                    );
                    if let Some(hashes) = handoff.hand_off(ino).await {
                        return handed_off(hashes).await;
                    }
                    wait = after;
                }
                Some(recheck) => wait = recheck,
            },
            Err(_) => wait = after,
        }
    }
}

/// A drain's handoff check: `None` when this node's S3 path is stalled
/// (hand the chunks off now), else how long to wait before looking again.
/// The drain's own first request goes out a little after the drain
/// starts, so at the drain's first check (`after` in) it has not waited
/// `after` yet: look again when it will have, not a whole `after` later
/// (which doubled the handoff time under a fresh black hole). Nothing
/// asked of S3: the full `after`. `now_ms`: the current time (unix ms).
fn handoff_recheck(
    stats: &crate::upload::UploadRuntime,
    after: std::time::Duration,
    now_ms: i64,
) -> Option<std::time::Duration> {
    let after_ms = after.as_millis() as i64;
    match stats.uploads_stall_in(now_ms, after_ms) {
        Some(0) => None,
        Some(ms) => Some(
            std::time::Duration::from_millis(ms as u64)
                .clamp(std::time::Duration::from_millis(50), after),
        ),
        None => Some(after),
    }
}

/// Once this node's S3 path has made no progress for
/// `chunk_handoff_after_ms`, hand the chunks `back` closes forwarded as
/// pending to a peer (checked every second: a cheap in-memory look while
/// nothing is forwarded).
async fn forwarded_handoff_watch(handoff: ChunkHandoff) {
    let after_ms = chunk_handoff_after_ms();
    if after_ms == 0 {
        return;
    }
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        // Plan 31 C8: a held upload is not a stalled one — handing the
        // chunks to a peer would send them over the metered network.
        if handoff.upload.has_forwarded()
            && !handoff.upload.hold.is_held()
            && handoff.upload.uploads_stalled(after_ms as i64)
        {
            handoff.hand_off_forwarded().await;
        }
    }
}

/// EC2 finding 1: moving chunks through a peer when this node cannot
/// reach S3 — the requester's side (`hand_off`) and the peer's
/// (`accept`).
#[derive(Clone)]
struct ChunkHandoff {
    node_id: u64,
    meta: Arc<Meta>,
    cache: Arc<DiskCache>,
    store: Arc<ChunkStore>,
    compression: CompressionSetting,
    upload: Arc<crate::upload::UploadRuntime>,
    peers: constellation_net::Peers,
    view: Arc<LeaseView>,
    /// This node's own sync task: an accepted handoff acks the remote
    /// rows the uploaded chunks satisfy (`SyncRequest::ChunksDurable`).
    sync_tx: mpsc::UnboundedSender<SyncRequest>,
}

impl ChunkHandoff {
    fn possible(&self) -> bool {
        self.peers.is_enabled() && self.upload.coop.is_some()
    }

    /// Hand `ino`'s pending chunks to a peer — the lease holder first,
    /// then any other connected peer — and, once one has them in S3,
    /// acknowledge them here as uploaded: the chunks handed off. `None`:
    /// nobody could.
    async fn hand_off(
        &self,
        ino: constellation_fs_core::Ino,
    ) -> Option<Vec<constellation_fs_core::ChunkHash>> {
        let hashes = self.meta.pending_uploads_for_ino(ino).ok()?;
        self.hand_off_hashes(hashes.clone(), &[], ino)
            .await
            .then_some(hashes)
    }

    /// Hand `hashes` (pending here) to a peer: the nodes in `first`, then
    /// the lease holder, then any other connected peer. Once one has them
    /// in S3 they are acknowledged here as uploaded, and reported to the
    /// nodes a `back` close forwarded them to (`UploadRuntime::note_up`),
    /// exactly as if this node's own upload had put them there. `false`:
    /// nobody could. (`ino` is for the log line; 0 for a round's.)
    async fn hand_off_hashes(
        &self,
        hashes: Vec<constellation_fs_core::ChunkHash>,
        first: &[u64],
        ino: constellation_fs_core::Ino,
    ) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(coop) = self.upload.coop.clone() else {
            return false;
        };
        let hashes: Vec<constellation_fs_core::ChunkHash> = hashes
            .into_iter()
            .filter(|h| self.meta.upload_pending_for_hash(h).unwrap_or(false))
            .collect();
        if hashes.is_empty() {
            // Uploaded meanwhile (or by another drain).
            return true;
        }
        self.upload
            .handoff
            .last_needed_ms
            .store(wait_now_ms(), Relaxed);
        self.upload.handoff.sent.fetch_add(1, Relaxed);
        let _offer = coop.offer(&hashes);
        let holder = self.view.status().holder;
        let mut targets: Vec<u64> = Vec::new();
        for n in first.iter().copied().chain([holder]) {
            if n != 0 && n != self.node_id && !targets.contains(&n) {
                targets.push(n);
            }
        }
        for p in self.peers.snapshot() {
            if p.node_id != self.node_id && !targets.contains(&p.node_id) && p.connected && !p.ro {
                targets.push(p.node_id);
            }
        }
        let req_id = constellation_store_s3::lease::now_unix_ms() as u64;
        'peer: for peer in targets {
            for batch in hashes.chunks(CHUNK_HANDOFF_BATCH) {
                let payload = constellation_net::Payload::ChunkHandoff {
                    requester: self.node_id,
                    req_id,
                    hashes: batch.iter().map(|h| h.0).collect(),
                };
                match self
                    .peers
                    .request_to_node_timeout(peer, &payload, CHUNK_HANDOFF_TIMEOUT)
                    .await
                {
                    Ok(constellation_net::Payload::ChunkHandoffReply {
                        uploaded: true, ..
                    }) => {}
                    other => {
                        tracing::info!(peer, ino, reply = ?other.map(|_| ()), "chunk handoff refused or failed");
                        continue 'peer;
                    }
                }
            }
            // Every chunk is in S3 now: this node's claims on them are
            // satisfied, whichever inode made them.
            crate::store_io::store_call(|| {
                for hash in &hashes {
                    for i in self.meta.pending_inos_for_hash(hash).unwrap_or_default() {
                        let _ = self.meta.ack_upload(hash, i);
                    }
                }
            })
            .await;
            for hash in &hashes {
                self.upload.existence.insert(hash);
                self.upload.note_up(hash);
                if !self.meta.upload_pending_for_hash(hash).unwrap_or(true) {
                    self.cache
                        .set_state(hash, constellation_fs_core::cache::ChunkState::Clean);
                }
            }
            self.upload.handoff.ok.fetch_add(1, Relaxed);
            self.upload
                .handoff
                .chunks
                .fetch_add(hashes.len() as u64, Relaxed);
            tracing::info!(
                peer,
                ino,
                chunks = hashes.len(),
                "a peer uploaded this node's chunks"
            );
            return true;
        }
        false
    }

    /// This node's upload of chunks a `back` close forwarded as pending
    /// (4798008: the sequencer holds back everything naming them until
    /// they are up) is making no progress: hand them to the nodes that
    /// await them, so the rest of the cluster sees the closes this node
    /// made while its own S3 path is down. One at a time.
    async fn hand_off_forwarded(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.upload.handoff.round_busy.swap(true, Relaxed) {
            return;
        }
        let forwarded = self.upload.forwarded_pending();
        if !forwarded.is_empty() {
            let mut first: Vec<u64> = forwarded.iter().flat_map(|(_, n)| n.clone()).collect();
            first.sort_unstable();
            first.dedup();
            let hashes = forwarded.into_iter().map(|(h, _)| h).collect();
            tracing::info!(
                awaited_by = ?first,
                "no upload has completed on this node for a while: handing chunks a back close forwarded to a peer"
            );
            self.hand_off_hashes(hashes, &first, 0).await;
        }
        self.upload.handoff.round_busy.store(false, Relaxed);
    }

    /// A peer that cannot reach S3 handed us `hashes`: fetch each from it,
    /// upload it, and keep a clean copy (a reader here, or one asking us,
    /// is served without S3). `true` once every one is in S3.
    async fn accept(&self, requester: u64, hashes: Vec<constellation_fs_core::ChunkHash>) -> bool {
        use futures::StreamExt;
        let Some(coop) = self.upload.coop.clone() else {
            return false;
        };
        let uploaded = hashes.clone();
        let results: Vec<Result<()>> = futures::stream::iter(hashes.into_iter().map(|hash| {
            let coop = coop.clone();
            async move {
                let data = coop.fetch_handed_off(requester, &hash).await?;
                let mode = self.upload.put_mode(&hash, data.len());
                self.store
                    .put_chunk_mode(&hash, &data, self.compression, mode)
                    .await?;
                self.upload.existence.insert(&hash);
                if !self.cache.contains(&hash) {
                    let _ = self.cache.insert(
                        &hash,
                        &data,
                        constellation_fs_core::cache::ChunkState::Clean,
                    );
                }
                self.upload
                    .handoff
                    .accepted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }))
        .buffer_unordered(4)
        .collect()
        .await;
        let failed = results.iter().filter(|r| r.is_err()).count();
        if failed > 0 {
            tracing::warn!(
                requester,
                failed,
                error = ?results.into_iter().find_map(|r| r.err()).map(|e| format!("{e:#}")),
                "could not upload a peer's handed-off chunks"
            );
            return false;
        }
        // Chunks the requester forwarded here as pending (a `back` close)
        // are up now: ack the rows that hold its manifests back, as its
        // own report would.
        let (reply, done) = tokio::sync::oneshot::channel();
        if self
            .sync_tx
            .send(SyncRequest::ChunksDurable {
                from: requester,
                hashes: uploaded,
                reply,
            })
            .is_ok()
        {
            let _ = done.await;
        }
        true
    }
}

/// Rebuild the namespace from the shared log through a side replica
/// bootstrapped from the head commit, and swap it in (`Action::
/// RebuildReplica`): a deposition recovery with uncaptured journal rows
/// (holder capture off), or a replica the log was pruned past (`Core::
/// after_gap_check`). Node-local state — identity, the journal, pending
/// uploads, `completed` — stays; the replicated namespace is replaced.
/// `keep_orphans` is asked right before the swap for the orphans some
/// view still has open (`Holds::open_orphans`): those records survive it
/// (`Meta::replace_ns_from_rebuilt`), so an open handle on an unlinked
/// file keeps reading through a rebuild.
pub(crate) async fn rebuild_replica(
    meta: &Meta,
    log: &LogStore,
    state_dir: &std::path::Path,
    keep_orphans: &(dyn Fn() -> std::collections::HashSet<constellation_fs_core::Ino>
          + Send
          + Sync),
) -> Result<()> {
    let view_path = state_dir.join(".replica-rebuild.db");
    let _ = std::fs::remove_dir_all(&view_path);
    crate::shipper::bootstrap(&view_path, log)
        .await
        .context("bootstrapping the shared log for a deposition rebuild")?;
    let side = Meta::open(&view_path)?;
    meta.replace_ns_from_rebuilt(&side, &keep_orphans())?;
    drop(side);
    let _ = std::fs::remove_dir_all(&view_path);
    Ok(())
}

// ---------------------------------------------------------------------
// The standalone driver: the same core, IO run inline, for tools and
// tests that need a tail, an acquisition or a ship without a daemon
// (in-daemon GC's standalone tail, the gc tests).
// ---------------------------------------------------------------------

/// The temp dir a standalone rebuild's side replica lives in, unique per
/// call in this process. The op id alone is not: every core counts its
/// ops from the same start, so two drivers in one process (the gc tool's
/// tail beside another, or two tests) reach a rebuild with the same id —
/// and one's `remove_dir_all` deleted, or its bootstrap wrote into, the
/// other's side replica, which then swapped in the wrong namespace.
fn standalone_rebuild_dir(op: OpId) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "constellation-standalone-rebuild-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        op.0
    ))
}

pub struct Standalone {
    core: Core,
    meta: Arc<Meta>,
    log: LogStore,
    leases: LeaseStore,
    publisher: Option<crate::mtree_publish::TreePublisher>,
    controls: HashMap<OpId, std::result::Result<ControlOk, String>>,
    next_control: u64,
    replies: HashMap<Rid, ClientReply>,
    queue: std::collections::VecDeque<Event>,
    pending_s3: std::collections::VecDeque<(OpId, S3Op)>,
    pending_publish: std::collections::VecDeque<(OpId, u64)>,
    /// `Action::RebuildReplica`s to run (the retention-gap tests need a
    /// real one).
    pending_rebuild: std::collections::VecDeque<OpId>,
    /// The open-orphan hold writer whose open set a rebuild preserves
    /// (tests; `None` keeps nothing).
    holds: Option<Arc<crate::holds::Holds>>,
    /// Timers due now, fired in order.
    due: std::collections::VecDeque<constellation_authority::TimerId>,
    /// When the step being absorbed took its `Now`: a timer is due when the
    /// time since then has reached its `after_ms` (the old test was
    /// `at <= now()` at the absorb).
    now_at: Instant,
}

#[allow(dead_code)] // the tools' and tests' driver; not every entry point has a caller yet
impl Standalone {
    pub fn new(
        meta: Arc<Meta>,
        store: Arc<dyn object_store::ObjectStore>,
        node_id: u64,
        lease_mode: LeaseMode,
    ) -> Self {
        Self::with_publisher(meta, store, node_id, lease_mode, None)
    }

    /// As [`Self::new`], with a tree publisher (tests of the publish and
    /// bootstrap paths).
    pub fn with_publisher(
        meta: Arc<Meta>,
        store: Arc<dyn object_store::ObjectStore>,
        node_id: u64,
        lease_mode: LeaseMode,
        publisher: Option<crate::mtree_publish::TreePublisher>,
    ) -> Self {
        let mut cfg = Config::defaults(node_id, 0);
        cfg.clock_id = constellation_platform::clock::protocol().id();
        cfg.clock_rate_budget_ppm = constellation_platform::clock::protocol().rate_budget_ppm();
        cfg.single_writer = lease_mode == LeaseMode::SingleWriter;
        cfg.p2p = false;
        cfg.inbox = false;
        cfg.publisher = publisher.is_some();
        cfg.forwarding = false;
        cfg.locks = false;
        cfg.ttl_ms = crate::lease::lease_ttl_ms();
        let mut core = Core::new(cfg);
        let mut out = Vec::new();
        let now_at = Instant::now();
        core.start(now(), &*meta, &mut out);
        let mut this = Self {
            core,
            meta,
            log: LogStore::new(store.clone()).with_partition(PARTITION),
            leases: LeaseStore::new(store, PARTITION, lease_mode),
            publisher,
            controls: HashMap::new(),
            next_control: 1 << 48,
            replies: HashMap::new(),
            queue: Default::default(),
            pending_s3: Default::default(),
            pending_publish: Default::default(),
            pending_rebuild: Default::default(),
            holds: None,
            due: Default::default(),
            now_at,
        };
        this.absorb(out);
        this
    }

    /// Ship everything and publish a commit (the holder, with a
    /// publisher), waiting for the publish itself.
    pub async fn publish(&mut self) -> Result<()> {
        self.control(Control::PublishNow).await?;
        self.settle().await
    }

    /// Ship everything and publish a commit as a snapshot needs it: the
    /// commit `(seq, root)` that now reflects this replica
    /// (`SyncRequest::Publish`'s answer in the daemon).
    pub async fn publish_commit(&mut self) -> Result<(u64, constellation_mtree::NodeHash)> {
        self.control(Control::PublishNow).await?;
        self.settle().await?;
        let epoch = self.core.ship().last_ship_epoch;
        self.publisher
            .as_mut()
            .context("this standalone driver has no tree publisher")?
            .publish_now(epoch)
            .await
    }

    /// A commit covering applied log position `applied`, with no ship
    /// first (`SyncRequest::Publish { through: Some(applied) }` in the
    /// daemon: a snapshot whose barrier already drained).
    pub async fn publish_through(
        &mut self,
        applied: u64,
    ) -> Result<(u64, constellation_mtree::NodeHash)> {
        let epoch = self.core.ship().last_ship_epoch;
        self.publisher
            .as_mut()
            .context("this standalone driver has no tree publisher")?
            .publish_through(epoch, applied)
            .await
    }

    /// What the daemon does when it starts a publisher (`node.rs`):
    /// adopt the chain head as the publish parent (tests of a follower
    /// whose parent is older than the head it follows).
    #[cfg(test)]
    pub(crate) async fn warm_up(&mut self) -> Result<()> {
        self.publisher
            .as_mut()
            .context("this standalone driver has no tree publisher")?
            .warm_up()
            .await
    }

    /// One `Action::FollowHead` of a non-holder's idle cadence, which this
    /// driver otherwise answers as failed: clear the dirty keys the chain
    /// head covers, leaving the publish parent where it was.
    #[cfg(test)]
    pub(crate) async fn follow_head(&mut self) -> Result<usize> {
        self.publisher
            .as_mut()
            .context("this standalone driver has no tree publisher")?
            .follow_head()
            .await
    }

    /// Copy the core's lease state into `view`, as the daemon's driver
    /// does after every event (tests that need the fast path's gate).
    pub fn mirror(&self, view: &crate::lease::LeaseView) {
        view.mirror(self.core.lease(), now(), self.core.config(), false);
    }

    /// Run every queued event and pending IO to completion (no timers).
    async fn settle(&mut self) -> Result<()> {
        while !self.queue.is_empty()
            || !self.pending_s3.is_empty()
            || !self.pending_publish.is_empty()
            || !self.pending_rebuild.is_empty()
        {
            self.step().await?;
        }
        Ok(())
    }

    /// Tail the log to head (no lease touched).
    pub async fn tail_to_head(&mut self) -> Result<()> {
        self.control(Control::TailToHead).await.map(|_| ())
    }

    /// The hold writer whose open orphans a rebuild keeps.
    pub fn set_holds(&mut self, holds: Arc<crate::holds::Holds>) {
        self.holds = Some(holds);
    }

    /// Ship everything, publish, release the lease and stop (a clean
    /// unmount's final flush).
    pub async fn shutdown(&mut self) -> Result<()> {
        self.control(Control::Shutdown).await.map(|_| ())
    }

    /// Plan 31 C8: forward-only / suspended (`Control::Authority`).
    pub async fn set_authority(&mut self, forward_only: bool, suspended: bool) -> Result<()> {
        self.control(Control::Authority {
            forward_only,
            suspended,
        })
        .await
        .map(|_| ())
    }

    /// Ship everything, publish, release the lease — and keep running
    /// (`Control::Flush`: a suspension's, or `leave`'s).
    pub async fn flush_release(&mut self) -> Result<()> {
        self.control(Control::Flush).await.map(|_| ())
    }

    /// The core, for tests that look at its lease state.
    pub fn core(&self) -> &Core {
        &self.core
    }

    /// Take the lease if it is free.
    pub async fn acquire(&mut self) -> Result<bool> {
        match self.control(Control::Acquire).await? {
            ControlOk::Lease { acquired, .. } => Ok(acquired),
            _ => Ok(false),
        }
    }

    /// One round: tail, ship the journal (as the holder).
    pub async fn sync(&mut self) -> Result<()> {
        self.control(Control::Barrier { ino: None })
            .await
            .map(|_| ())
    }

    /// Submit an op through the core (holder-local; no peers here).
    pub async fn submit(&mut self, rid: Rid, op: MutateOp) -> Result<ClientReply> {
        self.queue.push_back(Event::Submit {
            rid,
            op,
            policy: Policy::Client,
            tag: constellation_meta::locks::LockTag::NONE,
        });
        self.run_until(|s| s.replies.contains_key(&rid)).await?;
        Ok(self.replies.remove(&rid).expect("answered"))
    }

    async fn control(&mut self, req: Control) -> Result<ControlOk> {
        let op = OpId(self.next_control);
        self.next_control += 1;
        self.queue.push_back(Event::Control { op, req });
        self.run_until(|s| s.controls.contains_key(&op)).await?;
        self.controls
            .remove(&op)
            .expect("answered")
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Drive events, inline S3 and due timers until `done`.
    async fn run_until(&mut self, done: impl Fn(&Self) -> bool) -> Result<()> {
        let mut steps = 0u64;
        while !done(self) {
            steps += 1;
            anyhow::ensure!(
                steps < 100_000,
                "standalone authority driver did not settle"
            );
            if !self.step().await? {
                if let Some(id) = self.due.pop_front() {
                    self.queue.push_back(Event::Timer { id });
                } else {
                    anyhow::bail!("standalone authority driver stalled waiting for a timer");
                }
            }
        }
        Ok(())
    }

    /// One event, S3 request or publish; `false` when nothing is pending.
    async fn step(&mut self) -> Result<bool> {
        if let Some(event) = self.queue.pop_front() {
            self.now_at = Instant::now();
            let out = self.core.handle(now(), event, &*self.meta);
            self.absorb(out);
        } else if let Some((op, req)) = self.pending_s3.pop_front() {
            let result = self.run_s3(req).await;
            self.queue.push_back(Event::S3 { op, result });
        } else if let Some(op) = self.pending_rebuild.pop_front() {
            // A real rebuild (the retention-gap tests need one): the side
            // replica lives in a private temp dir.
            let dir = standalone_rebuild_dir(op);
            let _ = std::fs::create_dir_all(&dir);
            let holds = self.holds.clone();
            let keep = || {
                holds
                    .as_ref()
                    .and_then(|h| h.open_orphans().ok())
                    .unwrap_or_default()
            };
            let ok = match rebuild_replica(&self.meta, &self.log, &dir, &keep).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), "standalone rebuild failed");
                    false
                }
            };
            let _ = std::fs::remove_dir_all(&dir);
            self.queue.push_back(Event::RebuildDone { op, ok });
        } else if let Some((op, epoch)) = self.pending_publish.pop_front() {
            let ok = match self.publisher.as_mut() {
                Some(p) => match p.publish(epoch).await {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::warn!(error = %e, "standalone publish failed");
                        false
                    }
                },
                None => false,
            };
            self.queue.push_back(Event::PublishDone { op, ok });
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    async fn run_s3(&self, req: S3Op) -> S3Result {
        match req {
            S3Op::LeaseGet => S3Result::LeaseGet(self.leases.get().await.map_err(s3_failure)),
            S3Op::LeaseCreate { lease } => {
                S3Result::LeasePut(self.leases.try_create(&lease).await.map_err(cas_failure))
            }
            S3Op::LeaseSwap { lease, tag } => S3Result::LeasePut(
                self.leases
                    .try_swap(&lease, &tag)
                    .await
                    .map_err(cas_failure),
            ),
            S3Op::SegmentPut { seq, payload } => S3Result::SegmentPut(
                self.log
                    .put_segment(seq, &payload)
                    .await
                    .map_err(cas_failure),
            ),
            S3Op::SegmentRun { from, width } => {
                S3Result::SegmentRun(self.log.get_run(from, width).await.map_err(s3_failure))
            }
            S3Op::SegmentGap { from } => {
                S3Result::SegmentGap(self.log.first_segment_from(from).await.map_err(s3_failure))
            }
            S3Op::InboxPut { .. } => S3Result::InboxPut(Err(CasFailure::Failed("no inbox".into()))),
            S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
            S3Op::InboxDrain { .. } => S3Result::InboxDrain(Ok(Vec::new())),
            S3Op::InboxDelete { .. } => S3Result::InboxDelete(Ok(())),
            S3Op::InboxTombstone { .. } => S3Result::InboxTombstone(Ok(())),
            S3Op::InboxLastN { .. } => S3Result::InboxLastN(Ok(None)),
            S3Op::HeartbeatRead => S3Result::Heartbeats(Ok(Vec::new())),
            S3Op::HeartbeatPut { .. } => S3Result::HeartbeatPut(Ok(())),
        }
    }

    fn absorb(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Reply { rid, reply } => {
                    self.replies.insert(rid, reply);
                }
                Action::ControlDone { op, result } => {
                    self.controls.insert(op, result);
                }
                Action::SetTimer { id, after_ms, .. } => {
                    if u128::from(after_ms) <= self.now_at.elapsed().as_millis() {
                        self.due.push_back(id);
                    }
                }
                Action::UploadDirtyChunks { op, .. } => self.queue.push_back(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                }),
                Action::Publish { op, epoch } => self.pending_publish.push_back((op, epoch)),
                Action::FollowHead { op } => {
                    self.queue.push_back(Event::PublishDone { op, ok: false })
                }
                Action::S3 { op, req } => self.pending_s3.push_back((op, req)),
                Action::Send { to, msg } => {
                    if let Some(req) = msg.requests() {
                        self.queue.push_back(Event::PeerFailed {
                            req,
                            to,
                            outage: true,
                        });
                    }
                }
                Action::ConflictCopy { queue_seq, .. } => {
                    self.queue.push_back(Event::ConflictCopyDone {
                        queue_seq,
                        ok: false,
                    })
                }
                Action::RebuildReplica { op } => self.pending_rebuild.push_back(op),
                // No FUSE views here: nothing unflushed to flush.
                Action::LockFlush { ino, grant } => self.queue.push_back(Event::LockFlushed {
                    ino,
                    grant,
                    ok: true,
                }),
                Action::PersistLockHorizon { until } => {
                    self.queue.push_back(Event::LockHorizonPersisted {
                        until,
                        durable: Some(until),
                    })
                }
                Action::CancelTimer { .. }
                | Action::UploadAwaited { .. }
                | Action::Announce { .. }
                | Action::RoundDone { .. }
                | Action::RefreshRoster
                | Action::EpochClose
                | Action::EpochFlushed => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_u64_zero_ok;

    /// Finding 14: a lone node's authority core waited 18 s for the
    /// metadata store's writer lock (a FUSE commit's journal write held up
    /// by the overloaded host), asked S3 nothing meanwhile, and the own-S3
    /// watch reported "this node's S3 path is stalled" the moment it ran
    /// again. Silence with nothing asked is no S3 stall; a request left
    /// waiting through a window's worth of ticks is. A stall of the daemon
    /// itself is one (late) tick however long it lasted; a runtime whose
    /// ticks are all late still reports a real stall, later.
    #[test]
    fn the_own_s3_watch_needs_a_request_left_waiting() {
        use super::{own_s3_late_ticks, OwnS3Watch};
        use std::time::{Duration, Instant};
        let t0_ms = super::wait_now_ms();
        let t0 = Instant::now();
        let at = |s: f64| (t0 + Duration::from_secs_f64(s), t0_ms + (s * 1000.0) as i64);
        let upload = std::sync::Arc::new(crate::upload::UploadRuntime::for_test(false));
        let watch = || OwnS3Watch::new(upload.clone(), constellation_net::Peers::disabled());
        let w = super::s3_stall_ms() as f64 / 1000.0;
        let tick = |watch: &mut OwnS3Watch, s: f64| {
            let (now, now_ms) = at(s);
            watch.tick_at(now, now_ms).0
        };

        // S3 last answered 20 s ago and nothing is asked: never stalled.
        let _path = crate::backend::s3_path_for_test(t0_ms - 20_000);
        let mut wt = watch();
        for s in 0..=10 {
            assert!(!tick(&mut wt, s as f64), "nothing asked, {s} s");
        }

        // A request goes out at t0 and S3 never answers it: stalled once
        // it has waited the window through that many ticks, not before.
        crate::backend::s3_reset_for_test(t0_ms - 20_000);
        let mut wt = watch();
        let req = crate::backend::s3_request_for_test(t0_ms);
        for s in 0..w as u64 {
            assert!(!tick(&mut wt, s as f64), "{s} s");
        }
        assert!(tick(&mut wt, w), "unanswered for {w} s");
        // Seen once, it lasts through a late tick, until S3 answers.
        assert!(tick(&mut wt, w + 30.0));
        drop(req);
        assert!(!tick(&mut wt, w + 31.0), "answered");

        // The daemon stalls 18 s right after a request went out; its
        // answer is read only once the daemon runs again. The stall is
        // one late tick: not S3's unless still unanswered a window later.
        crate::backend::s3_reset_for_test(t0_ms - 20_000);
        let late_before = own_s3_late_ticks();
        let mut wt = watch();
        assert!(!tick(&mut wt, 0.0));
        let req = crate::backend::s3_request_for_test(t0_ms + 500);
        assert!(!tick(&mut wt, 18.0), "the daemon's own stall");
        assert!(own_s3_late_ticks() > late_before);
        for s in 1..w as u64 {
            assert!(!tick(&mut wt, 18.0 + s as f64), "{s} s after the stall");
        }
        assert!(tick(&mut wt, 18.0 + w), "unanswered {w} s after it");
        drop(req);

        // Every tick late (3.5 s apart), a request unanswered from t0:
        // still reported, after a window's worth of ticks.
        crate::backend::s3_reset_for_test(t0_ms - 20_000);
        let mut wt = watch();
        let _req = crate::backend::s3_request_for_test(t0_ms);
        let n = w as u64;
        for k in 0..n {
            assert!(!tick(&mut wt, k as f64 * 3.5), "late tick {k}");
        }
        assert!(tick(&mut wt, n as f64 * 3.5), "after {n} late ticks");
    }

    /// Under a fresh black hole the drain's first upload goes out a
    /// little after the drain started; the drain's first check (`after`
    /// in) finds it waiting 100 ms short of the window and looks again
    /// 100 ms later, not a whole `after` later (handoff at ~6 s, not ~12 s).
    /// The check is asked at `now`: it read the clock itself, and a test
    /// thread that got to it 100 ms late (waiting for another S3-path
    /// test's lock, or for a CPU on a loaded host) found the PUT already
    /// past the window.
    #[test]
    fn a_drain_rechecks_when_its_first_upload_has_waited_the_window() {
        use std::time::Duration;
        let after = Duration::from_millis(6_000);
        let now = super::wait_now_ms();
        let _path = crate::backend::s3_path_for_test(now - 60_000);
        let upload = crate::upload::UploadRuntime::for_test(false);
        // Nothing asked: the full `after`.
        assert_eq!(super::handoff_recheck(&upload, after, now), Some(after));
        // The drain started 6 s ago, its first PUT 100 ms after that.
        let put = crate::backend::s3_request_for_test(now - 5_900);
        assert_eq!(
            super::handoff_recheck(&upload, after, now),
            Some(Duration::from_millis(100)),
            "not looking again when the PUT has waited the window"
        );
        drop(put);
        // A PUT unanswered for the whole window: hand off now.
        crate::backend::s3_reset_for_test(now - 60_000);
        let _put = crate::backend::s3_request_for_test(now - 6_100);
        assert_eq!(super::handoff_recheck(&upload, after, now), None);
    }

    /// The holder's dismissal bookkeeping: a backup it drops (a
    /// candidate that timed out, a committed one reconfigured out) is
    /// told so, naming the dropped candidacy, for `DISMISS_FOR_MS`; the
    /// moment the node is selected again (a new candidacy) it is listed
    /// and no longer dismissed; a new epoch or no lease forgets it all;
    /// a node of unknown candidacy (0) is never dismissed.
    #[test]
    fn the_alive_book_dismisses_dropped_candidacies_until_reselected() {
        use super::{AliveBook, DISMISS_FOR_MS};
        let mut book = AliveBook::default();
        let held = Some((4, 100_000, 100_000, 100_000, 0));
        let t = book.update(1_000, held, vec![(2, 10), (3, 11)]).unwrap();
        assert_eq!(t.listed, vec![(2, 10), (3, 11)]);
        assert!(t.dismissed.is_empty());
        // Plan 43 C7-F F-a: the holder's guard rides the targets, and a
        // change of it republishes them (the driver compares the targets).
        assert_eq!(t.guard_wait, 0);
        let g = book
            .update(
                1_050,
                Some((4, 100_000, 100_000, 100_000, i64::MAX)),
                vec![(2, 10), (3, 11)],
            )
            .unwrap();
        assert_eq!(g.guard_wait, i64::MAX, "unknown while a gate is pending");
        assert_ne!(g, t);
        let g2 = book
            .update(
                1_060,
                Some((4, 100_000, 100_000, 100_000, 9_000)),
                vec![(2, 10), (3, 11)],
            )
            .unwrap();
        assert_eq!(g2.guard_wait, 9_000);
        assert_ne!(g2, g);
        let t = book.update(1_070, held, vec![(2, 10), (3, 11)]).unwrap();
        assert_eq!(t.guard_wait, 0);
        // Node 3's candidacy timed out.
        let t = book.update(1_100, held, vec![(2, 10)]).unwrap();
        assert_eq!(t.dismissed, vec![(3, 11)]);
        let t = book.update(1_200, held, vec![(2, 10)]).unwrap();
        assert_eq!(t.dismissed, vec![(3, 11)], "repeated");
        // Brought up again: listed under its new candidacy, no dismissal.
        let t = book.update(1_300, held, vec![(2, 10), (3, 12)]).unwrap();
        assert_eq!(t.listed, vec![(2, 10), (3, 12)]);
        assert!(
            t.dismissed.is_empty(),
            "still dismissing a re-selected node"
        );
        // Dropped again: the new candidacy is the one dismissed, until
        // the window ends.
        let t = book.update(1_400, held, vec![(2, 10)]).unwrap();
        assert_eq!(t.dismissed, vec![(3, 12)]);
        let t = book
            .update(1_400 + DISMISS_FOR_MS, held, vec![(2, 10)])
            .unwrap();
        assert!(t.dismissed.is_empty(), "told past the window");
        // Unknown candidacy: never dismissed.
        book.update(20_000, held, vec![(2, 10), (5, 0)]);
        let t = book.update(20_100, held, vec![(2, 10)]).unwrap();
        assert!(t.dismissed.is_empty());
        // A new epoch forgets the old one's dismissals; no lease, all.
        book.update(20_200, held, Vec::new());
        let t = book
            .update(20_300, Some((5, 100_000, 100_000, 100_000, 0)), Vec::new())
            .unwrap();
        assert!(
            t.dismissed.is_empty(),
            "an old epoch's dismissal carried over"
        );
        assert_eq!(book.update(20_400, None, Vec::new()), None);
    }

    /// The heartbeat task's tick: nothing without a lease or past its
    /// expiry; no beats once the driver loop has made no progress for
    /// longer than the stall (whatever it is stuck in), beats again once
    /// it progresses; idle (waiting for work, `busy_since` 0) is alive.
    #[test]
    fn the_holder_heartbeat_stops_with_the_lease_and_a_stuck_driver() {
        use super::{alive_tick, AliveTargets, AliveTick};
        use constellation_authority::HonourMs;
        let t = AliveTargets {
            epoch: 4,
            listed: vec![(2, 10)],
            dismissed: vec![(3, 11)],
            until_unix_ms: 50_000,
            until_honour: 50_000,
            until_wait: 50_000,
            guard_wait: 0,
        };
        assert_eq!(
            alive_tick(None, (1_000, HonourMs(1_000)), (0, 0, 0), 15_000),
            AliveTick::Idle
        );
        assert_eq!(
            alive_tick(Some(&t), (50_000, HonourMs(50_000)), (0, 0, 0), 15_000),
            AliveTick::Idle
        );
        let beats = AliveTick::Beat(vec![(2, 10, true), (3, 11, false)]);
        assert_eq!(
            alive_tick(Some(&t), (20_000, HonourMs(20_000)), (0, 0, 0), 15_000),
            beats
        );
        assert_eq!(
            alive_tick(Some(&t), (20_000, HonourMs(20_000)), (10_000, 0, 0), 15_000),
            beats
        );
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (10_000, 0, 0), 15_000),
            AliveTick::Hung(20_000)
        );
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (29_000, 0, 0), 15_000),
            beats
        );
    }

    /// The lag probe's view: a wake-up at least the threshold (an eighth
    /// of the takeover window) late is a lapse, from when it was due to
    /// when it woke; a probe that is itself that overdue is a lapse in
    /// progress (the same start, growing). A runtime that has run nothing
    /// for the stall bound stops the heartbeat, like a stuck driver,
    /// though the driver is parked waiting for work.
    #[test]
    fn the_lag_probe_reports_lapses_and_a_runtime_that_runs_nothing() {
        use super::{alive_tick, AliveTargets, AliveTick, OffCore};
        use constellation_authority::HonourMs;
        let off = OffCore::new(5_000);
        assert_eq!(off.lapse(1_000), (0, 0));
        assert_eq!(off.runtime_stalled_ms(1_000), 0, "not started");
        off.note_probe(10_000, 20);
        assert_eq!(off.lapse(10_010), (0, 0), "20 ms late is no lapse");
        off.note_probe(10_500, 450);
        assert_eq!(off.lapse(10_510), (10_050, 10_500));
        assert_eq!(
            off.lapse(10_500 + 50 + 187),
            (10_550, 10_737),
            "in progress"
        );
        assert_eq!(off.runtime_stalled_ms(10_500 + 50 + 1_000), 1_000);
        let t = AliveTargets {
            epoch: 1,
            listed: vec![(2, 1)],
            dismissed: Vec::new(),
            until_unix_ms: i64::MAX,
            until_honour: i64::MAX,
            until_wait: i64::MAX,
            guard_wait: 0,
        };
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (0, 15_001, 0), 15_000),
            AliveTick::Hung(15_001)
        );
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (0, 14_000, 0), 15_000),
            AliveTick::Beat(vec![(2, 1, true)])
        );
        // An echo the driver loop has not taken for the stall bound.
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (0, 0, 14_000), 15_000),
            AliveTick::Hung(16_000)
        );
        assert_eq!(
            alive_tick(Some(&t), (30_000, HonourMs(30_000)), (0, 0, 20_000), 15_000),
            AliveTick::Beat(vec![(2, 1, true)])
        );
    }

    /// The heartbeat lives as long as `Driver::run`: dropping its guard
    /// (run returned, or its task died) forgets the targets, tells the
    /// beat thread to stop and stops the lag probe.
    #[tokio::test]
    async fn the_heartbeat_task_stops_with_the_driver_loop() {
        use super::{AliveTargets, AliveTask};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        let alive = Arc::new(Mutex::new(Some(AliveTargets {
            epoch: 1,
            listed: Vec::new(),
            dismissed: Vec::new(),
            until_unix_ms: i64::MAX,
            until_honour: i64::MAX,
            until_wait: i64::MAX,
            guard_wait: 0,
        })));
        let probe = tokio::spawn(std::future::pending::<()>());
        let abort = probe.abort_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let off_core = Arc::new(super::OffCore::new(1_000));
        off_core.note_probe(1, 0);
        drop(AliveTask {
            stop: stop.clone(),
            off_core: off_core.clone(),
            probe,
            alive: alive.clone(),
        });
        tokio::task::yield_now().await;
        assert!(abort.is_finished(), "the probe still runs");
        assert_eq!(
            off_core.runtime_stalled(),
            std::time::Duration::ZERO,
            "a stopped probe reads as a stalled runtime"
        );
        assert!(stop.load(Ordering::Relaxed), "the beat thread was not told");
        assert!(alive.lock().unwrap().is_none(), "targets kept");
    }

    /// A backup's seal watch is handed the holder's latest arrival, not
    /// what its core got to: the peer service notes each heartbeat and
    /// append as it arrives, and the driver hands the core what is newer
    /// than it handed before (once per arrival; a newer epoch replaces an
    /// older one, an older epoch's late arrival is ignored).
    #[test]
    fn the_seal_watch_is_handed_the_holders_latest_arrival() {
        use super::{fresh_heard, OffCore};
        let off = OffCore::new(5_000);
        let mut handed = std::collections::HashMap::new();
        assert!(fresh_heard(off.heard(), &mut handed).is_empty());
        off.note_holder_heard(1, 4, 1_000, 5_000);
        off.note_holder_heard(1, 4, 1_300, 0);
        off.note_holder_heard(1, 4, 1_200, 4_000);
        off.note_holder_heard(2, 7, 900, 0);
        // The latest arrival, and the latest expiry any beat named.
        assert_eq!(
            fresh_heard(off.heard(), &mut handed),
            vec![(1, 4, 1_300, 5_000), (2, 7, 900, 0)]
        );
        assert!(
            fresh_heard(off.heard(), &mut handed).is_empty(),
            "handed twice"
        );
        off.note_holder_heard(1, 4, 1_600, 0);
        off.note_holder_heard(1, 3, 9_000, 9_000);
        assert_eq!(
            fresh_heard(off.heard(), &mut handed),
            vec![(1, 4, 1_600, 5_000)]
        );
        // A renewal named at an arrival already handed is fresh too: the
        // core's idea of the holder's expiry follows the stamps.
        off.note_holder_heard(1, 4, 1_600, 6_000);
        assert_eq!(
            fresh_heard(off.heard(), &mut handed),
            vec![(1, 4, 1_600, 6_000)]
        );
        // A newer epoch replaces the older one's arrival and expiry.
        off.note_holder_heard(1, 5, 1_500, 0);
        assert_eq!(
            fresh_heard(off.heard(), &mut handed),
            vec![(1, 5, 1_500, 0)]
        );
    }

    /// A backup answers the holder's heartbeat with whether its own
    /// driver is progressing: waiting for work, or inside a wake-up
    /// shorter than the bound — not a hung one.
    #[test]
    fn a_backup_answers_its_heartbeat_with_its_own_drivers_progress() {
        use super::OffCore;
        let off = OffCore::new(5_000);
        assert!(off.core_responsive(100_000), "idle");
        off.set_busy(100_000);
        assert!(off.core_responsive(104_000), "a slow step");
        assert!(off.core_responsive(105_000));
        assert!(!off.core_responsive(105_001), "stuck");
        off.set_busy(0);
        assert!(off.core_responsive(200_000), "idle again");
    }

    /// The off-core delegation renewal lane sends only while the driver
    /// is stuck (at least `DELEG_LANE_STUCK_MS`, at most the hung bound),
    /// and each renewal it sends is registered on the urgent lane first
    /// (`DelegRenewSentOffCore`, the send's stamp) and carries a lane id.
    /// Not twice within a period, nothing for a generation the core
    /// stopped publishing, and nothing with the lane off.
    #[test]
    fn the_delegation_lane_registers_then_sends_only_while_the_driver_is_stuck() {
        use super::{DelegLane, OffCore, Payload, SyncRequest};
        use constellation_authority::deleg_lane::{DELEG_LANE_STUCK_MS, LANE_REQ_BIT};
        use constellation_authority::{DelegRenewCert, HonourMs};
        let off = OffCore::new(5_000);
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        *off.deleg_certs.lock().unwrap() = vec![DelegRenewCert {
            gen: 7,
            root: 1,
            round: 3,
            next_due: HonourMs(10_000),
            period_ms: 2_500,
        }];
        let hung = 15_000;
        let mut lane = DelegLane::new(true);
        // The driver progresses, or is stuck for less than the minimum,
        // or past the hung bound: nothing, and nothing registered.
        for stuck in [0, DELEG_LANE_STUCK_MS - 1, hung + 1] {
            assert!(lane.tick(&off, 3, HonourMs(12_000), stuck, hung).is_empty());
        }
        // Stuck for 2 s with the renewal due: one registration, then one
        // request with a lane id, the same id and stamp.
        let sent = lane.tick(&off, 3, HonourMs(12_000), 2_000, hung);
        let reg = urgent.try_recv().expect("not registered");
        let (req_id, stamp) = match reg {
            SyncRequest::DelegRenewSentOffCore {
                gen: 7,
                round: 3,
                to: 1,
                req_id,
                sent,
            } => (req_id, sent),
            _ => panic!("not the lane's registration"),
        };
        assert_eq!(stamp, 12_000);
        assert!(req_id & LANE_REQ_BIT != 0);
        assert!(urgent.try_recv().is_err());
        assert_eq!(sent.len(), 1);
        // The off-core message: no backup, no stream head.
        match &sent[0] {
            (
                1,
                Payload::DelegRenewOffCore {
                    from: 3,
                    req_id: id,
                    gen: 7,
                    round: 3,
                },
            ) => assert_eq!(*id, req_id),
            other => panic!("{other:?}"),
        }
        // Not again within the period; again after it, with a new id.
        assert!(lane.tick(&off, 3, HonourMs(12_300), 2_300, hung).is_empty());
        assert!(lane.tick(&off, 3, HonourMs(14_499), 4_499, hung).is_empty());
        assert!(urgent.try_recv().is_err());
        let sent = lane.tick(&off, 3, HonourMs(14_500), 4_500, hung);
        assert_eq!(sent.len(), 1);
        match urgent.try_recv().expect("not registered") {
            SyncRequest::DelegRenewSentOffCore {
                req_id: id, sent, ..
            } => {
                assert_ne!(id, req_id);
                assert_eq!(sent, 14_500);
            }
            _ => panic!("not the lane's registration"),
        }
        // The core no longer publishes the generation: nothing.
        off.deleg_certs.lock().unwrap().clear();
        assert!(lane.tick(&off, 3, HonourMs(30_000), 2_000, hung).is_empty());
        assert!(urgent.try_recv().is_err());
        // Without a driver to register with (the urgent lane closed or
        // never set) a renewal is not sent.
        *off.deleg_certs.lock().unwrap() = vec![DelegRenewCert {
            gen: 8,
            root: 1,
            round: 1,
            next_due: HonourMs(0),
            period_ms: 2_500,
        }];
        drop(urgent);
        assert!(lane.tick(&off, 3, HonourMs(30_000), 2_000, hung).is_empty());
        // Off: nothing at all.
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        let mut off_lane = DelegLane::new(false);
        assert!(off_lane
            .tick(&off, 3, HonourMs(30_000), 2_000, hung)
            .is_empty());
        assert!(urgent.try_recv().is_err());
    }

    /// The driver tells the core of a freeze of the heartbeat thread as it
    /// wakes, before the thread has run to record it: from the tick it
    /// missed, while it is in progress; and the thread's own record, one
    /// atomic word, once it has run (`Event::OwnFreeze`).
    #[test]
    fn a_freeze_of_the_heartbeat_thread_is_seen_by_the_driver_before_the_thread_runs() {
        use super::OffCore;
        let off = OffCore::new(5_000);
        // Never started: nothing.
        assert!(off.beat_freezes(1_000_000).is_empty());
        // Ticking on time (next tick due at 1_000_300): nothing.
        off.note_beat_due(1_000_300);
        assert!(off.beat_freezes(1_000_400).is_empty());
        // The process was stopped: at 1_006_000 the driver wakes and the
        // thread has not run since its tick due at 1_000_300.
        assert_eq!(off.beat_freezes(1_006_000), vec![(1_000_300, 1_006_000)]);
        // The thread runs: it records the freeze it saw end (from, length),
        // and its next tick is due.
        off.note_beat_freeze(1_000_300, 5_702);
        off.note_beat_due(1_006_302);
        assert_eq!(off.beat_freezes(1_006_400), vec![(1_000_300, 1_006_002)]);
        // A later freeze in progress comes after it.
        assert_eq!(
            off.beat_freezes(1_009_000),
            vec![(1_000_300, 1_006_002), (1_006_302, 1_009_000)]
        );
    }

    /// The root capped a grant to about a second (its own lease was about to
    /// end): the answer reaches the P2P service, off the core, and the lane
    /// renews at a quarter of what was granted, not at the period of the
    /// last full grant, which would come after the root's reclaim.
    #[test]
    fn the_delegation_lane_follows_the_grant_the_root_named() {
        use super::{DelegLane, OffCore};
        use constellation_authority::{DelegRenewCert, HonourMs};
        let off = OffCore::new(5_000);
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        // The core's last renewal went out at 7_500 (period 2_500).
        *off.deleg_certs.lock().unwrap() = vec![DelegRenewCert {
            gen: 7,
            root: 1,
            round: 3,
            next_due: HonourMs(10_000),
            period_ms: 2_500,
        }];
        let mut lane = DelegLane::new(true);
        // Stuck at 10_100: the lane renews (due), and the root's answer
        // granted 1_072 ms.
        assert_eq!(lane.tick(&off, 3, HonourMs(10_100), 1_000, 15_000).len(), 1);
        assert!(urgent.try_recv().is_ok());
        off.note_granted_ttl(1, 7, 3, 1_072);
        // The next one is due a quarter of the grant after the send
        // (268 ms), not 2_500 ms.
        assert!(lane
            .tick(&off, 3, HonourMs(10_300), 1_200, 15_000)
            .is_empty());
        assert_eq!(lane.tick(&off, 3, HonourMs(10_368), 1_268, 15_000).len(), 1);
        assert!(urgent.try_recv().is_ok());
        // A full grant afterwards puts the period back at the certificate's.
        off.note_granted_ttl(1, 7, 3, 10_000);
        assert!(lane
            .tick(&off, 3, HonourMs(10_700), 1_600, 15_000)
            .is_empty());
        assert!(lane
            .tick(&off, 3, HonourMs(12_867), 3_700, 15_000)
            .is_empty());
        assert_eq!(lane.tick(&off, 3, HonourMs(12_868), 3_700, 15_000).len(), 1);
    }

    /// A freeze of the heartbeat thread is handed to the core only if the
    /// shared runtime's lag probe missed the same interval: a thread held up
    /// alone (blocked in a log line) while the runtime ran on is no freeze of
    /// the process, and the loss it causes is not excused.
    #[test]
    fn a_heartbeat_thread_held_up_alone_is_no_freeze_of_the_process() {
        use super::OffCore;
        let off = OffCore::new(5_000);
        // The probe ticks on time up to 1_000_250 and then keeps ticking:
        // no lapse. The heartbeat thread's next tick was due at 1_000_300
        // and it ran at 1_006_000: 5.7 s late, alone.
        off.note_probe(1_000_250, 0);
        off.note_beat_due(1_000_300);
        off.note_probe(1_006_000, 0);
        assert!(off.whole_process_freezes(1_006_010).is_empty());
        off.note_beat_freeze(1_000_300, 5_700);
        off.note_beat_due(1_006_300);
        assert!(
            off.whole_process_freezes(1_006_100).is_empty(),
            "a thread held up alone was handed over as a freeze"
        );
        // The runtime missed it too (the whole process was frozen): its
        // probe woke 5.75 s late. Now it is handed over.
        let off = OffCore::new(5_000);
        off.note_beat_due(1_000_300);
        off.note_probe(1_006_000, 5_750);
        off.note_beat_freeze(1_000_300, 5_700);
        off.note_beat_due(1_006_300);
        assert_eq!(
            off.whole_process_freezes(1_006_040),
            vec![(1_000_300, 1_006_000)]
        );
        // In progress: the driver wakes first, before either has run.
        let off = OffCore::new(5_000);
        off.note_probe(1_000_250, 0);
        off.note_beat_due(1_000_300);
        assert_eq!(
            off.whole_process_freezes(1_006_000),
            vec![(1_000_300, 1_006_000)]
        );
    }

    /// The lane's request failed at the transport: the core hears of it on
    /// the urgent lane (behind the registration) and drops the pending entry.
    #[test]
    fn a_failed_lane_send_is_reported_to_the_core_on_the_urgent_lane() {
        use super::{OffCore, SyncRequest};
        let off = OffCore::new(5_000);
        // No driver yet: nothing to tell, nothing panics.
        off.lane_send_failed(1, 5);
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        off.lane_send_failed(1, (1 << 62) | 5);
        match urgent.try_recv().expect("not reported") {
            SyncRequest::DelegRenewOffCoreFailed { to, req_id } => {
                assert_eq!((to, req_id), (1, (1 << 62) | 5));
            }
            _ => panic!("not the failure report"),
        }
        assert!(urgent.try_recv().is_err());
    }

    /// The lane follows only the root's answers, and no shorter than the
    /// floor: another node's answer is ignored, and a nearly-gone grant does
    /// not make the lane renew every tick.
    #[test]
    fn the_delegation_lane_follows_only_the_roots_answers_down_to_a_floor() {
        use super::{DelegLane, OffCore};
        use constellation_authority::{DelegRenewCert, HonourMs};
        let cert = DelegRenewCert {
            gen: 7,
            root: 1,
            round: 3,
            next_due: HonourMs(10_000),
            period_ms: 2_500,
        };
        // An answer from node 2 (not the root of the certificate): ignored.
        let off = OffCore::new(5_000);
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        *off.deleg_certs.lock().unwrap() = vec![cert.clone()];
        let mut lane = DelegLane::new(true);
        assert_eq!(lane.tick(&off, 3, HonourMs(10_100), 1_000, 15_000).len(), 1);
        assert!(urgent.try_recv().is_ok());
        off.note_granted_ttl(2, 7, 3, 1_072);
        assert!(lane
            .tick(&off, 3, HonourMs(10_400), 1_300, 15_000)
            .is_empty());
        // The root's: followed (268 ms), but a 600 ms grant is not followed
        // below the 250 ms floor.
        off.note_granted_ttl(1, 7, 3, 600);
        assert!(lane
            .tick(&off, 3, HonourMs(10_349), 1_249, 15_000)
            .is_empty());
        assert_eq!(lane.tick(&off, 3, HonourMs(10_350), 1_250, 15_000).len(), 1);
    }

    /// A renewal the core decided on but the driver has not dispatched (a
    /// blocking action ahead of the send in the same step) is not a
    /// renewal that went out: the certificate counts only answered ones,
    /// the lane renews at once, and one the driver dispatched postpones the
    /// lane's by a period.
    #[test]
    fn the_delegation_lane_counts_only_renewals_that_went_out() {
        use super::{DelegLane, OffCore};
        use constellation_authority::{DelegRenewCert, HonourMs};
        let off = OffCore::new(5_000);
        let (tx, mut urgent) = tokio::sync::mpsc::unbounded_channel();
        *off.urgent.lock().unwrap() = Some(tx);
        // The last answered renewal was at 7_500: due at 10_000. The core
        // decided on the next at 10_000, stuck before sending it.
        *off.deleg_certs.lock().unwrap() = vec![DelegRenewCert {
            gen: 7,
            root: 1,
            round: 3,
            next_due: HonourMs(10_000),
            period_ms: 2_500,
        }];
        let mut lane = DelegLane::new(true);
        assert_eq!(lane.tick(&off, 3, HonourMs(10_700), 700, 15_000).len(), 1);
        assert!(urgent.try_recv().is_ok());
        // Fresh lane (a new heartbeat thread, or the lane's book lost):
        // the core's renewal is dispatched at 10_900, the lane waits a
        // period from it, not from the decision.
        let mut lane = DelegLane::new(true);
        off.note_renewal_dispatched(7, 3, 10_900);
        assert!(lane
            .tick(&off, 3, HonourMs(13_399), 3_399, 15_000)
            .is_empty());
        assert_eq!(lane.tick(&off, 3, HonourMs(13_400), 3_400, 15_000).len(), 1);
        assert!(urgent.try_recv().is_ok());
    }

    /// The heartbeat goes out every interval however long earlier beats
    /// wait for their answers, up to as many as fit in one beat's
    /// timeout per peer; an answer frees a slot.
    #[test]
    fn beats_go_out_every_interval_up_to_a_cap_per_peer() {
        use super::{beat_done, beat_slot, beats_outstanding_max};
        use std::time::Duration;
        assert_eq!(beats_outstanding_max(Duration::from_millis(300)), 17);
        assert_eq!(beats_outstanding_max(Duration::from_secs(60)), 1);
        assert_eq!(beats_outstanding_max(Duration::ZERO), 64);
        let mut pending = std::collections::HashMap::new();
        for _ in 0..3 {
            assert!(beat_slot(&mut pending, 2, 3));
        }
        assert!(!beat_slot(&mut pending, 2, 3), "past the cap");
        assert!(beat_slot(&mut pending, 3, 3), "another peer");
        beat_done(&mut pending, 2);
        assert!(beat_slot(&mut pending, 2, 3));
        for _ in 0..3 {
            beat_done(&mut pending, 2);
        }
        assert!(!pending.contains_key(&2));
    }

    /// Only timers that measure a peer's silence (owners' grant expiries
    /// included) wait for the backlog that reached the node before they
    /// fired; renewals, ticks and this node's own deadlines never do, nor
    /// the backup's seal watch, which decides on the holder's arrival
    /// stamps. The drain handles what was queued in order, stops at the
    /// budget (the timer then goes back behind what else is due) and at a
    /// stopped core.
    #[test]
    fn only_silence_timers_wait_for_the_backlog_and_only_so_long() {
        use super::{drain_queued, measures_silence, Drained};
        use constellation_authority::TimerKind;
        use std::time::Duration;
        for kind in [
            TimerKind::ForwardTimeout,
            TimerKind::JobRequestTimeout,
            TimerKind::LockRequestTimeout,
            TimerKind::LockRenewTimeout,
            TimerKind::LockGrantExpiry,
            TimerKind::DelegExpiry,
            TimerKind::StreamWatchdog,
        ] {
            assert!(measures_silence(kind), "{kind:?}");
        }
        for kind in [
            TimerKind::BackupWatch,
            TimerKind::LockRenewTick,
            TimerKind::DelegRenew,
            TimerKind::BackupTick,
            TimerKind::Poll,
            TimerKind::ClientDeadline,
            TimerKind::StreamHeartbeat,
        ] {
            assert!(!measures_silence(kind), "{kind:?}");
        }
        // In order, only what was queued before (`pending`).
        let mut queue: std::collections::VecDeque<u32> = (1..=5).collect();
        let mut handled = Vec::new();
        let d = drain_queued(3, Duration::from_secs(5), || {
            let n = queue.pop_front()?;
            handled.push(n);
            Some(true)
        });
        assert_eq!(d, Drained::All);
        assert_eq!(handled, vec![1, 2, 3]);
        // An empty queue ends it early.
        let d = drain_queued(10, Duration::from_secs(5), || {
            queue.pop_front().map(|_| true)
        });
        assert_eq!(d, Drained::All);
        // Slow steps: the budget ends it.
        let mut queue: std::collections::VecDeque<u32> = (1..=5).collect();
        let d = drain_queued(5, Duration::from_millis(30), || {
            queue.pop_front()?;
            std::thread::sleep(Duration::from_millis(20));
            Some(true)
        });
        assert_eq!(d, Drained::OutOfTime);
        // One or two (a loaded host oversleeps), never all five.
        assert!((3..=4).contains(&queue.len()), "{queue:?}");
        // A stopped core.
        assert_eq!(
            drain_queued(5, Duration::from_secs(5), || Some(false)),
            Drained::Stopped
        );
    }

    /// Only an owner's expiries of grants it handed out wait for the
    /// renewals queued on the urgent lane; every other timer is handled
    /// in turn.
    #[test]
    fn only_grant_expiries_wait_for_queued_renewals() {
        use super::waits_for_renewals;
        use constellation_authority::TimerKind;
        for kind in [TimerKind::LockGrantExpiry, TimerKind::DelegExpiry] {
            assert!(waits_for_renewals(kind), "{kind:?}");
        }
        for kind in [
            TimerKind::BackupWatch,
            TimerKind::LockRenewTick,
            TimerKind::LockRenewTimeout,
            TimerKind::DelegRenew,
            TimerKind::ForwardTimeout,
            TimerKind::BackupTick,
        ] {
            assert!(!waits_for_renewals(kind), "{kind:?}");
        }
    }

    /// Only the timers that keep a grant this node holds skip the
    /// internal channel's backlog: a delegate's renewal and lapse, a lock
    /// holder's renewal tick. The other timers, the stream tick, the
    /// renewal timeouts and the owner's expiry of a grant included, keep
    /// their place in it.
    #[test]
    fn only_a_holders_grant_timers_are_prompt() {
        use super::prompt_timer;
        use constellation_authority::TimerKind;
        for kind in [
            TimerKind::DelegRenew,
            TimerKind::DelegLapse,
            TimerKind::LockRenewTick,
        ] {
            assert!(prompt_timer(kind), "{kind:?}");
        }
        for kind in [
            TimerKind::DelegExpiry,
            TimerKind::DelegStream,
            TimerKind::LockGrantExpiry,
            TimerKind::LockRenewTimeout,
            TimerKind::LockRequestTimeout,
            TimerKind::BackupWatch,
            TimerKind::BackupTick,
            TimerKind::ForwardTimeout,
            TimerKind::Poll,
        ] {
            assert!(!prompt_timer(kind), "{kind:?}");
        }
    }

    /// The answer to this node's lock renewal, or its failure, goes on
    /// the urgent lane, not behind the internal channel; it reaches the
    /// core as the owner's `LockRenewed` or as `PeerFailed`. Without a
    /// peer service it goes on the internal channel.
    #[test]
    fn lock_renewal_answers_take_the_urgent_lane() {
        use super::{lock_renew_answer, Internal, SyncRequest};
        use constellation_authority::{Event, LockRenewResult, OpId, PeerMsg};
        use constellation_meta::locks::GrantId;
        let (urgent_tx, mut urgent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (int_tx, mut int_rx) = tokio::sync::mpsc::unbounded_channel();
        let results = vec![(7, GrantId::default(), LockRenewResult::Lost)];
        lock_renew_answer(
            Some(&urgent_tx),
            &int_tx,
            3,
            OpId(11),
            Some(results.clone()),
            false,
        );
        lock_renew_answer(Some(&urgent_tx), &int_tx, 3, OpId(12), None, true);
        assert!(int_rx.try_recv().is_err());
        let mut events = Vec::new();
        while let Ok(req) = urgent_rx.try_recv() {
            let SyncRequest::LockRenewAnswered {
                to,
                req_id,
                results,
                outage,
            } = req
            else {
                panic!("not a renewal answer");
            };
            events.push(super::lock_renew_event(to, OpId(req_id), results, outage));
        }
        let [Event::Peer {
            from: 3,
            msg:
                PeerMsg::LockRenewed {
                    req: OpId(11),
                    results: got,
                },
        }, Event::PeerFailed {
            req: OpId(12),
            to: 3,
            outage: true,
        }] = events.as_slice()
        else {
            panic!("{events:?}");
        };
        assert_eq!(got, &results);
        lock_renew_answer(None, &int_tx, 4, OpId(13), None, false);
        assert!(matches!(
            int_rx.try_recv(),
            Ok(Internal::Event(Event::PeerFailed {
                req: OpId(13),
                to: 4,
                outage: false,
            }))
        ));
    }

    /// Two standalone drivers in one process whose cores reach a rebuild
    /// with the same op id get separate side replicas (before: both used
    /// `…-rebuild-<pid>-<op>`, and the shipper's two retention-gap tests,
    /// run side by side, swapped each other's namespace in).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn standalone_rebuilds_never_share_a_side_replica() {
        use super::{Meta, Standalone};
        use constellation_authority::OpId;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;
        use constellation_store_s3::LeaseMode;
        use object_store::memory::InMemory;
        use std::sync::Arc;
        use std::sync::Arc as StdArc;

        const FILES: usize = 40;
        // A log holding `FILES` files named `<tag>-<i>`.
        async fn log_of(tag: &'static str) -> StdArc<InMemory> {
            let store = StdArc::new(InMemory::new());
            let meta = Arc::new(Meta::open_in_memory().unwrap());
            meta.set_node_prefix(1).unwrap();
            let mut writer = Standalone::new(meta.clone(), store.clone(), 1, LeaseMode::Cas);
            assert!(writer.acquire().await.unwrap());
            for i in 0..FILES {
                meta.create(ROOT_INO, &format!("{tag}-{i}"), 0o644, 0, 0)
                    .unwrap();
            }
            writer.sync().await.unwrap();
            store
        }
        // A fresh replica on `store`, with a rebuild queued under `op`.
        fn rebuilder(store: &StdArc<InMemory>, op: OpId) -> (Arc<Meta>, Standalone) {
            let meta = Arc::new(Meta::open_in_memory().unwrap());
            meta.set_node_prefix(2).unwrap();
            let mut driver = Standalone::new(meta.clone(), store.clone(), 2, LeaseMode::Cas);
            driver.pending_rebuild.push_back(op);
            (meta, driver)
        }

        let alpha = log_of("alpha").await;
        let beta = log_of("beta").await;
        // The helper alone: distinct paths for one op id.
        let op = OpId(7);
        let (a, b) = (
            super::standalone_rebuild_dir(op),
            super::standalone_rebuild_dir(op),
        );
        assert_ne!(a, b);
        assert_eq!(a.parent(), Some(std::env::temp_dir().as_path()));

        // Overlapping rebuilds with the same op id, many rounds: each ends
        // with its own log's namespace and none of the other's.
        for round in 0..10 {
            let (meta_a, mut drv_a) = rebuilder(&alpha, op);
            let (meta_b, mut drv_b) = rebuilder(&beta, op);
            let gate = Arc::new(tokio::sync::Barrier::new(2));
            let (ga, gb) = (gate.clone(), gate);
            let ta = tokio::spawn(async move {
                ga.wait().await;
                drv_a.step().await.unwrap();
            });
            let tb = tokio::spawn(async move {
                gb.wait().await;
                drv_b.step().await.unwrap();
            });
            ta.await.unwrap();
            tb.await.unwrap();
            for i in 0..FILES {
                for (meta, own, other) in [(&meta_a, "alpha", "beta"), (&meta_b, "beta", "alpha")] {
                    assert!(
                        meta.lookup(ROOT_INO, &format!("{own}-{i}"))
                            .unwrap()
                            .is_some(),
                        "round {round}: the {own} replica lost {own}-{i}"
                    );
                    assert!(
                        meta.lookup(ROOT_INO, &format!("{other}-{i}"))
                            .unwrap()
                            .is_none(),
                        "round {round}: the {own} replica got {other}-{i}"
                    );
                }
            }
        }
    }

    /// The placement's share and rate knobs take `0` as a value (it turns
    /// splitting, the share floor or the rate floor off); unset or
    /// unparsable is the default.
    #[test]
    fn the_ack_policy_is_parsed_once_for_the_filesystem() {
        use super::{ack_s3_of, parse_ack_policy};
        assert_eq!(parse_ack_policy(None).unwrap(), None);
        assert_eq!(
            parse_ack_policy(Some(" S3 ")).unwrap().as_deref(),
            Some("s3")
        );
        assert_eq!(
            parse_ack_policy(Some("local")).unwrap().as_deref(),
            Some("local")
        );
        assert!(parse_ack_policy(Some("backup")).is_err());
        assert!(ack_s3_of(Some("s3")));
        assert!(!ack_s3_of(Some("local")));
        assert!(!ack_s3_of(None));
    }

    #[test]
    fn zero_is_a_value_for_zero_ok_knobs() {
        assert_eq!(parse_u64_zero_ok(Some("0"), 20), 0);
        assert_eq!(parse_u64_zero_ok(Some(" 35 "), 20), 35);
        assert_eq!(parse_u64_zero_ok(None, 20), 20);
        assert_eq!(parse_u64_zero_ok(Some("off"), 20), 20);
    }

    /// A holder (node 1, on `rt_a`, its shared runtime: one worker) and a
    /// backup (node 2, on a runtime of its own) over real P2P, the backup
    /// recording each beat's arrival and number; the pooled connection
    /// dialed by the holder's runtime, as its appends dial it.
    struct BeatRig {
        rt_a: tokio::runtime::Runtime,
        rt_b: tokio::runtime::Runtime,
        peers_a: constellation_net::Peers,
        peers_b: constellation_net::Peers,
        backup: std::sync::Arc<RigBackup>,
        _dir: tempfile::TempDir,
    }

    #[derive(Default)]
    struct RigBackup {
        beats: std::sync::Mutex<Vec<(std::time::Instant, u64)>>,
    }

    impl constellation_net::PeerService for RigBackup {
        fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
        fn lease_requested(
            &self,
            _part: String,
            _requester: u64,
            _epoch_applied: Option<u64>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = constellation_net::Payload> + Send + '_>,
        > {
            Box::pin(async move { constellation_net::Payload::Ping { node_id: 2 } })
        }
        fn holder_alive(
            &self,
            _holder: u64,
            _epoch: u64,
            _candidacy: u64,
            _listed: bool,
            beat: constellation_net::Beat,
        ) -> bool {
            self.beats
                .lock()
                .unwrap()
                .push((std::time::Instant::now(), beat.seq));
            true
        }
        fn node_id(&self) -> u64 {
            2
        }
    }

    impl BeatRig {
        fn new(tag: &str) -> Self {
            use constellation_net::{Payload, Peers};
            use std::time::Duration;
            let dir = tempfile::tempdir().unwrap();
            let topic = constellation_net::topic_for(Some(&[41u8; 32]), tag);
            let (key_a, _) = constellation_net::load_or_create(&dir.path().join("a.key")).unwrap();
            let (key_b, _) = constellation_net::load_or_create(&dir.path().join("b.key")).unwrap();
            let rt_b = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let rt_a = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let pa = rt_a
                .block_on(constellation_net::P2p::spawn(key_a, topic))
                .unwrap();
            let pb = rt_b
                .block_on(constellation_net::P2p::spawn(key_b, topic))
                .unwrap();
            let registry = vec![
                (
                    1u64,
                    pa.pubkey_hex(),
                    serde_json::to_value(pa.addr()).unwrap(),
                ),
                (
                    2u64,
                    pb.pubkey_hex(),
                    serde_json::to_value(pb.addr()).unwrap(),
                ),
            ];
            let peers_a = rt_a.block_on(async { Peers::new(pa, 1) });
            let peers_b = rt_b.block_on(async { Peers::new(pb, 2) });
            peers_a.refresh_registry(registry.clone());
            peers_b.refresh_registry(registry);
            let backup = std::sync::Arc::new(RigBackup::default());
            {
                let (peers_b, backup) = (peers_b.clone(), backup.clone());
                rt_b.spawn(async move { peers_b.serve(backup).await });
            }
            let ping = Payload::Ping { node_id: 1 };
            rt_a.block_on(peers_a.request_to_node_timeout(2, &ping, Duration::from_secs(10)))
                .unwrap();
            Self {
                rt_a,
                rt_b,
                peers_a,
                peers_b,
                backup,
                _dir: dir,
            }
        }

        /// The holder's heartbeat (beating node 2 every 300 ms, stall
        /// bound `stall`), the driver's prompt lane's receiver, and the
        /// guard.
        fn heartbeat(
            &self,
            off_core: std::sync::Arc<super::OffCore>,
            stall: i64,
        ) -> (
            tokio::sync::mpsc::UnboundedReceiver<super::Internal>,
            super::AliveTask,
        ) {
            use std::sync::{Arc, Mutex};
            let alive = Arc::new(Mutex::new(Some(super::AliveTargets {
                epoch: 1,
                listed: vec![(2, 1)],
                dismissed: Vec::new(),
                until_unix_ms: i64::MAX,
                until_honour: i64::MAX,
                until_wait: i64::MAX,
                guard_wait: 0,
            })));
            let (core_tx, _core_rx) = tokio::sync::mpsc::unbounded_channel();
            let (prompt_tx, prompt_rx) = tokio::sync::mpsc::unbounded_channel();
            let _in = self.rt_a.enter();
            let task = super::AliveTask::start(
                self.peers_a.clone(),
                alive,
                off_core,
                (std::time::Duration::from_millis(300), stall),
                1,
                (core_tx, prompt_tx),
                Arc::new(Mutex::new(
                    constellation_platform::process::ThreadRef::unknown(),
                )),
            );
            (prompt_rx, task)
        }

        /// The driver loop on `rt_a`, taking each echo after `delay`.
        fn driver(
            &self,
            off_core: std::sync::Arc<super::OffCore>,
            mut prompt_rx: tokio::sync::mpsc::UnboundedReceiver<super::Internal>,
            delay: std::time::Duration,
        ) -> tokio::task::JoinHandle<()> {
            self.rt_a.spawn(async move {
                while let Some(m) = prompt_rx.recv().await {
                    if matches!(m, super::Internal::Echo) {
                        tokio::time::sleep(delay).await;
                        off_core
                            .echo_sent
                            .store(0, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        }

        /// Block the holder's runtime (its one worker) for `ms`; returns
        /// when the block began.
        fn block(&self, ms: u64) -> std::time::Instant {
            let started = std::sync::Arc::new(std::sync::Barrier::new(2));
            let s = started.clone();
            self.rt_a.spawn(async move {
                s.wait();
                std::thread::sleep(std::time::Duration::from_millis(ms));
            });
            started.wait();
            std::time::Instant::now()
        }

        /// The longest gap between beats heard within `around` (its
        /// edges included).
        fn max_gap(&self, around: (std::time::Instant, std::time::Instant)) -> std::time::Duration {
            let heard: Vec<std::time::Instant> = self
                .backup
                .beats
                .lock()
                .unwrap()
                .iter()
                .map(|(t, _)| *t)
                .filter(|t| *t >= around.0 && *t <= around.1)
                .collect();
            let edges = [around.0]
                .into_iter()
                .chain(heard)
                .chain([around.1])
                .collect::<Vec<_>>();
            edges.windows(2).map(|w| w[1] - w[0]).max().unwrap()
        }
    }

    /// `snapsched-write-overhead`: a backup sealed a live holder
    /// `silent_ms=1501` in, the holder having logged nothing for 2.5 s —
    /// its heartbeat was a task of the daemon's shared runtime, over a
    /// pooled connection that runtime drove, and the runtime was held
    /// up. The heartbeat now runs on a thread of its own and sends on the
    /// liveness lane: with the holder's runtime blocked for 2.5 s (its one
    /// worker inside a blocking call), the backup still hears a beat
    /// every interval, numbered and stamped. The old shape — a task of
    /// the holder's runtime beating over the pooled connection — is
    /// silenced by the same block for all of it (the second half).
    #[test]
    fn the_holders_heartbeat_survives_a_starved_runtime() {
        use constellation_net::Payload;
        use std::time::Duration;
        let rig = BeatRig::new("starved-holder");
        let off = std::sync::Arc::new(super::OffCore::new(5_000));
        let (prompt_rx, task) = rig.heartbeat(off.clone(), 15_000);
        let _driver = rig.driver(off, prompt_rx, Duration::ZERO);
        std::thread::sleep(Duration::from_millis(1_500));
        let t0 = rig.block(2_500);
        std::thread::sleep(Duration::from_millis(3_000));
        drop(task);
        std::thread::sleep(Duration::from_millis(400));
        let thread_gap = rig.max_gap((t0, t0 + Duration::from_millis(2_500)));
        let seqs: Vec<u64> = rig
            .backup
            .beats
            .lock()
            .unwrap()
            .iter()
            .map(|(_, s)| *s)
            .collect();
        assert!(
            seqs.windows(2).all(|w| w[0] < w[1] || w[1] == 1),
            "beats out of their numbering: {seqs:?}"
        );

        // The old shape: a task of the holder's runtime, over the pool.
        rig.backup.beats.lock().unwrap().clear();
        let old = {
            let peers_a = rig.peers_a.clone();
            rig.rt_a.spawn(async move {
                let mut seq = 0;
                loop {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    seq += 1;
                    let beat = Payload::HolderAlive {
                        holder: 1,
                        epoch: 1,
                        candidacy: 1,
                        listed: true,
                        beat: constellation_net::Beat {
                            seq,
                            ..Default::default()
                        },
                    };
                    let peers_a = peers_a.clone();
                    tokio::spawn(async move {
                        let _ = peers_a
                            .request_to_node_timeout(2, &beat, Duration::from_secs(5))
                            .await;
                    });
                }
            })
        };
        std::thread::sleep(Duration::from_millis(1_500));
        let t1 = rig.block(2_500);
        std::thread::sleep(Duration::from_millis(3_000));
        old.abort();
        let task_gap = rig.max_gap((t1, t1 + Duration::from_millis(2_500)));
        eprintln!(
            "longest gap heard while the holder's runtime was blocked 2.5 s: beat thread \
             {thread_gap:?}, runtime task {task_gap:?}"
        );
        // Five intervals: the test process itself may be descheduled on
        // a loaded host; the old shape's gap is the whole block.
        assert!(
            thread_gap < Duration::from_millis(1_500),
            "the beat thread went silent for {thread_gap:?}"
        );
        assert!(
            task_gap >= Duration::from_millis(2_000),
            "the old shape kept beating ({task_gap:?}): the block did not starve the runtime"
        );
    }

    /// A driver loop that takes no message for the stall bound serves
    /// nobody, however often its runtime runs: the heartbeat stops and the
    /// backup seals it, as before the heartbeat left the runtime. Without
    /// the echo, the beat thread beat on (the runtime's lag probe ticked,
    /// the driver was parked waiting, not busy) and the holder kept its
    /// lease answering nothing. A driver that takes the echo, however
    /// slowly within the bound (a loaded one), keeps beating.
    #[test]
    fn a_holder_whose_driver_takes_nothing_stops_beating() {
        use std::time::{Duration, Instant};
        let rig = BeatRig::new("deaf-holder");
        // Loaded: each echo taken 1 s late, within a 1.5 s bound.
        let off = std::sync::Arc::new(super::OffCore::new(5_000));
        let (prompt_rx, task) = rig.heartbeat(off.clone(), 1_500);
        let driver = rig.driver(off, prompt_rx, Duration::from_millis(1_000));
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_millis(5_000));
        let loaded_gap = rig.max_gap((t0 + Duration::from_millis(500), Instant::now()));
        drop(task);
        driver.abort();
        std::thread::sleep(Duration::from_millis(400));
        rig.backup.beats.lock().unwrap().clear();
        // Deaf: the echo is never taken (the receiver is held, unread).
        let off = std::sync::Arc::new(super::OffCore::new(5_000));
        let (_prompt_rx, task) = rig.heartbeat(off, 1_500);
        let t1 = Instant::now();
        std::thread::sleep(Duration::from_millis(4_000));
        drop(task);
        let heard: Vec<Duration> = rig
            .backup
            .beats
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| t.duration_since(t1))
            .collect();
        eprintln!("loaded driver's longest gap {loaded_gap:?}; deaf driver's beats at {heard:?}");
        assert!(
            loaded_gap < Duration::from_millis(1_500),
            "a loaded driver's heartbeat went silent for {loaded_gap:?}"
        );
        let last = heard.last().copied().unwrap_or_default();
        assert!(
            last < Duration::from_millis(2_500),
            "a driver that takes nothing still beat {last:?} in (bound 1.5 s)"
        );
    }

    /// A driver loop that iterates on other lanes the whole time — its
    /// urgent lane saturated, the echo on the prompt lane never taken —
    /// is not a hung one: every wake-up answers the echo
    /// (`OffCore::loop_woke`, what `Driver::run` calls on each wake-up),
    /// and the heartbeat goes on. (Before, only taking the echo answered
    /// it, and such a loop was declared hung after the stall bound.)
    #[test]
    fn a_holder_whose_driver_iterates_on_another_lane_keeps_beating() {
        use std::time::{Duration, Instant};
        let rig = BeatRig::new("busy-urgent-holder");
        let off = std::sync::Arc::new(super::OffCore::new(5_000));
        // The prompt lane is held, unread; the loop wakes every 100 ms on
        // its urgent lane.
        let (_prompt_rx, task) = rig.heartbeat(off.clone(), 1_500);
        let iterating = {
            let off = off.clone();
            rig.rt_a.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    off.loop_woke(super::wait_now_ms());
                    off.set_busy(0);
                }
            })
        };
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_millis(4_000));
        let gap = rig.max_gap((t0 + Duration::from_millis(500), Instant::now()));
        iterating.abort();
        drop(task);
        eprintln!("longest gap of a holder whose loop iterates on its urgent lane: {gap:?}");
        assert!(
            gap < Duration::from_millis(1_500),
            "a loop that iterated was taken for hung: silent {gap:?}"
        );
    }

    /// The liveness lane recovers from a lost connection: the backup
    /// closes every connection (a suspension and resume), the next beat
    /// fails and clears the lane's slot, and the one after re-dials.
    #[test]
    fn the_liveness_lane_redials_after_its_connection_is_lost() {
        use std::time::{Duration, Instant};
        let rig = BeatRig::new("lane-redial");
        let off = std::sync::Arc::new(super::OffCore::new(5_000));
        let (prompt_rx, task) = rig.heartbeat(off.clone(), 15_000);
        let _driver = rig.driver(off, prompt_rx, Duration::ZERO);
        std::thread::sleep(Duration::from_millis(1_000));
        // A suspension and resume of the backup's P2P (plan 31 C8):
        // every connection closed with a close frame, then dials again.
        let closed = rig.rt_b.block_on(rig.peers_b.quiesce());
        rig.rt_b.block_on(rig.peers_b.unquiesce(false));
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_millis(4_000));
        drop(task);
        let after: Vec<Duration> = rig
            .backup
            .beats
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| *t > t0)
            .map(|(t, _)| t.duration_since(t0))
            .collect();
        eprintln!("closed {closed} connection(s); beats after: {after:?}");
        assert!(closed >= 1, "nothing to close");
        assert!(
            after.iter().any(|d| *d < Duration::from_millis(2_500)),
            "the lane never came back: {after:?}"
        );
    }

    /// `startup-link-lag`: a peer that enrols shows up connected in the
    /// core's links within a round trip, not on the next 1 s link tick
    /// (before, the root's core did not see a delegate that had just
    /// enrolled and recalled its fresh delegation instead of redirecting
    /// a write to it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_peer_that_enrols_reaches_the_cores_links_at_once() {
        use super::{peer_links_task, Internal, OwnS3Watch};
        use constellation_authority::Event;
        use constellation_net::{Payload, Peers};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        struct Quiet;
        impl constellation_net::PeerService for Quiet {
            fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
            fn lease_requested(
                &self,
                _part: String,
                _requester: u64,
                _epoch_applied: Option<u64>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>>
            {
                Box::pin(async move { Payload::Ping { node_id: 2 } })
            }
            fn node_id(&self) -> u64 {
                2
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let topic = constellation_net::topic_for(Some(&[40u8; 32]), "startup-link-lag");
        let (key_a, _) = constellation_net::load_or_create(&dir.path().join("a.key")).unwrap();
        let (key_b, _) = constellation_net::load_or_create(&dir.path().join("b.key")).unwrap();
        let pa = constellation_net::P2p::spawn(key_a, topic).await.unwrap();
        let pb = constellation_net::P2p::spawn(key_b, topic).await.unwrap();
        let registry = vec![
            (
                1u64,
                pa.pubkey_hex(),
                serde_json::to_value(pa.addr()).unwrap(),
            ),
            (
                2u64,
                pb.pubkey_hex(),
                serde_json::to_value(pb.addr()).unwrap(),
            ),
        ];
        let peers_a = Peers::new(pa, 1);
        let peers_b = Peers::new(pb, 2);
        peers_a.refresh_registry(vec![registry[0].clone()]);
        peers_b.refresh_registry(registry.clone());
        {
            let peers_b = peers_b.clone();
            tokio::spawn(async move { peers_b.serve(Arc::new(Quiet)).await });
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let upload = Arc::new(crate::upload::UploadRuntime::for_test(false));
        let own_s3 = OwnS3Watch::new(upload, peers_a.clone());
        tokio::spawn(peer_links_task(peers_a.clone(), own_s3, tx));
        let up = |links: &[constellation_authority::event::PeerLink]| {
            links.iter().any(|l| l.node == 2 && l.connected)
        };
        // The first links go out at once; node 2 is not enrolled yet.
        loop {
            match rx.recv().await {
                Some(Internal::Event(Event::Peers { links })) => {
                    assert!(!up(&links));
                    break;
                }
                Some(_) => {}
                None => panic!("the task stopped"),
            }
        }
        // Mid-tick, node 2 enrols.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let enrolled = Instant::now();
        peers_a.refresh_registry(registry);
        let took = loop {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(Internal::Event(Event::Peers { links }))) if up(&links) => {
                    break enrolled.elapsed();
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("the task stopped"),
                Err(_) => panic!("node 2 never reached the core's links connected"),
            }
        };
        eprintln!("enrolment to a connected link in the core's events: {took:?}");
        assert!(
            took < Duration::from_millis(500),
            "the core saw the enrolled peer's link only after {took:?}"
        );
    }

    /// Plan 39b: an `fsync`'s drain that succeeds through a peer handoff
    /// (this node's PUTs fail) delivers the durable report its chunk owes
    /// before it returns — the sequencer the `back` close forwarded the
    /// manifest to learns before the `fsync` does — as a drain this node
    /// uploaded itself does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_fsync_drain_through_a_handoff_reports_before_it_returns() {
        use super::{ChunkHandoff, Uploader};
        use constellation_fs_core::cache::{ChunkState, DiskCache};
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::{Meta, MetaStore};
        use constellation_net::{Payload, Peers};
        use constellation_store_s3::{ChunkStore, CompressionSetting};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// Node 2: accepts every handoff, records every durable report.
        struct Sequencer(Mutex<Vec<[u8; 32]>>);
        impl constellation_net::PeerService for Sequencer {
            fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
            fn lease_requested(
                &self,
                part: String,
                _requester: u64,
                _epoch_applied: Option<u64>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>>
            {
                Box::pin(async move {
                    Payload::LeaseHandoff {
                        part,
                        epoch: 0,
                        released: false,
                        etag: None,
                        head_seq: None,
                    }
                })
            }
            fn chunk_handoff_requested(
                &self,
                _requester: u64,
                req_id: u64,
                _hashes: Vec<[u8; 32]>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>>
            {
                Box::pin(async move {
                    Payload::ChunkHandoffReply {
                        req_id,
                        uploaded: true,
                    }
                })
            }
            fn chunks_durable(
                &self,
                _from: u64,
                hashes: Vec<[u8; 32]>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
                Box::pin(async move { self.0.lock().unwrap().extend(hashes) })
            }
            fn node_id(&self) -> u64 {
                2
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let topic = constellation_net::topic_for(Some(&[39u8; 32]), "fsync-handoff-report");
        let (key_a, _) = constellation_net::load_or_create(&dir.path().join("a.key")).unwrap();
        let (key_b, _) = constellation_net::load_or_create(&dir.path().join("b.key")).unwrap();
        let pa = constellation_net::P2p::spawn(key_a, topic).await.unwrap();
        let pb = constellation_net::P2p::spawn(key_b, topic).await.unwrap();
        let registry = vec![
            (
                1u64,
                pa.pubkey_hex(),
                serde_json::to_value(pa.addr()).unwrap(),
            ),
            (
                2u64,
                pb.pubkey_hex(),
                serde_json::to_value(pb.addr()).unwrap(),
            ),
        ];
        let peers_a = Peers::new(pa, 1);
        let peers_b = Peers::new(pb, 2);
        peers_a.refresh_registry(registry.clone());
        peers_b.refresh_registry(registry);
        let sequencer = Arc::new(Sequencer(Mutex::new(Vec::new())));
        {
            let (peers_b, sequencer) = (peers_b.clone(), sequencer.clone());
            tokio::spawn(async move { peers_b.serve(sequencer).await });
        }
        // Connected, so the handoff picks node 2 (an empty report: a no-op).
        let started = std::time::Instant::now();
        while !peers_a
            .snapshot()
            .iter()
            .any(|p| p.node_id == 2 && p.connected)
        {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "never connected"
            );
            let _ = tokio::time::timeout(
                Duration::from_secs(2),
                peers_a.request_to_node(
                    2,
                    &Payload::ChunksDurable {
                        from: 1,
                        hashes: Vec::new(),
                    },
                ),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let ino = meta.create(ROOT_INO, "relation", 0o644, 0, 0).unwrap().ino;
        let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 64 << 20).unwrap());
        let failing = crate::upload::pending_upload_tests::FailingStore::new();
        failing.set_fail_puts(true);
        let store = Arc::new(ChunkStore::new(failing.clone()));
        let data = vec![39u8; 4096];
        let hash = store.hash(&data);
        cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        meta.add_pending_upload(&hash, ino).unwrap();
        let mut runtime = crate::upload::UploadRuntime::for_test(false);
        runtime.coop = Some(crate::coop::Coop::new(
            cache.clone(),
            store.clone(),
            peers_a.clone(),
            1,
            1 << 20,
        ));
        let upload = Arc::new(runtime);
        // The `back` close forwarded the manifest to node 2 with the chunk
        // still pending here: node 2 is owed the report.
        upload.note_forwarded(&[hash], 2);
        let uploader = Uploader {
            cache: cache.clone(),
            meta: meta.clone(),
            store: store.clone(),
            compression: CompressionSetting::RAW,
            upload: upload.clone(),
            peers: Some(peers_a.clone()),
            node_id: 1,
        };
        let (sync_tx, _sync_rx) = tokio::sync::mpsc::unbounded_channel();
        let handoff = ChunkHandoff {
            node_id: 1,
            meta: meta.clone(),
            cache,
            store,
            compression: CompressionSetting::RAW,
            upload: upload.clone(),
            peers: peers_a,
            view: Arc::new(crate::lease::LeaseView::default()),
            sync_tx,
        };

        super::drain_with_handoff(uploader, Some(handoff), ino, true)
            .await
            .expect("the drain succeeds through the peer");
        assert!(
            upload.handoff.ok.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the chunk went up through the peer"
        );
        assert!(!meta.upload_pending_for_ino(ino).unwrap());
        assert_eq!(
            *sequencer.0.lock().unwrap(),
            vec![hash.0],
            "the sequencer had the report when the drain returned"
        );
        assert!(upload.take_durable_reports().is_empty(), "sent, not owed");
    }
}
