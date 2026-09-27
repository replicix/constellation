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

use crate::fusefs::{AcquireProgress, HandoffResult, SyncRequest};
use crate::lease::LeaseView;
use anyhow::{Context, Result};
use constellation_authority::action::ControlOk;
use constellation_authority::core::JobKind;
use constellation_authority::{
    Action, CasFailure, ClientReply, Config, Control, Core, EpochState, Event, InboxView,
    LockAnswer, LockOutcome, LockRenewResult, LockTestAnswer, LockTestOutcome, Ms, NodeId, OpId,
    PeerLink, PeerMsg, Policy, ReadAnswer, ReadGrantMsg, ReadIndexOutcome, S3Failure, S3Op,
    S3Result, ShipState, Stats, TimerKind, UploadResult,
};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::locks::{Grant, GrantId};
use constellation_meta::{JournalPos, Meta, MutateOp, MutateOutcome, Position, Rid};
use constellation_net::{LogEvent, Payload};
use constellation_store_s3::inbox::InboxStore;
use constellation_store_s3::lease::now_unix_ms;
use constellation_store_s3::log::PARTITION;
use constellation_store_s3::{
    ChunkStore, CompressionSetting, LeaseMode, LeaseStore, LogStore, StoreError,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A core step (handle + refresh + dispatch) longer than this is logged:
/// nothing else on the node's authority path — a holder's backup
/// heartbeats included — runs meanwhile (a third of the default 1.5 s
/// seal window).
const SLOW_STEP_US: u64 = 500_000;

/// What a forwarded mutation's reply carries back to the bridge: the
/// outcome, plan 30 §M6's `base`, the position and (§M11) the executing
/// delegation generation (0: the root).
pub type MutateReplyParts = (MutateOutcome, Option<u64>, Position, u64);

/// Plan 30 §M14: an owner's answer to a `LockRenew`.
pub type LockRenewResults = Vec<(constellation_fs_core::Ino, GrantId, LockRenewResult)>;

/// A delegation renewal's answer: the ttl, the lock grants handed over,
/// the remaining lock grace (ms) and the subtree's lock floor.
type DelegRenewReply = (u64, Vec<Grant>, u64, constellation_meta::Position);

/// The core's observable state, refreshed after every event, for
/// `status` and the background tickers (placement, atime, prune).
#[derive(Debug, Clone, Default)]
pub struct CoreStatus {
    pub stats: Stats,
    pub ship: Option<ShipState>,
    pub lease: constellation_api::LeaseStatus,
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
    /// Phase 2b: the placement's busiest subtrees, `(dir, node, node_ops,
    /// subtree_ops)`.
    pub placement_top: Vec<(u64, u64, u64, u64)>,
    /// Plan 30 §M14: `--locks cluster`, and the core's lock requests,
    /// parked waiters and recalls in flight.
    pub locks_cluster: bool,
    pub lock_requests_in_flight: usize,
    pub lock_waiters: usize,
    pub lock_recalls_in_flight: usize,
}

/// Short names for the trace line around every core step.
fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Submit { .. } => "Submit",
        Event::Peer { msg, .. } => match msg {
            PeerMsg::MutateRequest { .. } => "Peer(MutateRequest)",
            PeerMsg::MutateReply { .. } => "Peer(MutateReply)",
            PeerMsg::SegmentPublished { .. } => "Peer(SegmentPublished)",
            PeerMsg::LogSubscribe { .. } => "Peer(LogSubscribe)",
            PeerMsg::LogStream { .. } => "Peer(LogStream)",
            PeerMsg::LogStreamEnd { .. } => "Peer(LogStreamEnd)",
            _ => "Peer",
        },
        Event::PeerFailed { .. } => "PeerFailed",
        Event::Timer { .. } => "Timer",
        Event::UploadsDone { .. } => "UploadsDone",
        Event::PublishDone { .. } => "PublishDone",
        Event::ConflictCopyDone { .. } => "ConflictCopyDone",
        Event::RebuildDone { .. } => "RebuildDone",
        Event::LockFlushed { .. } => "LockFlushed",
        Event::Roster { .. } => "Roster",
        Event::Slack { .. } => "Slack",
        Event::Peers { .. } => "Peers",
        Event::Activity { .. } => "Activity",
        Event::SubscriberGone { .. } => "SubscriberGone",
        Event::Control { req, .. } => match req {
            Control::Nudge => "Control(Nudge)",
            Control::Journaled => "Control(Journaled)",
            Control::PublishNow => "Control(PublishNow)",
            Control::Barrier { .. } => "Control(Barrier)",
            Control::TailToHead => "Control(TailToHead)",
            Control::Acquire => "Control(Acquire)",
            _ => "Control",
        },
        Event::S3 { .. } => "S3",
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
        Action::Publish { .. } => "Publish",
        Action::FollowHead { .. } => "FollowHead",
        Action::Announce { .. } => "Announce",
        Action::ConflictCopy { .. } => "ConflictCopy",
        Action::RebuildReplica { .. } => "RebuildReplica",
        Action::LockFlush { .. } => "LockFlush",
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
    pub upload: Arc<crate::UploadRuntime>,
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
    /// Plan 30 M0's fault knob (`CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`):
    /// delay every forwarded-mutation reply this node sends, after the
    /// op executed (bug A's trigger). 0 in production.
    pub fault_reply_delay_ms: u64,
    /// The open-orphan hold writer (`crate::holds`), nudged after every
    /// applied segment so a foreign unlink of a locally open file is
    /// claimed within a round trip. `None` in tools and tests.
    pub holds: Option<Arc<crate::holds::Holds>>,
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
            let result = upload.run(None).await.map_err(|e| format!("{e:#}"));
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
    Control { req: Control, reply: ControlReply },
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
    c.delegation_ttl_ms = env_ms("CONSTELLATION_DELEGATION_TTL_MS", c.delegation_ttl_ms);
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

fn now() -> Ms {
    Ms(now_unix_ms())
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
    sync_tx: mpsc::UnboundedSender<SyncRequest>,
    sync_rx: mpsc::UnboundedReceiver<SyncRequest>,
    replies: HashMap<Rid, oneshot::Sender<ClientReply>>,
    mutate_replies: HashMap<OpId, oneshot::Sender<MutateReplyParts>>,
    handoff_replies: HashMap<OpId, oneshot::Sender<Option<HandoffResult>>>,
    /// Plan 30 §M8: peers' ReadIndex requests and recalls, by the id the
    /// driver minted for the core.
    read_index_replies: HashMap<OpId, oneshot::Sender<ReadIndexOutcome>>,
    recall_replies: HashMap<OpId, oneshot::Sender<()>>,
    /// Plan 30 §M11: peers' delegate-stream batches, renewals and
    /// recalls this node is answering.
    deleg_stream_replies: HashMap<OpId, oneshot::Sender<(u64, bool)>>,
    deleg_renew_replies: HashMap<OpId, oneshot::Sender<DelegRenewReply>>,
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
}

impl Driver {
    /// Build the driver; `run` drives it. `sync_tx` is what the rest of
    /// the daemon sends `SyncRequest`s on.
    pub fn new(
        deps: DriverDeps,
        sync_tx: mpsc::UnboundedSender<SyncRequest>,
        sync_rx: mpsc::UnboundedReceiver<SyncRequest>,
    ) -> Self {
        let node_id = deps.config.node_id;
        let core = Core::new(deps.config.clone());
        let leases = LeaseStore::new(deps.store_inner.clone(), PARTITION, deps.lease_mode);
        let inbox = match &deps.e2e {
            Some(keys) => InboxStore::new_e2e(deps.store_inner.clone(), keys.clone()),
            None => InboxStore::new(deps.store_inner.clone()),
        };
        let (int_tx, int_rx) = mpsc::unbounded_channel();
        Self {
            core,
            deps,
            node_id,
            leases,
            inbox,
            int_tx,
            int_rx,
            sync_tx,
            sync_rx,
            replies: HashMap::new(),
            mutate_replies: HashMap::new(),
            handoff_replies: HashMap::new(),
            read_index_replies: HashMap::new(),
            recall_replies: HashMap::new(),
            deleg_stream_replies: HashMap::new(),
            deleg_renew_replies: HashMap::new(),
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
        }
    }

    /// The driver's own event sender (tickers spawned by `run`).
    fn event_tx(&self) -> mpsc::UnboundedSender<Internal> {
        self.int_tx.clone()
    }

    pub async fn run(mut self) {
        let mut out = Vec::new();
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
        // daemon's registry poll.
        {
            let tx = self.event_tx();
            let peers = self.deps.peers.clone();
            tokio::spawn(async move {
                // Plan 30 §M9: when each peer's link came up, for the
                // backup selection's stability requirement.
                let mut since: HashMap<u64, Ms> = HashMap::new();
                loop {
                    let now = Ms(now_unix_ms());
                    let links: Vec<PeerLink> = peers
                        .snapshot()
                        .into_iter()
                        .map(|p| {
                            // Fault injection: a denied peer is a link that
                            // is down (the cut is a real one to the core:
                            // the inbox path, no redirects to it).
                            let connected = p.connected && !crate::fault::p2p_denied(p.node_id);
                            let since = if connected {
                                Some(*since.entry(p.node_id).or_insert(now))
                            } else {
                                since.remove(&p.node_id);
                                None
                            };
                            PeerLink {
                                node: p.node_id,
                                connected,
                                last_seen: if connected {
                                    p.last_seen.map(|at| {
                                        Ms(now_unix_ms() - at.elapsed().as_millis() as i64)
                                    })
                                } else {
                                    None
                                },
                                rtt_ms: p.rtt_ms,
                                since,
                            }
                        })
                        .collect();
                    if tx.send(Internal::Event(Event::Peers { links })).is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                }
            });
        }
        let mut step_end = std::time::Instant::now();
        loop {
            let internal = tokio::select! {
                biased;
                msg = self.int_rx.recv() => match msg {
                    Some(m) => m,
                    None => break,
                },
                req = self.sync_rx.recv() => match req {
                    Some(req) => match self.on_request(req) {
                        Some(internal) => internal,
                        None => continue,
                    },
                    None => break,
                },
            };
            let waited_us = step_end.elapsed().as_micros() as u64;
            let event = match internal {
                Internal::Event(event) => event,
                Internal::Control { req, reply } => {
                    let op = self.control_id();
                    self.controls.insert(op, reply);
                    Event::Control { op, req }
                }
            };
            self.deps
                .last_sync_ms
                .store(crate::prune::now_unix_ms(), Ordering::Relaxed);
            let kind = event_kind(&event);
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
            let actions = self.core.handle(now(), event, &*self.deps.meta);
            if applies_segments {
                if let Some(holds) = &self.deps.holds {
                    holds.nudge();
                }
            }
            let handled_us = started.elapsed().as_micros() as u64;
            // The mirror first: a FUSE thread must see the releasing flag
            // before the release's IO starts.
            self.refresh();
            let refreshed_us = started.elapsed().as_micros() as u64 - handled_us;
            let action_kinds: Vec<&'static str> = actions.iter().map(action_kind).collect();
            self.dispatch(actions);
            let dispatched_us = started.elapsed().as_micros() as u64 - handled_us - refreshed_us;
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
            if step_us >= SLOW_STEP_US {
                // Everything the core does (backup heartbeats included)
                // waits for this step: a long one silences the node.
                tracing::warn!(
                    event = kind,
                    actions = ?action_kinds,
                    handled_us,
                    refreshed_us,
                    dispatched_us,
                    "slow core step"
                );
            }
            step_end = std::time::Instant::now();
            if self.core.stopped() {
                break;
            }
        }
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
            .set_liveness(self.core.fast_tenure(), self.core.last_s3_fresh().0);
        let mut status = self.deps.status.lock().unwrap();
        status.stats = self.core.stats;
        status.ship = Some(self.core.ship().clone());
        status.lease = self.deps.view.status();
        status.gate_pending = lease.gate.is_some();
        status.job = self.core.job();
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
        drop(status);
        self.deps
            .epochs
            .set_claim_view(self.core.epoch_claim_view(now));
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
            SyncRequest::Publish { reply } => {
                control(Control::PublishNow, ControlReply::Publish(reply))
            }
            SyncRequest::Barrier { ino, reply } => {
                // Upload the inode's chunks first; the round ships the
                // journal.
                let tx = self.int_tx.clone();
                let upload = self.uploader();
                tokio::spawn(async move {
                    match upload.run(Some(ino)).await {
                        Ok(_) => {
                            let _ = tx.send(Internal::Control {
                                req: Control::Barrier { ino: Some(ino) },
                                reply: ControlReply::Done(reply),
                            });
                        }
                        Err(e) => {
                            let _ = reply.send(Err(format!("{e:#}")));
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
            SyncRequest::DrainInode { ino, reply } => {
                let upload = self.uploader();
                let handoff = (ino != 0).then(|| self.chunk_handoff());
                tokio::spawn(async move {
                    let r = drain_with_handoff(upload, handoff, ino).await;
                    let _ = reply.send(r.map_err(|e| format!("{e:#}")));
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
                reply,
            } => {
                let deps = Position::from_postcard(&deps);
                let op = match MutateOp::from_postcard(&op) {
                    Ok(op) => op,
                    Err(_) => {
                        let _ = reply.send((
                            MutateOutcome::Errno(libc::EINVAL),
                            None,
                            Position::ZERO,
                            0,
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
                                MutateOutcome::Errno(libc::EIO),
                                None,
                                Position::ZERO,
                                0,
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
                    },
                }))
            }
            SyncRequest::PeerDelegateStream {
                from,
                gen,
                txs,
                reply,
            } => {
                let req = self.control_id();
                self.deleg_stream_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from,
                    msg: PeerMsg::DelegateStream { req, gen, txs },
                }))
            }
            SyncRequest::PeerDelegRenew {
                from,
                gen,
                backup,
                stream_head,
                reply,
            } => {
                let req = self.control_id();
                self.deleg_renew_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from,
                    msg: PeerMsg::DelegRenew {
                        req,
                        gen,
                        backup: (backup != 0).then_some(backup),
                        stream_head,
                    },
                }))
            }
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
                reply,
            } => {
                self.replies.insert(rid, reply);
                if in_doubt {
                    let _ = self.int_tx.send(Internal::Control {
                        req: Control::InDoubt { rid },
                        reply: ControlReply::None,
                    });
                }
                let acked = self.drain_acks();
                if !acked.is_empty() {
                    let _ = self.int_tx.send(Internal::Event(Event::Activity {
                        last_write: Ms(self.deps.view.last_write_ms()),
                        acked_seqs: acked,
                    }));
                }
                Some(Internal::Event(Event::Submit { rid, op, policy }))
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
                from,
                txs,
                through,
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
                        from,
                        txs,
                        through,
                    },
                }))
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
                reply,
            } => control(
                Control::Lock {
                    ino,
                    mode,
                    blocking,
                },
                ControlReply::Lock(reply),
            ),
            SyncRequest::LockIdle { ino } => control(Control::LockIdle { ino }, ControlReply::None),
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
                reply,
            } => {
                let req = self.control_id();
                self.lock_request_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LockRequest {
                        req,
                        ino,
                        mode,
                        blocking,
                        sent: constellation_authority::Ms(sent),
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
                    sent: Ms(sent),
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
                Action::SetTimer { id, at, .. } => {
                    let tx = self.int_tx.clone();
                    let delay = Duration::from_millis((at.0 - now_unix_ms()).max(0) as u64);
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(Internal::Event(Event::Timer { id }));
                    });
                }
                Action::CancelTimer { .. } => {}
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
                    let last_write = Ms(self.deps.view.last_write_ms());
                    if round {
                        let _ = tx.send(Internal::Event(Event::Activity {
                            last_write,
                            acked_seqs: acked,
                        }));
                    }
                    let sync_tx = self.sync_tx.clone();
                    let bulk = self.bulk_pass.clone();
                    let wait = self.round_upload_wait;
                    // Plan 30 §M10: a frozen epoch's hold owner still
                    // flushes once S3 is back (the core only runs the
                    // upload for it after its probe found S3).
                    let holds = self.core.lease().epoch_held();
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
                            // (A frozen epoch that carries no lease has no
                            // hold owner: its members flush and close it.)
                            if epochs.is_frozen() && !holds && epochs.carrier().0.is_some() {
                                let _ = tx.send(Internal::Event(Event::UploadsDone {
                                    op,
                                    result: UploadResult::Skip,
                                }));
                                return;
                            }
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
                } => {
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
                            &meta, &sync_tx, &forward, node_id, &op, &refusal,
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
                        let ok = flush(ino);
                        let _ = tx.send(Internal::Event(Event::LockFlushed { ino, grant, ok }));
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
                            let _ = tx.send((outcome, base, position, gen));
                        });
                    } else {
                        let _ = tx.send((outcome, base, position, gen));
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
                through,
                refused,
                ..
            } => {
                if let Some(tx) = self.deleg_stream_replies.remove(&req) {
                    let _ = tx.send((through, refused));
                }
            }
            PeerMsg::DelegRenewed {
                req,
                ttl_ms,
                locks,
                lock_grace_ms,
                lock_floor,
                ..
            } => {
                if let Some(tx) = self.deleg_renew_replies.remove(&req) {
                    let _ = tx.send((ttl_ms, locks, lock_grace_ms, lock_floor));
                }
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
            } => {
                let payload = Payload::LockRequest {
                    requester: self.node_id,
                    req_id: req.0,
                    ino,
                    exclusive: crate::locks::exclusive(mode),
                    blocking,
                    sent: sent.0,
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
                self.lock_rpc(to, req, payload, |reply, req| match reply {
                    Payload::LockRenewed { req_id, results } if req_id == req.0 => {
                        Some(PeerMsg::LockRenewed {
                            req,
                            results: crate::locks::renew_results_of(results),
                        })
                    }
                    _ => None,
                });
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
            PeerMsg::DelegateStream { req, gen, txs } => {
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
                    let payload = Payload::DelegateStream {
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
                        Ok(Ok(Payload::DelegateStreamAck {
                            req_id,
                            gen,
                            through,
                            refused,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegateStreamAck {
                                    req,
                                    gen,
                                    through,
                                    refused,
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
            PeerMsg::DelegRenew {
                req,
                gen,
                backup,
                stream_head,
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let from = self.node_id;
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms * 2);
                if crate::fault::p2p_denied(to) {
                    let _ = tx.send(Internal::Event(Event::PeerFailed {
                        req,
                        to,
                        outage: true,
                    }));
                    return;
                }
                tokio::spawn(async move {
                    let payload = Payload::DelegRenew {
                        from,
                        req_id: req.0,
                        gen,
                        backup: backup.unwrap_or(0),
                        stream_head,
                    };
                    let reply = tokio::time::timeout(
                        timeout,
                        peers.request_to_node_timeout(to, &payload, timeout),
                    )
                    .await;
                    match reply {
                        Ok(Ok(Payload::DelegRenewed {
                            req_id,
                            gen,
                            ttl_ms,
                            locks,
                            lock_grace_ms,
                            lock_floor,
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegRenewed {
                                    req,
                                    gen,
                                    ttl_ms,
                                    locks: crate::locks::grants_of(&locks),
                                    lock_grace_ms,
                                    lock_floor: crate::locks::floor_of(&lock_floor),
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
                        })) if req_id == req.0 => {
                            let _ = tx.send(Internal::Event(Event::Peer {
                                from: to,
                                msg: PeerMsg::DelegRecalled {
                                    req,
                                    gen,
                                    through,
                                    locks: constellation_meta::locks::LockHandback {
                                        grants: crate::locks::grants_of(&locks),
                                        floor: crate::locks::floor_of(&lock_floor),
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
            } => {
                // A manifest naming chunks still uploading here (a `back`
                // close) says so; the recipient awaits them, and this
                // node reports them once they are up (`Uploader::run`).
                let pending =
                    crate::forwarded_pending_chunks(&self.deps.meta, &self.deps.cache, &op);
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
                                outcome: MutateOutcome::Errno(libc::EINVAL),
                                base: None,
                                position: Position::ZERO,
                                gen: 0,
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
                from,
                txs,
                through,
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let timeout = Duration::from_millis(self.core.config().backup_ack_timeout_ms);
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
                        tokio::time::sleep(timeout).await;
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
                        from,
                        txs,
                        through,
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
            while let Some(event) = rx.recv().await {
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
                if tx
                    .send(Internal::Event(Event::Peer { from: to, msg }))
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
            if !crate::forwarded_pending_chunks(&uploader.meta, &uploader.cache, &op).is_empty()
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
    upload: Arc<crate::UploadRuntime>,
    /// For the durable reports this pass owes (`meta::store::remote`).
    peers: Option<constellation_net::Peers>,
    node_id: u64,
}

impl Uploader {
    async fn run(&self, only_ino: Option<constellation_fs_core::Ino>) -> Result<()> {
        self.run_report(only_ino).await.map(|_| ())
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
    ) -> Result<crate::UploadReport> {
        let result = crate::upload_dirty_chunks_report(
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
        let Some(peers) = &self.peers else {
            return;
        };
        let mut reports = self.upload.take_durable_reports();
        if let Some(all) = reports.remove(&crate::REPORT_TO_ALL) {
            let known = peers.remote_snapshot();
            if known.is_empty() {
                // Right after a restart: nobody to tell yet.
                self.upload.requeue_report(crate::REPORT_TO_ALL, all);
            } else {
                self.upload.report_delivered(crate::REPORT_TO_ALL);
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
        let limit = crate::remote_chunk_wait();
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

/// EC2 finding 1: how long a drain (a write-through close, a forwarded
/// manifest's pre-publication upload, an `fsync`) waits on this node's
/// S3 path making no progress — no upload and no other S3 request
/// completing — before it hands its chunks to a peer
/// (`CONSTELLATION_CHUNK_HANDOFF_AFTER_MS`, default 6000, longer than
/// the 5 s registry poll so a working link always shows a completion
/// inside it; 0 disables).
fn chunk_handoff_after_ms() -> u64 {
    static AFTER: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *AFTER.get_or_init(|| {
        std::env::var("CONSTELLATION_CHUNK_HANDOFF_AFTER_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6000)
    })
}

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
async fn drain_with_handoff(
    upload: Uploader,
    handoff: Option<ChunkHandoff>,
    ino: constellation_fs_core::Ino,
) -> Result<()> {
    let (tx, mut done) = tokio::sync::oneshot::channel();
    let stats = upload.upload.clone();
    tokio::spawn(async move {
        let _ = tx.send(upload.run((ino != 0).then_some(ino)).await);
    });
    let stopped = || anyhow::anyhow!("the drain task stopped");
    let after_ms = chunk_handoff_after_ms();
    let Some(handoff) = handoff.filter(|h| after_ms > 0 && h.possible()) else {
        return done.await.unwrap_or_else(|_| Err(stopped()));
    };
    let after = std::time::Duration::from_millis(after_ms);
    let now = constellation_store_s3::lease::now_unix_ms();
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
                    if !tried {
                        tracing::info!(ino, %error, "drain failed; handing its chunks to a peer");
                        if handoff.hand_off(ino).await {
                            return Ok(());
                        }
                    }
                }
                return result;
            }
            Err(_) if !tried && stats.uploads_stalled(after_ms as i64) => {
                tried = true;
                tracing::info!(
                    ino,
                    waited_ms = wait.as_millis() as u64,
                    "no upload has completed on this node for a while: handing the drain's chunks to a peer"
                );
                if handoff.hand_off(ino).await {
                    return Ok(());
                }
            }
            Err(_) => {}
        }
        wait = after;
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
        if handoff.upload.has_forwarded() && handoff.upload.uploads_stalled(after_ms as i64) {
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
    upload: Arc<crate::UploadRuntime>,
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
    /// acknowledge them here as uploaded. `false`: nobody could.
    async fn hand_off(&self, ino: constellation_fs_core::Ino) -> bool {
        let rows = match self.meta.pending_uploads() {
            Ok(rows) => rows,
            Err(_) => return false,
        };
        let hashes: Vec<constellation_fs_core::ChunkHash> = rows
            .iter()
            .filter(|(_, i)| *i == ino)
            .map(|(h, _)| *h)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        self.hand_off_hashes(hashes, &[], ino).await
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
            .store(constellation_store_s3::lease::now_unix_ms(), Relaxed);
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
            let set: std::collections::HashSet<_> = hashes.iter().copied().collect();
            if let Ok(rows) = self.meta.pending_uploads() {
                for (hash, i) in rows {
                    if set.contains(&hash) {
                        let _ = self.meta.ack_upload(&hash, i);
                    }
                }
            }
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
        cfg.single_writer = lease_mode == LeaseMode::SingleWriter;
        cfg.p2p = false;
        cfg.inbox = false;
        cfg.publisher = publisher.is_some();
        cfg.forwarding = false;
        cfg.locks = false;
        cfg.ttl_ms = crate::lease::lease_ttl_ms();
        let mut core = Core::new(cfg);
        let mut out = Vec::new();
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
            let out = self.core.handle(now(), event, &*self.meta);
            self.absorb(out);
        } else if let Some((op, req)) = self.pending_s3.pop_front() {
            let result = self.run_s3(req).await;
            self.queue.push_back(Event::S3 { op, result });
        } else if let Some(op) = self.pending_rebuild.pop_front() {
            // A real rebuild (the retention-gap tests need one): the side
            // replica lives in a private temp dir.
            let dir = std::env::temp_dir().join(format!(
                "constellation-standalone-rebuild-{}-{}",
                std::process::id(),
                op.0
            ));
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
                Action::SetTimer { id, at, .. } => {
                    if at <= now() {
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
                Action::CancelTimer { .. }
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
}
