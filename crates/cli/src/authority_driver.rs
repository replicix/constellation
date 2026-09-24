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
    Action, CasFailure, ClientReply, Config, Control, Core, EpochState, Event, InboxView, Ms,
    NodeId, OpId, PeerLink, PeerMsg, Policy, ReadAnswer, ReadGrantMsg, ReadIndexOutcome, S3Failure,
    S3Op, S3Result, ShipState, Stats, TimerKind, UploadResult,
};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::{JournalPos, LogRecord, Meta, MutateOp, MutateOutcome, Position, Rid};
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
        Event::Roster { .. } => "Roster",
        Event::Peers { .. } => "Peers",
        Event::Activity { .. } => "Activity",
        Event::SubscriberGone { .. } => "SubscriberGone",
        Event::Control { req, .. } => match req {
            Control::Nudge => "Control(Nudge)",
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
        S3Op::InboxPut { .. } => "InboxPut",
        S3Op::InboxRun { .. } => "InboxRun",
        S3Op::InboxDrain { .. } => "InboxDrain",
        S3Op::InboxDelete { .. } => "InboxDelete",
        S3Op::InboxLastN { .. } => "InboxLastN",
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
    /// Plan 30 M0's fault knob (`CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`):
    /// delay every forwarded-mutation reply this node sends, after the
    /// op executed (bug A's trigger). 0 in production.
    pub fault_reply_delay_ms: u64,
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
            return upload.run(None).await.map_err(|e| format!("{e:#}"));
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
}

/// What reaches the driver task from the IO it spawned.
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
    c.dwell_ms = crate::lease::LEASE_MIN_DWELL_MS;
    c.wanted_grace_ms = crate::lease::LEASE_WANTED_GRACE_MS;
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
    c.read_index_deadline_ms = crate::cto::read_index_budget_ms();
    c
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
    mutate_replies: HashMap<OpId, oneshot::Sender<(MutateOutcome, Option<u64>, Position)>>,
    handoff_replies: HashMap<OpId, oneshot::Sender<Option<HandoffResult>>>,
    /// Plan 30 §M8: peers' ReadIndex requests and recalls, by the id the
    /// driver minted for the core.
    read_index_replies: HashMap<OpId, oneshot::Sender<ReadIndexOutcome>>,
    recall_replies: HashMap<OpId, oneshot::Sender<()>>,
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
        // The peer directory and the roster, on a ticker like the
        // daemon's registry poll.
        {
            let tx = self.event_tx();
            let peers = self.deps.peers.clone();
            tokio::spawn(async move {
                loop {
                    let links: Vec<PeerLink> = peers
                        .snapshot()
                        .into_iter()
                        .map(|p| PeerLink {
                            node: p.node_id,
                            connected: p.connected,
                            last_seen: p
                                .last_seen
                                .map(|at| Ms(now_unix_ms() - at.elapsed().as_millis() as i64)),
                        })
                        .collect();
                    if tx.send(Internal::Event(Event::Peers { links })).is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                }
            });
        }
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
            let actions = self.core.handle(now(), event, &*self.deps.meta);
            let handled_us = started.elapsed().as_micros() as u64;
            // The mirror first: a FUSE thread must see the releasing flag
            // before the release's IO starts.
            self.refresh();
            let refreshed_us = started.elapsed().as_micros() as u64 - handled_us;
            tracing::trace!(
                event = kind,
                actions = ?actions.iter().map(action_kind).collect::<Vec<_>>(),
                job = ?self.core.job(),
                handled_us,
                refreshed_us,
                "core step"
            );
            self.dispatch(actions);
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
        let cfg = self.core.config();
        let lease = self.core.lease();
        self.deps.view.mirror(lease, now, cfg);
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
        let _ = self.int_tx.send(Internal::Control {
            req: Control::Epoch {
                open: state.open,
                active: state.active,
                frozen: state.frozen,
                flushing: state.flushing,
                base: state.base,
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
            SyncRequest::DrainInode { ino, reply } => {
                let upload = self.uploader();
                tokio::spawn(async move {
                    let r = upload.run((ino != 0).then_some(ino)).await;
                    let _ = reply.send(r.map(|_| ()).map_err(|e| format!("{e:#}")));
                });
                None
            }
            SyncRequest::TailToHead { reply } => {
                control(Control::TailToHead, ControlReply::Done(reply))
            }
            SyncRequest::Acquire { reply } => {
                control(Control::Acquire, ControlReply::Acquire(reply))
            }
            SyncRequest::HandOff { requester, reply } => {
                let req = self.control_id();
                self.handoff_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::LeaseRequest { req },
                }))
            }
            SyncRequest::Mutate {
                requester,
                op,
                rid,
                acked_through,
                reply,
            } => {
                let op = match MutateOp::from_postcard(&op) {
                    Ok(op) => op,
                    Err(_) => {
                        let _ =
                            reply.send((MutateOutcome::Errno(libc::EINVAL), None, Position::ZERO));
                        return None;
                    }
                };
                let req = self.control_id();
                self.mutate_replies.insert(req, reply);
                Some(Internal::Event(Event::Peer {
                    from: requester,
                    msg: PeerMsg::MutateRequest {
                        req,
                        rid,
                        op,
                        acked_through,
                    },
                }))
            }
            SyncRequest::Submit {
                op,
                rid,
                policy,
                reply,
            } => {
                self.replies.insert(rid, reply);
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
            SyncRequest::Shutdown { reply } => {
                control(Control::Shutdown, ControlReply::Done(reply))
            }
        }
    }

    fn uploader(&self) -> Uploader {
        Uploader {
            cache: self.deps.cache.clone(),
            meta: self.deps.meta.clone(),
            store: self.deps.chunk_store.clone(),
            compression: self.deps.compression,
            upload: self.deps.upload.clone(),
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
                            if epochs.is_frozen() {
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
                    let designations = self.deps.designations.clone();
                    let meta = self.deps.meta.clone();
                    tokio::spawn(async move {
                        // Plan 30 §M7: a hint only. The segment reaches the
                        // holder's subscribers on their log streams (over
                        // QUIC between enrolled members, which hold the
                        // E2E key anyway), so gossip — relayed by third
                        // parties — never carries log content, sealed or
                        // not.
                        peers.announce_segment(PARTITION, seq, epoch).await;
                        if let Ok(seg) = constellation_authority::segment::decode(&payload) {
                            verify_flush_acks(&designations, &meta, seq, &seg.records).await;
                        }
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
                    tokio::spawn(async move {
                        let ok = match rebuild_deposed(&meta, &log, &state_dir).await {
                            Ok(()) => true,
                            Err(error) => {
                                tracing::warn!(error = %format!("{error:#}"), "rebuilding the deposed replica failed");
                                false
                            }
                        };
                        let _ = tx.send(Internal::Event(Event::RebuildDone { op, ok }));
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
            } => {
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
                            let _ = tx.send((outcome, base, position));
                        });
                    } else {
                        let _ = tx.send((outcome, base, position));
                    }
                }
            }
            PeerMsg::ReadIndexReply { req, outcome } => {
                if let Some(tx) = self.read_index_replies.remove(&req) {
                    let _ = tx.send(outcome);
                }
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
                tokio::spawn(async move {
                    let payload = Payload::ReadIndex {
                        requester,
                        req_id: req.0,
                        ino,
                        dir,
                        name,
                    };
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
                        })) if req_id == req.0 => {
                            let outcome = match status {
                                0 => ReadIndexOutcome::Ok {
                                    position: Position {
                                        seq: position_seq,
                                        pending: position_pending
                                            .map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                                    },
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
                            let outage = !peers.connection_alive(to).await;
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
                tokio::spawn(async move {
                    let payload = Payload::ReadRecall {
                        holder,
                        req_id: req.0,
                        ino,
                        grant,
                    };
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
                            let outage = !peers.connection_alive(to).await;
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
            } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let forward = self.deps.forward.clone();
                let timeout = Duration::from_millis(self.core.config().forward_timeout_ms);
                let requester = self.node_id;
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
                    };
                    let started = std::time::Instant::now();
                    let reply =
                        tokio::time::timeout(timeout, peers.request_to_node(to, &payload)).await;
                    match reply {
                        Ok(Ok(Payload::MutateReply {
                            req_id,
                            outcome,
                            base,
                            position_seq,
                            position_pending,
                        })) if req_id == req.0 => {
                            let position = Position {
                                seq: position_seq,
                                pending: position_pending
                                    .map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                            };
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
                                },
                            }));
                        }
                        _ => {
                            forward.record_err();
                            // Round 3a/4's rule: an outage only with no
                            // open connection left to the holder.
                            let outage = !peers.connection_alive(to).await;
                            let _ = tx.send(Internal::Event(Event::PeerFailed { req, to, outage }));
                        }
                    }
                });
            }
            PeerMsg::LeaseRequest { req } => {
                let tx = self.int_tx.clone();
                let peers = self.deps.peers.clone();
                let requester = self.node_id;
                let timeout = Duration::from_millis(self.core.config().handoff_request_timeout_ms);
                tokio::spawn(async move {
                    let payload = Payload::LeaseRequest {
                        part: PARTITION.to_string(),
                        requester,
                    };
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
                            let outage = !peers.connection_alive(to).await;
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
                    let outage = !peers.connection_alive(to).await;
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
                S3Op::InboxPut { batch } => S3Result::InboxPut(
                    inbox
                        .put_batch(&batch)
                        .await
                        .map(|_| ())
                        .map_err(cas_failure),
                ),
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

/// The chunk upload pass, as the driver's spawned tasks run it.
#[derive(Clone)]
struct Uploader {
    cache: Arc<DiskCache>,
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    compression: CompressionSetting,
    upload: Arc<crate::UploadRuntime>,
}

impl Uploader {
    async fn run(&self, only_ino: Option<constellation_fs_core::Ino>) -> Result<()> {
        crate::upload_dirty_chunks_report(
            &self.cache,
            &self.meta,
            &self.store,
            self.compression,
            &self.upload,
            only_ino,
            None,
        )
        .await
        .map(|_| ())
    }
}

/// Offline designation flush-ack (DESIGN.md §5.2): for every shipped
/// record touching a path designated to another node, wait (bounded) for
/// the designee's ack. Best effort: the segment is already durable.
async fn verify_flush_acks(
    designations: &crate::designation::DesignationManager,
    meta: &Meta,
    seq: u64,
    records: &[LogRecord],
) {
    let mut checked: std::collections::HashSet<String> = std::collections::HashSet::new();
    for rec in records {
        let dir = match rec {
            LogRecord::Mkdir { parent, .. }
            | LogRecord::Create { parent, .. }
            | LogRecord::Symlink { parent, .. }
            | LogRecord::Mknod { parent, .. }
            | LogRecord::Link { parent, .. }
            | LogRecord::Unlink { parent, .. }
            | LogRecord::Rmdir { parent, .. }
            | LogRecord::Rename { parent, .. } => Some(*parent),
            LogRecord::Setattr { ino, .. } | LogRecord::WriteManifest { ino, .. } => {
                meta.parent_of(*ino).ok().flatten()
            }
            _ => None,
        };
        let Some(dir) = dir else {
            continue;
        };
        let Ok(path) = meta.path_of(dir) else {
            continue;
        };
        if !checked.insert(path.clone()) {
            continue;
        }
        if !designations.await_flush_ack(&path, PARTITION, seq).await {
            tracing::warn!(
                seq,
                path,
                "designee did not ack this flush within the bound; the write is durable in S3 \
                 but the designee's view may lag briefly"
            );
        }
    }
}

/// A deposition recovery with uncaptured journal rows (holder capture
/// off): rebuild the namespace from the shared log through a side replica
/// bootstrapped from the head commit, and swap it in.
async fn rebuild_deposed(meta: &Meta, log: &LogStore, state_dir: &std::path::Path) -> Result<()> {
    let view_path = state_dir.join(".deposition-rebuild.db");
    let _ = std::fs::remove_dir_all(&view_path);
    crate::shipper::bootstrap(&view_path, log)
        .await
        .context("bootstrapping the shared log for a deposition rebuild")?;
    let side = Meta::open(&view_path)?;
    meta.replace_ns_from_rebuilt(&side)?;
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
        {
            self.step().await?;
        }
        Ok(())
    }

    /// Tail the log to head (no lease touched).
    pub async fn tail_to_head(&mut self) -> Result<()> {
        self.control(Control::TailToHead).await.map(|_| ())
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
            S3Op::InboxPut { .. } => S3Result::InboxPut(Err(CasFailure::Failed("no inbox".into()))),
            S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
            S3Op::InboxDrain { .. } => S3Result::InboxDrain(Ok(Vec::new())),
            S3Op::InboxDelete { .. } => S3Result::InboxDelete(Ok(())),
            S3Op::InboxLastN { .. } => S3Result::InboxLastN(Ok(None)),
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
                Action::RebuildReplica { op } => {
                    self.queue.push_back(Event::RebuildDone { op, ok: false })
                }
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
