//! One simulated node: the real `Core` driven over a real `Meta`, real
//! `LogStore`/`LeaseStore`/`CommitChain` on the simulated bucket, and the
//! simulated bus — the shape `node_runtime` takes in phase 2, minus FUSE
//! and everything the plan keeps out of scope.
//!
//! The driver obeys the one rule the core imposes: it never waits on an
//! action's completion while handling an event. S3 requests, sends,
//! timers and publishes are spawned; replica calls happen inside
//! `Core::handle`; results come back through the node's event queue.

use super::bus::Bus;
use super::chunks::ChunkWorld;
use super::clock::{Clock, NodeClockState};
use super::store::Bucket;
use constellation_authority::action::ControlOk;
use constellation_authority::{
    Action, Carrier, CasFailure, ClientReply, Config, Control, Core, EpochClaimView, Event, NodeId,
    OpId, PeerLink, PeerMsg, Policy, Replica, S3Failure, S3Op, S3Result, Seq, Stats, UploadResult,
};
use constellation_meta::locks::LockTag;
use constellation_meta::{
    CompletedOutcome, Meta, MutateOp, MutateOutcome, PublishBasis, Rid, TouchSet,
};
use constellation_store_s3::commits::{CommitChain, CommitPayload};
use constellation_store_s3::heartbeat::HeartbeatStore;
use constellation_store_s3::inbox::InboxStore;
use constellation_store_s3::{LeaseMode, LeaseStore, LogStore, StoreError};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// The event each node's driver is handling right now, for the stall
/// watchdog (`AUTHORITY_SIM_WATCHDOG=1`): a `Core::handle` that never
/// returns blocks the whole current-thread runtime, so only another OS
/// thread can say where it stopped.
pub static IN_FLIGHT: Mutex<Option<(NodeId, String, std::time::Instant)>> = Mutex::new(None);
/// Event counts by kind and the last simulated time seen, for the same
/// watchdog: a loop that keeps simulated time from advancing shows up as
/// a rising count at a frozen clock.
pub static EVENT_HISTOGRAM: Mutex<Option<(u64, BTreeMap<String, u64>)>> = Mutex::new(None);
/// What the run and each client thread are waiting on, for the watchdog.
pub static WAITING: Mutex<Option<BTreeMap<String, String>>> = Mutex::new(None);

/// `AUTHORITY_SIM_WATCHDOG=1`: per `(node, timer kind)`, the last fire's
/// simulated clock, the count, and the smallest interval between fires
/// (a timer re-armed at no delay shows as 0 here).
pub type TimerGaps = BTreeMap<String, (i64, u64, i64)>;
pub static TIMER_GAPS: Mutex<Option<TimerGaps>> = Mutex::new(None);

pub fn note_waiting(who: &str, what: String) {
    if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
        WAITING
            .lock()
            .unwrap()
            .get_or_insert_with(BTreeMap::new)
            .insert(who.to_string(), what);
    }
}

/// A commit the sim recorded when a node published: what the checker
/// compares against the log replayed to `applied`.
#[derive(Debug, Clone)]
pub struct CommitRecord {
    pub node: NodeId,
    pub seq: u64,
    pub applied: Seq,
    pub dump: Vec<(Vec<u8>, Vec<u8>)>,
}

/// What the tests can observe of a node's core, refreshed after every
/// event.
#[derive(Debug, Clone, Default)]
pub struct CoreView {
    pub stats: Stats,
    pub held_epoch: Option<u64>,
    pub lost: bool,
    pub gate_pending: bool,
    pub job: Option<constellation_authority::core::JobKind>,
    pub job_phase: Option<String>,
    pub clients_in_flight: usize,
    pub next_seq: Seq,
    pub events_handled: u64,
    pub applied_seq: Seq,
    pub journal_len: u64,
    pub speculation: constellation_meta::SpeculationCounts,
    /// Plan 30 §M9.
    pub ack: constellation_authority::core::AckView,
    /// Plan 30 §M10: what this node would claim in an epoch proposal, and
    /// the authority it has: a continuation-epoch hold, or an S3 lease
    /// (epoch, expiry) it believes it holds and has not lost.
    pub claim: EpochClaimView,
    pub epoch_held: bool,
    /// `(epoch, the lease's end on the node's honour clock, the same end on
    /// its wait clock)` of an S3 lease this node holds (the ends are 0 when
    /// its body is not this incarnation's: unusable).
    pub s3_held: Option<(u64, i64, i64)>,
    /// Plan 30 §M11.
    pub delegation: constellation_authority::core::DelegView,
    /// Plan 30 §M14: lock requests in flight (node side), waiters parked
    /// and recalls outstanding (owner side).
    pub lock_requests: usize,
    pub lock_waiters: usize,
    pub lock_recalls: usize,
}

/// Plan 30 §M10: a node's continuation-epoch state, as the sim's epoch
/// coordinator (`epochs.rs`) and the node's own `EpochClose` /
/// `EpochFlushed` actions move it (the daemon's `EpochManager`).
#[derive(Debug, Clone, Default)]
pub struct SimEpoch {
    pub open: bool,
    pub active: bool,
    pub frozen: bool,
    pub flushing: bool,
    pub base: Seq,
    pub members: Vec<NodeId>,
    pub carrier: Option<Carrier>,
    pub stale_below: u64,
}

impl SimEpoch {
    pub fn control(&self) -> Control {
        Control::Epoch {
            open: self.open,
            active: self.active,
            frozen: self.frozen,
            flushing: self.flushing,
            base: self.base,
            members: self.members.clone(),
            carrier: self.carrier,
            stale_below: self.stale_below,
        }
    }
}

pub struct Shared {
    replies: Mutex<HashMap<Rid, oneshot::Sender<ClientReply>>>,
    controls: Mutex<HashMap<OpId, oneshot::Sender<Result<ControlOk, String>>>>,
    next_control: AtomicU64,
    view: Mutex<CoreView>,
    pub incarnation: AtomicU32,
    next_seq: AtomicU64,
    pub alive: AtomicBool,
    paused_until: Mutex<Option<tokio::time::Instant>>,
    /// When the last pause began.
    paused_since: Mutex<Option<tokio::time::Instant>>,
    /// A runtime lag (`FaultKind::RuntimeLag`): until when the node's
    /// runtime is held up, and until when what peers send it is not yet
    /// stamped.
    lag: Mutex<Option<(tokio::time::Instant, tokio::time::Instant)>>,
    /// When the current runtime lag began.
    lag_since: Mutex<Option<tokio::time::Instant>>,
    /// What the off-core heartbeat beats to (the daemon's
    /// `AliveTargets`): the epoch and expiry of the lease this node
    /// holds, `Core::beat_guard_wait_ms` and `Core::liveness_targets`,
    /// after every event.
    liveness: Mutex<Option<Liveness>>,
    pub conflict_copies: AtomicU64,
    /// Plan 43 C7-F: the length of every takeover floor this node armed
    /// (simulated ms from the event that set it to its end), and the end
    /// last seen.
    pub floor_ms: Mutex<Vec<u64>>,
    floor_seen: AtomicI64,
    /// Plan 43 C7-F F-a: the beat guard published last, per epoch of this
    /// node's tenure `(epoch, guard, base)`, and how many times a finite
    /// guard rose after it had been published as a finite non-zero one (it
    /// may rise by what protects a lease-capped right: a subtree grace, a
    /// hold-era grant; `guard_base_rises` counts the rest, which must not).
    /// 0 → value is the acquisition setting a quarantine, not a rise.
    guard_seen: Mutex<(u64, i64, i64)>,
    pub guard_rises: AtomicU64,
    /// Rises of the guard's *base* (`Core::beat_guard_parts`: the floor and
    /// the quarantines), which are not explained by a subtree grace or a
    /// hold-era grant: must stay 0.
    pub guard_base_rises: AtomicU64,
    /// Rids whose effects were rolled back at some point (see
    /// `history.rs`): read from the replay queue whenever the core's
    /// rollback counters move.
    pub tentative: Mutex<BTreeSet<Rid>>,
    /// The rids whose `Refused` rows this node's unshipped journal held
    /// after its last event. A rollback drops a stranded refusal (its
    /// `completed` row goes with it; it is never replayed), so one that
    /// is gone after a rollback was answered from a decision the log
    /// never carried — tentative, like a rolled-back write (`locks-faults`
    /// seeds 196041, 204913: `Local` holders cut off or crashed past
    /// their lease; the generic tester could not place their refusals).
    /// So is one whose rid the log carries executed after the rollback:
    /// the same rid, forwarded to the root earlier and parked there, ran
    /// once the generation that refused it ended (`delegated-long-
    /// delegation`: a delegate cut from its root past its delegation
    /// refused a requester's resubmission).
    pub journaled_refusals: Mutex<BTreeSet<Rid>>,
    pub commits: Arc<Mutex<Vec<CommitRecord>>>,
    /// Plan 30 §M10.
    pub epoch: Mutex<SimEpoch>,
    /// M12 round 2: ops the modelled FUSE fast path executed here.
    pub fast_path_executed: AtomicU64,
    /// Every `InDoubt` reply of any incarnation (see `NodeEnv`).
    pub in_doubt: InDoubtLog,
    /// Plan 43 C0: deposition rebuilds (`Action::RebuildReplica`) this
    /// incarnation ran, and how many of them failed.
    pub rebuilds_ok: AtomicU64,
    pub rebuilds_failed: AtomicU64,
    /// The most backups this node's lease listed at once (`backup-two`).
    pub max_backups: AtomicU64,
    /// F1 (`StallStep`): the next event this node handles takes its `now`,
    /// then waits this long (ms) before the core handles it — a step that
    /// waited in the store, or a process stopped between its clock read
    /// and its writes.
    stall_next: Mutex<Option<u64>>,
    /// F1 (`WriterLockHold`): the next fast-path write here carrying a
    /// fencing token passes its check and then waits `(.1)` ms for the
    /// store's writer lock, while the core's next step waits `(.0)` ms
    /// behind the same slow writer (and goes first after it: priority
    /// writes).
    hold_next: Mutex<Option<(u64, u64)>>,
    /// Writes held that way, and those of them then refused at the write
    /// (`LockLapsed`) or executed (`Accepted`).
    pub held_writes: AtomicU64,
    pub held_refused: AtomicU64,
    pub held_accepted: AtomicU64,
    /// F1 (a held delegate write): the write parked inside its
    /// transaction, pinned, with the store's writer lock, until a writer
    /// that waits for that lock (`Meta::set_writer_wait_hook_for_tests`)
    /// or its time lets it go.
    held_ctl: Mutex<Option<HeldCtl>>,
    /// F1 (`DelegateWriteHold`): cut this delegate from `(.0)` (the root)
    /// for `(.1)` ms once its held write is pinned.
    hold_partition: Mutex<Option<(NodeId, u64)>>,
    /// F1: every held write that landed (executed), `(rid, simulated ms)`,
    /// for the ghost's check that none landed inside another node's
    /// critical section (`LockGhost::check_landings`).
    pub landed: Mutex<Vec<(Rid, u64)>>,
    /// The off-core delegation renewal lane's view of the driver
    /// (`deleg_lane_loop`, the daemon's `OffCore`): when its current step
    /// began, on this node's clock (0: none — only a `StallStep` is
    /// modelled as a stuck step), the certificates its core published
    /// after its last event, and the urgent lane the lane's registrations
    /// go to.
    busy_since: AtomicI64,
    /// When the lane thread's next tick is due, and the last freeze it saw
    /// end (the daemon's `OffCore::beat_due` / `beat_freeze`): a paused
    /// process skips its ticks, and the driver, waking first, reads the
    /// missed one (`deleg_lane::freezes`).
    beat_due: AtomicI64,
    beat_freeze: AtomicU64,
    /// The root's latest grant per generation and round, tapped off the bus
    /// as the answer arrives (the daemon's `OffCore::granted_ttl`).
    granted_ttl: Mutex<HashMap<(u64, u64), (NodeId, u64)>>,
    deleg_certs: Mutex<Vec<constellation_authority::DelegRenewCert>>,
    urgent: mpsc::UnboundedSender<Event>,
}

/// See `Shared::held_ctl`.
struct HeldCtl {
    go: std::sync::mpsc::Sender<bool>,
    committed: std::sync::mpsc::Receiver<()>,
    thread: std::thread::ThreadId,
}

/// The writer-wait hook: a writer other than the held write's own thread
/// waits for its commit, as every writer waits for a slow commit holding
/// the lock.
fn release_held_ctl(shared: &Shared) {
    let ctl = {
        let mut g = shared.held_ctl.lock().unwrap();
        if g.as_ref()
            .is_some_and(|c| c.thread == std::thread::current().id())
        {
            return;
        }
        g.take()
    };
    if let Some(c) = ctl {
        let _ = c.go.send(true);
        let _ = c.committed.recv();
    }
}

/// Every `InDoubt` reply a node's core sent: `(node, rid, the log
/// sequence its replica had applied at that moment)`. An in-doubt answer
/// for a rid whose completion was already in that applied prefix told a
/// client `EIO` for a write it could have been told had landed
/// (`check::check_in_doubt_answers`).
///
/// Plan 43 C0: the fourth field is the oracles' tick of the answer (I4:
/// "after" is exact against the clients' invocations).
pub type InDoubtLog = Arc<Mutex<Vec<(NodeId, Rid, u64, u64)>>>;

pub struct NodeHandle {
    pub id: NodeId,
    pub meta: Arc<Meta>,
    pub shared: Arc<Shared>,
    /// This node's clock (plan 30 §M8: possibly skewed).
    pub clock: Clock,
    tx: mpsc::UnboundedSender<Event>,
    task: tokio::task::JoinHandle<()>,
    /// Plan 43 C0: where the first submission of each rid is noted (I4).
    oracles: Arc<super::oracle::Oracles>,
}

pub struct NodeEnv {
    pub bucket: Arc<Bucket>,
    pub bus: Arc<Bus>,
    pub clock: Clock,
    pub commits: Arc<Mutex<Vec<CommitRecord>>>,
    pub in_doubt: InDoubtLog,
    pub config: Arc<dyn Fn(NodeId, u32) -> Config + Send + Sync>,
    /// See `SimConfig::panic_after_events`.
    pub panic_after_events: Option<u64>,
    /// See `SimConfig::fast_path`.
    pub fast_path: super::run::FastPath,
    /// Plan 30 §M8: every node's clock is off by up to this (ms), by a
    /// seeded constant per node.
    pub clock_skew_ms: i64,
    /// See `SimConfig::clock_offsets`.
    pub clock_offsets: Vec<(NodeId, i64)>,
    /// Plan 43 C7-H1, C7a: each node's host clock state (its cumulative
    /// wall step, its oscillator's rate, suspends and boots), created on
    /// first use. It outlives the node's incarnations: a host's clock does
    /// not reset when the process restarts. `FaultKind::ClockStep` adds to
    /// its step (only the node's WALL reading moves), `RateSkew`, `Suspend`
    /// and `RebootNode` change the oscillator (see `sim/clock.rs`).
    pub clocks: Mutex<HashMap<NodeId, Arc<NodeClockState>>>,
    /// See `SimConfig::rate_budget_ppm`.
    pub rate_budget_ppm: u32,
    /// See `SimConfig::node_rates`.
    pub node_rates: Vec<(NodeId, i64)>,
    /// See `SimConfig::wall_follows_rate`.
    pub wall_follows_rate: bool,
    pub seed: u64,
    /// Fix "capture under an epoch hold": the modelled chunks.
    pub world: Arc<ChunkWorld>,
    /// F1's non-vacuity knob (`SimConfig::tag_check_at_write`).
    pub tag_check_at_write: bool,
    /// F1's other one (`SimConfig::settle_tagged_writes`).
    pub settle_tagged_writes: bool,
    /// See `SimConfig::holder_beats`.
    pub holder_beats: super::run::HolderBeats,
    /// See `SimConfig::timers_first_after_lag`.
    pub timers_first_after_lag: bool,
    /// See `SimConfig::beats_credited`.
    pub beats_credited: bool,
    /// See `SimConfig::beats_lost_in_lag`.
    pub beats_lost_in_lag: bool,
    /// See `SimConfig::deleg_lane`.
    pub deleg_lane: bool,
    /// Plan 43 C0: the commit-rule oracles' shared plumbing.
    pub oracles: Arc<super::oracle::Oracles>,
    /// See `SimConfig::rebuild_stub`.
    pub rebuild_stub: bool,
    /// See `SimConfig::plant_ungated_fast_path`.
    pub plant_ungated_fast_path: bool,
}

impl NodeEnv {
    /// Node `id`'s host clock state (shared by its incarnations): its wall
    /// step, rate, suspends and boots.
    pub fn clock_state_of(&self, id: NodeId) -> Arc<NodeClockState> {
        self.clocks
            .lock()
            .unwrap()
            .entry(id)
            .or_insert_with(|| {
                let ppm = self
                    .node_rates
                    .iter()
                    .filter(|(n, _)| *n == id)
                    .map(|(_, ppm)| ppm)
                    .sum();
                NodeClockState::new(id, ppm, self.wall_follows_rate)
            })
            .clone()
    }

    /// Node `id`'s clock for incarnation `incarnation`: true time, off by a
    /// seeded constant within `±clock_skew_ms` and by its `clock_offsets`
    /// entry if it has one, and by the node's step. A new `ProtocolClock`
    /// per incarnation (plan 43 C7-H1: its id is `node << 32 |
    /// incarnation`; HONOUR and WAIT restart at its wall).
    pub fn clock_of(&self, id: NodeId, incarnation: u32) -> Clock {
        let offset = self.clock_offset_of(id);
        self.clock.for_node(
            offset,
            self.clock_state_of(id),
            (id << 32) | incarnation as u64,
            self.rate_budget_ppm,
        )
    }

    /// Node `id`'s constant clock error: a seeded draw within
    /// `±clock_skew_ms`, plus its `clock_offsets` entry if it has one.
    fn clock_offset_of(&self, id: NodeId) -> i64 {
        let fixed: i64 = self
            .clock_offsets
            .iter()
            .filter(|(n, _)| *n == id)
            .map(|(_, ms)| ms)
            .sum();
        if self.clock_skew_ms == 0 {
            return fixed;
        }
        let h = (self.seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let span = 2 * self.clock_skew_ms + 1;
        (h % span as u64) as i64 - self.clock_skew_ms + fixed
    }
}

impl NodeHandle {
    /// Start (or restart) node `id` over `meta`. A fresh `Meta` is a node
    /// with no durable state (a new mount); an existing one is a restart
    /// with its journal intact.
    /// Plan 30 §M10: a restart keeps the node's persisted epoch state
    /// (the daemon's `EpochManager` reloads it from the replica).
    pub fn start_with(
        env: &NodeEnv,
        id: NodeId,
        meta: Option<Arc<Meta>>,
        epoch: SimEpoch,
    ) -> NodeHandle {
        let meta = match meta {
            Some(m) => m,
            None => {
                let m = Meta::open_in_memory().expect("meta");
                m.set_node_prefix(id).expect("prefix");
                // A restart without the journal keeps no queue either.
                env.oracles.identity.lock().unwrap().wipe(id);
                Arc::new(m)
            }
        };
        let incarnation = meta.bump_incarnation().expect("incarnation");
        // One clock for the incarnation: the driver, the meta's token check,
        // the beat and lane threads and the handle all read it.
        let clock = env.clock_of(id, incarnation);
        // Plan 30 §M6: the session state is the process's; a restart is a
        // new FUSE session.
        meta.session().reset();
        let (tx, rx) = mpsc::unbounded_channel();
        let (urgent_tx, urgent_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            replies: Mutex::new(HashMap::new()),
            controls: Mutex::new(HashMap::new()),
            next_control: AtomicU64::new(1 << 48),
            view: Mutex::new(CoreView::default()),
            incarnation: AtomicU32::new(incarnation),
            next_seq: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            paused_until: Mutex::new(None),
            paused_since: Mutex::new(None),
            lag: Mutex::new(None),
            lag_since: Mutex::new(None),
            liveness: Mutex::new(None),
            conflict_copies: AtomicU64::new(0),
            floor_ms: Mutex::new(Vec::new()),
            floor_seen: AtomicI64::new(0),
            guard_seen: Mutex::new((0, 0, 0)),
            guard_rises: AtomicU64::new(0),
            guard_base_rises: AtomicU64::new(0),
            tentative: Mutex::new(BTreeSet::new()),
            journaled_refusals: Mutex::new(BTreeSet::new()),
            commits: env.commits.clone(),
            epoch: Mutex::new(epoch),
            fast_path_executed: AtomicU64::new(0),
            in_doubt: env.in_doubt.clone(),
            rebuilds_ok: AtomicU64::new(0),
            rebuilds_failed: AtomicU64::new(0),
            max_backups: AtomicU64::new(0),
            stall_next: Mutex::new(None),
            hold_next: Mutex::new(None),
            held_writes: AtomicU64::new(0),
            held_refused: AtomicU64::new(0),
            held_accepted: AtomicU64::new(0),
            held_ctl: Mutex::new(None),
            hold_partition: Mutex::new(None),
            landed: Mutex::new(Vec::new()),
            busy_since: AtomicI64::new(0),
            beat_due: AtomicI64::new(0),
            beat_freeze: AtomicU64::new(0),
            granted_ttl: Mutex::new(HashMap::new()),
            deleg_certs: Mutex::new(Vec::new()),
            urgent: urgent_tx,
        });
        {
            let shared = shared.clone();
            meta.set_writer_wait_hook_for_tests(Some(Arc::new(move || release_held_ctl(&shared))));
        }
        // F1: the token check at the write reads this node's clock, as
        // the daemon's reads its wall clock.
        {
            let clock = clock.clone();
            meta.locks().set_clock(Arc::new(move || clock.now()));
            meta.locks()
                .set_tag_check_at_write_for_tests(env.tag_check_at_write);
            meta.locks().set_settle_for_tests(env.settle_tagged_writes);
        }
        env.bus.attach(id, tx.clone());
        {
            let shared = shared.clone();
            env.bus.set_tap(
                id,
                Arc::new(move |from: NodeId, msg: &PeerMsg| {
                    if let PeerMsg::DelegRenewed {
                        gen, round, ttl_ms, ..
                    } = msg
                    {
                        if *ttl_ms > 0 {
                            shared
                                .granted_ttl
                                .lock()
                                .unwrap()
                                .insert((*gen, *round), (from, *ttl_ms));
                        }
                    }
                }),
            );
        }
        env.world.register(id, meta.clone());
        let mut cfg = (env.config)(id, incarnation);
        cfg.clock_id = clock.id();
        cfg.clock_rate_budget_ppm = env.rate_budget_ppm;
        env.oracles.register(
            id,
            meta.clone(),
            cfg.backup_rtt_budget_ms,
            cfg.backup_stable_ms,
        );
        let beat_every_ms = cfg.backup_heartbeat_ms.max(1);
        let lane = env.deleg_lane && cfg.delegation && cfg.p2p;
        let driver = Driver {
            id,
            meta: meta.clone(),
            core: Core::new(cfg),
            store: env.bucket.handle(id),
            bucket: env.bucket.clone(),
            world: env.world.clone(),
            bus: env.bus.clone(),
            step_now: clock.now(),
            clock: clock.clone(),
            tx: tx.clone(),
            rx,
            urgent_rx,
            shared: shared.clone(),
            rolled_back_seen: (0, 0),
            journal_seen: None,
            timers: HashSet::new(),
            timer_kinds: HashMap::new(),
            bootstrap: None,
            deferred: Vec::new(),
            events_handled: 0,
            panic_after_events: env.panic_after_events,
            fast_path: env.fast_path,
            held: None,
            ready: std::collections::VecDeque::new(),
            held_peers: std::collections::VecDeque::new(),
            peers_held_until: None,
            lag_queue: Vec::new(),
            lag_pending: None,
            freeze_handed: (0, 0),
            stamps_first: !env.timers_first_after_lag,
            credit_margin: env
                .beats_credited
                .then(|| (env.config)(id, 1).expiry_margin_ms as i64),
            beats_lost_in_lag: env.beats_lost_in_lag,
            oracles: env.oracles.clone(),
            forwards: HashMap::new(),
            rebuild_stub: env.rebuild_stub,
            plant_ungated_fast_path: env.plant_ungated_fast_path,
        };
        let task = tokio::spawn(driver.run());
        if lane {
            tokio::spawn(deleg_lane_loop(
                id,
                shared.clone(),
                env.bus.clone(),
                clock.clone(),
                beat_every_ms,
            ));
        }
        if env.holder_beats != super::run::HolderBeats::Off {
            tokio::spawn(beat_loop(
                id,
                shared.clone(),
                env.bus.clone(),
                clock.clone(),
                beat_every_ms,
                env.holder_beats == super::run::HolderBeats::Runtime,
            ));
        }
        NodeHandle {
            id,
            meta,
            shared,
            clock,
            tx,
            task,
            oracles: env.oracles.clone(),
        }
    }

    pub fn next_rid(&self) -> Rid {
        Rid {
            node: self.id,
            incarnation: self.shared.incarnation.load(Ordering::SeqCst),
            seq: self.shared.next_seq.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }

    pub fn alive(&self) -> bool {
        self.shared.alive.load(Ordering::SeqCst)
    }

    /// Submit a client op. The receiver errors if the node dies before
    /// answering (a FUSE thread's reply channel going away).
    pub fn submit(&self, rid: Rid, op: MutateOp) -> oneshot::Receiver<ClientReply> {
        self.submit_tagged(rid, op, LockTag::NONE)
    }

    /// [`Self::submit`] with a fencing token (plan 30 §M14 phase 2): the
    /// grants the submitting client holds.
    pub fn submit_tagged(
        &self,
        rid: Rid,
        op: MutateOp,
        tag: LockTag,
    ) -> oneshot::Receiver<ClientReply> {
        let (tx, rx) = oneshot::channel();
        if !self.alive() {
            return rx;
        }
        self.oracles.note_invoke(rid);
        self.shared.replies.lock().unwrap().insert(rid, tx);
        let _ = self.tx.send(Event::Submit {
            rid,
            op,
            policy: Policy::Client,
            tag,
        });
        rx
    }

    pub fn control(&self, req: Control) -> oneshot::Receiver<Result<ControlOk, String>> {
        let (tx, rx) = oneshot::channel();
        let op = OpId(self.shared.next_control.fetch_add(1, Ordering::SeqCst));
        self.shared.controls.lock().unwrap().insert(op, tx);
        let _ = self.tx.send(Event::Control { op, req });
        rx
    }

    pub fn view(&self) -> CoreView {
        self.shared.view.lock().unwrap().clone()
    }

    /// Plan 30 §M10: report the node's epoch state to its core.
    pub fn report_epoch(&self) {
        let req = self.shared.epoch.lock().unwrap().control();
        drop(self.control(req));
    }

    /// Fail-stop. In-flight S3 requests still land (their tasks hold the
    /// store), exactly like a killed process's outstanding PUTs.
    pub fn crash(&self, bus: &Bus) {
        self.shared.alive.store(false, Ordering::SeqCst);
        bus.detach(self.id);
        self.task.abort();
        self.shared.replies.lock().unwrap().clear();
        self.shared.controls.lock().unwrap().clear();
    }

    /// A stopped process whose timers keep running: events queue up and
    /// are handled, late, once the pause ends.
    pub fn pause_until(&self, until: tokio::time::Instant) {
        *self.shared.paused_until.lock().unwrap() = Some(until);
        *self.shared.paused_since.lock().unwrap() = Some(tokio::time::Instant::now());
    }

    /// Whether a pause holds this node's event loop now (its view is the
    /// one from before the pause, and it handles nothing).
    pub fn paused(&self) -> bool {
        self.shared
            .paused_until
            .lock()
            .unwrap()
            .is_some_and(|until| tokio::time::Instant::now() < until)
    }

    /// Plan 43 C7a: this node's core holds a takeover floor that has not
    /// ended on its wait clock (as of its last event).
    pub fn in_takeover_floor(&self) -> bool {
        let floor = self.shared.floor_seen.load(Ordering::SeqCst);
        floor != 0 && floor > self.clock.wait().0
    }

    /// F1 (`StallStep`): see `Shared::stall_next`.
    pub fn stall_next_step(&self, for_ms: u64) {
        *self.shared.stall_next.lock().unwrap() = Some(for_ms);
    }

    /// F1 (`WriterLockHold`): see `Shared::hold_next`.
    pub fn hold_next_tagged_write(&self, core_ms: u64, write_ms: u64) {
        *self.shared.hold_next.lock().unwrap() = Some((core_ms, write_ms));
    }

    /// F1 (`DelegateWriteHold`): the next tagged write this delegate
    /// executes on its fast path is held inside its transaction for
    /// `write_ms` (unless a writer lets it commit first), and the node is
    /// cut from `root` for `partition_ms` once it is.
    pub fn hold_next_delegate_write(&self, root: NodeId, partition_ms: u64, write_ms: u64) {
        *self.shared.hold_partition.lock().unwrap() = Some((root, partition_ms));
        *self.shared.hold_next.lock().unwrap() = Some((0, write_ms));
    }

    /// `FaultKind::RuntimeLag`: hold the node's runtime up until
    /// `until`, and leave what peers send it unstamped until
    /// `peers_until`.
    pub fn runtime_lag(&self, until: tokio::time::Instant, peers_until: tokio::time::Instant) {
        *self.shared.lag.lock().unwrap() = Some((until, peers_until));
        *self.shared.lag_since.lock().unwrap() = Some(tokio::time::Instant::now());
    }

    /// Paused, and since at least `d` ago.
    pub fn paused_for(&self, d: std::time::Duration) -> bool {
        self.paused()
            && self
                .shared
                .paused_since
                .lock()
                .unwrap()
                .is_some_and(|since| since + d <= tokio::time::Instant::now())
    }
}

struct Driver {
    id: NodeId,
    meta: Arc<Meta>,
    core: Core,
    store: Arc<dyn object_store::ObjectStore>,
    bucket: Arc<Bucket>,
    world: Arc<ChunkWorld>,
    bus: Arc<Bus>,
    clock: Clock,
    /// The `Now` the core was last stepped with: the anchor of the
    /// relative timers the step arms (`Action::SetTimer { after_ms }` counts
    /// from the reading, not from the dispatch: a step that waited between
    /// its clock read and its actions, `StallStep`, still fires its timers
    /// when the reading says).
    step_now: constellation_authority::Now,
    tx: mpsc::UnboundedSender<Event>,
    rx: mpsc::UnboundedReceiver<Event>,
    /// The daemon's urgent lane, as far as the delegation renewal lane
    /// uses it: its registrations are taken ahead of everything queued
    /// ([`Driver::next_event`]).
    urgent_rx: mpsc::UnboundedReceiver<Event>,
    shared: Arc<Shared>,
    rolled_back_seen: (u64, u64),
    /// `(journal tip, journal length)` when `Shared::journaled_refusals`
    /// was last taken.
    journal_seen: Option<(u64, u64)>,
    timers: HashSet<constellation_authority::TimerId>,
    /// What each pending timer is for (the watchdog's histogram names
    /// the kind, not just "Timer").
    timer_kinds: HashMap<constellation_authority::TimerId, constellation_authority::TimerKind>,
    /// Mount-time bootstrap (`shipper::bootstrap`): the replica tails to
    /// head before any client op is admitted. Submits arriving meanwhile
    /// wait here.
    bootstrap: Option<OpId>,
    deferred: Vec<Event>,
    /// Events this driver has handled, for `panic_after_events`.
    events_handled: u64,
    panic_after_events: Option<u64>,
    fast_path: super::run::FastPath,
    /// F1: the fast-path write held behind the slow writer, if any.
    held: Option<HeldWrite>,
    /// A runtime lag's aftermath ([`Driver::next_event`]): what goes
    /// next, before the queue; what peers sent, held until
    /// `peers_held_until`.
    ready: std::collections::VecDeque<Event>,
    held_peers: std::collections::VecDeque<Event>,
    peers_held_until: Option<tokio::time::Instant>,
    /// A runtime lag in progress: what arrived during it, and when it
    /// ends ([`Driver::finish_lag`]).
    lag_queue: Vec<Event>,
    lag_pending: Option<tokio::time::Instant>,
    /// The latest freeze of the lane thread handed to the core.
    freeze_handed: (i64, i64),
    /// After a lag, what the daemon's P2P handlers stamped meanwhile goes
    /// to the core before the timers ([`Driver::finish_lag`]).
    stamps_first: bool,
    /// `SimConfig::beats_credited`: the expiry margin a numbered beat's
    /// arrival stamp is credited with.
    credit_margin: Option<i64>,
    /// See `SimConfig::beats_lost_in_lag`.
    beats_lost_in_lag: bool,
    /// Plan 43 C0: the oracles, and the rid behind each forwarded request
    /// this driver handed its core (`(requester, req)`), for I1 at the
    /// `MutateReply`'s emission.
    oracles: Arc<super::oracle::Oracles>,
    forwards: HashMap<(NodeId, OpId), Rid>,
    rebuild_stub: bool,
    plant_ungated_fast_path: bool,
}

/// What the off-core heartbeat beats to: `(epoch, wall expiry, honour end,
/// wait end, guard (wait), targets)`: what the beat thread reads (the
/// daemon's `AliveTargets`, plan 43 C7-H1).
type Liveness = (u64, i64, i64, i64, i64, Vec<NodeId>);

/// F1 in the simulation (`WriterLockHold`): a fast-path write that passed
/// its fencing-token check and waits for the store's writer lock behind a
/// slow writer. It runs `execute_tagged` on an OS thread of its own, parked
/// by `set_before_write_hook_for_tests` between that check and its
/// transaction, holding nothing; the driver goes on meanwhile and lets it
/// go at `release_at`, waiting for it to finish (so nothing else of the
/// simulation runs alongside its transaction: deterministic).
struct HeldWrite {
    rid: Rid,
    op: MutateOp,
    epoch: u64,
    /// A delegate's write (parked inside its transaction, `Shared::held_ctl`
    /// holds its go), and its generation.
    deleg: Option<u64>,
    shared: Arc<Shared>,
    release_at: tokio::time::Instant,
    go: Option<std::sync::mpsc::Sender<bool>>,
    phase: std::sync::mpsc::Receiver<HeldPhase>,
    thread: Option<std::thread::JoinHandle<()>>,
}

enum HeldPhase {
    /// Past the check before the op (or, a delegate's, pinned inside its
    /// transaction), waiting at the hook.
    Checked,
    /// The outcome, a delegate's stream index, and when its transaction
    /// was over (simulated ms).
    Done(MutateOutcome, Option<u64>, u64),
}

impl HeldWrite {
    /// Let it go (if a writer has not already) and wait for its outcome.
    fn finish(mut self) -> (MutateOutcome, Option<u64>, u64) {
        if let Some(go) = self.go.take() {
            let _ = go.send(true);
        }
        if self.deleg.is_some() {
            release_held_ctl(&self.shared);
        }
        let outcome = loop {
            match self.phase.recv() {
                Ok(HeldPhase::Done(o, idx, at)) => break (o, idx, at),
                Ok(HeldPhase::Checked) => continue,
                Err(_) => break (MutateOutcome::Errno(constellation_types::Code::Io), None, 0),
            }
        };
        if let Some(t) = self.thread.take() {
            t.join().expect("held write thread");
        }
        outcome
    }
}

impl Drop for HeldWrite {
    /// The node died with the write still waiting: it never runs.
    fn drop(&mut self) {
        if let Some(go) = self.go.take() {
            let _ = go.send(false);
        }
        if let Some(c) = self.shared.held_ctl.lock().unwrap().take() {
            let _ = c.go.send(false);
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn outcome_of(
    r: Result<Vec<constellation_meta::LogRecord>, constellation_meta::MetaError>,
    epoch: u64,
) -> MutateOutcome {
    match r {
        Ok(records) => MutateOutcome::Accepted { epoch, records },
        Err(constellation_meta::MetaError::Conflict) => MutateOutcome::Conflict { manifest: None },
        Err(constellation_meta::MetaError::LockLapsed) => MutateOutcome::LockLapsed,
        Err(e) => MutateOutcome::Errno(e.code()),
    }
}

/// What a peer sent (the P2P receive path, which a runtime lag holds up
/// longest), as opposed to the node's own timers and IO completions.
fn from_a_peer(event: &Event) -> bool {
    matches!(event, Event::Peer { .. } | Event::HolderAlive { .. })
}

/// A beat of the holder's off-core heartbeat (numbered; an unnumbered one
/// is the driver's own hand-over of an arrival stamp).
fn is_numbered_beat(event: &Event) -> bool {
    matches!(event, Event::HolderAlive { seq, .. } if *seq != 0)
}

/// The off-core heartbeat (`holder_alive_loop` in the daemon): every
/// beat interval, while the node is alive, not stopped (`pause_until`:
/// the whole process) and holding an unexpired lease, a numbered beat to
/// each of its liveness targets. `on_runtime`: a task of the node's
/// runtime, as it was, held up by a runtime lag like everything else;
/// otherwise its own thread, which a runtime lag does not touch.
async fn beat_loop(
    id: NodeId,
    shared: Arc<Shared>,
    bus: Arc<Bus>,
    clock: Clock,
    every_ms: u64,
    on_runtime: bool,
) {
    let mut seq = 0u64;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(every_ms)).await;
        if !shared.alive.load(Ordering::SeqCst) {
            return;
        }
        let now = tokio::time::Instant::now();
        if shared
            .paused_until
            .lock()
            .unwrap()
            .is_some_and(|until| now < until)
        {
            continue;
        }
        if on_runtime {
            let lag = *shared.lag.lock().unwrap();
            if let Some((until, _)) = lag {
                if now < until {
                    // The task wakes when the runtime gets to it: once,
                    // at the lag's end, not once per missed interval.
                    tokio::time::sleep_until(until).await;
                    continue;
                }
            }
        }
        let Some((epoch, until_wall, until_honour, until_wait, guard, targets)) =
            shared.liveness.lock().unwrap().clone()
        else {
            continue;
        };
        // One reading per beat (plan 43 C7-H1, R7): the send stamp, the
        // lease's end and the guard are all of this node's wait clock, with
        // its id; the beats stop at the honour end.
        let now = clock.now();
        if now.honour.0 >= until_honour {
            continue;
        }
        seq += 1;
        for to in targets {
            bus.send_beat(
                (id, to),
                epoch,
                seq,
                super::bus::BeatStamps {
                    clock: clock.id(),
                    sent: constellation_authority::beat_sent_ms(now),
                    holder_expires: until_wait,
                    guard,
                    expires: until_wall,
                    echo: now.wait.0,
                },
            );
        }
    }
}

/// The off-core delegation renewal lane (`DelegLane` in the daemon's
/// `holder_alive_loop`): every beat interval, while the node is alive and
/// not stopped as a process (`pause_until`), if its driver has been stuck
/// in a step for `DELEG_LANE_STUCK_MS` and not past the hung bound, the
/// renewals that are due ([`constellation_authority::deleg_lane::due`],
/// from the certificates its core published after its last event) are
/// registered with the core on the urgent lane and sent to the root over
/// the bus. Only a `StallStep` is a stuck step here. Its own thread: a
/// runtime lag does not hold it up.
async fn deleg_lane_loop(
    id: NodeId,
    shared: Arc<Shared>,
    bus: Arc<Bus>,
    clock: Clock,
    every_ms: u64,
) {
    use constellation_authority::deleg_lane::{due, with_granted_ttl, FREEZE_MIN_MS, LANE_REQ_BIT};
    let mut seq = 0u64;
    let mut sent: HashMap<(u64, u64), constellation_authority::HonourMs> = HashMap::new();
    // When the previous tick expected this one (tokio's clock).
    let mut due_at: Option<tokio::time::Instant> = None;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(every_ms)).await;
        if !shared.alive.load(Ordering::SeqCst) {
            return;
        }
        if shared
            .paused_until
            .lock()
            .unwrap()
            .is_some_and(|until| tokio::time::Instant::now() < until)
        {
            continue;
        }
        let woke = tokio::time::Instant::now();
        let now = clock.now();
        // This tick ran; if it ran late, the process was frozen meanwhile.
        // Measured on the runtime's monotonic clock, as the daemon's
        // heartbeat thread does (`holder_alive_loop`): a wall step between
        // two ticks would invent or hide a freeze (plan 43 C7-H1).
        if let Some(due) = due_at {
            let late = woke.saturating_duration_since(due).as_millis() as i64;
            if late >= FREEZE_MIN_MS {
                shared.beat_freeze.store(
                    constellation_authority::deleg_lane::pack_freeze(now.wait.0 - late, late),
                    Ordering::SeqCst,
                );
            }
        }
        due_at = Some(woke + std::time::Duration::from_millis(every_ms));
        shared
            .beat_due
            .store(now.wait.0 + every_ms as i64, Ordering::SeqCst);
        let busy = shared.busy_since.load(Ordering::SeqCst);
        let stuck_ms = if busy != 0 { now.wait.0 - busy } else { 0 };
        let mut certs = shared.deleg_certs.lock().unwrap().clone();
        for c in certs.iter_mut() {
            if let Some((from, ttl)) = shared.granted_ttl.lock().unwrap().get(&(c.gen, c.round)) {
                if *from == c.root {
                    *c = with_granted_ttl(c, *ttl);
                }
            }
        }
        sent.retain(|k, _| certs.iter().any(|c| (c.gen, c.round) == *k));
        for c in due(&certs, now.honour, stuck_ms, SIM_HOLDER_STALL_MS, &sent) {
            seq += 1;
            let req = OpId(LANE_REQ_BIT | seq);
            if shared
                .urgent
                .send(Event::DelegRenewSentOffCore {
                    gen: c.gen,
                    round: c.round,
                    to: c.root,
                    req,
                    sent: now.honour,
                })
                .is_err()
            {
                return;
            }
            sent.insert((c.gen, c.round), now.honour);
            bus.send(
                id,
                c.root,
                PeerMsg::DelegRenewOffCore {
                    req,
                    gen: c.gen,
                    round: c.round,
                },
            );
        }
    }
}

/// `holder_stall_ms()`'s default in the daemon: a driver stuck longer is
/// hung, not slow (`CONSTELLATION_HOLDER_STALL_MS`).
const SIM_HOLDER_STALL_MS: i64 = 15_000;

impl Driver {
    async fn run(mut self) {
        let now = self.clock.now();
        let mut out = Vec::new();
        self.step_now = now;
        self.core.start(now, &*self.meta, &mut out);
        self.dispatch(out);
        let bootstrap = OpId(u64::MAX);
        self.bootstrap = Some(bootstrap);
        self.step_now = now;
        let out = self.core.handle(
            now,
            Event::Control {
                op: bootstrap,
                req: Control::TailToHead,
            },
            &*self.meta,
        );
        self.dispatch(out);
        // Plan 30 §M10: a restarted member reports its persisted epoch.
        let epoch = self.shared.epoch.lock().unwrap().clone();
        if epoch.open || epoch.flushing {
            self.step_now = now;
            let out = self.core.handle(
                now,
                Event::Control {
                    op: OpId(self.shared.next_control.fetch_add(1, Ordering::SeqCst)),
                    req: epoch.control(),
                },
                &*self.meta,
            );
            self.dispatch(out);
        }
        self.refresh_view();
        // The driver's directory: the roster and the peer links, refreshed
        // on a ticker like the daemon's registry poll and `probe_all`.
        {
            let (tx, bus, id) = (self.tx.clone(), self.bus.clone(), self.id);
            let oracles = self.oracles.clone();
            let p2p = self.core.config().p2p;
            let clock = self.clock.clone();
            tokio::spawn(async move {
                // Plan 30 §M9: since when each link has been up without a
                // break (the directory's `since`).
                let mut up_since: HashMap<NodeId, constellation_authority::WaitMs> = HashMap::new();
                loop {
                    let nodes = bus.nodes();
                    if tx
                        .send(Event::Roster {
                            write_eligible: nodes.clone(),
                        })
                        .is_err()
                    {
                        return;
                    }
                    let now = clock.now();
                    // With P2P off the directory is empty (`Peers::disabled`).
                    let links: Vec<PeerLink> = nodes
                        .into_iter()
                        .filter(|n| *n != id && p2p)
                        .map(|n| {
                            let connected = bus.linked(id, n);
                            let since = if connected {
                                Some(*up_since.entry(n).or_insert(now.wait))
                            } else {
                                up_since.remove(&n);
                                None
                            };
                            PeerLink {
                                node: n,
                                connected,
                                last_seen: None,
                                rtt_ms: connected.then(|| bus.rtt(id, n)),
                                since,
                            }
                        })
                        .collect();
                    oracles.note_directory(
                        id,
                        links
                            .iter()
                            .filter_map(|l| l.since.map(|s| (l.node, s.0)))
                            .collect(),
                    );
                    if tx.send(Event::Peers { links }).is_err() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            });
        }
        loop {
            // F1: a held fast-path write goes on at its time, between
            // events (`HeldWrite`; `Driver::next_event` wakes for it).
            let Some(mut event) = self.next_event().await else {
                break;
            };
            if self.bootstrap.is_some() && matches!(event, Event::Submit { .. }) {
                self.deferred.push(event);
                continue;
            }
            let paused = *self.shared.paused_until.lock().unwrap();
            if let Some(until) = paused {
                if tokio::time::Instant::now() < until {
                    tokio::time::sleep_until(until).await;
                }
                *self.shared.paused_until.lock().unwrap() = None;
            }
            let now = self.clock.now();
            // The daemon's `Driver::hand_freeze`: a freeze of the lane thread
            // the driver sees as it wakes, before the thread has run again.
            for (from, until) in constellation_authority::deleg_lane::freezes(
                self.shared.beat_freeze.load(Ordering::SeqCst),
                self.shared.beat_due.load(Ordering::SeqCst),
                now.wait.0,
            ) {
                let (hf, hu) = self.freeze_handed;
                if from != hf || until > hu + 200 {
                    self.freeze_handed = (from, until);
                    // (A step like any other: `handle` may emit actions.)
                    self.step_now = now;
                    let actions = self.core.handle(
                        now,
                        Event::OwnFreeze {
                            from: constellation_authority::WaitMs(from),
                            until: constellation_authority::WaitMs(until),
                        },
                        &*self.meta,
                    );
                    self.dispatch(actions);
                }
            }
            // F1 (`StallStep`): this step's `now` is taken; then it waits
            // (in the store, or stopped), and decides with that `now`.
            let stall = self.shared.stall_next.lock().unwrap().take();
            if let Some(ms) = stall {
                tracing::debug!(
                    node = self.id,
                    now = now.wall.0,
                    ms,
                    "sim: step stalled after its clock read"
                );
                // The step is stuck: the delegation renewal lane sees it
                // (`deleg_lane_loop`) from here.
                self.shared.busy_since.store(now.wait.0, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                self.shared.busy_since.store(0, Ordering::SeqCst);
            }
            // A beat's arrival stamp: when this node got to it (the
            // daemon stamps it in its P2P handler; a lag delays both).
            if let Event::HolderAlive {
                at,
                from,
                listed,
                sent,
                seq,
                ..
            } = &mut event
            {
                if at.0 == 0 {
                    *at = now.wait;
                }
                // The credited negative control: the arrival replaced by the
                // credit the daemon no longer counts (it compares the
                // holder's clock reading with this node's).
                if let (Some(margin), true) = (self.credit_margin, *seq != 0 && *sent != 0) {
                    *at = (*at).min(constellation_authority::WaitMs(*sent + margin));
                }
                // Answered as the daemon's P2P handler answers it, when
                // this node gets to it: the holder hears the backup is
                // alive (`Event::BackupAlive`, stamped with the beat's
                // send time) and gives it the slow-backup grace.
                if *listed && *seq != 0 {
                    self.bus.answer_beat((self.id, *from), *seq);
                }
            }
            let debug = tracing::enabled!(tracing::Level::DEBUG);
            let debug_event = if debug {
                format!("{event:?}")
            } else {
                String::new()
            };
            let marks_before = self.core.stats.takeovers
                + self.core.stats.segments_applied
                + self.core.stats.shadows_installed
                + self.core.stats.hints_installed;
            if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
                let text = format!("{event:?}");
                let kind = match &event {
                    Event::Timer { id } => format!("Timer:{:?}", self.timer_kinds.get(id).copied()),
                    _ => text
                        .split(|c: char| !c.is_alphanumeric())
                        .take(2)
                        .collect::<Vec<_>>()
                        .join(":"),
                };
                let mut h = EVENT_HISTOGRAM.lock().unwrap();
                let (clock, hist) = h.get_or_insert_with(|| (0, BTreeMap::new()));
                *clock = now.wall.0 as u64;
                *hist.entry(format!("node{}:{kind}", self.id)).or_default() += 1;
                drop(h);
                if let Event::Timer { .. } = &event {
                    let mut g = TIMER_GAPS.lock().unwrap();
                    let m = g.get_or_insert_with(BTreeMap::new);
                    let e = m.entry(format!("node{}:{kind}", self.id)).or_insert((
                        now.wall.0,
                        0,
                        i64::MAX,
                    ));
                    if e.1 > 0 {
                        e.2 = e.2.min(now.wall.0 - e.0);
                    }
                    e.0 = now.wall.0;
                    e.1 += 1;
                }
                *IN_FLIGHT.lock().unwrap() = Some((self.id, text, std::time::Instant::now()));
            }
            self.events_handled += 1;
            if let Event::Peer {
                from,
                msg: PeerMsg::MutateRequest { req, rid, .. },
            } = &event
            {
                self.forwards.insert((*from, *req), *rid);
            }
            if let Some(actions) = self.fast_path(now, &event) {
                self.dispatch(actions);
                continue;
            }
            if self.id == 1 && self.panic_after_events == Some(self.events_handled) {
                panic!(
                    "injected panic after {} events (SimConfig::panic_after_events)",
                    self.events_handled
                );
            }
            let lock_timer = match &event {
                Event::Timer { id } if debug => self
                    .timer_kinds
                    .get(id)
                    .map(|k| format!("{k:?}"))
                    .filter(|k| k.contains("Lock")),
                _ => None,
            };
            self.before_core(&event);
            self.step_now = now;
            let actions = self.core.handle(now, event, &*self.meta);
            if debug && (debug_event.contains("Lock") || lock_timer.is_some()) {
                let debug_event = match &lock_timer {
                    Some(k) => format!("{debug_event} ({k})"),
                    None => debug_event.clone(),
                };
                // `RUST_LOG=sim=debug`: plan 30 §M14's exchanges, timed.
                let sent: Vec<String> = actions
                    .iter()
                    .filter_map(|a| {
                        let t = format!("{a:?}");
                        t.contains("Lock").then_some(t)
                    })
                    .collect();
                tracing::debug!(
                    node = self.id,
                    now = now.wall.0,
                    event = %debug_event,
                    ?sent,
                    held = ?self.meta.locks().held_all(),
                    grants = ?self.meta.locks().grants_snapshot(),
                    "lock event"
                );
            }
            if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
                *IN_FLIGHT.lock().unwrap() = None;
                {
                    let mut h = EVENT_HISTOGRAM.lock().unwrap();
                    let (_, hist) = h.get_or_insert_with(|| (0, BTreeMap::new()));
                    for a in &actions {
                        let k = match a {
                            Action::S3 { req, .. } => format!(
                                "S3:{}",
                                format!("{req:?}")
                                    .split([' ', '{', '('])
                                    .next()
                                    .unwrap_or("")
                            ),
                            Action::SetTimer { kind, .. } => format!("SetTimer:{kind:?}"),
                            Action::Send { msg, .. } => format!(
                                "Send:{}",
                                format!("{msg:?}")
                                    .split([' ', '{', '('])
                                    .next()
                                    .unwrap_or("")
                            ),
                            other => format!("{other:?}")
                                .split([' ', '{', '('])
                                .next()
                                .unwrap_or("")
                                .to_string(),
                        };
                        *hist.entry(format!("node{}:A:{k}", self.id)).or_default() += 1;
                    }
                }
                let clients: Vec<String> = self
                    .core
                    .clients()
                    .map(|(rid, phase)| {
                        format!("{}.{}.{}:{phase:?}", rid.node, rid.incarnation, rid.seq)
                    })
                    .collect();
                note_waiting(
                    &format!("node {}", self.id),
                    format!(
                        "sizes={:?} events={} job={:?} lease={:?} gate={} ack={:?} clients={clients:?}",
                        self.core.container_sizes(),
                        self.events_handled,
                        self.core.job(),
                        self.core.lease().epoch(),
                        self.core.lease().gate.is_some(),
                        self.core.ack_view(),
                    ),
                );
            }
            let marks_after = self.core.stats.takeovers
                + self.core.stats.segments_applied
                + self.core.stats.shadows_installed
                + self.core.stats.hints_installed;
            if debug && (marks_after != marks_before || debug_event.starts_with("Submit")) {
                // `RUST_LOG=sim=debug`: the root directory after every
                // event that changed the replica or submitted an op.
                let listing: Vec<String> = constellation_meta::MetaStore::readdir(
                    &*self.meta,
                    constellation_fs_core::types::ROOT_INO,
                )
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|e| format!("{}={:#x}", e.name, e.ino))
                        .collect()
                })
                .unwrap_or_default();
                tracing::debug!(
                    node = self.id,
                    now = now.wall.0,
                    event = %debug_event,
                    applied = Replica::applied_seq(&*self.meta).unwrap_or(0),
                    ?listing,
                    "replica"
                );
            }
            self.note_rollbacks();
            self.note_journaled_refusals();
            self.note_identity();
            self.dispatch(actions);
            self.refresh_view();
        }
    }

    /// The next event to handle. Under a runtime lag
    /// (`NodeHandle::runtime_lag`) nothing is handled until it ends;
    /// then the core first hears of the lapse (`Event::OwnLag`, as the
    /// daemon's lag probe tells it), then the timers and IO completions
    /// queued meanwhile, while what peers sent stays unstamped until the
    /// lag's peer deadline — the order that makes a silence watch look
    /// before the backlog of beats is in.
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            // The urgent lane first (the lane's registrations: they must
            // precede their answers, and the timers that fired meanwhile).
            let mut urgent = Vec::new();
            while let Ok(e) = self.urgent_rx.try_recv() {
                urgent.push(e);
            }
            for e in urgent.into_iter().rev() {
                self.ready.push_front(e);
            }
            if let Some(event) = self.ready.pop_front() {
                return Some(event);
            }
            let now = tokio::time::Instant::now();
            if let Some(t) = self.peers_held_until {
                if now >= t {
                    self.peers_held_until = None;
                    self.ready.extend(self.held_peers.drain(..));
                    continue;
                }
            }
            if self.lag_pending.is_some_and(|t| now >= t) {
                self.finish_lag();
                continue;
            }
            // Wake for the first of: the peer lane's hold, the lag's end,
            // F1's held write (released between events).
            let held_at = self.held.as_ref().map(|h| h.release_at);
            let wake = [self.peers_held_until, self.lag_pending, held_at]
                .into_iter()
                .flatten()
                .min();
            let event = match wake {
                Some(t) => tokio::select! {
                    biased;
                    Some(e) = self.urgent_rx.recv() => {
                        self.ready.push_back(e);
                        continue;
                    }
                    _ = tokio::time::sleep_until(t) => {
                        if held_at.is_some_and(|h| h <= tokio::time::Instant::now()) {
                            self.release_held();
                        }
                        continue;
                    }
                    e = self.rx.recv() => e?,
                },
                None => tokio::select! {
                    biased;
                    Some(e) = self.urgent_rx.recv() => {
                        self.ready.push_back(e);
                        continue;
                    }
                    e = self.rx.recv() => e?,
                },
            };
            if self.beats_lost_in_lag && is_numbered_beat(&event) && {
                // The lane is down only while the lag lasts, not until
                // an event finds the lag over.
                let now = tokio::time::Instant::now();
                let until = self.shared.lag.lock().unwrap().map(|(until, _)| until);
                until
                    .into_iter()
                    .chain(self.lag_pending)
                    .any(|until| now < until)
            } {
                // The liveness lane to this node is down while its runtime
                // is held up (`SimConfig::beats_lost_in_lag`).
                continue;
            }
            if self.lag_pending.is_some() {
                self.lag_queue.push(event);
                continue;
            }
            // Kept while it lasts: a heartbeat on the runtime
            // (`beat_loop`) is held up by it too.
            let lag = *self.shared.lag.lock().unwrap();
            if let Some((until, _)) = lag {
                self.lag_queue.push(event);
                self.lag_pending = Some(until);
                continue;
            }
            if self.peers_held_until.is_some() && from_a_peer(&event) {
                self.held_peers.push_back(event);
                continue;
            }
            return Some(event);
        }
    }

    /// A runtime lag ended: the core hears of it first (`Event::OwnLag`),
    /// then (`stamps_first`, the daemon's order) what its P2P handlers
    /// stamped meanwhile, then the timers and IO completions that queued,
    /// while what peers sent waits for the lag's peer deadline.
    fn finish_lag(&mut self) {
        self.lag_pending = None;
        let lag = self.shared.lag.lock().unwrap().take();
        let Some((_, peers_until)) = lag else {
            self.ready.extend(self.lag_queue.drain(..));
            return;
        };
        let mut queued = std::mem::take(&mut self.lag_queue);
        while let Ok(e) = self.rx.try_recv() {
            if !(self.beats_lost_in_lag && is_numbered_beat(&e)) {
                queued.push(e);
            }
        }
        let now = self.clock.now();
        let since = self.shared.lag_since.lock().unwrap().take();
        let lapsed = since.map_or(0, |t| {
            tokio::time::Instant::now().duration_since(t).as_millis() as i64
        });
        self.ready.push_back(Event::OwnLag {
            from: constellation_authority::WaitMs(now.wait.0 - lapsed),
            until: now.wait,
        });
        if self.stamps_first {
            // The daemon's P2P handlers stamp the holder's beats and
            // appends as its runtime gets to them — at the lapse's end —
            // and its driver hands those stamps to the core, as unnumbered
            // beats, before any timer (`Driver::hand_heard`); the appends
            // themselves (its sync lane) come after the timers, and the
            // numbered beats with the rest of what peers sent.
            // Each stamp carries the highest expiry the beats and appends
            // it stands for named, as the bridge's does
            // (`OffCore::note_holder_heard`).
            let mut stamped: Vec<((NodeId, u64), i64)> = Vec::new();
            for e in &queued {
                let key = match e {
                    Event::HolderAlive {
                        from,
                        epoch,
                        listed: true,
                        expires,
                        ..
                    } => Some(((*from, *epoch), expires.0)),
                    Event::Peer {
                        msg:
                            PeerMsg::BackupAppend {
                                holder,
                                epoch,
                                expires_unix_ms,
                                ..
                            },
                        ..
                    } => Some(((*holder, *epoch), *expires_unix_ms)),
                    _ => None,
                };
                if let Some((key, expires)) = key {
                    match stamped.iter_mut().find(|(k, _)| *k == key) {
                        Some((_, e)) => *e = (*e).max(expires),
                        None => stamped.push((key, expires)),
                    }
                }
            }
            for ((from, epoch), expires) in stamped {
                self.ready.push_back(Event::HolderAlive {
                    from,
                    epoch,
                    candidacy: 0,
                    listed: true,
                    at: now.wait,
                    seq: 0,
                    clock: 0,
                    sent: 0,
                    holder_expires: 0,
                    guard: 0,
                    expires: constellation_authority::Ms(expires),
                });
            }
        }
        self.peers_held_until = Some(peers_until);
        for e in queued {
            if from_a_peer(&e) {
                self.held_peers.push_back(e);
            } else {
                self.ready.push_back(e);
            }
        }
    }

    fn refresh_view(&self) {
        let lease = self.core.lease();
        let mut view = self.shared.view.lock().unwrap();
        view.stats = self.core.stats;
        view.held_epoch = lease
            .epoch()
            .filter(|_| lease.held.is_some() && !lease.lost);
        view.lost = lease.lost;
        view.gate_pending = lease.gate.is_some();
        view.job = self.core.job();
        view.job_phase = self.core.job_phase();
        view.clients_in_flight = self.core.clients().count();
        view.next_seq = self.core.ship().next_seq;
        view.events_handled += 1;
        view.applied_seq = Replica::applied_seq(&*self.meta).unwrap_or(0);
        view.journal_len = Replica::journal_len(&*self.meta).unwrap_or(0);
        view.speculation = self.meta.speculation_counts().unwrap_or_default();
        view.ack = self.core.ack_view();
        self.shared
            .max_backups
            .fetch_max(view.ack.backups.len() as u64, Ordering::Relaxed);
        view.delegation = self.core.deleg_view();
        let locks = self.core.lock_view();
        view.lock_requests = locks.requests_in_flight;
        view.lock_waiters = locks.waiters;
        view.lock_recalls = locks.recalls_in_flight;
        let now = self.clock.now();
        let floor = self.core.takeover_floor_until().0;
        if floor != 0 && floor != self.shared.floor_seen.swap(floor, Ordering::SeqCst) {
            self.shared
                .floor_ms
                .lock()
                .unwrap()
                .push((floor - now.wait.0).max(0) as u64);
        } else if floor == 0 {
            self.shared.floor_seen.store(0, Ordering::SeqCst);
        }
        view.claim = self.core.epoch_claim_view(now);
        view.epoch_held = lease.epoch_held();
        view.s3_held = match &lease.held {
            Some((l, _)) if !lease.lost && !lease.epoch_held() && l.holder == self.id => {
                // The lease's end on this node's honour clock, 0 when the
                // body carries none of this incarnation's (`honour_until`).
                Some((
                    l.epoch,
                    lease.honour_until(self.core.config()).map_or(0, |h| h.0),
                    lease.wait_until(self.core.config()).map_or(0, |w| w.0),
                ))
            }
            _ => None,
        };
        let wall_expiry = match &lease.held {
            Some((l, _)) => l.expires_unix_ms,
            None => 0,
        };
        let certs = self.core.deleg_renew_certs();
        let liveness = view.s3_held.map(|(epoch, until_honour, until_wait)| {
            let targets = self.core.liveness_targets().into_iter().map(|(n, _)| n);
            let (base, protecting) = self.core.beat_guard_parts(&*self.meta);
            let guard = base.max(protecting);
            let mut seen = self.shared.guard_seen.lock().unwrap();
            if seen.0 != epoch {
                *seen = (epoch, 0, 0);
            }
            if guard != i64::MAX {
                if seen.1 > 0 && guard > seen.1 {
                    self.shared.guard_rises.fetch_add(1, Ordering::SeqCst);
                }
                seen.1 = guard;
            }
            if base != i64::MAX {
                // A base that has passed guards nothing any more: a later
                // quarantine is a new duty, not the rise of a published one
                // (a reboot loads its restart horizon as a full span, which
                // outlives the grants it stands for; plan 43 C7a, commit 6:
                // `locks-blips` seed 1339, the horizon passed at 7.4 s and a
                // re-adopted own lease at 10.3 s set a new quarantine).
                if seen.2 > 0 && base > seen.2 && seen.2 > self.clock.wait().0 {
                    self.shared.guard_base_rises.fetch_add(1, Ordering::SeqCst);
                }
                seen.2 = base;
            }
            (
                epoch,
                wall_expiry,
                until_honour,
                until_wait,
                guard,
                targets.collect(),
            )
        });
        drop(view);
        *self.shared.liveness.lock().unwrap() = liveness;
        *self.shared.deleg_certs.lock().unwrap() = certs;
    }

    /// Whenever the core rolled something back, the replay queue names
    /// the rids whose acknowledgements are now tentative.
    fn note_rollbacks(&mut self) {
        let s = self.core.stats;
        let now = (s.speculation_rolled_back, s.local_rolled_back);
        if now == self.rolled_back_seen {
            return;
        }
        self.rolled_back_seen = now;
        let mut t = self.shared.tentative.lock().unwrap();
        if let Ok(queue) = self.meta.pending_replays() {
            for q in queue {
                t.insert(q.rid);
            }
        }
        self.shared
            .journaled_refusals
            .lock()
            .unwrap()
            .retain(|rid| match self.meta.completed_outcome(*rid) {
                // The refusal's row is gone, or the log now carries the rid
                // executed: the refusal was rolled back either way.
                Ok(None | Some(CompletedOutcome::Executed { .. })) => {
                    t.insert(*rid);
                    false
                }
                _ => true,
            });
    }

    /// Plan 43 C0, I2 (b): where this node holds each rid queued for
    /// execution — its replay queue and the core's client map (its own
    /// clients' submissions and its in-flight replays, one entry per rid).
    fn note_identity(&self) {
        let queued: BTreeSet<Rid> = self
            .meta
            .pending_replays()
            .map(|q| q.into_iter().map(|s| s.rid).collect())
            .unwrap_or_default();
        let executing: BTreeSet<Rid> = self.core.clients().map(|(rid, _)| rid).collect();
        let tick = self.oracles.tick.load(Ordering::SeqCst);
        self.oracles
            .identity
            .lock()
            .unwrap()
            .update(self.id, queued, executing, tick);
    }

    /// Plan 43 C0, I1: an answer leaves this node (a `MutateReply` to a
    /// requester when `reply`, else its own client's reply). Definitive
    /// answers are judged now, against every store as it is at this
    /// emission; a requester passing a sequencer's answer on is a relay.
    fn note_answer(&self, rid: Rid, outcome: &MutateOutcome, reply: bool) {
        use super::oracle::{committed_at_answer, outcome_kind, AckEntry, AnswererState, Expected};
        let Some(kind) = outcome_kind(outcome) else {
            return;
        };
        let tick = self.oracles.next_tick();
        let at_ms = self.clock.elapsed_ms();
        let relay = !reply
            && self
                .oracles
                .reply_kinds
                .lock()
                .unwrap()
                .get(&rid)
                .is_some_and(|v| v.iter().any(|(n, k)| *n != self.id && *k == kind));
        if reply {
            self.oracles
                .reply_kinds
                .lock()
                .unwrap()
                .entry(rid)
                .or_default()
                .push((self.id, kind));
        }
        let judged = (!relay).then(|| {
            let st = AnswererState::of(&self.core, &self.meta, self.id, self.clock.now().wait.0);
            let j = committed_at_answer(&self.oracles, &self.bus, self.id, &st, rid, outcome);
            if j.class.is_none() && !(j.expected == Expected::Unjournaled && kind.is_refusal()) {
                let text = format!(
                    "I1 acked_implies_committed: node {} answered rid {rid:?} ({}) at t={at_ms} ms, \
                     expected {}, committed by none: {}",
                    self.id,
                    kind.name(),
                    j.expected.name(),
                    j.detail
                );
                // `AUTHORITY_SIM_ORACLE_DEBUG=1`: every violation, as judged.
                if std::env::var_os("AUTHORITY_SIM_ORACLE_DEBUG").is_some() {
                    eprintln!("ORACLE-I1 t={at_ms} {text}");
                }
                self.oracles.i1_violations.lock().unwrap().push(text);
            }
            j
        });
        self.oracles.acks.lock().unwrap().push(AckEntry {
            node: self.id,
            rid,
            tick,
            at_ms,
            kind,
            reply,
            relay,
            judged,
        });
    }

    /// A `MutateReply` to `to` for its request `req`: judged as the answer
    /// to the rid that request carried.
    fn note_forward_answer(&self, to: NodeId, req: OpId, outcome: &MutateOutcome) {
        match self.forwards.get(&(to, req)).copied() {
            Some(rid) => self.note_answer(rid, outcome, true),
            None => {
                if super::oracle::outcome_kind(outcome).is_some() {
                    self.oracles.acks_unmapped.fetch_add(1, Ordering::Relaxed);
                    if std::env::var_os("AUTHORITY_SIM_ORACLE_DEBUG").is_some() {
                        eprintln!(
                            "ORACLE-UNMAPPED t={} node {} to {to} req {req:?}: {outcome:?}",
                            self.clock.elapsed_ms(),
                            self.id
                        );
                    }
                }
            }
        }
    }

    /// Retake `Shared::journaled_refusals` when the journal changed.
    fn note_journaled_refusals(&mut self) {
        let seen = (
            Replica::journal_tip(&*self.meta),
            Replica::journal_len(&*self.meta).unwrap_or(0),
        );
        if self.journal_seen == Some(seen) {
            return;
        }
        self.journal_seen = Some(seen);
        let refusals: BTreeSet<Rid> = Replica::journal_txs_from(&*self.meta, 0, usize::MAX)
            .into_iter()
            .flat_map(|tx| tx.records)
            .filter_map(|rec| match rec {
                constellation_meta::LogRecord::Refused { rid, .. } => Some(rid),
                _ => None,
            })
            .collect();
        *self.shared.journaled_refusals.lock().unwrap() = refusals;
    }

    /// M12 round 2: the daemon's FUSE fast path (`SimConfig::fast_path`).
    /// A client op of a node whose lease is open for a new mutation, and
    /// which holds no delegation and is not acknowledgement-gated,
    /// executes here, outside the core, exactly as
    /// `view::View::mutate_op_rebasable_with_rid` does: admitted by
    /// `Meta::root_fast_path` (`Checked`) or blindly (`Unchecked`), then
    /// `execute_mutate`, the acknowledgement fed to the core as
    /// `Event::Activity` and the keys to the placement. `None`: not a
    /// fast-path op; the core handles the event.
    fn fast_path(
        &mut self,
        now: constellation_authority::Now,
        event: &Event,
    ) -> Option<Vec<Action>> {
        use super::run::FastPath;
        if let Event::Submit {
            rid,
            op,
            policy: Policy::Client,
            tag,
        } = event
        {
            if !tag.is_empty() && self.held.is_none() {
                if let Some(actions) = self.delegate_hold(now, *rid, op, tag) {
                    return Some(actions);
                }
            }
        }
        if self.fast_path == FastPath::Off {
            return None;
        }
        let Event::Submit {
            rid,
            op,
            policy: Policy::Client,
            tag,
        } = event
        else {
            return None;
        };
        let cfg = self.core.config();
        let epoch = self.core.lease().new_mutation_epoch(now, cfg)?;
        if self.core.lease().releasing
            || (self.core.ack_gated() && !self.plant_ungated_fast_path)
            || !self.core.deleg_view().mine.is_empty()
        {
            return None;
        }
        let admission = match self.fast_path {
            FastPath::Checked => Some(self.meta.root_fast_path(op)?),
            _ => None,
        };
        // F1 (`WriterLockHold`): this write waits for the writer lock
        // behind a slow writer, past its check. (The delegation gate is
        // not held across the wait: the configurations that hold writes
        // delegate nothing.)
        if !tag.is_empty() && self.held.is_none() {
            let hold = self.shared.hold_next.lock().unwrap().take();
            if let Some((core_ms, write_ms)) = hold {
                drop(admission);
                return Some(self.hold_write(now, *rid, op, tag, epoch, core_ms, write_ms));
            }
        }
        let outcome = outcome_of(
            constellation_meta::execute_tagged(&self.meta, op, Some(*rid), tag, now),
            epoch,
        );
        drop(admission);
        Some(self.fast_path_done(now, *rid, op, outcome))
    }

    /// F1: start the held write (`HeldWrite`) and stall the core's next
    /// step behind the same writer. Refused by its check before the
    /// wait, it is answered at once.
    #[allow(clippy::too_many_arguments)]
    fn hold_write(
        &mut self,
        now: constellation_authority::Now,
        rid: Rid,
        op: &MutateOp,
        tag: &LockTag,
        epoch: u64,
        core_ms: u64,
        write_ms: u64,
    ) -> Vec<Action> {
        let (phase_tx, phase) = std::sync::mpsc::channel();
        let (go, go_rx) = std::sync::mpsc::channel::<bool>();
        let rt = tokio::runtime::Handle::current();
        let thread = {
            let (meta, op, tag) = (self.meta.clone(), op.clone(), tag.clone());
            let checked = phase_tx.clone();
            let clock = self.clock.clone();
            std::thread::spawn(move || {
                // The node's clock is the runtime's paused one.
                let _rt = rt.enter();
                constellation_meta::set_before_write_hook_for_tests(Some(Box::new(move || {
                    let _ = checked.send(HeldPhase::Checked);
                    go_rx.recv().unwrap_or(false)
                })));
                let r = constellation_meta::execute_tagged(&meta, &op, Some(rid), &tag, now);
                constellation_meta::set_before_write_hook_for_tests(None);
                let _ = phase_tx.send(HeldPhase::Done(
                    outcome_of(r, epoch),
                    None,
                    clock.elapsed_ms(),
                ));
            })
        };
        let mut held = HeldWrite {
            rid,
            op: op.clone(),
            epoch,
            deleg: None,
            shared: self.shared.clone(),
            release_at: tokio::time::Instant::now() + std::time::Duration::from_millis(write_ms),
            go: Some(go),
            phase,
            thread: Some(thread),
        };
        match held.phase.recv() {
            Ok(HeldPhase::Checked) => {
                tracing::debug!(
                    node = self.id,
                    ?rid,
                    core_ms,
                    write_ms,
                    "sim: a tagged fast-path write passed its check and waits for the writer lock"
                );
                self.shared.held_writes.fetch_add(1, Ordering::Relaxed);
                *self.shared.stall_next.lock().unwrap() = Some(core_ms);
                self.held = Some(held);
                Vec::new()
            }
            Ok(HeldPhase::Done(outcome, _, _)) => {
                held.go = None;
                if let Some(t) = held.thread.take() {
                    t.join().expect("held write thread");
                }
                self.fast_path_done(now, rid, op, outcome)
            }
            Err(_) => panic!("held write thread vanished"),
        }
    }

    /// F1 (`WriterLockHold` on a delegate): the delegate's own client's
    /// write under a lock another sequencer minted, on the delegate's
    /// fast path (`View`'s `delegate_execute`), held *inside* its
    /// transaction — checked at the write, pinned, the writer lock held —
    /// as a slow commit keeps it. The core goes on; the first writer that
    /// waits for the lock (a settle, a segment, a forward) lets it commit
    /// first, as it would wait for it. `None`: no hold armed, or the op is
    /// not this delegate's to execute here.
    fn delegate_hold(
        &mut self,
        now: constellation_authority::Now,
        rid: Rid,
        op: &MutateOp,
        tag: &LockTag,
    ) -> Option<Vec<Action>> {
        if self.shared.hold_next.lock().unwrap().is_none() {
            return None;
        }
        let constellation_meta::delegation::Ownership::Delegated(d) =
            self.meta.resolve_ownership(&TouchSet::from_op(op))
        else {
            return None;
        };
        if d.node != self.id {
            return None;
        }
        let gen = d.gen;
        let view = self.core.deleg_view();
        if !view
            .mine
            .iter()
            .any(|m| m.1 == gen && !m.3 && now.wall.0 < m.2)
        {
            return None;
        }
        let session = self.meta.session();
        let deps = session.deps().filter(|dp| session.reaches(dp))?;
        let (_, write_ms) = self.shared.hold_next.lock().unwrap().take()?;
        let epoch = self.core.ship().max_epoch.max(1);
        let (phase_tx, phase) = std::sync::mpsc::channel();
        let (go, go_rx) = std::sync::mpsc::channel::<bool>();
        let (committed_tx, committed) = std::sync::mpsc::channel::<()>();
        let rt = tokio::runtime::Handle::current();
        let thread = {
            let (meta, op, tag) = (self.meta.clone(), op.clone(), tag.clone());
            let checked = phase_tx.clone();
            let mut go_rx = Some(go_rx);
            let clock = self.clock.clone();
            std::thread::spawn(move || {
                let _rt = rt.enter();
                constellation_meta::write_check_hook::set(Some(Box::new(move |at| {
                    if at != constellation_meta::write_check_hook::At::Pinned {
                        return true;
                    }
                    let _ = checked.send(HeldPhase::Checked);
                    go_rx.take().is_none_or(|rx| rx.recv().unwrap_or(false))
                })));
                let r = meta.delegate_execute(&op, Some(rid), gen, deps, &tag, now);
                constellation_meta::write_check_hook::set(None);
                let at = clock.elapsed_ms();
                let _ = committed_tx.send(());
                let idx = r.as_ref().ok().map(|(_, idx)| *idx);
                let _ = phase_tx.send(HeldPhase::Done(
                    outcome_of(r.map(|(records, _)| records), epoch),
                    idx,
                    at,
                ));
            })
        };
        let tid = thread.thread().id();
        let mut held = HeldWrite {
            rid,
            op: op.clone(),
            epoch,
            deleg: Some(gen),
            shared: self.shared.clone(),
            release_at: tokio::time::Instant::now() + std::time::Duration::from_millis(write_ms),
            go: None,
            phase,
            thread: Some(thread),
        };
        match held.phase.recv() {
            Ok(HeldPhase::Checked) => {
                tracing::debug!(
                    node = self.id,
                    ?rid,
                    gen,
                    write_ms,
                    "sim: a delegate's tagged write is pinned in its transaction (a slow commit)"
                );
                *self.shared.held_ctl.lock().unwrap() = Some(HeldCtl {
                    go,
                    committed,
                    thread: tid,
                });
                // Cut from every other node (the root first among them):
                // nothing reaches it that would make it write, so the held
                // commit lasts until the node's own next writer.
                let cut = self.shared.hold_partition.lock().unwrap().take();
                if let Some((_, ms)) = cut.filter(|(_, ms)| *ms > 0) {
                    let me = self.id;
                    let others: Vec<NodeId> =
                        self.bus.nodes().into_iter().filter(|n| *n != me).collect();
                    for n in &others {
                        self.bus.set_partition(me, *n, true);
                    }
                    let bus = self.bus.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                        for n in others {
                            bus.set_partition(me, n, false);
                        }
                    });
                }
                self.shared.held_writes.fetch_add(1, Ordering::Relaxed);
                self.held = Some(held);
                Some(Vec::new())
            }
            Ok(HeldPhase::Done(outcome, idx, _)) => {
                if let Some(t) = held.thread.take() {
                    t.join().expect("held write thread");
                }
                if let Some(idx) = idx {
                    session.note_stream(gen, idx);
                }
                Some(self.fast_path_done(now, rid, op, outcome))
            }
            Err(_) => panic!("held write thread vanished"),
        }
    }

    /// F1: the held write's turn came: it runs its transaction now.
    fn release_held(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        let (rid, op) = (held.rid, held.op.clone());
        let _ = held.epoch;
        let deleg = held.deleg;
        let (outcome, idx, landed_at) = held.finish();
        match &outcome {
            MutateOutcome::LockLapsed => {
                self.shared.held_refused.fetch_add(1, Ordering::Relaxed);
            }
            MutateOutcome::Accepted { .. } => {
                self.shared.held_accepted.fetch_add(1, Ordering::Relaxed);
                // When it landed: a writer may have let it commit before
                // its time (`Shared::held_ctl`).
                self.shared.landed.lock().unwrap().push((rid, landed_at));
            }
            _ => {}
        }
        if let (Some(gen), Some(idx)) = (deleg, idx) {
            self.meta.session().note_stream(gen, idx);
        }
        tracing::debug!(node = self.id, ?rid, ?outcome, "sim: the held write ran");
        let now = self.clock.now();
        let actions = self.fast_path_done(now, rid, &op, outcome);
        self.dispatch(actions);
    }

    /// The fast path's answer to an op it executed: the acknowledgement
    /// to the core and the reply.
    fn fast_path_done(
        &mut self,
        now: constellation_authority::Now,
        rid: Rid,
        op: &MutateOp,
        outcome: MutateOutcome,
    ) -> Vec<Action> {
        self.shared
            .fast_path_executed
            .fetch_add(1, Ordering::Relaxed);
        self.core
            .place_note_local(&[TouchSet::from_op(op)], &*self.meta);
        self.step_now = now;
        let mut actions = self.core.handle(
            now,
            Event::Activity {
                last_write: now.wait,
                acked_seqs: vec![rid.seq],
            },
            &*self.meta,
        );
        actions.push(Action::Reply {
            rid,
            reply: ClientReply::Outcome(outcome),
        });
        actions
    }

    fn dispatch(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Reply { rid, reply } => {
                    match &reply {
                        ClientReply::InDoubt => {
                            let applied = Replica::applied_seq(&*self.meta).unwrap_or(0);
                            let tick = self.oracles.next_tick();
                            self.shared
                                .in_doubt
                                .lock()
                                .unwrap()
                                .push((self.id, rid, applied, tick));
                        }
                        ClientReply::Outcome(outcome) => self.note_answer(rid, outcome, false),
                    }
                    if let Some(tx) = self.shared.replies.lock().unwrap().remove(&rid) {
                        let _ = tx.send(reply);
                    }
                }
                Action::Send { to, msg } => {
                    // (`OpId(0)`: the core re-executing a parked request whose
                    // `Held` reply was sent already; that reply matches no
                    // request at the requester, which gets its answer from
                    // the dedup when it retries: not an answer.)
                    if let PeerMsg::MutateReply { req, outcome, .. } = &msg {
                        if *req != OpId(0) {
                            self.note_forward_answer(to, *req, outcome);
                        }
                    }
                    self.bus.send(self.id, to, msg)
                }
                Action::S3 { op, req } => self.spawn_s3(op, req),
                Action::SetTimer { id, after_ms, kind } => {
                    self.timers.insert(id);
                    self.timer_kinds.insert(id, kind);
                    let tx = self.tx.clone();
                    // Counted from the reading of the step that armed it
                    // (see `step_now`): it fires when the node's wait clock
                    // reads that plus `after_ms`. The runtime's clock is
                    // true time; the node's oscillator may run off it
                    // (`RateSkew`) or stand still (`Suspend`), so the
                    // deadline is the inverse of the oscillator and is
                    // checked again when the sleep ends (a rate or a
                    // suspend announced after the timer was set).
                    let target = self.step_now.wait.plus(after_ms);
                    let clock = self.clock.clone();
                    tokio::spawn(async move {
                        loop {
                            let when = clock.instant_at_wait(target);
                            tokio::time::sleep_until(when).await;
                            if clock.wait() >= target {
                                break;
                            }
                            // Not there yet (never with a steady clock):
                            // never spin at one instant.
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        }
                        let _ = tx.send(Event::Timer { id });
                    });
                }
                Action::CancelTimer { id } => {
                    self.timers.remove(&id);
                    self.timer_kinds.remove(&id);
                }
                Action::UploadDirtyChunks {
                    op, ino, complete, ..
                } => {
                    // The modelled pass (`chunks.rs`): this node's own
                    // dirty chunks go up when S3 is reachable, remote rows
                    // are acked once their chunk is there; nothing is
                    // ever unrecoverable, so nothing is held back. A
                    // complete pass (a barrier, a flush, a handoff) is a
                    // durability need: the metered hold exempts it.
                    let s3_up = !self.bucket.is_cut(self.id);
                    if complete {
                        self.world.upload_forced(self.id, &self.meta, ino, s3_up);
                    } else {
                        self.world.upload_pass(self.id, &self.meta, ino, s3_up);
                    }
                    let _ = self.tx.send(Event::UploadsDone {
                        op,
                        result: UploadResult::Done { held: 0 },
                    });
                }
                Action::Publish { op, epoch } => self.spawn_publish(op, epoch),
                Action::FollowHead { op } => {
                    let _ = self.tx.send(Event::PublishDone { op, ok: true });
                }
                Action::RebuildReplica { op } => self.spawn_rebuild(op),
                Action::RoundDone { .. } => {}
                // Chunk metered-own-rows: past the modelled metered hold
                // (`ChunkWorld::hold_ms`); without one the round's pass
                // has uploaded them already.
                Action::UploadAwaited { inos } => {
                    self.world.upload_awaited(
                        self.id,
                        &self.meta,
                        &inos,
                        !self.bucket.is_cut(self.id),
                    );
                }
                Action::LockFlush { ino, grant } => {
                    // Plan 30 §M14: no file data in this simulation, so the
                    // flush is only a delay (seeded by the grant, so a
                    // replay repeats it): the release still races the
                    // recalls, renewals and requests that arrive meanwhile.
                    let tx = self.tx.clone();
                    let delay = (grant.seq.wrapping_mul(7) ^ ino) % 25;
                    tokio::spawn(async move {
                        if delay > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        }
                        let _ = tx.send(Event::LockFlushed {
                            ino,
                            grant,
                            ok: true,
                        });
                    });
                }
                Action::PersistLockHorizon { until } => {
                    // Written here (the simulation has no core thread to
                    // keep free), answered as an event: the answers it
                    // holds go out one event later, behind whatever
                    // arrives meanwhile.
                    let durable = self
                        .meta
                        .note_lock_grant_horizon(until, self.clock.now())
                        .ok();
                    let _ = self.tx.send(Event::LockHorizonPersisted { until, durable });
                }
                Action::EpochClose | Action::EpochFlushed => {
                    // `EpochManager::close` / `finish_flushing`.
                    let req = {
                        let mut e = self.shared.epoch.lock().unwrap();
                        if matches!(action, Action::EpochClose) {
                            if !e.open {
                                continue;
                            }
                            e.open = false;
                            e.active = false;
                            e.frozen = false;
                            e.flushing = true;
                        } else {
                            e.flushing = false;
                        }
                        e.control()
                    };
                    let _ = self.meta.promise_join_end();
                    let op = OpId(self.shared.next_control.fetch_add(1, Ordering::SeqCst));
                    let _ = self.tx.send(Event::Control { op, req });
                }
                Action::RefreshRoster => {
                    let _ = self.tx.send(Event::Roster {
                        write_eligible: self.bus.nodes(),
                    });
                }
                Action::Announce {
                    seq,
                    epoch,
                    payload,
                } => {
                    // `Peers::announce_segment` is a no-op with P2P off.
                    // Plan 30 §M7: a hint only; the log travels on streams.
                    let _ = payload;
                    if self.core.config().p2p {
                        self.bus
                            .broadcast(self.id, PeerMsg::SegmentPublished { seq, epoch })
                    }
                }
                Action::ConflictCopy {
                    queue_seq,
                    rid,
                    reason,
                    ..
                } => {
                    tracing::warn!(node = self.id, ?rid, reason, "conflict copy");
                    self.shared.conflict_copies.fetch_add(1, Ordering::SeqCst);
                    let _ = self.tx.send(Event::ConflictCopyDone {
                        queue_seq,
                        ok: true,
                    });
                }
                Action::ControlDone { op, result } => {
                    if self.bootstrap == Some(op) {
                        self.bootstrap = None;
                        if let Err(error) = &result {
                            tracing::warn!(
                                node = self.id,
                                error,
                                "bootstrap tail failed; serving anyway"
                            );
                        }
                        for event in std::mem::take(&mut self.deferred) {
                            let _ = self.tx.send(event);
                        }
                    } else if let Some(tx) = self.shared.controls.lock().unwrap().remove(&op) {
                        let _ = tx.send(result);
                    }
                }
            }
        }
    }

    /// Plan 43 C0: `Action::RebuildReplica`, as the engine runs it
    /// (`authority_driver::rebuild_replica`): a side replica built from the
    /// bucket's log — read through this node's faulted handle, with the
    /// epoch fencing every replica applies — swapped in by the production
    /// `Meta::replace_ns_from_rebuilt`. (The engine bootstraps from the head
    /// commit and replays from there; the simulation replays from genesis,
    /// which is the same state. `SimConfig::rebuild_stub` keeps the old stub
    /// that always failed, as a negative control.)
    fn spawn_rebuild(&self, op: OpId) {
        let (tx, shared, meta) = (self.tx.clone(), self.shared.clone(), self.meta.clone());
        let incarnation = shared.incarnation.load(Ordering::SeqCst);
        if self.rebuild_stub {
            shared.rebuilds_failed.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(Event::RebuildDone { op, ok: false });
            return;
        }
        let (store, node) = (self.store.clone(), self.id);
        tokio::spawn(async move {
            let result = rebuild_from_log(store, node, &meta, &shared, incarnation).await;
            // A process that died (or restarted) meanwhile has no rebuild
            // to count and nobody to tell.
            if !shared.alive.load(Ordering::SeqCst)
                || shared.incarnation.load(Ordering::SeqCst) != incarnation
            {
                return;
            }
            let ok = match result {
                Ok(()) => true,
                Err(error) => {
                    tracing::debug!(node, error, "sim: the deposition rebuild failed");
                    false
                }
            };
            let counter = if ok {
                &shared.rebuilds_ok
            } else {
                &shared.rebuilds_failed
            };
            counter.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(Event::RebuildDone { op, ok });
        });
    }

    /// What the daemon does around the core for a forwarded manifest
    /// (`authority_driver.rs`, the `MutateRequest` arm): the chunks still
    /// pending on the sender are enrolled here, before the op executes.
    fn before_core(&self, event: &Event) {
        if let Event::Peer {
            from,
            msg:
                PeerMsg::MutateRequest {
                    op: MutateOp::SetManifest { ino, manifest, .. },
                    ..
                },
        } = event
        {
            self.world
                .enroll_forwarded(&self.meta, *from, *ino, manifest);
        }
    }

    fn spawn_s3(&self, op: OpId, req: S3Op) {
        let store = self.store.clone();
        let tx = self.tx.clone();
        let world = self.world.clone();
        let bucket = self.bucket.clone();
        let id = self.id;
        let (meta, s3_up) = (self.meta.clone(), !self.bucket.is_cut(self.id));
        tokio::spawn(async move {
            let leases = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
            let log = LogStore::new(store.clone());
            let result = match req {
                S3Op::LeaseGet => {
                    S3Result::LeaseGet(leases.get().await.map_err(|e| S3Failure(e.to_string())))
                }
                S3Op::LeaseCreate { lease } => {
                    S3Result::LeasePut(leases.try_create(&lease).await.map_err(cas_failure))
                }
                S3Op::LeaseSwap { lease, tag } => {
                    S3Result::LeasePut(leases.try_swap(&lease, &tag).await.map_err(cas_failure))
                }
                S3Op::SegmentPut { seq, payload } => {
                    world.check_segment(id, seq, &payload);
                    S3Result::SegmentPut(log.put_segment(seq, &payload).await.map_err(cas_failure))
                }
                S3Op::SegmentRun { from, width } => S3Result::SegmentRun(
                    log.get_run(from, width)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::SegmentGap { from } => S3Result::SegmentGap(
                    log.first_segment_from(from)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxPut { batch } => {
                    // As the daemon (`inbox_manifests_pending`): the inbox
                    // carries no pending-chunk list, so a manifest's
                    // chunks go up first, past the metered hold.
                    let inos: Vec<constellation_fs_core::Ino> = batch
                        .ops
                        .iter()
                        .filter_map(|o| match MutateOp::from_postcard(&o.op) {
                            Ok(MutateOp::SetManifest { ino, .. }) => Some(ino),
                            _ => None,
                        })
                        .collect();
                    world.upload_awaited(id, &meta, &inos, s3_up);
                    let inbox = InboxStore::new(store.clone());
                    S3Result::InboxPut(
                        match world.upload_before_inbox(id, &batch, !bucket.is_cut(id)) {
                            Ok(()) => inbox
                                .put_batch(&batch)
                                .await
                                .map(|_| ())
                                .map_err(cas_failure),
                            Err(e) => Err(CasFailure::Failed(e)),
                        },
                    )
                }
                S3Op::InboxRun {
                    epoch,
                    node,
                    from,
                    width,
                } => S3Result::InboxRun(
                    InboxStore::new(store.clone())
                        .get_run(epoch, node, from, width)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxDrain { below_epoch } => S3Result::InboxDrain(
                    InboxStore::new(store.clone())
                        .drain_below(below_epoch)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxDelete { key } => S3Result::InboxDelete(
                    InboxStore::new(store.clone())
                        .delete(key)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxTombstone { batch } => S3Result::InboxTombstone(
                    InboxStore::new(store.clone())
                        .put_tombstone(&batch)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxLastN { epoch, node } => S3Result::InboxLastN(
                    InboxStore::new(store.clone())
                        .last_n(epoch, node)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::HeartbeatRead => S3Result::Heartbeats(
                    HeartbeatStore::new(store.clone())
                        .read_all()
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::HeartbeatPut { promise } => S3Result::HeartbeatPut(
                    HeartbeatStore::new(store.clone())
                        .put(&promise)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
            };
            let _ = tx.send(Event::S3 { op, result });
        });
    }

    /// `TreePublisher::publish` at this level: the log-prefix state of the
    /// replica (`Meta::publish_basis_at` substitution applied to a raw
    /// `ns` dump) becomes a real CAS-created commit whose root is the
    /// hash of that state, and the sim keeps the state for the checker.
    fn spawn_publish(&self, op: OpId, epoch: u64) {
        let meta = self.meta.clone();
        let tx = self.tx.clone();
        let store = self.store.clone();
        let shared = self.shared.clone();
        let node = self.id;
        let now = self.clock.now();
        tokio::spawn(async move {
            let snapshot =
                meta.read_consistent(|snap| -> Result<_, constellation_meta::MetaError> {
                    let applied = meta.applied_seq_at(snap)?;
                    let basis = meta.publish_basis_at(snap)?;
                    let dump = meta.ns_dump_at(snap)?;
                    Ok((applied, basis, dump))
                });
            let (applied, dump) = match snapshot {
                Ok((applied, PublishBasis::AsIs, dump)) => (applied, dump),
                Ok((applied, PublishBasis::Substituted(view), dump)) => {
                    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = dump.into_iter().collect();
                    for (key, before) in view.iter() {
                        match before {
                            Some(value) => {
                                map.insert(key.to_vec(), value.to_vec());
                            }
                            None => {
                                map.remove(key);
                            }
                        }
                    }
                    (applied, map.into_iter().collect())
                }
                Ok((_, PublishBasis::Defer, _)) | Err(_) => {
                    let _ = tx.send(Event::PublishDone { op, ok: false });
                    return;
                }
            };
            let mut hasher = blake3::Hasher::new();
            for (k, v) in &dump {
                hasher.update(&(k.len() as u64).to_le_bytes());
                hasher.update(k);
                hasher.update(&(v.len() as u64).to_le_bytes());
                hasher.update(v);
            }
            let root: [u8; 32] = *hasher.finalize().as_bytes();
            let chain = CommitChain::new(store);
            let mut head = shared
                .commits
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.seq)
                .max()
                .unwrap_or(0);
            let mut ok = false;
            for _ in 0..8 {
                let payload =
                    CommitPayload::single_root(constellation_mtree::NodeHash(root), Vec::new())
                        .with_author(node, epoch)
                        .with_applied(applied);
                let commit = payload.at(head + 1, now.wall.0);
                match chain.create(&commit).await {
                    Ok(()) => {
                        shared.commits.lock().unwrap().push(CommitRecord {
                            node,
                            seq: commit.seq,
                            applied,
                            dump,
                        });
                        ok = true;
                        break;
                    }
                    Err(StoreError::CasConflict) => match chain.discover_head(head).await {
                        Ok(Some(h)) => head = h,
                        _ => break,
                    },
                    Err(_) => break,
                }
            }
            let _ = tx.send(Event::PublishDone { op, ok });
        });
    }
}

/// See `Driver::spawn_rebuild`.
async fn rebuild_from_log(
    store: Arc<dyn object_store::ObjectStore>,
    node: NodeId,
    meta: &Meta,
    shared: &Shared,
    incarnation: u32,
) -> Result<(), String> {
    let log = LogStore::new(store);
    let side = Meta::open_in_memory().map_err(|e| e.to_string())?;
    side.set_node_prefix(3_000_000 + node)
        .map_err(|e| e.to_string())?;
    let (mut next, mut max_epoch) = (1u64, 0u64);
    loop {
        let run = log
            .get_run(next, 8)
            .await
            .map_err(|e| format!("reading segment {next}: {e}"))?;
        if run.is_empty() {
            break;
        }
        for (seq, payload) in run {
            let seg = constellation_authority::segment::decode(&payload)
                .map_err(|e| format!("segment {seq}: {e}"))?;
            if seg.epoch > 0 && seg.epoch < max_epoch {
                Replica::skip_segment(&side, seq).map_err(|e| e.to_string())?;
            } else {
                Replica::apply_segment(
                    &side,
                    seq,
                    seg.epoch,
                    seg.through,
                    &seg.rows,
                    &seg.origins,
                    &seg.records,
                )
                .map_err(|e| format!("applying segment {seq}: {e}"))?;
                max_epoch = max_epoch.max(seg.epoch);
            }
            next = seq + 1;
        }
    }
    // The process may have died, or restarted, while the log was read.
    if !shared.alive.load(Ordering::SeqCst)
        || shared.incarnation.load(Ordering::SeqCst) != incarnation
    {
        return Err("the node restarted during its rebuild".into());
    }
    meta.replace_ns_from_rebuilt(&side, &HashSet::new())
        .map_err(|e| e.to_string())
}

fn cas_failure(e: StoreError) -> CasFailure {
    match e {
        StoreError::CasConflict | StoreError::AlreadyExists => CasFailure::Conflict,
        other => CasFailure::Failed(other.to_string()),
    }
}

/// The current lease object, read straight from the bucket (checkers).
pub async fn read_lease(bucket: &Bucket) -> Option<constellation_store_s3::Lease> {
    let leases = LeaseStore::new(bucket.raw(), "p0", LeaseMode::Cas);
    leases.get().await.ok().flatten().map(|(l, _)| l)
}

/// Whether `meta` is exactly the log prefix (nothing outstanding).
pub fn is_quiescent(meta: &Meta) -> bool {
    !meta.has_outstanding_speculation()
        && Replica::journal_len(meta).unwrap_or(1) == 0
        && meta
            .pending_replays()
            .map(|q| q.is_empty())
            .unwrap_or(false)
}
