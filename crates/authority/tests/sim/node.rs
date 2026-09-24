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
use super::clock::Clock;
use super::store::Bucket;
use constellation_authority::action::ControlOk;
use constellation_authority::{
    Action, Carrier, CasFailure, ClientReply, Config, Control, Core, EpochClaimView, Event, NodeId,
    OpId, PeerLink, PeerMsg, Policy, Replica, S3Failure, S3Op, S3Result, Seq, Stats, UploadResult,
};
use constellation_meta::{Meta, MutateOp, PublishBasis, Rid};
use constellation_store_s3::commits::{CommitChain, CommitPayload};
use constellation_store_s3::heartbeat::HeartbeatStore;
use constellation_store_s3::inbox::InboxStore;
use constellation_store_s3::{LeaseMode, LeaseStore, LogStore, StoreError};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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
    pub s3_held: Option<(u64, i64)>,
    /// Plan 30 §M11.
    pub delegation: constellation_authority::core::DelegView,
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
    pub conflict_copies: AtomicU64,
    /// Rids whose effects were rolled back at some point (see
    /// `history.rs`): read from the replay queue whenever the core's
    /// rollback counters move.
    pub tentative: Mutex<BTreeSet<Rid>>,
    pub commits: Arc<Mutex<Vec<CommitRecord>>>,
    /// Plan 30 §M10.
    pub epoch: Mutex<SimEpoch>,
}

pub struct NodeHandle {
    pub id: NodeId,
    pub meta: Arc<Meta>,
    pub shared: Arc<Shared>,
    /// This node's clock (plan 30 §M8: possibly skewed).
    pub clock: Clock,
    tx: mpsc::UnboundedSender<Event>,
    task: tokio::task::JoinHandle<()>,
}

pub struct NodeEnv {
    pub bucket: Arc<Bucket>,
    pub bus: Arc<Bus>,
    pub clock: Clock,
    pub commits: Arc<Mutex<Vec<CommitRecord>>>,
    pub config: Arc<dyn Fn(NodeId, u32) -> Config + Send + Sync>,
    /// See `SimConfig::panic_after_events`.
    pub panic_after_events: Option<u64>,
    /// Plan 30 §M8: every node's clock is off by up to this (ms), by a
    /// seeded constant per node.
    pub clock_skew_ms: i64,
    pub seed: u64,
}

impl NodeEnv {
    /// Node `id`'s clock: the shared one, off by a seeded constant within
    /// `±clock_skew_ms`.
    pub fn clock_of(&self, id: NodeId) -> Clock {
        if self.clock_skew_ms == 0 {
            return self.clock;
        }
        let h = (self.seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let span = 2 * self.clock_skew_ms + 1;
        self.clock
            .skewed((h % span as u64) as i64 - self.clock_skew_ms)
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
                Arc::new(m)
            }
        };
        let incarnation = meta.bump_incarnation().expect("incarnation");
        // Plan 30 §M6: the session state is the process's; a restart is a
        // new FUSE session.
        meta.session().reset();
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            replies: Mutex::new(HashMap::new()),
            controls: Mutex::new(HashMap::new()),
            next_control: AtomicU64::new(1 << 48),
            view: Mutex::new(CoreView::default()),
            incarnation: AtomicU32::new(incarnation),
            next_seq: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            paused_until: Mutex::new(None),
            conflict_copies: AtomicU64::new(0),
            tentative: Mutex::new(BTreeSet::new()),
            commits: env.commits.clone(),
            epoch: Mutex::new(epoch),
        });
        env.bus.attach(id, tx.clone());
        let cfg = (env.config)(id, incarnation);
        let driver = Driver {
            id,
            meta: meta.clone(),
            core: Core::new(cfg),
            store: env.bucket.handle(id),
            bus: env.bus.clone(),
            clock: env.clock_of(id),
            tx: tx.clone(),
            rx,
            shared: shared.clone(),
            rolled_back_seen: (0, 0),
            timers: HashSet::new(),
            timer_kinds: HashMap::new(),
            bootstrap: None,
            deferred: Vec::new(),
            events_handled: 0,
            panic_after_events: env.panic_after_events,
        };
        let task = tokio::spawn(driver.run());
        NodeHandle {
            id,
            meta,
            shared,
            clock: env.clock_of(id),
            tx,
            task,
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
        let (tx, rx) = oneshot::channel();
        if !self.alive() {
            return rx;
        }
        self.shared.replies.lock().unwrap().insert(rid, tx);
        let _ = self.tx.send(Event::Submit {
            rid,
            op,
            policy: Policy::Client,
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
    }
}

struct Driver {
    id: NodeId,
    meta: Arc<Meta>,
    core: Core,
    store: Arc<dyn object_store::ObjectStore>,
    bus: Arc<Bus>,
    clock: Clock,
    tx: mpsc::UnboundedSender<Event>,
    rx: mpsc::UnboundedReceiver<Event>,
    shared: Arc<Shared>,
    rolled_back_seen: (u64, u64),
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
}

impl Driver {
    async fn run(mut self) {
        let now = self.clock.now();
        let mut out = Vec::new();
        self.core.start(now, &*self.meta, &mut out);
        self.dispatch(out);
        let bootstrap = OpId(u64::MAX);
        self.bootstrap = Some(bootstrap);
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
            let p2p = self.core.config().p2p;
            let clock = self.clock;
            tokio::spawn(async move {
                // Plan 30 §M9: since when each link has been up without a
                // break (the directory's `since`).
                let mut up_since: HashMap<NodeId, constellation_authority::Ms> = HashMap::new();
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
                                Some(*up_since.entry(n).or_insert(now))
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
                    if tx.send(Event::Peers { links }).is_err() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            });
        }
        while let Some(event) = self.rx.recv().await {
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
                *clock = now.0 as u64;
                *hist.entry(format!("node{}:{kind}", self.id)).or_default() += 1;
                *IN_FLIGHT.lock().unwrap() = Some((self.id, text, std::time::Instant::now()));
            }
            self.events_handled += 1;
            if self.id == 1 && self.panic_after_events == Some(self.events_handled) {
                panic!(
                    "injected panic after {} events (SimConfig::panic_after_events)",
                    self.events_handled
                );
            }
            let actions = self.core.handle(now, event, &*self.meta);
            if std::env::var_os("AUTHORITY_SIM_WATCHDOG").is_some() {
                *IN_FLIGHT.lock().unwrap() = None;
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
                        "job={:?} lease={:?} gate={} ack={:?} clients={clients:?}",
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
                    now = now.0,
                    event = %debug_event,
                    applied = Replica::applied_seq(&*self.meta).unwrap_or(0),
                    ?listing,
                    "replica"
                );
            }
            self.note_rollbacks();
            self.dispatch(actions);
            self.refresh_view();
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
        view.clients_in_flight = self.core.clients().count();
        view.next_seq = self.core.ship().next_seq;
        view.events_handled += 1;
        view.applied_seq = Replica::applied_seq(&*self.meta).unwrap_or(0);
        view.journal_len = Replica::journal_len(&*self.meta).unwrap_or(0);
        view.speculation = self.meta.speculation_counts().unwrap_or_default();
        view.ack = self.core.ack_view();
        view.delegation = self.core.deleg_view();
        let now = self.clock.now();
        view.claim = self.core.epoch_claim_view(now);
        view.epoch_held = lease.epoch_held();
        view.s3_held = match &lease.held {
            Some((l, _)) if !lease.lost && !lease.epoch_held() && l.holder == self.id => {
                Some((l.epoch, l.expires_unix_ms))
            }
            _ => None,
        };
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
        if let Ok(queue) = self.meta.pending_replays() {
            let mut t = self.shared.tentative.lock().unwrap();
            for q in queue {
                t.insert(q.rid);
            }
        }
    }

    fn dispatch(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Reply { rid, reply } => {
                    if let Some(tx) = self.shared.replies.lock().unwrap().remove(&rid) {
                        let _ = tx.send(reply);
                    }
                }
                Action::Send { to, msg } => self.bus.send(self.id, to, msg),
                Action::S3 { op, req } => self.spawn_s3(op, req),
                Action::SetTimer { id, at, kind } => {
                    self.timers.insert(id);
                    self.timer_kinds.insert(id, kind);
                    let tx = self.tx.clone();
                    let when = self.clock.at(at);
                    tokio::spawn(async move {
                        tokio::time::sleep_until(when).await;
                        let _ = tx.send(Event::Timer { id });
                    });
                }
                Action::CancelTimer { id } => {
                    self.timers.remove(&id);
                    self.timer_kinds.remove(&id);
                }
                Action::UploadDirtyChunks { op, .. } => {
                    // No chunks in this simulation: every upload pass is
                    // immediately complete and holds nothing back.
                    let _ = self.tx.send(Event::UploadsDone {
                        op,
                        result: UploadResult::Done { held: 0 },
                    });
                }
                Action::Publish { op, epoch } => self.spawn_publish(op, epoch),
                Action::FollowHead { op } => {
                    let _ = self.tx.send(Event::PublishDone { op, ok: true });
                }
                Action::RebuildReplica { op } => {
                    // Holder capture is on in the simulation: never asked.
                    let _ = self.tx.send(Event::RebuildDone { op, ok: false });
                }
                Action::RoundDone { .. } => {}
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

    fn spawn_s3(&self, op: OpId, req: S3Op) {
        let store = self.store.clone();
        let tx = self.tx.clone();
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
                    S3Result::SegmentPut(log.put_segment(seq, &payload).await.map_err(cas_failure))
                }
                S3Op::SegmentRun { from, width } => S3Result::SegmentRun(
                    log.get_run(from, width)
                        .await
                        .map_err(|e| S3Failure(e.to_string())),
                ),
                S3Op::InboxPut { batch } => {
                    let inbox = InboxStore::new(store.clone());
                    S3Result::InboxPut(
                        inbox
                            .put_batch(&batch)
                            .await
                            .map(|_| ())
                            .map_err(cas_failure),
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
                let commit = payload.at(head + 1, now.0);
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
