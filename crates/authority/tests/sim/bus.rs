//! The simulated P2P bus: seeded delay (which reorders), seeded drops,
//! partitions and dead nodes. A request (`PeerMsg::requests()`) that
//! cannot be delivered comes back to its sender as `Event::PeerFailed`,
//! the way a failed dial or a lost connection does through
//! `Peers::request_to_node`; a reply or a gossip push that is lost is just
//! lost — the core's own timers cover it.
//!
//! Plan 30 §M7: log-stream frames (`LogStream`, `LogStreamEnd`) travel in
//! FIFO lanes per (holder, subscriber) pair, as on a QUIC stream, and the
//! stream faults break exactly what a stream promises: `stream_drop_p`
//! loses one frame (the subscriber must see the gap in the frame numbers),
//! `stream_reorder_p` delivers one frame late, past its successors, and
//! `stream_cut_p` drops the subscriber on the holder's side
//! (`Event::SubscriberGone`, the driver's overflow drop) while the
//! subscriber's end reports the broken stream (`Event::PeerFailed` for
//! its subscription). A frame to a dead node is the holder's write error:
//! `SubscriberGone`.

use constellation_authority::{Event, NodeId, OpId, PeerMsg};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// A node's tap on the messages it receives (see [`Bus::taps`]).
pub type Tap = Arc<dyn Fn(NodeId, &PeerMsg) + Send + Sync>;

/// What a beat of the holder's off-core heartbeat carries (plan 43 C7-H1):
/// the holder's wait clock reading when it sent it (`sent`) and its id
/// (`clock`), the end of its lease and its guard on that clock, the lease
/// body's wall expiry (`expires`), and the holder's wait reading `echo`,
/// which the daemon's holder keeps in the closure that waits for the answer
/// (`Event::BackupAlive.at`): the bus keeps it here by `(holder, backup,
/// seq)`.
#[derive(Clone, Copy, Debug)]
pub struct BeatStamps {
    pub clock: u64,
    pub sent: i64,
    pub holder_expires: i64,
    pub guard: i64,
    pub expires: i64,
    pub echo: i64,
}

thread_local! {
    /// The buses alive on this thread (a run's bus lives and dies on the
    /// thread that runs it: `run_seed`'s current-thread runtime). A run
    /// whose bus outlives it leaks every node's `Meta` with it (its store's
    /// threads and directory): `a_run_leaves_no_bus_behind`.
    pub static LIVE_BUSES: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

pub struct Bus {
    rng: Mutex<StdRng>,
    /// The holder's wait stamp of each beat in flight, for the answer.
    beat_echo: Mutex<BTreeMap<(NodeId, NodeId, u64), i64>>,
    delay: (u64, u64),
    drop_p: f64,
    partitions: Mutex<HashSet<(NodeId, NodeId)>>,
    dead: Mutex<HashSet<NodeId>>,
    /// Ordered: `broadcast` draws one delay per recipient from the seeded
    /// rng, so recipient order must be the same on every replay.
    senders: Mutex<BTreeMap<NodeId, mpsc::UnboundedSender<Event>>>,
    /// What a node's P2P service sees of a message as it arrives, before the
    /// node's driver queue (the daemon's handlers run off the core): the
    /// off-core renewal lane's tap on the root's answers.
    taps: Mutex<BTreeMap<NodeId, Tap>>,
    /// Plan 30 M0's `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`: extra
    /// delay on a holder's `MutateReply`s, per sender.
    reply_delay: Mutex<HashMap<NodeId, u64>>,
    /// `SimConfig::deleg_answer_delay`.
    deleg_answer_delay: Mutex<(u64, u64)>,
    /// `SimConfig::request_delay_to`.
    request_delay_to: Mutex<Option<(NodeId, (u64, u64))>>,
    /// Plan 43 C7b: when each `(from, to)` pair last had a message
    /// delivered (the lease-claim oracle's "the holder heard from the
    /// backup").
    last_delivered: Mutex<HashMap<(NodeId, NodeId), tokio::time::Instant>>,
    pub sent: Mutex<u64>,
    pub dropped: Mutex<u64>,
    /// M7: per (from, to) lane, when its last frame is delivered.
    lanes: Mutex<HashMap<(NodeId, NodeId), tokio::time::Instant>>,
    stream_faults: Mutex<StreamFaults>,
    pub stream_frames: Mutex<u64>,
    segment_frames: Mutex<u64>,
    pub stream_faults_injected: Mutex<u64>,
    /// Plan 30 §M9: the measured RTT the directory reports per pair (a
    /// "far" pair is out of the backup budget); default twice the max
    /// delay.
    rtts: Mutex<HashMap<(NodeId, NodeId), u64>>,
    /// Every off-core heartbeat beat is lost (`SimConfig::beats_lost`: the
    /// liveness lane broken while the data lane works).
    beats_lost: std::sync::atomic::AtomicBool,
    /// Plan 43 C7-F F-c (`SimConfig::lock_mirror_drop`): the probability
    /// that a `LockMirror` is lost, whatever the link.
    mirror_drop: Mutex<(f64, Option<tokio::time::Instant>)>,
}

/// M7: the stream faults (probabilities per frame).
#[derive(Debug, Clone, Default)]
pub struct StreamFaults {
    pub drop_p: f64,
    pub reorder_p: f64,
    pub cut_p: f64,
    /// Scripted loss: drop these segment-carrying frames (0-based, in the
    /// order the bus saw them, over every lane).
    pub drop_segment_frames: Vec<u64>,
}

impl Drop for Bus {
    fn drop(&mut self) {
        LIVE_BUSES.with(|n| n.set(n.get() - 1));
    }
}

impl Bus {
    pub fn new(seed: u64, delay: (u64, u64), drop_p: f64) -> Arc<Self> {
        LIVE_BUSES.with(|n| n.set(n.get() + 1));
        Arc::new(Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed ^ 0xb05_b05)),
            beat_echo: Mutex::new(BTreeMap::new()),
            delay,
            drop_p,
            partitions: Mutex::new(HashSet::new()),
            dead: Mutex::new(HashSet::new()),
            senders: Mutex::new(BTreeMap::new()),
            taps: Mutex::new(BTreeMap::new()),
            reply_delay: Mutex::new(HashMap::new()),
            deleg_answer_delay: Mutex::new((0, 0)),
            request_delay_to: Mutex::new(None),
            last_delivered: Mutex::new(HashMap::new()),
            sent: Mutex::new(0),
            dropped: Mutex::new(0),
            lanes: Mutex::new(HashMap::new()),
            stream_faults: Mutex::new(StreamFaults::default()),
            stream_frames: Mutex::new(0),
            segment_frames: Mutex::new(0),
            stream_faults_injected: Mutex::new(0),
            rtts: Mutex::new(HashMap::new()),
            beats_lost: std::sync::atomic::AtomicBool::new(false),
            mirror_drop: Mutex::new((0.0, None)),
        })
    }

    /// Lock mirrors are lost with probability `p` from `from_ms` after now
    /// on.
    pub fn set_mirror_drop(&self, p: f64, from_ms: u64) {
        *self.mirror_drop.lock().unwrap() = (
            p,
            Some(tokio::time::Instant::now() + Duration::from_millis(from_ms)),
        );
    }

    /// Whether the scripted loss takes this message (a `LockMirror`; a
    /// draw only when the loss is on, so no other configuration's stream
    /// moves).
    fn mirror_lost(&self, msg: &PeerMsg) -> bool {
        let (p, from) = *self.mirror_drop.lock().unwrap();
        p > 0.0
            && matches!(msg, PeerMsg::LockMirror { .. })
            && from.is_some_and(|f| tokio::time::Instant::now() >= f)
            && self.rng.lock().unwrap().random_bool(p)
    }

    /// Plan 30 §M9: what the directory reports as the RTT from `a` to
    /// `b` (both directions).
    pub fn set_rtt(&self, a: NodeId, b: NodeId, rtt_ms: u64) {
        let mut r = self.rtts.lock().unwrap();
        r.insert((a, b), rtt_ms);
        r.insert((b, a), rtt_ms);
    }

    pub fn rtt(&self, a: NodeId, b: NodeId) -> u64 {
        self.rtts
            .lock()
            .unwrap()
            .get(&(a, b))
            .copied()
            .unwrap_or(2 * self.delay.1.max(1))
    }

    /// See `SimConfig::beats_lost`.
    pub fn set_beats_lost(&self, lost: bool) {
        self.beats_lost
            .store(lost, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_stream_faults(&self, faults: StreamFaults) {
        *self.stream_faults.lock().unwrap() = faults;
    }

    fn roll(&self, p: f64) -> bool {
        p > 0.0 && self.rng.lock().unwrap().random_bool(p.min(1.0))
    }

    pub fn attach(&self, node: NodeId, tx: mpsc::UnboundedSender<Event>) {
        self.senders.lock().unwrap().insert(node, tx);
        self.dead.lock().unwrap().remove(&node);
    }

    /// Replace `node`'s tap (see [`Bus::taps`]).
    pub fn set_tap(&self, node: NodeId, tap: Tap) {
        self.taps.lock().unwrap().insert(node, tap);
    }

    pub fn detach(&self, node: NodeId) {
        self.senders.lock().unwrap().remove(&node);
        self.dead.lock().unwrap().insert(node);
    }

    /// Cut (or heal) both directions between `a` and `b`.
    pub fn set_partition(&self, a: NodeId, b: NodeId, cut: bool) {
        let mut p = self.partitions.lock().unwrap();
        if cut {
            p.insert((a, b));
            p.insert((b, a));
        } else {
            p.remove(&(a, b));
            p.remove(&(b, a));
        }
    }

    pub fn set_deleg_answer_delay(&self, range: (u64, u64)) {
        *self.deleg_answer_delay.lock().unwrap() = range;
    }

    /// How long ago `from` last had a message delivered to `to` (`None`:
    /// never).
    pub fn since_delivery(&self, from: NodeId, to: NodeId) -> Option<Duration> {
        self.last_delivered
            .lock()
            .unwrap()
            .get(&(from, to))
            .map(|t| t.elapsed())
    }

    pub fn set_request_delay_to(&self, delay: Option<(NodeId, (u64, u64))>) {
        *self.request_delay_to.lock().unwrap() = delay;
    }

    pub fn set_reply_delay(&self, node: NodeId, delay_ms: Option<u64>) {
        let mut d = self.reply_delay.lock().unwrap();
        match delay_ms {
            Some(ms) => {
                d.insert(node, ms);
            }
            None => {
                d.remove(&node);
            }
        }
    }

    pub fn nodes(&self) -> Vec<NodeId> {
        self.senders.lock().unwrap().keys().copied().collect()
    }

    /// Whether `a` can currently reach `b` (no partition, `b` attached):
    /// what the driver's peer directory reports as `connected`.
    pub fn linked(&self, a: NodeId, b: NodeId) -> bool {
        !self.partitions.lock().unwrap().contains(&(a, b))
            && !self.dead.lock().unwrap().contains(&b)
            && self.senders.lock().unwrap().contains_key(&b)
    }

    fn delay(&self) -> Duration {
        let (lo, hi) = self.delay;
        let ms = if hi > lo {
            self.rng.lock().unwrap().random_range(lo..=hi)
        } else {
            lo
        };
        Duration::from_millis(ms)
    }

    fn deliverable(&self, from: NodeId, to: NodeId) -> bool {
        if self.partitions.lock().unwrap().contains(&(from, to)) {
            return false;
        }
        if self.dead.lock().unwrap().contains(&to) {
            return false;
        }
        if self.drop_p > 0.0 && self.rng.lock().unwrap().random_bool(self.drop_p) {
            return false;
        }
        true
    }

    pub fn send(self: &Arc<Self>, from: NodeId, to: NodeId, msg: PeerMsg) {
        if matches!(
            msg,
            PeerMsg::LogStream { .. } | PeerMsg::LogStreamEnd { .. }
        ) {
            self.send_frame(from, to, msg);
            return;
        }
        *self.sent.lock().unwrap() += 1;
        let mut delay = self.delay();
        if matches!(msg, PeerMsg::MutateReply { .. }) {
            if let Some(extra) = self.reply_delay.lock().unwrap().get(&from) {
                delay += Duration::from_millis(*extra);
            }
        }
        if let (PeerMsg::MutateRequest { tag, .. }, Some((target, (lo, hi)))) =
            (&msg, *self.request_delay_to.lock().unwrap())
        {
            // Only an op that carries a lock token is late: the delay of
            // every request would starve the clients instead.
            if to == target && hi > 0 && !tag.is_empty() {
                let extra = self.rng.lock().unwrap().random_range(lo..=hi);
                delay += Duration::from_millis(extra);
            }
        }
        if matches!(
            msg,
            PeerMsg::DelegateStreamAck { .. } | PeerMsg::DelegRenewed { .. }
        ) {
            let (lo, hi) = *self.deleg_answer_delay.lock().unwrap();
            if hi > 0 {
                let extra = self.rng.lock().unwrap().random_range(lo..=hi);
                delay += Duration::from_millis(extra);
            }
        }
        let deliverable = self.deliverable(from, to) && !self.mirror_lost(&msg);
        let req: Option<OpId> = msg.requests();
        let bus = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if deliverable {
                let tx = bus.senders.lock().unwrap().get(&to).cloned();
                if let Some(tx) = tx {
                    let tap = bus.taps.lock().unwrap().get(&to).cloned();
                    if let Some(tap) = tap {
                        tap(from, &msg);
                    }
                    bus.last_delivered
                        .lock()
                        .unwrap()
                        .insert((from, to), tokio::time::Instant::now());
                    let _ = tx.send(Event::Peer { from, msg });
                    return;
                }
            }
            *bus.dropped.lock().unwrap() += 1;
            if let Some(req) = req {
                let tx = bus.senders.lock().unwrap().get(&from).cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(Event::PeerFailed {
                        req,
                        to,
                        outage: true,
                    });
                }
            }
        });
    }

    /// M7: one log-stream frame, in its lane.
    fn send_frame(self: &Arc<Self>, from: NodeId, to: NodeId, msg: PeerMsg) {
        *self.sent.lock().unwrap() += 1;
        *self.stream_frames.lock().unwrap() += 1;
        let req = match &msg {
            PeerMsg::LogStream { req, .. } | PeerMsg::LogStreamEnd { req, .. } => *req,
            _ => unreachable!(),
        };
        if self.dead.lock().unwrap().contains(&to) {
            // The holder's write fails: its driver drops the subscriber.
            self.deliver_later(
                from,
                Duration::ZERO,
                Event::SubscriberGone { node: to, req },
            );
            return;
        }
        let faults = self.stream_faults.lock().unwrap().clone();
        let is_frame = matches!(msg, PeerMsg::LogStream { .. });
        if matches!(
            msg,
            PeerMsg::LogStream {
                segment: Some(_),
                ..
            }
        ) {
            let mut n = self.segment_frames.lock().unwrap();
            let index = *n;
            *n += 1;
            if faults.drop_segment_frames.contains(&index) {
                *self.stream_faults_injected.lock().unwrap() += 1;
                *self.dropped.lock().unwrap() += 1;
                return;
            }
        }
        if is_frame && self.roll(faults.cut_p) {
            *self.stream_faults_injected.lock().unwrap() += 1;
            self.deliver_later(
                from,
                Duration::ZERO,
                Event::SubscriberGone { node: to, req },
            );
            self.deliver_later(
                to,
                self.delay(),
                Event::PeerFailed {
                    req,
                    to: from,
                    outage: false,
                },
            );
            return;
        }
        if is_frame && self.roll(faults.drop_p) {
            *self.stream_faults_injected.lock().unwrap() += 1;
            *self.dropped.lock().unwrap() += 1;
            return;
        }
        if !self.deliverable(from, to) {
            *self.dropped.lock().unwrap() += 1;
            return;
        }
        let now = tokio::time::Instant::now();
        let mut at = now + self.delay();
        if is_frame && self.roll(faults.reorder_p) {
            // Late, past the frames sent after it; the lane does not move.
            *self.stream_faults_injected.lock().unwrap() += 1;
            let last = self.lanes.lock().unwrap().get(&(from, to)).copied();
            at = at.max(last.unwrap_or(now)) + Duration::from_millis(30);
        } else {
            let mut lanes = self.lanes.lock().unwrap();
            let lane = lanes.entry((from, to)).or_insert(now);
            at = at.max(*lane + Duration::from_millis(1));
            *lane = at;
        }
        let bus = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(at).await;
            let tx = bus.senders.lock().unwrap().get(&to).cloned();
            match tx {
                Some(tx) => {
                    let _ = tx.send(Event::Peer { from, msg });
                }
                None => *bus.dropped.lock().unwrap() += 1,
            }
        });
    }

    /// One beat of `from`'s off-core heartbeat (`Event::HolderAlive`,
    /// `Payload::HolderAlive` in the daemon) to `to`: a request whose
    /// answer nothing waits on, so a lost one is just lost. Its arrival
    /// stamp is the receiving driver's (`node.rs`).
    pub fn send_beat(
        self: &Arc<Self>,
        (from, to): (NodeId, NodeId),
        epoch: u64,
        seq: u64,
        stamps: BeatStamps,
    ) {
        *self.sent.lock().unwrap() += 1;
        if self.beats_lost.load(std::sync::atomic::Ordering::Relaxed) {
            *self.dropped.lock().unwrap() += 1;
            return;
        }
        if !self.deliverable(from, to) {
            *self.dropped.lock().unwrap() += 1;
            return;
        }
        {
            let mut echo = self.beat_echo.lock().unwrap();
            echo.insert((from, to, seq), stamps.echo);
            // Bounded: a beat that was never answered leaves its stamp.
            while echo.len() > 4096 {
                echo.pop_first();
            }
        }
        let delay = self.delay();
        self.deliver_later(
            to,
            delay,
            Event::HolderAlive {
                from,
                epoch,
                candidacy: 0,
                listed: true,
                at: constellation_authority::WaitMs(0),
                seq,
                clock: stamps.clock,
                sent: stamps.sent,
                holder_expires: stamps.holder_expires,
                guard: stamps.guard,
                expires: constellation_authority::Ms(stamps.expires),
            },
        );
    }

    /// `from`'s answer to a beat of `to`'s sent at `sent`
    /// (`Payload::HolderAliveAck` in the daemon): `Event::BackupAlive` at
    /// the holder. Lost like the beat when the link is.
    pub fn answer_beat(self: &Arc<Self>, (from, to): (NodeId, NodeId), seq: u64) {
        if !self.deliverable(from, to) {
            return;
        }
        // The holder's own stamp of that beat (its wait reading: the core
        // compares the echo with its own stamps).
        let Some(sent) = self.beat_echo.lock().unwrap().remove(&(to, from, seq)) else {
            return;
        };
        let delay = self.delay();
        self.deliver_later(
            to,
            delay,
            Event::BackupAlive {
                from,
                at: constellation_authority::WaitMs(sent),
            },
        );
    }

    fn deliver_later(self: &Arc<Self>, to: NodeId, delay: Duration, event: Event) {
        let bus = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let tx = bus.senders.lock().unwrap().get(&to).cloned();
            if let Some(tx) = tx {
                let _ = tx.send(event);
            }
        });
    }

    /// Gossip to every attached node but the sender.
    pub fn broadcast(self: &Arc<Self>, from: NodeId, msg: PeerMsg) {
        for to in self.nodes() {
            if to != from {
                self.send(from, to, msg.clone());
            }
        }
    }
}
