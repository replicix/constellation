//! The simulated P2P bus: seeded delay (which reorders), seeded drops,
//! partitions and dead nodes. A request (`PeerMsg::requests()`) that
//! cannot be delivered comes back to its sender as `Event::PeerFailed`,
//! the way a failed dial or a lost connection does through
//! `Peers::request_to_node`; a reply or a gossip push that is lost is just
//! lost — the core's own timers cover it.

use constellation_authority::{Event, NodeId, OpId, PeerMsg};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Bus {
    rng: Mutex<StdRng>,
    delay: (u64, u64),
    drop_p: f64,
    partitions: Mutex<HashSet<(NodeId, NodeId)>>,
    dead: Mutex<HashSet<NodeId>>,
    /// Ordered: `broadcast` draws one delay per recipient from the seeded
    /// rng, so recipient order must be the same on every replay.
    senders: Mutex<BTreeMap<NodeId, mpsc::UnboundedSender<Event>>>,
    /// Plan 30 M0's `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`: extra
    /// delay on a holder's `MutateReply`s, per sender.
    reply_delay: Mutex<HashMap<NodeId, u64>>,
    pub sent: Mutex<u64>,
    pub dropped: Mutex<u64>,
}

impl Bus {
    pub fn new(seed: u64, delay: (u64, u64), drop_p: f64) -> Arc<Self> {
        Arc::new(Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed ^ 0xb05_b05)),
            delay,
            drop_p,
            partitions: Mutex::new(HashSet::new()),
            dead: Mutex::new(HashSet::new()),
            senders: Mutex::new(BTreeMap::new()),
            reply_delay: Mutex::new(HashMap::new()),
            sent: Mutex::new(0),
            dropped: Mutex::new(0),
        })
    }

    pub fn attach(&self, node: NodeId, tx: mpsc::UnboundedSender<Event>) {
        self.senders.lock().unwrap().insert(node, tx);
        self.dead.lock().unwrap().remove(&node);
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
        *self.sent.lock().unwrap() += 1;
        let mut delay = self.delay();
        if matches!(msg, PeerMsg::MutateReply { .. }) {
            if let Some(extra) = self.reply_delay.lock().unwrap().get(&from) {
                delay += Duration::from_millis(*extra);
            }
        }
        let deliverable = self.deliverable(from, to);
        let req: Option<OpId> = msg.requests();
        let bus = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if deliverable {
                let tx = bus.senders.lock().unwrap().get(&to).cloned();
                if let Some(tx) = tx {
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

    /// Gossip to every attached node but the sender.
    pub fn broadcast(self: &Arc<Self>, from: NodeId, msg: PeerMsg) {
        for to in self.nodes() {
            if to != from {
                self.send(from, to, msg.clone());
            }
        }
    }
}
