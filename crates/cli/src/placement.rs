//! Holder-driven lease placement: migrate the lease to the write-rate
//! weighted medoid of recent writers (DESIGN.md placement ADR).

use constellation_net::{PathKind, Payload, Peers};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const WINDOW_BUCKETS: usize = 12; // 12 × 5 s = 60 s
const BUCKET_MS: u64 = 5_000;
const HYSTERESIS: f64 = 0.7;
const DWELL: Duration = Duration::from_secs(60);
/// Minimum absolute cost improvement (ms, RTT-weighted by op count)
/// required before a migration is worth it. RTT samples are single
/// unsmoothed readings, and several writers on the same LAN or on
/// loopback measure near-identical, noisy round-trips; without a floor
/// here the hysteresis ratio alone is not enough to hold still, because
/// a ratio test is meaningless once both costs are within measurement
/// noise of zero. That produced a live migrate-every-DWELL ping-pong
/// among four co-located writers hammering one inode, each holder
/// "recommending" a peer purely off sub-millisecond jitter.
const MIN_ABS_IMPROVEMENT_MS: f64 = 5.0;

pub fn placement_enabled() -> bool {
    match std::env::var("CONSTELLATION_LEASE_PLACEMENT") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    }
}

#[derive(Default)]
struct Bucket {
    /// ops per node_id in this bucket
    ops: HashMap<u64, u64>,
}

pub struct Placement {
    buckets: Mutex<VecDeque<Bucket>>,
    last_rotate: Mutex<Instant>,
    last_migrate: Mutex<Option<Instant>>,
    /// peer_id -> (from_id -> rtt_ms)
    rtts: Mutex<HashMap<u64, HashMap<u64, u16>>>,
    pub last_reason: Mutex<Option<String>>,
}

impl Placement {
    pub fn new() -> Self {
        let mut buckets = VecDeque::new();
        buckets.push_back(Bucket::default());
        Self {
            buckets: Mutex::new(buckets),
            last_rotate: Mutex::new(Instant::now()),
            last_migrate: Mutex::new(None),
            rtts: Mutex::new(HashMap::new()),
            last_reason: Mutex::new(None),
        }
    }

    fn maybe_rotate(&self) {
        let mut last = self.last_rotate.lock().unwrap();
        if last.elapsed() < Duration::from_millis(BUCKET_MS) {
            return;
        }
        *last = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        buckets.push_back(Bucket::default());
        while buckets.len() > WINDOW_BUCKETS {
            buckets.pop_front();
        }
    }

    pub fn note_forwarded(&self, requester: u64) {
        self.note_ops(requester, 1);
    }

    /// An op of this node's own client the core executed (or had
    /// forwarded).
    pub fn note_local(&self, node_id: u64) {
        self.note_ops(node_id, 1);
    }

    /// `n` writes by `node`. The holder's own FUSE fast path executes
    /// outside the core, so the driver counts those in bulk (M16: without
    /// them a busy holder looked idle to its own cost function, and one
    /// light remote writer could pull the lease away from it).
    pub fn note_ops(&self, node: u64, n: u64) {
        if n == 0 || !placement_enabled() {
            return;
        }
        self.maybe_rotate();
        let mut buckets = self.buckets.lock().unwrap();
        if let Some(b) = buckets.back_mut() {
            *b.ops.entry(node).or_insert(0) += n;
        }
    }

    pub fn note_peer_rtts(&self, from: u64, rtts: Vec<(u64, u16)>) {
        let mut map = self.rtts.lock().unwrap();
        map.insert(from, rtts.into_iter().collect());
    }

    fn ops_window(&self) -> HashMap<u64, u64> {
        let buckets = self.buckets.lock().unwrap();
        let mut out = HashMap::new();
        for b in buckets.iter() {
            for (id, n) in &b.ops {
                *out.entry(*id).or_insert(0) += *n;
            }
        }
        out
    }

    fn rtt(&self, a: u64, b: u64) -> Option<f64> {
        if a == b {
            return Some(0.0);
        }
        let map = self.rtts.lock().unwrap();
        if let Some(ms) = map.get(&a).and_then(|m| m.get(&b)) {
            return Some(*ms as f64);
        }
        if let Some(ms) = map.get(&b).and_then(|m| m.get(&a)) {
            return Some(*ms as f64);
        }
        None
    }

    fn cost(&self, holder: u64, ops: &HashMap<u64, u64>) -> Option<f64> {
        let mut total = 0.0;
        for (w, n) in ops {
            let rtt = self.rtt(holder, *w)?;
            total += (*n as f64) * rtt;
        }
        Some(total)
    }

    /// If a better holder exists, return its node id.
    pub fn recommend(&self, self_id: u64, peers: &Peers) -> Option<u64> {
        if !placement_enabled() {
            return None;
        }
        {
            let last = self.last_migrate.lock().unwrap();
            if let Some(t) = *last {
                if t.elapsed() < DWELL {
                    return None;
                }
            }
        }
        // Candidates must have a direct path.
        let direct: Vec<u64> = peers
            .snapshot()
            .iter()
            .filter(|p| p.path == PathKind::Direct)
            .map(|p| p.node_id)
            .collect();
        let (best, self_cost, best_cost) = self.best_holder(self_id, &direct)?;
        *self.last_reason.lock().unwrap() = Some(format!(
            "migrate {self_id} -> {best}: cost {self_cost:.0} -> {best_cost:.0}"
        ));
        Some(best)
    }

    /// The decision of [`Self::recommend`] over `candidates` (peers with
    /// a direct path): `(better holder, cost here, cost there)`.
    fn best_holder(&self, self_id: u64, candidates: &[u64]) -> Option<(u64, f64, f64)> {
        let ops = self.ops_window();
        if ops.is_empty() {
            return None;
        }
        let self_cost = self.cost(self_id, &ops)?;
        let mut best = self_id;
        let mut best_cost = self_cost;
        for &node in candidates {
            // Candidate must be a recent writer.
            if ops.get(&node).copied().unwrap_or(0) == 0 {
                continue;
            }
            let Some(c) = self.cost(node, &ops) else {
                continue;
            };
            if c < best_cost {
                best_cost = c;
                best = node;
            }
        }
        if best == self_id {
            return None;
        }
        if self_cost - best_cost < MIN_ABS_IMPROVEMENT_MS {
            return None;
        }
        if best_cost >= HYSTERESIS * self_cost {
            return None;
        }
        Some((best, self_cost, best_cost))
    }

    pub fn mark_migrated(&self) {
        *self.last_migrate.lock().unwrap() = Some(Instant::now());
    }

    /// Gossip our RTT vector.
    pub async fn gossip_rtts(&self, peers: &Peers, self_id: u64) {
        let snap = peers.snapshot();
        let rtts: Vec<(u64, u16)> = snap
            .iter()
            .filter_map(|p| {
                let ms = p.rtt_ms?;
                Some((p.node_id, ms.min(u16::MAX as u64) as u16))
            })
            .collect();
        // Also record our own view.
        self.note_peer_rtts(self_id, rtts.clone());
        let _ = peers
            .gossip(Payload::PeerRtts {
                node_id: self_id,
                rtts,
            })
            .await;
    }
}

impl Default for Placement {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M16: the holder's own writes count. A holder doing 200 fast-path
    /// writes and one peer 50 ms away forwarding 10: moving the lease
    /// would cost the holder's 200 ops a WAN hop each, so it stays. The
    /// same window without the holder's writes (what the cost function
    /// saw before the fast path was counted) moves it.
    #[test]
    fn a_busy_holders_fast_path_keeps_the_lease() {
        let p = Placement::new();
        p.note_peer_rtts(1, vec![(2, 50)]);
        for _ in 0..10 {
            p.note_forwarded(2);
        }
        assert_eq!(
            p.best_holder(1, &[2]).map(|(n, _, _)| n),
            Some(2),
            "only the remote writer is counted: the lease moves to it"
        );
        p.note_ops(1, 200);
        assert_eq!(
            p.best_holder(1, &[2]),
            None,
            "the holder's own writes keep it"
        );
    }
}
