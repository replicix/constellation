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
/// The newest buckets (the last 5-10 s): a move must also pay off for
/// who is writing *now*. The 60 s window alone moved the lease away from
/// a holder that had just started writing to a node whose burst had
/// ended; that node never wrote again, the holder's next close had to
/// take the lease back through S3, and every writer and reader stalled
/// for the round trips of two handoffs (3-5 s at 300 ms per S3 request;
/// EC2 campaign 6, the visibility-s3-latency scenario).
const RECENT_BUCKETS: usize = 2;
/// See [`Placement::writing_now`].
const OFFER_ACTIVE: Duration = Duration::from_secs(3);
/// How long a claim counts as the answer to an offer this node made
/// ([`Placement::declines_claim`]).
const OFFER_VALID: Duration = Duration::from_secs(10);
/// Timestamped op counts kept for [`Placement::ops_within`].
const RECENT_KEEP: Duration = Duration::from_secs(10);
/// See [`Placement::declined`].
const DECLINE_QUIET: Duration = Duration::from_secs(30);

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
    /// When this node's own clients last wrote (see [`Self::writing_now`]).
    last_own: Mutex<Option<Instant>>,
    /// The last [`RECENT_KEEP`] of op counts, timestamped: who is
    /// writing *now*, at a finer grain than the 5 s buckets.
    recent: Mutex<VecDeque<(Instant, u64, u64)>>,
    /// The node this holder last offered the lease to, and when.
    offered: Mutex<Option<(u64, Instant)>>,
    /// When this holder last declined a claim of its offer: no new offer
    /// for [`DECLINE_QUIET`] (the decision settles rather than cycling
    /// offer, claim, decline every evaluation).
    declined: Mutex<Option<Instant>>,
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
            last_own: Mutex::new(None),
            recent: Mutex::new(VecDeque::new()),
            offered: Mutex::new(None),
            declined: Mutex::new(None),
        }
    }

    /// Ops of `node` in the last `window` (at most [`RECENT_KEEP`]).
    pub fn ops_within(&self, node: u64, window: Duration) -> u64 {
        let recent = self.recent.lock().unwrap();
        recent
            .iter()
            .rev()
            .take_while(|(at, _, _)| at.elapsed() < window)
            .filter(|(_, n, _)| *n == node)
            .map(|(_, _, ops)| *ops)
            .sum()
    }

    /// This holder offered the lease to `node`.
    pub fn note_offer(&self, node: u64) {
        *self.offered.lock().unwrap() = Some((node, Instant::now()));
    }

    /// Whether this holder should decline `requester`'s claim of its
    /// offer. A placement offer is made on up to a minute of history and
    /// claimed a moment later; by then the holder may be the one writing
    /// (a burst on the requester ended and one here began). Handing the
    /// lease over then moved the sequencer away from the active writer,
    /// whose next writes took it back through S3 — a flip and a flop,
    /// seconds of stalls at slow S3 (`visibility-s3-latency`, 1 in 5).
    /// So a claim is declined while this node's clients wrote more than
    /// the requester's in the last [`OFFER_ACTIVE`]. Only claims of an
    /// offer this node made in the last [`OFFER_VALID`] are judged: other
    /// lease requests (a node that cannot forward) keep today's rules.
    pub fn declines_claim(&self, self_id: u64, requester: u64) -> bool {
        let offered = self
            .offered
            .lock()
            .unwrap()
            .is_some_and(|(n, at)| n == requester && at.elapsed() < OFFER_VALID);
        let decline = offered
            && self.ops_within(self_id, OFFER_ACTIVE) > self.ops_within(requester, OFFER_ACTIVE);
        if decline {
            *self.declined.lock().unwrap() = Some(Instant::now());
        }
        decline
    }

    /// One new bucket per [`BUCKET_MS`] elapsed (an idle stretch leaves
    /// empty buckets behind, so "recent" means recent).
    fn maybe_rotate(&self) {
        let mut last = self.last_rotate.lock().unwrap();
        let due = (last.elapsed().as_millis() / u128::from(BUCKET_MS)) as usize;
        if due == 0 {
            return;
        }
        *last += Duration::from_millis(BUCKET_MS * due as u64);
        let mut buckets = self.buckets.lock().unwrap();
        for _ in 0..due.min(WINDOW_BUCKETS) {
            buckets.push_back(Bucket::default());
        }
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
        self.note_own(node_id, 1);
    }

    /// `n` writes by this node's own clients (`node_id` is this node).
    pub fn note_own(&self, node_id: u64, n: u64) {
        if n > 0 {
            *self.last_own.lock().unwrap() = Some(Instant::now());
        }
        self.note_ops(node_id, n);
    }

    /// Whether this node's own clients wrote in the last
    /// [`OFFER_ACTIVE`]. An offered lease is claimed only then: the
    /// holder's window (up to a minute of history) can name a node whose
    /// writes have ended, and taking the lease there only moves the
    /// sequencer away from whoever writes now — through S3, a few round
    /// trips each way.
    pub fn writing_now(&self) -> bool {
        self.last_own
            .lock()
            .unwrap()
            .is_some_and(|t| t.elapsed() < OFFER_ACTIVE)
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
        {
            let mut buckets = self.buckets.lock().unwrap();
            if let Some(b) = buckets.back_mut() {
                *b.ops.entry(node).or_insert(0) += n;
            }
        }
        let now = Instant::now();
        let mut recent = self.recent.lock().unwrap();
        while recent
            .front()
            .is_some_and(|(at, _, _)| now.duration_since(*at) >= RECENT_KEEP)
        {
            recent.pop_front();
        }
        recent.push_back((now, node, n));
    }

    pub fn note_peer_rtts(&self, from: u64, rtts: Vec<(u64, u16)>) {
        let mut map = self.rtts.lock().unwrap();
        map.insert(from, rtts.into_iter().collect());
    }

    fn ops_window(&self) -> HashMap<u64, u64> {
        self.ops_last(WINDOW_BUCKETS)
    }

    /// Ops per node in the newest `n` buckets.
    fn ops_last(&self, n: usize) -> HashMap<u64, u64> {
        self.maybe_rotate();
        let buckets = self.buckets.lock().unwrap();
        let mut out = HashMap::new();
        for b in buckets.iter().rev().take(n) {
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
        if self
            .declined
            .lock()
            .unwrap()
            .is_some_and(|t| t.elapsed() < DECLINE_QUIET)
        {
            return None;
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
        // And for the writers of the last few seconds: a candidate that
        // is not writing now (or a holder that writes alone now) keeps
        // the lease where it is.
        let recent = self.ops_last(RECENT_BUCKETS);
        let (Some(here), Some(there)) = (self.cost(self_id, &recent), self.cost(best, &recent))
        else {
            return None;
        };
        if recent.get(&best).copied().unwrap_or(0) == 0 || there >= HYSTERESIS * here {
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

    /// Campaign 6: a remote writer's burst fills the window, then it
    /// stops and the holder starts writing. The window alone still
    /// favours the old writer; the lease must stay with the holder, who
    /// is the one writing now.
    #[test]
    fn a_finished_burst_does_not_pull_the_lease_from_the_writer_of_now() {
        let p = Placement::new();
        p.note_peer_rtts(1, vec![(2, 50)]);
        p.note_ops(2, 60);
        // The burst ended two buckets ago; the holder writes since.
        {
            let mut b = p.buckets.lock().unwrap();
            b.push_back(Bucket::default());
            b.push_back(Bucket::default());
        }
        p.note_ops(1, 35);
        assert_eq!(p.best_holder(1, &[2]), None);
        // Were the old writer still writing now, the move stands.
        p.note_ops(2, 60);
        assert_eq!(p.best_holder(1, &[2]).map(|(n, _, _)| n), Some(2));
    }

    /// `visibility-s3-latency`: the holder offers the lease to a node
    /// whose burst dominated the window; by the time the claim arrives
    /// the holder is the one writing. It declines; once it is idle and
    /// the requester writes again, it hands over. A claim it never
    /// offered is not judged here.
    #[test]
    fn a_claim_is_declined_while_the_holder_writes_more() {
        let p = Placement::new();
        p.note_ops(2, 50);
        p.note_offer(2);
        assert!(
            !p.declines_claim(1, 2),
            "the requester writes, the holder not"
        );
        p.note_ops(1, 60);
        assert!(p.declines_claim(1, 2), "the holder writes more now");
        assert!(!p.declines_claim(1, 3), "never offered to node 3");
        // Older than the active window: only what happens now counts.
        {
            let mut r = p.recent.lock().unwrap();
            let old = Instant::now() - OFFER_ACTIVE - Duration::from_millis(1);
            for e in r.iter_mut() {
                e.0 = old;
            }
        }
        p.note_ops(2, 1);
        assert!(!p.declines_claim(1, 2));
    }

    #[test]
    fn idle_time_rotates_the_buckets() {
        let p = Placement::new();
        p.note_ops(2, 10);
        *p.last_rotate.lock().unwrap() -= Duration::from_millis(BUCKET_MS * 3);
        assert!(
            p.ops_last(RECENT_BUCKETS).is_empty(),
            "15 s ago is not recent"
        );
        assert_eq!(p.ops_window().get(&2), Some(&10));
    }
}
