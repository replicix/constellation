//! Latency-adaptive source selection (DESIGN.md §7).
//!
//! Pure scoring: no I/O. The fetch path feeds every real transfer back
//! in through [`Selector::record_ok`] / [`Selector::record_err`] and
//! asks [`Selector::pick`] / [`Selector::hedge_deadline_ms`] for the
//! next decision.
//!
//! Score is predicted `ETA = TTFB_ewma + size/goodput_ewma + penalties`
//! (recent errors, in-flight queue). A challenger must beat the
//! incumbent by more than [`HYSTERESIS`] (~20%) before we switch.
//! Hedging fires when a fetch outlives its predicted P95 first byte
//! *plus* its predicted body time; at most one hedge per fetch (the
//! caller enforces the cap).
//!
//! Rendezvous hashing is a composable alternative for "which peer
//! should hold this chunk" — this module only ranks sources that already
//! claim to have it.

use constellation_net::PathKind;
use std::collections::HashMap;

/// Switch only when the challenger is this much faster than the incumbent.
pub const HYSTERESIS: f64 = 0.20;
/// Headroom on the hedge deadline over the predicted transfer, so
/// ordinary jitter does not fire a second request on every fetch.
const HEDGE_SLACK: f64 = 1.5;
const ALPHA: f64 = 0.25;
/// Prior for a **peer** we have never timed: LAN-ish 2 ms TTFB and
/// 100 MiB/s. Real samples replace this within a handful of fetches.
const PEER_PRIOR_TTFB_MS: f64 = 2.0;
const PEER_PRIOR_GOODPUT_BPS: f64 = 100.0 * 1024.0 * 1024.0;
const PEER_PRIOR_P95_MS: f64 = 8.0;
/// Unsampled S3 is the correctness anchor, not a LAN peer. A 50 ms
/// TTFB prior keeps it behind a healthy peer (DESIGN.md §7 last
/// resort) while still beating a peer that just errored (~50 ms of
/// `err_rate` penalty on top of that peer's TTFB).
const S3_PRIOR_TTFB_MS: f64 = 50.0;
const S3_PRIOR_GOODPUT_BPS: f64 = 50.0 * 1024.0 * 1024.0;
const S3_PRIOR_P95_MS: f64 = 80.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceId {
    S3,
    Peer(u64),
}

impl SourceId {
    pub fn label(self) -> String {
        match self {
            Self::S3 => "s3".into(),
            Self::Peer(id) => format!("peer-{id}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceStats {
    pub ttfb_ewma_ms: f64,
    pub goodput_bps: f64,
    pub err_rate: f64,
    pub ttfb_p95_ms: f64,
    pub in_flight: u32,
    pub samples: u64,
    pub transport_rtt_ms: Option<f64>,
    pub path: PathKind,
}

impl Default for SourceStats {
    fn default() -> Self {
        Self::peer_prior()
    }
}

impl SourceStats {
    fn peer_prior() -> Self {
        Self {
            ttfb_ewma_ms: PEER_PRIOR_TTFB_MS,
            goodput_bps: PEER_PRIOR_GOODPUT_BPS,
            err_rate: 0.0,
            ttfb_p95_ms: PEER_PRIOR_P95_MS,
            in_flight: 0,
            samples: 0,
            transport_rtt_ms: None,
            path: PathKind::Unknown,
        }
    }

    fn s3_prior() -> Self {
        Self {
            ttfb_ewma_ms: S3_PRIOR_TTFB_MS,
            goodput_bps: S3_PRIOR_GOODPUT_BPS,
            err_rate: 0.0,
            ttfb_p95_ms: S3_PRIOR_P95_MS,
            in_flight: 0,
            samples: 0,
            transport_rtt_ms: None,
            path: PathKind::Unknown,
        }
    }

    fn prior(id: SourceId) -> Self {
        match id {
            SourceId::S3 => Self::s3_prior(),
            SourceId::Peer(_) => Self::peer_prior(),
        }
    }

    pub fn eta_ms(&self, size: u64) -> f64 {
        let xfer = if self.goodput_bps > 1.0 {
            (size as f64) / self.goodput_bps * 1000.0
        } else {
            1_000.0
        };
        let err_pen = self.err_rate * 200.0;
        let q_pen = f64::from(self.in_flight) * self.ttfb_ewma_ms * 0.5;
        let transport_floor = self.transport_rtt_ms.unwrap_or(0.0);
        let path_penalty = if self.path == PathKind::Relay {
            20.0
        } else {
            0.0
        };
        self.ttfb_ewma_ms.max(transport_floor) + xfer + err_pen + q_pen + path_penalty
    }
}

#[derive(Debug, Default)]
pub struct Selector {
    stats: HashMap<SourceId, SourceStats>,
    incumbent: Option<SourceId>,
}

impl Selector {
    pub fn stats(&self, id: SourceId) -> SourceStats {
        self.stats
            .get(&id)
            .cloned()
            .unwrap_or_else(|| SourceStats::prior(id))
    }

    pub fn all_stats(&self) -> Vec<(SourceId, SourceStats)> {
        let mut v: Vec<_> = self.stats.iter().map(|(k, v)| (*k, v.clone())).collect();
        v.sort_by_key(|(id, _)| match id {
            SourceId::S3 => 0,
            SourceId::Peer(n) => *n,
        });
        v
    }

    pub fn eta_ms(&self, id: SourceId, size: u64) -> f64 {
        self.stats(id).eta_ms(size)
    }

    /// Best source among `candidates`. S3 is always a legal last resort
    /// even if it was not listed — the caller usually includes it.
    pub fn pick(&mut self, candidates: &[SourceId], size: u64) -> SourceId {
        if candidates.is_empty() {
            return SourceId::S3;
        }
        let mut best = candidates[0];
        let mut best_eta = self.eta_ms(best, size);
        for &c in &candidates[1..] {
            let eta = self.eta_ms(c, size);
            if eta < best_eta {
                best = c;
                best_eta = eta;
            }
        }
        if let Some(inc) = self.incumbent {
            if candidates.contains(&inc) {
                let inc_eta = self.eta_ms(inc, size);
                // Challenger must be >20% faster.
                if best != inc && best_eta * (1.0 + HYSTERESIS) >= inc_eta {
                    return inc;
                }
            }
        }
        self.incumbent = Some(best);
        best
    }

    /// Ranked remaining sources for a hedge (excludes `primary`).
    pub fn next_best(
        &self,
        candidates: &[SourceId],
        primary: SourceId,
        size: u64,
    ) -> Option<SourceId> {
        candidates
            .iter()
            .copied()
            .filter(|c| *c != primary)
            .min_by(|a, b| {
                self.eta_ms(*a, size)
                    .partial_cmp(&self.eta_ms(*b, size))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Wait this long for the *whole* chunk before firing a hedge.
    ///
    /// The caller races this against a fetch that only resolves once the
    /// last byte has landed, so a bare P95-first-byte deadline would
    /// expire on every healthy transfer — it compares a first-byte
    /// estimate against an end-to-end outcome. Budgeting the expected
    /// body time too, plus [`HEDGE_SLACK`], makes the hedge mean what
    /// DESIGN.md §7 intends: this source is running late, not merely
    /// still running.
    pub fn hedge_deadline_ms(&self, id: SourceId, size: u64) -> u64 {
        let s = self.stats(id);
        let xfer = if s.goodput_bps > 1.0 {
            (size as f64) / s.goodput_bps * 1000.0
        } else {
            1_000.0
        };
        ((s.ttfb_p95_ms + xfer) * HEDGE_SLACK).max(1.0).ceil() as u64
    }

    pub fn begin(&mut self, id: SourceId) {
        self.stats
            .entry(id)
            .or_insert_with(|| SourceStats::prior(id))
            .in_flight += 1;
    }

    pub fn end(&mut self, id: SourceId) {
        if let Some(s) = self.stats.get_mut(&id) {
            s.in_flight = s.in_flight.saturating_sub(1);
        }
    }

    /// A raced request was canceled because another source completed.
    /// This is not a sample and not an error; only release its queue slot.
    pub fn record_cancelled(&mut self, id: SourceId) {
        self.end(id);
    }

    pub fn record_transport(
        &mut self,
        id: SourceId,
        rtt: Option<std::time::Duration>,
        path: PathKind,
    ) {
        let stats = self
            .stats
            .entry(id)
            .or_insert_with(|| SourceStats::prior(id));
        stats.transport_rtt_ms = rtt.map(|value| value.as_secs_f64() * 1000.0);
        stats.path = path;
    }

    pub fn record_ok(&mut self, id: SourceId, ttfb_ms: f64, bytes: u64, total_ms: f64) {
        let s = self
            .stats
            .entry(id)
            .or_insert_with(|| SourceStats::prior(id));
        if s.samples == 0 {
            s.ttfb_ewma_ms = ttfb_ms.max(0.1);
            s.ttfb_p95_ms = (ttfb_ms * 2.0).max(ttfb_ms + 1.0);
        } else {
            s.ttfb_ewma_ms = ALPHA * ttfb_ms + (1.0 - ALPHA) * s.ttfb_ewma_ms;
            // Cheap P95 stand-in: track a high EWMA of observed TTFB.
            let high = ttfb_ms.max(s.ttfb_ewma_ms);
            s.ttfb_p95_ms = ALPHA * high * 1.5 + (1.0 - ALPHA) * s.ttfb_p95_ms;
        }
        if total_ms > ttfb_ms && bytes > 0 {
            let body_s = ((total_ms - ttfb_ms) / 1000.0).max(0.000_001);
            let gp = bytes as f64 / body_s;
            s.goodput_bps = if s.samples == 0 {
                gp
            } else {
                ALPHA * gp + (1.0 - ALPHA) * s.goodput_bps
            };
        }
        s.err_rate *= 1.0 - ALPHA;
        s.samples += 1;
        s.in_flight = s.in_flight.saturating_sub(1);
    }

    pub fn record_err(&mut self, id: SourceId) {
        let s = self
            .stats
            .entry(id)
            .or_insert_with(|| SourceStats::prior(id));
        s.err_rate = ALPHA * 1.0 + (1.0 - ALPHA) * s.err_rate;
        s.samples += 1;
        s.in_flight = s.in_flight.saturating_sub(1);
        // A dead peer should lose the incumbent slot so the next pick
        // can leave it.
        if self.incumbent == Some(id) {
            self.incumbent = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_candidates_fall_back_to_s3() {
        let mut s = Selector::default();
        assert_eq!(s.pick(&[], 1000), SourceId::S3);
    }

    #[test]
    fn faster_peer_wins_on_a_cold_selector() {
        let mut s = Selector::default();
        // Seed S3 as slow, peer 2 as fast.
        s.record_ok(SourceId::S3, 200.0, 1_000_000, 400.0);
        s.record_ok(SourceId::Peer(2), 1.0, 1_000_000, 10.0);
        let pick = s.pick(&[SourceId::S3, SourceId::Peer(2)], 1_000_000);
        assert_eq!(pick, SourceId::Peer(2));
    }

    #[test]
    fn hysteresis_keeps_the_incumbent_for_a_small_gap() {
        let mut s = Selector::default();
        s.record_ok(SourceId::Peer(1), 10.0, 1_000_000, 20.0);
        s.record_ok(SourceId::Peer(2), 11.0, 1_000_000, 21.0);
        let first = s.pick(&[SourceId::Peer(1), SourceId::Peer(2)], 1_000_000);
        assert_eq!(first, SourceId::Peer(1));
        // Peer 2 is slightly faster now but not 20% faster.
        s.record_ok(SourceId::Peer(2), 9.5, 1_000_000, 19.0);
        let second = s.pick(&[SourceId::Peer(1), SourceId::Peer(2)], 1_000_000);
        assert_eq!(second, SourceId::Peer(1), "must hold the incumbent");
    }

    #[test]
    fn hysteresis_yields_when_the_challenger_is_clearly_faster() {
        let mut s = Selector::default();
        s.record_ok(SourceId::S3, 200.0, 1_000_000, 400.0);
        s.pick(&[SourceId::S3], 1000); // incumbent = S3
        s.record_ok(SourceId::Peer(1), 2.0, 1_000_000, 5.0);
        let pick = s.pick(&[SourceId::S3, SourceId::Peer(1)], 1_000_000);
        assert_eq!(pick, SourceId::Peer(1));
    }

    const MIB: u64 = 1024 * 1024;

    /// The bug this encodes: the deadline used to be a P95 *first byte*
    /// while the caller races it against a fetch that resolves on the
    /// *last* byte, so every healthy transfer tripped a hedge and the
    /// fleet issued two requests for every chunk.
    #[test]
    fn a_healthy_transfer_finishes_inside_its_own_hedge_deadline() {
        let mut s = Selector::default();
        // 1 MiB, first byte at 100 ms, done at 200 ms.
        s.record_ok(SourceId::S3, 100.0, MIB, 200.0);
        let deadline = s.hedge_deadline_ms(SourceId::S3, MIB);
        assert!(
            deadline > 200,
            "an identical repeat would hedge: deadline {deadline} ms vs a 200 ms transfer"
        );
        assert!(
            deadline < 1_000,
            "deadline {deadline} ms is too loose to catch a stalled source"
        );
    }

    /// A cold source must not hedge on the mere fact that a large chunk
    /// takes longer than a small one to arrive.
    #[test]
    fn hedge_deadline_grows_with_the_chunk_size() {
        let s = Selector::default();
        let small = s.hedge_deadline_ms(SourceId::Peer(1), MIB);
        let large = s.hedge_deadline_ms(SourceId::Peer(1), 8 * MIB);
        assert!(large > small, "{large} ms is not later than {small} ms");
    }

    /// Goodput has to move off its prior, or the transfer term of the
    /// ETA is a constant and selection degenerates to TTFB-only.
    #[test]
    fn goodput_is_learned_from_the_body_time() {
        let mut s = Selector::default();
        let prior = s.stats(SourceId::S3).goodput_bps;
        s.record_ok(SourceId::S3, 100.0, MIB, 200.0);
        let learned = s.stats(SourceId::S3).goodput_bps;
        let expected = MIB as f64 / 0.1; // 1 MiB body in 100 ms
        assert!(
            (learned - expected).abs() / expected < 0.01,
            "learned {learned} B/s, expected ~{expected} B/s (prior was {prior})"
        );
    }

    /// Why the fetch path must measure the two halves separately: one
    /// combined duration carries no body time and teaches nothing.
    #[test]
    fn a_sample_with_no_body_time_leaves_goodput_untouched() {
        let mut s = Selector::default();
        let prior = s.stats(SourceId::S3).goodput_bps;
        s.record_ok(SourceId::S3, 200.0, MIB, 200.0);
        assert_eq!(s.stats(SourceId::S3).goodput_bps, prior);
    }

    #[test]
    fn error_evicts_the_incumbent() {
        let mut s = Selector::default();
        s.record_ok(SourceId::Peer(3), 2.0, 1000, 4.0);
        assert_eq!(
            s.pick(&[SourceId::Peer(3), SourceId::S3], 1000),
            SourceId::Peer(3)
        );
        s.record_err(SourceId::Peer(3));
        assert!(s.stats(SourceId::Peer(3)).err_rate > 0.0);
        // After eviction, a slow-but-reliable S3 can win against a
        // high-error peer on the next pick.
        let pick = s.pick(&[SourceId::Peer(3), SourceId::S3], 1000);
        assert_eq!(pick, SourceId::S3);
    }

    #[test]
    fn a_cancelled_race_loser_is_not_an_error_sample() {
        let mut s = Selector::default();
        let id = SourceId::Peer(3);
        s.begin(id);
        s.record_cancelled(id);
        let stats = s.stats(id);
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.samples, 0);
        assert_eq!(stats.err_rate, 0.0);
    }

    #[test]
    fn transport_rtt_and_relay_path_break_a_cold_peer_tie() {
        let mut s = Selector::default();
        let direct = SourceId::Peer(1);
        let relay = SourceId::Peer(2);
        s.record_transport(
            direct,
            Some(std::time::Duration::from_millis(3)),
            PathKind::Direct,
        );
        s.record_transport(
            relay,
            Some(std::time::Duration::from_millis(3)),
            PathKind::Relay,
        );
        assert_eq!(s.pick(&[relay, direct], 1 << 20), direct);
    }

    #[test]
    fn measured_transfer_data_can_override_a_path_penalty() {
        let mut s = Selector::default();
        let direct = SourceId::Peer(1);
        let relay = SourceId::Peer(2);
        s.record_transport(
            direct,
            Some(std::time::Duration::from_millis(50)),
            PathKind::Direct,
        );
        s.record_ok(direct, 50.0, 1 << 20, 150.0);
        s.record_transport(
            relay,
            Some(std::time::Duration::from_millis(2)),
            PathKind::Relay,
        );
        s.record_ok(relay, 2.0, 1 << 20, 8.0);
        assert_eq!(s.pick(&[direct, relay], 1 << 20), relay);
    }

    #[test]
    fn next_best_skips_the_primary() {
        let mut s = Selector::default();
        s.record_ok(SourceId::Peer(1), 5.0, 1000, 10.0);
        s.record_ok(SourceId::Peer(2), 50.0, 1000, 80.0);
        s.record_ok(SourceId::S3, 200.0, 1000, 400.0);
        let c = [SourceId::Peer(1), SourceId::Peer(2), SourceId::S3];
        assert_eq!(
            s.next_best(&c, SourceId::Peer(1), 1000),
            Some(SourceId::Peer(2))
        );
    }
}
