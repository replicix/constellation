//! Additive-increase / multiplicative-decrease search for upload
//! concurrency, and the elastic gate that enforces it.
//!
//! A single upload's wall time is dominated by RTT to the bucket
//! region, not by the local uplink. A high-RTT, high-bandwidth path (a
//! home connection in the EU against a `us-west-2` bucket, say) needs
//! many requests in flight at once to fill that bandwidth-delay
//! product; a fixed pool size picked for one path is wrong for
//! another. [`AdaptiveConcurrency`] estimates aggregate goodput from
//! per-upload service time and uses slow-start doubling, then
//! coarse-to-fine probes: a clear goodput gain keeps climbing; a clear
//! regression reverts and halves the step; a noisy window is measured
//! again instead of treating WAN jitter as a knee. Periodic probes
//! detect improved conditions. Upload failures are congestion signals,
//! but a burst from already in-flight requests is coalesced rather than
//! repeatedly halving the target to one.
//!
//! This is a pure policy: it only tracks numbers and returns the
//! target concurrency. The caller owns the actual gate ([`ConcurrencyGate`],
//! semaphore-like) that enforces the target and feeds back
//! `on_success`/`on_error`.
//!
//! Extracted from `constellation`'s `cli::writeback` so `bench/uploadbench`
//! can exercise the exact production algorithm — not a re-implementation
//! that could silently drift from what actually ships.

mod gate;

pub use gate::{ConcurrencyGate, ConcurrencyPermit, OwnedConcurrencyPermit};

use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct AdaptiveConcurrency {
    current: usize,
    min: usize,
    max: usize,
    window_start: Instant,
    window_bytes: u64,
    window_service_seconds: f64,
    window_completed: u64,
    /// Throughput measured at the last accepted concurrency.
    baseline_throughput: f64,
    stable_windows: u32,
    /// Windows discarded after changing the gate target. Completions in
    /// the first window were partly issued at the old target, so comparing
    /// it with a steady-state baseline gives a systematically wrong result.
    settling_windows: u32,
    /// Accepted concurrency before an upward probe. The baseline remains
    /// the throughput measured at this target until the probe is accepted.
    pre_probe_current: Option<usize>,
    /// Step selected by a rejected probe. Halving the failed step turns the
    /// coarse multiplicative climb into a local search around the knee.
    next_probe_step: Option<usize>,
    /// Coalesce a burst of failures from requests that were already in
    /// flight when the first congestion signal arrived.
    last_backoff: Option<Instant>,
    /// Consecutive probe windows that were neither a clear gain nor a
    /// clear regression. Live paths (high RTT, completion batching)
    /// otherwise fail a 5% gain test on noise and stall at the start
    /// concurrency.
    probe_inconclusive: u32,
    probe_interval: Duration,
    max_window: Duration,
    error_backoff_interval: Duration,
}

impl AdaptiveConcurrency {
    /// Re-evaluate after this many samples land in a window, provided the
    /// window has also run for at least [`Self::PROBE_INTERVAL`].
    pub const MIN_WINDOW_SAMPLES: u64 = 3;
    /// Evaluate a window at this age even if samples are still trickling
    /// in — otherwise a low-throughput window at concurrency 1 would
    /// never accumulate `MIN_WINDOW_SAMPLES` and the search would stall.
    /// High-RTT uploads often take 1–4 s each, so 2 s windows were mostly
    /// noise and caused false "no gain" rejections on live WAN paths.
    pub const PROBE_INTERVAL: Duration = Duration::from_secs(3);
    pub const MAX_WINDOW: Duration = Duration::from_secs(8);
    /// Windows held flat before re-probing upward, in case the path
    /// improved (more peer bandwidth free, less contention, etc).
    const REPROBE_EVERY: u32 = 4;
    /// A probe must buy at least this much total goodput to keep climbing.
    /// Per-slot throughput is deliberately not used: it naturally falls on
    /// every smoothly saturating real link, even while another upload
    /// still adds substantial total throughput.
    const MIN_PROBE_GAIN: f64 = 1.05;
    /// A clear goodput drop (5%) reverts immediately. The band between
    /// this and [`MIN_PROBE_GAIN`] needs a second window before it is
    /// treated as a knee, so a single noisy WAN sample cannot stall the
    /// search.
    const PROBE_REGRESS: f64 = 0.95;
    /// Noisy probe windows to collect before treating the band as a
    /// plateau rather than keeping an open verdict.
    const INCONCLUSIVE_BEFORE_DECIDE: u32 = 2;
    /// One full observation window is discarded after every target change.
    const SETTLING_WINDOWS: u32 = 1;
    #[cfg(test)]
    const ERROR_BACKOFF_INTERVAL: Duration = Self::MAX_WINDOW;

    pub fn new(initial: usize, min: usize, max: usize) -> Self {
        Self::with_intervals(initial, min, max, Self::PROBE_INTERVAL, Self::MAX_WINDOW)
    }

    /// Construct the same controller with observation windows tuned to the
    /// caller's transfer duration. Fetch readahead uses shorter windows than
    /// multi-second uploads so a finite read can adapt before it completes.
    pub fn with_intervals(
        initial: usize,
        min: usize,
        max: usize,
        probe_interval: Duration,
        max_window: Duration,
    ) -> Self {
        let min = min.max(1);
        let max = max.max(min);
        let probe_interval = probe_interval.max(Duration::from_millis(1));
        let max_window = max_window.max(probe_interval);
        Self {
            current: initial.clamp(min, max),
            min,
            max,
            window_start: Instant::now(),
            window_bytes: 0,
            window_service_seconds: 0.0,
            window_completed: 0,
            baseline_throughput: 0.0,
            stable_windows: 0,
            settling_windows: 0,
            pre_probe_current: None,
            next_probe_step: None,
            last_backoff: None,
            probe_inconclusive: 0,
            probe_interval,
            max_window,
            error_backoff_interval: max_window,
        }
    }

    pub fn current(&self) -> usize {
        self.current
    }

    /// Record a completed upload of `bytes` with its end-to-end service
    /// `latency`, observed at `now`. Returns the (possibly unchanged)
    /// target concurrency.
    ///
    /// The window objective is `target × bytes / service_time`, not bytes
    /// crossing an arbitrary wall-clock window boundary. Concurrent
    /// requests tend to finish in batches, so completion-count windows can
    /// make identical operating points alternate between apparently fast
    /// and slow. Service time estimates aggregate goodput without that
    /// phase error, and also naturally captures network/S3 queueing.
    pub fn on_success(&mut self, now: Instant, bytes: u64, latency: Duration) -> usize {
        self.window_bytes += bytes;
        self.window_service_seconds += latency.as_secs_f64().max(0.001);
        self.window_completed += 1;
        let elapsed = now.saturating_duration_since(self.window_start);
        let min_samples = Self::MIN_WINDOW_SAMPLES.max((self.current as u64 / 2).min(12));
        let ready = (elapsed >= self.probe_interval && self.window_completed >= min_samples)
            || elapsed >= self.max_window;
        if !ready {
            return self.current;
        }
        let throughput =
            self.current as f64 * self.window_bytes as f64 / self.window_service_seconds;
        self.evaluate_window(throughput, now);
        self.current
    }

    /// Record an upload that failed all its retries, observed at `now`.
    /// The first failure is an immediate congestion signal. Further
    /// failures from the same in-flight burst are coalesced.
    pub fn on_error(&mut self, now: Instant) -> usize {
        if self
            .last_backoff
            .is_some_and(|last| now.saturating_duration_since(last) < self.error_backoff_interval)
        {
            return self.current;
        }
        self.last_backoff = Some(now);
        let target = (self.current / 2).max(self.min);
        self.current = target;
        self.reset_window(now);
        // Drain completions issued at the old, overloaded target before
        // establishing a baseline at the reduced target.
        self.settling_windows = Self::SETTLING_WINDOWS;
        self.baseline_throughput = 0.0;
        self.stable_windows = 0;
        self.pre_probe_current = None;
        self.next_probe_step = None;
        self.probe_inconclusive = 0;
        self.current
    }

    fn evaluate_window(&mut self, throughput: f64, now: Instant) {
        if self.settling_windows > 0 {
            self.settling_windows -= 1;
            self.reset_window(now);
            return;
        }

        if let Some(pre_probe) = self.pre_probe_current {
            let attempted_step = self.current.saturating_sub(pre_probe).max(1);
            if throughput >= self.baseline_throughput * Self::MIN_PROBE_GAIN {
                // The extra concurrency bought meaningful total goodput.
                // Accept it and immediately test the next step. If this was
                // already a narrowed probe, retain that local step instead
                // of jumping back to slow-start doubling.
                self.pre_probe_current = None;
                self.probe_inconclusive = 0;
                self.baseline_throughput = throughput;
                self.stable_windows = 0;
                if attempted_step < pre_probe.max(1) {
                    self.next_probe_step = Some(attempted_step);
                }
                self.start_probe();
            } else {
                // No clear gain. Could be the knee or a noisy window —
                // confirm before reverting so WAN jitter does not stall
                // slow-start at the opening concurrency.
                self.probe_inconclusive += 1;
                if throughput < self.baseline_throughput * Self::PROBE_REGRESS
                    || self.probe_inconclusive >= Self::INCONCLUSIVE_BEFORE_DECIDE
                {
                    self.pre_probe_current = None;
                    self.probe_inconclusive = 0;
                    self.current = pre_probe;
                    self.next_probe_step = Some((attempted_step / 2).max(1));
                    self.stable_windows = 0;
                    self.settling_windows = Self::SETTLING_WINDOWS;
                }
            }
            self.reset_window(now);
            return;
        }

        if self.baseline_throughput <= 0.0 {
            self.baseline_throughput = throughput;
            self.stable_windows = 0;
            self.start_probe();
        } else {
            // Same-target measurements are noisy in production. Track them
            // with an EWMA, but do not interpret an unrelated bandwidth dip
            // as evidence that concurrency itself is too high.
            self.baseline_throughput = self.baseline_throughput * 0.75 + throughput * 0.25;
            self.stable_windows += 1;
            if self.stable_windows.is_multiple_of(Self::REPROBE_EVERY) {
                self.start_probe();
            }
        }
        self.reset_window(now);
    }

    fn start_probe(&mut self) {
        let previous = self.current;
        // Slow-start doubling fills high-bandwidth-delay paths in a
        // handful of windows. Rejected probes install a smaller
        // local-search step.
        let step = self
            .next_probe_step
            .take()
            .unwrap_or_else(|| self.current.max(1));
        self.current = (self.current + step).min(self.max);
        if self.current != previous {
            self.pre_probe_current = Some(previous);
            self.settling_windows = Self::SETTLING_WINDOWS;
            self.probe_inconclusive = 0;
        }
    }

    fn reset_window(&mut self, now: Instant) {
        self.window_start = now;
        self.window_bytes = 0;
        self.window_service_seconds = 0.0;
        self.window_completed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed one full window's worth of samples at a given achieved
    /// throughput (bytes/sec) and return the resulting target.
    fn feed_window(ac: &mut AdaptiveConcurrency, now: &mut Instant, throughput: f64) -> usize {
        *now += AdaptiveConcurrency::PROBE_INTERVAL;
        let bytes = 4 * 1024 * 1024;
        let latency =
            Duration::from_secs_f64(bytes as f64 * ac.current() as f64 / throughput.max(1.0));
        let mut result = ac.current();
        debug_assert_eq!(ac.window_completed, 0);
        for _ in 0..64 {
            result = ac.on_success(*now, bytes, latency);
            if ac.window_completed == 0 {
                break;
            }
        }
        result
    }

    #[test]
    fn adaptive_concurrency_new_clamps_to_bounds() {
        assert_eq!(AdaptiveConcurrency::new(100, 1, 10).current(), 10);
        assert_eq!(AdaptiveConcurrency::new(0, 1, 10).current(), 1);
        assert_eq!(AdaptiveConcurrency::new(4, 8, 32).current(), 8);
    }

    /// A high-RTT/high-bandwidth path only gets more goodput out of more
    /// concurrency up to a point (the link's bandwidth-delay product). The
    /// search should climb toward that knee and stop overshooting wildly.
    #[test]
    fn adaptive_concurrency_climbs_toward_a_throughput_plateau() {
        let mut ac = AdaptiveConcurrency::new(2, 1, 64);
        let mut now = Instant::now();
        let plateau_at = 16usize;
        let per_conn = 2_000_000.0; // bytes/sec per connection below the knee
        for _ in 0..40 {
            let effective = ac.current().min(plateau_at);
            feed_window(&mut ac, &mut now, effective as f64 * per_conn);
        }
        assert!(
            ac.current() >= plateau_at - 4,
            "expected to climb near the {plateau_at}-way knee, got {}",
            ac.current()
        );
        assert!(
            ac.current() <= plateau_at + 8,
            "expected to settle near the knee rather than run away, got {}",
            ac.current()
        );
    }

    #[test]
    fn adaptive_concurrency_backs_off_multiplicatively_on_error() {
        let mut ac = AdaptiveConcurrency::new(16, 1, 64);
        let mut now = Instant::now();
        assert_eq!(ac.on_error(now), 8);
        // Errors from uploads already in flight at the old target are one
        // congestion episode, not independent reasons to keep halving.
        assert_eq!(ac.on_error(now), 8);
        now += AdaptiveConcurrency::ERROR_BACKOFF_INTERVAL;
        assert_eq!(ac.on_error(now), 4);
        now += AdaptiveConcurrency::ERROR_BACKOFF_INTERVAL;
        assert_eq!(ac.on_error(now), 2);
        now += AdaptiveConcurrency::ERROR_BACKOFF_INTERVAL;
        assert_eq!(ac.on_error(now), 1);
        // Never below the floor.
        now += AdaptiveConcurrency::ERROR_BACKOFF_INTERVAL;
        assert_eq!(ac.on_error(now), 1);
    }

    #[test]
    fn adaptive_concurrency_climbs_a_smoothly_saturating_link() {
        let mut ac = AdaptiveConcurrency::new(4, 1, 64);
        let mut now = Instant::now();
        let rtt_seconds = 0.150;
        let object_bytes = 4_194_304.0;
        let link_bytes_per_second = 125_000_000.0;

        // Shared-pipe model: every added request lowers per-slot goodput,
        // while total throughput still improves smoothly toward the link
        // limit. This is the shape that exposed the old efficiency bug.
        for _ in 0..50 {
            let n = ac.current() as f64;
            let throughput =
                n * object_bytes / (rtt_seconds + object_bytes * n / link_bytes_per_second);
            feed_window(&mut ac, &mut now, throughput);
        }

        assert!(
            ac.current() >= 12,
            "expected to exploit a smooth high-BDP link, got {}",
            ac.current()
        );
        // The shared-pipe model still pays a few percent at 32→64 (87% vs
        // 93% of link). Doubling therefore legally lands on the cap; the
        // important failure mode is stalling at the start concurrency.
        assert!(
            ac.current() <= 64,
            "expected not to exceed the configured max, got {}",
            ac.current()
        );
    }

    #[test]
    fn adaptive_concurrency_holds_near_flat_throughput() {
        let mut ac = AdaptiveConcurrency::new(4, 1, 64);
        let mut now = Instant::now();
        // First window establishes the baseline and probes upward once.
        let after_first = feed_window(&mut ac, &mut now, 1_000_000.0);
        // Subsequent windows at the *same* throughput are "flat": the
        // search must not keep compounding growth the way it does when
        // throughput is genuinely still improving.
        let mut last = after_first;
        for _ in 0..8 {
            last = feed_window(&mut ac, &mut now, 1_000_000.0);
        }
        assert!(
            last <= after_first + 8,
            "flat throughput must not cause runaway growth: {after_first} -> {last}"
        );
    }

    #[test]
    fn adaptive_concurrency_climbs_through_noisy_windows() {
        let mut ac = AdaptiveConcurrency::new(4, 1, 64);
        let mut now = Instant::now();
        let per_conn = 2_000_000.0;
        let knee = 32usize;
        for i in 0..60 {
            let effective = ac.current().min(knee) as f64;
            let noise = if i % 2 == 0 { 0.88 } else { 1.12 };
            feed_window(&mut ac, &mut now, effective * per_conn * noise);
        }
        assert!(
            ac.current() >= 16,
            "±12% window noise must not stall slow-start, got {}",
            ac.current()
        );
    }

    #[test]
    fn adaptive_concurrency_recovers_after_error_cooldown() {
        let mut ac = AdaptiveConcurrency::new(16, 1, 64);
        let mut now = Instant::now();
        ac.on_error(now);
        assert_eq!(ac.current(), 8);
        // The first window drains completions issued at the overloaded
        // target; the next establishes a clean baseline and probes up.
        let during_settle = feed_window(&mut ac, &mut now, 3_000_000.0);
        assert_eq!(during_settle, 8, "settling window must hold, not grow");
        let mut grew = during_settle;
        for _ in 0..8 {
            let throughput = ac.current() as f64 * 500_000.0;
            grew = feed_window(&mut ac, &mut now, throughput);
        }
        assert!(grew > 8, "search must resume climbing after settling");
    }
}
