//! Concurrency controllers under comparison.
//!
//! Every controller answers the same two questions from the same two
//! events — "a request just succeeded, carrying `bytes`" and "a request
//! just failed after retries" — and returns the concurrency target it
//! wants next. [`crate::run`] drives the target through a
//! [`constellation_upload_concurrency::ConcurrencyGate`], the exact
//! primitive `constellation` uses in production, so the comparison is
//! about the *policy*, not the plumbing.

use std::time::{Duration, Instant};

pub trait Controller: Send {
    fn name(&self) -> &'static str;
    fn current(&self) -> usize;
    /// A request succeeded, transferring `bytes` over `latency`.
    fn on_success(&mut self, now: Instant, bytes: u64, latency: Duration) -> usize;
    /// A request failed after exhausting retries. `slowdown` distinguishes
    /// an explicit S3 503 SlowDown / throttling response (or, in `sim`
    /// mode, its synthetic equivalent) from a generic failure — both are
    /// congestion signals, but a real deployment may want to log them
    /// differently.
    fn on_error(&mut self, now: Instant, latency: Duration, slowdown: bool) -> usize;
}

/// Baseline: never adapts. The point of the comparison.
pub struct FixedController {
    concurrency: usize,
}

impl FixedController {
    pub fn new(concurrency: usize) -> Self {
        Self {
            concurrency: concurrency.max(1),
        }
    }
}

impl Controller for FixedController {
    fn name(&self) -> &'static str {
        "fixed"
    }
    fn current(&self) -> usize {
        self.concurrency
    }
    fn on_success(&mut self, _now: Instant, _bytes: u64, _latency: Duration) -> usize {
        self.concurrency
    }
    fn on_error(&mut self, _now: Instant, _latency: Duration, _slowdown: bool) -> usize {
        self.concurrency
    }
}

/// "This implementation": the exact policy `constellation` ships,
/// unmodified, imported from `constellation-upload-concurrency`.
pub struct AimdController {
    inner: constellation_upload_concurrency::AdaptiveConcurrency,
}

impl AimdController {
    pub fn new(initial: usize, min: usize, max: usize) -> Self {
        Self {
            inner: constellation_upload_concurrency::AdaptiveConcurrency::new(initial, min, max),
        }
    }
}

impl Controller for AimdController {
    fn name(&self) -> &'static str {
        "aimd"
    }
    fn current(&self) -> usize {
        self.inner.current()
    }
    fn on_success(&mut self, now: Instant, bytes: u64, latency: Duration) -> usize {
        self.inner.on_success(now, bytes, latency)
    }
    fn on_error(&mut self, now: Instant, _latency: Duration, _slowdown: bool) -> usize {
        self.inner.on_error(now)
    }
}

/// PID experiment: a `pid`-crate loop regulating concurrency against a
/// *latency-inflation ratio* (current windowed mean latency / the best
/// latency observed at low concurrency), à la TCP Vegas / Netflix's
/// gradient concurrency limiters. Ratio 1.0 means "no queueing"; the
/// setpoint is a small tolerated inflation (e.g. 1.3x) — comfortably
/// past baseline RTT, short of a saturated pipe. This is
/// bandwidth-saturation aware (unlike a pure error-rate PID, which
/// would happily climb forever on a backend that never errors but also
/// never gets faster).
///
/// A failed request is folded into the same signal as an extreme
/// latency sample (`error_latency_penalty` multiples of the setpoint
/// latency) rather than a separate channel, so one control loop reacts
/// to both "too slow" and "outright rejected" — and, symmetrically,
/// also applies an immediate multiplicative-decrease step on error so
/// its worst-case reaction time to a hard failure is comparable to the
/// AIMD controller's, rather than waiting out a full window.
pub struct PidController {
    pid: pid::Pid<f64>,
    current: f64,
    min: f64,
    max: f64,
    baseline_latency: Duration,
    window_start: Instant,
    window_latency_sum: Duration,
    window_count: u64,
    min_window_samples: u64,
    window_interval: Duration,
    error_latency_penalty: f64,
}

impl PidController {
    /// `target_ratio`: tolerated latency inflation over baseline before
    /// the controller starts backing off (Vegas-style knob; 1.3 means
    /// "queueing up to 30% over the best-seen latency is fine").
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        initial: usize,
        min: usize,
        max: usize,
        target_ratio: f64,
        kp: f64,
        ki: f64,
        kd: f64,
        step_limit: f64,
    ) -> Self {
        let mut pid = pid::Pid::new(target_ratio, step_limit);
        pid.p(kp, step_limit).i(ki, step_limit).d(kd, step_limit);
        Self {
            pid,
            current: initial.max(min).min(max) as f64,
            min: min.max(1) as f64,
            max: max.max(1) as f64,
            baseline_latency: Duration::MAX,
            window_start: Instant::now(),
            window_latency_sum: Duration::ZERO,
            window_count: 0,
            min_window_samples: 3,
            window_interval: Duration::from_secs(2),
            error_latency_penalty: 6.0,
        }
    }

    fn note_latency(&mut self, latency: Duration) {
        // A slow first few probes at concurrency 1 would otherwise
        // become a permanently pessimistic baseline; only trust the
        // baseline once we have seen a handful of samples, and let it
        // keep improving (transport warms up, DNS/TLS gets cached).
        if latency < self.baseline_latency {
            self.baseline_latency = latency;
        }
    }

    fn maybe_step(&mut self, now: Instant) -> usize {
        let elapsed = now.saturating_duration_since(self.window_start);
        if elapsed < self.window_interval || self.window_count < self.min_window_samples {
            return self.current.round() as usize;
        }
        let mean_latency = self.window_latency_sum.as_secs_f64() / self.window_count as f64;
        let baseline = self.baseline_latency.as_secs_f64().max(0.001);
        let ratio = mean_latency / baseline;
        let output = self.pid.next_control_output(ratio);
        self.current = (self.current + output.output).clamp(self.min, self.max);
        self.window_start = now;
        self.window_latency_sum = Duration::ZERO;
        self.window_count = 0;
        self.current.round() as usize
    }
}

impl Controller for PidController {
    fn name(&self) -> &'static str {
        "pid"
    }

    fn current(&self) -> usize {
        self.current.round() as usize
    }

    fn on_success(&mut self, now: Instant, _bytes: u64, latency: Duration) -> usize {
        self.note_latency(latency);
        self.window_latency_sum += latency;
        self.window_count += 1;
        self.maybe_step(now)
    }

    fn on_error(&mut self, now: Instant, latency: Duration, _slowdown: bool) -> usize {
        self.note_latency(latency);
        let penalty = self.baseline_latency.mul_f64(self.error_latency_penalty);
        self.window_latency_sum += penalty.max(latency);
        self.window_count += 1;
        // Symmetric with AIMD's immediate halving: do not wait out the
        // window for a hard failure.
        self.current = (self.current / 2.0).max(self.min);
        self.window_start = now;
        self.window_latency_sum = Duration::ZERO;
        self.window_count = 0;
        self.current.round() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advance(n: &mut Instant, d: Duration) -> Instant {
        *n += d;
        *n
    }

    #[test]
    fn fixed_controller_never_moves() {
        let mut c = FixedController::new(5);
        let now = Instant::now();
        assert_eq!(c.on_success(now, 1024, Duration::from_millis(10)), 5);
        assert_eq!(c.on_error(now, Duration::from_millis(10), true), 5);
        assert_eq!(c.current(), 5);
    }

    #[test]
    fn pid_backs_off_immediately_on_error() {
        let mut c = PidController::new(16, 1, 64, 1.3, 4.0, 0.5, 0.1, 8.0);
        let mut now = Instant::now();
        // Warm up the baseline latency.
        c.on_success(now, 4 << 20, Duration::from_millis(100));
        now = advance(&mut now, Duration::from_millis(1));
        let before = c.current();
        let after = c.on_error(now, Duration::from_millis(100), true);
        assert!(
            after < before,
            "an error must push concurrency down immediately: {before} -> {after}"
        );
    }

    #[test]
    fn pid_grows_when_latency_stays_near_baseline() {
        let mut c = PidController::new(4, 1, 64, 1.3, 4.0, 0.5, 0.1, 8.0);
        let mut now = Instant::now();
        let mut last = c.current();
        for _ in 0..40 {
            now = advance(&mut now, Duration::from_millis(700));
            last = c.on_success(now, 4 << 20, Duration::from_millis(100));
        }
        assert!(
            last > 4,
            "steady low latency should let the PID climb, got {last}"
        );
    }

    #[test]
    fn pid_holds_back_when_latency_inflates_with_load() {
        let mut c = PidController::new(4, 1, 64, 1.3, 4.0, 0.5, 0.1, 8.0);
        let mut now = Instant::now();
        // Baseline established at low concurrency.
        for _ in 0..6 {
            now = advance(&mut now, Duration::from_millis(700));
            c.on_success(now, 4 << 20, Duration::from_millis(100));
        }
        let after_baseline = c.current();
        // Latency now scales with the (rising) concurrency level, as it
        // would under real queueing: the PID should stop climbing.
        let mut last = after_baseline;
        for _ in 0..40 {
            now = advance(&mut now, Duration::from_millis(700));
            let inflated =
                Duration::from_millis(100 * last.max(1) as u64 / after_baseline.max(1) as u64);
            last = c.on_success(now, 4 << 20, inflated);
        }
        assert!(
            last <= after_baseline + 6,
            "latency scaling with concurrency should cap growth: {after_baseline} -> {last}"
        );
    }
}
