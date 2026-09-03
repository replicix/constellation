//! Deterministic synthetic upload target for reproducible controller
//! comparisons, independent of real S3/network variance.
//!
//! Models:
//! - a fixed propagation RTT (EU home connection -> us-west-2),
//! - a shared upstream pipe of finite bandwidth (the "1 Gbps home
//!   link", or equally a per-account S3 throughput ceiling) divided
//!   across in-flight requests,
//! - a server-side comfortable concurrency (`capacity`) beyond which
//!   queueing delay and 503 SlowDown probability both start climbing,
//!   and
//! - an optional fault window during which SlowDown probability is
//!   forced high, to exercise push-back-then-recover deterministically.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct SimConfig {
    pub base_rtt: Duration,
    /// Aggregate bytes/sec available across all in-flight requests.
    pub bandwidth_bytes_per_sec: u64,
    /// In-flight requests beyond which the server starts throttling.
    pub capacity: usize,
    pub seed: u64,
    /// [start, start+duration) measured from the run's t=0: SlowDown
    /// probability is forced to `fault_error_rate` in this window.
    pub fault_start: Option<Duration>,
    pub fault_duration: Duration,
    pub fault_error_rate: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            base_rtt: Duration::from_millis(150),
            bandwidth_bytes_per_sec: 1_000_000_000 / 8, // ~1 Gbps
            capacity: 24,
            seed: 42,
            fault_start: None,
            fault_duration: Duration::from_secs(15),
            fault_error_rate: 0.0,
        }
    }
}

pub struct SimTarget {
    cfg: SimConfig,
    active: AtomicUsize,
    rng: Mutex<StdRng>,
    run_start: Instant,
}

#[derive(Debug)]
pub enum SimError {
    SlowDown,
}

impl SimTarget {
    pub fn new(cfg: SimConfig) -> Arc<Self> {
        Arc::new(Self {
            rng: Mutex::new(StdRng::seed_from_u64(cfg.seed)),
            cfg,
            active: AtomicUsize::new(0),
            run_start: Instant::now(),
        })
    }

    /// Upload one object of `size` bytes. Returns the simulated latency
    /// and, on success, nothing further; on failure, [`SimError`].
    pub async fn put(&self, size: u64) -> Result<Duration, SimError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let started = Instant::now();
        let result = self.simulate(size, active).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let elapsed = started.elapsed();
        result.map(|_| elapsed)
    }

    async fn simulate(&self, size: u64, active: usize) -> Result<(), SimError> {
        let over_capacity = active.saturating_sub(self.cfg.capacity);

        // Overload probability climbs smoothly past capacity: 0 right
        // at capacity, ~63% once double capacity is in flight.
        let overload_error_rate = if over_capacity == 0 {
            0.0
        } else {
            1.0 - (-(over_capacity as f64) / self.cfg.capacity.max(1) as f64).exp()
        };

        let in_fault_window = self.cfg.fault_start.is_some_and(|start| {
            let t = self.run_start.elapsed();
            t >= start && t < start + self.cfg.fault_duration
        });
        let error_rate = if in_fault_window {
            overload_error_rate.max(self.cfg.fault_error_rate)
        } else {
            overload_error_rate
        };

        let roll: f64 = self.rng.lock().await.random();
        if roll < error_rate {
            // A real SlowDown response is cheap for S3 to produce; model
            // it as a short, mostly-RTT-bound rejection rather than a
            // full-latency failed transfer.
            tokio::time::sleep(self.cfg.base_rtt).await;
            return Err(SimError::SlowDown);
        }

        // Shared-pipe bandwidth is split evenly across in-flight
        // requests; extra queueing delay accrues once past capacity.
        let share = self.cfg.bandwidth_bytes_per_sec / active.max(1) as u64;
        let service = Duration::from_secs_f64(size as f64 / share.max(1) as f64);
        let queueing = self.cfg.base_rtt.mul_f64(over_capacity as f64 * 0.15);
        tokio::time::sleep(self.cfg.base_rtt + service + queueing).await;
        Ok(())
    }
}
