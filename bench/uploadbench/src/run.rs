//! Drives one [`Controller`] against one upload target for a fixed
//! duration, recording enough to compare controllers afterwards:
//! throughput, latency percentiles, concurrency trajectory, and error
//! behavior (in particular, how quickly concurrency drops when errors
//! start and how quickly it recovers once they stop).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use constellation_upload_concurrency::ConcurrencyGate;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::controller::Controller;
use crate::live::{looks_like_slowdown, S3Target};
use crate::sim::SimTarget;

pub enum Target {
    Sim(Arc<SimTarget>),
    Live(Arc<S3Target>),
}

pub enum UploadOutcome {
    Success {
        latency: Duration,
        bytes: u64,
    },
    /// `slowdown` marks a throttling-flavored failure (S3 503 SlowDown
    /// or its simulated equivalent) as opposed to some other error.
    Error {
        latency: Duration,
        slowdown: bool,
    },
}

impl Target {
    async fn upload(&self, seq: u64, body: &Bytes) -> UploadOutcome {
        let size = body.len() as u64;
        match self {
            Target::Sim(sim) => {
                let started = Instant::now();
                match sim.put(size).await {
                    Ok(latency) => UploadOutcome::Success {
                        latency,
                        bytes: size,
                    },
                    Err(_slowdown) => UploadOutcome::Error {
                        latency: started.elapsed(),
                        slowdown: true,
                    },
                }
            }
            Target::Live(s3) => {
                let started = Instant::now();
                match s3.put(seq, body.clone()).await {
                    Ok(latency) => UploadOutcome::Success {
                        latency,
                        bytes: size,
                    },
                    Err(error) => UploadOutcome::Error {
                        latency: started.elapsed(),
                        slowdown: looks_like_slowdown(&error),
                    },
                }
            }
        }
    }
}

/// One periodic time-series sample.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub t: Duration,
    pub concurrency_target: usize,
    pub in_flight: usize,
    pub cumulative_bytes: u64,
    pub cumulative_successes: u64,
    pub cumulative_errors: u64,
}

pub struct RunResult {
    pub controller_name: &'static str,
    pub samples: Vec<Sample>,
    pub latencies_ms: Vec<f64>,
    pub total_bytes: u64,
    pub total_successes: u64,
    pub total_errors: u64,
    pub duration: Duration,
}

pub struct RunConfig {
    pub duration: Duration,
    pub object_size: u64,
    pub sample_interval: Duration,
}

/// Run `controller` against `target` for `cfg.duration`, continuously
/// keeping `controller.current()` uploads in flight (re-checked on
/// every gate acquire, so the controller's live adjustments take
/// effect immediately rather than only at task-spawn boundaries).
pub async fn run(
    controller_name: &'static str,
    controller: impl Controller + 'static,
    target: Target,
    cfg: &RunConfig,
) -> RunResult {
    debug_assert_eq!(
        controller.name(),
        controller_name,
        "controller_name argument must match the controller's own name()"
    );
    let gate = Arc::new(ConcurrencyGate::new(controller.current()));
    let controller = Arc::new(AsyncMutex::new(controller));
    let target = Arc::new(target);

    let cumulative_bytes = Arc::new(AtomicU64::new(0));
    let cumulative_successes = Arc::new(AtomicU64::new(0));
    let cumulative_errors = Arc::new(AtomicU64::new(0));
    let latencies_ms: Arc<std::sync::Mutex<Vec<f64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let body = Bytes::from(vec![0xABu8; cfg.object_size as usize]);
    let start = Instant::now();
    let seq = Arc::new(AtomicU64::new(0));

    let mut samples = Vec::new();
    let mut in_flight_tasks = FuturesUnordered::new();
    let mut next_sample_at = Duration::ZERO;

    loop {
        let elapsed = start.elapsed();
        if elapsed >= cfg.duration && in_flight_tasks.is_empty() {
            break;
        }

        if elapsed >= next_sample_at {
            samples.push(Sample {
                t: elapsed,
                concurrency_target: gate.target(),
                in_flight: gate.in_flight(),
                cumulative_bytes: cumulative_bytes.load(Ordering::Relaxed),
                cumulative_successes: cumulative_successes.load(Ordering::Relaxed),
                cumulative_errors: cumulative_errors.load(Ordering::Relaxed),
            });
            next_sample_at += cfg.sample_interval;
        }

        if elapsed < cfg.duration && in_flight_tasks.len() < gate.target() {
            let gate = gate.clone();
            let target = target.clone();
            let controller = controller.clone();
            let body = body.clone();
            let seq = seq.fetch_add(1, Ordering::Relaxed);
            let cumulative_bytes = cumulative_bytes.clone();
            let cumulative_successes = cumulative_successes.clone();
            let cumulative_errors = cumulative_errors.clone();
            let latencies_ms = latencies_ms.clone();
            in_flight_tasks.push(tokio::spawn(async move {
                let permit = gate.acquire().await;
                let outcome = target.upload(seq, &body).await;
                drop(permit);
                let now = Instant::now();
                match outcome {
                    UploadOutcome::Success { latency, bytes } => {
                        cumulative_bytes.fetch_add(bytes, Ordering::Relaxed);
                        cumulative_successes.fetch_add(1, Ordering::Relaxed);
                        latencies_ms
                            .lock()
                            .unwrap()
                            .push(latency.as_secs_f64() * 1000.0);
                        let new_target = controller.lock().await.on_success(now, bytes, latency);
                        gate.set_target(new_target);
                    }
                    UploadOutcome::Error { latency, slowdown } => {
                        cumulative_errors.fetch_add(1, Ordering::Relaxed);
                        let new_target = controller.lock().await.on_error(now, latency, slowdown);
                        gate.set_target(new_target);
                    }
                }
            }));
            continue;
        }

        if let Some(done) = in_flight_tasks.next().await {
            if let Err(e) = done {
                tracing::error!(error = %e, "upload task panicked");
            }
        } else {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // Final sample so the trajectory's tail is captured exactly.
    samples.push(Sample {
        t: start.elapsed(),
        concurrency_target: gate.target(),
        in_flight: gate.in_flight(),
        cumulative_bytes: cumulative_bytes.load(Ordering::Relaxed),
        cumulative_successes: cumulative_successes.load(Ordering::Relaxed),
        cumulative_errors: cumulative_errors.load(Ordering::Relaxed),
    });

    let latencies_ms = std::mem::take(&mut *latencies_ms.lock().unwrap());
    RunResult {
        controller_name,
        samples,
        latencies_ms,
        total_bytes: cumulative_bytes.load(Ordering::Relaxed),
        total_successes: cumulative_successes.load(Ordering::Relaxed),
        total_errors: cumulative_errors.load(Ordering::Relaxed),
        duration: start.elapsed(),
    }
}
