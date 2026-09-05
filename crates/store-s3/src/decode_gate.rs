//! Priority-aware serialization for whole-object E2E decrypt.
//!
//! Whole-object AEAD briefly materializes the decoded plaintext of an
//! entire chunk in RAM (`store.rs`'s `get_chunk_to_writer_e2e`). Left
//! unbounded, N simultaneous GET completions would multiply peak RSS by
//! N. [`DecodeGate`] bounds that to `capacity` concurrent decrypts —
//! exactly like a semaphore — but a [`Priority::Demand`] acquire (the
//! read the application is blocked on right now) always cuts ahead of
//! queued [`Priority::Background`] acquires (prefetch, scan-ahead,
//! cooperative-cache warm), so a bulk prefetch backlog cannot make a
//! foreground read wait behind an arbitrarily deep queue of decrypts
//! nobody is blocked on yet.
//!
//! This does not raise the bound on concurrent decrypts — that would
//! defeat the RSS cap the gate exists to enforce. It only reorders who
//! gets the next permit.
//!
//! Progress guarantee: a background acquire is skipped only while
//! `demand_waiting > 0`, re-checked on every permit release, so it
//! proceeds the instant demand goes idle. A time-boxed override
//! (`BACKGROUND_STARVE_TIMEOUT`) additionally lets background compete
//! on equal footing after waiting that long, so a continuous stream of
//! demand reads cannot starve background forever.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How urgently the caller needs its decrypt to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// The read the application is blocked on right now.
    Demand,
    /// Speculative work with no one waiting on it yet (prefetch,
    /// scan-ahead, cooperative-cache warm).
    Background,
}

/// Background yields to waiting demand for at most this long before it
/// competes on equal footing, so demand cannot starve background
/// indefinitely under a continuous foreground read stream.
const BACKGROUND_STARVE_TIMEOUT: Duration = Duration::from_millis(250);

pub struct DecodeGate {
    capacity: usize,
    in_flight: AtomicUsize,
    demand_waiting: AtomicUsize,
    notify: tokio::sync::Notify,
}

impl DecodeGate {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            in_flight: AtomicUsize::new(0),
            demand_waiting: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub async fn acquire(self: &Arc<Self>, priority: Priority) -> DecodeGatePermit {
        if priority == Priority::Demand {
            self.demand_waiting.fetch_add(1, Ordering::AcqRel);
        }
        let started = Instant::now();
        loop {
            // Register interest before checking, so a release that lands
            // between the check and the wait is not missed.
            let notified = self.notify.notified();
            let deferring_to_demand = priority == Priority::Background
                && self.demand_waiting.load(Ordering::Acquire) > 0
                && started.elapsed() < BACKGROUND_STARVE_TIMEOUT;
            if !deferring_to_demand {
                let cur = self.in_flight.load(Ordering::Acquire);
                if cur < self.capacity
                    && self
                        .in_flight
                        .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    if priority == Priority::Demand {
                        self.demand_waiting.fetch_sub(1, Ordering::AcqRel);
                        // Background may have been deferring on our
                        // presence; wake it to re-check now we've stopped
                        // waiting (we may still hold the only permit, but
                        // that is a capacity fact it re-checks too).
                        self.notify.notify_waiters();
                    }
                    return DecodeGatePermit { gate: self.clone() };
                }
            }
            notified.await;
        }
    }
}

pub struct DecodeGatePermit {
    gate: Arc<DecodeGate>,
}

impl Drop for DecodeGatePermit {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn demand_cuts_ahead_of_a_queued_background_acquire() {
        let gate = Arc::new(DecodeGate::new(1));
        let held = gate.acquire(Priority::Background).await;

        let bg_gate = gate.clone();
        let bg_acquired = Arc::new(AtomicUsize::new(0));
        let bg_acquired2 = bg_acquired.clone();
        let bg = tokio::spawn(async move {
            let _permit = bg_gate.acquire(Priority::Background).await;
            bg_acquired2.store(1, Ordering::SeqCst);
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let demand_gate = gate.clone();
        let demand_acquired = Arc::new(AtomicUsize::new(0));
        let demand_acquired2 = demand_acquired.clone();
        let demand = tokio::spawn(async move {
            let _permit = demand_gate.acquire(Priority::Demand).await;
            demand_acquired2.store(1, Ordering::SeqCst);
            // Hold the permit for an observable window so the assertions
            // below can catch background still waiting behind it.
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(held);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            demand_acquired.load(Ordering::SeqCst),
            1,
            "demand must acquire before the earlier-queued background waiter"
        );
        assert_eq!(
            bg_acquired.load(Ordering::SeqCst),
            0,
            "background must still be waiting behind demand"
        );

        demand.await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            bg_acquired.load(Ordering::SeqCst),
            1,
            "background proceeds once demand releases and no one else is waiting"
        );
        bg.await.unwrap();
    }

    #[tokio::test]
    async fn background_alone_makes_progress_without_any_demand() {
        let gate = Arc::new(DecodeGate::new(1));
        let permit = gate.acquire(Priority::Background).await;
        drop(permit);
        let _permit = gate.acquire(Priority::Background).await;
    }

    #[tokio::test(start_paused = true)]
    async fn background_is_not_starved_forever_by_continuous_demand() {
        let gate = Arc::new(DecodeGate::new(1));

        let bg_gate = gate.clone();
        let bg_done = Arc::new(AtomicUsize::new(0));
        let bg_done2 = bg_done.clone();
        let bg = tokio::spawn(async move {
            let _permit = bg_gate.acquire(Priority::Background).await;
            bg_done2.store(1, Ordering::SeqCst);
        });
        tokio::time::sleep(Duration::from_millis(5)).await;

        // A steady drip of short-lived demand acquires, each releasing
        // before the next arrives, for well beyond the starve timeout.
        let deadline = Instant::now() + BACKGROUND_STARVE_TIMEOUT * 3;
        while Instant::now() < deadline && bg_done.load(Ordering::SeqCst) == 0 {
            let permit = gate.acquire(Priority::Demand).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
            drop(permit);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        tokio::time::timeout(Duration::from_millis(50), bg)
            .await
            .expect("background must eventually run despite continuous demand")
            .unwrap();
        assert_eq!(bg_done.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_demand_acquires_all_eventually_succeed() {
        let gate = Arc::new(DecodeGate::new(1));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let g = gate.clone();
            tasks.push(tokio::spawn(async move {
                let permit = g.acquire(Priority::Demand).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(permit);
            }));
        }
        for t in tasks {
            tokio::time::timeout(Duration::from_secs(5), t)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
