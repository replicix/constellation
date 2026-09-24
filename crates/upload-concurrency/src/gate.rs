//! Elastic semaphore whose target can change at any time.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A concurrency gate is a semaphore where the permit count (`target`)
/// can be adjusted live. An `acquire()` call blocks until fewer than
/// `target` permits are checked out. Shrinking the target never
/// preempts in-flight work — it only makes the next `acquire()` wait a
/// little longer, which is what "stop starting new uploads until the
/// queue drains a bit" should mean.
pub struct ConcurrencyGate {
    in_flight: AtomicUsize,
    target: AtomicUsize,
    notify: tokio::sync::Notify,
}

impl ConcurrencyGate {
    pub fn new(initial: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            target: AtomicUsize::new(initial.max(1)),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub fn target(&self) -> usize {
        self.target.load(Ordering::Relaxed)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn set_target(&self, n: usize) {
        self.target.store(n.max(1), Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    pub async fn acquire(&self) -> ConcurrencyPermit<'_> {
        loop {
            // Register interest before checking, so a `set_target`/release
            // that lands between the check and the wait is not missed.
            let notified = self.notify.notified();
            let target = self.target().max(1);
            let cur = self.in_flight.load(Ordering::Acquire);
            if cur < target {
                if self
                    .in_flight
                    .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return ConcurrencyPermit { gate: self };
                }
                continue;
            }
            notified.await;
        }
    }

    /// A permit for latency-sensitive work (plan 30 §M7: a small
    /// `fsync`/write-through inode drain) that must not queue behind a
    /// bulk backlog. It is granted while fewer than `target + reserve`
    /// permits are out, so it only ever competes with other priority
    /// work: ordinary waiters stop at `target`, and `notify_waiters` wakes
    /// every waiter at once, so an ordinary `acquire` behind a backlog of
    /// hundreds of queued uploads would win a slot about as often as each
    /// of them does. The overshoot is bounded by `reserve`.
    pub async fn acquire_priority(&self, reserve: usize) -> ConcurrencyPermit<'_> {
        loop {
            let notified = self.notify.notified();
            let limit = self.target().max(1) + reserve;
            let cur = self.in_flight.load(Ordering::Acquire);
            if cur < limit {
                if self
                    .in_flight
                    .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return ConcurrencyPermit { gate: self };
                }
                continue;
            }
            notified.await;
        }
    }

    /// Acquire a permit that can be moved into a spawned task.
    pub async fn acquire_owned(self: &Arc<Self>) -> OwnedConcurrencyPermit {
        loop {
            let notified = self.notify.notified();
            let target = self.target().max(1);
            let cur = self.in_flight.load(Ordering::Acquire);
            if cur < target {
                if self
                    .in_flight
                    .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return OwnedConcurrencyPermit { gate: self.clone() };
                }
                continue;
            }
            notified.await;
        }
    }
}

pub struct ConcurrencyPermit<'a> {
    gate: &'a ConcurrencyGate,
}

impl Drop for ConcurrencyPermit<'_> {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.notify.notify_waiters();
    }
}

pub struct OwnedConcurrencyPermit {
    gate: Arc<ConcurrencyGate>,
}

impl Drop for OwnedConcurrencyPermit {
    fn drop(&mut self) {
        self.gate.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.gate.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn acquire_blocks_at_target_and_wakes_on_release() {
        let gate = Arc::new(ConcurrencyGate::new(2));
        let p1 = gate.acquire().await;
        let p2 = gate.acquire().await;
        assert_eq!(gate.in_flight(), 2);

        let gate2 = gate.clone();
        let waiter = tokio::spawn(async move {
            let _p3 = gate2.acquire().await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "third acquire must block at target 2"
        );

        drop(p1);
        waiter.await.unwrap();
        drop(p2);
    }

    /// Plan 30 §M7: with every ordinary slot taken (and ordinary waiters
    /// queued), a priority acquire still gets one of the reserve slots at
    /// once; ordinary waiters never use the reserve.
    #[tokio::test]
    async fn a_priority_acquire_skips_the_ordinary_queue() {
        let gate = Arc::new(ConcurrencyGate::new(2));
        let _a = gate.acquire().await;
        let _b = gate.acquire().await;
        let gate2 = gate.clone();
        let ordinary = tokio::spawn(async move {
            let _c = gate2.acquire().await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let prio = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            gate.acquire_priority(1),
        )
        .await
        .expect("a priority acquire waited behind the ordinary queue");
        assert_eq!(gate.in_flight(), 3);
        assert!(
            !ordinary.is_finished(),
            "ordinary work must not use the reserve"
        );
        drop(prio);
        ordinary.abort();
    }

    #[tokio::test]
    async fn set_target_up_wakes_a_waiter_immediately() {
        let gate = Arc::new(ConcurrencyGate::new(1));
        let _p1 = gate.acquire().await;
        let gate2 = gate.clone();
        let waiter = tokio::spawn(async move {
            let _p2 = gate2.acquire().await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        gate.set_target(2);
        waiter.await.unwrap();
    }

    #[tokio::test]
    async fn owned_permit_can_move_to_a_task() {
        let gate = Arc::new(ConcurrencyGate::new(1));
        let permit = gate.acquire_owned().await;
        let task = tokio::spawn(async move {
            drop(permit);
        });
        task.await.unwrap();
        assert_eq!(gate.in_flight(), 0);
    }
}
