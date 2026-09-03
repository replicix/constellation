//! Elastic semaphore whose target can change at any time.

use std::sync::atomic::{AtomicUsize, Ordering};

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
}
