//! Requester-side ordering gate (plan 29 M5).
//!
//! `node_runtime.rs`'s `SyncRequest::Forward` arm used to await each
//! forward's network round trip inline, which meant a single sync task
//! serialized every forwarded mutation for this node (plan 29 M4's
//! measured bottleneck). Spawning each forward onto its own task fixes
//! the throughput problem, but a requester can never have two forwards
//! whose conflict-key sets overlap in flight unordered: two creates in
//! the same directory both bump the parent's mtime/ctime/nlink; two
//! `SetManifest`s on one file race each other's base; a rename touches
//! two parents. If both reach the holder and get applied back here
//! (`forward::apply_accepted` → `Meta::install_shadow`) in a different
//! relative order than the holder actually executed them, the
//! requester's replica can diverge from the holder's: replay applies
//! records assuming they arrive in the holder's causal order, and the
//! speculation log's rollback (plan 30 §M3a) assumes shadows were
//! captured in that order too.
//!
//! `KeyGate` is a coarse, all-or-nothing mutex over conflict keys
//! (`forward::conflict_keys`'s output, ordinarily inode numbers):
//! `acquire` waits until every requested key is free, then holds all of
//! them until the returned [`KeyGuard`] drops. Two disjoint key sets
//! never wait on each other, so unrelated directories/files still run
//! fully concurrently; two overlapping sets resolve in the order they
//! called `acquire` (see [`GateInner::progress`]). There is no
//! per-key/nested locking (and so no lock-ordering deadlock risk): every
//! acquire is a single all-or-nothing check under one mutex.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use constellation_fs_core::Ino;

struct Waiter {
    id: u64,
    keys: Vec<Ino>,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct GateInner {
    busy: HashSet<Ino>,
    /// FIFO by arrival (`acquire` call order).
    queue: VecDeque<Waiter>,
    next_id: u64,
}

impl GateInner {
    /// Grant every currently-queued waiter (oldest first) whose keys do
    /// not conflict with `busy`, extending `busy` with each grant before
    /// checking the next. A waiter blocked on a busy key never
    /// head-of-line-blocks a later, disjoint waiter behind it in the
    /// queue -- but two waiters that *do* share a key are always
    /// resolved in the order they arrived, because the earlier one (by
    /// definition scanned first) claims the key before the later one is
    /// even considered. That is the FIFO-per-key property this module's
    /// correctness argument depends on.
    fn progress(&mut self) {
        let mut i = 0;
        while i < self.queue.len() {
            let conflict = self.queue[i].keys.iter().any(|k| self.busy.contains(k));
            if conflict {
                i += 1;
                continue;
            }
            let w = self.queue.remove(i).expect("index in bounds");
            self.busy.extend(w.keys.iter().copied());
            w.notify.notify_one();
            // The element that was at `i` is gone; the next one shifted
            // down into `i`, so do not advance.
        }
    }
}

/// The gate. Cheap to construct; kept as an `Arc` (see
/// `ForwardState::gate`) so a [`KeyGuard`] can release on its own without
/// borrowing anything else.
#[derive(Default)]
pub struct KeyGate {
    inner: Mutex<GateInner>,
}

impl KeyGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Wait until every key in `keys` is free, then hold all of them.
    /// An empty key set is granted immediately (nothing to serialize
    /// against). Cancellation-safe: dropping the returned future before
    /// it resolves (or dropping the `KeyGuard` it produces, at any
    /// point) always releases whatever this call reserved and re-runs
    /// [`GateInner::progress`], so a concurrent waiter on the same keys
    /// is never stranded.
    pub async fn acquire(self: &Arc<Self>, mut keys: Vec<Ino>) -> KeyGuard {
        keys.sort_unstable();
        keys.dedup();
        let notify = Arc::new(Notify::new());
        let (id, granted) = {
            let mut inner = self.inner.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;
            inner.queue.push_back(Waiter {
                id,
                keys: keys.clone(),
                notify: notify.clone(),
            });
            inner.progress();
            let still_queued = inner.queue.iter().any(|w| w.id == id);
            (id, !still_queued)
        };
        // `guard` is constructed *before* the await below, so if this
        // whole `acquire` future is dropped while suspended waiting on
        // `notify`, `guard`'s `Drop` still runs (it is a live local in
        // this frame) and cleans up the queue entry.
        let mut guard = KeyGuard {
            gate: self.clone(),
            id,
            keys,
            granted,
        };
        if !granted {
            notify.notified().await;
            guard.granted = true;
        }
        guard
    }
}

/// RAII hold on a [`KeyGate`]'s key set.
pub struct KeyGuard {
    gate: Arc<KeyGate>,
    id: u64,
    keys: Vec<Ino>,
    granted: bool,
}

impl Drop for KeyGuard {
    fn drop(&mut self) {
        let mut inner = self.gate.inner.lock().unwrap();
        if self.granted {
            for k in &self.keys {
                inner.busy.remove(k);
            }
        } else {
            // Cancelled before ever being granted: just drop the queue
            // entry, so `progress` does not keep treating these keys as
            // spoken for on this waiter's behalf.
            inner.queue.retain(|w| w.id != self.id);
        }
        inner.progress();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn disjoint_keys_run_concurrently() {
        let gate = KeyGate::new();
        let g1 = gate.acquire(vec![1]).await;
        // A disjoint acquire must not block behind an unrelated held key,
        // even though the gate already has an outstanding guard.
        let g2 = tokio::time::timeout(Duration::from_millis(200), gate.acquire(vec![2]))
            .await
            .expect("disjoint acquire must not block");
        drop(g1);
        drop(g2);
    }

    #[tokio::test]
    async fn overlapping_keys_serialize() {
        let gate = KeyGate::new();
        let g1 = gate.acquire(vec![7]).await;
        let gate2 = gate.clone();
        let waiting = tokio::spawn(async move { gate2.acquire(vec![7]).await });
        // Give the spawned task a chance to enqueue and block.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "overlapping acquire must block");
        drop(g1);
        let g2 = tokio::time::timeout(Duration::from_millis(200), waiting)
            .await
            .expect("must be granted once the holder releases")
            .unwrap();
        drop(g2);
    }

    #[tokio::test]
    async fn overlapping_waiters_are_fifo() {
        let gate = KeyGate::new();
        let g0 = gate.acquire(vec![1]).await;
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for n in 0..5u32 {
            let gate = gate.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let _g = gate.acquire(vec![1]).await;
                order.lock().unwrap().push(n);
                // Hold briefly so a would-be out-of-order grant would be
                // observable rather than racing past unnoticed.
                tokio::time::sleep(Duration::from_millis(5)).await;
            }));
            // Ensure each waiter enqueues before the next is spawned, so
            // arrival order is deterministic.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(g0);
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn dropped_waiter_releases_and_does_not_strand_others() {
        let gate = KeyGate::new();
        let g1 = gate.acquire(vec![9]).await;
        let gate2 = gate.clone();
        // A waiter that gets cancelled (e.g. its owning task aborted or
        // timed out) before being granted must not leave a ghost queue
        // entry that blocks anyone else who wants key 9.
        let cancelled = tokio::time::timeout(Duration::from_millis(20), gate2.acquire(vec![9]));
        assert!(cancelled.await.is_err(), "expected the timeout to fire");
        drop(g1);
        // A fresh acquire on the same key must succeed promptly; if the
        // cancelled waiter were still queued, `progress` would keep
        // key 9 assigned to it forever and this would hang.
        let g2 = tokio::time::timeout(Duration::from_millis(200), gate.acquire(vec![9]))
            .await
            .expect("cancelled waiter must not strand key 9");
        drop(g2);
    }

    #[tokio::test]
    async fn dropped_holder_releases_correctly() {
        let gate = KeyGate::new();
        let g1 = gate.acquire(vec![3, 4]).await;
        drop(g1);
        // Both keys must be free again, not just one of them.
        let g2 = tokio::time::timeout(Duration::from_millis(200), gate.acquire(vec![3]))
            .await
            .expect("key 3 must be free");
        let g3 = tokio::time::timeout(Duration::from_millis(200), gate.acquire(vec![4]))
            .await
            .expect("key 4 must be free");
        drop(g2);
        drop(g3);
    }

    /// Two tasks each acquire a two-key set in opposite orders repeatedly.
    /// A naive per-key-lock-in-request-order implementation could
    /// deadlock here; `KeyGate` cannot, because every acquire is
    /// all-or-nothing under one mutex.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_deadlock_with_opposite_key_orders() {
        let gate = KeyGate::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..50 {
            let g = gate.clone();
            let c = counter.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = g.acquire(vec![1, 2]).await;
                c.fetch_add(1, Ordering::Relaxed);
            }));
            let g = gate.clone();
            let c = counter.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = g.acquire(vec![2, 1]).await;
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }
        let all = tokio::time::timeout(Duration::from_secs(5), async {
            for t in tasks {
                t.await.unwrap();
            }
        })
        .await;
        assert!(all.is_ok(), "deadlocked");
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn empty_key_set_never_blocks() {
        let gate = KeyGate::new();
        let g1 = gate.acquire(vec![]).await;
        let g2 = tokio::time::timeout(Duration::from_millis(200), gate.acquire(vec![]))
            .await
            .expect("empty key sets never conflict");
        drop(g1);
        drop(g2);
    }
}
