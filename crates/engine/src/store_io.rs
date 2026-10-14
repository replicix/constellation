//! Synchronous metadata-store writes from async code (finding 14).
//!
//! The metadata store's writes are synchronous: each takes the store's
//! single writer lock and appends to its journal, and on a slow or
//! overloaded disk that is seconds (a journal `write(2)` waits for device
//! I/O on btrfs and ext4). A tokio task that writes the store blocks the
//! runtime worker running it; enough of them (an upload pass acking its
//! chunks, the completion pruner, the publisher clearing its dirty marks,
//! all waiting on one stalled commit) leave the runtime no worker, and
//! every other task of the daemon — P2P, the registry poll, timers, the
//! uploads' own S3 answers — stops with them.
//!
//! [`store_call`] runs such a write where it stands, in the same task (so
//! one task's writes and the reads after them stay in order; the store's
//! single writer lock orders them with everyone else's), but tells the
//! runtime first (`tokio::task::block_in_place`): the worker's other tasks
//! move to a new worker while this one waits. At most [`STORE_CALLS`] do so
//! at once; more wait for a turn without holding a worker. On a
//! current-thread runtime (tests, short-lived tools) the call just runs.
//!
//! Two constraints of `block_in_place`:
//! - It panics in a task polled by a `tokio::task::LocalSet` (`run_until`,
//!   `spawn_local`), on a multi-thread runtime too, and the runtime-flavor
//!   check here cannot see that. The daemon's runtimes (`cli`'s `main.rs`,
//!   `serve.rs`, `handover.rs`) therefore run no `LocalSet`, and
//!   `tests::nothing_uses_a_local_set` fails the build's tests if any
//!   workspace source starts to.
//! - The worker's core moves to a thread of the runtime's blocking pool.
//!   With that pool saturated (`max_blocking_threads` threads all busy)
//!   the handoff queues behind them, and the worker's other tasks wait
//!   meanwhile as they did before; [`STORE_CALLS`] keeps store writes from
//!   being the ones that saturate it.
//!
//! The synchronous writers stay synchronous by design: the FUSE threads,
//! the `fsync` waiters and the authority core's step write the store on
//! threads of their own (the core's step is one task, and a handoff per
//! step would cost more than the worker it frees). What goes through
//! [`store_call`] is a store write made by an ordinary async task.

use std::sync::OnceLock;
use tokio::sync::Semaphore;

/// How many tasks may wait in a store write at once
/// ([`store_call`]): each holds a thread of the runtime's blocking pool.
pub(crate) const STORE_CALLS: usize = 8;

fn permits() -> &'static Semaphore {
    static PERMITS: OnceLock<Semaphore> = OnceLock::new();
    PERMITS.get_or_init(|| Semaphore::new(STORE_CALLS))
}

/// Run `f`, a synchronous metadata-store write, from async code without
/// holding a runtime worker while it waits (see the module docs).
pub(crate) async fn store_call<T>(f: impl FnOnce() -> T) -> T {
    let _turn = permits().acquire().await.expect("never closed");
    in_place(f)
}

fn in_place<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Four tasks of a two-worker runtime wait in store writes that do not
    /// return (a commit blocked in its journal write); `run` makes each
    /// call. Returns how long a timer task took for ten 10 ms ticks
    /// meanwhile, or `None` when it made none in 2 s.
    fn ticks_while_writes_block(through_store_call: bool) -> Option<Duration> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let entered = Arc::new(AtomicUsize::new(0));
        let blocked = |rx: Arc<Mutex<std::sync::mpsc::Receiver<()>>>, entered: Arc<AtomicUsize>| {
            move || {
                entered.fetch_add(1, Ordering::SeqCst);
                let _ = rx.lock().unwrap().recv();
            }
        };
        let writers: Vec<_> = (0..4)
            .map(|_| {
                let f = blocked(release_rx.clone(), entered.clone());
                rt.spawn(async move {
                    if through_store_call {
                        store_call(f).await
                    } else {
                        f()
                    }
                })
            })
            .collect();
        // Both workers are taken by now when the writes hold them.
        let started = Instant::now();
        while entered.load(Ordering::SeqCst) < 1 && started.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(50));
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        rt.spawn(async move {
            let started = Instant::now();
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = done_tx.send(started.elapsed());
        });
        let took = done_rx.recv_timeout(Duration::from_secs(2)).ok();
        // Release every write (the ones still waiting for a turn too).
        for _ in 0..4 {
            let _ = release_tx.send(());
        }
        for w in writers {
            rt.block_on(w).unwrap();
        }
        rt.shutdown_timeout(Duration::from_secs(1));
        took
    }

    /// Store writes that block leave the runtime its workers: a timer
    /// keeps ticking on time. Made directly on the workers, they starve it.
    #[test]
    fn blocked_store_writes_do_not_starve_the_runtime() {
        assert_eq!(
            ticks_while_writes_block(false),
            None,
            "the direct calls were expected to hold both workers"
        );
        let took = ticks_while_writes_block(true).expect("the timer never ran");
        assert!(
            took < Duration::from_millis(1_000),
            "ten 10 ms ticks took {took:?}"
        );
    }

    /// `block_in_place` panics under a `tokio::task::LocalSet`, whatever
    /// the runtime (see the module docs): no workspace source may run one.
    #[test]
    fn nothing_uses_a_local_set() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        // Spelled apart so this file does not match itself.
        let banned = [
            ["task::", "LocalSet"].concat(),
            ["spawn_", "local("].concat(),
            ["Local", "Runtime"].concat(),
        ];
        let mut found = Vec::new();
        let mut dirs = vec![root.join("crates"), root.join("bench"), root.join("tools")];
        while let Some(dir) = dirs.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().is_some_and(|n| n != "target") {
                        dirs.push(path);
                    }
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap_or_default();
                    // Code only: the docs that state the rule name it.
                    let code = text.lines().filter(|l| !l.trim_start().starts_with("//"));
                    for line in code {
                        for b in &banned {
                            if line.contains(b.as_str()) {
                                found.push(format!("{}: {}", path.display(), line.trim()));
                            }
                        }
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "store_call's block_in_place panics under a LocalSet: {found:?}"
        );
    }

    /// One task's store calls run in its own order, on a current-thread
    /// runtime too.
    #[test]
    fn a_tasks_store_calls_keep_their_order() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let order = rt.block_on(async {
            let mut order = Vec::new();
            for i in 0..5 {
                order.push(store_call(|| i).await);
            }
            order
        });
        assert_eq!(order, vec![0, 1, 2, 3, 4]);
    }
}
