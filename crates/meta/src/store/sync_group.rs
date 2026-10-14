//! Concurrent [`Meta::sync`](crate::Meta::sync) calls share their journal
//! syncs (finding 14).
//!
//! Every `fsync(2)` on a mount, and every write whose safety rests on
//! being remembered (M16), syncs the metadata journal. Upstream fjall
//! syncs the journal file once per call, and on btrfs an `fsync` holds the
//! file's inode lock for as long as the disk takes, which every commit's
//! journal write (made with the store's single-writer lock held) then
//! waits for. With eight `fsync`s in flight a commit waited for all eight
//! in turn, and with it every writer of the store, the authority core's
//! step included (`slow commit: … persist=4361ms`, xcheckfs's
//! `fio --fsync=16` on a loaded host).
//!
//! Here at most one journal sync (a *round*) runs at a time. A round
//! admits callers until its flush: the round's sync calls `close` with the
//! journal writer's lock held, right before it flushes the journal
//! (`Database::persist_with`, vendored fjall change 8). A caller is
//! admitted only while the round has not closed, under this group's
//! mutex; its commits returned before it called, and a commit releases the
//! journal writer's lock before it returns, so they are in the flush (and
//! the sync) of the round it joined. A caller that comes after the close
//! waits for the next round. A call waits for at most the round in flight
//! and its own; the journal sees one sync per round however many callers
//! it serves.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

#[derive(Default)]
struct Rounds {
    /// A round is running.
    running: bool,
    /// The running round has not flushed yet: a caller may join it.
    admitting: bool,
    /// The last round started, and the last one finished.
    started: u64,
    finished: u64,
    /// The rounds whose sync failed (or panicked), as runs of
    /// consecutive rounds: after a failed sync the database is poisoned
    /// (`Database::persist`) and every later round fails too.
    failed: Vec<std::ops::RangeInclusive<u64>>,
    /// Calls that have chosen the round they need (tests wait on it).
    #[cfg(test)]
    entered: u64,
}

#[derive(Default)]
pub(crate) struct SyncGroup {
    rounds: Mutex<Rounds>,
    done: Condvar,
}

impl SyncGroup {
    fn lock(&self) -> MutexGuard<'_, Rounds> {
        self.rounds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stop admitting callers to the running round (its flush is next).
    fn close(&self) {
        self.lock().admitting = false;
    }

    /// Run `sync` as a round, or join one another caller runs. `sync` must
    /// call the `close` it is handed with the journal writer's lock held,
    /// right before its flush (`persist_with`); a round whose sync never
    /// calls it (it failed first) admits until it ends, which is safe: it
    /// failed, and so does every caller it served. A caller that joined a
    /// round that failed gets `failed()`; the one that ran it, its own
    /// error (logged once, with the round).
    pub(crate) fn run<E: std::fmt::Display>(
        &self,
        sync: impl FnOnce(&dyn Fn()) -> Result<(), E>,
        failed: impl FnOnce() -> E,
    ) -> Result<(), E> {
        let mut r = self.lock();
        // A running round that has not flushed yet covers this call's
        // commits; one that has (or no round at all) does not, and the
        // next round does.
        let need = if r.running && r.admitting {
            r.started
        } else {
            r.started + 1
        };
        #[cfg(test)]
        {
            r.entered += 1;
        }
        loop {
            if r.finished >= need {
                return if r.failed.iter().any(|f| f.contains(&need)) {
                    Err(failed())
                } else {
                    Ok(())
                };
            }
            if r.running {
                r = self.done.wait(r).unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            // Nobody runs `need` (no round is in flight, so the last one
            // started has finished: `started == need - 1`): this caller
            // does.
            r.running = true;
            r.admitting = true;
            r.started += 1;
            let round = r.started;
            drop(r);
            // A panic in `sync` must not leave the round running for ever.
            struct Finish<'a>(&'a SyncGroup, u64, bool);
            impl Drop for Finish<'_> {
                fn drop(&mut self) {
                    let mut r = self.0.lock();
                    r.running = false;
                    r.admitting = false;
                    r.finished = self.1;
                    if !self.2 {
                        match r.failed.last_mut() {
                            Some(last) if *last.end() + 1 == self.1 => {
                                *last = *last.start()..=self.1;
                            }
                            _ => r.failed.push(self.1..=self.1),
                        }
                    }
                    self.0.done.notify_all();
                }
            }
            let mut finish = Finish(self, round, false);
            let result = sync(&|| self.close());
            finish.2 = result.is_ok();
            drop(finish);
            if let Err(e) = &result {
                tracing::error!(
                    round,
                    error = %e,
                    "metadata journal sync failed; the calls it served fail"
                );
            }
            return result;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Wait until `n` calls have chosen the round they need.
    fn wait_entered(group: &SyncGroup, n: u64) {
        let started = Instant::now();
        while group.lock().entered < n {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "calls never entered"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Sixteen callers at once share syncs, and each is served by a round
    /// whose flush (its `close`) came after the call.
    #[test]
    fn concurrent_calls_share_syncs_that_flush_after_them() {
        let group = Arc::new(SyncGroup::default());
        let syncs = Arc::new(AtomicU64::new(0));
        let flushes = Arc::new(Mutex::new(Vec::<Instant>::new()));
        let callers: Vec<_> = (0..16)
            .map(|i| {
                let (group, syncs, flushes) = (group.clone(), syncs.clone(), flushes.clone());
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(i % 4));
                    let called = Instant::now();
                    group
                        .run(
                            |close| {
                                syncs.fetch_add(1, Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(5));
                                close();
                                flushes.lock().unwrap().push(Instant::now());
                                std::thread::sleep(Duration::from_millis(50));
                                Ok::<(), &str>(())
                            },
                            || "poisoned",
                        )
                        .unwrap();
                    // The round that served this call flushed after it, so
                    // some flush did (the latest is at least that late).
                    let latest = *flushes.lock().unwrap().iter().max().unwrap();
                    assert!(latest >= called, "no flush after the call");
                })
            })
            .collect();
        for c in callers {
            c.join().unwrap();
        }
        let n = syncs.load(Ordering::SeqCst);
        assert!(n < 16, "{n} syncs for 16 concurrent calls: none shared");
    }

    /// A call made while a round waits for the journal writer's lock (a
    /// commit ahead of it blocked in its write) joins that round; one made
    /// after the round flushed waits for the next.
    #[test]
    fn a_call_joins_a_round_until_its_flush() {
        let group = Arc::new(SyncGroup::default());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (flush_tx, flush_rx) = std::sync::mpsc::channel::<()>();
        let (flushed_tx, flushed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first = {
            let group = group.clone();
            std::thread::spawn(move || {
                group.run(
                    |close| {
                        entered_tx.send(()).unwrap();
                        flush_rx.recv().unwrap();
                        close();
                        flushed_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok::<(), &str>(())
                    },
                    || "poisoned",
                )
            })
        };
        entered_rx.recv().unwrap();
        let ran = Arc::new(AtomicU64::new(0));
        let call = |group: Arc<SyncGroup>, ran: Arc<AtomicU64>| {
            std::thread::spawn(move || {
                group.run(
                    |close| {
                        ran.fetch_add(1, Ordering::SeqCst);
                        close();
                        Ok::<(), &str>(())
                    },
                    || "poisoned",
                )
            })
        };
        // Before the flush: joins the first round.
        let joined = call(group.clone(), ran.clone());
        wait_entered(&group, 2);
        flush_tx.send(()).unwrap();
        flushed_rx.recv().unwrap();
        // After the flush: the next round.
        let next = call(group.clone(), ran.clone());
        wait_entered(&group, 3);
        release_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        joined.join().unwrap().unwrap();
        next.join().unwrap().unwrap();
        assert_eq!(
            ran.load(Ordering::SeqCst),
            1,
            "only the late call ran a round"
        );
    }

    /// A failed sync fails every call it served: the one that ran it with
    /// its error, the ones that joined it as `failed()`.
    #[test]
    fn a_failed_sync_fails_the_calls_it_served() {
        let group = Arc::new(SyncGroup::default());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first = {
            let group = group.clone();
            std::thread::spawn(move || {
                group.run(
                    |close| {
                        close();
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok::<(), &str>(())
                    },
                    || "poisoned",
                )
            })
        };
        entered_rx.recv().unwrap();
        // Two calls after the first round flushed, while it is held: one
        // runs the next round, which fails before its flush, the other
        // joins it.
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let group = group.clone();
                std::thread::spawn(move || group.run(|_| Err("io"), || "poisoned"))
            })
            .collect();
        wait_entered(&group, 3);
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Ok(()));
        let mut results: Vec<_> = waiters.into_iter().map(|w| w.join().unwrap()).collect();
        results.sort();
        assert_eq!(results, vec![Err("io"), Err("poisoned")]);
    }

    /// A sync that panics fails the calls that joined it, and the next
    /// call runs a round of its own.
    #[test]
    fn a_panicking_sync_does_not_wedge_the_group() {
        let group = Arc::new(SyncGroup::default());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let first = {
            let group = group.clone();
            std::thread::spawn(move || {
                group.run(
                    |close| {
                        close();
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok::<(), &str>(())
                    },
                    || "poisoned",
                )
            })
        };
        entered_rx.recv().unwrap();
        // Two calls while the first round is held: one runs the next
        // round, which panics before its flush, the other joins it.
        let calls: Vec<_> = (0..2)
            .map(|_| {
                let group = group.clone();
                std::thread::spawn(move || {
                    group.run(
                        |_| -> Result<(), &str> { panic!("sync panicked") },
                        || "poisoned",
                    )
                })
            })
            .collect();
        wait_entered(&group, 3);
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Ok(()));
        let outcomes: Vec<_> = calls.into_iter().map(|c| c.join()).collect();
        assert_eq!(
            outcomes.iter().filter(|o| o.is_err()).count(),
            1,
            "one panicked"
        );
        assert!(outcomes.iter().any(|o| matches!(o, Ok(Err("poisoned")))));
        assert_eq!(group.run(|_| Ok::<(), &str>(()), || "poisoned"), Ok(()));
    }
}
