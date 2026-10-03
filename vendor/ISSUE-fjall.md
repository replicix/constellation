# `Database` drop can block forever: shutdown loop does a blocking `send(Close)` into a bounded channel

## Version

- fjall 3.1.10 and 3.1.11 from crates.io (the `Drop` code is identical; 3.1.10 is upstream commit `3adaa50261c9be58484971dc561cdd563156765e`)
- Linux 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.98.1 (48a229cea 2026-09-01)

## Problem

`impl Drop for DatabaseInner` (`src/db.rs:70-80`) waits for the worker threads with

```rust
while self.active_thread_counter.load(Relaxed) > 0 {
    let _ = self.worker_pool.sender.send(WorkerMessage::Close); // db.rs:76, blocking
    std::thread::sleep(Duration::from_micros(10));
}
```

The channel is `flume::bounded(1_000)` (`src/worker_pool.rs:52`) and the pool keeps its own `rx`, so it never disconnects. While workers are busy with a flush or compaction they do not receive anything, so one `Close` is added per turn and the channel fills up after about 1000 turns; the dropping thread then parks inside `send`. Each worker that finishes takes one `Close`, and flume moves the parked `Close` into the freed slot, so the channel stays full. The counter is decremented only when the worker's thread function returns (`ActiveThreadGuard`, `worker_pool.rs:95`), after the `log::debug!("Worker #{i} closes because DB is dropping")` line. If the dropping thread wakes and re-checks the counter after the last worker took its `Close` but before it decremented, it sees `1` and calls `send` on a full channel that nobody reads any more: it blocks forever (all `fjall:worker` threads are gone; confirmed in gdb).

## Reproducer

`Cargo.toml`:

```toml
[dependencies]
fjall = "=3.1.11"
log = "0.4"
```

`src/main.rs` (run with a plain `cargo run`, i.e. a debug build; in release the flushes finish too quickly for the queue to fill):

```rust
use fjall::{Database, KeyspaceCreateOptions};
use std::{sync::mpsc, time::Duration};

// Models a worker thread that is descheduled (or logs to a slow sink) right
// after it received `Close`, but before it decrements `active_thread_counter`.
struct SlowLog;
impl log::Log for SlowLog {
    fn enabled(&self, m: &log::Metadata) -> bool {
        m.level() == log::Level::Debug
    }
    fn log(&self, r: &log::Record) {
        if r.args().to_string().contains("closes because DB is dropping") {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn flush(&self) {}
}

fn main() {
    if std::env::var("WIDEN").as_deref() != Ok("0") {
        log::set_logger(&SlowLog).unwrap();
        log::set_max_level(log::LevelFilter::Debug);
    }
    for iter in 0..10 {
        let dir = std::env::temp_dir().join(format!("fjall-drop-hang-{}-{iter}", std::process::id()));
        let db = Database::builder(&dir).worker_threads(4).open().unwrap();
        let keyspaces: Vec<_> = (0..4)
            .map(|k| {
                db.keyspace(&format!("ks{k}"), || {
                    KeyspaceCreateOptions::default().max_memtable_size(16 * 1024 * 1024)
                })
                .unwrap()
            })
            .collect();
        let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ iter;
        let mut value = vec![0u8; 1024];
        for i in 0..48_000usize {
            for b in value.chunks_mut(8) {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                b.copy_from_slice(&x.to_le_bytes()[..b.len()]);
            }
            keyspaces[i % 4].insert(x.to_be_bytes(), &value).unwrap();
        }
        // Seal the memtables so every worker has a flush/compaction in flight.
        for ks in &keyspaces {
            ks.rotate_memtable().unwrap();
        }
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            drop(keyspaces);
            drop(db); // <- hangs here
            tx.send(()).unwrap();
        });
        if rx.recv_timeout(Duration::from_secs(10)).is_err() {
            println!("BUG REPRODUCED: dropping Database did not return within 10 s (iteration {iter})");
            std::process::exit(1);
        }
        let _ = std::fs::remove_dir_all(&dir);
        println!("iteration {iter}: drop ok");
    }
}
```

The window between the last worker taking `Close` and decrementing the counter is a few instructions wide, so the logger above widens it the way a descheduled thread or slow log sink would (it sleeps 2 ms on exactly that line). `WIDEN=0` runs without it, and then the hang is rare (it did not occur in 20 iterations here); it needs the last worker to be preempted in the window, which we do see on a heavily oversubscribed machine.

## Expected vs actual

- Expected: `drop(db)` returns after the in-flight flushes/compactions finish; all 10 iterations print `drop ok`.
- Actual: the drop never returns (3 of 3 runs hang on iteration 0, also on 3.1.10).

```
$ cargo run
BUG REPRODUCED: dropping Database did not return within 10 s (iteration 0)
```

With the fix below (via `[patch.crates-io]`) the same program passes reliably (3 runs of 20 iterations, about 14 s each):

```
iteration 19: drop ok
```

## Suggested fix

Never block in the shutdown loop, and drain the queue each turn so it cannot fill:

```diff
-        while self.active_thread_counter.load(std::sync::atomic::Ordering::Relaxed) > 0 {
-            let _ = self.worker_pool.sender.send(WorkerMessage::Close);
-            std::thread::sleep(std::time::Duration::from_micros(10));
-        }
+        loop {
+            let live = self.active_thread_counter.load(std::sync::atomic::Ordering::Relaxed);
+            if live == 0 {
+                break;
+            }
+            let _ = self.worker_pool.rx.drain().count();
+            for _ in 0..live {
+                let _ = self.worker_pool.sender.try_send(WorkerMessage::Close);
+            }
+            std::thread::sleep(std::time::Duration::from_micros(10));
+        }
```

This still waits for every in-flight job to finish and discards only queued messages, like the drains right before and after the loop. A worker already waiting in `recv` gets the `Close` handed over directly by flume, so the drain cannot starve it.

## Notes

- More likely with many keyspaces, small memtables, slow disks, debug builds or a CPU-oversubscribed host: anything that keeps workers busy for more than ~1000 loop turns (tens of ms) and makes the last worker likely to be preempted.
- Worker-side blocking sends into the same channel (worker 0 re-queueing `Compact`, `inner_rotate_memtable` sending `Flush`) could join the deadlock when the channel is full; the per-turn drain releases them too.

---

# `SingleWriterTxDatabase::write_tx` can starve a writer for as long as others keep writing: the single-writer lock is an unfair `std::sync::Mutex`

## Version

- fjall 3.1.11 from crates.io (`src/tx/single_writer/mod.rs` is the same in 3.1.10)
- Linux 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28), 32 vCPUs

## Problem

Every write transaction of a `SingleWriterTxDatabase` holds `single_writer_lock: Arc<Mutex<()>>` (`src/tx/single_writer/mod.rs:29`, taken in `write_tx`, line 61) until it commits or rolls back. `std::sync::Mutex` makes no fairness promise, and on Linux it is not fair in practice: a thread that unlocks and immediately locks again takes the mutex back before a woken waiter gets to run. With a few threads writing back to back, a thread that writes occasionally can wait for seconds — in the run below, for the whole 20 s the others kept writing — although each holder keeps the lock for 5 ms.

We hit this in a metadata store where one thread's writes are latency-critical (a sequencer that must answer its peers within a second) and a dozen worker threads write back to back under load. The store's own test with eight busy writers measured the sequencer's wait for the lock at a median of 0.85 s and a worst case of 12.5 s, for a lock each holder kept for 5 ms.

## Reproducer

`Cargo.toml`:

```toml
[dependencies]
fjall = "=3.1.11"
```

`src/main.rs` (`cargo run --release`, `FJREPRO_DIR` a directory on a local disk):

```rust
use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn percentile(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() - 1) as f64 * p) as usize]
}

/// Eight threads run write transactions back to back, each holding the
/// single-writer lock ~5 ms, for 20 s. A ninth thread asks for a short
/// transaction every 10 ms meanwhile and measures how long `write_tx()`
/// takes to return.
fn lock(db: SingleWriterTxDatabase) {
    let ks = db.keyspace("ks", KeyspaceCreateOptions::default).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let busy: Vec<_> = (0..8u64)
        .map(|t| {
            let (db, ks, stop) = (db.clone(), ks.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut tx = db.write_tx();
                    tx.insert(&ks, (t << 32 | i).to_be_bytes(), b"v");
                    std::thread::sleep(Duration::from_millis(5));
                    tx.commit().unwrap();
                    i += 1;
                }
                i
            })
        })
        .collect();
    {
        let stop = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Ordering::Relaxed);
        });
    }
    std::thread::sleep(Duration::from_millis(100));
    let mut waits = Vec::new();
    let mut i = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        let mut tx = db.write_tx();
        waits.push(started.elapsed());
        tx.insert(&ks, (u64::MAX - i).to_be_bytes(), b"v");
        tx.commit().unwrap();
        i += 1;
        std::thread::sleep(Duration::from_millis(10));
    }
    let others: u64 = busy.into_iter().map(|t| t.join().unwrap()).sum();
    let n = waits.len();
    let p50 = percentile(&mut waits, 0.5);
    let max = *waits.last().unwrap();
    println!(
        "lock: in 20 s the busy writers ran {others} transactions (~5 ms each), the measured \
         thread {n}: its write_tx() wait p50 {p50:?} max {max:?}"
    );
}

fn main() {
    let path = std::path::Path::new(&std::env::var("FJREPRO_DIR").unwrap())
        .join(format!("fjrepro-lock-{}", std::process::id()));
    lock(SingleWriterTxDatabase::builder(&path).open().unwrap());
    let _ = std::fs::remove_dir_all(&path);
}
```

## Expected vs actual

- Expected: the measured thread waits for at most the holders queued ahead of it (8 × 5 ms), so about 40 ms, and runs about 1500 transactions in 20 s.
- Actual (4 runs, host load 10–40):

```
$ cargo run --release
lock: in 20 s the busy writers ran 3945 transactions (~5 ms each), the measured thread 1: its write_tx() wait p50 19.909548916s max 19.909548916s
lock: in 20 s the busy writers ran 3955 transactions (~5 ms each), the measured thread 2: its write_tx() wait p50 6.439546617s max 13.454152012s
lock: in 20 s the busy writers ran 3948 transactions (~5 ms each), the measured thread 9: its write_tx() wait p50 118.351428ms max 8.19839703s
lock: in 20 s the busy writers ran 3455 transactions (~5 ms each), the measured thread 7: its write_tx() wait p50 484.966003ms max 10.145629469s
```

With the fix below (via `[patch.crates-io]`):

```
lock: in 20 s the busy writers ran 3940 transactions (~5 ms each), the measured thread 435: its write_tx() wait p50 35.664633ms max 43.294906ms
lock: in 20 s the busy writers ran 3835 transactions (~5 ms each), the measured thread 423: its write_tx() wait p50 35.753034ms max 359.538824ms
```

(Two other runs with the fix had one multi-second outlier each, 7.8 s and 22 s, during which the busy writers stalled too — 2189 and 990 transactions instead of ~3900: a whole-database stall from a slow disk, not the lock; see the version-history lock below.)

## Suggested fix

Hand the lock over in arrival order. A ticket lock is enough (`src/tx/single_writer/fair_lock.rs`, new):

```rust
use std::sync::{Condvar, Mutex, MutexGuard};

/// A FIFO lock: write transactions get it in the order they asked for it.
#[derive(Default)]
pub struct FairLock {
    /// (next ticket to hand out, ticket being served)
    state: Mutex<(u64, u64)>,
    turn: Condvar,
}

/// Holds the lock until dropped.
pub struct FairGuard<'a>(&'a FairLock);

impl FairLock {
    fn state(&self) -> MutexGuard<'_, (u64, u64)> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn lock(&self) -> FairGuard<'_> {
        let mut s = self.state();
        let ticket = s.0;
        s.0 += 1;
        while s.1 != ticket {
            s = self.turn.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        FairGuard(self)
    }
}

impl Drop for FairGuard<'_> {
    fn drop(&mut self) {
        self.0.state().1 += 1;
        self.0.turn.notify_all();
    }
}
```

```diff
--- a/src/tx/single_writer/mod.rs
+++ b/src/tx/single_writer/mod.rs
@@ -2,16 +2,15 @@
 // This source code is licensed under both the Apache 2.0 and MIT License
 // (found in the LICENSE-* files in the repository)
 
+mod fair_lock;
 mod keyspace;
 mod write_tx;
 
 use crate::{
     keyspace::KeyspaceKey, Config, Database, KeyspaceCreateOptions, PersistMode, Snapshot,
 };
-use std::{
-    path::Path,
-    sync::{Arc, Mutex},
-};
+use fair_lock::FairLock;
+use std::{path::Path, sync::Arc};
 
 pub use keyspace::SingleWriterTxKeyspace;
 pub use write_tx::WriteTransaction;
@@ -26,7 +25,7 @@
 #[derive(Clone)]
 pub struct TxDatabase {
     pub(crate) inner: Database,
-    single_writer_lock: Arc<Mutex<()>>,
+    single_writer_lock: Arc<FairLock>,
 }
 
 impl Openable for TxDatabase {
@@ -57,8 +56,7 @@
     #[must_use]
     #[expect(clippy::missing_panics_doc)]
     pub fn write_tx(&self) -> WriteTransaction<'_> {
-        #[expect(clippy::expect_used)]
-        let guard = self.single_writer_lock.lock().expect("poisoned tx lock");
+        let guard = self.single_writer_lock.lock();
 
         let mut write_tx = WriteTransaction::new(
             self.clone(),
--- a/src/tx/single_writer/write_tx.rs
+++ b/src/tx/single_writer/write_tx.rs
@@ -8,7 +8,8 @@
     Guard, Iter, Keyspace, PersistMode, Readable, SingleWriterTxDatabase,
 };
 use lsm_tree::{UserKey, UserValue};
-use std::{ops::RangeBounds, sync::MutexGuard};
+use super::fair_lock::FairGuard;
+use std::ops::RangeBounds;
 
 /// A single-writer (serialized) cross-keyspace transaction
 ///
@@ -21,7 +22,7 @@
 /// are not held somewhere forever.
 #[clippy::has_significant_drop]
 pub struct WriteTransaction<'a> {
-    _guard: MutexGuard<'a, ()>,
+    _guard: FairGuard<'a>,
     inner: BaseTransaction,
 }
 
@@ -79,7 +80,7 @@
     pub(crate) fn new(
         db: SingleWriterTxDatabase,
         nonce: SnapshotNonce,
-        guard: MutexGuard<'tx, ()>,
+        guard: FairGuard<'tx>,
     ) -> Self {
         Self {
             _guard: guard,
```

A fair lock costs throughput when many threads contend (each handover wakes the next in line instead of letting the running thread continue); a database that does not want that could make it an option. Constellation (the downstream project) needed more than fairness and vendors a variant with a priority class: a thread that sets `set_write_priority(true)` goes ahead of every ordinary writer, and an ordinary writer that has waited 50 ms gets the next turn after a priority holder (`vendor/fjall/src/tx/single_writer/writer_lock.rs` in that repository).

---

# `Journal::persist` holds the journal writer's lock across the fsync: one `persist(SyncAll)` stalls every commit for as long as the disk takes

## Version

- fjall 3.1.11 from crates.io (`src/journal/mod.rs` is the same in 3.1.10)
- Linux 7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64, rustc 1.99.0 (b940084d7 2026-09-28); the database on ext4 (a loop device), see Notes for btrfs

## Problem

`Journal::persist` (`src/journal/mod.rs:122-125`) takes the journal writer's mutex and calls `Writer::persist`, which flushes the buffer and then `sync_all`s / `sync_data`s the file with the mutex still held. Every commit (`WriteBatch::commit`, and so every `Keyspace::insert` and write transaction) takes the same mutex to append to the journal. So while one thread waits for an fsync — `Database::persist(PersistMode::SyncAll)`, which an application calls for the writes it must not lose — every other writer of the database waits too: hundreds of milliseconds to seconds on a busy disk. Only the flush needs the lock: a sync through a second descriptor of the same open file covers everything written before it.

## Reproducer

`Cargo.toml` as above; `src/main.rs`:

```rust
use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn percentile(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() - 1) as f64 * p) as usize]
}

/// A plain `Database` (no single-writer lock: each insert commits on
/// its own, under the journal writer's lock). One thread writes 16 MiB
/// into the journal and calls `persist(SyncAll)`, over and over; another
/// inserts one small row every 5 ms and measures each insert.
fn fsync(db: Database) {
    let big = db.keyspace("big", KeyspaceCreateOptions::default).unwrap();
    let small = db.keyspace("small", KeyspaceCreateOptions::default).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let syncer = {
        let (db, big, stop) = (db.clone(), big.clone(), stop.clone());
        std::thread::spawn(move || {
            let value = vec![7u8; 64 * 1024];
            let mut syncs = Vec::new();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..256 {
                    big.insert(i.to_be_bytes(), &value).unwrap();
                    i += 1;
                }
                let started = Instant::now();
                db.persist(PersistMode::SyncAll).unwrap();
                syncs.push(started.elapsed());
            }
            syncs
        })
    };
    let mut commits = Vec::new();
    let started = Instant::now();
    let mut i = 0u64;
    while started.elapsed() < Duration::from_secs(20) {
        let t = Instant::now();
        small.insert(i.to_be_bytes(), b"v").unwrap();
        commits.push(t.elapsed());
        i += 1;
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let mut syncs = syncer.join().unwrap();
    let sync_p50 = percentile(&mut syncs, 0.5);
    let sync_max = *syncs.last().unwrap();
    let n = commits.len();
    let p50 = percentile(&mut commits, 0.5);
    let p99 = percentile(&mut commits, 0.99);
    let max = *commits.last().unwrap();
    let over_10ms = commits.iter().filter(|d| **d > Duration::from_millis(10)).count();
    println!(
        "fsync: {} persist(SyncAll) calls, p50 {sync_p50:?} max {sync_max:?}; {n} small \
         inserts meanwhile: p50 {p50:?} p99 {p99:?} max {max:?}, {over_10ms} over 10 ms",
        syncs.len(),
    );
}

fn main() {
    let path = std::path::Path::new(&std::env::var("FJREPRO_DIR").unwrap())
        .join(format!("fjrepro-fsync-{}", std::process::id()));
    fsync(Database::builder(&path).open().unwrap());
    let _ = std::fs::remove_dir_all(&path);
}
```

## Expected vs actual

- Expected: the small inserts (one journal append each, no sync) take microseconds whatever the other thread's fsyncs take; about 3500 of them run in 20 s.
- Actual (two runs; the host's disk is shared, so the fsync times vary):

```
$ cargo run --release
fsync: 11 persist(SyncAll) calls, p50 63.473053ms max 21.6178426s; 13 small inserts meanwhile: p50 45.910126ms p99 5.328379907s max 21.614324191s, 11 over 10 ms
fsync: 63 persist(SyncAll) calls, p50 85.60125ms max 692.053307ms; 708 small inserts meanwhile: p50 1.17µs p99 457.938351ms max 2.039754827s, 87 over 10 ms
```

With the fix below:

```
fsync: 44 persist(SyncAll) calls, p50 160.477329ms max 1.171099406s; 2920 small inserts meanwhile: p50 1.27µs p99 27.59µs max 753.228702ms, 21 over 10 ms
fsync: 58 persist(SyncAll) calls, p50 74.629125ms max 3.150283865s; 2339 small inserts meanwhile: p50 3.58µs p99 53.652693ms max 14.40366593s, 31 over 10 ms
```

The fix takes the fsync off every insert's path (p99 from 0.46–5.3 s to 28–54 µs, 4–200× as many inserts in the same time). The remaining outliers are other stalls that block inserts while the disk is slow (the memtable write halt, and the version-history lock below), not the journal lock.

## Suggested fix

Flush under the lock, sync after releasing it:

```diff
--- a/src/journal/mod.rs
+++ b/src/journal/mod.rs
@@ -120,8 +120,22 @@
 
     /// Persists the journal.
     pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
-        let mut journal_writer = self.get_writer()?;
-        journal_writer.persist(mode).map_err(Into::into)
+        // Flush under the writer's lock, sync after releasing it: holding
+        // the lock across the fsync stalled every commit for as long as
+        // the disk took.
+        let Some((file, path)) = self.get_writer()?.flush_for_sync(mode)? else {
+            return Ok(());
+        };
+        match mode {
+            PersistMode::SyncAll => file.sync_all().inspect_err(|e| {
+                log::error!("Failed to fsync journal file at {}: {e:?}", path.display());
+            }),
+            PersistMode::SyncData => file.sync_data().inspect_err(|e| {
+                log::error!("Failed to fsyncdata journal file at {}: {e:?}", path.display());
+            }),
+            PersistMode::Buffer => Ok(()),
+        }
+        .map_err(Into::into)
     }
 
     pub fn recover<P: AsRef<Path>>(
--- a/src/journal/writer.rs
+++ b/src/journal/writer.rs
@@ -199,6 +199,36 @@
         })
     }
 
+    /// Flushes the IO buffer; for a syncing `mode`, returns a duplicate of
+    /// the file's descriptor (and its path) to sync without the writer's
+    /// lock. A sync through it covers everything written before the flush.
+    pub(crate) fn flush_for_sync(
+        &mut self,
+        mode: PersistMode,
+    ) -> std::io::Result<Option<(std::fs::File, PathBuf)>> {
+        log::trace!(
+            "Persisting journal at {} with mode={mode:?}",
+            self.path.display(),
+        );
+        if self.is_buffer_dirty {
+            self.file.flush().inspect_err(|e| {
+                log::error!(
+                    "Failed to flush journal IO buffers at {}: {e:?}",
+                    self.path.display(),
+                );
+            })?;
+            self.is_buffer_dirty = false;
+        }
+        match mode {
+            PersistMode::Buffer => Ok(None),
+            PersistMode::SyncAll | PersistMode::SyncData => self
+                .file
+                .get_ref()
+                .try_clone()
+                .map(|file| Some((file, self.path.clone()))),
+        }
+    }
+
     /// Persists the journal file.
     pub(crate) fn persist(&mut self, mode: PersistMode) -> std::io::Result<()> {
         log::trace!(
```

Durability is unchanged: the buffer is flushed under the lock, and the duplicated descriptor refers to the same open file, so the sync covers everything any commit appended before the call. Writes appended meanwhile may or may not be covered, as with any concurrent fsync; nothing records "synced through X", so nothing claims they are. A journal rotation still syncs the old file under the lock. Errors still reach the caller (and `Database::persist` still poisons the database on one).

## Notes

- On btrfs the benefit is smaller: btrfs takes the inode lock in `fsync`, so a commit whose journal append reaches `write(2)` (the buffer's flush) still waits for an fsync of the same file in progress. The reproducer above therefore runs on ext4.
- Related, in lsm-tree 3.1.10 (fjall's storage): every memtable insert first takes its tree's version-history lock (`Tree::append_entry`, `version_history.read()`), and a flush (`register_tables`) or a compaction holds that lock for writing while `persist_version` writes and fsyncs the new version file, fsyncs the directory and rewrites `current` (more fsyncs). Under I/O pressure a commit of a few rows then waits seconds for the disk with the single-writer lock held; Constellation measured up to 11.2 s inside one commit's memtable inserts (its vendored fjall logs a commit's phases when it takes ≥ 500 ms). Persisting the version before taking the write lock, and only swapping it in under the lock, would keep fsyncs off every writer's path.
