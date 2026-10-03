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
