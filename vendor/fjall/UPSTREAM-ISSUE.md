# Draft upstream issue for fjall-rs/fjall (not filed)

**Title:** `Database` drop can hang forever: blocking `send(Close)` into a full worker channel after the last worker has exited

---

### Version

fjall 3.1.10 (commit `3adaa50`), flume 0.12.0, Linux x86_64.

### What happens

Dropping the last `Database` handle while flushes or compactions are still
running sometimes never returns. The dropping thread is parked here:

```
flume::Sender<WorkerMessage>::send
fjall::db::<impl Drop for DatabaseInner>::drop   (src/db.rs:76)
```

At that point every `fjall:worker` thread has already exited. Inspecting the
hung process in gdb shows:

- `active_thread_counter == 0`;
- the worker channel's queue holds 1000 `Close` messages;
- there is one pending sender (the dropping thread) and no waiting receiver.

### Mechanism

```rust
self.stop_signal.send();
let _ = self.worker_pool.rx.drain().count();
while self.active_thread_counter.load(Relaxed) > 0 {
    let _ = self.worker_pool.sender.send(WorkerMessage::Close); // blocking
    std::thread::sleep(Duration::from_micros(10));
}
```

The worker channel is `flume::bounded(1_000)`, and `WorkerPool` keeps its own
`rx`, so the channel never disconnects.

1. While workers are busy with a flush or compaction, one `Close` lands per
   loop turn. After about 1000 turns (tens of milliseconds) the channel is
   full and the dropping thread parks in `send`.
2. Each worker finishes its job, receives one `Close` and exits. Each receive
   frees a slot, and flume moves the parked `Close` into it and wakes the
   dropping thread, so the channel stays full.
3. After the last worker has received its `Close`, it still counts in
   `active_thread_counter`. `ActiveThreadGuard` decrements only when the thread
   function returns, after `log::debug!("Worker #{i} closes because DB is
   dropping")`. If the dropping thread re-checks the counter inside that
   window, it sees 1 and calls `send` on a full channel that nobody will read
   again.

The window is small, but a worker preempted at the wrong moment (an
oversubscribed machine, a test suite, a slow `log` sink) is enough. The same
blocking `send` would also deadlock against the worker-side blocking sends:
worker 0 re-queuing `Compact`, and `inner_rotate_memtable`'s `send(Flush)`
when a `RotateMemtable` is being handled. If the channel is full and the only
live worker is blocked in one of those, the dropping thread cannot make
progress either.

### Reproducer

Use a debug build with 4 workers, several keyspaces with 16 MiB memtables,
about 48 MB of random 1 KiB values, and every memtable rotated right before
the drop:

```rust
let db = Database::builder(dir.path()).worker_threads(4).open()?;
let kss: Vec<_> = (0..4).map(|k| db.keyspace(&format!("ks{k}"),
    || KeyspaceCreateOptions::default().max_memtable_size(16 << 20)).unwrap()).collect();
for i in 0..48_000u64 { kss[(i % 4) as usize].insert(random_key(), random_1k())?; }
for ks in &kss { ks.rotate_memtable()?; }
drop(kss);
// drop on a helper thread; treat "not done in 15 s" as the hang
let (tx, rx) = std::sync::mpsc::channel();
std::thread::spawn(move || { drop(db); tx.send(()).ok(); });
assert!(rx.recv_timeout(Duration::from_secs(15)).is_ok(), "Database drop hung");
```

- **On an idle 32-core machine** this almost never hangs (0 in 60 runs).
- **With 3× CPU oversubscription** (96 busy-loop processes) it hangs in
  about 1 of 3 iterations.
- **To make it deterministic,** install a `log::Log` that sleeps 2 ms on the
  `"closes because DB is dropping"` record. That record is exactly the gap
  between the worker's last `recv` and the counter decrement. With that
  logger, 10 out of 10 single iterations hang.

### Suggested fix

Never block in the drop loop. Drain on every turn, which also releases any
worker blocked in a send, and offer one `Close` per live worker with
`try_send`:

```diff
-        while self
-            .active_thread_counter
-            .load(std::sync::atomic::Ordering::Relaxed)
-            > 0
-        {
-            let _ = self.worker_pool.sender.send(WorkerMessage::Close);
-            std::thread::sleep(std::time::Duration::from_micros(10));
-        }
+        loop {
+            let live = self
+                .active_thread_counter
+                .load(std::sync::atomic::Ordering::Relaxed);
+
+            if live == 0 {
+                break;
+            }
+
+            let _ = self.worker_pool.rx.drain().count();
+
+            for _ in 0..live {
+                let _ = self.worker_pool.sender.try_send(WorkerMessage::Close);
+            }
+
+            std::thread::sleep(std::time::Duration::from_micros(10));
+        }
```

- **A drain cannot take a `Close` from a waiting worker.** Flume hands a send
  directly to a receiver already parked in `recv`, so that message never sits
  in the queue. A busy worker finds a fresh `Close` on the next turn.
- **Shutdown semantics do not change.** Drop still waits for in-flight jobs
  before the teardown. It discards only queued messages, the same way the
  existing drains before and after the loop already do. Unflushed data stays
  covered by the journal (`Journal::drop` syncs, and recovery replays).

An alternative is to replace the counter polling with joining the stored
`thread_handles`, after the channel has been made unable to block: either an
unbounded channel, or dropping every `Sender` and having workers exit on
`Disconnected`. The current structure makes that harder, because `Keyspace`
also holds a `Sender`. The loop change above is the smallest fix.

With this change, the reproducer passes 150/150 widened iterations and 50/50
iterations under 3× oversubscription.
