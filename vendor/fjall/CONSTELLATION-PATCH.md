# Vendored fjall 3.1.10: shutdown deadlock fix

This directory is fjall **3.1.10** exactly as published on crates.io
(checksum `cd201c93…c33f`, upstream commit
`3adaa50261c9be58484971dc561cdd563156765e`), used through
`[patch.crates-io]` in the workspace `Cargo.toml`. Only the files a build
needs are kept (`src/`, `Cargo.toml`, `README.md`) plus the license files
(`LICENSE-MIT`, `LICENSE-APACHE`). fjall is dual-licensed MIT OR Apache-2.0,
and both texts stay here unchanged.

## What is changed

There are two changes, both marked `CONSTELLATION PATCH`:

1. **`src/db.rs`, `impl Drop for DatabaseInner`**: this is the fix. The
   worker shutdown loop never blocks now.
2. **`Cargo.toml`, a `[lints.rust] warnings = "allow"` table at the end**:
   this is build hygiene only. A path dependency is not built with
   `--cap-lints allow` the way a registry crate is, so upstream's existing
   warnings (dead code, `unsafe_code`) would otherwise show up in every build
   of this workspace.

The directory is also listed in the workspace `exclude` list. That way it is
built as a plain dependency, and `cargo clippy --workspace`, `cargo fmt --all`
and `cargo test --workspace` do not treat it as a member.

## The bug

Upstream's drop loop looks like this:

```rust
self.stop_signal.send();
let _ = self.worker_pool.rx.drain().count();
while self.active_thread_counter.load(Relaxed) > 0 {
    let _ = self.worker_pool.sender.send(WorkerMessage::Close); // blocking
    std::thread::sleep(Duration::from_micros(10));
}
```

The channel is `flume::bounded(1_000)`. The pool keeps its own `rx`, so the
channel never disconnects, and a blocking `send` into a full channel waits
until somebody `recv`s.

1. Workers that are in the middle of a flush or compaction do not receive
   anything. The loop adds one `Close` per turn, so after about 1000 turns
   (a few tens of milliseconds, since `sleep(10µs)` really takes about 60µs)
   the channel is full and the dropping thread is parked inside `send`.
2. Each worker finishes its job, `recv`s one `Close` and exits. Each `recv`
   frees one slot, and flume immediately moves the parked sender's `Close`
   into it and wakes the sender. The channel stays full.
3. After the **last** worker takes its `Close`, it is still counted in
   `active_thread_counter`. `ActiveThreadGuard` decrements only when the
   thread function returns, and a `log::debug!("Worker #{i} closes because DB
   is dropping")` still runs before that. If the dropping thread wakes, sleeps
   10µs and re-checks the counter before that decrement, it sees `1` and calls
   `send` on a full channel that nobody will ever read again. It blocks
   forever.

This was confirmed in gdb on the hung process. `active_thread_counter == 0`,
the queue holds 1000 `Close` messages, one sender is pending (the dropping
thread, at `db.rs:76`), no receivers are waiting, and every `fjall:worker`
thread has exited.

Two hypotheses were checked and ruled out as the cause:

- **Leaked counter.** 3.1.10 already decrements through `ActiveThreadGuard`
  on every exit path (return, error, panic), and `claim_and_spawn` counts only
  threads that actually spawned. The counter does reach 0, but only after the
  dropping thread has committed to a `send` that can never complete.
- **A worker blocking on a follow-up enqueue.** Some worker-side sends really
  do block: worker 0 re-queues `Compact`, and `RotateMemtable` leads to
  `inner_rotate_memtable`, which calls `send(Flush)`. Both could join a
  deadlock if the channel were full. They were not involved in the observed
  hangs, though: no worker was alive. Because the channel is FIFO and the
  queue is drained before the first `Close`, they are much harder to hit. The
  fix covers them anyway, because it drains the channel on every turn.

The simulator hits this under load because oversubscribed CPUs make it likely
that the last worker is preempted inside that window. Production can hit it
too: unmounting drops `Meta`, and with it the fjall `Database`.

## The fix

```rust
loop {
    let live = self.active_thread_counter.load(Relaxed);
    if live == 0 { break; }
    let _ = self.worker_pool.rx.drain().count();
    for _ in 0..live {
        let _ = self.worker_pool.sender.try_send(WorkerMessage::Close);
    }
    std::thread::sleep(Duration::from_micros(10));
}
```

- **`try_send` means the dropping thread never parks.** The loop ends as soon
  as the counter reaches 0.
- **Draining on every turn keeps the queue from filling.** It holds at most
  one `Close` per live worker plus whatever follow-up work a finishing worker
  enqueued. `drain()` also calls flume's `pull_pending`, which releases any
  worker that is blocked sending into a full channel.
- **A discard cannot starve a worker.** Flume hands a `send` or `try_send`
  directly to a receiver that is already waiting in `recv`, so that `Close`
  never sits in the queue where a drain could take it. A worker that is busy
  finds a fresh `Close` on the next turn after it finishes.

### Durability

On drop, upstream never flushes memtables and never waits for queued work.
It discards the queue before this loop and again after it, waits only for the
jobs already running, and then relies on the journal. `Journal::drop` does
`persist(SyncAll)` once the last `Arc<Journal>` goes away, and recovery
replays sealed and active memtables from the journals. Journals are deleted
only by `JournalManager::maintenance` once every keyspace has flushed past
them. The patch keeps all of this:

- It still waits for `active_thread_counter == 0` before clearing the flush
  manager, keyspaces and journal manager. Flushes and compactions already
  running finish exactly as before.
- It discards only queue messages: `Close`, `Compact`, `Flush` and
  `RotateMemtable`. That is the same kind of work upstream's own drains
  discard. A dropped `Flush` or `RotateMemtable` leaves its data in a
  memtable backed by a journal that has not been evicted, and the next open
  replays it. A dropped `Compact` is an optimisation. Upstream also lost these
  messages in practice: work enqueued during shutdown lands behind the `Close`
  messages, and the final drain throws it away.
- Nothing about journal persistence, `Journal::drop` or recovery changes.

## Tests

`crates/meta/tests/fjall_drop_deadlock.rs` opens a database with 4 workers,
writes about 48 MB into 4 keyspaces with 16 MiB memtables, seals every
memtable, and drops the database on a helper thread with a timeout. It
repeats this `FJALL_DROP_ITERS` times (default 12).

By default the test installs a `log` logger that sleeps 2 ms on the worker's
"closes because DB is dropping" line. That line runs exactly inside the race
window, so the logger widens the window the way a preempted worker would.
`FJALL_DROP_WIDEN=0` turns the logger off.

| setup                                             | unpatched         | patched     |
|---------------------------------------------------|-------------------|-------------|
| widened, idle 32-core machine                     | 10/10 runs hang on iteration 0 | 150/150 ok |
| not widened, idle machine                         | 0/60 hang         | —           |
| not widened, 96 spinning processes on 32 cores    | hung on iteration 4, 1, 0 and 1 in 4 runs (about 1 in 3) | 50/50 ok |

## Dropping the patch

Once upstream fixes this (see `../ISSUE-fjall.md`, the issue to file):

1. Remove the `[patch.crates-io]` table and the `"vendor/fjall"` entry in
   `exclude` from the workspace `Cargo.toml`.
2. Delete `vendor/fjall/`.
3. Bump `fjall` in `crates/meta`, `crates/cli` and `crates/harness` to the
   fixed release, then run `cargo update -p fjall`.
4. Keep `crates/meta/tests/fjall_drop_deadlock.rs`. It should keep passing
   with the upstream fix, including under the widened window.

If the fix is still needed when moving to a newer fjall before upstream has
it, vendor that version instead and re-apply the `db.rs` hunk. The hunk is
self-contained and only touches `impl Drop for DatabaseInner`.
