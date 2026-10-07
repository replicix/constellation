# Vendored fjall 3.1.10: shutdown deadlock fix, fair writer lock, sync outside the journal lock, slow-commit report, version GC outside the version lock

This directory is fjall **3.1.10** exactly as published on crates.io
(checksum `cd201c93…c33f`, upstream commit
`3adaa50261c9be58484971dc561cdd563156765e`), used through
`[patch.crates-io]` in the workspace `Cargo.toml`. Only the files a build
needs are kept (`src/`, `Cargo.toml`, `README.md`) plus the license files
(`LICENSE-MIT`, `LICENSE-APACHE`). fjall is dual-licensed MIT OR Apache-2.0,
and both texts stay here unchanged.

## What is changed

There are six changes, each marked `CONSTELLATION PATCH`:

1. **`src/db.rs`, `impl Drop for DatabaseInner`**: this is the fix. The
   worker shutdown loop never blocks now.
2. **`Cargo.toml`, a `[lints.rust] warnings = "allow"` table at the end**:
   this is build hygiene only. A path dependency is not built with
   `--cap-lints allow` the way a registry crate is, so upstream's existing
   warnings (dead code, `unsafe_code`) would otherwise show up in every build
   of this workspace.
3. **`src/tx/single_writer/writer_lock.rs` (new), `mod.rs`, `write_tx.rs`,
   `lib.rs`**: the single-writer lock is a `WriterLock` with a priority
   class (`fjall::set_write_priority`), not a `Mutex<()>`. See
   [Change 3](#change-3-a-fair-writer-lock-with-a-priority-class).
4. **`src/journal/mod.rs`, `src/journal/writer.rs`**: `Journal::persist`
   syncs the journal file outside the journal writer's lock. See
   [Change 4](#change-4-sync-outside-the-journal-lock).
5. **`src/batch/mod.rs`, `src/batch/slow_commit.rs` (new)**: a commit that
   takes 500 ms or more logs where the time went. See
   [Change 5](#change-5-a-slow-commit-says-where-its-time-went).
6. **`src/keyspace/mod.rs`, `Cargo.toml`**: a memtable rotation's version
   GC drops old versions and removes their files after the version lock is
   released, and fjall builds against the vendored lsm-tree
   (`../lsm-tree`). See
   [Change 6](#change-6-version-gc-outside-the-version-lock).

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

## Change 3: a fair writer lock with a priority class

Every write transaction of a `SingleWriterTxDatabase` takes one lock.
Upstream it is a `std::sync::Mutex<()>`, which is not fair: a thread that
releases it and asks again takes it back before a woken waiter runs.
Constellation's metadata store has one writer that must not wait behind
the others, the node's authority core (a step of it is the node's whole
authority: replies to forwarded writes, lock grants and renewals,
delegation and backup traffic), and a dozen FUSE workers writing back to
back. Under `stress-ng-fs-nodes` the core waited seconds per transaction
for a lock each holder kept for milliseconds. Reproduced in
`crates/meta/src/store/gate.rs`'s test with eight busy writers: median
0.85 s, worst 12.5 s for the would-be priority writer with the mutex;
under one writer's hold (5 ms) with this lock.

- A thread that called `set_write_priority(true)` is served before every
  ordinary writer that is waiting.
- An ordinary writer that has waited `DEFER_MAX` (50 ms) is overdue: when
  the last holder was a priority writer, the lock goes to an overdue
  writer before the next priority one. A priority writer that writes
  continuously alternates with the others instead of starving them.
- Every wait is bounded (10 ms re-checks), so a wakeup that went to a
  waiter which could not take the lock is never the last one.
- There is no new lock-order constraint: a priority writer waits only for
  the current holder, as with the mutex.

`writer_lock.rs` has two unit tests (`cargo test --manifest-path
vendor/fjall/Cargo.toml --lib`, not run by the workspace; listed in
`docs/how-to-guides/development/TESTING.md`);
`crates/meta/src/store/gate.rs` tests the same through `Meta`.

## Change 4: sync outside the journal lock

Every commit appends to the journal under the journal writer's mutex.
Upstream's `Journal::persist` (behind `Database::persist`, which
Constellation calls for every `fsync` on a mount and for every write whose
safety rests on being remembered) held that mutex across the file's
`fsync`, so one thread's sync stalled every writer of the database for as
long as the disk took: seconds on a busy disk, observed as a 2.5 s sync
inside a core step. Now the buffer is flushed under the mutex and the
file is synced through a duplicate of its descriptor
(`Writer::flush_for_sync`) after the mutex is released. A sync covers
everything flushed before it, which is everything committed before the
call; writes appended meanwhile may or may not be included, as with any
concurrent `fsync`. A journal rotated meanwhile was synced by the
rotation itself. Errors are returned (and poison the database in
`Database::persist`) as before.

## Change 5: a slow commit says where its time went

A write transaction holds the single-writer lock until its commit
returns, so one slow commit stalls every writer of the database (and the
authority core's priority, change 3, cannot help: it waits for the
holder). `OwnedWriteBatch::commit` now times its phases — the journal
lock, the journal append, the batch's own persist, the keyspace table's
lock, the memtable inserts, the rotation and back-pressure checks — and a
commit of 500 ms or more is logged at `warn` (`slow commit: … ms for N
items (journal_lock=…ms memtables=…ms …)`). Nothing else changes.

What it showed under `stress-ng-fs-nodes` (overload-cascade, 2026-10-03):
commits of a few rows took up to 11.2 s, nearly all of it in the
memtable inserts. An insert is lock-free in the memtable, but first takes
its LSM tree's version-history lock (`RwLock::read`). A flush
(`register_tables`) or a compaction holds that lock for writing while it
persists the new version (`persist_version`: the version file's and the
directory's fsyncs, then `rewrite_atomic` of `current`, more fsyncs), so
under I/O pressure every insert into that keyspace waits for the disk,
with the database's single-writer lock held. That was lsm-tree 3.1.10's
design; the vendored lsm-tree no longer does any disk I/O under that lock
(`../lsm-tree/CONSTELLATION-PATCH.md`, `lsm-version-lock`).

## Change 6: version GC outside the version lock

`Keyspace::inner_rotate_memtable` garbage-collects every keyspace's old
versions after a rotation. Upstream calls `SuperVersions::maintenance` on
the guard of the tree's version history **write** lock
(`get_version_history_lock()`), and `maintenance` removes the old version
files and drops the old super versions there. Dropping the last one that
references a compaction's inputs runs their `Drop`, which unlinks the
files: after a major compaction, every input table's file was unlinked with
the lock held that every insert into the tree waits for. Now the guard only
takes the stale versions out (`take_garbage`, the vendored lsm-tree's) and
is released before `StaleVersions::remove` drops them and removes their
files. Errors are logged as before.

`Cargo.toml`'s `lsm-tree` dependency gains `path = "../lsm-tree"`, so that a
standalone build of this crate (`cargo test --manifest-path
vendor/fjall/Cargo.toml`) uses the vendored lsm-tree, which has
`take_garbage`. The workspace's `[patch.crates-io]` entry resolves any other
`lsm-tree` dependent to the same copy.

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

1. Remove the `fjall` entry of `[patch.crates-io]` and the `"vendor/fjall"`
   entry in `exclude` from the workspace `Cargo.toml`.
2. Delete `vendor/fjall/`.
3. Bump `fjall` in `crates/meta`, `crates/cli` and `crates/harness` to the
   fixed release, then run `cargo update -p fjall`.
4. Keep `crates/meta/tests/fjall_drop_deadlock.rs`. It should keep passing
   with the upstream fix, including under the widened window.

If the fix is still needed when moving to a newer fjall before upstream has
it, vendor that version instead and re-apply the `db.rs` hunk. The hunk is
self-contained and only touches `impl Drop for DatabaseInner`.

Changes 3, 4 and 5 have no upstream counterpart: dropping the vendored copy
means re-applying them to the new version (or giving up the writer
priority and the unlocked sync, which brings back the stalls described
above). Change 6 goes with the vendored lsm-tree
(`../lsm-tree/CONSTELLATION-PATCH.md`, "Dropping the patch").
