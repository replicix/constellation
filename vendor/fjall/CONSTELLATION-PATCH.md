# Vendored fjall 3.1.10: shutdown deadlock fix, fair writer lock, sync outside the journal lock, slow-commit report, version GC outside the version lock, journal rotation without fsyncs under the journal lock

This directory is fjall **3.1.10** exactly as published on crates.io
(checksum `cd201c93…c33f`, upstream commit
`3adaa50261c9be58484971dc561cdd563156765e`), used through
`[patch.crates-io]` in the workspace `Cargo.toml`. Only the files a build
needs are kept (`src/`, `Cargo.toml`, `README.md`) plus the license files
(`LICENSE-MIT`, `LICENSE-APACHE`). fjall is dual-licensed MIT OR Apache-2.0,
and both texts stay here unchanged.

## What is changed

There are seven changes, each marked `CONSTELLATION PATCH`:

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
7. **`src/journal/rotation.rs` and `rotation_test.rs` (new),
   `src/journal/{mod,writer,manager,reader,batch_reader}.rs`,
   `src/worker_pool.rs`, `src/recovery.rs`, `src/db.rs`**: a journal
   rotation does no fsync under the journal writer's lock, and recovery
   syncs every journal and checks the rotation markers that keep its
   crash ordering. See
   [Change 7](#change-7-journal-rotation-without-fsyncs-under-the-journal-lock).

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
concurrent `fsync`. A sync that started before a rotation syncs the old
journal, which holds everything committed before it; one that starts after
a rotation also waits for the old journal's sync (change 7). Errors are returned (and poison the database in
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

## Change 7: journal rotation without fsyncs under the journal lock

Before each flush, a worker rotates the journal once it is over 64 MB.
Upstream did it with the journal writer's mutex held across three fsyncs:
the old journal's `sync_all`, the new file's `sync_all` after `set_len`
(pre-allocation), and the folder's. Every commit takes that mutex, so every
commit waited for them once per 64 MB of journal: 337–351 ms with an 80 ms
fsync, and 530–675 ms commits under `stress-ng-fs-nodes`, all
`journal_lock` (`lsm-version-lock` in `docs/plans/v1/PROGRESS.md`).
`rotation::rotate_if_full` now does it in three steps:

1. **Prepare, outside the lock**: create, pre-allocate and sync the next
   file (`Writer::create_new`), fsync the folder. A rotation mutex
   (`Journal::lock_rotation`) keeps two workers from preparing the same
   file; it is held up to the swap.
2. **Swap, under the lock** (with the journal manager's and keyspaces'
   locks, in upstream's order): `Writer::swap_to` writes out the old
   journal's buffer to the OS, switches to the new file and writes the
   **rotation marker** into it: an empty batch (`Start { item_count: 0,
   seqno: L }`, `End` with the checksum of nothing) whose seqno `L` is that
   of the last batch written to the old journal (`Writer::last_seqno`). The
   new writer holds a `Seal` for the old journal's sync, chained to any
   earlier seal still pending. No fsync.
3. **Seal, outside the lock**: `SealedJournal::sync` `sync_all`s the old
   journal and completes the seal (a dropped `SealedJournal` fails it, so
   nobody waits forever). The worker then waits for every pending seal
   before it flushes, as a flush used to start only after any rotation's
   syncs (it queued on the journal lock); a commit never waits for it. The
   next rotation need not wait (its seal is chained to this one), so the
   active journal stays near 64 MB under load.

A persist with `SyncAll` or `SyncData` (`Journal::persist`, outside the
lock as in change 4, and `Writer::persist`, the in-lock path of a batch
with durability) waits for the pending seals after its own sync, and
fails if one failed. `PersistMode::Buffer` and commits without durability
never wait: they reach the OS at once, as before, so a process crash loses
nothing.

**Recovery** (`RotationChain`, in `recover_sealed_memtables` and the active
journal's replay in `Database::recover`): first, before reading anything,
`Database::recover` `sync_all`s every sealed journal, oldest first
(`rotation::sync_sealed_journals`), then the active one (upstream). Then
journals are replayed oldest first, as before. When a journal after the
first starts with a marker whose seqno is not the previous journal's last
batch seqno, that journal and every later one are emptied (`set_len(0)`,
synced; an emptied sealed journal has no watermarks, so journal
maintenance deletes it) and not replayed. A sealed journal with a batch
that does not read back whole (`ChecksumMismatch`, `InsufficientLength`,
`TooManyItems`, which upstream returns as an error from
`Database::open`) is truncated after its last whole batch and ends the
replay the same way: every later journal is emptied. Upstream journals
never start with an empty batch, so the marker check never fires on them.
The on-disk format is unchanged: upstream's recovery reads a marker as a
batch with no items.

`Writer::last_seqno`, the next marker's seqno, is set only once a batch's
`End` entry is written (`write_raw`, `write_clear`, `write_batch`,
`write_marker`): a batch that fails part-way (the caller gets an error and
the database goes on) is not the journal's last batch, so a marker never
names it.

### Why a crash at any point recovers a prefix

Seqnos are allocated under the journal lock (`batch/mod.rs`,
`keyspace/mod.rs` insert/remove/clear) and each batch is written before
the lock is released, so batch seqnos strictly increase in journal order,
across files too (recovery restarts the counter above the highest
recovered seqno). The reader (`JournalReader`, `JournalBatchReader`)
stops at the first invalid entry and truncates there, so a journal
replays a prefix of its batches. Let `N` be the old journal, `N+1` the
new one, `L` the marker's seqno:

- **Before the swap** (`N+1` prepared, empty): a crash recovers `N` as
  sealed and `N+1` as active and empty. `N` replays a prefix of its
  batches. A write acknowledged as durable was synced in `N` by its
  persist, so it is in that prefix.
- **After the swap, before `N`'s sync completes**: `N+1` holds the marker
  and later batches; any of its pages may or may not be on disk, and so
  may `N`'s unsynced tail. If `N` ends with batch `L`, `N` is complete:
  every batch before `L` precedes it in the file, and the reader stops at
  the first entry that does not decode. A page of `N` that never reached
  the disk reads back as zeros (`N` was pre-allocated with `set_len`, so
  an unwritten page is a hole), and zeros do not decode as an entry, so a
  lost page in the middle stops the reader there and `N` cannot end with
  `L`. (Past the pre-allocation, a lost page is past the file's durable
  length or reads as zeros too, as file systems that do not expose stale
  data guarantee; if a lost page still leaves a later batch's `End`
  readable, that batch fails its checksum, below. Either way `N` ends
  before `L`.)
  Then `N+1`'s prefix follows `N`: the whole is a prefix. If `N` does not
  end with `L`, `N` lost its tail; `N+1` and every later journal are
  emptied, and what remains is `N`'s prefix. Nothing emptied was
  acknowledged as durable: a durable persist after the swap returns only
  after `N`'s sync, which would have made `L` durable. If `N+1`'s first
  page is lost, `N+1` replays nothing, and the next journal's marker (if a
  later rotation happened) names `N+1`'s last seqno, which is then
  missing: emptied too.
- **A restart between these steps, then a power loss**: the cases above
  assume that the persists that matter are the crashed process's. After a
  process crash before `N`'s sync (or before the swap, when `N` holds
  unsynced `Buffer` commits and `N+1` is already there), `N`'s tail is
  still in the page cache: the new process reads `N` whole, finds the
  chain intact, and replays everything. Its writer has no pending seal and
  no `last_seqno`, so without more, a durable persist there would sync
  only the active journal, and a later power loss could still take `N`'s
  tail: after the swap, the marker check would then empty `N+1` and the
  active journal with the new process's durable writes; before the swap,
  the new process's writes go into `N+1` with no marker, and would replay
  right after `N`'s cut tail, not a prefix and not detectable. Recovery
  therefore syncs every sealed journal before it reads them, and the
  active one after: when `Database::open` returns, every journal is
  durable as it was read, which is the state a crash at the end of
  `Database::open` would recover, and the new process starts as from a
  rotation whose syncs all completed. A power loss during recovery,
  before those syncs, loses nothing the new process acknowledged (it has
  acknowledged nothing yet) and leaves the files as one of the crashed
  process's own power-loss cases above. The new files a recovery creates
  (an active journal when there was none) are synced with their folder
  (`Journal::create_new`); the files it reads were created, and their
  folder synced, before anything was written to them, so their folder
  entries need no new sync.
- **A sealed journal with a batch that does not read back whole** (a lost
  page in the middle of a batch whose `End` entry, on a later page,
  survived; or damage to synced data): its whole batches before that one
  are replayed and every later journal is emptied, so recovery still
  replays a prefix instead of failing. Upstream returned the error from
  `Database::open`. Remaining risk: damage to a synced sealed journal
  (bit rot, not a power loss) now loses the later journals, including
  writes acknowledged as durable, instead of refusing to open; the log
  says which journals were discarded (`warn`). The active journal keeps
  upstream's behaviour (such a batch fails `Database::open`): it has the
  same exposure to a lost middle page as upstream's.
- **After `N`'s sync**: `N` is complete and ends with `L`; `N+1` replays
  a prefix. Upstream's guarantees.
- A first journal with a marker is not checked: its predecessor was
  deleted by journal maintenance, which deletes oldest first and only
  journals whose data every keyspace has flushed to tables.
- A rotation from a writer that wrote no batch (a recovered journal with
  no writes since) writes no marker: recovery syncs every journal on open
  (`Database::recover`) and a new journal is synced on creation, so that
  journal has no tail to lose.

Not changed: fjall never syncs the journal before a flush, so a flushed
table can hold a write whose predecessors (in another keyspace) are in an
unsynced journal tail; that is upstream's behaviour, with or without this
change. Upstream's own one-call rotation (`Writer::rotate`, used by its
journal unit tests, and compiled only for tests) runs the same steps.

### Tests

`src/journal/rotation_test.rs` (in `cargo test --manifest-path
vendor/fjall/Cargo.toml --lib`), with test-only hooks keyed by database
folder (`rotation::test_hooks`: rotation threshold, slow journal syncs,
callbacks at `Prepared`, `Swapped`, `Sealed`, and a callback after every
journal sync, `rotation::after_sync`, which recovery's truncations and
discards call too):

- `commits_never_wait_for_a_rotation_sync`: every journal sync slowed by
  300 ms, three rotations; a writer inserting every millisecond must stay
  under 150 ms. Upstream's rotation: 900 ms (fails); this one: 3.5 ms.
- `a_crash_before_the_swap_recovers_a_prefix`,
  `a_crash_before_the_sealed_journal_is_synced_recovers_a_prefix`,
  `a_crash_after_the_sealed_journal_is_synced_loses_nothing`: 100 writes
  persisted with `SyncAll`, 40 more, then at the kill point 40 more and
  crash images: the folder as is (a process crash: all 180 recovered) and
  power-loss images with the old journal's unsynced tail cut at four
  points from the durable length, the new journal kept whole. Each image
  must recover keys `0..n` with `n ≥ 100`; the cut at the durable length
  after the swap must recover exactly 100. Without the marker check that
  image recovers key 140 right after 99 (fails).
- `a_durable_persist_after_the_swap_waits_for_the_sealed_journal` and
  `a_rotation_does_not_wait_for_the_previous_sealed_sync` (the second
  rotation completes while the first sync is held; a `SyncAll` persist
  waits for both).
- `a_restart_after_a_crash_before_the_swap_syncs_the_sealed_journal` and
  `a_restart_after_a_crash_before_the_sealed_sync_syncs_the_sealed_journal`:
  restart, then power loss. Every journal sync records the file's contents
  (what a power loss keeps, at worst). The process "dies" at `Prepared`
  (resp. `Swapped`) with 180 writes, 100 of them synced; the sealed
  journal is cut to its written length (as if it had outgrown its
  pre-allocation, so recovery's truncation does not sync it by the way).
  The image restarts with no workers, the test asserts that the sealed
  journal was synced during `Database::open`, writes key 180 with a
  `SyncAll` persist, and then every journal is reset to its last synced
  contents. That image must recover all 181 keys. Without recovery's sync:
  100 keys (`Swapped`: `N+1` discarded with the durable key 180) and keys
  `0..100` then 180 (`Prepared`: not a prefix).
- `a_damaged_batch_in_a_sealed_journal_ends_the_prefix`: one value byte in
  the middle of a synced sealed journal changed; recovery must open and
  replay `0..n` with `0 < n < 140` (upstream: `ChecksumMismatch` from
  `Database::open`).
- `writer::last_seqno_test::a_failed_batch_is_not_the_last_seqno`: a
  batch that fails part-way (the file turned read-only) leaves
  `last_seqno` at the last whole batch.

`crates/meta/tests/compaction_stall.rs`'s
`journal_rotations_never_freeze_commits` measures commits across real
64 MB rotations (numbers in `fjall-journal-rotation`, PROGRESS).

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

Changes 3, 4, 5 and 7 have no upstream counterpart (7 is filed in
`../ISSUE-fjall.md`): dropping the vendored copy
means re-applying them to the new version (or giving up the writer
priority and the unlocked sync, which brings back the stalls described
above). Change 6 goes with the vendored lsm-tree
(`../lsm-tree/CONSTELLATION-PATCH.md`, "Dropping the patch").
