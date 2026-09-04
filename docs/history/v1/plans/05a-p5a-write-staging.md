# Plan 05a — Phase 5a: bounded-memory write staging

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–05
committed. Spec: `docs/DESIGN.md` §7 (cache states: `dirty` is never
evicted until uploaded), §9 (reserve-before-accept, the "evict → throttle
→ ENOSPC" ladder), §6 (close-to-open). Roadmap exit: a file many times
the cache budget is written with a flat RSS ceiling, and a `kill -9`
mid-write leaves no orphaned staging bytes.

**Goal: daemon RSS must not scale with the size of the file being
written.** Today it does, linearly, and a 10 GB copy grows the process
until the writer stops. This plan does not add streaming uploads (that
is 05b) — it only moves in-flight bytes out of RAM and onto disk, and
fixes two correctness gaps that become the normal path once 05b makes
deferred uploads routine.

## Why the two prerequisite fixes belong here

Both are real, verified in tree, and narrow *today* only because eager
upload almost always succeeds on the first try. Under 05b's write-back
they are the common case, so 05b is unsafe without them. They also
supply the durable "not yet uploaded" set that 05b's zero-RTT dedup
tier and drain accounting both read.

**1. Dirty state does not survive a crash.** `DiskCache::rescan`
(`crates/fs-core/src/cache.rs`) marks every rediscovered chunk `Clean`,
and the test `rescan_rebuilds_accounting` asserts exactly that, with a
comment claiming "the meta journal is the source of truth for pending
uploads" — aspirational, not implemented. After `kill -9` with an
un-uploaded chunk, `dirty_chunks()` returns empty, so
`upload_dirty_chunks` (`crates/cli/src/main.rs`) uploads nothing, while
the `WriteManifest` record is still in the journal and ships happily.
Remote nodes then 404 on that chunk: `ls` works, `read` fails.

**2. Clean unmount skips the upload gate.** After `fuser::mount2`
returns, `crates/cli/src/main.rs` calls `ship.shutdown_all(&mut keepers)`
with no `upload_dirty_chunks` first. If an earlier `try_upload_dirty`
failed and only logged (it is best-effort by design), an orderly unmount
strands exactly the same way. Note the asymmetry that proves this is an
oversight rather than a decision: the `Leave` handler and the epoch-close
path in `run_managed_sync_round` both already upload before flushing.

## Step 1 — `pending_upload`: the durable not-yet-uploaded set

New table in `crates/meta/src/sqlite.rs`, written in the **same
transaction** as the journal record so it cannot drift from the log:

```sql
CREATE TABLE IF NOT EXISTS pending_upload (
  hash BLOB NOT NULL,
  ino  INTEGER NOT NULL,
  PRIMARY KEY (hash, ino)
);
```

- `SqliteMeta::set_manifest` already opens a transaction to update the
  inode and journal a `WriteManifest`. Insert one `pending_upload` row
  per chunk hash in that same transaction. Same-hash-different-inode is
  why the key is a pair; dedup across inodes is handled by the upload
  side, not by dropping rows.
- `pending_uploads() -> Vec<(ChunkHash, Ino)>` and
  `ack_upload(hash, ino)` (delete). Deleting a row for a hash that is
  still pending under another inode must not affect that other row.
- Replace `cache.dirty_chunks()` as the drain source in
  `upload_dirty_chunks`. The cache's in-memory `Dirty` marking stays as
  the eviction guard it already is (`plan_eviction` refuses to evict
  dirty entries) — this plan does not change cache eviction, it changes
  *who decides what still owes S3 a PUT*.
- A pending row whose chunk is missing from the local cache after a
  crash is unrecoverable content: the bytes were never uploaded and the
  staging file is gone. Log it loudly, surface a counter, and leave the
  row. Do **not** silently drop it and do **not** let the segment ship;
  `set_skip_ship` already exists for exactly this "journal must wait"
  state. Reaching this state requires losing staging before the cache
  insert, which the ordering in Step 4 makes impossible on a clean
  crash — it is a torn-disk case, so report it rather than paper over it.

This is what finally makes the `rescan_rebuilds_accounting` comment
true, so **amending that test's comment and adding the companion
meta-side assertion is explicitly authorized** (see Tests). The cache's
behavior is unchanged; only the claim about where truth lives becomes
accurate. This is a contract change, not the assertion-loosening
CONVENTIONS.md rule 4 forbids.

## Step 2 — upload before clean unmount

In `crates/cli/src/main.rs`, run `upload_dirty_chunks` (now
pending-table driven) before `ship.shutdown_all(&mut keepers)` on the
post-`mount2` path. On failure, do not ship: leave the journal
un-shipped with `set_skip_ship(true)` and exit non-zero with a message
naming `constellation status` — the next mount drains it. An unmount
that silently publishes manifests for chunks that are not in S3 is worse
than an unmount that refuses to finish cleanly.

## Step 3 — the staging area

New module `crates/cli/src/staging.rs`, standalone so it unit-tests
without FUSE (the way `cli/src/sources.rs` isolates the selection
policy). One sparse file per open, dirty inode:

```
<state_dir>/staging/<ino>.<gen>
```

`<gen>` is a per-mount monotonic counter, so a stale file from a
previous life is never confused with a live one and GC needs no locking.

Public shape, roughly:

- `Staging::create(dir, ino, gen, budget) -> Result<Staging>`
- `write_at(offset, buf)` — `std::os::unix::fs::FileExt::write_at`
- `read_at(offset, buf)` — for chunk materialization
- `set_len(len)` — truncate
- `dirty_indices() -> impl Iterator<Item = u64>`
- `file_len()`, `reserved_bytes()`, `discard()`

**Per-inode RAM becomes `file_len`, `base: Option<Manifest>`, and a
dirty chunk-index set.** State the ratio honestly rather than claiming
O(1): a plain `BTreeSet<u64>` is ~8 bytes per dirty 4 MiB chunk, so a
10 GB file costs ~20 KB and a 10 TB file ~20 MB. Sequential append —
the shape that actually matters, and the shape `rsync` and `cp` produce
— collapses to a handful of runs under run-length encoding, so store
runs (`Vec<(start, end)>` merged on insert), not individual indices.
Roaring is available if a random-write workload ever justifies it; a
run-length set is a few dozen lines and enough here.

**mmap is rejected, deliberately.** A `SIGBUS` from ENOSPC or from
truncation of a mapped region kills the daemon with no errno path, and
there is no way to apply backpressure to a writer that is just touching
memory. `pwrite` returns ENOSPC as a value, which the FUSE boundary can
map through the existing `errno()`. There is no `mmap`/`memmap2`
anywhere in the workspace today; keep it that way, and say why in the
module docs so the next reader does not "optimize" it.

## Step 4 — wire the FUSE write path

Replace `WriteState.chunks: BTreeMap<u64, Vec<u8>>`
(`crates/cli/src/fusefs.rs`) with the staging handle plus the dirty-run
set. The surrounding logic keeps its shape:

- `do_write` (`crates/cli/src/fusefs_ops.rs`) keeps its
  `ChunkLayout::slices` loop, but issues `write_at` at the logical
  offset instead of growing a `Vec`, and records the touched chunk
  indices.
- `materialize_chunk` reads pending bytes back with `read_at`, so peak
  RSS becomes O(chunk_size × concurrency) instead of O(file_len).
- `truncate` maps 1:1: `set_len` on the staging file, drop dirty runs
  past the new end, and keep the existing final-chunk re-cut in
  `flush_inode` (the shrink case that reuses `old_hashes`).
- `flush_inode` seals **one chunk at a time**: read that chunk's range
  back, hash it, `cache.insert(&hash, &data, ChunkState::Dirty)`, drop
  the buffer, next. Never build a `Vec` of all chunk bodies. The
  existing `epoch_active` check and `try_upload_dirty` call stay exactly
  as they are — 05b changes them, 5a does not.

Ordering that must hold: a chunk's bytes are in the cache (durable,
`write_atomic` does `sync_data` + rename) **before** `set_manifest`
journals the record and inserts the pending row. That is already the
order `flush_inode` uses; preserve it.

**Durability parity, unchanged.** `write()` returns after `pwrite` with
no fsync — the same POSIX contract as today's RAM buffer, and the same
as page cache on a local filesystem. `fsync`/`fdatasync`/`O_SYNC` seal
affected chunks into the cache first and then run the existing
`sync_barrier` (`crates/cli/src/fusefs.rs`), so `--fsync-mode s3` still
means "durable in the shared log". Staging itself is scratch and is
never fsynced: mount-time GC clears it, so a never-fsynced,
never-closed write is lost exactly as it is today. Do not add an fsync
of the staging file — it would cost write throughput to protect data
that POSIX does not promise.

## Step 5 — staging budget

Staging bytes are reserve-before-accept, like the cache
(`DiskCache::insert`), so one large write cannot fill the partition the
cache lives on:

- `CONSTELLATION_STAGING_BUDGET`, default a fraction of `--cache-size`
  (1/4 is a reasonable default; document the choice where it is read).
- Reservation failure is a clean `ENOSPC` at the FUSE boundary with no
  partial state — match the rollback discipline `DiskCache::insert`
  already implements.
- Surface `staging_bytes` and `staging_budget_bytes` on `CacheStatus`
  (`crates/api/src/types.rs`) and show them in `status`. The harness
  asserts against these, so they are not decoration.

Throttling before ENOSPC is 05b's dirty-budget work; 5a only needs the
hard bound and a clean errno.

## Step 6 — mount-time GC

At mount, before the FUSE loop starts, delete everything under
`<state_dir>/staging/`. Nothing there can be live: the generation
counter guarantees a new mount never reuses a name, and staging holds
only data POSIX permits losing on a crash. Log the reclaimed byte count
at info level — a large number after a crash is the operator's signal
that a big write was in flight.

## Tests

### Unit — `cli::staging`

- Round-trip: writes spanning chunk boundaries via `ChunkLayout::slices`
  read back byte-exact; unwritten ranges inside `file_len` read back as
  zeros (sparse hole, not stale bytes).
- Truncate down drops dirty runs past the new end and re-cuts the
  boundary chunk; truncate up, then write past the old end, leaves a
  hole rather than stale bytes. This closes the debt bullet
  `docs/PROGRESS.md` records as "a truncate to a non-chunk boundary
  followed by extension needs a targeted test".
- Budget: reserve-before-accept returns the staging-full error and
  leaves no partial state, mirroring the assertion style of
  `pinned_and_dirty_not_evicted` in `crates/fs-core/src/cache.rs`.
- Dirty-run memory: a sequential append of N chunks must stay within a
  numeric ceiling (a handful of runs, not N entries), so the
  "not linear in file size" property is a test rather than a comment.
  A deliberately fragmenting random-write pattern is a second case with
  its own, higher, ceiling — assert the run-merge actually merges.
- GC removes orphaned `staging/*` files and leaves a live generation
  alone.

### Unit — `meta::sqlite`

- `set_manifest` inserts `pending_upload` rows in the same transaction
  as the `WriteManifest` record; a forced transaction failure leaves
  neither the journal row nor the pending rows.
- `ack_upload` deletes one row; a second inode still pending on the same
  hash keeps its own row.
- `pending_uploads()` after drop-and-reopen (`SqliteMeta::open` on the
  same file — the crash simulation) still returns the un-uploaded
  hashes.
- **Authorized amendment:** reword the `rescan_rebuilds_accounting`
  comment in `crates/fs-core/src/cache.rs` so it states what is now
  true — the cache legitimately returns `Clean` after a rescan because
  the pending-upload table, not the cache, is the source of truth — and
  add a `fs-core`-side or `cli`-side test that a post-crash `Clean`
  cache plus a pending row still yields a drain. The existing
  assertions on accounting stay as they are.

### In-process multi-node — `cli`

Follow the pattern at the bottom of `crates/cli/src/shipper.rs`
(`object_store::memory::InMemory` + `SqliteMeta::open_in_memory()`):

- A node with an un-uploaded chunk must **not** ship the segment that
  references it: force `put_chunk` to fail, assert no segment object
  appears and the journal rows are retained.
- After the upload succeeds, the segment ships, and a second in-process
  node applies it and fetches the chunk successfully.
- Restart with a pending row present: the drain finds it even though the
  cache reports every chunk `Clean`. This is the regression test for
  prerequisite 1.
- Clean shutdown with a pending row uploads before flushing, and refuses
  to ship if the upload fails. Regression test for prerequisite 2.

### Harness — `crates/harness/src/scenarios.rs`

- `big-file-write`: mount with a small `--cache-size` (64 MiB) and write
  a file several times that size, sampling daemon RSS during the write.
  Assert a fixed RSS ceiling **and** that RSS does not track bytes
  written — the phase's whole point. `Client` already keeps the child
  handle and derives a pid for `kill9`/`pause`
  (`crates/harness/src/client.rs`), so add a small
  `Client::rss_bytes()` that reads `VmRSS` from `/proc/<pid>/status`.
  Content is model-verified on readback. Keep the file size sane for CI
  (a few hundred MiB is enough to prove the slope is flat).
- `staging-crash`: `kill -9` mid-write, remount, assert the mount
  succeeds, `staging/` is empty after GC, and the file is at its last
  fsynced/closed size. Absent or short is a pass; corrupt is not.
- `unmount-drain`: fail the eager upload (toxiproxy cut scoped to the S3
  path, or a kill-switch env), then unmount cleanly. A second node must
  read the file with no missing chunk. **This fails on today's tree** —
  it is the regression test for prerequisite 2, and seeing it fail
  before the fix is the point.

pjdfstest is the real acceptance gate for this phase: the staging
rewrite touches exactly the truncate, extend, and hole semantics its
8798 checks hammer. `fio-latency` and `fio-blips` (random writes, now
routed through staging) are the second tripwire.

## Out of scope

- Eager/streaming upload, parallel uploads, the dedup ladder, and the
  write-through/write-back policy: all 05b. Leave `try_upload_dirty`
  and `upload_dirty_chunks` serial here.
- Throttling as dirty bytes approach the budget: 05b. 5a stops at a
  hard bound with a clean ENOSPC.
- Serving dirty chunks to peers: `get_servable` deliberately excludes
  them, and `dirty_chunks_are_not_servable` asserts it. Unchanged.
- Slice overlays / random-write optimization: format-reserved
  (DECISIONS.md ADR-11).

## Gates + report

Per CONVENTIONS.md, all six gates green. Add a **Phase 5a** milestone
table to `docs/PROGRESS.md` in the established item/state/where style,
retarget the two phase-1 debt bullets this plan closes, and record the
prerequisite gaps as fixed. Update `docs/TESTING.md` with the three new
scenarios. Paste the `big-file-write` RSS ceiling and the observed
peak into the report — that number is the phase's exit criterion.
