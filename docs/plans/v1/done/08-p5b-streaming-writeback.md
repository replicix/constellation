# Plan 08 — Phase 5b: streaming writes and the write-back policy

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–06 and
**07** committed — this plan drains the staging area chunk by chunk and
relies on the durable `pending_upload` table for crash-safe tracking of
what still owes S3 a PUT. Spec: `docs/DESIGN.md` "Streaming writes:
files larger than the cache" (already specified — implement it, do not
edit DESIGN.md, CONVENTIONS.md rule 5), §7 (cache states, source
selection EWMAs), §9 (the "evict → throttle → ENOSPC" ladder), §6
(close-to-open). Roadmap exit: a file larger than the local cache is
written successfully, and an rsync-shaped workload under injected S3
latency is several times faster under write-back than write-through.

The problem this solves, measured: on a European host against a
`us-west-2` bucket, `close()` pays at least one full S3 round trip per
chunk, serially, inside the single-threaded FUSE loop. The phase-1
benchmark note in `docs/PROGRESS.md` already names this — "the import
rate is serial-flush bound (one chunk PUT + manifest per close through
the single-threaded FUSE loop); parallel writeback is the known phase-2+
lever". This is that lever.

## Three levers, all in the same three functions

`flush_inode` and `try_upload_dirty` (`crates/cli/src/fusefs.rs`) and
`upload_dirty_chunks` (`crates/cli/src/main.rs`).

## Step 1 — Eager upload (streaming writes)

When a sequential writer crosses a chunk boundary, that chunk is final.
Seal it then, instead of at `flush`:

- Seal = read the range back from staging, hash, `cache.insert(Dirty)`,
  record the pending row, queue the upload, release the staging range.
- Only the in-flight window is held locally, so **maximum file size
  stops being bounded by local disk** and is bounded by S3 instead.
  That is the DESIGN.md streaming-writes property and this phase's
  headline exit criterion.
- Seal each boundary chunk exactly once. A chunk still being appended to
  must not be sealed, and repeated writes inside an already-sealed chunk
  must re-dirty it (correct, and rare) rather than corrupt the seal.
- The manifest is still committed once, at `flush_inode`. Eager upload
  moves *chunk* work earlier; it does not publish a partial file, so
  close-to-open is untouched.
- Detection can reuse the sequential-write signal the prefetcher already
  computes for reads (`cli/src/prefetch.rs`); a simpler "all bytes below
  this offset are written" high-water mark is acceptable and easier to
  reason about. Document which one you chose.

## Step 2 — Parallel uploads, and the two per-chunk costs

A bounded worker pool (`CONSTELLATION_UPLOAD_CONCURRENCY`, default 8,
sane range 1–32) replaces both serial loops. Uploads are independent —
content-addressed, immutable, and the only ordering constraint in the
system is "all chunks a segment references are in S3 before that segment
ships", which is a barrier at ship time, not an ordering among PUTs.

Two costs only matter once the pool exists, and both are real:

### 2a. The dedup probe costs a whole extra round trip

`ChunkStore::put_chunk` (`crates/store-s3/src/store.rs`) does a `head()`
before every PUT, so each chunk is 2 RTTs. The probe is never a
correctness requirement — chunks are content-addressed and `get_chunk`
verifies blake3 on read — so it is worth its RTT only when dedup
actually hits. On request price alone the break-even is ~8% (HEAD
$0.0004/1k against PUT $0.005/1k), but on a thin uplink a hit also saves
transmitting the chunk body, so the real threshold is bandwidth-aware.
A bulk first write hits ~0%; an `rsync` re-copy of an unchanged tree
hits ~100%. A single fixed choice is wrong for one of them, so build a
ladder, cheapest tier first:

1. **Local durable set — 0 RTT.** A chunk in the cache and absent from
   `pending_upload` is known durable in S3: skip the HEAD *and* the PUT.
   This covers re-copies, repeated edits to one chunk, and retries after
   a failed upload, which together are most real hits. This tier is only
   safe because 07 landed — before it, `rescan` called a never-uploaded
   chunk `Clean`.
2. **`PutMode::Create` — 1 RTT** for chunks not locally known. The
   backend is already built `.with_conditional_put(ETagMatch)`
   (`crates/cli/src/backend.rs`), and object_store 0.12.5 turns
   `PutMode::Create` into a single `If-None-Match: *` PUT, mapping the
   412 to `AlreadyExists` (`aws/mod.rs`); treat that as success.
   `create_fs` already uses this mode and `doctor`'s
   `Capabilities.create_if_absent` already probes support, so fall back
   to a plain PUT when it reports false. **Document the caveat:**
   object_store sends no `Expect: 100-continue`, so on a hit the body
   still crosses the wire. This buys the round trip and avoids the
   overwrite (which matters on versioned buckets), not the bandwidth.
3. **Adaptive remote HEAD**, kept only while the measured hit rate of
   probes that actually reached S3 justifies it, with hysteresis so it
   cannot flap per chunk. Reuse the EWMA shape `cli/src/sources.rs`
   already uses for source selection rather than inventing a second
   estimator. Surface the rate and the current decision in `status`.

Follow-up, explicitly **not** this phase: `layout::chunk_key`'s comment
notes the `chunks/<aa>/<bb>/` levels exist "for LIST parallelism", so a
cold node facing a high-dedup import could seed a local bloom of S3
contents with paged LISTs (1000 keys/request) and skip probing
altogether, reusing plan 06's bloom code. Note it in PROGRESS.md as a
lever, do not build it.

### 2b. Compression runs on the async runtime

`format::encode_object` at `crates/store-s3/src/store.rs:144` is a
synchronous zstd burn inside an `async fn`. Harmless while uploads are
serial; at 8–16 way concurrency it starves tokio workers and the whole
daemon (including the control API and the sync task) gets choppy. Move
it to `spawn_blocking`, exactly as `crates/cli/src/coop.rs:258` already
does for cache reads.

## Step 3 — Write-through / write-back

- **`through`** (today's behavior, stays the default): `close()` seals
  and uploads inline, then journals.
- **`back`**: `close()` seals into the cache as `Dirty`, records the
  pending rows, journals locally, and returns. The pool drains in
  parallel behind it.

The existing invariant — every chunk a segment references is in S3
before that segment ships — remains the only gate, and it is already
enforced by `upload_dirty_chunks` running before `run_sync_round` inside
`run_managed_sync_round`. So under write-back cross-node visibility lags
but never breaks: a remote node never sees a manifest whose chunks are
missing.

Controls:

- `--write-mode {through,back}` at mount.
- Dynamic switch over the control API (`Request::SetWriteMode` or an
  extension of the existing request enum) plus a CLI verb, because the
  useful pattern is "flip to back for a bulk copy, flip back after".
  Switching `back → through` must drain first and must not lose pending
  rows.
- `status` reports the mode, dirty bytes, and pending upload count. The
  scenarios assert on these.

Escapes, so the mode never silently weakens a promise the application
made explicitly:

- `fsync`, `fdatasync`, and `O_SYNC` force the through path for that
  operation.
- `--fsync-mode s3` always forces through — it is an explicit request
  for durability in the shared log, and satisfying it locally would be
  a lie.
- Note in the module docs *why* the plain-fsync case is arguable:
  local-only fsync would still satisfy POSIX (the data is durable on
  this node's disk and survives `kill -9` and reboot), and forcing
  through means databases and `rsync --fsync` get no benefit from
  write-back at all. This plan keeps the conservative choice — fsync
  means S3 — because the alternative silently converts a node-loss
  exposure into an acknowledged write. Record the trade-off; do not
  quietly pick the fast option.

Backlog is bounded by the dirty budget with a **throttle before
ENOSPC**, filling in the ladder DESIGN.md §9 describes ("evict clean →
throttle writes → ENOSPC") where the code currently jumps straight to
ENOSPC. Delay grows as dirty bytes approach the budget; ENOSPC only at
the hard limit.

Write-back is a generalization of a mechanism already in the tree, which
is the best evidence it fits: continuation epochs already skip eager
upload (`if !epoch_active { self.try_upload_dirty(..) }` in
`flush_inode`) and drain later via `set_skip_ship` +
`upload_dirty_chunks` at epoch close. Reuse that machinery instead of
building a parallel path.

## Trade-offs to document (in the module docs and PROGRESS.md)

- **close() latency:** through pays at least one S3 RTT per chunk; back
  pays local disk only.
- **Small-file rsync:** through is RTT-bound; back is disk-bound. This
  is the main win.
- **Writer-node durability:** both survive process crash and reboot
  after fsync. Back keeps data only on that node's disk until drained.
- **Cross-node visibility:** P2P push (`announce_segment` in
  `crates/cli/src/shipper.rs`) fires only *after* a segment is durable
  in S3, so it shortens the post-ship tail from a poll interval to a
  LAN RTT in both modes — it does not skip the upload gate. Through is
  therefore roughly "upload this close's chunks + ship + announce" (the
  sync interval is the no-P2P fallback); back adds the drain backlog in
  front of that same ship. Peer chunk serving also refuses `Dirty`
  (`get_servable`), so a peer cannot fetch the bytes early either.
- **Permanent node loss before drain:** through has the data in S3;
  back loses un-drained writes with the node. Same exposure class as an
  offline designation, and it must be documented as such rather than
  buried.
- **Authority transfer stays correct:** lease handoff, `leave`, and
  epoch close already force a drain — just slower when the backlog is
  large. `handoff.rs` declines rather than releasing when the flush
  fails, which is the behavior write-back depends on.
- **Peer cache:** write-back data is not served to peers until drained.
  Serving dirty chunks would be safe (blake3-verified) but is a
  separate, optional follow-up.
- **Cache pressure:** dirty chunks are never evicted, so a large backlog
  shrinks the effective read cache.
- **Write amplification:** back is *better* — repeated edits to one
  chunk before drain coalesce into a single PUT.
- **`--fsync-mode s3` barrier cost:** it currently barriers on a global
  `run_managed_sync_round`. Under write-back that is an expensive
  barrier, so scope it to the inode's chunks (or its partition) rather
  than the whole mount.

## Tests

### Unit — upload pool, mode, throttle

Keep the mode/throttle policy in a pure module so it tests with a fake
clock and no I/O.

- Pool honours its concurrency bound: a counting `ObjectStore` wrapper
  over `InMemory` records max in-flight and it never exceeds the
  configured value. A failing chunk retries and **keeps its pending
  row** rather than being dropped.
- Write-mode state machine: `through` vs `back`; `fsync`, `fdatasync`,
  `O_SYNC`, and `--fsync-mode s3` each force through for that
  operation; a dynamic `back → through` switch drains and loses nothing
  from the pending set.
- Throttle ladder: as dirty bytes approach the budget the injected delay
  grows, and ENOSPC appears only at the hard limit. Assert we never
  ENOSPC *before* throttling.
- Eager-seal trigger: a sequential append past a boundary seals chunk
  N-1 exactly once; repeated writes inside one chunk do not seal
  repeatedly; two writes to the same not-yet-drained chunk produce
  **one** PUT (the write-amplification claim, tested).
- Dedup ladder, with the counting wrapper recording requests by method:
  a locally-known-durable chunk issues **zero** requests; an unknown
  chunk under `PutMode::Create` issues **one** and treats
  `AlreadyExists` as success; a backend reporting
  `create_if_absent: false` falls back to plain PUT; the adaptive probe
  turns HEAD off after a run of misses and back on after hits, with
  hysteresis preventing per-chunk flapping.
- Compression is off the runtime: a pool-saturation test where N
  concurrent uploads of incompressible data must not stall an unrelated
  task spawned on the same runtime.

### In-process multi-node — `cli`

- Write under `back` on node A, drain, then assert node B applies the
  segment and fetches every chunk with no miss — "visibility lags but
  never breaks".
- Lease handoff with a large write-back backlog drains before releasing,
  so B never observes a manifest pointing at an absent chunk.
- Write-back combined with a continuation epoch neither double-drains
  nor loses pending rows (the two mechanisms share
  `set_skip_ship`/`upload_dirty_chunks`, so this is the collision test).

### Harness — `crates/harness/src/scenarios.rs`

- `writeback-latency`: 150 ms toxiproxy S3 latency, rsync-shaped
  workload (many small files, sequential closes), run once per mode.
  Assert `back` beats `through` by a conservative factor — **gate at 3x**
  to stay non-flaky even though the expected gap is far larger — and
  that both are model-verified identical. This is the scenario that
  proves the premise behind the phase.
- `writeback-bigfile`: a file ten times the cache budget under `back`
  with eager upload. Assert an RSS ceiling *and* a cache-usage ceiling
  while the write completes — the "bounded by S3, not local disk" exit
  criterion. Reuse `Client::rss_bytes()` from 07.
- `writeback-drain`: write under `back`, unmount cleanly, assert
  `status` showed pending uploads returning to zero before unmount
  finished, and a second node reads everything.
- `writeback-fsync`: under `back`, write and `fsync`, then `kill -9`.
  The fsynced bytes must survive the remount; an unsynced, unclosed
  tail may be missing but never corrupt.
- `writeback-backpressure`: tiny cache, `back`, S3 cut. Writes must
  throttle and ultimately ENOSPC rather than grow without bound or
  deadlock, and the mount must recover after heal — the bound the
  trade-off list promises.

### Benchmark

Extend `harness bench` (already reporting import, walk, and cold-read)
with a write-back import figure, and paste the through-vs-back numbers
into the report and `docs/PROGRESS.md`. This directly answers the
phase-1 note about serial-flush-bound import.
`10-p8b-hardening-packaging.md` adds perf regression gates, so these
become its baseline inputs.

## Out of scope

- Serving dirty chunks to peers (safe, but a separate change to
  `get_servable` and the coop path).
- P2P propagation of unshipped records — the gap that makes epoch-mode
  writes invisible until S3 returns. Real, but a visibility-plane
  design, not a write-path one. Record it in PROGRESS.md if not already
  filed; do not fold it in here.
- The LIST-seeded existence bloom (Step 2a follow-up).
- Slice overlays for random writes: format-reserved (DECISIONS.md
  ADR-11).

## Gates + report

Per CONVENTIONS.md, all six gates green, with pjdfstest at 8798/8798 and
an empty baseline — the write path is the risk area, so `fio-latency`,
`fio-blips`, and pjdfstest are the tripwires, not formalities. Add a
**Phase 5b** milestone table to `docs/PROGRESS.md`, update
`docs/TESTING.md` with the five new scenarios, and paste the
`writeback-latency` through-vs-back timings and the `writeback-bigfile`
ceilings into the report.
