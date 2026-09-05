# Plan 16 — Phase 10a: adaptive prefetch, streaming fetch, scan-ahead

Read `docs/history/v1/plans/CONVENTIONS.md` first (ground rules, gates,
build commands — they all still apply; the plans directory moved, the
rules did not). Spec: `docs/explanation/DESIGN.md` §7 ("Prefetcher
(after mountpoint-s3): per-handle sequential detection, adaptive
readahead … reset on seek"). The current implementation falls short of
that spec; this plan implements it properly and extends it. Do not
edit DESIGN.md.

## Problem (measured)

Cold sequential read of a large file from an EU client against a
us-west-2 bucket (~180 ms probe RTT, ~400 ms observed S3 TTFB):

```
dd if=/tmp/constellation/bigfile of=/dev/null bs=1M
121634816 bytes (122 MB) copied, 61.6 s, 2.0 MB/s
```

Per-source panel during the run: S3 goodput EWMA 3.5 Mbit/s
(~0.44 MB/s per stream). The prefetcher (`crates/cli/src/prefetch.rs`)
keeps a **fixed** `DEPTH = 8` chunks in flight. 8 × 0.44 MB/s ≈ the
observed 2 MB/s: each 4 MiB GET rides one cold-ish TCP/TLS stream that
never leaves slow start, and the parallelism is pinned at 8 no matter
how far the reader outruns it. The pipe (BDP ≈ 100 Mbit/s × 0.4 s ≈
5 MB *per healthy stream set*) is never filled.

Second workload, same mount:

```
tar cvpf /tmp/test.tar /tmp/constellation      # or rsync from the mount
```

tar/rsync stat and open files **one at a time, in readdir order**.
Every small file is one cold chunk = one ~0.5–1 s S3 round trip,
serialized. The sequential-within-a-file prefetcher never has anything
to do (most `/usr/bin` files fit in a single 4 MiB chunk); throughput
degenerates to ~1–2 files/s regardless of bandwidth. All metadata
(readdir, manifests, chunk hashes) is **local** (redb) — only chunk
bodies need the network — so the upcoming read set is knowable for
free. Nothing exploits that today.

Third defect, orthogonal: every fetch buffers the whole chunk in RAM
(`store.get_chunk` → `res.bytes().await` → `Vec<u8>`, then a second
copy for decode). Raising parallelism without fixing this turns a
256 MiB read window into ≥ 512 MiB of transient RSS. Inflight data
must stream to disk in small pieces, as the write path already does
(plan 05a bounded-RAM staging; xs3lerator does the same on the proxy
side).

## What the reference implementations do (read them, they are local)

**mountpoint-s3** (`/tmp/mountpoint-s3/mountpoint-s3-fs/src/prefetch.rs`
and `prefetch/backpressure_controller.rs`):

- One logical stream per file handle with a **read window** (bytes the
  producer may fetch beyond the reader's cursor).
- Window starts small (~1.1 MiB), **doubles on `PartQueueStall`** —
  i.e. the *reader had to wait for the network*. That single signal is
  what makes it adapt to any latency/bandwidth combination without
  modeling either: stalls keep doubling the window until stalls stop
  or the cap is hit. Cap: 2 GiB (or derived from free memory), knob
  `sequential_prefetch_multiplier = 2`, `min = read_part_size`.
- Window **scales down under memory pressure** (failed reservation
  against a memory pool), never below min.
- Seeks: small backward seeks (≤ 1 MiB) replay from a retained window;
  forward seeks within 16 MiB wait for inflight data instead of
  abandoning the stream; anything else resets the stream to the
  initial window.

**ZFS zfetch** (`/tmp/zfs/module/zfs/dmu_zfetch.c`):

- Multiple concurrent **streams per file**; a stream is matched if the
  access lands within `zfetch_max_reorder` (16 MB) of its position —
  tolerates the out-of-order reads the kernel readahead generates.
- Prefetch **distance** starts at the demand size, doubles per hit up
  to `zfetch_min_distance` (4 MB), then grows by **+1/8 only when
  needed** (`zs_more`: the reader reached un-prefetched data — again a
  stall signal) up to `zfetch_max_distance` (64 MB).
- Distance growth is suppressed when prefetch already holds too much
  of ARC (global memory guard).
- Idle streams are reaped after `zfetch_min_sec_reap` (1 s eligible,
  2 s forced) so dead cursors do not pin state.

Common core, which is what we adopt: **grow the window on
demand-read stalls, shrink/reset on seeks and memory pressure, cap it,
tolerate reordering, and reap idle streams.** Neither system tries to
estimate RTT or bandwidth explicitly for this purpose — the stall
signal subsumes both. We keep our source selector (RTT/goodput EWMA,
`crates/cli/src/sources.rs`) for *where* to fetch; the window decides
*how much* to fetch.

## Current-code map (all paths relative to repo root)

- `crates/cli/src/prefetch.rs` — the whole current prefetcher (105
  lines). Fixed `DEPTH = 8`; sequential test is `offset == 0 ||
  cursor == offset` (any reorder or overlapping read kills it);
  spawns unbounded `rt.spawn` fetches; dedup via `inflight:
  Mutex<HashSet<ChunkHash>>`; `is_inflight` polled by the read path.
- `crates/cli/src/fusefs_ops.rs` `do_read` (~line 1155) — calls
  `prefetch.on_read(...)` then fetches chunk slices synchronously.
- `crates/cli/src/fusefs.rs` `fetch_chunk` (~line 1086) — cache →
  spin-wait on `is_inflight` (2 ms sleeps) → `coop.fetch` or
  `store.get_chunk`, then `cache.insert`. **This is where demand
  stalls are visible** — the scale-up signal lives here.
- `crates/cli/src/coop.rs` — `fetch`/`fetch_uncached` (~line 356):
  per-chunk source selection + hedging; `fetch_from` (~line 492)
  returns a whole `Vec<u8>` from S3 (`get_chunk_timed`) or a peer;
  `settle` feeds the selector (TTFB, goodput, errors).
- `crates/store-s3/src/store.rs` `get_chunk_timed` (~line 330) —
  `store.get(&key)` → `res.bytes().await` (whole object in RAM) →
  optional e2e decrypt → `format::decode_object` (second buffer) →
  blake3 verify.
- `crates/store-s3/src/format.rs` — 16-byte header (magic, version,
  codec id, level, uncompressed len), payload raw or zstd.
- `crates/store-s3/src/e2e.rs` — XChaCha20-Poly1305 whole-object AEAD
  (not streamable without chunked AEAD; see step 3 fallback).
- `crates/fs-core/src/cache.rs` — `DiskCache::insert(&[u8])`
  (reserve-before-accept, atomic tmp+rename), `get`, `contains`.
- `crates/harness/src/scenarios.rs` `readahead` (~line 2536) — 32 × 1
  MiB chunks under 60 ms injected latency must beat serial/2.
- FUSE session: `crates/cli/src/main.rs` ~1994–2020, multi-threaded
  (`fuse_config.n_threads`), so reads can arrive concurrently and
  slightly out of order even for one sequential reader.
- `TODO.md` lines 3–5 name this work.

## Deliverables

1. **Adaptive per-stream window prefetcher** replacing fixed `DEPTH`.
2. **Streaming chunk fetch**: S3 GET bodies stream to the disk cache
   in ≤ 256 KiB pieces; bounded RAM regardless of window size.
3. **Scan-ahead ("directory readahead") for tar/rsync-like walks**:
   when a workload opens+reads files of one directory in readdir
   order, prefetch the *next files'* chunks.
4. Config knobs + docs + `constellation status`/web-UI visibility.
5. Tests: unit, harness scenarios (existing `readahead` must still
   pass and get a tighter sibling; new scan-ahead scenario).

Non-goals: peer-transport streaming (p2p messages already arrive as
whole frames; capped by concurrency instead), kernel `max_readahead`
tuning, write-path changes, range-GET sub-chunk fetches.

## Step 1 — adaptive window prefetcher (`crates/cli/src/prefetch.rs`)

Rewrite the module around per-inode **streams** (zfetch) driving a
byte-denominated **window** (mountpoint):

```rust
struct Stream {
    /// Next expected sequential offset (reader cursor high-water).
    cursor: u64,
    /// Bytes beyond `cursor` we are allowed to have in flight/cached.
    window: u64,          // clamped to [min_window, max_window]
    last_hit: Instant,    // for reaping
}
```

State: `streams: Mutex<HashMap<Ino, Stream>>` (one stream per inode is
enough — FUSE gives us no handle identity in `do_read` today and a
single sequential reader is the case that matters; document this and
leave multi-stream-per-inode as a follow-up), plus the existing
`inflight: HashSet<ChunkHash>` dedup, plus a **global fetch scheduler**
(step 1c).

**1a. Sequential detection with reorder tolerance.** Replace the
equality test. An access at `offset` matches a stream if
`offset + len > cursor && offset <= cursor + REORDER_WINDOW` where
`REORDER_WINDOW = 16 MiB` (zfetch's `zfetch_max_reorder`). On match:
`cursor = max(cursor, offset + len)`, `last_hit = now`. On mismatch:
**reset** the stream (`window = min_window`, new cursor) — that is the
"reset on seek" the spec names. `offset == 0` still starts a stream.
The FUSE session is multi-threaded; overlapping in-flight reads of one
sequential run must not reset the window (the tolerance above is what
prevents it — add a unit test that interleaves `0,128K` / `256K` /
`128K` and asserts the stream survives).

**1b. Window growth on stall, not on hit.** The window only grows when
the *demand* path blocks on the network — mountpoint's
`PartQueueStall`, zfetch's `zs_more`:

- New method `Prefetcher::note_stall(ino)`. Called from
  `fusefs.rs::fetch_chunk` whenever it (a) enters the
  `is_inflight` wait loop, or (b) has to fetch the chunk itself after
  a cache miss — but only if the inode currently has a live stream
  (random reads must not inflate windows).
- On stall: `window = min(window * 2, max_window)`.
- No shrink-on-quiet needed (unlike mountpoint we do not hold the
  window in RAM; it lands in the disk cache). Reset happens on seek
  (1a) and reap (1d).

Defaults (env-overridable, step 4): `min_window` = 2 chunks
(8 MiB at the 4 MiB default — zfetch's doubling ramp from 1 chunk
would cost several stalled round trips on a 400 ms path for no
benefit), `max_window` = 256 MiB, and `max_window` is additionally
clamped to ¼ of the disk-cache budget so prefetch can never churn the
cache it feeds (zfetch's ARC guard, translated).

**1c. Scheduling: adaptive bounded concurrency, fairness, demand
priority.** Today every prefetched chunk is a naked `rt.spawn` — window
growth would stampede. Add one global scheduler owned by the
`Prefetcher`:

- Reuse `constellation_upload_concurrency::{AdaptiveConcurrency,
  ConcurrencyGate}` for one mount-wide S3 background-fetch gate. Start
  at 8 (the existing readahead depth and the latency-efficient floor in
  the local mountpoint-S3 benchmark) and slow-start toward
  `prefetch_max_concurrency` (default 128).
  Feed it actual S3-leg bytes and service time only: a peer completion,
  cache hit, losing hedge, or logical bytes not transferred must not
  inflate measured goodput. Errors/timeouts are congestion signals.
  This is intentionally the already benchmarked aggregate-goodput
  controller, not a latency/PID controller: filling BDP raises latency,
  and live upload benchmarks showed the useful knee varying from 16
  (VPN-limited WAN) through 64 (same-region EC2) to 128 (unconstrained
  WAN). `CONSTELLATION_PREFETCH_CONCURRENCY` pins a fixed target for
  debugging/oracle runs; otherwise
  `CONSTELLATION_PREFETCH_MAX_CONCURRENCY` is only the ceiling.
- Background fetches acquire the adaptive gate; **demand fetches never
  touch it** (they stay on the caller's path, so a reader is never
  queued behind its own readahead). Demand concurrency is therefore
  additionally bounded by the FUSE worker count, not by this gate.
- Do not spawn one task per byte-window entry and leave them queued on
  the gate. Keep per-stream ordered want queues in a round-robin
  dispatcher, admitting at most
  `max(2, gate_target / live_ready_streams)` per stream before rotating.
  Scan-ahead is another producer in the same rotation. This prevents
  one 256 MiB big-file window from occupying every permit or placing
  hundreds of dormant Tokio tasks ahead of another reader.
- On every `on_read` that matches a stream, compute the want-set:
  chunk indices from the first gap after `cursor` up to
  `cursor + window`, skip `cache.contains` and `inflight` members,
  enqueue the rest (dispatcher task: acquire permit → fetch → insert →
  feed the S3 controller when applicable → drop inflight marker and
  wake the dispatcher). Same dedup discipline as now, and the existing
  `is_inflight`/wait handshake with `fetch_chunk` stays.
- Keep fetches **chunk-index-ordered** when spawning so the chunk the
  reader needs next tends to complete first.

**1d. Stream reaping.** In `on_read` (cheap, amortized): drop streams
with `last_hit` older than 2 s (zfetch `zfetch_min_sec_reap`), and
keep `forget(ino)` on last close as today. Bound the map (e.g. 512
streams, evict oldest) — tar walks touch hundreds of thousands of
inodes.

**1e. Feed the pin path.** `DESIGN.md` §7: "Pins reuse the prefetcher
at full parallelism." Wherever pin fetching currently issues its own
gets (grep `pin` in `crates/cli`), leave behavior unchanged in this
plan but route any direct `store.get_chunk` calls it makes through
the streaming fetch of step 2 if the diff is trivial; otherwise note
it in the report as follow-up.

## Step 2 — streaming fetch: bounded RAM end to end

**2a. `DiskCache` file-based insert** (`crates/fs-core/src/cache.rs`):

```rust
/// Begin writing a chunk body straight to cache-owned storage.
/// Returns a writer over `<root>/<xx>/<hash>.tmp-<nonce>`.
pub fn begin_spill(&self) -> Result<SpillFile, CoreError>;
/// Rename a fully written+verified spill into place, with the same
/// reserve-before-accept accounting as `insert` (size from stat).
pub fn commit_spill(&self, hash: &ChunkHash, spill: SpillFile,
                    state: ChunkState) -> Result<(), CoreError>;
```

`SpillFile` deletes its tmp on drop (abort-safe). Reuse the eviction
planning from `insert`; the only new logic is size-by-stat and rename
instead of write. Keep `insert(&[u8])` for all existing callers.

**2b. Streaming decode** (`crates/store-s3/src/format.rs` + new code in
`store.rs`):

```rust
pub async fn get_chunk_to_writer(
    &self, hash: &ChunkHash, out: &mut (impl std::io::Write + Send),
) -> Result<(u64 /*bytes*/, Duration /*ttfb*/, Duration /*total*/), StoreError>
```

- `store.get(&key)` → `into_stream()`; consume the byte stream.
- Parse the 16-byte header from the first piece(s).
- Codec `Raw`: pipe payload bytes through a running `blake3::Hasher`
  into `out`.
- Codec `Zstd`: wrap `out` in `zstd::stream::write::Decoder` (the
  zstd crate supports incremental write-side decode), hash the
  *decompressed* bytes (a small `Write` adapter that tees into the
  hasher — put it in `format.rs` with unit tests).
- Verify the final hash against `hash`; error = caller aborts spill.
- **E2E mounts** (`self.e2e.is_some()`): XChaCha20-Poly1305 remains
  whole-object AEAD and must not expose unauthenticated plaintext.
  Stream ciphertext into a second cache-owned tempfile, then mmap/read
  that completed file for one-shot authenticated decrypt and feed the
  resulting encoded plaintext through the same streaming decoder into
  the destination spill. Serialize one-shot decrypts with a one-permit
  e2e decrypt gate so simultaneous GET completions cannot recreate
  `concurrency × chunk_size` heap pressure. The unavoidable whole
  plaintext buffer then exists only during decrypt/decode, not for the
  network transfer's lifetime. Delete both spills on authentication or
  decode failure. Do not invent a chunked-AEAD format in this plan.
- Peak RAM per in-flight fetch becomes O(stream piece) ≈ 256 KiB;
  excluding the serialized e2e one-shot buffer, versus
  ≥ 2 × chunk_size × depth today.

**2c. Wire it into the fetch paths** (`crates/cli/src/coop.rs`):

- `fetch_from(SourceId::S3, …)`: replace `get_chunk_timed` with
  `begin_spill` → `get_chunk_to_writer` → `commit_spill(Clean)`,
  return a new `FetchResult::Spilled { bytes, ttfb_ms, total_ms }`
  variant. `settle` records selector stats exactly as today (it has
  bytes + both timings — goodput learning is unchanged) and, for the
  demand path, re-reads the chunk via `cache.get` (page-cache-warm,
  one copy — acceptable; note it). Peer arm unchanged (whole frame
  arrives in RAM anyway; concurrency cap bounds it).
- No-coop mounts: `prefetch.rs` and `fusefs.rs::fetch_chunk` call the
  same spill sequence directly instead of `get_chunk` + `insert`.
- Keep hedging semantics identical: a spill that loses the race is
  simply dropped (tmp cleanup on drop makes this safe); the
  `commit_spill` of the winner is idempotent w.r.t. an
  already-present entry (return Ok, drop tmp — mirror `insert`'s
  existing same-hash early-return).

## Step 3 — scan-ahead for tar/rsync-style walks

The workload: stat + open + read every file of a directory in readdir
order, one at a time. Within-file readahead cannot help (files are
mostly single-chunk); the win is fetching the **next files'** chunks
while the current file is being read. All the inputs are local:
`meta.readdir(dir)` gives the entry order, `load_manifest(ino)` +
`chunk_list` give each file's chunk hashes without touching S3
(manifest spill blobs are chunks too — fetch them through the same
pipeline, they are small and few).

**3a. Detection.** New module `crates/cli/src/scan.rs`, owned by
`ConstellationFs` next to the prefetcher. Per-directory state
(`HashMap<Ino /*dir*/, Scan>`, LRU-bounded, reap like 1d):

```rust
struct Scan {
    /// readdir-ordered file inodes of this directory (files only).
    order: Vec<Ino>,
    /// Index of the last opened-for-read member.
    pos: usize,
    hits: u32,            // consecutive in-order (or near-order) opens
    window: u64,          // bytes of look-ahead, adaptive like step 1
    last_hit: Instant,
}
```

- Hook `open` (readonly opens) and `lookup`-then-`read` in
  `fusefs_ops.rs`: on first read of an inode, tell the scanner
  `scan.note_read(parent_dir, ino)`. Parent dir is available from the
  metadata layer (`meta` has the tree; if a cheap parent lookup is
  missing, capture the dir ino in `readdir`/`lookup` and remember the
  child→dir edge in the scanner's own bounded map).
- The scanner lazily materializes `order` from `meta.readdir` on the
  second in-order hit (not on every readdir — directories are listed
  for many reasons).
- **Trigger**: `hits >= 3` consecutive opens that each land within a
  small forward distance (≤ 8 entries) of the previous one. Skips are
  normal (rsync skips up-to-date files, tar skips sockets), so
  in-order means monotonically increasing position, not `pos + 1`.

**3b. Action.** While a scan is live, keep the union of the next
files' chunks inside the byte window: walk `order[pos+1..]`, for each
file load manifest → chunk hashes (skip cached/inflight), and hand
them to **the same scheduler** from step 1c (same semaphore, same
dedup, same streaming fetch), until `window` bytes are enqueued.
Window: start 16 MiB, double via the same stall signal — `fetch_chunk`
already calls `note_stall(ino)`; route it to the scanner too when the
inode belongs to a live scan (a stall on a scanned file means the walk
is outrunning the look-ahead), cap 256 MiB shared with the same
cache-budget clamp. Recompute lazily on each `note_read` (cheap:
resume from a remembered index, not from `pos`).

- Descent into subdirectories: when the walker opens a *directory*
  member (readdir of a child dir of a live scan), extend the scan
  depth-first the way tar/rsync walk — treat the child dir as the new
  active scan and remember the parent to resume when the child's
  order is exhausted. Keep it simple: a stack of (dir, pos), max
  depth 16; wrong guesses only waste already-capped window bytes.
- Eviction safety: scan-ahead inserts are `Clean` — evictable, and the
  budget clamp keeps them a minority of the cache.
- Interlock with step 1: a big file inside the walk gets its own
  sequential stream the moment it is read; scan-ahead only pulls each
  file's **first** `min(file_len, 2 chunks)` bytes so one huge file
  cannot monopolize the scan window — the per-file stream takes over
  from there.

**3c. Metadata prefetch is explicitly *not* needed** — readdir,
getattr, manifests are all redb-local. If profiling during validation
shows `lookup`/`getattr` round trips dominating tar on a warm data
cache, record it in the report; do not chase it in this plan.

## Step 4 — configuration, observability, docs

New env knobs (parse in the owning module, mirror
`CONSTELLATION_PART_AUTOSPLIT`'s style; document every one in
`docs/reference/configuration.md`):

| var | default | meaning |
|---|---|---|
| `CONSTELLATION_PREFETCH_MIN_BYTES` | `8388608` | initial/floor window per stream |
| `CONSTELLATION_PREFETCH_MAX_BYTES` | `268435456` | window cap (also clamped to cache_budget/4) |
| `CONSTELLATION_PREFETCH_CONCURRENCY` | unset | pin global background-fetch concurrency; otherwise adaptive |
| `CONSTELLATION_PREFETCH_MAX_CONCURRENCY` | `128` | adaptive background-fetch ceiling |
| `CONSTELLATION_SCAN_AHEAD` | `on` | scan-ahead master switch |

Counters on the existing status path (`Coop::report` /
`constellation status` / web UI, wherever the peers panel gets its
numbers): per-mount `prefetch_inflight`, `prefetch_window_bytes`
(max over live streams), `prefetch_stalls`, `prefetch_gate_target`,
`scan_ahead_files`, `scan_ahead_bytes`. The dd-style investigation
above was only possible because the panel existed — extend it.

Docs: update `docs/reference/configuration.md`,
`docs/explanation/DESIGN.md` is **not** edited; add a short
`docs/reference/features/prefetch.md` describing the algorithm and
citing mountpoint-s3/zfetch as the models, and delete TODO.md lines
3–5 (the `- /// How many chunks…` bullet).

## Step 5 — tests and gates

Unit (colocated `#[cfg(test)]`, InMemory object store like
`shipper.rs` tests):

- prefetch: reorder within 16 MiB keeps the stream; a seek resets the
  window to min; stalls double it up to the cap; cap respects the
  cache-budget clamp; idle streams are reaped; inflight dedup holds
  under concurrent `on_read`s.
- scheduler: demand fetch is never blocked by an exhausted gate;
  spawn order is index-ordered; multiple ready streams are serviced
  fairly; adaptive target slow-starts, rejects a no-gain probe, backs
  off on errors, and a fixed override disables adaptation; peer/cache
  completions do not feed the S3 controller.
- cache: `begin_spill`/`commit_spill` — accounting parity with
  `insert`, tmp cleanup on drop/abort, idempotent double-commit,
  eviction planning applies.
- format: streaming decode == buffered decode for raw and zstd
  (round-trip property test over sizes 0, 1, header-boundary,
  multi-piece), hash mismatch rejects, truncated stream rejects.
- scan: 3-in-order-opens trigger; skips tolerated; big-file handoff
  fetches only the first 2 chunks; subdirectory descent; `off` knob.

Harness (`crates/harness/src/scenarios.rs`):

- `readahead` (existing) must pass unchanged — it is the regression
  tripwire for step 1.
- New `readahead-adaptive`: like `readahead` but 200 ms injected
  latency, 128 chunks; assert elapsed < serial/6 — a fixed depth-8
  pipeline cannot pass, doubling-to-cap can. Choose the bound after
  measuring locally; leave ×2 headroom for CI, and say in a comment
  what depth-8 would score.
- New `scan-ahead`: write ~200 files × 256 KiB in one directory,
  remount cold, 60 ms latency, read files in readdir order one by
  one; assert elapsed < serial/4 and content hashes match.
- Full sweep for the gates in CONVENTIONS.md: fmt, clippy `-D
  warnings`, `cargo test --workspace`, `tests/smoke.sh`,
  `tests/integration.sh`, then `harness run readahead
  readahead-adaptive scan-ahead coop-cache-hit fio-latency
  disjoint-write-4 forwarded-mutations` (the last two guard against
  regressions from touching coop/fetch paths).

Live validation (manual, coordinator): EU machine against the USW2
bucket — `dd if=<mount>/bigfile of=/dev/null bs=1M` should move from
2 MB/s toward link speed as the window opens (expect ≥ 10× on that
path), and `tar cf /dev/null <mount>/usr` should show scan-ahead
counters climbing with wall-clock per file well under one RTT.

## Risks / notes for the implementer

- **Do not regress random-read latency**: every window/scan action
  must be gated on detection; a random reader must see exactly one
  chunk fetch per miss, no amplification. Add a unit test asserting
  zero prefetch spawns for a random access pattern.
- `fetch_chunk`'s 2 ms `is_inflight` spin is unchanged in scope, but
  `note_stall` must fire *before* entering the wait so the window
  reacts on the first stalled read, not the second.
- Hedged S3+peer fetches of the same chunk with spills: two writers to
  two distinct tmp files — safe; whichever commits first wins,
  the loser's commit is the idempotent no-op case (2c).
- The selector's goodput EWMA is per-source across all parallel
  streams; as adaptive parallelism rises the per-stream number will drop
  while aggregate rises. That is fine for ranking (relative), but
  mention it in `prefetch.md` so nobody "fixes" the panel number.
- Upload and download controllers independently optimize opposite
  directions. Full-duplex paths tolerate that; a half-duplex access
  link may not. A shared bidirectional NIC budget needs separate live
  evidence and is explicitly follow-up work, not a reason to pin
  either controller here.
- Watch `staging`/writeback interplay: prefetch inserts are `Clean`
  and must never evict `Dirty` (existing `plan_eviction` already
  guarantees this — keep it that way in `commit_spill`).
- macOS: no change needed, but `SpillFile` uses rename-into-place —
  keep it on the same filesystem as the cache root (it is, by
  construction).


