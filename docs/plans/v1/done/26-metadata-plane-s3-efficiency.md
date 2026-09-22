# Plan 26 — Metadata plane: checkpoint cadence, log transport, sticky leases

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §4 (metadata plane, log, checkpoints,
partitions, leases), §7 (existence hints are not spec'd; plan 14), §14
(bucket GC). Code context, in the order you will touch it:

- `crates/cli/src/gc.rs` — `metadata_candidates` (log-retention floor).
- `crates/cli/src/shipper.rs` — `Shipper` (tail/ship loop, checkpoint
  trigger, `bootstrap`).
- `crates/store-s3/src/log.rs` — `LogStore` (segments, `CheckpointRef`,
  `CheckpointVector`, `put_checkpoint_with_vector`, `get_latest_checkpoint`).
- `crates/cli/src/node_runtime.rs` — the sync task (`interval_ms` poll,
  `SyncRequest` handling, `run_managed_sync_round` call site).
- `crates/cli/src/lease.rs` + `crates/store-s3/src/lease.rs` —
  `LeaseKeeper`, `Lease`, idle release.
- `crates/cli/src/existence.rs` + `crates/cli/src/main.rs` (upload path
  around `existence.contains`) — mount-time S3 existence LIST.

This plan makes **no bucket-format change** beyond two additive fields.
Plan 27 (`27-merkle-packed-checkpoints.md`) replaces the checkpoint
*format*; it depends on this plan and nothing here should anticipate it.

**Compatibility policy (applies to this plan and plan 27):** the project
is pre-release. Do **not** keep read paths for old objects, do not bump
`v`/format versions, do not add `legacy` env switches. When a field or
object shape changes, change it and recreate test buckets. Where this
plan says "add a field", it means a plain required field unless the
`serde(default)` is needed for a *different* reason stated there.

Every quantitative claim below was measured on 2026-09-10 against a
standard AWS S3 bucket in us-west-2 (from Hungary over 1 Gbit/s
residential fibre, and from an EC2 instance in the same region) and
against an OVH 3-AZ bucket in Milan (from Hungary). The full tables are
in the Appendix; step text cites the numbers it relies on.

## Problem

### What a checkpoint is, and what depends on it

A checkpoint is a whole-DB `VACUUM INTO` copy of the SQLite replica
(`meta::snapshot`, `crates/meta/src/replay.rs`), stripped of node-private
tables and kv keys, written as `checkpoints/p0/<seq>.zst` + a `LATEST`
pointer + `checkpoints/VECTOR.json` (per-partition applied seq). It is the
license to truncate the log and the only bounded way to build a fresh
replica (`shipper::bootstrap`). Invariants other code relies on:

- `LATEST` must always name a present object (write order snapshot →
  LATEST → VECTOR).
- `VECTOR.json` must never over-claim: bootstrap replays strictly after
  `vector.applied[part]`.
- Log-retention GC must never delete a segment above a partition's
  vector entry.
- Replay is contiguous (`seq == applied + 1`) and idempotent
  (max-merge), so a conservative vector is safe.
- GC keeps the newest 2 checkpoints to cover a bootstrap that read an
  older `LATEST` a moment before it moved.
- Checkpoint PUTs are plain (non-CAS) to fixed keys; the partition
  lease holder is the only writer, so there is no clobber race.

### Six things that are wrong today

1. **Cadence is count-fixed.** `CHECKPOINT_EVERY = 32` shipped segments
   triggers a full-DB copy regardless of DB size. Measured on a 1.85M-file
   rsync: 641 checkpoints, **54.55 GiB**, protecting a 0.17 GiB log; 639
   were dead on arrival. GC is a daily backstop, not a bound.

2. **Idle polling is the dominant steady-state S3 cost.** The sync task
   polls every 500 ms with no idle backoff
   (`node_runtime.rs` `interval_ms`; the `poll.reset` only defers it when
   nudges arrive) and every round issues one `LIST` per partition
   (`shipper.rs` `tail_all`). Ten idle nodes ≈ 20 LIST/s ≈ 1.7M
   LIST/day. LIST is the most expensive request class (12.5× a GET on
   AWS) and — measured — a GET-404 is **not slower** than an idle LIST
   (HU→AWS 177 vs 175 ms; OVH 34 vs 34 ms).

3. **The lease holder tails its own partition.** `sync_all_inner` runs
   `tail_all` before every `ship_all`. If we hold the lease nobody else
   can append, so that LIST returns nothing. Measured: dropping it takes a
   far writer from **2.6 → 5.1 seg/s** (HU→AWS), 24.5 → 33 (same region),
   5.4 → 6.2 (OVH).

4. **Checkpoint download is a single TCP stream.** `get_latest_checkpoint`
   does `store.get(..).bytes()`. Measured for 64 MiB: HU→AWS **23.5 s vs
   6.3 s** with parallel ranged GETs; OVH 6.6 vs 2.7 s; same-region 1.3
   vs 0.3 s. A 400 MiB image at 12M files is ~2.5 min vs ~40 s from
   Europe. Same asymmetry on the upload side.

5. **Cold-write latency is 3 S3 RTTs on the FUSE path.** After 30 s idle
   the lease is released (`lease.rs` `DEFAULT_IDLE_RELEASE_MS`). The next
   mutation blocks in `require_lease_for` on `SyncRequest::Acquire`, which
   is GET lease → (LIST + GETs) → CAS PUT, ~0.6–1 s from Europe, queued
   behind whatever sync round is in progress.

6. **Latent data-loss bug in multi-partition GC.** `gc.rs`
   `metadata_candidates` computes `floor = LATEST.seq - retention` where
   `LATEST.seq` is `covered = max(next_seq-1)` over *all* partitions
   (`shipper.rs` `checkpoint()`), then applies that floor to
   `list("log")` — every partition's segments. A child partition at seq
   40 while p0 is at 100,000 loses segments 31..40 even though
   `VECTOR.json` says the snapshot only has it to 30. Autosplit is off by
   default, so it is latent, not observed.

Also observed but handled elsewhere: mount-time existence LIST
(`existence.rs`) scans every chunk object (59.5k → 23 s; O(hours) at
10M) although the replica's `chunk_ref` table already holds the
referenced-hash set — Step 8.

## Goal

After this plan, on every workload and from any distance to S3:

- checkpoint write traffic ≤ ~`1/ratio` bytes per byte of shipped log,
  footprint ≤ newest-2, with no dependency on the GC timer;
- an idle node issues no S3 requests beyond lease renewal and heartbeat;
- a lease holder's ship loop is PUT-only; a follower's steady-state poll
  is a GET-404 with exponential idle backoff, gossip resetting it;
- checkpoint download/upload use parallel ranged I/O;
- a node that keeps writing never re-acquires its lease; a node that
  wants a held lease can signal that through S3 alone;
- no mount-time chunk LIST;
- per-partition log retention; the bug in finding 6 is gone.

Non-goals (all plan 27): changing the checkpoint *format*, partial
replicas, packing, changing who may publish a checkpoint.

## Settled decisions

- **Cadence is bytes-of-shipped-log ÷ last-snapshot-bytes** with the
  `CHECKPOINT_EVERY` count floor kept as a *minimum* and the opt-in time
  floor untouched. Ratio default 1.0.
- **Superseded checkpoints are pruned inline** after `LATEST` moves,
  newest-2 kept; GC stays the backstop.
- **Holders do not tail held partitions**, except (a) the takeover
  witness `tail_to_head`/`tail_part_to_head`, (b) after a
  `put_segment` → `AlreadyExists` (crash recovery of our own unacked
  segment), (c) after a deposition.
- **Tailer probes with GET-next (pipelined k)**; LIST is used only in
  catch-up mode (all k probes hit). Measured: catch-up over 64 segments
  is 1.3–1.8× faster with k=16 than LIST-per-round on all three paths,
  and idle cost drops from LIST to GET class.
- **Ship loop does not pipeline PUTs.** The measured 5–10× from
  pipelining is real, but a lease holder gets the same records/s by
  letting the journal accumulate during the in-flight PUT and shipping
  one larger segment (`SEGMENT_BATCH` = 10k records ≈ 50k rec/s at
  200 ms RTT, far above FUSE rates). Pipelining adds a gap-on-failure
  hazard for no throughput we need. Add a byte cap instead.
- **Leases are sticky.** Idle release happens only when a `wanted_by`
  requester is recorded in the lease object or a P2P handoff asks.
- **Existence hint comes from `chunk_ref`**, not from a bucket LIST.
- **Log retention is per partition against `VECTOR.json`.**

## Step 0 — Fix the log-retention floor (bug, do first)

`crates/cli/src/gc.rs` `metadata_candidates`:

1. Read `checkpoints/VECTOR.json` (reuse `LogStore::get_checkpoint_vector`).
   If there is a `LATEST` but no vector, that is a corrupt bucket: return
   an error and mark nothing (no fallback — see the compatibility policy).
2. For each object under `log/`, parse `<part>` from the path
   (`location.parts()[1]`) and `<seq>` from the filename. Mark
   `log-retention` **only if** `vector.applied.get(part)` is `Some(v)` and
   `seq < v.saturating_sub(retention_segments)`. A partition absent from
   the vector is never pruned (the snapshot does not contain it yet).
3. Evidence JSON gains `"partition"` and `"vector_applied"`.

Unit test (gc.rs test module, `InMemory` store): two partitions,
`VECTOR.json = {p0: 1000, p1: 30}`, `LATEST.seq = 1000`, retention 128.
Segments `p1/0..40` present. Assert: p0 segments `< 872` marked, **no**
p1 segment marked (30 − 128 saturates to 0); p1 segments `1..40` untouched.
Add a second case with `p1: 300` → p1 `< 172` marked.

## Step 1 — Proportional checkpoint cadence

`crates/cli/src/shipper.rs`:

- Add fields `bytes_since_ckpt: u64`, `last_ckpt_bytes: u64` to `Shipper`
  (init 0 in `attach_with_mode`).
- In `ship_part`, after `put_segment` succeeds, `self.bytes_since_ckpt +=
  payload.len() as u64` next to `shipped_since_ckpt += 1`. (`payload` is
  the *uncompressed* postcard envelope — consistent with `snap.len()`
  being uncompressed.)
- In `checkpoint()`, after a successful `put_checkpoint_with_vector`:
  `self.last_ckpt_bytes = snap.len() as u64; self.bytes_since_ckpt = 0;`.
- Replace the free fn `checkpoint_is_due(shipped, since_last, min)` with

  ```rust
  fn checkpoint_is_due(
      shipped_since_ckpt: u64,
      bytes_since_ckpt: u64,
      last_ckpt_bytes: u64,
      ratio: f64,
      since_last_ckpt: Option<Duration>,
      min_interval: Duration,
  ) -> bool {
      if shipped_since_ckpt < CHECKPOINT_EVERY { return false; }
      let bytes_ok = last_ckpt_bytes == 0
          || (bytes_since_ckpt as f64) >= ratio * (last_ckpt_bytes as f64);
      if !bytes_ok { return false; }
      match since_last_ckpt { None => true, Some(e) => e >= min_interval }
  }
  ```

  `ratio` comes from `checkpoint_ratio()`: env
  `CONSTELLATION_CHECKPOINT_RATIO`, `f64`, default `1.0`, values `<= 0`
  are rejected (fall back to default with a `warn!`). Read once at attach
  into a `ckpt_ratio: f64` field (same reasoning as `autosplit`).
- Update the existing test
  `checkpoint_trigger_is_segment_count_until_a_floor_is_set` to pass
  `last_ckpt_bytes = 0` (behaviour unchanged) and add
  `checkpoint_trigger_is_byte_proportional_once_seeded`: with
  `last_ckpt_bytes = 1_000_000`, ratio 1.0: 32 segments + 999_999 bytes →
  false; + 1_000_000 → true; ratio 0.5 → true at 500_000; count below 32
  → false regardless of bytes.

### Seed `last_ckpt_bytes` across restarts

`crates/store-s3/src/log.rs`:

- `CheckpointRef { seq, bytes: u64 }` (plain required field).
  `put_checkpoint_with_vector` writes `bytes: snapshot.len() as u64`.
- Add `pub async fn get_checkpoint_ref(&self) -> Result<Option<CheckpointRef>>`
  (GET `LATEST` only; `NotFound → None`). `get_latest_checkpoint` uses it.

`Shipper::attach_with_mode` is sync and has no runtime; add
`pub async fn seed_checkpoint_baseline(&mut self)` that best-effort reads
`get_checkpoint_ref()` and sets `last_ckpt_bytes = r.bytes` (0 on
error/absent → count trigger governs the first checkpoint, exactly as
today). Call it from `node_runtime.rs` right after `attach_with_mode`
inside the existing `rt.block_on(...)` that runs `adopt_root`.

Tests (log.rs): `CheckpointRef` round-trips with `bytes` set.

## Step 2 — Inline compaction of superseded checkpoints

`crates/store-s3/src/log.rs` `put_checkpoint_with_vector`, **after** the
`LATEST` PUT and the `VECTOR.json` PUT have both succeeded:

```rust
// Best effort: a failure here is logged and left to daily GC.
if let Err(e) = self.prune_superseded_checkpoints(seq).await {
    tracing::warn!(error = %e, "inline checkpoint prune failed; GC will catch up");
}
```

`prune_superseded_checkpoints(keep_seq)`: `list(checkpoints/p0/)`, parse
`<seq>.zst` names, sort ascending, delete all but the newest 2, and never
delete `keep_seq` (defensive even though it is always among the newest 2).
Use `store.delete_stream` or per-key `delete` with bounded concurrency
(8). Ignore `NotFound` on delete.

Defensive companion in `get_latest_checkpoint`: if the snapshot GET
returns `NotFound`, re-read `LATEST` once and retry the GET; only then
error. (Covers a bootstrap that loses the race against two rapid
checkpoints.)

Tests (log.rs): after 5 `put_checkpoint_with_vector` calls only the
newest 2 objects remain and `LATEST` resolves; wrap the store in a small
`ObjectStore` decorator that fails `delete` and assert the put still
succeeds and `LATEST` is readable; `get_latest_checkpoint` recovers when
the first-named object is deleted between the pointer read and the GET
(simulate by deleting it in a decorator's `get` hook on first call).

## Step 3 — The holder does not tail partitions it holds

`crates/cli/src/shipper.rs`:

- Change `tail_all(&mut self)` to `tail_all_except(&mut self, held:
  &HashSet<String>)`; `tail_all` becomes `tail_all_except(&HashSet::new())`
  and stays the implementation of `tail_to_head` (the takeover witness
  **must** see every stream).
- In `sync_all_inner`, build `held` from `leases`:
  `leases.iter().filter(|(_, k)| k.ship_epoch().is_some()).map(|(p, _)| p.clone())`
  and call `tail_all_except(&held)`. Same in `shutdown_all`.
- `sync_one(part, lease)`: skip `tail_part(part)` when
  `lease.ship_epoch().is_some()`.
- In `ship_part`, on `Err(StoreError::AlreadyExists)`: call
  `self.tail_part(part).await?` **before** `return Ok(true)`. Without
  this, a restarted holder whose own unacked segment already sits at
  `next_seq` would loop forever (the tail that used to absorb it is now
  skipped). The existing "recovered unacked segment" branch in
  `apply_decoded_segment` handles the rest.
- A keeper transitioning to lost (`is_lost()`) must trigger one forced
  `tail_part` on its next round; `ship_epoch()` returns `None` for a lost
  keeper so it naturally drops out of `held` — verify with a test rather
  than assuming.

Tests (shipper.rs multi-node pattern):
- `holder_ships_without_listing_its_own_stream`: wrap `InMemory` in a
  counting decorator; node A holds p0 and ships 10 rounds; assert
  `list_with_offset` count for `log/p0` is 0 after the acquisition tail.
- `restarted_holder_recovers_own_unacked_segment_without_tailing_all`:
  ship a segment, drop the ack (simulate by re-inserting the journal rows
  / or by putting the segment via `LogStore` directly and leaving the
  journal), re-attach, sync; assert journal empty and `next_seq` advanced.
- Existing convergence tests must still pass unchanged.

## Step 4 — GET-next tailer and idle backoff

### 4a. `LogStore::get_run`

`crates/store-s3/src/log.rs`:

```rust
/// Fetch segments `from, from+1, …` concurrently (`k` in flight) and
/// return the longest contiguous run present, in order. A `NotFound`
/// at `from + i` ends the run at `i`; anything fetched beyond the first
/// gap is discarded (it is unreachable until the gap fills).
pub async fn get_run(&self, from: u64, k: usize) -> Result<Vec<(u64, Vec<u8>)>, StoreError>
```

Implementation: `futures::stream::iter(from..from+k).map(|seq| async {
(seq, self.store.get(&key).await) }).buffered(k)`, collect in order, stop
at the first `Err(object_store::Error::NotFound)`, propagate any other
error. Decrypt/decompress via the existing `open_segment`. In E2E a
missing DEK surfaces as before.

### 4b. Tailer rewrite

`shipper.rs` — replace the body of `tail_part` / `tail_part_listed` with:

```text
loop {
    next = parts[part].next_seq
    run  = log.get_run(next, TAIL_GET_CONCURRENCY)          // k = 8 today; raise to 16
    for (seq, payload) in run { apply_segment_payload(part, seq, payload) }
    if run.len() < k { return }                              // steady state: done
    // catch-up mode: all k hit, we may be far behind. One LIST tells us
    // how far; then fetch the rest with the existing 8/16-wide GET
    // pipeline and loop once more to catch the tail.
    seqs = log.list_segments_from(parts[part].next_seq)
    fetch contiguous run of seqs pipelined, apply in order
}
```

`tail_all_except` keeps its parallel-across-partitions shape but calls
the new per-partition routine instead of a LIST sweep. Raise
`TAIL_GET_CONCURRENCY` to 16 (measured 1693 vs 2201 ms for k=16 vs 8 on
HU→AWS; 279 vs 333 same-region).

`bootstrap()` keeps using LIST + sequential GET for its replay (it is
always in catch-up mode); optionally route it through the same
pipelined fetch — not required.

### 4c. Idle backoff in the sync task

`crates/cli/src/node_runtime.rs`, the `'sync` loop:

- New env `CONSTELLATION_SYNC_IDLE_MAX_MS`, default `30_000`.
  `interval_ms` (default 500) is the *floor*.
- Keep `idle_rounds: u32`. After each poll-triggered round, decide
  whether it was **productive**: any segment applied (compare
  `spool.head_seq` before/after), any segment shipped, or journal
  non-empty. Productive → `idle_rounds = 0`; else `idle_rounds += 1`.
- Next poll deadline = `min(interval_ms << idle_rounds, idle_max_ms)`.
  Any `SyncRequest` (nudge, barrier, acquire, forward, gossip-driven
  `Nudge` from `P2pBridge`) resets `idle_rounds = 0` and the deadline to
  `interval_ms`.
- Log at `debug!` when backoff reaches the ceiling and when it resets.

Consequence to document: with P2P down, a follower's freshness bound
degrades from 0.5 s to `idle_max_ms` after ~6 idle rounds; the moment a
segment arrives it snaps back to 0.5 s. This is the documented posture
(DESIGN §12 "eventual S3 polling closes it").

Tests:
- log.rs: `get_run` returns `[1,2,3]` when 1–3 exist and 4 is missing
  even if 5 exists; returns empty when `from` is missing; k > run length.
- shipper.rs: `tailer_uses_get_probes_not_list_in_steady_state` — counting
  decorator; after catch-up, 10 idle rounds issue 0 LISTs and exactly 10
  GET-404s per partition (k probes fire concurrently; count the
  *first-miss* GET, or assert `list` count == 0 and `get` count > 0).
- node_runtime.rs (there is a test module at the bottom): a pure function
  `next_poll_ms(interval, idle_rounds, max) -> u64` unit-tested for the
  doubling and the cap; the loop calls it.

## Step 5 — Ship loop hygiene (no pipelining)

`shipper.rs`:

- Add `SEGMENT_MAX_BYTES: usize = 4 << 20`. `take_journal_grouped` is
  count-bounded; after building `records`, if the encoded payload exceeds
  the cap, truncate the batch to the largest prefix under the cap (encode
  once with all, then binary-search or walk record sizes — records carry
  their postcard length via `to_postcard().len()`). Whatever is cut
  stays journaled and ships next round; `ack_journal_rows_at` gets only
  the shipped seqs. Atime ride-along rows count against the cap too.
- `ship_all` currently takes a fresh `take_journal_grouped` per
  partition *and* `ship_part` takes another. Take it **once** per
  `ship_all` and pass the batch into `ship_part` (signature
  `ship_part(&mut self, part, lease, batch: JournalBatch)`); keep a thin
  `ship_part_taking(part, lease)` wrapper for `sync_one`.
- `note_xpart_shipped` and `consider_xpart_aborts` each call
  `take_journal_grouped(usize::MAX)` to scan for a `RenameXpartDst`; add
  `SqliteMeta::journal_has_xpart_dst(txid) -> bool` (one indexed-ish
  query, or a scan without materializing every record) and use it.

Why no PUT pipelining: see Settled decisions. Record the measured
numbers in a comment on `ship_part` so the next person does not re-derive
them.

Test: `oversized_batch_is_split_at_the_byte_cap` — journal 3 records whose
encoded size is ~3 MiB each; one round ships one segment (≤ cap) and
leaves two journaled; three rounds drain everything; the peer converges.

## Step 6 — Parallel ranged checkpoint download and multipart upload

`crates/store-s3/src/log.rs`:

- `get_latest_checkpoint`: replace `store.get(key).bytes()` with
  `get_object_parallel(&self.store, &key, RANGE_BYTES, CONCURRENCY)`:
  `head` for size; if size ≤ `RANGE_BYTES` do a single GET; else build
  `[0..r, r..2r, …]` and call `store.get_ranges(&key, &ranges)` — it
  already runs ranges concurrently internally, but its coalescing is
  tuned for small ranges; if measured throughput is not within 20% of the
  Appendix numbers, issue `get_range` calls through
  `futures::stream::iter(..).buffered(CONCURRENCY)` yourself.
  `RANGE_BYTES = 8 MiB`, `CONCURRENCY` from
  `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY` (default 8). Assemble in order
  into one `Vec<u8>` (E2E: the AEAD seal covers the whole body, so it has
  to be whole before `decrypt_object` anyway).
- `put_checkpoint_with_vector`: for bodies > 16 MiB use
  `object_store::WriteMultipart` (`store.put_multipart(&key)` → `put`
  chunks of 8 MiB, `finish()`); below that keep the single PUT. Note
  `InMemory` supports multipart; the local file backend does too.
- Do the same for the snapshot GET in `shipper::bootstrap` automatically
  (it calls `get_latest_checkpoint`).

Tests: round-trip a 40 MiB random body through put/get in `InMemory`,
E2E and plain; assert byte equality. Assert a 1 KiB body takes the
single-GET path (counting decorator: exactly one `get`, zero `head`s —
or accept one `head`; document which).

## Step 7 — Sticky leases with an S3-visible handoff request

### 7a. Lease object

`crates/store-s3/src/lease.rs` `Lease`: add `pub wanted_by: Vec<u64>`
(node ids, sorted, deduped; plain required field). `granted()` and
`released()` clear it; `renewed()` **preserves** it. `LEASE_VERSION`
stays as is (no bump policy).

### 7b. Requester side

`crates/cli/src/shipper.rs` `acquire_lease_for`: on `Plan::Busy` **and**
`lease_mode == LeaseMode::Cas`, if `prev.wanted_by` does not contain
`self.node_id`, CAS-swap `prev` with ourselves appended (same holder,
same epoch, same expiry — only `wanted_by` changes) using the tag from
`classify`. `CasConflict` → ignore (someone else moved it; we retry on the
next `Acquire`). Return `Ok(false)` as today. `classify()` must therefore
return `prev` and `tag` inside `Plan::Busy` — extend the variant.

Rate-limit: remember `wanted_registered_at: HashMap<part, Instant>` on
the `Shipper`; do not re-register within `lease_ttl_ms / 2`.

### 7c. Holder side

`crates/cli/src/lease.rs`:

- `renew_now`: on `CasConflict`, `diagnose_lost_renew` already re-reads.
  Add a branch: if `cur.holder == self.node_id && cur.epoch == mine.epoch
  && !cur.released && !cur.is_expired(now)` then this is a `wanted_by`
  edit, not a deposition — adopt `(cur, new_tag)` into `self.held`, copy
  `cur.wanted_by` into a new `self.wanted: Vec<u64>`, and retry the renew
  once with the fresh tag. Only the existing conditions (different holder
  / higher epoch / expired-and-taken) mean deposed.
- `idle_release_due(journal_backlog)` becomes:

  ```rust
  !epoch_held && held.is_some() && journal_backlog == 0
      && !self.wanted.is_empty()
      && idle_for_ms() >= idle_release_ms
      && held_for_ms() >= LEASE_MIN_DWELL_MS   // 5_000; prevents ping-pong
  ```

  There is no switch to restore unconditional idle release; the harness
  scenario `sticky_lease_handoff_over_s3` is what proves the new path.
- `release()` writes `released: true` with `wanted_by` cleared.
- A P2P `HandOff` request (existing) is unchanged and remains the fast
  path; it does not need `wanted_by`.

### 7d. Requester retry loop

`crates/cli/src/fusefs.rs` `require_lease_for`: the retry sleep is a
fixed 100 ms. Make it `100 → 200 → … → 2000 ms` doubling, so a
long-waiting FUSE thread does not drive 10 classify GETs/s. Keep
`acquire_deadline`.

### Worst-case reasoning to write into the module doc

With P2P down: a requester registers `wanted_by` at its first `Acquire`
(1 CAS RTT); the holder notices at its next renewal (≤ TTL/2 = 30 s),
finishes any in-flight batch, releases; the requester claims on its next
retry. Today's worst case is the same 30 s (idle release) or an EIO after
2×TTL if the holder is busy. So sticky leases are never worse and remove
the 3-RTT re-acquire from the common path entirely. With P2P up,
forwarding (ADR-14) means the requester never needs the lease at all.

Tests (shipper.rs multi-node, `InMemory` + `LeaseMode::Cas`):
- `active_holder_never_releases_idle_without_a_requester`: A holds, goes
  idle for `2 × idle_release_ms` (drive `idle_for_ms` via a test clock or
  set `CONSTELLATION_LEASE_IDLE_RELEASE_MS=1` and sleep); assert A still
  holds and `nodes()`… i.e. the lease object still names A.
- `requester_registers_wanted_by_and_gets_the_lease`: B calls
  `acquire_lease` → false, lease object has `wanted_by == [B]`; A
  `renew_now()` → adopts, `idle_release_due` true after dwell; A
  releases; B acquires; A's late segment is fenced (epoch bumped).
- `wanted_by_edit_is_not_a_deposition`: A renews after B's edit and stays
  holder with the same epoch.
- lease.rs (store): `renewed` keeps `wanted_by`; `released`/`granted`
  clear it.

## Step 8 — Existence hint from `chunk_ref`, no mount-time LIST

`crates/cli/src/existence.rs`:

- **Delete the mount-time LIST seed** (`spawn_seed`, `LIST_CONCURRENCY`,
  the `list_enabled` flag and `CONSTELLATION_EXISTENCE_LIST`). The bloom
  stays as an in-process cache of hashes this node has itself uploaded or
  been hinted about by peers (`insert`, `note_peer_hint`).
- Add the replica as the authoritative hint source:
  `Existence::with_meta(meta: Arc<SqliteMeta>)`; `contains(hash)` returns
  `Some(true)` if the bloom hits **or** `meta.chunk_ref_exists(hash)`
  (new `SqliteMeta` fn: `SELECT 1 FROM chunk_ref WHERE chunk_hash = ?1
  LIMIT 1`, via `with_reader`). Because `chunk_ref` is maintained by
  replay of *foreign* records too, a hash referenced anywhere in the
  cluster is a hit. Otherwise return `None` (unknown → today's adaptive
  fallback). `Some(false)` no longer exists: without a complete LIST
  nothing can prove absence, and the conditional PUT does not need it.
- The consumer in `main.rs` (`existence.contains(hash)` → `Probe` vs
  create) is unchanged: a hit still means "HEAD first, PUT on 404"; the
  conditional PUT remains the correctness operation.
- `report()` gains `chunk_ref_hits`.

Tests: `chunk_ref_hit_selects_probe_without_a_list` — in-memory meta with
one replayed `WriteManifest`; `contains` → `Some(true)`. Remove the
LIST-seed tests (`seed_for_test` callers in `main.rs` tests) or rewrite
them against `insert`.

## Step 9 — Configuration & docs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_CHECKPOINT_RATIO` | `1.0` | shipped-log bytes ÷ last snapshot bytes to fire a checkpoint (count floor `CHECKPOINT_EVERY` still applies) |
| `CONSTELLATION_CHECKPOINT_MIN_INTERVAL_S` | `0` | unchanged opt-in time floor |
| `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY` | `8` | parallel 8 MiB ranges for checkpoint GET/PUT |
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | unchanged; now the *floor* of the idle backoff |
| `CONSTELLATION_SYNC_IDLE_MAX_MS` | `30000` | ceiling of the idle poll backoff |
| `CONSTELLATION_LEASE_IDLE_RELEASE_MS` | `30000` | unchanged; now only applies when a requester is registered |
| `CONSTELLATION_EXISTENCE_LIST` | removed | mount-time chunk LIST seed is gone |
| `CONSTELLATION_LOG_RETENTION_SEGMENTS` | `128` | unchanged; now applied per partition |

`docs/reference/configuration.md`: document each. `docs/explanation/`
is **not** edited (CONVENTIONS rule 5); record in `PROGRESS.md` that
DESIGN §4 "A holder releases after about 30 seconds without a mutation"
is now conditional on a registered requester, and that §12's polling
backstop has an idle ceiling.

## Step 10 — Harness scenarios

`crates/harness/src/scenarios.rs`:

- `ckpt_bulk_ingest_bounded`: single writer ingests N (seeded) small
  files; assert `checkpoints/p0/` object count ≤ 2 at the end and total
  checkpoint bytes ≤ 3× the final snapshot size; a fresh node bootstraps
  and its tree matches the oracle.
- `idle_cluster_is_quiet`: 3 nodes, 60 s idle after convergence, S3
  proxy request log (toxiproxy or the harness's request counter if one
  exists; otherwise count via the `InMemory`-decorator pattern in an
  in-process test instead) shows ≤ `nodes × (lease renewals + heartbeats
  + 3)` requests and zero LISTs of `log/`.
- `wan_writer_ships_put_only`: latency toxic 200 ms on S3; one holder
  writes 500 files; assert zero LISTs of `log/p0` during the burst and
  that a follower converges within `idle_max_ms + 5 s` with P2P disabled.
- `sticky_lease_handoff_over_s3`: P2P disabled; A writes, B writes to the
  same partition → B's write completes within `lease_ttl/2 + dwell +
  10 s` (not EIO), A's late segment is fenced, both converge.
- `multi_partition_retention_is_per_partition`: force a split
  (`CONSTELLATION_PART_AUTOSPLIT=on`, low `PART_SPLIT_OPS`), write
  100k+ records to p0 and 10 to p1, run `gc run`, then bootstrap a fresh
  node and assert p1's files are present (regression for Step 0).

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus paste into the report:
- Before/after checkpoint object count and bytes for the ingest scenario.
- The `idle_cluster_is_quiet` request tally by class (LIST/GET/PUT/HEAD).
- The `sticky_lease_handoff_over_s3` timing.
- Confirmation that FUSE-path small-file latency is unchanged or better
  (the single-threaded 465–501 µs/file figure in the `shipper.rs` module
  doc is the baseline).

## Appendix — S3 assumption measurements

Script: `bench/s3_assumptions.py` (boto3; `S3_BUCKET`, optional
`S3_ENDPOINT`/`S3_REGION`/`S3_ADDRESSING`; `--cleanup` removes its
objects). It measures
the primitives and loops that a metadata-plane redesign would rely on:
per-request latency, reader catch-up (LIST-per-round vs GET-next),
writer ship (LIST+PUT vs PUT-only vs pipelined), and bootstrap download
shapes (one image vs many small blobs vs packed blobs).

Three paths, same day (2026-09-10):

| path | client | bucket |
|---|---|---|
| **HU→AWS** | Hungary, 1 Gbit/s residential, no VPN | AWS S3 standard, us-west-2 |
| **EC2→AWS** | EC2 in us-west-2b (same region as bucket) | same AWS bucket, separate prefix |
| **HU→OVH** | Hungary, same residential link | OVH Object Storage, 3-AZ replicated, Milan (`s3.eu-south-mil.io.cloud.ovh.net`), path-style |

No bucket names below. All times are wall clock; prims are p50 of n=20
unless noted. Segment size = 9.5 KiB (a typical Constellation log
segment). Bootstrap payload = 64 MiB.

### Per-request latency (p50)

| op | HU→AWS | EC2→AWS | HU→OVH |
|---|---:|---:|---:|
| LIST empty | 175 ms | 14 ms | 34 ms |
| LIST 1000 keys | 331 ms | 171 ms | 344 ms |
| LIST StartAfter → 10 (today's tail) | 180 ms | 20 ms | 39 ms |
| GET hit 200 B | 187 ms | 28 ms | 35 ms |
| GET 404 | 177 ms | 11 ms | 34 ms |
| HEAD hit | 176 ms | 13 ms | 27 ms |
| PUT 200 B | 194 ms | 28 ms | 109 ms |
| PUT 10 KiB | 193 ms | 31 ms | 111 ms |
| PUT If-None-Match:* (segment CAS) | 193 ms | 28 ms | 156 ms |
| HEAD + PUT If-Match (lease renew) | 387 ms | 46 ms | 174 ms |
| PUT If-Match stale → 412 | 599 ms | 74 ms | 112 ms |
| same, client retries=1 | 671 ms | 79 ms | 130 ms |

Observations:

- On AWS every primitive shares one RTT floor (~180 ms HU, ~15–30 ms
  same-region). A GET-404 is **not slower** than an idle LIST, and is
  ~12× cheaper per request — so replacing idle poll LIST with
  speculative GET-next is free on latency and a clear win on cost.
- A 1000-key LIST page is ~2× a GET everywhere (AWS HU 331, OVH 344,
  AWS same-region 171). Keep LIST for catch-up; use GET-next for
  steady-state.
- Stale `If-Match` → 412 is slow on AWS even with client retries
  disabled (671 ms HU). That is server-side, not botocore. OVH returns
  412 in ~RTT. Worth knowing for fencing paths that treat 412 as the
  branch signal — they should not assume "fast fail".
- OVH PUTs are asymmetric: GET/LIST/HEAD ~35 ms, but PUT p50 ~110 ms
  and p90 300–350 ms. Segment CAS is correspondingly slower than a
  plain GET. Conditional writes (`If-None-Match` / `If-Match`) work
  and fence correctly (412 on conflict).

### Reader catch-up (64 segments) and idle poll

| | HU→AWS | EC2→AWS | HU→OVH |
|---|---:|---:|---:|
| LIST-per-round (today) | 3132 ms | 458 ms | 685 ms |
| GET-next k=8 | 2201 ms | 333 ms | 596 ms |
| GET-next k=16 | 1693 ms | 279 ms | 476 ms |
| idle LIST p50 | 173 ms | 14 ms | 34 ms |
| idle GET-404 p50 | 170 ms | 25 ms | 33 ms |

Catch-up is a modest win for GET-next (roughly 1.3–1.8×); the
structural win is idle cost and request class, not catch-up latency.
Same-region idle LIST is *faster* than GET-404 (14 vs 25 ms) —
likely S3 LIST of a warm empty suffix is cheaper than a miss GET —
but the request-price argument still favors GET-next at volume.

### Writer ship (32 segments)

| | HU→AWS | EC2→AWS | HU→OVH |
|---|---:|---:|---:|
| LIST+PUT per seg (today) | 2.6 seg/s | 24.5 seg/s | 5.4 seg/s |
| PUT only, sequential | 5.1 seg/s | 33.1 seg/s | 6.2 seg/s |
| PUT pipelined depth=4 | 16.0 seg/s | 128 seg/s | 22.6 seg/s |
| PUT pipelined depth=8 | 29.4 seg/s | 231 seg/s | 41.6 seg/s |
| duplicate-seq CAS → 412 | OK | OK | OK |

Removing the holder's self-tail roughly doubles WAN ship throughput
on AWS (2.6 → 5.1) and is a smaller but free win same-region and on
OVH (where LIST is cheap relative to PUT). Pipelining to depth 4–8
gives another 3–7× everywhere, and CAS still rejects a duplicate
seq afterwards — the "sole appender; readers wait for contiguity"
argument holds on both backends.

### Bootstrap download (~64 MiB)

| | HU→AWS | EC2→AWS | HU→OVH |
|---|---:|---:|---:|
| 1×64 MiB, transfer mgr (ranged) | 6.3 s / 10 MiB/s | 0.30 s / 214 MiB/s | 2.7 s / 24 MiB/s |
| 1×64 MiB, single GET | 23.5 s / 2.7 MiB/s | 1.3 s / 49 MiB/s | 6.6 s / 9.7 MiB/s |
| 4096×16 KiB, conc=64 | 17.4 s / 3.7 MiB/s | 6.9 s / 9.2 MiB/s | 13.3 s / 4.8 MiB/s |
| 4096×16 KiB, conc=256 | 15.9 s / 4.0 MiB/s | 7.4 s / 8.6 MiB/s | 13.2 s / 4.9 MiB/s |
| 64×1 MiB packed, conc=16 | 3.4 s / 19 MiB/s | 0.26 s / 249 MiB/s | 2.0 s / 31 MiB/s |
| 64×1 MiB packed, conc=64 | 5.1 s / 13 MiB/s | 0.24 s / 263 MiB/s | 2.2 s / 29 MiB/s |

### Conclusions for the redesign

1. **Cadence (this plan) is still the right immediate fix** — it bounds
   checkpoint *write* amplification regardless of path. The numbers
   above are about *read* and *steady-state* cost.
2. **Holder must not LIST its own partition.** Confirmed on all three
   paths; ~2× ship rate on the far AWS path.
3. **Idle poll → GET-next; catch-up keeps LIST.** Latency-neutral or
   better; large request-cost win for many idle nodes.
4. **Pipeline segment PUTs (depth 4–8).** Safe under sole-appender +
   contiguous apply; ~5–10× ship rate on WAN.
5. **Bootstrap from loose Merkle tree blobs is too slow** relative to
   a parallel-ranged image (HU→AWS: 16 s vs 6 s; same-region: 7 s vs
   0.3 s). A Merkle checkpoint should publish **packed ~1 MiB blobs
   from day one**, which matches or beats the ranged image on every
   path (HU→AWS 3.4 s, HU→OVH 2.0 s, same-region 0.26 s). The
   incremental-write property is unchanged; only read granularity
   moves.
6. **Today's single-GET checkpoint download is leaving 3–4× on the
   table** even with no format change (HU→AWS 23 s → 6 s via the
   transfer manager). Worth doing independently of everything else.
7. **OVH Milan is a viable near-client backend** for a HU node: ~35 ms
   read RTT, CAS works. Writes are 3–4× reads; ship pipelining
   matters more there than on same-region AWS. The LIST-1000 page is
   as expensive as on far AWS (~340 ms) — another reason not to poll
   with LIST when idle.

