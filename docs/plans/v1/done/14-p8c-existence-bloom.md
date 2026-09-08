# Plan 14 — Phase 8c: existence bloom for cold high-dedup uploads

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–13
committed (phase 8b is `8d98ac8` / `4c77b4a`). Spec: `docs/DESIGN.md`
§2 (chunk key sharding / LIST parallelism), §7 (bloom parameters;
peer digests are cache membership, not S3 membership), §14 rule 2
(condemned hashes are absent for dedup). Do not edit DESIGN.md.

This is the follow-up named in plan 08 step 2a and in
`docs/PROGRESS.md` phase 5b deferred notes.

## Problem

A cold node importing a tree that already exists in the bucket still
pays per-chunk HEAD or Create-PUT. The 5b ladder already skips work
for the *local* durable set. It cannot see objects that only live in
S3 or in a peer's cache.

## Invariant (non-negotiable)

**No bloom hit may skip the PUT.** A false positive would journal a
manifest that points at a missing object. Bloom membership is only a
*probe selector*:

- `contains == false` on a complete S3 LIST seed: the hash was not in
  the listed key set. Skip HEAD and Create-PUT (AlreadyExists on a
  race with a concurrent uploader is still success).
- `contains == true` on the LIST seed **or** any peer digest: likely
  present. Confirm with HEAD (`ChunkPutMode::Probe`) or
  `PutMode::Create`. Never ack the pending row without a successful
  store call.
- Peer-digest `contains == false` does **not** mean absent from S3.
  Fall through to the LIST seed / Create / adaptive HEAD.

Condemned hashes still force `Overwrite` inside `put_chunk_mode`.
Test that a LIST-seed or peer-hint hit cannot resurrect the skip.

## Ladder (cheapest first)

Wire this in `UploadRuntime::put_mode` (`crates/cli/src/main.rs`)
per hash, after the existing local-durable skip in `cache_for_upload`
(that path never reaches the pool):

1. **Local durable** — already implemented. Do not regress.
2. **Peer digest hint** — if any live, non-stale peer filter
   `contains` the hash, choose `Probe`. Reuse the cooperative-cache
   digest map already maintained by `cli::coop` (no new P2P messages).
   Dirty chunks are already excluded from those filters
   (`is_servable`); do not change that. Add a small read-only API
   (`Coop::peer_digest_contains` or equivalent) rather than reaching
   into coop internals.
3. **LIST-seeded S3 bloom miss** — if the seed is marked complete and
   the hash is absent, choose `Create` (or `Overwrite` when
   `create_if_absent` is false) **without** HEAD.
4. **LIST-seeded S3 bloom hit** — choose `Probe`.
5. **Existing adaptive HEAD / Create** — unchanged fallback when the
   seed is incomplete, disabled, or still loading.

## LIST seed

- Run **in the background after mount**, never block the FUSE loop.
  Uploads that race the seed use rungs 4–5.
- Page `chunks/` (1000 keys/request). Parse the last path component
  as a 64-char hex `ChunkHash`; skip unparseable keys. Use the two
  hex-pair shards (`chunks/<aa>/`) for LIST parallelism as DESIGN.md
  §2 already documents — 256 prefix lists, bounded concurrency
  (8 is fine).
- Implement listing/parsing in `store-s3` (`ChunkStore::list_chunk_hashes`
  or a small `existence` helper). Do **not** add a `store-s3 → net`
  dependency (iroh). The bloom itself lives in the CLI and reuses
  `net::bloom`'s hashing (`k=7`, 10 bits/entry).
- The gossip `MAX_BITS_BYTES` cap must **not** apply. This filter is
  local and never framed. Size it for listed `n` at 10 bits/entry
  with a hard RSS cap (`CONSTELLATION_EXISTENCE_BLOOM_BYTES`, default
  4 MiB ≈ 3.3M chunks). If LIST would exceed the cap, stop inserting,
  mark the seed **incomplete**, and treat further hashes as unknown
  (rung 5), not as misses.
- Kill switch: `CONSTELLATION_EXISTENCE_LIST=off` skips the LIST
  entirely (peer hints still work). `CONSTELLATION_EXISTENCE_PEER_HINT=off`
  skips rung 2. `CONSTELLATION_COOP=off` already means no digests;
  treat that as peer-hint off.
- After a successful local PUT, insert the hash into the existence
  bloom (add-only). Do not try to delete on GC; a stale hit only
  costs a HEAD that 404s, then PUT. Document that.
- Refresh: do not re-LIST on a timer in this phase. Mount-time seed
  plus insert-on-put is enough. Note in PROGRESS.md that a post-GC
  re-LIST would tighten FPR; out of scope.

## Status

Extend `WritebackStatus` (additive serde defaults):

- `existence_listed: u64`
- `existence_complete: bool`
- `existence_bloom_hits` / `existence_bloom_misses`
- `existence_peer_hints`

Harness and unit tests assert on these. Keep `/metrics` in sync if
the encoder already dumps writeback gauges.

## Tests

Unit (counting `ObjectStore` wrapper over `InMemory`, same idiom as
5b):

- LIST seed of N known hashes: a seeded hash issues **HEAD only**
  (Probe), not a body PUT; an unknown hash issues **Create and no
  HEAD**.
- Bloom false-positive fixture: force `contains == true` for a hash
  that is not in the bucket; the upload must still HEAD or Create and
  must not ack without a store call.
- Peer-hint: with an injected digest that contains hash H, `put_mode`
  is Probe even when the LIST seed is empty/incomplete.
- Peer miss does not skip HEAD if adaptive probe is on.
- Condemned hash with both blooms claiming present still Overwrite-PUTs.
- Incomplete seed (cap exceeded) never treats unlisted hashes as
  misses.
- Key parser: `chunks/aa/bb/<64 hex>` round-trips; junk keys ignored.

Harness:

- `existence-bloom-dedup`: node A writes a unique tree and unmounts.
  Node B mounts with a **fresh state dir** (empty local durable set,
  coop optional), waits until `existence_complete`, copies the same
  bytes. Assert `existence_listed >= unique chunks`,
  `existence_bloom_misses` stays near zero for the copy, content
  model-verified, and a raw bucket LIST shows no extra objects for
  those hashes. Kill `CONSTELLATION_EXISTENCE_LIST=off` in a second
  pass (or a distinct scenario beat) and show the copy still works,
  just without those counters.
- `existence-peer-hint`: A holds the chunks in cache and has gossiped
  digests (`CONSTELLATION_DIGEST_INTERVAL_S=1`). B copies the same
  tree with `CONSTELLATION_EXISTENCE_LIST=off` so LIST cannot steal
  the credit. Assert `existence_peer_hints >= 1` and the copy is
  model-verified. `CONSTELLATION_COOP=off` on B must not crash and
  must not report peer hints.

## Out of scope

- Gossiping the S3 existence bloom (peer cache digests already cover
  the warm-fleet case; a second gossip channel is not needed).
- Serving dirty chunks; record-bearing P2P.
- Re-LIST after GC.
- Changing bloom FPR parameters.

## Gates + report

Per CONVENTIONS.md. Update `docs/PROGRESS.md` with a Phase 8c table,
`docs/TESTING.md` with the two scenarios, and `docs/plans/README.md`
if you add the row (the coordinator may already have). Paste harness
counter lines. Note the LIST cost (keys listed, wall time) on the
dedup scenario so we know the seed is cheaper than per-chunk HEAD
for that shape.
