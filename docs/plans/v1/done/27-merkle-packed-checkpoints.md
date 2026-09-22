# Plan 27 — Merkle checkpoints: incremental, packed, engine-independent

Read `docs/plans/v1/CONVENTIONS.md` first, then plan 26
(`26-metadata-plane-s3-efficiency.md`) — this plan **requires** plan 26
to be in the tree (Steps 0–6 in particular) and reuses its Appendix
measurements. Compatibility policy is the same as plan 26: pre-release,
no old-format read paths, no version bumps, recreate test buckets.

Spec context: `docs/explanation/DESIGN.md` §4 (checkpoints), §13
(snapshot tree objects — the representation this plan generalizes), §14
(GC roots), ADR-5 (partial replicas as the 100M+ path), ADR-11 (`pack`
reservation). Code context:

- `crates/fs-core/src/tree.rs` — `Tree`, `TreeEntry` (git-style directory
  object; `encode`/`decode`, `CTR2` magic).
- `crates/cli/src/snapshot.rs` — `SnapshotManager::build_tree` (today: a
  full recursive walk with one conditional PUT per manifest and per tree
  blob, no cached hashes).
- `crates/meta/src/sqlite.rs` — schema (`inode`, `dentry`, `xattr`,
  `partition`, `xpart_pending`, `snapshot`, `deref`, `chunk_ref`, `kv`),
  `recursive_size_conn`, `snapshot_children`, `apply_foreign`.
- `crates/meta/src/replay.rs` — `SqliteMeta::snapshot` (the `VACUUM INTO`
  that this plan deletes), `apply_one`.
- `crates/store-s3/src/log.rs` — `put_checkpoint_with_vector`,
  `get_latest_checkpoint`, `CheckpointRef`, `CheckpointVector`.
- `crates/cli/src/shipper.rs` — `Shipper::checkpoint`, `bootstrap`.
- `crates/cli/src/gc.rs` — `snapshot_roots`, `metadata_candidates`.

## Problem

After plan 26 a checkpoint is still a whole-DB SQLite image. Three costs
remain O(DB) rather than O(change):

1. **Write.** Every checkpoint copies and uploads the entire replica
   (~1 GB pre-zstd at the §11 reference scale). Plan 26 bounds *how
   often*; it cannot bound *how much*.
2. **Read.** Bootstrap downloads the entire image. Measured (plan 26
   Appendix, 64 MiB, Europe → us-west-2): 6.3 s with ranged parallel
   GETs. Linear in DB size: ~40 s at 400 MiB, ~100 s at 1 GB.
3. **Coupling.** The bucket format *is* SQLite's file format. Swapping
   the local engine (ADR-9 keeps LMDB as an alternative) would be a
   bucket migration, and a replica can never be partial (ADR-5).

Two further defects the current representation has:

- `SnapshotManager::build_tree` walks the whole subtree and issues one
  conditional PUT per file manifest and per directory — O(files) S3
  requests per `snapshot create`, contradicting DESIGN §13 "repeated
  snapshots … upload only the changed spine".
- The checkpoint is written by "the" writer under `checkpoints/p0/` with
  plain PUTs; in a multi-partition filesystem the p0 holder checkpoints
  other holders' partitions on their behalf (plan 26 keeps this).

### What the measurements say about the read side

From the plan 26 Appendix (64 MiB, three paths, HU→AWS / EC2→AWS / HU→OVH):

- 4096 × 16 KiB loose blobs, 256-way: **15.9 s / 7.4 s / 13.2 s**
- 64 × 1 MiB packed blobs, 16-way: **3.4 s / 0.26 s / 2.0 s**
- 1 × 64 MiB ranged parallel: 6.3 s / 0.30 s / 2.7 s

Loose directory blobs are 2.5–25× slower to bootstrap from than a
ranged image; packed ~1 MiB blobs match or beat it on every path. So the
Merkle checkpoint **ships packed from day one**; loose per-directory
blobs never appear in the bucket.

## Goal

- Checkpoint write cost ∝ directories changed since the last checkpoint,
  independent of DB size. A checkpoint of an idle tree uploads one small
  root object.
- Bootstrap = parallel download of packs (bandwidth-bound, like today's
  ranged image), then a bulk load into SQLite — no log replay beyond the
  tail `(vector, head]`.
- The bucket checkpoint format is a content-addressed tree of packed
  blobs plus a small sidecar — no engine file format in the bucket.
- `snapshot create` and `checkpoint` are the **same builder**; snapshot
  cost becomes O(changed spine) as §13 promises.
- Any partition holder may publish a checkpoint (vector-monotonic CAS on
  a `ROOT` pointer); log retention is per partition against the published
  vector (already so after plan 26 Step 0).
- The representation supports a later *partial replica* (load
  directories lazily) without another format change. Building that
  mode is **not** in this plan.

Non-goals: packing *data* chunks (ADR-11 `pack` for file content stays
deferred), CDC, changing the log format, LMDB.

## Design

### Objects

```
checkpoints/ROOT                 # JSON, CAS-swapped (If-Match)
checkpoints/packs/<hex>          # pack: concatenated tree/sidecar blobs, ~1 MiB target
```

`ROOT`:

```json
{
  "root": "<blake3 of the root Tree blob>",
  "sidecar": "<blake3 of the sidecar blob>",
  "vector": { "p0": 123456, "p1": 40 },
  "packs": "<blake3 of the pack index blob>",
  "created_unix_ms": 0,
  "publisher": 7
}
```

- **Tree blob** = `fs_core::Tree` as today (`CTR2`), one per directory,
  with two additions to `TreeEntry`: `ino: u64` and `ctime_ns: i64`
  (bootstrap must reproduce inode numbers exactly — they are in log
  records — and ctime is an attribute). `nlink` is derived
  (directories: 2 + subdirs; files: number of dentries pointing at the
  ino, computed at load). Hard links: the same `ino` appears under two
  names; the loader dedups the `inode` row. `rdev` is added for
  `Mknod`. `manifest_or_tree_hash` for files stays the manifest hash; the
  manifest bytes themselves are **inlined in the tree entry** when
  ≤ 8 chunks (matches the `inode.manifest` inline rule) so bootstrap does
  not fetch a manifest chunk per file — add `manifest_inline:
  Option<Vec<u8>>`. Spilled manifests keep referencing the chunk store as
  today.
- **Wide directories**: a directory with more than `TREE_FANOUT = 4096`
  entries is encoded as a **HAMT**-style shard: the parent `Tree` holds
  `entries = []` and `shards: Vec<ChunkHash>` (256 buckets by the first
  byte of `blake3(name)`); each bucket is itself a `Tree` blob (possibly
  sharded again at the next byte if still > `TREE_FANOUT`). Add
  `shards: Vec<ChunkHash>` to `Tree`; a `Tree` has either entries or
  shards, never both.
- **Sidecar blob** (postcard, one per checkpoint, expected KBs–MBs):
  - `partition` rows,
  - `xpart_pending` rows,
  - `snapshot` rows,
  - `kv` replicated keys (`quota_max_bytes` and anything else
    `snapshot()` keeps today — enumerate them while deleting it),
  - orphan inodes (`nlink == 0`, still referenced by `holds/`): full
    inode row + manifest,
  - `deref` rows with `deref_unix_ms > now - gc.horizon` (so a
    bootstrapped node has a usable GC candidate set).
- **Pack index blob** (postcard): `Vec<(ChunkHash blob, ChunkHash pack,
  u32 offset, u32 len)>` for every blob reachable from `root` +
  `sidecar`. This is what makes bootstrap "download the packs" instead
  of "walk the tree": the loader reads the index, fetches every pack it
  names (parallel, ranged if > 8 MiB), and decodes blobs by offset.

Blobs are compressed individually with zstd (level 3) **before** packing
so the index offsets are stable; in E2E mode each blob is AEAD-sealed
under the p0 DEK with its blake3 as AAD, then packed; packs and `ROOT`
are opaque to the provider (`ROOT` is sealed too, AAD = key).

### Incremental build: cached directory hashes

Schema (`sqlite.rs`): add to `inode` — `tree_hash BLOB` (NULL = unknown)
and `tree_dirty INTEGER NOT NULL DEFAULT 1`. Both are **replicated**
state in the sense that every replica computes the same values, but they
are recomputed locally and never travel in log records.

Dirty propagation: every place that mutates a directory's *contents or
attributes* — `mutate.rs` (local ops) and `replay.rs` `apply_one`
(foreign ops) — marks the directory dirty and walks to the root marking
ancestors dirty, stopping at the first ancestor already dirty. The walk
is the same parent chain `recursive_size_conn`'s callers use for
`rsize`/`rcount`. Attribute changes on a *file* dirty its parent (the
entry lives in the parent's blob). Hard-link/unlink/rename dirty both
parents. `setxattr` on a directory dirties it (xattrs are in its own
blob) and its parent (no — the parent stores only the child hash; the
child's hash changes, so the parent re-hashes anyway: mark parent
dirty).

Builder (`crates/cli/src/checkpoint.rs`, new; `snapshot.rs` delegates to
it): `build(root_ino) -> (root_hash, Vec<NewBlob>)`:

```text
fn hash_dir(ino):
    if !dirty(ino) and tree_hash(ino).is_some(): return tree_hash(ino)   // O(1) skip
    children = snapshot_children(ino)                                    // one indexed query
    for each subdir child: child_hash = hash_dir(child.ino)              // recursion only into dirty
    encode Tree (sharded if > TREE_FANOUT), hash, stage blob if hash != tree_hash(ino)
    UPDATE inode SET tree_hash = ?, tree_dirty = 0 WHERE ino = ?
    return hash
```

The build runs inside **one read transaction** on a dedicated reader
connection (WAL) so it sees a consistent DB at a known log position; the
`tree_hash` writes go through the writer connection *after* the build,
in one batch, guarded by `tree_dirty` still being 0 for that ino (a
mutation during the build re-dirties, and the later `UPDATE … WHERE
tree_dirty = 0 AND tree_hash IS NULL OR …` must not clear it —
simplest: record `(ino, hash, dirty_seen_at_build)` and only write
rows whose `tree_dirty` is unchanged; use a per-row `dirty_gen` counter
if a boolean is not enough — implementer's choice, test it).

Consistency with the log position: the builder needs `vector` = every
partition's `applied_seq_of` **as of the same read transaction**. Read
them inside the transaction (they are `kv` rows), not from `Shipper`
state.

Staged blobs are zstd'd, (sealed), appended into packs of ≥ 1 MiB (last
pack may be smaller), packs are hashed and PUT with conditional create
(idempotent), then the sidecar and pack index are built and PUT the same
way, then `ROOT` is CAS-swapped.

### Publishing: any holder, vector-monotonic CAS

`put_checkpoint_root(new: Root, prev_tag: Option<Etag>)`: `If-Match`
swap (or `If-None-Match: *` when no `ROOT` exists). Before swapping, the
publisher reads the current `ROOT` and refuses (returns
`CheckpointRefused::StaleVector`) unless `new.vector[p] >= cur.vector[p]`
for every `p` in `cur.vector`. On `CasConflict` re-read and re-check
once, then give up until the next trigger. This lets an always-on node
near S3 publish while a laptop across an ocean does not have to.

Who triggers: `Shipper::checkpoint` as today (plan 26's proportional
trigger), for every node holding at least one lease. Followers never
publish. A node that holds a lease but whose vector is behind another
holder's on some partition simply gets `StaleVector` and tries again
later — no coordination beyond the CAS.

### GC

`metadata_candidates`:
- `checkpoints/packs/*`: keep every pack named by the current `ROOT`'s
  pack index **and** by the previous `ROOT` (store `prev: Option<hash of
  previous pack index>` in `ROOT`, one level of history — the
  keep-newest-2 rule in pack form). Everything else under
  `checkpoints/packs/` is a mark. Snapshot roots (`snaps/*`) also pin
  packs: `snapshot_roots` must include the packs their trees live in.
  Simplest rule: a pack is live if it appears in any live pack index
  (current `ROOT`, previous `ROOT`, every `snaps/<id>.json`'s
  `packs` field — add it to `SnapshotRecord`).
- Log retention: unchanged from plan 26 Step 0, reading the vector from
  `ROOT` instead of `VECTOR.json`.
- Delete the `superseded-checkpoint` rule and `checkpoints/p0/*`,
  `LATEST`, `VECTOR.json` handling entirely.

### Bootstrap

`shipper::bootstrap`:

1. GET `ROOT` (→ none: empty replica, replay from seq 1 as today).
2. GET pack index; GET every pack (parallel, `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY`).
3. Open SQLite with indexes **dropped** (or create tables without
   secondary indexes), `PRAGMA synchronous=OFF, journal_mode=OFF` for
   the load only; walk the tree **from the decoded blobs in memory**
   (iterative stack, not recursion — depth can be large), inserting
   `inode`/`dentry`/`xattr`/`chunk_ref` rows in batches of 10k per
   transaction; set `tree_hash`/`tree_dirty=0` from the blobs (the
   loaded tree is by construction clean). Load the sidecar. Recreate
   indexes, restore pragmas.
4. Set `applied_seq_of(p) = vector[p]` for each partition, then replay
   `(vector[p], head]` exactly as today.
5. `rsize`/`rcount` are not stored: run the existing seeding
   (`recursive_size_conn(ROOT_INO)`) once after load, as `open` already
   does for the usage counter; if plan 17's virtual rsize needs per-dir
   values, compute them in the same bottom-up pass as the load (each
   `Tree` blob has its children's sizes) — do this, it is free.

Measured expectation (plan 26 Appendix): pack download for a 64 MiB
checkpoint ≈ 3.4 s Europe→us-west-2, 0.26 s in-region; the SQLite bulk
load of ~1M rows/s dominates in-region.

## Steps

Each step leaves the tree building and `cargo test --workspace` green.
Steps 1–3 are pure additions; Step 4 flips the writer; Step 5 flips the
reader; Step 6 deletes the old format.

### Step 1 — Tree format additions (`fs-core/src/tree.rs`)

- `TreeEntry`: add `ino: u64`, `ctime_ns: i64`, `rdev: u64`,
  `manifest_inline: Option<Vec<u8>>`.
- `Tree`: add `shards: Vec<ChunkHash>`; `Tree::new` rejects entries +
  shards both non-empty; `Tree::sharded(shards)` constructor.
- Change the magic to `CTR3`; delete `CTR1`/`CTR2` decode paths.
- Tests: encode/decode round-trip with every new field; sharded tree
  round-trip; decode of `CTR2` bytes is an error.

### Step 2 — Dirty tracking in the replica (`meta`)

- Schema: `inode.tree_hash BLOB`, `inode.tree_dirty INTEGER NOT NULL
  DEFAULT 1`. Bump nothing; `open` on an existing DB adds the columns
  with `ALTER TABLE` if missing (local DBs are throwaway too, but
  `ALTER` is one line and avoids remounting every dev box).
- `SqliteMeta::mark_dirty_chain(conn, dir_ino)`: `UPDATE inode SET
  tree_dirty=1 WHERE ino=?` then walk `dentry_by_ino` → parent until a
  row is already dirty or `ROOT_INO`. Call it from every mutating path in
  `mutate.rs` and `replay.rs::apply_one` — enumerate the `LogRecord`
  variants and state for each which directories get dirtied (table in a
  doc comment on the fn). `Atime` records do **not** dirty (atime is not
  in the tree; if it must be, add it to `TreeEntry` first — decide and
  document; recommended: include `atime_ns` and dirty on `Atime` only
  when the replica's atime mode is on).
- `SqliteMeta::tree_children(ino) -> Vec<TreeChild>` — like
  `snapshot_children` but returning every field `TreeEntry` needs
  including `tree_hash`/`tree_dirty` of subdirectories.
- `SqliteMeta::set_tree_hashes(&[(ino, hash)])` — batch UPDATE that only
  clears `tree_dirty` for rows whose `tree_dirty` was 1 at build time
  and no mutation happened since (see Design; simplest correct approach:
  the build reads `tree_dirty` as `dirty_gen INTEGER` — replace the
  boolean with a monotonically increasing per-row generation, and the
  UPDATE is `SET tree_hash=?, clean_gen=dirty_gen WHERE ino=? AND
  dirty_gen=?`; "dirty" ≡ `dirty_gen != clean_gen`). Prefer this
  generation scheme; it removes the race by construction.
- Tests: create → parent chain dirty to root; second create under the
  same dir does not re-walk (assert with a counter or by pre-cleaning an
  ancestor and checking it stays clean when a deeper ancestor is already
  dirty); replay of a foreign `Rename` dirties both parents; a mutation
  between build-read and `set_tree_hashes` leaves the row dirty.

### Step 3 — Builder + pack writer (`cli/src/checkpoint.rs`, new)

- `CheckpointBuilder { meta, store, e2e }` with
  `build(&self) -> Result<Built { root, sidecar, vector, blobs: Vec<Blob> }>`
  per Design (single read transaction; recursion only into dirty dirs;
  sharding above `TREE_FANOUT`).
- `Packer`: takes `Vec<Blob>`, returns `Vec<Pack { hash, body, entries }>`
  and the pack index blob. Target pack size env
  `CONSTELLATION_CHECKPOINT_PACK_BYTES`, default `1 << 20`.
- `publish(&self, built) -> Result<PublishOutcome>`: PUT packs
  (conditional create, `buffered(io_concurrency)`), PUT sidecar/index
  packs the same way, then CAS `ROOT` per Design. Return
  `Published { root, packs_uploaded, bytes }` or `Refused(StaleVector)`.
- `snapshot.rs`: `SnapshotManager::create` calls `CheckpointBuilder`
  for the subtree root (build is parametrized by root ino), stores
  `root_hash` and `packs` in `SnapshotRecord`, and **deletes**
  `build_tree`. `load_tree`/`load_manifest`/`walk_refs` learn to resolve a
  blob hash through a pack index (in-memory `HashMap<ChunkHash,
  (pack, off, len)>` built from the record's index; fetch the pack range
  with `get_range`).
- Tests (in-process, `InMemory`): build twice with no change → second
  build stages 0 blobs and publishes only `ROOT`; touch one file at depth
  5 → exactly 6 blobs staged (5 dirs + root) and ≤ 1 new pack; a
  directory with 10k entries shards into 256 buckets and round-trips;
  `StaleVector` refusal when a second publisher has a lower vector;
  E2E: pack bodies contain no filename plaintext.

### Step 4 — Flip the writer (`shipper.rs`)

- `Shipper::checkpoint` builds and publishes through `CheckpointBuilder`.
  `snap.len()` for plan 26's `last_ckpt_bytes` becomes **the total bytes
  of blobs staged** (so the proportional trigger compares log bytes with
  what a checkpoint actually costs now — for an idle tree that is
  ~0, and the `CHECKPOINT_EVERY` floor governs).
- Delete `SqliteMeta::snapshot` (the `VACUUM INTO`), the
  `constellation-ckpt-*` temp file handling, and the
  `put_checkpoint_with_vector` / `CheckpointRef` / `CheckpointVector` /
  `LATEST` / `VECTOR.json` code in `log.rs` (Step 6 collects the rest).
- Every partition holder triggers; `Refused(StaleVector)` is logged at
  `debug!` and resets nothing (the trigger fires again next time).

### Step 5 — Flip the reader

- `shipper::bootstrap` per Design (index → packs → bulk load → tail).
  Put the loader in `crates/meta/src/load.rs`:
  `SqliteMeta::bulk_load_tree(root: &Tree, resolve: impl Fn(&ChunkHash)
  -> Result<Tree>, sidecar: &Sidecar)`; drop/recreate indexes inside it;
  set `tree_hash`/generations as clean.
- `gc.rs`: new pack liveness rule per Design; remove
  `superseded-checkpoint`; log retention reads the vector from `ROOT`.
- `fsck`: add a check that every pack named by `ROOT`'s index exists
  (HEAD) and that decoding the root blob succeeds.

### Step 6 — Delete the old format

Grep for `LATEST`, `VECTOR.json`, `checkpoints/p0`, `layout::checkpoint`,
`CheckpointRef`, `CheckpointVector`, `put_checkpoint`, `get_latest_checkpoint`,
`get_checkpoint_vector`, `snapshot()` (the meta fn), and remove every
remaining use. `layout::checkpoint` becomes `layout::checkpoint_root()`
and `layout::checkpoint_pack(hash)`. Update `docs/explanation/DESIGN.md`
§2 layout block? **No** — CONVENTIONS rule 5: record the layout change in
`PROGRESS.md` for the spec owner.

### Step 7 — Config & docs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_CHECKPOINT_PACK_BYTES` | `1048576` | target pack size |
| `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY` | `8` (plan 26) | parallel pack GET/PUT |
| `CONSTELLATION_TREE_FANOUT` | `4096` | entries per directory blob before HAMT sharding |

`docs/reference/configuration.md` and a new
`docs/reference/features/checkpoints.md` describing the object model,
the vector-monotonic publish rule, and how to inspect a `ROOT` by hand.

## Tests

Unit/integration (in-process):
- Everything listed under Steps 1–3.
- Bootstrap round-trip: build a 50k-file tree with hard links, symlinks,
  mknod, xattrs, a 10k-entry directory, orphans held, two partitions, a
  pending xpart; publish; bootstrap a fresh `SqliteMeta`; assert
  table-by-table equality with the source **excluding** node-private
  tables and `tree_*` columns, then assert `tree_hash` of the root equals
  `ROOT.root`.
- Bootstrap then tail: publish at vector `{p0: 100}`, ship 50 more
  segments, bootstrap, assert convergence.
- Two holders (p0 on A, p1 on B) both checkpoint; the second publisher
  sees `StaleVector` when behind and succeeds after tailing; a third node
  bootstraps from whichever `ROOT` won and converges.
- Snapshot create twice with one change → second upload is O(spine)
  (count PUTs).
- GC: after 3 publishes, packs referenced only by the oldest `ROOT` are
  marked; packs referenced by a snapshot are not.

Harness (`scenarios.rs`):
- `merkle_ckpt_ingest_o_change`: ingest N files, then touch 10 files;
  assert the second checkpoint's uploaded bytes < 1% of the first.
- `merkle_bootstrap_far`: latency toxic 200 ms; fresh node bootstraps a
  ~100k-file tree; assert wall time ≤ 2× the in-process pack download
  estimate from the Appendix and that the tree matches the oracle.
- Rerun plan 26's `ckpt_bulk_ingest_bounded` and
  `multi_partition_retention_is_per_partition` unchanged.

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus paste: first vs second
checkpoint bytes for the ingest scenario; bootstrap wall time and pack
count for the far scenario; the pjdfstest tally (must stay full);
confirmation that `getattr` p99 during a checkpoint build is unchanged
(the `VACUUM INTO` stall of 23 ms noted in `replay.rs` is gone; nothing
new may take its place).
