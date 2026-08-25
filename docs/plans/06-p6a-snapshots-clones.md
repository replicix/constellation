# Plan 06 — Phase 6a: snapshots, clones, subtree mounts

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–05
committed. Spec: `docs/DESIGN.md` §13 "Snapshots and Clones" —
follow it precisely; the section is prescriptive. Log ops
`snap_create`, `snap_delete`, `clone` are already reserved in the op
registry (§4).

## Step 1 — Tree objects

- New module `fs-core/src/tree.rs` (or `store-s3` if it fits better —
  decide and document): a snapshot serializes a subtree's metadata
  into content-addressed **tree blobs** stored through the chunk
  store (they are chunks; same put/get/verify path, same GC story).
- Blob format (versioned, self-describing header like chunk objects):
  a directory blob lists entries `{name, kind, mode, uid, gid, size,
  mtime_ns, target?, manifest_or_tree_hash}` sorted by name; a file's
  entry embeds its manifest bytes' hash (put the manifest itself as a
  blob when it exceeds the inline threshold — mirror the manifest
  spill logic). Directory blob hash = blake3 of the encoded blob, so
  unchanged directories hash identically across snapshots (the
  "changed spine only" property — test it).
- `snaps/<snap-id>.json`: `{v, name, path, created_unix_ms, creator,
  root: tree_hash}`. snap-id = hash of `path@name` (stable); CAS
  create so duplicate names fail cleanly.

## Step 2 — snapshot create / ls / delete

- `constellation snapshot create <path>@<name>` (CLI → control API →
  daemon): under the subtree's write lease, flush pending state,
  build the tree (walk the replica: one indexed query per dir), put
  new blobs (skip existing hashes — that IS the dedup), write
  `snaps/` object, journal `snap_create {path, name, root_hash}`.
  The record makes the snapshot visible to every node at a
  well-defined log prefix.
- `snapshot ls [path]`, `snapshot delete <path>@<name>` (`snap_delete`
  record + remove `snaps/` object; courtesy gossip warning if mounted
  elsewhere per DESIGN — implement the warning if gossip mount-state
  is cheap, otherwise note it deferred).
- Replay: `snap_create`/`snap_delete` update a `snapshot` table in
  the replica (id, path, name, root_hash, created) — that table is
  what `.constellation/` lookups and `snapshot ls` read.

## Step 3 — `.constellation/snapshot/<name>/` synthetic tree

- Every directory resolves a synthetic child `.constellation`
  (**hidden from readdir, visible to explicit lookup** — DESIGN §13;
  pjdfstest must stay green, watch for tests that enumerate).
- Under it: `snapshot/` listing snapshots whose path covers this
  directory; under each name, the frozen tree served from tree blobs
  via the normal read path (chunk cache + prefetcher work unchanged
  because data chunks are ordinary chunks).
- Implementation: synthetic inode range (e.g. top bit set) mapped by
  a resolver in `fusefs.rs`; snapshot dentries/attrs come from tree
  blobs, decoded on demand and LRU-cached in memory. Everything is
  read-only (EROFS on any mutation).
- Deleting a snapshot flips open views to ESTALE for new operations
  (drop the resolver's mapping when the `snap_delete` record
  applies).

## Step 4 — Clones

- `constellation clone <path>@<name> <new-path>`: journal a `clone`
  record; the new subtree's metadata is **copied lazily**: the clone
  root starts as a stub pointing at the snapshot tree hash; directory
  materialization happens on first write beneath a stub dir
  (copy-on-write of the metadata spine — data chunks are never
  copied, manifests reference the same hashes). Reads through a stub
  resolve directly from tree blobs.
  If lazy materialization proves too invasive for the SQLite replica
  model, the acceptable fallback is EAGER metadata copy (walk the
  tree, insert rows, journal one `clone` record; data still shared).
  Choose based on effort, document the choice in PROGRESS.md — the
  DESIGN allows "metadata copied lazily" as an optimization, and
  correctness comes first.
- Clones are ordinary subtrees afterwards: leases, snapshots of
  clones, everything applies.

## Step 5 — Subtree and snapshot mounts

- `constellation mount --s3 ... <inner-path> <mountpoint>`: root the
  FUSE view at any path (resolve at mount, re-resolve on remount;
  ESTALE if deleted later).
- `<path>@<name>` mounts the snapshot read-only; `--rw` auto-creates
  a clone (named via `--clone-name`, or `--ephemeral` = deleted on
  unmount).
- One daemon per (bucket,prefix) serving multiple mounts (DESIGN §13)
  is a bigger refactor — implement only if the current
  one-process-per-mount model makes any test impossible; otherwise
  each mount stays its own process sharing the state dir READ paths
  safely (SQLite WAL allows multi-process readers; writers need the
  lease anyway). Document what you did in PROGRESS.md.

## Step 6 — GC roots (bookkeeping only)

Full GC is plan 09. Here: make snapshot trees enumerable as GC roots
(a `constellation debug snap-refs <snap-id>` walking a tree and
listing referenced chunk hashes — the GC plan builds on it) and make
sure `snap_delete` leaves the blobs alone (grace-period reclamation
is GC's job).

## Tests

Unit: tree blob encode/decode round-trip; unchanged-subtree hash
stability (snapshot twice, count new blobs = changed spine only);
clone COW (write to clone does not touch snapshot tree). Harness:
- `snapshot-lifecycle`: create files; snapshot; keep writing; verify
  `.constellation/snapshot/<name>/` serves the FROZEN content on both
  nodes while live content moved on; delete the snapshot; verify
  ESTALE on the view and live tree unaffected.
- `clone-workflow`: snapshot, clone, diverge both live and clone,
  model-verify both trees on both nodes; delete origin snapshot;
  clone unaffected.
- `snapshot-mount`: mount `<path>@<name>` read-only (mutations
  EROFS), mount `--rw --ephemeral`, write, unmount, verify the
  ephemeral clone is gone.

## Gates + report

Per CONVENTIONS.md. pjdfstest is the canary for the synthetic
`.constellation` entry — zero regressions allowed.
