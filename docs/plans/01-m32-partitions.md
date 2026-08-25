# Plan 01 — M3.2: Partitions (automatic split/merge, cross-partition rename)

Read `docs/plans/CONVENTIONS.md` first. Prerequisite: plan 00 done
(leases verified and committed). Spec: `docs/DESIGN.md` §4
"Partitions", §4 "Metadata log" (per-partition streams), and the log
op registry (`part_split`, `part_merge`, `rename_xpart`).

## Goal

A filesystem starts as one partition (`p0`) rooted at `/`. Make the
metadata plane genuinely multi-partition: per-partition log streams,
checkpoints, and leases; automatic split/merge at directory
boundaries; cross-partition rename via a linked two-record commit.
No P2P (that is M3.3); no offline designation (phase 4).

This is the largest structural change since phase 1. Work in the
order below — each step compiles and passes unit tests before the
next.

## Step 1 — Partition map as replicated metadata

- New log records: `PartSplit { part, at_ino, new_part, time_ns }`,
  `PartMerge { part, into_part, time_ns }` (extend
  `meta::record::LogRecord`; replay in `meta::replay`; TouchSet
  entries). A partition is identified by a string id (`p0`, `p1`, ...;
  allocate `p{n}` with n monotonically from a kv counter).
- The **partition map** lives in the SQLite replica (new table:
  `partition(id TEXT PRIMARY KEY, root_ino INTEGER NOT NULL)` plus a
  reverse index) and is mutated ONLY by replaying `part_split` /
  `part_merge` records. Every replica converges on the same map
  because the records flow through the log like everything else.
  Bootstrapping keeps working (records replay; checkpoint carries the
  table).
- `SqliteMeta` gets `partition_of(ino) -> PartitionId`: walk up the
  dentry tree to the nearest partition root (cache the resolution;
  invalidate on split/merge/rename). Root of `/` is always `p0`.
- **Which stream carries a split record?** The PARENT partition's
  stream carries `part_split` (the child stream starts empty at seq 1
  after the split record is durable). `part_merge` is carried by the
  surviving (parent) stream. Document this in the module doc.

## Step 2 — Multi-stream shipper

Generalize `cli/src/shipper.rs` from the hardcoded `p0`:

- `LogStore` already takes a partition string in its key helpers —
  lift the `PARTITION` const into a per-instance field.
- The journal must record which partition each record belongs to
  (compute at journaling time from the mutated inode/dentry; add a
  `part` column to the journal table). `take_journal` groups by
  partition; the syncer ships each partition's batch to its own
  stream, holding that partition's lease (one `LeaseKeeper` per
  partition, lazily created).
- Tailing: track `applied_seq` per partition (kv key
  `applied_seq/<part>`); tail all partitions in the map. Checkpoints
  stay per-partition (`checkpoints/<part>/...`) and cover only that
  partition's stream; the DB snapshot itself is whole-DB — a
  checkpoint for part X asserts "this snapshot covers X up to seq N
  and every other partition up to its recorded applied_seq". Simplest
  correct approach: keep ONE checkpoint (under `p0`'s key space, as
  now) recording the full applied_seq vector in a small JSON sidecar
  (`checkpoints/VECTOR.json`); bootstrap = snapshot + per-partition
  replay from the vector. Implement that.
- Sequence-number CAS conflict handling stays per-stream and
  unchanged.

## Step 3 — Automatic split/merge policy

Per DESIGN.md the triggers are: offline designation (phase 4 — skip),
long-lived foreign leases, and log traffic thresholds. Implement the
traffic trigger now, in the syncer:

- Maintain a per-directory write-op EWMA on the lease-holding node
  (cheap: count journaled records per top-level-directory-under-
  partition-root as they are journaled).
- Split when a direct subdirectory of a partition root sustains more
  than `CONSTELLATION_PART_SPLIT_OPS` (default 512) records across at
  least two consecutive shipped segments AND another node is actively
  tailing (visible: foreign segments in other partitions, or simply
  more than one node registered — keep the heuristic simple and
  documented; single-node filesystems must never split).
- Merge when a child partition has shipped no records for
  `CONSTELLATION_PART_MERGE_IDLE_S` (default 3600; use a small value
  in tests) — the holder of the PARENT partition performs the merge
  (acquiring the child's lease first, then `part_merge`, then the
  child stream is sealed: a `sealed` marker object in the child's log
  prefix; tailers treat sealed+fully-applied as removable from the
  active set).
- Splits/merges must be rare and hysteretic. Both are metadata-only
  log records; no data moves.

## Step 4 — Cross-partition rename

`rename_xpart` linked two-record commit (DESIGN.md §4):

- When rename source and destination resolve to different partitions,
  journal a pair sharing a fresh `txid`: `RenameXpartSrc { txid, part,
  from_parent, name, ino, time_ns }` in the source stream and
  `RenameXpartDst { txid, part, to_parent, new_name, ino, attr?,
  time_ns }` in the destination stream. The mutating node must hold
  BOTH partitions' leases (acquire in a canonical order — sort by
  partition id — to avoid deadlock with a concurrent renamer).
- Local SQLite apply is a single transaction as usual; the pairing
  matters for replicas/bootstrap: a replica applies the rename only
  when it has BOTH records ("both streams must contain the pair").
  Implement a small pending-xpart table: on seeing one half, park it;
  on seeing the other, apply atomically. Bootstrap replay uses the
  same path.
- **Recovery rule (must be documented in the module doc and tested):**
  a `RenameXpartSrc` whose partner never appears (writer crashed
  between the two PUTs) is resolved at the moment ANY node next writes
  to either stream: if the dst record is absent from the dst stream at
  or before the dst stream's head at that time, the orphan src record
  is voided by appending a `RenameXpartAbort { txid }` to the src
  stream (the file stays at its source). The lease + epoch machinery
  makes this race-free: only the current holder of the src partition
  may append the abort.

## Step 5 — Tests

Unit (in-process, InMemory object store — extend the two-node pattern
in shipper tests):
- split: records land in the child stream after the split point; a
  second node tails both streams and converges.
- merge: child sealed, records flow to parent, replicas converge.
- xpart rename: both halves applied on a tailing replica; abort path
  (drop the dst half before shipping, crash the writer, verify the
  next writer appends the abort and every replica keeps the file at
  the source).
- bootstrap with the applied-seq vector across 3 partitions.

Harness scenarios (`crates/harness/src/scenarios.rs`):
- `partition-split`: two nodes, one FS; node A hammers `/hot` with a
  low `CONSTELLATION_PART_SPLIT_OPS` until a split happens (observe
  via control API — expose the partition map in `StatusReport`);
  model-verify both nodes' full trees afterwards; then let `/hot` go
  idle with a tiny merge threshold and verify the merge + continued
  correctness.
- `rename-across-partitions`: force a split (env thresholds), then
  rename files/dirs between the two partitions from both nodes
  (serialized by leases), model-verify; include a kill9 of the
  renamer between operations to exercise recovery (the abort rule).

## Control API

Extend `StatusReport` with `partitions: [{id, root_path, lease:
{held, holder, epoch, expires_in_ms}}]` (serde defaults). Update the
`status` output.

## Out of scope

P2P/gossip (M3.3), offline designation + reintegration (phase 4),
foreign-lease-driven splits (needs P2P liveness signals; note as
deferred in PROGRESS.md).

## Gates + report

Per CONVENTIONS.md. All existing scenarios must stay green — the
single-partition fast path must not regress (a single-node FS never
splits; verify pjdfstest + bench are unchanged).
