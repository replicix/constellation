# Plan 29 — fjall 3 as the node-local metadata engine

Read `docs/plans/v1/CONVENTIONS.md` and plan 28 first. **Execution
override (user, 2026-09-22):** milestones are executed by subagents,
verified by the coordinator, and **committed per milestone** (the
"do not commit" ground rule does not apply to this plan). No backwards
compatibility is required anywhere: on-disk formats, bucket objects,
CLI flags and tests may change; drop tests that no longer apply, adapt
the rest, add new ones.

## Why

`bench/enginebench/RESULTS.md` (2026-09-22) compared SQLite (today's
engine), the plan 28 §11b mtree-as-local-engine design, redb and fjall on
the §P6 key encoding, aged by 15M mutations. At the realistic regime
(5M entries, 256 MiB cache ≪ DB) only fjall kept scaling reads across
cores (3.05M/s fresh, 1.5M/s aged at 32 threads, against ~0.1M/s for
SQLite and a *falling* curve for mtree), and it wrote 16× less per
mutation than SQLite. `readdirplus` costs the same as `readdir` with the
§P6 dentry attr copy; SQLite pays a join. So the local engine becomes
fjall, storing the §P6 encoding directly, while the **bucket format stays
exactly plan 28's** (prolly tree + packs + commit chain): the node's live
state and the published tree then share one key/value encoding, and a
publish is a delta of changed keys rather than a translation.

## fjall 3 features used (verified in `/tmp/fjall`, v3.1.10)

- `SingleWriterTxDatabase`: one write transaction at a time with
  read-your-writes, non-blocking snapshot readers. Same serialization
  model as today's SQLite writer mutex, so every "namespace change +
  journal row in one transaction" invariant carries over.
- `Snapshot` (seqno MVCC): replaces `with_reader` + `read_consistent`.
  A publisher/fsck/bootstrap-tail reads one consistent view without a
  transaction on the writer.
- Keyspaces with cross-keyspace atomic commits (tx and `WriteBatch`):
  namespace, journal, node-local state and derived indexes live in
  separate LSM trees with tuned options, committed together.
- Per-keyspace options: point-read-heavy namespace (`expect_point_read_hits`,
  filter policy, restart interval for prefix scans), prefix-scanned
  journal/indices without filters, pinning/partitioning of filters and
  indexes for large DBs.
- Durability: `PersistMode::Buffer` per commit (process-crash safe, same
  as SQLite `synchronous=NORMAL` in WAL), `persist(SyncAll)` at fsync and
  shutdown barriers.
- Bulk `start_ingestion` for bootstrap-from-commit.
- Compaction filters where a derived/local keyspace needs lazy expiry
  (e.g. shadow records, expired dispositions).
- Key-value separation only if large values (spilled xattrs/manifests)
  measurably hurt; off by default.

**Configuration** (from `RESULTS.md` "fjall 3 tuning", validated at
5M entries / 256 MiB: aging 259 s vs fjall 2's 342 s, aged getattr
5.0 µs vs 5.2 µs fresh, no latency cliff, peak RSS 3.3 GiB). The v3
defaults are **not** usable here: `worker_threads` defaults to
`min(cores, 4)`, compaction falls behind under churn, L0 crosses the
20/30-run write stall/halt thresholds and aged point reads fall off a
37× cliff. Use:

- `Database::builder(..).cache_size(CONSTELLATION_META_CACHE_BYTES,
  default 256 MiB).worker_threads(16-ish, scaled to cores)`;
- namespace keyspace: `expect_point_read_hits(true)`,
  `data_block_hash_ratio_policy(HashRatioPolicy::all(0.5))`,
  `filter_block_pinning_policy` and `index_block_pinning_policy`
  `PinningPolicy::new([true, true, true, false])` (L0–L2 pinned).

Not used: optimistic transactions (writers stay serialized; commit order
must equal journal order). fjall's internal Version/SuperVersion is not a
user API; it makes flush/compaction non-blocking for readers for free.

## Milestones

Every milestone ends with fmt, clippy `-D warnings`, `cargo test
--workspace` green, a commit, and the e2e/harness scenarios it touches
run. The full expensive suite (all scenarios, pjdfstest, integration)
runs at the end.

### M0 — shrink the SQLite engine before porting it
- **M0a** Remove namespace partitions: `partition` table, `PartSplit`,
  `PartMerge`, autosplit/merge heat tracking, cross-partition rename
  (`RenameXpartSrc/Dst/Abort`, `xpart_pending`, parking), per-partition
  leases/applied vectors (single stream `p0`; commits' `applied` becomes
  the single seq). Plan 28 §P4 deletes them; autosplit is off by
  default, so no default behaviour changes. Retire scenarios that only
  test partitions.
- **M0b** Remove the legacy `VACUUM INTO` checkpoint, `CheckpointVector`,
  `checkpoints/*` and `CONSTELLATION_CHECKPOINT_SNAPSHOT`: bootstrap is
  commit-or-genesis-replay only; read-only members bootstrap from other
  writers' commits; log retention floors on the head commit only.
- **M0c** Remove the `deref` table and the `superseded-checkpoint` rule
  (plan 28 §P10): chunk GC marks live chunks from the replica's manifests
  plus snapshot trees, and candidates come from the orphan LIST pass with
  the horizon + condemned handshake. Remove dead API (`reintegrate_commit`
  and the other zero-caller methods in the inventory).

### M1 — fjall engine behind the (reduced) SqliteMeta API
New `crates/meta/src/fjall/` implementing the M0 API on a
`SingleWriterTxDatabase`, values in the §P6/mtree record encoding
(inline payloads locally; spilling happens only at publish). Port
replay (TouchSet suppression, atime max-merge, epoch fencing), mutate,
reintegrate, scratch, pins, epochs, shadow, pending_upload, chunk_ref,
atime journal, snapshots/clones, tree-builder reads, bootstrap loader.
Port the meta unit tests to the new engine; switch every caller;
delete `sqlite.rs` and the rusqlite dependency.

### M2 — publisher and bootstrap native to fjall
Dirty-key tracking written in the same transaction as each change
(replaces `Touched`-from-records), publisher reads one fjall `Snapshot`,
edits are key deltas; bootstrap loads a commit with `start_ingestion`.

### M3 — the open issues
Directory-local ino allocation (plan 28 §S1b); per-directory recursive
size without a recursive CTE (§P7); `blobs/` GC with a two-mark horizon;
wire `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S`; root-cause and fix
`atime-eventual`, `deposed-reintegration`, `chaos-ci` (create-storm
EIO), `named-shared-daemon` (umount hang). Chunk GC computes liveness
from the local replica: tail the log to its head before marking and
again after the condemned-list wait, so a replica lagging behind a
writer that deduplicated against an old chunk cannot delete it. Publish
a commit on idle too (today only every 32 segments and at shutdown, so
a quiet node's head commit, and with it the log-retention floor, lags).

### M4 — decide on leaseless optimistic commits (§P3)
Design note with measurements; implement only if M3 leaves lease
serialization as the measured bottleneck.
