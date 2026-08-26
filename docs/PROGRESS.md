# Progress

Status of the implementation against ROADMAP.md. Updated as work lands.

## Phase 1 — Single-node FUSE on S3: **IN PROGRESS (~90%)**

### Done

| Item | State | Where |
|---|---|---|
| Workspace scaffolding, shared deps | done | `Cargo.toml`, `crates/*` |
| Chunking math, manifest model (inline + spilled chunk lists) | done | `fs-core::{chunk,manifest}` |
| Core types (inodes, attrs), chunk-size validation | done | `fs-core::types` |
| LRU disk cache, reserve-before-accept, dirty/pinned states | done | `fs-core::cache` |
| Codec registry (raw/zstd + incompressible guard) | done | `store-s3::codec` |
| Self-describing chunk object format | done | `store-s3::format` |
| S3 key layout (chunks, log, checkpoints, leases, holds) | done | `store-s3::layout` |
| Chunk store: put/get, hash verification, `fs meta` CAS create | done | `store-s3::store` |
| Backend capability probe (create-if-absent, etag CAS) | done | `store-s3`, `doctor` |
| SQLite metadata engine behind `MetaStore` trait | done | `meta::sqlite` |
| Journaled mutations (log records in same tx as namespace change) | done | `meta::{record,sqlite}` |
| Orphan inodes (unlink-while-open), reap on last close | done | `meta`, `cli::fusefs` |
| CLI: `fs create`, `mount`, `doctor`, `status` | done | `crates/cli` |
| Backend URLs: `s3://` (incl. custom endpoint) and `file://` | done | `cli::backend` |
| FUSE ops: lookup/getattr/setattr/readdir/read/write/create/ mkdir/unlink/rmdir/rename/symlink/readlink/truncate/fsync/ mknod (fifo/socket/dev)/link (hard links) | done | `cli::{fusefs,fusefs_ops}` |
| POSIX semantics: rename kind checks + subtree cycle + same-inode no-op, ENAMETOOLONG, atime (noatime-style), parent mtime+ctime on namespace ops, truncate mtime | done | `meta::sqlite`, `cli::fusefs_ops` |
| Test framework: smoke test, floci S3 emulator, containerized FUSE lane, GitHub Actions CI (fmt/clippy/unit/integration) | done | `tests/`, `docker-compose.yml`, `.github/workflows/ci.yml`, docs/TESTING.md |
| pjdfstest compliance lane | done — **8798/8798 pass, empty baseline** | `tests/compliance.sh`, `tests/pjdfstest-baseline.txt` |
| Stress lane: fio write-verify + stress-ng metadata churn | done | `tests/stress.sh` |
| Fault-injection harness: floci+toxiproxy orchestration, seeded workloads, model-oracle verification | done | `crates/harness`, docs/TESTING.md |
| Metadata log shipping to S3 (CAS segments, crash-safe ack, duplicate-writer detection) | done | `store-s3::log`, `cli::shipper` |
| Metadata checkpoints (DB snapshot + LATEST pointer, auto every 32 segments + on unmount) | done | `meta::replay`, `cli::shipper` |
| Fresh-node bootstrap (checkpoint restore + log replay, ino continuation) | done | `cli::shipper::bootstrap`, `meta::replay` |
| kill -9 + remount recovery | done — harness `kill9-remount` scenario | `crates/harness` |
| Prefetcher: sequential readahead, in-flight dedup with the read path | done — harness `readahead` scenario proves pipelining under injected latency | `cli::prefetch` |
| Control API skeleton + spool observability | done — unix-socket JSON API (`status --state-dir`), backlog/error asserted by the `s3-outage` scenario | `crates/api`, `cli::main` |
| Census-scale import benchmark | done — `harness bench` (files/fanout/size/budget configurable) | `crates/harness/src/bench.rs` |

Unit tests: 48 across fs-core / store-s3 / meta / cli. End-to-end smoke
test green on both the local-file backend and floci S3 (host and fully
containerized lanes). pjdfstest: full pass, no exclusions. Harness: 9
fault-injection scenarios green (S3 outage/flap/latency/bandwidth,
crash+remount, cold cache, fresh-node bootstrap).

### Remaining for phase 1

Nothing — see the exit criteria below.

### Phase 1 exit criteria (ROADMAP.md)

- [x] pjdfstest passes — full pass (8798/8798), empty failure baseline
- [x] kill -9 + remount recovers — harness `kill9-remount` scenario verifies
      committed state survives SIGKILL across three crash/remount rounds
- [x] census-scale import measured — `harness bench` baseline (20k x 4 KiB
      files against local floci S3): import ~545 files/s durable-in-S3,
      metadata walk ~418k files/s, cold read-back ~31k files/s. The
      import rate is serial-flush bound (one chunk PUT + manifest per
      close through the single-threaded FUSE loop); parallel writeback
      is the known phase-2+ lever.

**Verdict: phase 1 is functionally complete.** Durability (metadata log
+ checkpoints + fresh-node bootstrap), crash recovery, readahead, spool
observability, and the benchmark baseline are all in and verified by
the oracle-based harness.

## Known design-debt in the current code (fix within phase 1)

- `flush_inode` re-cuts only the final chunk on shrink; a truncate to a
  non-chunk boundary followed by extension needs a targeted test.
- Writes buffer dirty chunks in memory per inode until flush; fine for
  phase-1 file sizes, but the eager-upload path (DESIGN.md streaming
  writes) is not implemented yet.
- `setattr` journals a redundant record when invoked for truncate (size
  is also recorded by the subsequent `write_manifest`).

## Phase 2 — Second node, close-to-open: **DONE**

| Item | State | Where |
|---|---|---|
| Node registry: CAS-claimed cluster-unique node ids | done | `store-s3::nodes` |
| Node-scoped ino allocation (`prefix << 40 \| counter`) — concurrent nodes can never collide | done | `meta::sqlite` |
| Segment envelope (`{v, node, records}`, versioned; legacy bare arrays decode as node 0) | done | `cli::shipper` |
| Log tailing + replica application (poll + on-demand, contiguous-run ordering) | done | `cli::shipper::Shipper::sync` |
| Convergent replay: last-wins upsert semantics, idempotent under checkpoint/segment overlap | done | `meta::replay` |
| Leaseless conflict handling: foreign records touching pending local state are skipped (ours are later in the log and win everywhere); counted + logged, surfaced via control API | done | `meta::replay::TouchSet`, `cli::shipper` |
| Multi-writer CAS collision: loser re-tails and retries at the next sequence | done | `cli::shipper` |
| Crash-atomic ack (journal ack + applied-seq in one tx) | done | `meta::sqlite::ack_journal_at` |
| Publication point: close() nudges an immediate sync round | done | `cli::fusefs_ops` |
| fsync modes: `--fsync-mode local` (default) / `s3` (fsync blocks until the record is durable in the shared log) | done | `cli::{main,fusefs}` |
| Checkpoints strip node identity (a restored node claims its own id/counter) | done | `meta::replay::snapshot` |
| Two-node convergence unit tests (disjoint, same-name conflict, sequential cross-edits, lost-response recovery) | done | `cli::shipper` tests |
| Harness scenarios: `two-clients-shared`, `git-workflow` | done | `crates/harness` |

### Phase 2 exit criteria (ROADMAP.md)

- [x] Two nodes mount concurrently with close-to-open semantics through
      S3 alone (no P2P) — `two-clients-shared` model-verifies each
      node's subtree through the *other* node's mount every block
- [x] Simulation layer green for two-node histories — in-process
      convergence tests over a shared in-memory object store cover
      disjoint writes, same-name conflicts (deterministic last-wins,
      never silent), sequential cross-node edits, and lost-response
      recovery
- [x] git-workflow scenario clean — stage/publish/edit ping-pong with
      exact content verification in both directions

Known phase-2 scope limits (by design, lifted in phase 3): concurrent
conflicting writes to the *same* dentry/inode from two nodes resolve
deterministically (log order) but are only detected+logged, not
prevented — leases make them impossible in the default mode; attribute
and entry invalidation is TTL-based (1 s) rather than push-based, so
cross-node visibility is sync-interval + TTL, not gossip-RTT.

## Phase 3 — Leases + P2P fast path: **IN PROGRESS**

### M3.1 — single-partition (p0) lease-based write authority

| Item | State | Where |
|---|---|---|
| Lease object (`{v, partition, holder, epoch, expires_unix_ms, released}`) + `LeaseStore` CAS create/renew/release over `PutMode::Create`/`Update{etag}` | done | `store-s3::lease` |
| `LeaseMode::SingleWriter` fallback for backends without `If-Match` (e.g. `file://`): swaps degrade to unconditional PUTs, exclusion assumed not enforced, loud warning on mount | done | `store-s3::lease`, `cli::main` |
| `LeaseKeeper` state machine: classify → commit, renew at half-TTL, idle release (`CONSTELLATION_LEASE_IDLE_RELEASE_MS`, default 2000 ms), deposition detection (renew CAS failure → permanently `lost`) | done | `cli::lease` |
| Takeover ordering enforced structurally: a handover claim requires a `TailedToHead` witness that only the shipper's tail can mint | done | `cli::lease`, `cli::shipper::acquire_lease` |
| `LeaseView` lock-free snapshot (atomics) for the FUSE write gate — nanosecond fast path when held, hands off to the sync task only when not currently usable | done | `cli::lease`, `cli::fusefs` |
| FUSE write gate: every mutating op (not reads) blocks on `require_lease()`, bounded wait ~2×TTL then EIO; a deposed node refuses immediately | done | `cli::fusefs`, `cli::fusefs_ops` |
| Segments stamped with the lease epoch; shipping refused without a usable lease; a segment with an epoch below the max observed is a fencing violation and is skipped, not applied | done | `cli::shipper` |
| Sync task: `Acquire` requests preempt an in-flight ship/renew round (nested `select!` with a pending-request queue) so a FUSE thread waiting on the lease is never stuck behind S3's multi-minute retry budget | done | `cli::main` |
| Control API: `StatusReport.lease: {held, holder, epoch, expires_in_ms, lost}` | done | `crates/api` |
| Harness: `Client::pause()`/`resume()` (SIGSTOP/SIGCONT), `lease-handover` (cooperative idle-release handover, epoch advances, zero conflicts), `lease-fencing` (SIGSTOP the holder, peer takes over after TTL expiry, resumed holder detects deposition and refuses to ship) | done | `crates/harness` |
| Unit tests: CAS create/renew/expired-takeover/epoch-bump/release/CAS-conflict-loser (`store-s3::lease`), acquire-before-ship/refuse-when-deposed/epoch-in-envelope/takeover-tail-ordering (`cli::shipper`) | done | `store-s3::lease`, `cli::shipper` tests |

### Phase 3 exit criteria (ROADMAP.md)

- [x] Single-authority invariant holds under simulated partitions — the
      `lease-fencing` harness scenario freezes the holder (SIGSTOP),
      lets a peer take over after TTL expiry (only after tailing the
      frozen holder's flushed log), and verifies the resumed holder
      detects its own deposition, refuses to ship, and that the
      deposed node's stranded journal never reaches the shared log or
      corrupts the new holder's namespace. `two-clients-shared` and
      `git-workflow` both assert `conflicts == 0` on every node: the
      leaseless convergence path is unreachable in normal operation.
- [ ] Lease transfer ~1 RTT when peers connected — not yet: no P2P
      exists (M3.3). Today's handover latency is the idle-release
      window (default 2 s) plus one CAS round trip, not a gossip push;
      `lease-handover`'s wall-clock is the baseline to compare once
      P2P lands.

### M3.1 scope limits (by design, addressed in later M3.x / phase 4)

- **Handover latency**: without P2P a holder cannot know a peer is
  waiting, so cooperative hand-back is time-based (write-idle for
  `CONSTELLATION_LEASE_IDLE_RELEASE_MS`), not push-based. A peer
  contending for a busy lease otherwise waits out the full TTL.
- **Stranded-journal reintegration**: a deposed holder's unshipped
  journal is preserved on local disk but has no path back into the
  shared log. Reintegration is phase 4 scope.

### M3.2 — partitions (automatic split/merge, cross-partition rename)

| Item | State | Where |
|---|---|---|
| Partition map table `partition(id, root_ino)` mutated by `PartSplit`/`PartMerge` log records; `partition_of(ino)` walks to the nearest root (cached, invalidated on split/merge/rename) | done | `meta::sqlite`, `meta::record`, `meta::replay` |
| Split record on the **parent** stream; child stream starts empty at seq 1. Merge on the surviving parent stream. Child stream `seal()` marker | done | `meta::replay` module doc, `store-s3::log`, `cli::shipper` |
| Per-partition journal column, `applied_seq/<part>`, `LogStore` per partition, one `LeaseKeeper` per partition (lazy) | done | `meta::sqlite`, `cli::{shipper,main}` |
| Whole-DB checkpoint under p0 plus `checkpoints/VECTOR.json` (`CheckpointVector`); bootstrap = snapshot + per-partition replay | done | `store-s3::log`, `cli::shipper::bootstrap` |
| Traffic split: a direct subdirectory of a partition root accumulating ≥ `CONSTELLATION_PART_SPLIT_OPS` (default 512) records over an **unbroken run** of ≥2 shipped segments, **and** ≥2 nodes in the registry. Intermittently written directories never split; single-node filesystems never split | done | `cli::shipper::{note_shipped,maybe_split}` |
| Idle merge: child quiet for `CONSTELLATION_PART_MERGE_IDLE_S` (default 3600); parent holder (also holding the child) journals `part_merge` and seals the child stream. Merging clears the subtree's traffic so it cannot immediately re-split | done | `cli::shipper::maybe_merge` |
| Partition ids are **node-scoped** (`p<node>_<n>`; plain `p<n>` on the genesis prefix) so two nodes splitting concurrently can never mint the same id and diverge | done | `meta::sqlite::alloc_part_id` |
| `rename_xpart` linked two-record commit, applied and journaled in ONE transaction (a crash cannot strand a plain `Rename` in place of the pair); pending table until both halves; abort of durable orphan src (`RenameXpartAbort`) by the src holder | done | `meta::{sqlite,replay}`, `cli::shipper`, `cli::fusefs_ops` |
| Control API: `StatusReport.partitions: [{id, root_path, lease}]` (serde defaults; legacy `lease` is p0) | done | `crates/api`, `cli::main` |
| A journaled batch for a partition with no lease keeper yet acquires one lazily, so a stranded child-partition journal after a remount still ships | done | `cli::shipper::ship_all` |
| Unit tests: split child stream + replica converge, merge+seal, xpart happy+abort, bootstrap vector across 3 partitions, traffic policy (arms, single-node, intermittent, file-writes, merge hysteresis), id uniqueness, stranded journal, `status` deadlock | done | `cli::shipper`, `meta::sqlite`, `api` tests |
| Harness: `partition-split`, `rename-across-partitions` | done | `crates/harness` |

### M3.2 scope limits (by design, addressed in later M3.x / phase 4)

- **Foreign-lease-driven splits**: DESIGN.md also splits when a foreign
  lease is long-lived on a subtree. That needs P2P liveness signals
  (M3.3); deferred. Today's only automatic trigger is log-traffic
  thresholds plus the ≥2-node registry heuristic.
- **Offline designation**: phase 4.
- **P2P/gossip**: M3.3.

## Later phases

Not started (phases 4–8, and M3.3 of phase 3). No code exists for
P2P/gossip, pin/offline, cooperative cache, snapshots/E2E, web UI, or
GC. M3.2 (partitions) is in the working tree — see the Phase 3 table
above.
