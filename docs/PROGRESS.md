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

## Later phases

Not started (phases 3–8). No code exists for leases, P2P,
pin/offline, cooperative cache, sharing/snapshots/E2E, web UI, or GC.
