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

Unit tests: 48 across fs-core / store-s3 / meta / cli. End-to-end smoke
test green on both the local-file backend and floci S3 (host and fully
containerized lanes). pjdfstest: full pass, no exclusions. Harness: 9
fault-injection scenarios green (S3 outage/flap/latency/bandwidth,
crash+remount, cold cache, fresh-node bootstrap).

### Remaining for phase 1

| Item | State | Notes |
|---|---|---|
| Control API skeleton | **not started** | CLI currently links the internals directly; the API layer (`crates/api`) is an empty stub |
| Census-scale import benchmark | **not started** | `bench/` reserved |

### Phase 1 exit criteria (ROADMAP.md)

- [x] pjdfstest passes — full pass (8798/8798), empty failure baseline
- [x] kill -9 + remount recovers — harness `kill9-remount` scenario verifies
      committed state survives SIGKILL across three crash/remount rounds
- [ ] census-scale import within budget — not measured

**Verdict: phase 1 is nearly done.** The durability story is complete
(metadata log + checkpoints in S3, fresh-node bootstrap proven by the
oracle); prefetcher, API skeleton, and the import benchmark remain.

## Known design-debt in the current code (fix within phase 1)

- `flush_inode` re-cuts only the final chunk on shrink; a truncate to a
  non-chunk boundary followed by extension needs a targeted test.
- Writes buffer dirty chunks in memory per inode until flush; fine for
  phase-1 file sizes, but the eager-upload path (DESIGN.md streaming
  writes) is not implemented yet.
- `setattr` journals a redundant record when invoked for truncate (size
  is also recorded by the subsequent `write_manifest`).

## Later phases

Not started (phases 2–8). No code exists for log tailing, leases, P2P,
pin/offline, cooperative cache, sharing/snapshots/E2E, web UI, or GC.
