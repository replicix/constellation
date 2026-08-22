# Progress

Status of the implementation against ROADMAP.md. Updated as work lands.

## Phase 1 — Single-node FUSE on S3: **IN PROGRESS (~70%)**

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

Unit tests: 44 across fs-core / store-s3 / meta / cli. End-to-end smoke
test green on both the local-file backend and floci S3 (host and fully
containerized lanes). pjdfstest: full pass, no exclusions.

### Remaining for phase 1

| Item | State | Notes |
|---|---|---|
| Metadata log shipping to S3 | **not started** | journal records exist locally, but no segment writer/uploader; layout keys reserved. Without it, namespace lives only in the local SQLite replica — a fresh node cannot reconstruct the tree from the bucket. This is the biggest gap. |
| Metadata checkpoints/snapshots (single partition) | **not started** | needed with log shipping (bounded replay) |
| Prefetcher | **not started** | sequential-read detection → readahead into cache |
| Control API skeleton | **not started** | CLI currently links the internals directly; the API layer (`crates/api`) is an empty stub |
| kill -9 + remount recovery test | **not started** | crash-consistency exit criterion |
| Census-scale import benchmark | **not started** | `bench/` reserved |

### Phase 1 exit criteria (ROADMAP.md)

- [x] pjdfstest passes — full pass (8798/8798), empty failure baseline
- [ ] kill -9 + remount recovers — untested
- [ ] census-scale import within budget — not measured

**Verdict: phase 1 is not finished.** The mount works end-to-end for a
single node, but the durability story (metadata log + checkpoints in S3)
and the exit gates are outstanding.

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
