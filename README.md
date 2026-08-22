# Constellation

A distributed POSIX file system on S3: any number of machines mount the same
bucket read/write, stay conflict-free by construction, keep working offline,
and use encrypted P2P links between peers purely as a latency fast path.
One static binary — FUSE mount, daemon, CLI, and web UI. The only
infrastructure is an S3-compatible bucket with conditional-write support.

**Status: design phase.** No functional code yet; the documents below are
the deliverable so far.

## Documentation

| doc | contents |
|---|---|
| [docs/GOALS.md](docs/GOALS.md) | goals, non-goals, motivating scenarios, prior-art critique |
| [docs/DESIGN.md](docs/DESIGN.md) | full architecture: data/metadata planes, leases, pin/offline/epochs, caching, compression, security, failure matrix |
| [docs/DECISIONS.md](docs/DECISIONS.md) | ADRs — what was rejected and why |
| [docs/TESTING.md](docs/TESTING.md) | correctness strategy: property tests, deterministic simulation, POSIX suites, e2e scenarios |
| [docs/ROADMAP.md](docs/ROADMAP.md) | phased milestones to v1 |

## Layout

```
crates/
  cli/        constellation binary (CLI + daemon + FUSE)
  fs-core/    VFS core: inodes, chunking, cache, prefetcher
  meta/       metadata replica, log, partitions, leases, authority
  store-s3/   S3 layout, CAS, codecs, GC
  net/        iroh P2P, gossip, cooperative cache, source selection
  api/        control API + embedded web UI + metrics
bench/
  dbbench/    embedded-DB benchmark behind DECISIONS.md ADR-9
```

## Building

```bash
cargo build --workspace   # scaffold only for now
cargo test --workspace
```
