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

## Known design-debt in the current code

- `setattr` journals a redundant record when invoked for truncate (size
  is also recorded by the subsequent `write_manifest`).

The two bullets previously recorded here — the shrink/extend truncate
gap and RAM scaling linearly with file size — are **closed by phase
5a** below.

## Phase 5a — bounded-memory write staging: **DONE**

| Item | State | Where |
|---|---|---|
| `pending_upload(hash, ino)` table, written in the **same transaction** as the `WriteManifest` journal record (`set_manifest_dirty`); `pending_uploads()`/`ack_upload()` drain API | done | `meta::sqlite` |
| `upload_dirty_chunks` drains `SqliteMeta::pending_uploads()` instead of `DiskCache::dirty_chunks()`, so the durable set — not the cache's in-memory accounting — decides what still owes S3 a PUT; a pending chunk missing from the local cache is refused loudly rather than silently dropped | done | `cli::main` |
| Clean unmount runs `upload_dirty_chunks` before `ship.shutdown_all`; a failed drain sets `skip_ship(true)` and exits non-zero instead of shipping a manifest for un-uploaded content | done | `cli::main` |
| `cli::staging`: one sparse file per open dirty inode (`<state_dir>/staging/<ino>.<gen>`), `pwrite`/`pread` only (no `mmap` — see the module doc for why), reserve-before-accept `StagingBudget`, run-length `DirtyRuns` (a few dozen bytes for a sequential append, not one entry per chunk) | done | `cli::staging` |
| FUSE write path rewired onto staging: `WriteState` holds `Staging` + `file_len` + `base: Option<Manifest>` instead of a `BTreeMap<u64, Vec<u8>>`; `flush_inode` seals one chunk at a time (read staged range → hash → `cache.insert(Dirty)` → drop the buffer), so peak RSS is `O(chunk_size)`, not `O(file_len)` | done | `cli::{fusefs,fusefs_ops}` |
| Partial writes into an untouched chunk seed the unwritten bytes from committed content first (no spurious holes); truncate maps 1:1 onto `Staging::set_len` + dirty-run retain, re-cutting the boundary chunk for free when already staged | done | `cli::fusefs_ops` |
| `CONSTELLATION_STAGING_BUDGET` (default `--cache-size / 4`, decoupled from the chunk-cache budget); reservation failure is a clean `ENOSPC` at the FUSE boundary with no partial state | done | `cli::{main,staging}` |
| `staging_bytes`/`staging_budget_bytes` on `CacheStatus`, surfaced through `status` | done | `crates/api::types`, `cli::main` |
| Mount-time GC deletes everything under `<state_dir>/staging/` before the FUSE loop starts (the generation counter guarantees nothing there can be live) and logs the reclaimed byte count | done | `cli::{staging::gc,main}` |
| `rescan_rebuilds_accounting`'s comment corrected: the cache legitimately returns `Clean` after a rescan because `pending_upload`, not the cache, is the source of truth for pending uploads | done | `fs-core::cache` |
| Unit tests: staging round-trip across chunk boundaries + sparse-hole reads, truncate-down dirty-run recut, truncate-up-then-extend hole (closes the retargeted debt bullet), budget reserve-before-accept with no partial state, sequential-append vs. fragmenting-random-write run-count bounds, mount-time GC, `discard`, `GenCounter` monotonicity | done | `cli::staging` tests |
| Unit tests: `set_manifest_dirty` same-tx atomicity (a forced bad-ino failure leaves neither the manifest nor a stray pending row), `ack_upload` per-inode isolation, `pending_uploads` survives a drop-and-reopen | done | `meta::sqlite` tests |
| Unit tests: `upload_dirty_chunks` drains a pending row even though a post-crash cache reopen reports the chunk `Clean` (regression for prerequisite 1); a failed drain (S3 unreachable) leaves the pending row for the next attempt rather than acking it (regression for prerequisite 2's unmount gate) | done | `cli::pending_upload_tests` |
| Harness scenarios: `big-file-write` (small `--cache-size`, write several times that budget, sample RSS — flat ceiling, not tracking bytes written), `staging-crash` (`kill -9` mid-write, remount, empty `staging/`, content at its last committed size), `unmount-drain` (cut S3, write, unmount, heal, remount-drain, unmount again; a second node must see no missing chunk — the regression test for prerequisite 2) | done | `crates/harness::scenarios` |

### Phase 5a exit criteria (plan 05a)

- [x] A file many times the cache budget is written with a flat RSS
      ceiling — `big-file-write` writes 300 MiB against a 64 MiB
      `--cache-size` and asserts peak RSS stays under a fixed
      `cache_size + 200 MiB` ceiling while sampling RSS throughout the
      write.
- [x] `kill -9` mid-write leaves no orphaned staging bytes —
      `staging-crash` asserts `staging/` is empty after the next
      mount's GC and the file is at (or a strict prefix of, per the
      no-fsync POSIX contract) its last committed size.

**Verdict: phase 5a is functionally complete.** This plan deliberately
does not add streaming/eager writeback, throttling short of the hard
`ENOSPC` bound, or slice overlays — all out of scope, deferred to
phase 5b (`docs/plans/05b-p5b-streaming-writeback.md`).

### Known visibility limit (not scheduled)

P2P carries no journal records: `Payload` (`net::message`) has
`SegmentPublished` (a hint to tail seq N *from S3*), lease/delegation,
epoch, digest, and chunk messages — nothing that ships records. Since
`announce_segment` fires only after a segment is durable in S3, and
epoch-mode handoff releases the lease locally without shipping
(`cli::main`, gated on `epochs.writes_ok()`), **a node's writes during a
continuation epoch are invisible to its peers until S3 returns**. The
ordered drain keeps the log linear and divergence-free, but close-to-open
(DESIGN.md §6) does not hold inside an epoch: a successor can take the
lease, read a pre-epoch version of a file the previous holder modified,
and overwrite it — a lost update, not a divergence. Peer chunk serving
also refuses `Dirty`, so the bytes are unreachable independently of the
manifest. Closing this needs a record-bearing P2P payload plus a
handoff precondition, i.e. a visibility plane rather than a write-path
change; it is deliberately out of scope for phases 5a/5b.

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

### M3.3 — P2P fast path (iroh endpoint, gossip, lease handoff)

S3 stays the source of truth and the commit point; every mechanism here
only removes waiting. `CONSTELLATION_P2P=off` disables the lot.

| Item | State | Where |
|---|---|---|
| Host Ed25519 node key (`~/.config/constellation/node.key`, 0600, `CONSTELLATION_NODE_KEY` override), generated on first mount | done | `net::identity` |
| Registry carries `pubkey` + `p2p_addr`; accept-time allowlist, so enrolment needs bucket write and IAM stays the trust root | done | `store-s3::nodes`, `net::allowlist` |
| iroh endpoint with relay and address publishing disabled — the registry is the only peer directory; addresses are injected into iroh via `MemoryLookup` | done | `net::endpoint` |
| Signed length-prefixed postcard messages, verified before use (gossip is relayed, so the transport peer is not the author) | done | `net::message` |
| Push invalidation: `SegmentPublished` gossip nudges the syncer | done | `net::peers::run_gossip`, `cli::shipper` |
| Lease handoff: requester asks the holder, holder flushes + releases, requester CAS-claims. Declining is always safe | done | `net::handoff`, `cli::main` |
| Control API `StatusReport.p2p { enabled, node_addr, peers[] }` | done | `crates/api`, `cli::main` |
| Harness: `p2p-invalidation`, `p2p-handover`, `p2p-partition-tolerance` | done | `crates/harness` |

Measured on the harness (S3 emulator, loopback P2P):

| Path | With P2P | S3 only | Bound being beaten |
|---|---|---|---|
| Cross-node visibility | **19–25 ms** | 3015 ms | 3 s sync interval |
| Lease handoff from an *active* holder | **27–33 ms** | — | 30 s idle window |

The roadmap exit criterion (lease transfer ~1 RTT when peers are
connected) therefore holds, and `p2p-invalidation` re-runs the same
workload with the kill switch to prove the S3 bound still applies.

### M3.3 scope limits

- **Cooperative chunk serving** over the endpoint is phase 5; the ALPN
  and message enum are deliberately extensible for it.
- **FUSE kernel invalidation + raised attr TTLs** (plan 02 step 3's
  second half) are not implemented: visibility is already gossip-driven,
  but the kernel attr/entry TTLs stay at 1 s rather than being raised to
  30 s with dynamic fallback. Raising them needs `fuser`'s notifier,
  which the current `mount2` call does not expose.
- **Offline designation acks**: phase 4.

## Phase 4 — Pin/offline, continuation epochs: **DONE**

### Phase 4a — pin/unpin + offline designation

| Item | State | Where |
|---|---|---|
| Cache states `{clean,pinned,dirty}` already modeled; LRU evictor already skips pinned/dirty (verified with a dedicated test, no change needed) | done | `fs-core::cache` |
| Pin registry: node-local `pin(path, ino, pinned_at)` table, NOT replicated — each node pins for itself | done | `meta::sqlite` |
| Subtree walk (`subtree_manifests`) + `pinned_ancestor` (files under a pinned dir are covered without re-pinning) | done | `meta::sqlite` |
| Admission check: estimate incoming bytes from unresident chunks × mean chunk size before reserving, refuse with an actionable message rather than half-filling a pin | done | `cli::pin::PinManager::pin` |
| Eager fetch: bounded-concurrency (8) chunk fill via a `JoinSet`, marking each `Pinned`; follows spilled chunk-list manifests too | done | `cli::pin` |
| Push-sync: pins re-filled after every successful sync round (both `Barrier` and `Nudge` paths), so a peer's write under a pinned path is fetched without waiting for a read | done | `cli::main`, `cli::pin::refresh_all` |
| Unpin demotes only chunks not covered by another still-active pin | done | `cli::pin::unpin` |
| CLI + control API: `pin`/`unpin`/`pins`, `Request::{Pin,Unpin,ListPins}`, `StatusReport.pins` | done | `crates/api`, `cli::main` |
| Designation objects `designations/<hash-of-path>.json`, CAS create/release (`DesignationStore`, mirrors `LeaseStore`'s CAS idioms) | done | `store-s3::designation` |
| Overlap check (component-aware, not string-prefix) at creation in both directions; TOCTOU close via re-list-after-create and self-delete if an older overlapping claim appears | done | `store-s3::designation` |
| Delegation grant/renew/expiry state machine (designee side: `DelegationGranter`; requester side: `DelegationHolder`), both driven by an injected clock for deterministic tests | done | `net::delegation` |
| P2P messages `DelegationRequest`/`DelegationGrant`/`FlushAck`, dispatched through the same signed-frame ALPN as the M3.3 messages | done | `net::message`, `net::peers` |
| FUSE gate: innermost covering designation resolved per mutation; designee ⇒ proceed; live delegation ⇒ proceed; else request one (bounded 2 s) ⇒ proceed or **EROFS**; `--ro` designations never gate writes | done | `cli::designation::DesignationManager::check`, `cli::fusefs::require_lease_for` |
| `constellation offline <path> [--ro]` / `online <path>`; only the designee may release (non-stealable, DESIGN.md) | done | `cli::designation`, `cli::main` |
| Control API `StatusReport.designations`, `Request::{Offline,Online,ListDesignations}` | done | `crates/api`, `cli::main` |
| Unit tests: pin admission math, pinned/dirty-skip-LRU (pre-existing + new), subtree walk, overlap rejection (both directions) incl. the TOCTOU tie-break, delegation grant/renew/expiry/per-requester isolation, decline/unrelated-message rejection | done | `fs-core::cache`, `meta::sqlite`, `store-s3::designation`, `net::delegation` tests |

### Phase 4a scope limits (honest, not silently dropped)

- **Flush-ack is verify-only, not a hard journal gate.** DESIGN.md's
  invariant is that the designee provably holds every committed change
  under its path before a foreign flush counts as published. The
  implementation asks the designee to ack a just-shipped segment
  (`DesignationManager::await_flush_ack`, wired into
  `Shipper::ship_part` as `verify_flush_acks`) — but by the time that
  runs, the segment's CAS-create has already succeeded and the log's
  exactly-once sequencing means it cannot be un-shipped without either
  double-shipping the same records under a new seq or restructuring the
  journal-ack/seq coupling that phases 1–3 rely on. A missed/timed-out
  ack is therefore logged as a warning (visible on the daemon and
  counted nowhere yet), not turned into "stay journaled and retry" as
  the plan's ideal describes. The ~1 RTT cost DESIGN.md budgets for is
  paid; the hard gate on it is not. Tightening this to a true
  pre-publish gate is follow-up work, likely needing a lease-epoch-style
  fencing token per segment rather than a boolean ack.
- **No harness scenarios yet** for `pin-follow`, `offline-designee-writes`,
  `offline-delegation` (plan 03's exact asks). The mechanisms are unit
  tested and manually verified end-to-end on a live mount (admission
  refusal, overlap refusal, `--ro`, designee-writes-through, `online`
  release-by-designee-only), and the full existing harness matrix (20
  scenarios) plus pjdfstest (8798/8798) stayed green with the new FUSE
  gate in the hot mutation path, but the specific multi-node fault
  scenarios plan 03 names (cut S3+P2P to the designee, verify EROFS
  elsewhere, heal, model-verify) are not yet automated.
- **Delegation is per-designation-path, not per-mutation-path.** A
  write under `/site/deep/nested` resolves the designation at `/site`
  and requests/holds one delegation for the whole `/site` claim, not a
  finer-grained one — matching DESIGN.md's per-path (not per-file)
  framing of the mechanism.

### Phase 4b — continuation epochs + reintegration

| Item | State | Where |
|---|---|---|
| Write-eligible roster from `NodeInfo.ro`; first-mount `--read-only-member` enrollment | done | `store-s3::nodes`, `cli::main` |
| Roster derivation **fails closed**: any unreadable/unparseable `nodes/` record aborts the roster instead of silently shrinking it (a short roster would let one node believe its component covers the cluster and open an epoch alone). `list_nodes` stays tolerant for the P2P directory, where a skipped record only costs a fast path | done | `store-s3::nodes::write_eligible_roster`, `cli::main::refresh_peers` |
| Persist-before-ack epoch promises, all-member activation, deterministic concurrent-proposer tie-break | done | `net::epoch`, `cli::epoch`, `meta::sqlite` |
| P2P-only lease handoff while S3 is unavailable; dirty data and metadata remain local | done | `cli::{main,lease,fusefs,shipper}`, `net::peers` |
| Epoch discipline: loss of any promised member freezes FUSE writes with `EROFS`; an open promise blocks S3 takeover | done | `cli::epoch`, `cli::{fusefs,lease}` |
| Ordered epoch drain: current holder uploads dirty chunks and ships first; prior holders tail, follow, release, and close locally | done | `cli::main`, `cli::shipper` |
| Reintegration: current-replica classification, clean re-journal, visible conflict materialization preserving stranded bytes and winner | done | `meta::reintegrate`, `cli::reintegrate` |
| Crash-safe/idempotent reintegration batch + persisted deposed unlock; automatic mount recovery and `constellation reintegrate` | done | `meta::sqlite`, `cli::{main,reintegrate}` |
| Persisted deposition is an authority gate, not only mount-recovery metadata: ordinary FUSE acquisition and shipper-created keepers remain tail-only while `lease_lost=1`; only explicit reintegration may temporarily acquire authority, and the bit clears only after its output ships successfully | done | `cli::{main,shipper,reintegrate}` |
| Control status for epoch and reintegration state/counters | done | `crates/api`, `cli::main` |
| Required fault scenarios | done | `continuation-epoch`, `epoch-member-lost`, `deposed-reintegration` |

### Phase 4 exit criteria / availability rows

| S3 | P2P component | Write-eligible coverage | Expected result | Proven by |
|---|---|---|---|---|
| down | healthy | all writers | writes continue under a continuation epoch, then drain without conflicts after heal | `continuation-epoch` |
| down | loses one promised member | incomplete | all survivors immediately become read-only (`EROFS`); resume/heal converges without divergence | `epoch-member-lost` |
| up | holder deposed with a stranded branch | n/a | clean records append; conflicting bytes materialize visibly while the shared winner remains | `deposed-reintegration` |

**Verdict: phase 4 is functionally complete.** The S3-down
availability rows and stranded-branch recovery are enforced in the
daemon and verified end-to-end by the three dedicated scenarios.

### Phase 4b scope limits (honest, not silently dropped)

- **Epoch liveness is poll-based.** `EpochManager::check_liveness` runs
  from the sync task, so the freeze after losing a member lands within
  one sync interval rather than instantly. The safety argument does not
  depend on the delay (the departed member is itself frozen by its own
  persisted promise, and `blocks_s3_takeover` refuses the S3 takeover
  path either way), but `epoch-member-lost` necessarily asserts the
  `EROFS` transition through `eventually()` rather than immediately.
- **`SqliteMeta::reintegrate_commit` is unused by the daemon.** The
  per-record mark-and-rejournal function plan 04 step B4 describes exists
  and is unit tested, but `cli::reintegrate` commits the whole
  reconciled batch through `commit_reintegration_batch` instead. That is
  still crash-safe — one transaction swaps the namespace, marks every
  disposition, and journals the output, so a crash leaves the stranded
  rows unmarked and the next mount redoes the batch — but the resume
  granularity is the batch, not "the first unmarked record". Worth
  collapsing to one path.

### Phase 4c — permanent roster leave

| Item | State | Where |
|---|---|---|
| Registry leave is a **tombstone** (`retired: true`, `retired_unix`), not a DELETE — numeric ids stay reserved forever so old log segments / ino prefixes never scramble onto a new host | done | `store-s3::nodes::{leave_node,claim_node_id}` |
| `write_eligible_roster` omits `ro \|\| retired`; `list_nodes` (P2P) omits retired; fail-closed roster rule unchanged | done | `store-s3::nodes` |
| Self-leave via control API: refuse open epoch; refuse live designations unless `--force`; flush + release leases; refuse stranded `lease_lost` journal (reintegrate first); tombstone; `kv_set("left","1")`; stop writes + fusermount | done | `cli::leave`, `cli::{main,fusefs}`, `Request::Leave` |
| Admin leave `leave --node-id N` from a live peer: refuse self; refuse live lease/designation unless `--force`; tombstone only | done | `cli::leave::admin_leave` |
| Remount of a left / retired id refuses with a clear error (fresh `--state-dir` to re-enroll); vanished/retired own record while mounted freezes writes (`EIO`) and persists `left=1` | done | `cli::main` |
| Status reports `enrolled: bool` | done | `crates/api::StatusReport` |
| CLI `constellation leave --state-dir … [--node-id N] [--force]` | done | `cli::main` |
| Unit + harness proofs | done | `store-s3::nodes`, `cli::leave` tests; harness `node-leave` |

**Verdict: phase 4c is functionally complete** (additive on top of 4b;
4b's availability-row verdict is unchanged). Unmount remains a
temporary departure; only explicit leave shrinks the write-eligible
roster. Spec: `docs/DESIGN.md` §1, §2 (`nodes/`), §5.3, §8
(Join / leave), §9, §10.

### Phase 5 — cooperative cache

| Item | State | Where |
|---|---|---|
| Bloom digest (~10 bits/entry, k=7, homemade double-hashing); FPR unit-tested. Caches past one 16 KiB frame split by hash prefix (`bucket_count_for`); 4 TiB → 128 buckets. Per-peer RSS cap 4 MiB. `k` clamped on the wire. | done | `net::bloom` |
| Incremental digest journal: insert/evict/state-change notes; `take_digest_events` on the 250 ms tick; overflow collapses to one snapshot. Publisher never clones the full set on the hot path. | done | `fs-core::DiskCache`, `cli::coop::DigestTracker` |
| Peer `ChunkRequest`/`ChunkResponse` on the ALPN stream, length-prefixed body (not through `MAX_FRAME`); serve clean/pinned only | done | `net::{endpoint,peers}`, `fs-core::DiskCache::get_servable` |
| Per-peer (4) + global (16) serving budget; excess → `found: false` | done | `cli::coop` |
| Streams handled concurrently per connection (bounded at 32); the chunk read runs on a blocking thread, not a runtime worker | done | `net::peers::handle_stream`, `cli::coop::serve_chunk` |
| Per-peer pooled QUIC connections; multiplexed streams; one redial through a shared gate | done | `net::endpoint` |
| Compact postcard wire for all P2P messages; gossip `max_message_size` 32 KiB with a 30 KiB content budget | done | `net::message`, `net::endpoint` |
| Latency-adaptive source selection: EWMA TTFB/goodput/err, QUIC RTT + path type, 20% hysteresis, one hedge when a fetch outlives its predicted P95 first byte **plus** predicted body time | done | `cli::sources`, wired in `cli::coop::fetch` |
| TTFB measured to the response head at both sources (`store.get`, peer control reply) so goodput is learned rather than left at its prior | done | `store-s3::get_chunk_timed`, `net::ChunkFetch` |
| Fetch path (FUSE + prefetch + pin) source-selects; blake3 verify; S3-leg-only retries; hash fail / miss / timeout → error that peer and fall back to S3 | done | `cli::{fusefs,prefetch,pin,coop}` |
| Kill switch `CONSTELLATION_COOP=off` (serving, using, and gossiping), parsed once at construction | done | `cli::coop::CoopConfig` |
| `StatusReport.coop` counters + per-source EWMAs + capacity/stale telemetry | done | `api::CoopStatus`, `cli::main` |
| Unit tests: bloom FPR, digest delta apply, selector hysteresis/hedge, frame budgets, pooling | done | `net::{bloom,message,peers}`, `cli::{coop,sources}` |
| Harness: `coop-cache-hit`, `coop-fallback`, `web-fleet`, `s3-retry` | done | `harness::scenarios` |

**Verdict: phase 5 is functionally complete.** Review remediation
closed every open finding with regression coverage. Harness counters
(seed 42):

- `coop-cache-hit`: B `peer_hits=8` `s3_fetches=0` `hedges=0–1`, read in 28–55 ms under 200 ms S3 latency (8 × 1 MiB).
- `coop-fallback`: A SIGSTOPped; B `s3_fetches=8` `hedges_fired=8` `peer_misses=0`, content hash-verified. Every fetch hedging is the *correct* reading here: the peer is frozen, so each one is genuinely late, but canceled losers are not failures.
- `web-fleet`: two cold readers × two reads; aggregate `s3_fetches=0` `peer_hits=16` against 8 unique chunks (writer already held them).
- `s3-retry`: cold remount with object_store retries off; toxiproxy cut on first GET, healed in the app retry window; hash-verified read, four logical S3 successes, no peer hits.

#### Phase 5 review remediation

The post-implementation review found the following defects and scaling
risks. They are recorded before remediation so a passing implementation
cannot erase the reason for each regression test.

| Finding | State | Required proof |
|---|---|---|
| Gossip used signed/base64/hex JSON sized against the 64 KiB direct-stream cap, while iroh-gossip retained its independent 4 KiB default; full bloom buckets and ordinary delta bursts were silently oversized | fixed | `message::{a_full_bloom_bucket_fits_the_real_gossip_budget,a_maximum_delta_batch_fits_the_real_gossip_budget}`, `peers::a_full_bloom_bucket_crosses_real_gossip` |
| The coop-enabled FUSE path bypassed the former bounded S3 retry loop | fixed | `coop::{s3_retry_succeeds_after_two_transient_failures,s3_retry_stops_after_the_bounded_attempt_count}` and harness `s3-retry` |
| A healthy primary canceled after a winning hedge was recorded as an error and peer miss | fixed | `sources::a_cancelled_race_loser_is_not_an_error_sample`; `coop-fallback` now reports `peer_misses=0` for canceled losers |
| Digest journal drains split adds/removes and lost event order within one publisher tick | fixed | `cache::digest_journal_preserves_{add_then_remove,remove_then_add}_order`, `coop::tracker_replays_*_in_order` |
| Peer digests had no `received_at` or expiry and survived peer churn indefinitely | fixed | `coop::stale_peer_digests_expire_and_old_generations_are_rejected`; holder lookup also prunes nodes absent from the live registry |
| Bucket count could flap at every power-of-two occupancy boundary, forcing repeated complete re-partitioning | fixed | `coop::bucket_count_has_downsize_hysteresis` |
| `pending_adds` used linear `Vec::retain`, was unbounded while buckets awaited snapshots, and was discarded on resize | fixed | deduplicated `HashSet`, snapshot collapse at a bounded limit; `pending_delta_overflow_collapses_to_snapshots`, `deltas_are_batched_without_losing_the_remainder` |
| Every chunk request dialed a fresh QUIC connection despite the server supporting multiplexed streams | fixed | `peers::{sequential_and_concurrent_chunks_reuse_one_connection,a_closed_pooled_connection_is_redialed_once,two_streams_on_one_connection_are_served_concurrently}` |
| Digest-log overflow behavior used a different compile-time threshold in tests than production | fixed | `DiskCache::open_with_digest_log_limit`; `cache::a_flood_of_changes_collapses_to_a_rebuild_snapshot` executes the production branch |
| Chunk requests used an undocumented hard-coded timeout inconsistent with the rest of P2P | fixed | named `CHUNK_REQUEST_TIMEOUT`; one outer timeout bounds pooled stream plus its single redial; `peers::chunk_timeout_bounds_the_wait_and_server_work_releases` |
| `CONSTELLATION_COOP` was reparsed on every hot-path fetch/serve and lacked direct regression coverage | fixed | immutable construction-time `CoopConfig`; `coop::{coop_kill_switch_values_are_parsed_once_at_construction,disabled_coop_neither_selects_nor_serves_peers}` |
| Saturating at `MAX_BUCKETS` silently raised bloom FPR with no status signal | fixed | `StatusReport.coop.digest_capacity_exceeded`; `coop::digest_capacity_limit_is_observable_before_fpr_degrades` |
| Cold-peer selection ignored available QUIC RTT/path evidence and paid a new handshake in every TTFB sample | fixed | `ChunkFetch` carries selected QUIC RTT/path; `sources::{transport_rtt_and_relay_path_break_a_cold_peer_tie,measured_transfer_data_can_override_a_path_penalty}` |

The compact wire uses raw 32-byte hashes, raw bloom bytes, a raw 32-byte
author, and a 64-byte signature. A full bucket therefore remains close
to its 16,384-byte bit vector and below the proven 30,720-byte signed
content budget; iroh-gossip is explicitly configured for 32 KiB.
At 4 MiB chunks, a 4 TiB cache selects 128 buckets: 2 MiB of digest
memory per observing peer. Rotating one 16 KiB snapshot every 30 seconds
is about 546 bytes/s per publishing node (plus bounded deltas), and a
complete 128-bucket refresh takes 64 minutes. Cache size and eviction
remain independent per node.

Validation (2026-08-28):

- `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`: pass.
- `tests/smoke.sh`; `tests/integration.sh`; release workspace build: pass.
- Full harness: all runnable scenarios passed; only `fio-latency` and `fio-blips` skipped because `fio` is absent. New `s3-retry` passed.
- pjdfstest: **8798 passed, 0 failed**, empty baseline.

### Known limits carried into later phases

- **`/metrics` and the web UI** (DESIGN.md §7) do not exist; the counters
  are only on `StatusReport.coop`. Phase 7.

### TiB-class caches

A cache is expected to run from a small slice of the dataset up to the
whole of it — a node may dedicate one or more full drives, so 1–4 TiB is
an ordinary size, not an extreme. At 4 MiB chunks that is 262k–1M
chunks. Per node, at 10 bits/entry (~1% FPR):

| Cache | Chunks | Filter | One-shot flood / 30 s | 128 buckets, one per 30 s |
|---|---|---|---|---|
| 52 GiB | 13k | 16 KiB | 4.4 kbit/s | fits a single frame |
| 1 TiB | 262k | 320 KiB | 87 kbit/s | ~2.7 kbit/s, full cycle ~32 min |
| 4 TiB | 1M | 1.25 MiB | 350 kbit/s | ~5.5 kbit/s, full cycle ~64 min |

Holding a 4 TiB peer's filter costs 1.25 MiB of RSS; eight such peers
cost 10 MiB. That is affordable. Re-flooding 1.25 MiB through the gossip
mesh every 30 s is not, which is why snapshots rotate one hash-prefix
bucket per interval. Add-only deltas cover inserts between rotations;
an eviction can linger as a false positive until that bucket next
rotates, then the requester falls back to S3.

Each node picks `buckets` from *its own* cache size. A 200 GiB node and
a 4 TiB node advertise different counts; the receiver uses the sender's
count on lookup. A receiver also refuses more than 4 MiB per peer, so
the largest cache in the fleet cannot dictate everyone else's memory.

Cache lifecycle is local: LRU, pins, and dirty holds run against that
node's budget only. The digest is advisory.

- A peer may advertise chunks it has since evicted.
- A peer may hold chunks it has not advertised yet.
- Either way the fetch falls back to S3.

## Later phases

Phases 5 and 5a are closed. Planned next: phase 5b (streaming writes
and the write-through/write-back policy) — see
`docs/plans/05b-p5b-streaming-writeback.md`. Not started: phases 6–8
(snapshots/E2E, web UI, GC). No code exists for any of these yet.
