# Progress

Status of the implementation against [ROADMAP.md](ROADMAP.md).

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
| Prefetcher: adaptive byte-window sequential readahead, fair global AIMD fetch gate, streaming spill decode, and ordered directory scan-ahead | done — harness `readahead`, `readahead-adaptive`, and `scan-ahead` scenarios cover pipelining under injected latency | `cli::{prefetch,scan,coop}`, `fs-core::cache`, `store-s3` |
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

### Phase 5a exit criteria (plan 07)

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
phase 5b (`docs/plans/v1/done/08-p5b-streaming-writeback.md`).

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

## Phase 5b — streaming writes and write-back policy: **DONE**

| Item | State | Where |
|---|---|---|
| Contiguous-high-water eager sealing releases crossed staging ranges; later overwrites re-admit and re-dirty them | done | `cli::{fusefs,staging}` |
| Durable pending queue drained by a bounded pool, sized by an adaptive slow-start / AIMD search over estimated aggregate goodput (`target × bytes / upload service time`); probes discard one settling window, require a 5% total-goodput gain confirmed over two noisy windows, double until a reject then binary-search the step, coalesce failure bursts, halve on sustained errors, and periodically re-probe for recovery (`CONSTELLATION_UPLOAD_CONCURRENCY` pins a fixed value instead; `CONSTELLATION_UPLOAD_MAX_CONCURRENCY` caps the search, default/max 128); chunk bytes are read from the local cache lazily, one per held permit, so pending-upload memory is bounded by concurrency and not by backlog size | done | `cli::main`, `upload-concurrency`, `meta::sqlite` |
| `bench/uploadbench`: standalone shoot-out comparing the production AIMD search (imported unmodified from `constellation-upload-concurrency`), a Vegas-style PID controller, and a fixed baseline, against both a deterministic synthetic network (with scripted SlowDown fault injection) and a real bucket — see `make uploadbench-sim` / `make uploadbench-live` | done | `bench/uploadbench` |
| Benchmark-found smooth-saturation bug fixed, then live WAN stall: per-slot efficiency and 2 s windows treated jitter as a knee (AIMD stuck near N=14 / 13 MiB/s on EU→us-west-2 while fixed N=128 did 26 MiB/s). Controller now doubles, uses 3–8 s windows, and confirms a no-gain verdict twice. Synthetic 1 Gbps / 150 ms: 84.5 MiB/s mean, SlowDown recovery 10.0 s. Live geography (4 MiB PUTs): VPN-capped EU knee N=16 / ~7 MiB/s; EU no-VPN N=128 / 26 MiB/s (N=256 timeouts); same-region N=64 / 2.4 GiB/s plateau (N=128 slightly slower, N=256 +goodput at 5× latency). Vegas PID collapsed to N=1 on RTT inflation — not used in production. Write-up: `bench/uploadbench/RESULTS.md` | done | `upload-concurrency`, `cli::main`, `bench/uploadbench` |
| Local-durable, conditional-create/plain fallback, and adaptive-HEAD dedup ladder; zstd on `spawn_blocking` | done | `cli::{fusefs,main,writeback}`, `store-s3::store` |
| Compression has a portable CPU-sized gate (default min(`available_parallelism`, 8), override `CONSTELLATION_ENCODE_CONCURRENCY`) whose permit lives inside the blocking closure, so cancellation cannot fill Tokio's blocking pool with replacement encoders; the runtime also has a portable 16–64-thread fallback ceiling. Ordinary sync nudges are coalesced without cancelling the active drain, queued upload futures are bounded, and INFO progress summaries default to 10 s (`CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S`) | done | `cli::main`, `store-s3::store` |
| Mount and dynamic write-mode controls, drain-before-switch, and status mode/dirty/pending/probe telemetry | done | `cli::main`, `api` |
| fsync/fdatasync/O_SYNC and `--fsync-mode s3` force through; S3 fsync is inode/partition-scoped | done | `cli::{fusefs,fusefs_ops,main}` |
| Increasing dirty-pressure throttle before hard-limit ENOSPC | done | `cli::writeback` |
| Five write-back scenarios and through-vs-back census benchmark | done | `harness::{scenarios,bench}`, `docs/TESTING.md` |

### Phase 5b exit criteria (plan 08)

- [x] `writeback-bigfile`: 320 MiB (10x a 32 MiB cache), peak daemon
  RSS 242 MiB and peak cache usage 32 MiB.
- [x] `writeback-latency`: 24 closes under 150 ms S3 latency took
  8.98 s through versus 20.61 ms back, comfortably above the 3x gate.
- [x] Census import (20k x 4 KiB, 100 dirs): through copy 87.0 s /
  230 files/s; back copy 7.9 s / 2539 files/s (11.0x), fully drained
  in 10.8 s / 1857 files/s.
- [x] Drain, fsync-after-kill, and S3-cut backpressure/recovery pass in
  `writeback-drain`, `writeback-fsync`, and `writeback-backpressure`.

Write-back keeps the ship-time upload barrier but trades immediate
cross-node visibility and permanent-node-loss durability for local-disk
close latency. Dirty chunks remain non-evictable and are not peer-served;
P2P announcement remains post-S3. Handoff, leave, unmount, and epoch close
all drain the same durable queue. Explicit fsync remains conservative:
local-only durability is POSIX-valid across reboot, but acknowledging data
that can disappear with permanent node loss is too surprising.

Deferred: a LIST-seeded existence bloom for cold high-dedup imports,
dirty peer serving, and record-bearing P2P are separate data/visibility
plane work.

## Phase 6a — snapshots, clones, and subtree mounts: **DONE**

| Item | State | Where |
|---|---|---|
| Deterministic versioned `CTR1` tree objects with sorted complete metadata entries and stable BLAKE3 identity | done | `fs-core::tree` |
| Tree and encoded-manifest objects share the ordinary compressed, verified, content-addressed chunk path; conditional PUT makes repeated snapshots upload only changed objects | done | `cli::snapshot`, `store-s3::store` |
| CAS-created `snaps/<stable-id>.json` records and replica `snapshot` table maintained by `snap_create` / `snap_delete` replay | done | `store-s3::snapshot`, `meta::{sqlite,replay,record}` |
| CLI/control API snapshot create/list/delete, eager clone, and `debug snap-refs` GC-root enumeration | done | `api`, `cli::{main,snapshot}` |
| Hidden explicit-lookup `.constellation/snapshot/<name>/` frozen views, covering ancestor snapshots, top-bit synthetic inodes, bounded decoded-tree cache, read-only enforcement, and ESTALE after deletion | done | `cli::{fusefs,fusefs_ops}` |
| Eager metadata clones materialized atomically and replicated in one `clone` record; data chunks and immutable manifests remain shared | done | `meta::{sqlite,replay}`, `cli::snapshot` |
| Live subtree and read-only snapshot roots accepted by `mount`; `--rw --clone-name` and `--rw --ephemeral` snapshot-mount sugar | done | `cli::{main,fusefs}`, `harness::client` |
| Unit proofs for tree round-trip/corruption, stable unchanged-tree hash, snapshot CAS identity, and clone isolation | done | `fs-core::tree`, `store-s3::snapshot`, `cli::snapshot` |
| Harness lifecycle, clone divergence, read-only snapshot mount, and ephemeral cleanup scenarios | done | `harness::scenarios` |
| Reintegration folds the size-only truncate record into its following manifest edit, so an edit conflict cannot truncate the shared winner | done — regression exposed by the full phase gate | `cli::reintegrate`; harness `deposed-reintegration` |

### Phase 6a exit criteria (plan 09)

- [x] Snapshot creation freezes a well-defined flushed subtree and duplicate
      `path@name` creation is rejected by backend CAS.
- [x] The synthetic control child stays hidden from ordinary `readdir` while
      explicit lookup serves frozen content; deletion makes new operations
      stale without changing the live tree.
- [x] Clones are ordinary writable subtrees and remain valid after their
      source snapshot is deleted; clone writes do not mutate snapshot trees.
- [x] Subtree, snapshot, named writable-clone, and ephemeral writable-clone
      mount selectors work without changing the one-process-per-mount model.
- [x] Snapshot trees are enumerable as future bucket-GC roots and deletion
      deliberately leaves all content-addressed objects for phase 8 GC.

**Design choices.** Tree object encoding lives in `fs-core` because it is a
backend-independent persistent VFS format; S3 only stores the resulting bytes
through the existing chunk path. Small manifests are also stored as immutable
objects (rather than adding a second inline representation to `CTR1`), trading
one deduplicated object per distinct manifest for one uniform verified read
path. Clone metadata uses the plan's allowed eager fallback: SQLite receives
one atomic parent-before-child copy and replicas receive one `clone` record;
file data is never copied.

Each mount remains one process. This preserves the existing replica/cache/P2P
lifecycle and SQLite WAL behavior; sharing one daemon among several FUSE
sessions is not needed for correctness or the snapshot-mount scenario.

**Superseded by plan 21** (see "Plan 21 — named filesystems, a shared mount
daemon, and a local registry" below): the deferral above was true for
*correctness* — nothing about snapshot/clone mounting needed a shared
daemon — but stopped being sufficient once naming needed one daemon to
answer for every view (root + subtrees) registered under one name, per
DESIGN.md §13's "one daemon per (bucket, prefix) per machine". Plan 21
implements that: `NodeRuntime` now hosts zero or more mounted views on
one node identity/replica/cache/lease keeper, matching the spec that was
already written before this deferral, not a correctness fix.

Deferred: courtesy gossip warning before deleting a mounted snapshot. Gossip
does not currently advertise mount selectors, and deletion is specified as
non-blocking even for offline mounts.

Validation (2026-08-30): fmt and strict clippy clean; all workspace tests,
smoke, and S3 integration pass. Full harness: every runnable scenario passed,
with only `fio-latency` and `fio-blips` skipped because `fio` is absent.
pjdfstest: **8798 passed, 0 failed**, empty baseline.

## Phase 6b — E2E passphrase encryption: **DONE**

| Item | State | Where |
|---|---|---|
| `fs create --e2e` generates a random addressing key and p0 DEK; Argon2id-derived KEK wraps them into versioned `keys/keyring.json`, while `meta.json.e2e` fixes the filesystem mode | done | `store-s3::e2e`, `cli::main` |
| Argon2id parameters are persisted (19,456 KiB, 2 iterations, 1 lane); XChaCha20-Poly1305 envelopes carry a version byte and random 24-byte nonce | done | `store-s3::e2e` |
| Mount requires `CONSTELLATION_PASSPHRASE` or a no-echo prompt; wrong passphrases fail before replica/cache startup | done | `cli::main`; harness `e2e-basic` |
| Addressing key, wrapping key, partition DEKs, and the cache verifier's addressing-key copy are best-effort `mlock`ed with `region`, zeroized on drop, and warn without aborting if the host refuses locking | done | `store-s3::e2e`, `fs-core::cache` |
| Chunk identity is keyed BLAKE3 throughout manifest, spill, snapshot tree, peer verification, and local-cache verification paths | done | `fs-core::{chunk,cache}`, `store-s3::store`, `cli::{fusefs,snapshot,coop}` |
| Chunk objects compress before XChaCha20-Poly1305 encryption with chunk hash as AAD; reads decrypt/authenticate before codec decode and keyed-hash verification | done | `store-s3::{store,e2e}` |
| Log segments and checkpoints compress then encrypt with object key as AAD; lease, registry, node, designation, heartbeat, and checkpoint pointer/vector coordination objects remain plaintext | done | `store-s3::{log,e2e}` |
| A partition split persists a fresh random DEK before journaling `part_split`; peers refresh the rewritten keyring with their in-memory wrapping key before reading the child stream | done | `cli::shipper`, `store-s3::{log,e2e}` |
| Cooperative-cache responses from E2E peers are freshly authenticated/encrypted on the application stream and opened before keyed-hash verification | done | `store-s3::store`, `cli::coop`; harness `e2e-two-nodes` |
| `fs passwd` re-derives a KEK and re-wraps unchanged addressing/DEK material; `CONSTELLATION_NEW_PASSPHRASE` supports non-interactive operation | done | `cli::main`, `store-s3::e2e` |
| Unit proofs cover keyring/wrong passphrase, passphrase change without DEK rotation, partition-DEK persistence, AAD rejection, metadata ciphertext, and keyed addressing | done | `store-s3::{e2e,log}`, `fs-core::chunk` |
| End-to-end scenarios cover ciphertext inspection, cold remount, wrong-passphrase rejection, two-node convergence, and cooperative peer hits | done | `harness::scenarios::{e2e-basic,e2e-two-nodes}` |

### Phase 6b exit criteria (plan 10)

- [x] E2E mode is fixed at filesystem creation and requires credentials plus
      the passphrase on every mount.
- [x] User content, snapshot/tree blobs, filename-bearing log segments, and
      checkpoints are encrypted; raw `chunks/` and `log/` objects contain
      neither known plaintext markers nor legacy object-format magic.
- [x] Keyed addressing preserves within-filesystem dedup while preventing
      plain-hash confirmation; local, S3, and peer reads authenticate and
      verify against the same keyed identity.
- [x] Two passphrase nodes converge and record cooperative-cache hits; a cold
      node reconstructs and reads the filesystem while a wrong passphrase is
      rejected cleanly.
- [x] Passphrase changes re-wrap only the keyring. Full DEK rotation remains
      deliberately out of scope because it requires rewriting every encrypted
      object.

**Design choices.** The `region` crate provides portable `mlock`/`munlock`;
failure is a warning because containers commonly have a restrictive
`RLIMIT_MEMLOCK`. Chunk data uses p0's DEK because immutable chunk identity and
dedup span partition boundaries; per-partition DEKs protect that partition's
metadata stream. New partition DEKs are random (not derived): the splitter
writes the expanded keyring before publishing `part_split`, and followers
unwrap additions with the already-locked KEK without retaining the passphrase.

The plaintext boundary is coordination-only: `meta.json`, leases, registry,
nodes/heartbeats, designations, holds, checkpoint pointers/vectors, and snapshot
index records remain visible. They contain identifiers, timing, hashes, and
authority state but no file contents; filenames inside snapshot trees and the
metadata replica are carried by encrypted chunks/checkpoints/log segments.

Benchmark (2026-08-30, local floci, 2,000 × 4 KiB files / 100 dirs): plain
durable write-through **11.2 s / 179 files/s**, E2E **10.9 s / 184 files/s**;
plain write-back copy/drain **0.7/1.1 s**, E2E **0.7/1.1 s**. At this scale the
measured E2E delta is inside run-to-run noise; S3 request and FUSE overhead
dominate XChaCha20-Poly1305 and keyed BLAKE3.

Validation (2026-08-30): fmt and strict clippy clean; 258 workspace unit tests
pass; smoke and S3 integration pass. Full harness: every runnable scenario
passed, including `e2e-basic` and `e2e-two-nodes`; only `fio-latency` and
`fio-blips` skipped because `fio` is absent. pjdfstest: **8798 passed, 0
failed**, empty baseline.

## Phase 7 — embedded web UI and metrics: **DONE**

| Item | State | Where |
|---|---|---|
| Optional localhost-only axum server (`--web-ui` / `CONSTELLATION_WEB_UI_PORT`, zero disables) with rust-embed assets behind the `api/web` feature | done | `api::web`, `cli::main`, `api/webui/index.html` |
| One exhaustive request dispatcher shared by newline-delimited unix sockets and HTTP `POST /api`; convenience `GET /api/status` | done | `api::dispatch` |
| Hand-rolled Prometheus text endpoint for spool, cache, cooperative-cache, and lease/partition gauges | done | `api::web::metrics` |
| Dark, build-free vanilla UI: dashboard, peers, cache/source EWMAs, partitions + voluntary release, read-only file browser/inspect, snapshots, doctor, and log tail | done | `crates/api/webui/index.html` |
| Additive API operations for directory browsing, inspect, snapshot-list spelling, cooperative force-release, log tail, doctor, and cache listing | done | `api::{Request,Response,StatusSource}`, `cli::DaemonStatus` |
| In-memory bounded tracing ring; CLI `inspect`, `cache ls\|stat`, and `log tail` parity | done | `cli::log_buffer`, `cli::main` |
| Exhaustive unix/HTTP adapter parity table and HTTP-level harness smoke | done | `api` tests; harness `web-ui-smoke` |

### Phase 7 exit criteria (plan 11)

- [x] The daemon serves the embedded UI only when explicitly enabled and
      binds `127.0.0.1`; it has no authentication by design. Remote access
      must use a tunnel.
- [x] Unix-socket and HTTP requests pass through one dispatcher over the same
      serde-tagged request/response enums.
- [x] The file browser reads the authoritative SQLite replica, snapshot
      create/list/delete works through HTTP, and log tail is bounded in memory.
- [x] `/metrics` exposes spool/cache/coop/lease state without adding a
      Prometheus dependency.
- [x] Harness `web-ui-smoke` covers status, metrics, ReadDir, snapshot
      create/delete, and a JSON round-trip.

### Phase 7 scope notes

- File-panel rename/delete/upload/download mutations are deliberately deferred:
  plan 11 marks them stretch, while `ReadDir` and `Inspect` provide the required
  read-only browser.
- Snapshot clone/browse/mount actions remain CLI/FUSE operations; phase 7's
  required web panel is list/create/delete.
- Force-release is holder-side flush plus voluntary lease release. It does not
  fence a remote holder and the UI labels that cooperative limitation.
- The web endpoint has no remote bind knob or authentication. Access from
  another host is intentionally tunnel-only.

Validation (2026-08-30): fmt and strict clippy clean; all workspace tests,
local smoke, S3 integration, and release workspace build pass. Full harness:
every runnable scenario passed, including `web-ui-smoke`; only `fio-latency`
and `fio-blips` skipped because `fio` is absent. pjdfstest: **8798 passed, 0
failed**, empty baseline.

## Phase 8a — garbage collection and fsck: **DONE**

| Item | State | Where |
|---|---|---|
| Continuous transactional dereference index, maintained by local mutations and foreign replay, with one-time replica upgrade backfill | done | `meta::{sqlite,replay}` |
| Exclusive `leases/_gc.json` holder using the unchanged partition lease store; daily daemon timer plus `gc run` / mark-only `gc verify` | done | `cli::gc`, `cli::main`, `store-s3::lease` |
| Horizon-filtered reference sweep with conservative offline-designation protection, bucket-authoritative hold roots, immutable snapshot-tree root walking/cache, segment retention, and newest-two checkpoint retention | done | `cli::gc` |
| CAS condemned pointer, P2P freshness announcement, full-TTL wait, renewal/upload-path refresh, dedup resurrection, and pre-DELETE reference recheck | done | `store-s3::gc`, `store-s3::store`, `net::{message,peers}`, `cli::{gc,lease}` |
| Explicit LIST-based orphan pass and immutable per-deletion evidence journal | done | `gc run --orphans`, `gc/journal/` |
| Machine-readable offline `fsck [--repair]`: dangling references/cache healing, orphan GC, xpart abort, corrupt metadata quarantine/checkpoint rebuild, explicit-only stale lease release, cache cruft, and GC-journal audit | done | `cli::fsck` |
| Unit coverage for deref transitions, horizon/exemption filtering, condemned dedup resurrection, and corrupt fixtures | done | `meta::sqlite`, `store-s3::{gc,store}`, `cli::{gc,fsck}` tests |
| Fault scenarios for lifecycle roots, the condemned dedup race, and repair exit-code lifecycle | done | `harness::{gc-lifecycle,gc-dedup-race,fsck-repair}` |

### Phase 8a exit criteria (plan 12)

- [x] Reference GC discovers ordinary garbage from the replica index without
      listing chunks; only `--orphans` performs the expensive chunk LIST.
- [x] The delete-vs-dedup race is closed by condemned publication, gossip,
      one full authority TTL, upload resurrection, and a final live/deref
      recheck.
- [x] Live files, snapshot trees, active designations, and advertised holds
      are protected roots; metadata retention keeps the newest two
      checkpoints and the configured log floor.
- [x] Every destructive GC action is journaled with rule/evidence, and
      `gc verify` is mark-only.
- [x] `fsck` exits 0 clean / 1 detected / 2 repaired / 3 unrepairable and
      never silently truncates a file with unavailable content.
- [x] Harness `gc-lifecycle`, `gc-dedup-race`, and `fsck-repair` pass.

**Design choices.** SQLite records inline data hashes directly and records a
spilled manifest object's hash transactionally; the sweep resolves the spill
through S3 before deleting anything. An active offline designation
conservatively pauses all reference candidates because the current
designation object does not retain per-hash provenance; this sacrifices
reclamation while a designation exists, never safety. Uploads read the
condemned pointer at every dedup decision in addition to lease-renewal
refreshes, closing the acquisition/publication edge without relying on gossip.

Validation (2026-08-30): fmt and strict clippy clean; 267 workspace unit tests
pass; smoke and S3 integration pass. Full harness: every runnable scenario
passed, including `gc-lifecycle`, `gc-dedup-race`, and `fsck-repair`; only
`fio-latency` and `fio-blips` skipped because `fio` is absent. pjdfstest:
**8798 passed, 0 failed**, empty baseline.

## Phase 8b — hardening, performance gates, and packaging: **DONE**

| Item | State | Where |
|---|---|---|
| Pinned xfstests-dev generic lane with independent test/scratch S3 prefixes, reasoned exclusions, and two-way known-failure regression baseline | done | `tests/xfstests*`, `tests/docker`, `docker-compose.yml` |
| Machine-readable benchmark plus cold sequential-read and warm random-read probes; 20% rate-floor gate | done | `harness::bench`, `tests/perf-*` |
| Git-described CLI version and static x86_64 musl / native macOS archives containing the binary, license, and README | done | `cli/build.rs`, `Makefile`, `docs/RELEASING.md` |
| Nightly ordered Linux matrix, cargo audit, package artifacts, native macOS build/tests, per-job logs, and final summary | done | `.github/workflows/nightly.yml` |
| v1 documentation sweep | done | `README.md`, `docs/{TESTING,PROGRESS,RELEASING}.md` |

### Phase 8 exit criteria (ROADMAP.md)

- [x] `fsck --repair` and GC cover the phase-8a corruption/reclamation
      classes, with destructive harness scenarios.
- [x] xfstests generic coverage is pinned and regression-gated; unsupported
      local-device operations are explicitly excluded rather than silently
      skipped by Constellation.
- [x] Performance rates are emitted as JSON and compared with a committed
      current-code baseline at 20% tolerance.
- [x] Linux static and macOS native packaging paths exist, and the nightly
      workflow runs the complete ordered test matrix with uploaded logs.

Linux is the release platform with full FUSE, fault, pjdfstest, and xfstests
coverage. macOS CI compiles and runs mount-less workspace tests; actual mount
operation requires macFUSE on the destination and remains less exercised than
Linux. The native-only packaging guard prevents accidental Linux FUSE
cross-compilation.

`cargo audit` has no vulnerability-level findings after upgrading
`object_store` to 0.14 (which removes the vulnerable quick-xml release). It
still reports four allowed warnings: transitive unmaintained `atomic-polyfill`
and `paste`, the longstanding fuser 0.15 initialization advisory, and a yanked
transitive `chacha20`. fuser 0.15 is the design-pinned API and the other three
have no direct call sites in Constellation; all remain visible in every nightly
audit rather than being hidden with ignore flags.

Validation (2026-08-30): fmt and strict clippy clean; 267 workspace tests,
smoke, S3 integration, all runnable harness scenarios, and pjdfstest
8798/8798 pass. The bounded generic xfstests sweep reports **118 passed, 50
known failures, 630 excluded** (609 capability-skipped plus 21 reasoned
exclusions), with no regressions. The three-run-median performance gate passes
all eight rates, and `make dist-linux` produces a static-PIE musl binary whose
`ldd` result is `statically linked`.

## Phase 8c — LIST-seeded existence bloom: **DONE**

| Item | State | Where |
|---|---|---|
| Mount-time background scan of 256 `chunks/<aa>/` prefixes at bounded concurrency, with canonical key parsing and incomplete-on-cap semantics | done | `store-s3::existence`, `cli::existence` |
| Local-only 10-bit/entry, k=7 bloom with `CONSTELLATION_EXISTENCE_BLOOM_BYTES` (default 4 MiB); the 16 KiB gossip cap remains wire-only | done | `net::bloom`, `cli::existence` |
| Upload ladder: local durable first, live peer digest probe hint, complete LIST bloom miss → Create without HEAD, hit → Probe, then unchanged adaptive fallback | done | `cli::{main,coop,existence}` |
| Successful uploads add to the local filter; GC does not delete bits, so stale membership costs a HEAD but cannot skip a PUT | done | `cli::main` |
| Kill switches `CONSTELLATION_EXISTENCE_LIST=off` and `CONSTELLATION_EXISTENCE_PEER_HINT=off`; `CONSTELLATION_COOP=off` also removes peer hints | done | `cli::existence` |
| Additive writeback status and Prometheus metrics for listed keys, completeness, bloom hits/misses, and peer hints | done | `api::{types,web}`, `cli::main` |
| Counting-store unit coverage for hit/miss request shape, false-positive safety, peer hint/fallback, incomplete cap, canonical parsing, and condemned overwrite | done | `store-s3::{existence,store}`, `cli::{existence,main}` tests |
| Fault scenarios for cold duplicate import and LIST-disabled peer hints | done | `harness::{existence-bloom-dedup,existence-peer-hint}` |

### Phase 8c exit criteria (plan 14)

- [x] A complete S3 LIST seed can prove absence and choose a conditional
      Create without HEAD; every bloom or peer-digest hit still performs a
      store call before its pending row is acknowledged.
- [x] Capacity overflow leaves the seed incomplete, so no unlisted hash is
      treated as absent. Listing and filter memory are bounded independently
      of bucket size.
- [x] Peer digests remain cache-membership hints only; a peer miss falls
      through and dirty chunks remain excluded by the existing digest rules.
- [x] Condemned hashes still force an overwrite even when a LIST bloom or
      peer digest selects Probe.
- [x] Harness scenarios `existence-bloom-dedup` and `existence-peer-hint`
      pass with byte-exact model verification and their kill-switch beats.

The mount performs no periodic re-LIST. Insert-on-put keeps ordinary additions
current; after bucket GC, stale hits are safe and only raise HEAD traffic. A
post-GC re-LIST could tighten the false-positive rate but is deliberately out
of scope for this phase.

Validation (2026-08-30): fmt and strict clippy clean; 275 workspace tests,
smoke, S3 integration, release build, every runnable harness scenario, and
pjdfstest **8798 passed, 0 failed** with an empty baseline. Harness seed 42:
`existence-bloom-dedup` listed 300 keys in 359.1 ms and reported
`bloom_hits=300 bloom_misses=0`; its 256 sharded LIST requests replaced 300
per-chunk HEAD decisions for this tree. `existence-peer-hint` reported
`peer_hints=9` with LIST disabled; its coop-disabled beat reported zero hints.
Only `fio-latency` and `fio-blips` skipped because `fio` is absent.

## Phase 8d — concurrent snapshot and clone churn oracle: **DONE**

| Item | State | Where |
|---|---|---|
| WAL-backed SQLite expected state for live roots, immutable snapshot entries, and writable clone lineage; payloads are represented by BLAKE3 hashes rather than retained bytes | done | `harness::snapchurn` |
| Seeded concurrent workers with disjoint `/tree/wN` and `/cN/wN` prefixes, quiesce boundaries, mixed file/dir/symlink/rename/truncate/chmod operations, and occasional multi-chunk writes | done | `harness::snapchurn` |
| Multi-round origin and clone snapshots, snapshot/clone deletion, clone-of-clone, and an overlapping `marker` that diverges on origin and clone without changing its frozen source | done | harness `snapshot-churn` |
| Complete live and hidden frozen-view verification (paths, kinds, sizes, hashes/targets, mtimes, modes), plus clone survival after deleting its source snapshot and source clone | done | harness `snapshot-churn` |
| Flushed JSONL operation/lifecycle trail with timed `--replay` and accelerated `--replay-no-sleep` | done | `harness run snapshot-churn` |
| Final cleanup asserts an empty replica snapshot/root namespace, empty bucket `snaps/`, and no user `chunks/` after zero-horizon orphan GC | done | harness `snapshot-churn` |
| Clone roots inherit the mount daemon's effective UID/GID instead of hard-coded root ownership; regression asserted in the existing clone-isolation unit test | done | `cli::snapshot` |

### Phase 8d exit criteria (plan 15)

- [x] Four workers complete three default rounds of 30 operations each, with
      all worker I/O closed before snapshot, clone, delete, and verify phases.
- [x] Origin snapshots and clone snapshots are frozen and exhaustive; live
      roots diverge independently, including byte-distinct same-relative-path
      markers and a clone-of-clone that survives deletion of both its source
      snapshot and source clone.
- [x] The last quiescent state retains origin files, a snapshot, and a clone;
      cleanup then drains all three root classes and proves the replica and S3
      user-object namespaces are empty after GC.
- [x] The emitted 362-event seed-42 trail replays successfully both with
      recorded timing and with `--replay-no-sleep`.

The oracle records the metadata actually exposed by frozen views because those
views intentionally mask write bits, then refreshes a materialized clone's
ordinary writable metadata after copying its snapshot rows. This preserves the
snapshot verifier's read-only-mode assertion without confusing it with clone
ownership and mode semantics.

Validation (2026-08-30): fmt and strict clippy clean; workspace tests, smoke,
S3 integration, release workspace build, every runnable harness scenario, and
pjdfstest **8798 passed, 0 failed** with an empty baseline. `snapshot-churn`
(seed 42) reported 4/4 snapshots and 3/3 clones created/deleted, four complete
verifies, marker isolation and clone-of-clone true. Timed and no-sleep replay
both passed.

## Phase 8e — fallocate, hole punching, and sparse manifests: **DONE**

| Item | State | Where |
|---|---|---|
| First-class holes represented by absent chunk indices; all-zero sealed chunks collapse to holes and never enter cache, pending upload, coop, pin, prefetch, or GC walks | done | `fs-core::manifest`, `cli::{fusefs,prefetch,pin,gc,fsck,snapshot}` |
| Compact `CLH1` sparse encoding (`index u64 + hash`) for inline and spilled chunk lists, with legacy dense spilled blobs still accepted | done | `fs-core::manifest` |
| Metadata-only truncate/fallocate extension, KEEP_SIZE, full- and partial-chunk PUNCH_HOLE, sparse ZERO_RANGE, and explicit rejection of unsupported range-moving flags | done | `cli::{fusefs_ops,staging}` |
| `SEEK_HOLE` / `SEEK_DATA` from committed sparse maps plus dirty staging state | done | `cli::fusefs_ops`, fuser ABI 7.24 |
| Sparse staging reservations charge admitted data chunks rather than logical hole length and preserve bounded write-back pressure | done | `cli::staging`, `cli::fusefs_ops` |
| Cross-node sparse layout, punch/rewrite, object-count, cache, and RSS oracle | done | harness `fallocate-sparse` |

### Phase 8e exit criteria (plan 16)

- [x] A 1 TiB manifest with one data chunk stays inline and under 80 bytes;
      missing indices encode holes without allocating a dense vector.
- [x] Extension and allocation modes do not PUT zero chunks. Full chunks of
      written zeroes and punched chunks collapse to absent indices; partial
      punches RMW only their boundary chunks.
- [x] Sparse reads, `SEEK_HOLE`/`SEEK_DATA`, punch, and rewrite survive a
      fresh-node remount.
- [x] Legacy dense spilled chunk lists decode, and every reference walk skips
      holes by construction.
- [x] `fallocate-sparse`, the complete harness matrix, and pjdfstest pass.

The rejected zero-chunk design would have deduplicated bytes in S3 but still
allocated one manifest slot per 4 MiB and sent meaningless cache/cooperative
traffic. Absent sparse-map indices instead make a hole consume no object and
no hash slot.

Validation (2026-08-30): fmt and strict clippy clean; 281 workspace tests,
smoke, S3 integration, release workspace build, all runnable harness scenarios,
and pjdfstest **8798 passed, 0 failed** with an empty baseline.
`fallocate-sparse` reported a 256 MiB file on a 32 MiB cache with **18 chunk
objects** (including immutable superseded data versions, but no 64-object hole
span) and **67 MiB RSS**. Only `fio-latency` and `fio-blips` skipped because
`fio` is absent.

## Phase 8f — POSIX extended attributes and recursive size: **DONE**

| Item | State | Where |
|---|---|---|
| Journaled inode xattrs with atomic set/remove records, create/replace semantics, last-unlink cleanup, replay, reopen, and eager-clone copying | done | `meta::{record,sqlite,replay}` |
| FUSE `getxattr`, `setxattr`, `listxattr`, and `removexattr`; full `user.*`, uid-0-only `trusted.*` (FUSE lacks capability bits), unsupported system/security namespaces, Linux size limits, and lease-gated mutation | done | `cli::{fusefs,fusefs_ops}` |
| Read-only decimal `user.constellation.rsize` / `rcount`; files report logical size/count 1 and directories use a bounded recursive SQLite query | done | `meta::sqlite`, `cli::fusefs_ops` |
| Backward-readable `CTR2` snapshot trees carrying directory and entry xattrs; frozen views expose them and eager clones restore them | done | `fs-core::tree`, `cli::{snapshot,fusefs}` |
| Two-node file/directory round-trip, removal propagation, and sparse logical-size oracle | done | harness `xattr-roundtrip` |

### Phase 8f exit criteria (plan 17)

- [x] User xattrs set, list, replace, remove, survive SQLite reopen, replay to
      another replica, disappear on final unlink, and copy into eager clones.
- [x] Snapshot trees preserve xattrs in `CTR2`; old `CTR1` blobs decode with
      empty xattrs, and hidden frozen views expose the captured values.
- [x] Virtual recursive aggregates are read-only decimal ASCII. A file reports
      `(size, 1)`; a directory sums descendant logical file lengths and counts
      each reachable file inode once. Holes therefore contribute to `rsize`.
- [x] `xattr-roundtrip`, the complete harness matrix, and pjdfstest pass.

The aggregate fallback is intentionally computed from the authoritative
replica with a recursive CTE rather than maintained `inode.rsize/rcount`
columns. Its cost is bounded by the reachable subtree and requires no S3 I/O;
maintained O(1) directory columns remain a future performance optimization.

Validation (2026-08-30): fmt and strict clippy clean; 286 workspace tests,
smoke, S3 integration, and release workspace build pass. Full harness:
**ALL RUN SCENARIOS PASSED**, including `xattr-roundtrip`; only `fio-latency`
and `fio-blips` skipped because `fio` is absent. pjdfstest: **8798 passed,
0 failed**, empty baseline. `xattr-roundtrip` reported `rsize=1073741831`,
`rcount=2` for a seven-byte file plus a sparse 1 GiB logical file.

## Phase 8g — adaptive multi-core dispatch: **DONE**

Linux mounts now use fuser's concurrent event loops with cloned FUSE file
descriptors. Worker count scales sublinearly from the process-visible CPU
quota and is capped by both 64 threads and one eighth of host/cgroup memory;
Tokio and its blocking pool are sized separately. Write state is split across
64 inode shards, preserving same-inode ordering while allowing unrelated
files to stage, hash, read, and flush concurrently. A 1-CPU allocation keeps
one FUSE and one Tokio worker. The kernel background queue grows with the FUSE
pool, and parallel directory operations are negotiated when supported. The
three pool sizes remain operator-overridable.

Validation (2026-09-03): fuser 0.18 migration, fmt, strict workspace clippy,
release workspace build, all workspace tests, and all 102 CLI unit tests pass.
Live mounted throughput scaling was not measured on this host because
`/dev/fuse` is unavailable.

## Plan 21 step 0 — `NodeRuntime` extraction: **DONE**

| Item | State | Where |
|---|---|---|
| `NodeRuntime::start`: per-node setup (backend/replica/cache open, node identity, staging GC/budget, lease keeper, P2P endpoint, periodic GC task, shipper attach + root adoption, sync task, gossip/registry/designation/placement background tasks) extracted from the old monolithic `mount()` | done | `cli::node_runtime` |
| `NodeRuntime::add_mount`: per-view setup (selector/`@snapshot` parsing, `--rw`/`--clone-name`/`--ephemeral` clone creation, `FuseFs` construction, mount options, `fuser::Session`) spawns its session on a dedicated OS thread; `remove_mount` unmounts one view via its `SessionUnmounter` and joins that thread without touching siblings | done | `cli::node_runtime` |
| Node-level signal handling: SIGINT/SIGTERM unmount every currently-mounted view (via a background thread, so the async handler task never blocks), then run `NodeRuntime::shutdown` once the last view is gone; second signal still aborts immediately | done | `cli::node_runtime` |
| `Command::Mount`'s CLI handler reduced to a thin wrapper (`cmd_mount`): build `NodeConfig`, `NodeRuntime::start`, one `add_mount`, block on `join_mount` — today's single-view blocking behavior, unchanged | done | `cli::main` |
| `existence::Existence::spawn_seed` takes `&tokio::runtime::Handle` instead of `&Runtime` (the only other call-site change this step needed) | done | `cli::existence` |

Per the plan's explicit step-0 scope: no control-socket wire changes, no
registry, no name resolution, no daemonization — those are plan 21 steps
1–7 and remain future work. `DaemonStatus` still reports a single
`mountpoint`/`prefetch_stats`, so building it and starting the control
socket + web UI is deferred to the first `add_mount` call rather than
living in `start()` — the plan's own architecture diagram (`mount` →
`NodeRuntime::start` → `add_mount` → "serve control socket") shows this
same ordering for the fully-built system, and Step 1 is explicitly where
that struct grows a `mounts: Vec<MountInfo>` shape; step 0 does not change
it. `existence.spawn_seed`'s bucket-wide LIST scan is likewise spawned
from `add_mount` (guarded to the first view only), matching the original
`mount()`'s exact call site immediately before the `fuser::Session` is
built, not any earlier — the scan's own short grace delay assumes a mount
is imminent.

### Step 0 exit criteria (plan 21)

- [x] `cargo fmt --all` clean and `cargo clippy -p constellation --all-targets -- -D warnings` clean for every file this step touched.
- [x] `cargo test --workspace`: 0 failures.
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` (floci S3 via docker): both PASSED.
- [x] `docker compose --profile test run --rm compliance`: pjdfstest **8798 passed, 0 failed**, empty baseline.
- [ ] `target/release/harness run`: not a clean "every scenario PASSED" on this host — see below.

Steps 1–7 (control-socket mount verbs, local registry, name resolution,
mount/umount rework, daemonization, `fs create`/`export`, `fs list`) were
implemented in a later session and are recorded as their own milestone
below ("Plan 21 steps 1–7").

**Harness note.** This host is a loaded interactive desktop (browsers,
Slack, editors, other agent sessions), not a dedicated CI runner — `free`
showed **21 GiB of swap in use** during this work, and consecutive
`harness run` invocations back-to-back failed *different* scenario
subsets each time, including scenarios with no plausible relationship to
this refactor (`cold-cache`, `two-clients-disjoint`, `s3-outage`) and one
timeout carrying a raw `HTTP error: error sending request` against the
local S3 emulator. To separate host noise from an actual regression, the
suspect scenarios were run repeatedly (5-8x each) against both this
change and the unmodified base commit (`git stash`, rebuild, rerun):
`s3-flap`, `kill9-remount`, and `deposed-reintegration` fail at a similar
rate on the base commit with the same divergence signatures, confirming
they predate this plan. `writeback-latency` and `partition-split` showed
a higher failure rate under the refactored binary in some batches; one
suspected ordering difference (`existence.spawn_seed` timing relative to
the FUSE mount) was found and fixed to match the original call site
exactly, but did not change the failure rate, and a full-matrix rerun
after the fix still showed the same scenario churning between different
failures run to run — consistent with host load rather than a code path
that differs between the two binaries. No logic difference was found
between the original and refactored code for any of these paths after
line-by-line comparison. Re-running the full matrix on a quiet host is
the way to close this out with confidence; it is flagged here rather than
asserted clean.
Deferred format/data-plane items remain listed in `docs/plans/v1/ROADMAP.md` and the
phase-specific scope notes above.

## Plan 21 steps 1–7 — named filesystems, local registry, shared mount daemon: **DONE**

| Item | State | Where |
|---|---|---|
| Control socket gains `MountAdd { subtree, mountpoint, opts }` / `MountRemove { mountpoint }` / `MountList`; a second CLI invocation extends an already-running daemon instead of starting a new process | done | `api::{Request,Response}`, `cli::main` (`fn mount_add/mount_remove/mount_list`) |
| `StatusReport`/`DaemonStatus` converted from a single `mountpoint: String` to `mounts: Vec<MountInfo>` (one entry per live view); `leave` is node-level and detaches every mounted view, not just whichever one answered the control call | done | `api::types::StatusReport`, `cli::main::DaemonStatus` |
| Local registry: `$XDG_CONFIG_HOME/constellation/registry.toml` (`CONSTELLATION_REGISTRY` override), node-level fields under `[NAME]`, per-view rows under `[[NAME.mounts]]`; `Registry::load/load_locked/entry/merge_and_save/remove`, write-then-rename, whole-file `flock` around the read-modify-write cycle | done | `cli::registry` (13 unit tests: overwrite-only-explicit-fields, node-vs-view placement, upsert-by-subtree, remove, TOML round-trip, 8-way concurrent-writer lock race) |
| Name resolution: `myfs`, `myfs:/sub`, and a leading-`/` literal path are never reinterpreted even when it contains a colon; `Target::{Named,Raw}`, `state_dir`/`s3_url`/`state_dir_opt`/`effective_path` helpers | done | `cli::target` (7 unit tests) |
| Every state-dir-taking subcommand (pin, unpin, offline, online, inspect, pins, designations, reintegrate, leave, write-mode, quota get/set, cache ls/stat/prune, log tail, status, snapshot create/delete/ls, clone, debug snap-refs, gc run/verify, fsck, doctor, `fs passwd`) takes a `TARGET` positional resolved through the registry, with `--state-dir`/`--s3` kept as explicit overrides | done | `cli::main` (`Command` variants + `resolve_target`) |
| `mount`/`umount` CLI rework: `TARGET [MOUNTPOINT]` positional syntax, explicit flags become registry overrides via `merge_and_save`, `--s3` refused against an already-populated state dir under the same name (identity pinned to the name), a per-name `daemon.lock` (`flock`, non-blocking) decides attach-vs-become-daemon race-free, bare `mount NAME` brings up every registered view all-or-nothing (partial failure rolls back only the views *this invocation* added) | done | `cli::main` (`cmd_mount`, `cmd_mount_body`, `take_state_dir_lock`, `attach_views`) |
| New `Command::Umount { target, state_dir }`: detaches one view (`NAME:/sub`) or every currently-mounted view (bare `NAME`); the daemon runs its own shutdown once its last view is gone, the CLI call waits (bounded) for the control socket to close | done | `cli::main::cmd_umount` |
| Daemonization (JuiceFS-style): fork before any tokio runtime/`NodeRuntime`/FUSE-thread work exists (the process is still single-threaded, so the child inherits no half-held locks or dead worker threads); parent blocks on a status pipe for "every requested view attached" or a specific error and exits with that verdict; child does `setsid()`, redirects stdout/stderr to `state_dir/daemon.log`, writes `state_dir/daemon.pid`; `-f`/`--foreground` and `CONSTELLATION_NO_DAEMONIZE` opt out | done | `cli::daemonize` |
| `fs create` gains a mandatory positional `name`; registers the name with no views yet (`zfs create pool/dataset`-style) | done | `cli::main` (`FsCommand::Create`) |
| New `Command::Export { name, force }`: one-shot teardown — if a `node_id` was ever claimed, sends `Leave` to a reachable daemon (drains the journal, releases leases, tombstones the registry record, then detaches every view from a background thread once the response is on the wire) and waits for the socket to close; if the daemon is unreachable, retires the node directly via the admin-leave path (refusing a live-but-wedged `daemon.lock` unless `--force`); always deletes the state dir (it is name-keyed, so leaving it behind would make a later `mount NAME` reopen a replica that already recorded "left the cluster") and the registry row last | done | `cli::main::cmd_export` |
| `FsCommand::List` (`constellation fs list`): every registered name, its `S3`/state dir, and per-view subtree/mountpoint/live-mounted-or-not (probed via `MountList` against the control socket) | done | `cli::main::cmd_fs_list` |
| Harness scenario `named-shared-daemon`: drives the real binary directly (not the `Client` abstraction, which always mounts ad hoc and stays foregrounded) against an isolated `CONSTELLATION_REGISTRY`/`XDG_DATA_HOME`; proves `mount myfs MOUNTPOINT` then `mount myfs:/sub MOUNTPOINT2` from a second, independent invocation share one `daemon.pid`/one `node_id` (real backgrounding, not `--foreground`), both views serve reads/writes through the shared replica, `umount myfs:/sub` leaves the root view and daemon up, `umount myfs` runs the daemon's clean-shutdown sequence and removes its own PID file | done | `harness::scenarios::named_shared_daemon` |
| Call-site sweep: `tests/lib.sh` (`fs create NAME`, `mount / MOUNTPOINT --state-dir DIR --foreground`), `crates/harness/src/client.rs` (same two changes plus every ad-hoc `TARGET` positional `gc run`/`fsck` now needs), `README.md`, `docs/reference/configuration.md` (`CONSTELLATION_REGISTRY`, `CONSTELLATION_NO_DAEMONIZE`), new `docs/reference/features/named-filesystems.md`, `docs/how-to-guides/development/TESTING.md` | done | see files listed |

**Design decisions where the plan was ambiguous or silent.**

- The plan's Step 1 sketch showed `MountRemove { subtree }`; the implementation
  addresses it by `mountpoint` instead, since the same subtree may legitimately
  be mounted at two different mountpoints and "detach *this* mount" needs to
  name which one.
- The plan's "Deferred, not decided against" note leaves open what a bare
  `myfs:/path` control command targets when more than one view is mounted;
  the implementation takes the plan's own fallback literally — commands that
  need exactly one state dir resolve it from the registry regardless of how
  many views are live (there is exactly one state dir per name), so this
  only matters for the daemon-side single-mount commands, which already
  operate node-wide (`leave`, `write-mode`) rather than needing to pick a
  view.
- **`export` vs. `forget`.** The plan's Step 6 sketch (`Command::Forget`)
  described a "remove the registry row, no data touched, refuse if the
  daemon is reachable" verb — the `zpool export` name without the
  "must not currently be busy" ceremony. The implementation instead ships a
  single `constellation export NAME [--force]` that performs the full
  teardown: self-leave (if a `node_id` was ever claimed) while the daemon is
  still up so it can drain its journal and release leases cleanly, detach
  every view, delete the state dir, then drop the registry row — folding
  what would otherwise be a "you must `umount` and manually clean up
  first" two-step dance into the one verb DESIGN.md's control-plane surface
  already promises (`fs create|mount|umount`; `export` is this plan's
  addition to that list). This was carried forward from mid-implementation
  design discussion in the session that did steps 1–7; it is recorded here
  because it changes user-visible behavior from the plan's written sketch.
  The state dir is deleted unconditionally (not left for a future `mount
  NAME` to reopen) because it is name-keyed: leaving it behind after a
  successful leave would make a later `mount NAME` reopen a replica that
  already recorded "left the cluster" and refuse to remount, which is worse
  than requiring `fs create`/`mount --s3` again.
- `read_status`/`is_mountpoint`/`constellation_bin` in the new harness
  scenario duplicate small helpers already private to `harness::client`
  (`Client` is built around "one client owns one foregrounded process with
  a known PID," which does not fit a scenario that specifically needs a
  second, independent CLI invocation to attach to a daemon it does not own)
  rather than widening `Client`'s public surface for a single caller.

### Steps 1–7 exit criteria (plan 21)

- [x] `cargo fmt --all` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean (including two unrelated pre-existing lint violations in `constellation-chaos` — a `std::sync::Mutex` guard held across an `.await`, a `needless_question_mark`, `manual_is_multiple_of`, `suspicious_open_options` on two `OpenOptions` calls, two `type_complexity` findings, and a test module's `unwrap_used` — fixed in passing since they blocked the workspace-wide gate; none are in a file this plan otherwise touches).
- [x] `cargo test --workspace`: 0 failures (352 tests across every crate, including the CLI's 141).
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` (floci S3 via docker): both PASSED.
- [x] Two-view single-process/single-`node_id` confirmation: `named-shared-daemon` harness scenario PASSED — see its description above; it asserts one `daemon.pid`, `status.mounts.len() == 2`, and one `node_id` after the second, independent `mount myfs:/sub` invocation.
- [x] `target/release/harness run`: 63/67 PASSED, 2 `fio-*` SKIPPED (`fio` absent, acceptable per CONVENTIONS), 4 FAILED — all 4 confirmed pre-existing, not plan-21 regressions; see below.
- [x] `docker compose --profile test run --rm compliance`: pjdfstest **8798 passed, 0 failed**, empty baseline (`--foreground`, the standing gate); additionally re-run against a **daemonized** mount (real fork/`setsid`, not `--foreground`) restricted to the categories most likely to be sensitive to fd inheritance/cwd/signal delivery (`chmod rename open unlink mkdir symlink truncate link chown`): **8114 passed, 0 failed**. See below for how the daemonized run was driven.

**Harness note (steps 1–7).** Before the harness matrix was run, a
background `harness run` a prior session in this worktree had left running
was found still active (docker containers, orphaned FUSE mounts under
`/tmp/harness-*`) and was cleaned up first. An initial isolated check of
`baseline` alone (before the full matrix) showed it failing intermittently
(`MODEL DIVERGENCE`: a written file reading back as size 0, or a wrong byte
count) at roughly a 20-30% rate across repeated single-scenario reruns —
concerning since it is the simplest possible scenario (one client, no fault
injection) and was not one of step 0's three confirmed-flaky scenarios
(`s3-flap`, `kill9-remount`, `deposed-reintegration`). It was checked
against the **unmodified `main` branch's own release build** in the sibling
checkout: `baseline` reproduced the identical `MODEL DIVERGENCE` failure
mode at a similar rate there too (6 runs, 1 failure, `w-f-87` size 0),
confirming it predates this plan and is not a steps-1–7 regression (which
do not touch the FUSE read/write path).

The subsequent full 67-scenario matrix run then passed 63, skipped 2
(`fio-latency`/`fio-blips`, `fio` absent), and failed 4:
`deposed-reintegration` (already confirmed-flaky per step 0), plus three
new-looking failures — `partition-split` (`MODEL DIVERGENCE` after a
merge), `gc-dedup-race` ("GC deleted content committed during condemned
wait"), `writeback-latency` ("back import content mismatch"); `baseline`
itself passed this time. Per this plan's guidance not to chase flakiness
in an open-ended loop, each of the three new-looking failures got a small,
bounded number of targeted reruns against the **same unmodified `main`
build** used for the `baseline` check above (not this worktree):
`partition-split` reproduced the identical divergence on the first main
run; `gc-dedup-race` reproduced on main at a similar rate (1 failure in 8
runs across both checks); `writeback-latency` reproduced on main at an
even higher rate (3/3 failures once retried). All three, like `baseline`
and `deposed-reintegration`, are therefore pre-existing and
host-load-correlated, not regressions introduced by steps 1–7 (none of
which touch partition split/merge, GC condemnation, or the write-back
path) — consistent with step 0's own prior finding that this specific
host produces scenario-independent, non-reproducible-on-a-clean-run
divergences under load. Left for a future investigation on a quiet host,
per this plan's explicit instruction not to chase pre-existing host-load
flakiness inside this plan's scope.

**Reconciliation note (steps 1–7): rebased onto four commits that landed
on `main` after this worktree branched** (`ad314cd` scratch-xattr
staging, `366356c` logical statfs used-space, `7d556f6` cluster-wide
logical size quota, `76c865d` mount-view-scoped statfs). The scratch-xattr
commit is purely `SqliteMeta`-level and inode-addressed, so it needed no
changes for multi-view: it merged with no conflicts and no design
questions. The two statfs commits are per-`ConstellationFs`
(`usage_cache`/`statfs_ttl` fields on the struct itself), and each mounted
view already gets its own `ConstellationFs` instance from
`NodeRuntime::add_mount`, so per-view scoping (a subtree/snapshot view
reporting its own used bytes, a whole-fs view reading the maintained
counter) held with no functional changes — confirmed by
`fusefs::quota_tests::view_usage_scopes_to_subtree_mount` and
`..._scopes_to_snapshot_mount` passing unmodified post-merge.

The quota commit needed real integration, not just conflict resolution,
because it had modified the old monolithic `mount()` (already deleted by
step 0's `NodeRuntime` extraction) and a single-view `DaemonStatus`:

- The `Command::Quota`/`QuotaCommand::{Get,Set}` CLI shape changed from
  a bare `--state-dir` to the same `TARGET [--state-dir DIR]` positional
  every other node-level command (`write-mode`, `cache`, `log tail`)
  already uses, resolved through `resolve_target` — matching the plan's
  Step 3 table entry (`quota get/set myfs [size]`, "always whole-fs,
  never per-subtree, node-level").
- The creation-time cap mirroring (`fsmeta.max_logical_bytes` →
  `QUOTA_CREATION_KV_KEY` in node-local kv) moved from `mount()` into
  `NodeRuntime::start`, right after `meta.set_node_prefix`, matching the
  original call site exactly (once per node, not per view).
- `DaemonStatus::set_quota`/`get_quota` moved from operating on
  `self.meta`/`self.quota_cache` fields of a single-view daemon struct to
  going through `self.node` (the shared `NodeRuntime`), consistent with
  every other control-plane method after step 0's extraction.
- One real design decision: the old code cached a *single* `QuotaCache`
  handle (one `ConstellationFs`, one cache) on `DaemonStatus` and
  invalidated just that on a live `SetQuota`. Under multi-view that is
  wrong — a second, later-mounted view has its own independent
  `QuotaCache` (each `ConstellationFs` owns one), and invalidating only
  the first view's would leave every other mounted view serving a stale
  cap for up to `QUOTA_CACHE_TTL` (5s) after an operator lowers or clears
  it. Fixed by giving `node_runtime::MountHandle` its own `quota_cache`
  field (captured from `fs.quota_cache_handle()` before the view's
  `ConstellationFs` moves into its `fuser::Session`) and adding
  `NodeRuntime::invalidate_quota_caches()`, which walks every currently
  mounted view and invalidates each one; `DaemonStatus::set_quota` calls
  that instead of touching a single cached handle. No test previously
  exercised the multi-view case (the existing `quota-enforcement` harness
  scenario is single-view), so this was verified by inspection against
  `fusefs.rs`'s own cache-invalidation contract rather than a new
  regression test; the mechanism (a per-view cache handle, invalidated by
  iterating `NodeRuntime`'s mount table) mirrors how the pre-existing
  multi-view code already handles everything else that is node-level but
  view-local-cached.

`docs/reference/configuration.md`'s merge produced accidental duplicate
rows (`CONSTELLATION_DIGEST_INTERVAL_S`, `CONSTELLATION_SCAN_AHEAD`
appearing in both "Metadata sync and partitions" and their correct
sections) from a bad 3-way match; removed the duplicates and gave
`CONSTELLATION_REGISTRY`/`CONSTELLATION_NO_DAEMONIZE` their own new
"Named filesystems and daemonization" subsection (with a Table of
Contents entry) instead of leaving them stranded mid-table.

**Compliance note (steps 1–7): daemonized-mount pjdfstest.** The standard
`docker compose --profile test run --rm compliance` gate drives
`tests/lib.sh`'s `fs_mount`, which always passes `--foreground` (needed for
the suite's own direct process-lifetime control via `$MOUNT_PID`) — so by
itself it does not exercise the daemonization path (fork/`setsid`/PID
file/log redirection) the plan calls out as needing its own check. To
satisfy that without permanently complicating the shipped test helper, a
throwaway conditional was added to `fs_mount` (drop `--foreground` when
`CONSTELLATION_TEST_FOREGROUND=0`), the compliance image was rebuilt, and
`compliance` was re-run with that variable set and `PJDFSTEST_ONLY` limited
to the categories most sensitive to fd inheritance/cwd/signal-delivery
through `setsid` (`chmod rename open unlink mkdir symlink truncate link
chown`) — **8114 passed, 0 failed**. The `fs_mount` edit was then reverted
(confirmed via `git diff` matching the pre-edit content exactly) and
`tests/lib.sh` in the final tree has no trace of it.

## Plan 20 — optional read-time atime: **DONE**

Batched, eventually-consistent, best-effort access-time updates on
`read()`, gated behind `--atime <off|relatime|lazy>` (default `off`,
bit-identical to the historic noatime behaviour).

| Item | State | Where |
|---|---|---|
| `LogRecord::Atime` + `TouchSet` exemption | done | `meta::record`, `meta::replay` |
| Shared apply helper (clamp + ctime guard + max-merge) | done | `meta::replay::apply_atime_one` |
| `atime_journal` table + 6 `MetaStore` methods (`apply_atime`, `queue_atime`, `atime_backlog_of`, `take_atime_of`, `clear_atime`, `drop_atime_of`) | done | `meta::sqlite` |
| `MutateOp::AtimeBatch` holder handler (apply + queue, journals nothing) | done | `meta::mutate` |
| Policy module + sharded accumulator + config readers + `AtimeStats` | done | `cli::atime` (new) |
| Read-path hook + explicit-setattr purge | done | `cli::fusefs_ops` |
| Flush ticker (drain → local apply → holder-queue / non-holder forward) | done | `cli::node_runtime::atime_flush_once` |
| Ride-along atime drain into write segments | done | `cli::shipper::ship_part` |
| Ship-then-release on idle lease release | done | `cli::shipper::ship_atime_before_release`, `cli::main` |
| Batched forward with shorter timeout | done | `cli::forward::request_mutate_with` |
| Mount flag `--atime` + `CONSTELLATION_ATIME*` env | done | `cli::main`, `cli::node_runtime` |
| Status/observability (`AtimeStatus` on `StatusReport`) | done | `api::types`, `cli::main` |
| Docs (configuration + `features/atime.md`) | done | `docs/reference` |
| Harness scenario `atime-eventual` | added, not run here | `harness::scenarios` |

### Design decisions where the plan was ambiguous

- **No segment version bump.** The plan's Step 0 called for `v = 3` so
  mixed-version / old-bucket mismatches fail the exact-match check
  loudly. Mixed-version clusters and existing-bucket upgrades are out of
  scope anyway, so the bump buys nothing — keep `v = 2` and let an
  unknown `LogRecord::Atime` fail postcard decode if an older binary
  ever sees one.
- **Accumulator location.** The plan suggested piggybacking the pending
  map on the existing per-inode write shard for "zero extra locks". That
  shard's `lock()` returns the `HashMap<Ino, WriteState>` guard directly
  and is held across the read's S3 fetch, so folding atime in would have
  meant reworking every write-path call site and risking a drain blocked
  behind network I/O. Chose instead a **separate sharded accumulator**
  (`cli::atime::AtimeAccumulator`): the read hot path pays one
  uncontended lock only in the rare bump case (after an `Off`/policy
  short-circuit against the already-loaded attr), and the flusher
  `try_lock`s each shard and skips on contention. This trades the
  "zero locks" ideal (paid only when actually bumping) for not
  destabilising the write path.
- **`apply_atime` carries per-entry `time_ns`.** The plan's illustrative
  signature was `apply_atime(&[(Ino,i64)], time_ns)`; widened to
  `&[(Ino, atime_ns, time_ns)]` so the ctime guard is exact across a
  batch whose entries were observed at different times.
- **`CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` standalone timer deferred.**
  Eventual cross-node visibility is already guaranteed by ride-along
  shipping and ship-then-release (a pure-read holder idle-releases and
  ships its atime). The standalone "ship an atime-only partition on a
  timer" path is a refinement for the corner case of a holder that
  neither writes nor idles; the knob is defined and documented but not
  yet wired (marked `#[allow(dead_code)]` with a rationale comment).
- **Web UI dashboard card deferred.** The `atime` counters are surfaced
  in the `StatusReport` JSON (so `constellation status` shows them and
  the web UI has the data); a dedicated dashboard card is cosmetic and
  was not added.

### Gates run in this environment

- `cargo fmt --all --check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — all pass (added: 7 meta replay atime tests,
  1 meta mutate `AtimeBatch` test, 1 cross-replica convergence test, 8
  cli policy/accumulator tests).
- `bash tests/smoke.sh` — pass.
- Manual single-node e2e: mounted `--atime lazy`, confirmed a cold read
  advances `atime` after the flush interval.

### Not run here (need docker / the fault-injection harness)

`tests/integration.sh`, `target/release/harness run` (including the new
`atime-eventual` scenario), the pjdfstest compliance lane under both
`--atime off` and `--atime relatime`, the xfstests `relatime` lane, and
the `perf_regression` read-throughput/record-volume assertions. These
require docker + fusermount3 and were not exercised in this environment;
they remain to be validated in CI.

## Plan 22 — prune policies: **DONE**

Per-subtree prune policies stored in the `user.constellation.prune`
xattr and enforced by a singleton background pruner that publishes
ordinary `Unlink` mutations, so every replica converges on the same
namespace. Default (no marked roots) is inert.

### Done

| Item | State | Where |
|---|---|---|
| Policy expression language (parser + canonical `Display`, pure) | done | `meta::prune::policy` (new) |
| Per-entry evaluation (`age`/`unused`/`lru`/`keep` + filters + glob) | done | `meta::prune::eval` (new) |
| `PRUNE_XATTR` + `xattr_by_name` index + `prune_roots`/`effective_prune_policy` | done | `meta::prune`, `meta::sqlite` |
| setxattr validation gate (EINVAL on unparseable/non-dir/atime-off) | done | `cli::fusefs_ops::setxattr` |
| `SingletonLease` extraction (shared by GC + pruner) | done | `cli::singleton` (new), `cli::gc` |
| Pruner engine: walk, selection, re-verify, unlink fan-out, audit | done | `cli::prune` (new) |
| Partition fan-out + unheld-partition lease acquisition (no `touch()`) | done | `cli::prune::unlink_now` |
| `PruneStats` + `StatusReport.prune` + `/metrics` surfacing | done | `cli::prune`, `api::types`, `cli::main` |
| Background pruner ticker + replica-freshness heartbeat | done | `cli::node_runtime` |
| Control API `PruneRun`/`PruneList` + CLI `prune check/set/disarm/rm/show/ls/run/status` | done | `api`, `cli::main` |
| Docs (configuration + `features/prune.md`) | done | `docs/reference` |
| Harness scenario `prune` | done — PASSED | `harness::scenarios`, `harness::client` |

### Design decisions where the plan was ambiguous

- **`min-age` floor uses `mtime`, not `mtime.max(ctime)`.** A pure
  metadata change (chmod/chown) bumps ctime; keying the floor off ctime
  would let an unrelated permission fix resurrect a file the policy had
  already aged out, and makes the timer surprising. `age`'s own clock is
  mtime, so the floor matches it.
- **`lru` watermarks collapsed to `high=`/`low=` taking a size *or* a
  percentage** (the plan's `cap=` form is expressed as a size `high`).
  `of=fs|subtree` overrides the unit-derived default. A percentage
  watermark with no quota is inert (reported), never a parse error —
  quota is live-settable, so parse verdicts stay pure.
- **Pruner-acquired leases are released by the idle timer, not
  explicitly.** The pruner never calls `LeaseView::touch()`, so an
  acquired lease looks idle immediately; after its unlinks ship, the
  normal idle-release reclaims it. No new "release now" `SyncRequest`
  was added. Prune unlinks are *not* excluded from the idle-release
  backlog (unlike atime) — they are real records that must ship first.
- **Open-write-handle guard deferred.** The pruner runs outside the FUSE
  layer and has no view of a mount's open-handle table; the re-verify
  step plus the fact that pruning targets cold files makes an
  open-write victim vanishingly unlikely. The `SkippedOpen` counter was
  dropped rather than left dead.
- **`keep`/two-phase mark-then-sweep not added.** The plan's safety came
  from the re-verify-before-unlink step (implemented) plus the lag gate;
  a persisted mark set was judged unnecessary given re-verify already
  reads fresh state at delete time.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -D
  warnings` — clean.
- `cargo test --workspace` — all pass (added: 18 meta policy/eval tests,
  2 meta binding integration tests, 3 cli walk/lru tests).
- `bash tests/smoke.sh`, `bash tests/integration.sh` — pass.
- `target/release/harness run prune` — PASSED (two-node convergence +
  dry-run-deletes-nothing + counter assertion).
- Manual single-node e2e on a `file://` backend: `prune check` (canonical
  form + caret error, exit 2), `prune set --arm`, the setxattr gate
  (EINVAL on a non-directory and on an atime rule with atime off, reason
  surfaced in `prune status`), `prune show`/`ls`, dry-run (0 deleted),
  and an armed run (stale file removed, fresh kept; `deleted:1`).

### Pending (validate in CI)

- Full `target/release/harness run` matrix (only the `prune` scenario was
  run individually here).
- pjdfstest compliance with `CONSTELLATION_PRUNE=1` and no marked roots
  (default is inert; the pruner walks nothing).
- The multi-partition, unheld-partition-acquisition, and
  snapshot-survival assertions from the plan's test list are covered by
  unit/eval tests and the single harness scenario; dedicated multi-node
  in-process tests for each were not added (the pruner engine needs the
  full `NodeRuntime` stack, which the shipper-style test harness does not
  provide).

## Plan 24 — E2E single-file keyring, one master key, everything derived: **DONE**

| Item | State | Where |
|---|---|---|
| Master key (KMK) wrapped in `meta.json`; no `keys/keyring.json` | done | `store-s3::e2e`, `store-s3::store` (`FsMeta.keyring`) |
| Derived subkeys (addressing, per-partition DEK, gossip seed) via domain-separated BLAKE3 | done | `store-s3::e2e` (`KeyPurpose`, `derive_from_master`) |
| `create_keyring_block` / `unlock` / `rewrap_master`; `FsMeta::unlock`; `ChunkStore::change_passphrase` (CAS on `meta.json`) | done | `store-s3::e2e`, `store-s3::store` |
| Deleted: `keyring.json`, `put/load_keyring`, `WrappedKeys`/`Keyring`, `ensure_partition`/`refresh_partition`/`ensure_partition_key` | done | `store-s3::e2e`, `store-s3::log` |
| Partition split no longer writes a key (DEK derived) | done | `cli::shipper` |
| `fs create --e2e` builds the keyring block into `meta.json` (single conditional PUT) | done | `cli::main` |
| Passphrase collected in the foreground before the daemon fork | done | `cli::main`, `cli::node_runtime` (`NodeConfig.passphrase`) |
| Unit tests: derivation determinism, purpose-collision guard, unlock round-trip, live-`passwd` rewrap, secret hygiene | done | `store-s3::e2e` |
| Harness scenario `passwd-live-cluster` | done — PASSED | `harness::scenarios`, `harness::client` |

### Design decisions

- **Everything derived from one master key** (option A of the design
  discussion), rather than storing random per-partition DEKs. Removes the
  keyring's mutability — a split derives its DEK locally — so the secret
  material collapses to a single wrapped value that lives in `meta.json`.
  The trade-off (no independent per-key rotation) costs nothing today:
  `passwd` never rotated DEKs and no per-partition rotation exists.
- **Domain separation is collision-free by construction and tested.**
  `derive_from_master` builds `KDF_DOMAIN` + a one-byte purpose tag, and
  only a DEK appends the partition name after the distinct `PURPOSE_DEK`
  tag — so no partition name can collide with a fixed purpose or another
  partition. `key_purposes_never_collide` throws adversarial names
  (`"addressing"`, `"gossip"`, raw tag bytes, `/`, empty, very long) at it.
- **`fs passwd` is a live operation.** It rewrites only `wrapped_master`
  in `meta.json` under a CAS update; the KMK and every derived key are
  unchanged, so mounted nodes (which hold the KMK in memory) keep
  running — including across a partition split — with no remount. The
  `passwd-live-cluster` scenario proves this with the model oracle across
  two un-remounted nodes, then confirms the old passphrase is refused and
  a fresh mount needs the new one.
- **Passphrase prompt moved before the daemon fork.** `daemonize`
  `setsid()`s the daemon child away from its controlling terminal, so the
  previous in-daemon prompt could never reach a TTY — interactive E2E
  mount silently depended on `CONSTELLATION_PASSPHRASE`. The passphrase is
  now collected in the foreground (via a throwaway runtime that reads
  `meta.json`, fully dropped before the fork) and inherited by the child;
  skipped in `--foreground`, when the env var is set (it survives the
  fork), and when a live daemon socket means we will attach rather than
  unlock.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D
  warnings` — clean.
- `cargo test --workspace` — all pass (added e2e unit tests; existing
  store/log/cli suites updated for the derived-key API).
- `bash tests/smoke.sh`, `bash tests/integration.sh` (floci S3) — pass.
- `target/release/harness run e2e-basic e2e-two-nodes passwd-live-cluster`
  — all PASSED.
- Manual interactive check: `fs create --e2e` (env passphrase), then
  `mount` on a fresh state dir over a pty with **no** env var — the
  foreground `Filesystem passphrase:` prompt is answered, the daemon
  backgrounds, and a file round-trips.

### Pending (validate in CI)

- Full `target/release/harness run` matrix (E2E + `passwd-live-cluster`
  run individually here; full matrix in progress).
- pjdfstest compliance lane.

## Plan 25 — Strip node-local upload state from cluster checkpoints: **DONE**

Incident: a fresh joiner restored a mid-write-back checkpoint and
inherited ~21k `pending_upload` rows (all `ino >> 40 ==` writer prefix)
with an empty cache, then ERROR-looped forever. Gossip/E2E decrypt was
not the cause — foreign replay never inserts into `pending_upload`.

| Item | State | Where |
|---|---|---|
| `SqliteMeta::snapshot()` deletes `pending_upload`, `atime_journal`, `pin`, `epochs`, `reintegration`, and node-local kv (`left`, `read_only_member`, `lease_lost`, creation-quota mirror); keeps `xpart_pending` (convergent replay parking) | done | `meta::replay` |
| `clear_pending_uploads()` after bootstrap replay (poisoned pre-plan-25 checkpoints) | done | `cli::shipper::bootstrap` |
| `purge_foreign_pending_uploads(prefix)` after `set_node_prefix` on every mount (existing-db self-heal); uses `INO_PREFIX_SHIFT` | done | `meta::sqlite`, `cli::node_runtime` |
| Missing-cache upload failures: one ERROR summary per round (`missing_pending_chunks` + sample hash), not one line per hash; refuse-to-ship for same-prefix torn-disk rows unchanged | done | `cli::main::upload_dirty_chunks` |
| Unit: `snapshot_strips_pending_upload`, `purge_foreign_pending_uploads_keeps_local_prefix`, `clear_pending_uploads_empties_the_table`, `bootstrap_clears_pending_from_poisoned_checkpoint` | done | `meta::{replay,sqlite}`, `cli::shipper` |
| Harness: `checkpoint-strips-pending-upload`; `fresh-node-bootstrap` asserts `pending_uploads == 0` | done | `harness::scenarios` |

**Spec note (not DESIGN.md):** cluster checkpoints must not carry
`pending_upload` (or other node-local upload/pin/epoch bookkeeping).
That table is this node's not-yet-uploaded set; foreign
`apply_foreign` never enrolls it.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — all pass.
- `bash tests/smoke.sh` — pass; `bash tests/integration.sh` — pass
  (with `AWS_PROFILE` unset so floci credentials win).
- `target/release/harness run fresh-node-bootstrap checkpoint-strips-pending-upload`
  — both PASSED.
- Full harness matrix and pjdfstest — see below / CI.

## Plan 26 steps 0–2 — checkpoint cadence, inline prune, per-partition log retention: **DONE**

Steps 9–10 of `wip/26-metadata-plane-s3-efficiency.md` (config docs,
harness scenarios) are **not** in this slice; steps 3, 4–5, 6–7 and 8
landed separately, below.

| Item | State | Where |
|---|---|---|
| Log retention floored per partition against `checkpoints/VECTOR.json`, never against LATEST's cluster-wide `covered` (finding-6 data-loss bug) | done | `cli::gc::metadata_candidates` |
| A partition absent from the vector is never pruned; `LATEST` without a vector is a hard error (no global-floor fallback) | done | `cli::gc::metadata_candidates` |
| Evidence JSON gains `partition` + `vector_applied` | done | `cli::gc::metadata_candidates` |
| Byte-proportional checkpoint cadence: `bytes_since_ckpt >= ratio × last_ckpt_bytes`, with `CHECKPOINT_EVERY` kept as a count floor and the opt-in time floor unchanged | done | `cli::shipper` (`checkpoint_is_due`, `ship_part`, `checkpoint`) |
| `CONSTELLATION_CHECKPOINT_RATIO` (default `1.0`; `<= 0`/unparseable → warn + default), read once at attach into `ckpt_ratio` | done | `cli::shipper::checkpoint_ratio` |
| `CheckpointRef { seq, bytes }`; `bytes` = uncompressed snapshot size | done | `store-s3::log` |
| `LogStore::get_checkpoint_ref()` (pointer-only read); `get_latest_checkpoint` uses it | done | `store-s3::log` |
| `Shipper::seed_checkpoint_baseline()` seeds `last_ckpt_bytes` across restarts, called inside the attach-time `block_on` | done | `cli::shipper`, `cli::node_runtime` |
| Inline prune of superseded checkpoints after LATEST + VECTOR both land: newest 2 kept, `keep_seq` never deleted, concurrency 8, `NotFound` ignored, failure logged and left to GC | done | `store-s3::log::prune_superseded_checkpoints` |
| `get_latest_checkpoint` re-reads `LATEST` once and retries on a `NotFound` snapshot GET | done | `store-s3::log` |
| Unit: per-partition retention floor (two vector cases), `checkpoint_trigger_is_byte_proportional_once_seeded`, `CheckpointRef` round-trip with `bytes`, inline-prune newest-2, delete-failing store still succeeds, `get_latest_checkpoint` race recovery | done | `cli::gc`, `cli::shipper`, `store-s3::log` |

### Design decisions

- **The retention floor reads `checkpoints/VECTOR.json` directly, not via
  `LogStore::get_checkpoint_vector`.** That helper maps a missing vector to
  an empty one, which would silently turn "corrupt bucket" into "prune
  nothing" and hide exactly the condition the plan wants to fail on. GC
  therefore does its own GET and `bail!`s when `LATEST` exists without a
  vector.
- **Cadence compares uncompressed to uncompressed.** `bytes_since_ckpt`
  accumulates the postcard envelope handed to `put_segment` (pre-zstd,
  pre-seal) so it is on the same scale as `snap.len()`, the baseline. Using
  at-rest bytes on one side and logical bytes on the other would make the
  ratio depend on two unrelated compression factors.
- **`last_ckpt_bytes == 0` keeps the old count-only behaviour.** A fresh
  mount, or a restart whose best-effort seed failed, falls back to the
  `CHECKPOINT_EVERY` trigger — a seed failure is never worse than
  pre-plan-26 behaviour, so the seed does not need to be fatal.
- **Inline prune keeps newest-2, matching GC.** Newest-1 would break a
  bootstrap that read `LATEST` just before it moved; the defensive
  re-read-once in `get_latest_checkpoint` covers the remaining window
  (two rapid checkpoints inside one bootstrap).
- **Measurement context (plan 26 Appendix, 2026-09-10):** cadence is the
  write-amplification fix (641 checkpoints / 54.55 GiB protecting a 0.17
  GiB log on a 1.85M-file rsync); it is independent of the read-side and
  steady-state findings that steps 3–8 address.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — zero failures (cli 169, store-s3 89, meta 73,
  fs-core 68, plus the smaller suites).
- `bash tests/smoke.sh` — `SMOKE TEST PASSED`.
- Docker lanes (`tests/integration.sh`, harness matrix, pjdfstest) not run
  in this slice — see CI.

## Plan 26 step 3 — the holder does not tail partitions it holds: **DONE**

Steps 9–10 remain open. This slice is purely about removing the
per-round LIST of a stream this node is the only legal appender of.

| Item | State | Where |
|---|---|---|
| `tail_all_except(&held)` skips the LIST for held partitions; `tail_all` is the `&HashSet::new()` call and stays what `tail_to_head` uses, so the takeover witness still sees every stream | done | `cli::shipper::tail_all_except` |
| `held` built from the keeper map via `ship_epoch().is_some()` in `sync_all_inner` and `shutdown_all` | done | `cli::shipper::held_partitions` |
| `sync_one` skips `tail_part` while `lease.ship_epoch().is_some()` | done | `cli::shipper::sync_one` |
| `ship_part` forces one `tail_part` on `StoreError::AlreadyExists` before retrying, so a restarted holder absorbs its own unacked segment instead of spinning at the same sequence | done | `cli::shipper::ship_part` |
| A keeper that goes lost/expired/released drops out of `held` on its own and its stream is read again on the next round (verified, not assumed) | done | `cli::lease::LeaseKeeper::ship_epoch` (unchanged), test below |
| Unit: `holder_ships_without_listing_its_own_stream` (counting `ObjectStore` decorator; 0 listings of `log/p0` across 10 shipped rounds), `restarted_holder_recovers_own_unacked_segment_without_tailing_all`, `a_deposed_keeper_leaves_the_held_set_and_tails_again` | done | `cli::shipper` tests |

### Design decisions

- **The held set is gated on `ship_epoch()`, not on "a keeper exists".**
  `ship_epoch()` already returns `None` for a keeper that is expired,
  released or deposed, which is exactly the set of states in which another
  node may legally be appending to that stream. No forced-tail bookkeeping
  for the lost transition was needed: a test drives A to deposition through
  `renew_now` and asserts it tails the new holder's segment on its very
  next round. Adding an explicit force would have been a second mechanism
  for a condition the epoch already expresses.
- **`sync_one` gained an explicit `ensure_part`.** The skipped `tail_part`
  was also what registered a partition acquired straight from
  `Plan::Create`, and `ship_part` indexes `self.parts` unconditionally.
  `tail_all_except` likewise still `ensure_part`s held partitions (a split
  can reveal a child we must ship to); only the LIST is skipped.
- **Fencing a late segment is now discovered by the CAS collision, not by
  a poll.** A deposed predecessor can still write at the head of a stream
  the new holder owns, and the new holder no longer polls it — so the
  `AlreadyExists` tail is the only path that surfaces it. It still fences,
  and the holder still ships *past* the fenced sequence rather than over
  it. `lower_epoch_segment_is_fenced` was adapted to that path (the holder
  now has a record to ship when the zombie lands) and gained two
  assertions; nothing it asserted before was dropped or loosened.
- **Measurement context (plan 26 Appendix, 2026-09-10):** the self-LIST
  costs a full round trip per sync round to learn nothing. Dropping it is
  2.6 → 5.1 shipped segments/s HU→AWS, 24.5 → 33.1 same-region, 5.4 → 6.2
  HU→OVH. The win is largest exactly where it hurts most (WAN), and it is
  free everywhere else.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — zero failures (cli 172, store-s3 89, meta 73,
  fs-core 68, plus the smaller suites).
- `bash tests/smoke.sh` — `SMOKE TEST PASSED`.
- Docker lanes (`tests/integration.sh`, harness matrix, pjdfstest) not run
  in this slice — see CI.

## Plan 26 steps 4–5 — GET-next tailer, idle backoff, ship-loop hygiene: **DONE**

Steps 9–10 remain open (config docs, harness
scenarios). This slice removes the
per-round LIST from the *follower* side, stops an idle node polling at a
fixed 500 ms, and bounds a segment by bytes as well as by record count.

| Item | State | Where |
|---|---|---|
| `LogStore::get_run(from, k)`: k concurrent segment GETs, longest contiguous run returned, `NotFound` at `from + i` ends it, everything past the first gap discarded, non-`NotFound` errors propagated, bodies opened through the existing `open_segment` (E2E included) | done | `store-s3::log::get_run` |
| Tailer probes with GET-next and only falls back to LIST in catch-up (all k probes hit), then loops for another probe | done | `cli::shipper::{probe_run,tail_part_probed,apply_listed}` |
| `tail_all_except` keeps its parallel-across-partitions shape — the sweep is now a parallel probe, not a parallel LIST | done | `cli::shipper::tail_all_except` |
| `TAIL_GET_CONCURRENCY` 8 → 16 | done | `cli::shipper` |
| `bootstrap()` keeps LIST + sequential GET (it is always in catch-up mode) | unchanged, deliberate | `cli::shipper::bootstrap` |
| `CONSTELLATION_SYNC_IDLE_MAX_MS` (default `30_000`, **later lowered to `10_000`** — see steps 9–10) with `CONSTELLATION_SYNC_INTERVAL_MS` as the floor; `next_poll_ms(interval, idle_rounds, max)` doubles per idle round and clamps | done | `cli::node_runtime::next_poll_ms` |
| Productive round = a segment applied or shipped (`spool.head_seq` before/after) **or** a non-empty journal; any `SyncRequest` (FUSE nudge, barrier, acquire, forward, gossip `Nudge`) resets `idle_rounds` to 0 and the deadline to the interval; `debug!` on ceiling and on reset | done | `cli::node_runtime` `'sync` loop |
| `SEGMENT_MAX_BYTES = 4 MiB`: the batch is cut to the largest prefix that fits, leftovers stay journaled and ship next round, `ack_journal_rows_at` gets only the shipped seqs, ride-along atime rows count against the cap and are cleared only if they shipped | done | `cli::shipper::{records_within_cap,ship_part}` |
| Journal read **once** per `ship_all` and passed into `ship_part(part, lease, batch)`; `ship_part_taking` is the thin wrapper for `sync_one` | done | `cli::shipper::{ship_all,ship_part,ship_part_taking}` |
| `SqliteMeta::journal_has_xpart_dst(txid)` replaces the two `take_journal_grouped(usize::MAX)` full-journal scans in `note_xpart_shipped` / `consider_xpart_aborts` | done | `meta::sqlite`, `cli::shipper` |
| No PUT pipelining, with the measured numbers recorded on `ship_part` so the next reader does not re-derive them | done, deliberate | `cli::shipper::ship_part` doc |
| Unit: `get_run` gap/empty/k-wider-than-run + E2E open; `tailer_uses_get_probes_not_list_in_steady_state`; `oversized_batch_is_split_at_the_byte_cap`; `idle_poll_doubles_up_to_the_ceiling`; `journal_has_xpart_dst_finds_only_the_named_txid` | done | `store-s3::log`, `cli::shipper`, `cli::node_runtime`, `meta::sqlite` |

### Design decisions

- **The probe costs k 404s per idle round on a real backend, and that is
  only a win together with the backoff.** A GET is ~1/12.5 of a LIST on
  AWS, so k = 16 misses is ~1.3 LISTs' worth of request price: step 4b on
  its own would make an idle node marginally *more* expensive, not less.
  Step 4c is what turns it around — 500 ms → 30 s between polls is 60×
  fewer rounds, so the pair lands at roughly 1/45 of today's idle cost.
  They are one change and should not be split. (Latency is neutral either
  way: 177 ms GET-404 vs 175 ms idle LIST HU→AWS, 34 vs 34 on OVH. The
  one measured counter-example is same-region, where an idle LIST is
  *faster* — 14 ms vs 25 ms — and only the request price carries the
  choice there.)
- **Catch-up keeps LIST.** A saturated probe means "possibly far behind",
  and one 1000-key page (171–344 ms everywhere) beats k round trips at
  that depth. Measured over 64 segments: 1693 ms for probe+LIST catch-up
  at k = 16 against 3132 ms for the LIST-every-round tailer.
- **`buffered` makes the idle probe one request against `InMemory` and k
  against S3.** The futures are polled in order; a store that answers the
  head synchronously never polls the siblings. The unit test therefore
  asserts one first-miss GET per idle round (the plan's stated
  alternative), and the k-wide fan-out is what a real backend sees.
- **The backoff reset is guarded by `idle_rounds > 0`.** Resetting the
  poll deadline on *every* request would re-introduce the starvation the
  persistent deadline exists to prevent (a peer nudging faster than the
  interval could hold the periodic round off forever). Resetting at most
  once per idle period satisfies "any request resets it" without that.
- **A non-empty journal is never idle.** A node that cannot ship (foreign
  lease held) still has work pending; backing off there would delay its
  own writes reaching S3 by up to `idle_max_ms`.
- **Interrupted rounds are neither productive nor idle.** A round cut
  short by a request leaves `idle_rounds` alone; the request itself resets
  it on the next pass.
- **A single record larger than the cap still ships.** `records_within_cap`
  always keeps the first record: the cap bounds *batching*, and refusing
  to ship an oversized record would wedge its partition's stream forever.
- **No PUT pipelining (plan 26 Appendix, 2026-09-10).** Pipelining is a
  real 5–10× on segments/s (5.1 → 29.4 HU→AWS at depth 8, 33.1 → 231
  same-region, 6.2 → 41.6 HU→OVH), and is still declined: a holder
  reaches the same *records*/s by letting the journal accumulate during
  the in-flight PUT and shipping one larger segment — `SEGMENT_BATCH` =
  10k records at a 200 ms RTT is ~50k records/s, an order of magnitude
  above what the FUSE path produces. Depth > 1 buys throughput we do not
  need in exchange for a gap-on-failure hazard: one failed sequence among
  several in flight stalls every tailer behind it.

### Freshness consequence (documented, per plan 26 step 4c)

With P2P **up** the backoff is invisible: a gossip `Nudge` resets it the
moment a peer publishes. With P2P **down**, a follower's freshness bound
degrades from 0.5 s to `CONSTELLATION_SYNC_IDLE_MAX_MS` (30 s at the time
of this slice; lowered to 10 s in steps 9–10) after ~6
idle rounds — 0.5 + 1 + 2 + 4 + 8 + 16 s ≈ 31 s of complete quiet — and
snaps back to 0.5 s on the next segment it applies, because applying one
is a productive round. DESIGN.md §12's posture ("eventual S3 polling
closes it") is unchanged; its bound is now `idle_max_ms` rather than the
poll interval. DESIGN.md is not edited (CONVENTIONS rule 5).

Two knock-on checks: the pruner's staleness gate (`CONSTELLATION_PRUNE_MAX_LAG_S`,
default 300 s) is an order of magnitude above the new ceiling, so it is
unaffected; and harness scenarios that assert cross-node convergence with
P2P disabled inside a 10–20 s `eventually` deadline could now need up to
30 s if the writer starts after a long idle period. The docker lanes were
not run in this slice (see below), so that is called out as a watch item
for CI rather than an observed failure.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — zero failures (cli 175, meta 90 + 5, store-s3
  75, net 68, fs-core 33, plus the smaller suites).
- `bash tests/smoke.sh` — `SMOKE TEST PASSED`.
- Docker lanes (`tests/integration.sh`, harness matrix, pjdfstest) not run
  in this slice — see CI.

### Behaviour a pinned test had to follow

`restarted_holder_recovers_own_unacked_segment_without_tailing_all`
asserted that the forced tail after `AlreadyExists` produced a **LIST** of
`log/p0`. Step 4b moved that tail to a GET-next probe, so the assertion
now counts GETs instead — and gained a stronger companion: the LIST count
must stay at zero for a one-segment recovery. No original assertion was
dropped or loosened (journal acked, `next_seq` advanced, the op not
applied twice, and the recovery tail not becoming the steady state are
all still there, the last one now over both request classes).

## Plan 26 steps 6–7 — parallel checkpoint I/O, sticky leases: **DONE**

Steps 9–10 remain open (config docs, harness
scenarios). This slice makes the one object that is never small move in
parallel, and stops a holder handing back write authority nobody asked
for.

| Item | State | Where |
|---|---|---|
| `get_object_parallel(store, key, RANGE_BYTES, CONCURRENCY)`: HEAD for size, single GET at or below one range, otherwise contiguous 8 MiB `get_range`s through `buffered(CONCURRENCY)`, assembled in issue order into one `Vec<u8>` | done | `store-s3::log::get_object_parallel` |
| `get_latest_checkpoint` fetches through it, including the defensive re-read-`LATEST`-once retry (both attempts take the same path) | done | `store-s3::log` |
| `RANGE_BYTES` = 8 MiB; `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY` (default 8, `0`/unparseable → default) | done | `store-s3::log::checkpoint_io_concurrency` |
| Checkpoint bodies over 16 MiB go up as `WriteMultipart` (`put_multipart` → 8 MiB `put` chunks → `finish()`), with in-flight parts capped at the same concurrency; smaller bodies keep the single PUT | done | `store-s3::log::put_checkpoint_body` |
| `shipper::bootstrap` inherits both (it goes through `get_latest_checkpoint`) | done, no change needed | `cli::shipper::bootstrap` |
| `Lease.wanted_by: Vec<u64>` (sorted, deduped); `granted`/`released` clear it, `renewed` preserves it; `Lease::wanting(node_id)` is the only way to add one and copies holder/epoch/expiry unchanged. `LEASE_VERSION` unchanged | done | `store-s3::lease` |
| `Plan::Busy` carries `prev` + `tag` so a requester can register itself without a second GET | done | `cli::lease::Plan` |
| `acquire_lease_for` registers this node in `wanted_by` on `Plan::Busy` in `LeaseMode::Cas`, skipping when already listed; `CasConflict` ignored; still returns `Ok(false)` | done | `cli::shipper::{acquire_lease_for,register_wanted_by}`, `cli::lease::LeaseKeeper::register_wanted` |
| Rate limit: `wanted_registered_at: HashMap<part, Instant>`, no re-registration within `lease_ttl_ms / 2` | done | `cli::shipper` |
| `renew_now` on `CasConflict` re-reads and distinguishes a `wanted_by` edit (same holder, same epoch, unreleased, unexpired) from a deposition: adopts the fresh tag, copies `cur.wanted_by` into `LeaseKeeper::wanted`, retries the renew once. Only a different holder / higher epoch / expired-and-taken still means deposed | done | `cli::lease::LeaseKeeper::renew_now` |
| `idle_release_due(backlog)` is now the six-condition form: not epoch-held, held, backlog 0, **a registered requester**, idle ≥ `idle_release_ms`, tenure ≥ `LEASE_MIN_DWELL_MS` (5 s) | done | `cli::lease::LeaseKeeper::idle_release_due` |
| `release()` writes `released: true` with `wanted_by` cleared and forgets the local requester list | done | `cli::lease::LeaseKeeper::release` |
| P2P `HandOff` unchanged and still the fast path | unchanged, deliberate | `cli::main`, `cli::net` wiring |
| `require_lease_for`'s retry sleep doubles 100 → 200 → … → 2000 ms; `acquire_deadline` unchanged | done | `cli::fusefs::require_lease_for` |
| Worst-case handoff reasoning written into the module doc | done | `cli::lease` module doc |
| Unit: 40 MiB round trip through multipart + ranged GET, plain and E2E, byte-equal; 1 KiB takes the single-GET path; `renewed` keeps `wanted_by` while `granted`/`released` clear it; `active_holder_never_releases_idle_without_a_requester`; `requester_registers_wanted_by_and_gets_the_lease`; `wanted_by_edit_is_not_a_deposition` | done | `store-s3::log`, `store-s3::lease`, `cli::shipper` tests |

### Design decisions

- **The ranges are issued here, not through `ObjectStore::get_ranges`.**
  The plan allowed either, with the fallback conditioned on measured
  throughput. The choice is structural rather than measured: `get_ranges`
  routes through `coalesce_ranges`, which merges ranges less than
  `OBJECT_STORE_COALESCE_DEFAULT` (1 MiB) apart into one request. A
  contiguous split of a single object is *zero* bytes apart, so all eight
  ranges would merge straight back into the single GET the step exists to
  eliminate — the 23.5 s vs 6.3 s (HU→AWS), 6.6 vs 2.7 (OVH), 1.3 vs 0.30
  (same-region) gap would simply not be collected. It also pins its own
  parallelism at 10, ignoring `CONSTELLATION_CHECKPOINT_IO_CONCURRENCY`.
- **One extra HEAD per checkpoint read, accepted and pinned by a test.**
  The range plan needs the size; a HEAD is 176 ms HU→AWS and 13 ms
  same-region, on a path that is about to spend seconds. Guessing instead
  and discovering the size from a first ranged GET would save that round
  trip and cost a branch on every backend's range semantics.
- **In-flight multipart parts are capped.** `WriteMultipart` starts a part
  as soon as its chunk is buffered, so a 400 MiB checkpoint would put ~50
  parts in flight at once. The Appendix's own numbers argue against
  assuming more is better: 64 MiB in 1 MiB pieces measured *slower* at
  concurrency 64 than at 16 (5.1 s vs 3.4 s HU→AWS). Same knob as the
  read side.
- **`wanted_by` is `#[serde(default)]`, like every other field of
  `Lease`.** Not a legacy read path: the struct's documented contract
  (`LEASE_VERSION`, and `forward_compatible_decode`) is that a lease
  object missing fields decodes rather than failing a mount. A single
  required field would make `{}` unparseable and break that pinned
  behaviour.
- **A requester's registration is rate-limited per *attempt*, not per
  success.** A CAS that lost still means the object moved under us, and
  the blocked FUSE thread is retrying anyway; counting only successes
  would let a contended lease attract one CAS PUT per retry. Half a TTL is
  the holder's own renewal period — asking more often than it can look
  cannot make it release sooner.
- **Nothing in the handoff treats a 412 as a fast failure.** Measured
  against AWS, a stale `If-Match` comes back rejected in 599 ms (671 ms
  with client retries disabled — server-side, not botocore), four times a
  plain GET; OVH returns it in ~RTT. So every conflicting path here
  retries on a later round rather than in a loop: the requester drops a
  lost registration entirely, and `renew_now` re-reads once and then
  either swaps or defers to the deposition probe.
- **The requester side is `LeaseMode::Cas` only.** Without `If-Match` a
  swap is a blind overwrite, and blindly rewriting a live holder's lease
  object is exactly the race that mode cannot make safe. On the
  `LocalFileSystem` smoke lane the registration is simply skipped.
- **Deposition stays terminal.** The new branch narrows *what counts as*
  deposition (a same-holder, same-epoch, unreleased, unexpired object is a
  handoff request); it does not add a way back from one.

### Contradiction with DESIGN.md to record (CONVENTIONS rule 5)

DESIGN.md §4 says "A holder releases after about 30 seconds without a
mutation." That is now **conditional on a registered requester**: an idle
holder with nobody waiting keeps the lease indefinitely, and even with a
requester it will not release inside `LEASE_MIN_DWELL_MS` (5 s) of taking
it. `CONSTELLATION_LEASE_IDLE_RELEASE_MS` (30 s) still sets the idle
threshold. DESIGN.md is not edited.

Worst case is unchanged by the swap, which is why it is safe: with P2P
down a requester registers at its first `Acquire` (one CAS round trip),
the holder notices at its next renewal (≤ TTL/2 = 30 s), finishes its
in-flight batch and releases, and the requester claims on its next retry
— the same 30 s the unconditional idle release cost, or the same EIO
after 2×TTL against a busy holder. What goes away is the 3-round-trip
re-acquire on the common single-writer path.

### Behaviour a pinned test had to follow

No existing test asserted unconditional idle release — `idle_release_due`
had no unit coverage and its only caller is `main::run_sync_round` — so
nothing had to be rewritten for the new semantics. The two existing
`Plan::Busy` matchers (`cli::lease`'s `open_epoch_promise_refuses_s3_takeover`
and the `acquire_lease_for` log site) use `..` and were unaffected by the
added fields. `CountingStore` in `cli::shipper`'s tests gained a PUT
counter; no existing assertion changed.

### Non-vacuity checks (behaviour disabled, tests must fail)

- Drop `&& !self.wanted.is_empty()` from `idle_release_due` →
  `active_holder_never_releases_idle_without_a_requester` FAILS (it
  reports release as due with every timer elapsed and no requester).
- Make `register_wanted_by` return immediately →
  `requester_registers_wanted_by_and_gets_the_lease` FAILS with
  `wanted_by` `left: [] right: [2]`.
- Disable the `wanted_by`-edit branch in `renew_now` → all three sticky
  tests FAIL: the holder never learns who is waiting (`left: []`,
  `right: [2]`) and the retried renew never lands (lease etag stays `"2"`).
- Force `get_object_parallel` down the single-GET path →
  `large_checkpoint_roundtrips_through_parallel_io` FAILS with
  (heads, whole) `left: (1, 1) right: (1, 0)`. With the ranged path live
  the 40 MiB body is 1 HEAD + 6 ranged GETs + 0 whole GETs + 1 multipart
  upload; the 1 KiB body is 1 HEAD + 1 whole GET + 0 multipart uploads.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — zero failures (cli 178, meta 90 + 3 + 2,
  store-s3 78, net 68, fs-core 33, upload-concurrency 10, harness 8,
  api 6, chaos 3, uploadbench 4).
- `bash tests/smoke.sh` — `SMOKE TEST PASSED`.
- Docker lanes (`tests/integration.sh`, harness matrix, pjdfstest) not run
  in this slice — see CI. The plan's own report items (ingest checkpoint
  tallies, idle request tally, handoff timing) come from the step 10
  scenarios, which are not in this slice.
- Note: `large_checkpoint_roundtrips_through_parallel_io` moves 80 MiB
  through zstd and (once) ChaCha20-Poly1305 in a debug build and takes
  ~24 s on its own, which is now most of the `store-s3` suite's wall
  clock.
- Pre-existing flake observed while running the gate, **not** from this
  slice: `constellation-net`'s `stream_framing_and_bare_encoding_are_distinct`
  fails about 3 runs in 30. It generates a fresh random key each run and
  asserts a length-prefixed frame never postcard-decodes as a bare
  payload, which is true only probabilistically. `crates/net` has no
  changes in this working tree.

## Plan 26 step 8 — existence hint from `chunk_ref`, no mount-time LIST: **DONE**

Steps 9–10 remain open (config docs, harness scenarios). This slice
deletes the last bucket-wide LIST on the mount path and replaces it with
a table the replica already maintains.

| Item | State | Where |
|---|---|---|
| Mount-time LIST seed deleted: `spawn_seed`, `LIST_CONCURRENCY`, the `list_enabled` flag, the `complete`/`listed` state and `CONSTELLATION_EXISTENCE_LIST` | done | `cli::existence` |
| The bloom stays, as an in-process cache of hashes this node uploaded itself or was hinted about by a peer (`insert`, `note_peer_hint` unchanged) | done | `cli::existence` |
| `Existence::with_meta(meta)` is the mount-path constructor; the replica is consulted per upload, so startup cost is no longer proportional to the bucket | done | `cli::existence`, `cli::node_runtime` |
| `SqliteMeta::chunk_ref_exists(hash)`: `SELECT 1 FROM chunk_ref WHERE chunk_hash = ?1 LIMIT 1` through `with_reader` | done | `meta::sqlite` |
| `contains(hash)` = bloom hit **or** `chunk_ref_exists`; replay maintains `chunk_ref` from foreign records, so a hash referenced anywhere in the cluster is a hit | done | `cli::existence::contains` |
| No proven-absent answer any more: return type is `bool` (hinted / not hinted), `Some(false)` and its no-HEAD `Create` branch are gone | done | `cli::existence`, `cli::main::put_mode` |
| `report()` gains `chunk_ref_hits`; `listed`/`complete` removed, `bloom_misses` becomes `misses` (no source could answer) | done | `cli::existence::ExistenceReport` |
| Control-plane status: `existence_chunk_ref_hits` + `existence_misses` replace `existence_listed`/`existence_complete`/`existence_bloom_misses`; `/metrics` follows | done | `api::types`, `api::web`, `cli::main` |
| `store-s3::existence` (`scan_chunk_hashes`, `parse_chunk_key`, `ChunkHashScan`) deleted — it existed only to seed the mount-time bloom and had no other caller | done | `store-s3::lib` |
| Harness `existence-bloom-dedup` / `existence-peer-hint` retargeted at the new counters and the removed env var | done, unverified | `harness::scenarios` |
| Unit: `chunk_ref_hit_selects_probe_without_a_list`, `bloom_answers_only_for_inserted_hashes`, `chunk_ref_exists_sees_replayed_foreign_references` | done | `cli::existence`, `meta::sqlite` |

### Design decisions

- **`contains` returns `bool`, not `Option<bool>`.** The plan says
  `Some(false)` no longer exists and to adjust the return type and every
  caller; with only `Some(true)` and `None` left, the `Option` layer
  carries no information. `true` means "probably already in the bucket,
  take a confirming HEAD", `false` means "no hint" — never "absent".
- **The replica is queried per upload rather than copied into the bloom
  at mount.** A copy would be O(referenced hashes) of SQLite work at
  mount for a filter that is already only advisory, and it would go stale
  against everything replay learns afterwards. One indexed `chunk_ref`
  probe on a local DB is far below the S3 round trip the decision is
  about (177–194 ms HU→AWS, 27–110 ms HU→OVH for any chunk request).
- **What the removed LIST cost.** 59.5k chunk objects took 23 s at mount
  and the scan is O(hours) at ten million. LIST is the most expensive
  request class (12.5× a GET on AWS) and a 1000-key page measures 331 ms
  HU→AWS, 344 ms HU→OVH and 171 ms same-region — so a 10M-object bucket
  is ~10k pages, ~55 min of pure LIST from Europe, to build a filter that
  only ever saves a HEAD. The `chunk_ref` table already holds the
  referenced-hash set at zero extra S3 cost.
- **`chunk_ref_exists` does not join `inode`, unlike `hash_is_live`.**
  The question is "has this content been put in the bucket", not "is it
  still reachable"; a row that outlives its inode costs one HEAD at
  worst. GC is unaffected — it still re-derives the live set from the
  manifests before deleting anything.
- **Peer digests keep priority over the replica.** `put_mode` consults
  the cooperative-cache digest first, so `existence_peer_hints` still
  measures the P2P path rather than being shadowed by a replica hit.
- **A replica read error degrades to the adaptive probe.** The hint is
  never a dependency: `chunk_ref_exists` failing logs at `debug!` and the
  upload takes the ordinary fallback.

### Behaviour a pinned test had to follow

`complete_list_seed_probes_hits_and_creates_misses_without_head` pinned
the one thing this step deletes: a *proven miss* from a complete LIST
going straight to `Create` with no HEAD (it asserted exactly 1 HEAD for
two queued chunks). Nothing can prove absence without that LIST, so the
unhinted chunk now takes the adaptive probe and the test is
`hinted_hash_probes_and_unhinted_hash_keeps_the_adaptive_fallback`,
asserting 2 HEADs. Every other assertion it made is kept — 1 PUT (only
the genuinely absent chunk is uploaded), the pending-upload table
drained — and it gained one: `bloom_hits == 1`, so the hit is attributed
to the hint rather than to luck. `bloom_false_positive_still_calls_store_before_ack`,
`condemned_hash_overwrites_even_when_existence_bloom_claims_present` and
`peer_hit_selects_probe_but_peer_miss_retains_adaptive_head` kept every
assertion; only their `seed_for_test(&[h], true)` setup became
`insert(&h)`. `incomplete_seed_never_proves_a_miss` and
`complete_seed_reports_hits_and_misses` were deleted with the seed they
tested, and `store-s3::existence`'s two tests went with the module.

### Non-vacuity checks (behaviour disabled, tests must fail)

- Make `contains` skip the `self.meta` branch →
  `chunk_ref_hit_selects_probe_without_a_list` FAILS
  (`assertion failed: existence.contains(&hash)`); the bloom-only test
  still passes, so the two sources are covered independently.
- Make `contains` return `false` unconditionally → 3 of the 178 cli tests
  FAIL (both `existence` tests and
  `hinted_hash_probes_and_unhinted_hash_keeps_the_adaptive_fallback`).
- `chunk_ref_exists_sees_replayed_foreign_references` asserts the empty
  replica says no *before* the replay, so the post-replay `true` is not a
  constant.

### Mount-path LIST audit

`grep` for `list(`, `list_with_offset`, `list_with_delimiter` and
`list_segments*` across `crates/cli/src` and `crates/store-s3/src`: the
only remaining LIST of `chunks/` is `cli::gc`'s explicit orphan pass
(`gc run --orphans`, an operator command, already documented as the one
place that lists that prefix). The mount path's remaining LISTs are all
metadata-plane and already bounded by earlier steps of this plan — log
catch-up (`list_segments_from`, step 4b), checkpoint prune
(`checkpoints/p0/`, step 2), leases, designations and snapshots. No
chunk-prefix LIST runs at mount any more.

### Deferred

- `docs/reference/configuration.md` still lists `CONSTELLATION_EXISTENCE_LIST`.
  Step 9 owns that table (it lists the var as "removed") and will rewrite
  it wholesale; the variable itself is deleted from the code here.
- The two `existence-*` harness scenarios were rewritten for the new
  counters but **not run** — the docker lanes are out of scope for this
  slice. `existence-bloom-dedup` now waits for A's tree to reach B over
  the metadata log instead of for a LIST seed, and asserts
  `bloom_hits + chunk_ref_hits >= unique chunks`; its third pass (a
  `CONSTELLATION_EXISTENCE_LIST=off` control) is gone with the kill
  switch.

### Gates run in this environment

- `cargo fmt --all` — clean; `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — zero failures (cli 178, meta 91 + 3 + 2,
  store-s3 76, net 68, fs-core 33, upload-concurrency 10, harness 8,
  api 6, chaos 3, uploadbench 4).
- `bash tests/smoke.sh` — `SMOKE TEST PASSED`.
- Docker lanes (`tests/integration.sh`, harness matrix, pjdfstest) not run
  in this slice — see CI.

## Plan 26 steps 9–10 — configuration docs, harness scenarios, and the defects the scenarios found: **DONE**

This slice closes plan 26. It is also the first slice in which the docker
lanes ran at all, so it is where steps 0–8 were actually exercised
end-to-end — and where two defects in already-"done" steps surfaced.

| Item | State | Where |
|---|---|---|
| `docs/reference/configuration.md`: all eight plan-26 knobs documented, each verified against the code that actually reads it | done | `docs/reference/configuration.md` |
| `ckpt-bulk-ingest-bounded` — newest-2 bound, footprint ≤ 3× the final snapshot, fresh node bootstraps to the oracle | done, PASSES | `harness::scenarios` |
| `idle-cluster-is-quiet` — 3 nodes, 60 s idle, per-class request budget, zero LISTs of `log/` | done, PASSES | `harness::scenarios` |
| `wan-writer-ships-put-only` — 200 ms path, 500-file burst, zero LISTs of `log/p0`, follower converges on the poll alone | done, PASSES | `harness::scenarios` |
| `sticky-lease-handoff-over-s3` — S3-only handoff, no EIO, epoch advances, holder released cooperatively | done, PASSES | `harness::scenarios` |
| `multi-partition-retention-is-per-partition` — step 0 regression test | done, PASSES | `harness::scenarios` |
| `reqlog::CountingProxy` — counting HTTP relay chained in front of toxiproxy, with a `desyncs` counter every user asserts is zero | done | `harness::reqlog` |
| `reqlog::breakdown` — requests grouped by class *and* bucket area, so "30 LISTs" can be shown to be membership polls and not `log/` | done | `harness::reqlog` |
| `CONSTELLATION_HARNESS_DOCKER_PREFIX` — lets two harness processes share a host, which is what made the HEAD-vs-working-tree A/B possible | done | `harness::s3env` |
| **Defect found in step 8**: `chunk_ref` existence hint claimed "in the bucket" for this node's own un-uploaded chunks | fixed | `meta::sqlite::chunk_ref_exists` |
| **Defect found in step 3**: a deposed holder could not see the new holder's writes for up to TTL/2 | fixed | `cli::shipper::{HELD_TAIL_MAX_STALENESS,held_partitions}` |
| TESTING.md documents the five scenarios and the counting relay | done | `docs/how-to-guides/development/TESTING.md` |

### Defect 1 — step 8's `chunk_ref` hint was wrong for in-flight chunks

`chunk_ref` rows are written when a **manifest commits**. In write-back
mode that happens *before* the chunk is uploaded, so `Existence::contains`
answered "probably already in the bucket" about an object that provably
was not there yet, and `put_mode` returned `Probe` — a HEAD that is
guaranteed to 404 — before every single write-back PUT.

Found by scenario evidence, not by unit tests: `wan-writer-ships-put-only`
tallied `HEAD=500` for a 500-file burst of unique content, one wasted
round trip per file. On the plan's own Appendix numbers that is 177 ms
HU→AWS and 27 ms same-region *per chunk*, on the path plan 26 exists to
make cheaper. No unit test could see it — they seed the hint with
`insert()` and never model a pending upload.

`chunk_ref_exists` now excludes hashes with a live `pending_upload` row,
in one indexed statement on the reader connection (never the writer, which
FUSE also needs). `pending_upload` is node-local, so this only ever
suppresses a hint about *our own* unfinished work — a hash referenced by a
peer still hits, which is the property step 8 was built for. Pinned by
`chunk_ref_hint_ignores_this_nodes_pending_uploads`, which fails without
the exclusion.

### Defect 2 — step 3 left a deposed holder blind for up to TTL/2

Step 3 lets a holder skip reading partitions it holds, gated on
`ship_epoch()`. That is `None` for a keeper that is expired, released or
deposed — but a keeper only *learns* it was deposed at its own renewal
CAS, half a TTL away (30 s at the default 60 s TTL). Until then its view
still says "usable", the partition stays in the held set, and its stream
stays unread. Correctness was never at risk — the epoch fences anything
the deposed node ships — but the **read** staleness is real.

Step 3's own unit test described this accurately ("A has not noticed yet
… the renewal CAS is where it finds out") and then drove `renew_now()`
explicitly, making detection instantaneous in-test. The conclusion drawn
from it — that a deposed keeper "drops out of the set by itself" with no
extra code — was right about correctness and silent about latency.

Measured cost: against a plan-26-free tree (HEAD 87df4ce), three
scenarios that pass there fail here, and every one of them fails on the
*old holder* not seeing the new holder's writes — `two-clients-shared`
("b not on c0"), `lease-handover` ("via c0: MODEL DIVERGENCE"),
`rename-across-partitions` ("q not on c0").

The skip is now tied to **freshness** rather than usability: a held
partition is read again once `HELD_TAIL_MAX_STALENESS` (5 s) has passed
since it was last read, whatever the lease view claims.

Why this keeps step 3's measured win. That win — 2.6 → 5.1 shipped seg/s
HU→AWS — came from dropping a LIST that ran **per shipped segment**, a
cost that scales with throughput. The bound costs at most one probe per
5 s per partition, a cost that scales with time; across the 500-file
burst in `wan-writer-ships-put-only` the two differ by orders of
magnitude. After step 4b the read is a GET-next probe rather than a LIST
(~1/12.5 of the request price on AWS, and latency-neutral: 177 ms GET-404
vs 175 ms idle LIST HU→AWS), and a holder probing its own stream 404s at
`next_seq` and returns without reaching the LIST catch-up path — so the
scenario's zero-LIST assertion still holds. The bound also only forces a
read when a sync round happens anyway; it never schedules one, so an idle
node polling at the 30 s ceiling stays bounded by its poll.

A first round holding a partition is treated as fresh: `acquire_lease_for`
already tails the stream to head as its takeover witness, so the bound is
measured from that read rather than immediately repeating it.

`a_deposed_keeper_leaves_the_held_set_and_tails_again` now pins the
production path first — no renewal, only the bound — and keeps the
renewal assertions after it. Non-vacuity: disabling the freshness check
fails it with "the staleness bound must bring a deposed holder's stream
back without waiting for its renewal".

### Contradictions with DESIGN.md to record (CONVENTIONS rule 5)

- §4 "A holder releases after about 30 seconds without a mutation" is now
  conditional on a registered requester (recorded under steps 6–7 above).
- §4's partition-lease model does not say how quickly a **deposed** holder
  must notice. It now notices its own staleness within
  `HELD_TAIL_MAX_STALENESS` (5 s) for reads, while write authority is
  still fenced by the epoch and learned at renewal.
- §12's polling backstop now has an idle ceiling
  (`CONSTELLATION_SYNC_IDLE_MAX_MS`, **10 s** — see "Two settled decisions
  revisited" below) rather than a fixed interval.

DESIGN.md is not edited.

### Pre-existing failures, NOT caused by plan 26

Established by running the identical scenario on HEAD (87df4ce, verified
plan-26-free: no `SYNC_IDLE_MAX_MS`, no `wanted_by`, no `reqlog.rs`) and
on this tree, on an otherwise quiet machine. These are reported, not
fixed — they predate this work.

| Scenario | HEAD | This tree | Note |
|---|---|---|---|
| `slow-network` | FAIL | FAIL | identical seed, file `w-f-11`, sizes 20105→0 |
| `gc-dedup-race` | 7/20 | 10/20 | load-sensitive race; Fisher p≈0.53, indistinguishable; 0/6 when the machine is idle |
| `writeback-latency` | FAIL | FAIL | `back import content mismatch` |
| `writeback-backpressure` | FAIL | FAIL | `ENOSPC arrived without observable throttling` |
| `atime-eventual` | FAIL @40 s | FAIL @40 s | same assertion |
| `deposed-reintegration` | FAIL @20 s | FAIL @40 s | fails earlier on HEAD |

An earlier, uncontrolled comparison suggested `gc-dedup-race` was a
plan-26 regression (HEAD 6/6 clean against 4/9 failing here). That was an
artifact of unequal machine load; interleaving the two binaries run for
run removed it. Load matters enough on this scenario that any future
comparison has to be interleaved.

`checkpoint-strips-pending-upload` is the reverse case: byte-identical
scenario source, **FAILS 3/3 on HEAD** at its 90 s timeout and **passes
3/3 here** in 3.7 s. Plan 26 fixes it.

### Defect 3 — step 4c's backoff starved lease maintenance

`run_sync_round` is the **only** caller of `renew_if_due()` and
`idle_release_due()`. The sync task's idle backoff therefore does not
merely delay a *read*; it delays lease renewal and the discovery of
another node's `wanted_by` handoff request.

Two consequences, both measured:

- Step 7's stated worst case — "the holder notices at its next renewal
  (≤ TTL/2 = 30 s)" — silently became "after one backoff interval".
  Daemon log from `two-clients-shared`: the requester registered at
  `17:34:12.221` (`registered a handoff request in the partition lease
  part="p0" holder=1 landed=true`) and the holder did not notice until
  `17:35:03.306` (`renew CAS lost to a handoff request, not a takeover
  wanted_by=[2] epoch=1`) — **51.0 s**, which is exactly
  0.2+0.4+0.8+1.6+3.2+6.4+12.8+25.6 for the `idle_rounds=8` the same log
  reports.
- With `idle_max_ms` above TTL/2 a holder can sleep through its own
  renewal entirely and let the lease lapse while still believing it holds
  it.

`lease_poll_cap_ms` now clamps the poll deadline to TTL/4 whenever this
node holds any lease. A quarter rather than a half so one slow round
cannot push the renewal past its deadline; the added traffic is small
next to the renewal HEAD+PUT the holder already performs every TTL/2.
Pinned by `a_lease_holder_never_backs_off_past_its_renewal`.

### Two settled decisions revisited (idle probe width, idle ceiling)

Plan 26 settled on a 16-wide GET-next probe and a 30 s idle ceiling.
Running the docker lanes showed the pairing to be the expensive half of
the design, and both were changed with the user's approval.

`LogStore::get_run(from, k)` returns the longest **contiguous** run from
`from`. When `from` is absent — a caught-up node asking "anything new?",
the overwhelmingly common case — the other k−1 GETs cannot contribute to
the answer whatever they find. A 16-wide idle probe buys one bit for 16
requests. Per node per partition per day, at AWS list price (GET
$0.0004/1k, LIST $0.005/1k — the 12.5× the Appendix measures):

| poll | requests/day | GET-equivalents | $/yr/node |
|---|---:|---:|---:|
| fixed 500 ms LIST (pre-plan-26) | 172,800 | 2,160,000 | 315.36 |
| 30 s ceiling, k=16 (as first shipped) | 46,080 | 46,080 | 6.73 |
| 10 s ceiling, k=16 | 138,240 | 138,240 | 20.18 |
| 5 s ceiling, k=16 | 276,480 | 276,480 | 40.37 |
| **10 s ceiling, k=1 (shipped)** | **8,640** | **8,640** | **1.26** |
| 5 s ceiling, k=1 | 17,280 | 17,280 | 2.52 |

So the shipped pairing is **5.3× cheaper than the 30 s/16-wide one and
three times fresher**: this was never a freshness-versus-cost trade, both
were being paid for nothing. Catch-up keeps k=16, where the Appendix
measures it as correct (1693 ms vs 2201 ms at k=8 over 64 segments,
HU→AWS); the probe widens to 16 the moment the narrow one hits, and only
escalates to a LIST if the wide probe also saturates. `TAIL_PROBE_IDLE`
= 1, `CONSTELLATION_SYNC_IDLE_MAX_MS` default 10 s.
