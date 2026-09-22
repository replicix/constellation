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
| **Defect found in step 4c**: the idle backoff starved lease renewal and handoff discovery (`run_sync_round` is their only caller) | fixed | `cli::node_runtime::lease_poll_cap_ms` |
| **Defect found in step 7c**: a transient read during a renewal CAS conflict dropped `self.held`, demoting a healthy holder | fixed | `cli::lease::LeaseKeeper::renew_now` |
| Idle probe narrowed to 1 GET, widening to 16 on a hit (settled decision revisited, user-approved) | done | `cli::shipper::TAIL_PROBE_IDLE` |
| `CONSTELLATION_SYNC_IDLE_MAX_MS` default 30 s → 10 s (settled decision revisited, user-approved) | done | `cli::node_runtime::SYNC_IDLE_MAX_MS` |
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

`chunk_ref_exists` now excludes a reference **from an inode this node is
currently uploading for**, in one indexed statement on the reader
connection (never the writer, which FUSE also needs).

The per-inode correlation is the whole fix, and getting it wrong first is
worth recording. An initial version excluded on the *hash* alone, which
looked right and passed every unit test — but `put_mode` runs while
draining `pending_upload`, so the hash being asked about always has a
local pending row. That version silently switched the hint off
altogether, and the harness caught what the unit tests could not:
`existence-bloom-dedup` inverted from `chunk_ref_hits=300 misses=0` to
`chunk_ref_hits=0 misses=300`. What actually makes content "already in
the bucket" is a reference from an inode we are *not* uploading right
now — a peer's replayed manifest, or one of our own already-acked files.

Pinned by two tests that fail without the respective halves:
`chunk_ref_hint_ignores_this_nodes_pending_uploads` (our brand-new
content must miss) and
`chunk_ref_hint_still_hits_when_a_peer_already_uploaded_the_content` (a
peer's content must hit while our duplicate is in flight — this one fails
against the hash-only predicate).

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

### Defect 4 — a transient read during a renewal CAS conflict demoted a healthy holder

`renew_now` `take`s `self.held` at the top. Plan 26's new `CasConflict`
branch then re-reads the lease object to tell a `wanted_by` edit from a
real deposition — with `self.store.get().await?`. On a transient read
failure that `?` returns **without putting the lease back**, so a holder
that was never deposed silently loses its lease handle; with the store
unreachable it cannot re-acquire either, and the next mutation fails with
EIO. The pre-plan-26 code had no intermediate read here (it went straight
to `diagnose_lost_renew`), so the path is new.

Found by reading while root-causing `node-leave`, **not** by that
scenario: the ablation below shows `node-leave` passes with or without
this fix. It is recorded on its own merits — a correctness bug on the
degraded path, fixed by restoring `self.held` before propagating.

### Why `node-leave` regressed, established by ablation

`node-leave` passes on a plan-26-free tree and failed reproducibly here
(38.0 / 38.1 / 38.7 / 38.8 / 39.0 s) with EIO. The errno origin, from the
daemon log with `RUST_LOG=constellation=debug`:

```
ERROR constellation::fusefs: another node holds the partition lease;
      failing the write with EIO waited=11.106991567s part="p0"
```

That is an `acquire_deadline` expiry (`2 × ttl_ms` = 10 s at this
scenario's 5 s TTL; the 11.1 s is the retry ladder's last attempt landing
just past it). The peer holding p0 could not run the sync round that
hands the lease over inside the deadline, because that round is also the
only thing that renews a lease and notices a handoff request.

Each candidate fix was reverted individually against the scenario:

| idle ceiling | TTL/4 lease clamp | `held` restoration | `node-leave` |
|---|---|---|---|
| 10 s | yes | yes | PASS (33.8 s) |
| **30 s** | yes | yes | **FAIL 2/2** |
| 10 s | **no** | yes | **FAIL 2/2** |
| 10 s | yes | **no** | PASS (33.4 s, 30.8 s) |

So it needed **both** the lower ceiling and the TTL/4 clamp, and neither
alone; the `held` restoration is unrelated to it. Both halves are
necessary because the clamp only binds a node while it *holds* something
(`ship_epoch().is_some()`), and the ceiling governs every round where it
does not.

### No harness deadline was raised

Every convergence scenario in this plan passes at its **original**
deadline. Provisional raises were made while the cause was still open and
have all been reverted; `two-clients-shared` (20 s / 30 s),
`lease-handover` (20 s / 30 s), `rename-across-partitions` (20 s),
`atime-eventual` (20 s / 40 s) and `deposed-reintegration` (40 s) are
byte-identical to their pre-plan-26 values. The reverted numbers are not
a weakened suite — they are the original one.

```
=== two-clients-shared PASSED in 236.3s
=== rename-across-partitions PASSED in 194.0s
=== lease-handover PASSED in 178.3s
=== node-leave PASSED in 33.8s
```

### `named-shared-daemon` hangs — pre-existing, and it blocks the matrix

The full harness matrix has never completed on this host, on either tree.
It is not dying: `named-shared-daemon` (the 77th and last scenario)
**hangs indefinitely**, so the run never reports a verdict for it and the
harness never exits.

Diagnosed live while it was stuck: the harness process was blocked in
`Command::output()` on a child `constellation umount myfs:/sub` that had
been running for 4m40s and never returned. The daemon it was unmounting
was healthy and still serving the other mount — its own log shows
`Unmounting .../mnt2`, `FUSE detached`, and then the daemon carrying on
(`joining the gossip topic`). So the sub-mount detaches and the `umount`
command fails to notice and exit. The leaked daemon then outlives the run
holding a live FUSE mount, which is why stray
`constellation mount myfs /tmp/harness-named-daemon-*/mnt1` processes and
their mounts accumulate across sessions.

**Pre-existing, not plan 26**: the same scenario run from the HEAD
worktree (87df4ce, plan-26-free) also hangs — killed by a 420 s timeout
with no verdict, leaving the same stray daemon and mount behind.

Consequence for the gate: `target/release/harness run` cannot be reported
green as a whole while one scenario never reports. Every other scenario
is run and accounted for by excluding this one explicitly. Fixing the
shared-daemon umount drain is out of scope for plan 26 and wants its own
change — it is a `umount` liveness bug in the plan 21 shared-daemon path,
not a metadata-plane one.

## Plan 28 step S2 — `crates/mtree`, the pure data structure: **DONE**

Step S2 of `wip/28-s3-native-metadata-store.md` §11. A new workspace
crate holding the prolly tree (probabilistic B-tree / Merkle search
tree) as a pure synchronous library — no S3, no tokio, no filesystem,
no async, no global state. It is a **productionization of
`bench/prollybench`'s `node.rs` + `tree.rs`**, the code §14 measured,
not a new design: the algorithm is unchanged and every property those
files assert is preserved. No existing crate was touched beyond the
workspace member entries, so the e2e lanes cannot be affected and were
not run (see "Gates" below).

| Item | State | Where |
|---|---|---|
| Node format: magic `MTRE`, explicit `FORMAT_VERSION`, level, entry count, offset table, then leaf `(key, value)` or interior `(first_key, child_hash, agg)` | done | `mtree::node::{encode, NodeRef}` |
| Keys and values opaque at this layer — the §P6 codec stays S3's job, and no structural decision may depend on key meaning | done | `mtree::node` (module docs state the boundary) |
| Boundary function over keys only: `u32::from_le_bytes(blake3(k)[0..4]) < u32::MAX / TARGET` at level 0, a different 4-byte window per level above it | done | `mtree::node::is_boundary` |
| `MIN_ENTRIES`/`MAX_ENTRIES` entry clamps, configurable, canonicality-safe per §14.1; defaults 1 / 256 | done | `mtree::config::Config`, `mtree::node` |
| Bulk build from a sorted iterator, streaming (O(depth × MAX_ENTRIES) memory) | done | `mtree::Tree::build` |
| Point read; ordered cursor with `next` and `seek`; prefix/resumable range scan | done | `mtree::Tree::{get, cursor, cursor_at, range}`, `mtree::Cursor` |
| Incremental `apply` of a sorted key delta (insert/update/delete) returning a new root, cost O(keys changed) | done | `mtree::Tree::apply` |
| Structural `diff` descending only where hashes differ, plus `delta` as applicable edits | done | `mtree::Tree::{diff, delta}` |
| Three-way `merge` returning `Merged::Root` or `Merged::Conflicts` (the exact overlapping key set) | done | `mtree::Tree::merge` |
| §P7 augmented aggregates — bytes, files, keys, max mtime — as a monoid, carried on every interior entry, combined bottom-up | done | `mtree::node::Agg`, `mtree::Tree::aggregate` |
| Leaf→aggregate projection supplied by the caller, because it needs the §P6 encoding | done | `mtree::config::{LeafAgg, no_leaf_agg}` |
| blake3 hashing with a **keyed** mode selectable at construction (§P13), covering node identity *and* the boundary function | done | `mtree::hash::{Hasher, NodeHash}`, `Config::keyed` |
| `thiserror` error enum; node decode rejects malformed input rather than panicking, with a separate O(n) `validate` for trust boundaries | done | `mtree::error::MtreeError`, `NodeRef::{new, parse, validate}` |
| `NodeStore` trait (sync, content-addressed) plus `MemoryNodeStore` with read/write counters | done | `mtree::store` |
| Reachability walk and level census, for S7's mark and for §14.1's table | done | `mtree::Tree::{reachable, census}` |
| Property suite: order independence (50 orders), incremental = bulk byte-identical, delete-then-reinsert, diff O(difference), disjoint merge agreement, exact conflict set, clamp canonicality under deletes, format pinning, seeded fuzz vs `BTreeMap`, corrupted-node no-panic fuzz | done | `mtree/tests/properties.rs` (14 tests) |
| Unit tests co-located per CONVENTIONS (encoding round-trips, version refusal, boundary rate, monoid laws, store semantics) | done | `mtree::{node,hash,store,config,tree}` (25 tests) |
| Added to `members` and `default-members`; `constellation-mtree` workspace dependency declared for S3/S4 | done | root `Cargo.toml` |

### Design decisions where the plan left a choice

- **The entry clamps moved from process globals into a `Config`.**
  prollybench keeps `MAX_ENTRIES` in a `static AtomicUsize`, which
  forced its tests to serialize on a mutex and would have made the
  shape of an on-bucket format depend on process state. `Config`
  carries the clamps, the hasher, and the leaf-aggregate projection, is
  validated at `Tree` construction, and is documented as a *format*
  parameter set rather than a tuning surface: two trees over the same
  key set with different clamps are different trees.
- **`MIN_ENTRIES` defaults to 1, i.e. off.** The plan asks for both
  clamps and both are implemented and tested (including at `(8, 64)`
  and `(16, 16)`), but §14 measured the tree with no lower clamp, and
  every number this crate is sized against comes from there. Raising it
  is now a configuration change, not a format change.
- **The keyed hasher also governs the boundary function.** §P13 only
  says node hashes are keyed, but the boundary function hashes keys too
  and it decides the tree's *shape*. Leaving it unkeyed would publish
  an oracle for split points and node sizes computable from a guessed
  key set — the same side channel ADR-8 is about. Keyed and unkeyed
  trees are both canonical; they differ in every hash and in nothing
  else, asserted by `a_keyed_tree_is_canonical_and_differs_in_every_hash`.
- **Aggregates are a fixed triple plus the key count, with a
  caller-supplied projection.** A generic monoid would have to be part
  of the on-bucket format contract, which nothing could then verify.
  What *is* caller-supplied is the leaf→`Agg` projection, because only
  the §P6 codec knows which key range holds the authoritative inode
  record and therefore which entries may count a file's bytes without
  double-counting the dentry attr copy. The key count is filled in by
  the tree rather than the projection, so it stays a structural fact.
  Per-directory recursive size is deliberately absent and the module
  docs say why (descendants are not contiguous in the key order).
- **`Agg::EMPTY` uses `max_mtime: 0`, not `i64::MIN`.** `i64::MIN` is
  the exact identity for a max over `i64`, but it varint-encodes to ten
  bytes in every interior entry of an otherwise-empty subtree, and an
  mtime of 0 already means "nothing to report" elsewhere in this repo.
  The cost is that a genuinely pre-1970 mtime is masked by a sibling
  with no mtime, which no filesystem this stores will produce.
- **Decode is split into an O(1) `new` and an O(entries) `validate`.**
  Every accessor is individually bounds-checked and returns
  `Malformed` instead of panicking, so the read path pays only the
  bounds checks Rust slicing does anyway and §14.2's lookup numbers are
  not quietly taxed by a validation pass per node view. Key *ordering*
  cannot be checked lazily — a node whose keys are out of order decodes
  fine and merely makes binary search lie — so `parse` (= `new` +
  `validate`) is the documented entry point for bytes arriving off a
  network, which is where S4's pack reader must call it.
- **The cursor is safe rather than clever.** prollybench's `entry()`
  launders a borrow through `std::slice::from_raw_parts` to keep the
  `entry`/`next` loop ergonomic. Here `entry()` takes `&mut self` and
  returns a borrow tied to it, and `diff` uses a private
  `materialize` + `peek` pair so it can hold two entries at once; there
  is no `unsafe` in the crate. `diff` still allocates only for keys it
  actually reports.
- **The store computes no hashes and the tree does no I/O.** `NodeStore`
  is `get`/`put` over `(NodeHash, level, bytes)`; the hash is computed
  by the tree because only the tree knows whether this filesystem
  addresses nodes with plain or keyed blake3. `level` is passed to
  `put` because it is free here and S4 wants it (interior nodes stay
  resident, §14.1's 19.75 MiB) without decoding a header.
- **Node-read counts are asserted through the store's counters.**
  "Diff cost tracks the difference and not the state" is a claim about
  node reads, so `MemoryNodeStore` counts them and the property test
  asserts against the counter, rather than `diff` returning its own
  cost in its signature.

### Relationship to `bench/prollybench`'s encoding

**Changed, deliberately, and the node hashes differ.** prollybench
writes `magic(4) + level(1) + count(4)`, a 9-byte header whose version
lives inside the magic (`MTN1`). The plan requires an explicit format
version, so this crate writes `magic(4) + version(1) + level(1) +
count(4)` — `b"MTRE"`, version 1 — a 10-byte header. Everything after
the header is byte-identical: the same little-endian offset table, the
same `klen/vlen` `u16` prefixes, the same leaf and interior entry
bodies, the same varint aggregates. The entry *partition* is identical
too, because the boundary function is unchanged. Nothing exists on any
bucket in either format, so there is no migration; from here on
`FORMAT_VERSION` is the migration story and
`the_node_encoding_and_root_hashes_are_pinned` is what makes an
accidental change fail loudly.

`bench/prollybench/**` was read and not modified (a parallel S1
measurement is live in there).

### Deliberately deferred

- **The §P6 key codec** — that is S3, and keeping it out is what lets
  the structure and the encoding be versioned independently.
- **zstd, AEAD sealing, packs, the node cache, the commit chain** — S4.
  Node bytes leave this crate encoded and uncompressed.
- **Parallel mark and compaction** (prollybench's `reachable_par`,
  rayon) — S7, and it needs a real store; the sequential `reachable`
  here is the algorithm, and it terminates on shared subtrees.
- **A `Tree::verify` / fsck entry point** — S6, which is where
  "recompute the root hash and compare" is actually wired. The
  primitives it needs (`NodeRef::parse`, `reachable`, `census`) are in.
- **Range aggregates** (`agg` over an arbitrary key range rather than a
  whole subtree) — §P7's per-directory `du` answer needs them, but it
  needs the key encoding first, so it lands with S3/S5.

### Plan 28 S2 exit criteria

- [x] `crates/mtree` exists, is pure and synchronous, and is in both
      `members` and `default-members`
- [x] Node format is explicitly versioned and pinned by a test with
      hard-coded hex and root hashes (plain and keyed)
- [x] Boundary function reads keys only, level-decorrelated, with both
      entry clamps specified and implemented
- [x] Bulk build, point read, ordered cursor (`next`/`seek`),
      incremental `apply`, `diff`, three-way `merge`, and §P7 monoid
      aggregates all present
- [x] Keyed hashing selectable at construction, covering node identity
      and the boundary function
- [x] Malformed node bytes produce errors, never panics — asserted over
      8,000 seeded corruptions
- [x] All seven required property tests green, plus a seeded
      `BTreeMap`-oracle fuzz
- [x] No existing crate's source modified

### Gates run in this environment

- `cargo fmt --all` — no diff (`--check` clean).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — **531 passed, 0 failed**, of which
  `constellation-mtree` contributes 40: 25 co-located unit tests, 14
  property/fuzz tests, 1 doctest. The `mtree` property suite runs in
  ~10 s in a debug build.
- One transient, unrelated failure worth recording:
  `constellation-meta --test checkpoint_concurrency ::
  snapshot_does_not_stall_concurrent_readers` failed once while a
  `cargo clippy --workspace` run was still saturating the host, and
  passed 4/4 afterwards (3× in isolation, 1× in a clean workspace run).
  It is a wall-clock latency threshold in a crate this step does not
  touch and does not link against, so it is load sensitivity in that
  test rather than a regression here.
- `bash tests/smoke.sh`, `bash tests/integration.sh`, the harness
  matrix and pjdfstest — **not run, and cannot be affected.** They
  exercise product wiring; this step adds a crate that no existing
  crate depends on and modifies no existing crate's source, so its
  contribution to every one of those lanes is dead code. Per §11's
  "steps that touch no product code cannot regress the e2e lanes and do
  not run them", and per this plan's gate for S2 ("fmt, clippy, and
  `cargo test --workspace` green. No other crate changes"). The full
  CONVENTIONS list applies from S5 onward, where `cli/shipper` is
  actually touched.

### Spec note

No contradiction with `docs/explanation/DESIGN.md` was found in this
step. The plan's §P1 writes the boundary predicate as
`blake3(k)[0..4]`, which is exactly what level 0 computes; the
per-level window above level 0 is an addition the predicate needs (with
one window for every level, every boundary key of level 0 would also be
one at level 1 and the tree would degenerate into a chain of one-entry
interior nodes). `boundary_rate_tracks_target` and
`level_zero_is_exactly_the_documented_predicate` pin both halves.

## Plan 28 step S4 — pack store, node cache, commit chain: **DONE**

Step S4 of `wip/28-s3-native-metadata-store.md` §11: the storage layer
beneath the metadata tree, in `crates/store-s3`. Three new modules plus
layout helpers, all **additive** — nothing calls them yet. The shipper,
`checkpoints/*`, `LATEST` and `VECTOR.json` are untouched; publishing
from the live replica is S5's job.

| Item | State | Where |
|---|---|---|
| `packs/<hex>` writer: ~1–16 MiB sealed concatenation of per-node zstd frames, self-describing header with a version and a flags byte | done | `store_s3::packs::{build_packs, BuiltPack, PackStore::put_pack}` |
| Packed **in key order** (`(level, first_key)`), which is what turns §P6's key locality into pack locality | done | `store_s3::packs::build_packs` |
| `packs/<hex>.idx` sibling index of `(node hash, level, first key, offset, clen, len)` | done | `store_s3::packs::PackIndex` |
| Single-node reads are ranged GETs into the pack | done | `store_s3::packs::PackStore::get_node_bytes` |
| `mtree::NodeStore` over `fs-core::cache` **verbatim** — same LRU, same reserve-before-accept, same blake3 verify-and-drop | done | `store_s3::node_cache::NodeCache` |
| Resolution ladder memory → disk cache → peer → S3, with the peer tier as a hook (`PeerNodeSource`) so `cli`'s cooperative cache plugs in without a dependency inversion | done | `store_s3::node_cache::{NodeCache, PeerNodeSource}` |
| Concurrent misses: no lock held across I/O, asserted by a store that deadlocks the test if the miss path serializes | done | `node_cache::tests::misses_are_concurrent` |
| Every node out of a pack, a peer or the disk cache goes through `NodeRef::parse`, never the O(1) constructor | done | `node_cache::NodeCache::accept` |
| Resident interior tier under a byte budget (§14.1's ~20 MiB), leaves left to the disk LRU | done | `node_cache::{MemoryTier, node_memory_bytes}` |
| `commits/<seq:016x>`, CAS-created with `If-None-Match: *`, immutable, carrying every §P2 field (`seq`, `parent`, `roots`, `packs`, `author`, `epoch`, `agg`, `intent`, `unix_ms`) | done | `store_s3::commits::{Commit, CommitChain::create}` |
| Head discovery by GET-next probe over `seq+1..seq+k`, LIST only as the catch-up fallback — the same shape as plan 26's segment tailer | done | `commits::CommitChain::{get_run, discover_head}` |
| Optimistic publish: CAS at `parent + 1`, 412 hands the winner to a caller-supplied rebase and retries without dropping the payload | done | `commits::CommitChain::publish` |
| The ordering invariant enforced by construction: unpacked nodes and non-durable packs both refuse the commit | done | `commits::CommitChain::{publish, assert_packs_durable}` |
| `layout.rs` helpers for both prefixes, in the `log_segment`/`checkpoint` style | done | `store_s3::layout::{pack, pack_index, packs_prefix, commit, commits_prefix}` |

### The index lives in a sibling object, not in the commit

§P8 offers both and the sibling wins on one structural argument: **a
pack outlives the commit that wrote it.** A commit names only the packs
*it* created, but most nodes a reader resolves live in packs written by
ancestors — many of them outside the retained commit window, since
§P10b deletes old commits freely and by design. An inline index would
therefore make "read a node" mean "first find the commit that
introduced its pack", an unbounded walk back through history that
becomes *impossible* once retention has deleted that commit, even
though the pack is still live and still reachable. Three secondary
consequences agree: the commit object stays O(new packs) rather than
O(nodes written) (a 10k-op commit rewrites ~1,100 nodes per §14.5,
so ~44 KiB of index in an object that is otherwise a few hundred
bytes); the index's lifetime is exactly the pack's, so S7 deletes a
pair with no cross-object bookkeeping; and a partial replica can fetch
a few KiB of index without touching a 1–16 MiB body. The cost is two
PUTs to write a pack and two GETs to read one whole, both amortized
over ~128 nodes.

The index is **untrusted**: it says where bytes are, never what they
are. Every node is hashed against the hash the caller asked for and
then structurally parsed, so a lying index is a failed read.

### §14.2's one-pack-per-directory property, and the test that holds it

`build_packs` sorts by `(level, first_key)` before filling. §P6 makes a
directory's dentries one contiguous key range; the sort turns that into
pack locality, which is what §14.2 measured as "distinct packs per
`ls -la` = 1". `NodeCache` counts distinct packs touched *at level 0*
— exactly §14.2's column — and
`node_cache::tests::one_directory_is_one_pack` builds 64 directories ×
500 dentries into 256 KiB packs, scans each directory from a cold cache
and asserts **60 of 64 directories are exactly one pack, none worse
than two**. The four stragglers straddle a pack seal, which is a
function of where the seal falls and not of the ordering. A control in
the same test reads 8 dentries from each of the 64 directories against
an equally cold reader and touches many packs, so the assertion is not
vacuous. Level participates in the sort ahead of the key so interior
nodes cluster together, which is what makes S6's interior-only partial
replica a few whole-pack GETs.

### The crash-ordering invariant

Every node and pack a commit names is durable **before** the commit
object is CAS-created. `publish` refuses to run while the node cache
holds unpacked nodes, and re-verifies on every attempt that each pack
the payload names is present (body *and* index) before the CAS. So:

- crash **before** the CAS → orphan packs, which are garbage that S7's
  reachability sweep reclaims and which no reader can see;
- crash **after** it → a commit all of whose nodes provably exist.

There is no third state. Two tests inject a failure into each half of
the window:

- `commits::tests::no_commit_ever_names_a_missing_node` fails every
  `commits/*` PUT, asserts the orphan packs are on the bucket and *no*
  commit exists, walks every commit that does exist and resolves its
  roots from a cold cache, then unfreezes the store and asserts the
  retry lands one commit naming the same content-addressed packs.
- `commits::tests::a_crash_during_the_pack_puts_leaves_only_orphans`
  fails the index PUT of the first pack, asserts the failed seal put
  the batch back on the pending list, asserts nothing committed, then
  re-seals and asserts the half-written pack is *completed* rather than
  duplicated (one body and one index per pack, and the count matches
  the commit's).

CAS contention is
`commits::tests::the_loser_of_a_cas_race_retries_without_losing_its_payload`:
two writers build disjoint trees against the same empty parent, A takes
`seq 1`, B gets 412, sees the winner, and lands at `seq 2` with its own
roots and its own pack list intact.

Head discovery is `head_discovery_probes_then_falls_back_to_list`
(probe finds the head inside the window; a saturated window hands over
to LIST) and `a_gap_falls_back_to_list` (retention deletes 3..8, the
probe sees nothing at all, and only the LIST can find that 10 is the
head).

### Design decisions where the plan left a choice

- **`publish` CASes at `parent + 1`, not at "wherever the head is
  now."** §P3's rule is that a writer holds the parent it read, and
  taking the head at publish time would silently overwrite whatever
  landed in between instead of surfacing it. The caller passes the
  parent; a 412 is then informative rather than an accident.
- **Rebase is a callback, not a policy in this module.** §P3's
  structural rebase (diff the winner against the parent, splice a
  disjoint write-set, re-execute an overlapping one) needs the key
  codec and the operation semantics, neither of which live in
  `store-s3`. `publish` retries the CAS around a caller-supplied
  rebase and guarantees only that the loser's payload is carried into
  the next attempt rather than dropped. S5 supplies the real one.
- **`put` does not touch S3; `seal_packs` does.** `mtree::apply` writes
  one node at a time and a per-node PUT is exactly the shape §P8
  exists to avoid. Pending nodes sit in the disk cache as `Dirty`,
  which is already `fs-core::cache`'s word for "present locally, not
  yet durable upstream, never evicted"; `seal_packs` packs them and
  demotes them to `Clean`. This is also what makes the ordering
  invariant checkable rather than merely documented.
- **The memory tier stops admitting at its budget instead of evicting.**
  A second LRU beside `fs-core::cache`'s would be two eviction policies
  to reason about; overflowing into the disk cache is correct and only
  slower, and §14.1/§14.5 say the interior is ~20 MiB against a 64 MiB
  default.
- **`build_packs` takes its target verbatim; only the env knob is
  range-checked.** §P8's 1–16 MiB exists to stop an operator turning
  packing off, not to stop a caller that computes a size from its own
  shape.
- **A pack hash is its own type.** `PackHash` is neither a `NodeHash`
  nor a `ChunkHash`: the three namespaces are swept by different rules
  (§P10) and confusing them should not typecheck.
- **Duplicate concurrent misses on the same node are accepted.**
  Single-flight would need a per-hash wait map — a lock on the hot path
  — to save a duplicate ranged GET of an 8 KiB immutable node.

### New config knobs

- `CONSTELLATION_PACK_TARGET_BYTES` (default 4 MiB, accepted range
  1–16 MiB) — sealed pack size. Read in `packs::pack_target_bytes`.
- `CONSTELLATION_NODE_MEMORY_BYTES` (default 64 MiB, `0` disables) —
  byte budget for the resident interior tier. Read in
  `node_cache::node_memory_bytes`.
- `CONSTELLATION_COMMIT_PROBE_WINDOW` (default 8) — GET-next probe
  width before the tailer falls back to LIST. Read in
  `commits::probe_window`.

### Left as hooks

- **§P13 AEAD sealing.** Both pack objects carry a `flags` byte and two
  reserved bytes; `FLAG_SEALED_NODES` is defined and *refused* on read,
  so switching sealing on is a flag flip plus a seal/open pair around
  the per-node frames — no format version bump. Commit objects seal the
  same way `log.rs` already seals segments (`encrypt_object` with the
  object key as AAD), and `Commit::v` versions the payload rather than
  the sealing. **Keyed hashing needs nothing further**: `NodeCache`
  takes an `mtree::Hasher`, so a keyed filesystem already addresses and
  verifies nodes under its addressing key; the documented precondition
  is that the `DiskCache` be opened with `open_keyed` under the same
  key, which the caller (S5) wires.
- **S7.** `PackStore::{get_body, contains}` and
  `CommitChain::list_from` are the primitives the sweep and the
  compactor need; `PackIndex` carries each node's level and first key so
  a compactor can rewrite a partially dead pack while preserving key
  order.
- **S6.** `NodeCache::load_pack_indices` is how a reader that did not
  write the tree learns where anything is; a partial replica loads the
  interior packs' indices and nothing else.

### Deliberately deferred

- **Wiring into `cli/src/shipper.rs`** — S5, and the step scope is
  explicitly additive-only.
- **§P3's structural rebase** — needs the §P6 codec and operation
  semantics; the seam is `publish`'s rebase callback.
- **E2E sealing of packs and commits** — hooks only, per the step's
  instruction.
- **A single-flight miss path and pack read coalescing** — measure
  first; §14.9 says the miss path scales, and coalescing adds a lock to
  the path that has to.
- **`HEAD` as a hint object** — §P2 says it is never authoritative and
  the probe already finds the head in one round; adding a second,
  lying source of truth before anything needs it is not free.

### Plan 28 S4 exit criteria

- [x] `packs/<hash>` writer and reader with an index and ranged
      single-node reads
- [x] Packed in key order, with §14.2's one-distinct-pack-per-directory
      property asserted in a test
- [x] `mtree::NodeStore` implemented over `fs-core::cache` verbatim,
      with the memory → disk → peer → S3 ladder
- [x] Concurrent misses, asserted by a test that deadlocks if the path
      serializes
- [x] Every node read from a pack goes through `NodeRef::parse`
- [x] `commits/<seq:016x>` CAS-created with every §P2 field
- [x] Head discovery by probe with LIST as the catch-up fallback
- [x] `InMemory` tests for CAS contention (412, correct retry, payload
      preserved) and for head discovery including a gap
- [x] Crash-ordering invariant tested on both sides of the pack-PUT /
      commit-CAS window
- [x] `layout.rs` helpers for both prefixes
- [x] Additive only: no existing behaviour changed, no `crates/cli`,
      `crates/mtree` or `bench/prollybench` file touched

### Gates run in this environment

- `cargo fmt --all` — no diff (`--check` clean).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — **565 passed, 0 failed**, of which
  `constellation-store-s3` contributes 110 (up from 76): 34 new
  co-located tests across `packs`, `node_cache`, `commits` and
  `layout`.
- `bash tests/smoke.sh` — **SMOKE TEST PASSED.**
- `bash tests/integration.sh`, the harness matrix and pjdfstest —
  **not run.** They need docker and they exercise product wiring this
  step deliberately does not touch: nothing in the product depends on
  the three new modules yet, so their contribution to those lanes is
  dead code. Per §11, the full CONVENTIONS gate list starts at S5,
  where `cli/shipper` is actually wired.
- Environment note: `/dev/fuse` is not exposed inside the sandbox this
  work was done in, so
  `node_runtime::tests::two_views_of_one_node_converge_with_a_peer_and_pin_is_view_agnostic`
  and `tests/smoke.sh` fail there with "No such file or directory".
  Both were re-run with `/dev/fuse` available and pass; the figures
  above are from that run.

### Spec note

No contradiction with `docs/explanation/DESIGN.md` was found in this
step. One wording note for the coordinator, not a contradiction: §P8
writes the index as "`packs/<hash>.idx` — or inline the index in the
commit" as if the two were interchangeable. They are not — an inline
index cannot survive commit retention, as argued above — so the choice
is forced rather than free, and the plan reads as though it is open.

## Plan 28 step S7a — reachability mark and pack compactor: **DONE**

Step S7a of `wip/28-s3-native-metadata-store.md` §11 — the library half
of metadata GC, in `crates/store-s3`. Three new modules (`mark.rs`,
`compact.rs`, `parallel.rs`, 2,195 lines) plus one additive `pub fn` in
`packs.rs`, all **additive**: nothing calls them, no `crates/cli` file
is touched, and today's chunk GC — the `deref` table, the deref-txid
bookkeeping, the `superseded-checkpoint` rule, the `gc.horizon`, the
condemned-list handshake, holds, reintegration verification — is
unchanged. §P10 retires the first three only once the tree is
authoritative for metadata, which is well past this step. S7b owns the
`cli/gc` wiring, the retention policy and the rate budgets.

| Item | State | Where |
|---|---|---|
| Reachability mark from a root set of commit sequence numbers → live node hashes + live packs | done | `store_s3::mark::{mark, live_set, LiveSet}` |
| Walk terminates on shared subtrees, so marking N commits costs O(their differences) — asserted, not assumed | done | `mark::tests::marking_a_chain_costs_the_differences` |
| Parallel work-stealing frontier over not-yet-visited child hashes, deduplicated by hash, on a private rayon pool | done | `mark::mark`, `parallel::thread_pool` |
| `packs/*` catalogued by one LIST plus a concurrent index fetch, because a pack outlives the commit that wrote it (§P8) | done | `store_s3::mark::PackCatalog` |
| Pack classification into fully live / fully dead / partially dead, from the `.idx` entries alone with no node body decoded | done | `store_s3::compact::{Sweep::classify, PackFate, PackVerdict}` |
| Fully dead packs deleted with their `.idx` sibling at zero rewrite cost | done | `compact::Compactor::delete_dead` |
| Partially dead packs rewritten: survivors read out, re-emitted **in key order**, originals retired afterwards | done | `compact::Compactor::compact` |
| The ordering invariant — a replacement is durable before the original is deleted — with a failure injected on each side | done | `compact::tests::a_failed_{replacement_put,delete}_leaves_every_live_node_readable` |
| Concurrent rewrite path: independent output packs compressed and sealed in parallel, byte-identical to the serial writer | done | `packs::{build_packs_concurrent, build_packs_in}` |
| Restartable cursor and batch size on both sweep paths | done | `compact::{Reclaim::next, batch_of, cursor_after}` |
| Rate-budget mechanism for S7b to drive, with no policy in this crate | done | `compact::{CompactionPacer, Unpaced}` |

### The mark, and what the test actually proves

§P10's affordability claim is that marking a retained window of N
commits costs O(their differences) rather than O(N × state), because
content addressing makes an unchanged subtree the *same* subtree in
every commit that contains it. `marking_a_chain_costs_the_differences`
turns that into an exact identity rather than a plausible inequality.
`MemoryNodeStore` counts distinct writes, so the reachable set of a
17-root chain is knowable independently of the walk: it must be the
base tree's node count plus exactly the nodes the 16 later commits
introduced. Measured on a 60,000-key tree with four keys changed per
commit:

| root set | reachable nodes | node visits |
|---|---:|---:|
| 1 (the base tree) | 574 | 574 |
| 17 (base + a 16-commit chain) | 690 | 690 |

- **`visits == nodes` in both rows**, which is the property: a shared
  subtree is entered once across the whole root set. Any re-entry makes
  `node_visits` exceed `nodes.len()` and the test fails.
- **690 visits for 17 roots against 574 for one.** A walk that restarted
  per root would cost 17 × 574 = 9,758. The marginal cost of a retained
  commit is 7.25 nodes.
- The non-vacuity guard is in the test: the chain introduces 116 nodes
  against a 574-node tree, so the inequality is not passing because the
  deltas happen to be large.

This is the same structural fact §14.6 measured for `diff` (a one-key
diff of a 35.8M-key tree costs 20 node reads), applied to the walk.

The walk reads nodes through `NodeRef::new`, not `parse`, and the
consequence is stated in the module doc rather than left implicit: a
mark is only as sound as its store's verification, and a corrupt
interior node that hid a live subtree would be the one corruption that
loses data instead of leaking space. `NodeCache` is why that is safe —
it hash-checks and `parse`s every byte that arrives from a pack, a peer
or the disk cache — and re-validating in the walk would double its cost
to re-check bytes this process just verified.

### The crash-ordering invariant for a rewrite

S4's invariant was "every pack a commit names is durable before the
commit exists". The rewrite's mirror image is **a replacement pack must
be durable before the original it replaces is deleted**, and the two
crash outcomes are asymmetric on purpose:

- **PUTs done, DELETEs not** — a live node has two copies. Harmless:
  both hash to the same node, no reader can tell, and the next round's
  mark classifies the loser and the next sweep reclaims it.
- **DELETEs done, PUTs not** — a live node has no copy. Data loss, and
  unrecoverable.

So `compact` awaits *every* replacement PUT before deleting anything,
and one failed PUT aborts the batch with nothing deleted, because a
partial replacement set cannot safely retire any original. Both sides
have a test that injects the failure and then asserts, from a cold
cache that knows only what the bucket says, that the whole live set
still resolves:

- `a_failed_delete_leaves_every_live_node_readable` — deletes refuse;
  asserts the replacements landed, the originals are *still present*
  (the permitted duplicate), every delete is reported in
  `Reclaim::delete_failures` rather than swallowed, the live set
  resolves, and a retry with deletes working completes the batch.
- `a_failed_replacement_put_leaves_every_live_node_readable` — the
  second replacement PUT fails; asserts the batch errors, *no* original
  was retired, the live set resolves, the half-written replacement is
  catalogued as incomplete rather than as dead, and the retry converges.

Two smaller guards fall out of the same argument and have their own
tests: a rewrite may never delete a pack it just reproduced
(`replacements` is subtracted from the delete list), and pairing a
`Sweep` with a live set other than the mark it was classified against
is refused rather than applied (`a_sweep_from_a_different_mark_is_refused_not_applied`)
— otherwise a stale mark would silently drop survivors.

### §14.9's compaction ceiling: measured, mostly lifted, and the residual named

§14.9 found compaction topping out at ~1.7× with threads "because the
pack writer is serial", against mark's ~4×. That is the difference
between GC costing ~2 cores and GC costing most of the machine, since
§14.5 measured the compactor rewriting 117% of the bytes the commit
path itself writes.

`build_packs_concurrent` restructures the writer in three phases:
compress every node in parallel; decide pack boundaries **serially**, by
exactly `build_packs`' rule; assemble, count-patch and hash the
independent output packs in parallel. Keeping the boundary pass serial
is deliberate — it is what makes the output byte-identical to
`build_packs`, which `concurrent_and_serial_builds_agree_byte_for_byte`
asserts across four node counts, four targets and two widths. A scheme
that partitioned the node list up front to avoid the pass would cut
packs at different places, and pack composition is exactly what §14.2's
one-directory-one-pack property is a statement about.

Measured on this host — **4 physical cores / 8 hardware threads, an
i7-8650U, a 15 W mobile part that throttles hard** — over 259.3 MiB of
incompressible synthetic nodes in 64 packs at a 4 MiB target, half the
nodes dead so every pack is partially dead. Medians of three runs;
run-to-run spread was ±15% and the serial baseline itself varied
104–139 MiB/s, so read the *scaling* column and not the absolutes.
MiB/s is live node bytes carried over per second, the same quantity
§14.5 reports as 14.79 GiB per run.

**End to end (`Compactor::compact`: GET bodies, decompress, verify,
parse, re-emit in key order, PUT, DELETE; `InMemory` bucket):**

| threads | MiB/s | scaling | §14.9 scaling |
|---:|---:|---:|---:|
| 1 | 97 | 1.00× | 1.00× (167 MiB/s) |
| 4 | 260 | **2.78×** | 1.68× (281 MiB/s) |
| 8 | 259 | **2.66×** | 1.67× (279 MiB/s) |
| 16 | 231 | 2.34× | 1.53× (256 MiB/s) |

**Pack writer alone (`build_packs_concurrent`):**

| threads | MiB/s | scaling |
|---:|---:|---:|
| serial `build_packs` | 129 | 1.00× |
| 1 | 132 | 1.00× |
| 4 | 257 | 2.16× |
| 8 | 350 | **3.17×** |
| 16 | 348 | 2.88× |

**Where the remaining time goes**, per `where_the_rewrite_time_goes` and
a phase-instrumented run of `compact` (259 MiB read, 130 MiB rewritten):

| phase | 1 thread | wide | scaling |
|---|---:|---:|---:|
| `(level, first key)` sort + dedup | 0.08 ms | 0.11 ms | serial, **0.006% of the run** |
| compress (parallel) | 1.82 s | 0.55 s | 3.3× |
| decide pack boundaries | 0.009 ms | 0.011 ms | serial, **0.0005% of the run** |
| assemble + blake3 (parallel) | 0.17 s | 0.043 s | 3.4× |
| read pack bodies | 0.6 ms | 0.6 ms | — |
| PUT replacements | 127 ms | 34 ms | 3.7× |

The honest reading, in three parts:

1. **§14.9's diagnosis is fixed.** The writer's serial fraction is now
   0.007% of the rewrite. Nothing in the path is serial in any amount
   that Amdahl notices.
2. **It still does not scale linearly, and the reason is this host, not
   the code.** Every *parallel* phase independently saturates at
   3.3–3.7× on 4 physical cores — compression, sealing and PUTs all land
   in the same place, which is what a memory-bandwidth ceiling looks
   like rather than a lock. §14.9's own memory-resident lookup column
   saturated at 3.31× at 16 threads and 3.38× at 32 for the same
   reason. 16 threads is 4× oversubscribed here and regresses, exactly
   as §14.9's 16-thread row does.
3. **One flat phase was found and removed.** Before this step's last
   change, reading pack bodies cost 219 ms and did **not** shrink with
   threads at all — 44% of the 8-thread wall clock — because
   `PackStore::get_body` ends in `.to_vec()`, and a `memcpy` is a
   `memcpy` on any number of cores. Reading bodies as `bytes::Bytes` and
   slicing frames out of them took that phase from 219 ms to 0.6 ms and
   is most of the 1.67× → 2.7× improvement. The writer change alone got
   the writer to 3.17× but left end-to-end near 1.95×, which is a
   useful warning: the ceiling had moved, and measuring only the
   component §14.9 blamed would have missed it.

Sizing consequence for S7b: at ~260 MiB/s on four cores, §14.5's
14.79 GiB per GC round is ~58 s of wall clock at full width, or the
same work spread thinner under `CONSTELLATION_COMPACT_BYTES_PER_S`.
"GC is most of the machine" becomes "compaction is ~3 cores while it
runs", and it is the rate budget rather than the thread width that
decides how much of the machine that is.

### Design decisions where the plan left a choice

- **The level-synchronous frontier, rather than a shared concurrent
  visited-set consulted per edge.** Every node in a frontier sits at the
  same level of the tree, so there is no straggler to wait on, and the
  only serial step is inserting the next level's child hashes into
  `seen` — hashing, no I/O. Deduplication has to be serial somewhere,
  because "have I visited this hash" *is* the termination condition, and
  doing it once per level over an already-gathered vector beats a lock
  on the hot edge.
- **A pack body with no `.idx` sibling is reported, never swept.** It is
  the state a crash between `put_pack`'s two PUTs leaves *and* the state
  a healthy writer is in for a few milliseconds during every seal. It
  holds no resolvable node, so it costs only space, and deleting it
  safely needs the same age horizon and condemned-list handshake today's
  chunk orphan pass uses — which is policy, and therefore S7b's.
  `PackCatalog::incomplete` gives that policy something to work from.
- **A failed DELETE is reported, not raised.** The pack is unreachable
  garbage either way and the next round re-classifies it, whereas
  aborting a batch on one 503 would discard the record of every pack
  that *was* reclaimed. A failed PUT is the opposite and aborts at once.
- **Packs are processed in ascending hash order.** Stable across runs
  and independent of LIST ordering, so a restart cursor is one 32-byte
  value and a resumed run repeats no work. Re-running a completed batch
  is harmless: the DELETEs are idempotent and the PUTs are
  content-addressed.
- **The concurrent writer falls back to `build_packs` at width 1.**
  Holding every frame in its own allocation costs one extra `Vec` per
  node and one extra copy of every byte, which the serial writer avoids
  by compressing straight into the body it is filling. Measured: 1.75 s
  serial against 2.66 s for the concurrent path pinned to one thread.
  The cost buys parallelism, so it is paid only when there is
  parallelism to buy.
- **`build_packs_concurrent` is a new `pub fn` in `packs.rs` rather than
  a refactor of `build_packs`.** It reuses `seal` and the header
  constants and duplicates the framing loop; not one existing line of
  S4's output changed, and the duplication is pinned by the
  byte-identity test. A shared-helper refactor would have been tidier
  and was declined because the step's scope is additive.
- **The compactor takes an `mtree::Hasher`.** A rewrite re-verifies
  every node it moves, so a keyed (E2E, §P13) filesystem compacts with
  no change; a mismatched hasher turns the whole compaction into a hash
  error rather than corrupting anything.
- **`Compactor` owns its rayon pool for its lifetime.** A sweep is many
  batches and a pool per batch would be a thread-spawn storm; `mark`
  builds one per call because it runs once per round.

### New config knobs

- `CONSTELLATION_GC_THREADS` (default: one per core; `0` means the
  same) — width of the mark and rewrite pools. Read in
  `parallel::gc_threads`. A private pool rather than rayon's global one
  because GC is the one subsystem whose parallelism an operator must be
  able to cap (§14.5: GC and compaction consumed 939 s of 1,205 s), and
  because the measurement tests have to set the width per run.
- `CONSTELLATION_COMPACT_BYTES_PER_S` is **not** implemented here. §S7
  assigns the rate budget to S7b; `CompactionPacer` is the mechanism it
  drives, and `Unpaced` is the default because a library that slept by
  default would be making S7b's decision for it.

### What S7b drives

- `mark::live_set(chain, cache, &[seq], threads)` — resolve §P10's root
  set (newest commit, retained window, `snaps/*`, clones, unexpired
  `holds/*`) to commit sequence numbers and hand them over; it returns
  the live node set, the live packs, the catalog the sweep is classified
  against, and `missing_roots` for sequence numbers retention already
  removed (reported, not fatal: "the newest commit vanished" and "a
  retained commit aged out mid-run" are the same observation with very
  different meanings, and only the caller can tell them apart).
- `Sweep::classify(&catalog, &live)` plus `Sweep::{fully_dead,
  partially_dead, fully_live, rewrite_bytes, reclaimable_bytes,
  whole_pack_death_fraction}` — the last is §14.5's 0.7% headline as a
  live metric, so the plan's assumption can be re-checked against real
  traffic rather than taken on faith.
- `Compactor::{delete_dead, compact}` with `max_packs` and
  `resume_after`, returning `Reclaim { deleted, written, nodes_moved,
  bytes_read, bytes_written, delete_failures, next }` — enough to drive
  a rate budget from observed throughput.
- `PackCatalog::incomplete()` — orphan bodies for the horizon pass.
- `parallel::{gc_threads, effective_threads}` for pool sizing.
- Two new `StoreError` variants: `Node` (from `MtreeError`) and
  `Parallel` (pool construction, `spawn_blocking` join).

### Deliberately deferred

- **All of `cli/gc` wiring, retention policy, scheduling and the rate
  budget** — S7b's, and it needs S5's real commit stream.
- **Retiring the `deref` table and the `superseded-checkpoint` rule** —
  §P10 retires them only once the tree is authoritative for metadata.
  Nothing here touches them.
- **Sweeping incomplete pack bodies** — needs the age horizon and the
  condemned handshake, which are policy. Reported instead.
- **Chunk-plane packing** — §P8 allows it; no part of this step needs it.
- **Avoiding the `put_pack` body clone** — 34 ms of a 500 ms batch after
  the read-side fix, and removing it means duplicating the one primitive
  that carries the pack-durability ordering. Measure again if the PUT
  phase ever stops scaling.
- **A `rayon::par_sort` for the pre-pack sort** — it is 0.006% of the
  rewrite; parallelizing it would be noise.
- **§0.2b's steady-state re-run against the real store** — §S7's gate
  asks for it, and it needs S5's publisher plus S7b's scheduler to
  produce a steady state at all. The per-round quantities it would
  measure (`whole_pack_death_fraction`, `rewrite_bytes`) are exposed so
  that run is a matter of reading counters.

### Plan 28 S7a exit criteria

- [x] Mark from a root set of commit sequence numbers → live node set +
      live pack set
- [x] The walk terminates on shared subtrees, proved by a test that
      compares a 1-root mark against a 17-root chain and asserts the
      visit count tracks the differences (690 vs 574, against 9,758 for
      a naive re-walk)
- [x] Mark parallelized as a work-stealing frontier deduplicated by
      hash, on a width-capped pool
- [x] Packs classified fully live / fully dead / partially dead from the
      `.idx` entries alone
- [x] Fully dead packs deleted with their `.idx` sibling at zero rewrite
      cost
- [x] Partially dead packs rewritten in key order, originals retired
      only after the replacements are durable
- [x] The ordering invariant tested on both sides — failed DELETE and
      failed replacement PUT — each asserting every live node still
      resolves from a cold cache
- [x] The rewrite path made concurrent, byte-identical to the serial
      writer, with the scaling measured at 1/4/8/16 threads and the
      residual ceiling attributed by phase
- [x] Restartable cursor and batch size on both sweep paths
- [x] A mechanism for S7b's rate budget, with no policy in this crate
- [x] Additive only: no `crates/cli` file touched, no `crates/mtree`
      file touched, no `bench/prollybench` file touched, no behavioural
      change to S4's three modules, nothing calling the new code

### Gates run in this environment

- `cargo fmt --all` — no diff (`--check` clean). It left the in-flight
  `crates/cli/src/{lease,shipper,node_runtime}.rs` and
  `crates/meta/src/sqlite.rs` untouched.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace` — **614 passed, 0 failed**, plus the 2
  `#[ignore]`d measurement tests. `constellation-store-s3` contributes
  131 (up from 110): 21 new co-located tests across `mark`, `compact`,
  `packs` and `parallel`.
- `bash tests/smoke.sh` — **SMOKE TEST PASSED.**
- Measurements: `cargo test --release -p constellation-store-s3 --lib
  -- --ignored --nocapture --test-threads=1` reproduces both tables
  above. `--test-threads=1` matters: run in parallel the two
  measurements contend for the same four cores and the 1-thread rows
  read 30% low.
- `bash tests/integration.sh`, the harness matrix and pjdfstest — **not
  run.** They need docker and they exercise product wiring this step
  deliberately does not touch: nothing in the product calls `mark.rs` or
  `compact.rs`, so their contribution to those lanes is dead code. Per
  §11 the full CONVENTIONS gate list starts at S5, and the §S7 gate that
  re-runs §0.2b's steady-state shape needs S5's publisher and S7b's
  scheduler to have a steady state to measure.
- Environment note: the sandbox this work was done in does not expose
  `/dev/fuse`, so
  `node_runtime::tests::two_views_of_one_node_converge_with_a_peer_and_pin_is_view_agnostic`
  and `tests/smoke.sh` fail there with "No such file or directory".
  Both were re-run with `/dev/fuse` available and pass; the figures
  above are from that run.

### Spec note

No contradiction with `docs/explanation/DESIGN.md` was found in this
step, and no behavioural change was needed in `crates/mtree` or in S4's
three modules.

Two notes for the coordinator, neither a contradiction:

1. **§P10b's "packs die whole" hope should be read as retired, not
   merely unsupported.** §14.5 measured 0.7% and this step's
   classification reproduces the shape in miniature: an aged test tree
   leaves overwhelmingly *partially* dead packs, and the fixture has to
   tolerate zero whole-pack deaths rather than assert any. The cheap
   path is implemented and tested, but the plan should size the
   compactor as the normal path everywhere it currently hedges.
2. **§14.9's "compaction is ~2 cores" conclusion was measured against
   one component.** The serial pack writer was real and is fixed
   (3.17×), but on this host end-to-end compaction only reached ~1.95×
   until a *second* non-scaling phase was removed — the `Vec` copy at
   the end of `PackStore::get_body`, which was 44% of the 8-thread wall
   clock. Worth recording because the same shape is likely elsewhere:
   `object_store` handing back owned `Vec`s where `Bytes` would do.

## Plan 28 step S1 — settling the dentry attr copy: **DONE**

Step S1 of `wip/28-s3-native-metadata-store.md` §11, a measurement only:
`bench/prollybench` gained a variant switch and a new benchmark, and the
results are §14.10 of the plan. No `crates/**` file was touched, so the
e2e lanes cannot be affected and were not run. `cargo test --release` in
`bench/prollybench`: 19 passed, 0 failed.

**Verdict: keep the denormalized attr copy. §P6 stands as written.**
Three `0x02` value shapes were measured through the same code paths at
census scale, fresh and aged.

| Item | State | Where |
|---|---|---|
| Three `0x02` shapes behind one flag: `copy` (§P6), `nocopy` (ino + kind), `dentry-auth` (the dentry *is* the record for `nlink == 1`) | done | `bench/prollybench/src/keys.rs` (`Enc`) |
| Footprint, `setattr` B/op, cold `ls -la` and `getattr(ino)` per variant, at 1 and 8 threads | done | `bench/prollybench/src/b06.rs` |
| Aged corpus strengthened: hot-directory retention, churn over the eight widest directories, shuffled ino allocation | done | `bench/prollybench/src/corpus.rs` |
| Results written up as §14.10 | done | plan §14.10 |

### What decided it

`nocopy` saves 10.9% of stored leaf bytes and 1.3–1.5× on `setattr`, and
charges **12,039 pack reads for a cold `ls -la` against `copy`'s 30.4** —
396×, or 321 ms per directory against 2.2 ms. That gives up §14.2's
one-pack-per-directory property, which is the whole point of the
encoding. It is also the only cost measured anywhere in this plan that
**does not thread away**: every other read row scales 3.6–6.4×, and
`nocopy`'s cold `ls -la` is still 144× `copy` at 8 threads. Its tier-(b)
scaling of 1.88× is the worst measured in the plan, because a scan plus
~3,000 scattered point reads thrashes the 64 MiB cache the scan is
supposed to fit inside.

The decision rests on the two amplifications not being commensurable,
and §14.10 quantifies that rather than asserting it: a written byte
costs ~2.17 bytes of transfer that does not divide by cores, because it
is stored on every replica and rewritten by a compactor running at 117%
of the commit write rate; a pack read is a cached read that does divide.
(S7a has since taken compaction from 1.7× to 2.78×. That does not
disturb the verdict — threading changes the rewrite's wall clock, not
the byte volume, and the margin is three orders of magnitude clear of
it.)

### Two corrections to earlier sections

1. **Write amplification comes from the scattered `0x01` inode write,
   not from the attr copy.** On an aged tree, directory-local `setattr`
   costs 41 B/op with the copy and 40 B/op without — 3%. §14.4's reading
   that "the attr copy means each `chmod` writes two keys" is right
   about the mechanism and wrong about which key is expensive once a
   tree has aged. §P6 and §14.7 item 6 have been corrected.
2. **§14.8's aged corpus understated ino scatter by roughly 7×.** Its
   densest tracked directory shrank to 1.7k children after aging; with
   retention and churn it holds 4,241 children spread over 134 distinct
   1024-ino buckets against 4,994 over 12 when fresh. Aged
   directory-local `setattr` is **15.9× today's log bytes, not §14.8's
   2.1×** — the same gate failed by a wider margin. Read §14.10's rows
   in preference to §14.8's.

### What it opened

- **§S1b, ino allocation locality**, added to the plan's work order. The
  *identical* encoding costs 2.3× today's log bytes fresh and 15.9×
  aged, and that ~7× gap is entirely ino locality, because `alloc_ino`
  is a global `counter++` so a directory's children drift apart in the
  `0x01` range as the filesystem ages. Most of `dentry-auth`'s measured
  advantage is just that it never writes that key. Allocation is policy
  rather than format — an ino is already opaque, existing filesystems
  keep theirs, no migration is implied — so this can change after (B)
  ships, but the number should be known before the encoding is touched.
- **`dentry-auth` stays on the table** rather than being discarded with
  `nocopy`, with an explicit trigger: it is the only measured shape that
  writes less on *every* axis while keeping `ls -la` at one pack (24.2M
  keys against 35.8M, 13.6% fewer compressed leaf bytes, 25% less
  interior, aged directory-local `setattr` at **1.01×**), and it charges
  for that on `getattr(ino)` — 7.92 cold pack reads against 4.00, about
  half the throughput — which is a bad trade for the operation FUSE uses
  most. It would also make `link()` migrate a record into `0x01` when
  `nlink` rises, a product consequence this plan has not costed. Take it
  only if `setattr` cost becomes binding on the compactor budget.
- Nothing measured here helps the **scattered** case, where
  `dentry-auth` lands between the other two (632× against `copy`'s 756×
  and `nocopy`'s 506×).

### Process note worth keeping

`cargo test --release` does **not** rebuild `target/release/prollybench`
when the crate has no integration tests, so an early census run silently
executed a stale binary. Every published number comes from a run after
an explicit `cargo build --release`.

## Plan 28 step S3 — `mtree::keys`, the §P6 codec: **DONE**

Step S3 of `wip/28-s3-native-metadata-store.md` §11. The §P6 key
encoding and the value records it points at, added to `crates/mtree` as
two modules that depend on the S2 core and that **the core does not
depend on**. S1's verdict is implemented as written: the `0x02` dentry
carries the denormalized attr copy. No existing crate's source was
touched beyond `mtree/src/lib.rs`'s own module list, and nothing outside
`mtree` calls this yet, so the e2e lanes cannot be affected and were not
run (see "Gates" below).

| Item | State | Where |
|---|---|---|
| `0x01 \| ino` → the authoritative inode record: attrs, `nlink`, `rdev`, inline manifest or its hash, symlink target, inline xattrs | done | `mtree::keys::inode`, `mtree::record::InodeRecord` |
| `0x02 \| parent_ino \| name` → `(ino, kind)` **plus the denormalized attr copy** (S1's verdict, §14.10) | done | `mtree::keys::dentry`, `mtree::record::DentryRecord` |
| `0x03 \| ino \| xattr_name` → value, spilled to a blob hash above `VALUE_SPILL` | done | `mtree::keys::xattr`, `mtree::record::{Payload, place_value}` |
| `0x04 \| ino \| parent_ino \| name` → `()`, the reverse dentry index | done | `mtree::keys::rdentry`, `mtree::record::RDENTRY_VALUE` |
| `0x30 \| subsystem \| id` → record body (snapshots, clones, quota, designations, holds) | done | `mtree::keys::{subsystem, Subsystem}` |
| `0x10`–`0x2f` reserved and unused; refused by name on decode | done | `mtree::keys::RESERVED_RANGES`, `KeyError::ReservedRange` |
| Big-endian fixed-width ids, names last, so byte order is numeric and lexical order | done | `mtree::keys` (module docs state why) |
| Half-open scan ranges with an exclusive bound derived by prefix successor | done | `mtree::keys::{KeyRange, dentries_of, xattrs_of, names_of, names_of_in, records_of, whole_range}` |
| Total, non-panicking key decode with a distinct `thiserror` error | done | `mtree::keys::{Key::parse, KeyError}` |
| `XATTR_INLINE` (256 B) whole-set inlining; above it every name moves to `0x03` | done | `mtree::record::{place_xattrs, XattrPlacement, xattr_section_len}` |
| `VALUE_SPILL` (1 KiB) blob spill, with the caller's hash function (E2E keyed addressing) | done | `mtree::record::{Payload::place, place_value}` |
| Deterministic inode-value planner: xattr set, then manifest, then symlink target, until the record fits | done | `mtree::record::{plan_inode, InodePlan}` |
| §P7 leaf→`Agg` projection — closes S2's caller-supplied gap; only `0x01` is authoritative | done | `mtree::record::{leaf_agg, config}` |
| Local attr/record types declared from `meta`'s schema, so `mtree` gains no `meta` (or `fs-core`) dependency | done | `mtree::record::{Attrs, Kind, InodeRecord, DentryRecord, BlobHash}` |
| Round-trip tests for every range including the empty (`0x30` with no id) and maximal (`NAME_MAX`, spilled manifest, xattr set at the budget) cases | done | `mtree::{keys,record}` tests |
| Ordering tests: every field, `ino` across the bit-40 allocation shift and the `u64` extremes, names containing `0x00`/`0x2f`/`0xff` | done | `mtree::keys` tests, `mtree/tests/keys.rs` |
| §12's mechanical **"no mutable field is in the tree"** test, declarative and byte-level, plus the atime exclusion | done | `mtree/tests/keys.rs::no_mutable_field_is_in_the_tree`, `mtree::keys::Field` |
| Range-scan tests: one directory's dentries and one inode's xattrs are contiguous, and a bounded scan cannot run into the next | done | `mtree/tests/keys.rs` |
| Aggregate test against an independently computed model total, with hard-linked files so a double-count would show | done | `mtree/tests/keys.rs::the_aggregate_counts_each_file_once_and_ignores_the_dentry_copy` |
| Inline/spill boundary tests in both directions, at `XATTR_INLINE` and `VALUE_SPILL`, through a real tree | done | `mtree::record` tests, `mtree/tests/keys.rs` |

### The aggregate gap S2 left open, and why its test is not vacuous

S2 made the leaf→`Agg` projection a caller-supplied function pointer
because only the key codec knows which range holds the authoritative
record. `record::leaf_agg` contributes only for `Key::Inode`; `0x02`,
`0x03`, `0x04` and `0x30` contribute nothing, so the dentry's attr copy
cannot inflate a total. This is the one place in the plan where an error
would have been *consistently* wrong rather than detectably wrong — the
root hash attests to the aggregate, so every replica would have agreed
on an inflated `du`.

`the_aggregate_counts_each_file_once_and_ignores_the_dentry_copy`
computes its model total by iterating its own inode map, never the tree,
over a corpus whose inos deliberately straddle several
`(node_prefix << 40)` buckets and in which every fourth file is hard
linked from the root, so some inodes carry two dentry copies. Two guards
stop it passing vacuously: a tree built from the `0x01` records alone
must report the same bytes, files and max mtime, and the test asserts the
dentry copies really do carry non-zero sizes — so if the copy ever
stopped carrying attrs, the test would fail rather than quietly become
trivial. A follow-up incremental commit grows one file by 4 KiB,
rewriting its inode record and all its dentry copies, and the total must
move by exactly 4 KiB.

Verified against `meta::sqlite::recursive_size_conn`: both count size
and files for `kind == File` only, so `du`, `statfs` and quota do not
change meaning when the tree becomes their source.

### Design decisions where the plan left a choice

- **No `meta` and no `fs-core` dependency.** `meta` depends on
  `fs-core`, and S5/S6 need `meta` or code above it to depend on
  `mtree`, so an `mtree → meta` edge would close a cycle someone would
  have to unpick. `meta`'s schema was read and the field list
  redeclared as plain data; `Kind`'s discriminants match
  `fs_core::InodeKind::as_u8` (0–6, verified pairwise) so S5's mapping
  is a cast. The `fs-core` edge was avoidable too: a file's chunk list
  is carried as opaque encoded-manifest bytes, so this crate needs no
  manifest decoder and no `ChunkHash`.
- **`Attrs` has no `atime_ns` at all.** §P6 excludes atime from the
  tree, so it is absent from the record rather than merely absent from
  the keys — there is no field to set, and `ATTRS_LEN` is pinned at 49
  with its nine fields enumerated, so adding one fails a test on
  purpose. atime stays node-local and best-effort (today's
  `atime_journal` with max-merge on apply).
- **Value layout is versioned by `node::FORMAT_VERSION`, not by a
  per-record version byte.** A byte per record is ~36 MB at census
  scale and would have changed every §14 byte count for no benefit the
  node header does not already provide; the record encodings are pinned
  by hex tests instead.
- **Keys are big-endian, values little-endian.** Only keys are ever
  compared, so key endianness is load-bearing and value endianness is
  free — little-endian matches every other encoder in the repo.
- **`plan_inode`'s spill order is format, not heuristic.** The xattr
  set by the whole-set rule, then the manifest, then the symlink
  target, until the record fits `VALUE_SPILL`. Manifest first because
  it is the field that actually grows while a target is bounded by
  `PATH_MAX`, and because `fs-core` already spills chunk lists. Two
  writers that spilled different fields would produce different bytes
  for the same filesystem and therefore different root hashes.
- **Inline xattrs are sorted by `plan_inode`.** Canonicality reaches
  into the values: the same set in a different iteration order has to
  produce the same bytes, asserted by
  `inline_xattrs_are_sorted_so_the_record_is_canonical`.
- **A nameless key is a decode error, not a key.** A range's own prefix
  is a scan bound; storing it would make a directory's lower bound
  ambiguous. An over-long `0x01` key is refused too
  (`KeyError::Oversized`), because trailing bytes would let two
  distinct keys resolve to one inode.
- **`KeyError`/`RecordError` are separate from `MtreeError`.** Node
  structure arriving off a network and a byte string that is not a §P6
  key are different diagnoses; merging them would report bucket
  corruption for a codec-version mismatch, or the reverse.
- **The core stays ignorant of the codec.** `keys` and `record` depend
  on the core and nothing in `node.rs`, `tree.rs`, `config.rs`,
  `hash.rs`, `store.rs` or `error.rs` references them. S2's seam is
  what let the codec be tested against `MemoryNodeStore` in
  milliseconds, and no prefix is special-cased in the structure.

### Ordering traps found

The bit-40 straddle is a non-event under big-endian fixed-width inos,
which is the point; the trap it warns about is any variable-width
encoding, since a varint `ino` would order `(1 << 40)` before
`(1 << 40) - 1`. Three things did surprise:

- **The exclusive bound of the last directory.** `dentries_of(u64::MAX)
  .end()` is `[0x03]`, the start of the xattr range. Correct, but it
  means the bound cannot be computed as `parent + 1` (which overflows);
  it needs a general prefix successor that drops trailing `0xff`s, with
  the all-`0xff` case returning the empty string meaning "no upper
  bound".
- **A name may be a byte prefix of another name.** `dentry(7, b"a")`
  prefixes `dentry(7, b"a\x00")`, which is harmless *only* because the
  name is the last field. So "names go last, ids are fixed width" is
  load-bearing rather than tidy.
- **An over-long `0x01` key** decoded fine and ignored its tenth byte,
  which would have let two distinct keys resolve to the same inode.

### Deliberately deferred

- **The `0x30` record bodies themselves** — the codec carries the key
  and a spillable body; what a snapshot, clone, quota, designation or
  hold record *contains* is S5/S6's, and inventing it here would
  freeze a format nothing yet writes.
- **Range aggregates** (`agg` over an arbitrary key range, §P7's
  per-directory `du`) — the key encoding they need now exists, but the
  API belongs with the caller that answers `du`, which is S5/S6.
- **The `dentry-auth` escape hatch** (§S1, §14.10 reading 5) — not
  built, per S1's trigger.
- **Ino allocation locality** (§S1b) — a `bench/` measurement, and
  allocation is policy rather than format, so it can change after (B)
  ships.

### Plan 28 S3 exit criteria

- [x] All five §P6 ranges encode, decode and round-trip, including the
  empty and maximal cases
- [x] `0x10`–`0x2f` reserved: nothing encodes into the span, and a key
  that claims one is a named decode error
- [x] Big-endian throughout; encoded byte order equals numeric and
  lexical order for every field, asserted over the `ino` bit-40
  allocation shift, the `u64` extremes, and names containing
  `0x00`, `0x2f` and `0xff`
- [x] S1's decision implemented: the `0x02` value carries the
  denormalized attr copy
- [x] `XATTR_INLINE` (256 B) and `VALUE_SPILL` (1 KiB) implemented and
  tested across both boundaries in both directions
- [x] §12's mechanical "no mutable field is in the tree" test present,
  covering §P5's retracted indexes and atime
- [x] S2's aggregate gap closed: a §P6 projection that counts the
  `0x01` record and never the dentry copy, checked against an
  independently computed model total
- [x] `crates/mtree` gained no dependency — in particular not
  `constellation-meta` — and the tree core does not depend on the
  codec
- [x] No existing crate's source modified

### Gates run in this environment

- `cargo fmt --all` — no diff (`--check` clean).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test --workspace --no-fail-fast` — **613 passed, 1 failed, 2
  ignored**, of which `constellation-mtree` contributes 68: 47
  co-located unit tests (25 before this step), 6 new integration tests
  in `tests/keys.rs`, S2's 14 properties, and 1 doctest.
- The one failure is
  `node_runtime::tests::two_views_of_one_node_converge_with_a_peer_and_pin_is_view_agnostic`,
  which needs `/dev/fuse`; this sandbox does not expose it and the test
  fails in the mount call. Environmental, in a crate this step neither
  touches nor links against, and it passes on the host.
- One flake, recorded for the same reason S2 recorded it: a wall-clock
  latency test in `constellation-meta --test checkpoint_concurrency`
  failed once while a concurrent benchmark held the host at load
  average ~11, and passed 3/3 in isolation and 3/3 on the final full
  run.
- `bash tests/smoke.sh`, `bash tests/integration.sh`, the harness
  matrix and pjdfstest — **not run, and cannot be affected.** This step
  adds two modules to a crate no product crate depends on yet, so its
  contribution to those lanes is dead code. Per §11's rule that steps
  touching no product code do not run the e2e lanes. The full
  CONVENTIONS list applies from S5, where `cli/shipper` is wired.

## Plan 28 step S5 — builder from the live replica: **DONE**

Step S5 of `wip/28-s3-native-metadata-store.md` §11. Option (B) is
wired: `SqliteMeta` → §P6 `mtree` → pack + commit, driven from the
shipper beside today's checkpoint. The S5 subagent stalled mid-flight
after landing the code; the coordinator verified and ran gates.

| Item | State | Where |
|---|---|---|
| Incremental publish from journal / touched entities → `mtree::apply` | done | `cli/src/mtree_publish.rs` |
| Full rebuild on first publish / level mismatch | done | `TreePublisher::{publish, restore, hydrate}` |
| Rebase splice on lost CAS (key-granularity read-set) | done | `TreePublisher::splice`; test `two_publishers_with_disjoint_keys_both_survive` |
| Pack-before-commit for spilled values | done | `store-s3/src/blobs.rs` + spill test |
| Shipper / mount wiring; publisher on by default for writers | done | `shipper.rs`, `node_runtime.rs` |
| atime stays out of the tree | done | test `atime_never_reaches_the_tree` |
| §11a `configuration.md` for S4 knobs | done | `docs/reference/configuration.md` |
| `getattr` p99 during publish | done (2026-09-21) | 5.1 µs idle, 5–8 µs during a 1.3 s full publish of 300k keys; `mtree_publish::tests::getattr_latency_during_a_publish` (ignored, run by hand) |
| **Defect**: a replica *behind* the chain head could overwrite newer tree values with older ones | fixed | commits carry an `applied` vector, read in the plan's SQLite snapshot (`SqliteMeta::read_consistent`); publish and splice require `vector_covers` |
| **Defect**: a cancelled sync round dropped the publisher's taken batch for good | fixed | `TreePublisher::publish` and `NodeCache::seal_packs` are cancellation-safe; test `a_cancelled_publish_keeps_its_batch` |
| Harness matrix, pjdfstest | done | the final plan 28 gate run, below |

### Gates (2026-09-14)

- `cargo fmt --all --check` — pass
- `cargo clippy --workspace --all-targets -- -D warnings` — pass
- `cargo test --workspace` — 625 passed, 1 failed in sandbox
  (`node_runtime::…two_views…`: no `/dev/fuse`); **passes with FUSE**
- `mtree_publish` — 7/7 pass
- `tests/smoke.sh` — **PASSED**
- `tests/integration.sh` — **PASSED**
- `docker compose --profile test run --rm compliance` — **interrupted**.
  `.dockerignore` fixed so `bench/uploadbench` is copied (it is a
  workspace member). Rebuild was compiling when stopped; pjdfstest never
  ran.
- `target/release/harness run` — **interrupted mid-matrix**. Partial:
  FAILED `baseline`, `slow-network`, `s3-outage`, `s3-flap`,
  `kill9-remount` (MODEL DIVERGENCE / truncated file sizes); PASSED
  `latency`, `cold-cache`, `two-clients-disjoint`. Not diagnosed.
- FUSE publish p99 — not measured
- §11a store knobs — **documented** in
  `docs/reference/configuration.md` (Merkle metadata tree)

### Resolved 2026-09-21

- The harness divergences (`baseline`, `kill9-remount`: files reading back
  as size 0 or another file's size) were **not S5**: they reproduce on
  `95985b2`, before plan 28, about half the time on a 32-core host.
  `getattr` read the committed row *then* checked the write shard, so it
  could interleave with `flush_inode` and cache the pre-flush size for
  TTL. Fixed by reading under the shard lock (and giving `lookup` the
  same overlay); 0/24 failures after, from ~50%.
- The two defects in the table above; the second was found by
  `snapshot-churn` once snapshots were commit-backed.
- `CONSTELLATION_GC_THREADS` and the compaction knobs are documented with
  S7b.

## Plan 28 step S6 — reader: bootstrap, partial replica, `fsck`: **DONE**

| Item | State | Where |
|---|---|---|
| Bootstrap a fresh replica from the chain head: one cursor walk loads `0x01`/`0x02`/`0x03` into `inode`/`dentry`/`xattr` (spilled values resolved from `blobs/`), `0x30` into partitions, snapshot rows and the replicated quota; then tail each partition from the commit's `applied` vector | done | `cli::mtree_read::{bootstrap_from_commit, load_tree}`, `cli::shipper::bootstrap_from_tree` |
| Checkpoint fallback when the chain is empty; `CONSTELLATION_BOOTSTRAP_SOURCE=checkpoint` forces it | done | `cli::shipper::bootstrap` |
| `0x30` records written by the publisher (new `Subsystem::Partition`; bodies are versioned self-delimiting field lists) | done | `mtree::record::{encode_fields, decode_fields}`, `mtree_read::subsystem_state` |
| Publisher defers while a cross-partition rename half is parked, so no commit splits a pair (`xpart_pending` is not carried) | done | `mtree_publish::Planned::Parked` |
| Readers find nodes through the pack catalog, not a commit's own `packs`; `NodeCache` refreshes the catalog once per burst of misses (compaction moves nodes into packs no commit names) | done | `NodeCache::refresh_catalog`, miss path in `NodeStore::get` |
| Table-by-table equality of a bootstrapped replica with the publisher, with the checkpoint and every covered segment deleted so only the commit can supply the state | done | `shipper::tests::a_fresh_replica_bootstraps_from_the_commit_chain`, `SqliteMeta::dump_replicated` |
| Partial replica at reader level: `TreeReader` (lookup/getattr/readdir(plus)/listxattr from a root); interior-only warm-up reads no leaf, a cold `ls -la` touches one pack (≤ 2 across a seal, ≥ 75% of directories in one), a walk reads no leaf outside the visited directories bar one boundary leaf per side | done | `mtree_read::tests::{the_reader_answers_what_the_replica_answers, a_partial_replica_reads_only_the_directories_it_visits}` |
| `fsck`: verify every node of the head (hash, structure, each interior aggregate against its child, root against the commit) and, when a commit claims exactly the replica's state, rebuild through the publisher's code path into memory and compare roots, reporting differing keys | done | `cli::fsck::check_metadata_tree`, `TreeReader::verify`, `mtree_publish::rebuild_root` |
| Bootstrap wall time | measured locally | 100k inodes in 358 ms from a cold cache over an in-memory store (`getattr_latency_during_a_publish`); **not** measured on the plan 26 Appendix remote paths (no remote bucket on this host) |

Not carried by a commit, deliberately: atime (§P6), the `deref` table (a
replica that never saw a dereference misses some chunk-GC candidates; the
orphan pass collects them), parked `xpart_pending` halves (see above).

**Deferred to §11b:** serving FUSE from a partial SQLite replica needs
the engine swap (SQLite answers every FUSE call in (B)), so the partial
replica exists as the reader API and its tests, not as a mount mode.

### Plan 28 S6 exit criteria

- [x] Full bootstrap from a commit, checkpoint fallback, table-by-table
      equality with the source replica
- [x] Partial-replica tests at reader level (§12's shape)
- [x] `fsck` recomputes the root and verifies every node
- [ ] Bootstrap wall time on the plan 26 Appendix paths (no remote
      bucket available; local number recorded instead)

## Plan 28 — retiring the checkpoint: **DONE**

| Item | State | Where |
|---|---|---|
| A mount with a publisher writes no `VACUUM INTO` snapshot; the checkpoint cadence publishes a commit instead | done | `Shipper::checkpoint` |
| `CONSTELLATION_CHECKPOINT_SNAPSHOT=on` re-enables the snapshot (mounts without a publisher always write it) | done | `shipper::checkpoint_snapshot_forced` |
| Log retention floors on the head commit's `applied` vector once a commit exists, else on `checkpoints/VECTOR.json` | done | `gc::metadata_candidates`, test `log_retention_floors_on_the_head_commit_once_one_exists` |
| A bootstrap whose base the log was pruned past fails loudly instead of replaying from the gap | done | `shipper::replay_from`, test `replay_refuses_a_base_the_log_was_pruned_past` |

## Plan 28 — snapshots as retained tree roots: **DONE**

| Item | State | Where |
|---|---|---|
| A snapshot is `(commit seq, mtree root, dir ino)`, taken after a forced publish on the sync task | done | `SyncRequest::Publish`, `Shipper::publish_now`, `SnapshotManager::create` |
| `build_tree` deleted; `covering`, `clone_to`, the FUSE view and chunk-GC's snapshot roots read the tree through `FrozenObject`/`TreeReader` | done | `cli::snapshot`, `cli::fusefs`, `gc::snapshot_roots` |
| Legacy (v1, `fs-core` tree blob) snapshots removed outright — no established users | done | `SnapshotRecord` v2 only; snapshot roots never enter `deref` |
| Frozen against source and clone writes, down to xattrs and manifests; GC protects the frozen chunks | done | `snapshot::tests::a_tree_snapshot_is_frozen_against_source_and_clone_writes` |

## Plan 28 step S7b — metadata GC wiring, retention, rate budgets: **DONE**

| Item | State | Where |
|---|---|---|
| Commit retention: `CONSTELLATION_COMMIT_RETENTION` (64) and `..._RETENTION_S` (86400); a commit goes only when outside the newest N *and* older than S; head always kept | done | `cli::mtree_gc::expired_commits` |
| Mark from retained commits, every snapshot root, and nodes named by live holds | done | `mtree_gc::mark_roots` |
| Sweep with `Sweep::classify`; delete dead packs, compact partially dead ones under `CONSTELLATION_COMPACT_BYTES_PER_S` (32 MiB/s) with a restartable cursor in kv | done | `mtree_gc::run_inner` |
| Condemned-pack handshake: publish list, wait a lease TTL, re-mark, act only on still-dead condemned packs; index-less bodies deleted only past the horizon and after the wait | done | `gc/condemned-packs.json`, `store-s3::gc::{read,publish}_condemned_packs` |
| Publisher never deduplicates against a condemned pack and re-checks every pack it trusted immediately before the CAS (condemned or gone → forget, defer, re-upload) | done | `NodeCache::{set_condemned, start_dedup_log, dedup_is_sound}`, test `a_publisher_never_names_a_condemned_pack` |
| Round test: retired commits' packs reclaimed, retained commits and a snapshot of a retired commit fully readable from a cold cache, cursors cleared, the publisher keeps working across compaction | done | `mtree_gc::tests::a_round_reclaims_what_retired_commits_kept_and_nothing_else` |
| S7a's `Compactor` futures made `Send`-general (the daemon spawns GC) | done | `store-s3::compact` |
| Knobs documented | done | `configuration.md` (Garbage collection) |
| Steady-state plateau against the real store | done | harness `mtree-gc-plateau` (flat ~43.8 KB from round 2) |

**Deferred:** `blobs/` (values > 1 KiB spilled from nodes) is not swept.
A blob is content-addressed under one key forever, so it cannot use the
re-upload escape packs use and needs a two-mark horizon of its own;
spills are rare and an unreferenced blob only costs space.

### Plan 28 S7b exit criteria

- [x] Retention, mark, sweep, compaction and the rate budget wired into
      `gc::run`
- [x] Restartable cursor; condemned-list handshake for packs and
      incomplete packs
- [x] Knobs documented
- [x] Footprint plateau measured against the real store

## Plan 28 — other defects found while finishing (B)

| Defect | Fix | Where |
|---|---|---|
| `getattr`/`lookup` could cache a pre-flush size (pre-existing; the S5 harness failures) | read the committed row under the write-shard lock | `cli::fusefs_ops` |
| A framed gossip message sometimes decoded as a bare one (`postcard` ignores trailing bytes; ~1 in 20 keys) | exact decode, 64-byte signature, test asserts decode-then-verify | `net::message::Signed::decode` |
| Lease expiry margin (1 s) exceeded short TTLs, so a 200 ms-TTL node never shipped | margin clamped to TTL/4 | `cli::lease::expiry_margin_ms` |
| **E2E: the metadata tree was published in plaintext** — pack frames are zstd'd nodes whose keys are file names; `.idx` objects carry first keys; blobs hold xattr values and symlink targets; commits hold aggregates. `e2e-basic` scanned only `chunks/` and `log/` | §P13 sealing: `TreeSealing` derives per-kind keys; frames sealed with the node hash as AAD (`FLAG_SEALED_NODES`), `.idx`, blobs and commits sealed whole with their path as AAD; `e2e-basic` now scans `packs/`, `blobs/`, `commits/` for a known file name | `store-s3::{e2e::TreeSealing, packs, blobs, commits}`, test `sealed_packs_hide_names_and_need_the_key` |


## Plan 28 (B) — final gates (2026-09-21)

| Gate | Result |
|---|---|
| `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| `cargo test --workspace` | pass (one run hit the known load-sensitive `checkpoint_concurrency::snapshot_does_not_stall_concurrent_readers` ratio bound; 5/5 in isolation) |
| `tests/smoke.sh`, `tests/integration.sh` | PASSED |
| pjdfstest (`docker compose --profile test run --rm compliance`) | **8798 passed, 0 failed** (empty baseline) |
| `target/release/harness run` (full matrix, 78 scenarios) | 68 passed, 2 skipped (fio absent), 8 failed — see below |

Failures in the full matrix, triaged against the pre-plan-28 binary
(`95985b2`) under the same harness:

| Scenario | Verdict |
|---|---|
| `ckpt-bulk-ingest-bounded` | **fixed** (cancelled checkpoints left orphan bodies; pre-existing in milder form, worsened here) — passes 3/3 and in the confirmation run |
| `multi-partition-retention-is-per-partition` | ported to the commit floor; still fails because autosplit also splits the scenario's `cold/` directory, so p0 never outruns the child — **pre-existing** (same double split on `95985b2`), left as is |
| `checkpoint-strips-pending-upload`, `writeback-backpressure` | pass on rerun and in the confirmation run (timing) |
| `atime-eventual`, `deposed-reintegration`, `chaos-ci` | fail identically on `95985b2` — **pre-existing**, left as is |
| `named-shared-daemon` | the known plan 21 `umount myfs:/sub` hang (recorded under plan 26); killed after 18 min |

Confirmation run on the final binary of every scenario the late fixes
touch — `baseline`, `kill9-remount`, `cold-cache`, `fresh-node-bootstrap`,
`checkpoint-strips-pending-upload`, `ckpt-bulk-ingest-bounded`,
`snapshot-churn`, `snapshot-lifecycle`, `snapshot-mount`,
`clone-workflow`, `gc-lifecycle`, `mtree-gc-plateau`, `e2e-basic`,
`e2e-two-nodes`, `writeback-backpressure`, `two-clients-shared`,
`idle-cluster-is-quiet`, `lease-handover`: **18/18 PASSED**.

Partitions and splits remain in (B) because the per-partition op log is
still the transport; §P4 deletes them with the §11b engine swap, which
would also retire the split-timing scenario above.

## Plan 29 M0a — remove namespace partitions: **DONE**

| Item | State | Where |
|---|---|---|
| `partition` table/index, `alloc_part_id`/`next_part_id`/`partition_of`/`partition_of_conn`/`resolve_partition`/`partitions`/`invalidate_part_cache` and the `part_of/<ino>` kv cache deleted; every "which partition" lookup is now the constant `p0` | done | `meta::sqlite` |
| `LogRecord::PartSplit`/`PartMerge`/`RenameXpartSrc`/`RenameXpartDst`/`RenameXpartAbort` variants, `rename_xpart`, `alloc_xpart_txid`, the `xpart_pending` table and `park_xpart`/`unpark_xpart`/`mark_xpart_dst`/`xpart_dst_seen`/`pending_xparts`/`clear_pending_xpart`/`journal_has_xpart_dst`/`xpart_pending_count_reader`/`journal_on`/`journal_on_tx` deleted; replay's `park_or_apply_xpart`/`apply_xpart_pair` deleted | done | `meta::{record,sqlite,replay,reintegrate}` |
| FUSE cross-directory rename is now always a plain `MutateOp::Rename` through the ordinary forwarding/mutate path; no more dual-lease gating or `partition_of` lookup before acquiring a lease | done | `cli::fusefs_ops::rename`, `cli::fusefs::{require_lease_for,mutate_op_rebasable,finish_flush}` |
| Shipper autosplit/merge removed: `maybe_split_merge`/`maybe_split`/`maybe_merge`/`split_candidates`, heat tracking (`DirTraffic`, `dir_ops`, `note_shipped`, `part_split_ops`/`part_merge_idle_s`/`part_autosplit` and their env vars `CONSTELLATION_PART_SPLIT_OPS`/`_AUTOSPLIT`/`_MERGE_IDLE_S`), `consider_xpart_aborts`/`_all`, `note_xpart_shipped`; `ensure_part`/`PartState` collapse to the single `p0` entry the shipper always attaches with | done | `cli::shipper` |
| Per-partition applied vectors collapsed to a single `applied_seq`: `applied_seq_of`/`set_applied_seq_of`/`ack_journal_rows_at(..., part, ...)` removed in favor of the existing singular `applied_seq`/`set_applied_seq`/`ack_journal_at`; `SqliteMeta::applied_vector()` (a `BTreeMap<String,u64>`) replaced by `applied_seq()`/`applied_seq_reader()` | done | `meta::sqlite` |
| `Commit`/`CommitPayload.applied` changed from `BTreeMap<String,u64>` to a plain `u64`; `vector_covers(mine, theirs)` takes/returns `u64` | done | `store-s3::commits` (design choice: `u64`, not a one-key map — see report) |
| Publisher (`TreePublisher`), bootstrap (`shipper::{bootstrap,bootstrap_from_tree,replay_from}`, `mtree_read::bootstrap_from_commit`), and log retention (`gc::metadata_candidates`) updated for the `u64` position consistently | done | `cli::{mtree_publish,shipper,mtree_read,gc}` |
| `mtree::keys::Subsystem::Partition` (`0x06`) retired — variant removed outright (no compatibility needed), `SUBSYSTEMS`/`PUBLISHED_SUBSYSTEMS` shrunk; `mtree_publish`/`mtree_read` no longer write or read a partition-map `0x30` record | done | `crates/mtree::keys`, `cli::{mtree_publish,mtree_read}` |
| Control-API `StatusReport::partitions: Vec<PartitionStatus>` and the `constellation_partitions` web-UI gauge removed (the single `p0` lease is still reported via the existing `lease` field); `constellation_api` test `MultiPartition`/`status_with_partitions_does_not_deadlock` deleted (tested the now-removed field) | done | `api::{types,lib,web}` |
| Harness scenarios `partition-split`, `rename-across-partitions`, `multi-partition-retention-is-per-partition` and their sole helpers (`partition_ids[_from]`, `part_env`, `wait_for_split`, `head_commit`, `commit_covered`, `raw_json`) deleted; `passwd_live_cluster` de-autosplit-ified (still proves live passphrase rotation across two mounted nodes, minus the split-triggered DEK-derivation angle, which no longer applies) | done | `harness::scenarios` |
| `docs/reference/configuration.md` and `docs/how-to-guides/development/TESTING.md` updated: `CONSTELLATION_PART_SPLIT_OPS`/`_AUTOSPLIT`/`_MERGE_IDLE_S` rows and prose removed, log-retention prose de-partitioned | done | `docs/reference/configuration.md`, `docs/how-to-guides/development/TESTING.md` |
| New/adjusted test coverage for cross-directory rename as a plain `Rename` and for the single-stream applied position | done | `meta::sqlite::tests::{cross_directory_rename_journals_a_single_rename_record,cross_directory_rename_hardlink_noop_keeps_journal}` (replace the deleted `rename_xpart_*` tests), `meta::sqlite::tests::read_consistent_pins_one_snapshot_across_reads`, `store_s3::commits::tests::vector_cover_is_at_least_as_far_along` |

**Design choices:**
- `Commit.applied`/`CommitPayload.applied`: `u64`, not a one-entry `BTreeMap<String, u64>` — simpler end state per the plan's explicit either/or, and every caller (publisher, bootstrap, GC, fsck) reads/writes one number now.
- `journal`/`atime_journal`/`shadow`'s `part` columns and the `HashMap<String, LeaseKeeper>`/`HashMap<String, PartState>` shapes in `cli::shipper`/`cli::lease`/`cli::node_runtime` were **kept** rather than collapsed to bare fields: with autosplit/merge gone nothing ever inserts a second key, so they always hold exactly one `p0` entry. The plan allows this ("keep the code shape reasonable; a single `PartState` is fine"); collapsing them further touched many more call sites (forwarding, control-API status, node_runtime lease-map plumbing) for no behavior change.
- `constellation_net::EpochPromise`/`EpochMachine`'s `base: BTreeMap<String, u64>` (continuation-epoch wire protocol) was left as a map, fed a one-entry `{"p0": applied_seq}` via a new `cli::main::epoch_base` helper — it is a separate, already-generic P2P structure outside this milestone's explicit removal list.
- `mtree::keys::Subsystem::Partition` was removed outright (not just reserved) since no compatibility is required; `0x06` is documented as retired.

**Left over:** one historical doc comment in `meta::sqlite` (`cross_directory_rename_journals_a_single_rename_record`'s doc) names `RenameXpartSrc`/`Dst` to explain what the test replaces — the only remaining hit of the milestone's grep gate, kept because it is explanatory, not dead code.

DESIGN.md's "### Partitions" section (namespace split/merge, per-partition
leases and logs) is now stale relative to the tree: plan 28 §P4 already
called this out as something the eventual engine swap deletes, and M0a
implements exactly that deletion ahead of schedule per plan 29's work
order. Per CONVENTIONS this is recorded here rather than edited into
DESIGN.md.

### Plan 29 M0a exit criteria

- [x] `partition` table, `xpart_pending`, and every symbol listed in the
      milestone deleted; `grep -rn -i "xpart\|PartSplit\|PartMerge\|autosplit\|part_of\|partition_of" crates docs/reference`
      returns only the one explanatory doc-comment hit above
- [x] `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo test --workspace` — 0 failures (incl. `checkpoint_concurrency::snapshot_does_not_stall_concurrent_readers` on the first try)
- [x] `cargo build --release --workspace`; `harness run baseline kill9-remount fresh-node-bootstrap two-clients-shared lease-handover snapshot-churn mtree-gc-plateau gc-lifecycle` — 8/8 PASSED
- [x] `bash tests/smoke.sh` — PASSED
- [x] `docker compose --profile test run --rm compliance` (pjdfstest) — 8798/8798 passed, empty baseline (run for extra confidence given the rename-path change; not in the milestone's explicit gate list)

## Plan 29 M0b — remove the legacy `VACUUM INTO` checkpoint: **DONE**

| Item | State | Where |
|---|---|---|
| `SqliteMeta::snapshot()` (the `VACUUM INTO` copy) deleted, along with its three tests (`concurrent_snapshots_do_not_collide`, `snapshot_strips_journal`, `snapshot_strips_pending_upload`) and the whole-file integration test `crates/meta/tests/checkpoint_concurrency.rs` (existed solely to test it) | done | `meta::replay` |
| `LogStore::{put_checkpoint, put_checkpoint_with_vector, put_checkpoint_body, get_latest_checkpoint, get_checkpoint_ref, get_checkpoint_vector, prune_superseded_checkpoints}`, `CheckpointVector`/`CheckpointRef`, `get_object_parallel` (only caller was the checkpoint GET path), `layout::checkpoint`, and every `CONSTELLATION_CHECKPOINT_*` env var (`_SNAPSHOT`, `_RATIO`, `_MIN_INTERVAL_S`, `_IO_CONCURRENCY`) deleted; all `checkpoints/*` test/prod object paths gone | done | `store-s3::{log,layout,lib}` |
| Shipper: `checkpoint()` renamed `publish()` — "publish the tree" only, no snapshot fallback branch. Byte-proportional cadence machinery deleted (`bytes_since_ckpt`, `last_ckpt_bytes`, `ckpt_ratio`, `last_ckpt_at`, `checkpoint_ratio()`, `checkpoint_min_interval()`, the free `checkpoint_is_due()`, `CHECKPOINT_RATIO`, `CHECKPOINT_MIN_INTERVAL_S`, `seed_checkpoint_baseline()`); replaced with a single counter (`shipped_since_publish`) and a plain count floor (`PUBLISH_EVERY = 32`, same value as the old `CHECKPOINT_EVERY`). Publish still runs in a spawned task and resets its counter before the work, so a cancelled publish is not retried every following segment | done | `cli::shipper` |
| Bootstrap: `BootstrapSource`/`bootstrap_source()`/`CONSTELLATION_BOOTSTRAP_SOURCE` deleted. `bootstrap()` restores the commit chain head when one exists (unchanged path), otherwise replays the whole log from seq 1 (new: previously fell back to the legacy checkpoint). `replay_from`'s "log starts past our base" bail-loud check and its test (`replay_refuses_a_base_the_log_was_pruned_past`) kept verbatim — it now guards the genesis path too | done | `cli::shipper::{bootstrap,bootstrap_from_tree,replay_from}` |
| Read-only members: already had no publisher and always fell into the (now-deleted) forced-snapshot branch; they now simply publish nothing (the `checkpoint_snapshot_forced()` branch that used to trigger for them is gone) and bootstrap from writers' commits exactly like a fresh writer mount. No code path change was needed in `node_runtime`'s read-only branch itself, only removal of the now-dead `seed_checkpoint_baseline()` call | done | `cli::node_runtime` |
| Log retention floors on the head commit's `applied` position only; with no commit, nothing is pruned. `legacy_checkpoint_vector()` and the per-partition `BTreeMap<String,u64>` floor-source plumbing deleted; `metadata_candidates` simplified to one `Option<Commit>` lookup. The `superseded-checkpoint` rule (newest-2 checkpoint objects) deleted with the objects it pruned | done | `cli::gc::metadata_candidates` |
| `fsck`: `invalid_checkpoint` class and its repair (which re-wrote a checkpoint via `meta.snapshot()`) deleted from `check_metadata_objects`; segment checks and `check_metadata_tree` untouched | done | `cli::fsck` |
| Harness: `ckpt-bulk-ingest-bounded` scenario and its sole helper `checkpoint_snapshots()` deleted (only tested the now-gone inline snapshot prune). `checkpoint-strips-pending-upload` renamed `commit-strips-pending-upload`; asserts on a commit object existing (`count_commit_objects`) instead of a commit-or-checkpoint disjunction, and drops the legacy-snapshot fallback comment | done | `harness::scenarios` |
| `docs/reference/configuration.md` and `docs/how-to-guides/development/TESTING.md` updated: checkpoint cadence/env-var rows and prose replaced with the publish cadence, `CONSTELLATION_BOOTSTRAP_SOURCE` row removed, log-retention prose updated to the commit-only floor, `ckpt-bulk-ingest-bounded`/`checkpoint-strips-pending-upload` prose updated or removed | done | `docs/reference/configuration.md`, `docs/how-to-guides/development/TESTING.md` |

**Design choices:**
- **Publish cadence**: a plain segment count (`PUBLISH_EVERY = 32`, matching the old `CHECKPOINT_EVERY`) plus the pre-existing unconditional publish on `shutdown`/`shutdown_all`. The byte-proportional ratio gate existed only to size the whole-DB snapshot in proportion to what it let the log truncate; a publish already costs O(keys changed), so there is nothing analogous to bound. No new "idle" timer was added — `shutdown_all`'s existing "flush if `shipped_since_publish > 0`" on clean unmount is what the milestone's "on shutdown/idle" refers to; there was no separate idle-only trigger before this change either.
- **GC candidate path** (carried into M0c below): log retention's floor source collapsed from `Option<(CheckpointVector, source_json)>` to `Option<Commit>` — one number, one lookup, no per-partition map (M0a already made `Commit.applied` a `u64`).
- Bootstrap's genesis path reuses `replay_from(meta, log, PARTITION, 0)` verbatim — the same pruned-log bail-loud guard applies uniformly to "resume after a commit" and "resume from nothing."

**Left over:** `docs/explanation/DESIGN.md` still describes the `checkpoints/` bucket path, periodic-checkpoint log compaction/GC, and point-in-time restore via "checkpoint + log replay" (its §2 diagram, and the GC/time-travel sections) as current design. Per CONVENTIONS this is not edited; it is now stale relative to the tree, same as M0a left DESIGN.md's partition section stale. A handful of accurate historical/comparative comments remain in code (e.g. `store-s3::commits`' "there is no checkpoint" module doc, `cli::mtree_publish`'s "a checkpoint costs O(database)") — these describe what plan 28's commit chain replaced and are left per the plan's instruction that historical prose may stay if accurate.

## Plan 29 M0c — remove the `deref` table and dead API: **DONE**

| Item | State | Where |
|---|---|---|
| `deref` table, `deref_candidates`, `clear_deref`, `backfill_deref_once`, `next_deref_seq` deleted; deref maintenance stripped out of `track_manifest_transition` (its `seq`/`unix_ms` params — needed only for `deref_seq`/`deref_unix_ms` — dropped from the signature and all 10 call sites across `meta::{sqlite,replay}`); `chunk_ref` maintenance (the existence/liveness hint index) kept as-is | done | `meta::sqlite`, `meta::replay` |
| `hash_is_live` deleted too: it existed only to decide whether `track_manifest_transition` should insert a `deref` row, so it had no caller left once that branch was removed; its query-plan regression test (`hash_liveness_probe_does_not_scan_the_inode_table`) deleted with it, and `chunk_ref_index_is_backfilled_on_open`'s trailing assertion on it dropped (the rest of that test, backfill-on-open, is unchanged) | done | `meta::sqlite` |
| Deref-only unit tests deleted (`deref_tracks_manifest_replace_and_rereference`, `deref_tracks_last_unlink`, `deref_waits_for_the_last_reference_to_a_shared_hash`); `snapshot_roots_never_enter_deref` renamed `snapshot_create_and_delete_round_trips` and trimmed to the still-valid record/list/delete round trip it also exercised | done | `meta::sqlite` |
| `cli::gc::run_chunks`: the `deref-horizon` candidate rule and its `active_designation` gate (only ever guarded the deref path) deleted. Chunk candidates now come from the orphan LIST pass alone — the pass is unconditional, no longer behind an `orphans: bool` flag — followed by the unchanged condemned-list handshake, TTL wait, re-check-and-delete. The per-candidate re-check before delete simplified to "still live? still present (HEAD)? then delete", dropping the deref-specific "clear the stale index row" branches | done | `cli::gc::{run,run_held,run_chunks}` |
| `--orphans` CLI flag removed from `constellation gc run`/`gc verify` (it meant nothing once the orphan pass is the only path); `gc::run`/`run_held`/`run_chunks` lost the `orphans: bool` parameter; `run_gc_cli` and the periodic background GC call in `node_runtime` updated. `fsck::run`'s always-orphan `gc::run(..., true, ...)` call updated to drop the now-removed argument | done | `cli::{main,node_runtime,fsck,gc}` |
| Harness: `Client::gc_process`/`gc_run` lost the `orphans: bool` parameter (the `--orphans` arg it used to conditionally pass no longer exists); all four call sites (`snapchurn.rs` ×2, `scenarios.rs` ×2) updated to the no-arg form | done | `harness::{client,snapchurn,scenarios}` |
| Dead-API inventory verified caller-by-caller and disposed of: `reintegrate_commit` deleted (+ its sole test `reintegrate_commit_is_idempotent`, which existed only to test it) — real reintegration lands through `commit_reintegration_batch`, unrelated and kept. `shadow_expire` deleted (zero callers anywhere). `shadow_touch_set` deleted (zero *production* callers; its own two test assertions in `shadow_insert_and_retire` rewritten to check the `shadow` table's row count directly instead). `ack_journal_at` deleted (zero callers; the real path is `ack_journal_rows_at`, called from `cli::shipper`). `pending_uploads_for_inode` deleted (zero callers). `pinned_ancestor` deleted (zero *production* callers; the covering test `pins_cover_descendants` renamed `pins_round_trip_and_path_resolution_covers_descendants` and trimmed to the add/list/remove-pin and `resolve_path` coverage it also carried). `reseat_usage` deleted (zero callers anywhere — `SqliteMeta::init` already seeds the usage counter straight from `recursive_size_conn`, never through this method) | done | `meta::{sqlite,reintegrate}` |
| Verified **not** dead, kept: `open_in_memory` (used pervasively by tests across every crate); `raw` (used by `apply_foreign` in `meta::replay`, `pub(crate)` so not externally visible); `peek_journal_after` (called from `meta::mutate`'s `MutateOp` dispatch — the requester-forwarding path); `read_quota`/`usage_bytes_files` (the `MetaStore` trait's `quota()`/`usage()` impls call straight through to these) | done | inventory verification only, no change |
| `SqliteMeta::db_path()` also deleted as fallout: it existed solely for `snapshot()`'s WAL-consistent reader-connection path, and had no other caller once `snapshot()` was gone (M0b) | done | `meta::sqlite` |
| `store-s3::mark` module doc's "today's `deref` table ... stay exactly as they are" note updated to record that M0c actually retired both | done | `store-s3::mark` |

**Design choices:**
- Chunk-GC candidates: strictly the orphan LIST pass (§P10's design) — no flag, no cheap/expensive distinction left to make, since it is now the only pass. The periodic background GC call in `node_runtime` (previously `orphans: false`, i.e. deref-only) now always does the LIST pass; this is the expected consequence of retiring the second source, not an oversight — plan 28 §P10 says exactly this ("any node ... can GC by reading roots").
- `track_manifest_transition`'s signature shrank from 6 params to 4 (`conn, ino, old, new`) rather than keeping unused `seq`/`unix_ms` placeholders, since both were dead the moment the `deref` INSERT they fed was deleted.

### Plan 29 M0b+M0c exit criteria

- [x] `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo test --workspace` — 0 failures
- [x] `cargo build --release --workspace`; `harness run baseline kill9-remount cold-cache fresh-node-bootstrap commit-strips-pending-upload two-clients-shared gc-lifecycle gc-dedup-race snapshot-churn mtree-gc-plateau` — 10/10 PASSED. No read-only-member harness scenario exists (`harness list` checked) so none was run.
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` — both PASSED
- [x] `grep -rn -i "checkpoint\|deref" crates docs/reference` — every hit is either an unrelated `.as_deref()`/`mtree::NodeRef` match or accurate historical prose (what plan 28's commit chain replaced, or what M0b/M0c retired); no live code, table, env var, or CLI flag remains
- [ ] `docker compose --profile test run --rm compliance` (pjdfstest) — not run; not in this milestone's explicit gate list (unlike M0a's, which ran it for extra confidence on a rename-path change — this milestone touches no FUSE op semantics)

**Found and fixed while gating:** `commit-strips-pending-upload` (the M0b rename of `checkpoint-strips-pending-upload`) failed under the full-batch run: `sync_one`'s ship loop drains the *entire* current journal backlog into one segment every time it runs, so the scenario's tight 48-file write loop produced only 1-2 large segments, never enough to cross the `PUBLISH_EVERY` (32) floor — the commit never fired, so the "pending_uploads > 0 with a commit present" window was never observed inside the 90 s poll. This is a **pre-existing scenario bug**, not a functional regression: the segment-batching mechanics are untouched by M0b, and the scenario's premise ("48 files > the 32-segment floor, so a mid-flight publish is due") silently assumed one segment per file, which `SEGMENT_BATCH`/`ship_part_taking` never guaranteed. Fixed by pacing the 48 writes 120ms apart (wider than one shipper round trip, so each close's `nudge_sync` lands its own segment) and adding 80ms of S3 latency (which also keeps the not-yet-written later files' chunk uploads pending past the point the mid-flight commit fires) — verified stable over 7 consecutive runs. The assertions themselves are unchanged.

## Plan 29 M1 — fjall engine behind the (reduced) `MetaStore` API: **DONE**

| Item | State | Where |
|---|---|---|
| `crates/meta/src/sqlite.rs` (4515 lines) deleted; `SqliteMeta` replaced by `Meta` (`crates/meta/src/store/{mod,ns,reads,writes,atime,journal,misc,snapshot,scratch,bootstrap}.rs`, ~5.6 KLOC total) on `fjall::SingleWriterTxDatabase`. Every caller mechanically renamed (`sed s/SqliteMeta/Meta/`) plus the API-shape fixes below | done | `crates/meta/src/store/*`, workspace-wide callers |
| `rusqlite` removed from the workspace: `crates/meta`, `crates/cli` (unused dev-dep), `crates/harness` (its own, unrelated snapshot-churn Oracle rewritten onto `fjall`; `assert_replica_clean` rewritten onto the control socket + a real `readdir` of the mount, since `fjall`'s lock file — unlike SQLite/WAL — refuses a second process's open while the daemon is still mounted), and the root `[workspace.dependencies]` entry. `bench/enginebench`/`bench/dbbench` keep their own direct `rusqlite` deps (excluded from the workspace) | done | `crates/{meta,cli,harness}/Cargo.toml`, `Cargo.toml`, `crates/harness/src/{snapchurn,bench,client}.rs` |
| `replay.rs` rewritten onto the write-transaction model: `TouchSet` is unchanged (pure `LogRecord` logic); `apply_records`/`apply_foreign` open one `db.write_tx()` for the whole batch (every record, the ino-counter reclaim, and the persisted usage-counter delta), matching the old one-SQLite-transaction-per-batch guarantee | done | `crates/meta/src/replay.rs` |
| `mutate.rs`/`reintegrate.rs` needed only the type rename plus two new inherent methods (`max_journal_seq`, `peek_journal_after`) `mutate::execute`'s "snapshot seq, do the op, read back everything since" idiom already relied on; their existing tests pass unchanged | done | `crates/meta/src/{mutate,reintegrate}.rs` |
| `docs/reference/configuration.md`: new "Node-local metadata engine (plan 29)" section documenting `CONSTELLATION_META_CACHE_BYTES` (default 256 MiB) and the fixed `worker_threads`/`ns` keyspace tuning | done | `docs/reference/configuration.md` |
| New tests: `an_uncommitted_write_transaction_leaves_no_keyspace_touched`, `create_leaves_the_namespace_change_and_journal_row_together` (`crates/meta/src/store/mod.rs`, white-box — need `pub(crate)` field access); `crates/meta/tests/engine.rs` (10 tests): `read_consistent_is_isolated_from_a_write_made_while_the_snapshot_is_held`, `read_consistent_pins_the_applied_seq_vector_against_a_concurrent_bump`, `unlinking_the_last_link_of_an_open_file_moves_it_to_orphans_and_reap_removes_it`, `unlinking_one_of_several_hardlinks_does_not_orphan_the_inode`, `usage_counters_persist_across_reopen`, `usage_counters_survive_replay_of_a_foreign_batch`, `ns_equals_the_key_set_an_independent_rebuild_would_produce`, `a_64kib_xattr_value_round_trips_through_the_blob_spill`, `a_large_manifest_round_trips_through_the_blob_spill`, `payload_over_64kib_does_not_overflow_the_inline_u16_length`. Every pre-existing meta test (32 in `crates/meta/src/lib.rs`'s tree + `crates/meta/tests/prune_binding.rs`) ported with only the type rename and passes with identical assertions; none deleted | done | `crates/meta/src/store/mod.rs` (`mod tests`), `crates/meta/tests/engine.rs` |

**Keyspace summary** (all in one `fjall::SingleWriterTxDatabase` directory, `meta.db`/`meta.fjall`):

- `ns` — plan 28 §P6 encoding verbatim (`constellation_mtree::keys`/`record`): `0x01` inode, `0x02` dentry, `0x03` spilled xattr, `0x04` reverse dentry, `0x30` subsystem (snapshot, quota). Tuned per plan 29's benchmark: `expect_point_read_hits`, `data_block_hash_ratio_policy(0.5)`, L0–L2 filter/index pinning. Only `nlink > 0` inodes live here.
- `atime` — `ino → atime_ns` (i64 LE), the read-time overlay every `FileAttr`-returning read applies; `constellation_mtree::record::Attrs` has no atime field by design (§P6), so this genuinely new keyspace (the old engine kept `atime_ns` in the same `inode` row) is the M1 consequence of reusing the tree's record shape locally.
- `orphans` — `ino → InodeRecord` (no `0x02`/`0x03`/`0x04` keys: xattrs are cleared before an inode moves here, mirroring the old engine) for `nlink == 0` inodes a file descriptor still has open.
- `journal` — `seq: u64 BE → postcard(LogRecord)`; `seq` is `local["next_journal_seq"]`, incremented in the same write transaction as the append (SQLite `AUTOINCREMENT`'s never-reused-even-across-deletes guarantee, reproduced with an explicit counter).
- `atime_journal` — `ino: u64 BE → postcard(atime_ns, time_ns)`, the not-yet-shipped outbox (no `part` column — M0a already made every row belong to the one implicit partition).
- `local` — string-keyed settings/counters: `node_prefix`, `next_ino`, `applied_seq`, `next_journal_seq`, `next_shadow_id`, `usage_bytes`/`usage_files`, `quota_creation_bytes`, plus whatever `cli::mtree_publish`/lease/designation code stores via the generic `kv_get`/`kv_set`.
- `pending_upload`, `chunk_ref`/`chunk_ref_by_ino` — `hash(32) ++ ino(8 BE)` (and its `ino ++ hash` mirror), maintained transactionally with every manifest change exactly as before.
- `xattr_by_name` — `name ++ 0x00 ++ ino(8 BE) → value`, a **new**, deliberately redundant index (duplicates the value) so prune-policy root discovery (`constellation_meta::prune`) does not need to reach back into `ns`'s ino-major xattr storage.
- `scratch` — the same §P6 encoding as `ns`, in its own keyspace, never journaled/replicated — a scratch → shared publish (`Meta::publish_file`) is a record copy, not a translation.
- `pins`, `epochs`, `reintegration`, `shadow` — one reasonable key layout each (`pins`: path → (ino, pinned_at); `epochs`: epoch_id → postcard row; `reintegration`: seq → (disposition, detail); `shadow`: `epoch_be ++ id_be` → `Vec<LogRecord>`, so `shadow_retire_matching`'s `WHERE epoch <= ?` is a range scan instead of the old full-table scan).
- `blobs` — `blake3(bytes) → bytes`, a **new** local content-addressed store. §P6's spill rule (`VALUE_SPILL = 1024 B`) is applied locally exactly as at publish time (via `constellation_mtree::record::{plan_inode,place_value}`), but the spilled body lives here rather than being pushed to a bucket blob (that upload only happens when M2's publisher spills a key it is about to ship). This is also what keeps an inline `Payload` under its `u16`-length-prefix ceiling for a 64 KiB xattr or an arbitrarily large manifest: anything over 1 KiB always spills to a fixed-size 33-byte pointer first, so the local record never approaches the boundary the `constellation_mtree::record::Payload` codec only guards with a `debug_assert!`.

**Design decisions:**
- **`read_consistent`** (`Meta::read_consistent<T,E>(&self, f: impl FnOnce(&fjall::Snapshot) -> Result<T,E>) -> Result<T,E>`) replaces SQLite's begin-deferred-then-throwaway-read trick with fjall's native MVCC snapshot, handed to the closure explicitly rather than resolved through a thread-local reader connection. Every reader helper that used to have a `with_reader`/`*_reader` duplicate now has a plain convenience method (opens its own snapshot) and an `_at` twin taking `&impl Readable` (`applied_seq`/`applied_seq_at`, `child_ino`/`child_ino_at`, `tree_inode`/`tree_inode_at`, `links_of`/`links_of_at`, `scan_inos`/`scan_inos_at`, `snapshots`/`snapshots_at`). `cli::mtree_publish`'s `Builder::plan`/`rebuild`/`plan_subsystems` and `cli::mtree_read::subsystem_state` were threaded to take the enclosing `read_consistent`'s snapshot explicitly, so the applied-seq vector and every read a publish plan makes are provably the same point-in-time view — the exact property the old engine's mechanism existed for. `with_reader`/`READER_CONN` have no fjall equivalent and are simply gone.
- **`commit_reintegration_batch`** (the old engine's `ATTACH DATABASE` + wholesale `INSERT...SELECT` swap) is re-expressed as `Meta::commit_reintegration_batch(&self, side: &Meta, dispositions: &[(u64,String,String)], output: &[LogRecord])`: `side` is now an already-open reconciled `Meta` (fjall has no cross-database transaction) rather than a bare path. One `write_tx` on `self` wipes and bulk-copies `ns`/`orphans`/`atime` from a snapshot of `side`, rebuilds `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name` from the new `ns` (same "namespace replaced wholesale, so the reverse indexes are all stale" reasoning as before), adopts only `side`'s `applied_seq`, marks every reconciled journal row's disposition and deletes it, and journals the reconciliation's own `output` records — all inside that one transaction, so the crash-safety boundary ("after commit the namespace and disposition ledger cannot disagree") is identical. `cli::reintegrate::run` now builds `shared` as a real `Meta` (via `shipper::bootstrap` into a directory) and passes `&shared` instead of a path.
- **Usage counters** are now durably persisted, not just process-local: `local["usage_bytes"/"usage_files"]` is updated by `store::adjust_usage_tx` inside the *same* write transaction as every mutation that moves them (create/unlink/rename/setattr/set_manifest*/publish_file/eager_clone/replay's staged batch), and `Meta::open`/`open_in_memory` seed the in-memory `UsageTracker` from those persisted values instead of an O(namespace) `recursive_size(ROOT)` walk. Verified by `usage_counters_persist_across_reopen`.
- **Quota** moved from a `local` kv mirror to living natively as `ns`'s `0x30` Quota subsystem record (`Meta::write_quota` writes it directly, in the same transaction as journaling `SetQuota`; `Meta::read_quota` reads it, falling back to the never-journaled `quota_creation_bytes` local mirror). This was necessary, not optional: `cli::mtree_read::subsystem_state` (which builds the publisher's `0x30` map) originally re-derived the tree's quota record from the old `kv["quota_max_bytes"]` string mirror, and since M1 never populates that mirror (quota's source of truth is `ns` now), the publisher was silently never writing a quota record into the tree at all — caught by the ported `shipper::tests::a_fresh_replica_bootstraps_from_the_commit_chain` test (`dump_replicated()` mismatch after a bootstrap-from-commit round trip) and fixed by adding `Meta::replicated_quota_record_at` (returns `ns`'s raw `0x30` bytes under the caller's snapshot) for the publisher to copy verbatim.
- **Snapshot/quota codec migration**: `snapshot_record`/`quota_record`/their parsers moved from `cli::mtree_read` into `constellation_meta::store` (re-exported at the crate root) per the plan's explicit ask, so the replica and the publisher share one implementation; `cli::mtree_read` now imports them instead of keeping a duplicate.
- **Two small, deliberate behavior fixes** (documented as "no backwards compatibility required" per the plan): (1) `rename`'s replace-of-an-existing-target path now calls `track_manifest_transition` for the replaced file's dropped manifest, closing a `chunk_ref` leak the old engine had (flagged during the sqlite.rs inventory: the old `rename_in_tx` never updated `chunk_ref` on this path); (2) `scratch_mkdir` now pre-checks for an existing ino the way `scratch_create` always did, and `scratch_rename` now rejects an occupied destination with `MetaError::Exists` instead of silently overwriting it (the old code let a raw, unmapped SQLite constraint violation escape there).
- **`assert_replica_clean`** (harness, `snapshot-churn`/`snapshot-lifecycle`) used to open the node's `meta.db` file directly from the harness process while the node was still mounted — safe under SQLite/WAL, but fjall's lock file refuses a second process's open outright. Rewritten to ask the *live* daemon instead: snapshot count via a new `Client::snapshot_count()` (`constellation snapshot ls` over the control socket) and churn-root emptiness via a real `std::fs::read_dir` of the mounted filesystem, rather than any direct replica access.
- **`constellation gc run`/`gc verify` alongside a live mount** (`gc-lifecycle`, `mtree-gc-plateau`) hit the same class of problem, but as a genuine product-behavior regression rather than a test-only one: the old design deliberately let an independent `gc` CLI invocation open the *same* `meta.db` file a mounted daemon already had open (SQLite/WAL's whole point), and `harness::scenarios::{gc_lifecycle,mtree_gc_plateau}` (unlike `fsck-repair`, which already unmounts first) rely on exactly that — `fjall`'s single-process lock refuses it outright (`FjallError: Locked`). Fixed with a proper control-socket route rather than a workaround: a new `constellation_api::Request::GcRun{verify_only}` / `Response::GcReport{report: serde_json::Value}` (untyped JSON, since `constellation-api` sits below `cli` and must not depend on its report types) and `StatusSource::gc_run`, implemented in `DaemonStatus::gc_run` by calling the *same* `gc::run(...)` the CLI path uses against the daemon's own already-open `Arc<Meta>`/`ChunkStore` (a new `PinManager::chunks()` accessor supplies the latter — `DaemonStatus` already held everything else `gc::run` needs). `run_gc_cli` now pings `state_dir`'s control socket first: a live daemon answers there and the CLI just relays its `GcReport`; no daemon (nothing mounted, the offline-maintenance case) falls through to opening the store directly exactly as before, so `constellation gc run` still works with nothing mounted. `constellation fsck` was not given the same treatment — no exercised scenario invokes it while mounted (`fsck-repair` unmounts first) — but it has the identical latent lock risk if it were ever run that way; left as a known follow-up, not silently patched over.
- **`recursive_size`**: a DFS over `0x02` in one snapshot (no maintained per-directory counter — that is M3), same asymptotic behavior as the old `WITH RECURSIVE` CTE.
- **The `snapshot-churn` harness Oracle's own concurrency model** hit the identical single-process-lock wall a third time: its four seeded workers each independently called `Oracle::open(&oracle_path)` per round (valid under SQLite/WAL — many connections to one file from one process is exactly what it's for), and every open past the first failed with `FjallError: Locked` since `fjall` does not distinguish "another handle in this same process" from "another process." Fixed by making `Oracle` `#[derive(Clone)]` (its fields are already `Arc`-backed `fjall` handles) and sharing one instance across workers via `oracle.clone()` instead of re-opening by path; `worker_round` now takes an owned `Oracle` rather than a `&Path`.
- **`constellation export`** (`cmd_export`) had the same latent bug as `gc`/`fsck`, found by code review rather than by a failing gate (no harness scenario exercises `export` on a currently-mounted filesystem): it probed whether a node id was claimed by opening `meta.db` directly and treating any error — including a new `Locked` from a live daemon — as "not claimed," which would have skipped the graceful self-leave-over-the-control-socket path entirely and gone straight to deleting the state dir out from under a running daemon. Fixed by checking the control socket (`Request::Ping`) first: a daemon that answers has necessarily already claimed a node id, so only a *confirmed-not-running* daemon falls back to the direct `Meta::open` probe (safe again, since nothing holds the lock).
- **`commit_reintegration_batch`** initially missed copying `side.blobs` into `self.blobs` during the swap — caught in review (no exercised scenario currently drives a reintegration with a spilled payload): a `Payload::Spilled` reference in `side`'s `ns` would have resolved to an empty body after the swap, since its blob only existed in `side`'s own store. Fixed by adding a plain content-addressed union copy of `side.blobs` into `self.blobs` in the same transaction, alongside `ns`/`orphans`/`atime`.

`docs/explanation/DESIGN.md` still names SQLite as the node-local metadata engine (its architecture diagram, §"Every node keeps a full metadata replica in SQLite", and the replica-size/enabled-by-SQLite sections). Per CONVENTIONS this is not edited; it is now stale relative to the tree, same as M0a/M0b left its partitions/checkpoint sections stale.

**Left over (explicitly deferred, not this milestone's scope):** M2 (native publisher key-delta tracking, `start_ingestion` bootstrap) and M3 (directory-local ino allocation, per-directory recursive size, `blobs/` GC, the open harness flakes) are unchanged from plan 29's own milestone boundaries. `xattr_by_name`'s value duplication (rather than reaching back into `ns`) is a deliberate simplicity/consistency trade-off, not a follow-up.

### Plan 29 M1 exit criteria

- [x] `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo test --workspace` — 0 failures (32 pre-existing `crates/meta` tests + 2 new white-box tests + 11 new `crates/meta/tests/engine.rs` tests [10 assertions + 1 `#[ignore]`d perf-sanity test], plus every other crate's suite unchanged)
- [x] `cargo tree -i rusqlite` — no matching package in the workspace (errors "did not match any packages", i.e. it is not in the dependency graph at all)
- [x] `cargo build --release --workspace`; `target/release/harness run baseline kill9-remount cold-cache fresh-node-bootstrap commit-strips-pending-upload two-clients-shared lease-handover gc-lifecycle snapshot-churn snapshot-lifecycle clone-workflow mtree-gc-plateau e2e-basic` — 12/12 PASSED (first attempt surfaced and this milestone fixed three fjall-single-process-lock bugs: `gc run`/`gc verify` alongside a live mount, the harness's own `snapshot-churn` Oracle opening per-worker, and — found by review, not by this gate — `cmd_export`'s node-id probe; second attempt (with all three fixed) was clean)
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` — both PASSED
- [x] `docker compose --profile test run --rm compliance` (pjdfstest) — 8798 passed, 0 failed, empty baseline
- [x] Perf sanity (`crates/meta/tests/engine.rs::getattr_throughput_sanity`, `#[ignore]`d, 200k inodes, in-memory, release build): 682,460 ops/s single-threaded, 2,518,513 ops/s at 16 threads — reads keep scaling across threads, consistent with plan 29's benchmarked expectation for the tuned `ns` keyspace

## Plan 29 M2 — publisher and bootstrap native to fjall: **DONE**

| Item | State | Where |
|---|---|---|
| Dirty-key tracking centralised in the `ns` write path: a new `dirty` keyspace (`key -> counter: u64 BE`), a persisted `next_dirty_seq` counter, and `store::ns::Dirty` (`Untracked` for `scratch`, `Tracked` for `ns`) threaded through every helper that used to call `tx.insert`/`tx.remove` directly. `Meta::dirty_snapshot`/`clear_dirty_upto`/`has_dirty` are the publisher's whole read/retire API | done | `crates/meta/src/store/{mod,ns,misc,writes,snapshot,bootstrap}.rs`, `crates/meta/src/replay.rs` |
| New test `crates/meta/tests/dirty.rs` (6 tests): `every_mutating_api_dirties_the_keys_it_changes` diffs a full `ns` dump before/after every mutating call (mkdir/create/symlink/mknod/link/unlink/rmdir/rename incl. replace-existing/setattr/set_manifest/set_xattr incl. spill/remove_xattr/publish_file/set_manifest_dirty/set_manifest_with_base/record_snapshot/delete_snapshot/write_quota/eager_clone/`apply_records` replay/`commit_reintegration_batch`) against `dirty_snapshot`'s key set, generically (superset, not a hand-picked list) — plus `apply_atime_dirties_nothing`, `bootstrap_dirties_the_genesis_root`, `clear_all_dirty_empties_the_set_and_tracking_resumes`, and the two `clear_dirty_upto` counter-check tests | done | `crates/meta/tests/dirty.rs` |
| Publisher rewritten onto key deltas (`crates/cli/src/mtree_publish.rs`, `Touched`/`ReadSet`/`Builder`/full `rebuild` all deleted, ~1.6 KLOC net rewrite): `publish_batch` reads one fjall snapshot, takes `Meta::dirty_snapshot` as its whole read set, and for each key reads `ns`'s current value (absent → delete edit); `0x01`/`0x03` values are expanded (`Meta::tree_inode_at`/`resolve_local_payload_at`) and re-placed with `record::{plan_inode,place_value}` against the *published* hasher, collecting blobs to upload before the commit; everything else (`0x02`/`0x04`/`0x30`) copies verbatim via `Meta::ns_get_at`. On a landed commit (direct or spliced) `clear_dirty_upto` retires exactly the `(key, counter)` pairs this publish observed | done | `crates/cli/src/mtree_publish.rs` |
| §P3 splice simplified: the old read set (dirty entities' prefixes/keys) is gone because it is now identical to the write set — every decision that used to need a scan (a dentry's old value, a link count, "is anything else at this `0x03`/`0x04` range") is folded into the write path that already dirties the keys those decisions touch. `Plan::conflicts_with` is one check: does the winner's diff touch any key this plan edits | done | `crates/cli/src/mtree_publish.rs` (`Plan`, `TreePublisher::splice`) |
| Bootstrap rewritten onto `fjall::Keyspace::start_ingestion`: `Meta::ns_ingest_page` (bulk-loads pre-sorted `(key, value)` pages, bypassing the write-transaction/dirty path entirely — verified against `/tmp/fjall`'s `Ingestion::last_key` that ordering is checked *within* one session, not *across* sessions, which is what lets `cli::mtree_read::load_tree` keep two independently-paged streams: `main` for the four verbatim-order kinds and `local_xattrs` for re-encoded `0x03` entries, since a re-encoded `0x03` key sorts after every `0x01` key and cannot share a growing page with them), `Meta::encode_local_inode` (re-runs `record::plan_inode`/`place_value` against the *local* hasher — the mirror image of the publisher's conversion), `Meta::rebuild_derived_from_ns` (rebuilds `chunk_ref`/`chunk_ref_by_ino`/`xattr_by_name` and, newly, the persisted usage counters — a gap in M1's `load_tree_rows` path that never populated them after a bootstrap), and `Meta::clear_all_dirty` (retracts whatever `Meta::open`'s genesis root insert speculatively dirtied, since the ingested tree already equals the published one) | done | `crates/meta/src/store/bootstrap.rs`, `crates/cli/src/mtree_read.rs` |
| `rebuild_root` kept as the one full-namespace-walk code path, for `fsck` and the genesis/first-publish-with-no-parent case: iterates a `ns_dump_at` snapshot, converts each key's value with the same `republish_present` helper the incremental planner uses, and calls `Tree::build` (a bulk build, not `apply`) | done | `crates/cli/src/mtree_publish.rs`, used by `crates/cli/src/fsck.rs` |
| Idle publish (moved up from M3): `Shipper::publish_idle_due` — a publisher is attached, `Meta::has_dirty()`, and `CONSTELLATION_PUBLISH_IDLE_S` (default 30) has elapsed since the last publish attempt — checked in `sync_all_inner` once a round finds nothing left to ship. `shutdown`/`shutdown_all` now gate their final publish on `has_dirty()` rather than `shipped_since_publish`, so a replica that only ever tailed foreign segments still gets a last commit | done | `crates/cli/src/shipper.rs`, documented in `docs/reference/configuration.md` |
| Removed now-dead API: `Meta::tree_inode`/`links_of`/`links_of_at`/`scan_inos`/`scan_inos_at` (the old builder's read surface — `tree_inode_at` alone survives, now used by the new publisher), `applied_seq_reader`/`kv_get_reader`/`child_ino_reader`/`snapshots_reader` (unused duplicate accessors), `load_tree_rows`/`load_tree_subsystems`/`finish_tree_load` and `cli::mtree_read`'s `Subsystems`/`subsystem_state`/`PUBLISHED_SUBSYSTEMS` (superseded by ingestion — `0x30` keys need no special handling once bootstrap copies `ns` byte-for-byte) | done | `crates/meta/src/store/{reads,mod,snapshot}.rs`, `crates/cli/src/mtree_read.rs` |
| Tests adapted: the incremental-equals-rebuild property now compares each step's published root against an independent `rebuild_root` call rather than a second publisher's first publish (which no longer means anything under key-delta tracking — two publishers sharing one `Meta` would share one `dirty` keyspace too); the disjoint/overlapping/behind-head/ahead-winner §P3 race tests moved from "two publishers sharing one `Meta`" to `Fixture::peer` — a genuinely separate `Meta` under its own ino prefix, replayed to the same state and then bootstrap-equivalent (`clear_all_dirty`) before it diverges locally, which is what a second real node actually looks like; `a_restart_never_needs_a_rebuild` replaces the old vector-equality rebuild test; `a_cancelled_publish_keeps_its_batch` now asserts on `Meta::has_dirty()` instead of a removed `Touched` pending set | done | `crates/cli/src/mtree_publish.rs` |
| New tests: `a_key_re_dirtied_mid_publish_survives_the_clear` (a `SlowStore` gated by a `tokio::sync::Notify` — not a fixed sleep, so the test can release exactly one stalled `put_opts` and let the publish land — proves `clear_dirty_upto`'s counter check against a real publish, not just the unit-level meta test), `a_spilled_value_round_trips_through_publish_and_bootstrap` (plain and `E2eKeys`-keyed hashers, manifest + xattr + symlink target all spilled), `a_bootstrap_then_publish_is_a_small_delta` (bootstrap a 1,000-inode commit, one `create`, next publish's `intent.ops <= 8`) | done | `crates/cli/src/mtree_publish.rs`, `crates/cli/src/mtree_read.rs` |

**Design decisions:**

- **The local↔published spill conversion** is symmetric and lives at two call sites only. Publish direction (`republish_present` in `mtree_publish.rs`): "expand" a local `0x01`/`0x03` value to plaintext (`Meta::tree_inode_at`/`resolve_local_payload_at`, resolving any local `Payload::Spilled` against `Meta`'s own `blobs` keyspace), then "place" it against the *published* hash function (`BlobStore::hash`, keyed on E2E) with `record::plan_inode`/`place_value`, collecting new blob bodies to upload before the commit. Bootstrap direction (`Meta::encode_local_inode` in `crates/meta/src/store/bootstrap.rs`): the caller resolves a *published* `Payload::Spilled` reference via `BlobStore`/`Resolver` (whatever hash scheme the filesystem uses), then this re-places the plaintext against the *local* hash function (`Meta::hash_blob`, always plain blake3) with the same `plan_inode`/`place_value` primitives. Neither direction re-decides the inline-vs-spill threshold from scratch on an assumption; both call the real §P6 spill logic on the resolved plaintext, so a manifest that spilled only because of an unrelated large field on the *other* side re-decides correctly rather than assuming "spilled implies still spilled".
- **The splice's read-set collapse** is the milestone's central simplification, not just smaller code: under M1's key encoding, every read the old `Builder::plan` needed to make to *resolve* a batch (a dentry's stale target, a directory's current link set, "did this xattr set used to spill") is now a read some *write* already made and dirtied. `Plan::conflicts_with` therefore only has to ask "does the winner's diff intersect the keys I am about to write", because there is no separate class of "keys I only read". The plan's own worked argument (§P3, `docs/plans/v1/wip/29-fjall-metadata-engine.md`) held up under the four adapted race tests without needing a fallback broader check.
- **`fjall::Keyspace::start_ingestion` ordering** was verified against source (`~/.cargo/registry/.../lsm-tree-3.1.10/src/tree/ingest.rs`) rather than assumed: `Ingestion::last_key` is per-session state, reset by every `Ingestion::new`, so cross-session ordering is not required — only within one session's `write` calls. This is what licenses `load_tree`'s two-page design (`main` and `local_xattrs`, flushed independently) instead of a five-way split by key prefix, which would have needed buffering the whole tree per prefix bucket rather than paging a single ordered cursor pass.
- **Two-`Meta` race tests, not two-publisher-one-`Meta`.** M1's `Fixture` gave every `TreePublisher` in a test the *same* `Arc<Meta>`, which worked when the changed-key set was `Touched`, computed from records handed in by the test by hand. Under M2 the changed-key set is a property of the replica (the `dirty` keyspace), so two publishers sharing one `Meta` share one dirty set — the second one to run would find nothing left to publish that the first had not already cleared. `Fixture::peer` fixes this the way two real nodes actually differ: separate `Meta`s, separate ino prefixes, replayed to the same starting content, with `clear_all_dirty` marking that starting content as already-published (exactly what a real bootstrap's `Meta::clear_all_dirty` does) before the two diverge with independent local writes.
- **Usage counters after ingestion** were a latent gap in M1's `load_tree_rows` bootstrap path (it never called `adjust_usage_tx`, so a bootstrapped replica's `usage_bytes`/`usage_files` silently stayed at 0 until enough local/replayed writes happened to drift them back toward correct). `Meta::rebuild_derived_from_ns` closes this by summing reachable file sizes over `ns` once, after ingestion — the same asymptotic cost as the `chunk_ref`/`xattr_by_name` rebuild it now sits next to.

**Left over (explicitly deferred, not this milestone's scope):** plan 29 M3 (directory-local ino allocation, per-directory recursive size, `blobs/` GC with a two-mark horizon, the open harness flakes) and M4 (leaseless optimistic commits) are unchanged from plan 29's own milestone boundaries. `docker compose --profile test run --rm compliance` (pjdfstest) was not part of this milestone's requested gate list and was not re-run; nothing in this milestone touches a FUSE-visible code path (the change is entirely in the metadata engine's internal dirty-tracking and the publisher/bootstrap that consume it), so no compliance regression is expected, but this is a stated gap rather than a verified pass.

### Plan 29 M2 exit criteria

- [x] `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo test --workspace` — 0 failures (meta: 34 pre-existing + 6 new `dirty.rs` tests + 10 `engine.rs` + 1 ignored perf test + 2 `prune_binding.rs`; cli: 190 passed + 1 ignored measurement test, including 11 `mtree_publish` tests [2 new], 4 `mtree_read` tests [2 new], all `shipper`/`fsck`/`mtree_gc`/`snapshot` tests green; every other crate's suite unchanged)
- [x] `cargo build --release --workspace`
- [x] `target/release/harness run baseline kill9-remount cold-cache fresh-node-bootstrap commit-strips-pending-upload two-clients-shared lease-handover gc-lifecycle snapshot-churn snapshot-lifecycle clone-workflow mtree-gc-plateau e2e-basic e2e-two-nodes` — 14/14 PASSED
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` — both PASSED
- [x] Measurement (`mtree_publish::tests::getattr_latency_during_a_publish`, `#[ignore]`d, 100,000 files, release build): idle p50 2.227µs / p99 3.69µs / max 104.375µs (649,176 samples) vs. during a 1735 ms full publish (300,004 keys) p50 2.374µs / p99 4.303µs / max 248.461µs (697,900 samples) — getattr latency is essentially unaffected by a concurrent publish, confirming the "not stalling FUSE" claim still holds under key-delta publishing. Bootstrap from that same commit: 100,002 inodes, 100,001 dentries in 1,340 ms (in-memory store, cold node cache)

## Plan 29 M3a — the open issues: ino locality, GC liveness lag, `blobs/` GC, offline-tool lock avoidance, bootstrap speed: **DONE**

| Item | State | Where |
|---|---|---|
| Directory-local ino allocation (plan 28 §S1b): `INO_BLOCK_SIZE = 1024` per-directory block cursor (`ino_alloc: dir_ino(8 BE) -> block_start(8 BE) ++ used(4 BE)`, node-local, unjournaled), lazily reserving a fresh aligned block from the node's global `next_ino` counter when a directory has none yet or its active block is full. `alloc_ino_tx`/`Meta::allocate_ino` take a directory hint; every call site (`mkdir`/`create`/`symlink`/`mknod`, the snapshot clone loop, `fusefs_ops`'s four FUSE handlers) now passes the real parent. `reclaim_ino_counter` (replay's own-history guard) protects the whole block containing a reclaimed ino, not just the single value, since blocks are the unit `alloc_ino_tx` advances by | done | `crates/meta/src/store/mod.rs`, `crates/meta/src/store/{writes,snapshot}.rs`, `crates/cli/src/fusefs_ops.rs` |
| GC liveness must not lag the log (chunk GC computed the live set from the local replica alone): `gc::GcTail` tails the metadata log to head before marking and again after the condemned-list TTL wait, aborting the round (no mark, no delete) if either tail fails. `GcTail::Daemon` asks the live sync task to tail via a new `fusefs::SyncRequest::TailToHead` (added to `DaemonStatus`'s control-socket dispatch and the periodic in-daemon GC tick); `GcTail::Standalone`/`GcTail::standalone` attaches a throwaway tail-only `Shipper` to the caller's own `LogStore`/`Meta` (used by `constellation gc`, `fsck`'s internal orphan pass, and the pre-sync-channel periodic tick) | done | `crates/cli/src/gc.rs`, `crates/cli/src/fusefs.rs`, `crates/cli/src/node_runtime.rs`, `crates/cli/src/main.rs`, `crates/cli/src/fsck.rs` |
| `blobs/` GC (plan 28 S7b leftover), a two-mark horizon riding the pack sweep's existing mark rather than a second walk: `constellation_store_s3::mark` now decodes every `0x01`/`0x03` leaf value it already visits and returns the `Payload::Spilled` blob hashes found (`Mark::blob_hashes`); `mtree_gc::run` folds a blob round into the same commit-retention/mark/condemn/wait/re-mark round packs already do, tracking first-seen-unreferenced times in a bucket object (`gc/blob-candidates.json`, not node-local kv, so any node's round continues another's bookkeeping) and condemning (`gc/condemned-blobs.json`) only candidates whose horizon has elapsed *and* are still unreferenced at the re-mark | done | `crates/store-s3/src/mark.rs`, `crates/store-s3/src/{layout,gc}.rs`, `crates/cli/src/mtree_gc.rs` |
| Publisher-side half of the blob handshake: `BlobStore::put`'s `AlreadyExists`-is-success dedup rule cannot see a GC condemnation, so `mtree_publish`'s `publish_batch` re-reads `read_condemned_blobs` right before the commit CAS (mirroring the existing pack check) and defers the whole publish if any blob it just wrote is condemned — the retry either lands after GC clears the condemnation or re-uploads into a blob GC has by then forgotten | done | `crates/cli/src/mtree_publish.rs` |
| Offline tools vs the single-process lock: `constellation fsck` now routes through a running daemon's control socket exactly like `constellation gc` (M1) — new `Request::FsckRun`/`Response::FsckReport`, `StatusSource::fsck_run` (default-refusing) plus `DaemonStatus::fsck_run` (in-process `fsck::run` using the daemon's own store/meta/lease-mode, a fresh `LogStore` from `chunks.e2e_keys()`, and the daemon's `state_dir`/`compression`, both newly threaded into `DaemonStatus`); `run_fsck_cli` tries the socket first, falls through to a direct `Meta::open` only when nothing answers. `FsckReport`/`FsckIssue` gained `Deserialize` for the round trip. Audited every other subcommand for a similar gap: `scan` names no CLI subcommand (nothing to fix); `inspect`/`prune run`/`prune ls`/`prune status` never call `Meta::open` at all (control-socket only, so no raw-lock failure mode exists); `leave`/`export` already carried a daemon-aliveness check with a `Meta::open` fallback gated on the daemon being *confirmed* dead (pre-existing, from before this milestone) | done | `crates/api/src/{lib,types}.rs`, `crates/cli/src/{main,fsck}.rs` |
| Bootstrap speed: two independent fixes to `load_tree`'s dominant costs, found by instrumenting `mtree_publish::tests::getattr_latency_during_a_publish`'s bootstrap phase rather than guessing. (1) `InodeRecord.xattrs_spilled: bool` (a new record flag bit) replaces `xattrs.is_empty()` as the "does this inode's xattr set live in `0x03`" signal — an empty inline list is both the spilled case *and* the overwhelmingly common "no xattrs at all" case (§P6's census), so probing on emptiness paid a full tree range query per inode even when nothing was there; measured at ~900 ms of a 1.34–1.56 s, 100k-inode bootstrap. (2) `Meta::apply_bootstrap_indexes`/`BootstrapIndexBuilder` replace a post-ingest `rebuild_derived_from_ns` that re-read and re-decoded every `0x01` value from `ns` twice more (once for `chunk_ref`/`xattr_by_name`, once for usage) — `load_tree` already decodes each `InodeRecord` once to build its local encoding, so a builder fed from that same pass collects the identical derived state with zero re-decoding; measured at ~215 ms recovered. `rebuild_derived_from_ns`/`usage_from_ns_tx` deleted (their only caller); `rebuild_indexes_tx` kept (still used by `commit_reintegration_batch`) | done | `crates/mtree/src/record.rs`, `crates/meta/src/store/{bootstrap,misc}.rs`, `crates/cli/src/mtree_read.rs` |

**Measurements:**

- **Ino locality** (`crates/meta/tests/ino_locality.rs::measure_directory_local_setattr_amplification_old_vs_new_allocator`, `#[ignore]`d, release build): 64 directories interleaved over 400 rounds (25,600 files), old global-counter allocator reimplemented test-only as the baseline (no such code path remains in production). A directory-local "setattr" (rewrite every child inode of one directory in one commit) against the resulting aged tree, measured via `constellation_mtree::MemoryNodeStore`'s distinct-write counter: **213 distinct node writes under the old global counter, 5 under the new block allocator — 42.6×**. Interleaved-creation unit tests (`interleaved_creates_cluster_each_directorys_children_into_few_blocks`, `a_directory_that_exceeds_one_block_gets_a_second_contiguous_block`, `unrelated_directories_never_collide_on_the_same_ino`) are not ignored and run every `cargo test`.
- **Bootstrap** (`mtree_publish::tests::getattr_latency_during_a_publish`, `#[ignore]`d, 100,000 files, release build, in-memory store, cold node cache): **1,340–1,560 ms before this milestone's two fixes → 256–397 ms after**, comfortably under the 500 ms target and at or below the old SQLite loader's 358 ms. Per-phase instrumentation (added temporarily, not left in the tree) attributed the fix: the xattr-probe change recovered ~900 ms, the derived-index fold recovered ~215 ms.

**Design decisions:**

- **Ino block size is 1024**, matching §14.10's own "distinct 1024-ino bucket" spread statistic exactly, so a directory that stays within one block also stays within one bucket by that measure. No bucket migration is implied by growth past a block boundary or by this change at all: existing (pre-locality) inos are untouched, and an ino is opaque allocation policy, never format.
- **`blobs/` shares the pack sweep's one grace wait** rather than running its own two-mark round with a second lease-TTL sleep: both marks packs already compute (before and after the wait) now also carry `blob_hashes`, so blob condemnation piggybacks on marks that would happen anyway. This trades a small amount of decode work every pack-GC round (always paid, whether or not any blob is a candidate) for never doubling the round's wall-clock wait.
- **Blob candidate bookkeeping lives in the bucket** (`gc/blob-candidates.json`), not node-local kv, deliberately mirroring §P10's "no full local replica needed" — any node's round can continue the horizon clock a previous round (possibly on a different node) started.
- **The pack GC's synthetic benchmark fixtures used a raw `0x01`-range byte as filler** (`bench`-style pseudorandom key/value pairs unrelated to the real `InodeRecord` codec) to build large trees cheaply for `store-s3::mark`'s cost-scaling tests; extending `mark` to decode `0x01`/`0x03` values for blob extraction made that filler fail to decode. Fixed by moving the filler to the `0x02` (dentry) range, which `mark`'s blob extraction never inspects — a one-line change per fixture, no assertion loosened.
- **`fsck`'s in-daemon path does not re-fetch `fsmeta.json`** for compression settings the way a fresh standalone invocation must; it reuses the daemon's own `chunks: Arc<ChunkStore>` and threads `state_dir`/`compression` through `DaemonStatus` instead, avoiding an extra bucket round trip and keeping the in-daemon fsck's cache-cruft check working (a `state_dir: None` standalone-style call would silently skip it).

**Left over (explicitly deferred, not this milestone's scope):** plan 29 M4 (leaseless optimistic commits) is unchanged from plan 29's own milestone boundary. Per-directory recursive size without a recursive CTE (plan 28 §P7) and wiring `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` were named in plan 29's M3 scope but not in this M3a task list and were not touched. The `atime-eventual`, `deposed-reintegration`, `chaos-ci` (create-storm EIO), and `named-shared-daemon` (umount hang) harness flakes named in plan 29's M3 scope are unchanged — root-causing them was not part of this milestone's five tasks. `docker compose --profile test run --rm compliance` (pjdfstest) was not part of this milestone's given gate list and was not re-run; the ino allocator change touches the FUSE-visible create/mkdir/mknod/symlink path (inos are opaque to POSIX, so no compliance regression is expected, but this is a stated gap rather than a verified pass).

### Plan 29 M3a exit criteria

- [x] `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] `cargo test --workspace` — 0 failures across every crate (meta: existing suites + 3 new `ino_locality.rs` tests [+1 ignored measurement] + `xattrs_spilled_distinguishes_no_xattrs_from_spilled_and_round_trips` in `constellation-mtree`; cli: existing suites + `gc::tests::gc_tails_a_lagging_replica_before_marking_a_deduplicated_chunk` + 5 new `mtree_gc::tests` blob-GC tests + `fsck-while-mounted` harness scenario; store-s3: existing 129-test suite green after the two fixture fixes)
- [x] `cargo build --release --workspace`
- [x] `target/release/harness run baseline kill9-remount cold-cache fresh-node-bootstrap two-clients-shared gc-lifecycle gc-dedup-race snapshot-churn mtree-gc-plateau e2e-basic` plus `fsck-repair` and the new `fsck-while-mounted` — 12/12 PASSED
- [x] `bash tests/smoke.sh` and `bash tests/integration.sh` — both PASSED
- [x] Measurements above (ino locality 42.6×, bootstrap 1.34–1.56 s → 256–397 ms)

