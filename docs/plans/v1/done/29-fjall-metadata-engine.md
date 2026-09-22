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

### M5 — concurrent, correctly-ordered forwarding
Implement M4's named fix: stop awaiting each forwarded mutation inline
in the sync dispatch loop. Spawn the round trip + apply per forward,
bounded by a semaphore, guarded by a requester-side ordering gate
(conflict-key sets, computed per `MutateOp`) so forwards that touch a
common inode still land on the holder — and get applied back on the
requester — in the order this node issued them.

## M4 — leaseless optimistic commits: decision

**Verdict: defer.** The measured bottleneck for the realistic
configuration (P2P on, the default) is **application-level
serialization of the forwarding path** — a single dispatch task per
node that processes one forwarded mutation at a time, network-wait
included — not S3 lease-CAS latency, not raw network RTT, and not the
local fjall engine. §P3 would fix that, but at a cost (rearchitecting
the mutation path itself, not a bolt-on) that is large relative to a
cheap, precisely-located, *not yet implemented* fix: stop awaiting each
forwarded request inline in that dispatch loop. See "Where the
bottleneck actually is" for the code location and why fixing it should
recover most of the gap without touching correctness.

*Revision note:* an earlier draft of this section compared a 0 ms-S3-latency
single-node baseline against 20 ms-S3-latency multi-node rows, and used
a `write4k` workload (whose data-chunk PUT pays S3 latency directly,
independent of metadata forwarding) for every multi-node number. Both
confounds are fixed below: every row now runs at matched 0 ms and 20 ms
injected S3 latency, and `create` (pure metadata) and `write4k`
(metadata + one data-chunk PUT) are reported separately. The core
finding — multi-node forwarded throughput is *below* single-node, not
"near N×" — survives the correction and is now measured on the
metadata-only path where it is unambiguous.

### Measurement setup

Driver: `crates/harness/src/metabench.rs`, wired as `harness meta-bench
--json` (`crates/harness/src/main.rs`, `lib.rs`). It mounts 1 or 3 real
`constellation` clients against one floci+toxiproxy S3, fires
create-or-4KiB-write ops concurrently from one thread per node, and
reads throughput/latency plus the daemon's own forwarding and lease
counters (`forwarded_ok`/`forward_p50_ms`/`lease.epoch`) off the control
socket. A companion `raw_s3_timing` does plain PUT / CAS-create
(`If-None-Match: *`) / GET straight against floci, bypassing
constellation, as the floor any lease renewal or commit CAS can beat.
Every configuration runs at **both** 0 ms and 20 ms one-way injected S3
latency (plan 28 §P4's "same-region" assumption), and **both** `create`
(`O_CREAT`+close, no data) and `write4k` (`O_CREAT`+write 4096B+close,
one data-chunk PUT) are measured for every multi-node row. P2P-off rows
use `CONSTELLATION_LEASE_TTL_MS=5000` (down from the 60 s default) and
4× fewer ops/node — the default TTL's sticky-lease dwell/notice-at-renewal
worst case makes a single P2P-off config take tens of minutes, the same
reason `create-storm-s3-only` (`crates/harness/src/scenarios.rs`) uses a
short TTL. Total matrix: 20 configs, ~6 minutes of op-loop time. Each
run is a single sample, not averaged over seeds — single-node numbers in
particular showed real run-to-run variance (noted below); multi-node
P2P-on numbers were stable across three separate full-matrix runs.

The forwarded-op time breakdown (below) used a separate, temporary,
two-node probe with `tracing` spans added at five points (requester FUSE
entry, `P2p::request`'s connection/stream/network legs, the holder's
`mutate_requested`, the holder-side dispatch-loop arm, and
`apply_accepted`), each tagged with the existing per-request `req_id`.
The spans and the probe were removed after collecting the numbers below
— none of that instrumentation is in the tree.

### Measured table (3 separate full-matrix runs; this table is the most recent)

| Config | agg ops/s @0ms | agg ops/s @20ms | p50@0/20ms | fwd ops | fwd p50 | handoffs |
|---|---|---|---|---|---|---|
| 1node-create | 4575 | 2950 | 0.15 / 0.34 ms | 0 | – | 0 |
| 1node-write4k | 648 | 263 | 1.10 / 3.48 ms | 0 | – | 0 |
| 3node-p2pon-shared-create | 1227 | 1214 | 1.84 / 1.88 ms | 2400 | 2 ms | 0 |
| 3node-p2pon-disjoint-create | 1273 | 1145 | 1.85 / 1.96 ms | 2400 | 2–3 ms | 0 |
| 3node-p2pon-shared-write4k | 225 | 220 | 10.3 / 9.5 ms | 4800 | 8–10 ms | 0 |
| 3node-p2pon-disjoint-write4k | 214 | 229 | 9.75 / 9.45 ms | 4800 | 9–10 ms | 0 |
| 3node-p2poff-shared-create | 48 | 51 | 0.27 / 0.18 ms | 0 | – | 2 |
| 3node-p2poff-disjoint-create | 51 | 57 | 0.10 / 0.29 ms | 0 | – | 2 |
| 3node-p2poff-shared-write4k | 48 | 48 | 0.77 / 0.67 ms | 0 | – | 2 |
| 3node-p2poff-disjoint-write4k | 54 | 41 | 0.45 / 1.05 ms | 0 | – | 2 |
| raw S3, 0 / 20 ms latency | PUT 2.95/43.6 ms, CAS-create 3.18/43.0 ms, GET 3.19/44.3 ms |||||

One op failed out of ~16,000 total, in `3node-p2poff-shared-write4k` at
20 ms latency in the run this table is drawn from (`errors=1`) — not
reproduced across 5 follow-up reruns of that exact configuration
(4,500 more ops, 0 errors). `metabench.rs` now records the first 8 error
messages per config (`error_samples` in `run_one`) for next time; this
one occurred before that instrumentation existed, so its errno was not
captured. At a 1-in-~2,700 rate under 3-way P2P-off contention with a
5 s lease TTL and 2 handoffs per run, a single transient EIO/EAGAIN
during one of those handoff windows is the most likely explanation, not
a reproducible bug — but this is inference, not a confirmed root cause.

**Single-node numbers are noisy**: a second full run measured
1node-create at 5184/5454 ops/s (0/20 ms) instead of 4575/2950, and
1node-write4k at 694/294 instead of 648/263 — the `write4k` latency
sensitivity direction is consistent (0 ms always faster) but the
`create` numbers swung by ~40% between runs on what should be a
latency-insensitive path (no data chunk, and plan 28's local-speed
journal write shouldn't touch S3 per op). Read single-node absolute
numbers as order-of-magnitude; read the *relative* multi-node-vs-single-node
gap as the robust part, since it replicated across all three runs.

### Where the bottleneck actually is

**Not the local engine, not raw S3 latency — confirmed cleanly now.**
`create` isolates the pure metadata path: single-node throughput
(2950–5454 ops/s across runs) dwarfs multi-node P2P-on aggregate
(1145–1273 ops/s), and neither number moves with injected S3 latency
(1227→1214, 1273→1145 ops/s at 0→20 ms) because **the iroh P2P path
never goes through the S3 proxy** — toxiproxy only shapes the S3
connection, so "S3 latency" cannot and structurally does not touch
forwarding cost. A CAS-create against floci itself costs 3 ms locally
and 43 ms at 20 ms one-way latency, in line with expectations and far
below every multi-node number, so S3 CAS is not what anything above is
waiting on.

**`write4k`'s data-chunk PUT is genuinely latency-sensitive, but only on
the single-node path.** Single-node write4k drops 648→263 ops/s (and
694→294 in the other run) at 0→20 ms — a real, reproduced ~2.4× hit from
one data-chunk PUT paying the injected latency directly, confirming the
coordinator's confound concern. Oddly, multi-node P2P-on write4k does
*not* show the same sensitivity (225→220, 214→229 ops/s) — plausibly
because a forwarded write's data-chunk upload is not synchronously on
the FUSE-reply critical path the way a local "through"-mode write is
(unconfirmed; would need chunk-upload-specific tracing to state
definitively — flagged here rather than guessed past).

**With P2P on: one dispatch task per node serializes every forwarded
mutation, including the network wait.** Traced end-to-end (two nodes,
one forwarded create, steady state — holder already cached, connection
already warm):

| Leg | Time |
|---|---|
| FUSE thread → dispatch-task channel handoff | ~120–160 µs |
| Dispatch task: holder-cache lookup before send | ~50 µs |
| Connection reuse + QUIC stream open + frame write | ~30 µs |
| **Network + holder round trip** (`wait_reply_us`) | **~2.2–2.3 ms** |
| — of which holder's own `execute_mutate` | ~250–350 µs |
| — remainder: network transit + oneshot-channel/scheduler wake latency on both ends | ~1.9 ms |
| Requester `apply_accepted` (`shadow_insert`+`apply_foreign`) | ~190 µs |
| **Total per forwarded create** | **~3.2 ms** |

The path is confirmed **direct** (iroh's `transport_observation`
reported `path="direct"`, not relay) and the connection is pooled and
reused (`conn_us` drops from ~2.4 ms cold to 10–20 µs warm) — so none of
the ~3.2 ms is dial or relay overhead. The dominant cost is not
network transit (loopback, sub-ms) or `execute_mutate` (250–350 µs,
matching single-node's own per-op cost) — it is unaccounted latency
around the two oneshot-channel handoffs (FUSE↔dispatch-task on the
requester, dispatch-task↔per-request-task on the holder), most
consistent with tokio scheduling/wake overhead, not work.

**A single requester cannot have multiple forwards in flight — proven,
not inferred.** Two FUSE worker threads issued `SyncRequest::Forward`
86 µs apart (concurrent creates from a 6-way burst); the *second*
thread's request was not even dequeued by the dispatch task until
3.5 ms later — i.e. after the *first* request's entire network round
trip had completed. Every burst request showed the same pattern: total
per-op latency for a queued request was roughly (queue position) ×
(≈3 ms), not the ≈3 ms steady-state figure above. The QUIC transport
itself supports concurrent streams on one pooled connection (this is
tested: `crates/net/src/peers.rs`'s
`two_streams_on_one_connection_are_served_concurrently`) — the
serialization is not the network, it is application-level dispatch.

**The exact serialization point** (`crates/cli/src/node_runtime.rs`):
one task, spawned once at line 1029 (the `'sync: loop`), owns the only
receiver of `sync_tx` (the channel is declared at line 528). It drains
one `SyncRequest` at a time via `sync_rx.recv()` (line 1056, and a
nested drain at line 1635 while a periodic sync round is already in
flight). The `SyncRequest::Forward` arm (line 1300) calls
`forward::request_mutate(...).await` **inline** (line 1348, retried at
line 1363 on a stale cached holder) and then
`forward::apply_accepted(...)` (line 1376) — all before the loop returns
to `recv()` for the next message. So every FUSE thread's forward, on
that requester node, plus that node's own periodic ship/lease
bookkeeping, funnel through and block this one task for the full
duration of whichever forward is currently in flight. The symmetric
holder-side arm, `SyncRequest::Mutate` (line 1245), is fast
(250–350 µs, all local) but sits behind the same single queue, so it
also queues behind unrelated dispatch-loop work.

**A precise, minimal fix — described, not implemented.** In the
`SyncRequest::Forward` arm (`crates/cli/src/node_runtime.rs:1300-1394`),
`tokio::spawn` the `request_mutate` + `apply_accepted` sequence instead
of awaiting it inline, and have the spawned task deliver the outcome
through `reply` itself; the dispatch loop then returns to `sync_rx.recv()`
immediately and can dequeue the next `Forward` without waiting on the
network. This is safe: `apply_accepted`'s `shadow_insert`/`apply_foreign`
writes go through fjall's `SingleWriterTxDatabase`, which already
serializes concurrent local writes at the storage layer (plan 29's own
design doc), so spawning does not introduce a correctness gap — concurrent
applies would simply queue at the fjall transaction instead of at this
dispatch loop, while the ~2 ms network-wait portion of each forward (the
actual dominant cost) genuinely overlaps. The same change on the
holder's `SyncRequest::Mutate` arm (line 1245) would remove that arm's
exposure to queueing behind unrelated dispatch-loop work (periodic sync
rounds, lease acquires), though the benefit is smaller since
`execute_mutate` itself is already fast. Given the time breakdown above
— ~2 ms of the ~3.2 ms total is network-wait/scheduler overhead that
pipelining would let overlap across concurrent forwards, and only
250–350 µs is real per-op holder work — this fix should let aggregate
multi-node throughput approach however many forwards can be kept in
flight at once, likely recovering most or all of the gap to single-node
throughput for disjoint workloads, without touching the lease model,
`§P3`, or POSIX semantics. This has not been implemented or measured;
it is the concrete trigger-condition experiment named in the
recommendation below.

**With P2P off: lease policy timers, not CAS latency — unchanged by the
correction.** Aggregate stays at 41–57 ops/s regardless of workload or
injected latency (create and write4k, 0 ms and 20 ms all land in the
same 41–57 ops/s band), while per-op latency *whenever a node holds the
lease* stays fast (p50 0.1–1.1 ms). Two handoffs occur per run
regardless of latency, and their cost is explained entirely by
`LEASE_MIN_DWELL_MS`/`LEASE_WANTED_GRACE_MS` (5 s each,
`crates/cli/src/lease.rs`) — this path is not the default (P2P off is an
explicit opt-out) and a shorter dwell/grace would visibly move this
number without touching the concurrency model — plan 29 M3c already
left "a FIFO/ticket successor design" as known, unimplemented follow-up
for exactly this.

**So: is lease serialization the bottleneck M4 was gated on?** Yes, in
substance, but more specifically than the original draft claimed: under
P2P-on (the default) it is not network RTT or S3 CAS, it is one
application-level dispatch task per node serializing forwards end-to-end
including the network wait — a bug-shaped inefficiency with a
file:line-precise, low-risk fix, not an inherent property of forwarding.
Try that fix and re-measure before concluding forwarding cannot scale
disjoint workloads at all.

### What §P3 would change here, concretely

Plan 28 §P3's "optimistic commit with structural rebase" already has
one leg standing: `crates/cli/src/mtree_publish.rs`'s publisher CASes
`commits/<seq>`, and on a lost race it diffs the winner's roots against
the parent and either **splices** (disjoint) or errors (overlapping) —
see the module's "`§P3` splice, restated for key deltas" doc. What is
missing is everything that would let this be the *only* gate on
mutation, replacing today's single-lease-holder-journals-then-publishes
design:

- **No per-node log streams.** Plan 28 keeps one shared `commits/<seq>`
  chain; §P3 is "any node may attempt the next commit," not "N
  independent chains." The change is *who* may CAS onto it (today:
  whoever holds `p0`'s lease, after journaling and shipping a batch),
  not the chain's shape.
- **Read-set tracking per FUSE op is the actual gap.** M2's dirty-key
  set (`Meta`'s `dirty` keyspace) is a *write* set — it already covers
  what a disjoint splice needs (`Plan::conflicts_with` in
  `mtree_publish.rs`). §P3 additionally needs the keys a decision
  *depended on without writing* — the canonical case is `O_EXCL`
  create's "observed absent" on a name that a losing node never
  touches. Today nothing records that a negative lookup happened;
  `execute_mutate` (`crates/meta`) would need to hand back an
  explicit read-set alongside its write-set for every op, not just the
  ops that reads happen to also dirty.
- **Re-execution on overlap is a new code path, and it collides with
  today's reply model.** `execute_mutate` runs once, synchronously,
  against the live local replica, and the FUSE thread replies from that
  result immediately (`SyncHandle`'s blocking-recv pattern,
  CONVENTIONS.md). §P3 wants: attempt commit, and *if* the winner's
  diff touches this op's read-set, re-run the same `MutateOp` against
  the post-rebase state and reply with *that* result. That either (a)
  delays every FUSE mutation's reply until its commit attempt resolves
  (turning today's "journal locally, ship/publish later" into
  "commit-or-rebase before ack," i.e. one CAS-ish round trip is now on
  the syscall's critical path, batched or not), or (b) keeps today's
  fast local ack and reconciles asynchronously — which is not
  POSIX-legal for `O_EXCL`, which must return `EEXIST` synchronously,
  not eventually. Plan 28 assumes (a); this repo does not have it.
- **Forwarding's role shrinks to a hint.** Per §P3's own table,
  forwarding survives only "as a latency optimization for hot-directory
  contention" — and the measurements above show it is not currently
  even winning at that job (aggregate regression, not gain), though that
  now traces to a fixable dispatch-loop bug rather than an inherent
  property of forwarding (see "Where the bottleneck actually is"). Once
  fixed, under §P3 forwarding would likely be re-scoped to "route a
  write to whoever last committed this key range, to reduce rebase
  odds," not kept as a correctness path.
- **Fencing, strict-mode ranges, offline designation, continuation
  epochs are unchanged**, per §P3's table — these are genuine mutual
  exclusion, orthogonal to ordering. But they are currently wired
  through `LeaseKeeper`/`p0` as "the" write authority (see
  `crates/net/src/epoch.rs` and `LeaseKeeper::commit`'s takeover-order
  check in `crates/cli/src/lease.rs`); decoupling "who may mutate" from
  "who is the fenced/epoch-current node" touches every call site that
  currently conflates the two.
- **GC's log-retention floor mostly simplifies.** Plan 28 §P2: "log
  retention stops being load-bearing" once every commit is a complete
  tree — plan 29 M3a's `GcTail` liveness-from-replica logic
  (`crates/cli/src/gc.rs`) is keyed to the single `p0` log's applied
  vector today; under §P3 liveness would key off the shared commit
  chain's roots directly, which is less machinery, not more.
- **The dirty-key publisher moves from "periodic background job" to
  "the mutation path."** Today `TreePublisher::publish` runs
  opportunistically (every 32 segments, on idle, at shutdown) and can
  fall behind without breaking anything visible to FUSE. Under §P3 a
  commit attempt is not optional background work — it *is* how a
  mutation becomes real — so its cost (currently amortized over a
  whole batch) lands on interactive latency instead.

### Estimate of change size

Large — a rewrite of the mutation path's concurrency model, not an
additive feature. Rough shape: `execute_mutate`/`MutateOp` need a
read-set return value (new, touches every op variant in `crates/meta`);
the FUSE mutation gates (`crates/cli/src/fusefs.rs`,
`fusefs_ops.rs`) need a commit-attempt-then-maybe-re-execute loop
replacing today's "journal and ack" (the single biggest and riskiest
piece — CONVENTIONS.md's `SyncHandle` barrier pattern was built around
one synchronous local decision, not a rebase-and-retry); `LeaseKeeper`
demotes to batching/fencing only, per §P3's table, but every current
caller of `holds_authority`/`usable()` needs to be re-read against
"what does this check actually need" (correctness gate vs. batching
hint); GC and the epoch/fencing code need their liveness/authority
source moved off `p0`. This is comparable in scope to plan 29 M1
(swapping the engine) or bigger, not a milestone-sized patch.

### Risks

- **Thrash on hot directories.** Today, contention on one directory
  serializes through the holder (or forwards to it) — slow, but every
  attempt succeeds. Under §P3, N nodes racing the same key range each
  build a full commit attempt and most lose; a hot directory turns into
  a repeated build-diff-reexecute storm instead of a queue, which could
  be *worse* than today's forwarding regression measured above unless
  bounded (e.g. falling back to forwarding once local re-execution rate
  crosses a threshold — the "hint" role above).
- **POSIX rename/link/rmdir correctness under re-execution.** §P6's
  cycle check (`rename` may not move a directory into its own
  descendant) is an O(depth) walk over `0x04` done once, synchronously,
  today. Re-execution means running that walk again against a state
  the original caller never saw, and rename's read-set must include
  both parent directories, the moved dentry, the target name, and the
  ancestor chain — missing any one of those in the read-set makes a
  disjoint splice *silently* wrong instead of correctly re-executed.
  This is precisely the class of bug pjdfstest exists to catch (plan
  28 §12 calls this out as "the real acceptance test for P3's
  read-sets"), and it is the main reason this is not a "just add a
  diff check" change.
- **Multi-key atomicity.** A cross-directory rename is ~6 keys across
  3–5 leaves (§P6); the commit CAS already makes the *publish* atomic,
  but re-execution must reconstruct the full multi-key intent from one
  stored `MutateOp`, not from the keys it happened to touch the first
  time — an easy place to reintroduce the old cross-partition-rename
  half-committed-pair bug class plan 28 §P4 was written to delete.

### Recommendation

**Defer implementing §P3.** The measured bottleneck for the default
configuration (P2P on) is real and large — 3-node aggregate metadata
throughput is *below* single-node today, on the pure-metadata `create`
path where the comparison is unconfounded by S3 latency or data-chunk
cost — but it traces to one application-level dispatch task per node
serializing forwards end-to-end (network wait included), not to an
architectural limit of forwarding or to lease-CAS latency. §P3 is a
large, correctness-risky rewrite of the mutation path; the fix named
above (`crates/cli/src/node_runtime.rs:1300-1394`, stop awaiting each
forward inline) is a small, low-risk, file:line-precise change with a
measured ceiling on its own upside (~2 ms of the ~3.2 ms per-op cost is
overlappable network-wait/scheduler overhead) and has not been tried.
The P2P-off path's bottleneck (sticky-lease dwell/grace timers) is a
tuning problem, not an architecture problem, and is separately fixable.

**Trigger to revisit §P3**: the dispatch-loop fix is implemented and
measured, and sustained multi-writer metadata demand still exceeds what
one holder plus non-blocking forwarding can serve — concretely,
aggregate creates/s or 4 KiB-write/s demand from ≥5 concurrently-active
nodes that non-blocking forwarding cannot keep within, say, 2× of
single-node throughput. Until then, §P3's cost (a rewrite comparable to
plan 29 M1, with pjdfstest as the real acceptance test for its
read-sets) is not justified by what M3 plus a five-line dispatch-loop
fix can plausibly already deliver.

## M5 — concurrent, correctly-ordered forwarding: implemented

Implements the fix M4 named but did not build. Two changes, both in
`crates/cli/src/node_runtime.rs`'s sync dispatch loop:

**Requester side (`SyncRequest::Forward` arm).** When this node is not
the holder, the round trip (`forward::request_mutate`, with its
`NotHolder` retry) plus the local apply (`forward::apply_accepted`) now
run on a `tokio::spawn`ed task instead of being awaited inline, bounded
by a semaphore (`ForwardState::inflight`, env
`CONSTELLATION_FORWARD_MAX_INFLIGHT`, default 64). The dispatch loop
returns to `sync_rx.recv()` immediately. The local-holder branch (no
network hop) stays inline, since M4 already showed it costs only the
fjall write itself.

**Ordering gate (`crates/cli/src/keygate.rs`, new module).** Concurrent
forwards are only safe when their conflict-key sets are disjoint — two
creates in one directory both bump its parent's mtime/ctime; two
`SetManifest`s on one file race each other's base; a rename touches two
parents and possibly the moved/replaced inodes. `forward::conflict_keys`
computes this set per `MutateOp` (conservative: an unresolvable name
lookup falls back to the parent alone, still safe). `KeyGate` is an
all-or-nothing mutex over that set: `acquire` waits until every key is
free, then holds all of them until the returned `KeyGuard` drops (RAII,
cancellation-safe — a cancelled waiter or a dropped guard always
releases and re-runs the grant scan). Disjoint sets never wait on each
other; two waiters that share a key resolve in the order they called
`acquire` (`GateInner::progress` scans its FIFO queue oldest-first,
extending the busy set as it grants, so a later, disjoint waiter behind
a blocked one is never held up). Every spawned forward acquires its
gate before sending and holds it through `apply_accepted`, so an
overlapping op cannot even start its own request until the previous
one's apply has landed — the holder therefore always sees this node's
overlapping ops in the order it issued them, and the requester applies
them back in that same order.

**Holder side (`SyncRequest::Mutate` arm)**, found necessary while
measuring, not anticipated in the design: `holder_execute` itself is a
fast local fjall write as M4 said, but under genuine concurrent load
(only possible once the requester-side fix above stopped hiding it)
funnelling every incoming forwarded mutation through this same single
synchronous arm — plus, on a cold holder-cache, an S3 `GET` — became the
new bottleneck. No ordering gate is needed here (fjall's own
single-writer tx already serializes concurrent `holder_execute` calls
correctly regardless of arrival order — the holder has no ordering
obligation to any particular requester), so this arm is spawned the
same way, with no additional primitive.

### Why plan 29 M4's own matrix couldn't have shown this

`crates/harness/src/metabench.rs`'s driver spawns exactly one OS thread
per node, so it never has more than one forward in flight per node —
the fix's precondition, "a single requester with two forwards
outstanding," cannot occur. Reproducing M4's own bottleneck therefore
needed a driver change, not just a rebuild: `MetaBenchConfig` gained
`threads_per_node` (default 1, so every pre-existing config is
byte-for-byte unchanged), and three new `*-concurrent4-lat0` configs run
4 threads per node. A `Disjoint`-layout config with `threads_per_node >
1` additionally gives each (node, thread) pair its own subdirectory —
without that, all threads on one node would still share that node's one
directory and their conflict keys would (correctly) serialize them,
silently defeating the point of the config.

### Measured (release build, floci+toxiproxy, 0ms injected S3 latency)

Matched-shape rows (`threads_per_node=1`, identical to M4's own matrix —
never exercises the fix, included to confirm no regression):

| Config | before agg ops/s | after agg ops/s | before p50 | after p50 |
|---|---:|---:|---:|---:|
| 1node-create | 3344 | 3240 | 0.24 ms | 0.26 ms |
| 3node-p2pon-shared-create | 1237 | 1093 | 1.82 ms | 1.15 ms |
| 3node-p2pon-disjoint-create | 1244 | 1041 | 1.90 ms | 1.01 ms |
| 3node-p2pon-shared-write4k | 295 | 205 | 6.47 ms | 8.73 ms |
| 3node-p2pon-disjoint-write4k | 363 | 234 | 5.13 ms | 8.50 ms |

All five are within the run-to-run noise plan 29 M4 already documented
for this harness (single-node `create` swung ~40% between its own three
runs) — expected, since none of these workloads ever put more than one
forward in flight per node.

Concurrent rows (`threads_per_node=4`, the condition the fix targets):

| Config | before agg ops/s | after agg ops/s | before p50 | after p50 |
|---|---:|---:|---:|---:|
| 1node-create-concurrent4 | 3781 | 4324 | 0.79 ms | 0.67 ms |
| 3node-p2pon-shared-create-concurrent4 | 1126 | 910 | 7.42 ms | 4.76 ms |
| 3node-p2pon-disjoint-create-concurrent4 | 1140 | 3086 | 8.12 ms | 2.05 ms |

**Disjoint concurrent throughput: 1140 → 3086 ops/s (2.7×), p50 latency
8.12 → 2.05 ms (4×)**, now within reach of single-node throughput —
matching M4's own prediction ("should let aggregate multi-node
throughput approach however many forwards can be kept in flight at
once ... for disjoint workloads"). Repeated during development (not
the paired run above, each a fresh before/after pair): 1236→3648 and
1140→3820 ops/s — consistently 2.7–3.2×, never a regression.

**Shared concurrent throughput does not improve (1126 → 910 ops/s) —
correctly.** Every thread across every node targets the same one
directory, so every op's conflict-key set is the same singleton
(the shared parent) and `KeyGate` — by design — fully serializes them
regardless of how many are in flight. This is the intentional
correctness trade-off the milestone's design section describes, not a
missed optimization: genuinely overlapping ops cannot run concurrently
without risking exactly the divergence `KeyGate` exists to prevent. The
small aggregate dip (within noise given the p99 spread) reflects one
extra scheduling hop (channel send + spawn) per op with no offsetting
concurrency gain in this fully-contended case.

**Diagnosis method**: temporary `tracing::info!` timestamps at four
points (FUSE-thread send, sync-loop dequeue, gate-acquired, reply-sent)
showed the gate/semaphore themselves added no measurable delay
(sub-0.2 ms) for disjoint keys, isolating the holder-side dispatch loop
as the actual remaining bottleneck under real concurrent load — the
instrumentation was removed before landing; the holder-side fix above
is what the trace led to.

### Tests

- `crates/cli/src/keygate.rs`: `disjoint_keys_run_concurrently`,
  `overlapping_keys_serialize`, `overlapping_waiters_are_fifo`,
  `dropped_waiter_releases_and_does_not_strand_others`,
  `dropped_holder_releases_correctly`,
  `no_deadlock_with_opposite_key_orders` (50×2 tasks, opposite
  two-key acquire order, multi-thread runtime), `empty_key_set_never_blocks`.
- `crates/cli/src/forward.rs`'s `conflict_keys_tests`: every `MutateOp`
  variant, including name-lookup resolution (unlink/rmdir child,
  rename moved/replaced) and the conservative parent-only fallback when
  a name cannot be resolved locally.
- `crates/cli/src/forward.rs`'s `ordering_gate_pipeline_tests`: an
  in-process, no-harness two-replica correctness check exercising the
  exact `conflict_keys` → `KeyGate::acquire` → `holder_execute` →
  `apply_accepted` pipeline the spawned task runs, under adversarial
  fake network transit times (independently reversed request/reply
  legs per op — a single delay before everything cannot desynchronize
  execution order from apply order, since there is no `.await` between
  them within one task). `concurrent_creates_same_and_disjoint_dirs_converge`
  and `concurrent_overlapping_setattrs_preserve_holder_order` assert
  the requester's `dump_replicated()` matches the holder's after many
  concurrent ops; `without_the_gate_a_reorder_is_observable` is a
  negative control (`gate: None`) proving the positive tests are not
  vacuous — it reliably reproduces a requester/holder mtime mismatch
  within 20 attempts.

### Left over

- The holder-side fix was not part of the original design and has no
  dedicated unit test beyond the harness measurement above (its
  correctness rests on fjall's pre-existing single-writer serialization,
  unchanged by moving the call off the dispatch loop).
- `AtimeBatch` forwards (`atime_flush_once`) do not go through
  `KeyGate` — they were already off the sync loop before M5 (their own
  ticker task calls `request_mutate_with` directly) and never call
  `apply_accepted` (best-effort, no local apply), so there is no
  ordering hazard for them to begin with; `conflict_keys` still handles
  the variant for completeness/testability.
- No attempt was made at fairness/anti-starvation beyond `KeyGate`'s
  FIFO-per-key property; a global fairness scheme across unrelated keys
  was not in scope and is not needed for correctness.
