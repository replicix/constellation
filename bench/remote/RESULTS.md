# Phase A real-world benchmark results

Run: `constellation-verify-20260922T154127Z-bench`, binary rev `a850019`
(`constellation --version` reports `01abaad-dirty` — matches the shipped
binary on all 4 hosts, consistent with `bin/REV`; not rebuilt).
Fleet: 4x EC2 (Ubuntu 24.04, 8 vCPU/20GiB, ~0.5ms RTT), plain FS
`bench-plain` at `s3://attila-test-.../constellation-verify-20260922T154127Z-bench`.
Raw JSON: `bench/remote/results/run1/*.json`. Driver: `phase_a.py` (see
`README.md` for how to re-run).

**Scope actually completed** (2.5h wall-clock budget): 2-node smoke test,
then rows **1, 2, 5, 6** of the spec's matrix at 1 and 4 nodes / 8
threads (1 thread for row 5, per spec). Rows **3, 4, 7, and the E2E
repeat** were **not run** — setup, protocol discovery (see README
"Pitfalls"), and chasing two real anomalies (below) consumed the budget
first. This is a partial result, not the full spec matrix.

## Row 1 — create empty files (8 threads)


| nodes | shared dir                              | disjoint dirs                                          |
| ----- | --------------------------------------- | ------------------------------------------------------ |
| 1     | 2400 ok, 0 err, **142 ops/s**, p99 74ms | 2400 ok, 0 err, **665 ops/s**, p99 22ms                |
| 4     | 2400 ok, 0 err, **216 ops/s**, p99 90ms | 1200 ok (see anomaly #1), 0 err, 7020 ops/s, p99 0.9ms |


Shared-directory contention costs ~3-4x throughput vs disjoint at both
scales, as expected (single-parent-inode metadata serialization).

## Row 2 — small file write (create + 4KiB write + close, 8 threads)


| nodes | dir      | write-mode | ops/s | p99 (ms) | errors             |
| ----- | -------- | ---------- | ----- | -------- | ------------------ |
| 1     | shared   | through    | 20.4  | 596      | 4 (see anomaly #2) |
| 1     | disjoint | through    | 270   | 65       | 0                  |
| 4     | shared   | through    | 45.2  | 2584     | 19                 |
| 4     | disjoint | through    | 478   | 61       | 24                 |
| 1     | shared   | back       | 5714  | 2.4      | 1                  |
| 1     | disjoint | back       | 5835  | 2.2      | 0                  |
| 4     | shared   | back       | 5204  | 2.8      | 24                 |
| 4     | disjoint | back       | 5181  | 2.2      | 24                 |


`back` (durable-local-queue, async ship) is **~20-100x faster** than
`through` (waits for S3 per chunk close), as expected. All non-zero
error counts here are the same "first write into a brand-new directory"
anomaly as row 1's — see anomaly #1/#2 below; steady-state (post-warm)
error rate was 0 in every row.

## Row 5 — 2 GiB sequential write/read, cold read cross-node

Scaled down to **200 MiB/thread** (not 2 GiB) to fit the time budget —
noted deviation, see README. 1 thread/node, 1 MiB blocks, fsync on
close for the write side; read side reads a *different* node's file
(rotated: a reads b's, b reads c's, etc.) after `status`-confirmed
quiesce, so it's a genuine cold cross-node/S3 read, not cache-warm.


| nodes | write-mode | write MB/s (per node)                             | read MB/s (per node, cross-node cold)                     |
| ----- | ---------- | ------------------------------------------------- | --------------------------------------------------------- |
| 1     | through    | 551.0                                             | 84.6                                                      |
| 1     | back       | 522.7                                             | 83.9                                                      |
| 4     | through    | 0 (node a, **EIO** — anomaly #2), 515-545 (b/c/d) | 56.8-83.7 (all 4 ok)                                      |
| 4     | back       | 0, 0, 0, 0 (**EIO on all 4**, anomaly #2)         | 16.6-65.3 (all 4 ok, read the earlier through-mode files) |


Write throughput (500+ MB/s) is well above read (~60-85 MB/s); the local
write path is clearly buffering/pipelining ahead of S3 durability even
in `through` mode for large sequential writes, while cross-node cold
reads are bound by real S3 GET throughput per stream (single-threaded
here, so no read parallelism was exploited — a caveat, not a limit of
the system).

## Row 6 — cross-node visibility latency (node a writes, b/c/d poll)

210 fsync'd marker-file writes at 20/s, 3 pollers, 630 total samples:


| poller | p50   | p90   | p99   | max   |
| ------ | ----- | ----- | ----- | ----- |
| b      | 16.2s | 20.3s | 21.2s | 21.3s |
| c      | 16.2s | 20.3s | 21.3s | 21.4s |
| d      | 15.7s | 20.0s | 21.2s | 21.3s |


0 timeouts (30s poll budget) — everything eventually became visible, but
**16-21 seconds is drastically higher than the ~1s** cross-node
visibility observed in the 2-node smoke test with an idle system. This
row ran immediately after row 5's 4x200MiB write burst; plausible
explanation is upload/ship backlog contention from that burst rather
than a fixed per-op cost, but `status` showed `journal_backlog: 0` and
`pending_uploads: 0` by the time we checked afterward, i.e. it had
already fully drained. **Not confirmed root-caused — flagged as an
anomaly, not fixed** (out of scope per the task).

## Anomalies / suspected bugs (evidence attached)



### 1. Concurrent first-`mkdir` of a brand-new shared directory: ENOENT for the losers

Reproducible: 4 nodes x 8 threads, each thread's very first op is
`os.makedirs(never-before-seen-dir/tN, exist_ok=True)` then a file
create inside it, all starting at the same wall-clock barrier. Only 1
of 4 nodes succeeds; the other 3 raise `ENOENT` **(errno 2)** on the
directory path itself, e.g.:

```
No such file or directory: '.../create-disjoint-race-repro-.../t4'
```

Repro script + full per-node error dump:
`bench/remote/results/run1_debug2/race_repro.json`. Correlated daemon
log line (node a, `bench-plain` log, same window):

```
metadata publish deferred: the winning commit overlaps this batch
  reason="conflict: commit 11 reflects log this replica has not applied (p0:18 against p0:15)"
```

(`bench/remote/results/anomaly_mkdir_race/log-a-bench-plain.txt`). This
reads as an optimistic-concurrency conflict on the metadata commit that
is surfaced to the FUSE caller as a hard `ENOENT` instead of being
retried transparently. **Effect is masked on a second attempt** because
`exist_ok=True` short-circuits once the directory exists — so this is
easy to miss in any test that doesn't use fully fresh directory names.

### 2. "Pending upload chunk ... missing from local cache" → EIO/ENOSPC cascade

Under concurrent first-writes into a fresh shared directory (row 2,
1 node, 8 threads, `write-mode through`) and separately under 4-node
concurrent large sequential writes into a shared directory (row 5, all
write-modes), writes started failing with **EIO (errno 5)** and, once
that happens, subsequent unrelated creates in the same directory start
failing with a clearly-spurious **ENOSPC (errno 28)** — spurious because
`df` showed 90GB free and `constellation status`'s quota was
`max_bytes: null, used_bytes: ~29MB`. Daemon log (node a):

```
ERROR constellation: pending upload chunks missing from local cache (unrecoverable content);
      leaving the pending rows and refusing to ship missing_pending_chunks=1 sample_hash=Some(ChunkHash(92e84bac2417))
WARN  constellation::fusefs: write-through upload failed error=pending upload chunk 92e84bac2417... missing from local cache ino=3298534921217
WARN  constellation::node_runtime: metadata sync failed; will retry error=pending upload chunk 92e84bac2417... missing from local cache
```

Full tail: `bench/remote/results/anomaly_write_through_missing_chunk/log-a-full-tail.txt`.
Row 5's 4-node write EIO: `bench/remote/results/run1/row5_seqwrite_bench-plain_through_n4_t1.json`
(node a: 200/200 write ops failed with errno 5) and
`row5_seqwrite_bench-plain_back_n4_t1.json` (all 4 nodes: 800/800
failed with errno 5). This looks like a real race where a chunk is
evicted from the local cache (or never staged) before the write-through
path can ship it, and the recovery path gives up rather than
re-encoding/re-staging — worth a deeper look at
`crates/cli/src/shipper.rs`'s pending-chunk handling under contention.
**Not fixed per task instructions** (measure and report only).

### 3. Harness gap: a fully-crashed worker is silently excluded from aggregates

`phase_a.py aggregate_ops()` skips `None` per-node results (fetch
failure or worker fatal error) rather than counting them as errors —
`bench/remote/results/run1/row1_create_empty_n4_t8_disjoint.json`
under-reports (1200 ok / 0 err, should show 3 nodes' worth of anomaly-1
failures) because that run predates the `run_threads_rec` exception-
guarding fix added mid-session (see `bench_worker.py`); it was not
rerun given time constraints. Anomaly #1's `race_repro.json` (same
session, after the fix) has the correct, fully-attributed error counts.
Phase B's driver should either fix this aggregation gap or treat any
`None` per-node result as a hard failure.

## P2P / forwarding (from the 2-node smoke test, `smoke.py`)

Confirmed via `constellation status`: peer-to-peer path is `"path": "direct"` (not relay) with sub-ms `rtt_ms`; the non-lease-holder node's
write showed `forwarded_ok: 2, forwarded_err: 0` after writing while the
other node held the lease — forwarding of non-holder mutations to the
lease holder works as designed, and cross-node visibility was ~1s on an
otherwise-idle system (contrast with row 6's 16-21s under load, above).

## Cleanup

All 4 hosts unmounted (`constellation umount bench-plain`, verified 0
running `constellation` processes and no stray FUSE mounts afterward).
S3 prefix `constellation-verify-20260922T154127Z-bench` (and the throw-
away smoke prefix `constellation-verify-20260922T153644Z`) were **left
in place** per instructions (Phase B may still need the hosts; prefix
deletion deferred, not performed).

## After plan 29 M6

M6 root-caused and fixed 2 of the 5 problems raised by this run; the
other 3 got a negative/narrowing result (see `PROGRESS.md`'s "Plan 29
M6" section for the full evidence trail). Summary, no full EC2 rerun of
this Phase A matrix was done (time budget; most rows received no
product change to re-measure):

- **Anomaly #1 (mkdir-race ENOENT)** — root-caused: plan 29 M5's
holder-side `tokio::spawn`-per-forward let a child's forwarded create
execute before its own brand-new parent's forwarded create, since
nothing orders two *different* nodes' forwards against each other.
Fixed with a bounded retry (`crates/cli/src/node_runtime.rs`,
`crates/cli/src/forward.rs`) on a transient `ENOENT` for create-family
ops. Verified with 3 dedicated in-process tests and a manually
reproduced 4-replica local `file://` mount (32 concurrent
`os.makedirs` into a brand-new shared directory, 0 errors after the
fix, across 3 repeats) — **not** re-run against real S3 on the EC2
fleet this session (see PROGRESS.md's Leftovers).
- **Anomaly #2 (missing-chunk EIO/ENOSPC)** — the local harness
scenario `writeback-backpressure`'s intermittent failure ("ENOSPC
arrived without observable throttling", ~2/5 runs) was root-caused
(a missing grace-sleep on the `DiskCache` dirty-budget's hard-limit
path, `crates/cli/src/fusefs_ops.rs::do_write`) and fixed — 10/10 and
then a further 5/5 clean runs after the fix, versus ~3/5 before. The
real-S3 "pending upload chunk ... missing from local cache" trigger
itself (this run's actual anomaly #2) was investigated at length but
not found; every insertion-then-eviction ordering this session could
think of was checked and is correct. A separate, real resilience bug
found while chasing it (one permanently-missing chunk anywhere on a
node blocks *every* inode's manifest from publishing, forever) is
pinned by a test but not fixed — see PROGRESS.md.
- **Row 1's shared-vs-disjoint create disparity** — ruled out a Constellation-internal per-directory lock via two local micro-benchmarks (metadata-engine-only and full-FUSE-with-near-zero- latency-backend, both ratio ~1.0). The disparity only appears under real S3 latency, which this session could not cheaply reproduce locally; leading hypothesis (unavoidable kernel per-directory VFS exclusivity amplifying an occasional S3-latency-bound stall) is documented but not confirmed or fixed.
- **Row 6's cross-node visibility latency** — the push/wake
architecture (gossip `SegmentPublished` -> immediate `SyncRequest`
wake, racing the idle-poll sleep) was read end-to-end and appears
correct; the 16-21s delay was not reproduced or root-caused this
session. Not fixed.
- **Row 5's cold-read throughput** — no gap: `crates/cli/src/prefetch.rs`
already implements adaptive parallel readahead (AIMD concurrency
ramp, growing window up to 2 GiB). The observed 56-85 MB/s is
consistent with a 200 MiB read finishing before the ramp reaches
steady state, not an absence of parallelism.

Gates run this session (local only): `cargo fmt`/`clippy -D warnings`/
`cargo test --workspace` all clean; `cargo build --release --workspace`;
`target/release/harness run writeback-backpressure` 15/15 clean runs
across two verification passes after the fix; the other 9 required
scenarios (`baseline kill9-remount two-clients-shared lease-handover chaos-ci create-storm-s3-only e2e-two-nodes idle-cluster-is-quiet fresh-node-bootstrap`) all PASSED; `bash tests/smoke.sh` PASSED.
## Plan 30 real-S3 results

The real-S3 verification of plan 30 (M16), 2026-09-25 to 2026-09-28.
The fleet was 4 × c5n.2xlarge in us-west-2 (AZs a, b, c, a), against
two backends: AWS S3 in the same region, and OVH Object Storage in Milan
(`s3.eu-south-mil.io.cloud.ovh.net`, one continent away from the
hosts). The drivers were ad hoc scripts built on `common.py` and
`bench_worker.py`, plus per-campaign workers left under `~/cbench/` on
the hosts. The full reports (`EC2-*-REPORT.md`, `OVH-BRUTAL-REPORT.md`,
`EC2-ROUND3-REPORT.md` in the `constellation-m14` worktree) are not in
the repo; this section keeps their numbers. Rows the reports themselves
flagged are marked **unreliable** or **proxy**, with the reason. What
each run found, and the commits that fixed it, is in plan 30
[§7.4](../../docs/plans/v1/done/30-write-path-resilience-and-scale-out.md#74-verification-record).

### Runs

| Run | Dates (UTC) | Binary | Backends |
|---|---|---|---|
| EC2 brutal | 2026-09-25 | `b4e7cbe-dirty` (M14 WIP on `2522402`) | AWS |
| OVH brutal | 2026-09-25 | the same binary | OVH, plus an AWS-vs-OVH perf phase |
| Round 3 | 2026-09-25 | `df14686`, A/B against `5384772` | both |
| Campaign 4 | 2026-09-25 | `edd3d5d` | AWS; OVH for the soak and part of Part A |
| Campaign 5 | 2026-09-25/26 | `5face1b`, `ee3f65b` | both (git under `flock` only) |
| Campaign 6 | 2026-09-26/27 | `216ce6c` (Parts A–C), `7a681ee` then `a432373` (Part D) | both |

The campaign 6 report names `a432373` for Part C, but Part C ran on
2026-09-26, before that commit existed; by date it ran `216ce6c`. `7a681ee`
changed only docs over `a1bed13`.
| Campaign 7 | 2026-09-27/28 | `2ff95df` (Parts A, B, D), `cb847f8` (Parts C, E) | both |
| Campaign 8 | 2026-09-28 | `5437fa6` | both; **in progress**, measurements pending |

### Failover time

`kill -9` of the lease holder, ten rounds, timed until another node
holds the lease. On this fleet every holder had a backup in budget, so
these are seal-based takeovers.

| Run | Binary | Backend | Result | Note |
|---|---|---|---|---|
| Campaign 4 | `edd3d5d` | AWS | p50 3.54 s, max 4.68 s (2.34–4.68 s), 10/10 with a backup | |
| Campaign 6 D1 | `7a681ee` | AWS | p50 3.90 s, mean 9.24 s; round 0 59.79 s, rounds 1–9 2.5–4.1 s | round 0 is the first failover on a fresh filesystem; unexplained |
| Campaign 6 D1 | `7a681ee` | OVH | round 0 59.82 s, round 1 6.0 s | **unreliable**: 8 of 10 rounds recorded "no holder found" (driver bug) |
| Campaign 7 E1 | `cb847f8` | both | nominal p50 2.66 s, p99 2.79 s from 5 AWS rounds | **unreliable**: the driver waited for one fixed survivor to become holder and misread fast remounts as failures |
| Campaign 8 E | `5437fa6` | both | pending | corrected driver |

Related availability measurements:

| Case | Run, binary | AWS | OVH |
|---|---|---|---|
| One node's P2P cut off; a write on the majority side | EC2/OVH brutal, `b4e7cbe-dirty` | 36.6 s (isolate one), 24.7 s (2+2) | 0.77 s isolating the holder; 21.5 s on the non-holder side of a 2+2 |
| | Campaign 7, `2ff95df` | 0.43 s | 0.61 s |
| | Campaign 8, `5437fa6` | 0.46 s | 0.86 s |
| S3 cut on one node; create+write+`fsync`+close on it | EC2/OVH brutal, `b4e7cbe-dirty` | blocked for the outage (75–90 s) | blocked for the outage (85 s+) |
| | Campaign 7, `2ff95df` | 6.1 s | 6.7 s |
| | Campaign 8, `5437fa6` | 6.46 s | 120 s, `EIO` (open finding A-1) |
| `kill -9` + remount, 20 rounds | Campaigns 7 (`2ff95df`) and 8 (`5437fa6`) | 20/20, no zombies; ~7.5 s per round in campaign 8 | the same |

The brutal runs' majority-side stall was not a failover: an isolated
node's inbox escalation made the healthy holder release its lease
(fixed in `71dc7e7`). The S3-cut stall was a close uploading inline; a
node with no S3 now hands its chunks to a peer (`71dc7e7`).

### Visibility latency

A writer on one node writes and closes (with `fsync`) small files at a
fixed pace. Pollers on other nodes run concurrently and record when
each file first reads back with its content. Latency is measured from
the writer's write, so on OVH it includes the writer's own close (one
~0.2 s PUT under `through`).

| Run | Binary | Events | AWS p50 / p99 | OVH p50 / p99 | Note |
|---|---|---|---|---|---|
| Phase A row 6 | `a850019` | 630 | 16.2 s / 21.2 s | — | **unreliable**: the pollers started only after the writer finished (plan 30 M7) |
| OVH brutal | `b4e7cbe-dirty` | 150 overwrites of one file | — | ≈ 205 ms / max ≈ 275 ms | |
| Round 3 | `df14686` | `bench_worker` visibility row, 4 nodes | 5.6–6.0 s / 6.4–6.6 s | — | **not comparable**: the row's method; base `5384772` gave the same |
| Campaign 4 | `edd3d5d` | 200 | 28.4 ms / 62.7 ms (max 237.7) | — | |
| Campaign 6 D2 | `7a681ee` | 500 | 87.9 ms / 124.0 ms | 95.6 s / 160.1 s | OVH **unreliable**: its poller ran on the node wedged by finding B-1 and started ~160 s late |
| OVH visibility fix | `a432373` → fix of `7dfc05b` | 300 each way, 3 nodes | — | 0.41–0.46 s / 0.45–0.56 s → 0.217–0.220 s / 0.29–0.31 s | open-to-read; the fix removed an S3 GET per read |
| Campaign 7 Part A | `2ff95df` | 500 | 33.4 ms / 56.0 ms | 210 ms / 298 ms | 0 timeouts |
| Campaign 7 E2 | `cb847f8` | 200 | writer 29.6 / 76.3 ms; poller 59.0 / 113.9 ms | writer 209.7 / 347.8 ms; poller 272.5 / 668.9 ms | 0 timeouts |
| Campaign 8 Part A | `5437fa6` | 500 | 33.6 ms / 53.0 ms | 216 ms / 247 ms | 0 timeouts |

### S3 requests per operation

Campaign 7 E3 (`cb847f8`): requests per 100 operations, as the report
gives them (GET / HEAD / PUT / LIST).

| Operation | AWS | OVH |
|---|---|---|
| create | 45 / 6 / 111 / 13 | 45 / 6 / 110 / 14 |
| write | 57 / 8 / 115 / 13 | 56 / 6 / 114 / 11 |
| read | 9 / 0 / 2 / 13 | 21 / 4 / 6 / 12 |
| stat | 4 / 0 / 1 / 12 | 5 / 0 / 1 / 13 |
| readdir | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| rename | 45 / 6 / 92 / 13 | 45 / 6 / 90 / 12 |
| unlink | 45 / 6 / 93 / 12 | 42 / 4 / 90 / 14 |

Earlier runs (EC2 brutal R2-2,
campaigns 4 and 6) had no request counters and reported
`spool.ship_rounds_completed` deltas instead: **proxy**, not requests.
The local harness (`small-file-write-path`) counts one S3 request per
close of a small file with new content, on the sequencer and on other
nodes (since `4798008`). Delegation left the request count per file
unchanged (one host, plan 30 M11).

### Idle cost

Four mounted, idle nodes. S3 requests per minute per node.

| Run | Binary | Result | Note |
|---|---|---|---|
| Harness `idle-cost` | before `71dc7e7` | holder 237, followers 144 | local; registry GETs for every record every 5 s |
| Harness `idle-cost` | `71dc7e7` | holder 27, followers 29.5 | local |
| Campaign 7 E4, 15 min | `cb847f8` | AWS 26.3–28.1; OVH 25.7–27.7 | |
| EC2 brutal R2-2, 616 s | `b4e7cbe-dirty` | 5.4–6.0 ship rounds and ~1.5 reconcile rounds per minute | **proxy** |
| Campaign 4, 300 s | `edd3d5d` | 6.2–6.6 ship rounds per minute | **proxy** |
| Campaign 6 D4, 15 min | `7a681ee`/`a432373` | AWS 6.0–39.3, node b 185.5; OVH 8.3–9.5, node b 233.7 ship rounds per minute | **proxy**; node b's outlier unexplained (it had the wedged daemon earlier in that run) |

What remains at idle: the 5 s registry LIST, the 10 s designation
LIST, the log-stream backstop or the holder's staleness probe, the
commit-chain poll and the lease renewals.

### Close-to-open cost (`--cto bounded` vs `strict`)

| Run | Binary | Workload | `bounded` | `strict` |
|---|---|---|---|---|
| Campaign 4 | `edd3d5d`, AWS | 100 files created on A, then `stat`+`open`+`read` on B: cold, then warm | cold p50 27.4 ms, p99 44.1 ms; warm p50 0.15 ms, p99 0.41 ms | cold p50 29.5 ms, p99 76.8 ms; warm p50 0.39 ms, p99 0.63 ms |
| Campaign 4 | `edd3d5d` | close-to-open probe: write+close on A, then open+read on B | — | 0 stale in 1000 iterations (AWS) and 300 (OVH); p50 821 ms (AWS) and 1144 ms (OVH) per iteration, including the driver's round trips |
| Campaign 6 D5 | `7a681ee`, both | 1000 warm `stat`s on one node | p50 0.006 ms; p99 0.47 ms (AWS), 0.53 ms (OVH) | p50 0.006–0.007 ms; p99 0.63 ms (AWS), 0.37 ms (OVH) |
| Campaign 7 E5 | `cb847f8`, both | 30 warm `stat`s | 1.17 ms (AWS), 1.07 ms (OVH) | 10.15 ms (AWS), 10.19 ms (OVH) |
| Campaign 8 E | `5437fa6` | | pending | pending |

Campaign 6's run was on a lone node, which is its own sequencer, so
`strict` added nothing. Campaign 7's report gives each figure for the
30 calls and reads them as about 0.3 ms more per call under `strict`
(0.04 against 0.34 ms). That is the ~9× behind keeping `bounded` as the
default
([ADR-30](../../docs/explanation/DECISIONS.md#adr-30---cto-bounded-stays-the-default)).

### Untar: default (`through`) vs `--write-mode back`

| Run | Binary | Tree | AWS | OVH |
|---|---|---|---|---|
| OVH brutal | `b4e7cbe-dirty` | `/usr/share/doc`, 4,557 entries, default | — | 143 files in 90 s (0.63 s/file) |
| Round 3 | `df14686` | `/usr/share/doc`, ~4,560 files, default, fresh idle mount | 0.040–0.050 s/file | 0.403 s/file on the sequencer, 0.745 s/file on another node |
| Round 3 | `df14686` | 50 × 4 KiB files, default | 1.5 ms/file with identical content, 60.1 ms/file unique | — |
| Campaign 6 D6 | `7a681ee`/`a432373` | linux headers, 17,559 files, default | 16,842 in the 600 s cap (~28 files/s), 10,452 ship rounds | 2,937 in the 600 s cap (~4.9 files/s), 167 ship rounds |
| Campaign 6 D6 | the same | the same, `back` | all in 21.7 s (810 files/s), 5 ship rounds | all in 140.0 s (125 files/s), 8 ship rounds |
| Campaign 7 E6 | `cb847f8` | the same, default | done in ~10 min 13 s (~29 files/s; includes `fs create` and mount) | 6,504 files in 1200 s, not finished (~5.4 files/s) |
| Campaign 7 E6 | `cb847f8` | the same, `back` | copy done; the `umount` drain passed the driver's 60 s | not reached |
| Campaign 8 E | `5437fa6` | the same, both modes | pending | pending |

Under `through` (the default), each file with new content pays its
chunk's S3 PUT in `close()`. Since `4798008` that is one request per
small file (it was two or three). A single-threaded extractor therefore
runs at about one PUT latency per file: ~35 ms on AWS, ~0.2 s on OVH.
`back` acknowledges from a durable local queue and uploads in parallel.
The OVH brutal and round 3 figures predate `edd3d5d` and `4798008`, when
a non-owner's close also waited for the log.

### AWS vs OVH

From the OVH brutal perf phase (`b4e7cbe-dirty`, node a, both backends
mounted at once and run back to back) unless noted.

Raw S3 requests through the `aws` CLI, 20 per kind. The CLI's own
startup (~730 ms) dominates the absolute numbers, so the difference is
the signal:

| Request | AWS p50 | OVH p50 | OVH − AWS |
|---|---|---|---|
| PUT | 767.7 ms | 1416.3 ms | +648.6 ms |
| GET | 747.6 ms | 1237.9 ms | +490.3 ms |
| HEAD | 744.9 ms | 1227.7 ms | +482.8 ms |
| LIST | 754.4 ms | 1240.5 ms | +486.1 ms |
| PUT `If-None-Match: *` | 764.9 ms | 1429.1 ms | +664.2 ms |

A 1 GiB `aws s3 cp` uploaded at 524.9 MiB/s to AWS and 70.0 MiB/s to
OVH, and downloaded at 289.1 and ~161 MiB/s.

| Workload (FUSE) | AWS p50 / p99 | OVH p50 / p99 |
|---|---|---|
| `create_empty`, 1 node | 0.2 / 0.2 ms | 0.2 / 0.2 ms |
| `small_write`, no `fsync`, 1 node | 0.4 / 54.1 ms | 0.4 / 559.3 ms |
| `stat_hot` | 0.0 / 0.1 ms | 0.0 / 0.1 ms |
| `readdir` | 0.1 / 0.3 ms | 0.1 / 0.3 ms |
| `rename_unlink` | 0.1 / 0.2 ms | 0.1 / 0.2 ms |
| 4 KiB write + `fsync`, default policy | 53.3 ms | 389.9 ms |
| 4 KiB write + `fsync`, `ack=s3`, `through` | 64.8 ms | 391.9 ms |
| Shared-directory create+write+close, sequencer node | 3.0 ms (round 3: 3.4 ms) | 2.2 ms (round 3: 2.4 ms) |
| Shared-directory create+write+close, other nodes | 53.0 / 82.6 ms (round 3: 49.5 / 488.7–516.5 ms) | 401.7 / 565–571 ms (round 3: 401.5–403.0 / 2127.7–2327.6 ms) |

The other-node shared-directory cost (one S3 round trip, waiting for
the log behind the node's own unshipped row) was fixed in `edd3d5d`:
the pre-S3 stream now answers the wait (5.9–14 ms at 100 ms per S3
request in the harness). It has not been re-measured on EC2.

Round 3 (`df14686`) also measured:

- **Dedup.** 500 identical 1 MiB files from 4 nodes added exactly
  1,048,592 bytes of chunks on both backends.
- **Large file.** A 4 GiB file took 10.1 s on AWS and 5.8 s on OVH,
  with the same sha256 on all 4 nodes. **Inflated**: `bench_worker`'s
  `seq_write` repeats one 1 MiB block, so the file dedups to one chunk.
  Campaign 4's 10 GiB of unique data wrote at ~120 MB/s (85.5 s), and
  the cross-node read and verify took 100.1 s (AWS, `edd3d5d`).

### Phase A rows, M14 A/B (round 3, AWS, `df14686` vs `5384772`)

Three interleaved repetitions of each row. p50 is the mean of the
per-node p50s, p99 the maximum of the per-node p99s.

| Row | Nodes | `df14686` p50 | `df14686` p99 | Verdict against `5384772` |
|---|---|---|---|---|
| `create_empty` | 1 | 0.40–0.43 ms | 0.65–0.81 ms | no difference |
| `create_empty` | 4 (really 3; see below) | 10.6–11.6 ms | 22.1–31.7 ms | no difference |
| `small_write` | 1 | 0.79–0.80 ms | 54–72 ms | no difference |
| `small_write` | 4 (really 3) | 42.7–57.3 ms | 474–1071 ms | noisy, no consistent regression |
| `stat_hot` | 1, 4 | ~0.03–0.05 ms | ~0.1–0.2 ms | no difference |
| `readdir` | 1 | 0.33 ms | 0.51–0.63 ms | no difference |
| `readdir` | 4 | 8.4–8.7 ms | 20.8–25.7 ms | p50 6–12% faster |
| `rename_unlink` | 4 | — | 156.2–184.3 ms | p99 +9–17% in all three repetitions (open, not investigated) |
| `seq_write`, 1 GiB, `fsync` | 1, 4 | 0.80–0.88 ms | 2.42–2.80 ms | no difference (repeated-block content) |
| `seq_read`, 1 GiB | 1, 4 | 11.9–13.9 ms | 12.5–17.3 ms | no consistent difference |
| `untar`, 25 s cap | 1 | 574 files | — | no difference (~0.043 s/file) |
| `untar`, 25 s cap | 4 | 956 files | — | no difference |

The 4-node `create_empty` and `small_write` rows are 3-node data: node
a's files from the 1-node phase already existed, so its 4-node ops all
failed with `EEXIST`.

A `flock`-protected counter with 4 nodes × 4 threads ended at 1000 of
1000 on `df14686`, at a per-acquisition p50 of 559–1702 ms. On
`5384772`, whose locks were node-local, it ended at 451 of 1000 (p50
90–321 ms). On one node the cluster locks were faster once warm
(3.5–43.7 ms against 13.6–159.4 ms).

### Scale

| Run | Binary | Backend | Result |
|---|---|---|---|
| Campaign 4 | `edd3d5d` | AWS | 200,000 empty files from 4 nodes (348 s), identical on all nodes; a fresh node mounted them from S3 in 3 min 53 s. A 100,000-entry directory from 4 nodes (89.7 s), identical everywhere |
| Campaign 6 C2 | `216ce6c` | both | 199,998 creates from 3 nodes, 7 failed (not investigated); listings identical. Fresh mount from S3: 173.5 s on AWS; on OVH it had not finished at 600 s |
| Campaign 6 C3, C4 | `216ce6c` | both | 10 GiB file written on one node, identical sha256 on another; 100,000-entry directory on 2 nodes (one transient `EACCES` on AWS right after a concurrent first mount, not reproduced in 5 retries on `7a681ee`) |

### Not measured

- Delegation throughput on EC2 (plan 30 M11 measured it on one host
  only).
- Failover time on OVH, and a trustworthy failover distribution on the
  final binary: campaign 8 Part E was still running.
