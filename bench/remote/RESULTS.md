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

| nodes | shared dir | disjoint dirs |
|---|---|---|
| 1 | 2400 ok, 0 err, **142 ops/s**, p99 74ms | 2400 ok, 0 err, **665 ops/s**, p99 22ms |
| 4 | 2400 ok, 0 err, **216 ops/s**, p99 90ms | 1200 ok (see anomaly #1), 0 err, 7020 ops/s, p99 0.9ms |

Shared-directory contention costs ~3-4x throughput vs disjoint at both
scales, as expected (single-parent-inode metadata serialization).

## Row 2 — small file write (create + 4KiB write + close, 8 threads)

| nodes | dir | write-mode | ops/s | p99 (ms) | errors |
|---|---|---|---|---|---|
| 1 | shared | through | 20.4 | 596 | 4 (see anomaly #2) |
| 1 | disjoint | through | 270 | 65 | 0 |
| 4 | shared | through | 45.2 | 2584 | 19 |
| 4 | disjoint | through | 478 | 61 | 24 |
| 1 | shared | back | 5714 | 2.4 | 1 |
| 1 | disjoint | back | 5835 | 2.2 | 0 |
| 4 | shared | back | 5204 | 2.8 | 24 |
| 4 | disjoint | back | 5181 | 2.2 | 24 |

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

| nodes | write-mode | write MB/s (per node) | read MB/s (per node, cross-node cold) |
|---|---|---|---|
| 1 | through | 551.0 | 84.6 |
| 1 | back | 522.7 | 83.9 |
| 4 | through | 0 (node a, **EIO** — anomaly #2), 515-545 (b/c/d) | 56.8-83.7 (all 4 ok) |
| 4 | back | 0, 0, 0, 0 (**EIO on all 4**, anomaly #2) | 16.6-65.3 (all 4 ok, read the earlier through-mode files) |

Write throughput (500+ MB/s) is well above read (~60-85 MB/s); the local
write path is clearly buffering/pipelining ahead of S3 durability even
in `through` mode for large sequential writes, while cross-node cold
reads are bound by real S3 GET throughput per stream (single-threaded
here, so no read parallelism was exploited — a caveat, not a limit of
the system).

## Row 6 — cross-node visibility latency (node a writes, b/c/d poll)

210 fsync'd marker-file writes at 20/s, 3 pollers, 630 total samples:

| poller | p50 | p90 | p99 | max |
|---|---|---|---|---|
| b | 16.2s | 20.3s | 21.2s | 21.3s |
| c | 16.2s | 20.3s | 21.3s | 21.4s |
| d | 15.7s | 20.0s | 21.2s | 21.3s |

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
of 4 nodes succeeds; the other 3 raise **`ENOENT` (errno 2)** on the
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

Confirmed via `constellation status`: peer-to-peer path is `"path":
"direct"` (not relay) with sub-ms `rtt_ms`; the non-lease-holder node's
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
