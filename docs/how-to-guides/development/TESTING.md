# Testing

Constellation has seven test lanes, from fastest to most realistic. The
fast lanes run on every PR (`.github/workflows/ci.yml`); the full matrix,
xfstests, performance gate, audit, and macOS build run nightly and on manual
dispatch (`.github/workflows/nightly.yml`).

| Lane | Command | Backend | Needs | Speed |
|---|---|---|---|---|
| Unit tests | `cargo test --workspace` | in-memory / tempdir | Rust | seconds |
| Host smoke | `tests/smoke.sh` | local directory (`object_store` LocalFileSystem) | Rust, fuse3 | ~2 s |
| Host integration | `tests/integration.sh` | floci S3 (container) | + docker | ~10 s |
| Containerized | `tests/compose-test.sh` | floci S3 (container) | docker only | ~5 min cold |
| Fault injection | `cargo run -p constellation-harness -- run` | floci S3 via toxiproxy | Rust, fuse3, docker | ~3–6 min |
| xfstests | `make xfstests` | floci S3, separate test/scratch prefixes | docker | long |
| Performance | `make perf-gate` | local floci S3 | Rust, fuse3, docker | minutes |

The fault-injection lane includes **`chaos-ci`**: same-path conflict races
across three local mounts of one filesystem (create/mkdir/unlink/rename
storms, overlapping writes, close-to-open checks) via
[`constellation-chaos`](../../reference/tools/chaos.md). Run it alone with
`cargo run -p constellation-harness -- run chaos-ci --seed 42`. For
multi-node hour-long soaks, see
[Run a chaos soak](run-chaos-soak.md).

The containerized lane runs three suites (all by default, or pick:
`tests/compose-test.sh smoke stress`):

| Suite | Script | What it does |
|---|---|---|
| `smoke` | `tests/smoke.sh` | end-to-end functional walkthrough |
| `compliance` | `tests/compliance.sh` | full pjdfstest POSIX suite (~8800 checks) diffed against `tests/pjdfstest-baseline.txt` |
| `stress` | `tests/stress.sh` | fio sequential + random-write with data verification, stress-ng dentry/dir/rename/symlink/open churn, post-stress mount sanity check |

## POSIX compliance (pjdfstest)

`tests/compliance.sh` runs the complete
[pjdfstest](https://github.com/pjd/pjdfstest) suite against a mounted
constellation filesystem. Current state: **all 8798 checks pass and the
baseline file is empty** — any new failure is a regression and fails CI.

Mechanics worth knowing:

- TAP lines marked `# TODO` by pjdfstest (documented Linux/POSIX
  divergences, e.g. SUID clearing on directory chown) are expected
  failures, not regressions.
- `PJDFSTEST_CATEGORIES="rename,link"` restricts the run while
  iterating on a fix; `PJDFSTEST_DETAILS=all` prints every failing
  check's expectation vs. result.
- The baseline diff is two-way: unexpected failures fail the run, and
  baselined-but-now-passing entries are reported so the baseline can be
  tightened (it stays empty until a genuinely unfixable case appears).

## The smoke test

`tests/smoke.sh [backend-url]` is the single end-to-end script all
integration lanes share. It creates a filesystem, runs `doctor`, mounts
it over FUSE, and exercises: namespace ops (mkdir/rename/symlink),
multi-chunk files, partial in-place edits, truncate, append,
unlink-while-open orphan semantics, unmount/remount persistence, and
cold-cache reads after wiping the local chunk cache.

The backend URL decides where it runs:

```bash
tests/smoke.sh                          # local file backend in a tmpdir
tests/smoke.sh s3://bucket/prefix       # any S3 backend (AWS_* env applies)
```

## POSIX compliance: pjdfstest

`tests/compliance.sh` runs the full [pjdfstest](https://github.com/pjd/pjdfstest)
suite (~8800 checks: errno correctness for every namespace syscall,
sticky-bit and permission semantics, hard links, special nodes) against
a constellation mount and diffs failures against
`tests/pjdfstest-baseline.txt`:

- a failure **not** in the baseline → regression → the suite fails
- a baseline entry that now passes → improvement → warning to update
  the baseline

**The baseline is currently empty: constellation passes all 8798
applicable pjdfstest checks.** Any new failure is a regression.

The suite must run as root (pjdfstest switches uids), so it lives in the
containerized lane; the mount uses `--allow-other`. pjdfstest is built
from a pinned commit in the runner image. For fast iteration, restrict
categories: `PJDFSTEST_ONLY="rename chown" tests/compliance.sh`; dump
all failure detail lines with `PJDFSTEST_DETAILS=all`.

## Stress: fio + stress-ng

`tests/stress.sh` runs on a live mount:

- **fio with end-to-end verification** (catches data corruption):
  sequential write + sha1 verify, random 4k–256k writes + crc32c
  verify, two concurrent writers + verify. `STRESS_SIZE` (default 32M)
  scales the files.
- **stress-ng metadata churn**: `dentry`, `dir`, `rename`, `symlink`,
  `open` stressors, 2 workers each, `STRESS_TIMEOUT` (default 15s) per
  stressor.
- a post-stress canary write verifies the mount survived.

## S3 emulation: floci

Integration tests use [floci](https://github.com/floci-io/floci)
(`floci/floci:1.7.0-compat`, pinned in `docker-compose.yml`) as the S3
backend: MIT-licensed, no auth token, ~90 MB image, millisecond startup.
LocalStack Community was retired in March 2026 (2026.x images require an
auth token), and floci is wire- and init-hook-compatible with it.

Critically for constellation, floci's S3 implements **both conditional
write primitives** the design depends on:

- `If-None-Match: *` (create-if-absent) — required from phase 1
  (`fs create` races, chunk immutability)
- `If-Match` etag CAS — required for leases and the ordered metadata log
  (phase 3)

`constellation doctor` probes both against any backend. The plain-file
backend supports only create-if-absent, which is fine for single-node
phase 1; the S3 lanes validate the full contract.

The `tests/docker/floci-init.sh` hook (mounted at the
LocalStack-compatible `/etc/localstack/init/ready.d/` path) pre-creates
the `constellation-ci` bucket. Each test run uses a unique
`run-<epoch>-<pid>` prefix, so reruns never collide even with the
emulator left running.

## Containerized lane (what CI runs)

`tests/compose-test.sh` runs *everything* in containers — including the
FUSE mount itself. The `smoke` compose service (profile `test`) gets:

- `/dev/fuse` device passthrough
- `CAP_SYS_ADMIN` (mount syscall)
- `apparmor:unconfined` (default docker profile denies mount)

The runner image (`tests/docker/Dockerfile`) is a two-stage build:
`rust:1-bookworm` compiles a release binary (with cargo cache mounts for
fast local rebuilds), and a `debian:bookworm-slim` + fuse3 stage runs it.
Only docker is needed on the host — no Rust toolchain, no fuse3.

Reasons this lane exists (vs. mounting on the host):

- **Reproducibility**: userspace (distro, fuse3, glibc) is pinned in the
  image; identical locally and in CI.
- **Isolation**: leaked mounts or hung FUSE daemons die with the
  container namespace instead of littering the host.
- **Future topology tests**: multi-node scenarios become "scale the
  mount service to N containers against one emulator", and network
  partitions become a toxiproxy container — impossible to express
  cleanly on a bare runner.

Caveats: the kernel is still shared with the host (FUSE behavior is
kernel-dependent), and hardened runners that forbid `CAP_SYS_ADMIN`
can't run this lane — use the host lanes there.

## Fault injection: the harness (`crates/harness`)

`cargo run -p constellation-harness -- run [scenario ...] [--seed N]`
is the Jepsen-style lane. The harness orchestrates everything itself:

- **Topology**: floci (S3) + [toxiproxy](https://github.com/Shopify/toxiproxy)
  on a private docker network; constellation clients run on the host and
  reach S3 *through* the proxy, so every scenario can inject faults on
  the S3 path (hard cuts, latency+jitter, bandwidth caps, sliced
  packets, mid-stream timeouts).
- **Oracle**: every workload op is applied both to the real mount and to
  an in-memory filesystem model; `verify` walks the real tree and fails
  on any divergence (missing/extra entries, kind, size, content hash,
  symlink target). A divergence means constellation lost or corrupted
  data.
- **Determinism**: workloads are seeded (`--seed`); a failure reproduces
  exactly.
- **Client lifecycle**: mount, clean unmount, `kill -9` (crash), remount,
  chunk-cache wipe — all first-class scenario operations.

Scenarios (see `harness list`): `baseline`, `latency`, `slow-network`,
`s3-outage`, `s3-flap`, `kill9-remount` (the phase-1 crash-recovery exit
criterion), `cold-cache`, `two-clients-disjoint`,
`fresh-node-bootstrap` (a brand-new node reconstructs the namespace and
data purely from S3 — the plan 28 commit chain plus log replay, or a
genesis replay of the whole log when no commit exists yet — and must
match the model exactly; also asserts `writeback.pending_uploads == 0`
so a joiner never inherits the writer's upload queue),
`commit-strips-pending-upload` (writer crashes mid-write-back after a
metadata commit exists while `pending_uploads > 0`; a fresh joiner must
bootstrap with pending == 0, no missing-chunk log spam, and readable
data once the writer finishes draining), `readahead` (cold sequential read of a
multi-chunk file under injected latency must beat the serial-fetch
floor, proving the prefetcher pipelines), `readahead-adaptive` (a
128-chunk read under 200 ms latency must expand the byte window and beat
one-sixth of the serial floor), and `scan-ahead` (200 small files read
in directory order under 60 ms latency must beat one-quarter of the
serial floor). Suite-under-fault scenarios
run the external tools from the stress lane through the same fault
injector: `fio-latency` (crc32c-verified random writes under 80 ms S3
latency), `fio-blips` (fio verify while S3 is cut for 800 ms every
~4 s — the retry layer must absorb the blips), and `stress-ng-flap`
(metadata churn during S3 flapping; the mount must stay healthy and
the spool drain afterwards). Scenarios declare required host binaries
and are skipped loudly when a tool is missing (CI installs fio and
stress-ng, so nothing is skipped there).

Phase-2 scenarios exercise **one filesystem mounted by two nodes**
(close-to-open through S3 alone): `two-clients-shared` (each node runs
a seeded workload in its own subtree; after every block each node's
tree is model-verified *through the other node's mount*, asserting
zero conflicts), and `git-workflow` (stage → atomic-rename publish on
A, consume+edit+restructure on B, publish back — exact content
verification in both directions). Verification points sit at block
boundaries where all files are closed, matching close-to-open
durability semantics; cross-node checks poll with a deadline because
propagation is asynchronous (sync interval + FUSE TTLs).

`atime-eventual` (plan 20) mounts two nodes with `--atime relatime`: a
cold read on one node must eventually advance `atime` on the holder,
and — with S3 cut via toxiproxy — reads must keep succeeding at full
speed while the atime updates are simply lost (atime never blocks a
read).

### Chaos CI (`chaos-ci`)

`harness run chaos-ci` mounts **three** clients on one filesystem and
runs the shared [`constellation-chaos`](../../reference/tools/chaos.md)
**Ci** profile: barrier-synchronized create/mkdir/unlink/rmdir/rename
storms, overlapping and disjoint byte writes, register duels, chmod
atomicity, and close-to-open visibility checks. Unlike
`two-clients-shared` (disjoint subtrees + in-memory model), chaos
targets **same-path concurrent conflicts** and checks a Jepsen-style
history.

```bash
cargo run -p constellation-harness --release -- run chaos-ci --seed 42
```

On failure, artifacts land under the scenario tempdir's `chaos-store/`
(`config.json`, `history.jsonl`, `failure.md`). Re-check offline with
`chaos check --history …/history.jsonl`. Multi-node hour-long soaks
use the same library over TCP — see
[Run a chaos soak](run-chaos-soak.md).

### Create storm without P2P (`create-storm-s3-only`)

`harness run create-storm-s3-only` reproduces plan 29 M3's leftover: with
`CONSTELLATION_P2P=off` (each client on its own node key, forwarding
disabled), three clients hammer create/write/read/unlink of their own
uniquely-named files in one shared directory for 30 s (`CHAOS_CREATE_STORM_SECS`
overrides). A contended-but-healthy cluster must never surface EIO on a
mutation just because another node holds the lease — only a genuinely
unreachable S3 may. The FUSE acquire wait is progress-based (plan 29 M3c):
it keeps retrying as long as the lease keeps changing hands (or its
holder/epoch otherwise visibly moves), and only gives up once the same
(holder, epoch) has sat unchanged for a full 2xTTL. The lease TTL is 20 s
(deadline 40 s), leaving real headroom over the sticky-lease dwell/grace
floors (5 s each, fixed regardless of TTL) even when a waiter needs more
than one dwell+grace cycle to win the CAS. After the storm, every client
writes a marker file and the scenario asserts all three mounts converge to
the same directory listing and contents.

Phase-3 scenarios exercise the partition lease (DESIGN.md §4/§5):
`lease-handover` (A writes, goes write-idle, and cooperatively releases
the lease; B must acquire it within a few seconds — not a 60 s TTL
wait — and write its own files; the epoch strictly advances across
each handover and both nodes report zero conflicts) and `lease-fencing`
(A holds the lease with unshipped records and is frozen with
`SIGSTOP`; after the TTL expires B takes over — legally, only after
tailing everything A had flushed — and writes; A is then resumed with
`SIGCONT` and must discover via a failed renew CAS that it was
deposed, report `lost` over the control API, keep its stranded journal
rather than shipping or discarding it, and refuse further mutations;
a third, fresh node bootstrapping from the shared log alone must see
exactly B's namespace). `Client::pause()`/`resume()` wrap
`SIGSTOP`/`SIGCONT`; querying the control socket is done *before*
pausing, since a stopped daemon cannot answer it.

Phase-4b scenarios cover the availability matrix's S3-down rows and
stranded-branch recovery:

- `continuation-epoch` cuts S3 for both write-eligible nodes while P2P
  remains healthy, asserts the all-member epoch through the control API,
  writes real file data from both nodes using a P2P-only lease handoff,
  heals S3, and verifies ordered drain, convergence, and zero conflicts.
- `epoch-member-lost` stops one promised member with `SIGSTOP`; the
  survivor must freeze and return `EROFS`, then resume cleanly when the
  member returns and converge after S3 heals.
- `deposed-reintegration` creates both a clean stranded file and an
  edit-vs-edit conflict on a deposed holder. On-demand reintegration must
  retain B's winner, publish A's clean file, and materialize A's exact
  bytes as `shared/.constellation-conflict/same@<node>-<ts>` on both
  mounts. The scenario verifies state transitions rather than sleeping
  across a race: B must hold an epoch newer than A's, resumed A must
  report `lost`, and B must then idle-release before A reintegrates.
  A focused shipper test also sets the durable `lease_lost` bit with no
  in-memory keeper and proves ordinary sync cannot create one or ship
  the stranded journal; the explicit reintegration sync path is the
  only bypass.

Phase-4c scenarios prove permanent leave (and that unmount alone is
not leave):

- `node-leave` mounts three write-eligible peers. With C merely
  **unmounted**, cutting S3 on A+B must **not** open a continuation
  epoch (C still counts on the roster). Admin `leave --node-id C`
  from A then shrinks the roster; after a refresh, A+B **can** open an
  epoch under S3 cut and converge after heal. A fresh third writer
  then self-leaves via `constellation leave` (tombstone + unmount);
  A+B again form an epoch under cut. Registry records are retired
  tombstones, not deleted, so numeric ids are never recycled.

Plan 29 M0a removed namespace partitions entirely (the `partition`
table, autosplit/merge, and the `RenameXpartSrc`/`Dst`/`Abort` linked-pair
rename protocol): there is exactly one metadata log stream and one
partition lease, both named `p0`, and a cross-directory rename is an
ordinary `Rename`. The `partition-split`, `rename-across-partitions`,
and `multi-partition-retention-is-per-partition` scenarios that used to
exercise that machinery are gone with it.

M3.3 scenarios exercise the P2P fast path (DESIGN.md §8, §12). Each one
sets a distinct `CONSTELLATION_NODE_KEY` per client, because the node key
is per *host* and the harness runs both "hosts" on one machine — sharing
a key would give both nodes the same identity.

`p2p-invalidation` measures cross-node visibility twice: once with the
fast path and a deliberately long 3 s sync interval (so a fast result
cannot be attributed to polling), then again with
`CONSTELLATION_P2P=off`, which must fall back to the poll bound. It
asserts the push median is both under the sync interval and less than
half the poll median. Observed: ~19–25 ms versus 3015 ms.

`p2p-handover` makes A the *active* holder under a 30 s idle-release
window, then has B write. Without the fast path B would wait out that
window or the TTL; with it, B asks A directly, A flushes and releases,
and B's CAS succeeds. It asserts the write completes in well under half
the idle window and that the epoch advanced. Observed: ~27–33 ms.

Phase 5 scenarios exercise the cooperative cache (DESIGN.md §7). Each
one uses a distinct `CONSTELLATION_NODE_KEY` and
`CONSTELLATION_DIGEST_INTERVAL_S=1` so blooms propagate in the harness
without waiting the 30 s production interval. Small caches fit in one
hash-prefix bucket (the common harness case); a TiB-class cache rotates
one bucket per interval rather than flooding a 1 MiB snapshot. S3 is
toxiproxied to 200 ms so a peer hit is unambiguously cheaper than a GET.

- `coop-cache-hit`: A writes a multi-chunk file and ships it; B (cold
  cache) reads it. B's `status.coop.peer_hits` must exceed `s3_fetches`
  and the read must finish under the all-S3 serial bound. It also
  asserts `hedges_fired < chunks`: a hedge is supposed to rescue a late
  transfer, and one that fires on every fetch doubles request load while
  sending the spare copy to the source the selector just rejected.
- `s3-retry`: a single mounted client writes four chunks, unmounts,
  drops its cache, and remounts with object_store retries disabled.
  Toxiproxy cuts S3 for the first cold GET and heals inside the
  application retry window. The hash-verified read must complete with
  exactly four logical S3 successes and no peer hits or hedges. This
  isolates the S3-leg retry from whole-fetch retries that would repeat
  peer selection.
- `coop-fallback`: same setup, then A is SIGSTOPped. B must still
  complete a hash-verified read from S3 (`s3_fetches >= 1`); hedges /
  peer errors record the dead source. Here every fetch *should* hedge —
  the peer is frozen, so each one really is late.
- `web-fleet`: one writer, two cold "web" readers each reading twice.
  Aggregate `s3_fetches` on the readers stays near the unique-chunk
  count; `peer_hits` covers the rest.

Phase 5a/5b scenarios exercise the write path itself, so they assert on
process and cache *ceilings* rather than only on content. `Client` grows
a `rss_bytes()` helper (reading `VmRSS` from `/proc/<pid>/status` for the
pid it already derives for `kill9`/`pause`), because "RSS does not track
bytes written" is the phase-5a exit criterion and cannot be checked from
the mount alone.

- `big-file-write`: a small `--cache-size` (64 MiB) and a file several
  times that, with RSS sampled *during* the write. Asserts a fixed
  ceiling and a flat slope, plus byte-exact model-verified readback.
- `staging-crash`: `kill -9` mid-write, then remount. The mount must
  come up, `staging/` must be empty after GC, and the file must be at
  its last fsynced/closed size — short or absent passes, corrupt does
  not.
- `unmount-drain`: fails the eager upload (S3 cut scoped to the upload),
  then unmounts cleanly; a second node must read the file with no
  missing chunk. This one **fails on the pre-5a tree**, which is the
  point: it is the regression test for the clean-unmount upload gap.
- `writeback-latency`: 150 ms injected S3 latency and an rsync-shaped
  workload (many small files, sequential closes) run once per
  `--write-mode`. Write-back must beat write-through by at least 3x —
  deliberately conservative against a much larger expected gap — with
  both runs model-verified identical.
- `writeback-bigfile`: a file ten times the cache budget under
  write-back with eager upload, asserting an RSS ceiling *and* a
  cache-usage ceiling while the write completes. This is the
  "file size bounded by S3, not local disk" criterion.
- `writeback-drain`: write under write-back, unmount cleanly; pending
  uploads must reach zero before unmount returns, and a second node
  must read everything.
- `writeback-fsync`: write, `fsync`, `kill -9` under write-back. The
  fsynced bytes must survive the remount; an unsynced, unclosed tail may
  be missing but never corrupt.
- `writeback-backpressure`: tiny cache, write-back, S3 cut. Writes must
  throttle and then ENOSPC rather than grow without bound or deadlock,
  and the mount must recover after heal.

The scenarios mount with `--write-mode back`; `writeback-drain` also
uses the `write-mode through --state-dir ...` control operation and
asserts that it returns only after `writeback.pending_uploads` reaches
zero. Status also exposes dirty bytes, pool concurrency, and the
adaptive remote-HEAD hit rate/decision. The measured seed-42 latency
baseline is 8.98 s through versus 20.61 ms back for 24 small files;
the big-file ceiling run is 320 MiB on a 32 MiB cache with 242 MiB peak
RSS and 32 MiB peak cache use.

Phase 6a scenarios exercise immutable subtree trees and alternate mount
roots:

- `snapshot-lifecycle` advances a live file after snapshot creation and
  proves explicit `.constellation/snapshot/<name>/` lookup still returns the
  frozen bytes. It separately checks that `.constellation` is absent from
  ordinary `readdir`, then deletes the snapshot and requires new frozen-view
  reads to fail while the live file remains intact.
- `clone-workflow` eagerly clones snapshot metadata, independently edits the
  origin and clone, verifies both byte strings, and checks the frozen source
  remained unchanged. Deleting the snapshot must not affect the clone.
- `snapshot-mount` mounts `<path>@<name>` as a FUSE root, checks exact content
  and `EROFS` on mutation, then mounts `--rw --ephemeral`, writes the clone,
  cleanly unmounts, and verifies the temporary namespace entry disappears.

Phase 6b scenarios exercise E2E passphrase mode:

- `e2e-basic` creates with `--e2e`, runs a seeded workload beside a known
  repeated-`A` marker, and reads raw `chunks/` and `log/` objects through
  floci's direct endpoint. No marker or legacy chunk/zstd magic may be
  visible. A fresh state directory must cold-bootstrap and return exact bytes;
  a deliberately wrong passphrase must fail before mounting.
- `e2e-two-nodes` mounts one E2E filesystem on distinct P2P identities, writes
  an eight-chunk file, and cold-reads it through the second node under 200 ms
  S3 latency. The keyed hash must verify and cooperative-cache `peer_hits`
  must advance, proving encrypted peer responses interoperate.

Phase 7 adds `web-ui-smoke`: one mounted daemon enables its localhost web
listener, then the harness uses ordinary HTTP (no browser automation) to
exercise `GET /api/status`, `POST /api` with `ReadDir`, snapshot
create/list/delete, and `GET /metrics`. It also JSON-round-trips the status
response and requires spool, cache, and lease gauge names. This checks the
embedded server and shared control dispatcher while keeping frontend rendering
out of the fault-injection lane.

Phase 8a adds three destructive-integrity scenarios:

- `gc-lifecycle` creates live, dead, and snapshot-only chunks under a
  zero-second test horizon. Reference GC must remove only the dead chunk,
  preserve both root classes, and write deletion evidence to `gc/journal/`.
- `gc-dedup-race` starts GC, waits until `gc/condemned.json` is visible, then
  commits identical content during the full-TTL wait. The writer must
  re-upload the condemned hash and the completed file must remain byte-exact
  after the collector's final recheck.
- `fsck-repair` removes a referenced bucket chunk while retaining its local
  cache copy, uploads an old-enough orphan, and plants a torn log segment. It
  asserts exit codes 1 (detected), 2 (repaired), then 0 (clean), remounts, and
  verifies the healed file bytes.
- `fsck-while-mounted` (plan 29 M3a) runs `constellation fsck` against a state
  dir a daemon still holds — `fjall`'s single-process lock refuses a second
  process's direct open, so `fsck` must route through the running daemon's
  control socket instead. Asserts a clean (`exit 0`) JSON report rather than a
  lock error, proving the routing worked and not just that fsck ran offline.

Phase 8c adds two upload-existence scenarios (plan 26 step 8 replaced their
mount-time bucket LIST with the replica's `chunk_ref` index):

- `existence-bloom-dedup` writes a unique multi-chunk tree on A, unmounts it,
  then mounts B with a fresh state directory and waits for A's tree to arrive
  over the metadata log. B writes identical bytes without warming its local
  cache first; hint hits must advance (`existence_chunk_ref_hits` plus
  `existence_bloom_hits`), misses stay near zero, the model oracle verifies
  both files, and a direct bucket LIST proves no additional chunk keys
  appeared. The scenario prints the hit counters by source.
- `existence-peer-hint` waits for A's clean-cache digest and writes identical
  bytes through B. B must report at least one peer upload hint — the peer
  digest is consulted ahead of the replica, so it keeps the credit — yet every
  hint still selects a confirming store operation. A fresh
  `CONSTELLATION_COOP=off` node repeats the write with zero peer hints and
  exact model verification.

Phase 8d adds `snapshot-churn`, a seeded out-of-core lifecycle oracle. Its
disk-backed `fjall` database stores live rows per writable root, immutable
rows per snapshot, and clone lineage while four concurrent workers mutate
disjoint `wN` prefixes
across `/tree` and current `/cN` roots. Every worker joins before lifecycle
operations. Three default rounds create and delete origin and clone snapshots,
create and delete clones, force a clone-of-clone, and write different bytes to
the same relative `marker` in origin and clone. Each round exhaustively walks
all live trees and explicit hidden
`.constellation/snapshot/<name>/` views, comparing type, size, hash or symlink
target, mtime, and mode.

The final round deliberately leaves an origin file, snapshot, and clone for one
last verification before cleanup. Cleanup requires an empty replica snapshot
table and root namespace, empty bucket `snaps/`, then runs zero-horizon orphan
GC and requires `chunks/` to be empty; coordination prefixes remain allowed.
Every successful mutation and lifecycle boundary is flushed as JSONL. Preserve
the trail outside the scenario temp directory and replay either with original
timing or immediately:

```bash
CONSTELLATION_SNAPCHURN_AUDIT=/tmp/snap-audit \
  target/release/harness run snapshot-churn --seed 42
target/release/harness run snapshot-churn \
  --replay /tmp/snap-audit/snapshot-churn-42-<timestamp>.jsonl
target/release/harness run snapshot-churn \
  --replay /tmp/snap-audit/snapshot-churn-42-<timestamp>.jsonl \
  --replay-no-sleep
```

Scale is controlled by `CONSTELLATION_SNAPCHURN_WORKERS` (default 4),
`CONSTELLATION_SNAPCHURN_ROUNDS` (default 3, minimum 3), and
`CONSTELLATION_SNAPCHURN_OPS` (operations per worker per round, default 30).
`CONSTELLATION_SNAPCHURN_AUDIT` selects the trail directory; without it the
scenario temp directory is used and printed.

Phase 8e adds `fallocate-sparse`. It creates a 256 MiB file with a 32 MiB
cache, writes only the endpoints and three middle chunks, punches a complete
middle chunk, verifies `SEEK_HOLE`/`SEEK_DATA`, rewrites inside the hole, and
cold-reads the layout through a fresh node. The direct bucket LIST must remain
a small constant rather than gaining 64 zero objects, while daemon RSS stays
under the fixed cache-plus-overhead ceiling. The scenario prints file size,
chunk-object count, RSS, and cache budget; seed 42 measured 256 MiB, 18
objects, 67 MiB RSS, and a 32 MiB cache.

Phase 8f adds `xattr-roundtrip`. One node sets `user.foo` on a file and its
directory, then a second node waits for log sync and verifies both values.
Removal must propagate as `ENODATA`. The same directory contains a seven-byte
file and a sparse file with a 1 GiB logical length; the virtual
`user.constellation.rsize` must return `1073741831` and
`user.constellation.rcount` must return `2`, proving holes count by logical
`file_len` without materializing data.

Because every one of these changes lands in the write path, pjdfstest
(truncate, extend, and hole semantics) and the `fio-*` scenarios are the
real regression tripwires for both phases, not just the new scenarios.

Plan 28 adds `mtree-gc-plateau` and reshapes two older scenarios around the
metadata commit chain:

- `mtree-gc-plateau` rewrites a third of a fixed 300-file set in each of twelve
  rounds, forces a metadata commit per round (taking and immediately deleting
  a snapshot publishes one), and runs `constellation gc` with a two-commit
  retention window (`CONSTELLATION_COMMIT_RETENTION=2`,
  `..._RETENTION_S=0`, unpaced compaction). The `packs/` byte footprint of
  the late rounds must stay within 1.5x the early rounds', and a fresh node
  must then bootstrap from the surviving commit to the oracle. Seed 42
  measured a flat ~43.8 KB from round 2 on; without GC it grows ~20 KB a
  round.
- `commit-strips-pending-upload` accepts a plan 28 commit as the mid-flight
  bootstrap base (plan 29 M0b retired the legacy `VACUUM INTO` checkpoint
  entirely, so a commit is now the only mid-flight base there is).
- `fresh-node-bootstrap` now rebuilds from the newest commit plus the log
  tail after its applied vector.
- `snapshot-churn` honours `CHAOS_KEEP_TMP` like the other scenarios.

Plan 26 adds five metadata-plane scenarios. They assert on **S3 request
classes and counts**, which no other lane can see: toxiproxy is a TCP fault
injector with no notion of HTTP, and floci logs bucket lifecycle only.
`crates/harness/src/reqlog.rs` is a counting HTTP relay chained in front of
toxiproxy (client -> counter -> toxiproxy -> floci), so a scenario can say
"zero LISTs of `log/p0` during the burst" about the actual wire while every
toxic still applies. It parses client-to-upstream HTTP/1.1 request lines only,
and exposes a `desyncs` counter that every scenario using it asserts is zero,
so a miscounted stream fails the scenario instead of silently under-reporting.

- Plan 29 M0b removed `ckpt-bulk-ingest-bounded`: it existed only to bound the
  legacy `VACUUM INTO` checkpoint's inline prune (newest-2 snapshots), which
  no longer exists — a metadata commit is a delta of changed keys, not a
  whole-DB copy, so there is nothing analogous left to bound this way.
  `mtree-gc-plateau` is the scenario that now covers the metadata-plane
  footprint over repeated rewrites.
- `idle-cluster-is-quiet` converges three nodes, leaves them idle for 60 s, and
  counts every request each one makes on its own relay. Nothing may list `log/`
  at all, no LIST may fall outside the membership and designation polls, and
  the total must stay inside a budget derived from the configured periods
  (lease renewal, the 5 s registry poll, the 10 s designation poll, and the
  backed-off metadata probe itself). Seed 42 measured 563 requests
  (LIST=90 GET=472 PUT=1) against a budget of 675.
- `wan-writer-ships-put-only` puts 200 ms on the S3 path and counts the
  holder's requests across a 500-file burst: none may list `log/p0`. The
  follower runs with `CONSTELLATION_P2P=off`, so no gossip `Nudge` can reset
  its backoff and convergence has to come from the poll alone, within
  `CONSTELLATION_SYNC_IDLE_MAX_MS` plus 5 s.
- `sticky-lease-handoff-over-s3` disables P2P, so the whole lease negotiation
  happens over conditional writes. An idle holder with nobody waiting must keep
  the lease well past `CONSTELLATION_LEASE_IDLE_RELEASE_MS`; B's first write
  must then complete — not EIO — inside `TTL/2 + dwell + 10 s`, the fencing
  epoch must advance, and A must have released cooperatively rather than been
  deposed.
`p2p-partition-tolerance` cuts P2P with the kill switch on one node
(toxiproxy only fronts S3, so this is how plan 02 specifies simulating an
unreachable peer) and re-runs the shared-filesystem workload: everything
must still converge over S3.

**Phase 4a (pin/offline, DESIGN.md §5.2, §7, §9) has no harness
scenarios yet.** `pin-follow`, `offline-designee-writes`, and
`offline-delegation` (plan 03's asks) are not automated; pin admission,
overlap rejection, and delegation grant/expiry are covered by unit
tests instead (`fs-core::cache`, `meta::store`, `store-s3::designation`,
`net::delegation`), and the operator-facing paths (`pin`/`unpin`/
`offline`/`online`/`--ro`, admission refusal, overlap refusal,
designee-writes-through, release-by-designee-only) were verified
manually end-to-end on a live single-node mount. The existing 20-scenario
matrix plus pjdfstest stayed green with the new per-mutation designation
check added to the FUSE write gate, but that only proves the gate is a
no-op when no designation exists — it does not exercise the multi-node
fault scenarios (cut S3+P2P to the designee, verify EROFS elsewhere,
heal, model-verify) plan 03 describes.

Requires docker + fusermount3 + a release binary on the host
(`CONSTELLATION_BIN` overrides discovery). Containers are labeled
`constellation-harness=1` and removed on drop, even when a scenario
panics.

### `forward-timeout-reexec` (plan 30 M2, exactly-once forwarding)

Two nodes, `CONSTELLATION_LEASE_TTL_MS=10000` and
`CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS=1500` (over the 500 ms
default forward timeout) on both. Five rounds alternate which node
holds the lease and exercise `O_EXCL` create, `mkdir`, `unlink`,
`rename`, and `link`: the requester's forward times out after the
holder already executed the op. Before plan 30 M2,
`mutate_op_rebasable` (`crates/cli/src/fusefs.rs`) fell back to
acquiring the lease and re-executing the op locally — wrong errno
(`EEXIST`/`ENOENT`) on a call POSIX says must succeed (bug A). With M2's
rid-based exactly-once identity, the same fallback instead retries the
same rid (to the same holder, then a redirected one) and, failing that,
resolves the in-doubt op against the `completed` keyspace before ever
executing it again.

Two non-vacuity checks, both required, checking different things:

- **Per round**, `forwarded_err` must rise — proves the fault actually
  engaged (a slow host that raced the fault deterministically anyway
  would make this scenario pass for the wrong reason).
- **Across the whole run**, `forward_dedup_hits + forward_indoubt_resolved`
  (summed over both nodes) must rise at least once — proves the
  exactly-once *mechanism* engaged somewhere, not just that every round
  happened to resolve safely by coincidence (e.g. mkdir/link's
  idempotent-merge convergence, below). This is checked in aggregate,
  not per round: a race can correctly resolve *either* by the holder or
  lease path recognizing a genuine retry (dedup rises) *or* by the
  lease path legitimately finding the op was never completed anywhere
  and executing it fresh exactly once (no dedup signal at all, since
  there was nothing to dedup) — both are correct, and which one a given
  round takes depends on exactly when the holder ships relative to the
  requester's retry timing. What must never happen — a second execution
  — is what the existing `ino_agrees` check (below) catches directly.

The failure message lists every anomalous round: op, errno (or missing
dedup evidence for the whole run), and a diagnosis.

**`ino_agrees`**: existence converging on both nodes is necessary but
not sufficient — a create-family op whose local re-execution raced
ahead of the holder's shipped record (rather than seeing it and failing
`EEXIST`) creates a *second*, independent inode under the same name,
which ordinary same-name-conflict replay resolves silently. `ino_agrees`
compares the kernel inode number of the resulting name across both
nodes, catching that even when no errno reaches the caller.

### Known-bug reproductions (plan 30 M0/M1, bug B pending M3)

`harness list` prints a second catalog after the ordinary scenario list,
headed `known-bug reproductions (expected to FAIL until fixed)`, backed
by `scenarios::KNOWN_BUG_REPROS` (`crates/harness/src/scenarios.rs`) —
kept out of `SCENARIOS` so a bare `harness run` (no names) never treats
a documented bug as a regression. `harness run <name>` resolves a name
in either list. When a later milestone fixes the bug, its scenario
moves into `SCENARIOS`, unchanged, as the regression test.

Reproducing these needs two things no scenario had before:

- `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` (default 0, see
  [Configuration](../../reference/configuration.md)) sleeps this long,
  holder-side, right before a forwarded mutation's reply is sent — after
  the op already executed. A `SIGSTOP` cannot substitute: the holder's
  handoff path and a forwarded execution race for the same lock, so
  freezing the process risks freezing the handoff instead of the reply.
- `reqlog::CountingProxy` (otherwise just a request counter) gained
  `cut()`/`heal()`: a scenario hands one client `counting_proxy().endpoint()`
  instead of the shared `env.endpoint`, so that one node's S3 path can
  drop while its peers' stay up.

Scenarios:

- **`holder-crash-phantom-shadow`** (bug B, third node takes over). Three
  nodes; A's S3 goes through its own `CountingProxy` switch. A takes the
  lease, then its S3 is cut — it still acks a forwarded mutation from
  memory for a few seconds — B's forwarded `create_new("phantom")` lands
  and is applied on B's replica (`forward::apply_accepted`) before A is
  killed. C takes over once A's lease expires and never saw the
  stranded record. B and C disagree on `phantom`; a fresh node D, mounted
  after B cleanly unmounts (publishing a commit), inherits whichever
  side published first. Fails listing each disagreement; passes once M3
  lands.
- **`holder-crash-phantom-new-holder`** (bug B, the requester takes
  over). Same stranding, but B itself becomes the next holder. C's
  `create_new("phantom")` should then succeed — the name was never
  created in the durable history — but B validates the create against
  its own stranded phantom entry and answers `EEXIST`.

All three poll the control API (`lease`, `forwarded_err`) and the
mounted namespace with `eventually` rather than sleeping and hoping.

The harness also runs **fully containerized** (`make harness-docker`,
compose service `harness`): the image bundles the binaries plus fio and
stress-ng, mounts the host docker socket (floci/toxiproxy become
sibling containers), uses host networking so their published
`127.0.0.1` ports stay reachable, and gets `/dev/fuse` +
`CAP_SYS_ADMIN` for the mounts. Only docker is needed on the host.

The harness also hosts the census-scale import benchmark:

```bash
cargo run -p constellation-harness -- bench \
  --files 20000 --file-size 4096 --fanout 100 [--budget-s N] [--json]
```

It stages a many-small-files tree, imports it (`cp -r`) into a mount
against floci S3, and reports import (copy + durable-in-S3), warm
metadata-walk, and cold read-back rates; `--budget-s` turns the durable
import time into a hard gate. Add `--e2e` to create both benchmark
filesystems in passphrase mode (`CONSTELLATION_PASSPHRASE` is supplied by the
harness) for an otherwise identical encryption-overhead comparison.

JSON output contains import, durable import, metadata-walk, cold small-file
read, cold sequential large-file MiB/s, and warm 4 KiB random-read IOPS.
`tests/perf-gate.sh` runs the committed workload from
`tests/perf-baseline.json` and fails when any rate falls more than the
baseline's 20% tolerance. It compares the median of three runs so scheduler
noise in the sub-second metadata and warm-cache probes does not create a
spurious regression.

## xfstests

The nightly container builds pinned xfstests-dev revision
`56c410ad0f69da5b13c5807bc47b4876dcfa02b2` and runs its generic group with
`FSTYP=fuse`. Test and scratch mounts are independent Constellation filesystems
on separate S3 prefixes. `tests/xfstests-exclude.txt` records tests that need
local block-device controls or exceed the bounded network-filesystem runtime,
with a reason on every entry.
`tests/xfstests-baseline.txt` records reproducible semantic failures. The
runner compares failures both ways: new failures fail the job and newly
passing tests request baseline removal.

## Forwarded mutations and scratch directories

Run the metadata mutation unit tests directly while changing the forwarded
operation schema or holder execution:

```bash
cargo test -p constellation-meta mutate
```

These tests cover postcard round trips and authoritative
validate-and-journal behavior for `MutateOp`. The full workspace tests cover
the surrounding shipper, lease fencing, replay, and FUSE helpers:

```bash
cargo test --workspace
```

For forwarding changes, also exercise two mounted nodes against one S3 prefix.
Keep a partition leased by node A, mutate it from node B, and assert:

- B's `forwarded_ok` rises without repeated `handed the lease to a peer`;
- B reads the accepted shadow immediately;
- both replicas converge after the holder ships;
- `CONSTELLATION_P2P=off` or an unreachable holder falls back through S3; and
- `--fsync-mode s3` does not return before the barrier is durable.

For scratch changes, mark a shared directory with
`user.constellation.scratch=1`, then verify local create/write/rename/unlink,
cross-node invisibility, regular-file Publish, `EXDEV` boundary failures,
identical-manifest dedupe, and purge after remount.

There are currently no dedicated forwarded-mutation, placement, or
scratch-directory scenarios in `constellation-harness`; its existing
multi-client and P2P-invalidation scenarios test adjacent fallback and
convergence behavior only. Add a named harness scenario before relying on
fault-injection coverage for holder crash-after-ack or Publish crash windows.

## Named filesystems and the shared mount daemon (plan 21)

`named-shared-daemon` drives the real `constellation` binary directly
(not the `Client` harness abstraction, which always mounts ad hoc via
explicit `--state-dir`/`--s3` and stays in the foreground for direct
process-lifetime control) against an isolated `CONSTELLATION_REGISTRY`/
`XDG_DATA_HOME`. It proves the daemon-sharing model end to end: `mount
myfs MOUNTPOINT` daemonizes for real (fork + `setsid`, not
`--foreground`); a second, independent CLI invocation, `mount
myfs:/sub MOUNTPOINT2`, attaches to that already-running daemon over its
control socket instead of starting a second process — asserted by both
mountpoints sharing one `daemon.pid` and the daemon reporting exactly
one `node_id`. It then exercises `umount myfs:/sub` (root view keeps
serving, daemon stays up) followed by `umount myfs` (last view: the
daemon runs its clean-shutdown sequence, exits, and removes its own PID
file).

## CI notes

- The `integration` job builds the runner image via buildx with
  `type=gha` layer caching. Source changes invalidate the cargo build
  layer (cache mounts don't persist across GHA runs); a cold build is
  ~40 s on top of image pulls.
- `SMOKE_IMAGE` tells `tests/compose-test.sh` to use the pre-built image
  instead of building via compose.
- Lint gates are `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`.
