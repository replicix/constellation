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

Plan 30 M4 added three whole-history checkers and two whole-cluster
checks. The history checkers run on the complete history at the end of
the run (the cheap per-step invariants still run after every step):
**exactly-once** (a `create` refused with `EEXIST` whose own unique
content is in the file, a storm whose effect happened with no attempt
succeeding, an `append` applied twice or after reporting failure) and
**Elle-style dependency cycles** over rename and link histories (the
`move_read` and `link_read` families race renames and hard links of a
seeded file against reads of every name; each path is single-assignment,
so every observation maps to one version and any `ww`/`wr`/`rw`/real-time
cycle is a linearizability violation). After the history passes, both
`chaos-ci` and `chaos-soak-4` wait for every node to drain, mount a
**fresh node** from the bucket, and require every replica's tree under
the work root — the fresh one's included — to be identical
(**convergence at quiescence**), then decode every log segment in the
bucket and require every `Completed { rid }` to appear once
(**exactly-once in the log**).

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
`lease-handover` (two nodes with no P2P path — they share one node
key — and a 10 s TTL: in each of three rounds and both directions, a
node writes a 25-op block while the other holds, every op is answered
(through the holder's inbox, then locally — plan 30 M13's hybrid), it
escalates and holds the lease within dwell + half a TTL + the wanted
grace, both converge on the model; the epoch strictly advances across
every handover, B's lease provably came from sustained inbox demand,
and both nodes report zero conflicts) and `lease-fencing`
(A holds the lease with unshipped records and is frozen with
`SIGSTOP`; after the TTL expires B takes over — legally, only after
tailing everything A had flushed — and writes; A is then resumed with
`SIGCONT` and must discover that it was deposed (a failed renew CAS, or
B's epoch marker), never ship under its old epoch, and — plan 30 M3b —
recover by itself: its stranded `mkdir` is rolled back and replayed by
rid through B exactly once with no conflict copy (`speculation.
depositions`/`local_rolled_back` rise, `lost` clears, the journal and
replay queue drain), after which A writes through B like any node; a
third, fresh node bootstrapping from the shared log alone must see
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
- `deposed-reintegration` (rewritten for plan 30 M3b) strands three
  edits on holder A while its S3 path is cut: a new file
  (`clean-from-a`), an overwrite of a baseline file B never touches
  (`a-only`), and an overwrite of `same`, which B then overwrites too
  after taking the expired lease. Recovery is automatic — no
  `reintegrate` call: resumed A learns it was deposed (its renewal finds
  B's lease, or it tails B's epoch marker), rolls its unshipped journal
  back from before-images, and its replay drain re-executes each
  transaction by rid through B. On both mounts `same` keeps B's winner,
  `clean-from-a` and `a-only` carry A's content, and
  `shared/.constellation-conflict` holds exactly one entry,
  `same@<node>-<ts>`, with A's losing bytes: a conflict copy only for the
  genuine overlap. Non-vacuity on A: `speculation.local > 0` before the
  pause (holder capture), then `depositions >= 1`,
  `local_rolled_back >= 1`, `replay_conflicts >= 1` (mirrored in
  `reintegration.conflicts_materialized`), `lease.lost == false`, and an
  empty replay queue. `lost` is never polled for `true`: the recovery
  clears it within one sync round.

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
`CONSTELLATION_DIGEST_INTERVAL_S=1` so membership propagates in the
harness without waiting the 30 s production interval. They run in the
default `CONSTELLATION_COOP_DIGEST=exact` mode (plan 30 §M15): pushed
deltas every 250 ms plus a 1 s summary heartbeat, reconciliation on any
gap. See [Cooperative cache membership](../../reference/features/cooperative-cache.md).
S3 is toxiproxied to 200 ms so a peer hit is unambiguously cheaper than
a GET.

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
- `coop-exact-churn` (plan 30 §M15): three nodes, each with a 24-chunk
  cache, run eight seeded rounds. In each round every node writes a
  fresh 4-chunk file and reads two recent files written by others, with
  S3 100 ms away. Every node adds and evicts chunks while its peers look
  them up. The scenario asserts:
  - zero `peer_false_positives` fleet-wide (a holder answering `Absent`;
    `RecentlyRemoved` propagation races are reported separately as
    `peer_stale_misses` and allowed);
  - `peer_hits > 0`;
  - every cache stayed within budget;
  - after quiescence, each node's `peer_set_entries` equals the sum of
    its peers' `local_set_entries`, i.e. the mirrors are exact.
- `coop-digest-compare`: the same churn workload run twice, first with
  `CONSTELLATION_COOP_DIGEST=bloom`, then `exact`. It prints one line per
  mode: peer hits and misses, false positives, stale misses, S3 fetches,
  fleet digest bytes and bytes/s, messages, digest CPU, and
  reconciliation rounds with µs per round. It asserts only the
  exact-mode invariants; the printed numbers are the measurement behind
  the default digest mode.

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

### `holder-ships-under-forward-load` (plan 30 M2b, ship-round starvation)

Three nodes (own P2P node keys), one established as the lease holder for
a shared directory before the burst starts. The other two then hammer
that directory with `O_CREAT` from 4 threads each (6,400 creates total)
— every single one a forward (ADR-14) to the holder. A background thread
polls the holder's `control_status()["spool"]["journal_backlog"]` every
5ms throughout the burst.

Before plan 30 M2b, `node_runtime`'s sync task dropped its in-flight
`run_managed_sync_round` for every `SyncRequest` other than `Nudge`,
including the holder's own `SyncRequest::Mutate` for each forwarded
mutation. Under this load shape a request arrives roughly every 0.7ms
while a round takes ~2ms (one S3 PUT), so the holder almost never
finished a round — measured (on this host, against the pre-M2b, post-M2
binary) at a peak `journal_backlog` of 12,799 during the burst, matching
the "thousands, for the whole burst" shape the coordinator's own
instrumentation found while measuring M2. With M2b's fix (`Mutate`/
`Forward` dispatched from inside the round's own `select!` loop instead
of cancelling it — see `crates/cli/src/node_runtime.rs`'s module doc and
`crates/cli/src/lease.rs`'s "Locking rules" section), the same load
measures 186-266 on this host across repeated runs.

Two checks:

- `journal_backlog` never exceeds 500 *at any sampled point* during the
  burst — chosen with over 2x headroom above the measured post-fix range
  and more than 25x tighter than the pre-fix failure mode, so a
  regression back to round-cancelling starvation fails immediately
  rather than needing to reach four digits first. Sampled continuously
  (not just checked once at the end) because a holder that starves for
  the whole burst and only catches up right at the end would otherwise
  pass an end-of-burst-only check while still exhibiting the bug.
- Every one of the 6,400 created files is visible from both non-holder
  mounts within 2 seconds of the burst ending.

### Known-bug reproductions (plan 30 M0; currently empty)

`harness list` prints a second catalog after the ordinary scenario list,
headed `known-bug reproductions (expected to FAIL until fixed)`, backed
by `scenarios::KNOWN_BUG_REPROS` (`crates/harness/src/scenarios.rs`) —
kept out of `SCENARIOS` so a bare `harness run` (no names) never treats
a documented bug as a regression. `harness run <name>` resolves a name
in either list. When a later milestone fixes the bug, its scenario
moves into `SCENARIOS` as the regression test. Plan 30 M2 moved
`forward-timeout-reexec` (bug A) and M3a moved both bug B scenarios
below, so the list is empty today.

Reproducing these needed two things no scenario had before:

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

Bug B scenarios (plan 30 M3a regression tests, in `SCENARIOS`):

- **`holder-crash-phantom-shadow`** (third node takes over). Three
  nodes; A's S3 goes through its own `CountingProxy` switch. A takes the
  lease, then its S3 is cut — it still acks a forwarded mutation from
  memory for a few seconds — B's forwarded `create_new("phantom")` lands
  and is installed on B's replica as a speculation-log shadow
  (`forward::apply_accepted`) before A is killed. C takes over once A's
  lease expires. C's first segment strands B's shadow: B rolls it back
  and replays the create by rid through C. Passes when B and C see
  `phantom` as the same inode, B's `status.speculation` shows the
  rollback and the replay (non-vacuity) and no replay conflict, and a
  fresh node D — mounted after B cleanly unmounts and publishes — sees it
  too, as the same inode.
- **`holder-crash-phantom-new-holder`** (the requester takes over). Same
  stranding, but B itself becomes the next holder. B's takeover gate
  rolls the shadow back and replays the create locally before B serves
  anything, so C's `create_new("phantom")` must fail with `EEXIST` (the
  create B's application was told succeeded is durable), and B, C and a
  fresh D agree the name exists, as one inode.

Plan 30 M3b scenarios (in `SCENARIOS`; `deposed-reintegration` above
was rewritten for the same milestone):

- **`takeover-marker-strands-promptly`** (the epoch marker). The bug-B
  rig again, with C as the requester: C's forwarded
  `create_new("phantom")` is acked by A from memory (A's S3 cut) and A is
  killed. B then takes the expired lease through an op that is
  *refused* — `rmdir` of a non-empty directory, `ENOTEMPTY` — so B's own
  op ships nothing. (`mkdir` of an existing name is not used: the kernel
  can answer `EEXIST` from its dentry cache without calling the daemon,
  so it might never take the lease.) Within 5 s of B's `rmdir`
  returning, C's `speculation.rolled_back` must rise; only B's epoch
  marker can cause that. B's `speculation.epoch_markers >= 1`, and the
  first log segment at an epoch newer than A's, read straight from the
  bucket (`segment_header` decodes the envelope's `node`/`epoch`/record
  count without the record types), must be B's, at B's epoch, with zero
  records. The stranded create then replays by rid through B exactly
  once: B, C and a fresh D (after C's clean unmount publishes) see
  `phantom` as one inode, with no replay conflict. All three nodes run
  with `CONSTELLATION_SYNC_IDLE_MAX_MS=1000`, so the 5 s bound does not
  depend on the P2P push alone.
- **`holder-publishes-log-prefix`** (holder publish). Holder A runs a
  paced `mkdir` burst (~200/s, metadata only: no close nudges, no chunk
  uploads) with `CONSTELLATION_SYNC_INTERVAL_MS=50` under 25 ms of
  toxiproxy latency each way, so every ship round leaves fresh
  transactions journaled behind it and the 32-segment publish cadence
  fires every few seconds mid-burst. `CONSTELLATION_PUBLISH_IDLE_S` is
  not set: the idle publish only runs on a round whose journal is
  empty. Non-vacuity: a status sample (every 100 ms) must see a new
  A-authored head commit (read from `commits/` directly, bypassing the
  proxy) while A's `speculation.local > 0`. A is then SIGKILLed the
  moment its next commit is visible in the bucket, before its round can
  ship what was journaled meanwhile. B has tailed the log since the
  start; a fresh D bootstraps from the head commit plus the log after it.
  Once both have applied through the log head, B and D must list exactly
  the same `burst/` entries, those must be a contiguous prefix of A's
  mkdir sequence, and A must have acknowledged more mkdirs than that
  (the kill caught an unshipped tail, which neither node shows). The
  head commit's `applied` must not exceed the log head. The harness
  cannot decode log records, so "commit == log prefix at `applied`" is
  not checked in isolation; D == B is the observable form of it (a
  commit that carried the dead holder's unshipped work would show up on
  D only).

- **`poison-record-isolation`** (plan 30 M4, L7). Deterministic through
  two test-only fault points on holder A (write-back):
  `CONSTELLATION_FAULT_LOSE_CHUNKS` (the upload pass drops `broken`'s one
  chunk from the cache right before reading it) and
  `CONSTELLATION_FAULT_HOLD_SYNC_FILE` (A's sync rounds are held while the
  file exists; A writes `<file>.held` once a round has seen the hold, so
  no round is in flight). With rounds held, A writes `broken`, `chmod`s it
  (a record that depends on the held manifest) and writes `after`; the
  hold is lifted and the first round finds the chunk gone. B must see
  `after` and `broken`'s create (empty, mode unchanged) — everything but
  the held manifest and chmod; A's `status.held` must list the inode, its
  one lost chunk and both held transactions; A must publish a new commit
  while they are held, and a fresh node D bootstrapped from the bucket
  must see exactly what B sees. `repair drop-held` over the control
  socket must then replay the chmod (B sees mode 0600), materialize
  `/.constellation-conflict/broken@…` (full length, the lost chunk a
  hole of zeros), and leave A's held set, pending conflict copies and
  journal empty.
- **`publish-only-holder`** (plan 30 M4, L8; a measurement). Three nodes,
  each behind its own counting relay, `CONSTELLATION_PUBLISH_IDLE_S=2`,
  lease placement off. A 20 s idle window and a 20 s busy window (all
  three nodes writing 512 B files every 20 ms, the followers' through
  forwarding), each followed by a drain. Prints each node's requests by
  class and bucket area per window (the milestone's request-count record)
  and requires that a node which did not hold the lease during a window
  PUT no commit and read no condemned list, and that the holder published
  during the busy window.
Plan 30 M13 scenarios (the S3 inbox; all three run with
`CONSTELLATION_P2P=off`, the holder and each requester on a counting
relay of its own so requests can be attributed per role):

- **`stale-base-rename-divergence`** (plan 30 M5 phase 2, the
  `MutateReply::base` rule on the wire). Three nodes with own P2P keys;
  A's and B's sync rounds are held with
  `CONSTELLATION_FAULT_HOLD_SYNC_FILE=<root>/hold-{a,b}` (each daemon
  writes `<hold>.held` from the first round that sees the file, so the
  scenario knows no round is still shipping). `f1` and `f2` exist
  everywhere. A (holder) unlinks `f2`, which stays in its journal; B,
  still seeing both, renames `f1` over `f2` — a shape B's own
  validation accepts (an `O_EXCL` create of a name B still sees would be
  refused locally and never forwarded). A executes it and replies with
  `base` = its unshipped position (the unlink overlaps the rename's
  keys). Post-fix B waits in `AwaitingLog` — the scenario checks the
  rename has *not* returned 3 s later — then the holds lift, A ships
  both records, the rename returns OK, and A, B and C must agree `f2`
  is `f1`'s inode with `f1`'s content and `f1` is gone. Against a
  pre-fix build (main before M5 phase 2: `CONSTELLATION_BIN=<main
  build>`), B installs the rename as a shadow onto the stale replica and
  returns at once; the arriving `Unlink(f2)` then removes the renamed
  entry and the rename record finds no `f1`, so B has no `f2` at all
  (the convergence check fails first, the early-return check second).
- **`sticky-lease-handoff-over-s3`** runs with `CONSTELLATION_INBOX=off`
  since M5 phase 2: it tests the S3-only cooperative handoff, and with
  M13's inbox on a lone blocked write is answered through the holder's
  inbox instead (no handoff, which is the point of the inbox).
- **`inbox-create-storm-p2p-off`** (the hybrid on a storm, measured).
  Three nodes, the holder established first with a 20 s TTL and idle
  release off. Two requesters run `create-storm-s3-only`'s
  create/read/unlink loop into one shared directory for 30 s
  (`CHAOS_CREATE_STORM_SECS`), 16 threads each. Linux serializes creates
  in a directory, so every op is one sequential inbox round trip and
  the requesters' demand is sustained: they escalate (`wanted_by`), the
  lease moves at the holder's dwell, and the non-holder of the moment is
  still served by the inbox. Every op must get its errno right; the
  requesters' `status.inbox.submitted_ops` and somebody's `executed_ops`
  must be non-zero; and the aggregate rate must be at least the 41 ops/s
  floor of the ping-pong band (an absolute floor: a same-run
  `CONSTELLATION_INBOX=off` baseline would need a second cluster and
  another storm-length run, and the meta-bench sweep reports that
  number on every gate). Prints the round-2 breakdown, escalations and
  lease requests per node, the handoff count (epoch delta) and each
  requester's inbox PUT count (batching is not asserted: it cannot form
  on a VFS-serialized directory).
- **`inbox-sporadic-write-p2p-off`** (the hybrid on sporadic writes).
  Same rig; `r1` writes one file every 2.5–3.5 s, 16 times. The lease
  epoch must not move, `r1`'s `escalations` and `lease_requests` must be
  zero, every file must appear on the holder, at least 16 ops must have
  gone through the inbox, and — excluding the first write, which pays
  the 5 s registry first-contact tax and is printed separately — p50
  must be within the warm poll ceiling (`CONSTELLATION_INBOX_IDLE_MAX_MS`,
  2 s) and p99 within it plus a second. Today's path for the same write
  registers `wanted_by`, waits up to TTL/4 for the holder's lease round,
  and moves the lease twice.
- **The meta-bench's roster barrier** (plan 30 M5 phase 2). Multi-node
  `harness meta-bench` configs wait, before starting the clock, until
  every node's `status.inbox.roster` names every node: with P2P off a
  holder polls the inboxes of the requesters its last registry read
  named, and the bench's mount-then-write-immediately shape had the
  holder acquiring before the other two nodes registered, so it polled
  nobody until the driver's 5 s registry refresh — a mount-order
  artifact, not what the bench measures (`inbox-sporadic-write-p2p-off`
  measures that "first-contact tax" on purpose and prints it
  separately).
- **`inbox-requester-crash-mid-batch`**. The holder's S3 is cut (it can
  neither poll nor renew), `r1` submits a create (its FUSE thread blocks
  on the outcome), the batch is seen in the bucket, `r1` is SIGKILLed,
  the holder heals. The orphaned batch executes exactly once (the name
  appears on the holder and on `r2` as one inode). `r1` remounts under a
  new incarnation, resumes its numbering by LIST-last past the batch the
  holder kept as its high-water mark (`status.inbox.next_n >= 2` after
  its next write), and that next write is polled and executed.
- **`inbox-holder-takeover-pending-batch`**. Same rig; the holder is
  killed with `r1`'s batch unread. `r2` writes and wants the lease once
  the 6 s TTL runs out; whichever node takes over (`r2`, or `r1` itself
  once its inbox wait finds the register claimable) drains the old
  epoch's inbox inside its takeover gate. `r1`'s blocked create returns
  success — not `EIO` — exactly once, `r1` and `r2` agree on one inode,
  some node's `drained_batches` rose, and the epoch-1 batch objects are
  gone from the bucket.

All of them poll the control API and the mounted namespace with
`eventually` rather than sleeping and hoping. (Since plan 30 M4 only the
lease holder publishes commits, so a follower's clean unmount no longer
does; a fresh node bootstraps from whatever head commit the holder last
published plus the log after it, which is the same state.)

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

## The authority simulation (plan 30 M5)

`crates/authority` holds the sans-IO authority core and, under
`tests/sim.rs`, a deterministic simulation that runs several real cores
over real `Meta`, `LogStore`, `LeaseStore` and `CommitChain` instances on
a simulated bucket (seeded latency, per-node cuts, scripted 412/409/404/
500/timeout and applied-then-lost answers) and a simulated P2P bus
(seeded delay, drops, partitions, pauses, crashes and restarts, fresh
joins), with seeded workloads. Histories are checked with Stateright's
`LinearizabilityTester`; every run also checks convergence at quiescence
against the log replayed onto a fresh replica, that every commit is the
log prefix at its claimed position, and exactly-once over the log's
`Completed { rid }` records.

Since plan 30 M5 phase 2 the core under the simulation *is* the
daemon's decision code: `crates/cli/src/authority_driver.rs` drives the
same `Core` from the sync loop (S3, P2P, timers and the upload pass are
its actions), so a seed that fails here is a daemon bug, not a model
bug.

```bash
cargo test -p constellation-authority                 # 1,000 CI seeds + regressions, ~15 s release / ~50 s debug
AUTHORITY_SIM_SEED=152 cargo test -p constellation-authority --test sim replay_seed -- --nocapture --exact
AUTHORITY_SIM_SEED=200 AUTHORITY_SIM_CONFIG=bugb ...  # buga | bugb | s3:<rule index> | single | long | inbox
RUST_LOG=constellation_authority=debug,sim=debug ...  # narrate a replay
cargo test -p constellation-authority --test sim -- --ignored long_random   # AUTHORITY_SIM_SEEDS, AUTHORITY_SIM_START
```

A failing seed prints its replay command. The regression tests
(`regression_bug_a_slow_holder_replies`,
`regression_bug_b_holder_dies_with_unshipped_forwards`,
`regression_scripted_s3_error_codes`, `stale_base_speculation_is_found`,
`regression_inbox_p2p_off` — M13's inbox with P2P off: ops answered
through the log, sustained demand escalating to a lease request —
`regression_inbox_batch_withdrawn_before_p2p_forward`,
`regression_gated_resubmission_checks_completed` and
`regression_resubmitted_rid_withdraws_its_batch`, all found by the
long configuration against the production core in phase 2) are the
simulation's counterparts of the model crate's `today_finds_bug_*`
tests; see PROGRESS.md's "Plan 30 M5" sections for what each covers.

The checker design, settled in phase 2: the exact log-witnessed
linearizability check runs on every seed; Stateright's
`LinearizabilityTester` additionally checks the bounded histories (≤ 80
events, ≤ 6 tentative ops) where its search finishes. A run's summary
prints both counts.

Round 3 added, after the tester's gate run: `RUST_LOG=constellation::authority_driver=trace`
prints one line per core step (`event`, `actions`, `job`, `handled_us`,
`refreshed_us`), one per S3 op issued and done (kind, latency), and the
forward round trip on the requester; `constellation=trace` adds the
holder's forwarded-mutate service time. The per-second `awk` over a kept
`mount.log` in PROGRESS.md's round-3 section is the intended use.

Tools and tests that need the authority without a daemon (`gc`'s
standalone tail, the shipper's bootstrap/replay tests) use
`authority_driver::Standalone`: the same `Core`, stepped inline over a
real store with no spawned tasks.

## CI notes

- The `integration` job builds the runner image via buildx with
  `type=gha` layer caching. Source changes invalidate the cargo build
  layer (cache mounts don't persist across GHA runs); a cold build is
  ~40 s on top of image pulls.
- `SMOKE_IMAGE` tells `tests/compose-test.sh` to use the pre-built image
  instead of building via compose.
- Lint gates are `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`.
