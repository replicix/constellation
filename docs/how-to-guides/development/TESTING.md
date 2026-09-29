# Testing

Constellation has seven test lanes, from fastest to most realistic. The
fast lanes run on every PR (`.github/workflows/ci.yml`); the full matrix,
xfstests, performance gate, audit, and macOS build run nightly and on manual
dispatch (`.github/workflows/nightly.yml`).

| Lane | Command | Backend | Needs | Speed |
|---|---|---|---|---|
| Unit tests | `cargo test --workspace` | in-memory / tempdir | Rust | seconds |
| Host smoke | `tests/smoke.sh` (= `harness smoke`) | local directory (`object_store` LocalFileSystem) | Rust, fuse3 | ~2 s |
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
integration lanes share. It is a thin wrapper: the test itself is
`harness smoke [backend-url]` (`crates/harness/src/smoke.rs`, a step for
step Rust port of what the script used to be). The wrapper finds the harness
binary (`CONSTELLATION_HARNESS_BIN`, else the `harness` next to
`$CONSTELLATION_BIN` (same build: `make smoke`, the suite image), else
`$CARGO_TARGET_DIR/{debug,release}/harness`, else `harness` on `PATH`, else
it builds it) and forwards
its arguments; `CONSTELLATION_BIN` still selects the binary under test
(default `$CARGO_TARGET_DIR/debug/constellation`). It creates a filesystem, runs `doctor`, mounts
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

`git-under-flock` (EC2 campaign 4 B-1/B-2) mounts four nodes; two of
them alternately `git commit` (a note appended and three fresh files per
commit) to one repository, taking turns under `flock` on a turn file.
Under the lock each committer first checks that `refs/heads/master`
reads as the last acknowledged commit (a stale read there is the lost
update the soak found: commits based on an old `HEAD`). After the run
every node drains, a fresh node mounts from the bucket, and on every
node, the fresh one included, the repository tree (every `.git` file:
path, size, link count, content) must be identical, `git fsck --full`
clean, and every acknowledged commit present and an ancestor of `HEAD`.
`git-under-flock-gc` also runs `git gc` under the lock every 8 commits;
`git-under-flock-faults` injects `kill -9` (a random node, the lease
holder or the whole cluster), `SIGSTOP`, P2P isolation of one node and an
S3 cut of one node (each node reaches S3 through its own relay); there
a stale turn is only reported (a turn can begin before writes that a
crashed node held unshipped are replayed), and a commit it would lose
still fails the end-state check.
Every variant also checks the marker file the previous turn wrote under
the lock after its commit (campaign 5's check, done right: one marker for
the last commit by anyone), and that no two turns held the lock at once
(each turn records when it got and released it).
`git-under-flock-b2b` is campaign 5's shape: back to back, 5–20 files per
commit (new files and appends to any tracked file), with the turn
duration reported per decile and the slowest turns broken into steps; a
turn over `GIT_FLOCK_MAX_TURN_S` (30 s) or any failed turn fails it.
`git-under-flock-rounds` runs that workload `GIT_FLOCK_ROUNDS` (3) times,
each in a new repository, against the same daemons (the stall campaign 5
saw "never on a daemon's first workload"); the last round mounts the
fresh node and checks every round's repository.
`git-under-flock-causal` (EC2 campaign 7, finding B-1) runs the b2b
workload with every node that does not commit reading through its own
mount: a reader watches the reflog and `refs/heads/master` and, for
every commit either names, checks that the commit object, its tree and
every object under it are visible (git writes them all before the
reflog line, and the reflog line before the ref rename, so a reader
that sees the publication must see the objects); the ref must never
move to a commit that does not descend from the previous one; every
`GIT_FLOCK_FSCK_EVERY_S` (20 s) the reader takes the turn lock, checks
that the ref is the last acknowledged commit and runs `git fsck
--full`. The lock is what makes fsck meaningful: it scans the object
directories first and reads the refs and reflogs afterwards, so run
while a commit is in flight it reports `missing blob/tree/commit` and
`invalid reflog entry` on any filesystem. A reader is killed and
remounted `GIT_FLOCK_READER_RESTARTS` (2) times and keeps checking
while it catches up; since the killed reader may be the sequencer or
its backup, the committers' turn checks (stale ref, overlapping,
failed or slow turns) are then reported as under faults, not fatal.
It needs 3–4 nodes (3 by default); with `GIT_FLOCK_COMMITTERS=last`
the sequencer is a reader.
`GIT_FLOCK_SECS` sets the duration (60 s; 150 s with faults, 180 s for
b2b and causal, 90 s per round), `GIT_FLOCK_NODES` the node count (2–4),
`GIT_FLOCK_COMMITTERS=last` makes the last two nodes commit (neither is
the sequencer), `GIT_FLOCK_S3_LATENCY_MS` / `GIT_FLOCK_ENV=K=V,...` add S3
latency and mount environment, and `GIT_FLOCK_RUST_LOG` sets the
daemons' `RUST_LOG`.
Every variant also reports lock fairness from its turn records: the
acquire wait per decile, the turns that followed the same committer's
turn, and the turns granted ahead of a committer that had asked earlier
(EC2 campaign 8's B-1 measured this, not staleness: its "stale local
HEAD at turn start" compared a committer's `HEAD` with the *other*
committer's marker, so it fired on every double turn; in all 795 flagged
turns the `HEAD` read was the latest commit by anyone).
Every variant polls each node's FUSE request watchdog
(`status.fuse_requests`, see `CONSTELLATION_FUSE_REQUEST_STALL_S`)
during the run and fails at the end if any request went unanswered past
the threshold (90 s there: the first write after a whole-cluster
`kill -9` legitimately waits for the dead lease to expire; EC2 campaign
7's B-2 hang was for good); its git commands and tree walks are bounded
(`GIT_FLOCK_GIT_TIMEOUT_S`, 120 s) so a hung mount fails the scenario
with the node's watchdog report instead of hanging the harness, and the
daemons' logs are kept per incarnation (`mount.log.<n>`) on failure.

`lock-grant-dead-generation` (EC2 campaign 7, finding B-2) reproduces
the hang's mechanism directly: `d1` delegated to `c` and written into
by `c` and `b`, the turn file locked and released by `b` and `a` (the
lock's floor now names the generation), the delegation ended, `b`
remounted, then `b` takes the lock. The grant raises `b`'s session
watermark to a generation that ended before its incarnation and to the
root's journal position; 21 lookups on `b` must then be fast, with no
session timeout. On a build without the fix the lookups take 126 s (69
timeouts of 2 s).

`atime-eventual` (plan 20) mounts two nodes with `--atime relatime`: a
cold read on one node must eventually advance `atime` on the holder,
and — with S3 cut via toxiproxy — reads must keep succeeding at full
speed while the atime updates are simply lost (atime never blocks a
read).

### Sharding and machine-readable results (plan 31 C0)

`harness run` takes three flags for CI matrices and for tools that compare
runs:

- `--shard i/n` (1-based, e.g. `--shard 2/4`) runs only every n-th scenario
  of the *selected* list: after name filtering, the scenario at position
  `idx` belongs to shard `idx % n + 1`. The partition is deterministic, so
  the n shards together run each selected scenario exactly once. Bad syntax
  (`0/4`, `5/4`, `2`, `a/b`) is rejected before anything starts.
- `--results-json <path>` writes a JSON file after the run, also when
  scenarios failed (the exit code is unchanged, and the stderr output is
  the same as without the flag). Shape (schema 1):

  ```json
  {"schema": 1, "lane": "linux-fuse", "seed": 42, "shard": "2/4",
   "started_at": 1790000000,
   "scenarios": [{"name": "baseline", "outcome": "passed", "seconds": 12.3, "reason": null}]}
  ```

  `outcome` is `passed`, `failed` or `skipped`; `reason` is the skip reason
  (`fio not installed`) or the failure error text, `null` for a pass;
  `shard` is `null` for an unsharded run. The format is documented in
  `crates/harness/src/results.rs`; `tests/parity.py` (plan 31) reads it.
- `--lane <name>` sets the `lane` recorded in that file. Default:
  `<os>-<frontend>`, plus `-process` under `--s3-backend process`, so a
  plain run is `linux-fuse` and a native-S3 run is `linux-fuse-process`.
  An explicit `--lane` always wins.
- The file also records `s3_backend` (`docker` or `process`) and `frontend`
  (`fuse`). They are additive; schema stays 1 and readers must not require
  them.

```sh
harness run --shard 2/4 --lane linux-fuse --results-json results-2.json
```

### Frontends and S3 backends (plan 31 C6)

Two switches select what a run is measured against; together with the OS
they name the *lane* that `tests/parity.py` compares.

- `--frontend <name>` is the filesystem frontend the clients mount through.
  Only `fuse` exists so far; any other value is rejected up front. It is
  recorded in the results file and forms the lane name.
- `--s3-backend docker|process` (also `harness bench` and `harness
  meta-bench`) picks the S3 server, with the same `S3Env` surface for
  scenarios either way (proxied endpoint for clients, direct endpoint for
  harness-side checks, the toxiproxy handle, the bucket already created,
  teardown on drop):
  - `docker` (default): today's floci 1.7.0-compat + toxiproxy 2.12.0
    containers on a private docker network.
  - `process`: a native `versitygw` (posix backend on a temp dir, credentials
    `test`/`test`) and a native `toxiproxy-server`, both on 127.0.0.1 with
    free ports. No Docker, so it runs on hosts that lack it (macOS/Windows
    CI). The temp dir and both processes are removed when the environment is
    dropped.
  - The environment variable `CONSTELLATION_HARNESS_S3_BACKEND` is the
    alternative to the flag (the flag wins).
  - Binaries: `CONSTELLATION_VERSITYGW_BIN` / `CONSTELLATION_TOXIPROXY_BIN`,
    else `PATH`, else `~/.local/bin`. A missing one is an error naming the
    install script.
  - versitygw rejects anonymous requests, so the harness's own raw bucket
    reads/writes (`crate::s3auth`) are SigV4-signed under this backend.
  - The `versitygw` used must enforce `If-None-Match: *` on PutObject (the
    daemon refuses a backend that does not); the pinned v1.8.0 does (older
    releases, including the 1.0.14 docker image, silently overwrite).

```sh
bash tests/ci/install-native-s3.sh                 # into ~/.local/bin
target/release/harness run basic-rw --s3-backend process --frontend fuse \
    --results-json results-linux-fuse-process.json
CONSTELLATION_HARNESS_S3_BACKEND=process target/release/harness run
cargo test -p constellation-harness --test s3_process_backend -- --ignored
```

`tests/ci/install-native-s3.sh [DEST]` installs the pinned `versitygw` and
`toxiproxy-server` (`NATIVE_S3_INSTALL=release|go|auto`). `release` downloads
the GitHub release archives and verifies pinned sha256 sums; those sums are
**empty in the script and must be filled in by whoever first runs it with
network access to the releases** (they were not guessable or verifiable when
it was written), and `auto` uses `go install` (integrity from the Go module
proxy/checksum database) for any platform whose sums are still empty. The
ignored `s3_process_backend` test starts each backend, does a PUT/GET
through the proxy with the daemons' own S3 client, observes a latency toxic,
a cut and a heal, exercises the signed raw helpers, and checks teardown.

### Scenario capabilities (`Cap`, plan 31 C6)

A scenario that needs something only some frontends have says so:
`Scenario::caps` (next to `requires`, default `&[]`). `Cap` is
`constellation_vfs::Cap`, **derived** from the frontend's `FrontendCaps`
declaration (`FrontendCaps::caps()`; the harness's `caps::caps_of`), never
kept as a second table: `FuseAbort` is `abortable`, `ClusterLocks` is
`cluster_locks`, `Xattrs`, `HardLinks`, `Fallocate`, `SeekHole`,
`SpecialFiles`, `PushInval`/`PushInvalFull`, `PerCloseFlush`,
`VirtualXattrsListed`, `CaseInsensitive`, `KeepOpenUnlinked`. `harness run`
skips a scenario whose caps the selected `--frontend` lacks with the reason
`requires capability <Cap>` (the cap as a whole word, which is what the
parity file's `cap = "<Cap>"` wildcard entries and `tests/parity.py`'s
`is_cap_skip` match; a missing tool, `... not installed`, never counts), and
`harness list` shows `[needs: ...]`. Today's tags are deliberately few: the
lock scenarios (`ClusterLocks`), `fuse-inval-storm` and
`git-under-flock-faults` (`FuseAbort`), `xattr-roundtrip` (`Xattrs`),
`fallocate-sparse` (`Fallocate`, `SeekHole`), the open-orphan scenarios
(`KeepOpenUnlinked`), `subtree-confinement` (`HardLinks`, `Xattrs`).
`--without-cap <Cap>` (repeatable) runs as a frontend lacking that
capability, to exercise the skip path and the parity wildcard without a
second frontend:

```sh
target/release/harness run xattr-roundtrip --without-cap Xattrs \
    --results-json /tmp/r.json      # skipped: "requires capability Xattrs"
```

`Client` no longer shells out to `fusermount3` or calls `kill(2)`: unmount,
lazy detach, FUSE abort, `SIGSTOP`/`SIGCONT` (`pause`/`resume`) and the
`kill -9` of `kill9`/`kill9_within` go through `constellation-platform`
(`mounts.unmount`/`abort_fuse`, `process.suspend`/`resume`/`kill`), so a
macOS or Windows lane drives the same scenarios; Linux behaviour is
unchanged (the platform unmount runs the same `fusermount3 -u` / `-uz`).

### `harness smoke`

`harness smoke [backend]` is `tests/smoke.sh` ported to Rust (see above):
create + `doctor`, refused double create, mount, namespace ops, a 3.5 MiB
multi-chunk file, partial edit, truncate, append, unlink-while-open,
rm/rmdir, remount, cold-cache read, `status`. The backend is a directory
(default: a fresh temp dir) or `s3://bucket/prefix` with `AWS_*` in the
environment.

### `harness interop write|verify`

The cross-OS interop lane (plan 34's macOS lanes use it): the bucket a fs
was written into on one OS is mounted and checked on another.

```sh
harness interop write  --bucket-dir bucket/ [--backend file|process] [--seed 42]
harness interop verify --bucket-dir bucket/ [--backend file|process]
```

`write` mounts a **fresh** filesystem and writes a deterministic tree
derived from the seed: small files around the 4 KiB/1 MiB boundaries, a
24 MiB multi-chunk file, a 96 MiB sparse file, patched/shrunk/grown files,
non-ASCII and 255-byte names, nested directories with modes (`0700`, sticky),
a 200-entry directory, symlinks (relative, dangling, long), hard links (incl.
one whose original name is unlinked), xattrs (set, replaced, removed, on a
file and a directory), a FIFO, device nodes with large major/minor numbers
(only when run as root), renames, operations the fs must refuse
(`ENOTEMPTY`, `EEXIST`, `ENOENT`, `ENAMETOOLONG`, `ENODATA`, asserted at
write time), and a snapshot followed by divergence of the live tree. It
unmounts and leaves the bucket in `--bucket-dir` (which must be empty), with
`INTEROP.json` (seed, whether devices were written) written last.

`verify` mounts the bucket with a fresh state dir and checks every item:
content, size, mode, mtime (exact, ns), link counts and inode identity,
readlink targets, xattrs, `rdev` major/minor, the exact directory listings,
and both the frozen snapshot view and the live tree. It reports all problems
before failing. Backends: `file` (default) is the local file backend directly
in `--bucket-dir` (verify mounts a copy, leaving the artifact untouched);
`process` runs the fs on versitygw and moves the bucket through the S3 API
(one file per object, imported by PUTs), never by copying versitygw's data
directory, whose object metadata lives in xattrs that a cross-OS tar loses.

Known deviation recorded by the lane: `link(2)` sets the target inode's
mtime to "now" (POSIX: only ctime changes), so the mtime of hard-linked
`t/hl/a` is not compared until that is fixed.

### Platform parity (`tests/parity.py`)

`tests/parity.py` compares the results files of any number of lanes against
a reference lane (default `linux-fuse`) and writes a Markdown summary
(CI appends it to `$GITHUB_STEP_SUMMARY`); exit status 0 = parity holds,
1 = violations, 2 = unusable input.

```sh
python3 tests/parity.py --expect tests/platform-parity.toml \
    [--require-lane linux-fuse --require-lane linux-fuse-process] results/results-*.json
python3 -m unittest discover -s tests -p 'test_parity.py' -v   # the checker's own tests
```

Inputs are `--results-json` files (schema 1). Files with the same `lane` are
shards of one run and are merged by concatenating `scenarios`; the same
scenario twice in one lane is an error. Rules, for every non-reference lane
and scenario:

- the outcome must equal the reference's, unless an `[[expect]]` entry covers
  that (scenario, lane);
- a scenario present in one lane and absent from the other is a violation
  (a lost shard must not look like a pass);
- any `failed`, in any lane including the reference, is a violation, and
  `failed` can never be expected;
- every `--require-lane` must have results, so a lane whose job died before
  writing its results file fails the check instead of dropping out of it
  (nightly requires every lane it runs).

`tests/platform-parity.toml` holds the expectations. It is seeded empty:
`linux-fuse-process` differs from `linux-fuse` only in the S3 backend, so any
difference is a bug to fix or a scenario-specific entry to explain. An entry:

```toml
[[expect]]
scenario = "<name>"        # or "*" (capability skips only)
lanes    = ["macos-nfs"]
outcome  = "skipped"       # the only outcome that may differ
cap      = "<Cap>"         # required with scenario = "*"
reason   = "why this lane legitimately differs"
```

Entries are two-way, like the xfstests baseline: an entry whose lane or
scenario is not in the results, whose stated outcome is not what the lane
reports, or whose lane now equals the reference, fails the check as stale.
A wildcard covers the skipped scenarios of the listed lanes whose recorded
skip reason names the `cap` as a whole word, so it cannot also excuse an
unrelated skip; a missing-tool skip (`<tool> not installed`) is never
covered by a wildcard. It is stale if it covers none. Optional extras:
a top-level `reference = "<lane>"` and `[lane."<name>"] reference = "<lane>"`
give a lane its own reference. `tests/test_parity.py` runs the checker on
synthetic results sets (all equal passes; a deliberate mismatch fails
closed; covered skip; stale entry; wildcard without `cap`; `failed`
expected; shard merging; missing reference lane; ...).

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

Plan 30 M8 added a **close-to-open** checker (`sessions::check_close_to_open`):
an observation issued after another worker's write to the same path
completed must not show a state strictly older than that write (same
black-box explanation rule as the session checkers). It is enforced when
the history was recorded with `--cto strict` mounts (the coordinator
records an `Info` event `cto:strict`; `CONSTELLATION_CHAOS_ENFORCE_CTO=1|0`
overrides) and reported otherwise. `harness run chaos-ci-strict` runs the
Ci profile with every mount `--cto strict`; in strict mode the
coordinator also records the *first* round of each step's verify reads
(issued after every op of the step completed), not only the converged
one. `chaos check` prints the checker's coverage.

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
  writes real file data from both nodes (B's write is forwarded to A and
  must complete within 10 s: B follows A's log stream through the epoch
  and A streams its epoch journal ahead; before, it waited 40 s for the
  log), heals S3, and verifies ordered drain, convergence, and zero
  conflicts. It prints how many transactions B installed from the stream.
- `epoch-member-dies-with-chunk`: three nodes, all lose S3; C writes two
  files in the epoch (manifests forwarded to A and streamed to B, chunks
  only on C) and B reads the first from C
  (`status.coop.epoch_member_fetches`). C stops: B's read of the second
  fails with `EIO`, never other bytes; S3 returns for A and B, A defers
  the manifests (`status.held.deferred`, `held.remote` naming C's
  chunks) and B keeps them as speculation, and a fresh S3-only node
  never meets a manifest naming a missing chunk. A's own write after
  the close reaches S3 (the log head moves; the fresh node reads it)
  while C is away — the epoch journal is captured, so only C's writes
  wait. Then, by the seed's parity: an even seed brings C back and every
  node reads both files; an odd seed gives C up with `repair drop-held
  <ino> --remote` on A, and every node converges on the two files as
  empty (conflict copies on A, the refusals in the log, B's streamed
  copies and C's own rolled back once C returns).
- `epoch-member-lost` stops one promised member with `SIGSTOP`; the
  survivor must freeze and return `EROFS`, then resume cleanly when the
  member returns and converge after S3 heals. Both scenarios run a 20 s
  lease TTL: an epoch carries only a usable lease, and at 5 s a cut just
  before a renewal left about 0.7 s for the formation, so the epoch
  carried nothing.
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
  D only). Plan 30 M9 round 2: the scenario pins
  `CONSTELLATION_BACKUP_RTT_BUDGET_MS=0` — with a backup in budget (any
  LAN peer, by default) the acknowledged tail is no longer "unshipped
  work the kill catches": the sealed backup re-ships it, and every
  acknowledged mkdir reaches the log. That is the companion
  **`holder-publishes-log-prefix-backup`** (budget 5 ms): the same
  burst and kill with B listed as A's backup; B must seal and take
  over (`backup_takeovers >= 1`, `seals >= 1`), the log must hold at
  least every acknowledged mkdir (the one in flight at the kill may be
  on B only), and D == B as before. It prints the tail rows B
  re-applied.

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
- **Plan 30 M6 session scenarios** (`session-*`; each prints the node's
  `status.session` block: reads checked, fast / covered / waited /
  timed out, replay-blocked, `observed` raises and the log2-ms wait
  histogram — the read-latency measurement):
  - `session-exists-observed`: A holds with its sync held and journals
    `g` then `f`; B's create of `f` is refused; B's `stat g` (and
    `stat f`) wait for A's position and succeed once the hold lifts
    0.5 s later (before M6 `stat g` answered `ENOENT` at once);
  - `session-forwarded-ryw`: `touch a; ls; stat .; stat a; cat a` on a
    non-holder right after each forwarded create, with A's shipping held
    and then running: `waited` must stay 0 (installed shadows raise
    nothing);
  - `session-stale-base-rename`: runs `stale-base-rename-divergence`;
  - `session-ryw-after-holder-kill`: C's forwarded create is acked by A
    (S3 cut), A is killed, B takes over; C stats its own file every 20 ms
    through the stranding and the replay and must never miss it;
  - `session-wait-degrades`: with `CONSTELLATION_SESSION_WAIT_MS=1500`
    and A's shipping held after a refusal, B's lookups answer after the
    budget (no `EIO`), one warning, `timeouts` counted, fast again once A
    ships;
  - `session-idle-latency`: after a 3-node write burst an idle read
    phase never waits; a single node never waits.
- **`visibility-after-burst`** (plan 30 M7; the local form of the EC2
  bench's row 6). Three nodes, every S3 path behind a 10 ms toxiproxy
  latency and a `CountingProxy`. A writes a 192 MiB write-back burst
  (24 × 8 MiB, incompressible), then 60 fsync'd marker files at 10/s
  while B and C poll `vis/` every 10 ms (`readdir`, then the marker's
  content); a marker's latency runs from A's `open` to the first poll
  that reads it back. Runs twice on fresh filesystems — log streams off
  (`CONSTELLATION_LOG_STREAMS=0`: gossip hints plus the S3 GET-next
  tailer) and on (after both pollers report `status.log_stream.live`) —
  and asserts, for every poller, cross-node visibility p99 < 2 s, no
  marker missed, and with streams on at most 4 S3 tail GETs (`GET` of a
  `log/` key) during the markers; prints both runs' GET counts.
  Diagnosis knobs (harness environment): `VIS_BURST_FILES`,
  `VIS_LATENCY_MS`, `VIS_DEBUG=1` (per-marker write times and
  latencies), `VIS_RUST_LOG=info,constellation_authority::stream=debug,constellation::log_stream=debug`
  (every segment's ship → stream send → receive → apply, with
  `CHAOS_KEEP_TMP=1`).
- **Plan 30 M8 `cto-*` scenarios** (each prints the nodes'
  `status.cto` block: strict reads by how they were answered — as the
  sequencer, under a read delegation, after a ReadIndex round trip, by
  an S3 tail, degraded — the ReadIndex latency histogram, and the
  sequencer's grants, recalls sent / acked / outwaited, acknowledgements
  that waited and for how long, `Held` replies):
  - `cto-strict`: three nodes (A sequencer, W writer, R reader), every
    mount `--cto strict` (`CONSTELLATION_CTO=strict`). 40 iterations:
    W (forwarding) or A (every fourth, the sequencer's own write)
    overwrites `f` and creates `n<i>`, and the harness — the
    out-of-band channel — at once opens both on R: every read must see
    the close; R never degraded. Prints R's open latency and the
    writers' close latency.
  - `cto-bounded`: the same loop under `--cto bounded`, documenting the
    staleness strict removes (reads that missed the close, and how long
    until they saw it; fails only if a close never becomes visible).
  - `cto-delegation-recall`: R's 30 opens of `f` cost at most a few
    ReadIndex round trips (then local under the delegation); W's 10
    overwrites each recall R's delegation (acked) before W's close
    returns, and R reads each new content at once. Prints R's first
    open against its delegated opens and W's close latency with and
    without an outstanding delegation.
  - `cto-recall-unreachable`: `CONSTELLATION_READ_DELEGATION_TTL_MS=3000`.
    R takes a fresh delegation on `f` and is frozen (SIGSTOP); W's close
    of `f` (forwarded), then A's own, return only after A outwaited the
    grant — at least TTL + margin after R's open, at most a few seconds
    more — with `recalls_expired` counted; R, thawed, reads the new
    content.
  - `cto-latency`: single-node strict against bounded open latency on the
    same workload (strict must ask no sequencer), and a LAN
    non-sequencer's first strict open of each of 50 files (one ReadIndex
    each) against its repeat opens (none).
  - Diagnosis: `RUST_LOG=info,constellation_authority::core::readindex=debug,constellation::cto=debug`
    (with `CHAOS_KEEP_TMP=1`) traces every strict read (how it was
    answered, the position, the wait) and every ReadIndex answer
    (touched, head, pending, granted) and recall.
  - `cto-second-node-joins`: a lone strict node keeps a kernel cache
    (TTL half the lease margin); a second node mounts and writes, and the
    first node's very next open must see it (the latch).
  - `cto-strict-root` (EC2 finding R2-4, campaign 4's A-0): nodes mount
    a fresh filesystem at the same moment — two (strict, then bounded),
    four and three (strict) — and two in order (strict); every node's
    root must be owned by the mounting user and writable. Before the fix the concurrent strict round left the root
    `root:root 0755` for good (`adopt_root` was one `Policy::System`
    attempt, and the strict kernel-cache drain kept the new holder's
    gate shut for that attempt).
- **EC2 real-S3 findings** (`crates/harness/src/scenarios/ec2.rs`):
  - `s3-cut-one-node`: three nodes; a non-holder's S3 is black-holed
    (`CountingProxy::blackhole`: accepted, never answered — a firewall
    `DROP`, not a refusal) with the product's S3 retry budget
    (`Client::without_env`). Under `--write-mode back` and `through`, a
    create + write + close with and without `fsync` must complete during
    the cut (the drain hands its chunks to a peer, which uploads them:
    `status.writeback.handoffs_*`), be visible on the other nodes, and an
    `ls -la` of a 300-file directory (every write shard), an `ls -la` of
    the closing file's own directory and a root `stat` on the cut node
    must answer while a close is in flight. A `back` close's chunks
    (forwarded as pending, awaited at the sequencer) are handed off too.
    Then the cut node's S3 is *refused* for 25 s (its rounds fail fast):
    it must propose no continuation epoch (`status.epoch.proposals`; a
    live member answers its `PingS3` probe with `Pong.s3_ok`) and every
    close completes.
  - `p2p-partition-one-node`: four nodes, placement off, all writing to
    one directory; one loses P2P to the rest (`CONSTELLATION_FAULT_P2P_
    DENY_FILE` both ways, S3 everywhere) for 40 s. The lease must not
    change hands (the isolated node's inbox escalation used to take it,
    or bounce it around the majority), majority writes stay under 12 s,
    the isolated node's writes complete through the inbox; then the
    holder itself is isolated and the others' writes must also stay
    under 12 s. Prints `inbox.leases_kept_for_p2p_side`.
  - `idle-cost`: four converged nodes idle for 120 s on the product's
    default intervals; S3 requests per node per minute by kind and key
    area, on each node's relay and in `status.s3` (they agree). Budget
    60/min per node (measured ~40).
  - `idle-cost-link-flap`: `idle-cost` with the lease holder's peer
    directory flagging every link down (the P2P deny file) for 5 s every
    15 s, as one late registry-tick ping round does under host load. The
    same budget, and the holder's inbox GETs stay at most one per idle
    requester plus one per cold ceiling (39): a flap never makes it poll
    hot or from scratch (the gate on 7dfc05b saw 96 inbox GETs; the old
    schedule costs ~2,500 here).
- **Plan 30 M9 scenarios** (each prints the nodes' `status.ack` block:
  the lease's acknowledgement policy, backups and candidate, the durable
  journal seq, parked acknowledgements, whether the fast path is gated;
  this node as a backup — whom it backs, through what, what it sealed —
  and the counters: backups added / removed, reconfiguration CAS,
  appends and acks, ack timeouts, acknowledgements that waited and for
  how long, aborted, streamed ahead / installed / dropped, seals,
  takeovers, tail rows applied, `ack=s3` fast takeovers, floor waits,
  stale-liveness refusals, reads blocked on durability). Every mount has
  a 20 s lease TTL and idle release off, so a takeover within seconds
  can only be seal-based (or `ack=s3`'s fast path), never expiry:
  - `backup-failover`: four nodes; three rounds of "30 files written on
    the holder (backup-policy write latency), the holder killed, its
    backup seals and holds within a few seconds, every acknowledged
    file is on it at once, the dead node remounts and converges (its
    stranded journal replays by rid: already completed)". Four nodes so
    that each round's backup is a never-restarted node (a node remounted
    with the same identity is unreachable over P2P by its peers for a
    long while — a P2P-layer matter; the remounted nodes converge over
    S3). Prints the failover-time distribution.
  - `backup-departs`: the backup unmounts; the holder (behind a counting
    proxy) reconfigures it out and the third node in, at most 4 lease
    CAS, while writes keep completing. Prints the S3 requests of the
    reconfiguration (lease PUTs beyond the renewals) and the write
    latency before and during it.
  - `no-peer-in-budget`: `CONSTELLATION_BACKUP_RTT_BUDGET_MS=0`: the
    local policy, no backup or candidate, the fast path open, nothing
    waited; the holder killed, the second node's write waits for the
    lease to expire (seconds; no seal, no fast takeover).
  - `ack-s3-failover`: a filesystem created with `ack_policy = s3`
    (`CONSTELLATION_ACK=s3` in `fs create`'s environment): the fast
    path is gated and every acknowledgement waited for the log (a
    follower through S3 alone sees each acknowledged file); the holder
    frozen (SIGSTOP), a peer's write takes the lease over well inside
    the TTL (`s3_fast_takeovers`); the thawed holder is deposed with no
    conflict.
  - `single-node-unchanged`: one node with default knobs (behind a
    counting proxy): local policy, no backup, the fast path open, every
    M9 counter zero over 200 writes; prints their latency and S3
    requests (M9 is a no-op on one node).
  - `backup-failover-with-delegation` (strict; `CONSTELLATION_CTO=strict`,
    `CONSTELLATION_READ_DELEGATION_TTL_MS=3000`,
    `CONSTELLATION_READ_INDEX_BUDGET_MS=20000`): the reader holds a
    delegation on `f` from the holder, which is killed; the backup takes
    over inside the lease and writes `f`: its acknowledgement returns
    only past the delegation horizon (`ack_floor_waits`, at least the
    delegation TTL after the reader's last grant), and of the reader's
    continuous samples of `f`, none started after the acknowledgement
    is stale; the reader never degraded a strict read.
  - `backup-partition`: the holder and its backup lose each other's P2P
    (`CONSTELLATION_FAULT_P2P_DENY_FILE`: each node's file lists the
    peer it cannot reach) while both keep S3 and the third node, which
    writes throughout; within 12 s the holder has reconfigured the
    backup out or the sealed backup has taken over — never two holders
    — and after the heal every file is everywhere with no conflict and
    exactly one holder.
- **Campaign 6 B-1 scenarios** (`crates/harness/src/scenarios/rejoin.rs`):
  a node `kill -9`ed while it held the lease hung on its remount. The
  hang was in the state-dir handshake, not P2P: the killed daemon's last
  thread was stuck in the kernel, so its `daemon.lock` flock and its
  `control.sock` listener outlived it, and the remount attached to a
  listener nobody served, forever.
  - `holder-kill-rejoin`: four nodes (the M9 cluster, backups on);
    `HOLDER_KILL_ROUNDS` (10) rounds of "5 files written on the holder,
    then kill -9 the holder / the holder with a `flock` held on it / one
    of its backups, wait 0, 1.5 or 4 s, remount it with P2P on within
    60 s, every node converges on every file"; prints the remount-time
    distribution and each node's seal/takeover counters.
  - `stale-daemon-lock`: one node; after a clean unmount, `harness
    mute-daemon` (a hidden subcommand) holds `daemon.lock` and a
    `control.sock` listener that accepts and never answers. A mount
    (`CONSTELLATION_CONTROL_TIMEOUT_MS=2000`,
    `CONSTELLATION_ATTACH_TIMEOUT_MS=6000`) must exit within the bound
    naming the live holder's pid and refusing to take over; `status`
    must fail within the control timeout; with the holder classified as
    killed by the kernel (`CONSTELLATION_FAULT_ASSUME_WEDGED_PID`, the
    stand-in for a zombie with SIGKILL pending, which no test can
    fabricate) the next mount rotates the lock (`daemon.lock.wedged-<pid>`),
    serves the old data, and logs the takeover and its startup phases;
    a clean remount after the holder is gone serves everything.
- **Plan 30 M11 scenarios** (`crates/harness/src/scenarios/m11.rs`):
  delegated sub-sequencers, one log. Every mount has a 20 s lease TTL,
  idle release off, no backup peer (`CONSTELLATION_BACKUP_RTT_BUDGET_MS=0`),
  a 3 s grant TTL (`CONSTELLATION_DELEGATION_TTL_MS=3000`) and a handoff
  grace of 10 minutes (`CONSTELLATION_LEASE_WANTED_GRACE_MS=600000`: the
  root keeps its lease through the bursts of forwarded writes the
  measurement compares against); each
  scenario prints the nodes' `status.delegation` block (the table, the
  grants held, the root's generations and every M11 counter):
  - `delegated-subtrees`: three nodes behind counting proxies; `d1` is
    delegated to `b` and `d2` to `c` (`delegate` on the root's control
    socket). Each node's writes into its subtree are executed by the
    delegate and appended by the root; everything converges on every
    node; the root never appends a stream batch whose deps it lacks
    (`deps_unsatisfied_at_append` is 0); same-subtree writes recall
    nothing; the delegates make no S3 request for their writes; the
    root's segment PUTs do not grow. Prints each node's write latency on
    its delegated subtree against the same node's forwarded writes
    (before the delegation) and the root's local ones, the aggregate
    throughput of both nodes writing concurrently with and without
    delegation, the cross-subtree rename's latency (it recalls both
    generations) and the S3 requests per node in each phase.
  - `cross-subtree-rename`: both delegates write in the background while
    `c` renames `d1/x-7` into `d2`: the root recalls and drains both
    generations, executes the rename after their streams and ends them;
    the file is exactly where the rename put it on every node, nothing
    is lost or duplicated, `d1` is delegated again (a new generation)
    and `undelegate` recalls it.
  - `delegate-crash`: the delegate of `d1` is killed right after an
    acknowledged write, without a backup; the root reclaims the
    unrenewable grant within the grant TTL (`reclaimed` or
    `recalls_expired`), a third node's write into `d1` completes through
    the root, the dead node remounts with its journal (its acknowledged
    writes replay by rid), everything converges and `d1` is delegated
    again at a higher generation.
  - `marker-order`: three writers (the root and both delegates) each
    write data into `d1` (delegated to `b`) then a marker into `d2`
    (delegated to `c`), for 12 s; three watchers list `d2` continuously
    and read the data of every marker they see: no node ever shows a
    marker without its data (the marker's `deps` carry the data's stream
    position; `c` waits for the root's segment carrying it).
  - `delegate-partition`: the delegate of `d1` loses its P2P link to the
    root (`CONSTELLATION_FAULT_P2P_DENY_FILE`, both ways; S3 and the
    third node stay): its writes inside the grant stay local, it stops on
    its own clock when it cannot renew (within two grant TTLs), the root
    outwaits its recall or reclaims the grant, the third node's and the
    delegate's later writes go through the root (the delegate's over the
    S3 inbox); after the heal every file is everywhere with no conflict,
    the root still holds, and `d1` is delegated again.
  - `p2p-off-no-delegation`: `CONSTELLATION_P2P=off` on two nodes:
    `status.delegation.enabled` is false, `delegate` is refused, nothing
    is ever delegated, appended or executed by a delegate, and both
    nodes' writes complete as before.
  - `single-node-unchanged` (M9's) still passes: delegation is on by
    default but a single node never delegates.
  - Phase 2b (the same cluster; the placement is opt-in per node with
    `CONSTELLATION_DELEGATION_PLACEMENT=1`, its knobs
    `CONSTELLATION_DELEGATION_WINDOW_MS` (30 s), `_MIN_OPS` (200),
    `_DOMINANCE` (70 %), `_LEAVE` (50 %), `_DWELL_MS` (60 s) and
    `_COOLDOWN_MS` (30 s)):
    - `root-failover-with-delegates`: four nodes with M9 backups
      (`CONSTELLATION_BACKUP_RTT_BUDGET_MS=50`); `d1` and `d2` delegated
      to `b` and `c`, both writing in the background; the root is killed
      mid-burst once it lists a backup; the backup seals and takes the
      lease over, learns the two generations from the log (`inherited`),
      the delegates re-stream what the old root never shipped
      (`restreams`); every acknowledged file is on every node, the dead
      root remounts and converges, no conflict copies.
    - `delegate-crash-backup`: `b` (delegate of `d1`) has a backup, `c`
      (a LAN peer in budget); its acknowledgements wait for the backup
      (`acks_parked`, `backup_appends`, `backup_acks`); `b` is killed
      mid-burst; the root seals the backup (`seals_sent`), drains its
      tail (`sealed_drained`), ends the generation and delegates `d1` to
      `c`, which writes locally; `b` remounts and converges.
    - `auto-placement`: no operator. `CONSTELLATION_DELEGATION_PLACEMENT=1`
      with a 4 s window, 20 ops, a 4 s dwell and a 2 s cool-down; `b`
      writes into `d1` steadily and the root delegates `d1` to `b` by
      itself (`place_delegated`, `kinds` says `Placed`); then `c` takes
      the writes over and `b` stops: after the dwell the placement
      recalls `b`'s generation (`place_recalled`) and after the cool-down
      delegates `d1` to `c`; the counters bound the moves (no flapping);
      everything converges.
    - `designation-as-delegation` (plans 03–05): `offline /site` on `b`
      becomes a designated generation in the root's table (the 10 s
      designation poll syncs it, `designated: true`); `c`'s and the
      root's writes under `/site` are forwarded to `b`; `b` cut from
      everyone (the deny files) keeps writing locally — its grant never
      lapses — while `c`'s write under `/site` is refused `EROFS` once
      `b`'s grant lapsed on the root's clock (`refused_designated`; the
      generation is never reclaimed); after the heal everything
      converges; `online` recalls it and `c`'s writes go through the root
      again.
    - `cross-subtree-rename` now also checks the root's automatic
      re-delegation after the cross-subtree op (`redelegated`).
    - `delegate-partition` now exercises a real cut: the deny file is
      honoured by every P2P request (forwards, lease requests, read
      indexes, recalls, the delegation and backup messages) and a denied
      peer is reported as a link that is down, so the stopped delegate's
      write after the cut goes through the holder's inbox.
  - Plan 30 M12 (hot directories). The placement is now **on by
    default** (`CONSTELLATION_DELEGATION_PLACEMENT=off` pins the single
    sequencer; the M11 scenarios above pin it off, `auto-placement`,
    `hash-range-split-merge` and the M12 scenarios name the knob empty
    to test the default); `CONSTELLATION_DELEGATION_SPLIT` (20) is the
    share of a hot directory's ops several nodes must each write for the
    root to split its names into hash ranges (0 turns splitting off).
    `constellation delegate <dir> --to <node> --range <idx>/<count>`
    delegates one range by hand (`count` 2, 4, 8 or 16);
    `status.delegation.table` entries carry `range` (`"<idx>/<count>"`,
    empty for a whole directory), the report `place_splits` and
    `place_range_recalls`.
    - `shared-dir-multi-writer`: four nodes behind counting proxies
      creating unique names in one directory, after a warm-up round.
      Phase 0, the single sequencer: three nodes forward every create to
      the root. Phase 1, `shared` split four ways by hand (`0/4` to `b`,
      `1/4` to `c`, `2/4` to `d`, `3/4` the root's): each node executes
      its range locally and forwards the rest to the range's delegate;
      the root only appends. Prints the throughput of both phases and the
      S3 requests per node; asserts every range delegate executed, the
      chunk PUTs are identical (one per file), the delegates ship no
      segments, the root's log PUTs did not grow, every name is on every
      node, the directory listing is identical everywhere with every
      name once, no conflict copies.
    - `hash-range-split-merge`: the placement on by default (a 4 s
      window, 40 ops, a 4 s dwell, a 2 s cool-down); four writers into
      `shared`, none dominant: the root splits `shared` into hash ranges
      delegated to the qualifying writers (`place_splits`, the table's
      `range` entries), each range delegate executes locally; the writers
      stop, and after the dwell the ranges are recalled
      (`place_range_recalls`) and `shared` is whole again; everything
      converges.
    - `cross-range-rename`: `shared` split two ways by hand; `b` renames
      a name of its range into a name of `c`'s (the harness picks names
      by their hash): a cross-range op — the root recalls both ranges,
      executes the rename after their streams (`cross_subtree`,
      `recalls_sent`, `ended`), and delegates both again (new
      generations, `redelegated`); the file is where the rename put it on
      every node, both delegates execute locally again.
    - Model: `cargo test -p constellation-model --release --test hotdir`
      (commutative parent attributes under an HLC, hash-range ownership,
      the shared/exclusive parent hold, with the naive counterexamples).
    - Sim: `shared_dir_*` (a hot shared directory with the single
      sequencer, split by the placement, split by hand with cross-range
      renames, and under random faults); a split directory's history is
      checked for linearizability per hash range
      (`SimConfig::check_range_bits`).
    - pjdfstest must stay a full pass: every inode timestamp is now an
      HLC stamp (`crates/meta/src/hlc.rs`) and a parent's times merge by
      `max`.
    - M12 round 2. The root's three fast paths outside the core (the
      FUSE mutation path, the write-back manifest commit, the prune's
      unlink) admit an op only when `Meta::root_fast_path` says no live
      delegation owns its keys — a range's name, a file whose name is in
      a split directory — holding the delegation gate shared across the
      execution (a grant's transaction holds it exclusively);
      `status.delegation.fast_path_routed` counts what they sent through
      the core instead. The sim models the fast path
      (`SimConfig::fast_path`: `Checked` runs the admission, `Unchecked`
      is the daemon before round 2): `shared_dir_fast_path_respects_the_ranges`
      and `shared_dir_unchecked_fast_path_is_caught` (a per-range
      linearizability violation on seed 77002 without the admission),
      replayable as `AUTHORITY_SIM_CONFIG=shared-dir-fast-path[-unchecked]`.
      Pinned: `long_sessions_seed_10146_recovered_segment_is_in_the_base_window`
      and `flex_crash_seed_30299_restarted_member_adopts_the_carried_hold`.
      The placement delegates a range only to a node that *dominates*
      it (`CONSTELLATION_DELEGATION_DOMINANCE`, as for a subtree): names
      spread uniformly across the writers never split a directory (that
      split only adds a hop to every op); the sim's `shared_dir_*`
      workloads and `hash-range-split-merge`'s writers give each node
      names that hash into its own range. A forward answered `NotHolder`
      by a delegate this node's table still names the owner is retried
      after a stream tick (`deleg_grant_lag_retries`) instead of
      executed here, which recalled the grant just given; a node whose
      inbox demand escalated drops the escalation once the holder is
      reachable over P2P again, and the demand window counts overlapping
      waits once. `shared-dir-multi-writer`'s log-PUT check passes on the
      count or on the rate (the root ships per shipper round, so the
      count tracks the phase's wall time). The M11/M12 cluster helper
      waits until every node *reaches* every peer before the first phase
      (a link still being set up sent a node's first ops through the
      root's inbox, whose escalation moved the lease).
      A sim failover ends at the next *answered* op (accepted or
      refused), not the next `Ok`; `AUTHORITY_SIM_TRACE_OPS=1` prints
      every client return with its simulated time. New:
      `a_delegating_root_cut_from_s3_forms_no_epoch_and_fails_over`
      (`AUTHORITY_SIM_CONFIG=delegated-holder-cut`): a `Recall` applied
      from the log voids the generation on every replica.
      A convergence failure now prints the client history like a
      linearizability one; `chaos-soak-4` keeps every mount's full log
      next to its `history.jsonl` on failure; `meta-bench` prints each
      node's delegation counters (splits, recalls, fast path, routed,
      forwarded) when delegation is on.
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
- **`inbox-withdraw-hole`**. Three nodes, P2P on; the requester `b` is
  cut from the holder `a` over P2P (the deny files), and `a`'s inbox
  polls are paused (`CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE`), so
  `b`'s write sits unread in its inbox. The link comes back: `b`
  withdraws the batch (`status.inbox.withdrawn_ops`) and forwards the
  write over P2P. Then `b` is cut again and the polls resume. `b`'s next
  three writes go through the inbox, numbered after the withdrawn
  batch. Each must finish within 20 s (the in-doubt deadline is 60 s),
  none may take the lease path, and the holder must have read the
  tombstone (`tombstones_read`). Everything converges after the heal.
  Before the fix the withdrawal DELETEd the batch. The holder then
  probed the hole forever and executed none of the later writes: the
  first took the lease path after 18 s and pulled the lease to `b`.

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

## Cross-node `flock`/`fcntl` (plan 30 M14)

`--locks cluster` (the default when P2P is on; `CONSTELLATION_LOCKS`)
makes POSIX and `flock` locks cluster-wide: per-node, per-file *grants*
leased from the file's owning sequencer, cached across local unlocks,
recalled on conflict, and fenced with `EIO` once a grant lapses. `--locks
local` is the node-local behaviour of before (and the only mode without
P2P). Knobs: `CONSTELLATION_LOCK_TTL_MS` (5000), `CONSTELLATION_LOCK_CACHE_IDLE_MS`
(30000). `constellation status` has a `locks` section (both tables'
counters).

Model: `cargo test -p constellation-model --release --test locks -- --nocapture`
(`crates/model/src/locks.rs`; counterexamples for no fencing, a grant
before expiry plus margin under drift, failover without grace, moves
that lose state, an outwaited delegate without grace; the design clean).

Simulation: `cargo test -p constellation-authority --release` runs the
lock configurations (`locks`, `locks-partition`, `locks-skew`,
`locks-failover`, `locks-failover-backup`, `locks-faults`,
`locks-delegated`) with a mutual-exclusion ghost; replay one with
`AUTHORITY_SIM_SEED=<seed> AUTHORITY_SIM_CONFIG=<config> RUST_LOG=sim=debug,constellation_authority::core::locks=debug cargo test -p constellation-authority --release --test sim replay_seed -- --nocapture --exact`.

Harness scenarios (the harness process is the application: it calls
`flock(2)`/`fcntl(2)` on the mounts):

| Scenario | What it checks |
|---|---|
| `flock-cross-node` | an exclusive `flock` on one node refuses (`EWOULDBLOCK`) and blocks the other until the unlock; shared locks coexist; `fcntl` ranges conflict across nodes and `F_GETLK` sees the remote holder; a write under the lock is read by the next holder; then `--locks local` for the record (both nodes hold `LOCK_EX`) |
| `sqlite-two-nodes` (needs `sqlite3`) | concurrent `sqlite3` writers on one database from two nodes; `PRAGMA integrity_check` ok on both, every committed row present |
| `lock-holder-partitioned` | the lock holder cut from the owner is fenced (`EIO`) and the waiter is granted only after ttl + margin, never before the fence; the healed node locks again |
| `lock-failover` | the holder is killed with a lock held under an M9 backup; the successor reclaims/mirrors the grant (no `EIO` for the locker), refuses the contender throughout, grants it after the unlock |
| `lock-latency` | measurements: first lock from a non-sequencer, cached re-locks, the sequencer's own locks, a contended handoff, and a lone node under `cluster` against `local` |
| `lock-holder-killed-contention` | four nodes increment one `flock`ed counter (read, add, write, `fsync`); the holder is `kill -9`'d with the lock held. The survivors stall only until its grant is outwaited (ttl + margin, never less than its remaining life), hand the lock on with no further outwait (the owner's `recalls_expired` is 1), and the count is exact. Then a node killed while parked first in line costs the next waiter nothing (granted within milliseconds of the unlock, not ttl + margin later) |
| `lock-fence-at-close` | B writes under a lock and is cut from the owner past its grant; C takes the lock and writes. B's close returns `EIO` (`fcntl` closed while still locked; `flock` unlocked first, then closed), and the file holds C's data on every node |

On failure `lock-failover`, `lock-holder-killed-contention` and
`lock-fence-at-close` keep the node logs under `/tmp/harness-m14-logs`.

`chaos-ci` and `chaos-soak-4` diagnose a convergence failure before they
tear down. For the path the coordinator could not get agreement on, they
print what each node serves (length, BLAKE3), its metadata view
(`inspect`: inode, size, mtime, the manifest's digest, length and chunk
ids) and its position (`head_seq`, the lease). They sample again at 30,
60 and 120 s: a late agreement is slowness, and a node still serving
other content after two minutes is a divergence.

## Findings of the real-S3 runs (OVH, EC2)

| Scenario | What it checks |
|---|---|
| `concurrent-create-no-excl` | four nodes `open(O_CREAT)` one new name at once, **without** `O_EXCL`: every open succeeds on the one inode and every racer's byte lands in it (also with `O_TRUNC`); with `O_EXCL` exactly one wins and the rest get `EEXIST`. On the sequencer and three forwarding nodes, in a subtree delegated to `b`, and through the S3 inbox (`d` cut from every peer). Two nodes first-touching a new SQLite database never fail (needs `sqlite3`, else that part is skipped). `CREATE_RACE_ROUNDS` (20) |
| `nonowner-op-latency` | with S3 100 ms away each way and the sequencer's backup up, each of four nodes in turn runs what an untar does per entry into one shared directory (create+write+close, a whole `tar` file: create/close/`utimensat`/`chmod`, mkdir, symlink, link, chmod, chown, `utimensat`); every node's median stays under half an S3 round trip, the non-owners' included (their forwards are answered from the pre-S3 stream, not the log). The write-through close of a new chunk (`UniqueWrite`) pays its one PUT on every node and is reported, not asserted. Placement is off so the root stays the sequencer. `NONOWNER_LAT_MS` (100), `NONOWNER_OPS` (12), `NONOWNER_ONLY`/`NONOWNER_NODES` narrow a run |
| `delegated-op-latency` | `nonowner-op-latency` with the shared directory delegated to `b` (manual, placement off; the root and `b` each with a backup): `c`, `d` and the root forward to the delegate, and a reply the delegate evaluated behind its own unappended rows is answered from the root's pre-S3 stream of its append (`ack.awaited_log_streamed_deleg`), not from S3; the same medians are asserted, and that `/shared` stayed `b`'s generation throughout. Same knobs |
| `slow-s3-no-seal` | every S3 request ≥ 1.5 s (`SLOWSEAL_LAT_MS` 750 each way), product lease/sync/retry defaults, root lease and delegation placement off (so any lease change is a finding): all three nodes write small files (write-through), rename and mkdir for `SLOWSEAL_SECS` (180) s; no backup ever seals the live holder, the holder keeps its lease and epoch, and it has a backup at least 90% of the time. Prints each node's `ack` counters. `SLOWSEAL_RUST_LOG`, `SLOWSEAL_KEEP_LOGS=1` for diagnosis |
| `visibility-s3-latency` | EC2 campaign 6 D2-OVH: every S3 request ≥ 300 ms (`VISLAT_MS` 150 each way), product defaults (500 ms sync round, 60 s lease, default S3 retries, root lease placement on). A non-holder, then another node, writes `VISLAT_EVENTS` (60) small files (write+`fsync`+close, write-through) every 50 ms while two other nodes poll for each in order; each poller's p99 from the writer's `open` to the read of the right content stays under `VISLAT_P99_MS` (2 s), its median from the writer's close under half an S3 round trip (the content comes from the writer over P2P), and it GETs at most `events/10` chunks from S3. Prints per-phase close costs, S3 requests by area, the fresh-hint and stream-ahead counters. `VIS_DEBUG=1` prints every event, `VISLAT_RUST_LOG`/`VISLAT_KEEP_LOGS=1`/`VISLAT_PIN_LEASE=1` for diagnosis |
| `sqlite-first-touch-latency` (needs `sqlite3`) | EC2 campaign 6 A-1: every S3 request ≥ 300 ms (`SQLITE_LAT_MS` 150 each way), product defaults; `SQLITE_ROUNDS` (50) rounds of two nodes running `CREATE TABLE IF NOT EXISTS` + `INSERT` on one new database at once, cycling through every pair of three nodes, alternately in a directory the root sequences and one delegated to `b` (the locks granted by a delegate, capped by its delegation). No round may fail (`disk I/O error` was the lock fence: a delegate's short grant lapsed before its renewal, fixed in `a1bed13`; this fails every delegated round on `216ce6c`), and every database holds both rows on every node. `SQLITE_STRACE_DIR=<dir>` records each racer's failed syscalls |
| `small-file-write-path` | S3 100 ms away each way; the sequencer and a non-owner each close 12 small unique files, under `--write-mode through`, then `back`. Asserted per writer from its counting relay: one chunk PUT per file and no chunk HEAD or `gc/condemned.json` GET in front of it; the sequencer takes the non-owner's durable report instead of checking S3 itself. `through`: close p50 under 1.5 S3 round trips on both; `back`: under half a round trip on both. Every file then reads back right on a third node. `WRITEPATH_LAT_MS` (100), `WRITEPATH_FILES` (12) |
| `nonowner-back-crash` | S3 1 s away each way; the non-owner that is not the sequencer's backup closes files under `back` (fast), and a reader on the sequencer waits for a chunk still uploading instead of failing. Then more files, and the writer is killed with its uploads in flight: the sequencer awaits them (`status.writeback.remote_chunks_awaited`), no other node sees content S3 cannot serve, and after the remount (the pending uploads go up, reported to every peer) a third node reads every file right and `fsck` finds no dangling reference. `WRITEPATH_CRASH_LAT_MS` (1000), `WRITEPATH_FILES` |

## Cross-target type-check (`make check-cross`)

`make check-cross` (`tools/check-cross.sh`, CI job `cross-check`) runs
`cargo check` from Linux for two other targets, to keep the code portable
ahead of the Windows and macOS frontends (plan 31):

- `aarch64-apple-darwin`: the whole default workspace, one check
  (census name `workspace`).
- `x86_64-pc-windows-gnu`: each library crate separately (`-p <crate>`;
  every workspace member except `crates/cli`, `crates/harness` and
  `bench/*`), so one failing crate does not hide the others.

Build scripts get their C compiler and archiver from `tools/zcc` and
`tools/zar` (thin wrappers over `zig cc` / `zig ar`; `zig` on `PATH`, or
`python3 -m ziglang` from the `ziglang` pip package), so no cross
toolchain or SDK is needed; nothing is linked. Prerequisites: `rustup
target add aarch64-apple-darwin x86_64-pc-windows-gnu`, and zig.

The script prints a census table (target, crate, ok/FAIL, first error
line) and keeps the full logs in a temp directory. Failures that exist
today are listed in `tools/check-cross-known-failures.txt` (`<target>
<crate>` per line, `#` comments). The list is two-way, like the xfstests
baseline: the check fails on a failure that is not listed (a regression),
and on a listed entry that now passes (`STALE: remove from known
failures`), so fixing a crate means deleting its line in the same change.
`CHECK_CROSS_TARGETS=x86_64-pc-windows-gnu make check-cross` runs one
target only.

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

## The conformance kit, `MockVfs` and property tests (plan 31 C6)

Everything below the frontend is one engine behind one contract, the
`constellation_vfs::Vfs` trait; these three tools test the contract from both
sides without a kernel, without S3 and in seconds.

### `vfs::conformance`: the kit

`crates/vfs/src/conformance/` is a suite that drives *any* `Vfs`. Run it
against the reference filesystem (what the `conformance` CI job runs):

```sh
cargo test -p constellation-vfs --features conformance
cargo test -p constellation-vfs --features conformance the_reference_target -- --nocapture   # the per-test report
CONSTELLATION_CONFORMANCE_RESULTS=/tmp cargo test -p constellation-vfs --features conformance   # + conformance-<lane>.json in the harness results shape
```

Each run prints a table (`PASS`/`FAIL`/`SKIP` per `group::name` with the
failure message or the skip reason) and returns a `Report`; the JSON form
(`Report::to_json`) has the harness's results-file shape (scenario names
`conformance/<group>::<name>`) so the parity checker extends to it.

- **Tests** are named functions listed in `conformance::TESTS` (name, group,
  required `Cap`s), each seeded (`RunOptions::seed` mixed with the name) and
  deterministic in what it does; concurrent tests check invariants or a
  model, never an interleaving. Groups: `namespace` (create/lookup/mkdir/
  rmdir/unlink/rename incl. `NOREPLACE`/`EXCHANGE`, hard links, symlinks,
  special files, name limits, the `EEXIST`/`ENOENT`/`ENOTEMPTY`/`ENOTDIR`/
  `EISDIR` refusals, an unlinked-open file, a seeded model replay), `io`,
  `xattr`, `readdir` (cookies, removal between pages, concurrent create),
  `concurrency`, `deferral` (exactly-once completion, off-thread completion of
  a blocked lock, a panicking responder, the drop fail-safe), `cancellation`,
  `invalidation`, `confinement` (plan 31 §6.12: `..` at the view root, inodes
  outside the subtree `ESTALE`, `.constellation/snapshot` mirrors at the
  view's own path, `link` with and without `confine_links`, `EXDEV` only when
  set).
- **Skips are explicit.** A test whose caps the frontend lacks skips with
  `requires capability <Cap>`; one that needs something only the fixture can
  offer (a snapshot hook, a subtree view, a second view with recorded
  events) skips naming that; the cancellation group skips where a target does
  not honour `CancelToken` on waits, naming the Linux FUSE gap (fuser 0.18
  delivers no `FUSE_INTERRUPT`, so a Linux mount never sets a token: plan 31
  §6.3). `Declared::rename_flags` is the engine's declared gap
  (`View::rename` ignores `RENAME_NOREPLACE`/`EXCHANGE` today).
- **Plugging a target in.** Implement `ConformanceTarget` (a factory of fresh
  `Fixture<V>`s: the `Vfs`, the root inode, the `FrontendCaps`, optional
  hooks) and call `run_all(&target, None)` or `run(&target, &RunOptions {
  caps, .. })` for each frontend's `FrontendCaps` (the target builds its
  `PolicyStack` with `PolicyStack::for_caps`). The reference target
  (`conformance::reference::RefTarget`) is the worked example;
  `crates/vfs/src/conformance/ENGINE_TARGET.md` is the engine's instance,
  ready to drop into `crates/engine/tests/conformance.rs`.
- **The oracle** is the kit's own small path-based model (`oracle.rs`), not
  `crates/model`, which is a Stateright model of the authority protocol.

### `MockVfs`

`constellation_vfs::mock::MockVfs` (feature `mock`, implied by
`conformance`; always built for the crate's own tests) is a `Vfs` that
**records** every call (op, typed arguments, caller, thread, deadline,
cancelled-at-call) and every completion (which call, outcome, thread),
**scripts** replies per op (`on_<op>` queues one, `always_<op>` sticks: a
fixed result, a closure of the call, "return now, complete from another
thread after N ms", "drop the responder", "hold it forever"), and has an
optional **reference mode** (`MockVfs::reference(caps)`): a small correct
in-memory filesystem (namespace, sparse content, xattrs, byte-range locks
with blocking waits on a thread of their own, subtree views with
confinement and `confine_links`, snapshots behind `.constellation`, events to
other views) that behaves as the given `FrontendCaps` allow. Scripts take
precedence over it, so one op can be made to fail in a working filesystem.

`crates/frontend-fuse/tests/wire.rs` uses it to test the FUSE adapter
without a kernel: fuser's `Request`/`Reply*` types cannot be built by hand,
but `fuser::Session::from_fd` accepts any descriptor, so a `SOCK_DGRAM`
socket pair carries real FUSE messages between a fake kernel (the test) and
the whole adapter. That covers the decoding of every op's flags/modes/
whence/lock types/rename and xattr flags as the `Vfs` sees them, the
attribute/entry/statfs/dirent/lock encodings and the xattr size-probe
protocol, the errno of every `Code`, `FUSE_INIT`'s negotiation, and the
completion paths: inline on the fuser worker, deferred from another thread
with the event loop free meanwhile, a dropped responder answered `EIO`, and
a contended `F_SETLKW` answered from the view's wait thread once released.
What it cannot cover is what the kernel does above the daemon (permission
checks, the page cache, path walks, `FUSE_INTERRUPT`); the harness scenarios
and pjdfstest cover the real mount.

### Property tests

`proptest` in `crates/types/tests/properties.rs` (`Code`'s wire, Linux and
Darwin round trips over arbitrary values, unknown numbers mapping to `Io`,
the serde form; `Rdev`'s glibc and FUSE packings) and
`crates/vfs/tests/policy_properties.rs` (`NamePolicy`, `XattrPolicy`'s
classification and listing filter never leaking a hidden name, `IdentityMap`,
`PolicyStack::for_caps`, the flag algebra, `ReadData`).

## Subtree confinement (plan 31 §6.12)

`subtree-confinement` has one daemon serve three views of one
filesystem: the whole tree, a volume view (`/volumes/pv-1
--confine-links`) and a maintenance view (`/ --confine-links`), the two
volumes marked as link domains (`trusted.constellation.link_domain`),
snapshots taken of pv-1, pv-2 and `/`. Through the kernel mounts it
checks that `..` at the volume view's root is the host directory (nothing
above the volume listed) and `sub/..` is the view's root; that the
volume's `.constellation/snapshot` lists its own snapshot and the root's
(mirrored at pv-1's path), never pv-2's; that `link()` within a volume
works, across mounts is the kernel's `EXDEV`, and through a handle to a
file moved out of the volume (`linkat(AT_EMPTY_PATH)`) is the view's
`EXDEV`; that the maintenance view refuses links (and renames of a
multiply-linked file) between volumes while the plain whole-tree view
does not; plus ordinary write/read/rename/xattr/`flock`/unlink through
the volume view. The in-process cases (forged and stale inode numbers
answered `ESTALE`, snapshot views, `ViewQos` admission) are
`constellation-engine`'s `view::confine_tests` and `view::qos_tests`.

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
AUTHORITY_SIM_SEED=200 AUTHORITY_SIM_CONFIG=bugb ...  # buga | bugb | s3:<rule index> | single | long | inbox | sessions | sessions-inbox | plain
RUST_LOG=constellation_authority=debug,sim=debug ...  # narrate a replay
cargo test -p constellation-authority --test sim -- --ignored long_random   # AUTHORITY_SIM_SEEDS, AUTHORITY_SIM_START
```

A failing seed prints its replay command. The regression tests
(`regression_bug_a_slow_holder_replies`,
`regression_bug_b_holder_dies_with_unshipped_forwards`,
`regression_scripted_s3_error_codes`, `stale_base_speculation_converges`
(it used to find a divergence with stale-base speculation on; EC2
campaign 4 B-2 closed that window in the meta layer),
`regression_inbox_p2p_off` — M13's inbox with P2P off: ops answered
through the log, sustained demand escalating to a lease request —
`regression_inbox_batch_withdrawn_before_p2p_forward`,
`regression_gated_resubmission_checks_completed` and
`regression_resubmitted_rid_withdraws_its_batch`, all found by the
long configuration against the production core in phase 2) are the
simulation's counterparts of the model crate's `today_finds_bug_*`
tests; see PROGRESS.md's "Plan 30 M5" sections for what each covers.

Plan 30 M6: clients also read (the CI shards with probability 0.3 after
each op, `session_guarantees_hold` with 0.7, P2P and inbox), through the
session wait (`Meta::session_ready`, polled — the runtime is
single-threaded), and every run checks per-node read-your-writes and
monotonic reads against the log's version order (`tests/sim/session.rs`;
violations on names a tentative op touched are counted, not failed).
`session_wait_off_is_found` is the non-vacuity seed: with the wait off
the checker must find a violation.

Plan 30 M7: with P2P on (every configuration but the inbox ones), the
holder serves log streams and followers apply streamed segments through
the tail path; gossip hints carry no payload. The bus carries stream
frames in FIFO lanes per (holder, subscriber), like a QUIC stream, and
`SimConfig::stream_faults` breaks them: `drop_p` (a lost frame),
`reorder_p` (a frame delivered past its successors), `cut_p` (the holder
drops the subscriber — `Event::SubscriberGone` — while the subscriber's
end breaks), and `drop_segment_frames` (scripted loss by index). A frame
to a dead node is the holder's write error (`SubscriberGone`). Tests:
`streams_carry_the_log_and_save_tail_gets` (60 seeds with streams on and
off: streamed applies and tail skips happen, S3 GETs drop),
`stream_faults_are_survived` (150 seeds of the CI configuration plus
stream loss, reorder and cuts: every check holds, and each fault kind
was met), `regression_stream_gap_detected` (scripted loss of two
segment frames: the gap is noticed at the next frame, the sequence comes
from S3, the subscriber resubscribes, every replica converges).
`AUTHORITY_SIM_CONFIG=streams-off | stream-faults | stream-gap` replays
them; the shard summary prints the stream counters.

Plan 30 M8: `SimConfig::strict` makes client reads `cto=strict` (local
under a read delegation on the directory, else `Control::ReadIndex`
through the core, then the session wait at the answer's position), and
`SimConfig::clock_skew_ms` gives every node a clock off by a seeded
constant within ± that (leases and delegations are judged on it). Every
run checks close-to-open across nodes (`tests/sim/cto.rs`: a read
invoked after another node's write returned `Ok` must be explained by
that write's state or a later one, with the log as the version order),
enforced under `strict` with P2P and reported otherwise. Tests:
`strict_close_to_open_holds` (60 seeds, CI faults; delegations,
recalls, ReadIndex all exercised), `strict_close_to_open_holds_with_clock_skew_and_loss`
(±200 ms clocks — the sim's margin is 500 ms — and 3 % P2P loss, so
recalls are lost and outwaited), `strict_without_delegations_holds`,
`strict_with_p2p_off_tails_s3` (reported only: no ReadIndex without P2P),
and the non-vacuity seeds `bounded_mode_reads_stale` and
`delegations_without_recall_are_found` (`Config::recall_before_ack =
false`). `long_strict` (`#[ignore]`) is the long configuration in strict
mode with skew and loss. `AUTHORITY_SIM_CONFIG=strict | strict-skew |
long-strict` replays them.

Plan 30 M9: `SimConfig::strict_durability` makes two things hard
failures that the checks otherwise only report — an acknowledged op
rolled back (`acked_rolled_back`), and a refusal that observed a
tentative (acknowledged-but-unsealed) effect (`observed_tentative`);
`SimConfig::rtts` gives each link an RTT (the backup selection's budget)
and the links a `since`. `run::backup_core_config` / `ack_s3_core_config`
set the M9 knobs (short takeover and ack timeouts, `ack_s3`), and the
faults `CrashBackup`, `PartitionBackup` (the holder and its backup lose
each other; both keep S3) and `CutS3Holder` join the CI set. The run
report carries `M9Totals` (backups added / removed, appends, acks
waited, seals, takeovers, tail rows applied, `ack=s3` fast takeovers,
floor waits, streamed ahead / installed) and the failover-time samples
(holder crash → successor holds). Tests, each 20–60 seeds:
`backup_no_acked_op_lost` (three LAN nodes, CI faults: a backup is
chosen, acknowledgements wait, nothing acknowledged is lost),
`backup_failover_reships_the_tail` (holder crashes mid-burst after S3 is
cut from it: the backup seals, takes over before the TTL, re-ships its
tail), `ack_s3_no_acked_op_lost` (`ack=s3`: no backups, fast takeover on
holder silence, the log slot the only fence), `no_peer_in_budget_is_todays_behaviour`
(far peers: no backup, no appends, no seal, TTL failover),
`backup_departs_reconfigures`, `backup_partition_reconfigures_or_seals`,
`fast_failover_with_delegations_keeps_close_to_open` (strict readers
hold delegations when the holder dies inside its lease: the successor
waits the grant horizon out — `ack_floor_waits` — and close-to-open is
enforced), `pre_s3_streaming_installs_and_retires` (slow S3 makes the
stream-ahead window visible), and the non-vacuity seed
`local_policy_rollbacks_are_found` (today's `Local` policy under the
holder crash does roll an acknowledged op back, and strict durability
catches it), and `regression_refused_forward_is_not_re_executed`
(long-backup seeds 50064, 50068 and 50126, long-acks3 seed 50277: an op the
holder refused must not be executed a second time by a replay by rid
or an inbox drain, and a refusal must not leave a fast-taken-over
holder from its stale replica — the holder journals definitive
refusals as outcomes, and the answer waits for the row like any other;
backup seed 753, backup-departs seed 1122, backup-crash-slow seeds
1407 and 1402: a requester's own op that travels both on the holder's
pre-S3 stream and in its reply is one entry, whichever arrives first —
neither re-applied by a shadow, nor sent down the lease path again, nor
re-applied by the segment over later streamed rows; backup-strict seed 1328: a takeover gate waiting
out the delegation horizon ships the re-applied backup tail meanwhile;
long-backup seed 50412: a streamed refusal's `completed` row and inbox
watermark are written when its segment lands). `long_backup` (`#[ignore]`) is the long
configuration with backups, crashes and partitions (odd seeds under
`ack=s3`). `AUTHORITY_SIM_CONFIG=backup | acks3 | backup-far |
backup-strict | backup-crash | backup-crash-slow | acks3-crash |
backup-departs | backup-partition | long-backup | long-acks3` replays
them.

Plan 30 §M10's continuation epochs: `flex` (two of three nodes lose S3
and the third, form an epoch of 2/3 and keep writing) and `flex-crash`
(plus a crash with restart and random faults); `long_flex`
(`#[ignore]`, `AUTHORITY_SIM_SEEDS`) runs both. `sweep_config` takes
`AUTHORITY_SIM_CONFIG=flex | flex-crash` for large parallel sweeps
(`AUTHORITY_SIM_START`, `AUTHORITY_SIM_SEEDS`, `AUTHORITY_SIM_THREADS`)
and prints how many transactions members installed from an epoch's
stream. `flex_crash_regression_seeds` pins every flex-crash seed below
20 000 that ever failed, `flex_crash_seed_30702_an_epoch_hold_goes_only_to_a_caught_up_member`
the handoff rule, and `flex_members_follow_the_epoch_holders_stream`
the epoch stream (members install the hold owner's journal and their
forwards are answered from it). `RUST_LOG=constellation_authority=debug`
narrates a replay with each line stamped `t=<simulated ms>`, the fault
log's clock.

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
- Nightly (`nightly.yml`) also runs `harness-lanes-linux` (the
  `linux-fuse-process` lane: `--s3-backend process`, natives installed by
  `tests/ci/install-native-s3.sh`), `conformance`
  (`cargo test -p constellation-vfs --features conformance`) and `parity`,
  which downloads every `harness-*` artifact (each carries its
  `results-<lane>.json`; the `harness` job's is `results-linux-fuse.json`),
  runs the checker's unit tests, then `tests/parity.py` and appends its
  table to the job summary. The harness steps use `set -o pipefail` so the
  run's exit status survives `tee`.
