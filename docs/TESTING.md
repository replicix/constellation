# Testing

Constellation has five test lanes, from fastest to most realistic. All
of them run in GitHub Actions on every PR (`.github/workflows/ci.yml`).

| Lane | Command | Backend | Needs | Speed |
|---|---|---|---|---|
| Unit tests | `cargo test --workspace` | in-memory / tempdir | Rust | seconds |
| Host smoke | `tests/smoke.sh` | local directory (`object_store` LocalFileSystem) | Rust, fuse3 | ~2 s |
| Host integration | `tests/integration.sh` | floci S3 (container) | + docker | ~10 s |
| Containerized | `tests/compose-test.sh` | floci S3 (container) | docker only | ~5 min cold |
| Fault injection | `cargo run -p constellation-harness -- run` | floci S3 via toxiproxy | Rust, fuse3, docker | ~3 min |

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
data purely from S3 — checkpoint restore plus log replay — and must
match the model exactly), `readahead` (cold sequential read of a
multi-chunk file under injected latency must beat the serial-fetch
floor, proving the prefetcher pipelines). Suite-under-fault scenarios
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

M3.2 scenarios exercise the partition map (DESIGN.md §4):
`partition-split` (two nodes, one FS; `CONSTELLATION_PART_SPLIT_OPS`
is lowered so node A hammering `/hot` produces a child partition
visible on the control API `partitions` list; both nodes'
trees are model-verified; then `/hot` goes idle under a small
`CONSTELLATION_PART_MERGE_IDLE_S` and the child merges back to `p0`
with continued correctness) and `rename-across-partitions` (same low
split threshold, then files/dirs renamed between `/hot` and `/cold`
from both nodes — serialized by the two leases — model-verified;
a `kill9` of the renamer between operations, remount, and a further
cross-partition rename prove recovery, including the abort rule for
a durable `RenameXpartSrc` whose dst half never appears). A
single-node filesystem never splits (the heuristic requires ≥2
registered nodes).

**Both scenarios tune the policy through the environment, and the
daemon reads those variables from its own environment.** Never export
`CONSTELLATION_PART_*` into the shell that runs the harness: every
other scenario inherits them, and a tiny split threshold makes
otherwise-unrelated scenarios (`lease-handover`, which assumes a
single partition) split and fail in confusing ways.

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

`p2p-partition-tolerance` cuts P2P with the kill switch on one node
(toxiproxy only fronts S3, so this is how plan 02 specifies simulating an
unreachable peer) and re-runs the shared-filesystem workload: everything
must still converge over S3.

**Phase 4a (pin/offline, DESIGN.md §5.2, §7, §9) has no harness
scenarios yet.** `pin-follow`, `offline-designee-writes`, and
`offline-delegation` (plan 03's asks) are not automated; pin admission,
overlap rejection, and delegation grant/expiry are covered by unit
tests instead (`fs-core::cache`, `meta::sqlite`, `store-s3::designation`,
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

The harness also runs **fully containerized** (`make harness-docker`,
compose service `harness`): the image bundles the binaries plus fio and
stress-ng, mounts the host docker socket (floci/toxiproxy become
sibling containers), uses host networking so their published
`127.0.0.1` ports stay reachable, and gets `/dev/fuse` +
`CAP_SYS_ADMIN` for the mounts. Only docker is needed on the host.

The harness also hosts the census-scale import benchmark:

```bash
cargo run -p constellation-harness -- bench \
  --files 20000 --file-size 4096 --fanout 100 [--budget-s N]
```

It stages a many-small-files tree, imports it (`cp -r`) into a mount
against floci S3, and reports import (copy + durable-in-S3), warm
metadata-walk, and cold read-back rates; `--budget-s` turns the durable
import time into a hard gate.

## CI notes

- The `integration` job builds the runner image via buildx with
  `type=gha` layer caching. Source changes invalidate the cargo build
  layer (cache mounts don't persist across GHA runs); a cold build is
  ~40 s on top of image pulls.
- `SMOKE_IMAGE` tells `tests/compose-test.sh` to use the pre-built image
  instead of building via compose.
- Lint gates are `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`.
