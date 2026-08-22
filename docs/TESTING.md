# Testing

Constellation has five test lanes, from fastest to most realistic. All
of them run in GitHub Actions on every PR (`.github/workflows/ci.yml`).

| Lane | Command | Backend | Needs | Speed |
|---|---|---|---|---|
| Unit tests | `cargo test --workspace` | in-memory / tempdir | Rust | seconds |
| Host smoke | `tests/smoke.sh` | local directory (`object_store` LocalFileSystem) | Rust, fuse3 | ~2 s |
| Host integration | `tests/integration.sh` | floci S3 (container) | + docker | ~10 s |
| Containerized | `tests/compose-test.sh` | floci S3 (container) | docker only | ~5 min cold |
| Fault injection | `cargo run -p constellation-harness -- run` | floci S3 via toxiproxy | Rust, fuse3, docker | ~90 s |

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
floor, proving the prefetcher pipelines). The two-client
scenario is phase-1 scoped (disjoint namespaces, shared bucket); it
upgrades to shared-namespace linearizability checks when multi-writer
leases land. Verification points sit at block boundaries where all
files are closed, matching close-to-open durability semantics.

Requires docker + fusermount3 + a release binary on the host
(`CONSTELLATION_BIN` overrides discovery). Containers are labeled
`constellation-harness=1` and removed on drop, even when a scenario
panics.

## CI notes

- The `integration` job builds the runner image via buildx with
  `type=gha` layer caching. Source changes invalidate the cargo build
  layer (cache mounts don't persist across GHA runs); a cold build is
  ~40 s on top of image pulls.
- `SMOKE_IMAGE` tells `tests/compose-test.sh` to use the pre-built image
  instead of building via compose.
- Lint gates are `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings`.
