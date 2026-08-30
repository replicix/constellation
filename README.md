# Constellation

A distributed POSIX file system on S3: any number of machines mount the same
bucket read/write, stay conflict-free by construction, keep working offline,
and use encrypted P2P links between peers purely as a latency fast path.
One static binary — FUSE mount, daemon, CLI, and web UI. The only
infrastructure is an S3-compatible bucket with conditional-write support.

**Status:** usable single-/multi-node FUSE mounts on S3; see
[docs/PROGRESS.md](docs/PROGRESS.md) for phase detail.

## Quick start: mount a real S3 bucket

S3 auth uses `object_store`'s `AmazonS3Builder::from_env()` — the usual
`AWS_*` variables. Region is `AWS_DEFAULT_REGION` (defaults to
`us-east-1`).

**Named profiles (`AWS_PROFILE` / `aws --profile …`) do not work by
themselves.** Constellation never reads `~/.aws/credentials` or
`~/.aws/config`. Export the profile into the environment first:

```bash
eval "$(aws configure export-credentials --format env --profile dataiku)"
export AWS_DEFAULT_REGION=eu-west-1   # if the profile does not set it
```

Other options that work without a profile: static
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and optional
`AWS_SESSION_TOKEN`), or instance / IRSA / ECS task credentials.

```bash
cargo build -p constellation --release
BIN=./target/release/constellation

BUCKET=s3://my-bucket/constellation-demo   # empty prefix on first create
mkdir -p /mnt/constellation

$BIN doctor --s3 "$BUCKET"                 # checks If-None-Match / If-Match
$BIN fs create --s3 "$BUCKET"              # once per prefix
$BIN mount --s3 "$BUCKET" /mnt/constellation
# … use /mnt/constellation …
fusermount3 -u /mnt/constellation
```

Local state (metadata DB + chunk cache) defaults to
`~/.local/share/constellation/<fs-uuid>/`; override with `--state-dir`.
Needs FUSE (`fusermount3`) on the host.

## Documentation

| doc | contents |
|---|---|
| [docs/GOALS.md](docs/GOALS.md) | goals, non-goals, motivating scenarios, prior-art critique |
| [docs/DESIGN.md](docs/DESIGN.md) | full architecture: data/metadata planes, leases, pin/offline/epochs, caching, compression, security, failure matrix |
| [docs/DECISIONS.md](docs/DECISIONS.md) | ADRs — what was rejected and why |
| [docs/TESTING.md](docs/TESTING.md) | correctness strategy: property tests, deterministic simulation, POSIX suites, e2e scenarios |
| [docs/ROADMAP.md](docs/ROADMAP.md) | phased milestones to v1 |

## Layout

```
crates/
  cli/        constellation binary (CLI + daemon + FUSE)
  fs-core/    VFS core: inodes, chunking, cache, prefetcher
  meta/       metadata replica, log, partitions, leases, authority
  store-s3/   S3 layout, CAS, codecs, GC
  net/        iroh P2P, gossip, cooperative cache, source selection
  api/        control API + embedded web UI + metrics
bench/
  dbbench/    embedded-DB benchmark behind DECISIONS.md ADR-9
```

## Building

```bash
cargo build --workspace
cargo test --workspace
```
