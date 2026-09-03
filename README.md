# Constellation

A distributed POSIX file system on S3: any number of machines mount the same
bucket read/write, stay conflict-free by construction, keep working offline,
and use encrypted P2P links between peers purely as a latency fast path.
One static binary — FUSE mount, daemon, CLI, and web UI. The only
infrastructure is an S3-compatible bucket with conditional-write support.

**Status:** phases 1–8 are implemented: single- and multi-node mounts,
leases and offline epochs, cooperative caching, snapshots, E2E encryption,
the web UI, fsck/GC, and release hardening. Linux is the fully exercised
platform; macOS receives build and mount-less test coverage, while FUSE
mount behavior requires macFUSE and remains less battle-tested. Automated
crash reporting is future phase-9 work. See
[docs/PROGRESS.md](docs/PROGRESS.md) for detailed evidence and limitations.

## Quick start: mount a real S3 bucket

S3 auth uses the official AWS SDK default credential chain via
[`aws-config`](https://docs.rs/aws-config) (same as the AWS CLI / SDKs):

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`)
- Shared config/credentials files (`~/.aws/config`, `~/.aws/credentials`)
- Named profiles via `AWS_PROFILE` (including SSO after `aws sso login`)
- EC2 instance role / ECS task role / EKS IRSA / process credentials

Region comes from the profile or `AWS_REGION` / `AWS_DEFAULT_REGION`
(defaults to `us-east-1`). Localstack-style knobs such as `AWS_ENDPOINT`
and `AWS_ALLOW_HTTP` still work.

```bash
export AWS_PROFILE=my-profile          # or rely on env keys / instance role
# aws sso login --profile my-profile   # when the profile uses SSO
cargo build -p constellation --release
BIN=./target/release/constellation

BUCKET=s3://my-bucket/constellation-demo   # empty prefix on first create
mkdir -p /mnt/constellation

$BIN doctor --s3 "$BUCKET"                 # checks If-None-Match / If-Match
$BIN fs create --s3 "$BUCKET"              # once per prefix
$BIN mount --s3 "$BUCKET" /mnt/constellation
$BIN status                                # from another shell
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
| [docs/RELEASING.md](docs/RELEASING.md) | release artifacts and tag checklist |

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
