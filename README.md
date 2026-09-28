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
crash reporting remains future work. See
[docs/plans/v1/PROGRESS.md](docs/plans/v1/PROGRESS.md) for the v1
implementation evidence.

Current write-path features include:

- **Forwarded mutations:** non-holders send writes to the current lease holder
  over iroh, avoiding handoff churn while S3 CAS remains authoritative.
- **Scratch directories:** mark a shared directory with
  `user.constellation.scratch=1` for node-private temporary files, then rename
  a completed regular file out to publish it atomically.

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

$BIN doctor demo --s3 "$BUCKET"            # checks If-None-Match / If-Match
$BIN fs create demo --s3 "$BUCKET"         # registers "demo", once per prefix
$BIN mount demo /mnt/constellation         # backgrounds itself by default
$BIN status demo                           # from another shell
# … use /mnt/constellation …
$BIN umount demo
```

`demo` is a name registered on this machine (like `zpool`/`zfs`), resolved
through `$XDG_CONFIG_HOME/constellation/registry.toml`
(`CONSTELLATION_REGISTRY` overrides it) — see
`docs/reference/features/named-filesystems.md`. `mount demo:/sub MOUNTPOINT`
mounts just a subtree, and a bare `mount demo` (no mountpoint) brings up
every view already registered for it. `constellation export demo` is the
only way to un-register a name (leaves the cluster, detaches every view,
deletes its state dir). For an ad-hoc, unregistered mount — no name, no
registry writes — pass `--s3`/`--state-dir` with a literal path instead:
`$BIN mount / /mnt/constellation --s3 "$BUCKET" --state-dir /tmp/scratch`.
Add `-f`/`--foreground` to any `mount` to skip backgrounding (useful for
debugging or supervising it with another process manager).

Local state (metadata DB + chunk cache) defaults to
`$XDG_DATA_HOME/constellation/<name>/` for a registered name, or
`~/.local/share/constellation/<fs-uuid>/` for an unregistered one; override
with `--state-dir`. Needs FUSE (`fusermount3`) on the host.

On Linux, request handling and the Tokio runtime size themselves from the
CPU quota visible to the process. FUSE workers grow as roughly `2×sqrt(CPUs)`
(1 worker on a 1-CPU host, 12 on 32 CPUs, capped at 64), then shrink further
if their 16 MiB request buffers would consume more than one eighth of the
host or cgroup memory limit. Runtime workers scale linearly to 32 CPUs.
`CONSTELLATION_FUSE_THREADS`, `CONSTELLATION_TOKIO_THREADS`, and
`CONSTELLATION_BLOCKING_THREADS` provide explicit positive-integer overrides,
bounded at 64, 32, and 256 respectively. The selected plan is logged at
startup; blocking workers are created lazily rather than at startup. The FUSE
kernel background queue scales with the worker count, and Linux parallel
directory dispatch is requested when the kernel supports it.

## Documentation

Docs follow [Diátaxis](https://diataxis.fr/). Start at
[docs/README.md](docs/README.md).

| doc | contents |
|---|---|
| [docs/explanation/GOALS.md](docs/explanation/GOALS.md) | goals, non-goals, motivating scenarios, prior-art critique |
| [docs/explanation/DESIGN.md](docs/explanation/DESIGN.md) | full architecture: data/metadata planes, leases, pin/offline/epochs, caching, compression, security, failure matrix |
| [docs/explanation/DECISIONS.md](docs/explanation/DECISIONS.md) | ADRs — what was rejected and why |
| [docs/how-to-guides/development/TESTING.md](docs/how-to-guides/development/TESTING.md) | correctness strategy: property tests, deterministic simulation, POSIX suites, e2e scenarios |
| [docs/how-to-guides/development/RELEASING.md](docs/how-to-guides/development/RELEASING.md) | release artifacts and tag checklist |
| [docs/reference/configuration.md](docs/reference/configuration.md) | runtime environment variables and defaults |
| [docs/reference/features/named-filesystems.md](docs/reference/features/named-filesystems.md) | the local registry, one daemon per name, daemonization, `export` |
| [docs/plans/v1/](docs/plans/v1/) | v1 roadmap, progress, and implementation plans (`done/` / `wip/`) |

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
  dbbench/    embedded-DB benchmark behind docs/explanation/DECISIONS.md ADR-9
```

## Building

```bash
cargo build --workspace
cargo test --workspace
```

## License

Constellation is released under the [Mozilla Public License 2.0](LICENSE)
(SPDX: `MPL-2.0`). Modified copies of its source files must be published
under the same license when distributed; combining it with code under other
licenses is allowed.
