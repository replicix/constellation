# Configuration

Runtime environment variables. Set them before mounting (or before the
CLI command that reads them); values are typically parsed once at
startup. AWS credentials follow the ordinary AWS SDK chain
(`AWS_REGION`, `AWS_PROFILE`, shared config files, IMDS, etc.) and are
not listed here.

## Table of Contents

- [Environment variables](#environment-variables)
  - [Identity and secrets](#identity-and-secrets)
  - [Leases and mutations](#leases-and-mutations)
  - [Metadata sync and partitions](#metadata-sync-and-partitions)
  - [P2P and cooperative cache](#p2p-and-cooperative-cache)
  - [Prefetch and scan-ahead](#prefetch-and-scan-ahead)
  - [Uploads, staging, and encoding](#uploads-staging-and-encoding)
  - [S3 client](#s3-client)
  - [Existence hints](#existence-hints)
  - [Garbage collection](#garbage-collection)
  - [FUSE and runtime threads](#fuse-and-runtime-threads)
  - [Filesystem stats](#filesystem-stats)
  - [Named filesystems and daemonization](#named-filesystems-and-daemonization)
  - [Control UI](#control-ui)
- [Boolean values](#boolean-values)
- [Build-time](#build-time)
- [References](#references)

## Environment variables

### Identity and secrets

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_NODE_KEY` | `~/.config/constellation/node.key` | path | host Ed25519 key for the P2P endpoint id; one key per host, not per mount |
| `CONSTELLATION_PASSPHRASE` | prompt | string | E2E filesystem passphrase (`fs init`, mount, `fs passwd`); required non-interactively |
| `CONSTELLATION_NEW_PASSPHRASE` | prompt | string | new passphrase for `fs passwd` |

### Leases and mutations

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_LEASE_TTL_MS` | `60000` | milliseconds, positive | S3 lease expiry and renewal |
| `CONSTELLATION_LEASE_IDLE_RELEASE_MS` | `30000` | milliseconds | release an idle held lease |
| `CONSTELLATION_FORWARD_TIMEOUT_MS` | `500` | milliseconds | forwarded mutation request |
| `CONSTELLATION_FORWARD` | `on` | boolean | requester-side mutation forwarding; `off` makes non-holder writes acquire the lease instead |
| `CONSTELLATION_LEASE_PLACEMENT` | `on` | boolean | holder-driven placement |

### Metadata sync and partitions

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | milliseconds | background log tail/ship poll |
| `CONSTELLATION_PART_SPLIT_OPS` | `512` | operations, positive | heat threshold used only when autosplit is on |
| `CONSTELLATION_PART_AUTOSPLIT` | `off` | boolean | heat-driven automatic partition split |
| `CONSTELLATION_PART_MERGE_IDLE_S` | `3600` | seconds | automatic idle partition merge |
| `CONSTELLATION_CHECKPOINT_MIN_INTERVAL_S` | `0` | seconds | minimum checkpoint spacing; `0` disables the time floor |

`CONSTELLATION_PART_AUTOSPLIT` is off unless set to `on`, `1`, or `true`.
When it is off, directory heat does not carve new partitions; idle
children still merge under `CONSTELLATION_PART_MERGE_IDLE_S`. Split/merge
machinery remains for opt-in and for filesystems that already split.
Heat was a proxy for lease contention from before forwarded mutations;
a single-writer tree walk (rsync, unpack) otherwise splits every hot
directory and leaves extra streams to LIST and leases to renew.

### P2P and cooperative cache

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_P2P` | `on` | boolean | complete iroh fast path |
| `CONSTELLATION_P2P_RELAY` | `off` | `off`, `default`/`public`/`n0`, or comma-separated relay URLs | iroh relay policy |
| `CONSTELLATION_P2P_RELAY_TOKEN` | unset | string | optional bearer token for custom relays |
| `CONSTELLATION_COOP` | `on` | boolean | cooperative cache |
| `CONSTELLATION_DIGEST_INTERVAL_S` | `30` | seconds, minimum `1` | cooperative-cache digest gossip |
| `CONSTELLATION_DIGEST_TTL_S` | `4 × DIGEST_INTERVAL_S` | seconds, at least `2 × DIGEST_INTERVAL_S` | how long a peer digest stays usable |

`CONSTELLATION_P2P=off` disables forwarding, placement messages, segment
push, handoff acceleration, and cooperative peer transfer. Correctness and
eventual convergence continue through S3.

### Prefetch and scan-ahead

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PREFETCH_MIN_BYTES` | `8388608` | bytes, positive | initial adaptive sequential-read window |
| `CONSTELLATION_PREFETCH_MAX_BYTES` | `2147483648` | bytes, positive | window ceiling, additionally capped at one quarter of cache budget |
| `CONSTELLATION_PREFETCH_CONCURRENCY` | unset | requests, positive | pin background-fetch concurrency instead of adapting it |
| `CONSTELLATION_PREFETCH_MAX_CONCURRENCY` | `128` | requests, `1..512` | adaptive background-fetch ceiling |
| `CONSTELLATION_SCAN_AHEAD` | `on` | boolean | ordered directory-walk readahead |

### Uploads, staging, and encoding

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_STAGING_BUDGET` | `--cache-size / 4` | bytes | in-flight unflushed write staging cap; exhaustion returns `ENOSPC` |
| `CONSTELLATION_UPLOAD_CONCURRENCY` | unset (adaptive) | requests, positive | pin write-back upload pool size instead of adapting it |
| `CONSTELLATION_UPLOAD_MAX_CONCURRENCY` | `128` | requests, `1..128` | adaptive upload concurrency ceiling |
| `CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S` | `10` | seconds, positive | INFO progress summary period for pending uploads |
| `CONSTELLATION_ENCODE_CONCURRENCY` | `min(CPUs, 8)` | workers, positive | parallel chunk compression/encryption gate |

### S3 client

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_S3_MAX_RETRIES` | object_store default | count | override `RetryConfig.max_retries` for `s3://` backends |
| `CONSTELLATION_S3_RETRY_TIMEOUT_MS` | object_store default | milliseconds | override `RetryConfig.retry_timeout` for `s3://` backends |

### Existence hints

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_EXISTENCE_BLOOM_BYTES` | `4194304` | bytes, minimum `8` | local S3 existence bloom RSS cap |
| `CONSTELLATION_EXISTENCE_LIST` | `on` | boolean | mount-time bucket LIST that seeds the bloom |
| `CONSTELLATION_EXISTENCE_PEER_HINT` | `on` | boolean | use peer digests as existence hints; `CONSTELLATION_COOP=off` also disables this path |

### Garbage collection

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_GC_INTERVAL_S` | `86400` | seconds | background GC tick interval |
| `CONSTELLATION_GC_HORIZON_S` | `604800` | seconds | age before unreferenced chunks are eligible (`0` for tests) |
| `CONSTELLATION_LOG_RETENTION_SEGMENTS` | `128` | segments | sealed log segments kept before GC |

### FUSE and runtime threads

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_FUSE_THREADS` | host heuristic (Linux), else `1` | threads, `1..64` | FUSE dispatcher threads |
| `CONSTELLATION_TOKIO_THREADS` | `min(CPUs, 32)` | threads, `1..32` | multi-thread Tokio worker count |
| `CONSTELLATION_BLOCKING_THREADS` | `clamp(4×CPUs, 4..256)` | threads | Tokio blocking-pool ceiling |

### Filesystem stats and quota

Used space reported by `df`/`statfs` is the logical sum of reachable file
sizes under the mounted view (same semantics as `user.constellation.rsize`
at the mount root), not physical S3 bytes after dedup or compression.

A whole-filesystem mount answers from an in-memory counter maintained on
commit, so it is exact and costs nothing. A subtree or snapshot mount has
to walk its own root instead, and that aggregate is cached:

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_STATFS_TTL_S` | `5` | seconds | cache TTL for a *scoped* mount's `statfs`/`df` used-space aggregate; `0` disables caching. Ignored by a whole-filesystem mount, which needs no cache |

Free space is deliberately *not* scoped to the view: it reports what a
writer can still consume, which is whole-filesystem headroom under the cap
below. `df`'s Size column is therefore used + free, which collapses to the
cap for a whole-filesystem mount.

By default capacity is effectively unbounded. An optional cluster-wide
logical byte cap can be set:

- At create time: `constellation fs create --max-size <SIZE> ...`
- Live on a mount: `constellation quota get|set --state-dir <dir> ...`

`quota set` accepts a byte size (`10G`), or `unlimited` / `0` to clear the
cap. Enforcement is best-effort (local admission check against this node's
replica plus the current inode's uncommitted growth beyond its committed
size); concurrent writers on other nodes can overshoot slightly until
journals catch up. Growth through `write`, `ftruncate`, and `fallocate` is
gated; shrinking never is.

Live quota changes are journaled on `p0` and do **not** rewrite
`meta.json`. The two sources compose by precedence: a replicated
`SetQuota` always wins, and the creation-time `--max-size` applies only
while no such record exists. Each node mirrors `meta.json`'s cap into
node-local state at mount rather than seeding it into the journal, so a
node that has not yet tailed a live change can never re-publish a cap an
operator has cleared.

### Named filesystems and daemonization

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_REGISTRY` | `$XDG_CONFIG_HOME/constellation/registry.toml` | path | local named-filesystem registry file (see [named filesystems](features/named-filesystems.md)) |
| `CONSTELLATION_NO_DAEMONIZE` | unset | boolean-ish (any non-empty value) | force `--foreground` behavior for every `mount` (CI/harness convenience) |

### Control UI

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_WEB_UI_PORT` | `0` (disabled) | TCP port | localhost control UI; bare `--web-ui` listens on `8080` |

## Boolean values

Most boolean switches are enabled when unset. `CONSTELLATION_PART_AUTOSPLIT`
and `CONSTELLATION_P2P_RELAY` default off. `off`, `0`, and `false`
(case-insensitive) disable a switch that defaults on.

## Build-time

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_GIT_DESCRIBE` | `git describe --tags --always --dirty` | string | version string embedded by `crates/cli/build.rs` |

## References

- [Forwarded mutations](features/forwarded-mutations.md)
- [Lease placement](features/lease-placement.md)
- [P2P relays](features/p2p-relays.md)
- [Prefetch](features/prefetch.md)
