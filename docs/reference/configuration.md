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
  - [Metadata sync](#metadata-sync)
  - [Merkle metadata tree (plan 28)](#merkle-metadata-tree-plan-28)
  - [Read-time atime](#read-time-atime)
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
  - [Fault injection (testing only)](#fault-injection-testing-only)
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
| `CONSTELLATION_LEASE_IDLE_RELEASE_MS` | `30000` | milliseconds | idle threshold for releasing a held lease — **only** once a requester is registered |
| `CONSTELLATION_FORWARD_TIMEOUT_MS` | `500` | milliseconds | forwarded mutation request |
| `CONSTELLATION_FORWARD` | `on` | boolean | requester-side mutation forwarding; `off` makes non-holder writes acquire the lease instead |
| `CONSTELLATION_FORWARD_MAX_INFLIGHT` | `64` | count, positive | forwards actually in flight (network round trip + apply) at once per node (plan 29 M5); does not affect correctness, only concurrency — ops queued behind the requester-side ordering gate are unaffected by this bound |
| `CONSTELLATION_LEASE_PLACEMENT` | `on` | boolean | holder-driven placement |

#### Sticky leases

Leases are sticky: a holder with nobody waiting for its partition keeps
it indefinitely, so a node that keeps writing never pays to re-acquire.
`CONSTELLATION_LEASE_IDLE_RELEASE_MS` still sets the idle threshold, but
four conditions now have to hold together before an idle holder gives a
lease back — nothing left to ship, a requester registered, idle for at
least the threshold, and held for at least a 5 s minimum dwell (which
stops two competing writers ping-ponging a partition).

A requester registers itself by appending its node id to the `wanted_by`
list on the lease object with a single `If-Match` swap, which the holder
picks up at its next renewal (at most half a TTL away). This path needs
conditional writes, so it applies to `s3://` backends only; over P2P, a
direct `HandOff` request is the fast path and needs none of it.

The worst case is unchanged by stickiness — a requester waits out the
same idle window it used to, or the same TTL against a genuinely busy
holder — while the common single-writer case loses three S3 round trips
per cold write. See [Diagnose lease
thrash](../how-to-guides/operations/diagnose-lease-thrash.md).

### Metadata sync

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | milliseconds | background log tail/ship poll; the **floor** of the idle backoff |
| `CONSTELLATION_SYNC_IDLE_MAX_MS` | `10000` | milliseconds | **ceiling** of the idle poll backoff |

Plan 29 M0b retired the whole-DB `VACUUM INTO` checkpoint and its
byte-proportional cadence, I/O concurrency knob, and
`CONSTELLATION_CHECKPOINT_SNAPSHOT` escape hatch: a metadata commit (see
below) is a delta of changed keys rather than a copy of the namespace, so
none of that machinery has an equivalent here. The publish cadence is a
plain segment count (`PUBLISH_EVERY = 32`, not currently a knob), plus an
unconditional publish on clean unmount.

### Merkle metadata tree (plan 28)

Writers publish a content-addressed Merkle map of metadata as a chain of
commits (plan 28 option (B)) on the publish cadence above; a fresh
replica bootstraps from the chain's head commit plus a log replay from
its `applied` position, or — a genuinely fresh filesystem with no commit
yet — a replay of the whole log from the beginning (plan 29 M0b).
These knobs size the pack store, the in-memory node cache, and the
commit-chain poll. They are reachable on every non-read-only mount.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PACK_TARGET_BYTES` | `4194304` (4 MiB) | bytes, **1–16 MiB** accepted | sealed pack body size before the writer starts the next pack; out of range falls back to the default |
| `CONSTELLATION_NODE_MEMORY_BYTES` | `67108864` (64 MiB) | bytes; `0` disables | RAM budget for interior metadata nodes; `0` forces every read through the disk cache (used by partial-replica tests) |
| `CONSTELLATION_COMMIT_PROBE_WINDOW` | `8` | positive integer | how many commit slots ahead of the known head one poll probes before falling back to a LIST |

Pack target is the main write-amplification / request-overhead trade-off
for metadata: smaller packs mean more S3 objects and more compaction
work; larger packs mean slower cold directory fetches when a pack spans
unrelated key ranges. The 4 MiB default sits in the measured sweet spot
from plan 28 §P8 / §14.

### Node-local metadata engine (plan 29)

The node-local replica is an `fjall` 3 database (a directory,
`meta.fjall`/`meta.db`), replacing the SQLite engine. One knob:

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_META_CACHE_BYTES` | `268435456` (256 MiB) | bytes | fjall block cache size (`Database::builder(..).cache_size(..)`); the plan 29 benchmarked configuration at 5M entries |

`worker_threads` (fjall's compaction/flush pool) is not an env knob: it
is derived as `min(available_parallelism, 16).max(4)` at open, since
fjall's own default (`min(cores, 4)`) falls behind on compaction under
sustained write churn (plan 29 `RESULTS.md` "fjall 3 tuning"). The `ns`
and `scratch` keyspaces (the §P6-encoded namespace) additionally set
`expect_point_read_hits`, a non-zero data-block hash-ratio, and pin
L0–L2 filter/index blocks resident — not configurable, since they were
chosen from the aged benchmark rather than as a workload-dependent
trade-off.

#### Idle poll backoff

The sync task polls on a deadline that starts at
`CONSTELLATION_SYNC_INTERVAL_MS` and **doubles after every round that
found nothing to do**, clamped at `CONSTELLATION_SYNC_IDLE_MAX_MS`. A
round is productive — and resets the backoff to the floor — when it
applies or ships a segment, or when the journal is non-empty. So does any
request that reaches the sync task: a close/fsync nudge, a barrier, a
lease acquisition, a forwarded mutation, or a gossip `Nudge` from a peer.

An idle node's poll is what it costs the cluster to sit still, and the
poll is no longer a `LIST`: the tailer probes with a **single**
speculative segment `GET`, widens to 16 concurrent GETs as soon as that
hits, and only falls back to a listing when the wide probe also saturates
and it may be far behind. With P2P up the backoff is invisible, because a
peer publishing anything resets it. With P2P **down**, a follower's
worst-case staleness grows from the interval to the ceiling after roughly
five idle rounds (0.5 + 1 + 2 + 4 + 8 s ≈ 15 s of complete quiet) and
snaps back to the interval on the next segment it applies.

The one-wide idle probe is what makes the 10 s ceiling affordable.
`get_run` returns the longest *contiguous* run from the sequence asked
for, so when that sequence is absent — a caught-up node asking "anything
new?" — the other 15 GETs of a 16-wide probe cannot contribute to the
answer. Per node per partition per day, at AWS list price:

| poll | requests/day | GET-equivalents |
|---|---:|---:|
| fixed 500 ms `LIST` (pre-plan-26) | 172,800 | 2,160,000 |
| 30 s ceiling, 16-wide probe | 46,080 | 46,080 |
| 10 s ceiling, 16-wide probe | 138,240 | 138,240 |
| **10 s ceiling, 1-wide probe (shipped)** | **8,640** | **8,640** |

Lower the ceiling further if a P2P-less deployment needs a tighter
freshness bound; raise the floor if request volume matters more than
latency.

A node that currently **holds** a partition lease never backs off past a
quarter of `CONSTELLATION_LEASE_TTL_MS`, whatever the ceiling says: the
sync round is also what renews the lease and what notices another node's
handoff request, so backing off past the renewal cadence would let a
holder sleep through its own renewal.

#### Publish cadence

A publish writes a metadata commit — a delta of the `ns` keys a fjall
write transaction actually touched (plan 29 M2: every write dirties its
own key, in the same transaction, in a `dirty` keyspace the publisher
reads directly), CAS-created as one immutable object — and it is what
licenses log truncation (log retention floors on the head commit's
`applied` position; see below). Plan 29 M0b retired the whole-DB `VACUUM
INTO` checkpoint this superseded, along with its byte-proportional
cadence: a publish costs O(keys changed), not O(database), so there is
nothing to sub-linearly amortize against namespace size the way the old
ratio gate did.

The cadence is:

- a plain count floor — at least 32 shipped segments since the last
  publish (`PUBLISH_EVERY`, not currently a knob);
- an idle timer — `dirty` is non-empty and no publish has run for
  `CONSTELLATION_PUBLISH_IDLE_S` (default 30), checked once per sync
  round after there is nothing left to ship. Without this, a node that
  is mostly idle, or mostly tailing *foreign* segments (which dirty `ns`
  just as surely as a local write, but never advance the shipped-segment
  counter), could leave its head commit — and the log-retention floor
  riding on it — stale indefinitely;
- an unconditional publish on clean unmount (or whenever the mount
  otherwise drains its journal for shutdown), gated only on `dirty`
  being non-empty.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PUBLISH_IDLE_S` | `30` | seconds | idle-publish interval |

### Read-time atime

Optional, best-effort, eventually-consistent access-time updates on
`read()` (plan 20). The default is `off` and behaves exactly as before
(noatime). The mount flag `--atime <off|relatime|lazy>` selects the
mode; `CONSTELLATION_ATIME` overrides the flag. See
[features/atime.md](features/atime.md) for semantics and limits.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_ATIME` | `off` | `off`, `relatime`, `lazy` | read-time atime mode; overrides `--atime` |
| `CONSTELLATION_ATIME_GRANULARITY_S` | `86400` (relatime) / `1` (lazy) | seconds | bump threshold; resolved after the mode is known |
| `CONSTELLATION_ATIME_FLUSH_MS` | `10000` | milliseconds | accumulator flush period |
| `CONSTELLATION_ATIME_MAX_PENDING` | `65536` | distinct inodes | accumulator cap; overflow drops the new entry |
| `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` | `300` | seconds | max delay before an atime-only partition ships on its own |
| `CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS` | `200` | milliseconds | batched-forward timeout to the lease holder |
| `CONSTELLATION_ATIME_SKEW_TOLERANCE_S` | `300` | seconds | clamp applied to a claimed atime on apply |
| `CONSTELLATION_ATIME_RO_FORWARD` | `0` | boolean | let a read-only member forward atime batches |

Atime never blocks a read, never acquires a lease, and never delays an
unmount or lease handoff. A dropped bump only costs freshness.

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
| `CONSTELLATION_EXISTENCE_PEER_HINT` | `on` | boolean | use peer digests as existence hints; `CONSTELLATION_COOP=off` also disables this path |

An upload consults, in order, the peer cache digests, this node's bloom
of hashes it has itself uploaded, and the replica's `chunk_ref` table —
which replay maintains from *foreign* records too, so content referenced
anywhere in the cluster is a hit. A hit means "take a confirming HEAD
first"; the conditional PUT remains the correctness operation either way,
so a hint is never load-bearing.

There is no mount-time bucket LIST and no
`CONSTELLATION_EXISTENCE_LIST` switch to turn one on: seeding the bloom
that way cost 23 s at 59.5k chunk objects and hours at ten million, to
build a filter that only ever saves a HEAD. Nothing can prove a chunk
*absent* without such a scan, so the hint has two answers (hinted / not
hinted), never "proven absent".

### Garbage collection

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_GC_INTERVAL_S` | `86400` | seconds | background GC tick interval |
| `CONSTELLATION_GC_HORIZON_S` | `604800` | seconds | age before unreferenced chunks are eligible (`0` for tests) |
| `CONSTELLATION_LOG_RETENTION_SEGMENTS` | `128` | segments | sealed log segments kept before GC |
| `CONSTELLATION_COMMIT_RETENTION` | `64` | commits, at least 1 | newest plan 28 metadata commits always kept by GC |
| `CONSTELLATION_COMMIT_RETENTION_S` | `86400` | seconds | commits younger than this are kept however many there are; a commit is deleted only when it is outside the newest `CONSTELLATION_COMMIT_RETENTION` *and* older than this |
| `CONSTELLATION_COMPACT_BYTES_PER_S` | `33554432` (32 MiB/s) | bytes per second; `0` unpaced | read budget for metadata pack deletion and compaction in a GC round |
| `CONSTELLATION_GC_THREADS` | one per core | threads, positive | width of the metadata mark and pack rewrite pools |
| `CONSTELLATION_COMPLETION_RETENTION_S` | `900` | seconds | plan 30 M2: how long the node-local `completed` keyspace is retained, and the floor log-segment retention respects regardless of `CONSTELLATION_LOG_RETENTION_SEGMENTS` |
| `CONSTELLATION_HOLDER_CAPTURE` | on | `0`/`off`/`false` to disable | plan 30 M3b internal switch: capture a holder's own journaled writes as speculation (before-images). Off is the performance-gate fallback — a holder with an unshipped journal then defers publishing, and a deposed holder rebuilds its namespace from the shared log instead of rolling back. Not a tuning knob |

Log retention is evaluated against the position a fresh replica resumes
from: the head plan 28 commit's `applied` position. With no commit yet
(a genuinely fresh filesystem), nothing is pruned — the log is the only
copy of history there is. A segment is prunable only once the head
commit's `applied` position is more than
`CONSTELLATION_LOG_RETENTION_SEGMENTS` ahead of it *and* it is older
than `CONSTELLATION_COMPLETION_RETENTION_S` (plan 30 M2's coverage
rule: an in-doubt forwarded op can only be resolved against `completed`
if every segment since it was first sent is still there to have been
tailed, so retention never drops a segment younger than that window no
matter how far ahead the head commit has moved). A node that
re-bootstraps across a gap wider than the retention window cannot
resolve an op stranded in that gap; such an op fails with `EIO` rather
than being retried or re-executed. (A bootstrap from a base the log was
pruned past refuses instead of replaying from the gap.)

Metadata GC (plan 28 S7b) runs as a second phase of every GC round:
commit retention by the two knobs above, a reachability mark from the
retained commits, every snapshot root and live holds, then deletion of
dead packs and compaction of partially dead ones under
`CONSTELLATION_COMPACT_BYTES_PER_S`. Packs to be removed are published in
`gc/condemned-packs.json` and the round waits one lease TTL and re-marks
before touching them; writers never deduplicate against a condemned pack.

Spilled values under `blobs/` (plan 29 M3a) ride the same mark — it
already decodes every `0x01`/`0x03` leaf value it visits and now also
returns the `Payload::Spilled` blob hashes it found — but get a second,
independent two-mark horizon rather than the pack sweep's condemn/wait/
re-mark in one round: a blob unreferenced at a mark is recorded in
`gc/blob-candidates.json` (a bucket object, not node-local kv, so any
node's round can continue another's bookkeeping) with the time it was
first seen unreferenced, and is only condemned once a *previous* round's
sighting is at least `CONSTELLATION_GC_HORIZON_S` old and it is still
unreferenced. Condemned blobs are published in `gc/condemned-blobs.json`
and share the pack sweep's one lease-TTL wait and re-mark; a blob
re-referenced before its horizon elapses drops out of the candidate list
instead of being condemned. `mtree_publish` re-checks this list right
before its commit CAS, mirroring the pack check, because `BlobStore::put`
treats an already-present object as a dedup hit without checking
liveness.

### Retention pruning

Prune policies are stored per subtree in the `user.constellation.prune`
xattr and evaluated by a singleton background pruner. See
[`features/prune.md`](features/prune.md) for the policy grammar.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PRUNE` | `1` | boolean | master switch for the background pruner |
| `CONSTELLATION_PRUNE_INTERVAL_S` | `3600` | seconds | background prune tick interval |
| `CONSTELLATION_PRUNE_GRACE_S` | `86400` | seconds | quiet period after a marked directory's ctime changes |
| `CONSTELLATION_PRUNE_MAX_LAG_S` | `300` | seconds | replica-staleness refusal threshold |
| `CONSTELLATION_PRUNE_SCAN_BUDGET_MS` | `5000` | milliseconds | per-run walk budget before the cursor is saved |
| `CONSTELLATION_PRUNE_FORWARD_TIMEOUT_MS` | `2000` | milliseconds | unlink forward timeout to the lease holder |

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

### Fault injection (testing only)

Knobs with no purpose outside the fault-injection harness
(`crates/harness`) and its scenarios. Never set these against a real
filesystem.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` | `0` (disabled) | milliseconds | sleeps this long, holder-side, immediately before replying to a forwarded mutation — after the op has already executed and the keepers lock has been released. Used to make a forwarded mutation's reply race the requester's own `CONSTELLATION_FORWARD_TIMEOUT_MS` deadline deterministically (plan 30 `forward-timeout-reexec`); logs one `tracing::warn!` at startup when non-zero |

## Boolean values

Most boolean switches are enabled when unset. `CONSTELLATION_ATIME_RO_FORWARD`
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
- [Read-time atime](features/atime.md)
