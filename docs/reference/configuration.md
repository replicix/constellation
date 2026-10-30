# Configuration

Mount options, per-filesystem settings, and runtime environment
variables. Set environment variables before mounting (or before the CLI
command that reads them); values are typically parsed once at startup.
AWS credentials follow the ordinary AWS SDK chain (`AWS_REGION`,
`AWS_PROFILE`, shared config files, IMDS, etc.) and are not listed here.

## Table of Contents

- [Mount and filesystem options](#mount-and-filesystem-options)
  - [Mount flags](#mount-flags)
  - [Read-path transports at a glance (plan 38)](#read-path-transports-at-a-glance-plan-38)
  - [FUSE passthrough](#fuse-passthrough)
  - [Per-filesystem settings](#per-filesystem-settings)
  - [Precedence](#precedence)
  - [Plan 30 commands](#plan-30-commands)
- [Environment variables](#environment-variables)
  - [Identity and secrets](#identity-and-secrets)
  - [Leases and mutations](#leases-and-mutations)
  - [Exactly-once and speculation](#exactly-once-and-speculation)
  - [Sessions, close-to-open, and log streams](#sessions-close-to-open-and-log-streams)
  - [Backups, acknowledgement, and failover](#backups-acknowledgement-and-failover)
  - [Continuation epochs](#continuation-epochs)
  - [Delegation and placement](#delegation-and-placement)
  - [The S3 inbox](#the-s3-inbox)
  - [Cluster locks](#cluster-locks)
  - [Metadata sync](#metadata-sync)
  - [Merkle metadata tree (plan 28)](#merkle-metadata-tree-plan-28)
  - [Node-local metadata engine (plan 29)](#node-local-metadata-engine-plan-29)
  - [Read-time atime](#read-time-atime)
  - [P2P and cooperative cache](#p2p-and-cooperative-cache)
  - [Prefetch and scan-ahead](#prefetch-and-scan-ahead)
  - [Uploads, staging, and encoding](#uploads-staging-and-encoding)
  - [S3 client](#s3-client)
  - [Existence hints](#existence-hints)
  - [Garbage collection](#garbage-collection)
  - [Retention pruning](#retention-pruning)
  - [FUSE and runtime threads](#fuse-and-runtime-threads)
  - [Listing and `READDIRPLUS` in the status](#listing-and-readdirplus-in-the-status)
  - [Filesystem stats and quota](#filesystem-stats-and-quota)
  - [Named filesystems and daemonization](#named-filesystems-and-daemonization)
  - [Logging](#logging)
  - [Control UI](#control-ui)
  - [Control allowlist (plan 31 §9.5 / plan 33 U1)](#control-allowlist-plan-31-95--plan-33-u1)
  - [Fault injection (testing only)](#fault-injection-testing-only)
- [Boolean values](#boolean-values)
- [Build-time](#build-time)
- [References](#references)

## Mount and filesystem options

### Mount flags

The `mount` flags that choose consistency, durability and lock
semantics. Plan 30 added `--cto` and `--locks`; the acknowledgement
policy is the filesystem's (`fs create --ack-policy`, below), not a
mount flag. `--fsync-mode` and `--write-mode` are older and are listed
because they combine with it. See [Durability and failover](features/durability-and-failover.md),
[Close-to-open modes](features/cto-modes.md) and
[Cluster locks](features/cluster-locks.md).

| Flag | Default | Values | Env default | Persisted in the registry | Meaning |
|---|---|---|---|---|---|
| `--cto` | `bounded` | `bounded`, `strict` | `CONSTELLATION_CTO` | no | close-to-open mode (plan 30 M8). `strict`: an open, lookup or listing sees every close another node completed before it began |
| `--locks` | `cluster` with P2P, `local` without | `local`, `cluster` | `CONSTELLATION_LOCKS` | no | `flock`/`fcntl` scope (plan 30 M14). An explicit `cluster` with P2P off fails the mount |
| `--cache-verify` | `admit` | `admit`, `always` | `CONSTELLATION_CACHE_VERIFY` | no | when a disk-cache read re-hashes the chunk file it read (plan 38 §2.3). Every chunk is blake3-verified exactly once regardless — **while it streams in** from S3 or a peer, before it is ever admitted. `admit`: the local copy is trusted afterwards; a file a restart's directory scan found is still hashed on its first read (and then trusted). `always`: every disk read hashes again, so local corruption after admission (a bad sector, a scrub miss, a writer into the cache directory) is caught on the next read instead of at the next restart, at one blake3 pass per read. Echoed in `node.status` as `cache.cache_verify` (node-wide, so the per-mount `fuse` section does not repeat it); an unparseable `CONSTELLATION_CACHE_VERIFY` is warned about and ignored (see below the table), while an unparseable `--cache-verify` fails the mount before the daemon forks. `always` also turns FUSE passthrough off (see [FUSE passthrough](#fuse-passthrough) below; `node.status` reports the reason `cache_verify_always`), and zero-copy reads (the mount negotiates plain `uring` instead of `uring_zc`, plan 38 Z4b): both let the kernel serve a chunk file without the daemon seeing the bytes |
| `--fuse-transport` | `auto` (`dev-fuse` under `CONSTELLATION_PROFILE=mobile`) | `auto`, `dev-fuse` | `CONSTELLATION_FUSE_TRANSPORT` | no | Which transport this daemon's **plain** Linux mounts are served over (plan 38 §2.4). `auto`: run the ladder — FUSE-over-io_uring when the binary carries the `io-uring` cargo feature (every Linux build of `constellation` since plan 38 Z2c), the kernel is 6.14+ with `fuse.enable_uring=Y`, and the sandbox permits `io_uring_setup(2)`; `/dev/fuse` otherwise, logged once rather than failing the mount. **A mount with cluster locks (`--locks cluster`, the default with P2P) takes the ladder too** (decided 2026-10-05; see the note below the table), accepting the ring's lock-wait budget — a contended blocking `fcntl`/`flock` lock past `depth - 1` waiters on one CPU's queue is answered **`ENOLCK`** instead of waiting (counted in `lock_wait_downgrades`) — on a deeper queue (32) unless `--fuse-uring-queue-depth` says otherwise. `dev-fuse`: `read(2)`/`writev(2)` on `/dev/fuse`, every kernel and every platform. Runtime-negotiated, never a build-time choice, and fixed for a connection's life: what a mount actually got is `node.status`'s per-mount `transport` (`dev_fuse`/`uring`/`uring_zc`, in `mounts[]` and in `fuse.mounts[]`), and the label `transport` on that mount's `constellation_vfs_ops_total`/`constellation_vfs_op_seconds` rows. A mount that asked for the ring (`auto`) and got `/dev/fuse` records why in `fuse.mounts[].last_fallback` — `reason`, the first rung that refused, in this order: `no_io_uring_feature`, `kernel_not_offered`, `handover_capable`, `ring_setup_failed`; with a free-text `detail` and the time — and counts once in `constellation_fuse_transport_fallbacks_total{from,to,reason}`; `dev-fuse` asked for no ring and records no fallback. **A mount served over a ring cannot be handed to another process image**, so `daemon --upgrade`/`node.handoff` refuses it by name (plan 38 §3(e)/Z0a) — unmount and remount is the only upgrade path for such a mount. A `view.mount` with a mountpoint path — on a daemon or a headless `constellation serve` node, a CSI engine pod's included — is a plain mount and follows this setting like any other (on `serve`, through `CONSTELLATION_FUSE_TRANSPORT` and the profile). Mounts that are handover-capable by construction (`view.mount` on a descriptor somebody else opened, and anything an upgrade resumes) are `dev_fuse` whatever this says — they report `handover_capable` where the ring would otherwise have been granted, and an earlier rung's reason (the kernel, the build) where it would not |
| `--fuse-uring-queue-depth` | `8`; `32` for a mount with cluster locks | a positive integer | `CONSTELLATION_FUSE_URING_QUEUE_DEPTH` | no | ring entries per kernel queue, read only when a mount's transport resolves to a ring; when set it applies to every ring mount of the daemon. The reserved buffer is `possible CPUs x depth x (max_write + a page)` of `MAP_NORESERVE` address space per mount (plan 38 §4) — resident only as far as traffic touches it — so this is the knob for an operator who has measured their own trade-off. It also bounds blocking lock waits: at most `depth - 1` of one queue's entries wait for a lock (see the note below the table), so a depth of `1` serves every contended blocking `fcntl`/`flock` lock with `ENOLCK`. Reported per mount as `node.status`'s `fuse.mounts[].uring_queue_depth` and `/metrics`' `constellation_fuse_uring_queue_depth{mountpoint,transport}` — `0` on a `/dev/fuse` mount, which has no ring queues. Handover-capable mounts never use it (they are `dev_fuse`). The reservation needs the address space even though it stays non-resident: a depth-32 mount reserves about 16 GiB on a host with 32 possible CPUs (measured, plan 38 Z2 in PROGRESS), so under `vm.overcommit_memory=2` (strict accounting) or a tight `RLIMIT_AS` (`ulimit -v`) the ring's setup fails and the mount comes up on `/dev/fuse` with the fallback reason `ring_setup_failed` — lower this knob (or raise the limit) there |
| `--fsync-mode` | `local` | `local`, `s3` | none | yes | what `fsync()` waits for. `local`: every chunk of the file still queued on this node is in the bucket (whoever wrote it, including a writer that already closed it under `--write-mode back`; on the node that sequenced a `back` close made elsewhere, also the chunks that writer has not uploaded yet) and the node's metadata store is forced to disk. `s3`: also the journal up to the call is in the bucket. The same in a continuation epoch: an `fsync` waits for the bucket (or a peer's chunk handoff) there too (plan 39b; before, `local` uploaded only what the `fsync` itself published, and nothing inside an epoch). `s3` also forces `--write-mode through`. Whatever it waits for, it waits like an NFS `hard` mount (plan 39): a *transient* S3 failure (timeout, refused or reset connection, 5xx, `SlowDown`, …) is retried with backoff until the data is durable, never answered `EIO`; a failure waiting cannot fix (`AccessDenied`, `NoSuchBucket`, a disabled KMS key, a queued chunk lost from this node's disk and not in the bucket, …) is `EIO` at once. Killable, not interruptible (as NFS `hard`): a signal the process handles (a timer, a caught `SIGINT`/`SIGTERM`) leaves it waiting, and only a signal that kills the process (`SIGKILL`, an unhandled default-fatal signal) ends the wait (`EINTR`, which the dying process never sees) — detected from the caller thread's pending `SIGKILL` in `/proc/<tid>/status`, so a caller the daemon cannot see there (another pid namespace, `hidepid`) is not killable until S3 returns or a timeout below ends the wait. An `O_SYNC`/`O_DSYNC` write waits the same way and answers `EIO` when cut short. In every failed case the data stays pending and the next `fsync` waits for it again. `fsyncdir` (an `fsync` of a directory) is the same barrier for the directory's entries |
| `--fsync-timeout` | unset (wait until durable) | a duration: `500ms`, `30s`, `2m`, or a bare number of seconds; `0`/`off`/`hard` = wait until durable (on the flag this overrides `CONSTELLATION_FSYNC_TIMEOUT`) | `CONSTELLATION_FSYNC_TIMEOUT` | no | **the opt-in `soft` mode** (plan 39). Bounds how long an `fsync` (and an `O_SYNC`/`O_DSYNC` write) waits for an unreachable S3; when it elapses the call fails `EIO` with the data still pending: it keeps uploading in the background, and the next `fsync` waits for it again. **Warning**, as nfs(5) says of `soft`: use it "only when client responsiveness is more important than data integrity". Nothing is dropped, but an application that treats `EIO` from `fsync` as "the data is lost" (and acts on it — retries elsewhere, reports failure, PostgreSQL PANICs) gets that outcome for data that was merely late. Node-wide; an invalid flag fails the mount before the fork, an invalid env value is warned about and ignored. Reported as `node.status.fsync.mode` (`hard`/`soft`) and `timeout_ms`. Independently of it, when the host sets the kernel's FUSE request timeout (`fs.fuse.default_request_timeout` / `max_request_timeout`, Linux 6.15+), every `fsync` wait is capped just below it (`t − min(t/5, 5 s)`, logged once at warn, `node.status.fsync.kernel_cap_ms`): past that the kernel would abort the whole connection, failing every open file of the mount |
| `--write-mode` | `through` | `through`, `back` | none | yes | chunk close policy. `through`: `close()` returns once the file's chunks are in S3 and its manifest is committed at the sequencer (one S3 round trip for a small file). `back`: `close()` returns once the chunks are queued durably on this node's disk and the manifest is committed at the sequencer (no S3 round trip, on the sequencer and on any other node); the bytes live only on this node until the upload drains. `fsync`, `O_SYNC`, `O_DSYNC`, `--fsync-mode s3` and a cluster lock's release always act as `through`. `constellation write-mode TARGET MODE` changes it on a running mount (switching to `through` drains the queue). Use `back` for bulk imports (untar, rsync, `cp -r`) and switch back afterwards; see [When to use `--write-mode back`](features/durability-and-failover.md#when-to-use---write-mode-back) |

Values are case-insensitive for `--cto`, `--locks`, `--cache-verify` and
`--fuse-transport`; any other value fails the mount, and so does an
unparseable `CONSTELLATION_FUSE_TRANSPORT` or
`CONSTELLATION_FUSE_URING_QUEUE_DEPTH` — a transport is not a safety
setting whose stricter reading survives a typo, and silently ignoring
`auto` would leave the kernel's fast path off, which is the opposite of
what was asked for. The one exception is
`CONSTELLATION_CACHE_VERIFY`: an unparseable value there is **warned about
and ignored** (the flag, or the default, applies), so a stale value in a
shell profile cannot brick every mount started from it — `mount` prints the
warning and the mode it is using, the daemon logs it, and
`node.status`'s `cache.cache_verify` reports the mode that took effect.

The flags that are not persisted apply to
the mount command that carries them: a later bare `mount NAME` uses the
environment or the default again. `--fuse-transport` and
`--fuse-uring-queue-depth` are the one pair that is **node-wide rather than
per-view**: they belong to the daemon, so a `mount` that merely *attaches* a
second view to an already-running daemon cannot change them (that daemon
resolved them when it started, and a connection's transport is fixed for its
life); such a `mount` prints a warning and carries on with the daemon's.
Stop the daemon, or set the environment variable before the mount that
starts it.

### Read-path transports at a glance (plan 38)

Every knob that decides how a Linux mount's reads reach the kernel (plan 38),
in one place; each is explained in the rows and sections around it. All are
**daemon-wide**, read once when the daemon starts, and none exists on other
platforms (macOS, Windows and Android mounts do not go through `/dev/fuse`).

| Knob | Default | Values | Needs |
|---|---|---|---|
| `--cache-verify` / `CONSTELLATION_CACHE_VERIFY` | `admit` | `admit`, `always` | nothing. `always` turns passthrough and zero-copy off |
| `--fuse-transport` / `CONSTELLATION_FUSE_TRANSPORT` | `auto`; `dev-fuse` under `CONSTELLATION_PROFILE=mobile` | `auto`, `dev-fuse` | the ring: kernel 6.14+ with `fuse.enable_uring=Y`, `io_uring_setup(2)` allowed by the sandbox (Docker's default seccomp denies it). No privilege |
| `--fuse-uring-queue-depth` / `CONSTELLATION_FUSE_URING_QUEUE_DEPTH` | `8`; `32` for a mount with cluster locks | positive integer | a mount that gets the ring |
| `CONSTELLATION_FUSE_PASSTHROUGH` | unset: read-only mounts only | `1`/`on`/`true`/`yes`, `0`/`off`/`false`/`no` | kernel 6.9+ (`CONFIG_FUSE_PASSTHROUGH`), `CAP_SYS_ADMIN` in the initial user namespace, a cache directory on a filesystem the kernel accepts as a backing file (not overlayfs). Works on every transport and without the `io-uring` feature |
| `CONSTELLATION_FUSE_URING_ZERO_COPY` | `off` | `off`, `auto`, `pinned` | the ring, kernel 7.3+ with io_uring buffer pools, `CAP_SYS_ADMIN` |
| `CONSTELLATION_FUSE_ZERO_COPY_MIN_READ` | `512k` | bytes, `<n>k`, `<n>m` | a `uring_zc` mount |

**Precedence.** For `--fuse-transport` and `--fuse-uring-queue-depth` the
**environment wins over the flag**, and the flag over the default (the engine
profile's: `auto`, or `dev-fuse` on `mobile`), so one host can be put on one
transport without editing what starts the daemon; `--cache-verify` is the same
(`CONSTELLATION_CACHE_VERIFY` wins over the flag). The passthrough and
zero-copy knobs are environment-only. An unparseable value fails the mount,
**except** `CONSTELLATION_CACHE_VERIFY`, which is warned about and ignored
(see above). The transport and queue depth belong to the daemon: a `mount`
that only attaches a view to a running daemon cannot change them.

**What a mount gets, in order** (the first rule that applies wins; the
per-mount `node.status` `transport` and `last_fallback` say which did):

1. **Handover-capable mounts are `dev_fuse`, whatever the knobs say**: a
   `view.mount` on a descriptor somebody else opened (plan 37's CSI engine pods)
   and every mount `daemon --upgrade` resumes. A ring session cannot be handed
   to another process image, so these never take the ring (`handover_capable`).
2. **The mobile profile defaults to `dev-fuse`**: the ring's buffers are not
   worth a phone's memory budget. `CONSTELLATION_FUSE_TRANSPORT` still overrides
   it on a Linux host running that profile.
3. **`dev-fuse`** asks for no ring. **`auto`**
   asks for the ladder for every plain mount, a mount with cluster locks (the
   default with P2P) included, which gets queue depth 32 instead of 8.
4. The ladder takes the ring where the build, kernel and sandbox grant it and
   falls back to `/dev/fuse` otherwise, logged once with the reason, never as a
   failed mount. Zero-copy queues are added only when
   `CONSTELLATION_FUSE_URING_ZERO_COPY` is `auto` or `pinned`, and the mount
   is then `uring_zc`.

Whether `auto` should also give cluster-lock mounts the ring was an open
decision after plan 38; the project owner **decided yes on 2026-10-05**
(plan 38 close-out in `docs/plans/v1/PROGRESS.md`): such a mount takes the
ladder like any other, with the lock-wait budget described in the note below.

**FUSE passthrough** (plan 38 §3(c): the kernel reads a single-chunk file
straight from its cached chunk) is on by default for **read-only** mounts
(snapshot views) where the kernel and the process allow it, and off for
writable mounts unless opted in; see [FUSE passthrough](#fuse-passthrough) below for its
knob, `CONSTELLATION_FUSE_PASSTHROUGH`, and what it changes. Zero-copy reads
(plan 38 Z4b, below) are counted (`fuse.mounts[].zero_copy_reads`,
`constellation_fuse_zero_copy_reads_total`).

**io_uring zero-copy queues** (plan 38 Z4a), **opt-in**: with
`CONSTELLATION_FUSE_URING_ZERO_COPY=auto` (or `pinned`) a mount that gets
the ring also sets its queues up for zero-copy where the kernel offers
buffer pools (7.3+) and the daemon has `CAP_SYS_ADMIN`; it then reports the
transport `uring_zc` instead of `uring` (`fuse.mounts[].transport`, the
`transport` label). On such a mount (plan 38 Z4b) a **read-only open** of a
file of at least `CONSTELLATION_FUSE_ZERO_COPY_MIN_READ` bytes that no
handle can write and that has no unpublished writes is marked for
zero-copy (a frozen snapshot file too), and each of its reads of at least
that size that lies inside **one chunk** whose file is in the disk cache,
was verified by this daemon is
answered by the kernel reading that chunk file straight into the reader's
pages (one `READ_FIXED`; buffered and `O_DIRECT` alike) — no copy through
the daemon. Every other read — a smaller one, one crossing a chunk
boundary, of a chunk not cached yet, of a file with
local writes pending — is answered from the memory cache or a fetch, as on
`uring`, on the same mount. While a zero-copy read is in flight, and while
a marked handle stays open for the last four chunks it read, the chunk is
pinned in the disk cache, like a passthrough open's (`cache.open_pins`).
Passthrough, where a mount has it, still wins for the single-chunk files it
serves. `--cache-verify always` turns zero-copy off: the mount does not
even set up zero-copy queues and negotiates `uring`. The other requests'
payloads travel in the queues' buffer pools — which cost memory. Such a
mount (and one that tried zero-copy and was refused) negotiates `max_write`
= one request's pages, 1 MiB, instead of 16 MiB; no request changes size.

**When to turn it on.** Measured with `tests/read-cpu-gate.sh` (plan 38
Z4b, root, 32 vCPU, kernel 7.3.0-rc4, 4 MiB chunks, 1 GiB memory tier;
daemon CPU-seconds per GiB read, median of 3, a loaded host; PROGRESS "Plan 38 Z4"):

| lane | `uring` | `uring_zc` | |
|---|---|---|---|
| cold sequential, 1 MiB buffered | 1.86 CPU-s/GiB, 1 014 MiB/s, 856 MiB RSS | 1.52, **1 459 MiB/s, 354 MiB RSS** | every read zero-copy |
| warm disk, 1 MiB buffered | 0.64, 1 043 MiB/s | 0.70, 957 MiB/s | chunk files read zero-copy |
| warm memory, 1 MiB `O_DIRECT` | 0.10, 4 163 MiB/s | 0.18, 1 759 MiB/s | zero-copy even though the memory tier holds the chunk |
| 4 KiB random `O_DIRECT` | 3.41 | 5.86 | below the threshold: bytes |
| 64 KiB files, cold | 7.12 | 6.98 (earlier run) | files below the threshold are not marked |

A zero-copy read wins where the alternative is a fetch into the memory
tier (cold, streaming reads of large files: higher throughput at under half
of the daemon's memory). A read of at least the threshold goes zero-copy
whether or not the memory tier holds the chunk: on a marked open, a read
answered with bytes is bounced into the kernel's registered pages one copy
more than a `READ_FIXED` costs. Reads below the threshold are still answered
with bytes and pay that bounce, which is what the 4 KiB lane shows. Hence
`off` by default; turn it on for mounts that mostly stream large files once (media,
backups, datasets read through), not for ones that re-read a working set.

| Knob | Default | Meaning |
|---|---|---|
| `CONSTELLATION_FUSE_ZERO_COPY_MIN_READ` | `512k` | On a `uring_zc` mount, the smallest read answered zero-copy and the smallest file whose open is marked (`<n>`, `<n>k`, `<n>m`, binary units; `0`: every read and file). A zero-copy read of a chunk file measured costlier than the memory hit it replaces below 512 KiB (64 KiB +35%, 128 KiB +15%, 256 KiB +5–14%) and the same or cheaper from 512 KiB up (`tests/read-cpu-gate.sh --size-lanes`). An unparseable value fails the mount. Read once, when the daemon starts |
| `CONSTELLATION_FUSE_URING_ZERO_COPY` | `off` | `off` (the default): no zero-copy queues and no pools; the mount negotiates `uring` (see "When to turn it on" above). `auto`: zero-copy queues where allowed, their buffer pools handed to the kernel **unregistered**: nothing is resident at mount time, a pool page becomes resident when a request first uses it and **is never given back**, so a busy mount converges on the whole pool — **possible CPUs x queue depth x 1 MiB per mount**: 256 MiB at 32 CPUs and depth 8 (the default), 1 GiB at depth 32 (a mount with cluster locks); with 8 CPUs, 64 MiB and 256 MiB. `pinned` (opt-in): the same pools registered with io_uring, all of that resident and pinned **from the moment the mount is made**, charged to `RLIMIT_MEMLOCK` unless the daemon has `CAP_IPC_LOCK`; it saves the kernel a per-request import of the buffer. Any other value fails the mount, as an unparseable `CONSTELLATION_FUSE_TRANSPORT` does. Read once, when the daemon starts. A kernel, capability or limit that refuses is not a failure: the mount negotiates `uring`, and a refusal after the attempt began is logged once (`io_uring zero-copy unavailable: …`) |

> **Note — the ring transport, and what it means for cluster-lock
> mounts.** Since plan 38 Z2c `auto` is the default for plain mounts
> (the mobile profile excepted). It only reaches the ring on a kernel with
> `fuse.enable_uring=Y` (6.14+) whose sandbox permits `io_uring_setup(2)`.
> Over the ring, a ring thread dispatches only reads of data and metadata
> itself (`read`, `getattr`, `readlink`, xattr reads, `statfs`, `access`).
> Everything that can wait on S3, a lease or another node — opens, writes,
> `close`'s flush/release, `fsync`, namespace changes, locks, lookups and
> directory listings — runs on a pool of `n_threads` offload threads, and a
> read that would wait for the store or for another op on the same file
> defers to the engine's completion pool. What stays on the ring thread can
> still wait a bounded time on its own node, as a read always could: the
> session wait for this node's own writes (at most
> `CONSTELLATION_SESSION_WAIT_MS`) and the view's admission gate when
> `max_inflight_ops` is set.
>
> **What a ring cannot do that `/dev/fuse` can: let a waiting request hold
> nothing.** Over a ring every request holds its entry until it is answered
> — including one answered later from another thread: a cold read on the
> completion pool, an offloaded flush waiting on S3, and a blocking
> `F_SETLKW`/`flock` handed to the view's `lock-wait` thread. The kernel
> queues a request on the queue of the CPU that issued it and, while all of
> that queue's entries are held, the request waits there; it cannot be
> answered on another queue or over `/dev/fuse`. For waits that end by
> themselves (the store, a lease, the session wait) that is a stall of that
> CPU's requests for as long as the wait. For a blocking lock it would be a
> **deadlock**: `depth` processes on CPU *k* blocked in `F_SETLKW` on a lock
> held by process *P*, *P* on CPU *k* issues a `write()` before its unlock —
> the write waits for an entry only the unlock could free, and every later
> request from CPU *k* hangs with it. So at most `depth - 1` entries of a
> queue may be held by blocking lock requests; a further one on that queue
> is served without waiting — granted if the lock is free, **`ENOLCK`** ("no
> lock resources") if it is contended — instead of taking the queue's last
> entry. Every such downgrade is counted: `node.status`'s
> `fuse.mounts[].lock_wait_downgrades` and `fuse.lock_wait_downgrades_total`,
> and `/metrics`' `constellation_fuse_lock_wait_downgrades_total`.
> `/dev/fuse` mounts never do this, and mounts under `--locks local` are
> unaffected (the kernel keeps their locks and never sends a blocking lock
> request).
>
> A cluster-lock mount is exactly the one that sends blocking lock requests.
> Plan 38 Z2c kept it on `/dev/fuse` under `auto` (a `cluster_locks`
> fallback) with a separate opt-in; the project owner **decided on
> 2026-10-05** that `auto` puts it on the ring like any plain mount, accepting
> the `ENOLCK` behaviour above for a 2–3x cut in daemon CPU on reads, and the
> `cluster_locks` rung is gone. Such a mount gets a deeper default queue
> (32 entries, so 31 contended waiters per CPU before the next is refused,
> against 7 at the ordinary depth 8; the price is address space, not
> memory — see plan 38 §4 and PROGRESS "Plan 38 Z2"); an explicit
> `--fuse-uring-queue-depth` wins. Where the applications on a mount pile
> dozens of contended blocking locks onto one CPU and do not handle `ENOLCK`
> by retrying, mount with `--fuse-transport dev-fuse` (or `--locks local`
> where the locks need not be cluster-wide); what the lock scenarios measured
> under the new default is in PROGRESS, "cluster-lock-ring-auto".
>
> **What a ring holds is reported by the ring itself.** A request on the
> ring is first held in its entry, then (unless it is a read of data or
> metadata) queued for one of the mount's worker threads before the view
> sees it, so under overload `fuse_requests` can show nothing in flight while
> callers wait. Each ring session therefore reports, per mount in
> `node.status`'s `fuse.mounts[]` (and `/metrics`, labelled `mountpoint`):
> `ring_entries_held_long` (`constellation_fuse_ring_entries_held_long`), the
> entries held in userspace past `CONSTELLATION_FUSE_REQUEST_STALL_S`
> (default 30 s; 0 turns this watchdog off), blocking locks aside, each also
> logged once with its opcode, state and a per-queue census; and
> `ring_stranded_commits` (`constellation_fuse_ring_stranded_commits_total`),
> replies whose ring thread was never woken for them. A ring thread's wait
> for the kernel is bounded at 1 s, so such a reply is flushed within a second
> and logged (at most once a minute per ring) instead of leaving its caller
> waiting until another request reaches that ring (PROGRESS,
> "ring-stress-hang"). Both are 0 on `/dev/fuse`.
>
> `CONSTELLATION_FUSE_URING_FAULT=malformed-register` is **fault injection
> for the test harness only** (`transport-refused-registration`): a mount
> that asks for the ring registers it malformed, the kernel refuses, and the
> mount falls back to `/dev/fuse`. Never set it otherwise.

### FUSE passthrough

On Linux 6.9+ (built with `CONFIG_FUSE_PASSTHROUGH`), a daemon holding
`CAP_SYS_ADMIN` answers a **read-only open of a file whose whole content is
one chunk already verified in the local disk cache** (and not held in the
daemon's chunk memory tier, below) by handing the kernel the chunk file
itself (plan 38 §3(c), Z3): the kernel then serves every read
of that descriptor from the chunk file, and the daemon never sees them.

**By default only read-only mounts use it** (a snapshot view, which the
kernel mounts `ro`). A writable mount uses it only with
`CONSTELLATION_FUSE_PASSTHROUGH=1`, because there it changes what a legal
`open(O_RDWR)` returns (`ETXTBSY`, below) — on a read-only mount that open
is `EROFS` before the daemon sees it, so the change cannot arise. Every
condition is checked once per mount (and per open), and anything short of
all of them is the ordinary path; a mount that asked and could not have it
logs that **once** as `passthrough unavailable: <reason>`. A mount checks
the kernel really accepts a file of the cache directory as a backing file
once, before it serves anything: a process whose `CAP_SYS_ADMIN` is only a
user namespace's (`EPERM`), or a cache directory on a stacked filesystem
such as overlayfs (`ELOOP`), turns it off for the mount with reason
`backing_open`.

**Passthrough works on every transport** (`--fuse-transport`): the backing
file is registered on the connection's `/dev/fuse` descriptor, which a ring
session has too, and the open's reply carries its id over the ring as over
`/dev/fuse` (harness `passthrough-on-every-transport`). A read-only snapshot
mount takes no cluster locks, so under the default `auto` it is not held
back on `/dev/fuse` for them and gets the ring where the host grants it —
passthrough for the handles of verified chunks, the ring for everything
else.

**A chunk the daemon's memory tier holds is never handed over**
(`CONSTELLATION_CHUNK_MEMCACHE_BYTES`): a memory hit is served faster by the
daemon than by the kernel reading the chunk file, which after the page
cache drops means a disk read. Passthrough is taken only for verified
chunks that are on disk but not in memory — in practice chunks the memory
tier has evicted (a working set larger than the tier), or every chunk with
the tier off. Since a file's first read admits its chunk to memory, a
small, hot set of files is read the ordinary way.

A snapshot view's files qualify under the same rule (plan 38 Z3c): a
snapshot file of at most one chunk, whose chunk is verified in the disk
cache and not held in memory, is served by passthrough, and an open of it
for writing is `EROFS` as always. The first open of such a file after a
daemon start is ordinary when its chunk was found on disk rather than
fetched (that read verifies it). Files under `<dir>/.constellation/snapshot/`
on a writable mount qualify too, when that mount opted in. Measured on a
snapshot view (4096 files of 64 KiB, 4 MiB chunks, medians, PROGRESS "Plan
38 Z3c"): with a 1 GiB memory tier nothing is served by passthrough, and
the default is at parity with `CONSTELLATION_FUSE_PASSTHROUGH=0` (page cache
warm 2.16 s vs 2.29 s, after `drop_caches` 2.19 s vs 2.35 s); with the tier
off every open is passthrough, and the default is at parity or faster (warm
2.14 s vs 2.25 s, after `drop_caches` 7.34 s vs 7.83 s).

| Knob | Default | Meaning |
|---|---|---|
| `CONSTELLATION_FUSE_PASSTHROUGH` | unset: read-only mounts only | `1` (also `on`/`true`/`yes`) turns it on for writable mounts too, **with the `ETXTBSY` caveat below**; `0` (`off`/`false`/`no`) turns it off for every mount the daemon makes, read-only ones included. Any other value fails the mount, as an unparseable `CONSTELLATION_FUSE_TRANSPORT` does. Read once, when the daemon starts (like `--fuse-transport`). An embedder sets the same through `MountOptions::passthrough` |
| `--cache-verify always` | — | turns it off whatever the variable says (the reason reported is `cache_verify_always`) |

What a mount got is `node.status`'s per-mount `fuse.mounts[].passthrough`:
`enabled`, `opens` (descriptors the kernel serves from a chunk file right
now), `unavailable_reason` (`writable_mount` — the default on a writable
mount —, `disabled`, `cache_verify_always`, `no_cap_sys_admin`, `kernel`,
`backing_open`, `platform`) and
`refused_opens` (below); `cache.open_pins` counts the disk-cache pins those
descriptors hold. `/metrics` has `constellation_fuse_passthrough_opens`,
`constellation_cache_open_pins` and
`constellation_fuse_transport_fallbacks_total{from="passthrough",to,reason}`
— the per-mount reasons above, plus per-open `backing_open` (the kernel
refused to register the chunk file: typically a cache directory on a
stacked filesystem such as overlayfs, `ELOOP`), `cached_handle_open` (the
file was also open the ordinary way) and `backing_busy` (the file changed
while a passthrough descriptor of it was open).

What changes for applications:

- **A passthrough descriptor keeps the bytes it was opened on until it is
  closed** — close-to-open. That is what a write on another node always
  meant; on the *same* mount it is new: a descriptor opened *before* a
  local writer opened the file does not see what the writer writes (every
  open *after* the writer does, and while any descriptor can write the file,
  no open of it is passthrough). Close and reopen to see new bytes.
- **On a writable mount that opted in, a read-write open of a file that is
  open in passthrough mode fails with `ETXTBSY`** until those descriptors
  close — whichever process holds them. The kernel requires every open of
  such a file to share its chunk file (`fs/fuse/iomode.c`: anything else is
  `EIO`), and a read-write one could write into the cache through a shared
  `mmap`. Programs that hit this while another process reads the same
  file: `fopen(f, "r+")`, Java `RandomAccessFile(f, "rw")`, Python
  `open(f, "r+b")`, SQLite opening a database read-write while a read-only
  connection holds it, `fallocate(1)`. Write-only opens (appenders,
  `dd of=`, `rsync --inplace`), new files and editors that save by rename
  are unaffected: a write-only open is served (by the daemon) and is
  visible to every later open. When the view itself refuses the open
  (`EACCES`, `ENOENT`), that is the answer, not `ETXTBSY`.
- **An `mmap` can show older bytes than `read(2)`** on a writable mount
  that opted in: while any passthrough descriptor of a file is open, a
  later descriptor of it (one opened after a write, local or from another
  node, or a write-only one) reads and writes through the daemon and sees
  the current bytes, but the kernel maps *its* `mmap` onto the chunk file
  the first passthrough descriptor was opened on — the old bytes — until
  every passthrough descriptor of the file is closed. Nothing in the
  daemon can change this (the kernel has no way to revoke a backing file).
- **`O_DIRECT` reads of a passthrough descriptor go to the cache's block
  device** even when the chunk is in the page cache.
- **An eligible open counts as a read for atime**: with `--atime` on, it
  bumps the file's atime even if the application never reads a byte.
- A chunk under an open passthrough descriptor is never evicted; the disk
  cache's budget can therefore be held by open files, bounded by the open
  files themselves.
- Passthrough descriptors survive `daemon --upgrade`: the new image keeps
  the kernel's backing files and the cache pins. The handover format
  changed with it (version 3), so an upgrade from or to a binary without
  passthrough is refused by `daemon --upgrade`'s ABI check; unmount and
  remount instead.

### Per-filesystem settings

Stored in the bucket's `meta.json` and read by every mount.

| Setting | Set with | Default | Meaning |
|---|---|---|---|
| `ack_policy` | `fs create --ack-policy local\|s3` (default `CONSTELLATION_ACK`) | `local` (absent) | what a mutation's acknowledgement waits for (plan 30 M9), for every mount and every lease tenure. `local`: journaled by its sequencer, plus a backup when one is in budget. `s3`: the record's segment is in the bucket. Fixed at creation (there is no `fs set` for it, and no per-mount override: an acknowledgement is the sequencer's, so a per-mount setting could not apply to ops another node sequences) |
| `epoch_slack` (`f`) | `fs create --epoch-slack N`, `fs set epoch-slack TARGET N` | `0` (absent) | how many write-eligible nodes a continuation epoch may form without (plan 30 M10). With `f > 0`, an S3 takeover of an expired lease needs `f` other nodes' heartbeat promises. See [Durability and failover](features/durability-and-failover.md#flexible-continuation-epochs) |

`fs set epoch-slack` refuses `N` at or above the write-eligible roster
size and warns when `N > roster − 2`, because a single crashed holder
then blocks TTL failover until it returns. Mounted nodes pick up a
change within about a minute. With `f > 0`, `CONSTELLATION_PROMISE_TTL_S`
must be at most a quarter of `CONSTELLATION_LEASE_TTL_MS`; otherwise
`fs create`, `fs set` and the mount fail. A mounted node that started
at `f = 0` with a longer TTL and sees `f` raised clamps its TTL to a
quarter of the lease TTL and logs an error.

### Precedence

| Setting | Order (first that is set wins) |
|---|---|
| `ack_policy` | `meta.json` only (written by `fs create`: `--ack-policy`, else `CONSTELLATION_ACK`, else `local`) |
| `cto` | `--cto`, `CONSTELLATION_CTO`, `bounded` |
| `locks` | `--locks`, `CONSTELLATION_LOCKS`, `cluster` if P2P is on, else `local` |
| `atime` | `CONSTELLATION_ATIME`, `--atime`, `off` (the environment wins here, unlike the rows above) |
| `epoch_slack` | `meta.json` only |

### Plan 30 commands

| Command | Meaning |
|---|---|
| `constellation delegate TARGET --to NODE [--range IDX/COUNT]` | delegate a directory's subtree, or one of `COUNT` name-hash ranges of it (`COUNT` is 2, 4, 8 or 16), to node `NODE`. The node running it must hold the lease. See [Delegations](features/delegations.md) |
| `constellation undelegate TARGET` | recall the delegation on a directory (refused for a designation: use `constellation online`) |
| `constellation delegations TARGET` | list the live delegation table |
| `constellation reintegrate TARGET` | run a deposed holder's recovery now: roll back the stranded journal and replay it by rid (it also runs on its own) |
| `constellation repair drop-held TARGET INO` | discard held-back records of an inode into a conflict copy (plan 30 M4, see [Write-path hygiene](features/write-path-hygiene.md)) |
| `constellation repair abandon-group TARGET --epoch E` | let a lease claim that waits for the tail of a lost group (`status.lease.waiting_for_group`) go on without it; rows that group acknowledged may be lost. Admin. See [Durability and failover](features/durability-and-failover.md#a-claim-waits-for-a-lost-groups-tail) |
| `constellation repair abandon-group TARGET --gen G` | on the root: end the delegation generation `G` that waits for its group's tail (`status.delegation.gens[].waiting_for_group`) at the root's cursor, and record it in the log (`GroupAbandoned`); rows only that group held, acknowledged to their clients, are lost. Exactly one of `--epoch` and `--gen`. Admin. See [Delegations](features/delegations.md#failures) |

## Environment variables

### Identity and secrets

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_NODE_KEY` | `$XDG_CONFIG_HOME/constellation/node.key`, else `~/.config/constellation/node.key`, else `/etc/constellation/node.key` | path | host Ed25519 key for the P2P endpoint id; one key per host, not per mount |
| `CONSTELLATION_PASSPHRASE` | prompt | string | E2E filesystem passphrase (`fs init`, mount, `fs passwd`); required non-interactively |
| `CONSTELLATION_NEW_PASSPHRASE` | prompt | string | new passphrase for `fs passwd` |

### Leases and mutations

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_LEASE_TTL_MS` | `60000` | milliseconds, positive | S3 lease expiry and renewal |
| `CONSTELLATION_LEASE_IDLE_RELEASE_MS` | `30000` | milliseconds | idle threshold for releasing a held lease — **only** once a requester is registered |
| `CONSTELLATION_FORWARD_TIMEOUT_MS` | `500` | milliseconds | forwarded mutation request |
| `CONSTELLATION_FORWARD` | `on` | boolean | requester-side mutation forwarding; `off` makes non-holder writes acquire the lease instead |
| `CONSTELLATION_SNAPSHOT_FORWARD_TIMEOUT_MS` | `30000` | milliseconds | a snapshot batch (create, delete, hold) forwarded to the root-lease holder, which drains, ships and publishes before it answers; on timeout the caller gets an error and nothing moves the lease (see [Forwarded mutations](features/forwarded-mutations.md#snapshot-batches)) |
| `CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS` | `100000` | diff keys | skip-empty (plan 32 §3.4): how many keys of the tree diff between a create's `skip_if_unchanged_since` root and the holder's fresh commit are examined for one under the snapshot's directory; past it the directory counts as changed and the snapshot is taken. The budget counts **every** diff key, subsystem rows (snapshot rows, holds, other `0x30` keys) included, not only the ones that could map under the directory. `0` takes every snapshot whose tree root moved |
| `CONSTELLATION_LEASE_PLACEMENT` | `on` | boolean | holder-driven placement of the root lease (see [Lease placement](features/lease-placement.md)) |
| `CONSTELLATION_LEASE_DWELL_MS` | `5000` | milliseconds; `0` means the default | a lease handed over cannot be handed back before this (it stops two competing writers ping-ponging it) |
| `CONSTELLATION_LEASE_WANTED_GRACE_MS` | `5000` | milliseconds; `0` means the default | a requester registered in `wanted_by` is answered within this, busy holder or not |
| `CONSTELLATION_HLC_SKEW_TOLERANCE_S` | `300` | seconds, read once at start | how far ahead of this node's wall clock a peer's stamp may be and still advance its hybrid logical clock: a replayed record's (applied as is either way), and a forwarded close's change time (past it the file's ctime is the commit's time instead); a stamp further ahead is not observed (a warning, once; see `crates/meta/src/hlc.rs`) |

The lease's expiry margin (a lease is usable only while at least this
much of it is left, and every grant backed by it is capped by it) is
`min(1000 ms, CONSTELLATION_LEASE_TTL_MS / 4)`. It is not a separate
knob. Plan 29 M5's `CONSTELLATION_FORWARD_MAX_INFLIGHT` no longer
exists: since plan 30 M5 each forward runs as its own task, ordered only
by the requester-side key gate. Forwards are still bounded, by their
callers and the transport rather than a knob: a requester has one
forward per blocked caller (a FUSE worker thread, one of the bounded
write-back uploads, the one-at-a-time replay queue), each retried at
most `forward_retries` (3) times before it takes the lease path; a
holder serves at most 32 concurrent streams per peer (the P2P layer's
per-connection limit), and a further request waits in QUIC flow control
on the requester's side. See
[Forwarded mutations](features/forwarded-mutations.md#concurrent-forwarding-correctly-ordered).

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

### Exactly-once and speculation

Plan 30 M2–M3. See [Forwarded mutations](features/forwarded-mutations.md).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_COMPLETION_RETENTION_S` | `900` | seconds | how long the node-local `completed` keyspace keeps a rid, and the age below which log GC never prunes a segment (the coverage rule; see [Garbage collection](#garbage-collection)). It also sets the `completed` prune cadence (a quarter of it, clamped to 30–3600 s) and caps an inbox op's in-doubt deadline at half of it |
| `CONSTELLATION_HOLDER_CAPTURE` | on | exact `0`, `off` or `false` (case-sensitive) disable it | internal switch: capture a holder's own journaled writes as speculation (before-images). Off is the M3b performance-gate fallback: a holder with an unshipped journal then defers publishing, and a deposed holder rebuilds its namespace from the shared log instead of rolling back. Not a tuning knob |

### Sessions, close-to-open, and log streams

Plan 30 M6–M8. See [Close-to-open modes](features/cto-modes.md).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_CTO` | `bounded` | `bounded`, `strict` | default for `--cto`; the flag wins |
| `CONSTELLATION_SLOW_OP_MS` | `2000` | milliseconds; `0` turns it off | a FUSE mutation or create, or a client op of the authority core, slower than this is logged at WARN (the core's line lists the states the op went through: forwarded, parked at a delegate, waiting for an acknowledgement, ...) |
| `CONSTELLATION_FUSE_REQUEST_STALL_S` | `30` | seconds; `0` turns the monitor off | the FUSE request watchdog: every request a FUSE worker handles is registered with the wait it last noted (a write shard, the inode's operation lock, the session wait, a chunk fetch, a reply from the authority core, ...); one older than this is logged at WARN, again every further threshold, and once more when it completes; `status` reports them under `fuse_requests` (a blocking lock request is listed, never counted) |
| `CONSTELLATION_FUSE_STALL_BACKTRACE` | unset | `1` | with the watchdog: the first report of a stalled request also writes the handling thread's backtrace to the daemon log (a diagnostic for a build with symbols); so does the report of an authority driver that has made no progress for `CONSTELLATION_HOLDER_STALL_MS` (the driver's thread) |
| `CONSTELLATION_SESSION_WAIT_MS` | `2000` | milliseconds; `0` disables the wait | how long a read waits for this node's replica to reach the position its client already observed (read-your-writes, monotonic reads). On timeout the read answers from the replica and is counted as degraded, never an error |
| `CONSTELLATION_READ_INDEX_BUDGET_MS` | `2000` | milliseconds, at least 1 | how long a strict read waits for the sequencer's ReadIndex answer before it reads the replica anyway (degraded) |
| `CONSTELLATION_READ_DELEGATIONS` | on | boolean | this node, as sequencer, grants read delegations to strict readers. Off: every strict read costs a round trip. Always off with P2P off |
| `CONSTELLATION_READ_AHEAD_GRANTS` | `128` | 0 to 512; `0` turns it off | how many children of a directory one ReadIndex answer (a strict listing, or a lookup under a held directory delegation that misses a child's) grants read delegations on, so a cold scan costs a round trip per this many entries, not per entry. Each is an ordinary grant |
| `CONSTELLATION_READ_GRANTS_PER_NODE` | `8192` | count | the most read delegations one node may hold live from this sequencer before a read-ahead grants it no more (the single grant a read asks for is still made); bounds the grant table every write scans. A directory bigger than this is scanned with a round trip per further child until earlier grants lapse |
| `CONSTELLATION_READ_DELEGATION_TTL_MS` | `5000` | milliseconds, at least 1 | a read delegation's lifetime, renewed while in use; also capped by the granting lease |
| `CONSTELLATION_LOG_STREAMS` | on | boolean | followers subscribe to the holder's direct log stream instead of polling S3 (plan 30 M7). Always off with P2P off |
| `CONSTELLATION_LOG_STREAM_HEARTBEAT_MS` | `1000` | milliseconds; `0` means the default | the holder's heartbeat frame to a subscriber it has sent nothing to |
| `CONSTELLATION_LOG_STREAM_TIMEOUT_MS` | `3500` | milliseconds; `0` means the default | a subscription with no frame for this long is dead; the subscriber falls back to S3 and resubscribes |
| `CONSTELLATION_LOG_STREAM_BACKSTOP_MS` | `10000` | milliseconds; `0` means the default | a caught-up subscriber still probes S3 with one GET this often |
| `CONSTELLATION_LOG_GAP_CHECK_MS` | `300000` | milliseconds; `0` means the default | a follower whose GET-next probe found nothing confirms with one LIST, this often, that it is at the head and not past a pruned log (DESIGN.md §14 "Falling behind segment GC"); a lease takeover always checks |
| `CONSTELLATION_LOG_GAP_HINT_CHECK_MS` | `5000` | milliseconds; `0` means the default | the same check, at most this often, when a gossip hint or the stream's head lies past the cursor |
| `CONSTELLATION_LOG_STREAM_QUEUE` | `1024` | frames, positive | frames queued per subscriber |
| `CONSTELLATION_LOG_STREAM_BUFFER_BYTES` | `33554432` (32 MiB) | bytes, positive | segment bytes queued per subscriber before the holder drops it back to S3 tailing. The holder never waits for a subscriber |
| `CONSTELLATION_KERNEL_INVALIDATE` | on | boolean | push kernel entry and inode invalidations for records applied from other nodes (and, plan 42, for a commit through another view of the node, a write session dropped without a commit, and a stranding rollback). A notification is held back while a FUSE request is in flight on its inode (the kernel would block it behind that request) and dropped once it is older than the lookup TTL: by then every entry and attribute the kernel cached before the change has expired, but a file's pages never expire, so a dropped data notification marks the file's pages as not current and its next open drops them (plan 42 F3). Off, no open keeps a file's pages (`FOPEN_KEEP_CACHE`): the kernel would never hear of another node's change |
| `CONSTELLATION_KERNEL_INVAL_STALL_S` | `5` | seconds, positive | a kernel invalidation blocked in the kernel longer than this is logged at WARN with the request count in flight on its inode |

### Backups, acknowledgement, and failover

Plan 30 M9. See [Durability and failover](features/durability-and-failover.md).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_ACK` | unset | `local`, `s3` | default for `fs create --ack-policy`. A mount whose environment sets it to something other than the filesystem's policy logs a warning and uses the filesystem's |
| `CONSTELLATION_BACKUP_RTT_BUDGET_MS` | `5` | milliseconds; `0` accepted | a peer is a backup candidate only while its measured RTT to the holder is within this. `0` means never use a backup |
| `CONSTELLATION_BACKUPS` | `1` | count; `0` accepted | the most backups a holder keeps. `0` means none |
| `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` | `1000` | milliseconds; `0` means the default | a backup that makes no acknowledgement progress for this long is removed by a lease CAS before the holder acknowledges anything further — unless it answers the holder's liveness heartbeat and is not answering appends short: then only after `CONSTELLATION_BACKUP_SLOW_MAX_MS` |
| `CONSTELLATION_BACKUP_SLOW_MAX_MS` | `10000` | milliseconds; at least the ack timeout | how long a backup known alive (a loaded node whose core acknowledges late) may go without acknowledgement progress before it is removed anyway; also how long an append waits for its answer |
| `CONSTELLATION_BACKUP_TAKEOVER_MS` | `1500` | milliseconds; `0` means the default | holder silence after which a backup seals the epoch and takes the lease over (under `ack=s3`, any peer may). The silence does not count across the watcher's own runtime lapses (at most one window plus the lapses' length more), and the window grows by twice the latest lateness of the holder's heartbeats within the last minute (at most four windows); see [Durability and failover](features/durability-and-failover.md#seal-based-failover). Liveness only: safety comes from the seal and the log-slot CAS |
| `CONSTELLATION_BACKUP_HEARTBEAT_MS` | `300` | milliseconds, clamped to at most a third of the takeover time | the holder's heartbeat append to an idle backup, and the interval of its off-core liveness heartbeat (`HolderAlive`) |
| `CONSTELLATION_HOLDER_STALL_MS` | `15000` | milliseconds | a holder whose authority driver has not woken for this long since the heartbeat sent it an echo, has spent this long in one wake-up (one core step, or anything else it does between waits for work), or whose runtime has run nothing for this long (its lag probe), stops its off-core liveness heartbeat, so its backups seal it a takeover window later (up to four, if its beats had come late); that long is a hung authority, not a busy one |
| `CONSTELLATION_PRE_S3_STREAMING` | on | boolean | stream backup-acknowledged transactions to log-stream subscribers before they reach S3; subscribers hold them as speculation. Only used while the lease has a backup. Always off with P2P off |

### Continuation epochs

Plan 30 M10. The slack `f` itself is a per-filesystem setting
(`epoch_slack`, see [Per-filesystem settings](#per-filesystem-settings)).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PROMISE_TTL_S` | lease TTL / 4 (15 s) | seconds, positive | how long a heartbeat promise binds its writer. Must be at most a quarter of the lease TTL when `epoch_slack > 0`, or `fs create`, `fs set epoch-slack` and the mount fail; with slack 0 a violation only logs a warning, and a later raise of the slack clamps it on the mounted node |

Promises are published on demand, not on a timer: when a would-be taker
asks over P2P, when a node sees a lease expire unrenewed, or when its
slack changes. A cluster with `epoch_slack = 0` writes no promises.

### Delegation and placement

Plan 30 M11–M12. See [Delegations](features/delegations.md). An
unparsable value means the default. For the millisecond knobs `0` also
means the default; for `_MIN_OPS`, `_DOMINANCE`, `_LEAVE` and `_SPLIT`,
`0` is a value (percentages above 100 read as 100).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_DELEGATION` | on | boolean | this node delegates subtrees (as the root) and accepts delegations (as a delegate). Off, or P2P off: no `Delegate` record is ever written |
| `CONSTELLATION_DELEGATION_TTL_MS` | the lock TTL (`CONSTELLATION_LOCK_TTL_MS`, 20 s), at least `5000` | milliseconds | a delegation grant's lifetime, renewed at half of it (a quarter while it has lock grants out). Lock grants under a delegation never outlive it, so it bounds them; it is also how long a dead delegate's subtree waits to be reclaimed |
| `CONSTELLATION_DELEG_LANE` | on | `off`, `0` or `false` turns it off | the off-core delegation renewal lane: while this node's authority driver is stuck in a step (from a quarter of the renewal period, at most 500 ms, to `CONSTELLATION_HOLDER_STALL_MS`), its heartbeat thread sends the delegation renewals that are due, so a core blocked behind a slow store or log write does not lose the grant; for A/B runs only (see [Delegations](features/delegations.md#grants-renewal-and-expiry)) |
| `CONSTELLATION_DELEG_ROOT_LANE` | on | `off`, `0` or `false` turns it off | the root's side of the same: while this node's authority driver is stuck in a step (as long as the delegate's lane waits, to `CONSTELLATION_HOLDER_STALL_MS`), its P2P service answers a delegation renewal itself, as the RPC reply of the request, by extending the generation's window in the shared window table, under the lease certificate the core published (the cap by the lease, the freshness rule, no recall or seal begun, nothing for the lock side to hand over); the core still answers the request in full; for A/B runs only (see [Delegations](features/delegations.md#grants-renewal-and-expiry)) |
| `CONSTELLATION_DELEGATION_PLACEMENT` | on | boolean | the root delegates dominated subtrees (and, with `CONSTELLATION_DELEGATION_SPLIT`, splits hot shared directories) by itself. Off keeps manual `delegate` only. Needs `CONSTELLATION_DELEGATION` |
| `CONSTELLATION_DELEGATION_WINDOW_MS` | `30000` | milliseconds | placement's sliding window of ops per directory and node |
| `CONSTELLATION_DELEGATION_MIN_OPS` | `200` | ops per window; `0` accepted | rate floor: a subtree (or, for a split, the directory itself) needs this many ops in the window. `0`: no floor |
| `CONSTELLATION_DELEGATION_DOMINANCE` | `70` | percent; `0` accepted | the share of a subtree's ops one node needs to be given it. `0`: the top writer of a subtree above the rate floor gets it (and no directory is a split candidate, since some node always "dominates") |
| `CONSTELLATION_DELEGATION_LEAVE` | `50` | percent; `0` accepted | a placed delegation whose delegate stays below this share for a whole dwell is recalled. `0`: never recalled for its share, only for the rate |
| `CONSTELLATION_DELEGATION_SPLIT` | `0` (off) | percent; `0` accepted | expert setting, off by default ([ADR-38](../explanation/DECISIONS.md#adr-38-automatic-directory-hash-range-splits-are-off-by-default)): when set (`20` is the tuned value), a hot directory that no node dominates is split into name-hash ranges when several nodes each write at least this share of it. It helps only when each writer's names hash into its own range; with ordinary names a split ran at 0.01 to 0.91 times the throughput of none ([results](../../bench/dirsplit/RESULTS.md), [Delegations](features/delegations.md#hot-shared-directories)). Delegating the directory whole is the recommended tool for writers far from the root. Ignored (with a warning) on a node with a `--cto strict` mount, where hand splits are refused too |
| `CONSTELLATION_DELEGATION_DWELL_MS` | `60000` | milliseconds | how long a recall condition must hold before placement recalls |
| `CONSTELLATION_DELEGATION_COOLDOWN_MS` | `30000` | milliseconds | a recalled directory is not placed or split again for this long |

### The S3 inbox

Plan 30 M13. See [Forwarded mutations — the inbox](features/forwarded-mutations.md#the-inbox-forwarding-without-p2p).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_INBOX` | on | boolean | forward mutations through the holder's S3 inbox when there is no P2P path to it. Off restores the pre-plan-30 behaviour: a non-holder with no P2P path takes the lease |
| `CONSTELLATION_INBOX_P2P_GRACE_MS` | `3000` | milliseconds | how long an outage to the holder (a failed dial, or a transport error that evicted its connection) may last, with nothing heard from it since, before the inbox counts the P2P path as gone. A known peer never talked to is reachable; a timeout on a still-open connection is never an outage |
| `CONSTELLATION_INBOX_HOT_MS` | `20` | milliseconds, clamped to `[1, CONSTELLATION_SYNC_INTERVAL_MS]` | the holder's poll interval for a requester right after a hit, and for the ~25 misses after it |
| `CONSTELLATION_INBOX_IDLE_MAX_MS` | `2000` | milliseconds | the *warm* poll ceiling: a requester that submitted within the last minute is polled at least this often. Per requester, doubling from `CONSTELLATION_SYNC_INTERVAL_MS` on every miss |
| `CONSTELLATION_INBOX_COLD_MAX_MS` | `CONSTELLATION_SYNC_IDLE_MAX_MS` (10000) | milliseconds | the *cold* poll ceiling, after about a minute of misses: what an idle P2P-off cluster pays per requester (one GET per interval on the holder) |
| `CONSTELLATION_INBOX_POLL_WIDTH` | `4` | count, at least 1 | batches fetched per poll after a hit (a saturated poll is repeated at once); an idle poll fetches one |
| `CONSTELLATION_INBOX_TAIL_MS` | `20` | milliseconds, at least 1 | the requester's log-tail interval while one of its ops waits for an outcome. With the hot poll this sets the floor of an inbox round trip |
| `CONSTELLATION_INBOX_RECHECK_MS` | `1000` | milliseconds, at least 100 | how often a waiting requester re-reads the lease object, to notice a takeover (re-submit under the new epoch) or an expired holder (take the lease path) |
| `CONSTELLATION_INBOX_ESCALATE` | on | boolean | a requester with sustained inbox demand asks for the lease. Off keeps it on the inbox however busy it is |
| `CONSTELLATION_INBOX_ESCALATE_WINDOW_MS` | `10000` | milliseconds, at least 1000 | the sliding window over this node's inbox-answered ops that decides "sustained" |
| `CONSTELLATION_INBOX_ESCALATE_OPS` | `8` | count, at least 2 | inbox-answered ops in the window that make demand sustained. De-escalation needs the window under half of this and half of the wait threshold |
| `CONSTELLATION_INBOX_ESCALATE_WAIT_MS` | `1500` | milliseconds | cumulative inbox round-trip wait in the window that makes demand sustained, the term that fires first on a slow (real) S3. It needs at least five ops in the window and leaves the single slowest out, so one slow op (a first contact) never moves the lease |
| `CONSTELLATION_INBOX_ESCALATE_RETRY_MS` | `2000` | milliseconds, at least 100 | the longest gap between an escalated requester's lease requests (they back off from 100 ms) |

### Cluster locks

Plan 30 M14. See [Cluster locks](features/cluster-locks.md).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_LOCKS` | `cluster` with P2P, `local` without | `local`, `cluster` | default for `--locks`; the flag wins |
| `CONSTELLATION_LOCK_TTL_MS` | `20000` | milliseconds; `0` means the default | a lock grant's lifetime, renewed in the background while held. A holder whose node *stops* for longer (a partition, a frozen or dead daemon; a slow disk alone no longer does, see `CONSTELLATION_LOCK_LANE`) loses the lock: its node fails I/O on the locked files, and every write and namespace operation of the lock's owner on any file, with `EIO`. A waiter behind a *crashed* holder waits up to `ttl + margin`; see [Cluster locks](features/cluster-locks.md#configuration) |
| `CONSTELLATION_LOCK_LANE` | `on` | `off`, `0` or `false` turns it off | the lock renewal lane (plan 41): renews this node's own lock grants, and the ones it holds at an owner, off the authority core from the certificate the core publishes, so a core stuck in a metadata-store write (a slow journal write on the device) does not lapse them; always on, `off` for A/B runs and the stall scenarios' fail-before (see [Cluster locks](features/cluster-locks.md#renewals-off-the-core)) |
| `CONSTELLATION_ROOT_PROBE` | on | `off`, `0` or `false` turns it off | the root's own proof that it still holds its lease (plan 41 D3): while this node's authority driver is stuck in a step (and not hung, `CONSTELLATION_HOLDER_STALL_MS`), a tenure that can be taken over before its lease expires (a listed backup, or `ack=s3`) reads its lease object itself, off the core, when the delegation answerer or the lock lane's own renewals find the core's last proof of liveness stale (at most one read in flight and one per `backup_takeover_ms`); a read that names this node and the tenure's epoch lets them answer within `backup_takeover_ms` of its send; for A/B runs only (see [Delegations](features/delegations.md#grants-renewal-and-expiry), [Cluster locks](features/cluster-locks.md#renewals-off-the-core)) |
| `CONSTELLATION_LOCK_LANE_MAX_STALL_MS` | `120000` | milliseconds | the lane's renewals of grants held at another node stop once the authority driver has made no progress for this long: a hung core is not a slow one, and its grants lapse as they always did |
| `CONSTELLATION_LOCK_CACHE_IDLE_MS` | `30000` | milliseconds; `0` means the default | how long a grant with no local lock under it is kept (so an uncontended re-lock costs nothing) before it is released |
| `CONSTELLATION_LOCK_CLOSE_WAIT_MS` | `100` | milliseconds; `0` turns the wait off | how long after a `close(2)` a conflicting non-blocking lock request or lock test may wait for the closed description's OFD and `flock` locks to go (the kernel releases them after `close` returned; a single-worker mount never waits for them), and how long a non-blocking request may wait for an earlier lock turn's close still publishing the file (or its operation in doubt); never longer than this from the request itself; see [Cluster locks](features/cluster-locks.md#close-and-release) |

### Metadata sync

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | milliseconds | background log tail/ship poll; the **floor** of the idle backoff |
| `CONSTELLATION_SYNC_IDLE_MAX_MS` | `10000` | milliseconds | **ceiling** of the idle poll backoff |
| `CONSTELLATION_ROUND_UPLOAD_WAIT_MS` | `250` | milliseconds; `0` means the default | plan 30 M7: how long a sync round waits for the upload pass before shipping what is already shippable. A transaction whose manifest still names a pending chunk is deferred to a later round, so a write-back backlog no longer blocks every other record |

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
holder sleep through its own renewal. Its poll also fires at the moment
the renewal falls due (half the TTL left), so the renewal goes out then
rather than up to a quarter TTL later: what is left of the lease caps
the root's delegations and lock grants, and through a delegation every
delegated lock grant. A lease short enough that a delegation renewal it
caps falls under four margins, or a lock grant of the root's own under
two, is renewed at the next round instead (at most once per lease expiry, and only if renewing buys at least a margin):
below about eight margins (8 s at the 1 s margin) a delegated lock's
window ended before the half-TTL renewal.

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

Only the lease holder publishes (plan 30 M4). Every node used to
publish its own view of the same log, which cost each of them the
condemned-list reads, the head discovery and the planning of every
publish, and grew with the node count. The cadence, on the holder, is:

- a plain count floor — at least 32 shipped segments since the last
  publish (`PUBLISH_EVERY`, not currently a knob);
- an idle timer — `dirty` is non-empty and no publish has run for
  `CONSTELLATION_PUBLISH_IDLE_S` (default 30), checked once per sync
  round after there is nothing left to ship. Without this, a holder
  that is mostly idle could leave its head commit — and the
  log-retention floor riding on it — stale indefinitely;
- a publish on clean unmount (or whenever the mount otherwise drains
  its journal for shutdown), when `dirty` is non-empty and the node
  still holds the lease.

A node that does not hold the lease runs the idle timer too, but instead
of publishing it probes the chain head: once the head commit's `applied`
position covers its own applied position, and it has nothing speculative
outstanding and nothing of its own journaled, it clears the dirty marks
the head already reflects (so its dirty set does not grow without bound,
and it can publish the moment it becomes the holder). An explicit
publish — a snapshot — still publishes on any node.

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
| `CONSTELLATION_ATIME_SKEW_TOLERANCE_S` | `300` | seconds | clamp applied to a claimed atime on apply |
| `CONSTELLATION_ATIME_RO_FORWARD` | off | exactly `1` or `true` enable it | let a read-only member forward atime batches |

Atime never blocks a read, never acquires a lease, and never delays an
unmount or lease handoff. A dropped bump only costs freshness.

### P2P and cooperative cache

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_P2P` | `on` | boolean | complete iroh fast path |
| `CONSTELLATION_P2P_RELAY` | `off` | `off` (also `disabled`, `none`, `0`, `false`), `default`/`public`/`n0`, or comma-separated relay URLs | iroh relay policy |
| `CONSTELLATION_P2P_RELAY_TOKEN` | unset | string | optional bearer token for custom relays |
| `CONSTELLATION_P2P_INTERFACES` | unset (every interface) | comma-separated interface names, `*` and `?` wildcards; empty or `*` admits all | which local interfaces' addresses this node publishes in the registry as its P2P address, and whose loss resets its peer connections |
| `CONSTELLATION_P2P_INTERFACES_DENY` | `docker*,br-*,veth*,virbr*,vnet*,cni*,flannel*,cali*,vxlan*,cilium*,weave*,podman*,lxcbr*,lxdbr*,kube-*` | comma-separated patterns as above; empty or `none` denies nothing | interfaces never published, even if `CONSTELLATION_P2P_INTERFACES` matches them. The default refuses container and VM plumbing, whose addresses come and go with other workloads. VPN and overlay tunnels (`tun*`, `wg*`, `tailscale*`) are admitted on purpose: they may be the only route between two hosts |
| `CONSTELLATION_P2P_PATH_IDLE_MS` | `5000` | milliseconds, clamped to `300`..`15000` | how long a direct QUIC path may receive nothing before it is abandoned and its data moves to another path — only when the connection has another path open: a connection's last path is never abandoned on this timer. Each path is pinged every third of it. iroh's default is 15 s, and iroh caps it there (and ignores a keepalive above 5 s) |
| `CONSTELLATION_P2P_DIAL_TIMEOUT_MS` | `5000` | milliseconds, minimum `500` | how long one dial to a peer may take. The request that started it fails (and falls back to S3) and the next request dials afresh. Raise it if first dials over a relay (relay handshake plus holepunching) take longer than this |
| `CONSTELLATION_COOP` | `on` | boolean | cooperative cache |
| `CONSTELLATION_COOP_DIGEST` | `exact` | `exact` (alias `rbsr`) or `bloom`; an unknown value warns and uses `exact` | how peers learn each other's cached chunks: exact mirrors kept by reconciliation, or bloom digests |
| `CONSTELLATION_DIGEST_INTERVAL_S` | `30` | seconds, minimum `1` | exact: summary heartbeat and liveness sweep; bloom: snapshot rotation |
| `CONSTELLATION_DIGEST_TTL_S` | `4 × DIGEST_INTERVAL_S` | seconds, at least `2 × DIGEST_INTERVAL_S` | how long an unconfirmed peer mirror or digest stays usable |

`CONSTELLATION_P2P=off` disables forwarding, placement messages, segment
push, handoff acceleration, and cooperative peer transfer. Correctness and
eventual convergence continue through S3.

The published P2P address carries only addresses on admitted interfaces.
If no admitted interface has an address, every address is published
rather than none. iroh still exchanges all local addresses with a peer
in-band, so paths to the others may open, but each node selects a path to
an address its peer published whenever one is open. The rest serve only as
backups. The published address leaves out addresses no local interface
holds (NAT mappings a relay observed), which iroh re-learns in-band; it
keeps the relay URL. A node re-publishes its address within about 5 s of an
admitted address or its home relay changing, and peers re-check their
connection to it only if the address that connection uses is no longer
published. When an admitted local address disappears, the peer connections
with a path over it are closed and re-dialed over the remaining addresses
(typically well under a second); connections that never used it carry on.

How fast a dead link is noticed depends on its paths. A connection with
another path open moves to it after `CONSTELLATION_P2P_PATH_IDLE_MS`. A
connection with a single path (the usual case between two hosts) is
checked when a request over it goes unanswered: if nothing at all comes
back within 3 s, it is replaced. Otherwise it closes after iroh's 30 s
connection idle timeout.

All nodes of a fleet should use the same `CONSTELLATION_COOP_DIGEST`: a
mixed pair does not use each other as chunk sources. See
[Cooperative cache membership](features/cooperative-cache.md) for the
protocol, message bounds and counters.

### Prefetch and scan-ahead

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_PREFETCH_MIN_BYTES` | `8388608` | bytes, positive | initial adaptive sequential-read window |
| `CONSTELLATION_PREFETCH_MAX_BYTES` | `2147483648` | bytes, positive | window ceiling, additionally capped at one quarter of cache budget |
| `CONSTELLATION_CACHE_READ_RESERVE_PCT` | `0` | percent, `0..90` | share of the chunk cache that dirty (not yet uploaded) chunks may not take: writers are throttled, then refused (`ENOSPC`), that much earlier, so reads and readahead keep room under a write burst. Plan 31 C7b: 128 MiB cache, uploads capped at 4 MB/s, a writer running — a cold sequential reader gets 12–13 MiB/s at `0`, 19–23 at `25`, 66 at `50` (110–124 alone). Off by default: it moves where a small cache pushes back on writers |
| `CONSTELLATION_CHUNK_MEMCACHE_BYTES` | 1/64 of the engine's memory share, at most 128 MiB (16 MiB with the `mobile` profile's on-demand background; an eighth of an explicit `server` memory budget), never more than `--cache-size` | bytes; `0` disables | chunk memory cache: verified chunk contents kept in RAM so a cached read is served as a shared slice, without re-reading and re-hashing the whole chunk from the disk cache (the first read of a chunk verifies it once and admits it; concurrent first reads load it once). Scan-resistant 2Q eviction (a large sequential read does not flush the hot set). Entries go when their disk-cache entry goes (eviction, prune, corruption). A chunk held here is never handed to the kernel by FUSE passthrough (the memory hit is faster; see "FUSE passthrough"). E2E: holds the plaintext the disk cache already holds, in process memory (not locked, so swappable like any read buffer). Counters: `node.status` `cache.memory_*`, `/metrics` `constellation_cache_memory_*`. Measured (see PROGRESS "Chunk memory cache"): a disk-cached 256 MiB sequential read, 4 MiB chunks, goes from ~75 to ~400 MiB/s at the default 128 MiB (~1.5 GiB/s once the file fits); cached random 4 KiB reads of a resident file from ~500 to ~50k IOPS |
| `CONSTELLATION_READDIR_COOKIE_BITS` | `62` | `62` or `31`; any other value fails the start | width of a directory entry's `readdir` cookie (the `d_off` / `telldir` value). A cookie is a keyed hash of the entry's name (SipHash-1-3 under a key derived from the filesystem's `gossip_secret`, or from the master key under E2E), so a listing resumes after the name it stopped at whatever is created or removed meanwhile; it is the same on every node and in every stream. `62` gives `[3, 2^62 + 2]`. `31` gives `[3, 2^31 - 2]` for legacy 32-bit userspace (non-LFS `getdents` fails with `EOVERFLOW` above 2^31 - 1, and `telldir`/`seekdir` cannot round-trip 62 bits); two names of one cookie are then common at scale and only make a cold resume list a name twice, never skip one (a page never holds two equal cookies). A filesystem that is not E2E and has no `gossip_secret` (created before the field existed) is refused at start: recreate it |
| `CONSTELLATION_READDIR_HINT_BYTES` | 16 MiB | bytes | the node's cache of recently served directory pages (the last four per directory, least recently used directory out first; ~8 bytes per entry plus two names per page), shared by every view. A cookie found in a remembered page resumes in O(page); one that is not costs one scan of the directory's names in order, and answers `ENOENT` if the name it stands for is gone. The counters (hint hits, cold scans, lost cookies, cookie collisions, hint bytes) are `node.status`'s `readdir` block and the `constellation_readdir_*` metrics ([Listing and `READDIRPLUS` in the status](#listing-and-readdirplus-in-the-status)). Pages of a directory that is open are not left to this cache: see `CONSTELLATION_READDIR_PIN_BYTES`. **`ENOENT` on a resume**: a cookie that no remembered or pinned page covers and whose name has been removed has no place to resume from, so the page answers `ENOENT` (`lost_cookies`). It never skips or repeats a name. It can still occur only for a `seekdir`/`telldir` cookie from before a pin bound gave pages up, for a lister that does not keep the directory open between requests (an `nfsd` re-export, which opens it per request; `browse.readdir`) whose pages this cache evicted; a sequential `getdents` loop, `rm -rf` and `find -delete` over an open directory never meet it |
| `CONSTELLATION_READDIR_PIN_BYTES_PER_DIR` | 128 MiB | bytes | plan 40: the listing pages kept for one open directory beyond the hint cache. While a directory is open (`opendir` answered, `releasedir` not yet), every page served for it from the earliest cookie any open handle could still return (`telldir`) onward is pinned, so a listing that deletes what it has read, a `seekdir` back to a deleted entry and a handover mid-listing resume exactly. About 8 bytes per listed name (128 MiB is some 16M names). Past this bound the oldest pages of that directory become ordinary hints, except each open stream's last page, which is kept whatever the bounds say, so a sequential listing stays exact at any size; only a `seekdir` back into the dropped region can then meet `ENOENT`. Counted: `pinned_bytes`, `exempt_bytes`, `pin_overflows` |
| `CONSTELLATION_READDIR_PIN_BYTES` | 512 MiB | bytes | plan 40: the same bound for the whole node, across directories (the directory with the most pinned bytes gives up first). An unparseable value of either pin bound warns and keeps the default |
| `CONSTELLATION_PREFETCH_CONCURRENCY` | unset | requests; `0` is treated as `1` | pin background-fetch concurrency instead of adapting it |
| `CONSTELLATION_PREFETCH_MAX_CONCURRENCY` | `128` | requests, `1..512` | adaptive background-fetch ceiling |
| `CONSTELLATION_SCAN_AHEAD` | `on` | boolean | ordered directory-walk readahead |

### Uploads, staging, and encoding

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_STAGING_BUDGET` | `--cache-size / 4` | bytes | in-flight unflushed write staging cap; exhaustion returns `ENOSPC`. It counts each chunk a write session has staged bytes in (its length), and the at most two released chunk slots a session keeps in its staging file for the next chunks it writes, so the staging files' disk use stays under it; the kept slots are free space to writers (left out of the throttling and of a view's `max_staging_bytes`), given back (punched) whenever a reservation would not fit beside them |
| `CONSTELLATION_UPLOAD_CONCURRENCY` | unset (adaptive) | requests, positive | pin write-back upload pool size instead of adapting it |
| `CONSTELLATION_UPLOAD_MAX_CONCURRENCY` | `128` | requests, `1..128` | adaptive upload concurrency ceiling |
| `CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S` | `10` | seconds, positive | INFO progress summary period for pending uploads |
| `CONSTELLATION_PROBE_MIN_BYTES` | `262144` | bytes; `0` lets every chunk probe | a chunk smaller than this is uploaded with one conditional create instead of a `HEAD` first, unless a hint (existence cache, a peer's digest) says it is already in S3: a probe's miss costs a second serialized round trip, which costs more than resending a small body |
| `CONSTELLATION_REMOTE_CHUNK_WAIT_S` | `60` | seconds | on the sequencer, for a chunk another node's `--write-mode back` close forwarded while it was still uploading: how long a read of it, or a pass that must leave nothing pending (an `fsync` barrier, a forced publish, an unmount's final flush), waits for the uploader's report. Past it the read fails with `EIO` and the pass leaves the manifest unshipped |
| `CONSTELLATION_OWN_RECORD_WAIT_MS` | `10000` | ms; `0` turns the mechanism off | on a node that is not the sequencer, for a `--write-mode back` close forwarded with its chunks still pending here, when uploads are held on a metered network (`CONSTELLATION_PROFILE_UPLOADS=unmetered-only`). The sequencer ships the close's records, and those of every later op depending on it (a `chmod` or `rename` of the file), only once those chunks are up, and its answer to the forwarded op names the files and says whether anything else brings the op's records back first: when nothing does (no backup holds them, so no pre-S3 stream carries them; the stream stops before them at a chunk this node lacks; `ack=s3`; a delegate's execution) the node uploads them at once. When the sequencer's stream carries them, they stay held, and this is the safety net: an op still waiting this long (the stream stalled, say its backup was lost) uploads them anyway, and an upload is repeated this often while the op still waits. Long on purpose: a backed stream answers in milliseconds, and a grace near that would upload on the metered network whenever a loaded host is slow. `0` restores the 120 s stall ending in doubt (for reproducing it). On the sequencer, a reply parked for its acknowledgement (`ack=s3`) keeps what its transaction waits for this long before working it out again (it is re-sent every `recall_hold_ms`) |
| `CONSTELLATION_ENCODE_CONCURRENCY` | `min(CPUs, 8)` | workers, positive | parallel chunk compression/encryption gate |
| `CONSTELLATION_COMPOSE_CONCURRENCY` | `min(CPUs, 8)` | chunks, positive; `1` composes one at a time | how many dirty chunks one flush (`close`, `fsync`) composes at once, its own thread included: each chunk's committed content with the session's writes laid over it, hashed and kept durably in the chunk cache (`fdatasync`) before the manifest commit. Finding 09: 8 KiB random writes with an `fsync` every 16 dirty ~10 chunks of a 64 MiB file per `fsync` |
| `CONSTELLATION_COMPOSE_MEMORY` | `67108864` (64 MiB) | bytes | node-wide cap on the memory the compose workers beyond each flush's own thread hold, two chunks each: 8 at 4 MiB chunks, none at 64 MiB chunks (one chunk at a time, as without it) |

### S3 client

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_S3_MAX_RETRIES` | object_store default | count | override `RetryConfig.max_retries` for `s3://` backends. One request's retry series: an `fsync` no longer depends on it (plan 39 — it retries transient failures itself, see `--fsync-timeout`) |
| `CONSTELLATION_S3_RETRY_TIMEOUT_MS` | 30000 | milliseconds | override `RetryConfig.retry_timeout` for `s3://` backends (object_store's own default is 180 s). Bounds one request series, not an `fsync` |
| `CONSTELLATION_FSYNC_TIMEOUT` | unset | duration (`2s`, `500ms`, `1m`) | default for `mount --fsync-timeout`, the opt-in `soft` bound on an `fsync`'s wait for S3 (see [Mount flags](#mount-flags)) |
| `CONSTELLATION_CHUNK_HANDOFF_AFTER_MS` | 6000 | milliseconds, 0 disables | a drain (write-through close, a non-holder's forwarded close, `fsync`) waiting while this node's S3 path makes no progress (no upload and no other S3 request completes, while one of its requests waits that long or failed unanswered) for this long hands its chunks to a peer that can reach S3, which uploads them. The drain looks again as soon as its first request has waited this long, so the handoff starts about this long after that request |
| `CONSTELLATION_S3_STALL_MS` | 6000 | milliseconds, 0 disables | EC2 campaign 8 A-1: no S3 request of this node completing for this long, while one of them waits that long (or failed unanswered), marks its S3 path stalled (`status.own_s3.stalled`). Metadata ops then keep forwarding to the holder over P2P instead of taking the lease path, which needs S3. See [durability and failover](features/durability-and-failover.md) |
| `CONSTELLATION_S3_LESS_OP_DEADLINE_MS` | 20000 | milliseconds | while this node's S3 path is stalled (and not every peer reports S3 down), a metadata op not answered this long after submission fails with `EIO` (in doubt) instead of waiting for the 2 × TTL deadline |
| `CONSTELLATION_CAS_BUSY_RETRIES` | `5` | count | plan 30 M4: how many times a conditional PUT answered `409 Conflict` (another conditional write on the key in flight) is retried as the same attempt, backing off 50 ms doubling to 1 s, before the round reports the store's error. See [write-path hygiene](features/write-path-hygiene.md) |

### Existence hints

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_EXISTENCE_BLOOM_BYTES` | `4194304` | bytes, minimum `8` | local S3 existence bloom RSS cap |
| `CONSTELLATION_EXISTENCE_PEER_HINT` | `on` | boolean | use peer cache membership (exact mirrors, or blooms in `bloom` mode) as existence hints; `CONSTELLATION_COOP=off` also disables this path |

An upload consults, in order, peer cache membership, this node's bloom
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
| `CONSTELLATION_GC_HORIZON_S` | `604800` | seconds | age before unreferenced chunks are eligible (`0` for tests); also the age before a `snaps/` object with no snapshot row is deleted as an orphan (plan 32 §0.3, never younger than one lease TTL) |
| `CONSTELLATION_LOG_RETENTION_SEGMENTS` | `128` | segments | sealed log segments kept before GC |
| `CONSTELLATION_GC_SNAP_WALK` | `diff` | `diff` or `full` | how chunk GC finds snapshot chunks: `diff` walks each snapshot chain's oldest snapshot once and diffs the rest (plan 32 §0.2), with roots from `snaps/` and the replica's snapshot rows; `full` is the old walk of every snapshot's whole subtree each round, kept for one release |
| `CONSTELLATION_GC_SPILL_CACHE_MIB` | `64` | MiB of decoded chunk lists; `0` caches nothing | cap on the spilled chunk lists a `diff` snapshot walk keeps decoded during one GC pass (plan 32 §0.2). The cache is freed when the pass ends; a list evicted before it is read again, or larger than the cap, is fetched from the bucket again, so a smaller cap trades GETs for memory in the daemon running GC |
| `CONSTELLATION_SNAPACCT` | `auto` | `auto`, `on`, `off` | the node-local snapshot space-accounting index (plan 32 §6.3, `<state dir>/snapacct/`; advisory, never published, never consulted by GC). `auto` does nothing until a size is asked for, then builds the index and keeps it maintained while the node has a snapshot-policy root (`user.constellation.snapshots`) or its web UI is serving; otherwise it catches up per query. `on` builds at start and always maintains; `off` never builds (size queries answer "off"). A *size request* is `snapshot ls` showing a size column (or `--json`; control `snapshot.list` with `sizes: true`), `snapshot space`, `snapshot delete --dry-run` and a multi-snapshot delete's confirmation, and control `snapshot.reclaim`/`snapshot.space`/`snapshot.space.verify`; a plain `snapshot.list` only shows sizes the index already has. While the index builds, sizes read `building (N%)`; with `off` they read `-`, and `snapshot space` and `snapshot.reclaim` refuse. An unknown value reads as `auto` |
| `CONSTELLATION_SNAPACCT_REFRESH_S` | `60` | seconds, ≥ 1 | how often a maintained index diffs the newest commit this replica has applied against the one its live flags are as of (the "as of" of every number), and re-reads the snapshot rows as a backstop to the change hint. A policy's `budget=` (plan 32 Step 8) acts only on an index whose last pass is at most 2 × this old (else the run counts `budget_stale` and deletes nothing for the budget), and a `budget=` root with no index yet under `auto` starts its build at the first expiry run |
| `CONSTELLATION_SNAPACCT_BUDGET_MS` | `500` | milliseconds, ≥ 1 | a pass's time budget, checked between index operations (one full walk of a chain's first snapshot is not split; a pass always makes at least one) and between the spilled-list GETs of a full live-flag recompute (one per file over 32 MiB on a new index); while building, the task rests as long as it worked. The build resumes where it stopped after a restart |
| `CONSTELLATION_COMMIT_RETENTION` | `64` | commits, at least 1 | newest plan 28 metadata commits always kept by GC |
| `CONSTELLATION_COMMIT_RETENTION_S` | `86400` | seconds | commits younger than this are kept however many there are; a commit is deleted only when it is outside the newest `CONSTELLATION_COMMIT_RETENTION` *and* older than this |
| `CONSTELLATION_COMPACT_BYTES_PER_S` | `33554432` (32 MiB/s) | bytes per second; `0` unpaced | read budget for metadata pack deletion and compaction in a GC round |
| `CONSTELLATION_GC_THREADS` | one per core | threads; `0` means one per core | width of the metadata mark and pack rewrite pools |
| `CONSTELLATION_COMPLETION_RETENTION_S` | `900` | seconds | plan 30 M2: the floor below which a log segment is never pruned (see below and [Exactly-once and speculation](#exactly-once-and-speculation)) |
| `CONSTELLATION_HOLD_REFRESH_MS` | half the lease TTL | milliseconds | how often a node re-stamps its open-orphan hold (`holds/<node>.json`, DESIGN.md §3); the hold expires three refresh periods after its last write |

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
pruned past refuses instead of replaying from the gap.) A replica the
log was pruned past — at mount, on a running tail, or when it tries to
take the lease — is rebuilt from the head commit rather than left
probing a deleted slot (DESIGN.md §14 "Falling behind segment GC";
`CONSTELLATION_LOG_GAP_CHECK_MS` above).

The `_gc` lease is renewed through the round and every delete batch is
fenced on it (a CAS on the lease object); a round that lost the lease to
another round stops at once. A round's candidate pass re-lists hashes
the current condemned pointer already names, so the pointer always
carries everything an interrupted round could still delete.

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
| `CONSTELLATION_PRUNE` | on | exactly `0` or `false` disable it (`off` does not) | master switch for the background pruner |
| `CONSTELLATION_PRUNE_INTERVAL_S` | `3600` | seconds | background prune tick interval |
| `CONSTELLATION_PRUNE_GRACE_S` | `86400` | seconds | quiet period after a marked directory's ctime changes |
| `CONSTELLATION_PRUNE_MAX_LAG_S` | `300` | seconds | replica-staleness refusal threshold |
| `CONSTELLATION_PRUNE_SCAN_BUDGET_MS` | `5000` | milliseconds | per-run walk budget before the cursor is saved |

### Snapshot schedules

Snapshot policies are stored per directory in the
`user.constellation.snapshots` xattr (plan 32). Every node runs a
scheduler ticker; one of them leads, through the `_snapsched` singleton
lease, and creates the due snapshots at the root-lease holder (creation
never moves the write lease). With no policy anywhere a tick makes no S3
request at all. After creating, the leader **expires** what the policy no
longer keeps (plan 32 Step 4): only unheld `auto` snapshots of a root whose
policy parses and is not paused, never a manual, held (`csi:`, `user:` or
plain) or orphaned one, never while a refusal gate is up. Which snapshots
go is `retention::evaluate` over the replica's rows; the clock only decides
when. After a root's policy changes — or the first time the scheduler sees
a root — a grace window keeps everything the previous policy (or, for a
first sighting, anything) still keeps; the windows are recorded in
`snapsched/state.json` (the leader writes it with an ETag CAS; a lost CAS
deletes nothing that run). Pausing and resuming is not a change.
`CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS` (skip-empty) is in the
snapshot table above. The policy grammar, the retention rule and the
space columns are in
[`features/snapshot-policies.md`](features/snapshot-policies.md). A policy's
`budget=` setting parses but nothing enforces it yet. Space accounting
knobs (`CONSTELLATION_SNAPACCT*`) and `CONSTELLATION_GC_SNAP_WALK` are in
[Garbage collection](#garbage-collection).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_SNAPSCHED` | on | `0`, `false`, `off` disable it | whether this node may lead the scheduler; a disabled node still executes batches as the root-lease holder and still answers `snapshot.sched.status` |
| `CONSTELLATION_SNAPSCHED_TICK_MS` | `10000` | milliseconds | scheduler tick on every node; a due snapshot is taken within one tick of its bucket's start |
| `CONSTELLATION_SNAPSCHED_MAX_LAG_S` | `300` | seconds | a tick refuses (`refused_lag`) when the replica trails the log tail by more than this |
| `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT` | `5000` | snapshots | a policy root with this many live auto snapshots (held ones included) gets no more: `capped_roots`, `last_error` naming the root, and an error in `snapshot.sched.status` |
| `CONSTELLATION_SNAPSCHED_GRACE_S` | `86400` | seconds | the grace window after a root's policy changes (canonical form, `paused` ignored) or the root is first seen: until it closes, expiry deletes only what the old policy (every policy replaced inside the window) also expires, and nothing at all for a first sighting; survivors count `skipped_grace` (per run, so the same survivors count again every run). Each window is recorded with its end (this length added when it opens), and a node keeps a window open until the later of that end and its own length: a leader configured shorter (or `0`) never closes a window opened under a longer one. `0` opens no new windows. Wall-clock time: a leader whose clock is ahead closes windows early by that much |
| `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` | `60` | seconds | the least time between two expiry runs of one policy root |
| `CONSTELLATION_SNAPSCHED_MAX_DELETES` | `500` | snapshots | the most one root's expiry run deletes, the tier rule's victims and then the `budget=` ones together (the budget waits while the tier rule alone fills it); the rest go in the next run |
| `CONSTELLATION_SNAPSCHED_EXPIRE_BATCH` | `32` | snapshots, `1..256` | victims per delete batch; before each batch the leader renews `_snapsched` (a fenced renewal stops the run) and re-reads every victim (gone, held, re-owned → `skipped_reverify`). `1` renews and re-reads per victim |
| `CONSTELLATION_SNAPSCHED_RESIGN_AFTER` | `3` | ticks, `0` = never | a leader whose snapshot batches cannot reach the root-lease holder over P2P for this many ticks in a row gives `_snapsched` back and stays out for one lease TTL, so a node that can reach the holder (the holder itself always can) leads instead |

The `_snapsched` lease's TTL is `CONSTELLATION_LEASE_TTL_MS` (default
60 s), raised to at least three ticks. `constellation snapshot sched status`
shows, per node, whether it leads, the counters, and every policy root's
state and next snapshot; `snapshot sched run [--dry-run]` runs one tick now.
A node reports `leader` only until its own lease deadline passes: a leader
whose runtime stalled past it says "not leading" even before its next tick
finds the lease taken. The JSON (`--json`, and `node.status`'s `snapsched`)
also carries `lease_epoch` (the epoch this node last took or renewed) and,
while leading, `lease_until_unix_ms`.
A leader that cannot reach the store (an S3 outage) counts every due root's
snapshot as `create_failed` on each tick, and catches up with one snapshot
(not one per missed bucket) once the store is back. `create_failed` counts
once per due root per tick, so a 90 s outage with one root and a 1 s tick
reads about 90; the `constellation_snapsched_create_failed_total` counter on
`/metrics` is the same number, so read its rate, not the value, as "ticks
that could not create".

`constellation snapshot policy rm <fs:path> --expire` deletes a root's
unheld auto snapshots together with its policy, after confirming their
count (`--yes` skips the question); without `--expire` they are kept,
orphaned. `snapshot ls` shows, for each auto snapshot of an armed root,
`KEPT BY` (`5m·1h·1d`, `last`, `grace`) and `EXPIRES` (`in 6d 23h`, `now`,
`never`). Every deletion is recorded in the tick's `snapsched/journal/`
object with its reason.

### FUSE and runtime threads

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_FUSE_THREADS` | host heuristic (Linux), else `1` | threads, `1..64` | FUSE dispatcher threads |
| `CONSTELLATION_TOKIO_THREADS` | `min(CPUs, 32)` | threads, `1..32` | multi-thread Tokio worker count |
| `CONSTELLATION_BLOCKING_THREADS` | `clamp(4×CPUs, 4..256)` | threads | Tokio blocking-pool ceiling |
| `CONSTELLATION_COMPLETION_THREADS` | `64` | threads, `1..1024` | the engine's completion pool (one per process): threads that finish a deferred op — today a *cold* read, whose chunk is in no local cache — so the FUSE worker that took the request is free at once. Started on demand, exit after 30 s idle; past the bound, deferred reads queue (plan 31 C7b) |
| `CONSTELLATION_DEFER_COLD_READS` | on | `0`/`false`/`off` disable | whether a cold read defers to the completion pool (on) or waits on the FUSE worker as before plan 31 C7b (off; a diagnostic switch — with 4 workers and 16 concurrent cold reads behind 200 ms of S3 latency, a cached `open`+`read` then waits up to ~1.3 s for a worker) |
| `CONSTELLATION_HANDOVER_READ_DRAIN_MS` | `30000` | milliseconds | how long a FUSE session handover (`daemon --upgrade`) waits for deferred reads still being answered before it gives up and keeps serving in place (a socket handoff's `drain_timeout_ms`, plan 37 §8, overrides it per request) |
| `CONSTELLATION_HANDOFF_STANDBY_TIMEOUT_S` | `300` | seconds | how long `serve --handoff-socket`, finding its state dir held, waits as a handoff standby for a sealed handoff before it exits 0 (plan 37 §8; a CSI engine pod's startup and liveness probes also ask the handoff socket, so kubelet lets it wait). Once sealed, it waits for the sender's commit until the seal's `deadline_ms`, and after the commit for the state dir without a bound |
| `CONSTELLATION_FUSE_WRITE_REPAIR` | `auto` | `auto`, `on`, `off` | plan 42 W2: before a write-through write that may have raced a drop of its pages (race 1) is answered, the pages reads fetched since that drop are invalidated, from a repair lane (two threads and a watchdog per session); a read no known drop explains marks the file instead (its next open drops the pages). `auto`: on unless the kernel keeps write-through folios under writeback for the `WRITE` (patch 0001; see `CONSTELLATION_FUSE_KERNEL_PROBE`); `on` forces it (on a kernel with the fix every repair of the write's own pages waits for its bound); `off` turns every repair into a mark. Counted per mount in `fuse.mounts[].kernel_cache` (`repairs`, `repair_pages`, `repairs_late`, `repairs_released`, `repair_wait_ms`, `repair_wait_max_ms`, `ambiguous_writes`) |
| `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS` | `1000` | milliseconds, positive | plan 42 W2's bound: a repair whose notification has waited this long while nothing else of the file is in flight on the daemon (the write alone) is abandoned — the write is answered and the file marked (`repairs_late`). Time a repair waits for another request of the file is not counted. A repair that waits on the write's own last page (the writer keeps it locked until its reply, so no read can have made it stale) is answered after 10 ms (`repairs_released`): every writer of the file waits for it |
| `CONSTELLATION_FUSE_DIR_ATTR_TTL` | `auto` | `auto`, `zero`, `entry` | plan 42 W3a: `zero` answers every directory's attributes with an attribute TTL of 0 (the entry TTL kept), closing race 2a at a `GETATTR` per directory component of every path walk (the mount uses `default_permissions`); `entry` keeps the view's TTL. `auto`: `zero` under `--cto strict` (a lone sequencer's TTL included) unless the kernel marks attributes valid only after storing them (patch 0002); in bounded mode `entry` (plan 42 Q1, option B: `zero` made warm `git status` 2.6 to 7.9 times and `make -q` 2.8 to 4.2 times slower on ext4, xfs, btrfs and tmpfs); set `zero` to close race 2a there at that cost |
| `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY` | `auto` | `auto`, `on`, `off` | plan 42 W3b: every namespace reply (`mkdir`, `mknod`, `symlink`, `link`, `create`, `unlink`, `rmdir`, `rename`) is preceded by an attribute-only `FUSE_NOTIFY_INVAL_INODE` for each directory it changed and each inode a rename moved, so a `GETATTR`/`LOOKUP` reply from before the change is rejected (race 2b). The notice invalidates the inode's whole attribute mask, so each one costs the next path walk through a directory a `GETATTR` (the mount uses `default_permissions`); `auto` therefore writes it, in bounded mode, only for an inode an attribute reply may still be on its way to the kernel for (a request being served across the change, which then notices itself, or a reply answered to a task that has sent no attribute or namespace request since and has not exited, however long ago: a mark is never dropped by time, so a reply the kernel installs late under load is covered; the marks are bounded, and a full table writes the notice before the reply instead; plan 42 F21), and for every operation under `--cto strict`, where a directory's attributes have no lifetime anyway; for the directories, off when the kernel bumps a directory's attribute version when it changes it (patch 0003); the notice for the inode a rename moved stays on there, because 0003 does not bump that inode's version (its `ctime` would stay stale, plan 42 F16). `on`: every operation, in either mode (diagnosis; a `GETATTR` per operation in a busy directory). `off`: none. Counted per mount in `fuse.mounts[].kernel_cache` (`dir_attr_notices` written, `dir_attr_notices_skipped` changed inodes no reply was on its way for) |
| `CONSTELLATION_FUSE_READDIRPLUS` | `auto` | `auto`, `always`, `off` | plan 40: whether the mount asks the kernel for `READDIRPLUS` (`FUSE_DO_READDIRPLUS`), which answers each listed name with what a `LOOKUP` of it answers, so `ls -l` needs no request per name. `auto` (with `FUSE_READDIRPLUS_AUTO`): the kernel asks for it in a directory it is stat'ing and for the first chunk of any listing; `always`: for every chunk (benchmarks, workloads that stat everything); `off`: plain `READDIR` only. Each entry with a node id is a kernel lookup reference, counted like a `LOOKUP`'s, and a directory's attributes keep `CONSTELLATION_FUSE_DIR_ATTR_TTL`'s rule. A kernel that does not offer it leaves the mount on `READDIR` (logged once at info). An unknown value fails the mount. Read once, when the daemon starts; a session resumed by `daemon --upgrade` under `off` still answers the plus requests its kernel already negotiates, name-only. What the connection agreed, the pages answered and the entries handed with attributes or name-only (with the reason) are per mount in `node.status` and `/metrics` ([Listing and `READDIRPLUS` in the status](#listing-and-readdirplus-in-the-status)) |
| `CONSTELLATION_FUSE_KERNEL_PROBE` | `auto` | `auto`, `off` | plan 42 §5.4: at the daemon's first mount, a background thread mounts a private in-memory probe filesystem (a temporary directory under `$TMPDIR`), tests whether the kernel carries patches 0001 and 0003, and unmounts it (a few hundred ms, about 2 s on a kernel with 0001; until it answers the release table decides; a daemon that exits meanwhile gives it 200 ms and then aborts it). `off`: the table and the overrides decide. The verdicts are in `node.status`'s `fuse.mounts[].kernel_workarounds.verdicts` (`fixed`/`not_fixed`/`unknown`, source `probe`/`table`/`override`) and the daemon's log |
| `CONSTELLATION_PREOPENED_INIT_WAIT_MS` | `10000` | milliseconds | how long a `view.mount{PreopenedFd}` waits for the connection's `FUSE_INIT` before refusing the descriptor as not a fresh mount (a handed-over connection is resumed, never re-initialised) |
| `CONSTELLATION_FUSE_KEEP_CACHE` | `auto` | `auto`, `off` | plan 42 W1: whether an open may keep the file's pages in the kernel's page cache (`FOPEN_KEEP_CACHE`). `auto`: only when the open finds the file's committed content unchanged since the last open that dropped its pages, that drop known complete, and nothing seen since could have left a page that differs from what a read answers (a data invalidation written or lost, a failed or short write, a direct-I/O open, a write the race-1 bookkeeping could not vouch for); never under `--cto strict`, with `CONSTELLATION_KERNEL_INVALIDATE=0`, or for a passthrough open. `off`: every open drops the pages, as before plan 42 (diagnosis). Not tied to the kernel: keeping current pages is a speed win with or without the kernel fixes. Read once at start; any other value fails the mount. Counted per mount in `node.status`'s `fuse.mounts[].kernel_cache` (`keep_cache_opens`, `dropping_opens` by reason, `suspect_marks` by mark) and `/metrics` (`constellation_fuse_keep_cache_opens_total`, `constellation_fuse_dropping_opens_total{reason}`, `constellation_fuse_suspect_marks_total{mark}`) |

The control socket is not served by these runtimes: a thread of its own
(`cst-control`, one more per server, with a single-threaded runtime of its
own and idle at zero cost) accepts, shakes hands, reads requests and writes
each call's final response, so a `Hello` is answered while every worker of
the shared runtime is busy. Only `node.status` and `node.ping` are answered
independently of it: the other methods' bodies run on the shared runtime's
blocking pool, many parking on the runtime inside, and handlers that are
futures and streams (whose `Event`/`Chunk` frames are written from the
shared runtime) wait for a worker (see the module docs of
`crates/control/src/server.rs`). `node.ping` is answered off the runtime
but fails (`Failed`) once the runtime's lag probe shows it has run nothing
for 30 s, so a CSI engine pod's liveness/startup/readiness probe
(`control-relay --ping`), the controller's `Identity.Probe` and a `mount`'s
attach check (`daemon_socket_is_live`) still tell a wedged daemon from a
slow one: long enough for a store stall or a transient hog, short enough
to get a wedged pod restarted. The `--await-unlock` credential gate and a
handoff standby answer a plain ping (they have no runtime to attest).

The CSI node plugin (`constellation-csi --node`; the chart sets these from
`engineProfile.handoff` and `node.metricsPort`):

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_CSI_HANDOFF_DRAIN_TIMEOUT_MS` | `5000` | milliseconds | plan 37 §8 step 2: how long an engine pod being replaced may wait for the reads and `fsync`s it answers off its FUSE workers (an op held on a worker is not bounded by it: the total is) |
| `CONSTELLATION_CSI_HANDOFF_TOTAL_TIMEOUT_MS` | `30000` | milliseconds | every step of an engine-pod handoff up to its commit; the old pod serves again by itself (and refuses a commit) 5 s after it |
| `CONSTELLATION_CSI_HANDOFF_RESUME_TIMEOUT_MS` | `300000` | milliseconds | after the commit, how long the replacement may take to serve before the handoff is left unresolved (the replacement is kept, pending, and adopted once it serves — never deleted on this timeout) |
| `CONSTELLATION_CSI_HANDOFF_MAX_ATTEMPTS` | `3` | attempts | handoffs tried per engine pod and desired spec before the rollout leaves the old pod serving |
| `CONSTELLATION_CSI_ROLLOUT_INTERVAL_S` | `30` | seconds, `0` off | how often the node plugin looks for engine pods whose spec drifted |
| `CONSTELLATION_CSI_METRICS_ADDR` | unset (off) | `host:port` | the node plugin's `constellation_csi_handoff_*` counters as Prometheus text at `GET /metrics` (the chart sets it from `node.metricsPort`, off by default) |

### Listing and `READDIRPLUS` in the status

The settings are `CONSTELLATION_FUSE_READDIRPLUS` (FUSE and runtime
threads, above) and the four `CONSTELLATION_READDIR_*` variables
(Prefetch and scan-ahead, above). What they do is visible in `node.status`
and `/metrics` (plan 40 §4.11).

Per mount, `fuse.mounts[]`:

| Field | Metric | What |
|---|---|---|
| `readdirplus` | `constellation_fuse_readdirplus{mode}` | the mode `FUSE_INIT` agreed: `off`, `auto` or `always` (a kernel that does not offer it: `off`; a session resumed by `daemon --upgrade`: its connection's, not the new image's knob) |
| `readdirplus_pages` | `constellation_fuse_readdirplus_pages_total` | `READDIRPLUS` pages answered |
| `readdirplus_entries`, `readdirplus_name_only` | `constellation_fuse_readdirplus_entries_total{kind="attr"\|"name_only"}` | entries the kernel's buffer took, with attributes (each a lookup reference the kernel gives back by `FORGET`) or name-only (the kernel `LOOKUP`s the name when it needs it) |
| `readdirplus_name_only_why` | `constellation_fuse_readdirplus_name_only_total{reason}` | the name-only ones by reason: `dots` (`.` and `..`, always), `view` (the view could not vouch for the attributes: `--cto strict` on a node that is not a lone sequencer, or a name removed or renamed between the page's names and its attribute read), `watch_full` (the table of attribute reply marks is full: a pid-0 lister of a huge directory), `knob_off` (this image has `off` but the connection still asks for plus), `held` (the kernel already holds the name's dentry, leading to the listed inode, from a lookup with more than a quarter of its TTL left: a listing right after the names were looked up, as `git status` makes) |
| `unlinked_refs` | `constellation_fuse_unlinked_refs` | files the view unlinked and keeps for the kernel's lookups until their `FORGET`; steady growth is a lookup the kernel never gives back |
| `kernel_refs`, `forgets_*` | `constellation_fuse_kernel_refs`, `constellation_fuse_forgets_total{outcome}` | the kernel's lookup counts |

Only a page that succeeded is counted, and only what the kernel's buffer
took. `ls -l` of a cold directory under `auto` should show few `lookup`
operations per listed name: `readdirplus_entries` rises and the
`op="lookup"` series of `constellation_vfs_ops_total` does not. Operation
counts and latency of the pages are the `op="readdirplus"` series of
`constellation_vfs_ops_total` and `constellation_vfs_op_seconds`.

Per node, `readdir` (the hint cache is shared by every view):

| Field | Metric | What |
|---|---|---|
| `hint_hits` | `constellation_readdir_hint_hits_total` | pages resumed from a remembered page: `O(page)` |
| `cold_scans`, `cold_scan_entries` | `constellation_readdir_cold_scans_total`, `constellation_readdir_cold_scan_entries_total` | pages resumed by scanning the directory from its start (a hint miss: a `seekdir`, an evicted page), and the entries those scans read. Many of them with a large `cold_scan_entries` is a lister the cache is too small for (`CONSTELLATION_READDIR_HINT_BYTES`) |
| `lost_cookies` | `constellation_readdir_lost_cookies_total` | cold resumes whose name was gone: answered `ENOENT` (see `CONSTELLATION_READDIR_HINT_BYTES`) |
| `cookie_collisions` | `constellation_readdir_cookie_collisions_total` | equal cookies of different names seen (rare at 62 bits, common at scale with `CONSTELLATION_READDIR_COOKIE_BITS=31`) |
| `hint_bytes` | `constellation_readdir_hint_bytes` | bytes the hint cache holds (bounded by `CONSTELLATION_READDIR_HINT_BYTES`) |
| `open_streams`, `pinned_dirs` | `constellation_readdir_open_streams`, `constellation_readdir_pinned_dirs` | directory handles open, and the directories they are on |
| `pinned_bytes`, `exempt_bytes` | `constellation_readdir_pinned_bytes`, `constellation_readdir_exempt_bytes` | bytes pinned for open directories (bounded by the two pin settings), and the open streams' last pages kept past those bounds |
| `pin_overflows` | `constellation_readdir_pin_overflows_total` | times a pin bound unpinned pages |

### Filesystem stats and quota

Used space reported by `df`/`statfs` is the logical sum of reachable file
sizes under the mounted view (same semantics as `user.constellation.rsize`
at the mount root), not physical S3 bytes after dedup or compression.

A whole-filesystem mount answers from an in-memory counter maintained on
commit, so it is exact and costs nothing. A subtree or snapshot mount has
to walk its own root instead, and that aggregate is cached:

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_SNAPSHOT_MANIFEST_CACHE` | `8192` | entries | per mounted view: snapshot files' manifests kept in memory (least recently used evicted), shared by the reads of a snapshot file and the passthrough check at its open, so a snapshot file's manifest is loaded from the snapshot's tree once rather than at every read. Snapshot manifests are immutable, so nothing is ever invalidated; an entry is a few hundred bytes (a longer chunk list is a separate blob, not cached here). `0` disables |
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
| `CONSTELLATION_NO_DAEMONIZE` | unset | presence (any value, even empty) | force `--foreground` behavior for every `mount` (CI/harness convenience) |
| `CONSTELLATION_SHUTDOWN_STALL_S` | `120` | seconds, positive | an unmount's drain that makes no progress (the journal and pending uploads stop shrinking, and the final metadata publish reads, writes and locates no tree nodes) for this long gives up and exits non-zero, leaving everything on disk for the next mount; a waiting `umount` then fails with what was left |
| `CONSTELLATION_ATTACH_TIMEOUT_MS` | `120000` | milliseconds, positive | how long a `mount` that found `daemon.lock` held keeps trying to attach to a daemon that does not answer; also how long a `mount` waiting for a draining daemon's exit tolerates that daemon not answering (the wait itself has no wall-clock bound: it lasts as long as the drain makes progress) |

### Logging

A daemon (`mount`, `serve`, a resumed `daemon --resume-from`) never waits
for its log's sink: every log line goes into a queue and one writer thread
writes the queue to stderr (`daemon.log`, a container's pipe, a harness
`mount.log`). When the sink stalls longer than the queue holds, the
incoming line is dropped and counted, `WARN` and `ERROR` keep the queue's
last eighth to themselves, and one `log lines dropped` line says how many
were lost once the sink catches up; `constellation status` and `/metrics`
carry the counters (`log.dropped_lines`, `constellation_log_dropped_lines_total`).
The recent lines stay in the in-memory ring `constellation log tail` reads
whether or not the sink got them. A short-lived command (`status`,
`umount`, `fs create`, ...) writes to stderr itself, as it always did.
What the daemon had queued is written before it exits, before the
handover's `exec` (an upgrade first drains the log, up to 2 s, while the
views are still served, and the last wait before the `exec` is 20 ms; what
is still unwritten then travels to the new image in a memfd and is the first
thing it writes, in order; only what cannot be carried is counted as dropped
lines by the new image) and
before an abort. A write to the sink that fails (a full disk, an I/O error)
drops its chunk, counted the same way (`log.sink_errors` too). A `kill -9`
loses what was queued. `RUST_LOG` filters as before (default `info`).

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_LOG_QUEUE_BYTES` | `16777216` (16 MiB) | bytes; below `65536` is raised to it; unparsable keeps the default (with a warning in the log) | the daemon's log queue bound, read once at start, each line counted with its overhead until it is written. At the info level a busy node writes a few KB/s, so the default holds minutes of a stall. Memory is taken only while lines wait (a queue of 1 024-entry blocks, freed as the writer consumes them) and stays within the bound, plus at most 64 panic messages, the `log lines dropped` summaries the bound's reserve holds, and the queue's slack: two part-used 16 KiB blocks, up to four spare blocks kept by the queue and four emptied ones held by the writer (at most 128 KiB in all), and the writer's chunk buffer (256 KiB plus one line, shrunk after 1 MiB). A line's allocation is freed once copied for writing, while its bytes stay counted until the write returns. **`0` keeps the synchronous writer** (a slow log sink stalls every thread that logs): for A/B measurements and failing-before tests only |
| `RUST_LOG` | `info` | `tracing` filter | which events are logged (`aws_config` stays at `warn` unless named). The `log lines dropped` line ignores the filter |

### Control UI

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_WEB_UI_PORT` | `0` (disabled) | TCP port | localhost control UI; bare `--web-ui` listens on `8080` |

### Control allowlist (plan 31 §9.5 / plan 33 U1)

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_CONTROL_POLICY` | `<config dir>/control-allow.toml` | path | the control-protocol allowlist (below); a missing file means only the daemon's own uid may connect, as `admin` |

The daemon's own uid is always `admin` and is not expressible in the file (a
truncated or malformed allowlist can only narrow access, never widen it past
the owner); the file is otherwise a pure allowlist — absence of a matching
`[[grant]]` denies even `viewer`. A parse error is loud (logged at `error`)
and never falls open: the daemon runs with the owner-only policy — its own
uid as `admin`, every other principal denied, including a `kind = "service"`
grant below. The file is read once at startup; there is no reload yet (a
`SIGHUP` reload that keeps the previously loaded grants on a parse error is
plan 33's, not this subset's), so a typo takes effect on the next restart
and takes every non-owner grant with it.

```toml
[[grant]]
group = "constellation-ops"      # a group name, or a numeric gid
role = "operator"

[[grant]]
uid = 1001
role = "viewer"

[[grant]]
device = "ed25519:AbC…"          # a remote device key (plan 33's pairing flow)
role = "viewer"

[[grant]]
sid = "S-1-5-21-…"               # a Windows SID (plan 35)
role = "admin"
```

Exactly one subject key per entry (`uid`, `group`, `device`, `sid`, or
`kind = "service"` below); `role` is mandatory on every entry. Group *names*
are resolved to gids when the file is loaded; an unresolvable name is a load
error. A principal's role is the highest of every entry that matches it,
regardless of file order.

**Service principals** (plan 33 U1, for plan 37's CSI node plugin): a
`kind = "service"` grant additionally narrows a `unix` grant to one listening
socket, for an automated caller that shares a uid — often `0` — with
unrelated processes on the host:

```toml
[[grant]]
kind = "service"
principal = "uid:0"
socket = "/var/lib/constellation-csi/sockets/pv-1/control.sock"
role = "admin"
label = "csi-node-plugin"
```

`principal`, `socket` and `label` are all mandatory on a `service` entry
(and are rejected on any other kind). It matches only when the caller's peer
uid equals the one in `principal` **and** the connection arrived on the
daemon's own listening socket at exactly that canonical path (never a path
the client claims). A `socket` that does not exist yet when the allowlist
loads is accepted — the pod may create it after the grant is written — and
logged at `debug`, not an error; a `socket` that exists but cannot be
resolved (an unreadable parent directory, a symlink loop) is logged at
`warn`, because the unresolved path will not match a daemon whose own bound
path canonicalised differently.

Audit entries (below) from a matched service grant carry
`"principal":{"kind":"service","uid":0,"socket":"…","label":"csi-node-plugin"}`
instead of the usual `"unix:uid=…"` string, so the trail distinguishes an
automated driver from a human operator at the same uid. This holds whenever
the service grant matched, even if a broader row (or the owner rule) granted
the caller a higher role — the row that actually set the role is in the
daemon's `debug` log of every mutating call, alongside the resolved role and
the method's minimum.

The audit log of mutating calls (`control-audit.jsonl` in the daemon's state
dir) is one JSON line per call; see the `constellation_control::audit` module
docs for its exact fields. A call whose client named whom it acts for
(`Request.on_behalf_of`, e.g. the CSI driver's PersistentVolume name, plan
37 K6a) carries `"on_behalf_of":"pvc-…"` in its line; it is the caller's
claim, validated to at most 253 bytes of `[A-Za-z0-9._:/@-]` and never read
by authorization. The plan 37 CSI node plugin's grant on an engine pod is
`role = "admin"` (its `view.mount`/`view.unmount`/`fs.unlock` are
admin-only methods), one row per engine pod's socket.

**`constellation serve --await-unlock`** (plan 37 §9, K6a: Kubernetes engine
pods): the S3 credentials and an E2E passphrase come from the first
`fs.unlock` on the control socket (naming the `--s3` URL), never from the
environment (`AWS_ACCESS_KEY_ID`, `CONSTELLATION_PASSPHRASE` are not read).
Until then the socket — already under this allowlist and writing to this
audit log — answers only `node.ping` and `fs.unlock`, and everything else
`Unavailable` ("this engine is waiting for fs.unlock to supply its
credentials"). The credentials are checked against the bucket before the
daemon starts, and an E2E filesystem's passphrase against its keyring (a
wrong pair or passphrase is an `fs.unlock` error, `Unavailable`, and the
engine keeps waiting); `SIGTERM`/`SIGINT` while it waits exits 0 at once.
A later `fs.unlock` rotates them in the running engine after one trial
read of `meta.json` with the new pair: a pair S3 refuses is refused
(`Denied`; `Unavailable` when S3 could not be asked) and the pair in use
stays. The rotation shows in `fs.list` as the daemon's own entry's
`credentials_generation` and `credentials_in_use` (the generation its S3
clients last signed with). The process runs with core dumps off
(`RLIMIT_CORE` 0, `PR_SET_DUMPABLE` 0), as do both CSI plugins.

S3 error responses never reach an error text with their body: every S3
client keeps only the error's `<Code>` and a fixed message (`S3 refused
the credentials` for 401/403), because an authentication error's body
echoes the request's access key id and string to sign. A controller-owned
engine pod reads `CONSTELLATION_CONTROL_POLICY=/etc/constellation-csi/controller-engine/control-allow.toml`
from the image: one `kind = "service"` grant for uid 65532 on its socket,
`role = "admin"`, `label = "csi-controller"`.

### Fault injection (testing only)

Knobs with no purpose outside the fault-injection harness
(`crates/harness`) and its scenarios. Never set these against a real
filesystem.

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS` | `0` (disabled) | milliseconds | sleeps this long, holder-side, immediately before replying to a forwarded mutation — after the op has already executed and the keepers lock has been released. Used to make a forwarded mutation's reply race the requester's own `CONSTELLATION_FORWARD_TIMEOUT_MS` deadline deterministically (plan 30 `forward-timeout-reexec`); logs one `tracing::warn!` at startup when non-zero |
| `CONSTELLATION_FAULT_LOSE_CHUNKS` | unset | comma-separated chunk hashes (hex) | plan 30 M4: the upload pass deletes these chunks from the local cache right before reading them, so a pending upload of one finds its content gone (the "missing from local cache" condition, at a precise point; `poison-record-isolation`) |
| `CONSTELLATION_FAULT_P2P_DENY_FILE` | unset | a file path, one node id per line, re-read on every use | plan 30 M9/M11: drops P2P traffic to and from the listed nodes and reports them unconnected, for backup appends, pre-S3 streaming, delegation, lock, lease-request and promise messages (a one-sided partition) |
| `CONSTELLATION_FAULT_COMMIT_FAIL` | unset | `<suffix>` or `lapse:<suffix>` | plan 42 K1: every second publication of a file whose path below the view's root ends with `<suffix>` fails before it reaches the sequencer — as a compose/commit failure that retires the session uncommitted, or with `lapse:` as the sequencer's refusal for a lapsed lock grant (the session is discarded) — so a write session the kernel's pages hold ends without becoming the row (`kernel-race-uncommitted*`, `kernel-race-discard-local`) |
| `CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS` | `0` (disabled) | milliseconds | plan 42 K1: `getattr` of a directory sleeps this long after it read the attributes and before it answers — a reply generated before a change and processed after it (race 2b, `kernel-race-dir-version`) |
| `CONSTELLATION_FAULT_GETATTR_DELAY_INO` | unset | an inode number | plan 42 K1: narrows `CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS` to this one inode, a directory or not (`kernel-race-rename-ctime`) |
| `CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US` | `0` (disabled) | microseconds | plan 42 K1: the FUSE adapter's `write` sleeps this long before it hands the request on — a worker preempted after reading a `WRITE` (race 1's window, `kernel-race-*-delay`, `kernel-race-notify`) |
| `CONSTELLATION_FAULT_VIEW_WRITE_DELAY_US` | `0` (disabled) | microseconds | plan 42 K1: the view's `write` sleeps this long after it registered the write in flight and before it applies it (a slow server) |
| `CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT` | unset | `1` | plan 42 §7.2: an open's keep-cache decision ignores the S5 mark (the race-1 bookkeeping could not vouch for a page) — the naive keep-cache plan 42 F1 guards against, for a scenario's fail-before only; warned about at start |
| `CONSTELLATION_FAULT_UNSEEN_DROP_MS` | unset | milliseconds | plan 42 §7.2: every this often the FUSE session drops the pages of every file it knows open for writing with a data notification it announces nowhere (no arm, no mark) — a drop the write-race bookkeeping cannot see, standing in for memory reclaim (which on Linux 7.3 does not drop a page a writer holds); `kernel-race-unseen-drop` |
| `CONSTELLATION_FAULT_HOLD_DRAIN_FILE` | unset | a file path | test-only: while the file exists a clean shutdown's drain waits before it uploads or ships anything, and writes `<path>.held` once it does (a long clean-unmount drain, for `umount`/`mount` tests) |
| `CONSTELLATION_FAULT_RUNTIME_HOG_FILE` | unset | a file path whose content is `<tasks>:<block_ms>:<every_ms>[:<node>,...]`, re-read every 200 ms | while the file exists, every `every_ms` the daemon (only the listed nodes, if any) spawns `tasks` tasks that each block a worker of its shared runtime for `block_ms` (a runtime starved by blocking calls while the process and the host are fine) |
| `CONSTELLATION_FAULT_LOG_STALL_FILE` | unset | a file path whose content is `<ms>[ once][ pid=<n>]`, read at every write to the log's sink | plan 44: while the file exists, each write to the daemon's log sink first sleeps `<ms>` holding std's stderr lock, as a stalled `write(2)` to a busy disk does (on the log writer thread; with `CONSTELLATION_LOG_QUEUE_BYTES=0` on the logging thread, where every thread that logs then waits). `once` removes the file at the first write that sees it, `pid=<n>` stalls only the daemon of that pid (several share a path). Write the file whole, by a rename. It fires once per `write()` call of the writer, retries after `WouldBlock` and partial writes included, so a stall file without `once` stalls each of them. Never logs |
| `CONSTELLATION_FAULT_HOLD_SYNC_FILE` | unset | a file path | plan 30 M4: while the file exists every managed sync round returns at once (nothing uploads, ships or publishes, and the lease is not renewed — keep holds short); a held round writes `<path>.held`, so a harness knows no round is still in flight |

## Boolean values

Most boolean switches are enabled when unset, and `off`, `0`, and
`false` (trimmed, case-insensitive) disable them. The exceptions:

- `CONSTELLATION_ATIME_RO_FORWARD` defaults off and is enabled only by
  exactly `1` or `true`.
- `CONSTELLATION_P2P_RELAY` defaults off (see its row).
- `CONSTELLATION_PRUNE` is disabled only by exactly `0` or `false`.
- `CONSTELLATION_HOLDER_CAPTURE` is disabled only by exactly `0`,
  `off` or `false` (case-sensitive, not trimmed).
- `CONSTELLATION_NO_DAEMONIZE` is enabled by its presence, whatever the
  value.

Several switches are forced off when P2P is off, whatever they say:
`CONSTELLATION_LOG_STREAMS`, `CONSTELLATION_READ_DELEGATIONS`,
`CONSTELLATION_PRE_S3_STREAMING` and `CONSTELLATION_DELEGATION` (and
with it placement), and the default of `CONSTELLATION_LOCKS`.

## Build-time

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_GIT_DESCRIBE` | `git describe --tags --always --dirty` | string | version string embedded by `crates/cli/build.rs` |

## References

- [Forwarded mutations](features/forwarded-mutations.md)
- [Lease placement](features/lease-placement.md)
- [Close-to-open modes](features/cto-modes.md)
- [Durability and failover](features/durability-and-failover.md)
- [Delegations](features/delegations.md)
- [Cluster locks](features/cluster-locks.md)
- [P2P relays](features/p2p-relays.md)
- [Prefetch](features/prefetch.md)
- [Read-time atime](features/atime.md)
