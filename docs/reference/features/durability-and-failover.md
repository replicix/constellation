# Durability and failover

What a write's acknowledgement means, where acknowledged writes live
until S3 has them, and how the cluster keeps writing when the lease
holder or the bucket goes away (plan 30 M9–M10). Durability comes in
layers matched to the topology: nothing is added where no peer is near,
a synchronous backup is used where one is, and waiting for S3 is
opt-in.

## Table of Contents

- [Terminology](#terminology)
- [Choosing a mode](#choosing-a-mode)
- [Details](#details)
  - [What "on disk" means](#what-on-disk-means)
  - [Layer A: requesters keep what they were acknowledged](#layer-a-requesters-keep-what-they-were-acknowledged)
  - [Layer B: a backup within the RTT budget](#layer-b-a-backup-within-the-rtt-budget)
  - [Seal-based failover](#seal-based-failover)
  - [Layer C: `ack=s3`](#layer-c-acks3)
  - [No client observes a tentative effect](#no-client-observes-a-tentative-effect)
  - [Pre-S3 streaming](#pre-s3-streaming)
  - [`--fsync-mode` and `--write-mode`](#--fsync-mode-and---write-mode)
  - [Flexible continuation epochs](#flexible-continuation-epochs)
  - [Epochs and fast takeovers](#epochs-and-fast-takeovers)
  - [Known limits](#known-limits)
- [Configuration](#configuration)
- [Status](#status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Holder**: the node with the root lease, which sequences and ships
  the log. A delegate (see [Delegations](delegations.md)) plays the same
  role for its subtree and picks its own backup the same way.
- **Tenure**: one holder's time under one lease epoch.
- **Acknowledgement policy** (`ack_policy` on the lease object):
  `Local`, `Backup` or `S3`. It belongs to the tenure.
- **Backup**: a peer that holds a synchronous copy of the holder's
  unshipped journal.
- **Seal**: a backup's persisted "epoch *e* is sealed": it refuses every
  further epoch-*e* append, so the old holder can acknowledge nothing
  more.
- **Continuation epoch**: a group of nodes that keeps writing during a
  bucket outage by moving leases among themselves over P2P.
- **Epoch slack (`f`)**: how many write-eligible nodes a continuation
  epoch may form without.
- **Promise**: a node's persisted word, published in
  `heartbeat/<node>`, that it joins no continuation epoch before a given
  time.

## Choosing a mode

| Topology | Policy | Added ack latency | Holder crashes and comes back before a takeover | Holder away past a takeover | Failover |
|---|---|---|---|---|---|
| Single node | `Local` | none | nothing lost | n/a | n/a |
| Only distant peers | `Local` | none | nothing lost | forwarded ops are replayed by their requesters; the holder's own un-shipped writes come back when it returns, as a replay | lease TTL (60 s default) |
| A peer within the RTT budget | `Backup` (automatic) | one round trip to the backup | nothing lost | nothing lost | detection + one CAS (≈ 1.5 s measured) |
| `--ack s3`, any topology | `S3` | one S3 round trip per group commit | nothing lost | nothing lost | detection + one CAS (needs P2P) |

There is no `backup` setting: `Backup` is chosen automatically when a
peer is in budget and `--ack s3` is not in effect.

## Details

### What "on disk" means

A node's metadata store commits to OS buffers, not to stable storage. A
commit survives a process crash, but a power loss can drop the most
recent commits until something syncs them: an `fsync()` on the mount,
or an orderly shutdown. This applies to the holder's journal, to a
backup's copy, to a seal, and to a promise. Two nodes that lose power
together (the same rack) can lose Layer-B-acknowledged writes.

### Layer A: requesters keep what they were acknowledged

Always on, and free. A requester keeps the records of every forwarded op
it was acknowledged as speculation, with the op and its rid. If the
holder dies before shipping, the next holder's epoch strands that
speculation and the requester replays each op by rid through the new
holder; exactly-once identity makes the replay safe (see
[Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)).
The holder's own writes that never shipped come back when it returns,
through the same rollback and replay. Failover waits for the lease TTL.

### Layer B: a backup within the RTT budget

**Selection.** Candidates are write-eligible peers that are connected,
whose measured RTT to the holder is within
`CONSTELLATION_BACKUP_RTT_BUDGET_MS` (5 ms), and whose link has been up
for at least 2 s. The one connected longest is preferred, up to
`CONSTELLATION_BACKUPS` (1). Backups are added one at a time, at most
one reconfiguration every 3 s, except that an empty set is filled at
once. No candidate means no backup and `Local`, so a cluster whose
peers are all far away never pays a synchronous round trip.

**Append path.** The holder streams whole journal transactions to each
backup (`BackupAppend`), up to 8 appends in flight, acknowledged
cumulatively. Transactions journaled while every slot is busy ride the
next append, which is the group commit. The backup refuses appends from
a sealed epoch or an older holder, stores them in its `backup_tail`
keyspace, and replies. It trims its copy as segments ship. An idle
backup gets a heartbeat append every `CONSTELLATION_BACKUP_HEARTBEAT_MS`
(300 ms).

**Acknowledgement.** Under `Backup`, the holder answers a mutation
(accepted *or refused*: a refusal was evaluated against the same
unshipped state) only once every backup listed in the *committed* lease
object holds the journal through the position it was evaluated at. The
op has already executed; only its reply waits. The FUSE fast path is
closed under this policy, so local writes wait the same way.

**Reconfiguration.** A backup that makes no acknowledgement progress for
`CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` (1 s), or whose link goes down, is
removed by a lease CAS that bumps `config_version`. Until that CAS
lands, waiting acknowledgements still need it. A new backup is first
streamed the unshipped journal, and is CASed into the lease only once
it has caught up.

### Seal-based failover

A backup that has heard nothing from its holder for
`CONSTELLATION_BACKUP_TAKEOVER_MS` (1.5 s):

1. **seals**: persists "epoch *e* sealed" and answers every later
   epoch-*e* append with `sealed`;
2. re-reads the lease, and continues only if it still names that holder
   and epoch and still lists this backup;
3. takes the lease at epoch *e*+1 by CAS, tails S3 to head, and ships an
   epoch marker at the next log slot (a create-if-absent, which fences
   any late segment of the old holder);
4. re-applies its backup tail, deduplicated by rid, as its own journal
   (the marker records the takeover with a `TailFollows` record);
5. opens its view.

Why it is safe: the old holder needs every listed backup to acknowledge
anything it acknowledges, so after the seal it can acknowledge nothing,
whether it is dead, slow, or only cut off from this backup. A backup's
takeover CAS and the holder's CAS removing that backup are on the same
lease version, so exactly one wins. None of the timeouts decides safety;
they only decide when to try.

A node that is not listed as a backup must wait 3 s (twice the takeover
time) past a `Backup` lease's expiry before it may claim it, so a listed
backup gets there first. A new tenure starts with no backups and picks
them again. A write-back file whose chunks never reached S3 before the
holder died is held (see [Write-path hygiene](write-path-hygiene.md)),
not published naming missing chunks.

### Layer C: `ack=s3`

`--ack s3`, `CONSTELLATION_ACK=s3`, or a filesystem created with
`fs create --ack-policy s3` makes every acknowledgement wait until the
record's segment is CAS-created in the log. Sync rounds group-commit
whatever is waiting. No backups are used.

The lease then records `ack_policy = S3`, and any peer may take it over
when the holder falls silent for `CONSTELLATION_BACKUP_TAKEOVER_MS`,
before the TTL: every acknowledged record is in a log slot below the
taker's epoch marker, and the marker's create-if-absent fences the old
holder. Silence is read from the holder's log stream (it heartbeats the
stream), so fast takeover needs P2P. With P2P off, `ack=s3` still gives
S3 durability but fails over at the TTL.

The policy belongs to the tenure: it is set by the mount that acquired
the lease. A `--ack s3` node forwarding to a holder whose tenure is
`Local` gets that holder's acknowledgements.

### No client observes a tentative effect

Under `Backup` and `S3`, the holder's reads, and its answers to other
clients' ops, wait for any unshipped rows they would observe to become
durable. So no client of any node sees an effect that a failover could
roll back: for example, a holder never refuses another node's
`create f2` with `EEXIST` because of a rename it has acknowledged but
could still lose.

### Pre-S3 streaming

Under `Backup`, once a batch is backup-acknowledged the holder streams it
to its log-stream subscribers ahead of S3 (`StreamAhead`), except to its
backups, at most every 5 ms. Subscribers apply it as speculation, which
retires when the segment arrives and is rolled back if a takeover
strands it. Visibility no longer waits for S3.
`CONSTELLATION_PRE_S3_STREAMING=0` turns it off.

### `--fsync-mode` and `--write-mode`

These are older, per-mount knobs that combine with the policies above:

- `--fsync-mode local` (default): `fsync()` forces the node's metadata
  store to disk and nudges the shipper.
  `--fsync-mode s3`: `fsync()` also waits until the inode's chunks and
  records are in the bucket. It is the per-call form of Layer C.
- `--write-mode through` (default): `close()` waits for the file's chunk
  uploads. `back`: `close()` returns once the uploads are queued durably
  on local disk. `fsync`, `O_SYNC`, `O_DSYNC` and `--fsync-mode s3`
  always act as `through`. `constellation write-mode TARGET MODE`
  switches a running mount; switching to `through` drains the queue
  first.

### Flexible continuation epochs

When S3 is unreachable, nodes that can still reach each other over P2P
may form a continuation epoch: leases move among the members over P2P,
writes journal locally, and everything flushes when S3 returns. With the
default `epoch_slack = 0`, an epoch must contain every write-eligible
node, because a missing node might still reach S3 and take an expired
lease.

With `epoch_slack = f > 0` (`fs create --epoch-slack f`, or
`constellation fs set epoch-slack TARGET f`):

- An epoch needs `N − f` members of the write-eligible roster.
- A *promise* is a node's persisted word that it joins no epoch before
  `no_epoch_until` (its own clock). It is written to
  `heartbeat/<node>` only after it is persisted locally, and only on
  demand: when a would-be taker asks over P2P, when the node sees a lease
  expire unrenewed and nobody can ask it (it then re-reads the lease
  every 5 s), or when its slack changes. A steady cluster writes no
  heartbeats.
- A node joins an epoch only once its last issued promise has expired,
  and issues no promise while its epoch is open.
- An S3 takeover of an expired lease another node held needs at least
  `f` *other* roster nodes whose promise outlasts the lease's recorded
  expiry. The taker reads `heartbeat/` and asks every roster peer over
  P2P, waiting up to 1 s for replies. Otherwise it refuses and retries
  later.
- The promise TTL (`CONSTELLATION_PROMISE_TTL_S`, default a quarter of
  the lease TTL) must be at most a quarter of the lease TTL.
- A taker honours the largest slack any roster node advertises, so a
  change of `f` is safe while it propagates (about a minute).

Why it is safe: a taker's `f` promisers and an epoch's `N − f` members
share at least one node, whose promise would have to be both expired
(to join) and still binding after the lease's expiry (to count for the
taker), which clock drift within the lease margin rules out.

Operational rules:

- `fs set epoch-slack` refuses `f ≥ N` and warns when `f > N − 2`: a
  single crashed holder then blocks TTL failover until it returns or is
  retired with `constellation leave --node-id`, which also fences its
  leases.
- A member that can still reach S3 declines to join an epoch.
- Writes inside an epoch are acknowledged on the hold owner's disk
  alone (no backups).
- A TTL takeover costs one `heartbeat/` LIST even at `f = 0`.

### Epochs and fast takeovers

A fast takeover (a seal, or `ack=s3`) happens before a lease expires, so
promises cannot gate it. Epochs are kept away from such leases instead:

- an epoch carries a node's lease only if nobody outside the epoch can
  take it over: `Local`, or `Backup` with every listed backup a member.
  An `S3` lease is never carried; its writes stall through the outage.
- a backup that is a member of an open epoch runs no seal watch until
  the epoch closes.

### Known limits

- "Persisted" survives a process crash, not a power loss, until synced
  (see [What "on disk" means](#what-on-disk-means)).
- One backup survives one failure. Two failures inside one backup's
  window fall back to Layer A replay.
- With P2P off: no backups, no fast takeover, no pre-S3 streaming.
- A mount-level `--ack` affects only tenures that mount acquires.
- A node that enrolls during an open epoch is not accounted for.
- An epoch's hold owner that dies leaves the other members frozen until
  it returns or is retired.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `--ack local\|s3` | the filesystem's `ack_policy`, else `local` | acknowledgement policy for tenures this mount acquires |
| `fs create --ack-policy local\|s3` | `local` | per-filesystem default |
| `--fsync-mode local\|s3` | `local` | what `fsync()` waits for |
| `--write-mode through\|back` | `through` | what `close()` waits for |
| `CONSTELLATION_ACK` | unset | default for `--ack` |
| `CONSTELLATION_BACKUP_RTT_BUDGET_MS` | `5` | backup candidates' RTT limit; `0` disables backups |
| `CONSTELLATION_BACKUPS` | `1` | most backups; `0` disables backups |
| `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` | `1000` | a backup without progress this long is removed |
| `CONSTELLATION_BACKUP_TAKEOVER_MS` | `1500` | holder silence before a seal (or an `ack=s3` takeover) |
| `CONSTELLATION_BACKUP_HEARTBEAT_MS` | `300` | heartbeat append to an idle backup |
| `CONSTELLATION_PRE_S3_STREAMING` | on | stream backup-acknowledged transactions ahead of S3 |
| `fs create --epoch-slack f`, `fs set epoch-slack TARGET f` | `0` | continuation epoch slack |
| `CONSTELLATION_PROMISE_TTL_S` | lease TTL / 4 | promise lifetime |

See [Configuration](../configuration.md) for parsing rules.

## Status

`constellation status` (JSON) reports:

- `ack`: `ack_s3`, `policy`, `backups`, `candidate`, `config_version`,
  `durable`, `parked_acks`, `gated`; as a backup: `backing_holder`,
  `backing_epoch`, `backing_acked`, `sealed_epoch`; traffic:
  `backups_added`, `backups_removed`, `reconfig_cas`, `backup_appends`,
  `backup_acks`, `backup_ack_timeouts`, `backup_persisted`; waits:
  `acks_waited`, `ack_wait_ms_total`, `acks_aborted`; streaming:
  `streamed_ahead`, `streamed_installed`, `streamed_dropped`; failover:
  `seals`, `backup_takeovers`, `backup_tail_applied`,
  `s3_fast_takeovers` (triggers, not wins), `ack_floor_waits`,
  `stale_liveness_refusals`, `epoch_carry_refused`,
  `refusals_journaled`, `reads_durability_blocked`;
- `epoch`: `epoch_slack`, `carrier`, `promise_until_ms`,
  `promise_puts`, `promise_requests_answered`,
  `promise_requests_refused`, `promise_checks`,
  `takeovers_refused_promises`, `promise_flush_exempt`, `stale_claims`.

## Troubleshooting

### Writes got slower after a second node joined

A peer within `CONSTELLATION_BACKUP_RTT_BUDGET_MS` became a backup:
`ack.policy` is `Backup` and every acknowledgement now waits for it
(`ack_wait_ms_total`). That round trip is the price of a 1.5 s failover
with nothing lost. `CONSTELLATION_BACKUPS=0` turns it off.

### No backup is chosen on a LAN

Check `ack.candidate` and the peer's RTT in `status.p2p`. The peer must
be write-eligible, connected for at least 2 s, and within the budget.
`--ack s3` also disables backups.

### A takeover is refused with `epoch_slack` set

`epoch.takeovers_refused_promises` rises: fewer than `f` other nodes
promised past the lease's expiry. Either enough nodes are unreachable
that an epoch could be holding the lease, or `f` is too large for the
cluster. Bring nodes back, retire the dead holder with
`constellation leave --node-id`, or lower `f`.

### Writes stall during a bucket outage under `ack=s3`

Expected: an `S3` lease is never carried into a continuation epoch
(`ack.epoch_carry_refused`), because any peer may take it over fast.

## FAQ

- **Does a single node pay anything?** No. There are no peers, so no
  backup, no promise and no message.
- **Can I force a backup across a WAN?** Raise
  `CONSTELLATION_BACKUP_RTT_BUDGET_MS`. Every acknowledgement then pays
  that round trip.
- **What does `ack=s3` cost?** One S3 conditional PUT per group commit on
  the acknowledgement path: roughly the bucket's latency from the
  holder.

## References

- Plan 30 §3, §M9, §M10 ([plan](../../plans/v1/wip/30-write-path-resilience-and-scale-out.md))
- [ADR-21](../../explanation/DECISIONS.md#adr-21-layered-durability-and-seal-based-failover),
  [ADR-22](../../explanation/DECISIONS.md#adr-22-flexible-quorum-continuation-epochs-with-promises)
- [`crates/authority/src/core/backup.rs`](../../../crates/authority/src/core/backup.rs),
  [`crates/authority/src/core/promise.rs`](../../../crates/authority/src/core/promise.rs),
  [`crates/store-s3/src/lease.rs`](../../../crates/store-s3/src/lease.rs),
  [`crates/store-s3/src/heartbeat.rs`](../../../crates/store-s3/src/heartbeat.rs),
  [`crates/meta/src/store/backup.rs`](../../../crates/meta/src/store/backup.rs)
- Models: `crates/model/src/backup.rs`, `crates/model/src/flex.rs`;
  harness: `backup-failover`, `backup-departs`, `no-peer-in-budget`,
  `ack-s3-failover`, `single-node-unchanged`, `epoch-missing-node`,
  `epoch-slack-zero-unchanged`
- [Forwarded mutations](forwarded-mutations.md), [Cluster locks](cluster-locks.md)
