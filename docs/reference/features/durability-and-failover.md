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
  - [When to use `--write-mode back`](#when-to-use---write-mode-back)
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
  `Local`, `Backup` or `S3`. It is recorded on each tenure: `S3` when
  the filesystem's policy is `s3` (`fs create --ack-policy s3`), else
  `Backup` or `Local` depending on whether a backup is in budget.
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
| A peer within the RTT budget | `Backup` (automatic) | one round trip to the backup | nothing lost | nothing lost | detection + one CAS (≈ 1.5 s in the harness; 2.3–4.7 s from `kill -9` to a new holder on EC2, with one unexplained 60 s first failover on a fresh filesystem; see [RESULTS.md](../../../bench/remote/RESULTS.md#failover-time)) |
| `fs create --ack-policy s3`, any topology | `S3` | one S3 round trip per group commit | nothing lost | nothing lost | detection + one CAS (needs P2P) |

There is no `backup` setting: `Backup` is chosen automatically when a
peer is in budget and the filesystem's policy is not `s3`. "Nothing
lost" is for the failures named in the column; power loss is covered in
[What "on disk" means](#what-on-disk-means).

## Details

### What "on disk" means

A node's metadata store commits to OS buffers (fjall
`PersistMode::Buffer`), not to stable storage. A commit survives a crash
of the process, but a power loss or kernel crash of that machine can
drop the commits the OS had not yet written back (Linux writes dirty
pages back within about 30 s by default) unless something synced them:
an `fsync()` on the mount, an orderly shutdown, or one of the syncs
below.

**Safety state is always synced.** Every write whose loss would break a
safety rule, not just lose work, is forced to stable storage before the
node acts on it:

- a promise, before its `heartbeat/` PUT, and the epoch join gate (a
  member publishes no promise);
- a continuation epoch's persisted state (the promise to a proposer,
  its membership, the hold) whenever it changes;
- a backup's seal (and a delegate backup's), before the backup refuses
  an append, reads the lease to take over, or acknowledges the seal;
- the read-grant horizon a restarted holder quarantines on.

So a power loss can make a node forget work, never a promise it made.

**The durability contract, per layer.** What an acknowledged mutation
survives before its segment is in the bucket:

| Policy | Survives | Can be lost |
|---|---|---|
| `Local` | any failure of the *requester* of a forwarded op (it is on the holder), a holder process crash, and a holder machine failure for forwarded ops (their requesters replay them, Layer A) | the holder's *own* clients' unshipped writes, if the holder machine loses power or its kernel crashes (they are lost together with the clients that wrote them); a forwarded op if its requester and the holder both lose power |
| `Backup` | any failure of the holder (process, kernel, power), any failure of a backup (the holder still has it, and removes the backup by lease CAS), a backup's process crash at any time | writes acknowledged within the OS write-back window if the holder **and every backup** lose power (or their kernels crash) together: a correlated failure, such as one power domain. A backup commits an append before acknowledging it but does not fsync it (an fsync per append would put a disk flush on every acknowledgement, the latency Layer B exists to avoid) |
| `S3` | everything short of losing the bucket | nothing |

Plan 30 §3's "nothing lost" for `Backup` is the single-failure
contract: one machine, whatever happens to it. Use `ack_policy = s3`, or
`fsync()` with `--fsync-mode s3`, for writes that must survive a
correlated power loss.

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
(300 ms). Heartbeats never wait on S3: they continue while a slow lease
renewal leaves the lease inside its expiry margin (nothing new is
admitted then), and stop only once the lease has actually expired.
The renewal itself never waits behind the log either: a sync round
ships until the journal is empty, which under sustained writes and slow
S3 can take longer than the lease lasts, so a renewal that comes due
between two segments goes out before the next one. (It used to wait
for the round's end; the lease lapsed under a live, writing holder, and
its backup, which stops hearing heartbeats at expiry, sealed it.)

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

1. **seals**: persists and fsyncs "epoch *e* sealed", and answers
   every later epoch-*e* append with `sealed`;
2. re-reads the lease, and continues only if it still names that holder
   and epoch and still lists this backup;
3. takes the lease at epoch *e*+1 by CAS, tails S3 to head, and ships an
   epoch marker at the next log slot (a create-if-absent, which fences
   any late segment of the old holder);
4. re-applies its backup tail, deduplicated by rid, as its own journal
   (the marker records the takeover with a `TailFollows` record);
5. opens its view.

A node that answers an append with `sealed` (it sealed that epoch,
possibly in an earlier life) is dropped and never invited again for the
epoch, and no acknowledgement waits for it as a backup that could be
had.

The 1.5 s window does not adapt to S3 latency, and does not need to:
nothing on the heartbeat path touches S3 (the backup channel is P2P
only, the renewal is never starved by the ship loop), and backups are
chosen within `CONSTELLATION_BACKUP_RTT_BUDGET_MS` (5 ms), so the window
is several hundred times the link's RTT. Scaling it with the P2P RTT
would change nothing inside the budget. A longer window would only delay
a genuine failover, and the seal is safe at any threshold, so there is
nothing to gain in safety either. `slow-s3-no-seal` (1.5 s per S3
request, three minutes of writes from every node) checks that no live
holder is sealed.

A backup that restarts with its role persisted does not count its own
downtime as the holder's silence: silence counts from the moment its
P2P link to the holder is up. Until then it re-reads the lease every
`CONSTELLATION_BACKUP_TAKEOVER_MS` and seals and takes over only a lease
that has expired (a holder that stopped renewing).

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

A filesystem created with `fs create --ack-policy s3` (or with
`CONSTELLATION_ACK=s3` in the environment of `fs create`) makes every
acknowledgement wait until the record's segment is CAS-created in the
log. Sync rounds group-commit whatever is waiting. No backups are used.

The lease then records `ack_policy = S3`, and any peer may take it over
when the holder falls silent for `CONSTELLATION_BACKUP_TAKEOVER_MS`,
before the TTL: every acknowledged record is in a log slot below the
taker's epoch marker, and the marker's create-if-absent fences the old
holder. Silence is read from the holder's log stream (it heartbeats the
stream), so fast takeover needs P2P. With P2P off, `ack=s3` still gives
S3 durability but fails over at the TTL.

The policy is the filesystem's, fixed at creation, and every mount runs
with it. There is no per-mount `--ack`: what an acknowledgement means is
decided by the sequencer that gives it (the tenure's `ack_policy`), so a
per-mount setting could only ever apply to the tenures its own mount
acquired, and a node asking for `s3` while forwarding to a `local`
holder would have got `local` acknowledgements. `CONSTELLATION_ACK` is
only `fs create`'s default; a mount whose environment sets it to
something else logs a warning and uses the filesystem's policy.

### No client observes a tentative effect

Under `Backup` and `S3`, the holder's reads, and its answers to other
clients' ops, wait for any unshipped rows they would observe to become
durable. So no client of any node sees an effect that a failover could
roll back: for example, a holder never refuses another node's
`create f2` with `EEXIST` because of a rename it has acknowledged but
could still lose.

### Pre-S3 streaming

Under `Backup`, once a batch is backup-acknowledged the holder streams it
to its log-stream subscribers ahead of S3 (`StreamAhead`), backups
included, at most every 5 ms. Subscribers apply it as speculation, which
retires when the segment arrives and is rolled back if a takeover
strands it. Visibility no longer waits for S3.
`CONSTELLATION_PRE_S3_STREAMING=0` turns it off.

The stream stops before a transaction whose manifest names a chunk that
is not in S3 yet (a write-back close, see below): a subscriber could
fetch its bytes from nowhere, since dirty chunks are never served. That
transaction and what follows it reach subscribers with the segment,
whose ship waits for the chunks, or with a later stream once they are
up. The node that forwarded such a manifest is the exception: it has the
bytes, so it is streamed past it. A batch that arrives before the
segment it follows (the two travel on different streams) waits for that
segment instead of being dropped. So does one that does not follow what
the subscriber holds (it arrived while a job had the replica's cursor,
or an earlier batch was lost): the holder streams each transaction only
once, so dropping it left the subscriber behind the stream, seeing every
later write through S3 until the writer paused. It waits until a segment
closes the gap and is installed then.

A continuation epoch's hold owner streams its journal the same way, as
it grows (its writes are acknowledged on its disk alone), to the members
that keep following its log stream. The chunk rule is lifted there: no
chunk can reach S3 before the close, and the members serve one another
their dirty chunks while the epoch is open. A member that (re)subscribes
gets the epoch journal from its start again. See
[Flexible continuation epochs](#flexible-continuation-epochs).

### `--fsync-mode` and `--write-mode`

These are older, per-mount knobs that combine with the policies above:

- `--fsync-mode local` (default): `fsync()` forces the node's metadata
  store to disk and nudges the shipper.
  `--fsync-mode s3`: `fsync()` also waits until the inode's chunks and
  records are in the bucket. It is the per-call form of Layer C.
- `--write-mode through` (default): `close()` waits for the file's chunk
  uploads. `back`: `close()` returns once the uploads are queued durably
  on local disk. `fsync`, `O_SYNC`, `O_DSYNC`, `--fsync-mode s3` and a
  cluster lock's release always act as `through`. `constellation
  write-mode TARGET MODE` switches a running mount; switching to
  `through` drains the queue first.

What a `close()` has done when it returns, by mode and node:

| | Sequencer (lease holder, or the file's delegate) | Any other node |
|---|---|---|
| `through` | chunks in S3; manifest journaled and acknowledged under the ack policy | chunks in S3; manifest forwarded, committed at the sequencer and acknowledged |
| `back` | chunks queued on this node's disk; manifest journaled and acknowledged | chunks queued on this node's disk; manifest forwarded, committed at the sequencer and acknowledged, naming the chunks still uploading here |
| S3 round trips in the close, small new file | `through` 1, `back` 0 | `through` 1, `back` 0 |

In every mode the log never names a chunk S3 lacks: a `back` manifest is
shipped (and streamed ahead to other nodes) only once its chunks are up.
For a non-owner's `back` close the sequencer enrolls the chunks the
forward names as pending uploads it awaits from that node, before it
executes the op; the node reports them once they are up, and the
sequencer checks S3 itself if the report never comes (2 s, doubling to
16 s).

### When to use `--write-mode back`

For bulk imports of many small files — `tar x`, `rsync`, `cp -r`,
unpacking a build tree — where waiting for S3 at every `close()` makes
the import S3-latency bound (one round trip per file, 50–400 ms on real
buckets). Switch it on for the import and back afterwards:

```bash
constellation write-mode myfs back
tar xf big.tar -C /mnt/myfs/dst
constellation write-mode myfs through   # drains the queue first
```

What `back` gives up, and what it keeps:

- **Durability.** Until the upload drains, the bytes exist only on the
  writing node's disk (its chunk cache, fsynced with the metadata). A
  crash or reboot of that node loses nothing: the queue is journaled and
  uploads on the next mount. Losing the node's disk for good before the
  drain loses those files' content: their manifests stay held on the
  sequencer (`status.writeback.remote_chunks_awaited`, the file reads
  `EIO` there after `CONSTELLATION_REMOTE_CHUNK_WAIT_S`), and `constellation
  repair drop-held` turns them into conflict copies with holes.
- **Visibility.** Other nodes see the new content once the chunks are
  up and the manifest ships (bounded close-to-open: that much later than
  under `through`). The writing node sees it at once. On the sequencer
  a read waits for the chunk rather than failing. Unrelated changes the
  same node makes afterwards can become visible first (the ship plan
  defers only what depends on the pending file, as for the sequencer's
  own write-back since plan 30 M7).
- **`--cto strict`.** An `open` elsewhere still sees every completed
  close's manifest (the sequencer has it); reading the content waits for
  the upload on the sequencer, and elsewhere reads the file as it was
  until the manifest arrives there (the ReadIndex wait is bounded by
  `CONSTELLATION_SESSION_WAIT_MS`, then degrades). Applications that
  need a completed `close()` to be readable everywhere at once should
  not use `back`, or should `fsync`.
- **`fsync`.** One on a file with unflushed writes flushes them as
  `through` would (upload, then commit). One after the `close()` under
  `--fsync-mode local` returns once this node's store is on disk — the
  chunks are, in its cache — as it does everywhere; under `--fsync-mode
  s3` it also uploads the file's pending chunks first.
- **Correctness is unchanged**: rebases, exactly-once forwarding,
  conflict detection, locks (a lock's release flushes through) and
  failover behave as under `through`.

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
- Raising `f` at runtime (`fs set epoch-slack`) on a mount whose
  promise TTL exceeds a quarter of the lease TTL (never checked at mount
  while `f` was 0) clamps the TTL to lease TTL / 4, with an error in the
  log; a mount that starts with `f > 0` and an invalid TTL refuses to
  mount.
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
- An epoch carries a lease only if the holder's claim was usable
  (outside the lease margin) when it acked. With the default half-TTL
  renewal, an outage that starts just before a renewal leaves the holder
  about `TTL/2 − margin` for a formation; short TTLs can miss it.
- An epoch whose claim resolution carried no lease has no hold owner, so
  no write can execute under it: its members refuse writes with `EROFS`
  (as a frozen epoch does) rather than letting them time out as `EIO`.
  Once S3 is back, any member closes it, frozen or not (nothing was
  written under it), and the lease is decided by CAS again.
- A member of an epoch that carries a lease closes it once S3 is back
  and the lease object is no longer the carried lease (the hold owner's
  flush re-claimed it), frozen or not: a missing member (paused, dead)
  does not keep the others in the epoch after the owner's flush. The
  owner ends its log streams at the close; a member follows it again as
  soon as the flush's first segment is announced (gossip) or tailed.
- A holder whose carried lease reaches it after the lease margin (the
  activation took a moment) still adopts the hold: members promise
  nothing while the epoch is open, so no taker can act on the expiry.
- A member that can still reach S3 declines to join an epoch. A node
  whose own S3 fails asks every live member to probe S3 (`PingS3`,
  answered by a lease GET bounded at 300 ms: `Pong.s3_ok`); if any
  reaches it, the node does not propose (its outage is its own, not the
  bucket's: `status.epoch.own_s3_outage`) and asks again 1 s later,
  until its own S3 works again. Its closes meanwhile hand their chunks
  to a peer that reaches S3 (`CONSTELLATION_CHUNK_HANDOFF_AFTER_MS`).
  The probe is fresh each time, so a cluster-wide outage is not
  mistaken for a local one.
- Writes inside an epoch are acknowledged on the hold owner's disk
  alone (no backups). A write another member forwards to the hold owner
  leaves its chunks on that member.
- The hold owner journals its epoch writes captured (before-images),
  as an ordinary holder does, under the epoch its flush will ship under.
  At the close the flush ships everything the plan can: a manifest
  waiting for a member's chunk is *deferred* (`status.held.deferred`),
  and only what depends on it waits with it; the hold owner's own later
  writes and every other node's reach S3 meanwhile. `status.held.remote`
  lists each awaited chunk with the node expected to upload it and how
  long it has waited. A deposed hold owner recovers like a deposed
  holder: its journal is rolled back and replayed by request id.
- Members keep following the hold owner's log stream through the epoch
  (S3 cannot deliver the log then, so the stream is the only way). The
  hold owner keeps serving the segments it shipped before the outage,
  and it streams its epoch journal ahead as it grows: members install
  those transactions as speculation (retired by the segments the flush
  ships at the close). A forwarded write whose reply depends on the hold
  owner's unshipped journal completes as soon as the stream delivers
  it, and not before: it never waits for the log (which used to cost the
  40 s forward deadline, then `EIO` and a retry). Members serve one
  another the chunks their epoch writes name (dirty, not in S3 until
  the close), and a reader asks the members when no digest names a
  holder.
- The hold moves to another member over P2P only if its journal is
  empty and the requester has applied the holder's whole log (inside
  an epoch it could not catch up from S3). A node that hands its hold
  away, or closes its epoch, persists that and never adopts that hold
  again, even after a restart.
- Once S3 is back, a member that does not own the hold closes its epoch
  only after reading the lease object and finding the carried lease
  gone (re-claimed by the hold owner's flush, released, or taken over):
  before that the hold owner may still hold the epoch's authority, and
  the member keeps promising nothing. Meanwhile it uploads its own
  pending chunks, which the hold owner's flush waits for
  (`CONSTELLATION_REMOTE_CHUNK_WAIT_S`) before it publishes. The hold
  owner re-claims the carried lease when it closes, even with nothing to
  flush (the obligation is persisted), so the members always see it
  move.
- An op already sent to a holder is never refused `EROFS` when an epoch
  freezes: it may have taken effect. It gets the log's answer if this
  node has applied one, else `EIO` (in doubt, retried by the same
  request id).
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

- A power loss (or kernel crash) of the holder and every backup
  together can lose `Backup`-acknowledged writes of the last few
  seconds; any single machine's failure cannot. Safety state (promises,
  seals, epoch state) is synced and survives it (see
  [What "on disk" means](#what-on-disk-means)).
- One backup survives one failure. Two failures inside one backup's
  window fall back to Layer A replay.
- With P2P off: no backups, no fast takeover, no pre-S3 streaming.
- A node that enrolls during an open epoch is not accounted for.
- An epoch's hold owner that dies leaves the other members frozen until
  it returns or is retired.
- A member that dies holding the only copy of a chunk its epoch write
  named (a forwarded close; nothing reaches S3 during an epoch): other
  members reading that file get `EIO` (never other bytes), and the hold
  owner defers that write's manifest (and what depends on it) until the
  chunk is in S3 — the member returns and uploads it, or the operator
  gives the member up with `constellation repair drop-held <ino>
  --remote` ([Write-path hygiene](write-path-hygiene.md#a-chunk-only-a-departed-node-had)).
  Everything else keeps shipping.
- A member whose own S3 path stays broken after the outage cannot read
  the lease object, so it cannot tell the epoch is over: it stays frozen
  (`EROFS`) until its S3 works or the missing member returns over P2P.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `fs create --ack-policy local\|s3` | `CONSTELLATION_ACK`, else `local` | the filesystem's acknowledgement policy, for every mount and tenure; fixed at creation |
| `--fsync-mode local\|s3` | `local` | what `fsync()` waits for |
| `--write-mode through\|back` | `through` | what `close()` waits for (see [When to use `--write-mode back`](#when-to-use---write-mode-back)) |
| `CONSTELLATION_REMOTE_CHUNK_WAIT_S` | `60` | how long the sequencer's readers and must-finish passes wait for a non-owner's `back` chunks |
| `CONSTELLATION_ACK` | unset | default for `fs create --ack-policy` (a mount only warns when it disagrees with the filesystem) |
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
  `takeovers_refused_promises`, `promise_flush_exempt`, `stale_claims`,
  `streamed_ahead` (hold owner), `streamed_installed`,
  `forwards_streamed` (member), `handoffs_behind`;
- `held`: `deferred` (transactions waiting for a chunk still uploading,
  a member's included) and `remote[]` (each awaited chunk: `ino`,
  `path`, `node`, `chunk`, `age_s`); see
  [Write-path hygiene](write-path-hygiene.md#held-records-statusheld).

## Troubleshooting

### Writes got slower after a second node joined

A peer within `CONSTELLATION_BACKUP_RTT_BUDGET_MS` became a backup:
`ack.policy` is `Backup` and every acknowledgement now waits for it
(`ack_wait_ms_total`). That round trip is the price of a 1.5 s failover
with nothing lost. `CONSTELLATION_BACKUPS=0` turns it off.

### No backup is chosen on a LAN

Check `ack.candidate` and the peer's RTT in `status.p2p`. The peer must
be write-eligible, connected for at least 2 s, and within the budget.
A filesystem created with `--ack-policy s3` uses no backups.

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

- Plan 30 §3, §M9, §M10 ([plan](../../plans/v1/done/30-write-path-resilience-and-scale-out.md))
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
