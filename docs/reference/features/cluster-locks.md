# Cluster locks

`flock` and `fcntl` locks that exclude each other across nodes (plan 30
M14). A lock taken on one node blocks a conflicting lock on every other
node, and what the previous holder wrote under its lock is what the
next holder reads under its own. This is what SQLite, git's
`index.lock` and other lock-based applications need to share files
between nodes safely. It is on by default whenever P2P is on.

## Table of Contents

- [Terminology](#terminology)
- [Modes](#modes)
- [Details](#details)
  - [Grants and local locks](#grants-and-local-locks)
  - [Leases, renewal and fencing](#leases-renewal-and-fencing)
  - [Conflicts, recalls and blocking waits](#conflicts-recalls-and-blocking-waits)
  - [Coherence from one holder to the next](#coherence-from-one-holder-to-the-next)
  - [Failover](#failover)
  - [Delegations](#delegations)
  - [Without P2P](#without-p2p)
  - [Cost](#cost)
  - [Limits](#limits)
- [Configuration](#configuration)
- [Status](#status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Owning sequencer**: the node that sequences a file's mutations. It
  is the lease holder, or the delegate of the file's subtree or name-hash
  range (see [Delegations](delegations.md)). It also holds the file's
  lock table.
- **Grant**: a node's shared or exclusive right to lock one whole file,
  given by the owning sequencer and leased for a short time.
- **Local lock**: a process's `fcntl` byte-range lock or `flock` lock,
  kept on its own node under the grant that node holds.
- **Recall**: the owning sequencer asking a node to give a grant back
  because another node wants a conflicting one.
- **Fenced**: a node holds local locks on a file, but its grant has
  lapsed. All I/O on that file from that node fails with `EIO`.

## Modes

| `--locks` | Behaviour |
|---|---|
| `cluster` (default with P2P) | Locks exclude each other across nodes. The FUSE mount negotiates `FUSE_POSIX_LOCKS` and `FUSE_FLOCK_LOCKS`, so the kernel sends every lock request to the daemon. |
| `local` | Pre-plan-30 behaviour. The kernel keeps every lock node-local and never asks the daemon, so two nodes can both hold an exclusive lock on the same file. |

`CONSTELLATION_LOCKS` supplies the default when the flag is absent. The
mode applies to every view the daemon serves; there is no per-subtree
setting. With P2P off the mode is always `local`, and an explicit
`--locks cluster` fails the mount (`cluster locks need P2P; use --locks
local`). Snapshot views never take cluster locks. If the kernel does not
offer the lock capabilities, the daemon logs a warning and locks stay
node-local.

## Details

### Grants and local locks

Locks work on two levels:

- The **grant** is the cross-node unit. The owning sequencer gives one
  node a shared or exclusive grant on a whole inode. Its table is keyed
  by `(node, inode)` and never sees byte ranges or lock owners.
- **Local locks** are the kernel's lock requests from this node's
  processes, keyed by the kernel's lock owner, with POSIX byte ranges.
  They are resolved on the node, under the grant it holds.

A node keeps a grant after its last application unlocks. An
uncontended re-lock by the same node therefore costs no message: SQLite
takes about five lock operations per transaction, and only the first
one on a file goes to the sequencer. A cached grant with no local lock
under it is released after `CONSTELLATION_LOCK_CACHE_IDLE_MS` (30 s),
and its renewals stop. Upgrading a shared grant to an exclusive one is
a new grant.

### Leases, renewal and fencing

A grant lasts `CONSTELLATION_LOCK_TTL_MS` (5 s). It never outlives the
authority that backs it: the owning sequencer caps it at its own lease
(or delegation) expiry minus the lease's margin. The timing rule is the
same as for [read delegations](cto-modes.md#read-delegations):

- the node honours a grant until `sent + ttl − margin`, measured from
  when it sent the request or renewal, on its own clock;
- the sequencer treats it as live until `granted + ttl + margin`, on its
  own clock;
- `margin` is the lease's expiry margin (`min(1 s, lease TTL / 4)`), so
  the rule holds while clocks stay within half the margin of each other.

A grant is renewed half-way through the window the node honours it for
(`(ttl − margin) / 2` after the send: 2 s at the defaults), one
`LockRenew` message per owning sequencer carrying every grant that is
due; the renewal tick is armed for the earliest renewal point, so a
short grant is renewed inside its window too. A grant is renewed only
while it is still honoured; a lapsed grant is never renewed.

A delegate's grants are capped by what is left of its own delegation,
so it keeps that authority topped up: while it has grants out it renews
the delegation at a quarter of its TTL, a lock renewal that finds less
than `2 × margin` of it left renews it at once, and it grants nothing
new on less than `2 × margin` (the request waits for the delegation's
renewal). Before, it could hand out a grant of barely more than the
margin, which its holder then honoured for a fraction of a second: the
grant lapsed while the application still held its lock, and the owner
granted the lock to another node (two `git commit`s under one `flock`
at once, EC2 campaign 5's `index.lock: File exists` stall; harness
`git-under-flock-rounds`). Note that a lapsed grant fences I/O only on
the locked file: an application that guards *other* files with the
lock, as git does, runs on unprotected, so the grant must not lapse.

A node whose grant lapsed (for example, because it was partitioned from
the sequencer for longer than the TTL) is **fenced** on that file:
`read`, `write`, `flush`, `fsync`, `fallocate` and truncating `setattr`
fail with `EIO`, for every process on the node. This is NFSv4's rule:
the application learns that its lock is gone instead of writing on
without it. The fence lifts when the file's local locks are gone (unlock
or close) or a new grant arrives. Namespace operations (create, rename,
unlink) are not fenced. With no local lock anywhere on the node, the
fence check is a single atomic load.

Data written under a grant that ended without its release's flush is
**never published**. That covers a grant that lapsed with local locks
under it, a lapsed or lost grant the node was caching with no lock under
it (the application unlocked but has not closed the file yet), and
locks removed after the lapse (an unlock or a close under the fence). The
file's unpublished writes on that node are discarded, and the kernel's
cached pages of the file are dropped with them:

- `close` (the kernel's `flush`) checks the fence *before* it drops the
  closing process's locks, so a close under the fence fails with `EIO`;
- `release`, `fsync`, and the flush that precedes a recalled grant's
  release check it too;
- a close or `fsync` of a file whose unlocked writes were discarded fails
  with `EIO`, including when the application unlocked first;
- the next lock or write on the file first discards what is left. The
  next `close` or `fsync` then reports `EIO` once.

Publishing that data later would overwrite what the next holder wrote
under its own grant.

### Conflicts, recalls and blocking waits

When a node asks for a grant that conflicts with grants held elsewhere:

1. The owning sequencer recalls the conflicting grants (`LockRecall`).
   A node that does not answer is outwaited: its grant is gone at
   `granted + ttl + margin`.
2. A recalled node releases the grant (`LockReleased`) once no local
   lock is left under it **and** the file's dirty data has been flushed
   to the log. A grant with local locks under it is released when the
   last one is unlocked.
3. A blocking request (`F_SETLKW`, `flock` without `LOCK_NB`) is
   parked, first-in first-out per inode. The request is answered
   `Waiting` after a short hold (`recall_hold_ms`, 250 ms). The node
   re-sends it every `(ttl − margin) / 2`, and the grant is pushed to the
   node (`LockGranted`) when it becomes free. A parked request is granted
   live from its latest arrival, not from when it is served. A waiter
   silent for longer than `ttl − margin` is passed over: a grant would
   already have lapsed when it arrived. After `4 × ttl` of silence the
   waiter is dropped. So a node that dies while queued costs the waiters
   behind it at most one `ttl + margin` from its last message, and
   nothing if the lock frees later than that.
   The queue is served strictly in arrival order: a waiter that cannot
   be granted yet (its holder's recall is out) holds everyone behind
   it, a re-send keeps its position, and the holder's own next request
   — the owner's included — queues behind everyone parked meanwhile. A
   grant that goes unused (its push found no waiting request, its reply
   lapsed on arrival, the requester's owner changed) is outwaited, and
   the node it was for keeps its old position when it asks again
   (`requeued_in_place`) rather than parking behind everyone who asked
   since. A push is accepted whichever node it comes from; the reply to
   a request a push already answered installs the id the owner
   re-affirmed the grant under; a recall naming a newer id of the same
   owner than the one held applies to the held grant (which adopts the
   id); and a release naming an id the owner has since replaced ends
   the grant there too (`released_superseded`). Each of these gaps
   otherwise cost every waiter a `ttl + margin` outwait, and let the
   released node re-lock under its cached grant ahead of them (EC2
   campaign 8: one committer waiting 16–28 s while the other took turn
   after turn).
4. A non-blocking request (`F_SETLK`, `flock -n`) that conflicts gets
   `EAGAIN` at once. The recall still goes out, so a retry succeeds once
   the other node's application unlocks (SQLite's busy loop relies on
   this).

Two local owners on the same node that conflict are resolved on the
node, without the sequencer.

### Coherence from one holder to the next

A grant carries a position, and before the local lock is taken the node
waits for its replica to reach it and drops the kernel's cached pages
and attributes for the file (the invalidation is waited for at most
1 s). The position covers *everything* the previous holders did under
the lock, not only the locked file:

- the sequencer's position for the file;
- joined with what each releasing node had seen or been acknowledged
  when it released (`LockReleased` carries its session frontier: the
  positions of every reply its clients got, from the root and from
  delegates, and — when the releaser is the root holder — its whole
  unshipped journal). The owner keeps the join per file and every later
  grant of the file carries it. A grant that was outwaited instead of
  released carries the owner's own position.

`status.locks` says whether that ever failed: `grants_waited` and
`grant_wait_ms_total` count the grants whose floor the replica had not
reached on arrival (the first read under the lock waited), and
`grants_degraded` the ones that gave up after the session budget — the
reads under such a grant answer from what the replica has, and the
daemon logs it at WARN with the floor and its applied position. Under
a fault-free run `grants_degraded` stays 0 (EC2 campaign 8: 0 on AWS,
1 in 30 minutes on OVH).

The position also becomes the new holder's session watermark, so a read
of *any* file on that node waits for it (bounded by
`CONSTELLATION_SESSION_WAIT_MS`; a part of it the node can never reach —
a delegation generation that ended before the node's current incarnation
— is voided from the persisted delegation table, or the watermark is
dropped after `CONSTELLATION_SESSION_WATERMARK_TTL_MS`; see
[Close-to-open modes](cto-modes.md)), and the kernel's caches of every file
another node changed are dropped when the replica changes, including
on the holder that executed the change for it. Together with the
flush-before-release in the previous section, this gives lock-protected
read-modify-write across nodes for a set of files guarded by one lock:
the previous holder's writes are in the log before its grant moves, and
the next holder reads them. Git under an `flock` turn file is the
standard case: without it the next committer could read the
`refs/heads/master` the previous one had replaced and commit on top of
the old commit, losing the other's (EC2 campaign 4 B-1; the
`git-under-flock` harness scenario).

The join lives in the owner's memory, so it moves with the lock table
when the owner changes:
- **A delegation granted.** The root sends the subtree's floor with
  every renewal it grants: the join of its per-file floors under the
  subtree and of any floor on a directory above or inside it. The
  delegate puts that floor on the whole subtree.
- **A delegation recalled.** The delegate's answer carries the
  subtree's floor back. A delegate that is outwaited instead leaves
  the root's own position, taken once the root has appended the
  delegate's stream, as the subtree's floor.
- **A takeover.** The holder mirrors the join of every floor it knows
  to its backups, and a fast successor puts it on the whole namespace.
  Any new tenure (a takeover, a restart, the lease back after losing
  it) makes no new grant until every delegation it inherited has
  renewed with it. Each renewal carries the stream index its delegate
  has executed. The tenure then floors the whole namespace with its own
  position joined with those indices. A release that the previous
  tenure recorded may name a delegate's stream beyond what that
  delegate has re-streamed to the new root, and the renewal is how the
  new root learns how far the stream goes.

A floor on a directory or on the whole namespace is coarser than a
per-file one. It can make a grant wait for a position that the file
does not need, but only once: floors are watermarks, and a reached one
costs nothing. A node's session watermark holds at most 8 delegation
streams, and streams the node has already applied are dropped first.
The wait on the grant itself always covers the grant's whole floor. A
later read of another file could lose a stream only when more than 8
unapplied streams are owed at once; the newest are kept then. A grant
that was outwaited instead of released still carries only the owner's
own position.

Two node-side rules keep a holder's I/O inside its grant. An exclusive
local lock is fenced unless the honoured grant is exclusive too. This
matters when the exclusive grant lapsed and another local process's
request then brought a shared one. A grant whose id is older than one
the same owner already gave this node for the file is refused: the
owner has replaced it.

### Failover

- **TTL takeover** (the holder crashed and had no backup): the new
  holder starts with an empty lock table. Every grant of the old tenure
  was capped by the old lease, so it has already lapsed on its node,
  which is fenced until its application unlocks and locks again. There
  is nothing to reclaim.
- **Fast takeover** (a sealed backup, or `ack=s3`; see [Durability and
  failover](durability-and-failover.md)): the new holder waits out a
  grace period before it grants anything new, and accepts renewals of
  grants it does not know as *reclaims*. The grace is the same floor
  that protects read delegations: `min(old expiry, marker + takeover
  window + max(read delegation TTL, lock TTL) + 2 × margin)`. The holder
  also mirrors its lock table to its backups asynchronously
  (`LockMirror`), and a fast successor installs the last mirror. The
  mirror is only an availability aid; the grace is what keeps locks
  exclusive.
- **Restart inside the lease**: the persisted grant horizon also covers
  lock grants. A holder that restarts grants nothing new until that
  horizon has passed, and accepts reclaims meanwhile.

While any grant is live, the holder does not release the root lease
when idle and declines a cooperative handoff. A cached grant (up to
`CONSTELLATION_LOCK_CACHE_IDLE_MS` after the last unlock) therefore
keeps the lease where it is.

### Delegations

The lock table follows the subtree. The root hands a subtree's grants to
its delegate with the delegate's first renewal, and gets them back when
the delegation is recalled. The receiver restamps each grant so that it
never expires at the owner before it lapses at the node.

The handoff rides a renewal reply, so it can arrive after the
delegation's recall, or not at all. The root therefore keeps a copy of
what it handed. Every renewal it grants re-sends the copies that are
still live, and the delegate installs each grant once. It starts
serving locks only from a renewal that carried them. When a delegation
ends without handing a grant back, the root puts its copy back in its
own table. The next delegation of the subtree then takes that grant
along. A copy is skipped when the table already holds a newer grant for
the same node on that file, or a conflicting grant. A delegate that was
outwaited (it stopped answering) also leaves a grace period on its
subtree.

Some graces cover grants the root cannot hand over, because it never
knew them. The main case is after taking over a lease that its holder
released, for example on a graceful shutdown with locks held. A
delegation's first renewal therefore carries what is left of any grace
that covers its subtree. A delegate that starts serving inside a grace
makes no new grants on the subtree until the grace has passed, plus the
margin, and accepts reclaims meanwhile.

Name-hash range delegations need nothing extra: a file's lock owner is
resolved from its primary link, like every other ownership lookup.

### Without P2P

Locks never go through the [S3 inbox](forwarded-mutations.md#the-inbox-forwarding-without-p2p).
One S3 round trip per lock, plus renewals every 2.5 s as S3 writes,
would make SQLite unusable and would fence I/O whenever S3 is slow. So:

- with P2P off, the mode is `local`;
- with P2P on but the owning sequencer unreachable, a non-blocking lock
  fails with `ENOLCK` after the forward retries, and a blocking lock
  keeps retrying until the sequencer is reachable again. The node
  re-reads the lease from S3 to find the current owner.

### Cost

- A single node is its own sequencer: a lock or unlock is one call into
  the authority core, about 10–30 µs per lock/unlock pair against
  about 0.1–0.6 µs for kernel-local locks. No message is sent.
- A node that is not the sequencer pays one round trip for the first
  lock on a file, and nothing for re-locks while it keeps the grant.
- A conflict costs a recall round trip plus the recalled node's flush.
- Workloads that never lock pay one atomic load per I/O.

WAN numbers are measured separately (`lock-latency`, `bench/remote`).

### Limits

- **A blocked lock wait cannot be interrupted.** fuser 0.18 does not
  deliver `FUSE_INTERRUPT`, so Ctrl-C (or `SIGKILL`) of a process
  blocked in `F_SETLKW` or `flock` returns only once the lock is granted.
- **No deadlock detection.** There is no `EDEADLK`: two owners waiting
  on each other wait forever, as they do with `flock`.
- **`flock` and `fcntl` share one table.** fuser delivers `flock`
  through `setlk`, so a `flock` and an `fcntl` lock taken by the *same*
  process on the same file conflict with each other. Linux keeps them
  independent.
- **`getlk` reports grants, not only locks.** A node that merely caches
  an idle grant is reported as holding a whole-file lock (pid 0), and a
  non-blocking lock may fail once against such a grant before the recall
  frees it. If the owning sequencer cannot be reached, `getlk` reports
  the file as unlocked.
- **Adjacent ranges of one owner are not merged**, so `getlk` can
  report a piece of a range.
- **The fence is checked when a flush starts, not when it lands.** A
  flush that passes the fence while its grant is still honoured
  publishes even if the grant lapses while the flush is in flight. The
  two margins (`2 × margin`) cover a flush that finishes promptly. A
  flush stalled for longer can land after another node was granted the
  lock, because the sequencer does not check grants on a commit.
- OFD locks and mandatory locks get no special handling.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `--locks local\|cluster` | `cluster` with P2P, else `local` | the mode |
| `CONSTELLATION_LOCKS` | unset | default for `--locks`; the flag wins |
| `CONSTELLATION_LOCK_TTL_MS` | `5000` | a grant's lifetime, renewed at half of it while held |
| `CONSTELLATION_LOCK_CACHE_IDLE_MS` | `30000` | how long a grant with no local lock under it is kept |

The margin is the lease's; see [Configuration](../configuration.md#cluster-locks).

## Status

`constellation status` (JSON, under `locks`) shows the mode and counters
for both roles:

- node side: `grants_held`, `requests`, `local_hits`,
  `local_conflicts`, `granted`, `would_block`, `unavailable`,
  `grant_ms_total` and the `grant_ms` histogram, `renewals`, `lost`,
  `recalled`, `recalled_busy`, `released`, `fenced_io`;
- sequencer side: `grants_table`, `grants_made`, `recalls_sent`,
  `recalls_released`, `recalls_expired`, `reclaimed`, `waiters_parked`,
  `grace_refusals`, `requeued_in_place`, `released_superseded`,
  `requests_in_flight`, `waiters`, `recalls_in_flight`.

These counters are not exported to `/metrics` or shown in the web UI.

## Troubleshooting

### I/O fails with `EIO` on a locked file

The node's grant lapsed: `fenced_io` rises and `lost` counts the grants.
The usual cause is a partition from the owning sequencer longer than
`CONSTELLATION_LOCK_TTL_MS`, or a TTL takeover of the root lease. Close
the file or unlock, then lock again. The application must assume that
another node may have taken the lock in between. If `close` or `fsync`
returns `EIO`, the writes it made under the lapsed grant, and not yet
flushed, were discarded. The file holds what the next holder wrote.

### `flock -n` or `F_SETLK` fails once, then succeeds

Another node still cached a grant on the file. The first attempt sends
the recall and returns `EAGAIN`; the next attempt gets the grant. A
steady `would_block` count on a file nobody else locks means two nodes
alternate on it.

### A lock waits forever

A blocking wait retries until the owning sequencer is reachable. Check
`unavailable` and P2P connectivity to the holder or delegate. The wait
cannot be interrupted (see [Limits](#limits)).

### The mount fails with `cluster locks need P2P`

`--locks cluster` (or `CONSTELLATION_LOCKS=cluster`) was given with P2P
off. Use `--locks local`, or turn P2P on.

## FAQ

- **Does a single-node mount pay for cluster locks?** Barely: it is its
  own sequencer, so every lock operation is one local call.
- **Are locks durable across a crash?** No. POSIX locks do not survive
  their process, and a crashed node's grants lapse with their TTL.
- **Do locks slow down files nobody locks?** No: the fence check is one
  atomic load when the node holds no local lock.

## References

- Plan 30 §M14 ([plan](../../plans/v1/done/30-write-path-resilience-and-scale-out.md))
- [ADR-25](../../explanation/DECISIONS.md#adr-25-cluster-locks-are-leased-grants-from-the-owning-sequencer)
- [`crates/authority/src/core/locks.rs`](../../../crates/authority/src/core/locks.rs) (protocol),
  [`crates/meta/src/locks.rs`](../../../crates/meta/src/locks.rs) (tables),
  [`crates/cli/src/locks.rs`](../../../crates/cli/src/locks.rs) (mount option and FUSE side)
- Model: `crates/model/src/locks.rs`; harness: `flock-cross-node`,
  `sqlite-two-nodes`, `lock-holder-partitioned`, `lock-failover`,
  `lock-latency`
- [Close-to-open modes](cto-modes.md), [Durability and failover](durability-and-failover.md),
  [Delegations](delegations.md)
