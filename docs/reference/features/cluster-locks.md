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
  - [The fencing token](#the-fencing-token)
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
  lapsed. All I/O on that file from that node fails with `EIO`, and so
  does every write and namespace operation the lock's owner issues on
  the node, on any file.
- **Lock owner**: the kernel's owner of a local lock (`flock`: the open
  file; `fcntl`: the process's file table), and the process that took
  it.
- **Fencing token**: what a lock owner's mutation carries to whichever
  sequencer executes it: the grants the owner's locks are under, and
  when the node stops honouring each. See
  [The fencing token](#the-fencing-token).

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

A grant lasts `CONSTELLATION_LOCK_TTL_MS` (20 s). It never outlives the
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
(`(ttl − margin) / 2` after the send: 9.5 s at the defaults), one
`LockRenew` message per owning sequencer carrying every grant that is
due; the renewal tick is armed for the earliest renewal point, so a
short grant is renewed inside its window too. A grant is renewed only
while it is still honoured; a lapsed grant is never renewed. A recalled
grant is renewed while something still pins it: local locks under it,
or the one local lock it was granted for and has not served yet. That
first lock is taken only after the grant's floor is reached (up to the
session budget, 2 s) and the kernel's cache of the file is dropped
(up to 1 s), which can be past the renewal point. The first use is
waited for only that long: a recalled grant whose first lock has not
come within the session budget + 1 s + 1 s of its install is no longer
renewed and is released, and so is one whose requester gave up (its
answer lost, a non-blocking request that lost a race). Otherwise the
node would renew it for ever while the owner's waiters waited.

A delegate's grants are capped by what is left of its own delegation,
so it keeps that authority topped up: while it has grants out it renews
the delegation at a quarter of its TTL, a lock renewal that finds less
than `2 × margin` of it left renews it at once, and it grants nothing
new on less than `2 × margin` (the request waits for the delegation's
renewal). A grant under a delegation is therefore never longer than
what is left of the delegation (`CONSTELLATION_DELEGATION_TTL_MS`, by
default the lock TTL, so a delegate's grants get about the same window as
the root's; at the 5 s it used to default to, a lock in a delegated
subtree was honoured for under 3 s and renewed every second or so, and
it lapsed under load: `stress-ng-fs-nodes`).

A renewal is never lost to a slow owner. Renewals (lock and delegation)
and the holder's heartbeat reach the owner's authority driver on a lane
of their own, ahead of the ordinary peer requests, and a peer request
whose sender gave up (its request timed out) stops occupying one of the
connection's stream slots, so a backlog of them never shuts a peer's
renewals out. A renewal that is not answered within its timeout is sent
again at the next tick, but its answer still counts when it comes (for
up to a minute): a granted renewal is honoured from that renewal's own
send, as an answer in time would have been. Before, every answer later
than the timeout (500 ms) was thrown away, and an owner whose answers
all came late let the grant lapse under the holder's writes although it
had renewed it each time. A late `Lost` or `NotOwner` is ignored (the
renewal sent since decides). A delegation's renewals and its stream
batches go further: they and the root's answers are one-way messages,
so no answer can be lost to a timeout at all (see
[Delegations](delegations.md#the-append-path-and-dependencies)).

A file locked through a delegate and then unlinked while locked
(`stress-ng`'s lock stressors do) is in no delegated subtree any more,
so from the unlink on its owner is the root, by location: its holder
renews there, and another node asking for it is routed there too. The
delegate's grants on such an inode go to the root with the stream batch
that carries the unlink, and the root installs them in the step that
applies it; until it has applied that row, the root itself still routes
the inode to the delegate. Grants the root had moved to the delegation itself (waiting
for the delegate's first renewal, or handed and not handed back) come
back to its table at the same point, and a recall answer never puts the
delegate's older copy of such a grant over the root's record. Before,
the root did not have the grant at all: it answered the holder's
renewal `Lost` (its writes discarded with `EIO`) and, in the window
before that, could grant the inode to another node while the holder
still honoured its grant. A grant the root never got is never adopted
at a renewal: another node may have held the lock in between.

The batch may never reach a live root: the root dies first, or the
delegate does and its rows reach the root from its backup, or from the
log, without the grants. Two rules cover that. A delegate that is the
root when its own generation ends (it took the root over, and ends the
generation it inherited) keeps the grants on inodes that are the
root's now in its table instead of dropping them. And a generation
that ends without handing its grants back (outwaited, sealed, drained
from its backup) leaves a grace on its subtree, as before, that now also
covers every unlinked inode: such an inode is under no directory, and
which subtree it left is not recorded. During that grace the root makes
no new grant on an unlinked inode, and a holder whose grant it never
got reclaims it with its next renewal, as under any grace.

A recall answer that comes after its generation ended at the root brings
no grants back. A delegate paused past its delegation's window (or cut
off for it) answers the recall it finds queued when it resumes, with
the grants still in its table; by then the root has outwaited and ended
the generation, and every one of those grants has lapsed at its holder
(the delegation's window caps them). Installed with a fresh window, as
a timely answer's grants are, such a copy would make the root treat a
dead holder as live: it kept the next generation's grant out when the
file was unlinked, then granted the copy's old holder an upgrade beside
the delegate's live exclusive grant (`locks-unlinked-delegated-partition`
seed 7455). Its floor and barrier still count while the root knows the
generation.

A grant lapses when its holding node, or that node's path to the
sequencer, stalls for longer than the TTL: a partition, a stopped or
swapped-out daemon, an overloaded host. The sequencer then outwaits the
grant and may give the lock to another node while the application on
the first node still holds its `flock`. That node is **fenced**:

- **On the locked file**, `read`, `write`, `flush`, `fsync`, `fallocate`
  and truncating `setattr` fail with `EIO`, for every process on the
  node. This fence lifts when the file's local locks are gone (unlock or
  close) or a new grant arrives.
- **The lock's owner is fenced on every file of the mount.** An
  application that guards *other* files with the lock — git creates,
  links and unlinks loose objects and renames refs under one `flock` of
  a turn file — gets `EIO` from every operation it issues that reads or
  changes data or the namespace: `read`, `write`, `fsync`, `fallocate`,
  `setattr`, an open for writing, `create`, `mknod`, `mkdir`,
  `symlink`, `link`, `unlink`, `rmdir`, `rename`, `setxattr` and
  `removexattr`. The owner is recognised by its **process**: a lock
  records the process (thread group) of the thread that took it, named
  by its pid and start time, and a request is fenced when it comes from
  any thread of that process or of a process it started — whichever
  thread took the lock (`flock turn.lock git commit …` fences the git
  under the `flock` command; a program that locks from a worker thread
  and runs git as a child is fenced as a whole). The start time keeps a
  recycled pid from being taken for the fenced process. A request that
  carries the kernel's lock owner and names the fenced owner is fenced
  too, but that only helps `fcntl` locks with direct I/O: the kernel
  sends a lock owner only with direct-I/O reads and writes, and for
  `flock` the lock's owner is the open file while a write's is the
  process's file table, so it never matches a `flock`. Lookups, `getattr`, `readdir`, `close`
  and the lock calls are not fenced, so the application can still see
  the tree and unlock. Other processes on the node are not fenced.
- The owner fence lifts when that owner's local locks are gone (unlock
  or close), and only then: a new grant on the file does not lift it,
  because the lock was not held throughout.

This is NFSv4's rule for an expired lease (and what CephFS, GPFS and
Lustre do to an evicted client): the application learns from an error
that its lock is gone, instead of writing on without it while another
node holds it. It must unlock (or close), lock again, and redo its work
from what the next holder left. With no local lock anywhere on the
node, each fence check is a single atomic load.

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

### The fencing token

The fences above are checked on the node, when an operation starts. An
operation admitted while the grant was still honoured can still reach
its sequencer after the grant lapsed: a forward stalled in flight (a
stopped daemon, a slow link), a retry of it, or its replay by rid after
a holder change. By then another node may hold the lock and have
written under it. So the check also happens where the write is recorded
(Chubby's sequencers, Kleppmann's fencing tokens).

- **Tag.** A mutation (any namespace operation, or a manifest commit)
  issued by a lock owner — matched as the owner fence matches it: the
  process of the thread that took a lock, its threads and the processes
  it started, or the kernel's lock owner — carries a **token**: per
  minting sequencer, the grant whose window ends first among those the
  owner's locks are under, as `(grant id, until)`. `until` is when this
  node stops honouring the grant (`sent + ttl − margin` of its latest
  request or renewal), on its clock. Operations of processes that hold
  no lock carry nothing and are not checked. Which grants an operation
  is under is worked out at its first mutation (a close works it out
  before it drops the closer's locks); each mutation's `until` is taken
  when that mutation is sent, from the grant's current window, so a
  renewal since the operation began counts (a slow close or a retry is
  not refused for the window it set out with). A recalled grant's own
  flush carries the grant it is about to release.
- **Check.** Whichever sequencer executes the operation checks each
  token. This includes the holder or delegate executing a forward, the
  requester's own lease path, the takeover gate's local replay, the
  inbox drain, and the FUSE fast paths on the sequencer itself.
  - A sequencer whose table holds the grant live (the minter, or the
    delegate a grant moved to) judges it exactly: it accepts the token
    whatever its `until` says, since nothing conflicting can be granted
    while the grant is there. A holder that keeps renewing is never
    refused for a slow operation.
  - Except a grant installed from a copy rather than made or renewed
    here: a backup's lock mirror installed at a takeover, or a root's
    copy of what it handed a delegate, reinstated when the delegation
    ended. The mirror is asynchronous, so such a copy may name a grant
    its last sequencer had already ended (and granted to someone else).
    Until its holder renews it here, the window judges every token
    naming it; from that renewal on, only tokens whose `until` runs past
    the renewal pass whatever the window, so an operation sent before
    the copy was confirmed is still judged by its window.
  - A sequencer that ended the grant (released it, or outwaited it: its
    record expired) refuses the token at once, inside its window. It
    remembers the ended id for `ttl + 2 × margin` past the end of its
    record (at most 4096 ids).
  - Any other sequencer refuses the token once its own clock reaches
    `until`. The minter records the grant live until `granted + ttl +
    margin` and grants nothing conflicting before that, which is at
    least `2 × margin` after `until`. So while clocks stay within that
    of each other (the lease machinery assumes half a margin), no
    conflicting grant exists while a token is still accepted anywhere.
    That holds whether or not the executor minted the grant: git's turn
    file is the root's while its objects may live in a delegated
    subtree.
- **Refusal.** Nothing is executed or journaled (only an inbox refusal
  is journaled, as every inbox outcome is), and a retry of the rid is
  refused the same way.
  - If the issuing node still holds and honours every grant the token
    named, under a window renewed since the token was taken, the
    executor judged a stale window (a delegate's clock check, a forward
    that waited): the node sends the operation again under a fresh rid
    and a fresh token, at most 3 times. An ended grant is renewed no
    more, so this stops.
  - Otherwise the operation fails with `EIO`. A refused manifest commit
    (a close, `fsync`, a recalled grant's flush) is never published into
    the file, which the next holder may have written since: its content
    becomes a **conflict copy** under the root of the mount that wrote
    it (the filesystem root, or a subtree mount's root such as a CSI
    volume), `.constellation-conflict/<path>@<node>-<time>-<seq>`, where
    `<path>` is the file's path below that root with `/` written as
    `%2F` (`%` as `%25`; only the last 200 bytes of a longer one) and
    `<seq>` the refused operation's rid sequence (two copies of one file
    within a second do not collide). Not beside the file: a lock guards
    directories that mean something to their application (beside
    `.git/refs/heads/master.lock` a copy is a ref with a bad name, and
    `git fsck` fails). Every open description of the file reports `EIO`
    once. A refused replay also leaves a conflict copy, never its
    effect: for an operation issued under a lock (one that carried a
    fencing token), under the root of the deepest mounted view above its
    file, named the same way (beside the file only when no mounted view
    contains it); for any other operation beside its file, as before.
  - A conflict copy keeps its source's owner and only the owner's
    permission bits (`mode & 0700`): away from the file, its ancestors no
    longer keep other users out. The `.constellation-conflict` directory
    is created `0700`, owned by the owner of the directory it is created
    in (the volume's owner on a subtree mount; `root` at the filesystem
    root, where only `root` can reach the copies).
  - When this happens: an operation sent at time `t` is refused only if
    the grant ended at its minter, or if it reaches another sequencer
    after its token's `until` — the grant's window at `t`, `ttl −
    margin` after the latest renewal before `t` (9.5–19 s at the default
    20 s TTL; the delegate's clock). A refusal by window is re-sent
    while the grant is still honoured, so only an operation slower than
    the grant's remaining life, or one whose grant truly ended, fails.
- **Release ordering.** A node does not release a grant (on a recall, or
  when its idle cache expires) while an operation tagged with it is in
  flight, or while one of its own stranded operations tagged with it
  waits in the replay queue. If such an operation ended in doubt (no
  answer: it may still execute somewhere), or is queued, the node waits
  until the token's `until` has passed, after which every executor that
  does not hold the grant refuses it (the minter refuses it once the
  release ends the grant). So a released grant's stale operation never
  lands after the next holder's writes. The last operation in flight
  wakes the waiting release; a 250 ms poll is the fallback. If the wait
  outlasts the grant's window, the grant lapses there and its owner
  outwaits it.
  The same holds for the next turn on the same node under the same
  cached grant: a lock taken when no local lock is left on the file
  waits (or `EAGAIN` without blocking; `locks.predecessor_waits`) while
  an operation an earlier turn tagged with the grant is in flight or in
  doubt inside its window. The grant is live at its minter, so such an
  operation would otherwise pass there after the new turn's writes.
- **Replays.** The token is persisted with the operation wherever a
  replay by rid can come from: the requester's shadow, the executor's
  journal row (a deposed holder replays its journal) and the replay
  queue. A replay therefore carries its token after a daemon restart
  too. It is judged like any operation: at the minter, a grant still
  live passes; elsewhere, a replay that comes after its token's
  `until` is refused, and the acknowledged operation becomes a conflict
  copy. A failover longer than the window (`ttl − margin`) therefore
  turns acknowledged operations under a lock whose files a delegate
  sequences into conflict copies.
- **Restart.** A sequencer persists, before it answers, the latest
  `until` it promised in a grant *or a renewal* to another node. After
  a restart inside its lease it grants nothing new before that has
  passed. (It used to persist grants only, and so could regrant while a
  holder still honoured a renewed window.) A node's grants to itself
  die with it and leave no such wait. A renewal persists it only when it
  extends a grant in the table (one for an unknown id does not lengthen
  the next restart's wait), rounded up by a quarter TTL, so peers
  holding grants cost a synced write about every quarter TTL rather
  than every second.

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
   id); and a release naming an id the owner has since replaced
   recalls the newer one (`released_superseded`) — the node may hold it,
   if its next request overtook its release — which a node that never
   installed it (it released the older id and has nothing on the inode
   since, nor an op waiting there) answers with its release at once,
   refusing that id from then on (a push of it may still be in flight,
   and would install for the node's next op on the inode).
   Each of these gaps
   otherwise cost every waiter a `ttl + margin` outwait, and let the
   released node re-lock under its cached grant ahead of them (EC2
   campaign 8: one committer waiting 16–28 s while the other took turn
   after turn).
   A node the sequencer cannot reach loses that position. The P2P link
   can fail in one direction only: the node's own requests, and the
   answers on its connection, still arrive, but a push or a recall the
   sequencer sends is lost. A recall that fails with no connection left
   to the node, or two grants in a row pushed to it and never renewed nor
   released, mark the node unreachable (`peers_unreachable`). Its waiter
   is then granted only over a request of its own: it is told to ask
   again at once, so one is nearly always held at the sequencer, and it
   is passed over while none is, once it has been silent for longer
   than two of its request cycles (`unreachable_passed_over`). The mark
   goes when the node acknowledges
   a recall. Before, such a node was pushed the lock, recalled, outwaited
   and pushed it again, keeping its place each time, and the waiters
   behind it never got a turn (`git-under-flock-causal`: 35 minutes).
   Every request carries the requester's incarnation. A request from a
   new incarnation drops the waiters its previous one left queued, with
   their kept positions and the reachability learned of it
   (`incarnation_waiters_dropped`); a request it had held at the
   sequencer is answered `Waiting`. A request still in flight from an
   older incarnation is answered `Waiting` and not served
   (`stale_incarnation_requests`). The previous incarnation's grants stay
   until they are released or outwaited.
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
  released has no such position: the next grant of the file waits for a
  cut instead (below).

**After an outwait.** A holder that stops answering (a partition, a
stopped daemon) never says what it was acknowledged under its grant,
and that may be in a stream the owner's position does not name: a
delegate's rows the root has not appended yet, the holder's own when it
is the delegate of the files it wrote. Granting on with the owner's own
position let the next holder read the older state (sim
`locks-unlinked-delegated-dbackup-random` seed 276; 54 in 3000 seeds of
`locks-delegated-partition`). So a grant record that ends unreleased —
outwaited after a recall, or found expired by the next request — leaves
a **barrier** on its file at the record's end, and the file's next grant
waits (parked, or `EWOULDBLOCK`) until a **cut** as of that time is its
floor:

- The fencing token bounds what is left to learn. The holder's operations
  are refused everywhere from its window's end, which is `2 × margin`
  minus the transit and the holder's clock skew before the record's end —
  positive by the margin rule (margin ≥ skew + transit) — so whatever was
  executed under the grant was executed before the record ended.
- A cut is the root's own position (its log, its unshipped journal, its
  own generations) joined with every live generation's stream index as
  its delegate's latest renewal reported it. Each `DelegRenew` carries
  the index the delegate has executed and when it sent the renewal, on
  its clock, and a cut is as of the earliest of those sends. A
  generation that has ended counts no more: what the root appended of it
  is in the root's position, and the rest was never appended, so those
  acknowledgements are tentative (replayed by rid, where the outwaited
  holder's token refuses them).
- A designation counts like any generation while it is online (renewed
  within one delegation TTL). An offline one adds the index it last
  renewed with but no longer holds a cut back: its designee writes while
  isolated and is never reclaimed, so a lock holder's writes in a
  designated subtree, made after its designee's last renewal and while
  the designee is cut off, are ordered for the next holder only as far
  as that renewal went.
- The holder whose record left the barrier does not wait on it: what it
  was acknowledged under the grant is in its own position (a live holder
  whose renewal came late asks again), and its new *exclusive* grant ends
  the barrier (nobody is granted past it before its release or its
  outwait). A shared one does not: another node's shared grant beside it
  still waits for the cut. A barrier carried from a delegate (with an unlink) does not
  say whose record it was, so it holds everyone.
- The root takes its own cut when it grants. A delegate owner takes the
  one its root sends with every granting renewal answer, joined with its
  own executed stream, and renews at once while a grant waits for a
  fresher one.
- A holder cut off from everyone is often the delegate of the files it
  wrote. Its stream cannot be heard from either, so the cut waits until
  the root has ended that generation (outwaited, sealed or drained from
  its backup), as every grant under it would.
- A barrier settles once, into an ordinary floor; later grants do not
  wait. It costs nothing while nothing is outwaited. After an outwait,
  the next grant waits for every live delegate's next renewal (at most a
  quarter of the delegation TTL with grants out, half without), on top
  of the `ttl + margin` the outwait already took.
- Barriers move with the lock table like floors: a granting renewal
  carries the latest barrier under the subtree and a recall answer
  carries the delegate's back; an outwaited delegation leaves one on its
  subtree (its holders' releases went with it); a file unlinked under a
  delegate takes the delegate's barrier to the root with the stream batch
  that carries the unlink (sim `locks-unlinked-delegated-partition` seed
  4067); an unlinked file is under every directory barrier, as under
  every subtree grace. A restart inside the lease forgets its grant
  table, so every grant of the previous incarnation ends unreleased: it
  leaves a barrier on everything as of the persisted horizon, which its
  holders may write until (a new tenure's floor, below, is a cut taken
  when the tenure begins, before those holders stop). The same holds
  for a takeover of a released lease (a barrier as of the end of its
  quarantine) and for a node that drops its tenure's grant table and may
  claim the lease again (as of the end of the grants it dropped).
- The holder itself is not asked: its frontier would be the exact
  floor, but a holder that cannot be reached cannot send it, and one
  that can be reached is not outwaited.

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
  delegate's stream, as the subtree's floor, and a barrier on the
  subtree (above).
- **A takeover.** The holder mirrors the join of every floor it knows
  to its backups, and a fast successor puts it on the whole namespace.
  Any new tenure (a takeover, a restart, the lease back after losing
  it) makes no new grant until every delegation it inherited has
  renewed with it. Each renewal carries the stream index its delegate
  has executed. The tenure then floors the whole namespace with its own
  position joined with those indices. A release that the previous
  tenure recorded may name a delegate's stream beyond what that
  delegate has re-streamed to the new root, and the renewal is how the
  new root learns how far the stream goes. A delegation the tenure made
  itself is not waited for: its stream starts in this tenure, so no
  earlier floor names it.

A floor on a directory or on the whole namespace is coarser than a
per-file one. It can make a grant wait for a position that the file
does not need, but only once: floors are watermarks, and a reached one
costs nothing. A node's session watermark holds at most 8 delegation
streams, and streams the node has already applied are dropped first.
The wait on the grant itself always covers the grant's whole floor. A
later read of another file could lose a stream only when more than 8
unapplied streams are owed at once; the newest are kept then. A cut
names every live generation, so it is cut down the same way when more
than 8 are live.

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
  grants it does not know as *reclaims*. The grace is `min(old expiry,
  marker + takeover window + max(read delegation TTL, lock TTL) + 2 ×
  margin)`. It holds back new lock grants only: the acknowledgement
  floor that protects read delegations is computed with the read
  delegation TTL alone, so a long lock TTL does not stall writes after a
  takeover. The holder also mirrors its lock table to its backups
  asynchronously (`LockMirror`), and a fast successor installs the last
  mirror. The mirror is only an availability aid; the grace is what
  keeps locks exclusive.
- **Restart inside the lease**: lock grants persist a horizon of their
  own (next to the read delegations' one). A holder that restarts grants
  no lock until that horizon has passed, and accepts reclaims meanwhile;
  acknowledgements wait only for the read delegations' horizon. Only
  grants to other nodes extend the horizon: a grant the holder made to
  itself died with its process (its local locks and held grants live in
  memory, and a handover is refused while a cluster lock is held), so a
  lone node that remounts grants at once. Renewals extend it too (rounded
  up by a quarter TTL). The write is durable (`fsync`) and runs off the
  authority core: the grant or renewal answer that needs it is held
  until it lands, later needs coalesce into the next write, and every
  other event goes on meanwhile — a slow disk delays only the answers
  waiting for it (`status` `locks.horizon_held`, `horizon_writes`). A
  write that fails refuses what it held (a grant `Busy`, a renewal
  `NotOwner`). Nothing is answered before its window is on disk, so a
  restart never waits out less than a peer was told.
- **An S3 blip** (a [continuation
  epoch](durability-and-failover.md#flexible-continuation-epochs)
  carrying the holder's lease): the grants stand. Inside the epoch the
  hold owner keeps granting and renewing. When S3 returns, the close
  lets the lease go locally until the flush re-claims it, but the grant
  table stays. If the re-claim's CAS replaces exactly the lease the
  close let go (or the object a re-claim CAS in doubt wrote over it),
  nobody held it in between and every grant goes on. If anything else
  is acquired next, the table is dropped, and nothing is granted over
  the dropped grants until they lapse. Until the re-claim lands (a few
  S3 round trips: 0.35 to 6 s were measured under load), a request for
  a new grant is answered `EAGAIN` if a kept grant conflicts with it,
  and otherwise waits for the re-claim, up to
  `CONSTELLATION_S3_LESS_OP_DEADLINE_MS` (20 s), not refused `ENOLCK`; P2P
  or not, and also when S3 is cut again before the re-claim lands (then
  the next epoch carries the lease again, see
  [continuation epochs](durability-and-failover.md#flexible-continuation-epochs)).
  Requests parked at the close are told to ask again. Renewals keep
  going to the holder. Fencing tokens go on too: a kept grant keeps its
  id, so an operation tagged with it before the close passes at the
  holder after the re-claim while the grant is live there (whatever its
  token's `until`). An operation forwarded inside the re-claim window
  is told to come back (`Held`), and is judged once the lease is held
  again. A release that waited through the close for tagged operations
  ends the grant in the kept table. A renewal of a kept grant moves the
  restart horizon as any renewal does. Where the table is dropped, a
  token naming one of its grants is judged by its `until`, and nothing
  conflicting is granted before that has passed. After the flush, the lease stays while grants are
  live, as with any idle holder. Any other `Busy` from an owner (no
  fresh S3 liveness, a lease not marked as granting yet) still fails a
  non-blocking request with `ENOLCK` after the usual few attempts.
  Before, every close dropped
  the table: each lock holder's next renewal was answered `lost` (its
  I/O fenced with `EIO`), a new lock could fail with `ENOLCK`, and in a
  cluster the owner could grant a conflicting lock while the old holder
  was still in its critical section (`stress-ng-fs-faults`; the
  `locks-blips` simulation).

While any grant is live, the holder does not release the root lease
when idle and declines a cooperative handoff. A cached grant (up to
`CONSTELLATION_LOCK_CACHE_IDLE_MS` after the last unlock) therefore
keeps the lease where it is. The handoff is checked again when its
flush ends: a grant made while it uploaded and shipped declines it
too.

A release that does drop live grants (a flush before `leave` or a
suspension, a graceful shutdown with locks held) leaves them honoured
by their holders until they lapse. Whoever holds the lease next makes
no new grant until then and accepts reclaims meanwhile: a successor
that takes over the released lease waits `ttl + margin` from its
takeover, and the releasing node, should it claim its own released
lease back (no takeover), waits until the last dropped grant would have
expired. Both waits are kept on the node, not with the tenure, so they
survive that tenure's own release: a successor whose next epoch flush
released the lease a moment later, then claimed it back, granted over
its predecessor's live exclusive grant (`locks-blips-tight` seed 2723).

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
own table when the delegation ends (an outwaited one too), only copies
still live and with the window the root gave them. The next delegation
of the subtree then takes that grant along, even one made in the same
step. A copy
is skipped when the table already holds a newer grant for the same
node on that file, or a conflicting grant. An expired record in the
table is no grant and never keeps a copy out. A delegate that was
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
One S3 round trip per lock, plus renewals every few seconds as S3 writes,
would make SQLite unusable and would fence I/O whenever S3 is slow. So:

- with P2P off, the mode is `local`;
- with P2P on but the owning sequencer unreachable, a non-blocking lock
  fails with `ENOLCK` after the forward retries, and a blocking lock
  keeps retrying until the sequencer is reachable again. The node
  re-reads the lease from S3 to find the current owner, and a
  non-blocking lock re-reads it once before it gives up on the owner it
  knew (a dead holder whose successor gossip has not announced yet).
- a successor that has taken the lease over but may not grant yet (its
  lease is not marked as granting, or its S3 liveness is stale) still
  refuses a non-blocking lock that a grant it knows of conflicts with
  (`EWOULDBLOCK`, as before the failover); only a lock it would have to
  grant gets `ENOLCK` after the retries.

### Cost

- A single node is its own sequencer: a lock or unlock is one call into
  the authority core, about 10–30 µs per lock/unlock pair against
  about 0.1–0.6 µs for kernel-local locks. No message is sent.
- A node that is not the sequencer pays one round trip for the first
  lock on a file, and nothing for re-locks while it keeps the grant.
- A conflict costs a recall round trip plus the recalled node's flush.
- Workloads that never lock pay one atomic load per I/O, and one per
  write or namespace operation for the owner fence and the fencing
  token (8 ns per operation measured).
- The fencing token: while this node holds any local lock, each
  operation that mutates takes its caller's tag once (its `/proc` start
  time and the lock table: about 7.5 µs measured on this host, the same
  for a process that turns out to hold no lock). A tagged forward is 21
  bytes longer (one grant: a full-width node id, the seq, the window);
  an untagged one 1 byte. The executor's check is one lookup per token
  (11 ns measured). No extra message is sent. While locks are
  held under honoured grants, the owner fence costs a second load. While
  an owner is fenced, every such operation on the node takes the lock
  table's mutex and reads its thread's `/proc` entry (its start time, so
  a recycled pid is judged afresh); a thread not yet seen reads `/proc`
  up its parent chain once (the verdict is cached per thread until the
  fenced set changes). Taking a lock reads the locking thread's `/proc`
  entries once (its process and their start times).

WAN numbers are measured separately (`lock-latency`, `bench/remote`).

### Limits

- **A wait for another node's lock cannot be interrupted.** A signal
  (Ctrl-C, `SIGKILL`, a handled `SIGALRM`) ends a blocked `F_SETLKW` or
  `flock` with `EINTR` while it waits on a lock held on its own node, or
  before its request reaches the owning sequencer. Once the owner has
  parked the request because another node holds a conflicting grant, the
  wait returns only once the lock is granted.
- **`EINTR` even under `SA_RESTART`.** A signal whose handler was
  installed with `SA_RESTART` restarts a native `F_SETLKW` transparently.
  Over FUSE the wait ends with a plain `EINTR` (as with libfuse), so the
  program sees it and must retry.
- **No deadlock detection.** There is no `EDEADLK`: two owners waiting
  on each other wait until a signal ends one of the waits, as they do
  with `flock`.
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
- **What the fences cannot see.** An operation admitted while the grant
  was honoured, whose effect lands after the lapse, is caught by the
  fencing token in these cases: a forward stalled in flight, a retry or
  replay of it by rid (after a restart too), an `open(O_TRUNC)` or
  `rename` forwarded just before the lapse, a manifest commit (close,
  `fsync`, a recalled grant's flush) that started under the grant and
  landed after it, and a write to another file that its owner closes
  while still holding the lapsed lock. These remain:
  - **Data published after the locks are gone.** A write the owner made
    to another file under the lock is published by whoever closes or
    `fsync`s the file later, or by a `syncfs` of the mount. If that happens after the owner unlocked,
    the commit carries no token. That is also the case after a lapse:
    the owner's unlock or close lifts its fence. On the locked file
    itself, the taint still discards such data. The token cannot carry
    these, because the data does not remember which lock it was written
    under.
  - **Operations of processes the owner fence cannot match:** see the
    next items. They carry no token either.
  - **A stranded op of another node,** replayed here as pre-S3 streamed
    speculation, carries no token (the stream carries records, not
    tokens). Its own requester's replay carries one, and the rid makes
    whichever lands first the only execution.
  - **The clock assumption.** The window check (executors that do not
    hold the grant) holds while an executor's clock is within
    `2 × margin` of the holder's. That is four times the lease's own
    assumption.
- **The owner fence knows processes, not intentions.** A process the
  lock holder started is fenced only while it is still the holder's
  descendant: one that daemonized (re-parented to `init`) is not. A
  process that merely shares the lock file without taking the lock is
  not fenced either. Reads through a passthrough handle (plan 38) never
  reach the daemon and are not fenced.
- **Shared-mmap writeback is not fenced by owner.** Dirty pages of a
  shared `mmap` are written back by the kernel's flusher threads: the
  request carries a kernel worker's pid and no lock owner, so a fenced
  owner's mmap'd writes to *other* files are published. (On the locked
  file itself the per-file fence and the discard at publication still
  apply.)
- **Without `/proc`, and where it lies.** Without `/proc` (macOS,
  other platforms), the owner fence matches a request by the pid it
  carries alone: requests with the pid that took the lock are fenced,
  and the processes it started are not. The same happens with
  `hidepid=2` (the lineage cannot be read). Under a pid namespace (an
  app pod under the CSI node service), the kernel sends the daemon pid
  0, which reads as no pid at all: no owner fence. The fencing token is
  worked out by the same match, so it does not make these cases safe;
  only the per-file fence and the taint protect them then.
- OFD locks and mandatory locks get no special handling.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `--locks local\|cluster` | `cluster` with P2P, else `local` | the mode |
| `CONSTELLATION_LOCKS` | unset | default for `--locks`; the flag wins |
| `CONSTELLATION_LOCK_TTL_MS` | `20000` | a grant's lifetime, renewed half-way through the window it is honoured for |
| `CONSTELLATION_LOCK_CACHE_IDLE_MS` | `30000` | how long a grant with no local lock under it is kept |

The margin is the lease's; see [Configuration](../configuration.md#cluster-locks).

**Choosing the TTL.** The TTL is how long a lock holder's node may stall
(a partition from the sequencer, a stopped daemon, an overloaded host)
before its lock can be lost under the application: past it the owner
outwaits the grant, and the application is fenced. It is also how long
the next waiter waits when the holding node *crashed* (`ttl + margin`
after the dead node's last renewal: about 21 s at the default), and
how long a new root lease holder grants no lock after a fast takeover
or a restart inside its lease. A healthy holder's unlock hands the
lock on at once whatever the TTL; renewals cost one message per
`(ttl − margin) / 2`. Raise the TTL where stalls are common and crashes
rare; lower it where a crashed holder must be replaced quickly and the
applications cope with `EIO`. Grants under a delegation are capped by
the delegation's TTL anyway.

## Status

`constellation status` (JSON, under `locks`) shows the mode and counters
for both roles:

- node side: `grants_held`, `requests`, `local_hits`,
  `local_conflicts`, `granted`, `would_block`, `unavailable`,
  `grant_ms_total` and the `grant_ms` histogram, `renewals`, `lost`,
  `recalled`, `recalled_busy`, `released`, `fenced_io`,
  `owners_fenced` (lock owners fenced on this node), `owner_fenced_ops`
  (their operations refused with `EIO`, on any file),
  `first_use_abandoned` (recalled grants released before their first
  lock, see [Leases, renewal and fencing](#leases-renewal-and-fencing)),
  `fenced_owners` (the owners fenced now: `owner`, `pid`, `since_ms`,
  and `grant`, the `[minter, seq]` whose end fenced it), `margin_ms`
  (the clock-skew margin the lock machinery assumes),
  `tagged_ops` (mutations that carried a fencing token),
  `release_waits` (releases held back for one) and `predecessor_waits`
  (new local turns held back for an earlier turn's tagged mutations);
- sequencer side: `grants_table`, `grants_made`, `recalls_sent`,
  `recalls_released`, `recalls_expired`, `reclaimed`, `waiters_parked`,
  `grace_refusals`, `requeued_in_place`, `released_superseded`,
  `peers_unreachable` (nodes taken for unreachable, see
  [Conflicts, recalls and blocking waits](#conflicts-recalls-and-blocking-waits)),
  `unreachable_passed_over` (their waiters passed over meanwhile),
  `incarnation_waiters_dropped` (waiters a restarted node's previous
  incarnation left queued), `stale_incarnation_requests` (requests from
  an older incarnation, answered and not served), `requests_in_flight`, `waiters`, `recalls_in_flight`,
  `token_rejections` (operations refused for a dead fencing token).

These counters are not exported to `/metrics` or shown in the web UI.

## Troubleshooting

### I/O fails with `EIO` on a locked file, or on any file while a lock is held

The node's grant lapsed: `fenced_io` rises on the locked file,
`owner_fenced_ops` on the lock owner's operations elsewhere, and `lost`
counts the grants. An operation that was already on its way when the
grant lapsed is refused by its sequencer instead: `token_rejections`
rises there, and the daemon logs "the lock grant it was issued under is
no longer live". The usual cause is a stall of the node, or a
partition from the owning sequencer, longer than
`CONSTELLATION_LOCK_TTL_MS` (on the holder itself, a stall of its
authority core: renewals of its own grants run there too, so a
`slow core step` of that length in the log fences them), or a TTL
takeover of the root lease. An S3 blip no longer does (see
[Failover](#failover)). Close
the file or unlock, then lock again. The application must assume that
another node may have taken the lock in between. If `close` or `fsync`
returns `EIO`, the writes it made under the lapsed grant, and not yet
flushed, were not published into the file, which holds what the next
holder wrote. If the sequencer refused them (the fencing token), they
are kept as a conflict copy under `/.constellation-conflict/` at the
filesystem root, named after the file's path, logged as "kept as a
conflict copy". Writes discarded on the node
itself (the per-file taint) are not kept.

### `flock -n` or `F_SETLK` fails once, then succeeds

Another node still cached a grant on the file. The first attempt sends
the recall and returns `EAGAIN`; the next attempt gets the grant. A
steady `would_block` count on a file nobody else locks means two nodes
alternate on it.

### A lock waits forever

A blocking wait retries until the owning sequencer is reachable. Check
`unavailable` and P2P connectivity to the holder or delegate. A wait
parked at the owner cannot be interrupted (see [Limits](#limits)).

### The mount fails with `cluster locks need P2P`

`--locks cluster` (or `CONSTELLATION_LOCKS=cluster`) was given with P2P
off. Use `--locks local`, or turn P2P on.

## FAQ

- **Does a single-node mount pay for cluster locks?** Barely: it is its
  own sequencer, so every lock operation is one local call.
- **Are locks durable across a crash?** No. POSIX locks do not survive
  their process, and a crashed node's grants lapse with their TTL.
- **Do locks slow down files nobody locks?** No: each fence check is
  one atomic load when the node holds no local lock.
- **Why is the TTL so long?** A lapse is not harmless: the application
  that held the lock gets `EIO` and must redo its work. A long TTL makes
  lapses rare; the price is paid only when a holder's node crashes. See
  [Configuration](#configuration).

## References

- Plan 30 §M14 ([plan](../../plans/v1/done/30-write-path-resilience-and-scale-out.md))
- [ADR-25](../../explanation/DECISIONS.md#adr-25-cluster-locks-are-leased-grants-from-the-owning-sequencer)
- [`crates/authority/src/core/locks.rs`](../../../crates/authority/src/core/locks.rs) (protocol),
  [`crates/meta/src/locks.rs`](../../../crates/meta/src/locks.rs) (tables),
  [`crates/engine/src/locks.rs`](../../../crates/engine/src/locks.rs) (mount option and FUSE side),
  [`crates/engine/src/view/lock_gate.rs`](../../../crates/engine/src/view/lock_gate.rs) (the fences)
- Model: `crates/model/src/locks.rs`; harness: `flock-cross-node`,
  `sqlite-two-nodes`, `lock-holder-partitioned`, `lock-failover`,
  `lock-latency`, `git-under-flock-b2b` and `-rounds` (the owner fence),
  `git-under-flock-faults` with `GIT_FLOCK_STRICT=1` (the fencing
  token); sim `locks_lapsed_owner_is_fenced_on_other_files`
- [Close-to-open modes](cto-modes.md), [Durability and failover](durability-and-failover.md),
  [Delegations](delegations.md)
