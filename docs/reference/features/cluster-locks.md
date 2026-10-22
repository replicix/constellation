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
  - [Close and release](#close-and-release)
  - [Leases, renewal and fencing](#leases-renewal-and-fencing)
  - [A lapsed grant is asked for back](#a-lapsed-grant-is-asked-for-back)
  - [The fencing token](#the-fencing-token)
  - [Conflicts, recalls and blocking waits](#conflicts-recalls-and-blocking-waits)
  - [Waiting for a grace](#waiting-for-a-grace)
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
  They are resolved on the node, under the grant it holds, and kept as
  Linux keeps them (`posix_lock_inode`): an owner's touching or
  overlapping ranges of one type merged into one, a range of the other
  type inside one splitting it, each owner's locks in start order, and
  the owners in the order of their first lock on the file. A `getlk`
  answered on the node names the lock Linux would name, the first in
  that order that is in the way: a whole merged range, never a piece
  of one.

A node keeps a grant after its last application unlocks. An
uncontended re-lock by the same node therefore costs no message: SQLite
takes about five lock operations per transaction, and only the first
one on a file goes to the sequencer. A cached grant with no local lock
under it is released after `CONSTELLATION_LOCK_CACHE_IDLE_MS` (30 s),
and its renewals stop. Upgrading a shared grant to an exclusive one is
a new grant.

### Close and release

With the lock capabilities the kernel keeps no lock of its own for the
file, so it sends no unlock when a descriptor is closed. The daemon
learns of a close from two requests:

- `FLUSH`, on every `close(2)`, which the close waits for. It names the
  closing process's POSIX lock owner, and the node drops that owner's
  locks on the file there: POSIX says any close does.
- `RELEASE`, once the last descriptor of an open file description is
  gone. The node drops the description's OFD and `flock` locks there:
  the owners that locked through it, which it keeps per description,
  since the kernel names the `flock` owner only, and no OFD owner unless
  the description also used `flock` (the same owner id). A
  `FLUSH` cannot drop them: a `dup`'s close, a forked child's or an
  `exec`'s `O_CLOEXEC` close sends one too, and the description and its
  locks outlive those.

The kernel sends `RELEASE` in the background, after `close(2)` has
returned (`fuse_file_release`), and another FUSE worker can serve the
process's next request first. So a description whose last close was
just answered may still hold its locks for a moment, typically 0.1–5 ms.
A non-blocking lock (`F_SETLK`, `F_OFD_SETLK`, `flock` with `LOCK_NB`)
or a lock test that conflicts only with such locks waits for their
`RELEASE`, then asks again: as on a local file system, it does not see
the locks of a description closed before it was made. The wait lasts at
most `CONSTELLATION_LOCK_CLOSE_WAIT_MS` (100 ms) from the close, and
never more than that from the request itself, however many closes of
the description's other descriptors follow (a shell that holds
`flock -n 9` and runs a command per loop closes a copy of descriptor 9
with each command's exit). Past that, or when a lock request, unlock or
test through the description shows it open, the request is refused at
once. A close that was not the last (a `dup`'s) is followed by no
`RELEASE`, so a request in its shadow is refused late, never wrongly.
A lock test that another lock is in the way of too does not wait, and
names that other lock, as Linux, which dropped the closed
description's locks at the close, does.

The kernel names an OFD or `flock` lock owner after the open file
description, and a description opened right after another's last
close can take its place in memory, and so its owner id, before that
one's `RELEASE` arrives. A lock request of that owner through the new
description waits for the old `RELEASE` first (within the same bounds),
so the closed description's locks go with it instead of passing to the
new description. A POSIX owner locking through another description
after a different process closed one it shares is followed by no
`RELEASE`: it waits out the close window at most, and keeps its locks. A late `RELEASE` names its description's `flock` owner; when a new
description at that address holds the id by then (it unlocked before
the close, so its successor did not wait), the new description's locks
are left alone.
Nothing is released before its `RELEASE`.

A mount served by a single FUSE worker (`CONSTELLATION_FUSE_THREADS=1`,
or a view mounted with `fuse_threads` 1) does not wait: its requests
are served in the order the kernel queued them (strictly so over
`/dev/fuse` and on a one-CPU host; over the ring as long as the closing
CPU's queue has a free entry), so the `RELEASE` that
`close(2)` queued is served before the process's next request, and a
wait would only hold the one worker that has to serve it.

The `RELEASE` then publishes what was written under the locks, under
the grant's [fencing token](#the-fencing-token) (so does a `FLUSH`,
which drops the closer's POSIX locks before it publishes), and a new
turn on the node waits for that publication — counted from before the
close drops the locks until the commit is answered, for a close that
drops locks of its own caller on the file and publishes it; not the
upload of the file's chunks that a `through` close waits for after its
commit, which only the closing caller waits for — and for an
operation of the earlier turn that ended in doubt. A non-blocking request with
nothing else in its way waits for them too, instead of answering
`EAGAIN` at once, within the same bound of
`CONSTELLATION_LOCK_CLOSE_WAIT_MS` from its arrival (both waits together
never exceed it; this one also on a single worker, since those
operations finish without one). A blocking request always waited for
them. A new turn does not wait for the other operations still in flight
under the grant: every operation of a process that holds a lock carries
the lock's grant, but one still waiting for its answer (another
thread's `fsync` of another file, say) is concurrent with the unlock,
as on a local file system, where it may take effect after the next
holder's writes too. A non-blocking lock nobody holds is granted at
once beside it (it used to wait for it and then answer `EAGAIN` with no
lock in the way). This narrowing is safe because the node commits one
file's publications one at a time (the file's operation lock), so the
new turn's commit of the file is not sent before an earlier one was
answered, and a stale commit replayed later is refused by its manifest
base; the wait keeps the landing order at the minter for what the
application already counts as done. An operation of the same process
that ended in doubt, on any file, still pins the grant until its
window ends (up to `ttl − margin`): in that time a non-blocking request
on a file nobody locks answers `EAGAIN`, after a sequencer or S3 blip
(through xcheckfs, which runs every application as one process, that
is any mirrored application's operation).

### Leases, renewal and fencing

A grant lasts `CONSTELLATION_LOCK_TTL_MS` (20 s). It never outlives the
authority that backs it: the owning sequencer caps it at its own lease
(or delegation) expiry minus the lease's margin. The timing rule is the
same as for [read delegations](cto-modes.md#read-delegations):

- the node honours a grant until `sent + ttl − margin`, measured from
  when it sent the request or renewal, on its own clock — also for a
  reply that arrives after a push already answered the request, or is
  handled late (a stalled step): one counted from its arrival was
  honoured past the owner's record of it (sim `locks-stall-step`: two
  exclusive holders);
- the sequencer treats it as live until `granted + ttl + margin`, on its
  own clock;
- `margin` is the lease's expiry margin (`min(1 s, lease TTL / 4)`), so
  the rule holds while clocks stay within half the margin of each other.
  That is the offset between nodes. A clock's *rate* is paid by the clock
  itself (plan 43 C7a): each node's honour and wait readings carry its
  oscillator budget (0.1 % by default), so `margin ≥ 2ρ·T + transit` holds
  with the `2ρ·T` (40 ms on a 20 s grant) already in the readings and the
  margin left as slack for transit and rounding.

What a node honours is measured on its **honour clock** (plan 43 C7-H1): a
monotone reading that never under-counts real time and follows a forward
wall-clock step but ignores a backward one. A node's held grants, its
renewal points, the fences on its locks, the lease and the delegation it
caps a grant by are all stamped and judged on it. A backward step of the
node's wall clock therefore changes nothing: a grant lapses when its window
ends, not later. A forward step ends the windows it covers at once: if it
is larger than what is left of a held grant's window, the grant lapses and
its owner is fenced (`EIO`) as for any lapse. That is the expected cost of
a large forward step. The sequencer's own record of a grant is measured on its **wait
clock** (plan 43 C7-H1): a monotone reading that never over-counts and
ignores every wall step. A forward step of the owner's wall clock therefore
does not make it drop a record early and grant to another node while the
holder still honours its window, and a backward one does not make a waiter
wait longer. The same holds for what an owner waits out otherwise: a
restart's or a takeover's quarantine, a grace on a subtree, an outwaited
delegation, a successor's takeover floor. What is persisted across a restart
(the grant horizon) is written as a boot stamp (plan 43 C7a: its wall end, the
span ahead of the write, the boot id and the raw clock's end) and read back as
time left on the new process's wait clock: on the raw clock when the node
restarts on the same boot, so no step of the wall clock, up or down, while the
node was running or down changes it, and as the whole recorded span after a
reboot. A fencing token's window stays a wall value, so the executor's check of
it keeps the offset bound.

A grant is renewed half-way through the window the node honours it for
(`(ttl − margin) / 2` after the send: 9.5 s at the defaults), one
`LockRenew` message per owning sequencer carrying every grant that is
due; the renewal tick is armed for the earliest renewal point, so a
short grant is renewed inside its window too — re-armed whenever a held
grant's renewal point moves, a late reply's included (sim
`locks-delegated-production-ttl-heavy` seed 3850: a 2.1 s grant
extended by the reply to a request its push had answered, with the
tick still at the 5 s cadence, lapsed under its I/O). A grant is renewed
while it is still honoured; a lapsed grant is never *reclaimed* (its owner
cannot tell what it granted since), but one that pins local locks or writes
is asked for back
([below](#a-lapsed-grant-is-asked-for-back)). A recalled
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

An owner extends a window only as far as it may grant one. The lease
holder of a tenure that can be taken over before its lease expires (a
listed backup, or `ack=s3`; see
[Durability and failover](durability-and-failover.md)) renews a grant
(its own grants at its renewal tick included), accepts a reclaim, and
renews a delegation only with fresh S3 liveness (an S3 request sent
within `backup_takeover_ms` succeeded without showing another holder)
and with its lease marked as granting, exactly as it grants. On stale
liveness it decides the renewal, reads the lease once, and answers when
the read shows it still holds; if the read shows a successor, it
answers `NotOwner { 0 }` (the holder renews or reclaims at the
successor) and steps down (a delegation's answer is dropped, and the
delegate asks again). An idle fast-tenure root therefore answers a
renewal one S3 round trip later; the holder counts the answer from its
own send, late or not. A root that cannot reach S3 answers no renewal
at all: if the read fails, every answer it held is refused, so a
fast-tenure root cut from S3 for longer than about `(ttl − margin) / 2`
(9.5 s at the defaults) lets every lock grant it sequences lapse, its
own included, and their holders are fenced, even while its backups
still acknowledge its writes. Before, such a root kept renewing until
its lease ran out — and so did one its backups had sealed, still
reached by lock holders and not yet aware of it, up to the lock TTL
past its successor's grace. A lone node, or any lease holder without
backups under the default `ack=local`, is not affected.

A fast takeover's successor is itself marked as granting from its first
CAS (no strict mounts needed), and so is the taker of a released lease
whose tenure left lock grants (or a grace) behind (the release marks its
lease exactly then, whatever the released lease said before; grants moved
toward a delegate count): both wait out grants of the tenure before, and a
successor of theirs, sealed in before they granted anything, must wait out
the same grants. A successor with no such mark in its lease would arm no grace, and
a lock the first holder's tenure granted without the mirror seeing it could
be granted over.

A mark of this kind (inherited, or written for a duty the tenure stood on
when it became sealable: below) does not outlive what it stands on. Once the
tenure's floor has passed and nothing is live (no grant record at all, live
or expired; no grace or quarantine, lock or read; no read grant; no
delegation; no answer held), and the tenure has itself granted and answered
nothing since its claim, its housekeeping clears the mark by CAS: a seal of
it then arms no floor, and a chain of seals stops paying one each. Only
inherited marks are cleared: a mark the tenure made for its own grants
(`ensure_granting_marked`), or `strict_mounts`', is not (a bursty workload
would pay a mark CAS on its first grant after each idle period and a clear CAS
per idle period). While the clear is out no lock is granted, no renewal or
reclaim answered and no strict read served, and the tenure marks nothing: a
clear whose outcome is not known (a lost answer, a conflict) is settled by a
lease read first, and the tenure adopts whatever mark the object holds. Once
it landed the tenure is a fresh unmarked fast tenure: any grant or renewal
marks the lease again first, as for the taker of an unmarked released lease.
A seal racing the clear only over-waits (it replaced the marked version) or
replaces the unmarked one and finds nothing live. A grant record that stays in
the table (a holder that died with its grant cached, and nothing conflicts with
it to purge it) keeps the mark: a lock the holder let go (idle release,
recall) is gone from the table, and the clear follows it.

A tenure marks its lease from the moment it can be sealed whenever it then
waits out, or answered, something a sealer must wait out too: lock grants
(live, moved toward a delegate, or a record a renewal can revive), a subtree
grace or a quarantine, a live write delegation, a takeover floor still to
be armed or running, read grants or a read quarantine, what it granted under
a continuation epoch's hold. A claim CAS writes an unmarked lease unless it inherits a
predecessor's duty, and the duties it does not cover are real: a node that
re-claims its *own* released lease waits out the grants it let go (their
records are dropped, so no backup listed later is ever sent them), a
restart's quarantine runs over the lease the node re-adopts, the reclaims a
`Local` tenure admits are never mirrored to a backup added after them, and
grants an epoch's close kept stand under the re-adopted lease's own mark. A
tenure becomes sealable in two places, and marks in both when such a duty is
live: its claim CAS under `ack=s3` (a lease that says `ack=s3` is sealable
from its first write), and the reconfiguration that lists its first backup
(`issue_reconfig`; its retry against a fresher lease re-evaluates, and the
landing marks if a duty arose with the CAS out). While a reconfiguration that makes
the tenure sealable without marking it is out, the tenure answers no
lock renewal or reclaim and issues no mark (`Core::tenure_marked`,
`ensure_granting_marked`): a reclaim under a grace that began since would
stand in a version a sealer may replace as the CAS lands (and while such a
CAS's outcome is not known, until the lease is next read or written); the
answers go
once it settles and the lease is marked. Nothing is marked for a tenure
with nothing of the kind live at its first backup listing (a start-up's or a
handoff's), so a seal of it still waits for nothing.

A delegate's grants are capped by what is left of its own delegation,
so it keeps that authority topped up: while it has grants out it renews
the delegation at a quarter of its TTL, a lock renewal that finds less
than `2 × margin` of it left renews it at once, and it grants nothing
new on less than `2 × margin` (the request waits for the delegation's
renewal). The root caps the delegation in turn by what is left of its
lease, less the margin, and renews its lease as soon as half its TTL
is gone (its sync poll is armed for that moment), so a delegated grant
is not squeezed by a lease left close to its expiry. Every layer keeps
its own margin (the lease's usable end, the delegate's window, its
grant cap, the holder's window), so a delegated lock's window ends four
margins before the lease expires, and its own grants' two: under a
lease of eight margins (a 6 s lease at the 1 s margin) that comes
before the half-TTL renewal. So the root also renews its lease at its
next sync round when it answers a delegation renewal it capped below
`4 × margin` (what a delegate needs for a `2 × margin` lock grant), or
a lock grant or renewal of its own it capped below `2 × margin`; at
most once per lease expiry, and only if the renewal buys at least a
margin. At the default
60 s lease the cap never comes that close. A grant under a
delegation is therefore never longer than what is left of the delegation (`CONSTELLATION_DELEGATION_TTL_MS`, by
default the lock TTL, so a delegate's grants get about the same window as
the root's; at the 5 s it used to default to, a lock in a delegated
subtree was honoured for under 3 s and renewed every second or so, and
it lapsed under load: `stress-ng-fs-nodes`).

A renewal is never lost to a slow owner. Renewals (lock and delegation)
and the holder's heartbeat reach the owner's authority driver on a lane
of their own, ahead of the ordinary peer requests, and a peer request
whose sender gave up (its request timed out) stops occupying one of the
connection's stream slots, so a backlog of them never shuts a peer's
renewals out. At the holder, the renewal tick and the owner's answer
come ahead of the driver's internal channel too, so a backlog of
log-stream frames there neither delays a renewal nor holds back its
answer until the grant has lapsed (see
[Durability and failover](durability-and-failover.md)). A renewal that is not answered within its timeout is sent
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

A renewal (or request) that finds no owner it can reach reads the root
lease to learn the holder. Under an S3 cut that read fails. The node
then asks the active continuation epoch's carrier, or failing that, the
holder of the last lease object it saw, if that object has not expired
on its clock. It only asks that node: the node answers from its own
table, and a `NotOwner` answer clears the guess again. Before, a holder
that knew no owner found none for the whole cut, and its grant lapsed
under its I/O although the root that would renew it was reachable over
P2P (`locks-blips-tight-delegated`). Only a reply of the root's that
executed (or refused) the op names the lease holder: a `NotHolder` or
`Busy` reply names nobody. A recalled delegate answers the ops parked on
it `NotHolder` with no generation. Before, that made it the asking
node's cached lease holder, and two former delegates sent each other
their lock renewals until the grants lapsed.

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

A recall answer that comes once its generation's window is outwaited at
the root brings no grants back: the generation has ended, is sealing its
backup, or is past its `until` (an expiry still retrying). A delegate
paused past its delegation's window (or cut off for it) answers the
recall it finds queued when it resumes, with the grants still in its
table; by then the root has outwaited the window (and, unless it is
sealing or retrying, ended the generation), and every one of those
grants has lapsed at its holder (the delegation's window caps them).
Installed with a fresh window, as a timely answer's grants are, such a
copy would make the root treat a dead holder as live: it kept the next
generation's grant out when the file was unlinked, then granted the
copy's old holder an upgrade beside the delegate's live exclusive grant
(`locks-unlinked-delegated-partition` seed 7455). Its floor and barrier
still count while the root knows the generation.

A recall answer installs each grant it hands back once, by (grant,
recall): the generation stays live until the delegate's stream has
caught up with the position the answer names, and a copy of the answer
that arrives meanwhile (a re-send) would otherwise install the grants
again — a record the root had released or superseded since back in its
table (from which a late renewal is admitted), and one it kept restamped
later than the first copy's. The copy's grants are dropped, counted
(`recall_answer_duplicates`); its floor and barrier are joins and
restate what the first had.

A grant lapses when its holding node, or that node's path to the
sequencer, stalls past what is left of the grant's window: a partition,
a stopped or swapped-out daemon, an overloaded host. Renewed half-way
through, a grant has between `(ttl − margin) / 2` and `ttl − margin` of
its window left at any moment (9.5–19 s at the defaults): a stall
under 9.5 s never lapses it, a longer one shorter than the TTL can.
The node that sequences the lock itself (a lone node, or the lease
holder's own locks) is no exception: its renewal is a step of the same
authority core, and a stalled core renews nothing. The xcheckfs run of
2026-10-13 stalled a lone node's core for 18 s (a metadata-store write
waiting on a journal write the overloaded host held up) and its grants
lapsed. Its window is not stretched to the lease's for its own grants,
because the node cannot tell from inside a stall that nobody acted on
the expiry: a backup's seal or an `ack=s3` takeover deposes a holder
well inside its lease TTL, and the successor's grace assumes every
grant lapsed at its holder within a lock window. (The close-and-release
waits above are bounded separately, by
`CONSTELLATION_LOCK_CLOSE_WAIT_MS`; they do not stretch a window
either.) The sequencer then outwaits the grant (its own too: the
record lives until `ttl + margin` from its send, while the lapsed grant
itself is dropped at the node's first renewal tick after the stall) and
may give the lock to another node while the application on the first
node still holds its `flock`. That node is **fenced** — unless the owner can prove nobody else was
granted the lock since, in which case its I/O only waited
([below](#a-lapsed-grant-is-asked-for-back)):

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
  by its pid and an identity no later process with that pid has (its
  pidfd's inode number on a 64-bit host whose kernel has pidfs, Linux 6.9
  and later; its start time elsewhere), and a request is fenced when it
  comes from
  any thread of that process or of a process it started — whichever
  thread took the lock (`flock turn.lock git commit …` fences the git
  under the `flock` command; a program that locks from a worker thread
  and runs git as a child is fenced as a whole). The identity keeps a
  recycled pid from being taken for the fenced process. It is not the
  start time where pidfs exists because a request can come from inside
  its caller's `execve(2)`, and reading `/proc/<pid>/stat` there can wait
  for that exec: `stat` (and `wchan`) take the task's `exec_update_lock`
  since Linux commit 6650527444da ("proc: protect ptrace_may_access() with
  exec_update_lock (part 1)", 7.2-rc1, `Cc: stable`), and until
  e780259b54e6 ("exec: do_close_on_exec() before taking
  exec_update_lock", 7.3-rc3, `Cc: stable`) the exec closes the caller's
  `O_CLOEXEC` descriptors while it holds that lock — a `FLUSH` for each
  one on the mount, which waits for the daemon's answer while the
  daemon's read waits for the exec: the close never ends, and only
  aborting the FUSE connection does. A `READ` of a binary stored on the
  mount can come from the same window. A kernel with the first commit and
  without the second, and without pidfs (before 6.9, or
  `CONFIG_FS_PID=n`, or `pidfd_open(2)` refused by a seccomp filter),
  still has the hang; the daemon warns at start when it names processes
  by their start time. A request that
  carries the kernel's lock owner and names the fenced owner is fenced
  too, but that only helps `fcntl` locks with direct I/O: the kernel
  sends a lock owner only with direct-I/O reads and writes, and for
  `flock` the lock's owner is the open file while a write's is the
  process's file table, so it never matches a `flock`. Lookups, `getattr`, `readdir`, `close`
  and the lock calls are not fenced, so the application can still see
  the tree and unlock. Other processes on the node are not fenced. A
  process that issues requests on behalf of others (a FUSE passthrough,
  a file server, a differential tester such as xcheckfs) is one owner:
  one lapsed grant of any lock it took fences every request it sends
  until the lapsed lock's owner holds no lock any more — every `fcntl`
  lock of the process (they share one owner), or that `flock`ed open
  file's.
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

### A lapsed grant is asked for back

Plan 43 C7-F F-c. A grant can lapse without anyone having taken its lock:
the node that sequences it is cut from S3 and cannot renew (it answers a
renewal only with fresh S3 liveness), and its successor, a backup that seals
the cut holder at the end of its lease, exists only after the grants it made
have lapsed (a 20 s lease: the locker's grant lapses about 9.5 s after the
cut, the successor takes over at about 13.6 s; no owner could honour the grant
across that gap without breaking the cap on which the successor's grace
rests). Failing every I/O of the lock's owner with `EIO` from the lapse
(25 of 50 reads of the harness's in-process locker at the 20 s lease, 175 of
200 at the 60 s one) is an error caused by a fault, not a fact about the
lock. So a grant that lapses while it **pins** something — local locks
under it, or a write made under it that may not be published yet — is
**lapsing**, not lapsed:

- Nothing is captured or tainted for it. The I/O that would be fenced —
  on the locked file, the lock owner's ops on any file, the publication
  points (`close`, `fsync`, a recalled grant's flush), a write's discard of
  an older taint, and a new lock request on the file — **waits** instead:
  the op is parked on the engine's `lock-wait` pool (it holds no frontend
  worker and no ring offload thread; a parked op asked again from the start
  once the verdict is in: every fence precedes an op's first effect), and
  answers as if it had been issued then. A signal (FUSE `INTERRUPT`) ends
  the wait with `EINTR`: the kernel waits uninterruptibly for the answer to
  a request it sent, so a wait that could not be interrupted would leave its
  process unkillable for as long as the park lasts. (A caught signal now
  gives `EINTR` from `read`/`write` during a park instead of `EIO`.)
  An op the park gate did not hold — it looks before the op's admission and
  at an estimate of the grant's end, so a grant that lapses while the op
  queues, or after a forward step of the clock, passes it — meets the fence
  later, and there it is **not** left to wait on whatever thread it is on:
  a read, which runs on an io_uring ring's thread, is handed to the pool at
  its fence (`lapse_late_parks`); any other op waits on its own thread (an
  *inline* wait, `lapse_inline_waits`, `lapse_inline_wait_ms_total`) that
  ends with its caller's signal (`EINTR`, `lapse_inline_interrupted`: the
  op's cancellation, or the one its frontend registers for the wait when
  asked, `Responder::begin_wait`; so does the wait of a mutation for its
  fencing token), with the verdict, or at the lapse's bound — and never on
  a ring thread (`fuser-ring-N`): there it is refused, `EIO` as before F-c,
  counted `lapse_inline_refused_thread`, which a load campaign expects to
  stay 0. Over a ring a wait with no lock-wait entry to hold is refused too
  (`lapse_park_refused_ring`). (An op the frontend did not wrap to park —
  it began more than a second before the grant's end, so it has no
  registration to ask for — has only the verdict and the bound to end its
  inline wait.)
- The holder sends a **late renewal** of the same grant, routed like any
  renewal (the lease is read when the owner is not known), every
  `lock_poll_ms` (at most a second) until it is answered.
- The owner **admits** it only where it can prove that no grant conflicting
  with it was issued since the grant was made, by any owner: a grant it
  made is *settled* (a mint is made only where no grace, floor or barrier
  holds the inode, so every grant before it was issued before it; a move
  keeps what it was), and a settled record stays *provable* as long as it
  is in the owner's table, expired or not, or — dropped by an outwait, not a
  release — in a bounded memory of lapsed records, which every change that
  could let another node act on the inode forgets (a conflicting grant, a
  newer grant of the same node, the inode leaving the table, a deposition).
  The model (`crates/model/src/locks.rs`, "Plan 43 C7-F F-c") has the
  argument and the three naive variants it found violating: a late
  reclaim of a grant the owner does not know inside a grace (it may have
  outwaited it and granted another node, which a successor never heard of),
  a successor that trusts a mirror that is not the old root's last, and a
  memory not forgotten by a conflicting grant. Anything else is refused
  (`Lost`), grace or no grace.
- A successor knows the old root's table only through the asynchronous
  mirror, which cannot tell it whether it missed a later one. So the old
  root's last mirror is made exact: when its lease is no longer usable
  (the end of the window in which it may add a record) it sends its
  backups its whole table as the **final mirror** of that lease, naming the
  lease body, re-sends it every backup heartbeat until the lease ends,
  and adds nothing under that lease from then on (it answers `NotOwner`:
  no grant, no renewal, no late admission, no move-in; a renewed lease is
  another body and serves again). A fast successor whose takeover replaced
  exactly that body installs the records with their flags and keeps the
  lapsed ones as its memory; any other mirror (no final one, another
  body, a renewed lease) is installed unsettled as before and a late
  renewal of its records is refused. A final mirror that arrives after the
  takeover is ignored.
- Admitted, the grant is honoured again from the late renewal's send
  (`sent + ttl − margin`, the usual rule); the parked I/O runs. Refused, the
  owner is fenced as a lapse always did it, from the end of the grant's
  window. No answer within `lock_owner_wait_max_ms` of the lapse (112.5 s at
  the defaults, the bound of every wait across a failover): the same.
  A late `Ok` after that is ignored for the fence (the owners captured stay
  fenced) and the grant is released normally. An op in flight when the
  grant lapsed, which its executor refused as lapsed, is sent again under a
  fresh token once the grant is honoured again.
- A recall of a lapsing grant marks it recalled, and it is released after
  the verdict; a new local lock request on the file waits for the verdict
  (it must not replace the lapsing grant, which would fence its owners); an
  unlock is allowed, and the grant stays lapsing while a write under it may
  be unpublished, else it is dropped. The hint that a write was made under
  the grant (`written`) stays set until the grant ends, even after a publish:
  a grant that wrote once is parked for, not dropped, whenever it lapses
  (a late renewal costs a message, an unneeded drop costs data).

What the application sees: a lock held through an owner's failover costs
latency, not an error, and no data is discarded — **if** the owner can prove
nothing was granted over it. It sees `EIO` as before when the owner refuses
(another node was, or may have been, granted the lock; there is no final
mirror: `ack=s3` without backups, a TTL takeover, a predecessor that died, a
mirror that was cut off; the grant was reclaimed in a grace, which is not
settled), and when nobody answers in time. The same applies to a lapse
without a failover (a holder or its link stalled past its window): the owner
still has the record unless it granted the inode to another node since.
Residuals: a grant reclaimed in a grace that lapses later still fences; a
lock in a subtree whose delegate's authority lapsed with the root's; and a
lock whose owner is a successor that did not take over the old root's
lease (a TTL or released takeover): there is nobody that can prove it.

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
  request or renewal). The window lives on the node's honour clock; the
  token carries it as a wall-clock time, minted when the mutation is built
  (the wall clock now plus what is left of the honour window), because the
  executor judges it on its own wall clock (so that comparison is still
  bound by the clock offset, plan 43 C7b). Operations of processes that hold
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
  - **Where the write happens.** The check that counts is made inside
    the operation's own store transaction, once it holds the store's
    single writer lock and before it writes anything, on the
    executor's clock read there. An operation can wait for that lock
    for seconds behind a slow writer (a journal write blocked on the
    disk), or in an authority-core step that read its clock before it
    waited. A check made before the wait could pass a grant that ended
    during it, and the late write then landed after the next holder's
    writes and was answered `OK` (load review F1). An earlier check
    before the wait remains, and only refuses sooner.
  - **Until the commit.** A write that passed this check holds the
    writer lock until it commits, which can still take seconds. Its
    grants stay *pinned* until then. Before this node reports a
    position as covering what it executed — the floor of a grant, the
    cut an outwait barrier waits for, a delegate's stream head on a
    renewal — it waits for every pinned write under a grant its table
    ended or whose record there ran out (`locks.settle_waits`). The next
    holder's floor therefore has every write that passed a check. A
    write that checks later finds the grant ended, or its window over,
    and is refused. An executor that does not hold the grant judges it
    by the window, and its writes reach the next holder only through a
    cut as of the outwait, which is `2 × margin` after the window's
    end. Such an executor (a delegate running a write under a lock the
    root minted) waits only for a pinned write whose token's window is
    over on its clock: a cut as of an earlier time settles no barrier.
    So a delegate's renewal waits for a tagged commit only after that
    commit outlasted its window, not for every one in flight.
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
  refused the same way. A refusal by the check at the write is the same
  refusal (`LockLapsed`, `EIO` or a re-send as below), and is also
  counted as `locks.token_rejections_at_write`.
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
    once: the one whose close, `fsync` or `O_SYNC` write was refused
    reports it with that answer, and not again at its next `fsync` (whose
    data then lands). A refused replay also leaves a conflict copy, never its
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
  release ends the grant). The last operation in flight wakes the
  waiting release; a 250 ms poll is the fallback. If the wait outlasts
  the grant's window, the grant lapses there and its owner outwaits it.
  After that wait, a grant under which an operation ended in doubt, or
  with a stranded one still queued, is **not released**. Its token is no
  longer checked anywhere, but an executor that checked it in time may
  still be committing the operation, and a release would let the owner
  grant the lock at once, from a floor that lacks it. The node drops
  the grant and leaves it to its owner's outwait (`locks.doubt_lapses`).
  The outwait's barrier waits for a cut that every executor settles its
  pinned writes for. On the owner's own grants this costs nothing extra.
  When the owner's own grant runs out unreleased while one of its
  operations tagged with it is in flight or ended in doubt, it leaves
  the same barrier: its own release floor does not have that operation.
  An operation in doubt is resolved, and the grant released as usual,
  once its `Completed` (or its refusal) has reached this node's replica
  with the log: then it has committed, or never will. One that ended in
  doubt without a request id (the simulation's clients) is never
  resolved. The next holder waits for the owner's record to end
  (`ttl + margin` from the last renewal, about `2 × margin` past the
  holder's window) and then for the barrier's cut. At a root with live
  delegations, the cut also waits for every live delegate's next
  renewal sent after the record's end: up to a quarter of the delegation
  TTL while the delegate has grants out, half otherwise. At a delegate,
  it waits for the root's next granting renewal answer, which the
  delegate asks for at once.
  The same holds for the next turn on the same node under the same
  cached grant, for what the earlier turn already counts as done: a
  lock taken when no local lock is left on the file waits (a
  non-blocking request for a short while, then `EAGAIN`, see
  [Close and release](#close-and-release); `locks.predecessor_waits`)
  while a close's publication of the file is in flight under the grant,
  or an operation tagged with it ended in doubt inside its window
  (pinned as soon as it is found in doubt, before its answer goes out).
  The grant is live at its minter, so such an operation would otherwise
  pass there after the new turn's writes. An operation still waiting
  for its answer is concurrent with the unlock, and only the release
  waits for it.
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
   live from its latest arrival, not from when it is served — but never
   ending sooner than the node's live grant on the file that it
   replaces (an upgrade, or a re-affirmation under a new id): the
   sequencer keeps one record per node and file, and the node honours
   the later of the two windows. Two ops of one node waiting in one
   mode are two waiters, and the node's re-sends refresh only the first,
   so the second's arrival can be many seconds old when both are served
   in one pass. Its grant, counted from there, replaced the first one's
   record with one that ended inside the window the node honoured, and
   the sequencer outwaited it and granted the lock to another node while
   the first op was still in its section.
   A grant is pushed only from a request recent enough for the grant to
   reach its holder before its first renewal is due: no older than half
   of `ttl − margin` of the *granted* ttl (the holder renews that far
   into its window). That is 9.5 s for an uncapped 20 s grant, but a
   delegate's grants (and a lease-capped root's) are capped by what is
   left of its authority: about 2 to 3 s under a delegation of 5 s, so
   for them it is 0.5 to 1 s, against a waiter that re-sends every 9.5 s
   (the daemon's default delegation TTL is the lock TTL, `Config::
   default_delegation_ttl_ms`: its delegates' grants are capped at 13 to
   18 s and the window at 6 to 8.5 s, so the rule asks less there; it is
   for a short delegation TTL, a delegate whose renewals lag under load,
   a root near the end of its lease, and a push slow in transit). Served
   from an older request, a grant counted from that request reached its
   holder lapsed (below), asked again behind the next waiter's recall, and
   the owner's record of it, counted from the same request, was expired
   when it was made: the next waiter of the pass was granted over it, or
   the unused grant was outwaited. So a waiter whose turn has come with an
   older request is not granted. The owner asks it to ask again
   (`asked_again`: the `LockGranted` push carrying `Waiting { retry_ms: 0
   }`, no new message) and holds its turn for up to two request cycles
   (`2 × (recall_hold_ms + poll)`, 600 ms at the defaults, and four round
   trips of the node more as the core measures them, at most three times
   that): nothing that conflicts with it is granted to anyone else
   meanwhile, and a conflicting non-blocking request is refused `EAGAIN`
   (`reserved_waits`; the lock is promised and in flight, so it is a known
   conflict). The node answers the ask by re-sending the request of every
   op of it waiting on the inode with none in flight (one in flight is
   left alone: its reply carries the grant), and the owner grants over
   that request, from its arrival. The ask grants nothing and extends
   nothing. A waiter that does not ask again within the hold is passed
   over until its next request (`ask_again_passed_over`), keeps its place
   in the queue when it comes, and two asks in a row left unanswered mark
   its node unreachable (below). Any request that comes after an ask, in
   the hold or after the pass-over, is heard: it resets what unused grants
   and unanswered asks counted against the node.
   A waiter's place is where its application's wait began. Every request
   carries how long the application has waited (`waited_ms`, a duration:
   the clocks of nodes are never compared; the engine's lock call and the
   sim's client send it with every round), and the owner queues the
   request from `now − waited_ms` (at most `4 × ttl` back), the owner's
   own ops likewise (`Control::Lock`). A client that lost a round to a
   recall, or to another client of its node, so keeps its place; it
   used to join the queue again at the back, and among the clients of the
   owner's own node, which take the grant in one pass and race for it, the
   same few lost every round for a minute (`locks-long-grants`: the owner
   node's longest wait over 50 s in 11 of 15000 seeds before, 30 with the
   ask alone). A grant that is installed and not recalled answers every
   other op of the node on the inode that it covers at once (an op's
   request in flight is moved to `done_reqs`, as a push moves it): one
   push used to wake the one op it was matched to and leave the others to
   their own re-send, 9.5 s later at the production lock ttl
   (`covered_woken`). A grant that arrives recalled admits one local lock
   and wakes no other. The waiters those ops left at the owner, and a
   waiter an ask finds no op of its node for (an op another op's grant
   covered, or that left), are cancelled (`LockCancel`, one way; the owner
   drops its parked waiters of the node there that it last had a request
   of before the cancel was decided: `cancels_sent`, `cancelled`), so that
   the owner does not hold its queue for a turn nobody takes.
   `waited_ms` is the requester's claim and only places a request in the
   queue: a peer that overstated it could jump ahead by at most `4 × ttl`
   (80 s at the production lock ttl), the same bound as a served node's kept
   place. The cancel's `upto` and a request's `sent` are the requester's
   own wall stamps, compared only with each other: a backward step of its
   clock between a request and a cancel can make the owner drop a waiter
   that is still waited for (liveness only: its next re-send parks it
   again; plan 43 C7-H1 moves both stamps to the wait clock).
   The cost of an ask is one round trip per handoff to a stale waiter.
   A waiter
   silent for longer than `ttl − margin` is passed over (the ask covers
   the range between half of it and that: older, a request is taken for
   lost): a grant would already have lapsed when it arrived. A grant that
   arrives with less
   than a quarter of the margin of its window left (the window counts
   from the request's send) is treated as lapsed on arrival: the node
   asks again, and the owner re-affirms the grant from the fresh
   request. This is now the fallback for a push held up in transit or
   behind a stalled step; before the ask it was the normal case of a
   delegate's queue. Installed, it lapsed under the I/O it let in before a
   renewal could come back: a waiter served from a delegate's queue
   near the end of its re-send interval, under a ttl capped by the
   delegation, got 5 to 20 ms of it (`locks-delegated-writes`). After `4 × ttl` of silence the
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
   `EAGAIN` at once, also while a waiter's turn is held after an ask
   (above). The recall still goes out, so a retry succeeds once
   the other node's application unlocks (SQLite's busy loop relies on
   this). One that nothing conflicts with, but which the owner may not
   grant yet, waits (next section).

Each grace ends at a time the owner knows, and a timer at that time
serves the parked waiters it held back (`grace_end_serves`): they used to
be served only at their next re-send, up to `recall_hold_ms` plus a retry
later.

### Waiting for a grace

A non-blocking request for a lock that the owner's table shows no
conflicting grant for, and which the owner may not grant *yet*, waits
until it may, and is then granted, instead of failing with `EAGAIN`. What
the owner may not grant yet:

- a grace: the takeover floor of a fast successor, a restart's horizon, a
  released takeover's or a dropped tenure's grace, a subtree grace after
  an outwaited delegate ([Failover](#failover));
- a new root tenure's floor, not known yet;
- an outwaited holder's barrier, not settled yet;
- a delegate's authority, too short to grant anything the holder could
  keep (the delegation is renewed meanwhile);
- an owner that may not grant at all yet: its S3 liveness is stale (a
  holder cut from S3 whose lease is not over, which a backup could seal)
  or its lease is not marked as granting yet (`stale_owner_answers`); it
  answered `Busy` to such a request, which the requester retried three
  times and failed `ENOLCK`;
- an owner that cannot be reached at all: its link is dead and the lease,
  read again, still names it (a failover under way, its successor not
  named yet). The requester asks again as a fresh request, with fresh
  attempts and a lease read, routed to the successor as soon as the lease
  names it (`unreachable_owner_waits`). The lease reads of the requests
  that need one are shared: one read in flight serves every request that
  asks meanwhile, and its result is taken as it stands by the requests of
  the next quarter second (`holder_reads` made, `holder_reads_shared` and
  `holder_reads_cached` requests that made none), so hundreds of waiters
  for a dead owner cost an S3 read about every quarter second, not one a
  second each. Without P2P there is no path to wait for: `ENOLCK`, as
  documented.

A refusal or a request to wait is answered at once, never queued behind
an answer the owner holds back for a lease read or a durable write (see
"Answer order" below).

A conflicting grant, and the requester's own grant under recall, still
answer `EAGAIN` at once, as before, whichever of these the owner is
waiting for (behind a new tenure's floor too): only a lock nobody holds
waits. The
owner answers "wait", with the time to ask again (the grace's remainder,
at most a second, so a request is routed afresh when the ownership moves;
a quarter of the forward backoff for the others), and the requester asks
again without spending one of its attempts, so any number of answers
leads to the grant.

The wait never holds a frontend thread. A request that has to wait is
handed to a bounded pool of `lock-wait` threads (at most 16, however many
requests wait: a wait is a timer and a short step every so often; a
thread whose step is at an owner that does not answer, which can take
seconds, is not waited for: a thread starts to watch the others' turns
while it is there, up to the 16), and the mount answers it from there.
Ops parked behind a lapsing grant (above) are served by up to four threads
of their own, and run again on a pool of their own (`lock-redo`): a parked
close or `fsync` waits on the store for as long as it is down, and a lock
request waiting for the new owner, or another parked op's next look at its
verdict, waits for no thread of that. It holds neither the FUSE worker nor, over
the io_uring transport, an offload thread (the `n_threads` of the pool,
shared by every ring: a wait of 23.5 s on one would have stalled whatever
else is offloaded to it). It counts as a deferred reply, so an upgrade's
handover refuses while it waits, and it is not reported as a stalled
request. The FUSE adapter registers the wait for the caller's signal, and
counts it, only when the request has to wait, so a request answered at once
(the usual one) pays for neither. A frontend that cannot answer from
another thread waits in the call, as for a blocking lock.

**Over the io_uring transport the waiting request holds its ring entry.**
The kernel queues a request on the queue of the CPU that sent it and
serves it from no other queue and not over `/dev/fuse`; a request that is
not answered keeps its entry, and while every entry of a queue is held the
kernel holds back every further request of that CPU, whatever the file or
the opcode, until one is answered (here: until the grace ends, up to the
wait's bound). That is why a queue lends at most `depth − 1` of its entries
to requests that wait on something other than the daemon, one budget for
`FUSE_SETLKW` and for these waits (`RingCommit::reserve_lock_wait` in the
vendored fuser, asked for through `ReplyEmpty::hold_for_wait`), so that
one entry stays free for the others. When the budget is spent, a request
that would have to wait is answered at once, as before the wait existed:
`EAGAIN` for a non-blocking request, `ENOLCK` for a `FUSE_SETLKW` served
as non-blocking past the budget; both counted (`grace_wait_refused_ring`,
and the ring's `lock_wait_downgrades`), and the load campaign fails on
them. With the default depth of 32 that takes 31 simultaneous lock waits
on one CPU's queue.

How it ends:

- granted: the lock is taken at the end of the grace (the wait is as long
  as the grace is, up to 23.5 s after a mid-lease crash with the defaults,
  longer with delegations). After an S3-cut holder's failover the wait
  starts at the stale owner and lasts until its backup has taken over and
  its successor's grace is over: 37 to 61 s measured at the production
  lease (60 s: a request began 15.6 s after the cut, the takeover came at
  +53.6 s, the first lock was granted at +77.1 s), about half that at the
  harness's 20 s lease;
- `EAGAIN`, if the table then shows a conflict, or if the request has
  waited `lock_owner_wait_max_ms` from its start, one bound for every wait
  on an owner: a stale owner's lease (it may have just been renewed) and
  the margin clock skew adds to its expiry, its backup's acquisition
  (`acquire_retry_max_ms` at most), and the successor's grace with the
  bound an operation has without a sequencer (`lock_grace_wait_max_ms`:
  the takeover window, the longest grant, the delegations' reclaim horizon
  and the two margins, plus 20 s; 49.5 s at the defaults): 112.5 s with the
  defaults, 72.5 s at a 20 s lease. Computed from the node's
  configuration, no knob, and finite for every configuration: no wait is
  unbounded, one for a continuation epoch with no successor included. That
  `EAGAIN` is counted (`grace_wait_gave_up`); a load campaign expects it
  to stay 0;
- `EINTR`, when a signal reaches the process (the mount cancels the wait
  as it does for `F_SETLKW`): Linux documents `EINTR` for `F_SETLK` on a
  file system that waits, as NFS does. It is observed within 100 ms
  whatever the request is doing: between attempts (the wait pool looks at
  every waiting request a slice at a time) and during one, where the wait
  for the core's answer (a round trip to the owner, a few seconds against
  an owner that cannot be reached) is made in slices of 50 ms that look at
  the cancellation. A request's first attempt registers for the signal
  when the core's answer is more than one slice late (an attempt answered
  at once registers nothing). One the core had not answered when the signal
  ended it may be granted a lock afterwards: the engine keeps that answer,
  and the next request for the file waits for the grant's floor (what the
  previous holders wrote) and drops the kernel's caches of it before it
  takes a lock under the grant, as the request would have; a grant no
  request takes is a first use abandoned, as for an answer that was lost.

A request told to wait for an owner that cannot say when it may grant (a
floor, a barrier, a stale or an unreachable owner: the owner hints
`lock_poll_ms`, a quarter of the forward backoff, 50 ms) asks again with
backoff: twice the last interval, up to a second, so a hundred requests
waiting forty seconds for a stale owner ask it a few hundred times in all,
not two thousand a second. A hint longer than the polling one is a grace's
remainder, exact, and is followed as told.

**Answer order.** An owner sends the answers to one peer in order: an
answer made after a held one waits behind it (a grant must not reach its
holder before the recall it follows). A stale owner holds the renewals it
answers until a lease read says it still holds, and with S3 away that read
takes as long as S3 does (20 s in the harness's failover): the refusal, or
request to wait, to a different request of the same peer sat behind it and
arrived after the request's own timeout, three times in a row. Answers
that grant and extend nothing (a wait, a refusal, a redirection) now go
ahead of held ones. Counted: `answers_order_bypassed` (answers that went
ahead: in the S3-cut failover at the production lease, 22 to the
contender, behind a renewal held 2.1 to 19.6 s, median 8.6 s: each would
have waited out the request's timeout) and `answers_order_held` (answers
that still wait for the order alone); a debug line of
`constellation_authority::core::locks` says for how long.

The precedent is the grace period of NFS and NLM: their clients retry a
request the server refused for its grace, blocking or not, until the
grace ends, and a signal ends the wait. A `FUSE_SETLKW` past the
`depth − 1` waiters of its queue is served as a non-blocking request and
cannot wait: it answers `ENOLCK` at once, a free lock in a grace
included, as before.

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
  executed under the grant was executed before the record ended. A
  write that passed its check before then and is still committing is
  waited for by whoever reports a position for the cut (see
  [The fencing token](#the-fencing-token), "Until the commit").
- The owner's own grant needs no barrier: what its clients were
  acknowledged is in its release floor (its whole unshipped journal,
  not only the lock file's rows), which becomes the file's floor when
  the record is outwaited — by the expiry timer, or found expired by
  the next request. (The second case left no floor: after the owner's
  core stalled past its own grant's window, the next holder read the
  older version of a file the lock guarded, sim `locks-stall-step`.) It
  takes a barrier only while one of its own mutations tagged with the
  grant may still land at another executor: one in flight, or one that
  ended in doubt.
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
  for a takeover of a marked released lease (a barrier as of the end of its
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
  which is fenced until its application unlocks and locks again (its
  holder asks for the grant back and is refused: the new holder has no
  table that could prove nothing was granted since, and there is no final
  mirror of a lease that was never given up). There is nothing to
  reclaim.
- **Fast takeover** (a sealed backup, or `ack=s3`; see [Durability and
  failover](durability-and-failover.md)): the new holder waits out a
  grace period before it grants anything new, and accepts renewals of
  grants it does not know as *reclaims*. The grace covers what the old
  holder answered within the takeover window of its last proof of S3
  liveness, which is all it may answer, grants and renewals alike (see
  [Leases, renewal and fencing](#leases-renewal-and-fencing)). The grace
  is `marker + takeover window + max(read delegation TTL, lock TTL) + 2 ×
  margin` on the successor's own clock, counted from its own marker, or,
  if earlier, the old lease's end as the old holder's own heartbeat places
  it on the successor's clock, plus one margin (`X + min(arrival − sent) +
  margin`, where `X` is the latest of the expiry of the lease the takeover
  replaced, the expiries the beats named and the guard they named: what the
  old holder itself waited out, such as its own takeover floor; see [the
  heartbeat's cap](durability-and-failover.md#what-a-seals-successor-waits-for)).
  The old lease's expiry read against the successor's clock never cuts it
  short: that is a stamp of the old holder's clock, and a successor ahead
  of it would read it as past and grant over locks the old holder's nodes
  still honour; the heartbeat's `sent` is on the holder's clock too, but
  it enters only as a difference on that clock, added to the beat's
  arrival stamp on the successor's. (A takeover in the last seconds of the
  old lease therefore waits the full grace unless the old holder was
  beating, where it used to wait only until that expiry; a holder cut from
  S3 and alive beats to the end of its lease, so its successor's grace
  ends about a margin after it; and a seal or an `ack=s3` takeover is a
  fast one, with the grace, even when the successor's clock reads the old
  lease as already expired.) It holds back new lock
  grants only: the acknowledgement floor that protects read delegations
  is computed with the read delegation TTL alone, so a long lock TTL does
  not stall writes after a takeover. The holder also mirrors its lock
  table to its backups asynchronously (`LockMirror`), and a fast
  successor installs the last mirror. For new grants and reclaims the
  mirror is only an availability aid; the grace is what keeps locks
  exclusive. What the mirror adds, when it is the old holder's **final**
  one (sent when its lease stopped being usable, naming that lease body,
  and the takeover replaced exactly that body), is *proof*: the old holder
  added nothing under that lease afterwards, so a grant of its table that
  lapsed is asked for back by its holder and admitted without having been
  granted over ([A lapsed grant is asked for
  back](#a-lapsed-grant-is-asked-for-back)); from any other mirror the
  successor installs the records as before and refuses such a late
  renewal. Until the grace ends, a
  non-blocking request for a lock nothing conflicts with waits for it
  (see [Waiting for a grace](#waiting-for-a-grace)) instead of failing
  `EAGAIN`; a blocking one is parked, and served at the grace's end.
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
  restart never waits out less than a peer was told. A restart after a
  reboot (another boot id) cannot read the time left on the raw clock and
  waits the span the horizon was written with (up to a lock's TTL, its margin,
  a quarter TTL and a second: 27 s at the defaults) before the first lock
  grant, but only if grants were live when the node stopped: a graceful
  shutdown deletes a horizon that has already passed, so an idle node's
  reboot waits for nothing.
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
  live, as with any idle holder. An owner that may not grant at all yet
  (no fresh S3 liveness, a lease not marked as granting yet) makes a
  non-blocking request for a lock nothing conflicts with wait for it,
  as for a grace ([Waiting for a grace](#waiting-for-a-grace)); it used
  to answer `Busy`, which a non-blocking request failed with `ENOLCK`
  after the usual few attempts. Before, every close dropped
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
that takes over the released lease waits until `lock_ttl + 2 margins`
after it first *saw the release* (plan 43 C7b: the first read, on its own
wait clock, that showed the released key: the releaser's grants were issued
before its release CAS, so each is over by `lock_ttl` after that CAS, and
the first sight is after it landed; a taker that saw the release long ago
waits nothing, a cold one the whole grace; either way no grant is made
before the floor covers the delegates' streams up to the grace's end; the
configuration warns when
`lock_ttl + 2 margins` exceeds the lease TTL), and the releasing node, should it claim its own released
lease back (no takeover), waits until the last dropped grant would have
expired. Both waits are kept on the node, not with the tenure, so they
survive that tenure's own release: a successor whose next epoch flush
released the lease a moment later, then claimed it back, granted over
its predecessor's live exclusive grant (`locks-blips-tight` seed 2723).

The released lease says whether there is anything to wait out: a
release marks it (`granted_delegations`) when the lock table holds any
grant record (live, or expired but still renewable: a renewal is served
while the release is under way), a grant moved toward a delegate, a
subtree grace or a quarantine. A successor that takes over an unmarked
released lease grants at once. An idle release and a handoff wait for an
empty table, so they are never marked by a grant; before this rule their
taker waited `ttl + margin` all the same (harness `lock-failover`: the
successor's idle release raced the locker's unlock, and the contender
waited 16 s for a lock nobody held).

From the mark on, an unmarked release extends nothing: while its CAS is
out (or being read back) the root answers every renewal `NotOwner { 0 }`
and the holder relearns the owner. The table can still take a record
then — a reclaim under a read delegation's quarantine, which the mark
does not count, or the grants a delegate's recall answer hands back —
and a renewal served from it would give its holder a window past the
release, beside the grants of a taker that grants at once.

### Delegations

The lock table follows the subtree. The root hands a subtree's grants to
its delegate with the delegate's first renewal, and gets them back when
the delegation is recalled. A grant does not get a fresh lock TTL at its
receiver, as it would from a restamp (`now + ttl + margin`): it moves with
what is left of its record, a *duration* the sender measures as the grant
leaves its table (the root as the delegation is granted, re-measured at
every re-send of the handoff; the delegate as it answers the recall), and the
receiver records `min(restamp, now + left + margin)` from the receipt.

The restamp was the cost of every move to whoever waited behind a grant whose
holder was gone: a push that lapsed or found no op, a holder fenced under a
delegate's short authority, a holder that released at the old owner after the
move (its release finds no record there, and the new owner keeps the copy).
Nobody releases such a record, so the waiter waited for it to end: a whole lock
TTL again at every move, 21 s at the production TTL. A move of the heavy
delegated production-TTL configuration (nine ops on a file, renames crossing
subtrees) of the sim failed 729 of 2000 seeds, the same configuration without
moves 28; carried, 33.

The carry is safe because the sender's record already ended no sooner than any
window its holder honours, the sender gives the grant up as it measures (the
root's copy is frozen, the delegate stops serving at the recall; neither renews
it afterwards), the receipt is after the measure, and the margin added at it
pays for the second clock: each hop adds one, and a carried record is never
longer than a restamp. Only durations cross nodes. The root also never lets the
carried record end before one it already has for the same node on the inode (a
reclaim it answered meanwhile). The mirror to the backups, the grants that leave
with an unlinked file and a grace's end are still restamped. `lock_moved_carried`
(status `locks.moved_carried`) counts the grants a move installed carried. (The
model checks this: `crates/model/src/locks.rs`, `MoveStamp::CarryAll`.)

A delegate's grants are capped by its delegation (`delegation_ttl −
2 × margin` at most, less what is spent since its last renewal): 13 to
18 s under the daemon's default delegation TTL (the lock TTL: `Config::
default_delegation_ttl_ms`, which `CONSTELLATION_DELEGATION_TTL_MS`
overrides), about 2 to 3 s under a short one (the core's own 5 s, and the
sim's). A handoff to a waiter whose last request is older than half of
that (minus the margin) costs one round trip more, the ask described under
[Conflicts, recalls and blocking
waits](#conflicts-recalls-and-blocking-waits), not a 9.5 s re-send or an
outwait.

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

A recall's answer takes the subtree's grants back to the root, which
ends the generation in its own journal at once. The other nodes' tables
drop the generation only when the `Recall` record reaches them, and
under an S3 cut (a continuation epoch recalls every delegation) that is
after the cut. Meanwhile their tables still name the delegate. The
recalled delegate therefore answers lock requests and renewals for the
subtree `NotOwner` naming the root that recalled it, also for a
generation it never installed. A node that hears that from the delegate
its table names sends the subtree's requests and renewals to that root
until its table drops the generation, or until the root itself answers
`NotOwner`. Before, the delegate answered `NotOwner { 0 }`, the holders
found no owner for the whole cut, and their grants lapsed under their
I/O while the root had them and would have renewed them
(`locks-blips-tight-delegated`: 482 fenced I/Os in 4000 seeds, now 0).

Name-hash range delegations need nothing extra: a file's lock owner is
resolved from its primary link, like every other ownership lookup.

### Without P2P

Locks never go through the [S3 inbox](forwarded-mutations.md#the-inbox-forwarding-without-p2p).
One S3 round trip per lock, plus renewals every few seconds as S3 writes,
would make SQLite unusable and would fence I/O whenever S3 is slow. So:

- with P2P off, the mode is `local`;
- with P2P on but the owning sequencer unreachable, a blocking lock keeps
  retrying until the sequencer is reachable again, and a non-blocking
  lock waits too, up to `lock_owner_wait_max_ms` (112.5 s at the
  defaults; [Waiting for a grace](#waiting-for-a-grace)), where it used
  to fail `ENOLCK` after the forward retries. The node re-reads the lease
  from S3 to find the current owner, and a non-blocking lock re-reads it
  once before it asks again as a fresh request (a dead holder whose
  successor gossip has not announced yet). Without P2P, `ENOLCK`.
- a successor that has taken the lease over but may not grant yet (its
  lease is not marked as granting, or its S3 liveness is stale) still
  refuses a non-blocking lock that a grant it knows of conflicts with
  (`EWOULDBLOCK`, as before the failover); a lock it would have to grant
  waits for it ([Waiting for a grace](#waiting-for-a-grace); it got
  `ENOLCK` after the retries before).

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
- The check at the write: every journaled transaction looks for a
  pending token and admission (two thread-local reads). A tagged one
  takes the lock table's mutex once more to check and pin, and once to
  unpin. A position this node reports costs one atomic load while no
  tagged write is in a transaction. It costs one writer-lock turn, that
  is a wait for the commit in flight, while a write under a grant this
  table ended is in one, or a write under another sequencer's grant
  whose token's window has passed here. A delegate's renewal therefore
  waits for a slow tagged commit only once that commit outlasted its
  window.
- The fencing token: while this node holds any local lock, each
  operation that mutates takes its caller's tag once (its `/proc` status,
  its process's identity and the lock table: about 7.5 µs measured on this host with
  the start-time read, which the pidfd read is slightly cheaper than; the same
  for a process that turns out to hold no lock). A tagged forward is 21
  bytes longer (one grant: a full-width node id, the seq, the window);
  an untagged one 1 byte. The executor's check is one lookup per token
  (11 ns measured). No extra message is sent. While locks are
  held under honoured grants, the owner fence costs a second load. While
  an owner is fenced, every such operation on the node takes the lock
  table's mutex and reads its thread's `/proc` entry and identity (so
  a recycled pid is judged afresh); a thread not yet seen reads `/proc`
  up its parent chain once (the verdict is cached per thread until the
  fenced set changes). Taking a lock reads the locking thread's `/proc`
  entries once (its process and their identities).

WAN numbers are measured separately (`lock-latency`, `bench/remote`).

### Limits

- **A wait for another node's lock cannot be interrupted.** A signal
  (Ctrl-C, `SIGKILL`, a handled `SIGALRM`) ends a blocked `F_SETLKW` or
  `flock` with `EINTR` while it waits on a lock held on its own node, or
  before its request reaches the owning sequencer. Once the owner has
  parked the request because another node holds a conflicting grant, the
  wait returns only once the lock is granted. (A non-blocking request's
  wait for an owner's grace, [Waiting for a grace](#waiting-for-a-grace),
  is interruptible wherever the owner is: it is retried from the mount,
  not parked at the owner.)
- **`EINTR` even under `SA_RESTART`.** A signal whose handler was
  installed with `SA_RESTART` restarts a native `F_SETLKW` transparently.
  Over FUSE the wait ends with a plain `EINTR` (as with libfuse), so the
  program sees it and must retry.
- **Handoffs the ask does not cover.** Two causes of a waiter's wait past
  the round trip are known and not changed by it. (1) A grant moved with
  its subtree (a delegation recalled, a rename across subtrees) carries what
  was left of its record ([Delegations](#delegations)); a holder that is gone
  is outwaited for that, not a lock ttl more. (2) A
  delegate's own idle grant lapses with its short authority
  (`ShortAuthority` waits), and the outwait and the barrier after it cost
  `ttl + margin` and more (3 to 4.5 s). A waiter whose op nothing waits
  for at its node and whose cancel was lost, or whose op left meanwhile,
  is asked once in vain: 600 ms of its queue's wait
  (`ask_again_passed_over`). In the sim's heavy delegated
  production-TTL configuration chains of 4 to 11 such outwaits in one wait
  fail 33 of 2000 seeds (28 of the same without moves).
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
    `fsync`s the file later. If that happens after the owner unlocked,
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
- OFD locks and mandatory locks get no special handling, except that
  an OFD lock ends with its open file description
  ([Close and release](#close-and-release)).
- A description's OFD and `flock` locks go with the kernel's
  `RELEASE`, which comes after `close(2)` returned. A request behind
  them waits for it for up to `CONSTELLATION_LOCK_CLOSE_WAIT_MS` from
  the close, and never longer than that from the request itself; a
  `RELEASE` the kernel holds back longer (asynchronous I/O of the
  description still in flight, a full background queue) is not waited
  for, and the request is refused as before. A single-worker mount
  takes no such wait.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `--locks local\|cluster` | `cluster` with P2P, else `local` | the mode |
| `CONSTELLATION_LOCKS` | unset | default for `--locks`; the flag wins |
| `CONSTELLATION_LOCK_TTL_MS` | `20000` | a grant's lifetime, renewed half-way through the window it is honoured for |
| `CONSTELLATION_LOCK_CACHE_IDLE_MS` | `30000` | how long a grant with no local lock under it is kept |
| `CONSTELLATION_LOCK_CLOSE_WAIT_MS` | `100` | how long a non-blocking request may wait for a just-closed description's locks to go, counted from the close, and for an earlier turn's operations still in flight — never longer than this from the request itself ([Close and release](#close-and-release)); `0`: no wait; a single-worker mount never waits for the closed description's locks |

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
  `grace_waits` (non-blocking requests that waited for an owner's grace,
  see [Waiting for a grace](#waiting-for-a-grace)), `grace_wait_ms_total`
  (the time those waits took in all), `grace_wait_gave_up` (the ones
  that gave up with `EAGAIN` after `lock_owner_wait_max_ms`; 0 in every
  load campaign), `grace_wait_refused_ring` and `grace_wait_refused_pool`
  (waits that could not begin and failed `EAGAIN` as before: the ring
  queue's lock-wait budget was spent, or no `lock-wait` thread could
  start; 0 in every load campaign), `unreachable_owner_waits` (requests
  told to wait because their owner could not be reached), `holder_reads`,
  `holder_reads_shared` and `holder_reads_cached` (the lease reads those
  requests made, and the requests that shared one),
  `recall_answer_duplicates` (grants of a delegate's recall answer dropped
  because an earlier copy had handed them back),
  `stale_owner_answers`, `answers_order_bypassed` and
  `answers_order_held` (above),
  `fenced_owners` (the owners fenced now: `owner`, `pid`, `since_ms`,
  and `grant`, the `[minter, seq]` whose end fenced it), `margin_ms`
  (the clock-skew margin the lock machinery assumes),
  `tagged_ops` (mutations that carried a fencing token),
  `release_waits` (releases held back for one) and `predecessor_waits`
  (lock requests of new local turns held back for an earlier turn's
  close publication or mutation in doubt, once per request),
  and, plan 43 C7-F F-c, `lapse_parks` (grants whose window ended while
  they pinned local locks or writes: the I/O they fence waited),
  `lapse_parks_resumed` (the owner admitted a late renewal: nothing was
  lost), `lapse_parks_refused` (the owner refused: `EIO` as before),
  `lapse_park_gave_up` (no verdict within `lock_owner_wait_max_ms`),
  `lapse_park_ms_max` (the longest park), `lapse_parked_ops` (ops that
  waited), `lapse_park_interrupted` (ended by their caller's signal,
  `EINTR`), `lapse_park_refused_ring` and `lapse_park_refused_pool` (ops
  that could not be held, for want of a ring queue entry or a thread, and
  failed `EIO` at once; 0 in every load campaign), and `lapse_late_parks`,
  `lapse_inline_waits`, `lapse_inline_wait_ms_total`,
  `lapse_inline_interrupted` and `lapse_inline_refused_thread` (ops that
  found the grant lapsing at a fence: parked there, waited on their own
  thread, for how long in all, ended by a signal, refused on a ring thread;
  see above);
- sequencer side: `grants_table`, `grants_made`, `recalls_sent`,
  `recalls_released`, `recalls_expired`, `reclaimed`, `waiters_parked`,
  `grace_refusals`, `grace_wait_answers`, `not_yet_answers` and
  `stale_owner_answers` (non-blocking requests told to wait: for a
  grace, for a tenure floor, a barrier or a delegate's authority, and
  for an owner that may not grant at all yet), `grace_end_serves` (grace
  ends that served parked waiters), `requeued_in_place`,
  `released_superseded`,
  `peers_unreachable` (nodes taken for unreachable, see
  [Conflicts, recalls and blocking waits](#conflicts-recalls-and-blocking-waits)),
  `unreachable_passed_over` (their waiters passed over meanwhile),
  `incarnation_waiters_dropped` (waiters a restarted node's previous
  incarnation left queued), `stale_incarnation_requests` (requests from
  an older incarnation, answered and not served), `asked_again`
  (waiters asked to send their request again because their last one was
  too old for a pushed grant), `ask_again_passed_over` (those that did
  not within the hold; 0 without a fault), `reserved_waits` (requests
  refused or parked for a turn held for an asked waiter),
  `asks_answered` (node side: asks answered by a re-sent request),
  `covered_woken` (node side: ops answered by a grant installed for
  another op of the node), `cancels_sent` (node side: `LockCancel`s sent),
  `cancelled` (owner side: waiters a cancel dropped),
  `requests_in_flight`, `waiters`, `recalls_in_flight`,
  `token_rejections` (operations refused for a dead fencing token),
  `late_admitted` and `late_refused` (late renewals admitted, and
  refused as unproven), `mirror_final_sent` (final mirrors this node began
  as an old root) and `mirror_final_installed` / `mirror_final_mismatch`
  (as a successor: fast takeovers whose mirror was the final one of the
  lease replaced, and ones whose final mirror named another lease).

These counters are not exported to `/metrics` or shown in the web UI.

## Troubleshooting

### I/O fails with `EIO` on a locked file, or on any file while a lock is held

The node's grant lapsed and its owner did not give it back: `fenced_io`
rises on the locked file, `owner_fenced_ops` on the lock owner's
operations elsewhere, and `lost` counts the grants. Since plan 43 C7-F
F-c a lapse alone is no longer this: while the owner has not answered the
holder's late renewal (`lapse_parks` rises, then `lapse_parks_resumed`)
the I/O only waits, and it fails `EIO` when the owner refused
(`lapse_parks_refused`: another node may have been granted the lock, or the
owner cannot prove otherwise, e.g. after a TTL takeover or with no final
mirror) or when no verdict came within `lock_owner_wait_max_ms`
(`lapse_park_gave_up`). An op that waits for a verdict for seconds is a
`lapse_park_ms_max` of that length; a process stuck in `read` or `write`
on a locked file during a failover is parked, not hung (a signal ends it
with `EINTR`). An operation that was already on its way when the
grant lapsed is refused by its sequencer instead: `token_rejections`
rises there, and the daemon logs "the lock grant it was issued under is
no longer live". The usual cause is a stall of the node, or a
partition from the owning sequencer, longer than
`CONSTELLATION_LOCK_TTL_MS` (on the holder itself, a stall of its
authority core: renewals of its own grants run there too, so a
`slow core step` of that length in the log fences them; the disk under
`daemon.log` is not such a cause (unless the queue is off,
`CONSTELLATION_LOG_QUEUE_BYTES=0`): a daemon's log writer never blocks the
core, and a `log lines dropped` line says where that disk made the log lose
lines), or a TTL
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

### `flock -n` or `F_SETLK` takes seconds to return

The owner is in a grace (a failover, a restart) and the request waits for
it ([Waiting for a grace](#waiting-for-a-grace)): `grace_waits` rises on
the node, `grace_wait_answers` on the owner. It is granted at the end of
the grace, or fails `EAGAIN` after `lock_owner_wait_max_ms`
(`grace_wait_gave_up`), or `EINTR` on a signal. After the failover of a
holder cut from S3 it can take most of a minute at the production lease
(the stale owner first, then its successor's grace): `stale_owner_answers`
on the old owner, `unreachable_owner_waits` on the node.

### `flock -n` or `F_SETLK` fails once, then succeeds

Another node still cached a grant on the file. The first attempt sends
the recall and returns `EAGAIN`; the next attempt gets the grant. A
steady `would_block` count on a file nobody else locks means two nodes
alternate on it. On one node, with `predecessor_waits` rising, an
operation of the locking process ended in doubt (S3 or the sequencer
unreachable) and pins the file's grant until its window ends, or a
close of the file was still publishing past
`CONSTELLATION_LOCK_CLOSE_WAIT_MS` (see
[Close and release](#close-and-release)).

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
