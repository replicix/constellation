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
  - [fsync during an S3 outage](#fsync-during-an-s3-outage)
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
  an append, starts its takeover, or acknowledges the seal;
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
op has already executed; only its reply waits. The holder's FUSE fast
path executes its own writes the same way and then waits for their rows
to be durable before it answers. If the lease is lost during that wait
(or the wait runs out), the write is in doubt: it is resubmitted through
the authority core under the same rid, in one event that marks the rid in
doubt and submits it (`SubmitInDoubt`), so the core never sees the op
without its mark. The core then answers it from `completed` wherever the
rid already has an outcome, and executes it only if it has none. If a
deposition also queued the write's row for replay by rid, the replay and
the resubmission are the same op: whichever runs first decides it, and
the other finds that outcome and does not run again.

**Reconfiguration.** A backup that makes no acknowledgement progress for
`CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` (1 s), or whose link goes down, is
removed by a lease CAS that bumps `config_version`. Until that CAS
lands, waiting acknowledgements still need it. A new backup is first
streamed the unshipped journal, and is CASed into the lease only once
it has caught up.

### Seal-based failover

A backup that has heard nothing from its holder for
`CONSTELLATION_BACKUP_TAKEOVER_MS` (1.5 s):

1. re-reads the lease, and continues only if it still names that holder
   and epoch, still lists this backup, and the holder was not heard
   while it was read — unless the lease it read has expired: a holder
   stops acting a margin before its expiry, so what reached the backup
   then was sent before (the backlog of a stall of the backup's own
   runtime), and waiting another window would miss the claim grace
   below. A lease that has not expired and lists the backup continues
   only if the holder's silence, measured in the backup's attentive
   time (its own lapses discounted), is still a window or more;
   otherwise the holder is given a window again, no later than the
   lease's expiry, where the lease is read again ("Silence under host
   load" below). A backup the lease no longer lists was removed by
   a live holder (which stops appending once the removal lands, so the
   backup hears silence): it gives its role and tail up **without
   sealing**, and the holder may invite it back as a candidate in the
   same epoch. That is what happens across a `daemon --upgrade` or a K5
   handoff of the backup node: the holder drops it during the restart
   gap and brings it back once its link to the new incarnation is
   connected again (5-23 s here, or longer: the P2P link recovery
   after a peer restart is slow, an open bug; then the
   reconfiguration rate limit and the catch-up), instead of
   running without a backup for the rest of its epoch;
2. **seals**: persists and fsyncs "epoch *e* sealed", and answers
   every later epoch-*e* append with `sealed`;
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
only, the renewal is never starved by the ship loop, the liveness
heartbeat does not wait for the core at all), and backups are
chosen within `CONSTELLATION_BACKUP_RTT_BUDGET_MS` (5 ms), so the window
is several hundred times the link's RTT. Scaling it with the P2P RTT
would change nothing inside the budget. `slow-s3-no-seal` (1.5 s per S3
request, three minutes of writes from every node) checks that no live
holder is sealed. It does adapt to the host (see *Silence under host
load* below): the seal is safe at any threshold, so a longer window
costs only failover time, and only while a host has shown that it
stalls.

**What "heard" means: liveness off the core's step.** The holder's
appends come from its authority core, and a core step can take seconds
on an overloaded node (CPU, disk, and the metadata writes of a busy
mount all queue in front of it): its backups used to take that for death
and seal a live holder about 1.5 s in (`stress-ng-fs-nodes`, three nodes
of 44 stress-ng stressors each). So, besides the appends, the holder's
driver sends each listed backup (and a candidate being brought up) a
`HolderAlive` heartbeat every `CONSTELLATION_BACKUP_HEARTBEAT_MS`, off
the core's step (and off the daemon's shared runtime: see *Silence
under host load* below). It stops when the lease expires, when the driver
stops, and when the driver loop has made no progress for
`CONSTELLATION_HOLDER_STALL_MS` (15 s; a core step, or anything else the
driver does between waits for work): that long is a hung authority, and
its backups then seal it a takeover window later. A dead process or a
cut link stops it at once, so a crash still fails over in about 1.5 s.
A backup counts a heartbeat like an append, from the moment it
**arrived**, not from when its own core got to it: the P2P bridge
records the latest arrival of each holder's heartbeats and appends (per
holder and epoch, off the core's step), and the driver hands those
arrivals to the core immediately before it evaluates the seal watch. A
backup whose own steps take seconds therefore never reads its backlog as
the holder's silence (one sealed a live holder at `silent_ms=21872`
while the holder was sending to it). Heartbeats and lock and delegation
renewals (and the answers to a delegate's and a lock holder's own
renewals) also reach the driver on a lane of their own, served before
everything else, the driver's internal channel included. The timers
that keep a grant this node holds come next, ahead of the internal
channel: a delegate's renewal and lapse, and a lock holder's renewal
tick. A backlog of log-stream frames therefore never lapses a
delegation or a lock grant. A renewal's timeout stays on the internal
channel, so an answer queued when it fires is taken first. An owner's expiry of a grant it handed out (a
lock grant, a delegation) is handled after the renewals queued there
when it fired. The other timers that measure a peer's silence (request
and renewal timeouts, grant expiries, the log-stream watchdog) are still
handled after the ordinary peer requests that were queued before they
fired, for at most 500 ms of them, after which the timer goes back once
behind whatever else is due. A non-holder's log-stream frames stay on
the driver's internal channel in arrival order with replies and the
other timers;
the frames it has not applied yet are bounded by the holder's
per-subscriber budget (32 MiB), past which its subscription stops
reading and the holder drops it to S3 tailing, as it would anyway. The heartbeat goes out every interval whatever earlier
beats are still waiting for their answers (each waits up to 5 s; at most
as many in flight per peer as fit in that wait), so one slow answer
never silences it past the 1.5 s takeover window.

**Silence under host load.** A busy host can hold up a daemon's shared
runtime for seconds — every worker inside a blocking store call, or
simply not scheduled — without stopping the process.
`snapsched-write-overhead` sealed a live holder `silent_ms=1501` in that
way: the holder had logged nothing for 2.5 s, no slow step, no stuck
driver, because its heartbeat was a task of that runtime, and so were
the timer that sent it and the QUIC connection's driver that carried it.
Three things make such lag cost latency only:

- *The holder's heartbeat runs on a thread of its own*, with a runtime of
  its own, and sends on a P2P connection of its own (the liveness lane:
  dialed, and so driven, by that runtime), so it leaves the process
  whatever the shared runtime is doing. It still stops for a hung
  authority: when the driver loop has not woken for
  `CONSTELLATION_HOLDER_STALL_MS` since the heartbeat sent it an echo on
  its prompt lane (any wake-up answers it, so an echo queued behind a
  busy lane does not stop a working driver; a step stuck that long, a
  frozen runtime, or one starved to a slice every few seconds that
  serves nobody all fail it), when one wake-up of the loop has lasted
  that long, or when the shared runtime has run nothing for that long
  (its lag probe, below, has not ticked). Its backups then seal it a
  window later — up to four, if its beats had lately come late. What
  can hold one core step that long: a metadata write waiting for the
  store's writer lock behind a busy mount's writers, the journal's
  fsync on a stalled disk, and, under cluster locks, the wait before a
  lock position is reported for a fenced write that passed its token
  check under a grant that has since ended and is still committing
  (`Meta::settle_tagged_writes`; it holds the writer lock until it
  commits). Each is latency up to the bound; a step held past it is a
  hung authority, and its tenure is sealed, as it was before the
  heartbeat left the runtime. `status` shows how close a node's steps
  come to the bound: `ack.longest_step_ms` (and `longest_step_kind`, its
  event) since the mount, `ack.recent_longest_step_ms` over the last
  minute or two. A backup
  whose appends fail at the transport while its heartbeat answers come
  on the liveness lane is unreachable on the data lane: it is removed at
  the ordinary acknowledgement timeout, not kept the slow-backup grace.
- *A watcher does not count silence across its own lapses.* A task of
  the shared runtime (the lag probe) wakes every 50 ms and notes how
  late it woke; a lapse of at least an eighth of the takeover window
  (187 ms) is the node's own, and the driver tells the core before it
  handles anything else (a probe that is itself overdue counts as a
  lapse in progress). Beats that reached the node during a lapse are
  stamped only when its runtime gets to them — after the seal watch's
  timer, often — so silence counts from the lapse's end; but never from
  later than one window plus the lapses' own length after the holder
  was last heard, so a node that lags again and again still acts on a
  dead holder. Before the holder's first beat or append (or a read of the
  lease object) has told a backup its lease's expiry, the reach is a flat
  half window, lapses included: its read then comes at most 1.5 windows
  after the holder's last word in wall time, inside the claim grace past
  an expiry it could not know of.
- *A holder whose beats came late lately gets a longer window.* Each beat
  carries its number and the holder's send time. A beat later than its
  schedule — sent late (the holder's process was held up: its host), or
  heard late on the way there (what the watcher's own lapses held up is
  not counted: those are discounted above) — by at least the lag
  threshold lengthens the window by twice its lateness for a minute, up
  to four windows (6 s at the defaults). A beat is credited as heard no
  later than its send time plus the expiry margin, so one held for
  seconds in a retransmit queue does not keep a holder that died
  meanwhile.

Each beat also names the expiry of the lease it beats for, and the
beats stop there; so does every `BackupAppend`, which carries the expiry
of the lease the holder holds when it builds the append (the same source
as the beats'; 0 when it holds none, which a backup takes as unknown).
The backup takes the latest of what its beats and appends named and its
own last read of the lease object: all lower bounds (an epoch's expiry
only moves later), so they end the discount early, never late. The
daemon's peer service notes the expiry a beat names with the beat's
arrival stamp (and the one an append names with the append's), so the
backup knows the latest expiry any of them named even when its core is
seconds behind on the messages themselves. Within the expiry margin of
that expiry the watch counts plain silence again and reads the lease, as
it always did: silence there may be the lease ending (a holder cut from
S3 that could not renew), and a listed backup must then seal and claim
within the 3 s (twice the window) during which nobody else may claim a
`Backup` lease past its expiry — its tail holds what the holder
acknowledged but did not ship. The margin decides when the watch
*reads*, not whether an unexpired lease is *sealed* (next paragraph).

Once that expiry has passed, the watch reads the lease at once, however
recently it heard the holder (it also looks at the expiry itself, not
only a window after the last word). After a lapse of the backup's own
across the expiry, its P2P handlers stamp the holder's backlog of beats
and appends at the lapse's end, and the driver hands those stamps to the
core before the watch's timer: a window counted from them ended past the
claim grace, and another node's claim took the lease over from a log
without the backup's tail. The lease object decides: expired and still
listing the backup, its epoch is sealed (also when the holder was heard
during the read: that is the backlog, the holder stops acting a margin
before its expiry); unexpired and still listing it, the holder's silence
is judged in the backup's attentive time (its own lapses discounted, a
window plus their length at most; the window is the plain T here, without
the late-beat extension, which the margin ends), and a holder heard
within a window of that is given a window again, no later than the
lease's expiry, where the watch reads again and an expired lease is
sealed; moved on, the role ends. So a backup frozen with its holder's
lease at the margin does not seal it: on 2026-10-16 `snapsched` under a
host load of 43, the holder's core was held up 6.7 s (no renewal, its
10 s lease inside the margin) while the backup's runtime lapsed 2.9 s
with the liveness lane to it broken, and the backup, counting plain
silence (`silent_ms=3226 attentive_ms=3226`), sealed a live holder. A
seal is only ever delayed by this (ADR-12), and inside the margin the
worst case moves from at once to at the expiry, at most the margin (1 s)
later and still inside the claim grace. The appends close what used to
remain: with the liveness lane broken for the whole tenure and the
backup never having read the lease during it, no beat named the expiry,
and a backup lapsing across it counted plain silence from its last
credit, past the claim grace (the holder cut from S3 for a TTL and a
lapse of the backup's own at the expiry were all it took). A backup is
listed, and holds a role to watch for, only once an append has reached
it, and the holder sends one at least every heartbeat interval, so for
any tenure that sent it an append the expiry is known to within the
holder's last renewal. What remains is a tenure that sent this node no
append and no beat: it has nothing of the holder's to lose (the holder
acknowledges nothing a listed backup has not held), and a backup that
restarted mid-tenure starts by reading the lease. A backup that knows no
expiry when it reads still reads the lease within 1.5 windows of the
holder's last word, as before; what it reads bounds the seal by the
lease's expiry while the backup keeps lapsing, where it used to seal at
once, so it can take up to the lease's remaining TTL to fail over a dead
holder (still inside the claim grace past that expiry).

So a dead holder's backup reads the lease to seal it **at most five
windows (7.5 s at the defaults) into the silence, counted in the time the
backup itself was running** — the window, the lateness extension (three
windows at most, within a minute of a late beat), and one window more if
the backup lapsed meanwhile; the time it spent held up comes on top,
since it could not act then anyway — and exactly one window (1.5 s, as
before) when neither side has shown lag in the last minute; then the
lease read, the seal's fsync and the takeover follow as before. In wall
time that is about `(T + window) / (1 − f)` for a backup held up a
fraction `f` of the time: twice as long at half time, ten times at 90 %
(such a node is a poor successor anyway). The TTL path (60 s plus the backups' grace) is unaffected. Under
`ack=s3` the same rules apply to the subscribers that watch the holder
(below). None of it is a safety input: the seal and the log-slot CAS
fence at any window, and nothing the holder does — its strict-read
freshness rule, its grants — relies on the window being short.

The heartbeat also tells the holder whether its backups are alive: a
backup answers it with whether its own authority driver is progressing
(waiting for work, or in a wake-up that started within half of
`CONSTELLATION_BACKUP_SLOW_MAX_MS`). Such a backup that is slow to
acknowledge (a loaded node whose core takes seconds over a step) is kept
for up to `CONSTELLATION_BACKUP_SLOW_MAX_MS` (10 s) without
acknowledgement progress, and an append waits that long for its answer.
Before, an append's answer was abandoned after
`CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` (1 s) and the backup dropped for
making no progress: under `stress-ng-fs-nodes` the holder dropped and
re-added candidates every second or two and had no backup most of the
time, so every acknowledgement waited for S3. A backup that does not
answer the heartbeat (dead, cut off), that answers that its driver is
stuck, or that answers appends short (alive but stuck), still goes after
the ack timeout. A candidate that times out is not brought up again for
a while — the reconfiguration rate limit doubled per consecutive timeout
(6 s, 12 s, ... at most a minute) — so another peer goes first and the
holder does not re-stream the journal from zero to the same loaded node
every few seconds; a candidate that makes it into the set starts over,
and so does one that has not timed out for a minute past its last
backoff.

A backup the holder dropped (a candidate that timed out, a committed
backup reconfigured out) used to keep watching and seal the live holder
1.5 s later, harmlessly but for good (a sealed node is never invited back
for the epoch). The holder now keeps telling it for 10 s that it was
dropped (`HolderAlive` with `listed: false`), and the backup drops its
role and tail instead. The dismissal names the **candidacy** it ends:
each bring-up of a backup gets a fresh one (time-based, so a restarted
holder's never repeats), carried in every `BackupAppend`, and a backup
applies a dismissal only to the candidacy it is backing under. A node
dropped and brought up again within the 10 s is therefore never wiped by
the old candidacy's dismissal still in flight — the holder counts the new
candidacy's acknowledgements from zero, and those stand for the tail the
backup holds now. A lost dismissal leaves the old behaviour.

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

**What a seal's successor waits for.** The old holder's *clients* may
still honour what it granted: a strict-read delegation, a lock grant, a
write delegation. A successor that took a lease over by a seal or an
`ack=s3` takeover, and whose predecessor served strict reads or locks
(the lease's `granted_delegations` mark, set before the first answer, or
at once on a takeover that itself waits out such a predecessor's grants:
see below),
therefore acknowledges no mutation, and grants no new lock, until its own
marker time plus the takeover window plus the grant's TTL plus two expiry
margins: the read-delegation TTL (plus the write delegations' reclaim
horizon when some were live) for acknowledgements, the longer of the
read-delegation and lock TTLs for new lock grants (about 8.5 s and
23.5 s at the defaults, plus the reclaim horizon when write delegations
were live). The wait counts only the successor's own elapsed time. It is
never shortened to the old lease's expiry: that is a stamp of the old
holder's clock, and a successor whose clock runs ahead of it by more than
the lease has left would read it as past and wait for nothing while the
old holder's grants were still honoured. Nor does the successor's reading
of that expiry decide whether the takeover was a fast one: a seal or an
`ack=s3` takeover is, whatever its clock says.

A tenure that waits out its predecessor's grants is one its own successor
must wait out, even if it serves nothing first. The claim CAS of a
takeover that arms a floor (a seal or an `ack=s3` takeover of a marked
tenure) therefore writes the new lease already marked, and so does the
claim of a released lease that says its tenure left lock grants behind (a
release marks its lease exactly when the lock table holds a live grant, a
grant moved toward a delegate or a grace; the taker of a released lease
grants no lock for a grace of its own, and the mark of the lease being
released is not carried over). A
second seal inside the first successor's floor, before it had granted
anything itself, found an unmarked lease and armed no floor, and the grants
the first floor protected (those of the tenure before, which the lock
mirror had not carried) were open to conflicting ones. A handoff that
left nothing live behind releases an unmarked lease, however the tenure
before it was marked: a seal of its taker waits for nothing. (What
remains: a mark a tenure inherits stays for its life, so a seal of it
arms a floor even if it never granted anything, and a chain of seals
keeps paying one.)

The cost: a takeover in the last seconds of the old lease waits the whole
bound where it used to wait only until the expiry, and so does a takeover
after the lease ran out of a holder cut from S3 but alive on P2P: its
backup seals at the expiry it reads, which used to be a TTL takeover with
no wait at all. Measured with strict reads and a held lock,
that failover's first acknowledged write moved from the takeover itself to
about 8.5 s after it, and the first new lock grant to about 23.5 s after
it; a non-blocking lock request on a free file waits for the grace
meanwhile, on a thread of the mount's pool, and is granted at its end
(`EAGAIN` only after 112.5 s with the defaults, `EINTR` on a signal; with
a holder cut from S3 the wait begins at the stale owner and lasts 37 to
61 s at the production lease; see [Cluster
locks](cluster-locks.md#waiting-for-a-grace)); a request that a
ring-transport mount has no queue entry to wait on is answered at once,
as before.
Only a tenure that served strict reads or locks costs its successor
anything. Shortening the wait from the successor's own observation of the
old holder's last renewal is planned (plan 43, C7-F).

The successor keeps the lease through its takeover gate and until both
waits have passed: it does not idle-release it to a node that wants it,
nor serve a peer's handoff request or a continuation epoch's flush
release. An operator's flush (`leave`, a suspension) and a shutdown
(unmount, stop, the in-place upgrade's handover) are parked *before they
start*: nothing is uploaded, shipped or published for them until the gate
and the floor are over (the rounds still ship, and the node serves as
before), then the flush runs and releases. So a `leave` or an unmount
right after a seal takes up to the floor's remainder longer (23.5 s, or
29.5 s with write delegations, at the defaults; 8.5 s and 14.5 s with
locks off). The in-place upgrade waits for it *before it detaches the
views*, which keep serving meanwhile: only the drain that follows stalls
the applications, as without a seal; and it waits out a floor in full,
because a new image would re-adopt the lease without it (the floor is in
memory only, plan 43 Appendix B row 14). The floor is never given up on.
A takeover gate that stays pending is: after a fixed 40 s cap (the gate
only) a shutdown or `stop` ends without releasing, and the lease lapses by
its TTL, which outlasts the floor, so the next holder takes it over by TTL
or seal; a `leave` or a suspension reports that the release did not happen
and the node, which keeps renewing, releases the lease at an idle round
once the gate is over. Releasing inside the floor would hand the lease to
a node that starts without it (a released lease is no fast takeover), and
a lease released inside the gate left the log naming the releaser root of
delegations its delegates could no longer use.

`ack_floor_waits` counts the waits, and the log line "took a lease over by
seal or ack=s3 from a tenure that served strict reads or locks" carries
the wait in `wait_ms`.

A node that is not listed as a backup must wait 3 s (twice the takeover
time) past a `Backup` lease's expiry before it may claim it, so a listed
backup gets there first. A new tenure starts with no backups and picks
them again. A write-back file whose chunks never reached S3 before the
holder died is held (see [Write-path hygiene](write-path-hygiene.md)),
not published naming missing chunks.

Without a listed backup (none in budget, or the holder dropped it
before it died), the dead holder is replaced by a TTL takeover, which
the lease path starts. A node with an op the holder (or a delegate
whose stream the holder appends) already accepted, and that waits
longer than `CONSTELLATION_INBOX_P2P_GRACE_MS` (3 s) for the log to
carry it, reads the lease from the sync round and takes it over once it
is claimable ("ops wait for a root whose lease ran out: taking the root
over"). A lease it last saw live is not read again before the expiry it
showed, so a slow but live holder costs about one read per renewal, and
a suspended node reads nothing. A live holder's lease is never
claimable, and nothing is asked of a live holder: the takeover neither
registers in `wanted_by` nor requests a handoff, even when its own read
finds the holder renewed. Such an op used to wait for its client
deadline (2 × TTL) and end in doubt first. A delegate's own writes take
the lease path once its unrenewed grant lapses (see
[Delegations](delegations.md#failures)).

A holder that is deposed (paused or partitioned past its lease while a
backup or another node took over) answers no application mutation with
`EIO` for that: a mutation issued on it before its recovery has run is
forwarded to the new holder after the recovery, behind the node's own
stranded writes, never refused (see
[Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)).

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
S3 durability but fails over at the TTL. The stream's frames, its
heartbeat included, come from the holder's core: a single 1.5 s step (a
busy host) used to silence it and hand the lease to the first
subscriber that looked. The holder's off-core heartbeat (see *Silence
under host load* above) now also goes to its subscribers, a beat counts
like a frame on a live subscription, and silence is measured as for the
seal watch (not across the watcher's own lapses, a longer window after
late beats); without a live subscription the link decides, as before.

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

- `--fsync-mode local` (default): `fsync()` (and `fdatasync()`) uploads
  every chunk of the file still queued on this node — whoever wrote it,
  through whichever descriptor, including writers that already closed it
  under `--write-mode back` — forces the node's metadata store to disk and
  nudges the shipper. Concurrent `fsync`s share the store's journal sync:
  each waits for one whose flush of the journal came after the call (at
  most the one in flight and the next), so the journal sees one sync for
  many callers and commits do not queue behind each of them (finding 14).
  The file's data is then in the bucket; its latest
  manifest is as durable as the ack policy makes this node's journal.
  `--fsync-mode s3`: `fsync()` also waits until the journal up to the call
  (the file's records) is in the bucket. It is the per-call form of
  Layer C.
- Before plan 39b the two modes were asymmetric: under `local`, an
  `fsync` only uploaded the chunks of writes it published itself, so
  `open` + `fsync` of a file closed under `back` (a PostgreSQL
  checkpointer's pattern) returned with the chunks only on this node's
  disk. Linux's `fsync(2)` covers "all modified in-core data of the
  file"; now both modes do.
- **While it waits** (plan 39), `fsync()` behaves like an NFS `hard`
  mount in every mode: see [fsync during an S3 outage](#fsync-during-an-s3-outage).
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

### fsync during an S3 outage

Plan 39. What an `fsync` waits for is the mode's (above); how it waits is
the same in every mode:

- **Transient failures are waited out.** A timeout, a refused or reset
  connection, a DNS failure, a 5xx, a 429/`SlowDown`, a lost conditional
  write, an expired token the credential provider will renew: the attempt
  (the inode's chunk drain with its peer handoff, the journal barrier, a
  forwarded commit left in doubt) is retried after a backoff of 100 ms
  doubling to 5 s, with jitter, until it succeeds. No `EIO` for a blip or
  an outage, however long — the NFS `hard` contract. `fio --end_fsync=1`
  under 800 ms S3 cuts (`fio-blips`) no longer sees `EIO`.
- **Permanent failures are `EIO` at once**: `AccessDenied`, `NoSuchBucket`,
  `InvalidRequest`/`InvalidArgument`, a disabled or deleted KMS key, a
  pending chunk this node lost. The data stays pending and every later
  `fsync` of the file tries again, returning `EIO` while the condition
  lasts — never a false success.
- **Killable, not interruptible** — again NFS `hard`. A signal the process
  handles or ignores (a timer, `SIGINT` with a handler, `SIGTERM` caught
  for a clean shutdown) leaves the `fsync` waiting: no `EINTR`, which
  applications that treat any `fsync` failure as fatal would act on
  without any outage. A signal that kills the process (`SIGKILL`, or an
  unhandled `SIGTERM`/`SIGINT`) ends the wait within about 100 ms and the
  process dies; the data stays pending and keeps uploading in the
  background. The same holds for an `O_SYNC`/`O_DSYNC` write, which
  answers `EIO` rather than `EINTR` when its wait is cut short.
- **Opt-in bound**: `mount --fsync-timeout 30s` (`CONSTELLATION_FSYNC_TIMEOUT`)
  ends a wait with `EIO` after that long, data pending — nfs(5)'s `soft`,
  with its warning: only when responsiveness matters more than integrity to
  the application. A host with the kernel's FUSE request timeout set
  (`fs.fuse.default_request_timeout`/`max_request_timeout`) caps every wait
  just below it, because the kernel would otherwise abort the connection.
- **Never a false success.** No failure drops data; every `fsync` drains
  whatever of the file is still queued on this node, so the one after a
  failed `fsync` (or a failed close, or an `O_SYNC` write that failed)
  waits for it again, in both modes. A queued chunk that is gone from the
  node's disk cache and not in the bucket either (a torn or replaced
  disk) cannot be uploaded by anyone: the `fsync` fails `EIO`, and so does
  every later `fsync` of the file until it is rewritten or removed.
- **Chunks another node still has to upload.** On the node that
  sequenced a `back` close made elsewhere (the lease holder), the file's
  chunks the writer had not uploaded yet are part of the file as that
  node sees it: an `fsync` there waits until they are in the bucket (the
  writer's report, or the node's own S3 check), with the same hard-mount
  policy. `CONSTELLATION_REMOTE_CHUNK_WAIT_S` does not end that wait with
  a success; `--fsync-timeout` ends it with `EIO`. A node that neither
  wrote the file nor sequenced the close does not see the writer's queue
  and does not wait for it.
- **An unlinked file that is still open**: an `fsync` of it uploads its
  queued chunks like any other file's (a deleted file's data is still the
  file's until its last descriptor closes); garbage collection reclaims
  them afterwards.
- **Inside a continuation epoch** (S3 away; a single node enters one within
  about a second) an `fsync` still waits for the file's chunks to reach the
  bucket — through a peer that can reach S3 (chunk handoff, after
  `CONSTELLATION_CHUNK_HANDOFF_AFTER_MS`), or once S3 returns — in both
  modes (plan 39b). The epoch lets `close()` and every other mutation go
  on without S3, its writes as durable as their nodes; an `fsync` asks for
  more than that. Until plan 39b a `--fsync-mode local` `fsync` returned
  there with the chunks only on this node.
- **`fsync` of a directory** (`fsyncdir`) is the same barrier for its
  entries: their mutations committed, the local store synced, under
  `--fsync-mode s3` the journal shipped. A directory has no chunks, so
  there is nothing to upload; it does not upload its files' chunks either
  (`fsync` each file for that, as on Linux). Before plan 39 it was a
  silent no-op.

Visible as `status.fsync` (`waiting`, `longest_wait_ms`, `retries`,
`timeouts`, `permanent_errors`, `interrupted`, …) and `constellation_fsync_*`
on `/metrics`; a wait past 10 s logs "fsync: S3 unreachable, still trying"
once per inode, and "done waiting" when it ends.

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
  `through` would (upload, then commit). One after the `close()` — from
  any descriptor or process on the writing node — uploads the chunks the
  close left queued before it returns, in both `--fsync-mode`s (plan
  39b), and tells the sequencer they are up, so a reader there does not
  wait for them even if the writer dies right after. An `fsync` on the
  sequencer waits for the writer's queue (see [fsync during an S3
  outage](#fsync-during-an-s3-outage)); on any other node it does not
  see it: the queue belongs to the writing node until it drains.
- **Metered networks** (`CONSTELLATION_PROFILE_UPLOADS=unmetered-only`).
  The upload queue waits for an unmetered network, but an op that cannot
  finish without its node's chunks does not. When a node that is not the
  sequencer forwards a `back` close, the sequencer ships the close's
  records only once that node's chunks are up, and with them every later
  op that depends on the close (a `chmod`, `rename` or `unlink` of the
  same file: the ship plan defers it with the close). Its answer to any
  forwarded op names the files whose chunks on the node the op waits for,
  and says whether anything else brings the op's records back to the node
  first. A backed sequencer's pre-S3 stream does, so the chunks stay held
  — unless the stream stops before the op at a chunk the node does not
  have (the sequencer's own write-back, another node's forwarded close):
  then only the log brings it, and it is answered as below. When nothing
  does (no backup: no peer within the RTT budget, the first seconds of a
  tenure, `CONSTELLATION_BACKUPS=0`; or `ack=s3`, where the
  acknowledgement itself waits for the records to land; or a delegate)
  and the node must wait for the records (the answer came on state it
  has not applied yet, or the acknowledgement waits), it uploads those
  files' chunks at once. If the log also waits for another node's chunks,
  the op waits for that node's uploads too. An op still waiting
  `CONSTELLATION_OWN_RECORD_WAIT_MS` (10 s) after the sequencer said its
  stream would carry the records uploads the chunks anyway (the stream
  stalled: its backup was lost, say). Before, such an op waited out its
  120 s deadline and failed in doubt.

  The same holds for what the node *reads* after an op. A close answered
  at once keeps its chunks held, and its records wait on the sequencer.
  A later op whose answer the node has to catch up with (a refusal, or
  an op that completes through the log) makes every later read wait
  until the node's log reaches the sequencer's state at that op, which
  includes the held close. The node does not wait for, or upload, the
  rows of its own ops that its replica already carries: the answer names
  which of the sequencer's unshipped transactions through that state are
  the node's own, and the node excuses those whose effects it has (the
  close it installed when it was answered; a refusal, which changes
  nothing). Its reads then wait only for everything else through that
  state: another node's op that ships on its own arrives with the log;
  one that the sequencer holds back behind this node's chunks (another
  node's `chmod` of the held file) is named in the answer, and the node
  uploads those chunks, again only when nothing else would bring those
  records to it first. So does an op of its own it does not have yet
  (the op itself, waiting for the log; an earlier incarnation's). Before,
  such reads waited out `CONSTELLATION_SESSION_WAIT_MS` again and again
  until the wait was abandoned (4–10 s `open`, `rename` and `chmod`
  calls on AWS); and for a while after that fix every such answer made
  the node upload the held close, which defeated the hold on a metered
  network. A stranded op the node replays after a takeover uploads its
  chunks at once even when the sequencer says its stream carries the
  transaction: the stream may have carried it before the node's recovery
  rolled it back, and it does not resend.
- **Unmounting.** The last view's unmount drains the queue: the daemon
  uploads every pending chunk and ships the journal (then publishes a
  last commit, which the next lease holder does instead should it stall)
  before it exits. `constellation umount` waits for that by
  default and reports its progress; when it returns successfully the
  data is in the bucket and the daemon has exited, so nothing that
  happens to the node afterwards (a `kill -9`, a reboot, losing its
  disk) loses it. A drain that cannot finish (the bucket unreachable
  until `CONSTELLATION_SHUTDOWN_STALL_S`, a lost queued chunk) makes
  `umount` fail, naming what stays queued on the node for its next
  mount. `umount --no-wait`, or a Ctrl-C while it waits, leaves the
  drain running in the background with the durability described above
  until it is done; a `mount` issued meanwhile waits for the drain to
  end and then serves the rest of the queue. A large backlog (gigabytes
  queued on a host whose uploads fell behind) takes as long to drain as
  to upload. See [what `umount` waits
  for](named-filesystems.md#what-umount-waits-for).
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
- Retiring a node gives up its unshipped journal: writes it acknowledged
  as holder or as an epoch's hold owner that never reached S3 (or the
  successor's backup tail) are lost, as they would be if its disk were
  gone. Other nodes drop what they had streamed of that journal
  (`replays_of_retired_dropped`) instead of replaying it through the
  successor, so no fragment of it surfaces. This covers ops the retired
  node sequenced for others too; ops it forwarded to a live holder are
  that holder's and are kept.
- An epoch carries a lease only if the holder's claim was usable
  (outside the lease margin) when it acked. With the default half-TTL
  renewal, an outage that starts just before a renewal leaves the holder
  about `TTL/2 − margin` for a formation; short TTLs can miss it.
- An epoch whose claim resolution carried no lease has no hold owner, so
  no write can execute under it: its members refuse writes with `EROFS`
  (as a frozen epoch does) rather than letting them time out as `EIO`.
  That includes a write submitted before the epoch activated (or froze)
  that is still waiting for the lease (reading the lease object,
  acquiring, or backing off between attempts) and was never sent to a
  holder: it is refused `EROFS` at the activation, where it used to wait
  for the 2 × TTL (120 s) deadline and fail `EIO`. A write that was sent
  to a holder is answered as at a freeze (below). (The 20 s bound for a
  node without S3 does not apply in an epoch, and is not needed there:
  nothing waits for the lease in it.)
  Once S3 is back, any member closes it, frozen or not (nothing was
  written under it), and the lease is decided by CAS again.
- An open epoch's members probe S3 at the sync interval
  (`CONSTELLATION_SYNC_INTERVAL_MS`, 500 ms) at first, backing off to
  four times it (2 s) while nothing changes, and return to the sync
  interval as soon as an S3 request succeeds after a failure (S3 is
  back). A member with nothing to ship (an epoch carrying no lease, or
  one carrying another member's) used to go idle and probe only every
  `CONSTELLATION_SYNC_IDLE_MAX_MS` (10 s), so it kept refusing writes
  with `EROFS` up to 10 s after S3 returned; now that ends within 2 s
  and the close's few S3 requests. A frozen epoch that stays open with
  S3 reachable (a dead member waiting for `leave`) costs each member
  one probe round every 2 s: about 30 tail reads a minute, and as many
  lease reads on a member of an epoch carrying another member's lease.
- A node that closed an epoch while holding a lease that still stands,
  and has not re-claimed it yet, claims that lease for a new epoch as
  if it held it (until the lease would expire, less the margin). The
  close lets the lease go locally; claiming nothing, an epoch formed in
  that window (an S3 cut starting just after the last one ended, often
  while the re-claim's CAS was in flight) carried no lease and refused
  every write with `EROFS` on the node whose lease had been standing
  all along (`stress-ng-fs-faults`). An epoch carrying it makes the node
  hold the lease again, as before the close, with its epoch hold: writes
  and locks go on, and the next close re-claims it. If the re-claim's
  CAS was answered in doubt (S3 cut while it was in flight), the node
  claims the object that CAS may have written (same lease epoch, the
  later expiry), usable only while the earlier object is. If a CAS of
  its own is still out, or one in doubt wrote another lease epoch, the
  claim is carried only by an epoch of that node alone.
- If S3 is cut again after the hold owner closed but before its
  re-claim lands, the other members are still in the last epoch (they
  wait for the carried lease to move, below), and a member of an open
  epoch joins no other. So a member accepts one proposal in place of
  its open epoch: from that epoch's carrier, a member of it that has
  closed it (it proposes only when it is in no open epoch), once the
  epoch was activated here, and only if this member does not own the
  epoch's hold itself. It goes from one epoch to the next without
  closing, so it promises and takes over nothing in between. If that
  proposal is aborted (another member refused it), the member closes
  although the carried lease has not moved: no hold of the old epoch
  remains (its carrier closed it), and the carrier's claim on the lease
  its close let go ends at that lease's expiry, so the member is like
  any closed node. The new
  epoch resolves its claims as any other does, and the closed carrier's
  claim is the lease its close let go. Before, no epoch could form until
  the re-claim, and with a 6 s lease the lease and its lock grants could
  lapse inside two back-to-back cuts (fenced I/O).
- A lease being released is a different state from one an epoch's close
  let go. The close sends no release: the lease stays in S3 as this
  node's, and nobody can take it without a CAS on that object or the
  promises of the members, so an epoch may carry it (above). A release
  CAS, once it lands, makes the lease anyone's without promises, so no
  hold stands on a lease being released or released. A node still
  claims a lease it is releasing (the release may fail, and an epoch
  carrying nothing refuses every write), but an activation carrying a
  lease it is releasing, or one it has released or given up with the
  release in doubt since (it acked before the release began: a paused
  node handles its events late), adopts no hold. The node owes the
  epoch's close instead, and that obligation is persisted. Once S3 is
  back it closes the epoch, and re-claims the lease if the release did
  not land. If the release ends with the lease kept (a failed handoff
  flush, a re-read that finds the lease standing), it holds the epoch
  after all and the obligation is dropped. If it gives the lease up in
  doubt while holding the epoch, the hold goes, the move is owed, and
  writes are refused (`EROFS`) until the epoch closes: nobody holds it.
  The lease a release CAS is about to replace is persisted before the
  CAS goes out (`local["lease_released"]`), so a restart after the CAS
  landed owes the move too, rather than holding an epoch on a lease
  anyone may have claimed; the record is cleared when the release ends
  with the lease kept or the node acquires a lease.
- The close keeps the lease's [cluster lock](cluster-locks.md#failover)
  grants. The re-claim's CAS replacing the lease the close let go (or
  the object a re-claim CAS in doubt may have written over it) proves
  that nobody held it in between, so they stand. Any other acquisition
  drops them, and grants nothing over them until they lapse. After the
  flush, a lease with live grants is kept rather than released (it is
  released as an idle lease is, once they are gone). A node re-adopting
  its own lease keeps the lease's granting mark, so a re-claim that S3
  cuts again before it could mark the lease does not refuse every lock.
- A member of an epoch that carries a lease closes it once S3 is back
  and the lease object has moved past the carried lease (the hold
  owner's flush re-claimed it), frozen or not: a missing member (paused,
  dead) does not keep the others in the epoch after the owner's flush.
  An earlier object of the same holder and lease epoch has not moved
  past it: the carrier claimed what its re-claim CAS in doubt may have
  written, and that CAS did not land. The
  owner ends its log streams at the close; a member follows it again as
  soon as the flush's first segment is announced (gossip) or tailed.
- A holder whose carried lease reaches it after the lease margin (the
  activation took a moment) still adopts the hold: members promise
  nothing while the epoch is open, so no taker can act on the expiry.
  The same holds for the lease an epoch's close let go (an activation
  that reaches a paused node late).
- A member that can still reach S3 declines to join an epoch. A node
  whose own S3 fails asks every live member to probe S3 (`PingS3`,
  answered by a lease GET bounded at 300 ms: `Pong.s3_ok`); if any
  reaches it, the node does not propose (its outage is its own, not the
  bucket's: `status.epoch.own_s3_outage`) and asks again 1 s later,
  until its own S3 works again. Its closes meanwhile hand their chunks
  to a peer that reaches S3 (`CONSTELLATION_CHUNK_HANDOFF_AFTER_MS`,
  6 s after the drain's first request went unanswered).
  The probe is fresh each time, so a cluster-wide outage is not
  mistaken for a local one.
- A node whose own S3 path stalls (no S3 request of it completes for
  `CONSTELLATION_S3_STALL_MS`, 6 s, while one of them waits that long or
  failed unanswered; `status.own_s3.stalled`) keeps its
  metadata ops on P2P (EC2 campaign 8 A-1). A forward that times out or
  hears `Busy` is retried to the holder, backing off to at most 1 s,
  instead of ending in the lease path, whose every step (the lease
  read, the CAS, the inbox) is an S3 request that would wait out the
  S3 retry budget (30 s) and fail. An op already waiting on the lease
  path when the stall is detected is forwarded again. A client op not
  answered `CONSTELLATION_S3_LESS_OP_DEADLINE_MS` (20 s) after it was
  submitted fails with `EIO` (in doubt; retried by the same rid it is
  deduplicated), where it used to wait for the 2 × TTL (120 s)
  deadline. That bound is what a node with neither S3 nor a working
  P2P path to the holder answers within. It is not applied while
  the live peers answer (`PingS3`, asked every 5 s during the stall)
  that none of them reaches S3 either: a bucket outage is the
  continuation epoch's. A lone node has no peer to ask
  (`peers_reach_s3` stays `null`): the deadline applies to it (with
  forwarding on, the default, and no continuation epoch open), and
  nothing is forwarded (there is no holder but itself); a bucket outage
  ends that when the node's epoch of one forms. The test needs a
  request left waiting: silence alone is not a stall. A daemon that
  stalled itself (frozen, or its authority core waiting 18 s on a
  metadata-store write, as in the xcheckfs run of 2026-10-13) asked
  nothing and used to read as stalled for a moment when it ran again.
  The watch also counts its once-a-second ticks: S3 must have left a
  request waiting through as many consecutive ticks as the window has
  seconds, and a stall of the daemon itself (its runtime held up, the
  answers that had arrived unread) is one late tick however long it
  lasted (`status.own_s3.late_ticks`); a daemon whose ticks are all late
  still reports a real stall, later. A stall already seen lasts until S3
  answers or a chunk PUT lands. `status.s3.outstanding` and `oldest_outstanding_ms` show the
  requests waiting. On a holder whose lease is still valid nothing waits
  on the deadline (its ops execute
  locally); that run's `own_s3.deadlines` stayed 0.
- A node restarted after a crash with nothing of its own to ship does
  not reacquire the lease its previous incarnation held. If no backup
  seals that lease, it stays in force until it expires, and a peer's
  forward used to be answered `NotHolder`. The requester then took
  the lease path, which cannot run without S3. Now a forward that
  reaches a node the lease object still names (unreleased) makes that
  node re-adopt its own lease (a takeover of it, through the gate). The
  requester is answered `Held`, through the gate too, and retries
  meanwhile (`status.own_s3.readopted_for_forward`). Every node reads
  the lease when it starts, so a restarted one knows before the first
  forward arrives. A lease this node released (a handoff in flight) or
  that names another node is never claimed this way, and a re-adoption
  that does not land is not tried again for a TTL.
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
  freezes (or activates without a lease): it may have taken effect. It
  gets the log's answer if this node has applied one, else `EIO` (in
  doubt, retried by the same request id). Neither is a stranded op's
  replay: it stays queued and is resent once the epoch is over.
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
- `syncfs(2)`, `sync -f` and `sync(2)` make nothing durable on a
  constellation mount; only `close()` and `fsync()` do (as described
  above, per `--write-mode` and `--fsync-mode`). Linux sends
  `FUSE_SYNCFS` only where `fc->sync_fs` is set (mainline
  `fs/fuse/inode.c`, `fuse_sync_fs`): on `fuseblk` connections since 6.18
  (`d3906d8f3cee`, "fuse: enable FUSE_SYNCFS for all fuseblk servers")
  and on virtiofs since 5.14 (`2d82ab251ef0`, "virtiofs: propagate
  sync() to file server"), never on a plain `fuse` mount, so that an
  unprivileged server cannot stall the host's `sync`. Constellation
  mounts plain `fuse`: the calls write the kernel's dirty pages to the
  daemon (which stages them like any write) and return 0 without asking
  it anything. Under `--write-mode back` that leaves data written but not
  closed or `fsync`ed where it was — an open file's writes only in this
  node's staging (lost if the daemon dies before the close), a closed
  file's queued chunks only on this node's disk (lost with the node until
  the upload drains). An application or script that relies on `sync` to
  make such data durable must `fsync` the files (or close them under
  `--write-mode through`) instead.
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
| `--fsync-timeout DUR`, `CONSTELLATION_FSYNC_TIMEOUT` | unset (wait until durable) | the opt-in `soft` bound on an `fsync`'s wait for an unreachable S3 (see [fsync during an S3 outage](#fsync-during-an-s3-outage)) |
| `--write-mode through\|back` | `through` | what `close()` waits for (see [When to use `--write-mode back`](#when-to-use---write-mode-back)) |
| `CONSTELLATION_REMOTE_CHUNK_WAIT_S` | `60` | how long the sequencer's readers and must-finish passes wait for a non-owner's `back` chunks |
| `CONSTELLATION_ACK` | unset | default for `fs create --ack-policy` (a mount only warns when it disagrees with the filesystem) |
| `CONSTELLATION_BACKUP_RTT_BUDGET_MS` | `5` | backup candidates' RTT limit; `0` disables backups |
| `CONSTELLATION_BACKUPS` | `1` | most backups; `0` disables backups |
| `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` | `1000` | a backup without progress this long is removed (one known alive: after the slow bound) |
| `CONSTELLATION_BACKUP_SLOW_MAX_MS` | `10000` | a backup known alive without progress this long is removed anyway |
| `CONSTELLATION_BACKUP_TAKEOVER_MS` | `1500` | holder silence before a seal (or an `ack=s3` takeover) |
| `CONSTELLATION_BACKUP_HEARTBEAT_MS` | `300` | heartbeat append to an idle backup; the off-core liveness heartbeat's interval |
| `CONSTELLATION_HOLDER_STALL_MS` | `15000` | a driver that has not woken this long since the heartbeat's echo, or is stuck in one wake-up this long, or a runtime that has run nothing this long, stops the off-core liveness heartbeat (a hung authority is sealed) |
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
  `s3_fast_takeovers` (triggers, not wins), `dead_root_acquires` (TTL
  takeovers started for ops waiting on a dead root), `ack_floor_waits`,
  `stale_liveness_refusals`, `epoch_carry_refused`,
  `refusals_journaled`, `reads_durability_blocked`;
- `epoch`: `epoch_slack`, `carrier`, `promise_until_ms`,
  `promise_puts`, `promise_requests_answered`,
  `promise_requests_refused`, `promise_checks`,
  `takeovers_refused_promises`, `promise_flush_exempt`, `stale_claims`,
  `streamed_ahead` (hold owner), `streamed_installed`,
  `forwards_streamed` (member), `handoffs_behind`;
- `fsync` (plan 39): `mode` (`hard`/`soft`), `timeout_ms`,
  `kernel_cap_ms`, `waiting`, `longest_wait_ms`, `max_wait_ms`, `waited`,
  `retries`, `timeouts`, `permanent_errors`, `interrupted`;
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

### An application hangs in `fsync` during a bucket outage

Expected (plan 39): `fsync` waits until the data is durable, like an NFS
`hard` mount. `status.fsync.waiting` and `longest_wait_ms` show it, and the
log says "fsync: S3 unreachable, still trying" with the last error. It
returns once S3 is back. A handled signal (`Ctrl-C` in a program that
catches it, a timer) does not end it; killing the process (`kill -9`, or a
signal the process does not handle) does, within about 100 ms.
If the application must not block that long, mount with `--fsync-timeout`
and make sure it treats `EIO` from `fsync` as "not yet durable", not as
"lost". An `fsync` that fails at once with `EIO` and
`status.fsync.permanent_errors` rising is a failure waiting cannot fix: the
log names it (`AccessDenied`, `NoSuchBucket`, a KMS key, …).

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
