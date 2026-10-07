# Delegations

Delegated sub-sequencers (plan 30 M11–M12). The root lease holder can
hand a subtree, or one name-hash range of a busy directory, to another
node. That node then sequences every mutation there at local speed and
streams the results to the root, which appends them to the one log.
There is still one lease in S3 and one log; delegations are a table
inside the root's authority, maintained over P2P.

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [The delegation table](#the-delegation-table)
  - [Ownership](#ownership)
  - [How a delegate executes](#how-a-delegate-executes)
  - [The append path and dependencies](#the-append-path-and-dependencies)
  - [Grants, renewal and expiry](#grants-renewal-and-expiry)
  - [Cross-subtree operations and recall](#cross-subtree-operations-and-recall)
  - [Automatic placement](#automatic-placement)
  - [Hot shared directories](#hot-shared-directories)
  - [Offline designations](#offline-designations)
  - [Failures](#failures)
  - [What stays with the root](#what-stays-with-the-root)
- [Commands](#commands)
- [Configuration](#configuration)
- [Status](#status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Root**: the node holding the root lease in S3. It is the only node
  that appends to the log.
- **Delegate**: a node that sequences one subtree (or one hash range of
  a directory) for the root.
- **Generation (`gen`)**: one delegation's lifetime, numbered globally.
  A recall ends a generation; a re-delegation starts a new one.
- **Owner**: the node that sequences a key: the delegate whose
  delegation contains it, else the root.
- **Cross-subtree op**: an operation whose keys have two owners, such
  as a rename between two delegated subtrees.
- **Recall**: the root taking a delegation back. The delegate drains its
  stream and stops, or the root waits out its grant.
- **`deps`**: the delegate stream positions a requester has observed,
  carried with its forwarded ops so the root never appends an effect
  before its causes.
- **Placement**: the root's automatic choice of what to delegate to
  whom.

## Details

### The delegation table

`Delegate { dir, node, gen }` and `Recall { dir, gen }` log records
maintain a replicated delegation table. It is stored in the namespace
(so a bootstrapped replica learns it from the head commit), and every
node resolves ownership against its own copy.

- Delegations never overlap: a directory under a delegation, or one
  with a delegated descendant, cannot be delegated. There is no
  sub-delegation.
- A node holds at most one whole-subtree delegation (plus designations).
- The root directory itself is never delegated.
- No delegation is made inside a continuation epoch. When an epoch
  opens, the root recalls every live generation; delegation resumes
  after the epoch closes.
- Delegation needs P2P. With P2P off, or `CONSTELLATION_DELEGATION=off`,
  no `Delegate` record is ever written.

### Ownership

- A dentry `(parent, name)` belongs to the innermost delegation that
  contains `parent`.
- An inode's own keys (attributes, xattrs, reverse links) belong to the
  delegation that contains its **primary link's** parent.
- An operation whose keys resolve to two owners is cross-subtree. An
  exclusive hold on a directory resolves to the directory itself, so an
  `rmdir` or rename of a delegated root is cross-subtree too.
- Ownership is a local ancestor walk; there is no lookup service.

A forward goes to the owner. A node with a stale table that sends an op
to the wrong node gets `NotHolder` with the right owner, and follows up
to two redirects (a delegate names the root, the root names a delegate).

Routing uses the links the authority core sees. A node with no link to
the delegate sends the op to the root. The root redirects to the
delegate only while its own link to it is up; otherwise it recalls the
generation (see [Cross-subtree operations and recall](#cross-subtree-operations-and-recall)).
The driver sends the core its links every second, and again as soon as
a peer enrols or leaves or a link goes up or down. A newly enrolled peer
is probed at once, so its link shows within a round trip. Before, a
peer could be invisible to the core for up to a second after it
enrolled, and a write into a delegation made to it in that second was
recalled (`startup-link-lag`). `p2p.peers[].core_connected` in
`constellation status` shows the core's view of each link.

### How a delegate executes

A delegate validates each op against its own replica. That replica is
authoritative for the subtree because every mutation under it goes
through the delegate. It then:

1. journals the transaction as speculation (see
   [Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)),
   with the generation's next stream index and the requester's `deps`;
2. acknowledges under its own durability layer: a backup chosen by RTT
   to the delegate, exactly like the root's (see
   [Durability and failover](durability-and-failover.md)), or Layer A
   when no peer is in budget. The root is never the delegate's backup;
3. streams its transactions to the root in index order
   (`DelegateStream`, one batch in flight per generation).

The delegate's own FUSE writes under its delegation take the local fast
path. Its journal rows retire when a root segment carries them, and are
stranded (rolled back, replayed by rid through the new owner) if the
generation ends first. A delegate refusal is journaled into the stream
like any other outcome.

### The append path and dependencies

The root appends a delegate's stream in index order after checking that
the generation is live and was granted to the sender. It does not
re-validate the operations. It journals them with the delegate's origin,
so the segment carries it and every replica, the delegate included,
learns the generation's applied index.

Stream batches (`DelegateStream`) and renewals (`DelegRenew`) are one-way
messages, and so are the root's answers (`DelegateStreamAck`,
`DelegRenewed`): the receiving node acknowledges a message once it is
queued for its authority core, so no request slot waits for the root's
core and no answer is lost to a request timeout. The stream
acknowledgement is cumulative (`through`: the root holds the
generation's stream through that index, whichever batch it answers), so
an answer to a batch the delegate already gave up on and re-sent still
counts, answers that overtake each other never move the delegate's
cursor back, and duplicates change nothing. One batch is in flight per
generation; one not answered within the request timeout is sent again
from the acknowledged cursor after a backoff (4 ticks, doubling to
2 s), and the root, which skips the rows it holds, answers it with the
same cursor. Every batch and renewal carries the delegate's stream
round: its incarnation and a fresh id at the install and at every change
of root. An answer counts only from the root the generation streams to,
in the round it answers, so an old root's answer (or one from an
earlier tenure of the same node, or one to the delegate's previous
incarnation, whose request ids the new one reuses) never credits the
new root with rows it does not have. The root counts a batch from the
generation's delegate as a sign of life, as it counts a renewal: while
the delegate streams, re-sends included, the root neither outwaits the
grant nor seals the delegate's backup. Before, a root that answered
later than the 2 s batch timeout (1 s for a renewal) had every answer
thrown away; the delegate re-sent a batch the root already held every
2.5 s, and the root sealed the generation as silent at the very cursor
the delegate was streaming from (`stress-ng-fs-nodes`: the delegate's
writes waited 100–220 s). Most of that lateness was the delegate's own
core falling behind the holder's log: every tailed segment rewound its
whole unappended stream. A segment now goes under a delegate's
transactions only where it overlaps them (see
[Forwarded mutations](forwarded-mutations.md#speculation-and-stranded-op-recovery)).

A forward carries `deps`: for each delegate stream, the highest
acknowledged position the requester has observed. The root appends a
transaction only once everything in its `deps` is in its replica, and a
delegate executes an op only once its `deps` are in *its* replica. So
no replica ever contains a record whose causes are missing: data
written in `D1` and then a marker written in `D2` are never seen in the
wrong order (`marker-order`). A requester tracks at most 8 streams; if
its observed set overflows, it sends the op to the root instead. A
dependency on a generation that has ended is void, except one past the
generation's cut (the transaction was acknowledged but never appended,
so it is being replayed by rid): an op with such a dependency is not
executed (a delegate or the root answers `Held`; an inbox op stays in
the inbox and its requester withdraws it), and the requester re-sends
it with fresh `deps` once its own replays have landed. A node holds its
new writes while it has stranded ops of its own to replay, so its
writes stay in the order it issued them. Which generations a replica
holds, and which have ended, is rebuilt when it reopens its store
(from the delegation table's highest generation and live rows, and the
log's per-generation index): a watermark naming a generation that
ended before a restart — a lock grant's floor carries the releaser's
frontier — is reached at once, instead of every read waiting out the
session budget for the rest of the process.

Replies from a delegate carry `(gen, idx)` in their position, so the
session guarantees of [Close-to-open modes](cto-modes.md) cover
delegated writes too.

A reply the requester cannot install at once (the delegate evaluated
the op behind its own transactions the log does not have yet, such as
a close right after the requester's create) waits for that
transaction. When the root has a backup, the root streams its journal,
its appends of the delegate streams included, to every node ahead of
S3 (see [Durability and failover](durability-and-failover.md)); the
requester's op is answered as soon as that stream installs the root's
append of it, one LAN round trip or two, not an S3 round trip. A node's
dependency on the root's journal (the `deps` of its next forward) is
likewise reached through that stream, so the root's own writes into a
delegated directory do not wait for its ship either. Status:
`ack.awaited_log_streamed_deleg`.

### Grants, renewal and expiry

- A grant lasts `CONSTELLATION_DELEGATION_TTL_MS` (by default the lock
  grant TTL, 20 s, and at least 5 s: lock grants under a delegation never
  outlive it, and at 5 s a lock in a delegated subtree was honoured for
  under 3 s and lapsed on any slow step). The delegate
  renews it at half its TTL (`DelegRenew`); the root answers with a new
  grant capped by the root's own lease (`expires − margin`), so a grant
  never outlives the lease it came from. A renewal not answered within
  the request timeout is sent again; a grant that comes later still
  counts, from its own renewal's send, for up to a minute.
- The delegate honours a grant until `sent + ttl − margin` on its own
  clock; the root considers it live until `granted + ttl + margin` on
  its clock (the same discipline as read delegations and lock grants).
- The root reclaims a grant nobody renewed (a crashed delegate) by
  itself, and grants no renewal once a recall or reclaim has begun.
- The delegate's grant lapses when it cannot get it renewed by the
  root's earliest reclaim: at `until + 2 × margin` (`until` is `sent +
  ttl − margin`, the root reclaims at `granted + ttl + margin` with
  `granted ≥ sent`, so the delegate lapses first, by the request's
  one-way delay), or the reclaim horizon (`ttl + margin`) after the
  install for a grant it never got renewed. The root is then dead or
  cut off. Lapsed, the generation executes and parks nothing (it still
  streams what it holds), and its ops, the parked ones first, take the
  ordinary route: to a live root, which recalls the generation first,
  else the inbox and the lease path. It keeps asking for a renewal
  every request timeout: a link that recovers before the root reclaims
  gets the grant back, and only a refusal or the log ends the
  generation. Status: `lapsed`. A lapse is always safe: it only gives up
  execution while the grant is not honoured anyway.
- An acknowledgement from a stream that ended before the root appended
  it is tentative: the row is stranded and the requester replays the op
  by rid.
- Every renewal carries the stream index the delegate has executed and
  when it sent the renewal, on its clock. The root keeps the latest of
  each generation: a lock owner that waited a holder out learns from
  them how far every stream went, and a granting answer carries the
  root's resulting cut to the delegate (see [Cluster
  locks](cluster-locks.md#coherence-from-one-holder-to-the-next)).

### Cross-subtree operations and recall

Cross-subtree operations go to the root: renames and hard links across
delegations, and `rmdir` or rename of a delegated root. So does any op
the root receives for a key under a live delegation (for example, from a
node with a stale table, or from the S3 inbox).

Before executing such an op, the root **recalls** the involved
generations:

1. it sends `DelegRecall`;
2. the delegate answers `DelegRecalled { through }` and stops executing;
3. the root waits for the stream to catch up to `through`, or, if the
   delegate is unreachable, for the grant to expire (if the delegate
   reported a backup, the root first asks the backup to seal);
4. the root journals `Recall { dir, gen }`, which ends the generation;
   later stream records of it are refused.

The root then executes the op itself. There is no two-phase commit.
After a cross-subtree op the root delegates the same subtrees again,
under new generations.

### Automatic placement

Placement is on by default (`CONSTELLATION_DELEGATION_PLACEMENT`). The
root sees every op's origin (its rid) and directory, keeps a sliding
window (`CONSTELLATION_DELEGATION_WINDOW_MS`, 30 s) of ops per directory
and node, and evaluates it every tenth of the window:

- **Delegate** the *topmost* directory `D` below `/` such that one node
  wrote at least `CONSTELLATION_DELEGATION_DOMINANCE` (70%) of the ops
  under `D`, the subtree saw at least `CONSTELLATION_DELEGATION_MIN_OPS`
  (200) ops in the window, and the node is connected, write-eligible,
  not the root and holds no other whole delegation. Walking up while the
  condition holds gives one delegation over a writer's whole working
  set, not a swarm of leaves. This follows Ceph's finding (Mantle,
  SC'15): move metadata to its dominant writer rather than spreading it.
- **Recall** a placed delegation when, for a whole dwell
  (`CONSTELLATION_DELEGATION_DWELL_MS`, 60 s), the delegate's share
  stayed below `CONSTELLATION_DELEGATION_LEAVE` (50%) or the subtree's
  rate below half the floor. Entering at 70% and leaving at 50% is the
  hysteresis.
- **Cool down** a recalled directory for
  `CONSTELLATION_DELEGATION_COOLDOWN_MS` (30 s) before it can be placed
  again, so it never ping-pongs.

Manual delegations and designations are never recalled by placement.
This placement is separate from the root lease's own placement (see
[Lease placement](lease-placement.md)), which moves the root lease
itself.

### Hot shared directories

Several writers creating files in one directory (plan 30 M12):

- **Commutative parent attributes.** Each node runs a hybrid logical
  clock (HLC); inode timestamps are HLC stamps, and a parent's `mtime`
  and `ctime` merge by `max`. A parent's `nlink` changes by deltas
  implied by the record (mkdir and rmdir ±1). Timestamps still increase
  (pjdfstest passes). The record format did not change.
- **Shared parent holds.** A create, unlink or link holds `(parent,
  name)` exclusively and the parent *shared*; `rmdir`, renaming the
  parent and `setattr` on it hold the parent exclusively. So creates of
  different names in one directory no longer serialize on the parent,
  on the requester or at the sequencer.
- **Name-hash ranges (GIGA+).** A directory can be split into 2, 4, 8 or
  16 ranges of its names' hashes, each delegated as its own generation.
  A range covers the directory's own names only; the contents of a
  subdirectory resolve past the split directory. A rename across ranges
  is cross-subtree (both ranges recalled, then re-delegated). `readdir`
  is unaffected, since there is one log. Ranges nobody holds stay with
  the root.

Placement splits a directory that is hot (at least the rate floor of ops
in the directory itself), that no node dominates, and that several nodes
each write at least `CONSTELLATION_DELEGATION_SPLIT` (20%) of. It uses
the smallest power of two that gives every such node a range. Each
range goes only to a node that dominates **that range** (70%). Names
hashed uniformly across writers give no range a dominant writer, so
such a directory stays with the root: on a LAN, splitting it measured
20–40% slower than leaving it (every op gained a hop), and the shared
parent holds already remove the serialization. In practice automatic
splits fire only when writers' names fall into distinct hash ranges;
`constellation delegate --range` does it by hand. A directory whose
ranges have all been recalled is merged back.

### Offline designations

An offline designation (`constellation offline <path>`) is a
non-stealable delegation to the designee: the same machinery, without
TTL expiry. While the designee is reachable, other nodes' writes under
the path are forwarded to it. An op under a designation that reaches the
root is refused with `EROFS`, and a cross-subtree op involving one with
`EXDEV`; a designation is never recalled to execute one. `--ro`
designations create no delegation.

### Failures

- **Delegate crashes, no backup**: the root reclaims the grant after
  `ttl + margin` and ends the generation. Requesters replay their
  acknowledged ops by rid through the root.
- **Delegate crashes, with a backup**: the root has the backup seal,
  drains what it holds, and continues (`delegate-crash`).
- **The delegate's backup is new or lost rows**: the delegate re-sends
  only what it has not seen in the log. The backup's acknowledgement
  covers every row it has from the log (its applied stream index), plus
  the rows it holds contiguously above that. When the backup is behind
  the log, its answer is short; the delegate then appends again on the
  stream tick, not on every answer. A node that stops backing the root
  discards only the root's tail, never the delegate streams it backs.
- **Delegate partitioned from the root**: it stops at `sent + ttl −
  margin`, before the root may reclaim; its unstreamed rows are stranded
  and replayed.
- **Delegate restarts or is handed off** (a crash and remount, `daemon
  --upgrade`, a K5 engine-pod handoff: the same node identity and
  journal): it re-adopts the generations the table names for it when
  it starts, renews them at once, and re-streams its unappended rows
  from the log's index. Its writes wait for that first renewal (one
  round trip), not for the root to reclaim the grant, which used to
  hold them for about the grant's ttl (`delegate-handoff-renewal`). A
  generation that ended meanwhile is refused at the renewal and dropped
  when the log says so.
- **Root dies and the delegate is no backup** (no backup in budget, or
  the root dropped it before it died): only a TTL takeover replaces the
  root. The delegate's grant lapses unrenewed (above), its writes take
  the lease path, and it takes the root over once the lease is
  claimable, then ends the generation itself: within the lease TTL plus
  the 3 s a non-backup waits (`delegate-root-loss-ttl`). Before, its
  writes parked on renewals the dead root never answered, and nothing
  started the takeover (`fsync`s stuck for minutes on kind). A third
  node whose op a delegate accepted waits for the root's append; it
  takes the root over the same way (see
  [Durability and failover](durability-and-failover.md#layer-b-a-backup-within-the-rtt-budget)).
  With the delegate a listed backup, the seal-based failover applies
  (`delegate-root-loss`, `delegate-root-blackhole`).
- **Root fails over**: generations of the old root are inherited by the
  new one. The initial grant is capped by the old root's lease, so an
  inherited grant is already dead at a TTL takeover and the successor
  may reclaim at once. After a fast failover the delegates re-stream to
  the new root from the index the log already has
  (`root-failover-with-delegates`). The backup that takes over
  re-journals the predecessor's appends of delegate streams under their
  delegation origin, in the predecessor's order; if it was itself a
  delegate, its own unappended transactions are rolled back and
  replayed by rid after that tail, never shipped ahead of it. Such a
  generation is marked in the same local transaction: its stream now
  has a hole (the replayed rows are root rows), so the node never
  executes in it again. It ends the generation itself once its gate
  opens; if it crashes or loses the lease first, it comes back as a
  stopped delegate that answers a recall at the log's index of the
  stream, and the next root ends it there. A generation the successor inherits is learned when its takeover gate
  opens, whichever round that is in.
- While any generation is live, the root does not release its lease when
  idle and declines a cooperative handoff.

### What stays with the root

- Cross-subtree operations, and everything not under a delegation.
- Appending to the log, publishing commits, GC.
- Writes from a requester whose observed stream set is full.

## Commands

| Command | Meaning |
|---|---|
| `constellation delegate TARGET --to NODE` | delegate the directory `TARGET` to node `NODE`. Run it on the root lease holder. Refused if the directory overlaps a live delegation, is the root, or `NODE` already holds a whole delegation |
| `constellation delegate TARGET --to NODE --range IDX/COUNT` | delegate one of `COUNT` (2, 4, 8 or 16) name-hash ranges of the directory |
| `constellation undelegate TARGET` | recall the delegation (all ranges) on a directory. Refused for a designation, which only `constellation online` releases |
| `constellation delegations TARGET` | list the live table: directory, path, node, generation, designated, range |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CONSTELLATION_DELEGATION` | on | delegate and accept delegations |
| `CONSTELLATION_DELEGATION_TTL_MS` | lock TTL (20 s), at least `5000` | grant lifetime |
| `CONSTELLATION_DELEGATION_PLACEMENT` | on | automatic placement and splits |
| `CONSTELLATION_DELEGATION_WINDOW_MS` | `30000` | placement window |
| `CONSTELLATION_DELEGATION_MIN_OPS` | `200` | rate floor per window |
| `CONSTELLATION_DELEGATION_DOMINANCE` | `70` | percent to be given a subtree or a range |
| `CONSTELLATION_DELEGATION_LEAVE` | `50` | percent below which a placed delegation is recalled |
| `CONSTELLATION_DELEGATION_SPLIT` | `20` | percent per writer that makes a hot directory a split candidate |
| `CONSTELLATION_DELEGATION_DWELL_MS` | `60000` | recall dwell |
| `CONSTELLATION_DELEGATION_COOLDOWN_MS` | `30000` | cool-down after a recall |

For the millisecond knobs `0` means the default. For `_MIN_OPS`,
`_DOMINANCE`, `_LEAVE` and `_SPLIT` it is a value: `SPLIT=0` turns hash
range splits off, `LEAVE=0` never recalls a placed delegation for its
share (only for its rate), `DOMINANCE=0` gives a subtree to its top
writer whatever the share, and `MIN_OPS=0` drops the rate floor. See
[Configuration](../configuration.md#delegation-and-placement).

## Status

`constellation status` (JSON, under `delegation`) shows `enabled`, the
table, `mine` (this node's generations), `gens`, `kinds`, `backups`, and
`placement`: the busiest subtrees as `(dir, node, node_ops,
subtree_ops)`. Counters include `executed`, `fast_path_executed`, `lapsed`,
`fast_path_routed`, `forwarded_to_delegate`, `deps_waits`,
`streamed_txs`, `appended_txs`, `cross_subtree`, `recalls_sent`,
`recalls_drained`, `recalls_expired`, `reclaimed`, `ended`,
`redelegated`, `deps_overflow_to_root`, `refused_designated`, and the
placement's `place_evaluations`, `place_delegated`, `place_recalled`,
`place_splits`, `place_range_recalls`, `place_skipped_cooldown` and
`place_skipped_unreachable`.

## Troubleshooting

### A writer's subtree is never delegated

Check `delegation.placement`: the writer needs 70% of the subtree's ops
and the subtree at least 200 ops per 30 s window. It must be connected
to the root over P2P and write-eligible, and must not already hold a
whole delegation. A recently recalled directory waits out the cool-down
(`place_skipped_cooldown`).

### A rename between two directories is slow

It is cross-subtree: the root recalls both delegations first. If one
delegate is unreachable, the rename waits for its grant to expire
(`recalls_expired`).

### Writes under a subtree fail with `EROFS` or `EXDEV`

The subtree is designated offline to another node that is unreachable
(`EROFS`), or the op crosses into a designated subtree (`EXDEV`). See
`constellation designations`.

### The root lease never moves

A live delegation (or a lock grant) keeps the root lease where it is.
Recall the delegations, or turn placement off, to let it move.

## FAQ

- **Does delegation change S3 traffic?** No. There is still one log;
  delegates make no S3 request for their writes, and the root's log
  PUTs carry them. Measured request counts per file were unchanged.
- **Does a single node pay anything?** No: with no peers nothing is ever
  delegated.
- **How much faster is it?** On one host, two writers in two
  directories went from about 610 files/s with a single sequencer to up
  to 950 delegated. The gain is largest when the root is far away:
  delegated writes stop paying a round trip to it.

## References

- Plan 30 §M11–§M12 ([plan](../../plans/v1/done/30-write-path-resilience-and-scale-out.md))
- [ADR-23](../../explanation/DECISIONS.md#adr-23-delegated-sub-sequencers-over-one-log)
- [`crates/authority/src/core/delegate.rs`](../../../crates/authority/src/core/delegate.rs),
  [`crates/authority/src/core/placement.rs`](../../../crates/authority/src/core/placement.rs),
  [`crates/meta/src/delegation.rs`](../../../crates/meta/src/delegation.rs),
  [`crates/meta/src/hlc.rs`](../../../crates/meta/src/hlc.rs)
- Models: `crates/model/src/delegation.rs`, `crates/model/src/hotdir.rs`;
  harness: `delegated-subtrees`, `cross-subtree-rename`,
  `delegate-crash`, `marker-order`, `root-failover-with-delegates`,
  `auto-placement`, `shared-dir-multi-writer`, `delegate-root-loss`,
  `delegate-root-blackhole`, `delegate-root-loss-ttl`,
  `delegate-handoff-renewal`
- [Lease placement](lease-placement.md), [Forwarded mutations](forwarded-mutations.md),
  [Cluster locks](cluster-locks.md)
