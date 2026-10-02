# Forwarded mutations

Forwarded mutations let a node that does not own a key ask the node
that sequences it (the lease holder, or the delegate of the key's
subtree) to execute a write over iroh. They reduce lease churn without
changing S3 authority. Delivery is **exactly-once** (plan 30 M2, the
RIFL design applied to forwarding): a forwarded mutation carries a
stable request id, and neither a slow reply nor a takeover can make it
execute twice. Without a P2P path, the same ops go through the
holder's S3 inbox instead.

## Table of Contents

- [Terminology](#terminology)
- [Wire messages](#wire-messages)
- [Request flow](#request-flow)
- [Routing to delegates](#routing-to-delegates)
- [Exactly-once identity and in-doubt handling](#exactly-once-identity-and-in-doubt-handling)
- [Acknowledgement and durability](#acknowledgement-and-durability)
- [Speculation and stranded-op recovery](#speculation-and-stranded-op-recovery)
- [Status and logs](#status-and-logs)
- [Failure and fallback](#failure-and-fallback)
- [The inbox: forwarding without P2P](#the-inbox-forwarding-without-p2p)
- [Snapshot batches](#snapshot-batches)
- [References](#references)

## Terminology

- **Holder**: the node with the live S3 lease and the only node allowed
  to append the log.
- **Sequencer**: the node that validates and orders a key's mutations:
  the holder, or the delegate whose subtree contains the key (see
  [Delegations](delegations.md)).
- **Requester**: a node sending an operation to the sequencer.
- **Position**: the state a reply was evaluated against (see
  [Close-to-open modes](cto-modes.md#positions)).
- **Speculation**: anything a node applied to its replica ahead of the
  durable log, recorded with the before-images needed to undo it (plan 30
  M3a, the node-local speculation log).
- **Shadow**: a speculation entry for a forwarded op's accepted records,
  applied by the requester before the holder's segment arrives. It
  carries the op and its rid, so it can be replayed if it is stranded.
- **Hint**: a speculation entry for the entry an `EEXIST` refusal was
  about, installed early so the caller's next lookup finds it.
- **Local entry**: a speculation entry for one of this node's own
  journaled transactions — a holder's unshipped journal (plan 30 M3b).
  It retires when the transaction ships.
- **Epoch marker**: an empty log segment a new holder ships right after
  taking the lease over from a holder that did not release it.
- **Rid**: `(node, incarnation, seq)`, the exactly-once identity of one
  mutation — see below.

## Wire messages

Both messages are signed iroh payloads. Mutation bodies use postcard encoding.

`MutateRequest` contains:

- `part`: the log stream (always `p0`; partitions were removed in plan 29).
- `requester`: requester's node id.
- `req_id`: requester-local correlation id, fresh on every attempt (wire
  request/reply matching only — see `rid` below for the identity that
  matters for exactly-once execution).
- `epoch_seen`: epoch hint; currently sent as `0`.
- `op`: encoded `MutateOp`.
- `rid`: `(node, incarnation, seq)` — stable across every retry of this
  op, unlike `req_id`.
- `acked_through`: the highest contiguous `rid.seq` of this requester's
  incarnation whose reply it has already received; lets the holder drop
  its in-memory dedup cache for anything it no longer needs.
- `deps` (plan 30 M11): the requester's observed position, including the
  delegate streams it has seen. The sequencer does not execute or append
  the op before its replica has all of it (see
  [Delegations](delegations.md#the-append-path-and-dependencies)).
- `pending`: for a `SetManifest`, the chunks it names that are still
  uploading on the requester (a `--write-mode back` close; empty after a
  `through` close, which uploads first). The recipient enrolls them as
  pending uploads it awaits from the requester *before* it executes the
  op, so nothing naming them ships, or streams ahead to another node,
  until they are in S3. The requester reports them with
  `ChunksDurable { from, hashes }` once they are up; the recipient
  checks S3 itself if that report never comes. Computed when the
  request is sent, so a retry or a replay by rid names what is pending
  then. The inbox path carries no such list: a manifest goes there only
  after its chunks are up. See
  [Durability and failover](durability-and-failover.md#--fsync-mode-and---write-mode).

`MutateReply` contains the matching `req_id`, an encoded
`MutateOutcome`, and:

- `base`: the last shipped log sequence that touched the op's keys when
  the sequencer evaluated it, or none when its unshipped journal had
  already touched one of them. The requester installs the reply's records
  ahead of the log only once its replica has applied `base`; otherwise
  it waits for the log to deliver them in order.
- `position` (`position_seq`, `position_pending`, `position_streams`,
  plan 30 M6): the state the op was evaluated against. It feeds the
  requester's session guarantees (see
  [Close-to-open modes](cto-modes.md#session-guarantees)).
- `gen` (plan 30 M11): the delegation generation that executed the op,
  or 0 for the root.

The outcomes:

- `Accepted { epoch, records }`
- `Errno(errno)`
- `NotHolder { holder }`
- `Busy`
- `Conflict { manifest }`: a stale `SetManifest` base (see below).
- `Exists { records, epoch }`: an `EEXIST` refusal carrying the entry
  that is there and the sequencer's epoch. The reply's position gives
  the floor of the hint the requester installs.
- `Held { retry_ms }` (plan 30 §M8): the op executed, but its
  acknowledgement waits until the `cto=strict` read delegations other
  nodes hold on what it touched are recalled (or outwaited: TTL plus the
  lease's drift margin). The holder answers this before the requester's
  forward timeout; the requester retries the same `rid` after `retry_ms`
  without spending an attempt, and the retry is answered from the
  holder's dedup once the wait is over (or re-attaches to it).

An empty or undecodable outcome is treated as `Busy`.

## Request flow

1. A mutating FUSE operation checks its partition lease view.
2. If the local node is holder, it validates and journals the operation
   locally.
3. Otherwise it reads the cached holder, or classifies the S3 lease, and sends
   `MutateRequest`.
4. The receiver verifies that its lease is usable, executes the mutation in
   one metadata transaction, and returns the journal records.
5. The requester applies those records to its local replica as a shadow in
   its speculation log, in one transaction with the before-images of every
   key they touch (once its replica has reached the reply's `base`).
6. The holder ships its journal to S3 and streams the shipped segment to
   its log-stream subscribers (plan 30 M7). `SegmentPublished` gossip is
   only a hint to tail now; it carries no segment bytes.

A `NotHolder` response updates the cached owner and permits a retry at
the redirected node, up to two redirects (a delegate names the root, the
root names a delegate).

What the sequencer's `Accepted` means for durability depends on the
lease's acknowledgement policy; see
[Acknowledgement and durability](#acknowledgement-and-durability).

## Routing to delegates

With delegations (plan 30 M11), the sequencer of an op is resolved per
key from the replicated delegation table: a key under a delegated
subtree (or a delegated name-hash range) belongs to that delegate,
everything else to the holder. A requester sends the op to the owner. An
op whose keys have two owners (a rename across delegations, `rmdir` of a
delegated root) goes to the holder, which recalls the delegations
involved before it executes. An op a node sends to a stale owner is
answered `NotHolder` with the right one. Everything in this page (rids,
dedup, speculation, positions) applies to a delegate as it does to the
holder. See [Delegations](delegations.md).

## Exactly-once identity and in-doubt handling

Every `MutateOp` a FUSE call issues is assigned one rid at the top of
`mutate_op_rebasable`, before any forward or lease-acquisition attempt,
and keeps that same rid across every retry — the same holder, a
redirected holder, or the lease-acquisition fallback. `incarnation` is
this mount's own counter, persisted in the node-local `local` keyspace
and bumped once, before serving any mutation; `seq` is a per-incarnation
counter that resets to 0 every mount, but the incarnation bump alone
keeps the pair from ever repeating even across a crash (`kill9-remount`
never reuses a rid).

**Holder-side dedup.** Executing an op appends `LogRecord::Completed
{ rid }` in the same fjall transaction as the op's own record(s), so it
ships in the same segment. Before executing a forwarded op, the holder
checks:

- an in-memory `recent` map of rids it has executed as holder but not
  yet had acknowledged (`acked_through`) — this answers a retry against
  a still-live holder with the identical `Accepted` reply, without
  re-executing;
- the durable, node-local `completed` keyspace, populated by replaying
  `Completed` records — this is what a *different* node (after a
  takeover) checks instead.

An executed rid is never executed again. A definitive refusal
(`Errno`, `Exists`) is an outcome too: since plan 30 M9 the sequencer
journals it as `Refused { rid, code }` (the portable `Code`, carried as
its own wire number rather than an OS errno), which ships like `Completed`
and enters `completed` on every replica, so a second execution of the
rid (a retry after a lost reply, a deposed holder's replay, an inbox
drain) answers the same errno instead of re-evaluating the op. A
transient refusal (`Conflict`, a stale manifest base) is not recorded; a
`SetManifest` rebase is a new op with a new rid.

**Requester-side retry.** A timeout, a transport error, or `Busy` leaves
the op *in doubt*, never refused — only an explicit `Errno`, `Conflict`,
or `Exists` ends an op without executing it. An in-doubt op retries the
same rid, in order:

1. the same holder, with backoff (three attempts, ~200ms/400ms/600ms —
   a slow holder is more common than a dead one);
2. a redirected holder, if a `NotHolder` reply named one;
3. only then the lease-acquisition path. Once it holds (tailing to head
   first — see below), it looks the rid up in `completed`: found means
   the op already happened, so it returns success without executing;
   not found means it executes locally, with the same rid.

**Coverage rule.** Resolving an in-doubt op against `completed` is only
valid if this replica has tailed every segment since the op was first
sent — otherwise an execution sitting in a segment it has not seen yet
would look like "never happened" and get re-executed. A fast P2P
handoff can leave the tail briefly behind the departing holder's last
upload, so the lease-acquisition fallback tails to head, and polls for
up to ~750ms, before trusting `completed`'s silence. GC respects the
same rule: log segments and `completed` rows younger than
`CONSTELLATION_COMPLETION_RETENTION_S` (default 900s, see
[Configuration](../configuration.md)) are never pruned, regardless of
what `CONSTELLATION_LOG_RETENTION_SEGMENTS` would otherwise allow. A
node that re-bootstraps across a gap wider than that window cannot
resolve an op stranded in it; such an op fails with `EIO`, never with a
re-execution.

## Acknowledgement and durability

What `Accepted` (and a refusal) means depends on the lease's
acknowledgement policy (plan 30 M9, see
[Durability and failover](durability-and-failover.md)):

| Policy | The reply leaves the sequencer once |
|---|---|
| `Local` (no peer within the RTT budget) | the op is committed to the sequencer's local journal |
| `Backup` (a peer within the budget) | every backup listed in the lease object holds the journal through the op |
| `S3` (`fs create --ack-policy s3`) | the segment carrying the op is in the bucket |

In every case the requester applies the returned records at once, so
read-your-writes does not wait for shipping, and it keeps them as
speculation with the op and its rid, so an op acknowledged under
`Local` is replayed by rid if the sequencer dies before shipping it.

`--fsync-mode s3` makes `fsync()` wait for the inode's dirty chunks and
records to reach S3. The default `--fsync-mode local` forces the node's
own metadata store to disk and nudges the background shipper.

## Speculation and stranded-op recovery

Plan 30 M3 makes every node's optimism explicit. A node's replica is
always a prefix of the durable log plus speculation it can take back:

- **Capture.** A shadow, a hint, or — on the holder — one of its own
  journaled transactions is applied through a capture context threaded
  into the one funnel every namespace write goes through, so the
  before-image of every key it touches — and the usage delta — lands in
  the node-local `spec` keyspace in the same transaction as the write. A
  segment tailed while older speculation is outstanding is captured the
  same way (as redo material), even though it is durable. While any
  speculation exists, every namespace write is captured.
- **Holder capture (M3b).** The holder's unshipped journal is speculation
  too: each journaled transaction writes, in its own fjall transaction, a
  `journal_tx` row (where it ends in the journal, the op and rid a replay
  re-executes, the epoch it ran under) and a `Local` speculation row with
  its before-images. Capture is on by default; the internal switch
  `CONSTELLATION_HOLDER_CAPTURE=0` turns the before-images off (the
  milestone's performance-gate fallback, see "Publishing" and
  "Deposition").
- **Transactions ship whole.** A shipped segment never splits a journaled
  transaction: batches end on a transaction boundary and the byte cap cuts
  before one. An op's records and its `Completed { rid }` therefore reach
  the log together or not at all, so a replay by rid can never re-execute
  an op that took effect.
- **Retirement.** A shadow retires when a tailed segment carries its
  `Completed { rid }`. A hint retires when the applied position reaches
  the floor the holder answered with. A local entry retires when its
  transaction ships. Rows are deleted once nothing older is outstanding.
- **Stranding.** The fencing rule means a holder's epoch can no longer add
  to the log once a segment from a later epoch exists. So applying a
  segment from a later epoch than an outstanding entry's *strands* it: the
  holder that accepted it died or was deposed with the op still unshipped.
  A takeover strands everything below the new epoch the same way, and so
  does a holder learning it was deposed (see "Deposition").
- **Rollback and redo.** Stranding restores before-images in reverse order
  down to the earliest stranded entry (usage too, and the chunk-reference,
  xattr and orphan indexes derived from the restored records), then
  re-applies everything after it that still stands — captured segments
  from their records, still-outstanding speculation from its records — in
  one transaction. The replica is then exactly the log prefix plus the
  surviving speculation. A stranded local transaction's journal rows are
  deleted, and so is its rid's `completed` row: it never took effect.
- **A late segment goes before local work.** A holder's unshipped
  transactions can only ship after anything it tails. So a segment tailed
  while it has any (a deposed holder's late, unfenced write) is applied
  *before* them: they are rolled back, the segment applied, and they are
  redone on top through the replay path — exactly what every other
  replica computes once they ship after it.
- **A tailed segment goes under the speculation it overlaps.** The
  segment is earlier in the log than every shadow, hint or streamed
  transaction still outstanding here, so when its records touch any of
  their keys (or it completes a shadow) the same rewind puts it in its
  log place: those rows are rolled back, the segment applied, and what
  still stands redone on top. Applied on top instead, a record re-ran
  against state that already held its own effect and later ones, and
  replay is not idempotent across that (EC2 campaign 4 B-2: git's loose
  object — `create tmp`, `link tmp obj`, `unlink tmp` — re-created with
  one link under the shadow of the `unlink`, so the segment's own
  `unlink tmp` dropped the inode under `obj`; the replica published the
  dangling name as holder and every fresh node lost the object).
  Independently, a durable `create`/`mkdir`/`symlink`/`mknod` whose inode
  already exists is that record applied again (inodes are never reused)
  and changes nothing: the first application stands. That covers the
  paths that apply a durable transaction over speculation of their own,
  such as a root appending a delegate's transaction over its shadow of
  the same op.
- **Replay by rid.** Each stranded op is queued, in original order, and
  sent again with its original rid down the ordinary forward path to
  whoever holds the lease now; if a queued op cannot reach a holder for
  10 s, the node takes the lease itself. Exactly-once holds because every
  holder answers a rid it already executed from `recent` or `completed`
  without executing it again. An accepted replay becomes a fresh shadow
  and retires normally. A stranded transaction without an op of its own is
  replayed from its records: a lone manifest commit as an optimistic
  `SetManifest`, anything else (a snapshot row, a quota change, a clone)
  as `MutateOp::Records`, under a rid derived from its journal position.
  A manifest replay first uploads the inode's pending chunks, so the
  holder never receives a manifest naming a chunk S3 does not have.
- **Refusals.** A replay the log no longer admits (the name was taken in
  the meantime, the target is gone, a manifest's base moved on) is a
  genuine conflict. It is materialized as a
  `.constellation-conflict/<name>@<node>-<ts>` copy and counted. An
  `unlink`/`rmdir` refused with `ENOENT` is not a conflict: the name is
  gone either way. A size-only `setattr` (the `O_TRUNC` half of a
  truncating write) queued before a manifest commit for the same inode is
  folded into it: the commit carries the final size, and truncating on its
  own would cut whatever the log put there when the commit is refused.
- **Takeover gate and epoch marker.** A node that wins a lease CAS keeps
  its lease view closed — no FUSE write, local forward or peer's forwarded
  op executes, and nothing ships — until its gate completes:
  1. a takeover from a holder that did not release first ships an empty
     *epoch-marker* segment at the new epoch. It fences any late segment
     of the old epoch, so nothing the gate replays can be duplicated by a
     late copy of the same op, and every other node strands the old
     epoch's speculation as soon as it tails it — even when the takeover's
     own triggering op is refused and ships nothing;
  2. it rolls back what the new epoch strands and executes every queued
     replay locally, in order.

  A new holder therefore never validates an op against phantom state, and
  an op its own clients were told succeeded lands before anything issued
  after the takeover. If the marker or a local replay fails, the lease
  stays held with the view closed and every sync round retries the gate;
  new mutations wait (and fail with `EIO` at the acquire deadline if the
  gate can never complete) rather than run ahead of the queued ops. The
  continuation-epoch path runs the same gate. Before a claim that follows
  a P2P handoff, the node first tails to the departing holder's last
  shipped segment, so nothing it shipped is mistaken for stranded. A
  forward reply accepted at an older epoch that arrives after this node's
  takeover is not installed; the op is queued for replay, and the caller
  is answered `Busy` so its lease path executes it here by the same rid.
- **Deposition (M3b).** A holder learns it was deposed when a renewal
  finds another holder, or when it tails a segment from a later epoch (the
  new holder's marker, at the latest; a holder that has tailed a higher
  epoch than its own renews at once). The next sync round tails to head
  and strands its unshipped transactions: rolled back from their
  before-images and queued for replay by rid through the new holder.
  Only a genuine overlap then becomes a conflict copy. This replaced
  reintegration's classify-against-a-side-replica pass; the `reintegrate`
  command still exists and runs the same recovery at once. With holder
  capture off, the namespace is instead rebuilt from the shared log (a
  side replica bootstrapped from the head commit) and the journal's ops
  queued the same way.
- **Publishing.** A commit is always the log-prefix state at the
  `applied` position it claims. A node with outstanding shadows or hints
  does not publish; its dirty set waits for them to retire or roll back. A
  holder with an unshipped journal publishes with every key that journal
  touched replaced by its earliest before-image — exactly its value at the
  last shipped position — and leaves those keys dirty for the publish
  after the journal ships. With holder capture off, a holder does not
  publish while its journal is non-empty. A snapshot (which needs a commit
  reflecting the replica as it stands) ships first and retries while any
  of this is outstanding.
- **Releases are fenced exactly.** A release or handoff raises a
  "releasing" flag on the lease view for its whole final flush and release
  CAS, and every local mutation is admitted through that view (counted in
  flight before it checks the flag, and waited for by the release). No
  local write can land between the final flush and the release, however
  slow the flush.

### The shadow does not win conflicts

A shadowed record is an optimism about *timing*, never about *order*. The
holder has already sequenced it, so a peer's record for the same inode may
legitimately follow it in the log. When the requester tails the holder's
segment it therefore applies every record in log order, and the shadow plays
no part in the conflict check — only genuinely *unshipped* local journal
records may suppress a foreign record (`TouchSet`), because those alone are
guaranteed to sort after anything being tailed.

Letting the shadow suppress is a permanent divergence, not a delay: a skipped
record is dropped as the segment is marked applied and never revisited, so
every requester in a multi-writer duel keeps its own value forever. With N
writers on one file you get N distinct values that never reconcile — the
shape a chaos `chmod_duel` or `write_overlap` convergence failure takes.
Regression test: `shipper::tests::forwarded_duel_on_one_inode_converges_on_every_replica`.

### Manifest commits are optimistic

`SetManifest` carries a whole-file manifest plus the `base_manifest` it was
composed on. The holder installs it only if that base is still current;
otherwise it refuses with `Conflict`, carrying the manifest that *is* current.
Without the check, a whole-file image composed on an old base silently drops
every chunk that landed in between — which is how concurrent disjoint
`WriteAt`s from several nodes lost all but the last patch.

The requester rebases rather than failing: it lays this flush's own chunks
over the returned image and retries, up to `MANIFEST_COMMIT_ATTEMPTS`, then
gives up with `EAGAIN`. Carrying the current manifest in the refusal is what
makes one round trip enough; waiting to tail the holder's segment instead
would make progress depend on shipping. A `Conflict { manifest: None }` means
this node executed the mutation itself, so its own replica is already
authoritative and the rebase reads from there.

Rebasing adopts the longer of the two file lengths, so a peer's concurrent
extension survives. A flush that *shortened* the file relative to its own
base is a truncate and keeps its own length instead.

### Concurrent forwarding, correctly ordered

A requester's forwards run concurrently, not one at a time: each network
round trip runs as its own task, and the authority core goes straight
back to its queue instead of awaiting the round trip inline (plan 29 M4
measured the inline version as the reason 3-node forwarded throughput
came in *below* single-node). Plan 29's
`CONSTELLATION_FORWARD_MAX_INFLIGHT` bound no longer exists, and none is
needed: forwards are bounded by their sources. Each is one blocked
caller on the requester (a FUSE worker thread, one of the bounded
write-back uploads, or the replay queue, which runs one op at a time),
retried at most `forward_retries` (3) times before it takes the lease
path. On the holder, the P2P layer serves at most 32 concurrent
streams per peer connection (`MAX_CONCURRENT_STREAMS`,
`crates/net/src/peers.rs`), and a requester's further streams wait in
QUIC flow control, so a slow holder backs its requesters off instead of
queueing without limit: its queue is at most 32 requests per peer, and
retries of a timed-out request take slots from that peer's own budget,
not another's.

Concurrency is safe only because a requester-side ordering gate (in the
authority core's client, `crates/authority/src/core/client.rs`)
serializes any two forwards whose *conflict-key sets* overlap: every
key an op reads or writes. Since plan 30 M12, a create, unlink or link
holds its `(parent, name)` entry exclusively and the parent directory
only *shared* (parent `mtime`/`ctime` merge by `max`, and `nlink`
changes as a delta), so creates of different names in one directory run
in parallel. `rmdir`, renaming a directory and `setattr` on it hold it
exclusively; two `SetManifest`s on one file race each other's base; a
rename touches two parents and possibly the moved or replaced inodes.
Ops with disjoint key sets run fully in parallel; overlapping ops
resolve in the order this node issued them, so they land on the holder,
and get installed back as shadows on the requester, in that same order.
Without this, two forwards that race the network could complete out of
order and leave the requester's replica diverged from the holder's —
replay assumes records arrive in the holder's execution order, and the
speculation log's rollback assumes shadows were captured in it.

## Status and logs

`constellation status` and the web UI expose:

- `forwarded_ok`: accepted forwarded requests made by this node.
- `forwarded_err`: timeouts, transport/decode failures, redirects, and
  non-accepted replies.
- `forward_p50_ms`: median of the last 256 successful forward latencies.
- `forward_dedup_hits`: forwarded requests the holder answered from
  `recent`/`completed` instead of re-executing (plan 30 M2).
- `forward_retries`: same-rid forward retries this node's requester side
  made before falling back to the lease-acquisition path (plan 30 M2).
- `forward_indoubt_resolved`: in-doubt ops the lease path resolved
  against `completed` instead of re-executing (plan 30 M2).
- `placement_reason`: the last holder placement recommendation.
- `speculation.outstanding`: shadows and hints not yet confirmed by the
  log. While non-zero, this node does not publish commits.
- `speculation.pending_replay`: stranded ops queued for replay by rid.
- `speculation.rolled_back`: shadows and hints rolled back because a later
  epoch stranded them.
- `speculation.stranded_replayed`: stranded ops replayed and accepted.
- `speculation.replay_conflicts`: refused replays materialized as conflict
  copies.
- `speculation.local`: this node's own unshipped transactions captured as
  speculation (a holder's journal). These do not stop a publish.
- `speculation.local_rolled_back`: own transactions rolled back after a
  deposition.
- `speculation.depositions`: deposition recoveries run.
- `speculation.epoch_markers`: epoch-marker segments shipped after a
  takeover.
- `speculation.gate_pending`: this node holds the lease but its takeover
  gate has not completed; new mutations wait.
- `speculation.copies_pending`, `speculation.copies_stalled`: refused
  replays whose conflict copy is still being written (it backs off and
  retries, and is never dropped).

The same counters are exported as `constellation_speculation_*` metrics.
A rollback logs `segment from a later epoch stranded speculative state`
(or `takeover stranded speculative state`); a refused replay logs
`stranded op replay refused; materializing a conflict copy`; a deposition
logs `LEASE LOST` and then `deposition recovered`; a marker logs
`shipped the takeover's epoch marker`.

Under a steady multi-writer workload, `forwarded_ok` should rise while
`handed the lease to a peer` should disappear. That log line means the older
handoff path ran; repeated occurrences indicate forwarding or placement is
not stable.

## Failure and fallback

Forwarding is optional. A timeout, empty reply, `Busy`, stale holder, or
unreachable peer leaves the op in doubt and retries the same rid (see
above) before falling back to the normal lease-acquisition path. With no
P2P path to the holder at all (P2P disabled, or an outage past its
grace), the op goes through the S3 inbox instead (see below). Handoff can
release a reachable holder immediately; otherwise the requester waits for
release or TTL expiry and claims through S3 CAS. Every path — same-holder
retry, redirected-holder retry, or the lease-acquisition fallback — resolves
to exactly one execution, never a repeat.

The default request timeout is 500 ms
(`CONSTELLATION_FORWARD_TIMEOUT_MS`). An op still in doubt at its client
deadline (twice the lease TTL) fails with `EIO` and is never
re-executed. Setting `CONSTELLATION_FORWARD=off`
disables requester-side forwarding entirely: every non-holder mutation
takes the lease-acquisition path (P2P handoff, then S3 CAS) directly, with
no rid retries to attempt first, restoring writer-follows-lease placement.
A holder crash after `Accepted` under the `Local` policy strands the op
in its unshipped journal; the requester's speculation log rolls the shadow
back and replays the op by rid through the next holder (see above). Under
`Backup`, the sealed backup that takes over already holds it. A holder that was deposed rather
than killed rolls its own unshipped journal back the same way and replays
it by rid through the new holder. Epoch fencing, and the new holder's
epoch marker, prevent a competing append history.

## The inbox: forwarding without P2P

Plan 30 M13. When a requester has no P2P path to the holder — P2P is
off, the holder is not in its peer directory, or an *outage* to it has
lasted longer than `CONSTELLATION_INBOX_P2P_GRACE_MS` with nothing
heard from it since — it forwards through the bucket instead of taking
the lease (the "ping-pong" that capped P2P-off clusters at 41–57
ops/s). An outage is a failed dial or a transport error that evicted
the connection: a forward that came back `Busy` with no open QUIC
connection to the holder. A slow or timing-out reply on a connection
that is still open is never an outage, however long it goes on — it
stays on the same-rid retries above and the lease fallback, as before
M13. And an outage ends the moment anything is heard from the holder:
its entry in the peer directory shows it connected, or any successful
exchange with it (a lease request, a ping, a chunk fetch, a gossip
neighbor-up) is later than the outage began. That is the whole rule: a
peer the requester has never talked to is reachable and gets dialed,
as before M13, and the inbox engages only for a holder that cannot be
reached at all.

**The hybrid.** The inbox is for *sporadic* writes: one occasional
write from a non-holder is answered without moving the lease and
without disturbing the holder. A requester whose inbox demand is
*sustained* — at least `CONSTELLATION_INBOX_ESCALATE_OPS` (8)
inbox-answered ops, or at least five ops whose waits on their round
trips add up (the time covered by any of them, leaving the single
slowest out) to `CONSTELLATION_INBOX_ESCALATE_WAIT_MS` (1.5 s), within
`CONSTELLATION_INBOX_ESCALATE_WINDOW_MS` (10 s) — *escalates*: it asks
for the lease through the ordinary lease path (`wanted_by`, answered by
the holder's dwell and grace rules, so no new ping-pong is created; its
batches also carry a `wants_lease` flag the holder sees at its next
poll) and keeps writing through the inbox until the lease arrives; the takeover
gate's drain of lower epochs then executes whatever is still queued, in
order, before its first local op, and later writes run locally at full
speed. It stops asking once the window has fallen below half of both
thresholds; when it goes quiet as holder, the usual idle release hands
the lease back or on. Why: Linux serializes creates in one directory
(the parent's lock is held for the whole create), so on a create storm
each op is one sequential inbox round trip and batches cannot form —
there the lease must move, as it does today. `status.inbox.escalated`,
`escalations`, `lease_requests`, `inbox_ops` and `local_ops` show which
regime a node is in. An escalation is dropped if the holder becomes
reachable over P2P again. Before an op that went into the inbox is
forwarded over a P2P path that came back (or held back because a
dependency was lost), the requester withdraws its own batch, so the op
is not executed through both paths out of order; the rid dedups either
way. It withdraws by overwriting the batch with a **tombstone** (the
same key, no ops; one plain PUT), never by DELETE: a deleted key was a
hole in the numbering, and a holder that had not read it GET-nexted it
forever while every later batch of that requester in the epoch waited
for the in-doubt deadline. The holder reads the tombstone, executes
nothing and steps past it. Other ops the withdrawn batch carried are
re-submitted by rid at the front of the queue; a holder that read the
batch before the overwrite deduplicates the copies.

1. The op, with its rid and its `deps` (what this node had observed,
   including the delegate streams it was answered from, as a P2P
   forward carries them; batch format version 3), goes into the next
   **batch object**
   `inbox/<epoch>/<node>/<n>` under the epoch the lease object currently
   shows (one CAS-created PUT; everything a node's FUSE threads queue
   while a PUT is in flight shares the next batch, so requests per op
   fall as load rises). The requester-side ordering gate is released as
   soon as the op is queued — its order is the queue's, which the holder
   honours — so overlapping ops (every create in one directory) pipeline
   instead of taking one round trip each. Numbering is per `(epoch,
   node)`, restarts at 0 in every epoch, and a (re)started requester
   resumes it with one LIST of its own prefix.
2. The **holder polls** each requester's next batch with one GET
   (`inbox/<epoch>/<node>/<cursor>`), on a per-requester schedule: every
   `CONSTELLATION_INBOX_HOT_MS` (20 ms) right after a hit and for ~25
   misses after it, then doubling from the sync interval up to
   `CONSTELLATION_INBOX_IDLE_MAX_MS` (2 s) for a requester that submitted
   within the last minute, up to the sync loop's idle ceiling (10 s)
   after that. A requester with an outcome pending tails the log every
   `CONSTELLATION_INBOX_TAIL_MS` (20 ms), so a round trip on a local S3
   is a few tens of milliseconds. The holder polls the write-eligible
   roster minus itself minus the peers it is P2P-connected to, so a
   single node and a healthy P2P cluster poll nothing; a node that
   appears in the roster is polled at the next round (the registry poll
   nudges one). A requester the holder starts polling is polled once at
   once and is never hot until a poll hits; it starts warm only if it
   wrote recently (a P2P forward or an inbox op within about a minute,
   or it wants the lease), cold otherwise (with P2P off every requester
   starts warm, once per tenure). A requester whose link comes back
   keeps its cursor and schedule for the rest of the tenure, so a
   flapping link never polls faster than one that stayed down. An idle
   poll fetches one batch; only a poll after a hit fetches
   `CONSTELLATION_INBOX_POLL_WIDTH`. Batches execute in order through the same dedup a P2P
   forward gets (`recent`, `completed`), and an op whose `deps` name a
   delegate stream the holder does not have yet waits for it (a marker
   is never appended ahead of the data a delegate acknowledged before
   it).
3. **Outcomes ride the log.** An executed op ships its records,
   `Completed { rid }` and an `InboxAck` (its batch position); a refused
   one ships `Refused { rid, code }` plus the ack. The requester, which
   tails the log anyway, returns to the FUSE caller when the outcome is
   applied — success once its own records are in the replica
   (read-your-write holds), or the errno. A stale manifest base comes
   back as `ESTALE` and the requester rebases from its own replica,
   which has tailed the refusing segment. Nothing is speculated: an
   inbox op installs no shadow.
4. **GC.** The holder deletes a batch once the segment with its outcomes
   has shipped, keeping each requester's newest one (the high-water mark
   a restarted requester resumes from). A takeover drains every older
   epoch's batches inside its gate — after its own stranded ops, before
   its view opens — and deletes them. A batch with an op under a live
   delegation cannot run inside the gate (the delegation is recalled
   first): that batch and the same requester's later ones are kept, in
   order, while every other requester's batches still drain. Once the
   view is open the holder runs the kept batches from its poll tick,
   through the ordinary admitted path (which starts the recall and
   waits for it), before it polls that requester under the new epoch,
   and deletes them once they ran. A generation that such a recall
   ended is not re-delegated while the op still waits to run (it would
   recall again, forever). `drained_batches` counts batches that
   executed.

**Why refusals must be outcomes.** On the inbox path the *holder*
re-reads batches (a successor's drain), and a requester may re-submit a
rid whose refusal sits in a segment it has not tailed yet (it noticed
the takeover by reading the lease). Re-evaluating a refused `create(x)`
after `x` was unlinked would execute it: the caller was told `EEXIST`
and `x` appears anyway. So a `Refused` record enters `completed` on
every replica, and every dedup site — the holder's executor, the drain,
the lease path's in-doubt check, a stranded replay — answers the rid
with the errno. The Stateright model shows both naive variants failing
(`crates/model/tests/inbox.rs`: a drain without rid dedup, and refusals
not deduplicated). This is why plan 30 M13 introduced `Refused`; M9
then extended it to P2P refusals (see above).

**Exactly-once across epochs.** An epoch-`e` batch when the lease moves
to `e+1`: the new holder drains it inside its gate, and the requester —
which learns of the takeover from the lease object it re-reads every
`CONSTELLATION_INBOX_RECHECK_MS` while waiting — deletes its stale batch
and re-submits the same rid under `e+1`; a deposed holder's rollback
replays the same rid a third way. All three meet the same dedup. The
`InboxAck` watermark (per `(epoch, node)`, never pruned) keeps a drain
older than the `completed` retention window exact without trusting a
clock. An op with no outcome by `min(2 × TTL, retention/2)` is in doubt
and takes the lease path, whose takeover gate drains the batch and whose
in-doubt check then finds the rid.

`status.inbox` reports both roles' counters (`submitted_ops`,
`pending_ops`, `resubmitted_ops`, `withdrawn_ops`, `unavailable`,
`executed_ops`, `refused_ops`, `deduped_ops`, `drained_batches`, `polls`,
`poll_hits`, `tombstones_read`, `gc_deleted`, `tracked_requesters`),
exported as `constellation_inbox_*`.

Locks and `cto=strict` ReadIndex never go through the inbox: with no P2P
path, cluster locks are unavailable and a strict open tails S3 instead
(see [Cluster locks](cluster-locks.md#without-p2p) and
[Close-to-open modes](cto-modes.md#without-p2p)).

### Known gaps

- `MAX_BATCH_BYTES` (1 MiB) is defined but not enforced; a batch is
  bounded by its op count (512).

## Snapshot batches

Snapshot rows are journaled metadata too — a create writes the
`snaps/<id>.json` object and a `SnapCreate2` row, a delete removes the
row and then the object, a hold writes a `SnapHold` row — so they can
only reach the cluster from the root-lease holder. They do not go
through `MutateRequest`: a create drains, ships and publishes a metadata
commit first, which is work for the holder's sync task and its bucket,
not an op the authority core sequences. Instead every snapshot row write
is an item of a **snapshot batch** (plan 32 Step 0.1,
`crates/engine/src/snapshot_batch.rs`), routed like a system op and
never by moving the lease:

| This node | The lease object | The batch |
|---|---|---|
| holds a usable root lease | — | executes here |
| does not | names a live holder, and P2P is on here | uploads this node's own pending chunks (a batch with a create), then is sent to the holder as `SnapshotBatchRequest` |
| does not | names a live holder, and P2P is off here | asks for the lease as every write in that mode does (`wanted_by`, cooperative handover) and executes here once it has it |
| does not | names nobody, an expired or released lease, or this node | acquires the free lease and executes here |

A live holder is never preempted while a peer path to it exists. A
forward that fails (the holder unreachable or not dialable, a transport
error, `CONSTELLATION_SNAPSHOT_FORWARD_TIMEOUT_MS`, the holder answering
`NotHolder` because it released meanwhile) is the caller's error;
nothing falls back to acquiring — the scheduler retries on its next
tick. **P2P off is a different case**: with no peer path at all
(`Peers::disabled()`, an S3-only cluster) there is nothing to forward
to, and a non-holder writes nothing in that mode without the lease, so a
snapshot operation takes the old route: `Acquire` registers in
`wanted_by`, the call fails "held by another node" while the holder
still holds, the holder hands over at its next round, and a retry runs
the batch locally — exactly what snapshots did before batches. (The S3
inbox, M13, is not used for snapshot batches.)

Before a forwarded batch that creates a snapshot, the requester drains
its own pending chunk uploads (`SyncRequest::DrainInode { ino: 0 }`, all
of them: chunks are not indexed by subtree). Its writes reach the
holder as forwarded rows whose chunks only it has, and the holder's
drain cannot ship those rows until the chunks are in S3; this is what
the requester's own `Barrier` did when it took the lease. A drain
failure is the caller's error, before anything is forwarded.

`snapshot create`, `snapshot delete` and `snapshot hold`/`release` all
take this path (a multi-selector `snapshot delete`, control method
`snapshot.delete_many`, sends its `Delete` items in batches of at most
`MAX_SNAPSHOT_DELETES_PER_BATCH` = 256, one after another, each under
its own rid, so the request and its reply fit a 64 KiB peer frame; a
batch that fails leaves the call partial — its snapshots "not
confirmed", the later ones "not attempted" — rather than an error after
earlier batches deleted); `clone` still takes the lease and a barrier
(`acquire_namespace_barrier`), and `quota set` takes the lease alone.

`SnapshotBatchRequest { requester, req_id, rid, items }` carries the
items (`Create`, `Delete`, `Hold`) as typed postcard data, and
`SnapshotBatchReply { req_id, outcome }` answers `Done(results)`,
`NotHolder` or `Failed(reason)`; per-item results are `Created` (with
the recorded row), `Skipped`, `AlreadyExists`, `Deleted`, `NotFound`,
`HoldSet` (the row), `Refused` and `DeletedObjectRemains` (the row is
deleted but deleting the `snaps/` object failed; `snapshot.delete`
reports it as an error, as before batches, `snapshot.delete_many` counts
the snapshot as deleted and logs a warning, and the orphan object is left
to plan 32 §0.3's reconciliation). A forwarded reply clips every
`Refused`/`DeletedObjectRemains`/`Failed` reason to
`MAX_SNAPSHOT_REASON_BYTES` = 192 bytes (a refusal names the snapshot's
path, which is unbounded); `message.rs`'s
`maximum_snapshot_delete_batch_fits_a_frame` pins the largest delete
request and its all-refused reply under `MAX_FRAME`. Both were appended
at the end of `Payload`. The executing holder drains what `Barrier` drains for each
distinct create path, forces **one** publish for the whole batch, then
runs the items in order.

**Exactly once.** The rid is allocated once per batch
(`next_system_rid`) and reused by every retry. The executing node keeps
the last 1024 batch results by rid and answers a duplicate from them
(batches execute one at a time there, so a duplicate that arrives while
its original runs waits for it). A retry that crosses a holder change
reaches a node without that memory; the `snaps/` create-if-absent is the
backstop — the retried create finds its own object and answers
`AlreadyExists` (a manual `snapshot create` reports that as "already
exists", plan 32's scheduler as success), a retried delete finds no row
(`NotFound`), and a hold is idempotent. A batch with an item refused
because the executor lost the lease mid-batch (`Refused` starting "this
node lost the root write lease") is not kept: that item did not run, and
the retry under the same rid runs it. Callers that retry use
`SnapshotBatcher::submit(rid, items)` with one rid from `next_rid()`;
the control-protocol commands allocate one per request.

**Inside a delegated subtree** the drain is the root holder's own
`Barrier`: it ships the root's journal, which holds whatever the
delegate has streamed to the root, and sends the delegate nothing. A
snapshot of a delegated subtree therefore freezes the delegate's writes
as of its last streamed batch (pinned by the authority core test
`a_barrier_inside_a_delegated_subtree_does_not_reach_the_delegate`).

## References

- [Lease placement](lease-placement.md), [Delegations](delegations.md)
- [Durability and failover](durability-and-failover.md),
  [Close-to-open modes](cto-modes.md)
- [Configuration](../configuration.md)
- [ADR-14](../../explanation/DECISIONS.md#adr-14-forward-mutations-to-the-lease-holder-instead-of-moving-the-lease),
  [ADR-18](../../explanation/DECISIONS.md#adr-18-exactly-once-forwarding-with-request-ids),
  [ADR-19](../../explanation/DECISIONS.md#adr-19-the-replica-is-a-log-prefix-plus-explicit-speculation),
  [ADR-24](../../explanation/DECISIONS.md#adr-24-the-hybrid-s3-inbox-for-writes-without-a-p2p-path)
- [DESIGN.md §4–6](../../explanation/DESIGN.md)
