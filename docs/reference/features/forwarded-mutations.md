# Forwarded mutations

Forwarded mutations let a non-holder ask the partition lease holder to
sequence a write over iroh. They reduce lease churn without changing S3
authority. Delivery is **exactly-once** (plan 30 M2, the RIFL design
applied to forwarding): a forwarded mutation carries a stable request
id, and neither a slow reply nor a takeover can make it execute twice.

## Table of Contents

- [Terminology](#terminology)
- [Wire messages](#wire-messages)
- [Request flow](#request-flow)
- [Exactly-once identity and in-doubt handling](#exactly-once-identity-and-in-doubt-handling)
- [Acknowledgement and durability](#acknowledgement-and-durability)
- [Speculation and stranded-op recovery](#speculation-and-stranded-op-recovery)
- [Status and logs](#status-and-logs)
- [Failure and fallback](#failure-and-fallback)
- [References](#references)

## Terminology

- **Holder / sequencer**: the node with the live S3 lease and the only node
  allowed to append the partition log.
- **Requester**: a non-holder sending an operation to the sequencer.
- **Speculation**: anything a node applied to its replica ahead of the
  durable log, recorded with the before-images needed to undo it (plan 30
  M3a, the node-local speculation log).
- **Shadow**: a speculation entry for a forwarded op's accepted records,
  applied by the requester before the holder's segment arrives. It
  carries the op and its rid, so it can be replayed if it is stranded.
- **Hint**: a speculation entry for the entry an `EEXIST` refusal was
  about, installed early so the caller's next lookup finds it.
- **Rid**: `(node, incarnation, seq)`, the exactly-once identity of one
  mutation — see below.

## Wire messages

Both messages are signed iroh payloads. Mutation bodies use postcard encoding.

`MutateRequest` contains:

- `part`: target partition.
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

`MutateReply` contains the matching `req_id` and an encoded `MutateOutcome`:

- `Accepted { epoch, records }`
- `Errno(errno)`
- `NotHolder { holder }`
- `Busy`
- `Conflict { manifest }`: a stale `SetManifest` base (see below).
- `Exists { records, ship_floor, epoch }`: an `EEXIST` refusal carrying the
  entry that is there, the holder's next ship position, and its epoch.

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
   key they touch.
6. The holder ships its journal to S3. A small segment may also be carried in
   the `SegmentPublished` gossip payload; larger segments are fetched from S3.

A `NotHolder` response updates the cached holder and permits one retry at the
redirected node.

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

An executed rid is never executed again. Refusals are not recorded: a
retried refused op is simply re-evaluated, and takes effect (or not) at
the retry, which is still linearizable. A `SetManifest` rebase is a new
op with a new rid.

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

`Accepted` means the holder committed the operation to its local journal. It
does **not** mean the partition segment is already on S3. The requester applies
the returned records so read-your-write does not wait for segment shipping.

Mounts using `--fsync-mode s3` add the existing inode/partition `Barrier`:
`fsync()` waits for dirty chunks and metadata through that point to reach S3.
The default `--fsync-mode local` only requires local durability and nudges the
background shipper.

## Speculation and stranded-op recovery

Plan 30 M3a makes the requester's optimism explicit. A node's replica is
always a prefix of the durable log plus speculation it can take back:

- **Capture.** A shadow or hint is applied through a capture context
  threaded into the one funnel every namespace write goes through, so the
  before-image of every key it touches — and the usage delta — lands in
  the node-local `spec` keyspace in the same transaction as the write. A
  segment tailed while older speculation is outstanding is captured the
  same way (as redo material), even though it is durable.
- **Retirement.** A shadow retires when a tailed segment carries its
  `Completed { rid }`. A hint retires when the applied position reaches
  the floor the holder answered with. Rows are deleted once nothing older
  is outstanding.
- **Stranding.** The fencing rule means a holder's epoch can no longer add
  to the log once a segment from a later epoch exists. So applying a
  segment from a later epoch than an outstanding shadow's or hint's
  *strands* it: the holder that accepted it died or was deposed with the
  op still unshipped. A takeover strands everything below the new epoch
  the same way.
- **Rollback and redo.** Stranding restores before-images in reverse order
  down to the earliest stranded entry (usage too, and the chunk-reference,
  xattr and orphan indexes derived from the restored records), then
  re-applies everything after it that still stands — captured segments
  from their records, still-outstanding speculation from its records — in
  one transaction. The replica is then exactly the log prefix plus the
  surviving speculation.
- **Replay by rid.** Each stranded shadow's op is queued, in original
  order, and sent again with its original rid down the ordinary forward
  path to whoever holds the lease now; if a queued op cannot reach a
  holder for 10 s, the node takes the lease itself. Exactly-once holds
  because every holder answers a rid it already executed from `recent` or
  `completed` without executing it again. An accepted replay becomes a
  fresh shadow and retires normally.
- **Refusals.** A replay the log no longer admits (the name was taken in
  the meantime, the target is gone) is a genuine conflict. It is
  materialized as a `.constellation-conflict/<name>@<node>-<ts>` copy,
  like a reintegration conflict, and counted. An `unlink`/`rmdir` refused
  with `ENOENT` is not a conflict: the name is gone either way.
- **Takeover gate.** A node that wins a lease CAS rolls back what the new
  epoch strands and executes every queued replay locally, in order,
  before its lease view opens. A new holder therefore never validates an
  op against phantom state, and an op its own clients were told succeeded
  lands before anything issued after the takeover. Before a claim that
  follows a P2P handoff, the node first tails to the departing holder's
  last shipped segment, so nothing it shipped is mistaken for stranded.
- **Publishing.** A node with outstanding speculation does not publish a
  metadata commit; its dirty set waits for the speculation to retire or
  roll back. Commits are therefore built from log-prefix state. (The
  holder's own unshipped journal is plan 30 M3b.)

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

A requester's forwards run concurrently, not one at a time: the daemon's
sync task hands each `Forward` request off to its own task (bounded by
`CONSTELLATION_FORWARD_MAX_INFLIGHT`, default 64) and immediately goes
back to draining its queue, instead of awaiting the round trip inline
(plan 29 M4 measured the inline version as the reason 3-node forwarded
throughput came in *below* single-node).

Concurrency is safe only because a requester-side ordering gate
(`crate::keygate::KeyGate` in `crates/cli`) serializes any two forwards
whose *conflict-key sets* overlap — every inode an op reads or writes,
including parents (two creates in one directory both bump its
mtime/ctime; two `SetManifest`s on one file race each other's base; a
rename touches two parents and possibly the moved/replaced inodes).
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
- `pushed_segments_applied`: segment payloads applied directly without an S3
  fetch.
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

The same counters are exported as `constellation_speculation_*` metrics.
A rollback logs `segment from a later epoch stranded speculative state`
(or `takeover stranded speculative state`); a refused replay logs
`stranded op replay refused; materializing a conflict copy`.

Under a steady multi-writer workload, `forwarded_ok` should rise while
`handed the lease to a peer` should disappear. That log line means the older
handoff path ran; repeated occurrences indicate forwarding or placement is
not stable.

## Failure and fallback

Forwarding is optional. A timeout, empty reply, `Busy`, stale holder, disabled
P2P, or unreachable peer leaves the op in doubt and retries the same rid (see
above) before falling back to the normal lease-acquisition path. Handoff can
release a reachable holder immediately; otherwise the requester waits for
release or TTL expiry and claims through S3 CAS. Every path — same-holder
retry, redirected-holder retry, or the lease-acquisition fallback — resolves
to exactly one execution, never a repeat.

The default request timeout is 500 ms
(`CONSTELLATION_FORWARD_TIMEOUT_MS`). Setting `CONSTELLATION_FORWARD=off`
disables requester-side forwarding entirely: every non-holder mutation
takes the lease-acquisition path (P2P handoff, then S3 CAS) directly, with
no rid retries to attempt first, restoring writer-follows-lease placement.
A holder crash after `Accepted` strands the op in its unshipped journal;
the requester's speculation log rolls the shadow back and replays the op by
rid through the next holder (see above). The dead holder's own unshipped
journal is still resolved by reintegration until plan 30 M3b. Epoch
fencing prevents a competing append history.

## References

- [Lease placement](lease-placement.md)
- [Configuration](../configuration.md)
- [ADR-14](../../explanation/DECISIONS.md#adr-14-forward-mutations-to-the-lease-holder-instead-of-moving-the-lease)
- [DESIGN.md §4–6](../../explanation/DESIGN.md)
