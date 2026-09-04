# Forwarded mutations

Forwarded mutations let a non-holder ask the partition lease holder to
sequence a write over iroh. They reduce lease churn without changing S3
authority.

## Table of Contents

- [Terminology](#terminology)
- [Wire messages](#wire-messages)
- [Request flow](#request-flow)
- [Acknowledgement and durability](#acknowledgement-and-durability)
- [Status and logs](#status-and-logs)
- [Failure and fallback](#failure-and-fallback)
- [References](#references)

## Terminology

- **Holder / sequencer**: the node with the live S3 lease and the only node
  allowed to append the partition log.
- **Requester**: a non-holder sending an operation to the sequencer.
- **Shadow**: accepted records stored and applied locally by the requester
  before the corresponding S3 segment arrives.

## Wire messages

Both messages are signed iroh payloads. Mutation bodies use postcard encoding.

`MutateRequest` contains:

- `part`: target partition.
- `requester`: requester's node id.
- `req_id`: requester-local correlation id.
- `epoch_seen`: epoch hint; currently sent as `0`.
- `op`: encoded `MutateOp`.

`MutateReply` contains the matching `req_id` and an encoded `MutateOutcome`:

- `Accepted { epoch, records }`
- `Errno(errno)`
- `NotHolder { holder }`
- `Busy`

An empty or undecodable outcome is treated as `Busy`.

## Request flow

1. A mutating FUSE operation checks its partition lease view.
2. If the local node is holder, it validates and journals the operation
   locally.
3. Otherwise it reads the cached holder, or classifies the S3 lease, and sends
   `MutateRequest`.
4. The receiver verifies that its lease is usable, executes the mutation in
   one metadata transaction, and returns the journal records.
5. The requester inserts those records into its shadow table and applies them
   to its local replica.
6. The holder ships its journal to S3. A small segment may also be carried in
   the `SegmentPublished` gossip payload; larger segments are fetched from S3.

A `NotHolder` response updates the cached holder and permits one retry at the
redirected node.

## Acknowledgement and durability

`Accepted` means the holder committed the operation to its local journal. It
does **not** mean the partition segment is already on S3. The requester applies
the returned records so read-your-write does not wait for segment shipping.

Mounts using `--fsync-mode s3` add the existing inode/partition `Barrier`:
`fsync()` waits for dirty chunks and metadata through that point to reach S3.
The default `--fsync-mode local` only requires local durability and nudges the
background shipper.

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

## Status and logs

`constellation status` and the web UI expose:

- `forwarded_ok`: accepted forwarded requests made by this node.
- `forwarded_err`: timeouts, transport/decode failures, redirects, and
  non-accepted replies.
- `forward_p50_ms`: median of the last 256 successful forward latencies.
- `pushed_segments_applied`: segment payloads applied directly without an S3
  fetch.
- `placement_reason`: the last holder placement recommendation.

Under a steady multi-writer workload, `forwarded_ok` should rise while
`handed the lease to a peer` should disappear. That log line means the older
handoff path ran; repeated occurrences indicate forwarding or placement is
not stable.

## Failure and fallback

Forwarding is optional. A timeout, empty reply, `Busy`, stale holder, disabled
P2P, or unreachable peer falls back to the normal lease acquisition path.
Handoff can release a reachable holder immediately; otherwise the requester
waits for release or TTL expiry and claims through S3 CAS.

The default request timeout is 500 ms
(`CONSTELLATION_FORWARD_TIMEOUT_MS`). A holder crash after `Accepted` can
strand records in its journal. Epoch fencing prevents a competing append
history; reintegration reports and resolves the stranded branch.

## References

- [Lease placement](lease-placement.md)
- [Configuration](../configuration.md)
- [ADR-14](../../explanation/DECISIONS.md#adr-14-forward-mutations-to-the-lease-holder-instead-of-moving-the-lease)
- [DESIGN.md §4–6](../../explanation/DESIGN.md)
