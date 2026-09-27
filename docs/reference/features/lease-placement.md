# Lease placement

Placement moves sequencing toward the nodes that write. There are two
levels since plan 30:

- **Subtree placement** (plan 30 M11–M12, on by default): the root
  lease holder delegates a subtree, or a name-hash range of a hot
  directory, to the node that dominates its writes. That node then
  sequences it at local speed. This is the main mechanism; it is
  described in [Delegations](delegations.md#automatic-placement) and
  summarized [below](#subtree-placement).
- **Root lease placement** (ADR-15): the holder offers the root lease
  itself to the writer that minimizes the forwarding latency of
  everything still sequenced by the root.

## Table of Contents

- [Terminology](#terminology)
- [Subtree placement](#subtree-placement)
- [Root lease placement](#root-lease-placement)
  - [Cost function](#cost-function)
  - [Inputs and candidates](#inputs-and-candidates)
  - [Migration thresholds](#migration-thresholds)
  - [What stops the root lease from moving](#what-stops-the-root-lease-from-moving)
- [Configuration and status](#configuration-and-status)
- [Troubleshooting](#troubleshooting)
- [References](#references)

## Terminology

- **Root lease**: the one S3 lease; its holder appends the log and
  sequences everything not delegated.
- **Delegation**: a subtree or name-hash range sequenced by another node
  for the root (see [Delegations](delegations.md)).
- **Writer weight**: operations attributed to a node in the recent window.
- **Medoid**: an actual writer node minimizing weighted RTT to all writers.
- **Dwell**: minimum time between two placement moves.

## Subtree placement

The root sees every op's origin node (from its rid) and directory. Over a
sliding window (30 s by default) it:

- delegates the **topmost** directory below `/` that one node dominates
  (at least 70% of the subtree's ops, and at least 200 ops in the
  window) to that node, if it is connected, write-eligible and holds no
  other whole delegation;
- recalls a placed delegation when the delegate's share stays below 50%,
  or the subtree's rate below half the floor, for a 60 s dwell;
- splits a hot directory that no node dominates, but that several nodes
  each write at least 20% of, into name-hash ranges, giving each range
  to a node that dominates that range;
- keeps a recalled directory in a 30 s cool-down.

It never delegates the root directory and never recalls a manual
delegation or a designation. The inputs are op counts only, not RTT:
a dominant writer gains local-speed sequencing wherever it is. See
[Delegations — automatic placement](delegations.md#automatic-placement)
and [hot shared directories](delegations.md#hot-shared-directories).

## Root lease placement

### Cost function

The current holder evaluates each eligible candidate `c` with:

```text
cost(c) = Σ writer_ops(w) × rtt(c, w)
```

Writer counts are kept in twelve five-second buckets: a rolling window
of about 60 seconds. The holder counts the ops it accepted from each
requester and its own ops, both those that went through the authority
core and those its FUSE fast path executed (the driver drains a count
of the latter after every core event). The holder evaluates every 5 s.

### Inputs and candidates

Each node gossips `PeerRtts { node_id, rtts }`, where `rtts` is a vector of
`(peer_id, rtt_ms)` samples. The holder combines those vectors with recent
writer counts.

A migration candidate must:

- have written during the current window;
- have a direct iroh path from the current holder; and
- have RTT information to every weighted writer.

CPU load and S3 distance are not inputs. There is no election: the current
holder computes the recommendation and sends `LeaseOffer`; S3 CAS still
commits the holder change.

### Migration thresholds

Placement recommends a move only when:

- `best_cost < 0.7 × current_cost` (30% or greater improvement);
- the improvement is at least 5 ms of RTT-weighted cost in absolute
  terms, so co-located writers do not trade the lease on sub-millisecond
  jitter;
- the move also pays off over the last two buckets (5–10 s): the
  candidate wrote there, and its cost over them is below 0.7 × the
  holder's; and
- at least 60 seconds have elapsed since the previous successful migration.

The offered node claims the lease only if its own clients wrote in the
last 3 s, and a claim the holder declines does not add the node to the
lease's `wanted_by` (the holder offers again while the move still pays).
Without these, a minute of history could hand the lease from a holder
that had just started writing to a node whose burst had ended; that
node never wrote again, the holder's next close had to take the lease
back through S3, and every writer and reader stalled for two handoffs'
S3 round trips (3–5 s at 300 ms per request; `visibility-s3-latency`).

These thresholds prevent small RTT changes or short bursts from moving the
lease repeatedly.

### What stops the root lease from moving

The holder declines a handoff, and does not release the lease when idle,
while any delegation generation or cluster-lock grant is live: both are
capped by the root lease and would have to be recalled first. So a
`LeaseOffer` has no effect while delegations or lock grants exist.

## Configuration and status

Root lease placement is enabled by default. Disable it before mounting:

```bash
export CONSTELLATION_LEASE_PLACEMENT=off
```

`0` and `false` are equivalent. Forwarding remains enabled; only automatic
root lease offers stop. Subtree placement has its own switch,
`CONSTELLATION_DELEGATION_PLACEMENT`, and its thresholds are the
`CONSTELLATION_DELEGATION_*` knobs (see
[Configuration](../configuration.md#delegation-and-placement)).

`constellation status` exposes `placement_reason` for the root lease,
for example:

```text
migrate 3 -> 7: cost 2400 -> 900
```

The value is the most recent recommendation, not a guarantee that the offered
node completed its S3 lease claim. Subtree placement reports under
`delegation.placement` (the busiest subtrees as `(dir, node, node_ops,
subtree_ops)`) and the `delegation.place_*` counters.

## Troubleshooting

If the root lease never moves, check that the candidate has recent
writes, a direct (not relay) path, complete RTT samples, enough cost
improvement, and that the 60-second dwell has elapsed, and that no
delegation or lock grant is live.

If leases still move frequently with placement disabled, inspect
`handed the lease to a peer`; that is the requester's fallback handoff
path (a requester with no P2P path escalating from the S3 inbox, or a
lease-path fallback), not placement.

## References

- [Delegations](delegations.md)
- [Forwarded mutations](forwarded-mutations.md)
- [Diagnose lease thrash](../../how-to-guides/operations/diagnose-lease-thrash.md)
- [Configuration](../configuration.md)
- [ADR-15](../../explanation/DECISIONS.md#adr-15-holder-driven-placement-no-election),
  [ADR-23](../../explanation/DECISIONS.md#adr-23-delegated-sub-sequencers-over-one-log)
- [`crates/cli/src/placement.rs`](../../../crates/cli/src/placement.rs) (root lease),
  [`crates/authority/src/core/placement.rs`](../../../crates/authority/src/core/placement.rs) (subtrees)
