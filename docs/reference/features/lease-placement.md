# Lease placement

Lease placement moves a partition's sequencer toward the recent writers that
would otherwise pay the most forwarding latency.

## Table of Contents

- [Terminology](#terminology)
- [Cost function](#cost-function)
- [Inputs and candidates](#inputs-and-candidates)
- [Migration thresholds](#migration-thresholds)
- [Configuration and status](#configuration-and-status)
- [Troubleshooting](#troubleshooting)
- [References](#references)

## Terminology

- **Writer weight**: operations attributed to a node in the recent window.
- **Medoid**: an actual writer node minimizing weighted RTT to all writers.
- **Dwell**: minimum time between successful migrations.

## Cost function

The current holder evaluates each eligible candidate `c` with:

```text
cost(c) = Σ writer_ops(w) × rtt(c, w)
```

Writer counts are retained in twelve five-second buckets: a rolling
approximately 60-second window. Local holder operations and accepted
forwarded operations both contribute.

## Inputs and candidates

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

## Migration thresholds

Placement recommends a move only when:

- `best_cost < 0.7 × current_cost` (30% or greater improvement); and
- at least 60 seconds have elapsed since the previous successful migration.

These thresholds prevent small RTT changes or short bursts from moving the
lease repeatedly.

## Configuration and status

Placement is enabled by default. Disable it before mounting:

```bash
export CONSTELLATION_LEASE_PLACEMENT=off
```

`0` and `false` are equivalent. Forwarding remains enabled; only automatic
placement offers stop.

`constellation status` exposes `placement_reason`, for example:

```text
migrate 3 -> 7: cost 2400 -> 900
```

The value is the most recent recommendation, not a guarantee that the offered
node completed its S3 lease claim.

## Troubleshooting

If placement never recommends a move, check that the candidate has recent
writes, a direct (not relay) path, complete RTT samples, enough cost
improvement, and that the 60-second dwell has elapsed.

If leases still move frequently with placement disabled, inspect
`handed the lease to a peer`; that is the requester's fallback handoff path,
not placement.

## References

- [Forwarded mutations](forwarded-mutations.md)
- [Diagnose lease thrash](../../how-to-guides/operations/diagnose-lease-thrash.md)
- [Configuration](../configuration.md)
- [ADR-15](../../explanation/DECISIONS.md#adr-15-holder-driven-placement-no-election)
