# Configuration

Runtime environment variables for metadata synchronization, leases, and the
P2P fast path. Set them before mounting; values are read by the daemon.

## Table of Contents

- [Environment variables](#environment-variables)
- [Boolean values](#boolean-values)
- [References](#references)

## Environment variables

| Variable | Default | Unit / values | Subsystem |
|---|---:|---|---|
| `CONSTELLATION_LEASE_TTL_MS` | `60000` | milliseconds, positive | S3 lease expiry and renewal |
| `CONSTELLATION_LEASE_IDLE_RELEASE_MS` | `30000` | milliseconds | release an idle held lease |
| `CONSTELLATION_FORWARD_TIMEOUT_MS` | `500` | milliseconds | forwarded mutation request |
| `CONSTELLATION_LEASE_PLACEMENT` | `on` | boolean | holder-driven placement |
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | milliseconds | background log tail/ship poll |
| `CONSTELLATION_PART_SPLIT_OPS` | `512` | operations, positive | automatic partition split |
| `CONSTELLATION_PART_MERGE_IDLE_S` | `3600` | seconds | automatic idle partition merge |
| `CONSTELLATION_CHECKPOINT_MIN_INTERVAL_S` | `0` | seconds | minimum checkpoint spacing; `0` disables the time floor |
| `CONSTELLATION_DIGEST_INTERVAL_S` | `30` | seconds, minimum `1` | cooperative-cache digest gossip |
| `CONSTELLATION_COOP` | `on` | boolean | cooperative cache |
| `CONSTELLATION_P2P` | `on` | boolean | complete iroh fast path |
| `CONSTELLATION_P2P_RELAY` | `off` | `off`, `default`/`public`/`n0`, or comma-separated relay URLs | iroh relay policy |
| `CONSTELLATION_P2P_RELAY_TOKEN` | unset | string | optional bearer token for custom relays |

`CONSTELLATION_P2P=off` disables forwarding, placement messages, segment
push, handoff acceleration, and cooperative peer transfer. Correctness and
eventual convergence continue through S3.

## Boolean values

Boolean switches are enabled when unset. `off`, `0`, and `false`
(case-insensitive) disable them.

## References

- [Forwarded mutations](features/forwarded-mutations.md)
- [Lease placement](features/lease-placement.md)
- [P2P relays](features/p2p-relays.md)
