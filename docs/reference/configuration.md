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
| `CONSTELLATION_FORWARD` | `on` | boolean | requester-side mutation forwarding; `off` makes non-holder writes acquire the lease instead |
| `CONSTELLATION_LEASE_PLACEMENT` | `on` | boolean | holder-driven placement |
| `CONSTELLATION_SYNC_INTERVAL_MS` | `500` | milliseconds | background log tail/ship poll |
| `CONSTELLATION_PART_SPLIT_OPS` | `512` | operations, positive | heat threshold used only when autosplit is on |
| `CONSTELLATION_PART_AUTOSPLIT` | `off` | boolean | heat-driven automatic partition split |
| `CONSTELLATION_PART_MERGE_IDLE_S` | `3600` | seconds | automatic idle partition merge |
| `CONSTELLATION_CHECKPOINT_MIN_INTERVAL_S` | `0` | seconds | minimum checkpoint spacing; `0` disables the time floor |
| `CONSTELLATION_DIGEST_INTERVAL_S` | `30` | seconds, minimum `1` | cooperative-cache digest gossip |
| `CONSTELLATION_COOP` | `on` | boolean | cooperative cache |
| `CONSTELLATION_P2P` | `on` | boolean | complete iroh fast path |
| `CONSTELLATION_P2P_RELAY` | `off` | `off`, `default`/`public`/`n0`, or comma-separated relay URLs | iroh relay policy |
| `CONSTELLATION_P2P_RELAY_TOKEN` | unset | string | optional bearer token for custom relays |
| `CONSTELLATION_PREFETCH_MIN_BYTES` | `8388608` | bytes, positive | initial adaptive sequential-read window |
| `CONSTELLATION_PREFETCH_MAX_BYTES` | `268435456` | bytes, positive | window ceiling, additionally capped at one quarter of cache budget |
| `CONSTELLATION_PREFETCH_CONCURRENCY` | unset | requests, positive | pin background-fetch concurrency instead of adapting it |
| `CONSTELLATION_PREFETCH_MAX_CONCURRENCY` | `128` | requests, `1..128` | adaptive background-fetch ceiling |
| `CONSTELLATION_SCAN_AHEAD` | `on` | boolean | ordered directory-walk readahead |

`CONSTELLATION_P2P=off` disables forwarding, placement messages, segment
push, handoff acceleration, and cooperative peer transfer. Correctness and
eventual convergence continue through S3.

`CONSTELLATION_PART_AUTOSPLIT` is off unless set to `on`, `1`, or `true`.
When it is off, directory heat does not carve new partitions; idle
children still merge under `CONSTELLATION_PART_MERGE_IDLE_S`. Split/merge
machinery remains for opt-in and for filesystems that already split.
Heat was a proxy for lease contention from before forwarded mutations;
a single-writer tree walk (rsync, unpack) otherwise splits every hot
directory and leaves extra streams to LIST and leases to renew.

## Boolean values

Most boolean switches are enabled when unset. `CONSTELLATION_PART_AUTOSPLIT`
and `CONSTELLATION_P2P_RELAY` default off. `off`, `0`, and `false`
(case-insensitive) disable a switch that defaults on.

## References

- [Forwarded mutations](features/forwarded-mutations.md)
- [Lease placement](features/lease-placement.md)
- [P2P relays](features/p2p-relays.md)
