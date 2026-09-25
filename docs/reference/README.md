# Reference

Technical descriptions of how specific parts of Constellation work.

## Features

- [P2P relays](features/p2p-relays.md) — relay modes, env vars, trust model, status fields
- [Forwarded mutations](features/forwarded-mutations.md) — wire protocol, exactly-once rids, speculation and stranded-op recovery, the S3 inbox, status
- [Lease placement](features/lease-placement.md) — subtree placement by dominant writer, root-lease weighted-medoid cost, thresholds, placement status
- [Delegations](features/delegations.md) — delegated sub-sequencers over one log: ownership, recall, placement, hash-range splits of hot directories
- [Close-to-open modes](features/cto-modes.md) — positions and session guarantees, direct log streams, `--cto bounded|strict`, ReadIndex and read delegations
- [Durability and failover](features/durability-and-failover.md) — acknowledgement layers, backups chosen by RTT, seal-based failover, `ack=s3`, `--fsync-mode`/`--write-mode`, flexible continuation epochs
- [Cluster locks](features/cluster-locks.md) — cross-node `flock`/`fcntl` as leased grants, `EIO` fencing, `--locks cluster|local`
- [Cooperative cache membership](features/cooperative-cache.md) — exact mirrors vs blooms, reconciliation protocol, false-positive accounting
- [Scratch directories](features/scratch-directories.md) — node-private staging and Publish semantics
- [Read-time atime](features/atime.md) — optional, batched, best-effort access-time updates
- [Remote support](features/remote-support.md) — compile-time-gated support sessions: modes, capability table, trust model, audit
- [Named filesystems](features/named-filesystems.md) — the local registry, one daemon per name, daemonization, `export`
- [Write-path hygiene](features/write-path-hygiene.md) — conditional-write error codes, `doctor`'s provider probes and versioning, held records and `repair drop-held`, holder-only publishing

## Project

- [Configuration](configuration.md) — mount flags, per-filesystem settings, runtime environment variables and defaults

## Tools

- [Chaos](tools/chaos.md) — multi-node FS consistency stress (CI + soak)
