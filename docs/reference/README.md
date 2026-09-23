# Reference

Technical descriptions of how specific parts of Constellation work.

## Features

- [P2P relays](features/p2p-relays.md) — relay modes, env vars, trust model, status fields
- [Forwarded mutations](features/forwarded-mutations.md) — wire protocol, ack semantics, fallback, status
- [Lease placement](features/lease-placement.md) — weighted-medoid cost, thresholds, placement status
- [Cooperative cache membership](features/cooperative-cache.md) — exact mirrors vs blooms, reconciliation protocol, false-positive accounting
- [Scratch directories](features/scratch-directories.md) — node-private staging and Publish semantics
- [Read-time atime](features/atime.md) — optional, batched, best-effort access-time updates
- [Remote support](features/remote-support.md) — compile-time-gated support sessions: modes, capability table, trust model, audit
- [Named filesystems](features/named-filesystems.md) — the local registry, one daemon per name, daemonization, `export`
- [Write-path hygiene](features/write-path-hygiene.md) — conditional-write error codes, `doctor`'s provider probes and versioning, held records and `repair drop-held`, holder-only publishing

## Project

- [Configuration](configuration.md) — runtime environment variables and defaults

## Tools

- [Chaos](tools/chaos.md) — multi-node FS consistency stress (CI + soak)
