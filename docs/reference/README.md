# Reference

Technical descriptions of how specific parts of Constellation work.

## Features

- [P2P relays](features/p2p-relays.md) — relay modes, env vars, trust model, status fields
- [Forwarded mutations](features/forwarded-mutations.md) — wire protocol, ack semantics, fallback, status
- [Lease placement](features/lease-placement.md) — weighted-medoid cost, thresholds, placement status
- [Scratch directories](features/scratch-directories.md) — node-private staging and Publish semantics
- [Read-time atime](features/atime.md) — optional, batched, best-effort access-time updates
- [Remote support](features/remote-support.md) — compile-time-gated support sessions: modes, capability table, trust model, audit
- [Named filesystems](features/named-filesystems.md) — the local registry, one daemon per name, daemonization, `export`

## Project

- [Configuration](configuration.md) — runtime environment variables and defaults

## Tools

- [Chaos](tools/chaos.md) — multi-node FS consistency stress (CI + soak)
