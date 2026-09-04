# P2P relays

Optional iroh relay modes for Constellation's P2P fast path. Relays help
peers connect when published direct addresses are not mutually reachable
(NAT, private VPC IPs vs off-VPN clients). They never replace the S3 node
registry, and they are never required for filesystem correctness.

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [Modes](#modes)
  - [Environment variables](#environment-variables)
  - [What the registry publishes](#what-the-registry-publishes)
  - [Trust model](#trust-model)
  - [Shared relays and multi-tenancy](#shared-relays-and-multi-tenancy)
  - [Status surface](#status-surface)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Direct path**: QUIC between two endpoints using IP addresses from the
  registry (LAN, VPN, or public IPs).
- **Relay path**: encrypted traffic temporarily forwarded by an iroh
  relay until a direct path works, or for the whole session if hole
  punching never succeeds.
- **Home relay**: the relay URL an endpoint registers with; included in
  the published `EndpointAddr` when relays are enabled.
- **Registry**: `nodes/<id>.json` objects in the filesystem bucket; the
  only peer directory and allowlist source.

## Details

### Modes

| Mode | When | Behaviour |
|---|---|---|
| **Disabled** (default) | `CONSTELLATION_P2P_RELAY` unset / `off` | `RelayMode::Disabled`. Dial only registry addresses. |
| **Public (n0)** | `default` / `public` / `n0` | iroh production relays (`RelayMode::Default`). |
| **Custom** | one or more relay URLs | `RelayMode::Custom` pointing at your relays (or a chosen subset). |

Every node that should talk to the others over relays must use the **same
mode** (same public map, or the same custom URL list). Mixed fleets where
some nodes disable relays and others enable them will only connect when
direct addresses happen to work.

### Environment variables

| Variable | Purpose |
|---|---|
| `CONSTELLATION_P2P` | Kill switch for the whole P2P stack (`off` / `0` / `false`). Independent of relays. |
| `CONSTELLATION_P2P_RELAY` | Relay policy (see modes above). Comma-separated URLs for custom maps. |
| `CONSTELLATION_P2P_RELAY_TOKEN` | Optional shared bearer token for custom relays that use `access.shared_token`. Ignored for disabled/public modes. |

Implementation: `constellation_net::RelayPolicy` → iroh `RelayMode` at
endpoint bind (`crates/net/src/{relay,endpoint}.rs`).

### What the registry publishes

On mount, each node publishes `endpoint.addr()` into `nodes/<id>.json`
(`p2p_addr`). With relays enabled that address includes the home relay
URL in addition to any local IPs. Peers inject those records into iroh via
`MemoryLookup` — there is still **no** global pkarr/DNS publishing.

### Trust model

- **Allowlist**: only registry-enrolled pubkeys are accepted on the direct
  ALPN. A relay cannot introduce an unenrolled peer.
- **Signed messages**: gossip payloads are signed by the author key; a
  relay cannot impersonate a node.
- **End-to-end crypto**: the relay forwards opaque QUIC; it cannot read
  or forge Constellation payloads.
- **S3 remains truth**: leases, logs, and chunks commit on S3. Relays
  affect latency and reachability only.
- **Relay tokens gate admission, not membership**: a shared bearer token
  (or n0 public access) only decides who may *use the relay*. Who may
  join a filesystem is still registry enrollment + bucket IAM.

### Shared relays and multi-tenancy

Running **one** self-hosted relay for many Constellation filesystems
(tenants) is the same security shape as using n0's public relays: the
relay is a shared blind forwarder; isolation between tenants is
**client-side**.

| Concern | Who enforces it |
|---|---|
| Join this filesystem / dial its ALPN | S3 node registry allowlist |
| Impersonate a node / rewrite gossip | Endpoint keys + signed messages |
| Read relayed bytes | End-to-end encryption (relay is blind) |
| Enroll a rogue node id | Bucket write IAM |
| Use the relay at all | Relay `access` (open, shared token(s), allowlist, …) |
| Capacity / noisy neighbors | Ops (separate relays or hosts if needed) |

**Recommended default for multi-tenant ops:** one fleet relay (optionally
one shared token for all tenants) and rely on per-filesystem registries.
That matches trusting n0's public map, with a smaller admitted client set
when a token is set.

**What a shared token does *not* do:** create per-tenant overlays. Clients
that present any configured token enter the **same** forwarder pool.
Knowing another admitted endpoint's id is enough to *ask* the relay to
forward; the remote Constellation node still refuses anyone not in its
registry.

**Multiple tokens** (`access.shared_token = ["a", "b"]`) are an OR-list for
admission (rotation, several secrets). They are **not** tenant namespaces.
For stronger separation (capacity, blast radius, or refusing foreign
endpoints at the relay), run a **separate relay URL (or host) per tenant**,
or use a custom upstream `AccessControl` / HTTP auth callout — stock
shared-token mode will not partition routing.

### Status surface

`StatusReport.p2p.relay` is a short label: `disabled`, `default`, a single
custom URL, or `custom(N urls)`. The peers UI shows it next to the P2P
enabled flag. Per-peer coop stats may report `path: relay` once a transfer
actually used a relayed QUIC path.

## Troubleshooting

### Peers stay disconnected with relays enabled

- Confirm **every** node has the same `CONSTELLATION_P2P_RELAY` value
  (and the same token if used).
- Confirm nodes can reach the relay URL (HTTPS/HTTP) from their network —
  EC2 security groups and laptop firewalls must allow outbound to the
  relay; self-hosted relays need inbound from both sides.
- Inspect published `p2p.node_addr` / peer dial addrs: a home relay URL
  should appear when relays are on.
- Remount after changing the env var (policy is fixed at bind time).

### Public relays work from the office but not from a locked-down VPC

Outbound HTTPS to n0 relay hostnames may be blocked. Use a
[self-hosted relay](../../how-to-guides/operations/run-iroh-relay.md)
inside a network both sides can reach, or open egress to the n0 relay
FQDNs.

### High err% with low hit% on distant peers

Relays make the path *possible*, not necessarily fast. Chunk request
timeouts and LAN-sized selector priors can still fail WAN transfers —
see coop source selection docs / status `path` and RTT.

## FAQ

- **Do I need relays on a VPC-only fleet?** No. Leave the default
  (disabled) if every node can route to every published address.
- **Does enabling relays publish my nodes to the public internet
  directory?** No. Constellation still does not use iroh's pkarr/DNS
  address lookup publishers. Only enrolled peers that read your bucket
  learn dial info.
- **Can one node use public relays and another a private relay?** Not
  usefully — they must share a map. Prefer one policy per filesystem.
- **Are relays required for correctness?** No. `CONSTELLATION_P2P=off` or
  unreachable peers fall back to S3.
- **Is one shared relay safe for many tenants (like n0's public
  relays)?** Yes for confidentiality and filesystem membership: isolation
  is the registry allowlist + E2E crypto, not the relay. Expect possible
  noisy-neighbor load on a shared host.
- **Does a shared `CONSTELLATION_P2P_RELAY_TOKEN` isolate tenants?** No.
  It only restricts who may use the relay. Give every tenant the same
  token if you want one locked-down fleet relay; membership isolation is
  unchanged.
- **Does iroh support multiple relay tokens for multi-tenancy?** The
  server can list several shared tokens, but any of them admits a client
  into one pool. Use separate relays (or custom access control) if you
  need tenant-partitioned admission or capacity.

## References

- [Enable P2P relays](../../how-to-guides/operations/enable-p2p-relays.md)
- [Run a self-hosted iroh relay](../../how-to-guides/operations/run-iroh-relay.md)
- [ADR-13](../../explanation/DECISIONS.md#adr-13-optional-iroh-relays-default-remains-registry-direct)
- [DESIGN.md §8](../../explanation/DESIGN.md) (security / registry)
- [iroh relay overview](https://www.iroh.computer/docs/concepts/relay)
- Upstream `iroh-relay` README (server binary, access control, `--dev`)
