# Enable P2P relays

Use this when some Constellation nodes cannot dial each other with the
addresses in the S3 registry alone — for example a laptop off-VPN trying
to reach EC2 instances that only have private IPs, while a VPN-connected
machine on the same LAN can.

For background and the full env-var table see
[P2P relays (reference)](../../reference/features/p2p-relays.md).

## Before you start

- P2P must be on (`CONSTELLATION_P2P` unset or not `off`).
- **Every** node that should participate sets the **same** relay policy
  before mount (remount after changes).
- Prefer relays only where needed; leave the default (disabled) for
  fleets that already share a VPC or VPN.

## Option A — n0 public relays

On each node:

```bash
export CONSTELLATION_P2P_RELAY=default   # aliases: public, n0
# then mount as usual
```

Nodes need outbound HTTPS to n0's relay hostnames. No self-hosted
infrastructure. Public relays are a shared blind forwarder; filesystem
isolation stays with the S3 registry (same as a self-hosted fleet
relay — see the [multi-tenancy notes](../../reference/features/p2p-relays.md#shared-relays-and-multi-tenancy)).

## Option B — self-hosted relays

1. Run one or more relays reachable from all participants — see
   [Run a self-hosted iroh relay](run-iroh-relay.md).
2. Point every node at those URLs:

```bash
export CONSTELLATION_P2P_RELAY='https://relay.example.com.'
# multiple:
# export CONSTELLATION_P2P_RELAY='https://relay-a.example.com.,https://relay-b.example.com.'
```

3. If the relay uses shared-token access:

```bash
export CONSTELLATION_P2P_RELAY_TOKEN='your-shared-secret'
```

One token shared by all tenants on that relay is fine: it only controls
relay admission, not which machines may join which filesystem.

## Verify

After remount:

```bash
constellation status --state-dir "$STATE" | jq .p2p.relay
# expect: "default", a URL, or "custom(N urls)" — not "disabled"
```

In the control UI, Peers detail shows `relay …`. Peer rows should move
to connected once dial/relay succeeds; coop transfers may show
`path: relay` when the QUIC path is relayed.

## Related

- [P2P relays reference](../../reference/features/p2p-relays.md)
- [Run a self-hosted iroh relay](run-iroh-relay.md)
