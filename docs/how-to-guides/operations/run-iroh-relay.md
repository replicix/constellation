# Run a self-hosted iroh relay

Operate one or more [iroh-relay](https://github.com/n0-computer/iroh)
servers so Constellation nodes can use
`CONSTELLATION_P2P_RELAY=<your-urls>` without n0's public relays.

This guide summarizes the upstream `iroh-relay` binary. Prefer the
version matching Constellation's `iroh` crate (currently 1.x).

## What you need

- A host (or container) reachable on HTTPS (production) or HTTP (dev)
  from every Constellation node that will use the relay.
- For production TLS: certificates (Let's Encrypt or manual).
- Optional: shared access tokens if you do not want an open relay.

## Build the server

From an iroh checkout (or crates.io release tags):

```bash
cargo build \
  --profile optimized-release \
  --package iroh-relay \
  --features server
# binary: target/optimized-release/iroh-relay
```

Docker images are also published upstream (`n0computer/iroh-relay`); see
`docker/README.md` in the iroh repository.

## Local / lab (`--dev`)

HTTP only, no QUIC address-discovery endpoint, default port **3340**:

```bash
iroh-relay --dev
# listen: http://localhost:3340  (or the host's reachable IP)
```

Point Constellation nodes at that URL:

```bash
export CONSTELLATION_P2P_RELAY='http://RELAY_HOST:3340'
```

Use this for laptop↔laptop or CI-style tests. Do not expose `--dev`
relays to the public internet.

## Production config (HTTPS)

Minimal TOML sketch (field names follow current `iroh-relay` config;
confirm against the binary's `--help` / upstream README for your tag):

```toml
# relay.toml
access = "everyone"
# Or lock it down:
# access.shared_token = ["long-random-token"]
# access.allowlist = ["<endpoint-id-hex>", "..."]

enable_quic_addr_discovery = true

[tls]
cert_mode = "LetsEncrypt"   # or Manual + cert/key paths
# …
```

Run:

```bash
iroh-relay --config-path /etc/iroh-relay/relay.toml
```

Typical ports (defaults; override in config as needed):

| Port | Role |
|---|---|
| 80 / 443 | HTTP→HTTPS / relay HTTPS |
| 3478/udp | STUN-style helpers (when enabled) |
| 7824/udp | QUIC address discovery (when enabled) |
| 9090 | Metrics (optional) |

Open inbound from client networks; Constellation clients only need
egress to the relay URL.

### Shared-token access

On the relay:

```toml
access.shared_token = ["fleet-secret"]
# Several secrets are allowed (OR for admission — not per-tenant overlays):
# access.shared_token = ["fleet-secret", "rotation-next"]
```

(or `IROH_RELAY_ACCESS_TOKEN` for a single token override)

On every Constellation node:

```bash
export CONSTELLATION_P2P_RELAY='https://relay.example.com.'
export CONSTELLATION_P2P_RELAY_TOKEN='fleet-secret'
```

Restart/remount nodes after changing tokens.

Tokens only gate **who may use the relay**. They do not create separate
routing domains: every admitted client shares one forwarder pool.
Constellation still refuses dials from nodes not in that filesystem's
S3 registry — the same model as n0's public relays. Giving every tenant
the same fleet token on one relay is therefore a normal multi-tenant
choice; use separate relay hosts/URLs only when you need capacity or
blast-radius isolation. See
[Shared relays and multi-tenancy](../../reference/features/p2p-relays.md#shared-relays-and-multi-tenancy).

## Multiple relays

List several URLs so clients can pick a working / nearer home relay:

```bash
export CONSTELLATION_P2P_RELAY='https://relay-eu.example.com.,https://relay-us.example.com.'
```

All participants should share the same list (and token).

## Checklist

- [ ] Relay process healthy; HTTPS answers on the configured URL
- [ ] Security groups / firewalls allow client → relay
- [ ] Every Constellation node has the same `CONSTELLATION_P2P_RELAY`
      (and token if used)
- [ ] `status` shows `p2p.relay` as your URL / `custom(N urls)`
- [ ] Off-VPN and private-IP nodes show connected peers

## Related

- [Enable P2P relays](enable-p2p-relays.md)
- [P2P relays reference](../../reference/features/p2p-relays.md)
- Upstream: `iroh-relay` README (access control, `--dev`, QUIC AD, Docker)
