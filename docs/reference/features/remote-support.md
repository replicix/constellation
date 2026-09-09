# Remote support mode

> **Status: specified, not yet implemented.** This document is the
> reference for the design in
> [`docs/plans/v1/wip/23-remote-support-mode.md`](../../plans/v1/wip/23-remote-support-mode.md).
> It is written ahead of the code so the operator-facing contract is
> settled before the wire format is. Sections marked *planned* have no
> running code behind them yet.

Remote support mode lets a Constellation support engineer connect to one
running node over the existing P2P transport, inspect it, and — if the
node's operator chose read-write mode — repair it. It exists so a
problem that only reproduces on a user's cluster can be debugged without
VPN access, SSH, an inbound port, or a screen-share.

Two facts govern everything else:

- **The code is not in a normal build.** Remote support is behind the
  `remote-support` Cargo feature, off by default. A default binary has no
  support ALPN, no listener, no flags, and no dispatcher.
- **A session is armed by the mount command and ends with the process.**
  There is no timeout, no renewal, and no way to arm a session remotely.
  The operator's kill switch is stopping the support process and
  starting their normal binary.

## Contents

- [Builds](#builds)
- [Arming a node](#arming-a-node) — the `mount` flags
- [Modes](#modes) — `ro` vs `rw`, and what each permits
- [Capability table](#capability-table)
- [Trust model](#trust-model)
- [The ticket](#the-ticket)
- [Discovery across a cluster](#discovery-across-a-cluster)
- [Programmatic interface](#programmatic-interface) — scripts and AI agents
- [Audit log](#audit-log)
- [Status fields](#status-fields)
- [Extending this document](#extending-this-document)

## Builds

| Build | Feature | `--version` | Support flags |
|---|---|---|---|
| Release (default) | off | `1.x.y` | absent; `--support` fails to parse |
| Support | `remote-support` | `1.x.y (+remote-support)` | present |

```bash
cargo build --release --features remote-support
# or, for a distributable artifact:
make dist-support     # constellation-<version>-support-<target>
```

A support build is a normal build in every other respect: same version,
same on-disk formats, same cluster protocol. Nodes running a support
build interoperate with nodes running a release build, and a support
build with no `--support` flag behaves exactly like a release build plus
one WARN line at mount.

## Arming a node

Support is armed by adding flags to the usual `mount` command:

```
--support <ro|rw>                 # required to arm anything
--support-allow <pubkey-hex>      # required, repeatable
--support-no-data                 # optional narrowing
--support-max-sessions <n>        # default 1, cap 4
--support-advertise / --no-support-advertise    # default: advertise
```

`--support` without `--support-allow` is a parse error, and so is
`--support-allow` without `--support`. A mount can therefore never end
up armed for nobody, or armed for everybody, through a typo.

| Flag | Effect |
|---|---|
| `--support ro` | Read-only session. No request that changes any state is accepted. |
| `--support rw` | Read-write session. Everything the node itself can do to the filesystem and to its own local state. |
| `--support-allow <hex>` | Adds one client public key to the accept set. Exact match; no wildcards, no refresh, no fallback. |
| `--support-no-data` | Refuses reads of file **contents** inside the mounted namespace; in `rw`, also refuses namespace mutations. Filenames, sizes, and tree layout remain visible through the metadata replica and the logs — this flag is about contents, not about privacy in general. |
| `--support-max-sessions` | Concurrent support connections. Exists for two engineers on one node, not for fan-in. |
| `--support-advertise` | Publishes a support descriptor (mode, support pubkey, addresses) in this node's registry entry, so a client that reached one node can discover the others. Writing it requires bucket write permission, so the registry stays IAM-gated. |

## Modes

Mode is chosen per node, at mount, by the node's operator. A cluster
where one node is `rw` and the rest are `ro` — repair on the broken node,
observation everywhere else — is a normal and recommended configuration.

**`ro`** grants inspection: logs, status, counters, thread dumps,
read-only queries against the metadata replica, a consistent replica
snapshot, and reads of node-local files under the allowed roots. Nothing
in `ro` can append a log record, mutate the namespace, or change cluster
state — a property asserted by the `support-session-readonly` harness
scenario, not merely by code review.

**`rw`** grants everything `ro` does, plus parity with what the node
itself can do: mutating control-plane operations, namespace writes,
node-local config and cache repair, and a controlled remount or
shutdown. Two things are excluded from "parity", deliberately:

- **No shell and no arbitrary process execution.** The node cannot run
  commands on its host, so neither can a support session. This keeps a
  leaked support key a filesystem-scoped incident rather than a
  root-on-the-host one.
- **No out-of-band writes to the SQLite replica.** The replica is a
  projection of the shared log; a direct `UPDATE` would silently
  desynchronise this node from the cluster. The node never does this, so
  support does not either. Repairs go through log-producing control
  operations and `fsck --repair`.

Mode can be lowered on a running daemon and never raised:

```bash
constellation support off --state-dir "$STATE"   # rw → ro, then ro → disarmed
```

The transition applies to connections already open, not just to new
ones. Raising the mode, or adding a key, requires a remount — an
explicit act by the operator on their own machine.

## Capability table

Read this as the contract: a request not listed here does not exist, and
a request listed as `ro`-refused is refused by the server, not merely
hidden by the client.

| Request | `ro` | `rw` | What it does |
|---|---|---|---|
| `Hello` | ✓ | ✓ | Version, build flags, mode, node id, `no_data` flag. Mandatory first request. |
| `Status` | ✓ | ✓ | The same `StatusReport` the control socket serves. |
| `Metrics` | ✓ | ✓ | Prometheus text plus internal counters not exposed on `/metrics`. |
| `ProcInfo` | ✓ | ✓ | pid, uptime, RSS, thread and fd counts, tokio runtime metrics. |
| `ThreadDump` | ✓ | ✓ | Backtraces of all threads — the request you want when a mount is wedged. |
| `LogTail`, `LogFollow` | ✓ | ✓ | Bounded log ring and a live subscription. |
| `LogLevel` | ✓ | ✓ | Runtime `tracing` filter change; restored at session end. |
| `DbSchema`, `DbQuery`, `DbSnapshot` | ✓ | ✓ | Read-only replica access in **both** modes; snapshot via `VACUUM INTO`. |
| `ListDir`, `StatPath`, `ReadFile` | ✓ | ✓ | Node-local files under the allowed roots, minus the deny list. |
| `Control` (read-only subset) | ✓ | ✓ | Pass-through to the local control API. |
| `Roster` | ✓ | ✓ | Cluster members and which of them are armed. |
| `FsRead` | ✓* | ✓* | File contents inside the mounted namespace. `*` refused under `--support-no-data`. |
| `Control` (mutating) | ✗ | ✓ | pin/unpin, offline/online, write mode, quota, gc, `fsck --repair`, reintegrate, snapshot, clone, leave. |
| `FsWrite`, `FsCreate`, `FsUnlink`, `FsRename`, `FsSetAttr`, `FsSetXattr` | ✗ | ✓* | Namespace repair through the node's own code paths. |
| `WriteFile`, `RemoveFile` | ✗ | ✓ | Node-local config and cache-file repair, same roots and deny list. |
| `Remount`, `Shutdown` | ✗ | ✓ | The wedged-daemon escape. `Remount` re-arms with the same flags, never wider. |
| `Bye` | ✓ | ✓ | Clean close. |

Mutations run through the node's own internal operations. They respect
leases, delegations, quotas, and epochs, and they produce ordinary
`LogRecord`s — a support-driven write is indistinguishable downstream
from a local one, and inherits every invariant the system already
proves.

### Always refused, in both modes

- Reads of `node.key`, `*.key`, `*.pem`, `credentials*`, E2E keyring
  material, and anything the backend configuration marks secret — after
  canonicalisation, so symlink and `..` tricks do not reach them.
- Any path resolving outside the allowed roots (state dir, config dir,
  the daemon's log files).
- Writes to the metadata replica.
- Anything not in the capability table.

A denied request answers `Denied` with a reason. It never answers "not
found": a lie there would only send the engineer debugging a phantom.

## Trust model

| Threat | What stops it |
|---|---|
| Anyone, against a default build | The code is not in the binary. This is the only defense that survives a bug in our own code. |
| Anyone, against a support build mounted without `--support` | No listener is spawned. |
| A network attacker against an armed node | They must present an allowed client key; the QUIC/TLS handshake proves possession. Unknown keys are dropped at accept, with no registry lookup and no S3 traffic. |
| A compromised support key, node in `ro` | Read-only by construction, secret paths denied, everything audited and visible in `status`. |
| A compromised support key, node in `rw` | **Nothing prevents it.** `rw` is equivalent to handing the node over. What the design provides is that this was a deliberate, announced, revocable choice: a distinct mode the operator typed, shown in the banner and in `status`, logged per operation, endable by stopping the process, and lowerable without one. |

The registry allowlist that governs cluster peers
([P2P relays](p2p-relays.md), DESIGN §8) is **not** consulted for
support connections, and a support client never becomes a cluster peer:
it does not gossip, hold leases, or appear in any roster.

The support listener uses a fresh key generated at mount and never
written to disk. It is not the host node key, so accepting support says
nothing about cluster membership, and the ticket is worthless once the
process stops.

## The ticket

Arming a node prints a ticket — a `CST1`-prefixed base32 string carrying
the session's public key, direct addresses, relay URL, mode, node id,
and a hash of the filesystem UUID. It is the operator's to share, and it
is what a support engineer needs to connect.

A ticket is not a secret in the usual sense: possession alone grants
nothing, because the node still requires the connecting key to be in its
`--support-allow` set. It is, however, a description of the operator's
network, so treat it as sensitive and send it through the support case,
not a public channel.

Tickets are single-process. A remount produces a new one.

## Discovery across a cluster

A support client has no bucket credentials and cannot read the node
registry, so cluster discovery goes through a node it already reached:
`Roster` returns that node's registry view — members, roles, last-seen —
and, for each, the support descriptor it advertised or "not armed".

The roster is a phone book, not a capability. Connecting to a second
node succeeds only if that node was independently armed with the same
support key. No node can authorise support on another node's behalf, and
there is no cluster-wide grant.

## Audit log

`<state_dir>/support/audit.jsonl`, mode `0600`, append-only, one JSON
object per event:

```json
{"ts":"…","event":"request","session_id":"…","client_key":"…","mode":"rw",
 "request":"FsWrite","args_summary":"/data/x.parquet@0+4096","result":"ok",
 "bytes":4096,"log_seq":["p0:1841"]}
```

Mutating requests record the log-record sequence numbers they produced,
so an operator can reconcile every change a session made against their
own log. The file rotates to `.1` at a size cap and keeps two
generations; nothing truncates it silently. It survives the session and
the process — it is the operator's record, not ours.

## Status fields

`constellation status` and the control API gain:

```jsonc
"support": {
  "mode": "rw",                       // or "ro"; absent when disarmed
  "allowed_keys": ["ab12…", "cd34…"], // fingerprints
  "no_data": false,
  "connected": [
    {"client_key": "ab12…", "since": "…", "requests": 41, "bytes": 91234}
  ],
  "audit_path": "/var/lib/constellation/fs-a/support/audit.jsonl"
}
```

The web UI shows an armed node as a persistent banner coloured by mode.
An operator should never have to wonder whether a session is connected
right now.

## Programmatic interface

Everything a support session can do is reachable non-interactively, from
a script or an AI agent working under an engineer's supervision. It is
the same session and the same server-side enforcement — the programmatic
surface is a front end, not a second path.

### The local proxy

```bash
constellation support attach --ticket CST1… --socket /run/user/1000/case.sock --clamp ro
```

`attach` holds one P2P session open and re-exposes it on a local unix
socket speaking the **control-API line protocol** (one JSON request per
line, one JSON response per line — the same protocol a local daemon
serves). Any tool that already speaks to a Constellation control socket
therefore works against a remote node unchanged.

`--clamp ro|rw` is a **client-side ceiling**. Against a node the
operator armed `rw`, `--clamp ro` makes the proxy refuse to forward
mutating requests at all. It is not a security boundary — the node's
mode is, and the node enforces it regardless — it is how an agent is
given inspection rights on a node that a human could also repair.
`attach` defaults to `--clamp ro`.

### Calls

```bash
constellation support call --socket … status --json
constellation support call --socket … logs --lines 200 --grep lease --json
constellation support call --socket … db "select part, seq from log_head" --json
constellation support call --socket … raw '{"req":"ThreadDump"}' --json

printf '%s\n' '{"req":"Status"}' '{"req":"ProcInfo"}' \
  | constellation support call --socket … --batch --json
```

`--ticket` may be given to `call` directly when a persistent proxy is
not worth it.

### Output contract

| Rule | Detail |
|---|---|
| stdout is data | With `--json`, stdout carries exactly one JSON value per response. Banners, progress, and warnings go to stderr. |
| Every response is an envelope | `{"ok":true,"req":"Status","node":"3","mode":"ro","data":{…}}` or `{"ok":false,"req":"FsWrite","error":{"kind":"needs_rw","message":"…"}}`. |
| Errors are classified | `error.kind` ∈ `needs_rw`, `denied_path`, `not_found`, `unsupported`, `clamped`, `timeout`, `transport`, `busy`. Branch on `kind`, never on prose. |
| Bulk goes to files | `DbSnapshot`, `ReadFile`, `pull`, and unbounded `LogTail` write under `--out`; the envelope carries `{"path":…,"bytes":…,"blake3":…}`. |
| Truncation is announced | Bounded by `--lines`, `--grep`, `--limit`; a shortened response sets `"truncated":true` and reports how much was dropped. |
| Everything has a deadline | `--timeout`, default 30 s. A misbehaving node must surface as `kind:"timeout"`, not as a stalled caller. |

Exit codes: `0` ok, `1` request failed, `2` usage, `3` refused by mode
or clamp, `4` denied by the node's path or secret rules, `5` transport
or session lost, `6` timeout. A batch exits non-zero if any request
failed; `--stop-on-error` stops at the first.

### Capability discovery

```bash
constellation support capabilities --socket … --json
```

Returns the machine-readable form of the [capability
table](#capability-table): each request's name, mode requirement,
whether it mutates, whether it touches user data, its argument types,
and whether *this* node offers it given its version, `--support-no-data`
setting, and the active clamp. It is generated from the same exhaustive
dispatcher `match`, so it cannot drift from what the server accepts.

Callers are expected to read this and plan against it rather than
hardcoding a request set and discovering by trial that the node is old,
narrowed, or clamped.

### Mutations from a program

Three independent yeses are required, one of them the operator's:

1. the node was armed `rw` by its operator,
2. the proxy or call was started with `--clamp rw`,
3. the invocation passes `--allow-mutations`.

`--dry-run` validates and reports what a mutating request would do
without sending it. Every request and response is appended to an NDJSON
transcript under `--out`. The operator's audit log remains the
authority; the transcript is the support side's copy and should
reconcile with it line for line.

### MCP server (optional)

`constellation support mcp --socket …` exposes each capability as an MCP
tool over stdio, with mode, clamp, and `--support-no-data` reflected in
which tools are advertised at all — so a model's tool list is already
the truth about what it may do. Marked optional in
[plan 23](../../plans/v1/wip/23-remote-support-mode.md); it may ship
after the rest.

### What this changes for the operator

Nothing about the guarantees. Automated requests arrive over the same
session, are checked against the same mode, and appear in the same audit
log, one entry each. A node in `ro` cannot be changed by an agent any
more than by a human; a clamp can only narrow what the operator already
granted, never widen it.

## Extending this document

New capabilities are added here **before** they are added to the code,
because this table is the contract an operator consents to. When you add
a request:

1. Add a row to [Capability table](#capability-table) with its mode
   column filled in for both modes, and a one-line description written
   from the operator's point of view ("what can they see or change"),
   not the implementation's.
2. If it is refused in `ro`, say so explicitly rather than leaving the
   cell empty — a blank cell reads as an oversight.
3. If it can reach user data, mark it `*` and add it to the
   `--support-no-data` description above.
4. If it can reach secrets, it does not get a row; it gets an entry in
   [Always refused](#always-refused-in-both-modes).
5. Add the matching arm to the dispatcher's exhaustive `match` (there is
   no `_` arm, so the compiler will insist) and the matching case to the
   mode table-test.
6. Note it in the two how-to guides if it changes what either side
   actually types.

## Related

- [Enable remote support](../../how-to-guides/operations/enable-remote-support.md) — operator side, task by task
- [Run a support session](../../how-to-guides/development/run-a-support-session.md) — support-engineer side
- [P2P relays](p2p-relays.md) — the transport this rides on, and the registry allowlist it does *not* use
- [Plan 23](../../plans/v1/wip/23-remote-support-mode.md) — the implementation plan
