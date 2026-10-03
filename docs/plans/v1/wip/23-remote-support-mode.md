# Plan 23 — Remote support mode (compile-time optional, mount-armed)

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §8 (P2P security, node registry as trust
root), §10 (control plane). Code context: `crates/net/src/endpoint.rs`
(the iroh endpoint and its accept loop), `crates/net/src/identity.rs`
(host node key), `crates/net/src/allowlist.rs` (the *registry* allowlist
— deliberately **not** reused here), `crates/net/src/peers.rs` +
`crates/store-s3` nodes (the registry entries a node publishes),
`crates/api/src/lib.rs` (control socket and its request set),
`crates/cli/src/log_buffer.rs` (in-memory log ring),
`crates/cli/src/main.rs:57` (the `mount` flag set this plan extends).
Independent of plans 19–22; may be executed in any order relative to
them.

## Goal

When a user hits a problem we cannot reproduce, they restart the
affected mount(s) with a **support build** and two extra flags. Each such
node then accepts a P2P connection from exactly the support key they
were given, and we can inspect — or, if they chose `rw`, repair — that
running node with the same reach the node itself has.

Three properties carry the design:

- **Absent unless compiled in.** The feature is behind a Cargo feature
  that is off by default. In a default build there is no support ALPN,
  no accept path, no dispatcher, no flag — nothing to reach, nothing to
  fuzz, nothing to CVE. A user who needs debugging is handed a *support
  build*: same source, same version, feature on.
- **Armed by the mount, ended by the process.** Support is a `mount`
  flag, so the session is the daemon: it inspects the live process that
  actually has the problem — its log ring, its counters, its lease and
  epoch state, its wedged threads. There is **no TTL and no expiry**.
  The user's kill switch is the one they already understand and fully
  control: stop the support process, start the normal binary. A timer
  would only add a way for a session to die mid-diagnosis.
- **Two modes, chosen by the user at mount: `ro` and `rw`.** `ro` is
  strictly non-mutating. `rw` grants everything the node itself can do —
  we are not limited to describing the fix, we can apply it. Escalation
  from `ro` to `rw` is a remount, i.e. an explicit human act on the
  user's side. De-escalation without a remount is possible; escalation
  is not.

Non-goals: remote shell or arbitrary command execution; a support agent
that phones home or survives a restart; unattended access; our own NAT
infrastructure beyond iroh's existing relay support; support access
granted by anything other than the user typing a key on a command line.

## Settled decisions

Taken from the design discussion; do not relitigate them.

- **Feature name `remote-support`, default off**, on `constellation-net`
  and `constellation`, the latter forwarding to the former. Every line
  lives behind `#[cfg(feature = "remote-support")]`, including the `mod`
  declarations and the clap flags themselves.
- **Armed at mount, never at runtime.** `--support <ro|rw>` plus one or
  more `--support-allow <pubkey-hex>`. No flag, no listener. There is no
  control request that turns support *on*: the arming decision is made
  by whoever starts the process, which is the same authority that chose
  to run a support build at all.
- **Separate ALPN, separate allowlist.** `constellation-support/1`,
  never the cluster ALPN (`constellation/3`, `crates/net/src/message.rs`); the registry allowlist
  (`crates/net/src/allowlist.rs`) is untouched and uninvolved. A support
  client is not a cluster peer and must never become one by accident —
  in particular, it does not gossip, does not hold leases, and does not
  appear in any roster.
- **Session-scoped ephemeral server identity.** The support listener
  generates a fresh Ed25519 key at mount and never writes it to disk;
  the host node key from `~/.config/constellation/node.key` is neither
  exposed nor reused. The ticket therefore dies with the process, which
  is exactly the lifetime the user was promised.
- **The client key is long-lived and ours.** We publish support-engineer
  public keys; the user passes them with `--support-allow`. Exact-set
  match, no refresh, no wildcard, no fallback.
- **`rw` means node parity, not host parity.** In `rw` a support session
  can do anything the Constellation node can do to the filesystem and to
  its own local state. It cannot do things the *node* cannot do: no
  shell, no arbitrary process spawning, no reading outside the node's
  own directories. That boundary is what keeps a leaked support key a
  filesystem-scoped incident rather than a root-on-the-host one, and it
  costs us nothing — every repair we would reach for is a node
  operation.
- **Never write the SQLite replica out of band, in either mode.** The
  replica is a projection of the shared log; a direct `UPDATE` breaks
  convergent replay and desynchronises this node from the cluster
  silently. The node itself never does this, so "parity with the node"
  excludes it. Repairs go through log-producing control operations and
  `fsck --repair`, which is the codebase's existing, tested repair path.
  `DbQuery` stays read-only in `rw` too.
- **Cluster-wide by composition.** A cluster is debugged by arming
  several nodes and opening a session to each; the client can enumerate
  and fan out, but there is no cluster-wide grant and no node that can
  authorise support on another node's behalf.
- **Everything is audited locally**, on the user's disk, in both modes,
  and the audit survives the session.

## Threat model (state it, then design to it)

1. **Anyone at all, against a default build.** The code is not in the
   binary. The only defense that cannot be undermined by a bug in our
   own code, which is why it is the headline property.
2. **A network attacker against a support build mounted without the
   flags.** No listener exists; the accept task is never spawned.
3. **A network attacker against an armed node.** They must present the
   exact allowed client key; QUIC's TLS handshake is what proves
   possession. Unknown keys are dropped at accept with no lookup and no
   S3 traffic — the cluster allowlist's refresh-on-miss behaviour is an
   amplification path that must **not** be copied here.
4. **A compromised support key against an `ro` node.** Read-only by
   construction (Step 5), secret paths denied, everything audited, the
   session visible in `status` and the web UI while it runs.
5. **A compromised support key against an `rw` node.** This is real, and
   the honest answer is: `rw` is equivalent to handing us the node. The
   design's job is to make that a *deliberate, visible, revocable*
   choice — a distinct mode the user typed, announced in the banner and
   in `status`, logged per operation, endable by stopping the process,
   and de-escalatable to `ro` without one. The docs say this plainly
   rather than implying that scoping makes `rw` safe.
6. **A support build left running in production.** It identifies itself
   in `--version`, in a startup WARN, in `status`, and — when armed —
   in a persistent web UI banner naming the mode. We do not try to make
   support builds refuse to run; that only invites a patched binary.

## Step 1 — The compile-time gate

`crates/net/Cargo.toml`, `crates/cli/Cargo.toml`:

```toml
[features]
default = []
remote-support = []
```

with `constellation`'s `remote-support` forwarding to
`constellation-net/remote-support`.

- `crates/net/src/lib.rs`: `#[cfg(feature = "remote-support")] pub mod
  support;` — ALPN constant, message enum, dispatcher, all inside.
- `crates/cli/src/main.rs`: the `--support` / `--support-allow` args on
  `Mount`, the `Support { .. }` command (client side, Step 6), and their
  handlers are `#[cfg(feature = "remote-support")]`. **Do not** add a
  stub arm that prints "not supported": a stub puts the strings in every
  shipped binary and tells an attacker exactly what to look for. A
  default build rejects `--support` with clap's ordinary unknown-argument
  error.
- Packaging: `make dist-support` and a documented
  `cargo build --release --features remote-support`, producing artifacts
  named `constellation-<version>-support-<target>` so a support build is
  never mistaken for a release build on disk.
- `--version` prints `<version> (+remote-support)` on a support build.
  This is the one place advertising the capability is right: the user
  must be able to tell at a glance which binary they are running.

Acceptance is a test, not an inspection: `tests/support-build-gate.sh`
builds without the feature and asserts, via `strings` on the release
binary, the absence of `constellation-support/1`, `--support-allow`, and
a sentinel internal symbol; then builds with the feature and asserts all
three are present. Nightly CI, next to plan 19's `tests/binary-hygiene.sh`
if that exists.

## Step 2 — Arming: the mount flags

```
constellation mount --s3 ... /mnt/x \
    --support ro|rw \
    --support-allow <client-pubkey-hex> [--support-allow <hex>...] \
    [--support-no-data] \
    [--support-max-sessions 1] \
    [--support-advertise|--no-support-advertise]
```

- `--support` requires at least one `--support-allow` (clap `requires`),
  and `--support-allow` without `--support` is an error. A mount cannot
  end up armed for nobody or open to everybody through a typo.
- `--support-no-data` narrows either mode: the session may not read or
  write the **contents** of files inside the mounted namespace. Document
  it honestly — filenames, sizes, and layout are still visible through
  the replica and the logs, so this flag is about contents, not about
  privacy in general. In `rw`, it also disables namespace mutations,
  leaving control-plane repair only.
- `--support-max-sessions` defaults to 1; cap 4. Concurrent sessions
  exist for the case where two of us look at one node, not for fan-in.
- `--support-advertise` (default on) publishes a small support
  descriptor in this node's registry entry: `{support: {mode, pubkey,
  addrs}}`. It is how a client that reached *one* node learns which
  other cluster nodes are armed (Step 7) — the client has no bucket
  credentials and cannot read the registry itself. Writing it needs
  bucket write permission, so the registry stays IAM-gated as ever;
  `--no-support-advertise` opts out for users who do not want the fact
  recorded in the bucket.

At mount, the daemon prints — and this block is what the user pastes to
us — and repeats it into the log:

```
Constellation remote support is ARMED  (support build 1.x.y)
  mode        : rw   ← this session may modify data and cluster state
  allowed     : <client pubkey hex>
  session key : <ephemeral pubkey hex>
  node        : <node id>  fs <uuid>
  audit log   : /var/lib/constellation/fs-a/support/audit.jsonl

  ticket: CST1<base32(postcard{ephemeral pubkey, direct addrs, relay url, mode, node id, fs-uuid-hash})>

Support ends when this process stops. Run `constellation support off
--state-dir …` to drop to read-only, or `--support-allow` nothing and
remount to disarm.
```

While a session is connected the daemon prints one line per served
request to the console. The live view of us working is worth more to a
user's trust than any amount of policy text — and in `rw` it is the only
real-time check they have.

Invariants:

- The support endpoint binds its own iroh endpoint with only the support
  ALPN, **never joins gossip**, and never dials cluster peers.
- Accept compares the peer key against the `--support-allow` set and
  nothing else. Misses are closed, counted, logged, and rate-limited.
- The ephemeral key exists in memory only.
- The accept loop and the log/status/thread-dump handlers run on their
  own tokio task and must not depend on the FUSE worker threads or on
  the metadata lock. When a mount is wedged — the case we are most often
  called for — `logs`, `status`, `proc`, and `threads` must still
  answer, even while namespace requests block. Add a regression test that
  holds the metadata lock and asserts those four still respond.

## Step 3 — De-escalation without a remount

One control request, `SupportDowngrade`, reachable from the local
control socket (`constellation support off --state-dir <p>`), and
**monotone**: `rw` → `ro`, `ro` → disarmed. It can never raise the mode
or add a key. Rationale: the user's stop-the-process kill switch is
correct but coarse — it unmounts a live filesystem. Someone who granted
`rw`, watched the repair land, and now wants us out of write reach
should not have to disrupt their workload to get it. Because the
transition only ever narrows, exposing it costs nothing: a compromised
control socket could already do far worse than turn support off.

Downgrade takes effect on in-flight connections, not only on new ones:
mode is consulted per request.

## Step 4 — Wire protocol

New feature-gated module `crates/net/src/support/`: `mod.rs` (ALPN,
version negotiation, session), `proto.rs` (postcard `SupportRequest` /
`SupportResponse`), `mode.rs` (mode + capability check), `server.rs`
(accept, dispatch, audit), `client.rs` (dial, request helpers,
streaming).

Framing: one request per QUIC bi-stream, a single length-prefixed
postcard frame capped at 64 KiB (same reasoning as
`crates/net/src/message.rs`). Responses may stream: a header frame, then
body frames up to 1 MiB, then an end marker with a total-byte count and
a BLAKE3 digest, so truncation is detectable rather than silently short.
`Hello` must be the first request; anything else closes the connection.
Version mismatch is a warning, never a hard failure — a session whose
whole purpose is diagnosing an old build must still connect — and
unknown variants answer `Unsupported { name }` so the client degrades.

| Request | `ro` | `rw` | Notes |
|---|---|---|---|
| `Hello { client_version }` | ✓ | ✓ | returns version, build flags, mode, node id, `no_data` flag |
| `Status`, `Metrics`, `ProcInfo`, `ThreadDump` | ✓ | ✓ | `StatusReport`, Prometheus text plus internal counters, pid/RSS/fds/tokio metrics, all-thread backtraces |
| `LogTail { lines }`, `LogFollow` | ✓ | ✓ | `LogBuffer::tail` and a live subscription |
| `LogLevel { directive }` | ✓ | ✓ | runtime `tracing` filter reload; restores at session end |
| `DbSchema`, `DbQuery { sql, limit }`, `DbSnapshot` | ✓ | ✓ | read-only in both modes (see Settled decisions); snapshot via `VACUUM INTO`, streamed |
| `ListDir`, `StatPath`, `ReadFile` | ✓ | ✓ | node-local files under the allowed roots, minus the deny list |
| `Control { request }` — read-only subset | ✓ | ✓ | pass-through to the local control API |
| `FsRead { path, offset, len }` | ✓* | ✓* | user data; `*` = refused under `--support-no-data` |
| `Control { request }` — mutating | ✗ | ✓ | pin/unpin, offline/online, write mode, quota, gc, fsck --repair, reintegrate, snapshot, clone, leave |
| `FsWrite`, `FsCreate`, `FsUnlink`, `FsRename`, `FsSetAttr`, `FsSetXattr` | ✗ | ✓* | namespace repair through the node's own paths |
| `WriteFile`, `RemoveFile` (node-local) | ✗ | ✓ | config and cache-file repair, under the same roots and deny list |
| `Remount`, `Shutdown` | ✗ | ✓ | the wedged-daemon escape; `Remount` re-arms with the *same* flags, never wider |
| `Roster` | ✓ | ✓ | cluster members plus which are armed (Step 7) |
| `Bye` | ✓ | ✓ | clean close |

Every variant is classified in one `match` with no `_` arm, so adding a
request without deciding its mode fails to compile.

## Step 5 — What the modes actually enforce

Enforcement is server-side; the client's UI is not a control.

**Mode check first, on every request**, from the *current* mode (Step 3
can lower it mid-session). A mutating request in `ro` returns `Denied
{ reason: NeedsRw }` — never a generic error, because the engineer must
be able to tell "your user chose ro" from "this build is old".

**Path scoping** for node-local file access, reads and writes alike:
resolve, follow symlinks, canonicalise, then require the result inside
an allowed root (state dir, config dir, the daemon's log files). On top,
a **deny list** applied after canonicalisation: `node.key`, `*.key`,
`*.pem`, `credentials*`, E2E keyring material, and anything the backend
config marks secret. Denied paths return `Denied`, never "not found" — a
lie here just burns the engineer's time. The deny list applies in `rw`
too: parity with the node does not include exfiltrating the key that
*is* the node's identity.

**SQL restriction.** `DbQuery` opens the replica on a **second,
read-only connection** (`mode=ro`, `PRAGMA query_only=ON`, extensions
disabled) so a parser bug cannot write. On top: one statement, starting
with `SELECT`, `EXPLAIN`, or an allow-listed `PRAGMA`; no `ATTACH`; row
limit 1000 by default, hard cap 100k; a busy timeout so a runaway query
cannot starve the daemon's own connections. Columns holding wrapped key
material are returned as `<redacted>`, enumerated explicitly in code
with a schema test that fails when an unclassified such column appears.

**Mutations run through the node's own code paths.** `FsWrite` and
friends call the same internal operations the FUSE layer calls; they do
not reach around the metadata layer, do not bypass leases, delegations,
quotas, or the epoch machinery, and produce ordinary `LogRecord`s. A
support-driven write is indistinguishable, downstream, from a local
one — which is the point: it inherits every invariant the system already
proves, and the harness oracle applies to it unchanged.

**Log redaction is best-effort and documented as such.** We filter
obvious secrets (URLs with embedded credentials, `Authorization`
headers, config-marked secrets) out of `LogTail`/`LogFollow`, but a log
line is arbitrary text and we will not claim a guarantee we cannot keep.
The hard guarantees are the mode check, the path deny list, the
read-only DB connection, and the audit.

## Step 6 — Client side

```
constellation support connect --ticket CST1... [--out ./case-1234/]
constellation support collect --ticket CST1... [--fs] --out ./case-1234/
```

`connect` dials the server key from the ticket (iroh authenticates it;
a wrong or MITM'd key cannot complete the handshake), sends `Hello`,
prints the node's mode prominently, and opens a REPL:

```
logs [n] | logs -f | level <directive>
status | metrics | proc | threads
db schema | db "<SELECT ...>" | db pull
ls <path> | cat <path> | pull <path>
control <json>                       # rw for mutating requests
fs cat|write|rm|mv|chmod <...>       # rw only
nodes | use <node>                   # cluster fan-out, Step 7
help | quit
```

- **Everything is transcripted** under `--out` (default
  `./constellation-support-<date>-<shortkey>/`): requests, replies,
  pulled files. A case is one directory, and we never re-ask the user
  for something we already have.
- **In `rw`, mutating commands require a typed confirmation** the first
  time in a session, and every one is echoed into the transcript and the
  server's audit log. This is a guard against our own mistakes, not
  against an attacker — it is client-side and says so.
- `collect` is the non-interactive bundle we will actually run most of
  the time: status, metrics, proc, threads, the last 5000 log lines, db
  schema, db snapshot, state-dir listing. With `--fs` it runs against
  every armed node the roster reports and writes one subdirectory per
  node.
- Streaming responses show progress and verify the trailing digest; a
  mismatch is an error, not a warning.

## Step 6b — Programmatic interface (agent-facing)

The REPL is for a human with a hypothesis. Most inspection is better
done by something that can issue fifty requests and correlate them — a
script, or an AI agent working the case with an engineer supervising.
That needs a non-interactive surface with a stable contract, and it must
be the *same* surface the REPL uses, not a second implementation that
drifts.

### The local proxy

```bash
constellation support attach --ticket CST1… --socket /run/user/1000/case-1234.sock [--clamp ro] &
```

`attach` holds one P2P session open and exposes it on a **local unix
socket speaking the existing control-API line protocol** (`crates/api`:
one JSON request per line, one JSON response per line). Consequences
worth the small amount of code:

- Every tool that already speaks to a Constellation control socket works
  against a remote node unchanged, including the web UI.
- One QUIC session serves many commands; an agent issuing fifty requests
  pays the handshake once.
- The socket is the agent's blast radius. It is a normal unix socket
  with normal permissions, so an agent gets access by being handed a
  path, and loses it when the `attach` process exits.

`--clamp ro` is a **client-side ceiling**: even against a node the user
armed `rw`, the proxy refuses to forward mutating requests. It is not a
security boundary (the node's mode is), it is a way to let an agent
inspect an `rw` node without being able to change it. Default for
`attach` is `--clamp ro`; raising it to `rw` is explicit.

### One-shot and batch

```bash
constellation support call --socket … status --json
constellation support call --socket … logs --lines 200 --grep "lease" --json
constellation support call --socket … db "select part, seq from log_head" --json
constellation support call --socket … raw '{"req":"ThreadDump"}' --json

# batch: NDJSON requests on stdin, NDJSON responses on stdout
printf '%s\n' '{"req":"Status"}' '{"req":"ProcInfo"}' \
  | constellation support call --socket … --batch --json
```

`--ticket` is accepted directly by `call` as well, for the one-command
case where a persistent proxy is not worth it.

### The machine contract

This is the part that matters for an agent, so it is specified rather
than left to emerge:

- **stdout is data, stderr is narration.** With `--json`, stdout carries
  exactly one JSON value per response and nothing else — no banners, no
  progress, no colour. Progress and warnings go to stderr.
- **Every response is an envelope**, so a failure is parseable rather
  than inferred from an empty body:
  `{"ok":true,"req":"Status","node":"3","mode":"ro","data":{…}}` /
  `{"ok":false,"req":"FsWrite","error":{"kind":"needs_rw","message":"…"}}`.
  `error.kind` is a closed, documented set (`needs_rw`, `denied_path`,
  `not_found`, `unsupported`, `clamped`, `timeout`, `transport`,
  `busy`) — an agent branches on `kind`, never on prose.
- **Exit codes** are distinct enough to branch on without parsing:
  `0` ok, `1` request failed, `2` usage error, `3` refused by mode or
  clamp, `4` denied by the node's path/secret rules, `5` transport or
  session lost, `6` timeout. A batch exits non-zero if any request
  failed, and `--stop-on-error` stops at the first.
- **Bulk bodies go to files, not into stdout.** `DbSnapshot`,
  `ReadFile`, `pull`, and unbounded `LogTail` write into `--out` and the
  JSON envelope carries `{"path":…,"bytes":…,"blake3":…}`. An agent must
  never have to hold a 200 MB replica snapshot in a context window to
  find out it was truncated.
- **Everything is bounded and says when it truncated.** `logs` takes
  `--lines` and `--grep`; `db` takes `--limit`; every truncated response
  carries `"truncated":true` with the count that was dropped. Silent
  truncation is how an agent reaches a confident wrong conclusion.
- **`--timeout` on every call**, defaulting to 30 s, because the node
  being debugged is by definition misbehaving and a hung request must
  surface as `kind:"timeout"` rather than a stalled agent.

### Capability discovery

```bash
constellation support capabilities --socket … --json
```

Returns the machine-readable form of the capability table: every request
name, its mode requirement, whether it mutates, whether it touches user
data, its arguments and their types, and whether *this* node supports it
(version skew, `--support-no-data`, clamp). It is generated from the
same exhaustive `match` that dispatches requests, so it cannot drift
from what the server actually accepts.

An agent is expected to call this first and plan against it, rather than
hardcoding a request set and discovering by trial that the node is old,
narrowed, or clamped.

### Mutations from a program

Client-side guards, distinct from the node's enforcement:

- Mutating requests require `--allow-mutations` on the `attach`/`call`
  invocation *and* a node in `rw` *and* a clamp of `rw`. Three
  independent yeses, one of which is the user's.
- `--dry-run` resolves and validates a mutating request, reports exactly
  what it would do, and does not send it. An agent proposing a repair
  should produce a dry-run transcript for the engineer to read before
  anything is sent.
- Every request and response is appended to `--transcript` (default:
  under `--out`) in NDJSON, whichever entry point was used. The
  operator's audit log remains the authority; the transcript is our copy
  and must reconcile with it line for line.

### Optional: an MCP server

If it is cheap when the above exists — and it should be, since it is one
more front end over the same proxy — add:

```bash
constellation support mcp --socket …    # stdio MCP server
```

exposing each capability as a tool, with the mode/clamp/`no_data`
restrictions reflected in which tools are advertised at all, so a
model's tool list is already the truth about what it may do. Mark this
sub-step **optional**: skip it if the session protocol is not settled by
the time the rest of the plan is green, and note the skip in the report.
Do not let it hold up Step 9's gates.

### Tests for this step

- Envelope shape and `error.kind` for one representative of each error
  class; exit-code mapping table-tested against those.
- `--json` stdout purity: run with a warning-producing condition, assert
  stdout parses as exactly N JSON values and the warning appears on
  stderr.
- Clamp: `--clamp ro` against an `rw` node refuses a mutating request
  with `kind:"clamped"` and exit 3, and the node's audit log shows no
  such request arriving — proof the clamp refuses locally rather than
  relying on the node.
- `capabilities` output matches the dispatcher: a test that walks the
  request enum and asserts every variant appears exactly once.
- Batch: mixed success/failure NDJSON, with and without
  `--stop-on-error`.
- Bulk redirection: `DbSnapshot` writes a file and returns a digest that
  matches it; stdout stays a single small envelope.
- Proxy lifetime: killing `attach` removes the socket, and an in-flight
  `call` exits 5.

## Step 7 — Clusters

A cluster is debugged node by node; there is deliberately no cluster
grant. What the plan does provide is discovery, because the client has
no bucket credentials and cannot read the node registry:

- `Roster` returns the connected node's registry view: node ids, roles,
  last-seen, and for each node the support descriptor it advertised
  (mode, support pubkey, addrs) or "not armed".
- `nodes` / `use <node>` in the REPL opens a second session to another
  armed node, reusing the same client key. It succeeds only if that node
  was independently armed with our key — no node can authorise support
  on another's behalf, and the roster is a phone book, not a capability.
- `collect --fs` fans out over armed nodes, in parallel, with per-node
  failures reported rather than aborting the run. A partial bundle from
  four of five nodes is the normal outcome and must be usable.
- Mode is per node. A cluster where one node is `rw` and the rest `ro`
  is a supported, sensible configuration — repair on the broken node,
  observation everywhere else — and the client's prompt shows the mode
  of the node it is currently addressing.

## Step 8 — Visibility and audit

- `<state_dir>/support/audit.jsonl`, append-only, `0600`, one JSON
  object per event: `{ts, event, session_id, client_key, mode, request,
  args_summary, result, bytes}`. Mutating requests additionally record
  the resulting log-record sequence numbers, so a user can reconcile
  every change we made against their own log. Size cap rotates to
  `.1` and keeps two files; we never truncate.
- `StatusReport` gains `support: Option<SupportStatus>` — `mode`,
  `allowed_keys` (fingerprints), `connected: Vec<{client_key, since,
  requests, bytes}>`, `no_data`, `audit_path` — surfaced in
  `constellation status` and as a persistent web UI banner coloured by
  mode. A user must never have to wonder whether we are connected right
  now.
- Daemon log: INFO on arm with the full parameter set, INFO per request,
  WARN per denial or rejected peer, WARN on downgrade.
- A support build logs one WARN at every mount, armed or not.

## Step 9 — Tests

Unit (feature-gated, in `crates/net/src/support/`):

- Ticket round-trip; garbage and truncated tickets rejected usefully.
- Mode table-test: for every request variant, assert refusal in `ro`
  when it mutates and acceptance in `rw`, driven by the same exhaustive
  match as the dispatcher.
- Downgrade is monotone: `rw`→`ro`→disarmed succeeds; every widening
  transition is rejected. Downgrade mid-connection denies the next
  mutating request on that same connection.
- Path resolution: `..`, symlink, and absolute escapes denied; deny-list
  hits denied inside an allowed root; denied in `rw` as well.
- SQL gate: writes and `ATTACH` rejected by the parser *and* separately
  by the read-only connection (belt and braces, tested independently).
- Redaction: schema-classification test fails on an unclassified column.
- Flag validation: `--support` without `--support-allow` and vice versa
  both fail to parse.

Integration (`tests/support-session.sh`, feature-gated, relay disabled,
loopback direct addressing):

- Arm `ro` on a real mount; exercise one request per capability; assert
  one audit entry each; assert every mutating request is refused.
- Arm `rw`; write a file through `FsWrite`, verify it through the mount,
  verify the audit entry names the log-record sequence.
- Disallowed key: refused, no data, rejection audited.
- Wedged-daemon case: hold the metadata lock, assert `logs`, `status`,
  `proc`, `threads` still answer within a deadline.
- `support off` downgrades a live `rw` session to `ro` and then to
  disarmed; the client sees the change without reconnecting.
- Stopping the daemon ends the session; nothing survives to a remount
  that lacks the flags.
- `support collect` produces the expected bundle; `collect --fs` across
  a two-node cluster with one node unarmed produces one subdirectory and
  one reported failure.

Harness: two scenarios.

- `support-session-readonly` — arm `ro` while a workload runs, drive the
  whole read surface, and assert with the model oracle that the
  namespace is byte-identical before and after and that the session
  appended **no** log record. "Read-only cannot change your filesystem"
  deserves an oracle, not a code review.
- `support-session-repair` — arm `rw`, have the session perform a
  namespace repair, and assert the oracle accepts the result as an
  ordinary mutation: it converges on every node, survives a restart, and
  is indistinguishable from the same change made locally.

## Step 10 — Docs

The operator- and engineer-facing documentation is **already written**,
ahead of the code, because the capability table is the contract a user
consents to and it should not be reverse-engineered from an
implementation:

- [`docs/reference/features/remote-support.md`](../../../reference/features/remote-support.md)
  — modes, the full capability table, always-refused list, trust model,
  ticket format, registry descriptor, audit schema, status fields, and
  an "Extending this document" section stating what must be documented
  before a new request is added.
- [`docs/how-to-guides/operations/enable-remote-support.md`](../../../how-to-guides/operations/enable-remote-support.md)
  — the operator's task guide.
- [`docs/how-to-guides/development/run-a-support-session.md`](../../../how-to-guides/development/run-a-support-session.md)
  — our side.

The implementing model's job for this step is therefore:

- Remove the "Status: specified, not yet implemented" banners from all
  three files once the gates are green.
- Reconcile every command, flag, field name, JSON shape, and error
  string in those docs against what was actually built. Where the
  implementation had to diverge, **change the docs and say so in the
  report** — a doc that describes a capability the code does not have is
  worse than no doc, because a user consented to it.
- Follow the reference's own extension rules for any request added
  beyond Step 4's table.
- `docs/plans/v1/PROGRESS.md` — milestone rows and exit criteria per
  CONVENTIONS.md.

## Definition of done

The standard gates from `CONVENTIONS.md`, run **twice**: once with the
default feature set (the important one — the feature must be invisible
and the suite unaffected) and once with `--features remote-support`,
plus:

- `tests/support-build-gate.sh` passes in both directions.
- `tests/support-session.sh` passes.
- `target/release/harness run support-session-readonly support-session-repair`
  — both PASSED.
- `constellation --version` and `constellation mount --help` on a default
  build mention neither support nor `--support-allow`; on a support build
  both do.
