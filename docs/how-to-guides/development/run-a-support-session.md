# Run a support session

> **Status: specified, not yet implemented** (see
> [plan 23](../../plans/v1/wip/23-remote-support-mode.md)). The commands
> below are the intended interface; they do not run yet.

For support engineers. The operator's side of this is
[Enable remote support](../operations/enable-remote-support.md); the
contract both sides are held to is
[Remote support (reference)](../../reference/features/remote-support.md).

Read the two rules before the commands:

1. **Connect only against a ticket the user sent in the case thread.**
   Not one from a chat window, not one relayed by a third party.
2. **Ask for `rw` only when a repair is the actual plan**, and say what
   you intend to change before they remount. Every `rw` action lands in
   an audit log the user reads afterwards. Write with that in mind.

## Prepare

Get the user a support build of **their** version, not the latest:

```bash
git checkout v1.4.2
cargo build --release --features remote-support
make dist-support        # constellation-1.4.2-support-x86_64-linux
```

You need a support build locally too — the client side is behind the
same feature.

Your client key is a normal Constellation node key file. Keep it where
you keep other production credentials; it is the whole of your side of
the authentication:

```bash
CONSTELLATION_NODE_KEY=~/.config/constellation/support.key \
  constellation host init
# publish the printed public key to the case
```

## Collect first, interact second

Most cases are answered by the bundle. Run it before opening a REPL:

```bash
constellation support collect \
  --ticket 'CST1QF4X…' \
  --out ./case-1234/
```

That pulls status, metrics, process info, a full thread dump, the last
5000 log lines, the metadata schema, a consistent replica snapshot, and
a state-dir listing. Across a cluster:

```bash
constellation support collect --ticket 'CST1QF4X…' --fs --out ./case-1234/
```

`--fs` asks the connected node for its roster and fans out to every node
that is armed with your key, one subdirectory each. Nodes that are not
armed, or unreachable, are reported and skipped — a partial bundle is
the normal outcome and is still usable.

## Interactive

```bash
constellation support connect --ticket 'CST1QF4X…' --out ./case-1234/
```

The prompt shows the node and its mode. Everything — requests, replies,
pulled files — is transcripted under `--out`, so a case is one directory
and you never have to ask the user twice for the same artifact.

```
node3(ro)> status
node3(ro)> logs 200
node3(ro)> logs -f
node3(ro)> level constellation=debug,constellation_net=trace
node3(ro)> threads
node3(ro)> db schema
node3(ro)> db "select part, seq, epoch from log_head order by part"
node3(ro)> db pull
node3(ro)> ls /var/lib/constellation/fs-a
node3(ro)> cat /var/lib/constellation/fs-a/config.json
node3(ro)> pull /var/lib/constellation/fs-a/journal/0001.seg
node3(ro)> nodes
node3(ro)> use node5
```

`level` raises log verbosity on the live daemon and is restored when the
session ends. Raise it, reproduce, `logs -f`, then put it back yourself
rather than leaving it to the teardown path.

`db` is read-only in both modes, always. If you find yourself wanting an
`UPDATE`, the answer is a control-plane operation or `fsck --repair` —
the replica is a projection of the log, and writing it behind the log's
back desynchronises that node from the cluster silently.

## Scripted and agent-driven inspection

The REPL is for a human with a hypothesis. When the work is "issue fifty
requests and correlate them" — which is most inspection — drive the
session programmatically. An AI agent doing the first pass while you
read its transcript is the intended shape of this.

### Open a session once, reuse it

```bash
constellation support attach \
  --ticket 'CST1QF4X…' \
  --socket /run/user/$UID/case-1234.sock \
  --clamp ro \
  --out ./case-1234/ &
```

`attach` holds the P2P session and re-exposes it on a local unix socket
speaking the ordinary control-API line protocol. One handshake serves
every later command, and any tool that already talks to a Constellation
control socket now talks to the remote node.

Hand the agent the socket path. That path is its entire reach: it can
do exactly what the session permits, and it loses access the moment you
kill `attach`.

`--clamp ro` is the important flag. Even when the user armed the node
`rw`, the clamp makes the proxy refuse mutating requests locally — so
you can hold repair rights yourself while an agent inspects. Default is
`ro`; widening is explicit.

### Have the agent discover, not guess

```bash
constellation support capabilities --socket /run/user/$UID/case-1234.sock --json
```

This returns every request the node will actually accept right now,
with mode requirements and argument types, accounting for version skew,
`--support-no-data`, and the clamp. An agent should call it first and
plan against it; hardcoding a request set produces confident nonsense
against an older node.

### Calls

```bash
S=/run/user/$UID/case-1234.sock
constellation support call --socket $S status --json
constellation support call --socket $S logs --lines 500 --grep "lease" --json
constellation support call --socket $S db "select part, seq from log_head" --limit 200 --json
constellation support call --socket $S raw '{"req":"ThreadDump"}' --json

printf '%s\n' '{"req":"Status"}' '{"req":"ProcInfo"}' '{"req":"ThreadDump"}' \
  | constellation support call --socket $S --batch --json > triage.ndjson
```

What an agent can rely on — the full contract is in the
[reference](../../reference/features/remote-support.md#programmatic-interface):

- stdout is JSON only; narration is on stderr.
- Every response is an envelope with `ok`, and failures carry
  `error.kind` from a closed set (`needs_rw`, `denied_path`,
  `unsupported`, `clamped`, `timeout`, …). Branch on `kind`, never on
  message text.
- Exit codes distinguish the cases: `3` refused by mode or clamp, `4`
  denied by the node's own rules, `5` session lost, `6` timeout.
- Bulk responses go to files under `--out` and the envelope carries the
  path, size, and digest — never let a replica snapshot land in a
  context window.
- Truncated responses say so, with a count. Silent truncation is how an
  agent reaches a wrong conclusion confidently.

### Guardrails worth keeping

- **Keep the agent clamped to `ro` by default**, including on `rw`
  nodes. Let it propose repairs; run them yourself, or lift the clamp
  deliberately for one command.
- Mutating requests need all three of: node in `rw`, `--clamp rw`, and
  `--allow-mutations`. Use `--dry-run` to get a description of the
  change without sending it, and paste that into the case before acting.
- Everything the agent does is in the user's audit log with its own
  entry, indistinguishable from your own requests. Volume is visible;
  keep it proportionate.
- The NDJSON transcript under `--out` is your record. After an `rw`
  session it must reconcile line for line with the user's audit log.

If it has shipped, `constellation support mcp --socket $S` exposes the
same capabilities as MCP tools over stdio, with the clamp and the node's
mode reflected in which tools exist at all — the tool list becomes the
permission boundary the model sees.

## Repair (`rw` nodes)

A `rw` node accepts everything a `ro` node does, plus:

```
node3(rw)> control '{"req":"Pin","path":"/data/hot"}'
node3(rw)> control '{"req":"SetWriteMode","mode":"through"}'
node3(rw)> fs rm /data/broken.parquet
node3(rw)> fs write /data/fixed.json ./local-fixed.json
node3(rw)> fs mv /data/a /data/b
node3(rw)> remount
```

Working rules:

- **Say it, then do it.** Announce each change in the case before you
  make it. The user is watching the console line-by-line, and a surprise
  write is how trust ends.
- The first mutating command in a session asks you to confirm. That
  guard is client-side and exists to catch your own slips, not to stop
  an attacker.
- Mutations go through the node's own code paths: they respect leases,
  delegations, quotas, and epochs, and they produce ordinary log
  records. You are not reaching around the filesystem; you are using it.
- `remount` re-arms with the same flags, never wider. If you need a
  wider mode, the user remounts — you cannot escalate from inside.
- Some things are unreachable from a session on purpose. If a case needs
  passphrase or key operations, walk the user through running them.

## When the mount is wedged

The support endpoint runs on its own task and does not depend on the
FUSE worker threads or the metadata lock, so a hung mount still answers:

```
node3(ro)> threads      # who is blocked, and on what
node3(ro)> status
node3(ro)> logs 500
node3(ro)> proc
```

Namespace requests may block; those four will not. `threads` plus the
last few hundred log lines is usually the whole diagnosis.

## Finishing

```
node3(ro)> quit
```

Then tell the user to stop the support process and go back to their
normal build, or — if they want to keep the mount up — to run
`constellation support off`, which drops `rw` to `ro` and then to
disarmed without a remount.

After an `rw` session, post a summary into the case listing every change
you made. The user's audit log carries the log-record sequence numbers
for each one; your summary should match it line for line. If those two
disagree, the audit log is right.

## Related

- [Remote support (reference)](../../reference/features/remote-support.md) — capability table, modes, ticket format, audit schema
- [Enable remote support](../operations/enable-remote-support.md) — what the user is reading
- [Testing](TESTING.md) — reproducing locally before asking for a session
