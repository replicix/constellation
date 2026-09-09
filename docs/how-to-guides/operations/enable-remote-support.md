# Enable remote support

> **Status: specified, not yet implemented** (see
> [plan 23](../../plans/v1/wip/23-remote-support-mode.md)). The commands
> below are the intended interface; they do not run yet.

Use this when Constellation support asks for a live look at a node —
a hang, a corruption, a performance cliff, anything that does not
reproduce anywhere but your cluster.

You stay in control of all four decisions: whether to run a support
build at all, which key may connect, whether that key may only look or
also change things, and when it all ends.

For the full contract — every request a session can make, what is always
refused, the trust model — see
[Remote support (reference)](../../reference/features/remote-support.md).

## Before you start

- You have a **support build** of the same version you are running.
  Normal builds cannot be talked into support mode; the code is not in
  them. Check with `constellation --version`, which prints
  `(+remote-support)` on a support build.
- You have the **support engineer's public key** from your case thread —
  a 64-character hex string. Confirm it through a channel you trust; it
  is the only thing that decides who may connect.
- You can restart the mount. Arming support is a remount, by design.

## Step 1 — Decide the mode

| Mode | Grant | Choose it when |
|---|---|---|
| `ro` | Support can inspect: logs, counters, thread dumps, metadata queries, node-local files. It cannot change anything — enforced by the node, not by the tooling. | Default. Diagnosis, "what is it doing", "why is this slow". |
| `rw` | Support can do anything the node itself can do: pin, gc, `fsck --repair`, snapshot, write and delete files in the filesystem, repair local config, remount. | You have agreed on a repair and want it applied rather than described. |

Start with `ro`. You can always remount into `rw` after the diagnosis,
and going the other way — dropping from `rw` to `ro` — does not even
need a remount (Step 5).

Read `rw` literally: it is the equivalent of handing over the node.
Every action is logged and visible to you live, but nothing *prevents*
one. Grant it when you want a fix applied, not as a convenience.

## Step 2 — Arm the mount

Unmount, then mount with the support build and two extra flags:

```bash
constellation mount --s3 "$BACKEND" /mnt/data --state-dir "$STATE" \
  --support ro \
  --support-allow 8f2c…<the engineer's 64-hex key>…a91b
```

The daemon prints a block like this, and repeats it into its log:

```
Constellation remote support is ARMED  (support build 1.4.2)
  mode        : ro
  allowed     : 8f2c…a91b
  session key : 41d0…77e3
  node        : 3  fs 9d1e…
  audit log   : /var/lib/constellation/fs-a/support/audit.jsonl

  ticket: CST1QF4X…

Support ends when this process stops.
```

Optional narrowing, added to the same command line:

```bash
  --support-no-data          # no reads of your file contents
  --support-max-sessions 2   # two engineers at once (default 1)
  --no-support-advertise     # do not record "armed" in the S3 registry
```

`--support-no-data` blocks file *contents*. Filenames, sizes, and
directory layout stay visible through the metadata replica — if the
names themselves are sensitive, say so in the case rather than relying
on this flag.

## Step 3 — Send the ticket

Paste the `ticket:` line into your support case. That is all support
needs; there is no port to open and no inbound firewall rule to add — the
node dials out through the same P2P path it already uses.

The ticket describes your network, so send it through the case, not a
public channel. It grants nothing on its own: a connection is still
refused unless the connecting key is the one you allowed.

If your nodes need relays to be reachable, the usual relay configuration
applies — see [Enable P2P relays](enable-p2p-relays.md).

## Step 4 — Watch the session

Three views, all yours:

```bash
# Is anyone connected right now?
constellation status --state-dir "$STATE" | jq .support
```

The mount console prints one line per request as it is served. The web
UI shows a persistent banner, coloured by mode, whenever the node is
armed.

Support may drive the session with scripts or an AI agent rather than by
hand — expect bursts of requests rather than a steady human pace. This
changes nothing about your guarantees: automated requests arrive over
the same session, are checked against the same mode, and land in the
same audit log, one entry each. A node in `ro` cannot be changed by an
agent any more than by a person.

## Step 5 — End it, or lower it

Ending is stopping the process:

```bash
umount /mnt/data          # or however you stop the mount
# then mount again with your normal build
```

Nothing survives that: the session key existed only in memory, the
ticket is now worthless, and a normal build cannot be armed at all.

If you would rather not disrupt a running workload, lower the mode in
place:

```bash
constellation support off --state-dir "$STATE"
```

The first call turns `rw` into `ro`; a second turns `ro` into disarmed.
It takes effect on the connection that is already open, not just on new
ones. This only ever narrows — raising the mode or adding a key requires
a remount, which is to say, requires you.

## Step 6 — Afterwards

The audit log outlives the session:

```bash
jq -r 'select(.event=="request") | "\(.ts) \(.mode) \(.request) \(.args_summary) \(.result)"' \
  "$STATE"/support/audit.jsonl
```

After an `rw` session, reconcile what was changed against your own
records — mutating entries carry the log-record sequence numbers they
produced:

```bash
jq -r 'select(.log_seq != null) | "\(.ts) \(.request) \(.args_summary) → \(.log_seq|join(","))"' \
  "$STATE"/support/audit.jsonl
```

Keep the file with the case. It is your record of what happened, written
on your disk, and nothing in the support path can rewrite it.

## Multi-node clusters

Arm each node you want reachable, separately:

```bash
# on every node that should be inspectable
constellation mount … --support ro --support-allow 8f2c…a91b
# on the one node you agreed to have repaired
constellation mount … --support rw --support-allow 8f2c…a91b
```

Mixed modes are the recommended shape: `rw` on the broken node, `ro`
everywhere else. There is no cluster-wide grant, and no node can
authorise support on another node's behalf — a node that you did not arm
stays unreachable even while its neighbour is connected.

Unless you pass `--no-support-advertise`, an armed node records that
fact in the S3 registry so support can see which nodes are reachable
without you listing them by hand. Support still cannot connect to any
node you did not arm with their key.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `unexpected argument '--support'` | Not a support build. | `constellation --version` should print `(+remote-support)`. |
| Mount refuses to start: `--support requires --support-allow` | Armed for nobody. | Add the engineer's key. |
| Support reports "connection refused / key not allowed" | Key mismatch, or the mount was restarted without the flags. | Compare the key in your command with the one in the case, character for character. |
| Support connects but every request fails `Denied: needs rw` | The node is in `ro` — working as intended. | Remount with `--support rw` if you have agreed to a repair. |
| `Denied` on a path you expected to be readable | Secret-path deny list, or outside the allowed roots. | Nothing to fix; the node refuses those in every mode. |
| Support says the node is unreachable | No usable network path. | Configure relays — see [Enable P2P relays](enable-p2p-relays.md). |
| Session died mid-diagnosis | The daemon stopped or was remounted. | Re-arm and send the new ticket; tickets are per-process. |

## Related

- [Remote support (reference)](../../reference/features/remote-support.md) — capability table, trust model, ticket format
- [Enable P2P relays](enable-p2p-relays.md) — reachability for nodes without a shared network
- [Diagnose lease thrash](diagnose-lease-thrash.md) — often enough on its own, before asking for a session
