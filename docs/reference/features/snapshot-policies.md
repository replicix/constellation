# Automatic snapshot policies and snapshot space

A directory carries its complete snapshot schedule in the
`user.constellation.snapshots` extended attribute: "every 5 minutes, keep
them a day; hourly for a week; daily for a month". From then on the
cluster creates and expires those snapshots with nobody watching.
Alongside, every snapshot shows how much space it holds (`USED`,
`WRITTEN`, `REFER`) and `snapshot space` shows where the space is.

The whole feature is optional and inert: with no policy anywhere, the
scheduler makes no S3 request at all, and the accounting index is built
only when someone asks for a size.

## Table of Contents

- [Terminology](#terminology)
- [Quick start](#quick-start)
- [Policy grammar](#policy-grammar)
- [Settings](#settings)
- [Examples](#examples)
- [Retention](#retention)
- [Why the newest snapshot, not the clock](#why-the-newest-snapshot-not-the-clock)
- [Creating snapshots](#creating-snapshots)
- [Expiry, holds and orphans](#expiry-holds-and-orphans)
- [Grace after a policy change](#grace-after-a-policy-change)
- [CLI](#cli)
- [Space accounting](#space-accounting)
- [Web UI](#web-ui)
- [Metrics and status](#metrics-and-status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Policy root**: a directory carrying the `user.constellation.snapshots`
  xattr. Policies do not inherit and do not merge; a nested policy root
  is a second, independent stream.
- **Tier**: `<every>:<keep>`, "take one every *every*, keep them for *keep*".
- **Bucket**: the calendar slot of one interval (the 14:05 five-minute
  slot, the 14:00 hour, the 28 September day). Buckets tile the calendar.
- **Representative**: the *oldest* snapshot in a bucket. It is the one
  the bucket's tier keeps.
- **Anchor**: the newest snapshot of a policy. Retention windows are
  measured back from it, never from the clock.
- **`auto` / manual**: `origin` of a snapshot. Only `auto` snapshots (a
  policy's own) can ever be expired automatically.
- **Hold**: a flag that keeps a snapshot from deletion and expiry,
  optionally with an owner (`held_by`).
- **Orphan**: an `auto` snapshot whose policy root has no policy any
  more (or one that does not parse).
- **Scheduler leader**: the one node holding the `_snapsched` lease; it
  creates and expires for the whole cluster.

## Quick start

```bash
# See what a policy means before setting it (local, no daemon needed).
constellation snapshot policy check '5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest'

# Bind it (prints the delta first; asks if it would expire anything).
constellation snapshot policy set myfs:/projects '5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest'

# The same thing through the mount, with the same validation:
setfattr -n user.constellation.snapshots \
         -v '5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest' /mnt/myfs/projects
getfattr -n user.constellation.snapshots /mnt/myfs/projects

constellation snapshot ls myfs:/projects        # the snapshots, with space
constellation snapshot policy rm myfs:/projects # setfattr -x does the same
```

The expression is validated at `setfattr` time: an unparseable policy, a
non-directory, or a scratch root (see
[`scratch-directories.md`](scratch-directories.md)) fails with `EINVAL`
and is never written. Because FUSE cannot return a message with the
errno, the reason is cached and shown in `constellation status` (as
`snapsched.last_parse_error`); `snapshot policy set` and `policy check`
print it with a caret under the offending byte.

## Policy grammar

```
policy   := clause ((";" | WS) clause)*
clause   := tier | key "=" value | "paused"
tier     := interval ":" keep
interval := <n>"m"  (n divides 60)     1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m
          | <n>"h"  (n divides 24)     1h 2h 3h 4h 6h 8h 12h
          | "1d" | "1w" | "1mo" | "1y"
          | <n>"s"  (test only)        10s 12s 15s 20s 30s
keep     := <n>("min"|"m"|"h"|"d"|"w"|"mo"|"y") | "*"       "*" = forever
```

Intervals are exactly the set that divides the calendar, so buckets tile
it and a daily boundary never falls mid-bucket; `7m` is refused with the
list of valid values. `60m` is `1h`, `24h` is `1d`.

**Units: `m` is minutes, `mo` is months.** This feature deletes data and
minute-versus-month is the likeliest misreading, so the ambiguous shapes
are refused with both readings spelled out: `1M` anywhere, a bare-`m`
keep in a policy that also has a `1mo`-or-coarser tier (`5m:6m 1mo:1y`),
and `1d:6m` (six minutes is shorter than a day). Write `6mo` or `6min`.
The canonical form always prints minutes as `min`.

Other constraints, each reported at a byte offset: at least one tier; no
duplicate intervals; `keep` at least one `every` bucket, calendar-aware
(`1mo:30d` is refused because March has 31 days, `1mo:31d` is fine).

`paused` stops both creation and expiry and keeps everything. It is the
maintenance switch: unlike removing and re-adding a policy, it does not
start a [grace window](#grace-after-a-policy-change).

**Canonical form.** Tiers sorted finest first, then settings in the order
of the table below, defaults omitted, e.g. `5m:1d 1h:7d 1d:30d 1mo:1y;
tz=Europe/Budapest`. `snapshot policy check` prints it; the stored value
is whatever you wrote, so `getfattr` shows what you set.

Parsing is pure: no clock, no environment, no host timezone database
(the tzdb is bundled in the binary, so a static build needs no
`/usr/share/zoneinfo`). Every node therefore reads a policy identically.

## Settings

| Key | Default | Meaning |
|---|---|---|
| `tz=<IANA name>` | `UTC` | Timezone in which buckets of 1h and coarser are aligned. Validated against the bundled tzdb. |
| `day-start=HH:MM` | `00:00` | Shifts day/week/month/year boundaries (e.g. `02:00` for a nightly off-peak daily). Must be a **whole hour**, and a multiple of the finest hourly tier (`day-start=03:00` with a `2h` tier is refused). On a DST day where that time does not exist, the first instant at or after it is used. |
| `week-start=mon\|sun` | `mon` | First day of a `1w` bucket (ISO weeks by default). |
| `last=<n>` | `1` | Always keep the `n` newest snapshots. Minimum 1: the newest is never auto-deleted. |
| `skip-empty=yes\|no` | `yes` | Do not create a snapshot when nothing under the root changed since the previous one. |
| `budget=<size>` | none | **Parsed and stored, not enforced.** Nothing consults it yet: a policy with `budget=` expires exactly what it would without. The web editor shows the field disabled. |

## Examples

The steady-state bound is the sum of `keep / every` over the tiers; the
real count is lower because of union semantics (the midnight snapshot is
the representative of the 5m, 1h and 1d buckets at once). `snapshot
policy check` prints the bound and a simulated count.

| Policy | Meaning | Bound | Simulated (`policy check`) |
|---|---|---|---|
| `1h:1d 1d:7d` | hourly for a day, daily for a week | ≤ 31 | 30 |
| `15m:1d 1h:2d 1d:30d 1mo:1y` | a typical project tree | ≤ 96+48+30+12 = 186 | 159 |
| `5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest` | dense | ≤ 288+168+30+12+12 = 510 | 473 |
| `1d:14d 1mo:*; day-start=02:00` | nightly at 02:00, monthlies forever | none: 14 + months elapsed | 26 after a year |
| `1d:7d; last=3; skip-empty=no` | exactly one per day, even when idle | ≤ 7 | 7 |

`policy check` warns on a sub-minute tier and on a bound above 2000. The
web editor's presets (Light, Standard, Dense) simply write the first
three tier sets above; there is no stored preset name.

## Retention

Stated once. A policy's **candidates** are its `auto` snapshots of that
root that are **not held**. A held snapshot is invisible to the policy
whatever its owner: never expired, never counted against the others.
Then:

1. The **anchor** is the creation time of the newest candidate.
2. For each tier `I:K`, a candidate is its bucket's **representative** if
   it is the *oldest* candidate in that `I`-bucket (aligned in `tz`,
   shifted by `day-start`).
3. A candidate is **kept** if it is among the newest `last` candidates,
   or for some tier it is the representative **and** its bucket starts
   after *(the anchor's `I`-bucket start − `K`)*. `K = *` always keeps.
   Month and year arithmetic is calendar arithmetic in `tz`. The
   comparison is strict, so `5m:1d` keeps exactly 288 buckets, the
   anchor's included.
4. Every other candidate **expires**.

Every tier that keeps a snapshot is listed in `KEPT BY` (`5m·1h·1d`).
`EXPIRES` is a forecast ("if snapshots keep arriving on schedule"), not
an input. The same function serves the scheduler, `policy check`,
`policy set`'s delta and the web timeline: there is one implementation
and no surface restates it.

## Why the newest snapshot, not the clock

Windows are measured back from the **newest snapshot**, not from "now".
If the scheduler is down, or S3 is unreachable, or the whole cluster is
off for a week, the anchor stops moving, so the windows stop moving, so
**nothing expires**. A clock-anchored rule would age the entire history
away during the very outage that stopped new snapshots arriving, and
could leave a root with nothing. The clock decides only *when* to create
and *when* an expiry run may happen; it can delay a deletion, never cause
one. `last ≥ 1` keeps the newest snapshot regardless. Window length is
likewise a duration ("keep a day"), not a count, because a count and a
duration diverge exactly when `skip-empty` leaves buckets empty.

The healthy outage signature is `create_failed` climbing while `expired`
stays at 0.

## Creating snapshots

Every daemon runs a scheduler tick (default every 10 s); one node leads
through the `_snapsched` singleton lease, which it keeps and renews, so
there is no acquire/release churn. It uses its own lease, not `_prune`'s,
so a long prune walk never delays a snapshot. Creation runs **at the
root-lease holder** as a batch (local, or forwarded over P2P): taking an
automatic snapshot never steals the write lease from the node that is
writing. A node with `CONSTELLATION_SNAPSCHED=0` never leads but still
serves batches as holder.

- **Due**: a root is due when no auto snapshot of it, held or not, was
  created inside the current finest-interval bucket.
- **Catch-up, not backfill**: after downtime it takes one snapshot now,
  the first of the current bucket. Missed buckets stay empty.
- **Names**: `auto-<UTC bucket start>`, e.g. `auto-20260928T1405Z`
  (`auto-20260928T140510Z` for seconds tiers). UTC, so DST never makes two
  identical labels; no tier in the name, because a snapshot's tiers
  change over its life; the *bucket*, not the creation instant, so a new
  leader retrying after a failover hits "already exists" and no bucket
  gets two snapshots. `CREATED` is the truthful creation time (the
  leader may execute late and cross into the next bucket, which then
  counts as covered); `snapshot ls` prints it in UTC.
- **Skip-empty**: with `skip-empty=yes` the holder compares the new tree
  with the previous snapshot's and creates nothing if no key under the
  root changed (atime-only changes are ignored). A skipped bucket needs no
  snapshot because the previous one *is* that state. The check reads at
  most `CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS` diff keys; a longer diff
  counts as changed.
- **Cap**: a root with `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT` live auto
  snapshots (held included) gets no more until some are gone.
- **Refusal gates**: a tick creates and deletes nothing if the node has
  departed, is epoch-frozen, is a read-only member, or its replica trails
  the log by more than `CONSTELLATION_SNAPSCHED_MAX_LAG_S`.

## Expiry, holds and orphans

After creating, the leader expires what retention says, per root, at most
once per `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S`, up to
`CONSTELLATION_SNAPSCHED_MAX_DELETES` per run, oldest first. Before each
delete batch it renews its lease and re-reads every victim (still there,
still `auto`, still this root's, still unheld); the holder checks the
hold once more per item, so a `hold` that lands in between wins. Each
deletion is recorded with its reason in a `snapsched/journal/` bucket
object.

**Never deleted automatically:** manual snapshots; held snapshots;
orphans; anything while the root is `paused`; anything while a refusal
gate is up.

**Holds** (`snapshot hold`, `snapshot create --hold`) pin a snapshot. A
hold may carry an owner, `held_by`, in a namespace:

| `held_by` | Owner | Notes |
|---|---|---|
| (empty) | an operator's plain hold | released by a plain `snapshot release` |
| `user:<name>` | a person or script | |
| `csi:<id>` | the [Kubernetes CSI driver](../../plans/v1/wip/37-kubernetes-csi.md) | a `VolumeSnapshot` is a held snapshot; the driver owns the hold |

`policy:` is reserved and anything unprefixed is refused. Releasing (or
re-holding) a snapshot requires the **same owner**: `snapshot release
--by csi:<id>`; a plain release of a `csi:`-held snapshot is refused with
the owner named, so an operator cannot casually unpin a volume snapshot
the driver believes is safe. `--force` overrides (admin only). Releasing
clears the owner. `snapshot delete` of a held snapshot is refused too,
unless `--force`. In `snapshot ls`, `KEPT BY` reads `held`, `held: user`
or `held: csi`.

**Orphans — removing a policy freezes its snapshots.** `setfattr -x`,
`snapshot policy rm`, or a policy that stops parsing leaves the root's
auto snapshots in place, listed as `auto (orphaned)` with `KEPT BY` and
`EXPIRES` blank, and nothing ever expires them. `snapshot policy rm
--expire` is the explicit, confirmed way to delete the unheld ones along
with the policy. A deleted directory's snapshots are orphaned the same
way; a renamed directory keeps its policy and snapshots (the identity is
the directory's inode, not its path).

## Grace after a policy change

Shortening `1d:1y` to `1d:7d` must not delete a year of history within
one tick. The leader records, per root, the canonical policy last seen
and when (`snapsched/state.json`, written only by the leader with an ETag
CAS; a lost CAS deletes nothing that run). A root whose policy changed
less than `CONSTELLATION_SNAPSCHED_GRACE_S` (24 h) ago expires only what
**both** the new policy and every policy it replaced inside the window
would expire; a root seen for the first time expires **nothing** for the
window, so adopting a directory that already has auto snapshots is safe.
Creation follows the new policy immediately.

`paused` is not part of the canonical form compared, so pause and resume
open no window. `snapshot policy set` and the web editor show the delta
(what it would expire, the grace note) before writing. The window is
wall-clock time recorded with its end, so a leader with a shorter
configured grace never closes a window opened under a longer one.

## CLI

```
constellation snapshot policy set   <fs:path> '<expr>' [--yes] [--dry-run]
constellation snapshot policy show  <fs:path>
constellation snapshot policy ls    [<fs[:path]>]
constellation snapshot policy rm    <fs:path> [--expire] [--yes]
constellation snapshot policy pause|resume <fs:path>
constellation snapshot policy check '<expr>' [--simulate <dur>] [--against <fs:path>] [--json]

constellation snapshot ls     [<fs[:path]>] [-o cols] [-s col] [-p] [--json] [--auto|--manual|--orphaned]
constellation snapshot space  [<fs[:path]>] [--verify] [--json]
constellation snapshot delete <sel>... [--dry-run] [--yes] [--force]
constellation snapshot create <fs:path@name> [--hold] [--by <owner>]
constellation snapshot hold    <sel>... [--by <owner>] [--force]
constellation snapshot release <sel>... [--by <owner>] [--force]
constellation snapshot sched status [--json]
constellation snapshot sched run [--dry-run] [--json]
```

- **`policy set`** parses (caret on error, exit 2), evaluates the new
  policy against the root's real snapshots, prints the delta (what it
  creates, how many snapshots it would expire, the reclaim estimate, the
  grace note), asks when it would expire anything (`--yes` skips,
  `--dry-run` writes nothing), warns on sub-minute tiers and a bound over
  2000, then writes the xattr.
- **`policy check`** explains an expression locally: canonical form,
  steady-state bound, a simulated count over `--simulate` (`30d`, `12w`,
  `6mo`, `1y`; `m` is minutes), warnings. `--against` evaluates it over a
  directory's real snapshots through the daemon and lists what it would
  expire.
- **`policy ls` / `show`** list every root (path, ino, policy, state —
  armed, paused, unparseable, orphaned — and snapshot count) or one root's
  state with each snapshot's keeping reasons.
- **`snapshot ls`** is a table; `--json` is the machine shape.
  Default columns: `NAME CREATED ORIGIN USED WRITTEN REFER KEPT BY
  EXPIRES`; `-o` also takes `lsize`, `id`, `seq`, `creator`, `policy`;
  `-s used` sorts by space (find the one eating it); `-p` prints exact
  bytes and Unix ms. A footer names the commit the sizes are as of.
  `EXPIRES` is `in 6d 23h`, `now`, `never`, or `-` when no policy decides
  (orphaned, paused).
- **`snapshot delete`** takes `path@name`, `path@a%b` (that directory's
  snapshots from `a` to `b`, inclusive), `path@prefix*`, several at once.
  `--dry-run` prints what would be deleted or refused and `would reclaim
  ≈ X in N chunks (after GC)`; deleting more than one asks first with the
  same line. Exits non-zero if any was not deleted; a dry run exits 0.
- **`snapshot sched status`** reports, per node, whether it leads, the
  counters, and per root: `PATH POLICY STATE NEXT LAST CREATED ERROR`.
  The scheduler is per node: run it on each node to find the leader.
  **`sched run`** takes the lease if free and creates every due snapshot
  now (never deletes); `--dry-run` takes no lease.

## Space accounting

Sizes are **logical, deduplicated chunk bytes, before compression**,
from a node-local accounting index (`<state dir>/snapacct/`: derived,
rebuildable from the replicated snapshot rows, never published, never
consulted by GC). Every node can answer locally, and nodes agree to the
byte. A chunk counts at the largest size it occurs at in any snapshot.

| Column | Meaning | Additive? |
|---|---|---|
| `REFER` | distinct chunks the snapshot references | no |
| `LSIZE` | apparent size (`-o lsize`) | no |
| `WRITTEN` | chunks not in the previous surviving snapshot of the same directory | yes, along a chain |
| `USED` | chunks referenced by this snapshot and by **nothing else**: no other snapshot, not the live tree | no |

**`USED` does not sum.** A chunk shared by two snapshots is in neither
one's `USED`, yet deleting both returns it. So the sum of `USED` is less
than what deleting every snapshot returns, and deleting one snapshot can
make chunks it shared with exactly one neighbour appear in that
neighbour's `USED`. That is why `snapshot delete --dry-run` computes the
real figure for a *set* of snapshots instead of adding `USED` columns.

`snapshot space [fs[:path]]` is the ZFS `usedby*` breakdown:

```
live data (logical)                 1.02T
snapshots, total (usedbysnapshots)  96.4G   ← deleting every snapshot returns this
  unique to one snapshot            7.1G    ← Σ USED; not the total (see note)
  shared by ≥2 snapshots only       89.3G
shared between live and snapshots   —       (costs nothing extra)
awaiting GC                         12.0G   (freed snapshots; horizon 7d)
estimated physical (×0.61 compression, from last GC round): snapshots, total ≈ 58.8G
```

`--verify` is a brute-force oracle: it walks every snapshot, diffs the
exact sets against the index and exits non-zero on a mismatch. It is slow
on a large filesystem.

**Space returns after GC.** Deleting a snapshot (by hand or by expiry)
removes names and metadata roots; the chunks go when garbage collection
next runs and the chunks are older than `CONSTELLATION_GC_HORIZON_S`
(default 7 days). Until then they show as *awaiting GC*, and the delete
line says "after GC" for that reason. Physical figures are estimates
(logical bytes times the compression ratio the last GC round measured;
`≈` marks them).

**Freshness.** Creation and deletion update the index when applied; the
live tree is refreshed every `CONSTELLATION_SNAPACCT_REFRESH_S` (60 s),
so `shared with live` lags by at most that. Queries never return partial
numbers: while the index builds they answer `building (37%)`. With
`CONSTELLATION_SNAPACCT=off` size columns show `-` and `snapshot space`
refuses. Build cost is one walk of each chain's oldest snapshot plus the
diffs, about 90 bytes of local disk per indexed chunk.

Excluded from per-snapshot numbers: metadata (a snapshot pins a
whole-filesystem tree whose packs are shared with its neighbours) and
clones (live trees, so their chunks count as live).

## Web UI

`snapshots.html` on the control UI (`--web-ui`, see
`CONSTELLATION_WEB_UI_PORT`) renders what the API returns; it holds no
retention or accounting logic.

- **Space overview**: the breakdown above as one stacked bar.
- **Policy roots**: a card per root with the canonical expression, state
  (next snapshot, last created, last error; paused, capped, orphaned
  badges), the snapshot count, Σ `USED`, and Edit / Pause / Remove
  (Remove + expire) buttons.
- **Policy editor**: tier rows, presets, advanced settings, a two-way
  expression field validated by `snapshot.policy.check`, a retention
  timeline (the simulator's output: which snapshots a tier keeps, what
  expires, the count over time), and Save, which shows the same delta and
  confirmation as `policy set`.
- **Snapshot table**: filters, sorting, multi-select with a reclaim
  footer, delete, and hold buttons (a `csi:`-owned hold is shown as
  "external" and cannot be released from the page). A *written over time*
  chart per directory.
- A banner on the dashboard and the page when a root is unparseable or
  capped, or a refusal happened in the last 10 minutes.

## Metrics and status

`/metrics` exports `constellation_snapsched_*` — gauges `leader`,
`roots`, `paused_roots`, `unparseable_roots`, `capped_roots`,
`orphaned_snapshots`, `last_create_unix_ms`, `last_refused_unix_ms`;
counters `ticks`, `created`, `skipped_empty`, `create_failed`, `expired`,
`skipped_reverify`, `skipped_grace`, `refused_lag`, `refused_state`
(`budget_expired` and `budget_stale` exist and stay 0) — plus per-root
`constellation_snapsched_root_snapshots{root_ino}` and
`..._root_used_bytes{root_ino}` (Σ `USED`), labelled by inode, never by
path. `constellation_snapacct_*` covers the index (`building`,
`build_progress_pct`, `indexed_chunks`, `index_bytes`, `as_of_seq`,
`refresh_ms_last`, `verify_mismatches`, …). `create_failed` counts once
per due root per tick, so read its rate. The same sections are in
`node.status`; `constellation status` prints a scheduler and an
accounting block on stderr (stdout stays JSON).

## Troubleshooting

### Snapshots are silently not being taken

`constellation status` prints a `WARNING:` (and the UI shows a banner) when
any of these is true; `snapshot sched status` repeats it:

- **unparseable** (`unparseable_roots` > 0): the xattr arrived by replay
  with an expression this build rejects. Replay never validates;
  the scheduler fails closed, creating **and deleting nothing** for that
  root. Fix the expression; `last_parse_error` has the reason.
- **capped** (`capped_roots` > 0): the root hit
  `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT`; `last_error` names it. Shorten
  keeps, delete snapshots, or raise the cap.
- **refused** (`refused_lag`, `refused_state`): the leader's replica is
  stale, or it is departed, frozen or read-only. Nothing happens until it
  recovers; a healthy node takes over the lease.
- **nobody leads**: no node runs with `CONSTELLATION_SNAPSCHED` on, or
  every holder is unreachable (a leader that cannot reach the root-lease
  holder for `CONSTELLATION_SNAPSCHED_RESIGN_AFTER` ticks resigns).

### A policy change deleted nothing

Within the grace window that is intended. See
[Grace](#grace-after-a-policy-change); `skipped_grace` counts survivors
only grace keeps.

### Space did not come back

Deleting snapshots only starts the clock; see *Space returns after GC*.
`snapshot space` shows the bytes under *awaiting GC*.

## FAQ

- **Does a snapshot cost anything to take?** No data is copied. It pins
  a metadata root; the cost is the chunks it keeps alive and a metadata
  publish at the holder.
- **Why is one snapshot's `USED` tiny when its removal frees a lot?**
  Shared chunks. Use `snapshot delete --dry-run` for the real figure.
- **Can two policies overlap?** A nested policy root is an independent
  stream with its own snapshots of the subtree.
- **What are seconds tiers for?** Tests only: they let a harness run a
  real schedule in minutes. The grammar accepts them (validity must not
  depend on the environment) and `policy set` warns.

## References

- [`configuration.md`](../configuration.md) — the `CONSTELLATION_SNAPSCHED*`,
  `CONSTELLATION_SNAPACCT*` and `CONSTELLATION_GC_SNAP_WALK` knobs.
- [`prune.md`](prune.md) — the xattr-policy precedent, and how pruning
  interacts with snapshots.
- [`forwarded-mutations.md`](forwarded-mutations.md) — holder-side snapshot
  batches.
- [`scratch-directories.md`](scratch-directories.md) — scratch roots cannot
  carry a policy.
- [Plan 32](../../plans/v1/done/32-snapshot-policies-and-space.md) — the design
  and its rationale.
