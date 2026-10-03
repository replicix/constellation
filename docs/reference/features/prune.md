# Retention pruning

Prune policies remove stale entries from a subtree automatically. A
directory carries its complete policy in the `user.constellation.prune`
extended attribute; there is no separate registry, so a policy has the
same lifecycle as any other metadata and cannot dangle.

## Table of Contents

- [Model](#model)
- [Marking a directory](#marking-a-directory)
- [Policy grammar](#policy-grammar)
- [Rules](#rules)
- [Filters](#filters)
- [Settings and arming](#settings-and-arming)
- [Inheritance](#inheritance)
- [How pruning runs](#how-pruning-runs)
- [Snapshots and clones](#snapshots-and-clones)
- [Pruning under automatic snapshot policies](#pruning-under-automatic-snapshot-policies)
- [CLI](#cli)
- [Consistency and limits](#consistency-and-limits)
- [References](#references)

## Model

- **The policy is the marker.** A directory's `user.constellation.prune`
  xattr holds its whole policy. Deleting the policy is `setfattr -x`.
- **Cluster state by construction.** Xattrs are replicated shared
  metadata, so every node agrees on what a subtree's policy is.
- **One decider.** A singleton background pruner (holding the `_prune`
  cluster lease) makes the pruning decision from its replicated replica
  and publishes ordinary `Unlink` mutations. Every replica therefore
  converges on the same namespace.
- **Namespace only.** Pruning unlinks names. Reclaiming the underlying
  S3 bytes remains bucket GC's job, on its own schedule.

## Marking a directory

```bash
setfattr -n user.constellation.prune -v 'age(90d)' /mnt/c/scratch/build
```

The policy is validated at `setfattr` time: an unparseable or misplaced
policy fails with `EINVAL` and is never written. Because the FUSE layer
cannot return a message with the errno, the reason is also cached and
shown in `constellation status` (and `constellation prune set` prints it
with a caret). Only directories may carry a policy.

## Policy grammar

```
policy    := clause (";" clause)*
clause    := rule "(" args ")" | flag | key "=" value | "!"
```

Whitespace is insignificant. Durations take `s m h d w y`; sizes take
`K M G T` (powers of 1024); percentages are `<n>%`; globs may be quoted.
Multiple rules form a **union** — an entry is pruned if any rule selects
it — and clause order is irrelevant.

## Rules

| Rule | Meaning | Needs atime |
|---|---|---|
| `age(<dur>)` | mtime older than `<dur>` | no |
| `unused(<dur>)` | atime older than `<dur>` | yes |
| `lru(high=<n>, low=<n>)` | usage crossed `high`: unlink coldest-by-atime within this subtree until it drops below `low` | yes |
| `keep(<n>)` | keep the newest `<n>` entries per directory, unlink the rest | no |

`unused` and `lru` need read-time atime; a policy using them is rejected
at `setfattr` time on a mount with atime off. See
[`atime.md`](atime.md). Because atime is best-effort and eventually
consistent, an `unused`/`lru` policy can be off by one cold interval.

### `lru` watermarks

`high` and `low` each take **either a size or a percentage**:

> A size is bytes of the marked subtree. A percentage is of the
> filesystem quota.

| Policy on `/scratch` | Measures | Behaviour |
|---|---|---|
| `lru(high=500G, low=300G)` | `/scratch` bytes | keeps the subtree between 300–500 GB |
| `lru(high=500G, low=70%)` | `/scratch` bytes | evicts to 350 GB (70% of `high`) |
| `lru(high=85%, low=70%)` | whole-FS vs quota | when the filesystem passes 85% full, `/scratch` is the tree sacrificed until it is back under 70% |
| `lru(high=500G, low=300G, of=fs)` | whole-FS bytes | same global trigger without needing a quota |

`of=fs|subtree` overrides the default (`fs` if either watermark is a
percentage, `subtree` otherwise). A percentage-watermark `of=fs` policy
on a filesystem with **no quota** is inert — it never triggers and is
reported as such by `constellation prune ls`; set a quota later and it
starts working.

## Filters

Named arguments on any rule: `min-size=`, `max-size=`, `only=<glob>`,
`except=<glob>` (repeatable), `uid=`, `gid=`, and `min-age=<dur>`. The
`min-age` floor (default 1h, never 0) spares any entry whose **mtime** is
younger than it, whatever the rule says — a metadata-only change
(chmod/chown) does not reset the timer.

`lru` never selects a hardlinked entry (`nlink > 1`): unlinking one name
frees no bytes, so it cannot move the watermark. `age`/`unused`/`keep`
prune hardlinked names normally — the data survives through the remaining
link.

## Settings and arming

- `every=<dur>` — minimum interval between runs (default 1h).
- `max=<n>` — cap on unlinks per run (default 10000).
- `rate=<n>/s` — unlink rate ceiling (default 50/s).
- `off` — exempt this subtree; cancels any inherited policy. Must be the
  only clause.
- `!` — **arm** the policy for real deletion. A policy without `!` is
  dry-run: it evaluates and writes an audit record but deletes nothing.

```
age(90d)                     # dry-run
age(90d); !                  # armed
unused(30d, except='*.keep', min-size=1M); !
lru(high=85%, low=70%); every=6h; !
```

## Inheritance

The nearest ancestor carrying the xattr governs a path, entirely —
policies do not merge. A subtree opts out with `off`. The filesystem
root may carry a policy, which is how a whole-filesystem policy is
expressed. Scratch roots (`user.constellation.scratch`) are never
pruned.

## How pruning runs

The pruner runs periodically (and on demand via `constellation prune
run`). One pass:

1. Refuses to run if the node has departed, is epoch-frozen, is a
   read-only member, or its replica trails the log tail by more than
   `CONSTELLATION_PRUNE_MAX_LAG_S` — pruning from a stale replica is how
   you delete a file someone recreated.
2. Walks each marked subtree, skipping scratch roots and nested policies,
   selecting victims from the replicated inode rows.
3. Waits out a grace period (`CONSTELLATION_PRUNE_GRACE_S`) after a
   policy's directory ctime changes, so a freshly installed policy does
   not sweep immediately.
4. Re-verifies each victim against fresh state immediately before
   unlinking it — a read or write between selection and execution spares
   it. This is what makes a lossy atime stream safe.
5. Publishes an `Unlink` per victim: executed locally when this node
   holds the partition, forwarded otherwise, or under a lease this node
   acquires for an otherwise-unheld partition (never preempting a live
   holder). It never marks the lease busy, so pruning does not fight a
   real writer.

Each run writes one audit record under `prune/journal/` in the bucket. A
dry run writes the same record with zero deletes.

## Snapshots and clones

Pruning is an ordinary unlink, so a file captured by a snapshot or clone
before it was pruned stays fully readable there. Its chunks become
reclaimable by GC only once the last reference — live *or* snapshot — is
gone.

## Pruning under automatic snapshot policies

A directory under an [automatic snapshot policy](snapshot-policies.md)
(`user.constellation.snapshots`) is snapshotted on a schedule, so a
pruned file stays in **every automatic snapshot that captured it** until
those snapshots expire. Under a dense policy (`5m:1d 1h:7d 1d:30d …`)
that can be a month or more.

- Prune alone will not free the space: the bytes are released only when
  the last snapshot that holds them expires (or is deleted) **and** bucket
  GC has run past its horizon.
- To see where the space is, run `constellation snapshot space
  <fs>:/path` (live data, unique to one snapshot, shared, awaiting GC) and
  `constellation snapshot ls -s used`. `snapshot delete --dry-run
  <selectors>` shows what deleting a set of snapshots would return.
- Pruning and snapshots are independent: prune never touches a snapshot,
  and expiry never touches a live file. If a subtree needs its space back
  sooner, shorten its snapshot tiers (mind the 24 h
  [grace window](snapshot-policies.md#grace-after-a-policy-change)) or
  delete the snapshots that pin the most.

## CLI

```
constellation prune check '<expr>'         # validate only (exit 2 on error)
constellation prune set <path> '<expr>' [--arm]
constellation prune disarm <path>          # drop the arming token
constellation prune rm <path>              # remove the policy
constellation prune show <path>            # effective policy + where inherited
constellation prune ls <target>            # every marked root (daemon)
constellation prune run <target> [--path P] [--dry-run]
constellation prune status <target>        # counters
```

## Consistency and limits

- The decision is made from replicated state only — never a node-local
  accumulator, cache residency, or pins — so a node's own unflushed
  reads cannot change what it prunes.
- Prunes publish through the normal mutation path, so every other node
  learns of them exactly as it learns of `rm`.
- A dropped atime bump only costs freshness (an entry judged on its
  previous atime); it can never make two nodes prune different sets.
- Pruning never removes a directory: a tree's shape survives its
  contents.

## References

- [`configuration.md`](../configuration.md) — the `CONSTELLATION_PRUNE_*`
  knobs.
- [`atime.md`](atime.md) — the read-time atime feature `unused`/`lru`
  depend on.
- [`snapshot-policies.md`](snapshot-policies.md) — automatic snapshots and
  `snapshot space`, which decide when pruned bytes are actually released.
- [`scratch-directories.md`](scratch-directories.md) — the xattr-marked
  root precedent.
