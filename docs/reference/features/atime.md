# Read-time atime

By default Constellation mounts with noatime semantics: `read()` never
changes a file's access time, and `atime` moves only when something sets
it explicitly (`utimensat`, `touch -a`). This keeps reads free of extra
locks, metadata reads, and S3 traffic.

When an operator opts in, Constellation can bump `atime` on read —
**optionally, batched, eventually consistent, and best effort**. It is
never allowed to slow a read, acquire a lease, fail an operation, or
delay an unmount or lease handoff. Losing a bump is legal; it only costs
freshness.

## Table of Contents

- [Modes](#modes)
- [Enabling it](#enabling-it)
- [What "eventually consistent" means here](#what-eventually-consistent-means-here)
- [The page-cache limit](#the-page-cache-limit)
- [Best-effort guarantees](#best-effort-guarantees)
- [Observability](#observability)
- [Tuning](#tuning)
- [References](#references)

## Modes

- **off** (default) — reads never move atime. Bit-identical to the
  historic behaviour.
- **relatime** — Linux relatime semantics: bump only when the current
  atime is older than the granularity (default 24 h), **or** when it
  trails mtime or ctime. The trailing clause is what keeps "has this
  been read since it changed?" working, which is the one question most
  tooling actually asks of atime. Under relatime a read-heavy workload
  emits at most one update per inode per day.
- **lazy** — bump whenever the atime is older than a small granularity
  (default 1 s), without the relatime trailing shortcut. Closer to
  strict atime, but still coalesced per flush interval and still lossy.

There is deliberately **no `strict` mode**: Constellation cannot honour
strict-atime semantics (updates are coalesced and may be dropped) and
will not name a mode as though it could.

## Enabling it

Mount flag, or the environment override (env wins over the flag):

```bash
constellation mount myfs /mnt/myfs --atime relatime
```

```bash
CONSTELLATION_ATIME=lazy constellation mount myfs /mnt/myfs
```

All tunables are environment variables documented in
[configuration.md](../configuration.md#read-time-atime).

## What "eventually consistent" means here

A read on node A becomes visible on node B after a flush interval plus
normal segment shipping. No tighter bound is promised. The mechanism:

1. A read that passes the policy records a pending bump in an in-memory
   accumulator, coalesced per inode with `max()`. Repeat reads of a hot
   file are a branch and nothing else.
2. A flush ticker (default every 10 s) drains the accumulator, applies
   every bump to the **local** replica immediately (so local `stat`
   reflects local reads), and publishes per partition:
   - if this node holds the partition lease, the bumps are queued for
     the shipper;
   - otherwise they are forwarded to the holder in one best-effort
     batch (a read-only member does this only with
     `CONSTELLATION_ATIME_RO_FORWARD=1`);
   - with no reachable holder, the bump simply stays local.
3. Queued bumps **ride along** with the next log segment the partition
   ships for any reason, and are flushed in a final segment when the
   holder releases the lease while idle ("ship-then-release"). Atime
   never triggers a segment PUT on its own on the hot path, so a
   read-heavy workload does not turn into a stream of S3 writes.

Merge is order-free: records apply with `max()` under a guard, so a
duplicated, delayed, or reordered record can never make two replicas
disagree. An explicit `touch -a` (which also bumps ctime) always wins
over an in-flight read bump from before it.

## The page-cache limit

Constellation does not open files with `direct_io` or `keep_cache`, so
repeated reads within one open handle are served from the kernel page
cache and never reach the filesystem. **Atime therefore moves on
page-cache misses, not on every `read(2)` call.** Under relatime's 24 h
granularity this is invisible. Under lazy it means a hot, fully cached
file's atime can lag what a local filesystem would report — this is
expected, not a bug.

The same caching applies to observation: a peer's `stat` is served from
its kernel attribute cache until that entry's timeout expires, so
cross-node visibility can lag the flush and ship intervals by the
attribute-cache TTL on top.

## Best-effort guarantees

- A read never blocks on atime and never fails because of it.
- Atime never acquires a partition lease. A read-only node that never
  holds a lease and never successfully forwards simply keeps atime
  local forever — an acceptable outcome by construction.
- Atime is invisible to conflict detection: a pending local bump never
  suppresses a foreign namespace or attribute change, and is never
  suppressed by pending local work.
- A claimed atime is clamped to `now + skew tolerance` on apply, so one
  badly skewed clock cannot park an inode's atime in the far future.
- The in-memory accumulator is capped (default 65536 distinct inodes);
  on overflow the new entry is dropped and counted.

## Observability

`constellation status` reports an `atime` block: the mode plus counters
for `queued`, `coalesced`, `applied`, `dropped_cap`, `forward_ok`,
`forward_err`, `local_only`, and `skew_clamped`. Steadily climbing
`dropped_cap` or `forward_err` is the signal that atime is being
lossy — which is legal, but worth seeing.

## Tuning

See [configuration.md](../configuration.md#read-time-atime) for the full
table. The levers that matter most:

- `CONSTELLATION_ATIME_GRANULARITY_S` — how often an inode may bump.
- `CONSTELLATION_ATIME_FLUSH_MS` — how often the accumulator drains.
- `CONSTELLATION_ATIME_RO_FORWARD` — whether a read-only member
  publishes its atime (atime carries no authority, so this is the one
  record class a read-only member may forward).

## References

- [Forwarded mutations](forwarded-mutations.md)
- [Configuration](../configuration.md#read-time-atime)
- [DESIGN.md §4](../../explanation/DESIGN.md)
