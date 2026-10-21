# Kernel cache coherence (Linux FUSE)

What the Linux kernel caches for a Constellation mount, when an open may
keep a file's pages, and the workarounds for three races in the kernel's
FUSE client (plan 42): what each closes, what it costs, how to tell from
`node.status` which apply, and what stays open on a kernel without the
fixes.

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [What the kernel caches](#what-the-kernel-caches)
  - [Keeping a file's pages across opens (W1)](#keeping-a-files-pages-across-opens-w1)
  - [Race 1 and the write-race repair (W2)](#race-1-and-the-write-race-repair-w2)
  - [Races 2a and 2b and directory attributes (W3)](#races-2a-and-2b-and-directory-attributes-w3)
  - [Kernels with the fixes](#kernels-with-the-fixes)
  - [Settings](#settings)
  - [What stays open](#what-stays-open)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Page cache**: the kernel's copy of a file's bytes, per open inode. A
  read served from it never reaches the daemon.
- **Drop**: the kernel discarding a file's cached pages — at an open
  answered without `FOPEN_KEEP_CACHE`, on a data notification
  (`FUSE_NOTIFY_INVAL_INODE`), on an attribute reply with a new size, and
  by memory reclaim.
- **Write-through write**: a buffered `write(2)` on a mount without the
  writeback cache (every Constellation mount): the kernel copies the bytes
  into its pages and sends a synchronous `FUSE_WRITE` before the call
  returns.
- **Stamp**: the identity of a file's committed content (size, times,
  manifest) that the engine answers an open with.
- **Suspect**: the frontend's mark that a file's cached pages may differ
  from what a read of the view answers.

## Details

### What the kernel caches

Entries (names) and attributes for the entry/attribute TTL (1 s in
bounded mode, 0 under `--cto strict`, up to 1 s for a lone strict
sequencer), and pages, which have no lifetime: they stay until something
drops them. Another node's change reaches the kernel as a notification
(`CONSTELLATION_KERNEL_INVALIDATE`, [Close-to-open modes](cto-modes.md));
a notification held back past its bound for a request in flight is
dropped, which costs entries and attributes nothing (they have expired)
but would leave pages stale — so a dropped data notification marks the
file suspect, and its next open drops the pages.

### Keeping a file's pages across opens (W1)

Before plan 42 every open dropped the file's pages, so a file read three
times with an open per read was read from the daemon three times. Now an
open keeps them (`FOPEN_KEEP_CACHE`) only when all of these hold:

- the engine answered a stamp: bounded mode, kernel invalidation on, a
  regular file, no data notification for it still on its way;
- the last open that dropped the pages was answered the same stamp, and
  its drop is known to be complete;
- nothing since could have made a page differ from what a read answers:
  no data notification written, lost or dropped (S1, S2), no write
  answered with an error or a short count (S3), no direct-I/O open (S4),
  nothing the race-1 bookkeeping could not vouch for (S5);
- the open is not a truncating `create`, not a passthrough open, and
  `CONSTELLATION_FUSE_KEEP_CACHE` is not `off`.

The engine also reports what changes content under a kernel without
changing the stamp or reaching that kernel: a write session dropped
without a commit, a commit through another view of the same node, and a
stranding rollback (a deposed holder's unshipped transactions restored
from their before-images) — each a data invalidation, so the file is
suspect. Under `--cto strict` no open keeps pages: every strict open drops
them, as before.

### Race 1 and the write-race repair (W2)

A write-through write copies into an uptodate page and unlocks it before
its `FUSE_WRITE` reaches the daemon. A drop in that window lets a read
fetch the page before the write is applied; the kernel then serves the
pre-write bytes for an acknowledged write until the page is dropped again
(on Linux 7.3, measured: about a thousand stale read-backs a minute with
one `open`/`close` loop beside the writers). The frontend notes every drop
it causes or can see coming and the pages reads fetched since; when the
write that may have been in the window is answered, it first invalidates
the pages those reads fetched — from a repair lane, never from the FUSE
worker, with a bound on the wait that runs only while nothing else of the
file is in flight. A read it cannot attribute to a known drop marks the
file suspect instead (its next open drops the pages, as every open used
to).

### Races 2a and 2b and directory attributes (W3)

- **2b**: a `GETATTR`/`LOOKUP` reply generated before a `mkdir` but
  processed after it cancels the directory's invalidation, and the old
  `st_nlink` is cached for the TTL. Every namespace reply is now preceded
  by an attribute-only notification for each directory it changed (and
  for the moved inodes of a rename, whose `ctime` changes), which makes
  the kernel reject such a reply. One 40-byte write per operation.
- **2a**: a `stat` racing another task's install of fresh directory
  attributes can copy the old `st_nlink` once. Closed only by giving
  directories' attributes no kernel lifetime (TTL 0), which costs a
  `GETATTR` per directory component of every path walk (the mount uses
  `default_permissions`): 2–11 times the FUSE requests of stat-heavy
  walks (`git status`, `make -q`, `find`); measured on ext4, xfs, btrfs and
  tmpfs, warm `git status` was 2.6 to 7.9 times and `make -q` 2.8 to 4.2
  times slower (7 000 against 293 000 FUSE requests). Under `--cto strict`
  it is the default; in bounded mode the default is the view's TTL
  (`entry`, plan 42 Q1): the exposure is one transient `stat` result of a
  directory, never data. `CONSTELLATION_FUSE_DIR_ATTR_TTL=zero` closes it
  at that cost.

### Kernels with the fixes

The kernel patches (0001 keeps write-through folios under writeback for
the `WRITE`; 0002 marks attributes valid only after storing them; 0003
bumps a directory's attribute version when it changes) make the
workarounds unnecessary. Each workaround's `auto` follows its fix's
verdict: a setting `on`/`off` overrides it; else the daemon probes 0001
and 0003 at its first mount, on a private mount of its own (a few hundred
milliseconds, in the background); else a table of releases decides (empty
until the patches are merged). An unknown verdict keeps the workaround on.
Keep-cache (W1) is independent of the kernel.

### Settings

| Variable | Values (default first) | What |
|---|---|---|
| `CONSTELLATION_FUSE_KEEP_CACHE` | `auto`, `off` | W1 |
| `CONSTELLATION_FUSE_WRITE_REPAIR` | `auto`, `on`, `off` | W2's repair (`off` on an unfixed kernel: hits mark the file suspect instead) |
| `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS` | `1000` | W2's bound |
| `CONSTELLATION_FUSE_DIR_ATTR_TTL` | `auto`, `zero`, `entry` | W3a (`auto`: `zero` under strict; `entry` in bounded mode, Q1) |
| `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY` | `auto`, `on`, `off` | W3b |
| `CONSTELLATION_FUSE_KERNEL_PROBE` | `auto`, `off` | the probe of the kernel's fixes |

Read once at start; an unparseable value fails the mount. The reference
entries are in [configuration](../configuration.md), "FUSE and runtime
threads".

### What stays open

On a kernel without the fixes:

- a race-1 stale read after a drop the daemon cannot see (memory reclaim,
  `fadvise`, `drop_caches`) — bounded by the next open, as before; on
  Linux 7.3 those paths do not drop a page a writer holds, so it did not
  reproduce;
- a stale page dirtied through a shared `mmap` before the repair lands;
- a repair past its bound (a write answered before its repair; the file
  is marked suspect);
- a direct-I/O write while another descriptor holds the file's pages;
- race 2a for regular files after a foreign change (one `stat` result);
- race 2a for directories in bounded mode while `entry` is the default.

## Troubleshooting

### Is keep-cache working?

`node.status`, `fuse.mounts[].kernel_cache`: `keep_cache_opens` counts the
opens that kept pages, `dropping_opens` the others by reason
(`stamp_changed`, `suspect`, `no_stamp`, `no_entry`, `drop_in_flight`,
`trunc`, `off`, `passthrough`), `suspect_marks` the marks by source
(`s1`–`s5`). Many `stamp_changed` drops on files nobody else writes are a
local writer's commits (each changes the content); many `suspect` drops
follow data notifications (another node's writes, lock grants).

### Writes are slower on a file another process keeps opening

With every open dropping the pages (`--cto strict`, or
`CONSTELLATION_FUSE_KEEP_CACHE=off`) and a reader of the same pages, many
writes wait for a repair: `kernel_cache.repairs`, `repair_wait_ms` and
`repairs_released` (a repair that waited on the write's own last page)
show it. `repairs_late` should be 0; a nonzero count means a repair hit
its bound — a notification that waited for something else, or a kernel
with the race-1 fix and `CONSTELLATION_FUSE_WRITE_REPAIR=on`.

### Which workarounds apply here?

`fuse.mounts[].kernel_workarounds`: each setting, whether it is in effect
(`write_repair_on`, `dir_attr_ttl_zero`, `dir_change_notify_on`), and per
fix the verdict and its source (`probe`, `table`, `override`).

## FAQ

- **Why not the writeback cache (`FUSE_WRITEBACK_CACHE`)?** It closes race
  1, but changes when writes reach the daemon, what `close` and `fsync`
  guarantee and who owns `mtime` — a design of its own, not a workaround.
- **Does keep-cache weaken close-to-open?** No: an open keeps only pages
  it can prove current; anything unknown drops, as every open did before.
  Under `--cto strict` every open still drops.

## References

- Plan 42 (`docs/plans/v1/wip/42-kernel-race-workarounds.md`)
- ADR-34 in [DECISIONS](../../explanation/DECISIONS.md)
- [Close-to-open modes](cto-modes.md), [Cluster locks](cluster-locks.md)
- Code: `crates/frontend-fuse/src/{pagecache,repair,kernel_fixes,workarounds,reply,notify}.rs`,
  `crates/engine/src/view/kernel_cache.rs`, `crates/engine/src/kernel_inval.rs`
