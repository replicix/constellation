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
  - [Directory listings with `READDIRPLUS`](#directory-listings-with-readdirplus)
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
  the kernel reject such a reply. The notice invalidates the whole
  attribute mask, so it costs the next path walk through the directory a
  `GETATTR` (`default_permissions`); under `--cto strict` that is free
  (directory attributes have no lifetime), and in bounded mode the notice
  is written only for an inode an attribute reply may still be on its way
  to the kernel for: a request being served across the change (its reply
  writes the notice itself), or a reply answered to a task that has sent
  no attribute or namespace request since and has not exited (a task
  installs a reply before its next request; the mark is dropped on that
  evidence, a notice for the inode or the task's exit, never by time, so
  a reply installed late under load is covered; a full table of marks
  writes the notice before the reply instead). An
  `untar` or `cp -a` in one task sends none; another task's `stat` of the
  directory is noticed once. Counted: `dir_attr_notices` written,
  `dir_attr_notices_skipped` not needed.
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

### Directory listings with `READDIRPLUS`

With `CONSTELLATION_FUSE_READDIRPLUS` on (`auto`, the default), the kernel
asks for a directory's entries together with their attributes (plan 40), so
`ls -l` costs no `LOOKUP` per name. An entry with attributes is what a
`LOOKUP` of its name answers, and the kernel takes it as one: a dentry, an
inode, one more lookup of its node id (given back by `FORGET`). The three
workarounds treat it as the attribute reply it is:

- **W3a**: a directory's entry carries attribute TTL 0 where `LOOKUP` does
  (`CONSTELLATION_FUSE_DIR_ATTR_TTL`), and keeps its entry TTL.
- **W3b**: a page installs the attributes of many inodes in one
  `getdents`. Each is marked for the listing task like any attribute reply,
  all in one set (a mark per inode, not one mark replaced by the next), so a
  namespace change of any listed inode that lands before the task's next
  request is noticed and the kernel rejects the stale entry. Under
  `--cto strict` a node that is not the lone sequencer hands every entry
  name-only (its attributes have no kernel lifetime, so the kernel would
  discard them), and a name removed or renamed between the page's names and
  its attribute read goes name-only on any mount. When the table of marks is full
  (a pid-0 lister, such as an `nfsd` re-export, of a huge directory) the
  rest of the page goes name-only instead of writing a notice per entry;
  the kernel looks those names up when it needs them, which is correct and
  slower.
- **A3**: a regular file's entry with a size other than the one last handed
  to the kernel arms the file's page-cache check exactly as a `LOOKUP`
  reply does, since the kernel drops the file's pages when it installs a
  new size.

`.` and `..` are always name-only (the kernel skips them and never forgets
them). Every entry that goes name-only is counted by reason
(`fuse.mounts[].readdirplus_name_only_why`).

What a listing costs the metadata store (plan 45, ADR-36). A directory
entry holds a copy of its inode's attributes only while the inode is a
directory or has one name; a file with two or more names has a *remote*
entry (`ino` and `kind`) and its attributes live in the inode record alone.

| Listing | Cost per listed name |
|---|---|
| plain `READDIR`, `readdir` pages | the entry's `ino` and `kind`, one range scan, whatever the number of names |
| `READDIRPLUS` entry, single-name file or directory | the attributes from the entry's copy: no extra read |
| `READDIRPLUS` entry, hard-linked file | one inode-record read (`0x01` point read, ~0.5 µs warm, one block cold), under the same shard as a `LOOKUP` of the name; `du` reads each inode once |
| `link`, `unlink`, `rename`, `chmod`, a write's close at *n* names | 1 to 6 keys, flat in *n* (it was *n* + 5) |

A tree without hard links lists exactly as it did before the rule; a tree
of them (a package manager's content store, `cp -al` backups) pays the one
read per hard-linked name.

### Kernels with the fixes

The kernel patches (0001 keeps write-through folios under writeback for
the `WRITE`; 0002 marks attributes valid only after storing them; 0003
bumps a directory's attribute version when it changes) make the
workarounds unnecessary, except W3b's notice for the inode a rename
moves: 0003 as first sent does not bump that inode's version, so the
notice stays on with it (plan 42 F16). Each workaround's `auto` follows its fix's
verdict: a setting `on`/`off` overrides it; else the daemon probes 0001
and 0003 at its first mount, on a private mount of its own (a few hundred
milliseconds, about 2 s on a kernel with 0001, in the background); else a table of releases decides (empty
until the patches are merged). An unknown verdict keeps the workaround on.
Keep-cache (W1) is independent of the kernel.

### Settings

| Variable | Values (default first) | What |
|---|---|---|
| `CONSTELLATION_FUSE_KEEP_CACHE` | `auto`, `off` | W1 |
| `CONSTELLATION_FUSE_WRITE_REPAIR` | `auto`, `on`, `off` | W2's repair (`off` on an unfixed kernel: hits mark the file suspect instead) |
| `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS` | `1000` | W2's bound |
| `CONSTELLATION_FUSE_DIR_ATTR_TTL` | `auto`, `zero`, `entry` | W3a (`auto`: `zero` under strict; `entry` in bounded mode, Q1) |
| `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY` | `auto`, `on`, `off` | W3b (`auto`: every operation under strict, narrowed in bounded mode; `on`: every operation; with kernel patch 0003 only the moved inode's notice of a rename remains) |
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
- race 2a for directories in bounded mode while `entry` is the default;
- race 2b for a reply installed more than a second after the daemon
  generated it (the daemon slow to hand it over, or the task not
  scheduled for that long), in bounded mode.

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
(`write_repair_on`, `dir_attr_ttl_zero`, `dir_change_notify_on`, and
`dir_change_notify_dirs_on`: false with patch 0003 while the moved inodes'
notices stay on), and per fix the verdict and its source (`probe`,
`table`, `override`). `/metrics` has the same as the gauge
`constellation_fuse_kernel_workaround{mountpoint,workaround}` (1 in
effect, 0 not; `workaround` is `keep_cache`, `write_repair`,
`dir_attr_ttl_zero`, `dir_change_notify` or `dir_change_notify_dirs`).

### Is `ls -l` served from the listing?

`fuse.mounts[].readdirplus` is the mode the connection agreed (`off` when
the kernel does not offer `READDIRPLUS` or the knob is `off`). With `auto`
or `always`, `readdirplus_entries` should rise by about one per listed name
and the `lookup` operations of `constellation_vfs_ops_total` should not.
`readdirplus_name_only_why` says why entries did not carry attributes:
mostly `dots`; `view` in numbers on a `--cto strict` mount that is not a lone sequencer; `watch_full` for
a pid-0 lister of a very large directory.

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
- Plan 40 (`docs/plans/v1/wip/40-readdirplus.md`)
- Code: `crates/frontend-fuse/src/{pagecache,repair,kernel_fixes,workarounds,reply,notify,plus_stats}.rs`,
  `crates/engine/src/view/kernel_cache.rs`, `crates/engine/src/kernel_inval.rs`
