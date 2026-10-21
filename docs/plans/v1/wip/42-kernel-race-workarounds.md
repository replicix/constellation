# Plan 42 — Working around three Linux FUSE races: conditional keep-cache, write-race repair, directory attributes

Read `docs/plans/v1/CONVENTIONS.md` first. This plan makes Constellation's
Linux FUSE frontend correct on today's kernels against three races in the
kernel's FUSE client that the kernel patch package (a draft series of three
`fs/fuse` patches with standalone reproducers, not upstream yet) describes and
reproduces on every distribution kernel from 5.10 to 7.3-rc7:

1. **A stale page after a write-through write** (patch 0001). A buffered
   `write(2)` copies into an uptodate folio and unlocks it before its
   `FUSE_WRITE` reaches the daemon (`fuse_fill_write_pages`). Anything that
   drops the file's pages meanwhile — an `open(2)` answered without
   `FOPEN_KEEP_CACHE` (every Constellation open today), one of this daemon's
   own `FUSE_NOTIFY_INVAL_INODE` notifications, a `GETATTR` reply that shows
   a new size, memory reclaim, `fadvise(DONTNEED)` — lets a read fetch the
   page from the daemon before the `WRITE` is applied. The kernel then serves
   the pre-write bytes for an acknowledged write until the page is dropped
   again; through a shared `mmap` the stale page can even be written back over
   the newer data.
2. **2a: attributes marked valid before they are stored** (patch 0002). A
   `stat` running while another task installs a `GETATTR` reply after an
   invalidation can see the mask cleared and `i_time` valid and copy the old
   `st_nlink` (or size, times) of a directory a completed `mkdir` changed.
3. **2b: a reply from before a directory change cancels its invalidation**
   (patch 0003). `fuse_dir_changed` invalidates without bumping
   `attr_version`, so a `GETATTR`/`LOOKUP` reply the daemon generated before a
   `mkdir` but the kernel processes after it is cached, and the old
   `st_nlink` is served for the whole attribute TTL.

The three workarounds were decided with the maintainer: answer `open` with
`FOPEN_KEEP_CACHE` when this node knows the kernel's pages are current;
re-invalidate a written range when a page may have been refetched under an
in-flight write; and give directory attributes no kernel cache lifetime in
bounded mode. Planning found that each, as first stated, is either unsafe
or incomplete (§4, findings F1–F16): keep-cache as stated makes stale pages
from undetectable drops **persist across opens** where today the next open
repairs them; a time-bounded record of notices is not bounded by anything
the kernel guarantees; re-invalidating the written range from the handler
deadlocks on the writer's own locked folio, and doing it after the reply
leaves the writer's own read-back stale; and a directory attribute TTL of 0
costs a `GETATTR` per directory component of every path walk, because the
mount uses `default_permissions` (measured, Appendix A). The design in §5
keeps the three ideas and fixes each:

- **W1 — conditional `FOPEN_KEEP_CACHE`** (§5.1): an open keeps the
  kernel's pages only when the inode's committed content is what it was at
  the last open that dropped them (a content stamp read from the replica in
  the same read the open already makes), no data notification for it is
  pending, dropped or delivered since, and nothing the frontend saw since
  could have left a stale page (a write error, a write through a direct-I/O
  handle, a read the race-1 repair could not rule out). Fail-safe: anything
  unknown drops, exactly as every open does today.
- **W2 — write-race repair** (§5.2): the frontend arms an inode before every
  page drop it causes or can see coming, records the page ranges of reads
  while an inode is armed or written, and when the first write that
  completes after a drop overlaps such a read, invalidates **only the pages
  those reads fetched** before that write is answered — on a dedicated
  repair lane with a bounded wait, never from a FUSE worker. The candidate
  write is identified by the kernel's own serialization (a write-through
  write holds the inode lock from copy to reply), not by a time bound.
- **W3 — directory attributes** (§5.3): 2b is closed outright by an
  attribute-only notification for the parent directory, written before the
  reply of every local namespace operation (it bumps the kernel's
  `attr_version`, so a reply generated before the change is rejected; the
  moved inode of a `rename` too). 2a can be closed only by answering
  directory attributes with an attribute TTL of 0 while keeping the 1 s
  entry TTL, at a cost Appendix A measures as material; whether bounded
  mode pays it is open (Q1, pending a quiet-host measurement, §8.1).

On kernels known to carry the fixes, W2 and W3 switch off by themselves
(§5.4: a version table filled in once the patches land, plus manual
overrides); W1 stays, as a speed win. `--cto strict` keeps dropping the
cache at every open (§5.5). What remains open on an unfixed kernel is listed
in §5.9 with its exposure.

Rules carried into this plan from the maintainer: consistency and integrity
first, then performance, never one for the other (a trade-off is reported,
not shipped); trace the history and the existing integrity arguments before
changing the kernel-invalidation or reply code, and extend them in code
comments and docs; no backward-compatibility shims and **no version bumps**
(`HANDOVER_VERSION`, handoff and on-disk formats change in place); every fix
comes with a test that fails before it; correct under host load (CPU
starvation, pauses, slow disks) with only latency allowed to degrade; a
change must be neutral or better on ext4, xfs, btrfs and tmpfs backing
stores, measured; complex changes get an independent review before they
land.

## Dependencies

- Plan 30 (§M7 kernel invalidation, §M8 `--cto strict` and the TTL rules,
  §M14 lock-grant invalidation), plan 31 (C4 `Vfs` contract and the FUSE
  adapter, C4b session handover, the conformance kit), plan 38 (the
  replicix `fuser` fork, io_uring transport, passthrough and zero-copy
  opens), plan 39 (the view's handle table). All committed.
- Plan 40's R2 (kernel lookup counts in the adapter, `crates/frontend-fuse/src/refs.rs`,
  commit `91591c6`): committed; W1/W2's per-inode table follows its lifetime
  (`Forgot::Last`).
- Plan 40 (READDIRPLUS) is **not** a dependency. If it lands first, its
  plus entries are attribute-bearing replies and §5.3's directory TTL and
  §5.2's size-change arming apply to them (called out where they matter);
  if this plan lands first, plan 40 inherits both rules (record in plan
  40's §4.5 when committing this one).
- No `fuser` fork change is needed: `ReplyEntry::entry_with_ttls` (separate
  entry and attribute TTLs), `Notifier::inval_inode` with a byte range and
  `FopenFlags::FOPEN_KEEP_CACHE` exist in the pinned revision
  (`b6af219`).

## 1. History

Verified against `main` at `28fec6a`.

| When | Commit | What |
|---|---|---|
| 2026-08-22 | `8e5b5bd` (phase 1) | DESIGN §12 lists "Writeback cache + `FOPEN_KEEP_CACHE` … invalidated exactly on remote change" as a fast path; never built. Every open since is answered without `FOPEN_KEEP_CACHE`, which is what made close-to-open hold for pages: the kernel drops a file's pages at every open. |
| 2026-09-24 | `0f9583d` (plan 30 M7) | Kernel invalidation for foreign applies (`kernel_inval`), 1 s entry/attribute TTL (`view::TTL`). |
| 2026-09-25 → 26 | `df14686`, `ee3f65b` (plan 30 M14) | Lock grants drop the file's pages and attributes (`InodeInvalidator::invalidate_and_wait`, `crates/engine/src/locks.rs:823`); the holder's kernel sees other nodes' executed ops (`note_foreign_executed`). |
| 2026-09-26 | plan 30 M8 | `--cto strict`: entry and attribute TTL 0 (`View::ttl`, `crates/engine/src/view/mod.rs:1227`), except a lone strict sequencer (`cto::lone_kernel_ttl`, up to 1 s). |
| 2026-09-27 | `b2c3436` | A killed daemon wedged by its own notification (EC2 campaign 6, B-1): one dedicated notifier thread, hold-back while a request is in flight on the inode, drop past `DEFER_MAX` (`crates/engine/src/kernel_inval.rs:146`). The drop's argument — "everything the kernel cached before the apply has expired one TTL later" — holds for entries and attributes, **not for pages** (pages have no TTL; F3). |
| 2026-09-29 | `650021b`, `43268fc` (plan 31 C4, C4b) | The adapter over `Vfs`; `FrontendEvents`; session handover with a notify gate that drops invalidations during the gap and re-invalidates every open file's data at resume (`crates/cli/src/handover.rs:1240`). |
| 2026-10-03 | `5f289b4` | Entry invalidations only for names the kernel may hold (`KernelEntries`). |
| 2026-10-09 | `8a6f96c` | xcheckfs differential runs: stale reads in the write-back lanes traced to the kernel (race 1); pending sessions overlay size/mtime in `getattr`. |
| 2026-10-13 | (kernel patch package) | Race 1 found on a Linux test host with an experimental FUSE file system "neither using `FUSE_WRITEBACK_CACHE` nor `FOPEN_KEEP_CACHE`" (Constellation): 320 stale reads in 160 k writes with one extra `open`/`close` thread, 0 without; `copy_file_range` through the splice fallback persisted 256–630 corrupt copies per 20 s. Races 2a/2b found with `nlink_rw.py`. |
| 2026-10-14 | `2acd591` | `FUSE_COPY_FILE_RANGE` answered by a copy in the daemon (`view::copy`), closing race 1's corruption-at-rest path through splice. The stale page itself remains. |
| 2026-10-13 → 14 | `91591c6` (plan 40 R2) | Kernel lookup counts in the adapter (`refs.rs`): a per-kernel-inode table freed at the last `FORGET`. |

## 2. The races, as they reach Constellation

All kernel references are to mainline `4056b3c9d3c9` (v7.3-rc7-28), as the
kernel patch package cites them.

### 2.1 Race 1: stale page after a write-through write

Mechanism: `fuse_cache_write_iter` (`fs/fuse/file.c`) takes `inode_lock`
and calls `fuse_perform_write`; `fuse_fill_write_pages` (file.c:1315)
copies the user data into page-cache folios and **unlocks every uptodate
folio before the request is sent**; `fuse_send_write_pages` (file.c:1237)
sends the synchronous `WRITE` and, after the reply, unlocks the last folio
only if it was not uptodate (`ia->write.folio_locked`, a partial write into
an uncached folio). An `invalidate_inode_pages2*` in that window removes the
unlocked folio; a read, readahead or fault of any byte of that page then
allocates a new folio and reads it from the daemon, which has not applied
the `WRITE` yet; the new folio is uptodate with the old bytes, and stays so
after the `WRITE` returns.

Three kernel facts (KF1–KF3) the design below relies on, each from that code:

- **KF1 — one write-through write per inode at a time.** `fuse_cache_write_iter`
  holds `inode_lock` across the copy, the `WRITE` round trip and the reply
  (also for `O_DIRECT` writes on a cached handle, `generic_file_direct_write`
  under the same lock; `fuse_direct_write_iter` takes it too — exclusively,
  since Constellation never sets `FOPEN_PARALLEL_DIRECT_WRITES`). A `write(2)`
  larger than one request sends its requests one after another, each filled
  after the previous one's reply. So at any instant at most one buffered or
  direct `WRITE` of an inode is anywhere between "bytes copied into the page
  cache" and "answered". Only page writeback (`FUSE_WRITE_CACHE` in the
  request's `write_flags`, a shared `mmap` store's writeback) runs without
  the lock, and its folios are under writeback for the whole round trip, so
  nothing drops them.
- **KF2 — the writer holds a lock only on a folio it filled itself.** The
  only folio a write-through writer keeps locked past the copy is the last
  one of its request when it is not uptodate after the copy (a partial write
  into a folio that was not cached). A read cannot be in flight on that
  folio (a read locks the folio it fills, and the writer's
  `__filemap_get_folio(FGP_WRITEBEGIN)` would have waited for it) and
  readahead skips present folios. So a folio a *read* allocated and filled
  is never locked by an in-flight writer — unless it was dropped once more
  and the writer re-created it.
- **KF3 — the window is not bounded in time.** It spans the rest of the fill
  loop (a 1 MiB request copies 256 pages after unlocking the first),
  `fuse_simple_request` allocating and queueing the request, its time in
  the kernel's queue, the daemon worker's read of it and the worker's path
  to the engine. A preempted or CPU-throttled writer (cgroup `cpu.max`)
  stretches the part before the queue arbitrarily; Constellation negotiates
  no FUSE request timeout (`FUSE_REQUEST_TIMEOUT`, v6.14+, which would
  abort the whole connection and starts only at queueing anyway). No
  expiry time for "a write may still be in flight" is a guarantee (F4).

What drops a file's pages in a Constellation mount, today:

| Trigger | Where | Daemon knows when |
|---|---|---|
| `open`/`create` answered without `FOPEN_KEEP_CACHE` | `fuse_open` file.c:293, `fuse_create_open` dir.c:924 | yes: it sends the reply |
| `open(O_TRUNC)`: this adapter does not negotiate `FUSE_ATOMIC_O_TRUNC` (`crates/frontend-fuse/src/passthrough.rs:88`), so the kernel strips `O_TRUNC` from the `OPEN` (`fuse_send_open`) and truncates with a `SETATTR size=0` without a file handle (`fuse_do_setattr`, `ATTR_OPEN`) | `truncate_pagecache` + `invalidate_inode_pages2` after the `SETATTR` reply, under `inode_lock` | yes: it answers the `SETATTR` (A3) |
| `create` with `O_TRUNC` (the `CREATE` keeps the flag) | `fuse_create_open` | yes |
| A data notification (`FUSE_NOTIFY_INVAL_INODE`, offset ≥ 0): a foreign apply, a lock grant, the handover resume | `fuse_reverse_inval_inode` inode.c:570 | yes: the sink writes it |
| An attribute reply with a size different from `i_size` (no `FUSE_EXPLICIT_INVAL_DATA`) | `fuse_change_attributes_i`: `truncate_pagecache` + `invalidate_inode_pages2` | yes, if it tracks the size it told the kernel |
| `FUSE_AUTO_INVAL_DATA` mtime checks | `fuse_change_attributes_i` | n/a: not negotiated (fuser's `INIT_FLAGS` is `FUSE_ASYNC_READ | FUSE_BIG_WRITES`, the adapter's `init` adds none) |
| Memory reclaim, `fadvise(DONTNEED)`, `drop_caches`, cgroup memory limits | mm | **no** |

Evidence on Constellation: the package's original runs (an `open`/`close`
thread beside a randomized checker: 1 stale read in 5–10 runs; the `pcrace`
pattern 320 in 160 k writes) and this plan's baseline on `28fec6a`
(Appendix A.3). `copy_file_range`'s persisted corruption is closed since
`2acd591`; the stale page that a later `read`, `sendfile`, `mmap` or backup
tool sees is not.

### 2.2 Race 2a: cached attributes marked valid before they are stored

`fuse_change_attributes_common` (inode.c:210) clears `inval_mask` (l. 232)
and sets `i_time` (l. 235) before it stores `nlink`, mode, times (l. 239 on)
and before the caller stores `i_size`; `fuse_update_get_attr` (dir.c:1539)
reads the mask and `i_time` without `fi->lock` and copies the inode's fields.
A `stat` concurrent with another task's install after an invalidation
returns the pre-invalidation values. The stale value is returned once (the
cache itself ends up right), but by a `stat` that began after the change
completed. Reproduced by `nlinkrace` (8 threads, `mkdir`/`rmdir` serialized
against `stat`s by a reader/writer lock): 403 in 4.27 M stats on the
package's VM, on every distribution kernel tested, and on Constellation
(Appendix A.3). It needs an attribute install racing with cached reads, so it
is closed when the kernel never treats a directory's attributes as valid:
an attribute TTL of 0 makes `i_time` 0, and every `stat` then refreshes
(F9). Regular files are exposed the same way in principle, but only through
foreign changes and lock grants (a local write or `setattr` updates the
kernel's fields itself and bumps `attr_version`); §5.9 lists it.

### 2.3 Race 2b: a pre-change reply cancels a directory's invalidation

`fuse_dir_changed` (dir.c:325) runs after every `mkdir`, `rmdir`, `create`,
`unlink`, `rename`, `link`, `symlink`, `mknod` reply and invalidates the
directory's attributes without bumping `fi->attr_version`. A reply carrying
the directory's attributes — `GETATTR` of it, `LOOKUP` of its name, a
`READDIRPLUS` entry for it — whose request was sent before the change passes
the `attr_version` check (inode.c:343), stores the old attributes and clears
the invalidation; the old `st_nlink` is then cached for the whole attribute
TTL (1 s here). `fuse_reverse_inval_inode` (inode.c:559) does bump
`attr_version`, which is why a *foreign* change, told by a data notification,
is not exposed. `dirver` reproduces it deterministically with a server that
delays `GETATTR` replies (20 of 20 rounds on every kernel); Constellation
reproduces it with this plan's fault point (§7.3).

## 3. What the code does today

- **Open replies.** `View::open` (`crates/engine/src/view/ops.rs:1009`)
  answers `Opened { fh, backing, zero_copy }`; the adapter's `OpenReply`
  (`crates/frontend-fuse/src/reply.rs:280`) turns `OpenAnswer::Plain` into
  `FopenFlags::empty()` (`fopen`, `reply.rs:260`) and passthrough into
  `FOPEN_PASSTHROUGH` (± `FOPEN_DIRECT_IO`); `CreateReply` (`reply.rs:334`)
  likewise. No reply carries `FOPEN_KEEP_CACHE`, so the kernel drops every
  page of the file at every open. That is the only thing keeping a
  remote change from being served from pages an earlier open cached when
  its notification was dropped (F3), and the only repair of a race-1 stale
  page (the next open).
- **TTLs.** `View::attr_out` (`mod.rs:1179`) puts `View::ttl()` (1 s; 0
  under strict; `lone_kernel_ttl` for a lone strict sequencer) into every
  `Attr`; `EntryReply` uses it for both the entry and the attribute
  validity (`ReplyEntry::entry`), `AttrReply` for the attributes. Directories
  and files alike. The mount uses `default_permissions`
  (`crates/platform/src/mounts.rs:127`), so `fuse_permission` refreshes a
  directory's attributes whenever they are invalid or expired, on every path
  component a walk crosses (`MAY_EXEC`).
- **Push invalidation.** `kernel_inval` (module doc,
  `crates/engine/src/kernel_inval.rs:1-132`): foreign applies
  (`invalidations`, `:532`) become `Inval::Entry` and `Inval::Inode { data }`
  notifications; one `kernel-inval` thread sends them; a notification whose
  `gate()` inode (`:168`) has a request in flight (`InFlight`, registered by
  `lookup`, `readdir`, namespace ops, `setattr`, `read`, `write`, `fallocate`,
  `copy_file_range` — not `open`, `getattr`) is held back and dropped after
  `DEFER_MAX` = 1.5 s (`:146`, `:749`) with a debug line and nothing else.
  Attribute-only notifications are never held back ("takes no sleeping
  lock"). The sink (`FuseNotifySink`, `crates/frontend-fuse/src/notify.rs:158`)
  writes `inval_inode(ino, 0, 0)` for data, ignores every error, and drops
  everything while its handover gate is closed.
- **Local changes reach no other kernel.** Only foreign applies are
  reported; a commit by another view on the same node, a control-plane or
  reintegration change produces no notification (each view has its own
  write sessions, `WriteShards::new()` in `View::new`, `mod.rs:516`). Today
  harmless for pages (the next open drops them).
- **Writes.** `View::write` (`ops.rs:1196`) registers `InFlight`, runs
  `do_write` and answers (for `O_SYNC`, after the publication, on the fsync
  pool). The adapter (`adapter.rs:1266`) ignores `write_flags`
  (`FUSE_WRITE_CACHE` marks page writeback).
- **Per-kernel-inode state.** `KernelRefs` (`crates/frontend-fuse/src/refs.rs`):
  sharded map, about 32 bytes per inode the kernel holds, entry removed at
  `Forgot::Last` (and at `Underflow`), exported across a handover.

## 4. Findings: what is wrong with the workarounds as first stated

Each finding names the fix in §5.

- **F1 — keep-cache makes undetected stale pages persistent.** Today a
  race-1 stale page lives until the next open of the file. With
  `FOPEN_KEEP_CACHE` it lives until reclaim or a notification — possibly
  for the mount's lifetime, across every later open by every process. The
  drops Constellation cannot see (reclaim, `fadvise`, `drop_caches`, cgroup
  memory limits — the last routine in Kubernetes pods) still cause race 1.
  As first stated ("keep when no notice was skipped"), W1 would turn a
  stale read that the next open repairs into one that nothing repairs.
  **Fix:** W1 keeps only while W2's bookkeeping has ruled out every read
  that could have fetched a page under an in-flight write since the last
  drop; a read it cannot rule out marks the inode *suspect* and the next
  open drops, as today (§5.1, §5.2.4). On a kernel with patch 0001 no read
  can fetch such a page, so the rule is moot there.
- **F2 — "no notice skipped" is not "the pages are current".** Changes that
  never produce a notification for this kernel: a commit through another
  view of the same node (a second mount, a subtree mount, a CSI volume of
  the same filesystem), control-plane and reintegration changes,
  `CONSTELLATION_KERNEL_INVALIDATE=0`. Changes whose notification is still
  queued or held back when the open is answered. Notifications the sink drops
  (handover gate) or that fail. **Fix:** W1 compares a content stamp of the
  committed inode, read by the open from the replica, with the stamp at the
  last drop (catches every committed change by any path), refuses while a
  data notification for the inode is pending in the engine, and marks the
  inode suspect for every data notification delivered, dropped or failed
  (§5.1).
- **F3 — `DEFER_MAX`'s argument does not hold for pages.** The comment and
  the doc say a held-back notification can be dropped because everything
  cached before has expired one TTL later. Pages never expire. Today the
  drop-at-open hides it; with W1 a dropped data notification must mark the
  inode suspect, and the comment, `configuration.md:532` and
  `cto-modes.md:177` must say what the drop costs (§5.1.3, K6).
- **F4 — no time bound on "a write may be in flight".** The record of
  recent notices with an expiry "after the kernel's FUSE request timeout"
  is unsafe: the timeout is not negotiated, is optional and aborts the
  connection when it fires, and does not cover the pre-queue part of the
  window (KF3), which CPU starvation stretches without limit. **Fix:** KF1
  identifies the only write that can be in the window at a drop — the first
  non-writeback write of the inode to complete after it — so the record is
  resolved by an event, not a clock (§5.2.2).
- **F5 — the obvious re-invalidation deadlocks or comes too late.**
  Invalidating the written range from the `WRITE` handler before the reply
  blocks on the writer's own locked tail folio (KF2) — forever, since the
  reply that unlocks it waits for the notification — and on a kernel with
  patch 0001 on every folio of the request (they are under writeback).
  Invalidating after the reply, through `kernel-inval`, leaves the writer's
  own `pread` right after `pwrite` (the `pcrace` pattern, and any
  read-your-write) racing the notification, which it usually wins, plus a
  window in which other readers see new data then old again. **Fix:**
  invalidate only the pages that recorded *reads* fetched (KF2: never the
  writer's folio on an unfixed kernel, except after a further unseen drop),
  before the reply, from a repair lane with a bounded wait so that no
  worker ever waits on a notification and the rare blocked case costs
  latency, not a hang (§5.2.3).
- **F6 — own notifications are not the only drops.** The open without
  keep-cache (the trigger in every original reproduction), the truncating
  `SETATTR` an `open(O_TRUNC)` turns into (no `FUSE_ATOMIC_O_TRUNC`), lock-grant notifications, the handover resume's re-invalidation and
  the size-change refresh drop pages too. **Fix:** all of them arm the
  inode (§5.2.1).
- **F7 — a stale page can be written back.** If an application dirties the
  stale page through a shared writable `mmap` before the repair lands, its
  writeback (or the repair's own `launder_folio`) writes the old bytes over
  the acknowledged write. The repair shortens the window to the `WRITE`'s
  service time; it cannot close it (§5.9).
- **F8 — directory TTL 0 is a per-walk cost, and strict is not all 0.**
  With `default_permissions`, an expired directory means a `GETATTR` for
  every component of every path lookup, not only for `stat` of the
  directory; Appendix A measures it per workload. A lone strict sequencer
  answers with `lone_kernel_ttl` (up to 1 s), not 0, so strict mounts are
  exposed to 2a/2b too. **Fix:** §5.3 applies the directory rules to both
  modes.
- **F9 — 2b has a cheap complete fix; 2a does not.** An attribute-only
  `FUSE_NOTIFY_INVAL_INODE` for the parent, written after the change is
  applied and before its reply, bumps `fi->attr_version` above the snapshot
  every earlier `GETATTR`/`LOOKUP`/`READDIRPLUS` request took, so their
  replies are rejected whenever they arrive; it takes no sleeping lock and
  can be written from the handler. 2a, by contrast, needs `i_time` already
  expired at the moment of the racing install, i.e. every reply that
  carried the directory's attributes in the last TTL had attribute TTL 0: a
  "TTL 0 only for recently changed directories" rule leaves the first
  change after a quiet period exposed. **Fix:** §5.3.
- **F10 — forcing W2 on a fixed kernel costs a timeout per affected write.**
  With patch 0001, recorded pages can be the writer's own folios under
  writeback (a read before the write's copy), so a forced repair waits for
  its bound. Hence `auto` off on fixed kernels and the documented warning
  (§5.4).
- **F11 — a write error leaves a newer page than the daemon's.** After a
  failed `WRITE` the uptodate folio keeps the unwritten bytes
  (`1a061c554253`, v7.3-rc1); today the next open drops it. **Fix:** a
  failed write marks the inode suspect (§5.1).
- **F12 — direct-I/O handles write past the page cache.** A write through a
  handle answered `FOPEN_DIRECT_IO` (plan 38's read-write passthrough
  opens) does not update cached pages another handle may hold. **Fix:**
  such a write marks the inode suspect (§5.1).
- **F13 — a write session can end without becoming the row.** The write
  succeeded, the kernel's pages hold its bytes, and the engine later
  retires or drops the session without committing it: `publish_inode`'s
  `FlushFail { ws: None }` (`crates/engine/src/view/flush.rs:887`), the
  lapsed-grant refusal (`:811-858`) and `discard_lock_writes`
  (`lock_gate.rs:147`) — which invalidate only under `--locks cluster`
  (`if let Some(l) = self.cluster_locks()`), so a `--locks local` discard
  is silent —, and the last close's `drop_writes` after an `ESTALE`/
  `NotFound` flush (`ops.rs:1470`). The row and its stamp do not change and
  no notification is produced, so W1 as first designed would keep pages the
  view no longer answers; today the next open drops them. **Fix:** every
  such path pushes a data invalidation, unconditionally (§5.1.5).
- **F14 — a stranding rollback restores a row verbatim.** Deposition
  recovery and the takeover gate roll unshipped transactions back from
  their before-images (`Meta::strand_below_epoch`, `strand_for_takeover`,
  `strand_recalled_speculation`, `crates/meta/src/store/spec.rs:3136`,
  `:3181`, `:3221`): manifest, size, mtime **and ctime** return to the
  pre-transaction values, so the stamp returns to one a dropping open may
  have recorded, while this kernel holds the rolled-back bytes; no hook
  fires. **Fix:** the stranding pass reports what it rolled back and the
  engine invalidates it (§5.1.5).
- **F15 — a kernel with patch 0001 the table does not know makes every
  repair a stall.** With 0001 the writer's folios are under writeback until
  the reply, so a repair overlapping them waits for the bound. Stable
  backports will not be visible in version numbers, so a wall-clock bound
  alone turns a missing table row into a 1 s latency on every repaired
  write, and a wall-clock bound also fires spuriously when the repair
  merely waits for a starved worker. **Fix:** an in-flight-aware bound and
  a deterministic mount-time probe of 0001's behaviour (§5.2.3, §5.4).
- **F16 — the renamed inode's ctime is the same race as 2b.**
  `fuse_rename_common` updates or invalidates the moved inode's ctime
  (`fuse_update_ctime`) without bumping its `attr_version`, so a reply for
  it generated before the rename caches the old ctime for the TTL. **Fix:**
  W3b notifies the moved inode(s) too (§5.3.1).

## 5. Decisions and design

### 5.1 W1: `FOPEN_KEEP_CACHE` when the pages are known current

An open keeps the kernel's pages of a regular file iff **all** of these hold
when its reply is written:

1. the engine answered a cache stamp `s` for the inode (§5.1.1: bounded
   mode, invalidation on, a regular file, no data notification pending);
2. the frontend's entry for the inode says the last open that *dropped* the
   pages was answered with the same stamp `s` (§5.1.2);
3. the entry is not *suspect* — nothing seen since that drop could have
   left a page that differs from what a read of the view answers (§5.1.3);
4. the open is not a `create` carrying `O_TRUNC` (an `open(O_TRUNC)` never
   reaches the daemon with the flag: the kernel sends a `SETATTR size=0`
   first, which A3 covers and whose shrinking reply the kernel follows with
   its own drop), the answer is `OpenAnswer::Plain` (a plain or
   zero-copy open; never passthrough), and `CONSTELLATION_FUSE_KEEP_CACHE`
   is not `off`.

Otherwise the reply carries no `FOPEN_KEEP_CACHE` (the kernel drops the
pages, as today), and *before* its bytes are written the entry records the
drop: `dropped_at = s` (or `None` when the engine answered none), `suspect =
false`, and the inode is armed for W2 (§5.2.1). An inode with no entry is
not current. Every rule fails towards dropping.

#### 5.1.1 Engine: the cache stamp (`constellation-vfs`, `constellation-engine`)

- `Opened` (`crates/vfs/src/types.rs:114`) gains `pub cache: Option<CacheStamp>`,
  `CacheStamp(pub u128)`, documented as "the content identity of the inode as
  this open read it; a frontend may keep the pages it cached since a drop
  answered with the same stamp, if nothing else it knows of says otherwise.
  `None`: drop." `Opened::new` sets `None`; frontends that do not cache
  ignore it.
- `View::open` (`ops.rs:1009`) and `View::create` (the `Opened` it
  answers for an existing file; a new file has no pages, its stamp is
  answered too) set it when **all** hold, else `None`:
  - not `--cto strict` (any strict view, the lone sequencer included: §5.5);
  - the view is registered with the kernel invalidator (`InFlight` not
    `disabled()`: `CONSTELLATION_KERNEL_INVALIDATE=0` answers `None`), or
    the view is frozen (a snapshot view: content immutable, the stamp is a
    constant per inode, `(snapshot root, ino)`);
  - the inode is a regular file (live or a frozen snapshot file);
  - **no data notification for the inode is pending** in `kernel_inval`:
    a new `InFlight::data_pending(ino) -> bool` over a per-inode counter in
    `State` (`kernel_inval.rs:220`) — incremented in `Shared::push`
    (`:256`) for every `Inval::Inode { data: true }` of a `Batch` or
    `Waited`, decremented by the run thread when a merge coalesces it into
    an entry already pending, when it is sent and when it is dropped (each
    under the state lock). A notification still in the queue, held back
    for a request in flight, or being written counts as pending. The send
    loop writes one notification to every view's sink in turn; the counter
    is decremented once, after the last view's sink returned (per view it
    would under-count on a multi-view node).
  - the stamp itself: a new `Meta::content_stamp(ino) -> Result<Option<CacheStamp>, MetaError>`
    (and its scratch counterpart) that decodes the inode's `0x01` record
    once and hashes, with `blake3` (already a `constellation-meta`
    dependency) truncated to 128 bits,
    `(size, mtime_ns, ctime_ns, manifest identity)`, where the manifest
    identity is the record's `InodeRecord::manifest` payload
    (`crates/mtree/src/record.rs:486`) as stored: the `BlobHash` of a
    `Payload::Spilled` manifest, the bytes of an inline one (no blob read,
    no hashing of a large manifest). `View::open` uses this one record read for both the
    attributes it needs today and the stamp (no second point read). A
    symlink/dir/special file answers `None`.
- `View::open`'s synthetic `Frozen` file branch answers the constant stamp.

Why a stamp of the committed row and not a count of notifications: it
catches every committed content change by any path — another view of the
same node, a control-plane or reintegration change, a foreign apply whose
notification is still being built — because the open reads the replica
itself (F2). What a view's own write session holds is not in it: those
bytes are in this kernel's pages already (write-through), and the commit
of them changes the row, so the next open after a close drops once
(today's behaviour; a later optimization may exempt a view's own commits,
§10 Q4). ABA (content changed and changed back to an identical row) is covered by
rule 3: every foreign change's notification marks the entry suspect, and
so do §5.1.5's events — a commit through another view of the node, a
session dropped uncommitted, a stranding rollback (which restores a row
verbatim, ctime included). A commit through *this* view changes the row
under pages that already hold its bytes, so needs no event. (Before
§5.1.5, a commit through another view could in principle have repeated a
row: a commit's `ctime` is `max(row ctime, min(session ctime, now))`,
`commit_ctime`, `crates/meta/src/store/writes.rs:43`; K2's unit test still
pins that two successive commits of different content yield different
stamps.)

#### 5.1.2 Frontend: the page-cache table (`crates/frontend-fuse/src/pagecache.rs`, new)

`PageCacheTable`: sharded like `KernelRefs` (same `SHARDS`, a `Mutex<HashMap<Ino, PcEntry>>`
per shard), one per session (`FuseFs`), keyed by the kernel's node id
(the view's numbering, as the replies). `PcEntry`:

```rust
struct PcEntry {
    dropped_at: Option<CacheStamp>, // W1: the stamp of the last dropping open
    suspect: bool,                  // W1: see 5.1.3
    live_arms: SmallVec<[Arm; 2]>,  // W2: drops announced, not yet known complete
    drop_done: bool,                // W2: a drop completed, no write claimed since
    handles: u32,                   // W2: open handles (any access) of this session
    write_handles: u32,             // W2: open handles with write access
    pending: SmallVec<[PendingWrite; 1]>, // W2: non-writeback WRITEs in progress
    reads: ReadSet,                 // W2: page ranges of reads, <= 16 ranges inline
    last_size: Option<u64>,         // W2: the size last handed to the kernel
}
```

- Created by the first open/create reply, arming, write registration or
  write-handle count of an inode; removed when `refs.forgot` answers
  `Forgot::Last` or `Forgot::Underflow` (`adapter.rs:1387`): the kernel
  has evicted the inode and its pages, or the count is in doubt and absence
  is the safe state. `Unknown`/`Partial` keep it (an entry of a node id
  whose count is unknown cannot arise: entries are created only by replies
  and requests for inodes a reply already counted — state the invariant
  with a `debug_assert` at the `Unknown` arm).
- **Not exported across a handover.** A resumed session starts with an
  empty table: no inode is current, the first open of each drops once;
  `handles` and `write_handles` are seeded from the view's handle snapshot
  (`HandleTableSnapshot::opens` and `::writers`, `crates/engine/src/view/handoff.rs`),
  passed by `resume_mount` (`crates/cli/src/handover.rs:1179`) to
  `FuseSession::resume`. The table is built, seeded and attached to the
  responders **before** the resumed session reads its first request, so
  the resume's re-invalidation thread (`handover.rs:1240`, which writes
  `Invalidation::Data` straight through the sink) and the first replies
  find it. No handoff format change.
- A remount is a new session: empty.
- Memory: at most one entry per inode the kernel holds (bounded by the
  kernel's inode cache, which `refs` already tracks), about 160 bytes with
  a full inline `ReadSet`; an entry exists only for inodes opened, written
  or invalidated in this session.

The decision lives in `OpenReply::done` / `CreateReply::done`
(`reply.rs:280`, `:334`), which already compute the `OpenAnswer`: on
`Plain`, look the entry up under its shard lock, decide, record, unlock,
then write the reply with or without `FopenFlags::FOPEN_KEEP_CACHE` (OR-ed
into the plain or zero-copy flags). The responder carries the table
(`OpenCtx` gains `pc: Arc<PageCacheTable>`).

#### 5.1.3 Suspect marks

`suspect = true` (creating no entry: an absent entry is already not
current) for:

| Mark | Where | Why |
|---|---|---|
| S1 every `Invalidation::Data` the sink writes, any source (foreign apply, lock grant, handover resume) | `FuseNotifySink::invalidate`, before the write | ABA; a lost-in-kernel failure (`invalidate_inode_pages2` returns `EBUSY` for a folio it could not drop and the kernel ignores it) |
| S2 every data notification **not delivered**: dropped past `DEFER_MAX`, refused by the closed handover gate, or a sink write that failed with anything but `ENOENT` | new `FrontendEvents::undelivered(&self, batch: &[Invalidation])` (default no-op), called by `kernel_inval`'s run thread for each drop (`:749`) in every view's numbering; the sink's own gate and error paths | F3: pages have no TTL |
| S3 a `WRITE` (writeback or not) answered with an error **or a short count** (`n < len`: the kernel turns it into `-EIO` in `fuse_perform_write` and keeps the unwritten tail in the folios) | `WriteReply::done` | F11 |
| S4 an open answered `FOPEN_DIRECT_IO` (passthrough read-write, plan 38) | `OpenReply::done` / `CreateReply::done` | F12: its writes bypass the page cache |
| S5 W2 could not rule out a stale read, or a repair was late or skipped | §5.2.3, §5.2.4 | F1 |
| (via S1) every engine event of §5.1.5: a write session retired or dropped without a commit, a commit through another view, a stranding rollback | the engine pushes `Inval::Inode { data: true }`, which the sink writes (S1) or reports undelivered (S2) | F13, F14 |

A dropping open clears the mark (§5.1); nothing else does.

#### 5.1.4 Why a kept page is current

Claim: when an open is answered with `FOPEN_KEEP_CACHE`, every page the
kernel holds for the inode equals what a read of the view answered or would
answer at the time the open read its stamp, except through the residuals of
§5.9.

Let `D` be the dropping open whose stamp `s` the entry holds. Pages present
at the keeping open `O` entered the cache after `D`'s drop (`D`'s reply was
written after `dropped_at = s` was recorded, and the kernel drops before
`D`'s `open(2)` returns; `invalidate_inode_pages2` failures are §5.9's R7):

- **Read-filled pages** are what the view answered when read. A content
  change committed after that read changes the row, so `O`'s stamp differs
  from `s` and `O` drops — unless the row is byte-identical again (ABA),
  which needs a foreign change (its notification sets S1, or is pending and
  fails rule 1, or was dropped and sets S2) or a local commit through
  another view repeating the row exactly (R8). A change committed after `O`'s stamp read is concurrent
  with `O`, as in today's bounded mode; its notification (pending, so it
  will be delivered or set S2) drops what `O` kept.
- **Write-filled pages** hold what this kernel wrote through: current
  unless the write failed or was short (S3), went through a direct handle
  (S4), was never committed (§5.1.5: a data invalidation, so S1/S2), or
  race 1 replaced a page under it — which W2 either repairs before the
  write is answered or turns into S5 (§5.2.5).
- **Rows that change without a new stamp** (a stranding rollback, F14) or
  without a notification to this view (a commit through another view of
  the node) are invalidated by §5.1.5's events.

The stamp is compared only by equality, so a hash collision would be the
only false "current": 2⁻¹²⁸ per comparison.

#### 5.1.5 Engine events: content that changes under a kernel without a notification

Three engine-side changes produce today no notification for the kernel
that holds the affected pages. Each now pushes `Inval::Inode { ino, data:
true }` through the node's `KernelInvalidator` (and, for a rollback, the
entries the rolled-back records touched), which reaches the frontend as an
ordinary data notification (A2 + S1, or S2 when undelivered):

1. **A write session retired or dropped without a commit** (F13).
   `WriteShards::retire` (`crates/engine/src/view/shards.rs:464`) and
   `View::drop_writes` (`flush.rs:923`) take a reason
   (`Committed | NothingChanged | Uncommitted`); every `Uncommitted` call
   invalidates the inode in **every** view of the node, independent of
   `cluster_locks()`: `publish_inode`'s `FlushFail { ws: None }`, the
   lapsed-grant refusal, `discard_lock_writes` (the existing
   `l.invalidate(ino)` calls become this one event, so `--locks local` is
   covered), `release`'s `drop_writes` of an orphan or of a session whose
   flush answered `ESTALE`. `NothingChanged` (`ws.changed_nothing()`) needs
   none. The view gets an `InodeInvalidator` in `FsDependencies` (beside
   `inflight`), `None` with invalidation off — then W1 keeps nothing anyway.
2. **A commit through another view of the node** (Q3, decided).
   `commit_manifest_local`, `commit_manifest_forwarded` and
   `commit_manifest_with_rebase` (`flush.rs:1286`, `:1379`, `:1473`) push
   one `Inval::Inode { data: true }` per commit to every registered view
   *except* the committing one: a new `Msg::BatchExcept(view_id, batch)` /
   `InodeInvalidator::invalidate_others(view_id, ino)` in `kernel_inval`,
   skipped in `run`'s `send` for that view. It closes R8 for the
   multi-view case and today's cross-view staleness of open descriptors,
   at one notification per commit (not per write).
3. **A stranding rollback** (F14). The three stranding passes return in
   `Stranded` the records of the transactions they rolled back; their
   callers (the takeover gate, deposition recovery, the replay drain) feed
   them to `kernel_inval::invalidations` and push the result to every
   view, as a foreign apply of the same rows would have. Pushed after the
   rollback commits (as the foreign hook is), so the drop follows the
   restored row.

The conformance kit gains `a_session_dropped_without_commit_invalidates`
(the recording view is told `Data` for its own dropped session) and
`a_commit_through_another_view_invalidates_this_one`; the engine's unit
tests cover each retire/drop reason and the rollback push.

### 5.2 W2: write-race repair

#### 5.2.1 Arming

An inode is **armed** while a drop of its pages has been announced and no
write has been claimed (§5.2.3) since that drop was known to be complete:
`armed = !live_arms.is_empty() || drop_done`. Each drop the frontend causes
or can see coming adds an `Arm` immediately **before** the reply or
notification that causes it, and the arm becomes *complete* (it leaves
`live_arms` and sets `drop_done`) on the evidence that the kernel has
finished the drop:

| Arm | Added in | The drop | Complete when |
|---|---|---|---|
| A1 an open or create answered without `FOPEN_KEEP_CACHE`, or a `create` with `O_TRUNC` | `OpenReply`/`CreateReply`, before the reply bytes; the arm names the reply's file handle | `fuse_open`/`fuse_create_open`, in the opener's context after it reads the reply — at a time the daemon does not see | the first request that carries that file handle (`READ`, `WRITE`, `FLUSH`, `RELEASE`, `FSYNC`, `GETATTR`/`SETATTR` with a handle, `LSEEK`, `FALLOCATE`, `COPY_FILE_RANGE`, the lock ops): the kernel installs the descriptor only after `fuse_finish_open`'s drop. At most 4 handle arms per entry; past that one *overflow* arm completes when `handles` drops to 0 |
| A2 a data notification (whole file or range), any source | `FuseNotifySink::invalidate`, before the write (also S1) | `fuse_reverse_inval_inode`, inside the write | the sink's write returns |
| A3 an attribute-bearing reply for a regular file whose size differs from `last_size` (then `last_size = size`) — including the `SETATTR size=0` an `open(O_TRUNC)` sends | `EntryReply`, `AttrReply`, `CreateReply` (and plan 40's plus entries), before the reply bytes | `fuse_change_attributes_i`'s size-change invalidation, in the requester's context | at once (no evidence exists; see below) |
| A4 a repair (§5.2.3) | the repair lane, before its write | the repair itself | the repair's write returns |

A3 completes at once because nothing tells the daemon when the requesting
task installs the reply. (For a truncating `SETATTR` the kernel's drop runs
under `inode_lock`, so no write-through write can be in flight across it,
KF1: completing at once is exact there.) The drop it announces can race a local write
only when that write does not extend the file (an extending write sets
`FUSE_I_SIZE_UNSTABLE`, and the kernel then ignores the reply) and the size
changed under it — a foreign size change concurrent with a local
overwrite, i.e. two nodes writing one file without a lock. If such a drop
lands after the claim that cleared its arm, its stale page is caught as a
`Plain` hit (S5, §5.2.4): bounded by the next open, as today.

`last_size` also takes `max(last_size, off + len)` at every write claim
and the size of every `setattr` reply (an `AttrReply`, covered by A3). A
wrong `last_size` only arms spuriously or not at all for a size the kernel
already holds (no invalidation then).

#### 5.2.2 Recording

- **Write registration** (`adapter.rs:1266`, before `vfs.write`): a `WRITE`
  without `FUSE_WRITE_CACHE` in its `write_flags` (`fuser::WriteFlags`) is
  pushed to `pending` as `(unique, off, len)`. Writeback writes are neither
  registered nor claimed (KF1: their folios are under writeback).
- **Handles**: `handles += 1` at every successful open or create reply,
  and `write_handles += 1` when its flags include write access; both `-= 1`
  at `release` (`adapter.rs:1438` has the flags). An A2 arm whose write the
  closed gate refuses is completed at once (nothing was dropped; S2 is set).
- **Read registration** (`adapter.rs:1236`, before `vfs.read`; every read,
  zero-copy included): if the entry exists and `armed || !pending.is_empty()
  || write_handles > 0`, the read's page range `[off / P, ceil((off + size)
  / P))` (`P` = the kernel page size, `sysconf(_SC_PAGESIZE)`) is added to
  `reads` with its kind: `Pending` if `pending` is non-empty, else `Armed`
  if armed, else `Plain`. `ReadSet` keeps at most 16 disjoint ranges;
  merging two of different kinds keeps the stronger (`Pending` > `Armed` >
  `Plain`); on overflow it merges the two closest ranges. A read of an
  inode with neither a write handle nor a pending write nor an arm costs
  one shard-lock lookup and records nothing.

The registration happens before the engine reads the data, under the same
shard lock the claim takes, so a read either is recorded before the claim
or reads after the claim's write was applied.

Three facts the code comments must carry (they are what a later reader
would "fix"):

- **A claim clears every record, also of pages it does not cover.** Safe:
  any later write of such a page is copied after this claim's reply (KF1),
  and a read in flight on a page blocks a writer's
  `__filemap_get_folio(FGP_WRITEBEGIN)` on it, so a recorded read's fetch
  always precedes any later copy into the same page.
- **A multi-request `write(2)`** (larger than `max_write`) is several
  successive candidates, each filled after the previous reply and claimed
  in turn; `pending` holds one at a time.
- **A request the kernel aborts** (a fatal signal) after a worker read it
  is still answered by the daemon (the kernel waits for `FR_FINISHED` once
  the request left its pending list), so a claim always comes; a connection
  abort ends the session and its table.

An A1 arm of a passthrough open completes only at its `RELEASE` (its reads
never reach the daemon); while it lives, plain reads of the inode are
recorded `Armed` — a bounded cost, repairs only where a write overlaps.

#### 5.2.3 Claim and repair

At `WriteReply::done` (`reply.rs:397`) — after the engine applied the write
(for `O_SYNC`, after the publication too) and before the reply — for a
registered write of inode `X`:

1. Under the shard lock: remove the write from `pending`; on an error
   answer set S3, clear `reads` and `drop_done`, reply. On success, take
   `hits = reads ∩ pages(off, len)`, clear `reads`, clear `drop_done`
   (live arms stay: their drops may still be under way, and the next claim
   after they complete is theirs), update `last_size`. If another
   registered write is still pending (not expected by KF1), keep the
   records and `drop_done`, set S5 and log once at warn.
2. Hits of kind `Plain` set S5 (§5.2.4) and are not repaired.
3. If no `Armed`/`Pending` hit remains, or W2 is off (§5.4), reply.
4. Otherwise hand the reply and the hit ranges (merged, at most 16) to the
   session's **repair lane** and return; the worker (or the fsync-pool
   thread of an `O_SYNC` write) does not wait.

The repair lane (`crates/frontend-fuse/src/repair.rs`, new): two threads per
session, a FIFO queue, and a watchdog thread. A job is `{ ino, ranges,
reply: Mutex<Option<ReplyWrite + count>>, entered_at: Mutex<Option<Instant>> }`.
A lane thread takes a job; enters the `NotifyGate` (`notify.rs`) — closed
gate: set S5, reply at once, done; sets A4 on the entry, `entered_at =
now`; writes one `notifier.inval_inode(ino, start * P, (end - start) * P)`
per range; exits the gate; takes the reply: still there, reply (counted
`repairs`); already taken by the watchdog, set S5 (counted `repairs_late`).
**The bound is in-flight-aware** (F15). The notification can wait only for
a folio lock or for writeback, and each of those is released either by the
answer to some *other* request of the inode that a FUSE worker will give
(a `READ` filling the folio, a writeback or `launder_folio` `WRITE`, which
`invalidate_inode_pages2_range` itself may send) — no cycle, just a wait —
or by the answer to `W` itself, which the lane holds (the cycle). The
adapter therefore counts the requests in progress per inode
(`PcEntry::ops_in_flight`: incremented at the adapter's entry of every
request carrying the inode, decremented when its reply is written, `W`
excluded once it is handed to the lane). A job's clock runs only while it
is entered **and** `ops_in_flight == 0` — `W` alone on the inode — and the
watchdog (ticking every 10 ms) answers `W` and sets S5 once that clock
exceeds `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS` (default 1000). While
another request of the inode is in progress the wait is counted
(`repair_wait_ms`) and never ends the job. A queued job is answered early
only when both lane threads are inside jobs whose clocks have run out. A
job whose thread has not been scheduled yet never times out: CPU starvation
and daemon pauses delay the reply, they never send it ahead of the repair.
What the count cannot see is a request the kernel has queued that no
worker has read yet — the residual `kernel_inval`'s module doc accepts
(the queueing delay), which the bound covers.

Why this cannot deadlock: the notification can block only on a folio lock,
writeback, `i_mmap` (for `unmap_mapping_pages`) or a `launder_folio`
`WRITE` (`mm/truncate.c`, `invalidate_inode_pages2_range`); it takes no
inode lock and no `i_rwsem`, and the lane holds nothing a worker needs. By
KF2 the writer `W` holds neither a lock nor writeback on a folio a recorded
read filled, on an unfixed kernel — with one exception, stated in the code:
the folio was dropped again unseen (or a read of it failed and left a
non-uptodate folio) and `W` re-created it as a partial, non-uptodate tail;
then the repair waits for `W`'s reply, and this is the only case in which
the in-flight-aware clock runs out on an unfixed kernel. With patch 0001,
`W`'s folios are under writeback and a recorded read may have covered one
(F10) — W2 is off there once the probe or the table says so (§5.4). In
every blocked case the watchdog answers `W` after the bound, the reply
unblocks the notification, and the inode is suspect. A `kill -9` while a lane thread is
blocked is the B-1 case of `kernel_inval`'s module doc, handled the same
way (the zombie reaper). The lane writes through the gate, so a detach waits
for repairs under way like for any notification.

Why before the reply: the writer's own read right after `write(2)`
returns is the common way an application sees race 1; a repair after the
reply races it (F5). The cost is the notification's latency on the writes
that need one (§8's worst-case benchmark measures it; with W1, the reopen pattern of Appendix A.4 does not arm at all).

#### 5.2.4 Plain hits: ambiguous, so suspect instead of repaired

A `Plain` hit is a read of `W`'s pages that entered after the previous
claim, before `W` was registered, with no known drop in between. Either it
completed before `W` copied its bytes (read-modify-write of a cold page:
harmless, and frequent for editors, `sqlite`, `git`'s index) or an unseen
drop (reclaim, `fadvise`) let it refetch a page under `W` (race 1). The
daemon cannot tell them apart. Repairing every one would cost a
notification and a refetch per such write; marking the inode suspect costs
exactly what every open costs today and restores today's repair of an
unseen-drop stale page: the next open drops it (F1).

#### 5.2.5 Why the claim catches every stale page from a known drop

Let a drop `D` of `X`'s page `p` be announced by an arm added at `t_A`
(A1, A2, A4: the arming thread adds the arm before it writes the reply or
notification that causes `D`) and known complete at `t_C`, after `D` has
finished. A stale page needs a write `W` that copied into `p` before `D`
removed it and whose data the daemon applied after a read `R`, issued
after `D` removed `p`, fetched it. `X` stays armed from `t_A` until the
first claim that begins after `t_C`. `W` was not applied before `D`
removed `p`, so `W`'s claim (which follows its apply) comes after `t_A`;
by KF1, no other write of `X` is filled before `W` is answered, so every
claim between `t_A` and `W`'s is of a write applied before `D` reached
`p`, and the first claim after `t_C` is `W`'s or a later one — `X` is
armed at `W`'s claim. `R` entered the adapter after `t_A`: if before `W`'s
claim, it was recorded `Armed` or `Pending` (armed or `W` pending at its
entry); if after, it read after `W` was applied and fetched the new bytes.
So `p` is in `W`'s repair set, and the repair drops the stale folio before
`W` is answered (or, past the bound, just after, with S5). For a drop the
daemon cannot see (and for A3's), `W` is through a handle with write
access, so `write_handles > 0` while `W` is in progress, `R` is recorded
(`Plain` at worst), and `W`'s claim repairs it or sets S5. ∎

With W1, `A1` happens only at dropping opens, so on a workload whose files
stay current (Appendix A.4) arming and recording are rare; with
`CONSTELLATION_FUSE_KEEP_CACHE=off` every open arms (the test
configuration of §7.2, which exercises the repair at today's drop rate).

### 5.3 W3: directory attributes

W3 has two independent parts, one per race, each with its own kernel
verdict and setting.

#### 5.3.1 W3b — 2b: an attribute-only notification for the changed parent

After the engine has applied a local namespace operation and before its
reply is written, the adapter writes `FUSE_NOTIFY_INVAL_INODE(parent,
offset = -1, len = 0)` for every directory the operation changed:

| Operation | Responder (`reply.rs`) | Parents |
|---|---|---|
| `mkdir`, `mknod`, `symlink` | `EntryReply` | `parent` |
| `link` | `EntryReply` | `newparent` |
| `create` | `CreateReply` | `parent` (also when it opened an existing file: the kernel runs `fuse_dir_changed` only for a new entry, but the notice is harmless) |
| `unlink`, `rmdir` | `EmptyReply` → a new `DirChangeReply(EmptyReply, parent)` | `parent` |
| `rename`, `exchange` | `RenameReply` | `parent` and `newparent` (once if equal), **and the moved inode** — both inodes for `exchange` (F16: `fuse_rename_common` updates or invalidates their ctime without bumping `attr_version`) |

Only on success. The responders gain the parent node id(s) and the
session's notifier (`Arc<NotifyGate>`, for its `Notifier`); `EntryReply`
gets `Option<Ino>` because `lookup` uses it too (`None` there). For the
moved inode(s) the adapter needs their node ids, which `rename`'s answer
does not carry today: `Vfs::rename`'s responder becomes
`Responder<Renamed>` with `Renamed { moved: Ino, exchanged: Option<Ino> }`
in the view's numbering (`crates/vfs/src/vfs.rs`, the engine's
`View::rename`, `MockVfs`/`RefFs`). `link` needs nothing more: the kernel
installs the linked inode's reply through `fuse_iget(..., attr_version =
0)`, which bumps its version; `unlink`, `rmdir` and rename-over go through
`fuse_entry_unlinked`, which bumps too.

Why it closes 2b: the kernel's `GETATTR` (`fuse_do_getattr`), `LOOKUP`
(`fuse_lookup_name`) and `READDIRPLUS` paths take the `attr_version`
snapshot they later check (`fi->attr_version > attr_version` →
rejected, inode.c:343) *before* they send the request.
`fuse_reverse_inval_inode` sets `fi->attr_version` to a fresh
`atomic64_inc_return(&fc->attr_version)` under `fi->lock` (`fuse_reverse_inval_inode`, inode.c:559).
A reply that carries the parent's pre-change attributes was generated by
the daemon before it applied the change, so its request was sent before
the notification was written, its snapshot is lower, and it is rejected
whenever it arrives — before the notification (then the notification
invalidates what it stored) or after (then it is not stored). A request
sent after the notification is answered after the change. The kernel's
own `fuse_dir_changed` after the reply then invalidates once more,
harmlessly.

Why it may be written from the replying thread, against `kernel_inval`'s
first rule: that rule exists because a notification can block in the
kernel behind a request in flight. An attribute-only one cannot:
`fuse_notify_inval_inode` takes `fc->killsb` for reading — written only
while a mount is torn down, by `fuse_mount_remove` briefly and by
`fuse_conn_destroy` across `fuse_abort_conn` + `fuse_wait_aborted`, which
end every request (the one being answered included) without the daemon, so
that wait is bounded by the abort, not a cycle — and `fuse_reverse_inval_inode` with `offset < 0` takes only
`fi->lock` (a spinlock); `fuse_ilookup` waits at most for an inode being
created or freed, which needs no daemon answer, and `iput` cannot drop the
last reference (the kernel holds the parent across the operation).
`kernel_inval` already never holds an attribute-only notification back
(`Inval::gate` is `None`). So the notification is written with the
notifier directly, bypassing the handover gate's *open* check (a detaching
session still answers requests and must still protect them); after
`retire` there is no notifier and nothing to protect. Errors are ignored
(`ENOENT`: the kernel holds no inode for the parent, so no attributes to
protect). The module docs of `kernel_inval` and `notify` record this
exception and its argument.

Cost: one `write(2)` on `/dev/fuse` of 40 bytes per namespace operation,
in the operation's latency (measured in §8's untar and `rm -rf`
benchmarks). Setting: `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY` (`auto` = on
unless the kernel has `DirChangeBumpsVersion`; `on`; `off`).

#### 5.3.2 W3a — 2a: directory attributes are never cached by the kernel

Every reply that carries a directory's attributes carries attribute TTL 0
and keeps the view's entry TTL: `EntryReply` answers
`ReplyEntry::entry_with_ttls(attr_ttl = 0, entry_ttl = attr.ttl)` when
`attr.kind == FileKind::Dir`, `AttrReply` answers TTL 0 for a directory,
and plan 40's plus entries do the same per entry. Regular files and the
other kinds keep `attr.ttl`. In every `--cto` mode: under strict both are
0 already except on a lone sequencer (`lone_kernel_ttl`, F8), which this
covers. The rule is the FUSE frontend's (it answers a Linux kernel
artifact), so the `Vfs` contract and the engine are unchanged; the
directory test is the `Attr::kind` the reply already carries.

Why it closes 2a: with `attr_valid = 0`, `fuse_time_to_jiffies` gives
`i_time = 0`, so `time_before64(i_time, jiffies)` holds for
every reader at every moment and `fuse_update_get_attr` never answers a
directory's `stat` from the cache, whatever order the installer's stores
become visible in. Measured: `nlinkrace` 56 mismatches in 1.7 M stats
without, 0 in 1.5 M with (Appendix A.3).

Why there is no narrower safe variant (F9): the racing reader sees the
*previous* `i_time` when the installer has cleared the mask but not yet
stored the new `i_time`, so the previous reply's validity must already
have run out. The daemon cannot know when that happens — `i_time` is the
install time plus the TTL, and the install happens in the requesting
task, which a loaded host can delay without bound after the reply was
written. "TTL 0 only for recently changed directories" therefore leaves
the first change after a quiet period exposed whenever the directory's
attributes were fetched within the TTL before it. Holding the first
post-change reply for the directory until the previous `i_time` has surely
passed *is* bounded once W3b is in place (a pre-change reply installed
after the change is rejected and never sets `i_time`, so the stale value is
at most the change time plus the TTL), but it costs up to the TTL — 1 s —
on the first `stat` after every change, worse than TTL 0 for `make` and
`git`; rejected. A short positive TTL narrows the window without closing
it.

What it costs (Appendix A.2): with `default_permissions`, every path walk
refreshes the attributes of each directory it crosses — a `GETATTR` per
directory component of every path lookup (`fuse_permission` →
`fuse_perm_getattr`, after the RCU walk falls back to a reference walk),
plus one per `stat` of a directory. Entries are unaffected (the entry TTL
stays). Measured on Appendix A's 50,000-file tree: the FUSE requests of
stat-heavy workloads grow 2–6× (`find`, `du`, `rsync -n` 2.1–2.4×; `git
status` 4.8×; `ls -lR` 5.7×; `make -q` warm 11×; a loop of deep-path
`stat`s 6–40×), all of the growth `GETATTR`s of directories, and the
fastest of six runs per configuration (the least disturbed by the shared
host's load) took +14–53 % for `find`, `du`, `rsync -n`, +96–147 % for
`make -q`, +131–164 % for `ls -lR` and `git status`, and 3–15× for the
deep-`stat` loop. The cost is material: it falls on exactly the workloads
ADR-30 kept bounded mode for (builds, `find`, `git status`).

Setting: `CONSTELLATION_FUSE_DIR_ATTR_TTL` = `auto`, `zero`, `entry`
(`entry`: the entry TTL, as before this plan — 2a stays open, 2b stays
closed by W3b). Under `--cto strict` (TTL 0 already, except a lone
sequencer's `lone_kernel_ttl`, F8) `auto` is `zero` in either option below.
What `auto` means in bounded mode was Q1 (decided below: option B):

- **Option A — `zero`** (2a closed in bounded mode, until the table or the
  probe finds `AttrsValidAfterStore`): pays the cost above on every
  stat-heavy walk.
- **Option B — `entry` in bounded mode, `zero` under strict** (the
  reviewer's recommendation): 2a's exposure is one transient `stat` of a
  directory's `st_nlink`/size/times, only while another task is
  concurrently installing a refresh of the same directory right after a
  change — never the data path, never persistent (W3b closes the
  persistent variant, 2b) — while the cost lands on exactly the workloads
  ADR-30 kept bounded mode for, at 2–11× the FUSE requests, which no
  re-measurement turns into noise. The trade-off is recorded in the ADR.
  If the quiet-host run shows `git status` and `make -q` within +20 % of
  `entry` under option A, the balance tips to A.

K5 implements both values and the switch; the default in bounded mode is
written in only when Q1 is decided (before K5 lands).

**Q1 decided 2026-10-21 (maintainer): option B**, together with the
review's finding F21 (W3b's notice had been turned off in bounded mode,
because it invalidated the whole attribute mask and cost one `GETATTR` per
namespace operation, which left 2b open as well): W3b stays on in bounded
mode with F21's narrowed trigger (only the attributes the change moved, only
where it closes 2b), its cost measured before K5 lands. 2a remains open in
bounded mode as the documented transient one-`stat` exposure; strict mode
closes both. The measurement (§8.1, run without the quiet-host gate,
strict `entry`/`zero` ABBA interleave, 8 samples per mode, real ext4, xfs
and btrfs partitions and tmpfs on the build box under co-tenant load):
warm `git status` under `zero` is 6.2× (ext4), 5.7× (xfs), 2.6× (btrfs)
and 7.9× (tmpfs) of `entry`, warm `make -q` 3.2×, 2.8×, 4.2× and 3.0×;
requests 7k → 293k and 10k → 85k. The margin over the +20 % rule is 2–8×
in every block.

### 5.4 Settings and kernels with the fixes

Five settings, environment variables read once when the daemon starts
(CONVENTIONS: `CONSTELLATION_*`, documented where read and in
`docs/reference/configuration.md` under "FUSE and runtime threads"); an
unparseable value fails the mount, like `CONSTELLATION_FUSE_TRANSPORT`:

| Setting | Values (default first) | What |
|---|---|---|
| `CONSTELLATION_FUSE_KEEP_CACHE` | `auto`, `off` | W1. `auto`: §5.1 in bounded mode, never under strict. `off`: every open drops, as before this plan (diagnosis; W2 then arms at every open). Not tied to the kernel: keep-cache is a speed win with or without the fixes. |
| `CONSTELLATION_FUSE_WRITE_REPAIR` | `auto`, `on`, `off` | W2's repair (§5.2.3). The recording and the suspect marks of §5.2.4 run whatever the value — W1 relies on them. `auto` = on unless the kernel is known to carry the race-1 fix; `off` on an unfixed kernel turns every repair hit into S5 (the next open drops; the writer's read-back may be stale). |
| `CONSTELLATION_FUSE_WRITE_REPAIR_WAIT_MS` | `1000`, positive integer | the repair lane's bound (§5.2.3) |
| `CONSTELLATION_FUSE_DIR_ATTR_TTL` | `auto`, `zero`, `entry` | W3a (§5.3.2); `auto` in bounded mode pending Q1 |
| `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY` | `auto`, `on`, `off` | W3b (§5.3.1) |

**Detection** (`crates/frontend-fuse/src/kernel_fixes.rs`, new). One table,
the only place to edit when the patches land:

```rust
pub(crate) enum KernelFix {
    /// 0001 "fuse: keep folios under writeback while a write-through WRITE is in flight"
    WriteThroughWriteback,
    /// 0002 "fuse: mark cached attributes valid only after storing them"
    AttrsValidAfterStore,
    /// 0003 "fuse: bump attr_version when a directory is changed"
    DirChangeBumpsVersion,
}

/// First releases carrying each fix: the mainline tag (`(major, minor, rc)`,
/// `rc: None` for the final release) and, per stable branch, the first
/// sublevel with the backport. Empty until the patches are merged.
const FIXED_IN: &[(KernelFix, Option<(u32, u32, Option<u32>)>, &[(u32, u32, u32)])] = &[
    (KernelFix::WriteThroughWriteback, None, &[]),
    (KernelFix::AttrsValidAfterStore, None, &[]),
    (KernelFix::DirChangeBumpsVersion, None, &[]),
];
```

**Probes** (decided, Q6): two fixes are checked by behaviour at mount,
because stable backports will not show in version numbers (F15). Both run
from a daemon thread once the session serves requests with at least two
workers, against a hidden scratch file and directory of the view (the
synthetic registry's), never blocking the mount: the mount answers at
once and the settings start as `NotFixed` until the probes finish
(~300 ms).

- **0001 (`WriteThroughWriteback`)**: a helper thread `pwrite`s 4 KiB to
  the scratch file while the adapter's `write` handler for that inode holds
  its reply for 200 ms (a probe-only hook, released on a timer, so it
  cannot hang); meanwhile a third thread writes
  `inval_inode(ino, 0, 4096)`. If the notification returns only after the
  reply is released, the kernel keeps write-through folios under writeback
  for the `WRITE` → `Fixed`; if it returns while the reply is still held →
  `NotFixed`. Deterministic: on an unfixed kernel the folio is unlocked and
  uptodate during the `WRITE`, on a fixed one it is under writeback.
- **0003 (`DirChangeBumpsVersion`)**: the `dirver` pattern on the scratch
  directory: a `stat` whose `GETATTR` the daemon holds 50 ms after reading
  the attributes, a `mkdir` in the directory meanwhile, a `stat` after; the
  old `st_nlink` → `NotFixed`.
- 0002 (`AttrsValidAfterStore`) has no deterministic probe (the window is a
  few instructions); the table and the override decide it.

`kernel_has(fix) -> Verdict { Fixed, NotFixed, Unknown }` parses
`uname(2)`'s `release` (`7.3.0-0.rc4.260925g….fc46.x86_64` → `7.3.0`, rc 4;
the distribution suffix ignored; an unparseable release is `Unknown`):
`Fixed` if at or above the mainline tag, or on a listed stable branch at or
above its sublevel; `NotFixed` otherwise. The effective verdict is the
override if set, else the probe's for 0001/0003, else the table's; `auto`
treats `Unknown` as not fixed. A distribution kernel that carries a backport under an older version
number is the reason for the overrides: `on`/`off` win over the table. The
verdicts and the effective settings are logged once at mount (info) and
reported in `node.status` per mount as `kernel_workarounds: { keep_cache,
write_repair, dir_attr_ttl, dir_change_notify, verdicts: { fix: (verdict,
source: probe | table | override) } }` (`crates/control/src/proto/types.rs`).
The race-2a and race-2b parts of W3 follow `AttrsValidAfterStore` and
`DirChangeBumpsVersion` respectively.

The table is not a code redesign away from being filled in: adding a
row's values is a one-line change with a unit test per entry (§7.1). If
upstream also gives a protocol signal (decided, Q5: ask for a *behaviour*
flag for 0001 only — "write-through folios stay under writeback for the
`WRITE`, so `FUSE_NOTIFY_INVAL_INODE` waits for in-flight `WRITE`s", a
server-visible change — and none for 0002/0003, which are pure fixes),
it becomes one more source in `kernel_has` without touching its callers;
it would reach mainline only, so the probe and the table stay the real
detection, and nothing waits for it.

What a wrong verdict costs: W2 forced on a fixed kernel costs the repair
bound on writes whose pages a recorded read had covered (F10; the
in-flight-aware clock runs then, since `W` is alone), never a hang — and
the 0001 probe makes that case need a wrong override; W3 forced on costs its measured per-walk cost; W2 or W3 off on an
unfixed kernel reopens exactly the race it covers (W1 still does not keep a
suspect inode's pages).

### 5.5 `--cto strict`

Unchanged: no keep-cache (the engine answers `cache: None`), so every
strict open drops the file's pages as today. Strict promises that an open
sees every close completed elsewhere before it began; dropping at the open
makes that independent of how and when the notifications for those closes
reach the kernel (the ReadIndex wait makes the replica current, the drop
makes the pages follow it). Keep-cache under strict would rest the promise
on §5.1.4's argument plus the notification pipeline's timing for every
strict read, for a mode whose users chose correctness over speed and
already pay TTL 0. W2 matters more under strict (every open arms) and runs
as in bounded mode; W3 applies to the lone strict sequencer's TTL
(`lone_kernel_ttl`, F8) as to bounded mode's.

### 5.6 Lifetimes: FORGET, inode reuse, handover, remount

- **`FORGET`.** `Forgot::Last`/`Underflow` remove the inode's `PcEntry`
  (the kernel evicted the inode with its pages, or absence is the safe
  state). A write in flight holds a file reference, so no `FORGET` can
  precede the claim of a write that is still pending.
- **Inode reuse.** Constellation never reuses inode numbers (generation 0,
  `View::entry_out`), so a node id names one inode for the session; a
  later lookup of an evicted inode creates a fresh kernel inode with no
  pages and finds no entry.
- **Handover / `daemon --upgrade`.** The table is not exported (§5.1.2):
  the resumed session's opens drop once per inode; `handles` and
  `write_handles` are seeded from the handle snapshot (so a write through a
  handle opened before the upgrade still has its reads recorded); arms the
  old process had not seen complete are lost, which the resume's
  re-invalidation of every open file re-arms, and a race through a lost arm
  still ends as a `Plain` hit (S5); the gate's dropped notifications are
  covered (the old process stops answering; the resumed host re-invalidates
  every open file's data, which arms A2 and marks S1 in the new table).
  A repair the old process could not finish before detaching replied
  through the closed gate with S5, which the new process cannot see — but
  the new process holds no entries, so nothing is current there either.
- **Remount.** A new connection: new kernel inodes, new table.
- **Lost notification.** Every way a data notification can fail to reach
  the kernel ends in S2 (dropped, gate, error) or keeps the inode pending
  (rule 1); a sink that panics ends `kernel-inval` (its existing
  behaviour: `ThreadExit`), and from then on every data notification stays
  pending — the counter is never decremented — so no inode with a change
  after that point is kept again. The failure mode is dropping, never
  stale data.

### 5.7 Observability

`node.status` per mount (and `/metrics` as `constellation_fuse_*`
counters): `keep_cache_opens`, `dropping_opens` by reason (`no_stamp`,
`stamp_changed`, `suspect`, `no_entry`, `trunc`, `off`), `suspect_marks`
by mark (S1–S5), `repairs`, `repair_pages`, `repairs_late`,
`repair_wait_ms` (histogram), `ambiguous_writes`, `dir_attr_notices`
(§5.3), the engine's §5.1.5 counts (`uncommitted_invalidations` by reason,
`other_view_invalidations`, `strand_invalidations`), plus
`kernel_workarounds` with each verdict's source (§5.4). A late repair logs at warn once
per inode per minute (it means a blocked notification or a misdetected
kernel).

### 5.8 Documentation

- `kernel_inval.rs` module doc and the `DEFER_MAX` comment: a dropped
  data notification leaves pages stale until the next dropping open; it now
  marks the inode suspect (F3). `configuration.md:532` and
  `cto-modes.md:177` likewise.
- `crates/vfs/src/events.rs`: `undelivered`; `crates/vfs/src/types.rs`:
  `Opened::cache` and `CacheStamp`.
- `docs/reference/configuration.md`: the four settings and the detection.
- A new `docs/reference/features/kernel-cache-coherence.md` (feature
  template, `docs/documentation-guide/feature-documentation-template.md`):
  what the kernel caches, keep-cache and its rules, the three races and
  what each workaround closes, the residuals of §5.9, the settings, how to
  tell from `status` which apply.
- DESIGN.md §12 (proposed text, recorded in PROGRESS per CONVENTIONS rule
  5): "`FOPEN_KEEP_CACHE` … invalidated exactly on remote change" becomes
  "keep-cache when the open finds the content unchanged since the kernel
  last dropped it (no writeback cache)"; §6's kernel invalidation notes the
  directory-attribute rules. ADR: a new ADR-3x "Kernel FUSE race
  workarounds" in `docs/explanation/DECISIONS.md` (context: the kernel
  patch package's races; decision: §5.1–§5.4; consequences: §5.9, Appendix
  A; rejected: §5.10).

### 5.9 What stays open on an unfixed kernel

| # | Race | Exposure | Bound |
|---|---|---|---|
| R1 | Race 1 after a drop the daemon cannot see (reclaim, `fadvise(DONTNEED)`, `drop_caches`, cgroup memory limit) | a read of an acknowledged write may return the old bytes | until the next open of the file (S5 via §5.2.4) — today's bound, never longer |
| R2 | A stale page dirtied through a shared writable `mmap` before the repair (F7) | its writeback can write old bytes over an acknowledged write | needs `write(2)` and `mmap` stores into the same page concurrently; window = the `WRITE`'s service time after a known drop |
| R3 | A repair past its in-flight-aware bound: the writer re-created a folio after an unseen drop (§5.2.3), or W2 forced on a fixed kernel against the probe | the writer's read-back may be stale | until the repair lands (it continues), then S5; counted `repairs_late` |
| R4 | A direct-I/O write (`O_DIRECT` through a `FOPEN_DIRECT_IO` handle, plan 38's read-write passthrough) while another descriptor has pages cached | the cached pages miss the write | until that descriptor's next dropping open (S4); POSIX does not order buffered and direct I/O |
| R5 | A remote change while a local write is in flight | covered: the data notification is a known drop (A2), W2 repairs the pages the local write overlaps; the remote change's own content is close-to-open (concurrent writers on two nodes without a lock are a conflict the manifest merge decides) | — |
| R6 | Race 2a for **regular files** after a foreign change or a lock grant: one `stat` concurrent with another task's refresh returns pre-change attributes | one stale `stat` result (the cache ends right); a reader under a lock grant could see the pre-grant size once | not closed; the per-walk cost of TTL 0 for files is not justified (§5.10); closed by patch 0002 |
| R7 | `invalidate_inode_pages2` failing on a folio (dirty again after `launder_folio`, `EBUSY`), which the kernel ignores | that page survives a drop | today's exposure; S1 makes the next keep-cache decision drop again |
| R8 | A row that returns to a recorded stamp (ABA): a commit through another view, a stranding rollback's before-image (deposition recovery, takeover), an uncommitted session | closed by §5.1.5's invalidations (S1), or S2 if one is dropped; residual only if a rollback path is added that does not report its records — the K2 test lists every `strand_*` entry point | — |

### 5.10 Rejected

- **`FUSE_WRITEBACK_CACHE`.** Its folios stay under writeback for the
  `WRITE` (no race 1), but it changes the durability and ordering model
  (writes acknowledged before the daemon sees them, `close` and `fsync`
  semantics, mtime ownership, the lock fence, `O_APPEND` sizes) — a plan of
  its own, not a workaround.
- **`FUSE_EXPLICIT_INVAL_DATA`** (would remove the size-change drop, A3):
  without the kernel's own invalidation on a size change, a held-back or
  dropped notification leaves a partial last page cached at the old EOF,
  so a reader of a remotely appended file reads zeros where data is —
  worse than the drop it removes.
- **A time-bounded notice record** (as first stated): F4.
- **Re-invalidating the whole written range** from the handler or after
  the reply: F5.
- **Holding back every open's reply until no write is in flight**: the
  daemon cannot see a write that is still in the kernel (KF3), and an open
  would wait for a whole `write(2)`.
- **TTL 0 for regular files' attributes** (to close R6): a `GETATTR` per
  `stat`, `read` beyond the cached size and permission check of every file,
  for a single-`stat` staleness after foreign changes that bounded mode
  already permits for concurrent `stat`s.
- **Repairing ambiguous (`Plain`) hits**: §5.2.4.
- **A wall-clock-only repair bound**: fires while the repair merely waits
  for a worker under load, and turns an undetected fixed kernel into a
  stall per repaired write (F15); §5.2.3's clock runs only while the write
  is alone on the inode.
- **Holding the first post-change directory reply** until the previous
  attribute validity has passed: bounded with W3b, but up to 1 s per first
  `stat` after a change (§5.3.2).

## 6. Milestones

Each milestone ends with `cargo fmt`, clippy `-D warnings` and the tests it
names green; K1's fail-before numbers are recorded in PROGRESS before K2
starts. Build and run on a Linux test host (16 cores, the kernel's
`fuse` module, `fusermount3`, `stress-ng`, `fio`); never share a cargo
target directory between worktrees.

### K1 — Reproducers, fault points, scenarios: fail before

- **Workloads** (`crates/harness/src/kernel_races.rs`, new), written in
  Rust after the kernel patch package's reproducers — the patterns, not the
  code: the package's C sources are GPL-2.0 and this repository is
  MPL-2.0. Each returns counts and exits non-zero on a violation:
  - `pc_race(file, secs, opener, stat, readers)`: 4 writers each own pages
    `id, id+4, …` of a 64-page file, `pwrite` the first half of a random
    own page with a fresh byte and `pread` it back; readers `pread` the
    second half of random pages; an optional thread `open`/`close`s the
    file (or `stat`s it). A read-back that differs is re-read with
    `O_DIRECT`; counts `writes`, `stale`, `stale_server_correct`.
  - `notify_race(file, secs)`: `pc_race` with the opener replaced by a
    thread that takes and drops an `flock` on the file in a loop, run on
    two nodes at once so the grant moves (§7.2).
  - `unseen_drop_race(file, secs)`: `pc_race` with the opener replaced by
    `posix_fadvise(POSIX_FADV_DONTNEED)` in a loop; on every stale
    read-back it also opens a **new** descriptor and reads the page:
    `stale_after_reopen` must be 0 (today's guarantee, F1).
  - `cfr_race(src, dst, secs)`: `pc_race`'s writers `copy_file_range` the
    written half to `dst` and check it with `O_DIRECT` (a regression guard
    for `2acd591`: 0 before and after).
  - `nlink_race(base, secs, threads, sync)`: 4 directories, each behind an
    application reader/writer lock; writers `mkdir`/`rmdir` children with
    the lock exclusive, readers `statx` with it shared and check
    `st_nlink == 2 + children`; `sync` uses `AT_STATX_FORCE_SYNC` for the
    first stat after a change (control: 0).
  - `dir_version(dir, rounds)`: the `dirver` round: toggle `a`; thread
    `stat(dir)`; after a quarter of the delay toggle `b`; join; `stat(dir)`
    must show `2 + a + b`.
- **Fault points** (test-only, `CONSTELLATION_FAULT_*`, documented in
  `configuration.md` "Fault injection"):
  - `CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US=N`: the adapter sleeps `N` µs
    in `write` before anything else (before W2's registration, before
    `vfs.write`): a worker preempted between reading a `WRITE` and handing
    it on — the part of KF3's window the daemon cannot see.
  - `CONSTELLATION_FAULT_VIEW_WRITE_DELAY_US=N`: `View::write` sleeps after
    `InFlight::enter`, before `do_write` (the package's slowed server).
  - `CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS=N`: `View::getattr` of a
    directory sleeps after `current_attr`, before answering (the package's
    `PT_GETATTR_DELAY_US`); also used for a regular file by the rename
    `ctime` scenario (`CONSTELLATION_FAULT_GETATTR_DELAY_INO=<ino>` narrows
    it to one inode).
  - `CONSTELLATION_FAULT_COMMIT_FAIL=<path-suffix>`: the next commit of a
    file whose name ends so fails as `FlushFail { ws: None }` (the session
    is retired uncommitted, F13); with `…=lapse:<suffix>` as a lapsed-grant
    refusal instead.
  The adapter's lives in `crates/frontend-fuse` (a cached env read, like
  `engine::fault`).
- **Scenarios** (`crates/harness/src/scenarios/kernel_races.rs`, new; rows
  in `SCENARIOS`; `requires: &[]`): §7.2's table. Each scenario runs once on
  the default transport and once with `Client::with_io_uring_denied()`
  (`dev_fuse`), named `…` and `…-dev-fuse`.
- **Fail before**: run every scenario on the base build (`28fec6a` plus
  K1 only) and record the counts. Expected from Appendix A.3: `pc_race`
  with the opener ≥ 100 stale per 60 s on both transports, `nlink_race`
  ≥ 20 mismatches per 60 s (near the `uring` baseline's rate — that row
  may raise its duration to 180 s), `dir_version` 20/20. A scenario whose
  base count is below its §7.2 threshold is not accepted: raise its delay
  or duration until it is.

### K2 — Engine: the cache stamp, pending data notifications, undelivered

- `CacheStamp`, `Opened::cache` (`crates/vfs/src/types.rs`); `MockVfs` and
  `RefFs` answer `None` (they model no page cache).
- `Meta::content_stamp` (+ scratch), unit tests: equal for an unchanged
  inode across reads; different after a `WriteManifest`, a `Setattr` with
  a size, a commit of a session (`do_write` + publish); unchanged by
  `Atime`, an mtime-only `Setattr`, an xattr; two successive commits of
  different content differ (R8).
- `View::open`/`create` answer it under §5.1.1's conditions; the frozen
  branch answers the constant stamp; strict answers `None` (unit tests in
  `crates/engine/src/view/vfs_tests.rs`, one per condition, each failing
  with the condition removed).
- `kernel_inval`: the `data_pending` counter, `InFlight::data_pending`,
  `FrontendEvents::undelivered` (default no-op) called for every dropped
  data notification in each view's numbering. Unit tests beside the
  existing ones (`kernel_inval.rs:833`): pending while queued, held back,
  and blocked in a sink; zero after send, after drop, after coalescing 50
  duplicates; `undelivered` called once per view on a drop; a panicking
  sink leaves the counter up.
- §5.1.5's events: `WriteShards::retire`/`drop_writes` with a reason and
  the unconditional uncommitted-session invalidation (the
  `cluster_locks()`-only calls folded in); `InodeInvalidator` in
  `FsDependencies`; `Msg::BatchExcept`/`invalidate_others` at the three
  commit paths (Q3); `Stranded` carrying the rolled-back records and the
  push at every caller of `strand_below_epoch`, `strand_for_takeover`,
  `strand_recalled_speculation`. Unit tests: one per retire/drop reason
  (`Uncommitted` invalidates under `--locks local` too, `NothingChanged`
  does not, `Committed` invalidates only the other views); a stranding
  pass that rolls a `WriteManifest` back invalidates its inode; a list of
  the `strand_*` entry points pinned by a test, so a new one must decide.
- Comment and doc fixes of F3.
- Conformance kit (`crates/vfs/src/conformance/invalidation.rs`, the
  engine target): `open_answers_the_same_stamp_for_unchanged_content`,
  `a_second_views_commit_changes_the_stamp`,
  `a_foreign_change_is_pending_until_delivered` (`settle` then the stamp
  changed and nothing pending), `strict_opens_answer_no_stamp`,
  `a_session_dropped_without_commit_invalidates`,
  `a_commit_through_another_view_invalidates_this_one`.

### K3 — Frontend: W1

- `pagecache.rs` with `PageCacheTable`, `PcEntry` (W2 fields present but
  unused until K4), the lifetime rules of §5.1.2/§5.6 wired into `forget`
  and session start/resume (`write_handles` seeding).
- The decision in `OpenReply`/`CreateReply`; `FOPEN_KEEP_CACHE` OR-ed into
  the plain and zero-copy answers.
- S1–S4 marks; `undelivered` in `FuseNotifySink`; the gate and error paths.
- `CONSTELLATION_FUSE_KEEP_CACHE` (`auto`/`off`, plus the fault-only
  `CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT=1` that makes the
  decision ignore S5 — the naive variant F1 guards against, for §7.2's
  fail-before of `kernel-race-unseen-drop`).
- Unit tests (`pagecache.rs`): the decision table (each of rules 1–4 and
  S1–S5 alone flips keep to drop); a dropping open records and clears;
  `Forgot::Last` removes; a resumed table keeps nothing; seeding.
- Wire tests (`crates/frontend-fuse/tests/wire.rs` style, driving opcodes
  end to end): an `OPEN` reply carries `FOPEN_KEEP_CACHE` exactly when the
  table says so; a `NOTIFY_INVAL_INODE` written for a data invalidation is
  preceded by the mark.

### K4 — Frontend: W2

- Arming A1–A4, recording, claim, `repair.rs` (lane, watchdog), S5.
- `CONSTELLATION_FUSE_WRITE_REPAIR`, `…_WAIT_MS`.
- Unit tests: `ReadSet` (merge, kinds, overflow); the claim partition
  (`Pending`/`Armed` repaired, `Plain` suspect); a claim with no
  registration (writeback write) does nothing; an error claim clears and
  marks; a short count (`n < len`) marks S3; the watchdog never answers a
  job whose thread has not entered it, never while `ops_in_flight > 0`
  (a `READ` of the inode in progress, fake clock past the bound), and
  answers an entered job whose `W` has been alone past the bound; a closed
  gate replies at once with S5.
- A deterministic adapter-level test of §5.2.5 with a scripted kernel
  (`tests/wire.rs`): arm by an `OPEN` reply, a `READ` of page 3, a `WRITE`
  covering pages 3–4: the `WRITE` reply is preceded by
  `NOTIFY_INVAL_INODE(off = 3 * P, len = P)` and by nothing for page 4.

### K5 — W3

- W3b: the parent notifications in the five responders, the moved
  inode(s) of `rename`/`exchange` (`Responder<Renamed>` through the `Vfs`
  contract, the engine, `MockVfs`, `RefFs`), the setting; unit test that
  each namespace reply is preceded by an attribute-only
  `NOTIFY_INVAL_INODE` for exactly its parents and moved inodes
  (`tests/wire.rs`), and none on an error answer or for `lookup`.
- W3a: directory attribute TTL 0 in `EntryReply`/`AttrReply` (and plan
  40's plus entries if landed), the setting with both values (the bounded
  default as Q1 decides; `zero` under strict); wire test that a directory's
  `entry_out` has `attr_valid == 0` and `entry_valid == 1 s` and a file's
  both 1 s, in bounded mode and for a lone strict sequencer.
- `kernel-race-dir-*` rows of §7.2.

### K6 — Detection, status, docs

`kernel_fixes.rs` with the empty table, the 0001 and 0003 probes (§5.4;
the probe-only reply hold in the adapter's `write`, the 50 ms directory
`GETATTR` hold) and the verdict precedence (override > probe > table);
the parser tests (the test host's
release string, an Ubuntu `-generic`, an Amazon Linux `.amzn2023`, an rc,
garbage → `Unknown`; a filled-in test table: at/after/before the mainline
tag, a stable branch at and below its sublevel); `node.status` fields,
metrics, the schema (`crates/control/src/schema.rs`) and the docs of §5.8.

### K7 — System tests, campaigns, benchmarks

§7 and §8 on the Linux test host, including §7.5's run on a patched
kernel and §8.1's quiet-host measurement; results in
`bench/kernel-races/RESULTS.md` (new) and PROGRESS.

## 7. Tests

### 7.1 Unit and conformance

As listed per milestone. Every unit test that guards a rule is checked to
fail with that rule removed (note the check in the test's comment, as the
lock-model knobs of plan 41 do).

### 7.2 Harness scenarios (`kernel_races.rs`)

Every scenario: one node unless stated, a fresh filesystem, default write
mode and `--locks cluster`, the run's counts printed as one `RESULT` line.
"Base" is the K1 build; "after" the build of the milestone that closes it.
A threshold "≥ M" applies to the base run and makes it a valid fail-before.
"0" means 0 in every run of the campaign (§7.4), not on average.

| Scenario | Workload, configuration | Base | After |
|---|---|---|---|
| `kernel-race-open` | `pc_race` with opener, 60 s | stale ≥ 100 | after K3: `keep_cache_opens` > 0 and stale lower (an open still drops whenever a publication of the writers' sessions changed the stamp); after K4: stale 0 |
| `kernel-race-open-repair` | same, `CONSTELLATION_FUSE_KEEP_CACHE=off` (every open drops) | stale ≥ 100 | stale 0, `repairs` > 0, `repairs_late` 0 (K4) |
| `kernel-race-open-repair-delay` | same with `CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US=200` | stale ≥ 100 | stale 0, `repairs_late` 0 (K4) |
| `kernel-race-notify` | two nodes: A runs `pc_race` without opener, with `CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US=200`; a thread on A and one on B take and drop an `flock` on the same file in turns, so every grant on A follows a recall from B and writes a data notification (`ClusterLocks::granted`, `locks.rs:788`; a lock under a grant A still holds is set locally, `local_set`, and notifies nothing — hence two nodes), 60 s | stale ≥ 20 | stale 0 (K4) |
| `kernel-race-notify-foreign` | two nodes: B overwrites a *second* file that A's readers also read, while A runs `pc_race` on the first: a guard that foreign data notifications on other inodes cost A nothing (`repairs` 0 on the first file) | stale 0 | stale 0, `repairs` 0 |
| `kernel-race-unseen-drop` | `unseen_drop_race` (the `fadvise(DONTNEED)` thread), 60 s | (fail-before: the K4 build with `CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT=1`, the naive keep-cache) `stale_after_reopen` ≥ 1; the base build: stale ≥ 1 and `stale_after_reopen` 0 | `stale_after_reopen` 0 without the knob; stale reads allowed (R1), each followed by a dropping open of reason `suspect` (K4) |
| `kernel-race-cfr` | `cfr_race` with opener, 60 s | corrupt 0 | corrupt 0 (guard) |
| `kernel-race-strict` | `pc_race` with opener, `--cto strict` | stale ≥ 100 | stale 0, no `keep_cache_opens` (K4) |
| `kernel-race-handover` | `pc_race` with opener and `KEEP_CACHE=off` while `daemon --upgrade` runs at 20 s and 40 s | stale ≥ 100 | stale 0; after each resume the first open of the file drops (K4) |
| `kernel-race-uncommitted` | `pc_race` without opener; every 2 s `CONSTELLATION_FAULT_COMMIT_FAIL` fails a commit of the file (retired uncommitted), then the workload closes and reopens every descriptor and checks each written page against an `O_DIRECT` read of what the view answers; `--locks cluster` and `--locks local`; a `…=lapse:` variant under `--locks cluster` | (fail-before: the K3 build without §5.1.5) a reopened read differing from the view ≥ 1 | 0 (K2+K3) |
| `kernel-race-discard-local` | `--locks local`: a lapsed-grant-style discard (`discard_lock_writes` through the fault) while a reader holds the file's pages; reopen and compare with the view | ≥ 1 before §5.1.5 | 0 |
| `kernel-race-handover-keep` | `pc_race` with opener, `KEEP_CACHE=auto`, `daemon --upgrade` at 20 s and 40 s | — | stale 0; after each resume the file's first open drops (`dropping_opens{no_entry}` ≥ 1), later opens keep |
| `kernel-race-other-view` | one node, two views of the filesystem (a whole mount and a subtree mount over the same files): view A writes and closes, view B reopens and reads | 0 (every open drops today) | 0 stale; B's next open after each of A's commits drops (`dropping_opens{suspect}`), and B's open descriptors are told (Q3) |
| `kernel-race-dir-nlink` | `nlink_race`, 8 threads, 60 s; and `sync` (control) | mismatches ≥ 20; control 0 | 0 in ≥ 4× the base's stats (K5) |
| `kernel-race-dir-version` | `dir_version`, 20 rounds, `CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS=200` | 20/20 stale | 0/20 (K5), also with W3's TTL part off (the parent notice alone closes 2b) |
| `kernel-race-rename-ctime` | `dir_version`'s pattern on a file: `stat(f)` whose `GETATTR` is held 200 ms (`CONSTELLATION_FAULT_GETATTR_DELAY_INO`), `rename(f, g)` meanwhile, then `stat(g)`: ctime must be the rename's | 20/20 old ctime | 0/20 (K5, W3b with the moved inode) |
| `kernel-race-dir-version-remote` | two nodes: B `mkdir`s in the directory during A's delayed `stat` | 0 (the data notification bumps `attr_version`) | 0 (guard) |
| `kernel-race-dir-ttl-strict-lone` | `nlink_race` on a lone `--cto strict` node (`lone_kernel_ttl` > 0, F8) | mismatches ≥ 20 | 0 (K5) |

### 7.3 xcheckfs lockstep

xcheckfs with an ext4 primary and a Constellation secondary
(`--mismatch log`), write-through and write-back mounts, 30 min each: (a)
its stress lane plus one thread `open`/`close`ing the files it writes
(the package's `o-cwt`/`o-cwb` runs), (b) the `pc_race` and `nlink_race`
workloads run through the xcheckfs mount. Pass: 0 data and attribute
mismatches attributed to the secondary, and `ctl episodes` empty, on the
final build; the base build's run is recorded (the original evidence saw
1 stale read in 5–10 runs of (a); (b) shows them in every run).

### 7.4 Stress and load campaigns

On the Linux test host, for each of `kernel-race-open-repair`,
`-open-repair-delay`, `-notify`, `-unseen-drop`, `-dir-nlink`,
`-dir-version`: 10 seeds × 2 transports × these load profiles, 60 s each:

| Profile | Injection |
|---|---|
| L0 | none |
| L1 CPU starvation | `stress-ng --cpu 0 --cpu-method all` (every CPU) for the whole run, daemon and workload at default priority |
| L2 throttled daemon | the daemon in a cgroup with `cpu.max` = 50 % of one CPU (`systemd-run --user --scope -p CPUQuota=50%`) |
| L3 pauses | `Client::pause`/`resume` of the daemon: 20–80 ms stops every 200–500 ms (seeded) |
| L4 memory pressure | `stress-ng --vm 2 --vm-bytes 75% --vm-keep` (reclaim of the mount's pages: unseen drops) |

Pass, numerically: every row of §7.2 meets its "after" column in every run
(e.g. `kernel-race-open-repair` under L1: 0 stale in 10 × 2 runs of 60 s
where the base build shows ≥ 100 per run), with one exception that is
counted, not failed: under L4, stale reads of `pc_race` variants that
`unseen_drop_race`'s rule permits (R1: `stale_after_reopen` must still be
0). With the in-flight-aware bound (§5.2.3), a repair's clock runs only
while its write is alone on the inode, so starvation (L1, L2) and pauses
(L3) lengthen `repair_wait_ms` but do not end jobs: **`repairs_late` must
be 0 in every profile on an unfixed kernel**, and every stale read-back is
a failure. A late repair there means the self-block of §5.2.3 (an unseen
drop that re-created the writer's folio) or a bug: each one is
investigated and listed in RESULTS. No run may hang, show `ESTALE`/`EIO`, or leave a kernel task
in `D` for more than the bound (check `/proc/*/stack` of the workload at
the end of each run).

The campaign is run on the base build for the fail-before rows and on the
final build; results by profile in `bench/kernel-races/RESULTS.md`.

### 7.5 A kernel with the fixes

The kernel patch package's VM harness (a QEMU/KVM guest booting mainline
`4056b3c9d3c9` with patches 0001–0003, `CONFIG_CGROUP_WRITEBACK=y`) runs
the final build once with every §7.2 row at L0:

- the probes report `WriteThroughWriteback` and `DirChangeBumpsVersion`
  `Fixed` (`node.status … verdicts`, `source: probe`), so W2 and W3b are
  off; W3a follows the table/override (no probe);
- every race row is 0 with W2/W3 off (the kernel closes them);
- with `CONSTELLATION_FUSE_WRITE_REPAIR=on` forced, `kernel-race-open-repair`
  shows the F10 stall: `repairs_late` > 0 and `repair_wait_ms` at the bound
  — the only check of the detection path before a release carries the
  fixes;
- the same guest on the unpatched base kernel reports `NotFixed` from both
  probes (the probes' fail-before).

## 8. Benchmarks and acceptance

No regression on any backing store: the node's state and cache
directories on ext4, xfs, btrfs (compression off and `zstd`) and tmpfs
(loop images are fine), 3 interleaved runs per configuration (A/B/A/B, as
Appendix A), reported as median and spread; "beyond noise" = slower than
the base by more than the larger of 3 % and twice the base's spread.

| Benchmark | Expected |
|---|---|
| reopen-read: 5,000 files of 16 KiB, read each 3 times with an `open`/`close` per read (warm) | faster: the 2nd and 3rd reads hit kept pages (`read` request count ≈ ⅓ of base) |
| `git status`, `ls -lR`, `find`, `make -q`, `rsync -n` on Appendix A's tree | W3's cost as decided (§5.3); everything else neutral |
| untar of a 50 k-file tree; `cp -a` of it | neutral (W3's parent notice per namespace op: measured) |
| `fio` sequential write 1 MiB, random write 4 KiB, random rw 70/30 4 KiB, 4 jobs | neutral |
| `pc_race` without opener (write + read-back, the recording path) | neutral |
| worst case for W2: `pc_race` with opener and `KEEP_CACHE=off` | report write latency p50/p99 with and without W2 (the repair's cost) |
| open latency: 100 k `open`/`close` of one cached file | neutral or better (one record decode instead of the `getattr` read, one table lookup) |
| database-style: one long-lived read-write handle, 4 KiB random reads and writes 50/50 over a 1 GiB file, 4 threads, 60 s | neutral (recording is always on here: one shard lookup per read and write; ambiguous hits only set S5) |
| commit-heavy multi-view: two views, many small files written and closed in one | report the cost of Q3's one notification per commit |

### 8.1 Quiet-host measurement for Q1 (to run before K5; a separate task)

The numbers of Appendix A.2 were taken on a host shared with other
campaigns; their request counts stand, their wall times do not. Q1 is
decided on this measurement:

- **Host gate.** Before every run: no other campaign's load marker on the
  host and no other agent's FUSE mounts active; the 1-minute load average
  below 1.0 (wait, re-check, record it per run); CPU frequency governor and
  turbo state recorded; nothing else of this plan running.
- **Build.** The K5 build (or, if run before K5, a build with only the
  W3a switch), release mode, `CONSTELLATION_FUSE_DIR_ATTR_TTL=entry` (T1)
  against `=zero` (T0).
- **Tree.** Appendix A.1's generator (20 × 10 × 10 directories × 25 files,
  `git` repository, 5,000-target `Makefile`, an `rsync -a` copy on the
  host), plus a real source tree of at least 50,000 files copied in once
  (a large open-source checkout available on the host).
- **Backing stores.** The node's backend, state and cache directories on
  each of ext4, xfs, btrfs (compression off) and tmpfs, as loop images of
  the same size; one filesystem per backing store, created once.
- **Interleaving.** Per backing store: 10 rounds; each round unmounts
  (waiting for the daemon to exit), mounts T1 or T0, verifies the mode
  with Appendix A.1's probe (50 deep `stat`s: 250 `GETATTR`s under T0, 0
  under T1), runs every workload twice (cold, warm), and the next round
  runs the other mode first (A/B/B/A …).
- **Workloads.** `ls -lR`, `find -type f`, `find -newer`, `du -s`, `git
  status --porcelain`, `make -q all`, `rsync -an --delete`, 10,000 deep
  `stat`s; plus `make -j8` of a small real C project with everything up to
  date, and `git status` on the real tree.
- **Recorded per run.** Wall time, FUSE request counts by op and the
  daemon's service time (`node.status` op counters before and after), the
  load average, the transport (`uring` and, in a second pass, `dev_fuse`
  via `CONSTELLATION_FUSE_TRANSPORT=dev-fuse`).
- **Reported.** Per workload, backing store and transport: median and
  min–max wall time, the T0/T1 ratio of medians with a 95 % interval
  (bootstrap over rounds), request counts. The Q1 tipping rule: if `git
  status` and `make -q` (warm) are within +20 % under T0 on every backing
  store, option A; else option B.

## 9. Risks

- **Over-eager suspect marks** cost keep-cache, never correctness:
  measured by `dropping_opens` reasons in the reopen-read benchmark.
- **The repair lane under a storm** (`KEEP_CACHE=off`, many writers): two
  threads may become a latency bottleneck; the worst-case benchmark
  decides whether the count needs to scale with writers.
- **A misdetected fixed kernel** with W2 forced on (F10): latency only;
  the warn line names the setting.
- **Plan 40 landing first**: its plus entries must apply A3 and §5.3's
  directory TTL; the review of whichever plan lands second checks it.
- **Large folios** (FUSE gaining large-folio buffered writes): KF2's
  argument is per folio and the repair is page-ranged, so a larger folio
  only widens what one notification drops; recheck when the kernel's FUSE
  write path changes.

## 10. Open questions and decisions

- **Q1 (W3a in bounded mode) — open, pending §8.1.** Directory attribute
  TTL 0 closes 2a and costs, on the measured tree, 2–11× the FUSE requests
  of stat-heavy walks and, on the fastest of six runs on a shared host,
  +14–53 % (`find`, `du`, `rsync -n`), about +100–160 % (`make -q`, `ls
  -lR`, `git status`) and 3–15× for deep-path `stat` loops (§5.3.2,
  Appendix A.2); no narrower variant is safe. Option A: `zero` in bounded
  mode. Option B (the reviewer's recommendation): `entry` in bounded mode,
  `zero` under strict including the lone sequencer, the trade-off in the
  ADR; tipping to A if `git status` and `make -q` stay within +20 % on a
  quiet host. The bounded default is marked "pending the quiet-host
  measurement" until then; decide before K5 lands.
- **Q2 — decided: the in-flight-aware bound** (§5.2.3), 1 s by default; the
  campaign may raise it to 2 s if L2's throttling shows the pre-queue part
  of the window above 1 s. Past the bound the write is answered and the
  inode suspect, so the bound is never a correctness hole.
- **Q3 — decided: yes.** One `Inval::Inode { data: true }` per commit to
  the node's other views (§5.1.5).
- **Q4 — decided: not now.** Recorded condition for a later plan: advance
  `dropped_at` to the post-commit stamp only for a commit through this view
  that is **not** a `commit_manifest_with_rebase` over a concurrent change,
  of an entry whose `dropped_at` equals the pre-commit stamp and that is
  not suspect, under the shard lock; with a test that writes the same file
  concurrently from another node.
- **Q5 — decided:** when the patch package is sent, ask upstream for a
  behaviour flag for 0001 only (folios under writeback for the `WRITE`'s
  duration, so `FUSE_NOTIFY_INVAL_INODE` waits for in-flight `WRITE`s);
  none for 0002/0003. Detection stays probe + table (+ override); nothing
  waits for the flag.
- **Q6 — decided: yes**, for 2b and extended to 0001 (§5.4's probes); no
  probe for 2a.

## 11. Exit criteria

- [x] K1: workloads, fault points, scenarios; base counts recorded in
      PROGRESS, each meeting its threshold.
- [x] K2: stamp, pending counter, `undelivered`, §5.1.5's three events
      (uncommitted sessions, other views' commits, stranding rollbacks),
      unit and conformance tests green; F3's comment and docs corrected.
- [x] K3: W1 with S1–S4; its unit and wire tests; `kernel-race-open` meets its K3 column.
- [ ] K4: W2; `kernel-race-open`, `-open-repair*`, `-notify*`, `-unseen-drop`,
      `-strict`, `-handover` 0 as §7.2 states.
- [ ] K5: W3b (parents and moved inodes) and W3a's switch, the bounded
      default per the Q1 decision taken on §8.1's measurement; `-dir-*` and
      `-rename-ctime` rows met.
- [ ] K6: detection table, the 0001 and 0003 probes and their tests,
      status, metrics, docs, ADR.
- [ ] K7: §7.3 xcheckfs runs clean; §7.4 campaign passes on the final
      build (`repairs_late` 0 in every profile); §7.5 on a patched kernel;
      §8 acceptance met on every backing store; §8.1 recorded.
- [ ] Every CONVENTIONS gate green; pjdfstest 8798/8798; `stress-ng-fs`
      within its baseline.

## 12. PROGRESS.md

A section `## Plan 42 — Kernel FUSE race workarounds` in the established
style (intro, "What landed" table, "Unchanged on purpose", "Gates" with
dates, kernel release and backing stores), rows:

| Item | State | Where |
|---|---|---|
| Reproducers, fault points, scenarios; base counts (K1) | | `crates/harness/src/kernel_races.rs`, `scenarios/kernel_races.rs`, `crates/engine/src/fault.rs` |
| Cache stamp, pending data notifications, `undelivered` (K2) | | `crates/vfs/src/{types,events}.rs`, `crates/meta/src/store/`, `crates/engine/src/{kernel_inval,view/ops}.rs` |
| Conditional keep-cache (K3) | | `crates/frontend-fuse/src/{pagecache,reply,notify,adapter}.rs` |
| Write-race repair (K4) | | `crates/frontend-fuse/src/{pagecache,repair,reply,adapter}.rs` |
| Directory attributes (K5) | | `crates/frontend-fuse/src/{reply,adapter,notify}.rs` |
| Detection, status, docs (K6) | | `crates/frontend-fuse/src/kernel_fixes.rs`, `crates/control/src/proto/types.rs`, `docs/reference/`, `docs/explanation/DECISIONS.md` |
| Campaigns, xcheckfs, benchmarks (K7) | | `bench/kernel-races/RESULTS.md` |

Also record: the base counts next to the final ones; the proposed DESIGN
text (§5.8); the Q1 decision with §8.1's numbers and Q2–Q6 as decided in
§10; the kernel releases tested, the patched guest included, and the
probes' verdicts on each.

## Appendix A — Measurements on the Linux test host

Host: 16 cores, 30 GiB, Fedora with the distribution kernel
`7.3.0-0.rc4.260925g165768bb7026.42.fc46` (unpatched), the node's backend,
state and cache on btrfs (`compress=zstd:1`), built from `28fec6a` in
release mode. The host was shared with other test campaigns during the
runs (load average 9–36, recorded per run); request counts are not
affected by load, timings are, which is why the runs were interleaved.

### A.1 Method

- Filesystems created with a local-directory backend (`fs create … --s3
  <dir>`), mounted with `mount / <mnt> --s3 <dir> --state-dir <dir>
  --foreground`, default options (`--cto bounded`, write-through,
  `default_permissions`), FUSE transport `uring` unless stated.
- A scratch-only build knob (never committed, not part of this plan's
  code) made `EntryReply` answer `entry_with_ttls(attr_ttl = 0, entry_ttl
  = 1 s)` and `AttrReply` answer TTL 0 for directories — exactly W3a.
- The tree: 20 × 10 × 10 directories of 25 small C files each (50,000
  files, 2,221 directories, depth 4), a `git` repository of it, a
  `Makefile` with 5,000 up-to-date targets depending on a source and a
  header each, an `rsync -a` copy of it on the host for `rsync -n`.
- Interleaved A/B: one filesystem remounted alternately without (T1) and
  with (T0) the knob, each round in the opposite order of the previous
  one, every workload run twice per mount (run 1 after the remount: cold
  kernel caches; run 2: warm). FUSE request counts per run from
  `node.status`'s op counters (`constellation_vfs_ops_total`, frontend
  `fuse`), the daemon's service time from their histogram sums.

### A.2 Cost of directory attribute TTL 0 (W3a)

Six interleaved rounds (load average 20–75 during the runs; another
campaign's load injector was active for rounds 3–6). "Fastest" is the
quickest of the six runs per cell, the least disturbed by that load; the
medians are dominated by it (several T1 medians are slower than T0's).
Every T0 mount was checked before its round: 50 `stat`s of 5-deep paths
cost 250 `GETATTR`s under T0 and 0 under T1.

| Workload | Run | FUSE requests T1 → T0 | of which `GETATTR` T1 → T0 | Daemon service ms T1 → T0 | Wall s, fastest run T1 → T0 (Δ) | Wall s, median T1 → T0 |
|---|---|---|---|---|---|---|
| `ls -lR` | cold | 119,329 → 686,116 (5.7×) | 12 → 566,821 | 1,451 → 3,179 | 2.62 → 6.52 (+149 %) | 10.05 → 7.81 |
| `ls -lR` | warm | 119,380 → 686,136 (5.7×) | 18 → 566,821 | 1,259 → 3,139 | 2.72 → 6.29 (+131 %) | 10.88 → 7.62 |
| `find -type f` | cold | 6,703 → 19,985 (3.0×) | 344 → 13,404 | 100 → 155 | 0.21 → 0.32 (+53 %) | 0.30 → 0.44 |
| `find -type f` | warm | 6,704 → 17,986 (2.7×) | 2,052 → 13,404 | 95 → 130 | 0.26 → 0.27 (+4 %) | 0.43 → 0.30 |
| `find -newer F -type f` | cold | 61,942 → 130,404 (2.1×) | 117 → 68,636 | 684 → 942 | 1.71 → 1.95 (+14 %) | 6.22 → 2.50 |
| `find -newer F -type f` | warm | 61,939 → 130,570 (2.1×) | 5 → 68,636 | 711 → 938 | 1.53 → 1.87 (+22 %) | 5.94 → 2.47 |
| `du -s` | cold | 61,939 → 130,568 (2.1×) | 5 → 68,634 | 732 → 937 | 1.56 → 2.03 (+30 %) | 5.52 → 2.35 |
| `du -s` | warm | 61,940 → 130,568 (2.1×) | 6 → 68,634 | 729 → 932 | 1.51 → 1.91 (+26 %) | 5.59 → 2.20 |
| `git status --porcelain` | cold | 55,497 → 263,744 (4.8×) | 203 → 221,390 | 972 → 2,609 | 0.36 → 0.94 (+159 %) | 4.31 → 1.65 |
| `git status --porcelain` | warm | 58,177 → 278,762 (4.8×) | 726 → 221,392 | 937 → 3,342 | 0.37 → 0.97 (+164 %) | 1.67 → 1.75 |
| `make -q all` (5,000 targets) | cold | 11,226 → 45,103 (4.0×) | 38 → 34,213 | 118 → 241 | 0.28 → 0.69 (+147 %) | 0.48 → 0.77 |
| `make -q all` | warm | 3,232 → 35,644 (11.0×) | 76 → 34,213 | 41 → 131 | 0.20 → 0.38 (+96 %) | 0.33 → 0.47 |
| `rsync -an --delete` | cold | 56,995 → 134,348 (2.4×) | 198 → 74,838 | 664 → 950 | 1.60 → 2.42 (+51 %) | 5.79 → 2.74 |
| `rsync -an --delete` | warm | 61,956 → 136,778 (2.2×) | 10 → 74,838 | 724 → 996 | 1.68 → 2.49 (+48 %) | 3.50 → 3.51 |
| 10,000 `stat`s of 5-deep paths | cold | 10,277 → 61,037 (5.9×) | 1 → 50,000 | 102 → 291 | 0.16 → 0.54 (+230 %) | 0.47 → 0.81 |
| 10,000 `stat`s of 5-deep paths | warm | 1,286 → 51,280 (39.9×) | 0 → 50,000 | 18 → 175 | 0.03 → 0.43 (+1447 %) | 0.08 → 0.58 |

Reading it: the extra requests are almost exactly one `GETATTR` per
directory a path walk crosses (`git status` 221 k ≈ 4.4 per file, the
deep-`stat` loop 5 per `stat`), about 11 µs of daemon time each on this
host, plus the kernel round trip. Entry TTLs were unchanged, so `LOOKUP`
counts did not move. A quiet-host re-measurement (and the same table on
`dev_fuse`, and with the backing store on ext4, xfs and tmpfs, where this
cost does not depend on the backing store but the baseline does) is
§8.1, and Q1 waits for it.

### A.3 Race baselines on `28fec6a`

The package's reproducers (built from its sources on the host, not part
of the repository), 20 s per run unless stated:

| Run | `uring` mount | `dev_fuse` mount |
|---|---|---|
| `pcrace` + opener | **620** stale / 4,774 writes (all re-read correct with `O_DIRECT`: stale page cache) | **300** stale / 322,884 writes |
| `pcrace`, no opener | 0 / 4,592 | 0 / 541,712 |
| `pcrace` + `stat` loop | — | 0 / 373,637 |
| `cfrrace` + opener | — | 0 corrupt / 132,389 copies (`FUSE_COPY_FILE_RANGE` since `2acd591`) |
| `nlinkrace`, 8 threads, cached | **6** mismatches / 211,226 stats; **56** / 1,716,350 in 30 s | **118** / 1,389,514 |
| `nlinkrace`, sync after change | 0 / 235,418 | — |
| `nlinkrace` with directory attribute TTL 0 (the scratch knob) | **0** / 1,477,324 in 30 s | — |

(The `uring` mount's low write rate in the first rows is the daemon
publishing a 50,000-file tree at the same time.) The reproducers confirm
that every Constellation open drops the pages (race 1 needs the opener and
appears with it on both transports), that 2a is present on this kernel,
and that an attribute TTL of 0 for directories removes it.

### A.4 How often an open drops today

1,000 files of 16 KiB, each read three times with an `open`/`read`/`close`
per read (`dev_fuse`): every round costs 1,000 `OPEN`, 1,000 `READ` and
1,000 `GETATTR` requests (0.41 s, 0.27 s, 0.12 s); the second and third
`READ`s exist only because each open drops the pages. Under W1 the files
are current after the first round's opens, so rounds 2 and 3 send no
`READ`, and no open arms W2.

## Sources

- The kernel patch package (draft series 0001–0003 against mainline
  `4056b3c9d3c9`, its README, reproducers `pcrace`, `cfrrace`,
  `nlinkrace`, `dirver`, `mixstress`, the rejected
  `alt-reinvalidate-after-write.diff`, and its distribution-kernel and
  bisection results). Kernel functions cited: `fs/fuse/file.c`
  (`fuse_cache_write_iter`, `fuse_perform_write`, `fuse_fill_write_pages`,
  `fuse_send_write_pages`, `fuse_open`, `fuse_fsync`,
  `fuse_dio_wr_exclusive_lock`), `fs/fuse/dir.c` (`fuse_dir_changed`,
  `fuse_update_get_attr`, `fuse_permission`, `fuse_perm_getattr`,
  `fuse_create_open`), `fs/fuse/inode.c` (`fuse_change_attributes_common`,
  `fuse_change_attributes_i`, `fuse_reverse_inval_inode`,
  `fuse_time_to_jiffies`).
- The October 2025 fuse-devel thread "[PATCH 5.15] fuse: Fix race
  condition in writethrough path" (the same race 1 through
  `FUSE_AUTO_INVAL_DATA`).
- In-repo: `crates/engine/src/{kernel_inval,locks,cto}.rs`,
  `crates/engine/src/view/{mod,ops,io,handoff}.rs`,
  `crates/frontend-fuse/src/{adapter,reply,notify,refs,dentries,passthrough,session}.rs`,
  `crates/vfs/src/{types,events}.rs`, `crates/cli/src/handover.rs`,
  `crates/meta/src/store/writes.rs`, `crates/mtree/src/record.rs`,
  `docs/explanation/{DESIGN,DECISIONS}.md` (§6, §12; ADR-4, ADR-20,
  ADR-30), `docs/reference/configuration.md`,
  `docs/reference/features/cto-modes.md`; commits listed in §1.
