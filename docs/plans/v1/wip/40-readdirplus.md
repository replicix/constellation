# Plan 40 — FUSE READDIRPLUS: attributes with the listing

Read `docs/plans/v1/CONVENTIONS.md` first. This plan makes the Linux FUSE
frontend serve `FUSE_READDIRPLUS`: a directory page that carries, with each
name, the entry and the attributes a `LOOKUP` of that name would answer. The
kernel then installs dentries and inode attributes for the whole page, and
`ls -l`, `find -ls`, `du`, `rsync`, `git status` and every other "list, then
`lstat` each name" walk stop sending one or two requests per name. The
metadata format was built for this from plan 28 on (each dentry carries a copy
of its child's attributes); nothing ever served it.

It also replaces how a directory page is produced. Today every page re-reads,
hashes and sorts the whole directory (§2), which is quadratic in a full
listing and would turn plus pages (about four times fewer names per page) into
a regression for large directories. After this plan a page is a name-ordered
range scan of the dentries, resumed from a deterministic, keyed cookie through
a small handle-free hint cache (§4.9): O(page) for plain and plus pages alike,
no per-stream memory, no write-path cost.

Every `READDIRPLUS` entry that carries a node id is a **kernel reference
exactly like a lookup** (§3, §4.4). Getting that wrong either reaps an inode
the kernel still references (`ESTALE` on an `O_PATH` descriptor or a removed
working directory: integrity) or leaks an orphan until unmount. The
adapter's lookup accounting that this needs was found to be missing for
today's code too, and is landing separately as an integrity fix (§4.4, R2);
this plan builds on it.

Rules carried into this plan from the maintainer: consistency and integrity
first, then performance, never one for the other (a trade-off is reported,
not shipped); no backward-compatibility shims and **no version bumps**
(`HANDOVER_VERSION`, handoff and on-disk formats change in place); every fix
or behaviour change comes with a test that fails before it (or, for a new
feature, against the naive implementation it guards against).

## Dependencies

- Plan 31 (C4's `Vfs` contract with the `plus` flag, `DirSink`, the FUSE
  adapter, the conformance kit and `MockVfs`/`RefFs`; C4b's session handover).
- Plan 30 (§M6 session waits, §M7 kernel invalidation, §M8 `cto=strict`),
  plan 38 (the replicix `fuser` fork and its branch process; the io_uring
  dispatch), plan 39 (the view's handle table and its handover). All
  committed.
- The standalone integrity fix "kernel lookup counts in the FUSE adapter"
  (this plan's R2, §4.4): must be committed before R5.
- Coordinates with nothing else in flight. The only shared external
  artefact is the `fuser` fork (a new, independent branch, §4.3).

## 1. History: designed since phase 1, never served

Verified against `main` at `9330d63`.

| When | Commit | What |
|---|---|---|
| 2026-08-22 | `8e5b5bd` (phase 1, SQLite) | DESIGN lists `READDIRPLUS`: "attributes returned with each dentry from a single `dentry JOIN inode` query — kills the stat-storm that slows `ls -la`, `find`, and rsync". No code. |
| 2026-09-12 → 09-21 | `95985b2`, `e613afb` (plan 28) | §P6 key table: the `0x02` dentry value is `(ino, kind)` "+ a denormalized attr copy for READDIRPLUS" (`docs/plans/v1/done/28-s3-native-metadata-store.md:313`, `:350`, `:404`, `:1010-1012`). `bench/prollybench/RESULTS.md:172-204`: dropping the copy costs a cold `ls -la` 12,039 pack reads per 1,000-entry directory instead of 30.4 — "keep the attr copy". |
| 2026-09-22 | `4c6705f` (enginebench) | `bench/enginebench/RESULTS.md:286`: "`readdirplus` cost is the single most important asymmetry measured" — SQLite joins `inode` per row (2.2× its own readdir), the §P6 engines' `0x02` value already carries the attrs, so readdirplus costs what readdir costs. |
| 2026-09-22 | `6c81b71` (plan 29 doc) | `docs/plans/v1/done/29-fjall-metadata-engine.md:19`: "`readdirplus` costs the same as `readdir` with the §P6 dentry attr copy". |
| 2026-09-22 | `645a3a9` (plan 29 M3c) | Every `0x01` write goes through `ns::put_inode_record` (`crates/meta/src/store/ns.rs:342`), which rewrites each link's `0x02` copy in the same transaction: "readdirplus, `recursive_size` and the published tree all read attrs from the dentry copy". Invariant test `dentry_copy_tests` (`crates/meta/src/store/mod.rs:2071`). |
| 2026-09-24 | `92c4078` (plan 30 M6) | Session-wait key table in `PROGRESS.md:11216-11218`: `lookup` waits `Dentry(parent, name)` "then `Ino(child)` for the attrs"; "`readdir`/`readdirplus(dir)` → `Dir(dir)`: every dentry of `dir`". In code the key type is `ReadKey` (`crates/meta/src/session.rs:263`). |
| 2026-09-27 | `7a681ee` (DESIGN as built) | DESIGN §12 (`docs/explanation/DESIGN.md:1523-1532`) lists `READDIRPLUS` under "Designed but not built"; the bullet (`:1561-1563`) became "from a single replica scan". |
| 2026-09-29 | `650021b` (plan 31 C4a) | `Vfs::readdir(.., cookie, plus: bool, r)` (`crates/vfs/src/vfs.rs:241-249`). |
| 2026-09-29 | `b265b6f` (plan 31 kit) | `conformance::readdir::plus_lists_the_same_entries` (`crates/vfs/src/conformance/readdir.rs:82`): plain and plus pages list the same `(ino, name, next)` — nothing about attributes, because `DirSink::add` has no attribute argument. |
| 2026-09-29 | `ed9b40b` (plan 31 C4) | Cookies became name hashes (`dir_cookie`, `crates/engine/src/view/ops.rs:2269`: unkeyed FNV-1a, `(h >> 2) + 3`): a listing resumes after the *name* it stopped at. DESIGN §12 still says "`readdir` resumes by position today" — stale since this commit. |
| 2026-10-09 → 10-13 | `8a6f96c`, `9447477`, `5804289`, `1cfc265`, `88f246f`, `34fe917` | The kernel-reference contract this plan must extend (§2, §4.4). |

No commit ever served a `READDIRPLUS` reply (`git log -S readdirplus` lists
only docs, benches, comments and the conformance case).

Two DESIGN statements are corrected by this plan (recorded in PROGRESS;
CONVENTIONS rule 5): "`readdir` resumes by position today" (§12, `:1529`),
and "stable readdir cursors: key-ordered cursors give exact resumption
offsets" (`:1552-1554`; plan 28 says the cursor "is a key"). A *name* is an
exact cursor, but FUSE, NFSv3 and `telldir` carry a 64-bit offset, which
cannot encode a position among arbitrary names: the cursor is not free (§4.9).

## 2. What the code does today

- **Contract.** `Vfs::readdir(cx, ino, fh, cookie, plus, r)`
  (`crates/vfs/src/vfs.rs:241`). `DirSink::add(ino, next, kind, name) -> bool`
  (`crates/vfs/src/responder.rs:139-144`) has no attributes, TTL or
  generation. `Entry { attr: Attr, generation }`, `Attr::ttl` one TTL for
  entry and attributes (`crates/vfs/src/types.rs:61-91`).
- **Engine.** `View::readdir` (`crates/engine/src/view/ops.rs:1518`) names
  the flag `_plus` and ignores it. Per page it: enters the op watch and
  admission, `enter!` (confinement), `inflight.enter(&[dir])` (the kernel
  invalidation hold-back, `crates/engine/src/kernel_inval.rs:431-468`); at
  `cookie == 0` only, `strict_read(dir, true, None, &[ReadKey::Dir(dir)])`
  (`crates/engine/src/view/write_gate.rs:118`); reads **the whole directory**
  (`Meta::readdir` → `ns::readdir_entries`, `crates/meta/src/store/ns.rs:581`,
  decoding `DentryRecord::ino_and_kind` and allocating a `String` per name),
  hashes and sorts every name, and emits from `partition_point(cookie)`. A
  page costs O(n log n) in the directory's size; a full listing of 10M names
  at ~680 names per 32 KiB page is ~14,700 such passes. Synthetic
  directories (`.constellation`, snapshot mirrors) list `synthetic_entries`
  with positional cookies over an immutable listing; scratch directories
  `scratch_readdir`.
- **What a lookup answers** (`ops.rs:131-230`): `strict_read(.., Dentry)`
  only (`ops.rs:200-205`; the M6 table's "then `Ino(child)`" is not in the
  code — a drift recorded for the maintainer, §8), `meta.lookup(parent,
  name)` (the `0x02` dentry copy plus an `atime` point read,
  `crates/meta/src/store/writes.rs:1345-1351`), then `current_attr(ino)`
  (`crates/engine/src/view/mod.rs:1142`): the inode row read **under the
  write shard**, with the write session's pending size/mtime/ctime overlaid —
  the reason is documented there (the kernel stores a reply's size in
  `i_size`; a stale one makes an `O_APPEND` writer overwrite acknowledged
  bytes). `entry_out` (`mod.rs:1196`) calls `note_reached`
  (`crates/engine/src/view/confine.rs:124`: a confined view remembers inodes
  it handed out) and sets generation 0. TTL is `View::ttl()` (`mod.rs:1223`):
  1 s (`TTL`, `mod.rs:189`), 0 under `--cto strict`, or
  `cto::lone_kernel_ttl()` for a lone strict sequencer.
- **getattr** (`ops.rs:232`): `session_wait(&[ReadKey::Ino(ino)])`, then
  `current_attr`.
- **Kernel references (before R2).** The adapter's `forget`
  (`crates/frontend-fuse/src/adapter.rs:1375`) ignores `nlookup` and
  forwards every `FORGET` to `Vfs::forget` — "the view keeps no lookup
  counts (plan 31 §6.11)"; fuser's default `batch_forget` calls `forget` per
  node. `View::forget` (`ops.rs:274`) → `forget_dead` + `forget_unlinked`
  (`mod.rs:943`): an orphan this view unlinked and kept for the kernel's
  reference (`unlinked_refs`, `reap_after_unlink` `mod.rs:817`,
  `keeps_referenced` `:865`; `9447477`, directories since `5804289`) is
  reaped at its `FORGET`. The view's handoff doc
  (`crates/engine/src/view/handoff.rs:7-13`) states the view keeps no lookup
  table; `unlinked_refs` crosses a handover.
- **Push invalidation.** The engine's `kernel_inval` (module doc,
  `crates/engine/src/kernel_inval.rs:1-120`) holds a notification back while
  a request on the inode is in flight and drops it past `DEFER_MAX`
  (`:146`). The adapter writes an entry invalidation only for a name the
  kernel may hold (`KernelEntries`, `crates/frontend-fuse/src/dentries.rs`,
  rule at `:44-63`), recorded from the replies that install dentries —
  `LOOKUP`, `CREATE`, `MKDIR`, `MKNOD`, `SYMLINK`, `LINK`, `RENAME`
  (`:27-34`). A readdirplus reply would install dentries the table never
  hears of.
- **FUSE adapter.** `init` (`adapter.rs:886-926`) adds
  `FUSE_PARALLEL_DIROPS`, the lock flags and `FUSE_PASSTHROUGH`; never
  `FUSE_DO_READDIRPLUS`/`FUSE_READDIRPLUS_AUTO`. `readdir`
  (`adapter.rs:1421-1440`) passes `plus = false`; there is no `readdirplus`
  method (fuser's default answers `ENOSYS`) and no `opendir`/`releasedir`
  (fuser's defaults: fh 0, no open flags, so no `FOPEN_CACHE_DIR`;
  `fsyncdir`'s doc, `adapter.rs:1355-1362`, relies on "always 0").
  `DirReply` (`crates/frontend-fuse/src/reply.rs:548-565`).
- **Mock / reference fs.** `MockVfs` records `plus` and calls
  `RefView::readdir(ino, cookie, sink)` (`crates/vfs/src/mock/reffs.rs:1666`),
  which has no `plus` and uses insertion-sequence cookies; `RefView::forget`
  drops the buried attrs (`:919`).

## 3. Kernel semantics that bind the design

Linux mainline at `4056b3c9d3c9` (2026-10-14), `fs/fuse/` unless noted.

- **K1 Negotiation.** The kernel always offers `FUSE_DO_READDIRPLUS |
  FUSE_READDIRPLUS_AUTO` (`inode.c:1462`). `process_init_reply`
  (`inode.c:1322-1326`) sets `do_readdirplus`, and `readdirplus_auto` only
  together with it (AUTO alone is ignored). Protocol 7.21, opcode 44.
- **K2 When plus is asked** (`fuse_use_readdirplus`, `readdir.c:15-29`):
  never without `do_readdirplus`; always without `readdirplus_auto`; under
  AUTO when the directory's `FUSE_I_ADVISE_RDPLUS` was set (tested and
  cleared first) or `ctx->pos == 0`. The bit is set by a *positive*
  `fuse_lookup` in the directory (`dir.c:659-660`) and by a revalidation
  fast-path hit on a child that readdirplus linked under AUTO
  (`FUSE_I_INIT_RDPLUS`, `dir.c:461-468`, set at `readdir.c:261-262`).
  So under AUTO one open directory gets `READDIR` and `READDIRPLUS`
  interleaved at arbitrary offsets: **their cookies must be
  interchangeable** (ours are: §4.9).
- **K3 No ENOSYS fallback.** An error answer to `READDIRPLUS` reaches
  `getdents` (`readdir.c:402-403`); nothing turns plus off. A server that
  negotiated it must serve it — including a resumed one (§4.10).
- **K4 Per entry** (`parse_dirplusfile` `readdir.c:291-334`,
  `fuse_direntplus_link` `:153-268`):
  - node id 0 → not linked, no `nlookup`, still emitted (`:168-177`);
  - `.` and `..` → not linked, no `nlookup`, **no FORGET** (`:179-188`);
  - node id 1 (`FUSE_ROOT_ID`) or invalid attributes → `-EIO` for the
    entry, which is then force-forgotten (`:190-193`);
  - an existing dentry with the same node id → `nlookup++`,
    `fuse_change_attributes(.., attr_version)` (`:224-232`); a different
    node id, a negative dentry or a stale inode (generation/type differ) →
    `d_invalidate` and a new dentry (`:206-236`): **a plus entry overrides
    what the dentry cache held**;
  - a new dentry → `fuse_iget` (`nlookup++`, `inode.c:511-514`) and
    `d_splice_alias` (on error `nlookup--`, fix `b8bd342d50cb`);
  - every non-zero return of `fuse_direntplus_link` →
    `fuse_force_forget(file, nodeid)` (`readdir.c:270-289`, `:328-330`): a
    synchronous `FUSE_FORGET` with **`nlookup = 1`**, sent while the kernel
    may well hold the same inode through other dentries — a **partial
    forget**;
  - entry and attribute timeouts are applied as `LOOKUP`'s
    (`fuse_change_entry_timeout`, `dir.c:288-309`); `attr_version` and the
    evict counter are sampled **before** the request (`readdir.c:393-394`).
- **K5 The whole reply is linked, and any of its cookies may come back.**
  Once the user's buffer is full the rest of the reply is still linked
  (references taken) but not emitted (`readdir.c:313-330`); the next
  `getdents` asks again from the last *emitted* entry's cookie — any cookie
  of the previous reply, not only its last — and the server answers (and
  counts) those entries again. Every count is balanced by eventual
  `FORGET`s. The request size is `clamp(ctx->count, PAGE_SIZE, max_pages <<
  PAGE_SHIFT)` (`readdir.c:383`, `dabb90391028`, `4dd6f6d3085a`; older
  kernels: one page).
- **K6 Leak paths** (server counted, kernel never forgets): an empty name,
  a name over `FUSE_NAME_MAX` or containing `/` (`-EIO` for it and all
  later entries, `readdir.c:306-311`), a truncated trailing record, a
  reply larger than requested (rejected whole, `dev.c:1868-1876`). The
  daemon never produces any of them (names come from the replica through
  the name policy; fuser never overfills).
- **K7 Zero timeouts.** A dentry and attributes with timeout 0 are still
  instantiated with a reference (`dir.c:290-298`, `inode.c:235`); the next
  walk revalidates with `LOOKUP` and the next `stat` sends `GETATTR` — pure
  overhead. Under AUTO an `entry_valid` of 0 means later chunks fall back to
  `READDIR` (no revalidation fast-path hit).
- **K8 Notifications.** Plus-installed dentries are ordinary dentries.
  `notify_inval_entry` takes the parent's `i_rwsem` exclusively
  (`dir.c:1612`) while `getdents` holds it shared for the whole round trip
  (`fs/readdir.c:105-120`): a notification about the directory waits for
  the readdirplus and is applied after its dentries are installed.
  `notify_inval_inode` bumps `fi->attr_version` (`inode.c:545-575`), so an
  in-flight plus reply sampled earlier cannot overwrite that inode's
  attributes (`inode.c:343-347`); for an inode the kernel does not hold yet
  it answers `-ENOENT` and does nothing, so a plus reply that read the
  replica just before the change installs the old attributes for one TTL —
  exactly what a `LOOKUP` does today (the TTL bound the design accepts).
- **K9 Attribute installation** goes through `fuse_change_attributes` /
  `fuse_iget` → `fuse_change_attributes_i` — the path `LOOKUP` and
  `GETATTR` replies take (`inode.c:392-397`, `:516`). The writeback cache
  (not negotiated by constellation) would make the kernel ignore the
  server's size for regular files (`inode.c:304-341`). Readdirplus
  therefore inherits the attribute race the proposed upstream kernel patch
  for the FUSE attribute race addresses, and adds more installs through
  that same function; no plus-specific kernel change is needed.
- **K10 Readdir cache.** With `FOPEN_CACHE_DIR` a complete cache serves
  `getdents` without any request (no plus, no attribute refresh) until
  invalidated (`readdir.c:496-638`); plus output fills the cache with the
  dirent part only (`readdir.c:117-127`), and cached cookies written by one
  stream are later presented to the server by other streams. Constellation
  answers `OPENDIR` (fuser's default) without the flag, so no cache.
- **K11 Epoch.** `fuse_direntplus_link` reads `fc->epoch` after the reply
  (`readdir.c:196`), unlike `fuse_lookup` (before the request). Irrelevant:
  constellation never sends `FUSE_NOTIFY_INC_EPOCH`.
- **K12 Resume from a bare cookie.** FUSE directories use
  `generic_file_llseek` (`dir.c:2439`): `seekdir` passes any earlier `d_off`
  straight through, and `rewinddir` is offset 0. `nfsd` opens the directory
  per request and seeks to the client's cookie on a fresh file description
  (`fs/nfsd/vfs.c`, `nfsd_readdir`, which ignores the verifier). A cookie
  must therefore resolve without any per-handle state.

## 4. Decisions and design

### 4.1 Negotiation and the knob

`CONSTELLATION_FUSE_READDIRPLUS` = `auto` (default) | `always` | `off`,
read where the other `CONSTELLATION_FUSE_*` knobs are
(`crates/frontend-fuse/src/session.rs`, `MountOptions`), carried into
`KernelTuning` (`adapter.rs:93`) as `readdirplus: ReaddirPlus`, documented in
`docs/reference/configuration.md`. In `FuseFs::init`:

- `auto` → `add_capabilities(FUSE_DO_READDIRPLUS | FUSE_READDIRPLUS_AUTO)`;
- `always` → `FUSE_DO_READDIRPLUS` only (every chunk plus, K2);
- `off` → neither (today's behaviour).

A refused capability (an old kernel) is logged once at info and the session
serves plain `READDIR`. **Why `auto`**: K2 makes the kernel ask for plus when
the directory is being stat'ed (a positive lookup or a revalidation of a
plus-linked child) and for the first chunk, so `ls`, `find -name` and
`getdents`-only walks keep plain pages (a plus record is ~4× a plain one, so a
page holds ~4× fewer names), while `ls -l` and `find -ls` get plus from the
second chunk on. `always` exists for benchmarks and workloads that stat
everything.

### 4.2 The contract

`DirSink` gains one method with a default (other frontends and sinks keep
compiling and behaving as today):

```rust
/// One entry of a `plus` listing. `entry`: what a `lookup` of `name` in the
/// listed directory answers right now (attributes, TTL, generation), which
/// the frontend may hand its platform as a reference exactly like a
/// lookup's — or `None`: the name only (`.`/`..`, a name whose answer the
/// view cannot vouch for, a zero TTL). Returns `true` when full (not added).
fn add_entry(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8],
             entry: Option<&Entry>) -> bool {
    let _ = entry;
    self.add(ino, next, kind, name)
}
```

- `Vfs::readdir`'s doc (`vfs.rs:238-249`) states the contract: under `plus`
  a view calls `add_entry`; every `Some(entry)` it hands out is a reference
  the platform holds until `Vfs::forget`, exactly as a `lookup`'s `Entry`;
  the entry is what `lookup(dir, name)` would answer at that moment, waited
  for as `getattr` of the child waits (§4.5); `.` and `..` are always
  name-only; plain and plus pages use the same cookies; **a view never
  answers a page with an error after it added an entry** (a per-name
  failure makes that entry name-only), so a frontend can account the added
  entries when the page succeeds and none when it fails (§4.7). The cookie
  rules (deterministic in the name, resolvable from any earlier reply,
  `Stale` when a cold cookie's name is gone) are stated there too (§4.9).
- `Vfs::forget`'s doc (`vfs.rs:61-70`) is unchanged in meaning: the
  platform no longer references the inode **at all** — a frontend whose
  platform sends partial forgets counts them itself (§4.4).
- `responder::DirEntry` gains `entry: Option<Entry>`; `CollectDir`
  (`responder.rs:155-190`) and the kit's `ProbeDir`
  (`crates/vfs/src/conformance/facade.rs:386-397`) record it.
- `OpKind::Readdirplus => "readdirplus"` (`crates/vfs/src/ctx.rs:44-75`), so
  the op metrics and `OpWatch` separate the two; the engine's watch name is
  `"readdirplus"` under `plus`.
- No `Vfs::opendir`/`releasedir`: the listing design needs no handle state
  (§4.9).

### 4.3 The `fuser` fork

Nothing in the fork is *required* to serve plus: the trait method
(`src/lib.rs:788-798`, default `ENOSYS`), `ReplyDirectoryPlus`
(`src/reply.rs:1134-1185`), the opcode decode (`ll/request.rs:1836-1839`),
dispatch (`request.rs:487-499`), the ACL allowlist (`request.rs:104`) and
both transports are in place. On io_uring, `READDIR`/`READDIRPLUS` run on
the offload threads under the default `RingDispatch::Reads`
(`session.rs:1242-1266`; deliberately, `replicix/PATCHES.md:338-340`), while
`RELEASEDIR` runs on the ring thread itself (`session.rs:1258`) — this plan
adds nothing to `releasedir`. Two API limits matter:

1. `ReplyDirectoryPlus::add` takes the node id from `attr.ino` and the dirent
   type from `attr.mode` (`ll/reply.rs:478-479`, `:532-544`): there is no way
   to send a **name-only entry** (node id 0, real `d_ino` and type). Node id
   0 with `d_ino` 0 is not acceptable: glibc up to 2.35 skips records with
   `d_ino == 0` (fixed in 2.36, BZ #12165), and on any glibc `ls -i` would
   show 0 for them — so name-only entries carry the real `d_ino`.
2. No `remaining_capacity()` (upstream has it).

**Decision:** a new fork branch `readdirplus` off `v0.18.0` (REPLICIX.md's
one-branch-per-change rule), merged into `replicix`, adding
`ReplyDirectoryPlus::add_name(ino, offset, kind, name) -> bool` (node id 0,
`entry_valid`/`attr_valid` 0, zeroed `fuse_attr`, `d_ino = ino`,
`d_type` from `kind`) and the `remaining_capacity()` backport, with unit
tests for the encoding (fork `cargo test --lib`, with and without
`--features io-uring`). One TTL for entry and attributes stays (constellation
uses one for lookups too, `reply.rs:92`). Constellation's `Cargo.toml`
`[patch]` rev moves to the new `replicix` merge.

### 4.4 Kernel references: the adapter counts lookups (R2, landed separately)

**The hole in today's code.** The adapter forwards every `FORGET` as "the
kernel let go" (`adapter.rs:1375`), and its comment argues that the one
partial forget — the single lookup a dentry revalidation's `LOOKUP` added
when it returned another inode than the dentry's (`fuse_dentry_revalidate`)
— never names an unlinked inode. It can: the kernel holds inode Y under
name `m` and X under `n`; another node runs `mv m n`; a local `stat n`
revalidates `n`, whose `LOOKUP` answers Y ≠ X, and the kernel sends
`FORGET(Y, 1)` while `m`'s dentry still holds Y. That `FORGET` is queued
asynchronously and may be dequeued by a FUSE worker after a local `rm n`
unlinked Y's last name (`reap_after_unlink` put Y in `unlinked_refs`): the
forward then reaps the orphan (`View::forget_unlinked`) while the kernel
still references it, and `fstat` of an `O_PATH` descriptor of `m` answers
`ESTALE` instead of `nlink 0`. Readdirplus adds a second partial-forget
source (K4's `fuse_force_forget`). **Decision: the FUSE adapter keeps the
kernel's lookup counts** and calls `Vfs::forget` only when an inode's count
reaches zero. This is being implemented now as a standalone integrity fix;
**the executor verifies it is present** and builds on it (names below are
the intended ones; use whatever the fix landed with):

- A sharded `ino → count` table in `crates/frontend-fuse/` (`KernelRefs`):
  `took(ino)`, `forgot(ino, n) -> Last | Partial | Unknown`,
  `export()`/`import()`, `len()`.
- Counted on success only, before the reply is written (a `FORGET` can only
  follow the reply, but may be read by another worker before a post-send
  increment would run): `EntryReply` (lookup, mknod, mkdir, symlink, link;
  `reply.rs:87`) and `CreateReply` (`reply.rs:329`), in their `done(Ok)`.
  Errors count nothing. A reply the kernel then drops (connection abort)
  over-counts: the inode is kept until the session ends — a bounded leak,
  never a premature reap.
- `forget` and an explicit `batch_forget` (opcode 42, instead of fuser's
  per-node default) subtract; `Last` → `Vfs::forget`; `Partial` → nothing;
  `Unknown` (never counted: the root's implicit reference) → `Vfs::forget`
  (the old behaviour), counted. Underflow clamps to zero.
- **Handover.** Two records carry the table: the in-process `FuseHandoff`
  (`crates/frontend-fuse/src/session.rs:693`; exported at detach after the
  session stopped reading, imported by `FuseSession::resume`, `:1339`,
  before it serves) and the cross-process `MountHandoff`
  (`crates/cli/src/handover.rs:356-375`), following the `passthrough`
  field's pattern (`handover.rs:888`, `:1105`;
  `crates/cli/src/handoff_socket.rs:640`). `FORGET`s queued in the kernel
  during the gap are read by the next image after the import.
- **No version bump.** `handover.rs:124-125` says "Bump it on any change to
  what crosses"; it is reworded to the maintainer's rule (formats change in
  place, no bump, no compatibility shim). Consequence: the first `daemon
  --upgrade` across the fix cannot parse the other image's handoff and must
  refuse cleanly before anything is detached, so that upgrade is a remount,
  not a handover. The fix verifies the refusal is clean and records it in
  PROGRESS; R2 below re-checks it.
- `handoff.rs`'s module doc (`:7-13`) and the adapter's `forget` comment are
  corrected: the *frontend* keeps the lookup table, both partial forgets are
  named.

This plan adds one counting site: each plus entry handed with a node id
(§4.7).

### 4.5 What a plus entry says: a lookup's attributes, waited for as `getattr`

For each name of the page the engine produces what `lookup(dir, name)`
would answer at that moment, after the session wait `getattr` of the child
does, or a name-only entry. The page loop (names come from the listing,
§4.9):

1. **Zero TTL** (`self.ttl().is_zero()`, `--cto strict` on a node that is
   not a lone sequencer) → every entry name-only; skip 2–4. K7: attributes
   with timeout 0 are pure overhead, and under AUTO the kernel then falls
   back to plain pages by itself. A lone strict sequencer's short TTL is
   non-zero and gets plus entries (its TTL bound is already respected by
   `cto::lone_kernel_drain_ms`, which covers every reply that carries it).
2. **Session wait per batch.** Take the next K = 256 names of the listing
   (K exceeds any plus page: 32 KiB / ~176 B ≈ 186 records; a larger
   `max_pages` buffer just takes another batch), call
   `self.session_wait(&keys)` once with one `ReadKey::Ino(child)` per name,
   then emit (steps 3–4) until the sink is full or the batch is used up;
   repeat. `Dir(dir)` (the listing's cookie-0 wait) alone is weaker than
   `getattr`'s `Ino(child)`: a queued replay or an unshipped durable-gated
   row that touched only the child's inode does not match `Dir(dir)`
   (`KeySet::touches`, `crates/meta/src/session.rs:370-376`;
   `Meta::unshipped_seq_for`, `crates/meta/src/store/mod.rs:1151-1175`). The
   fast path is two loads. The wait comes **before** any write-shard lock
   (plan 30 §M6: never block holding a shard).
3. **Attributes under the shard, from the dentry copy.** For each name:
   `let shard = self.writes.lock(ino)`; `self.meta.lookup(dir, name)`
   (`scratch_lookup` in a scratch directory) — the `0x02` copy plus the
   `atime` point read, as `MetaStore::lookup` does; if the dentry is gone,
   now names another inode, or the read fails → name-only (the kernel will
   `LOOKUP` it; the page itself never fails after an entry was added);
   else overlay `self.writes.pending(&shard, ino)` and release. This is as
   fresh as `current_attr`: `put_inode_record` writes the copy in the same
   transaction as the inode row (`645a3a9`), and a flush commits and
   retires a session under the same shard lock, so the read sees either the
   session or its committed row — the race `current_attr`'s doc describes
   cannot occur. It costs one point read per name, reads the bytes the §P6
   encoding put there for this purpose, and never decodes attributes for
   names outside the page.
4. **The entry** — `entry_out(&visible_attr(attr))` (`mod.rs:1196`): the
   view's TTL, generation 0, and `note_reached` for confined views. A child
   that is the view root or `ROOT_INO` (impossible by construction) is
   emitted name-only defensively (K4: node id 1 is `-EIO`).

Synthetic directories: each child's entry is `lookup_synthetic(dir, name)`
(`crates/engine/src/view/synthetic.rs:220`), what a lookup answers there
(snapshot mirrors: the frozen attributes; inactive nodes → name-only). The
synthetic numbering already crosses a handover (`HandleTableSnapshot.
synthetic`). `getattr`'s `dead_attr` fallback is not needed: a listed name is
linked.

### 4.6 Session guarantees, strict mode, speculation

- The listing keeps its `strict_read(dir, true, None, &[Dir(dir)])` at
  cookie 0 (strict: a `ReadIndex` or a delegation; bounded: M6's wait);
  §4.5 step 2 adds the per-child `Ino` keys per batch, so a plus entry is
  never older than a `getattr` of the child at that moment — stronger than
  `lookup` as coded (`Dentry` only, §2), as the M6 table intended.
- `ReadKey::Dir` is "never covered by speculation" (`session.rs:268-270`):
  plus inherits that; the per-child `Ino` keys may be covered by a shadow
  exactly as `getattr`'s are.
- `--cto strict`: §4.5 step 1. Pending-size overlay: step 3. Lock fences,
  owner fences and the write gate are untouched (plus reads, never writes).

### 4.7 The adapter: `readdirplus` and push invalidation

- `FuseFs::readdirplus(req, ino, fh, offset, reply: ReplyDirectoryPlus)`
  next to `readdir` (`adapter.rs:1421`): `obs().begin(OpKind::Readdirplus)`,
  then `vfs.readdir(.., offset, true, op.responder(DirPlusReply { .. }))`.
- `DirPlusReply` (`reply.rs`, beside `DirReply`) implements `DirSink`:
  `add_entry(.., Some(e))` → `ReplyDirectoryPlus::add(ino, next, name,
  &e.attr.ttl, &fuse_attr(&e.attr), Generation(e.generation))` and, if it
  fit, remembers `(ino, name, ttl)`; `None` (and plain `add`) →
  `add_name(ino, next, kind, name)`. **Accounting on success only:** in
  `done(Ok)`, right before `reply.ok()`, it takes one kernel reference per
  remembered entry (`KernelRefs::took`) and records each name in
  `KernelEntries` (expiry now + TTL + `slack`); `done(Err)` accounts
  nothing (the contract guarantees no error after an entry was added,
  §4.2).
- **`KernelEntries` coverage, per name, no directory marker.** The engine's
  `InFlight` (`inflight.enter(&[dir])` in `View::readdir`) already holds
  every notification about the directory back until `r.done` has returned,
  and `done` records the names before it writes the reply, so a held-back
  notification sees them as cached when it is finally written, and the
  kernel applies it after the reply's dentries (K8). A directory-wide
  "plus in flight" marker would add nothing for our own names and would
  make every *foreign* name's notification during the listing look cached —
  each one a write that parks the notification thread on the directory's
  `i_rwsem` (`dentries.rs`'s whole reason to exist). Name-only entries
  install nothing and are not recorded. The module doc's list of
  installing replies (`dentries.rs:27-34`) adds `READDIRPLUS`.
- Child attribute invalidations (`notify_inval_inode` of a child) need no
  hold-back for plus: they take no directory lock, and K8's `attr_version`
  bump makes the kernel discard an older plus attribute for an inode it
  holds; for one it does not hold yet, the old attributes live one TTL,
  exactly as after a `LOOKUP` today (K8).

### 4.8 Views, policies and the edges

- **Subtree and snapshot views, confinement.** `enter!` and the dir check
  are unchanged; every plus entry passes `entry_out` → `note_reached`
  (`reached` crosses a handover), so an inode handed out by plus stays
  addressable exactly like a looked-up one. Snapshot views are frozen and
  never registered for invalidation; their entries carry the view TTL.
- **Scratch areas**: the scratch keyspace uses the same `(parent, name)`
  encoding, so its listing pages the same way (§4.9); attributes via
  `scratch_lookup` + the overlay (scratch files have write sessions,
  `ops.rs:176-193`).
- **Removed directories** (`5804289`): `readdir` of an orphan directory
  answers `ENOENT` before any entry (`ns::require_dir` checks `ns` only);
  the new page function keeps that check.
- **Permissions.** Mounts use `default_permissions`
  (`crates/platform/src/mounts.rs:127`): the kernel checks read permission
  on the directory at open and search permission at every path walk,
  cached dentry or not; `getdents` returns only `d_ino`/`d_type`. Plus
  attributes therefore expose nothing a caller could not `stat`; the engine
  does no per-caller check in `lookup` either.
- **xattrs**: not involved (no virtual xattr is listed or computed).
- **atime**: plus entries report the `atime` table's value, as `lookup`
  does; listing a directory bumps no atime, plus or not (plan 20 bumps on
  file reads only).
- **Hidden names**: `.constellation` stays hidden from listings (DESIGN
  §12).
- **Synthetic directories** keep their positional cookies over immutable
  listings (exact, since a frozen directory never changes); their
  whole-listing cost per page is unchanged here (a follow-up if snapshot
  mirrors of huge directories matter).

### 4.9 Listing pages: name order, keyed cookies, a handle-free hint cache

**Why it must change.** A plain record is a 24-byte `fuse_dirent` plus the
padded name (~48 B for a 20-byte name); a plus record adds the 128-byte
`fuse_entry_out` (~176 B). With glibc's 32 KiB `getdents` buffer clamped by
`max_pages` (K5) a plain page holds ~680 names and a plus page ~185; on a
kernel before `dabb90391028`, one 4 KiB page: ~85 and ~23. With today's
O(n log n) page (§2), a 100k-entry `ls -l` costs ~540 (or ~4,300) full
read-and-sorts of 100k entries instead of ~150 (or ~1,200) today, and a
full listing of 10M names is already ~14,700 of them — quadratic either way.

**Decision (the research's option (c\*); prior art in §5).** Dentries are
already keyed `(parent, name)` (`crates/mtree/src/keys.rs`), so:

- **Pages are name-ordered range scans** of `0x02 | dir | …` from a resume
  name (exclusive), in batches (K = 256, §4.5), emitted until the sink is
  full. No full read, no sort, no allocation of the listing: O(page). This
  applies to plain `READDIR` too (it fixes today's quadratic listing) and
  to scratch directories. New `Meta` read: `readdir_after(parent, after:
  Option<&[u8]>, limit) -> Vec<DirEntry>` (plus a scratch twin), keeping
  `require_dir`; `Meta::readdir` stays for its other callers.
- **The cookie is a keyed hash of the name**: `dir_cookie(name) =
  (siphash13(k, name) >> 2) + 3` — the same `[3, 2^62 + 2]` range as today
  (clear of 0 = rewind, 1 = after `.`, 2 = after `..`, below `i64::MAX`),
  identical for plain and plus pages, in every stream, across restart and
  handover, and on every node. The key `k` is derived per filesystem from
  the secret material it already stores durably — under E2E from the master
  key (as the gossip seed is), otherwise from `FsMeta::gossip_secret`
  (`crates/store-s3/src/store.rs:88-104`) — with a domain-separation label,
  so `meta.json` does not change. A filesystem so old it has no
  `gossip_secret` derives from its UUID, which is not secret: logged once
  at warn (the maintainer may prefer a new `meta.json` field, §8). Why
  keyed: with today's unkeyed FNV-1a a user can create names with equal
  cookies and make another user's listing skip or repeat entries (ext4
  seeds its hash per filesystem for the same reason).
- **Hint cache** (`crates/engine/src/view/listing.rs`, new), keyed by
  `(directory, cookie)` — **never by file handle**. Each served page
  records `{anchor: the name before the page's first entry (None: the
  directory's start), cookies: the emitted entries' cookies in order}`; the
  last 4 pages per directory are kept, LRU across directories, under a hard
  byte cap (`CONSTELLATION_READDIR_HINT_BYTES`, default 16 MiB per node,
  shared by the node's views; ~16 B per remembered cookie plus one name per
  page). A request cookie `c` found at index `i` of a remembered page
  resumes by scanning from that page's anchor and skipping every entry whose
  cookie is in `cookies[0..=i]` (the ones the kernel consumed); everything
  else is emitted. That is exact even if the entry with cookie `c`, or the
  anchor, has since been removed (a key seek past a name does not need the
  name to exist), covers K5's "resume from any cookie of the previous
  reply", interrupted reads, `seekdir` back within the last pages and a
  different handle (`nfsd`, K12), and costs O(page).
- **Cold cookie** (not in the cache: a far `seekdir`, `nfsd` re-export, a
  restart or handover mid-listing, eviction): one O(n) scan of the
  directory's keys in name order — hashing names, no sort, no listing kept
  — until a name hashes to `c`; resume after it and prime the cache. A
  collision (two names with cookie `c`) resumes after the **first** in name
  order: entries between them may be listed twice, never skipped (at 62
  bits the expected number of colliding pairs is ~n²/2⁶³ ≈ 1×10⁻⁵ at 10M
  entries; counted).
- **Cold cookie whose name is gone** (no hint, and no name hashes to it —
  the entry was removed and the hint lost): the page answers
  `Code::Stale` (`ESTALE`) rather than guessing — the only inexact case,
  documented in `Vfs::readdir` and `configuration.md`. It needs a listing
  paused across a restart, handover or eviction *and* removal of exactly
  that entry.
- **In-page collisions.** A page never holds two entries with equal
  cookies: when the next entry's cookie already occurs in the page, the
  page ends before it (it starts the next page, anchored by name), so a
  remembered page maps each cookie to one entry.
- **Concurrent mutation.** A name-ordered scan resumed after a name returns
  every entry present throughout exactly once (inserts and removals
  elsewhere cannot shift it); entries created ahead of the cursor appear,
  behind it do not; a rename that moves a name past the cursor may show it
  twice (removed and added: allowed). This matches the libfuse low-level
  contract and POSIX `readdir(3p)`, and is stronger than today's (no
  dependence on a per-page re-sort).
- **32-bit userspace.** Non-LFS 32-bit glibc programs fail `getdents` with
  `EOVERFLOW` on a `d_off` above 2³¹−1, and `telldir`/`seekdir` (`long`)
  cannot round-trip a 62-bit cookie on any 32-bit userspace; FUSE gives the
  server no hint of the caller's ABI. Decision: 62 bits by default, and a
  per-mount `CONSTELLATION_READDIR_COOKIE_BITS=31` for legacy 32-bit fleets
  (cookie `(h mod (2³¹ − 4)) + 3`). At 31 bits collisions are common at
  scale (~0.5 % of entries at 10M), but they only affect the cold path
  (duplicates, never skips) since in-page cookies are unique.
- **Memory and writes.** O(1) per stream, the shared hint cap, nothing per
  open directory; no write-path cost at all. Nothing crosses a handover;
  `opendir`/`releasedir` stay fuser's defaults (fh 0), so `fsyncdir`'s
  "always 0" doc (`adapter.rs:1355-1362`) stays true and nothing new runs
  on the io_uring ring thread for `RELEASEDIR`.
- **The cookie function changes once.** A listing paused across the first
  handover from an FNV-cookie image presents an old cookie: cold, no match,
  `ESTALE` once (no compatibility shim, maintainer rule). Recorded in
  PROGRESS.

**Rejected and deferred.**

- *(a) Per-open listing snapshots* (this plan's first draft): ≈0.5–1 GB per
  open stream at 10M entries (48–96 B per entry), a first page that costs a
  full read, a budget that leaves exactly the large directories uncovered,
  and cookies that cannot be ordinals if they must be stream-independent.
- *(b) A node-local `(parent, hash, name)` index* (btrfs/TableFS/bcachefs
  style): exact and stateless, but one more put/delete per dentry change
  (two per rename) on every write path — local ops, replay, inbox, scratch,
  speculation rollback, bootstrap — and a rebuild after a tree bootstrap.
  **Documented escalation, not built**: if `readdir_cold_scans` shows the
  O(n) fallback matters (busy `nfsd` re-export of huge directories, frequent
  restarts), build it lazily per directory above a size threshold.
- *`FOPEN_CACHE_DIR`*: now safe (cookies are stream-independent, K10) —
  a follow-up, enabled per directory below a size cap, relying on the
  existing parent-inode notification to reset the kernel cache.

### 4.10 Transport and handover

- `/dev/fuse` and io_uring: no difference (§4.3).
- A resumed session gets no `FUSE_INIT`: it serves `READDIRPLUS` whenever
  the kernel sends it (K3), whatever the new image's knob says; the
  `NegotiatedInit.flags` decide, as for passthrough
  (`crates/frontend-fuse/src/session.rs:712-713`). The fork's
  `check_resumable` (`src/session.rs:277-292`) refuses only flags outside
  `InitFlags::all()`, which contains both bits
  (`src/ll/flags/init_flags.rs:36-38`); constellation's `session.rs` never
  mentions them today. With `off` in the new image the entries are
  name-only (logged once).
- What crosses: the lookup table (§4.4); `KernelEntries` keeps its
  `resumed()` unfiltered window (`dentries.rs:68-71`), which covers
  plus-installed dentries too; the hint cache does not cross (a cold cookie
  after a handover takes the scan, §4.9).

### 4.11 Observability

- Per session (`SessionStats`, `crates/frontend-fuse/src/stats.rs:355`) and
  in `node.status` per mount: `readdirplus` (`off`/`auto`/`always`, as
  negotiated), `readdirplus_entries`, `readdirplus_name_only`, and (from
  R2) `kernel_refs`, `forgets_final`, `forgets_partial`, `forgets_unknown`.
- Per view in `node.status`: `unlinked_refs` (the orphans kept for kernel
  references — there is no orphan count in status today), and the listing
  counters `readdir_hint_hits`, `readdir_cold_scans`,
  `readdir_cold_scan_entries`, `readdir_stale_cookies`,
  `readdir_cookie_collisions`, `readdir_hint_bytes`.
- `/metrics`: `constellation_fuse_readdirplus_entries_total{kind="attr"|
  "name_only"}`, `constellation_readdir_cold_scans_total`,
  `constellation_readdir_stale_cookies_total`,
  `constellation_readdir_hint_bytes` (gauge), plus R2's
  `constellation_fuse_kernel_refs` and
  `constellation_fuse_forgets_total{outcome}`; the op metrics gain
  `op="readdirplus"`. Status schema re-blessed
  (`crates/control/src/schema.rs`).

## 5. Prior art for §4.9

- **Local Linux file systems make the cookie a property of the structure,
  so resume is a stateless seek.** ext4 (htree): hash order, a per-filesystem
  *seeded* name hash as the cookie, 31 or 63 bits by the caller's ABI; the
  31-bit era showed how fast collisions grow (≈0.1 % at 2k entries, >90 % at
  100k) — LWN "Widening ext4's readdir() cookie" (lwn.net/Articles/544520).
  XFS: the entry's address in a data space that never moves (31 bits).
  btrfs: a second key per name, a per-directory creation index
  (`BTRFS_DIR_INDEX_KEY`), bounded at open (`last_index`) — option (b)'s
  model and its write cost. tmpfs/libfs: per-dentry offsets in a maple tree,
  added because `nfsd` opens per request (commit `a2e459555c5f`), and the
  rename-chase CVE-2024-46701. bcachefs and OpenZFS: the hash is the key
  (probing / a collision differentiator). Sources:
  github.com/torvalds/linux `fs/ext4/dir.c`, `fs/xfs/xfs_dir2_readdir.c`,
  `fs/btrfs/inode.c`, `fs/libfs.c`; github.com/openzfs/zfs
  `module/zfs/zap_micro.c`.
- **Network and distributed file systems.** NFS carries a 64-bit cookie
  plus a verifier; RFC 8881 §18.23: cookies "SHOULD NOT change if the
  directory is modified", 0–2 reserved; Linux `nfsd` ignores the verifier
  and seeks a fresh file per request (rfc-editor.org/rfc/rfc1813.txt,
  rfc8881.txt; `fs/nfsd/vfs.c`). **CephFS is the closest precedent**: the
  client keeps the *last name* per open directory and the MDS resumes with a
  `lower_bound` on `(hash, name)` — exact even if that name was deleted; a
  bare hash cookie is used only after `seekdir` (`fs/ceph/dir.c`,
  github.com/ceph/ceph `src/mds/Server.cc`). FreeBSD tmpfs: a hash cookie
  plus a cached last cursor and "recover if direntry for cookie was removed"
  (`sys/fs/tmpfs/tmpfs_subr.c`). GlusterFS shows what lossy cookie
  transformation does (broken by ext4's 64-bit cookies). HDFS can use a
  name cursor because its RPC carries one; FUSE cannot.
- **Object-store FUSE clients use ordinal offsets** into a per-handle or
  per-directory list (mountpoint-s3, gcsfuse, goofys, s3fs, rclone,
  SeaweedFS, JuiceFS, libfuse's high-level API): cheap, but meaningless
  across handles, restarts and `nfsd`; JuiceFS and SeaweedFS keep a name
  cursor for the next batch but degrade to O(offset) rescans after any seek
  (github.com/awslabs/mountpoint-s3, github.com/juicedata/juicefs
  `pkg/meta/base.go`).
- **FUSE servers.** libfuse's low-level contract: an offset is one
  previously returned "for the same directory handle", and changes "must
  never cause readdir() to skip over unrelated entries or to report them
  more than once" (github.com/libfuse/libfuse `include/fuse_lowlevel.h`) —
  `nfsd` and the kernel readdir cache break the handle scoping in practice.
  go-fuse keeps `lastRead` and mountpoint-s3 `last_response` only to replay
  a partly consumed reply (K5); the Linux fix `c6cdd51404b7` and
  `FILLDIR_FLAG_NOINTR` came from READDIRPLUS skipping an entry on a signal.
- **The take-away**: an LSM keyed `(parent, name)` lists in name order for
  free; systems that need a numeric cookie on top either key by hash
  (ext4, TableFS, bcachefs, ZFS) or add a second key (btrfs). The chosen
  design keeps the name-order key, makes the cookie a keyed hash of the
  name, and pays for resolution with a small cache and, rarely, one scan.

## 6. Milestones

Each milestone ends green on `cargo fmt`/`clippy -D warnings`/`cargo test
--workspace`; the full CONVENTIONS gates run at the end (§10).

- **R0 — Baseline benchmark.** `bench/readdirplus/` (new): `run.sh` and
  `RESULTS.md` (§8). Run today's binary (no plus, today's listing) and
  record every number §8 lists, including the write-path benchmarks and the
  1M/10M listings. Nothing else changes.
- **R1 — fuser fork.** Branch `readdirplus` (§4.3): `add_name`,
  `remaining_capacity`, encoding tests; merge into `replicix`; bump the
  `Cargo.toml` rev; record the branch in the fork's `REPLICIX.md` table and
  `replicix/PATCHES.md`.
- **R2 — Kernel references: landed separately (integrity fix), verify
  present.** Check that the adapter keeps lookup counts (§4.4): the table,
  counting in `EntryReply`/`CreateReply` on success, `forget` and
  `batch_forget` overrides, the count in both `FuseHandoff` and
  `MountHandoff`, the reworded `HANDOVER_VERSION` comment, the corrected
  `forget` and `handoff.rs` docs, and its tests — at least: wire
  (`crates/frontend-fuse/tests/wire.rs`) `LOOKUP` twice then `FORGET(ino,
  1)` does not reach `MockVfs::forget` and a second one does; the same
  with `BATCH_FORGET` (opcode 42) carrying two nodes; an unknown ino's
  `FORGET` is forwarded; engine (`crates/engine/src/view/vfs_tests.rs`) an
  unlinked file referenced twice survives a partial forget with `getattr`
  answering `nlink 0`, and the final forget reaps it; the deterministic
  revalidation reproducer of §7 `readdirplus-orphan-forget`'s first phase.
  Re-check that the first `daemon --upgrade` across it refuses cleanly
  (a remount) and that PROGRESS records it. Anything missing is completed
  here before R5.
- **R3 — Listing pages and cookies (plain `READDIR` too).** §4.9: the keyed
  cookie and its key derivation, `Meta::readdir_after` (and the scratch
  twin), the page loop in `View::readdir`, the hint cache with its byte
  cap, the cold scan, `Stale` for a vanished cold cookie, the in-page
  collision rule, the 31-bit knob, counters. Tests: meta — `readdir_after`
  pages equal a full scan, resume after a removed name; engine — a full
  listing of 100k names reads each dentry O(1) times (a scan counter; on
  `main` the count is quadratic: **fail-before**); resume from every cookie
  of the last reply (K5) and from a cookie three pages back (hint hits);
  resume from a cookie through a second, fresh view of the node (no shared
  handle; cold scan, then hits); a cold cookie whose name was removed is
  `Stale`, never a silent skip; two names forced to one cookie (a test-only
  key/collision hook) are never skipped by a cold resume; plain and plus
  pages from one cookie sequence list the same entries; the cookie of a
  name is identical on two nodes of one filesystem and differs between two
  filesystems; the 31-bit mode stays below 2³¹−1. The kit's
  `cookies_resume_where_they_left_off`, `removal_between_pages_never_
  repeats_or_loses` and `concurrency::creates_in_one_directory_are_all_
  visible` stay green on both targets.
- **R4 — Contract, reference fs and kit.** `DirSink::add_entry`,
  `DirEntry::entry`, `CollectDir`/`ProbeDir`, `OpKind::Readdirplus`, the
  doc text of §4.2. `RefView::readdir` takes `plus` and hands
  `Some(entry)` equal to its own `lookup` (`.`/`..` name-only), so the kit
  has an oracle; `MockVfs` passes `plus` through. Conformance cases in
  `crates/vfs/src/conformance/readdir.rs` (registered as in `mod.rs:674`):
  - `plus_entries_are_what_lookup_answers`: every `Some(entry)` equals
    `lookup(dir, name)` and `getattr(ino)` (attributes and kind; TTL equal
    to the lookup's), after creates, a `chmod`, a `truncate`, an open write
    session with pending bytes (size and mtime overlaid), a hard link;
  - `plus_dot_and_dotdot_are_name_only`;
  - `plus_and_plain_cookies_interleave`: alternate plain/plus pages of 7
    from every cookie: same sequence as one plain listing (K2);
  - `a_plus_entry_is_a_reference`: list with plus, `unlink` the name
    through another client, `getattr(ino)` answers `nlink 0` until the
    listing client's `forget`, then `NotFound`/`Stale` (the kit's `Client`
    treats plus entries as references, `client.rs:180-189`);
  - `plus_in_a_removed_directory_is_not_found`;
  - `plus_of_a_subtree_view_stays_confined` (`confinement.rs` style);
  - `a_cookie_resumes_from_a_fresh_client`: a second client continues a
    listing from any cookie the first was handed (both targets resolve it;
    the engine answers `Stale` only for a name removed in between, which
    the case accepts in place of a resume — a skip is a failure).
  The kit runs against `RefTarget` and the engine target
  (`ENGINE_TARGET.md`); both must pass.
- **R5 — Engine plus.** `View::readdir` (`ops.rs:1518`): §4.5 for real,
  scratch and synthetic directories, the zero-TTL rule, the batch session
  wait, the watch name. Tests in `crates/engine/src/view/vfs_tests.rs`
  (the feature is new, so "fail-before" means failing against the naive
  implementation each test guards against, reached through a test-only
  switch or by construction): attributes equal `getattr` with a write
  session on this view (pending size); a flush that retires the session
  between the page's listing and its attribute reads (a new `#[cfg(test)]`
  thread-local hook between the two, in the style of `copy::AFTER_STEP` /
  `vfs_tests::BetweenSteps`, `vfs_tests.rs:2001-2015`) — fails if the
  dentry copy is read outside the shard (e.g. taken from the listing's
  scan); `--cto strict` (name-only) and a lone strict sequencer (entries);
  a snapshot mirror and a subtree view (`note_reached`); a two-view test
  where a rename on view B between view A's listing batch and its
  attribute read makes A's entry name-only. Cross-node (in-process
  multi-node pattern): node B's `chmod` acknowledged to B's client, then
  node A's plus page after A applied it shows the new mode; a queued replay
  touching only the child holds the plus page as it holds `getattr` (fails
  with a `Dir(dir)`-only wait).
- **R6 — Adapter.** Knob, `init`, `readdirplus`, `DirPlusReply` with its
  success-only accounting, the `KernelEntries` records, resume behaviour
  (§4.10). Wire tests: `init_negotiates_readdirplus_per_knob`
  (auto/always/off and a kernel that does not offer it);
  `readdirplus_encodes_entries_and_name_only_records` (parse
  `fuse_direntplus`: node ids, `d_ino`, types, TTLs, `.`/`..` with node id
  0 and their real `d_ino`, the buffer-full stop); `readdirplus_counts_
  only_on_success` (a scripted page that fails after entries were offered
  takes no reference and records no name; a successful one takes exactly
  one per entry with a node id, before the reply bytes reach the kernel
  socket); `entry_invalidations_cover_names_handed_by_readdirplus`
  (extends `wire.rs:487`'s case: an invalidation the view queues while the
  plus request is in flight and releases after `done` is written for a
  listed name; one for a name only ever listed name-only, and one for a
  foreign name of the same directory, is not); a resumed session serves
  `READDIRPLUS` with the knob `off` (name-only).
- **R7 — Observability and docs.** §4.11 (including the per-view
  `unlinked_refs` count); `docs/reference/configuration.md`
  (`CONSTELLATION_FUSE_READDIRPLUS`, `CONSTELLATION_READDIR_HINT_BYTES`,
  `CONSTELLATION_READDIR_COOKIE_BITS`, the `ESTALE` case);
  `docs/how-to-guides/development/TESTING.md` (scenarios); the proposed
  DESIGN §12 text for the coordinator (CONVENTIONS rule 5: not edited
  here): move `READDIRPLUS` to "Built", replace "resumes by position
  today" and the "stable readdir cursors" bullet with the keyed-cookie,
  name-order design and its one inexact case.
- **R8 — System tests and the after-benchmark.** §7's harness scenarios and
  xcheckfs runs, §8's after-numbers in `bench/readdirplus/RESULTS.md`
  against R0, the acceptance check. If plus acceptance fails on any host
  file system or kernel, the default becomes `off` and the report says
  why; if listing acceptance fails, the report says so and R3 is not
  shipped as is (never ship a regression, never loosen the bar).

Order: R0, R1, R2 (verify), R3, R4, R5, R6, R7, R8. R3 lands before any plus
entry is served (plus pages need O(page) listings); R2 before R6.

## 7. System-level tests

Harness scenarios (`crates/harness/src/scenarios.rs`, CONVENTIONS checklist;
each 3× green, seeded):

- `readdirplus-attrs-match` — two nodes, `auto`. Node B runs a seeded
  workload (`Workload`) of chmod/chown/utimes/truncate/append/rename/unlink/
  link in a 2,000-entry directory; node A loops `ls -l --time-style=+%s.%N`
  and `find -ls`. After each step settles (`eventually()`, A's applied
  position past B's ack), A's listing equals the `Model` oracle; A's
  per-mount `readdirplus_entries` grew (plus was really used).
- `readdirplus-local-appender` — one node, `--write-mode back` and
  `through`: four `O_APPEND` writers keep files open and append numbered
  records while two listers loop `ls -l` and one process `chmod`s the
  files; every file ends with every record in order (a stale plus size
  would make the kernel position appends over acknowledged bytes).
- `readdirplus-orphan-forget` — two phases, both deterministic.
  (1) The revalidation partial forget (§4.4): node A's kernel holds Y under
  `m` and X under `n` (A `stat`s both); wait past TTL plus slack; node B
  `mv m n`; on A open an `O_PATH` descriptor of `m`, `stat n` (its `LOOKUP`
  answers Y ≠ X: `FORGET(Y, 1)` while `m`'s dentry holds Y), then `rm n`
  on A; `fstat` of the descriptor answers `nlink 0`, never `ESTALE`, and
  `forgets_partial` rose by exactly 1. (2) Plus references: processes take
  `O_PATH` descriptors of names they found through `ls -l`, another process
  unlinks and `rmdir`s them; every `fstat` answers `nlink 0`; after the
  processes exit and the mount is unmounted, the per-view `unlinked_refs`
  count was 0 before the unmount once the kernel let go (where the harness
  may drop the dentry cache), and `gc-open-orphan-hold`'s bucket check
  finds no held data.
- `readdirplus-strict` — `--cto strict`, two nodes: all plus entries are
  name-only (`readdirplus_name_only` > 0, `readdirplus_entries` = 0),
  `cto-strict`'s visibility assertions hold under `ls -l` loops; then the
  second node leaves (lone sequencer): entries again.
- `readdirplus-handover` — `daemon --upgrade` under an `ls -lR` loop and
  `O_PATH` references: `kernel_refs` after the resume equals before minus
  the forgets read meanwhile, no `ESTALE`, orphans reaped after their
  forgets (the `session-handover-idle`/`upgrade-under-load` pattern); a
  listing paused mid-directory across the upgrade completes (cold scan),
  with no entry skipped or repeated.
- `readdir-huge-delete` — a 1M-entry directory (`--write-mode back`), `rm
  -rf` of it (gnulib `fts` reads up to 100,000 entries per round and
  deletes between rounds on the same stream), and in parallel `find -ls`
  of it from a second process: no entry is left behind, `rmdir` succeeds,
  no `ESTALE` (the hint cache keeps the deleters' cookies resolvable), and
  `readdir_cold_scans` stays 0.
- `readdir-cookie-cold` — a 200k-entry directory listed by a program that
  `telldir`s every 1,000th entry, then (after a `daemon --upgrade`, so the
  hints are gone) `seekdir`s to each saved position in random order: each
  resume continues correctly (cold scan), and one whose name was removed
  meanwhile answers `ESTALE`.
- `fuse-inval-storm` runs a second time with the lister doing `ls -l`
  (plus) instead of `getdents` only: no stall, no wedge (`kill -9` lands as
  before), and `KernelEntries` writes notifications for plus-listed names.
- Existing scenarios that list directories (`git-workflow`,
  `git-under-flock*`, `visibility-after-burst`, `subtree-confinement`,
  `snapshot-mount`, `scratch-publish`, `session-handover-idle`,
  `upgrade-under-load`, `stress-ng-fs`, `stress-ng-fs-nodes`) run with the
  default `auto` and must stay green; `stress-ng-fs` includes `dir`,
  `dirdeep`, `dirmany`, `getdent`, `link`, `rename` stressors. pjdfstest
  stays 8798/8798.

xcheckfs (an ext4 primary, a constellation mount as secondary; its `lstat`s
after each `getdents` make the kernel ask for plus under `auto`):

- `xcheckfs verify` of a 200k-file tree and a mounted session running, for
  one hour each: `git clone` + `git status`/`git checkout` loops on a large
  repository, `rsync -a` of a source tree into the mount and back,
  `find -ls`, `ls -lR`, `du -s`, `tar c | tar x` of the tree, `rm -rf` of a
  100k-entry directory, and a directory-heavy stress (`stress-ng --dir
  --dirmany --getdent --link --rename`) — once with
  `CONSTELLATION_FUSE_READDIRPLUS=off` and once with `auto`. **Zero
  findings attributable to this plan**; any finding present in both runs
  and on the R0 binary is pre-existing and reported separately. Two nodes
  for the second hour (the other node runs the same workload in a sibling
  directory), so remote changes invalidate plus-installed dentries.

## 8. Benchmarks and acceptance

`bench/readdirplus/run.sh` (R0, reused in R8), on a Linux test host and an
S3 bucket of the usual harness setup, release build:

- **Trees**: directories of 1k, 10k, 100k, 1M and 10M regular files (names
  12–40 bytes, sizes 0–64 KiB; the 1M/10M ones for listing and memory only),
  plus a 50k-file tree of 2,000 directories.
- **Listing workloads**: `ls -f` and `ls` (no stat), `find -name x` (no
  stat: `d_type`), `ls -l`, `ls -la`, `find -ls` / `find -newer f` /
  `find -size +1k` (these stat every name; `find -type f` does not, it uses
  `d_type`), `du -s`, `rsync -a --dry-run` into an empty local directory,
  `git status` on a checked-out repository, and a second `ls -l` right
  after a first (the `stat` storm with the kernel cache warm). A separate
  `xargs stat` storm is not a plus workload under `auto` (no listing
  precedes it) and is measured only as a regression check.
- **Write-path workloads** (the listing change must cost nothing): a
  create/unlink/rename storm in one 100k-entry directory and `tar x` of a
  50k-file tree, `--write-mode through` and `back`.
- **States**: kernel-cold (fresh mount, replica warm), warm (second run
  within the TTL), and replica-cold (fresh state dir, bootstrapped); one
  node and two nodes (the second idle).
- **State-dir host file systems**: ext4, xfs and btrfs (the maintainer
  rule: no regression on any of them); kernels: one with `dabb90391028`
  (large readdir buffers) and one LTS kernel without it, where available.
- **Measured**: wall time (median of 7, IQR), FUSE requests by op (the op
  metrics from `/metrics` before/after), daemon CPU (user+sys from
  `/proc/<pid>/stat`), daemon peak RSS, `kernel_refs`, `readdir_hint_bytes`,
  cold-scan time at 1M and 10M.
- **Modes compared**: R0 baseline; R8 with `off`, `auto`, `always`.

Acceptance (R8, per host file system and kernel):

1. **Plain listing**: `ls -f`, `ls` and `find -name` faster than R0 at 10k
   entries and above (no per-page full sort), and within the noise band
   (≤ 3 % or ≤ 1 IQR, whichever is larger) or faster at 1k; with plus
   `off` the same.
2. **Write path**: the create/unlink/rename storm and `tar x` within the
   noise band of R0.
3. **Request reduction**: `ls -la`/`ls -l` of the 1k and 10k directories,
   kernel-cold, `auto`: ≥ 90 % fewer FUSE requests than R0. With a 1 s TTL a
   listing that takes longer than a second pays `GETATTR`s again for its
   earliest entries when they are stat'ed after their TTL ran out; the
   100k and larger directories are therefore judged on time (4), not on the
   request count, and the request count there is reported.
4. **Time**: `ls -l`, `find -ls`, `du -s`, the second `ls -l` and `rsync
   --dry-run`, kernel-cold, faster than R0 at 1k, 10k and 100k entries
   (target ≥ 2× at 10k; any slower case blocks the default `auto`).
5. **Resources**: daemon peak RSS while listing the 1M and 10M directories
   within R0 + the hint cap + `kernel_refs` × 32 B (no per-stream growth);
   CPU per listed name for `ls -l` at most R0's.
6. **Page cost**: per-page service time at 1M entries within 2× of that at
   1k (O(page)); a cold resume at 10M is reported (one scan) and must not
   exceed the time of one R0 page at 10M.

## 9. Risks and open questions for the maintainer

Decide before the executor starts:

- **Plain-page semantics under the new listing** (§4.9): listings become
  name-ordered (today: cookie order), entries created ahead of a running
  listing appear and behind it do not, a renamed name may appear twice, and
  a cold cookie whose name is gone is `ESTALE` (the only inexact case; with
  `nfsd` re-export the error maps to a stale-handle error, not
  `BAD_COOKIE`, since a FUSE server cannot produce the latter). Accept?
- **The first upgrade across R2 is a remount** (no version bump, the
  handoff format changed); confirm, and that a listing paused across the
  first upgrade across R3 gets one `ESTALE` (old FNV cookie).
- **Cookie key source** (§4.9): derived from existing per-filesystem secret
  material (no `meta.json` change), UUID fallback for filesystems without a
  gossip secret — or a new `meta.json` field written at create time?
- **`lookup`'s session wait drift**: the M6 table (`PROGRESS.md:11216`)
  says `lookup` waits `Dentry` then `Ino(child)`; the code
  (`ops.rs:200-205`) waits `Dentry` only. Plus entries wait as `getattr`
  (§4.5); should `lookup` be brought in line (separate fix)?

Open, decided by measurement or later:

- **Default `auto` vs `always`** (§4.1): `auto` is proposed. `always` makes
  every `getdents`-only walk pay ~4× the pages.
- **Strict mode**: with `--cto strict` on a non-lone node, plus pages are
  name-only (§4.5); should the knob's default resolve to `off` there
  (saves the larger first page)? Proposed: keep `auto`, measure.
- **31-bit cookies** default off; legacy 32-bit userspace must opt in.
- **Cold-scan cost**: if `readdir_cold_scans` is material in production
  (busy `nfsd` re-export of huge directories, frequent restarts), build
  option (b) lazily for large directories (§4.9).
- **`FOPEN_CACHE_DIR`** follow-up (§4.9, K10).
- **Kernel attribute race**: plus installs attributes through the same
  `fuse_change_attributes` path as `LOOKUP`/`GETATTR` (K9), so it is
  exposed to, and fixed by, the proposed upstream kernel patch for the FUSE
  attribute race; R8's xcheckfs runs must be compared with that race's
  known findings, not counted as plus findings.
- **Over-count leak**: an entry reply the kernel never processes keeps its
  inode counted until the session ends (§4.4); bounded and never a reap.

## 10. Exit criteria

- [ ] fuser fork branch `readdirplus` (name-only entries,
      `remaining_capacity`), merged into `replicix`, rev bumped (R1).
- [ ] Kernel lookup counts present and verified (landed separately), with
      `BATCH_FORGET`, both handoff records, the clean first-upgrade refusal
      recorded (R2).
- [ ] Name-ordered O(page) listings, keyed stream-independent cookies, the
      handle-free hint cache under its byte cap, the cold scan, `ESTALE`
      for a vanished cold cookie, the collision rules, the 31-bit knob,
      with the fail-before scan-count test (R3).
- [ ] `DirSink::add_entry`, reference-fs oracle, the seven conformance cases
      green on the reference and the engine targets (R4).
- [ ] Plus entries equal `lookup`'s attributes after `getattr`'s wait
      (pending overlay, batch session waits, strict name-only, synthetic,
      scratch, confinement), each test failing against the naive variant it
      guards against (R5).
- [ ] Adapter `readdirplus`, negotiation knob, success-only accounting,
      per-name `KernelEntries` records, resumed sessions (R6).
- [ ] Status/metrics/schema (with per-view `unlinked_refs`), configuration
      and TESTING docs, proposed DESIGN text recorded (R7).
- [ ] Harness scenarios 3× green; xcheckfs runs with zero findings from this
      plan; §8 acceptance met on every host file system and kernel
      measured, or the default left `off` / R3 held back with the numbers
      reported (R8).
- [ ] Every CONVENTIONS gate green; pjdfstest 8798/8798.

## 11. PROGRESS.md

Add a section `## Plan 40 — FUSE READDIRPLUS` in the established style
(intro paragraph, "What landed" table, "Unchanged on purpose", "Gates"
with dates, kernel and host file systems), with these rows:

| Item | State | Where |
|---|---|---|
| Baseline benchmark (R0) | | `bench/readdirplus/` |
| fuser fork `readdirplus` branch (R1) | | fork `REPLICIX.md`, `Cargo.toml` rev |
| Kernel lookup counts (R2, landed separately: verified) | | the integrity fix's section; `crates/frontend-fuse/`, `crates/cli/src/handover.rs` |
| Listing pages and cookies (R3) | | `crates/engine/src/view/listing.rs`, `ops.rs` `readdir`, `Meta::readdir_after` |
| Plus contract (R4) | | `crates/vfs/src/responder.rs`, `vfs.rs`, `ctx.rs`, `mock/reffs.rs`, `conformance/readdir.rs` |
| Engine plus entries (R5) | | `crates/engine/src/view/ops.rs` `readdir` |
| Adapter readdirplus + negotiation (R6) | | `adapter.rs`, `reply.rs` `DirPlusReply`, `dentries.rs` |
| Observability + docs (R7) | | `stats.rs`, `control/src/schema.rs`, `configuration.md`, `TESTING.md` |
| Harness + xcheckfs + after-benchmark (R8) | | `crates/harness/src/scenarios.rs`, `bench/readdirplus/RESULTS.md` |

Also record under the section: DESIGN §12's "`readdir` resumes by position
today" is stale since `ed9b40b`, and its "stable readdir cursors are free /
the cursor is a key" holds only for variable-length cursors, not FUSE's
64-bit offsets; the proposed DESIGN §12 text (R7); the one-time `ESTALE` for
a listing paused across the first upgrade across R3.

## Sources

- Linux `fs/fuse/readdir.c`, `dir.c`, `inode.c`, `file.c`, `dev.c`,
  `notify.c`, `fuse_i.h`, `include/uapi/linux/fuse.h`, `fs/readdir.c`,
  `fs/dcache.c`, `fs/nfsd/vfs.c` at mainline `4056b3c9d3c9`; history:
  `0b05b18381ee` (readdirplus), `4582a4ab2a0e`/`634734b63ac3` (AUTO),
  `c6cdd51404b7`, `6c26f71759a6`, `b8bd342d50cb`, `dabb90391028`,
  `4dd6f6d3085a`, `51a8de6c50bf`, `0fa8346099b5`.
- libfuse `include/fuse_lowlevel.h` (`fuse_add_direntry_plus`: lookup count
  for every entry except `.` and `..`; the readdir offset contract; the
  notify-from-readdir deadlock warning).
- glibc `sysdeps/unix/sysv/linux/` `readdir`, `getdents` (`EOVERFLOW` for a
  narrow `d_off`), `opendir` (buffer size); BZ #12165 (`d_ino == 0`).
- `fuser` fork (github.com/replicix/fuser) at `b6af219`: `src/lib.rs`,
  `src/reply.rs`, `src/ll/reply.rs`, `src/ll/flags/init_flags.rs`,
  `src/request.rs`, `src/session.rs`, `REPLICIX.md`, `replicix/PATCHES.md`;
  upstream `cberner/fuser` master (`remaining_capacity`).
- POSIX.1-2024 `readdir`, `seekdir`; RFC 1813 §3.3.16, RFC 8881 §18.23.
- Prior art (§5): ext4, XFS, btrfs, libfs/tmpfs, bcachefs (v6.16), OpenZFS,
  CephFS, FreeBSD tmpfs, GlusterFS, mountpoint-s3, JuiceFS, go-fuse, gnulib
  `lib/fts.c` (`FTS_MAX_READDIR_ENTRIES`); LWN 544520 and 767132.
- In-repo: `bench/enginebench/RESULTS.md`, `bench/prollybench/RESULTS.md`,
  plans 28, 29, 30, 31, 38, 39, `PROGRESS.md` (plan 30 M6 key table).
