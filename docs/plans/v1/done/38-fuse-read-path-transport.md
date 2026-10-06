# Plan 38 — FUSE read-path transport: io_uring, zero-copy, passthrough

> **Status: done (Z0–Z5).** What shipped, with Constellation's own measured
> numbers per transport, is `docs/plans/v1/PROGRESS.md` "Plan 38 — close-out".
> Where this document and that section differ, PROGRESS is what the tree does:
> zero-copy (§3(d)) shipped **opt-in** (`CONSTELLATION_FUSE_URING_ZERO_COPY`
> defaults to `off`), `auto` keeps cluster-lock mounts on `/dev/fuse` (Z2c),
> and passthrough is on by default for read-only mounts only (Z3b review).

Read `docs/plans/v1/CONVENTIONS.md` first. This plan replaces the Linux
read path's fixed shape — one kernel copy through `/dev/fuse` `writev(2)`,
plus an in-process memory cache of verified chunk bytes (plan 31 C7b's
follow-up) — with a **runtime-detected transport ladder**: FUSE-over-io_uring
(kernel 6.14+), its zero-copy extension (kernel 7.3+), and FUSE passthrough
for single-chunk read-only opens (kernel 6.9+), falling back to today's
`/dev/fuse` `writev` path wherever the kernel, capabilities, or platform
don't offer the rest. Linux only; FreeBSD and every other Constellation
platform keep exactly today's path — the portable fallback is a hard
requirement, not a temporary one.

The decisions below were made by a benchmark (`bench/fuse-read-path`, a
small libfuse lowlevel filesystem serving chunk-file-shaped data, driven by
fio, run on 2026-09-30) and are recorded with their evidence in
`bench/fuse-read-path/RESULTS.md`; the numbers live in
`bench/fuse-read-path/results/summary-tables.md` (medians of 3 repeats,
quoted throughout below) and `results/full.jsonl` (every cell), and
`bench/fuse-read-path/README.md` explains the modes and workloads cited
throughout. The kernel, libfuse and fuser facts this plan rests on are
listed with their sources in the Sources table at the end.

**VERIFIED** means checked this session against `main`'s source at
`3a8b335` (`git show`/`git grep`, or a direct read of the working tree,
read-only — `main` is plan 31 C0–C8 implemented plus the chunk memory
cache, merged from `claude/sleepy-davinci-sqbw3a` by `3a8b335`, and carries
every frontend-fuse/engine/vfs crate this plan touches:
`crates/{types,platform,vfs,engine,frontend-fuse,control}`, `vendor/fuser`
with its patch series) or against upstream primary source (kernel.org,
`fs/fuse/dev.c`, the 7.3 pull request, libfuse source, the vendored
`fuser` crate). **REPORTED** means a secondary source
(the Skory/fuser fork's PRs and design gist, its own README-stated numbers)
not independently re-derived in this session — milestone Z1 (§7) re-verifies
every REPORTED claim this plan's first vendoring step depends on before
anything is built on it, exactly as plan 37's K0 re-checks its own REPORTED
fuser facts (`docs/plans/v1/done/37-kubernetes-csi.md` §1, VERIFIED/REPORTED
paragraph).

## Dependencies

- **Plan 31**, specifically:
  - **C4**'s `constellation-vfs` contract (`Vfs::read`, `ReadData =
    SmallVec<[Bytes; 4]>`, VERIFIED `crates/vfs/src/types.rs:343`) and the
    FUSE adapter built on it (`crates/frontend-fuse/src/adapter.rs`,
    `crates/frontend-fuse/src/reply.rs`) — this plan changes how a `Vfs::read`
    completion reaches the kernel, not the trait itself.
  - **C4b**, the FUSE session handover machinery (§6.11 of plan 31):
    `MountSource::{Path, PreopenedFd}`, `FuseSession::detach`/`resume`,
    the vendored `fuser` patch `from_fd_resumed`/`NegotiatedInit`
    (VERIFIED `vendor/fuser/CONSTELLATION-PATCH.md`,
    `crates/frontend-fuse/src/session.rs`). This plan's ring transport must
    compose with that handover, not bypass it — §3(e) below is exactly that
    seam. It was the one place this plan found an open, UNVERIFIED question
    rather than a settled design. Milestone Z0a answered it on 2026-09-30
    (`bench/fuse-uring-handover/RESULTS.md`): a ring session **cannot** be
    handed over losslessly, so handover-capable sessions stay on `/dev/fuse`.
  - **C7b**'s cold-read deferral (`crates/engine/src/view/io.rs`'s
    `cold_probe`, VERIFIED, module doc + `do_read_detached` at
    `crates/engine/src/view/io.rs:191`) and its "REJECT splice" finding
    (§2 below reconciles the two: C7b's rejection was about *userspace*
    buffer-pool allocation on the `Bytes` path, not about the kernel copy,
    RAM triplication or CPU this plan measured — see "Reconciling with
    C7b" in §2).
- **The merged chunk memory cache** (`crates/fs-core/src/memcache.rs`,
  `crates/fs-core/src/cache.rs`'s `get_shared`/`load_shared`, landed as the
  "Chunk memory cache" follow-up to C7b, VERIFIED against
  `docs/plans/v1/PROGRESS.md`'s "## Chunk memory cache" section on
  `main`). This plan keeps that tier — it is the source of verified bytes
  for every path that does not read the chunk file directly (§3(d)) — and
  extends its budget accounting (§4), not its design.

**Coordinates with plan 37** (`docs/plans/v1/done/37-kubernetes-csi.md`): the
CSI engine pod's session handover (plan 37's K0/K5) and this plan's ring
transport both touch `FuseSession`/`fuser::Session`; §3(e)'s fallback policy
(`transport: Auto | DevFuse` on a per-mount basis) is written so a
handover-capable session (plan 37's pods) stays out of the ring transport
without this plan and plan 37 fighting over the same knob. Z0a settled that
it must stay out: a ring session cannot be handed over losslessly, so plan
37's pods are `DevFuse` for good (§3(e)). Plan 37
also runs inside a container, where seccomp profiles commonly block
`io_uring` entirely (§8, Risks) — this plan's runtime detection is what
keeps that combination safe rather than a silent crash.

**Coordinates with plan 32** (`docs/plans/v1/done/32-snapshot-policies-and-space.md`,
space accounting): §3(c)'s passthrough eligibility rule pins a chunk file
open for as long as the kernel holds a backing fd on it (§3(c)'s "pin
while open" decision). A pinned-while-open chunk cannot be evicted by
`prune_to`/`plan_eviction`, which changes what "space currently used by the
disk cache" means while passthrough opens exist; this plan's engine-side
hook (§3(c)) is the fact plan 32's own session needs, not an edit to
plan 32's document.

**Independent of plans 34, 35, 36** (macOS/Windows/Android ports): none of
those frontends mount through `fuser`/`/dev/fuse` (plan 34 is NFSv4.1, 35 is
WinFsp, 36 is SAF/proxy-fd), so nothing here touches them, and `ReadData`
— the one type they all consume from `constellation-vfs` — is unchanged by
this plan (§3 changes only what the FUSE adapter and `vendor/fuser` do with
a `ReadData` once it reaches the Linux boundary).

Linux only. FreeBSD keeps the current `/dev/fuse` `writev` path unchanged
(fuser supports FreeBSD; the ring transport, zero-copy and passthrough are
all Linux kernel features with no FreeBSD analogue) — this is not a gap to
close later, it is the permanent fallback every other platform and every
downgrade path in §2 lands on.

## 1. Why

### The read path today, in copies and RAM

Two read paths matter to this plan's baseline, one of them history. Before
`3a8b335` merged plan 31 into `main`, `main`'s only FUSE implementation was
`crates/cli`'s stock-`fuser`-0.18 path below; that code
(`crates/cli/src/fusefs_ops.rs`, `fusefs.rs`) no longer exists on `main` —
it is kept below purely as history, with its original line numbers
unchanged, because the bench's `copy` mode models exactly its shape. `main`
today runs the engine/vfs/frontend-fuse path plan 31 built, plus the chunk
memory cache. Both shapes were measured against this plan's bench to give
its verdicts their baseline:

- **Pre-merge `main` (`crates/cli`, stock `fuser` 0.18 from crates.io —
  gone from the tree as of `3a8b335`)**: `Filesystem::read`
  (`crates/cli/src/fusefs_ops.rs:764`) → `do_read_detached` (`:1504-1590`) →
  `fetch_chunk_for_inode` (`fusefs.rs:2383`) → `DiskCache::get`
  (`crates/fs-core/src/cache.rs:362`, the pre-memcache method): `fs::read`s
  the **whole** chunk file and BLAKE3-verifies it on **every** kernel read,
  regardless of how small the kernel's request is. Copies on a cache hit:
  (1) disk → `Vec` (`fs::read`, whole chunk), (2) chunk → reply buffer
  (`out.extend_from_slice`, `fusefs_ops.rs:1579`), (3) the kernel's own copy
  on `writev(2)`. Two userspace copies plus one kernel copy, and a
  whole-chunk re-hash per request — this is the path this plan's bench
  calls `copy` mode, the baseline every other mode is measured against.
- **`main` today (plan 31 implemented + the chunk memcache, merged by
  `3a8b335`)**: `View::do_read_detached` (`crates/engine/src/view/io.rs:191`)
  builds a `ReadData` (`SmallVec<[Bytes; 4]>`, `crates/vfs/src/types.rs:343`)
  from `DiskCache::get_shared` (`crates/fs-core/src/cache.rs:475`) slices —
  verified once, on admission, and shared thereafter as `Bytes`, zero-copy
  in userspace. `crates/frontend-fuse/src/adapter.rs:637`'s `read` hands the
  engine's `ReadData` to `ReadReply` (`crates/frontend-fuse/src/reply.rs:111-119`),
  whose `done` calls `self.0.data(&data.contiguous())` — a gather copy only
  when the read spans more than one `Bytes` segment (almost never: `ReadData`'s
  own doc calls that "almost always one segment"), then `fuser`'s
  `ReplyData::data` → `ChannelSender::send` → `nix::sys::uio::writev`
  (VERIFIED `vendor/fuser/src/channel.rs:135`, `vendor/fuser/src/reply.rs:208`).
  So `main` already has **one kernel copy** on a hot read — the same
  ceiling `copy` mode has — but pays for it with RAM the pre-memcache path
  didn't: a hot chunk's bytes now live **three times** at once — the chunk
  file's own page cache (until the disk cache evicts it), the memcache's
  anonymous-RSS verified copy, and the kernel's FUSE inode page cache (the
  buffered-read case; direct I/O skips that third copy). The memcache's own
  measurements (`docs/plans/v1/PROGRESS.md`, "Chunk memory cache") show why
  it exists: a disk-cache-resident 256 MiB sequential read, kernel page
  cache dropped, went from 72.6–76.6 MiB/s (whole-chunk re-read + re-hash
  per kernel request) to 381–407 MiB/s once verified bytes are shared — but
  that fix moved the cost from disk I/O to RSS (128 MiB desktop default,
  `CONSTELLATION_CHUNK_MEMCACHE_BYTES`), not away from the kernel copy.

### What the benchmark showed

`bench/fuse-read-path` (README, "Modes" table) isolates exactly these
costs — `copy` (the pre-merge-`main`/`crates/cli`-style baseline, without
its whole-chunk re-read: one `pread` of the requested range, one
`writev`), `memcache` (`main`'s shape: resident verified bytes, one
`writev`), and the
Linux-specific reply paths — on a real kernel 7.3.0-rc4 host (c8a.4xlarge,
16 vCPU, 30 GiB, btrfs, 4 MiB chunks, fio). The headline numbers (warm =
chunk files resident in the page cache, i.e. what a hot Constellation disk
cache looks like):

- **`splice(2)`/`vmsplice(2)` buy nothing and actively hurt.** seq1 buffered
  4,808 vs `copy`'s 6,704 MiB/s (−28% with `SPLICE_F_MOVE`); rand128k-aio
  2,829 vs 4,321 (−35%); CPU/GiB unchanged (0.13 vs 0.14). `SPLICE_F_MOVE`
  steals the chunk file's page-cache pages into the FUSE inode's page cache
  (backing-file residency fell from 100% to 0–75% in these cells) — for
  Constellation this would evict its own disk cache on every large read.
  `vmsplice`/`mmap` never let the kernel steal a page-cache page at all
  (`SPLICE_F_GIFT` degrades to a copy — `pipe_buf_try_steal` needs a
  refcount of 1, which a page-cache page never has), so they are
  indistinguishable from `copy` everywhere except O_DIRECT sequential reads
  (`splice` 10,266 vs `copy` 6,954 MiB/s, +48%), which the io_uring
  transport matches or betters anyway (10,503–18,534 MiB/s in the same
  cell). libfuse also never splices a reply under two pages
  (README "Caveats"): every 4 KiB workload's `splice` numbers measure
  nothing but the `writev` fallback.
- **FUSE-over-io_uring removes the syscalls and roughly a third to a fifth
  of the daemon's CPU per byte**, at equal or better throughput on every
  workload except single-stream buffered sequential: rand4k-8j-dio (the
  cell closest to Constellation's hot random-4K-under-load case) goes from
  464.5 kIOPS at 3.71 daemon CPU s/GiB (`copy`) to 1,510.1 kIOPS at 0.78
  (`uring`) — **3.25× the IOPS at 4.7× less CPU per byte served**, p99
  latency 8.8 µs vs 36.1 µs. rand4k-1j-dio: 121.8 → 280.5 kIOPS (2.3×),
  p99 10.1 → 3.8 µs. The one regression is single-reader buffered
  sequential — seq1 5,007 vs 6,704 MiB/s (−25%), seq8 17,770 vs 22,756
  (−22%) — because the kernel queues a CPU's requests to that CPU's own
  ring, so one sequential reader's readahead is served by one ring thread
  where the `/dev/fuse` transport spreads it over the whole worker pool
  (README "Caveats"); CPU/GiB still drops (seq1 0.08 vs 0.14, −43%). RSS
  grows by the ring payload buffers: ~130 MiB measured on this 16-CPU host
  at the bench's defaults (§4 works out what that means at Constellation's
  own worker-count, not the kernel's per-CPU count).
- **io_uring zero-copy (7.3+) fixes that regression and roughly triples the
  transport's own win.** seq1 6,360 MiB/s (= `copy` −5%) at 0.04 daemon CPU
  s/GiB — **3.1× less CPU than `copy` (0.139), 2.7× less than `memcache`
  (0.121)**; seq1-dio
  18,534 MiB/s (2.7× `copy`, 1.3× `memcache`) at 0.05; smallfiles 2,844
  MiB/s (2× `copy`'s 1,430); rand128k-aio 4,971 (+15%) at 0.07. It needs
  `CAP_SYS_ADMIN`, kernel-registered buffer pools, and — because the kernel
  reads straight from the backing file into the client's pages — the daemon
  reading the **chunk file directly**, bypassing the memcache on that one
  path (§3(d) and the trust-model note below).
- **FUSE passthrough is the ceiling, and it is not close.** Single-chunk
  read-only opens on kernel 6.9+: seq1 30,797 MiB/s (4.6× `copy`), seq8
  85,333 (3.7×), rand128k-aio 24,381 (5.6×), smallfiles 6,564 (4.6×), **0
  daemon CPU, 5 MiB RSS** — the daemon never sees these reads once the file
  is open (README "Modes": "Reads never reach the daemon: the ceiling").
  `fuser` 0.18 already implements the kernel ioctls
  (`ReplyOpen::opened_passthrough`, VERIFIED `vendor/fuser/src/reply.rs:397`;
  `open_backing`, `:370`; `BackingId`, `vendor/fuser/src/passthrough.rs`) —
  this plan is new eligibility and wiring logic on top of code that is
  already vendored, not a new vendoring problem.

Priority order the memo settles on for Constellation's actual shape
(S3-backed, disk chunk cache, portable): **io_uring transport > passthrough
(for the files it covers) > io_uring zero-copy (7.3+) ≫ mmap > splice ≈
vmsplice (rejected)**. §2 turns this into an adopt/reject table with every
option's evidence and §7 turns it into milestones in that order.

## 2. Decisions

### 2.1 Verdicts

| # | Option | Verdict | Evidence (warm unless noted; `bench/fuse-read-path/results/summary-tables.md`) |
|---|---|---|---|
| 1 | `splice(2)` fd→pipe→`/dev/fuse` | **REJECT** | seq1 4,808 vs `copy` 6,704 MiB/s (−28%) with `SPLICE_F_MOVE` (= `copy` without it); seq8 −10%; rand128k-aio 2,829 vs 4,321 (−35%); CPU/GiB unchanged (0.13 vs 0.14). libfuse never splices replies < 8 KiB (every 4 KiB read falls back to `writev`: 3.6M fallbacks in a `rand4k-1j-dio` cell, 13.9M in a `rand4k-8j-dio` cell — one per read). `SPLICE_F_MOVE` steals the chunk file's page-cache pages into the FUSE inode (backing residency 100→75% seq, 100→0% smallfiles): a MOVE read would evict Constellation's own disk cache. Cold: libfuse's non-blocking fd→pipe splice returns short on uncached pages and falls back to copy (4.8k–25.6k fallbacks per cold `seq8` cell). Only measured win: O_DIRECT seq1 10,266 vs 6,954 (+48%), which io_uring matches/betters anyway (§2, row 5). |
| 2 | `mmap` + `vmsplice(2)` | **REJECT** | Indistinguishable from `copy` in every cell (seq1 6,639 vs 6,704; seq8 22,568 vs 22,756; rand4k-1j-dio 124k vs 122k IOPS). `fs/fuse/dev.c` always copies user pages regardless of flags ("Can't control lifetime of pipe buffers" — VERIFIED against `fs/fuse/dev.c` on `torvalds/master`); `SPLICE_F_GIFT` is accepted but nothing is stolen (residency stays 100%). Plus file-backed RSS equals the whole working set (1,041–8,209 MiB in these cells, vs `copy`'s 8–15 MiB). |
| 3 | `mmap` alone (reply from a mapping, the portable zero-copy option) | **not adopted** | ≈ `copy` on sequential; rand4k-1j-dio 191k vs 122k IOPS (+57%, but a fault-around artifact of `read_ahead_kb`, not a design win — README "Caveats"); smallfiles +14%; loses to `memcache` on small files (1,631 vs 3,200 MiB/s). Serves bytes the kernel re-reads from disk **unverified** after any eviction of the underlying page — the memcache's admit-once-verify model, kept below, does not have that gap. |
| 4 | `memcache` (`main`'s in-memory verified tier, already merged) | **keep, unchanged design** | seq1-dio 14,372 vs 6,954 (2.1×), smallfiles 3,200 vs 1,430 (2.2×), rand4k-1j-dio 175k vs 122k IOPS; but ≈ `copy` on buffered sequential (6,759 vs 6,704), rand128k-aio **−32%** (2,942, worse than `copy`'s 4,321; the bench did not isolate why — the cell was pre-loaded, so it is not miss cost — and Z0b's fio gate re-measures this shape on Constellation's own memcache before anything is concluded from it); cold sequential costs **2–3× the daemon CPU** of plain `copy` (0.64 vs 0.24 s/GiB: a cold miss loads and hashes the whole chunk before serving any of it). RSS is the resident working set (capped 128 MiB desktop / 16 MiB mobile in Constellation, 8 GiB in the bench's own default). |
| 5 | FUSE-over-io_uring transport (kernel ≥ 6.14) | **ADOPT — the primary result** | rand4k-8j-dio 1,510.1k vs 464.5k IOPS (3.25×) at 0.78 vs 3.71 CPU s/GiB (4.7× less), p99 8.8 vs 36.1 µs; rand4k-1j-dio 280.5k vs 121.8k (2.3×), p99 3.8 vs 10.1 µs; buffered rand4k-8j same IOPS (kernel page cache serves it either way) but daemon CPU 0.63 vs 3.14 s/GiB (5× less); smallfiles 2,170 vs 1,430 (+52%); seq1-dio 10,503 vs 6,954 (+51%); rand128k-aio equal (4,321 vs 4,321). **Regression**: single-stream buffered sequential seq1 5,007 vs 6,704 (−25%), seq8 17,770 vs 22,756 (−22%) — one CPU's ring serves that CPU's readahead, where `/dev/fuse` spreads it over the worker pool; CPU/GiB still −43% (seq1 0.08 vs 0.14) and −24% (seq8 0.19 vs 0.25). Cold: same throughput as `copy`, CPU/GiB −29% (seq1 cold: 0.17 vs 0.24). RSS +~130 MiB of ring payload buffers on this 16-CPU bench host (queues × depth × max_write); Constellation's existing `/dev/fuse` path already spends 16 MiB per worker (`crate::threads::FUSE_BUFFER_BYTES`, VERIFIED `crates/frontend-fuse/src/threads.rs:10`) — §4 works out the comparison at Constellation's worker count, not the kernel's per-CPU count the bench measured at. `uring-bufpool` (7.3's kernel-managed pool) is within noise of plain `uring` everywhere measured. |
| 6 | io_uring zero-copy (kernel ≥ 7.3, `READ_FIXED` from the chunk file into the request's registered pages) | **ADOPT as the second step, behind the transport** | seq1 6,360 MiB/s (= `copy` −5%) at 0.044 CPU s/GiB (3.1× less than `copy`'s 0.139, 2.7× less than `memcache`'s 0.121); seq8 23,883 (+5%) at 0.11 (2.2× less); seq1-dio 18,534 (2.7× `copy`, 1.3× `memcache`) at 0.05; rand128k-aio 4,971 (+15%) at 0.07; smallfiles 2,844 (2× `copy`); rand4k-dio ≈ the plain transport (0-copy is already CPU-bound on request overhead at 4 KiB, not on the copy). Cold CPU/GiB halves versus `copy` (0.12 vs 0.24). Fixes the transport's buffered-sequential regression (seq1 6,360 vs the plain transport's 5,007). Needs `CAP_SYS_ADMIN`, kernel buffer pools, and the daemon reading chunk files **directly** — no memcache copy on that path (§3(d), trust-model note below). Userspace side is a draft: `joannekoong/libfuse`'s `zero_copy_v7` branch, **REPORTED** unmerged as of this research (3 bugs the bench itself found and documents: queue-depth zeroed by CLI option order, an `invalid commit_id=0` abort at unmount, the zero-copy flag not handed back on `READ` — README "Caveats"); the Skory/fuser fork explicitly defers zero-copy. |
| 7 | FUSE passthrough (kernel ≥ 6.9, whole backing file per open) | **ADOPT for single-chunk read-only opens** | seq1 30,797 (4.6×), seq8 85,333 (3.7×), rand128k-aio 24,381 (5.6×), smallfiles 6,564 (4.6×), **0 daemon CPU, 5 MiB RSS**. O_DIRECT reads on a passthrough fd go to the physical disk even when warm (`disk_read_mib` shows it in the raw results) — correct O_DIRECT semantics, but a behaviour change from today's path, which always serves O_DIRECT from the verified cache. `fuser` 0.18 already implements it (`ReplyOpen::opened_passthrough`, `KernelConfig::max_stack_depth`, VERIFIED `vendor/fuser/src/lib.rs:223-268`, `vendor/fuser/src/session.rs:185,771-772`). Eligible only when a file's data is exactly one cached chunk file (file size ≤ chunk size, i.e. every single-chunk file — the modal case for source trees and small config/data files); needs `CAP_SYS_ADMIN`; the daemon never sees these reads, so atime bump, the scan-ahead offset-0 trigger, and read metrics must move to `open()` (§3(c)). |

### Reconciling with C7b's "REJECT splice"

Plan 31 C7b's own candidate (b) (`docs/plans/v1/PROGRESS.md`, "Mountpoint-derived
candidates", row (b)) already says "REJECT `splice`" — this plan does not
reopen that verdict, it answers a different question with it. C7b's rejection
was about **userspace allocation**: "zero-copy is held via `Bytes`", i.e.
whether Constellation needed a `splice`-based buffer pool to avoid per-op
heap allocation on the read path, and the answer there was no — `ReadData`'s
`Bytes` segments already give zero-copy in userspace without `splice`. This
plan's row 1 measures something C7b never benchmarked: the **kernel-side**
copy `splice` would additionally avoid (it doesn't — `fs/fuse/dev.c` copies a
splice-replied 4 KiB request exactly as `writev` would, and libfuse falls
back to `writev` under 8 KiB anyway), the **RAM triplication** a MOVE splice
would cause (it evicts Constellation's own disk cache to feed the kernel's),
and the **CPU** it costs relative to what it buys (nothing, on every
workload but O_DIRECT sequential, which io_uring already beats). Both
verdicts are REJECT; they are reject-for-different-reasons, and this
document is the one that measured the reasons C7b's was written without.

### 2.2 Priority order

**io_uring transport > passthrough (for single-chunk files) > io_uring
zero-copy (7.3+) ≫ mmap > splice ≈ vmsplice (rejected)**, matching §1's
summary. The portable fallback — unconditionally, on every platform and
every downgrade — stays exactly today's path: `ReadData` (`Bytes`) → `fuser`
`writev` on `/dev/fuse` (VERIFIED `vendor/fuser/src/channel.rs:135`,
`vendor/fuser/src/reply.rs:208`; FreeBSD too — `fuser` supports it, none of
the Linux-specific kernel features this plan adds do). NFS (plan 34) and
WinFsp (plan 35) frontends are untouched: neither mounts through `fuser`.

### 2.3 Trust model: `--cache-verify {admit, always}`

**Where the hash is computed today.** A chunk is hashed **while it streams
in from the network**: `get_chunk_to_writer` feeds every decoded piece
through a `StreamingDecoder` wrapping a `blake3::Hasher` on its way into
the cache-owned spill file, and refuses the chunk on `HashMismatch` before
`commit_spill` ever sees it (VERIFIED `crates/store-s3/src/store.rs:764-780`;
the E2E variant spills the ciphertext first to keep it out of RAM, then
decrypts, decodes and hashes in one pass into the plaintext spill,
`:840-845`; peer fetches verify the same way, `crates/engine/src/coop.rs:1086`).
Hashing in flight costs nothing measurable — BLAKE3 runs at several GB/s
per core against a network stream delivering tens of MB/s — and no
zero-copy mechanism in this plan interferes with it: the decode is a
userspace pass in any case. That is the verification this plan means by
"on admission", and it stays exactly as it is.

What is **redundant** today is the second hash on the disk side.
`commit_spill` (VERIFIED `crates/fs-core/src/cache.rs:614-657`) records the
disk entry but does not admit the bytes to the memory tier, so the first
`get_shared` of a just-fetched chunk goes through `load_shared`
(`:528-544`) → `get_disk` (`:547-585`), which reads the whole file back and
hashes it **again** — and `get_disk` re-hashes on *every* disk read, not
just the first. A cold sequential read therefore hashes every chunk twice
and reads it from disk once more than necessary; that is the cost the bench
shows as `memcache`'s cold sequential CPU (0.64 vs `copy`'s 0.24 s/GiB,
§2.1 row 4). Two changes, both small, remove it under the `admit` model:

1. **Admit fetched bytes to the memcache directly from the fetch.**
   `fetch_chunk_for_inode` already holds the verified `Vec` after
   `commit_spill` (VERIFIED `crates/engine/src/view/flush.rs:144-171`); it
   becomes the `Bytes` the read is answered from *and* is inserted into the
   memory tier under the state lock exactly as `load_shared` does today
   (`memory.insert`, `cache.rs:537`, same "only if the disk entry still
   exists" check). No extra I/O, no extra hash; the first reads of a cold
   chunk hit memory instead of re-reading the file.
2. **A `verified: bool` on the disk cache's accounting `Entry`**
   (`struct Entry { size, state, atime }`, VERIFIED `cache.rs:41-46`, held in
   `State.entries`, in RAM only — rebuilt from a directory scan at every
   start). Set at the `commit_spill` creation site (`cache.rs:647`: this
   process hashed these bytes), left clear at the startup-scan creation
   site (`cache.rs:370`: files found on disk that nobody in this process has
   verified). Under `--cache-verify admit`, `get_disk` skips the hash for a
   verified entry and still hashes an unverified one (then marks it
   verified); under `always` it hashes every disk read as today. Memory
   cost: none — the `bool` fits in `Entry`'s existing alignment padding
   next to the one-byte `ChunkState` (24 bytes before and after); the
   worst case, had it not fit, would be 8 bytes per cached chunk.

With those two in place, every path — memcache, zero-copy, passthrough,
the plain `/dev/fuse` fallback — trusts a chunk file exactly once it has
been hashed by this process, and re-hashes exactly the files a restart
found on disk. Zero-copy (§2.1 row 6) and passthrough (§2.1 row 7) — and
`splice`, had it won — let the **kernel** serve the chunk file's bytes
directly, without the daemon re-verifying them on that particular read.
After admission, a bit flip on the **local disk** —
hardware fault, a `btrfs`/`ZFS` scrub miss underneath, or (outside this
plan's threat model) an attacker with write access to the cache directory —
would reach a reader of a zero-copy or passthrough-opened file without the
daemon's own hash catching it on that read. That is the trust model every
local filesystem already has for its own page cache (and exactly what
`btrfs`/`ZFS` checksumming underneath is for), not a new one Constellation
invents — but it is a real change from "the daemon re-verifies every byte
on every read," and this plan makes it an explicit, operator-controlled
decision rather than an implicit side effect of enabling a faster path.

- **`--cache-verify admit`** (default): verify once, in flight, when the
  chunk is fetched (unchanged), plus once per file a restart found on disk;
  `get_disk` skips the re-hash for entries this process verified (change 2
  above). Zero-copy and passthrough are eligible; the memcache stays the
  source of verified bytes for the non-zero-copy/non-passthrough paths.
- **`--cache-verify always`**: today's behaviour, made explicit and
  guaranteed — every disk read re-verifies (`get_disk` ignores the
  `verified` bit). Disables zero-copy (§2.1 row 6) and passthrough (§2.1
  row 7) outright (both bypass the memcache/daemon by construction, so
  there is no "verify always but use zero-copy" mode to build); the
  io_uring transport (§2.1 row 5) and the plain `/dev/fuse` fallback are
  unaffected, since both still route every read through
  `fetch_chunk_for_inode`/`get_shared`. Change 1 above (direct admission
  of fetched bytes) applies in both modes: the bytes it admits were hashed
  in flight.
- Surfaced as a per-engine `EngineConfig`/CLI flag (`--cache-verify`), an
  env override (`CONSTELLATION_CACHE_VERIFY`, matching the `CONSTELLATION_*`
  convention, CONVENTIONS.md "Code style"), and echoed in `node.status`
  (§5) so an operator or the CSI driver (plan 37) can see which mode a
  running engine is in without guessing from which transport negotiated.

### 2.4 The fallback ladder

Every step below is **runtime-detected**, never a build-time choice — the
same binary runs identically on a 6.9 kernel, a 6.14 kernel, a 7.3 kernel,
or FreeBSD, taking whatever the running kernel and the process's
capabilities actually offer:

1. **io_uring zero-copy** — kernel ≥ 7.3 (`FUSE_HAS_IO_URING_BUFPOOL` init
   flag offered, REPORTED from the 7.3 pull request; re-verified against
   the running kernel's negotiated `InitFlags` at mount time, not assumed
   from `uname`) **and** `CAP_SYS_ADMIN` **and** the `io-uring` cargo
   feature (§3(a)) built in **and** the request qualifies (§3(d): a read
   that lies within one resident chunk; chunk-spanning reads take the
   memcache path on the same session).
2. **io_uring transport (no zero-copy)** — kernel ≥ 6.14
   (`CONFIG_FUSE_IO_URING` + `fuse.enable_uring=Y` REPORTED negotiated,
   re-verified at mount; VERIFIED against the 6.14 kernel doc
   `docs.kernel.org/6.14/filesystems/fuse-io-uring.html`) **and** the
   `io-uring` feature built in **and** the session is not one this plan's
   handover policy (§3(e)) routes to `DevFuse`.
3. **`/dev/fuse` `writev`** — always available, the terminal fallback for
   every kernel below 6.14, every non-Linux platform, every seccomp profile
   that blocks `io_uring` (plan 37's pods, §8), and every session the
   handover policy pins to it.

**Passthrough** (§2.1 row 7) is independent of this ladder — it needs kernel
≥ 6.9 and `CAP_SYS_ADMIN` but nothing about the transport the *rest* of the
mount's reads use, so a mount can run `/dev/fuse` writev for its general
traffic and still passthrough-open the single-chunk files that qualify.

Every downgrade — a kernel too old, a missing capability, a refused ring
registration mid-INIT, a seccomp `EPERM` on `io_uring_setup` — is **logged
once** (not per-request: a busy mount would otherwise spam the log on every
read that falls back) and reflected in `node.status`'s transport field
(§5), so "the fast path silently never engaged" (§8, Risks: `CAP_SYS_ADMIN`
not available) is observable rather than a support mystery.

## 3. Design

### 3(a) Vendored `fuser`: a second patch series behind a cargo feature

`vendor/fuser` already carries one named patch series,
`patches/0001-constellation-session-handover.patch` (VERIFIED
`vendor/fuser/CONSTELLATION-PATCH.md`: `negotiated-init`, `from-fd-resumed`,
`detach`, applied via `tools/vendor-fuser.sh` against the pristine
crates.io 0.18.0 source, hunks marked `CONSTELLATION PATCH (<name>)`). This
plan adds a second series, `patches/0002-io-uring-transport.patch`, hunks
marked `CONSTELLATION PATCH (io-uring)`, following the same discipline:
`tools/vendor-fuser.sh` re-applies both series in order against any new
upstream `fuser` release and fails loudly (not silently) on a conflicting
hunk (VERIFIED the script already applies every `patches/*.patch` present,
in filename order: `vendor-fuser.sh`'s `for patch in "$@"` loop over
`"$patches"/*.patch`).

The patch brings in the FUSE-over-io_uring userspace half the
Skory/fuser fork built (**REPORTED**, `github.com/Skory/fuser`, branches
`io-uring/*`, PRs #1–#8, open drafts as of the research date, design gist
`gist.github.com/Skory/4fb49ff602919596fa3c444c8abecd87`) behind a new
cargo feature `io-uring`, off by default so a build without the feature is
byte-for-byte today's vendored fuser:

- `KernelConfig::io_uring: bool` (default `false`) and
  `KernelConfig::io_uring_queue_depth: usize` (REPORTED Skory default 8) —
  new fields on the existing `fuser::KernelConfig` (VERIFIED the struct
  already lives in `vendor/fuser/src/lib.rs` and already carries
  `max_stack_depth` for passthrough at `:223`, so this is one more field on
  an existing, already-extended struct, not a new config surface).
- **One ring per worker thread**, the kernel's per-CPU queues partitioned
  across them (REPORTED Skory design: `SINGLE_ISSUER` + `DEFER_TASKRUN`),
  matching Constellation's existing `recommended_workers` sizing
  (`crates/frontend-fuse/src/threads.rs:14`, `2·⌈√cpus⌉`) rather than the
  kernel's one-ring-per-CPU default the bench measured at — §4 is explicit
  about why that difference matters for the RSS budget.
- A dedicated **`/dev/fuse` reader thread** for `INIT`/`FORGET`/`INTERRUPT`/
  notifications (REPORTED Skory design) — these never go over a ring; the
  ring only carries request/reply pairs the kernel's io_uring machinery
  handles.
- **`ReplyData::fill(|buf| ..)`** (REPORTED Skory API): writes a read
  reply's bytes directly into the ring entry's payload buffer instead of
  `ReplyData::data(&[u8])`'s copy-then-`writev`, and a gather variant that
  accepts `ReadData`'s `SmallVec<[Bytes; 4]>` segments directly (this
  plan's own addition on top of Skory's single-buffer `fill` — see
  §3(b)'s `ReadReply` change) so a multi-segment `ReadData` (the rare case,
  per `ReadData`'s own doc) doesn't force a `contiguous()` copy first.
- **Replies allowed from any thread**, not just the ring's own worker
  (REPORTED Skory design, and load-bearing for Constellation: the engine's
  completion pool answers deferred cold reads, VERIFIED
  `crates/engine/src/view/io.rs`'s module doc, "Cold reads defer" — a ring
  reply path that only worked from the issuing worker would silently
  regress every cold read back onto a blocking frontend thread the moment
  the ring transport is on).
- **Graceful fallback to `/dev/fuse`** when the kernel or a seccomp policy
  refuses ring registration (REPORTED Skory design) — this is where §2.4's
  runtime ladder's step 2→3 transition is actually implemented, inside the
  vendored patch, not re-invented in `constellation-frontend-fuse`.
- **`NegotiatedInit` extended with the negotiated transport** — the existing
  patch series already added `NegotiatedInit` (VERIFIED
  `vendor/fuser/CONSTELLATION-PATCH.md`, "negotiated-init" patch: kernel and
  protocol versions, agreed `InitFlags`, `max_readahead`, `max_write`,
  `max_background`, `congestion_threshold`, `time_gran_ns`, `max_pages`,
  `max_stack_depth`); this plan adds a `transport: Transport` field
  (`DevFuse | Uring | UringZeroCopy`) so the handover gates (the existing
  `check_resumable()`, same file, and `FuseSession::detach`) can refuse any
  session whose transport is not `DevFuse`. Z0a showed that a ring session
  can be neither handed over losslessly nor downgraded to `/dev/fuse`
  (§3(e)), so this is exactly the check §3(e) needs and nothing more.
- `clone_fd` is ignored when the ring transport is active (REPORTED Skory:
  a ring session doesn't need the `FUSE_DEV_IOC_CLONE` per-worker
  descriptor trick `channel.rs`'s existing `clone_fd` does, VERIFIED
  `vendor/fuser/src/channel.rs`'s `clone_fd` method — the ring's own
  per-worker queues replace what `clone_fd` exists for).

**Milestone Z1 (§7) is where every REPORTED claim above gets checked before
anything depends on it**, by a fixed rubric mirroring plan 37's K0 (which
re-verified its own REPORTED fuser claim the same way, §6.11's "Risk,
verified against the vendored source, not assumed"): test coverage (the
Skory fork's own suite — 44–47 unit tests, pjdfstest parity, REPORTED —
re-run against *our* vendored copy, not trusted from the README), pjdfstest
parity specifically (does it stay 8798/8798 against a real kernel, the same
bar C4 and C7 hold, `docs/plans/v1/wip/31-core-frontend-backend.md` §15
items 4–5), and code size/shape (how much of it is a clean lift into
`vendor/fuser`'s existing layout versus a rewrite against this plan's own
`NegotiatedInit`/`Transport` extension). **Vendor-vs-rewrite is decided at
Z1, by that rubric, not assumed here** — the design above is what to vendor
*if* Z1's re-verification confirms it is sound; if it is not (an
undocumented soundness bug, a licensing mismatch beyond the MIT the fork
already carries, REPORTED "MIT like fuser"), Z1 is the milestone that says
so and the fallback is a narrower from-scratch patch against just the ring
primitives Constellation's own design needs, following the same
`CONSTELLATION PATCH (io-uring)` hunk-marking discipline either way.

### 3(b) `constellation-frontend-fuse` adapter changes

- **`ReadReply`'s gather path** (`crates/frontend-fuse/src/reply.rs:111-119`):
  today `done` always calls `self.0.data(&data.contiguous())` — a copy
  whenever `ReadData` has more than one segment, always on the
  `/dev/fuse` path regardless. With the `io-uring` feature and a ring
  session, `ReadReply::done` calls the vendored `ReplyData::fill`'s gather
  variant (§3(a)) with `data.segments()` (VERIFIED
  `crates/vfs/src/types.rs:360-362`'s `segments(&self) -> &[Bytes]`)
  directly, writing each segment into the ring payload in place: one copy
  (into the payload), zero syscalls, versus today's one copy (gather, when
  multi-segment) plus one `writev`. On a `/dev/fuse`-transport session
  (fallback, or a handover-pinned `DevFuse` session, §3(e)), `ReadReply`
  keeps exactly today's `contiguous()` + `data()` path — this is an
  additive branch on the existing type, not a rewrite of it.
- **`WriteData` borrowed from the payload**: the write side's equivalent —
  a ring-delivered write's bytes already live in the entry's payload
  buffer, so `Vfs::write`'s `WriteData::Borrowed(data)` (VERIFIED
  `crates/frontend-fuse/src/adapter.rs`'s `write` handler, already passing
  `WriteData::Borrowed(data)` from the plain `/dev/fuse` request buffer)
  borrows from the ring payload instead of the per-worker 16 MiB buffer
  (`crates/frontend-fuse/src/threads.rs:10`) when a ring session delivers
  it — no new `WriteData` variant, the existing `Borrowed` already expresses
  "this slice outlives the call, don't copy it."
- **Open reply for passthrough** (§3(c)): `OpenReply`
  (`crates/frontend-fuse/src/reply.rs`'s existing `Opened` responder) grows
  a branch that calls `ReplyOpen::opened_passthrough` (VERIFIED
  `vendor/fuser/src/reply.rs:397`) with a `BackingId` obtained from
  `ReplyOpen::open_backing` (`:370`) when the engine's `open()` (§3(c))
  reports the file is passthrough-eligible; otherwise the existing
  `self.0.opened(FileHandle(o.fh.0), FopenFlags::empty())` path is
  unchanged.
- **Metrics**: every `constellation_vfs_ops_total`/`constellation_vfs_op_seconds`
  emission (VERIFIED `crates/control/src/web.rs:820-825`'s `render_vfs_ops`,
  the existing `{frontend,view,op,outcome}`/`{frontend,view,op}` label set —
  `view` is omitted for a view with none — descended from plan 31 §6.10)
  gains a `transport` label (`dev_fuse | uring | uring_zc`) on the
  FUSE frontend's rows — Constellation's other frontends (NFS, WinFsp, SAF)
  always report `dev_fuse`-equivalent-or-N/A, so the label's cardinality
  stays bounded to the four values this plan introduces plus whatever those
  other frontends already used.
- **`node.status`**: see §5.

### 3(c) Passthrough eligibility and the engine-side hooks it needs

**Eligibility rule**: a file is passthrough-eligible on `open()` exactly
when — read-only open (`O_RDONLY`, no write intent: `open_flags(flags)`
already decodes this, `crates/frontend-fuse/src/adapter.rs:237`), the
file's committed length is ≤ the filesystem's chunk size (so its whole
content is exactly one chunk), that one chunk is resident in the disk
cache (`DiskCache::contains`, VERIFIED `crates/fs-core/src/cache.rs`'s
`contains` method, immediately preceding `get_shared`), the inode has **no**
`WriteState` (`crates/engine/src/view/io.rs`'s `do_read_detached` already
threads `ws: Option<&WriteState>` through every read — the same check that
routes a read through the pending-write overlay is the check that must
*fail* for passthrough to be offered) and **no pending truncate** (a
`WriteState` with a `floor` set, same struct). Any of these being false at
`open()` time falls through to the ordinary `Opened` reply — passthrough
eligibility is decided once, at open, never re-evaluated mid-open (closer
to open-to-open than a live re-check, matching "invalidation semantics =
close-to-open" below).

**Engine-side hook — the new fact `open()` needs to answer.** Today's
`Vfs::open` (`crates/vfs/src/types.rs`'s trait definition,
`fn open<R: Responder<Opened>>(&self, cx: &OpCtx, ino: Ino, flags: OpenFlags, owner: OpenOwner, r: R)`,
VERIFIED plan 31 §6.2) answers with an `Opened` (VERIFIED
`crates/frontend-fuse/src/reply.rs`'s `OpenReply` consuming exactly that
type) that has no notion of "and here is a backing fd." This plan extends
`Opened` with an optional field — `backing: Option<PassthroughChunk>` where
`PassthroughChunk` carries the chunk file's path (or a pre-opened
`OwnedFd`, cheaper: opening it once in the engine and handing the fd across
avoids a second `open(2)` in the frontend) — populated by `View::open`
exactly when the eligibility rule above holds. This is new API surface on
an existing trait's associated type, not a new trait method: every other
frontend (NFS, WinFsp, SAF) simply never sees `backing: Some(..)`, since
none of them call `opened_passthrough`.

**Pin-while-open, not deferred free — and why.** `DiskCache` already has an
eviction-resistant state, `ChunkState::Pinned` (VERIFIED
`crates/fs-core/src/cache.rs:31-36`), used for subtree pin/offline
designation (plan 3/`docs/plans/v1/done/03-p4a-pin-offline.md`) — but
reusing it here would conflate two different lifetimes: a subtree pin is an
operator decision, persisted, visible via `pin`/`unpin` commands and
surfacing in `node.status`; a passthrough-open pin is ephemeral, entirely a
function of how many open file descriptors the kernel currently holds on
that chunk, and must clear itself the instant the last `close(2)` lands
(or, on a crash, the instant the process restarts — a crash drops every
open fd anyway, so nothing needs to persist across it). This plan therefore
adds a **separate, in-memory open-refcount**, not a third meaning for
`ChunkState`: `DiskCache` gains `pin_open(hash) -> OpenPin` (a guard
incrementing a per-hash `u32` under the existing state lock) and
`plan_eviction`/`prune_to` (VERIFIED `crates/fs-core/src/cache.rs:904`,
`:747` respectively) treat a chunk with a nonzero open-refcount as
non-evictable, the same way they already skip `Pinned`/`Dirty` entries
(`clean_by_recency`'s existing filter, VERIFIED the `Pinned` branch at
`:879`'s `merge_state` and the `filter(|entry| entry.state == ChunkState::Pinned)`
calls at `:413`/`:778`). `View::open`'s engine-side hook (above) calls
`pin_open` when it reports `backing: Some(..)`; `Vfs::release` — already the
last-close hook (VERIFIED plan 31 §6.2, `fn release<R: Responder<()>>`,
doc "Last close of this handle") — drops the guard. **The alternative this
plan rejects**: "accept the deferred free" (let eviction proceed and unlink
the file on disk while a backing fd keeps the inode alive past the
unlink, relying on Linux's normal unlink-while-open semantics to keep bytes
readable until the last fd closes). That would work — POSIX guarantees
it — but it silently detaches the disk cache's own accounting (`st.used`)
from what is actually still consuming space on disk for as long as any
passthrough open outlives its eviction, which is exactly the ambiguity
plan 32's space-accounting work (§"Coordinates with plan 32" above) cannot
tolerate: an evicted-but-still-open chunk must either still count against
the budget (pin-while-open, chosen) or plan 32 needs to know to special-case
it, and nothing in plan 32's current design does. Pin-while-open keeps the
existing accounting invariant exactly as it already reads
(`crates/fs-core/src/cache.rs`'s module doc: "Pinned and dirty entries are
never removed") at the cost of one more non-evictable state a very large
number of small-file passthrough opens could in principle starve the cache
with — bounded in practice by "one open fd per currently-open single-chunk
file," the same bound every other filesystem's open-file table already
imposes.

**Scan-ahead and atime move to `open()`.** Today's sequential-scan trigger
(`crates/engine/src/view/io.rs:217-220`, `if offset == 0 { self.scan.note_read(ino); ... }`)
and the read-time atime bump (`io.rs`'s `do_read_detached`, the `self.atime.on_read(..)`
call immediately after the EOF early return) both fire from inside the read
path — which a passthrough-opened file's reads never reach again once
`opened_passthrough` is sent (README, "Reads never reach the daemon"). Both
therefore move into `View::open`'s passthrough branch, fired once at open
time for an eligible file (a single-chunk file's "offset 0" and "this file
was read" are, for that file, exactly the same event as "it was opened for
read," so nothing about scan-ahead's or atime's own semantics changes —
only *when*, within the file's one read, the existing call happens).

**Invalidation semantics: close-to-open.** An open passthrough handle keeps
reading the chunk it was opened on for its whole lifetime, even if that
chunk is superseded (a remote write lands a new manifest pointing at a
different chunk hash) while the handle stays open — the kernel's backing-fd
model has no mid-open invalidation hook to revoke a `BackingId`
mid-flight, and plan 30's `cto=strict` already promises exactly this
granularity of consistency ("close-to-open", VERIFIED referenced at plan 31
§6.11 "close-to-open consistency, which plan 30's cto=strict already
promises" — this plan does not weaken or strengthen that promise, it is the
one case (a passthrough open) where Constellation's *existing* close-to-open
contract is also the *literal* mechanism (the kernel's own fd semantics)
rather than an invalidation message this plan would otherwise have to send
and didn't).

**Local writers, and the kernel's one-mode-per-inode rule (Z3b, as
built).** Two facts the eligibility rule above did not account for, found
in `fs/fuse/iomode.c`/`passthrough.c`/`backing.c` (master, re-read for Z3b)
and confirmed on 7.3-rc4:

1. *The kernel keeps one I/O mode per inode.* While any handle of an inode
   is open with `FOPEN_PASSTHROUGH`, every other open of it must be too, on
   the **same** backing file (`fuse_file_io_open`: an ordinary reply is
   `EIO` to the opener, a different backing id `EBUSY` → `EIO`); while any
   is open the ordinary, page-cached way, a passthrough open is refused
   (`ETXTBSY` → `EIO`). The engine decides per open; the FUSE adapter keeps
   a per-inode table (`crates/frontend-fuse/src/passthrough.rs`) and shapes
   every reply to that rule: an eligible open goes passthrough only when no
   ordinary handle of the inode is open (or about to be: a write-intent
   open registers before it calls the view); while some handle is in
   passthrough mode every open reuses its backing id — plain passthrough for
   a read-only open the engine offered the same chunk, otherwise
   `FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO`, which sends the handle's reads and
   writes to the daemon (so it sees the current bytes) and leaves only
   `mmap` on the backing file.
2. *The kernel opens the backing file with the opener's flags*
   (`backing_file_open(file, file->f_flags, …)`), so a read-write open
   answered with the backing id could write the content-addressed chunk
   file through a shared writable `mmap`. A **read-write open of an inode
   in passthrough mode is therefore refused with `ETXTBSY`** until its
   passthrough handles close — the errno Linux already gives an open for
   writing of a running executable. A write-only open cannot be `mmap`ed
   and is served (direct I/O, by the daemon).

The coordinator's local read-after-write requirement is met as follows: an
open is never passthrough while any handle of the inode can write it (the
view counts write-access handles, `writers`, and refuses) or while it has a
write session. **A write-intent open that arrives *after* a passthrough open
is close-to-open on the same mount**: the earlier passthrough handle keeps
the bytes it was opened on until it is closed, exactly as for a writer on
another node, while every open *after* the writer sees the writer's bytes
(asserted by the `passthrough-local-writer` scenario and documented for
users in `docs/reference/configuration.md`, "FUSE passthrough").

**Default: read-only mounts only (review 38-z3b, coordinator decision).**
The two facts above force a non-standard outcome on one legal POSIX call.
Once an inode has a passthrough handle, `fuse_file_io_open` turns any
reply to a later open that is not `FOPEN_PASSTHROUGH` on the same backing
(plain, or `FOPEN_DIRECT_IO` alone) into `EIO`, and the only other answer
(`FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO`) makes `fuse_passthrough_open` open
the backing file with the opener's `f_flags` — writable for an `O_RDWR`
opener, whose `MAP_SHARED|PROT_WRITE` `mmap` (routed to
`fuse_passthrough_mmap`) would write the content-addressed chunk. Demotion
is impossible: there is no revoke, and `fuse_inode_uncached_io_start`
requires the same `fb`. So `open(O_RDWR)` of a file another process holds
open by passthrough must get *some* error; `ETXTBSY` is the least bad, but
its timing depends on what else happens to have the file open, and it hits
ordinary programs: `fopen(f, "r+")`, Java `RandomAccessFile(f, "rw")`,
Python `open(f, "r+b")`, SQLite's read-write open while a `?mode=ro`
connection holds the database, `fallocate(1)`. pjdfstest cannot see it
(it never holds a file open across another process's open). Against a gain
of ~25 % CPU-s/GiB on the cold small-file lane only, the decision is:

- **Read-only mounts** (`MountOptions::read_only`, a frozen snapshot view,
  which the kernel mounts `ro`): passthrough **on by default** — there
  `open(O_RDWR)` is `EROFS` in the VFS before FUSE sees it, so the conflict
  is unreachable.
- **Writable mounts**: **off by default** (`node.status` reason
  `writable_mount`, not logged as a downgrade). `CONSTELLATION_FUSE_PASSTHROUGH=1`
  (or `MountOptions::passthrough = PassthroughPolicy::On`) is the
  documented opt-in carrying the `ETXTBSY` caveat and the list above
  (`docs/reference/configuration.md`, "FUSE passthrough").

*Frozen files (Z3c).* The only read-only mounts are frozen snapshot views,
whose files `View::open` answers as synthetic `Frozen` nodes, so Z3b's
default negotiated passthrough there (`enabled: true`) without serving a
file by it. Z3c extends eligibility to them
(`View::frozen_passthrough_backing`): the same shape and residency rule —
one inline chunk holding exactly the file's `size` bytes, resident and
verified in the disk cache, `--cache-verify admit`, a frontend that
consumes backing files — and the same pin-while-open guard. What
immutability makes moot is left out: a snapshot file has no write session,
no writer and no pending truncate, and its open for writing is `EROFS`
before the rule runs. The frozen manifest comes from a per-view cache of
snapshot manifests (`frozen_manifests`, bounded by
`CONSTELLATION_SNAPSHOT_MANIFEST_CACHE`, default 8192 entries, LRU; never
invalidated, because a snapshot manifest is immutable) that `read_frozen`
shares, so the open's load is the reads' too — before Z3c every read of a
frozen file loaded it again — and it is looked up at open only for a file
no larger than a chunk. Neither read-path hook moves to the open, because
a frozen read fires neither (snapshots have no atime, and scan-ahead walks
the live tree). A synthetic inode is not counted in `opens` (that table
names live inodes to the open-orphan hold writer), so its `release` trims
the pins to the handles of it the plan-39 handle table still lists, which
a handover carries too. The same rule serves a frozen file reached through
`<dir>/.constellation/snapshot/<name>/` on a writable mount that opted in.
A *live* view mounted read-only would need no change — its opens take the
live rule, which does not depend on the mount — but no product path makes
one today (`MountOptions::read_only` is set only for a frozen view; a CSI
read-only volume is a read-only *bind* of a writable FUSE mount).

*Not for a chunk held in memory (Z3c review).* Measured on a snapshot view
(4096 × 64 KiB files, 4 MiB chunks, a 1 GiB memory tier), Z3c as first
written served every open by passthrough once the chunks were verified,
and after `drop_caches` that took ~3× the wall time and ~1.8× the daemon
CPU of the memory hits it replaced (7.7 s against 2.4 s), with no gain on a
warm page cache; with the memory tier off it was at parity or better. The
default must not regress, so the residency check both rules share
(`pin_backing`) refuses a chunk the memory tier holds
(`DiskCache::in_memory`, a peek that moves neither the tier's counters nor
its recency). Passthrough is therefore taken only for verified chunks on
disk and not in memory: those the memory tier has evicted, or all of them
with the tier off. The cost: the verifying first read admits a chunk to
memory, so a small, hot set of files is rarely served by passthrough; it
engages where the daemon would read the disk cache anyway, which is where
Z3b measured its win (an empty memory tier). With it, the default is at
parity with `CONSTELLATION_FUSE_PASSTHROUGH=0` or better on every row
(medians, PROGRESS "Plan 38 Z3c"): memory tier on, nothing is passthrough,
page cache warm 2.16 s vs 2.29 s and after `drop_caches` 2.19 s vs 2.35 s;
tier off, every open is passthrough, warm 2.14 s vs 2.25 s and after
`drop_caches` 7.34 s vs 7.83 s.

The `ETXTBSY` refusal is made at the open's *reply*, after the view
answered (the view's handle is released again), so the view's own
`EROFS`/`ENOENT`/`EACCES` win over it. A related edge the kernel leaves on
opted-in writable mounts: a `FOPEN_PASSTHROUGH | FOPEN_DIRECT_IO` handle
(write-only opener, `backing_busy`, an open after a writer) `read()`s the
current bytes through the daemon, but `fuse_file_mmap` routes its `mmap`
to `fuse_passthrough_mmap` whenever `ff->passthrough` is set, so it maps
the *old* chunk until every passthrough handle of the inode closes — not
fixable from userspace, documented next to the `ETXTBSY` note.

`CapEff` showing `CAP_SYS_ADMIN` is not proof the kernel's `capable()`
(initial user namespace) agrees, and a cache directory on overlayfs is
refused as stacked (`ELOOP`): each session therefore registers and closes
one probe file in the cache directory before it serves
(`PassthroughState::probe`, `Vfs::passthrough_probe`), and a failure turns
passthrough off for the session with reason `backing_open`, logged once,
instead of reporting `enabled` while every open falls back.

**Handover keeps the pins (Z3b).** A backing id belongs to the connection,
not to the process (`fc->backing_files_map`), and the kernel's
`fuse_backing` holds its own reference to the backing file, so the kernel
goes on serving a handed-over passthrough handle from the old process's
chunk file. The FUSE session's handoff therefore carries the adapter's
per-inode table with the ids still registered (`FuseHandoff::passthrough`;
the resumed session reuses an id for later opens of that inode and closes
it at the last release), the view's snapshot carries each passthrough
handle's chunk hash and the write-intent counts
(`HandleTableSnapshot::{passthrough, writers}`), the resumed view re-pins
those chunks, and the old image keeps its own pins until it `exec`s
(`close_view_for_handover` no longer drops them). What crosses changed, so
`HANDOVER_VERSION` is 4 (plan 39 took 3 for its per-open handles and
discard error events, which cross alongside): an image without the table
is refused by the handover ABI check rather than resumed.

### 3(d) Zero-copy on 7.3+

`FUSE_IO_URING_CMD_ADD_QUEUE` with the `FUSE_URING_ZERO_COPY` flag,
`FUSE_IO_URING_CMD_ADD_BUFPOOL` registering buffers with
`IORING_URING_CMD_FIXED`, `FOPEN_IO_URING_ZERO_COPY` set on eligible opens'
replies, and `IORING_OP_READ_FIXED` issued from the chunk file's fd at the
chunk's byte offset straight into the request's registered pages — all
REPORTED from the 7.3 FUSE pull request (Joanne Koong, `[PATCH v7 0/6] fuse:
add io-uring buffer pools`) and the kernel doc's companion zero-copy
description, re-verified at Z4 (§7) the same way Z1 re-verifies the plain
transport. **One `READ_FIXED` per request**: a kernel read that spans two
chunks (crosses a chunk boundary) cannot be answered by one `READ_FIXED`
against one chunk file, and Constellation has no whole-file backing object
to fall back to the way the bench's `uring-zc` mode does (README: "a
request that spans two chunks is read from `<name>.whole`" — `chunkfs`'s
`.whole` copy exists purely for the bench; Constellation's chunk files are
the only on-disk representation a file ever has). **Chunk-spanning reads
therefore fall back to the memcache path** (§2.1 row 4, unchanged), not to
a second `READ_FIXED` or a synthesized whole-file read — this is a hard
per-request routing decision in `View::do_read_detached`
(`crates/engine/src/view/io.rs:191`, which already slices a read into
per-chunk `layout.slices(offset, len)` at the point `push`ing bytes into
`ReadData` happens): a request whose slice list has length 1 and whose
slice covers a resident, verified chunk is zero-copy-eligible; anything
longer routes through the existing `get_shared` path exactly as it does
today. `CAP_SYS_ADMIN` gates zero-copy exactly as it gates passthrough
(§2.4's ladder, step 1). **The memcache stays** for the non-zero-copy path
and is the *only* path when `--cache-verify always` is set (§2.3) — nothing
about the memcache's design, eviction policy, or budget changes; this plan
only adds a second, parallel way to answer a read that bypasses it when the
operator has opted into the `admit` trust model and the request qualifies.

### 3(e) Session handover: resolved by Z0a — handover-capable sessions stay on `/dev/fuse`

Plan 31 §6.11 built FUSE session handover (`FuseSession::detach`/`resume`,
the vendored `from_fd_resumed`) for two consumers: `constellation daemon
--upgrade` on plain Linux, and plan 37's CSI engine-pod replacement. Both
rely on two properties of the `/dev/fuse` transport. First, `detach` *stops
reading `/dev/fuse` and drains in-flight ops* before the fd changes hands.
Second, everything that arrives in the gap waits in the kernel's queue until
the next process's first `read(2)`. The question was whether a *new
process* can take over a connection whose io_uring queues belonged to the
old process's io_uring instance, which is process-local and dies with it.

**Z0a answered it** (VERIFIED: `bench/fuse-uring-handover/RESULTS.md`, a
raw-uapi C server, 4 variants × 5 repeats with two continuous verified
readers, on 6.17.0 (Ubuntu 25.10), 7.0.0 (Ubuntu 26.04) and
7.3.0-rc4 (Fedora Rawhide), 60 runs, identical behaviour on all three, and
predicted from `fs/fuse/dev_uring.c` of each version before running):

- **(a) The connection survives.** It is aborted only when the last
  `/dev/fuse` file reference goes (`fuse_dev_release`), and the new process
  holds one. io_uring teardown (explicit or by process death) only runs
  `fuse_uring_cancel` on the old process's *available* entries. No
  `ENOTCONN`, no `dmesg` line, in 60/60 runs.
- **(b) Requests never return to `/dev/fuse`.** Once every queue has had an
  entry, `fuse_uring_do_register` switches `fiq->ops` to the ring, and
  nothing switches it back. After the handover, B's `/dev/fuse` reader got 0
  requests in 60/60 runs. Without ring entries, every request waits in its
  per-CPU queue. **A resumed ring session can therefore not be downgraded to
  `/dev/fuse`.** The plan's "(a)+(b) → `Auto` with the resumed session
  downgraded" outcome is impossible.
- **(c) Re-registration works mechanically but not losslessly.** B's
  `FUSE_IO_URING_CMD_REGISTER`s on the already-initialised connection are
  accepted (720/720), and B then serves new requests correctly. Two defects
  make it a different thing from the `/dev/fuse` handover:
  1. **In-flight loss.** Every request that sat in one of the old process's
     ring entries when its io_uring went away is orphaned. There is no
     commit-without-fetch or unregister command, so a server cannot stop
     taking requests except by destroying its io_uring. Re-armed entries
     go to the head of the available list, so the old process takes most
     requests while it lives, even with B's entries registered. The new
     process cannot complete an orphaned request either: commit reads the
     reply through the entry's registered user pointer, which points into
     the old address space. Its caller blocks in `request_wait_answer`,
     **unkillable** (D after `SIGTERM`), until the connection is aborted
     through fusectl. This hit 30/30 in-flight reads per kernel when B
     registered after A was gone, and 12/30 over all kernels when B
     registered first (overlap).
  2. **Gap stall.** A request queued while no entry was available is not
     dispatched by a REGISTER. It moves only when a later request on the
     same per-CPU queue completes a commit. It finished 3.74–3.78 s after
     B's REGISTER, at the moment an unrelated request touched that CPU's
     queue (30/30 runs). On a quiet mount it waits indefinitely.

  B also has to register on every possible CPU's queue, with payloads sized
  from the negotiated INIT. On 7.3 a single failed REGISTER disables the
  ring for the connection (`fch->io_uring = 0`) while the ring stays
  "ready", which leaves the mount unservable (source reading). Zero-copy
  and bufpool queues (Z4) are worse still: their pool is bound to the
  registering process's address, and `ADD_QUEUE`/`ADD_BUFPOOL` refuse an
  existing queue (source reading, not run).

**The policy, settled.** A new `transport: TransportPolicy` field
(`Auto | DevFuse`) on `MountOptions` (VERIFIED the existing `MountOptions`
struct, `crates/frontend-fuse/src/session.rs`, already carrying `fs_name`,
`allow_other`, `read_only`, `n_threads`, `tuning: KernelTuning` — this is a
sibling field on the same "how a view is mounted" struct, not a new config
type). `Auto` (the default for a plain, non-handover-capable daemon mount)
lets §2.4's ladder run as designed. **A session opened with handover in
mind — plan 37's CSI engine pods, and `constellation daemon --upgrade`'s own
target mount — is `DevFuse`, permanently** (on every kernel through
7.3-rc5; this is no longer "until the spike proves otherwise"). The transport
is fixed when the mount starts, because a connection whose ring became ready
can never serve over `/dev/fuse` again. It follows that **`FuseSession::detach`
(and so `node.handoff` / `daemon --upgrade`) refuses a session whose
negotiated transport is not `DevFuse`**, with an error that names the
transport. Tearing the mount down and remounting is the only upgrade path
for such a session. Plain mounts were never going to be handed over, so
`Auto` costs them nothing.

Revisit only if upstream FUSE gains both (i) a way for a server to stop
taking requests on its entries without losing the connection (a
commit-without-fetch or unregister-entry command, or a "quiesce ring" that
returns requests to the `fiq` queue), and (ii) dispatch of already-queued
requests when an entry is registered. Z0a's `bench/fuse-uring-handover/run.sh`
is the test to re-run.

## 4. Threading, memory and CPU budgets

**Ring payload memory.** The bench's own measurement — ~130 MiB RSS on a
16-CPU host, libfuse's defaults (one ring per CPU, queue depth 8,
`max_write` 1 MiB payload per entry: 16 × 8 × 1 MiB ≈ 128 MiB, README
"Caveats" "io_uring memory") — is **not** Constellation's number to adopt
directly, because Constellation's worker count is `2·⌈√cpus⌉`
(`crates/frontend-fuse/src/threads.rs:14`), not one-per-CPU: at 16 CPUs
that is 8 workers (`2·⌈√16⌉ = 2·4`), at 64 CPUs (the hard cap,
`FUSE_THREAD_HARD_MAX`, `threads.rs:7`) it is capped at 64 workers — never
more than the CPU count the kernel would otherwise give one ring each. With
one ring per **worker** (§3(a)'s design, not per CPU), the formula is
`workers × queue_depth × max_write`: at 8 workers, queue depth 8
(REPORTED Skory default), Constellation's own `max_write`
(`fuser`'s existing per-worker request buffer is 16 MiB,
`FUSE_BUFFER_BYTES`, `threads.rs:10` — whether a ring entry's payload
should be sized the same or to the bench's smaller 1 MiB default is a Z1
implementation choice re-verified against real RSS, not assumed here), the
budget spans roughly 64 MiB (8×8×1 MiB) to 1 GiB (8×8×16 MiB) depending on
that choice — **Z1's gate (§7) is exactly this: measure real RSS on
Constellation's own worker count and 6.14+ kernel, and report it next to
this range**, not promise a number the bench's different topology doesn't
license. Today's existing `/dev/fuse` path already spends `workers × 16
MiB` (`FUSE_BUFFER_BYTES`) regardless of this plan — 128 MiB at 8 workers —
so the ring transport's *additional* cost, not its absolute size, is the
number that matters for a budget decision, and it is bounded by whichever
`max_write` choice Z1 measures against that existing baseline.

**`EngineProfile` sizing.** The ring payload budget is host memory the
`io-uring` feature spends regardless of `EngineProfile` (it is fixed by
worker count and queue depth, not by the chunk memcache's `EngineProfile`-driven
budget, VERIFIED `crates/engine/src/profile.rs:163-172`'s
`chunk_memcache_default`) — but it competes for the same host RAM the
memcache and the `EngineHost`'s `ResourceBudget` share partitions
(`crates/engine/src/profile.rs`'s module doc, "an explicit per-engine
override of the equal share an `EngineHost` would give it"). For the
`mobile` profile specifically (`EngineProfile::mobile()`,
`crates/engine/src/profile.rs`, `BackgroundMode::OnDemand`, 16 MiB memcache
cap) the ring transport is not expected to be worth its RSS on a phone's
tight budget at all — §8 (Risks) covers this as "ring RSS on the mobile
profile," and the practical answer is `TransportPolicy::DevFuse` (§3(e)) on
that profile by default, not a smaller ring sizing, since Android (plan 36)
doesn't mount through the Linux kernel's `/dev/fuse`+io_uring path in the
first place (it is an in-app engine behind SAF) and so never reaches this
plan's ladder at all — this line exists only for a Linux `server`/`desktop`
profile engine running under real memory pressure, where `DevFuse` is
still the right escape hatch. **`server`** (`EngineProfile::server`,
explicit `memory_budget`/`cache_budget`, plan 37's CSI engine pods): the
ring budget is a fixed per-engine cost independent of how many PVs one pod
serves (one FUSE mount, one ring set, regardless of view count within the
engine) — worth stating explicitly since plan 37's `EngineHost` runs N
engines sharing a budget, and N mounts would mean N ring budgets, a real
multiplier §4's formula above should be read as "per mount," not "per
host."

**Knobs.** `CONSTELLATION_FUSE_TRANSPORT` (`auto | dev-fuse`, matching
`TransportPolicy`'s two variants, §3(e); env override following the
existing `CONSTELLATION_PROFILE`/`CONSTELLATION_CHUNK_MEMCACHE_BYTES`
pattern, `crates/engine/src/profile.rs:28`, `:186-247`) and
`CONSTELLATION_FUSE_URING_QUEUE_DEPTH` (overriding the `io_uring_queue_depth`
default, for an operator who has measured their own RSS/throughput
trade-off and wants a specific point on it rather than the shipped
default).

**Expected CPU/GiB targets — stated as targets to re-measure, not
promises.** Carried from §1/§2's bench numbers, on the understanding that
Constellation's own daemon (chunk hashing, manifest lookups, the `View`'s
op-lock and prefetch bookkeeping around each read — none of which
`chunkfs`, the bench's minimal filesystem, does) will not reproduce them
exactly: **rand4k-8j-dio-shaped load (hot random reads under concurrency,
O_DIRECT)**: ~4.7× less daemon CPU per GiB served with the plain transport
versus today's `writev` path (bench: 0.78 vs 3.71 s/GiB); **single-stream
buffered sequential**: expect a throughput *regression* (bench: −22% to
−25%) unless the request happens to land on one ring's CPU favorably, with
CPU/GiB still improving (−24% to −43%); **zero-copy-eligible single-chunk
sequential reads**: ~3.1× less CPU than today's plain-copy path, ~2.7× less
than the memcache path (bench: 0.044 vs 0.139/0.121 s/GiB); **passthrough-eligible
opens**: effectively 0 daemon CPU, by construction (the daemon never sees
the read). Z1/Z3/Z4's gates (§7) are where these become Constellation's own
recorded numbers, on Constellation's own binary, against Constellation's
own workloads (`vfs-bench`, the harness's `fio-latency`/`bench` scenarios,
and this plan's new fio-based gate, §6).

## 5. Observability and control

- **Metrics**: `constellation_vfs_ops_total{frontend,view,op,outcome}` and
  `constellation_vfs_op_seconds{frontend,view,op}` (VERIFIED
  `crates/control/src/web.rs:820-825`) gain a `transport` label
  (`dev_fuse | uring | uring_zc`) on FUSE-frontend rows (§3(b)). New
  counters mirroring the existing `constellation_cache_memory_*` family
  (VERIFIED PROGRESS.md "Chunk memory cache", "Observability" row:
  `constellation_cache_memory_{budget_bytes,used_bytes,chunks,protected_bytes}`
  gauges, `..._{hits,misses,coalesced,evictions}_total` counters) for this
  plan's own new state: `constellation_fuse_passthrough_opens` (gauge, opens
  currently backed by a `BackingId`), `constellation_fuse_zero_copy_reads_total`
  (counter), `constellation_fuse_transport_fallbacks_total{from,to,reason}`
  (counter, the "every downgrade logged once" requirement from §2.4 made
  queryable, not just logged).
- **`node.status`** (VERIFIED `crates/control/src/methods.rs:191-192`'s
  `NodeStatus` method, `/metrics`'s existing translation of it per
  `crates/control/src/web.rs:12`): a new `fuse` section reporting the
  negotiated `transport` per mount (`dev_fuse | uring | uring_zc`), ring
  queue depth, `cache_verify` (`admit | always`, §2.3), passthrough open
  count, zero-copy read count, and the most recent transport fallback (if
  any) with its reason — the same shape the existing `cache.memory_*`
  fields already established for the memcache follow-up
  (PROGRESS.md "Chunk memory cache", "Observability" row), extended rather
  than reinvented.
- **`stats.subscribe`** samples (VERIFIED `crates/engine/src/control/streams.rs:198`'s
  existing `c("vfs_ops_total", ops)` pattern): the new counters/gauges above
  are added to the same sampled set, following the same `c(name, value)`
  idiom already used for `vfs_ops_total` and the memcache counters.
- **Tracing spans**: every op already gets an `OpId` and a span
  (`crates/engine`'s `Observer`, VERIFIED referenced at plan 31 §6.10) — the
  FUSE adapter's span gains a `transport` field alongside the existing
  `op`/`frontend` fields, so a trace shows which path served a given read
  without cross-referencing metrics.
- **`OpWatch`** (the stalled-op watchdog, `fuse_watch.rs`'s generalisation,
  VERIFIED plan 31 §6.8) is **unchanged**: a ring-delivered or
  passthrough-served op still goes through the same `OpId`/watchdog
  lifecycle as a `/dev/fuse` one (passthrough opens simply never generate
  the read ops `OpWatch` would track, since the kernel serves them
  directly — nothing to watch is exactly correct for a request the daemon
  never received).

## 6. Testing

- **Transport matrix lane**: every harness scenario (`crates/harness/src/scenarios.rs`,
  VERIFIED the `SCENARIOS` array's existing `requires: &'static [&'static str]`
  field, the gating mechanism already used for optional host binaries)
  relevant to the read path runs twice on a 6.14+ CI kernel — once with
  `CONSTELLATION_FUSE_TRANSPORT=dev-fuse`, once with `=auto` — so a
  transport-specific regression shows up as a scenario failing on exactly
  one lane, not a blanket failure. A separate **7.3 lane** additionally
  runs the zero-copy-specific scenarios (below); on any host below 7.3 (or
  without `CAP_SYS_ADMIN`) those scenarios **SKIP loudly** (the existing
  `requires` idiom, matching CONVENTIONS.md's "fio/stress-ng scenarios SKIP
  loudly if the tools are missing... any FAILED is not" — the same
  discipline applied to a missing kernel feature instead of a missing
  binary).
- **pjdfstest 8798/8798 on every transport** — the unconditional bar every
  plan 31 milestone already holds (VERIFIED plan 31 §15, "C4"/"C5" DoD
  items: "pjdfstest stays 8798/8798... Constellation has no compliance
  exceptions"), run once per transport in the matrix lane above; this
  plan adds no exceptions, and none are permitted by CONVENTIONS.md's
  "Constellation has no compliance exceptions; do not add any."
- **`tests/wire.rs`-style adapter tests over a socketpair, unit-level, no
  kernel** — extending the existing pattern (VERIFIED
  `crates/frontend-fuse/tests/wire.rs`'s module doc: a `SOCK_DGRAM` socket
  pair standing in for `/dev/fuse`, since `fuser::Session::from_fd` "accepts
  *any* descriptor," against a `MockVfs`) with a `Kernel` counterpart that
  speaks the ring commit protocol (`FUSE_IO_URING_CMD_REGISTER`/
  `COMMIT_AND_FETCH`) instead of `read`/`writev`, proving the adapter's
  translation layer (fuser decode → `Vfs` call → responder → fuser encode)
  is identical regardless of transport, without a real kernel or root.
- **Fault injection** (the harness's existing toxic/kill9/pause idioms,
  VERIFIED CONVENTIONS.md "Harness scenario checklist", "Fault injection
  via `env.s3_proxy()` toxics"): kernel refusing ring registration mid-`INIT`
  (assert clean fallback to `/dev/fuse`, not a hung mount); seccomp blocking
  `io_uring_setup` (the plan 37 container case, §8); `ENOMEM` on the ring
  mapping (assert fallback, not a crash); abort via
  `/sys/fs/fuse/connections/<id>/abort` while rings are armed (the existing
  `abortable: true` FrontendCaps flag and its harness use, VERIFIED plan 31
  §6.6, extended to the ring transport); `detach` of a session running
  the ring transport is **refused** with a clear error, and the mount keeps
  serving (§3(e): Z0a showed that a ring handover orphans in-flight
  requests), promoted to a standing scenario once Z2 has a ring transport to
  run it against. **Passthrough-specific**: eviction attempted
  while a chunk is passthrough-open (assert the pin-while-open guard, §3(c),
  holds — the chunk is not evicted, and the guard's refcount is exactly the
  open-fd count); a remote write landing a new manifest while a passthrough
  file is open (assert close-to-open: the open handle keeps reading the old
  chunk, a fresh open after close sees the new one); O_DIRECT-on-passthrough
  behaviour (assert it reads from disk even when the chunk is warm in the
  page cache — the documented behaviour change from §2.1 row 7, made an
  explicit, asserted test rather than a surprise).
- **Perf gate**: `tests/perf-gate.sh`/`vfs-bench` (VERIFIED PROGRESS.md's
  "Plan 31 C7b" section, `vfs-bench` already gated with per-op allocation
  ceilings in `tests/perf-baseline.json`) stays as-is — this plan adds no
  new VFS-level allocation cost (§3(b)'s changes touch what happens *after*
  a `ReadData` leaves `View::read`, not the `View`/`Vfs` boundary
  `vfs-bench` measures) — plus a **new fio-based gate** modelled on
  `bench/fuse-read-path`'s own method (a real mount, fio driving it, daemon
  `rusage` sampled over the window) that records **daemon CPU seconds per
  GiB served** and **daemon RSS** on a real Constellation mount, per
  transport, and fails only on a large regression against its own prior
  run (not against `bench/fuse-read-path`'s numbers, which measured a
  different, minimal filesystem — §4's explicit point about not promising
  the bench's exact figures). `bench/fuse-read-path`'s own numbers
  (`results/summary-tables.md`, written up in `bench/fuse-read-path/RESULTS.md`)
  are the **reference** this gate's floors are chosen against, not the gate
  itself.

## 7. Milestones

Each milestone ends in a gate; all of CONVENTIONS.md's standing gates
(`cargo fmt`/`clippy`/`test`, `tests/smoke.sh`, `tests/integration.sh`,
`harness run`, pjdfstest 8798/8798) run at every milestone, cumulative, per
CONVENTIONS.md's "Definition of done" — only the milestone-specific
additions are listed below.

- **Z0a — The handover spike (first; independent; touches no
  Constellation code).** A self-contained experiment that answers §3(e)'s
  question on real kernels and records the answer. It is written so that a
  session with no other context can execute it.

  *The question.* Process A serves a FUSE connection over io_uring.
  Constellation's handover (`FuseSession::detach` → `SessionHandoff` →
  `Session::from_fd_resumed` in the next process image, VERIFIED
  `crates/frontend-fuse/src/session.rs`, `vendor/fuser/CONSTELLATION-PATCH.md`)
  passes the `/dev/fuse` fd to process B and A exits — taking its io_uring
  instance, and therefore the ring entries the kernel had registered
  against it, with it. Three things are unknown: **(a)** does the
  connection survive the loss of its ring entries, or does the kernel
  abort it (`ENOTCONN` to every client)? **(b)** if it survives, do
  requests flow to plain `read(2)`/`write(2)` on `/dev/fuse` again, so B
  can serve without rings? **(c)** can B register fresh queues/entries
  (`IORING_OP_URING_CMD` with `FUSE_IO_URING_CMD_REGISTER`) on that
  already-`INIT`'d connection, and if the command returns 0, do requests
  actually arrive on B's ring afterwards?

  *Why not use Constellation or libfuse as-is.* Nothing in the tree speaks
  the ring protocol (Z1 vendors it), and stock libfuse cannot resume a
  handed-over connection: `fuse_session_process_buf_internal` rejects any
  first request that is not `FUSE_INIT`, and it is `_do_init` that calls
  `fuse_uring_start(se)`. Two routes; pick one, keep the other as the
  fallback if the first stalls:
  1. **libfuse, patched** (preferred; least code). `bench/fuse-read-path`
     already builds libfuse master from source (`build.sh`, `third_party/`)
     and has a working ring-mode filesystem (`src/chunkfs.c`, `--mode
     uring`, `-o io_uring`). Add a `--resume-fd N --init <max_write>,<max_pages>,<flags>`
     mode: `fuse_session_mount(se, "/dev/fd/N")` (libfuse accepts an
     already-open descriptor as the mountpoint), then a small patch to
     `lib/fuse_lowlevel.c` that marks the session initialised (`se->got_init`,
     the `se->conn.*` fields the INIT reply would have set, taken from the
     command line) and calls `fuse_uring_start(se)` directly, so the
     first request seen may be an ordinary one. Add a `--handover` mode to
     A: after N ring-served reads, fork B with the `/dev/fuse` fd
     inherited (equivalent to `SCM_RIGHTS` for the kernel's purposes),
     tear down A's rings (`fuse_uring` teardown → `io_uring_queue_exit`)
     and exit; B resumes.
  2. **Raw uapi in C with liburing** (fallback; clearest observations):
     `mount(2)` with `fd=`/`rootmode=`/`user_id=`/`group_id=`, read
     `FUSE_INIT`, reply with `FUSE_OVER_IO_URING` set, register
     `nr_cpus × depth` entries per the kernel's own
     `Documentation/filesystems/fuse-io-uring.rst` and `include/uapi/linux/fuse.h`
     of the running kernel (`fuse_uring_cmd_req`, `fuse_uring_ent_in_out`,
     the header/payload iovec layout — copy the mechanics from libfuse
     master's `lib/fuse_uring.c`, which is the reference implementation),
     serve a one-file read-only filesystem (`GETATTR`/`LOOKUP`/`OPEN`/
     `READ`/`RELEASE`/`READDIR`, `ENOSYS` for the rest), then the same
     fork-and-exit handover.

  *Read the kernel first, then test.* Before running anything, read
  `fs/fuse/dev_uring.c` for each kernel tested — `fuse_uring_register`,
  `fuse_uring_create` (the ring is per `fuse_conn`, `fc->ring`),
  `fuse_uring_stop_queues`, `fuse_uring_destruct`, `fuse_uring_cancel`
  (what `IO_URING_F_CANCEL` on io_uring exit does to the queues), and
  whatever decides between the ring and the classic `fiq` queue when a
  request is queued (`fuse_uring_ready`/`fc->io_uring` flags) — and write
  down a prediction for (a), (b), (c) with function names and line numbers
  for that version. The report records prediction and observation side by
  side; a mismatch is the most valuable finding the spike can produce.

  *Variants to run* (each: mount, start a client loop reading the probe
  file with `dd … iflag=direct bs=1M` under `timeout`, confirm via A's
  counters that reads are arriving over the ring and not `/dev/fuse`,
  trigger the handover, keep the client loop running through it):
  1. A tears down its ring and exits; B does **nothing** but `read(2)` on
     `/dev/fuse` and answer with `write(2)` — measures (a) and (b).
  2. As 1, then B registers fresh entries — measures (c); record every
     command's return value (0 / `-EINVAL` / `-EALREADY` / `-EBUSY` /
     `-ENOTCONN` / hang) and, on 0, whether the next client reads land on
     B's ring or still on `/dev/fuse`.
  3. B registers **before** A tears down (overlap): does the kernel accept
     two registrants, refuse the second, or replace the first?
  4. A exits **without** tearing down explicitly (process death only) — the
     crash/`kill -9` shape plan 31 C4b's harness already exercises for
     `/dev/fuse`.
  For each: the client loop's outcome (data correct / short / `EIO` /
  `ENOTCONN` / hung until timeout), `dmesg` lines from `fuse`, and
  `/sys/fs/fuse/connections/<id>/{waiting,congestion_threshold}` before
  and after.

  *Kernels.* At least two: a **7.3-rc** (the Fedora Rawhide cloud AMI the
  read-path bench used, kernel `7.3.0-0.rc4`; 7.3's `ADD_QUEUE` decoupled
  ring creation from entry registration — "fuse: decouple fuse_ring
  creation from ent registration" in the 7.3 pull — so the answer may
  differ from 6.14's) and a **6.14–6.18** kernel (a Fedora 43 cloud AMI,
  same owner `125523088429`, or Ubuntu 26.04's 7.0 as a middle point).
  Launch them the way `bench/fuse-read-path/RUNBOOK.md` describes (same
  profile, private subnet, security group, key and tags), `enable_uring=Y`
  on both, everything run as root.

  *Deliverables.* `bench/fuse-uring-handover/` with the spike program (or
  the chunkfs extension plus the libfuse patch as a `.patch` file), a
  `run.sh` that executes the four variants and captures the observables,
  a `RUNBOOK.md`, and a `RESULTS.md` holding: the kernel-source reading per
  version (function names, line numbers, the prediction); a table variant
  × kernel → (a)/(b)/(c) outcomes with return codes and `dmesg`; and the
  **decision** it implies for `TransportPolicy` (§3(e)): (c) works →
  `Auto` for handover-capable sessions; (a)+(b) only → `Auto` with the
  resumed session downgraded to `/dev/fuse` and the downgrade logged and
  visible in `node.status`; not even (a) → `DevFuse` for handover-capable
  sessions and `daemon --upgrade` refusing to detach a ring session. The
  coordinator copies that decision into §3(e) and `PROGRESS.md`.
  **Gate**: `RESULTS.md` covers ≥ 2 kernels with all four variants, every
  cell has a recorded outcome (a hang past the timeout is an outcome), and
  the decision paragraph names which of the three policies applies.
  Timebox: if neither route yields a working ring-mode A within a day of
  effort, stop and report what blocked it rather than approximating.

  **Done 2026-09-30** (route 2, `bench/fuse-uring-handover/`, results in its
  `RESULTS.md` and `results/summary-tables.md`). Three kernels (6.17.0,
  7.0.0, 7.3.0-rc4), all four variants, 5 repeats each, every cell with an
  outcome. (a) yes, (b) **no**, (c) yes but lossy: in-flight requests are
  orphaned (unkillable until a fusectl abort), and requests queued in the
  gap wait for unrelated traffic on their queue. The prediction matched the
  observation everywhere except one detail: 7.3's `-ECANCELED` path for
  cancelled dispatches never triggered, and every loss was a hang.
  **Decision:** handover-capable sessions are `DevFuse`, permanently, and
  `detach` refuses a ring session (§3(e)). None of the three pre-listed
  outcomes applied as written. (b)'s "no" rules out the downgrade option,
  and (c)'s loss rules out `Auto`.
- **Z0b — Baselines and verify-once (independent of Z0a; before Z1).**
  The fio-based CPU-s/GiB + RSS gate (§6) is built and run against
  `main`'s current `/dev/fuse`-only path, establishing Constellation's own
  baseline numbers (not the bench's) for later milestones to compare
  against. The transport matrix lane's skeleton is added (currently only
  exercising `dev-fuse`, since nothing else exists yet). §2.3's two
  verify-once changes land (direct memcache admission of fetched bytes;
  the `verified` bit on `Entry` and `get_disk` honouring it under
  `admit`), with the `--cache-verify` flag itself, so the fio gate records
  the baseline before and after them and every later milestone compares
  against the cheaper read path, not the double-hash one. **Gate**: the
  existing fs-core `cache`/`memcache` and engine `view::memcache_tests`
  suites extended with: a fetched chunk is resident in memory without a
  second disk read; a startup-scanned file is hashed on first read and a
  corrupt one is dropped; a verified entry is not re-hashed under `admit`
  and is under `always`; memory ⊆ disk still holds under concurrent
  fetch/remove/prune; the fio gate's cold-sequential daemon CPU s/GiB is
  recorded before and after.
- **Z1 — Vendored ring transport behind the feature, off by default.**
  `patches/0002-io-uring-transport.patch` lands (§3(a)), gated by the
  `io-uring` cargo feature; every REPORTED Skory-fork claim §3(a) depends on
  is re-verified against the actually-vendored code by the fixed rubric
  (test coverage, pjdfstest parity, code size/shape) and the vendor-vs-rewrite
  call is made and recorded. The feature is buildable and the transport
  matrix lane's `auto` leg exists, but `TransportPolicy` still defaults
  every mount to `DevFuse` at this milestone (nothing yet defaults *on*
  except in the lane's explicit test legs) — harness + pjdfstest green on
  both transports when explicitly selected. `NegotiatedInit.transport` and
  the `detach`/`check_resumable()` refusal of non-`DevFuse` sessions (§3(a),
  §3(e)) land here, with the transport, not later.
- **Z2 — Adapter integration, on by default where the kernel offers it.**
  §3(b)'s `ReadReply`/`WriteData`/metrics changes land; `TransportPolicy::Auto`
  becomes the default for non-handover-capable mounts (plain desktop/server
  daemons); handover-capable mounts (CSI, `daemon --upgrade`) stay
  `DevFuse` (Z0a, §3(e)). `node.status`/metrics/docs updated per §5.

  **Z2c (the default flip) is blocked on the ring-queue lock-wait problem
  being settled** (found in the Z2a review). Over a ring every request
  holds its entry until it is answered — including one answered later from
  another thread (a cold read on the completion pool, an offloaded flush, a
  blocking `F_SETLKW`/`flock` on the view's `lock-wait` thread) — and the
  kernel queues a request on the issuing CPU's queue until one of that
  queue's `depth` entries is free; it cannot be answered on another queue
  or over `/dev/fuse` (`fuse_uring_queue_fuse_req`; the reply must be the
  `COMMIT_AND_FETCH` of its own entry). `/dev/fuse` has no such coupling: a
  waiting request holds nothing there. Waits that end by themselves (the
  store, a lease, the session wait) only stall that CPU's requests; a
  blocking lock can **deadlock** it: at depth 8, 8 processes on CPU *k*
  block in `F_SETLKW` on a lock held by *P*; *P*, on CPU *k*, issues a
  `write()` before its unlock; the write waits behind entries only the
  unlock can free, and the mount hangs for every request from CPU *k*.
  Z2a's mitigation (the protocol allows no other answer): a queue lends at
  most `depth - 1` entries to blocking lock requests; a further one on that
  queue is served as non-blocking, granted if free and answered **`ENOLCK`**
  if contended (`RingCommit::reserve_lock_wait` in the vendored fuser; the
  deterministic test is `wire_uring.rs`'s
  `blocking_lock_waits_never_take_a_queues_last_entry`). That removes the
  deadlock at the price of a POSIX-legal but unusual error under a burst of
  contended waits on one CPU. Before `auto` becomes a default Z2c must pick
  one of: (a) accept the `ENOLCK` budget as is (document it as the ring's
  behaviour under `--locks cluster`); (b) keep mounts with cluster locks
  (`FrontendCaps::cluster_locks`, the default with P2P) on `dev_fuse` under
  `auto`, recorded as a fallback reason — which would leave most real mounts
  on `/dev/fuse` and the ring lane needing `--locks local` for coverage;
  (c) a deeper default queue (the budget grows with it, at `queues x depth x
  max_write` of reserved address space); or a combination, e.g. (a) with a
  deeper queue only on mounts with cluster locks.

  **Settled (maintainer, 2026-10-02; implemented by Z2c): option (b) now,
  with a ring opt-in; revisit after Z4.** Under `auto` a mount whose
  frontend has cluster locks resolves to `/dev/fuse`, recorded as the
  fallback reason `cluster_locks` (between `kernel_not_offered` and
  `handover_capable` in the ladder's order: a fallback names the first
  rung that refused). Mounts with `--locks local` (or no P2P) get the ring
  under `auto`. `--fuse-transport uring` / `CONSTELLATION_FUSE_TRANSPORT=uring`
  is the opt-in that puts a cluster-lock mount on the ring anyway, accepting
  the `depth - 1` budget and its `ENOLCK`, with a deeper default queue for
  such mounts (`CLUSTER_LOCKS_URING_QUEUE_DEPTH` = 32: 31 contended waiters
  per CPU instead of 7, for 4x the reserved address space and no measurable
  resident memory — PROGRESS "Plan 38 Z2"). Every downgrade is counted
  (`lock_wait_downgrades` per mount and process-wide in `node.status`,
  `constellation_fuse_lock_wait_downgrades_total`), and the budget is tested
  on a real kernel (`transport-lock-wait-budget`). The default for
  cluster-lock mounts is **to be revisited after Z4's zero-copy numbers**:
  since the harness's and most real daemons run with P2P, `auto` leaves most
  mounts on `/dev/fuse` until then (PROGRESS "Plan 38 Z2" counts how many).
  The mobile profile's default is `dev-fuse` (§4, §8). A read-only snapshot
  mount takes no cluster locks, so `auto` gives it the ring, and Z3's
  passthrough composes with either transport (the backing id is registered
  on the connection's `/dev/fuse` descriptor, which a ring session has
  too; harness `passthrough-on-every-transport`). A `view.mount` with a
  path — on a daemon or a headless `serve` node, CSI engine pods included —
  is a plain mount and follows the same policy. The handoff now carries
  `uring` and a per-mount (absent) queue depth, so `HANDOVER_VERSION` is 6.

  **Decided (project owner, 2026-10-05): yes — option (a) with the deeper
  queue.** The close-out's open question (PROGRESS "Plan 38 — close-out") is
  answered: under `auto` a cluster-lock mount takes the ladder like any plain
  mount, at queue depth 32 unless `--fuse-uring-queue-depth` says otherwise,
  accepting the `depth - 1` lock-wait budget and its `ENOLCK` (counted in
  `lock_wait_downgrades`). The `cluster_locks` fallback rung is removed, with
  no compatibility kept; `uring` is now the same ladder as `auto`.
  Handover-capable mounts and the mobile profile stay `dev_fuse`. What the
  lock scenarios measured under the new default is in PROGRESS,
  "cluster-lock-ring-auto". The 2026-10-02 text above is history.
- **Z3 — Passthrough for single-chunk read-only opens.** §3(c)'s
  eligibility rule, the `Opened`/`View::open` extension, the pin-while-open
  `DiskCache` guard, and the scan-ahead/atime move to `open()` land,
  independent of the ring transport (passthrough needs only 6.9+ and
  `CAP_SYS_ADMIN`, not the `io-uring` feature) — a build without the
  `io-uring` feature still gets passthrough.
- **Z4 — Zero-copy on 7.3+.** §3(d)'s `ADD_QUEUE`/`ADD_BUFPOOL`/`READ_FIXED`
  wiring, the chunk-spanning-read fallback to the memcache path, and the
  7.3 CI lane (SKIP elsewhere) land; `--cache-verify`'s `always` mode is
  wired to disable both this and Z3's passthrough.
- **Z5 — Close-out.** `docs/plans/v1/PROGRESS.md` gets this plan's
  milestone write-up with Constellation's own measured numbers (the fio
  gate's Z0b baseline next to Z1/Z3/Z4's results, per transport); `README.md`/
  `docs/how-to-guides/development/TESTING.md`/`docs/reference/configuration.md`
  (the new knobs, §4) updated; a decision record explicitly reconciling
  this plan's §2.1 row 1 verdict with C7b's own "REJECT splice" (§2's
  "Reconciling with C7b" paragraph, restated in PROGRESS.md's permanent
  record so a future reader doesn't find two REJECT-splice entries and
  wonder if they contradict).

**Which milestones stand alone if a later one slips**: **Z1–Z3 stand
alone** — the ring transport (Z1–Z2) and passthrough (Z3) are independent
of each other (passthrough needs no `io-uring` feature) and each is a
complete, gated, shippable improvement over today's path by itself. **Z4
depends on Z1–Z2** (zero-copy is "behind the transport," §2.1 row 6) and is
the one milestone most exposed to upstream churn (§8) — it is explicitly
the one this plan expects might slip past a release without blocking the
others. **Z5** depends on whichever of Z1–Z4 actually shipped, and writes
up exactly that set, not a promise of all four.

## 8. Risks

- **Draft-quality upstream code.** The Skory/fuser fork is open PRs, not a
  released crate, and the zero-copy userspace side
  (`joannekoong/libfuse`'s `zero_copy_v7`) is an unmerged draft with three
  bugs this plan's own bench run found (§2.1 row 6). Mitigation: Z1's fixed
  rubric re-verifies before vendoring (§3(a)); the `io-uring` feature is
  off by default until Z2 and passthrough (Z3) ships independently of it
  entirely, so a stalled or abandoned upstream fork degrades this plan to
  "passthrough only," not "nothing," and the portable `/dev/fuse` fallback
  is never at risk regardless.
- **Kernel API churn between 6.14 and 7.3, and 7.3's own rc status.** The
  bench itself ran on `7.3.0-rc4` (VERIFIED `bench/fuse-read-path/results/summary-tables.md`'s
  header) — not a released kernel as of the research date — and the 6.14
  feature set that shipped may not be bit-for-bit what 7.3 assumes.
  Mitigation: §2.4's ladder is runtime-negotiated against the *running*
  kernel's actually-agreed `InitFlags` (never assumed from a version
  number), so a kernel that changes an ABI detail between rc and release
  either negotiates correctly or falls back cleanly — the fallback path is
  exercised by the fault-injection scenarios (§6) specifically so "the
  kernel changed something" degrades to `/dev/fuse`, not to a crash.
- **Per-CPU queue imbalance for single readers.** §1/§2's measured
  regression (buffered single-stream sequential, −22% to −25%) is a
  structural property of one-ring-per-CPU, not a bug Z1 can fix.
  Mitigation: zero-copy (Z4) closes this gap for the files it covers
  (§2.1 row 6, "Fixes the transport's buffered-sequential regression"); for
  everything else, the CPU/GiB numbers still improve even when throughput
  regresses (§1), and `TransportPolicy::DevFuse` remains available as a
  per-mount escape hatch (§3(e)) for a workload dominated by single-stream
  sequential reads where the throughput regression outweighs the CPU
  saving.
- **seccomp/containers blocking `io_uring`** — plan 37's CSI engine pods run
  in containers, and `io_uring` is a common seccomp-profile deny-list entry
  (a well-known kernel attack-surface concern independent of this plan).
  Mitigation: exactly what §2.4's ladder and §3(a)'s "graceful fallback"
  are for — a refused `io_uring_setup` is caught and falls back to
  `/dev/fuse`, exercised by the fault-injection lane (§6) under a seccomp
  profile that denies it; plan 37's own pods can additionally pin
  `TransportPolicy::DevFuse` explicitly if a cluster's baseline seccomp
  profile makes detection-and-fallback unnecessary overhead.
- **`CAP_SYS_ADMIN` not available.** Both zero-copy and passthrough need it;
  an unprivileged daemon (a common deployment shape this plan does not
  change) silently gets neither, and §2.4 requires this to be *visible*,
  not silent. Mitigation: the "logged once, reflected in `node.status`"
  requirement (§2.4, §5) is load-bearing here specifically — an operator or
  the CSI driver can see `transport: dev_fuse (uring unavailable:
  CAP_SYS_ADMIN)` rather than inferring it from throughput alone; the bench
  itself documents the unprivileged-daemon case separately (README, "Local
  runs (unprivileged)": pipes below `pipe-user-pages-soft` get 2-page pipes
  that silently fall back for `splice` modes — this plan's own zero-copy/
  passthrough paths are the `CAP_SYS_ADMIN`-gated ones the ladder already
  handles, but the general lesson — "an unprivileged daemon degrades
  silently unless something says so" — is exactly what §2.4/§5 are built to
  avoid repeating).
- **Ring RSS on the mobile profile.** §4 already resolves this as "not
  applicable" for plan 36's Android port (SAF, not `/dev/fuse`) but a Linux
  daemon explicitly run under `EngineProfile::mobile()` (e.g. a constrained
  Linux device, not Android specifically) would still pay the ring budget.
  Mitigation: `TransportPolicy::DevFuse` as the practical default for that
  profile (§4), an explicit operator choice rather than an automatic one
  this plan makes for them — `mobile()`'s existing `BackgroundMode::OnDemand`
  cap on the memcache (16 MiB, `crates/engine/src/profile.rs:163-172`)
  signals memory is already tight on that profile, and nothing about this
  plan should be read as overriding that signal.
- **Trust model.** §2.3's `admit`/`always` split moves real, if well-precedented,
  risk onto the operator's choice. Mitigation: `admit` is not the default by
  silent omission — it is `EngineProfile`/CLI-flag explicit, logged, and
  `always` remains one flag away with zero-copy/passthrough cleanly
  disabled rather than partially engaged, exactly as §2.3 specifies.
- **Handover (resolved by Z0a).** A ring session cannot be handed over
  losslessly or downgraded to `/dev/fuse` (§3(e)). Handover-capable sessions
  are therefore `DevFuse` for good. That costs plan 37 and `daemon --upgrade`
  nothing they have today, but they never get the ring's CPU savings (§1).
  The remaining risk is a *misconfigured* session: a mount started with
  `Auto` that someone later tries to hand over. Mitigation: `detach` refuses
  any non-`DevFuse` session (§3(e)). Without that refusal, a handover under
  load would leave callers unkillable until a fusectl abort. The refusal
  therefore lands with the transport (Z1), and it is a standing harness
  scenario (§6).

## 9. Definition of done

The CONVENTIONS.md gates (fmt/clippy clean, `cargo test --workspace` zero
failures, `tests/smoke.sh`/`tests/integration.sh` pass, `harness run` every
scenario PASSED — fio/stress SKIP acceptable only for a genuinely absent
binary, never for a kernel feature this plan is responsible for degrading
out of gracefully — pjdfstest 8798/8798, `PROGRESS.md`/`TESTING.md`
updated), plus, per milestone (cumulative):

1. **Z0a** (**done** 2026-09-30): `bench/fuse-uring-handover/RESULTS.md` exists with the
   kernel-source reading, the four variants' outcomes on at least two
   kernels (a hang past the timeout counts as an outcome), and the
   `TransportPolicy` decision for handover-capable sessions (any of the
   three outcomes is valid — §3(e) only requires the answer, not a
   particular answer); §3(e) and `PROGRESS.md` carry that decision.
   Result: three kernels, 60 runs; decision `DevFuse` for handover-capable
   sessions plus `detach` refusing ring sessions (§3(e)).
2. **Z0b**: the fio-based CPU-s/GiB + RSS gate exists and runs against
   `main`'s `/dev/fuse`-only baseline; the transport matrix lane's
   skeleton exists; §2.3's verify-once changes and `--cache-verify` land
   with their tests, and the fio gate records cold-sequential daemon CPU
   s/GiB before and after them.
3. **Z1**: `patches/0002-io-uring-transport.patch` exists, hunks marked
   `CONSTELLATION PATCH (io-uring)`, re-applies cleanly via
   `tools/vendor-fuser.sh`; every REPORTED claim §3(a) depends on is
   re-verified by the stated rubric and the vendor-vs-rewrite decision is
   recorded; the `io-uring` feature builds, off by default; harness +
   pjdfstest 8798/8798 green on both transports when explicitly selected
   via `CONSTELLATION_FUSE_TRANSPORT`.
4. **Z2**: `ReadReply`/`WriteData`/metrics/`node.status` changes land per
   §3(b)/§5; `TransportPolicy::Auto` is the default for non-handover-capable
   mounts; the transport matrix lane runs both legs in CI on a 6.14+
   kernel; every downgrade path (§2.4) is covered by a fault-injection
   scenario (§6) and confirmed to log once and appear in `node.status`.
5. **Z3**: passthrough eligibility (§3(c)), the `Opened`/`View::open`
   extension, the pin-while-open `DiskCache` guard, and the scan-ahead/
   atime move all land; the passthrough-specific fault-injection scenarios
   (§6: eviction-while-open, remote-write-invalidation, O_DIRECT behaviour)
   pass; this works independent of the `io-uring` feature being enabled.
6. **Z4**: zero-copy wiring lands behind kernel 7.3 detection and
   `CAP_SYS_ADMIN`; the chunk-spanning-read fallback to the memcache path is
   asserted by a scenario that constructs exactly that case; the 7.3 CI
   lane runs where available and SKIPs loudly elsewhere; `--cache-verify
   always` is confirmed (by a test, not just by design) to disable both
   zero-copy and passthrough.
7. **Z5**: `docs/plans/v1/PROGRESS.md` carries this plan's milestone
   write-up with Constellation's own measured numbers per transport (not
   the bench's, cited as the reference); `README.md`/`TESTING.md`/
   `docs/reference/configuration.md` document every new knob
   (`--cache-verify`, `CONSTELLATION_FUSE_TRANSPORT`,
   `CONSTELLATION_FUSE_URING_QUEUE_DEPTH`); the C7b-reconciliation decision
   record is written; this plan moves from `wip/` to `done/` per
   `docs/plans/v1/README.md`'s "Execution protocol."
8. **Report**: each milestone's report includes the harness summary line,
   the pjdfstest tally, the fio-gate's CPU-s/GiB and RSS numbers next to
   the prior milestone's, and (from Z1 on) which transport legs of the
   matrix lane ran and passed.

## Sources

| Source | Used for | VERIFIED / REPORTED |
|---|---|---|
| `bench/fuse-read-path/results/summary-tables.md` (this repo) | Every numeric claim in §1, §2.1, §4, §8 | VERIFIED (this session's own benchmark run) |
| `bench/fuse-read-path/README.md` (this repo) | Mode/workload definitions, caveats cited throughout §1–§4, §6 | VERIFIED |
| `bench/fuse-read-path/RESULTS.md` (this repo) | The results write-up: every verdict in §2.1, the mechanism explanations (page stealing, the < 8 KiB splice threshold, the per-CPU queue effect), the run-to-run spread caveats | VERIFIED (this session's own run and analysis) |
| `https://www.kernel.org/` (2026-09-30: mainline 7.3-rc5, stable 7.2.8) | §2.4's "7.3 is not released" framing, §8's rc-status risk | VERIFIED |
| `https://raw.githubusercontent.com/torvalds/linux/master/fs/fuse/dev.c` (`fuse_dev_splice_write`, `fuse_try_move_folio`, `fuse_check_folio`) | §2.1 rows 1–2: page stealing conditions, "always copy user pages" | VERIFIED |
| `[GIT PULL] fuse update for 7.3` (`https://ratatoskr.run/linux-fsdevel/2026/08/17457979/t`), `[PATCH v7 0/6] fuse: add io-uring buffer pools` (`https://ratatoskr.run/fuse-devel/2026/08/17414159/t`), Phoronix "Linux 7.3 FUSE" | §3(d)'s zero-copy uapi (`ADD_QUEUE`, `ADD_BUFPOOL`, `FOPEN_IO_URING_ZERO_COPY`, `FUSE_HAS_IO_URING_BUFPOOL`), the 1 MiB buffered-read change, CAP_SYS_ADMIN | VERIFIED (cover letter and pull request are primary) |
| `https://docs.kernel.org/6.14/filesystems/fuse-io-uring.html`, Phoronix "Linux 6.14 FUSE" | §2.4 step 2's kernel floor, `enable_uring`, the register/commit_and_fetch cycle | VERIFIED |
| libfuse master `lib/fuse_lowlevel.c`, `example/passthrough_hp.cc`, PR #1640 (bufpools, merged 2026-09-29) | libfuse's splice defaults, `fuse_req_get_payload`, `fuse_passthrough_open` — the bench's own mechanics (§1, §2.1) | VERIFIED |
| `main` at `3a8b335` (`crates/vfs/src/types.rs`, `crates/frontend-fuse/src/{adapter,reply,session,threads}.rs`, `crates/engine/src/{profile,view/io,view/flush}.rs`, `crates/fs-core/src/{cache,memcache}.rs`, `crates/control/src/{methods,web}.rs`, `crates/engine/src/control/{ops,streams}.rs`) | Every VERIFIED file:line citation in §1, §3, §4, §5 | VERIFIED (`git show`/`git grep` and a direct read of the working tree, this session) |
| `vendor/fuser/{CONSTELLATION-PATCH.md,src/{reply,channel,passthrough,lib,session}.rs}` on `main` | The existing patch series' shape and precedent (§3(a)), `ReplyData`/`ChannelSender::send`/passthrough API (§1, §3(b), §2.1 row 7) | VERIFIED |
| `docs/plans/v1/PROGRESS.md` on `main`, "## Chunk memory cache" and "## Plan 31 C7b" sections | The memcache's design/measurements (§1, §2.1 row 4), the deferral/Mountpoint-candidate findings this plan reconciles with (§2's "Reconciling with C7b") | VERIFIED |
| `docs/plans/v1/CONVENTIONS.md` | Gates, reporting format, harness scenario checklist (§6, §7, §9) | VERIFIED (this repo, `main`) |
| `docs/plans/v1/done/37-kubernetes-csi.md` (lines 1–120) | Style template, VERIFIED/REPORTED convention, the K0 re-verification precedent §3(a)/§7 follow | VERIFIED (this repo, `main`) |
| `docs/plans/v1/wip/31-core-frontend-backend.md` §6.2, §6.4, §6.6, §6.9, §6.11, §11 (C4, C7), §14, §15 | The `Vfs`/`Responder`/`FrontendCaps`/threading contract this plan builds on (§3), the session-handover design and risk (§3(e)), the perf-target and DoD style (§4, §9) | VERIFIED (this repo, `main`) |
| kernel.org, `fs/fuse/dev.c` (`torvalds/master`), the 6.14 FUSE-io_uring kernel doc, the 7.3 FUSE pull request | §2.1 rows 1–2's splice/vmsplice mechanism, §2.4's kernel version gates, §3(a)/§3(d)'s transport/zero-copy mechanism | VERIFIED (per the research notes' own primary-source citations) |
| `bench/fuse-uring-handover/RESULTS.md`, `results/summary-tables.md`, `results/*/summary.jsonl` (this repo) | §3(e)'s handover findings and decision, §6's refusal scenario, §7 Z0a's result, §8's handover risk | VERIFIED (Z0a's own run, 2026-09-30, three kernels) |
| `fs/fuse/dev_uring.c`, `dev.c` at `gregkh/linux` `v6.17.13`, `v7.0.14` and `torvalds/linux@165768bb7026` (= `v7.3-rc5` for `dev_uring.c`): `fuse_dev_release`, `fuse_uring_cancel`, `fuse_uring_do_register`, `fuse_uring_register`, `fuse_uring_cmd`, `fuse_uring_queue_fuse_req`, `fuse_uring_ent_avail`, `fuse_uring_next_fuse_req`, `fuse_uring_commit`, `fuse_uring_send_in_task`, 7.3's `fuse_uring_add_queue`/`add_bufpool` | §3(e)'s mechanisms (why (a)/(b)/(c) come out as they do), with line numbers per version in RESULTS.md | VERIFIED (read before the run, predictions confirmed by it) |
| `github.com/Skory/fuser` (PRs #1–#8, design gist) | §3(a)'s vendoring design | REPORTED — re-verified at Z1 (§7) |
| `joannekoong/libfuse` `zero_copy_v7` branch | §2.1 row 6, §3(d)'s zero-copy userspace state and known bugs | REPORTED — re-verified at Z4 (§7) |
