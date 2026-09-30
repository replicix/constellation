# FUSE read path: zero-copy techniques — results (2026-09-30)

## 1. Question and verdict

Constellation serves every FUSE read out of a local disk chunk cache (4 MiB chunk files,
S3-backed). Today that's `pread` into a buffer, `fuse_reply_buf` → `writev(2)` on
`/dev/fuse`: one userspace copy, one kernel copy, per read. Linux has several
Linux-specific ways to avoid some or all of that copy — `splice(2)`, `vmsplice(2)`,
FUSE-over-io_uring, io_uring zero-copy, FUSE passthrough — plus an in-memory verified
cache tier (`memcache`, on `main` since the plan 31 merge) and plain `mmap`. This
benchmark (`bench/fuse-read-path`, a small libfuse lowlevel filesystem, `src/chunkfs.c`)
measures what each one actually buys over the `copy` baseline on workloads shaped like
Constellation's read path, so plan 38 could decide what to build.

| # | option | verdict | headline evidence (warm unless noted) |
|---|---|---|---|
| 1 | `splice(2)` fd→pipe→/dev/fuse | **REJECT** | seq1 4,808 vs copy 6,704 MiB/s (−28%) with `SPLICE_F_MOVE`; rand128k-aio 2,829 vs 4,321 (−35%); every 4 KiB reply falls back to `writev` (3.6M fallbacks in one `rand4k-1j-dio` cell); `SPLICE_F_MOVE` steals the chunk file's page-cache pages into the FUSE inode (residency 100→75% on `seq1`, 100→0% on `smallfiles`); cold falls back to copy on short splices (4.8k–25.6k fallbacks per cold `seq8` cell). Only win: O_DIRECT `seq1` 10,266 vs 6,954 MiB/s (+48%), which io_uring matches or beats without the fallback storm. |
| 3 | `mmap` + `vmsplice(2)` | **REJECT** | Indistinguishable from `copy` in every cell (`seq1` 6,639 vs 6,704, `seq8` 22,568 vs 22,756, `rand4k-1j-dio` 124k vs 122k IOPS); the kernel always copies user pages on vmsplice (`fs/fuse/dev.c`: "can't control lifetime of pipe buffers"); `SPLICE_F_GIFT` is accepted but nothing is stolen (residency stays 100%). |
| — | `mmap` alone (reply from a mapping, portable) | **not adopted** | ≈ copy on sequential; `rand4k-1j-dio` 191k vs 122k IOPS (+57%), `smallfiles` +14%; loses to memcache on small files (1,631 vs 3,200 MiB/s); the big cold-random wins are a page-fault-around artifact, not real savings; serves bytes the kernel re-reads from disk unverified after eviction. |
| — | memcache (Constellation's in-memory verified tier) | **keep** (already on `main`) | `seq1-dio` 14,372 vs 6,954 MiB/s (2.1×), `smallfiles` 3,200 vs 1,430 (2.2×), `rand4k-1j-dio` 175k vs 122k IOPS; ≈ copy on buffered sequential; `rand128k-aio` −32% (2,942); cold sequential costs 2–3× the daemon CPU (0.64 vs 0.24 s/GiB on `seq1`: whole-chunk loads); RSS tracks the resident working set (capped 128 MiB in Constellation, 8 GiB here). |
| 2a | FUSE-over-io_uring transport (kernel ≥ 6.14) | **ADOPT** — the primary result | `rand4k-8j-dio` 1,510k vs 464k IOPS (3.25×) at 0.78 vs 3.71 daemon CPU s/GiB (4.7× less), p99 8.8 vs 36 µs; `rand4k-1j-dio` 280k vs 122k IOPS (2.3×); buffered `rand4k-8j` same IOPS (served from the FUSE page cache) but daemon CPU 0.63 vs 3.14 s/GiB (5× less); `smallfiles` +52%; `seq1-dio` +51%. Regression: single-stream buffered sequential −25% (`seq1`) and −22% (`seq8`) — io_uring queues a CPU's requests to that CPU's ring, so one reader's readahead is served by one ring thread — but CPU/GiB is still 43% lower. RSS +~130 MiB of ring payload buffers. |
| 2b | io_uring zero-copy (kernel ≥ 7.3, `READ_FIXED` into the request's pages) | **ADOPT as the second step**, behind the transport | `seq1` 6,360 MiB/s (copy −5%) at 0.04 daemon CPU s/GiB (~3.1× less than copy, ~2.7× less than memcache); `seq8` 23,883 (+5%) at 0.11 (2.2× less); `seq1-dio` 18,534 MiB/s (2.7× copy, 1.3× memcache); `rand128k-aio` 4,971 (+15%) at 0.07; `smallfiles` 2,844 (2×); cold CPU/GiB roughly halves (0.12 vs 0.24 on `seq1`). Fixes the transport's buffered-sequential regression. Userspace side is an unmerged draft (`joannekoong/libfuse` `zero_copy_v7`; 3 bugs found by this bench); the fuser fork defers it. |
| 4 | FUSE passthrough (kernel ≥ 6.9, whole backing file per open) | **ADOPT for single-chunk read-only opens** | `seq1` 30,797 MiB/s (4.6×), `seq8` 85,333 (3.7×), `rand128k-aio` 24,381 (5.6×), `smallfiles` 6,564 (4.6×), 0 daemon CPU, 5 MiB RSS — reads never reach the daemon at all. O_DIRECT reads go to the backing disk (`seq1-dio` drops to 520 MiB/s warm) — correct O_DIRECT semantics, but a behaviour change vs. today. Only files that are exactly one cached chunk (≤ chunk size) qualify. |

Priority for Constellation (S3-backed, disk chunk cache, portable frontends still needed):
**io_uring transport > passthrough (single-chunk files) > io_uring zero-copy (7.3+) ≫
mmap > splice ≈ vmsplice (rejected)**. The portable fallback path is unchanged: `ReadData`
(`Bytes`) → fuser `writev` on `/dev/fuse` (works on FreeBSD; NFS/WinFsp frontends
untouched). See `README.md` for the mode/column reference and plan 38
(`docs/plans/v1/wip/38-fuse-read-path-transport.md`) for the full architectural reasoning,
including the trust-model change that zero-copy and passthrough imply (verification moves
from "every read" to "on cache admission").

## 2. Setup and method

**Hardware/OS.** `c8a.4xlarge` (16 vCPU, 30 GiB RAM), Fedora Rawhide, kernel
`7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64`, store filesystem btrfs
(`compress=zstd:1`) on a 60 GiB gp3 root volume (6000 IOPS / 500 MB/s). libfuse built
from master at commit `bf4fdf9df8e2` for every mode except `uring-zc`, which is built
from the unmerged `joannekoong/libfuse` branch `zero_copy_v7` — the only userspace
implementation of kernel 7.3's `FUSE_URING_ZERO_COPY`. fio `fio-3.43`. Chunk size 4 MiB,
matching Constellation's disk cache. Run date 2026-09-30, matrix executed by an operator
following `RUNBOOK.md` end to end (provision → build → smoke → generate the 26.5 GiB
store → run → fetch results); wall time 2h51m for the full matrix.

**Matrix.** 9 workloads (`seq1`, `seq1-dio`, `seq8`, `rand4k-1j`, `rand4k-1j-dio`,
`rand4k-8j`, `rand4k-8j-dio`, `rand128k-aio`, `smallfiles`) × 11 modes (`copy`,
`memcache`, `mmap`, `splice`, `splice-nomove`, `vmsplice`, `vmsplice-gift`, `uring`,
`uring-bufpool`, `uring-zc`, `passthrough`) × {warm, cold} × 3 repeats = **594 cells**,
every one of which finished `ok` with its correctness check passing. Results are medians
over the 3 repeats (`results/summary-tables.md`, regenerated from `results/full.jsonl` by
`summarize.py`); `results/full.md` adds ratios-vs-copy and per-cell notes.

**Correctness, per cell.** Before measuring, `mkdata verify` reads `odd_0` (a file
spanning several chunks with a short last chunk) sequentially and at 300 random
offsets/lengths up to 2 MiB crossing chunk boundaries, both buffered and with O_DIRECT,
plus `small_1`, and `cmp`s `odd_0` against the concatenation of its chunk files. A
mismatch would abort the matrix; none did, across all 594 cells.

**Fallback and silent-downgrade counters.** A mode can silently fail to do what its name
says: libfuse falls back from splice to `writev` on short replies, a vmsplice pipe can be
too small, `uring-zc` can fall back to a non-zero-copy read. `chunkfs` interposes
`writev`/`splice`/`vmsplice` (libfuse calls them through the PLT) and counts, per reply,
which syscall path actually carried it, plus how many reads came over io_uring and how
many were genuinely zero-copy. These become the `fallbacks` column below and the `checks`
warnings in `full.md` — they're why `splice`'s rand4k bandwidth numbers look competitive
while being useless in practice (§4).

**warm vs. cold.** `warm` means the workload's backing chunk files are resident in the
page cache before the run — what a hot Constellation disk cache looks like. `cold` means
`drop_caches` just before the run. For workloads where every byte is read exactly once
(`seq1`, `seq1-dio`, `seq8`, `smallfiles`), cold throughput is flat at the disk's ceiling
— **~570 MiB/s** for a single stream on this gp3 volume, scaling with job count for
`seq8` — so only the daemon-CPU column is informative there. For the random workloads
(`rand4k-*`, `rand128k-aio`) cold throughput is **not** flat: several modes load a whole
4 MiB chunk (or fault in a surrounding range) on first touch and serve the rest of that
chunk's offsets from memory, so cold bandwidth depends heavily on the mode; that's real
data, covered in §4/§5 rather than a "CPU-only" simplification.

**`daemon.cpu_s_per_gib`.** The daemon's own `getrusage(RUSAGE_SELF)` (user+sys) over the
fio measurement window, divided by GiB the daemon actually served — not GiB fio read.
Page-cache hits on the FUSE inode never reach the daemon, so **buffered `rand4k-*` runs
are served out of the FUSE page cache after the first pass** (a fresh mount starts with an
empty FUSE cache, but 30 s of random 4 KiB reads over a 1 GiB file revisits most offsets);
the `-dio` variants bypass the FUSE page cache on every request and are what actually
measures the daemon's read-path cost.

## 3. Results

Columns: mode, bandwidth (MiB/s) or IOPS (kIOPS for 4 KiB random), daemon CPU seconds per
GiB served, daemon peak RSS (MiB), fallback replies, and backing-file page-cache residency
after the run (%). All 11 modes. One representative workload's warm table plus a
CPU-focused cold table per group; the remaining workloads in each group are covered in §4
and in full in `results/summary-tables.md`.

### Sequential buffered — `seq1` (1 job, bs=1M; `seq8` is 8 parallel streams, see §4 for its numbers)

| mode | bw MiB/s | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 6,704 | 0.14 | 8 | 0 | 100 |
| memcache | 6,759 | 0.12 | 4,117 | 0 | 100 |
| mmap | 6,919 | 0.13 | 4,113 | 0 | 100 |
| splice | 4,808 | 0.13 | 5 | 0 | 75 |
| splice-nomove | 6,693 | 0.13 | 5 | 0 | 100 |
| vmsplice | 6,639 | 0.12 | 4,113 | 0 | 100 |
| vmsplice-gift | 6,682 | 0.13 | 4,113 | 0 | 100 |
| uring | 5,007 | 0.08 | 134 | 0 | 100 |
| uring-bufpool | 5,185 | 0.07 | 134 | 0 | 100 |
| uring-zc | 6,360 | 0.04 | 133 | 0 | 100 |
| passthrough | 30,797 | 0.00 | 5 | 0 | 100 |

Cold (bandwidth flat at ~569 MiB/s — CPU is the story):

| mode | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|
| copy | 0.24 | 8 | 0 | 100 |
| memcache | 0.64 | 4,120 | 0 | 100 |
| mmap | 0.23 | 4,113 | 0 | 100 |
| splice | 0.23 | 5 | 0 | 50 |
| splice-nomove | 0.21 | 5 | 0 | 100 |
| vmsplice | 0.25 | 4,113 | 0 | 100 |
| vmsplice-gift | 0.26 | 4,113 | 0 | 100 |
| uring | 0.17 | 134 | 0 | 100 |
| uring-bufpool | 0.17 | 134 | 0 | 100 |
| uring-zc | 0.12 | 133 | 0 | 50 |
| passthrough | 0.00 | 5 | 0 | 100 |

### Sequential O_DIRECT — `seq1-dio` (`seq8` has no O_DIRECT variant)

| mode | bw MiB/s | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 6,954 | 0.11 | 8 | 0 | 100 |
| memcache | 14,372 | 0.06 | 4,117 | 0 | 100 |
| mmap | 7,907 | 0.09 | 4,113 | 0 | 100 |
| splice | 10,266 | 0.08 | 5 | 0 | 100 |
| splice-nomove | 9,267 | 0.09 | 5 | 0 | 100 |
| vmsplice | 8,359 | 0.10 | 4,113 | 0 | 100 |
| vmsplice-gift | 9,570 | 0.09 | 4,113 | 0 | 100 |
| uring | 10,503 | 0.09 | 134 | 0 | 100 |
| uring-bufpool | 11,161 | 0.08 | 134 | 0 | 100 |
| uring-zc | 18,534 | 0.05 | 133 | 0 | 100 |
| passthrough | 520 | 0.00 | 5 | 0 | 100 |

`passthrough` at 520 MiB/s is not a regression in the usual sense: O_DIRECT on a
passthrough fd reads the *backing chunk file* with O_DIRECT, from disk, even though the
chunk is warm (`disk_read_mib` ≈ 4,095 MiB in that cell) — correct O_DIRECT semantics, a
behaviour change from today's always-buffered daemon read.

Cold (flat at ~569 MiB/s):

| mode | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|
| copy | 0.19 | 8 | 0 | 100 |
| memcache | 0.37 | 4,117 | 0 | 100 |
| mmap | 0.17 | 4,113 | 0 | 100 |
| splice | 0.14 | 5 | 0 | 100 |
| splice-nomove | 0.18 | 5 | 0 | 100 |
| vmsplice | 0.19 | 4,113 | 0 | 100 |
| vmsplice-gift | 0.20 | 4,113 | 0 | 100 |
| uring | 0.17 | 134 | 0 | 100 |
| uring-bufpool | 0.17 | 134 | 0 | 100 |
| uring-zc | 0.13 | 133 | 0 | 50 |
| passthrough | 0.00 | 5 | 0 | 0 |

### Random 4 KiB — `rand4k-8j-dio` (8 jobs, O_DIRECT: the headline cell; 1-job and buffered variants in §4)

| mode | kIOPS | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 464.5 | 3.71 | 8 | 0 | 100 |
| memcache | 461.3 | 3.63 | 1,042 | 0 | 100 |
| mmap | 465.6 | 3.61 | 1,041 | 0 | 100 |
| splice | 462.7 | 3.73 | 5 | 13,881,314 | 100 |
| splice-nomove | 458.7 | 3.74 | 5 | 13,761,149 | 100 |
| vmsplice | 466.0 | 3.64 | 1,041 | 0 | 100 |
| vmsplice-gift | 462.7 | 3.64 | 1,041 | 0 | 100 |
| uring | 1,510.1 | 0.78 | 134 | 0 | 100 |
| uring-bufpool | 1,466.2 | 0.81 | 134 | 0 | 100 |
| uring-zc | 1,489.3 | 0.83 | 134 | 0 | 100 |
| passthrough | 6.2 | 0.00 | 5 | 0 | 100 |

Cold:

| mode | kIOPS | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 9.1 | 2.44 | 8 | 0 | 94 |
| memcache | 452.4 | 3.60 | 1,069 | 0 | 100 |
| mmap | 447.2 | 3.58 | 1,041 | 0 | 100 |
| splice | 9.1 | 2.49 | 5 | 273,495 | 93 |
| splice-nomove | 9.1 | 2.49 | 5 | 271,825 | 93 |
| vmsplice | 450.9 | 3.63 | 1,041 | 0 | 100 |
| vmsplice-gift | 451.7 | 3.61 | 1,041 | 0 | 100 |
| uring | 9.1 | 1.45 | 134 | 0 | 93 |
| uring-bufpool | 9.2 | 1.33 | 134 | 0 | 93 |
| uring-zc | 9.1 | 1.46 | 134 | 0 | 47 |
| passthrough | 6.2 | 0.00 | 5 | 0 | 0 |

Not flat: `memcache`/`mmap`/`vmsplice*` read far more than the 4 KiB the kernel asked for
on a miss, so their "cold" IOPS are close to their warm numbers (§4/§5). `copy`, `splice*`
and `uring*` issue exactly the requested 4 KiB and stay disk-bound.

### Random 128 KiB, libaio — `rand128k-aio`

| mode | bw MiB/s | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 4,321 | 0.10 | 8 | 0 | 100 |
| memcache | 2,942 | 0.14 | 1,042 | 0 | 100 |
| mmap | 4,112 | 0.10 | 1,041 | 0 | 100 |
| splice | 2,829 | 0.17 | 5 | 0 | 75 |
| splice-nomove | 2,760 | 0.16 | 5 | 0 | 100 |
| vmsplice | 2,646 | 0.17 | 1,041 | 0 | 100 |
| vmsplice-gift | 2,541 | 0.19 | 1,041 | 0 | 100 |
| uring | 4,321 | 0.10 | 134 | 0 | 100 |
| uring-bufpool | 4,376 | 0.09 | 134 | 0 | 100 |
| uring-zc | 4,971 | 0.07 | 133 | 0 | 100 |
| passthrough | 24,381 | 0.00 | 5 | 0 | 100 |

Cold (not flat — same whole-chunk-load artifact):

| mode | bw MiB/s | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 187 | 0.32 | 8 | 0 | 100 |
| memcache | 758 | 0.47 | 1,042 | 0 | 100 |
| mmap | 731 | 0.31 | 1,041 | 0 | 100 |
| splice | 148 | 0.28 | 5 | 0 | 61 |
| splice-nomove | 191 | 0.24 | 5 | 0 | 100 |
| vmsplice | 723 | 0.22 | 1,041 | 0 | 100 |
| vmsplice-gift | 722 | 0.32 | 1,041 | 0 | 100 |
| uring | 194 | 0.22 | 134 | 0 | 100 |
| uring-bufpool | 194 | 0.22 | 134 | 0 | 100 |
| uring-zc | 194 | 0.20 | 133 | 0 | 50 |
| passthrough | 201 | 0.00 | 5 | 0 | 100 |

### Small files — `smallfiles` (4,096 × 64 KiB files, one short chunk each, sequential open)

| mode | bw MiB/s | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|---|
| copy | 1,430 | 0.34 | 8 | 0 | 100 |
| memcache | 3,200 | 0.17 | 289 | 0 | 100 |
| mmap | 1,631 | 0.29 | 273 | 0 | 100 |
| splice | 1,882 | 0.24 | 5 | 0 | 0 |
| splice-nomove | 1,454 | 0.34 | 5 | 0 | 100 |
| vmsplice | 1,896 | 0.24 | 273 | 0 | 100 |
| vmsplice-gift | 1,333 | 0.38 | 273 | 0 | 100 |
| uring | 2,170 | 0.20 | 134 | 0 | 100 |
| uring-bufpool | 2,207 | 0.19 | 134 | 0 | 100 |
| uring-zc | 2,844 | 0.17 | 133 | 0 | 100 |
| passthrough | 6,564 | 0.00 | 5 | 0 | 100 |

Cold (flat at ~65-68 MiB/s: each file read exactly once):

| mode | cpu s/GiB | maxrss MiB | fallbacks | resid % |
|---|---|---|---|---|
| copy | 0.48 | 8 | 0 | 100 |
| memcache | 0.72 | 289 | 0 | 100 |
| mmap | 0.51 | 273 | 0 | 100 |
| splice | 0.49 | 5 | 0 | 0 |
| splice-nomove | 0.51 | 5 | 0 | 100 |
| vmsplice | 0.54 | 273 | 0 | 100 |
| vmsplice-gift | 0.52 | 273 | 0 | 100 |
| uring | 0.40 | 134 | 0 | 100 |
| uring-bufpool | 0.39 | 134 | 0 | 100 |
| uring-zc | 0.38 | 133 | 0 | 50 |
| passthrough | 0.00 | 5 | 0 | 100 |

## 4. Reading the numbers

**splice / splice-nomove.** `splice-nomove` tracks `copy` closely everywhere — still one
kernel copy, just via `splice` instead of `writev`. `splice` (`SPLICE_F_MOVE`) is worse on
every sequential and 128 KiB workload (`seq1` −28%, `seq8` 20,429 vs 22,756 = −10%,
`rand128k-aio` −35%): when a pipe page lines up with a FUSE page-cache page, the kernel
steals it out of the *chunk file's* page cache (`page_cache_pipe_buf_try_steal` in
`fs/splice.c`, then `fuse_try_move_folio` in `fs/fuse/dev.c`) and hands it to the FUSE inode instead — `backing_resident_after_pct`
drops from 100% to 75% (`seq1`, `seq8`) and to 0% on `smallfiles`. For Constellation that
means every MOVE-mode read would evict a page out of its own disk cache. On 4 KiB reads
it's moot: libfuse never splices replies under two pages by design, so every `rand4k-*`
cell shows every reply falling back to `writev` (fallbacks = daemon.reads, e.g. 3,640,661
of 3,640,661 on `rand4k-1j-dio`/warm, 13,881,314 of the same on `rand4k-8j-dio`/warm) — its
bandwidth there is really `copy`'s numbers with extra bookkeeping. Cold makes it worse:
libfuse's file→pipe splice is non-blocking, so an uncached page returns short and falls
back to pread+writev (4,848 fallbacks on cold `seq8`/splice, 25,644 on cold
`seq8`/splice-nomove, out of ~35-42k replies). The one real win is O_DIRECT sequential
(`seq1-dio` 10,266 vs 6,954, +48%), which io_uring matches or beats without any of this.

**vmsplice / vmsplice-gift.** Statistically indistinguishable from `copy` everywhere
(`seq1` 6,639 vs 6,704, `seq8` 22,568 vs 22,756, `rand4k-1j-dio` 124k vs 122k IOPS):
`fs/fuse/dev.c` always copies user pages on vmsplice because it "can't control the
lifetime of pipe buffers." `SPLICE_F_GIFT` doesn't change that for a page-cache-backed
mapping — the kernel can only steal a page whose refcount is exactly 1, which an mmapped
page-cache page never has, so gift silently degrades to a copy (residency stays 100%
everywhere). File-backed RSS roughly equals the working set (4,113 MiB on `seq1`, matching
`mmap`), since the mapping pins those pages.

**mmap alone.** Same one-copy-on-fault behaviour as vmsplice, without the extra
bookkeeping — the more obvious portable choice. It wins on `rand4k-1j-dio` (191k vs 122k
IOPS, +57%) and `smallfiles` (+14%) but loses to memcache on small files (1,631 vs 3,200),
and its huge cold-random numbers (`rand4k-1j-dio`/cold 217k IOPS vs copy's 1.9k) are a
fault-around artifact, not a real win: a page fault on the mapping pulls in
`read_ahead_kb` worth of surrounding pages, so a "cold" random-4K workload turns warm
within seconds (mmap's residency is 100% in every cold random cell while `copy` stays at
20-94%). It also serves bytes the kernel re-reads from disk unverified after the mapping
is evicted — the trust-model note in §1 applies here too.

**memcache.** Consistent win on O_DIRECT and small files — `seq1-dio` 2.1×, `smallfiles`
2.2×, `rand4k-1j-dio` 175k vs 122k IOPS — because the memory tier skips the disk read and
the BLAKE3 hash entirely on a hit. Flat on buffered sequential (already served from the
FUSE page cache) and loses on `rand128k-aio` (−32%): a miss loads and hashes the *whole*
4 MiB chunk regardless of how much of it the kernel asked for, so the daemon does more
work per request than `copy`'s direct pread. Most visible cold: `seq1`/cold daemon CPU is
0.64 s/GiB vs `copy`'s 0.24 (2.7×), because every miss triggers a full chunk read+hash. RSS
shows up as anon memory (the resident working set), unlike mmap/vmsplice's file-backed RSS.

**FUSE-over-io_uring transport (2a, the primary recommendation).** Wins on every O_DIRECT
workload, and it's a CPU story as much as throughput: `rand4k-8j-dio` 464k → 1,510k IOPS
(3.25×) while daemon CPU per GiB drops 4.7× (3.71 → 0.78 s/GiB) and p99 drops 4×
(36 → 8.8 µs) — moving read/writev off the syscall path into `fuse_req_get_payload()`
removes both the `read()` off `/dev/fuse` and the `writev()` reply. The regression is
single-stream buffered sequential: `seq1` −25%, `seq8` 17,770 vs 22,756 = −22%. The kernel
queues a request to the ring belonging to the CPU that issued it (one ring per CPU), so
one sequential reader's readahead chain is served by a single ring thread, where
`/dev/fuse` spreads the same readahead across the whole worker pool. Even there, daemon
CPU per GiB is still 43% lower (0.08 vs 0.14) — the work is cheaper, just less parallel for
that access pattern. RSS goes up ~126 MiB (8 → 134 MiB): one queue per CPU, each holding
`--uring-q-depth` (8) × `max_write` (1 MiB), ~128 MiB on 16 CPUs. `uring-bufpool` (7.3's
pooled buffers) tracks `uring` closely everywhere; it doesn't change the picture.

**io_uring zero-copy (2b, the second step).** A further CPU cut once the transport is in
place: `seq1` daemon CPU drops to 0.04 s/GiB, roughly 3.1× less than `copy` (0.14) and
2.7× less than `memcache` (0.12), at bandwidth within 5% of `copy`. `seq1-dio` more than
doubles `copy`'s bandwidth (18,534 vs 6,954, 2.7×) and beats `memcache` by 1.3×. It's the
only option that fixes the transport's buffered-sequential regression: `seq8` goes from
`uring`'s 17,770 back up to 23,883 (+5% over `copy`) at under half the CPU (0.11 vs 0.25)
— `READ_FIXED` reads straight into the client's registered pages regardless of which ring
they arrive on, so the per-CPU-queue effect above stops mattering. Cold daemon CPU roughly
halves vs `copy` (`seq1`/cold 0.12 vs 0.24). On `rand4k-*-dio` its numbers track the plain
transport's within a few percent rather than beating it — the transport already removed
the syscalls zero-copy would otherwise save on a 4 KiB reply. Least mature option: the
userspace side only exists in the unmerged `zero_copy_v7` draft, which this bench found
three bugs in (§5).

**Passthrough (4).** The ceiling — the daemon is out of the loop entirely: 0.00 CPU s/GiB
and 5 MiB RSS in every cell, 3.7-5.6× `copy`'s bandwidth on sequential and 128k-random,
4.6× on small files. The catch is `seq1-dio`: 520 MiB/s, *below* `copy`'s buffered number,
because O_DIRECT on a passthrough fd reads the backing chunk file with O_DIRECT too —
straight from disk, bypassing the page cache even though the chunk is warm (`disk_read_mib`
≈ 4,095 MiB in that cell). Correct POSIX semantics, a real behaviour change from today's
always-buffered daemon read, and it only applies to files that are exactly one chunk.

## 5. Findings that matter beyond the numbers

- **Kernel 7.3 sends 1 MiB buffered reads, not 128 KiB.** `daemon.req_size_hist` for
  `seq1`/warm/`copy` is `{"1M": 4096}` — every request was exactly 1 MiB. 7.3 lifted the
  historical 128 KiB buffered-readahead cap (`max_pages` = 256, set from libfuse's
  `max_write` = 1 MiB); on 7.0 the same workload arrives as ≤256 KiB requests. Anything
  sized around the old 128 KiB limit needs re-checking against 7.3+.
- **libfuse does not enable `FUSE_CAP_SPLICE_WRITE`/`MOVE` by default.** `do_init` only
  turns on `SPLICE_READ` unless the daemon explicitly requests write/move — `chunkfs`'s
  `splice` mode does request them (`negotiated.splice_write`/`splice_move` = 1 there),
  which is why the page-stealing and fallback behaviour above is visible at all. A daemon
  that doesn't ask for these flags gets silent `writev()` with no signal anything different
  was tried.
- **`zero_copy_v7` has three known bugs** found while building this bench (`README.md`,
  `RUNBOOK.md`): `-o io_uring_zero_copy` writes an int through a bool and zeroes
  `io_uring_q_depth` unless that option comes later on the command line; the daemon aborts
  with `Received invalid commit_id=0` at unmount (stats are still recorded, written before
  unmount); and the zero-copy flag isn't handed back on `READ`, so `chunkfs` keeps it in
  the file handle itself. Treat the 2b numbers as "what the mechanism can do," not "what's
  shippable today."
- **io_uring ring memory is a fixed, visible RSS cost.** ~126 MiB above baseline on this
  16-CPU box (`uring`/`uring-bufpool`/`uring-zc` ≈133-134 MiB maxrss vs `copy`'s 5-15 MiB),
  from one queue per CPU × queue depth (8) × `max_write` (1 MiB). Tunable via
  `--uring-q-depth`, but committed per mount regardless of load, on top of fuser's existing
  16 MiB-per-worker `/dev/fuse` read buffers.

## 6. Caveats

- **Release-candidate kernel.** `7.3.0-0.rc4` — the io_uring zero-copy and
  buffered-readahead changes this bench measures landed in the 7.3 merge window days
  before this run; none of it has shipped in a stable kernel yet.
- **btrfs, not Constellation's real deployment target.** The store lives on btrfs
  (`compress=zstd:1`); checksums, compression and large-folio behaviour are btrfs-specific,
  and cold-read latency and the page-stealing residency percentages would differ on
  ext4/xfs.
- **Run-to-run spread.** Medians come from only 3 repeats. Computing max/min bandwidth
  spread per cell, the following **warm, buffered** (non-`-dio`) cells exceed 15% spread
  across repeats — treat their medians as directional:

  | workload/mode | repeat values (MiB/s) | spread |
  |---|---|---|
  | `smallfiles` / memcache | 3,240.5, 1,790.2, 3,200.0 | 81% |
  | `seq8` / passthrough | 93,090.9, 85,333.3, 55,351.4 | 68% |
  | `smallfiles` / splice-nomove | 1,369.0, 2,169.5, 1,454.5 | 59% |
  | `smallfiles` / splice | 1,422.2, 2,098.4, 1,882.4 | 48% |
  | `smallfiles` / mmap, vmsplice-gift, vmsplice | 1,326-1,855 / 1,326-1,842 / 1,707-2,032 | 40/39/19% |
  | `seq1` / passthrough | 31,030.3, 30,797.0, 25,924.1 | 20% |
  | `seq8` / uring | 15,693.5, 17,770.1, 18,492.1 | 18% |
  | `seq1` / memcache | 6,759.1, 6,747.9, 7,831.7 | 16% |

  Every offender is either `smallfiles` (4,096 tiny opens, most sensitive to scheduling
  noise) or a `passthrough`/`uring`-class mode at the top of the throughput range, where
  small absolute differences are large percentages. The random-4K and O_DIRECT cells
  behind the io_uring headline numbers are not in this list — their repeats agree to a few
  percent (see `results/full.jsonl`).
- **`cpu_s_per_gib` excludes fio's and the kernel's own CPU.** Passthrough's "0.00 daemon
  CPU" moves that work to the kernel's page-cache/backing-file path, not off the machine;
  read it with `sys_cpu_busy_pct` when comparing options that shift work between the
  daemon, fio and kernel threads.
- **Single box, single run.** No cross-instance-type or cross-kernel comparison; absolute
  numbers are specific to this `c8a.4xlarge` and this kernel build.

## 7. How to reproduce

`README.md` documents the full mode/workload/column reference and how to build and run
the suite locally (unprivileged, without cold-cache or io_uring/passthrough modes) or as
root. `RUNBOOK.md` is the exact procedure used for this run: provision a Fedora Rawhide
box, `./build.sh --zc`, smoke-test every mode, generate the 26.5 GiB store, then
`sudo ./run.sh --results results/full.jsonl matrix` for the full 594-cell matrix
(~2h51m), followed by `./summarize.py results/full.jsonl > results/full.md` to regenerate
the tables from the raw JSONL.
