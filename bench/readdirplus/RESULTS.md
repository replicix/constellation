# Plan 40 — READDIRPLUS and name-ordered listings: benchmark results

Status: **measured, 2026-10-31; the rerun of 2026-11-05 (items 2, 5, 6, 10M, `always`, strict, handoff, trees on main `64409f78`) follows this paragraph and supersedes the "not run" lists**; the paragraph below is the 10-31 run, on the quiet timing host, against main at `7538c07f` (the held-names change
`8f5af546` is in it). The plan's §8 matrix was run in part; "What was not run" lists the rest. The raw cell files
are not kept in the repository; every table below is `report.py`'s output on them.


## Summary: §8 acceptance as amended (2026-11-05)

Final verdict per item of the amended §8 (plan 40, `docs/plans/v1/done/40-readdirplus.md`, §8 and §8.1). Sources: the
10-31 run (listings, requests, page costs) and the 11-05 rerun below (write path, pins, resources, trees). Ratios are
`auto` over R0 (`base`) or over `off` as stated; below 1 is faster.

| Item | Amended bound | Measured | Verdict |
|---|---|---|---|
| 1 Plain listing | faster than R0 at 10k+; 1k: `off` in the noise, `auto` within +3 ms | 10k 0.49-0.53, 100k 0.05-0.09; 1k `off` 0.79-0.90, `auto` +2.5 to +2.8 ms | met |
| 2 Write path | storm and `tar x` in the noise of R0 | storm `auto`/`off` median 1.03 (0.82-1.21); `tar x` write-back 0.92-1.11, write-through 0.99-1.18 | met |
| 3 Requests | `ls -l`/`-la` at 1k, 10k: at least 90 % fewer, not counting the per-name `listxattr` | 99.4 % (1k), 99.8 % (10k), 99.9 % (100k); with `listxattr` 49.8-49.9 % | met as amended |
| 4 Time | `ls -l`, `ls -la`, `rsync -n` at least 2x at 10k and 100k; `find -ls`, `du -s` at least 1.15x at 100k, at most 1.3x at 10k; second `ls -l` at most 1.4x (1k, 10k), 2.8x faster at 100k; trees bounded | `ls -l` 0.37-0.44, 0.34-0.39; `rsync` 0.27-0.29, 0.16-0.17; `find -ls`/`du -s` 0.71-0.84 (100k), 0.86-1.30 (10k); second `ls -l` 1.03-1.35, 0.27-0.35; `git status` 1.01-1.04 cold, 0.99-1.12 warm; `find -type f` 1.48-1.67; `make -q` warm 1.40-1.62; `ls -lR` 0.45-0.51; `rsync -n` 0.26-0.33 | met as amended |
| 5 Resources | R0 + hint cap + refs x 170 B + pinned (1M, 10,000 open dirs); 10M reported; CPU at most R0 | 1M `ls -l` 343-347 MiB (R0 728-738); 10,000 dirs +212-267 MiB against a bound of +189 over R0 (275-287 MiB); 10M `ls -l` 1,703 MiB (about 175 B per ref); 8.05 B pinned per name; CPU 15.0 s against R0's 149.9 s | met as amended; **the 32 B per reference of the first draft was wrong: measured 120-170 B** |
| 6 Pins | zero `lost_cookies`; `releasedir` p99 under 1 ms; handover at most +1 s at 1M | zero on four file systems; p99 17-24 us; 0.49-0.60 s at 1M (10M reported: 4.25 s, handoff 169 MiB) | met |
| 7 Page cost | O(page); cold resume at most one R0 page | pages 0.8-0.9 ms from 10k to 1M (1M over 100k 1.05x); resume 1.4-1.5 ms at 1M, 1.2 ms at 10M (R0: 275 ms at 1M) | met in substance; the literal "1M within 2x of 1k" is not (a 1k directory has no full page) |

`auto` stays the default. Not run: an LTS kernel without `dabb90391028`; `base` and `off` at 10M; two nodes other than
strict mode. One node, one host (four file systems).

## Rerun 2026-11-05 (P40-01): items 2, 5, 6, the 10M cell, `always`, strict mode, handoff size, trees on current main

Measured on the same quiet timing host (16 cores, 30 GB, Linux 7.3.0-rc4, ring transport), main `64409f78` (`auto`,
`off`, `always`) against `base` = `73ea9987` as before, built on the host. One node. State dir and file store on the
named file system; the harness runs (`readdir-huge-delete`, strict) keep their state there and use the harness's S3
container. Scripts: this directory's; `cell.py` gained an `opendirs` phase, `BIG_WRITE_MODE`, a populate RSS sampler and a
`root` store, `listtool.py` a `holdmany` mode, `report-rerun.py` prints the tables below, and the harness a
`readdirplus-strict-bench` scenario. Every cell gated on the 1-minute load below 4 and none gave up. **No correctness
failure appeared in any run**: all four `readdir-huge-delete` runs passed with zero `lost_cookies` and zero `cold_scans`,
every handover returned 0 and every listing after the 10M populate was complete.

What was cut or differs from the plan: **the 10M cell ran once, on the host's system disk (btrfs), in write-through
mode, not on ext4** (see "The 10M cell"); `base` and `off` were not run at 10M (a `base` page costs 225-270 ms at 1M and
grows with the directory: one `base` `ls -f` of 10M would take about three hours); the write-path cells ran once per
version and file system and twice for `auto`, with one `tar x` per mode instead of three (one write-through `tar x` on
`base` takes 5-16 minutes); the pins, handover and open-directory rows have no `base` baseline except the RSS with open
directories, because `base` has no pins.

### Item 2: write path (100,000-entry create/rename/unlink storm in one directory, `tar x` of the 50k-file tree; wall seconds)

**write-through**

| fs | workload | base | off | auto (run 1) | auto (run 2) | auto/off | auto/base |
|---|---|---|---|---|---|---|---|
| ext4 | storm_create | 21.9 | 15.5 | 18.1 | 16.5 | 1.12 | 0.79 |
| ext4 | storm_rename | 19.1 | 19.0 | 18.0 | 17.1 | 0.92 | 0.92 |
| ext4 | storm_unlink | 13.8 | 12.8 | 16.1 | 14.5 | 1.19 | 1.11 |
| ext4 | untar | 764.5 | 322.3 | 340.4 | 331.5 | 1.04 | 0.44 |
| xfs | storm_create | 10.9 | 10.9 | 12.1 | 11.7 | 1.09 | 1.10 |
| xfs | storm_rename | 12.7 | 14.7 | 14.0 | 13.8 | 0.94 | 1.09 |
| xfs | storm_unlink | 12.6 | 10.9 | 11.9 | 12.4 | 1.11 | 0.97 |
| xfs | untar | 946.1 | 191.9 | 230.5 | 222.2 | 1.18 | 0.24 |
| btrfs | storm_create | 20.5 | 20.6 | 19.0 | 15.2 | 0.83 | 0.84 |
| btrfs | storm_rename | 14.9 | 22.3 | 24.1 | 14.6 | 0.87 | 1.30 |
| btrfs | storm_unlink | 24.8 | 24.0 | 24.7 | 24.2 | 1.02 | 0.99 |
| btrfs | untar | 908.7 | 253.2 | 254.0 | 280.3 | 1.06 | 0.29 |
| tmpfs | storm_create | 9.9 | 11.2 | 9.5 | 10.6 | 0.89 | 1.01 |
| tmpfs | storm_rename | 14.1 | 13.3 | 14.6 | 14.4 | 1.09 | 1.02 |
| tmpfs | storm_unlink | 11.9 | 12.4 | 11.5 | 11.8 | 0.94 | 0.98 |
| tmpfs | untar | 318.0 | 71.2 | 69.4 | 72.2 | 0.99 | 0.22 |

**write-back**

| fs | workload | base | off | auto (run 1) | auto (run 2) | auto/off | auto/base |
|---|---|---|---|---|---|---|---|
| ext4 | storm_create | 16.9 | 16.2 | 17.6 | 20.2 | 1.17 | 1.12 |
| ext4 | storm_rename | 16.2 | 18.7 | 18.3 | 18.2 | 0.98 | 1.12 |
| ext4 | storm_unlink | 16.9 | 14.2 | 15.9 | 13.5 | 1.03 | 0.87 |
| ext4 | untar | 268.1 | 242.4 | 231.0 | 229.9 | 0.95 | 0.86 |
| xfs | storm_create | 10.8 | 10.0 | 11.0 | 10.6 | 1.08 | 1.00 |
| xfs | storm_rename | 13.1 | 15.1 | 14.9 | 15.2 | 1.00 | 1.15 |
| xfs | storm_unlink | 13.1 | 12.4 | 13.0 | 11.7 | 1.00 | 0.94 |
| xfs | untar | 148.8 | 139.6 | 146.7 | 148.9 | 1.06 | 0.99 |
| btrfs | storm_create | 21.3 | 21.9 | 21.4 | 14.5 | 0.82 | 0.84 |
| btrfs | storm_rename | 18.2 | 28.5 | 24.5 | 26.0 | 0.89 | 1.39 |
| btrfs | storm_unlink | 28.2 | 20.3 | 23.6 | 25.6 | 1.21 | 0.87 |
| btrfs | untar | 213.9 | 258.0 | 243.7 | 228.8 | 0.92 | 1.10 |
| tmpfs | storm_create | 11.6 | 10.7 | 12.2 | 10.3 | 1.05 | 0.97 |
| tmpfs | storm_rename | 14.8 | 13.3 | 13.2 | 15.0 | 1.06 | 0.96 |
| tmpfs | storm_unlink | 13.8 | 11.5 | 12.7 | 12.2 | 1.08 | 0.90 |
| tmpfs | untar | 30.5 | 26.8 | 30.4 | 29.1 | 1.11 | 0.97 |

**The listing change costs nothing visible in the write path.** The storm is the same within the host's noise:
`auto`/`off` over the 24 storm cells has a median of 1.03 (0.82-1.21), `auto`/`base` 0.99 (0.79-1.39); two `auto` runs
of one cell differ by 7 % at the median and up to 65 % (btrfs). `tar x`, write-back: `auto`/`off` 0.92-1.11,
`auto`/`base` 0.86-1.10. `tar x`, write-through: `auto`/`off` 0.99-1.18 (xfs 1.18, 192 s against 222-231 s), but **`base`
is 2.3 to 4.9 times slower than both `off` and `auto`** (765-946 s against 190-340 s on ext4, xfs and btrfs; tmpfs 318
against 70 s). That is not plan 40: `off` (the same build, the old listing) is as fast as `auto`, so something between
`73ea9987` and main made small-file write-through creation faster. The comparison that isolates the listing change is
`auto` against `off`, within the noise. One run per cell, so no IQRs; the spread of the two `auto` runs is the noise
estimate.

### Item 6: pins

`readdir-huge-delete` (1,000,000-entry directory, `rm -rf` under the 1 MiB hint cap with 20 filler directories of 20,000
names listed in a loop, `find -ls` beside it; the plan's fillers hold 200,000):

| fs | created (s) | rm -rf (s) | lister rounds | pinned_dirs max | lost_cookies | cold_scans | result |
|---|---|---|---|---|---|---|---|
| ext4 | 244 | 159 | 522 | 2 | 0 -> 0 | 0 -> 0 | PASSED |
| xfs | 193 | 157 | 505 | 2 | 0 -> 0 | 0 -> 0 | PASSED |
| btrfs | 197 | 159 | 515 | 2 | 0 -> 0 | 0 -> 0 | PASSED |
| tmpfs | 190 | 160 | 516 | 2 | 0 -> 0 | 0 -> 0 | PASSED |

Zero `lost_cookies` and zero `cold_scans` on all four. `rm -rf` took 157-160 s on each, about 160 us per entry. The
sampler (every 250 ms) saw `hint_bytes` at 0 whenever it looked, so the pressure on the hint cache was lighter than the
cap suggests; the scenario's conditions held all the same.

`releasedir` (`closedir` timed by the caller while a cold scan of the same directory runs, 2,000 opens):

| directory | file system | p50 (us) | p99 (us) | max (us) |
|---|---|---|---|---|
| 1M | ext4 | 16 | 24 | 43 |
| 1M | xfs | 10 | 17 | 25 |
| 1M | btrfs | 16 | 23 | 30 |
| 1M | tmpfs | 10 | 17 | 41 |
| 10M | btrfs (system disk) | 15 | 21 | 892 |

p99 is 17-24 us against the 1 ms bound. The 1,000 releases of the 10,000-directory cell below: p99 2-3 us, max 170-320 us.

Handover (`daemon --upgrade` over a `/dev/fuse` session with one stream open at the middle of the directory, so one
pinned directory; wall time of the whole command):

| directory | pinned bytes (per listed name) | upgrade wall | largest memfd | RSS with the pin |
|---|---|---|---|---|
| 1M, ext4 | 4,026,215 (8.05 B) | 0.60 s | 30 MiB | 129 MiB |
| 1M, xfs | 4,026,215 (8.05 B) | 0.52 s | 30 MiB | 124 MiB |
| 1M, btrfs | 4,026,215 (8.05 B) | 0.51 s | 30 MiB | 129 MiB |
| 1M, tmpfs | 4,026,215 (8.05 B) | 0.49 s | 30 MiB | 124 MiB |
| **10M, btrfs (system disk)** | 40,250,515 (8.05 B) | **4.25 s** | 169 MiB | 592 MiB |

At 1M the whole command is under 1 s on every file system, so the "at most 1 s added" bound holds with room. **At 10M
it took 4.25 s**; the pin is 38 MiB of the 169 MiB handoff, the rest is the node's state for 10M entries. Reported, as
the plan asks; there is no unpinned 10M handover to subtract.

### 1M and 10M directories (median of the runs; kernel-cold)

| cell | version | populate (s) | ls -f (s) | ls -f cpu (s) | ls -f RSS (MiB) | ls -l (s) | ls -l cpu (s) | ls -l RSS (MiB) | cold resume (ms) | cold-scan ref ls -f (s) |
|---|---|---|---|---|---|---|---|---|---|---|
| btrfs 1M | auto | 323 | 0.52 | 0.3 | 322 | 20.8 | 15.3 | 347 | 1.5 | 0.55 |
| ext4 1M | auto | 212 | 0.50 | 0.3 | 413 | 17.2 | 12.7 | 343 | 1.5 | 0.52 |
| tmpfs 1M | auto | 149 | 0.48 | 0.3 | 226 | 13.9 | 9.7 | 346 | 1.4 | 0.50 |
| xfs 1M | auto | 154 | 0.49 | 0.3 | 268 | 17.9 | 11.6 | 344 | 1.3 | 0.50 |
| btrfs 1M | base | 384 | 117.11 | 117.0 | 1001 | 163.0 | 154.5 | 738 | 280.8 | 118.03 |
| ext4 1M | always | 213 | 3.86 | 3.1 | 438 | 22.0 | 15.6 | 347 | 3.3 | 4.37 |
| tmpfs 1M | base | 148 | 117.05 | 116.9 | 683 | 166.2 | 156.0 | 732 | 278.8 | 117.27 |
| xfs 1M | base | 147 | 100.58 | 100.3 | 746 | 138.1 | 129.9 | 728 | 238.6 | 99.04 |
| root 10M | auto | 4428 | 6.12 | 6.6 | 1138 | 231.8 | 177.9 | 1703 | 1.2 | 5.65 |

At 1M `auto` lists in 0.48-0.52 s where `base` takes 100-117 s and `ls -l` takes 14-21 s against 138-166 s; `base`'s cold
resume costs 239-281 ms where `auto`'s costs 1.3-1.5 ms; peak RSS of `ls -l` is 343-347 MiB against `base`'s 728-738 MiB.
`always` (ext4): `ls -f` 3.9 s (7.7 times `auto`: every `getdents`-only walk pays plus pages), `ls -l` 22.0 s (1.28 times
`auto`), cold resume 3.3 ms, RSS as `auto`'s. `base` on xfs, btrfs and tmpfs behaves as the ext4 `base` of 10-31 did
(one page 225-270 ms at 1M; ext4 earlier 261 ms).

### Item 5: resources at 10M and with 10,000 open directories

10M, `auto`, btrfs system disk: peak RSS of `ls -f` 1,138 MiB (one plus page of 721 entries, 4,068 requests) and of `ls -l`
**1,703 MiB** (10,000,001 `kernel_refs`, 13,838 plus pages, 10,014,289 requests, 232 s wall, 178 s daemon CPU); 592 MiB
with the pinned stream alone. The bound R0 + hint cap + `kernel_refs` x 32 B + pinned bytes cannot be evaluated at 10M
(no R0 number there). At 1M it can and holds for `ls -l`: R0 peaks at 728-738 MiB, `auto` at 343-347 MiB.

10,000 directories open at once (10,000 directories of 100 names under one parent, one stream each with one name read;
RSS taken when the 10,000th is open; "delta" = RSS at 10,000 open minus RSS before the first `opendir`):

| fs | version | RSS idle (MiB) | +1,000 open | +10,000 open | delta at 10,000 (MiB) | kernel_refs at 10,000 | pinned bytes | pinned dirs | releasedir p50 / p99 / max of 1,000 (us) |
|---|---|---|---|---|---|---|---|---|---|
| ext4 | base | 76 | 85 | 173 | 98 | 10001 | - | - | 2/3/265 |
| ext4 | auto | 80 | 108 | 308 | 228 | 1010001 | 8900000 | 10000 | 1/2/322 |
| xfs | base | 442 | 451 | 528 | 86 | 10001 | - | - | 2/3/226 |
| xfs | auto | 492 | 510 | 704 | 212 | 1010001 | 8900000 | 10000 | 1/3/201 |
| btrfs | base | 479 | 488 | 575 | 96 | 10001 | - | - | 1/3/168 |
| btrfs | auto | 87 | 113 | 354 | 267 | 1010001 | 8900000 | 10000 | 1/3/167 |
| tmpfs | base | 361 | 374 | 453 | 92 | 10001 | - | - | 2/4/250 |
| tmpfs | auto | 493 | 522 | 716 | 223 | 1010001 | 8900000 | 10000 | 1/2/251 |

`auto` takes **212-267 MiB** for 10,000 open directories against `base`'s 86-98 MiB. The bound is R0 + hint cap (16 MiB) +
`kernel_refs` x 32 B (1,010,001 refs, 31 MiB: the plus page of the first read of each directory instantiates all its 100
names) + pinned bytes (8.5 MiB) = **about 140-155 MiB**; the measurement exceeds it by 60-110 MiB. Read the other way the
daemon holds roughly **120-170 B per kernel reference** (hint cache included), not 32. At 1,000 open directories `auto`
adds 18-29 MiB over idle against `base`'s 9. The idle RSS before the first `opendir` differs between cells (76-493 MiB,
independent of the mode), which is why the delta is the figure to read. After all streams close, pinned bytes and
pinned directories return to 0 and RSS stays at its peak.

### The 10M cell

**Not on ext4.** The test partition has 3.24M inodes and 49 GB and the file store spends one inode and about 5 KB per
entry; the first attempt was stopped by me at 1.9M inodes and 10 GB used after 15 minutes (projected need 10M inodes,
50 GB). It ran on the host's system disk (btrfs, no inode limit).

**Populating 10M entries in one directory strains the daemon in both write modes** (16 threads creating empty files, 4-5k
creates/s at first; not a plan 40 item, reported because it decides whether the cell is runnable). Write-back: the
daemon's RSS grew about 3.3 KB per entry to 17-18 GB at 5.5M entries, `fjall` journal commits took up to 5 s under the
journal lock, one `create` stalled for 610 s and the host went into reclaim (51 % I/O wait, load 19); I stopped it at
5.52M entries. Write-through: the populate completed (4,428 s, 10M entries), the daemon at 12.7 GiB after 10 minutes, 17
GiB at 6.6M entries, a peak of 21.5 GiB (host load 16-25 throughout). Latency and memory, not corruption: the listings,
pin and handover after the populate were exact. The memory per created entry belongs to the create path, not the listing;
noted for the maintainer.

### §9: strict mode and the handoff size

- **Handoff size**: the largest memfd the daemon held during the upgrade with the pinned directory: 30 MiB at 1M (all four
  file systems), 169 MiB at 10M; the pin itself is 4.0 MB and 40 MB (8.05 B per listed name, as estimated). The plan's
  "~80 MB for a pinned 10M directory" is twice what was measured.
- **Strict mode** (`--cto strict` with a second node, harness S3 container; A remounted for each run so it is
  kernel-cold, B idle; 10 runs per cell, `off` and `auto` in alternating scenario runs):

| directory | listing | mode | wall (ms, median of 10) | plus pages | plus entries with attributes | name-only entries |
|---|---|---|---|---|---|---|
| 1,000 | `ls -f` | off | 4.5 | 0 | 0 | 0 |
| 1,000 | `ls -f` | auto | 5.0 | 1 | 0 | 744 |
| 1,000 | `ls -l` | off | 3,351 | 0 | 0 | 0 |
| 1,000 | `ls -l` | auto | 3,349 | 3 | 0 | 1,002 |
| 10,000 | `ls -f` | off | 8.9 | 0 | 0 | 0 |
| 10,000 | `ls -f` | auto | 9.0 | 1 | 0 | 744 |
| 10,000 | `ls -l` | off | 33,750 | 0 | 0 | 0 |
| 10,000 | `ls -l` | auto | 33,579 | 15 | 0 | 10,002 |

  With a second node every plus entry is name-only, so `auto` saves nothing for `ls -l` (the kernel looks each name up,
  3.3 ms an entry through the S3 path) and its larger first page costs +0.5 ms at 1k and +0.1 ms at 10k for `ls -f`
  against `off`. `auto` is not slower than `off` under strict beyond that; resolving the knob to `off` there would save
  at most half a millisecond a listing. Keep `auto`.

### Trees on current main, `auto` over `off`, cold and warm

Three `pair` cells per file system (18 runs per mode and state), the replica warm as before:

| workload | state | ext4 off | ext4 auto | ext4 ratio | xfs off | xfs auto | xfs ratio | btrfs off | btrfs auto | btrfs ratio | tmpfs off | tmpfs auto | tmpfs ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ls_lR | cold | 2633.1 (475.2) | 1355.4 (213.5) | 0.51 | 2646.5 (264.7) | 1346.1 (182.7) | 0.51 | 2209.4 (494.6) | 1117.0 (148.8) | 0.51 | 2540.8 (137.0) | 1165.0 (231.3) | 0.46 |
| ls_lR | warm | 2590.1 (427.8) | 1321.0 (32.8) | 0.51 | 2585.6 (523.6) | 1303.3 (183.5) | 0.50 | 2112.1 (528.5) | 1071.8 (199.6) | 0.51 | 2462.8 (82.9) | 1120.2 (233.6) | 0.45 |
| find_f | cold | 258.2 (43.3) | 422.7 (29.0) | 1.64 | 261.1 (39.9) | 427.9 (29.4) | 1.64 | 266.0 (22.2) | 411.9 (44.9) | 1.55 | 278.2 (32.0) | 413.0 (27.3) | 1.48 |
| find_f | warm | 215.7 (9.98) | 360.9 (13.1) | 1.67 | 218.0 (30.7) | 359.4 (19.8) | 1.65 | 216.8 (35.0) | 347.6 (30.3) | 1.60 | 226.3 (22.4) | 337.0 (36.0) | 1.49 |
| git_status | cold | 594.9 (36.6) | 606.0 (28.7) | 1.02 | 591.6 (36.6) | 595.8 (26.3) | 1.01 | 575.9 (41.0) | 601.5 (21.3) | 1.04 | 585.1 (41.0) | 608.0 (34.2) | 1.04 |
| git_status | warm | 260.5 (19.7) | 257.9 (46.7) | 0.99 | 257.2 (20.2) | 269.8 (30.0) | 1.05 | 247.8 (26.1) | 271.8 (39.6) | 1.10 | 242.8 (15.4) | 272.3 (22.0) | 1.12 |
| make_q | cold | 798.5 (44.9) | 816.8 (55.7) | 1.02 | 802.9 (87.4) | 834.8 (61.2) | 1.04 | 728.2 (54.5) | 836.4 (61.1) | 1.15 | 754.6 (52.4) | 838.9 (70.1) | 1.11 |
| make_q | warm | 418.7 (45.3) | 594.6 (49.9) | 1.42 | 420.0 (54.1) | 588.9 (54.5) | 1.40 | 354.9 (52.6) | 574.0 (71.6) | 1.62 | 397.2 (40.6) | 581.5 (52.5) | 1.46 |
| rsync_n | cold | 3441.7 (673.0) | 1098.1 (115.1) | 0.32 | 3348.4 (620.0) | 1094.8 (59.2) | 0.33 | 3511.1 (452.9) | 1086.7 (75.0) | 0.31 | 3624.2 (492.3) | 1058.7 (54.2) | 0.29 |
| rsync_n | warm | 3244.2 (693.4) | 1014.2 (131.1) | 0.31 | 3587.8 (703.4) | 942.5 (76.4) | 0.26 | 3404.4 (692.0) | 941.1 (100.5) | 0.28 | 3503.5 (595.6) | 940.5 (93.3) | 0.27 |

Current main matches 10-31: `find -type f` 1.48-1.67, `make -q` warm 1.40-1.62 (cold 1.02-1.15), `git status` 1.01-1.04
cold and 0.99-1.12 warm (10-31: 1.10-1.23), `ls -lR` 0.45-0.51, `rsync -an` 0.26-0.33.

Not run in this rerun: `base`/`off` at 10M; `always` on xfs, btrfs, tmpfs and at 10M; the replica-cold and two-node (idle
second node) states except strict above; an LTS kernel without `dabb90391028`.

---

## What was compared

| Version | Build | `CONSTELLATION_FUSE_READDIRPLUS` |
|---|---|---|
| `base` (R0) | `73ea9987`, the commit before plan 40's listing commits (the vfs contract only) | none (the knob does not exist) |
| `off`, `auto` | `7538c07f` | the mode |

Host: 16 cores, 30 GB, Linux 7.3.0-rc4 (`enable_uring` Y, io_uring FUSE transport), one node, a file-store backend.
State dir (and backend) on ext4, xfs, btrfs (default mount options) and tmpfs. The host's 1-minute load was below 4
at the start of every phase (72 gates, highest 3.54, none gave up); nothing else ran on it.

## How it was run

`abba.sh` with `pair` cells (new): one cell runs `off` and `auto` **alternately inside one populated state dir**, `off`
then `auto` on even repetitions and `auto` then `off` on odd ones (ABBA at the grain of one run), each run on a
fresh mount of the same replica, so the host's drift cancels run by run and both modes read the same bytes. A `base`
cell runs the old build alone. Kernel-cold is a fresh mount with the replica warm (the host's page cache is not
dropped).

| Phase | Cells | Repetitions |
|---|---|---|
| listings, 1k / 10k / 100k entries (`ls -f`, `ls`, `find -name`, `ls -l`, `ls -la`, `find -ls`, `find -newer`, `find -size`, `du -s`, `rsync -a -n`, a second `ls -l` right after a first, `xargs stat`) | ext4: 2 `pair` cells; xfs, btrfs, tmpfs: 1 each; `base`: 1 per file system | 5 per mode per cell (ext4: 10 per mode), `base` 3 |
| tree workloads: the 50k-file tree of 2,000 directories (`gen-tree.py`: a git repository and a 5,000-target Makefile), `ls -lR`, `find -type f`, `git status --porcelain`, `make -q`, `rsync -an --delete` onto a copy; each cold on a fresh mount and warm (the same command again at once) | 3 `pair` cells per file system | 6 per mode per cell = 18 per mode, state and file system |
| 1M-entry directory, `ls -f`, `ls -l`, cold resume | `pair` on ext4 and tmpfs; `base` on ext4 | 3 per mode (`base` 1) |

Medians (IQR) of the runs, pooled over the cells of a file system. Each run records the wall time, the daemon's CPU,
its peak RSS, the FUSE requests by op and their summed service time (`node.status` `vfs_ops`), and the mount's
READDIRPLUS counters. The tree workloads repeat plan 42 §8's (`bench/kernel-races`), with the replica warm instead
of the host's caches dropped.

What was not run: plan 40 §8 item 2 (the create/rename/unlink storm and `tar x`, write-through and write-back);
item 5's RSS formula at 10M and with 10,000 open directories; item 6 (pins, `releasedir` p99, the handover with a
pinned directory); the 10M-entry directory (about an hour to create per cell); `always`; the replica-cold and
two-node states; an LTS kernel without `dabb90391028`; `base` on xfs, btrfs and tmpfs at 1M. The populate of the tree
and of the 1M directory used `--write-mode back`; the measured mounts use the default.

## Verdict per §8 item

Ratios are `auto` over the named version of the median kernel-cold wall time on ext4 / xfs / btrfs / tmpfs (below 1
is faster), unless the text says otherwise.

1. **Plain listing** (`ls -f`, `ls`, `find -name`): **met at 10k and above, not met at 1k for `auto`.** `auto`/`base`:
   10k 0.49-0.53, 100k 0.05-0.09 (11 to 20 times faster); `off`/`base` 10k 0.35-0.40, 100k 0.05-0.09, 1k 0.79-0.90.
   At 1k `auto` takes 4.4-4.85 ms against `base`'s 1.8-2.4 ms (2.0-2.5 times, +2.5 to +2.8 ms) and `off`'s 1.5-1.9 ms:
   the kernel asks `READDIRPLUS` for the first chunk of every listing, one plus page of 721 entries (about 2 ms of
   daemon time and 1 ms of kernel work to instantiate them) that `ls` and `find -name` do not use. At 10k `auto` is
   1.26-1.43 times `off` and at 100k 1.02-1.06 times.
2. **Write path**: **not measured here; see the rerun below (met).**
3. **Request reduction** (`ls -l`, `ls -la`, 1k and 10k, kernel-cold, `auto`, at least 90 % fewer than R0): **not met
   as worded, met without the per-name `listxattr`.** Counting every request, 49.8 % (1k) and 49.9 % (10k) fewer:
   `ls` sends one `listxattr` per name (1,001 and 10,001 of the 1,007 and 10,019 requests) and no plus page can answer
   it. Without them: 6 requests for 1k against 1,003 (99.4 % fewer) and 18 against 10,007 (99.8 %); 100k: 145 against
   100,051 (99.9 %; 100,146 against 200,052 in all). The same numbers on all four file systems (at most 2 requests
   apart). The pages: `ls -l` of 1k is 3 plus pages (333 entries per page on average, 1 lookup left), of 10k 15
   (667), of 100k 140 (714); a plain page carries 500, 1,667 and 2,381 names at the same sizes. `find -ls` and `du -s`
   keep their requests: 286 against 1,004 at 1k (71.5 % fewer), 9,290 against 10,008 at 10k (7.2 %), 99,329 against
   100,047 at 100k (0.7 %): after the first chunk the kernel does not ask for plus (fts reads the whole directory
   before it stats a name).
4. **Time** (`ls -l`, `find -ls`, `du -s`, the second `ls -l`, `rsync -n`, faster than R0 at 1k, 10k and 100k, at
   least 2 times at 10k): **met for `ls -l`, `ls -la` and `rsync -n`; not met for `find -ls`, `du -s` at 10k and the
   second `ls -l` at 1k and 10k.** `auto`/`base`: `ls -l` 0.29-0.39 (1k), 0.37-0.44 (10k), 0.34-0.39 (100k);
   `ls -la` 0.33-0.43, 0.36-0.42, 0.26-0.39; `rsync -n` 0.64-0.66, 0.27-0.29, 0.16-0.17 (3.4 to 6 times faster at 10k and 100k);
   `find -ls` 0.41-0.46 (1k), **0.86-1.02** (10k), 0.71-0.77 (100k); `du -s` 0.39-0.45, **0.84-1.30** (10k; xfs
   1.30, ext4 0.96, btrfs 0.84, tmpfs 0.95), 0.69-0.84; second `ls -l` **1.03-1.12** (1k), **1.04-1.35** (10k), 0.27-0.35
   (100k). Against `off`: `find -ls` and `du -s` 0.41-0.47 at 1k, 0.93-1.11 at 10k, 0.85-1.11 at 100k (no gain after
   the first chunk), the second `ls -l` 1.08-1.45 at 1k and 1.18-1.23 at 10k (the first chunk of the repeated listing
   is a plus page again, about 2.7 us an entry of daemon time for names the kernel already holds). `xargs stat`, the
   regression check: 0.89-1.07 of `base` and 0.91-1.05 of `off` (no change).
5. **Resources**: **CPU met; RSS reported, the bound not evaluated.** Daemon CPU of `ls -l` (user plus sys, ext4):
   0.01 s (1k), 0.13 s (10k), 1.15 s (100k), 15.0 s (1M) for `auto` against `base`'s 0.04, 0.34, 4.1, 149.9 s and
   `off`'s 0.04, 0.32, 2.9, 30.4 s: per listed name it is lower with `auto` than with R0, and for `off` too. Peak RSS
   during the 1M `ls -l` (ext4): `base` 737 MiB, `off` 303 MiB, `auto` 337 MiB. The pins, 10M and 10,000 open
   directories were not measured.
6. **Pins**: **not measured here; see the rerun below (met).**
7. **Page cost**: **O(page) shown; the literal "within 2 times of 1k" not met, because the 1k directory has no full page.**
   Daemon service time per directory page (`readdir`, ext4; the other file systems within 10 %): `base` 0.31 ms (1k),
   2.5 ms (10k), 22.5 ms (100k), 261 ms (1M), growing with the directory; `off` 0.24, 0.62, 0.79, 0.83 ms; `auto`
   (plain pages of `ls -f`) 0.77, 0.92, 0.82, 0.84 ms (its 1k listing is one 721-entry plus page and two plain ones).
   1M over 100k: 1.05 times; over 1k: 3.5 times (`off`), because a 1k listing is two pages of 500 names on average
   and the page of a large directory carries 2,381-2,451; per listed name 0.47 us at 1k and 0.34 us at 1M. A cold
   resume at 1M (restart, seek to the middle, first entry): 1.4-1.5 ms against `base`'s 275 ms (one `base` page at 1M:
   261 ms). 10M was not run.

### The tree workloads: did the held names remove the warm slowdown?

`auto` over `off`, median wall time (18 runs each), ext4 / xfs / btrfs / tmpfs:

| Workload | cold | warm |
|---|---|---|
| `ls -lR` | 0.53 / 0.50 / 0.48 / 0.51 | 0.55 / 0.52 / 0.47 / 0.47 |
| `rsync -an` | 0.35 / 0.31 / 0.34 / 0.33 | 0.32 / 0.29 / 0.27 / 0.29 |
| `git status` | 1.01 / 1.01 / 1.01 / 1.03 | 1.10 / 1.14 / 1.12 / 1.23 |
| `make -q` | 1.09 / 1.02 / 1.10 / 1.05 | 1.46 / 1.41 / 1.64 / 1.40 |
| `find -type f` | 1.58 / 1.55 / 1.59 / 1.53 | 1.67 / 1.66 / 1.71 / 1.66 |

**Partly.** `git status` is the case `8f5af546` fixed: cold it is 1.01-1.03 (1.29 before), warm it is 1.10-1.23 with
the daemon's service time 123-149 ms against 105-110 ms for `off` (230 against 111 ms before); its cold listing is now 3 plus
entries and about 57,500 name-only ones, its warm listing about 4,800 plus entries and 52,700 name-only ones. `find -type f` and `make -q` are **not** removed: 1.5-1.7 times slower on every file system, cold and
warm. They never look up a name before reading its directory, so no name is held; `find -type f` takes 2,234 plus
pages with 53,080 entries with attributes (daemon service time 179-190 ms against 71-80 ms, requests 8,940 against
11,172, wall 337-349 ms against 198-209 ms), `make -q` 2,001 plus pages with about 48,750 entries. The cause is the
kernel's heuristic (`pos == 0` asks plus for the first chunk of every directory, here 2,000 directories of 25
names), not a per-name cost the daemon can skip: nothing is held, and the attributes are used by nobody.

## Measurements (`report.py` output)

In the tables below `x` is a request count without the per-name `listxattr`, "svc" the daemon's summed service time, and `reqs` FUSE requests.

### Ratios of kernel-cold wall time (median), per file system: auto/off, auto/base, off/base

| workload | entries | auto/off ext4 | auto/off xfs | auto/off btrfs | auto/off tmpfs | auto/base ext4 | auto/base xfs | auto/base btrfs | auto/base tmpfs | off/base ext4 | off/base xfs | off/base btrfs | off/base tmpfs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ls_f | 1000 | 2.84 | 2.93 | 2.75 | 2.81 | 2.53 | 2.44 | 2.44 | 2.37 | 0.89 | 0.83 | 0.89 | 0.84 |
| ls_f | 10000 | 1.43 | 1.42 | 1.42 | 1.39 | 0.51 | 0.50 | 0.51 | 0.51 | 0.36 | 0.35 | 0.36 | 0.37 |
| ls_f | 100000 | 1.06 | 1.06 | 1.03 | 1.04 | 0.06 | 0.05 | 0.05 | 0.06 | 0.05 | 0.05 | 0.05 | 0.05 |
| ls | 1000 | 2.55 | 2.47 | 2.47 | 2.53 | 2.02 | 2.04 | 2.04 | 2.09 | 0.79 | 0.83 | 0.83 | 0.83 |
| ls | 10000 | 1.28 | 1.27 | 1.26 | 1.28 | 0.51 | 0.50 | 0.49 | 0.50 | 0.40 | 0.39 | 0.39 | 0.39 |
| ls | 100000 | 1.03 | 1.02 | 1.04 | 1.03 | 0.09 | 0.09 | 0.09 | 0.09 | 0.09 | 0.08 | 0.08 | 0.08 |
| find_name | 1000 | 2.71 | 2.65 | 2.56 | 2.56 | 2.42 | 2.25 | 2.30 | 2.30 | 0.89 | 0.85 | 0.90 | 0.90 |
| find_name | 10000 | 1.36 | 1.38 | 1.38 | 1.40 | 0.52 | 0.53 | 0.51 | 0.53 | 0.38 | 0.38 | 0.37 | 0.38 |
| find_name | 100000 | 1.04 | 1.03 | 1.03 | 1.04 | 0.07 | 0.07 | 0.07 | 0.07 | 0.07 | 0.06 | 0.06 | 0.07 |
| ls_l | 1000 | 0.41 | 0.32 | 0.45 | 0.41 | 0.39 | 0.29 | 0.39 | 0.39 | 0.94 | 0.90 | 0.86 | 0.96 |
| ls_l | 10000 | 0.48 | 0.35 | 0.33 | 0.43 | 0.43 | 0.39 | 0.37 | 0.44 | 0.88 | 1.11 | 1.13 | 1.01 |
| ls_l | 100000 | 0.49 | 0.43 | 0.38 | 0.44 | 0.34 | 0.34 | 0.35 | 0.39 | 0.69 | 0.78 | 0.92 | 0.88 |
| ls_la | 1000 | 0.36 | 0.40 | 0.33 | 0.37 | 0.37 | 0.39 | 0.33 | 0.43 | 1.02 | 0.98 | 0.98 | 1.17 |
| ls_la | 10000 | 0.42 | 0.39 | 0.38 | 0.41 | 0.41 | 0.36 | 0.37 | 0.42 | 0.98 | 0.92 | 0.99 | 1.03 |
| ls_la | 100000 | 0.37 | 0.53 | 0.36 | 0.36 | 0.28 | 0.39 | 0.26 | 0.33 | 0.77 | 0.74 | 0.72 | 0.92 |
| find_ls | 1000 | 0.45 | 0.41 | 0.42 | 0.43 | 0.42 | 0.41 | 0.46 | 0.46 | 0.94 | 1.01 | 1.08 | 1.07 |
| find_ls | 10000 | 1.05 | 1.11 | 0.99 | 0.94 | 1.02 | 0.91 | 0.86 | 0.91 | 0.97 | 0.81 | 0.87 | 0.96 |
| find_ls | 100000 | 0.99 | 1.04 | 0.85 | 1.08 | 0.72 | 0.73 | 0.71 | 0.77 | 0.72 | 0.70 | 0.83 | 0.72 |
| find_newer | 1000 | 0.46 | 0.42 | 0.46 | 0.41 | 0.44 | 0.36 | 0.43 | 0.41 | 0.96 | 0.86 | 0.93 | 1.02 |
| find_newer | 10000 | 0.93 | 0.99 | 0.85 | 0.91 | 1.01 | 1.01 | 0.80 | 0.93 | 1.08 | 1.01 | 0.94 | 1.02 |
| find_newer | 100000 | 1.00 | 0.89 | 1.20 | 0.97 | 0.83 | 0.63 | 0.81 | 0.81 | 0.83 | 0.71 | 0.68 | 0.83 |
| find_size | 1000 | 0.45 | 0.44 | 0.44 | 0.44 | 0.44 | 0.38 | 0.43 | 0.45 | 0.98 | 0.87 | 0.98 | 1.02 |
| find_size | 10000 | 0.92 | 0.74 | 0.98 | 0.81 | 0.84 | 0.80 | 1.03 | 0.89 | 0.90 | 1.08 | 1.05 | 1.09 |
| find_size | 100000 | 0.93 | 0.98 | 1.07 | 0.96 | 0.68 | 0.77 | 0.72 | 0.63 | 0.73 | 0.78 | 0.67 | 0.65 |
| du_s | 1000 | 0.42 | 0.47 | 0.42 | 0.41 | 0.45 | 0.42 | 0.39 | 0.41 | 1.09 | 0.90 | 0.94 | 1.00 |
| du_s | 10000 | 1.01 | 1.07 | 1.00 | 0.93 | 0.96 | 1.30 | 0.84 | 0.95 | 0.95 | 1.21 | 0.84 | 1.02 |
| du_s | 100000 | 1.09 | 1.11 | 0.93 | 1.01 | 0.80 | 0.84 | 0.69 | 0.73 | 0.73 | 0.76 | 0.74 | 0.72 |
| rsync_n | 1000 | 0.67 | 0.67 | 0.67 | 0.68 | 0.66 | 0.64 | 0.64 | 0.66 | 0.98 | 0.96 | 0.96 | 0.98 |
| rsync_n | 10000 | 0.29 | 0.28 | 0.32 | 0.28 | 0.27 | 0.29 | 0.28 | 0.29 | 0.94 | 1.03 | 0.90 | 1.05 |
| rsync_n | 100000 | 0.24 | 0.21 | 0.25 | 0.25 | 0.17 | 0.16 | 0.16 | 0.17 | 0.70 | 0.76 | 0.64 | 0.70 |
| ls_l_2nd | 1000 | 1.10 | 1.08 | 1.29 | 1.45 | 1.04 | 1.12 | 1.03 | 1.12 | 0.95 | 1.03 | 0.79 | 0.77 |
| ls_l_2nd | 10000 | 1.23 | 1.18 | 1.20 | 1.21 | 1.04 | 1.04 | 1.11 | 1.35 | 0.84 | 0.88 | 0.93 | 1.11 |
| ls_l_2nd | 100000 | 0.40 | 0.37 | 0.34 | 0.40 | 0.31 | 0.28 | 0.27 | 0.35 | 0.78 | 0.77 | 0.78 | 0.89 |
| xargs_stat | 1000 | 0.96 | 1.04 | 1.01 | 1.00 | 0.89 | 1.01 | 0.94 | 0.99 | 0.93 | 0.97 | 0.93 | 0.99 |
| xargs_stat | 10000 | 1.01 | 1.01 | 0.93 | 0.92 | 1.07 | 1.04 | 0.90 | 0.99 | 1.05 | 1.03 | 0.96 | 1.07 |
| xargs_stat | 100000 | 1.05 | 1.00 | 1.01 | 0.91 | 1.05 | 1.00 | 0.98 | 0.90 | 1.01 | 1.00 | 0.97 | 0.98 |

### Listing workloads: kernel-cold wall time, median ms (IQR); ratio = auto / off

#### ext4

| workload | entries | base | off | auto | auto/off | auto/base |
|---|---|---|---|---|---|---|
| ls_f | 1000 | 1.80 (0.05) | 1.60 (0.07) | 4.55 (0.10) | 2.84 | 2.53 |
| ls_f | 10000 | 18.7 (0.35) | 6.70 (0.15) | 9.55 (0.10) | 1.43 | 0.51 |
| ls_f | 100000 | 1022.9 (1.60) | 54.4 (1.90) | 57.6 (0.52) | 1.06 | 0.06 |
| ls | 1000 | 2.40 (0.10) | 1.90 (0.08) | 4.85 (0.10) | 2.55 | 2.02 |
| ls | 10000 | 25.3 (0.40) | 10.0 (0.10) | 12.8 (0.15) | 1.28 | 0.51 |
| ls | 100000 | 1089.3 (13.0) | 93.2 (1.15) | 96.2 (0.98) | 1.03 | 0.09 |
| find_name | 1000 | 1.90 (0.05) | 1.70 (0.07) | 4.60 (0.08) | 2.71 | 2.42 |
| find_name | 10000 | 19.6 (0.55) | 7.40 (0.08) | 10.1 (0.15) | 1.36 | 0.52 |
| find_name | 100000 | 1055.9 (7.50) | 69.1 (0.85) | 71.8 (1.42) | 1.04 | 0.07 |
| ls_l | 1000 | 50.8 (3.25) | 48.0 (2.70) | 19.8 (2.65) | 0.41 | 0.39 |
| ls_l | 10000 | 446.9 (49.1) | 394.1 (71.4) | 190.4 (37.7) | 0.48 | 0.43 |
| ls_l | 100000 | 5039.1 (397.1) | 3489.8 (555.7) | 1708.0 (283.7) | 0.49 | 0.34 |
| ls_la | 1000 | 46.9 (3.80) | 48.0 (5.65) | 17.1 (3.58) | 0.36 | 0.37 |
| ls_la | 10000 | 450.7 (56.7) | 443.4 (42.9) | 185.6 (31.9) | 0.42 | 0.41 |
| ls_la | 100000 | 5439.7 (215.4) | 4173.3 (744.1) | 1534.4 (352.3) | 0.37 | 0.28 |
| find_ls | 1000 | 33.3 (4.15) | 31.2 (3.30) | 14.1 (1.53) | 0.45 | 0.42 |
| find_ls | 10000 | 301.8 (10.1) | 293.5 (27.4) | 308.0 (14.1) | 1.05 | 1.02 |
| find_ls | 100000 | 3150.2 (288.8) | 2283.4 (313.5) | 2253.8 (170.5) | 0.99 | 0.72 |
| find_newer | 1000 | 31.4 (3.90) | 30.2 (3.77) | 13.8 (1.05) | 0.46 | 0.44 |
| find_newer | 10000 | 272.5 (53.3) | 293.5 (18.4) | 274.0 (52.6) | 0.93 | 1.01 |
| find_newer | 100000 | 3081.9 (235.6) | 2549.6 (290.0) | 2550.2 (380.7) | 1.00 | 0.83 |
| find_size | 1000 | 30.9 (2.70) | 30.2 (1.95) | 13.7 (1.05) | 0.45 | 0.44 |
| find_size | 10000 | 330.0 (42.8) | 298.6 (28.4) | 276.1 (41.8) | 0.92 | 0.84 |
| find_size | 100000 | 3490.7 (188.1) | 2539.1 (341.4) | 2358.7 (416.4) | 0.93 | 0.68 |
| du_s | 1000 | 28.9 (3.25) | 31.5 (3.97) | 13.1 (0.88) | 0.42 | 0.45 |
| du_s | 10000 | 290.7 (34.0) | 275.1 (9.37) | 279.0 (37.1) | 1.01 | 0.96 |
| du_s | 100000 | 3056.1 (261.2) | 2238.5 (206.8) | 2450.8 (358.1) | 1.09 | 0.80 |
| rsync_n | 1000 | 74.2 (2.25) | 73.0 (2.57) | 48.8 (0.25) | 0.67 | 0.66 |
| rsync_n | 10000 | 352.8 (4.45) | 330.5 (35.3) | 96.6 (0.80) | 0.29 | 0.27 |
| rsync_n | 100000 | 3305.3 (273.2) | 2301.2 (494.1) | 561.1 (9.92) | 0.24 | 0.17 |
| ls_l_2nd | 1000 | 17.6 (2.05) | 16.6 (1.22) | 18.3 (3.18) | 1.10 | 1.04 |
| ls_l_2nd | 10000 | 178.0 (19.3) | 150.2 (37.9) | 185.2 (29.8) | 1.23 | 1.04 |
| ls_l_2nd | 100000 | 5152.5 (469.1) | 4021.4 (547.8) | 1613.4 (262.4) | 0.40 | 0.31 |
| xargs_stat | 1000 | 44.9 (2.95) | 41.6 (1.73) | 39.9 (3.50) | 0.96 | 0.89 |
| xargs_stat | 10000 | 366.8 (16.9) | 386.9 (17.9) | 391.4 (30.6) | 1.01 | 1.07 |
| xargs_stat | 100000 | 3318.4 (319.4) | 3344.4 (366.0) | 3496.9 (269.5) | 1.05 | 1.05 |

#### xfs

| workload | entries | base | off | auto | auto/off | auto/base |
|---|---|---|---|---|---|---|
| ls_f | 1000 | 1.80 (0.05) | 1.50 (0.10) | 4.40 (0.10) | 2.93 | 2.44 |
| ls_f | 10000 | 18.9 (0.35) | 6.60 (0.10) | 9.40 (0.10) | 1.42 | 0.50 |
| ls_f | 100000 | 1026.7 (10.3) | 53.4 (0.90) | 56.4 (0.50) | 1.06 | 0.05 |
| ls | 1000 | 2.30 (0.05) | 1.90 (0.00) | 4.70 (0.10) | 2.47 | 2.04 |
| ls | 10000 | 25.2 (0.55) | 9.90 (0.10) | 12.6 (0.10) | 1.27 | 0.50 |
| ls | 100000 | 1116.0 (5.90) | 93.9 (2.00) | 95.7 (0.60) | 1.02 | 0.09 |
| find_name | 1000 | 2.00 (0.05) | 1.70 (0.10) | 4.50 (0.00) | 2.65 | 2.25 |
| find_name | 10000 | 19.2 (0.40) | 7.30 (0.10) | 10.1 (0.30) | 1.38 | 0.53 |
| find_name | 100000 | 1054.5 (5.35) | 68.3 (0.30) | 70.3 (0.70) | 1.03 | 0.07 |
| ls_l | 1000 | 51.9 (6.85) | 46.5 (8.40) | 15.0 (2.60) | 0.32 | 0.29 |
| ls_l | 10000 | 370.3 (28.2) | 411.7 (53.1) | 145.8 (24.2) | 0.35 | 0.39 |
| ls_l | 100000 | 5136.9 (370.4) | 4021.7 (41.1) | 1748.0 (46.8) | 0.43 | 0.34 |
| ls_la | 1000 | 49.9 (6.80) | 48.7 (5.10) | 19.4 (0.50) | 0.40 | 0.39 |
| ls_la | 10000 | 493.2 (16.1) | 452.1 (110.1) | 177.7 (3.50) | 0.39 | 0.36 |
| ls_la | 100000 | 4311.7 (408.9) | 3184.1 (813.1) | 1677.0 (336.2) | 0.53 | 0.39 |
| find_ls | 1000 | 34.9 (2.20) | 35.4 (3.90) | 14.4 (1.10) | 0.41 | 0.41 |
| find_ls | 10000 | 334.5 (1.95) | 272.0 (27.0) | 303.2 (10.5) | 1.11 | 0.91 |
| find_ls | 100000 | 3240.8 (173.9) | 2276.5 (150.8) | 2377.0 (426.0) | 1.04 | 0.73 |
| find_newer | 1000 | 35.4 (3.35) | 30.5 (3.10) | 12.7 (1.60) | 0.42 | 0.36 |
| find_newer | 10000 | 271.9 (22.8) | 275.9 (16.0) | 274.4 (13.6) | 0.99 | 1.01 |
| find_newer | 100000 | 3462.3 (220.0) | 2458.1 (272.9) | 2182.9 (393.2) | 0.89 | 0.63 |
| find_size | 1000 | 33.1 (5.55) | 28.7 (1.10) | 12.6 (1.60) | 0.44 | 0.38 |
| find_size | 10000 | 293.8 (8.15) | 318.3 (51.6) | 235.4 (54.1) | 0.74 | 0.80 |
| find_size | 100000 | 3228.1 (278.1) | 2527.0 (234.5) | 2486.4 (131.3) | 0.98 | 0.77 |
| du_s | 1000 | 32.5 (1.65) | 29.3 (1.60) | 13.8 (1.60) | 0.47 | 0.42 |
| du_s | 10000 | 229.0 (36.0) | 277.6 (50.2) | 298.4 (19.4) | 1.07 | 1.30 |
| du_s | 100000 | 3106.7 (287.7) | 2362.8 (239.9) | 2616.5 (236.2) | 1.11 | 0.84 |
| rsync_n | 1000 | 76.1 (1.05) | 72.7 (2.20) | 48.7 (0.10) | 0.67 | 0.64 |
| rsync_n | 10000 | 328.2 (8.90) | 339.2 (31.6) | 96.5 (0.50) | 0.28 | 0.29 |
| rsync_n | 100000 | 3385.6 (264.1) | 2578.7 (352.3) | 549.8 (6.20) | 0.21 | 0.16 |
| ls_l_2nd | 1000 | 14.7 (1.95) | 15.2 (3.90) | 16.4 (3.80) | 1.08 | 1.12 |
| ls_l_2nd | 10000 | 160.1 (19.9) | 140.9 (8.10) | 166.9 (41.4) | 1.18 | 1.04 |
| ls_l_2nd | 100000 | 5010.1 (47.7) | 3869.0 (64.7) | 1420.2 (370.3) | 0.37 | 0.28 |
| xargs_stat | 1000 | 41.6 (0.75) | 40.4 (2.20) | 42.1 (1.40) | 1.04 | 1.01 |
| xargs_stat | 10000 | 369.2 (23.4) | 379.1 (78.6) | 382.9 (39.7) | 1.01 | 1.04 |
| xargs_stat | 100000 | 3560.9 (71.6) | 3548.4 (12.5) | 3547.1 (44.0) | 1.00 | 1.00 |

#### btrfs

| workload | entries | base | off | auto | auto/off | auto/base |
|---|---|---|---|---|---|---|
| ls_f | 1000 | 1.80 (0.10) | 1.60 (0.00) | 4.40 (0.00) | 2.75 | 2.44 |
| ls_f | 10000 | 18.5 (0.15) | 6.70 (0.00) | 9.50 (0.20) | 1.42 | 0.51 |
| ls_f | 100000 | 1033.1 (3.55) | 54.1 (0.30) | 55.9 (0.80) | 1.03 | 0.05 |
| ls | 1000 | 2.30 (0.05) | 1.90 (0.10) | 4.70 (0.00) | 2.47 | 2.04 |
| ls | 10000 | 25.3 (0.20) | 9.90 (0.20) | 12.5 (0.10) | 1.26 | 0.49 |
| ls | 100000 | 1105.0 (11.6) | 92.6 (0.50) | 96.1 (1.40) | 1.04 | 0.09 |
| find_name | 1000 | 2.00 (0.00) | 1.80 (0.00) | 4.60 (0.10) | 2.56 | 2.30 |
| find_name | 10000 | 19.8 (0.45) | 7.30 (0.10) | 10.1 (0.10) | 1.38 | 0.51 |
| find_name | 100000 | 1058.6 (8.20) | 68.6 (1.10) | 70.6 (0.30) | 1.03 | 0.07 |
| ls_l | 1000 | 49.1 (6.55) | 42.3 (6.50) | 19.2 (0.30) | 0.45 | 0.39 |
| ls_l | 10000 | 407.1 (46.9) | 458.9 (148.0) | 150.1 (41.2) | 0.33 | 0.37 |
| ls_l | 100000 | 4339.9 (409.0) | 4001.8 (848.1) | 1508.6 (240.6) | 0.38 | 0.35 |
| ls_la | 1000 | 46.4 (2.70) | 45.5 (8.60) | 15.1 (3.10) | 0.33 | 0.33 |
| ls_la | 10000 | 441.8 (22.5) | 436.8 (49.5) | 165.3 (39.3) | 0.38 | 0.37 |
| ls_la | 100000 | 5288.2 (42.5) | 3833.0 (791.8) | 1384.2 (10.0) | 0.36 | 0.26 |
| find_ls | 1000 | 31.1 (4.90) | 33.5 (3.50) | 14.2 (3.10) | 0.42 | 0.46 |
| find_ls | 10000 | 328.8 (10.0) | 287.5 (32.5) | 283.9 (27.9) | 0.99 | 0.86 |
| find_ls | 100000 | 3164.7 (34.1) | 2631.4 (354.5) | 2235.4 (76.5) | 0.85 | 0.71 |
| find_newer | 1000 | 31.7 (3.30) | 29.6 (1.90) | 13.6 (1.10) | 0.46 | 0.43 |
| find_newer | 10000 | 318.4 (44.3) | 298.4 (2.40) | 255.1 (68.1) | 0.85 | 0.80 |
| find_newer | 100000 | 3142.5 (157.1) | 2138.5 (159.7) | 2557.3 (111.4) | 1.20 | 0.81 |
| find_size | 1000 | 31.8 (5.75) | 31.2 (4.60) | 13.6 (1.30) | 0.44 | 0.43 |
| find_size | 10000 | 279.9 (20.8) | 294.5 (14.4) | 287.7 (56.6) | 0.98 | 1.03 |
| find_size | 100000 | 3474.7 (238.4) | 2339.4 (157.4) | 2509.5 (513.4) | 1.07 | 0.72 |
| du_s | 1000 | 32.6 (0.90) | 30.5 (2.50) | 12.8 (0.60) | 0.42 | 0.39 |
| du_s | 10000 | 307.7 (16.1) | 258.9 (58.5) | 258.1 (24.5) | 1.00 | 0.84 |
| du_s | 100000 | 3401.1 (188.1) | 2532.4 (58.1) | 2362.6 (356.1) | 0.93 | 0.69 |
| rsync_n | 1000 | 75.8 (2.00) | 72.4 (3.30) | 48.8 (0.20) | 0.67 | 0.64 |
| rsync_n | 10000 | 339.4 (38.8) | 304.2 (49.8) | 96.4 (0.60) | 0.32 | 0.28 |
| rsync_n | 100000 | 3430.9 (264.0) | 2201.0 (260.7) | 557.6 (6.70) | 0.25 | 0.16 |
| ls_l_2nd | 1000 | 15.9 (2.05) | 12.6 (3.80) | 16.3 (3.90) | 1.29 | 1.03 |
| ls_l_2nd | 10000 | 158.1 (6.90) | 146.7 (8.00) | 175.7 (5.20) | 1.20 | 1.11 |
| ls_l_2nd | 100000 | 4989.1 (408.8) | 3909.1 (125.2) | 1333.1 (86.7) | 0.34 | 0.27 |
| xargs_stat | 1000 | 43.9 (1.50) | 40.8 (4.20) | 41.4 (2.80) | 1.01 | 0.94 |
| xargs_stat | 10000 | 411.6 (44.4) | 396.3 (12.6) | 368.4 (23.1) | 0.93 | 0.90 |
| xargs_stat | 100000 | 3589.5 (150.9) | 3492.6 (99.6) | 3513.0 (77.5) | 1.01 | 0.98 |

#### tmpfs

| workload | entries | base | off | auto | auto/off | auto/base |
|---|---|---|---|---|---|---|
| ls_f | 1000 | 1.90 (0.05) | 1.60 (0.00) | 4.50 (0.10) | 2.81 | 2.37 |
| ls_f | 10000 | 18.8 (0.20) | 6.90 (0.10) | 9.60 (0.10) | 1.39 | 0.51 |
| ls_f | 100000 | 1025.7 (9.70) | 54.7 (1.40) | 57.0 (1.70) | 1.04 | 0.06 |
| ls | 1000 | 2.30 (0.10) | 1.90 (0.00) | 4.80 (0.00) | 2.53 | 2.09 |
| ls | 10000 | 25.2 (0.20) | 9.90 (0.20) | 12.7 (0.30) | 1.28 | 0.50 |
| ls | 100000 | 1104.1 (6.15) | 93.8 (0.80) | 96.9 (0.50) | 1.03 | 0.09 |
| find_name | 1000 | 2.00 (0.05) | 1.80 (0.10) | 4.60 (0.00) | 2.56 | 2.30 |
| find_name | 10000 | 19.2 (0.60) | 7.30 (0.00) | 10.2 (0.20) | 1.40 | 0.53 |
| find_name | 100000 | 1062.4 (15.6) | 69.2 (0.60) | 72.1 (0.50) | 1.04 | 0.07 |
| ls_l | 1000 | 49.8 (2.00) | 47.9 (2.10) | 19.5 (2.00) | 0.41 | 0.39 |
| ls_l | 10000 | 440.7 (49.2) | 446.8 (49.5) | 192.5 (30.9) | 0.43 | 0.44 |
| ls_l | 100000 | 4460.4 (415.2) | 3912.9 (917.1) | 1734.0 (390.2) | 0.44 | 0.39 |
| ls_la | 1000 | 41.9 (4.15) | 49.1 (0.80) | 18.0 (3.60) | 0.37 | 0.43 |
| ls_la | 10000 | 453.5 (14.4) | 468.3 (30.2) | 190.9 (36.1) | 0.41 | 0.42 |
| ls_la | 100000 | 4609.4 (220.3) | 4244.3 (741.6) | 1525.4 (17.3) | 0.36 | 0.33 |
| find_ls | 1000 | 31.9 (3.35) | 34.2 (2.40) | 14.6 (0.60) | 0.43 | 0.46 |
| find_ls | 10000 | 314.8 (7.10) | 303.7 (5.80) | 285.8 (52.3) | 0.94 | 0.91 |
| find_ls | 100000 | 3624.4 (72.7) | 2591.9 (265.2) | 2788.1 (138.4) | 1.08 | 0.77 |
| find_newer | 1000 | 30.6 (2.45) | 31.1 (1.70) | 12.6 (0.80) | 0.41 | 0.41 |
| find_newer | 10000 | 293.2 (24.6) | 297.8 (26.3) | 272.2 (5.50) | 0.91 | 0.93 |
| find_newer | 100000 | 3157.4 (314.7) | 2630.0 (406.5) | 2551.0 (305.1) | 0.97 | 0.81 |
| find_size | 1000 | 30.1 (4.40) | 30.6 (3.10) | 13.5 (1.00) | 0.44 | 0.45 |
| find_size | 10000 | 283.6 (32.7) | 309.6 (44.3) | 251.6 (55.4) | 0.81 | 0.89 |
| find_size | 100000 | 3568.5 (252.0) | 2330.7 (382.6) | 2247.7 (186.9) | 0.96 | 0.63 |
| du_s | 1000 | 31.3 (1.60) | 31.3 (4.10) | 12.8 (0.20) | 0.41 | 0.41 |
| du_s | 10000 | 309.5 (33.7) | 314.4 (51.3) | 292.6 (38.7) | 0.93 | 0.95 |
| du_s | 100000 | 3113.9 (107.3) | 2248.4 (456.7) | 2277.8 (338.3) | 1.01 | 0.73 |
| rsync_n | 1000 | 74.0 (3.20) | 72.5 (4.80) | 49.0 (0.10) | 0.68 | 0.66 |
| rsync_n | 10000 | 344.8 (26.6) | 362.4 (37.6) | 99.9 (0.90) | 0.28 | 0.29 |
| rsync_n | 100000 | 3377.7 (67.5) | 2366.3 (440.0) | 583.7 (8.60) | 0.25 | 0.17 |
| ls_l_2nd | 1000 | 17.1 (1.90) | 13.2 (3.80) | 19.1 (2.00) | 1.45 | 1.12 |
| ls_l_2nd | 10000 | 136.3 (19.1) | 151.9 (12.8) | 183.9 (36.6) | 1.21 | 1.35 |
| ls_l_2nd | 100000 | 4345.5 (403.9) | 3863.9 (621.6) | 1528.2 (120.5) | 0.40 | 0.35 |
| xargs_stat | 1000 | 45.1 (3.05) | 44.8 (2.90) | 44.6 (3.70) | 1.00 | 0.99 |
| xargs_stat | 10000 | 379.8 (35.8) | 407.9 (38.5) | 374.8 (17.2) | 0.92 | 0.99 |
| xargs_stat | 100000 | 3587.9 (222.9) | 3531.7 (406.0) | 3222.9 (561.2) | 0.91 | 0.90 |

### FUSE requests per run (median); `x` = without the per-name `listxattr`

#### ext4 (the other file systems differ by at most 2 requests in any cell)

| workload | entries | base / base x | off / off x | auto / auto x | auto vs base: fewer, without `listxattr` (with) |
|---|---|---|---|---|---|
| ls_l | 1000 | 2004 / 1003 | 2006 / 1005 | 1007 / 6 | 99.4 % (49.8 %) |
| ls_l | 10000 | 20008 / 10007 | 20010 / 10009 | 10019 / 18 | 99.8 % (49.9 %) |
| ls_l | 100000 | 200052 / 100051 | 200052 / 100051 | 100146 / 145 | 99.9 % (49.9 %) |
| ls_la | 1000 | 2006 / 1003 | 2008 / 1005 | 1009 / 6 | 99.4 % (49.7 %) |
| ls_la | 10000 | 20010 / 10007 | 20012 / 10009 | 10021 / 18 | 99.8 % (49.9 %) |
| ls_la | 100000 | 200056 / 100053 | 200056 / 100053 | 100148 / 145 | 99.9 % (49.9 %) |
| find_ls | 1000 | 1004 / 1004 | 1006 / 1006 | 286 / 286 |  |
| find_ls | 10000 | 10008 / 10008 | 10010 / 10010 | 9290 / 9290 |  |
| find_ls | 100000 | 100047 / 100047 | 100048 / 100048 | 99329 / 99329 |  |
| du_s | 1000 | 1004 / 1004 | 1006 / 1006 | 286 / 286 |  |
| du_s | 10000 | 10008 / 10008 | 10010 / 10010 | 9290 / 9290 |  |
| du_s | 100000 | 100047 / 100047 | 100048 / 100048 | 99329 / 99329 |  |
| ls_l_2nd | 1000 | 1004 / 3 | 1006 / 5 | 1007 / 6 |  |
| ls_l_2nd | 10000 | 10008 / 7 | 10010 / 9 | 10019 / 18 |  |
| ls_l_2nd | 100000 | 200054 / 100053 | 200054 / 100053 | 100148 / 147 |  |
| rsync_n | 1000 | 1003 / 1003 | 1005 / 1005 | 6 / 6 |  |
| rsync_n | 10000 | 10007 / 10007 | 10009 / 10009 | 18 / 18 |  |
| rsync_n | 100000 | 100046 / 100046 | 100047 / 100047 | 143 / 143 |  |

### Daemon service time per directory page (`readdir` + `readdirplus`), us, and pages per run

| workload | entries | ext4 off | ext4 auto | xfs off | xfs auto | btrfs off | btrfs auto | tmpfs off | tmpfs auto |
|---|---|---|---|---|---|---|---|---|---|
| ls_f | 1000 | 237.29 (2 pages) | 765.72 (3 pages) | 213.83 (2 pages) | 725.92 (3 pages) | 226.27 (2 pages) | 723.77 (3 pages) | 230.57 (2 pages) | 763.82 (3 pages) |
| ls_f | 10000 | 624.64 (6 pages) | 918.29 (6 pages) | 612.70 (6 pages) | 895.16 (6 pages) | 610.58 (6 pages) | 896.76 (6 pages) | 639.31 (6 pages) | 920.98 (6 pages) |
| ls_f | 100000 | 793.74 (42 pages) | 821.85 (43 pages) | 773.73 (42 pages) | 797.91 (43 pages) | 779.08 (42 pages) | 801.12 (43 pages) | 795.12 (42 pages) | 817.94 (43 pages) |
| big_ls_f | 1000000 | 834.98 (408 pages) | 839.17 (409 pages) |  |  |  |  | 743.76 (408 pages) | 731.26 (409 pages) |
| ls | 1000 | 243.57 (2 pages) | 765.10 (3 pages) | 230.37 (2 pages) | 720.74 (3 pages) | 224.14 (2 pages) | 721.19 (3 pages) | 231.98 (2 pages) | 750.08 (3 pages) |
| ls | 10000 | 618.29 (6 pages) | 921.29 (6 pages) | 612.33 (6 pages) | 900.72 (6 pages) | 615.61 (6 pages) | 882.94 (6 pages) | 622.89 (6 pages) | 914.95 (6 pages) |
| ls | 100000 | 794.76 (42 pages) | 817.79 (43 pages) | 780.01 (42 pages) | 800.00 (43 pages) | 775.36 (42 pages) | 802.80 (43 pages) | 803.05 (42 pages) | 823.75 (43 pages) |
| ls_l | 1000 | 220.07 (2 pages) | 1014.32 (3 pages) | 214.95 (2 pages) | 944.53 (3 pages) | 210.51 (2 pages) | 956.01 (3 pages) | 228.14 (2 pages) | 1002.24 (3 pages) |
| ls_l | 10000 | 606.62 (6 pages) | 1835.60 (15 pages) | 603.01 (6 pages) | 1795.42 (15 pages) | 586.38 (6 pages) | 1804.82 (15 pages) | 612.48 (6 pages) | 1812.99 (15 pages) |
| ls_l | 100000 | 820.40 (42 pages) | 1871.51 (140 pages) | 799.66 (42 pages) | 1804.54 (140 pages) | 802.33 (42 pages) | 1852.62 (140 pages) | 824.31 (42 pages) | 1852.76 (140 pages) |
| big_ls_l | 1000000 | 895.18 (408 pages) | 1946.08 (1385 pages) |  |  |  |  | 786.78 (408 pages) | 1831.22 (1385 pages) |

### Daemon CPU (user + sys seconds per run, median) for `ls -l`, and peak RSS (MiB) at 1M entries

| entries | ext4 base | ext4 off | ext4 auto | xfs base | xfs off | xfs auto | btrfs base | btrfs off | btrfs auto | tmpfs base | tmpfs off | tmpfs auto |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0.04 | 0.04 | 0.01 | 0.03 | 0.03 | 0.01 | 0.03 | 0.04 | 0.01 | 0.04 | 0.03 | 0.01 |
| 10000 | 0.34 | 0.32 | 0.13 | 0.31 | 0.32 | 0.11 | 0.34 | 0.36 | 0.10 | 0.37 | 0.38 | 0.13 |
| 100000 | 4.13 | 2.88 | 1.15 | 4.08 | 2.96 | 1.09 | 3.81 | 2.97 | 0.99 | 3.81 | 3.14 | 1.17 |
| 1000000 | 149.9 s, 737 MiB | 30.4 s, 303 MiB | 15.0 s, 337 MiB |  |  |  |  |  |  |  | 28.6 s, 300 MiB | 10.6 s, 339 MiB |

### Tree workloads (50k files, 2,000 directories), wall ms median (IQR); ratio = auto / off

| workload | state | ext4 off | ext4 auto | ext4 ratio | xfs off | xfs auto | xfs ratio | btrfs off | btrfs auto | btrfs ratio | tmpfs off | tmpfs auto | tmpfs ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ls_lR | cold | 2200.7 (419.6) | 1158.2 (287.8) | 0.53 | 2538.1 (248.5) | 1260.0 (184.4) | 0.50 | 2374.6 (469.5) | 1138.0 (239.6) | 0.48 | 2299.8 (426.8) | 1176.5 (194.2) | 0.51 |
| ls_lR | warm | 2045.2 (560.2) | 1132.1 (260.6) | 0.55 | 2448.3 (512.0) | 1283.6 (199.2) | 0.52 | 2362.5 (462.4) | 1115.8 (188.5) | 0.47 | 2371.7 (567.4) | 1114.3 (188.3) | 0.47 |
| find_f | cold | 261.3 (33.9) | 413.1 (22.9) | 1.58 | 260.6 (26.4) | 402.7 (29.1) | 1.55 | 257.1 (37.3) | 407.6 (22.7) | 1.59 | 260.3 (26.2) | 399.6 (43.0) | 1.53 |
| find_f | warm | 208.9 (27.0) | 349.0 (25.7) | 1.67 | 207.3 (43.4) | 344.3 (30.3) | 1.66 | 198.2 (53.5) | 337.9 (31.8) | 1.71 | 203.8 (30.3) | 337.8 (12.1) | 1.66 |
| git_status | cold | 589.7 (50.1) | 595.0 (33.2) | 1.01 | 582.0 (32.9) | 586.7 (29.1) | 1.01 | 570.1 (34.9) | 576.1 (31.4) | 1.01 | 598.6 (53.5) | 618.2 (24.7) | 1.03 |
| git_status | warm | 238.0 (19.7) | 262.4 (31.1) | 1.10 | 241.0 (16.9) | 275.1 (30.1) | 1.14 | 244.1 (16.8) | 273.9 (35.0) | 1.12 | 246.9 (25.1) | 304.3 (51.0) | 1.23 |
| make_q | cold | 743.7 (82.8) | 813.9 (61.2) | 1.09 | 806.9 (86.8) | 823.2 (57.1) | 1.02 | 734.0 (105.4) | 804.5 (74.3) | 1.10 | 786.7 (93.1) | 827.1 (90.8) | 1.05 |
| make_q | warm | 393.8 (64.0) | 573.8 (50.1) | 1.46 | 415.3 (57.6) | 587.6 (25.8) | 1.41 | 343.4 (53.3) | 561.9 (61.5) | 1.64 | 407.2 (14.2) | 569.6 (69.8) | 1.40 |
| rsync_n | cold | 3068.7 (615.0) | 1080.2 (103.5) | 0.35 | 3444.2 (447.4) | 1066.6 (70.8) | 0.31 | 3058.3 (661.8) | 1049.2 (87.7) | 0.34 | 3271.8 (577.3) | 1071.2 (58.4) | 0.33 |
| rsync_n | warm | 2859.4 (542.9) | 926.4 (134.6) | 0.32 | 3190.0 (670.3) | 926.1 (100.5) | 0.29 | 3483.6 (626.1) | 953.3 (78.5) | 0.27 | 3399.6 (717.4) | 971.7 (101.2) | 0.29 |

Requests per run, off / auto (median), and daemon service ms:

| workload | state | ext4 reqs off/auto | ext4 svc ms off/auto | xfs reqs off/auto | xfs svc ms off/auto | btrfs reqs off/auto | btrfs svc ms off/auto | tmpfs reqs off/auto | tmpfs svc ms off/auto |
|---|---|---|---|---|---|---|---|---|---|
| ls_lR | cold | 123351 / 66123 | 926 / 460 | 123352 / 66124 | 1021 / 593 | 123352 / 66122 | 923 / 458 | 123352 / 66123 | 910 / 448 |
| ls_lR | warm | 123340 / 66122 | 876 / 443 | 123344 / 66124 | 972 / 581 | 123341 / 66122 | 871 / 451 | 123342 / 66122 | 861 / 435 |
| find_f | cold | 11172 / 8940 | 99 / 196 | 11172 / 8940 | 102 / 199 | 11172 / 8940 | 97 / 193 | 11172 / 8940 | 96 / 193 |
| find_f | warm | 11172 / 8940 | 71 / 187 | 11172 / 8940 | 80 / 190 | 11172 / 8940 | 71 / 179 | 11172 / 8940 | 71 / 179 |
| git_status | cold | 66494 / 66493 | 2586 / 2551 | 66494 / 66493 | 2534 / 2562 | 66494 / 66493 | 2532 / 2549 | 66494 / 66493 | 2643 / 2664 |
| git_status | warm | 11432 / 11431 | 107 / 123 | 11432 / 11431 | 110 / 127 | 11432 / 11431 | 110 / 126 | 11432 / 11432 | 105 / 149 |
| make_q | cold | 26239 / 21217 | 253 / 309 | 26239 / 21217 | 279 / 322 | 26239 / 21217 | 273 / 310 | 26239 / 21217 | 254 / 312 |
| make_q | warm | 14524 / 15180 | 96 / 215 | 14856 / 15221 | 112 / 236 | 14319 / 15124 | 94 / 219 | 14844 / 15290 | 97 / 218 |
| rsync_n | cold | 145341 / 30796 | 1270 / 431 | 145343 / 30794 | 1337 / 434 | 145342 / 30794 | 1274 / 418 | 145343 / 30794 | 1257 / 425 |
| rsync_n | warm | 145340 / 30793 | 1233 / 396 | 145342 / 30793 | 1304 / 432 | 145344 / 30792 | 1233 / 404 | 145344 / 30793 | 1209 / 398 |

### Big directories (results)

- ext4 base populate 1000000: {"wall": 207.9}
- ext4 base cold_resume 1000000: {"line": "cold_resume_first_entry_ms=275.4 next500_ms=0.1 entries=501"}
- ext4 off populate 1000000: {"wall": 211.2}
- ext4 auto cold_resume 1000000: {"line": "cold_resume_first_entry_ms=1.5 next500_ms=0.1 entries=501"}
- tmpfs off populate 1000000: {"wall": 144.3}
- tmpfs auto cold_resume 1000000: {"line": "cold_resume_first_entry_ms=1.4 next500_ms=0.1 entries=501"}

Runs per cell group (`ls_l` at the smallest size): {"ext4": {"base": 3, "off": 10, "auto": 10}, "xfs": {"base": 3, "off": 5, "auto": 5}, "btrfs": {"base": 3, "off": 5, "auto": 5}, "tmpfs": {"base": 3, "off": 5, "auto": 5}}

## Running it

```
D=~/cst-p40-8   # bin/base, bin/main, tree.tar (python3 gen-tree.py DIR; tar -C DIR/.. -cf tree.tar <basename>)
cat > jobs.txt <<JOBS          # `pair` = off and auto alternated in one cell; `base` = the old build
ext4 pair 1
ext4 base 1
JOBS
PHASES=list REPS=5 D=$D bench/readdirplus/abba.sh jobs.txt out-list/
PHASES=trees REPS=6 TREE_REPS=6 TREE2=$D/tree.tar D=$D bench/readdirplus/abba.sh jobs-pair.txt out-trees/
PHASES=big BIG_SIZES=1000000 BIG_LISTS_ONLY=1 REPS=3 D=$D bench/readdirplus/abba.sh jobs.txt out-big/   # add 10000000 for the plan's 10M
PHASES=write D=$D bench/readdirplus/abba.sh jobs.txt out-write/   # item 2: base, off, auto as separate cells
python3 bench/readdirplus/report.py out-list/ out-base/ out-big/ out-trees/
# the 2026-11-05 rerun (jobs lines may name the store `root`: $HOME/p40root on the system disk, for the 10M cells)
PHASES=write REPS=2 TREE=$D/tree.tar D=$D bench/readdirplus/abba.sh jobs.txt out-write/   # one tar x per mode
PHASES=opendirs D=$D bench/readdirplus/abba.sh jobs.txt out-od/   # RSS with 10,000 open directories
PHASES=big BIG_SIZES=10000000 BIG_WRITE_MODE=through REPS=1 D=$D bench/readdirplus/abba.sh jobs-root.txt out-big10m/
CONSTELLATION_BIN=$D/bin/main TMPDIR=/mnt/fs-ext4/h target/release/harness run readdir-huge-delete
STRICT_BENCH_MODE=off|auto STRICT_BENCH_REPS=5 target/release/harness run readdirplus-strict-bench
python3 bench/readdirplus/report-rerun.py OUTROOT   # tables of the rerun section
```

`abba.sh` is resumable; each cell (`cell.py`) gates every phase on the host's 1-minute load (`LOAD_MAX`, default 4),
restarts the daemon for every repetition of a kernel-cold run, and records wall time, the daemon's CPU and service
time per op, its peak RSS, the FUSE requests by op and the mount's READDIRPLUS counters. `report.py` prints the
tables above; `summarize.py` prints the plan's acceptance verdicts per cell group for single-mode cells
(`off`, `auto`, `always` against `base`), as before.

What the scripts do not do: the "two nodes (the second idle)" and replica-cold states; the 10M-entry directory takes
about 75 minutes to create in write-through mode and does not fit the test partitions (`BIG_SIZES`, store `root`); the
strict-mode timing is the harness scenario `readdirplus-strict-bench`.
