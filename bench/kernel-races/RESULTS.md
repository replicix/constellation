# Plan 42 — kernel FUSE race workarounds: results

Linux 7.3 distribution kernel, 16 cores, 30 GiB. The host was shared
with other campaigns during every run: its 1-minute load average was
45–130 and its disk was saturated (PSI io `some` avg60 60–85 % during the
benchmarks). **Request counts and race counts are the results here;
wall times are not**, except where a section says otherwise: the two
sections at the end (§8.1 and F21) interleave the compared modes (ABBA),
which cancels the host's drift but not its noise. Base: `80a4e3d` plus K1 (the scenarios and fault points only).
Final: the plan's commits on `516f82e`.

## §7.2 scenarios, L0 (no injected load)

See PROGRESS, "Plan 42", for the per-scenario table (base against
final): every race row met its "after" column on the final build on
both transports, `repairs_late` 0 in every run.

## §7.4 load campaign (reduced)

`kernel-race-open-repair`, `-open-repair-delay`, `-notify`,
`-unseen-drop`, `-dir-nlink`, `-dir-version`, each on both transports,
one seed (42), 60 s, under the harness's load injectors
(`CONSTELLATION_HARNESS_LOAD`):

| Profile | Injection | Runs | Passed | Notes |
|---|---|---|---|---|
| L1 CPU starvation | `hogs:cpu=16:load=100` (stress-ng on every CPU) | 12 | 12 | 3 first failed to mount within the harness's 120 s (no run started); passed with `CONSTELLATION_HARNESS_DEADLINE_SCALE=4` |
| L2 throttled daemon | `cpu:50` (cgroup `cpu.max`, half a CPU per daemon) | 12 | 12 | |
| L3 pauses | `pause:50:300` (SIGSTOP 50 ms every 300 ms) | 12 | 12 | |
| L4 memory pressure | `hogs:vm=2:vmbytes=25%` | 12 | 12 | 25 % instead of the plan's 75 %: the host was shared |

Every run: stale read-backs 0 (`kernel-race-unseen-drop` aside, whose
unannounced drops are allowed stale reads, R1: 0–69 per run), no stale
page surviving a reopen, `repairs_late` 0, st_nlink mismatches 0, 0 of
20 stale rounds (every profile ran on the build before F21's commit,
with W3b on for every operation). Not run: 10 seeds per
cell (the plan's 600 runs did not fit beside the host's other work), the
base build under load (its L0 counts already exceed every threshold).

## §8 benchmarks (A = base, B = the K6 build, before F21; W3b on for every operation)

Request counts from `node.status` (`vfs_ops`); 1–3 interleaved rounds
per store; wall times dominated by the host's IO saturation.

| Benchmark | Stores | A | B | Reading |
|---|---|---|---|---|
| reopen-read, 5 000 × 16 KiB, 3 passes, open per read: daemon `read`s in passes 2+3 | tmpfs, ext4, xfs, btrfs, btrfs-zstd | 10 000 | 0 (ext4: 0 and 677) | W1 works: the 2nd and 3rd reads hit kept pages; wall time of passes 2+3 0.37–0.58× on the disk stores |
| untar 20 000 files, `cp -a` of it: `GETATTR`s | all five | 99–1 612 | 20 084–20 202 | **F21**: one `GETATTR` per created file, from W3b's notice; fixed by narrowing the notice in bounded mode (F21, last section) |
| `find`, `ls -lR`, `du`: `GETATTR`/`LOOKUP` | all five | | | no consistent difference (counts swing with the kernel's cache state in both) |
| 100 000 `open`/`close` of one cached file | all five | 100 000 opens | 100 000 opens | `GETATTR`/`LOOKUP` 5–45 in both; wall 0.38–0.83× on disk stores (tmpfs noise) |
| two views, 5 000 small files written in one, all seen in the other | all five | | | all 5 000 seen in every run; wall inconclusive (tmpfs 1.9×, xfs 0.65×, ranges overlap elsewhere) |
| fio sequential/random write, 70/30, database-style | tmpfs, ext4 | | | indicative only: tmpfs 0.86–0.97×, ext4 noise-dominated; xfs, btrfs not measured (daemons did not start within 5 min on the saturated host) |

Not met as stated: §8's "no regression on any backing store" on wall
times of these rows (the host was saturated); on request counts the one
regression found (F21) is fixed, and measured by interleaved runs in the
last section.

## §7.3 xcheckfs lockstep

Setup: ext4 primary, the final build before the review's
fixes as secondary, write-through and write-back; the stress lane with
an opener thread, and the race workloads through the lockstep mount
and directly on the Constellation mount. Results: no data mismatch attributed
to the secondary in any lane on the final build (base: 1 per mode);
the race-1 workload run directly on the mount: 0 stale reads on the
final build against 2446 / 640 on the base. Directory `st_nlink`
mismatches remain on the final build (17 direct, 4 + 1 through
xcheckfs; base 6–21 and 1–4): race 2a, open in bounded mode while
W3a's default is `entry` (Q1, decided); that build still had W3b on
for every operation, so 2b was closed there (the narrowed notice was not
part of those runs).
Also seen, not investigated: `mtime` mismatches of 1.2–6.4 s in the
stress lane (final 4 and 3, base 0 and 1), on a host whose metadata
commits took up to 17 s; and the final build's runs did far fewer
operations in the same time (the host's disk, both builds collapsed
alike when run side by side). Not run: 30 minutes per lane, xcheckfs
at its current head (its own stat-past-the-cache change would mask
the nlink race).

## §7.5

Run on a QEMU/KVM guest, mainline `4056b3c9d3c9` with and without patches
0001–0003; the table and the findings are in plan 42 §7.5 (probes Fixed on
the patched kernel and NotFixed on the base; every row 0, the moved inode's notice kept
with 0003 (17 of 20 old ctimes without it; 0 of 20 with 0003 v2 and no notice);
the F10 stall shown by `kernel-race-repair-stall`; a daemon exit during the
probe hung on the patched kernel until `kernel-race-probe-exit`'s fix).

## §8.1 Q1: directory attribute TTL `entry` against `zero` (run without the quiet-host gate)

Decides Q1 (plan 42 §10): **option B**, `entry` in bounded mode. Final
build of K5 with only the setting changed
(`CONSTELLATION_FUSE_DIR_ATTR_TTL`), release mode, io_uring transport.
Strictly interleaved ABBA (`entry, zero, zero, entry`), 4 blocks, so 8
samples per mode per workload per store; each sample is a fresh mount
(unmounted and waited for before the next) and a cold then a warm run.
Stores: ext4, xfs and btrfs partitions and tmpfs, each holding a tree of
55 052 files (a git repository, a 5 000-target Makefile, 10 000 deep
paths). No load gate: the host's other agents kept its 1-minute load
between 8 and 90 (median 11 to 25, system CPU busy 0.7 to 0.87), so
absolute times and spreads are inflated; the probe (50 deep `stat`s
twice) showed 0 `GETATTR`s under `entry` and 250 under `zero` in every run, and the
workloads' own CPU time moves with the wall time.

Warm `git status --porcelain` and warm `make -q all`, `zero` over `entry`
(median wall ratio; requests):

| Store | `git status` | `make -q` |
|---|---|---|
| ext4 | 6.22 | 3.18 |
| xfs | 5.70 | 2.81 |
| btrfs | 2.58 | 4.17 |
| tmpfs | 7.89 | 3.03 |

Requests: `git status` about 7 000 against 293 000 (`GETATTR` 2 230
against 230 600), `make -q` 10 111 against 84 567. The other workloads
under `zero`: `ls -lR` 3.0 to 3.7 times, `find -newer` 1.4 to 1.9, `du -s`
1.3 to 1.6, `rsync -an` 1.5 to 2.2, 10 000 warm deep `stat`s 12 to 13,
`find -type f` 1.3 to 1.7. The rule of §8.1 (both within +20 %, else B)
fails by 2 to 8 times in every block (lowest bootstrap bound 2.0). Not
run: the dev-fuse transport, `make -j8` and a real source tree's `git status`.

## F21: W3b's narrowed notice against none

The narrowed notice (plan 42 §5.3.1, `crates/frontend-fuse/src/attrwatch.rs`)
is `CONSTELLATION_FUSE_DIR_CHANGE_NOTIFY=auto` in bounded mode; `off` is
the cost reference (no notice at all), `on` the first implementation's
(a notice for every operation). Every mount reports the setting it ran
(`node.status`), and the notices written and skipped. `entry` directory
TTL throughout; the same tree and stores as above; fresh mount per
sample; the stat poller of `cp_a_poll` is one process `stat`ing the
target and its two subtrees every 20 ms.

Cost, in one line: **requests equal `off`** (median difference 0.01 %, at
most +0.4 %: within 100 requests of 10 000 to 73 000), against `on`'s +5 000
`GETATTR`s in every creating or removing workload; **wall time close to
`off`**: the median of the cells' `auto`/`off` wall ratios is 1.01 on the
namespace workloads (range 0.69 to 1.16) and 1.02 on the read-only
workloads (0.97 to 1.12), the spread being the host's noise (the cells'
`on`/`off` ratios range 0.77 to 1.28 although `on` only adds requests).
A single-threaded `untar`/`cp -a`/`rm -rf` of 5 224 operations
wrote one narrowed notice (`on`: 5 224), 20 008 renames one, 6 002
`mkdir`/`rmdir` one; the polled `cp -a` one to four, since only the
polled directories change a few times.

An earlier build of the same code, which scanned every shard's marks at
each namespace operation, measured the same on all four stores (read-only
workloads, 64 cells: median wall ratio 1.00, 0.68 to 1.24, requests
equal; namespace workloads on ext4, xfs and btrfs: 1.06, 0.86 to 1.35,
requests +0.01 %, up to +2.6 %: `mv_storm` on btrfs, 27 032 against
26 350 where `on` makes 35 952); the final build skips the scan when no
mark is live.

### Namespace workloads, final build (2 blocks of ABBA-style palindromes `auto, off, on, on, off, auto`, 4 samples per mode per cell)

| store | workload | requests off / auto / on | GETATTR off / auto / on | notices auto / on | wall auto/off (block ratio) | wall on/off |
|---|---|---|---|---|---|---|
| ext4 | untar | 35742 / 35746 / 40772 | 24 / 26 / 5050 | 1 / 5224 | 1.03 | 1.03 |
| ext4 | cp_a | 72440 / 72430 / 77459 | 52 / 51 / 5074 | 1 / 5224 | 0.98 | 0.98 |
| ext4 | rm_rf | 10782 / 10791 / 15808 | 1 / 2 / 5002 | 1 / 5224 | 1.03 | 1.14 |
| ext4 | mv_storm | 26352 / 26357 / 35959 | 224 / 224 / 9826 | 1 / 20008 | 1.02 | 1.02 |
| ext4 | mkdir_storm | 9004 / 9005 / 15005 | 0 / 1 / 6001 | 1 / 6002 | 0.97 | 1.22 |
| ext4 | cp_a_poll | 73188 / 73136 / 78180 | 66 / 64 / 5069 | 1 / 5224 | 0.94 | 1.02 |
| xfs | untar | 35712 / 35712 / 40745 | 13 / 14 / 5042 | 1 / 5224 | 0.81 | 1.03 |
| xfs | cp_a | 72401 / 72408 / 77427 | 43 / 45 / 5066 | 1 / 5224 | 0.96 | 1.00 |
| xfs | rm_rf | 10742 / 10758 / 15768 | 1 / 2 / 5002 | 1 / 5224 | 1.09 | 1.26 |
| xfs | mv_storm | 26350 / 26350 / 35952 | 224 / 223 / 9825 | 1 / 20008 | 1.02 | 1.13 |
| xfs | mkdir_storm | 9004 / 9005 / 15005 | 0 / 1 / 6001 | 1 / 6002 | 1.12 | 1.19 |
| xfs | cp_a_poll | 73085 / 73099 / 78077 | 60 / 64 / 5064 | 2 / 5224 | 1.21 | 1.00 |
| btrfs | untar | 35702 / 35704 / 40729 | 10 / 12 / 5035 | 1 / 5224 | 0.97 | 1.01 |
| btrfs | cp_a | 72336 / 72346 / 77373 | 24 / 28 / 5050 | 1 / 5224 | 0.99 | 1.04 |
| btrfs | rm_rf | 10562 / 10605 / 15714 | 0 / 1 / 5001 | 1 / 5224 | 1.03 | 1.13 |
| btrfs | mv_storm | 26350 / 26344 / 35952 | 224 / 224 / 9826 | 1 / 20008 | 0.90 | 1.28 |
| btrfs | mkdir_storm | 9004 / 9005 / 15005 | 0 / 1 / 6001 | 1 / 6002 | 0.99 | 1.10 |
| btrfs | cp_a_poll | 72864 / 72876 / 77586 | 44 / 48 / 5038 | 4 / 5224 | 1.04 | 0.77 |
| tmpfs | untar | 35710 / 35715 / 40740 | 12 / 14 / 5038 | 1 / 5224 | 1.15 | 1.15 |
| tmpfs | cp_a | 72366 / 72366 / 77394 | 35 / 35 / 5058 | 1 / 5224 | 0.98 | 1.04 |
| tmpfs | rm_rf | 10712 / 10736 / 15750 | 1 / 2 / 5002 | 1 / 5224 | 0.99 | 1.06 |
| tmpfs | mv_storm | 26350 / 26350 / 35952 | 223 / 224 / 9826 | 1 / 20008 | 0.94 | 1.04 |
| tmpfs | mkdir_storm | 9004 / 9005 / 15005 | 0 / 1 / 6001 | 1 / 6002 | 0.75 | 1.16 |
| tmpfs | cp_a_poll | 73249 / 73158 / 78197 | 74 / 67 / 5070 | 1 / 5224 | 0.85 | 1.08 |

### Read-only workloads, final build, ext4 (4 ABBA blocks, `auto, off, off, auto`, 8 samples per mode per cell)

| workload | run | requests off / auto | wall auto/off (block ratio) |
|---|---|---|---|
| ls_lR | cold | 118948 / 118948 | 1.07 |
| ls_lR | warm | 118939 / 118939 | 0.96 |
| find_f | cold | 6705 / 6705 | 1.04 |
| find_f | warm | 6704 / 6704 | 1.00 |
| find_newer | cold | 61759 / 61759 | 1.01 |
| find_newer | warm | 61758 / 61758 | 1.04 |
| du_s | cold | 61758 / 61758 | 1.00 |
| du_s | warm | 61758 / 61758 | 1.05 |
| git_status | cold | 62070 / 62070 | 1.03 |
| git_status | warm | 6986 / 6986 | 1.08 |
| make_q | cold | 20337 / 20337 | 1.09 |
| make_q | warm | 10111 / 10111 | 0.95 |
| rsync_n | cold | 61766 / 61766 | 1.02 |
| rsync_n | warm | 61766 / 61766 | 1.00 |
| deep_stat | cold | 12222 / 12222 | 1.04 |
| deep_stat | warm | 0 / 0 | 1.00 |

## F21 follow-up (review F2/F3/F4): marks that never expire

`attrwatch.rs` dropped a mark a second after its reply was generated, which
let race 2b through for a reply the kernel installs later (over io_uring the
ring thread commits a reply on its next pass, after the requests it reaped
with it; a task can wait for a CPU longer than a second). A mark now goes on
evidence only: the task's next attribute or namespace request, a notice for
the inode, the task's exit (`kill(tid, 0)` when a change finds the mark or a
full table is swept); it is indexed by inode; the table is bounded (32 768
marks of tasks with an id, as many for pid 0), and a full table writes the
notice before the reply. Cost against `off`, ABBA (`auto, off, off, auto`),
`entry` directory TTL, io_uring, release build, a **reduced tree of 12 201
files** (a git repository, `a00`/`a01` of 5 242 entries, `src/` 4 000 files,
`deep/` 2 000, a 1 000-target Makefile; the 55 052-file tree of the sections
above was not rebuilt), fresh mount per sample; the host's other agents were
running (load 5 to 9). Stores: `/mnt/fs-ext4`, `/mnt/fs-xfs`, `/mnt/fs-btrfs`
and tmpfs. Namespace workloads: 2 blocks, so 4 samples per mode per cell;
read-only: 3 blocks, 6 samples per mode per cell, cold and warm. New
workloads: `cp_a_churn` and `mkdir_churn`, whose background task is a shell
loop that spawns `stat` of the target and its two subtrees as fast as it can
(a process, so a task id, per `stat`: the pruning cost with many short-lived
processes).

**Requests equal `off`**: over the 84 cells that are not churn the median
difference is 0.00 %, the largest +0.65 % (`rm_rf` on tmpfs, 9 759 against
9 644, where `off` alone ranges 9 644 to 10 281), the smallest -0.10 %. The 16
read-only cells of every store send identical requests (+/- 2 of
10 000 to 37 000). The churn workloads' requests vary by 5 to 10 % between
runs of the same mode (the loop's `stat`s are requests, and a faster run
spawns more); their GETATTR counts match (54 to 60 against 54 to 59), and
the narrowed notice wrote none: each `stat` task is gone when the change
finds its mark, so the mark is dropped by the exit probe. A single-task
`untar`/`cp_a`/`rm_rf`/`mv_storm`/`mkdir_storm` wrote none (the shell that
prepared the run has exited; the previous build wrote one); the polled
`cp_a` wrote 6 to 10 per run (a live poller stats the target every 20 ms:
one needless notice per change that finds its mark; the previous build, 1 to
4). Wall time, block ratios `auto`/`off` (host noise included; the
workloads' own CPU time moves with it): median 1.05 over the 32 namespace
cells (0.87 to 1.24), 0.99 over the 64 read-only cells (0.83 to 1.21).

| store | workload | requests off / auto | GETATTR off / auto | notices auto (per run) | wall auto/off (block ratio) |
|---|---|---|---|---|---|
| ext4 | untar | 36544 / 36546 | 8 / 10 | 0 (0-0) | 1.02 |
| ext4 | cp_a | 73292 / 73288 | 24 / 23 | 0 (0-0) | 0.98 |
| ext4 | rm_rf | 10062 / 10125 | 0 / 1 | 0 (0-0) | 1.09 |
| ext4 | mv_storm | 26260 / 26251 | 44 / 43 | 0 (0-0) | 0.87 |
| ext4 | mkdir_storm | 9004 / 9004 | 0 / 0 | 0 (0-0) | 1.05 |
| ext4 | cp_a_poll | 73810 / 73840 | 64 / 66 | 8 (7-9) | 1.11 |
| ext4 | cp_a_churn | 89510 / 89492 | 58 / 59 | 0 (0-0) | 0.97 |
| ext4 | mkdir_churn | 10414 / 10535 | 2 / 2 | 0 (0-0) | 1.01 |
| xfs | untar | 36536 / 36536 | 5 / 5 | 0 (0-0) | 1.22 |
| xfs | cp_a | 73263 / 73264 | 14 / 14 | 0 (0-0) | 1.02 |
| xfs | rm_rf | 10010 / 10053 | 0 / 1 | 0 (0-0) | 1.09 |
| xfs | mv_storm | 26161 / 26217 | 44 / 44 | 0 (0-0) | 1.06 |
| xfs | mkdir_storm | 9004 / 9004 | 0 / 0 | 0 (0-0) | 0.98 |
| xfs | cp_a_poll | 73793 / 73777 | 61 / 62 | 8 (8-10) | 1.06 |
| xfs | cp_a_churn | 88790 / 88422 | 56 / 57 | 0 (0-0) | 0.98 |
| xfs | mkdir_churn | 10499 / 10444 | 2 / 2 | 0 (0-0) | 1.04 |
| btrfs | untar | 36538 / 36538 | 6 / 6 | 0 (0-0) | 1.17 |
| btrfs | cp_a | 73280 / 73288 | 20 / 22 | 0 (0-0) | 1.03 |
| btrfs | rm_rf | 9880 / 9878 | 0 / 0 | 0 (0-0) | 1.21 |
| btrfs | mv_storm | 26194 / 26226 | 43 / 44 | 0 (0-0) | 1.13 |
| btrfs | mkdir_storm | 9004 / 9004 | 0 / 0 | 0 (0-0) | 0.95 |
| btrfs | cp_a_poll | 73656 / 73680 | 58 / 58 | 8 (8-10) | 1.08 |
| btrfs | cp_a_churn | 84632 / 86432 | 54 / 55 | 0 (0-0) | 0.99 |
| btrfs | mkdir_churn | 10621 / 10670 | 2 / 2 | 0 (0-0) | 1.00 |
| tmpfs | untar | 36536 / 36538 | 5 / 6 | 0 (0-0) | 1.24 |
| tmpfs | cp_a | 73272 / 73280 | 17 / 19 | 0 (0-0) | 1.08 |
| tmpfs | rm_rf | 9976 / 10040 | 0 / 1 | 0 (0-0) | 1.02 |
| tmpfs | mv_storm | 26138 / 26216 | 44 / 44 | 0 (0-0) | 1.05 |
| tmpfs | mkdir_storm | 9004 / 9004 | 0 / 0 | 0 (0-0) | 1.07 |
| tmpfs | cp_a_poll | 73804 / 73730 | 60 / 62 | 8 (6-10) | 1.07 |
| tmpfs | cp_a_churn | 88532 / 87874 | 56 / 56 | 0 (0-0) | 0.98 |
| tmpfs | mkdir_churn | 10336 / 10539 | 2 / 2 | 0 (0-0) | 1.17 |

Bookkeeping, `cargo test --release -p constellation-frontend-fuse --lib --
--ignored --nocapture attrwatch::tests::cost` (a task's `arrive` plus reply,
a change of another inode every 64th, per thread, 500 000 rounds; the box
shared; the previous build, same test, back to back, two runs each):
1 thread 112 / 111 ns (previous 112 / 112), 4 threads 342 / 342 ns (previous
404 / 406), 8 threads 1 116 / 1 053 ns (previous 1 305 / 1 252). Equal at one
thread, faster under contention (cache-line padded shards).

Plan 40 P6a (a task's marks are a set of inodes): the same test on a quiet
16-core box (load below 6), against the build before, ABBA, 16 runs each,
medians: 1 thread 113 / 112 ns, 4 threads 372 / 373 ns, 8 threads
1 064 / 1 109 ns. Within noise.

Pruning: sweeping a full table of 32 768 marks of tasks that no longer exist
takes 3 to 8 ms (about 0.1 to 0.25 us per mark: one `kill(2)` each, outside
the shard locks; at most once per 100 ms, so below 8 % of a thread even
with nothing to free); a change finding 10 000 marks of exited tasks on one
inode drops them in 0.9 ms; 3 000 real `true` processes marked and reaped,
then swept: 0.3 to 0.5 ms (the processes themselves took 550 ms).

## §8 acceptance: base against final on every backing store (K7)

**Not met as stated.** Two findings, details below: (1) the W2 worst case
(§8's report row) costs a factor of 11 to 16 in writes per second and 100 ms at p99, on every store;
(2) the commit-heavy row's per-commit p50 in the full cell is 17 to 50 % higher on the four disk stores
(beyond noise on btrfs only), single view as well, but a standalone run of the same workload on a fresh file system
shows no difference between any of the builds, so it is not reproduced as a property of the code alone. Every other
row is neutral or better, within the noise rule.

**Builds.** Base: `c3bda7ea`, the parent of K1. Final: `5964fd68`, origin/main when the run
started. Release mode, `--cto bounded`, write-through, io_uring transport, Linux 7.3.0 rc4
(distribution kernel, unpatched, so W1, W2 and W3b are all in effect), 16 cores, 30 GiB.
**The two builds differ by 106 commits, not only by plan 42**: K1 to K6, F21 and F2 are the first
eight commits after the base; the other ninety-odd are plan 40, 41, 43 and 45 work (among them
the journal flush at a clean close, the hard-link dentry layout, the lock lane). A regression
below is therefore a regression of `base..final`; `63c8790d` (the plan 42 commits alone) is the
control build for attributing one, see "Not attributed".

**Method.** A *cell* is one build on one store (`bench/kernel-races/cell.sh`): a fresh file
system whose backend (`file://`), state directory (metadata, journal) and chunk cache all lie on the
store, and these phases: `tar -x` of Appendix A.1's tree (20 x 10 x 10 directories of 25 files,
50 000 files, a packed git repository, a 5 000-target Makefile, built by `gen-tree.py`), `cp -a` of it;
remount, drop the kernel's caches, then each of `ls -lR`, `find -type f`, `git status --porcelain`,
`make -q`, `rsync -an --delete` of the tree against its copy, cold and at once again warm; reopen-read
(5 000 files of 16 KiB, 3 passes, `open`/`read`/`close` per file, remounted before pass 1); 100 000 `open`/`close`
of one file; `fio` (4 jobs, `psync`: sequential write 1 MiB, 4 x 256 MiB, fsync at the end; 4 KiB random write
and 4 KiB random 70/30, 20 s each); database style (`kb db`: one read-write handle on a 1 GiB file, 4 threads,
60 s, 4 KiB random reads and writes 50/50); commit-heavy (`kb commit`: 5 000 files of 4 KiB created, written
and closed, with one view, then with a second view of the same node attached and a thread there waiting for
all of them to appear); `pc_race` without an opener (20 s; 4 writers of half pages, 2 readers of the other
halves, as the harness's); `pc_race` with an opener (an `open`/`close` loop) with the defaults, with
`KEEP_CACHE=off`, and, final only, with `KEEP_CACHE=off WRITE_REPAIR=off` (W2 off). Request counts come
from `node.status` before and after each workload. Stores: ext4, xfs, btrfs (`compress` off) and btrfs
`compress=zstd` (`mount -o remount` of the same partition, restored to the original options afterwards),
each a partition of one NVMe device, and tmpfs. The cells ran as ABABAB per store (3 runs per
configuration) by `abba.sh`, results aggregated by `agg.py` and `report.py`: median and spread
(max minus min over the median) of the 3 runs; "beyond noise" is a final slower than the base by more than the
larger of 3 % and twice the base's spread.

**Host load.** Other agents shared the host. Before each phase (not only each cell) the cell waited for the
1-minute load average to be below 4 (it recorded the load and the wait; the longest wait was 27 minutes, in the first cell;
no phase gave up, 228 phases of the disk-store cells). The 1-minute load average includes this benchmark's own tasks waiting in FUSE
requests, so the phases' means read 1.4 to 11.2; a sampler (`sampler.py`, every 5 s) recorded the CPU
used by everything outside the benchmark's own session: at most 0.7 cores on average in any phase of the
disk-store cells (median 0.04), against 16 cores. A strict reading of "load below 4 during the benchmark" is therefore not what held;
the load at every phase's start was. The devices were shared too, which the CPU does not
show: `cp -a` ranged from 65 to 665 s between runs of the same configuration, so its wall time says nothing.

### Ratio final over base

| Benchmark | ext4 | xfs | btrfs | btrfs-zstd | tmpfs |
|---|---|---|---|---|---|
| untar 50k files | 0.99 | 1.10* | 0.96 | 0.96 | 0.91 |
| cp -a of it | 0.54 | 1.04 | 0.80 | 1.37 | 1.16 |
| ls -lR cold | 0.91 | 0.85 | 0.88 | 0.99 | 0.89 |
| ls -lR warm | 0.92 | 0.77 | 0.82 | 0.74 | 0.89 |
| find cold | 0.81 | 0.89 | 0.86 | 0.81 | 0.81 |
| find warm | 0.85 | 0.75 | 0.87 | 0.60 | 0.94 |
| git status cold | 0.97 | 0.95 | 0.86 | 0.86 | 1.00 |
| git status warm | 0.96 | 0.83 | 0.92 | 0.97 | 0.86 |
| make -q cold | 0.99 | 0.94 | 1.08 | 0.99 | 1.05 |
| make -q warm | 1.01 | 0.90 | 1.05 | 0.97 | 1.04 |
| rsync -n cold | 1.05 | 1.07 | 0.96 | 1.00 | 0.93 |
| rsync -n warm | 0.98 | 1.08 | 1.03 | 0.93 | 0.87 |
| reopen-read pass 1 | 1.09 | 0.94 | 0.74 | 0.78 | 1.11 |
| reopen-read pass 2 | 0.63 | 0.63 | 0.69 | 0.54 | 0.48 |
| reopen-read pass 3 | 0.64 | 0.69 | 0.69 | 0.68 | 0.50 |
| open+close mean | 1.05 | 0.56 | 0.93 | 1.01 | 0.60 |
| open+close p50 | 1.34 | 0.52 | 0.85 | 0.99 | 0.59 |
| open+close p99 | 0.77 | 0.66 | 1.00 | 1.00 | 0.71 |
| fio seq write 1M, MiB/s | 1.00 | 1.00 | 0.96 | 0.90 | 0.99 |
| fio rand write 4k, IOPS | 1.00 | 1.02 | 1.04 | 1.17 | 1.00 |
| fio rand rw 70/30, read IOPS | 1.05 | 1.05 | 1.01 | 0.87 | n/a |
| fio rand rw 70/30, write IOPS | 1.04 | 1.06 | 1.00 | 0.86 | n/a |
| pc_race no opener, writes | 1.07 | 1.16 | 1.06 | 0.92 | 1.03 |
| pc_race no opener, write p50 | 1.08 | 1.06 | 1.10 | 0.90 | 3.08 |
| pc_race no opener, write p99 | 1.25 | 0.91 | 0.78 | 1.17 | 0.91 |
| db 4 threads, ops/s | 0.89 | 1.02 | 1.00 | 1.00 | n/a |
| db read p50 | 1.00 | 0.99 | 1.00 | 1.00 | n/a |
| db read p99 | 1.00 | 1.01 | 0.80 | 3.57 | n/a |
| db write p50 | 1.06 | 1.02 | 0.96 | 1.00 | n/a |
| db write p99 | 1.01 | 0.91 | 1.01 | 1.10 | n/a |
| commit-heavy, 1 view, per commit mean | 1.07 | 1.17 | 1.42 | 2.05 | 0.96 |
| commit-heavy, 1 view, per commit p50 | 1.28 | 1.17 | 1.45* | 1.50 | 1.02 |
| commit-heavy, 1 view, per commit p99 | 0.97 | 0.79 | 1.00 | 1.02 | 0.70 |
| commit-heavy, 2 views, per commit mean | 0.94 | 1.17 | 1.12 | 1.41 | 0.93 |
| commit-heavy, 2 views, per commit p50 | 1.06 | 1.16 | 1.47* | 1.48 | 0.97 |
| commit-heavy, 2 views, per commit p99 | 0.78 | 1.03 | 1.03 | 1.06 | 0.60 |
| commit-heavy, 2 views, all seen in the other | 0.88 | 1.17 | 1.05 | 1.37 | 0.84 |

(`n/a`: not valid, see tmpfs below. tmpfs `pc_race` p50 is bimodal, 14 to 33 us or 100 to 105 us from run to run on either build, so its ratio says nothing.) Row by row against §8's table:

- **reopen-read**: *faster*. The daemon's `read` requests in passes 2 and 3: 5 000 + 5 000 on the base,
  0 + 0 on the final (all stores); pass 1 5 000 on both. Wall time of passes 2 and 3 is 0.54 to 0.69 of the base
  on every store.
- **`git status`, `ls -lR`, `find`, `make -q`, `rsync -n`**: neutral or faster (0.60 to 1.08, none beyond noise
  on a disk store); the requests are the same to within 0.2 % (e.g. `git status` warm 7 049 against 6 984, `ls -lR`
  118 906 against 118 903, `GETATTR` of `ls -lR` 3 against 2): W3a's `entry` default and F21's narrowed notice cost
  nothing here. The faster side is not W3's doing (the requests are equal); it was not attributed.
- **untar, `cp -a`**: requests equal (untar 386 196 against 386 184 on ext4; the narrowed notice wrote none:
  57 264 changed inodes skipped it); wall time neutral except **xfs untar, 309 s against 340 s (1.10, beyond noise)**:
  the three final runs took 308, 346 and 340 s, the base's 317, 307 and 309; no other store shows it (0.96 to
  0.99), tmpfs 0.89. A standalone repeat (three rounds of base, `63c8790d`, final in turn) gave base 314, 278, 332 s, plan 42 alone 351, 297, 349 s, final 328, 316, 336 s: a 17 % spread, no effect beyond it. `cp -a`: see Host load.
- **`fio`** (4 jobs): neutral, 0.96 to 1.06 on ext4, xfs and btrfs; 0.86 to 1.17 on btrfs `zstd`, whose spreads are 6 to 89 %.
- **`pc_race` without an opener**: neutral (writes 0.92 to 1.16 of the base on the final's side of the ratio, spreads 2 to 60 %).
- **open latency**: neutral or better (mean 25 to 43 us on the final; ext4 p50 25 to 33 us has a 65 % spread, xfs
  45 to 23 us).
- **database style**: neutral (ops/s 0.89 to 1.02; ext4's 230 against 259 has an 18 % base spread). Every
  p50 within 6 %.
- **commit-heavy**: see below.

### W2's cost: `pc_race` with an opener (§8's report row)

| Store | variant | writes | stale read-backs | write p50 | write p99 | repairs | repairs released |
|---|---|---|---|---|---|---|---|
| ext4 | base (no W1/W2) | 335 341 | 567 | 216 us | 307 us | - | - |
| ext4 | final, defaults | 28 863 | 0 | 235 us | 108.0 ms | 1 698 | 170 |
| ext4 | final, KEEP_CACHE=off, repair on | 32 557 | 0 | 235 us | 106.7 ms | 1 788 | 168 |
| ext4 | final, KEEP_CACHE=off, repair off | 331 337 | 573 | 219 us | 308 us | - | - |
| xfs | base (no W1/W2) | 352 275 | 523 | 205 us | 296 us | - | - |
| xfs | final, defaults | 27 725 | 0 | 233 us | 107.7 ms | 1 629 | 171 |
| xfs | final, KEEP_CACHE=off, repair on | 32 895 | 0 | 235 us | 107.2 ms | 1 782 | 167 |
| xfs | final, KEEP_CACHE=off, repair off | 329 478 | 562 | 220 us | 311 us | - | - |
| btrfs | base (no W1/W2) | 368 903 | 499 | 195 us | 283 us | - | - |
| btrfs | final, defaults | 30 980 | 0 | 216 us | 107.5 ms | 1 734 | 171 |
| btrfs | final, KEEP_CACHE=off, repair on | 37 357 | 0 | 220 us | 105.7 ms | 1 989 | 168 |
| btrfs | final, KEEP_CACHE=off, repair off | 349 485 | 481 | 207 us | 296 us | - | - |
| btrfs-zstd | base (no W1/W2) | 364 257 | 507 | 197 us | 288 us | - | - |
| btrfs-zstd | final, defaults | 30 795 | 0 | 217 us | 107.7 ms | 1 662 | 170 |
| btrfs-zstd | final, KEEP_CACHE=off, repair on | 31 859 | 0 | 222 us | 106.8 ms | 1 703 | 169 |
| btrfs-zstd | final, KEEP_CACHE=off, repair off | 351 014 | 497 | 206 us | 294 us | - | - |
| tmpfs | base (no W1/W2) | 473 672 | 332 | 149 us | 227 us | - | - |
| tmpfs | final, defaults | 28 950 | 0 | 166 us | 107.8 ms | 1 799 | 175 |
| tmpfs | final, KEEP_CACHE=off, repair on | 41 227 | 0 | 169 us | 106.2 ms | 2 201 | 169 |
| tmpfs | final, KEEP_CACHE=off, repair off | 446 631 | 288 | 157 us | 241 us | - | - |

Per store the base is the row without W1/W2. The final's writes drop to a ninth to a sixteenth, with the p50 within 20 us of the base's and **p99 106 to 108 ms** against 0.3 ms, on every store, with `KEEP_CACHE` at its default or off alike: the stale
read-backs the base shows (480 to 570 in 20 s) are 0 on the final, and 480 to 570 again with the repair off (W1
alone does not prevent them with this workload: the opener drops the pages). **First look at the cause**: `pc_race` writes
the first half of a page, so the repair's notification finds the write's own last page partial and locked (the writer holds
it until its reply) and waits for `TAIL_GRACE`, 100 ms (`crates/frontend-fuse/src/repair.rs`); the counters agree: about
1 700 repairs per run, of which 170 (10 %) were "released" by that grace, and the p99 sits at the grace. The repair lane has two
threads (`LANE_THREADS`) and a FIFO queue, so while those jobs sit out their 100 ms the other writers' repairs queue
behind them: the 29 000 writes of a run spend all of their four threads' 80 s in writes (mean 2.7 ms). Not fixed here; the plan's
§7.5 row "half page: writes 107 ms" is the same stall seen on a patched kernel. On the unpatched kernel the plan's `kernel-race-repair-stall` scenario
(whole pages) answers its writes in under 1 ms (§7.5), so the stall is specific to partial-page writers.

### Commit-heavy, two views: the cost of Q3's notification

With a second view attached the final sends many more requests: ext4 80 041 against 35 156 (`GETATTR` 3 746 against 29,
`LOOKUP` 51 208 against 10 127), xfs 55 747 against 35 055, btrfs 53 180 against 35 045, btrfs `zstd` 49 790 against 35 019,
because every commit invalidates the file in the other view, whose waiting thread then looks it up again;
with one view the requests are equal (30 022 against 30 022). The writer's cost per commit with two views is the
same as with one view on the final (ext4 p50 2 062 against 2 008 us, xfs 1 572 against 1 562, btrfs 861 against 853,
`zstd` 841 against 841): the notification itself is not visible in the writer's latency, and the second view's thread sees
all 5 000 files 0.0 to 0.2 s after the last close on the final, against 0.1 to 1.0 s on the base (its attribute lifetime): Q3's
notification shortens the other view's lag.

**The final's per-commit latency is higher than the base's in the full cell, with one view as well** (p50: ext4 1 571 to
2 008 us, xfs 1 339 to 1 562, btrfs 590 to 853 (beyond noise, spreads 7 and 3 %), btrfs `zstd` 559 to 841; tmpfs
1.02; p99 equal). This is not Q3's notification (one view has none) and not W3b (a `create` costs no notice; the notices written are 0).

### Attribution runs: the commit-heavy rise is not reproduced on a fresh file system

`micro.sh commit`: the same 5 000-file workload, one view, each run on a newly created file system and a fresh mount
(no earlier phases), four rounds in alternating order, on btrfs, ext4 and xfs, with four builds: base, `63c8790d` (K1 to F2 alone, "plan 42"),
final, and final with `KEEP_CACHE=off WRITE_REPAIR=off DIR_CHANGE_NOTIFY=off`. Per-commit p50 (us), medians of the four rounds:

| store | base | plan 42 alone | final | final, W1/W2/W3b off |
|---|---|---|---|---|
| btrfs | 275 | 284 | 266 | 275 |
| ext4 | 269 | 273 | 265 | 262 |
| xfs | 239 | 251 | 248 | 232 |

All within the single runs' spread (btrfs base 198 to 284); no build is slower. So the rise seen in the cells needs the state that the earlier phases leave (the fio
and database phases wrote about 3 GiB through the same mount before the commit phase; the sampler shows no other work), and it is not
attributed to any of the plan 42 commits or knobs: the plan 42 commits alone and the final with its three workarounds off behave like the base on a clean file system.
A cell-level control (the same phases with `63c8790d`) was not run.

### Raw counters of the final, reopen-read pass 2 and 3 (any store)

`keep_cache_opens` 5 000 in each of passes 2 and 3 and `read` 0; the 100 000 opens of the open-latency run are all kept opens (100 001 with the warm-up), 3 to 4 `GETATTR`s on either build.

### tmpfs

The first attempt filled the host's 16 GiB of `/dev/shm` (the backend keeps every version of the fio and database files and
the chunk cache holds 4 GiB more) at the database phase. The cells were rerun (`FRESH=1`, which `abba.sh` sets for tmpfs: the
writing phases start on a new, empty file system, with a 2 GiB cache; the open-latency file is its own file), and
the tables are that rerun, 3 runs per build. Its `fio` random 70/30 job produced no result on either build (not investigated, `n/a`).
Two cells had other work of just above 1 core in the `misc` phase. The tmpfs rows show nothing beyond noise except the W2 worst case
(writes 29 000 against 474 000 per 20 s, p99 108 ms against 0.2 ms).

### Reproduce

`gcc -O2 -pthread -o kb kb.c`; `gen-tree.py DIR` and `tar` it; `abba.sh JOBS OUT` with lines `STORE VERSION RUN`; `agg.py OUT`,
`report.py OUT`. Absolute medians (base / final):

| Metric | ext4 | xfs | btrfs | btrfs-zstd | tmpfs |
|---|---|---|---|---|---|
| untar 50k files | 424 / 420 | 309 / 340 | 281 / 270 | 304 / 292 | 85.4 / 78 |
| cp -a of it | 255 / 138 | 185 / 193 | 379 / 304 | 213 / 292 | 254 / 295 |
| ls -lR cold | 4.5 / 4.1 | 4.58 / 3.88 | 3.83 / 3.38 | 3.26 / 3.22 | 2.94 / 2.6 |
| ls -lR warm | 2.83 / 2.62 | 2.74 / 2.12 | 2.94 / 2.42 | 3.17 / 2.35 | 2.89 / 2.56 |
| find cold | 0.316 / 0.256 | 0.262 / 0.232 | 0.277 / 0.237 | 0.236 / 0.191 | 0.311 / 0.251 |
| find warm | 0.261 / 0.223 | 0.229 / 0.171 | 0.241 / 0.21 | 0.257 / 0.155 | 0.23 / 0.217 |
| git status cold | 0.658 / 0.637 | 0.693 / 0.659 | 0.677 / 0.584 | 0.677 / 0.581 | 0.578 / 0.577 |
| git status warm | 0.238 / 0.229 | 0.268 / 0.222 | 0.238 / 0.219 | 0.237 / 0.23 | 0.27 / 0.232 |
| make -q cold | 0.857 / 0.85 | 0.869 / 0.814 | 0.862 / 0.928 | 0.853 / 0.844 | 0.884 / 0.929 |
| make -q warm | 0.49 / 0.494 | 0.49 / 0.443 | 0.514 / 0.539 | 0.475 / 0.462 | 0.454 / 0.47 |
| rsync -n cold | 5.5 / 5.76 | 5.29 / 5.66 | 5.46 / 5.25 | 4.96 / 4.98 | 4.19 / 3.89 |
| rsync -n warm | 4.15 / 4.09 | 3.47 / 3.75 | 4.07 / 4.21 | 4.25 / 3.97 | 4.21 / 3.65 |
| reopen-read pass 1 | 0.414 / 0.451 | 0.471 / 0.444 | 0.436 / 0.325 | 0.423 / 0.33 | 0.398 / 0.443 |
| reopen-read pass 2 | 0.27 / 0.171 | 0.274 / 0.171 | 0.188 / 0.129 | 0.239 / 0.129 | 0.276 / 0.133 |
| reopen-read pass 3 | 0.269 / 0.172 | 0.247 / 0.171 | 0.188 / 0.13 | 0.187 / 0.128 | 0.261 / 0.129 |
| open+close mean | 30 us / 32 us | 45 us / 25 us | 39 us / 36 us | 43 us / 43 us | 43 us / 26 us |
| open+close p50 | 25 us / 33 us | 45 us / 23 us | 39 us / 33 us | 44 us / 43 us | 43 us / 25 us |
| open+close p99 | 59 us / 46 us | 65 us / 42 us | 60 us / 60 us | 63 us / 63 us | 60 us / 43 us |
| fio seq write 1M, MiB/s | 125 / 124 | 131 / 131 | 60 / 63 | 61 / 67 | 919 / 924 |
| fio rand write 4k, IOPS | 267 / 268 | 187 / 183 | 26 / 25 | 28 / 24 | 3 773 / 3 766 |
| fio rand rw 70/30, read IOPS | 492 / 469 | 367 / 349 | 83 / 82 | 80 / 92 | n/a / n/a |
| fio rand rw 70/30, write IOPS | 219 / 211 | 165 / 156 | 38 / 38 | 37 / 43 | n/a / n/a |
| pc_race no opener, writes | 455 653 / 425 731 | 600 800 / 519 648 | 497 306 / 469 557 | 419 477 / 453 979 | 775 107 / 755 643 |
| pc_race no opener, write p50 | 43 us / 46 us | 32 us / 34 us | 39 us / 43 us | 48 us / 44 us | 33 us / 103 us |
| pc_race no opener, write p99 | 3393 us / 4231 us | 3887 us / 3551 us | 4923 us / 3831 us | 3942 us / 4598 us | 380 us / 346 us |
| db 4 threads, ops/s | 230 / 259 | 177 / 173 | 45 / 45 | 39 / 39 | n/a / n/a |
| db read p50 | 183 us / 184 us | 198 us / 197 us | 204 us / 203 us | 198 us / 198 us | n/a / n/a |
| db read p99 | 66.6 ms / 66.4 ms | 119.2 ms / 120.8 ms | 971.5 ms / 774.3 ms | 314.1 ms / 1119.9 ms | n/a / n/a |
| db write p50 | 994 us / 1051 us | 884 us / 899 us | 637 us / 614 us | 631 us / 632 us | n/a / n/a |
| db write p99 | 510.7 ms / 515.5 ms | 939.9 ms / 859.3 ms | 2400.7 ms / 2427.6 ms | 2142.9 ms / 2357.2 ms | n/a / n/a |
| commit-heavy, 1 view, per commit mean | 2229 us / 2396 us | 1360 us / 1588 us | 3979 us / 5.7 ms | 1225 us / 2515 us | 262 us / 252 us |
| commit-heavy, 1 view, per commit p50 | 1571 us / 2008 us | 1339 us / 1562 us | 590 us / 853 us | 559 us / 841 us | 240 us / 245 us |
| commit-heavy, 1 view, per commit p99 | 9.8 ms / 9.5 ms | 2693 us / 2124 us | 1002 us / 1004 us | 986 us / 1002 us | 730 us / 509 us |
| commit-heavy, 2 views, per commit mean | 2838 us / 2672 us | 1391 us / 1621 us | 961 us / 1078 us | 612 us / 861 us | 312 us / 291 us |
| commit-heavy, 2 views, per commit p50 | 1941 us / 2062 us | 1354 us / 1572 us | 585 us / 861 us | 570 us / 841 us | 249 us / 240 us |
| commit-heavy, 2 views, per commit p99 | 8.9 ms / 6.9 ms | 3175 us / 3276 us | 1011 us / 1040 us | 954 us / 1007 us | 1462 us / 876 us |
| commit-heavy, 2 views, all seen in the other | 15.2 / 13.4 | 7.14 / 8.32 | 5.11 / 5.39 | 3.14 / 4.3 | 2.11 / 1.76 |
