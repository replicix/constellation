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

Not run: no `/dev/kvm` and no QEMU on the test host.

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
