# Commit rule: `ack-latency` results (plan 43)

Per milestone, on four file systems (ext4, xfs, btrfs, tmpfs): the median over
five runs of p50 and p90 and the largest p99, per topology, actor and op, from
`bench/commit-rule/run.sh` (the `ack-latency` harness scenario,
`crates/harness/src/scenarios/commit.rs`).

**Status: the C0 baseline is pending its measurement run.** C0 ships the
scenario, the script and this file; the four-file-system baseline needs a
quiet host, mounts of each file system (xfs and btrfs need root) and hours,
and no code depends on it. It is a separate measurement job that runs before
C1 starts (listed as an open exit item in `docs/plans/v1/PROGRESS.md`,
"Plan 43 C0"). The run fills in the tables below and replaces this note:

    bench/commit-rule/run.sh --fs ext4=/mnt/ext4,xfs=/mnt/xfs,btrfs=/mnt/btrfs,tmpfs=/dev/shm

## C0 baseline

| file system | topology | actor | op | p50 (us) | p90 (us) | max p99 (us) |
|---|---|---|---|---:|---:|---:|
| _pending_ | | | | | | |

## C2 (plan 43 C2c, C2d; G1)

The delegate's acknowledgement waits for its group (C2c) and its fast path
writes, then waits (C2d). Gate G1 (plan 43 section 10): topology 3's
delegate (the root near, no recorded backup) within the budget of topology 2's
holder (a backed holder) plus 500 us, at p50 and p90, per op. Measured by
`bench/commit-rule/run.sh` (the `ack-latency` scenario) on **one file
system only**: release builds, loopback, box 1 with ext4 state, 8 runs per
tree and 200 samples per run, the trees in alternating and rotating order, a
run started only with the one-minute load below 4; the medians of the 8
runs' p50 and p90, in microseconds. The four-file-system run of the plan
(ext4, xfs, btrfs, tmpfs, five runs) is not done yet: it needs the quiet host
and root for xfs and btrfs, and it stays open with the C0 baseline above
(still pending): the "pre-C2c" column is a build of the parent of C2c, not
C0's. The numbers are those of PROGRESS ("Plan 43 C2c", "Plan 43 C2d", "Plan
43 G1's p90"), copied here as plan 43 section 9 asks.

| op | holder, topology 2: p50 / p90 | delegate, topology 3, pre-C2c | C2c | C2d | G1 (holder + 500) |
|---|---|---|---|---|---|
| create | 361 / 584 | 143 / 241 | 471 / 784 | 462 / 758 | met / met |
| rename | 258 / 349 | 127 / 230 | 522 / 1487 | 455 / 1180 | met / over by 331 |
| unlink | 239 / 342 | 109 / 214 | 445 / 1742 | 434 / 1185 | met / over by 343 |
| chmod | 235 / 365 | 60 / 142 | 423 / 2044 | 396 / 878 | met / over by 13 |

Topology 4 (a recorded backup), the delegate's p50 / p90: create 497 / 774
(pre-C2c), 508 / 823 (C2c), 487 / 710 (C2d); rename 610 / 1580, 578 / 1581,
488 / 1243; unlink 487 / 1652, 519 / 1719, 461 / 1188; chmod 438 / 1575,
433 / 1586, 427 / 562. `write` (4 KiB and `fsync`, dominated by the disk;
reported as the median across runs, the host met 9-36 ms `fsync` stalls in
some of them): topology 3 3,212 / 4,236 (pre-C2c), 3,551 / 4,469 (C2c), 3,516 /
4,471 (C2d); topology 4 3,587 / 4,537, 3,590 / 4,509, 3,614 / 4,522; topology 2's
holder 3,428, 3,430, 3,403 (p50).

C2d meets G1 at p50 on all four ops and at p90 on `create`; the p90 of
`rename` and `unlink` is over the budget by 0.33 ms, `chmod`'s by 13 us. The
tail is the root's append round trip landing behind a core step on the
delegate or the root.

### G1's p90, the two delays (plan 43 G1 p90; main `62fa6c7f`; not landed)

`bench/commit-rule/g1-abba.sh`, ABBA on box 1 against main, 8 rounds, 200
samples, topology 3's delegate, p50 / p90 in us (topology 2's holder on main
in brackets). The acknowledgement on the urgent lane gained nothing; a backed
root shipping appended rows with its next regular round (the deferral) cut
p90 by 51-58 % under load and is **not landed**: it widens the C3 section 5.4
item 4 gap (`placement-hot` with the lease-reads knob, 1 to 3 of 2,000 seeds),
so it waits for C3 (plan 46 M1.2).

| op | load below 1: main -> deferral | load 18-21: main -> deferral |
|---|---|---|
| create | 499 / 1,120 -> 494 / 1,022 (383 / 717) | 1,222 / 2,367 -> 790 / 1,164 (784 / 1,814) |
| rename | 490 / 1,301 -> 488 / 1,421 (267 / 468) | 1,202 / 2,508 -> 741 / 1,129 (390 / 1,095) |
| unlink | 495 / 1,356 -> 482 / 1,407 (265 / 380) | 1,252 / 2,556 -> 714 / 1,064 (364 / 844) |
| chmod | 451 / 630 -> 432 / 757 (228 / 373) | 1,114 / 2,256 -> 665 / 1,005 (305 / 788) |

### C2e to C2g

C2e (the end of a delegation waits for its group), C2f (a lone root answers
other nodes from the archive) and C2g (the settle before `EIO`) add no wait
to the steady-state write path of topologies 1 to 4: C2f's archive wait is on
a lone root's answer to another node (topology 5, the WAN rows, still to be
measured with the four-file-system run: expected 28-190 ms by S3 distance,
plan 43 section 7), and C2e and C2g act at a generation's end and at a
deadline. Their sweeps and oracle counts are in PROGRESS, not here.

## ADR-52: the stale-clock re-check (plan 46 M1.3), topology 1

Cost of the rule: one clock read per `Singleton` answer in the core and one
honour read per `Singleton` fast-path write (a lone root's own-client
operations are the fast path). Measured on the quiet box with release builds,
`ACK_LATENCY_TOPOLOGIES=1` (a lone root, own-client operations), five
rounds of `run.sh --runs 5 --samples 300` per file system and tree (the
parent of the change and the change, in alternating order, each round's
medians), ext4, xfs and btrfs on separate devices, tmpfs under `/tmp`.
Microseconds; each cell is the median over the five rounds of the round's
p50, p90 and largest p99.

| fs | op | rounds | base p50/p90/p99 | new p50/p90/p99 | d p50 | d p90 |
|---|---|---:|---|---|---:|---:|
| ext4 | create | 5 | 82 / 101 / 898 | 86 / 110 / 926 | +4 | +9 |
| ext4 | rename | 5 | 89 / 116 / 1108 | 91 / 122 / 1149 | +2 | +6 |
| ext4 | unlink | 5 | 62 / 85 / 1157 | 65 / 88 / 1085 | +3 | +3 |
| ext4 | chmod | 5 | 36 / 60 / 988 | 48 / 80 / 914 | +12 | +20 |
| ext4 | write | 5 | 3117 / 4016 / 5251 | 3119 / 4011 / 6530 | +2 | -5 |
| xfs | create | 5 | 86 / 110 / 195 | 84 / 113 / 193 | -2 | +3 |
| xfs | rename | 5 | 89 / 121 / 205 | 72 / 99 / 186 | -17 | -22 |
| xfs | unlink | 5 | 60 / 97 / 177 | 60 / 88 / 168 | +0 | -9 |
| xfs | chmod | 5 | 35 / 61 / 136 | 35 / 61 / 125 | +0 | +0 |
| xfs | write | 5 | 1955 / 2064 / 2730 | 1951 / 2064 / 2810 | -4 | +0 |
| btrfs | create | 5 | 89 / 116 / 187 | 86 / 105 / 174 | -3 | -11 |
| btrfs | rename | 5 | 92 / 128 / 194 | 77 / 100 / 180 | -15 | -28 |
| btrfs | unlink | 5 | 70 / 99 / 158 | 58 / 81 / 149 | -12 | -18 |
| btrfs | chmod | 5 | 42 / 75 / 127 | 37 / 66 / 125 | -5 | -9 |
| btrfs | write | 5 | 3097 / 3797 / 4307 | 3084 / 3805 / 4784 | -13 | +8 |
| tmpfs | create | 5 | 83 / 105 / 175 | 85 / 107 / 216 | +2 | +2 |
| tmpfs | rename | 5 | 68 / 92 / 336 | 79 / 125 / 397 | +11 | +33 |
| tmpfs | unlink | 5 | 56 / 84 / 172 | 68 / 110 / 220 | +12 | +26 |
| tmpfs | chmod | 5 | 35 / 62 / 144 | 35 / 65 / 147 | +0 | +3 |
| tmpfs | write | 5 | 129 / 200 / 1045 | 129 / 189 / 951 | +0 | -11 |

The same protocol with the parent's binary on both sides (an A/A, five rounds,
the two copies in alternating order) shows the run-to-run noise of a cell:

| fs | op | A p50/p90 | A' p50/p90 | d p50 | d p90 |
|---|---|---|---|---:|---:|
| ext4 | create | 80 / 105 | 89 / 111 | +9 | +6 |
| ext4 | rename | 84 / 113 | 83 / 114 | -1 | +1 |
| ext4 | unlink | 66 / 105 | 60 / 89 | -6 | -16 |
| ext4 | chmod | 39 / 62 | 36 / 63 | -3 | +1 |
| tmpfs | create | 84 / 101 | 88 / 118 | +4 | +17 |
| tmpfs | rename | 69 / 99 | 80 / 120 | +11 | +21 |
| tmpfs | unlink | 55 / 92 | 60 / 94 | +5 | +2 |
| tmpfs | chmod | 35 / 65 | 35 / 62 | +0 | -3 |

The changes in `create`, the answer the plan gates, are inside that noise on
every file system (+4/+9 ext4, -2/+3 xfs, -3/-11 btrfs, +2/+2 tmpfs); the other
operations move both ways by as much between identical binaries. Neutral on
all four file systems.
