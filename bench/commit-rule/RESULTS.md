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
operations are the fast path); after the review round, also two honour reads
in the fast path's gate-off acknowledgement (only a row that waited under a
durability gate takes it) and a clone of the close's commit inputs on a
time-bounded lone root (so a stalled close can re-commit through the core).
Measured with release builds, `ACK_LATENCY_TOPOLOGIES=1` (a lone root,
own-client operations), `ACK_LATENCY_SAMPLES=300`, seven rounds per file
system; each round runs three binaries in a rotating order: the parent of the
change (`base`), a second copy of the same parent binary (`base'`, the A/A:
the noise of a cell) and the change (`new`). ext4, xfs and btrfs on separate
partitions of one device, tmpfs under `/tmp`. The box was shared with other
jobs during the runs (1-minute load 15 to 32), so the noise is larger than on
a quiet box; the A/A shows how large. Microseconds; each cell is the median
over the seven rounds of the round's p50, p90 and p99; `d` is against `base`.

| fs | op | n | base p50/p90/p99 | base' (A/A) p50/p90/p99 | new p50/p90/p99 | A/A d p50 | A/A d p90 | new d p50 | new d p90 |
|---|---|--:|---|---|---|--:|--:|--:|--:|
| ext4 | create | 7 | 88 / 121 / 565 | 83 / 101 / 166 | 69 / 97 / 131 | -5 | -20 | -19 | -24 |
| ext4 | rename | 7 | 70 / 107 / 961 | 99 / 136 / 1043 | 75 / 106 / 1078 | +29 | +29 | +5 | -1 |
| ext4 | unlink | 7 | 56 / 85 / 997 | 69 / 110 / 1035 | 59 / 86 / 870 | +13 | +25 | +3 | +1 |
| ext4 | chmod | 7 | 37 / 63 / 609 | 46 / 86 / 686 | 35 / 66 / 405 | +9 | +23 | -2 | +3 |
| ext4 | write | 7 | 3084 / 3953 / 4893 | 3113 / 4038 / 4864 | 3091 / 3999 / 4956 | +29 | +85 | +7 | +46 |
| xfs | create | 7 | 74 / 102 / 199 | 83 / 115 / 256 | 79 / 106 / 177 | +9 | +13 | +5 | +4 |
| xfs | rename | 7 | 71 / 123 / 230 | 72 / 98 / 182 | 80 / 118 / 241 | +1 | -25 | +9 | -5 |
| xfs | unlink | 7 | 57 / 88 / 155 | 56 / 89 / 138 | 58 / 108 / 155 | -1 | +1 | +1 | +20 |
| xfs | chmod | 7 | 37 / 67 / 111 | 37 / 66 / 87 | 37 / 69 / 108 | +0 | -1 | +0 | +2 |
| xfs | write | 7 | 1978 / 2108 / 3256 | 1991 / 2125 / 3353 | 2011 / 2181 / 3931 | +13 | +17 | +33 | +73 |
| btrfs | create | 7 | 86 / 122 / 187 | 63 / 80 / 125 | 65 / 92 / 138 | -23 | -42 | -21 | -30 |
| btrfs | rename | 7 | 71 / 105 / 152 | 73 / 109 / 185 | 74 / 103 / 184 | +2 | +4 | +3 | -2 |
| btrfs | unlink | 7 | 58 / 93 / 153 | 59 / 90 / 122 | 59 / 91 / 154 | +1 | -3 | +1 | -2 |
| btrfs | chmod | 7 | 37 / 70 / 116 | 36 / 67 / 109 | 38 / 72 / 116 | -1 | -3 | +1 | +2 |
| btrfs | write | 7 | 3070 / 3799 / 4017 | 3087 / 3796 / 4171 | 3070 / 3807 / 4124 | +17 | -3 | +0 | +8 |
| tmpfs | create | 7 | 84 / 107 / 217 | 82 / 101 / 168 | 84 / 103 / 149 | -2 | -6 | +0 | -4 |
| tmpfs | rename | 7 | 71 / 107 / 188 | 74 / 114 / 225 | 66 / 92 / 205 | +3 | +7 | -5 | -15 |
| tmpfs | unlink | 7 | 57 / 85 / 134 | 60 / 108 / 177 | 57 / 91 / 202 | +3 | +23 | +0 | +6 |
| tmpfs | chmod | 7 | 35 / 64 / 98 | 37 / 80 / 130 | 37 / 67 / 102 | +2 | +16 | +2 | +3 |
| tmpfs | write | 7 | 118 / 183 / 338 | 133 / 218 / 411 | 132 / 196 / 466 | +15 | +35 | +14 | +13 |

Reading: every cell of `new` is inside the spread the A/A shows for its own
cell or for the same operation on another file system (the A/A moves by as much
as -42 to +85 µs; `new` by -30 to +73, the largest being the 2 ms `write` of xfs,
+33 µs or 1.7% at p50, where a single honour read cannot be 33 µs).
The two cells the first measurement (five rounds, no xfs or btrfs A/A) flagged,
ext4 `chmod` (+12/+20 then) and tmpfs `unlink` (+12/+26 then), are -2/+3 and
+0/+6 now, against A/A of +9/+23 and +3/+23: that was box noise. `create`, the
answer the plan gates, is -19/-24 (ext4), +5/+4 (xfs), -21/-30 (btrfs) and
+0/-4 (tmpfs), the btrfs and ext4 gains being the A/A's own spread. Neutral on
all four file systems.
