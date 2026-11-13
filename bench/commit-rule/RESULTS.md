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


## Plan 46 M1.3 step 9, the own stream's honour rule (ADR-55, R-H / R-X): topologies 1 and 8

A = the step before this round (`aae238d3`), B = this round; ABBA (A B, B A),
two rounds, 200 samples per op and run, one box, the harness's own daemons, each
file system as the `TMPDIR` (`bench/commit-rule/g1-abba.sh`, topologies `1` and
`8`; one 1-minute load before each run: 0.04 to 0.5). Topology 8 runs under
`net-wan` (`tc netem`, a 200 ms round trip) with `ACK_LATENCY_SLASH_LOAD=1`:
two writers load `/` while the root-own ops are measured, one on the root (its
own rows in `/`) and one on b (forwarded to the root). Each of their writes
waits for the archive (about 200 ms): the root's writer wrote about 30 files
in the measured phase of a run and b's 1 to 2, so the journal of `/` holds
unshipped root rows throughout, but the load is light; the stream's ops are
measured beside it, not under a saturated `/`. Median over the runs of p50 / p90 (us); B/A of
the p50.

| file system | topology / actor / op | A | B | B/A |
|---|---|---|---|---|
| tmpfs | 8 root-own create | 128 / 180 | 118 / 150 | 0.92 |
| tmpfs | 8 root-own rename | 144 / 192 | 118 / 150 | 0.82 |
| tmpfs | 8 root-own unlink | 90 / 134 | 94 / 137 | 1.04 |
| tmpfs | 8 root-own chmod | 70 / 111 | 70 / 112 | 1.00 |
| tmpfs | 8 root-own write | 159 / 295 | 222 / 358 | 1.40 |
| tmpfs | 8 first op under the grant | 252 | 318 | 1.26 |
| tmpfs | 8 root-slash create | 1702 / 1970 | 1690 / 2076 | 0.99 |
| tmpfs | 8 root-slash rename | 203087 / 203696 | 203078 / 203686 | 1.00 |
| tmpfs | 1 holder create | 91 / 119 | 81 / 104 | 0.89 |
| tmpfs | 1 holder write | 146 / 204 | 150 / 206 | 1.03 |
| ext4 | 8 root-own create | 101 / 131 | 96 / 114 | 0.95 |
| ext4 | 8 root-own rename | 116 / 151 | 114 / 161 | 0.98 |
| ext4 | 8 root-own unlink | 100 / 142 | 102 / 149 | 1.02 |
| ext4 | 8 root-own chmod | 77 / 108 | 60 / 95 | 0.78 |
| ext4 | 8 root-own write | 3296 / 4328 | 3312 / 4342 | 1.00 |
| ext4 | 8 first op under the grant | 189 | 260 | 1.38 |
| ext4 | 8 root-slash create | 1760 / 2692 | 1754 / 2542 | 1.00 |
| ext4 | 1 holder create | 92 / 120 | 78 / 104 | 0.85 |
| ext4 | 1 holder write | 3090 / 4061 | 3124 / 4038 | 1.01 |
| xfs | 8 root-own create | 119 / 140 | 112 / 140 | 0.94 |
| xfs | 8 root-own rename | 150 / 190 | 150 / 205 | 1.00 |
| xfs | 8 root-own unlink | 99 / 147 | 112 / 154 | 1.13 |
| xfs | 8 root-own chmod | 90 / 127 | 91 / 130 | 1.01 |
| xfs | 8 root-own write | 2028 / 2181 | 2042 / 2190 | 1.01 |
| xfs | 8 first op under the grant | 300 | 243 | 0.81 |
| xfs | 8 root-slash create | 1706 / 2134 | 1728 / 1924 | 1.01 |
| xfs | 1 holder create | 88 / 113 | 72 / 87 | 0.82 |
| xfs | 1 holder write | 1946 / 2058 | 1968 / 2068 | 1.01 |
| btrfs | 8 root-own create | 108 / 136 | 124 / 148 | 1.15 |
| btrfs | 8 root-own rename | 155 / 204 | 136 / 182 | 0.88 |
| btrfs | 8 root-own unlink | 103 / 144 | 124 / 162 | 1.20 |
| btrfs | 8 root-own chmod | 90 / 138 | 90 / 130 | 1.00 |
| btrfs | 8 root-own write | 3212 / 3858 | 3198 / 3866 | 1.00 |
| btrfs | 8 first op under the grant | 299 | 251 | 0.84 |
| btrfs | 8 root-slash create | 1716 / 2002 | 1720 / 1950 | 1.00 |
| btrfs | 1 holder create | 82 / 112 | 90 / 113 | 1.10 |
| btrfs | 1 holder write | 3083 / 3819 | 3071 / 3803 | 1.00 |

Reading: the root's own ops stay at local speed (60 to 220 us, and the `write`
of the file system's fsync, 2 to 3.3 ms) with `/` loaded, in A and in B alike;
the root-slash ops are the archive wait, unchanged (203 ms for a `rename`, 1.7
ms for a `create` and a `write` that the archive batches). After the first op
under the grant (240 to 320 us: the one archive wait of the record, already
there at the first honour) no op of the stream waits for any root row of `/`:
the rule's second half looks at the rows on the stream's keys only. Topology 1
(a lone holder, no stream) is neutral: the p50 ratios of 0.81 to 1.23 are
tens of microseconds on two rounds, both ways on the same cell across file
systems (the largest, `rename` 1.22/1.23 on xfs and btrfs, is 83 to 101 us and
72 to 88 us). The `first_under_grant` ratios (1.26, 1.37, 0.81, 0.84) are one
sample per run, and the other way round on two file systems; the one 1.40
(tmpfs `root-own` `write`, 159 to 222 us) is the tmpfs's own spread, 1.01 to
1.01 on the three other file systems, where the cell is the fsync.

## Plan 46 M1.3 step 8, the sticky handover (ADR-56): topologies 3 and 7, and the fast path's admission

A = `origin/main` before the step (`77b3a43a`), B = the step's tip; ABBA (A B, B A),
two rounds, 100 samples per op and run, one box that other jobs share at times (the
1-minute load before each run: 0.04 to 2.4), the harness's own daemons, each file system
as the `TMPDIR` (`ext4` and `xfs` on loop images, `btrfs` the box's root, `tmpfs`).
Topology 7 (`7-wan-sticky`) runs under `net-wan`. Median over the runs of p50 / p90 (us);
B/A of the p50. The ops on the delegate's fast path (topology 3 `delegate`, topology 7
`sticky`) are the ones the handover's per-subtree activity record could touch; they are
neutral within the noise of two runs on every file system (0.78 to 1.21 for 100 us ops on
the sticky holder in topology 7, both ways; the tmpfs and ext4 rows above 1.1 are within
that noise, and the same ops with the handover off in both are neutral, below). In B the
forwarding writer's ops of topology 7 (`forward-to-sticky`, not tabulated) take 50 to 200
us where A's take the 120 ms round trip: consistent with the writer dominating the
holder's subtree for the length of the run and being handed it, the handover being on at
its defaults here (an A row of 58 ms for `sticky create` on ext4 is a slow run of A).

| file system | topology / actor / op | A | B | B/A |
|---|---|---|---|---|
| tmpfs | 3 delegate chmod | 662 / 841 | 661 / 862 | 1.00 |
| tmpfs | 3 delegate create | 695 / 823 | 685 / 839 | 0.99 |
| tmpfs | 3 delegate rename | 695 / 835 | 693 / 818 | 1.00 |
| tmpfs | 3 delegate unlink | 665 / 828 | 676 / 849 | 1.02 |
| tmpfs | 3 delegate write | 760 / 917 | 730 / 880 | 0.96 |
| tmpfs | 7 sticky chmod | 73 / 113 | 87 / 131 | 1.19 |
| tmpfs | 7 sticky create | 104 / 135 | 122 / 142 | 1.17 |
| tmpfs | 7 sticky first_under_grant | 298 / 298 | 234 / 234 | 0.79 |
| tmpfs | 7 sticky rename | 155 / 206 | 160 / 201 | 1.03 |
| tmpfs | 7 sticky unlink | 114 / 150 | 119 / 150 | 1.04 |
| tmpfs | 7 sticky write | 182 / 321 | 215 / 335 | 1.18 |
| ext4 | 3 delegate chmod | 664 / 838 | 673 / 854 | 1.01 |
| ext4 | 3 delegate create | 727 / 837 | 699 / 853 | 0.96 |
| ext4 | 3 delegate rename | 715 / 860 | 704 / 849 | 0.98 |
| ext4 | 3 delegate unlink | 676 / 806 | 698 / 837 | 1.03 |
| ext4 | 3 delegate write | 7112 / 12257 | 7164 / 12313 | 1.01 |
| ext4 | 7 sticky chmod | 76 / 104 | 92 / 125 | 1.21 |
| ext4 | 7 sticky create | 58600 / 68483 | 122 / 158 | 0.00 |
| ext4 | 7 sticky first_under_grant | 67618 / 67618 | 275 / 275 | 0.00 |
| ext4 | 7 sticky rename | 134 / 167 | 143 / 188 | 1.07 |
| ext4 | 7 sticky unlink | 92 / 123 | 116 / 150 | 1.26 |
| ext4 | 7 sticky write | 6504 / 9548 | 6550 / 12235 | 1.01 |
| xfs | 3 delegate chmod | 690 / 861 | 681 / 859 | 0.99 |
| xfs | 3 delegate create | 693 / 824 | 707 / 849 | 1.02 |
| xfs | 3 delegate rename | 709 / 859 | 729 / 856 | 1.03 |
| xfs | 3 delegate unlink | 675 / 815 | 689 / 833 | 1.02 |
| xfs | 3 delegate write | 6761 / 8690 | 6842 / 8701 | 1.01 |
| xfs | 7 sticky chmod | 87 / 129 | 90 / 129 | 1.03 |
| xfs | 7 sticky create | 116 / 148 | 111 / 154 | 0.96 |
| xfs | 7 sticky first_under_grant | 343 / 343 | 228 / 228 | 0.66 |
| xfs | 7 sticky rename | 128 / 184 | 105 / 142 | 0.82 |
| xfs | 7 sticky unlink | 127 / 163 | 127 / 172 | 1.00 |
| xfs | 7 sticky write | 6226 / 8766 | 6149 / 8658 | 0.99 |
| btrfs | 3 delegate chmod | 646 / 814 | 644 / 828 | 1.00 |
| btrfs | 3 delegate create | 726 / 844 | 688 / 809 | 0.95 |
| btrfs | 3 delegate rename | 695 / 844 | 713 / 866 | 1.03 |
| btrfs | 3 delegate unlink | 689 / 869 | 690 / 837 | 1.00 |
| btrfs | 3 delegate write | 3690 / 3884 | 3708 / 3924 | 1.00 |
| btrfs | 7 sticky chmod | 92 / 139 | 92 / 118 | 1.00 |
| btrfs | 7 sticky create | 110 / 154 | 103 / 126 | 0.94 |
| btrfs | 7 sticky first_under_grant | 242 / 242 | 214 / 214 | 0.88 |
| btrfs | 7 sticky rename | 160 / 208 | 158 / 215 | 0.99 |
| btrfs | 7 sticky unlink | 131 / 166 | 122 / 166 | 0.93 |
| btrfs | 7 sticky write | 3189 / 3801 | 3130 / 3721 | 0.98 |

With the handover off in both (`CONSTELLATION_STICKY_HANDOVER_TENURE_MS=100000000`, four
rounds, topology 7): tmpfs, the holder's ops 0.78 to 1.05 (p50), the forwarded ones 0.99 to
1.03; ext4, the holder's ops 0.83 to 1.12 (create 1.06, rename 1.12, unlink 0.83, write
0.99; 82 to 163 us), the forwarded ones 0.99 to 1.03: both directions, so the noise of
100 us ops on this box, not a cost.

The delegate fast path's admission itself (`DelegateView::admit_for`, an ignored
microbenchmark, `cargo test --release -p constellation-engine --lib admit_for_threads --
--ignored --nocapture`), A = the same test on the parent, three rounds ABBA, ns per
admission per thread (median): 1 thread A 337, B 343 (1.018); 16 threads A 14377, B 14663
(1.020, the entries lock is the whole cost, as in A); 64 threads A 64338, B 65960
(1.025). The cost B adds is a lookup in the per-subtree record and two atomics under the
lock the admission holds anyway (the earlier third global mutex of the first version
is gone): about 7 ns on an admission that precedes a 600 us op.
