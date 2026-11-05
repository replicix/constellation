# Lock renewal lane: results (plan 41 L8)

`bench/lock-lane/run.sh`: 3 runs per file system and lane setting, alternated
(lane off, then on), release build. `lock-latency` medians of p50 and p90 over runs,
microseconds; the acceptance is no regression (at most 3 % or one IQR of the runs).

## btrfs, lane off

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 37 | 58 |
| B (non-sequencer) first LOCK_EX on a file | 313 | 416 |
| B cached fcntl F_SETLK+F_UNLCK | 23 | 32 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 44 | 53 |
| contended handoff (A unlock -> B granted) | 1494 | 1699 |

## btrfs, lane on

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 44 | 54 |
| B (non-sequencer) first LOCK_EX on a file | 277 | 366 |
| B cached fcntl F_SETLK+F_UNLCK | 23 | 28 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 42 | 53 |
| contended handoff (A unlock -> B granted) | 1424 | 1459 |

## ext4, lane off

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 43 | 52 |
| B (non-sequencer) first LOCK_EX on a file | 292 | 406 |
| B cached fcntl F_SETLK+F_UNLCK | 24 | 30 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 42 | 48 |
| contended handoff (A unlock -> B granted) | 1443 | 1487 |

## ext4, lane on

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 43 | 57 |
| B (non-sequencer) first LOCK_EX on a file | 289 | 380 |
| B cached fcntl F_SETLK+F_UNLCK | 23 | 28 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 | 52 |
| contended handoff (A unlock -> B granted) | 1436 | 1529 |

## tmpfs, lane off

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 44 | 58 |
| B (non-sequencer) first LOCK_EX on a file | 299 | 415 |
| B cached fcntl F_SETLK+F_UNLCK | 23 | 28 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 42 | 53 |
| contended handoff (A unlock -> B granted) | 1395 | 1429 |

## tmpfs, lane on

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 43 | 58 |
| B (non-sequencer) first LOCK_EX on a file | 303 | 394 |
| B cached fcntl F_SETLK+F_UNLCK | 23 | 26 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 | 54 |
| contended handoff (A unlock -> B granted) | 1430 | 1522 |

## xfs, lane off

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 48 | 58 |
| B (non-sequencer) first LOCK_EX on a file | 279 | 317 |
| B cached fcntl F_SETLK+F_UNLCK | 32 | 35 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 31 | 40 |
| contended handoff (A unlock -> B granted) | 1427 | 1484 |

## xfs, lane on

| measurement | p50 (us) | p90 (us) |
|---|---:|---:|
| A (the sequencer) LOCK_EX | 32 | 45 |
| B (non-sequencer) first LOCK_EX on a file | 300 | 387 |
| B cached fcntl F_SETLK+F_UNLCK | 27 | 46 |
| B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 | 53 |
| contended handoff (A unlock -> B granted) | 1441 | 1536 |

## Stall tolerance (`lock-stall-lone-node`, stalls 5000 ms)

| file system | lane | runs passed | runs failed |
|---|---|---:|---:|
| btrfs | off | 3 | 0 |
| btrfs | on | 3 | 0 |
| ext4 | off | 3 | 0 |
| ext4 | on | 3 | 0 |
| tmpfs | off | 3 | 0 |
| tmpfs | on | 3 | 0 |
| xfs | off | 3 | 0 |
| xfs | on | 3 | 0 |

## The branch against main (lock-latency, lane on, 3 runs alternated per file system)

Devbox with ext4, xfs, btrfs and tmpfs; `main` is the commit the branch is
based on. Medians of the runs. The requests that go through the authority core
(the first lock from a non-sequencer, the contended handoff) are within
+-5 % on every file system; the sequencer's own lock and the cached
operations are node-local and do not touch the core. The cached re-lock is
bimodal run to run on both sides (24 to 26 us or 42 to 52 us in the same
binary, three runs each, see `cached re-lock` in the lane on/off tables above
too), so its median is the draw, not the branch: it is the only line that
differs by more than 12 %.

| file system | measurement | main p50 / p90 (us) | after p50 / p90 (us) | p50 delta |
|---|---|---:|---:|---:|
| btrfs | A (the sequencer) LOCK_EX | 41 / 58 | 42 / 48 | +2 % |
| btrfs | B (non-sequencer) first LOCK_EX on a file | 296 / 370 | 284 / 398 | -4 % |
| btrfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 26 | 30 / 35 | +31 % |
| btrfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 / 48 | 45 / 56 | +5 % |
| btrfs | contended handoff (A unlock -> B granted) | 1452 / 1554 | 1420 / 1470 | -2 % |
| ext4 | A (the sequencer) LOCK_EX | 43 / 48 | 42 / 48 | -1 % |
| ext4 | B (non-sequencer) first LOCK_EX on a file | 306 / 398 | 315 / 399 | +3 % |
| ext4 | B cached fcntl F_SETLK+F_UNLCK | 35 / 47 | 31 / 35 | -12 % |
| ext4 | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 44 / 53 | 43 / 50 | -1 % |
| ext4 | contended handoff (A unlock -> B granted) | 1419 / 1458 | 1473 / 1539 | +4 % |
| tmpfs | A (the sequencer) LOCK_EX | 42 / 50 | 38 / 45 | -9 % |
| tmpfs | B (non-sequencer) first LOCK_EX on a file | 262 / 352 | 270 / 352 | +3 % |
| tmpfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 26 | 23 / 26 | -1 % |
| tmpfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 25 / 31 | 35 / 42 | +42 % |
| tmpfs | contended handoff (A unlock -> B granted) | 1448 / 1535 | 1400 / 1506 | -3 % |
| xfs | A (the sequencer) LOCK_EX | 42 / 50 | 34 / 41 | -19 % |
| xfs | B (non-sequencer) first LOCK_EX on a file | 289 / 357 | 290 / 398 | +0 % |
| xfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 27 | 24 / 38 | +4 % |
| xfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 25 / 30 | 45 / 57 | +77 % |
| xfs | contended handoff (A unlock -> B granted) | 1429 / 1485 | 1439 / 1605 | +1 % |


Stall tolerance is in the table above: `lock-stall-lone-node` (5 s) passes
3 of 3 on every file system with the lane on, and passes with the lane off
too, as the lapse is read from the parking there (not an `EIO`).


## Plan 41 section 9 acceptance (2026-11, quiet timing box)

Release build of this branch (plus the harness additions below), a Linux
7.3 test host with a local NVMe volume (ext4, xfs, btrfs partitions) and
tmpfs, `floci` as the S3 backend. Every cell is one `harness run` (a fresh
file system, mount, workload, remount and fsck); the lane-off arm runs the
same binary with `CONSTELLATION_LOCK_LANE=off`. Each run prints one
`P41RESULT` line (scenarios `lock-stall-lone-node`, `lock-device-stall-lone-node`,
`lock-store-stall-past-lease`); a "lapse" is a run in which an operation
parked for a lapsed grant (`lapse_parks > 0`, which is how plan 43's lapse
parking shows a lapse: the I/O waits and then answers OK), and an "EIO run"
one with any `EIO`, fenced op, fenced owner or lost lock. Lane-on arms are
the acceptance; lane-off arms are the recorded baseline.

### Item 1: lone node, own grants (`lock-stall-lone-node`, 5 runs per cell)

The core's own metadata commit holds the store's writer lock for D seconds
(3 s lock TTL, default lease). Lapse runs / EIO runs out of 5; the
parked time is the longest an operation waited on a lapsed grant.

| fs | D (s) | lane off: lapse runs | off: EIO runs | off: longest park (ms) | lane on: lapse runs | on: EIO runs | on: own renewals per run |
|---|---:|---:|---:|---:|---:|---:|---:|
| ext4 | 5 | 5 / 5 | 0 / 5 | 3008-3014 | 0 / 5 | 0 / 5 | 7-8 |
| ext4 | 10 | 5 / 5 | 0 / 5 | 8003-8006 | 0 / 5 | 0 / 5 | 12-13 |
| ext4 | 15 | 5 / 5 | 0 / 5 | 12997-13017 | 0 / 5 | 0 / 5 | 17-18 |
| ext4 | 20 | 5 / 5 | 0 / 5 | 17993-17998 | 0 / 5 | 0 / 5 | 22-24 |
| ext4 | 25 | 5 / 5 | 0 / 5 | 22984-22990 | 0 / 5 | 0 / 5 | 27-28 |
| ext4 | 30 | 5 / 5 | 0 / 5 | 27981-27988 | 0 / 5 | 0 / 5 | 32-34 |
| ext4 | 40 | 5 / 5 | 0 / 5 | 37972-37976 | 0 / 5 | 0 / 5 | 42-43 |
| xfs | 5 | 5 / 5 | 0 / 5 | 3011-3012 | 0 / 5 | 0 / 5 | 7-9 |
| xfs | 10 | 5 / 5 | 0 / 5 | 8002-8009 | 0 / 5 | 0 / 5 | 12-12 |
| xfs | 15 | 5 / 5 | 0 / 5 | 12996-13004 | 0 / 5 | 0 / 5 | 17-19 |
| xfs | 20 | 5 / 5 | 0 / 5 | 17993-18000 | 0 / 5 | 0 / 5 | 21-24 |
| xfs | 25 | 5 / 5 | 0 / 5 | 22987-22992 | 0 / 5 | 0 / 5 | 27-29 |
| xfs | 30 | 5 / 5 | 0 / 5 | 27981-27989 | 0 / 5 | 0 / 5 | 32-34 |
| xfs | 40 | 5 / 5 | 0 / 5 | 37973-37977 | 0 / 5 | 0 / 5 | 41-44 |
| btrfs | 5 | 5 / 5 | 0 / 5 | 3008-3009 | 0 / 5 | 0 / 5 | 7-7 |
| btrfs | 10 | 5 / 5 | 0 / 5 | 8003-8007 | 0 / 5 | 0 / 5 | 12-12 |
| btrfs | 15 | 5 / 5 | 0 / 5 | 12998-13001 | 0 / 5 | 0 / 5 | 17-17 |
| btrfs | 20 | 5 / 5 | 0 / 5 | 17994-17998 | 0 / 5 | 0 / 5 | 21-22 |
| btrfs | 25 | 5 / 5 | 0 / 5 | 22989-22995 | 0 / 5 | 0 / 5 | 27-27 |
| btrfs | 30 | 5 / 5 | 1 / 5 | 27982-27987 | 0 / 5 | 0 / 5 | 32-32 |
| btrfs | 40 | 5 / 5 | 0 / 5 | 37973-37980 | 0 / 5 | 0 / 5 | 42-42 |
| tmpfs | 5 | 5 / 5 | 0 / 5 | 3007-3010 | 0 / 5 | 0 / 5 | 8-9 |
| tmpfs | 10 | 5 / 5 | 1 / 5 | 8002-8007 | 0 / 5 | 0 / 5 | 13-14 |
| tmpfs | 15 | 5 / 5 | 0 / 5 | 12998-13003 | 0 / 5 | 0 / 5 | 18-19 |
| tmpfs | 20 | 5 / 5 | 0 / 5 | 17991-17999 | 0 / 5 | 0 / 5 | 23-23 |
| tmpfs | 25 | 5 / 5 | 0 / 5 | 22989-22993 | 0 / 5 | 0 / 5 | 28-29 |
| tmpfs | 30 | 5 / 5 | 0 / 5 | 27982-27990 | 0 / 5 | 0 / 5 | 33-33 |
| tmpfs | 40 | 5 / 5 | 0 / 5 | 37974-37976 | 0 / 5 | 0 / 5 | 42-43 |

Lane on: zero lapses, zero `EIO`, zero fenced owners, zero lost locks in all
140 runs, D = 5 to 40 s on all four file systems; every run passed the
scenario (tree, locked file, fsck, also after a remount). Lane off: every
run lapsed at every D (the parked time is D minus about 2 s: the grant
ended 2 s into the stall), so the baseline lapses from 5 s on, earlier than
the plan's 9.5 s estimate. Two lane-off runs (btrfs D = 30, tmpfs D = 10, one
each) also answered 1 `EIO` and 2 fenced ops.

The default lease outlasts D = 40 s here, so this grid never reaches the
lease cap. The cap is `lock-store-stall-past-lease` (lease 12 s, stall
20 s), lane on, 5 runs on each file system: all 20 runs passed (the lane
renewed 10 to 11 times, the grant lapsed at the lease's usable end and
operations parked 11.1 to 11.2 s of the 20 s, never to the end of the stall).
The "lapse at the cap +- 1 s" bound is shown by that parking time, not
measured against `cap_until` (the scenario does not print the status
timestamp).

### Item 2: holder at a healthy owner (`lock-store-stall-holder`, 3 runs per cell)

Two nodes, B holds the lock granted by A, B's store stalled D s while A's
client waits on the same lock. `CONSTELLATION_HARNESS_HOLDER_STALL_MS` sets
D (new knob). Per cell: B's write after the stall, locks lost, owners fenced.

| D (s) | lane off (all 4 file systems, 12 runs) | lane on (all 4 file systems, 12 runs) |
|---:|---|---|
| 10 | B's write `EIO`, lost 1, fenced 1; A granted about 5 s after it asked | write OK, lost 0, fenced 0, A not granted; `remote_ok` 10-11 |
| 30 | `EIO`, lost 1, fenced 1; A granted at about 5 s | OK, lost 0, fenced 0; `remote_ok` 30-31 |
| 60 | `EIO`, lost 1, fenced 1; A granted at about 5 s | OK, lost 0, fenced 0; `remote_ok` 60-61 |

Zero lapses for D up to 60 s with the lane on. Max-stall bound
(`CONSTELLATION_LOCK_LANE_MAX_STALL_MS=30000`, stall 40 s, lane on, 3 runs per
file system): in all 12 runs the lane stopped renewing at the bound
(`remote_ok` 29 to 31), B's grant lapsed (write `EIO`, lost 1) and A was
granted 34.1 s (tmpfs, btrfs, xfs) to 35.0 s (ext4) after it asked: bounded
(the overlap check is not part of this scenario).

### Item 4: not run

Fast tenure with a backup: no `lock-store-stall-backup` scenario exists on
this branch and L6 is deferred (plan 46 stage 3, D-8), so there is nothing
to measure; the lane adds no renewal for `ack=backup` (L0 rule, unit tests).

### Real-device reproduction (`lock-device-stall-lone-node`, 10 runs per file system)

State dir (the scenario's TMPDIR) on an ext4, xfs or btrfs over `dm-delay`
over a loop file on the box's local disk; the dm table is `delay <dev> 0 0
<dev> 0 0 <dev> 0 1000`: every flush takes 1 s (an `fsync` took about 1.2 s;
delaying writes too made an `fsync` 5 s and the daemon's S3 bootstrap
minutes). The device is fast for mount and bootstrap, swapped to the delayed
table while the workload runs, back to fast for the remount (the scenario's
`CONSTELLATION_HARNESS_DEVICE_HOOK`). 8 threads `fsync` on the mount, the
lone-node workload runs, then the device is frozen with `dmsetup suspend` for
15 s and resumed. Every run also passed tree, file and fsck checks.

| fs | lane | runs | lapse runs | EIO / fenced runs | own renewals (sum) |
|---|---|---:|---:|---:|---:|
| ext4 | off | 10 | 5 | 0 | 0 |
| ext4 | on | 10 | 0 | 0 | 160 |
| xfs | off | 10 | 6 | 0 | 0 |
| xfs | on | 10 | 0 | 0 | 113 |
| btrfs | off | 10 | 3 | 0 | 0 |
| btrfs | on | 10 | 0 | 0 | 119 |

Lane on: zero lapses and zero owner-fenced `EIO` in 30 of 30 runs. Lane off:
lapses (parked I/O, no `EIO`: plan 43's parking) in 14 of 30 runs. A device
freeze sticks the core only when a metadata write lands in it, so the lane
had nothing to renew in some lane-on runs (own renewals 0); those are not
lapses.


## Plan 41 section 9 items 6 and 7 and the xcheckfs cells (2026-11-05)

Release build of this branch (main 64409f78 plus the harness scenarios
`lock-lane-overhead` and `lock-xcheckfs-cell`), a 16-core Linux 7.3 test host
that no other job held during the runs, `floci` as
the S3 backend, two nodes (A: the sequencer, B: a holder) unless stated. The
lane-off arm is the same binary with `CONSTELLATION_LOCK_LANE=off`. Summary:
at 100 held grants or fewer the lane adds no S3 request and about one
`LockRenew` entry per grant per minute; **the 0.5 % of a core bound with 10,000
held grants is not met (13.8 to 22.4 %), and the lane thread is not armed only
when needed (about 957 wake-ups per minute with no grant held)**. The xcheckfs
cells lost no lock and fenced no owner.

### Item 6: renewal traffic and S3 requests (`lock-lane-overhead`, 60 s windows)

B holds 1, then 100 in total, exclusive `flock`s granted by A,
default lock TTL (20 s). "core" is `locks.renewals` (the authority core's
answered renewals), "lane" the lane's `remote_sent` (entries, not messages:
with one grant an entry is a message). S3 requests are the node's own counters.

| held at B | lane | core renewals / min | lane entries / min | per grant per min | S3 GET / PUT / LIST on B |
|---:|---|---:|---:|---:|---|
| 1 | off | 9 | 0 | 9 | 7 / 0 / 5 |
| 1 | on | 9 | 0 | 9 | 7 / 0 / 5 |
| 100 | off | 900 | 0 | 9.0 | 7 / 0 / 4 |
| 100 | on | 900 | 100 | 10.0 | 7 / 0 / 4 |

With the lane on, 100 grants cost one extra renewal entry per grant in the
minute (+11 %, a hair over the plan's +-10 %); with 1 grant none. No S3
request is added: B's GET, PUT and LIST counts are the same lane on and off
(the node's background S3 traffic is 7 to 25 GETs per minute in every phase,
held grants or not). A grant's renewal rate does not depend on
`CONSTELLATION_LOCK_TTL_MS`: with a 10-minute TTL the core still renewed about
every 6.7 s (9 per minute), because the lease's usable window caps the grant.
So "10,000 grants held, none due" cannot be set up: they are always due.
The messages per grant could not be counted (`remote_sent` counts entries;
the lane's request counter is not in status); the 1-grant row is exact.

### Item 7: idle cost and the lane's CPU

The lane's thread (`cst-lock-lane`) CPU from `/proc/<pid>/task/<tid>/schedstat`
and its context switches from `status`, over 60 s; a node with no lock held:

| state | lane thread CPU | lane thread context switches / min |
|---|---:|---:|
| lane off | no thread | 0 |
| lane on, no grant held anywhere | 0.010 % of a core | 957 |
| lane on, 1 / 100 grants held at B | 0.010 / 0.013 % | 962 / 963 |

CPU is negligible, but **the lane ticks every 250 ms (50 ms at the shortest)
whether or not a grant is held and does not sleep until `next_renewal_ms`**
(`crates/engine/src/lock_lane.rs`, `lane_loop`: it wakes, finds
`held_count() == 0` and sleeps again), about 16 wake-ups a second, not the
zero the plan asks for. Whole-process wake-ups at idle are about 12,000 per
minute, so it is under 8 % of the process's own. Not met as written.

With 10,000 held grants (the files created and locked by 32 threads, 60 s
window after a 10 s settle; one run per cell, each cell is a fresh pair of
nodes):

| held | where | lane | lane thread CPU | process CPU | renewals / min (core + lane) | lapse parks | locks lost |
|---|---|---|---:|---:|---:|---:|---:|
| 10,000 at B | remote | off | none | 64 % | 25 + 0 | not recorded | 0 |
| 10,000 at B | remote | on | **16.3 %** | 76 % | 28,292 + 24,312 | not recorded | 0 |
| 10,000 at B, 10 min TTL | remote | off | none | 64 % | 161 + 0 | 2,600 | 0 (9,900 held at the end) |
| 10,000 at B, 10 min TTL | remote | on | **13.8 %** | 65 % | 31,634 + 143,180 (20,581 answered) | 3,217 | 0 |
| 10,000 at A | own | off | none | 59 % | 84,608 + 0 | 0 | 0 |
| 10,000 at A | own | on | **22.4 %** | 75 % | 72,684 + 31,836 | **20,000** | 0 |

The 0.5 % bound is missed by 28 to 45 times. The lane is not the only thing
that suffers at this scale: with the lane off, 10,000 remote grants are barely
renewed by the core (25 to 161 renewals in a minute) and 2,600 operations
park on lapsed grants. With 10,000 held at both A and B (the first runs, both
arms) B lost 2,498 (lane off) or 3,040 (lane on) locks and fenced 9,133 or
10,000 owners, so 20,000 grants in total are beyond what the core renews. What the lane
changes: at 10,000 own grants it adds 20,000 lapse parks (none with it off),
and at 10,000 remote grants it sends 143,180 renewal entries a minute for
20,581 answered ones (a request carries every due entry and most are
unanswered or late while the owner's core is busy); B's core steps for the
lane's results (`LaneRenewed`, 10,000-entry batches) took 0.63 s on average
(107 steps, up to 1.05 s, `slow core step` warnings; lane-on run with the long TTL). Those two are findings
for the plan, not tuned away.

Artifacts of the measurement: node A's "process CPU" of the idle phase shows an
impossible value in the first row of the raw lines (a thread that exited
during the window); the lane thread's own figures are not affected. At the end
of a 10,000-grant run closing the descriptors never finished (every `close`
waits for its lock's release, 4 `lock-wait` threads at 15 % CPU each for more
than 10 minutes, no progress), so the scenario kills the daemons instead.

### xcheckfs cells (`lock-xcheckfs-cell`)

The sibling project's stress lane (`tests/external/run-stress.sh`: 9 fio jobs
with verification and one stress-ng stressor after another, among them `lockf`, `fcntl`,
`flock`, `locka` and `lockofd`, then a mixed run; `--check thorough`,
`STRESS_TIMEOUT=10`, 2 workers) runs with node A's mount as xcheckfs's
secondary and a tree on a local ext4 (or, in the first runs, the host's btrfs)
as the primary. The daemons run write-back, `--locks cluster`, the lane on
(the acceptance) or off (the baseline), release build. The pass criterion is
the plan's: no lock-lapse finding, here read as `locks.lost`,
`owners_fenced`, `owner_fenced_ops` and `fenced_io` all 0 on every node at
the end (sampled every 30 s during the run) and no `EIO` mismatch.

| cell | lane | length | xcheckfs operations | lost / owners fenced / fenced io | lapse parks | xcheckfs mismatches |
|---|---|---:|---:|---|---|---|
| finding 08: 1 node, write-back, host overloaded (32 CPU hogs, 8 `fsync` writers, 4 `sync-file`, load average up to 52 on 16 cores), state on ext4 | on | 11.5 min | 1,085,502 | 0 / 0 / 0 | 0 | 0 (`OK`) |
| same, the primary on btrfs | on | 10 min | 1,040,536 | 0 / 0 / 0 | 0 | 2 (see below) |
| 2 nodes, both state dirs on a loop-backed `dm-delay` device (1 s flush) after the mounts, B holds 20 `flock`s and re-locks one every 0.5 s | on | 23.5 min | 5,545,535 | 0 / 0 / 0 on A and B | B 20 | 3 (see below) |
| 2 nodes as above plus a 12 s `dmsetup suspend` every 150 s (9 freezes) | on | 23.5 min | 5,525,588 | 0 / 0 / 0 on A and B | B 79 | 3 (see below) |
| 2 nodes with the freezes | off | 24 min | 5,292,765 | 0 / 0 / 0 on A and B | B 97 | 1 (see below) |

No lock lapse finding: with the lane on no node lost a lock, fenced an owner
or answered a fenced `EIO` in any cell, and no xcheckfs mismatch is an `EIO`.
The finding-08 cell did not reproduce finding 08's stall on this host
(the core's longest step was 61 to 77 ms, `lane.max_stuck_ms`), so it passes
for want of a stall; the freeze cell stalled B's core for 12 s (lane
`max_stuck_ms` 12,075, 274 stuck ticks) and is the one that exercised the
lane. It does not tell the arms apart: both nodes' state dirs share the
frozen device, so the owner A is frozen as well (the owner's stall is plan 41's
L5, not in this scope) and B's grants lapse and park its re-lock thread
either way (79 parks lane on, 97 lane off, no `EIO`, no lock lost; B's lane sent 2,199 renewal
entries and had 262 answered). The first two-node cell (delay only, no freeze)
never stuck the core (`max_stuck_ms` 3 to 19 ms): a 1 s flush delay slows
the daemons' `fsync` threads but not the core.

Mismatches, all unrelated to locks:

- `getattr` `mtime` on the mount root, the first operation (two-node cells,
  all arms): the secondary root's mtime is 2 s after the primary's, set when
  the write-back flush of the first create lands (2.0 to 2.2 s later in every
  two-node run). Not seen in the one-node cells.
- `setattr verify` of `stress-ng utime` at the maximum 64-bit time (cells
  with the primary on btrfs, 2 or 3 per run): the time range of the two
  file systems; the ext4-primary run has none. xcheckfs's probe of the time
  range (its commit `dd18db3`) is the area.
- After the unmount, `verify` found one `mtime` difference on an `mmap` fio file
  (lane-off freeze cell): a write-back `mmap` timestamp.
- The `fio` `fsync` job (an `fsync` every 16 writes) timed out at 300 s
  in every cell with the delayed device (the mount still answered):
  latency only, no error.

## Plan 41 item 7: the lane's fixes, before and after (2026-11-05)

Same host, scenario and method as the section above (`lock-lane-overhead`,
60 s windows after a 10 s settle, one run per cell, a fresh pair of nodes per
run, release builds). "Before" is the section above (the base commit);
"after" is this branch. New: `CONSTELLATION_HARNESS_OVH_CLOSE=1` closes the
held descriptors at the end (16 threads, bounded at 300 s) and counts the
`close` errors; the close counters are totals since the start.

Causes found (details in PROGRESS, "Plan 41 item 7: the lane at 10,000 grants"):
a single renewal recomputed the park and fence hints over every held grant
and local lock (`LockTables::track`, O(n) per renewal, so a round was
O(n^2)); a `LockRenew` of every due grant did not fit a 64 KiB P2P frame
past about a thousand entries, failed, timed out (a lease read each) and the
next round was larger; the core's renewal tick fired at every grant's own
renewal point (about once a millisecond); the lane ticked every 250 ms
whatever was held, looked up in-flight entries by scanning every request,
re-sent whole rounds to a busy owner; and the core's per-step refresh waited
for the lane's mutex through a round.

| measurement | before | after |
|---|---:|---:|
| lane wake-ups per minute, no grant held | 957 | 0 |
| lane thread CPU, idle | 0.010 % | 0.000 % |
| lane wake-ups per minute, 1 / 100 / 10,000 held at B | 962 / 963 / n.a. | 69 / 87 / 105 |
| lane thread CPU, 10,000 own grants (A) | 22.4 % | 0.136 % |
| lane thread CPU, 10,000 remote grants (B) | 16.3 % | 0.093 % |
| lane thread CPU, 10,000 at A and 10,000 at B | n.a. | 0.088 % (A) / 0.110 % (B) |
| process CPU, 10,000 remote grants (B) | 76 % | 0.82 % |
| renewals per grant per minute, 100 held (lane on / off) | 10.0 / 9.0 | 9.0 / 9.0 |
| renewals per grant per minute, 10,000 remote (lane on / off) | 5.3 / 0.0 | 9.0 / 9.0 |
| renewals per grant per minute, 10,000 own (lane on / off) | 10.5 / 8.5 | 9.0 / 9.0 |
| lane renewal entries sent / answered, 10,000 remote, 10 min TTL | 143,180 / 20,581 | 86,634 / 86,634 |
| lapse parks in the window, 10,000 own (lane on) | 20,000 | 0 |
| lapse parks in the window, 10,000 remote, 10 min TTL (lane off) | 2,600 | 0 |
| locks lost / owners fenced, 10,000 at A and at B (lane on) | 3,040 / 10,000 | 0 / 0 |
| locks lost / owners fenced, 10,000 at A and at B (lane off) | 2,498 / 9,133 | 0 / 0 |
| S3 GETs per minute on B, 10,000 remote (lane on / off) | n.a. | 7 / 7 |

"Renewals per grant per minute" is the node's `locks.renewals` (the core's and
the lane's together, one per extension) over held grants; 9 is the rate the
lease's usable window sets at the default TTL. With the lane on most of them are
now the lane's (it wakes at the renewal point; the core's tick is coalesced with
64 or more grants held), with no extra message: a renewal is sent by whichever
takes the grant's `renewing` mark first. The 0.5 % bound of item 7 is met with
10,000 grants (own, remote, both); a round's work is O(due grants) plus one walk
of the held grants per wake-up (at most four a second).

Closing the descriptors at the end:

| cell | lane | close time | `close` errors | owners fenced | locks lost |
|---|---|---:|---:|---:|---:|
| 9,900 remote at B, 10 min TTL | off, base commit | > 10 min (killed) | n.a. | n.a. | n.a. |
| same | off / on, first fixes only (no chunking, no tick gap) | > 300 s / > 300 s | n.a. | 0 / 0 | 0 / 0 |
| same | off, after | 60 s | 0 | 0 | 0 |
| same | on, after | 60 s | 0 | 0 | 0 |
| 10,000 own at A | off / on, first fixes only | 129 s / 129 s | n.a. | 9,428 / 9,216 | 473 / 0 |
| same | off, after | 129 s | 9,427 | 9,445 | 1,466 |
| same | on, after | 111 s | 7,285 | 7,285 | 0 |
| 10,000 at A and at B, all 20,000 | off, after | 203 s | 3,350 | 3,350 (A) | 0 |
| same | on, after | 211 s | 1,408 | 1,408 (A) | 0 |

("First fixes only": this branch's first round, the `track` and lane fixes
without the frame-sized rounds and the tick gap; the base commit's run closed
nothing, the scenario killed the daemons.)

The remote case no longer hangs and closes cleanly. **Closing 10,000 locked
files of the sequencer's own grants still fences most of their owners (an `EIO`
at `close`), lane on or off**: not fixed here. In a diagnostic run (another
host, logs kept) the core's renewal steps took 1 to 58 s while the closes ran:
the late renewals of the grants that lapsed are each admitted at O(n) cost on
the owner table (`LockTables::admit_late`: a scan of every record for a
conflicting one, a `retain` over all of them, lookups in the lapsed memory), one
`info` line each (98,336 lines), and the renewal tick walks every held grant
several times with one table lock per grant (`local_idle`). With the step
refresh no longer waiting for the lane the same diagnostic closed 10,000 own
grants in 59 s with no error, but the measurement host above did not: the
cascade starts whenever a few seconds pass without a renewal. Spec in PROGRESS.

### No regression on normal workloads (`lock-latency`, base commit against this branch)

Three runs alternated per file system (base, then branch), lane on, medians of
p50 / p90 in microseconds. The branch run here is this branch before its last
two changes (the core no longer waits on the lane's mutex in its refresh; a
resumed lapse counts down instead of a walk), neither of which is on these paths;
tmpfs and ext4 were run again on the final branch (below).

| file system | measurement | base p50 / p90 (us) | branch p50 / p90 (us) | p50 delta |
|---|---|---:|---:|---:|
| btrfs | A (the sequencer) LOCK_EX | 42 / 48 | 44 / 57 | +2 % |
| btrfs | B (non-sequencer) first LOCK_EX on a file | 343 / 430 | 311 / 447 | -10 % |
| btrfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 27 | 23 / 27 | -1 % |
| btrfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 44 / 50 | 44 / 57 | -0 % |
| btrfs | contended handoff (A unlock -> B granted) | 1640 / 1690 | 1611 / 1731 | -2 % |
| btrfs | lone node `cluster` flock LOCK_EX+UN | 43 / 56 | 44 / 54 | +3 % |
| ext4 | A (the sequencer) LOCK_EX | 49 / 56 | 42 / 49 | -16 % |
| ext4 | B (non-sequencer) first LOCK_EX on a file | 318 / 404 | 333 / 488 | +5 % |
| ext4 | B cached fcntl F_SETLK+F_UNLCK | 23 / 26 | 23 / 26 | -0 % |
| ext4 | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 / 50 | 44 / 50 | +2 % |
| ext4 | contended handoff (A unlock -> B granted) | 1539 / 1640 | 1587 / 1653 | +3 % |
| ext4 | lone node `cluster` flock LOCK_EX+UN | 42 / 54 | 45 / 54 | +7 % |
| tmpfs | A (the sequencer) LOCK_EX | 43 / 49 | 46 / 59 | +6 % |
| tmpfs | B (non-sequencer) first LOCK_EX on a file | 309 / 357 | 345 / 451 | +12 % |
| tmpfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 26 | 23 / 25 | -1 % |
| tmpfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 44 / 53 | 45 / 54 | +1 % |
| tmpfs | contended handoff (A unlock -> B granted) | 1559 / 1691 | 1568 / 1631 | +1 % |
| tmpfs | lone node `cluster` flock LOCK_EX+UN | 44 / 56 | 42 / 48 | -3 % |
| xfs | A (the sequencer) LOCK_EX | 43 / 52 | 44 / 59 | +3 % |
| xfs | B (non-sequencer) first LOCK_EX on a file | 354 / 452 | 304 / 401 | -14 % |
| xfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 27 | 23 / 28 | -0 % |
| xfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 / 48 | 46 / 56 | +7 % |
| xfs | contended handoff (A unlock -> B granted) | 1624 / 1713 | 1618 / 1764 | -0 % |
| xfs | lone node `cluster` flock LOCK_EX+UN | 42 / 49 | 42 / 53 | +0 % |

The final branch, tmpfs and ext4 again (three runs alternated):

| file system | measurement | base p50 / p90 (us) | branch p50 / p90 (us) | p50 delta |
|---|---|---:|---:|---:|
| ext4 | A (the sequencer) LOCK_EX | 43 / 56 | 44 / 59 | +2 % |
| ext4 | B (non-sequencer) first LOCK_EX on a file | 306 / 494 | 344 / 451 | +13 % |
| ext4 | B cached fcntl F_SETLK+F_UNLCK | 23 / 27 | 23 / 26 | +0 % |
| ext4 | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 43 / 49 | 43 / 49 | -0 % |
| ext4 | contended handoff (A unlock -> B granted) | 1616 / 1656 | 1626 / 1699 | +1 % |
| ext4 | lone node `cluster` flock LOCK_EX+UN | 42 / 47 | 43 / 52 | +1 % |
| tmpfs | A (the sequencer) LOCK_EX | 50 / 55 | 43 / 49 | -15 % |
| tmpfs | B (non-sequencer) first LOCK_EX on a file | 300 / 367 | 325 / 402 | +8 % |
| tmpfs | B cached fcntl F_SETLK+F_UNLCK | 23 / 28 | 29 / 46 | +28 % |
| tmpfs | B cached re-lock (LOCK_EX after LOCK_UN, same file) | 42 / 49 | 43 / 50 | +2 % |
| tmpfs | contended handoff (A unlock -> B granted) | 1530 / 1667 | 1530 / 1714 | -0 % |
| tmpfs | lone node `cluster` flock LOCK_EX+UN | 43 / 53 | 42 / 51 | -1 % |

Every line is within one run-to-run IQR but one: B's first lock on a file (20
samples a run) moves -14 to +13 % between file systems and rounds; over the 18
runs per side it is 309 us (base) against 330 us (branch), median of the run
medians, with the base's own IQR 47 us (298 to 345 us): within one IQR, but the
pooled median is 7 % higher, which these runs cannot tell from noise (the base's
runs alone spread 248 to 374 us). That request goes through the authority cores
of both nodes and the lane is not on its path (below 64 held grants the core's tick
is unchanged); a longer series of that one measurement is the next check. The
tmpfs `fcntl` line in the second round (+28 %) is one bimodal run (the first round
showed -1 % there).

Stall tolerance on the branch (lane on): `lock-stall-lone-node` (stalls 5, 12 and
20 s) passed 2 of 2 on every file system, and 2 of 2 again on tmpfs and ext4 on the
final branch; `lock-store-stall-holder` passed on every file system and again on
tmpfs and ext4 on the final branch.
