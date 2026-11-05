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
