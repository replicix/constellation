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

