# Plan 40 — READDIRPLUS and name-ordered listings: benchmark

Status: **pending, box.** The scripts below are written and smoke-tested
(one tiny cell on tmpfs, both `list,write` and `big` phases), but the numbers
need the benchmark box's three disk file systems and a quiet host (load < 4),
which another campaign held when this was written. No after-numbers are
recorded yet; nothing below is a measurement.

## What it compares

| Version | Build | `CONSTELLATION_FUSE_READDIRPLUS` |
|---|---|---|
| `base` | the commit before plan 40's listing commits (`73ea9987`: the vfs contract only; the kernel lookup counts of R2 are in it) | none (the knob does not exist) |
| `off`, `auto`, `always` | main with plan 40 | the mode |

State-dir host file systems: ext4, xfs, btrfs and tmpfs (`/dev/shm`); the
kernel is whatever the host runs (the cell records it, and whether
`enable_uring` is set); an LTS kernel without `dabb90391028` is a second host.

## Running it

```
D=~/cst-p40-8   # bin/base, bin/main, tree.tar (python3 gen-tree.py DIR; tar -C DIR/.. -cf tree.tar <basename>)
cat > jobs.txt <<JOBS          # interleave the versions (ABBA)
ext4 base 1
ext4 auto 1
ext4 off 1
ext4 always 1
ext4 auto 2
ext4 base 2
...
JOBS
PHASES=list,write  D=$D bench/readdirplus/abba.sh jobs.txt out/
PHASES=big BIG_SIZES="1000000"  D=$D bench/readdirplus/abba.sh bigjobs.txt out-big/   # 1M; add 10000000 for the plan's 10M
python3 bench/readdirplus/summarize.py out/
```

`abba.sh` is resumable; each cell (`cell.py`) gates every phase on the host's
1-minute load (`LOAD_MAX`, default 4), restarts the daemon for every
repetition of a kernel-cold run, and records wall time, the daemon's CPU,
its peak RSS, the FUSE requests by op and the mount's READDIRPLUS counters.
`summarize.py` prints median (IQR) tables and the verdict of acceptance items
1-4 per cell group, and the section 9 measure-later items (`auto` against
`always`; the handoff size as the largest memfd the daemon held during
`daemon --upgrade` with a pinned directory, a lower bound; the detach-to-serve
gap as that command's wall time; `releasedir` as the `closedir` latency a
client sees, p50/p99, while a cold scan runs).

What the script does not do: the plan's "two nodes (the second idle)" state and the replica-cold state (a fresh state directory, bootstrapped) are not cells yet; the 10M-entry directory takes about an hour to
create (`BIG_SIZES`); `git status` is measured on the 50k-file tree only; a
strict-mode cell is a scenario, not a benchmark (`readdirplus-strict` shows
the name-only behaviour; the cost of strict `auto` against `off` is the
`off`/`auto` difference of a two-node cell, which this script does not run).
