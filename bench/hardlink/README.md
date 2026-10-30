# Hard-link benchmark (plan 45, section 7 "Box")

What a name added to or removed from a hard-linked file costs through a real
mount, and what a listing costs, on four backing file systems, before and
after plan 45 (a dentry holds the attribute copy only while its inode has one
name; [ADR-36](../../docs/explanation/DECISIONS.md)). The results and the
verdict are in [RESULTS.md](RESULTS.md); the full per-file-system tables with
the spread of every run are in [tables.md](tables.md). The plan is
[done/45-hard-links.md](../../docs/plans/v1/done/45-hard-links.md); the
meta-level counterpart (no FUSE, one store) is
`crates/meta/tests/hardlink_perf.rs`.

## What runs

`cell.py` runs one *cell*: one constellation binary on one backing file
system. Every mount is a plain `/dev/fuse` session
(`CONSTELLATION_FUSE_TRANSPORT=dev-fuse`) over a `file://` backend, with the
state directory and the backend under a directory of the file system being
tested (ext4, xfs, btrfs, or tmpfs). Every phase gets a fresh file system
(`fs create`), so one phase's tree never sits under another's
measurement. Phases, in this order:

| phase | what it does | records |
|---|---|---|
| `link` | one `stress-ng --link 1 --link-ops 1` bogo-op (8,192 links, then 8,193 unlinks, of one file) | its wall time; `meta.db` bytes and `.jnl` files while still mounted; then `kill -9` and the time the next mount takes (it replays the journals), and a clean unmount and mount of the recovered state |
| `burst` | 8,192 links of one 4 KiB file over two directories, then the unlinks, while a second process creates files in a third directory in a loop (a pause of 200 us between creates) | the creates' p50/p99/max before the burst (idle), during the links and during the unlinks; the links' and unlinks' own p50 at 2,000 names; `meta.db` after. The build under test runs three link+unlink rounds (more samples for a p99), the baseline one (a baseline round takes minutes) |
| `control` | the `burst` phase with single-name creates and unlinks in place of the links and unlinks, three rounds | the same numbers: what any write burst costs a concurrent `create` on this file system. The reference the `burst` tail is read against |
| `list` | a directory of 1,000 files of `nlink` 1: `ls -la`, `find -ls`, `du -s`, cold (a fresh mount for every repetition: the kernel's and the daemon's caches empty, the OS page cache warm) and warm (the same mount, one untimed pass, then back to back); 9 repetitions | wall time and the FUSE requests of each run (`status`' `vfs_ops` delta) |
| `trees` | (i) a pnpm-like tree: 10,000 files with 10 names each, spread over 1,000 package directories of 100 names (a file's ten names are in ten different directories); (ii) an rsnapshot-like tree: 50,000 files in 500 directories and six `cp -al` generations of it (7 names per file). `ls -laR` of the pnpm tree and of the last generation, cold (3 repetitions) and warm (5) | wall time and FUSE requests |

The creator of `burst` and `control` is a separate process, so the burst's
syscalls and the creates' do not share a Python interpreter. Its samples carry
their start time on the monotonic clock, and the burst's windows are cut from
the same clock.

## Builds

| name | what |
|---|---|
| `h0` | the commit before plan 45's code (`1bc76d4c`, the H0 baseline: the meta-level measurement only) |
| `head` | the build under test: `main` when plan 45 closed (`ac897d7b`) |
| `headnp` | `head` with `CONSTELLATION_FUSE_READDIRPLUS=off`, for the `list` and `trees` phases only |

`h0` predates plan 40 (FUSE `READDIRPLUS`): the baseline lists with
`readdir` and a `lookup` per entry, `head` with plus entries. A listing cell
of `head` against `h0` is therefore a plan 45 and plan 40 difference; `headnp`
asks the kernel for plain `readdir`, as `h0` does, and is the like-for-like
for what plan 45 changed. Both are in the tables.

## Method

- **A B B A.** `abba.sh` takes a job list (`STORE VERSION RUN` per line) and
  runs the cells one at a time, resumable (a finished cell is listed in
  `OUTDIR/done`). Job lists interleave the builds (`h0 head head h0`) so the
  host's drift cancels; the tables give the median over the runs of each build
  and the spread of the run values (the noise a ratio is read against).
- **A quiet host.** The cells run on a dedicated timing host (16 vCPUs, one EBS
  volume with a partition per file system, tmpfs in `/dev/shm`) that is shared
  in turns: a cell starts only when no other job holds it (a line in a lock
  file) and the 1-minute load average is below 4 (`LOAD_MAX`), and each phase
  waits for the same load again (`PHASE_WAIT`, then it records `gate_ok=false`).
  Every cell runs under `timeout` (`CELL_TIMEOUT`, 45 minutes by default).
- **Why a `kill -9` remount.** The journals a burst leaves are replayed by the
  next mount; the baseline's are hundreds of megabytes, so that is the remount
  time the plan's gate is about.
- **Limits.** One binary pair, one host, one volume type. The tmpfs cells
  measure the daemon and FUSE without a disk. No cell here has S3 latency (the
  backend is a directory); the S3 log is not what plan 45 changed.

## Rerunning

```sh
# Needs: Linux with FUSE, stress-ng, python3, fusermount3, and the two binaries:
#   $D/bin/cst-h0    the baseline build (cargo build --release -p constellation at 1bc76d4)
#   $D/bin/cst-head  the build under test
D=$HOME/hl-bench
printf 'ext4 h0 1\next4 head 1\next4 head 2\next4 h0 2\n' > jobs.txt
D=$D PHASES=link,burst,control bench/hardlink/abba.sh jobs.txt out-ext4-lbc   # needs /mnt/fs-ext4
bench/hardlink/summarize.py out-ext4-lbc > tables.md
```

`abba.sh` maps a store to a directory (`/mnt/fs-ext4`, `/mnt/fs-xfs`,
`/mnt/fs-btrfs`, `/dev/shm`; edit the `case` for other mounts). One cell
directly:

```sh
python3 bench/hardlink/cell.py BIN /mnt/fs-ext4 ext4-head-r1 out.jsonl --phases link,burst
```

Knobs, all environment variables of `cell.py`: `REPS` (9), `BIG_COLD` (3),
`BIG_WARM` (5), `LOAD_MAX` (4), `PHASE_WAIT` (900 s), `BURST_N` (8192),
`BURST_ROUNDS` (1; `abba.sh` sets it to `BURST_ROUNDS_HEAD`, 3, for every
build but `h0`), `CONTROL_ROUNDS` (3), `PACE_US` (200), `MOUNT_WAIT` (900 s: the
baseline's remount after a burst takes minutes). `summarize.py --compact`
prints the tables without the spreads.
