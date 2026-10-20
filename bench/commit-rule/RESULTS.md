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
