# Chaos (`constellation-chaos`)

Multi-node filesystem consistency stress tool. Shared ops, generators,
and checkers drive both a fast local CI cluster and a multi-node TCP
soak.

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [CLI](#cli)
  - [Profiles](#profiles)
  - [Operations (Tier A)](#operations-tier-a)
  - [Checkers](#checkers)
  - [Failure pack](#failure-pack)
- [Troubleshooting](#troubleshooting)
- [References](#references)

## Terminology

- **Coordinator**: process that schedules ops, records history, and runs checkers (`chaos run` or `harness` `chaos-ci`).
- **Worker**: process that executes POSIX ops against one mount (`chaos worker`, or an in-process mount in `LocalCluster`).
- **Profile**: `Ci` (short fixed rounds) or `Soak` (duration-based).
- **History**: JSONL invoke/ok/fail/info events (Jepsen-style).
- **Barrier**: synchronization point before a storm and before quiesce reads.
- **LocalCluster**: N mount paths on one host; no TCP.
- **TcpCluster**: coordinator connects to remote `chaos worker` listeners.

## Details

### CLI

```bash
chaos worker --listen 0.0.0.0:7400 --mount /PATH
chaos run --workers host:7400,… --profile soak --duration 4h --seed 42 --store ./chaos-store
chaos run --mounts /mnt/a,/mnt/b --profile ci --seed 42 --store ./out
chaos check --history ./chaos-store/<run>/history.jsonl
```

Harness entry (preferred for CI):

```bash
cargo run -p constellation-harness --release -- run chaos-ci --seed 42
```

### Profiles

| Profile | Duration | Typical topology | Workload |
|---|---|---|---|
| `ci` | fixed rounds (~minutes) | 2–3 local mounts via harness | Tier A subset, small files |
| `soak` | `--duration` | N remote workers | Full Tier A mix, optional multi-chunk writes |

### Operations (Tier A)

| Op | Conflict intent |
|---|---|
| Create / Mkdir / Unlink / Rmdir / Rename | Exactly-one-winner storms |
| WriteFull | Register duel (unique payloads) |
| WriteAt (overlap) | No torn mix of patches |
| WriteAt (disjoint) | All patches persist |
| Append / Truncate / Chmod | Concurrent metadata/data races |
| Read / ReadAt / Stat | Quiesce / close-to-open observation |

### Checkers

- Exactly-one-winner for namespace storms
- Register linearizability for WriteFull/Read
- Torn-write ban on overlapping WriteAt
- Disjoint-write persistence
- Chmod attr atomicity
- Cross-worker convergence after quiesce barriers (coordinator polls up to
  `quiesce_timeout_secs` for close-to-open propagation before checking).
  Observations are grouped per verify op — `path` for `Read`/`Stat`,
  `path@offset+len` for `ReadAt` — so the several spans of a
  `write_disjoint` round are compared span-by-span, not against each other
- Unexpected-errno allowlist (`EEXIST`, `ENOENT`, `EISDIR`, `ENOTDIR`, `ENOTEMPTY`, `ESTALE`, …)

### Failure pack

Under `--store/<run_id>/`:

| File | Contents |
|---|---|
| `config.json` | run id, profile, seed |
| `history.jsonl` | full event stream |
| `failure.md` | checker, message, op ids |
| `success` | present on pass |

## Troubleshooting

### Unexpected errno / EIO

Mount may be unhealthy or lease-deposed. Inspect constellation logs on
the failing worker; history records `worker_id` and `errno_name`.

### Hung barrier / no progress

Confirm every `--workers` address accepts TCP and that `chaos worker`
is still running. LocalCluster barriers are instantaneous; hangs usually
mean a blocked FUSE op on a mount.

### Mount path not visible on other workers

Close-to-open: visibility is checked only after the writer closes and
the quiesce barrier. Pre-barrier staleness is allowed. If post-barrier
reads still diverge, the history's `quiesce_*` window is the
counterexample. Verify reads drop the page cache first, so a divergence
describes daemon state, not a client cache.

### Lasting divergence after a quiesce barrier

Every worker keeping its *own* value past the timeout is a real finding,
not lag: it means a record was dropped rather than delayed, since a
replica that merely lagged would still hold *some* peer's value.
Reproduce with the same `--seed` and a filtered debug log on each node,
then read them together with `history.jsonl`:

```bash
RUST_LOG=constellation=debug constellation mount … 2>&1 | tee /tmp/node.log
```

Keep the `2>&1`: tracing writes to stderr, so piping stdout alone leaves
an empty log file. The conflict line below is a `WARN` and shows up even
without `RUST_LOG`.

| Log line | Reading |
|---|---|
| `conflict: pending local op wins over foreign record` | a peer's record was skipped because this node had unshipped journal rows. Skips are permanent, so one per diverged path explains the whole failure. Legitimate only for records this node has *not* yet shipped — see [forwarded mutations](../features/forwarded-mutations.md#the-shadow-does-not-win-conflicts) |
| `FENCING VIOLATION: segment from a superseded lease epoch` | a deposed holder's late segment was dropped |
| `shipped log segment` | the seq/epoch actually published, to line up against peers |
| `continuation epoch active` | S3 was unreachable; per-epoch visibility limits apply |

Group the verify observations by `path@offset+len`: a single path with
several `ReadAt` spans (`write_disjoint`) is expected to show one value
per span.

### Offline re-check

```bash
chaos check --history path/to/history.jsonl
```

Does not need mounts or workers.

## References

- [Testing](../../how-to-guides/development/TESTING.md) — `chaos-ci` in the harness lane
- [Run a chaos soak](../../how-to-guides/development/run-chaos-soak.md)
- [DESIGN.md §6](../../explanation/DESIGN.md) — close-to-open consistency
