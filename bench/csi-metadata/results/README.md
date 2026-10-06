# Plan 37 K0 Track B — `CreateVolume` metadata-op throughput ladder

Raw results for `docs/plans/v1/done/37-kubernetes-csi.md`'s "K0 results" →
"Track B" section. Driver: `crates/harness/src/csi_meta_ladder.rs`, run via
`target/release/harness csi-meta-ladder`.

## What this measures

`Controller.CreateVolume` for a pool-layout volume (§5 of the plan) runs,
through the control socket, against one real `constellation` daemon + one
Constellation filesystem backed by the harness's docker floci + toxiproxy
S3:

1. `browse.mkdir{/volumes/<name>}`
2. `browse.xattr{set}` × 6 (`user.pv`, `user.pvc`, `user.namespace`,
   `user.capacity`, `user.source`, `user.created` — `user.*` because
   `XattrPolicy::check_name` (`crates/vfs/src/policy.rs`) refuses any other
   namespace for a non-root caller; §5's shorthand `{pv,pvc,...}` is read as
   the attribute's local name, not its wire namespace)
3. `quota.set{max_bytes}` — **not** §5's `quota.set{subtree, bytes}`, which
   does not exist: the control protocol's `SetQuotaParams` is `{max_bytes}`
   only, a filesystem-wide cap. This measures the RPC that exists today, and
   since the whole failure mode found here *is* that RPC's whole-filesystem
   barrier, a future subtree-scoped quota may not reproduce it. The plan's
   Track B section carries this caveat next to the guidance.

(`fs.create` happens once per pool, via `Client::fs_create`, matching every
other harness bench's filesystem setup — not re-run per volume.)

Two grids, same concurrency ladder (1, 4, 8, 16, 32, 64, 256 — all committed
in `CONCURRENCY_LADDER`, so a plain run reproduces every published row):

- **`createvolume`**: the sequence above, at rising concurrency and rising
  *cumulative* subtree count on one unsharded pool (each concurrency level
  grows its own fresh pool from 0 subtrees through every checkpoint in
  order, so later checkpoints reflect a larger existing pool, not just more
  load).
- **`ping`**: `node.ping` (no metadata touched at all) at the same total
  op count, over the same single shared connection and the same
  task-per-slot shape, to rule out both the control socket — framing,
  dispatch, `spawn_blocking` handoff (`unary()` in
  `crates/engine/src/control/mod.rs`) — and the client itself.

Recorded per step, not just latency: successful-sequence and **failed**-
sequence percentiles separately (a sequence that dies on its trailing
`quota.set` still contributes its seven good samples, and the failing op is
itself timed), failure counts per op kind, S3 request counts and breakdown
(`S3Env::counting_proxy()`), daemon CPU (`/proc/<pid>/stat`), the replica's
`spool.journal_backlog`/`ship_rounds_completed` and `lease.held`/`lease.lost`
(`node.status`), and `/proc/loadavg`'s 1-minute figure at both ends of the
step. The observability fields are `null` rather than `0` when the underlying
read fails, so a genuine zero is distinguishable from a missing measurement.

## Files

| File | What |
|---|---|
| `run2-grid-full.jsonl` / `.log` | **Primary run.** The whole committed ladder (1,4,8,16,32,64,256) × checkpoints (100,1000,5000,10000) in one command, with the final driver. The plan's main Track B table is this file. Host 1-minute load fell from 143 to 37 over the run (recorded per step). |
| `run3-repro-4-8-16-64.jsonl` / `.log` | A second run with the final driver at concurrency 4/8/16/64, host load ~29-35, for the cross-run host-dependence table. |
| `grid-1-4-16-64-256.jsonl` / `.log` | First-pass run (concurrency 1,4,16,64,256), **earlier schema** — no per-step load average, no failure latencies, no lease fields, and `daemon_cpu_pct`/backlog fields are `0` rather than `null` on a failed read. Kept as a third data point for the cross-run table. |
| `knee-8-32.jsonl` / `.log` | Concurrency 8 and 32 from the first pass, when those levels were not yet in the committed ladder. Same earlier schema. |
| `reproducibility-16-64.jsonl` / `.log` | First-pass rerun at concurrency 16 and 64. Same earlier schema. |

Each `.jsonl` line is one `CreateVolumeStepReport`, `PingStepReport` or
`ConcurrencySummary` (distinguish them by their keys — `volumes`, `pings`,
`attempted`; schemas: the doc comments in `csi_meta_ladder.rs`); the `.log`
is the matching human-readable `stderr`, ending in the summary table.

The `seq/s` figures the plan publishes are `ConcurrencySummary`'s, i.e.
`sum(volumes) / sum(wall_s)` over a level's checkpoints — computed by
`csi_meta_ladder::summarize`, not by hand.

## Re-running

```
unset CARGO_TARGET_DIR
cargo build --release -p constellation-harness -p constellation
target/release/harness csi-meta-ladder --json > out.jsonl 2> out.log
```

Needs docker (floci + toxiproxy containers) and no root. Knobs:

- `CONSTELLATION_HARNESS_DOCKER_PREFIX=<name>` — run alongside another
  harness/docker-using process without fighting over containers.
- `CONSTELLATION_CSI_LADDER_CONCURRENCY=16,64` — only this comma-list of
  concurrency levels (diagnostic knob for reruns; the committed ladder
  already contains every published level).
- `CONSTELLATION_CSI_LADDER_QUICK=1` — tiny ladder (concurrency 1,4;
  checkpoints 5,20) to smoke-test the driver itself in seconds.
- `CHAOS_KEEP_TMP=1` — keep the per-run state dirs/mount logs after exit.

A full run is seven concurrency levels × 10,000 sequences plus the
`node.ping` comparison, each level on its own freshly created and mounted
filesystem (14 daemon mount/unmount cycles in total). On this 32-core host at
load 30-140, run 2 spent **128.6 s inside the measured steps** (65 s of that
is the deliberately single-threaded `concurrency=1` leg) and roughly three
minutes wall clock once docker startup and the 14 daemon start/mount/unmount
cycles are counted; run 3's four levels took 61 s wall for 47 s of
measurement with docker already up. **Results are host-load dependent** — compare
reruns using the `loadavg_1min_*` fields, not across hosts.

## Headline finding (see the plan doc for the full tables and discussion)

Two findings on top of each other, both in the c=16…64 region:

1. **A super-linear p99 knee** in the sequences that succeed: p99 at the
   10,000-subtree checkpoint goes 22.3 ms at c=16 → 89.0 ms at c=32 (4.0×
   for a 2× concurrency step) → 171.1 ms at c=64 → 635.9 ms at c=256.
   Hundreds of milliseconds, not tens. These percentiles are
   survivorship-biased at the high levels (see below), which is why the
   failed sequences are now timed separately.
2. **A failure-rate cliff**: the fraction of `CreateVolume` sequences that
   fail outright with the control error `journal not shipped: no lease`
   (run 2: 0% at c=1, 0.20% at c=8, 9.6% at c=32, 71.7% at c=64, 94.5% at
   c=256). The magnitude is host-dependent — across the runs here c=8 ranges
   0.16% to 12.6% — so **no safe concurrency above 1 is published**.

Root cause, traced to the code rather than inferred: `quota.set` — called
once per `CreateVolume` — runs `EngineControl::set_quota` →
`self.snapshot_barrier("/")` (`crates/engine/src/control/service.rs:1082`),
which registers a `Control::Barrier` round-waiter
(`crates/authority/src/core/mod.rs:1864-1869`) and fails it with exactly this
message if the *whole node's* journal backlog isn't back to zero by the end
of that sync round (`crates/authority/src/core/jobs.rs:1963`, `:1999-2005`)
— not because any lease was lost: this is a single, uncontested node, and
every step records `lease_held: true` / `lease_lost: false`.
`browse.mkdir`/`browse.xattr` never hit this path (read from
`crates/engine/src/control/browse.rs`: no barrier call), and the artifacts
agree — `failures_by_op` is `{"quota": n}` on every failing step, zero `mkdir` and
zero `xattr` failures in the two runs' 110,000 sequences. The more concurrent
`mkdir`/`xattr` traffic is in flight, the less often a `quota.set`'s barrier
round catches the backlog at zero, and there is no automatic retry. A failure
is *cheap* (failing `quota.set`: 20.4 ms p50 / 47.3 ms p99 at c=64), which is
the measurement behind the guidance that K2 should retry the step.

`node.ping` on the same grid and the same client shape runs at 56.6k-348k
ops/s with p99 from 0.08 ms (c=1) to 2.6 ms (c=256) and zero errors —
two orders of magnitude above the ~1.2-1.4k sequences/s the `CreateVolume`
grid reaches, ruling out the control socket, dispatch path and client.
