# Directory hash-range split benchmark: results

How the benchmark runs is in [README.md](README.md). The full per-cell tables
(throughput, latency percentiles, root CPU, S3 requests, errors, convergence)
are in [tables.md](tables.md). They were made by `analyze.py` from three
repetitions of every cell: topologies T1 to T5 with 2, 4 and 8 writers, plus
the crossover sweep `X` (T5 with 4 writers, a root RTT of 2 to 60 ms, and S3
at 20 or 80 ms).

## Verdict

**A hash-range split of a directory never pays off on real names.** With
random names (`uniform` creates, `mix` of create and unlink, `rename`), a
hand split into one range per writer is slower than no split in every
topology, at every writer count and at every root RTT measured. It runs at
0.01× to 0.91× the throughput, and usually below 0.2×. The automatic
placement (`b`, `SPLIT=20`) never split a directory with random names. On
those workloads it mostly measured the same as `a` (b/a 0.97 to 1.03). The
exceptions do not come from a split:

- T3 with 2 writers, `uniform`: 1.80×, because the placement delegated the
  directory whole.
- T3 `rename` (0.84×) and T4 `mix` and `rename` (0.45× to 1.34×): `b` runs
  on a cluster of its own, and these cells varied that much from cluster
  to cluster.

**A split pays off only when every writer's names hash into its own range**
(`own`, synthetic). Then it runs at 1.2× to 3.3× within one AZ, and up to
60× when the writers are far from the root. No application chooses names by
their hash, so this is the upper bound, not a use case.

**Delegating the directory whole to one writer** (`d`) is what helps when a
directory's writers are far from the root. T5 runs at 12× to 39× and T4 at
26× to 209× the throughput of no delegation. Inside one AZ the gain is 0.9×
to 3.6×. In T4 the gain is only w0's: see "Bugs found" below.

## c against a (hand split / no split, 4 writers, ops/s)

| topology | uniform | mix | rename | own (synthetic) | d/a uniform |
|---|---|---|---|---|---|
| T1 one AZ, S3 20 ms | 0.06 | 0.11 | 0.17 | 1.43 | 0.94 |
| T2 one AZ, S3 80 ms | 0.02 | 0.03 | 0.04 | 1.42 | 0.97 |
| T3 half the writers 60 ms away | 0.05 | 0.10 | 0.10 | 2.13 | 1.16 |
| T4 every node in its own region | 0.34 | 0.44 | 0.19 | - | 117 * |
| T5 writers together, root 60 ms away | 0.72 | 0.60 | 0.32 | - | 24 |

\* Only w0 made progress (see "Bugs found"). With 2 and 8 writers the
picture is the same: c/a with real names is 0.01 to 0.91.

## Crossover sweep (T5, 4 writers, uniform; ops/s c/a, p50 in ms)

| root RTT | S3 20: c/a | a p50 | c p50 | S3 80: c/a | a p50 | c p50 | own c/a (S3 20 / 80) |
|---|---|---|---|---|---|---|---|
| 2 ms | 0.13 | 19 | 148 | 0.04 | 20 | 509 | 3.3 / 2.7 |
| 5 ms | 0.29 | 43 | 152 | 0.08 | 44 | 586 | 7.5 / 5.9 |
| 10 ms | 0.27 | 42 | 161 | 0.08 | 42 | 516 | 9.4 / 8.6 |
| 20 ms | 0.44 | 82 | 188 | 0.15 | 82 | 533 | 18 / 16 |
| 60 ms | 0.70 | 243 | 355 | 0.38 | 243 | 661 | 52 / 47 |

The gap narrows as the root moves away, but it does not close:

- Without a split, an op's latency grows by about 4× the root RTT. With a
  split it grows by about 4.2×, because most names fall in another
  writer's range and the op is forwarded.
- On top of that, a split op waits for the other ranges' streams through
  the root's S3 log (`deps_waits`). That is a fixed ~145 ms at S3 20 ms and
  ~500 ms at S3 80 ms.

With both slopes near 4×, c/a tends towards about 0.95 and does not reach 1
at any RTT. A slower S3 makes a split worse.

## Why

A writer's create of a random name lands in its own range with probability
1/N. Every other create is forwarded to the writer that owns the range,
which costs a P2P round trip, the same as forwarding to the root. Ops in one
range then wait for the other ranges' streams to be applied, which goes
through the root's S3 log. So a split adds a range-to-range ordering cost and
removes nothing. Delegating the directory whole has no such cross-range
dependency.

## Bugs found by the benchmark (followed up separately)

- **T4 with `d`:** the other writers complete 0 ops in the 15 s window,
  every repetition, with 2, 4 and 8 writers. A few creates return EIO
  (3 to 7 per phase).
- **T3, 4 writers, `rename` under `c`:** one writer thread was stuck in a
  file system call.
- **T4, 4 writers, `rename` under `b`:** a name did not become visible on
  another writer within 60 s.
- **T4, 4 writers, `own` under `b` (300,000 creates in the window):** the
  root listed only 373,000 of 656,000 names after 120 s. A writer listed
  all of them after 205 s, so the root's catch-up is slow, but no name was
  lost.
- **Root slowdown after a split's cross-range renames:** the root stayed
  slow afterwards. Fixed on main: the root forgets ended delegation
  generations.

## Recommendation

- Set automatic range splits off by default (`CONSTELLATION_DELEGATION_SPLIT=0`).
- Keep the hand split only as an explicit, expert setting, or remove plan 30
  M12.
- Rely on whole-directory and subtree delegation for directories whose
  writers are far from the root.

The "failed phases" in tables.md are mostly `d` phases skipped because the
bench delegated to a node that already held a delegation. That is a bench
limitation, not a file system error.
