# Plan 47 — Chunk-location digests: research, measure, then decide

> **Status (2026-11-07): proposed.** Phase P0 (research) is next. Phase P3
> (design and implementation) runs only if P0 and P1 together show that a
> change is worth making, and only after the maintainer accepts the
> decision record (P2).

Read `docs/plans/v1/CONVENTIONS.md` first.

## 1. Why this plan

The cooperative cache needs to know which peer can serve a chunk, so that a
read can fetch it from a peer instead of S3. Today
(`crates/engine/src/coop/exact.rs`, `crates/net/src/reconcile.rs`, plan 30
§M15) each node keeps an **exact mirror** of every peer's servable set:

- keys are the first 8 bytes of the chunk hash (`key_of`);
- the owner pushes deltas (adds and removes) every 250 ms and a summary
  (fingerprint, count, seq) every digest interval;
- a gap, a reorder, a restart or a summary whose fingerprint differs starts
  a range-based set reconciliation (RBSR, aligned 4-bit prefix ranges,
  additive fingerprints) that transfers only the differing ranges;
- a lookup is a binary search in the mirror; a peer whose set exceeds
  `MAX_MIRROR_KEYS` (8 Mi keys, 64 MiB) is not mirrored;
- the older Bloom digest (`CONSTELLATION_COOP_DIGEST=bloom`, plans 06/14)
  remains as an option. It was demoted because a false positive costs a
  wasted peer round trip and a Bloom filter cannot record removals.

The question from the maintainer (2026-11-07) is whether a different
structure (for example a cuckoo filter) would serve better. Nobody has
measured the current design's costs at fleet scale, and the trade-offs
(memory per peer, wire bytes, repair cost, false positives, CPU per tick)
pull in different directions. This plan answers the question with
evidence: **no number in the decision may be estimated where it can be
measured.**

## 2. Scope

In scope: the representation and exchange of *which peer holds which
chunk* (publishing, mirroring, repair, lookup) and its counters. Out of
scope: chunk transfer itself, the cache's eviction policy, the P2P
transport, the authority stream.

Invariants any candidate must keep (or the plan records exactly what it
gives up and the maintainer accepts it):

- **No data risk.** Every fetched chunk stays hash-verified; a wrong
  location answer may cost a declined fetch and an S3 fallback, never
  wrong data.
- **Off the I/O path.** No digest work on a read or write path beyond a
  local lookup; repair runs on background tasks.
- **Bounded memory per peer** and a defined behaviour above the bound.
- **Frames within the P2P cap** (`MAX_FRAME`, 64 KiB, signed postcard).
- **Removals take effect** within a bounded time (the reason Bloom was
  demoted).

## 3. Phase P0 — research (one agent, read-only, ≤ 2 days)

Survey the candidate designs and write `bench/coop-digest/RESEARCH.md`:
for each, how it would publish, mirror, repair and look up, its expected
asymptotic costs, what it gives up, and the measurable predictions P1 must
test. Candidates, at least:

1. **Status quo**: exact mirror + pushed deltas + RBSR (baseline).
2. **Bloom** (the existing option) and **counting / scalable Bloom**
   (deletes at a size cost).
3. **Cuckoo filter** (deletes, low false-positive rate for its size;
   insertion can fail at high load; deleting an absent item corrupts it;
   cannot enumerate, so it cannot drive RBSR).
4. **Quotient / counting quotient filter** (deletes, resizable, mergeable).
5. **Static filters** (xor, binary fuse, ribbon): smallest per key, but
   rebuilt on change — viable only as periodic snapshots.
6. **Compact exact storage** of the same protocol: the wire and repair
   unchanged, the mirror stored as delta-coded / Elias–Fano blocks
   instead of a flat `Vec<u64>`.
7. **Set-reconciliation alternatives** for the repair step: invertible
   Bloom lookup tables (IBLT), Minisketch-style sketches, Negentropy
   (the reasons it was rejected are in `reconcile.rs`; re-check them).
8. **Hybrids**: a filter as the in-memory lookup index over an exact
   on-disk or compressed mirror; exact for small peers, filter above a
   size threshold.

Deliverable also includes: which existing Rust crates implement each
(licence, maintenance, `unsafe`, fit with the 64 KiB frame and the 8-byte
keys), and the benchmark plan for P1 with the predictions to test. Sources
cited; claims about other systems quoted with references, not paraphrased
from memory.

## 4. Phase P1 — benchmarks (≤ 4 agent-days)

Build `bench/coop-digest/` (a microbenchmark crate plus harness runs) and
measure **every** P0 candidate that survived P0's review, against the
status quo, on the same inputs. Metrics:

| Metric | How |
|---|---|
| Receiver memory per peer (bytes per key, RSS delta) | microbench, keys 10 k … 8 Mi |
| Publisher memory and CPU per 250 ms tick | microbench, churn 0.1 % … 10 % per minute |
| Wire bytes, steady state (deltas + summaries) per peer-minute | microbench and harness counters |
| Repair cost after a gap: bytes, round trips, wall time | microbench: gaps of 1, 1 k, 100 k keys; restart; initial sync |
| Lookup latency p50/p99 | microbench, hit and miss |
| False positives and false negatives per lookup; wasted peer round trips | microbench oracle against the true set; harness `peer_false_positives` |
| Staleness: time from a node's change to every mirror reflecting it | harness, with injected loss/reordering |
| Insert failures / rebuilds (filters) | microbench at 50–95 % load |

Workloads: 2, 8, 32 and 64 peers; per-peer sets from 10 k to 8 Mi keys;
uniform and bursty churn (a cache rebuild); message loss and reordering;
peer restarts. Harness runs on the devboxes with real nodes; WAN latency,
if needed, by the harness's existing delay injection (no host network
configuration changes outside the test boxes). Timing runs on the quiet
box, A/B/B/A, enough runs to report medians with spread; results in
`bench/coop-digest/RESULTS.md` with the commands to reproduce them.

Every job runs under a timeout. Benchmarks must not change product
defaults; candidate implementations live in the bench crate (or behind a
test-only feature) until P3.

## 5. Phase P2 — decision record (maintainer)

A short decision record (`bench/coop-digest/DECISION.md`, and an ADR if the
answer is a change) comparing the candidates on the P1 numbers only. The
decision rule, fixed now so the numbers decide:

- **Change only if** a candidate is at least **2× better** on a metric that
  P1 shows to be a real cost at the fleet sizes we target (receiver memory,
  steady wire bytes, or repair bytes/time), **and** it does not regress the
  others beyond noise, **and** its false-positive rate costs fewer wasted
  peer round trips per minute than the status quo's measured staleness
  declines, **and** it keeps every invariant of §2.
- Otherwise the decision is "keep the status quo" (possibly with a
  smaller, measured improvement such as compact mirror storage), and the
  plan closes after P2 with the benchmarks kept as a regression suite.

## 6. Phase P3 — design and implementation (only on a "change" decision)

Written after P2, as a step table in the style of plan 46's breakdowns:
the design (wire messages, mirror structure, repair, lookup, limits),
migration (the maintainer's rule: no compatibility, no version bump;
mixed versions fall back to S3, as Bloom and exact do today), status and
metrics, and removal of whatever the change replaces. Each step has a
fail-before test where it fixes behaviour, the harness scenarios for
removals and repair, and the P1 benchmark re-run as its acceptance.

## 7. Exit criteria

- [ ] P0: `RESEARCH.md` reviewed; candidate list and predictions agreed.
- [ ] P1: `RESULTS.md` with reproducible measurements for every candidate
      kept after P0; no estimated figure in the comparison.
- [ ] P2: decision recorded and accepted by the maintainer (ADR on a change).
- [ ] P3 (if a change): implemented; the P1 suite shows the predicted gain;
      no regression on ext4, xfs, btrfs or tmpfs hosts; harness coop
      scenarios green; every CONVENTIONS gate green; pjdfstest 8798/8798;
      docs (DESIGN, feature reference, configuration) updated.

## 8. Dependencies and scheduling

Independent of plan 46's authority work (different crates: `engine::coop`,
`net::reconcile`, `net::bloom`). P0 can start at once; P1 shares the
devboxes with plan 46 under the box ledger and uses the quiet box only in
turns.
