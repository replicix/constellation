# Plan 47 — Chunk-location digests: research, measure, then decide

> **Status (2026-11-12): done.** P0-P2 done, P3 implemented and reviewed. The
> maintainer's P2 decision (2026-11-07): keep the exact mirror, fix the
> range-repair livelock, add delta-log replay as the first repair step with
> range-based repair as the fallback; no storage change
> (`bench/coop-digest/DECISION.md` section 7). P3 delivered both; evidence in
> `bench/coop-digest/RESULTS.md` section 12. The closing harness matrix
> (2026-11-12, release, seed 42, six shards): 302 scenarios, 290 pass, 11 skip
> (they need `CAP_SYS_ADMIN`), and the last failure, `atime-eventual`, fixed
> (the scenario now waits for the reader's `stat` to show the file's size
> before the cold read, since bounded close-to-open allows stale content until
> the log catches up).

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

### 6.1 What P3 did (2026-11-07, maintainer's decision)

| Step | Change | Fail-before / tests | Evidence |
|---|---|---|---|
| P3.1 | `Session` walks depth first (a stack, `crates/net/src/reconcile.rs`): the first `Items` answers are applied within a few rounds, so a session cut by `MAX_ROUNDS` keeps its progress and the next one continues; the livelock of breadth-first walks on 8 Mi-key peers (gaps of 50 k or more, a 100 k burst, a 160 k restart) is gone | `a_large_gap_on_a_large_set_converges_because_sessions_keep_progress` (3 M keys, 60 k changes; fails on the parent: "session 4 used 256 rounds and applied nothing") | RESULTS 12.1: 8 Mi keys, 100 k gap: 26.1 MiB in 3 sessions (P1: failed, 517 MiB); 30 k: 12.1 MiB (was 24.6) |
| P3.2 | Delta-log replay: `DeltaLog` (3 MiB, one repair's reach, and 10 minutes by default; memory counted by actual allocation; `CONSTELLATION_COOP_REPLAY_LOG_BYTES`, 0 = off), `ReplayRequest`/`ReplayReply` (signed postcard, 48 KiB pages, at most 64; a backlog beyond 64 pages is refused at once), `Mirror::exact_seq`/`apply_replay`, the engine's first repair step in `run_session` with the session as fallback; result accepted only if count and root fingerprint equal the owner's summary at the last replayed seq | net: small gap, paging, truncated by size and age, restart, silent change, reordering, differing mirror refused, malformed page, restart clears the exact point, frame fit, QUIC paging with session fallback; engine: log follows publishing, silent tick clears it | RESULTS 12.2: 100 k changes on 8 Mi keys 767 KiB / 22.6 ms against 26.1 MiB / 314 ms; 1 k: 8.2 KiB against 777 KiB |
| P3.3 | Status: `replay_hits`, `replay_misses`, `replay_bytes`, `replay_log_bytes` (control schema re-blessed), `/metrics` `constellation_coop_replay_*`; harness `coop-churn` prints them | schema test | RESULTS 12.5: `replay_hits=2` over real nodes |
| P3.4 | Bench: `product` repair method and `--plog` A/B in `bench/coop-digest`; the generic initiator is depth first too | | RESULTS 12.2-12.4 |

Steady wire is byte-identical and publisher CPU within noise (RESULTS 12.3);
the owner's log is 1.1 / 3.1 / 3.1 MB at 0.1 / 1 / 10 % churn per minute on
8 Mi keys (12.4).

## 7. Exit criteria

- [x] P0: `RESEARCH.md` reviewed; candidate list and predictions agreed
      (the maintainer's P2 decision of 2026-11-07 is made on it).
- [x] P1: `RESULTS.md` with reproducible measurements for every candidate
      kept after P0; no estimated figure in the comparison (the section 7
      CPU "derived wall" columns are computed from measured CPU and rounds
      and say so).
- [x] P2: decision recorded and accepted by the maintainer (2026-11-07: keep
      the exact mirror, fix the livelock, replay first; `DECISION.md` section 7).
      No ADR: no candidate replaced the status quo.
- [ ] P3 (the decision's two repair changes):
  - [x] implemented (depth-first sessions, delta-log replay); the P1 suite
        shows the gain: RESULTS section 12, re-run after the review fixes
        (12.5.1).
  - [x] review fixes (2026-11-07): a backlog beyond one repair's reach (64
        pages) is refused at once and the log default is that reach (3 MiB);
        the new message variants are appended to `Payload`; a session cut at
        the round cap does not mark the mirror fresh; the log counts its
        memory by actual allocation (RESULTS 12.4). Fail-before: the
        backlog and the confirmation tests fail with their fix removed.
  - [x] no regression on ext4, xfs, btrfs or tmpfs hosts: the change is in
        `net::reconcile`, `engine::coop`, the control status and the bench;
        no filesystem, cache-storage or read/write path code is touched, and
        steady wire and publisher CPU are unchanged within noise (12.3).
  - [x] harness coop scenarios green (`coop-cache-hit`, `coop-fallback`,
        `coop-exact-churn`, `coop-digest-compare`): three rounds, 12 of 12;
        replay used by real nodes.
  - [x] gates (box, Linux 7.3, 2026-11-07): `cargo fmt --all -- --check` and
        clippy `--workspace --all-targets -D warnings` clean;
        `cargo test --workspace` release 5,044 passed, 0 failed; debug 5,046
        passed, 1 failed (`handover::tests::a_log_tail_round_trips_through_a_memfd`,
        passes alone 3 of 3: it closes a descriptor and expects `EBADF`, which
        another test thread can reuse the number of; not this plan's code);
        `tests/smoke.sh` and `tests/integration.sh` passed; pjdfstest 8798/8798
        on the ring (`tests/compliance.sh`, baseline empty).
  - [x] the full harness matrix. Closing run (2026-11-12, release, `harness
        run --seed 42`, six shards): 302 default scenarios, 290 passed, 11
        skipped (they need `CAP_SYS_ADMIN`), 1 failed: `atime-eventual`,
        which did not reproduce alone (3 of 3 passed). Cause: the reader read
        0 bytes of an 11-byte file because the kernel's attribute cache still
        held size 0 inside its 1 s TTL; bounded close-to-open allows stale
        content until the log catches up. The scenario now waits for a `stat`
        that shows the size; the product is unchanged. Every scenario of the
        earlier matrix's failures (the EIO group, the cto and metered group,
        `epoch-holder-retired`, `delegate-partition`,
        `designation-as-delegation`, `lease-claimant-ahead`, `node-leave`)
        passes.
  - [x] a load-induced failure of `crates/net/tests/addr_churn.rs` (the link
        did not recover for 150 s after the selected address vanished): fixed
        by 3f6f578c (the dial survives its caller's timeout; the evict hook
        re-dials and re-joins gossip with backoff) and bab474ea (re-dial
        races closed); `addr_churn` x60 under 40-worker CPU load: 59 passed,
        0 stalls, 1 run recovered in 2.34 s against the test's 2 s bound. The
        failure this bullet names was non-recovery; the overrun is a latency
        degradation under extreme load (the bound is tight there), so the
        bullet is met.
  - [x] docs (feature reference, configuration, DESIGN, DECISION, RESULTS,
        PROGRESS) updated.

## 8. Dependencies and scheduling

Independent of plan 46's authority work (different crates: `engine::coop`,
`net::reconcile`, `net::bloom`). P0 can start at once; P1 shares the
devboxes with plan 46 under the box ledger and uses the quiet box only in
turns.
