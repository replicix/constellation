# Plan 47 P2 — decision record

Status: **decided by the maintainer on 2026-11-07 (section 7); outcome of the
implementation in section 7.2.** The text below, up to section 6, is the draft
that applied the plan's §5 rule mechanically to the P1 numbers in
`RESULTS.md`. Every figure below is quoted from a table there (section
numbers in brackets); nothing is new. Where the rule needs a judgement that
the numbers cannot make (what counts as "a real cost", what counts as
"noise"), the judgement is marked **[maintainer]** and the result is given for
both readings.

## 1 The rule (plan §5)

Change only if a candidate is

1. at least **2x better** on a metric that P1 shows to be a real cost at the
   fleet sizes we target (receiver memory, steady wire bytes, or repair
   bytes/time), **and**
2. does not regress the others beyond noise, **and**
3. its false-positive rate costs fewer wasted peer round trips per minute than
   the status quo's measured staleness declines, **and**
4. keeps every invariant of §2.

Otherwise: keep the status quo (possibly with a smaller, measured
improvement), and the plan closes after P2.

## 2 What P1 shows to be a real cost (target: up to 64 peers x 8 Mi keys, maintainer 2026-11-07)

| Metric | Status quo at the target | Measured in |
|---|---|---|
| Receiver memory | 5.30 GiB per node (64 x 8 Mi keys, 11.11 B/key, allocator-live); 725 MiB at 64 x 1 Mi keys; peers above 8 Mi keys are not mirrored at all | RESULTS §2 |
| Steady wire | 715 KB per peer-minute at 1 % churn on 8 Mi keys, i.e. 45 MB/min received per node (derived, 63 peers); 6.4 MB per peer-minute at 10 % churn (405 MB/min, derived). The same bytes for every receiver-side candidate | §6 |
| Repair | **fails to converge** on 8 Mi-key peers for gaps >= 50 k changes, a 100 k burst and a 160 k-difference restart (50 real sessions, 517 MiB, no key applied); 21-25 MiB and 3 sessions at 30 k; initial sync of 8 Mi keys is 56.7 MiB, 1 037 rounds, 5 sessions | §7 |
| Staleness declines | 3.75e-4 per lookup of a recently removed key per peer (1 % loss, 5 % reordering, 1 % churn); 1.5e-4 to 8.9e-4 across loss 0-10 % | §8 |

**[maintainer]** Whether 5.3 GiB per node is "a real cost" depends on the
node's memory budget; the repair failure is a real cost under any reading.
The steady wire is small at 1 % churn (0.75 MB/s received) and larger at 10 %.

## 3 The rule applied

Ratios are status quo over candidate (higher is better) unless marked `x
worse`. "Memory" is live B/key at 64 x 8 Mi keys; "FP" is the measured
rate per lookup per peer at 8 Mi keys (RESULTS §3); "break-even f" is the
fraction of lookups that must target a key removed within the last minute
for the status quo's declines (3.75e-4 per such lookup) to exceed the
candidate's false positives, `f = epsilon / 3.75e-4`; a candidate satisfies
conjunct 3 only if the real fraction is above that (not measured, no
production data).

| Candidate | Memory | (1) >= 2x on memory, wire or repair | FP rate | (3) break-even f | (2) regressions beyond noise (measured) | (4) invariants |
|---|---|---|---|---|---|---|
| C6a EF-coded exact | 5.77 B, **1.93x** | **no** (1.93 < 2) | 0 | n/a | receiver apply 9.4-17.5x worse, lookup 4.8-5.3x worse; repair bytes equal | all kept |
| C6b suffix, w = 32 | 4.39 B, **2.53x** | **yes** (memory) | 2.8e-8 (28 in 1.0e9) | 7.5e-5 | repair bytes 1.5-1.9x worse at 1 k-20 k differences on 8 Mi keys (1.06x at 1 k on 1 Mi keys), 1.5x at a 1 Mi-key restart (5.69 vs 3.81 MiB); receiver apply 1.04-1.62x worse; lookup 2.0-2.6x worse at 64 peers; slack growth +31-76 % under churn; but converges at 100 k+ where C1 does not | I1-I5 kept; needs the leaf-depth session parameter (§1.3) |
| C3 cuckoo, f = 32 | 5.03 B, **2.21x** | **yes** (memory) | 2.0e-9 (2 in 1.0e9) | 5.3e-6 | repair bytes 2.2-3.2x worse for sparse gaps; shard refetch on growth (1 493 for +75 % over 10 min, 8 peers) | I1-I5 kept; needs the leaf-depth parameter and refetch on insert failure |
| C3 cuckoo, f = 24 | 3.85 B, 2.9x | yes | 3.8e-7 | 1.0e-3 | as above | as above |
| C3 cuckoo, f = 16 | 2.66 B, 4.18x | yes | 1.0e-4 | 0.27 | as above | as above |
| C4 quotient, eps = 2^-24 | 5.58 B, **1.99x** | **no** (1.99 < 2) | 2.1e-8 | 5.6e-5 | repair bytes 2.2-3.2x worse; receiver apply 0.84-1.3x; no growth refetches | as C3 |
| C4 quotient, eps = 2^-14 | 3.64 B, 3.05x | yes | 1.9e-5 | 0.05 | as above | as C3 |
| C5 fuse + overlay | 2.70 B, 4.11x | yes (memory) | 1.7e-5 | 0.045 | **steady wire 10.5x (1 Mi) to 14.4x (8 Mi) worse at K = 64**; repair bytes 13-26x worse at 1 k differences; owner builds snapshots | I1-I5 kept |
| C8a filter over on-disk exact | 2.73 B, 4.07x (+ 8 B/key on disk) | yes (memory) | 0 | n/a | receiver apply 47-470x worse (synchronous merge), receiver repair CPU 6-62x worse; a disk probe on every hit | I2 reads as **[maintainer]**: a disk probe counts as a local lookup (answer 2026-11-07) |
| Bloom (existing option) | 0.53 B (saturated above 3.3 M keys) | yes (memory) | 0.82 % at 1 Mi keys, 23 % at 8 Mi keys | n/a | removals wait for the next snapshot (p99 29.7-58 s, max up to 300 s), 29 % of recently removed keys still hit; steady wire 1.5x worse | **I5 broken** |
| RIBLT (repair only) | n/a | yes (repair bytes 20-30x for sparse gaps, 4.9x at 100 k on 1 Mi keys; converges at 8 Mi keys) | n/a | n/a | initial sync 2.4x worse; CPU 95 ns per key per side per repair (0.76 s per side at 8 Mi keys for one difference), exact mirrors only | I1-I5 kept |
| Delta-log replay (repair only) | n/a | yes (11x at 1 Mi / 100 k; 16.9x repair bytes per peer-minute in the simulation) | n/a | n/a | none on the common path; cannot repair restarts or silent bursts (no deltas exist); owner keeps 0.7 MB/min of log at 1 % churn on 8 Mi keys | I1-I5 kept |

## 4 Result of the mechanical reading

- **Conjunct 1 is met** by C6b (w = 32), C3 (f = 16-32), C4 (eps = 2^-14),
  C5, C8a and Bloom on memory; by RIBLT and replay on repair bytes; **not** by
  C6a (1.93x) or C4 at eps = 2^-24 (1.99x), and not by any candidate on steady
  wire (identical for every receiver-side candidate; worse for Bloom and C5).
- **Conjunct 2 fails for every memory candidate** if "noise" is read strictly:
  each one regresses repair bytes for sparse gaps (C6b 1.5-1.9x at the worst
  measured points, C3/C4 2.2-3.2x, C5 13-26x) or CPU (C6a, C8a) or lookup
  latency (C6a, C6b), or wire (C5, Bloom). The only regression-free repair
  candidates are replay (for gaps inside its log) and, for sparse gaps, RIBLT
  (CPU aside).
- **Conjunct 3** is met by the candidates whose measured FP rate is at most
  about 1e-8 to 1e-7 for any workload in which at least one lookup in 10^4 targets
  a recently removed key (C6b w = 32, C3 f = 32, C4 eps = 2^-24); it is not
  met by the f = 16 class (C3 f = 16, C4 eps = 2^-14, C5) unless more than
  5-27 % of lookups target recently removed keys. **[maintainer]** supply the real fraction if it is known.
- **Conjunct 4**: Bloom breaks I5; C5 and C3/C4 need the leaf-depth session
  parameter and a refetch path; C8a puts a disk probe on the lookup path
  (accepted as local by the maintainer's answer); the rest keep I1-I5.

**Mechanical outcome: no candidate satisfies all four conjuncts, so the rule
says keep the status quo, with the smaller measured improvements below.**
The closest candidates, should the maintainer weigh conjunct 2 differently, are
C6b w = 32 (2.53x memory, 2.8e-8 measured, repair 1.5x worse at sparse gaps
unless replay covers them) and C3 f = 32 (2.21x, 2.0e-9, repair 2.2-3.2x worse).
Both would also make peers above 8 Mi keys visible (C6b w = 32: 64 MiB and
125 MiB per peer at 16 M and 32 M keys; C3 f = 16: 41 and 77 MiB).

## 5 Smaller measured improvements the rule allows

1. **Fix the repair livelock** (a defect, independent of the digest chosen):
   on 8 Mi-key peers the real session never converges for >= 50 k
   differences. Candidates measured in P1: replay (covers every gap whose
   deltas are retained: 768 KiB, 17 frames for 100 k changes), a leaf-depth rule
   (as in `respond_leaf`) or size-aware descent for the owner's answers, and
   RIBLT as a fallback (1.70 MiB for the same 100 k, 2.70 MiB for the restart).
2. **Delta-log replay in front of RBSR**: 16.9x fewer repair bytes per
   peer-minute (4 548 B vs 76 827 B at 1 % loss, 5 % reordering) and 11x fewer
   at 100 k changes on 1 Mi keys, no extra receiver memory, on any candidate;
   owner log of ~0.7 MB/min at 1 % churn on 8 Mi keys.
3. **Compact exact storage (C6a)**: 1.93x less receiver memory (5.77 vs 11.11
   B/key), zero false positives, wire and repair unchanged; costs 9-17x receiver
   apply CPU and 5x lookup latency (about 50 us per 64-peer lookup against 9.6 us),
   and misses the plan's 2x threshold by 4 %.
4. **A shrink-to-fit policy for the shard vectors** would remove most of the
   C6b-style slack growth and some of C1's (+2-9 % under churn, 28 B/key RSS
   after a real initial sync against 11.9 live): measured slack, not a candidate.

## 6 Open points for the maintainer

- Is 5.3 GiB per node at 64 x 8 Mi keys a real cost? If yes, C6b w = 32 / C3
  f = 32 with replay as the repair path is the measured configuration closest to
  clearing the rule; if no, close after P2 with items 1 and 2.
- Is 2.8e-8 (C6b w = 32) or 2.0e-9 (C3 f = 32) per lookup per peer "about 1e-8"?
  C6b at w = 40 (5 B per suffix) would be about 1e-10 at 5.4 B/key (not run).
- Replay and the leaf-depth rule are protocol additions; P0's stance
  (no compatibility, no version bump, mixed versions fall back to S3) applies.
- The status quo's `peer_stale_misses` in a real fleet was not measured (the
  candidates are not in the daemon); the break-even `f` above is the interface
  to whatever the harness or production later reports.

## 7 Decision (maintainer, 2026-11-07) and outcome (P3)

### 7.1 The decision

**Keep the exact mirror** (C1: the flat sorted shards, 11.1 B per key, no false
positives, no compatibility change to the storage). No candidate of section 3
is adopted, so the rule's mechanical outcome stands. Two repair changes from
section 5 are made, no storage change:

1. **Fix the range-based repair livelock** (section 5, item 1).
2. **Delta-log replay as the first repair step, RBSR as the fallback** (section 5,
   item 2). Protocol additions follow P0's stance: no compatibility shim and no
   version bump; a peer that cannot answer a replay request (older version,
   bloom mode, replay off) makes the mirror run the session, and a mixed
   fleet falls back to S3 as it does today for every digest message.

The open points of section 6 are closed by this: 5.3 GiB per node at 64 x 8 Mi
keys is accepted; C6b, C3 and C5 are not built; the rule's "smaller measured
improvement" clause is what this plan delivers. Shrink-to-fit of the shard
vectors (section 5, item 4) and compact storage (item 3) are not done.

### 7.2 Outcome (measured with the product code, RESULTS section 12)

| Item | P1 (status quo) | P3 | Where |
|---|---|---|---|
| Real session, 8 Mi keys, gap of 50 k or 100 k changes, 100 k burst, restart (160 k differences) | **FAILED**: 50 sessions, 517 MiB, nothing applied | converges: 50 k in 2 sessions and 17.2 MiB, 100 k in 3 sessions and 26.1 MiB, burst 3 sessions and 26.2 MiB, restart 3 sessions and 32.7 MiB | 12.1 |
| Real session, 8 Mi keys, 30 k gap | 24.6 MiB, 3 sessions, 611 rounds | 12.1 MiB, 2 sessions, 294 rounds | 12.1 |
| Sparse gaps, 1 Mi / 8 Mi keys, 1 k changes | 545 / 852 KiB | 504 / 777 KiB | 12.1 |
| Repair of a gap the owner's log still covers (product path: replay first), 8 Mi keys | n/a | 1 k: 8.2 KiB; 10 k: 77 KiB; 30 k: 232 KiB; 100 k: 767 KiB (17 rounds, 22 ms CPU) against 26.1 MiB (314 ms) by session | 12.2 |
| Silent burst or restart | RBSR | unchanged cost plus 227 B for the failed replay probe (burst); no probe on restart | 12.2 |
| Steady wire (delta + summary) | 715 KB per peer-minute at 1 % churn on 8 Mi keys | identical bytes (the log is local) | 12.3 |
| Publisher CPU per 250 ms tick, log on against off (box 3, A/B/B/A, 4 runs each) | | within noise: 8 Mi keys +0.0 % (1 %) and +0.2 % (10 %); 1 Mi keys +2.8 % (1 %) and +2.3 % (10 %), about 1 us of a 6.5 / 44 us tick | 12.3 |
| Owner memory for the log | none | 1.2 MB (0.1 %), 7.3 MB (1 %), 8.4 MB (cap reached at 10 %, 2.6 minutes of history), allocator-live, on 8 Mi keys; 8 MiB default cap, one log per node | 12.4 |

Not changed, and not claimed: receiver memory (11.1 B per key), staleness,
lookup latency.
