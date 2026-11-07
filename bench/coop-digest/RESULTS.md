# Plan 47 P1 — results

Phase P1 of `docs/plans/v1/wip/47-chunk-location-digests.md`. Every number
below was produced by a run of the commands in "Reproducing"; the raw rows
are in `raw/` (one file per suite). Nothing is estimated; where a figure is
computed from measured ones the text says "derived" and shows the formula.

Contents: [1 What was run](#1-what-was-run) ·
[2 Receiver memory](#2-receiver-memory-per-peer) ·
[3 False positives and negatives](#3-false-positives-and-negatives) ·
[4 Lookup latency](#4-lookup-latency) ·
[5 Publisher and receiver CPU per tick](#5-publisher-and-receiver-cpu-per-250-ms-tick) ·
[6 Steady wire](#6-steady-state-wire-bytes) ·
[7 Repair](#7-repair-after-a-gap-a-restart-and-initial-sync) ·
[8 Staleness under loss and reordering](#8-staleness-under-loss-and-reordering) ·
[9 Filter insert failures](#9-filter-insert-failures-and-rebuilds) ·
[10 Crates](#10-crates-licence-and-unsafe) ·
[11 Not measured](#11-what-was-not-measured) ·
[Reproducing](#reproducing)

## 1 What was run

### 1.1 Environment

| Item | Value |
|---|---|
| Product code read and reused | `origin/main` at `31c79e49` (`crates/net/src/{reconcile,bloom,message}.rs`, `crates/engine/src/coop/exact.rs`) |
| Hardware | two devboxes of one instance type: 16 vCPU AMD EPYC 9R45, 30 GiB RAM, local NVMe. Correctness, size, wire and false-positive runs on the shared box (its CPU numbers are **not** cited); every timing figure (lookup latency, CPU per tick, repair CPU) on the otherwise idle box, interleaved A/B/B/A over 4 repetitions, median (min..max) |
| Toolchain | Rust 1.98 (the repository pin), `--release`, thin LTO, one codegen unit |
| Binary | two builds of `bench/coop-digest`, hashes in the last section; the unit tests (acc fingerprint equals the real one, generic RBSR equals the real `Session`, `respond_leaf(None)` equals `respond`, every candidate follows a lossy chain and repairs) pass on the committed source |
| Keys | uniform random u64, splitmix64 seeded (`--seed`, default 1), sorted and deduplicated per peer; peer `p` uses `seed ^ f(p)` so peers are disjoint with overwhelming probability |
| Memory | `live B/key` = bytes the counting global allocator reports live (requested sizes) after building the mirrors, divided by peers x keys; `RSS` = `/proc/self/statm` resident delta over the same span (adds malloc overhead and fragmentation) |

### 1.2 Candidates implemented

All candidates sit behind one trait (`LocationMirror` in `src/mirror.rs`);
the owner side is the real `reconcile::respond` over the real `KeySet`.

| Id | Spec argument | What it is | Reuse |
|---|---|---|---|
| C1 | `c1` | status quo: the real `reconcile::Mirror` (flat sorted `Vec<u64>` shards) | real type, real `apply_delta` |
| Bloom | `bloom` | the existing option: one `net::bloom::Bloom` per hash-prefix bucket (10 bits/key, k = 7, <= 16 KiB, <= 256 buckets); snapshot each 30 s digest interval, add-only deltas in between | real `Bloom`, `bucket_count_for`, `bucket_index`, real `CacheDigest` / `CacheDigestDelta` payloads for wire |
| C3 | `c3:<f>:<keys/shard>` | cuckoo filter per shard: 4-way buckets, `f`-bit fingerprints, any bucket count (`alt = (H(fp) - i) mod m`), built at 85 % load, victim slot, per-shard exact accumulator | in-tree (the crate `cuckoofilter` has 8-bit fingerprints and evicts a random item on overflow) |
| C4 | `c4:<log2 1/err>:<keys/shard>` | `qfilter` 0.3.1 resizeable rank-select quotient filter per shard, same shard/accumulator shape as C3; grows by doubling up to 2x its build size | crate `qfilter`, exact version pinned |
| C5 | `c5:<keys/bucket>` | `xorf::BinaryFuse16` per prefix bucket (4096 keys) + exact add/remove overlay; the owner re-snapshots a bucket after `K` changes to it (`--resnap`); a repair refetches the bucket and the receiver rebuilds the filter | crate `xorf`, exact version pinned |
| C6a | `c6a` | the exact mirror with each <= 128-key shard Elias-Fano coded (implicit prefix) | in-tree |
| C6b | `c6b:<suffix bytes>:<keys/shard>` | implicit shard prefix + a `w`-bit suffix per key (duplicate suffixes kept as repeated entries = refcount) + exact accumulator per shard | in-tree |
| C8a | `c8a` | in-memory C3 filter in front of an **exact mirror on local disk** (sorted run, sparse in-memory index every 128th key, in-memory add/remove overlay merged into a new file at 0.5 % of n); a filter hit is confirmed by a 1 KiB `pread`; repair works to any depth | in-tree |
| RBSR | method `rbsr` | the real owner (`respond`) and a generic initiator that reproduces the real `Session` (parity test) and also drives the shard-granular mirrors | real `respond`, `Query`, `Reply`, `Answer`, `encode_keys` |
| RBSR (real session) | method `real` | `reconcile::sync_in_memory` repeated up to 50 times on the real mirror type | the real driver |
| RIBLT | method `riblt` | rateless IBLT over u64 (in-tree: the reference mapping, 32-bit checksum, count sent as deviation from `N/(1+j/2)`), batches of 32 symbols doubling to 3072 | in-tree; exact mirrors only (it must enumerate its own keys) |
| Replay | method `replay` | the owner keeps its last deltas; a receiver with a gap asks for "deltas after seq" and re-applies them on the exact base | real `Delta`, real `apply_delta` path |

Not built, and why: counting and scalable Bloom (dropped in RESEARCH §5);
fixed IBLT, Minisketch, Negentropy (dropped in RESEARCH §5); C8c narrow
keys end to end (deferred in RESEARCH §5, wire unchanged here).

### 1.3 Model choices to read the numbers with

- **Sender.** `LocalSet` is `pub(super)`, so `Publisher::absorb`
  (`src/publisher.rs`) is a copy of `LocalSet::absorb` at the commit above,
  minus the recent-removals bookkeeping; deltas are the real `Delta` type.
- **Wire.** Every size is `net::message::wire_len` of the real payload
  (`CacheSetDelta`, `CacheSummary`, `ReconcileRequest`, `ReconcileReply`,
  `CacheDigest`, `CacheDigestDelta`): postcard body plus the 100-byte signed
  envelope; gossip fan-out and QUIC framing are not included. Candidate
  messages that do not exist today (C5 snapshots, RIBLT batches, replay
  requests) are postcard-encoded structs plus the same envelope, and C5
  asserts <= `MAX_FRAME`.
- **Shard-granular mirrors (C3, C4, C5, C6b)** cannot answer a fingerprint
  below their shard depth `g`. They need one protocol addition that is
  **not** in the wire sizes: the session opens with the receiver's leaf depth
  and the owner answers a differing range at depth >= `g` with its `Items`
  (`respond_leaf`, a copy of `respond` plus that rule; identical to `respond`
  when no depth is given). A range that would split below `g` is expanded into
  its depth-`g` sub-ranges. Without this the owner answers `Children`
  that the receiver cannot use.
- **Shard sizing.** C3/C4/C5/C6b size their shards from the owner's
  advertised count when the mirror is created and never reshard (they hold no
  full keys). C1 and C6a reshard locally.
- **Churn.** Synthetic: each 250 ms tick removes random present keys and adds
  fresh random keys (half and half keeps the set level; `--add-frac` changes
  it); `churn/min` is the percentage of the set changed per minute. "Burst"
  is a single tick of 100 k changes (the silent publish path).
- **Simulation (`sim`).** One owner, `P` receivers, virtual 250 ms clock. Each
  delta and summary is dropped with probability `loss` per receiver; a
  fraction `reorder-pct` of messages is delayed 1-3 ticks (window of 4). A
  receiver starts a session on a diverged delta or a mismatching summary, at
  least 500 ms after its previous one; a session costs `rounds x 20 ms` of
  extra staleness for the keys it fixes. A change's staleness is the time
  from the change (uniform inside its tick) until that mirror answers
  correctly. Lookups per tick and peer: 20 held keys, 20 keys removed within
  the last 60 s, 20 random absent keys.
- **Repair timing.** CPU is measured; "derived wall" adds `rounds x RTT`
  (1 ms and 50 ms) and is labelled derived.
- **RIBLT CPU** includes building both sides' symbol state from scratch for
  each repair (nothing cached), as in a first implementation.

## 2 Receiver memory per peer

Metric: bytes the receiver holds per key of a peer's set. Source: `raw/mem.md`
(`run.sh mem`), counting allocator, one mirror per peer, built in bulk from
the owner's sorted keys (`live` = requested heap bytes; the RSS figure in
the raw file adds malloc overhead).

Live bytes per key, one peer, by keys per peer (bulk build; each cell is
`live / RSS` B/key; the RSS of the 10 k column is dominated by process
baseline):

| candidate | 10,000 | 100,000 | 1,000,000 | 4,000,000 | 8,000,000 | 16,000,000 | 32,000,000 |
|---|---|---|---|---|---|---|---|
| C1 exact | 13.41 / 36.04 | 11.60 / 12.62 | 11.88 / 12.15 | 11.88 / 12.16 | 11.11 / 11.25 | 10.37 / 10.44 | n/a |
| C6a EF-coded exact | 7.83 / 16.38 | 7.15 / 7.50 | 6.54 / 6.81 | 6.29 / 6.56 | 5.77 / 6.45 | n/a | n/a |
| C6b suffix w=32 | 5.24 / 13.93 | 4.98 / 5.32 | 4.79 / 5.03 | 4.79 / 5.02 | 4.39 / 4.66 | 4.20 / 4.27 | 4.10 / 4.14 |
| C6b suffix w=24 | 4.24 / 13.11 | 3.98 / 4.42 | 3.79 / 4.06 | 3.79 / 4.05 | 3.39 / 3.63 | n/a | n/a |
| C6b suffix w=16 | 3.24 / 11.88 | 2.98 / 3.36 | 2.79 / 3.05 | 2.79 / 3.04 | 2.39 / 2.58 | n/a | n/a |
| C3 cuckoo f=12 | 2.26 / 10.65 | 2.15 / 2.25 | 2.07 / 2.14 | 2.07 / 2.14 | 2.07 / 2.14 | n/a | n/a |
| C3 cuckoo f=16 | 2.86 / 10.65 | 2.74 / 2.79 | 2.66 / 2.72 | 2.66 / 2.71 | 2.66 / 2.71 | 2.66 / 2.71 | 2.51 / 2.53 |
| C3 cuckoo f=24 | 4.04 / 11.88 | 3.93 / 3.97 | 3.85 / 3.91 | 3.85 / 3.91 | 3.85 / 3.91 | n/a | n/a |
| C3 cuckoo f=32 | 5.23 / 13.11 | 5.12 / 5.16 | 5.03 / 5.07 | 5.03 / 5.07 | 5.03 / 5.06 | n/a | n/a |
| C4 quotient eps=2^-14 | 3.99 / 37.27 | 3.18 / 5.98 | 3.64 / 3.90 | 3.64 / 3.76 | 3.63 / 3.73 | 3.63 / 3.72 | 3.54 / 3.57 |
| C4 quotient eps=2^-24 | 6.04 / 33.59 | 4.81 / 6.88 | 5.60 / 5.92 | 5.59 / 5.70 | 5.58 / 5.69 | n/a | n/a |
| C5 fuse16+overlay | 2.78 / 17.61 | 2.71 / 3.52 | 2.70 / 2.81 | 2.70 / 2.73 | 2.70 / 2.72 | 2.70 / 2.71 | 2.70 / 2.71 |
| Bloom (10 b/key, k=7) | 1.27 / 9.01 | 1.26 / 1.23 | 1.26 / 1.26 | 1.05 / 1.05 | 0.53 / 0.53 | n/a | n/a |
| C8a cuckoo over disk exact | 2.95 / 18.02 | 2.81 / 4.18 | 2.73 / 2.85 | 2.73 / 2.80 | 2.73 / 2.79 | 2.73 / 2.78 | 2.57 / 2.60 |

Reading it against the P0 predictions:

- **C1-a** (8-12 B/key): measured 11.1-13.4 B/key (11.88 at 1 Mi, 11.11 at
  8 Mi). The upper end of the documented range.
- **C6a-a** (5.4-6.6): 5.77 at 8 Mi, 6.54 at 1 Mi, 7.8 at 10 k (per-shard
  headers dominate small sets).
- **C6b-a** (3.4-4.8 at the table's settings): 4.79 at 1 Mi and 4.39 at
  8 Mi for w = 32; 3.79 / 3.39 for w = 24. Matches the formula.
- **C3-a** (2.5 B/key + 24 B per bucket at ~2^-16): 2.66 B/key at f = 16,
  3.85 at f = 24, 5.03 at f = 32.
- **C5-a** (2.25): 2.70 (the 4096-key bucket headers and overlay vectors add
  0.45 B/key).
- **C4-a** (within 10 % of C3 at equal ε): not confirmed. C4 at ε = 2^-14
  measures 1.9e-5 false positives and 3.64 B/key; C3 f = 19 would be the
  equal-ε point, between the measured f = 16 (2.66 B, 1.0e-4) and f = 24
  (3.85 B, 3.8e-7): about 3.0 B/key by linear interpolation of those two
  measured rows (derived). C4's power-of-two slot count costs it about 20 %.

At the target fleet shape (every node mirrors 64 peers of 8 Mi keys,
**measured, not scaled**: 64 distinct mirrors built in one process):

64 peers (totals, allocator-live / RSS delta):

| candidate | 64 x 1M keys | live B/key | 64 x 8M keys | live B/key | 8M ratio vs C1 |
|---|---|---|---|---|---|
| C1 exact | 725.12 MiB / 756.39 MiB | 11.88 | 5.30 GiB / 5.36 GiB | 11.11 | 1.00x |
| C6a EF-coded exact | 399.39 MiB / 422.61 MiB | 6.54 | 2.75 GiB / 3.08 GiB | 5.77 | 1.93x |
| C6b suffix w=32 | 292.15 MiB / 313.80 MiB | 4.79 | 2.09 GiB / 2.22 GiB | 4.39 | 2.53x |
| C3 cuckoo f=16 | 162.56 MiB / 173.20 MiB | 2.66 | 1.27 GiB / 1.29 GiB | 2.66 | 4.18x |
| C4 quotient eps=2^-14 | 221.88 MiB / 234.21 MiB | 3.64 | 1.73 GiB / 1.77 GiB | 3.64 | 3.05x |
| C5 fuse16+overlay | 164.72 MiB / 172.70 MiB | 2.70 | 1.29 GiB / 1.29 GiB | 2.70 | 4.11x |
| Bloom (10 b/key, k=7) | 76.68 MiB / 84.43 MiB | 1.26 | 256.75 MiB / 257.02 MiB | 0.53 | 20.96x |
| C8a cuckoo over disk exact | 166.39 MiB / 177.14 MiB | 2.73 | 1.30 GiB / 1.32 GiB | 2.73 | 4.07x |

Initial sync into an empty mirror with the real protocol (n = 1M, one peer), live B/key and RSS B/key afterwards:

| candidate | live B/key | RSS B/key | note |
|---|---|---|---|
| C1 exact | 11.88 | 28.18 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |
| C6a EF-coded exact | 6.54 | 23.30 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |
| C6b suffix w=32 | 4.79 | 16.67 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |
| C3 cuckoo f=16 | 2.66 | 14.44 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |
| C4 quotient eps=2^-14 | 3.64 | 15.59 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |
| C5 fuse16+overlay | 2.70 | 14.50 | initial sync: 2 sessions, 258 rounds, 7.16 MiB |

Above the 8 Mi cap (one peer):

| candidate | n | live B/key | live |
|---|---|---|---|
| C1 (real daemon rule) | 16,000,000 | not mirrored: peer treated as unknown | - |
| C1 exact | 16,000,000 | 10.37 | 158.18 MiB |
| C6b suffix w=32 | 16,000,000 | 4.20 | 64.04 MiB |
| C3 cuckoo f=16 | 16,000,000 | 2.66 | 40.64 MiB |
| C4 quotient eps=2^-14 | 16,000,000 | 3.63 | 55.46 MiB |
| C5 fuse16+overlay | 16,000,000 | 2.70 | 41.19 MiB |
| C8a cuckoo over disk exact | 16,000,000 | 2.73 | 41.59 MiB |
| C6b suffix w=32 | 32,000,000 | 4.10 | 125.07 MiB |
| C3 cuckoo f=16 | 32,000,000 | 2.51 | 76.54 MiB |
| C4 quotient eps=2^-14 | 32,000,000 | 3.54 | 108.14 MiB |
| C5 fuse16+overlay | 32,000,000 | 2.70 | 82.39 MiB |
| C8a cuckoo over disk exact | 32,000,000 | 2.57 | 78.45 MiB |

Other memory facts, all measured:

- **Growth under churn** (two minutes at 1 % and 10 % per minute, publisher
  state excluded; table in section 6): C1 grows 2-9 %, C3 0 %, C4 2-7 %,
  C5 7-10 % (overlay), **C6b 31-76 %**: its `Vec<u8>` suffix shards are
  spliced on every add/remove and keep amortised slack; a shrink-to-fit
  policy would be needed.
- **After the real initial sync** (empty mirror filled through RBSR), RSS is
  2.4-5.4x the live bytes for every candidate because the shard vectors grow by
  doubling and fragment the heap (C1: 28.2 B/key RSS for 11.9 live; C3: 14.4
  for 2.7). Allocator-live is the figure used throughout; production RSS would
  be at the larger end for a mirror that was built by sync rather than in bulk.
- **Above the 8 Mi cap** the real daemon treats the peer as unknown
  (`MAX_MIRROR_KEYS`). Every approximate candidate can mirror it: 16 M keys
  cost 64 MiB (C6b), 41 MiB (C3), 55 MiB (C4) per peer, 32 M keys 125 / 77 /
  108 MiB. This is the "approximate visibility above the cap" the maintainer
  asked for, with the false-positive rates of section 3.
- **Bloom** is smallest in the table but meaningless above ~3.3 M keys: its
  256 x 16 KiB cap makes it saturate (section 3).

## 3 False positives and negatives

Metric: against the owner's exact set (the oracle: random 64-bit keys are
absent from every disjoint peer set with probability > 1 - 3e-11), per
lookup per peer. A "hit" lookup of peer `j`'s key also probes the other peers,
whose sets are disjoint, so their claims are false positives too. Source:
`raw/fp.md` and `raw/fp-large.md`.

| candidate | n per peer | peers | false negatives | false positives / lookups | measured FP rate per lookup per peer |
|---|---|---|---|---|---|
| C1 exact | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C6a EF-coded exact | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C6b suffix w=16 | 1,000,000 | 8 | 0 | 12816 / 13500000 | 9.49e-4 |
| C6b suffix w=24 | 1,000,000 | 8 | 0 | 50 / 13500000 | 3.70e-6 |
| C6b suffix w=32 | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C3 cuckoo f=12 | 1,000,000 | 8 | 0 | 21998 / 13500000 | 1.63e-3 |
| C3 cuckoo f=16 | 1,000,000 | 8 | 0 | 1337 / 13500000 | 9.90e-5 |
| C3 cuckoo f=24 | 1,000,000 | 8 | 0 | 8 / 13500000 | 5.93e-7 |
| C3 cuckoo f=32 | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C4 quotient eps=2^-14 | 1,000,000 | 8 | 0 | 271 / 13500000 | 2.01e-5 |
| C4 quotient eps=2^-24 | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C5 fuse16+overlay | 1,000,000 | 8 | 0 | 191 / 13500000 | 1.41e-5 |
| Bloom (10 b/key, k=7) | 1,000,000 | 8 | 0 | 110814 / 13500000 | 8.21e-3 |
| C8a cuckoo over disk exact | 1,000,000 | 8 | 0 | 0 / 13500000 | 0.00e0 |
| C1 exact | 8,000,000 | 4 | 0 | 0 / 12700000 | 0.00e0 |
| C6a EF-coded exact | 8,000,000 | 4 | 0 | 0 / 12700000 | 0.00e0 |
| C6b suffix w=16 | 8,000,000 | 4 | 0 | 23753 / 12700000 | 1.87e-3 |
| C6b suffix w=32 | 8,000,000 | 4 | 0 | 1 / 12700000 | 7.87e-8 |
| C3 cuckoo f=16 | 8,000,000 | 4 | 0 | 1300 / 12700000 | 1.02e-4 |
| C3 cuckoo f=32 | 8,000,000 | 4 | 0 | 0 / 12700000 | 0.00e0 |
| C4 quotient eps=2^-14 | 8,000,000 | 4 | 0 | 243 / 12700000 | 1.91e-5 |
| C5 fuse16+overlay | 8,000,000 | 4 | 0 | 212 / 12700000 | 1.67e-5 |
| Bloom (10 b/key, k=7) | 8,000,000 | 4 | 0 | 2944775 / 12700000 | 2.32e-1 |
| C6b suffix w=32 | 16,000,000 | 2 | 0 | 2 / 12300000 | 1.63e-7 |
| C6b suffix w=24 | 16,000,000 | 2 | 0 | 183 / 12300000 | 1.49e-5 |
| C3 cuckoo f=16 | 16,000,000 | 2 | 0 | 1252 / 12300000 | 1.02e-4 |
| C3 cuckoo f=24 | 16,000,000 | 2 | 0 | 4 / 12300000 | 3.25e-7 |
| Bloom (10 b/key, k=7) | 16,000,000 | 2 | 0 | 9548089 / 12300000 | 7.76e-1 |

Large false-positive runs (5 seeds x 50M random absent keys x 4 peers, n = 8M per peer, per candidate):

| candidate | seeds | false positives / lookups | pooled FP rate per lookup per peer | false negatives | formula |
|---|---|---|---|---|---|
| C6b suffix w=32 | 5 | 28 / 1,000,035,000 | 2.80e-08 | 0 | n/2^(shard_bits+32) = 8e6/2^48 = 2.8e-8 |
| C3 cuckoo f=32 | 5 | 2 / 1,000,035,000 | 2.00e-09 | 0 | 2*4*load/2^32 ~ 1.6e-9 |
| C3 cuckoo f=24 | 5 | 383 / 1,000,035,000 | 3.83e-07 | 0 | ~4.1e-7 |
| C4 quotient eps=2^-24 | 5 | 21 / 1,000,035,000 | 2.10e-08 | 0 | <= 6e-8 |

- **False negatives: zero in every run**, for every candidate, after build or
  repair (also in every simulation: `final FN` column of section 8).
- **C1-d**: 0 false positives in 13.5 M lookups per candidate at 1 Mi keys and
  12.7 M at 8 Mi (C1, C6a, C8a).
- **C3-b** (`2bα/2^f`): f = 16 measures 9.9e-5 / 1.02e-4 against a predicted
  1.0e-4; f = 24 measures 3.8e-7 (predicted 4.1e-7).
- **C6b-b** (`n/2^(shard_bits+w)`): 28 false positives in 1.0e9 lookups at
  8 Mi keys, w = 32 = **2.8e-8**, exactly the formula's 2.8e-8; at 16 Mi keys 2 in
  12.3 M = 1.6e-7 (formula 5.7e-8; two events, consistent).
  **The maintainer's "about 1e-8, if measured" is met to within 3x** by
  C6b at w = 32 (and by C6b at w = 40, not run: ε drops 256x), by C3 at
  f = 32 (**2.0e-9**, 2 in 1.0e9) and by C4 at ε = 2^-24 (2.1e-8).
- **Bloom** (the existing option): 0.82 % at 1 Mi keys (formula 0.82 %),
  23 % at 8 Mi keys and 78 % at 16 Mi, because the 256 x 16 KiB cap is
  reached at ~3.3 M keys.
- C5's 1.4e-5 to 1.7e-5 is the `BinaryFuse16` rate (2^-16).

Expected wasted peer round trips (derived, `P · ε · L`): a lookup that no
peer can serve probes `P` mirrors, so at `P = 64` and `L = 10^6` mirror-consulting
lookups per minute a node wastes `64 · ε · 10^6` round trips per minute:
C6b w = 32 (2.8e-8) 1.8; C3 f = 32 (2.0e-9) 0.13; C4 ε = 2^-24 (2.1e-8) 1.3;
C3 f = 24 (3.8e-7) 24; C3 f = 16 (1.0e-4) 6 400; C4 ε = 2^-14 (1.9e-5) 1 200.
`L` is not measured (no production data); the formula is the contract.

## 4 Lookup latency

Metric: latency of one lookup that consults all `P` peers' mirrors (what
`exact_holders` does), hit (key of one peer) and miss (random key), plus the
single-peer mean from a batch. Timing box only, 4 interleaved repetitions,
order reversed on even repetitions (A B B A), 200 000 lookups each, warm
caches. Source: `raw/t-lookup.md`, summarised by `summarize.py`.
C8a's hit path includes one 1 KiB `pread` per hit against a warm page cache;
cold-cache reads were not measured (it would need root to drop caches).

#### n = 10,000 keys per peer, 8 peers (fan-out over all peers; ns; median (min..max) of 4 runs)

| candidate | hit p50 | hit p99 | miss p50 | miss p99 | single-peer hit mean | single-peer miss mean | miss p50 vs C1 |
|---|---|---|---|---|---|---|---|
| C1 exact | 100 (100..100) | 130 (130..130) | 100 (100..100) | 130 (130..130) | 15.4 (15.3..15.9) | 7.3 (7.3..7.3) | 1.00x |
| C6a EF-coded exact | 1780 (1780..1780) | 3430 (3430..3440) | 1780 (1780..1780) | 3440 (3440..3440) | 226.2 (226.0..228.7) | 215.9 (215.4..216.2) | 17.80x |
| C6b suffix w=32 | 270 (270..270) | 330 (330..340) | 270 (270..270) | 330 (330..330) | 40.6 (40.3..41.2) | 28.9 (28.8..28.9) | 2.70x |
| C3 cuckoo f=16 | 120 (120..120) | 130 (130..130) | 110 (110..110) | 120 (120..120) | 27.6 (27.4..27.8) | 10.9 (10.8..10.9) | 1.10x |
| C4 quotient eps=2^-14 | 140 (140..140) | 200 (200..200) | 120 (120..120) | 190 (190..190) | 22.4 (22.4..22.4) | 12.3 (12.2..12.3) | 1.20x |
| C5 fuse16+overlay | 50 (50..50) | 60 (60..60) | 50 (50..50) | 60 (60..60) | 12.8 (12.7..12.9) | 3.9 (3.9..3.9) | 0.50x |
| Bloom (10 b/key, k=7) | 170 (170..170) | 230 (230..230) | 150 (150..150) | 250 (240..250) | 18.9 (18.7..19.2) | 16.1 (16.0..16.2) | 1.50x |
| C8a cuckoo over disk exact | 320 (320..330) | 370 (360..370) | 120 (120..120) | 130 (130..130) | 196.0 (194.5..203.9) | 11.3 (11.3..11.5) | 1.20x |

#### n = 1,000,000 keys per peer, 8 peers (fan-out over all peers; ns; median (min..max) of 4 runs)

| candidate | hit p50 | hit p99 | miss p50 | miss p99 | single-peer hit mean | single-peer miss mean | miss p50 vs C1 |
|---|---|---|---|---|---|---|---|
| C1 exact | 660 (660..670) | 1385 (1370..1390) | 650 (650..650) | 1290 (1240..1320) | 35.0 (34.0..35.9) | 36.7 (34.8..36.8) | 1.00x |
| C6a EF-coded exact | 3595 (3540..3770) | 7405 (7300..7480) | 3550 (3470..3610) | 7270 (7190..7470) | 343.7 (342.1..345.2) | 337.7 (336.3..338.8) | 5.46x |
| C6b suffix w=32 | 810 (790..810) | 2390 (2320..2460) | 775 (770..840) | 2180 (2130..2220) | 62.7 (61.8..62.8) | 54.5 (54.0..57.2) | 1.19x |
| C3 cuckoo f=16 | 220 (210..220) | 570 (550..610) | 205 (200..210) | 420 (350..430) | 39.2 (39.1..39.9) | 16.7 (16.4..16.9) | 0.32x |
| C4 quotient eps=2^-14 | 295 (280..300) | 705 (700..730) | 230 (230..230) | 530 (530..550) | 32.6 (32.3..33.0) | 19.7 (19.5..19.9) | 0.35x |
| C5 fuse16+overlay | 90 (90..100) | 340 (320..360) | 90 (90..90) | 235 (230..280) | 14.2 (13.6..14.3) | 5.8 (5.8..6.0) | 0.14x |
| Bloom (10 b/key, k=7) | 220 (220..220) | 310 (310..320) | 200 (200..200) | 330 (330..330) | 21.1 (21.1..21.2) | 19.9 (19.6..20.1) | 0.31x |
| C8a cuckoo over disk exact | 740 (720..740) | 1275 (1230..1290) | 210 (210..210) | 440 (430..450) | 350.9 (345.0..354.0) | 17.5 (17.4..17.5) | 0.32x |

#### n = 1,000,000 keys per peer, 64 peers (fan-out over all peers; ns; median (min..max) of 4 runs)

| candidate | hit p50 | hit p99 | miss p50 | miss p99 | single-peer hit mean | single-peer miss mean | miss p50 vs C1 |
|---|---|---|---|---|---|---|---|
| C1 exact | 8255 (8220..8280) | 10456 (10290..10610) | 8275 (8230..8280) | 10420 (10320..10471) | 77.9 (72.9..80.8) | 39.7 (38.7..42.1) | 1.00x |
| C6a EF-coded exact | 39460 (39271..39600) | 63410 (63151..63720) | 39525 (39311..39680) | 63400 (63100..63701) | 376.1 (367.0..377.2) | 349.5 (345.5..351.3) | 4.78x |
| C6b suffix w=32 | 21070 (20980..21650) | 27495 (27390..28140) | 21080 (20880..21650) | 27436 (27360..28140) | 93.4 (92.2..98.9) | 73.1 (68.1..80.0) | 2.55x |
| C3 cuckoo f=16 | 6565 (6520..6620) | 10610 (10250..10640) | 6555 (6540..6620) | 10295 (10260..10490) | 73.1 (71.4..77.8) | 28.8 (26.8..30.9) | 0.79x |
| C4 quotient eps=2^-14 | 5075 (5060..5120) | 7375 (7060..7550) | 4980 (4950..4990) | 6965 (6870..7320) | 78.8 (77.1..80.1) | 33.5 (31.9..34.7) | 0.60x |
| C5 fuse16+overlay | 2620 (2600..2650) | 3000 (2980..3020) | 2610 (2590..2650) | 2985 (2960..3020) | 51.5 (48.3..56.3) | 12.2 (11.2..13.3) | 0.32x |
| Bloom (10 b/key, k=7) | 4340 (4310..4380) | 5620 (5580..5680) | 4240 (4230..4260) | 5575 (5550..5591) | 57.7 (56.3..60.0) | 21.9 (21.8..22.2) | 0.51x |
| C8a cuckoo over disk exact | 8005 (7930..8040) | 11820 (11660..12030) | 6680 (6600..6710) | 10430 (10240..10530) | 522.6 (515.6..534.9) | 30.5 (28.3..31.6) | 0.81x |

#### n = 8,000,000 keys per peer, 64 peers (fan-out over all peers; ns; median (min..max) of 4 runs)

| candidate | hit p50 | hit p99 | miss p50 | miss p99 | single-peer hit mean | single-peer miss mean | miss p50 vs C1 |
|---|---|---|---|---|---|---|---|
| C1 exact | 9630 (9480..9890) | 14375 (14110..14871) | 9645 (9500..9900) | 14375 (14150..14870) | 258.1 (254.2..268.0) | 141.8 (132.5..147.2) | 1.00x |
| C6a EF-coded exact | 50946 (50160..51330) | 91186 (90011..91570) | 51055 (50330..51431) | 91110 (89811..91511) | 754.2 (750.5..759.2) | 762.2 (743.9..769.4) | 5.29x |
| C6b suffix w=32 | 19270 (18300..19600) | 26220 (24940..26640) | 19255 (18360..19620) | 26075 (25070..26420) | 216.6 (210.8..221.8) | 251.4 (246.5..257.5) | 2.00x |
| C3 cuckoo f=16 | 6655 (6330..6870) | 7665 (7260..8310) | 6650 (6300..6850) | 7675 (7260..9670) | 108.3 (105.3..114.7) | 80.0 (74.3..86.0) | 0.69x |
| C4 quotient eps=2^-14 | 6030 (5940..6080) | 7686 (7580..7750) | 5950 (5830..6000) | 7605 (7450..7680) | 136.9 (133.6..138.6) | 64.9 (64.8..67.5) | 0.62x |
| C5 fuse16+overlay | 2726 (2720..2740) | 3230 (3200..3240) | 2735 (2690..2760) | 3220 (3190..3280) | 116.5 (112.3..121.5) | 35.0 (34.6..35.6) | 0.28x |
| Bloom (10 b/key, k=7) | 5805 (5790..5840) | 6620 (6580..6650) | 5835 (5790..5850) | 6625 (6580..6650) | 63.9 (61.8..64.8) | 31.2 (30.6..31.6) | 0.60x |
| C8a cuckoo over disk exact | 9300 (9140..9310) | 13365 (12980..13490) | 7510 (7400..7530) | 11690 (11560..11790) | 864.6 (853.5..895.7) | 84.3 (81.0..85.2) | 0.78x |

Predictions: **C3-e** (within 2x of C1): confirmed, C3 is *faster* than C1 at
64 peers (0.69-0.79x of C1's miss p50), because C1 binary-searches a shard
reached through two dependent loads while a cuckoo probe touches two adjacent
buckets. **C6a**: 4.8-5.3x slower than C1 (the Elias-Fano shard is decoded
sequentially). **C6b**: 2.0-2.6x slower at 64 peers (byte-wise binary search
over `Vec<u8>`; 1.2x at 8 peers and 1 Mi keys). **C4-c** (slower than cuckoo):
not confirmed; C4 is 0.62x of C1, slightly faster than C3 at 64 peers and
slower at 8. All of them are microseconds against a network round trip of
hundreds of microseconds or more.

## 5 Publisher and receiver CPU per 250 ms tick

Metric: mean and p99 CPU of one tick, as measured by timing the real
`reconcile` calls: the publisher's `absorb` (the copied `LocalSet::absorb`)
and one receiver's `apply_delta` for the same deltas. Timing box, 4
interleaved repetitions of a 2-minute run each; microseconds per tick; median
(min..max). Source: `raw/t-steady.md`.

#### n = 1,000,000, churn 1% per minute (per 250 ms tick, microseconds; median (min..max) over runs)

| candidate | publisher absorb mean | publisher p99 | receiver apply mean | receiver p99 | receiver mean vs C1 |
|---|---|---|---|---|---|
| C1 exact | 6.9 (6.4..7.5) | 9.4 (7.9..16.2) | 6.3 (6.0..6.6) | 9.1 (9.0..9.8) | 1.00x |
| C6a EF-coded exact | 6.4 (6.0..7.2) | 9.4 (7.7..12.5) | 59.2 (58.6..59.8) | 67.0 (65.1..69.5) | 9.41x |
| C6b suffix w=32 | 6.1 (5.9..6.2) | 8.8 (7.5..13.1) | 6.5 (6.3..6.7) | 11.1 (9.9..11.4) | 1.04x |
| C3 cuckoo f=16 | 6.0 (5.8..6.1) | 7.7 (7.5..8.2) | 3.9 (3.8..4.0) | 6.1 (5.9..7.4) | 0.63x |
| C4 quotient eps=2^-14 | 6.0 (5.8..6.1) | 8.2 (8.1..8.4) | 6.3 (6.1..6.3) | 12.6 (11.4..14.1) | 1.00x |
| C5 fuse16+overlay | 5.8 (5.7..6.1) | 8.4 (7.6..10.4) | 2.5 (2.5..2.6) | 3.1 (2.9..3.1) | 0.40x |
| Bloom (10 b/key, k=7) | 5.5 (5.4..5.7) | 8.3 (7.4..8.5) | 1.6 (1.6..1.6) | 2.6 (2.5..2.7) | 0.26x |
| C8a cuckoo over disk exact | 7.4 (6.8..9.4) | 13.8 (11.8..18.0) | 297.2 (241.7..355.8) | 16.4 (15.6..17.5) | 47.26x |

#### n = 1,000,000, churn 10% per minute (per 250 ms tick, microseconds; median (min..max) over runs)

| candidate | publisher absorb mean | publisher p99 | receiver apply mean | receiver p99 | receiver mean vs C1 |
|---|---|---|---|---|---|
| C1 exact | 46.6 (46.2..47.6) | 66.2 (61.7..67.1) | 32.4 (31.5..33.4) | 65.8 (64.8..68.6) | 1.00x |
| C6a EF-coded exact | 43.2 (42.3..44.0) | 60.6 (59.6..62.5) | 566.5 (566.1..567.8) | 602.9 (589.7..613.8) | 17.47x |
| C6b suffix w=32 | 43.2 (41.1..43.5) | 61.8 (53.5..63.1) | 38.6 (38.1..40.0) | 67.7 (60.0..81.1) | 1.19x |
| C3 cuckoo f=16 | 39.6 (38.9..40.2) | 55.2 (53.0..57.9) | 25.3 (24.5..25.6) | 39.2 (36.6..44.7) | 0.78x |
| C4 quotient eps=2^-14 | 40.6 (39.4..41.3) | 58.2 (54.9..60.7) | 42.0 (41.1..42.1) | 66.8 (62.8..68.5) | 1.29x |
| C5 fuse16+overlay | 39.0 (38.7..39.2) | 56.5 (53.8..60.0) | 18.6 (18.5..18.7) | 24.7 (24.6..24.9) | 0.57x |
| Bloom (10 b/key, k=7) | 44.0 (43.3..44.3) | 58.5 (56.0..59.3) | 11.2 (11.2..11.3) | 15.9 (15.2..17.9) | 0.35x |
| C8a cuckoo over disk exact | 82.0 (81.7..82.6) | 103.4 (102.8..104.8) | 15103.3 (14142.2..16175.2) | 641987.8 (641493.5..642864.5) | 465.81x |

#### n = 8,000,000, churn 1% per minute (per 250 ms tick, microseconds; median (min..max) over runs)

| candidate | publisher absorb mean | publisher p99 | receiver apply mean | receiver p99 | receiver mean vs C1 |
|---|---|---|---|---|---|
| C1 exact | 102.6 (102.1..106.3) | 118.5 (117.0..123.2) | 92.9 (91.7..98.4) | 113.5 (112.3..119.6) | 1.00x |
| C6a EF-coded exact | 109.2 (108.7..110.2) | 123.0 (121.4..125.9) | 896.7 (895.4..899.2) | 939.1 (927.3..946.4) | 9.65x |
| C6b suffix w=32 | 106.8 (105.9..109.9) | 123.2 (118.8..126.5) | 150.9 (150.3..155.1) | 236.7 (229.5..248.7) | 1.62x |
| C3 cuckoo f=16 | 99.3 (97.3..99.7) | 117.4 (112.5..118.2) | 74.6 (72.9..74.8) | 99.1 (98.6..101.2) | 0.80x |
| C4 quotient eps=2^-14 | 97.9 (96.4..99.2) | 116.3 (110.9..119.9) | 94.2 (91.7..94.9) | 132.9 (129.6..134.7) | 1.01x |
| C5 fuse16+overlay | 95.6 (95.3..98.0) | 116.0 (113.6..117.7) | 19.8 (19.7..20.0) | 28.0 (27.1..29.8) | 0.21x |
| Bloom (10 b/key, k=7) | 106.5 (105.4..111.3) | 127.2 (121.8..131.1) | 11.6 (11.1..12.0) | 22.2 (20.6..24.0) | 0.12x |
| C8a cuckoo over disk exact | 111.2 (109.5..112.5) | 140.9 (138.7..151.0) | 31364.2 (30924.0..31729.6) | 175.3 (171.2..178.0) | 337.52x |

#### n = 8,000,000, churn 10% per minute (per 250 ms tick, microseconds; median (min..max) over runs)

| candidate | publisher absorb mean | publisher p99 | receiver apply mean | receiver p99 | receiver mean vs C1 |
|---|---|---|---|---|---|
| C1 exact | 956.7 (936.8..984.5) | 1160.5 (1126.1..1198.0) | 897.4 (883.2..908.5) | 982.9 (971.4..993.8) | 1.00x |
| C6a EF-coded exact | 998.8 (989.5..1031.9) | 1217.7 (1171.2..1240.4) | 9039.2 (9017.2..9073.8) | 9196.9 (9166.6..9223.7) | 10.07x |
| C6b suffix w=32 | 913.3 (905.8..919.4) | 1185.1 (1165.3..1203.4) | 1181.4 (1166.7..1232.2) | 1741.0 (1691.2..1779.7) | 1.32x |
| C3 cuckoo f=16 | 899.1 (895.1..901.8) | 1144.8 (1101.0..1153.1) | 692.4 (689.5..699.9) | 860.9 (810.3..883.6) | 0.77x |
| C4 quotient eps=2^-14 | 906.1 (897.6..913.2) | 1098.7 (976.1..1148.7) | 750.0 (748.0..756.5) | 1086.2 (1067.5..1111.9) | 0.84x |
| C5 fuse16+overlay | 870.7 (868.4..882.0) | 1101.1 (1073.1..1114.9) | 146.9 (146.0..150.9) | 161.4 (159.0..195.6) | 0.16x |
| Bloom (10 b/key, k=7) | 913.6 (902.9..926.3) | 1313.2 (1298.9..1338.1) | 145.9 (144.2..146.9) | 178.6 (174.6..187.8) | 0.16x |
| C8a cuckoo over disk exact | 1153.8 (1150.0..1181.9) | 1607.9 (1595.8..1622.5) | 421161.1 (420648.7..421781.5) | 5196741.4 (5195760.3..5204307.9) | 469.33x |

- The **publisher side is identical for every receiver-side candidate**
  within noise (C3-C6 variants are within 0.9-1.1x of C1; the spread across
  repetitions is up to 7 %): the ticks cost 7 us at 1 Mi keys / 1 %
  per minute, 103 us at 8 Mi / 1 %, 0.96 ms at 8 Mi / 10 % (0.4 % of a tick).
- **Receiver apply**: C3 0.63-0.80x of C1, C5 0.16-0.57x, Bloom 0.12-0.35x
  (it ignores removals), C4 1.0-1.3x, C6b 1.04-1.62x, **C6a 9.4-17.5x**
  (decode and re-encode of an Elias-Fano shard per key), **C8a 47-470x**
  (the disk overlay merge rewrites the run file inline: p99 5.2 s at 8 Mi keys
  and 10 % churn, longer than the tick; a real implementation would merge in
  the background, which this one does not).
- **C5 snapshot cost** (owner side, measured in `raw/steady.md`, correctness
  box): building one 4096-key `BinaryFuse16` takes 0.07-0.10 ms; at 1 % churn
  and `K = 64` that is 245 snapshots per two minutes at 1 Mi keys and 1 953 at
  8 Mi keys, i.e. about 41 us and 0.33 ms per tick on average (derived: count x
  build time / 480 ticks), 6x and 3x the `absorb` cost, still < 0.2 % of a tick.
- Publisher memory is unchanged: 11.33 MiB for 1 Mi keys (the `KeySet`).
  The delta log for replay (`raw/steady.md`, last column) holds 28 KB/minute
  at 0.1 % churn to 6.4 MB/minute at 10 % churn on 8 Mi keys (64 MB for a ten
  minute log at the top of the grid).

## 6 Steady-state wire bytes

Metric: bytes per peer-minute that one receiver pulls for one owner:
`CacheSetDelta` frames plus the two 30 s `CacheSummary` heartbeats (real
`wire_len`, signed envelope included, gossip fan-out and QUIC framing not).
Source: `raw/steady.md`.

| n | churn/min | changes per tick | delta wire B/peer-min (C1, C3, C4, C5, C6a, C6b, C8a: identical) | summary wire B/peer-min | Bloom delta wire | Bloom snapshot wire | C5 snapshot wire (resnap=64) |
|---|---|---|---|---|---|---|---|
| 1,000,000 | 0.1% | 4.2 | 42,491 | 248 | 40,944 | 2,529,234 | 0 |
| 1,000,000 | 1% | 41.7 | 122,932 | 248 | 185,280 | 2,529,318 | 1,294,288 |
| 1,000,000 | 10% | 416.7 | 877,311 | 248 | 1,613,696 | 2,526,797 | 15,593,578 |
| 8,000,000 | 0.1% | 33.3 | 105,168 | 250 | 153,440 | 8,448,256 | 0 |
| 8,000,000 | 1% | 333.3 | 715,014 | 250 | 1,294,624 | 8,448,256 | 10,312,910 |
| 8,000,000 | 10% | 3333.3 | 6,427,911 | 250 | 12,835,456 | 8,448,256 | 125,283,177 |

Receiver bytes per key at start and after the two measured minutes of churn (allocator-live, publisher state excluded):

| candidate | n | churn/min | B/key at start | B/key after | growth |
|---|---|---|---|---|---|
| C1 exact | 1,000,000 | 1% | 11.88 | 12.09 | +2% |
| C6a EF-coded exact | 1,000,000 | 1% | 6.54 | 6.55 | +0% |
| C6b suffix w=32 | 1,000,000 | 1% | 4.79 | 6.26 | +31% |
| C3 cuckoo f=16 | 1,000,000 | 1% | 2.66 | 2.67 | +0% |
| C4 quotient eps=2^-14 | 1,000,000 | 1% | 3.64 | 3.72 | +2% |
| C5 fuse16+overlay | 1,000,000 | 1% | 2.70 | 2.90 | +7% |
| Bloom (10 b/key, k=7) | 1,000,000 | 1% | 1.26 | 1.26 | +0% |
| C8a cuckoo over disk exact | 1,000,000 | 1% | 2.73 | 2.79 | +2% |
| C1 exact | 1,000,000 | 10% | 11.88 | 12.82 | +8% |
| C6a EF-coded exact | 1,000,000 | 10% | 6.54 | 6.54 | +0% |
| C6b suffix w=32 | 1,000,000 | 10% | 4.79 | 7.84 | +64% |
| C3 cuckoo f=16 | 1,000,000 | 10% | 2.66 | 2.67 | +0% |
| C4 quotient eps=2^-14 | 1,000,000 | 10% | 3.64 | 3.90 | +7% |
| C5 fuse16+overlay | 1,000,000 | 10% | 2.70 | 2.96 | +10% |
| Bloom (10 b/key, k=7) | 1,000,000 | 10% | 1.26 | 1.26 | +0% |
| C8a cuckoo over disk exact | 1,000,000 | 10% | 2.73 | 2.77 | +1% |
| C1 exact | 8,000,000 | 1% | 11.11 | 11.32 | +2% |
| C6a EF-coded exact | 8,000,000 | 1% | 5.77 | 5.77 | +0% |
| C6b suffix w=32 | 8,000,000 | 1% | 4.39 | 6.44 | +47% |
| C3 cuckoo f=16 | 8,000,000 | 1% | 2.66 | 2.66 | +0% |
| C4 quotient eps=2^-14 | 8,000,000 | 1% | 3.63 | 3.70 | +2% |
| C5 fuse16+overlay | 8,000,000 | 1% | 2.70 | 2.89 | +7% |
| Bloom (10 b/key, k=7) | 8,000,000 | 1% | 0.53 | 0.53 | +0% |
| C8a cuckoo over disk exact | 8,000,000 | 1% | 2.73 | 2.80 | +3% |
| C1 exact | 8,000,000 | 10% | 11.11 | 12.06 | +9% |
| C6a EF-coded exact | 8,000,000 | 10% | 5.77 | 5.77 | +0% |
| C6b suffix w=32 | 8,000,000 | 10% | 4.39 | 7.74 | +76% |
| C3 cuckoo f=16 | 8,000,000 | 10% | 2.66 | 2.66 | +0% |
| C4 quotient eps=2^-14 | 8,000,000 | 10% | 3.63 | 3.89 | +7% |
| C5 fuse16+overlay | 8,000,000 | 10% | 2.70 | 2.96 | +10% |
| Bloom (10 b/key, k=7) | 8,000,000 | 10% | 0.53 | 0.53 | +0% |
| C8a cuckoo over disk exact | 8,000,000 | 10% | 2.73 | 2.80 | +3% |

C5 owner re-snapshot sweep (1% churn/min; snapshot wire per peer-minute):

| n | resnap after K changes to a bucket | snapshots in 2 min | snapshot wire B/peer-min | overlay entries at end | vs delta wire |
|---|---|---|---|---|---|
| 1,000,000 | 64 | 245 | 1,294,288 | 4291 | 10.5x |
| 1,000,000 | 16 | 1124 | 5,941,354 | 1909 | 48.3x |
| 1,000,000 | 256 | 0 | 0 | 19893 | 0.0x |
| 1,000,000 | 1024 | 0 | 0 | 19893 | 0.0x |
| 8,000,000 | 64 | 1953 | 10,312,910 | 34796 | 14.4x |
| 8,000,000 | 16 | 8990 | 47,475,411 | 15439 | 66.4x |
| 8,000,000 | 256 | 0 | 0 | 159211 | 0.0x |
| 8,000,000 | 1024 | 0 | 0 | 159211 | 0.0x |

- **Deltas and summaries are byte-identical for C1, C3, C4, C5, C6a, C6b and
  C8a** (the candidates change only what the receiver stores): the C6b/C3
  claim "wire unchanged" holds by construction and by measurement.
- **Bloom** is not cheaper in steady state: its snapshots cost 2.53 MB per
  peer-minute at 1 Mi keys and 8.45 MB at 8 Mi keys regardless of churn, and
  its add-only deltas carry 32-byte hashes, so at 1 % churn it uses 1.5x C1's
  delta bytes (185 KB vs 123 KB per peer-minute at 1 Mi keys).
- **C5-b confirmed**: re-snapshotting a bucket after 64 changes costs 10.5x
  (1 Mi keys) and 14.4x (8 Mi keys) the delta wire at 1 % churn; `K = 16` costs
  48x and 66x; with `K >= 256` nothing is shipped in the two measured minutes
  and the overlay grows (19.9 k entries at 1 Mi keys, 159 k at 8 Mi keys,
  8 B each). At 0.1 % churn nothing needed shipping in two minutes either; the
  steady cost there is the overlay, not the wire.
- Received per node (derived): `63 x` the per-peer figure; at 8 Mi keys and
  1 % churn that is 63 x 715 KB = 45 MB per minute (0.75 MB/s) for C1 and every
  receiver-side candidate. The gossip duplication factor was not measured.

## 7 Repair after a gap, a restart and initial sync

Metric: wire bytes, round trips and CPU for one peer's mirror to catch up with
its owner. Scenarios on a peer of `n` keys: `gap<N>` = `N` changes (half adds,
half removes) published in ticks of at most 14 k changes that the mirror
missed; `burst100k` = one tick of 100 k changes (the silent publish path, no
deltas exist); `restart` = a new incarnation whose set differs by 2 % of `n`
in each direction (160 k differences at 8 Mi keys); `initial` = an empty
mirror. A session is capped at 256 rounds (`MAX_ROUNDS`), as in the product;
the driver repeats sessions up to 200 times (the `real` driver up to 50) and
reports FAILED if the mirror does not reach the owner's root. Sources:
`raw/repair.md` (bytes, rounds; deterministic, correctness box) and
`raw/t-repair.md` (CPU; timing box, 4 interleaved repetitions).

#### RBSR (generic initiator, real owner), n = 1,000,000 keys per peer: result, sessions, rounds, wire bytes

| scenario | C1 | C6a | C6b w=32 | C3 f=16 | C4 | C5 | C8a |
|---|---|---|---|---|---|---|---|
| gap1 | 6.5 KiB / 3 rounds / 1 sess | 6.5 KiB / 3 rounds / 1 sess | 6.3 KiB / 2 rounds / 1 sess | 7.5 KiB / 2 rounds / 1 sess | 7.5 KiB / 2 rounds / 1 sess | 31.2 KiB / 1 rounds / 1 sess | 6.5 KiB / 3 rounds / 1 sess |
| gap1k | 547.4 KiB / 13 rounds / 1 sess | 547.4 KiB / 13 rounds / 1 sess | 578.5 KiB / 11 rounds / 1 sess | 1.73 MiB / 33 rounds / 1 sess | 1.73 MiB / 33 rounds / 1 sess | 7.01 MiB / 250 rounds / 1 sess | 547.4 KiB / 13 rounds / 1 sess |
| gap100k | 8.20 MiB / 164 rounds / 1 sess | 8.20 MiB / 164 rounds / 1 sess | 8.05 MiB / 147 rounds / 1 sess | 7.98 MiB / 144 rounds / 1 sess | 7.98 MiB / 144 rounds / 1 sess | 7.18 MiB / 256 rounds / 1 sess | 8.20 MiB / 164 rounds / 1 sess |
| burst100k | 8.20 MiB / 164 rounds / 1 sess | 8.20 MiB / 164 rounds / 1 sess | 8.05 MiB / 147 rounds / 1 sess | 7.98 MiB / 144 rounds / 1 sess | 7.98 MiB / 144 rounds / 1 sess | 7.18 MiB / 256 rounds / 1 sess | 8.20 MiB / 164 rounds / 1 sess |
| restart | 3.81 MiB / 81 rounds / 1 sess | 3.81 MiB / 81 rounds / 1 sess | 5.69 MiB / 104 rounds / 1 sess | 7.92 MiB / 144 rounds / 1 sess | 7.92 MiB / 144 rounds / 1 sess | 7.18 MiB / 256 rounds / 1 sess | 3.81 MiB / 81 rounds / 1 sess |
| initial | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess | 7.16 MiB / 258 rounds / 2 sess |

#### Other repair methods, n = 1,000,000: result / sessions / rounds / wire bytes

| scenario | C1 real session (`sync_in_memory` x50) | C1 RIBLT | C6a RIBLT | C1 delta-log replay | C3 / C4 / C6b / C5 replay |
|---|---|---|---|---|---|
| gap1 | 5.9 KiB / 3 rounds / 1 sess | 649 B / 1 rounds / 1 sess | 649 B / 1 rounds / 1 sess | 249 B / 1 rounds / 1 sess | 249 B / 249 B / 249 B / 249 B |
| gap1k | 544.8 KiB / 13 rounds / 1 sess | 27.2 KiB / 6 rounds / 1 sess | 27.2 KiB / 6 rounds / 1 sess | 8.2 KiB / 1 rounds / 1 sess | 8.2 KiB / 8.2 KiB / 8.2 KiB / 8.2 KiB |
| gap100k | 8.16 MiB / 164 rounds / 1 sess | 1.66 MiB / 49 rounds / 1 sess | 1.66 MiB / 49 rounds / 1 sess | 766.1 KiB / 16 rounds / 1 sess | 766.1 KiB / 766.1 KiB / 766.1 KiB / 766.1 KiB |
| burst100k | 8.16 MiB / 164 rounds / 1 sess | 1.66 MiB / 49 rounds / 1 sess | 1.66 MiB / 49 rounds / 1 sess | n/a | n/a / n/a / n/a / n/a |
| restart | 3.80 MiB / 81 rounds / 1 sess | 367.1 KiB / 15 rounds / 1 sess | 367.1 KiB / 15 rounds / 1 sess | n/a | n/a / n/a / n/a / n/a |
| initial | n/a | 16.86 MiB / 446 rounds / 1 sess | 16.86 MiB / 446 rounds / 1 sess | n/a | n/a / n/a / n/a / n/a |

#### RBSR (generic initiator, real owner), n = 8,000,000 keys per peer: result, sessions, rounds, wire bytes

| scenario | C1 | C6a | C6b w=32 | C3 f=16 | C4 | C5 | C8a |
|---|---|---|---|---|---|---|---|
| gap1 | 7.2 KiB / 4 rounds / 1 sess | 7.2 KiB / 4 rounds / 1 sess | 7.4 KiB / 3 rounds / 1 sess | 7.7 KiB / 2 rounds / 1 sess | 7.7 KiB / 2 rounds / 1 sess | 66.5 KiB / 4 rounds / 1 sess | 7.2 KiB / 4 rounds / 1 sess |
| gap1k | 856.4 KiB / 21 rounds / 1 sess | 856.4 KiB / 21 rounds / 1 sess | 1.25 MiB / 26 rounds / 1 sess | 1.83 MiB / 34 rounds / 1 sess | 1.83 MiB / 34 rounds / 1 sess | 21.71 MiB / 400 rounds / 2 sess | 856.4 KiB / 21 rounds / 1 sess |
| gap10k | 5.48 MiB / 136 rounds / 1 sess | n/a | 9.64 MiB / 182 rounds / 1 sess | 15.41 MiB / 275 rounds / 2 sess | 15.41 MiB / 275 rounds / 2 sess | n/a | n/a |
| gap20k | 9.18 MiB / 229 rounds / 1 sess | n/a | 17.20 MiB / 319 rounds / 2 sess | 26.70 MiB / 474 rounds / 2 sess | 26.70 MiB / 474 rounds / 2 sess | n/a | n/a |
| gap30k | 24.69 MiB / 611 rounds / 3 sess | n/a | 23.61 MiB / 435 rounds / 2 sess | 34.97 MiB / 621 rounds / 3 sess | 34.97 MiB / 621 rounds / 3 sess | n/a | n/a |
| gap50k | **FAILED** (200 sessions, 2.04 GiB) | n/a | 33.84 MiB / 620 rounds / 3 sess | 45.51 MiB / 809 rounds / 4 sess | 45.51 MiB / 809 rounds / 4 sess | n/a | n/a |
| gap75k | **FAILED** (200 sessions, 2.03 GiB) | n/a | 42.84 MiB / 784 rounds / 4 sess | 52.20 MiB / 927 rounds / 4 sess | 52.20 MiB / 927 rounds / 4 sess | n/a | n/a |
| gap100k | **FAILED** (200 sessions, 2.03 GiB) | **FAILED** (200 sessions, 2.03 GiB) | 49.06 MiB / 896 rounds / 4 sess | 55.35 MiB / 983 rounds / 4 sess | 55.35 MiB / 983 rounds / 4 sess | 56.36 MiB / 1038 rounds / 5 sess | **FAILED** (200 sessions, 2.03 GiB) |
| burst100k | **FAILED** (200 sessions, 2.03 GiB) | **FAILED** (200 sessions, 2.03 GiB) | 49.06 MiB / 896 rounds / 4 sess | 55.36 MiB / 983 rounds / 4 sess | 55.36 MiB / 983 rounds / 4 sess | 56.36 MiB / 1038 rounds / 5 sess | **FAILED** (200 sessions, 2.03 GiB) |
| restart | **FAILED** (200 sessions, 2.03 GiB) | **FAILED** (200 sessions, 2.03 GiB) | 56.99 MiB / 1041 rounds / 5 sess | 57.69 MiB / 1024 rounds / 4 sess | 57.69 MiB / 1024 rounds / 4 sess | 56.35 MiB / 1039 rounds / 5 sess | **FAILED** (200 sessions, 2.03 GiB) |
| initial | 56.68 MiB / 1037 rounds / 5 sess | 56.68 MiB / 1037 rounds / 5 sess | 56.68 MiB / 1037 rounds / 5 sess | 57.63 MiB / 1034 rounds / 5 sess | 57.63 MiB / 1034 rounds / 5 sess | 55.94 MiB / 1043 rounds / 5 sess | 56.68 MiB / 1037 rounds / 5 sess |

#### Other repair methods, n = 8,000,000: result / sessions / rounds / wire bytes

| scenario | C1 real session (`sync_in_memory` x50) | C1 RIBLT | C6a RIBLT | C1 delta-log replay | C3 / C4 / C6b / C5 replay |
|---|---|---|---|---|---|
| gap1 | 6.4 KiB / 4 rounds / 1 sess | 657 B / 1 rounds / 1 sess | 657 B / 1 rounds / 1 sess | 249 B / 1 rounds / 1 sess | 249 B / 249 B / 249 B / 249 B |
| gap1k | 852.2 KiB / 21 rounds / 1 sess | 28.1 KiB / 6 rounds / 1 sess | 28.1 KiB / 6 rounds / 1 sess | 8.2 KiB / 1 rounds / 1 sess | 8.2 KiB / 8.2 KiB / 8.2 KiB / 8.2 KiB |
| gap10k | 5.45 MiB / 136 rounds / 1 sess | 213.4 KiB / 11 rounds / 1 sess | n/a | 77.5 KiB / 2 rounds / 1 sess | 77.5 KiB / 77.5 KiB / 77.5 KiB / n/a |
| gap20k | 9.13 MiB / 229 rounds / 1 sess | 370.4 KiB / 15 rounds / 1 sess | n/a | 154.9 KiB / 4 rounds / 1 sess | 154.9 KiB / 154.9 KiB / 154.9 KiB / n/a |
| gap30k | 24.57 MiB / 611 rounds / 3 sess | 527.3 KiB / 19 rounds / 1 sess | n/a | 232.3 KiB / 5 rounds / 1 sess | 232.3 KiB / 232.3 KiB / 232.3 KiB / n/a |
| gap50k | **FAILED** (50 sess, 518.54 MiB) | 880.2 KiB / 28 rounds / 1 sess | n/a | 385.9 KiB / 9 rounds / 1 sess | 385.9 KiB / 385.9 KiB / 385.9 KiB / n/a |
| gap75k | **FAILED** (50 sess, 517.87 MiB) | 1.28 MiB / 39 rounds / 1 sess | n/a | 578.8 KiB / 13 rounds / 1 sess | 578.8 KiB / 578.8 KiB / 578.8 KiB / n/a |
| gap100k | **FAILED** (50 sess, 517.41 MiB) | 1.70 MiB / 50 rounds / 1 sess | 1.70 MiB / 50 rounds / 1 sess | 768.5 KiB / 17 rounds / 1 sess | 768.5 KiB / 768.5 KiB / 768.5 KiB / 768.5 KiB |
| burst100k | **FAILED** (50 sess, 517.41 MiB) | 1.70 MiB / 50 rounds / 1 sess | 1.70 MiB / 50 rounds / 1 sess | n/a | n/a / n/a / n/a / n/a |
| restart | **FAILED** (50 sess, 516.82 MiB) | 2.70 MiB / 76 rounds / 1 sess | 2.70 MiB / 76 rounds / 1 sess | n/a | n/a / n/a / n/a / n/a |
| initial | n/a | n/a | n/a | n/a | n/a / n/a / n/a / n/a |

### CPU of the repairs (timing box)

#### n = 1,000,000, scenario gap1 (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 3 | 6.5 KiB | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C1 exact | real | ok | 3 | 5.9 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C1 exact | riblt | ok | 1 | 649 B | 95.1 (95.0..95.5) | 94.9 (94.4..95.3) |
| C1 exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C6a EF-coded exact | rbsr | ok | 3 | 6.5 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C6a EF-coded exact | riblt | ok | 1 | 649 B | 95.1 (94.6..95.3) | 106.8 (105.7..107.6) |
| C6a EF-coded exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C6b suffix w=32 | rbsr | ok | 2 | 6.3 KiB | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C6b suffix w=32 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C3 cuckoo f=16 | rbsr | ok | 2 | 7.5 KiB | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C3 cuckoo f=16 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C4 quotient eps=2^-14 | rbsr | ok | 2 | 7.5 KiB | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C4 quotient eps=2^-14 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C5 fuse16+overlay | rbsr | ok | 1 | 31.2 KiB | 0.1 (0.1..0.1) | 0.2 (0.2..0.2) |
| C5 fuse16+overlay | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C8a cuckoo over disk exact | rbsr | ok | 3 | 6.5 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C8a cuckoo over disk exact | riblt | ok | 1 | 649 B | 95.3 (94.8..95.4) | 97.2 (96.8..98.1) |
| C8a cuckoo over disk exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |

#### n = 1,000,000, scenario gap100k (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 164 | 8.20 MiB | 19.0 (18.8..19.2) | 43.6 (43.4..43.7) |
| C1 exact | real | ok | 164 | 8.16 MiB | 0.0 (0.0..0.0) | 64.2 (63.6..64.7) |
| C1 exact | riblt | ok | 49 | 1.66 MiB | 349.7 (348.9..355.7) | 400.1 (399.1..403.8) |
| C1 exact | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 4.2 (4.2..4.2) |
| C6a EF-coded exact | rbsr | ok | 164 | 8.20 MiB | 18.8 (18.4..19.5) | 202.6 (201.9..203.1) |
| C6a EF-coded exact | riblt | ok | 49 | 1.66 MiB | 350.0 (349.1..351.0) | 538.7 (534.1..540.0) |
| C6a EF-coded exact | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 130.9 (130.7..131.2) |
| C6b suffix w=32 | rbsr | ok | 147 | 8.05 MiB | 6.8 (6.2..7.4) | 19.6 (19.6..19.8) |
| C6b suffix w=32 | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 6.3 (6.3..6.6) |
| C3 cuckoo f=16 | rbsr | ok | 144 | 7.98 MiB | 7.0 (6.9..7.3) | 29.9 (29.5..30.0) |
| C3 cuckoo f=16 | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 3.9 (3.9..3.9) |
| C4 quotient eps=2^-14 | rbsr | ok | 144 | 7.98 MiB | 7.3 (6.7..7.6) | 36.6 (36.4..36.9) |
| C4 quotient eps=2^-14 | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 7.8 (7.8..7.9) |
| C5 fuse16+overlay | rbsr | ok | 256 | 7.18 MiB | 10.3 (9.5..10.7) | 28.2 (27.8..28.6) |
| C5 fuse16+overlay | replay | ok | 16 | 766.1 KiB | 0.0 (0.0..0.0) | 3.2 (3.1..3.3) |
| C8a cuckoo over disk exact | rbsr | ok | 164 | 8.20 MiB | 19.8 (19.6..19.9) | 2717.6 (2709.6..2718.3) |
| C8a cuckoo over disk exact | riblt | ok | 49 | 1.66 MiB | 350.0 (349.0..351.2) | 2579.5 (2578.3..2616.5) |
| C8a cuckoo over disk exact | replay | ok | 16 | 766.1 KiB | 0.1 (0.0..0.1) | 3872.8 (3856.3..3904.3) |

#### n = 1,000,000, scenario gap1k (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 13 | 547.4 KiB | 2.5 (2.5..2.6) | 3.3 (3.3..3.4) |
| C1 exact | real | ok | 13 | 544.8 KiB | 0.0 (0.0..0.0) | 6.1 (6.0..6.2) |
| C1 exact | riblt | ok | 6 | 27.2 KiB | 220.9 (220.5..221.3) | 220.9 (220.7..221.7) |
| C1 exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C6a EF-coded exact | rbsr | ok | 13 | 547.4 KiB | 2.4 (2.3..2.6) | 16.6 (16.5..16.8) |
| C6a EF-coded exact | riblt | ok | 6 | 27.2 KiB | 220.9 (220.2..221.6) | 235.4 (234.6..235.6) |
| C6a EF-coded exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 1.4 (1.4..1.4) |
| C6b suffix w=32 | rbsr | ok | 11 | 578.5 KiB | 1.1 (1.0..1.2) | 1.7 (1.7..1.7) |
| C6b suffix w=32 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C3 cuckoo f=16 | rbsr | ok | 33 | 1.73 MiB | 1.9 (1.8..2.1) | 6.5 (6.5..6.6) |
| C3 cuckoo f=16 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C4 quotient eps=2^-14 | rbsr | ok | 33 | 1.73 MiB | 1.9 (1.8..2.1) | 7.9 (7.9..8.1) |
| C4 quotient eps=2^-14 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C5 fuse16+overlay | rbsr | ok | 250 | 7.01 MiB | 10.2 (9.6..10.5) | 27.1 (26.8..28.0) |
| C5 fuse16+overlay | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C8a cuckoo over disk exact | rbsr | ok | 13 | 547.4 KiB | 2.4 (2.3..2.4) | 18.4 (18.3..18.7) |
| C8a cuckoo over disk exact | riblt | ok | 6 | 27.2 KiB | 221.1 (220.5..222.0) | 223.2 (222.9..224.6) |
| C8a cuckoo over disk exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.2 (0.2..0.2) |

#### n = 1,000,000, scenario initial (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 258 | 7.16 MiB | 10.9 (10.6..10.9) | 28.1 (27.8..28.8) |
| C1 exact | riblt | ok | 446 | 16.86 MiB | 642.8 (641.3..643.7) | 609.2 (586.6..613.8) |
| C6a EF-coded exact | rbsr | ok | 258 | 7.16 MiB | 10.4 (9.9..10.9) | 45.7 (45.0..46.0) |
| C6a EF-coded exact | riblt | ok | 446 | 16.86 MiB | 642.2 (641.2..644.1) | 1979.1 (1975.7..1990.0) |
| C6b suffix w=32 | rbsr | ok | 258 | 7.16 MiB | 10.4 (10.0..10.9) | 13.8 (13.4..14.2) |
| C3 cuckoo f=16 | rbsr | ok | 258 | 7.16 MiB | 10.4 (10.3..10.6) | 25.6 (25.1..26.0) |
| C4 quotient eps=2^-14 | rbsr | ok | 258 | 7.16 MiB | 10.3 (10.1..10.7) | 32.2 (31.7..32.5) |
| C5 fuse16+overlay | rbsr | ok | 258 | 7.16 MiB | 10.3 (9.9..10.8) | 27.9 (27.6..28.4) |
| C8a cuckoo over disk exact | rbsr | ok | 258 | 7.16 MiB | 11.2 (11.0..11.3) | 15868.5 (15847.0..15930.5) |
| C8a cuckoo over disk exact | riblt | ok | 446 | 16.86 MiB | 642.0 (637.7..644.2) | 19997.0 (19941.2..20044.0) |

#### n = 1,000,000, scenario restart (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 81 | 3.81 MiB | 12.4 (12.4..12.5) | 21.9 (21.6..22.2) |
| C1 exact | real | ok | 81 | 3.80 MiB | 0.0 (0.0..0.0) | 35.4 (35.0..36.0) |
| C1 exact | riblt | ok | 15 | 367.1 KiB | 298.1 (297.4..298.8) | 308.4 (307.0..308.5) |
| C6a EF-coded exact | rbsr | ok | 81 | 3.81 MiB | 12.2 (11.6..12.8) | 109.0 (108.3..109.6) |
| C6a EF-coded exact | riblt | ok | 15 | 367.1 KiB | 298.1 (297.8..298.5) | 347.4 (346.9..347.6) |
| C6b suffix w=32 | rbsr | ok | 104 | 5.69 MiB | 6.0 (5.2..6.5) | 14.1 (14.1..14.1) |
| C3 cuckoo f=16 | rbsr | ok | 144 | 7.92 MiB | 7.2 (6.8..8.0) | 29.6 (29.2..30.0) |
| C4 quotient eps=2^-14 | rbsr | ok | 144 | 7.92 MiB | 7.0 (6.7..7.4) | 36.2 (36.0..36.2) |
| C5 fuse16+overlay | rbsr | ok | 256 | 7.18 MiB | 10.3 (9.9..10.7) | 27.7 (27.6..28.4) |
| C8a cuckoo over disk exact | rbsr | ok | 81 | 3.81 MiB | 12.4 (12.1..12.7) | 604.8 (591.0..614.2) |
| C8a cuckoo over disk exact | riblt | ok | 15 | 367.1 KiB | 298.4 (298.0..298.9) | 429.2 (411.1..431.7) |

#### n = 8,000,000, scenario gap1 (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 4 | 7.2 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C1 exact | real | ok | 4 | 6.4 KiB | 0.0 (0.0..0.0) | 0.2 (0.2..0.2) |
| C1 exact | riblt | ok | 1 | 657 B | 758.1 (754.0..758.6) | 775.7 (771.3..779.9) |
| C1 exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C6a EF-coded exact | rbsr | ok | 4 | 7.2 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C6a EF-coded exact | riblt | ok | 1 | 657 B | 756.5 (753.6..757.2) | 865.8 (862.4..869.0) |
| C6a EF-coded exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C6b suffix w=32 | rbsr | ok | 3 | 7.4 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C6b suffix w=32 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C3 cuckoo f=16 | rbsr | ok | 2 | 7.7 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C3 cuckoo f=16 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C4 quotient eps=2^-14 | rbsr | ok | 2 | 7.7 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C4 quotient eps=2^-14 | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C5 fuse16+overlay | rbsr | ok | 4 | 66.5 KiB | 0.2 (0.2..0.2) | 0.3 (0.3..0.4) |
| C5 fuse16+overlay | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |
| C8a cuckoo over disk exact | rbsr | ok | 4 | 7.2 KiB | 0.1 (0.1..0.1) | 0.1 (0.1..0.1) |
| C8a cuckoo over disk exact | riblt | ok | 1 | 657 B | 755.9 (754.2..757.3) | 789.7 (787.0..792.1) |
| C8a cuckoo over disk exact | replay | ok | 1 | 249 B | 0.0 (0.0..0.0) | 0.0 (0.0..0.0) |

#### n = 8,000,000, scenario gap100k (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | FAILED | 51200 | 2.03 GiB | 11078.4 (11033.2..11205.4) | 13799.3 (13757.6..13832.9) |
| C1 exact | real | FAILED | 12800 | 517.41 MiB | 0.0 (0.0..0.0) | 6174.2 (6134.7..6202.5) |
| C1 exact | riblt | ok | 50 | 1.70 MiB | 2784.3 (2783.2..2788.7) | 2875.6 (2874.8..2878.4) |
| C1 exact | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.2) | 24.0 (23.2..24.4) |
| C6a EF-coded exact | rbsr | FAILED | 51200 | 2.03 GiB | 11269.1 (11237.2..11365.9) | 110107.0 (109800.7..110473.8) |
| C6a EF-coded exact | riblt | ok | 50 | 1.70 MiB | 2786.8 (2784.0..2789.4) | 3205.6 (3190.2..3212.1) |
| C6a EF-coded exact | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.1) | 262.4 (258.7..262.7) |
| C6b suffix w=32 | rbsr | ok | 896 | 49.06 MiB | 61.5 (60.8..62.2) | 130.3 (129.6..131.1) |
| C6b suffix w=32 | replay | ok | 17 | 768.5 KiB | 0.1 (0.0..0.1) | 31.4 (30.3..33.8) |
| C3 cuckoo f=16 | rbsr | ok | 983 | 55.35 MiB | 53.4 (52.9..54.9) | 222.2 (221.0..223.3) |
| C3 cuckoo f=16 | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.1) | 13.9 (13.3..15.6) |
| C4 quotient eps=2^-14 | rbsr | ok | 983 | 55.35 MiB | 54.9 (53.4..56.5) | 273.1 (272.6..274.0) |
| C4 quotient eps=2^-14 | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.1) | 20.8 (20.0..22.0) |
| C5 fuse16+overlay | rbsr | ok | 1038 | 56.36 MiB | 58.1 (56.7..58.8) | 244.5 (243.8..245.1) |
| C5 fuse16+overlay | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.1) | 4.0 (3.9..4.0) |
| C8a cuckoo over disk exact | rbsr | FAILED | 51200 | 2.03 GiB | 11374.4 (11340.9..11502.8) | 81166.0 (80030.5..81444.0) |
| C8a cuckoo over disk exact | riblt | ok | 50 | 1.70 MiB | 2784.8 (2783.0..2790.2) | 12420.0 (12406.5..12495.5) |
| C8a cuckoo over disk exact | replay | ok | 17 | 768.5 KiB | 0.1 (0.1..0.1) | 10215.9 (10212.7..10291.9) |

#### n = 8,000,000, scenario gap1k (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | ok | 21 | 856.4 KiB | 4.6 (4.6..4.6) | 5.8 (5.8..5.9) |
| C1 exact | real | ok | 21 | 852.2 KiB | 0.0 (0.0..0.0) | 10.6 (10.6..10.7) |
| C1 exact | riblt | ok | 6 | 28.1 KiB | 1764.3 (1758.8..1765.5) | 1782.0 (1775.9..1785.5) |
| C1 exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.3 (0.3..0.3) |
| C6a EF-coded exact | rbsr | ok | 21 | 856.4 KiB | 4.7 (4.6..4.8) | 30.2 (30.1..30.4) |
| C6a EF-coded exact | riblt | ok | 6 | 28.1 KiB | 1762.1 (1756.1..1769.7) | 1872.2 (1862.9..1877.3) |
| C6a EF-coded exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 2.9 (2.9..2.9) |
| C6b suffix w=32 | rbsr | ok | 26 | 1.25 MiB | 3.1 (2.9..3.1) | 4.9 (4.8..5.0) |
| C6b suffix w=32 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.5 (0.4..0.5) |
| C3 cuckoo f=16 | rbsr | ok | 34 | 1.83 MiB | 3.1 (3.0..3.2) | 7.8 (7.7..7.8) |
| C3 cuckoo f=16 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.2 (0.2..0.2) |
| C4 quotient eps=2^-14 | rbsr | ok | 34 | 1.83 MiB | 3.2 (3.1..3.2) | 9.5 (9.4..9.5) |
| C4 quotient eps=2^-14 | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.3 (0.3..0.4) |
| C5 fuse16+overlay | rbsr | ok | 400 | 21.71 MiB | 22.7 (22.6..22.8) | 93.7 (92.9..94.5) |
| C5 fuse16+overlay | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.1 (0.1..0.1) |
| C8a cuckoo over disk exact | rbsr | ok | 21 | 856.4 KiB | 4.7 (4.5..4.8) | 38.0 (37.6..38.3) |
| C8a cuckoo over disk exact | riblt | ok | 6 | 28.1 KiB | 1762.4 (1756.4..1767.6) | 1795.5 (1782.1..1800.6) |
| C8a cuckoo over disk exact | replay | ok | 1 | 8.2 KiB | 0.0 (0.0..0.0) | 0.5 (0.5..0.5) |

#### n = 8,000,000, scenario restart (CPU ms, median (min..max) over runs)

| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |
|---|---|---|---|---|---|---|
| C1 exact | rbsr | FAILED | 51200 | 2.03 GiB | 10940.8 (10872.7..11069.7) | 13721.8 (13659.0..13925.2) |
| C1 exact | real | FAILED | 12800 | 516.82 MiB | 0.0 (0.0..0.0) | 6024.0 (6010.4..6057.8) |
| C1 exact | riblt | ok | 76 | 2.70 MiB | 2983.6 (2982.0..2984.6) | 3119.3 (3115.7..3123.3) |
| C6a EF-coded exact | rbsr | FAILED | 51200 | 2.03 GiB | 11116.5 (11072.1..11220.9) | 109631.1 (109435.7..110190.8) |
| C6a EF-coded exact | riblt | ok | 76 | 2.70 MiB | 2986.5 (2982.5..2991.6) | 3591.0 (3575.5..3593.9) |
| C6b suffix w=32 | rbsr | ok | 1041 | 56.99 MiB | 69.2 (67.7..69.8) | 150.1 (148.7..150.9) |
| C3 cuckoo f=16 | rbsr | ok | 1024 | 57.69 MiB | 55.6 (54.2..57.4) | 232.7 (231.6..233.5) |
| C4 quotient eps=2^-14 | rbsr | ok | 1024 | 57.69 MiB | 56.3 (55.1..58.4) | 284.3 (283.6..287.2) |
| C5 fuse16+overlay | rbsr | ok | 1039 | 56.35 MiB | 56.8 (55.5..58.6) | 242.8 (241.4..244.1) |
| C8a cuckoo over disk exact | rbsr | FAILED | 51200 | 2.03 GiB | 11200.8 (11188.0..11320.0) | 83020.2 (82725.2..83748.6) |
| C8a cuckoo over disk exact | riblt | ok | 76 | 2.70 MiB | 2983.8 (2980.8..2985.3) | 17806.7 (17768.2..17829.9) |

### 7.1 What the numbers say

- **The status quo does not converge on large gaps at large peers.** With
  the real session driver (`reconcile::sync_in_memory`, repeated 50 times) on
  8 Mi keys, a gap of 30 k changes takes 3 sessions and 24.6 MiB, but gaps of 50 k,
  75 k, 100 k, a 100 k burst and a restart (160 k differences) never converge:
  each session spends its 256 rounds on the breadth-first frontier (about 114
  `Children` answers fit in one 48 KiB reply and tens of thousands of level-16
  ranges mismatch, so level 20 and the first `Items` are never reached), applies nothing, and
  the next session repeats it exactly (517 MiB and 12 800 rounds spent in 50
  sessions; none of the added keys had arrived: 119 false negatives in the
  20 000-key sample, the sample's whole share of the 50 000 added keys). At 1 Mi keys the same
  gaps converge (164 rounds, 8.2 MiB for 100 k) because level-16 ranges are
  small enough to be answered as `Items` directly. Shard-granular mirrors do
  converge there only because the owner answers `Items` at the shard depth
  (the leaf-depth rule of section 1.3); the same rule, or a depth-first / size
  aware descent, would fix the status quo. This is a defect of the repair
  protocol independent of which digest is stored.
- **Initial sync of 1 Mi keys: 258 rounds, 2 sessions, 7.16 MiB** (C1-c
  predicted ~143 rounds, ~7 MB; the bytes match, the rounds do not because
  replies carry ~28 KiB rather than 48 KiB); 8 Mi keys: 1 037 rounds, 5
  sessions, 56.7 MiB.
- **Sparse gaps (C1-b)**: 3-4 rounds and 6.5-7.2 KiB for one difference
  (predicted 9.5 KiB + 1.1 KB per difference; measured 0.55 KB per difference at
  1 000 differences: 547 KiB at 1 Mi keys, 856 KiB at 8 Mi keys).
- **Approximate mirrors repair slightly differently**: for 1 000 differences
  at 8 Mi keys C6b needs 1.25 MiB (1.5x C1), C3 and C4 1.83 MiB (2.2x), C5 21.7 MiB
  (26x, 2 sessions); at 1 Mi keys C6b is 1.06x, C3/C4 3.2x, C5 13.1x (**C6b-c
  confirmed**: +0.9 KB per isolated difference was predicted, +0.03-0.4 KB
  measured because the leaf is one shard of ~61 keys; **C3-d** `7·n/2^b`
  per difference confirmed in shape). At 100 k differences they need
  8 MiB (1 Mi keys; C1 needs 8.2 MiB) or 49-56 MiB (8 Mi keys, 4-5 sessions) and
  succeed at 8 Mi keys where C1 does not.
- **RIBLT (C7b)**: 649-657 B for one difference (1 round) and 27-28 KiB for
  1 000 differences (6 rounds), **20x less than RBSR** at 1 Mi and 30x at 8 Mi
  (27 B per difference, predicted 18-22); 1.66 MiB for 100 k differences at 1 Mi
  keys (4.9x less than RBSR's 8.2 MiB) and 1.70 MiB at 8 Mi keys where RBSR
  fails; 367 KiB (10.6x less than RBSR's 3.81 MiB) at 1 Mi keys and 2.70 MiB at 8 Mi keys for a restart. It **loses on initial sync**:
  16.9 MiB at 1 Mi keys vs 7.2 MiB (2.4x, C7b-b confirmed), and it exists only
  for mirrors that can enumerate their keys (C1, C6a, C8a). Its CPU is the cost:
  95 ms owner plus 95 ms receiver for **one** difference at 1 Mi keys, 758 + 776 ms at
  8 Mi keys, 2.8 + 2.9 s for 100 k differences at 8 Mi keys, because both sides
  rebuild their symbol state from scratch (no cache) at about 95 ns per key per
  side; a design that maintains the coded symbols incrementally (the paper's
  suggestion) was not built.
- **Delta-log replay (C7d-a confirmed)**: wire equals the missed deltas
  exactly: 249 B (1 key), 8.2 KiB (1 k), 766-769 KiB (100 k in 7 ticks) in
  one request (16-17 reply frames for 100 k), **11x less than RBSR at 1 Mi keys
  and the only method that is cheap in both bytes and CPU** (0.3-24 ms receiver
  CPU for C1). It repairs every candidate identically (the table shows C3, C4,
  C5 and C6b at the same bytes), cannot repair a restart or a silent burst
  (no deltas exist), and costs owner memory: the log needed for ten minutes at
  1 % churn on 8 Mi keys is about 7 MB (700 KB/minute, section 6).
- **CPU of RBSR itself** (timing box): 19 ms owner + 44 ms receiver for 100 k
  differences at 1 Mi keys, 6.8 + 19.6 ms for C6b; C6a spends 203 ms on the
  receiver; C8a's disk-backed receiver spends 2.7 s (1 Mi keys) to 81 s (8 Mi
  keys, failing run) because every range probe reads blocks.

## 8 Staleness under loss and reordering

Metric: time from an owner's change until the mirror answers correctly, in
the in-process simulation (section 1.3), plus what a lookup gets wrong:
`stale-positive` = lookups of keys removed within the last 60 s that still hit
(the status quo's wasted round trips, "declines"); `missed` = held keys that
miss; `false-positive` = random absent keys that hit. Counts are wrong /
total lookups, each lookup against one peer. Source: `raw/sim.md`
(`run.sh sim`; 10 virtual minutes, 100 000 keys per peer).

| candidate | n | peers | churn/min | loss / reorder | staleness p50 / p99 / max ms | unresolved | sessions per peer-min | repair B/peer-min | stale-positive (removed <=60 s) | missed (held) | false-positive (absent) | final FN / unconverged peers |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Exact | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61,438 | 94/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21,230 | 56/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76,827 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175,776 | 269/384000 | 2/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212,401 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12,660 | 233/383680 | 1/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253,481 | 142/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Exact | 100,000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75,953 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 / 0 |
| Exact | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278,718 | 152/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61,438 | 94/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21,230 | 56/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76,827 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175,776 | 269/384000 | 2/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212,401 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12,660 | 233/383680 | 1/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253,481 | 142/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Ef | 100,000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75,953 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 / 0 |
| Ef | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278,718 | 152/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 0% / 5% | 133 / 518 / 828 | 0 | 7.90 | 341,574 | 94/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 1% / 0% | 128 / 354 / 824 | 0 | 2.73 | 117,791 | 56/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 1% / 5% | 138 / 534 / 1051 | 0 | 9.82 | 425,001 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 10% / 0% | 147 / 589 / 1316 | 0 | 22.04 | 955,810 | 269/384000 | 2/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 1% | 10% / 5% | 154 / 705 / 1329 | 0 | 26.52 | 1,151,250 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 0.1% | 1% / 5% | 137 / 834 / 1408 | 0 | 2.04 | 85,784 | 233/383680 | 1/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 10% | 1% / 5% | 136 / 532 / 829 | 28 | 9.94 | 562,861 | 142/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 64 | 1% | 1% / 5% | 137 / 532 / 1078 | 0 | 9.73 | 420,688 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 / 0 |
| Suffix(4, 64) | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 136 / 532 / 829 | 42 | 9.94 | 597,201 | 152/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 35/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 0% / 5% | 131 / 473 / 768 | 0 | 7.90 | 131,880 | 94/384000 | 0/384000 | 52/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 1% / 0% | 128 / 294 / 764 | 0 | 2.73 | 45,270 | 56/384000 | 0/384000 | 35/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 164,677 | 144/384000 | 0/384000 | 52/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 10% / 0% | 141 / 529 / 1256 | 0 | 22.04 | 379,364 | 269/384000 | 2/384000 | 37/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 459,216 | 343/384000 | 3/384000 | 43/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 815 / 1348 | 0 | 2.04 | 24,937 | 233/383680 | 1/384000 | 42/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 10% | 1% / 5% | 134 / 495 / 214833 | 62 | 9.96 | 687,293 | 175/384000 | 4/384000 | 40/384000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 64 | 1% | 1% / 5% | 134 / 481 / 1018 | 0 | 9.73 | 162,643 | 1253/3072000 | 4/3072000 | 349/3072000 | 0 / 0 |
| Cuckoo(16, 256) | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 137 / 497 / 137438 | 48 | 27.40 | 1,071,900 | 200/384000 | 4/384000 | 38/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 6/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 0% / 5% | 131 / 473 / 768 | 0 | 7.90 | 131,880 | 94/384000 | 0/384000 | 7/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 1% / 0% | 128 / 294 / 764 | 0 | 2.73 | 45,270 | 56/384000 | 0/384000 | 10/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 164,677 | 144/384000 | 0/384000 | 15/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 10% / 0% | 141 / 529 / 1256 | 0 | 22.04 | 379,364 | 269/384000 | 2/384000 | 10/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 459,216 | 343/384000 | 3/384000 | 6/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 815 / 1348 | 0 | 2.04 | 24,937 | 233/383680 | 1/384000 | 10/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 10% | 1% / 5% | 134 / 495 / 809 | 36 | 9.94 | 686,980 | 150/384000 | 4/384000 | 14/384000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 64 | 1% | 1% / 5% | 134 / 481 / 1018 | 0 | 9.73 | 162,643 | 1253/3072000 | 4/3072000 | 77/3072000 | 0 / 0 |
| Quotient(14, 256) | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 134 / 495 / 829 | 42 | 9.94 | 835,301 | 152/384000 | 4/384000 | 14/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 5/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 0% / 5% | 132 / 504 / 863 | 0 | 7.90 | 707,955 | 94/384000 | 0/384000 | 4/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 1% / 0% | 128 / 339 / 824 | 0 | 2.73 | 239,309 | 57/384000 | 0/384000 | 8/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 1% / 5% | 137 / 517 / 1111 | 0 | 9.82 | 890,753 | 144/384000 | 0/384000 | 6/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 10% / 0% | 146 / 609 / 1396 | 0 | 22.04 | 2,140,240 | 276/384000 | 2/384000 | 10/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 1% | 10% / 5% | 152 / 718 / 1409 | 0 | 26.52 | 2,607,752 | 353/384000 | 3/384000 | 10/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 46,619 | 233/383680 | 1/384000 | 6/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 10% | 1% / 5% | 138 / 711 / 1088 | 28 | 9.94 | 5,217,913 | 145/384000 | 4/384000 | 6/384000 | 0 / 0 |
| Fuse(4096) | 100,000 | 64 | 1% | 1% / 5% | 136 / 516 / 1113 | 0 | 9.73 | 881,057 | 1275/3072000 | 4/3072000 | 34/3072000 | 0 / 0 |
| Fuse(4096) | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 138 / 920 / 1404 | 42 | 9.94 | 6,478,735 | 153/384000 | 4/384000 | 3/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 0% / 0% | 244 / 29694 / 81875 | 8 | 0.00 | 0 | 112207/384000 | 0/384000 | 3133/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 0% / 5% | 610 / 29690 / 81797 | 9 | 0.00 | 0 | 111815/384000 | 11/384000 | 3211/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 1% / 0% | 248 / 29852 / 82513 | 9 | 0.00 | 0 | 112936/384000 | 9/384000 | 3168/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 1% / 5% | 899 / 40709 / 111813 | 10 | 0.00 | 0 | 114481/384000 | 20/384000 | 3160/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 10% / 0% | 2757 / 57323 / 238149 | 18 | 0.00 | 0 | 135757/384000 | 61/384000 | 3125/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 1% | 10% / 5% | 3261 / 58067 / 119144 | 26 | 0.00 | 0 | 128674/384000 | 63/384000 | 3241/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 0.1% | 1% / 5% | 768 / 33803 / 66476 | 8 | 0.00 | 0 | 113853/383680 | 2/384000 | 3121/384000 | 0 / 0 |
| Bloom | 100,000 | 8 | 10% | 1% / 5% | 1142 / 42160 / 287574 | 374 | 0.00 | 0 | 116137/384000 | 215/384000 | 3424/384000 | 0 / 0 |
| Bloom | 100,000 | 64 | 1% | 1% / 5% | 816 / 29964 / 109775 | 95 | 0.00 | 0 | 911212/3072000 | 161/3072000 | 25329/3072000 | 0 / 0 |
| Bloom | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 176 / 29891 / 299333 | 242 | 0.00 | 0 | 116082/384000 | 254/384000 | 3404/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 0% / 0% | 124 / 247 / 249 | 0 | 0.00 | 0 | 0/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 0% / 5% | 132 / 489 / 803 | 0 | 7.90 | 61,438 | 94/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 1% / 0% | 128 / 321 / 804 | 0 | 2.73 | 21,230 | 56/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 1% / 5% | 137 / 499 / 1011 | 0 | 9.82 | 76,827 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 10% / 0% | 144 / 560 / 1296 | 0 | 22.04 | 175,776 | 269/384000 | 2/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 1% | 10% / 5% | 149 / 674 / 1309 | 0 | 26.52 | 212,401 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 0.1% | 1% / 5% | 136 / 820 / 1368 | 0 | 2.04 | 12,660 | 233/383680 | 1/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 10% | 1% / 5% | 135 / 511 / 809 | 28 | 9.94 | 253,481 | 142/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 64 | 1% | 1% / 5% | 135 / 498 / 1053 | 0 | 9.73 | 75,953 | 1253/3072000 | 4/3072000 | 0/3072000 | 0 / 0 |
| Hybrid(16, 256) | 100,000 | 8 | 10%, +75% growth | 1% / 5% | 135 / 512 / 809 | 42 | 9.94 | 278,718 | 152/384000 | 4/384000 | 0/384000 | 0 / 0 |
| Exact+replay | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4,548 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Exact+replay | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12,767 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64)+replay | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4,548 | 144/384000 | 0/384000 | 0/384000 | 0 / 0 |
| Suffix(4, 64)+replay | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12,767 | 343/384000 | 3/384000 | 0/384000 | 0 / 0 |
| Cuckoo(16, 256)+replay | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4,548 | 144/384000 | 0/384000 | 48/384000 | 0 / 0 |
| Cuckoo(16, 256)+replay | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12,767 | 343/384000 | 3/384000 | 44/384000 | 0 / 0 |
| Quotient(14, 256)+replay | 100,000 | 8 | 1% | 1% / 5% | 136 / 480 / 991 | 0 | 9.82 | 4,548 | 144/384000 | 0/384000 | 15/384000 | 0 / 0 |
| Quotient(14, 256)+replay | 100,000 | 8 | 1% | 10% / 5% | 146 / 647 / 1269 | 0 | 26.52 | 12,767 | 343/384000 | 3/384000 | 6/384000 | 0 / 0 |

- **Staleness is the same for every candidate that repairs** (C1, C3, C4, C5,
  C6a, C6b, C8a): p50 124-154 ms, p99 247-920 ms across the grid; the
  differences between rows are loss and reordering, not the structure. It is
  dominated by the 250 ms publish tick, the 500 ms minimum between sessions
  and 20 ms per session round (the p99 at 1 %/5 % is 499 ms for C1, 480 ms for
  C3, 534 ms for C6b: within the noise of one 10-minute run).
- **The status quo's stale declines** (the number the §5 rule compares false
  positives against): 56-343 of 384 000 lookups of recently removed keys (1.5e-4
  to 8.9e-4 per lookup per peer) across loss 0-10 %; **3.75e-4** at 1 % loss and
  5 % reordering (144 / 384 000), the headline row, identical for every
  candidate because all repair on the same schedule.
- **Bloom** (the existing option): removals wait for the next 30 s snapshot:
  p50 0.18-3.3 s, **p99 29.7-58 s, max 66-300 s**; 29 % of lookups of recently
  removed keys still hit (112 207 / 384 000 at no loss) against 1.5e-4 for the
  exact mirror; and 0.8 % false positives on absent keys. This is why ADR-26
  demoted it, reproduced.
- **Growth** (10 % churn per minute, 75 % additions, +75 % set size over 10
  minutes): the cuckoo mirror had **1 493 grow-sessions** over 8 peers
  (shards that filled past ~95 % and were refetched as `Items`), repair bytes
  1.07 MB per peer-minute against 279 KB for C1 (3.8x), and 48 events whose
  removal never took effect until a refetch (a removed key whose fingerprint
  still matches another key's: max staleness 137 s). **C4 absorbed the same
  growth with 0 grow-sessions** (its slot array doubles in place up to 2x its
  build size): staleness max 829 ms, repair 835 KB per peer-minute, which is the
  loss-repair cost of the larger shards, not growth. At level set size C3 had 2
  grow-sessions in 10 minutes.
- **Replay** (rows `+replay`): the same sessions, but a delta applies only
  in order on the exact base and a gap is repaired by replaying the log:
  **786 / 786 repairs by replay** at 1 % loss and 5 % reordering, repair
  4 548 B per peer-minute against 76 827 (C1), 425 001 (C6b), 164 677 (C3)
  and 164 677 (C4): **16.9x less than the status quo, and the same for every
  candidate** (staleness unchanged: 136 / 480 / 991 ms). At 10 % loss: 12 767 B vs
  212 401 B (16.6x), 2 122 / 2 122 by replay.
- **Convergence**: no peer ended with a mirror root different from the owner's
  and no held key was missing, for any candidate that repairs, in any row.
- **C5's repair** is the heaviest: 4-23x C1's repair bytes per peer-minute
  (typically 11-12x: 0.71 MB against 61 KB at 1 % churn, 5 % reordering, no
  loss; 6.5 MB against 279 KB at 10 % churn), because a one-key difference refetches a
  4 096-key bucket (31 KiB).

## 9 Filter insert failures and rebuilds

Metric: insert failures (an eviction chain of 500 kicks that finds no slot)
and rebuilds, for the cuckoo filter at 50-95 % load. Source: `raw/cuckoo.md`
(`run.sh cuckoo`), f = 16, b = 4. "First failure" fills a fresh filter until the
first failed chain; "steady churn" removes a random present key and inserts
a fresh one 2 M times at a fixed load, rebuilding at 85 % load after each
failure.

first failure (fill from empty; fraction of slots used at the first failed eviction chain), f=16, 300 trials
| slots | min | p5 | median | trials failing before 90% | before 95% |
|---|---|---|---|---|---|
| 256 | 0.926 | 0.957 | 0.980 | 0 | 10 |
| 1024 | 0.950 | 0.962 | 0.975 | 0 | 0 |
| 4096 | 0.955 | 0.964 | 0.971 | 0 | 0 |
| 16384 | 0.952 | 0.963 | 0.968 | 0 | 0 |

steady churn at a fixed load: remove a random present key, insert a fresh one (2000000 cycles per cell; a failure leaves the shard to be rebuilt from the owner)
| slots | target load | failures | failures per 1M cycles | rebuilds (refetches) |
|---|---|---|---|---|
| 256 | 50% | 0 | 0.0 | 0 |
| 256 | 70% | 0 | 0.0 | 0 |
| 256 | 80% | 0 | 0.0 | 0 |
| 256 | 85% | 0 | 0.0 | 0 |
| 256 | 90% | 1 | 0.5 | 1 |
| 256 | 93% | 74 | 37.0 | 74 |
| 256 | 95% | 110 | 55.0 | 110 |
| 4096 | 50% | 0 | 0.0 | 0 |
| 4096 | 70% | 0 | 0.0 | 0 |
| 4096 | 80% | 0 | 0.0 | 0 |
| 4096 | 85% | 0 | 0.0 | 0 |
| 4096 | 90% | 0 | 0.0 | 0 |
| 4096 | 93% | 0 | 0.0 | 0 |
| 4096 | 95% | 0 | 0.0 | 0 |

- **C3-c confirmed**: no failure below 90 % load at any size; the first failure
  arrives at a median 0.97-0.98 of capacity (min 0.926 at 256 slots, 0.95 at
  4 096+); 10 of 300 256-slot filters failed before 95 %.
- Under churn at a fixed load there are no failures up to 85 % at either size;
  256-slot shards fail 0.5, 37 and 55 times per million cycles at 90, 93 and
  95 %; 4 096-slot shards had none up to 95 %. Production shards are built at 85 %
  and fail only through **net growth** of 12 % per shard (the growth row of
  section 8: 1 493 refetches for +75 % over 10 minutes at 8 peers).
- Each failure costs a refetch of the shard (one `Items` of ~256 keys,
  ~1.8 KB, one round trip after the 500 ms session spacing) and, until it
  lands, later adds to that shard are refused and show up as missed keys.

## 10 Crates: licence and `unsafe`

From the crates.io API and the downloaded sources on 2026-11-07 (latest
non-yanked version); `unsafe` = occurrences of the keyword in `src/` with
comments removed (a count of lines, which includes `unsafe fn` and blocks), and
whether the crate declares `forbid(unsafe_code)`. Used by this bench: `xorf`
(C5) and `qfilter` (C4) at exact versions; `blake3`, `postcard` and `serde`
are the product's own dependencies.

| Crate | Version (date) | Licence | `unsafe` count | forbids unsafe | Used |
|---|---|---|---|---|---|
| fastbloom | 0.17.0 (2026-03-01) | MIT OR Apache-2.0 | 0 | no | no |
| bloomfilter | 3.0.2 (2026-09-25) | ISC | 0 | yes | no |
| growable-bloom-filter | 2.1.1 (2024-10-06) | MIT | 0 | no | no |
| cuckoofilter | 0.5.0 (2020-08-25) | MIT | 0 | no | no (8-bit fingerprints; evicts a random item on overflow) |
| scalable_cuckoo_filter | 0.5.1 (2025-12-08) | MIT | 0 | no | no |
| **qfilter** | 0.3.1 (2026-06-14) | MIT | 31 (unchecked u64 reads/writes in the filter's hot paths, `NonZero::new_unchecked`) | no | **C4** |
| **xorf** | 0.13.0 (2026-08-21) | MIT | 4 (all in the zero-copy DMA serialisation helpers `dma_fingerprints` and the `*Ref` parsers; the code path C5 uses has none; built with `default-features = false, features = ["binary-fuse"]`) | no | **C5** |
| ribbon-filter | 0.2.0 (2026-03-22) | MIT OR Apache-2.0 | 0 | yes | no (about 1 k downloads) |
| clubcard | 0.3.4 (2026-07-31) | MPL-2.0 | 1 | no | no |
| sucds | 0.11.0 (2026-10-25) | MIT OR Apache-2.0 | 28 | no | no (C6a is in-tree) |
| sux | 0.15.4 (2026-11-03) | Apache-2.0 OR MIT | 616 | no | no |
| vers-vecs | 1.10.2 (2026-08-10) | MIT OR Apache-2.0 | 5 | yes (feature-gated unsafe) | no |
| cacheline-ef | 2.0.0 (2026-08-27) | MIT | 7 | no | no |
| iblt | 0.1.0 (2018-05-05) | MIT | 0 | no | no (abandoned) |
| riblt | 0.1.1 (2024-06-24) | non-standard | 0 | no | no (licence unusable) |
| do-riblt | 1.0.2 (2025-12-31) | MIT | 0 | no | no (RIBLT is in-tree) |
| minisketch-rs | 0.1.9 (2020-05-17) | Apache-2.0 | 17 (FFI) | no | no |
| negentropy | 0.5.3 (2026-11-02) | MIT | 0 | yes | no |
| rbsr | 0.2.3 (2026-09-28) | MIT OR Apache-2.0 | 0 | yes | no |

`cargo geiger` was not run; the counts are a grep, so they overstate (docs and
`unsafe` inside macros) and understate (macro-generated code) the real surface.
Both new dependencies are pinned with `=` in `Cargo.toml` and in the crate's
own `Cargo.lock`.

## 11 What was not measured

- **Real-node harness runs** (`coop-exact-churn`, `coop-digest-compare`,
  `peer_false_positives` and `peer_stale_misses` counters, WAN delay
  injection): the candidates exist only in this crate, so only the status quo
  could run in the harness; the in-process simulation of section 8 (real
  `Delta`s, real `respond`, real `wire_len`) stands in for it. The status quo's
  `peer_stale_misses` in a real fleet is therefore not measured; section 8's
  stale-positive rate is the proxy.
- **Real churn and real lookup rates (`L`)**: none available; the 0.1-10 %
  per minute grid and the formula `P·ε·L` are used.
- **Gossip duplication factor and QUIC framing** in wire bytes.
- **Cold-cache disk probes (C8a)**: needs privileges to drop caches.
- **C8a background merge**: the overlay merge is synchronous in this
  prototype (section 5 shows what that costs); a background merge would change
  the receiver CPU and tail latency, not memory or accuracy.
- **Incrementally maintained RIBLT symbols** and a RIBLT for mirrors that
  cannot enumerate (C3, C4, C5, C6b); none was built.
- **Candidate parameters were sampled, not tuned**: shard targets of 64 (C6b)
  and 256 keys (C3, C4), `K` = 16, 64, 256, 1024 (C5), w = 16, 24, 32 (C6b),
  f = 12, 16, 24, 32 (C3). Larger or smaller shards move repair bytes and
  memory in opposite directions.
- **Resharding** of C3/C4/C5/C6b when a peer's set drifts far from the size
  its mirror was built for (a full resync); not simulated beyond the growth
  row.
- Timings for the 8 Mi-key initial sync (minutes per run) were measured once
  on the correctness box (`raw/repair.md`) and not repeated on the timing box;
  the 1 Mi-key initial sync was.

## Reproducing

Everything runs from `bench/coop-digest/` on a Linux host with the repository
at the commit that contains this file; the crate is standalone (own
`Cargo.lock`, excluded from the workspace like `bench/enginebench`).

```
cd bench/coop-digest
cargo build --release --locked
cargo test --release                       # unit tests (parity with the real code)
export BIN=$PWD/target/release/coop-digest OUT=$PWD/raw COOP_DIGEST_DISK=/path/with/10GB
# correctness, size, wire, false positives (any box; minutes)
for s in cuckoo mem fp steady sim repair fp-large; do T=2400 ./run.sh $s; done
# timing (a quiet box; hours): interleaved repetitions, order reversed on even ones
for s in t-lookup t-steady t-repair; do T=3600 ./run.sh $s; done
python3 -I summarize.py raw lookup         # also: steady, repair
```

Each job runs under `timeout` (`T`, seconds). One-off commands behind the
tables, for example the real-session livelock:

```
$BIN repair --n 8000000 --specs c1 --scenarios gap50k --methods real,rbsr
$BIN lookup --spec c6b:4:64 --n 8000000 --peers 4 --ops 1000 --fp-ops 50000000 --seed 1
$BIN sim --spec c3 --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
```

Builds of record (sha256): `a00d8800a8b0f910e10c5f1c0def1271e4e165db0e764df1c39799ec00153492`
ran the `cuckoo`, `mem`, `fp`, `steady` and `sim` suites (correctness box) and
`t-lookup` and the first part of `t-steady` (timing box);
`7e7d295ffdbe7b45240183ce84f68a97c45a110e719817ddf5a0cffd8f18b86e` ran `repair`
and `fp-large` (correctness box) and `t-repair` and the rest of `t-steady`
(timing box). The two differ only in the false-negative check of the repair
command's `real` method (the first build checked the untouched candidate mirror
instead of the session's own, so its `real` rows read FAILED at 1 Mi keys; those
rows were discarded and re-run), in `run.sh` and in formatting; the timing code
paths (lookup, absorb, apply, respond) are identical. The committed source is the
second build's source and passes `cargo fmt --check`,
`cargo clippy --release --all-targets -- -D warnings` and `cargo test --release`.
