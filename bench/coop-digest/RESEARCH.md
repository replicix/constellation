# Plan 47 P0 — chunk-location digests: research

Status: P0 deliverable (read-only survey, 2026-11-07). Nothing here is a
measurement. Every number is either (a) read from the code at
`origin/main` (file and constant named), (b) derived from a formula given
next to it, or (c) quoted from a cited source. Anything else is marked
**[P1]**: a quantity P1 must measure before it can enter the decision.

Contents:

1. [Notation and the invariants](#1-notation-and-the-invariants)
2. [The status quo as implemented](#2-the-status-quo-as-implemented)
3. [Candidates](#3-candidates)
4. [Crates](#4-crates)
5. [Verdicts and the recommended P1 set](#5-verdicts-and-the-recommended-p1-set)
6. [P1 benchmark plan](#6-p1-benchmark-plan)
7. [Open questions for the maintainer](#7-open-questions-for-the-maintainer)
8. [References](#8-references)

---

## 1. Notation and the invariants

| Symbol | Meaning |
|---|---|
| `n` | keys in one peer's published set (chunks it can serve) |
| `P` | peers whose sets a node mirrors (fleet size − 1) |
| `U` | key universe; today `U = 2^64` (`reconcile::key_of`, top 8 bytes of the blake3 chunk hash) |
| `c` | changes (adds + removes) per peer per minute |
| `m` | keys in one delta (≤ `MAX_DELTA_KEYS` = 2048) |
| `d` | size of the symmetric difference between a mirror and its owner's set |
| `ε` | false-positive probability of one membership test against one peer |
| `L` | lookups per minute that consult the mirrors (local cache misses, plus `peer_digest_contains` calls from the upload path) |

The invariants of plan 47 §2, as used in the verdicts:

- **I1 No data risk.** A wrong answer may only cost a declined fetch (or,
  through `peer_digest_contains`, one confirming S3 HEAD) and an S3
  fallback. Every candidate below keeps I1, because chunk fetches are
  hash-verified regardless of how the holder was chosen; the location
  structure never vouches for content.
- **I2 Off the I/O path.** Only a local lookup on a read or write.
- **I3 Bounded memory per peer**, with a defined behaviour above the bound.
- **I4 Frames ≤ `MAX_FRAME`** (64 KiB, `crates/net/src/message.rs`), and
  gossip content ≤ `GOSSIP_CONTENT_LIMIT` (31 KiB).
- **I5 Removals take effect within a bounded time.**

The decision rule (plan §5) adds a fifth practical constraint: a candidate
that introduces false positives must cost fewer wasted peer round trips per
minute than the status quo's measured `peer_stale_misses`. A false
positive's cost is one extra peer round trip on a read, and one extra S3
HEAD on the upload path (`Coop::peer_digest_contains`, `coop.rs`), so the
expected wasted work per minute is about `L · Σ_peers ε_i` for a filter
(the probability that at least one of `P` peers falsely claims a chunk
that none holds is `1 − (1 − ε)^P ≈ P·ε`).

## 2. The status quo as implemented

Read from `crates/engine/src/coop/exact.rs`, `crates/net/src/reconcile.rs`,
`crates/engine/src/coop.rs`, `docs/reference/features/cooperative-cache.md`,
ADR-26 in `docs/explanation/DECISIONS.md`, plan 30 §M15 and its
PROGRESS.md entries.

**Publish.** `LocalSet::absorb` drains the cache's digest journal every
250 ms, computes the net change, and emits chained `Delta`s
(`incarnation`, `seq`, `base` and `root` fingerprints, adds and removes as
sorted varint gaps from 0, ≤ 2048 keys each). More than
`MAX_DELTAS_PER_TICK` (8) deltas' worth of change in one tick is published
silently (seq bumps, summary goes out). A `Summary` (incarnation, seq,
16-byte root, count; ~150 bytes) goes out every digest interval
(`CONSTELLATION_DIGEST_INTERVAL_S`, default 30 s). Deltas and summaries
travel on gossip. The publisher also keeps up to `RECENT_CAP` (262 144)
recent removals for decline classification.

**Mirror.** One `Mirror { keys: KeySet, incarnation, last_seq }` per peer.
`KeySet` shards keys by their top `shard_bits` bits (≤ 16) into sorted
`Vec<u64>` shards of target 64 keys, each with a cached additive
accumulator `Acc { count, Σk, Σmix(k) }` (24 bytes); a shard header is
48 bytes. A delta applied on its exact `base` keeps the mirror exact; any
other outcome queues a session. A peer advertising more than
`MAX_MIRROR_KEYS` (8 Mi) is not mirrored (treated as unknown).

**Repair (RBSR).** Initiator sends `Query { range, fp, count }` over
aligned 4-bit prefix ranges; the owner answers equal (nothing), `Items`
(≤ 32 keys, or initial sync that fits) or `Children` (16 × (fp, count)).
Replies ≤ `REPLY_BUDGET` 48 KiB, requests ≤ 512 queries. A session starts
at depth `start_bits_for(n)` (0 / 4 / 8 bits for n < 2 Ki / < 32 Ki /
larger). Also: a liveness probe of mirrors unconfirmed for two intervals,
≤ 4 concurrent sessions, ≥ 500 ms apart per peer, 5 s per round,
≤ 256 rounds.

**Lookup.** `exact_holders(key)`: under one `Mutex` over all mirrors,
prunes departed peers, then one shard index + binary search per fresh
mirror. An `Absent` decline removes the key from that mirror locally and
schedules a session.

### 2.1 Status-quo cost formulas

| Cost | Formula | At n = 1 Mi (4 TiB at 4 MiB chunks) |
|---|---|---|
| Receiver memory per peer | `8·n·s + 48·n/k̄` bytes, `s` = Vec capacity slack (1 … 2), `k̄` = keys per shard (16 … 128) | 8–12 B/key documented (`cooperative-cache.md`): 8–12 MiB per peer. Exact `s` distribution **[P1]** |
| Receiver memory per node | `P · n · (8–12 B)` | P = 63: 504–756 MiB |
| Hard cap per peer | `MAX_MIRROR_KEYS · 8` = 64 MiB; above: peer unknown | — |
| Delta wire per changed key | varint of a gap ≈ `⌈(log2(U/m)+1)/7⌉` bytes | m = 1: 10 B; m = 2048: ~8 B |
| Steady wire per node (received) | `P·(c·b_key + 150·60/interval)` B/min × gossip duplication factor **[P1]** | depends on `c` **[P1]** |
| Initial sync | ≈ `n · ⌈(log2(U/n)+1)/7⌉` bytes, `⌈that / 48 KiB⌉` rounds | ~7 B/key, ~7 MB, ~143 rounds (docs say ~150) |
| Repair, `d` isolated differences | `≈ q₀ + d · (ℓ · (37 + 430) + 37 + 7·n/2^b_leaf)` bytes, `ℓ` children levels between `start_bits` and the leaf depth where `n/2^b ≤ 32`; `q₀` = first request (256 × 37 B ≈ 9.5 KiB at start depth 8) | ℓ = 2 (start 8, leaf 16 bits, ~16 keys/leaf): ~1.1 KB per difference + 9.5 KiB once, 3 round trips |
| Repair, dense `d` | → Items for every touched leaf, ~7 B/key of those leaves | — |
| Lookup | `P` × (shift + binary search in ≤ 128 keys) under one mutex | ns-scale per peer **[P1]** |
| Publisher per tick | `O(m log m)` sort + `m` shard inserts (memmove ≤ 1 KiB) + one blake3 of 24 B per delta | **[P1]** |

The information-theoretic minimum for storing an arbitrary `n`-subset of
`U` exactly is `log2 C(U, n) ≈ n·(log2(U/n) + log2 e)` bits: 45.4 bits
(5.7 B) per key at n = 1 Mi, 42.4 bits (5.3 B) at 8 Mi. **No exact
representation of 64-bit keys can beat the current 8–12 B/key by more
than ~1.4–2.2×.** Getting further means storing fewer key bits, i.e.
accepting false positives — a filter, or a narrower key.

What the code has measured so far: one 3-node, 24-chunk, 1-s-interval
harness run (`coop-digest-compare`, PROGRESS.md): 2369 B/s fleet digest
traffic exact vs 637 B/s bloom, 0 false positives in both, 4 vs 11 stale
misses. That run is far from fleet scale; it does not inform P2.

## 3. Candidates

Each candidate: how it publishes, mirrors, repairs and looks up; costs;
what it gives up against §1's invariants; predictions for P1.

### C1. Status quo — exact mirror + deltas + RBSR

As §2. Gives up nothing (it defines the invariants). Its weaknesses are
the receiver's `P·n·8–12 B` and the cliff at 8 Mi keys (a big peer
becomes invisible rather than approximately visible).

Predictions for P1:
- C1-a: receiver bytes/key between 8 and 12 at n = 10 k … 4 Mi; above
  4 Mi (shard_bits capped at 16, so shards exceed 64 keys) the per-key
  overhead falls toward 8 but insert memmove cost rises linearly with
  `n / 2^16`.
- C1-b: repair of `d` isolated differences on n = 1 Mi costs
  `9.5 KiB + ~1.1 KB·d` and 3 round trips while `d ≪ 2^16`.
- C1-c: initial sync of 1 Mi keys: ~7 MB, ~143 round trips.
- C1-d: zero false positives (`peer_false_positives == 0`).

### C2. Bloom (existing option) and counting / scalable Bloom

**Bloom (today's `bloom` mode, plans 06/14).** Publish: per hash-prefix
bucket a 10-bits/entry, k = 7 filter, ≤ 16 KiB per bucket, ≤ 256 buckets
(`crates/net/src/bloom.rs`), resent per bucket per digest interval;
add-only deltas between. Mirror: the latest bucket filters (≤ 4 MiB per
peer). Repair: the next snapshot of that bucket. Lookup: k probes.
FPR `(1 − e^{−kn/m})^k` = 0.82 % at 10 bits/entry, k = 7 (formula);
optimal space `1.44·log2(1/ε)` bits/key [Bloom70; Cuckoo14 Table 2].

Gives up: **I5** — removals only by the next full snapshot of the bucket;
zero false positives. This is why ADR-26 demoted it.

**Counting Bloom** [SummaryCache00] replaces bits with counters (4-bit in
Summary Cache, the cooperative web-proxy design that is the closest prior
art to this problem), so deltas can carry removals. Space is ~4× a Bloom
filter for the same ε. Deleting an item that was never inserted corrupts
it (true of every deletable filter [Cuckoo14 §3]). Counters can overflow
(4-bit), a correctness bug only in the sense of false negatives.
**Scalable Bloom** [SBF07] grows by adding filters with tightening error;
no deletes, so it does not address I5 at all.

Costs: counting Bloom ≈ `4 · 1.44 · log2(1/ε)` bits/key: ~38 bits at
ε = 1 %, ~92 bits at ε = 2^-16 — no better than the exact mirror's 64–96
bits at any useful ε.

Predictions: C2-a counting Bloom ≥ exact mirror bytes/key for
ε ≤ 2^-10 (formula: `5.8·log2(1/ε) ≥ 64`) — so it cannot win on memory
where FP rates are acceptable.

### C3. Cuckoo filter

Structure [Cuckoo14]: `b`-way buckets of `f`-bit fingerprints, two
candidate buckets per item (partial-key cuckoo hashing). Space
`(log2(1/ε) + 3)/α` bits/item with `α = 95.5 %` at b = 4 (paper,
Table 2); load factor "84 %, 95 % or 98 %" for b = 2, 4, 8. Insertion can
fail near full load; inserting the same item more than `2b` times fails;
"to delete an item x safely, it must have been previously inserted"
(quoted, §3.3). It cannot enumerate keys.

Design as a mirror (receiver-side; the owner keeps its exact `KeySet`):
- **Publish:** unchanged (the same `Delta`s carry full 8-byte keys).
- **Mirror:** per peer, `2^b` prefix buckets, each with an exact `Acc`
  (24 B, updated from the full keys in deltas) and a cuckoo filter sized
  for the bucket. The root fingerprint is the sum of the bucket `Acc`s,
  so the existing `base`/`root` chain still verifies deltas exactly.
- **Apply rule:** a delta applies only on the exact base. A diverged
  delta is *not* applied (applying a remove for a key the mirror never
  had would delete another key's fingerprint — the paper's warning);
  the touched buckets are marked dirty.
- **Repair:** RBSR down to bucket granularity works unchanged (the
  receiver has exact `Acc`s per bucket). Below a bucket, the receiver
  cannot enumerate, so it asks for the whole bucket as `Items` and
  rebuilds that bucket's filter. Repair cost per isolated difference
  rises from ~32 keys to one bucket's keys (`7·n/2^b` bytes).
- **Lookup:** two bucket probes per peer.

Costs: at ε = 2^-16 (1.5e-5), 19.9 bits/key ≈ 2.5 B/key + 24 B per
bucket; at ε = 1 %, 10.1 bits. Against 8–12 B/key: ~3.2–4.8× smaller at
ε = 2^-16. Wire: unchanged for deltas and summaries; repair larger by the
bucket granularity.

Gives up: zero false positives (ε per lookup per peer); deltas off the
exact base cost a bucket refetch instead of being applied. Above the
memory bound it can do better than C1 (a big peer is approximately
visible, not invisible), but insert failure at load must have a defined
behaviour (resize the bucket's filter from the refetched bucket keys).

Predictions: C3-a bytes/key ≈ `(f + … )/α/8` within 10 % of
`(log2(1/ε)+3)/0.955/8`; C3-b measured FPR ≈ `2b/2^f` (paper, §4);
C3-c insert failures appear only above ~95 % load at b = 4; C3-d repair
bytes per isolated difference ≈ `7·n/2^b` + RBSR descent; C3-e lookups
are within 2× of C1's binary search **[P1]**.

### C4. Quotient / counting quotient filter

Structure: a quotient filter [QF12] splits a fingerprint into a quotient
(slot index) and an `r`-bit remainder stored in that slot with a few
metadata bits; the rank-select variant (RSQF/CQF) [CQF17] reduces the
metadata to about 2.125 bits per slot. The `qfilter` crate's README
tabulates "10.125" bits per item (r = 8) for an error of 0.00351 when
full, consistent with `r + 2.125` bits per slot. QFs support deletes,
merging and resizing, and — unlike cuckoo — store the fingerprints in
sorted order, so a QF *can* enumerate its fingerprints (not the original
keys).

Design: as C3 (buckets with exact `Acc`s, apply-on-exact-base, bucket
refetch below bucket granularity). Its extra over cuckoo: growth without
a rebuild from keys (resizing a QF consumes one remainder bit per
doubling, so ε degrades as it grows [QF12]); merge is irrelevant here.

Costs: `(r + 2.125)/α` bits per key, ε ≈ 2^-r at full load (qfilter
table); at r = 16: ~19 bits ≈ 2.3 B/key — about the same as cuckoo.
Same deletion caveat as cuckoo.

Gives up: as C3.

Predictions: C4-a memory within 10 % of C3 at the same ε; C4-b no insert
failures below the configured capacity; C4-c lookups slower than cuckoo
(rank/select, cluster scan) by a factor **[P1]**. Because C3 and C4 have
the same protocol shape and near-equal space, P1 needs only one of them
unless the first shows a problem (see §5).

### C5. Static filters (xor, binary fuse, ribbon) as periodic snapshots

Structures: xor filter ≈ `1.23·f` bits/key, ε = 2^-f [Xor20]; binary
fuse "within 13 % of the storage lower bound" (3-wise) or "within 8 %"
(4-wise) [BFuse22]; ribbon "space overhead below 10 %" and experimentally
below 1 % [Ribbon21]. All are built once from the full key list and
cannot be updated.

Design: owner builds one filter per prefix bucket and gossips/streams
snapshots of changed buckets; the receiver applies exact deltas to a
per-bucket **overlay** (added keys, removed keys) on top of the static
base: `hit = (base ∋ k ∧ k ∉ removed) ∨ k ∈ added`. Removals take effect
in one tick (I5 kept) because the overlay is exact. The overlay grows
until the owner ships a new bucket snapshot.

Costs: snapshot of one bucket of `s` keys at f = 16 binary fuse: ~18
bits/key → a 48 KiB frame carries ~21 k keys (formula: `48·1024·8/18`).
Steady state: deltas as today plus re-snapshots. If a bucket is
re-snapshotted after `k` changes, the re-snapshot wire per change is
`s·18/(8k)` bytes vs ~8–10 B per change for a delta alone: at
`s = 4096, k = 64` that is 144 B per change — an order of magnitude more
than C1's deltas, unless churn is low. Receiver memory: ~18 bits/key base
plus 8 B per overlay key. The construction is O(s) per bucket at the
owner [BFuse22 reports construction "more than twice as fast" as xor].

Gives up: wire efficiency under churn (re-snapshot amplification); zero
FPs; the owner must build filters (publisher CPU). Its best case is a
large, cold peer.

Predictions: C5-a receiver bytes/key ≈ 2.25 at f = 16 (binary fuse);
C5-b wire per change ≥ 10× C1's at 1 % churn/min for any `s/k ≥ 640`;
C5-c snapshot build for 21 k keys is sub-millisecond **[P1]**.

### C6. Compact exact storage of the same protocol

Wire, repair and lookup unchanged; only `KeySet`'s shard storage changes.
Options per shard: Elias–Fano [EF13] (`2 + ⌈log2(U/n)⌉` bits/key: 46 at
1 Mi, 43 at 8 Mi), delta-varint blocks (~7 B/key, the wire encoding), or
the implicit-prefix trick below. Shards are small (≤ 128 keys), so each
mutation re-encodes one shard: O(k̄) per insert, the same order as today's
memmove.

**C6a — compressed full keys.** EF or delta blocks of the full 64-bit
key. Saving vs today: `64·s / (2 + log2(U/n))` → 1.4× (no slack) to
2.1× (2× slack) at n = 1 Mi. Gives up nothing. Lookup decodes one
≤ 128-key shard (slower by a factor **[P1]**).

**C6b — implicit shard prefix + truncated suffix (exact accumulators,
approximate membership).** The shard index already encodes the top
`shard_bits` bits of each key, so storing the full key wastes them.
Store per shard the exact `Acc` (computed from full keys as they arrive
in deltas and `Items`, exactly as today) and a sorted array of `w`-bit
suffixes (bits `shard_bits … shard_bits+w`) with a per-suffix refcount
on collision. Effective key width is `shard_bits + w`; a lookup of an
absent key matches with probability `≈ n / 2^(shard_bits+w)` per peer.

| n | shard_bits | w | bytes/key (`w/8 + 48/k̄`) | ε per lookup per peer |
|---|---|---|---|---|
| 1 Mi | 14 | 32 | 4.75 | 1.5e-8 |
| 1 Mi | 14 | 24 | 3.75 | 3.8e-6 |
| 8 Mi | 16 | 32 | 4.4 | 3.0e-8 |
| 8 Mi | 16 | 24 | 3.4 | 7.6e-6 |

(derived from the formulas; `k̄ = n/2^shard_bits`). Wire and publish are
unchanged; RBSR works down to shard depth from the exact `Acc`s; a
difference below shard depth makes the receiver request the whole shard
as `Items` (≤ 128 keys ≈ 0.9 KB, vs ≤ 32 keys today). Removals apply
immediately (I5 kept). A diverged remove of a key the mirror never had
is a no-op unless another key in the shard shares its suffix (then that
key is lost: a false negative); either way the shard's `Acc` no longer
matches the owner's and the shard is refetched, so the "delete a
non-inserted item" hazard of C3/C4 reduces to a stale shard until the
next session. Combined with EF coding of the suffixes, ~`2 + w − log2 k̄`
bits/key is possible (formula), at more CPU per mutation.

Gives up: zero false positives (but at ε ~1e-8 per peer the expected
false positives per minute are `L·P·1.5e-8`); a shard resize must be
driven from the full keys, which the receiver no longer has → resize by
refetching (owner `Items`) or by sizing shards from the advertised
`count` up front.

Predictions: C6a-a 5.4–6.6 B/key with EF at 8 Mi … 10 k keys; C6b-a
3.4–4.8 B/key at the table's settings, i.e. ≥ 2× better than C1's
8–12 B (the plan's threshold) with wire bytes unchanged within noise;
C6b-b measured FPR matches `n/2^(shard_bits+w)`; C6b-c repair bytes per
isolated difference rise by ≤ ~0.9 KB.

### C7. Set-reconciliation alternatives for the repair step

These replace or front RBSR; publishing, mirroring and lookup stay as C1.

**C7a — IBLT** [IBLT11; Difference11]. Each side sums its keys into a
table of cells `(count, keySum, hashSum)`; subtracting tables and peeling
yields the difference. Needs ~`c·d` cells with `c` > 1 and a size
estimate (strata estimator [Difference11]). Cell ≈ 8 + 4 + 1–4 bytes.
Both tables are linear, so owner and mirror could maintain them
incrementally per change. A fixed-size IBLT fails to decode when `d`
exceeds its capacity → retry larger or fall back.

**C7b — Rateless IBLT** [RIBLT24]: an infinite stream of coded symbols,
"average overhead … converges to 1.35" symbols per difference, a symbol
being (XOR of items, a hash checksum, a variable-length count averaging
~1.05 B); decoding O(ℓ log d) per difference. Wire for `d` differences
≈ `1.35–1.72 · d · (8 + 4 + 1)` ≈ 18–22 B/difference (formula with the
paper's overhead range and 4-byte checksums), in one streaming exchange,
with no prior estimate of `d`. Cost: the owner computes symbols over
its whole set unless it keeps a cached prefix of symbols updated per
change; both sides need the same for the mirror. Versus RBSR's
~1.1 KB per isolated difference at n = 1 Mi: up to ~50× less repair wire
for small `d` (formula).

**C7c — Minisketch / PinSketch** [Minisketch]: a sketch of capacity `c`
over b-bit elements is `b·c` bits (64-bit keys: 8 B per unit of
capacity, i.e. optimal), but decoding is O(c²) ("can be too expensive
for applications where the difference is sufficiently large"), and the
capacity must be chosen in advance (Erlay [Erlay19] pairs it with
estimation and bisection). Linear, so maintainable incrementally at O(c)
per change. Only Rust access is an FFI binding to the C++ library.

**C7d — delta-log replay** (not in the plan's list; added because it
is the cheapest repair for the common case). The owner keeps its last
`R` deltas (each ≤ ~21 KiB); a receiver with a seq gap asks for "deltas
after seq s" in one round trip and replays them on the exact base. Wire
equals the missed deltas exactly. It cannot repair a restart
(incarnation change), a mirror that diverged for other reasons, or a gap
older than `R`; those fall back to RBSR. Owner memory: `R · avg delta
size` **[P1]**.

**C7e — Negentropy, re-checked** [Negentropy]. The reasons in
`reconcile.rs` and ADR-26, re-checked against the sources on 2026-11-07:

| Reason in `reconcile.rs` | Re-check |
|---|---|
| 40-byte items (timestamp + 32-byte id) | Still true: the README says "The ID must be 32 bytes in length"; the Rust crate exports a fixed `ID_SIZE`. Padding 8-byte keys to 32 bytes plus the 8-byte timestamp is 5× today's mirror. |
| Sealed vector storage; re-seal per change | Still true for the Rust crate: docs.rs for `negentropy` 0.5.3 lists only `NegentropyStorageVector`. The B-tree storages (incremental) are C++ only (README backend list). |
| Symmetric union-sync | **Weaker than stated.** The initiator learns both the ids it has that the server lacks and those it needs, so an asymmetric exact mirror (apply "need" as adds, "have" as removes) is expressible. Verify against the crate source in P1 if Negentropy is kept. |
| Own framing | **Weaker than stated.** The protocol has an optional frame size limit: "all messages created by this instance will be of length equal to or smaller than this number of bytes" (README), so its messages could be carried inside our signed postcard frames. |

Verdict unchanged on the two reasons that still hold (item size and
storage). The newer `rbsr` crate (0.2.3, MIT/Apache-2.0, generic over a
rank/select/aggregate store, Meyer's paper) is the closest crate fit
for RBSR but brings nothing the in-tree code lacks, and is young
(~3 k downloads). `iroh-docs` has its own range-based sync bound to its
document entries; not reusable here.

Predictions: C7b-a repair wire for `d ∈ {1, 1 k}` on n = 1 Mi is
18–22 B/difference plus a fixed header, vs C1's `9.5 KiB + 1.1 KB·d`;
C7b-b for `d = 100 k` RIBLT's advantage shrinks because RBSR degenerates
to bulk Items at ~7 B/key, which is *below* RIBLT's ~18 B/difference —
prediction: RIBLT loses for dense differences; C7d-a for gaps of 1 … R
deltas, replay wire = sum of missed delta sizes and 1 round trip.

### C8. Hybrids

**C8a — filter over an exact on-disk mirror.** An in-memory filter
(C3/C4) per peer answers lookups; the exact `KeySet` lives on local disk
(the engine bake-off of plan 28 lists suitable stores). A filter hit is
confirmed by a disk lookup before the peer is chosen, so no false
positive leaves the node. Memory: the filter only; disk: ≥ 8 B/key per
peer plus write amplification per delta. Gives up: arguably I2 (a disk
read on a read path, though only on a local cache miss that is about to
go to the network) — a maintainer call; a large amount of new
persistence code.

**C8b — exact below a size threshold, approximate above.** Peers with
`n ≤ T` keep C1 (or C6a) mirrors; larger peers switch to C6b or C3. This
replaces the 8 Mi cliff (peer invisible) with graceful degradation. It is
a policy on top of whichever approximate representation wins; P1
measures the representations, not the threshold.

**C8c — narrow keys end to end.** Use `w`-bit keys (e.g. 40) on the wire
and in RBSR, with owner-side refcounts for colliding keys. Delta wire per
key ≈ `⌈(w − log2 m + 1)/7⌉` bytes as varints (6 vs 8–10 B for `w = 40`,
formula), and receiver memory `w/8` B/key. It changes the wire, the
fingerprints and the owner, for less receiver gain than C6b (which keeps
the wire). Kept only as a fallback if P1 shows steady wire to be a real
cost.

## 4. Crates

Metadata from the crates.io API on 2026-11-07 (latest non-yanked
version and its release date). "unsafe" is from the crate's own
documentation where it says; otherwise **[P1]**: `cargo geiger` (or a
grep of the vendored source) during P1, before any adoption. None of
these crates is in `Cargo.lock` today.

| Crate | Version (date) | Licence | Implements | Fit with 8-byte keys / 64 KiB frames | unsafe |
|---|---|---|---|---|---|
| `fastbloom` | 0.17.0 (2026-03) | MIT OR Apache-2.0 | Bloom | any hash; ser size = m/8, bucket to fit | [P1] |
| `bloomfilter` | 3.0.2 (2026-09) | ISC | Bloom | as above | [P1] |
| `growable-bloom-filter` | 2.1.1 (2024-10) | MIT | scalable Bloom | no deletes → fails I5 | [P1] |
| (in tree) `constellation_net::bloom` | — | project | Bloom, buckets ≤ 16 KiB | today's option | — |
| `cuckoofilter` | 0.5.0 (2020-08) | MIT | cuckoo, 8-bit fp, b = 4 | fp too short (ε ≈ 3 %), unmaintained; on overflow "a random existing item is evicted" (README) | [P1] |
| `scalable_cuckoo_filter` | 0.5.1 (2025-12) | MIT | growable cuckoo | configurable ε; check serde and fp width | [P1] |
| `qfilter` | 0.3.1 (2026-06) | MIT | RSQF, removals, grow, merge, serde, u64 fingerprint API | good: fingerprints from our own key bits; needs x86-64 popcnt/BMI2 or a feature flag | README: "Only verifiable usages of unsafe" |
| `xorf` | 0.13.0 (2026-08) | MIT | xor 8/16/32, binary fuse 8/16/32, serde, no_std | u64 keys native ("Xor filters operate only on sets of 64-bit … integers"); dedup required; binary fuse build can fail on duplicates | [P1] |
| `ribbon-filter` | 0.2.0 (2026-03) | MIT OR Apache-2.0 | static ribbon | ~1 k downloads; immature | [P1] |
| `clubcard` | 0.3.4 (2026-07) | MPL-2.0 | "exact membership query filter for static sets" (CRLite) | static and built for a known universe; does not fit a moving cache set | [P1] |
| `sucds` | 0.11.0 (2026-10) | MIT OR Apache-2.0 | Elias–Fano, rank/select | static EF per shard (C6a) | [P1] |
| `sux` | 0.15.4 (2026-11) | Apache-2.0 OR MIT | EF and succinct structures | as above; MSRV 1.85 | [P1] |
| `vers-vecs` | 1.10.2 (2026-08) | MIT OR Apache-2.0 | EF, rank/select | as above | [P1] |
| `cacheline-ef` | 2.0.0 (2026-08) | MIT | per-cache-line EF of sorted ints | interesting for C6a lookups | [P1] |
| `iblt` | 0.1.0 (2018-05) | MIT | IBLT | abandoned | [P1] |
| `riblt` | 0.1.1 (2024-06) | non-standard | rateless IBLT | licence unclear → unusable without review | [P1] |
| `do-riblt` | 1.0.2 (2025-12) | MIT | rateless IBLT | ~400 downloads; would need review; an in-tree RIBLT over u64 is small | [P1] |
| `minisketch-rs` | 0.1.9 (2020-05) | Apache-2.0 | FFI to C++ minisketch | C++ build dependency, FFI unsafe by construction, unmaintained | FFI |
| `negentropy` | 0.5.3 (2026-11) | MIT | Negentropy v1, vector storage | 32-byte ids; see C7e | [P1] |
| `rbsr` | 0.2.3 (2026-09) | MIT OR Apache-2.0 | generic RBSR | no gain over in-tree; young | [P1] |

All candidate structures can be written in-tree at small size (cuckoo,
suffix arrays, EF over ≤ 128 keys, RIBLT over u64), as the RBSR was. P1
should benchmark one crate per structure where a credible crate exists
(`qfilter`, `xorf`, `scalable_cuckoo_filter` or an in-tree cuckoo,
`sucds`/`sux` for EF) and in-tree prototypes otherwise.

## 5. Verdicts and the recommended P1 set

| # | Candidate | Verdict | Reason |
|---|---|---|---|
| C1 | Status quo | **Keep (baseline)** | — |
| C2 | Bloom | **Keep as reference only** | the existing option; measured for the comparison, not a contender (fails I5 without full re-snapshots) |
| C2' | Counting Bloom | **Drop** | `≥ 5.8·log2(1/ε)` bits/key ≥ the exact mirror's 64 bits for ε ≤ 2^-10: no memory win at a usable ε, plus counter overflow |
| C2'' | Scalable Bloom | **Drop** | no removals: fails I5 |
| C3 | Cuckoo (bucketed, exact Acc) | **P1** | 3–5× smaller receiver at ε = 2^-16; removals in one tick; the maintainer's named candidate |
| C4 | Quotient filter | **P1, conditional** | same protocol and space as C3; benchmark via `qfilter` only if C3 shows insert failures or resize problems, else record "dominated by C3 within 10 %" |
| C5 | Static (binary fuse/xor/ribbon) + overlay | **P1, narrow** | only the large-cold-peer case; drop if C5-b (≥ 10× wire per change) holds at 0.1 % churn |
| C6a | Compressed exact | **P1** | the "smaller, measured improvement" the plan's rule anticipates; ≤ 2.1× by formula, zero FP |
| C6b | Implicit prefix + suffix | **P1 (lead candidate)** | ≥ 2× receiver memory at ε ~1e-8, wire unchanged |
| C7a | Fixed IBLT | **Drop** | dominated by C7b (needs a size estimate; fails on overflow) |
| C7b | Rateless IBLT | **P1 (repair only)** | the only repair alternative with a predicted ≥ 2× gain for sparse gaps |
| C7c | Minisketch | **Drop** | O(c²) decode, fixed capacity, only an unmaintained FFI binding |
| C7d | Delta-log replay | **P1 (repair only)** | cheapest repair for lost gossip; trivial |
| C7e | Negentropy | **Drop** | still 32-byte ids and a sealed vector in Rust (re-checked); RBSR in tree is the same family |
| C8a | Filter over on-disk exact | **Drop for P1, ask maintainer** | I2 question and large new persistence code; revisit only if C6b's memory is still too high |
| C8b | Exact below T | **Policy, not benchmarked** | composes with the winner |
| C8c | Narrow keys end-to-end | **Defer** | only if steady wire proves to be a real cost |

**Recommended P1 set:** C1 (baseline), C2 Bloom (reference), C3 cuckoo,
C6a compressed exact, C6b suffix mirror, C5 binary fuse + overlay
(narrow), C4 quotient (conditional); repair: C1 RBSR vs C7b RIBLT vs C7d
delta-log replay.

## 6. P1 benchmark plan

### 6.1 Crate layout

`bench/coop-digest/` as a standalone crate with its own `Cargo.lock`,
like `bench/enginebench` (not a workspace member, so product builds and
the lockfile are untouched):

```
bench/coop-digest/
  Cargo.toml           # path dep on crates/net (reconcile, bloom, message::wire_len)
  RESEARCH.md          # this file
  RESULTS.md           # P1 output, with reproduce commands
  src/lib.rs           # trait LocationMirror { apply_delta, apply_items,
                       #   range_acc, contains, bytes } + workload generators
  src/candidates/{exact.rs, compact_ef.rs, suffix.rs, cuckoo.rs,
                  quotient.rs, fuse_overlay.rs, bloom.rs}
  src/repair/{rbsr.rs, riblt.rs, delta_log.rs}
  src/oracle.rs        # the true set; counts FP/FN per lookup
  src/alloc.rs         # counting global allocator (live bytes)
  benches/*.rs         # criterion: lookup hit/miss, apply delta, publish tick
  src/bin/sim.rs       # multi-peer in-process simulation: loss, reorder, restart
  run.sh               # every job under `timeout`
```

### 6.2 Getting the real sender and receiver sizes

- **Receiver:** use `constellation_net::reconcile::{KeySet, Mirror}`
  directly for C1 (they are `pub`); `KeySet::approx_bytes` counts only
  `capacity·8 + 48/shard`, so measure with the counting allocator as
  well (live bytes before/after building `P` mirrors) and report both.
- **Sender:** `LocalSet` is `pub(super)` in the engine. Either copy
  `absorb` into the bench verbatim (and pin the source commit in
  RESULTS.md), or expose it under a test-only cargo feature — the plan
  allows either; copying avoids touching the engine in P1.
- **Wire:** encode real `Payload::CacheSetDelta`, `CacheSummary`,
  `ReconcileRequest`/`Reply` and take `constellation_net::message::wire_len`
  (or `Signed::encode` length) — never estimate from field sizes.
  New candidates' messages: postcard-encode the proposed structs and
  assert ≤ `MAX_FRAME` / `GOSSIP_CONTENT_LIMIT`.
- **Harness:** `coop-exact-churn` and `coop-digest-compare` with the
  `status.coop` counters (`digest_bytes_{sent,received}`,
  `digest_messages`, `digest_cpu_us`, `reconcile_*`,
  `peer_false_positives`, `peer_stale_misses`, `peer_set_{entries,bytes}`),
  at the production digest interval (30 s), not 1 s.

### 6.3 Workloads

- Peers `P` ∈ {2, 8, 32, 64}; per-peer `n` ∈ {10 k, 100 k, 1 Mi, 4 Mi,
  8 Mi}; keys are uniform u64 (blake3 prefixes; generate with a seeded
  PRNG, state the seed).
- Churn: uniform 0.1 %, 1 %, 10 % of `n` per minute, in 250 ms ticks;
  bursty: a full cache rebuild (rebuild flag, silent publish path).
- Loss/reorder: drop 0, 1, 10 % of deltas; reorder within a window of
  4; peer restart (new incarnation, same set ± 1 %); initial sync.
- Gaps for repair: `d` ∈ {1, 1 k, 100 k} on n = 1 Mi and 8 Mi.
- Lookups: 50 % hits / 50 % misses against the oracle, ≥ 10^7 misses
  per candidate so ε down to ~1e-6 is measurable; below that report
  "0 in N" and the formula.

### 6.4 Metrics (as plan §4) and the predictions they test

| Metric | Predictions tested |
|---|---|
| Receiver bytes/key (allocator) | C1-a, C3-a, C4-a, C5-a, C6a-a, C6b-a |
| Publisher CPU per tick | unchanged for receiver-side candidates (C3, C4, C6) within noise; C5 build cost (C5-c) |
| Steady wire per peer-minute | unchanged for C3/C4/C6; C5-b |
| Repair bytes / round trips / wall | C1-b, C1-c, C3-d, C6b-c, C7b-a, C7b-b, C7d-a |
| Lookup p50/p99 hit/miss, P = 64 | C3-e, C4-c, C6a lookup factor |
| FP/FN per lookup vs oracle | C1-d, C3-b, C6b-b; FN must be 0 after repair for all |
| Insert failures / rebuilds | C3-c, C4-b |
| Staleness (harness, injected loss) | all; compared with `peer_stale_misses` per the §5 rule |

Microbenchmarks run on one machine without boxes; timing numbers
(lookup, CPU per tick) on the quiet box, A/B/B/A, medians with spread,
every job under `timeout`. Memory, wire bytes, FP counts and round
trips are deterministic given the seed and need no quiet box.

## 7. Open questions for the maintainer

1. **Target fleet:** peers per fleet (is 64 the top?) and per-node cache
   size in chunks. Chunks are ≤ 4 MiB (`DEFAULT_CHUNK_SIZE`) but small
   files make smaller chunks, so keys per TiB can be far above 256 k.
   The receiver-memory case for a change depends entirely on `P · n`.
2. **Acceptable ε:** is a false-positive rate of ~1e-8 per lookup per
   peer (C6b at w = 32) acceptable as "effectively zero", given that
   `peer_digest_contains` turns a false positive into one S3 HEAD on the
   upload path? Or is zero by construction a requirement?
3. **Above the cap:** should a peer over the memory bound become
   approximately visible (C8b) instead of invisible as today?
4. **C8a and I2:** does a local disk probe on a local cache miss count
   as "a local lookup" under I2?
5. **Delta-log replay (C7d)** is not in the plan's list; may P1 include
   it?
6. **Loss injection:** the harness has delay injection (toxiproxy) but
   no gossip-loss knob for the coop plane was found; may P1 add a
   test-only fault knob for dropped/reordered deltas, as plan 30 M0 did
   for forwarding?
7. **Real churn:** is there any production or campaign data on cache
   turnover per minute? If not, P1 uses the plan's 0.1–10 % grid.

## 8. References

- [Bloom70] B. H. Bloom, "Space/time trade-offs in hash coding with
  allowable errors", CACM 1970. https://dl.acm.org/doi/10.1145/362686.362692
- [SummaryCache00] L. Fan, P. Cao, J. Almeida, A. Broder, "Summary
  Cache: a scalable wide-area web cache sharing protocol", IEEE/ACM ToN
  2000. https://pages.cs.wisc.edu/~jussara/papers/00ton.pdf
- [SBF07] P. S. Almeida et al., "Scalable Bloom Filters", IPL 2007.
  https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
- [Cuckoo14] B. Fan, D. G. Andersen, M. Kaminsky, M. Mitzenmacher,
  "Cuckoo Filter: Practically Better Than Bloom", CoNEXT 2014.
  https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
- [QF12] M. A. Bender et al., "Don't Thrash: How to Cache Your Hash on
  Flash", VLDB 2012. https://vldb.org/pvldb/vol5/p1627_michaelabender_vldb2012.pdf
- [CQF17] P. Pandey, M. A. Bender, R. Johnson, R. Patro, "A
  General-Purpose Counting Filter: Making Every Bit Count", SIGMOD 2017.
  https://dl.acm.org/doi/10.1145/3035918.3035963
- [Xor20] T. M. Graf, D. Lemire, "Xor Filters: Faster and Smaller Than
  Bloom and Cuckoo Filters", JEA 2020. https://arxiv.org/abs/1912.08258
- [BFuse22] T. M. Graf, D. Lemire, "Binary Fuse Filters: Fast and
  Smaller Than Xor Filters", JEA 2022. https://arxiv.org/abs/2201.01174
- [Ribbon21] P. C. Dillinger, S. Walzer, "Ribbon filter: practically
  smaller than Bloom and Xor", 2021. https://arxiv.org/abs/2103.02515
- [EF13] S. Vigna, "Quasi-succinct indices", WSDM 2013.
  https://arxiv.org/abs/1206.4300
- [IBLT11] M. T. Goodrich, M. Mitzenmacher, "Invertible Bloom Lookup
  Tables", 2011. https://arxiv.org/abs/1101.2245
- [Difference11] D. Eppstein, M. T. Goodrich, F. Uyeda, G. Varghese,
  "What's the Difference? Efficient Set Reconciliation without Prior
  Context", SIGCOMM 2011.
  https://www.ics.uci.edu/~eppstein/pubs/EppGooUye-SIGCOMM-11.pdf
- [RIBLT24] L. Yang, Y. Gilad, M. Alizadeh, "Practical Rateless Set
  Reconciliation", SIGCOMM 2024. https://arxiv.org/abs/2402.02668
- [Minisketch] P. Wuille et al., minisketch (PinSketch library).
  https://github.com/sipa/minisketch
- [Erlay19] G. Naumenko et al., "Erlay: Efficient Transaction Relay for
  Bitcoin", CCS 2019. https://arxiv.org/abs/1905.10518
- [RBSR] A. Meyer, "Range-Based Set Reconciliation", 2022.
  https://arxiv.org/abs/2212.13567
- [Negentropy] D. Hoyt, negentropy. https://github.com/hoytech/negentropy
- Crates: https://crates.io/crates/<name> for each row of §4;
  `qfilter` README https://github.com/arthurprs/qfilter; `xorf` docs
  https://docs.rs/xorf; `cuckoofilter` README
  https://github.com/axiomhq/rust-cuckoofilter; `rbsr` docs
  https://docs.rs/rbsr; `negentropy` docs https://docs.rs/negentropy.
