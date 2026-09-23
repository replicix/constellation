# Cooperative cache membership

How nodes learn which chunks their peers hold, so a local cache miss
can be fetched from a nearby peer instead of S3. Reading from a peer is
an optimization: S3 stays the source of truth, every peer chunk is
hash-verified, and a peer that declines or fails costs latency only.

## Table of Contents

- [Modes](#modes)
- [Exact mode protocol](#exact-mode-protocol)
  - [Keys and fingerprints](#keys-and-fingerprints)
  - [Pushed deltas and summaries](#pushed-deltas-and-summaries)
  - [Reconciliation sessions](#reconciliation-sessions)
  - [Message budget](#message-budget)
  - [Memory](#memory)
- [False-positive accounting](#false-positive-accounting)
- [Upload existence hints](#upload-existence-hints)
- [Environment variables](#environment-variables)
- [Status and metrics](#status-and-metrics)
- [Comparing the modes](#comparing-the-modes)

## Modes

`CONSTELLATION_COOP_DIGEST` selects the membership protocol:

| Mode | What peers exchange | Lookup precision |
|---|---|---|
| `exact` (default) | pushed adds/removes, a summary heartbeat, range-based set reconciliation on any gap | exact (8-byte keys; collisions ~n/2^64 per lookup) |
| `bloom` | per-hash-prefix bloom snapshots plus add-only deltas (plans 06/14) | ~1% false positives; removals stay advertised until the bucket's next snapshot |

Every node of a fleet should use the same mode. An exact node ignores
bloom digests and a bloom node declines reconciliation rounds, so a
mixed pair simply does not use each other as sources.

## Exact mode protocol

Implemented in `crates/net/src/reconcile.rs` (pure protocol) and
`crates/cli/src/coop/exact.rs` (publishing, mirrors, session driver).
Each node keeps its own *published set* (its clean or pinned chunks)
and one *mirror* of every peer's published set.

### Keys and fingerprints

- A chunk's key is the first 8 bytes of its hash, big-endian.
- A key range is an aligned prefix `(bits, path)`. Splitting a range
  gives its 16 children. Keys are uniform hashes, so prefixes are
  balanced without Negentropy's item-count boundaries.
- A range's fingerprint is `blake3(count ‖ Σk ‖ Σmix(k))[..16]`, where
  the sums are wrapping 64-bit and `mix` is the splitmix64 finalizer.
  The sums are additive, so a set maintains them in O(1) per insert or
  removal. Sets are sharded by key prefix (~64 keys per shard, cached
  sums), so any range's fingerprint costs at most a sum over shard
  accumulators or a scan of one small shard.

### Pushed deltas and summaries

- Every 250 ms a node folds its cache's membership journal into its
  published set. It gossips the net change as `CacheSetDelta`: sorted
  adds and removes, varint-gap-coded, at most 2048 keys per message.
  Each delta carries the owner's `seq` and the root fingerprints before
  and after it.
- A receiver applies a delta. If its mirror's root equals the delta's
  base, the mirror stays exact. Otherwise it still applies the delta
  (each key's newest state wins) and schedules a reconciliation session.
  Out-of-order and duplicate deltas are dropped by `seq`.
- Every digest interval a node gossips `CacheSummary`: incarnation,
  `seq`, root fingerprint and count (about 150 bytes). A receiver whose
  mirror does not match schedules a session.
- More than 8 delta messages' worth of change in one tick, such as a
  cache rebuild, is published silently. `seq` advances and a summary
  goes out at once, and peers reconcile.

### Reconciliation sessions

The mirror holder sends `ReconcileRequest` rounds directly to the owner
over QUIC. Each query is a range with the mirror's fingerprint and count
for that range. For each query the owner answers:

- nothing, if the range is equal;
- `Items`, its exact keys in the range, which *replace* the mirror's
  keys there. It does this when it holds at most 32 keys in the range,
  or when the mirror holds none (initial sync) and the keys fit;
- `Children`, the fingerprint and count of each of the 16 children. The
  initiator settles the equal and empty children locally and queries
  the rest next round.

A session starts at the root when no difference is known. It starts
deeper (16 or 256 ranges) when a summary already showed a difference on
a large set. A session ends when every range is settled or after 256
rounds. A session that straddled owner changes leaves a nearly current
mirror and schedules another session. That session only walks the
ranges that changed. Sessions to one peer are at least 500 ms apart,
at most 4 run at once, and each round has a 5 s timeout. No session is
ever on a read or write path.

A liveness sweep every digest interval probes peers whose mirror has not
been confirmed for two intervals. The probe is one root query, about
200 bytes each way when in sync. So a broken gossip mesh costs
freshness, not exactness, and a new peer is synced even before its
first summary arrives.

### Message budget

| Message | Bound |
|---|---|
| `CacheSummary` | ~150 bytes |
| `CacheSetDelta` | ≤ 2048 keys, ≤ ~21 KiB (inside the 31 KiB gossip content limit) |
| `ReconcileRequest` | ≤ 512 ranges, ~19 KiB |
| `ReconcileReply` | ≤ 48 KiB of answers; the owner stops early and reports `processed`, and the rest is re-sent (always under the 64 KiB stream frame) |

Rounds per session are about `log16(n / 32)`, plus one per 48 KiB of
differing keys. An initial sync costs about 7 bytes per key: roughly
7 MB for a 1M-chunk (4 TiB) peer, paged over about 150 rounds.

### Memory

A mirror costs ~8–12 bytes per peer chunk plus 48 bytes per shard. A
peer advertising more than 8M chunks (64 MiB of keys) is not mirrored:
it is treated as unknown, like the bloom mode's 4 MiB-per-peer cap. For
comparison, a bloom mode receiver holds at most 16 KiB × 256 buckets per
peer. Past ~350k chunks per peer (1.4 TiB at 4 MiB) exact mirrors use
more memory than blooms.

## False-positive accounting

A holder that does not serve a requested chunk says why:

| Decline | Meaning | Counted as |
|---|---|---|
| `Busy` | serving budget exhausted, coop off, local read error | `peer_misses` only |
| `RecentlyRemoved` | the holder dropped the chunk within `max(10 s, 2 × digest interval)`, or dropped it and has not published that yet | `peer_stale_misses` (a propagation race) |
| `Absent` | the holder does not have it and did not drop it recently | `peer_false_positives` |

The accounting works the same in both modes, so the counters compare
them fairly. In exact mode an `Absent` also removes that key from the
mirror and schedules a session.

`Busy` is a queueing signal, not a miss. A holder serves at most 4
chunks at a time per requester, and a requester never sends more than
that: a burst (a demand read plus its readahead) waits for one of its 4
slots to that peer. If a holder still answers `Busy` (its global cap is
shared with other requesters), the requester retries with a short
backoff (2 ms doubling to 20 ms). Both waits are bounded by the
selector's S3 ETA, the number the peer was ranked against. Only once
that runs out does the fetch go to S3, which stays the final fallback:
a read never fails without trying it.

## Upload existence hints

Plan 14's peer upload hint (`CONSTELLATION_EXISTENCE_PEER_HINT`) asks
"does any live peer's cache hold this hash?" before an upload, and a hit
selects a confirming HEAD. It now asks the exact mirrors, so it no
longer spends HEADs on bloom false positives. The semantics are
unchanged: a peer's cache membership is a hint, never proof that S3 has
the object. The node's local existence bloom of its own uploads stays a
bloom (`CONSTELLATION_EXISTENCE_BLOOM_BYTES`). It is not exchanged
between peers, it is RSS-capped, and a false positive there costs one
HEAD.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `CONSTELLATION_COOP` | `on` | cooperative cache on/off |
| `CONSTELLATION_COOP_DIGEST` | `exact` | `exact` or `bloom` |
| `CONSTELLATION_DIGEST_INTERVAL_S` | `30` | summary heartbeat and liveness sweep (exact); bloom snapshot rotation (bloom) |
| `CONSTELLATION_DIGEST_TTL_S` | `4 × interval` | a mirror or bloom unconfirmed for this long is not used for lookups |

## Status and metrics

The control API's `status` reply (JSON, under `coop`), `constellation
status`, and `/metrics` expose:

- `digest_mode`
- `peer_false_positives`, `peer_stale_misses`, `peer_misses`
- `digest_bytes_sent`, `digest_bytes_received`, `digest_messages`: all
  digest-plane traffic in either mode, measured as the signed payload
  size. Gossip fan-out is not included.
- `digest_cpu_us`: time spent building, applying and answering digests
- `reconcile_sessions`, `reconcile_rounds`, `reconcile_failures`,
  `reconcile_cpu_us`: the exact-mode sessions this node initiated. The
  CPU counter covers both sides of the rounds this node took part in.
- `local_set_entries`, `peer_set_entries`, `peer_set_bytes`

## Comparing the modes

The harness scenario `coop-digest-compare` runs the same seeded churn
workload in both modes and prints, per mode, fleet digest bytes/s,
false-positive and stale fetches, peer hits, and reconciliation CPU per
round. `coop-exact-churn` asserts the exact-mode invariants on their
own:

- zero false-positive peer fetches;
- real peer hits;
- after quiescence, every node's mirrors hold exactly its peers'
  published sets.
