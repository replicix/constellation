# Plan 05 — Phase 5: cooperative cache

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–04c
committed. Spec: `docs/DESIGN.md` §7 "Cooperative cache" and
"Latency-adaptive source selection". Roadmap exit: web-fleet scenario
meets TTFB targets; latency-matrix behavior converges (S3-near nodes
go direct, remote peers serve each other).

Chunks are content-addressed and self-verifying (blake3 hash = name),
so peer serving needs no trust and no invalidation — the security
model stays: allowlisted peers only.

## Step 1 — Digest gossip

- Each node maintains a bloom filter over the chunk hashes in its
  local cache (~10 bits/entry, target FPR ~1%; size dynamically from
  the entry count, rebuild on resize). Use a small, well-maintained
  bloom crate or implement double-hashing over blake3 outputs (~40
  lines, no dep) — prefer the latter, document the parameters.
- Publish via gossip: `CacheDigest { node_id, generation, bloom }`
  full snapshots every `CONSTELLATION_DIGEST_INTERVAL_S` (default 30)
  or after ±10% churn; incremental `CacheDigestDelta { adds: [hash],
  removals_generation }` between snapshots (keep it simple: deltas
  carry adds only; removals just raise FPR until the next snapshot).
- Store peers' digests locally (in-memory, per peer, with generation
  and received_at). A local miss consults them with ZERO per-request
  messages.

## Step 2 — Peer chunk serving

- New direct-message pair on the net layer: `ChunkRequest { hash }` /
  `ChunkResponse { hash, found: bool, data }` (stream the payload —
  chunks are up to the FS chunk size, e.g. 4 MiB; use an iroh
  bidirectional stream, length-prefixed, not a gossip message).
- Serving side: look up the cache; if present and clean/pinned, send;
  never serve dirty (unpublished) chunks — the manifest referencing
  them is not visible remotely yet anyway, but assert it.
- Receiving side verifies the blake3 hash before caching/using
  (existing verification path — route the peer fetch through the same
  code the S3 fetch uses).
- Concurrency limit per peer (e.g. 4 concurrent serves) and a global
  serving budget so a hot node does not melt; excess requests get
  `found: false` (the requester falls back).

## Step 3 — Source selection

Implement the DESIGN.md §7 policy, kept honest and simple:

- Per-source (S3 + each connected peer) EWMAs: TTFB, goodput, error
  rate — updated from every real transfer; peer RTT free from iroh's
  QUIC stats.
- Score = predicted `ETA = TTFB_ewma + size/goodput_ewma + penalty`
  (penalty: recent errors, queue depth). Choose min; ~20% hysteresis
  (switch only when the challenger beats the incumbent by >20%).
- Hedging: if first byte has not arrived by the source's P95 TTFB
  estimate, fire the next-best source in parallel; first byte wins,
  loser canceled. Cap: one hedge per fetch.
- Wire into the chunk fetch path (`fusefs.rs` fetch_chunk +
  prefetcher): the fetch function becomes source-selecting. S3
  remains the source of last resort and correctness anchor. A peer
  "hit" that fails hash verification is treated as an error signal
  for that peer AND falls back to S3 transparently.

## Step 4 — Observability

`StatusReport.coop: { peer_hits, peer_misses, s3_fetches,
hedges_fired, bytes_served_to_peers, per_source: [{id, ttfb_ms_ewma,
goodput_mbps_ewma, err_rate}] }`. Show in `status`. These numbers are
what the scenarios assert.

## Step 5 — Tests

Unit: bloom parameters (FPR at target load), digest delta application,
source-selection scoring incl. hysteresis and hedging trigger (fake
clock + scripted transfer outcomes; keep the policy in a pure module
`cli/src/sources.rs` so it tests without I/O).

Harness scenarios:
- `coop-cache-hit`: A reads a large file (populates its cache); B
  (cold cache) reads the same file while S3 latency is toxiproxied to
  200 ms and A—B latency is real-localhost (~0); assert via control
  API that B fetched most chunks from A (peer_hits >> s3_fetches) and
  the read completed well under the all-S3 bound.
- `coop-fallback`: same, but A is SIGSTOPped mid-read (or its serving
  disabled via env kill-switch): B must complete correctly from S3;
  hash-verified content; no scenario failure. Assert hedges/eviction
  of the dead peer from selection.
- `web-fleet`: three nodes; one writes content, two "web servers"
  read it repeatedly with cold caches under S3 latency; assert
  aggregate S3 fetch count stays near the unique-chunk count
  (each chunk pulled from S3 ~once, then peer-served) — the DESIGN
  web-fleet shape.

## Out of scope

Rendezvous hashing (noted as composable alternative — leave a comment
hook), cross-region simulation beyond toxiproxy latency shaping,
`/metrics` Prometheus endpoint (phase 7 pulls observability together).

## Gates + report

Per CONVENTIONS.md. Paste the `coop-cache-hit` and `web-fleet` fetch
counters into the report and PROGRESS.md.
