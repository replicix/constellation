# Constellation Roadmap

Phases build strictly on each other; each ends with its TESTING.md layers
green. The control API exists from phase 1 (the CLI is its first client).
Format elements (partitioned log, typed manifest entries, self-describing
objects) are final from phase 1 — later phases add code, never migrations.

## Phase 1 — Single-node FUSE on S3

Mount one bucket from one node: chunk store (compression, self-describing
headers), SQLite metadata store behind the engine trait, metadata log +
snapshots (single partition), LRU cache with reserve-before-accept ENOSPC
handling, prefetcher, `fs create|mount`, `status`, `doctor`, control API
skeleton. Exit: pjdfstest passes; kill -9 + remount recovers; census-scale
import within budget.

## Phase 2 — Second node, close-to-open

Log tailing + replica application, attribute/entry invalidation, dirty
flush protocol, fsync modes. Two nodes mount concurrently with close-to-open
semantics through S3 alone (no P2P yet). Exit: simulation layer green for
two-node histories; git-workflow scenario clean.

## Phase 3 — Leases + P2P fast path

Lease objects with CAS + fencing epochs, automatic partition split/merge,
cross-partition rename, iroh endpoint + registry allowlist + gossip
(invalidation push, lease handoff). Exit: single-authority invariant holds
under simulated partitions; lease transfer ~1 RTT when peers connected.

## Phase 4 — Availability: pin, offline, epochs

`pin/unpin` with eager push-sync; `offline/online` designation with
delegation + ack protocol; continuation epochs with persisted promises;
reintegration paths. Exit: the three-machine scenario suite (TESTING.md §5
items 1–4) passes under fault injection.

## Phase 5 — Cooperative cache

Bloom digests over gossip, peer chunk serving, latency-adaptive source
selection with hedging. Exit: web-fleet scenario meets TTFB targets;
latency-matrix simulations converge.

## Phase 6 — Sharing, snapshots + E2E mode

`share --export` with expiry GC; snapshots + clones (tree objects,
`.constellation/snapshot/` access, subtree/snapshot mounts, ESTALE-on-delete
semantics); optional passphrase E2E mode (keyring, compress-then-encrypt,
keyed addressing). Exit: export lifecycle, snapshot/clone lifecycle, and
E2E mount/recovery tests.

## Phase 7 — Web UI

Embedded UI over the control API: dashboard, peers, file browser, cache,
leases/designations, exports, compression, ops. Exit: UI feature parity
with CLI (same API, verified by shared tests).

## Phase 8 — Hardening

`fsck --repair` for every corruption class, GC (chunks, segments, exports),
xfstests sweep, performance regression gates, packaging (static musl builds,
Linux + macOS). Exit: nightly full matrix green; v1.

## Phase 9 — Secure Automated Crash Reporting Pipeline

Enable automatic capture and centralization of application crashes without exposing customer PII or proprietary source code strings in the client binary.

Implementation Details:
    - Integrate `crash-handler` and `minidump-writer` crates to catch panics and native OS faults.
    - Configure `Cargo.toml` (`debug = true`, `strip = "symbols"`) for server-side symbolication.
    - Set up a central collection backend (Sentry/GlitchTip) with `send_default_pii: false`.
    - Implement a client-side crash-dump staging mechanism to upload `.dmp` files safely upon next application restart.

Acceptance Criteria:
Successful end-to-end telemetry generation where server logs display exact Rust file line numbers, while the distributed binary contains zero plaintext function names or local variable values.

## Deferred (format-reserved)

Slice overlays (random-write workloads), packfiles (tiny-object costs), CDC
chunking, inline-manifest prefix, LMDB engine, partial metadata replicas +
leveled compaction (100M+ files), Windows/WinFsp. Full rationale for the
data-plane items: DECISIONS.md ADR-11.
