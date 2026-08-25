# Plan 03 — Phase 4a: pin/unpin + offline designation

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–02
committed (leases, partitions, P2P). Spec: `docs/DESIGN.md` §7
(pin/unpin), §5.2 (offline designation), §5 availability matrix.
Continuation epochs and reintegration are plan 04 — NOT here.

## Part A — pin/unpin

`constellation pin <path>` / `unpin <path>`: fully cache a subtree
and keep it current.

1. **Cache states**: `fs-core::cache` already models
   `{clean|pinned|dirty}`. Add promotion/demotion APIs and make the
   LRU evictor skip pinned entries (verify it already does; add a
   test).
2. **Pin registry**: pins are per-node, local state (a `pins` table in
   the node's SQLite replica: `path TEXT PRIMARY KEY, pinned_at`).
   They are NOT replicated — each node pins for itself.
3. **Admission check**: at pin time, compute the subtree's total size
   (one indexed query — the `rsize` recursive aggregate if present,
   otherwise walk manifests) and refuse with a clear error if it does
   not fit the cache budget together with existing pins
   (reserve-before-accept, DESIGN.md §9).
4. **Eager fetch**: on pin, background-download every chunk of the
   subtree (bounded concurrency, e.g. 8 parallel fetches, reusing the
   prefetcher's fetch path), marking each pinned.
5. **Push-sync**: when tailing applies a manifest change under a
   pinned path, immediately fetch the new chunks (gossip
   `SegmentPublished` already nudges the sync; hook the pin refresher
   into post-apply). New chunks are pinned, replaced chunks demoted to
   clean (evictable).
6. **CLI + control API**: `constellation pin|unpin <path>
   --state-dir ...` sends a control-API request to the running daemon
   (extend `api::Request` with `Pin{path}`, `Unpin{path}`,
   `ListPins`); `status` shows pinned bytes/count per pin. Errors
   (over budget, path missing) travel back over the API.

## Part B — offline designation

`constellation offline <path>` / `online <path>`: exactly one designee
per path, CAS-enforced, overlap-checked (DESIGN.md §5.2).

1. **Designation objects**: `designations/<hash-of-path>.json` =
   `{v, path, designee: node_id, created_unix_ms, released}` with
   CAS create/swap (reuse the lease-store CAS idioms; a small
   `DesignationStore` in store-s3). Overlap check at creation: list
   existing designations; refuse if any existing path is a prefix of
   the new one or vice versa. Document the TOCTOU window (two racing
   creates of overlapping paths both pass the list check) and close
   it: after CAS-create, re-list; if an overlapping designation with
   an older timestamp exists, self-delete and fail. (Cheap, correct
   enough; note it.)
2. **Designee reachable ⇒ everyone still writes**: while a designation
   exists and the designee is P2P-reachable, other nodes' mutations
   under the path require a short-TTL **delegation** from the
   designee: a signed P2P message `DelegationGrant { path, epoch,
   ttl_ms }` renewed like a mini-lease; every FLUSH of foreign records
   touching the path additionally requires a `FlushAck` from the
   designee before the segment PUT is considered published (the
   designee tails it and acks; invariant: the designee provably holds
   all committed changes under its path). Implement the ack as: after
   the segment PUT, the writer waits (bounded, e.g. 2 s) for the
   designee's ack of that seq before acking its own journal; on
   timeout the write STAYS journaled and visibility is delayed — do
   not fail the user's close() (document the trade-off: DESIGN.md
   says cost ≈ 1 RTT to the designee per foreign flush).
3. **Designee unreachable ⇒ delegations expire**: foreign nodes go
   read-only under the path (mutations return EROFS with a clear log
   line); the designee itself keeps writing regardless of S3 (its
   authority is the designation object, not the lease — the lease
   machinery must treat a designated path as excluded from ordinary
   lease authority: partition leases still order the LOG, so the
   designee's local writes journal under a designation flag and ship
   whenever it can acquire the stream head as usual; if S3 is also
   down, they wait in the journal — reintegration semantics beyond
   the journal backlog are plan 04).
4. **FUSE gating**: mutating ops resolve the innermost designation
   covering the path: designee ⇒ proceed (touch only local checks);
   non-designee with live delegation ⇒ proceed; non-designee without ⇒
   EROFS after the bounded delegation-request attempt.
5. **CLI/API/status**: `offline <path> [--ro]`, `online <path>` via
   control API; `--ro` grants the read guarantee without write
   authority (pin the subtree + designation record marked ro:
   non-designee writes are NOT restricted in --ro mode; it is a pin
   with a promise). `StatusReport.designations`.

## Tests

Unit: pin admission math + LRU-skips-pinned; designation overlap
rejection incl. the TOCTOU close; delegation grant/expiry state
machine over the loopback transport trait from plan 02.

Harness scenarios:
- `pin-follow`: A pins `/data`; B writes new files under `/data`; A's
  cache must contain the new chunks shortly after (assert via control
  API pinned bytes and by cutting S3 THEN reading the new file on A —
  reads must succeed from cache with S3 down).
- `offline-designee-writes`: designate A for `/site`; cut A's P2P and
  S3 entirely (SIGSTOP is not enough — use the toxiproxy cut for S3
  and `CONSTELLATION_P2P=off`... a restart is acceptable);
  A keeps writing under `/site`; B gets EROFS under `/site` and keeps
  writing elsewhere; heal; A's writes ship; model-verify both.
- `offline-delegation`: designate A for `/site`, all healthy; B
  writes under `/site` (delegated, acked); verify write visible on A
  ≤ the P2P bound; kill the designation (`online`), verify normal
  lease flow resumes.

## Gates + report

Per CONVENTIONS.md. The availability matrix rows "designee isolated"
and "node isolated, no designation" are this plan's exit criteria —
quote the matrix in PROGRESS.md and mark which rows are now
demonstrated by which scenario.
