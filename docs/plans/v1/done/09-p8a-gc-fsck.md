# Plan 09 — Phase 8a: garbage collection + fsck --repair

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–08
committed. Spec: `docs/DESIGN.md` §14 "Garbage Collection" (follow it
rule by rule — it is prescriptive about the race defenses) and §10
(`gc run|verify`, `fsck [--repair]`).

## Part A — Bucket GC

1. **GC lease**: `leases/_gc.json`, reusing `LeaseStore` unchanged
   (it already takes a partition string; `_gc` is just another name —
   assert the lease keeper does not entangle it with the partition
   map).
2. **Deref tracking (continuous, every node)**: while applying log
   records (local mutation AND foreign replay), record when a chunk's
   last reference disappears: a `deref (chunk_hash PK, deref_seq,
   deref_unix_ms)` table maintained incrementally — on
   `WriteManifest`/`snap_delete`/unlink-driven manifest drops, diff
   old vs new referenced hash sets (manifests are in the replica;
   decode and diff). A re-reference deletes the deref row. This makes
   the candidate set a local indexed query (DESIGN: "no bucket LIST
   for reference GC"). Backfill: a one-time full-replica scan
   populates the table on first mount with the new code (kv flag).
3. **Reference sweep (periodic, under the GC lease)**:
   `constellation gc run` + a daemon timer
   (`CONSTELLATION_GC_INTERVAL_S`, default daily; tests use seconds).
   Candidates: deref older than `gc.horizon`
   (`CONSTELLATION_GC_HORIZON_S`, default 7 days), MINUS the
   exemptions:
   - offline-designation exemption (deref under a path with an active
     designation),
   - open-handle holds: read `holds/*` from the bucket (authoritative
     claim; the prefix is usually empty),
   - snapshot/clone roots: any hash reachable from `snaps/` trees
     (use the plan-06 tree walker; cache the reachable set per snap
     root hash — it is immutable).
   Also collected in the same pass: log segments older than the
   newest checkpoint minus a retention floor
   (`CONSTELLATION_LOG_RETENTION_SEGMENTS`, default 128), and
   superseded checkpoints (keep the newest two).
4. **Condemned-list handshake**: before deleting chunks, the GC
   holder writes `gc/condemned.json` `{epoch, hashes, published_ms}`
   (CAS) + gossip `CondemnedPublished`, then waits ONE full lease TTL.
   Every writer re-reads the condemned pointer at its own lease
   renewal (wire into the LeaseKeeper renew path) and treats
   condemned hashes as absent for dedup: the chunk-put path re-uploads
   instead of skipping (idempotent PUT resurrects; the GC holder must
   re-check existence-timestamps or simply re-verify deref before the
   actual DELETE — implement re-check).
5. **Orphan sweep (on-demand)**: `gc run --orphans` — bucket LIST of
   `chunks/` vs the replica's known+condemned set; unknown keys older
   than the horizon are deleted. Guarded by the same lease +
   condemned handshake.
6. **Auditability**: every deletion appends `{key, rule, evidence,
   ts}` to a GC journal object (`gc/journal/<ts>.json`, append =
   write-new); `gc verify` runs mark-only and prints what would be
   collected and why.

## Part B — fsck [--repair]

`constellation fsck [--repair] --s3 ...` — offline-capable checker
(works against the bucket + optional local replica). Corruption
classes (each: detect always, repair under `--repair`, report
machine-readably):

1. Dangling manifest references (chunk missing from bucket):
   repair = re-upload from local cache if present, else record loss
   (list affected files, mark them; never silently truncate).
2. Orphan chunks (present, unreferenced, past horizon): repair =
   delete via the GC path (respecting holds/condemned).
3. Half-committed xpart rename pairs (plan 01's abort rule): repair =
   append the abort record.
4. Torn/invalid segment or checkpoint objects (decode failure):
   repair = quarantine (rename to `quarantine/`) + force-checkpoint
   from a healthy replica if available.
5. Stale lease objects (expired, holder gone): repair = clear
   released flag... no — leave leases alone except `--repair
   --force-release <part>` explicit admin action (fencing safety;
   document).
6. Cache-dir cruft (temp files, partial writes — DESIGN §9): repair =
   delete local cruft.
7. GC journal cross-check: every deletion in the journal must NOT be
   referenced by any current manifest; violations are reported
   loudly (this is the §14 auditability backstop).

Exit code: 0 clean, 1 issues-found (no repair), 2 repaired,
3 unrepairable. The harness scenarios assert codes.

## Tests

Unit: deref diff logic on manifest replace/unlink/snapshot-delete;
horizon + exemption filtering with fake clock; condemned handshake
(writer re-uploads when hash condemned); fsck detectors on
constructed corrupt fixtures (InMemory bucket: delete a chunk out
from under a manifest, write a torn segment, etc.).

Harness scenarios:
- `gc-lifecycle`: write files; delete half; snapshot the rest; run
  GC with tiny horizon/TTL envs; assert deleted files' unique chunks
  are gone from the bucket, snapshot-referenced ones remain, live
  files still read correctly (model-verified), and the GC journal
  records every deletion.
- `gc-dedup-race`: while GC is in its condemned-wait window, a
  writer dedup-writes content whose chunks are condemned: writer
  must re-upload; after GC completes, the file must read correctly
  (this is THE race — DESIGN §14 rule 2).
- `fsck-repair`: corrupt a mounted-then-unmounted bucket three ways
  (delete a referenced chunk while keeping a cache copy, orphan an
  unreferenced chunk, plant a torn segment); `fsck` detects all;
  `fsck --repair` fixes what it can; re-run reports clean; mount and
  model-verify.

## Gates + report

Per CONVENTIONS.md.
