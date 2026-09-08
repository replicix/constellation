# Plan 04 — Phase 4b: continuation epochs + reintegration

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–03
committed. Spec: `docs/DESIGN.md` §5.3 (continuation epochs), §9
failure table ("lease holder vanishes", S3-outage rows), §6 relaxed
mode's "conflicts detected and materialized, never silent". This plan
completes phase 4; the ROADMAP exit criterion is the availability
matrix's S3-down rows plus stranded-branch reintegration.

## Part A — Continuation epochs

When S3 is unreachable but the P2P-connected component contains ALL
write-eligible nodes, the cluster keeps writing (DESIGN.md §5.3).

1. **Write-eligibility roster**: derive from the node registry —
   every registered node that is not marked read-only (registry
   records gain an `ro: bool`, default false, set by a countersigned
   enrollment for RO members; for now a CLI flag on first mount:
   `--read-only-member`). The roster is cached locally and refreshed
   on every registry read; an epoch is only legal if the LIVE P2P
   component ⊇ the full roster minus RO members.
2. **Epoch activation**: when a node's S3 operations fail (the syncer
   already surfaces this) AND the P2P component covers the roster,
   any member may propose `EpochPropose { epoch_id, members, base:
   applied_seq_vector }`; every member signs and **persists the
   promise locally BEFORE activation** (a small `epochs` table:
   epoch_id, members, base vector, promised_at, state). Activation
   requires all members' signed acks (collected by the proposer,
   redistributed). During an active epoch: leases transfer P2P-only
   (the LeaseKeeper treats the epoch as its authority root), writes
   journal locally, nothing ships.
3. **Epoch discipline** (the two safety rules, test both):
   - A member that loses contact with any other member mid-epoch goes
     READ-ONLY immediately (it cannot know whether the departed node
     is writing via S3).
   - A departed member's persisted promise forbids it from taking
     epoch-held leases via S3 (i.e. the ordinary expired-lease
     takeover path must check the local promise table and refuse)
     until the epoch's holders have flushed. Both sides freeze — no
     conflict, by construction.
4. **Epoch end**: S3 returns ⇒ members flush their journals in lease
   order (the current holder of each partition ships first; others
   follow after tailing), then the epoch is marked closed
   (locally; there is no global epoch object — S3 CAS on the log
   itself is the serialization point, epochs only justify who could
   write while it was gone).

## Part B — Reintegration (stranded branches)

Two producers of stranded writes exist today: a deposed lease holder
(M3.1 keeps its journal, marked `lost`), and phase-4 designees /
epoch members whose flushes were delayed. Reintegration turns a
stranded journal into either clean appends or materialized conflicts
— never silent loss, never silent overwrite (DESIGN.md §6 relaxed
mode; §9 "bounded unflushed tail surfaces via reintegration rules").

1. **Reintegration procedure** (runs at mount, and on demand via
   `constellation reintegrate --state-dir ...`):
   - Tail the shared log to head (ordinary sync).
   - Replay the stranded journal records one by one against the CURRENT
     replica state: for each record, decide `clean` (its preconditions
     still hold: parent exists, name state matches what the record
     assumed, file content baseline unchanged) or `conflict`.
   - Clean records are re-journaled (fresh seq, current epoch) and
     ship normally.
   - Conflicting records are **materialized**: the affected file/dir
     is written to `<parent>/.constellation-conflict/<name>@<node>-<ts>`
     (create the conflict dir lazily; it is ordinary namespace content,
     fully visible on every node) and a `tracing::error` + control-API
     counter records it. Manifest-level conflicts (both sides edited
     the same file) materialize the stranded VERSION as the conflict
     file and leave the winner in place.
2. **Deposed-holder unlock**: after successful reintegration the
   `lost` flag clears and the node resumes normal operation (it may
   acquire leases again). Persist the transition.
3. **Ordering**: reintegration itself needs the write lease for the
   affected partitions (it appends records like any writer).
4. **Idempotence + crash safety**: reintegration must be resumable —
   process the stranded journal in order, marking each record's
   disposition in a `reintegration` table within the same SQLite tx
   that re-journals or materializes it. A crash mid-way resumes at the
   first unmarked record.

## Control API / CLI

`StatusReport.reintegration: { stranded_records, conflicts_materialized,
in_progress }`; `StatusReport.epoch: { active, epoch_id, members }`.
CLI: `constellation reintegrate`, `constellation status` showing both.

## Tests

Unit: epoch promise persistence + the two discipline rules (loopback
transport, InMemory store, fake clock where needed); reintegration
dispositions — build small precondition/conflict cases directly
against `SqliteMeta` (create-create same name, edit-vs-edit same
file, edit-vs-delete, parent-deleted) and assert clean/conflict
classification and the materialized layout.

Harness scenarios:
- `continuation-epoch`: two nodes, healthy P2P; cut S3 on BOTH via
  toxiproxy; both keep writing in their subtrees (epoch active —
  assert via control API); heal S3; both flush; model-verify both
  trees on both nodes; assert zero conflicts.
- `epoch-member-lost`: same setup, but SIGSTOP B mid-epoch; A must go
  read-only (writes EROFS) while B is gone; resume B; epoch resumes
  or ends cleanly; heal S3; verify no divergence.
- `deposed-reintegration`: extend the `lease-fencing` scenario's
  ending — after A is deposed with stranded writes, run
  `reintegrate` on A; non-conflicting stranded files appear on B;
  deliberately create one conflict (same path written on B during
  A's freeze) and assert the conflict file materializes on both
  nodes and nothing is lost (both byte-contents present somewhere).

## Gates + report

Per CONVENTIONS.md. This closes phase 4's availability matrix and
reintegration. Permanent membership shrinkage (`constellation leave`)
is plan 05 — NOT here.
