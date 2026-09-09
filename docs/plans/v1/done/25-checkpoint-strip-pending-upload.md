# Plan 25 — Strip node-local upload state from cluster checkpoints

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context: checkpoints are
cluster-shared metadata snapshots that a fresh node restores via
`shipper::bootstrap` (`docs/explanation/DESIGN.md` durability /
bootstrap sections as they describe checkpoint+log rebuild). Code
context: `crates/meta/src/replay.rs` (`SqliteMeta::snapshot`),
`crates/cli/src/shipper.rs` (`bootstrap`, checkpoint writers),
`crates/meta/src/sqlite.rs` (`pending_upload` table,
`set_manifest_dirty` / `add_pending_upload`), `crates/cli/src/main.rs`
(`upload_dirty_chunks`, `run_managed_sync_round`).

Builds on plan 07 (`pending_upload` as the durable not-yet-uploaded set)
and plan 08 (streaming write-back). Independent of plans 19 / 23.

**Incident that motivates this plan.** On a new E2E filesystem, only
node 1 wrote (rsync, write-back). Node 2 joined as a fresh member, only
read. Within seconds of bootstrap it had ~21k `pending_upload` rows,
an empty cache, and an endless ERROR loop:

```text
pending upload chunk missing from local cache (unrecoverable content);
leaving the pending row and refusing to ship
```

Live forensics: every pending row's `ino >> 40 == 1` (writer prefix);
zero rows for node 2; laptop `meta.db` birth time matched
`restored metadata checkpoint seq=1220`; journal/scratch empty. Gossip
/ E2E decrypt did **not** enroll the rows — foreign replay never
touches `pending_upload`. The rows came from the checkpoint.

## Goal

Make cluster checkpoints carry only **replicated** metadata. A fresh
node that restores a checkpoint taken mid-write-back on another node
must not inherit that node's upload obligations, must not open a
continuation epoch because of them, and must not spam multi-GB logs
retrying uploads it can never perform.

## Root cause (do not relitigate)

`SqliteMeta::snapshot()` already strips node-local identity and
ephemeral write state:

- `DELETE FROM journal`
- drop `scratch_*` / `shadow`
- `DELETE FROM kv` for `node_id` / `node_prefix` / `next_ino` /
  `applied_seq*` / `part_of/%`

It does **not** strip `pending_upload`. That table is node-local by
design (plan 07: "this node's not-yet-uploaded set"; foreign
`apply_foreign` / `WriteManifest` replay never inserts into it). But
checkpoints are cluster-shared. Bootstrap writes the snapshot wholesale
into a new `meta.db` (`shipper::bootstrap`), so the joiner inherits the
writer's unresolved upload queue with a fresh empty cache.

`upload_dirty_chunks` then requires `cache.get(&hash)` before any S3
probe, logs `ERROR` per missing hash, leaves the row, and refuses to
ship — every sync round, forever. That also keeps draining "failed",
which interacts badly with continuation-epoch / skip-ship logic.

## Settled decisions

Do not relitigate these.

- **Primary fix: strip in `snapshot()`.** Add
  `DELETE FROM pending_upload` next to the existing journal/scratch/kv
  cleans. Document in the function's comment that `pending_upload` is
  node-local upload obligation and must never leave the writing node
  via a cluster checkpoint. This is the permanent fix for every
  checkpoint written after this plan.
- **Defense for already-published bad checkpoints.** Existing S3
  checkpoints (including this incident's seq=1220) still contain the
  table. After bootstrap restores a snapshot — and on any mount that
  opens an existing `meta.db` — clear pending rows that cannot belong
  to this node:
  - **Bootstrap path:** clear `pending_upload` unconditionally once the
    replica is fully built (after snapshot restore *and* log replay —
    see step 2 for why a single post-replay clear is sufficient). A
    brand-new replica has never written local content; any pending rows
    in the restored DB are definitionally foreign.
  - **Existing-db mount path (self-heal):** once `node_prefix` is known,
    delete `pending_upload` rows whose `ino >> 40 != node_prefix`.
    Those rows are another node's obligation (or pre-prefix garbage).
    Keep same-prefix rows: those are this node's real crash-recovery
    backlog and must still drain.
- **Do not change the local torn-disk contract for same-prefix rows.**
  Plan 07's rule stands: a same-prefix pending row with no cache bytes
  is unrecoverable local content — leave the row, do not ship. What
  changes is the **logging**: one structured WARN (or ERROR) per
  distinct hash with a suppressed-repeat counter / rate limit, not
  one ERROR line per hash per sync round. The multi-GB log was the
  operational damage; the refuse-to-ship behaviour for true local
  loss stays.
- **Do not ack inherited rows by probing S3 and deleting them in the
  drain loop as the primary fix.** That would paper over the leak and
  turn every sync round into tens of thousands of HEADs on a poisoned
  joiner. Strip at snapshot + clear at bootstrap / foreign-prefix
  self-heal instead. Optional later: a one-shot `fsck`/`doctor`
  cleanup; out of scope here.
- **Audit other node-local tables in the same pass.** Confirm whether
  `pin`, `atime_journal`, open `epochs` / `reintegration` rows, or any
  other table that is documented node-local is also leaking through
  `snapshot()`. Strip anything that is node-local and not meaningful
  for a fresh replica. If a table is ambiguous, prefer stripping and
  note the call in PROGRESS.md rather than re-poisoning joiners.
  Replicated namespace tables (`inode`, `dentry`, `xattr`, `snapshot`,
  `chunk_ref`, `deref`, `partition`, …) stay.

## Out of scope

- Redesigning `pending_upload` schema or moving upload obligation into
  the log.
- Changing write-back / eager-seal enrollment on the writer.
- Changing E2E gossip sealing/decrypt (ruled out as a cause).
- Auto-healing already-running poisoned daemons without remount (ops
  can `umount` + remount after this lands; self-heal on mount covers
  them).
- Editing `docs/explanation/DESIGN.md` — record any contradiction in
  PROGRESS.md.

## Steps

### Step 1 — Strip `pending_upload` (and other node-local tables) in `snapshot()`

In `crates/meta/src/replay.rs` `SqliteMeta::snapshot`, after the
existing journal/scratch/kv deletes and before reading the file bytes:

```sql
DELETE FROM pending_upload;
-- plus any other node-local tables identified by the audit
```

Update the doc comment on `snapshot` to list what is stripped and why
(`pending_upload` = this node's upload queue; not cluster state).

Add / extend a unit test on `snapshot()` that:

1. Creates inodes, calls `set_manifest_dirty` so `pending_upload` is
   non-empty (and, if pins/atime_journal are also stripped, seed those).
2. Takes a snapshot, opens it as a new DB.
3. Asserts `pending_uploads()` is empty on the restored DB while inode
   manifests / namespace rows are intact.
4. Asserts identity kv keys are still absent (existing invariant).

### Step 2 — Bootstrap clears pending rows from restored snapshots

In `crates/cli/src/shipper.rs` `bootstrap`, clear the table once after
the replica is fully built — i.e. after `SqliteMeta::open` and after
the segment-replay loop, just before the final `tracing::info!`. A
single unconditional clear is correct and unambiguous: foreign replay
(`apply_records` / `apply_foreign`) never inserts into `pending_upload`
(stated in root cause), so there is no window where replay could add a
row that a pre-replay clear would miss. It is also a safe no-op on the
`from_seq == 0` path where no checkpoint existed and the DB was created
empty.

```text
meta.clear_pending_uploads()?;  // new helper, or raw DELETE FROM pending_upload
```

This makes already-published poisoned checkpoints safe for every new
joiner without requiring writers to re-checkpoint first. `bootstrap`
runs before `set_node_prefix` in `node_runtime` (the DB does not exist
yet, so no prefix is known here) — this is why the bootstrap clear is
unconditional rather than prefix-scoped; the prefix-scoped self-heal in
step 3 covers the *existing-db* remount path.

### Step 3 — Mount self-heal for foreign-prefix pending rows

When a node finishes claiming `node_id` / `set_node_prefix` on an
existing state dir (the non-bootstrap remount path in `node_runtime` —
in `crates/cli/src/main.rs` this is the `meta.set_node_prefix(node_id)?`
call around the `NodeRuntime::start` mount wiring, which runs on every
mount, bootstrap or not), delete pending rows whose inode prefix is not
this node. Put the helper on `SqliteMeta`, e.g.
`purge_foreign_pending_uploads(prefix) -> u64` (rows deleted), and log
an info line if `deleted > 0`.

Express the prefix split with the shared `INO_PREFIX_SHIFT` constant
(currently 40), not a hardcoded literal, so the code tracks the schema:

```text
-- rows whose high bits are not this node's prefix
DELETE FROM pending_upload
 WHERE (ino >> {INO_PREFIX_SHIFT}) != ?prefix;
```

Two correctness notes for that DELETE:

- `ino` is a signed SQLite `INTEGER`. This is safe because inode numbers
  never set the sign bit — `set_node_prefix` rejects any prefix
  `>= 1 << (64 - INO_PREFIX_SHIFT)`, so `ino >> 40` is a small
  non-negative value and the comparison is well-defined. Do not widen
  the prefix domain without revisiting this.
- Root and other pre-prefix/system inodes carry prefix 0 and will not
  equal a fresh node's nonzero prefix, but they never have
  `pending_upload` rows (only locally written file content does), so
  scoping the DELETE to `pending_upload` alone leaves shared namespace
  state untouched. Do **not** generalize this prefix filter to any
  replicated table.

Same-prefix rows stay for plan-07 crash recovery.

### Step 4 — Rate-limit the missing-cache ERROR in `upload_dirty_chunks`

In `crates/cli/src/main.rs` `upload_dirty_chunks`, when `cache.get`
misses:

- Keep `bail!(...)` / first-error aggregation so the round still fails
  and the journal does not ship over missing local bytes (plan 07).
- Replace the per-hash-per-round `tracing::error!` with a rate-limited
  or once-per-process-per-hash warning, plus a single summary line per
  round (`missing_pending_chunks=N, sample_hash=…`). Goal: a poisoned
  or torn-disk node must not write gigabytes of identical ERROR lines.

No change to the refuse-to-ship semantics for remaining same-prefix
rows.

### Step 5 — Harness regression

Extend or add a harness scenario (preferred name:
`checkpoint-strips-pending-upload`) that fails before this plan and
passes after:

1. Mount writer A. Write enough data under `--write-mode back` that
   `writeback.pending_uploads > 0` while A is still mounted (do **not**
   wait for drain / do not clean-unmount yet), **or** force a
   checkpoint while pending is non-zero if the shipper already
   checkpoints mid-flight (today it does every 32 segments — drive
   enough manifests to trigger one, assert via status that pending > 0
   around checkpoint time).
2. Clean-unmount A only after confirming a checkpoint exists on S3
   that was written while pending was non-zero **or** — simpler and
   deterministic — call whatever test hook / control path exists to
   force a checkpoint while pending > 0. If no control hook exists,
   add a narrow test-only or status-observable path rather than
   sleeping on segment counts; prefer deterministic.
3. Mount fresh node B on a new state dir (bootstrap).
4. Assert immediately (before any local writes on B):
   `B.control_status()["writeback"]["pending_uploads"] == 0`.
5. Assert B can read A's data (model oracle / byte compare) and that
   B's daemon log does not explode with
   `pending upload chunk missing` (e.g. count matching lines over a
   short window == 0).
6. Optional strengthening: while A still has pending > 0, take an
   explicit snapshot via the meta unit-test path in step 1 — the
   harness covers the integrated bootstrap path.

Also extend the existing `fresh-node-bootstrap` scenario
(`crates/harness/src/scenarios.rs`, `fn fresh_node_bootstrap`) with a
one-line assertion that
`c1.control_status()?["writeback"]["pending_uploads"].as_u64() == Some(0)`
right after the fresh node `c1.mount()` and before it writes
`bootstrap-proof`, so the invariant stays covered by the default matrix
even if the dedicated scenario is filtered. (Note: the scenario mounts
the fresh node as `c1`; the status-key pattern is already used by other
scenarios in this file, so match their exact accessor.)

### Interaction between steps 2 and 3 on a fresh node

Both the step-2 bootstrap clear and the step-3 prefix-scoped purge run
on a fresh node's first mount (`bootstrap` runs at db-create time, then
`set_node_prefix` runs unconditionally on every mount just after). This
is intended and harmless: after the unconditional bootstrap clear the
purge finds zero rows and is a no-op. Do not try to collapse them into
one — they cover different paths (fresh bootstrap vs. existing-db
remount inheriting a poisoned checkpoint from a *prior* boot).

## Tests (unit)

- `snapshot_strips_pending_upload` (step 1) — mandatory.
- `bootstrap_clears_pending_from_poisoned_checkpoint` — in-process:
  build a DB with pending rows, run the snapshot bytes through the
  same restore+clear sequence bootstrap uses, assert empty pending and
  intact namespace.
- `purge_foreign_pending_uploads_keeps_local_prefix` — seed rows with
  `ino` prefixes 1 and 2; purge for prefix 2; only prefix-1 rows gone.
- Existing `pending_upload_tests::failed_drain_leaves_the_pending_row…`
  and torn-disk commentary remain valid for **same-prefix** rows;
  do not weaken them.

## Definition of done

The standard gates from `CONVENTIONS.md`, all green:

1. `cargo fmt --all` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean.
2. `cargo test --workspace` — zero failures, including the new snapshot /
   purge / bootstrap unit tests.
3. `bash tests/smoke.sh` and `bash tests/integration.sh` — pass.
4. `target/release/harness run` — every scenario PASSED, including
   `checkpoint-strips-pending-upload` (and `fresh-node-bootstrap` with
   the new pending==0 assertion).
5. `docker compose --profile test run --rm compliance` — pjdfstest stays
   a FULL pass.
6. `docs/plans/v1/PROGRESS.md` updated with this milestone's rows.
   Mention in PROGRESS (not DESIGN.md) that checkpoints must not carry
   `pending_upload`. Update `docs/how-to-guides/development/TESTING.md`
   if a new harness scenario is added.

## Reporting back

Per CONVENTIONS.md: files changed; any extra node-local tables stripped
in the audit; gate results; anything deferred (e.g. doctor one-shot
cleanup) with a one-line justification.
