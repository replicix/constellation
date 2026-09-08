# Plan 15 — Phase 8d: concurrent snapshot and clone churn oracle

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plan 14 (8c)
on `main`. Spec: `docs/DESIGN.md` §13 (snapshots are frozen trees;
delete does not touch other snapshots, clones, or the live tree;
clones are ordinary writable subtrees sharing data chunks;
`.constellation` is lookup-only). Do not edit DESIGN.md.

Existing harness scenarios (`snapshot-lifecycle`, `clone-workflow`,
`snapshot-mount`) are small scripted proofs. This plan adds a
**seeded, concurrent, multi-round** churn that keeps an out-of-core
expected state, freezes it at each snapshot, clones those snapshots
into writable trees, mutates origin and clones independently, and
checks isolation when snapshots or clones are deleted.

## Shape

One harness scenario `snapshot-churn` (clones live in the same
scenario so a single audit trail covers the mixed history) plus
`crates/harness/src/snapchurn.rs`. Reuse `Client`, `S3Env`,
`snapshot_create` / `snapshot_delete` / `clone_snapshot`. Add
`rusqlite` (workspace, bundled) to the harness crate.

Default CI scale must finish in a few minutes: 4 workers, 3 rounds,
~30 ops/worker/round, files mostly small with occasional multi-chunk.
Scale with env (document where read):

- `CONSTELLATION_SNAPCHURN_WORKERS` (default 4)
- `CONSTELLATION_SNAPCHURN_ROUNDS` (default 3)
- `CONSTELLATION_SNAPCHURN_OPS` (ops per worker per round, default 30)
- `CONSTELLATION_SNAPCHURN_AUDIT` (directory for the trail; default
  the scenario temp dir, printed on failure)

**Writable roots.** The origin live tree is `/tree`. Each clone is
an ordinary subtree `/c{n}` produced by
`clone /tree@<snap> /c{n}` (or clone-of-clone from `/c{n}@<snap>`
at least once in the run). Workers own **disjoint** prefixes inside
**one assigned writable root** (`/tree/w0`, `/c1/w2`, …) so
concurrent POSIX writes do not have an undefined last-writer.
Snapshot create/delete, clone create/delete, and verifies happen
only at **quiesce barriers** (all workers joined, all files closed).

At least one origin file, one snapshot, and one clone must still
exist after the last mutation round (do not drain until cleanup).

## Oracle (expected state)

Do **not** store file payloads. Store per path:

- `root` (mount-absolute prefix: `/tree`, `/c1`, …)
- path relative to that root
- kind: file / dir / symlink
- size
- blake3 of content (files) or symlink target
- mtime_ns from `symlink_metadata` after the op that last changed
  that path
- mode bits if cheap (`stat.st_mode` user bits)

**SQLite is the source of truth.** WAL on disk is the out-of-core
path. Schema sketch:

- `live(root, path, kind, size, hash, mtime_ns, mode)` PK `(root,path)`
- `snap(id, name, src_root, created_unix_ms)` — `src_root` is the
  subtree that was snapshotted (`/tree` or `/c1`)
- `snap_entry(id, path, kind, size, hash, mtime_ns, mode)` — copy of
  that root's `live` rows at create time
- `clone(id, dest_root, src_snap_id)` — dest is a live root whose
  rows start as a copy of `snap_entry` and then diverge
- indexes on `snap_entry(id)`, `live(root)`

`snapshot create <src>@name` copies that root's live rows into
`snap_entry` after the daemon succeeds. `snapshot delete` drops that
id's snap rows only. `clone <src>@name <dest>` inserts a `clone` row
and copies `snap_entry` into `live` under `dest_root`. Deleting a
clone is recursive unlink of `dest_root` plus dropping those live
rows; snapshots taken *of* that clone are still independent frozen
trees until explicitly deleted.

Unit-test the oracle without FUSE: freeze isolation, clone copy then
diverge, delete origin snapshot without changing clone live rows,
delete clone without changing origin live or other snaps.

## Generator and audit trail

Seeded `StdRng` (harness `--seed`). Ops against a worker prefix
inside its assigned root: create, overwrite, append, truncate,
mkdir, rename (inside the prefix), unlink, rmdir, symlink, chmod if
you already stat mode. After success: upsert/delete the live row
(hash the bytes written; `stat` for mtime).

Workers are partitioned across **currently live writable roots**
(origin + clones) so clones are mutated, not only snapshotted.

**Audit trail** (JSONL, flush every line):

```json
{"t_ms":12,"worker":1,"op":"create","root":"/c1","path":"w1/f-3","args":{...}}
```

Include snapshot create/delete, clone create/delete, quiesce, and
verify. Header line records seed, workers, rounds.

`harness run` gains `--replay <trail.jsonl>` and `--replay-no-sleep`.
`--replay` is valid for `snapshot-churn` only. Replay does not use
the RNG; it sleeps `t_ms` gaps unless `--replay-no-sleep`.

## Per-round lifecycle

Each round, after workers join:

1. **Snapshot** the origin (every round except when at a cap of ~4
   origin snaps). Periodically also snapshot a clone (`/c{n}@cr{round}`).
2. **Clone** from a living origin snapshot into a new `/c{n}` when
   fewer than ~3 clones exist. At least one clone-of-clone (snapshot a
   clone, clone that snapshot) must occur in the full run.
3. **Mutate isolation:** after a clone exists, a later round must
   write different bytes on the same relative path in origin vs clone
   (workers already have disjoint prefixes *per root*; force one
   explicit pair of overlapping relative paths, e.g. both write
   `marker`, with different content). Verify the two hashes differ and
   the source snapshot is unchanged.
4. **Delete a snapshot** if more than 2 exist (not the newest, and
   not the last remaining source of a clone if that would make the
   next clone step impossible — clones must survive origin-snap
   delete). Verify:
   - deleted name is gone / ESTALE
   - remaining snapshots match `snap_entry`
   - clones whose source snap was deleted still match their `live`
     rows (DESIGN: deleting a snapshot never breaks a clone)
   - origin live still matches
5. **Delete a clone** if more than 1 exists. Verify dest is gone,
   origin and all snapshots unchanged, remaining clones unchanged.
6. At least once per round, verify **every** remaining snapshot,
   **every** remaining clone live tree, and the origin live tree.

Frozen-view verify: walk
`mnt/<src>/.constellation/snapshot/<name>/...` via explicit lookup
(`.constellation` hidden from `readdir`). Content hashes, types,
sizes, symlink targets, mtimes (record frozen mtime from `stat` on
the frozen path right after create if FUSE truncates). Extra
frozen entries not in `snap_entry` fail.

Live verify walks each writable root against `live` for that root,
ignoring `.constellation` in `readdir`.

## Final quiesce, then cleanup

After the last round: **at least one origin file, one snapshot, and
one clone remain**. Verify origin + clones + snapshots.

Cleanup (on the trail):

1. Delete remaining snapshots (views gone, `snapshot ls` empty).
2. Recursively remove remaining clones and `/tree`. Oracle live empty.
3. Mount has no user entries under those roots.
4. **Replica:** daemon sqlite `snapshot` table empty; no user inodes
   under `/tree` or `/c*`.
5. **S3:** LIST `snaps/` empty. `gc run --orphans` with the short
   horizon knobs `gc-lifecycle` uses. After GC, `chunks/` must not
   still hold objects only reachable from deleted files/snaps/clones.
   Allowlist remaining coordination objects (`nodes/`, `leases/`,
   `log/`, checkpoints, `meta.json`). A leftover snap object or a
   chunk still referenced by a deleted snapshot/clone is a failure.

Shared data chunks: origin and clone may share hashes while both
live; GC must not collect a chunk still referenced by either. The
cleanup order (delete snaps, then both trees, then GC) makes the
unreferenced set complete.

## CLI

```
harness run snapshot-churn --seed 42
harness run snapshot-churn --replay /tmp/snapchurn.jsonl
harness run snapshot-churn --replay /tmp/snapchurn.jsonl --replay-no-sleep
```

Print the audit path at the start of a live run.

## Tests

- Oracle unit tests: freeze/copy; clone then diverge; delete origin
  snapshot, clone rows intact; delete clone, origin + other snaps
  intact; clone-of-clone copy isolation.
- Trail serde round-trip of a fixture that includes clone ops.
- Harness `snapshot-churn` in `SCENARIOS`, must PASS in
  `harness run`. If time allows, immediately `--replay` the trail
  against a fresh FS; otherwise a unit fixture replay plus docs
  on how to replay.

## Out of scope

- Snapshot mounts / ephemeral `--rw` mounts (already
  `snapshot-mount`).
- E2E. Fault injection (S3 cut, kill -9).
- Implementation changes unless a real bug is found; then fix it
  and add a regression, do not weaken the oracle.

## Gates + report

Per CONVENTIONS.md. Phase 8d table in `docs/PROGRESS.md`,
`docs/TESTING.md` scenario blurb, README plans row. Report
rounds/workers, snapshots and clones created/deleted, verify
count, trail path, whether replay ran, and the origin-vs-clone
marker isolation result.
