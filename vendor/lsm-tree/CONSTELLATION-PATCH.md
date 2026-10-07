# Vendored lsm-tree 3.1.10: no disk I/O under the version lock

This directory is lsm-tree **3.1.10**, fjall's storage engine, exactly as
published on crates.io (checksum `5498808f…1b3e`, upstream commit
`9812163ae144213b6bb576a147fda066e7ab4ab1`; its `src/` is identical to that
commit's). It is used through `[patch.crates-io]` in the workspace
`Cargo.toml` and through a `path` on the vendored fjall's own dependency
(`../fjall/Cargo.toml`), so fjall's standalone test build uses it too. Only
the files a build needs are kept (`src/`, `Cargo.toml`, `README.md`) plus the
license files (`LICENSE-MIT`, `LICENSE-APACHE`). lsm-tree is dual-licensed
MIT OR Apache-2.0, and both texts stay here unchanged. The directory is in
the workspace `exclude` list, like `vendor/fjall`. Unlike `vendor/fuser`,
there is no re-vendoring script (fjall has none either): the changes are
edits in place, each marked `CONSTELLATION PATCH`.

## What is changed

1. **`src/version/super_version.rs`**: `SuperVersions::upgrade_version_unlocked`
   (new), the version change every flush, compaction and `clear` now goes
   through: the new version is persisted with no lock on the version
   history held, then installed under a short write lock.
   `SuperVersions::maintenance` is split into `take_garbage` (bookkeeping,
   under the lock) and `StaleVersions::remove` (dropping the old versions
   and removing their files, after it).
2. **`src/compaction/worker.rs`, `src/compaction/flavour.rs`**: a
   compaction finishes its output files (`CompactionFlavour::finish_files`)
   before it takes any lock, and builds the next version from the latest one
   (`CompactionOutput::next_version`). `do_compaction` works on a copy of the
   latest version instead of holding the read lock through the choice and
   the merge's preparation. `move_tables`, `drop_tables` and the merge commit
   use `upgrade_version_unlocked`.
3. **`src/tree/mod.rs`** (`register_tables`, `clear`), **`src/blob_tree/mod.rs`**
   (`clear`): `upgrade_version_unlocked`; `clear` now holds the compaction
   state mutex, as every other version change does.
4. **`src/version/persist.rs`**: a test-only hook (`SLOW_PERSIST`) that makes
   one tree's version persists slow, for the regression test.
5. **`Cargo.toml`**: a `[lints.rust] warnings = "allow"` table at the end,
   for the same reason as in `vendor/fjall` (a path dependency is not built
   with `--cap-lints allow`).

The vendored fjall carries the matching call-site change (its change 6:
`Keyspace::inner_rotate_memtable` uses `take_garbage` + `remove`), see
`../fjall/CONSTELLATION-PATCH.md`.

## The bug

Under `stress-ng-fs-nodes` the meta store logged
`slow commit: 15433 ms for 16 items (memtables=14707ms)` right after
`Starting major compaction` (`Meta::vacuum_churn` compacts churn keyspaces to
their last level). Delegates' log-apply steps took 0.5–0.8 s behind
compactions, and the authority core waited for the single-writer lock behind
such a transaction.

A commit's `memtables` phase is `Tree::append_entry` for each item, which
takes the tree's version history lock (`RwLock<SuperVersions>`) **for
reading** to find the active memtable. fjall holds the journal writer's mutex
across those inserts, so one tree's wait stalls every commit of the database.
These sections held the same lock **for writing** across disk I/O:

| holder | under the write lock |
|---|---|
| every flush (`register_tables`) | `persist_version`: the version file's fsync, the directory's fsync, the atomic rewrite of `current` (more fsyncs) |
| every compaction commit (merge) | `MultiWriter::finish` (the last output table's index, filter, fsync), `Table::recover` of every output table (open + read its trailer, metadata, index), `persist_version`, version GC (`remove_file` of old version files) |
| trivial moves, drops, `clear` | `persist_version`, version GC |
| fjall's memtable rotation (every keyspace) | version GC: dropping the last super version that references a compaction's inputs runs `Table`'s `Drop`, which **unlinks the input files**; `remove_file` of old version files |

With `std::sync::RwLock` a queued writer also blocks new readers, so a long
read hold (the compaction's choice and preparation, which open every input
table) stalled inserts as soon as a flush or a rotation queued behind it.

Measured with `crates/meta/tests/compaction_stall.rs` (a writer commits a
small transaction every millisecond while a major compaction of a 4M-row,
100–160-table keyspace runs; release build; this host):

| | idle NVMe, 5 runs: worst commit | ext4 on `dm-delay` (fsync ≈ 80 ms), 3 runs: worst commit |
|---|---|---|
| upstream 3.1.10 | 133–138 ms, all in `memtables` | 668–712 ms, all in `memtables` |
| patched | 40–61 µs | 337–351 ms, all in `journal_lock`: fjall's journal rotation by a flush, not the version lock (below) |

The commit p50/p99 are unchanged (4–6 µs / 15–22 µs on NVMe). On the
production host each fsync took seconds (I/O pressure `full` 40–46%), hence
15 s. Instrumenting the lock holders showed every flush holding the write
lock ~11 ms on the idle NVMe (one `persist_version`), and the major
compaction's commit ~113 ms (`consume_writer` 97 ms + `upgrade_version`
16 ms).

## The fix

`SuperVersions::upgrade_version_unlocked(history, serialized, path,
next_version, memtables, …)`:

1. Reads the latest super version (a brief read lock), computes the next
   on-disk `Version` from it, allocates the seqno.
2. `persist_version` with no lock on the history held.
3. Takes the write lock, takes the **latest** super version as of now,
   replaces its `version` and `seqno`, applies `memtables` (a flush releases
   the memtables it flushed; `clear` installs empty ones), appends it, bumps
   `visible_seqno`, and takes the garbage out. The lock is held for a few
   pointer swaps.

The latest on-disk version cannot change between 1 and 3: every version change
holds the tree's compaction state mutex (the `serialized` guard proves it at
the call). Upstream already held it in `register_tables`, the compaction
commits, moves, drops and ingestion; `clear` now holds it too. An `assert`
checks that the version id did not change. What can change in between is the
memtables: a rotation seals the active memtable (`replace_latest_version`),
and inserts land in it. That is why the installed super version is rebuilt
from the one that is latest at install time, not from the one read in step 1.
Version GC only pops from the front and never the latest, so it can run
concurrently.

Lock order is unchanged: compaction state, then version history.
Ingestion (`tree/ingest.rs`, `blob_tree/ingest.rs`) still persists under the
write lock (it allocates its seqno there on purpose). It is rare (bootstrap)
and serialized with the rest by the compaction state mutex.

### Crash safety

Nothing about what reaches disk, or in what order, changes:

- `current` names a version only after that version's file and the
  directory are synced, exactly as before. Versions reach disk in the order
  they are installed (serialized).
- A crash after step 2 and before step 3 is the same as a crash right after
  upstream's `persist_version`: recovery opens the new version.
- Old tables are deleted (`mark_as_deleted`) only after the new version is
  installed, as before. A compaction whose inputs vanished meanwhile
  (`clear`) now deletes its finished output files; upstream left them to
  recovery's orphan cleanup.
- Old version files are removed after the lock is released. One left behind
  by a crash is an orphan that recovery's `cleanup_orphaned_version`
  removes, as before.
- On-disk format: untouched.

## Tests

- `cargo test --manifest-path vendor/lsm-tree/Cargo.toml --all-features`:
  239 unit tests + 23 doctests, among them
  `version::super_version::version_lock_tests::inserts_never_wait_for_a_version_being_persisted`.
  It makes the tree's version persists take 400 ms (`SLOW_PERSIST`) and
  requires every insert during a flush and during a major compaction to take
  under 200 ms (and the data to be intact afterwards). Upstream fails it: an
  insert waited 410 ms for the flush.
- Upstream's integration suite (the `tests/` directory of commit `9812163`,
  not in the published crate) with these sources: 441 passed, 0 failed (as
  with the pristine sources, 440 + the new test).
- fjall 3.1.10's suite (the vendored `src/` in upstream's repository at
  `3adaa50`, `tests/` included) against this lsm-tree: 122 passed, 0 failed
  (also with the registry lsm-tree). The vendored fjall's `--lib` tests: 72
  passed.
- `crates/meta/tests/compaction_stall.rs` (in `cargo test -p
  constellation-meta`): the end-to-end measurement above; its default bound
  (1 s, `LSM_STALL_MAX_MS`) catches a freeze of the reported size without
  being sensitive to a loaded host's scheduling. The deterministic regression
  test is the lsm-tree one. The meta crate's crash and recovery tests pass
  unchanged.

## Dropping the patch

Once upstream persists versions outside the version lock (see
`../ISSUE-lsm-tree.md`, the issue to file):

1. Remove `lsm-tree` from `[patch.crates-io]` and `"vendor/lsm-tree"` from
   `exclude` in the workspace `Cargo.toml`, and the `path` on
   `vendor/fjall/Cargo.toml`'s `lsm-tree` dependency.
2. Re-make fjall's change 6 against the new API, or drop it if upstream's
   `maintenance` no longer drops versions under the caller's lock.
3. Delete `vendor/lsm-tree/`, `cargo update -p lsm-tree`.
4. Keep `crates/meta/tests/compaction_stall.rs`.
