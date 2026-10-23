# Plan 45 — Hard links: dentry attribute copies only for single-name inodes

Read `docs/plans/v1/CONVENTIONS.md` first. Plan 28 §P6 gives every `0x02`
dentry a copy of its inode's attributes, so that a directory listing with
attributes is one range scan, and plan 29 M3c keeps the copies exact: every
`0x01` write rewrites the copy of **each name** of the inode, in the same
transaction. For an inode with one name that is one extra key. For an inode
with *n* names every `link`, `unlink`, `rename`, `chmod`, `utimes`,
truncate and write commit costs O(n) key rewrites, and building *n* links is
O(n²): 7.4 ms per `link()` at 2,000 names through the FUSE mount, one
`stress-ng link` bogo-op of 8,192 links and 8,193 unlinks taking 38.7 s and
leaving 773 MB of journal behind. The shape is real (pnpm's content store,
`rsnapshot`/`cp -al` backup trees, `hardlink`/`rdfind` dedup, git-core's ~130
names for one binary).

This plan changes one rule: **a dentry carries the attribute copy only while
its inode is a directory or has exactly one name.** A non-directory with two
or more names has `Remote` dentries (`ino` + `kind`, 9 bytes) and its
attributes live in `0x01` alone. The form of every `0x02` value is a
deterministic function of the inode's `kind` and `nlink`, written in the
same transaction as the `0x01` record that decides it, so every replica
holds the same bytes and the same root hash. A listing of the 99.99 % of
names that are an inode's only name is byte for byte what it is today; a
listing with attributes reads one `0x01` record per hard-linked entry (the
cost `list_frozen` and btrfs, ZFS, CephFS and Lustre readdir-with-attributes
already pay for every entry).

The journal and open-time defect the same research found (a clean close
leaves memtables unflushed, so the next open replays hundreds of MB of
journal) is a separate fix with its own plan of record, not part of this
plan; the hard-link fix only removes the traffic that filled the journals
in minutes.

Rules carried into this plan from the maintainer: consistency and integrity
first, then performance, never one for the other (a trade-off is reported,
not shipped); no backward-compatibility shims and **no version bumps**
(on-disk and published formats change in place; a state directory written
before this plan holds `Full` dentries on hard-linked inodes and is
rebuilt, as plan 40 R2 records for its change); every fix or behaviour
change comes with a test that fails before it (or, for a new feature,
against the naive implementation it guards against); every bench and test
job runs under a timeout; a file-system-specific performance claim is
measured on ext4, xfs, btrfs and tmpfs, and "no regression" means within
noise on each.

## Dependencies

- Plan 28 (§P6's key table, §14.10's measurement of the copy), plan 29
  (M3c's copy maintenance and `dentry_copy_tests`), plan 30 (§M12: `nlink`
  is an additive delta and `ctime` a max-merge, so replay order commutes).
  All committed.
- Plan 40 (READDIRPLUS). Its plus-entries milestone (R5; "P7" in the coder
  spec) is the first live consumer of the copied attributes. **This plan
  lands before it** (decided 2026-10-22, §9), so R5's tests cover both
  dentry forms from the start. Plan 40's R3a/R3b page and cookie work reads
  only `ino` and `kind` and is unaffected either way.
- The xcheckfs stress lane's `stress-ng link` stressor (the evidence, §1).

## 1. History: the copy was priced for nlink 1

Verified against `main` at `7f4f5a1`.

| When | Commit | What |
|---|---|---|
| 2026-09-12 → 09-21 | `95985b2`, `e613afb` (plan 28) | §P6: the `0x02` value is `(ino, kind)` "+ a denormalized attr copy for READDIRPLUS". Its own cost note: "`setattr` writes the inode record *and* `nlink` dentry copies; `nlink` is 1 for all but a rounding error of files". |
| 2026-09-12 → 09-21 | plan 28 §14.10 | Three shapes measured: `copy`, `nocopy` (no attributes in any dentry) and `dentry-auth` (no `0x01` for `nlink` 1). `nocopy` lost because it makes **every** listing a scan plus scattered point reads (12,039 pack reads for a cold `ls -la` of 1,000 entries, against 30.4). `dentry-auth` lost because it doubles `getattr`. "Copy only while the inode has one name" was never measured: it keeps `nocopy`'s penalty on exactly the entries whose copies are expensive. |
| 2026-09-22 | `645a3a9` (plan 29 M3c) | `ns::put_inode_record` becomes the one path for every `0x01` write; it scans the inode's `0x04` reverse index (`links_of`) and rewrites every stale `0x02` copy. `recursive_size` reads `kind` and `size` from the copy. Invariant test `dentry_copy_tests` (`crates/meta/src/store/mod.rs`). |
| 2026-10 | xcheckfs stress lane | `stress-ng --link` finds the O(n²): 0.57 ms per `link()` at 250 names, 3.2 ms at 1,000, 7.4 ms at 2,000 (ext4 through the FUSE mount); one bogo-op 38.7 s, `meta.db` 773 MB in 184 journal files, the next mount over two minutes in "opening meta.db", concurrent creates queueing behind the big transactions (37 ms against 145 µs). The lane carried a longer bound for the stressor. |
| 2026-10-22 | research | the hard-link research (maintainer-accepted the same day): the scaling is the copy loop and nothing else; seven candidate designs; this plan is its §4 and §5. |

No commit ever made the copy optional. The two places that read it for
more than `ino` and `kind` are the engine's `lookup` (which then re-reads
`0x01` anyway, §2) and, once plan 40 R5 lands, plus entries.

## 2. What the code does today

Verified on `7f4f5a1`; the readers are those of research §2.2.

**Writers of `0x02` values** (every one must produce the right form, §4.2):

| Writer | Where | Today |
|---|---|---|
| `ns::put_inode_record` | `crates/meta/src/store/ns.rs` | writes `0x01`, then for **every** name in `links_of(ino)` rewrites a stale copy (`get_dentry_record` per name, decode, compare). Reached from `put_inode`, `put_inode_keeping_xattrs`, `put_spilled_xattr`, `misc::bump_nlink_touching`, `misc::touch_times_tx`, `setattr`. |
| `ns::put_dentry` | `ns.rs` | a new name: `0x02` from the attrs passed in, plus `0x04`. Callers: `insert_new_node`, `publish_file`, `link`, both `exchange_in_tx` swaps, replay's `insert_node`/`apply_link`/`apply_clone`, `clone_tree`, scratch. |
| `rename_in_tx`, `replay::apply_rename` | `writes.rs`, `replay.rs` | the moved name's `0x02` is `DentryRecord::new(ino, src.attrs)`, the **old copy**, then `bump_file_nlink_tx(…, 0, t)` (a ctime touch) rewrites every copy. |
| `replay::apply_setattr` | `replay.rs` | its own `ns_insert` of `0x01` followed by its **own** O(n) copy loop, bypassing `put_inode_record`. |
| `replay::apply_write_manifest` | `replay.rs` | `put_inode_keeping_xattrs` (which already rewrites every copy) then a second, redundant O(n) loop. |
| `Meta::scratch_set_manifest` | `store/scratch.rs` | the same redundant loop over the scratch keyspace. |

**Readers of the copy:**

| Reader | Fields used | Verified |
|---|---|---|
| `MetaStore::lookup` (`writes.rs`) | all (`FileAttr`) | the engine's `lookup` (`view/ops.rs`) reads `current_attr(ino)` from `0x01` under the write shard afterwards: the copy's attributes are decoded and discarded. Other callers (`confine.rs`, `create.rs`, `prune.rs`, `recovery.rs`) use `ino` or the attributes of a settled entry. |
| `readdir_entries`, `subtree_manifests`, `parents_of` | `ino`, `kind` | `DentryRecord::ino_and_kind` reads the first 9 bytes; works on both forms unchanged. |
| plan 40 R5 plus entries | all | not landed. `meta.lookup(dir, name)` under the shard with the pending overlay. |
| `Meta::recursive_size` (`reads.rs`) | `kind`, `size` | decodes the whole dentry. Counts a hard-linked file once per name. |
| `snapwalk::dentries` (`engine/src/snapwalk.rs`) | `ino`, `kind`, `nlink > 1` | only asks whether the file is hard linked (to read it once, count it per name). |
| `TreeReader::lookup`/`readdir` (`engine/src/mtree_read.rs`) | `kind` (`attrs` through `TreeEntry`) | `snapshot.rs` (path walk, manifest walk), `snapacct/verify.rs` use `kind`; `list_frozen` already reads `inode_row` per child. |
| `scratch_lookup` (`store/scratch.rs`) | all | scratch is private and never has a second name. |
| `mtree::record::leaf_agg` | none | `0x02` contributes nothing to aggregates, by design. |
| bootstrap `load_tree` | none | copies `0x02` values verbatim. |
| the published tree | none | `mtree_publish` carries `ns` values verbatim; the root hash covers the bytes. |
| sim oracle `mask_times` (`authority/tests/sim/check.rs`), `commit_probe` example | decode | must tolerate `Remote`. |
| `dentry_copy_tests`, `mtree/tests/keys.rs` | all | the invariant tests; become form-aware. |

Nothing in the live FUSE path reads packs: a node holds a full replica, so
"12,039 pack reads for a cold `ls -la`" describes the partial-replica reader
that only snapshot browsing uses, and that path already pays one inode read
per child.

## 3. Evidence (the workstation, meta level, release, no FUSE)

`crates/meta/tests/hardlink_perf.rs` (research test, kept as an `#[ignore]`d
measurement by H0): one file in one directory, *n* `link()`s, then `setattr`
and a manifest commit at *n*, a warm listing, `unlink` of every name, then
2,000 single-name create/setattr/manifest/unlink. "Variant (a)" is the
research's one-`if` patch (skip the copy loop for a non-directory with
`nlink > 1`).

| | today (n up to 4,096) | variant (a) (n up to 8,192) |
|---|---:|---:|
| `link()` p50 at 250 / 500 / 1,000 / 2,000 / 4,000 names | 0.54 / 1.47 / 3.53 / 10.8 / 25.0 ms | 0.018 ms flat |
| `unlink()` p50 at 4,000 / 2,000 / 1,000 names | 25.0 / 11.5 / 5.0 ms | 0.02–0.04 ms |
| `setattr` / manifest commit at *n* | 25.7 / 25.2 ms (4,096) | 0.012 / 0.015 ms (8,192) |
| warm `readdir` right after the burst | 29 µs per entry | 0.3 µs per entry |
| warm per-entry read of the copy / of `0x01` | 1.86 / 0.60 µs | 0.59 / 0.50 µs |

Three readings. The scaling is the copy loop and nothing else (the ratio at
4,000 names is ~1,300×). A burst leaves a version pile in fjall's memtable
that slows reads 50–100× (the "concurrent creates queue" effect); variant
(a) has none. And a `0x01` point read costs what a copy read costs, warm:
~0.5 µs per hard-linked entry is the price of `Remote`.

The S3 log is not affected: `LogRecord::Link` is O(1); the other replicas pay
the O(n) again when they replay it (same helpers), and the published tree
edits *n*+5 keys per link.

## 4. Decisions and design

### 4.1 The rule

A `0x02` value has two forms, told apart by length:

- **Full** (57 B: `ino(8)` + `attrs(49)`): written when the inode is a
  directory, or a non-directory with `nlink <= 1`.
- **Remote** (9 B: `ino(8)` + `kind(1)`): written when a non-directory has
  `nlink >= 2`.

(The research text says 65 B; `ATTRS_LEN` is 49 since the `rdev` change, so
`DENTRY_LEN` is 57 and a `Remote` saves 48 B per extra name.) `T = 1`
is the threshold: a copy exists iff the inode has exactly one name. The
form is one function of the `0x01` record, `DentryRecord::for_inode(ino,
&attrs)` (`crates/mtree/src/record.rs`), used by every writer.

`DentryRecord` becomes `{ ino, kind, attrs: Option<Attrs> }`: `decode` takes
9 bytes as `Remote`, 57 as `Full`, anything else is a `RecordError`.
`ino_and_kind` reads the first 9 bytes of either, so plain `readdir`, plan
40's page scans and bulk loading are untouched. `leaf_agg` keeps counting
`0x02` as nothing.

### 4.2 Writes

`ns::put_inode_record(…, ino, record, before: Option<Attrs>)`, where
`before` is the inode's attributes as the caller read them (or `None` for
a record with no earlier state):

1. write `0x01`;
2. `single = carries_copy(record.attrs)`; `was_single = before.map(carries_copy)`;
3. if `single`: scan `links_of` (one name, two for an instant inside an
   `unlink` that bumps before it removes) and rewrite any name whose bytes
   differ from `for_inode(ino, record.attrs)`;
4. else if `was_single != Some(false)` (the 1 → 2 transition, or no
   knowledge): scan and rewrite every name as `Remote`;
5. else nothing: the names are `Remote` and stay so; no scan, no read.

So `link` (1 → 2) rewrites one key, `link`/`unlink` at *n* ≥ 2 → *n*±1 ≥ 2
touch the `0x01`, the new or removed name's `0x02`/`0x04`, and the parent;
`unlink` 2 → 1 materialises the survivor's `Full` copy from the record it
just wrote; every `0x01` write on a multi-name inode is one key. Callers
that already hold the previous record pass `Some(rec.attrs)`; creates and
clones pass `None` (no names yet, the scan is empty); `put_dentry` and the
renames write `for_inode(ino, &inode_record.attrs)` from the **inode**, never
from an old dentry copy. A debug assertion cross-checks `before` against the
stored record, so a caller handing over a stale `before` fails in tests
instead of leaving a stale `Full` copy behind.

Replay uses the same helpers: `apply_setattr` drops its private loop and
calls `put_inode_record`; `apply_write_manifest` and `scratch_set_manifest`
drop their redundant loops; `apply_rename`, `apply_link`, `apply_exchange`
and `evict_dentry` already route through `bump_file_nlink_tx`, so they
inherit the transitions. Where a transaction removes a name and then
changes `nlink` (rename over a target), the removal comes first, so the
2 → 1 materialisation does not write a copy it removes in the same commit.

### 4.3 Reads

- `MetaStore::lookup` keeps returning `FileAttr`: a `Full` entry answers
  from the copy; a `Remote` entry falls back to one `0x01` point read
  (decided, §9). The engine's `lookup` reads `current_attr` afterwards
  anyway, so the live path's cost is unchanged.
- Plan 40 R5 calls `meta.lookup`; a hard-linked entry costs one `0x01`
  point read under the shard, as decided. The counter `Meta::inode_reads`
  (test-visible) counts `0x01` reads made on behalf of a dentry, so the
  gate "a listing of single-name entries reads none" is a test.
- `recursive_size`: `Full` file entries from the scan; `Remote` file
  entries through one `0x01` read per distinct inode (a map in the walk),
  counted once per name as today.
- `snapwalk::dentries`: `Remote` ⇒ hard linked, which is all it asks.
- `TreeReader` (`TreeEntry { name, ino, kind, attrs: Option<Attrs> }`):
  `snapshot.rs` and `snapacct/verify.rs` use `kind`.
- `scratch_lookup`: scratch files have one name, so entries are `Full`;
  a `Remote` there falls back to the scratch inode record.

### 4.4 Integrity and convergence

- The value of every `0x02` key is a function of the namespace:
  `Remote(ino, kind)` iff the named inode is a non-directory with
  `0x01.nlink >= 2`, else `Full(ino, 0x01.attrs)`. `nlink` is in the
  namespace, so two replicas with equal namespaces hold equal `0x02` bytes
  and equal root hashes. Nothing is refreshed later or in the background:
  both forms are written in the same transaction as the `0x01` that decides
  them (plan 28 §P6's argument, unchanged).
- Plan 30 §M12 commutativity: `nlink` is an additive delta and `ctime` a
  max-merge; the form is derived from the result of the delta in the same
  transaction, so any replay order that reaches the same `nlink` reaches the
  same form. Speculation rollback restores before-images key by key
  (`spec::restore_key_tx`), which covers the rewritten dentries.
- Kernel-visible semantics are unchanged: `getattr` and `lookup` answer from
  `0x01`; plus entries (plan 40) answer what `lookup` answers; no reader
  ever answers from a field a `Remote` entry lacks.
- **fsck** reports every violation of the form (`TreeReader::check_dentry_forms`,
  run by `fsck::check_metadata_tree_in` against the head commit): a
  `Full` value whose inode is a non-directory with `nlink >= 2`, a `Full`
  value that differs from its inode's attributes, a `Remote` value whose
  inode is a directory or has `nlink <= 1`, a `Remote` value whose kind
  differs from the inode's, an entry whose inode is missing, and a value of
  neither length. Class `dentry_form`, not repairable by fsck (a repair is
  a rebuild from the log).
- `dentry_copy_tests` becomes `every_mutation_keeps_each_dentry_in_the_right_form`:
  form-aware, extended with the transitions 1 → 2 → 3 → 2 → 1 → 0, a rename
  over a hard-linked target, an exchange of two hard-linked files, `setattr`
  and a manifest commit at *n* = 3, each locally and on replay.

### 4.5 Costs after the change

- `link`, `unlink`, `rename`, `setattr`, `write` commit, `chmod`, `utimes`
  on an inode with *n* names: 1–6 keys, O(1). `stress-ng link`'s 8,192 links
  and unlinks become ~16 k times the *n* = 1 cost.
- `ls -la`, `find`, `git status`, `du` on trees without hard links:
  byte for byte the same values, the same code path, **zero** extra reads.
- The same on hard-link trees: + one `0x01` point read per hard-linked entry
  (warm ~0.5 µs; cold one block), deduplicated per inode by `du`.
- Space: 48 B less per extra name.

### 4.6 Rejected alternatives

Copy for the first *K* names by `0x04` order, a CephFS-style primary dentry
(min or max `0x04` key), a copy without `nlink`/`ctime`, and a copy
threshold `T = 4` were costed in the research (§3): each keeps an O(K) or
election path in `link`, `unlink`, `rename` and replay for a listing win on
a handful of names, or fails the priority-1 gate (a listing of the
single-name tree must not change). `dentry-auth` stays off the table
(§14.10). Coalescing in the log changes nothing: the log is already O(1).

## 5. Milestones

Each milestone ends green on `cargo fmt`, `cargo clippy --workspace
--all-targets -- -D warnings` and the `meta`, `mtree`, `engine` and
`authority` lib tests; the authority simulation suite runs at the end of H3,
and the full CONVENTIONS gates at the end of the plan (§8). Every job runs
under a timeout.

- **H0 — Baseline.** Meta level (done locally, recorded in PROGRESS):
  `crates/meta/tests/hardlink_perf.rs`, an `#[ignore]`d release measurement
  of the research's scenario, run A/B/B/A against the binary built before H1.
  Box (*pending, box*): `bench/hardlink/` (`run.sh`, `RESULTS.md`) through
  dev-fuse on ext4, xfs, btrfs and tmpfs: `link()`/`unlink()` p50 at
  250/500/1,000/2,000/8,192 names; `chmod` and a 4 KiB `write`+`close` at
  *n* = 1 and 2,000; one `stress-ng --link 1 --link-ops 1` bogo-op's wall
  time, `meta.db` bytes and `.jnl` count after it and the remount time;
  cold and warm `ls -la`, `find`, `du -s` of (i) a 1,000-file directory with
  `nlink` 1, (ii) a pnpm-like tree (10 k store files, 10 names each across
  1,000 package directories of 100), (iii) an rsnapshot-like tree (7
  `cp -al` generations of 50 k files); concurrent `create` latency during a
  link burst. Nothing else changes.
- **H1 — `mtree::record`: two dentry forms, every reader, and fsck.**
  `DentryRecord { ino, kind, attrs: Option<Attrs> }`, `new`/`remote`/
  `for_inode`/`carries_copy`, `DENTRY_REMOTE_LEN`, `decode` by length, the
  pinned-encoding test (`the_dentry_encodings_are_pinned`: 9 and 57),
  `RecordError::BadDentryLength`. The type change reaches every consumer, so
  this commit also carries **all readers**, correct for both forms while
  nothing yet writes `Remote`: `MetaStore::lookup` (the `0x01` fallback),
  `Meta::inode_reads`, `recursive_size` (one read per distinct inode),
  `scratch_lookup`/`scratch_rename`, `TreeEntry { kind, attrs: Option }`,
  `snapwalk::dentries` (`Remote` ⇒ hard linked), `snapshot.rs` and
  `snapacct/verify.rs` (they use `kind`), the sim oracle's `mask_times` and
  the `commit_probe` example. `mtree/tests/keys.rs`: the model builder
  writes `Remote` for every file with a second link and checks the form
  from its own dentries; the aggregate test still counts `0x01` only
  (**fail-before**: a builder that writes `Full` on a hard link fails the
  form check, `the_form_check_rejects_…`). `TreeReader::check_dentry_forms`
  and fsck's `dentry_form` issue, with a corpus holding one of each
  violation (`fsck_reports_a_full_copy_on_a_hard_link`,
  `fsck_reports_a_remote_on_a_single_name`, `fsck_reports_a_stale_full_copy`,
  `fsck_reports_a_remote_directory_and_a_wrong_kind`,
  `fsck_reports_a_dangling_dentry_and_a_value_of_no_form`,
  `fsck_accepts_both_forms_in_the_right_places`). Meta reader tests with
  hand-written `Remote` dentries (`remote_dentry_reader_tests`).
- **H2 — `meta`: the rule and the transitions (writers).**
  `DentryRecord::for_inode` in `put_dentry`, `put_inode_record(…, before)`
  per §4.2, both renames, both exchanges, replay's `apply_*`/`evict_dentry`/
  `apply_setattr`/`apply_write_manifest`, scratch, clone. Tests
  (`crates/meta`):
  - `link_cost_is_flat_in_the_link_count` (**fail-before**): a `link` and
    an `unlink` at *n* = 500 change at most 6 `ns` keys (today *n*+5);
    `chmod`, a chown and a manifest commit at *n* = 500 change exactly the
    inode's own key (today *n*+1);
  - `every_transition_keeps_each_dentry_in_the_right_form_locally_and_on_replay`
    (`tests/hardlink.rs`: 1 → 2 → 3 → 2 → 1 → 0, a manifest commit, a chmod
    and an xattr at three names, a rename, an exchange of two hard-linked
    files, a rename over a hard-linked target, each step checked on the node
    and on a replica that replays the log batch by batch) and the in-crate
    `every_mutation_keeps_each_dentry_in_the_right_form_locally_and_on_replay`
    (plan 29's long scenario, form-aware); **fail-before**: the naive skip of
    the copy loop leaves a stale `Full` copy after the 1 → 2 transition;
  - `the_first_link_rewrites_the_one_existing_name_and_no_other`,
    `unlink_to_one_name_materialises_the_copy`,
    `lookup_of_a_remote_entry_equals_getattr`,
    `a_rename_over_a_hard_linked_target_keeps_the_form`,
    `an_exchange_of_two_hard_linked_files_keeps_the_form`;
  - `tests/dirty.rs` gains `link` at two names and `unlink` from three
    (the transition rewrites are dirty-marked); `tests/speculation.rs`
    gains `a_stranded_run_of_links_and_unlinks_restores_each_dentrys_form`
    (rollback across both transitions, byte for byte);
  - `replicas_converge_on_the_dentry_bytes` (a replica replaying the whole
    log holds the same `0x02` and `0x04` bytes);
  - `recursive_size_of_a_hard_link_tree_reads_each_inode_once` equals the sum
    over names and reads each inode once (the counter).
  Meta-level perf (§7) is measured here against H0's baseline.
- **H3 — Engine tests.** `a_hard_link_publishes_remote_dentries_that_bootstrap_verbatim`
  (`mtree_read`: the names of a hard-linked file publish as remote dentries,
  the reader returns them without attributes, `check_dentry_forms` passes, a
  bootstrap copies every `0x02` and `0x04` value verbatim);
  `a_hard_linked_files_changes_publish_a_constant_number_of_keys`
  (`mtree_publish`: a chmod at three names publishes one key, a link at most
  five); engine tests `a_listing_of_single_name_entries_reads_no_inode` (the
  priority-1 gate, by the `inode_reads` counter, through `View::readdir` and
  `View::lookup` as a plus listing will) and
  `a_listing_of_hard_linked_entries_reads_each_inode_once_per_entry`. The
  snapshot walks read `kind` only; `snapwalk`'s existing hard-link tests
  (`hardlinks_weigh_once_per_name_in_the_subtree` and the nested-subtree one)
  now run on remote dentries, which is their test of the `Remote` ⇒ hard
  linked rule.
  Plan 40 R5, when it lands after this, adds
  `plus_entries_of_hard_links_are_what_lookup_answers`.
- **H4 — Harness and xcheckfs.** Scenario `hardlink-burst`: 8,192 links then
  unlinks of one file on node A while node B replays; both roots equal;
  `ls -la` on B equals A; `meta.db` under 64 MB on both; `du` equal. The
  xcheckfs stress lane **keeps `--link 1`** in its mix with the normal bound
  (decided, §9). *Pending, box.*
- **H5 — Docs.** `DESIGN.md`'s statement of the dentry value forms is
  proposed text recorded in PROGRESS (CONVENTIONS rule 5), plan 28 §P6's
  dated pointer to this plan (done with the plan), `docs/reference`'s
  listing-cost table, PROGRESS.

## 6. Tests that must fail before

| Test | Fails against |
|---|---|
| `link_cost_is_flat_in_the_link_count` | today's `put_inode_record` (*n*+5 keys) |
| `every_mutation_keeps_each_dentry_in_the_right_form` | the naive "skip the loop when `nlink > 1`" (stale `Full` after 1 → 2; no `Full` after 2 → 1) |
| `the_dentry_encodings_are_pinned`, `fsck_reports_*` | `DentryRecord` of one form |
| keys.rs form check | a model builder writing `Full` on a hard link |
| `a_listing_of_single_name_entries_reads_no_inode` | a design that drops the copy for every entry (plan 28's `nocopy`) |
| `a_listing_of_hard_linked_entries_reads_each_inode_once_per_entry` | a `recursive_size`/listing that re-reads per name |
| replica convergence | a form decided by something other than `nlink` (e.g. the `0x04` count at write time) |

## 7. Perf gates (meta level, release, A/B/B/A, local; four file systems *pending, box*)

- `link()`/`unlink()` p50 at 2,000 names ≤ 1.5× the p50 at *n* = 1
  (today ~13×).
- `chmod` and a manifest commit at *n* = 2,000 ≤ 1.5× the *n* = 1 cost.
- nlink-1 `create`, `setattr`, `set_manifest`, `unlink`: within ±5 % of H0.
- A warm listing of an nlink-1 directory (readdir plus a `lookup` per entry,
  what plus entries do) reads zero `0x01` records (`Meta::inode_reads`).
- Box (*pending*): one `stress-ng link` bogo-op ≤ 5 s through dev-fuse,
  `meta.db` ≤ 64 MB after it, remount ≤ 2 s; cold and warm `ls -la`,
  `find`, `du` of the nlink-1 directory within noise of H0 on all four file
  systems; pnpm-like and rsnapshot-like `ls -la` warm within 2× of H0, cold
  reported, not gated; concurrent `create` p99 ≤ 1 ms during a link burst
  (today 37 ms).

## 8. Exit criteria

- [x] H0 baseline recorded at meta level (2026-10-22, PROGRESS "Meta level,
      before and after"); the box numbers *pending*.
- [x] `DentryRecord` two forms, every reader, fsck `dentry_form` check, the
      form tests (H1).
- [x] The rule and transitions in every writer, local and replay;
      `link_cost_is_flat_in_the_link_count` and the form-aware transition
      tests green, failing before (9 of 9); meta perf gates met (H2).
- [x] Engine tests, the `inode_reads` gate tests (H3); the authority
      simulation suite green.
- [ ] `hardlink-burst` and the xcheckfs `--link 1` lane (H4, *pending, box*).
- [ ] Docs and PROGRESS (H5).
- [ ] Every CONVENTIONS gate green; pjdfstest 8798/8798 (`link` and `unlink`
      are in its suites).

## 9. Decided by the maintainer (2026-10-22)

The research's questions (research §8), answered the
day it was accepted:

- **Q1, the copy threshold:** `T = 1`: the dentry copy only while a
  non-directory has one name.
- **Q2, shape:** (a): no primary copy, no election; `Remote` for every name
  once there are two.
- **Q3, what a `Remote` entry holds:** `ino` + `kind` (9 B), told apart from
  `Full` by length.
- **Q4, READDIRPLUS entries for hard-linked names:** one `0x01` point read
  per hard-linked entry (a plus entry answers what `lookup` answers).
- **Q5, ordering against plan 40:** this plan lands before plan 40's plus
  entries (R5).
- **Q7, `MetaStore::lookup`'s return:** keeps `FileAttr`, falling back to
  `0x01` for a `Remote` entry; a `lookup_entry` returning `Option<Attrs>` is
  a separate micro-optimisation, taken only if a profile shows the decode.
- **Q8, the xcheckfs stress lane:** keeps `--link 1` in its mix with its
  normal bound once H2 lands.
- **Q6, the open-time fix** (flush at a clean close, a smaller journal
  bound): a separate piece of work, not part of this plan.

Open, decided by measurement or later:

- Box numbers for the four file systems (H0, §7). If the pnpm-like warm
  `ls -la` is beyond 2× the H0 number, the options are the research's
  partial copy (c′) or a batched `0x01` read per page; neither is part of
  this plan.
- A state directory written before this plan holds `Full` dentries on
  hard-linked inodes; it is rebuilt (accepted: no backward compatibility).
  fsck reports it as `dentry_form`.

## 10. Implementation notes (2026-10-22)

What differs from the text above, found while building it:

- **The type change forces the readers into H1.** `DentryRecord::attrs` is an
  `Option`, so every consumer stops compiling in the commit that changes it;
  H1 therefore carries all readers (§5), H2 the writers, H3 the engine tests.
  Between H1 and H2 nothing writes the remote form, so H1 is a no-op for a
  running system and its tests write remote dentries by hand.
- **`before` is `Option<Attrs>`, not `Option<&InodeRecord>`.** `Attrs` is
  `Copy`, so a caller that mutates its record in place (`setattr`, the
  `nlink` bumps) keeps the earlier attributes with one assignment.
- **A rename over a hard-linked target removes the name first**, then changes
  `nlink`, so the 2 → 1 materialisation writes the survivor's copy once and
  never the removed name's. `unlink`, `publish_file` and replay's
  `evict_dentry` already did.
- **Replay had private copies of the loop.** `apply_setattr` wrote `0x01` and
  looped over the names itself; `apply_write_manifest` and
  `scratch_set_manifest` looped again after `put_inode_keeping_xattrs` had
  already done it. All three now go through `put_inode_record` only.
- **The meta-level measurement gates on a steady state.** `link_steady_at_N`
  and `unlink_steady_at_N` (a name linked and unlinked again at *N* names) are
  the numbers the 1.5× gate is read from; the burst windows are reported
  beside them. The first 100 `unlink`s after a 2,000-link burst
  (`unlink_p50_first_100`) cost what the steady state costs (16.5 µs against
  16.2 µs). An earlier reading of 57-100 µs for that window was the
  measurement's: its checkpoint at *n* held one sample, the very first
  `unlink` after the listing, now reported apart as
  `unlink_first_after_burst` (58-89 µs, one operation, not a window).
- **fsck's check is one `0x01` point read per dentry.** It streams (nothing but
  the violations is kept, 100 reported in full), and it reads the head
  commit's tree, so it costs the namespace's size in point reads on top of
  `verify`. A one-entry cache of the last inode read would save the reads of
  adjacent hard-linked names; not done.
- **`engine` lib tests on the workstation:** the five `view::owner_fence_tests`
  parking tests fail in a full `--lib` run on `origin/main` (7f4f5a1 plus
  documents) exactly as on this branch, and pass alone; not touched here.
- `DENTRY_LEN` is 57, not the research's 65 (§4.1).

## 11. PROGRESS.md

Add `## Plan 45 — Hard links: dentry attribute copies only for single-name
inodes` in the established style (intro, "What landed" table, "Unchanged on
purpose", "Gates"), with these rows:

| Item | State | Where |
|---|---|---|
| Baseline (H0) | meta level landed, box pending | `crates/meta/tests/hardlink_perf.rs`; `bench/hardlink/` |
| Two dentry forms, every reader, fsck check (H1) | | `crates/mtree/src/record.rs`, `crates/meta/src/store/{ns,writes,reads,scratch}.rs`, `crates/engine/src/{mtree_read,snapwalk,snapshot,fsck}.rs` |
| The rule and transitions (H2) | | `crates/meta/src/store/{ns,writes,misc,reads,scratch}.rs`, `crates/meta/src/replay.rs` |
| Engine tests (H3) | | `crates/engine/src/{mtree_read,mtree_publish}.rs`, `view/vfs_tests.rs` |
| Harness + xcheckfs (H4) | pending, box | `crates/harness/src/scenarios.rs` |
| Docs (H5) | | `docs/plans/v1/done/28-…` §P6 note, `docs/reference` |

## 12. Integrity items of the follow-ups review (2026-10-23)

- **O1: a root's append of a delegate's transaction ignores its own live
  shadow of that rid.** `Meta::apply_delegate_tx` dedups a rid by
  `completed` only, and a requester's shadow never writes `completed`. When
  the root forwarded an op to the delegate, installed the reply as a shadow,
  and shadowed a later op of the same names before the delegate's stream
  brought the first back, the append re-applies the first op's records on
  top of the later one. `install_streamed` adopts such a shadow in place by
  rid; this path has no such rule. Exchange's "swapped already" skip
  (`LogRecord::Exchange { ino, new_ino }`) catches the immediate repeat that
  `shared-dir-split+links` seed 498 hit, not a second exchange of the pair
  in between; and a rename has the same shape without any exchange (shadow
  `rename a b`, shadow `create a`, append `rename a b`: the new `a` replaces
  `b`, the renamed file is gone from the root's replica; not yet traced
  whether the later appends or the shadows' retirement repair it). Fix direction: in `apply_delegate_tx`, a live shadow of
  the rid is rolled back with what follows it, the transaction is journaled
  in its place, and the later rows are redone (the segment path's rewind,
  for a `Local` insertion), or the shadow is adopted as the journaled row.
  Failing tests, ignored: `o1_a_root_appending_*` in
  `crates/meta/tests/speculation.rs`.

  **Closed (2026-10-23).** The root cause: a shadow is keyed by rid in
  `spec_live` but writes no `completed` row, and `apply_delegate_tx`
  looked for the rid only in `completed`; nothing on that path asked the
  speculation log, so the root could not tell its own installed copy of
  the op from a new op on the same names. Every other apply path keys by
  rid already: a segment rolls back a shadow it completes and applies in
  its place (`apply_segment_rows`), the pre-S3 stream adopts it
  (`install_streamed`), a rebuild queues it for replay by rid, which
  `completed` dedups. Now `apply_delegate_tx` takes the shadow out by rid
  (`spec::unwind_shadow_for_append_tx`): when nothing installed after it
  touches its keys, its before-images alone are restored and the
  transaction is journaled at the end; otherwise every row from the
  shadow on is rolled back, the journaled rows (and hints, tailed rows)
  are redone, the transaction is journaled, and the later shadows are
  redone on top. That is the log's order at a root, and it keeps
  captured rows' `spec_seq`s rising with journal order. The shadow is
  dropped, not stranded. The same change fixes the immediate repeat's
  other face: appended over its shadow, the row's before-images were the
  shadow's state, so the root's publish overlay put the unshipped op into
  a commit ("not the log prefix"). Plan 43 C3 does not prescribe this:
  its D4 supersedes a shadow the takeover tail carries, C3-1's verbatim
  append dedups by origin; C3-1's `append_verbatim_tx` must keep this
  unwind. Tests: the two above, un-ignored, plus
  `o1_a_root_appending_over_its_own_shadow_takes_the_shadows_place` and
  `o1_a_root_appending_under_its_own_rows_keeps_the_journal_order`; sim
  `regression_a_root_appending_over_its_own_shadow_keeps_the_log_prefix`
  (the new `+stalebase` variant, seven seeds that failed before). The
  exchange's skip by state stays as replay's tolerance.

  **The stream's side of the same class (review of O1, 2026-10-23;
  `delegated-backup+links+stalebase` seed 138).** `install_streamed` adopted a
  live shadow of the transaction's rid as the transaction's effect without
  checking that the shadow's records were the transaction's. A replay of a
  stranded op answered by the holder's dedup (`Accepted` with no records: the
  rid was done there, at a position the replica lacks; the reply's `base` is
  `None`, so this is not the knob's doing, only rarer without it) installed a
  shadow that applied nothing; adopted in place, the stream's create was never
  applied, the rename streamed after it found no entry, and the segment skipped
  both as confirmed rows. Now a shadow whose effect records are not the
  transaction's is rolled back from its seq and the transaction's records are
  applied in its place, still as this node's own op (`same_effect`,
  `insert_streamed_tx` redoes the adopted row from the transaction's records).
  Test: `a_streamed_transaction_does_not_adopt_a_shadow_that_lacks_its_effect`.
  The O1 unwind itself never trusted the shadow's effect (it restores the
  before-images and applies the records).

  **M2, the refusal's side (closed 2026-10-23).** The `[Refused { rid }]`
  branch of `apply_delegate_tx` journaled a delegate's refusal without
  looking for a live shadow of that rid; the refusal's ship then retired the
  shadow in place (`ack_rows_tx` retires root shadows by the `Completed` and
  `Refused` rows a ship carries), so the refused op's effect stayed in the
  root's replica and reached its publishes. Reached when the root forwarded
  the op, installed the delegate's acceptance, and `repair drop-held` on the
  delegate let the transaction go (it was held back, so never streamed) and
  journaled the refusal. Introduced with the branch (`578ff9cf`); `2ff95df5`
  gave the segment path its rule for a refused rid's shadow ("a dropped op's
  shadow kept for good") but not this path. Now the refusal's transaction
  strands the shadow by rid first (`Meta::journal_refusal_over_shadow`:
  rolled back, the rows after it redone, its queued replay dropped, as the
  segment path does), before the refusal's own `Local` bracket, the same
  place O1's unwind runs. Stranded, not dropped: unlike an append, the
  refusal carries no effect to take the shadow's place, and the rollback is
  reported. Test:
  `m2_a_root_appending_a_refusal_strands_its_own_shadow_of_the_rid`.
- **O2: the `+links` sim links and exchanges within one directory only**
  (`with_links`' `dir_of` filter). The overlap rules are by inode and so
  directory-blind, but no sweep exercises a cross-directory link, rename or
  exchange of a hard-linked inode, nor one across a delegation boundary.

Closed in the same review: the holder's reply base now counts the
multi-name inodes an op takes a name from (`Core::reply_base`,
`Replica::linked_victims`), and an `Exists` hint is refused when live
speculation captured the inode it names (`live_speculation_touches`) or the
inode is an orphan here. Before, an unshipped `link a b` let a peer's
`unlink a` be installed as the inode's last name on the requester, and a
hint `Create { b, X }` then reset `X` to an empty file that reads of `b`
were answered from.

## Sources

- The hard-link research of 2026-10-22 (the document this plan implements; its section numbers are cited as "research §n").
- Plan 28 §P6 and §14.10; plan 29 M3c; plan 30 §M12; plan 40 §2, §4.5.
- `crates/meta/src/store/ns.rs` (`put_inode_record`, `put_dentry`),
  `crates/meta/src/replay.rs`, `crates/mtree/src/record.rs`,
  `crates/engine/src/{mtree_read,snapwalk,fsck}.rs`.
