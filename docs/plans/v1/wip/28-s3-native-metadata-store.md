# Plan 28 — S3-native metadata: one versioned Merkle map instead of replica + log + checkpoint

Read `docs/plans/v1/CONVENTIONS.md` first, then plans 26 and 27.

**This plan is a design study, not a work order.** It answers the
question "if we designed a consistent distributed filesystem on S3 from
the ground up, which primitives would we pick?" and it ends with a
recommendation that is deliberately *not* "rewrite everything". Steps 1+
are sketched at plan granularity so the size of the bet is visible; Step 0
is three benchmarks that decide whether the bet is worth taking at all.
Nothing here may be implemented before Step 0 reports.

Spec context: `docs/explanation/DESIGN.md` §4 (metadata plane), §6
(consistency), §12 (DB/FUSE fast paths), §13 (snapshots), §14 (GC);
ADR-2 (S3 CAS is the only arbiter), ADR-3, ADR-5, ADR-9 (SQLite),
ADR-10, ADR-11 (`pack`/`cdc` reservations), ADR-11's deferral reasoning,
ADR-14 (forwarding). Code context:

- `crates/meta/src/sqlite.rs` — the schema this plan replaces as a
  *source of truth* (it survives as a derived index).
- `crates/meta/src/record.rs`, `crates/meta/src/replay.rs` — the 23
  semantic `LogRecord` variants and the convergent-replay rules this
  plan deletes.
- `crates/cli/src/shipper.rs` — `ship_part`, `tail_part`, `checkpoint`,
  `bootstrap`; the segment CAS and GET-next tailer survive almost intact.
- `crates/store-s3/src/log.rs`, `crates/store-s3/src/lease.rs` — the CAS
  primitives that stay.
- `crates/fs-core/src/{tree,manifest,cache}.rs` — `Tree`/`TreeEntry` is
  the ancestor of the node format; `cache.rs` is reused verbatim as the
  node cache.

## 1. Why this plan exists

Plan 27 is the second attempt to bound checkpoint cost, and its design
has a tell: to make a Merkle tree of the namespace publishable
incrementally, it adds `inode.tree_hash` + `inode.tree_dirty`, a
dirty-propagation walk on every mutating path in both `mutate.rs` and
`replay.rs`, a HAMT sharding rule for wide directories, a pack index, a
bulk loader in `meta/src/load.rs`, and a test that asserts
table-by-table equality between the source replica and a replica
rebuilt from the tree.

That is a lot of machinery whose entire purpose is to keep **two
representations of the same truth in agreement**: SQLite rows (which the
filesystem reads) and content-addressed tree blobs (which the bucket
stores). Plan 27 also keeps a **third** representation — the semantic op
log — as the incremental transport, with its own convergence argument
(`TouchSet` suppression, `Atime` max-merge, `xpart_pending` parking,
epoch fencing, contiguity requirement).

Three representations, three conversion paths, three correctness
arguments, and an equality test standing in for a proof. The ground-up
question is whether one representation can do all three jobs. It can.

## 2. What today's stack is, stated precisely

| Job | Today | Cost driver |
|---|---|---|
| Local query | SQLite relational replica | full replica mandatory (ADR-5 blocks partial) |
| Incremental transport | semantic `LogRecord` segments, per-partition seq | replay must be contiguous, ordered, and op-wise idempotent |
| Bounded bootstrap | whole-DB `VACUUM INTO` checkpoint (plan 26), Merkle packs (plan 27) | O(DB), and it exists *only* to license log truncation |
| Linearization | per-partition lease CAS + segment seq CAS | lease is on the correctness path, so writers must serialize through a holder |
| Sharding | partitions = namespace ranges, split/merge as log records | cross-partition rename is a 2PC with a parking table and half-commit recovery rules |
| Verification | `fsck` walks and compares | no single value attests to "the filesystem is this" |

The coupling that hurts most is the last two rows plus the log's
contiguity requirement. Everything else in the system — content
addressing, blake3 identity, S3 CAS as the arbiter, leases, offline
designation, continuation epochs, IAM-as-access-control — is right and
this plan keeps it.

## 3. Reference numbers nothing may regress

From ADR-9, DESIGN §11, and the plan 26 Appendix (2026-09-10):

| Quantity | Value |
|---|---|
| Reference census | 11.9M entries, 1.3 TiB, 20.2M metadata records |
| SQLite replica | 1.60–2.0 GB (79 B/row measured) |
| Dentry lookups | 578K/s; readdir scans 521K/s; DB latency ~2 µs vs ~10 µs FUSE overhead |
| FUSE small-file create | 465–501 µs/file single-threaded |
| S3 RTT floor | ~180 ms HU→AWS, ~15–30 ms same-region, ~35 ms HU→OVH |
| Bootstrap 64 MiB | 3.4 s / 0.26 s / 2.0 s as 64×1 MiB packs, 16-way |
| Whole-DB checkpoint | ~1 GB pre-zstd at census scale |

## 4. The core idea

> **The filesystem's metadata is one ordered key-value map. Represent it
> as a content-addressed, order-independent Merkle map. Then a version of
> the filesystem is a single hash; a commit is a tiny CAS'd object naming
> that hash; the replica, the snapshot, the checkpoint, the diff, and the
> replication delta are all the same artifact.**

Two properties do the work:

1. **Canonical**: the tree's shape is a pure function of the key set, so
   two replicas holding the same logical content have byte-identical
   trees and equal root hashes. Convergence is checked, not argued.
2. **Structurally shared**: a commit that changes *k* keys creates
   O(*k* + depth) new nodes and reuses everything else, so "incremental
   checkpoint" is not a feature — it is the only thing the data structure
   can do.

Everything below follows from those two.

## 5. Primitives

### P1 — Shared state: a prolly tree (probabilistic B-tree / Merkle search tree)

A sorted map from `Vec<u8>` keys to small values, stored as immutable
content-addressed nodes (the structure behind Dolt/Noms; the same idea as
`fs-core::Tree` generalized from "one blob per directory" to "one blob
per key range").

- **Boundaries from keys only.** A node ends after key `k` when
  `u32::from_le_bytes(blake3(k)[0..4]) < u32::MAX / TARGET_ENTRIES`, with
  hard min/max entry clamps. Because the predicate reads the *key* and
  not the value, a `chmod` rewrites leaves along one path but never
  reshapes the tree; because it reads no history, insertion order cannot
  matter. Wide directories need no HAMT special case (plan 27 §Design):
  a 10M-entry directory is simply a long contiguous key range that the
  boundary function splits into ~85k leaves.
- **Node = the existing `Tree` blob, generalized**: magic, level, entry
  count, then either `(key, value)` pairs (leaf) or
  `(first_key, child_hash, subtree_aggregates)` triples (interior).
  Target 8 KiB, zstd'd, hashed with blake3 (keyed in E2E — see P13).
- **Values are small and bounded**: inode attrs + inline manifest
  (≤ 8 chunks, the existing `INLINE_CHUNKS_MAX` rule), dentry target,
  xattr value. Anything above `VALUE_SPILL = 1 KiB` becomes a blob hash,
  exactly as manifests already spill.

Determinism is the load-bearing property and gets a property test:
insert the same 100k random keys in 50 random orders, assert one root
hash; delete-then-reinsert, assert the original hash returns.

### P2 — The one linearization point: a CAS-created commit chain

```
commits/<seq:016x>        # CAS-created (If-None-Match: *), immutable
```

```json
{
  "seq": 4211,
  "parent": 4210,
  "roots": { "0": "<node hash>", "1": "<node hash>", "...": "..." },
  "packs": ["<pack hash>", "..."],
  "author": 7,
  "epoch": 19,
  "agg": { "bytes": 1402331136, "files": 11903221 },
  "intent": { "kind": "batch", "ops": 9812 },
  "unix_ms": 0
}
```

This is `put_segment`'s CAS with a different payload, so plan 26's
GET-next tailer, idle backoff, and pipelined catch-up (Step 4) carry over
unchanged — a follower still discovers the head by probing `seq+1..seq+k`
and still falls back to LIST only in catch-up mode. `HEAD` may exist as
a hint object; it is never authoritative.

Three consequences worth stating separately:

- **There is no checkpoint.** Every commit is a complete filesystem
  state. Plan 26 and 27 both exist to bound the cost of an artifact this
  design does not have.
- **Log retention stops being load-bearing.** Deleting old commit objects
  costs time-travel depth and nothing else; it can never strand a
  bootstrap. The plan 26 Step 0 data-loss bug class (a retention floor
  computed against the wrong partition's vector) cannot be expressed.
- **A node's view is always a version, not a prefix.** DESIGN §6's
  "consistent prefix of the authoritative history" strengthens to
  snapshot isolation at a named root hash, which is what §12's time
  travel, §13's snapshots, and `changes --since` all want anyway.

### P3 — Concurrency: optimistic commit with structural rebase; leases demoted

To commit, a writer holds `parent` = the root it read, a write-set, and a
read-set (the keys whose observed presence/absence its decision depended
on). It CAS-creates `commits/parent+1`. On 412, it re-reads the winner,
computes `diff(parent_roots, winner_roots)` — O(changed nodes), by
descending only where hashes differ — and:

- **disjoint** (`diff ∩ read_set = ∅`): splice its write-set onto the
  winner's tree, bump `parent`, retry. No filesystem work is re-done.
- **overlapping**: re-execute the affected operations against the new
  state, which is what produces the *correct* POSIX answer.

Worked example, `O_EXCL` create of the same name on two nodes: both
commit from root R; A wins at seq N. B's read-set contains
`(parent_ino, name)` observed absent; the diff shows that key now
present; B re-executes and returns `EEXIST`. **Correct with no lease, no
forwarding, and no holder.** Today this requires the partition lease
holder to serialize both writers.

Leases survive, demoted from *correctness* to *policy and batching*:

| Role | Today | Plan 28 |
|---|---|---|
| Exclusive write authority | required for any mutation | not required |
| Batching (write at local speed, publish per batch) | yes | yes — this is now the reason leases exist |
| Fencing a deposed holder | safety | policy guard against a stale holder rebasing a huge batch over hours of newer work |
| Strict-mode `fcntl` ranges, offline designation (§5.2), continuation epochs (§5.3) | yes | unchanged — these are genuine exclusion, not ordering |

Forwarding (ADR-14) also survives as a latency optimization for
hot-directory contention, where re-execution would otherwise thrash. It
is no longer the answer to "two nodes want to write".

### P4 — Sharding: shard the keyspace, not the namespace

`roots` is a map of shard id → root hash. A shard is a fixed slice of the
*key* space (high bits of the key hash, 256 shards, or a prefix-split
ring). Sharding buys parallel fetch, parallel rebuild, parallel diff, and
per-shard partial replicas.

Sharding buys **nothing** for linearization, and must not: there is one
commit pointer naming every shard root, so a commit spanning shards is
still one CAS and therefore still atomic. This is the single largest
structural simplification in the plan:

- `partition`, `PartSplit`, `PartMerge`, autosplit heat tracking,
  per-partition leases, `applied_seq/<part>`, `CheckpointVector`,
  `RenameXpartSrc`/`Dst`/`Abort`, `xpart_pending`, the half-committed-pair
  recovery rule, and `journal_has_xpart_dst` **all disappear**.
- A cross-directory `rename` is three key writes in one commit. Renames
  stay O(1) (ADR-3) and become atomic by construction rather than by
  two-phase protocol.

Throughput check, because one commit pointer sounds like a bottleneck:
one CAS per S3 RTT is 5/s far, ~30/s same-region, and a commit carries a
whole batch (today's `SEGMENT_BATCH` is 10k records). That is 50k–300k
metadata ops/s against a measured per-node FUSE ceiling of ~2,000
creates/s — ten nodes saturating FUSE need ~20k ops/s. The envelope fits
with an order of magnitude to spare, and it fits *better* than today
because losers rebase instead of blocking. If it ever stops fitting, the
escape valve is multiple commit chains with a cross-chain protocol — i.e.
exactly today's partitions, reintroduced only when measurements demand
them rather than on day one.

### P5 — Local storage: no database

SQLite's three jobs split into three primitives that are each much
smaller:

| SQLite job | Replacement |
|---|---|
| Ordered map, transactional | the prolly tree itself (immutable nodes + a root hash) |
| Durability of uncommitted writes | an append-only WAL file (`postcard` records + fsync), truncated at commit |
| Working set in RAM/disk | an in-memory ordered memtable (`BTreeMap`/skiplist) over a content-addressed node cache — **`fs-core::cache` verbatim**, keyed by blake3 like every chunk |

A read resolves memtable → node cache → peer → S3, which is the same
ladder `fs-core::cache` + `coop.rs` already implement for data chunks.
One cache, one eviction policy, one verification path, one P2P transfer
protocol for metadata and data alike.

Cold-read structure at census scale (worked in Appendix A): every non-leaf
node of a 23.8M-key filesystem totals **~10 MiB**, so the entire interior
is permanently RAM-resident and a lookup is one leaf access — cached,
zero I/O; cold, one ranged GET into a pack. At the ADR-5 stretch target
the interior no longer fits for free: Appendix B works 100M files with
xattrs at **~200 MiB of interior**, of which only the top two levels
(~1.2 MiB) are always resident and the level above the leaves is
LRU-cached, making a cold lookup two node reads instead of one. That is
still the shape that makes a partial replica (ADR-5's 100M+ path) a
property of the format rather than a project — and at that scale a
partial replica is not optional for anyone, since a full one is ~34 GiB
of leaves whether the engine is a tree or SQLite.

What is genuinely lost is SQL: `fsck`/`inspect` ergonomics, and §12's
"instant search" indexes. Neither needs SQLite back on the correctness
path, but — unlike the rest of this plan — they are recovered as
*node-local* state rather than as shared state:

- **A derived, node-local index** — SQLite or DuckDB — may be
  materialized from the tree for tooling and for `constellation find`.
  It is disposable and rebuildable, never authoritative, and it is
  rebuilt by a range scan (65 M dirents/s resident per §14.2, so a full
  rebuild at census scale takes seconds).

An earlier draft of this plan put secondary indexes **inside** the hashed
tree (`0x10 | name | ino`, `0x11 | mtime_be | ino`, `0x20 | chunk_hash |
ino`) and sold them as "replicate for free, attested by the root hash".
That is **retracted** — see §P6's *indexes stay out of the tree* rule.
The reasoning: an index over a *mutable* field needs a delete keyed by
the field's **old** value, and old values are scattered across the index
range, so a mass attribute change dirties one leaf per key. §14.4
measured the scattered shape at 758×–2,460× today's log bytes with only
the two primary keys in play; an in-tree mtime index would roughly double
that, and a `chunk_hash`-keyed one is worse still because hash order is
deliberately random. The lazy-cleanup escape does not exist either:
anything maintained opportunistically diverges between replicas and
destroys convergence-by-hash, which is the property the whole design
rests on.

### P6 — Key encoding (the most load-bearing table in this plan)

Big-endian throughout, so byte order is numeric order.

| Prefix | Key | Value | Why |
|---|---|---|---|
| `0x01` | `ino` | attrs, `nlink`, inline manifest or manifest hash, symlink target, rdev, **inline xattrs when the whole set ≤ `XATTR_INLINE = 256 B`** | authoritative inode record; point lookup by ino, which is how FUSE addresses everything |
| `0x02` | `parent_ino \| name` | `(ino, kind)` + a **denormalized attr copy** for READDIRPLUS | **readdir is one contiguous range scan**; a directory's dentries are adjacent, so they land in one pack and a cold `ls -la` of 1,000 files is one ranged GET |
| `0x03` | `ino \| xattr_name` | value (spilled to a blob above `VALUE_SPILL`) | only for xattr sets that exceeded `XATTR_INLINE`; `listxattr` is then a range scan |
| `0x04` | `ino \| parent_ino \| name` | `()` | reverse dentry index (hard links, path reconstruction, rename cycle check) — replaces `dentry_by_ino` |
| `0x30` | subsystem `\| id` | record | snapshots, clones, quota, designations, holds |

Prefixes `0x10`–`0x2f` are **reserved and deliberately unused** (ADR-11's
reservation habit): a future index whose key is provably immutable may
claim one, and nothing else may.

Keys are chosen so that **write locality equals read locality equals pack
locality**. An rsync, a `git checkout`, or a build writes one directory
at a time; those writes touch a contiguous key range, rewrite a handful
of adjacent leaves, and land in one new pack — which the next reader
fetches in one request. Getting this table right on day one is the whole
game; getting it wrong is the one mistake in this plan that would require
a bucket migration.

Five encoding decisions carry most of the weight and are worth stating as
rules rather than cells:

- **Dentries are keyed by the parent's ino, never by path.** This is what
  keeps `rename` O(1) at any subtree size (ADR-3): renaming a directory
  rewrites exactly one `0x02` key, one `0x04` key, and the two parents'
  mtime/ctime — the descendants' keys contain the directory's *ino*, which
  does not change, so a 10M-file directory move touches ~6 keys in 3–5
  leaves. A path-keyed encoding would rewrite 20M keys. Cycle checking
  (`rename` may not move a directory into its own descendant) is an
  O(depth) ancestor walk over `0x04`, exactly as `meta::sqlite` does today.
- **Small xattr sets live in the inode record.** The census says most
  files have none and the common non-empty case is one label (SELinux, or
  Constellation's own `user.constellation.*`). Inlining below 256 B keeps
  those out of the keyspace entirely — without this rule, 100M files × 2
  labels adds 200M keys and ~13 GiB of leaves (Appendix B). Sets above
  the bound spill to `0x03`, individual values above `VALUE_SPILL` spill
  to blobs, and Linux's 64 KiB-per-xattr ceiling is therefore bounded by
  the blob store rather than by node size. `fs-core::Tree` already inlines
  xattrs per entry (`put_xattrs`), so this matches existing practice.
- **The dentry carries a copy of the attrs READDIRPLUS returns.** FUSE
  addresses by ino and directories address by name, so *both* indexes are
  required — this is inherent to POSIX-over-FUSE and is identical in
  today's schema (`dentry` PK `(parent,name)`, `inode` PK `ino`, plus the
  `dentry_by_ino` index). Without the copy, `ls -la` of a directory is one
  range scan plus N scattered point lookups into the `0x01` range, which
  is fine on a full replica and bad on a partial one (N packs). With it,
  `ls -la` is a pure sequential scan. The cost is that `setattr` writes
  the inode record *and* `nlink` dentry copies; `nlink` is 1 for all but a
  rounding error of files, and both writes land in one atomic commit
  attested by one root hash — so unlike plan 27's SQLite-vs-tree pair
  there is no reconciliation path that could drift, and `fsck` is still
  just a hash recomputation. §14.4 also shows this is where scattered write
  amplification comes from — a `chmod` writes two keys in two distant
  ranges — so the copy is only the right trade if recursive attribute
  changes go through §P9 macros rather than key deltas.
- **Indexes stay out of the tree.** Only keys that are *immutable for the
  lifetime of the entity they describe* may live in the hashed tree. That
  admits the four primary ranges above (`ino` and `(parent_ino, name)`
  change only on `rename`, which is a single-key move) and excludes every
  index over a mutable or hash-distributed field. `find`-by-name,
  `find -newer`, prune policies, and the plan 26 existence hint are
  therefore node-local derived state, rebuilt from a range scan. Nothing
  is lost in capability: §P10's GC works by reachability from roots and
  so never needs a `chunk_ref` index, and the existence hint is advisory
  by construction.
- **atime never enters the tree.** With atime on, a `find` over 100M
  files would be 100M scattered inode writes — the §14.4 pathological
  shape, driven by reads. atime is already special-cased today (a
  separate `atime_journal`, max-merge on apply, excluded from `TouchSet`,
  noatime-style default), and plan 27 explicitly leaves the question
  open. Plan 28 closes it by exclusion: atime is node-local, best-effort,
  and never part of a commit or a root hash. A filesystem that genuinely
  needs cluster-visible atime gets coarse buckets (hour granularity) in a
  node-local range, never per-read precision in shared state.

### What FUSE actually asks for

FUSE issues no joins, no aggregates, and no ad-hoc queries. The complete
metadata workload is point lookups, range scans, and point writes — which
is what an ordered index is, and is the only part of SQLite that is
load-bearing here:

| FUSE op | Tree operation | Cost |
|---|---|---|
| `lookup(parent, name)` | point read `0x02` | 1 leaf (attrs come back with it) |
| `getattr(ino)` | point read `0x01` | 1 leaf |
| `readdir(ino, off)` | range scan `0x02 \| ino` from `off` (the cursor **is** a key, so §12's stable readdir cursors are free and exact) | ~1 leaf per FUSE page (~100 dirents) |
| `readdirplus` | same scan, attrs inline | same — no per-entry lookup |
| `getxattr` / `listxattr` | inode record, or point/range on `0x03` | 1 leaf |
| `create`/`mkdir`/`symlink`/`mknod` | write `0x01`, `0x02`, `0x04`, bump parent mtime | 3–4 keys, 2–3 leaves |
| `unlink` | delete `0x02`, `0x04`; decrement `nlink` on `0x01`; on `nlink→0` add to the orphan/deref set | 3–4 keys, 2–3 leaves |
| `rename` (file or directory, same or cross-directory) | move one `0x02` + one `0x04`, touch two parents | ~6 keys, 3–5 leaves, **independent of subtree size** |
| `rmdir` | emptiness probe (one range read on `0x02 \| victim`) then as `unlink` | 1 probe + 3 keys |
| `link` | write `0x02`, `0x04`; bump `nlink` | 3 keys |
| `setattr` / `write_manifest` | rewrite `0x01` + `nlink` dentry copies | 2 keys typical |
| `statfs`, `du` | interior aggregates (P7) | O(depth) resident reads |

### P7 — Augmented interior nodes

Each interior entry carries its subtree's aggregates: byte size, file
count, and (per shard) a max mtime. Consequences:

- `du -sh` on any subtree, `statfs`, quota accounting, and pin admission
  are O(depth) reads of already-resident interior nodes — no maintained
  `rsize`/`rcount` columns, no transactional aggregate updates on every
  mutation, no plan 17 "virtual rsize", and no seeding pass at mount
  (`recursive_size_conn(ROOT_INO)` disappears).
- The aggregate is *attested by the root hash*, so a wrong `rsize` is a
  detectable corruption rather than a silently drifted counter.

An aggregate must be a monoid over the key order to be computable
bottom-up. Byte totals and counts are; per-*directory* recursive size is
not (a directory's descendants are not contiguous under the `0x02`
encoding). So subtree aggregates are exact per key range and per shard,
and per-directory `du` is answered by a bounded range scan over the
directory's descendant dentries plus their inode aggregates. Measure this
against the current O(1) column before committing — it is the one §12
promise this plan makes *worse*, and Step 0 must quantify it.

### P8 — One packed blob store for metadata nodes and data chunks

ADR-11 defers packfiles because the write path needs batch/spool/seal and
"GC becomes compaction". Both costs are already mandatory here: nodes are
small (8 KiB) and numerous, and immutable-node garbage must be reclaimed
by rewriting packs regardless. So build compaction once and use it for
both planes.

```
packs/<hash>          # ~1–16 MiB, concatenated zstd'd (sealed) blobs
packs/<hash>.idx      # (hash, offset, len) — or inline the index in the commit
```

The plan 26 Appendix settles the granularity: 64×1 MiB packed blobs beat
both 4096 loose blobs (4.7×) and one ranged image (1.9×) on the far AWS
path, and beat everything same-region. Metadata nodes need packing *more*
than data chunks do, because they are smaller and read in clusters.

Data chunks stay individually addressed (ADR-3, ADR-10) and may be loose
or packed; the `pack` manifest entry type (ADR-11) is the reservation
that already allows this with no format change.

### P9 — Deterministic macro commits, hash-verified

DESIGN §12 promises `rm -rf` and `chown -R` on a million files as "one
compact log record". A pure key-delta cannot express that in O(1) bytes.
Two mechanisms recover it, and the second is strictly better than what
exists today:

1. **Unlink-then-reap.** Removing a subtree removes one dentry key. Every
   descendant key becomes unreachable from the dentry graph — i.e.
   *garbage*, not state — and a background reaper deletes it under the
   existing orphan/hold rules (§3, §14). `rm -rf` is O(1) in the commit.
   (POSIX `rm -rf` issues N unlinks from userspace anyway; the O(1) path
   matters for the bulk CLI/API, which is exactly what §12 claims.)
2. **Macro commits.** A commit may carry a compact *intent*
   (`{kind: "chmod_r", root: ino, mode: 0o644}`) instead of a key delta,
   together with the resulting root hashes. Every replica applies the
   macro locally, deterministically, and **must** arrive at the stated
   hashes. Bandwidth is O(1); local CPU is O(n); and divergence is
   *detected* rather than silent. Today a replay divergence on
   `unlink_subtree` would be invisible until `fsck`.

Macros are a closed, versioned registry (like today's op registry) and
are the only place where "apply this operation" survives. Everything else
is state.

### P10 — GC = reachability + compaction

The root set lives in the bucket and is small: the newest commit, the
retained commit window, every `snaps/*`, every clone, every unexpired
`holds/*`. Mark = walk those roots (a Merkle walk that terminates
immediately on shared subtrees, so marking N commits costs
O(their differences), not O(N × state)). Sweep = delete unreferenced
packs, or rewrite partially-dead packs (compaction).

What goes away: the `deref` table, the deref-txid bookkeeping woven
through replay, the `superseded-checkpoint` rule, and GC's dependence on
a *full local replica* — any node, or an external job with bucket
credentials, can GC by reading roots.

What stays, because it is sound and the race is real: the `gc.horizon`,
the condemned-list handshake, the offline exemption, holds, and
reintegration verification (§14). A writer can still commit a manifest
referencing a chunk it never uploaded (dedup hit), so the horizon plus
condemned handshake remain the defense.

### P10b — Bounding the change volume (the reaper and the compactor)

"Every commit is a complete state" only helps if the *history* of states
is bounded. Three distinct kinds of accumulation exist, and the design
handles them very differently:

1. **Commit objects.** Pure retention policy. Each is a few KB and names
   a complete state, so deleting old ones costs time-travel depth and
   nothing else. This is the structural win over today: a log segment
   below the checkpoint is load-bearing (deleting it wrong is plan 26
   finding 6, a latent data-loss bug), whereas a plan-28 commit is never
   required to reconstruct anything. `CONSTELLATION_COMMIT_RETENTION`
   (count) and `..._RETENTION_S` (age) bound it with no correctness cliff.
2. **Superseded nodes.** Every commit orphans the leaves it rewrote, and
   this is the real cost — it is the "GC becomes compaction" burden ADR-11
   named when it deferred packfiles, and this plan pays it deliberately
   rather than inheriting it later. **Measured** (§14.5, superseding this
   plan's original estimate): a mixed 10k-op commit rewrites **9.12 MiB**
   of nodes, which at one commit per 5 s is **154 GiB/day** — 21× the
   ~7.4 GiB/day this section first assumed from ~170 nodes per commit.
   The gap is mostly the 5% scattered share of the commit mix.
   For comparison, plan 26 measured **54.55 GiB** of checkpoints for a
   single 1.85M-file rsync and bounds that to ~2× the DB. So the garbage
   *rate* is comparable-to-better; what is new is that reclaiming it needs
   a compactor, not a `delete`.
   The hoped-for mitigation was packing locality (P6): a pack written by
   one commit being superseded *in its entirety* by later commits to the
   same key range, making whole-pack deletion the common case. **§14.5
   does not support that: 0.7% of packs died whole**, and the compactor
   rewrote 14.79 GiB of live nodes — 117% of what the commits themselves
   wrote. The footprint plateaus *because* of the compactor, not in spite
   of needing one. So the budget to plan for is a compactor sized at
   roughly the commit write rate (`CONSTELLATION_COMPACT_BYTES_PER_S`),
   parallelized per §0.5. The one untested hope: §14.5 picks a fresh
   random directory per commit, whereas a real writer returns to the same
   directories repeatedly, so that run measures the pessimistic end of
   pack death and the optimistic end is still unmeasured.
3. **Unreachable keys from bulk unlink.** P9's unlink-then-reap makes
   `rm -rf` O(1) in the commit, which means a 10M-file subtree removal
   leaves ~20M keys that are unreachable from the dentry graph but still
   occupy ~1.4 GiB of leaves until the reaper walks them. Honest
   consequence: for a window after a bulk delete, the metadata is larger
   than the live set, and the reaper needs its own rate budget and a
   restartable cursor. The comparison is favorable rather than free —
   §12's claim that `unlink_subtree` does `rm -rf` on a million files "in
   milliseconds" describes the *log record* being compact; the SQL
   transaction behind it is a million-row `DELETE`, which is seconds and
   holds the write lock. The tree version returns immediately and moves
   the work off the syscall path, at the price of owning a reaper.

The steady-state bucket footprint is therefore
`live nodes + retained-history nodes + packs awaiting compaction`, each
term bounded by an explicit knob, and none of them by a correctness
requirement. That is the property today's layout does not have.

### P11 — P2P: metadata becomes ordinary content

Because metadata nodes are content-addressed blobs, the cooperative cache
(§7), bloom digests, latency-adaptive source selection, and bao-verified
transfers apply to metadata with no new protocol. Two payoffs:

- **Bootstrap from a LAN peer.** S3 supplies the commit (one small GET);
  the 1.6 GiB of leaf nodes comes from a peer at LAN speed. Plan 27's
  bootstrap budget is a 3.4 s pack download from Europe; a peer makes it
  bandwidth-bound on the LAN instead, and correctness is unaffected
  because every node is hash-verified.
- **Merkle sync instead of log tailing.** "I have root R, you have R';
  send me what differs" is one request and O(change) bytes. Over S3 alone
  the commit's `packs` field serves the same purpose in 1–2 GETs, so no
  O(depth) sequential round trips.

### P12 — Optional latency tier: S3 Express One Zone

General-purpose S3 now has both conditional-write primitives this design
needs (`If-None-Match: *` for commit creation, `If-Match` for leases),
which is the whole reason the plan works without a coordinator. Directory
buckets (S3 Express One Zone) additionally offer single-digit-millisecond
latency and `PutObject` with `x-amz-write-offset-bytes` — a genuine
shared *append*.

That is a different primitive and changes one number: commit latency
drops from ~180 ms to ~5 ms on the far path, which makes per-operation
optimistic commits viable and removes most of the remaining motivation
for leases and forwarding. Shape: hot commit chain and WAL in a directory
bucket, packs and the durable commit history in a general-purpose bucket.

It must stay **optional** — one-AZ durability, ~7× storage price, and no
equivalent on MinIO, OVH, or R2 (all of which do support conditional
writes, per the plan 26 Appendix and vendor docs). The general-purpose
path is the baseline that must be correct and fast enough on its own.

### P13 — E2E: keyed hashing extends to metadata nodes

ADR-8's keyed-addressing argument (a plaintext-hash object key lets the
provider hash a known file and test for its presence) applies verbatim to
metadata: a plaintext-hashed node blob would let the provider test for a
known *directory listing*. So in E2E mode node hashes are
`blake3::keyed_hash` under the addressing key, nodes are AEAD-sealed with
their hash as AAD before packing, and commit objects are sealed with
their key as AAD — the same rules plan 27 §Design already sets for packs,
applied to the whole metadata plane. Dedup and structural sharing are
unaffected (one key per filesystem).

## 6. What this buys, against the spec

| DESIGN promise | Today | Plan 28 |
|---|---|---|
| §4 checkpoints | O(DB) write and read; plans 26+27 exist to bound it | no such artifact |
| §4 partitions, cross-partition rename | namespace ranges, split/merge records, 2PC + parking table | keyspace shards under one commit; rename is one atomic commit |
| §6 staleness | consistent prefix of the log | snapshot isolation at a named root hash |
| §12 change journal | `changes --since <txid>`, log-position only | `diff(any root, any root)` in O(difference) |
| §12 O(1) aggregates | maintained columns, per-mutation updates, mount-time seeding | interior-node aggregates, hash-attested (with the per-directory caveat in P7) |
| §12 instant search | node-local indexes nothing attests to | unchanged: still node-local, rebuilt by range scan (§P6 *indexes stay out of the tree*) — the one §12 promise this plan does **not** improve |
| §12 time travel | checkpoint + replay into a temp DB | check out a root hash |
| §13 snapshots | a separate tree-building path (`SnapshotManager::build_tree`) | a snapshot *is* a retained root hash; `build_tree` deletes |
| §14 GC | deref table + full replica + LIST sweep | reachability from bucket roots; any node, partial replica fine |
| ADR-5 partial replica | blocked by the whole-DB replica | falls out of the node cache |
| `fsck` | walk and compare | recompute the root hash |
| Offline reintegration (§5.2/5.3), relaxed mode (§6) | replay a stranded journal; conflicts "materialized" by op-level rules | three-way merge against the common-ancestor root; disjoint changes merge deterministically, overlaps surface as an explicit conflict set |

The last row deserves emphasis because it is the deepest win and the one
closest to the project's reason for existing. Constellation prevents
conflicts (ADR-2) and will keep preventing them by default. But the
modes that *cannot* prevent them — an isolated offline designee, a
continuation epoch, relaxed followers — currently reintegrate by
replaying op records with hand-reasoned rules. With a version DAG the
merge base is trivially identifiable, the two diffs are exact, disjoint
changes merge with a deterministic result both sides can verify by hash,
and genuine conflicts are a computed set rather than an emergent
property of replay order.

## 7. What it costs, honestly

1. **Point-read latency is the existential risk.** ADR-9 chose SQLite on
   measured numbers: 578K dentry lookups/s, ~2 µs against ~10 µs of FUSE
   overhead. A prolly-tree lookup with a resident interior is one leaf
   probe plus a decode; it should be comparable, but "should" is not a
   benchmark. If it lands below ~300K/s the plan is dead. **Step 0.1.**
2. **Write amplification on scattered commits.** A commit rewrites every
   leaf it touches. Clustered (the normal case: one directory at a time)
   a 10k-op commit rewrites ~170 nodes ≈ 1.4 MiB, ~450 KiB zstd'd — about
   45 bytes per op, comparable to a log record and with no checkpoint
   behind it. Scattered across 23.8M keys, the same commit rewrites ~10k
   leaves ≈ 80 MiB, ~27 MiB zstd'd. That worst case is real; it is also
   what plan 27 pays (a scattered change dirties a comparable number of
   directory blobs), so it is not a regression — but it must be measured,
   not assumed. **Step 0.2.**
3. **Losing SQL, and losing attested indexes with it.** Mitigated by a
   derived node-local view rebuilt by range scan, but
   `fsck`/`inspect`/support-session ergonomics get worse before they get
   better, and §P6's *indexes stay out of the tree* rule means search
   indexes stay exactly as unattested as they are today. An earlier draft
   of this plan claimed otherwise; §14.4's scattered-write numbers are why
   it no longer does.
4. **Optimistic concurrency needs disciplined read-sets.** Every FUSE
   operation must declare what its decision depended on. Miss one and you
   get a wrong POSIX answer under contention — a subtle, load-dependent
   bug class the lease model cannot produce. Mitigation: read-sets are
   *derived*, not hand-written — the tree cursor records every key it
   observed, including negative lookups. This is mechanical and testable;
   it is also the one place where this plan is easier to get subtly wrong
   than today's.
5. **A prolly tree is a real data structure with real failure modes.**
   A bad boundary function produces pathological node sizes; a
   non-deterministic one produces silent divergence. Property tests over
   random insertion orders are mandatory and cheap.
6. **Two background subsystems become mandatory** (P10b): a pack
   compactor and an unreachable-key reaper. Neither exists today, neither
   is on any latency path, and both need rate budgets and restartable
   cursors. This is the honest price of not having a checkpoint, and it is
   the cost ADR-11 named when it deferred packfiles.
7. **It is a rewrite of the metadata plane.** `meta` is ~9.3k lines,
   `shipper.rs` ~3.8k. Realistically ~60% of `meta` is replaced, ~30% of
   `shipper.rs` survives (the CAS, the tailer, the lease integration),
   and `fusefs*.rs` changes at the mutate boundary only.

## 8. Rejected alternatives

- **LSM over object storage (SlateDB/Delta shape: immutable SSTables + a
  CAS'd manifest).** This is the closest competitor and would also
  collapse replica/transport/checkpoint into one artifact, with cheaper
  writes (pure append, no O(log n) node rewrite). Rejected because the
  state has no canonical hash: "the filesystem" is a set of overlapping
  runs, so diffing two versions is a merge scan rather than O(difference),
  snapshots do not structurally share, verification is per-block rather
  than whole-state, and the offline-merge story of §6 never appears. For
  a metadata plane bounded by FUSE rates (thousands of ops/s, not
  millions) the write-path advantage buys nothing we need and the read
  advantages are exactly what this system's feature list is made of.
  A small local LSM (memtable + WAL) *is* used, materialized into the
  tree at commit — the Dolt arrangement.
- **Pure CRDT / gossip metadata.** Rejected for ADR-2's reason, unchanged:
  CRDTs cannot express locks, and merge-based namespaces produce exactly
  the conflicts this project exists to prevent. Note the distinction:
  a canonical Merkle map gives *convergence by construction* for
  identical content, which is not the same as automatic merge of
  divergent content, and this plan never claims the latter by default.
- **Raft/consensus among nodes.** ADR-2, unchanged: a 2-of-3 quorum
  cannot distinguish "laptop offline" from "partition", and a minority
  node with S3 access must not be fenced out.
- **DynamoDB / FoundationDB / etcd as the metadata arbiter.** Violates
  the no-dedicated-coordinator goal, adds a second durability and access
  domain, and breaks "the only mandatory infrastructure is one bucket".
- **Path-keyed metadata (`path -> attrs`, S3-native ordering).** Makes
  subtree operations range ops, but renames become O(subtree) rewrites —
  an explicit ADR-3 goal violation.
- **Keep SQLite as the source of truth and publish the tree beside it
  (plan 27).** The honest framing: plan 27 is this plan with the
  representations kept separate and a table-equality test standing in for
  a proof. It is the right *incremental* move and the wrong *terminal*
  design.

## 9. Relationship to plans 26 and 27, and the recommendation

**Plan 26 is unaffected and should ship as written.** Every one of its
steps is either a bug fix or a transport improvement that plan 28 keeps:
the per-partition retention fix, holders not tailing themselves, the
GET-next tailer with idle backoff, the byte-capped batch, parallel ranged
I/O, sticky leases with S3-visible handoff, and the existence hint from
the reference index. Only the checkpoint-cadence work (Steps 1–2) becomes
moot, and it is moot in plan 27 too.

**Plan 27 is where the fork is.** Three options:

- **(A) Ship 27 as written.** Gets bounded checkpoints now. Locks in the
  bucket format as "Merkle tree of *directories*", keeps three
  representations, and pays again later for the HAMT sharding rule,
  the dirty-propagation machinery, and the bulk loader — all of which
  plan 28 deletes.
- **(B) Ship 27's *goals* with plan 28's *format*.** Keep SQLite as the
  live engine and the op log as the transport for now, but make the
  published artifact a prolly tree over the P6 key encoding, packed per
  P8, rooted in a P2 commit object. The bucket format becomes final
  (ADR-5's promise), plan 27's HAMT and `TREE_FANOUT` knob vanish
  (boundaries are structural), and plan 28 later becomes an
  *incremental local-engine swap* against an unchanged bucket rather than
  a second format migration. Cost over (A): the key encoding and node
  format must be designed carefully now, and the builder writes key
  ranges instead of directory blobs.
- **(C) Do plan 28 now.** Correct terminal design, largest bet, and it
  stalls phases that depend on the metadata plane being stable.

**Recommendation: (B), gated on Step 0.** It buys the thing that is
expensive to change later (the on-bucket format and the key encoding) at
roughly plan 27's cost, and defers the thing that is cheap to change
later (which local engine answers `lookup`) until the benchmarks in Step
0 say whether SQLite should be replaced at all. If Step 0.1 says a prolly
tree cannot match ADR-9's lookup rates, (B) still leaves the project
strictly better off than (A), and ADR-9's engine trait keeps SQLite where
it is.

## 10. Step 0 — the measurements that decide this

**Step 0 ran on 2026-09-11. The harness is `bench/prollybench` and the
numbers, the gate verdicts, and what they change are in §14.**

No implementation work starts before these are in `bench/` with numbers
in this file. 0.1–0.3 are the original three gates and are reported in
§14; 0.5 was added by what §14 found. Each is a standalone binary against synthetic data at
census scale (23.8M keys, values sized from the reference census).

**0.1 Lookup and scan throughput** (`bench/prollybench`). Build the tree
over 23.8M keys; measure single-threaded random `lookup` by
`(parent_ino, name)`, sequential readdir scan, and `getattr` by ino, with
(a) everything resident, (b) interior resident and leaves on disk via
`fs-core::cache`, (c) cold. Compare against ADR-9's SQLite figures
measured on the same machine (578K lookups/s, 521K readdir/s). Report
node sizes, level count, interior bytes, and p50/p99 latency. **Gate:
≥ 300K lookups/s in (a) and ≥ 150K in (b), or the plan stops here.**
Repeat at Appendix B's 210M keys with a capped node cache to confirm the
two-node-read cold path and to validate Appendix B's sizing — not a gate
(nothing is tested at that scale), but the numbers go in Appendix B.

Include the three FUSE shapes the ordered encoding is supposed to make
scale-invariant: paged `readdir` of a 10M-entry directory, `rename` of a
directory holding 10M descendants, and `listxattr` on a spilled set.
**Gate: rename cost must not vary with subtree size.**

**0.2 Commit cost and reclamation across write shapes.** For clustered
(one directory), semi-clustered (a build tree), and scattered (random ino
`chmod`) batches of 1k/10k/100k ops: nodes rewritten, bytes before and
after zstd, packs produced, and CPU time. Compare against the
shipped-segment bytes the same ops produce today. **Gate: clustered ≤ 2×
today's log bytes per op, and scattered documented — no threshold, but
the number goes in this file.**

Run each shape against **both a freshly built tree and an aged one.** The
clustered numbers lean on an unstated property: `alloc_ino` is
`prefix << 40 | counter++`, so a tree imported in one pass has inos
ascending in traversal order, which makes the `0x01` range contiguous for
a directory-ordered write. On a tree edited over years, ino order and
directory order decorrelate, so the dentry leaves stay clustered while the
inode leaves scatter and the clustered shape drifts toward the scattered
one. Age the corpus by applying ~10 create/unlink/rename generations
before measuring. **Gate: the aged clustered shape must stay under 4×
today's log bytes per op.** If it does not, the encoding needs the
dentry to be authoritative for `nlink == 1` files — trading a second hop
on `getattr(ino)` for permanently clustered attribute writes — and that
is a format decision, so it must be settled before option (B) ships.

Then run the same workloads for an hour against a retention window and
measure what P10b claims: superseded node bytes per day, the fraction of
packs that die whole (deletable at zero rewrite cost) versus needing
compaction, and the reaper's throughput on a 1M-key unreachable subtree.
**Gate: steady-state bucket footprint must plateau, not grow.** If it
does not plateau, the checkpoint was doing more work than this plan
credits it for and (B) in §9 is the only defensible option.

**0.3 Diff, merge, and determinism.** `diff(R, R')` cost as a function of
the number of changed keys (assert O(difference), not O(state)); a
three-way merge of two disjoint 10k-op branches, asserting both sides
compute the same root hash; and the determinism property test (same key
set, 50 insertion orders, one root hash).

Optionally **0.4**: re-run the plan 26 Appendix primitives against a
directory bucket (S3 Express One Zone) to put a real number on P12's
commit latency.

**0.5 Thread scaling** — added after §14, because every number there is
single-threaded and that is not how the daemon runs (CONVENTIONS: "Linux
mounts use host-sized concurrent event loops"). Immutable
content-addressed nodes are the ideal read-parallel structure: a lookup is
a pure function of `(root hash, key)` over a node set nobody can mutate,
so there is no latch, no shared cursor, and no writer to exclude. Measure
lookup/scan throughput at 1, 2, 4, 8, 16, 32 threads in tiers (a) and (b);
commit build parallelized across §P4 shards and across nodes within a
shard; and mark + compaction parallelized per pack.

This matters most for the one gate that failed. §14.2's tier (b) is
42 k/s at a 64 MiB leaf cache, and its cost is a ranged read plus a zstd
decode per miss — which is exactly the part that parallelizes. If tier (b)
scales near-linearly, the 150 k/s bar is cleared at 4 threads with the
*small* cache, and §14.7's conclusion 1 ("needs ~1 GiB of leaf cache")
weakens to "needs ~1 GiB *or* 4 cores". **Gate: tier (b) ≥ 150 k/s
aggregate at ≤ 8 threads with a 64 MiB leaf cache.** Also report
compaction throughput at 32 threads against §14.5's 939 s of 1,205 s
wall — if that divides by cores, the "GC is most of the machine" finding
becomes "GC is one core".

## 11. Steps 1+ (sketch only; sequencing depends on §9's choice)

Each step must leave the tree building and `cargo test --workspace`
green, per CONVENTIONS.

1. **New crate `crates/mtree`**: node format, boundary function, cursor,
   bulk build, splice, diff, three-way merge, augmented aggregates.
   Pure, no I/O, exhaustively property-tested. This is the crate that
   either works or does not; it lands alone.
2. **Key encoding + codec** (`mtree::keys`): the P6 table, with
   round-trip and ordering tests (byte order equals numeric order for
   every field).
3. **Store integration**: pack writer/reader, node cache over
   `fs-core::cache`, commit object + CAS create + GET-next head discovery
   reusing `store-s3::log`. The commit chain replaces `checkpoints/*`,
   `LATEST`, and `VECTOR.json`.
4. **Builder from the live replica**: build a tree from `SqliteMeta` and
   publish it. At this point option (B) is complete and the bucket format
   is final; plans 26+27's goals are met and `SnapshotManager::build_tree`
   is deleted in favor of retaining a root hash.
5. **Reader**: bootstrap and partial replica from a commit; peer-served
   nodes; `fsck` = recompute the root hash.
6. **Engine swap** (only if 0.1 passed): `MetaStore` backed by
   `mtree` + WAL + memtable; SQLite becomes a derived view.
7. **Optimistic commit + rebase**; read-set capture in the tree cursor;
   leases demoted per P3.
8. **Delete partitions**: shards under one commit; `xpart_*`,
   `PartSplit`/`PartMerge`, and `CheckpointVector` removed.
9. **Macros** (P9) for bulk namespace ops, with hash verification.
10. **GC by reachability** (P10) and pack compaction.

## 12. Tests

Beyond the per-step unit and property tests:

- **Determinism fuzz**: random op sequences from the harness `Workload`
  applied in random valid orders on two replicas → equal root hashes.
- **Concurrent-commit matrix** (in-process, `InMemory`): N writers × op
  mix; assert linearizability against the harness `Model` oracle and that
  every `O_EXCL`, `rename`, `link`, and `rmdir` conflict resolves to the
  POSIX-correct errno.
- **Merge**: two branches from a common root, disjoint → deterministic
  merged root, both sides agree; overlapping → the conflict set is exactly
  the overlapping keys.
- **Partial replica**: bootstrap with a node budget that fits the interior
  only; `ls -la` of a cold directory costs exactly one pack GET (confirmed
  measured, §14.2); a `readdir`-driven walk never faults in leaves outside
  the directories it visits.
- **No mutable field is in the tree**: a mechanical test over the key
  codec asserting that every in-tree key is built only from fields that
  are immutable for the entity's lifetime (§P6). This is the guard that
  keeps the retracted secondary indexes, and atime, from creeping back.
- **pjdfstest stays a FULL pass** (8798/8798, empty baseline). Non-
  negotiable and the real acceptance test for P3's read-sets.
- Harness: `optimistic_commit_contention` (two nodes, same directory,
  200 ms S3 latency, no P2P — assert convergence and no lost op);
  `partial_replica_cold_walk`; `offline_branch_merge` (isolate a node,
  write both sides, reconnect, assert deterministic merge and an explicit
  conflict set for the overlap).

## 13. Gates + report

Per CONVENTIONS, plus paste into the report: the three Step 0 tables;
lookup/readdir throughput against the ADR-9 baseline on the same machine;
commit bytes per op for all three write shapes against today's shipped
log bytes; bootstrap wall time for full and partial replicas on the plan
26 Appendix paths; the pjdfstest tally; and `getattr`/`lookup` p99 during
an active commit (the `VACUUM INTO` stall this plan also eliminates must
not be replaced by a tree-rebuild stall).

## 14. Step 0 results (2026-09-11)

Harness: `bench/prollybench` — a standalone crate (excluded from the
workspace, like `bench/dbbench`) holding a real prolly tree: the node
format and boundary function, bulk build, cursor, incremental commit,
structural diff, three-way merge, augmented aggregates, the §P6 key
encoding, a pack writer with three residency tiers, a reachability
sweep, and a pack compactor. It depends on `constellation-meta` only so
the "bytes per op today" column is the shipped encoding (postcard
`LogRecord` envelope, zstd level 3) rather than a re-implementation. No
product code was touched.

The properties this design rests on are unit tests rather than prose
(`cargo test --release`, 15 tests): insertion order does not change the
root hash, an incrementally applied tree is byte-identical to a bulk
built one, delete-then-reinsert returns the original hash, diff is
O(difference), disjoint branches merge to one hash, and overlapping ones
produce the exact conflict set.

**Method and its limits.** Host: i9-14900K, 32 threads, ZFS, 62 GiB RAM.
Corpus: a synthetic census-shaped namespace — 11.9M entries, 1.50 TiB
logical, small-file median, realistic name lengths and fanout, with 1% of
directories wide (1k–5k children) and a slice of files carrying spilled
xattr sets. All four §P6 indexes are materialized (`0x01`, `0x02`,
`0x03`, `0x04`), which is **35.8M keys** — Appendix A's 23.8M counts only
inodes and dentries, so every number below carries the reverse-dentry
index the plan's own table requires. Everything is single-threaded, and
there is no S3 round trip anywhere: packs are local files, so these are
CPU and local-I/O numbers, not end-to-end ones.

### 14.1 Structure

| Level | Nodes | Entries | Encoded bytes | Mean entries/node | Mean node bytes |
|---|---:|---:|---:|---:|---:|
| 0 (leaf) | 306,096 | 35,819,001 | 2.73 GiB | 117 | 9.35 KiB |
| 1 | 2,676 | 306,096 | 19.57 MiB | 114 | 7.49 KiB |
| 2 | 31 | 2,676 | 179.64 KiB | 86 | 5.79 KiB |
| 3 (root) | 1 | 31 | 2.12 KiB | 31 | 2.12 KiB |

Bulk build: 35.8M keys in 15 s (2.36M keys/s). **Interior total 19.75
MiB**, leaves 2.73 GiB (1.16 GiB zstd'd, 43%). Four levels, as Appendix A
predicted, and the interior stays trivially resident at 1.7× the
appendix's key count — the partial-replica claim (ADR-5, §P5) holds.

What the appendix did not predict is the **node-size distribution**: leaf
entries per node are p50 81, p99 536, max 1545, because a keys-only
boundary function is geometric. Appendix A's "8 KiB nodes" describes the
mean (9.35 KiB), not the tail: the largest leaf is ~120 KiB, and a point
lookup that faults it in decompresses all of it for one key.

The plan called for "hard min/max entry clamps" and this benchmark first
omitted them out of a worry that a clamp reintroduces history into the
shape. That worry is wrong, and the correction matters: because a node's
*start* is itself context-free, "seal after MAX entries counted from the
start" is still a pure function of the key set. `--max-entries 256`
therefore clips the tail with canonicality intact (asserted in
`entry_clamp_keeps_the_tree_canonical`, including under deletes, which
are what force a clamped run to absorb its right neighbour). At 256 the
census tree becomes 344,308 leaves, p99 = max = 256, interior 22.24 MiB.
It does **not** move the read numbers materially (below), so the tail is
not what limits this design — but it is free insurance and should be in
the format from day one.

### 14.2 §0.1 — lookup and scan throughput

300k random lookups per tier, single-threaded, against the 35.8M-key
tree. Tier (a) everything resident; (b) interior resident, leaves in
packs on disk behind an LRU; (c) cold — nothing cached, page cache
dropped with `posix_fadvise(DONTNEED)`.

| Tier | lookup(parent,name) | p50 | p99 | getattr(ino) | p50 | readdir | pack reads/lookup | distinct packs per `ls -la` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| (a) resident | **747 k/s** | 1.3 µs | 2.0 µs | 651 k/s | 1.5 µs | 65.1 M dirents/s | 0.00 | 0 |
| (b) 64 MiB leaf cache | **42 k/s** | 22.2 µs | 61.0 µs | 27 k/s | 33.0 µs | 9.8 M dirents/s | 0.94 | 1 |
| (c) cold | 18 k/s | 54.6 µs | 95.1 µs | 19 k/s | 51.7 µs | 8.6 M dirents/s | 4.00 | 4 |

Tier (b) is only defined up to a cache size, and that turns out to be the
whole question:

| Leaf cache in tier (b) | lookup | p50 | p99 | pack reads/lookup |
|---|---:|---:|---:|---:|
| 64 MiB | 42 k/s | 22.8 µs | 60.8 µs | 0.94 |
| 256 MiB | 52 k/s | 18.1 µs | 56.9 µs | 0.75 |
| 1 GiB | 141 k/s | 2.2 µs | 44.0 µs | 0.25 |
| 4 GiB | 259 k/s | 1.9 µs | 33.1 µs | 0.11 |
| 4 GiB, `--max-entries 256` | 236 k/s | 2.0 µs | 34.4 µs | 0.13 |

**Gate 0.1(a) ≥ 300 k/s: PASS (747 k/s), at 0.72× the SQLite figure
`bench/dbbench` measured warm on this host (1.04 M/s) and 1.3× DESIGN
§11's census figure (578 k/s).**

**Gate 0.1(b) ≥ 150 k/s: FAIL at any leaf cache below ~1 GiB** (42 k/s at
64 MiB, 52 k/s at 256 MiB), and it only clears the bar at 1 GiB (141 k/s,
marginal) to 4 GiB (259 k/s). The clamp does not rescue it, so the cost
is not the node-size tail; it is that a random `lookup` on a cold leaf is
one ranged read plus a zstd decode, ~20 µs on this host, and a 64 MiB
cache over 2.73 GiB of leaves misses 94% of the time.

Two things must be said about that failure before it is used as a
verdict. First, it is not an apples-to-apples comparison: ADR-9's
1.04 M/s was measured with the whole SQLite replica in page cache, i.e.
against tier (a), not against a 64 MiB budget — a SQLite replica held to
64 MiB of page cache over a 1.6 GB DB would fault on nearly every lookup
too. Second, tier (b) as specified is a *cold-working-set* measurement:
the benchmark draws lookups uniformly at random over 11.9M entries, which
no real workload does. The gate as written therefore compares a cold tree
against a warm B-tree, and what it actually establishes is the size of
the cache the tree needs, not that the tree is slower per operation.
**The honest reading: the tree matches SQLite when both are resident, and
needs roughly 1 GiB of leaf cache — not 20 MiB — to stay above 150 k/s on
a uniformly random working set.**

Two results that do land exactly as the plan claims:

- **A cold `ls -la` is one ranged GET.** A 2,330-entry directory scan
  touches ~20 adjacent leaves that live in **one distinct pack** (the
  cold tier touches 3–4 for the wider directories). Appendix A's key
  encoding claim — write locality equals read locality equals pack
  locality — is confirmed as measured, not just as argued.
- **Sequential scans are enormously faster than point reads**: 65 M
  dirents/s resident, 8.6 M/s cold, against DESIGN §11's 521 k/s
  readdir baseline.

### 14.3 §0.1 — the three FUSE shapes

| Shape | Result |
|---|---|
| Paged `readdir` of a 10M-entry directory | 10,000,000 dirents in 100,000 pages of 100, **5.0 µs/page**, 19.9 M dirents/s. The cursor is the key, so resumption is an exact seek with no offset table. |
| `rename` of a directory with N descendants | N=2,330 → 5 keys / 10 nodes; N=2,913 → 5 / 10; N=4,577 → 5 / 10; **N=10,000,000 → 4 keys / 7 nodes / 88 KiB / 511 µs** |
| `listxattr` on a spilled set | 20 names in one range scan, 4.8 µs |

**Gate: rename cost must not vary with subtree size → PASS.** A directory
holding ten million descendants renames for the same handful of keys as
an empty one, which is ADR-3's promise made structural.

### 14.4 §0.2 — commit cost by write shape

Each batch is applied to the same census-scale root, so these are
marginal costs against a 35.8M-key filesystem. "Nodes" counts only nodes
whose content was not already stored; bytes include the §P2 commit
object.

| Shape | ops | keys written | nodes | bytes | zstd | packs | B/op | today's segment B/op | ratio | CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| clustered (one directory) | 1,000 | 3,000 | 25 | 349 KiB | 146 KiB | 1 | 150 | 45 | 3.31× | 3 ms |
| clustered | 10,000 | 30,000 | 260 | 2.36 MiB | 736 KiB | 1 | **75** | 45 | **1.67×** | 21 ms |
| clustered | 100,000 | 300,000 | 2,562 | 22.55 MiB | 6.45 MiB | 7 | **68** | 45 | **1.50×** | 178 ms |
| semi-clustered (build tree) | 1,000 | 3,000 | 68 | 957 KiB | 454 KiB | 1 | 465 | 46 | 10.11× | 6 ms |
| semi-clustered | 10,000 | 30,000 | 382 | 4.25 MiB | 1.68 MiB | 2 | 176 | 45 | 3.87× | 29 ms |
| semi-clustered | 100,000 | 300,000 | 2,662 | 24.85 MiB | 7.69 MiB | 8 | 81 | 45 | 1.78× | 177 ms |
| scattered (random-ino chmod) | 1,000 | 2,000 | 2,940 | 54.04 MiB | 23.66 MiB | 24 | 24,811 | 10 | 2,460× | 263 ms |
| scattered | 10,000 | 19,996 | 19,863 | 409 MiB | 152.75 MiB | 153 | 16,018 | 9 | 1,773× | 1.6 s |
| scattered | 100,000 | 199,158 | 102,875 | 1.71 GiB | 640.89 MiB | 641 | 6,721 | 9 | 758× | 6.9 s |

**Gate 0.2a "clustered ≤ 2× today's log bytes per op": PASS at the
batch sizes the system actually ships (1.67× at `SEGMENT_BATCH` = 10k,
1.50× at 100k), FAIL at 1k ops (3.31×).** Commit cost amortizes over the
batch, so the gate is really a statement about commit cadence: below a
few thousand ops per commit the tree pays for rewriting whole leaves to
change a few entries. Appendix A's 45 B/op estimate was optimistic by
~1.6×; the measured clustered figure is 68–75 B/op.

**Scattered, as promised, documented rather than gated: 758×–2,460×
today's bytes per op.** Two causes, and one of them is a design decision
this plan should revisit. A random-ino `chmod` touches a leaf per key and
that is inherent. But §P6's denormalized attr copy means each `chmod`
writes *two* keys in two distant ranges — the `0x01` inode record and the
`0x02` dentry copy — so the scattered case rewrites twice the leaves it
otherwise would. §P6 argues the copy is what makes `ls -la` a pure
sequential scan, and §14.2 confirms that payoff (one pack per directory).
The measurement says the price is paid entirely by scattered attribute
writes, which is the right trade only if `chown -R`-style operations go
through §P9 macros rather than through key deltas.

### 14.5 §0.2b — 20 minutes of steady state (P10b)

1,420 commits × 10,000 ops = 14.2M operations in 1,205 s against the
census tree, with leaves in packs on disk behind a 256 MiB LRU (the
residency a real node has). Commit mix: 70% clustered, 25% semi-clustered,
5% scattered; every creation is unlinked 64 commits later, so the live set
stays at census size. Retention: 64 commits, GC every 10.

| t (s) | commits | ops | bucket bytes | live | dead-but-unswept |
|---:|---:|---:|---:|---:|---:|
| 17 | 10 | 100k | 1.78 GiB | 1.78 GiB | 0 |
| 190 | 230 | 2.3M | 2.16 GiB | 1.90 GiB | 269 MiB |
| 466 | 560 | 5.6M | 2.08 GiB | 1.87 GiB | 214 MiB |
| 836 | 1,000 | 10.0M | 1.35 GiB | 1.13 GiB | 227 MiB |
| 1,128 | 1,330 | 13.3M | 2.11 GiB | 1.88 GiB | 234 MiB |
| 1,205 | 1,420 | 14.2M | 2.08 GiB | 1.86 GiB | 223 MiB |

- **Footprint plateaus: mean 1.77 GiB over the first half, 1.80 GiB over
  the second, drift +1.5%.** The sawtooth is the retention window — a
  generation of retained roots holds its nodes until the window slides
  past them, and 64 commits of history costs ~0.8 GiB on top of the
  1.02 GiB base tree. **Gate 0.2b: PASS** — with the caveat in the next
  two bullets, which is the real finding.
- **"Packs die whole" is false for this workload: 0.7% died whole**, not
  the common case §P10b assumes. The compactor rewrote 16,532 packs and
  **14.79 GiB of live nodes — 117% of the 12.64 GiB the commits
  themselves wrote.** The plateau exists *because* of the compactor, not
  in spite of needing one: switch it off and the footprint grows without
  bound (measured: +25.6% over half a run at small scale).
- **GC and compaction consumed 939 s of the 1,205 s of wall clock.** They
  are off the latency path, but at this cadence they are not a background
  trickle; they are most of the machine. Throughput fell from 47 k ops/s
  (small-scale, no compaction pressure) to 12 k ops/s here.
- **Superseded node bytes: 9.12 MiB per commit.** At P10b's own
  one-commit-per-5-s assumption that is **154 GiB/day, 21× the ~7.4
  GiB/day the plan estimates**. The plan's arithmetic assumed ~170 nodes
  and ~450 KiB per commit; measured, a mixed 10k-op commit rewrites an
  order of magnitude more, mostly because of the 5% scattered share.
- **Peak RSS of the writer: 1.87 GiB.** The *live* interior is 19.75 MiB;
  the rest is the benchmark's corpus arena plus interior nodes the store
  pins until their pack is swept. A client would LRU those, but it is
  worth stating plainly: the resident cost of this design is ~20 MiB of
  interior plus whatever leaf cache §14.2 says you must buy.
- **§P9 unlink-then-reap works as described**: unlinking a 1,000,002-key
  subtree is 2 key writes and 4 nodes in 0.42 ms, and the reaper then
  removes the 1M unreachable keys at **1.81 M keys/s** single-threaded in
  restartable 20k-key batches.

Caveat on locality, because it is the one number most sensitive to the
workload: each clustered commit in this run picks a *fresh* random
directory, so a pack's nodes are rarely re-superseded by a later commit
to the same key range. A real writer (rsync, a build, a `git checkout`)
returns to the same directories repeatedly, which is exactly the effect
§P10b relies on. This run therefore measures the pessimistic end of pack
death, and the optimistic end is untested.

### 14.6 §0.3 — diff, merge, determinism

| Changed keys | diff found | node reads | time | node reads / changed key |
|---:|---:|---:|---:|---:|
| 1 | 1 | 20 | 0.05 ms | 20.0 |
| 10 | 10 | 86 | 0.22 ms | 8.6 |
| 100 | 100 | 580 | 1.89 ms | 5.8 |
| 1,000 | 1,000 | 3,898 | 12.25 ms | 3.9 |
| 9,997 | 9,997 | 21,280 | 69.53 ms | 2.1 |
| 99,582 | 99,582 | 103,792 | 284.79 ms | 1.0 |

The tree holds 35.8M keys in every row: **diff cost tracks the difference
and not the state**, and the per-change constant *falls* as the change
set grows. A one-key diff of a 35.8M-key filesystem costs 20 node reads.

- Three-way merge of two disjoint 10k-op branches: 50 ms, and both
  directions compute the same root hash.
- Overlapping branches surface a conflict set of exactly the overlapping
  keys.
- Determinism: 100k keys inserted in **50 random orders, batched and
  incremental, produced the bulk-built root hash 50/50 times**;
  delete-then-reinsert returns the original hash.

**Gate 0.3: PASS.**

### 14.7 What Step 0 changes

1. **The read gate does not kill the plan, but it does kill the "no
   database, tiny replica" framing.** §P5 sells the local engine as
   "interior resident (10 MiB) plus a node cache"; measured, a uniformly
   random lookup workload needs ~1 GiB of leaf cache to stay above
   150 k/s. The tree is competitive with SQLite when both are resident
   (0.72× warm) and the partial-replica property is real, but the plan
   should stop implying that ~20 MiB of residency buys ADR-9 latency.
2. **§7's "existential risk" is not the one that materialized.** Point
   reads are fine (1.3 µs p50 resident). The costs that measured worse
   than the plan predicted are all on the *write and reclamation* side:
   commit bytes per op (1.5–1.7× today's rather than parity), superseded
   bytes per day (21× the estimate), and compaction (rewriting more bytes
   than the commits write, consuming most of a core).
3. **Appendix A's per-commit arithmetic should be restated** with 68–75
   B/op clustered, 9.12 MiB per mixed 10k-op commit, and the node-size
   distribution rather than its mean.
4. **The entry clamp is determinism-safe and should be specified**
   (`MAX_ENTRIES`, 256 measured), which also removes §7's "bad boundary
   function produces pathological node sizes" from the risk list.
5. **§P10b's "the common case is whole-pack deletion at zero rewrite
   cost" is not supported** by this workload (0.7%). Either the plan
   drops the claim and budgets a compactor at roughly the commit write
   rate, or it demonstrates the claim under a locality-preserving
   workload, which this run did not test.
6. **§P6's dentry attr copy is where scattered write amplification comes
   from**, and it should be named as such next to the `ls -la` benefit it
   buys (which the same run confirms: one pack per directory).
7. **§9's recommendation (B) survives Step 0 intact and is now the
   evidence-backed choice.** The bucket-format half of the bet — canonical
   Merkle map, §P6 key encoding, packs, commit chain — measured well:
   determinism, O(difference) diff, hash-agreeing merges, scale-invariant
   rename, one-GET directory reads, and a footprint that plateaus. The
   engine-swap half (option C, Step 6) is exactly what gate 0.1(b)
   declines to license today.

## Appendix A — worked numbers at census scale

Census: 11.9M entries, 1.3 TiB. Keys: 11.9M dentry (`0x02`) + 11.9M inode
(`0x01`) ≈ 23.8M primary keys, matching ADR-9's 20.2M-record benchmark.
Values: dentry ~40 B (plus the P6 attr copy), inode ~100 B average
(attrs + a 1–2 chunk inline manifest for the small-file median).

| Quantity | Derivation | Value |
|---|---|---|
| Logical key+value bytes | 11.9M×40 + 11.9M×100 | ~1.67 GB (vs SQLite's measured 1.60–2.0 GB — no size regression) |
| Target node size | `TARGET_NODE_BYTES` | 8 KiB |
| Leaf fanout | 8192 / ~70 B per entry | ~117 entries |
| Interior fanout | 8192 / ~48 B per (first_key, hash, agg) | ~170 children |
| Leaf nodes | 23.8M / 117 | ~203k |
| Level 1 / 2 / 3 | 203k/170, 1200/170, 8/170 | ~1200 / ~8 / 1 (root) |
| **Levels** | leaf + 3 interior | **4 (3 interior hops + 1 leaf)** |
| **Interior bytes (all non-leaf nodes)** | ~1209 × 8 KiB | **~10 MiB — permanently RAM-resident on every node** |
| Leaf bytes | 203k × 8 KiB | ~1.6 GiB (~550 MiB zstd'd on sorted keys) |
| Packs at 1 MiB | 1.6 GiB / 1 MiB | ~1,600 |
| Full bootstrap, HU→AWS | 550 MiB at the Appendix's 19 MiB/s packed rate | ~29 s (vs ~40 s for today's 400 MiB image, and ~0 from a LAN peer) |
| **Partial bootstrap** | interior only, ~10 packs | **< 1 s on every measured path** |
| Cold `ls -la`, 1,000-entry dir | 1,000 × 40 B = 40 KiB ≈ 5 adjacent leaves, one pack | **1 ranged GET** |
| Clustered 10k-op commit (predicted) | ~10k/117 ≈ 86 leaves + ancestors ≈ 170 nodes | ~1.4 MiB, ~450 KiB zstd ≈ ~45 B/op, 1 pack PUT + 1 commit CAS |
| Clustered 10k-op commit (**measured**, §14.4) | 260 nodes, 2.36 MiB raw | 736 KiB zstd = **75 B/op**, 1.67× today's log bytes |
| Scattered 10k-op commit | ~10k distinct leaves + ancestors | ~80 MiB, ~27 MiB zstd — the honest worst case (Step 0.2) |
| Commit throughput ceiling | 1 CAS/RTT × 10k ops/commit | 50k ops/s far, ~300k same-region, vs ~2,000 creates/s/node FUSE ceiling |

The two numbers that carry the argument: **10 MiB of interior** (so a
lookup is one leaf probe and a partial replica is free), and **45 bytes
per op with no checkpoint behind it** (so the artifact plans 26 and 27
exist to bound does not exist).

## Appendix B — the ADR-5 stretch envelope: 100M files with xattrs

Appendix A is the tested envelope (DESIGN §11: 1–10M files). ADR-5's
promise is that the *format* reaches 100M+ without a bucket migration, so
the format has to be sized there even though nothing will be tested there.
Assumptions: 100M entries; two xattrs on every file (a pessimistic
SELinux-style corpus), which the P6 inlining rule keeps out of the
keyspace; a mixed corpus where 5% of files exceed `XATTR_INLINE` and spill.

| Quantity | Derivation | Value |
|---|---|---|
| Primary keys | 100M dentry + 100M inode | 200M |
| Spilled xattr keys | 5% × 100M × 2 | 10M |
| **Total keys** | | **~210M** (vs ~500M without P6's inlining rule) |
| Leaf nodes | 210M / 117 | ~1.79M |
| Level 1 / 2 / 3 / 4 | /170 each | ~10.6k / ~62 / 1 (root) |
| **Levels** | | **4 (3 interior hops + 1 leaf)** — depth grows by one level per 170× of scale |
| Top two levels | 63 × 8 KiB | **~0.5 MiB — always resident** |
| **Level above the leaves** | 10.6k × 8 KiB | **~83 MiB — LRU-cached, so a cold lookup is 2 node reads** |
| Leaf bytes | 1.79M × 8 KiB | ~14 GiB (~4.7 GiB zstd'd) |
| Same corpus in SQLite | 79 B/row × 210M (ADR-9's measured density) | ~16.6 GB — **the full replica is infeasible on a laptop either way; only the tree can be partial** |
| Packs at 4 MiB | 14 GiB / 4 MiB | ~3,600 |
| **Partial bootstrap** | top levels + the L1 nodes on the mounted path | **~1 MiB, sub-second on every plan 26 path** |
| Cold `ls -la`, 1,000-entry dir | 1,000 × ~80 B (dentry + attr copy) = 80 KiB ≈ 10 adjacent leaves | **1 ranged GET** |
| `readdir` of a 10M-entry directory | 10M × 80 B = 800 MiB ≈ 100k leaves, scanned at ~100 dirents per FUSE page | ~1 leaf per page, fully prefetchable; the key *is* the cursor, so resumption is exact |
| **`rename` of a 10M-file directory** | one `0x02` + one `0x04` + two parents | **~6 keys, 3–5 leaves — independent of subtree size** |
| `rm -rf` of a 10M-file subtree | one dentry unlink; ~20M keys become unreachable | **O(1) commit**, then ~1.4 GiB of leaves reclaimed by the rate-budgeted reaper (P10b) |
| Clustered 10k-op commit | unchanged — commit cost tracks *keys touched*, not filesystem size | 260 nodes, 736 KiB zstd, **75 B/op** measured at census scale (§14.4) |

The scale-invariance in the last three rows is the point, and it is a
property of the P6 key encoding rather than of the tree: because dentries
are keyed by the parent's ino and commits are keyed by what they touch,
neither `rename`, nor `readdir` paging, nor commit size has the
filesystem's total size in its cost. What *does* grow with total size is
the resident interior (10 MiB → 83 MiB between the two appendices) and the
leaf corpus (1.6 → 14 GiB), and the second of those is why ADR-5 already
names partial replicas as the 100M+ path: at this scale a full replica is
~16 GB of local state in today's design and ~14 GiB in this one, so the
difference between the two designs is not size but whether a node is
*allowed* to hold a subset.
