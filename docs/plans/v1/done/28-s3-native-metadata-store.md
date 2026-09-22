# Plan 28 — S3-native metadata: one versioned Merkle map instead of replica + log + checkpoint

Read `docs/plans/v1/CONVENTIONS.md` first, then plans 26 and 27.

**This plan began as a design study and is now a work order.** It asks
"if we designed a consistent distributed filesystem on S3 from the ground
up, which primitives would we pick?", measures the answer, and then
narrows to the part worth building now. §5–§9 are the design and the
recommendation; §10 is the Step 0 measurements that decided whether the
bet was worth taking; **§14 is what they reported**; **§11 is the
resulting work order** for §9's option (B). The steps beyond (B) stay
sketched, in §11b.

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

- **Boundaries from keys only.** A node ends after key `k` when a 4-byte
  window of `blake3(k)` compares below `u32::MAX / TARGET_ENTRIES`, with
  min/max entry clamps. Because the predicate reads the *key* and
  not the value, a `chmod` rewrites leaves along one path but never
  reshapes the tree; because it reads no history, insertion order cannot
  matter. Wide directories need no HAMT special case (plan 27 §Design):
  a 10M-entry directory is simply a long contiguous key range that the
  boundary function splits into ~85k leaves.

  As built in S2 the predicate is **per level**: the window is
  `level % 8` and the target differs for leaves and interior nodes. The
  window has to rotate, or a key that is a boundary at level 0 is a
  boundary at every level and the tree degenerates into single-entry
  chains above the leaves. It remains a pure function of `(key, level)`,
  which is all canonicality requires. The clamps are
  `MAX_ENTRIES = 256` (§14.1) and `MIN_ENTRIES = 1`, i.e. the lower
  clamp is implemented but **off by default**, because every number in
  §14 was measured without one.
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
  just a hash recomputation. **S1 measured the alternative and this rule
  stands** (§14.10): dropping the copy saves 10.9% of stored bytes and
  1.3–1.5× on `setattr`, and charges 12,039 pack reads for a cold
  `ls -la` against 30.4. An earlier draft of this bullet blamed the copy
  for scattered write amplification; §14.10 reading 3 corrects that. The
  second key is nearly free on an aged tree (41 B/op with the copy
  against 40 without) — what costs is the **scattered `0x01` write**, so
  the lever is ino allocation locality (§S1b), not the copy. Recursive
  attribute changes should still go through §P9 macros rather than key
  deltas.
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
packs/<hash>.idx      # (node hash, offset, len, level, first key)
```

**Corrected in S4: the index must be a sibling object, not inlined in the
commit.** This plan originally offered the two as interchangeable. They
are not, and the argument is short: **a pack outlives the commit that
wrote it.** A commit names only the packs it created, while most nodes a
reader resolves live in packs written by ancestors — many of them already
outside the retained window, because §P10b deletes old commits freely on
the grounds that they are cheap and never load-bearing. With an inline
index, "read a node" would mean "first find the commit that introduced
its pack", which is an unbounded walk back through history and flatly
impossible once retention has removed that commit, even though the pack
itself is still live and reachable. Three lesser points agree: the commit
object stays O(new packs) rather than O(nodes written) (§14.5's 10k-op
commit rewrites ~1,100 nodes, so ~44 KiB of index inside an otherwise
few-hundred-byte object); the index's lifetime is exactly the pack's, so
§P10's sweep deletes a pair with no cross-object bookkeeping; and a
partial replica can fetch a few KiB of index without touching a 1–16 MiB
body. The price is one extra PUT per pack and one extra GET to read a
pack whole, amortized over ~128 nodes.

The index is **untrusted**: it says where bytes are, never what they are.
Every node is verified against the hash the caller asked for and then
parsed, so a corrupt or lying index is a read error, never a wrong
answer.

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
   parallelized per §0.5. The one untested hope was that §14.5 picks a
   fresh random directory per commit, whereas a real writer returns to
   the same directories repeatedly, so that run measures the pessimistic
   end of pack death. **S7a weakly corroborates against even that hope**:
   its classifier, run over an aged tree that *does* revisit directories,
   still leaves overwhelmingly partially-dead packs — enough that the
   fixture has to tolerate zero whole-pack deaths rather than assert any.
   That is a small synthetic tree rather than §14.5's 20 minutes of steady
   state, so it corroborates rather than settles; the decisive measurement
   would be §14.5 re-run with directory revisiting. **Treat whole-pack
   death as the rare case and the rewrite as the normal path everywhere
   this plan still hedges.** The cheap path is implemented and tested
   (S7a's `delete_dead`), it is simply not the one that will run.
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

**Amended in S2: the key governs the boundary function too, not only node
identity.** The boundary predicate hashes *keys* and thereby decides the
tree's shape, so with a plaintext predicate a provider who guesses a
plausible key set can compute where the splits fall and compare that
against the node sizes it stores — the same class of oracle ADR-8 exists
to close, just reached through the shape instead of the hash. Keying both
costs nothing (the hasher is already threaded through) and both modes
stay canonical; the consequence to state plainly is that a keyed tree and
a plaintext tree over identical content have **different shapes**, so the
mode is a property of the filesystem fixed at creation, not a setting.

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

## 11. The work order

Step 0 has reported (§14) and §9's option **(B)** is the evidence-backed
choice (§14.7.7), so this section is no longer a sketch. The scope of (B):

> The **on-bucket format** becomes a canonical Merkle map and the commit
> chain replaces the checkpoint, while **SQLite stays the live query
> engine** and the op log stays the incremental transport.

That buys the thing that is expensive to change later — the format, the
key encoding, ADR-5's no-migration promise — and defers the thing that is
cheap to change later, namely which engine answers `lookup`. Everything
that requires the engine swap is out of scope here and restated in §11b.

Per CONVENTIONS, every step ends with `cargo fmt --all` producing no
diff, `cargo clippy --workspace --all-targets -- -D warnings` clean, and
`cargo test --workspace` at zero failures; every step adds its rows to
`PROGRESS.md`; **no step commits anything.** Steps that touch no product
code cannot regress the e2e lanes and do not run them. From S5 onward the
full CONVENTIONS gate list applies, pjdfstest included.

| Step | Deliverable | Depends on | Product code touched |
|---|---|---|---|
| **S1** | Settle the dentry attr copy (measurement) — **DONE: copy stays** | — | none — `bench/` only |
| **S1b** | Ino allocation locality (measurement) | — | none — `bench/` only |
| **S2** | `crates/mtree` — the pure data structure — **DONE** | — | new crate only |
| **S3** | `mtree::keys` — the §P6 codec — **DONE** | S1, S2 | new crate only |
| **S4** | Pack store, node cache, commit chain — **DONE** | S2 | `store-s3` |
| **S5** | Builder from the live replica — **(B) complete** — **DONE** | S3, S4 | `cli/shipper` |
| **S6** | Reader: bootstrap, partial replica, `fsck` — **DONE** | S5 | `cli`, `meta` |
| **S7a** | Reachability mark + compactor, as library code — **DONE** | S4 | `store-s3` |
| **S7b** | GC wiring, retention, rate budgets — **DONE** | S5, S7a | `cli/gc` |

S1 and S2 are independent and start together. S1 is a short measurement
and S2 is the long pole, so gating S3 on S1 costs nothing.

### S1 — Settle the dentry attr copy — **DONE (2026-09-12): the copy stays**

Full numbers in §14.10. Three `0x02` value shapes (`copy` as §P6 writes
it, `nocopy` = ino + kind, and `dentry-auth` = the dentry is the record
for `nlink == 1`) measured through the same code paths at census scale,
fresh and aged. **§P6 stands as written.** `nocopy` saves 10.9% of stored
leaf bytes and 1.3–1.5× on `setattr`, and charges **12,039 pack reads for
a cold `ls -la` against 30.4** — giving up §14.2's one-pack-per-directory
property, which is the reason the encoding is shaped this way. It is also
the one cost in the plan that does not thread away: every other read row
scales 3.6–6.4×, while `nocopy` at 8 threads is still 144× `copy`.

The decision rests on the two amplifications not being commensurable, and
§14.10 quantifies that rather than asserting it: a written byte costs
~2.17 bytes of transfer that does **not** divide by cores (it is stored on
every replica and rewritten by a compactor that §14.5 measured at 117% of
the commit write rate and §14.9 measured parallelizing only ~1.7×),
whereas a pack read is a cached read that does divide.

S7a has since lifted that 1.7× to 2.78×, which **does not disturb the
verdict** and is worth saying why rather than leaving the stale citation
to be noticed later. Compactor threading changes the wall clock of the
rewrite, not the byte volume: a written byte is still stored on every
replica and still crosses the network once per replica, and neither of
those divides by cores at any thread count. Only the 2.17 multiplier's
second term gets cheaper in time, and the margin S1 measured — 396× the
pack reads, 144× still at 8 threads — is three orders of magnitude clear
of anything that correction moves.

Two results S1 was not asked for but which its numbers settle:

- **`dentry-auth` stays on the table** rather than being discarded with
  `nocopy`. It is the only measured shape that writes less on *every*
  axis while keeping `ls -la` at one pack: 24.2M keys instead of 35.8M,
  13.6% fewer compressed leaf bytes, 25% less interior, and aged
  directory-local `setattr` at **1.01×** today's log bytes where `copy`
  pays 15.9×. It charges on `getattr(ino)` — 7.92 cold pack reads against
  4.00, roughly half the throughput — which is a bad trade for the op
  FUSE uses most, and `link()` would have to migrate a record into `0x01`
  when `nlink` rises, a product complexity this plan has not costed. The
  trigger for revisiting it is `setattr` cost becoming binding on the
  compactor budget.
- **§14.8's aged row is superseded.** Its aged corpus let the densest
  tracked directory shrink to 1.7k children, understating ino scatter by
  roughly 7×; with hot-directory retention and churn it holds 4,241
  children over 134 distinct ino buckets, and aged directory-local
  `setattr` is **15.9× today's log bytes, not §14.8's 2.1×**. Same gate,
  failed by a wider margin. Read §14.10's rows in preference to §14.8's.

### S1b — Ino allocation locality

**Why this exists.** §14.10 reading 3 relocates the write-amplification
problem: the expensive key is the scattered `0x01` inode write, not §P6's
second key. And §14.10 bounds the prize precisely, because the *same*
encoding costs **2.3× today's log bytes on a fresh tree and 15.9× on an
aged one** — a ~7× gap that is entirely ino locality, since `alloc_ino`
is a global `counter++` and a directory's children therefore drift apart
in the `0x01` range as the filesystem ages. Most of `dentry-auth`'s
measured advantage is just that it never writes that key.

**Deliverable.** In `bench/prollybench`, an ino allocation policy that
places a new inode's number near its parent directory's — a per-directory
cursor into a reserved range, with a documented overflow rule when a
range fills — measured against today's global counter on §14.10's aged
corpus: directory-local and scattered `setattr` bytes per op, `ls -la`
pack reads, `getattr` cost, and the ino-bucket spread statistic §14.10
introduced (children per directory over distinct 1024-ino buckets).

**Why it is cheap to get wrong, which is why it comes before S3.** An ino
is already opaque and allocation is *policy*, not format: existing
filesystems keep their inos, only new allocations cluster, and no bucket
migration is implied. So unlike the attr copy this can change after (B)
ships — but knowing the number now tells us whether the aged-tree
amplification is a format problem at all, and it is cheap to measure.
**Gate:** none. It informs §P6 and §S1's `dentry-auth` trigger; it does
not block S3.

Original work order, for the record:

**Why this is first.** §14.8 leaves exactly one format question open, and
option (B) makes the format final, so it must be answered before §P6 is
frozen. §P6 keeps a denormalized attr copy in the `0x02` dentry so that
READDIRPLUS is a pure sequential scan. §14.2 confirms the payoff — a cold
`ls -la` touches one distinct pack — and §14.4/§14.8 confirm the price:
every `setattr` writes two keys in two distant ranges, which is 83–93×
today's bytes for directory-local setattr and roughly doubles the
scattered case. Neither number settles the trade, because **the
alternative was never measured.**

**Deliverable.** A no-attr-copy variant in `bench/prollybench` (dentry
value becomes `(ino, kind)` only, so `ls -la` is one `0x02` range scan
plus point reads into `0x01`), measured against the current encoding on:

- `setattr` bytes per op — the §14.4 and §14.8 rows recomputed, clustered
  and scattered, fresh and aged, at 1k/10k/100k.
- cold and tier-(b) `ls -la`: distinct packs touched and wall time, at 1
  and 8 threads. §14.9 showed the read path is thread-scalable, and that
  asymmetry may be what decides this.
- total leaf bytes, since the copy inflates every replica, not just
  writes.
- the same against an aged tree, where ino/directory decorrelation is
  supposed to make the no-copy variant worse.

**The asymmetry to weigh explicitly.** Read amplification is absorbed by
cache and by cores (§14.9) and is paid once per cold directory; write
amplification is permanent, is paid in S3 bytes by every replica, and is
paid *again* by the compactor (§14.5, which rewrote 117% of what the
commits wrote). State it, then recommend.

**Gate.** No product code changes. `cargo test --release` in
`bench/prollybench` stays green. Results land as §14.10 with an explicit
recommendation, and in `bench/prollybench/RESULTS.md`.

### S2 — `crates/mtree`: the pure data structure — **DONE (2026-09-12)**

Landed as `crates/mtree`, 3,321 lines, no existing crate's source touched:
`node.rs` (format, encode/parse, boundary function, aggregates),
`tree.rs` (build, cursor, apply, diff, merge), `config.rs`, `hash.rs`,
`store.rs` (a `NodeStore` trait plus a counting in-memory impl), and 40
tests — the eight required properties, a `BTreeMap`-oracle fuzz, and an
8,000-case corruption fuzz asserting no panic. `cargo fmt` clean, clippy
`-D warnings` clean, `cargo test --workspace` 531 passed / 0 failed
(independently re-verified; the single `node_runtime` FUSE-mount test
needs `/dev/fuse` and passes where it is exposed).

Four decisions worth carrying forward, all recorded in `PROGRESS.md`:

- The clamps, the hasher, and the aggregate projection live in a
  `Config` validated at construction and documented as a **format**
  parameter set rather than a tuning surface — two trees over one key set
  with different clamps are different trees. §P1 and §P13 above are
  amended to match what was built.
- The aggregate is the fixed §P7 triple plus a key count, fed by a
  caller-supplied leaf→`Agg` projection. It has to be caller-supplied:
  only the §P6 codec knows which key range holds the authoritative inode
  record, and therefore which entries may count a file's bytes without
  double-counting a denormalized dentry copy — **which is exactly what S1
  is deciding.** `mtree` must not be able to guess.
- Decode is split into an O(1) constructor and an O(entries) `validate`,
  because key *ordering* cannot be checked lazily. `parse` is the
  documented entry point for bytes arriving off a network.
- No `unsafe`. prollybench's cursor launders a borrow through
  `from_raw_parts`; the productionized cursor does not.

The node header changed deliberately — `b"MTRE"` + an explicit
`FORMAT_VERSION` byte, 10 bytes against prollybench's 9 — so node hashes
differ from §14's. Everything after the header is byte-identical and the
entry partition is unchanged, so §14's *measurements* carry over even
though its hashes do not. Nothing exists in either format on any bucket,
so there is no migration; `FORMAT_VERSION` is the story from here, and a
test pins the encoding hex, the plain and keyed root hashes of a fixed
1,000-key set, and the level census.

Original work order, for the record:

`bench/prollybench/src/node.rs` (333 lines) and `tree.rs` (1,041) already
implement this, and they are what §14 measured, so this is a
productionization rather than a green field. Read them first and preserve
every property they assert.

**Deliverable.** A new workspace crate `crates/mtree`, pure and
synchronous — no S3, no tokio, no filesystem — providing:

- **Node format**: magic, level, entry count, offset table, then leaf
  `(key, value)` pairs or interior `(first_key, child_hash, aggregate)`
  triples. Explicitly versioned, because this is an on-bucket format.
- **A boundary function over keys only**:
  `u32::from_le_bytes(blake3(key)[0..4]) < u32::MAX / TARGET_ENTRIES`,
  with `MIN_ENTRIES`/`MAX_ENTRIES` clamps. §14.1 settled that the clamp
  is canonicality-safe — a node's *start* is itself context-free, so
  "seal after MAX entries counted from the start" remains a pure function
  of the key set — and measured `MAX_ENTRIES = 256`, which clips a
  p99-of-536, max-of-1545 entry tail (a ~120 KiB worst-case leaf) at no
  measurable read cost. Specify the clamp; do not omit it.
- Bulk build, point read, an ordered cursor (next and seek), incremental
  apply of a sorted key delta, structural `diff`, three-way `merge`
  returning either a merged root or an exact conflict key set, and
  augmented subtree aggregates (bytes, count, max mtime) as a monoid.
- blake3 node hashing with a **keyed** mode for §P13, so E2E addressing is
  not retrofitted later.

**Property tests** — the reason the crate exists, and non-negotiable:
order independence over 50 random insertion orders; an incrementally
applied tree byte-identical to a bulk-built one; delete-then-reinsert
returning the original root; diff cost tracking the difference rather
than the state; disjoint branches merging to one hash from both
directions; overlapping branches yielding exactly the overlapping key
set; and the clamp preserving canonicality **under deletes**, which is
the case that forces a clamped run to absorb its right neighbour.

**Gate.** Added to `members` and `default-members`. fmt, clippy
`-D warnings`, and `cargo test --workspace` green. No other crate
changes.

### S3 — `mtree::keys`: the §P6 codec — **DONE (2026-09-12)**

`crates/mtree/src/keys.rs` (the five ranges, `Key::parse`, `KeyRange`)
and `crates/mtree/src/record.rs` (the values, the inline/spill planners,
the aggregate projection), plus `crates/mtree/tests/keys.rs`. 68 tests in
`mtree`, up from 14. S1's verdict is implemented as written: the `0x02`
dentry carries the attr copy. Verified independently: the tree core
references neither module, `mtree`'s dependency list is still exactly
`blake3` + `thiserror`, and `0x10`–`0x2f` is refused by name on decode.

**S2's aggregate gap is closed.** `record::leaf_agg` contributes only for
`Key::Inode`; `0x02`, `0x03`, `0x04` and `0x30` contribute nothing, so
the dentry copy cannot inflate a total. This was the one place in the
plan where an error would have been *consistently* wrong — the root hash
attests to the aggregate, so every replica would agree on an inflated
`du`. Its test computes the model total from its own inode map rather
than from the tree, over a corpus with hard links so a double count
shows, and two guards stop it passing vacuously. Confirmed against
`meta::sqlite::recursive_size_conn`: both count size and files for
`kind == File` only, so `du`, `statfs` and quota do not change meaning
when the tree becomes their source.

Six decisions carried forward, the first three of which **S5 depends
on**:

1. **`mtree` gained no dependency at all** — not `constellation-meta`,
   and not `constellation-fs-core` either. The record types are declared
   locally from `meta`'s schema, and a file's chunk list is carried as
   opaque encoded-manifest bytes so the crate needs no manifest decoder.
   The `meta → mtree` edge S5 wants is therefore still open.
2. **`Kind`'s discriminants match `fs_core::InodeKind::as_u8`** (0–6,
   verified pairwise), so S5's mapping is a cast rather than a match.
3. **`plan_inode`'s spill order is format, not heuristic**: xattr set,
   then manifest, then symlink target, until the record fits
   `VALUE_SPILL`. Two writers that spilled different fields would
   produce different bytes for the same filesystem and so different root
   hashes. Inline xattrs are sorted for the same reason — canonicality
   reaches into the values, not just the key order.
4. **Keys are big-endian, values little-endian.** Only keys are compared,
   so key endianness is load-bearing and value endianness is free; LE
   matches every other encoder in the repo.
5. **atime is absent rather than excluded.** `Attrs` has no atime field,
   so there is no way to reach a key *or a value* with it, and
   `ATTRS_LEN` is pinned at 49 bytes with its nine fields enumerated —
   adding a tenth fails a test on purpose. atime stays node-local in
   today's `atime_journal` with max-merge on apply.
6. **Names go last and ids are fixed width, which is load-bearing rather
   than tidy.** A name may be a byte prefix of another name, so any field
   appended after a name would make the field boundary undecidable. Three
   ordering traps were found and closed: a directory's exclusive upper
   bound needs a general prefix successor (it cannot be `parent + 1`,
   which overflows at `u64::MAX`), a nameless key is a decode error
   because it is a scan bound and storing it would make a directory's
   lower bound ambiguous, and an over-long `0x01` key is refused because
   trailing bytes would let two distinct keys resolve to one inode.

Deferred with reasons: the `0x30` record *bodies* (S5/S6 — inventing
them here would freeze a format nothing writes), range aggregates for
per-directory `du` (the keys exist; the API belongs with the caller that
answers it), and `dentry-auth` (§14.10 reading 5 keeps it on the table
with a trigger; building both shapes now would double the codec).

Original work order, for the record:

**Deliverable.** The §P6 table as a codec: `0x01` inode, `0x02` dentry
(shape per S1's decision), `0x03` spilled xattr, `0x04` reverse dentry,
and `0x30` subsystem records, with `0x10`–`0x2f` reserved and unused.
Big-endian throughout so byte order is numeric order. `XATTR_INLINE`
(256 B) inlining and `VALUE_SPILL` (1 KiB) blob spill.

**Tests.** Round-trip for every range; an ordering test asserting that
encoded byte order equals numeric and lexical order for every field,
including `ino` boundary cases; and §12's mechanical **"no mutable field
is in the tree"** test — every in-tree key built only from fields
immutable for the entity's lifetime. That test is what keeps the
retracted secondary indexes (§P5) and atime (§P6) from creeping back.

**Gate.** As S2.

### S4 — Pack store, node cache, commit chain — **DONE (2026-09-12)**

Landed as `crates/store-s3/src/{packs,node_cache,commits}.rs` (2,668
lines) plus five `layout.rs` helpers — purely additive, with nothing
calling it yet and `checkpoints/*`, `LATEST` and `VECTOR.json` untouched.
110 store-s3 tests (up from 93), `cargo test --workspace` 565/0,
`tests/smoke.sh` passed; independently re-verified.

- **The index placement is forced, not chosen** — see the correction in
  §P8 above, which is the substantive design change this step produced.
- **§14.2's one-pack-per-directory property is confirmed against a real
  store**: 64 directories × 500 dentries into 256 KiB packs, scanned from
  a cold cache, gave **60 of 64 directories in exactly one pack and none
  worse than two** (the stragglers straddle a pack seal), with a control
  in the same test reading scattered dentries and touching many packs so
  the assertion cannot pass vacuously. Packs are filled in
  `(level, first_key)` order, which is what turns §P6's key locality into
  pack locality — and sorting level first is also what will make S6's
  interior-only bootstrap a few whole-pack GETs.
- **The crash-ordering invariant has a real test**, not a comment:
  commit PUTs are failed, orphan packs are asserted present with no
  commit, every existing commit is walked and its roots resolved from a
  cold cache, and the retry is asserted to converge on the same
  content-addressed packs. A second test fails a pack's index PUT and
  asserts the re-seal *completes* the half-written pack instead of
  duplicating it.
- **An obligation handed to S5**: `publish` CASes at `parent + 1` where
  `parent` is the root the writer actually read — never at the current
  head, which would silently overwrite whatever landed in between.
  §P3's structural rebase is therefore a **caller-supplied callback**,
  because it needs the key codec and operation semantics that live above
  this crate. S4 guarantees only that a loser's payload is carried
  forward; **S5 must supply the real rebase**, and until it does, a lost
  CAS is an error rather than a merge.
- Hooks left: a `flags` byte on both pack objects with
  `FLAG_SEALED_NODES` defined and refused on read, so §P13's AEAD sealing
  is a flag flip plus a seal/open pair with no format bump; `NodeCache`
  already takes an `mtree::Hasher` for keyed mode. For §S7:
  `PackStore::{get_body, contains}`, `CommitChain::list_from`, and a
  `PackIndex` carrying each node's level and first key so a compactor can
  rewrite a partially dead pack in key order.
- Knobs: `CONSTELLATION_PACK_TARGET_BYTES` (4 MiB, 1–16 accepted),
  `CONSTELLATION_NODE_MEMORY_BYTES` (64 MiB),
  `CONSTELLATION_COMMIT_PROBE_WINDOW` (8).
- Known gap, deliberately left: no single-flight on duplicate concurrent
  misses. A lock there would sit on the exact path §14.9 says must scale,
  to save a duplicate ranged GET of an immutable 8 KiB node. Revisit only
  if measured.

Original work order, for the record:

**Deliverable**, in `crates/store-s3`:

- A `packs/<hash>` writer and reader — ~1–16 MiB sealed concatenations
  with an index (inline in the commit, or `packs/<hash>.idx`) and ranged
  reads for single nodes. Pack **in key order**: §14.2's "one distinct
  pack per directory" is the property to preserve.
- A node cache over `fs-core::cache` verbatim — the same eviction, the
  same blake3 verification, and the same peer-then-S3 resolution ladder
  the data plane already uses for chunks. Implement `mtree::NodeStore`
  against it; S2 defined that trait as the seam for exactly this.
  **Every node read from a pack must go through `NodeRef::parse`, not the
  O(1) constructor**: these are bytes off a network, and the ordering
  check is the part that cannot be done lazily.
- `commits/<seq:016x>`, CAS-created with `If-None-Match: *`, carrying the
  §P2 fields (`seq`, `parent`, `roots`, `packs`, `author`, `epoch`,
  `agg`, `intent`). Head discovery by GET-next probe over `seq+1..seq+k`
  with LIST only as the catch-up fallback, reusing plan 26's tailer shape
  and `store-s3::log`'s CAS.
- `layout.rs` helpers for both prefixes.

**The ordering invariant, which is the whole crash-safety argument**:
every node and pack a commit names must be durable *before* the commit
object is CAS-created. A crash before it leaves orphan packs, which are
garbage that S7 reclaims; a crash after it leaves a commit all of whose
nodes exist. There is no third state. Test it with `InMemory` plus an
injected failure between the pack PUT and the commit CAS.

**Gate.** CONVENTIONS gates 1–2, plus `InMemory` unit tests for CAS
contention (two writers, one 412, correct retry) and for head discovery.

### S5 — Builder from the live replica — **(B) complete** — **DONE (2026-09-14), gates partially open**

Landed by the S5 subagent before it stalled, verified and gated by the
coordinator. Code: `crates/cli/src/mtree_publish.rs` (~1.6k lines),
`crates/store-s3/src/blobs.rs`, shipper/`node_runtime` wiring (publisher
**on by default** for non-read-only mounts), additive `meta` helpers.
Seven `mtree_publish` unit tests green, including the disjoint-publisher
splice that fails a blind retry, incremental ≡ full rebuild, atime
exclusion, and pack-before-commit spill.

**Gates run (coordinator, 2026-09-14):**

| Gate | Result |
|---|---|
| `cargo fmt --all --check` | pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass |
| `cargo test --workspace` | **625 passed**, 1 failed in sandbox only (`two_views…` needs `/dev/fuse`); **passes unsandboxed** |
| `mtree_publish` (7 tests) | pass |
| `tests/smoke.sh` | **PASSED** |
| `tests/integration.sh` | **PASSED** |
| `docker compose … compliance` (pjdfstest) | **interrupted** — `.dockerignore` fixed to keep `bench/uploadbench` (workspace member). Second attempt was compiling inside the image when stopped; **pjdfstest still not executed**. |
| `target/release/harness run` | **interrupted mid-matrix** — early results: `baseline`/`slow-network`/`s3-outage`/`s3-flap`/`kill9-remount` **FAILED** (MODEL DIVERGENCE, truncated sizes); `latency`/`cold-cache`/`two-clients-disjoint` **PASSED**. Not diagnosed; continue later. |
| `getattr`/`lookup` p99 during publish | **not measured** |

§11a's store knobs are now in `docs/reference/configuration.md`
(Merkle metadata tree section). GC knobs remain S7b.

Original work order, for the record:

**Deliverable.** Build an `mtree` from `SqliteMeta` over the §P6 encoding
and publish it as a commit, wired into `cli/src/shipper.rs` where
`checkpoint` lives. After this step the bucket format is final (ADR-5's
no-migration promise), plans 26 and 27's checkpoint goals are met, and
`SnapshotManager::build_tree` is replaced by retaining a root hash (§13).

The incremental path is the point: a publish must cost O(keys changed
since the last commit), not O(DB). Drive it from the existing journal and
`applied_seq` watermark to find the changed key set, then `mtree::apply`.
A full rebuild happens only on first publish and in `fsck`.

**S5 also owes S4 the rebase callback.** `CommitChain::publish` CASes at
the parent the writer read and hands a lost race back to a
caller-supplied closure, because resolving it needs the §P6 codec and
operation semantics that `store-s3` cannot see. Until S5 supplies it a
lost CAS is simply an error. The §P3 shape to implement: diff the
winner's roots against the parent, and if the difference does not
intersect this batch's read-set, splice the write-set onto the winner and
retry; otherwise re-execute. In (B)'s scope the writer still holds the
partition lease, so a lost CAS should be rare and the disjoint splice is
enough — but it must be a real splice, not a blind retry, or two
publishers can silently drop one another's keys.

**Gate.** The full CONVENTIONS gate list from here on, pjdfstest
included. Plus: publishing must not stall FUSE. Report `getattr`/`lookup`
p99 during an active publish per §13 — eliminating the `VACUUM INTO`
stall is one of this plan's claims, and replacing it with a tree-build
stall would be a regression, not a win.

### S6 — Reader: bootstrap, partial replica, `fsck` — **DONE (2026-09-21)**

Landed in `cli/src/mtree_read.rs` (bootstrap loader, `TreeReader`,
`0x30` codec), `shipper::bootstrap_from_tree`, and
`fsck::check_metadata_tree`; details and exit criteria in `PROGRESS.md`.
Carried forward:

- **Commits carry an `applied` vector** (partition → highest applied
  segment), read in the same SQLite snapshot as the publisher's plan. It
  is both the bootstrap's resume point and the guard that closed the S5
  regression bug: a replica may only build on, or splice onto, a commit
  whose vector its own covers.
- **`0x30` is written**: snapshot rows, the replicated quota, and a new
  `Subsystem::Partition` for the partition map. `xpart_pending` is not
  carried; the publisher defers while a rename half is parked instead.
- **Readers use the pack catalog, never a commit's `packs`**, and the node
  cache refreshes it on a miss — a tree lives mostly in ancestors' packs
  and, after S7b's compaction, in packs no commit names.
- **The checkpoint is retired**: a publishing mount writes no `VACUUM
  INTO` snapshot (`CONSTELLATION_CHECKPOINT_SNAPSHOT=on` restores it), and
  log retention floors on the head commit's vector.
- **Snapshots are retained roots** `(seq, root, dir ino)` after a forced
  publish; `build_tree` and the legacy tree-blob snapshots are gone.
- **The partial replica is a reader API**, tested at reader level (§12's
  shape). Mounting from it is §11b's engine swap.
- Not measured: bootstrap wall time on the plan 26 Appendix paths (no
  remote bucket); locally 100k inodes load in 358 ms from a cold cache.

Original work order, for the record:

**Deliverable.** Rebuild a replica from a commit — plan 27's goal, now
against a canonical format. A full bootstrap, and a **partial** one that
fetches only the interior plus the leaves the mounted path touches, which
is ADR-5's 100M+ path and which §14.1's 19.75 MiB interior makes cheap.
`fsck` becomes "recompute the root hash and compare", verifying every
byte of metadata rather than walking and comparing.

**Tests.** Plan 27's table-by-table equality between the source replica
and one rebuilt from the tree — kept, because it remains the honest check
that builder and reader agree. Plus §12's partial-replica test: a node
budget that fits the interior only, `ls -la` of a cold directory costing
one pack GET, and a `readdir` walk that never faults in leaves outside
the directories it visits.

**Gate.** Full list, plus bootstrap wall time for full and partial
replicas on the plan 26 Appendix paths (§13).

### S7 — GC by reachability + pack compaction

**Split into S7a and S7b after S4 landed.** S4 deliberately left the
primitives this needs (`PackStore::{get_body, contains}`,
`CommitChain::list_from`, and a `PackIndex` carrying level and first key
so a partially dead pack can be rewritten in key order), so the mark and
the compactor are implementable and testable as library code against
synthetic commits, with no dependency on S5's publisher. **S7a** is that
library work in `store-s3`; **S7b** is the `cli/gc` wiring, the retention
policy, and the rate budgets, which do need a real commit stream and so
follow S5. The deliverable below is split accordingly — everything about
marking, sweeping and compacting bytes is S7a; everything about *when*
and *how fast* is S7b.

**S7a is DONE (2026-09-12)**, in `store-s3`'s `mark.rs`, `compact.rs` and
`parallel.rs` (2,195 lines) plus one additive `pub fn` in `packs.rs`.
Additive: nothing calls it, no `cli` file is touched, and today's chunk
GC is unchanged. 131 tests pass in `store-s3`, clippy clean. What S7b
inherits, and the three things worth knowing before wiring it:

- **§P10's affordability claim is now an identity, not an inequality.**
  Marking a 17-root chain over a 60,000-key tree visits 690 nodes where
  one root visits 574 — a marginal cost of 7.25 nodes per retained
  commit, against 9,758 for a walk that restarted per root. The test
  asserts `visits == nodes.len()`, so any re-entry into a shared subtree
  fails it, and it carries a non-vacuity guard so it cannot pass by the
  deltas happening to be large.
- **The crash-ordering rule is S4's, mirrored**: a replacement pack must
  be durable before the original it replaces is deleted. The two crash
  outcomes are deliberately asymmetric — PUTs-without-DELETEs leaves a
  live node with two identical copies, which no reader can distinguish
  and the next round reclaims, while DELETEs-without-PUTs loses data
  irrecoverably. So one failed PUT aborts the batch with nothing
  deleted, and both sides have a test that injects the failure and then
  resolves the whole live set from a cold cache.
- **A pack body with no `.idx` sibling is reported, never swept** — it is
  both the state a crash between `put_pack`'s two PUTs leaves *and* the
  state a healthy writer is in for a few milliseconds during every seal.
  Deleting it safely needs the age horizon and condemned-list handshake
  today's chunk orphan pass uses, which is policy, and therefore S7b's.
  `PackCatalog::incomplete` is what that policy reads.

**Deliverable.** Mark from the bucket root set — the newest commit, the
retained window, every `snaps/*`, every clone, every unexpired
`holds/*` — as a Merkle walk that terminates on shared subtrees, so
marking N commits costs O(their differences). Sweep unreferenced packs
and compact partially dead ones, under a rate budget
(`CONSTELLATION_COMPACT_BYTES_PER_S`) and with a restartable cursor,
because nothing depends on either completing promptly.

**Size this from §14.5, not from §P10b's original optimism**: 0.7% of
packs died whole, the compactor rewrote 117% of the bytes the commits
themselves wrote, and §14.9 found that mark parallelizes ~4× while
compaction reached only ~1.7×. **S7a has since done this work**: mark is
parallel, the writer is concurrent, and the flat phase §14.9 missed (a
`.to_vec()` in `get_body`) is gone, so end-to-end compaction is 2.78× at
4 threads and ~260 MiB/s. Size S7b's budget from that: §14.5's 14.79 GiB
per round is ~58 s at full width, about 3 cores while it runs. The
`deref` table, its txid bookkeeping,
and the `superseded-checkpoint` rule go away; the `gc.horizon`, the
condemned-list handshake, the offline exemption, holds, and reintegration
verification all stay (§P10).

**Gate.** Full list, plus a re-run of §0.2b's steady-state shape against
the real store: the footprint must plateau.

**S7b is DONE (2026-09-21)**, in `cli/src/mtree_gc.rs` as a second phase
of `gc::run`: commit retention, a mark from retained commits, snapshot
roots and holds, the sweep and paced compaction with a restartable kv
cursor, and index-less packs past the horizon. The delete-vs-dedup race
turned out to have a metadata twin that S7a's library could not see: a
publisher's `NodeCache::put` skips uploading any node whose location it
already knows, which can *resurrect* a node the mark found dead and name
a pack GC is about to delete. It is closed by the same shape as the chunk
handshake — `gc/condemned-packs.json`, a lease-TTL wait and a re-mark on
the GC side; no dedup against a condemned pack plus a pre-CAS check of
every pack the batch trusted on the publisher side. The plateau gate is
the harness scenario `mtree-gc-plateau`. `blobs/` is not swept yet (see
`PROGRESS.md`).

### 11a. Carried debt: the config knobs are undocumented

`docs/reference/configuration.md` is this repo's canonical env-knob
reference. Plan 28 knobs:

| Knob | Added by | Documented by |
|---|---|---|
| `CONSTELLATION_PACK_TARGET_BYTES` (4 MiB) | S4 | **S5 — done** (`configuration.md`, Merkle metadata tree) |
| `CONSTELLATION_NODE_MEMORY_BYTES` (64 MiB) | S4 | **S5 — done** |
| `CONSTELLATION_COMMIT_PROBE_WINDOW` (8) | S4 | **S5 — done** |
| `CONSTELLATION_GC_THREADS` (one per core) | S7a | **S7b — done** |
| `CONSTELLATION_COMPACT_BYTES_PER_S` (32 MiB/s) | S7b | **S7b — done** |
| `CONSTELLATION_COMMIT_RETENTION` (64), `..._RETENTION_S` (86400) | S7b | **S7b — done** |
| `CONSTELLATION_BOOTSTRAP_SOURCE` (`auto`) | S6 | **S6 — done** |
| `CONSTELLATION_CHECKPOINT_SNAPSHOT` (`off`) | S6 | **S6 — done** |

### 11b. Out of scope here — the engine swap

Deliberately deferred, in dependency order, each needing its own plan:
`MetaStore` backed by `mtree` + WAL + memtable with SQLite demoted to a
derived view (§P5); optimistic commit with structural rebase and read-set
capture in the cursor (§P3); deleting partitions in favour of keyspace
shards under one commit (§P4); and §P9 macros. §14.7.7 notes that gate
0.1(b) no longer *refuses* the engine swap once concurrency is counted
(§14.9) — it is still the larger bet, and (B) sequences it after the
format lands.

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

Per CONVENTIONS, plus paste into the report: the Step 0 tables (§14);
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

### 14.8 §0.2 aged tree (added after §14)

The fresh-tree clustered numbers lean on an unstated property: a one-pass
import keeps `0x01` order correlated with directory order because
`alloc_ino` is `counter++`. Age the census tree with 10 generations of
create / rename / unlink (20k ops each, 41.5 s), then re-measure both
create-shaped clustered commits and directory-local setattr — the shape
whose inode leaves are supposed to scatter.

| Tree | Shape | ops | B/op (zstd) | today's B/op | ratio |
|---|---|---:|---:|---:|---:|
| fresh | clustered create | 1k / 10k / 100k | 137 / 76 / 68 | 45 | 3.02× / 1.68× / 1.51× |
| fresh | clustered touch (one dir setattr) | 1k / 10k / 100k | 485 / 64 / 6 | 6 / 4 / 3 | **83× / 15× / 2.3×** |
| aged | clustered create | 1k / 10k / 100k | 151 / 75 / 68 | 45 | 3.34× / 1.66× / 1.50× |
| aged | clustered touch (one dir setattr) | 1k / 10k / 100k | 454 / 46 / 5 | 5 / 3 / 2 | **93× / 17× / 2.1×** |

**Gate 0.2 aged "clustered ≤ 4× today's log bytes": PASS for create
(worst 3.34×), FAIL for directory-local setattr (worst 93×).**

Two readings, and the second is the one that matters:

1. **Creates do not care about age.** New inos are still sequential
   (`alloc_ino` is still `counter++`), so a clustered create against an
   aged tree costs what it costs against a fresh one. The encoding does
   not need the "dentry authoritative for `nlink == 1`" escape hatch for
   create-shaped work.
2. **Directory-local setattr was never cheap, even on a fresh tree
   (83× at 1k ops).** Aging makes it only marginally worse (93×). The
   cost is §P6's dual-key write — every setattr rewrites a `0x01` and a
   `0x02` — not the decorrelation the gate was written to catch. Fresh
   burst-filled directories already have mostly-contiguous inos, and after
   aging the densest tracked directory is smaller (1.7k unique keys at
   100k ops vs 10k fresh), so the measurement understates the aged
   ino-scatter case rather than overstates it. The format decision the
   gate was meant to force is still open, but the evidence points at the
   attr copy itself (§14.7.6), not at aging.

### 14.9 §0.5 Thread scaling (added after §14)

Single-threaded tier (b) was 42–50 k/s at a 64 MiB leaf cache. Immutable
nodes make a lookup a pure function of `(root, key)`, so the miss path
(ranged read + zstd) should parallelize. Measured at census scale, 400k
lookups, 64 MiB leaf cache (packs under `/tmp` so a concurrent syncthing
daemon was not hashing the pack rewrite storm):

| Threads | (a) lookup/s | (a) scaling | (b) 64 MiB lookup/s | (b) scaling | pack reads/lookup |
|---:|---:|---:|---:|---:|---:|
| 1 | 694 k/s | 1.00× | 51 k/s | 1.00× | 0.94 |
| 2 | 1.23 M/s | 1.77× | 97 k/s | 1.90× | 0.94 |
| 4 | 2.01 M/s | 2.90× | **188 k/s** | 3.69× | 0.94 |
| 8 | 2.32 M/s | 3.34× | **326 k/s** | 6.41× | 0.94 |
| 16 | 2.29 M/s | 3.31× | 389 k/s | 7.65× | 0.94 |
| 32 | 2.34 M/s | 3.38× | 382 k/s | 7.51× | 0.94 |

**Gate 0.5: tier (b) ≥ 150 k/s aggregate at ≤ 8 threads with a 64 MiB
leaf cache → PASS (326 k/s at 8 threads, already clear at 4 with
188 k/s).** Near-linear to 8 threads on the miss path; (a) saturates
around 3.3× on this host's memory bandwidth. §14.7.1's "needs ~1 GiB of
leaf cache" therefore weakens exactly as the gate hoped: **~1 GiB *or*
4 cores with a 64 MiB cache clears 150 k/s on a uniformly random working
set.**

Commit build across §P4-shaped key-range shards (32k clustered ops,
apply each shard from the common root in parallel, merge left-to-right):

| Shards | wall | vs 1-thread |
|---:|---:|---:|
| 1 (whole) | 58 ms | 1.00× |
| 2 | 79 ms | 0.73× |
| 4+ | slower | merge dominates |

Shard-parallel apply does not beat a single-threaded apply of the same
batch once the merge is counted; the win, if any, is latency hiding
across independent writers, not wall-clock of one commit.

Mark + compaction against a dirty tip (1.04 GiB live / 1.25 GiB dead,
1,995 partially-dead packs):

| Threads | mark | mark scaling | compact MiB/s | compact scaling |
|---:|---:|---:|---:|---:|
| 1 | 8.8 s | 1.00× | 167 | 1.00× |
| 4 | 2.7 s | 3.20× | 281 | 1.68× |
| 8 | 2.1 s | 4.16× | 279 | 1.67× |
| 16 | 2.3 s | 3.88× | 256 | 1.53× |

Mark scales to ~4×; compaction tops out around 1.7× because the pack
writer is serial. §14.5's "GC is most of the machine" becomes "mark is a
few cores, compaction is ~2" — better, not free. The 939 s of 1,205 s
finding does not divide by 32.

> **The compaction half of this row is superseded by S7a.** The
> diagnosis was only partly right: making the pack writer concurrent
> took the *writer* to 3.17×, but left end-to-end near 1.95×. The larger
> flat phase was `PackStore::get_body` ending in `.to_vec()` — reading
> pack bodies cost 219 ms and did not shrink with threads at all, 44% of
> the 8-thread wall clock, because a `memcpy` is a `memcpy` on any
> number of cores. Reading bodies as `bytes::Bytes` and slicing frames
> out of them took that phase to 0.6 ms. End-to-end compaction is now
> **2.78× at 4 threads** against this row's 1.68×, and the writer's
> serial fraction is 0.007% of the rewrite. The residual is this host's
> memory bandwidth, not code: every parallel phase independently
> saturates at 3.3–3.7× on 4 physical cores, which is where this row's
> own memory-resident lookup column saturates too. Sizing for S7b
> becomes **~260 MiB/s on four cores**, so §14.5's 14.79 GiB per round
> is ~58 s at full width and "compaction is ~2 cores" becomes "~3 cores
> while it runs" — with the rate budget, not the thread width, deciding
> how much of the machine that is. The lesson worth keeping: measuring
> only the component this row blamed would have missed the real ceiling.

### 14.10 S1 — settling the dentry attr copy (added after §14)

§14.4 and §14.8 both ended by pointing at §P6's denormalized attr copy
without measuring the alternative, so measure it. Three `0x02` value
shapes run through the same code paths against the same census corpus,
selected by a flag: `copy` is §P6 as written (ino + a full attr copy),
`nocopy` is ino + kind, so `ls -la` becomes one `0x02` range scan plus a
point read per child into `0x01`, and `dentry-auth` is §11's escape hatch
— for `nlink == 1` the dentry *is* the record and there is no `0x01` key,
so `getattr(ino)` probes `0x04 | ino` for the name and then reads the
dentry. Directories keep their `0x01` record in all three. The aged tree
is §14.8's aging plus a per-generation churn of a quarter of the children
of the eight widest directories, with new inos allocated from a shuffled
pool so churn interleaves with ordinary creates; that fixes the defect
§14.8 admitted to, and the fix is what moves its numbers (below).

| Variant | keys | leaves | leaf bytes | zstd leaf bytes | interior | vs `copy` |
|---|---:|---:|---:|---:|---:|---:|
| copy | 35,819,001 | 306,096 | 2.73 GiB | 1.00 GiB | 19.75 MiB | — |
| nocopy | 35,819,001 | 306,096 | 2.20 GiB | 914.22 MiB | 19.75 MiB | **−10.9%** |
| dentry-auth | 24,218,019 | 207,159 | 2.02 GiB | 886.77 MiB | 14.87 MiB | **−13.6%** |

Four levels in every variant. The zstd column is every leaf compressed at
the pack writer's level rather than a sample, because the variants change
which key range dominates the first leaves and a sampled ratio would not
be comparable across them.

`setattr` bytes per op, the §14.4 and §14.8 rows recomputed per variant,
at 1k / 10k / 100k ops per commit:

| Tree | Shape | Variant | B/op (zstd) | today's B/op | ratio |
|---|---|---|---:|---:|---:|
| fresh | clustered (one dir) | copy | 485 / 64 / 6 | 6 / 4 / 3 | 83× / 15× / 2.3× |
| fresh | clustered | nocopy | 340 / 49 / 5 | 6 / 4 / 3 | 59× / 11× / 1.8× |
| fresh | clustered | dentry-auth | 348 / 34 / 3 | 6 / 4 / 3 | 60× / 7.9× / **1.2×** |
| fresh | scattered (random ino) | copy | 24,811 / 16,018 / 6,721 | 10 / 9 / 9 | 2,460× / 1,773× / 758× |
| fresh | scattered | nocopy | 15,109 / 10,577 / 4,509 | 10 / 9 / 9 | 1,498× / 1,170× / **508×** |
| fresh | scattered | dentry-auth | 19,374 / 13,313 / 5,605 | 10 / 9 / 9 | 1,921× / 1,473× / 632× |
| aged | clustered (one dir) | copy | 3,128 / 412 / 41 | 6 / 4 / 3 | 528× / 103× / 15.9× |
| aged | clustered | nocopy | 3,028 / 398 / 40 | 6 / 4 / 3 | 512× / 100× / 15.3× |
| aged | clustered | dentry-auth | 266 / 26 / 3 | 6 / 4 / 3 | 45× / 6.6× / **1.01×** |
| aged | scattered (random ino) | copy | 24,847 / 15,995 / 6,700 | 10 / 9 / 9 | 2,463× / 1,770× / 756× |
| aged | scattered | nocopy | 15,071 / 10,547 / 4,483 | 10 / 9 / 9 | 1,494× / 1,167× / **506×** |
| aged | scattered | dentry-auth | 19,414 / 13,293 / 5,601 | 10 / 9 / 9 | 1,925× / 1,471× / 632× |

`ls -la` of a wide directory, 32 directories of ≥ 1,000 entries scanned
whole. Distinct packs and pack reads are properties of the operation and
are traced on the single-threaded pass; the tier is re-prepared before
each pass, so the 8-thread column is a second cold run and not a replay
against the cache the serial pass just filled.

| Tree | Tier | Variant | packs/dir | pack reads/dir | ms/dir (1t) | ms/dir (8t) | scaling |
|---|---|---|---:|---:|---:|---:|---:|
| fresh | (b) 64 MiB | copy | 1.06 | 27.1 | 2.329 | 0.648 | 3.60× |
| fresh | (b) 64 MiB | nocopy | 2.41 | 57.5 | 9.586 | 3.985 | 2.41× |
| fresh | (b) 64 MiB | dentry-auth | 1.25 | 27.1 | 2.604 | 0.660 | 3.94× |
| fresh | (c) cold | copy | 4.06 | 30.4 | 2.248 | 0.459 | 4.90× |
| fresh | (c) cold | nocopy | 5.44 | **12,038.8** | **321.091** | **66.396** | 4.84× |
| fresh | (c) cold | dentry-auth | 2.41 | 30.4 | 3.612 | 0.773 | 4.67× |
| aged | (b) 64 MiB | copy | 1.03 | 26.0 | 4.514 | 0.842 | 5.36× |
| aged | (b) 64 MiB | nocopy | 3.44 | 72.0 | 7.863 | 4.185 | 1.88× |
| aged | (b) 64 MiB | dentry-auth | 1.12 | 26.1 | 2.986 | 0.561 | 5.32× |
| aged | (c) cold | copy | 4.03 | 29.3 | 4.437 | 0.691 | 6.42× |
| aged | (c) cold | nocopy | 6.94 | **11,804.3** | **287.898** | **68.928** | 4.18× |
| aged | (c) cold | dentry-auth | 2.12 | 29.3 | 2.979 | 0.518 | 5.75× |

`getattr(ino)`, which is what the escape hatch charges for: `copy` and
`nocopy` read one `0x01` leaf, `dentry-auth` reads two leaves in two
distant ranges.

| Tier | Variant | getattr/s (1t) | getattr/s (8t) | scaling | pack reads/getattr |
|---|---|---:|---:|---:|---:|
| (b) 64 MiB | copy | 16 k/s | 45 k/s | 2.91× | 0.95 |
| (b) 64 MiB | nocopy | 16 k/s | 55 k/s | 3.43× | 0.95 |
| (b) 64 MiB | dentry-auth | 8 k/s | 28 k/s | 3.55× | 1.93 |
| (c) cold | copy | 8 k/s | 33 k/s | 3.96× | 4.00 |
| (c) cold | nocopy | 7 k/s | 40 k/s | 5.37× | 4.00 |
| (c) cold | dentry-auth | 3 k/s | 13 k/s | 3.83× | 7.92 |

The two amplifications are not commensurable and the decision turns on
that. A read amplification is absorbed by cache and by cores, and it is
paid once per cold directory; the tables above show it thread away at
4–6×. A write amplification is permanent: it is S3 bytes stored on every
replica, it is paid again by the compactor, which §14.5 measured
rewriting 117% of what the commits themselves wrote, and §14.9 measured
that rewrite parallelizing only ~1.7× because the pack writer is serial.
A byte written therefore costs about 2.17 bytes of transfer that does not
divide by cores; a pack read costs a cached read that does.

Five readings:

1. **Dropping the copy is not a trade, it is a loss.** `nocopy` saves
   10.9% of stored leaf bytes and 1.3–1.5× on `setattr`, and it charges
   **12,039 pack reads for a cold `ls -la` against `copy`'s 30.4** — 396×
   — for 321 ms per directory against 2.2 ms. §14.2's "one pack per
   directory" is the whole point of the encoding and `nocopy` gives it up.
2. **That read cost is the one cost in this plan that does not thread
   away.** Every other read row scales 3.6–6.4×; `nocopy`'s cold `ls -la`
   scales 4.8× and is still 66 ms per directory at 8 threads, 144× `copy`
   at the same width. Its tier-(b) scaling is the worst measured anywhere
   (1.88×) because a scan plus 3,000 scattered point reads thrashes the
   64 MiB cache the scan is supposed to fit in.
3. **On the aged tree the write saving nearly vanishes.** Aged
   directory-local `setattr` at 100k ops costs 41 B/op with the copy and
   40 B/op without it — 3%. After aging, that directory's children have
   scattered `0x01` records, and it is the scattered inode write that
   costs, not the second key. §14.4's reading that "§P6's attr copy means
   each `chmod` writes two keys" is right about the mechanism and wrong
   about which key is expensive on an aged tree.
4. **§14.8's aged corpus understated ino scatter by roughly 7×, and the
   corrected corpus is what makes reading 3 visible.** Its densest tracked
   directory shrank to 1.7k children after aging; with hot-directory
   retention and churn it holds **4,241 children spread over 134 distinct
   1024-ino buckets, against 4,994 over 12 when fresh**. Aged
   directory-local `setattr` at 100k ops is **15.9× today's log bytes, not
   §14.8's 2.1×**. Substitute this row for §14.8's; the gate it fails is
   the same one, by a wider margin.
5. **The escape hatch is the only shape that writes less without reading
   worse.** `dentry-auth` holds 24.2M keys instead of 35.8M, 13.6% fewer
   compressed leaf bytes and 25% less interior, reads a directory in the
   same 30.4 pack reads as `copy`, and brings aged directory-local
   `setattr` to **1.01× today's log bytes** where `copy` pays 15.9×. It
   charges for it on `getattr(ino)`: 1.93 pack reads against 0.95 warm,
   7.92 against 4.00 cold, and half the throughput. It does not help the
   scattered case, where it lands between the other two (632× against
   `copy`'s 756× and `nocopy`'s 506×); nothing measured here fixes
   scattered.

**Recommendation: keep the attr copy. §P6 stands as written.** The
no-attr-copy variant buys a 10.9% footprint saving and a write saving
that is 1.4× on a fresh tree and 3% on an aged one, and pays for it with
the single read regression in this plan that neither cache nor cores
absorb.

Two things follow that are not the question S1 asked but are settled by
its numbers. First, `dentry-auth` should stay on the table rather than
being written off with `nocopy`: it is the only measured shape that
reduces writes on every axis while keeping `ls -la` at one pack, and the
trigger for taking it is `setattr` write cost becoming binding on the
compactor budget §14.5 sized. It is not free — `link()` would have to
migrate a record into `0x01` when `nlink` rises above 1, which is product
complexity this plan has not costed, and `getattr(ino)` doubles. Second,
and cheaper: most of `dentry-auth`'s advantage here is that it never
writes the scattered `0x01`, and the same effect is available to `copy`
by allocating inos near the parent directory. The fresh/aged pair bounds
the prize — the identical shape costs 2.3× fresh and 15.9× aged, the
difference being entirely ino locality — so §14.7.6 should name ino
allocation policy alongside the attr copy, and that experiment should run
before the encoding is changed.

Caveat on the wall-clock columns: this host was compiling throughout, so
differences under ~1.3× in the millisecond columns are noise. The byte
counts and the pack-read counts are deterministic and are not.

### 14.7 What Step 0 changes

1. **The single-threaded read gate does not kill the plan, and thread
   scaling rehabilitates the small-cache story.** §P5's "interior + tiny
   leaf cache" framing was wrong for one core (needs ~1 GiB to clear
   150 k/s alone) and right for a host-sized mount: **4 threads × 64 MiB
   clears the bar at 188 k/s, 8 threads at 326 k/s** (§14.9). The tree
   matches SQLite when both are resident (0.72× warm single-threaded;
   well above with a handful of cores) and the partial-replica property
   is real. Stop implying one core + 20 MiB buys ADR-9 latency; do claim
   that a normal concurrent mount does.
2. **§7's "existential risk" is not the one that materialized.** Point
   reads are fine (1.3 µs p50 resident). The costs that measured worse
   than the plan predicted are all on the *write and reclamation* side:
   commit bytes per op (1.5–1.7× today's rather than parity), superseded
   bytes per day (21× the estimate), and compaction (rewriting more bytes
   than the commits write; only ~1.8× faster with threads).
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
   workload, which this run did not test. Parallel mark helps (~4×);
   parallel compaction barely does (~1.8×).
6. **Write amplification comes from the scattered `0x01` write, not from
   §P6's attr copy** — superseded by §14.10, which measured the
   alternative that §14.4 and §14.8 only pointed at. The copy costs a
   second key that is nearly free once a tree has aged (41 B/op with it,
   40 without), while dropping it costs 396× the pack reads on a cold
   `ls -la`. So the copy stays, and the lever worth pulling is **ino
   allocation locality**: the identical shape costs 2.3× today's log
   bytes fresh and 15.9× aged, and that entire ~7× gap is ino scatter.
   §S1b is that experiment. Age does not move *create* cost (§14.8), and
   the `ls -la` benefit is confirmed at one pack per directory (§14.2,
   §14.10).
7. **§9's recommendation (B) survives Step 0 intact and is now the
   evidence-backed choice.** The bucket-format half of the bet — canonical
   Merkle map, §P6 key encoding, packs, commit chain — measured well:
   determinism, O(difference) diff, hash-agreeing merges, scale-invariant
   rename, one-GET directory reads, a footprint that plateaus, and
   thread-scaled reads that clear the bar the single-threaded run missed.
   The engine-swap half (option C, Step 6) is no longer refused by gate
   0.1(b) once concurrency is counted; it is still the larger bet, and
   (B) still sequences it after the format lands.

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
