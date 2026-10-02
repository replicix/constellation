# Constellation Design

Companion documents: [GOALS.md](GOALS.md), [DECISIONS.md](DECISIONS.md),
[TESTING.md](../how-to-guides/development/TESTING.md). The v1 phased
roadmap is at [ROADMAP.md](../plans/v1/ROADMAP.md). This document explains
how the system works and why; the reference pages under
[`reference/features/`](../reference/features/) have the exact rules,
knobs and status fields, and the ADRs record the alternatives rejected.

## Table of Contents

- [1. Overview](#1-overview)
  - [Hard constraints](#hard-constraints)
- [2. S3 Bucket Layout](#2-s3-bucket-layout)
- [3. Data Plane](#3-data-plane)
- [4. Metadata Plane](#4-metadata-plane)
- [5. Write Authority: the One Rule](#5-write-authority-the-one-rule)
- [6. Consistency Modes](#6-consistency-modes)
- [7. Caching and Data Movement](#7-caching-and-data-movement)
- [8. Security](#8-security)
- [9. Failure Handling](#9-failure-handling)
- [10. Control Plane](#10-control-plane)
- [11. Scale Targets](#11-scale-targets)
- [12. Metadata-DB and FUSE Fast Paths](#12-metadata-db-and-fuse-fast-paths)
- [13. Snapshots and Clones](#13-snapshots-and-clones)
- [14. Garbage Collection](#14-garbage-collection)

## 1. Overview

Every node runs the same single binary: FUSE mount + daemon + CLI + web UI.
The only mandatory infrastructure is one S3-compatible bucket, which stores
data chunks, the metadata log and its commit chain, the root lease, and the
node registry. Peers connect directly (iroh QUIC, NAT-traversed, encrypted)
as a latency fast path: they forward mutations to the node that sequences
them, stream the log to each other, back each other up, and serve each
other's cached chunks. Correctness never depends on P2P; S3 remains the
arbiter of record.

```
            +----------------- node -----------------+
  app ----> | FUSE (fuser)                            |
            |   VFS core: sessions, locks, caches     |
            |   authority core (sans-IO): lease,      |
            |     sequencing, delegation, failover    |
            |   meta store (fjall: replica, journal,  |
            |     speculation)                        |
            |   chunk cache (disk, LRU/pin/dirty)     |
            |   iroh endpoint: forwards, log streams, |<===> peers (QUIC)
            |     backups, gossip                     |
            |   control API (unix socket) + web UI    |
            +--------------------+--------------------+
                                 |
                                 v
            S3 bucket: chunks/  log/  commits/  packs/  blobs/
                       leases/  nodes/  inbox/  heartbeat/  ...
```

Trust model: **membership = bucket access.** Valid credentials (`AWS_*` env,
profiles, IMDS, ...) make a node a full member; IAM read-only credentials make
it a read-only follower (auto-detected). Constellation never requires the
ability to change bucket policy or IAM.

Enrollment is self-service on first mount (CAS-claim a cluster-unique node
id into `nodes/`). **Unmount is a temporary departure**: the registry
record stays, so the node remains write-eligible and may return with S3.
**`constellation leave` is the permanent departure** (§8): it retires the
registry record, so the node no longer counts toward a continuation
epoch's quorum (§5.3). Heartbeats and missed pings never drop a member.

### Hard constraints

Five constraints bind every mechanism below (plan 30 §2). A mechanism
that cannot meet one is not built.

1. **Portable S3 only.** GET, PUT, LIST and DELETE, plus
   `If-None-Match: *` and `If-Match` on PUT. Nothing AWS-only: no S3
   Express One Zone, no conditional DELETE, no `RenameObject`
   ([ADR-27](DECISIONS.md#adr-27-portable-s3-only-and-one-bucket)).
2. **One bucket.** A filesystem lives under one prefix of one bucket. No
   second bucket for disaster recovery and no quorum across buckets; a
   bucket outage is survived with authority that already exists (§9).
3. **A single node degrades to plain behaviour.** No peer means no
   backup, no delegation, no promise and no message: a single node pays
   nothing for the cluster machinery.
4. **No WAN round trip on every write.** Anything synchronous with a peer
   (a backup, a delegate) is chosen by measured RTT, never by assuming a
   LAN, so a cluster across continents still acknowledges at local speed.
5. **Safety never depends on failure detection**
   ([ADR-12](DECISIONS.md#adr-12-safety-never-depends-on-failure-detection)).
   Timeouts and silence decide only *when* to try something. What decides
   *whether* it is safe is a CAS on S3, a seal, a log-slot CAS, a lease
   margin, or a promise. A false suspicion costs availability, never
   correctness.

## 2. S3 Bucket Layout

The keys, as `crates/store-s3/src/layout.rs` and `nodes.rs` write them:

```
<prefix>/
  meta.json                       # fs UUID, format version, chunk size, compression,
                                  # gossip secret, E2E key envelope, quota,
                                  # ack_policy, epoch_slack
  nodes/<id:08x>.json             # self-enrolled members (pubkey, P2P addr, ro);
                                  # leave writes a tombstone {retired:true} —
                                  # the id is never recycled
  leases/p0.json                  # the root lease: the one arbiter of write authority (§4)
  leases/_gc.json, _prune.json    # singleton leases for bucket GC (§14) and pruning
  log/p0/<seq:016x>.zst           # the metadata log: ordered, CAS-created segments (§4)
  commits/<seq:016x>              # the commit chain: CAS-created roots of the metadata tree
  packs/<hash>, packs/<hash>.idx  # packed metadata-tree nodes and their indexes
  blobs/<hash>                    # metadata values too large to inline in a tree node
  inbox/<epoch>/<node>/<n>        # forwarded mutations from nodes with no P2P path (§4)
  heartbeat/<node:016x>.json      # continuation-epoch promises, written on demand;
                                  # never written while epoch_slack = 0 (§5.3)
  designations/<path-hash>.json   # offline designations (§5.2)
  snaps/<snap-id>.json            # user snapshots: name, path, tree root hash (§13)
  holds/<node-id>.json            # TTL'd holds the GC honours (§3, §14)
  gc/                             # condemned-set pointers, candidate bookkeeping,
                                  # the deletion journal (§14)
  gc/summary.json                 # the newest chunk round's census of chunks/
                                  # (objects, stored bytes, as-of); read only by
                                  # snapshot space accounting's compression estimate
  prune/journal/                  # the retention pruner's reports
  chunks/<aa>/<bb>/<hash>         # content-addressed data blocks (sharded by hash)
```

The segment, commit, inbox and heartbeat keys are zero-padded hex, so
lexicographic order is numeric order and a LIST with an offset is a seek.
In E2E mode the only secret in the bucket is the passphrase-wrapped master
key inside `meta.json` (§8); there is no separate keyring object.

Required S3 features are exactly hard constraint 1. `constellation fs
create` preflights them and refuses a backend without create-if-absent;
`constellation doctor` probes them and the provider's error codes (some
answer a losing conditional write with 409 before 412; see
[Write-path hygiene](../reference/features/write-path-hygiene.md#conditional-writes-and-their-error-codes)).

**Why two shard levels under `chunks/`**: AWS partitions on the *full key
string* at arbitrary byte positions — delimiters have no special meaning —
at ≥3,500 PUT / 5,500 GET per second per partitioned prefix, splitting
automatically (gradually, with transient 503s) wherever keys offer
lexicographic spread. Hash-named keys are the ideal case: S3 can cut at any
depth, so directory levels add nothing to AWS throughput and a flat
`chunks/<hash>` would partition identically. The two levels exist for
(a) **LIST parallelism**: the §14 orphan sweep scans 256 independent
prefixes (~39 pages each at 10M objects) instead of one sequential
10,000-page walk, and (b) **non-AWS backends**: MinIO and other
directory-backed stores map prefixes to filesystem paths where fan-out
matters. Deeper sharding would help neither and hurt directory-backed
stores. Expected load (3–10 nodes × ~16 in-flight prefetch) sits near a
single partition's baseline; burst cases (mass `pin` of a cold dataset) may
see brief 503s while S3 splits, absorbed by the client's standard
exponential backoff. `packs/` and `blobs/` are flat: they hold thousands of
objects where `chunks/` holds millions.

## 3. Data Plane

- Files are split into **fixed-size chunks — 4 MiB default, configurable
  per filesystem** (`fs create --chunk-size`, power of two 1–64 MiB;
  changeable later, affecting newly created files only). The chunk size is
  **recorded per file in its manifest**, so a file is always uniformly
  chunked (offset math needs one size per file), rewrites adopt the current
  setting, and a per-path inheritable override (compression-style) remains
  possible later with no format change. Trade-off guidance: larger chunks =
  fewer requests, better per-request S3 goodput, smaller manifests
  (media/VM images); smaller chunks = less RMW and re-upload amplification
  for scattered edits. Dedup does not cross chunk sizes (same content,
  different boundaries, different hashes), so changing the setting
  fragments dedup between old and new files until rewrites converge —
  self-healing, but worth knowing. Chunks are addressed by the **blake3
  hash of the uncompressed plaintext** — identity is independent of
  compression and encryption, so dedup and peer verification always work.
  Blake3 is a full cryptographic hash (~2^128 collision resistance;
  accidental collisions negligible at any reachable scale), fast enough
  (multi-GB/s per core, SIMD-parallel) that hash-on-write and
  verify-on-read are never the bottleneck, and native to iroh/bao for
  incrementally verified peer transfers. In E2E mode the addressing hash is
  **keyed** (see §8).
- Chunk objects are self-describing: header
  `magic, format_ver, codec_id (u16), level, uncompressed_len` + payload.
  Unknown `codec_id` on read = clear "upgrade" error. Codec registry:
  `0 = raw`, `1 = zstd` (full level range); adding codecs is a code change
  only, never a format migration.
- **Compression is a ZFS-style inheritable path attribute** carried in the
  metadata log: `constellation compression set <path> zstd:7` affects new
  writes under that path (nearest-ancestor wins); never recompresses existing
  objects. Incompressible guard: if compressed >= original, store raw.
  Compression runs across chunks on a worker pool; zstd internal MT only for
  big single blobs. Order in E2E mode: compress-then-encrypt.
- **Manifest spill**: a file's metadata row stores the chunk list inline when
  <= 8 chunks (~32 MiB; covers >99.9% of files per the reference census);
  larger files store the 32-byte hash of a *manifest blob* (the serialized
  chunk list) kept in the chunk store. Manifest entries are typed;
  reserved types: `inline`, `manifest`, `slice-overlay` (future: append small
  overwrites without chunk RMW), `pack` (future: many tiny chunks in one
  object), `cdc` (future: content-defined chunking). Reservations cost bytes
  now to guarantee zero migrations later.
- Old chunks are immutable; edits create new objects. Unreferenced objects
  are removed by background GC (snapshot-aware, grace period,
  fsck-verifiable; coordination in §14).

### Write path (partial writes)

`write()` materializes only the touched chunk(s) (cold: one 4 MiB GET),
applies the edit in the cache and marks the chunk dirty; repeated writes
coalesce. On `close()`/`fsync()` the node re-hashes, compresses (and
encrypts) the new chunk(s) and uploads them in parallel, then commits the
new manifest as one small metadata operation at the file's sequencer (§4).
Other nodes re-fetch only changed chunks: a 1-byte edit of a 1 GB file
syncs ~4 MiB.

One invariant holds in every mode: **the log never names a chunk S3
lacks.** A reader anywhere can therefore fetch every chunk a shipped
manifest names.

What `close()` and `fsync()` wait for is set per mount; what an
acknowledgement of the manifest means is set per filesystem:

- `--write-mode through` (the default): `close()` returns once the chunks
  are in S3 and the manifest is committed and acknowledged. A small new
  file costs one S3 request (the conditional create of its chunk) and one
  S3 round trip per close, on the sequencer and on any other node alike.
- `--write-mode back`: `close()` returns once the uploads are queued
  durably on local disk, with no S3 round trip in the close. The
  manifest ships only once its chunks are up. `fsync`, `O_SYNC`,
  `O_DSYNC`, `--fsync-mode s3` and a cluster lock's release always act
  as `through`. Meant for bulk imports (`tar x`, `rsync`); `constellation
  write-mode` switches a running mount.
- `--fsync-mode local` (default): `fsync()` forces the node's metadata
  store to disk. `--fsync-mode s3`: `fsync()` also waits until the inode's
  chunks and records are in the bucket.
- The acknowledgement policy (`fs create --ack-policy local|s3`, fixed
  for the filesystem) decides whether an acknowledged mutation lives on
  the sequencer, on a backup, or in S3 (§4, §9).

The exact per-mode contract, and what `back` gives up, are in
[Durability and failover](../reference/features/durability-and-failover.md#--fsync-mode-and---write-mode).

**A non-owner's `back` close.** A node that is not the file's sequencer
forwards the manifest at once, naming the chunks still uploading on it.
The sequencer enrolls them as pending uploads it awaits from that node,
*before* it executes the op, so every existing gate applies: the root's
ship plan defers the transaction and what depends on it, a delegate's
stream and backup feed stop before it, and the pre-S3 stream (§4) stops
before it for every subscriber except the forwarder, which has the bytes.
The forwarder reports the chunks once they are up (`ChunksDurable`); if
the report never comes, the sequencer checks S3 itself (2 s, doubling to
16 s). Readers on the sequencer, the only node that sees the manifest
early, wait for the chunk (`CONSTELLATION_REMOTE_CHUNK_WAIT_S`, 60 s).
The simpler "acknowledge the close locally, forward later" was rejected:
the close would be acknowledged before the sequencer validated it, a
`Conflict` after the close would have no write session left to rebase
and would become a conflict copy, `--cto strict` would no longer see
every completed close, and the node's own reads would need a new kind of
speculation.

**Dedup and the condemned check.** A chunk upload is a create-if-absent,
or a `HEAD` first when a positive hint (the existence cache, a peer's
chunk set) says the chunk is probably there already. A chunk below
`CONSTELLATION_PROBE_MIN_BYTES` (256 KiB) never probes on a guess: a
miss would cost a second serialized round trip, a hit only resends a
small body. Only a dedup hit, an upload that finds its chunk already in S3
and relies on it, needs GC's condemned pointer, and reads it after the
hit (§14).

**One node losing S3.** If a node's own S3 path makes no progress for
`CONSTELLATION_CHUNK_HANDOFF_AFTER_MS` (6 s), its drains hand the chunks
to a peer that reaches S3 (the lease holder first): the peer fetches them
from this node over P2P, verifies their hashes, uploads them, and
answers once they are in S3. The invariant is unchanged; only *who*
uploads changes, and `through` still means "in S3 before `close()`
returns". Later drains hand off at once while there is still no progress
(for 30 s). With no such peer (a single node, P2P off, every peer cut
too), a write-through close or `fsync` still needs S3: it fails with
`EIO` once the node's own upload has used its retry budget (about
3 × 30 s). A flush holds only a per-inode lock while it drains, so
`stat` and `ls` of other files never wait behind it.

### Streaming writes: files larger than the cache

Reads are size-unbounded by construction (prefetch ahead, evict clean
behind: a 10 TB file streams through a 50 GB cache). Writes get the same
property via **eager chunk upload**: because chunks are immutable,
content-addressed, and invisible until the manifest commits at
`close()`, the daemon uploads a chunk as soon as the writer moves past it —
hash, compress, PUT, demote dirty → clean → evictable. The cache then holds
only the in-flight window, and maximum file size is bounded by S3, not
local disk (a 10 TB file's spilled manifest is ~2.5M entries, tens of MB
compressed — one blob, loadable). Failure cases stay clean: an abandoned or
crashed write leaves orphan chunks for grace-period GC; a writer
re-dirtying an already-uploaded chunk produces a new hash and a superseded
garbage object (write amplification on pathological patterns, never
incorrectness). Two inherent exceptions: **pins** require full residency by
definition (admission-checked), and **offline writes** cannot drain
eagerly, so disconnected write capacity ≤ free cache space — the spool
bound of §9.

### Scratch directories

A shared directory marked with `user.constellation.scratch=1` is a
node-private staging namespace. Creates, writes, renames within the scratch
tree, and unlinks update only the local replica and cache: they do not acquire
a lease or append shared log records. Scratch contents are purged on every
mount and are never visible from another node.

Renaming a regular file from scratch into the shared namespace is an explicit
**Publish** operation. It uploads the file's chunks, then atomically creates
the shared inode and manifest through the ordinary mutation path. Existing
content-addressed chunks are reused. Moving shared content into scratch, or
publishing directories and other unsupported file types, fails with `EXDEV`;
hard links cannot cross the boundary. A crash before Publish can leave only
local scratch state and unreferenced content-addressed chunks for normal GC —
never a shared-log record that names an incomplete file.

### Unlink while open (orphan inodes)

POSIX semantics — `unlink` removes the *name* immediately; the inode and
data survive while any process holds an open handle; real deletion happens
at last close — hold **cluster-wide**: "any process" means on any node.
(NFSv3's `.nfsXXXX` silly-rename files exist because the server doesn't
know client opens; Constellation avoids the problem with explicit claims.)

- **Namespace**: the `unlink` record removes the dentry everywhere at once
  — but applying it never purges the inode: at nlink=0 every replica
  retains the inode + manifest in orphan state (deref-tracked), so a
  holding node resolves handle → manifest → chunks entirely from its own
  DB, independent of the unlinking node. A node applying it while holding
  open handles additionally marks the orphan held locally and keeps
  serving those handles normally.
- **GC protection = holds**: a node with open handles on orphaned inodes
  advertises them in a TTL'd `holds/<node-id>.json` record (`cli::holds`):
  the inodes and every chunk hash their manifests name, with an expiry.
  The §14 sweep keeps every chunk (and metadata node) a live hold names,
  both when it marks and again right before it deletes. Crash safety
  falls out: a dead node stops renewing, its hold expires, orphans reap —
  no leaked cruft.
- **Zero cost on the hot paths**: normal `open()` never touches holds — the
  record exists only for the rare intersection *open handle × unlinked
  inode*. It is one object per node (all its held orphans; absent when the
  set is empty, the usual state), written by a background task that the
  sync task nudges after every applied segment and that otherwise runs
  every `CONSTELLATION_HOLD_REFRESH_MS` (half the lease TTL; the expiry
  is three refresh periods out) — neither `open()` nor `unlink()` waits
  on it. The same task reaps every orphan no view has open, which is
  what removes a foreign unlink's record from a node that never had the
  file open.
- **What the asynchrony costs, precisely**: a GC round waits one lease
  TTL between publishing its candidates and deleting, and re-reads holds
  right before deleting, so a node that has applied the unlink by then
  and can reach S3 is covered. The horizon does *not* help here — it is
  measured from the chunk's write time, and a long-open old file is the
  common case (a log rotated away under a reader). A node that applies
  the unlink later than that — it lags the log by more than a lease TTL,
  or is partitioned from S3 — may find the chunks gone when it next reads
  uncached bytes: `EIO` on that handle, the edge noted below. This never
  dangles a committed reference: no committed state names an orphan.
- **Held across a rebuild**: a namespace rebuild on the holding node (a
  deposition recovery, a retention-gap rebuild, §14) carries the orphans
  its views have open across the swap, records and manifests included,
  and re-stamps the hold from the rebuilt replica; a node with several
  views reaps on `unlink` only when no view has the inode open.
- **Last close, cluster-wide**: when the final hold disappears (release or
  TTL expiry), the inode's chunks deref and ride the normal GC horizon.
- **Writes to orphans** are allowed (POSIX): flushed as inode-keyed log
  records, so other nodes that also had the file open see them under normal
  close-to-open rules — and better: `fsync()` (or close, or the background
  flush) is the *publication point*. Remote holders apply the record and
  push-invalidate the inode's pages (§12), so reads through their existing
  handles return the new bytes after flush + propagation (ms on a LAN) — no
  reopen needed, which for an orphan is impossible anyway. Concurrent
  orphan writers serialize at the file's sequencer like any file.
  (Contrast NFS: silly-rename `.nfsXXXX` name resurrection, and remote
  holders notice writes only when the attribute cache times out, seconds
  later.) Scratch shortcut: an orphan with a single local
  holder that was never opened elsewhere never flushes at all — the
  open-unlink-tmpfile pattern runs entirely in the local cache.
- **Fast path preserved**: unlinking a file nobody has open (the vast
  majority) takes the ordinary route — no holds, immediate deref.
- **Edge**: a node offline with an orphan open can only read its cached
  chunks anyway; if its unrenewable hold expired and GC collected the rest,
  uncached reads after reconnect return EIO. Optional mitigation:
  **orphan-prefetch** — on remote unlink of a locally-open file, eagerly
  fetch its remaining chunks so the handle is self-sufficient.

## 4. Metadata Plane

The metadata plane is one ordered log in S3, one lease that decides who
appends to it, and a full replica on every node. Everything else in this
section exists to make that simple shape fast (forwarding, delegation,
streaming) and safe to fail over (request ids, speculation, positions).

### Local store

Every node keeps a **full metadata replica** in [fjall](https://github.com/fjall-rs/fjall)
3, an LSM key-value store (plan 29,
[29-fjall-metadata-engine.md](../plans/v1/done/29-fjall-metadata-engine.md);
it replaced SQLite, the choice ADR-9 records). One writer transaction at a
time keeps "namespace change + journal row" atomic; readers take lock-free
snapshots. The keyspaces that matter here:

- `ns`: the replicated namespace, in plan 28's key encoding (inodes,
  dentries, spilled xattrs, reverse links, and subsystem rows such as the
  delegation table and the quota). Its key set is exactly what a published
  commit holds.
- `journal` and `journal_tx`: the local operations not yet in the log,
  with one row per transaction (where it ends, its op and rid, its epoch).
- `spec`, `spec_live`, `pending_replay`: the speculation log (below).
- `completed`: `rid → outcome` for every request the log has executed or
  refused.
- `backup_tail`: on a backup, the holder's unshipped journal (§9).
- `orphans`, `pending_upload`, `dirty`, `chunk_ref`, `local`: unlinked-but-open
  inodes, the upload queue, and node-local bookkeeping.

Every write to `ns` goes through one funnel that records the before-image
of each key it changes when the change is speculative. Commits reach OS
buffers, not stable storage; state that safety rests on is synced before
it is acted on (§9). Keyspaces with a tiny live set and a delete per
operation (the journal, the upload queue, speculation) are compacted in
the background once they churn, so a daemon does not slow down with
uptime.

The full tree is browsable offline on every node (`ls`/`stat`/`find` always
work); file *content* is readable as far as the chunk cache reaches.

### One log, one root lease

There is one log, `log/p0/<seq:016x>.zst`, and one lease over it,
`leases/p0.json`. (Plan 29 removed the per-subtree partitions and their
two-record cross-partition renames; plan 30 scales writes out with
delegations over the one log instead, below.)

- **Segments** are zstd batches of `LogRecord`s, CAS-created with
  `If-None-Match`: a sequence-number collision is the conflict detector.
  Each segment is stamped with the lease epoch (fencing: a deposed
  holder's late segment is refused) and with the journal position it
  ships through. A transaction is never split across segments, so an op
  and its `Completed { rid }` always land together. The holder ships
  whatever is journaled every sync round (500 ms, or at once when a
  `close()` or `fsync()` nudges it), in segments of at most 4 MiB.
- **Records** are the namespace operations (`mkdir`, `create`, `unlink`,
  `rename`, `link`, `symlink`, `setattr`, `write_manifest`, xattrs,
  `snap_create`, `snap_delete`, `clone`, quota, atime) plus plan 30's
  control records: `Completed { rid }` and `Refused { rid, errno }`
  (exactly-once, below), `InboxAck` (the S3 inbox), `Delegate` and
  `Recall` (the delegation table), and `TailFollows` (a sealed backup's
  takeover, §9). The set is an append-only registry, like the manifest
  entry types.
- **The commit chain** (plan 28,
  [28-s3-native-metadata-store.md](../plans/v1/done/28-s3-native-metadata-store.md))
  replaced checkpoints. Only the lease holder publishes commits: a
  content-addressed tree of the namespace (`packs/`, `blobs/`) whose root
  is CAS-created at `commits/<seq>` and records the log position it
  covers (`applied`). A new node restores the head commit and replays the
  log from there. Commits are log prefixes: the holder substitutes the
  before-image of every key its unshipped journal touched
  ([Write-path hygiene](../reference/features/write-path-hygiene.md#who-publishes-commits)).
- **The root lease** grants exclusive *append* authority over the log.
  The object records the holder, the epoch, the expiry, `released`, the
  nodes waiting for it (`wanted_by`), and since plan 30 the backups, a
  `config_version`, the `ack_policy`, whether the tenure granted read
  delegations, and the nodes retired while it named them. The TTL is 60 s,
  renewed at about half-TTL, and a lease is *usable* only with more than
  its expiry margin, `min(1 s, TTL/4)`, left.
- **Leases are sticky.** A holder releases only when a node has asked
  for the lease (`wanted_by`), its journal is empty, it has held the lease
  for 5 s, and it has been idle for 30 s or the request is 5 s old. It
  never releases while a delegation or a lock grant is live (both are
  capped by the lease), or while the only nodes asking are across a P2P
  partition from a side that is using it (they are served through the
  inbox instead). The holder may also move the lease to the
  write-rate-weighted medoid of the writers
  ([ADR-15](DECISIONS.md#adr-15-holder-driven-placement-no-election),
  [Lease placement](../reference/features/lease-placement.md)); since
  plan 30, placing subtrees on their writers (below) matters more.
- **Acquire and takeover.** A node acquires the lease by CAS (create, or
  swap an expired or released object), tails the log to head, ships an
  epoch marker at the next slot (which fences the old epoch in the log),
  and keeps its view closed behind a *takeover gate* until the
  predecessor's stranded work is settled (below). There is no "offline
  branch" to reintegrate any more: a predecessor's unshipped tail is
  replayed by its requesters (Layer A), re-shipped by a sealed backup
  (Layer B), or does not exist (`ack=s3`); see §9.

### Request ids and exactly-once

Every mutation carries a request id `Rid { node, incarnation, seq }`,
assigned once per FUSE operation and kept across every retry, redirect,
replay and path (P2P, inbox, lease). The incarnation is bumped at every
mount. Executing an op appends `Completed { rid }` in the same
transaction; a definitive refusal appends `Refused { rid, errno }`. Every
replica folds these into `completed`, and a sequencer answers a rid it
already executed from there (or from an in-memory map of recent
outcomes), never executing it twice.

A forward whose reply times out is *in doubt*. The requester retries the
same rid: the same holder three times with backoff, up to two redirects,
then the lease path, which tails to head and checks `completed`. Log GC
keeps every segment younger than `CONSTELLATION_COMPLETION_RETENTION_S`
(900 s), so the check always covers the op's whole window; an op still in
doubt at its deadline (twice the lease TTL) fails with `EIO`, never with
a re-execution. The same identity makes replays, backup re-shipping,
inbox drains and delegate streams exactly-once without further machinery
([ADR-18](DECISIONS.md#adr-18-exactly-once-forwarding-with-request-ids),
[Forwarded mutations](../reference/features/forwarded-mutations.md#exactly-once-identity-and-in-doubt-handling)).

### The replica is a log prefix plus explicit speculation

A node often applies effects before the log has them: a requester's
*shadow* of its acknowledged forward, an `Exists` hint from a refusal, the
holder's own unshipped transactions, records streamed ahead of S3, a
delegate's unappended transactions. Each is *speculation*: written with
the before-images of every key it touches, in the same transaction, and
retired when the log confirms it.

A segment from a later epoch *strands* older speculation, and so does a
deposition. Recovery rolls back to the earliest stranded entry, redoes
what still stands, and replays the stranded ops **by rid** through the
current sequencer. A replay the namespace no longer admits is a genuine
conflict and becomes a `.constellation-conflict/` copy; that is the only
source of conflict copies. A tailed segment that overlaps outstanding
speculation is applied *under* it (rolled back, applied in its log place,
redone), so a replica never holds a log record on top of state from later
in the log. A new holder's takeover gate strands, applies any backup
tail, runs its replay queue and drains the inbox before its view opens,
so it never validates against phantom state
([ADR-19](DECISIONS.md#adr-19-the-replica-is-a-log-prefix-plus-explicit-speculation),
[Forwarded mutations](../reference/features/forwarded-mutations.md#speculation-and-stranded-op-recovery)).

### Positions and log streams

Every reply carries the **position** it was evaluated at: the log
sequence, plus the unshipped journal position `(epoch, jseq)` if it had
one, plus per-delegation stream positions. A replica can tell from a
segment's `through` and its delegation origins whether it has everything
a position names. Positions are the currency of §6's session guarantees,
strict reads and lock handoffs.

Followers receive shipped segments over **direct log streams** from the
holder (`LogSubscribe`), applied through the same code as S3 tailing,
fencing included; a gap, timeout or slow subscriber falls back to S3
tailing. Gossip carries only hints, membership and digests (this
supersedes ADR-17's payload push). Under a backup (§9), the holder also
streams backup-acknowledged transactions **ahead of S3**, contiguously
from the applied log, and subscribers apply them as speculation: a
forward waiting for its own transaction is answered from that stream
instead of from the next S3 round trip. A continuation epoch's holder
(§5.3) streams its journal ahead the same way, and its members keep
following it
([Close-to-open modes](../reference/features/cto-modes.md#direct-log-streams),
[Durability and failover](../reference/features/durability-and-failover.md#pre-s3-streaming)).

### Acknowledgement and durability

What a sequencer's acknowledgement means is the tenure's `ack_policy`,
chosen per filesystem and topology
([ADR-21](DECISIONS.md#adr-21-layered-durability-and-seal-based-failover)):

- **`Local`** (Layer A, always, free): the op is on the sequencer's disk;
  its requester keeps it as speculation and replays it by rid if the
  sequencer dies.
- **`Backup`** (Layer B, automatic when a peer is within
  `CONSTELLATION_BACKUP_RTT_BUDGET_MS`, 5 ms): the holder streams whole
  journal transactions to its backup and acknowledges only what every
  listed backup holds. Failover takes about 1.5 s in the harness and
  2.3–4.7 s from `kill -9` to a new holder on EC2 ([RESULTS](../../bench/remote/RESULTS.md#failover-time)), and loses
  nothing acknowledged.
- **`S3`** (Layer C, `fs create --ack-policy s3`): acknowledged once its
  segment is in the log, group-committed per sync round.

A cluster whose peers are all far away picks no backup and stays at local
speed (hard constraint 4). §9 has the failover rules.

### Delegated sub-sequencers

One lease and one sequencer for the whole namespace would make every
other writer pay a round trip to it, across an ocean if need be. So the
root delegates subtrees to their dominant writers over P2P, keeping one
lease and one log
([ADR-23](DECISIONS.md#adr-23-delegated-sub-sequencers-over-one-log),
[Delegations](../reference/features/delegations.md)):

- `Delegate { dir, node, gen }` and `Recall { dir, gen }` records keep a
  replicated delegation table. Delegations never overlap and never nest.
  A grant lasts `CONSTELLATION_DELEGATION_TTL_MS` (5 s), is renewed by the
  delegate, and is capped by the root lease.
- **Ownership** is a local ancestor walk: a dentry belongs to the
  delegation containing its parent; an inode's own keys to the one
  containing its primary link's parent. An op whose keys have two owners
  is cross-subtree.
- The delegate validates against its replica (authoritative for the
  subtree, since every write there goes through it), journals as
  speculation, acknowledges under its own durability layer (a backup
  chosen by RTT to *it*), and streams its transactions in order to the
  root. The root appends them without re-validating, in order, and only
  once each transaction's `deps` (the delegate stream positions the
  requester had observed) are in its replica: no replica ever holds a
  record whose causes are missing.
- **Cross-subtree ops** (renames and hard links across delegations,
  `rmdir` or rename of a delegated root) go to the root, which *recalls*
  the delegations involved first (the delegate drains and stops, or is
  outwaited), then executes alone. There is no two-phase commit.
- **Placement** is automatic: the root sees every op's origin and
  delegates the *topmost* directory one node dominates (at least 70% of
  its ops in a 30 s window, above a rate floor), recalling when the share
  stays below 50% for a minute, with a cool-down before it moves again.
- **Hot shared directories**: parent `mtime`/`ctime` are hybrid logical
  clock stamps merged by `max` and `nlink` changes are deltas, so creates
  in one directory commute and hold their parent only *shared*. A
  directory can be split into up to 16 name-hash ranges (GIGA+), each
  delegated separately, but only to a node that dominates that range:
  names hashed uniformly across writers give no range a dominant writer,
  and splitting such a directory measured slower than leaving it with the
  root.

A node writing its own subtree therefore runs at local speed, and S3
request counts do not change, since there is still one log. Offline
designations (§5.2) reuse the same machinery without TTL expiry.
Leaseless optimistic commits, which would remove the sequencer
altogether, stay deferred
([ADR-28](DECISIONS.md#adr-28-leaseless-optimistic-commits-stay-deferred)).

### The S3 inbox

A requester with no P2P path to the holder (P2P off, the holder not in its
peer directory, or an outage to it longer than
`CONSTELLATION_INBOX_P2P_GRACE_MS`, 3 s) forwards through the bucket: it
CAS-creates batches `inbox/<epoch>/<node>/<n>`, the holder polls each
such requester with an adaptive backoff (and never one it is P2P-connected
to), and outcomes ride the log (`Completed`, `Refused`, `InboxAck`). An op
waiting in the inbox is re-sent over P2P by the same rid as soon as the
holder is reachable again. Sustained demand (8 inbox ops, or 1.5 s of
waiting, within 10 s) escalates to a lease request: a create storm still
moves the lease, a sporadic write does not. Locks and strict ReadIndex
never go through the inbox
([ADR-24](DECISIONS.md#adr-24-the-hybrid-s3-inbox-for-writes-without-a-p2p-path),
[Forwarded mutations](../reference/features/forwarded-mutations.md#the-inbox-forwarding-without-p2p)).

### Locks

Cluster `flock`/`fcntl` locks are leased grants kept by the owning
sequencer (the root or the delegate of the file), in the same core that
sequences the file's mutations. §6 explains them.

## 5. Write Authority: the One Rule

> A node may alter a subtree only while it holds an unexpired **authority
> chain** rooted in something that cannot be concurrently claimed.

Three roots exist:

- **the root lease** (§4), claimed by CAS on S3;
- **offline designations** (§5.2), non-stealable claims on a path;
- **continuation epochs** (§5.3), persisted agreements among the nodes
  of a P2P component during a bucket outage.

Every other grant is **derived** from the root lease and capped by it:
delegations (§4), read delegations (§6) and lock grants (§6). All derived
grants share one time discipline. The holder of a grant honours it until
`sent + ttl − margin` on its own clock, measured from when it sent the
request; the grantor treats it as live until `granted + ttl + margin` on
its clock; `margin` is the lease's expiry margin, `min(1 s, TTL/4)`. The
rule is safe while clocks drift apart by less than half the margin
(`margin > 2 × drift`), the same assumption the lease itself makes. A
grantor never gives more than it has left, so a grant never outlives the
lease it came from.

All transitions between roots are explicit and persisted before they take
effect. Heartbeats and silence now drive liveness decisions — a backup
seals a silent holder, an `ack=s3` peer takes one over, a node publishes a
promise when asked — but none of them fences anything. The fences are the
S3 CAS, the backup's seal, the log-slot CAS, the lease margin and the
promise (hard constraint 5).

### 5.1 Requesters, sequencers and the one appender

A node that does not own the keys of a mutation is a *requester*: it
forwards the op with its rid to the *owning sequencer*, which is the root
lease holder or the delegate of the subtree (or hash range). Sequencers
validate and order; only the root **appends** to the log. A requester
with a stale table is redirected (`NotHolder`, at most two hops).

If the owner does not answer, the requester retries the same rid (§4),
then goes through the S3 inbox if it has no P2P path to the holder, or
the lease path if it does. The lease path is the one mechanism that works
with nothing but S3: the requester registers in `wanted_by`, and the
holder releases (§4) or its lease expires and the requester takes it by
CAS. `CONSTELLATION_FORWARD=off` restores writer-follows-lease for every
write.

### 5.2 Offline designation

`constellation offline <path>` / `online <path>` — CAS-enforced, exactly one
designee per path, overlap-checked (`designations/`).

A designation is a **non-stealable delegation** to the designee
(`Delegate { designated: true }`): the same machinery as §4's delegations,
without TTL expiry, never recalled by placement or by a cross-subtree op.

- **Designee reachable → everyone still writes.** Other nodes' ops under
  the path are forwarded to the designee and sequenced there, and it
  streams them to the root like any delegate. At any instant the designee
  holds every change under the path, so its offline writes are always
  linear continuations.
- **Designee unreachable → others cannot write under the path.** An op
  under a designation that reaches the root is refused with `EROFS`; a
  cross-subtree op involving one with `EXDEV`. The designee keeps writing
  indefinitely, with or without S3 (its claim cannot be stolen), and its
  stream reaches the root when it reconnects.
- `offline --ro` grants a read guarantee (a pin) without write authority
  and creates no delegation.

### 5.3 Continuation epochs

When S3 is unreachable, nodes that still reach each other over P2P may
form a **continuation epoch**: leases move among the members over P2P,
writes journal locally, and everything flushes when S3 returns
([ADR-22](DECISIONS.md#adr-22-flexible-quorum-continuation-epochs-with-promises),
[Durability and failover](../reference/features/durability-and-failover.md#flexible-continuation-epochs)).

The danger is a missing node that still reaches S3 and takes an expired
lease the epoch is using. With `epoch_slack = 0` (the default) the epoch
therefore needs **every** write-eligible node: every live, non-read-only
registry record. Majority quorum is deliberately insufficient. With
`epoch_slack = f` (`fs create --epoch-slack`, `fs set epoch-slack`) it
needs `N − f`, and the missing nodes are held off by **promises**:

- A promise is a node's word that it joins no epoch before a given time
  (`heartbeat/<node>`, persisted and synced locally before the PUT). It
  is published on demand only: when a would-be taker asks over P2P, when
  the node sees a lease expire unrenewed and nobody can ask it, or when
  its slack changes. A steady cluster writes none.
- A node joins an epoch only once its own last promise has expired, and
  promises nothing while its epoch is open.
- An S3 takeover of an expired lease another node held needs `f` *other*
  nodes whose promises outlast the lease's recorded expiry. A taker's `f`
  promisers and an epoch's `N − f` members must share a node, whose
  promise would have to be both expired and binding.
- The promise TTL is at most a quarter of the lease TTL, and a taker
  honours the largest slack any node advertises, so changing `f` is safe
  while it propagates.

What an epoch may carry:

- It carries a node's lease only if nobody outside the epoch can take
  that lease over early: a `Local` lease, or a `Backup` lease whose
  backups are all members. An `S3` lease is never carried, since any peer
  may take it over before its expiry (§9).
- It carries a lease only if the holder's claim was usable (outside the
  margin) when it joined. An epoch that ends up carrying no lease has no
  one to sequence writes, and refuses them at once with `EROFS`.
- No delegations exist inside an epoch (the root recalls them when it
  opens), and writes are acknowledged on the epoch holder's disk alone.
- The hold owner's epoch journal is speculation like any holder's
  unshipped journal (§4, ADR-19): captured with before-images, under
  the epoch its flush will ship under (the carried lease's for the
  carrier; the next one for a member that took the hold over, which is
  what its flush CAS grants). So the flush's ship plan skips exactly a
  transaction that waits for a chunk only a member has, and what depends
  on it, and ships the rest; the publisher substitutes them; and a
  deposed hold owner rolls back and replays by rid like any deposed
  holder. A write whose chunk's only copy is on a member gone for good
  is dropped by the operator (`repair drop-held --remote`, §9): a
  refused replay and a conflict copy, its refusal in the log, never
  silent loss.
- Members keep following the epoch holder's log stream, the only way the
  log can reach them while S3 is away, and the holder streams its
  journal ahead as it grows (§4, "Positions and log streams"). A member's
  forwarded write therefore completes in one round trip; its reply is
  never answered before the stream has delivered what it depends on.
  Members serve each other the chunks their epoch writes name.
- The hold moves to another member only if the holder's journal is empty
  and the requester has applied the holder's whole log. A node that
  handed its hold away, or closed its epoch, never takes that hold back,
  even after a restart. A member closes its epoch only once the carried
  lease is gone from S3 (the holder re-claimed it by its flush, released
  it, or lost it).
- A member that can still reach S3 declines to join. A node whose own S3
  fails first asks every member to probe S3; if any does reach it, the
  outage is its own and it proposes nothing (its closes hand their chunks
  to a peer instead, §3).

If the component loses a member mid-epoch, the epoch **freezes**:
members refuse writes (`EROFS`) until it recovers or S3 returns. Shrinking
the write-eligible roster beyond `f` is an **operator action**
(`constellation leave`, §8), never a timeout: an unmounted or unreachable
node still counts until it is retired. An admin leave also fences the
retired node's leases, and epoch members abandon an epoch that carried a
retired node's lease. A node that enrolls during an epoch is a known gap.

### Availability matrix

Per subtree, with P2P on and the defaults unless stated. "Write" means the
op executes and is acknowledged; "EROFS" means it is refused at once.

| Situation | Sequencer (holder, delegate or designee) | Other nodes |
|---|---|---|
| All connected | write at local speed | write: one round trip to the sequencer, local speed in a subtree delegated to them |
| One node cut from P2P, S3 up | write | the cut node writes through the S3 inbox (a few S3 round trips per op); no cluster locks or strict ReadIndex for it |
| Holder cut from P2P, S3 up | writes, serves the others through their inbox until their demand moves the lease to their side | write through the inbox, then locally once the lease moves |
| One node loses S3, P2P up | write; its closes hand their chunks to a peer after 6 s | write |
| Holder dies, a backup within the RTT budget | — | write again in ≈ 1.5 s in the harness, 2.3–4.7 s on EC2 (seal-based takeover), nothing acknowledged lost |
| Holder dies, `ack=s3` | — | write again in ≈ 1.5 s in the harness, not measured on EC2 (any peer takes over), nothing acknowledged lost |
| Holder dies, no backup (`Local`) | — | write again after the lease TTL (60 s) plus margin; acknowledged forwards are replayed by their requesters; with `f > 0` the taker needs `f` promises |
| Delegate dies | — | writes under its subtree wait for the root to reclaim it (TTL + margin ≈ 6 s), or for its backup's seal |
| Delegate cut from the root | stops sequencing at `sent + ttl − margin`; its unstreamed ops are replayed through the root | write through the root once it reclaims the subtree |
| Designee isolated | write | `EROFS` under the path |
| S3 down, at least `N − f` write-eligible nodes connected | write (epoch) | write (epoch) |
| S3 down, fewer than `N − f` connected | writes until its lease's TTL, then refuses | cannot acquire; reads work |
| S3 down, `ack=s3` | acknowledgements stall until S3 returns | the same |

## 6. Consistency Modes

Three things together define what a reader sees:

- **Session guarantees, always**: a node never answers a read from a
  state older than one its clients have already seen.
- **Close-to-open, per mount**: `--cto bounded` (the default, chosen
  from the EC2 measurements in
  [ADR-30](DECISIONS.md#adr-30---cto-bounded-stays-the-default)) or
  `--cto strict`.
- **Cluster locks, per daemon**: `--locks cluster` (the default whenever
  P2P is on) or `local`.

Concurrent writers on different nodes serialize at the owning sequencer by
forwarding, not by moving a lease
([ADR-4](DECISIONS.md#adr-4-consistency-default--close-to-open-strict-and-relaxed-opt-ins),
[ADR-20](DECISIONS.md#adr-20-positions-session-guarantees-and-two-close-to-open-modes)).
The "relaxed" mode of earlier designs (write locally, detect conflicts on
reintegration) has not been built.

### Session guarantees

A node keeps an `observed` watermark: the positions (§4) of replies whose
effects it has not installed locally, such as a refusal, an `Exists`
without a hint, or an op that waited for the log. An accepted forward
installed as a shadow does not raise it: the shadow already covers its
keys. Every FUSE read path (`lookup`, `getattr`, `readlink`, `open`, the
first `readdir` chunk, `getxattr`, `listxattr`) waits until the replica
reaches `observed`, unless speculation already covers the keys it reads.
The wait is bounded by `CONSTELLATION_SESSION_WAIT_MS` (2 s); on timeout
the read answers from the replica and is counted as degraded. The result
is read-your-writes and monotonic reads for every client of a node, at no
cost when the node is idle
([Close-to-open modes](../reference/features/cto-modes.md#session-guarantees)).

### Close-to-open: bounded and strict

- **`bounded`** (default): an open reads the local replica, which follows
  the log within the visibility bound: the log stream's delivery on a
  healthy cluster, S3 tailing otherwise. Session guarantees still hold per
  node.
- **`strict`**: an `open`, `lookup` or first `readdir` chunk on a node that
  is not the sequencer sees every `close()` another node completed before
  it began. It asks the owning sequencer for a position (**ReadIndex**)
  and waits for its replica to reach it; the answer is a position, not a
  record, so a later segment of an older write can never regress it. The
  answer may carry a **read delegation** (5 s, capped by the lease), under
  which later opens of that inode are local. Before acknowledging any
  mutation that touches a delegated inode (including an unlinked or
  replaced one), the sequencer recalls the delegation or outwaits it; a
  forward held longer than half its timeout is answered `Held` and
  retried by rid. The sequencer persists a grant horizon before it
  answers, so it grants nothing new after a restart until earlier grants
  have expired, and a fast successor waits the horizon out before it
  acknowledges anything. Kernel caches have a zero TTL under strict,
  except on a sequencer that has not yet seen another node.

Strict costs nothing on a single node, about one round trip for a first
open on a LAN, and one WAN round trip for a write-then-open across
continents, the minimum strict close-to-open allows at that distance.
With P2P off there is no ReadIndex: a strict open tails S3 to head, so the
sequencer's own unshipped writes become visible at its next ship. A
`--write-mode back` writer's content becomes readable elsewhere only once
its chunks are up
([Close-to-open modes](../reference/features/cto-modes.md#strict-reads-readindex)).

`bounded` is the default
([ADR-30](DECISIONS.md#adr-30---cto-bounded-stays-the-default)). On EC2,
`strict` cost about 9× `bounded` per warm `stat` (about 0.3 ms more per
call, on AWS S3 and OVH alike), and `bounded` passed every correctness
check, including lock-coordinated git and SQLite, whose coherence comes
from the lock grant (see Cluster locks below). `strict` is for a reader that learns
of another node's `close()` out of band, with no lock between them
([Choosing a mode](../reference/features/cto-modes.md#choosing-a-mode)).

### Cluster locks

`flock` and `fcntl` locks exclude each other across nodes
([ADR-25](DECISIONS.md#adr-25-cluster-locks-are-leased-grants-from-the-owning-sequencer),
[Cluster locks](../reference/features/cluster-locks.md)):

- **Grants.** The owning sequencer gives nodes whole-file shared or
  exclusive *grants*; byte ranges and lock owners are resolved on the node
  under its grant. A grant is cached after the last unlock, so an
  uncontended re-lock costs no message.
- **Leased, with one time discipline.** A grant lasts
  `CONSTELLATION_LOCK_TTL_MS` (20 s), capped by the sequencer's own
  authority, and follows §5's discipline. It is renewed half-way through
  the window its holder honours it for, so a short grant is renewed
  inside its window too. A delegate grants nothing on less than
  `2 × margin` of delegation left, and keeps its delegation topped up
  while it has grants out.
- **Conflicts** recall the other grants; a recalled node flushes the
  file's dirty data to the log before it releases. Blocking requests park
  first-in first-out at the sequencer; non-blocking ones get `EAGAIN` and
  the recall still goes out.
- **Coherence from one holder to the next.** A grant carries a position
  the new holder waits for, and it drops its kernel cache of the file.
  The position covers every file, not only the locked one: a release
  carries the releaser's session frontier (every reply its clients got,
  and a root holder's unshipped journal), the owner joins it into every
  later grant of the file, and the new holder makes it its session
  watermark. These *floors* live in the owner's memory and move with the
  lock table when the owner changes: to a delegate and back, to a fast
  successor through the backup mirror, and a new tenure grants nothing
  until every inherited delegation has renewed with it. That is what
  keeps lock-protected read-modify-write of a *set* of files correct
  across nodes (git's refs under an `flock` turn file).
- **Fencing.** A node whose grant lapsed (for example, partitioned past
  the lock TTL) fails I/O with `EIO` on the files it holds locks on,
  until they are unlocked or a new grant arrives (NFSv4's rule), and its
  writes under the lapsed grant are never published. The lock's *owner*
  is fenced on every file: the process of the thread that took the lock
  (by thread group, named by pid and start time), all its threads and
  the processes it started get `EIO` from every write and namespace
  operation until the owner's locks are gone, because an application that guards other files with the
  lock, as git does, must not write on once another node may hold it.
  The limit: an operation checked before the lapse and applied after it
  (a stalled forward) is not caught; a fencing token checked by the
  sequencer would close that (PROGRESS, plan 30 M11 follow-up).
- **Failover.** After a TTL takeover every old grant has already lapsed,
  so there is nothing to reclaim. After a fast takeover the successor
  waits out a grace period and accepts reclaims. A delegation's first
  renewal carries what is left of any root grace over its subtree.
- **Limits.** A blocked lock wait cannot be interrupted, there is no
  deadlock detection, and one process's `flock` and `fcntl` locks on the
  same file conflict. Locks never use the S3 inbox: with P2P off the mode
  is `local`.

### Staleness, precisely

Two invariants define what "stale" can and cannot mean here:

1. **Cache staleness ≡ replica staleness.** Every kernel cache (entries,
   attributes, pages — §12) is invalidated in the same step
   that applies another node's change to the replica
   (`FUSE_NOTIFY_INVAL_*`): a tailed or streamed segment, a forwarded op
   the holder or a delegate executed in place, a delegate transaction the
   root appended, a transaction streamed ahead of S3. So caching adds no
   staleness beyond propagation lag. (Contrast NFS-style TTL caches, which
   stay stale even after the server knows better.) And a node's view is
   always a prefix of the authoritative history plus explicit speculation
   ahead of it: its own acknowledged ops, and records streamed
   contiguously from the log. It is never a mix of unrelated states.
2. **Reads may be stale; writes never act on stale state.** Every write
   is validated by the owning sequencer, whose replica is authoritative
   for its keys. Acquiring the lease requires applying the predecessor's
   log and passing the takeover gate (stranded speculation rolled back and
   replayed, backup tail applied, inbox drained). A write based on an
   outdated requester view therefore serializes against the sequencer's
   state and fails cleanly (e.g. `ENOENT`), rather than conflicting.

Worked example: node A deletes a file at t=0; node B `stat()`s it 1 ns
later and still sees it. Correct: no signal from A has reached B, so there
is no happens-before edge — serializing B's read before A's delete is a
legal ordering (nothing short of paying a round trip per stat could do
better; that is strict mode). The case that must work — A deletes, *then
tells B out of band*, then B looks — does: A's "done" is meaningful after
`close()`/flush, and B receives the record over the direct log stream
(milliseconds on a LAN); under `--cto strict`, B's ReadIndex makes it hold
by construction. If B instead tries to *write* over the deleted path,
invariant 2 forces its op after the delete regardless of what it had seen.

## 7. Caching and Data Movement

- **LRU chunk cache**: `cache.path` + `cache.max_size`; chunk states
  `clean` (evictable, LRU by DB-tracked atime), `pinned` (never evicted),
  `dirty` (never evicted until uploaded). Pressure order: evict clean →
  throttle writes → ENOSPC. Cached chunks are stored decompressed by default
  (config knob) for pread-fast reads.
- **`constellation pin <path>` / `unpin`**: fully cache a subtree,
  non-evictable, **eagerly push-synced**: pinned nodes fetch new chunks
  as soon as the records arrive, from the best source (LAN peer
  preferred) in parallel with the S3 upload. Admission check up front
  (pinned set must fit the budget). `unpin` demotes to evictable.
- **Prefetcher** (after mountpoint-s3): per-handle sequential detection,
  adaptive readahead 1 → ~16 chunks in flight, reset on seek; random reads
  fetch only the needed chunk (ranged within it if partial). Pins reuse the
  prefetcher at full parallelism.
- **Cooperative cache**: a local miss is fetched from a peer that has the
  chunk, found *locally*, with zero per-request messages. Every node
  keeps an **exact mirror** of each peer's published chunk set (its clean
  or pinned chunks, keyed by 8-byte hash prefixes): pushed deltas every
  250 ms keep mirrors current, a small summary heartbeat detects drift,
  and range-based set reconciliation (Negentropy-style additive
  fingerprints over hash-prefix ranges) repairs any gap. So a peer fetch
  never goes to a node that does not have the chunk, and a removal
  propagates within one tick
  ([ADR-26](DECISIONS.md#adr-26-exact-chunk-location-reconciliation-replaces-bloom-digests),
  [Cooperative cache membership](../reference/features/cooperative-cache.md)).
  The older bloom-filter digests (~1% false positives, add-only deltas)
  remain available as `CONSTELLATION_COOP_DIGEST=bloom`. Chunks are
  self-verifying (hash), so peer serving needs no trust or invalidation,
  and a wrong mirror only costs a declined fetch; S3 remains the source of
  truth. A chunk no mirror lists yet, named by a manifest just applied
  from another node, is asked of the node that wrote it first (the
  writer's delta usually arrives after the manifest), so reading a file
  another node has just closed costs no S3 GET
  ([Freshly written chunks](../reference/features/cooperative-cache.md#freshly-written-chunks)).
  A cache may be a thin slice of the dataset or the whole of it: a node
  is free to dedicate one or more full local drives, so 1–4 TiB is an
  ordinary size. Each node sizes and evicts independently; nothing
  assumes peers have equal cache budgets. A mirror costs its receiver
  8–12 bytes per peer chunk; a peer with more than 8M chunks is not
  mirrored.
- **Latency-adaptive source selection**: per-source EWMA of TTFB and goodput
  (S3 and each peer, learned from real transfers), peer RTT (free from QUIC)
  and path type, error rate, queue depth. Pick min predicted
  `ETA = TTFB + size/goodput + penalties`, with ~20% hysteresis and hedged
  requests (fire next-best after the P95 first-byte estimate). Emergent:
  S3-region nodes go direct, remote regions serve each other, cross-ocean
  caches are ignored. All stats in the web UI + `/metrics`.

## 8. Security

- **Transport**: iroh QUIC, TLS 1.3, mutual auth by node keypair
  (NodeId = pubkey). Relays forward ciphertext only.
- **Authorization**: accept-time allowlist against `nodes/` (self-
  enrollment requires bucket write, so IAM decides); read-only members are
  countersigned by a writer node. Unknown NodeIds are dropped post-handshake.
  Gossip topic ID derives from a secret in `meta.json` (bucket-readers only).
  Control messages (lease handoff, delegation grant/ack, epoch promises) are
  signed by node keys.
- **Join / leave**: first mount CAS-creates `nodes/<id>.json`. Permanent
  leave **tombstones** the record (`retired: true`, `retired_unix`) rather
  than DELETE, so numeric ids (log-segment origin, ino prefixes) are never
  reused by a later claim. Unmount alone never leaves — the live record
  stays and still counts toward a continuation epoch's quorum until
  retired (§5.3).

  - **Self-leave** (`constellation leave <fs>`): refuse if a
    continuation epoch is open locally, or if this node was deposed and
    its recovery has not run yet (`reintegrate` first). Refuse live offline
    designations unless `--force` (courtesy; force never skips the epoch
    or deposition checks). Then flush the journal, release the lease,
    write the tombstone, persist `left=1` in the local state dir, stop
    writing, and unmount. Remount of that state dir fails until the
    operator uses a **fresh** state dir (new id).
  - **Admin leave** (`constellation leave <fs> --node-id N`): a
    still-mounted peer with bucket write tombstones another member. It
    does not flush that node's journal. Refuse if `N` is the calling
    node, or if `N` currently holds a live lease or unreleased
    designation, unless `--force`. It also **fences** `N`'s leases: the
    lease object records `N` as retired, so `N` can never claim it again
    whatever it believes it holds, and epoch members abandon an epoch that
    carried `N`'s lease. A still-running target that sees its own record
    vanished or retired must stop writing (treat as deposition).
  - **Rejoin** is new enrollment: mount with a new state dir (or delete
    the old one) and claim a fresh id.

  Peers refresh `nodes/` periodically (~5 s, one LIST; records are
  re-read only when they changed); after a leave, survivors observe the
  smaller write-eligible roster without remounting.
- **Discovery unpublished by default**: peers learn addresses from the
  registry, so a leaked NodeId is not dialable by outsiders. Optional
  iroh relays (`CONSTELLATION_P2P_RELAY`, default off) add NAT/internet
  reachability without a global discovery service; the allowlist still
  gates who may connect (see [P2P relays](../reference/features/p2p-relays.md)).
- **Node key**: Ed25519 at `~/.config/constellation/node.key` (0600),
  generated by `host init`; identity + signing only, never an access
  credential.
- **Encryption at rest**: default = S3 SSE + TLS (provider trusted;
  credentials alone suffice to mount). Optional **E2E passphrase mode** (per
  filesystem, at `fs create`): a random master key, wrapped in `meta.json`
  by an argon2id passphrase-derived key; every data key (per-log
  XChaCha20-Poly1305 DEKs, the addressing key, the gossip topic seed) is
  derived from it, so changing the passphrase rewraps one envelope and
  rotates nothing. Mounting needs credentials + passphrase; unwrapped keys
  live in mlock'd RAM only. Registry, lease, designation and heartbeat
  objects stay plaintext: they carry coordination data, not names or
  contents.
- **Keyed addressing in E2E mode**: plain plaintext hashes as object keys
  would let the provider hash a known file and test for its chunks in the
  bucket (confirmation-of-file, the convergent-encryption leak). E2E
  filesystems therefore address chunks with **keyed blake3**
  (`blake3::keyed_hash`) under a per-filesystem secret derived from the
  master key. Dedup still works within the filesystem (same key
  everywhere), the provider learns nothing from object names, and the
  choice is fixed at `fs create` — a per-FS setting, never a migration.
- **Access = IAM.** Whole filesystem (bucket prefix): grant credentials
  with read or read-write as needed; read-only creds make a read-only
  follower member. Subtrees cannot be IAM-scoped independently — content
  addressing makes per-path policies impossible without a separate
  materialization layer, which is out of scope.

## 9. Failure Handling

**Reserve-before-accept**: every write path (FUSE write, pin download, log
append) reserves cache/DB space before touching disk; failure = clean ENOSPC,
no partial state. Chunk files are written temp-name + atomic rename; startup
GC and `fsck` remove identifiable cruft. DB-full flips the mount read-only,
keeping an emergency reserve so pending segments can still flush to S3.

An optional **logical size cap** (`fs create --max-size` /
`constellation quota set`) adds one more ENOSPC gate on the write path:
each node checks its maintained whole-FS usage counter plus the current
inode's uncommitted growth against the replicated quota. Enforcement is
best-effort — there is no synchronous cross-node reservation — so concurrent
writers can overshoot slightly until journals catch up. The counter itself
is safe at global scope (a rename never changes the total, only which name
holds the bytes); that is why a maintained aggregate was deferred for
per-directory `rsize`/`rcount` but is acceptable here. `statfs` still
scopes *used* space to the mounted view — a subtree or snapshot mount
walks its own root — while free space reports whole-filesystem headroom
under the cap, which is what a writer can actually consume.

### What "persisted" means

A node's store commits to OS buffers (fjall `PersistMode::Buffer`): a
commit survives a crash of the process, and the kernel writes it back
within seconds, but a power loss or kernel crash can drop the last
commits unless something synced them (an `fsync()` on the mount, an
orderly shutdown). State that *safety* rests on is synced before it is
acted on: a promise and the epoch join gate, an epoch's persisted state,
a seal, the read-grant horizon. So a power loss can make a node forget
work, never a promise. Backup appends are committed, not synced; that is
Layer B's single-failure contract below
([Durability and failover](../reference/features/durability-and-failover.md#what-on-disk-means)).

### Failover by topology

| Topology | Policy | Added acknowledgement latency | Holder crashes and returns before a takeover | Holder away past a takeover | Failover |
|---|---|---|---|---|---|
| Single node | `Local` | none | nothing lost | n/a | n/a |
| Only distant peers (no backup in budget) | `Local` (Layer A) | none | nothing lost | forwarded ops replayed by their requesters; the holder's own unshipped writes come back as a replay when it returns | lease TTL (60 s) + margin |
| A peer within the RTT budget | `Backup` (Layer B) | one round trip to the backup | nothing lost | nothing lost | detection + one CAS: ≈ 1.5 s in the harness, 2.3–4.7 s on EC2 ([RESULTS](../../bench/remote/RESULTS.md#failover-time)) |
| `fs create --ack-policy s3` | `S3` (Layer C) | one S3 round trip per group commit | nothing lost | nothing lost | detection + one CAS with P2P; the TTL without |

"Nothing lost" is the single-failure contract: any failure of one machine,
power included. A power loss of the holder and every backup together can
lose `Backup`-acknowledged writes of the last seconds; `ack=s3` (or
`fsync()` with `--fsync-mode s3`) covers that. For the first seconds of a
tenure, while a backup that could be had is being brought up, a `Local`
holder acknowledges nothing that rests on its disk alone: those
acknowledgements wait for S3.

### Seal-based failover

A backup that hears nothing from its holder for
`CONSTELLATION_BACKUP_TAKEOVER_MS` (1.5 s) **seals**: it persists and
syncs "epoch *e* sealed" and refuses every later epoch-*e* append. The
old holder needs every listed backup's acknowledgement for anything it
acknowledges, so after the seal it can acknowledge nothing more, whether
it is dead, slow or only cut off from this backup. The backup then
re-reads the lease, CASes it to epoch *e*+1, tails S3 to head, ships its
epoch marker (with `TailFollows`, so readers know the predecessor's tail
comes back after it), re-ships its backup tail deduplicated by rid, and
opens. A holder's CAS removing a silent backup and that backup's takeover
CAS are on the same lease version (`config_version`), so exactly one
wins. A node that is not a listed backup waits 3 s past a `Backup`
lease's expiry before claiming it, so a backup gets there first.

Under `ack=s3`, every acknowledged record is in a log slot below the
taker's marker, so any peer may take a silent holder over (silence is
read from its log stream, so this needs P2P); a holder that renewed its
lease recently has proven it reaches S3 and keeps it.

A fast successor (seal or `ack=s3`) waits out the previous tenure's
read-delegation and lock grants before it acknowledges a mutation or
grants a lock, and accepts lock reclaims meanwhile. A TTL successor has
nothing to wait for: every grant was capped by the lease that expired
([Durability and failover](../reference/features/durability-and-failover.md#seal-based-failover)).

Three rules keep false alarms cheap: a holder whose lease renewal is
merely slow keeps heartbeating its backup until the lease actually
expires; a backup that restarts counts silence only from the moment its
link to the holder is up; and a node that answered "sealed" for an epoch
is never invited back as its backup.

### Nobody observes a tentative effect

Under `Backup` and `S3`, the holder's reads and its answers to other
clients' ops wait until any unshipped rows they would observe are
durable, so no client of any node sees an effect a failover could roll
back. Under `Local` without a backup, a refusal can observe the holder's
acknowledged-but-unshipped effect, which a crash of the holder then rolls
back and replays later; that window is the price of Layer A's zero
latency.

### Delegates

- **Crash, no backup**: the root reclaims the grant after `ttl + margin`
  (≈ 6 s) and ends the generation; requesters replay their acknowledged
  ops by rid through the root.
- **Crash, with a backup**: the root has the delegate's backup seal,
  drains what it holds, and continues.
- **Partitioned from the root**: the delegate stops at
  `sent + ttl − margin`, before the root may reclaim; its unstreamed
  transactions are stranded and replayed by rid.
- **Root fails over**: the new root inherits the live generations, whose
  grants were capped by the old lease. After a TTL takeover they are
  already dead; after a fast takeover the delegates re-stream from what
  the log has, and a backup that was itself a delegate rolls back its own
  unappended rows and replays them after the predecessor's tail, never
  ahead of it.
- A dependency on a delegate transaction that was acknowledged but never
  appended is never executed around: the op waits (`Held`) until its own
  replay has landed
  ([Delegations](../reference/features/delegations.md#failures)).

### Lock holders

A node partitioned from a file's sequencer past the lock TTL loses its
grant and is fenced: `EIO` on the locked files until they are unlocked,
and on every write or namespace operation of the lock's owner, on any
file, until the owner's locks are gone.
The sequencer outwaits the grant (`granted + ttl + margin`) before
granting the lock elsewhere, so the fence always comes first. With P2P up
but the sequencer unreachable, a non-blocking lock fails with `ENOLCK`
and a blocking one keeps retrying.

### Other failures

| Failure | Behavior |
|---|---|
| node crash | journal, speculation, backup tail, seals, promises and epoch state reload from the local store; stranded ops replay by rid; dirty chunks re-upload; the lease is re-acquired or expires |
| forward times out (holder slow or gone) | same-rid retries, then the inbox (no P2P path) or the lease path; resolved exactly once; `EIO` at the client deadline (2 × TTL), never a re-execution |
| holder deposed (it lost the lease while alive) | it rolls its unshipped journal back and replays it by rid through the new holder; only true overlaps become conflict copies |
| P2P down, S3 up | everything works through the lease path and the S3 inbox, minus backups, delegations, strict ReadIndex, cluster locks and fast takeover |
| clock skew | lease, delegation, read-delegation and lock timing all assume `margin > 2 × drift` (±500 ms at the defaults); correctness otherwise relies on CAS ordering, never wall clocks |
| cache disk full | evict clean → throttle → ENOSPC (reads still stream uncached) |
| logical quota exceeded | ENOSPC on write/truncate/fallocate growth (best-effort; see above) |
| DB disk full | read-only mode + flush-to-recover; alarms in status/UI |
| corrupted chunk (local or S3) | hash verification on every read; local → refetch, S3 → error + fsck report (peer copies may heal) |

### Poison records

A pending upload whose chunk is gone from the local cache (a disk fault,
a manual deletion) cannot ever reach S3. It no longer blocks the node's
whole ship: each ship holds back only the transactions whose manifest
names such a chunk and every later transaction that touches the same keys
(a `chmod`, rename or unlink of that file). Everything else ships.
`status.held` lists what is held, and `constellation repair drop-held`
turns a held manifest into a conflict copy with the lost chunks as holes
and replays its dependents by rid
([Write-path hygiene](../reference/features/write-path-hygiene.md#held-records-statusheld)).

### What an S3 outage blocks

A node cannot distinguish "S3 is down globally" from "I am partitioned off,"
so the rules below are safe in both cases. Principle: **reads never block;
writes remain allowed exactly as far as pre-existing authority reaches, and
new authority cannot be created** (hard constraint 2: there is no second
bucket to fall back to).

Never blocked:

- All metadata reads — the tree replica is local; `ls`/`stat`/`find` work
  indefinitely.
- Content reads served by the local cache (pinned subtrees fully, by
  construction) or by any reachable peer (cooperative cache is S3-free).

Blocked immediately:

- Cold reads of chunks neither the node nor a reachable peer holds → EIO or
  block-with-timeout (mount option).
- Acquiring write authority the node does not already hold — lease
  acquire/steal is an S3 CAS. Exception: inside a continuation epoch, leases
  transfer P2P among epoch members.
- Every write from a node with no P2P path to the holder: the S3 inbox is
  in the bucket.
- Acknowledgements under `ack=s3`: they wait for a segment that cannot be
  written. An `S3` lease is never carried into an epoch.
- Visibility of new commits from nodes with no P2P path to us.
- Enrollment, leave, GC, commit publication, snapshot create/delete —
  deferred or rejected cleanly when S3 is down; none load-bearing. Leave
  never auto-fires on unmount or a missed heartbeat.

Degrades on a timer — writes under a held lease:

- The holder keeps writing at local speed (journal locally, flush later),
  but lease *renewal* is also an S3 CAS. At TTL expiry the subtree goes
  **read-only**: if the outage is really just this node's partition, a node
  with working S3 can legally take the expired lease (with `f > 0` only
  with `f` promises; a `Backup` lease only after the 3 s non-backup
  grace). A FUSE op still in doubt at its deadline (2 × TTL) fails with
  `EIO`, never re-executed.

Escape hatches (both are pre-arranged, non-stealable authority):

- **Offline designation** (§5.2): no TTL-steal on the designee's claim — it
  writes through any outage, indefinitely. This is exactly what
  `offline <path>` buys over a plain lease.
- **Continuation epoch** (§5.3): at least `N − f` write-eligible nodes in
  one P2P component keep writing collectively; nobody outside the
  component can take a lease without violating a persisted promise.
  After the outage, the flush ships everything except what waits for a
  chunk a member still holds (deferred, `status.held`), and the log
  keeps moving for everything else; a member gone for good with the
  only copy of such a chunk is an operator decision (`repair drop-held
  <ino> --remote` on the node that lists it under `held.remote`).

One node losing S3 while its peers still reach it is not an outage: its
closes hand their chunks to a peer after 6 s (§3), its metadata forwards
over P2P as usual, and it proposes no epoch, since a member it asks
reaches S3.

Three-machine example (desktop, laptop, server, all write-eligible): home
internet dies with all three on the LAN → they form an epoch and keep
writing. With the default `f = 0`, the laptop away and S3 down means no
epoch at all; with `f = 1`, desktop and server form one without it, while
the laptop alone (one of three) cannot. The laptop alone with no S3 is
read-only after the TTL, unless its paths were marked `offline`. The
laptop remote *with* S3 writes normally; the nodes that lost S3 follow the
rules above.

Cost bound: during an outage the journal and dirty chunks accumulate on
local disk, so outage duration is bounded by cache/DB space — then the
evict → throttle → ENOSPC ladder above applies. The spool is fully
observable: `constellation status` and the web UI dashboard show
outstanding unflushed state — journal records and dirty-chunk count/bytes,
oldest unflushed op age, current flush lag, spool growth rate, and
estimated headroom (time-to-full at the current rate) — with warning
thresholds surfaced as alerts and exported via `/metrics`, so a user can
see spooling build up long before the throttle engages.

## 10. Control Plane

One control API (unix socket default, optional TCP) serves the CLI, the
embedded web UI (axum + rust-embed, binds 127.0.0.1; remote via opt-in auth
or the iroh tunnel), and scripts. Surfaces: dashboard (health, sync lag,
spool backlog + headroom, see §9), peers (RTT, direct/relay, registry), file
browser (rename/move/delete/up/download, inspect, history), cache (states,
hit rates, prefetch efficiency, pins), leases/designations (+ admin
force-release), snapshots/clones (create, browse, delete, mounts),
compression settings, ops (log tail, fsck, doctor), Prometheus `/metrics`.

CLI highlights: `fs create|set|passwd|list`, `mount [SOURCE[@snap]]
MOUNTPOINT`, `umount`, `export`, `status` (includes `node_id` /
`enrolled` and the spool), `doctor`, `pin|unpin|pins`,
`offline|online|designations`, `delegate|undelegate|delegations`,
`leave [--node-id N] [--force]`, `reintegrate`, `write-mode`, `quota`,
`prune`, `inspect <path>`, `cache ls|stat|prune`, `log tail`,
`snapshot create|ls|delete`, `clone <path@snap> <dest>`, `gc run|verify`,
`fsck`, `repair drop-held`. The knobs are in
[Configuration](../reference/configuration.md).

## 11. Scale Targets

Designed/tested for 1–10M files, 1–5 TB, 3–10 nodes. Reference census
(11.9M entries, 1.3 TiB). The fjall replica was the most compact engine in
ADR-9's benchmark of the census corpus (0.66 GB, against SQLite's 1.60 GB).
A cooperative-cache mirror costs 8–12 bytes per peer chunk. Two format
choices keep growth to 100M+ files free of on-bucket migrations: typed
manifest entries (§3), and the commit chain's content-addressed tree
(plan 28), which a node can read partially rather than replicate whole.

## 12. Metadata-DB and FUSE Fast Paths

FUSE's aggressive caching and notification knobs are normally crippled by
one problem: the daemon can't know what changed remotely. The log-replica
architecture *always* knows — every remote change arrives as a log record —
which unlocks the aggressive end of every knob below. Complemented by what
an indexed, transactional replica answers directly.

Not every item below is built yet. Built: push invalidation, the
recursive aggregates (`user.constellation.rsize`), `SEEK_HOLE`/`SEEK_DATA`,
metadata-only `fallocate`/`truncate`, and time travel as snapshots (§13).
Designed but not built: `constellation find`, `constellation changes`, the
`unlink_subtree` and `copy_manifest` records (and so `copy_file_range`),
stable readdir cursors (`readdir` resumes by position today), negative
dentry caching (a lookup miss is answered `ENOENT`, which the kernel does
not cache), `READDIRPLUS`, the writeback cache with `FOPEN_KEEP_CACHE`,
and FUSE passthrough.

### Enabled by the local replica

- **Instant search** (`locate`/Everything-style): indexes on name, size,
  mtime, kind answer `constellation find` in milliseconds over 10M files
  with zero I/O — a complete, always-current replica makes results
  authoritative, something no scanning network FS can offer.
- **Change journal**: the metadata log already is a USN-journal equivalent.
  `constellation changes --since <txid>` (CLI + API stream) gives backup
  tools, indexers, and build systems exact deltas — no tree scans, ever.
  Zero extra cost; the log exists.
- **O(1) recursive aggregates**: per-directory recursive size/count
  maintained transactionally during log application. `du -sh` on any
  subtree is one row read (exposed via `user.constellation.rsize` xattr and
  the UI); pin admission checks become one indexed query instead of a walk.
- **Bulk namespace ops as transactions**: recursive delete/chown/chmod run
  as one transaction emitting one compact log record (`unlink_subtree`)
  — `rm -rf` on a million files in milliseconds, syncing to other nodes as
  a single record.
- **Stable readdir cursors**: key-ordered cursors give exact resumption
  offsets for arbitrarily large directories, avoiding FUSE's classic
  skipped/duplicated readdir entries.
- **Time travel**: a commit of the tree as of log position T = a read-only
  view of the tree as it was; content addressing keeps old chunks alive
  until GC. Named, GC-protected freezes of this are **snapshots** (§13).

### Enabled by the FUSE API

- **`READDIRPLUS`**: attributes returned with each dentry from a single
  replica scan — kills the stat-storm that slows `ls -la`, `find`, and
  rsync on every network FS.
- **Kernel TTLs + push invalidation**: every change from another node is
  pushed to the kernel with `notify_inval_entry`/`notify_inval_inode`
  (§6, invariant 1), so the entry/attribute TTL (1 s under `--cto
  bounded`, 0 under strict) is only a backstop.
- **Negative dentry caching**: ENOENT answers cached as negative entries
  under the same TTL and invalidated by the same push, so repeated misses
  (compilers probing include paths, `PATH` lookups) stay in the kernel.
- **`copy_file_range` / reflink as pure metadata**: content addressing makes
  every copy a manifest copy (`copy_manifest`) with chunks shared
  automatically — instant, zero-I/O copies of any size.
- **`SEEK_HOLE`/`SEEK_DATA`**: the chunk list knows exactly where holes are,
  so `cp --sparse`, rsync, and tar skip them instead of reading zeros.
- **`fallocate`/`truncate` as metadata-only ops**: extend or punch without
  touching chunks until data is written.
- **Writeback cache + `FOPEN_KEEP_CACHE`**: the kernel page cache absorbs
  small writes and persists pages across opens; invalidated exactly on
  remote change. Warm reads run at page-cache speed.
- **FUSE passthrough (Linux ≥ 6.9)**: for clean, fully-cached chunks,
  register the cache file with the kernel so reads bypass the daemon
  entirely — near-native throughput on cache hits. Optimization-phase item
  (splice is the fallback on older kernels).

## 13. Snapshots and Clones

ZFS-style, on any path: `constellation snapshot create <path>@<name>`
freezes the subtree under that name until the snapshot is deleted. Any
number of named snapshots per path; identity is `path@name`.

### Representation

A snapshot is a **git-style tree object**: at creation, the subtree's
metadata (dentries, inodes, manifest references) is serialized into
content-addressed blobs in the chunk store, and `snaps/<snap-id>.json`
records `{name, path, created, creator, tree root hash}`. Consequences:

- **Creation is cheap and gets cheaper**: tree blobs are content-addressed,
  so unchanged directories hash identically — repeated snapshots of a
  slowly-changing tree upload only the changed spine, and data chunks are
  never copied at all (they're immutable already).
- **Self-contained**: reading a snapshot needs no log replay or MVCC in the
  replica; nodes load tree blobs on demand (cached like any chunk).
- **GC roots**: every tree reachable from `snaps/` protects its metadata
  blobs and data chunks. Delete the snapshot, and GC (grace period, §3)
  reclaims whatever nothing else references.
- The `snap_create` record is written under the subtree's normal write
  authority (lease), so the frozen state is a well-defined log prefix —
  never a torn mid-transaction view.

### Access: `.constellation/snapshot/<name>/`

Every directory exposes a synthetic `.constellation/` (like `.zfs/`):
**hidden from readdir, resolvable by explicit lookup**, so rsync, `find`,
and backup tools never wander in accidentally. Under
`snapshot/<name>/` the frozen tree is served through the normal read path —
chunk cache, cooperative cache, and prefetcher all apply. Snapshot listings
also appear in the CLI (`snapshot ls [path]`) and web UI.

### Read-only snapshots, writable clones

Snapshots are **always immutable**. Writability is real and cheap, but it is
deliberately a separate object — a **clone**, exactly as in ZFS:

```
constellation clone /projects@friday /projects-fix
```

instantly creates a normal writable subtree initialized from the snapshot
tree (metadata copied lazily, data chunks shared; a `clone` log record).
Rationale for not offering "rw snapshots" directly: a writable snapshot is a
*branch*, and branches need everything live subtrees have — a log stream,
lease-managed write authority, multi-node visibility. Cloning into the
namespace reuses all of that machinery unchanged, while "writable snapshot
under `.constellation/`" would silently fork state in a hidden location and
break the one meaning of a snapshot name: *this exact frozen tree*. The
`--rw` selector exists as sugar: `mount --rw <path>@<name>` auto-creates a
clone (named, or ephemeral with `--ephemeral`: deleted on unmount) and
mounts it.

### Mounting subtrees and snapshots anywhere

A mount is just a choice of root inode, so any of these work:

```
constellation mount --s3 s3://bucket/prefix /            /mnt1
constellation mount --s3 s3://bucket/prefix /any/path    /mnt2
constellation mount --s3 s3://bucket/prefix /path@friday /mnt3   # read-only
constellation mount --s3 s3://bucket/prefix /path@friday /mnt4 --rw  # clone
```

One daemon per (bucket, prefix) per machine serves all its mounts — they
share the replica, chunk cache, leases, and P2P endpoint. Subtree mounts
confine the FUSE view, not authority: leases/pins/offline marks behave
identically however the tree is mounted.

### Deleting a snapshot that is in use

`snapshot delete` emits a `snap_delete` record and removes the `snaps/`
entry; it cannot reliably know about mounts on offline nodes, so deletion
is never blocked cluster-wide (best-effort courtesy: the CLI warns if
gossip shows the snapshot mounted somewhere; `--force` skips the prompt).
On every node, applying the record flips existing mounts and
`.constellation/snapshot/<name>` views of it to **ESTALE** for all new
operations — the standard "object went away under a network mount" signal.
Already-open file handles keep working on a best-effort basis for the GC
grace period (locally cached chunks keep serving; S3 fetches of
already-collected chunks fail with EIO). Clones are unaffected: they
reference chunks independently as GC roots, so deleting the origin snapshot
never breaks a clone.

## 14. Garbage Collection

Two mechanisms share the name; only one is distributed. **Local cache
eviction** (LRU over clean chunks, §7) is per-node and needs no
coordination — deleting a local copy loses nothing. **Bucket GC** reclaims
shared objects and is coordinated as follows.

### Who runs it

One node at a time, holding the `_gc` singleton lease (`leases/_gc.json`),
the same CAS + TTL object as the root lease. Every daemon runs a round on
its own cadence (`CONSTELLATION_GC_INTERVAL_S`, daily by default), and
`constellation gc run` triggers one by hand; a node that finds the lease
held skips the round. GC authors no log records: a round tails the log to
head like any follower.

A round outlives one lease TTL by construction (it waits a full TTL
between condemning and deleting), so the lease is **renewed through the
round and every destructive step is fenced on it**: the grace wait renews
every third of the TTL, and the delete loop — chunks, blobs, dead packs,
compaction batches, incomplete packs — renews (a CAS on the lease
object's ETag) before its first delete and every 64 deletes after that.
A refused renewal means another round took the lease; the round stops
on the spot. The fence is the store's CAS, never the round's clock: a
round whose process paused for minutes is stopped the same way. Without
this a second round could take the lapsed lease and publish a new
condemned pointer while the first was still deleting, which is exactly
the premise the writer-side argument below rests on.

### What a round collects

- **Chunks**, from one LIST-based orphan pass over `chunks/`: a chunk is
  a candidate when it was last written more than `gc.horizon` ago
  (`CONSTELLATION_GC_HORIZON_S`, 7 days) and nothing protects it — no
  live manifest in the round's replica (tailed to head first), no
  snapshot tree, no live `holds/` record. A hash the current condemned
  pointer already lists is a candidate *again*: the new pointer carries
  everything an interrupted or paused round could still delete (see
  below). (Plan 28 replaced the continuously maintained dereference
  index with this pass and a reachability walk; the bucket is the only
  source of truth.)
- **Log segments** below the head commit's `applied` position minus
  `CONSTELLATION_LOG_RETENTION_SEGMENTS` (128) **and** older than the
  completion retention (900 s), so an in-doubt op's coverage rule always
  holds (§4).
- **Metadata**: commits beyond the retained window, and packs and blobs
  that a reachability walk from the retained commits, snapshots and holds
  no longer reaches (compacting partly dead packs), under the same
  condemn-wait-recheck handshake as chunks
  ([Configuration](../reference/configuration.md#garbage-collection)).

### The dedup race, and the layered defense

The dangerous race is not delete-vs-read but **delete-vs-dedup**: a writer
may commit a manifest referencing an existing chunk *without uploading it*
(a dedup hit). Deleting such a chunk between the hit and the commit would
create a committed reference to nothing. Defense in depth:

1. **Horizon**: only chunks last written more than `gc.horizon` ago are
   candidates — one number kills every short-window race (flush lag, crash
   windows, epoch flushes).
2. **Condemned-list handshake**: before deleting, the round CAS-publishes
   its candidates as the condemned pointer (`gc/condemned.json`, with an
   epoch) and announces it over gossip, waits one full lease TTL, tails
   the log to head again and re-checks liveness. A writer that is not
   authoritative by then can no longer commit.
   **Writers read the pointer only after a dedup hit.** If the upload
   found its object absent (the create created it, or a `HEAD` missed and
   the bytes were PUT), the pointer is not read at all: with the object
   absent, a create and an unconditional PUT leave the bucket in the same
   state, and the pointer only ever chose between them. If the object was
   there, the writer reads the pointer *after* that answer (`If-None-Match`
   on the last ETag, so usually a bodyless `304`):
   - the hash is listed → upload the bytes anyway;
   - no pointer was ever published → the hit is sound;
   - the hash is not listed and the pointer is the same one the writer
     had read before it sent its existence request → sound: no round
     published in between, so the round whose list was current deletes
     nothing of it;
   - otherwise (the pointer moved, or nothing was read before: the first
     hit after a mount) → a `HEAD` sent after the read decides.

   Every execution of this order is one the older "read the pointer
   first" order could produce, so the argument above carries over, and a
   unique small file costs one S3 request instead of two.

   The premise of that argument is that **a hash absent from the current
   pointer is deleted by no round at all**. Two things hold it up, and
   neither depends on timing: the lease fencing above (a round deletes
   only while its own list is current, or at most one fenced batch past
   the moment another round took the lease), and the carry-over (the new
   round's pointer lists every still-present, still-unprotected hash the
   old pointer listed, so whatever that batch deletes is on the new list
   too — or was found protected by the new round, in which case the
   committing writer's dedup hit either preceded the old round's
   publication, and so its commit preceded the old round's post-wait
   re-check, or saw the old pointer and re-uploaded, which rule 3
   catches).
3. **Re-upload guard**: an identical-content PUT "resurrects" a condemned
   chunk only if it lands after the delete. So the delete loop `HEAD`s
   each candidate first and keeps any object whose `Last-Modified` moved
   past the marked listing's (by more than a `HEAD`'s one-second
   resolution): that object is not the one marked, and keeping it cannot
   dangle anything. Portable S3 has no conditional DELETE, so a write
   landing in the `HEAD`→`DELETE` gap remains possible: it needs a
   condemned hit re-PUT in that very gap *and* a commit after the round's
   second tail.
4. **Open-handle holds**: chunks named by a live `holds/*` record are
   exempt (§3, unlink while open), at the mark and again at the re-check
   right before deletion — the TTL wait between the two is the window a
   node has to claim a file that was unlinked under its open handle. The
   bucket record is the authoritative claim, so the sweep honors holds
   regardless of which node runs it.

Snapshot trees and clones are GC roots (§13); the grace behavior for
deleted snapshots is defined there.

### Falling behind segment GC

A node offline (or partitioned) longer than log retention cannot tail
its gap: the segments are gone. A GET-next `404` cannot tell "at head"
from "pruned past", and a replica that took the `404` for the head would
serve stale state for ever — and, taking the lease, would CAS-create its
epoch marker into the deleted slot and fork the log. The answer is one
LIST with offset from the cursor (`LogStore::first_segment_from`):
nothing at or after it is the head, the cursor itself is a segment that
landed meanwhile, anything later is a gap. Retention only ever deletes a
contiguous prefix below a commit's `applied`, so the answer cannot go
stale in the direction that matters.

- **Before a lease claim, always.** The takeover CAS is preceded by the
  check unless the acquisition reached a handoff's reported head, or is
  re-adopting this node's own lease (the object still names it, so nobody
  else appended). Segments above a holder's head are above every floor
  any round could have used: floors come from commits, commits from
  holders, and the CAS fences any later holder. A gap found here ends the
  acquisition unacquired; nothing was written.
- **On a running tail**, when a gossip hint or the direct stream's
  reported head lies at or past the cursor (at most every
  `CONSTELLATION_LOG_GAP_HINT_CHECK_MS`), once on the first empty probe
  after a mount, and as a backstop every `CONSTELLATION_LOG_GAP_CHECK_MS`
  (5 minutes: one LIST against roughly 150 idle GETs). A holder never
  checks; it is the sole appender.
- **At mount**, before anything tails or a view opens.

A gap rebuilds the replica in place: a side replica is bootstrapped from
the head commit plus the retained log (the fresh-node path, §4) and its
namespace swapped in; node-local state (identity, the journal, pending
uploads, `completed`) stays. The cursor restarts from the rebuilt
position. An op stranded in the gap cannot be resolved against
`completed` and fails with `EIO` rather than being re-executed
([Configuration](../reference/configuration.md#garbage-collection)).

### Auditability

Every deletion is journaled with its evidence (rule applied, the mark's
evidence, the condemned-set epoch) under `gc/journal/`;
`fsck` cross-checks deletions post-hoc, and `gc verify` runs the mark phase
without sweeping to report what *would* be collected and why.
