# Constellation Design

Companion documents: [GOALS.md](GOALS.md), [DECISIONS.md](DECISIONS.md),
[TESTING.md](../how-to-guides/development/TESTING.md). The original v1
phased roadmap is frozen at [ROADMAP.md](../history/v1/ROADMAP.md).

## 1. Overview

Every node runs the same single binary: FUSE mount + daemon + CLI + web UI.
The only mandatory infrastructure is one S3-compatible bucket, which stores
data chunks, the metadata log, leases, and the node registry. Peers connect
directly (iroh QUIC, NAT-traversed, encrypted) as a latency fast path;
correctness never depends on P2P.

```
            +----------------- node -----------------+
  app ----> | FUSE (fuser)                            |
            |   VFS core: inodes, leases, caches      |
            |   meta store (SQLite, engine trait)     |
            |   chunk cache (disk, LRU/pin/dirty)     |
            |   iroh endpoint + gossip                |<===> peers (QUIC)
            |   control API (unix socket) + web UI    |
            +--------------------+--------------------+
                                 |
                                 v
            S3 bucket: chunks/  log/  leases/  checkpoints/
                       snaps/  nodes/  keys/
```

Trust model: **membership = bucket access.** Valid credentials (`AWS_*` env,
profiles, IMDS, ...) make a node a full member; IAM read-only credentials make
it a read-only follower (auto-detected). Constellation never requires the
ability to change bucket policy or IAM.

Enrollment is self-service on first mount (CAS-claim a cluster-unique node
id into `nodes/`). **Unmount is a temporary departure** — the registry
record stays, so the node remains write-eligible and may return with S3.
**`constellation leave` is the permanent departure** (§8): it retires the
registry record so survivors can form continuation epochs without waiting
forever. Heartbeats and missed pings never drop a member (ADR-12: safety
never depends on failure detection).

## 2. S3 Bucket Layout

```
<prefix>/
  meta.json                     # fs UUID, format version, settings, E2E flag
  nodes/<node-id>.json          # self-enrolled members (pubkey, P2P addr, ro);
                                # leave writes a tombstone {retired:true} —
                                # the id is never recycled
  heartbeat/<node-id>           # liveness beacons (~15-30 s), UX only
  leases/<partition-id>.json    # CAS lease objects (the arbiter)
  log/<partition-id>/<seq>.zst  # ordered metadata log segments
  checkpoints/<partition-id>/<txid>.zst # periodic log-compaction checkpoints
  snaps/<snap-id>.json          # user snapshots: name, path, tree root hash (§13)
  holds/<node-id>.json          # TTL'd open-handle holds on orphaned inodes (§3)
  chunks/<a>/<b>/<hash>         # content-addressed blocks (sharded by hash)
  keys/keyring                  # only in E2E mode: wrapped DEKs
```

Required S3 features: GET/PUT/DELETE/LIST plus conditional writes
(`If-None-Match: *` for create, `If-Match: <etag>` for CAS). Verified by
`constellation fs create` / `doctor`.

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
exponential backoff.

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
  big single blobs (checkpoints). Order in E2E mode:
  compress-then-encrypt.
- **Manifest spill**: a file's metadata row stores the chunk list inline when
  <= 8 chunks (~32 MiB; covers >99.9% of files per the reference census);
  larger files store the 32-byte hash of a *manifest blob* (the serialized
  chunk list) kept in the chunk store. Manifest entries are typed;
  reserved types: `inline`, `manifest`, `slice-overlay` (future: append small
  overwrites without chunk RMW), `pack` (future: many tiny chunks in one
  object), `cdc` (future: content-defined chunking). Reservations cost bytes
  now to guarantee zero migrations later.
- Old chunks are immutable; edits create new objects. Unreferenced objects
  are removed by background GC (checkpoint- and snapshot-aware, grace
  period, fsck-verifiable; coordination in §14). GC roots: live manifests,
  checkpoint state, and every snapshot tree (§13).

### Write path (partial writes)

`write()` materializes only the touched chunk(s) (cold: one 4 MiB GET),
applies the edit in the cache and marks the chunk dirty; repeated writes
coalesce. On `close()`/`fsync()`: re-hash, compress, (encrypt,) PUT new
chunk(s) + new manifest in parallel, then journal a small log record.
Other nodes re-fetch only changed chunks: a 1-byte edit of a 1 GB file syncs
~4 MiB. `fsync` modes: **default** = locally durable + journaled, S3 flush
async within a bounded lag (safe: leases fence other writers); **paranoid** =
`fsync` returns only after chunk + log record are on S3.

### Streaming writes: files larger than the cache

Reads are size-unbounded by construction (prefetch ahead, evict clean
behind: a 10 TB file streams through a 50 GB cache). Writes get the same
property via **eager chunk upload**: because chunks are immutable,
content-addressed, and invisible until the manifest + log record commit at
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
  advertises them in a TTL'd `holds/<node-id>` record (renewed on the
  heartbeat cadence, gossiped for freshness). The §14 sweep treats held
  inodes' chunks as referenced. Crash safety falls out: a dead node stops
  renewing, its holds expire, orphans reap — no leaked cruft.
- **Zero cost on the hot paths**: normal `open()` never touches holds — the
  record exists only for the rare intersection *open handle × unlinked
  inode*. It is one batched object per node (all its orphans; absent when
  the set is empty, the usual state), written asynchronously on set changes
  and refreshed per heartbeat — neither `open()` nor `unlink()` waits on
  it. Asynchrony is safe because the GC horizon (days) already protects
  freshly-dereferenced chunks; a hold only needs visibility before the next
  sweep, not before the syscall returns.
- **Last close, cluster-wide**: when the final hold disappears (release or
  TTL expiry), the inode's chunks deref and ride the normal GC horizon.
- **Writes to orphans** are allowed (POSIX): flushed as inode-keyed log
  records, so other nodes that also had the file open see them under normal
  close-to-open rules — and better: `fsync()` (or close, or the background
  flush) is the *publication point*. Remote holders apply the record and
  push-invalidate the inode's pages (§12), so reads through their existing
  handles return the new bytes after flush + propagation (ms on a LAN) — no
  reopen needed, which for an orphan is impossible anyway. Concurrent
  orphan writers serialize through the partition lease like any file.
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

### Local store

Every node keeps a **full metadata replica** in SQLite (chosen by benchmark —
see DECISIONS.md ADR-9; the store sits behind an engine trait, LMDB is a
future alternative). Core tables:

```sql
inode  (ino PK, kind, size, mode, uid, gid, nlink, mtime, ctime,
        chunk_info BLOB,      -- typed: inline list | manifest hash
        comp_setting NULLABLE, -- explicit compression override, if any
        rsize, rcount)         -- recursive aggregates for dirs (§12)
dentry (parent, name, ino, PK(parent,name)) WITHOUT ROWID
partition (part_id PK, root_ino, log_pos, lease_state, ...)
cache  (hash PK, size, state {clean|pinned|dirty}, atime)   -- LRU accounting
journal (seq PK, record BLOB)  -- local ops not yet flushed to S3
```

The full tree is browsable offline on every node (`ls`/`stat`/`find` always
work); file *content* is readable as far as the chunk cache reaches.

### Metadata log

Each **partition** (see below) has one ordered log stream on S3. A segment is
a zstd batch of records; each record:

```
{seq, txid, node_id, sig, op, args...}
  op ∈ {mkdir, create, unlink, rename, link, symlink, setattr,
        write_manifest, set_comp, part_split, part_merge, rename_xpart,
        unlink_subtree, copy_manifest, snap_create, snap_delete, clone, ...}
```

(`unlink_subtree` and `copy_manifest` back the bulk-delete and reflink fast
paths in §12; `snap_*`/`clone` back snapshots (§13); like the manifest entry
types, unknown ops are a versioned, append-only registry.)

Segments are appended with `If-None-Match` on `log/<part>/<seq>` — the
sequence number collision is the conflict detector; losers re-tail and retry.
Followers tail segments (poll + gossip push) and apply them to their replica.
Periodic **checkpoints** compact a partition's history; segments older than
the checkpoint are GC'd. Point-in-time restore falls out of
checkpoint+segments.

### Partitions

A filesystem starts as a single partition rooted at `/`. Partitions **split
and merge automatically** at directory boundaries (triggered by offline
designation, long-lived foreign leases, or log traffic thresholds) — never
user-managed. Split/merge are log records; no data moves. Cross-partition
renames use a linked two-record commit (`rename_xpart` pair with a shared
transaction id; both streams must contain the pair for the rename to be
final, with a documented recovery rule for half-committed pairs).

### Leases (the authority mechanism)

A lease on `leases/<part>.json` (CAS-updated, TTL ~60 s, renewed at half-TTL)
grants one node exclusive write authority over a partition:

- The holder journals ops locally at local-FS speed and flushes segments
  asynchronously (bounded lag, default ~5 s / ~4 MiB).
- Acquire: CAS-create or CAS-swap an expired/released lease, after applying
  the previous holder's flushed log. Transfer is P2P-accelerated (holder
  flushes + hands off in one RTT) with the S3 CAS as the commit point.
- Fencing: every lease has an epoch counter; log segments carry it; a
  deposed holder's late flushes are rejected by sequence+epoch mismatch.
- Expired lease + unreachable holder: takeover is legal only after applying
  everything the holder flushed; the holder's unflushed tail (bounded lag)
  becomes an offline branch handled by reintegration rules (default modes
  prevent this by construction — see availability matrix).

## 5. Write Authority: the One Rule

> A node may alter a subtree only while it holds an unexpired **authority
> chain** rooted in something that cannot be concurrently claimed.

Three roots exist; all transitions between them are explicit, signed, and
persisted before activation. Safety never depends on failure detection —
heartbeats (`heartbeat/*`, P2P keepalives) feed status UX only.

### 5.1 Leases (default)

Section 4. The desktop that touches everything simply keeps its leases warm
and runs at local speed.

### 5.2 Offline designation

`constellation offline <path>` / `online <path>` — CAS-enforced, exactly one
designee per path, overlap-checked.

- **Designee reachable → everyone still writes.** Other nodes' authority for
  the subtree becomes a short-TTL *delegation* granted by the designee, and
  their flushes require the designee's ack. Invariant: at any instant the
  designee provably holds all committed changes, so its offline writes are
  always linear continuations. Cost: ~1 RTT to the designee per foreign
  flush (sub-ms on a LAN).
- **Designee unreachable → delegations expire**, others go read-only on that
  path; the designee writes indefinitely (its root claim is non-stealable)
  and reintegrates on reconnect. `offline --ro` grants a read guarantee
  without write authority.

### 5.3 Continuation epochs

If S3 is unreachable but the P2P-connected component contains **all
write-eligible nodes** (every *live* non-read-only registry record —
read-only followers and leave-tombstones with `retired: true` do not
count), members sign and locally persist a continuation epoch: leases
transfer P2P, writes journal locally, everything flushes when S3
returns. Majority quorum is deliberately insufficient (a minority node
with S3 access could legally take expired leases). If the component loses
a member mid-epoch, the remaining nodes go read-only; the departed member
persisted its epoch promise and must not take epoch-held leases via S3
until holders flush — both sides freeze, no conflict. Promises are
persisted before activation (crash-safe).

Shrinking the write-eligible roster is an **operator action**
(`constellation leave`, §8), never a timeout: an unmounted or
P2P-unreachable node still counts until explicitly retired, so a
remaining component cannot open an epoch while a still-enrolled writer
might take expired leases via S3.

### Availability matrix (per subtree)

| situation | designee/holder | others |
|---|---|---|
| all connected | write (local speed) | write (close-to-open) |
| node isolated, no designation | that node: read-only | write |
| designee isolated | write | read-only on path |
| S3 down, all writers on LAN | write (epoch) | write (epoch) |
| S3 down, writer missing from LAN | read-only | read-only |

## 6. Consistency Modes

- **close-to-open (default)**: `open()` sees the latest completed `close()`
  cluster-wide; concurrent writers on different nodes serialize via lease
  transfer.
- **strict (per mount/subtree)**: cross-node byte-range `fcntl` locks and
  pre-close visibility; every conflicting op pays coordination RTTs.
- **relaxed (per node/subtree, explicit opt-in)**: write locally, sync in
  background, conflicts *detected* and materialized on reintegration
  (never silent). Intended for backup followers and web fleets.

### Staleness, precisely

Two invariants define what "stale" can and cannot mean here:

1. **Cache staleness ≡ replica staleness.** Every kernel cache (entries,
   attrs, negative entries, pages — §12) is invalidated in the same step
   that applies a remote log record to the replica, so caching never adds
   staleness beyond log-propagation lag. (Contrast NFS-style TTL caches,
   which stay stale even after the server knows better.) And because
   records apply in log order, a node's view is always a consistent
   *prefix* of the authoritative history — "the world as of txid N,"
   never a mix.
2. **Reads may be stale; writes never act on stale state.** Any conflicting
   write needs the partition lease, and acquiring/holding it requires having
   applied the previous holder's flushed log (§4). A write based on an
   outdated view is therefore impossible: it serializes after the change it
   didn't see and fails cleanly (e.g. ENOENT), rather than conflicting.

Worked example: node A deletes a file at t=0; node B `stat()`s it 1 ns
later and still sees it. Correct: no signal from A has reached B, so there
is no happens-before edge — serializing B's read before A's delete is a
legal ordering (nothing short of paying a round trip per stat could do
better; that is strict mode). The case that must work — A deletes, *then
tells B out of band*, then B looks — does: A's "done" is meaningful after
`close()`/flush, and B applies the record via gossip-pushed tailing
(milliseconds on a LAN). If B instead tries to *write* over the deleted
path, invariant 2 forces its op after the delete regardless of what it had
seen.

## 7. Caching and Data Movement

- **LRU chunk cache**: `cache.path` + `cache.max_size`; chunk states
  `clean` (evictable, LRU by DB-tracked atime), `pinned` (never evicted),
  `dirty` (never evicted until uploaded). Pressure order: evict clean →
  throttle writes → ENOSPC. Cached chunks are stored decompressed by default
  (config knob) for pread-fast reads.
- **`constellation pin <path>` / `unpin`**: fully cache a subtree,
  non-evictable, **eagerly push-synced**: flushed records propagate via
  gossip, pinned nodes fetch new chunks immediately from the best source (LAN
  peer preferred) in parallel with the S3 upload. Admission check up front
  (pinned set must fit the budget). `unpin` demotes to evictable.
- **Prefetcher** (after mountpoint-s3): per-handle sequential detection,
  adaptive readahead 1 → ~16 chunks in flight, reset on seek; random reads
  fetch only the needed chunk (ranged within it if partial). Pins reuse the
  prefetcher at full parallelism.
- **Cooperative cache**: nodes gossip bloom-filter digests (+deltas) of their
  cached chunk sets (~10 bits/entry, ~1% FPR). A local miss checks peer
  digests *locally* — zero per-request messages — then fetches from the
  best source. Chunks are self-verifying (hash), so peer serving needs no
  trust or invalidation. Rendezvous hashing is a composable alternative
  policy.
  A cache may be a thin slice of the dataset or the whole of it: a node
  is free to dedicate one or more full local drives, so 1–4 TiB is an
  ordinary size. Each node sizes and evicts independently; nothing
  assumes peers have equal cache budgets. The digest is advisory (a peer
  may have evicted since advertising, or not yet advertised a new chunk);
  S3 remains the source of truth.
  A single bloom is capped at 16 KiB so it fits one gossip frame
  (~13k chunks). Larger caches are split by hash prefix into as many
  buckets as that node's own size needs (128 for 4 TiB at 4 MiB chunks).
  Receivers store each peer's buckets separately, use *that* peer's
  bucket count on lookup, and cap what they will retain (~4 MiB/peer)
  so the largest cache cannot dictate everyone else's memory. Full
  snapshots rotate one hash-prefix bucket per interval (~5 kbit/s for 4 TiB);
  add-only deltas cover inserts between rotations. Membership changes
  are a journal on the cache (`take_digest_events`), not a full-set clone
  on every tick.
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
  stays and still blocks continuation epochs until retired.

  - **Self-leave** (`constellation leave --state-dir …`): refuse if a
    continuation epoch is open locally, or if this node holds a stranded
    deposed journal (`reintegrate` first). Refuse live offline
    designations unless `--force` (courtesy; force never skips the epoch
    or stranded-journal checks). Then flush the journal, release every
    partition lease, write the tombstone, persist `left=1` in the local
    state dir, stop writing, and unmount. Remount of that state dir fails
    until the operator uses a **fresh** `--state-dir` (new id).
  - **Admin leave** (`leave --state-dir <live-peer> --node-id N`): a
    still-mounted peer with bucket write tombstones another member. It
    does not flush that node's journal. Refuse if `N` is the calling
    node, or if `N` currently holds a live lease or unreleased
    designation, unless `--force`. A still-running target that sees its
    own record vanished or retired must stop writing (treat as
    deposition).
  - **Rejoin** is new enrollment: mount with a new state dir (or delete
    the old one) and claim a fresh id. Optional `--rejoin` sugar on a
    spent state dir is not required.

  Peers refresh `nodes/` periodically (~5 s); after a leave, survivors
  observe the smaller write-eligible roster without remounting.
- **Discovery unpublished by default**: peers learn addresses from the
  registry, so a leaked NodeId is not dialable by outsiders.
- **Node key**: Ed25519 at `~/.config/constellation/node.key` (0600),
  generated by `host init`; identity + signing only, never an access
  credential.
- **Encryption at rest**: default = S3 SSE + TLS (provider trusted;
  credentials alone suffice to mount). Optional **E2E passphrase mode** (per
  filesystem, at `fs create`): per-partition XChaCha20-Poly1305 DEKs wrapped
  in `keys/keyring` by an argon2id passphrase-derived KEK; mounting needs
  credentials + passphrase; unwrapped DEKs live in mlock'd RAM only.
- **Keyed addressing in E2E mode**: plain plaintext hashes as object keys
  would let the provider hash a known file and test for its chunks in the
  bucket (confirmation-of-file, the convergent-encryption leak). E2E
  filesystems therefore address chunks with **keyed blake3**
  (`blake3::keyed_hash`) under a per-filesystem secret from the keyring.
  Dedup still works within the filesystem (same key everywhere), the
  provider learns nothing from object names, and the choice is fixed at
  `fs create` — a per-FS setting, never a migration.
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

| failure | behavior |
|---|---|
| node crash | journal + epoch promises replay from local DB; dirty chunks re-upload; leases re-acquired or expire naturally |
| S3 outage | reads from cache; writes continue under held leases/epoch; flush resumes on return |
| lease holder vanishes | takeover after applying its flushed log (fenced by epoch counter); bounded unflushed tail surfaces via reintegration rules |
| P2P down, S3 up | everything works, minus the fast path (higher latencies) |
| clock skew | TTLs measured with margins; correctness relies on CAS ordering, never wall clocks |
| cache disk full | evict clean → throttle → ENOSPC (reads still stream uncached) |
| DB disk full | read-only mode + flush-to-recover; alarms in status/UI |
| corrupted chunk (local or S3) | hash verification on every read; local → refetch, S3 → error + fsck report (peer copies may heal) |

### What an S3 outage blocks

A node cannot distinguish "S3 is down globally" from "I am partitioned off,"
so the rules below are safe in both cases. Principle: **reads never block;
writes remain allowed exactly as far as pre-existing authority reaches, and
new authority cannot be created.**

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
- Visibility of new commits from nodes with no P2P path to us.
- Enrollment, leave, GC, checkpoints, snapshot create/delete — deferred or
  rejected cleanly when S3 is down; none load-bearing. Leave never
  auto-fires on unmount or a missed heartbeat.

Degrades on a timer — writes under a held lease:

- The holder keeps writing at local speed (journal locally, flush later),
  but lease *renewal* is also an S3 CAS. At TTL expiry the subtree goes
  **read-only**: if the outage is really just this node's partition, a node
  with working S3 can legally take the expired lease (ADR-12: safety never
  depends on failure detection).

Escape hatches (both are pre-arranged, non-stealable authority):

- **Offline designation** (§5.2): no TTL-steal on the designee's claim — it
  writes through any outage, indefinitely. This is exactly what
  `offline <path>` buys over a plain lease.
- **Continuation epoch** (§5.3): all write-eligible nodes in one P2P
  component keep writing collectively; nobody outside the component can take
  a lease without violating a persisted promise.

Three-machine example: home internet dies → desktop + laptop keep writing
(epoch). Laptop alone with no S3 → read-only after TTL, unless its paths
were marked `offline`. Laptop remote *with* S3 → it writes normally; the
nodes that lost S3 follow the rules above.

Cost bound: during an outage the journal and dirty chunks accumulate on
local disk, so outage duration is bounded by cache/DB space — then the
evict → throttle → ENOSPC ladder above applies. The spool is fully
observable: `constellation status --spool` and the web UI dashboard show
outstanding unflushed state — journal records and dirty-chunk count/bytes
per partition, oldest unflushed op age, current flush lag vs the bounded-lag
target, spool growth rate, and estimated headroom (time-to-full at the
current rate) — with warning thresholds surfaced as alerts and exported via
`/metrics`, so a user can see spooling build up long before the throttle
engages.

## 10. Control Plane

One control API (unix socket default, optional TCP) serves the CLI, the
embedded web UI (axum + rust-embed, binds 127.0.0.1; remote via opt-in auth
or the iroh tunnel), and scripts. Surfaces: dashboard (health, sync lag,
spool backlog + headroom, see §9), peers (RTT, direct/relay, registry), file
browser (rename/move/delete/up/download, inspect, history), cache (states,
hit rates, prefetch efficiency, pins), leases/designations (+ admin
force-release), snapshots/clones (create, browse, delete, mounts),
compression settings, ops (log tail, fsck, doctor), Prometheus `/metrics`.

CLI highlights: `fs create|mount|umount`, `mount [SOURCE[@snap]] MOUNTPOINT`,
`leave [--node-id N] [--force]`, `pin|unpin`, `offline|online`,
`snapshot create|ls|delete|diff`, `clone <path@snap> <dest>`,
`compression set|get`, `status [--spool]` (includes `node_id` /
`enrolled`), `inspect <path>`, `cache ls|stat|evict|verify`,
`gc run|verify`, `log tail`, `fsck [--repair]`, `doctor`, `host init`.

## 11. Scale Targets

Designed/tested for 1–10M files, 1–5 TB, 3–10 nodes. Reference census
(11.9M entries, 1.3 TiB): SQLite replica ~2 GB (79 B/row measured),
checkpoint ~1 GB pre-zstd, cooperative-cache digest ~12 MB. The partitioned log format
and typed manifest entries are the two guarantees that growing to 100M+
files (partial replicas, leveled compaction, packing) never requires an
on-bucket format migration.

## 12. Metadata-DB and FUSE Fast Paths

FUSE's aggressive caching and notification knobs are normally crippled by
one problem: the daemon can't know what changed remotely. The log-replica
architecture *always* knows — every remote change arrives as a log record —
which unlocks the aggressive end of every knob below. Complemented by what
an indexed, transactional replica answers directly.

### Enabled by the SQLite replica

- **Instant search** (`locate`/Everything-style): indexes on name, size,
  mtime, kind answer `constellation find` in milliseconds over 10M files
  with zero I/O — a complete, always-current replica makes results
  authoritative, something no scanning network FS can offer.
- **Change journal**: the metadata log already is a USN-journal equivalent.
  `constellation changes --since <txid>` (CLI + API stream) gives backup
  tools, indexers, and build systems exact deltas — no tree scans, ever.
  Zero extra cost; the log exists.
- **O(1) recursive aggregates**: per-directory recursive size/count columns
  maintained transactionally during log application. `du -sh` on any subtree
  is one row read (exposed via `user.constellation.rsize` xattr and the UI);
  pin admission checks become one indexed query instead of a walk.
- **Bulk namespace ops as transactions**: recursive delete/chown/chmod run
  as one SQL transaction emitting one compact log record
  (`unlink_subtree`, §4) — `rm -rf` on a million files in milliseconds,
  syncing to other nodes as a single record.
- **Stable readdir cursors**: DB cursors give exact resumption offsets for
  arbitrarily large directories, avoiding FUSE's classic skipped/duplicated
  readdir entries.
- **Time travel**: checkpoint + log replay into a temp DB = read-only mount
  of the tree as of txid T; content addressing keeps old chunks alive until
  GC. Named, GC-protected freezes of this are **snapshots** (§13).

### Enabled by the FUSE API

- **`READDIRPLUS`**: attributes returned with each dentry from a single
  `dentry JOIN inode` query — kills the stat-storm that slows `ls -la`,
  `find`, and rsync on every network FS.
- **Long kernel TTLs + push invalidation**: entry/attr timeouts in minutes
  instead of seconds, with `notify_inval_entry`/`notify_inval_inode` called
  precisely when a remote log record applies. Metadata hot paths run
  in-kernel; correctness by push, not polling.
- **Negative dentry caching**: the replica is authoritative, so ENOENT is a
  certain answer and safely kernel-cached — compilers, linkers, and `$PATH`
  searches issue huge volumes of failed lookups most network FSs can't
  cache. Invalidated by the `create`/`rename` record applying (the TTL is
  only a backstop); consistency argument in §6 "Staleness, precisely".
- **`copy_file_range` / reflink as pure metadata**: content addressing makes
  every copy a manifest copy (`copy_manifest`, §4) with chunks shared
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

One node at a time, holding a **GC lease** (`leases/_gc.json`) — the same
CAS + TTL + fencing-epoch machinery as subtree leases, reused unchanged.
Any member is eligible; scheduling prefers always-on nodes (configurable
weight; the backup server is the natural home), with `constellation gc run
[--orphans]` for manual triggers. Coordination-wise, GC is just another
lease holder.

### When (three cadences, cheapest first)

- **Continuous, every node**: **deref tracking** — while applying log
  records, each replica records the txid at
  which a chunk's last reference disappeared. Free, and it makes the GC
  candidate set a local query: no bucket LIST for reference GC.
- **Periodic (default daily), under the GC lease**: the **reference
  sweep** — delete chunks whose last dereference is older than
  `gc.horizon` (default days), minus the exemptions below. Also collected:
  log segments older than the newest checkpoint (with a retention floor)
  and superseded checkpoints.
- **Rare / on-demand**: the **orphan sweep**, the only LIST-based pass —
  bucket keys vs. the replica's known-chunk set, catching uploads from
  crashed or abandoned writes that never committed a manifest. Expensive at
  10M+ keys; weekly/monthly or explicit.

### The dedup race, and the layered defense

The dangerous race is not delete-vs-read but **delete-vs-dedup**: a writer
may commit a manifest referencing an existing chunk *without uploading it*
(dedup hit) — and an offline designee may reference a chunk by hash without
even holding its bytes, flushing days later. Deleting such a chunk between
check and commit would create a committed reference to nothing. Defense in
depth:

1. **Horizon**: only chunks unreferenced for longer than `gc.horizon` are
   candidates — one number kills every short-window race (flush lag, crash
   windows, epoch flushes).
2. **Condemned-list handshake**: before deleting, the GC holder publishes
   the condemned set (CAS'd object + gossip broadcast) and waits one full
   lease TTL. Freshness rides on lease renewal — every active writer
   re-reads the condemned pointer at TTL/2 renewal anyway — and writers
   treat condemned chunks as *absent* for dedup: they re-upload instead of
   referencing (an identical-content PUT is idempotent and resurrects the
   chunk). A writer that hasn't renewed is, by definition, no longer
   authoritative and cannot commit.
3. **Offline exemption**: chunks whose last reference lay under a subtree
   with an active offline designation are exempt until that designation
   reintegrates — checkable locally, since designations are log records.
4. **Open-handle holds**: chunks of orphaned-but-open inodes are exempt
   while any node's TTL'd `holds/` record covers them (§3, unlink while
   open); expired holds release the exemption automatically. The mark
   phase reads all `holds/*` from the bucket (one LIST of a
   usually-empty prefix + node-count tiny GETs) — the bucket record is
   the authoritative claim, gossip only a freshness hint — so the sweep
   honors holds regardless of which node runs it, including the node
   that performed the unlink.
5. **Reintegration verification** (backstop): a flushing node HEADs any
   dedup-referenced chunk it never held bytes for and re-uploads from local
   data or raises a visible, fsck-reported error — never a silent dangle.

Snapshot trees and clones are GC roots (§13); the grace behavior for
deleted snapshots is defined there.

### Falling behind segment GC

A node offline longer than log retention cannot tail its gap; it rebuilds
its replica from the latest checkpoint instead (checkpoints are full
state). A documented, tested recovery path, not an error.

### Auditability

The GC holder journals every deletion with its evidence (rule applied,
deref txid, condemned-set epoch) to a GC journal in the bucket;
`fsck` cross-checks deletions post-hoc, and `gc verify` runs the mark phase
without sweeping to report what *would* be collected and why.

