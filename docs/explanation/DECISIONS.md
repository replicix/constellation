# Constellation Decisions (ADRs)

Each record: decision, alternatives rejected, and why. Context in
[DESIGN.md](DESIGN.md). New decisions (or changed minds) are appended here.

## ADR-1: Greenfield in Rust

**Decision**: build new, in Rust, as one static binary.
**Rejected**: forking JuiceFS (Go) — its metadata layer assumes an always-on
service; retrofitting offline-first would fight the architecture. Composing
existing tools (JuiceFS + sync glue) — cannot deliver conflict prevention.
Go — weaker fit: no iroh equivalent (libp2p is heavier, ~70% hole-punch vs
~90%+), and the FUSE/S3/embedded-DB crates in Rust are excellent.

## ADR-2: Metadata plane = S3 CAS + local replicas + leases, P2P as fast path

**Decision**: S3 conditional writes are the only linearizable arbiter; every
node has a full local metadata replica; subtree leases give holders
local-speed writes; iroh accelerates forwarded mutations,
handoff/invalidation, and segment delivery but is never required for
correctness. An empty `MutateReply`, refusal, timeout, or unreachable holder
makes the requester use the S3-backed lease path.
**Rejected**: central metadata service — load-bearing extra infrastructure;
if it's unreachable nothing can lock even though S3 is fine; and the offline
machinery is needed anyway, making the service redundant. Pure gossip/CRDT —
CRDTs cannot express locks (locks are consensus); merge-based systems produce
exactly the syncthing conflicts this project exists to prevent. Raft among
nodes — a 2-of-3 quorum cannot distinguish "laptop offline" from "partition",
and a minority node with S3 access must not be fenced out of reads.

## ADR-3: Content-addressed chunks, not 1:1 path mirror

**Decision**: fixed-size chunks (4 MiB default; per-FS setting, recorded
per file in its manifest so per-path overrides stay possible without
migration) keyed by plaintext blake3; namespace lives in
metadata.
**Rejected**: 1:1 object-per-path (s3fs/rclone/mountpoint style) — renames
become O(size) copies (explicit goal violation), no dedup, no atomic
multi-file ops, multi-writer correctness much harder.
**Consequence accepted**: the bucket is opaque; access control is at
bucket/prefix granularity via IAM credentials only — per-subtree IAM
scoping is not supported (ADR-7).

## ADR-4: Consistency default = close-to-open; strict and relaxed opt-ins

**Rejected**: strict-everywhere (every op pays RTTs; nobody needs it by
default) and relaxed-default (reintroduces conflicts; violates "correctness
first, user opts out").

## ADR-5: Scale envelope 1–10M files with a format that reaches 100M+

**Decision**: implement/test for 1–10M files, 1–5 TB, 3–10 nodes; partition
the log per subtree (automatic, invisible) and type all manifest entries so
larger scale needs no bucket-format migration.
**Rejected**: all-in-RAM small design (user census already at 10M files);
full 100M+ design now (partial replicas, leveled compaction ≈ 2–3× surface
for problems we don't have).

## ADR-6: Offline designation, not exclusive checkout

**Decision**: `offline <path>` names a *designee* that stays writable when
disconnected; while the designee is reachable everyone writes (foreign
flushes ack through the designee); designation is not a lock.
**Rejected**: checkout-as-lock (original sketch) — blocks other nodes even
while the designee is online, which the use case explicitly does not want.
The ack-through-designee protocol is what makes "everyone writes until the
designee leaves" conflict-free.

## ADR-7: Access control = bucket IAM; no internal user management

**Decision**: bucket credentials are the only identity; no user DB, ACL
mapping, share tokens, or gateway. uid/gid/mode are plain attributes
(NFS-style). Node registry is self-enrollment gated by bucket write.
**Rejected**: internal users + wrapped-key sharing + gateway (earlier
design) — a second identity system to administer; the user chose IAM.
**Rejected**: materialized `exports/` subtree shares (plain-object copies
under an IAM-scopable prefix) — duplicates data, complicates GC, and
presign/ expiry semantics do not fit the content-addressed model well
enough to be worth building.
**Consequence accepted**: no cryptographic isolation between holders of the
same bucket credentials; permissions are cooperative.

## ADR-8: Encryption default = SSE; E2E is a passphrase mode

**Decision**: by default the provider is trusted (SSE + TLS) so credentials
alone fully mount; optional per-FS E2E mode (argon2id KEK → wrapped DEKs in
the bucket keyring) for zero-trust storage.
**Rejected**: E2E-by-default (earlier choice) — incompatible with
"credentials alone = full use" (ADR-7); node-key-wrapped keyrings — an
open self-enrollment registry makes per-node wrapping meaningless.

## ADR-9: Local metadata store = SQLite behind an engine trait

**Decision**: SQLite (WITHOUT ROWID tables, WAL). Benchmarked at census scale
(20.2M records, desktop machine): 1.60 GB (79 B/row), 578K dentry lookups/s,
521K readdir scans/s — DB latency ≈ 2 µs vs ~10 µs FUSE overhead; plus SQL
tooling for fsck/inspect and single-file backup. Engine trait keeps LMDB
(787K/s lookups, 2.5 GB, mmap RSS) as a drop-in read-optimized alternative.
**Rejected**: redb (7× slower load, 4.3 GB, 59K/s lookups at this scale);
fjall/LSM (most compact at 0.66 GB and fastest load, but 4K/s untuned point
reads disqualify it for a lookup-heavy FS); RocksDB (C++ dependency and
tuning burden; fjall represented the LSM class). Bench: `bench/dbbench`.

## ADR-10: Chunk identity = uncompressed plaintext hash

**Decision**: compression (ZFS-style inheritable attribute, zstd all levels,
pluggable codec registry, self-describing object headers) and encryption are
storage encodings, invisible to identity.
**Rejected**: hashing stored bytes — would break dedup across settings
changes, force recompression on attribute change, and make peer serving
depend on encoding agreement.

## ADR-11: Format reservations instead of features

**Decision**: known future mechanisms are *reserved manifest entry types*
(`pack`, `cdc`, `slice-overlay`, inline-manifest prefixes), deliberately
unimplemented. Reserving costs bytes; retrofitting costs migrations. Each
can later coexist with existing data forever (e.g. packed and loose chunks
side by side) — enabling one is a code change, never a repack of history.

**Packfiles** (`pack` = pack hash + offset + length): the tiny-object fix.
The census maps ~10M mostly-small files to ~10M S3 objects of a few KB
median — each paying full per-request latency and cost (a cold directory of
1,000 small files = 1,000 GETs at full TTFB), making GC/fsck LISTs walk 10M+
keys, and interacting badly with storage classes that bill per-object
minimums (S3-IA counts every object as ≥128 KB). Bundling small chunks into
~64 MiB objects (git/borg/restic style) turns those into a handful of ranged
GETs, and packing locality becomes read locality for the prefetcher; dedup
is untouched (chunks stay individually addressed).
**Why deferred**: the write path needs batch/spool/seal, and — the real
cost — GC becomes *compaction*: rewriting packs as their chunks die, a whole
background subsystem with write-amplification tuning (where restic's `prune`
complexity lives). At 10M objects the pain is bounded; not worth it yet.

**CDC chunking** (`cdc`, content-defined boundaries via FastCDC-class
rolling hash, min/avg/max bounds): the content-shift fix. POSIX has no
byte-granular insert (`FALLOC_FL_INSERT_RANGE` is block-aligned only), so
"inserting" means the application rewrites the file with everything after
the edit shifted — a plain overwrite from the FS's view. With fixed 4 MiB
chunks, an in-place 1-byte overwrite of a 1 GB file syncs ~4 MiB, but a
rewrite containing a 1-byte shift changes every downstream boundary — all
256 chunks hash differently, ~1 GB uploads, dedup against the previous
version drops to zero. Content-anchored boundaries land in the same places
relative to the *content* and resynchronize a few chunks past the edit, so
the same rewrite uploads ~2–3 chunks (the local write I/O is identical
either way; CDC saves upload and storage, which is why backup tools that
only ever see whole new file versions rely on it).
**Why deferred**: it complicates offset→chunk mapping (cumulative-length
table + binary search instead of `offset / 4 MiB`), the RMW write path
(re-chunk the edit region), and sparse handling — and it only pays for
shift-heavy rewrites of large files (VM images, mbox, DB files), which the
reference corpus barely contains. If added, it becomes a per-path
inheritable attribute like compression, so only subtrees that need it pay.

The two compose (restic's architecture): CDC decides where chunks *end*,
packfiles decide how chunks are *stored* — hence two independent entry
types.

**Slice overlays** (`slice-overlay`): append small overwrites as
(offset, bytes) deltas on a base manifest instead of rewriting whole
chunks — a random-write amplification fix for the same large-mutable-file
workloads; same verdict.

## ADR-12: Safety never depends on failure detection

Heartbeats and RTT measurements drive UX and source selection only. Every
write requires an unexpired authority chain (lease TTL, delegation ack,
epoch promise) — correctness is preserved under arbitrary partitions,
including the all-writers-on-LAN continuation epoch (all-members rule, not
majority; see DESIGN.md §5.3).

## ADR-13: Optional iroh relays; default remains registry-direct

**Decision**: keep iroh `RelayMode::Disabled` as the Constellation default.
Peers dial addresses published in the S3 node registry (LAN/VPN topologies).
Operators may opt into n0 public relays or self-hosted relays via
`CONSTELLATION_P2P_RELAY` so nodes with no mutual L3 path (NAT, internet-only
EC2 private IPs vs off-VPN laptops) can still form the P2P fast path.

**Rejected**: enabling n0 relays by default — would send encrypted traffic
through third-party infrastructure without an explicit operator choice, and
most Constellation fleets already share a VPC/VPN where direct dialing works.
Global pkarr/DNS address publishing (iroh `N0` preset) — the registry remains
the only peer directory and trust root (DESIGN.md §8).

**Consequence**: all nodes that need to talk over relays must share the same
relay policy (same public map, or the same custom URL list + optional token).
Allowlist enrollment is unchanged: a relay only carries bytes between already
enrolled endpoints. A single self-hosted relay (with or without one shared
admission token for all tenants) is the same security shape as n0's public
relays — tenant isolation is registry/IAM/E2E crypto, not relay tokens.
Multiple `shared_token` values are an admission OR-list, not per-tenant
overlays; partition capacity with separate relay URLs when needed. Details:
[P2P relays — shared relays and multi-tenancy](../reference/features/p2p-relays.md#shared-relays-and-multi-tenancy).

## ADR-14: Forward mutations to the lease holder instead of moving the lease

**Decision**: a node without the partition lease sends its mutation to the
holder. The holder validates and journals the operation as the sole
sequencer, then returns the accepted records for the requester to shadow
locally. S3 lease CAS, epochs, and log fencing remain the authority.

**Rejected**: handoff-only operation — alternating writers move the lease on
every burst and pay flush plus CAS latency. Multi-appender or CRDT metadata —
either weakens the single ordered history or requires conflict semantics the
filesystem is designed to avoid. Per-node partitions — path placement would
leak into the namespace and cross-node operations would become distributed
transactions.

## ADR-15: Holder-driven placement, no election

**Decision**: the current holder computes
`cost(candidate) = sum(ops_writer * RTT(candidate, writer))` over recent
writers and offers the lease to the lowest-cost direct-path writer when the
improvement passes hysteresis and dwell limits. The holder already owns the
right to sequence the transition, so no election protocol is needed.

**Rejected**: CPU load or S3 distance as placement inputs — forwarding cost is
the latency between writers and sequencer; S3 shipping remains asynchronous,
and CPU is not currently the limiting signal. A distributed election adds
failure and tie-breaking states without adding authority.

## ADR-16: Scratch directories are explicit and node-private

**Decision**: only directories marked `user.constellation.scratch=1` contain
node-private entries. Their local create/write/unlink operations produce no
shared metadata. Renaming a regular file out is the explicit Publish boundary;
unsupported boundary crossings fail rather than partially sharing state.

**Rejected**: implicit deferred-create based on filename or write pattern —
applications and peers could not tell whether a path was shared, crash
recovery would have to infer intent, and close or rename could unexpectedly
publish temporary files.

## ADR-17: Segment payload push is an accelerator; S3 remains the source

**Decision**: `SegmentPublished` includes compressed segment bytes when they
fit the gossip budget. Receivers may apply the payload immediately, while the
same segment is still committed and recoverable from S3.

**Rejected**: treating gossip delivery as the commit or only copy — offline
and P2P-disabled nodes would lose history, retries would need a new durable
protocol, and ADR-2's single S3 authority would no longer hold.
