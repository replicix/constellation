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

**Amended by plan 30**: close-to-open now comes in two modes per mount,
`--cto bounded` (the default) and `--cto strict` (ADR-20). Cross-node
`flock`/`fcntl` locks are on by default whenever P2P is on (ADR-25),
rather than being part of an opt-in strict mode. The relaxed mode has
not been built.

## ADR-5: Scale envelope 1–10M files with a format that reaches 100M+

**Decision**: implement/test for 1–10M files, 1–5 TB, 3–10 nodes; partition
the log per subtree (automatic, invisible) and type all manifest entries so
larger scale needs no bucket-format migration.
**Rejected**: all-in-RAM small design (user census already at 10M files);
full 100M+ design now (partial replicas, leveled compaction ≈ 2–3× surface
for problems we don't have).

**Amended by plans 29 and 30**: plan 29 M0a removed the per-subtree log
partitions (automatic split and merge, cross-partition renames); there is
one log. Plan 30 scales writes out with delegated sub-sequencers over
that one log instead (ADR-23).

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

**Amended by plan 30**: heartbeats and silence now also drive liveness
decisions: a backup seals and takes over after holder silence (ADR-21),
and with `epoch_slack > 0` a TTL takeover needs heartbeat promises
(ADR-22). Neither makes safety depend on detection: a seal, a log-slot
CAS or an unexpired promise is what fences, and a false suspicion costs
only availability. The all-members rule becomes `N − f` members with
promises (ADR-22).

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

**Amended by plan 30**: forwarding is exactly-once (ADR-18), the
requester's shadow is recoverable speculation (ADR-19), what an
acknowledgement means depends on the durability layer (ADR-21), a
requester with no P2P path forwards through the S3 inbox (ADR-24), and
a forward goes to the owning sequencer, which may be a delegate
(ADR-23).

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

**Amended by plan 30**: this still places the root lease. Placement of
subtrees on their dominant writers, by the root, is ADR-23.

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

**Superseded by ADR-20 (plan 30 M7)**: `SegmentPublished` gossip no longer
carries segment bytes; it is a hint to tail now. Followers receive the
log over direct streams from the holder (`LogSubscribe`), and S3 remains
the source. The rejection above still holds.

---

The ADRs below come from plan 30 ([write-path resilience and
scale-out](../plans/v1/done/30-write-path-resilience-and-scale-out.md)).
Each gives the context, the decision, its consequences, the alternatives
rejected, and where it lives. The plan's §2 hard constraints (portable
S3 only, one bucket, single-node degradation, no WAN round trip on every
write, safety independent of failure detection) bind all of them.

## ADR-18: Exactly-once forwarding with request ids

**Context**: before plan 30, a forwarded mutation whose reply timed out
was re-executed through the lease path. If the holder had executed it,
the second execution saw its own effect: `O_EXCL` create, `mkdir` and
`link` returned `EEXIST`, and `unlink`, `rmdir` and `rename` returned
`ENOENT` (plan 30 bug A). Lock-file protocols such as git's `index.lock`
failed and left stale locks behind. A slow holder (suspend, I/O stall,
WAN jitter) was enough to trigger it.

**Decision**: apply RIFL (SOSP'15) to forwarding.
- Every `MutateOp` gets a request id `Rid { node, incarnation, seq }` at
  the top of `mutate_op_rebasable` and keeps it on every path: local,
  forwarded, retried, replayed or sent through the inbox.
  `incarnation` is persisted node-locally and bumped at every mount
  before serving.
- Executing an op appends `LogRecord::Completed { rid }` in the same
  transaction as the op's records, and a shipped segment never splits a
  transaction. Replay records the rid's outcome in a node-local
  `completed` keyspace on every replica.
- The sequencer answers a rid it already executed from an in-memory map
  of recent outcomes or from `completed`, and never executes it again.
- A timeout, transport error or `Busy` leaves the op *in doubt*. The
  requester retries the same rid: the same holder (three attempts with
  backoff), then a redirected holder, then the lease path. After tailing
  to head, the lease path looks the rid up in `completed`. Only an
  explicit refusal ends an op without executing it.
- Coverage rule: an in-doubt op may be resolved only against a
  `completed` table that has seen every segment since the op was first
  sent. Log GC keeps every segment younger than
  `CONSTELLATION_COMPLETION_RETENTION_S` (default 900 s), whatever the
  head commit says. The client deadline (twice the lease TTL) keeps an
  in-doubt op far inside that window; an op still in doubt at its
  deadline fails with `EIO`, never with a re-execution.

**Consequences**: every mutation carries a rid, and every executed op
adds one small `Completed` record to its segment. A definitive refusal
is an outcome too: it is journaled as `Refused { rid, errno }` (plan 30
M13 for the inbox, M9 for P2P forwards and delegates), so a second
execution of the rid answers the same errno instead of re-evaluating
the op against a state that may have changed. A transient refusal (a
stale manifest base) is not recorded; the requester rebases and retries
under a new rid. The same identity makes stranded-op replay (ADR-19),
backup re-shipping (ADR-21), inbox drains (ADR-24) and delegate replay
(ADR-23) exactly-once without further machinery.

**Rejected**: a longer forward timeout (it narrows the window but cannot
close it, and makes a dead holder cost more). Idempotent op rewriting,
such as turning a retried `O_EXCL` create into "succeed if it exists"
(wrong when another client created the name in between). Holder-side
dedup without durable completions (a takeover loses the dedup state,
which is exactly when retries happen).

**See**: plan 30 §M2; [Forwarded
mutations](../reference/features/forwarded-mutations.md#exactly-once-identity-and-in-doubt-handling);
[`crates/meta/src/rid.rs`](../../crates/meta/src/rid.rs),
[`crates/meta/src/record.rs`](../../crates/meta/src/record.rs),
[`crates/authority/src/core/client.rs`](../../crates/authority/src/core/client.rs),
[`crates/authority/src/core/holder.rs`](../../crates/authority/src/core/holder.rs);
model `crates/model/tests/today_bugs.rs`.

## ADR-19: The replica is a log prefix plus explicit speculation

**Context**: a requester applied an accepted forwarded op to its replica
at once, and a holder executed ahead of shipping. If the holder died
before shipping and another node took over, those effects never reached
the log but stayed in replicas for good (plan 30 bug B). A requester
could publish them in a commit that a fresh node then bootstrapped from.
A requester that became the next holder validated new ops against the
phantom state and answered `EEXIST` for a name that never existed.

**Decision**: every effect a node applies ahead of the durable log is
*speculation*, captured with its before-images so it can be taken back.
- The one funnel every `ns` write goes through records the before-image
  of each key (and the usage delta) in a node-local `spec` keyspace, in
  the same transaction. Kinds: a requester's shadow, an `Exists` hint, a
  holder's unshipped transaction (`Local`), and, while older speculation
  is outstanding, a tailed segment kept as redo material. Pre-S3
  streamed records (ADR-21) and delegate transactions (ADR-23) are
  speculation too.
- Speculation retires when the log confirms it: a shadow on its
  `Completed { rid }`, a hint when the applied position passes its
  floor, a local transaction when it ships.
- A segment from a later epoch *strands* older-epoch speculation, and a
  deposition strands a holder's unshipped journal. Recovery rolls back
  to the earliest stranded entry, redoes what still stands, and replays
  the stranded ops **by rid** through the current sequencer. ADR-18
  makes the replay exactly-once. A replay the log no longer admits is a
  genuine conflict, materialized as a `.constellation-conflict/` copy.
- A takeover gate keeps a new holder's view closed until it has shipped
  an epoch marker (which fences the old epoch in the log) and executed
  its queued replays. A new holder never validates against phantom
  state.
- Commits are log prefixes. Only the lease holder publishes (plan 30
  M4), and it substitutes the earliest before-image for every key its
  unshipped journal touched. A node with outstanding shadows or hints
  does not publish.

**Consequences**: deposed-holder reintegration no longer classifies a
stranded branch against a side replica: it is rollback plus replay by
rid, and only true overlaps become conflict copies. Holder capture costs
a before-image write per journaled transaction. It passed the milestone's
10% performance gate and stays on. `CONSTELLATION_HOLDER_CAPTURE=0` is
the internal fallback, which rebuilds a deposed holder from the shared
log instead. The Stateright model's `commits_are_log_prefixes` and
`converged_at_quiescence` properties hold under the `Recovery` variant.

**Rejected**: rebuilding the replica from the head commit after every
takeover (correct but O(namespace) per failover, and a requester would
still lose its own acknowledged writes). Deferring a requester's shadow
until the segment arrives (read-your-writes then waits on S3 shipping).
Recording speculation outside the funnel (every new write path would be
a new place to forget it).

**See**: plan 30 §M3, §M4;
[Forwarded mutations — speculation and stranded-op recovery](../reference/features/forwarded-mutations.md#speculation-and-stranded-op-recovery);
[`crates/meta/src/store/spec.rs`](../../crates/meta/src/store/spec.rs),
[`crates/meta/src/store/ns.rs`](../../crates/meta/src/store/ns.rs),
[`crates/authority/src/core/replay.rs`](../../crates/authority/src/core/replay.rs);
model `crates/model/tests/holder_side.rs`.

## ADR-20: Positions, session guarantees, and two close-to-open modes

**Context**: reads had no freshness barrier. `open()` did no
revalidation, so close-to-open was really bounded staleness, and every
session anomaly (a node not seeing its own refused create's winner, for
example) needed its own point fix. Cross-node visibility after a write
burst measured p50 16 s and p99 21 s on EC2 (plan 30 M7 later found
that this run largely measured the benchmark, whose pollers started
only after the writer finished; the M16 EC2 campaigns re-measured it
with concurrent pollers at p50 28–34 ms on AWS and about 210 ms on OVH,
see [RESULTS.md](../../bench/remote/RESULTS.md#plan-30-real-s3-results)).
Follower delivery rode
gossip payloads (ADR-17), which were capped in size and fell back to S3
for anything larger.

**Decision**:
- **Positions.** Every mutation reply carries the position it was
  evaluated at: the log sequence, plus the unshipped journal position
  `(epoch, jseq)` when it had one, plus per-delegation stream positions
  (ADR-23). A node keeps an `observed` watermark of positions whose
  effects it has not installed locally (a refusal, an `Exists` without a
  hint, an op that waited for the log).
- **Session guarantees.** Every FUSE read (`lookup`, `getattr`,
  `readlink`, `open`, the first `readdir` chunk, `getxattr`,
  `listxattr`) waits until the replica reaches `observed`, unless
  speculation already covers the keys it reads. The wait is bounded by
  `CONSTELLATION_SESSION_WAIT_MS` (2 s); on timeout the read answers from
  the replica and is counted as degraded. This gives read-your-writes
  and monotonic reads per node.
- **Direct log streams.** The holder serves its shipped segments to
  subscribers over direct QUIC streams (`LogSubscribe`), applied through
  the tail path with fencing. Gossip carries only hints, membership and
  digests. A lost or slow stream falls back to S3 tailing. This
  supersedes ADR-17.
- **Two close-to-open modes**, per mount (`--cto`):
  - `bounded` (the default): an open reads the local replica, which
    follows the log within the visibility bound; the session guarantees
    still hold per node.
  - `strict`: an open, lookup or first listing on a node that is not the
    sequencer sees every close another node completed before it began.
    It asks the owning sequencer for a position (**ReadIndex**) and waits
    for its replica to reach it. The answer may carry a short **read
    delegation** on the inode, under which later opens are local until
    the sequencer recalls it. The sequencer recalls every delegation on
    what a mutation touched before it acknowledges the mutation.
    Delegations are capped by the lease and time-bounded with the
    lease's margin, so a partitioned reader loses them by itself.

**Consequences**: a node never answers a read from a state older than
one its client has already seen. Strict mode costs nothing on a single
node, about one round trip on a first open on a LAN, and one WAN round
trip for a write-then-open cycle across continents, the minimum strict
close-to-open allows at that distance. A writer whose file is delegated
to a silent reader waits up to the delegation's TTL plus the lease
margin (6 s at the defaults). With P2P off there is no ReadIndex: a
strict open tails S3 to head, so the sequencer's own unshipped writes
are visible only after its next ship. The default stays `bounded`,
decided from the EC2 measurements in ADR-30. The first
visibility root causes found were a ship blocked behind the whole upload
pass, an `fsync` drain behind the bulk upload queue, and the kernel's 1 s
cache TTL; all three are fixed.

**Rejected**: revalidating every `open()` against S3 (an S3 round trip
per open, and S3 is not where a sequencer's unshipped writes are).
Revalidating against a reader's own replica alone (a reader cannot know
what it has not received without asking the sequencer). Installing the sequencer's
inode record as a hint in the ReadIndex answer (a later segment of an
older write could regress it; a per-read position floor is used
instead). A relay tree for log streams (not needed at the tested scale;
the holder serves every subscriber and drops slow ones to S3).

**See**: plan 30 §M6–§M8;
[Close-to-open modes](../reference/features/cto-modes.md);
[`crates/meta/src/session.rs`](../../crates/meta/src/session.rs),
[`crates/meta/src/readdeleg.rs`](../../crates/meta/src/readdeleg.rs),
[`crates/authority/src/core/readindex.rs`](../../crates/authority/src/core/readindex.rs),
[`crates/authority/src/core/stream.rs`](../../crates/authority/src/core/stream.rs);
models `crates/model/src/positions.rs`, `crates/model/src/cto.rs`.

## ADR-21: Layered durability and seal-based failover

**Context**: an acknowledgement existed only on the holder until S3 had
the records. A holder that died before shipping stranded what it had
acknowledged (bug B, fixed by ADR-19 as replay), and every writer waited
out the lease TTL (30–60 s) before anyone could take over. The fix had
to respect the topology: a synchronous copy on a peer across an ocean
would put a WAN round trip on every write (plan 30 §2, constraint 4).

**Decision**: durability comes in layers, matched to the topology.
- **Layer A (always, no added latency).** Requesters keep what they
  were acknowledged and replay it by rid after a takeover (ADR-18,
  ADR-19). The lease's `ack_policy` is `Local`.
- **Layer B (only when a peer is within an RTT budget).** The holder
  picks up to `CONSTELLATION_BACKUPS` (1) backups among write-eligible
  peers whose measured RTT is within `CONSTELLATION_BACKUP_RTT_BUDGET_MS`
  (5 ms), preferring the one connected longest. It streams whole journal
  transactions to them and acknowledges only what every backup listed
  in the committed lease object holds (`ack_policy = Backup`). Appends
  are pipelined, which group-commits whatever is journaled meanwhile.
  A backup that stops acknowledging is removed by a lease CAS before
  the holder acknowledges anything further without it; a new backup is
  streamed the unshipped tail and CASed in once it has caught up.
- **Seal-based failover.** A backup that hears nothing from the holder
  for `CONSTELLATION_BACKUP_TAKEOVER_MS` (1.5 s) persists and fsyncs
  "epoch *e* sealed" and refuses further epoch-*e* appends. The old holder can then
  collect no write-all acknowledgement, so it can acknowledge nothing
  more. The backup CASes the lease to epoch *e*+1, tails S3 to head,
  re-ships its backup tail deduplicated by rid, and opens.
- **Layer C (opt-in): `ack=s3`.** `fs create --ack-policy s3` (its
  default is `CONSTELLATION_ACK`) acknowledges a mutation only once its
  segment is CAS-created in the log, group-committed per sync round.
  The policy is the filesystem's, for every mount and tenure (M16: a
  per-mount `--ack` could only apply to the tenures its own mount
  acquired, since an acknowledgement is the sequencer's; a `--ack s3`
  requester forwarding to a `local` holder got local acknowledgements.
  It was removed rather than honoured per request).
  Any peer may then take over a silent holder before the TTL: the next
  log slot's create-if-absent CAS fences the old holder. `--fsync-mode
  s3` is the older, per-`fsync` form of the same guarantee.
- **Observers.** Under `Backup` and `S3` the holder does not answer a
  read or an op from state that includes acknowledged-but-not-yet-durable
  effects of other clients, so no client ever observes an effect that a
  failover could roll back.
- **Pre-S3 streaming.** Backup-acknowledged transactions are streamed to
  log subscribers ahead of S3, as speculation (ADR-19), so visibility
  does not wait for the bucket.

**Consequences**: a single node and a cluster of distant peers behave as
before (Layer A, TTL failover). A cluster with a peer in budget adds one
LAN round trip per acknowledgement and fails over in about 1.5 s (as
measured on the test host) with nothing acknowledged lost. `ack=s3` adds
one S3 round trip per group commit and needs P2P for its fast takeover;
with P2P off it gives durability but TTL failover. Safety never depends
on the timeouts (ADR-12): the seal and the log-slot CAS are the fences.
A fast takeover cannot be gated by an epoch's promises, so a
continuation epoch never carries an `S3` lease, and carries a `Backup`
lease only when every listed backup is a member (ADR-22).

The durability contract (M16) is plan 30 §3's single-failure one.
"Persisted" means committed to the node's fjall store
(`PersistMode::Buffer`): it survives a crash of the process, and the
kernel writes it back within seconds, but a power loss or kernel crash
can drop the last commits. What *safety* rests on is synced before it
is acted on — a promise and the epoch join gate, the epoch's persisted
state, a seal, the read-grant horizon — so a power loss makes a node
forget work, never a promise. Backup appends are committed, not synced:
a `Backup` acknowledgement survives any failure of the holder (power
loss included, as long as a backup's OS keeps its copy until the
takeover re-ships it) and any failure of a backup (the holder still
has it), but not a power loss of the holder and every backup together.
That is a correlated failure, outside the contract; `ack=s3` covers it.
An fsync per append would put a disk flush on every Layer B
acknowledgement, the latency the layer exists to avoid. One backup
survives one failure.

**Rejected**: a synchronous backup regardless of distance (a WAN round
trip on every write). A majority quorum of replicas (Raft-style) inside
the cluster (it cannot tell a sleeping laptop from a partition, ADR-2,
and adds a second arbiter beside S3). Failover by failure detection
alone (a false suspicion would create two writers; the seal makes a
false suspicion cost only availability). A per-backup snapshot on
addition (streaming the unshipped tail is enough and needs no extra
format).

**See**: plan 30 §3 and §M9;
[Durability and failover](../reference/features/durability-and-failover.md);
[`crates/authority/src/core/backup.rs`](../../crates/authority/src/core/backup.rs),
[`crates/meta/src/store/backup.rs`](../../crates/meta/src/store/backup.rs),
[`crates/store-s3/src/lease.rs`](../../crates/store-s3/src/lease.rs)
(`AckPolicy`); model `crates/model/src/backup.rs`.

## ADR-22: Flexible-quorum continuation epochs with promises

**Context**: with one bucket (ADR-27), a continuation epoch is the only
way to keep writing through a bucket outage without an offline
designation. The rule was that the epoch must contain *every*
write-eligible node (DESIGN.md §5.3), because any missing node might
still reach S3 and take an expired lease. As the node count grows, an
epoch becomes impossible to form: one sleeping laptop blocks everyone.

**Decision**: a per-filesystem slack `f` (`epoch_slack`, set with
`fs create --epoch-slack` or `fs set epoch-slack`, default 0).
- An epoch needs `N − f` members of the write-eligible roster.
- A *promise* is a node's persisted word that it joins no epoch before
  `no_epoch_until` (its own clock). It is written to
  `heartbeat/<node>` after being persisted locally, and only on demand:
  when a would-be taker asks over P2P, when the node sees a lease expire
  unrenewed and nobody can ask it, or when its slack changes.
- A node joins an epoch only once its own last issued promise has
  expired, and publishes no promise while its epoch is open.
- An S3 takeover of an expired lease that another node held needs at
  least `f` *other* roster nodes whose promise outlasts the lease's
  recorded expiry. A taker's `f` promisers and an epoch's `N − f`
  members must share a node, whose promise would have to be both
  expired and still binding.
- The promise TTL is at most a quarter of the lease TTL, so members'
  promises run out well before the leases an epoch wants to carry.
- A taker honours the largest slack any roster node advertises, so a
  change of `f` is safe while it propagates.

**Consequences**: with `f = 0` nothing changes and no heartbeat object
is ever written; a TTL takeover costs one `heartbeat/` LIST. With
`f = 1` a three-node cluster keeps writing through a bucket outage with
one node missing, and the steady state writes no heartbeat at all.
The price: a TTL takeover by a node that reaches S3 now needs `f`
promises, so `f` close to `N` can block TTL failover while a crashed
holder stays away (`fs set` refuses `f ≥ N` and warns when
`f > N − 2`); an admin `leave --node-id` fences the retired node's
leases. Writes inside an epoch are acknowledged on the hold owner's disk
alone. A member that can still reach S3 declines to join. A node that
enrolls during an epoch is a known gap.

**Rejected**: majority epochs without promises (a minority node with S3
access could legally take an expired lease: two writers). A steady
heartbeat refresh (it costs PUTs forever for an event that may never
happen). A second bucket as a witness (ADR-27).

**Amendment (capture under an epoch hold)**: the hold owner's epoch
journal is speculation in ADR-19's sense — captured with before-images,
under the epoch its flush will ship under: the carried lease's epoch
for the carrier, the next epoch for a member that took the hold over
(what its flush CAS on the carried object grants; nothing else can
touch that object while the epoch is open). Since M5 the hold had set
the capture epoch to 0 and journaled uncaptured, so the ship plan after
the close, unable to tell what depended on a transaction deferred on a
member's chunk, deferred everything after it: one member away with the
only copy of a chunk stalled the whole cluster's log at its pre-epoch
head. Now the plan skips exactly that transaction and its dependents
(a refusal counts as depending on the keys it observed), the publisher
substitutes, a deposed hold owner rolls back and replays by rid, and a
member gone for good is an operator decision: `repair drop-held <ino>
--remote` drops the write into a conflict copy whose refusal ships —
members' streamed copies and the requester's shadow roll back on it.
Consequences on the rules above: a member's streamed copy of a
transaction the tenure shipped past without naming is rolled back
(it was dropped), never confirmed; a stream-ahead a member already holds
is not installed again; a handoff whose flush cannot drain the journal
is declined (the deferred rows would bounce between epochs); a round
that shipped nothing does not follow itself at once.

**See**: plan 30 §M10;
[Durability and failover — flexible continuation epochs](../reference/features/durability-and-failover.md#flexible-continuation-epochs);
[`crates/authority/src/core/promise.rs`](../../crates/authority/src/core/promise.rs),
[`crates/store-s3/src/heartbeat.rs`](../../crates/store-s3/src/heartbeat.rs),
[`crates/net/src/epoch.rs`](../../crates/net/src/epoch.rs);
model `crates/model/src/flex.rs`. Amends ADR-12's all-members rule.

## ADR-23: Delegated sub-sequencers over one log

**Context**: one lease and one sequencer for the whole namespace capped
throughput at one node's store writer, and every other writer paid a
round trip to it (a WAN round trip across continents). Only one node
ever ran at local speed. The earlier per-subtree log partitions (removed
in plan 29 M0a) had needed a linked two-record commit, with its own
recovery rule, for every rename across partitions.

**Decision**: keep one root lease in S3 and one log, and let the root
delegate subtrees to their dominant writers over P2P.
- `Delegate { dir, node, gen }` and `Recall { dir, gen }` records keep a
  replicated delegation table. Delegations never overlap, and there is
  no sub-delegation. A grant lasts `CONSTELLATION_DELEGATION_TTL_MS`
  (5 s), is renewed by the delegate, and is capped by the root's lease.
- A dentry belongs to the delegation containing its parent; an inode's
  keys belong to the delegation containing its primary link's parent.
  An op whose keys span two owners is cross-subtree.
- The delegate validates against its replica (authoritative for the
  subtree, since every write there goes through it), journals as
  speculation (ADR-19), acknowledges under its own durability layer (a
  backup chosen by RTT to the delegate, or Layer A), and streams its
  transactions in order to the root.
- The root appends a delegate's stream in order, after checking the
  generation, without re-validating. A forward carries `deps` (the
  delegate stream positions the requester has observed), and the root
  appends a transaction only once everything in its `deps` is in its
  replica, so no replica ever holds a record whose causes are missing.
- Cross-subtree ops (renames and hard links across delegations, `rmdir`
  or rename of a delegated root) go to the root, which **recalls** the
  involved delegations first: the delegate drains its stream and stops,
  or is outwaited by its grant's TTL. The root then executes the op
  alone. There is no two-phase commit.
- **Placement** generalizes ADR-15: the root sees every op's origin and
  delegates the *topmost* directory that one node dominates (at least
  70% of its ops over a 30 s window, above a rate floor), recalling
  when the share stays under 50% for a dwell. This follows Ceph's
  finding (Mantle, SC'15) that metadata load should move to its
  dominant writer rather than be spread.
- **Hot shared directories** (plan 30 M12): parent attributes merge
  commutatively (HLC timestamps with `max`, `nlink` as deltas), creates
  and unlinks hold their parent *shared*, and a directory can be split
  into up to 16 name-hash ranges (GIGA+), each delegated separately.
  Placement delegates a range only to a writer that dominates it.
- Offline designations are non-stealable delegations: the same
  machinery without TTL expiry.

**Consequences**: a node writing its own subtree runs at local speed and
S3 request counts do not change, since there is still one log. The
cost moves to cross-subtree ops (a recall round trip, or a TTL wait if
the delegate is gone) and to the root, which must stay reachable to
append streams. The root lease stays put while any delegation or lock
grant is live. No delegation exists inside a continuation epoch.
Automatic hash-range splits fire only when writers' names fall into
distinct hash ranges; names hashed uniformly across writers give no
range a dominant writer, and splitting such a directory measured
20–40% slower on a LAN than leaving it with the root, so placement
leaves it alone. Manual `delegate --range` remains available.

**Rejected**: per-subtree partitions with their own logs (ADR-5's
original plan; cross-partition ops need distributed commit).
Multi-appender logs or CRDT metadata (ADR-14). Two-phase commit for
cross-subtree ops (recall is simpler and cross-subtree ops are rare in
the target workloads). Spreading load across sequencers by hashing
(it destroys locality; every writer would pay a hop). Leaseless
optimistic commits (ADR-28).

**See**: plan 30 §M11–§M12;
[Delegations](../reference/features/delegations.md);
[`crates/authority/src/core/delegate.rs`](../../crates/authority/src/core/delegate.rs),
[`crates/authority/src/core/placement.rs`](../../crates/authority/src/core/placement.rs),
[`crates/meta/src/delegation.rs`](../../crates/meta/src/delegation.rs),
[`crates/meta/src/hlc.rs`](../../crates/meta/src/hlc.rs);
models `crates/model/src/delegation.rs`, `crates/model/src/hotdir.rs`.
Amends ADR-5, ADR-14 and ADR-15.

## ADR-24: The hybrid S3 inbox for writes without a P2P path

**Context**: with P2P off, or with no path to the holder, every write
from a non-holder had to move the lease through S3. Alternating writers
then ping-ponged it, which capped a P2P-off cluster at 41–57 ops/s and
disturbed the holder's own work for every sporadic write.

**Decision**: a requester with no P2P path to the holder forwards
through the bucket, and moves the lease only when its demand is
sustained.
- "No P2P path" means P2P is disabled, the holder is not in the peer
  directory, or an outage to it (a failed dial, or a transport error
  that evicted the connection) has lasted longer than
  `CONSTELLATION_INBOX_P2P_GRACE_MS` (3 s) with nothing heard since. A
  slow reply on an open connection is never an outage.
- Ops, with their rids, are batched into CAS-created objects
  `inbox/<epoch>/<node>/<n>`. The holder polls each requester's next
  batch with one GET on an adaptive schedule (hot after a hit, a warm
  ceiling for a minute, then the sync loop's idle ceiling) and polls
  nobody it is P2P-connected to.
- Outcomes ride the log: `Completed { rid }`, or `Refused { rid, errno }`,
  plus an `InboxAck`. The requester already tails the log, so it reads
  its outcome there. Recording refusals (later extended to every
  definitive refusal, ADR-18) is required here because a successor's
  drain and a requester's re-submission can both re-read the same rid.
- A requester whose inbox-answered ops in a 10 s window reach 8, or
  whose round trips add up to 1.5 s over at least five ops (the single
  slowest left out), *escalates*: it asks for the lease through the
  ordinary `wanted_by` path and keeps using the inbox until the lease
  arrives. (Plan 30 M13 shipped 20 ops and 3 s; M5 retuned them.)

**Consequences**: a sporadic write from a non-holder costs a few S3
round trips and leaves the lease alone. A create storm still moves the
lease, as before, because Linux serializes creates in one directory:
each op is one sequential inbox round trip, and no batch can form. An
idle P2P-off cluster costs the holder one GET per requester per cold
interval. Cluster locks and `cto=strict` ReadIndex do not use the inbox
(ADR-25, ADR-20). `CONSTELLATION_INBOX=off` restores the pre-plan-30
behaviour.

**Rejected**: the inbox as a full replacement for lease movement. Plan
30 M13 measured it: under a per-directory create storm the inbox cannot
batch and loses to moving the lease. Polling every requester at a fixed
fast rate (S3 cost grows with node count while idle). Unrecorded
refusals on the inbox path: the Stateright model found both a drain
without rid dedup and undeduplicated refusals executing an op whose
caller had been told `EEXIST`.

**See**: plan 30 §M13;
[Forwarded mutations — the inbox](../reference/features/forwarded-mutations.md#the-inbox-forwarding-without-p2p);
[`crates/authority/src/core/inbox.rs`](../../crates/authority/src/core/inbox.rs),
[`crates/store-s3/src/inbox.rs`](../../crates/store-s3/src/inbox.rs);
model `crates/model/tests/inbox.rs`.

## ADR-25: Cluster locks are leased grants from the owning sequencer

**Context**: `flock` and `fcntl` were node-local, so SQLite or any other
lock-based application on two nodes could corrupt data. DESIGN.md's
strict mode (cross-node byte-range locks) had never been built.

**Decision**: `--locks cluster`, the default whenever P2P is on.
- The owning sequencer of a file (the lease holder, or the delegate of
  its subtree or range) grants whole-file shared or exclusive *grants*
  to nodes; byte ranges and lock owners are resolved on the node under
  its grant.
- Grants are leased (`CONSTELLATION_LOCK_TTL_MS`, 20 s), capped by the
  sequencer's own authority, and use read delegations' time discipline:
  the node honours a grant until `sent + ttl − margin`, the sequencer
  outwaits it until `granted + ttl + margin`.
- A node whose grant lapsed fails I/O on the files it holds locks on
  with `EIO` until they are unlocked or a new grant arrives (NFSv4's
  fencing rule), and fences the lock's *owner* (its process and the
  processes it started) on every file of the mount until the owner's
  locks are gone: an application guarding other files with the lock
  (git) must not write on without it. The TTL is long (20 s) so that
  lapses are rare; a crashed holder costs its waiters `ttl + margin`.
- A conflicting request recalls the other grants; a recalled node
  flushes the file's dirty data before it releases. The next grant
  carries a position the new holder waits for, and it drops its kernel
  cache of the file, so lock-protected read-modify-write works across
  nodes. The position covers every file, not only the locked one: a
  release carries the releaser's session frontier (every reply its
  clients got, and a root holder's unshipped journal), the owner joins
  it into every later grant of the file, and the new holder makes it
  its session watermark (EC2 campaign 4 B-1: git's refs under an
  `flock` turn file).
- Grants are cached after the last unlock, so an uncontended re-lock
  costs nothing.
- After a fast takeover, the successor waits out a grace period and
  accepts reclaims; after a TTL takeover every old grant has already
  lapsed. Locks never go through the S3 inbox.
- The grant table moves with a delegated subtree. The handoff rides a
  renewal reply, which the delegation's recall can overtake or which can
  be lost, so the root keeps a copy of what it handed. It re-sends the
  copy with every renewal and takes it back into its own table if the
  delegation ends without returning it. A grace period left at the root
  would not follow the subtree to its next delegate. So a delegation's
  first renewal also carries what is left of any root grace over its
  subtree, for example after a takeover of a released lease.

**Consequences**: lock users get correct cross-node exclusion by
default; workloads that never lock pay one atomic load per I/O; a single
node pays about 10–30 µs per lock/unlock pair. A blocked lock wait
cannot be interrupted (fuser 0.18 delivers no `FUSE_INTERRUPT`), there
is no deadlock detection, and a process's `flock` and `fcntl` locks on
the same file conflict with each other. With P2P off the mode is
`local`, and an explicit `--locks cluster` fails the mount.

**Rejected**: `local` as the default (silent corruption for the
applications that lock). Locks through the S3 inbox (one S3 round trip
per lock operation and renewals as S3 writes would make SQLite unusable
and fence I/O whenever S3 is slow). A separate lock server (ADR-2).
Byte-range state at the sequencer (more traffic for no benefit: ranges
are resolved where the processes are). A reclaim grace after a TTL
takeover (unnecessary: grants are capped by the lease they came from).

**See**: plan 30 §M14;
[Cluster locks](../reference/features/cluster-locks.md);
[`crates/authority/src/core/locks.rs`](../../crates/authority/src/core/locks.rs),
[`crates/meta/src/locks.rs`](../../crates/meta/src/locks.rs),
[`crates/cli/src/locks.rs`](../../crates/cli/src/locks.rs);
model `crates/model/src/locks.rs`. Amends ADR-4.

## ADR-26: Exact chunk-location reconciliation replaces bloom digests

**Context**: peers learned each other's cached chunks from bloom-filter
digests (~1% false positives). Bloom deltas are add-only, so an evicted
chunk stayed advertised until its bucket's next snapshot, and every false
positive cost a peer round trip before the fetch fell back to S3.

**Decision**: every node keeps an exact mirror of each peer's published
chunk set (clean or pinned chunks, keyed by the first 8 bytes of the
hash). Pushed deltas (`CacheSetDelta`) keep mirrors current, a small
summary heartbeat (`CacheSummary`) detects drift, and range-based set
reconciliation (Negentropy-style additive range fingerprints over
aligned 16-ary hash-prefix ranges) repairs any gap over a direct stream.
`CONSTELLATION_COOP_DIGEST=exact` is the default; `bloom` stays
available.

**Consequences**: zero false-positive peer fetches by construction (the
`peer_false_positives` counter makes this checkable). Removals propagate
within one publish tick (250 ms). By design, steady-state digest traffic
is a ~150-byte summary per interval plus a few bytes per changed chunk.
That is not yet confirmed by measurement: the only comparison run
(`coop-digest-compare`: 3 nodes, heavy churn, a 1 s digest interval
instead of the default 30 s) measured about 2.4 kB/s of fleet digest
traffic in exact mode against about 0.65 kB/s for blooms, with zero
false positives in both and fewer stale misses in exact mode. The bloom
code therefore stays until a production-interval measurement settles
it. The cost is
receiver memory: 8–12 bytes per peer chunk, which exceeds the bloom
mode's 4 MiB-per-peer cap beyond ~350k chunks per peer; a peer with more
than 8M chunks is not mirrored. An initial sync costs about 7 bytes per
key. Upload existence hints ask the exact mirrors too, so they no longer
spend HEADs on bloom false positives. The protocol carries no safety
state: a wrong mirror only costs a declined fetch.

**Rejected**: the `negentropy` crate (40-byte items, a sealed vector
that re-seals on every change, and symmetric union-sync rather than an
asymmetric mirror with removals). Rendezvous hashing (it decides where a
chunk *should* be, not where it is). Keeping blooms as the default (the
false positives are exactly what the counter was built to eliminate).

**See**: plan 30 §M15;
[Cooperative cache membership](../reference/features/cooperative-cache.md);
[`crates/net/src/reconcile.rs`](../../crates/net/src/reconcile.rs),
[`crates/cli/src/coop/exact.rs`](../../crates/cli/src/coop/exact.rs).

## ADR-27: Portable S3 only, and one bucket

**Context**: plan 28 §P12 proposed an optional latency tier on S3
Express One Zone: a directory bucket for the hot commit chain and WAL,
with single-digit-millisecond latency and appendable objects. That tier
is AWS-only, lives in one availability zone, costs about 7× as much to
store, and needs a second bucket. Plan 30 also considered buckets in
several providers or regions, for disaster recovery or as a quorum.

**Decision**: Constellation relies only on what every mainstream S3
implementation offers: GET, PUT, LIST and DELETE, plus `If-None-Match: *`
and `If-Match` on PUT. `fs create` preflights exactly these (it refuses a
backend without create-if-absent), and `doctor` probes them. Nothing
AWS-only is used: no S3 Express One Zone, no conditional DELETE, no
`RenameObject`. A filesystem lives in one bucket (under one prefix).
There is no second bucket for disaster recovery and no quorum across
buckets. Plan 28 §P12 is retired.

**Consequences**: the same binary runs on AWS S3, MinIO, R2, OVH and the
floci test backend, and every protocol can be tested against an
in-memory object store. Latency has to come from elsewhere: forwarding
to a sequencer and P2P delivery (ADR-14, ADR-20), delegated
sub-sequencers for locality (ADR-23), and durability layers matched to
the topology (ADR-21). With one bucket, a bucket outage is survived by
authority that already exists: held leases until their TTL, offline
designations, and continuation epochs (ADR-22). Durability beyond the
provider's own is the operator's business (bucket replication,
versioning); `doctor` reports versioning but nothing relies on it.

**Rejected**: an optional Express tier. It would be the only fast path
on AWS, untested everywhere else, and it adds a second bucket's failure
modes. Multi-bucket quorums (CASPaxos or Disk Paxos across providers):
every write would pay the slowest provider's round trip, and operators
would have to run several buckets for one filesystem.

**See**: plan 30 §2 (constraints 1 and 2) and §5; plan 28
[§P12](../plans/v1/done/28-s3-native-metadata-store.md#p12--optional-latency-tier-s3-express-one-zone)
(retired);
[`crates/store-s3/src/store.rs`](../../crates/store-s3/src/store.rs)
(`preflight`, `probe_conditional_writes`);
[Write-path hygiene](../reference/features/write-path-hygiene.md).

## ADR-28: Leaseless optimistic commits stay deferred

**Context**: plan 28 §P3 sketched writers committing without a lease:
CAS-create the next commit and, on a lost race, rebase structurally or
re-execute. That would remove the sequencer (and forwarding) entirely.

**Decision**: keep the lease-and-sequencer design. Plan 30 scales it out
with delegated sub-sequencers (ADR-23) instead of making commits
optimistic.

**Consequences**: a portable conditional PUT takes 28–190 ms depending
on distance to the bucket. The optimistic systems that work
(FoundationDB, Aurora DSQL, Tango, Aria) rely on a commit path in the low
milliseconds. At S3 latency, every conflicting write would pay one or
more S3 round trips, and a hot directory would livelock on retries.
Leases and forwarding keep the common case at local or LAN speed.

Revisit only if all of these hold:
- a real workload has no write locality, so ADR-23's placement cannot
  find a dominant writer or a hash-range split for it, and forwarding
  latency to the root dominates;
- a portable conditional PUT reaches the low milliseconds across the
  deployments that matter (ADR-27 still rules out an AWS-only tier);
- the structural rebase of plan 28 §P3 can run without the lease's
  fencing, including against a deposed holder's late batch.

**Rejected**: building §P3 now. It is the larger bet, and the benefit
appears only on workloads that ADR-23 does not already cover.

**See**: plan 30 §5; plan 28
[§P3](../plans/v1/done/28-s3-native-metadata-store.md#p3--concurrency-optimistic-commit-with-structural-rebase-leases-demoted).

## ADR-29: A state dir is taken over only from a process the kernel has already killed

**Context**: `mount` takes `daemon.lock` (`flock`) to decide between
becoming the daemon and attaching to the one that holds it. EC2
campaign 6 (finding B-1) showed that a held lock is not proof of a
daemon that will answer: a `kill -9`ed lease holder lingered as a zombie
whose last thread was stuck in the kernel, so its file table — the lock
and the `control.sock` listener — stayed alive, and every remount
attached to a listener nobody served and waited forever.

**Decision**: a lock holder is pinged before anything is asked of it,
every wait on it is bounded, and a holder that does not answer is
classified from `/proc`, not from a timeout. Only a process the kernel
itself reports as killed — thread-group leader a zombie and SIGKILL
pending, or every remaining thread past `exit_mm` — is taken over: its
lock inode is moved aside (the zombie keeps its lock on it) and the
mount becomes the daemon on a fresh lock. A holder that can still run
(sleeping, stopped by SIGSTOP, busy, or a zombie leader with live
threads) is never taken over; the mount fails within the attach timeout,
naming the pid and its state.

**Consequences**: a rejoining node never blocks its startup on a dead
predecessor, and the mutual exclusion the lock provides (one writer of
`meta.db` per state dir) is preserved without a liveness guess: the
only process displaced is one that can never run user code again. A
holder that is alive but wedged needs an operator (`kill -9`; if it then
lingers, abort its FUSE connection under
`/sys/fs/fuse/connections/*/abort`), and the next mount takes over.

**Rejected**: taking over on a timeout alone (a stopped or busy daemon
would be displaced while it could still write `meta.db`); never taking
over (the campaign's node could not rejoin until its kernel state was
resolved by hand).

**See**: `crates/cli/src/daemon_lock.rs`; the `stale-daemon-lock` and
`holder-kill-rejoin` harness scenarios;
[named filesystems](../reference/features/named-filesystems.md).

## ADR-30: `--cto bounded` stays the default

**Context**: ADR-20 built two close-to-open modes and left the default
to plan 30 M16, to be decided from real-S3 measurements. The EC2
campaigns measured both modes on AWS S3 (us-west-2) and on OVH (Milan)
with four nodes in one region.

**Decision**: `bounded` stays the default. `strict` remains a per-mount
opt-in (`--cto strict`, or `CONSTELLATION_CTO=strict`).

- **What strict costs.** Campaign 7 (`cb847f8`) timed 30 warm `stat`s:
  1.17 ms under `bounded` and 10.15 ms under `strict` on AWS, 1.07 ms
  and 10.19 ms on OVH. That is about 9× the per-op cost of `bounded`,
  roughly 0.3 ms more per `stat` (0.04 ms against 0.34 ms), the same on
  both backends. Under `strict` the kernel caches nothing (TTL 0), so
  each call reaches the daemon; a warm `bounded` `stat` is answered by
  the kernel. Campaign 4 (`edd3d5d`) found the same on cross-node opens:
  warm p50 0.15 ms against 0.39 ms, cold p99 44 ms against 77 ms. A lone
  sequencer pays nothing (campaign 6, one node: no difference).
- **What bounded gives up.** Nothing that the campaigns' correctness
  checks could see. Campaigns 7 and 8 passed every check under
  `bounded`:
  - concurrent 4-node first mounts (root owned by the mounting user,
    `mkdir` works);
  - the `O_CREAT` race and SQLite first touch;
  - exact lock counters after a holder kill, and the lock fence;
  - git under a `flock` turn lock: campaign 8 found 0 causal violations
    and 0 ref regressions in 568 checks on AWS and 246 on OVH;
  - 3 h soaks with faults, which converged byte for byte.

  Lock-coordinated sharing gets its cross-node coherence from the lock
  grant, which carries the releaser's frontier (ADR-25), not from the
  `cto` mode. Visibility under `bounded` was p50 28–34 ms and p99
  53–76 ms on AWS, and p50 about 210 ms and p99 250–350 ms on OVH.

**Consequences**: open- and stat-heavy workloads (builds, `find`,
`git status`, web serving) keep kernel caching and pay nothing for
close-to-open. Choose `strict` on the *reading* node when a reader must
see a `close()` on another node that it learned about out of band,
without a cluster lock between them. For example: a job on one node
writes output and signals a job on another node through a queue, an
HTTP call or `ssh`; a fleet serves files right after an uploader
elsewhere closes them. On one host, 12–16 of 80 `bounded` opens right
after another node's close saw the old content; `strict` saw none, and
campaign 4 found 0 stale reads in 1000 strict close-to-open iterations
on AWS and 300 on OVH. Strict costs one round trip to the sequencer on a
first open (one WAN round trip across continents) and about 0.3 ms per
call afterwards. With P2P off it guarantees only what is already in the
log.

**Rejected**: `strict` by default (its cost falls on every open and
`stat` of every workload, including the lock-coordinated ones that do
not need it, and it turns off kernel attribute caching). Switching modes
per directory automatically (not built; nothing in the measurements
called for it).

**See**: [Close-to-open modes](../reference/features/cto-modes.md#choosing-a-mode);
[RESULTS.md](../../bench/remote/RESULTS.md#plan-30-real-s3-results);
plan 30 [§7](../plans/v1/done/30-write-path-resilience-and-scale-out.md#7-close-out).
Amends ADR-20.
