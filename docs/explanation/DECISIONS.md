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
- A *promise* is a node's persisted word that it is in no open epoch and
  joins none for a while, a duration on its own wait clock, made for one
  lease key (plan 43 C7b: it names the key, so no clock of another node
  is compared). It is written to `heartbeat/<node>` after being persisted
  locally, and only on demand: when a would-be taker asks over P2P, when
  the node's own observation of a foreign lease key ran out and nobody
  can ask it, or when its slack changes.
- A node joins an epoch only once its own last issued promise has run
  out on its wait clock, and publishes no promise while its epoch is
  open.
- An S3 takeover of an expired lease that another node held needs at
  least `f` *other* roster nodes with evidence for the lease key it
  claims (a heartbeat naming the key, or a P2P grant to a request sent
  after its own observation of the key ran out). A taker's `f` promisers
  and an epoch's `N − f` members must share a node, which would have to
  be in no open epoch after the key's lease had ended and yet a member of
  an epoch that formed before.
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
- `Delegate { dir, node, gen }` and `RecallCut { dir, gen, through }`
  records keep a replicated delegation table (the end of a generation
  carries its cut: plan 43 C2). Delegations never overlap, and there is
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

**Amended (a stuck core and delegations, plan 41)**: both ends of a
delegation's renewal are steps of an authority core, and a core stuck in
one step (a metadata-store write behind a slow journal write) renewed or
answered nothing, so a delegation was reclaimed and lapsed with both nodes
alive. Two lanes beside the cores keep it: the delegate's, which sends the
renewals the stuck core cannot (`deleg_lane`, registered with the core and
counted as a late answer, honoured from the lane's own send), and, at the
root, its P2P service answering the renewal itself while the root's driver
is stuck (`deleg_lane::answer_root`): the generation's window lives in one
shared table (`DelegWindows`), extended by a `max` by the core and the
answerer alike, and **closed inside the core's step** by whatever ends or
shortens it or after which the core would grant no more (a recall, a seal,
the end, an epoch, the lease gone); the expiry decision reads the clock
inside the table's guard and closes in the same critical section, so a
window extended after a step took its `now` is re-armed, never reclaimed on
the stale read. The answer rests on the lease certificate the core publishes
at the end of every step (usability, the cap by the lease, the freshness rule
of a tenure that can be taken over early, the lease mark) and on a lock side
that has nothing to hand over; it carries no lock content, and the delegate
applies it only as an extension of a grant it already serves. Every
extension is one the core could have made then. Checked by the delegation
model (`Stall`, `LaneRenew`; the lane extending a closed slot, or without the
cap, violates `recall_safety`, the design is clean), the simulator
(`delegated-core-stall-root*`) and the harness (`delegate-store-stall` with
the root stalled). A root with a backup answers only within
`backup_takeover_ms` of its core's last proof of liveness until the lane can
prove its own (plan 41 P). Same pattern for lock grants: ADR-25's amendment.

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
  Renewed half-way through its window, a grant is sure to survive a
  *stop* of its holder (a partition, a frozen or dead process: nothing
  in it runs) only up to `(ttl − margin) / 2` (9.5 s), and one up to
  `ttl − margin` (19 s) only right after a renewal, never a whole TTL.
  A stall of the authority core alone (a metadata-store write behind a
  slow journal write) is no longer such a stop: see the amendment below.
- Each mutation of a lock owner carries a fencing token (Chubby's
  sequencer, Kleppmann's argument): its grants, the end of the window
  its node honours them for and what that window had left when the
  operation left the node. A sequencer that holds the grant (its
  minter) judges it exactly, live or ended; any other refuses it once
  its own wall clock reaches the end or its honour clock reaches its own
  receipt plus what was left (plan 43 C7b), so an operation admitted
  before a lapse and delayed past it never lands: safe with either the
  offset below the margin or the operation's one-way delay below
  `2 × margin`. A
  window, not only an id, because the sequencer executing a write often
  did not mint the lock (a delegated subtree under a root-owned lock
  file): checking the window costs no round trip to the minter and no
  revocation broadcast.
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

**Amended (close and release)**: the kernel drops nothing at close for
the locks it forwards: `FLUSH` (every `close(2)`, waited for) drops the
closing process's POSIX locks, and `RELEASE` (the description's last
close) its OFD and `flock` locks. `RELEASE` is sent after `close(2)`
returned, in the background, so a request served before it saw a closed
description's lock (`EAGAIN` where local file systems grant). A
non-blocking request or a test whose only conflicts are such locks now
waits for their `RELEASE`, for at most
`CONSTELLATION_LOCK_CLOSE_WAIT_MS` (100 ms) from the `FLUSH`, and never
more than that from the request itself (closes of the description's
other descriptors keep coming in a shell loop that holds the lock), and
a non-blocking request refused only because an earlier turn's
operations are still in flight under the grant (the release's
publication, or an op whose tag ends just after its answer) waits for
them as a blocking one does, within the same bound. A mount served by
a single worker takes no wait for a closed description: the kernel's
queue order serves the `RELEASE` first, and the wait would hold the
worker it needs. Rejected:
dropping OFD and `flock` locks at `FLUSH` (a `dup`'s close would drop
them while the description stays open); waiting for every `RELEASE` in
flight (a `dup`'s close is followed by none, and the daemon cannot tell
the last close from the others). See
[Close and release](../reference/features/cluster-locks.md#close-and-release).

**Amended (concurrent operations)**: a new turn on a node waits only
for what the earlier turn counts as done and may still land at the
minter after the new turn's writes — a close's (`FLUSH`, `RELEASE`)
publication of the file under the grant, and an operation tagged with
the grant that ended in doubt, pinned from the moment it is found in
doubt, before its answer. It no longer waits for every tagged operation
in flight: the fencing token is per process, so another thread's
operation (or, through xcheckfs, any mirrored application's) carried
the grant, and under load a non-blocking lock nobody held waited for it
past `CONSTELLATION_LOCK_CLOSE_WAIT_MS` and answered `EAGAIN`. An
operation still waiting for its answer is concurrent with the unlock;
the release still waits for it. What makes the narrowing safe beyond
that: the node commits one file's publications one at a time (its
operation lock), so a new turn's commit of the file is never sent
before an earlier one was answered, and a stale commit replayed later
is refused by its manifest base. Residual: an operation of the same
process that ended in doubt, on any file, still pins the grant for up
to its window, and a non-blocking request answers `EAGAIN` meanwhile.
Rejected: an unbounded wait for every
tagged operation (correct answers, but `F_SETLK` would stall behind an
unrelated `fsync`, for as long as an S3 outage lasts).

**Amended (a publication ends with its commit)**: a close's publication,
which a new turn on the node waits for, ends when its manifest commit is
answered, as the turn rule above says; under `--write-mode through` on
the sequencer the close then waits for the file's chunks to reach S3,
and that wait no longer counts. Nor does it hold the file's operation
lock: the lock orders the file's publications (one commit at a time)
and an mtime set against them, and that order is settled at the commit.
The upload is what the closing caller waits for, as `through` promises;
whoever needs the chunks in S3 next (an `fsync`, a lock release's flush,
the next `through` close) drains the file's pending chunks itself.
Before, both lasted through the upload: a non-blocking lock of the next
turn behind a close waited up to `CONSTELLATION_LOCK_CLOSE_WAIT_MS` and
answered `EAGAIN` with no lock in the way when an upload took longer,
and every read, write and close of the file queued one upload per close
(xcheckfs `lock_traffic_concurrent_with_io`, four threads opening,
writing and closing one file, timed out at 60 s on a loaded host). The
release ordering is unchanged: a recalled grant's release runs its own
flush first, which drains the file's pending chunks (sharing the
close's uploads in flight) and syncs the journal before the grant
moves.

**Amended (renewals on a fast tenure, plan 41 L0)**: a root whose tenure
can be taken over before its lease expires (a listed backup, or
`ack=s3`) extends a window it promised (a lock grant's renewal, its own
grants' included; a reclaim; a delegation's renewal) only as a new grant
is made: within `backup_takeover_ms` of the send of an S3 request that
proved the tenure, on a lease marked as granting. The successor's grace
covers no more. Renewals had checked only that the lease was usable, so
a root its backups had sealed, still reachable from lock holders, kept
renewing (its own grants and its peers') until it noticed the
deposition, up to `lock_ttl` past the successor's grace, and its
delegates kept granting in a subtree the successor reclaimed (the
model's `fast_takeover_renewals_without_the_probe_violate`). A renewal
decided on stale liveness is applied to the owner's table and its answer
held behind one lease read, sent once the read shows the tenure still
holds (a proof sent after the decision covers it) and refused
(`NotOwner { 0 }`; a delegation's answer dropped) if it shows a
successor. A root that cannot reach S3 therefore renews nothing: cut
longer than about `(ttl − margin) / 2`, its tenure's grants lapse even
while its backups acknowledge (before, they lasted until the lease
ran out). After a fast takeover, the successor outwaits an inherited
delegation `backup_takeover_ms` longer than the reclaim horizon, as its
lock quarantine adds that window. A lone node, and any `Local` tenure
without backups, is unchanged: no read, no wait. Rejected: answering `NotOwner { 0 }` at
once (each holder would re-read the lease from S3 and send again, the
same round trip, for every holder); a shorter TTL instead of the read
(a window still decided without a proof). See
[Leases, renewal and fencing](../reference/features/cluster-locks.md#leases-renewal-and-fencing).

**Amended (the token at the write, load review F1)**: an executor
checks the fencing token where the write happens: inside the
operation's own store transaction, with the store's writer lock held,
on its clock read there. The check before the operation stays, as an
early refusal. Before, that earlier check was the only one. It ran
before the operation waited for the writer lock: a FUSE thread behind
a slow journal write, or a core step that took its `now` before it
waited. A grant could end during the wait, the next holder be granted
and write, and the stale write then land and be answered `OK`. For
data, that was the next holder's read-modify-write rebased over it
(`commit_manifest_with_rebase`), silently losing an acknowledged
write. Then the order between the check and the next grant: a write
that passed the check *pins* its grants until it commits, which can
still take seconds. A position the node reports as covering what it
executed (a grant's floor or position, an outwait's cut, a delegate's
stream head) first waits for the pinned writes under grants no longer
live in its table — ended there, or, for another sequencer's grant,
past its token's window (earlier, no cut it reports can settle a
barrier that needs the write, so a delegate's renewal does not wait for
every commit in flight). They hold the writer lock, so taking it once is
the wait. A write that checks later is refused (the grant ended at its
minter, or its window is over elsewhere). So every write that passed
a check is under the next holder's floor. That held only for outwaits
and same-node releases. A release after an operation tagged with the
grant ended in doubt has no position for that operation, which may
still be committing at its executor. Such a grant is not released
while that op is unresolved (its `Completed` or refusal not yet in the
node's replica): the node drops it and its owner outwaits it, whose
barrier waits for the cut. An owner's own grant that runs out unreleased while
one of its operations tagged with it is in flight or in doubt leaves
the same barrier. The FUSE fast path also re-checks its lease, or its
delegation, inside the transaction. One that ran out sends the op
through the core rather than journaling a row a takeover would strand.
Rejected: re-checking only with a fresh clock after the lock is taken
(the commit can still outlast the window or follow the next grant);
holding the lock table's mutex across the commit (every lock operation
would wait for disk writes); bounding the commit's duration (a blocked
`write(2)` has no bound). See
[The fencing token](../reference/features/cluster-locks.md#the-fencing-token).

**Amended (renewals off the core, plan 41)**: the renewal of a grant was a
step of the node's authority core, and the core's step writes the metadata
store synchronously, so a store write that waits seconds on the device (a
FUSE writer's slow journal write holding the writer lock, the core's
priority write behind it; the xcheckfs run of 2026-10-13: 18 s and 24 s on
one node) lapsed the node's grants and fenced its lock owner with `EIO`
although nothing was wrong with the node but a slow disk. A **renewal lane**
beside the core now extends windows from memory alone — the lock tables
the FUSE threads already share — under a **certificate** the core publishes
at the end of every step, before the driver dispatches the step's actions
(`Core::lock_lane_view`, `crates/authority/src/lane.rs`). It renews this
node's own grants at its own owner table (one `LockTables` critical
section, `renew_own`) and sends `LockRenew` for the grants it holds at an
owner, applying `Ok` answers from the send as the core does; it never
grants, reclaims, refuses, releases, drops a grant, routes, touches the
store or logs, and reports every result to the core, which decides the rest.
It runs always (so every lock workload exercises it).

*What it may judge*: own grants only under the certificate — the root lease
held, usable and not fenced, no continuation epoch, no delegation (checked
live), not resuming, no unmarked release or final mirror — `now` below the
lease's usable end, and, for a tenure that can be taken over early, an S3
proof of liveness within `backup_takeover_ms` and a marked lease (the L0
freshness rule); held grants at a remote owner without such conditions
(routing is liveness only: a wrong owner answers `NotOwner`, which the core
handles), until the driver has been stuck `CONSTELLATION_LOCK_LANE_MAX_STALL_MS`
(120 s).

*The argument*: every extension at `t` is one the core could have made at
`t`. One process, one clock; the lane extends a record only if it is live
under the tables' mutex and every core decision that ends a record is one
critical section of the tables (`forget_outwaited_if_expired`,
`drop_held_if_lapsed`, `renew_own`: plan 41 L1), so it precedes or follows
an extension, never straddles it; the authority facts are time-bounded and
enforced by the lane (the cap, the freshness window) or withdrawn by the
certificate of the step that changes them, published before the step's
actions (a release's CAS, an epoch's recall) go out, so any extension
precedes the change and the successor's grace covers it.

*Checked by*: the lock model (`Stall`, `LaneRenewOwn`; the lane ignoring the
cap, ignoring a fast tenure's freshness, or — together — keeping its
certificate through a release and reviving dropped records, each violates
`mutual_exclusion`; the design with stalls, backups, TTL and released
takeovers is clean), the simulator on every lock configuration
(`locks-core-stall*`), the unit tests of the lane and of the certificate
(`lock_lane_core_tests`), and the harness (`lock-stall-lone-node`,
`lock-store-stall-holder`, `lock-store-stall-past-lease`).

*Not covered, on purpose*: a **stalled owner's** peers' grants (answering
their renewals off the core, and the restart horizon's lead: follow-up L5);
the lane's own lease probe for fast tenures (L6: until then a fast tenure's
own grants are renewed only while the core's last proof of liveness is fresh,
about 1.5 s into a stall); `ack=s3` holders, deposed on the core's silence;
a stall past the root lease (the grants end at its usable end); a hung core
(past the bound). *Rejected*: asynchronous store writes in the core (a
rewrite of the step discipline), bounding the core's blocking (timeouts
on a write that cannot be abandoned), a second lease renewer (it races
the core's CAS tags), and the journal's `write(2)` outside the store's
single-writer lock (vendored fjall change 10, plan 41 §4 D8: decided on
2026-10-22, withdrawn by the maintainer on 2026-10-23 after its review). A
commit can drop the lock across its `write(2)` only by publishing before its
bytes reach the OS, which lets the shipper or a backup read a commit a crash
then loses; restoring that order removes every gain, since a later commit
may depend on the stuck one and the kernel's inode lock serializes the
journal's writes anyway (the core's 12 s stall waited 62 µs for the lock).
The same pattern, for delegations, is `deleg_lane` / `DelegWindows` (the
delegate's renewal lane, and the root's answer off its core, plan 41 R).

**Amended (latency, not errors, across a failover; plan 43 C7-F)**: a lock
request or a lapsed grant used to fail where the owner could not act yet: a
non-blocking request in a grace got `EAGAIN`, one at a stale owner (cut from
S3, its lease not over) `ENOLCK` after three `Busy` answers, one at an
unreachable owner `ENOLCK` once its attempts were spent, and a grant that
lapsed across a root failover fenced its owner with `EIO` from the lapse
whether or not anyone else was granted the lock (25 of 50 reads of the
harness's in-process locker at a 20 s lease). The maintainer decided
(plan 43 §11 Q18, 2026-10-18; C7-F F-c) that these cost latency:
- *F-b*: a request for a lock nobody holds waits out the grace, the new
  tenure's floor, a barrier, a delegate's short authority, a stale owner and
  an unreachable one (asked again as a fresh request, routed to the
  successor once the lease names it), all under one bound from the
  request's start, `lock_owner_wait_max_ms = ttl + margin +
  acquire_retry_max + lock_grace_wait_max` (112.5 s at the defaults), then
  `EAGAIN`; a signal ends it with `EINTR`; a conflicting grant still
  answers `EAGAIN` at once. The wait holds no frontend thread.
- *F-c*: a grant that lapses while local locks or an unpublished write rest
  on it is *lapsing*: the I/O that would be fenced is parked and asked again
  once the verdict is in, the holder sends late renewals, and the owner
  admits one only where it can prove no conflicting grant was issued since
  (a *settled* record in its table or in a bounded memory of outwaited
  records that every change letting another node act on the inode forgets,
  or, at a fast successor, the old root's *final mirror* of the lease body
  it replaced). Refused, or unanswered within the same bound, the fence
  applies from the end of the grant's window as before.

Checked by the lock model (`late_reclaim` and the three naive variants
`late_admit_unknown`, `late_admit_nonfinal`, `late_lapsed_unsuperseded`,
each violating mutual exclusion), the simulator and the harness
(`s3-cut-holder-failover-with-locks`). *Rejected*: reclaiming an unknown late
grant inside a grace (the old owner may have outwaited it and granted
another node the successor never heard of). Plan 43 C4 moves grants into
the log and replaces the final mirror with it. See
[Cluster locks](../reference/features/cluster-locks.md); PROGRESS, "a stale
or unreachable owner is a wait" and "a lapsed lock holder parks instead of
failing".

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

## ADR-31: The FUSE adapter keeps the kernel's lookup counts

**Decision**: the Linux FUSE adapter counts, per node id, the lookups its
replies hand the kernel (every successful `LOOKUP`, `CREATE`, `MKNOD`,
`MKDIR`, `SYMLINK`, `LINK` reply, counted before the reply is written) and
subtracts each `FORGET`'s count; it tells the view the kernel let go of an
inode (`Vfs::forget`) only when the count reaches zero. In doubt it keeps
the inode: a `FORGET` of more than was counted, or of a node id never
counted (except the root, whose reference comes from the mount), means a
reply's count was missed, and is logged rather than forwarded — a bounded
leak instead of a reap under a live reference. The counts cross a session
handover with the connection. The
view itself still keeps no lookup table: what it needs is "the platform
references this inode no more", and only the frontend can know.

**Why**: the view reaps an orphan it kept for the kernel's reference at
its `forget` (an unlinked file reopened through an `O_PATH` descriptor,
DESIGN §3 "Kernel references count as holds"), so a `FORGET` that gives
back only some of the kernel's lookups must not reach it. The kernel sends
such partial forgets for inodes it still holds: a dentry revalidation
whose `LOOKUP` names another inode than the dentry's forgets that answer's
one lookup, as does an entry the kernel fails to instantiate, and
`READDIRPLUS` force-forgets an entry it fails to link. The adapter used to
forward every `FORGET`, reasoning that the revalidation one names an inode
a name just led to, never an unlinked one; the `FORGET` is read later than
it is sent, though, and another worker can apply the `LOOKUP` and the
`UNLINK` of that inode's last name first. On two nodes of one filesystem
on S3 that answered `ESTALE` for 2 of 20 descriptors (plan 40 R2).

**Rejected**: no counts (plan 31 §6.11's choice: inode numbers needed no
table across a handover; the partial forgets do). Counts in the view
(every frontend would pay for one platform's protocol, and the view would
need the frontend's reply ordering to count right). Counting only while
`READDIRPLUS` is negotiated (the revalidation race exists without it).

**Consequences**: one shard lock and a hash-map update per entry reply and
per `FORGET`, about 32 bytes per inode the kernel holds; a reply the kernel
never processes keeps its inode until the session ends (a bounded leak,
never a premature reap). The handoff changed shape in place (the
maintainer's rule: no version bump): the first `daemon --upgrade` across it
rolls back to the old image, and the views take a remount to move.

**See**: `crates/frontend-fuse/src/refs.rs`; PROGRESS, "Fix: a partial
`FORGET` reaped an orphan the kernel still referenced";
[plan 40 §4.4](../plans/v1/wip/40-readdirplus.md). Amends plan 31 §6.11.

## ADR-32: Logging never blocks a daemon thread

**Decision**: a daemon (`mount`, `serve`, a resumed `daemon --resume-from`)
never makes a thread wait for its log's sink. Every log line goes, under the
mutex the control ring already took, into the ring and into a bounded sink
queue as one `Arc<str>`; one writer thread (`cst-log-writer`) writes the
queue to stderr (`daemon.log`, a container's pipe, a harness `mount.log`) in
batches of up to 256 KiB. The queue is bounded in bytes
(`CONSTELLATION_LOG_QUEUE_BYTES`, 16 MiB by default): a line that does not
fit is dropped and counted, the incoming one (an ordinary line is admitted
within seven eighths of the bound, `WARN` and `ERROR` within all of it), and
one `log lines dropped` summary line, written outside `RUST_LOG`, marks the
gap once the queue is back under half. The writer starts only after the
daemonizer's fork; the queue is flushed with a bound at exit (1 s), before
the handover's `exec` (20 ms; what is left goes to the new image in a memfd) and
before an abort (500 ms), and a panic's message goes through the same queue.
The harness gives its daemons a 128 MiB queue and fails a run's verification
on any drop, so its log checkers never go silently blind.

**Why**: the 2026-10-17/18 investigation of delegations lost under a
co-tenant build on the same btrfs volume found five of the six core steps of
half a second or more spent in `LogWriter::write` (up to 12.55 s, one of
which lost a delegation by itself), and the holder's off-core heartbeat
thread blocked 7.25 s in stderr writes against a backup's 1.5 s takeover
window. A buffered `write(2)` to a busy btrfs waits for the page's writeback
(`folio_wait_bit_common`); `lazytime`/`noatime` do not remove that wait and
"logs on a fast disk" is advice an operator may not be able to follow (a
container's pipe, a single-volume host). The decision of 2026-10-14 (host
mitigations only, plan 41 §1) is reversed on that evidence: it was a
decision about which cost to accept, and a lost delegation is not one.

**Rejected**: a per-thread never-block mode for the threads that must not
stall (the evidence has the core, FUSE workers and tokio workers blocked
too). `tracing_appender::non_blocking` (one more allocation per line, a
bound in lines, a worker spawned before the fork, flushed only by a guard's
`Drop` that `exit` and `exec` skip, no level reserve, no in-log summary).
Dropping the oldest line (O(1) is the incoming one, it keeps the lines that
led into a stall, and it leaves one contiguous gap, marked where it
happened). Blocking, or waiting a bounded time, for `ERROR` (an error logged
by the core during a disk stall is exactly the case that cost a delegation).
`O_NONBLOCK` on fd 2 (a regular file ignores it; a pipe's lines split at
`EAGAIN`; the flags are shared with the parent). A separate logger process
(it would survive `kill -9`, but the in-process side still needs the queue
and it adds a process to supervise across the handover; a possible
follow-up).

**Consequences**: lines can be lost behind a stalled sink, counted
(`status` `log.dropped_lines`, `/metrics`) and marked in the log; so are the
lines of a write the sink refused (a full disk); a handover's `exec` loses
none: the unwritten queue goes to the new image in a memfd and is written
first (only a failed memfd falls back to a counted loss, plan 44 W6); the
in-memory ring `constellation log tail` reads keeps every line. A `kill -9`,
`SIGSEGV` or OOM kill loses what is queued (the lines of the last few
microseconds on a sink that keeps up; the harness flushes before its own
`kill -9` and reports what it could not as a partial tail). A stalled
`write(2)` in uninterruptible sleep still delays `execve` and process exit;
the flush bounds only our wait, as the logging thread's own write did before.
A panic's message is queued in order with the lines before it and no longer
appears on stderr ahead of them; a panic that then aborts the process from
inside the panic machinery (a panic in a `Drop` while unwinding) loses its
message if the writer had not written it yet. `CONSTELLATION_LOG_QUEUE_BYTES=0` keeps the
synchronous writer for A/B measurements and failing-before tests. The audit
trail (a synchronous, never-lossy file), the `SIGUSR2` backtrace handler, the
CSI driver, `chaos` and the harness's own logging are untouched.

**See**: `crates/engine/src/log_buffer.rs`; `log-sink-stall` and its control
arm ([TESTING.md](../how-to-guides/development/TESTING.md)); PROGRESS, "Fix:
logging never blocks a daemon thread (plan 44)";
[plan 44](../plans/v1/done/44-non-blocking-log-writer.md). Supersedes plan 41
§1's interim guidance.

## ADR-33: Commit is the configured group's acknowledgement (Vertical Paxos over S3)

**Status**: proposed (plan 43 C0 drafts the decision; each milestone amends
it, and C1 adds the rejected options).

**Decision**: an acknowledgement is given only for a **committed** row. A row
is committed when every member of the group in force when it was journaled
holds it (the root's holder and every backup the lease object lists; a
delegate and each recorded backup, or the root's appended row when none is
recorded), or when a log segment carrying it landed in S3 (the archive). The
lease object and the root's log are the configuration master, and the S3 slot
CAS is the second fence. A sequencer answers a client without its group only
when it is **lone** (no peer within the RTT budget) and answers its own node's
client: that answer is the one speculation the design keeps, counted, and the
only one a deposition may roll back. A non-stealable authority (an offline
designation, a continuation epoch's hold) acknowledges as final. A client is
answered `EIO` only for a request that is settled (committed, or abandoned by
the sequencer in force), and a committed row is re-shipped verbatim, under the
identity it was committed with, never executed again by rid.

**See**: [plan 43](../plans/v1/wip/43-commit-rule.md) §3-§5 (the terms, the
invariants I1-I5 and the acknowledgement classes); the simulation's oracles
that check them ([TESTING.md](../how-to-guides/development/TESTING.md),
"Commit-rule oracles"); PROGRESS, "Plan 43 C0 — The commit point, named and
checked". Amends nothing yet; C1-C5 amend ADR-2, ADR-14, ADR-18, ADR-19,
ADR-21, ADR-22, ADR-23 and ADR-25 as plan 43 §6 lists.

**C1-2 and C1-3 (plan 43)**: the class a row's answer waits for is decided
when the row is journaled and stored with it (`RowClass`; the fast path's
rows carry the class the core published with its gate; the one change
after the fact is a row answered with the gate off, re-tagged `Lone`), and
every answer to a forwarder says whether it is committed. A claim of the
root lease by TTL carries the tail of the old group or waits for it
(ADR-21 amended): the claimant asks the lease's group (`SealTail`; a
member seals the epoch before it answers, so the group commits nothing
more of it) and the first member's answer is the tail it re-ships. Where
no member answers the claim waits, and only the operator's `repair
abandon-group` lets it go on without. A tenure that re-ships a tail names
the old group in its lease (`inherited_tail`) until the log carries the
rows, so that a successor's death does not take the last copies of rows
the old group acknowledged. The lease, the log and the seals are the only
evidence: no claim compares clocks beyond the observation rule of C7b.

## ADR-34: Kernel FUSE race workarounds

**Context**: three races in the Linux kernel's FUSE client, reproduced on
every distribution kernel from 5.10 to 7.3 by a draft patch series and on
Constellation by the `kernel-race-*` scenarios: (1) a write-through
`write(2)` unlocks its uptodate page before its `FUSE_WRITE` reaches the
daemon, and a drop of the page in that window lets a read fetch the
pre-write bytes, which the kernel then serves for an acknowledged write;
(2a) the kernel marks cached attributes valid before it stores them, so a
`stat` racing an install after a directory change copies the old values
once; (2b) a reply generated before a directory change but processed after
it cancels the change's invalidation for the attribute TTL. Until plan 42
every open dropped a file's pages (no `FOPEN_KEEP_CACHE`), which made
close-to-open hold for pages and repaired race 1 at the next open.

**Decision**: (W1) an open keeps the file's pages exactly when they are
known current: the engine answers the open with a stamp of the committed
content, the last dropping open saw the same stamp and its drop is known
complete, and nothing since could have made a page differ from what the
view answers (a data notification written or lost, a failed or short
write, a direct-I/O open, a read the race-1 bookkeeping cannot vouch for);
never under `--cto strict`. The engine reports what changes content under
a kernel without changing the stamp: an uncommitted session, a commit
through another view, a stranding rollback. (W2) the frontend follows every
drop it causes or can see coming and the pages reads fetched since; the
write the kernel's own serialization identifies as the one that may have
been in the window has those pages invalidated before it is answered, from
a repair lane with a bound that runs only while the write is alone on the
file. (W3b) a namespace reply is preceded by an attribute-only
notification for the directories it changed and the inodes it moved,
which makes the kernel reject a reply from before the change — for every
operation under `--cto strict`, where it costs no request, and in bounded
mode only for an inode an attribute reply may still be on its way to the
kernel for, tracked until the task's next request, a notice for the inode
or the task's exit, never by a clock (plan 42 F21: the notice invalidates
the whole attribute mask, so each one costs the next path walk a
`GETATTR`). (W3a) a
directory's attributes get no kernel lifetime under `--cto strict`; in
bounded mode the default is the view's TTL (plan 42 Q1, decided: `zero`
there made warm `git status` 2.6 to 7.9 times and `make -q` 2.8 to 4.2
times slower, so race 2a stays a documented transient exposure). Each workaround's `auto`
follows its fix's verdict (an override, a probe on a private mount at
start, a table of releases); W1 is independent of the kernel.

**Consequences**: reopen-heavy workloads read from the page cache instead
of the daemon. Where W3b is on unconditionally (strict), each namespace operation
writes one more 40-byte notification; narrowed, the tracking of attribute
replies costs a few lock round trips per attribute request and a bounded
table (a full one writes the notice instead). A write on a file whose pages are dropped constantly (every
open dropping, a reader beside the writer) may wait for a repair; the one
self-block, a repair waiting on the write's own last page, is answered
after a short grace. Directory TTL 0 costs a `GETATTR` per directory
component of every path walk (`default_permissions`): 2–11 times the FUSE
requests of stat-heavy walks, measured (hence `entry` in bounded mode). Residuals on an unfixed kernel:
plan 42 §5.9 and [kernel cache coherence](../reference/features/kernel-cache-coherence.md).

**Rejected**: the writeback cache (a durability and ordering model of its
own); `FUSE_EXPLICIT_INVAL_DATA` (a remotely appended file would read
zeros at the old end); a time-bounded record of notices (no kernel bound on
the window); re-invalidating the whole written range from the handler (it
deadlocks on the writer's own locked page) or after the reply (the
writer's read-back wins the race); TTL 0 for files' attributes; repairing
ambiguous reads; a wall-clock-only repair bound; holding the first
post-change directory reply.

**See**: [plan 42](../plans/v1/wip/42-kernel-race-workarounds.md);
`crates/frontend-fuse/src/{pagecache,repair,kernel_fixes}.rs`,
`crates/engine/src/view/kernel_cache.rs`; PROGRESS, "Plan 42".

**Detection, and the moved inode (plan 42 §5.4, F16)**: `auto` follows a
fix's verdict, which is an override if set, else a probe's for 0001 and
0003, else the table of releases (empty until the patches are merged);
`Unknown` counts as not fixed, so a gap in detection keeps a workaround on.
The probes run once per process, after the first mount, on a mount of their
own (a tiny in-memory filesystem the daemon serves), so no view's namespace
is touched: 0001 by whether an inode notification waits for a held
`WRITE`'s reply, 0003 by whether a `GETATTR` reply held across a `mkdir`
is cached. 0002 has no deterministic probe. A kernel with 0003 drops only
the changed directories' notices: `fuse_rename_common` still updates or
invalidates the moved inode's `ctime` without bumping its `attr_version`,
the 2b pattern, so the moved inode's notice stays whatever the verdict
(`node.status` `dir_change_notify_on` true, `dir_change_notify_dirs_on`
false). Decided with the workarounds (plan 42 §10, Q5 and Q6). See
`crates/frontend-fuse/src/kernel_fixes.rs` and `pagecache.rs`
(`PageCacheConfig::dir_notice_modes`).

## ADR-35: Clocks: rate, not offset

**Context**: until plan 43 C7b the lease, the seal watch, the promises and
the fencing tokens compared a value one node wrote on its own clock (a
lease's wall expiry, a promise's end, a token's window) with another
node's clock, and were safe only while the clocks differed by less than
half the lease margin (±500 ms at the defaults). Measured in the simulation
at offsets of a few seconds: a claimant ahead of the holder took a live
lease by TTL and granted over the holder's grants, a backup ahead sealed
a live holder at every renewal, an executor behind ran a stale operation
after the next grant. A node's *rate* error, by contrast, is bounded by
its oscillator (tens of ppm; 1 000 ppm budgeted) and a wall clock can be
off by minutes. C7-H1 and C7a had already put every local duration on a
clock that cannot err the dangerous way.

**Decision**: safety needs a rate bound per node and causality (a message
or an S3 object is read after it was written), and no bound on the offset
between nodes. Three readings of a node's clock (`constellation_types::clock`):
HONOUR never under-counts elapsed time (what a holder may honour is measured
on it), WAIT never over-counts (what a waiter outwaits), WALL is for status,
logs and identity keys; each of the first two is scaled by the node's rate
budget `ρ`, so a window measured on one node is exact for any oscillator
within it. Every cross-node decision is then a duration measured on one node
from its own send or receipt, or an observation of one object:
- *The lease* (`LeaseState::observe`/`classify`): a claimant remembers the
  renewal key `(holder, epoch, holder_clock, holder_wait_ms, released)` and
  the instant on its wait clock it first read it, and claims by TTL once a
  read still shows the key one holder's TTL (`ttl_ms` in the body) later; a
  released or fenced lease is claimable at once. A cold claimant pays one
  full TTL from its first read (about 60 s at the defaults).
- *The seal watch*: the holder's beats and appends name its own wait-clock
  readings, and the backup places the lease's end on its own wait clock.
- *Promises*: made for one lease key and counted per key; no clock of
  another node is compared.
- *Fencing tokens* (phase 1): the executor honours a token from its own
  receipt for what the holder's window had left, on its honour clock; the
  confirmation of a copied grant is the holder's own send reading. A token
  with no receipt (an S3 inbox batch, the replay queue) is judged exactly
  or refused.
- A suspend the kernel counts short is covered by a pad of a second per
  detected suspend on HONOUR only.
- *The cut and barriers between a root and a delegate*: decided to be
  root-anchored, as of the root's own request and reply (plan 43 C7b's last
  commit, after C4); not landed, they still compare the two nodes' wall
  stamps.

**Consequences**: the fault-injecting harness takes any offset (up to a
day) and its checkers subtract the offset they injected; the simulation's
default mix draws a constant offset per node in `[-2 TTL, +2 TTL]` and wall
steps up to 10 s. A cold claim waits a TTL from its first read
where it waited for the holder's wall expiry. What stays offset-dependent,
and says so: the lock cut and barriers (until the root-anchored cut), the
absolute stamp of a token when the operation's delay is not below `2 ×
margin`, garbage collection against S3's `Last-Modified` (a horizon of a
week), and a lease body without `ttl_ms` (an older build's, a singleton's).

**Rejected**: estimating the offset from beats or ReadIndex round trips
(NTP-style) and correcting each compare (it needs P2P, which the S3-only
paths lack, a fresh estimate that a step breaks, and a round-trip bound: a
delay assumption in every compare instead of one); detecting an offset and
refusing to act (undetectable without P2P, a liveness outage, and an
undetected offset is still unsafe); having a holder that learns it was
sealed stop admitting local sections (the traced violations came from a TTL
claim, not from the sealed holder).

**See**: [DESIGN.md §5](DESIGN.md#5-write-authority-the-one-rule),
[plan 43](../plans/v1/wip/43-commit-rule.md) (Appendix B, the clock
inventory), `crates/types/src/clock.rs`, `crates/authority/src/core/lease.rs`,
`crates/store-s3/src/heartbeat.rs`, `crates/meta/src/locks.rs`.

## ADR-36: A dentry copies its inode's attributes only while the inode has one name

**Context**: plan 28 §P6 gave every dentry (`0x02`) a copy of its inode's
attributes, so that a listing with attributes is one range scan, and plan 29
M3c kept the copies exact: every write of the inode record (`0x01`) rewrote
the copy under each of the inode's names in the same transaction. §P6 priced
that on "`nlink` is 1 for all but a rounding error of files". For an inode
with *n* names every `link`, `unlink`, `rename`, `chmod`, `utimes`, truncate
and write commit rewrote *n* keys, and building *n* links was O(n²): 7.4 ms
per `link()` at 2,000 names through the mount, one `stress-ng link` bogo-op
38.7 s and 773 MB of journal. The shape is real (pnpm's content store,
`cp -al` backup trees, dedup tools).

**Decision**: a dentry carries the copy only while its inode is a directory
or has exactly one name (`T = 1`). A non-directory with two or more names has
*remote* dentries, `ino` and `kind` (9 bytes), told apart from a full one
(57 bytes) by length; there is no primary name and no election. The form is
one function of the inode record, `DentryRecord::for_inode`, used by every
writer, local and replay, in the transaction that writes the record: a
1 → 2 transition rewrites the first name as remote, 2 → 1 writes the
survivor's copy, and at two names or more an inode record write touches no
dentry. A lookup or a `READDIRPLUS` entry of a remote name reads the inode
record (one point read); `MetaStore::lookup` keeps returning the attributes.
`fsck` reports a dentry of the wrong form (`dentry_form`); a repair is a
rebuild from the log.

**Consequences**: `link`, `unlink`, `rename` and every attribute or manifest
commit on an inode with *n* names cost O(1) keys, measured flat in *n* (0.86
to 0.92 times the single-name cost at 2,000 names). A listing of single-name
entries is byte for byte what it was, with no extra read; a listing with
attributes of hard-linked names pays one inode read per such entry (what
btrfs, ZFS, CephFS and Lustre pay for every entry), and `du` reads each
inode once. Two replicas with the same namespace hold the same dentry bytes
and root hash: `nlink` is in the namespace and the form is derived from the
result of its delta in the same transaction (plan 30 §M12's commutativity).
A state directory written before the change holds full dentries on
hard-linked inodes and is rebuilt (no compatibility shim).

**Rejected**: no copy at all (plan 28 §14.10's `nocopy`: every listing a
scan plus a point read per entry, 12,039 pack reads for a cold `ls -la` of
1,000 entries against 30); attributes in the dentry alone (`dentry-auth`:
doubles `getattr`); a copy for the first *K* names, a primary dentry, a copy
without `nlink`/`ctime`, or a threshold `T = 4` (each keeps an O(K) or
election path in `link`, `unlink`, `rename` and replay for a listing win on
a handful of names).

**Status**: decided by the maintainer 2026-10-22 (plan 45 §9, Q1–Q5, Q7);
built (plan 45 H1–H3).

**See**: [plan 45](../plans/v1/wip/45-hard-links.md) §4 and
[§9](../plans/v1/wip/45-hard-links.md#9-decided-by-the-maintainer-2026-10-22);
plan 28 [§P6](../plans/v1/done/28-s3-native-metadata-store.md#p6--key-encoding-the-most-load-bearing-table-in-this-plan);
`crates/mtree/src/record.rs` (`DentryRecord::for_inode`),
`crates/meta/src/store/ns.rs` (`put_inode_record`),
`crates/engine/src/mtree_read.rs` (`check_dentry_forms`). Amends plan 28 §P6.

## ADR-37: Name-ordered listings with keyed cookies, and `READDIRPLUS` entries that are lookups

**Context**: DESIGN §12 designed `READDIRPLUS` (attributes with each entry,
from the dentry copies plan 28 §P6 keeps for it) and stable readdir cursors
from phase 1, and nothing served either. A directory page re-read, hashed
and sorted the whole directory, quadratic over a full listing, and plus
pages (about four times fewer names per page) would have made that worse.
Every plus entry that carries a node id is a kernel lookup reference, so
getting the accounting wrong either reaps an inode the kernel still holds
or leaks an orphan (ADR-31).

**Decision**:
- *Listings*: a page is a name-ordered range scan of the directory's
  dentries, resumed after a name. An entry's cookie is a keyed hash of its
  name (SipHash-1-3, 62 bits by default, 31 on opt-in for 32-bit
  userspace) under a key derived, never rotated, from the master key under
  E2E and otherwise from `gossip_secret` (label `readdir/cookie-v1`); a
  filesystem with neither is refused at mount. A small per-node cache of
  recently served pages, keyed by directory and cookie (never by file
  handle), turns a cookie back into its name; a cookie no page holds costs
  one scan of the names. While a directory is open its pages are pinned
  (128 MiB per directory, 512 MiB per node by default, each open stream's
  last page always), and the pins cross a handover. Listings become name
  ordered: an entry created ahead of a running listing appears, one behind
  it does not, and a renamed name may appear twice. The remaining inexact
  case (a cookie whose name was removed and whose page is gone) answers
  `ENOENT`.
- *Plus entries*: `READDIRPLUS` is negotiated as `auto` by default
  (`CONSTELLATION_FUSE_READDIRPLUS`: `auto`, `always`, `off`). An entry is
  what `lookup(dir, name)` answers at that moment, waited for as
  `getattr` of the child waits, and counted as a lookup reference only
  once the page's reply succeeds. An entry the view cannot vouch for is
  name-only: under `--cto strict` on a node that is not a lone sequencer
  (zero TTL), a name changed between the page and its attribute read, `.`
  and `..`, and a pid-0 lister whose attribute-reply marks are full (plan
  42 F21). A hard-linked name's attributes cost one inode read (ADR-36).

**Consequences**: a full listing is O(n) and a page O(page) for plain and
plus pages alike (50 000 names: about 10 ms against about 900 ms for the
per-page re-read, on four file systems); `ls -l` of a kernel-cold
directory takes about one request per page instead of one per name;
`rm -rf`, `find -delete`, `telldir`/`seekdir` and a `daemon --upgrade`
mid-listing resume exactly while the directory is open. A cookie changes
nothing on the write path. The first upgrade across the change is a
remount (no compatibility shim). The counters (`readdir` block in node
status: `lost_cookies`, `cold_scans`, `pin_overflows`, and the plus
counters per mount) report the inexact and costly cases.

**Rejected**: cursors by position in a per-stream snapshot (memory per open
stream, lost at a handover or a restart); a per-handle hint cache (an
`nfsd` re-export or a second handle cannot resume); an NFS-style bad-cookie
error (a FUSE server cannot send one; `ENOENT` is POSIX's "position
invalid"); plus entries straight from the scan without the `getattr` wait
(they could install attributes older than what the node's clients have
seen); a notice per entry when the attribute-mark table is full (the
name-only entry costs the kernel one lookup instead).

**Status**: decided by the maintainer 2026-10-22 (plan 40 §9, the
questions marked resolved and the coder spec's decisions); built (P1–P8;
the default `auto` stands unless the R8 measurements fail it).

**See**: [plan 40](../plans/v1/wip/40-readdirplus.md) §4.5, §4.9 and §9;
`crates/engine/src/view/{listing,cookie,plus}.rs`,
`crates/frontend-fuse/src/adapter.rs` (`ReaddirPlus`); PROGRESS, "Plan 40".
