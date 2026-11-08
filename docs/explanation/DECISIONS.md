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
  deadline fails with `EIO`, never with a re-execution — *settled* first
  since plan 43 C2 (below).

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

**Amended (plan 43 C2g, 2026-11-01; §5.3 item 5, §11 Q9, Q15, Q21 (b),
(e), (j))**: an op settles before its client hears `EIO`. Its own attempts
end a settle window before the deadline (a quarter of it, derived: no knob);
then a *settle* of the rid — a `settle` flag on the forward, the inbox op and
the lease path, not a new op — goes the op's ordinary route from its one
client entry. Every sequencer treats it like the op it carries (ownership,
recall, a delegate's grant, the takeover gate), never waits for its `deps`,
and answers the rid's outcome, or journals `Refused { rid, ABANDONED_CODE }`
(`ABANDONED_CODE = Code::Canceled`: no new portable code, the errno table has
no hole) and answers `MutateOutcome::Abandoned` once the row is committed.
Every dedup site maps the refusal to `Abandoned`; the engine answers `EIO`.
A settled `EIO` means the op did not and will not take effect: any other
copy executes only at a later sequencer in force for its keys, which holds
the refusal first (its `completed`, a takeover's re-shipped tail, a
delegation's drained stream). No answer within the window is the unsettled
`EIO` (in doubt, as before; counted). An op that never left its node is
settled there at once. An answer in a continuation epoch's hold is the
hold's, uncommitted (C2f): its settled `EIO` is as strong as the hold.
Rejected: a second full deadline for the settle (240 s to an `EIO`), and a
`MutateOp::Abandon` (it would reach the fast path, the replay queue and the
inbox format).

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

**Superseded in part by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(plan 46 stage 2)**: the capture and before-images stay (recovery still
rolls back from them), but the server's *replay by rid* of stranded ops
goes. An unacknowledged stranded op has one copy, its requester's retry;
an acknowledged row is never stranded on a node with write-eligible peers,
and a singleton's or an operator-abandoned row becomes a conflict copy
exported by the node that holds its data, not a replay.

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
10% performance gate and stays on. `CONSTELLATION_HOLDER_CAPTURE=0` was
the internal fallback, which rebuilt a deposed holder from the shared log
instead; it was removed on 2026-10-31 (plan 43) because the off path no
longer kept the invariants: in the simulation it lost and duplicated
acknowledged writes, for a root and for a delegation. Capture is always
on; the rebuild remains for the rows a deposition's strand leaves. The Stateright model's `commits_are_log_prefixes` and
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

**Superseded in part by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(plan 46 stages 1–2)**: Layer A (`Local`, an acknowledgement resting on
the holder's disk, replayed by rid after a takeover) survives only for a
singleton. A node with write-eligible peers and no backup in budget
answers under a sticky non-stealable grant (final) or waits for the
archive; `AckPolicy::Local` and `ack=local` go. Layers B and C are
unchanged.

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

**Superseded in part by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(plan 46 stage 1)**: a grant goes to a *node* and covers a set of
subtrees under one stream, one group and one cut (`DelegateNode`,
`SubtreeReleased`, `DelegateNodeGroup`, `DelegateNodeEnded` replace
`Delegate { dir, node, gen }`, `Recall`, `RecallCut`, `DelegGroup`); no
delegate is lone; placement is sticky (a long dwell, recall by request)
and far writers hold non-stealable grants released by the holder. The
one lease, one log, the ownership walk, cross-subtree recall instead of
2PC, and the HLC-stamped shared parents stay.

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
  and unlinks hold their parent *shared*. (A directory could also be
  split into up to 16 name-hash ranges, GIGA+; plan 46 D5 deleted that, ADR-38.)
- Offline designations are non-stealable delegations: the same
  machinery without TTL expiry.

**Consequences**: a node writing its own subtree runs at local speed and
S3 request counts do not change, since there is still one log. The
cost moves to cross-subtree ops (a recall round trip, or a TTL wait if
the delegate is gone) and to the root, which must stay reachable to
append streams. The root lease stays put while any delegation or lock
grant is live. No delegation exists inside a continuation epoch.
(Hash-range splits of a hot directory, once part of this decision,
measured slower on real names and were deleted by plan 46 D5, ADR-38.)

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
model `crates/model/src/delegation.rs` (the model `hotdir.rs` went with the splits, plan 46 D5).
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
`backup_takeover_ms` of a proof of liveness, and a stuck core makes none: the
lane proves its own with one read of the lease object, off the core, dated at
its send and judged by holder and epoch as the core judges its own (plan 41
D3; ADR-25's amendment has the argument). Same pattern for lock grants:
ADR-25's amendment.

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

*The root's own proof of liveness (plan 41 D3)*: a tenure that can be taken
over early answers only within `backup_takeover_ms` of an S3 request that
proved it live, and a stuck core makes none. While the core is stuck (and
not hung), the lanes that find only that proof stale (the lock lane's own
renewals, the delegation answerer) ask for a read of the lease object,
off the core, at most one started per stagger (a quarter of the window),
overlapping, the next following a stagger after the last while a renewal
waits (amended 2026-10-31, below); a read that names
this node at the certificate's epoch is a proof as of its **send**, judged
by the unchanged freshness rule at the answer; any other reading withdraws
the epoch's proof and ends its answers. A successor takes the tenure only
by a lease CAS and quarantines from its marker, so a read served before
the CAS, and an answer within the window of its send, is covered exactly
as the core's own proof is.

*Amendment (2026-10-31): the proof's reads overlap.* One read per window
made the proof as likely to fail as one lease GET is to miss the window,
twice or three times in a row: when the GET's round trip nears
`backup_takeover_ms` (a slow S3), a root whose core stalled lost its
delegates' grants although it held its lease throughout (the simulator's
`delegated-core-stall-root-backup` under lease GETs of 0.8 to 1 s against a
1 s window: 249 of 2,000 seeds). Now a read is started at most once per
*stagger*, a quarter of the window: a renewal that arrives within a stagger
of the newest read joins it and a later one starts the next; while renewals
wait, the next read follows a stagger after the last whether or not another
renewal arrives (`RootProbe::follow_up`); a read that proves nothing hands
its renewals to the next one still out, and one that proves the tenure live
also wakes the older reads' renewals. At most four reads are out at once, a
GET per stagger at most and only while the core is stuck and a renewal
waits (a window at most after the last one asked). The argument is
unchanged: each read is dated at its own send and judged by the same rule,
so more reads change how often a proof is had, never what it proves. Not
closed: a GET slower than the window every time proves nothing, by design;
the residue is a grant renewed a few hundred ms late (1 of 2,000 seeds), and
a proof that does not ride the lease GET (the backup's answers to the
holder's off-core beats) is the follow-up.

*Not covered, on purpose*: a **stalled owner's** peers' grants (answering
their renewals off the core, and the restart horizon's lead: follow-up L5);
`ack=s3` holders, deposed on the core's silence;
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
[plan 40 §4.4](../plans/v1/done/40-readdirplus.md). Amends plan 31 §6.11.

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

**Amended 2026-11-08** (plan 43 Q13): a self-sealed holder whose sealer's
claim cannot land re-claims at once, at the next epoch from its member copy;
one whose sealer's claim can still land waits the sealer's grace, not a TTL.

**Amended by [ADR-41](#adr-41-a-delegate-counts-the-roots-copy-only-through-rows-the-roots-tenure-has-committed-interim-rule-for-q16)
(2026-11-02)**: the root's copy in the group `{delegate, root}` counts only
through the rows its own tenure has committed, not its journaled cursor.

**Superseded in part by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(2026-11-01)**: the rule stands; what changes is its enforcement. The
commit rule becomes a property of the configuration (two row states,
committed-when-answered and final-without-group, instead of five classes
decided per row); the *lone* exception shrinks to a singleton (a node with
write-eligible peers never answers from its disk alone: a sticky grant or
the archive instead); the epoch hold waits for one member; C3's verbatim
re-ship is redone under one stream per node, and C5's lone branch is
deleted rather than isolated. The C1 and C2 paragraphs below describe
what landed and stays.

**Status**: accepted by the maintainer on 2026-10-17 (plan 43 §11: every
recommendation, and the latency gates G1–G6), built in steps. Landed: C0
(the commit point named and checked), C1-1 to C1-4 (the root group's commit
holds across a listing and a removal; the class at journal time; the TTL
claimant's sealed tail and the inherited tail; member copies, committed
shadows and streams, tail identity: nothing committed is re-executed), C2's
C2a to C2g (the answer's source; a generation's end carries its cut; a
delegate acknowledges once its group holds the row; its fast path writes and
then waits; the end of a generation waits for its group; a lone root answers
other nodes from the archive; an op is settled before `EIO`, ADR-18's
amendment), C2g′ and C2h's scenarios (the settle's classifier and the ratchet
of plan 46 M0.3; the strict flags stay per-configuration, D9 defers the
rest). Not yet: C3–C5 as redone under plan 46 stages 1–2. Each milestone amends this
record; C1's rejected options are below.
DESIGN.md states each part in the milestone that makes it true (plan 43
§6).

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

**See**: [plan 43](../plans/v1/done/43-commit-rule.md) §3-§5 (the terms, the
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
abandon-group` lets it go on without. The end of a delegation is the same
case (C2e): a generation whose latest `DelegGroup` record names backups is
ended only once one of them has sealed and the root has appended its whole
tail (older versions' backups are sealed once, never waited for), and when
none can answer it waits, `waiting_for_group`, until the operator's `repair
abandon-group --gen` ends it at the root's cursor and writes `GroupAbandoned`
into the log. A tenure that re-ships a tail names
the old group in its lease (`inherited_tail`) until the log carries the
rows, so that a successor's death does not take the last copies of rows
the old group acknowledged. The lease, the log and the seals are the only
evidence: no claim compares clocks beyond the observation rule of C7b.

**C1-4 (plan 43)**: nothing committed is re-executed. A deposed group tenure
keeps the rows its clients were told are committed as a **member copy** of
their epoch (a key range of the `backup_tail` keyspace of its own) instead of
queueing them for replay; a backup whose holder's lease moved on to a later
epoch keeps its tail the same way; a successor keeps the copy of the tail it
re-ships. A copy leaves only when the log carries the rows' identity (each
re-shipped transaction opens with `LogRecord::TailOf { epoch, jseq }`, kept
through any number of re-ships), when a marker's `TailFollowsThrough` says
the sealer's hold did not reach it (never committed), or, for the rows
nobody acknowledged, at the first marker of a newer epoch that announces no
tail of theirs. A requester's shadow or hint answered as committed and a
subscriber's transaction streamed as committed survive a takeover of their
epoch and retire by the re-ship; only `repair abandon-group` strands them and
queues the copy, the one re-execution of a committed row. Rejected: replaying
the committed rows from the deposed holder and relying on the dedup by rid
(the replay races the retry of the same rid and the ops invoked between: two
queues for one op); keeping the member copy in the journal (a journal row is
shipped by its holder, and the successor already ships the row); dropping a
copy at any marker without a tail announcement (the successor may itself be
replaced before it ships, D2). The formats are append-only: `TailOf` and
`TailFollowsThrough` are new `LogRecord` variants, `TailFollows` stays
decodable.

**C2c (plan 43)**: a delegate acknowledges once its group holds the row. The
generation's configuration is a log record, `DelegGroup { dir, gen, version,
backups }`, journaled by the root when the delegate's renewal asks for a
different backup (the request is the renewal's `backup`); with no record the
group is the delegate and the root, and the root's journaled append of the
row (`DelegateStreamAck.through`) is the second copy. The delegate learns its
group from its own replica's table only, never from a message: a
configuration counts once its master has it durably. While a change it asked
for is not recorded yet an acknowledgement needs the members of both versions,
and a backup leaves the record only once every index answered through it is at
or below the root's cursor or in the log. A delegate is **lone** when its
table records no backup, none is requested or appended to, and no peer — the
root included — is a backup candidate (linked, within the RTT budget, stable);
it then answers its own node's client from its disk and every other node's
forward from the root's append, and its fast path stays open only then. Its own
node's rows are answered from its disk only while its journal is within a
bounded lead of what the root holds (as many transactions as a stream batch
carries records, `delegation_stream_rows`); past the lead they wait for the
root's append too (2026-10-29: without the bound, its own node's create loop
ran the journal minutes ahead of a stream that carries one batch per round
trip, and every other node's forward, answered behind every earlier row,
starved until the client deadline answered `EIO`; rejected: answering
forwards from the delegate's disk, the speculation this ADR removed). The
lead is exact, not approximate: the core publishes the horizon (the committed
prefix plus the lead) with the class, and the fast path classes each row by
its own stream index against it under the writer lock, so what the fast path
journals between two core steps cannot pass the lead (the error only ever
goes toward the stronger class). The core's own ops are classed the same way
where they are journaled: the core decides an op's class from the journal as
it stands and then waits for the writer lock, and the fast path's rows go in
meanwhile, so the store classes the core's own row again by its place in the
stream against the committed prefix and horizon the core knew (which only lag
the truth), journals `DelegGroup` where the decision no longer holds, and the
core answers by the class the row was journaled with, parking it like any
`DelegGroup` row (2026-10-30: found by review; a rename the core decided
`Lone` was journaled past the lead and behind a create the fast path had
journaled `DelegGroup`). The lead flips a lone delegate between `Lone`
and `DelegGroup`, so a `Lone` row also waits for the gated rows before it:
while a gated row journaled for the delegate's own node's clients is
uncommitted, its later rows are `DelegGroup` too (2026-10-29: a create that
waited for the root, then a rename of it answered from the disk, and a root
crash drops the create at the strand and replays the rename, which fails on a
name never created; I5 allows this, the answer was `Lone`, but nothing is
gained by it; the same held for a delegate that turned lone after journaling
`DelegGroup` rows). The rule spans the node's generations, not one: a node
could hold two generations whose keys meet with neither ending (two ranges of a
split directory, or a range and a whole subtree; ranges are deleted, D5: an unlink of a file's primary
link moves the file's own keys to the generation of its next link), and a
takeover ends all of them together, so an open gated row of any of them holds
the `Lone` rows of the others (2026-10-30: found by review; a `setxattr` past
the lead, the unlink of the primary link, then a `removexattr` answered `Lone`
under the other generation, replayed alone at a root crash). Another node's
forward is not counted among those rows,
though holding the own rows behind it would be safe too: I5 allows rolling a
`Lone` answer back, so excluding forwards is a choice, made for cost. A
forward is `DelegGroup` by design, a delegate that serves remote nodes always
has one in flight, and holding its own rows behind every forward would take
the speculation from it altogether. The
class is fixed when the row is journaled, a retry's answer waits for its row
like the first answer did (D6), and the end of a generation drops the
uncommitted rows past its cut instead of replaying them (their clients were
never answered), keeping only a lone delegate's rows for replay. A generation
whose latest record names a backup is not ended before that backup answers its
seal: rows acknowledged through the group may be nowhere else (Q1, which C2e
completes with the operator's escape). A delegate's own copy is its journal,
whose commits reach the OS but not the disk: a process that held a generation
before a reboot may lack rows the group acknowledged, so it does not take the
generation up again (it records, synced, the boot of its first grant), and the
generation ends by the backup's tail or at the root's cursor; a restart on the
same boot finds the journal intact and renews (2026-10-30, C2e follow-up: the
renewal that ends the root's wait for the backup, and any renewal before the
grant ran out, used to hand such a delegate its generation back, its next rows
reusing indices the backup held). Rejected: counting a backup from the
renewal that names it, or from the root's uncommitted record (the record could
be lost at the root's deposition and the root would seal a different member
than the one clients were answered by); letting a retry's dedup answer leave at
once (it would acknowledge a row the generation's end may still drop). The
format is append-only: `DelegGroup` is a new `LogRecord` variant and the
table's persisted row is unchanged (a key of its own per generation).

**C2c follow-up and C3, in part (plan 43, 2026-10-31)**: a delegate's
committed answer is committed wherever it is held, and the delegate taking
the root over re-appends its own rows verbatim. A requester's shadow or hint
of a delegate's committed answer is a committed entry, as a root's is: a
root takeover does not roll it back (the delegate re-streams the row to the
next root, whose append takes the shadow out in place), and the takeover gate
takes the successor's own such shadow out only if the tail it re-applies
carries the row. A subscriber's stream entry of the root's append of a
delegate's row is committed as the root's own rows are, and the successor's
re-ship of such a row leads with its `TailOf` too, which the stream entry and
a deposed root's member copy retire by. A delegate that seals the root and
takes it over re-appends the rows of its own generations that the tail does
not carry after the tail, under their stream origin and in index order (C3
§5.4 item 3, without the member-copy move: the strand still rolls them back
first, and a generation the tail ends keeps the replay); they were executed
again by rid, and reached the log as root rows of the new tenure. (Found by
the `placement-hot` sweeps: I2c, I5.) Not yet: a deposed root rebuilt from
the log still queues the rows it appended for other generations as
`Records` replays (C3 §5.4 item 4). A deposed root's own journal copy of such
an append also retires once a later tenure's segment lands its outcomes under
another identity (the C3 follow-up of 2026-11-05 below).

**C3 follow-up (2026-11-05, seed 2560 of long-delegated-late-answers)**: a
root's journaled append of a delegate's row retires by its origin (a segment
that names `(gen, idx)`), strands at the generation's recall, and now also
retires when a segment of a later tenure carries every outcome the append
records (`Completed`, `Refused`) under another identity, by rid and without
the origin (`superseded_root_appends_tx`). It is rolled back, the segment's
records apply in its place, and nothing is queued: the log already holds the
rid's outcome and effect, so a replay could only be answered from the dedup,
and shipping the copy with this node's next tenure would land the rid twice
(I2) and apply its records over later state (log replay would diverge from
the replicas). The rule is the one a subscriber's stream entry of the same
append already had (`completed_streams`). An append with an outcome the log
lacks stays whole: it may be that outcome's only carrier. The log's first
outcome for a rid is the rid's, even where it disagrees with the append's (a
rescue by rid that refused what the delegate completed): that divergence is
L2b's, upstream, and plan 46 M1.2 D1(iii) (a cut that includes the group's
rows, appended with their origin) would end the non-verbatim landing itself.
Not changed: a delegate's own rows (epoch 0), member copies, backups' tails,
ADR-41's committed prefix.

**C2f (plan 43, 2026-10-31)**: a lone root answers other nodes from the
archive, and the fast path answers on its disk only where it is lone. The
class of a root's answer depends on who asked: a lone root (no listed or
doubted backup, no candidate, no eligible or unassessed peer, no epoch,
not `ack=s3`) answers its own node's client `Lone` and every other node's
forward, inbox op or retry `Archive`: the row is journaled `Archive`, so a
deposition keeps it as a member copy instead of queueing a replay beside the
requester's retry by rid (one identity per op), and the answer leaves once a
segment carries the row, `committed`. An answer parked for the root group
whose backup set empties while it waits is never released to another node on
the root's disk alone (no `Lone` re-tag for a remote): it waits for the
archive too; a continuation epoch's hold still answers at once (never a wait
for S3 there). A row of another node's op journaled `Lone` before this rule
is re-tagged `Archive` when another node asks for it again. Another node's
retry answered from an earlier execution waits for that execution's own row
in the log, not for the journal's tip (rows ship past a deferred one); a row
of a continuation epoch's hold is non-stealable only while the hold lasts;
and an answer parked for the log whose row was rolled back meanwhile (`repair
drop-held`) is aborted, not released, when the log covers its seq (C2g′: at
once, when the drain finds the dropped row's placeholder). An operator's
drop re-executes only this node's own rows; another node's unanswered row
is re-executed by that node's retry, in the dropped row's place, or
abandoned after 2 × TTL (C2g′, plan 46 M0.2: the row stays in the replay
queue as a placeholder, the drain holds what depends on it behind it, an op
arriving on its keys waits behind the queue, and the bound settles the rid
with C2g's record; a backed root's row is a placeholder only while its answer
is still parked for the group, which the core, where the drop now runs,
knows, and the retry takes the dropped row's place only while the dropping
node remains the sequencer). In a
continuation epoch's hold an answer to another node is the hold's
(non-stealable, uncommitted: a non-stealable answer is never said to be
committed, the maintainer's decision), its row re-tagged so. A node answers its own clients alone only while it is a
lone root or holds an epoch: the core says so with its gate, and says it no
longer may before the replica stops being the holder's. The FUSE fast
path's gate-off answer (`Ungated`) is given only where the core published
that the node is lone or holds an epoch; the gate off for any other reason (a
release, a handoff after the write) is in doubt, counted
(`ack.ungated_unexpected`) and resubmitted by rid. The cost is one S3 commit
on every write another node forwards to a lone root (plan 43 §11 Q2,
accepted). Rejected: answering the forward from the disk and keeping the
requester's replayable shadow (Layer A as it was: the answer rests on one
disk, and the deposed root's replay and the requester's retry are two copies
of one op, queued at once on two nodes).


**Plan 46 M1.1 step 4 (2026-11-05): the fast path carries the version, the
store no longer classes a row again by its place.** A delegate's fast path
publishes `(rule, version, through, horizon)`: under the writer lock the
store stamps the published rule only while the version it names is the one the
store records (the latest `DelegGroup` record applied), and, for a `Singleton`
rule, only while no gated row of the node's own that the published prefix does
not cover stands before it (the hysteresis, across the node's generations) and
the row's index is within the published horizon (the committed prefix plus the
lead: a lone delegate's own load stays bounded by what its stream carries until
step 6 takes the lead out of the commit). Anything else refuses the op
unwritten, and the core, which reads the configuration when it stamps the
row, decides it. A change of configuration between the publication and the
journal therefore reaches the core instead of being stamped with the version
of a rule that is no longer in force. The core's own op is stamped as it
decided, with one exception: it decides from the journal as it stood and
journals once it holds the writer lock, and the fast path's rows go in
meanwhile, so a `Singleton` row of the node's own clients that would land
behind an open gated row of the node's own is stamped `Group` at the same
version by the store, and the core reads the stamp back and waits for the
group (a `Singleton` row journaled behind a gated row is replayed without it
at a root's crash, after its client was answered). The core publishes the rule
it is about to stamp before it journals, so the fast path's rows that go in
while it waits carry the rule it decided and not a previous step's. The rows
a takeover re-journals (the predecessor's tail, the delegate's own rows)
carry the lease version of the successor, the configuration of the journal
they are rows of and not the commit configuration of the group that
committed them (step 5 must not read them as such), and the rows the root
appends of a delegate's stream carry the generation's applied `DelegGroup`
version (read in the journaling transaction, `Singleton` tag), never a
version-0 placeholder.

**Plan 46 M1.1 step 5 (2026-11-06): reconfiguration commits under old and
new, uniformly; the singleton transition is a lease bump.** A row keeps the
rule of the configuration it was journaled in, and a change of the
configuration commits its position under the old rule and the new one, every
later row under the new one. For the root, a configuration is the held
lease's backup set, acknowledgement policy and singleton flag
(`Lease::singleton`, new): when a landed change (a reconfiguration, a
singleton bump, an adopted re-read of one) replaces it, the replaced
configuration is closed at the journal's tip (`AckState::spans`), and nothing
past that tip is acknowledged before the closed configuration's own rule (its
backups' holds, or the archive) has committed the tip: `durable_jseq` is
capped by every closed configuration not yet covered, a singleton's closed
configuration never caps. For a delegate generation the same holds for the
`DelegGroup` record (`DelegateState::group_spans`): a row journaled under group
v1 is committed by v1's members, or the log, after v2 is applied (the root, for
a group with no backup). The singleton transition is the event-time test
(`backup_could_be_had`, which now counts a node that has sent the root a
request: a forward, a settle, an inbox batch, until a roster read or a directory snapshot issued after the request says otherwise) acting through the lease: when it
flips against the lease's flag the root issues a reconfiguration CAS of the
same backup set that moves the flag (`singleton_maintain`, in the event that
flipped it); the answer-alone rule needs the lease to say singleton, the test
to say singleton, and every closed configuration covered. Nothing is answered
alone from the trigger until the bump lands and the archive carries the
journal through it, on every path: the core's answers and the FUSE fast path's
alike (the published `lone_ok` is off while a closed configuration is
uncommitted, so a `Singleton` row journaled there waits like a gated one) (the
C8 model's conditions 1 to 3: peer to singleton waits for the archive,
singleton to peer from the event); the claim CAS of a tenure
stamps the flag from the claimant's test at that moment. The per-event flips
are gone: a gated row of a configuration that a change replaced is no longer
re-tagged `Lone` when the gate goes off (it is answered by its own rule once
the change that replaced it is covered); the re-tags that remain are the row of
the configuration in force answered on this disk alone (a row journaled before
the event's singleton test was assessed that the test then finds lone) and a row
of any version answered in an epoch's hold (a deposition replays it, as the answer said),
in the version the row was journaled in, never the placeholder 0
(`Meta::reclass_journal_tx`; the sim oracle judges every re-tag), and
a row journaled under the singleton's configuration is stamped `Singleton`
from the moment the lease says so though its answer waits for the closed
configuration. A claimant is in the roster (C8 condition 4): the admin leave
records the retirement in every lease, fencing the one the node held
(`Lease::fenced_for_retirement`) and marking the others
(`Lease::with_retired`), and `classify` refuses a claim by a node a lease
lists as retired, so a node retired while it was not root cannot claim either
(a lease object created after the leave is not covered: partitions' leases
exist from the filesystem's creation, and the node's own registry poll stops
it within seconds); the remaining loss is Q6's accepted window (a singleton's own
clients' rows journaled while its view had no peer, exported on return).
Rows a takeover re-journals carry the successor's version and are not read as
commit configurations by any of this (only the lease and the `DelegGroup`
records make a configuration).

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

**See**: [plan 42](../plans/v1/done/42-kernel-race-workarounds.md);
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
[plan 43](../plans/v1/done/43-commit-rule.md) (Appendix B, the clock
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
built (plan 45 H1–H4).

**See**: [plan 45](../plans/v1/done/45-hard-links.md) §4 and
[§9](../plans/v1/done/45-hard-links.md#9-decided-by-the-maintainer-2026-10-22);
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

**See**: [plan 40](../plans/v1/done/40-readdirplus.md) §4.5, §4.9 and §9;
`crates/engine/src/view/{listing,cookie,plus}.rs`,
`crates/frontend-fuse/src/adapter.rs` (`ReaddirPlus`); PROGRESS, "Plan 40".

## ADR-38: Automatic directory hash-range splits are off by default

**Superseded by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(plan 46 M1.2)**: the ranges are deleted by plan 46 D5 (done in M1.2
step 1), not kept as an expert setting: `CONSTELLATION_DELEGATION_SPLIT`,
`delegate --range` (clap refuses it), the `range` of the delegation
record and table (`DirOwner::Split`, `GenState`), `place_splits` and the
placement's split half, the model `hotdir`, the harness scenarios
`hash-range-split-merge`, `cross-range-rename` and `two-gens-ack-cost`,
the `shared-dir-split*` sims and the benchmark's configurations `b` and
`c`. It was the main source of a node holding several generations. The
text below is the decision as it stood; the evidence stands as the reason
for the deletion.

**Context**: plan 30 §M12 lets the root's placement split a hot shared
directory into 2 to 16 name-hash ranges (GIGA+) and delegate each range to
the node that dominates it, with `CONSTELLATION_DELEGATION_SPLIT` (20 %) as
the share that makes a node a candidate. The split was on by default. The
directory-split benchmark (`bench/dirsplit`, T1 to T5 with 2, 4 and 8
writers, a crossover sweep over the root's round trip of 2 to 60 ms at S3
20 and 80 ms, three repetitions per cell) measured what it buys.

**Decision**: the placement does not split directories by itself:
`CONSTELLATION_DELEGATION_SPLIT` defaults to `0` (`placement_split_pct`).
The split stays as an expert setting (`CONSTELLATION_DELEGATION_SPLIT=20`
turns the automatic split back on) and `constellation delegate --range`
splits by hand whatever the setting. Whole-directory and subtree
delegation, which the placement still does by itself, is the recommended
tool for writers far from the root.

**Evidence** ([results](../../bench/dirsplit/RESULTS.md)):
- With random names (creates, create and unlink, renames), a hand split
  ran at 0.01× to 0.91× the throughput of no split in every topology, at
  every writer count and every root round trip measured, usually below
  0.2×. At 4 writers: 0.06× in one AZ at S3 20 ms, 0.02× at S3 80 ms,
  0.34× with every node in its own region, 0.72× with the writers
  together and the root 60 ms away.
- The crossover sweep shows the gap narrowing as the root moves away
  (0.13× at 2 ms, 0.44× at 20 ms, 0.70× at 60 ms, S3 20 ms) but not
  closing, and a slower S3 widens it (0.04×, 0.15×, 0.38× at S3 80 ms).
- Why: a create of a random name lands in the writer's own range with
  probability 1/N, so most creates are forwarded to another delegate, and
  an op in one range waits for the other ranges' streams through the root's
  S3 log, about 145 ms at S3 20 ms and about 500 ms at S3 80 ms. A split
  adds a range-to-range ordering cost and removes nothing.
- The automatic placement never split a directory of random names (it
  delegates a range only to a node that dominates it) and measured the same
  as no split there.
- A split pays only when every writer's names hash into its own range
  (synthetic): 1.2× to 3.3× within one AZ, up to 60× far from the root. No
  application chooses names by their hash.
- Delegating the directory whole to one writer ran at 12× to 39×
  (writers together, root far away) and 26× to 209× (every node in its own
  region) the throughput of no delegation.

**Consequences**: a default mount never splits a directory, so the
costs above and the split-only behaviours do not apply to it: no read
delegation on a split directory (every lookup in it asks the name's
sequencer), and cross-range renames recalling both ranges. They matter
only to those who enable splits or split by hand. Turning splits off does
not by itself rule out a node holding two generations (a hand split, or a
whole directory beside a range): a row of one that depends on a row the
other ended without is never acknowledged, appended or drained
([ADR-39](#adr-39-a-delegates-group-acknowledgement-waits-for-the-rows-dependencies-on-its-other-generations),
[delegations](../reference/features/delegations.md#a-row-whose-cause-was-lost-with-another-generation)).

*No splits under `--cto strict`.* The listing of a split directory is
answered by one sequencer, whose position does not cover the other
ranges' streams, so a strict `readdir` can miss a name another range's
delegate just created
([cto-modes](../reference/features/cto-modes.md#known-gaps)). The
maintainer chose to forbid splits in strict mode rather than build a
fan-out (a ReadIndex at every range's delegate and at the root, the reader
waiting for all of them), for a feature that does not pay off on real
names. On a node with a strict mount the placement never splits a
directory, whatever `CONSTELLATION_DELEGATION_SPLIT` says (one warning at
mount), and a hand split (`constellation delegate --range`) is refused
with an error; whole-directory and subtree delegation stay allowed. The
check is the node's own mounts', so a file system with strict mounts on
other nodes leaves the setting off on the nodes that can hold the root
lease. The sims, harness scenarios and the
benchmark that cover splits set the knob explicitly
(`hash-range-split-merge`, the `shared-dir-split` sims, bench
configuration `b`); a sim checks that nothing splits by default. The
mechanism stays maintained, as an expert setting.

**Rejected**: keeping 20 as the default (a split that never paid off on
real names, and that a workload with name locality by chance could trigger
at a measured loss); removing plan 30 §M12 entirely (the synthetic case
shows a split can win, hand splits have users who know their naming, and
the sims and model that check ranges guard code shared with the recall and
cross-subtree paths); off by default with the knob kept (chosen).

**Status**: decided by the maintainer 2026-10-29 (the directory-split
benchmark's recommendation); built.

**See**: [bench/dirsplit/RESULTS.md](../../bench/dirsplit/RESULTS.md);
[Delegations](../reference/features/delegations.md#hot-shared-directories);
[configuration](../reference/configuration.md); plan 30 §M12 in
[PROGRESS](../plans/v1/PROGRESS.md); `crates/authority/src/core/mod.rs`
(`placement_split_pct`), `crates/authority/src/core/placement.rs`
(`place_splits`).

## ADR-39: A delegate's group acknowledgement waits for the row's dependencies on its other generations

**Superseded by [ADR-40](#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)
(plan 46 M1.2)**: with one stream per delegate node there is one total
order and one group per node, so a dependency across generations of one
node cannot exist; `deleg_caps`, `deleg_note_xdeps`, the xgen counters
(`status.delegation.xgen_*`) and the cross-generation handling of plan
45's key move (the open-elsewhere clause and its store half
`gated_open_elsewhere_tx`) were deleted in plan 46 M1.2 step 12. The key
move itself stays: inode keys follow the owner of the primary link's
parent, and an unlink that moves them is an ordinary single-owner op. The rule
below is right for the state it was built for; the state is removed.
(`SessionState::deleg_covered` stays: it is the fast path's
covered-for-good watermark, not part of this rule.)

**Context**: one node can hold two generations (the ranges of a split
directory, or a whole directory beside a range of another), and every op
of the node carries the node's whole session as its `deps`, its own rows
in every generation included. A `DelegGroup` acknowledgement waited for
the row's own generation's group only
([ADR-33](#adr-33-commit-is-the-configured-groups-acknowledgement-vertical-paxos-over-s3)).
So a row of B could be acknowledged through B's backup while it depended
on a row of A that was not committed, and when A then ended below that
row (reclaimed at the root's cursor; the row strands and is replayed by
rid, or never), B's end had no right answer: appending the row puts it in
the log ahead of its cause, or without it (the root's drain of B's sealed
tail did, by the void rule), and ending B before it drops rows the backup
acknowledged (the live recall did, once the stream stopped at such a row).
Waiting for the dependency waits for ever: nothing is appended at a voided
position.

**Decision**: a `DelegGroup` acknowledgement of row `(B, k)` waits until
B's group holds it and every row of B up to `k` has its dependencies on
the node's other generations committed by the same rule, computed to a
fixpoint over the node's generations (`Core::deleg_caps`); a dependency
lost with its generation aborts the acknowledgement. Then every row from
the first one with a lost dependency on was never acknowledged, and both
the live end (the delegate stops the generation before the row, its
recall answers the row before, the root appends no such row) and the
sealed tail's drain end the generation before it. The dependencies
counted include those on a generation the node no longer holds whose
rows are still in its journal and whose end it has not applied from the
log (committed only once the log carries them), and a row whose
dependency is lost when the delegate reads it for a batch stops the
generation before it, before any member holds it. The dependency is the
whole session's, not narrowed to the keys the row touches or the rows
the op read: the narrowing would let a causal dependency through (a read
of `/a/x` that wrote `/b/marker` shares no key) and the cost of the whole
session is small (below).

**Evidence** (the simulation, release build; the harness on one box):
- `delegated-two-gens-one-backed` (deleted with the ranges, plan 46 D5;
  node 2 held `d1` backed and a range of
  `d2` unbacked, wrote in both at once, lost the root and died): with
  the rule off, 40 of 200 seeds answered a write the log never carried
  (`returned success but completed 0 times`); with it, none of 2,000.
- The cost in the simulation, `delegated-two-gens-backed` (the same node,
  both generations backed by one node, four writers per node, no fault),
  seeds 0 to 1,999: 2,306 of 189,551 delegated acknowledgements (1.2 %)
  waited for the other generation's row, 12 ms on average (27,721 ms in
  all, 0.77 % of the 3,609,653 ms the acknowledgements waited
  altogether), 1,017 ms at most.
- The cost on real processes, the harness's `two-gens-ack-cost` (b holds
  `/d1` and range 0/2 of `/d2`, both backed by c; four threads on b create
  150 files each, two per directory), three runs against the same
  scenario with the previous daemon: creates p50 11.2, 11.2, 11.7 ms
  against 11.5, 11.6, 11.3 ms, p99 18.1, 18.0, 17.9 ms against 19.4, 19.5,
  17.2 ms. No difference above the run-to-run spread.
- A node with one generation, or with generations whose writers do not
  overlap in time, pays nothing: the rows' dependencies on the other
  generation are committed before they are read.

**Consequences**: the acknowledgement of a row on a node that writes two
generations at once can take the slower of the two groups; `status.delegation`
counts those (`xgen_acks_delayed`, `xgen_ack_delay_ms`,
`xgen_ack_delay_max_ms`). The delegate records the dependencies of each
row as it reads it for a batch and forgets them once the log carries the
row. The drain of a sealed tail still waits for a dependency that is only
not here yet. `Lone` and non-stealable rows are unchanged.

See: [Delegations](../reference/features/delegations.md#a-row-whose-cause-was-lost-with-another-generation);
`crates/authority/src/core/delegate.rs` (`deleg_caps`, `deleg_note_xdeps`,
`deleg_drain_tail`).

## ADR-40: Every acknowledgement is committed or final; one stream per delegate node; no server-side replay

**Status**: decided by the maintainer on 2026-11-01; to be built in the
stages of [plan 46](../plans/v1/wip/46-committed-or-final.md). Each stage
amends this record with what landed.

**Context**: six weeks after ADR-33, the acknowledgement and
ordering-node-change area had produced 55 bug rows (41 integrity), 239
`fix` commits in `crates/authority` and `crates/meta` since 2026-09-18, a
37 % induced-defect rate (19 of 51 closed rows were introduced by changes
in the area; 9 of 13 first reviews did not clear), 16 rows found by review
with no oracle firing (the sim never reached the states), and non-test
core code grown from 22.4k to 46.5k lines. The rules of ADR-33 and plan 43
are right. The bugs come from the structure that enforces them: (1) the
commit rule decided *per row* by a five-way class function over mutable
state (`AckClass`; rows 24, 29, 38, 39, 48, 51 of the review are the class,
the writer lock, the lead and the published horizon disagreeing by one
step); (2) an acknowledgement the *server* can take back, so the server
keeps a second executable copy of every op and member copies with four
drop rules exist to exempt committed rows from it (11 rows are copies
racing); (3) a delegate as a second sequencer whose configuration lives in
an uncommitted log tip, with one node running several generations whose
rows depend on each other (ADR-39's fixpoint; a fix series dropped
acknowledged rows twice); (4) authority state in messages (plan 43 C4, the
right fix, unbuilt). A survey of other systems (Lustre, CephFS, HDFS QJM,
Aurora, FoundationDB, Spanner, CockroachDB, TiKV, Calvin, Raft, NFSv4.1,
Speculator) found none that acknowledges and later rolls the
acknowledgement back: they acknowledge after a durable quorum, or the
*client* keeps and replays the request while the server only truncates,
or commit is a provable function of durable state; in all of them the
commit rule is a property of the configuration, fixed for a tenure.

**Decision**:
1. **Every acknowledgement is committed or final.** A row is either
   committed when answered (the configuration's group holds it, or the
   archive does) or final without a group (a non-stealable authority: a
   designee, a sticky grant, an epoch hold with one member). Nothing
   acknowledged is ever rolled back. The one exception is a **singleton**
   (no write-eligible peer in the roster), which answers from its disk:
   nobody else can take its lease while it lives, and its restart
   re-adopts its journal.
2. **The commit rule is a property of the configuration** (the lease for
   the root, the node stream's group record for a delegate), changed only
   by a reconfiguration that commits under old ∪ new. A row carries its
   configuration version; `RowCommit::{Group{version}, Final}` replaces
   the five classes and every per-row decision site.
3. **One stream per delegate node**, covering a set of subtrees:
   `DelegateNode { node, gen, dirs }`, `SubtreeReleased { node, gen, dir,
   at }` (a row in the delegate's stream), `StreamReleased`,
   `DelegateNodeGroup`, `DelegateNodeEnded` replace `Delegate`, `Recall`,
   `RecallCut`, `DelegGroup`. Hash-range splits and ADR-39's
   cross-generation machinery are deleted. **No delegate is lone**: a node
   is delegated only with a group within the RTT budget or under a
   non-stealable grant.
4. **Far writers hold sticky, non-stealable grants**, taken automatically
   by placement; the root takes them on the subtrees it writes itself
   (never on `/`) when it has write-eligible peers and no group. The root
   never times a sticky grant out: the holder releases (idle, default
   about 60 s; clean unmount; suspend, hibernate and shutdown through a
   systemd-logind delay inhibitor; planned network-down through a
   NetworkManager pre-down dispatcher; a cooperative handover;
   `constellation release <fs> [--wait]`), or the operator abandons. An
   optional self-fencing timeout is a setting, off by default. Placement
   is sticky (a long dwell; recall by request).
5. **The epoch hold is a group {owner, one member}**: its answers wait for
   one member's acknowledgement of the stream-ahead; a one-member epoch's
   hold is final.
6. **The server never re-executes.** The replay queue, `core/replay.rs`,
   `meta/replay.rs`, the fold, `derive_replay_op`, `Origin::Replay`, the
   replay half of drop-held and `ack=local` (`AckPolicy::Local`) are
   deleted. The requester's in-doubt retry by rid is the one copy of an
   unacknowledged op (the root's own op parked at a deposition takes the
   same in-doubt path the delegate's fast path takes today; plan 46 §5.5).
   A rolled-back *acknowledged* row exists only on a singleton or after an
   operator's abandon or a self-fence, and the node that holds its data
   exports it as a conflict copy (a new op), never as a replay. Conflict
   copies stay the artifact.
7. **Stages**: 0 — plan 43's C2g, C2g′ (drop-held option B) and C2h's
   scenarios without the flex strict flags; 1 — the Stateright model
   first (two-week timebox; if not proven, C3 as written goes first), then
   classes at the configuration, one stream per node with C3's verbatim
   re-ship folded in, sticky grants and placement, the hold as a group;
   2 — delete server-side replay and `ack=local`; 3 — C4 as specified,
   then C7b-8's root-anchored cut.

**Consequences**: a far writer's local speed is conditional on a sticky
grant (automatic), and a node lost mid-write blocks its subtrees for
other writers until it returns or the operator abandons, the failure mode
already accepted for groups (plan 43 G4) and designees (ADR-6). The root's
first op in a new subtree of its own waits one archive round trip for
the grant record; its ops directly in `/` wait for the archive when it has
peers and no backup. One LAN round trip inside a continuation epoch. A
singleton that dies with unshipped rows while a new node joins and takes
the lease after the TTL finds its rows exported as conflict copies on
return. Same-AZ latency, G1–G6, failover times and the S3 request counts
are unchanged. About 10–14k lines go, in the area that produced the bugs.
Mixed versions stay unsupported; filesystems created before the record
change are recreated (no shims, no version bump). Plan 43's C3 as
specified, C5, C6, the G1 p90 nudge and C2h's flex strict flags are
superseded; C2g, C2g′, C2h's scenarios, C4 and C7b-8 continue. ADR-19,
ADR-21, ADR-23, ADR-33, ADR-38 and ADR-39 are amended or superseded in
part as marked on each.

**Landed, no lone delegate (plan 46 M1.2 step 13)**: no delegate answers
from its disk alone. The root grants a placed or manual delegation only to a
node with a group within `CONSTELLATION_BACKUP_RTT_BUDGET_MS` (the root's
own link to it within the budget, or a backup its stream records); a sticky
or designated grant is final and needs none. A writer with neither is
forwarded to the root (placement counts `place_skipped_far`) until a sticky
grant (M1.3): its own ops cost the round trip to the root instead of the
disk. The lead (`delegation_stream_rows` as a commit input and the
published horizon), the lone↔group hysteresis (the gated mark and the
store's demotion of a `Singleton` row), `deleg_lost_after_ack` and the
`LoneLead` oracle are deleted; the batch size stays. The root cannot see a
backup a far node could find among its own peers, so such a node is not
delegated at first (a recorded backup does count). `Config::lone_delegate`
(`AUTHORITY_SIM_LONE_DELEGATE=1`) is the negative control that grants and
answers lone, as the model's `lone_delegate`.

**Rejected**:
- *Finish plan 43 as specified* (C3, C4, C5, C6, C7b-8): keeps five
  classes decided per row, the lead and the lone↔group flip, generations
  with cross-generation dependencies, the server-side replay queue (C5
  renames it) and member copies with four drop rules; 8–12 coder-weeks
  inside the densest bug cell at the observed 37 % induced-defect rate,
  with rows of the 29/39/51 shape still reachable afterwards.
- *A majority log among backups* (Raft/QJM): no two-node failover without
  S3 as witness (then it is B2 again), a WAN round trip when voters span
  continents (hard constraint 4), a second trust root; ADR-2's and the
  arbiter study's verdict, unchanged.
- *S3 Express One Zone as the log tier*: AWS-only, one AZ, a second
  bucket (hard constraints 1–2, ADR-27); even allowed, about 10 ms is ten
  LAN round trips.
- *Calvin-style deterministic ordering* (the log carries inputs with
  read/write sets, replicas execute): Calvin's own authors name the
  asynchronous replication of the sequencer log, the mode constellation
  would be in, as the failover problem; no gain over the verbatim
  re-ship of effects.
- *A client-owned replay list with `ack=local` kept as an opt-in*
  (Lustre/Ceph style for a lone root's own clients): correct, but it keeps
  a rollback-and-replay path, its persistence and its oracles alive for
  one opt-in that designations already cover for offline laptops; the
  requester's existing in-doubt retry covers every unacknowledged row
  once the root's own parked op takes the in-doubt path, so no list is
  needed.
- *Keeping lone delegates* (the lead, the flip): the source of rows 29,
  33, 39; a sticky grant gives the same latency with a final answer.
- *Hash-range splits as an expert setting* (ADR-38): the only source of
  multi-generation nodes that is not a hand action; measured to lose on
  real names.

**See**: [plan 46](../plans/v1/wip/46-committed-or-final.md) (the
diagnosis, the design, the milestones, the per-topology behaviour, the
open questions); [plan 43](../plans/v1/done/43-commit-rule.md) (the
rules, the invariants I1–I5, what continues); the arbiter study;
[DESIGN §4](DESIGN.md#metadata-plane) and [§9](DESIGN.md#failure-handling),
which change with the code in each stage. Public sources: CockroachDB's
parallel commits (the "never acknowledged before committed" property),
Lustre's recovery overview (transno, `last_committed`, the client replay
list), CephFS's `Server.cc` early replies and `clientreplay`, Ongaro's
Raft thesis §6.3 (client sessions), RFC 8881 §2.10.6 (sessions and the
reply cache), the Calvin paper (SIGMOD 2012), Speculator (SOSP 2005) and
Rethink the Sync (OSDI 2006).

## ADR-41: A delegate counts the root's copy only through rows the root's tenure has committed (interim rule for Q16)

**Status**: decided by the maintainer on 2026-11-02, as an interim rule
until plan 46 M1.2's member copy lands; M1.2 may revert it (below).
Amends [ADR-33](#adr-33-commit-is-the-configured-groups-acknowledgement-vertical-paxos-over-s3)
and plan 43 Q16.

**Amended by [ADR-42](#adr-42-a-takeover-gate-whose-journal-work-is-done-serves-the-generations-it-inherits)
(2026-11-03)**: the push runs while the root is usable *or in a takeover
gate whose journal work is done*.

**Amended by plan 46 M1.1 step 1 (2026-11-05, the maintainer's "M1.0
answers", plan 46 §11)**: the lone-journal branch of the Decision is
removed. `committed` counts the root's appended copy only once the root's
own configuration has committed it (`Core::config_committed_jseq`): shipped,
or held by every backup it gates on; a lone root (no backup listed, none to
be had), usable or in an ADR-42 gate, commits by the archive alone. "Only
taken over at the lease's expiry" does not make its journal a second copy: a
lone root paused past its expiry is taken over from the archive, and a row
the delegate answered on that journal is lost when the delegate then fails
(model `node_stream`: knob `root_journal_counts` violates
`acked_never_lost`). An epoch hold's journal still counts until plan 46 M1.4.
Cost: such a delegate's `DelegGroup` answers wait for the root's ship.

**Context**: with no backup recorded for its generation the group of a
delegate is `{delegate, root}`, and the delegate answered a row once the
root had *journaled its append* of it (`DelegateStreamAck.through`, plan 43
Q16: "the delegate cannot observe the root's own backup acknowledgements,
and does not need to: two copies are the single-failure contract"). The
second copy has to survive the failure the group is built for, the loss
of one member, and a root's journal does not always. A root whose backup
has sealed its epoch (the backup takes the lease at the marker) is deposed
without knowing it, for as long as the backup's seal takes to reach it. A
batch it appends in that window and acknowledges lands in a journal that
nothing recovers: the new holder's log does not carry the row, the
delegate's group counted it, and the requester was answered
(`delegated-backup` `+commit` seed 451: node 2 seals epoch 1 and takes the
lease at 2,785 ms; root 1 appends row 1 of generation 7 at 2,787 ms and
acknowledges it; the delegate answers at 2,796 ms; the lease's holder
lacks the row). The rows reach the log only when the deposed root's PUT
beats the marker, which is luck. Oracle I1 (no answer without a committed
row) found it in ten seeds of five configurations
(`delegated-backup`, `long-delegated-backup`, `placement-hot`,
`long-delegated-late-answers`, `delegated-two-gens-backlog`; the last
three and two of `long-delegated-backup` with slow lease reads). It is the window plan 43 C3 §5.4 item 4 names
(a deposed root's journal rows, recovered by C3's member copy); Q16
assumed that recovery.

**Decision**: the root's acknowledgement counts for the group `{delegate,
root}` only through the rows whose append **its own tenure has committed**:
- the row is shipped (`journal_acked_seq`, or in `ack.shipped_rows`), or
- the root holds the lease, it is not lost, and `last <= durable_jseq()`
  (every backup the root gates its own answers on holds it; `u64::MAX`
  for a lone root or an epoch hold, where the root's journal counts: a
  lone root can only be taken over at the lease's expiry, and nobody seals
  it).

A root with backups therefore acknowledges only once its backups hold the
append, so its successor is one of them, or has sealed one, and holds the
row. The wire carries both numbers: `DelegateStreamAck.through` stays the
appended cursor (the delegate's flow control: where it re-sends from, what
it drops from its journal window), and `committed` (at most `through`) is
the committed prefix (`Core::root_append_cap`). The delegate keeps it as
`DelegateState.root_committed` and uses it where it used the cursor: the
group's committed prefix (`deleg_group_through`) and the removal of a
backup from the record (`deleg_group_maintain`). It raises
`root_committed` before it drops an acknowledgement that appended nothing
new, so the root's push (below) is never lost to the echo checks. The root
pushes: after every event (`Core::root_stream_push`), a generation whose
committed prefix rose above the last one sent is sent an unsolicited
acknowledgement, on the round and request of the delegate's last batch,
while the root is usable; without it the delegate would wait for its own
next batch. A push that does not come is asked for (Consequences).
`holder_append_committed` and its use in C3 answers (`Lone` excludes a
lone tenure's appends) are unchanged.

**Q16's recorded alternative** (the root's committed cursor: three copies
on a LAN of three, about two round trips pipelined) was "recorded, not
planned"; this is that alternative, restricted to the root's own
tenure's commit rule, and not a new copy. The delegate still cannot see
the root's backups, and does not need to: it counts what the root says.

**Consequences**:
- A no-backup delegate's `DelegGroup` answers (another node's forwarded
  op, and its own client's op while the delegate is not lone) take one
  more root-to-backup round trip, pipelined with the root's own ship,
  plus the push's hop: the batch goes to the root, the root's append goes
  to its backup, the backup's acknowledgement raises the root's commit,
  and the root answers. A root with no backup listed but one that could be
  had (a candidate being brought up, a peer in budget) waits for the
  archive, as its own answers do. A lone root, an epoch hold, and
  a recorded delegate backup are unchanged. Measured (G1's harness,
  release, topology 3's delegate, p50 / p90): create 464 / 611 us to
  711 / 847, rename 455 / 579 to 965 / 1,089, unlink 439 / 555 to 998 /
  1,124, chmod 397 / 532 to 1,110 / 1,286; G1 (the holder plus 500 us)
  is then over for rename, unlink and chmod (p50 by 207, 244 and 374 us).
  The maintainer accepted the overrun as temporary, against plan 43's rule
  that a failing gate stops its milestone, until M1.2's member copy
  (below) or a cut of the cost. Most of it was not the rule's round trip
  but the root's cap: `root_append_cap` read the generation's
  `journal_origin` range from index 0, over the deleted keys of every row
  it had shipped, twice on the delegate's path and in every root step
  while a push was due. It now reads from past the committed prefix it
  sent (a row the tenure committed stays committed; a new round reads the
  whole range again), and G1 is met on every op: chmod 648 / 777 us,
  rename 700 / 848, unlink 682 / 825, with a p90 margin of 23 to 36 us.
  The rule's own price is about 0.23 ms (two hops and two steps); the
  push already leaves in the step of the backup's acknowledgement.
  PROGRESS, "Oracle I1: class A" and "Item A's cost".
- Where the delegate's own backup is the root's backup too (topology 3),
  the commit takes one local round trip more.
- The root's push is one-way and sent once per rise of the commit, and the
  delegate has no other word of it. A lost push, a root restarted or
  re-adopted between the acknowledgement and the commit (the new
  incarnation's generation has no last batch to push on, and the root is
  the same node, so nothing changes the delegate's round), and a round
  change of the delegate's (the push carries the old round and is dropped
  as stale) each left a quiet delegate's parked answer until its deadline
  and the settle, 9 to 12 s; the outcome was still correct, through the
  holder's committed row. Narrowed: a delegate whose stream has nothing to
  send, and for which an answer has waited a stream tick on a commit the
  root has not told it (`root_committed < streamed_through`), sends the
  root an empty batch on its round (`deleg_poke_root`), once per tick per
  generation while it waits. The commit awaited is the generation's own
  (a parked row above `root_committed`, its group counting the root's
  copy), or that of a generation an answer is held by through a
  dependency (`deleg_caps`: the ask goes to the generation awaited), or,
  once the recorded backup is gone, the one the removal of the backup
  from the record waits for (`deleg_group_maintain`: `acked_max <=
  root_committed`). The root already answers an empty batch with its
  cursor and its commit, on the delegate's round, and keeps it as the
  generation's last batch, so its pushes go on too; the delegate counts
  the answer like a push. The ask is tracked apart from the batch of rows
  in flight (`DelegateState.ask`): a row executed meanwhile streams at
  once, and the ask's answer, failure or timeout never touches the batch
  or the stream's back-off. A refusal, a failure or a timeout of the ask
  backs the asking off as the stream's does (200 ms doubling to 2 s), and
  an answer ends it. Nothing is sent while no answer waits on the root,
  nor while the push comes within the tick (the healthy path: one round
  trip after the batch's acknowledgement). When the ask arrives, the
  answer is released within a tick (50 ms) of the commit plus the round
  trip of the ask; after an ask that is lost, by the request timeout and
  the back-off; after a refusal, by up to 2 s. A stopped or lapsed
  delegate asks nothing (what it holds is the holder's to settle). What
  the sweeps show with every push dropped: the commit is raised by the ask
  hundreds to thousands of times per 1,000 seeds, and the answers that
  wait a second or more fall where the push was their only word
  (`delegated-backup`, `long-delegated-backup`); elsewhere those tails are
  other classes and do not move. Carrying `committed` on `DelegRenewed`
  instead would have reached the same cases at the renewal period (ttl /
  2, 2.5 s of the 5 s default), through the off-core renewal lane, and on
  the wire.
- No version bump, no shim: both ends change together (the project
  carries no compatibility for mixed versions).
- The sim's `+commit` oracle holds on the seeds that failed
  (`i1_a_delegate_counts_the_roots_copy_only_through_what_the_roots_tenure_committed`).

**May revert**: plan 46 M1.2 gives each delegate node one stream and a
member copy (C3's verbatim re-ship of a deposed root's rows, folded in),
so that the root's own tenure no longer has to commit a row before the
delegate may count it; M1.2 deletes the delegate's lone class and
`root_committed` with it, and M1.2 may return to the cursor if the member
copy covers the window.

See: [plan 43](../plans/v1/done/43-commit-rule.md) (§4 the oracle, Q16),
[plan 46](../plans/v1/wip/46-committed-or-final.md) M1.2;
`crates/authority/src/core/delegate.rs` (`root_append_cap`,
`root_stream_push`, `deleg_poke_root`, `on_delegate_stream`,
`on_delegate_stream_ack`, `deleg_group_through`).


## ADR-42: A takeover gate whose journal work is done serves the generations it inherits

**Status**: decided by the maintainer on 2026-11-03 (plan 46 M0.3, S5, option
(a)); interim until plan 46 M1.2. Amends
[ADR-41](#adr-41-a-delegate-counts-the-roots-copy-only-through-rows-the-roots-tenure-has-committed-interim-rule-for-q16)'s
push and plan 46 M0.3's S2.

**Amended by [ADR-44](#adr-44-a-takeover-gate-whose-journal-work-is-done-recalls-on-demand)
(2026-11-05)**: a shippable gate recalls on demand, drains and journals the cut
of a generation a pending op or settle needs ended; it still seals, reclaims,
outwaits and re-delegates nothing.

**Context**: a successor's takeover gate keeps its view closed for the
read-delegation quarantine (12.5 s in the sim) long after its journal work is
done (`PendingGate::shippable`: the marker landed, the predecessor's tail is
applied, the node's own delegate rows are re-appended, the replays have run,
the lock mirror is installed). It learned the delegation table only when it
opened, so it refused every delegate's batch at `through=0`, left renewals
unanswered (S2) and bounced settles on their keys. An op on another node's
subtree outlasted its deadline plus the settle window (9 s + 3 s) and was
answered `EIO`, and the op then took effect when the gate opened (I4
`+unsettled`: 216 of 1,000 `delegated-holder-cut` seeds, 77 of
`delegated-backup`, 26 of `placement-hot`, 81 of
`delegated-no-backup-root-near`, 185 of `long-delegated-backup`). The
quarantine protects keys the previous tenure may have read-delegated: the
grants of a superseded tenure or of a previous incarnation, which no table
this tenure holds lists, and a lone node's kernel caches. A live generation's
keys are not among them: every row of the generation touches only the keys it
owns (a delegate executes only what resolves to its generation; a cross-subtree
op ends it first), the root grants nothing on them after the `Delegate`
record, and a delegate executes nothing before a renewal granted after the
root's earlier grants on the subtree were recalled or outwaited and after that
root's quarantine (every *first* renewal waits for both).

**Decision**: once `shippable`, the gate takes on the table's generations of
other nodes (`delegation_sync` at level `RootSync::Gated`; its own are taken on
and ended at the open, as before):
- it appends and acknowledges their batches, with `committed` per ADR-41, and
  pushes rising commits (`root_stream_push`);
- it answers their renewals; a *first* renewal still waits for the read
  quarantine (`deleg_renew_waits_for_reads`, answered by
  `Timer::GrantQuarantine` while the gate is still pending);
- it redirects a settle on their keys to the delegate, when the generation is
  live, unrecalled, within its window, the delegate is reachable, the settle is
  not the delegate's own, and this tenure has granted the generation (else the
  settle is `Held`: a delegate that lapsed while the gate was not shippable
  answers `NotHolder { root }` and the two would bounce it).
It recalls, seals, reclaims and ends nothing while gated: `start_recall` and
`deleg_recall_plan` (`RecallPlan::Gated`) refuse inside a gate, `root_progress`
ends no generation (the open ends a drained one), `on_deleg_expiry` re-arms,
and the gate serves only a generation it took on from the table
(`GenState::inherited`, not recalled, not ended). A gate that is not shippable
or is releasing is unchanged. A settle of the gated root's own plans no recall
(`execute_local_outcome`): the gate ends its own generations at the open, and
`start_recall` does not know them.

**Two findings of the review, closed here**:
- *A key can enter a generation through a root-only row* (plan 45's key move):
  an inode's keys belong to the owner of its primary name's parent (the lowest
  `RDentry` key), so the unlink of the primary name, in a root directory, of a
  file also named in a delegated one resolved to the root alone, recalled
  nothing, and left the file in the delegation with the read delegations the
  root had made on it still live. `TouchSet::from_op_in_names` holds the name
  that becomes the primary one (for an unlink, the names a rename or an
  exchange moves, the name a rename overwrites, a stranded replay's records),
  applying the moves one after another to a per-inode name list, so an op that
  removes the primary and then the next name of one inode (a stranded replay's
  records) holds the third name too; the op is cross-subtree and the root recalls and re-delegates
  (`delegate_dir`'s read recalls then cover the file). It closes the same race
  for a usable root. Cost: one `RDentry` range seek per unlink, rename or
  exchange in `keys_of_op_in` and the FUSE fast paths, only while a delegation
  exists; the ops it makes cross-subtree are those of an inode with several
  names whose primary name leaves while another name is in a delegated
  subtree. A delegate no longer executes the unlink of the primary name of a
  file whose next name is in another generation of its own (the state a
  review follow-up of 5b6a6b1e built could not arise any more).
- *The gate could recall or end a generation once it knew one*: closed at the
  choke points above.

**Consequences**:
- Settles on another node's live generation are answered by its delegate
  within the window, a delegate's grant runs through its root's takeover
  instead of lapsing, and the gated root's own clients waiting for a
  delegate's row are answered once the row ships (`ship_landed` runs
  `answer_awaiting_log` when rows shipped inside a gate; without that call the
  op waited for the next appended batch, applied segment or inbox recheck). Windows of inherited
  generations start at `shippable`, not at the gate's opening (the bound is
  unchanged: any instant after the tenure began). The off-core lane stays off
  in the gate (a fenced root publishes no certificate).
- `committed` inside the gate: a lone gated root (no backup listed) counted its
  journal at once (`u64::MAX`, as a lone usable root under ADR-41) until plan
  46 M1.1 step 1 (ADR-41's amendment): it, like a backed one, raises it when
  the row ships (nothing is backed before the gate opens).
- New interaction found by the sweeps, fixed here (two I1 classes):
  - *Fix A.* Inside the gate the replica's `completed` also holds what the gate
    re-appended to its journal; `answer_awaiting_log` no longer answers an op of
    this node from it before it ships (I1: an inbox op answered from the
    re-shipped tail). The guard skips exactly the rids whose `Completed` or
    `Refused` row is still in the unshipped journal; the node's own ship answers
    the op: `ship_landed` calls `answer_awaiting_log` when rows shipped while the
    gate is pending (the other answers are the next appended batch, applied
    segment or sealed drain, the inbox recheck, `resolve_moot_waits` at the open
    and the op's deadline).
  - *Fix B.* A node that is a generation's root and its recorded backup
    acknowledges a backup append only through what the log holds
    (`root_log_floor`), not through its own unshipped appends of the stream
    (I1: a delegate answered a row its backup did not yet hold). The floor
    applies to a usable root and to a deposed root as well as a gated one: it
    holds whenever the node has unshipped holder appends of the generation,
    whether or not it still has the generation's `GenState` (a deposed root
    drops it in `deleg_on_lease_gone` and keeps its unshipped appends in the
    journal, and in `log_stream_idx`, until its recovery strands them). An
    earlier version of this change applied it only while the node held the
    `GenState`, which left the deposed root open; the usable root was never
    open. Cost, open: the floor reads the generation's unshipped holder
    appends from index 0 (`holder_appends`), which walks the deleted
    `journal_origin` keys of the generation's shipped history. The holder
    append tip only rises and a strand deletes journal rows without advancing
    the shipped watermark, so the tip is clamped to the journal's highest seq
    (every unshipped append has its last row in the journal above the
    watermark): the full scan runs only while this node holds unshipped holder
    appends, mainly a backup that became the root at a takeover, and not on a
    recovered, deposed holder that stays a follower. It never runs in
    `root_append_cap` (plan 43 G1). No lower bound for the scan could be shown
    safe from the state the core keeps (the backup's last acknowledgement and
    `committed_sent` both count rows that are still unshipped), so a bound
    needs new tracking of the generation's first unshipped append.
- What the gate still cannot answer, as before and by this decision: an op that
  needs a generation recalled or ended (a cross-subtree op, a generation whose
  delegate rebooted and is stopped), and a delegate that restarted in the gate
  (its first renewal waits for the quarantine). (The first class is closed by
  [ADR-44](#adr-44-a-takeover-gate-whose-journal-work-is-done-recalls-on-demand); the delegate
  restarted in the gate remains.) Over 1,000 seeds (`+commit`,
  before and after, without and with `AUTHORITY_SIM_LEASE_READS=0:0:800:1000`)
  the unsettled `EIO`s with a sequencer reachable fall from 252 / 470 to
  144 / 251 (`delegated-holder-cut`), 85 / 131 to 32 / 74 (`delegated-backup`),
  35 / 42 to 8 / 30 (`placement-hot`), 91 / 133 to 30 / 57
  (`delegated-no-backup-root-near`), 236 / 413 to 109 / 199
  (`long-delegated-backup`); the residual sampled (PROGRESS, "S5") is these
  classes, not the settle on a live delegate. The allowed count of "no
  sequencer in time, generation outwait, keys across generations" rises, and
  the classifier (`sim/reach.rs`) did not change: its input did. A gated
  renewal now publishes a later grant end, so `gen_end_due`'s `by_grant` moves
  later, and an op on a generation whose delegate is not in force becomes "no
  sequencer in time". That is a real outwait of the product, not a relabelling
  that hides I4 cases, and a real availability cost when a delegate dies inside
  the gate: the root cannot end its generation before the later window. The
  totals fall.
- No version bump, no shim.
- Residual, not new: a root grant on a directory whose entry names a multi-link
  inode with its primary link inside the subtree (ADR-36's dentry copy of a
  directory's attributes, and a remote-dentry lookup of a multi-link inode) is
  not recalled by the append of a delegate's row; a usable root's own grants
  have the same shape today. The gate adds the previous tenure's grants to this
  class for its span. A grant the previous root made only in memory is trusted
  through a delegate's `first: false` renewal (`until == 0` is the delegate's
  "never granted in this process"; a same-boot restart sends `first: true`
  again, conservatively).

**Rejected**: (b) a shippable gate recalls the generations (`DelegCut`): it
ends grants that plan 46's sticky grants are meant to keep, and it executes a
recall inside a view that is closed.
[ADR-44](#adr-44-a-takeover-gate-whose-journal-work-is-done-recalls-on-demand) takes the
on-demand form instead.

**May revert**: under plan 46 M1.2's one stream per node with a member copy and
sticky grants, the gate's inheritance of a node stream and its handling of
renewals are re-derived from the node's record. If M1.2's ownership walk or the
member copy makes the root's append unnecessary inside the gate, this rule goes
with `GenState`.

See: `crates/authority/src/core/delegate.rs` (`delegation_sync`,
`root_gated_shippable`, `gated_serves`, `on_delegate_stream`, `on_deleg_renew`,
`deleg_recall_plan`, `root_log_floor`), `holder.rs` (`gate_settle`),
`client.rs`, `crates/meta/src/replay.rs` (`TouchSet::from_op_in_names`);
[plan 46](../plans/v1/wip/46-committed-or-final.md) M0.3 S5; PROGRESS "S5".

## ADR-44: A takeover gate whose journal work is done recalls on demand

**Status**: decided by the maintainer on 2026-11-05 (plan 46 M0.4 follow-up;
the spec reviewed before it landed); interim until plan 46 M1.3. Amends
[ADR-42](#adr-42-a-takeover-gate-whose-journal-work-is-done-serves-the-generations-it-inherits)'s "recalls, seals, reclaims and ends nothing".

**Context**: under ADR-42 an op that needs a generation of another node
recalled (keys across generations; a generation its delegate disowned or whose
delegate is unreachable or past its window) waited for the gate to open (the
12.5 s read quarantine plus the drain) before the root sent the recall. Its
requester's deadline and settle window (9 s + 3 s) ran out meanwhile: the
settle was `Busy` (S1 answers only keys the gate owns), the `EIO` went
unsettled, and the op took effect after the open. The stricter I4 classifier
(SHOULD-2 of the M0.4 review) counts 432 such `EIO`s as violations over 2,000
seeds of the delegated configurations.

**Decision**: a shippable gate (lease held, not lost, not releasing,
`PendingGate::shippable`, `journal_ship_epoch` in force) recalls, on demand,
exactly the generations an open root would recall for a pending op or settle it
is asked to execute or settle, and only among the generations of other nodes it
took on from the table (`GenState::inherited`, not ended, not a designation):
- what: every involved generation of a cross-subtree op (`redelegate` set, the
  re-grant waits for the open); the one a single-generation forward or settle
  falls under when the open root's S3 test would not redirect it (ended,
  recalling, past its window, delegate unreachable, or the delegate's own op or
  settle: for an inherited generation `grant_in_flight` is false, so a
  delegate's own request counts as disowned also while its first renewal only
  waits for the quarantine; M1.3's holder-initiated release replaces that
  test); every involved generation of the gated root's own op or settle that
  reaches `route` (as `execute_local_outcome` plans it for an open root). Entry
  points: `on_forward`'s fenced branch, `route` and `on_forward_backoff` for the
  gated root's own ops and settles and the takeover drain's halt for an inbox
  op. One function decides, `Core::gate_recall_on_demand`;
  `start_recall` refuses in a gate that is not shippable and, in one that is,
  any generation not inherited or this node's.
- when: at the first such request after `shippable`, and again on a later one
  while no recall is in flight. Never at `shippable`, a sync, an expiry, a
  renewal or a batch.
- not: no seal, no reclaim or outwait (`on_deleg_expiry` re-arms), no end of a
  generation it did not recall on demand (its own end at the open; a recall kept
  from a usable phase ends at the open), no renewal of a generation it recalls
  (left unanswered, S2), no re-delegation, no execution before the open (the
  forward stays `Busy`).
- the drain and the end: the gate takes the recalled generation's batches
  (`gated_takes_stream`), resends the recall (L3) for `gate_recalled` only, and
  once `cursor >= through` journals the end (`RecallCut { through: cursor }`,
  `void_stream`, `note_void_cut`, its own speculation stranded, the returned
  lock grants installed) inside the gate.
- the settle: `Held` (`recall_hold_ms`, no attempt spent; before: `Busy`) while
  the recall is in flight; once the cut is journaled the keys are the gate's and
  S1 answers: a rid with no outcome gets the journaled `Refused { ABANDONED }`
  once its row ships, a rid that took effect is `Held` until the open (M8). The
  gated root's own settle waits in `Backoff` (`ClientOp::gate_recall_wait`) and
  is routed as a settle again, not down the lease path.

**Integrity**: the recall, the drain and the cut are the open root's code paths
under the same lease; the cut is a journal row after the marker, the tail and
the gate's own rows and after every appended row of the generation (tail rows
are at or below `log_stream_idx`, the inherited cursor); a deposition strands it
like an open root's (the cut retracted, the void lifted, the successor inherits
the generation live and the stopped delegate answers again from
`handed_back`); ADR-43's orphan rule sees a durable end of a listed number; the
drained batches are acknowledged with `committed` per ADR-41 as ADR-42 already
does, and inherit the lone gate's exposure (M0.4 STAGE-1, not new). No new
acknowledgement class (I1); the op is never parked at the gate and a
delegate-executed copy is in the log before the cut (I2, I2c); `Abandoned` only
on the gate's own keys for a rid with no outcome here (I4); the cut is at the
cursor and the gate's speculation of the generation strands and replays (I5).
The read quarantine covers every key it covered: the ended generation's keys
join it, and the gate's view stays closed until the open.

**Consequences**: over 2,000 seeds the stricter classifier's unsettled-`EIO`
violations fall from 281/676 (long-delegated-late-answers, without/with slow
lease reads) to 15/152, 29/65 to 2/32 (placement-hot), 325/627 to 35/100
(delegated-holder-cut), 174/406 to 2/70 (long-delegated-backup); no failing
seed; I1, I2, I2c, I5 unchanged; I4 late landings fall. Residual: the gated
root's own op whose live delegate waits for its first renewal, a rid that took
effect (held until the quarantine), a gate that is not shippable while the recall drains (seed 1796: a
gate paused past its lease with the recall in flight; the end is journaled at the
next event once it is shippable again, else at the open; the seed stays open as a
liveness case), an unreachable delegate (`Held` until the open; the
lease path could not answer either), a refusal not shipped by the window's end.
Cost: none on an open root; in the gate one recall per needed generation, 12 to
20 s earlier than before; a cross-subtree op's subtree is re-granted at the open
and may be recalled once more by the op's retry (`deleg_gate_redelegated` counts it); a forwarded settle is `Held`
instead of `Busy`. Two S5 unit tests changed their expectation (a disowned and
an outwaited generation: recalled and `Held`, were `Busy`); the m03 witness
seed moved from 2 to 449 (2 and 260 are closed by this rule). No version bump,
no shim.

**Rejected**: wholesale recall in the gate (ADR-42 (b): ends grants sticky
grants keep); a cut held until the open with the settle answered from the drain
state (a root refusal journaled on a delegated key); `Held` until the open (no
I4 fix); redirecting a cross-subtree settle to a delegate (it cannot settle
keys outside its subtree).

**May revert**: under plan 46 M1.2/M1.3 the recall is a request the holder
answers with `SubtreeReleased`; the rule becomes "the subtrees the op needs
released", the end the journaled release, and `GenState::gate_recalled` goes
with `GenState`.

*Amended by plan 46 M1.2 step 10:* the recall is a `ReleaseRequest { gen,
dirs }`, one way; the delegate answers in its stream with a `StreamReleased`
(or `SubtreeReleased`) row, and the gate ends the generation once it has
appended that row (the drain `root_progress` waits for), the L3 resend
applying to the request. The gate's on-demand recall asks for every subtree
of the stream; a release of some subtrees only is the open root's
(`request_release`), the gate sequences nothing it has not taken whole.

See: `crates/authority/src/core/delegate.rs` (`gate_recall_on_demand`,
`gated_takes_stream`, `start_recall`, `on_release_resend`,
`root_progress`), `holder.rs` (`on_forward`'s fenced branch, `gate_settle`),
`client.rs` (`route`, `on_forward_backoff`, `settle_route`,
`ClientOp::gate_recall_wait`), `inbox.rs`; tests
`igr_a_shippable_gate_recalls_the_generation_a_cross_op_needs` (fail-before),
`igr_a_gate_recalls_nothing_unprompted`, sim
`s5_a_gated_root_serves_another_nodes_generation`,
`m03_the_configurations_without_an_unsettled_eio_run_strict`; plan 46 M0.4
follow-up; the M0.4 review STAGE-1 "ADR-42's in-gate class" (superseded by this
decision).

## ADR-43: A generation number carries the epoch of the tenure that minted it

**Status**: decided by the maintainer on 2026-11-04 (plan 46 M0.3, the L4
follow-up). Amends the generation identity of
[ADR-23](#adr-23-delegated-sub-sequencers-over-one-log); the rule of ADR-41
and [ADR-42](#adr-42-a-takeover-gate-whose-journal-work-is-done-serves-the-generations-it-inherits)
(a gate inherits other nodes' generations *by number*) is unchanged.

**Context**: a root minted `max(its counter, table.max_gen() + 1)` from its own
view of the log. Two roots of different epochs whose views agree on `max_gen`
minted the same number. That happens whenever a root is deposed without
knowing it (a seal-based takeover, a suspend) and keeps granting, in two
directions: the deposed root's grant never ships and the successor then mints
the same number ("orphan first": the void its journaled end left met the
successor's grant, L4, placement-hot 6239, 15418), or the successor's grant is
in the log and the deposed root's lagging replica mints the same number for
another directory ("real first", placement-hot 16871: four nodes held the real
grant's rows and marks of the number when it was minted again). A probe on the
old mint counted it: 133 of 20,000 `placement-hot` seeds (knob
`AUTHORITY_SIM_LEASE_READS=0:0:800:1000`) and 0 to 5 of 2,000 in each delegated
configuration. Every per-generation key assumes the rule that the code states
("a generation number is a stream's identity, never reused across roots"): the
session void, the journal origin and the L1 mark, `deleg_idx`,
`deleg_stranded`, `deleg_held_boot`, `deleg_log_idx` and the void cut, the
delegate backup's tail, seal and acknowledgement, the read delegations' epoch.
With a reused number, state left by one grant is read as the other's: mostly
liveness (a stream gap, a delegate stopped, a backup sealed for good), and three
integrity paths (the origin-keyed retire, the L1 mark, the backup's tail and
acknowledgement). No sweep witnessed an integrity failure (no delegate executed a
row under a grant the log never carried), but the precondition of those paths
is reached often, and plan 46 M1.2 keeps `(gen, idx)` as the stream identity.

**Decision**: a generation number is minted by one tenure, in its own epoch's
block.
- `gen = epoch << 32 | n`, `epoch` the lease epoch of the S3 lease the minting
  root holds (`LeaseState::held_epoch`: one holder per lease epoch, every
  acquisition takes `epoch + 1`; a continuation-epoch hold has no such epoch and
  mints nothing, and `delegate_dir` refuses under one anyway), `n >= 1` from the
  node's persisted counter (`next_deleg_gen` in `local`, the value `gen + 1`).
- The counter is committed and `sync`ed before the number is returned, so before
  the grant leaves (the idiom of `kv_set_durable`). That sync is load-bearing: a
  node re-claiming its own lease keeps its epoch (`own` implies the previous
  epoch), so one epoch spans a crash and restart of its holder, and commits are
  `PersistMode::Buffer`: without it a host crash after the grant left could mint
  `n` again. The sim cannot witness this (a simulated crash keeps the `Meta`), so
  the meta test of the counter across a reopen covers persistence only.
- A root mints only in its own block; a number outside it, from any source, is
  refused (`MetaError::Invalid`, "outside epoch E's block"): `gen =
  max(counter, at_least, epoch << 32 | 1)`, and a `gen` whose epoch is not the
  minter's, because the counter or the log's floor `at_least = max_gen + 1`
  names a later tenure's number, is not minted (a legitimate holder of epoch `e`
  cannot see a block above `e`: epochs only rise and a higher epoch in the log
  deposes it; so the refusal is the deposed root's, and a rule of its own is not
  needed). `at_least` stays as the floor for a lost counter. A block is exhausted
  at `n = u32::MAX - 1`; an epoch above `GEN_MAX_EPOCH = 2^29 - 1` cannot mint
  (lease epochs grow by one per acquisition: unreachable).
- A re-adopted epoch continues its counter; a re-win after a loss is a new epoch
  (`epoch + 1`) and a new block; a replica rebuilt from a side replica keeps
  `local` and with it the counter. `max_gen` stays monotone in log order (`n`
  rises within a tenure, blocks rise with the epoch), so "the log's highest number
  floors the next mint" and `deps()`'s newest-first cap keep working.
- Number space. The numbers are large and sparse. `GEN_MAX_EPOCH` keeps every
  `BASE + gen` space of the code inside its own range: a delegate's backup tail
  (`DELEG_BACKUP_BASE + gen`, in `backup_tail` beside the member copies at
  `2 << 48 + epoch`) moves from `1 << 48` to `1 << 62`, because a number of epoch
  65,536 reached `2 << 48`, so a delegate's rows would have landed on member-copy
  slots and each side's clear would have deleted the other's (`backup_clear` now
  stops at the member copies' base; `deleg_backup_slot_never_meets_a_member_copy_slot`);
  the parked-wait ids (`1 << 62 + gen`) and the read delegations' epochs
  (`1 << 48 + gen`) stay below `2^63`. `tail_key` is fixed-width big-endian, a
  `Streams` entry and `Gen` are `u64`, the model crate's bitmask generations are
  its own abstract ones.
- The session's restart seeding walks the generations the store keeps a key of
  (`deleg_log_idx`, `deleg_void_cut`) and the live ones, not `1..=max_gen`, and
  voids a keyed generation the table does not list only at or below the table's
  highest (`seed_generations`): a key above it, which a deposed root's stranded
  appends leave behind (`retract_log_idx_tx` keeps the key while the strand rolls
  the table back below it), names a grant the log may yet carry, and is held, not
  voided. A generation the table ended before the open with no key left here (the
  rebuild window of a side replica that began after the end) is voided lazily, at
  the cut 0 the eager seeding gave it, when a root's check of a dependency on it
  says no (`Meta::reaches_deps`, `reaches_streams_deps`, and `deps_lost_deps`,
  which the executors ask first: `holder.rs` `mutate_request`, the delegate
  stream's append and drain; were only the reaches check to void, the first
  arrival of an op whose dependency is past the generation's real cut would pass
  `deps_lost`, be reached by the void and execute ahead of its cause; one table
  read, on that path only). Only a generation that ended before the open qualifies
  (`SessionState::ended_before_open`): the end of one this incarnation saw may be
  a streamed-ahead install the successor strands, and only the durable log voids
  those. The same rule holds for an end that only outstanding speculation carries,
  at the open and on the read path: a `Recall`/`RecallCut` among the records of a
  live `Shadow` or `Streamed` entry, for a generation with no `deleg_void_cut` key
  (`Meta::speculatively_ended_generations`), is not an end yet. Such an install
  wrote the table row without the void, and both the open's seeding (from the
  table the store holds) and a read's `void_ended_among` (from the current table)
  would otherwise void it, and the entry's strand relists the generation with
  nothing to lift the void (only a `Local` strand stages an unvoid): a dependency
  on rows of it the replica does not hold would pass `reaches_streams`, an op
  executing ahead of its cause (the seed-6239 class). The seeding treats such a
  generation as live, held through its log index, and `void_ended_among` skips it
  (the table and the set read in one snapshot); the durable `RecallCut` voids it at
  the record's cut. A stored table's `max_gen` also counts a streamed-ahead
  `Delegate`, so the bound `gen <= max_gen` can sit above the log's: a keyed number
  below such a grant of a later tenure is an orphan of an earlier one, and voiding
  it matches the orphan rule. (The first version voided any unlisted generation below `max_gen`; the
  sweep moved four seeds of `long-delegated-late-answers` 0..100 and the
  unsettled ceiling test failed.)
- The in-process rebuild (`replace_ns_from_rebuilt`: the driver's and the sim's
  retention and deposition rebuilds, nothing reopened) replaces `ns` wholesale, so
  a generation that ended inside its window would have no key here and the session
  would not be seeded again: a dependency on it would wait for the rest of the
  process, and after a restart the lazy void would meet it at cut 0 and report
  dependencies at or below its real cut as lost. The rebuild copies the side
  replica's `deleg_log_idx:*` and `deleg_void_cut:*` keys (raise-only, after the
  reset's strand), seeds the session from the store as an open does (the cut of an
  ended generation is set from the store, not raised over the session's: a deposed
  root's own stranded cut may sit above the log's) and takes the side replica's
  table, read before the commit, as the open-table snapshot. Not the table the
  store holds after it: the reset re-applies kept committed entries onto the
  rebuilt namespace, and a streamed-ahead `RecallCut` among them writes the table
  row without the void, which the restriction keeps out of `ended_before_open`.
- What the sweeps do not witness. The sim's nodes open one in-memory `Meta` and a
  simulated restart keeps it (`session().reset()` keeps the open-table snapshot,
  the empty one of the creation), so at a restart `ended_before_open` is false and
  the seeding runs on an empty store: the sim never exercises the restart's
  lazy void, and "the sweeps match main" says nothing about it; only the meta
  tests (`generation_epochs.rs`, the session unit tests) cover it. The sim does
  run the in-process rebuild, which refreshes the snapshot, so the restricted
  lazy void and the rebuild's key copy are reachable there, after a rebuild only.
- What an orphan number leaves behind keeps its own identity. An orphan number is
  below the successor's block and never listed. It is voided for good once the
  table passes it: on the root's forward path only after a restart or an
  in-process rebuild (the seeding, or the lazy void, meets it then); within an
  incarnation, only a read voids it (`void_ended_generations`), and not an end only
  outstanding speculation carries. The orphan-rows item is closed for the operator-abandon and the no-tail takeover cases
  (`OrphanProof`): an abandon of the root group of an epoch (`GroupAbandoned { gen: 0 }`) proves the log never grants a
  number of that epoch or an older one that it does not carry yet; a takeover marker that carries no tail
  (`TailFollowsThrough { prev_epoch, through: 0 }`, no other tail announced) proves it for numbers of `prev_epoch` only (its
  later segments are fenced, none of it is re-shipped, and only a tenure's own successor re-ships a tenure's tail; an older epoch's
  committed rows may still ride a tail a later claim fetches, D2; members may hold committed rows of that epoch, as after a
  claim without a tail, and drop them under member rule 2, and a committed entry's grant is never stranded, so stays listed).
  Only the first marker of an epoch carries the no-tail proof: a root that re-adopts its own lease after a restart keeps its
  epoch and ships a second marker whose no-tail record says nothing of the tail its tenure's first marker announced and its
  journal may still hold, so a replica honours the record on the first segment of an epoch only, and the claimant of a
  re-adoption never gets one. A covered number that the table no
  longer lists, that this replica holds no trace of in the log (no cut key, no log index) and that no outstanding speculation
  ends is ended as the log's `Recall` would, at what the log holds of it (0), after the cut loop (a generation the segment
  itself ends stays out by its cut key): its rows roll back and replay by rid (a delegate's gated row, `DelegGroup`, is
  dropped there instead, C2 §3.8, on the premise that a committed row is at or below the cut, which a cut of 0 breaks: an
  answered one is then rescued only by its other copies, the requester's persisted shadow and the deposed root's queued
  append, both replays by rid; the L2b pair, deferred to plan 46 M1.2, review M0.4 SHOULD-1; pin
  `an_orphan_end_queues_a_delegates_answered_gated_row`, ignored). Candidates are the table's list before the
  strand, the numbers a live entry's records grant (a streamed `Delegate` whose `RecallCut` streamed too), the generations
  this node holds delegate rows of (`deleg_gens_awaiting_end`) and those of its live shadows and hints. Old-mint numbers are
  left alone. The claimant never applies its own marker as a segment, so its takeover strand runs the same rule with its
  marker's proof (`strand_for_takeover_carrying`), and marks the ended numbers stranded: a claimant that was the orphan's
  delegate otherwise kept its rows of it and shipped them as its first root rows, rows of a generation the log never granted,
  ahead of the stranded root rows they were executed on. For the same reason the gate's re-append of its own delegate rows
  after a re-applied tail skips a generation the table does not list. Restart seeding exception: a kept cut above `max_gen`
  voids at restart; cut keys come only from a durable end the log applied, a root's own journaled end (retracted on strand),
  a rebuild's copy of the side replica's keys, or this rule, so a streamed-ahead end (no key) stays live. The N4 case (a
  streamed-ahead `Delegate(g)` and `RecallCut(g)` both outstanding at a restart, then stranded) is closed when the strand is a
  covered marker's. The claimant's proof is persisted per epoch (`local["gate_orphan_proof"]`) before its marker can
  ship (a failed write withholds the marker: the gate stays pending and retries), and the strand of a re-adoption of the same epoch reads it back (an other epoch's is ignored; a re-adoption writes none),
  so a claimant that crashed between its marker and its strand still runs the rule; a backup-role read or write failure fails the claim or acquisition (never "no role, no tail"), and a marker's abandon comes from the gate's `gate_abandoned`, not the in-memory claim; a takeover that ships no marker (a released
  lease) computes and keeps no proof. A claimant's strand also takes out, with or without a tail, its own delegate rows of every
  generation the table does not list (plan 46 M0.4 review MUST-1): with no tail nothing older than its claim that the log
  does not carry ever ships, so such a generation's `Delegate` never reaches the log; the rows are queued for replay by rid
  (the replay dedups against the rid's outcome in the log, so a rid the log settled `Refused` never executes), the generation
  is ended at what the log holds of it and marked stranded. This closes the integrity side of a takeover that carries a tail
  below a grant: the grant's delegate, claiming later without a tail (its predecessor had no backup, or released), shipped
  those rows as root rows under an origin the log never granted, completing rids the log had refused
  (`a_claimant_without_a_tail_ships_none_of_its_rows_of_a_generation_the_log_never_listed`). Residuals, still open: a
  takeover that carries a tail (a grant past the sealed `through` never ships, but a replica cannot tell where the grant sat
  without per-row tail facts), so the stall and the N4 wait remain there on every replica until the generation's delegate
  claims the root (no row of it reaches the log: only the delegate's journal holds them past the sealed `through`, and its
  own claim now strands them; the delegate's replica shows their effects meanwhile); a rebuild window spanning the marker; a re-adoption whose crashed predecessor never landed its first marker (the
  re-adoption's marker is then the first of the epoch; its tail is re-shipped from the persisted backup role, and a
  re-adopted lease that names an inherited tail never gets a no-tail record); the persisted proof's read failing (read as
  none; the crash window is then open); and a sticky grant (plan 46 M1.2: a later tenure
  journals `Delegate` for an older number), which voids the no-tail rule's premise that a number is minted and journaled only by
  its epoch's tenure: the rule must be revisited before sticky grants land.
- Plan 46 M1.2: `DelegateNode.gen` is minted by the same rule; a sticky grant keeps
  its minting epoch across tenures (the number is the grant's identity, not the
  holder's ownership); `gen_epoch(gen) == my epoch` now says "granted by this
  tenure".
- No backward compatibility: a store's old small counter and old numbers are
  harmless (the block's start is the floor, an old number is below every block),
  and no old on-disk state needs reading. No shim, no version bump.

**Residual, outside the model**: a state directory wiped and then re-adopting its
own epoch (`own` needs only `prev.holder == me`) starts the counter at 0 and is
protected only by `at_least` from the log: the same class as a lost journal.

**Rejected**:
- (B) Identity at every consumer: key `journal_origin`, `deleg_idx`, `deleg_gated`,
  `deleg_log_idx`, `deleg_void_cut`, `deleg_streamed`, `deleg_held_boot`,
  `deleg_stranded`, the backup tail and seal, `dl.sealed` and `dl.backing` by
  `(gen, grant epoch)`, and carry the grant epoch in every origin, stream batch,
  backup append and dependency. It is this rule spread over ten key families and
  four wire paths.
- (B') Purge a number's state when its grant is applied: covers "orphan first"
  only; in "real first" the orphan state is created after the real grant and
  nothing tells the two apart by number.
- (C) Nothing beyond L4's unvoid at a grant: a latent integrity hole whose
  precondition is reached in about 0.7% of `placement-hot` seeds, moved unchanged
  into M1.2.

**Consequences**:
- Two tenures never mint the same number, in either direction; the sim's oracle
  ("no generation number is minted twice", `Report::gens_minted`) fails a seed that
  does, and fails seeds 16871, 1659 and 2958 on the old mint.
- Origins, dependencies, stream batches and backup appends carry a number of 5
  varint bytes (epochs below 8; 6 below 1,024, 7 below 131,072, 8 below 16.7
  million) where they carried 1. Measured in the sim, whose rows are tiny: the log's
  segments grow by 2.0% stored (2.5% decompressed) in `placement-hot+commit` and by
  7 to 12% stored in the delegated configurations (PROGRESS "ADR-43").
- One `fsync` per grant (grants are rare: placement windows are 30 s).
- A generation whose end only outstanding `Shadow`/`Streamed` speculation carries is held live by the open
  and by the read path until the durable `RecallCut` arrives or the entry strands: a watermark or dependency on
  it waits for the log, where it was refused or passed early. Cost: one scan of the outstanding speculation at the
  open, and on a read only when unreached generations exist. Open: a `Local`-only race, older than this
  change: a read that took its generations before the root journaled a `Local` end and its snapshot between that
  commit and the strand leaves a void applied after the strand's staged unvoid (under one snapshot a `Shadow` or
  `Streamed` strand cannot relist a generation the read voids: the entry is live, so the generation is in the set,
  or not yet installed, so the generation is still listed); and a crash between
  the skipped streamed `RecallCut`'s apply and `persist_void_cut` restarts with the end durable and no key, so the
  open voids at the log index, not the record's cut (over-refusal of dependencies between the two, no reordering).
- A latent bug on the base the branch moves from (f188c707): the rolled-back
  hole pass (`hole_tail_rolled_back_tx`) scanned the backup tail's keyspace up to
  `MEMBER_COPY_BASE`, which took in a delegate's rows at `1 << 48` to `2 << 48`,
  and decoded them as `BackupTx`; a `DelegateTx` with a `rid` misparses, which
  either errors (aborting the `backup_append` or strand transaction) or yields
  garbage that matches no identity. Moving the delegate's slots to `1 << 62` and
  stopping the pass and `backup_clear` at the member copies' base fixes it.
- The CLI's abandon-group wait line shows a number as `epoch:n (number)` and the
  daemon's log lines as `epoch:n` (`GenLabel`); `--gen` takes the number, and the
  daemon's control replies print the number alone.

**May revert**: never as a whole; under plan 46 M1.2's one stream per node the minting
site moves to the `DelegateNode` record, the rule does not.

See: `crates/meta/src/delegation.rs` (`next_delegation_gen`, `GEN_MAX_EPOCH`,
`gen_epoch`), `crates/authority/src/core/delegate.rs` (`delegate_dir`),
`crates/meta/src/session.rs` (`seed_generations`, `reaches_deps`),
`crates/meta/src/store/backup.rs` (`DELEG_BACKUP_BASE`); tests
`two_tenures_never_mint_the_same_generation_number`,
`crates/meta/tests/generation_epochs.rs`, sim
`regression_no_generation_number_is_minted_twice`;
[plan 46](../plans/v1/wip/46-committed-or-final.md) M0.3, §5.2; PROGRESS "ADR-43".
