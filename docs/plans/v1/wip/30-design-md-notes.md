# Plan 30 M16 — notes for the DESIGN.md rewrite (§4–§6, §9)

Input for the coordinator's rewrite of `docs/explanation/DESIGN.md`
(coordinator-only, per plan 30 §M16). For each section: the stale
sentences, quoted with their line numbers in DESIGN.md as of main
`df14686`, then what the new text must say, with code references. The
decisions behind the new text are ADR-18 to ADR-28 in
[DECISIONS.md](../../../explanation/DECISIONS.md); the user-facing detail
is in the new reference pages ([cto modes](../../../reference/features/cto-modes.md),
[durability and failover](../../../reference/features/durability-and-failover.md),
[delegations](../../../reference/features/delegations.md),
[cluster locks](../../../reference/features/cluster-locks.md)) and the
updated [forwarded mutations](../../../reference/features/forwarded-mutations.md).
The rewrite can link to those instead of repeating them.

Code paths are relative to the repository root. "Core" means the
sans-IO authority core, `crates/authority/src/core/`.

## Table of Contents

- [Constraints to state up front](#constraints-to-state-up-front)
- [§4 Metadata Plane](#4-metadata-plane)
- [§5 Write Authority](#5-write-authority)
- [§6 Consistency Modes](#6-consistency-modes)
- [§9 Failure Handling](#9-failure-handling)
- [Stale text outside §4–§6 and §9](#stale-text-outside-46-and-9)
- [§3 and §14: the small-file write path](#3-and-14-the-small-file-write-path)

## Constraints to state up front

Nothing in DESIGN.md states plan 30's hard constraints. The rewrite
should add them once (in §1, or at the top of §4) and refer back:

1. **Portable S3 only.** GET, PUT, LIST, DELETE, plus `If-None-Match: *`
   and `If-Match` on PUT. No S3 Express One Zone, no conditional DELETE,
   no `RenameObject`. `fs create` preflights exactly these
   (`crates/store-s3/src/store.rs`, `ChunkStore::preflight`); `doctor`
   probes them and the providers' error codes
   (`crates/store-s3/src/probe.rs`, `probe_cas_semantics`). ADR-27.
2. **One bucket.** No disaster-recovery bucket, no quorum across
   buckets; bucket outages are survived with authority that already
   exists (held leases, designations, continuation epochs). ADR-27
   retires plan 28 §P12.
3. **Single node degrades to plain behaviour.** No peer means no
   backup, no delegation, no promise, no message.
4. **No WAN round trip on every write.** Backups are chosen by measured
   RTT (`CONSTELLATION_BACKUP_RTT_BUDGET_MS`), never by assuming a LAN.
5. **Safety never depends on failure detection** (ADR-12, amended):
   timeouts only decide *when* to try; seals, log-slot CAS, lease
   margins and promises decide *whether*.

## §4 Metadata Plane

### Stale

- **L232–245, "Local store"**: "Every node keeps a **full metadata
  replica** in SQLite (chosen by benchmark — see DECISIONS.md ADR-9 …)"
  and the SQL schema (`inode`, `dentry`, `partition`, `cache`,
  `journal` tables). The store has been fjall 3 since plan 29, with the
  plan 28 §P6 key encoding in an `ns` keyspace plus node-local
  keyspaces (`crates/meta/src/store/mod.rs`). Plan 30 added the
  node-local keyspaces `completed` (M2), `spec`, `spec_live`,
  `pending_replay`, `journal_tx` (M3), `backup_tail` (M9), plus the
  delegation table row in `ns` (M11). The `partition` table is gone.
- **L252–256**: "Each **partition** (see below) has one ordered log stream
  on S3." and the record shape `{seq, txid, node_id, sig, op, args...}`
  with `op ∈ {… part_split, part_merge, rename_xpart …}`. There is one
  stream (`p0`, `crates/store-s3/src/log.rs`, `PARTITION`); records are
  `LogRecord` (`crates/meta/src/record.rs`), which now include
  `Completed { rid }`, `Refused { rid, errno }`, `InboxAck`,
  `Delegate { dir, node, gen, designated, range }`, `Recall { dir, gen }`
  and `TailFollows { prev_epoch }`. `PartSplit`/`PartMerge`/`RenameXpart*`
  no longer exist (plan 29 M0a).
- **L268**: "Followers tail segments (poll + gossip push) and apply them to
  their replica." Gossip no longer pushes segments (M7).
- **L269–271**: "Periodic **checkpoints** compact a partition's history;
  segments older than the checkpoint are GC'd." Checkpoints were
  replaced by plan 28's commit chain; log retention floors on the head
  commit's `applied` position **and** on
  `CONSTELLATION_COMPLETION_RETENTION_S` (M2's coverage rule,
  `crates/cli/src/gc.rs`, `metadata_candidates`).
- **L273–281, "Partitions"**: the whole subsection ("Partitions **split and
  merge automatically** … Cross-partition renames use a linked
  two-record commit …"). Remove it. Plan 29 M0a deleted partitions;
  plan 30 M11 replaces the scaling role with delegations over one log.
- **L285–288**: "A lease on `leases/<part>.json` … grants one node
  exclusive write authority over a partition. The holder is also the
  partition's **sequencer**". One root lease (`leases/p0.json`) grants
  exclusive *append* authority over the one log; sequencing of a
  delegated subtree belongs to its delegate.
- **L290–293**: "then flushes segments asynchronously (bounded lag, default
  ~5 s / ~4 MiB). A forwarded ack means the holder journal contains the
  records; it does not mean the segment is already on S3." The holder
  ships every sync round (segments capped at 4 MiB,
  `Config::segment_max_bytes`), and what an acknowledgement means now
  depends on the lease's `ack_policy` (see below).
- **L294–295**: "A holder releases after about 30 seconds without a
  mutation". Leases are sticky: an idle release needs a registered
  requester (`wanted_by`), an empty journal, 30 s idle and a 5 s dwell;
  and never while a delegation generation or lock grant is live
  (`crates/authority/src/core/jobs.rs`, `round_release`;
  `core/holder.rs`, `on_lease_request`).
- **L296–299**: medoid placement is still true for the root lease, but
  it is no longer the main placement mechanism (subtree placement,
  below).
- **L305–308**: "Expired lease + unreachable holder: takeover is legal only
  after applying everything the holder flushed; the holder's unflushed
  tail (bounded lag) becomes an offline branch handled by reintegration
  rules". There is no offline branch any more: the unflushed tail is
  rolled back and replayed by rid (Layer A), or re-shipped by a sealed
  backup (Layer B), or does not exist (`ack=s3`).

### What the new §4 must say

1. **Local store**: fjall, the `ns` keyspace plus node-local keyspaces;
   one sentence each on `completed`, the speculation keyspaces,
   `backup_tail`. Link plan 29 for the engine.
2. **One log, one root lease.** Segments `log/p0/<seq:016x>.zst`,
   created with `If-None-Match` (the sequence collision is the conflict
   detector), stamped with the lease epoch (fencing). Every segment
   carries the journal position it ships through (`through`,
   `crates/authority/src/segment.rs`), and a transaction is never split
   across segments (`Replica::whole_tx_prefix`), so an op and its
   `Completed { rid }` land together.
3. **Request ids (ADR-18).** Every mutation carries
   `Rid { node, incarnation, seq }` (`crates/meta/src/rid.rs`), assigned
   once per FUSE op and kept across every retry, replay and path. The
   incarnation is bumped at every mount (`Meta::bump_incarnation`).
   Executed ops append `Completed { rid }`; definitive refusals append
   `Refused { rid, errno }` (`core/holder.rs`, `record_refusal`). Every
   replica keeps `rid → outcome` in `completed`; the sequencer dedups
   against it and an in-memory `recent` map. In-doubt requests retry the
   same rid (same holder ×3 with backoff, up to two redirects, then the
   lease path, which tails to head and checks `completed`); log GC keeps
   every segment younger than the completion retention (900 s).
4. **The replica is a log prefix plus explicit speculation (ADR-19).**
   Every effect ahead of the log (a requester's shadow, an `Exists` hint,
   the holder's unshipped transactions, pre-S3 streamed records, a
   delegate's unappended transactions) is captured with before-images in
   `spec` through the one `ns` write funnel
   (`crates/meta/src/store/ns.rs`, `Dirty::capturing`;
   `crates/meta/src/store/spec.rs`). A later epoch strands older
   speculation: rollback, redo, replay by rid (`core/replay.rs`); a
   refused replay is a `.constellation-conflict/` copy. A takeover
   ships an epoch marker and runs its gate (strand, apply backup tail,
   replay queue, inbox drain) before its view opens
   (`core/jobs.rs`, `complete_gate`). Only the holder publishes commits,
   substituting before-images for its unshipped keys
   (`Meta::publish_basis_at`), so every commit is a log prefix.
5. **Positions (ADR-20).** Every reply carries the position it was
   evaluated at (`crates/meta/src/session.rs`, `Position`); followers
   receive shipped segments over direct log streams from the holder
   (`core/stream.rs`, `LogSubscribe`), falling back to S3 tailing.
6. **Leases and acknowledgement.** The lease object
   (`crates/store-s3/src/lease.rs`, `Lease`) now carries `backups`,
   `config_version`, `ack_policy` (`Local | Backup | S3`),
   `granted_delegations` and `retired`, besides holder, epoch, expiry,
   `released` and `wanted_by`. Summarize the three layers (ADR-21) and
   point to §9 for failover.
7. **Delegated sub-sequencers (ADR-23).** `Delegate`/`Recall` records
   keep a delegation table; ownership rule (dentry by parent, inode keys
   by primary link's parent); delegate executes as speculation and
   streams to the root with `deps`; the root appends in order without
   re-validating; cross-subtree ops recall first; no 2PC; placement by
   dominant writer (70% in, 50% out, rate floor, dwell, cool-down);
   GIGA+ name-hash ranges for hot directories, only where a range has a
   dominant writer; HLC timestamps with `max` merges and shared parent
   holds (`crates/meta/src/hlc.rs`, `crates/meta/src/replay.rs`,
   `TouchSet`). Code: `core/delegate.rs`, `core/placement.rs`,
   `crates/meta/src/delegation.rs`.
8. **The S3 inbox (ADR-24).** Without a P2P path to the holder, a
   requester writes batches `inbox/<epoch>/<node>/<n>`; the holder polls
   with adaptive backoff, outcomes ride the log; sustained demand
   escalates to a lease request (8 ops or 1.5 s of waiting per 10 s).
   Code: `core/inbox.rs`, `crates/store-s3/src/inbox.rs`.
9. **Locks (ADR-25)** belong in §6 but the lock table lives with the
   sequencer; one sentence here pointing to §6.

## §5 Write Authority

### Stale

- **L315–317**: "Three roots exist; … Safety never depends on failure
  detection — heartbeats (`heartbeat/*`, P2P keepalives) feed status UX
  only." `heartbeat/<node>` objects are now M10 promises, which a TTL
  takeover reads when `epoch_slack > 0` (`crates/store-s3/src/heartbeat.rs`,
  `takeover_check`), and holder silence triggers a backup's seal and an
  `ack=s3` fast takeover (`core/backup.rs`, `on_backup_watch`,
  `watch_s3_holder`). Still safety-independent, but not UX-only.
- **L319–323**: "Forwarding adds **requesters**, not appenders. The lease
  holder remains the only node that assigns authoritative order and
  appends the partition log. … An unreachable or declining holder makes
  the requester fall back to the ordinary S3-backed lease path." The
  holder is still the only *appender*, but delegates assign order within
  their subtrees; and an unreachable holder now means same-rid retries,
  then the S3 inbox (sustained demand escalates to the lease path), not
  an immediate lease fallback.
- **L335–341 (§5.2)**: "Other nodes' authority for the subtree becomes a
  short-TTL *delegation* granted by the designee, and their flushes
  require the designee's ack." Since M11 a designation is a
  non-stealable delegation to the designee (`Delegate { designated: true }`):
  other nodes' ops under the path are *forwarded to and sequenced by*
  the designee; an op reaching the root under a designation is refused
  `EROFS`, a cross-subtree op involving one `EXDEV`
  (`core/delegate.rs`, `on_control_sync_designations`,
  `deleg_recall_plan`). `--ro` designations create no delegation
  (`crates/cli/src/designation.rs`).
- **L348–358 (§5.3)**: "If S3 is unreachable but the P2P-connected
  component contains **all write-eligible nodes** … Majority quorum is
  deliberately insufficient … If the component loses a member mid-epoch,
  the remaining nodes go read-only; … both sides freeze". With
  `epoch_slack = f` the epoch needs `N − f` members, and the missing
  node is held off by the promise rule (below). Also missing: the claim
  rule (`S3` leases never carried, `Backup` leases only with every
  backup a member), a member that reaches S3 declines, and local-only
  acknowledgement inside an epoch.
- **L360–364**: "an unmounted or P2P-unreachable node still counts until
  explicitly retired, so a remaining component cannot open an epoch
  while a still-enrolled writer might take expired leases via S3." True
  only for more than `f` such nodes.
- **L366–374, availability matrix**: row "S3 down, writer missing from
  LAN | read-only | read-only" is wrong with `f > 0` and at most `f`
  missing. Missing rows: a holder with a backup crashing (≈ 1.5 s
  failover), `ack=s3`, a delegate crashing, a delegate partitioned from
  the root.

### What the new §5 must say

1. **The authority chain** (unchanged rule, L312–313), now with these
   roots and derived grants: the root lease (S3 CAS); offline
   designations (non-stealable delegations); continuation epochs; and,
   derived from the root lease and capped by it, delegations
   (`CONSTELLATION_DELEGATION_TTL_MS`), read delegations and lock
   grants. Every derived grant uses one time discipline: the holder
   honours it until `sent + ttl − margin`, the grantor outwaits it until
   `granted + ttl + margin`, margin = the lease's expiry margin
   (`min(1 s, TTL/4)`), safe while `margin > 2 × drift`.
2. **Requesters, sequencers, appender.** Requesters forward to the
   owning sequencer (holder or delegate) with a rid; delegates sequence
   their subtrees; only the root appends. Unreachable owner: same-rid
   retries, then the inbox (P2P gone) or the lease path.
3. **§5.2 designations** as non-stealable delegations (above).
4. **§5.3 flexible continuation epochs (ADR-22).** `epoch_slack = f`
   (`meta.json`, `fs create --epoch-slack`, `fs set epoch-slack`);
   `N − f` members; promises (`heartbeat/<node:016x>.json`,
   `{node, no_epoch_until_unix_ms, epoch_slack, written_unix_ms}`),
   persisted locally before the PUT, published on demand only; join
   only after one's last issued promise expired; an S3 TTL takeover of
   another node's expired lease needs `f` other nodes' promises past the
   lease's expiry; promise TTL ≤ lease TTL / 4; the largest advertised
   slack wins; admin `leave --node-id` fences the retired node's leases
   (`Lease::retired`, `fence_retired`). The claim rule for fast-takeover
   leases (`core/promise.rs`, `lease_may_carry`; `core/backup.rs`,
   `epoch_may_carry`). The known gap: a node that enrolls during an
   epoch. Code: `core/promise.rs`, `crates/store-s3/src/heartbeat.rs`,
   `crates/net/src/epoch.rs`, `crates/cli/src/epoch.rs`.
5. **Availability matrix** with the rows above.

## §6 Consistency Modes

### Stale

- **L378–380**: "**close-to-open (default)**: `open()` sees the latest
  completed `close()` cluster-wide; concurrent writers on different
  nodes serialize via lease transfer." The default (`--cto bounded`) is
  bounded staleness plus per-node session guarantees; cluster-wide
  close-to-open is `--cto strict`. Concurrent writers serialize at the
  owning sequencer by forwarding, not by lease transfer.
- **L381–382**: "**strict (per mount/subtree)**: cross-node byte-range
  `fcntl` locks and pre-close visibility; every conflicting op pays
  coordination RTTs." Locks are now separate from the cto mode:
  `--locks cluster` is the default with P2P, per daemon (not per
  subtree), `flock` included. There is no pre-close visibility mode.
- **L383–385**: "**relaxed** … conflicts *detected* and materialized on
  reintegration". Not built. Either drop it or mark it unbuilt.
  Conflict copies now come only from refused replays of stranded ops.
- **L399–404, invariant 2**: "Every conflicting write is validated by the
  partition lease holder … Holding or acquiring the lease requires
  applying the predecessor's flushed log (§4)." Validated by the owning
  sequencer (holder or delegate); acquiring also requires the takeover
  gate (stranded speculation rolled back and replayed, backup tail
  applied, inbox drained).
- **L406–412**: "the requester immediately stores the acked records as a
  local **shadow** … When the holder publishes a small segment,
  `SegmentPublished` may carry the compressed segment bytes; peers can
  apply that payload directly. Larger segments carry only the usual hint
  to tail S3." Gossip carries no payloads since M7; the shadow is
  speculation with before-images that can be rolled back; it is
  installed only once the replica reaches the reply's `base`.
- **L417–422 (worked example)**: "B applies the record via gossip-pushed
  tailing (milliseconds on a LAN)". Via the direct log stream (or a
  strict ReadIndex, which makes the out-of-band case hold by
  construction).

### What the new §6 must say

1. **Session guarantees, always (ADR-20).** Positions on replies; the
   `observed` watermark raised only by replies whose effects are not
   installed; every FUSE read path waits (bounded by
   `CONSTELLATION_SESSION_WAIT_MS`, degraded not failed); gives
   read-your-writes and monotonic reads per node. Code:
   `crates/meta/src/session.rs`, `core/client.rs` (`observe`),
   `crates/cli/src/fusefs.rs` (`session_wait`).
2. **`--cto bounded` (default) and `--cto strict`.** Strict: ReadIndex
   to the owning sequencer on `open`, `lookup`, first `readdir` chunk;
   the answer is a position (not a record); read delegations (5 s,
   lease-capped, horizon persisted before answering); recall or
   outwait before any acknowledgement of a mutation touching a
   delegated inode (`Held` replies); P2P off degrades to "tail S3 to
   head". Kernel TTL 0 under strict, except a lone sequencer. Code:
   `core/readindex.rs`, `crates/meta/src/readdeleg.rs`,
   `crates/cli/src/cto.rs`. The default decision is M16's (EC2
   measurements).
3. **Direct log streams** replace ADR-17's gossip payloads
   (`core/stream.rs`), plus pre-S3 streaming as speculation under a
   backup.
4. **Cluster locks (ADR-25).** Whole-file leased grants at the owning
   sequencer, byte ranges resolved on the node; recall with
   flush-before-release; position + kernel cache drop on grant;
   `EIO` fencing of a lapsed grant; grace and reclaims after a fast
   takeover; nothing to reclaim after a TTL takeover; never through the
   inbox; limits (no interrupt of a blocked wait, no `EDEADLK`, `flock`
   and `fcntl` of one process conflict). Code: `core/locks.rs`,
   `crates/meta/src/locks.rs`, `crates/cli/src/locks.rs`.
5. **"Staleness, precisely"**: invariant 1 still holds (kernel caches
   are invalidated on apply, now pushed with `FUSE_NOTIFY_INVAL_*`,
   `crates/cli/src/kernel_inval.rs`); invariant 2 as amended above.

## §9 Failure Handling

### Stale

- **L558, "node crash"**: "journal + epoch promises replay from local DB".
  Also true of backup tails, seals, promises and the speculation log;
  note that "persisted" is fjall `PersistMode::Buffer`
  (`crates/meta/src/store/mod.rs`, `Meta::sync` doc): it survives a
  process crash, not a power loss, until an `fsync()` or clean
  shutdown syncs it. Since the M16 fixes, safety state (promises and
  the epoch join gate, epoch state, seals, the read-grant horizon) is
  synced before it is acted on; backup appends are not, which is the
  documented Layer B contract (a simultaneous power loss of holder and
  backups is out of it; ADR-21,
  [durability](../../../reference/features/durability-and-failover.md#what-on-disk-means)).
- **L559, "S3 outage"**: "writes continue under held leases/epoch".
  Under `ack=s3` acknowledgements stall at once (they wait for S3), and
  an `S3` lease is never carried into an epoch.
- **L560, "lease holder unreachable"**: "requester tries handoff, then
  falls back to lease release/TTL and S3 CAS takeover after applying the
  flushed log". Now: same-rid retries, then the inbox if the P2P path is
  gone, else the lease path; with a backup, a seal-based takeover in
  about 1.5 s; with `ack=s3`, a fast takeover by any peer; a non-backup
  must wait 3 s past a `Backup` lease's expiry; with `f > 0`, a TTL
  takeover needs promises.
- **L561, "holder crashes after forwarding ack"**: "acked records may
  remain in its stranded journal; lease fencing prevents a second
  history and reintegration surfaces the stranded branch". Now: under
  `Local`, requesters replay by rid (nothing acknowledged is lost unless
  the requester is also lost); under `Backup`, the sealed backup
  re-ships the tail; under `S3`, nothing acknowledged was unshipped. A
  deposed holder rolls back and replays by rid; only true overlaps
  become conflict copies.
- **L563, "clock skew"**: still right, but should name the margin rule
  (`margin > 2 × drift`) that every leased grant now relies on.
- **L596–601**: "At TTL expiry the subtree goes **read-only**: … a node
  with working S3 can legally take the expired lease". With `f > 0` the
  taker also needs `f` promises; a `Backup` lease has the 3 s
  non-backup grace; the FUSE client deadline is 2×TTL, after which an
  in-doubt op fails with `EIO` (never re-executed).
- **L605–610, escape hatches**: "**Continuation epoch** (§5.3): all
  write-eligible nodes in one P2P component keep writing collectively".
  `N − f` of them.
- **L612–615, three-machine example**: with `f = 1` the laptop alone
  cannot form an epoch (it is 1 of 3), but the desktop and server can
  while the laptop is away; state which `f` the example assumes.

### What the new §9 must say

1. A failure table keyed by topology, like plan 30 §3's table: single
   node; only distant peers (Layer A, TTL failover); a peer within the
   RTT budget (Layer B, ≈ 1.5 s, nothing acknowledged lost); `ack=s3`
   (Layer C, fast takeover with P2P, TTL without).
2. **Seal-based failover** (`core/backup.rs`): seal persisted before the
   CAS; the old holder cannot collect a write-all acknowledgement after
   the seal; backup tail re-shipped deduplicated by rid; `TailFollows`
   marker; reconfiguration by lease CAS (`config_version`); the
   read-delegation and lock grace after a fast takeover
   (`ack_floor_waits`).
3. **Observers of tentative effects**: under `Backup`/`S3` no client
   observes an acknowledged-but-undurable effect of another client
   (`Meta::durability_pending`).
4. **Delegate failures**: crash without backup (reclaimed after
   `ttl + margin`, replay by rid), with backup (root seals the backup),
   partition from the root, root failover with delegates (grants capped
   by the old lease; delegates re-stream).
5. **Lock holders**: partitioned past the lock TTL → `EIO` on the locked
   files until unlocked.
6. **What an S3 outage blocks**: add the inbox (a P2P-less requester
   cannot write at all during an outage, since the inbox is in the
   bucket), `ack=s3` stalls, and the flexible epoch rule.
7. **Poison records** (plan 30 M4): an unrecoverable pending chunk holds
   back only its inode's records and their dependents; `status.held`
   and `constellation repair drop-held`.

## Stale text outside §4–§6 and §9

Not in the rewrite's scope, but made stale by plan 30 (or earlier) and
worth a follow-up:

- **§1 diagram, L19 and L26**: "meta store (SQLite, engine trait)" and
  "S3 bucket: chunks/ log/ leases/ checkpoints/".
- **§1, L39–40**: "survivors can form continuation epochs without waiting
  forever" (with `f > 0`, up to `f` missing nodes never block).
- **§2 layout, L48–58**: `nodes/<node-id>.json` is `registry/<id>.json`;
  `heartbeat/<node-id>  # liveness beacons (~15-30 s), UX only` is
  `heartbeat/<node:016x>.json`, M10 promises, on demand, absent at
  `f = 0`; `leases/<partition-id>.json` is `leases/p0.json`;
  `log/<partition-id>/<seq>.zst` is `log/p0/<seq:016x>.zst`;
  `checkpoints/…` is gone (`commits/<seq:016x>`, `packs/`, `blobs/`);
  missing `inbox/<epoch>/<node>/<n>`, `designations/`, `gc/`.
  Source: `crates/store-s3/src/layout.rs`.
- **§3, L137–139**: "`fsync` modes: **default** … **paranoid**". The
  flags are `--fsync-mode local|s3` and `--write-mode through|back`,
  and the filesystem's `ack_policy` (`fs create --ack-policy
  local|s3`; there is no per-mount `--ack` since M16) sets what an
  acknowledgement means.
- **§7, L441–462**: cooperative cache described as bloom digests; the
  default is exact mirrors with range-based reconciliation (plan 30
  M15, ADR-26, `crates/net/src/reconcile.rs`); blooms remain as
  `CONSTELLATION_COOP_DIGEST=bloom`.
- **§8, L484–485 and L495–501**: "the live record stays and still blocks
  continuation epochs until retired" (only beyond `f`); admin leave now
  also fences the retired node's leases and makes epoch members abandon
  an epoch carried by it.
- **§11, L649–653, and §12, L663**: SQLite replica sizes and "the
  partitioned log format".
- **§12, L698–700**: "Segment push — including direct application of
  small `SegmentPublished` payloads — shortens the freshness window".
- **§14, L828–829 and L873–877**: "log segments older than the newest
  checkpoint" and "rebuilds its replica from the latest checkpoint".
- **GOALS.md, "Three machines, one life"**: "they form a continuation
  epoch (all write-eligible nodes present)" — true at `f = 0` only.

## §3 and §14: the small-file write path

From the fix for the OVH run's finding 3 (PROGRESS.md, "Fix: small-file
write path round trips"). Code: `crates/store-s3/src/gc.rs`
(`CondemnedView`), `crates/store-s3/src/store.rs` (`put_chunk_mode`,
`chunk_durable`), `crates/meta/src/store/remote.rs`,
`crates/cli/src/fusefs.rs` (`defers_upload`,
`commit_manifest_forwarded`), `crates/cli/src/main.rs` (the upload pass,
`put_mode`), `crates/authority/src/core/backup.rs` (`stream_ahead`,
`on_stream_ahead`), `crates/cli/src/gc.rs` (the delete loop).

- **§14, "The dedup race, and the layered defense", item 2.** The text
  says writers re-read the condemned pointer at lease renewal. The code
  never did: it read it before *every* chunk upload, which put a GET in
  front of every unique small file. Now the pointer is read only after
  S3 said the object already exists (a dedup hit), and the rule is:
  - **the object was absent** (the create created it, a `Probe`'s HEAD
    missed): no read. With the object absent a create and an
    unconditional PUT have the same effect and result, and the pointer
    only ever chose between them, so the old order would have left the
    bucket in the same state;
  - **the object was there** (at `t_e`): read the pointer after it
    (`t_r > t_e`). Listed → upload the bytes (as before). Absent (never
    published) → sound. Unlisted and with the same identity (`epoch`,
    `published_ms`) as the last read that completed before the
    existence request was sent → sound: no publication landed in
    between, so the round whose list was current deleted nothing of the
    object, and earlier rounds finished before `t_e`. Otherwise (it
    moved, or nothing was observed before: the first hit after a mount)
    → a `HEAD` after the read decides (found: "read, then object found"
    is the old order exactly).
  Every execution is one the old order could produce, so the rest of the
  argument (horizon, TTL wait, post-wait tail and re-check) carries over.
  The read is `If-None-Match` on the last ETag: a `304` while nothing
  changed. Same premise as before: a round deletes only while its own
  list is current (singleton GC lease; the pointer's CAS epoch).
- **§14, same item: "an identical-content PUT is idempotent and
  resurrects the chunk".** Only if the PUT lands after the delete. A
  writer that read the pointer late in the TTL wait, re-PUT the chunk
  and committed after the round's second tail (a write-back ship lagging
  its upload can) had its chunk deleted from under the commit. The
  delete loop now keeps an object whose `Last-Modified` moved past the
  marked listing's (by more than a HEAD's one-second resolution): the
  object is not the one marked. The HEAD→DELETE gap stays (no
  conditional DELETE in portable S3); a write inside it needs a
  condemned hit re-PUT in that very gap *and* a commit after the second
  tail.
- **§3, "Write path".** Add the per-mode close contract and its request
  count for a small unique file (one chunk), measured by the harness
  `small-file-write-path` (200 ms per S3 request): `through`, sequencer
  and non-owner alike, 1 S3 request (the conditional create) and one S3
  round trip per close (was 2 on the sequencer: pointer GET + create,
  and 3 on a non-owner whose ladder had switched to `Probe`: pointer
  GET, HEAD, PUT); `back`, 0 round trips in the close on either node
  (was: a non-owner's `back` close = its `through` close). A chunk below
  `CONSTELLATION_PROBE_MIN_BYTES` (256 KiB) probes only on a positive
  hint (existence cache, peer digest), never on the adaptive policy's
  guess.
- **§3/§5, a non-owner's `back` close** (new). It forwards the manifest
  at once, naming the chunks still pending on it; the sequencer enrolls
  them as pending uploads of its own, marked remote, *before* executing
  the op, so every existing gate applies: the root's ship plan defers
  the transaction and its dependents (M7), a delegate's stream and
  backup feed stop before it (`Meta::delegate_txs_from`), the pre-S3
  stream stops before it for everyone but the forwarder (which has the
  bytes and whose close may be waiting for the transaction). The
  forwarder reports the chunks once they are up (`Payload::
  ChunksDurable`); the sequencer checks S3 itself as a fallback (2 s,
  doubling to 16 s). Nothing about the forward changes: the base check
  and rebase, exactly-once rid, shadow, stranding and replay (the
  replayed forward names what is still pending then). The log still
  never names a chunk S3 lacks. Readers on the sequencer — the only node
  with the manifest before the chunk is up — wait for the chunk
  (`CONSTELLATION_REMOTE_CHUNK_WAIT_S`, 60 s). Why not the literal "defer
  the forward until the upload": the close would be acknowledged before
  the sequencer validated it (a `Conflict` after the close has no write
  session left to rebase: an acknowledged close would become a conflict
  copy), `--cto strict` would no longer see a completed close, and the
  node's own reads would need a new kind of speculation.
- **§6/§9, pre-S3 streaming (M9).** Two subscriber fixes the faster
  closes exposed: a `StreamAhead` batch that overtakes the segment it
  follows waits for it instead of being dropped, and a segment behind
  what the stream already installed no longer pulls the stream's cursor
  back. Before, either sent a forward waiting for its transaction to the
  log (one S3 round trip more per non-owner close).

