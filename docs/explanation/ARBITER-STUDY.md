# Design study: should S3 stop being the arbiter?

Status: study, no code changes. Written against constellation `main`
`a0b251d` (2026-10-16), the week's host-load timing review, the reviews of the F1 and F2
fixes and of the acknowledged-write-loss fix (2026-10-16), and the git
log since 2026-10-01. External sources are cited inline; §8 lists
them.

## 0. The question and the short answer

Today's rule: S3 conditional writes are the arbiter of record (the root
lease, the lease epoch, every log slot, the commit chain, membership,
designations, promises), and everything peer-to-peer (forwarding,
delegation, backups, log streams, fast-path acknowledgements) is a fast
path whose failure costs latency, never correctness. The question is
whether the data-corruption and ordering bugs the campaigns keep finding
would go away if S3 were demoted to storage (chunks, checkpoints) and a
quorum-replicated P2P log (Raft / Multi-Paxos / Vertical Paxos) became the
only way a file-system change is ordered and acknowledged.

The short answer, argued in the rest of the document:

1. The bugs that keep recurring are not caused by *where the arbiter is*.
   They are caused by **acknowledging before agreement** (five kinds of
   speculation that are rolled back and replayed by rid), by **copies of
   one operation living in several queues** (a delegate's own rows, the
   root's appended copy, a backup tail, a subscriber's streamed copy),
   and by **authority state that lives outside the log and moves in
   messages** (lock tables handed back and forth, restamped, revived by
   late answers; delegation generations re-adopted from counters). A
   quorum log removes those *only if* its acknowledgement rule is
   "committed by the replica set, or not acknowledged" and *only where a
   replica set exists*. The same rule can be applied to the current
   architecture without moving the arbiter (§5, §6).
2. The current design is already, structurally, a Vertical-Paxos /
   PacificA / Delos-shaped system: S3 is the configuration master (one
   CAS object per filesystem decides the epoch and the replica set), the
   holder plus its listed backups are the replica group (write quorum =
   all, read quorum = any one), the seal is the fence that lets the
   master reconfigure safely, and the S3 log slots are the durable
   archive of what the group committed. What the design does *not* do is
   treat group agreement as commit: commit is still defined as "in an S3
   slot", so everything the group agreed on is still speculation until
   the ship, and everything acknowledged without a group (Layer A, a
   delegate's own acknowledgement, the FUSE fast path) is speculation
   with no group at all. That dual commit point is where the recovery
   machinery (rollback, redo, replay by rid, folds, conflict copies,
   `TailFollows`, "nobody observes a tentative effect" waits) comes from.
3. A majority-quorum Raft/Paxos rewrite would bring back exactly the
   problems the design was built to avoid: a WAN round trip on every
   write whenever the voters span continents (hard constraint 4), no
   fault tolerance at two nodes without a witness, no writes at all
   through an S3-less partition unless the laptop problem is solved
   again with promises, and a second trust root beside IAM. It would also
   discard about 70k lines of authority code, the Stateright models and
   the deterministic simulation, for a protocol whose own ecosystem
   (etcd, CockroachDB, TiKV) still fights leader-lease, clock and
   election-storm-under-load bugs of the same family as F2 and L0.
4. Recommendation (§5): **keep S3 as the configuration master and fence,
   keep the P2P replica group, and change the commit rule, not the
   arbiter.** Concretely: (a) define commit as agreement of the configured
   replica set wherever one exists, (b) acknowledge nothing that is not
   committed or covered by an authority that cannot be taken over
   (offline designation), (c) move lock grants, releases and expiries and
   the delegation stream positions *into* the log so no authority state
   moves by message, (d) replace replay-by-rid-with-re-execution by
   log shipping for committed records and by a plain failure (`EIO`,
   same-rid retry) for uncommitted ones, (e) keep Layer A speculation in
   exactly one place, the lone or distant holder's own unshipped journal,
   and document it as the offline-first contract it is. The phased
   migration in §6 keeps the system correct at every step and reuses the
   sim, the models, the fencing tokens, the chunk store and the E2E layer
   unchanged.

## 1. The current design, stated as guarantees

### 1.1 What rests on S3 today, and why

From `DESIGN.md` §1, §4, §5, `DECISIONS.md` ADR-2, ADR-12, ADR-21,
ADR-22, ADR-27, plan 30 §2:

| Guarantee | S3 mechanism | Why S3 and not peers |
|---|---|---|
| Exactly one appender to the metadata log (write authority) | `leases/p0.json`, CAS-created or CAS-swapped when expired or released; epoch in the object | "S3 is the only required infrastructure" (GOALS); a lone node must work; a laptop away for days must not block a cluster of two (ADR-2 rejects Raft: "a 2-of-3 quorum cannot distinguish laptop offline from partition") |
| A deposed holder's late write never lands | `log/p0/<seq>` created with `If-None-Match: *`; segment stamped with the epoch; the successor's epoch marker takes the next slot | The fence must work when the deposed holder cannot be reached (dead, partitioned, paused); only the store both sides write to can be that fence |
| The replica set and the acknowledgement policy of a tenure | `backups`, `ack_policy`, `config_version` in the lease object; backup add/remove and a backup's takeover are CASes on the same version, so exactly one wins | Configuration must be agreed by something outside the group (the Vertical Paxos "master") |
| Membership and write-eligibility (the roster an epoch needs) | `nodes/<id>.json` CAS-claimed; tombstones; never recycled | Membership = bucket access (ADR-7) |
| Non-stealable write authority for an offline node | `designations/<path-hash>.json` CAS | Must be claimable with nothing but S3 and survive any partition |
| Keeping writing through a bucket outage without two writers | continuation epochs: `N − f` members plus promises in `heartbeat/` | The danger is a missing node that still reaches S3 and takes an expired lease |
| Forwarding with no P2P path | `inbox/<epoch>/<node>/<n>` | P2P is optional; S3 is not |
| Liveness proof for a fast tenure's renewals (plan 41 L0) | a recent successful lease read/renewal | A root sealed by its backups must stop extending windows; S3 is the one place the seal and the root meet |
| Durable archive of the namespace and its history | segments, `commits/`, `packs/`, `blobs/`, `snaps/` | storage |
| Content | `chunks/` content-addressed | storage |

Time-bounded derived grants (delegations 5 s, read delegations 5 s,
lock grants 20 s) are capped by the lease and share one clock
discipline: holder honours until `sent + ttl − margin`, grantor until
`granted + ttl + margin`, margin `min(1 s, TTL/4)`, each a duration on one
node's clock, so correct for any offset between the nodes (DESIGN §5; the lock
cut between a root and a delegate is the exception until plan 43 C7b's last
commit). None of these touches S3
on the hot path; they touch it through the lease they are capped by.

The original rationale for S3 as arbiter (GOALS "Key enablers", ADR-2,
plan 30 §2) was threefold: conditional writes made object storage a
*linearizable CAS* in 2024; the product goal is "S3 is the only required
infrastructure" with a lone node degrading to plain behaviour; and the
offline-first scenario (desktop, laptop, server) makes majority quorums
the wrong primitive, because the member that is away is the one that
wants to write.

### 1.2 What is acknowledged before S3 has it

The design has three durability layers (ADR-21) and, orthogonally, four
places where a node applies an effect before the log has it (ADR-19):

| Acknowledged by | Who holds it | What can take it back |
|---|---|---|
| `Local` holder (Layer A) | the holder's disk (fjall buffer mode) | a deposition: rollback from before-images, replay by rid through the successor; refusals become conflict copies |
| `Local` delegate | the delegate's disk, before the root appended it | the same, plus the root's appended copy and a backup's copy of the same rows (the 2026-10-16 loss) |
| `Backup` (Layer B) | holder + every listed backup, committed not fsynced | a correlated power loss of all of them; otherwise re-shipped by the sealing backup |
| `S3` (Layer C) | the log slot | nothing |
| FUSE fast path on the holder or delegate | the local store, admitted against the wall clock | the lease or delegation having run out before the write landed (F9; since `b4fe2e6` re-checked inside the transaction) |
| Requester's shadow | the requester's replica | the holder dying before the ship |
| Pre-S3 stream (`StreamAhead`) | subscribers | a takeover stranding it (although the group had agreed on it) |

Everything in the right-hand column is machinery that exists because the
left-hand column is not commit. Note the Layer B row: the record is held
by every member of the configured replica set, the set is recorded in a
CAS-protected configuration, and a seal fences the primary before any
reconfiguration — that is a committed write in PacificA's and Vertical
Paxos's sense — yet the code classifies it as speculation until the S3
slot exists, and the subscribers that received it ahead of S3 keep
before-images to roll it back.

### 1.3 Deployment model the design must serve

From GOALS and the availability matrix (DESIGN §5.3):

- a lone node (no peer: no backup, no delegation, no message);
- two nodes, often a desktop and a laptop, the laptop away for days;
- 3–10 nodes, same LAN or across continents (hard constraint 4: no
  WAN round trip on every write; backups are chosen by measured RTT,
  5 ms budget);
- an always-on read-mostly follower; read-only members (IAM read-only);
- writes must continue through a bucket outage when enough members see
  each other (continuation epochs) and on an offline designee with no
  connectivity at all;
- `ack` policy per filesystem: `local` (free), `backup` (automatic when a
  peer is within budget), `s3` (opt-in);
- E2E mode: the only secret in the bucket is the wrapped master key;
  segments and chunks are encrypted; lease, registry, designation and
  heartbeat objects are plaintext coordination data; peers are
  semi-trusted (enrollment needs bucket write, so IAM is the trust root;
  Byzantine members are out of scope; `security-audit-2026-09.md`).

### 1.4 What the test machinery already verifies

Worth stating because the recommendation reuses it: the sans-IO
authority core and the real `Meta` run under a deterministic simulation
(`crates/authority/tests/sim`: crash, pause, partition, S3 cut, scripted
S3 errors, step stalls, writer-lock holds, delegate write holds; checks
`check_convergence`, `check_commits`, `check_exactly_once`,
`check_in_doubt_answers`, lock mutual-exclusion ghosts, strict
durability); Stateright models for backups, cto, delegation, flexible
epochs, hot directories, inbox, locks and positions
(`crates/model`); the harness with ~35 fault scenarios, the chaos soak,
and since `516f82e` a host-load campaign with a lag probe, injectors and
invariant checkers. Whatever is recommended must be expressible as a
change the sim and the models can be made to fail on first.

## 2. The week's bugs, classified

The classification asks, for each finding: would a design in which every
file-system change is ordered and acknowledged only through a
quorum-replicated P2P log have made the bug **impossible** (the state it
needs cannot exist), **irrelevant** (the bug is somewhere else), or
would it **still exist** (perhaps with a different cost)?

### 2.1 The acknowledged-write loss (found 2026-10-16)

Mechanism: node a, delegate of its own directory and backup of root b,
executes and acknowledges `truncate` T and manifest commit M, streams
them to b; b appends both under a's rids into its journal and is paused
before its backup stream returns them to a; a seals b, takes the root,
strands its own unappended delegate rows and replays them after b's
tail; the replay *folds* T into M and records no outcome for T; deposed b
rebuilds from the log and queues its unshipped journal for replay by rid
as `MutateOp::Records` (no base check, no token); M's copy dedups, T's
copy executes after M and truncates the file to zero on every replica.
Ten acknowledged, fsynced files read back empty on both nodes; no
conflict copy; clean bucket log. No node acted on a stale epoch or token.

Classification: **impossible under a commit-before-ack rule**. The loss
needs (i) an acknowledgement (to a's application) of rows that only a's
disk and b's unshipped journal held, (ii) two copies of the same
operation in two replay queues (a's stranded delegate rows; b's `Records`
copy of a's rows), and (iii) a replay that *re-executes* operations
against new state (the fold) instead of re-shipping committed records
verbatim. In a system where a's acknowledgement waits for the group
(a + b here, both reachable: one LAN round trip) and recovery after b's
seal re-ships b's committed tail verbatim, none of the three states
exists. The fix's own "Not covered" paragraph ("a replay refused by a
remote sequencer is still not journaled there; a `Records` copy of the
op on a third node could then still execute; needs a wire change") says
the class is not closed. Note that the arbiter played no part: the same
loss would occur with a Raft log if a delegate acknowledged before its
rows were committed by the group and recovery re-executed by rid.

### 2.2 F1: fencing token checked before the writer lock (`b4fe2e6`)

Mechanism: `check_tag` runs before `execute_inner`, which then waits up
to 20 s for the store's writer lock; the grant lapses, the next holder is
granted and writes; the stale write lands and is answered `OK`; for data,
the next holder's read-modify-write is rebased over it (lost update).

Classification: **structurally narrowed, not eliminated.** The fix moves
the check to the serialization point (inside the transaction, writer
lock held). In a log-ordered design the same thing is obtained for free
*if* lock grant, release and expiry are themselves log entries: a write
tagged with grant g is refused when the log already holds g's expiry,
and the check is the leader's serial apply — this is how ZooKeeper
sessions and Chubby sequencers work. What remains in any design is the
executor that did not mint the grant (a delegate under a root-minted
lock): it learns the expiry only when the record reaches it, so either a
time window (today's token `until`) or an acknowledgement of the expiry
record (an outwait) still bridges that gap. One mechanism instead of
"minter judges exactly, others judge by window", but a window
nonetheless.

### 2.3 F2 / F2b: false seals under host load

Mechanism: the backup's silence watch, the holder's beat task and the
arrival stamps all run on a starved tokio runtime; a 1.5 s lag reads as
holder death; a seal deposes a live holder (safe by ADR-12), which rolls
back, replays by rid, and the new tenure waits its quarantine. Under
sustained load this repeated every few seconds.

Classification: **still exists; the cost changes.** Every lease- or
heartbeat-based system has it: etcd leaders step down under fsync
latency, CockroachDB added disk-stall detection and in 2025 moved from
expiration-based to leader leases precisely because node liveness
heartbeats were lost under load (§3.2). A quorum log makes a false
deposition *cheap* (a new term; nothing committed moves; proposals in
flight fail and are retried by the client under the same id) instead of
*an occasion for every other recovery path* (rollback, replay, folds,
quarantines). That cost reduction is available without Raft: it is a
consequence of commit-before-ack, not of the arbiter.

### 2.4 F3: a `flush` hangs while a recalled grant is renewed forever

Classification: **irrelevant** (a local wait with no name and no bound,
plus a P2P endpoint wedge). The lock-pin cap and stage naming are the
fix in any design.

### 2.5 F5: the lease renewal is one S3 request series at half TTL

Mechanism: one black-holed TCP connection consumes the half TTL; the
lease lapses; a TTL takeover strands the holder's work.

Classification: **irrelevant to ordering, present in every design in a
different coat.** With P2P ordering the holder's authority would still
rest on a renewable grant from the configuration master (S3) or on
heartbeats to a quorum; a black-holed path to whichever of those is the
witness has the same effect. What a commit-before-ack rule changes is
that a TTL takeover strands nothing acknowledged.

### 2.6 F9: fast-path admission against the wall clock, journal after expiry

Classification: **impossible** under commit-before-ack: a row journaled
on a node whose authority has run out is not acknowledged until the group
(or S3) has it, and a group that has reconfigured away from this node
refuses it. The present fix (re-check inside the transaction) narrows it
to the residual "began before expiry, committed after", which is still
stranded and replayed. That residual exists only because the local
journal write *is* the acknowledgement under `Local`.

### 2.7 F4, F6, F7: quarantine length, close-wait `EAGAIN`, degraded strict reads

Classification: **still exist.** F4 is the price of time-bounded read
delegations and lock grants under a fast successor (any lease-based
cache invalidation has a horizon the successor must wait out). F6 is the
kernel's `RELEASE` ordering. F7 is a design choice (answer stale rather
than block) that a Raft ReadIndex would face identically.

### 2.8 Plan 41 L0: a fast-tenure root renewed grants without fresh S3 liveness (`9d00417`)

Mechanism: a root sealed by its backups, still reachable from lock
holders, kept renewing their grants until it noticed the deposition.

Classification: **same family as Raft's leader-lease problem; one
mechanism instead of two.** A Raft leader may only serve leases while it
has heard from a quorum within the election timeout (CheckQuorum /
leader lease). Today the root must correlate three time bases: the lease
object (S3), the seal (backup), the grant windows (its clock). Under a
"committed by the replica set" rule the root's right to extend a window
is "the group acknowledged my latest heartbeat/append within T", which
is one check against one source. Still a lease, still clock-bounded.

### 2.9 The lock-table bugs of 2026-10-07/08

`unlinked-exclusion-5681` (an outwaited generation's undelivered grants
revived, restamped, beside its successor: two exclusive holders),
`unlinked-exclusion-7455` (a recall answer after the generation ended
put lapsed grants back into the root's table), `stale-read-outwaited`
(a grant granted after an outwait carried a position older than the
outwaited holder's acknowledged writes), `lease-cap-fenced` (four nested
margins shrank a delegated grant to zero), the earlier
`unlinked-under-lock` adoption that broke exclusion.

Classification: **impossible if lock state is in the log.** Every one of
these is a copy of authority state (a grant record, a floor, a barrier)
travelling in a message (a renewal reply, a recall answer, a handoff), being
installed by a step that did not know the generation had ended, or
expiring lazily in one table and not in another. If "grant g to node n
on inode i until t", "g released", "g expired", "g moved with
delegation gen k" are log records appended by the owning sequencer and
applied in order by every replica, there is one table, derived from the
log, and a late message cannot revive anything: it is just a stale
proposal refused by the serial apply. This is independent of whether the
log is ordered by S3 slots or by a P2P quorum; what matters is that the
lock state is *in* the log rather than beside it.

### 2.10 Delegation generation bugs (`c465b07`, `96f4565`)

A delegate that took the root over and crashed re-sent a stream batch
forever because its stream index was a persisted counter while the
strand had turned rows 3..5 into root rows; an outwaited generation's
grants; a generation a takeover strand holed.

Classification: **largely impossible under commit-before-ack with
verbatim re-shipping.** The hole exists because the delegate's rows were
re-executed as root rows (new identity) instead of appended as the
generation's rows (same identity, same `(gen, idx)`). If a delegate's
transactions are committed by its group and re-shipped verbatim by
whoever holds them, there is no renumbering, no hole, no "end the
generation at the cut". Per-subtree consensus groups (option D) would
reintroduce a version of this at the group boundary.

### 2.11 Everything else since 2026-10-01

Of the 188 commits since 2026-10-01, the large majority are unrelated
to the arbiter and would exist in any design: the Linux FUSE client
races and workarounds (plan 42: stale page after write-through, directory
attributes marked valid before stored, pre-change replies cancelling
invalidations), the partial-`FORGET` reap (ADR-31), orphan and `O_PATH`
semantics (six fixes on 2026-10-13), `F_GETLK`, OFD/`flock` drop at
`RELEASE`, the close-inside-`execve` deadlock (a kernel window between
two `Cc: stable` commits), fjall/lsm-tree stalls (`a29250c`, `2a89303`),
io_uring transport hangs, mtime/ctime stamping, the staging rewrite
(finding 09), `copy_file_range`, READDIRPLUS, CSI/Kubernetes, CI. The
engine crate alone is 103k lines against the authority core's 71k; the
corruption classes in this part of the system (the plan 42 races are
real stale-data bugs) have nothing to do with where metadata is ordered.

### 2.12 Summary table

| Class | Examples | Under commit-before-ack on a replica group | Under a Raft rewrite | Under today's design + fixes |
|---|---|---|---|---|
| Acknowledged before agreement, rolled back and replayed | tloss, F9, plan 30 bug B, stranded replays, conflict copies in default modes | impossible where a group exists; remains for a lone/distant `Local` holder (and must, or the lone node pays S3 latency) | identical | present, patched case by case |
| Several copies of one op in several queues, re-executed | tloss, the "Not covered" gap, `strand-takeover-quiescence` | impossible (one identity, verbatim re-ship) | identical | present |
| Authority state outside the log, moved by message | lock-table bugs of 10-07, L0, barriers/floors | impossible if the state is logged | impossible if the state is logged (not automatic in Raft either) | present; floors/barriers added |
| Time-of-check vs serialization point | F1 | one serialization point per group; the non-minter window remains | same | fixed at the write |
| Failure-detector false positives | F2, F2b, overload-cascade 1/2 | still exist; a false deposition costs a term change, not a recovery | same; etcd/CockroachDB history | fixed by attentive-time measurement |
| Lease and clock bounds for locks, caches, delegations | F4, L0, lease-cap, lock lapses under disk stalls (plan 41) | still exist | still exist | present |
| Liveness path through one connection | F5 | still exists (to the witness/quorum) | still exists | to fix |
| Local hangs, endpoint wedges | F3 | irrelevant | irrelevant | under bisect |
| Kernel, store, transport | plan 42, FORGET, exec/flush, fjall, io_uring | irrelevant | irrelevant | fixed |

## 3. Survey: how comparable systems split ordering from bulk data

Method: public documentation, papers, Jepsen and Antithesis reports,
engineering blogs and mailing lists (no private sources). Numbers are
quoted from the cited source; where a source was silent that is said.

### 3.1 File systems with object storage for data

**JuiceFS.** Data: immutable 4 MiB blocks in object storage; files are
64 MiB chunks holding *slices* (one per contiguous write; later slice
wins, compaction rewrites). All naming, attributes, slice lists, refcounts,
sessions and lock records live in a transactional metadata engine
(Redis, TiKV, etcd, SQL). A write uploads blocks first and commits the
slice to metadata second; orphan blocks are the failure residue. The
engine's transactions are the total order ("rename and all other
metadata operations are atomic, guaranteed by Redis transaction"); Redis
Cluster is supported only by keeping one volume in one hash slot.
Client contract: close-to-open; `--writeback` acknowledges before upload
and "data still in the write cache before upload completes can be lost
permanently". Engine down = file system down: requests retry
`--io-retries` (10) then `EIO`. The Redis engine loses acknowledged
writes on failover (asynchronous replication, "you may lose the writes
from the last second"); JuiceFS rates Redis reliability "low", TiKV/etcd
"high", and TiKV "requires at least three hosts". Single node: embedded
SQLite/BadgerDB. Benchmarked rename latency (their units): Redis 736–860,
TiKV 1799, etcd 2941. Testing: pjdfstest and LTP; no Jepsen or
simulation published. Lesson: a separate always-on metadata service is
the price of correctness, and the cheap engines lose acknowledged
writes exactly where constellation's Layer A would.

**DeepSeek 3FS.** Stateless metadata services over FoundationDB
(serializable snapshot isolation; create/link/unlink/rename are
read-write transactions, the rename loop check walks ancestors inside
the transaction); chunks replicated with CRAQ chains (writes serialized
at the head, versioned pending/committed, the tail commits). Membership
is lease-based from a manager: `lease_length` 1 min, extend every 10 s,
"a service stops and exits if it cannot reach the manager for T/2
seconds" (self-fencing). File length is eventually consistent during
writes (clients report every 5 s; exact at close/fsync). Built for
180-node clusters; FDB single-process is the SPOF at small size. Lesson:
even with a transactional store under it, membership is still a lease
with a clock, and the data plane is a write-all chain (CRAQ: write-all,
read-any with a dirty check), the same shape as a holder + backups.

**S3-only metadata (ObjectiveFS, s3ql, cunoFS, Mountpoint, goofys).**
s3ql: single `mount.s3ql` process per filesystem ("do not attempt to
mount the same file system on different computers"), metadata in a local
SQLite uploaded every 6 h, "modifications made after the most recent
metadata backup may be lost". ObjectiveFS (closed): "handles all the
coordination among the clients through the object store", claims
multiple concurrent writers and atomic renames; release notes mention
lease performance, the troubleshooting guide names NTP offset (< 1 s) as
a cause of delayed cross-machine visibility — an undocumented
clock-dependent lease protocol over S3. cunoFS: one object per file,
POSIX metadata as hidden objects, "consistency equivalent to NFS", no
arbiter. Mountpoint for S3 / goofys: sequential writes, commit on close,
no links, no locks, "no coordination between writes to the same object".
Lesson: no published S3-only design offers multi-writer POSIX with a
stated consistency contract; the ones that work either forbid multiple
writers or reduce POSIX. Constellation's lease-plus-forwarding design is
already past this class; the question is only whether the *commit*
should wait for S3.

**CephFS.** The MDS keeps the namespace in memory and journals every
metadata event to RADOS (itself a Paxos-governed, replicated store)
before applying; data flows client → OSD. One authoritative rank per
subtree (export pins; the dynamic balancer is off by default — Ceph's
own Mantle finding that load should move to its dominant writer, cited
by ADR-23). Client capabilities are the lease-like grants; eviction
blocklists the client at the OSDs so a stale client cannot write data
(data-plane fencing). Notably the MDS answers some requests *before*
journaling and relies on client replay at failover (`up:clientreplay`:
"replaying any client requests which were replied to but not yet
durable") — a Layer A acknowledgement with requester replay, exactly
constellation's. Failure detection: beacon 4 s, `mds_beacon_grace` 15 s,
`mds_reconnect_timeout` 45 s; monitors require clock drift < 50 ms
(`mon_clock_drift_allowed`). Measured recovery under load on the dev
list: 850–1,557 s with hundreds of sessions and a > 16 GB cache ("rejoin
is always the longest step"); the 2020 Edinburgh "anatomy of a CephFS
disaster" needed a journal reset and ~4 days of scanning; INWX lost the
whole FS to a monitor crash during an MDS downscale. A 1–2 node Ceph is
not HA (3 monitors). Lesson: a consensus store under the journal does not
make failover fast or recovery simple; the expensive parts are the
in-memory state rebuild and the client-replay reconciliation, which is
what constellation's takeover gate is.

**Google Colossus.** Curators (control plane) keep file metadata in
Bigtable, which bootstraps through Chubby (Paxos); data flows directly
between clients and D servers; custodians do background durability.
"Storing file metadata in Bigtable allowed Colossus to scale up by over
100x over the largest GFS clusters." Cross-row ordering, failure
handling and small-cluster behaviour are not published. Lesson: metadata
off the data path, in a sharded transactional store with a Paxos lock
service at the root — the same two-level shape (slow configuration
master, fast data plane).

### 3.2 Logs, quorums and leases

**Amazon Aurora.** "The log is the database": the single writer ships
redo records to storage; 10 GB protection groups with V=6, write quorum
4, read quorum 3 across three AZs; storage nodes "do not have a vote in
determining whether to accept a write, they must do so"; the instance
advances its commit point locally at 4/6 ("no consensus is required to
advance SCL, PGCL or VCL"); read quorums are used only at crash recovery
to find the volume-complete LSN, then the log is truncated above it.
Fencing is by **epoch, not lease**: "storage nodes will not accept
requests at stale volume epochs ... some systems use leases, but leases
introduce latency when one needs to wait for expiry. Aurora, rather than
waiting for a lease to expire, just changes the locks on the door."
Membership changes are reversible quorum-set epochs (a write during the
transition must satisfy both the old and the new 4/6). Continuous
backup to S3; S3 never on the latency path. Correlated failures are
why 2/3 was rejected ("not all faults are independent"). MIT's 6.824
critique: the single-writer model moves the hard problem to writer
failover and fencing. Lesson: a single sequencer plus a write-all-ish
quorum of dumb acceptors plus an epoch stamped on every request *is* a
sound design when the configuration/epoch lives in a separate authority
— constellation's lease epoch, slot CAS and seal are the same three
pieces. **Amazon EFS**: internals unpublished; NFS close-to-open,
advisory locks only, cross-AZ durability on close/sync.

**Neon.** Compute streams WAL to safekeepers (Paxos acceptors; the
compute is the proposer, pageservers are learners); "a WAL record is
durable once more than half of the WAL safekeepers have flushed it to
disk"; the documented example is 3 safekeepers, quorum 2; "each
safekeeper can serve exactly one proposer", a mismatched `nodeId` breaks
the connection and the stale proposer exits (fencing by term, not by
waiting). Commit latency is network-bound, not fsync-bound; S3 holds
immutable layers and is "the last line of defense"; a commit never waits
for pages or uploads. Loss of quorum stalls commits; the protocol has a
TLA+ spec. Lesson: the clean disaggregated design is proposer +
acceptor quorum + object-store archive, chosen Paxos-style precisely so
that an old primary "remains blocked". Needs three machines for one
fault.

**WarpStream, Bufstream, Kafka tiered storage.** WarpStream buffers
250 ms / 8 MiB, writes one object to S3, then commits its metadata to a
hosted replicated state machine ("the source of truth for ordering is
held in the Metadata Store"), and only then acknowledges; p99 produce
400–600 ms on standard S3, ~170 ms on S3 Express, 33 ms median only for
"Lightning Topics" with no ordering guarantees. Control-plane loss needs
a special degraded "Ripcord mode". Antithesis deterministic simulation
of the whole SaaS found a data-loss bug "in speculative-retry logic" that
occurred about once per wall-clock hour of testing. Bufstream: leaderless
brokers, metadata (offsets) in etcd/Postgres/Spanner; p99 500–1000 ms is
typical of the class; Jepsen 0.1.0 found lost acknowledged writes and
*metastable* stuck clients caused by "etcd lease expiry after brief
pauses or partitions" (agents not told their keys were deleted). Kafka
KIP-405 keeps the leader/ISR log as the arbiter and tiers only closed
segments; remote storage down = "Kafka should still be able to operate
normally". Lessons: (i) acknowledging after an object-store PUT costs
hundreds of milliseconds at p99 — Option A's cost confirmed by an
industry that accepted it only for streaming; (ii) a Raft-backed lease
(etcd) under brief pauses produced exactly the F2/L0 class of bug in a
production system; (iii) speculative retry is where DST found data loss.

**FoundationDB.** Coordinators run Active Disk Paxos for membership and
configuration; the transaction system (proxies, resolvers, transaction
logs) is reconfigured through them on any failure (a "recovery", sub-
second to seconds); the control plane and the data plane are separate.
Testing: "before building the database itself, we built a deterministic
database simulation framework"; > 250 million simulations per year.
Single-process FDB is the SPOF of 3FS at small scale. Lesson: the
Vertical-Paxos split (slow Paxos for configuration, fast specialised
protocol for data) is the production norm, and deterministic simulation
of the real code is how its correctness was obtained — constellation's
sim is the same investment and must not be discarded.

**CockroachDB and etcd (Raft).** CockroachDB (2024–25) replaced
expiration-based range leases, which depended on one node-liveness range
("if that range's leaseholder crashed, no normal lease could be renewed
until it expired"), with **leader leases** on a peer-to-peer Store
Liveness fabric: a node grants support for an epoch with an expiration,
a replica must be "fortified by a quorum of stores before it can become
leader"; claimed partition heal < 20 s, liveness failures < 1 s. Disk
stalls remain their own class: a WAL fsync past 20 s makes the process
self-terminate so leases move; an asymmetric stall can leave "a stale
leaseholder". etcd: Red Hat/OKD guidance keeps WAL fsync p99 under 10 ms
because slow fsync "can cause etcd to miss heartbeats ... and ultimately
experience request timeouts and temporary leader loss". Jepsen on etcd
3.4.3: strict serializable KV under pauses, crashes, skew, partitions
and membership changes, but "etcd locks are fundamentally unsafe" for
mutual exclusion in asynchronous networks (protect the resource with a
monotonic token it checks itself); the 2024 jetcd analysis found a
client that "incorrectly retries non-idempotent requests which may have
actually succeeded" — lost updates, i.e. the plan 30 bug A that request
ids fixed. Lessons: the Raft ecosystem's current bug frontier is leases,
clocks, disk stalls and client retry — the same families as F1, F2, L0,
plan 41 and ADR-18; a rewrite inherits them.

**Delos (virtual consensus) and Vertical Paxos / PacificA.** Delos
splits a shared log into a *VirtualLog* (reconfiguration) and
*Loglets* (ordering within one configuration). A loglet "does not even
have to implement fault-tolerant consensus" as long as it provides a
fault-tolerant, sticky `seal` and `checkTail`; the NativeLoglet is a
central sequencer forwarding to log servers with majority
acknowledgement and becomes unavailable (not unsafe) when the sequencer
fails; the MetaStore is "a single versioned register with a conditional
write" (new value plus expected version); reconfiguration is seal the
old loglet → read its tail → conditional-write the new configuration,
and any client can finish another's reconfiguration. Vertical Paxos
(Lamport, Malkhi, Zhou 2009) is the theory: an auxiliary configuration
master lets a replica group of f+1 tolerate f failures (NetChain's
summary) because fault tolerance is offloaded to reconfiguration; the
primary-backup protocols are its special case; it needs the master to be
available (synchrony for reconfiguration). PacificA: the configuration
manager is a separate Paxos cluster; the primary assigns sequence
numbers, prepares to *all* replicas, each logs and acknowledges
(write-all); the "primary invariant" (a replica serves as primary only
while the configuration names it, and leases are shorter than the
manager's grace) is the seal. Lesson: constellation's lease object is a
Delos MetaStore (a versioned register with a conditional write), its
holder+backups is a NativeLoglet with write-all instead of majority, its
backup seal is the loglet seal, and its S3 log is the archive. The
design is sound in this frame; what Delos insists on and constellation
does not yet do is that *commit is the loglet's acknowledgement*, with
the archive strictly downstream.

**Disk Paxos and S3 conditional writes as acceptors.** S3 shipped
`If-None-Match: *` on 2024-08-20 and `If-Match: <etag>` on 2024-11-25.
SlateDB fences writers by creating the next manifest/WAL object with
`If-None-Match` (object names as fencing points); turbopuffer batches
updates behind a stateless broker that acknowledges only after a
group-committed conditional write; Delta Lake / Iceberg commit tables
the same way. Published p50/p99 figures for conditional PUTs were not
found; ADR-28's own measurement (28–190 ms by distance) stands.
Commentary ("S3 does not replace consensus", 2026) draws the boundary:
single-key CAS is a fine configuration register and fence; it is not a
low-latency ordering service for every operation. Lesson: S3 is the
right MetaStore and the wrong loglet — which is what the current design
already says and Option A would violate.

**Hermes and CRAQ.** Hermes (ASPLOS'20): invalidation-based replication
with logical timestamps; a write sends INV to all, waits for all ACKs,
then VAL; linearizable local reads at every replica; "reliable
membership" (a lease-based membership service) is the fault-tolerance
foundation, and a blocked replica can replay a write because the value
travels with the invalidation. CRAQ (USENIX'09): chain replication with
reads apportioned to every node, dirty versions checked at the tail;
Meta's Delta uses 4-link chains. Both are write-all/read-any data planes
under an external membership authority — the B2 shape; Hermes is the
closest published analogue of constellation's "recall every read
delegation before acknowledging a mutation".

**CRDT and merge-based file systems.** ElmerFS (HotStorage'21) lists
what breaks without coordination: unique identifiers, name collisions,
cycles ("moving A into B concurrently with B into A can result in a
cycle"), divergent renames (a rename is a move of a link); its answer is
deterministic merge rules that surface the conflict to the user. The
2012 "File system on CRDT" paper offers either restricted names or
on-the-fly renaming of conflicting files (which "can look odd to users").
Syncthing produces conflict copies. Lesson: POSIX `rename`/`link`
semantics with cycle freedom and `O_EXCL` need a total order per
namespace region; a CRDT namespace is a different product (GOALS:
"conflicts are prevented, not merged"). ADR-2's rejection stands.

**Leases under pauses (Chubby, ZooKeeper, Kleppmann).** The canonical
failure: a holder pauses past its lease, the lock is granted elsewhere,
the holder resumes and writes. The remedy everywhere is a monotonic
fencing token checked by the resource (Hazelcast `FencedLock`, etcd's
lock README pointing at Kleppmann, ZooKeeper's sequential ephemeral
nodes as tokens; session expiry by heartbeat rather than TTL). Jepsen's
etcd verdict ("locks ... fundamentally unsafe" without resource-side
checks) is the same statement. Lesson: constellation's fencing token at
the write (`b4fe2e6`) is the industry's answer and stays in every
option; no quorum log removes the need for it.

**Deterministic simulation and Jepsen.** FoundationDB (simulation first),
TigerBeetle (VOPR), WarpStream (Antithesis over the whole SaaS), Jepsen
on etcd, Bufstream, Redis-Raft (21 issues, "total data loss on any
failover") and jetcd; Neon's TLA+ spec. The findings cluster in exactly
two places: lease expiry under pauses, and client/speculative retry of
non-idempotent operations. Constellation's sim and models already target
both; the recommendation keeps and extends them.

### 3.3 What the survey says about the question

1. Every multi-writer POSIX file system in production uses a separate
   ordering authority; none orders through S3 per operation; the ones
   that acknowledge only after an object-store PUT are streaming systems
   that accept 400–1000 ms p99.
2. The production shape for "object store for data" is a slow
   configuration master plus a fast, simple data-plane replication
   protocol (Aurora, Neon, FDB, Delos, PacificA). S3 conditional writes
   are a good configuration master; constellation uses them as one.
3. Lease, clock and retry bugs are where the consensus systems still
   bleed (CockroachDB's lease redesign, etcd under fsync stalls,
   Bufstream's metastable lease expiry, jetcd's retries). A Raft rewrite
   buys none of that back.
4. The systems that lost acknowledged data in testing did so through
   speculation: JuiceFS's Redis engine, WarpStream's speculative retry,
   CephFS's reply-before-journal with client replay, Bufstream 0.1.0.
   That is the class to remove, and it is a commit-rule property.

## 4. Options

Common vocabulary for the options, borrowed from Delos (virtual
consensus, OSDI'20) because it fits the existing design exactly:

- the **MetaStore** holds the configuration: the current epoch, the
  replica set (loglet) that may order writes, and the policy. Today this
  is `leases/p0.json` plus `nodes/` and `designations/`, all in S3.
- a **loglet** is the replica set that orders and stores entries for one
  epoch: today the holder plus its listed backups (write-all, read-any),
  or the holder alone (`Local`), or the S3 slot sequence (`ack=s3`).
- **seal** fences a loglet before the MetaStore installs the next one.
  Today the backup's seal, the log-slot CAS and the lease CAS together
  play that role.
- the **archive** is where sealed loglets' entries end up for readers
  and for bootstrapping: today `log/p0/` and the commit chain.

Latency figures used below: S3 conditional PUT 28–190 ms by distance
(ADR-28, measured); LAN RTT 0.2–1 ms; the backup RTT budget 5 ms;
cross-region RTT 60–200 ms; a fjall commit in buffer mode tens of µs, an
`fsync` 0.5–10 ms on SSD, seconds on a stalled disk (plan 41).

### Option A: keep S3 as arbiter; acknowledge only what S3 ordered

This is `fs create --ack-policy s3` (Layer C) made the only policy, plus
removing the requester's shadow, the delegate's own acknowledgement and
the FUSE fast path's local acknowledgement.

- **Guarantees.** Every acknowledged mutation is in a CAS-created log
  slot: a deposed holder can acknowledge nothing a successor does not
  see; no speculation, no rollback, no replay-by-rid of acknowledged
  ops, no conflict copies except from offline designations (which are
  non-stealable, so none). The fencing token is still needed for
  lock-protected writes (lock windows are time-based), but F1's lost
  update cannot be *acknowledged* before it is ordered.
- **Latency.** One S3 round trip per group commit on every metadata
  operation's acknowledgement: `mkdir`, `create`, `rename`, `unlink`,
  `setattr`, every `close()` under `through`. Linux serializes creates in
  a directory, so a `tar x`, `git checkout` or `npm install` becomes
  (number of entries) × (28–190 ms): a 10,000-file checkout takes 5–30
  minutes instead of seconds. ADR-28 rejected optimistic commits at S3
  latency for this reason; plan 30 M13 measured the inbox (one S3 round
  trip per op) at 41–57 ops/s. This is the same wall. Delegates do not
  help: their acknowledgement would also wait for the root's ship.
- **Availability.** Unchanged from today's `ack=s3`: nothing is
  acknowledged during a bucket outage; continuation epochs cannot carry
  an `S3` lease (any peer may take it over before expiry). The
  offline-first goal is lost except through designations.
- **Durability.** Best of all options (nothing acknowledged is ever
  lost short of losing the bucket); correlated node failures do not
  matter.
- **Operational model.** Simplest: no backups, no seals, no replica set;
  a node offline for days is just a follower that tails. One and two
  nodes work identically to ten.
- **E2E.** Unchanged.
- **Migration cost.** Small in code (it is a mode that exists) but it
  deletes the product: "local-FS-fast when you own the data" and
  "LAN-fast when peers are near" (GOALS) are gone.
- **Risk.** Low correctness risk, fatal performance risk. Not a
  candidate as the default; it stays as the mode for workloads that
  want it.

### Option B: a P2P quorum log for all metadata ordering; S3 for chunks, checkpoints and configuration

B comes in two shapes that differ in who the voters are.

**B1 — majority-quorum Raft/Multi-Paxos among the write-eligible
nodes, S3 as witness/configuration master only.** The log is replicated
to a majority of a fixed voter set; the leader is elected by the group;
S3 holds membership, the current configuration (voter set) and
checkpoints; the leader ships committed segments to S3 as an archive.

- **Guarantees.** Linearizable metadata, term-fenced leadership,
  commit = majority acknowledgement with fsync (the Raft contract) or
  without (then the same correlated-loss caveat as Layer B today). No
  speculative acknowledgement by construction if the FUSE fast path
  waits for commit.
- **Latency.** One round trip to the slowest member of the fastest
  majority per commit, pipelined. LAN: sub-millisecond, like Layer B.
  WAN: a 3-voter group with members on two continents pays 60–200 ms on
  every write — hard constraint 4 violated outright. Flexible quorums
  (Howard et al.) or leader-local "leases" do not escape it: the commit
  quorum must intersect every election quorum, so a leader cannot commit
  on a LAN-only quorum unless elections need the WAN majority *and* the
  voter set is arranged so that a LAN majority exists, which is a
  per-deployment arrangement, not an automatic one. Today's RTT-budget
  backup selection is exactly the automatic version of that arrangement.
- **Availability under partition.** 1 voter: no fault tolerance, but
  also no quorum to lose; it is today's lone node. 2 voters: no majority
  survives one failure; a witness is needed, and the only witness that
  does not need a third machine is S3 — so the 2-node case degenerates to
  "S3 decides who leads", today's lease. 3 voters: one failure tolerated
  with no reconfiguration pause (an advantage over write-all Layer B,
  which pauses for one CAS to drop the dead backup); a laptop away for
  days is fine *as a follower* but when the two others also lose S3 they
  still have a majority and keep writing — which is the ADR-2/ADR-22
  danger in reverse: if the laptop then reaches S3 alone, a Raft design
  needs the configuration master to refuse it; S3 can do that (it holds
  the configuration), so the "laptop problem" is solvable in B1 by the
  same promise/witness mechanism as today, not for free. 5 voters: two
  failures; rarely the deployment size here (3–10 nodes, many read-only
  or distant).
- **Writing through an S3 outage.** Better than today where a majority
  is on one LAN (no promises needed; the majority is the authority);
  worse where it is not (a 2-node LAN of a 3-node cluster with the third
  across the ocean has no majority without the WAN link).
- **Durability.** Majority on disk; with fsync per commit the quorum
  survives any minority power loss, better than Layer B's buffer mode;
  without fsync the same single-failure contract as today.
- **Operational model.** Membership changes are joint consensus or
  single-server changes through the log (etcd/Raft §6); a node offline
  for days must be removed from the voter set or it degrades the quorum's
  latency and, if it was one of three, its absence plus one more failure
  stops writes. Laptops as voters are a known anti-pattern; they would
  have to be non-voting learners, which brings back "a learner that
  wants to write while away" = offline designation = a second authority
  root beside the log.
- **E2E.** Peers already hold the keys; a P2P log leaks nothing new to
  S3. Peers are, however, now trusted for *ordering*, not only for
  speed; the threat model (semi-trusted peers, IAM as root) tolerates
  that, as it already trusts a backup's seal.
- **Migration cost.** A rewrite of the authority core (71k lines), the
  models (18k), the sim (6k + 12 modules) and much of the engine's
  driver (`authority_driver.rs`, 6.7k). The chunk store, `Meta`'s
  namespace encoding, the E2E layer, the FUSE frontend and the harness
  survive. Realistically months before parity with today's tested
  failure matrix; every lease-based component (locks, read delegations,
  kernel caches) is rebuilt on a new liveness source.
- **Risk.** High. The protocol family's own bug history (§3.2: etcd and
  CockroachDB Jepsen findings, leader-lease and clock bugs, election
  storms under disk stalls, membership-change bugs that took years to
  settle) is the same family as F2, L0 and plan 41; the rewrite trades
  known bugs for unknown ones. And the properties actually
  wanted (no speculative state, one identity per op, logged lock state)
  are properties of the *commit rule*, which B1 does not enforce by
  itself: a Raft-based file system that acknowledged writes at the
  leader before commit, or kept lock tables beside the log, would have
  every bug in §2.

**B2 — Vertical-Paxos / PacificA: S3 is the configuration master; the
data path is a primary-backup group chosen per epoch; write-all,
read-any; reconfiguration by CAS after a seal.** This is today's Layer B
with "committed" redefined as "every member of the configured group
holds it", the S3 slot demoted to the archive, and the fast path and
delegates forbidden to acknowledge outside a group.

- **Guarantees.** Exactly B1's for the group: nothing acknowledged
  without agreement; term (epoch) fencing by the seal + configuration
  CAS (Aurora's "change the locks on the door" rather than waiting out a
  lease); one identity per operation; verbatim re-ship on takeover (the
  successor is a group member and has everything: write-all gives
  read-any). The archive (S3 slot CAS) is a second fence that costs
  nothing and lets P2P-less nodes and fresh nodes bootstrap. Vertical
  Paxos's result applies: f+1 replicas tolerate f failures because the
  configuration master carries the fault tolerance, which is why two
  LAN nodes get a real failover here and not under B1.
- **Latency.** LAN: one round trip to the backup(s) per acknowledgement,
  pipelined — today's Layer B cost. WAN: the group is chosen by RTT
  budget; a cluster with no peer in budget has a group of one, which is
  Layer A (speculative by nature, §4 "the lone voter") or `ack=s3`.
  Delegates: a delegate's acknowledgement waits for its own group
  (delegate + a backup within *its* budget, which exists today) or for
  the root's group if it has none; a delegate without any peer in
  budget and a far root is the WAN case again and gets Layer A.
- **Availability.** Group of one: today's lone node. Two nodes on a
  LAN: a group of two with write-all; one failure → the survivor seals
  and takes over with a CAS (1.5 s / 2.3–4.7 s measured) — *better* than
  a 2-voter Raft, which cannot fail over without a witness; the second
  failure falls back to the TTL path. Three on a LAN: a group of two or
  three (write-all to two backups is a knob, `CONSTELLATION_BACKUPS`);
  the dead member is reconfigured out by one CAS; while the CAS is in
  flight writes park (today's `backup_ack_timeout` 1 s + the CAS), which
  is the price of write-all against Raft's majority — bounded and
  measured. Through an S3 outage: the group keeps committing (today's
  continuation epoch, with promises for the laptop problem; unchanged).
- **Durability.** As Layer B; a knob for fsync-per-append where the
  operator wants the Raft contract.
- **Operational model.** Unchanged: membership is the bucket, a node
  away for days is a follower, the group is re-chosen per tenure by
  RTT; nothing to configure.
- **E2E.** Unchanged.
- **Migration cost.** Medium and incremental (§6): the change is in the
  acknowledgement and recovery rules of the existing core, not in its
  arbiter; the sim, the models and the fencing tokens stay and are
  extended; large *deletions* (folds, replay-by-rid re-execution of
  committed rows, `TailFollows`, "nobody observes a tentative effect"
  waits under `Backup`, lock handbacks/restamps) rather than additions.
- **Risk.** Medium: the changes touch the recovery paths that have been
  the bug source, which is the point; each step has a fail-before test
  in the sim and the model.

### Option C: P2P only, S3 purely storage, no witness

Membership, leadership and ordering all among peers; S3 sees chunks and
checkpoints.

- **What breaks.** A lone node is fine (a group of one). Two nodes cannot
  agree on leadership through a partition: either both can lead (split
  brain) or neither (no writes) — the classic two-node problem, which is
  why every two-node product (Windows failover clustering, vSAN stretched
  clusters, Patroni) requires a witness. Three nodes with one laptop:
  the laptop away for days is fine as a follower, but a desktop/laptop
  pair with the server down has no majority. Offline designation needs a
  CAS somewhere to be exactly-one; without S3 it becomes a group
  decision that the designee must be present for, which defeats the
  purpose when the designee leaves in a hurry. Membership (who are the
  voters) must be bootstrapped from something; "the bucket is the
  membership" (GOALS, ADR-7) is lost. Bootstrapping a fresh node needs
  a checkpoint and the log tail from peers, so a fresh node with only
  S3 access (a read-only follower, the web fleet scenario) cannot join
  while peers are unreachable.
- **Verdict.** Not viable for the stated deployment model. Every
  production system in §3 that puts bulk data in object storage keeps a
  separate ordering/configuration authority (a metadata service, a
  Paxos group, or S3 conditional writes themselves); none runs without a
  witness at two nodes.

### Option D: hybrids

**D1 — per-subtree consensus groups (delegation as Raft groups).** Each
delegated subtree is its own group with its own leader; cross-subtree
operations need either two-phase commit or a recall into the parent
group. Today's design already is this with groups of size one or two
(delegate + its backup) and recall instead of 2PC (ADR-23). The group
boundary is where the `deps`, `Held`, generation-end-at-the-cut and
stream-hole machinery lives (§2.10); more groups mean more boundaries.
Worth keeping as the scale-out mechanism, not as the fix.

**D2 — leader leases for reads (Raft §6.4 / quorum leases).** Today's
read delegations and `cto=strict` ReadIndex are this. Unaffected by the
arbiter choice; keep.

**D3 — flexible quorums (Howard, Malkhi, Spiegelman 2016).** Write
quorum small and local, election quorum large: Q_w ∩ Q_e ≠ ∅ suffices.
With S3 as the election "quorum" (the lease CAS) and the RTT-chosen
group as the write quorum, today's design is flexible Paxos in that
exact sense; the seal is what makes the write quorum intersect the
election: a successor must obtain the seal of a current write-quorum
member (a backup) or outwait the lease. Naming it so clarifies why
write-all is required: with write-all, read-any holds, so the sealing
backup has every committed entry.

**D4 — S3 as a Disk-Paxos acceptor on the data path.** Using the slot
CAS as the commit for every entry is Option A; using S3 as one of three
acceptors with two peers makes every commit wait for S3 anyway (a
majority of {S3, a, b} without S3 is {a, b}, which is B2). No gain.

### 4.1 Side-by-side

| | A (ack after S3) | B1 (majority Raft, S3 witness) | B2 (Vertical Paxos: S3 master, RTT group, commit-before-ack) | C (P2P only) | Today |
|---|---|---|---|---|---|
| Ack latency, LAN | 28–190 ms per op | ~1 ms | ~1 ms | ~1 ms | ~0 (local) / ~1 ms (backup) |
| Ack latency, voters on two continents | 28–190 ms | 60–200 ms | ~0 (group of one, Layer A) or `ack=s3` by choice | 60–200 ms | ~0 (Layer A) |
| Speculative acknowledgement | none | none (if the fast path waits) | none where a group exists; Layer A only for a group of one | none | five kinds |
| 1 node | works | works | works | works | works |
| 2 nodes, one dies | TTL path | no failover without a witness (S3) | seal + CAS, ≈1.5–5 s | split brain or stop | seal + CAS |
| 3 nodes, one dies | TTL path | majority continues at once | one CAS pause (~1–2 s), then continues | majority continues | one CAS pause |
| S3 outage, members connected | no writes | writes if a majority is connected | writes (epoch, promises) | writes if a majority is connected | writes (epoch, promises) |
| Laptop offline for days, writing | designation only | designation only (learner) | designation | impossible safely | designation |
| Durability of an ack | bucket | majority (fsync optional) | group (fsync optional) | majority | holder disk / group / bucket by policy |
| Lock and delegation state | beside the log unless moved in | beside the log unless moved in | moved into the log (§6 step 3) | — | beside the log |
| Code change | mode flag; product loss | rewrite of authority, model, sim, driver | rules of the existing core; deletions | rewrite | — |
| New failure modes | none | leader leases, elections under load, membership changes | none new (same mechanisms, fewer states) | two-node problem | — |

## 5. Recommendation

**Do not move the arbiter. Change the commit rule.** Keep S3 as the
configuration master and fence (lease epoch, replica set, membership,
designations, promises, the slot CAS as the archive's fence) and keep
the RTT-chosen P2P replica group as the data path — this is Option B2,
and it is what the system already is in shape. Then enforce, everywhere a
group exists, the one rule a quorum log would have given: **nothing is
acknowledged that the configured group has not committed, and nothing
committed is ever re-executed — it is re-shipped.** Where no group exists
(a lone node; a holder whose peers are all beyond the RTT budget),
acknowledge locally as today but treat that as the offline-first contract
it is, with exactly one kind of speculation (the holder's own unshipped
journal), and offer `ack=s3` for those who want none.

Why this and not B1:

- The bug classes in §2 that a quorum log would remove are removed by
  the commit rule, not by the voting rule. B1 without the commit rule
  has every one of them; B2 with it has none of them.
- B1 breaks hard constraint 4 (WAN round trip on every write) for any
  geographically spread voter set and has no two-node story without S3
  as witness — at which point it is B2 with a majority instead of
  write-all, and write-all/read-any is the simpler takeover (the
  successor already has everything; no log repair, no "catch up the new
  leader" phase).
- B1 discards the deterministic simulation, the Stateright models and
  three weeks of fencing and lock work that are correct, for a protocol
  whose own implementations still produce F2/L0-class bugs (§3.2).
- The remaining hard problems (§7) are identical under B1 and B2; B1
  adds leader election and membership change to the list.

What to keep unchanged: the chunk store and content addressing; E2E
(the archive stays encrypted; coordination objects stay plaintext);
request ids and `Completed`/`Refused` records (RIFL: exactly-once is
needed by every client of every log, Raft included); the fencing-token
check at the write (`b4fe2e6`) and the owner fence; the takeover gate
and the epoch marker; the inbox for P2P-less requesters; continuation
epochs and promises; offline designations; the sans-IO core, the sim and
the models (extended, not replaced); the host-load campaign tooling.

What the rule retires (the deletions are the benefit):

- the fold of a truncate into a commit and every other re-execution of a
  committed row by rid (`Meta::fold_replay`, `MutateOp::Records` copies of
  committed rows, `truncate_took_effect`);
- a delegate's acknowledgement ahead of its group; the root's
  re-journaling of delegate rows under new identities; generation
  holes and "end at the cut";
- speculation kinds under `Backup`: the pre-S3 stream as speculation,
  the backup tail as a separate structure (`TailFollows`), the "nobody
  observes a tentative effect" waits, the requester's shadow for a
  group-committed op (the reply *is* the commit);
- lock handbacks riding renewal replies, restamping, `pending_returns`,
  mirrors as an availability aid, the "late recall answer" rules;
- conflict copies in every default mode except the lone/distant Layer A
  holder's branch (GOALS says there are none in the default modes; the
  rule makes that true for every node that has a group).

## 6. A phased migration that is correct at every step

Each step is a rule change in the existing core with a sim/model test
that fails before it; none changes a wire or on-disk format in a way
that needs compatibility (the project rule: formats change in place, no
version bumps).

**Step 0 — Name the commit point and check it.** Add to the sim an
oracle `acked_implies_committed`: every `OK` answered to a client names
a transaction that, at the answer, was held by every member of the
configuration current at that time (or by S3). Today it fails under
`Local` and for delegates; record the baseline. Add `one_identity_per_op`:
no rid is ever queued for execution on two nodes at once as two
different op kinds. Add the Delos vocabulary to `DESIGN.md` §4 so the
invariants have names: MetaStore (S3), loglet (the tenure's group),
seal, archive.

**Step 1 — Commit = group acknowledgement under `Backup`.** A
backup-acknowledged transaction is committed: subscribers install the
pre-S3 stream as committed records (no before-images, no rollback on a
takeover — the sealing backup re-ships it verbatim, and the only way it
can be absent from the archive is a correlated loss of the whole group,
already outside the contract). The requester's shadow retires at the
reply, not at the segment. The S3 slot CAS stays as the archive fence; a
deposed holder's late segment is still refused there. Fail-before: a sim
config where a takeover strands a backup-acknowledged `StreamAhead`
today (`strict_durability` counts rollbacks of acknowledged ops). Risk:
low; this is making the code believe what the design already argues in
`durability-and-failover.md` "Why it is safe".

**Step 2 — No acknowledgement outside a group where a group exists.**
(a) A delegate with a backup acknowledges after its backup holds the
row; a delegate without a backup forwards its acknowledgement to the
root's group (one extra P2P hop, LAN) or, if the root is beyond the
budget and no backup is in budget, is placed under Layer A explicitly
(status shows it; placement may prefer not to delegate there). (b) The
FUSE fast path on a `Backup` holder waits for the backup's
acknowledgement (it already does for the client; make the local store
write not the acknowledgement). (c) A stranded *unacknowledged* op is
failed to its client (`EIO`, or retried under the same rid by the
client machine) instead of being replayed as a conflict copy. Fail-before:
the tloss test shape (`a_folded_truncates_rid_is_settled...`) with the
fold removed must still pass because T is never acknowledged without
b; the "Not covered" third-node `Records` copy cannot exist.

**Step 3 — Authority state into the log.** Lock grant, release, expiry
(decided by the owning sequencer and journaled), handoff with a
delegation, floors and barriers as positions in the records; the
delegation table already is records (`Delegate`/`Recall`): add the
stream cut. Every replica derives its lock table from the log; the
owner's in-memory table is a cache. The fencing token keeps its window
only for an executor that has not yet applied the expiry record (the
non-minter case); the minter's judgement is the serial apply.
Fail-before: the `unlinked-exclusion-5681`/`7455` and
`stale-read-outwaited` seeds, and a model knob `lock_state_in_log=false`
that reproduces the revived-grant counterexample. Cost: a journal row per
lock op, group-committed with the data (cached grants make the uncontended
re-lock free, as today).

**Step 4 — Verbatim re-ship, never re-execute, for committed rows.**
Takeover gate and deposition recovery re-ship committed rows in their
original order under their original identities (`(epoch, jseq)` or
`(gen, idx)`), and re-execute by rid only ops that were *never*
committed anywhere (Layer A's own branch). Delete the fold and
`Records`-copy paths. Fail-before: the loss trace of 2026-10-16 as a
sim scenario.

**Step 5 — Layer A isolated and documented.** The only speculation left
is a `Local` holder's (lone or distant) own unshipped journal, and its
only consumers are its own clients. Document the contract in GOALS
terms: "a writer with no peer in budget is offline-first: its
acknowledgements are its own, and a deposition while it is alive and
partitioned brings its branch back as conflict copies". Offer `ack=s3`
and offline designation as the two ways to have none. Consider a longer
TTL for a `Local` tenure with no `wanted_by`, since nothing fast depends
on it.

**Step 6 (optional) — write-all to two backups on a LAN of three.** One
knob (`CONSTELLATION_BACKUPS=2`) and the existing reconfiguration CAS
give a group that survives two failures; measure the acknowledgement
cost (two LAN RTTs in parallel). Not required for correctness.

Each step reduces state; none adds a protocol. Steps 1–2 remove the
classes in §2.1, §2.6, §2.10; step 3 removes §2.9 and most of §2.8;
step 4 closes the "Not covered" gap of the 2026-10-16 fix for good.

## 7. The hard problems that remain under any option

- **Leases need time bounds.** Cluster locks, read delegations, kernel
  entry/attribute caches and the right of a sequencer to answer strict
  reads are all "nobody else acts until t"; a quorum log orders their
  *grant* and *expiry* but cannot make a paused holder stop at t without
  a clock. The F1 pattern (judge at the serialization point, on the
  clock there) and the outwait stay. Chubby, ZooKeeper, CockroachDB and
  etcd all carry this (§3.2, §3.7).
- **A leader or holder under a pause.** A `SIGSTOP`ped holder resuming
  past its seal must not acknowledge from its fast path: the check
  "still primary at the write" (the admission re-check inside the
  transaction) is required under B1 as much as today. Group commit makes
  the consequence of a missed check a refused proposal rather than a
  lost write, but the check stays.
- **Failure detection under load.** False seals (F2) and lapsed grants
  under disk stalls (plan 41) are liveness problems with the same
  envelope in every design; attentive-time measurement, dedicated
  threads for heartbeats and a renewal lane are the fixes, and the
  commit rule only makes a false positive cheap.
- **Quorum availability at small sizes.** Two nodes have exactly one
  failover path (seal + S3 CAS); when S3 is also away they have a
  continuation epoch only with both present. Nothing in the option space
  changes that; a third machine or a witness does.
- **The lone voter.** A node with no peer in budget acknowledges on its
  own disk or waits for S3. Offline designation is the correct tool for
  the laptop; `ack=s3` for the paranoid; Layer A with a visible branch
  for everyone else. No protocol removes this choice.
- **Clock envelope.** Rate: at most 0.1 % per node for every local window
  (plan 43 C7a: the honour and wait readings carry the budget, so a window
  measured on one node is exact for any oscillator within it). Offset
  (plan 43 C7b, ADR-35): unconstrained; the lease, the seal watch, the
  promises and the grant windows are durations on one node's clock or an
  observation of one object, and the non-minting token check needs the offset
  below the margin or the operation's one-way delay below 2 × margin (either
  alone); the root-to-delegate lock cut still compares walls until its
  root-anchored form lands. A quorum log would not relax any of this (lock
  windows and read leases are durations on a node's clock either way); a
  hybrid logical clock already stamps records.
- **Kernel and store.** The plan 42 races, FUSE `FORGET` semantics, the
  `execve` close window, fjall stalls: unaffected by anything above and
  the majority of the fixes this month.

## 8. Sources

Repository: `docs/explanation/{GOALS,DESIGN,DECISIONS}.md`;
`docs/reference/features/{forwarded-mutations,durability-and-failover,delegations,cluster-locks,cto-modes}.md`;
`docs/plans/v1/done/30-write-path-resilience-and-scale-out.md` (§2
constraints, §3 target design, §6 references); plans 41 and 42;
`docs/plans/v1/PROGRESS.md` entries of 2026-10-03 to 2026-10-16;
`docs/security-audit-2026-09.md`; the week's host-load timing review,
the reviews of the F1 (`b4fe2e6`) and F2 fixes, and the 2026-10-16
replay fix (in review when this was written).

File systems and stores:
- JuiceFS architecture, internals, metadata engines, Redis best practices, cache guide, POSIX compatibility: https://juicefs.com/docs/community/architecture , https://juicefs.com/docs/community/internals , https://juicefs.com/docs/community/databases_for_metadata , https://juicefs.com/docs/community/redis_best_practices , https://juicefs.com/docs/community/guide/cache , https://juicefs.com/docs/community/metadata_engines_benchmark , https://juicefs.com/docs/community/posix_compatibility/
- DeepSeek 3FS design notes and configs: https://github.com/deepseek-ai/3FS/blob/main/docs/design_notes.md , https://github.com/deepseek-ai/3FS/blob/main/configs/mgmtd_main.toml
- s3ql: https://www.rath.org/s3ql-docs/mount.html , https://www.rath.org/s3ql-docs/durability.html ; ObjectiveFS: https://objectivefs.com/features , https://objectivefs.com/faq , https://objectivefs.com/userguide ; cunoFS: https://cuno-cunofs.readthedocs-hosted.com/en/stable/user-guide-core-concepts.html ; Mountpoint semantics: https://github.com/awslabs/mountpoint-s3/blob/main/doc/SEMANTICS.md ; goofys: https://github.com/kahing/goofys
- CephFS: https://docs.ceph.com/en/latest/cephfs/mds-journaling/ , https://docs.ceph.com/en/latest/cephfs/mds-states/ , https://docs.ceph.com/en/latest/cephfs/mds-config-ref/ , https://docs.ceph.com/en/latest/cephfs/multimds/ , https://docs.ceph.com/en/latest/cephfs/eviction/ , https://lists.ceph.io/hyperkitty/list/dev@ceph.io/thread/VFY3A6CLBYLJ3MZSVCQA2Q5BTMFSHHZD/ , https://blogs.ed.ac.uk/mhagdorn/2020/09/09/anatomy-of-a-cephfs-disaster/
- Colossus: https://cloud.google.com/blog/products/storage-data-transfer/a-peek-behind-colossus-googles-file-system , https://cloud.google.com/files/storage_architecture_and_challenges.pdf
- Aurora: https://www.allthingsdistributed.com/2019/03/amazon-aurora-design-cloud-native-relational-database.html , https://www.cs.purdue.edu/homes/bb/cs542-20Spr/readings/impl/sigmod-18-amazon-aurora-avoiding-consensus.pdf , https://blog.acolyer.org/2019/03/27/amazon-aurora-on-avoiding-distributed-consensus-for-i-os-commits-and-membership-changes/ , https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-quorum-and-correlated-failure/ , https://pdos.csail.mit.edu/6.824/papers/aurora-faq.txt ; EFS: https://docs.aws.amazon.com/efs/latest/ug/how-it-works.html
- Neon: https://neon.com/docs/introduction/architecture-overview , https://github.com/neondatabase/neon/blob/main/docs/safekeeper-protocol.md , https://github.com/neondatabase/neon/blob/main/docs/walservice.md , https://jack-vanlightly.com/blog/2025/2/19/log-replication-disaggregation-survey-neon-and-multipaxos
- WarpStream: https://docs.warpstream.com/warpstream/overview/architecture , https://docs.warpstream.com/warpstream/overview/architecture/write-path , https://www.warpstream.com/blog/deterministic-simulation-testing-for-our-entire-saas , https://www.warpstream.com/blog/no-record-left-behind-how-warpstream-can-withstand-cloud-provider-regional-outages ; Bufstream: https://jepsen.io/analyses/bufstream-0.1.0 , https://buf.build/docs/bufstream/architecture/kafka-flow ; Kafka KIP-405: https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
- FoundationDB: https://cacm.acm.org/magazines/2023/6/273229-foundationdb-a-distributed-key-value-store , https://thestrangeloop.com/2014/testing-distributed-systems-w-slash-deterministic-simulation.html

Consensus, leases, fencing:
- Delos: https://www.usenix.org/conference/osdi20/presentation/balakrishnan , https://blog.acolyer.org/2020/11/09/delos/ , https://jack-vanlightly.com/blog/2025/2/5/an-introduction-to-virtual-consensus-in-delos
- Vertical Paxos: https://www.microsoft.com/en-us/research/publication/vertical-paxos-and-primary-backup-replication/ ; PacificA: https://www.microsoft.com/en-us/research/wp-content/uploads/2008/02/tr-2008-25.pdf , https://dsrg.pdos.csail.mit.edu/2013/06/06/pacifica/ ; NetChain (f+1 remark): https://arxiv.org/pdf/1802.08236 ; Flexible Paxos: https://arxiv.org/abs/1608.06696
- CockroachDB leader leases / store liveness: https://emptysqua.re/blog/review-scalable-leader-leases-for-multi-consensus-groups-in-cockroachdb/ , https://www.cockroachlabs.com/docs/stable/architecture/replication-layer (leader leases) ; disk-stall self-termination: CockroachDB docs on `storage_disk_stalled`
- etcd: https://jepsen.io/analyses/etcd-3.4.3 , https://etcd.io/blog/jepsen-343-results/ , https://jepsen.io/analyses/jetcd-0.8.2 , https://docs.okd.io/4.20/etcd/etcd-practices.html , https://github.com/etcd-io/etcd/blob/main/contrib/lock/README.md
- S3 conditional writes: AWS What's New 2024-08-20 (If-None-Match) and 2024-11-25 (If-Match); https://braindetox.kr/en/posts/s3_does_not_replace_consensus_conditional_writes_2026.html ; SlateDB and turbopuffer design notes via https://mohakchugh.is-a.dev/blog/s3-conditional-writes-distributed-coordination
- Hermes: https://arxiv.org/abs/2001.09804 ; CRAQ: https://www.cs.princeton.edu/~mfreed/docs/craq-usenix09.pdf
- CRDT file systems: https://pages.lip6.fr/Marc.Shapiro/papers/CRDT-filesystem-HotStorage-2021.pdf (ElmerFS) , https://arxiv.org/pdf/1207.5990
- Fencing tokens and leases: Kleppmann, "How to do distributed locking" (2016), https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html ; https://docs.hazelcast.org/docs/5.0/javadoc/com/hazelcast/cp/lock/FencedLock.html
- Two-node clusters need a witness: https://www.dbi-services.com/blog/two-node-patroni-does-not-failover-when-one-node-crashes/ , https://learn.microsoft.com/en-us/windows-server/failover-clustering/deploy-quorum-witness
- RIFL (exactly-once): https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf ; Raft dissertation (client sessions, ReadIndex): https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
