# Plan 46 — Committed or final: one stream per node, the commit rule per configuration, no server-side replay

> **Status (2026-11-05): stage 0 complete** (M0.1–M0.4, under the exit
> amended by D-1; see "As landed (M0.4, 2026-11-05)" in §6 and §12). Plan 43
> is done and closed into this plan (D-4, D-5). **Stage 1 is next**, starting
> with M1.0, the Stateright model.

Read `docs/plans/v1/CONVENTIONS.md` first. This plan changes the
*direction* of the acknowledgement and ordering-node work that plan 43
carries, on the maintainer's decisions of 2026-11-01
([ADR-40](../../../explanation/DECISIONS.md#adr-40-every-acknowledgement-is-committed-or-final-one-stream-per-delegate-node-no-server-side-replay)).
Plan 43's rules stand (S3 is the configuration master; nothing is
acknowledged that the configured group has not committed; nothing
committed is re-executed; authority state lives in the log; stranded
unacknowledged ops are settled before `EIO`). What changes is the
*structure* that enforces them:

1. **Every acknowledgement is committed or final.** A row is either
   *committed when answered* (its configuration's group holds it, or the
   archive does) or *final without a group* (answered by a non-stealable
   authority: a designee, a sticky grant, an epoch hold with one member).
   Nothing acknowledged is ever rolled back. The one exception is a
   **singleton** (no write-eligible peer in the roster), whose
   acknowledgement rests on its disk because nobody else can take its
   lease while it lives and its own restart re-adopts its journal.
2. **The commit rule is a property of the configuration, not of the
   row.** A sequencer's tenure (a root epoch, a node generation) has one
   commit rule, recorded in its configuration object and changed only by
   a reconfiguration record that commits under the old and the new
   configuration together. A row carries its configuration version. The
   five acknowledgement classes decided per row go.
3. **One stream per delegate node.** A delegation grant is to a *node*
   and covers a set of subtrees; the node has one stream, one group, one
   cut. Hash-range splits, per-range generations and the cross-generation
   dependency machinery go. No delegate is lone: a node is delegated only
   with a group (a backup within budget or the root within budget) or
   under a sticky, non-stealable grant.
4. **Far writers hold sticky grants.** A node with no peer within the RTT
   budget writes its subtrees under a non-stealable grant released only by
   the holder itself (idle, unmount, suspend, planned network-down, a
   cooperative handover, `constellation release`) or by the operator's
   `abandon`. The root takes sticky grants on the subtrees *it* writes
   when it has write-eligible peers and no group, never on `/`.
5. **The server never re-executes.** The replay queue, the fold,
   `derive_replay_op`, `Origin::Replay`, the replay half of drop-held and
   `ack=local` are deleted. A requester's in-doubt retry by rid is the
   one copy of an unacknowledged op; a rolled-back row that *was*
   acknowledged exists only on a singleton, and it becomes a conflict copy
   made by the node that holds its data, not a replay.
6. **Then C4 as specified**, on records that no longer meet a generation
   end that does not exist, and C7b-8's root-anchored cut after it.

Every milestone leaves `main` correct and shippable. Each stage deletes
the states the next stage would otherwise have to handle; the order is
chosen for that, not for size.

## Dependencies

- Plan 43 C0–C2f (committed): the oracles I1–I6, the sim rebuild, the
  operator escape, the root group's commit, the delegate's group, the
  generation's cut, a lone root answering other nodes from the archive.
  This plan keeps all of it.
- Plan 43 C2g, C2g′ and C2h's scenarios land first (stage 0 below); they
  are plan 43 milestones, executed as written, with the one narrowing
  stated under stage 0.
- Plan 41 L0 and the F1 fix (committed; the rules stay). Plan 44
  (committed). Plan 45 (committed; hard links no longer move keys between
  generations once there is one stream per node, §5.3).
- The campaign tooling, the `+commit` strict flags, the sim's lease-read
  knob (committed): this plan's fail-before evidence.
- **Plan 41** (lock renewals through disk stalls, open). The lane stays;
  what it renews changes. A sticky grant never times out, so it needs no
  renewal: the lane's delegation path applies to placed grants only, and
  plan 41's role table (§5) is updated in M1.3. The lock-table helpers
  plan 41 L1 fixed (`lock_on_delegated`, `lock_take_generation`,
  `deleg_drop_left`) are keyed by delegation generation and are rewritten
  by M1.2 (one stream per node) and M3.1 (C4). Plan 41's open
  measurements (the stall sweep, the real-device runs, `long_locks`
  ×1,000, the xcheckfs lock run) validate the lane and may run on today's
  code; their lock and delegation scenarios are re-run at M1.2's and
  M3.1's exits. Plan 41's L5/L6 follow-ups (peers' renewals at a stalled
  owner; the lane's own lease probe) wait until after M3.1, since lock
  state in the log changes their design.
- **Plan 40** (READDIRPLUS, open). Its "lone strict sequencer" kernel
  TTL (`cto::lone_kernel_ttl`, `lone_kernel_drain_ms`) means *no other
  node has shown itself* (`ReadDelegations::leave_alone`), i.e. this
  plan's singleton; it is unaffected, and a sticky holder (which sees
  other nodes) keeps TTL 0. M1.1 renames the ack-side "lone" so that the
  two meanings no longer share a word. The split-directory strict-readdir
  gap disappears with the splits (M1.2). Plan 40's remaining items run on
  today's code; its harness rows are part of M1.2's and M1.3's exits.
- **Plan 45** (hard links, done). M1.2 deletes its key-move rule (one
  stream owns every link's parent, or the op is cross-subtree). New
  behaviour: a `link()` or `rename()` across subtrees held by different
  nodes needs both grants, and while one holder is lost mid-write it
  fails with `EXDEV` (`mv` falls back to copy and delete; `ln` fails, as
  POSIX allows) until the holder returns or `repair abandon` runs;
  `delegations.md` (its hard-link section) says so in M1.3. `hardlink-burst`
  and plan 45's box gates on four file systems are re-run at M1.2's exit.
- **Plan 42** (kernel race workarounds, done). Its invalidations follow
  applied log and stream entries, which this plan does not change; the
  `kernel-race-*` harness rows are part of M1.2's and M1.3's exits.
- **Plan 44** (non-blocking log writer, done): no interaction; the new
  `sticky.rs` logs through it.
- **Plan 33** (control plane and UI): `status.ack.classes` becomes
  `{group, final, singleton, archive_waits}` (M1.1) and
  `status.delegation.sticky` is new (M1.3); the UI and dashboard follow in
  the same milestones.
- **Plans 34–36** (ports). The suspend release is per platform: macOS's
  system-will-sleep notification allows a delay; Windows gives about 2 s
  (best effort); an Android node holds no sticky grant, or releases it
  whenever the app goes to the background (plan 36 records which).
- Nothing else is in flight in the authority core.

## 1. History

| When | What | Where |
|---|---|---|
| 2026-09-24 | Plan 30 M9: backups, seals, `AckPolicy`, `ack=s3` | `0e1017b` |
| 2026-09-26..10-07 | Plan 30 M11/M12: delegations, generations, recall, hash-range splits, placement | PROGRESS "Plan 30 M11", "M12" |
| 2026-10-16/17 | The arbiter study (ten acknowledged, fsynced files read empty after a takeover) | `docs/explanation/ARBITER-STUDY.md` |
| 2026-10-17 | ADR-33: commit is the configured group's acknowledgement; plan 43 accepted | ADR-33 |
| 2026-10-22..10-31 | Plan 43 C0, C1-1..C1-4, C2a..C2f landed; ADR-38 (splits off), ADR-39 (cross-generation dependencies) | PROGRESS "Plan 43 …" |
| 2026-11-01 | The acknowledgement area reviewed structurally: 55 bug rows, the mechanism inventory, the survey of other systems, four options; the maintainer decides the staged direction | ADR-40, this plan |

## 2. Diagnosis

The figures are from the review of 2026-11-01 (one row per cluster of
causes, read from PROGRESS, the triage and the commits; not an
exhaustive audit).

- **55 bug rows since 2026-09-25, 41 of them integrity.** Primary
  mechanism: delegations 12, speculation 10, answer classes 8. As *any*
  label, delegations appear in 25 rows, the takeover gate in 13; the
  densest interaction is delegation × takeover gate (8 rows, plus an open
  one). 239 `fix` commits touched `crates/authority` or `crates/meta`
  since 2026-09-18, 94 of them since 10-23.
- **19 of 51 closed rows (37 %) were introduced by changes in this
  area**: 6 regressions from landed commits, 11 defects in a fix caught by
  its own review, 2 earlier fixes incomplete. 9 of 13 first reviews did
  not clear; each HOLD named a MUST defect inside new code. Two fix
  series dropped acknowledged rows before landing (ADR-39's, twice; the
  re-append answered before commit, 19 seeds).
- **16 rows were found by review or reading with no oracle firing**, 11
  of them reproducible only by a hand-written unit test: the sim's
  configurations do not reach the states ("sim configs never flip
  lone↔group").
- **Non-test authority core code grew 22.4k → 46.5k lines in five weeks
  of fixing**; `core/tests.rs` 6.4k → 48.4k; `meta/src` 22.7k → 51.9k.
- **Nothing in the rules is wrong.** Invariants I1–I5 are the right ones.
  The bugs come from four structural choices: (1) commit decided *per
  row* by a five-way class function over mutable state (`AckClass`:
  Archive, RootGroup, DelegGroup{gen}, NonStealable, Lone; chosen at
  journal time from inputs that change underneath it — rows 24, 29, 38,
  39, 48, 51 of the review are the class, the lock, the lead and the
  published horizon disagreeing by one step); (2) an acknowledgement the
  *server* can take back, so the server keeps a second executable copy of
  every op (rows 8, 10, 11, 13, 14, 25, 26, 35, 36, 50, 52 are copies
  racing; member copies with four drop rules exist to *exempt* committed
  rows from a rollback that should not apply to them); (3) a delegate as
  a second sequencer whose configuration lives in an uncommitted log tip,
  with one node running several generations with causal dependencies
  across them (ADR-39's fixpoint is the admission; rows 39, 41, 42, 43,
  part of 49/53); (4) authority state travelling in messages (plan 43 C4
  is the fix and is unbuilt).
- **No surveyed system acknowledges and later rolls the acknowledgement
  back.** Systems either acknowledge after a durable quorum with epoch
  fencing and truncation (Aurora, FoundationDB, HDFS QJM, Spanner, Raft),
  or reply early and let the *client* keep and replay the request while
  the server only truncates at recovery (Lustre's transno and
  `last_committed`, CephFS's unsafe replies and `clientreplay`), or make
  commit a provable function of durable state (CockroachDB's parallel
  commits, whose TLA+ property is "a client is never acknowledged before
  the transaction is committed"). Calvin's authors name asynchronous
  replication of the ordering log as the mode with "significant
  complexity in failover". In every surveyed system the commit rule is a
  property of the configuration (epoch, replica set), fixed for a tenure
  and changed by a reconfiguration that commits under old ∪ new. Sources
  in §13.

What forces early acknowledgement is hard constraints 3 and 4 together
with ADR-23: answer from the writer's own disk, scale writers by
delegation, never wait for S3. Constraint 3 forces it only for a
*singleton*, whose lone answer no other node can roll back. The rollback
states exist only because the `Lone` class was extended to nodes that
*have* peers: a root with distant peers, and a delegate with no backup
and the root beyond the budget. A sticky non-stealable grant gives such a
node the same zero latency with a final answer, at the failure mode the
maintainer has already accepted for groups (plan 43 G4: a claim waits for
its group without bound; `repair abandon-group`) and for designees (a
dead designee blocks its subtree until it returns or is released).

## 3. Decisions (the maintainer, 2026-11-01), with their reasons

| # | Decision | Reason |
|---|---|---|
| D1 | **Direction**: the staged recommendation of the review (stage 0 settle items; stage 1 one stream per node, classes per configuration, no lone delegates, sticky placement, the hold as a group, with C3's verbatim re-ship folded in; stage 2 delete server-side replay; stage 3 C4, then C7b-8). | Each stage deletes the states the next would have to handle; total effort ≈ 12–18 coder-weeks against 8–12 for finishing plan 43 as specified, for two row states instead of five, one sequencer per node instead of several, no server-side re-execution, and 10–14k fewer lines in the area that produced 41 integrity bugs in six weeks. |
| D2 (Q1) | **Far writers hold automatic sticky grants**, with holder-side idle release (default ≈ 60 s, configurable) and the release always initiated by the holder; release on clean unmount, on suspend/hibernate/shutdown (a systemd-logind delay inhibitor), on planned network-down (a NetworkManager pre-down dispatcher), by cooperative handover when another node wants the subtree, by `constellation release <fs> [--wait]`; operator `abandon` as the last resort; an optional self-fencing timeout as a setting, **off** by default. | A far writer keeps local speed with a *final* answer; the root never times a grant out, so no acknowledged row can be rolled back by a reclaim; the failure mode (a node lost mid-write blocks its subtree until it returns or the operator abandons) is the one already accepted for groups and designees. `sync`/`syncfs` never reach a plain FUSE daemon (DESIGN §3), so there is no sync hook; the lifecycle hooks above are where a holder can still act. |
| D3 (Q2) | **The root takes sticky grants on the subtrees it writes itself** (never on `/`) when it has write-eligible peers and no group, so no node with write-eligible peers ever answers from its disk alone; every acknowledgement is committed or final; nothing acknowledged is ever rolled back. A **singleton** still answers from its disk. The model covers the transition when the first peer joins. | Removes the last rollback of an acknowledged row on a node with peers, and with it the whole server-side replay. The singleton's answer cannot be rolled back by anyone else; its own restart re-adopts its journal. |
| D4 | **Drop `ack=local`.** The lease's `AckPolicy::Local` and the lone-root branch go; `fs create --ack-policy s3` stays (§5.6 says what replaces the flag's `local` value). Stage 2 is a *deletion* of server-side replay, without a client-owned replay list, provided the requester's existing in-doubt retry covers every unacknowledged row (§5.5 verifies it against the code and names the one small piece missing). | Designations cover offline laptops; nothing else needs a lone branch on a node with peers. A client-owned replay list would keep a rollback-and-replay path alive for one opt-in. |
| D5 (Q3) | **Delete hash-range splits** (the mechanism, not only the default). | ADR-38 measured 0.01×–0.91× on real names; splits are the main source of multi-generation nodes and of the cross-generation dependency machinery (ADR-39). |
| D6 (Q4) | **The epoch hold waits for one member**: the hold is a group {owner, one member}; its answers to other nodes wait for one member's acknowledgement of the stream-ahead. | Removes the hold's uncommitted `NonStealable` answers to members (rows 51, 52's variant 3) at one LAN round trip inside an epoch, whose members are on the LAN by construction. |
| D7 (Q5) | **Fold C3 into stage 1**, with a two-week timebox on the Stateright model; if the model is not proven by then, finish C3 as written first and start stage 1 after it. | One stream per node makes C3's items 1–3 smaller; the timebox bounds the risk of doing the recall path twice. |
| D8 (Q6) | **Keep conflict copies** as the artifact of a refused or stranded acknowledged row; the node that holds the data makes the copy. | GOALS promise them; the requester has the bytes. |
| D9 (Q7) | **Stop**: plan 43 C6, C7b-8 (until after C4), the G1 p90 nudge (the patch is saved; re-measure after stage 1), C2h's flex strict flags, C3 and C5 as specified; do not start C5. | C6 is not needed for correctness and the knob exists; C7b-8 moves with C4 and stage 1 changes the cut; the nudge is 0.33 ms on rename/unlink p90 and stage 1 removes one stream wait; the flex strict flags fail 36–81 of 2,000 seeds on states stage 1 changes (the hold's class); C3's items 1–3 are redone under one stream, item 4 (the open placement-hot seed) is folded into stage 1; C5 isolates a branch that stage 2 deletes. |

## 4. What the code does today (what each milestone replaces)

- **Classes per row.** `AckClass` (`crates/authority/src/core/backup.rs`)
  has five values; `root_ack_class_for` (`backup.rs`), `deleg_ack_class`
  (`core/delegate.rs`) and `RowClass` (`crates/meta/src/store/local.rs`)
  decide it when a row is journaled, from whether a peer is within budget
  and stable, whether a backup is listed, requested or removed, whether
  the delegate's journal is within `delegation_stream_rows` (256) of the
  root's cursor, which generation owns the keys and who asked. The fast
  path falls back to `journaled_class` (`crates/engine/src/view/write_gate.rs`);
  the store re-classes the core's row under the writer lock (ADR-33, C2c).
  `status.ack.classes` counts five classes; the sim's `LoneLead` oracle
  checks the lead.
- **The server rolls back and re-executes.** `strand_below_epoch`,
  `strand_for_takeover`, `strand_local_tx` (`crates/meta/src/store/spec.rs`)
  roll rows back from before-images and enqueue them (`pending_replay`);
  `crates/authority/src/core/replay.rs` (883 lines) drains the queue by
  rid through the forward path with `Origin::Replay`;
  `crates/meta/src/replay.rs` (1,755 lines) and `derive_replay_op`
  (`spec.rs`) rebuild ops from rows; `ReplayState.unacked` marks the rows
  whose reply was still parked when the tenure ended (`abort_durable_parks`,
  `core/readindex.rs`) so a refused replay is the client's answer. Member
  copies (`crates/meta/src/store/member.rs`, four drop rules) exempt
  committed rows from this. `repair drop-held` (`store/held.rs`) replays
  dependents by rid.
- **Delegation as a second sequencer with ranges and generations.**
  `Delegate { dir, node, gen, designated, range }`, `DelegGroup { dir, gen,
  version, backups }`, `RecallCut { dir, gen, through }`
  (`crates/meta/src/record.rs`); `GenState`/`DelegateState`, the per-range
  windows (`core/deleg_windows.rs`), `place_splits` and the 16-bucket name
  histograms (`core/placement.rs`), the lead (`delegation_stream_rows`),
  `deleg_caps`/`deleg_note_xdeps`/`deleg_covered` (ADR-39), the "unlink
  moves the file's keys to the next link's generation" rule, the
  lone↔`DelegGroup` hysteresis per generation, `deleg_lost_after_ack`,
  `deleg_xgen_acks_delayed`. Placement proposes and recalls on 30 s
  windows (70 %/50 %, a dwell, a cool-down). Designations are
  `Delegate { designated: true }` (`crates/engine/src/designation.rs`).
- **The lone root.** `AckPolicy::Local` (`crates/store-s3/src/lease.rs`)
  is the lease's state when no backup is listed; a root with distant peers
  answers its own node from its disk (`Lone`), other nodes from the
  archive (C2f). `fs create --ack-policy local|s3` and `CONSTELLATION_ACK`
  choose `Local` or `S3` at creation (`parse_ack_policy`,
  `crates/engine/src/authority_driver.rs`); `Local` becomes `Backup` when
  a backup is listed.
- **The epoch hold** answers members `NonStealable`, uncommitted (plan 43
  Q3, Q12); `hold_owner_never_deposed_except_by_retire` is the flex model's
  guard (`crates/model/src/flex.rs`).

## 5. Design

### 5.1 Two row states, decided by the configuration

A sequencer's **configuration** is the versioned object the reconfigurer
reads: for the root, the lease (`backups`, `config_version`,
`ack_policy`); for a delegate node, its generation's group record
(`DelegateNodeGroup`, §5.3). Each configuration has one **commit rule**:

- *group*: committed once every member of the configuration holds the
  row (the root's holder and listed backups; the delegate node and its
  recorded backup, or the root's appended row when none is recorded);
  with no member listed, committed once the archive carries it;
- *final*: a non-stealable configuration (a designation, a sticky grant,
  an epoch hold with one member): the answer is final when the holder's
  disk has the row, because no one can replace the holder without its
  own release record or the operator's abandon.

A row carries its **configuration version**, not a class:
`RowCommit::Group { config_version }` or `RowCommit::Final` replaces
`RowClass` and `AckClass`. A change of configuration is a reconfiguration
record that commits under old ∪ new: the lease's add-then-record and
remove-when-covered rules (plan 30 M9, plan 43 C1) applied uniformly to
the node stream's group. The per-row decision sites
(`root_ack_class_for`, `deleg_ack_class`, the under-lock re-class, the
published horizon with class, `journaled_class`) become one lookup of the
configuration in force at the row's journal position. The fast path
writes under the configuration version the core published and waits for
that configuration's commit point; a configuration change between the
publish and the journal is caught where it is today (the writer lock),
but it now compares two version numbers, not two class decisions.

`Lone` survives only as the **singleton** case: a root whose roster
(`nodes/`) holds no write-eligible peer answers from its disk; the sim and
model name it `Singleton`, and a counter (`status.ack.singleton_acks`)
replaces `lone_acks`. The singleton's rows are speculation in ADR-19's
sense only towards its own restart (which re-adopts its journal) and
towards the archive (which it reaches alone). §5.5 says what happens if a
singleton dies with unshipped rows and a new node joins before it
returns.

`Archive` is not a class: it is the group commit of a configuration with
no members (a root with no backup in budget answering a forward, plan 43
C2f). `AnswerSource` stays as what it is, the record of what the answer
waited for, for `status` and the oracles.

### 5.2 One stream per delegate node

A delegation grant is to a **node**, covering a set of subtrees. The
records (`crates/meta/src/record.rs`; the old ones go, §10):

```
DelegateNode      { node, gen, dirs: Vec<SubtreeGrant> }   // root record: a generation opens or its set grows
SubtreeGrant      { dir, kind: Placed | Sticky | Designated }
SubtreeReleased   { node, gen, dir, at: idx }              // delegate's stream: the holder releases one subtree
StreamReleased    { node, gen, through: idx }              // delegate's stream: the holder releases every subtree (unmount, suspend, `release`)
DelegateNodeGroup { node, gen, version, backups }          // root record: the stream's configuration (DelegGroup's successor)
DelegateNodeEnded { node, gen, through }                   // root record: the generation's end and cut (RecallCut's successor)
```

- The node has **one stream** `(gen, idx)`, one group, one cut. Its rows
  in every subtree it holds are one total order; a rename between two of
  its subtrees is an ordinary op. The ownership walk (`crates/meta/src/delegation.rs`)
  resolves a dentry to the delegation containing its parent as today, and
  then to the *node* holding that subtree; the table's row is keyed by
  `node` with the subtree set folded from the records.
- **Growing the set** is a root record (`DelegateNode` with the new
  `dirs`; the delegate learns it from its table, never from a message —
  the C2 spec's D4 rule). **Shrinking it** is a row *in the delegate's
  stream*: `SubtreeReleased { dir, at }` names the last index of the
  stream that touched `dir`, so the root sequences ops in `dir` only after
  appending through `at`. A recall by the root (a cross-subtree op, a
  placement decision, a handover) is a *request* to the delegate, answered
  by that record; a placed grant whose holder is unreachable ends by TTL
  as today (`DelegateNodeEnded` with the cut at the root's cursor, after
  the recorded backup's seal when one exists, plan 43 C2e); a sticky grant
  never ends by TTL (§5.4).
- **No lone delegate.** `DelegateNode` is written only for a node with a
  group within budget (a backup, or the root) or for a `Sticky` or
  `Designated` grant. The lead (`delegation_stream_rows` as a commit
  input), the lone↔group hysteresis, the delegate's `Lone` paths and
  `deleg_lost_after_ack` go. A far writer with neither is forwarded (WAN
  RTT, as a non-delegated writer today) until placement gives it a sticky
  grant (§5.4).
- **ADR-39 goes.** With one total order per node and one group, nothing
  is acknowledged past the cut without being in the sealed tail, so a lost
  dependency across generations cannot exist: `deleg_caps`,
  `deleg_note_xdeps`, `deleg_covered`, the xgen counters, and plan 45's
  "unlink moves the file's keys to the next link's generation" rule go.
- **C3's verbatim re-ship, under one stream.** The root appends the
  stream's rows verbatim under `(gen, idx)` (landed in part); the tail of
  a sealed stream is applied verbatim in one transaction
  (`MetaError::TailDiverged`); a delegate taking the root over re-appends
  its own stream's rows the tail lacks, verbatim, before ending its
  generation; a deposed root keeps another node's appended rows as a
  member copy and the next root seals the predecessor's stream (C3 §5.4
  item 4; the open `placement-hot` seed). The fold, `MutateOp::Records`
  copies, holed generations and the deposed root's rebuild-replay go here,
  not in stage 2, because with no lone delegate nothing a delegate answers
  is ever replayed.
- **Generation numbers (ADR-43, 2026-11-04).** `DelegateNode.gen` is minted
  by the same rule as `Delegate.gen` today: `gen = epoch << 32 | n` by the
  root tenure that grants it, in its own epoch's block, `n` from the node's
  counter synced before the grant leaves, a number outside the block refused
  (`Meta::next_delegation_gen`). `(gen, idx)` stays the stream identity, so
  the per-generation state a node keeps (journal origins, the delegate
  backup's tail and seal, `deleg_idx`, the L1 mark) is never read as
  another grant's. A sticky grant (§5.4) keeps its minting epoch across
  tenures: the number is the grant's identity, not the holder's
  ownership; `gen_epoch(gen) == my epoch` says "granted by this tenure".

The model decides the one open design point before code: whether a
subtree-set stream keeps `recall_safety` with one cut per stream. If it
needs a per-subtree cut that re-creates cross-cut dependencies, the
fallback is **one subtree per node** (a node dominating two trees gets
one grant on their common ancestor or one of them) with the rest of §5
unchanged. M1.0's chunk 4 decided it: one cut per stream holds, no
fallback (§9).

### 5.3 Sticky placement

Placement (`core/placement.rs`) proposes a delegation once and recalls
only for a cross-subtree op, an explicit command, a handover request, or
after a long dwell (`CONSTELLATION_PLACEMENT_DWELL_MS` raised to minutes;
the 30 s windows and 70 %/50 % thresholds stay as the *proposal* input
only). The split side (`place_splits`, the histograms, `--range`) goes.
Reconfigurations are where the delegation × takeover bugs live; sticky
placement reduces their frequency, as CephFS does by shipping its
balancer off and offering pins.

### 5.4 Sticky grants (D2, D3)

A `SubtreeGrant { kind: Sticky }` is a non-stealable grant with
cooperative release, the same machinery as a designation (which is
`kind: Designated`: released only by `online`). Its answers are
`RowCommit::Final`.

**Who gets one.** (a) Placement gives a sticky grant to a node that
dominates a subtree and has no peer within `CONSTELLATION_BACKUP_RTT_BUDGET_MS`
(5 ms), the root included (today's lone delegate). (b) The root takes a
sticky grant on each subtree *it* writes when it has write-eligible peers
and no group: placement's proposal walk applied to the root's own
origin, the topmost directory it dominates, never `/`. The grant record
commits by the archive (the root has no group), so the root's first op in
a new subtree waits one S3 round trip; afterwards its ops in that subtree
are final at local speed. The root's ops outside any sticky subtree (a
file directly in `/`, a subtree another node holds) wait for the archive
or are forwarded, as today's forwards to a lone root do. (c) By hand:
`constellation delegate --sticky <path>`.

**Release is the holder's.** The root never times a sticky grant out. The
holder releases by draining its stream, waiting until the root has
appended through its last row (the stream's commit), and writing
`SubtreeReleased` or `StreamReleased` in its stream; the root appends it
and writes `DelegateNodeEnded` (or the shrunken `DelegateNode`). Triggers,
all in the engine's `sticky` module (`crates/engine/src/sticky.rs`, new):

- *idle*: no op in the subtree for `CONSTELLATION_STICKY_IDLE_RELEASE_MS`
  (default 60,000; 0 disables);
- *clean unmount*: the view's close drains and releases before the
  daemon's handover or exit;
- *suspend, hibernate, shutdown*: a systemd-logind delay inhibitor
  (`Inhibit("sleep:shutdown", …, "delay")`, acting on `PrepareForSleep`
  and `PrepareForShutdown`), releasing within the inhibitor's window;
  the package ships a logind drop-in raising `InhibitDelayMaxSec` if the
  default is too short for a drain (`crates/platform`, Linux);
  macOS and Windows per their ports (plans 34, 35), best-effort;
- *planned network-down*: a NetworkManager pre-down dispatcher script
  calling `constellation release --wait` (shipped in the package, Linux);
- *cooperative handover*: the root's recall request on behalf of another
  node that wants to write the subtree. Until it is handed over, the other
  node's writes there are forwarded to the holder and answered by it
  (final, at the round trip to the holder). The root requests a handover
  only for the *dominant* writer (placement's share rule over its window,
  not the first forwarded write), and not within a *minimum tenure* after
  the grant last changed hands (default 30 s), so that two nodes
  alternating in one subtree settle on one holder instead of handing it
  back and forth. On a request, the holder releases at once when it has
  no op in flight in that subtree, or after it has been idle there for the
  handover grace (default 5 s). All three are settings; the idle release
  (≈ 60 s) is separate and applies with no other writer;
- *by hand*: `constellation release <fs> [--wait]`, a holder-side
  command releasing every sticky grant the node holds on `<fs>`
  (`--wait` blocks until the root has recorded the end; non-zero exit if
  not released). It is a top-level command, distinct from
  `snapshot release`.

**A node lost mid-write.** Its subtree is blocked for other writers
(`EROFS` at the root, `EXDEV` for cross-subtree ops, as under a
designation) until it returns and releases or the operator runs
`constellation repair abandon --node <id> [--gen <g>]` (the existing
`abandon-group`'s sibling: the root ends the generation at its cursor;
rows the holder acknowledged past the cut are *lost for others and kept
by the holder* as conflict copies on its return, §5.5). Meanwhile a
`link()` or `rename()` between its subtree and another node's fails with
`EXDEV` (see Dependencies, plan 45). Nothing the
holder's clients were told is rolled back by anyone but the operator, and
the operator's action is documented as the loss it is (I5's clause (b)).

**Self-fencing (optional, off).** `CONSTELLATION_STICKY_SELF_FENCE_MS=Y`
(default 0): a holder that has not heard the root for `Y` ms stops
admitting local writes in its sticky subtrees (`EROFS` to its own
applications) and keeps the ones acknowledged in the last `Y` ms
unstreamed marked; the root may reclaim after `X ≫ Y`
(`CONSTELLATION_STICKY_RECLAIM_MS`, honour-clock terms per ADR-35), and
the holder, on return, rolls back exactly the marked rows into conflict
copies. With the setting on, an acknowledged row *can* be rolled back
within the last `Y` ms before a partition; that is the setting's
documented contract and why it is off by default.

### 5.5 Stage 2: no server-side replay, and what the requester already covers

**Rule.** A deposed, sealed or ended sequencer never re-executes
anything. On deposition it *truncates*: committed rows stay as a member
copy (C1-4, unchanged); uncommitted rows are rolled back from their
before-images (`strand_below_epoch`'s undo half stays) and deleted, with
no queue entry. `core/replay.rs`, `meta/replay.rs`, `derive_replay_op`,
the fold and conditional truncate, `pending_replay`, `Origin::Replay`,
`ReplayState`, `mark_unacked_replays`, the replay half of `held.rs`, the
sim's `Origin::Replay` paths and the model's replay queue go.

**Verification against the code (to be re-done as stage 2's first
milestone, M2.0, before any deletion).** Who holds the one copy of an op
whose row is rolled back uncommitted?

- *Another node's forward* (`PeerMsg::Mutate`): its `ClientOp` entry
  (`core/client.rs`: `submit`, `on_client_deadline`) keeps the op until a
  definitive outcome; a parked reply aborted at the tenure's end is
  answered `Busy` (`abort_durable_parks`) and the requester retries by rid
  within its backoff; `completed` dedups; `in_doubt_rids` marks a
  resubmission in doubt; the split deadline settles it (C2g). Covered.
- *An inbox op*: durable in the batch, re-submitted by the inbox path
  under the next epoch (`InboxWaiting`, `InboxWithdraw`). Covered.
- *A delegate's own fast-path op whose generation is lost*: the write
  gate resubmits it in doubt by rid through the core
  (`write_gate.rs`, "leaves the op in doubt: resubmitted by rid through
  the core"; `the_delegates_fast_path_resubmits_in_doubt_when_its_generation_is_lost`).
  Covered.
- *A root's own op (fast path or core) parked for its group when the
  tenure ends*: **not covered by a client-path retry today.**
  `abort_durable_parks` answers remote parks `Busy` but puts the own
  node's rids into `ReplayState.unacked`, and the recovery that follows
  *queues their rows for replay by rid*; a refused replay is the client's
  answer. With the queue gone, the own-node op must take the same path
  the delegate's fast path takes: `abort_durable_parks` resubmits the
  own rid in doubt through `submit` (`Origin::Client`, `in_doubt_rids`),
  and the core-path own op keeps its `ClientOp` entry across the
  deposition instead of being answered from the queue. This is the one
  **minimal piece** stage 2 adds: no list, no persistence (the op is in
  the FUSE request that is still waiting; a daemon crash loses the
  request with its application, as any unacknowledged op), about the
  size of the delegate fast path's existing rule.
- *A row that was acknowledged and is rolled back*: only a singleton's
  (D3), or a sticky holder's past an operator's abandon or a self-fence
  (§5.4). These are not replayed either. The holder **exports** them as
  conflict copies: `conflict_export` (beside today's `conflict_copy` in
  `crates/engine/src/recovery.rs`, which stays as the writer) submits, for each rolled-back
  acknowledged row in journal order, a fresh create under
  `.constellation-conflict/` with the row's after-image (the manifest and
  chunks are the holder's own) through the normal client path, as a new
  op with a new rid. It is not a replay: it never re-executes the
  original op, never touches the original name, and refuses nothing (a
  copy's name is unique). D8 keeps the artifact; the producer changes.
- *A singleton that dies with unshipped acknowledged rows while a new
  node joins and claims the lease after the TTL*: on return the singleton
  finds its epoch superseded; its unshipped rows are exported as above.
  This is the one window where an acknowledged row is not in the
  namespace it was acknowledged into, and it is the single-node contract
  as today (ADR-21: a lone node's unsynced rows are lost with its
  clients; here they survive as copies). The model covers the join
  (`first_peer_join`): a root with a newly write-eligible peer answers
  nothing alone until its grant or group record is committed.

**Drop-held after stage 2.** Option B's placeholder (C2g′) exists to
order server-side re-executions behind a requester's retry; with none,
`repair drop-held` rolls back the held row and its dependents, journals
the seed's refusal (a conflict copy with holes, as today) and lets the
requesters retry; own acknowledged dependents (a singleton's) are
exported. The `AwaitRetry` kind and the park rule are deleted with the
queue; the bound (`Refused { rid, ABANDONED }` after 2 × TTL) stays as
the executor's settle for an absent requester (C2g).

### 5.6 `ack=local` and the `--ack-policy` flag (D4)

Today `fs create --ack-policy local|s3` (`CONSTELLATION_ACK` the default)
stores `local` or `s3` in the filesystem's meta; `ack_s3_of` turns it
into the lease's `AckPolicy::S3`, and anything else into
`AckPolicy::Local`, which becomes `Backup` when a backup is listed.
`local` therefore means "Layer A alone, Layer B with a peer in budget".
After stage 2 the lease's `Local` state has no behaviour of its own: a
root with no backup commits by the archive (its forwards already do,
C2f), by a sticky grant (its own subtrees), or alone only as a singleton
(derived from the roster, not configured). So `AckPolicy::Local` is
deleted; `AckPolicy::{Backup, S3}` stay (`Backup` with an empty set is the
archive rule). The flag keeps one value: `fs create --ack-policy s3`
(Layer C: every acknowledgement waits for the archive, no backups, fast
takeover by any peer — plan 39's recommendation for fsync-to-S3, plan
37's pool filesystems); a creation without it stores no policy, as today.
`local` is no longer accepted (no alias: no shims). The `cto`, `fsync`
and `write-mode` knobs are untouched.

### 5.7 The epoch hold as a group (D6)

The hold's configuration is {owner, one member}: an epoch with two or
more members commits a row when one member acknowledges the owner's
stream-ahead of it (`StreamAheadAck { epoch, through }`, a new P2P
message; members are on the LAN by construction). A one-member epoch's
hold is `Final` (non-stealable, as today; nobody else is in the epoch).
Row 51's hold classes and row 52's variant 3 go; `hold_owner_never_deposed_except_by_retire`
stays as the guard for the retire path.

*Model (M1.0 chunk C10, 2026-11-06).* `crates/model/src/flex.rs` models the
group: the owner executes into its journal and answers a row once a member
holds it (`StreamAhead`, then `AnswerHold`); a one-member epoch answers
at once (`Final`); `Close` flushes the journal into the log.
`hold_answer_committed_or_final` and `hold_owner_never_deposed_except_by_retire`
hold on the flex bounds (`N = 3, f = 1`, with and without drift, and
`N = 2, f = 1` with one-member epochs); the knob `hold_answers_uncommitted`
(today's answer to members) is found by the first. M1.4's model gate is met.

### 5.8 Stage 3: C4 as specified, then C7b-8

Plan 43 §5.5 and its coder spec, unchanged in substance: `LockGrant`,
`LockRelease`, `LockExpire`, `LockMove` in the log, grants answered after
the record commits (G3), the mirror and handoff machinery deleted, I3
strict. What stage 1 changes for it: a lock follows a *node stream*, not a
generation of a directory (`LockMove { to: node, gen }`), and no lock
record can meet a generation end that strands part of a node's rows (the
review's H4 reason for ordering C4 after C3 holds by construction). Then
C7b-8 (barriers and cuts on the wait clock, the root-anchored cut, the
echo of the root's send stamp) on the new records.

### 5.9 The integrity argument, restated

I1 (no definitive answer without a committed row) becomes CockroachDB's
property, stated in the model as `ack_implies_committed_by_config`: "a
client is never acknowledged before the row is committed by the
configuration in force at its journal position, or that configuration is
final". I2 (one identity, one executable copy) holds because the only
executable copy of an unsettled op is its requester's entry (no server
queue exists). I4 (`EIO` only after settlement) is C2g's, unchanged. I5
("a rollback of an acknowledged OK only if lone, or by operator action")
becomes "only on a singleton, or by operator action, or under
self-fencing", each a counted event with its export. I3 is C4's. The
oracles that check the deleted states (`LoneLead`, the queue's I2(b)
union) go; `strict_durability`, `strict_identity`, `strict_commit`,
`strict_settle` and `member_copies_drained` stay and run strict on every
configuration, flex configurations included (which D9 stops short of
today precisely because stage 1 changes the hold).

## 6. Milestones

Each milestone: fail-before evidence first; the model before the code
where the design is new; a fable review before the next starts; the
"Deleted" table in PROGRESS per removed mechanism (its introducing
commit, the bug it was for, the test that now shows the state cannot
arise), as plan 43 §15 prescribes.

### Stage 0 — settle (plan 43's C2g, C2g′, C2h scenarios; ≈ 1.5 weeks)

| M | Scope | Deletions | Fail-before | Oracles / sims / scenarios / model | Exit |
|---|---|---|---|---|---|
| **M0.1 C2g** | As plan 43 §5.3 item 5 and the C2 spec §6: `settle` on `MutateRequest` and `InboxOp`, the executor journals `Refused { rid, ABANDONED }`, the requester's split deadline. | — | I4 (zero hits today because the fix is unbuilt): `load-probe-takeover` seed 46's shape under `strict_settle`. | Model `eio_never_lands_late` (`protocol.rs`); sim `strict_settle` on every config; harness `takeover-loss-two-node`. | I4 strict everywhere; the ledger's late-landing allowance removed for non-singleton topologies. |
| **M0.2 C2g′** | Drop-held option B (the drop-held proposal's R1–R8): `AwaitRetry` placeholders, the drain stops at one, overlapping forwards parked, the bound settles with C2g's record; variant 3(i) for `NonStealable` shadows. | — | `flex-crash` ×2,000 under `strict_identity`: 777 I2(b) copies today; seeds 502/839/1526/1845 (the drop variant), 807 and 39 (I5). | New sim config `drop-held-chain` (and its requester-dies variant); harness `drop-held-remote-dependents`; I2(b) excludes `AwaitRetry` by kind. | Zero I2(b) copies; row 52 closed. (The placeholder is deleted again in M2.2; it is correct meanwhile.) |
| **M0.3 C2h scenarios** | `takeover-loss-two-node`, `tloss-two-node`, `delegated-group-switch`, the die/abandon pair; docs; `bench/commit-rule/RESULTS.md`'s C2 rows. **Not** the strict flags on flex configs (D9). | — | Each scenario fails against a build before its fix. | Harness catalogue; the campaign cell that found the loss ×20. | Scenarios green; the gate-before-M1 fixes landed; the flex strict flags and the other remaining I4 ceilings carried to named stage-1 milestones (D-1, 2026-11-04; see "As landed 2026-11-04/05"). |
| **M0.4** | Whole-C2 fable review (plan 43's "C2 review"). | — | — | — | Required changes landed. **Done (2026-11-05).** |

*As landed (M0.2, 2026-11-01; PROGRESS, "Plan 43 C2g′"):* the drop-held
proposal's R1–R8 as plan 43's C2g′ note records them. `flex-crash` ×2,000
under `strict_identity`: 677 I2 (b) copies in 493 failing seeds on
`92624175`, 0 after; the new sim configs `drop-held-chain` (420 of 500 seeds
failing before, 0 of 2,000 after) and `drop-held-chain-requester-dies` (298
of 500, 0 of 2,000); harness `drop-held-remote-dependents`. Seeds 502, 839,
1526 and 1845 (the drop variant the C2f note did not land) pass. `flex-crash`
807 stays an I5 finding under `strict_lone`: the operator's `drop-held` of an
acknowledged hold answer, which the oracle does not class as operator action
yet (`flex-crash+lone` fails 779 of 2,000 seeds on `92624175` and 778
after, 732 of them on a non-stealable OK; M1.4's flags). Seed 39 does not
fail on `92624175` in any `flex*` or `placement-hot` configuration under the
strict flags. The placeholder is deleted again in M2.2.

*Review fixes (M0.2, 2026-11-01; PROGRESS, "Plan 43 C2g′ review fixes"):* the
sequencer's own ops and a delegate's forwards stand behind the queue (R3 held
for forwards only); the own-replay hold is key-aware; `repair drop-held` runs in
the core, so a backed (group) row whose answer is still parked is a placeholder
too and its parked answer is aborted in the same step; a non-stealable shadow
whose owner's row never reaches the log is stranded after the bound; parks
behind the queue are aborted on lease loss and do not start the lease fallback.
"The retry executes in the dropped row's place" holds while the dropping node
remains the sequencer; after a takeover by another node the retry runs ungated
there (no acknowledged effect is lost). The oracles treat an operator's drop as
action on the rids it rolled back only (I5, I2c, the history witness); the
`drop-held-chain-backed` configuration ran 382 of 400 seeds failing before and
51 of 2,000 after (43 quiescence; the rest fail the same without the fix and
are not this work's: PROGRESS).

*Follow-up fixes (M0.2, 2026-11-02; PROGRESS, "Plan 46 M0.2: the four defects of `drop-held` on a backed root"):* the 51 seeds
the `drop-held-chain-backed` note left open were four defects of the drop, not a takeover. **A (integrity, reachable in
production)**: the drop removed the pending row of a lost chunk that an operation of the node, or a write session, still
named, so a manifest naming a chunk S3 never received shipped later; a chunk they name now keeps its row and marks, and the
engine fails a session that sealed a dropped chunk. **C (I2c)**: a drop run on a node deposed meanwhile made its conflict
copy with no outcome of the rid in the log; the refused entry now settles the rid through the sequencer in force first
(C2g's settle, with the dropped code), and makes no copy when the successor completed the original. **B (quiescence)**: a
committed streamed copy of a dropped row was never retracted at a member, and backups' tails still held it; a new log
record `RolledBack { idents }`, journaled ahead of the drop's refusals, retracts the member's copy and turns the tails'
transactions into holes. **D (liveness)**: a round that moved nothing while rows are held or deferred no longer renews the
handoff pause, which kept the holder's own ops waiting for as long as a peer wanted the lease. **The bound**:
`drop_held_await` is capped at `acquire_deadline - 2 * settle_window` (60 s, 6 s in the sim), so an own op on a dead
requester's placeholder's keys succeeds instead of ending in `EIO`. Not drop-held, left as a separate item: seeds 422, 1128
(history witness under reboots and clock steps).

*As landed (M0.3, 2026-11-02; PROGRESS, "Plan 43 C2h"):* the sim's I4 classifies each unsettled `EIO` from the sim's own
network and fault history (`sim/reach.rs`: partitions, nodes attached, paused, suspended or stalled, S3 cuts, lease bodies and
lease reads, what each core holds and is busy with, the delegation table as it changed): **isolated** (no sequencer reachable
for a retry interval and a round trip during the whole settle window), **no sequencer in time** (the only way was a lease
claim that, as the product implements it, could not finish inside the window: its TTL of the claimant's own observation, the
promise check, the CAS, the group's tail; or the root's outwait of a generation that does not live, its window plus a recall,
was not over), **generation tail unreachable** (the root is due to end the op's generation but reaches no holder of its tail), or
a violation. Ownership is decided at each instant from the generation history (the root does not count while a live grant owns
the keys; once a generation's grant ran out, or it was never installed, or the keys span generations, the root is the op's
sequencer after the allowance, if it is reachable, can commit and reaches the tail), and a sequencer counts only if it can
commit. `strict_unsettled`
(`+unsettled`) fails the run on a violation. 2,000 seeds of every C2 configuration with and without the lease-reads knob: of
28,256 unsettled `EIO`s 2,688 isolated, 4,988 no sequencer in time (2,457 of them keys across generations), 14 generation tail
unreachable and **20,566 violations** (the review round counted a generation that outlived its grant as isolation: 16,664 isolated,
11,461 violations; the first re-check 20,079, before keys across generations became due by their generations' own allowances).

**The M0.3 exit "I4 strict everywhere" is not met.** Strict mode is a flag of the two configurations with no unsettled `EIO`
(`delegated-long-delegation-prod`, `delegated-root-gone`) and runs in a ratchet test for more at zero violations without
the knob (18 in the ratchet at M0.4: fifteen after the settle fixes, three more after the M0.3 G and B fixes and the `long-backup` pass); the other 14 configurations are guarded there by ceilings on their violations and on their allowed unsettled `EIO`s (`m03_the_configurations_at_zero_violations_stay_there`, `crates/authority/tests/sim.rs`; eight of the 14 are at 0 violations, the rest are causes still open in PROGRESS; a fix lowers a ceiling). The violations are six
causes in the product (a root in its takeover gate answering a settle `Busy`; the new root's own settle falling to the lease
path inside its gate; a delegate's own op held by the root for itself; a redirect loop between root and delegate; a local
execution parked behind a recall; the lease path's last stretch), with seeds and lines in PROGRESS, and `delegated-group-switch`
615 and 1170 are real late landings (I4's first clause). **The product fixes for causes 1 to 3 are a gate before M1.** New:
sim `tloss-two-node` and `delegated-group-switch` (the latter fails 7 of 2,000 seeds: findings, two pinned in an ignored test),
the harness `takeover-loss-two-node`, the bug-B harness scenarios rewritten as "the state cannot arise" (plan 43 §7.5;
`takeover-marker-no-stranded-shadow`, `holder-crash-no-phantom-shadow`, `holder-crash-no-phantom-new-holder`), and
`bench/commit-rule/RESULTS.md`'s C2 rows. The scenarios check I4 in both directions: a create that returned success exists
everywhere, one that returned an error is absent everywhere unless the requester's core told an unsettled `EIO`
(`client_unsettled_eio` rose); in `takeover-marker-no-stranded-shadow` the requester's S3 is cut so that the takeover is the
refused `rmdir`'s node (the marker is asserted to be B's). Fail-before: `tloss-two-node` (the stall armed at 0.4 s, 40 ops) fails I1 on 500 of 500 seeds on C2c's parent,
`delegated-group-switch` on 490 of 500, `takeover-loss-two-node` empties files on the build of the loss in 4 of 6 seeds, the three
rewritten scenarios fail their first assertion before C2f. **Deferred, as D9 says: the flex strict flags** stay off until M1.4
(`flex-crash` has 433 isolated and 3,710 violations without the knob). The die/abandon pair is C2e's and swept clean (0 failing
seeds in 2,000 each; the table shows 36 + 27 unsettled `EIO`s with a sequencer reachable, strict off). The campaign cell that found the loss ×20: 20 of 20 pass.

*The gate-before-stage-1 fixes (M0.3, 2026-11-02; PROGRESS, "Plan 46 M0.3: three product causes of an `EIO` that was not
settled first"):* the I4 classifier (`+unsettled`) found `EIO`s that the requester was never settled for while a sequencer was
reachable, and three of the causes are product code, not the sim. **S1**: a takeover gate whose journal work is done
(`PendingGate::shippable`) answers a settle on the keys it owns — the root's, and a generation delegated to the gated node
itself — instead of `Busy` for the 12 to 20 s of the read quarantine and the drain: an unknown rid gets the journaled
`Refused { ABANDONED }`, answered once its row has shipped (the requester being the gated root itself takes the same path in
`settle_route`); a rid that took effect is `Held`, never `Accepted` before the quarantine (M8) and never `Abandoned`.
**S2**: a gated root leaves a delegation renewal unanswered instead of refusing it with `ttl 0`, which stopped the generation
at its delegate and made the opened root outwait it. **S3**: the root does not redirect a settle to a generation it is recalling
(an execution and a designation still are redirected), and a delegate's own op forwarded after its grant had time to arrive (it disowned the generation) is recalled and parked,
not held. Unsettled-EIO violations over 1,000 seeds, `+unsettled`, before and after: `backup-strict` 70 to 3,
`tloss-two-node` 28 to 0, `delegated-group-switch` 90 to 13 (seed 615's late landing gone), `delegated-holder-cut` 483 to
251, `delegated-backup` 147 to 89, `delegated-no-backup-root-near` 149 to 93, `long-delegated-late-answers` 696 to 309,
`long-delegated-backup` 497 to 246, `placement-hot` 107 to 26, `long-delegated` 6 to 4, `delegated-two-gens-root-crash` 5 to 0;
`flex-crash` 1,989 to 1,988 (cause 6, D9: untouched). What was left was S4 (a recall's cut answered before its read recalls)
and S5 (a settle on another node's generation inside a gate: the gate builds no generation state until it opens; **closed by
[ADR-42](../../../explanation/DECISIONS.md#adr-42-a-takeover-gate-whose-journal-work-is-done-serves-the-generations-it-inherits)**,
2026-11-03: a gate whose journal work is done takes on the live generations of other nodes, serves their batches and renewals and
redirects a settle on their keys to the delegate, and recalls, seals, reclaims and ends nothing; what it still cannot answer is an
op that needs a generation recalled or ended, and a delegate that restarted in the gate; PROGRESS, "S5"), and
cause 6, the maintainer's open decision. Not here: S4, the strict flags (`strict_unsettled` goes on where both ways
reach 0). The plain sweeps (2,000 seeds, both modes) add one failing seed on the fix, `long-delegated-late-answers` 1477 with the
knob, a latent replica divergence of a former delegate that main shows at 6183 and 7393 (PROGRESS, **L1**, closed: the cause is
the root's pre-S3 stream install, see the paragraph after this one; 4125 with the knob is not L1: an oracle gap in the session checker's refusal window, closed in
`tests/sim/session.rs` (PROGRESS, "Oracle: a journaled refusal is placed at its own `Refused` row"), pinned by
`lda_4125_slow_lease_reads_seed`);
stage 2 inherits the I2 class on a stranded replay
against the requester's re-ask (**L2**, main's `delegated-no-backup-root-near` 1680 with the knob; `placement-hot` 8557 fails only on 055de5f1, the tree before the settle fixes; closed in the meta store by a deposed root's rebuild
no longer queueing a gated append of a delegate's row, PROGRESS, "L2"; two sources of the same shape are open as L2b); S4 takes **L3**, the lost `DelegRecall` that is never sent again. **L4**, a void on a generation number that a deposed root's unshipped end left and that the successor's grant reuses (the node's next write then names no dependency on its own row), is fixed in `meta/src/replay.rs` (a `Delegate` record lifts the void). The ratchet's ceilings were lowered to the counts on the rebased tree (`delegated-holder-cut` 48 to 14, `long-delegated-late-answers` 63 to 25, `long-delegated-backup` 52 to 23, `placement-hot` 10 to 0, among others).

*L1 closed (2026-11-02; PROGRESS, "L1"):* the replica divergence on a former delegate was one integrity bug in the root's pre-S3
stream install: the early return for the root's append of the delegate's own row recorded nothing, and the delegate's `Local`
row then counted as later in the log than the root rows streamed after it (own `create x`, `RecallCut`, the root's `unlink x`:
`x` came back). Fixed by a persisted per-generation mark `deleg_streamed:{g} = epoch:idx` set when the stream delivers the
append of the own row `(g, i)`: the generation's `Local` rows at or below `i`, same epoch, are ordered ahead of later streamed
rows (`row_is_ordered_at`); cleared by the log's `Recall`/`RecallCut`, never by a streamed one. Pinned by `former_delegate.rs`
and the sim pin `l1_a_former_delegates_own_row_keeps_its_log_place` (seeds 3228, 6183, 7393 no knob; 1477, 5718, 9955 knob);
2,000-seed sweeps of nine configurations plus `long-delegated-late-answers` 2000..10000, measured on 055de5f1 before the rebase: no
seed fails on the fix that passes on main. **M1.2 must carry the rule.** With one stream per node, a delegate's own
row and then a root row on the released subtree can still be streamed ahead, so the mark is keyed by the node's stream
generation, set when the stream delivers the append of the node's own row, and cleared by the durable record that ends the
stream's `Local` rows (`DelegateNodeEnded`, and `SubtreeReleased` for what it releases), never by the streamed install of
that record (it arrives between the own row and the later root row). The lost-recall liveness item (L3) stays a separate item.

*As landed 2026-11-04/05 (maintainer decisions D-1 to D-3; PROGRESS, "Plan status, 2026-11-05"; M0.4, the one stage-0 item then left, landed the same day, below):*
the stage-0 exit is amended: **"I4 strict everywhere" becomes "the gate-before-M1 fixes landed; the remaining I4 ceilings carried to
named stage-1 milestones"** (D-1). What landed: **ADR-43** (generation numbers carry the epoch, `gen = epoch << 32 | n`) with its
leftovers (restart and rebuild void of a streamed-ahead end, `deleg_any` raised on rebuild, an operator's abandon ends the
generations it orphans, the claimant's own orphan rows and the no-tail takeover: the **orphan rule**, with its persisted proof);
**cause 6 re-traced** (D-2: only 7 knob violations were slow claim tails, now classified allowed; the ceilings lowered, none raised);
the product causes **G** (a refused seal tail that proves nothing is held: the member's `empty` proof flag) and **B** (a settle
answered `Held` by a reachable root is asked again there) fixed (`backup-crash` no-knob violations 5 to 0, `backup-strict` 11 to 2,
`long-delegated-backup` 2 to 0); **the undecodable tail** (a held `SealedTail` or `DelegSealed` that does not decode is a
`PeerFailed`, never an empty tail); **the member-copy read hold** (a read of a rolled-back member copy row waits for the re-ship,
seeds 5839, 15029; an in-memory `HeldCopy` set, 5 s cap); **seed 2560** (a deposed root's journal append of a delegate's row is
dropped when a later tenure's segment carries every outcome under another identity; the ADR-33 C3 follow-up amendment); the oracle
false positives of the seed triage (4947, 1525, 234) and the harness ledger's late-landing allowance removed; and **the strict
fixes**: dedup acknowledgements wait for the read quarantine (seed 217254; plan 41's P41-01), and a forwarded op recalls the
read delegations before it executes (strict-reader-ahead 217244, 260; maintainer's option (c), 2026-11-05; the cost is a strict
failover's first write at +3.5 to 4.8 s in five configurations). **Carried, with owners:** S4 (`DelegCut`, cause 5) is deferred to
**M1.3** (D-3), where sticky grants rework the recalls; the follow-up S1 (the root's own local ops recall before they execute) is kept
on branch `rbe2`, not landed (only reachable outside the clock envelope; adds two unsettled `EIO`s in `backup-strict+unsettled`),
and goes to M1.3 with it; the flex strict flags to **M1.4** (D9); the cause-6 buckets **C** (the claim's CAS lost to another write), **A** (the lease won,
the own op still in its gate) and **O** (forwarded to a re-adopting holder instead of claiming) to **M1.3** as lease-path and recall
items (the plan's milestone list does not name them; M1.3 is chosen by the lease-path rule). Not stage-0 items either: L2b and the
C3 rule to M1.2 (above); the strict-isolated-reader seeds are closed (the sim's restart kept read delegations; a product fix for the long variant).
**The in-gate class (2026-11-05, ADR-44):** closed except its residual classes (a delegate restarted in the gate, a rid that took
effect, a gate not shippable while the recall drains, an unreachable delegate). A shippable takeover gate recalls on demand, drains and
journals the cut ([ADR-44](../../../explanation/DECISIONS.md#adr-44-a-takeover-gate-whose-journal-work-is-done-recalls-on-demand));
the M0.4 review's STAGE-1 item "in-gate class, M1.3 with S4: do not add a gated recall now" is superseded by that decision. Under
M1.2/M1.3 the on-demand recall becomes a request the holder answers with `SubtreeReleased`.

*As landed (M0.4, 2026-11-05; PROGRESS, "MUST-1", "Plan 46 M0.4: what the whole-C2 review required, and what landed", "Plan 46 M0.4
follow-up: the classifier's SHOULD-2, SHOULD-3 and SHOULD-4", "Plan 46 M0.4 follow-up: ADR-44"):* the fable review of the whole C2 (on main
`39e85a17`) found one integrity hole and a set of SHOULDs. **MUST-1** (a claimant with no tail ships its rows of a generation the log never
listed as root rows under origin `(g, i)`, completing rids the log had refused: I2a, I2c, I4 late landing): fixed in
`Meta::strand_for_takeover_carrying` (the claimant's strand also takes its own delegate rows of every unlisted generation the log never ended,
whatever the claim carries; the replay by rid meets the log's `Refused { ABANDONED }` and forgets the entry); fails on main before the fix, with a
guard test for a listed generation and a core-level test of the gate wiring. **Landed SHOULDs:** a counting shipper guard (`status.ack.unjustified_ships`,
a debug assertion, a sim failure when any node's counter is not 0; not a refusal, which would hold a root's own appends of a delegate's rows
at the head of the journal); the member-copy read hold's cap counted and warned (`status.ack.held_copy_lapsed`); the documents (ADR-33's status,
DESIGN §4's `Local` bullet, plan 43 §4's I4 as built); `cargo fmt`. **The stricter I4 classifier** (SHOULD-2: keys across live generations the root
reaches are due after one recall round trip and its resend back-off; SHOULD-3: a restarted holder re-adopts its own unexpired lease at once;
SHOULD-4: a generation's group is its latest version, and the tenure group's tail is looked for over the whole stretch of the claim) moved 432 unsettled
`EIO`s from "no sequencer in time" to violations, all of them the in-gate class, which **ADR-44** closes (a shippable takeover gate recalls on demand,
drains and journals the cut of exactly the generations a pending op or settle needs; fable review changes landed): violations over 2,000 seeds fell
from 281 / 676 (no knob / slow lease reads) to 6 / 149 in `long-delegated-late-answers`, 325 / 627 to 35 / 98 in `delegated-holder-cut`, and similarly in
`long-delegated-backup` and `placement-hot`, no failing seed, I1 and I2 at 0. The ratchet's ceilings were **lowered** to the measured 0 and 0 for
`delegated-backup`, `delegated-holder-cut`, `long-delegated-backup` and `long-delegated-late-answers`; none was raised (the allowed ceiling of
`delegated-backup-and-delegate-die` moved from 0 to 26 when the waiting runs' `EIO`s began to be counted, a count the ceiling never saw). **Carried to
stage 1:** SHOULD-1 (the §3.8 drop of an answered gated row at an orphan cut of 0, with ADR-41's second copy; pin
`an_orphan_end_queues_a_delegates_answered_gated_row` ignored) to **M1.2** with L2b; the in-gate class's residual classes (a delegate restarted in the
gate, a rid that took effect, an unreachable delegate, a refusal not shipped by the window's end) to **M1.3** with S4, where the on-demand recall becomes
a request the holder answers with `SubtreeReleased`; ADR-41's lone-journal commit and ADR-43's no-tail proof against sticky grants to the **M1.0**
model, before M1.3; two classifier gaps in the lease-path arm (the generation-tail condition the root arm applies; the requester counted as a member holding
the tail, seed 435; both closed 2026-11-05, PROGRESS "Classifier: the lease path's two gaps closed") and the unmet `member_copies_drained` on `backup-crash-slow` 840's shape (`TailOf` retirement) to **M1.3**; `long-backup` with the knob,
seed 728 (`member_copies_drained`), untriaged, to **M1.3**. **Side findings fixed:** the stateright linearizability search was exponential in overlap
(seed 1498 of `far`, `backup`, `backup-two` ran past 20 minutes); it is the Wing and Gong search with Lowe's memoization now, cross-checked on 10,698
histories. Gates at the last commit: fmt and clippy `-D warnings` clean, authority lib 1,039 passed, the release sim suite 400 passed, 0 failed, 12 ignored.
**Stage 0 is complete** under D-1's amended exit; stage 1 (M1.0, the Stateright model) is next.

### Stage 1 — one stream per node, the rule per configuration (6–10 weeks)

| M | Scope | Deletions | Fail-before | Oracles / sims / scenarios / model | Exit |
|---|---|---|---|---|---|
| **M1.0 Model** (two-week timebox, D7: 2026-11-05 to 2026-11-19) | `crates/model/src/node_stream.rs`, a new untimed model (abstract TTL), built in chunks (§11, "M1.0 answers"); `delegation.rs` stays the timed model of placed grants (it never had the range dimension); a node holds a *set* of subtrees under one stream `(gen, idx)` with one group and one cut; `SubtreeReleased`/`StreamReleased` as stream rows; sticky grants (never ended by TTL; ended by the holder's record or `abandon`); the root's own sticky grants; the singleton and `first_peer_join`. Properties: `recall_safety`, `causal_cut`, `per_key_linearizable`, `exactly_once`, `designation_final` (re-proved), new `ack_implies_committed_by_config`, `released_subtree_sequenced_after_cut`, `sticky_ends_only_by_holder_or_operator`, `no_lone_answer_with_peer`, `two_subtrees_one_order` (a rename between a node's two subtrees needs no recall). `flex.rs`: the hold as {owner, one member}. | `crates/model/src/hotdir.rs` (the hash-range model), deleted with the splits in M1.2. | Knobs that re-introduce each deleted mechanism must violate: `lone_delegate` (a delegate answers without a group), `per_row_class`, `ttl_reclaims_sticky`. | Stateright exhaustive on the small topologies the model uses today; counterexamples checked in. | Properties proven, or at the timebox the fallback decided (one subtree per node) or C3 as written started first (D7). |
| **M1.1 Classes at the configuration** | `RowCommit::{Group{config_version}, Final}` replaces `RowClass`/`AckClass`; one lookup of the configuration in force replaces `root_ack_class_for`, `deleg_ack_class`, the under-lock re-class, the published horizon-with-class and `journaled_class`; the fast path carries the version; the lease's and the stream's reconfiguration records commit under old ∪ new uniformly; `status.ack.classes` becomes `{group, final, singleton, archive_waits}`. The configuration lookup still has to answer "singleton or not": the singleton test is `backup_could_be_had` as of the event (landed with the i1-592 fix: a node with a roster peer linked within the budget, stable or measured yet or not, is never lone, and the cached assessment is raised by every event); M2.3 replaces it with the roster's. It is M1.1's singleton test, kept as such. | The five classes; `LoneLead` oracle; the lead as a commit input. | Sims whose seeds fail today under `+commit` on the class decided one step early (`delegated-holder-cut +commit` seeds 124, 228, 501, 763, 1849, 1900; the C2f far seeds): pass with the rule; a knob `AUTHORITY_SIM_PER_ROW_CLASS=1` re-enabling the old decision must fail. | I1 strict (`strict_commit`) on **every** delegated config; new oracle `row_version_matches_config` (every acknowledged row's version is the configuration in force at its position). | Zero I1 on every config ×2,000; `status` and docs updated. |
| **M1.2 One stream per node, no lone delegate, C3 verbatim** | The records of §5.2; `DelegationTable` keyed by node with the folded subtree set; the ownership walk resolves to the node; recall becomes a request answered by `SubtreeReleased`; TTL end for placed grants writes `DelegateNodeEnded`; a node with no group and no sticky grant is forwarded, not delegated; C3 items 1–4 under one stream (verbatim root append, verbatim tail in one transaction, the delegate taking the root re-appends verbatim, the deposed root's member copy and predecessor seal); plan 45's key-move rule removed (one stream owns every link's parent or the op is cross-subtree). | `Delegate`, `Recall`, `RecallCut`, `DelegGroup`; `range`, `place_splits`, the histograms, `deleg_windows.rs`' per-range state, `delegate --range`, `CONSTELLATION_DELEGATION_SPLIT`; `deleg_caps`, `deleg_note_xdeps`, `deleg_covered`, `deleg_xgen_*`; `delegation_stream_rows` as a commit input (kept as the batch size); the lone↔group hysteresis; `deleg_lost_after_ack`; the fold, `MutateOp::Records`, holed generations, the deposed root's rebuild-replay, `Action::RebuildReplica`'s delegation branch; `Delegate.designated` (now `SubtreeGrant.kind`). | `delegated-two-gens-one-backed` (ADR-39's 40 of 200 seeds with the rule off) must pass with `deleg_caps` deleted, re-expressed as `delegated-two-dirs-one-backed` (one node, two subtrees, one stream); `placement-hot` ×2,000 including seed 593 (C3 item 4); `delegated-holder-cut` ×2,000; `long-delegated-late-answers-lr`'s 3 I2 copies. | New oracle `acked_at_or_below_cut` (every acknowledged row of a node generation is at or below its cut); I2c strict on every config; `member_copies_drained`; harness `placement-hot`, `delegate-backup-handoff-failover`, `delegated-group-switch`, new `two-subtrees-one-stream-rename` (a rename between a node's two subtrees under a root crash); `two-gens-ack-cost`, `hash-range-split-merge`, `shared-dir-split*` and bench config `b` retired. | Zero I1/I2/I2c/I5 on every config ×2,000; the harness catalogue green; the "Deleted" table complete. |
| **M1.3 Sticky grants and sticky placement** | §5.3 and §5.4: `SubtreeGrant::Sticky`, placement's sticky proposal for far writers and for the root's own subtrees (never `/`), the long dwell, the handover request; `crates/engine/src/sticky.rs` (idle release, unmount, logind inhibitor, NetworkManager dispatcher, `constellation release`), `repair abandon --node`; the self-fence setting (off); `status.delegation.sticky` (holders, age, last release reason). **Carried from stage 0 (D-1, D-3):** S4 (`DelegCut{req,gen,through}`: the recall's cut is answered before its read recalls; spec in the settle spec) built here with the recall rework, the follow-up S1 (branch `rbe2`), and the cause-6 lease-path buckets A, C, O. | The automatic recall on 30 s windows; the lone root's `Lone` answers for a node with write-eligible peers (its own subtrees now hold grants; the rest waits for the archive). | Sim: `far` and `far-root-writes` (the root with distant peers writing its own subtree) under `strict_durability` with non-singleton rollbacks counted: today's lone rollbacks must be zero after; a knob `AUTHORITY_SIM_TTL_RECLAIMS_STICKY=1` must violate `sticky_ends_only_by_holder_or_operator`. | Model M1.0's properties; sims `sticky-idle-release`, `sticky-handover-ping-pong` (two nodes alternating writes in one subtree with gaps above the grace: the grant changes hands at most once per minimum tenure; a one-off write forwards without a handover), `sticky-holder-dies` (the subtree blocks; `abandon` ends it; the returned holder exports copies), `first-peer-join`; harness `sticky-release-on-unmount`, `sticky-handover-alternating` (the handover count and each writer's latency, measured), `sticky-release-cmd`, `sticky-suspend-inhibitor` (a fake logind on the test bus), `sticky-abandon`; `ack-latency` topology 5 (the far writer's own ops at local speed, final). | Zero non-singleton rollbacks on every config; the carried I4 ceilings (S4, the in-gate residual classes, buckets A, C, O) at zero or re-pinned with the reason; G1/G2 re-measured (the G1 p90 nudge decision re-taken on the new numbers, D9). |
| **M1.4 The hold as a group** | §5.7: `StreamAheadAck`, the hold's answers wait for one member; a one-member epoch's hold is `Final`. | The hold's uncommitted `NonStealable` answers; variant 3(i)'s special case from M0.2. | `flex-crash` and the `flex*` configs under the strict flags D9 deferred (36–81 of 2,000 seeds fail today): zero after. | The flex model's property with the group; `strict_commit`, `strict_identity` on every flex config; harness `epoch-member-dies-with-chunk`, `flex-crash`'s scenario ×3. | The flex strict flags on (D9); the C2h deferral closed; the I4 ceilings of the flex configurations at zero. |
| **M1.5 Docs and gates** | DESIGN §4 "Delegated sub-sequencers", §4 "Acknowledgement and durability", §5.2, §9 "Delegates" and the failover table rewritten for the new records; `delegations.md`, `durability-and-failover.md`, `configuration.md`, `lease-placement.md`; ADR-40 amended with what landed; TESTING.md. | — | — | Full CONVENTIONS gates (pjdfstest 8798/8798); the four-file-system perf gates of §8. | All green; the stage reviewed. |

*M1.1 step 5 notes (from step 4's review, 2026-11-06):* the rows a takeover re-journals (the predecessor's adopted tail,
`apply_backup_tail`; this node's own delegate rows re-appended at the gate, `jobs.rs`) are stamped `Group{Root}` with the successor's
lease version: the configuration of the journal they are rows of, **not** the configuration that committed them (the predecessor's
group, at a version a backup does not record per row). Old ∪ new reconfiguration and the replay must not read these versions as
commit configurations; if step 5 needs them, `BackupTx` has to carry the predecessor's version per row.

*L2b (known gap, deferred 2026-11-04; PROGRESS, "L2b"):* **M1.2 must close L2b.** A committed delegate row that a
delegate-turned-root re-appends in a lone tenure, ending unshipped below the next root's cut, survives only as rescue copies
by rid (I2 in `long-delegated-backup` 13016; I2c in 13907, 14284, 15005, 15996, all under slow lease reads). M1.2's member
copy (C3 items 1–4) keeps the row's verbatim copy until the log carries it. The ignored sim test
`l2b_findings_a_committed_delegate_row_lands_with_its_identity` is the pin: it passes, and loses its `#[ignore]`, in M1.2.

*L3 (landed 2026-11-04; PROGRESS, "L3"):* the root sends an unanswered `DelegRecall` again, with a back-off from a request's
timeout to 2 s, and a delegate answers a repeated recall with the same lock hand-back. No design rule changes (ADR-41's
back-off pattern, ADR-42's gate recalls nothing). The recall's stall under a lost recall or answer went from the grant's
whole window (5 to 11 s) to a few hundred ms. The late ttl-0 refusal (L3 item (c)) is left out: it shortens nothing the root
waits for.

*Pre-M1.2 fixes (oracle I1, 2026-11-02; PROGRESS, "Oracle I1: the delegate's
lone test"):* three delegate-side defects were closed on the current code
because M1.2 is weeks away, and M1.2 removes the code they touch. The
delegate's lone test (`deleg_lone`) is the root's `backup_could_be_had`
(young links count; the lapse rule applies), with the same guard on the
delegate's fast path; a root does not answer its own forwarded op from its
unshipped append of a delegate's row (`completed_row_settled`); a delegate's
stream batch re-adopts the lease of a restarted root that it names. M1.2
deletes `deleg_lone` and the lone delegate with it (a node with no group is
forwarded or holds a sticky grant), and with it the first fix and the
delegate half of the guard; nothing here constrains its design. Class A
(the root acknowledging a delegate's batch through rows its own tenure has
committed only: `DelegateStreamAck.committed`, `DelegateState.root_committed`,
the root's push; [ADR-41](../../../explanation/DECISIONS.md#adr-41-a-delegate-counts-the-roots-copy-only-through-rows-the-roots-tenure-has-committed-interim-rule-for-q16))
landed on 2026-11-02 as an interim form of C3 item 4's member copy: M1.2's
member copy closes the window it covers, and M1.2 may revert the rule (it
deletes `root_committed` with the lone delegate and the per-row classes).

### Stage 2 — no server-side replay (4–6 weeks)

| M | Scope | Deletions | Fail-before | Oracles / sims / scenarios / model | Exit |
|---|---|---|---|---|---|
| **M2.0 Verify coverage** | Re-do §5.5's verification on the tree as it stands after stage 1; land the minimal piece: `abort_durable_parks` resubmits own rids in doubt through `submit`; a core-path own `ClientOp` survives the deposition; `conflict_export` for rolled-back acknowledged rows (singleton; abandon; self-fence). | — | Sim `backup-crash`, `long-backup` with the queue disabled by a knob (`AUTHORITY_SIM_NO_REPLAY_QUEUE=1`): own ops parked at a deposition must still get an answer (today they hang or are lost without the queue); `singleton-dies-peer-joins` (new): the returned singleton's rows appear as copies, byte-exact. | New oracle `every_submitted_op_settles` (liveness: no `ClientOp` and no FUSE request outlives 2 × TTL + the deadline without an outcome); `check_in_doubt_answers`; harness `takeover-loss-two-node`, `holder-kill-rejoin`, new `singleton-return-exports-copies`. | The knob's runs green on every config. |
| **M2.1 Delete the replay** | `core/replay.rs`, `meta/replay.rs`, `derive_replay_op`, the fold's remains, the conditional truncate, `pending_replay`, `Origin::Replay`, `ReplayState`, `mark_unacked_replays`, `QueuedReplay`, `Held` as a replay state, `settled_outcome`'s "a deposed journal will be rolled back" branch, the sim's `Origin::Replay` paths, the model's replay queue (`protocol.rs` `Recovery` becomes undo-only). `strand_*` keep the undo half and delete the rows. | ≈ 4–5k lines. | Every test that pinned a replay path converted to "the state cannot arise" (plan 43 §7.5's rule) or deleted with its mechanism; none weakened. | I2(b) over client entries only; I5 counts singleton/operator/self-fence rollbacks with their exports; `strict_durability` with the sim's `tentative` set equal to the singleton's unshipped rows. | Zero I2/I5 on every config ×2,000; the "Deleted" table. |
| **M2.2 Drop-held as refuse-and-retry** | §5.5's last paragraph: roll back, journal the seed's refusal (conflict copy with holes), requesters retry, own acknowledged dependents exported; the bound stays. | `AwaitRetry`, the drain's stop rule, the park rule (M0.2's), `replay_touches` waits. | `drop-held-chain` ×2,000 (from M0.2) stays green with the placeholder gone. | Harness `poison-record-isolation`, `drop-held-remote-dependents`, `backup-takeover-holds-missing-chunks*`. | Green; `status.held` lists only held rows. |
| **M2.3 `ack=local` removed** | §5.6: `AckPolicy::Local` deleted; `parse_ack_policy` accepts `s3` only; the singleton derived from the roster; `CONSTELLATION_ACK` documented with its one value. | `AckPolicy::Local`, `ack_policy()`'s `Local` fallbacks (the hold's is `Final` now), `lone_ok`, `Ungated`'s lone branch (`DurableWait::Ungated` stays for the singleton and `Final`). | A creation with `--ack-policy local` fails with a clear error (test); the singleton sims unchanged. | Sim `single` and `singleton-first-peer`; harness `single-node-baseline` within ±3 % (hard constraint 3). | Green; docs. |
| **M2.4 Docs and gates** | DESIGN §4 "The replica is a log prefix plus explicit speculation" rewritten (speculation is a requester's shadow and a singleton's unshipped journal, retired by the log, never replayed by the server); §9 "Nobody observes a tentative effect", "Poison records", the failover table; `forwarded-mutations.md` ("Speculation and stranded-op recovery"), `write-path-hygiene.md`; ADR-19 amended, ADR-40 amended. | — | — | Full CONVENTIONS gates; perf gates. | All green; the stage reviewed. |

### Stage 3 — authority state in the log

| M | Scope | Deletions | Fail-before | Oracles / sims / scenarios / model | Exit |
|---|---|---|---|---|---|
| **M3.1 C4** | Plan 43 §5.5 and its coder spec on the new records (§5.8). | The lock mirror and handoff machinery, floors and barriers in messages. | Plan 43 C4's: rows 05, 06, 23, 30, 47's shapes under `strict_locks`. | I3 strict on every lock config; `long_locks` ×1,000; the lock model's `lock_state_in_log`. | Plan 43 C4's exit items. |
| **M3.2 C7b-8** | Barriers and cuts on the wait clock, the root-anchored cut, the echo of the root's send stamp. Also plan 43 §11.2's open findings (D-4, 2026-11-05): a sim configuration that shows what C7b spec commit 6's honour bound fixes (stamp a forwarded op's receipt at the bus's delivery, then an executor-behind configuration with a stalled executor); the cold backup's wall hint (a live holder sealed once per start when the backup's wall is ahead, sealed late when it is behind); plan 41's lock lane stamp rule (any later off-core owner path takes the renewal's `(clock, sent)`, never confirms from the owner's clock). | — | Rows 9/30's stale reads (row 54). | The clock sims; `strict_reads`. | Plan 43 C7b's exit items. |

## 7. Latency and outage behaviour per topology

Figures: LAN RTT 0.2–1 ms, backup budget 5 ms, WAN 60–200 ms, S3
conditional PUT 28–190 ms by distance; measured forwards at topology 5
p50 ≈ 120 ms with 60 ms each way.

| Topology | Today | After stage 1 | After stage 2 | Outage / failure |
|---|---|---|---|---|
| Single node | own ops from disk | unchanged (singleton) | unchanged | a crash loses unsynced rows with their clients (ADR-21); a death followed by a new node's join: the returned singleton's unshipped rows become conflict copies (§5.5) |
| Same AZ, peer in budget | one LAN RTT (root group); delegate G1 ≤ holder + 0.5 ms | unchanged (the root's append or the backup is the group; one stream per node removes one stream wait) | unchanged | failover ≈ 1.5 s harness / 2.3–4.7 s EC2; nothing acknowledged lost; a delegate's crash: its backup's seal or the TTL end, rows past the cut unacknowledged → requesters retry |
| Cross-region, far writer (no peer in budget) | own ops 0 ms under a lone delegation, **rolled back** on root loss; forwards WAN RTT | own ops 0 ms under a sticky grant, **final**; the first op in a new subtree waits for the grant record's archive (28–190 ms, once); forwards into its subtree WAN RTT, final; cross-subtree ops recall by request | unchanged | the far writer dies holding a grant: its subtrees block for others (`EROFS`) until it returns and releases, or `repair abandon` (then its post-cut acknowledged rows return as its conflict copies); the root dies: the sticky holder keeps writing (its answers are final; its stream resumes at the next root, which inherits the grant from the log) |
| Cross-region, the root itself far from every peer | own ops 0 ms (`Lone`, rolled back on a TTL takeover); forwards wait for the archive (C2f) | own ops 0 ms under the root's own sticky grants, final, after one archive wait per new subtree; ops directly in `/` or in others' subtrees wait for the archive; forwards to no grant wait for the archive | unchanged | root death: a TTL claim (60 s + margin) by a peer; the dead root's sticky subtrees block until it returns or `abandon` (today: its lone rows come back as a replay with conflict copies) |
| S3 outage (continuation epoch) | members' forwards answered by the hold at once, uncommitted | + one LAN RTT (one member's `StreamAheadAck`); a one-member epoch's hold final | unchanged | as today (ADR-22); the hold is never deposed except by retire |
| Root death while delegates hold grants | placed grants die with the lease after a TTL takeover; after a fast takeover they re-stream | placed grants: unchanged; sticky grants and designations survive the root (they are log records; the next root honours them) | unchanged | a fast takeover re-streams every node stream from what the log has; verbatim, one stream each |
| Host load | only latency degrades (plan 44, 41) | unchanged | unchanged | a false seal costs a reconfiguration, never a rollback |

Gates kept: G1–G6 as accepted (plan 43 §9/§10); hard constraint 3 within
±3 % on topology 1; `vfs-bench` dispatch < 1 µs. New: the sticky
holder's own ops at the delegate's G1 budget; the first-op archive wait
measured and documented.

## 8. Performance gates (per milestone where §6 says so)

`bench/commit-rule/RESULTS.md` continues: `ack-latency` topologies 1–5
A/B/B/A on the box, on ext4, xfs, btrfs and tmpfs (the maintainer's
no-regression rule across file systems), after M1.2, M1.3, M2.1 and
M3.1; `two-gens-ack-cost` retired and `two-subtrees-one-stream` added;
`placement-hot`'s throughput before and after sticky placement (fewer
reconfigurations expected; a writer that moves waits a dwell for its
subtree to follow — measured and documented). Topology 1 within ±3 % at
every milestone.

## 9. Risks and fallbacks

| Risk | Where | Fallback |
|---|---|---|
| The subtree-set stream cannot keep `recall_safety` with one cut (a released subtree's rows interleave with kept ones so that the root's cursor cannot be split cleanly). | M1.0 | One subtree per node (a node dominating two trees gets one grant); the rest of §5 unchanged. Decided at the timebox (D7). **Decided by M1.0 chunk 4 (2026-11-05): not needed.** The root appends a node stream in order, so it sequences a released directory once its journal shows the `SubtreeReleased` row (past `at`); the kept subtrees' rows below `at` are appended with it (latency, not safety). `node_stream`'s `recall_safety`, `released_subtree_sequenced_after_cut`, `journal_valid`, I1, per-key linearizability and the causal cut hold with released and kept rows interleaved around `at`, a crash and a deposition; acting before appending through `at` is found (knob `root_executes_before_at`). |
| The ownership walk and the recall plan are rewritten; the recall-by-request path is new. | M1.2 | The model first; the fail-before sims above with the xgen machinery deleted; fable review before M1.3. |
| A far writer's product promise becomes conditional on a sticky grant; a holder lost mid-write blocks a subtree for others for as long as it is away. | M1.3 | Automatic grants by placement (D2), the release triggers, `release --wait` in lifecycle hooks, `abandon` documented; the optional self-fence for operators who prefer a bounded block to a final answer. |
| The logind inhibitor's window is shorter than a drain under load. | M1.3 | The shipped drop-in raising `InhibitDelayMaxSec`; a drain that cannot finish releases what it has and the rest is a lost-holder case on resume (its rows stay final and stream on resume; nothing is rolled back). |
| An own op parked at a deposition is lost without the replay queue. | M2.0 | The minimal piece of §5.5 lands and is sim-proven (`every_submitted_op_settles`) *before* M2.1 deletes anything. |
| Deleting records changes the log's wire ordering (postcard appends at the end of the enum): existing logs are unreadable. | M1.2, M3.1 | No reader for the old records (no shims, no version bump, the maintainer's rule); development filesystems are recreated; `fsck` reports an unreadable record as such. Mixed versions are unsupported as always. |
| Verification depends on the sim reaching the new states (the review found 16 rows the sim never reached). | all | Every milestone adds the configuration that reaches its state (named above) and a knob that re-enables the deleted mechanism and must fail; coverage counters (`saw` bits in the model, `status` counters in the sim) are exit items. |

## 10. Migration

No compatibility shims, no version bumps (the maintainer's rule). In
each milestone's commit: the old records (`Delegate`, `Recall`,
`RecallCut`, `DelegGroup`) are removed from `LogRecord` and the new ones
appended; `AckPolicy::Local` removed; `--ack-policy local` and
`delegate --range` refused; the sim and harness fixtures regenerated;
DESIGN.md updated in the same commit (CONVENTIONS rule 5) with the ADR
link. A cluster upgrades with every node stopped; a filesystem created
before M1.2 is recreated (as plan 29 required when partitions went).

## 11. Open questions for the maintainer

All eight answered by the maintainer on 2026-11-01: each as recommended below.

- **Q1 — The flag's spelling.** `fs create --ack-policy s3` as the one
  remaining value (§5.6), or rename the flag to `--ack s3` since it is a
  boolean now? *Decided: keep `--ack-policy s3`.*
- **Q2 — Self-fence defaults when enabled.** `Y` (holder stops) and `X`
  (root may reclaim): *decided* `Y` = 30 s, `X` = 10 × `Y`, both in
  honour-clock terms (ADR-35).
- **Q3 — Sticky grant for the root: proposal threshold.** The root takes
  a grant on the topmost directory it dominates by placement's rule
  (70 %, the ops floor) or on the first write into any subtree? *Decided:
  placement's rule; a first-write rule would grant on every touched
  directory.*
- **Q4 — Handover grace.** How long a sticky holder keeps a subtree after
  another node starts forwarding into it: *decided*: release when idle
  for 5 s, or at once when the holder has no op in flight in that subtree;
  amended (2026-11-01): a handover is requested only for the dominant
  writer (placement's share rule) and not within a 30 s minimum tenure
  after the last change of hands, against ping-pong between alternating
  writers; the three values are settings, measured in M1.3.
- **Q5 — `abandon`'s spelling.** `repair abandon --node` beside
  `repair abandon-group --epoch|--gen`, or fold the three into one
  `repair abandon` with a target. *Decided: fold, in M1.3.*
- **Q6 — The singleton's death-then-join window** (§5.5): accept the
  export-as-copies contract, or make a joining node refuse to claim a
  lease whose holder's last heartbeat is younger than the roster's join
  grace? *Decided: accept; the refusal would need a liveness rule we
  do not want (hard constraint 5).*
- **Q7 — G1 after stage 1.** Re-measure rename/unlink p90 at M1.3; apply
  the saved nudge patch only if the miss remains.
- **Q8 — The flex strict flags' owner.** M1.4 turns them on; if M1.0's
  timebox sends C3-as-written first, do they wait for stage 1 or go on
  with variant 3(i) alone? *Decided: wait.*

### M1.0 answers (the maintainer, 2026-11-05)

The M1.0 work breakdown's eight questions, each answered as recommended:

- **Where the model lives.** `crates/model/src/node_stream.rs` (new,
  untimed, abstract TTL; chunk 1 landed); `delegation.rs` stays for the
  clock margins of placed grants; `hotdir.rs` is deleted with the splits
  in M1.2. The M1.0 row above is corrected accordingly.
- **ADR-41's lone-journal commit is removed.** The root's appended copy
  counts as a delegate's second member only when the root's own
  configuration committed it (its backups hold it, or the archive does);
  a delegate whose root has no group waits for the archive (an S3 round
  trip, recorded as a transient: a delegate within budget of the root
  implies the root has a peer in budget, so the root should have a
  backup). See the ADR draft note below.
- **One commit rule per stream.** `Final` only when every grant of the
  stream is non-stealable; a node with a group commits designated-subtree
  rows by its group too; sticky and placed grants never mix in one stream.
- **A grant is honoured once its `DelegateNode` record is committed** by
  the root's configuration (the archive when the root has no group), for
  placed grants too: the premise ADR-43's no-tail rule needs.
- **Singleton → peer is a journaled configuration change**: the root
  journals a lease `config_version` bump when the roster first shows a
  write-eligible peer and answers nothing new alone until it commits;
  rows before it keep the singleton rule.
- **S4 in the model** is a one-bit read-recall step in the release path
  (chunk C9); the read side stays the sim's.
- **Pipelining.** M1.1 starts once chunks C1–C3 are green; C4–C11
  continue; M1.2 and M1.3 wait for their gates.
- **Timebox (D7):** 2026-11-05 to 2026-11-19.

**ADR draft note (ADR-41 amended; the ADR itself lands with M1.3's code).**
*Decision:* the `u64::MAX` branch of ADR-41 (a lone root's journal counts
as committed for the group `{delegate, root}`) is deleted; the root reports
`DelegateStreamAck.committed` only through rows its own configuration has
committed, and with no backup that is the archive. *Reason:* a lone root
can be taken over at the lease's expiry while paused; the claimant adopts
the archive alone, so a row the delegate answered on the root's journal is
lost when the delegate then fails. The model shows it as harm, not by
definition: with the I1 oracle off, `node_stream`'s knob
`root_journal_counts` violates `acked_never_lost` (root appends and
reports, delegate answers, delegate crashes, node 2 claims the lease from
the archive), and the same topology under the new rule is clean
(`c2_knob_root_journal_counts_loses_an_acked_row`,
`c2_design_t1_lone_root_crash_and_takeover`). *Cost:* such a delegate's
answers wait for the archive until the root has a backup. The text is
written into `DECISIONS.md` with M1.3's code, where the lone root's other
`Lone` answers go too.

### M1.0 answers, chunk C11 (the maintainer, 2026-11-06)

Two decisions the combined topologies forced:

- **Claim = release.** A sticky (or designated) holder that takes the root
  by a lease claim journals the release of its directories (`StreamReleased`)
  before its verbatim re-append and the end of its generation, so the end is
  the holder's own and `sticky_ends_only_by_holder_or_operator` holds. Found
  by X4 (the claim ended a sticky stream with no release of its own); the
  knob `claim_ends_sticky_silently` re-introduces the old behaviour and is
  found. Product gap: the claim's release record is not built (M1.3).
- **Recall, else expire.** The operator's `abandon` of a stream first recalls
  the read delegations in the abandoned directories; a reader that cannot
  be reached is waited out (its grant's expiry) before the root settles
  there, the rule a cut already follows (S4). No read delegation is granted
  on an ended stream. Found by X2r (the root settled while a read delegation
  was granted and unrecalled: `check_reads` treated the ended stream as
  gone); the knob `abandon_skips_read_recall` re-introduces it and is found
  by `settle_after_reads_recalled`. Product gap: `repair abandon --node` and
  its recall, else expiry, are M1.3.

*Model (M1.0 chunk C11, 2026-11-06).* `tests/node_stream.rs` composes the
mechanisms on six topologies (`c11_design_x1` placed everything with history,
causal cut, TTL, crash and deposition; `x1r` replicas and TTL; `x2` sticky and
placed side by side with release and abandon; `x2r` read delegations,
deadlines and abandon on a sticky stream; `x3` roster join/leave, root's own
sticky grant and a deposition; `x4` claim plus sticky): every `always`
property clean, every `sometimes` witness reached, with all knobs off. Every
knob's counterexample length is checked in (`TRACES`). The suite is 254 s
on the shared build box. An ill-formed X3 (a placed grant to a peer while the
roster was the root alone) is kept out: it is not a reachable configuration.
The `lib.rs` doc has the timing table and the node-stream action → code
mapping with the product gaps (M1.1 steps 5 to 7, M1.2 to M1.4, the claim's
release, abandon's recall, else expiry).

- TODO: go/no-go verdict (D7).
- TODO: C11's review (fable) cleared.

## 12. Exit criteria

- [x] Stage 0 (complete 2026-11-05): C2g, C2g′ and C2h's scenarios landed with their
      fail-before evidence; the gate-before-M1 fixes landed and the remaining
      I4 ceilings carried to named stage-1 milestones (D-1, 2026-11-04,
      replacing "I4 strict everywhere": S4 to M1.3, the flex strict flags
      to M1.4, cause-6 buckets A, C, O to M1.3; the in-gate class is closed by ADR-44, 2026-11-05, except its residual classes);
      `flex-crash` zero I2(b) (M1.4); the C2 review's changes landed
      (M0.4, landed 2026-11-05: MUST-1, the shipper guard, the stricter
      classifier, ADR-44).
- [ ] Stage 1: the model's properties proven (or the fallback decided and
      recorded at the timebox); `RowCommit` with two states and no per-row
      class decision site left; one stream per node with the records of
      §5.2 and no range, generation-set or xgen code left; no lone
      delegate; sticky grants with every release trigger of §5.4 and
      `constellation release`; the root's own sticky grants; the hold as a
      group; I1/I2/I2c/I5 strict on every config including flex ×2,000;
      the harness catalogue green with the retired scenarios removed,
      including plan 40's, 41's, 42's (`kernel-race-*`) and 45's
      (`hardlink-burst`, its box gates on four file systems) rows re-run at
      M1.2's and M1.3's exits; G1–G6
      re-measured on four file systems; DESIGN, the feature docs and
      ADR-40 updated; the "Deleted" tables complete.
- [ ] Stage 2: the coverage verification recorded with the minimal piece
      landed and proven; no `Origin::Replay`, no replay queue, no fold, no
      `derive_replay_op`, no `AckPolicy::Local`; conflict copies only from
      `conflict_export`; drop-held as refuse-and-retry; topology 1 within
      ±3 %; I2/I5 strict everywhere; docs and ADR-19/ADR-40 amended.
- [ ] Stage 3: plan 43 C4's and C7b-8's exit items; plan 41's lock
      scenarios re-run at M3.1's exit; plan 41 L5/L6 re-planned on the
      new lock records.
- [ ] The full CONVENTIONS gates at the end of every stage (pjdfstest
      8798/8798, empty baseline).
- [ ] Every milestone reviewed (fable) before the next started.

## 13. PROGRESS.md

One section per milestone, plan 43 §15's style: intro, "What landed",
"Deleted" (per mechanism: introducing commit, the bug it was for, the
test that shows the state cannot arise), "Unchanged on purpose", "Gates"
(dates, kernel, host file systems), and the baseline table of plan 43 C0
(I1/I2/I2c/I4/I5 per config) after each milestone, so the progression to
zero on every configuration is the plan's evidence.

## Sources

- Plan 43 (`docs/plans/v1/done/43-commit-rule.md`), ADR-33, ADR-38,
  ADR-39, the arbiter study (`docs/explanation/ARBITER-STUDY.md`).
- CockroachDB parallel commits (the "never acknowledged before committed"
  TLA+ property): https://www.cockroachlabs.com/blog/parallel-commits/
- TiKV async commit: https://tikv.github.io/sig-transaction/design/async-commit
- Lustre recovery (transno, `last_committed`, the client replay list,
  version-based recovery, commit-on-share):
  https://wiki.lustre.org/Lustre_Recovery_Overview
- CephFS early replies and client replay: `src/mds/Server.cc`
  (`journal_and_reply`, `early_reply`) and the kernel client
  `fs/ceph/mds_client.c`; MDS states (`clientreplay`):
  https://docs.ceph.com/en/latest/cephfs/mds-states/ ; the balancer off
  by default and pins: https://docs.ceph.com/en/latest/cephfs/multimds/
- Raft client sessions and exactly-once (Ongaro, PhD thesis §6.3):
  https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
- NFSv4.1 sessions and the reply cache (RFC 8881 §2.10.6):
  https://www.rfc-editor.org/rfc/rfc8881
- Calvin (asynchronous replication of the sequencer log: "significant
  complexity in failover"):
  http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf
- Aurora (quorum commit, epoch truncation):
  https://www.cs.purdue.edu/homes/csjgwang/CS592DisaggregatedDB/AuroraSIGMOD17.pdf ;
  FoundationDB: https://www.foundationdb.org/files/fdb-paper.pdf ;
  HDFS QJM epoch fencing: https://issues.apache.org/jira/browse/HDFS-3077
- Speculator and external synchrony (an early reply is safe only while
  it has not escaped the party that can be rolled back):
  https://web.eecs.umich.edu/~pmchen/papers/nightingale05.pdf ,
  https://web.eecs.umich.edu/~pmchen/papers/nightingale06.pdf
- Leases need fencing tokens:
  https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
- systemd-logind inhibitor locks (delay inhibitors, `PrepareForSleep`,
  `PrepareForShutdown`, `InhibitDelayMaxSec`):
  https://www.freedesktop.org/wiki/Software/systemd/inhibit/ ;
  NetworkManager dispatcher (`pre-down`): `NetworkManager-dispatcher(8)`.
