# Plan 43 — The commit rule: acknowledged means committed by the configured group

Read `docs/plans/v1/CONVENTIONS.md` first. This plan implements the
recommendation of the arbiter study (`docs/explanation/ARBITER-STUDY.md`
§5, Option B2): **S3 stays the configuration master** (the root lease
and its epoch, the log-slot fence, membership, designations, promises)
and the P2P replica group stays the data path; what changes is the
**commit rule**:

1. **Nothing is acknowledged that the configured replica group has not
   committed.** "Committed" is defined per sequencer (root holder or
   delegate) by a configuration recorded where the reconfigurer reads it
   (the lease object for the root's group, the log for a delegation's
   group), §4.
2. **Nothing committed is ever re-executed.** A committed row is
   re-shipped verbatim, under its original identity (`(epoch, jseq)` for
   a root row, `(gen, idx)` for a delegate row), by whoever reconfigures:
   no fold, no `MutateOp::Records` copy, no replay by rid, no conflict
   copy.
3. **Authority state lives only in the log.** Lock grants, releases,
   expiries and moves, a delegation's group and its stream cut are log
   records; every replica derives its lock table and its delegation cuts
   from the log; no message installs a grant, a floor or a cut.
4. **Stranded unacknowledged ops fail.** An op the group did not commit
   is answered `EIO` (or retried by its own client under the same rid);
   it never becomes a conflict copy and never lands after its client was
   told it failed.
5. **Speculation survives in exactly one documented place**: a lone
   sequencer's own journal (a root holder or delegate with no peer within
   the RTT budget), acknowledged to its own node's clients only — the
   offline-first contract. `ack=s3` and offline designation are the two
   ways to have none.

The study's §6 phased migration is the skeleton (milestones C0–C6),
plus the clock envelope the maintainer added to the scope (C7: safety
on a bounded clock *rate* instead of a ±500 ms cross-node *offset*;
two holes it found in today's code land first as standalone fixes,
C7-H1 and C7-H2). Every milestone leaves `main` correct and
shippable: each tightens one rule while keeping every other mechanism
in place, and each has a simulation or model test that fails before it.

Rules carried into this plan from the maintainer: consistency and
integrity first, then performance, never one for the other (a trade-off
is reported, not shipped); trace the history and the existing integrity
arguments before changing the authority, backup, delegation, replay or
lock code, and extend them in code comments and docs; no
backward-compatibility shims and **no version bumps** (wire messages,
log records, journal rows and on-disk formats change in place); every
fix or behaviour change comes with a test that fails before it (or,
for a new mechanism, against the naive variant it guards against);
under host load (CPU starvation, pauses, slow local disk, slow or
jittery S3 and peer links) no errors, failures or corruption — only
latency may degrade; a change made for one host file system must be
neutral or better on ext4, xfs, btrfs and tmpfs, measured; complex
milestones get an independent (fable) review before the next starts.

**This plan authorizes changes to `docs/explanation/DESIGN.md`**
(CONVENTIONS rule 5): the sections and the substance of the new text
are listed in §6; each milestone edits only the sections it makes true.

## Dependencies

- Plan 30 (all of it: §M3 speculation and replay by rid, §M9 backups,
  seals and `ack=s3`, §M10 continuation epochs, §M11 delegations, §M13
  the inbox, §M14 cluster locks and fencing tokens). Committed.
- Plan 41's L0 integrity fix (`9d00417`, `ee16b06`: fast-tenure
  renewals need fresh S3 liveness) and the F1 fix (`b4fe2e6`: the
  fencing token is checked where the write happens). Committed; this
  plan keeps both rules.
- The host-load campaign tooling (`516f82e`, `cac2d11`, `b0d1086`,
  `a0b251d`, `05ef4c9`). Committed; its ledger and checkers are this
  plan's system-level oracles.
- Coordinates with plan 41 L1–L4 (the renewal lane): C4 changes what a
  lock grant *is* (a log record) but not how a window is extended; the
  lane extends windows from the in-memory table, which C4 derives from
  the log. Whichever lands second rebases; neither blocks the other.
- Coordinates with nothing else in flight. Plans 40 and 42 touch the
  FUSE adapter and the view, not the authority core.

### Paused work: what this plan subsumes, what must land first

Several fixes are paused, waiting on this plan's ordering. Each is
classified below; the recommended order is at the end. The maintainer
decides (§11 Q7).

| Work in flight | What it fixes | Relation to plan 43 | Recommendation |
|---|---|---|---|
| **The takeover-loss fix in review** (latest revision: a folded truncate replays by its own rid as a base-checked conditional cut; `Conflict` and `LockLapsed` refusals journaled as rid outcomes on every sequencer path; `rebase_on_truncate` removed) | The 2026-10-16 acknowledged-write loss (study §2.1): ten acknowledged, fsynced files emptied on both nodes; and the review's H1 (a remote sequencer's unjournaled refusal lets a `Records` copy land over a newer acknowledged write) and H2 (a no-op completion misread as the truncate's effect: false conflict) | **Subsumed structurally by C2 + C3**: C2 makes the delegate's acknowledgement wait for its group (the truncate is never acknowledged on one disk), C3 deletes the fold, the `Records` copies and the deposed root's replay of appended delegate rows. What survives C3: journaling every definitive refusal as the rid's outcome (H1's rule; C3 keeps it as a general rule), and the conditional truncate as the replay shape **inside the lone-sequencer branch** (C5). | **Land first**, before C0. It closes a live loss on `main` (10 of 10 campaign runs before, 0 of 22 after); C2 is weeks away. C3's deletions then remove its fold parts; its tests stay as regression tests (most become "the state cannot arise" assertions, §7.5). |
| **The F2 fix in review** (holder heartbeat on its own thread and liveness lane, own-lag discount, late-beat window, echo through the driver for a starved runtime; F2b, the `ack=s3` holder's beat) | False seals of a live holder under host load (study §2.3) | **Independent** (liveness only; the study: "a false deposition is cheap" under B2, but it still costs a reconfiguration, a quarantine and latency). C1–C3 make a false seal cheap; F2 makes it rare. | **Land first** (with its review's M1–M3), before C0: every campaign in this plan runs under load profiles that false-seal today, which would drown C1's fail-before evidence in unrelated seals. |
| **Seed 9287** of `long-delegated-late-answers` (a client answered `EIO` in doubt for a write whose completion it had applied: a deposed root's journal still carries another delegation's appended row, so `settled_outcome`'s "a deposed journal will be rolled back" rule hides the log's outcome; fix idea `Meta::outcome_from_log`) | A wrong error to the application (in doubt for a landed write) | **Subsumed by C3** (a deposed root keeps another generation's rows as a *member copy*, outside its journal, §5.6: `journal_carries_outcome` can no longer name them). Copies-of-one-op family (two queues hold the row). | **Land first** if its review passes (small; the regression test stays; C3 deletes the `lost && journal_carries_outcome` branch it narrows). |
| **Seed 2945** of the same config (a marker visible without its data: a deposed root replayed a forwarded marker by rid with its own session's deps, so the new root logged it ahead of the delegate rows it depended on; fix idea: stranded replays carry the per-generation deps of the journal rows ahead of them) | Cross-subtree causal order (a reader sees a marker without its data) — an ordering corruption | **Subsumed by C1 + C3**: a deposed group tenure replays nothing by rid (C1, the marker was never acknowledged; its requester retries the same rid with its own, correct deps); the re-execution path that lost the deps no longer exists. Same family: re-execution by rid of a copy. | **Land first** if its review passes (it is an ordering corruption on `main` today); C3 deletes the `QueuedReplay::deps` field it adds together with the deposed-root replay. |
| The open design question with them: *should a deposed root drop other delegations' rows from its journal instead of rebuilding?* | — | **Answered by C3** (§5.6): neither drop nor rebuild-and-replay. A deposed root **moves** another generation's appended rows out of its journal into that generation's member-copy store (the delegate-backup tail keyspace that exists today), serves them verbatim to the next root that seals the generation, and discards them once the log carries them. Dropping them is unsafe until C2 (before C2 a delegate without a backup acknowledges on its own disk, and the root's copy may be the only other one). | Do **not** implement it in the interim seed fixes. The simulation's `RebuildReplica` stub (it answers `ok: false`, so a deposed root in the sim never recovers) is fixed in C0. |
| The ledger's late landing (`05ef4c9`: an op answered in doubt may land once, late, after the application's later ops on the name; "a late in-doubt rename or unlink can remove a later file", by design today) | — | **Ended by C2** (invariant I4: an `EIO` is answered only after the rid is settled — `Refused { rid, Abandoned }` committed — so no copy of it executes afterwards). C2 removes the ledger's late-landing allowance (`Ledger::with_client_deadline`'s `late` set) for every op not answered by a lone sequencer's branch. | Keep the allowance until C2; C2's exit removes it. |
| **C7-H1, C7-H2** (found by the clock-envelope research, §7 C7): every honour-side duration reads the wall clock, so a backward step extends an honoured window; `note_marker_landed`'s `min(prev_expires, bound)` gives a successor whose clock is ahead no quarantine | Two safety holes under clock steps and offsets beyond the margin | **Independent** of the commit rule; C7 builds on both | **Land first**, each with its fail-before sim config: C7-H2 is a two-line change with a bounded latency cost (a maintainer decision, §11 Q10a); C7-H1 is not small. It is the step-safe half of C7a: `ProtocolClock`, three readings per node and per core step, every local stamp and comparison on the honour or the wait clock, the lease body's and the beat's honour fields, the sim's clock model and `ClockStep`. It lands as four commits, with its own reviews (§7 C7, §11 Q19) |
| The host-load review's F1–F11 | | F1: landed (`b4fe2e6`); C4 keeps its rule (the minter judges at its serial apply). F2/F2b: the paused F2 fix above. F3 (a `flush` hang behind a recalled grant renewed forever): independent (under bisect). F4 (successor quarantine bound in `delegate-backup-handoff-failover`): independent harness fix; the "separate marks" refinement is independent. F5 (one lease renewal series at half TTL): independent, but **C1's TTL-path tail fetch makes F5's consequence cheaper** (a TTL successor now carries the group's committed tail). F6, F7: independent (documented contracts). F8 (stale `now` audit): C4 adds lock records and must follow F8's rule (decide at the serialization point). F9 (fast-path rows journaled after the lease ran out): **subsumed by C1 + C2** for every group tenure (the row is not acknowledged until the group holds it, and a sealed group refuses it); remains, documented, for the lone branch (C5). F10: keep the tests. F11: independent. | — |

**Recommended landing order:** the F2 fix → the takeover-loss fix →
seeds 9287 and 2945 → C7-H2 → **C7-F (F-a, F-b, F-c: the floor's cost taken
back, decided 2026-10-18)** → C7-H1 (the honour and wait clocks: C7a's step-safe half,
reviewed on its own, §7 C7) → C0 → the rest of C7a → C1 → C2 → C3 → C4
→ C7b → C5 → (C6, decision). Dependencies:
- C1 needs C0 (its fail-before evidence is C0's oracles).
- C2 needs C1 (a delegate's group commit rests on the root group's).
- **C3 (verbatim re-ship, the deletions) needs C1 and C2**: re-execution
  may be deleted only once every acknowledged row is committed; it also
  uses `Recall.through`, which C2 lands (§5.3 item 7).
- **C4 (lock and delegation state in the log) needs C3.** Until C3 a
  deposed root still queues its appended rows as `MutateOp::Records`
  replays and a delegate-turned-root replays its own rows; both dedup
  only by rid. A lock record is rid-less, so a `Records` copy of a
  `LockGrant` or `LockMove` row would always apply — a grant revived
  after its `LockExpire`, live grants moved to a dead generation — while
  I3 is satisfied (every replica folds the same, wrong log). Landing C4
  after C3 means no lock record ever meets a re-execution path (the
  review's H4; the alternative, filtering lock records out of every
  re-execution path until C3, is rejected as a second mechanism to
  delete later).
- C5 needs C4. C7a needs C0 (it reuses C0's sim plumbing). C7b needs C1
  (the claim path) and C4 (the lock barrier is a field of `LockExpire`).
- Every milestone that knowingly costs latency starts only after the
  maintainer accepted its measured budget (§10's gate list).

## 1. History

The mechanisms this plan changes, and why each exists (verified with
`git log -p` at `05ef4c9`).

| When | Commit | What |
|---|---|---|
| 2026-08-26 | `1450756` | M3.1 lease authority; the log-slot CAS as fence ("FENCING VIOLATION ... skipping" for a lower-epoch segment). |
| 2026-09-23 | `01e4f93` (plan 30 M3a) | The speculation log (`spec`, `spec_live`): requester shadows and `Exists` hints with before-images, stranding by a higher epoch, replay by rid. Fixed bug B (phantom effects of a dead holder). |
| 2026-09-23 | `5e18214` (M3b) | `SpecKind::Local` holder capture, `journal_tx`, deposed-holder rollback and replay by rid, the epoch marker, `MutateOp::Records` (replay of an op-less row), and **the fold** (`folded_into_later_manifest`, `crates/authority/src/core/replay.rs:75-99`: a size-only truncate replayed alone has no base check and would cut a newer write). |
| 2026-09-24 | `0e1017b` (M9) | `AckPolicy`, the lease's `backups`/`config_version`/`ack_policy`, `BackupAppend`/`BackupAck`, the seal, `apply_backup_tail`, `ack=s3` fast takeover, `StreamAhead` and `SpecKind::Streamed`, `durable_jseq`/`ack_need`, journaled `Refused`, the sim's `strict_durability`. |
| 2026-09-25 | `2522402` | Nine M9 failure classes: "wait while a backup could be had" (`eligible`/`unassessed`), `TailFollows`, own next-epoch tail re-ship, `backup_claim_grace_ms`, the sim's double-fault `budget_exceeded`. |
| 2026-09-25 | `1641850`, `5384772` | The fold restored in the takeover gate after `617499b` emptied a file; `truncate_base` and `rebase_on_truncate` (every truncate-then-write had become a conflict copy). |
| 2026-09-25 | `3c58839` (M16) | Seals fsynced, backup appends buffered (the single-failure contract), ack policy per filesystem. |
| 2026-09-25 | `381c66d`, `379a00d`, `216ce6c` | Lock hand-over with a delegation that survives an overtaking recall (`handoff`, `handed`, `moved_seen`, `install_if_consistent`); grace and floors carried to the delegate. |
| 2026-09-25 | `df14686` (M14) | Cluster locks; the asynchronous `LockMirror` to backups ("an availability aid; the grace is what keeps locks exclusive"). |
| 2026-09-26 | `5face1b` | A backup-tail transaction carries its delegation origin; a takeover strands the successor's own delegate rows and replays them **by rid after the tail**; `deps_lost` → `Held`. |
| 2026-10-04 | `f57db3c` | Fencing tokens; the restart horizon off the core. |
| 2026-10-06 | `4c5d184` | Grants on a file unlinked in a delegated subtree travel with the stream batch (`DelegateStream.leaving`); `LateRenew`. |
| 2026-10-07 | `96f4565` | A generation a takeover strand holed (its rows replayed as root rows) is marked `deleg_stranded` and ended, so a restarted delegate stops re-sending a batch forever (seeds 2477, 2982). |
| 2026-10-07 | `c465b07`, `9d82ad0`, `fb0c909` | `unlinked-exclusion-5681` (an outwaited generation's undelivered grants revived, restamped), `unlinked-exclusion-7455` (a recall answer after its generation ended put lapsed grants back): `pending_returns` removed, `lock_install_returned`'s late-answer rules. |
| 2026-10-07 | `304c30c` | `stale-read-outwaited`: barriers, cuts and stream-head stamps carried in renewals and recall answers. |
| 2026-10-07 | `d7d2eb8` | One-way delegate stream and renewal messages with cumulative acks. |
| 2026-10-08 | `17ee59c` | `lease-cap-fenced`: four nested margins shrank delegated grants to zero. |
| 2026-10-15 | `9d00417`, `ee16b06` | Plan 41 L0: fast-tenure renewals need fresh S3 liveness; a renewal is never answered with a weaker record. |
| 2026-10-16 | `b4fe2e6` | F1: the fencing token checked inside the transaction; the fast path re-checks its lease or delegation there too. |
| 2026-10-16 | `516f82e` … `05ef4c9` | The host-load campaign tooling; the ledger; `05ef4c9` made the ledger accept an in-doubt op's late landing (by design today). |
| 2026-10-16 | `7f79ba9` | The arbiter study. |

The pattern the study names (§2.12): every one of the 2026-10-01..16
data-loss and ordering bugs on the authority path is an
acknowledgement given before agreement, a second copy of one op in
another queue, or authority state that moved in a message. Each fix
above patched one interleaving; this plan removes the states.

## 2. What the code does today

Line numbers at `05ef4c9`.

### 2.1 The acknowledgement gate (root holder)

- `AckPolicy { Local, Backup, S3 }` (`crates/store-s3/src/lease.rs:153-170`);
  the lease object carries `backups`, `config_version`, `ack_policy`
  (`:119-134`); `Lease::reconfigured` bumps `config_version`
  (`:221-231`). A filesystem chooses `local` or `s3` only
  (`parse_ack_policy`, `crates/engine/src/authority_driver.rs:726-735`);
  `Backup` is entered automatically when a peer is within
  `backup_rtt_budget_ms` (5, `CONSTELLATION_BACKUP_RTT_BUDGET_MS`),
  `backups_max` 1 (`CONSTELLATION_BACKUPS`). A new tenure starts with
  `backups: []` and `Local` (`crates/authority/src/core/lease.rs:396-424`).
- Selection and reconfiguration: `backup_candidates`, `backup_select`,
  `backup_timeouts`, `drop_backup`, `backup_promote`, `issue_reconfig`
  (a `LeaseSwap` CAS on the object version), `removal_safe`,
  `on_reconfig_put`/`on_reconfig_reread` (`crates/authority/src/core/backup.rs:1026-1528`).
- **The gate**: `durable_jseq()` (`backup.rs:487-536`) — `u64::MAX`
  (no gate) under an epoch hold; `shipped_through` under `S3`; under
  `Local`/`Backup` with no committed backup, `shipped_through` while a
  backup *could* be had (a candidate, `eligible`, or unassessed), else
  `u64::MAX`; otherwise the minimum of the committed backups' `acked`,
  at least `shipped_through`. `ack_need(&Position)` (`:556-562`).
  Parked answers are released by `complete_ready`
  (`crates/authority/src/core/readindex.rs:1410`).
- **Where an OK is produced**: a forwarded op — `holder_execute`
  (`crates/authority/src/core/holder.rs:623-734`), then `ack_need`, then
  `park_reply` or an immediate `MutateReply` (`holder.rs:285-405`); the
  holder's own op through the core — `execute_local`
  (`crates/authority/src/core/client.rs:1781-1897`), `park_finish`;
  the requester — `on_mutate_reply` installs a **shadow** and answers
  (`client.rs:1043-1094`) or waits for the log (`AwaitingLog`,
  `:987-1042`).
- **The FUSE fast path writes first and waits after**: `root_fast_path`
  (`crates/engine/src/view/write_gate.rs:422`) → `lease.admit()` →
  `execute_admitted` (journal commit) → if `ack_gated`,
  `ack_when_durable` → `session.wait_durable(jseq, 30 s)`
  (`write_gate.rs:453-536`; `crates/meta/src/session.rs:1286-1308`).
  `DurableWait::Ungated` (the gate went off while the op waited)
  answers OK **on the local disk alone**. The module doc of
  `backup.rs:27-36` says the fast path is closed under a non-`Local`
  policy; that is stale since `0e1017b` (C1 corrects it).
- The holder's reads wait for durability of unshipped rows they would
  observe (`Meta::durability_pending`, `session.rs:1522`): DESIGN §9
  "Nobody observes a tentative effect".

### 2.2 What a deposition or takeover does to acknowledged work

- **Seal path** (`on_backup_watch` → `on_takeover_get` → `backup_seal`
  (fsynced) → `Acquire` → marker → `complete_gate`,
  `backup.rs:2154-2350`, `crates/authority/src/core/jobs.rs:1179-1265`).
  `apply_backup_tail` (`backup.rs:2364-2428`) **re-applies the tail's
  records** through `apply_records_journaled_inner(adopted = true)`
  (`crates/meta/src/store/local.rs:840-967`, skip-on-conflict), journals
  them as **new rows** (new jseqs, the successor's epoch) with the
  original rid, and marks the marker `TailFollows { prev_epoch }`
  (`crates/meta/src/record.rs:245-253`; `jobs.rs:4242-4247`).
- **Subscribers' pre-S3 copies are rolled back at a seal**: the
  successor's marker is a higher-epoch segment, so it strands every
  requester's `Shadow` and every subscriber's `Streamed` entry of the
  predecessor (`crates/meta/src/store/spec.rs:264-270`, `:2711-2729`);
  they are rolled back from before-images and queued for replay by rid,
  and the re-shipped tail completes them again moments later.
  `backup_no_acked_op_lost` (`crates/authority/tests/sim.rs:1500-1510`)
  documents these *transient rollbacks of acknowledged ops* as expected.
- **A deposed holder** (`deposed`, `jobs.rs:1334-1355`;
  `recover_deposed`, `:1381-1410`): `strand_below_epoch` rolls back its
  own `Local` rows and queues **every** one for replay by rid —
  including rows the sealing backup holds and re-ships, and rows
  acknowledged under `Backup`. Rows of other generations it appended
  (`Local { gen != 0 }`) are never stranded by an epoch
  (`stranded_by`, `spec.rs:264-269`), so `journal_len() > 0` →
  `Action::RebuildReplica` → `reset_for_rebuilt_ns_tx`
  (`spec.rs:2016-2052`) queues them as `MutateOp::Records` replays (no
  base check, no token): the second copy in the 2026-10-16 loss.
- **A TTL takeover of a `Backup` lease** by a non-listed node (after
  `expires + backup_claim_grace_ms`, `lease.rs:345-372`, "a double
  fault: the tail is lost") obtains **no tail from anyone**
  (`backup_tail_epoch` needs `bk.role` to match, `jobs.rs:2873-2884`).
  The group's committed-but-unshipped rows survive only as the deposed
  holder's replays and the requesters' shadows: re-executed against new
  state, refused into conflict copies. The sim counts it as
  `budget_exceeded`. A listed backup claiming at expiry never seals (it
  still re-ships its own tail: `backup_tail_epoch` matches its role).
- **The replay queue** (`pending_replay`, `QueuedReplay`,
  `spec.rs:294-341`) and its executors: the drain (`on_drain_tick`,
  `replay.rs:107-243`, every 250 ms) and the gate's
  `replay_queue_locally` (`replay.rs:516-633`, which journals **no
  refusal** at `05ef4c9`). What is queued as what: a `Shadow` and an own
  `Streamed` as the original op, a foreign `Streamed` as `Records`
  copies of another node's committed rows, a `Local` row as its op or
  `derive_replay_op` (`spec.rs:913-967`; a lone `WriteManifest` as
  `SetManifest`, anything else `Records`).
- **Conflict copies**: `refuse_replay` → `try_copy` →
  `Action::ConflictCopy` → `recovery::materialize_remote`
  (`replay.rs:322-436`; `crates/engine/src/recovery.rs:316-385`), named
  `{name}@{node}-{ts}-{rid_seq}` under `.constellation-conflict`
  (`crates/meta/src/reintegrate.rs`). Not for unacked or foreign rows.
  Other sources: a close refused for a lapsed grant
  (`View::keep_refused_commit`), `repair drop-held`.

### 2.3 Delegations

- A delegate executes (`delegate_execute_now`,
  `crates/authority/src/core/delegate.rs:884-1092`), journals the row
  as `Local { gen }` speculation with origin `(gen, idx)` (`idx` from
  the node-local `deleg_idx:{gen}`), and acknowledges when
  `deleg_stream_durable` (`:1100-1143`): under `ack=s3` once a segment
  carries it; with a backup once the backup holds it; **with no backup,
  at once on its own disk**. Its FUSE fast path is closed only while it
  has a backup (`deleg_fast_path_gated`, `:411-413`).
- The delegate's backup is chosen **by the delegate alone**
  (`deleg_backup_select`, `:1148-1187`: within budget, never the root),
  reported to the root in `DelegRenew.backup`; it is in no
  configuration the root or a successor root reads before
  reconfiguring. In a two-node cluster a delegate has no backup (its
  only peer is the root): Layer A — the 2026-10-16 shape.
- The root appends stream rows in order (`on_delegate_stream`,
  `:3301-3439`; `Meta::apply_delegate_tx`, `local.rs:490-520`) as root
  journal rows with origin `(gen, idx)` and the original rid; acks are
  cumulative `DelegateStreamAck { through }`.
- Generation end (`end_generation`, `:3164-3238`): `Recall { dir, gen }`
  is journaled; the **cut is not in the record** — every replica infers
  it from what it holds of the stream (`void_stream(gen,
  stream_applied(gen))`, replay at `crates/meta/src/replay.rs:782-826`;
  `void_cuts`, `deleg_log_idx:{gen}` in node-local kv).
- A recalled or outwaited delegate's unappended rows are stranded by the
  `Recall` and replayed by rid; a delegate that takes the root over
  (seal path) strands its own unappended rows and replays them **as
  root rows** (`strand_for_takeover`, `spec.rs:3181-3215`; the fold
  applies there), leaving a hole in its generation (`96f4565`'s mark).
- Placement (`crates/authority/src/core/placement.rs`) has no RTT
  budget and does not consider whether the delegate will have a backup.

### 2.4 Locks

All lock authority state is in memory (`LockTables`,
`crates/meta/src/locks.rs:779-816`; `LockState`,
`crates/authority/src/core/locks.rs:163-416`); **no `LogRecord` variant
carries lock state** (`record.rs:37-305`). The only durable lock write
is the restart horizon (`KV_LOCK_GRANT_HORIZON`,
`crates/meta/src/readdeleg.rs:515-559`, synced, off the core). State
moves in messages: `LockMirror` to backups (installed restamped and
`UNCONFIRMED` at a fast takeover, `lock_install_mirror`,
`locks.rs:4235`); the handoff with a delegation in `DelegRenewed.{locks,
lock_grace_ms, lock_floor, lock_cut_at, lock_cut, lock_barrier}`
(`handoff`, `handed`, `moved_seen`, `lock_install_moved`); recall answers
`DelegRecalled.locks: LockHandback { grants, floor, barrier }`
(`lock_install_returned`, `:4660-4733`, with the late-answer rules);
leaving grants in `DelegateStream.{leaving, leaving_barriers}`; stream
heads in `DelegRenew.{stream_head, stream_head_at}`. Restamping
(`restamp(now) = now + ttl + margin`) at five install sites.

### 2.5 Continuation epochs and designations

- An epoch hold acknowledges on the hold owner's disk alone
  (`durable_jseq` returns `u64::MAX` under `epoch_held()`); the hold
  owner streams its whole journal ahead to members (`stream_ahead_soon`
  to the journal tip), which install it as speculation. "A deposed hold
  owner recovers like a deposed holder" (`promise.rs:1101-1110`).
- A designation (`Delegate { designated: true }`) is never reclaimed;
  its designee acknowledges on its own disk.

### 2.6 The sim and the models today

- The sim's end-of-run checks (`crates/authority/tests/sim/run.rs:2603-3077`)
  include `strict_durability` (only for `backup_config`, `long_backup`,
  `ack_s3` and their derivatives: `acked_rolled_back` is **reported,
  not failed**; `observed_tentative > 0` fails), linearizability with
  tentative exemptions, `check_exactly_once` (acked ⇒ in the log once,
  but only on converged runs), `check_in_doubt_answers`. No delegated,
  flex or lock config is strict. **Nothing checks an acknowledgement
  at the moment it is given**, against the members of the configuration
  then in force.
- The sim's `RebuildReplica` answers `ok: false` ("never asked"), so a
  deposed root with appended delegate rows never finishes its recovery
  in the sim (found with seeds 9287 and 2945).
- `crates/model/src/backup.rs` has `acked_never_lost`, which counts a
  live deposed holder's stranded replay set as "not lost" — i.e. it
  accepts re-execution. `crates/model/src/locks.rs` models message-borne
  handoffs, mirrors and reinstatement (knobs `replicate`, `move_state`,
  `reinstate_handed`, `resend_handoff`, `handoff_grace`).

## 3. Terminology (Delos / Vertical Paxos, mapped to the code)

The study's vocabulary (§4, from Delos, OSDI'20, and Vertical Paxos /
PacificA). C0 adds it to DESIGN §4; code comments and docs use it from
then on. No type is renamed for the sake of the vocabulary.

| Term | Meaning here | Code |
|---|---|---|
| **Configuration master** (Delos: MetaStore) | The versioned register that decides who may order writes and with which group. For the root: the lease object in S3 (`leases/p0.json`: holder, epoch, `backups`, `config_version`, `ack_policy`), plus `nodes/` and `designations/`. For a delegation: the root's log (`Delegate`, the new `DelegGroup`, `Recall`) — itself fenced by the root's lease. | `Lease`, `LeaseState::classify`, `issue_reconfig`; `DelegationTable` |
| **Configuration** | One version of it: `(epoch, config_version, holder, backups, policy)` for the root; `(gen, group_version, delegate, backups)` for a delegation. | `Lease`; new `DelegGroup` record (§5.3) |
| **Group** (Delos: loglet) | The nodes that hold a sequencer's rows for one configuration: root group = holder + listed backups; delegation group = delegate + its recorded backups, or delegate + root when it records none (§5.4). Write-all, read-any. | `durable_jseq`, `deleg_stream_durable` |
| **Commit** | A row is committed when every member of the configuration in force when it was journaled holds it (holder journal and every listed backup's `backup_tail`; delegate journal and every recorded member's copy), or when an archive slot carrying it landed. | §4 I1 |
| **Seal** (Delos: seal) | A member's persisted, synced refusal of any further row of a configuration; it fences the old sequencer before a reconfiguration. Today: `backup_seal` (root), `DelegSeal` (delegation). C1 adds a seal by a holder of its own epoch on request (`SealTail`, §5.3). | `Meta::backup_seal`, `PeerMsg::DelegSeal` |
| **Sealed tail** (Delos: `checkTail` + the sealed suffix) | The rows a sealed member holds past the archive; re-shipped verbatim by the reconfigurer. | `backup_tail` keyspace; `apply_backup_tail` (re-shipped verbatim from C3) |
| **Archive** | The S3 log (`log/p0/<seq>`, slot CAS `If-None-Match`) and the commit chain: where committed rows end up, for followers, fresh nodes and P2P-less requesters. A second fence (a deposed sequencer's late segment collides). | `LogStore::put_segment`, `apply_incoming` fencing |
| **Lone sequencer** | A root holder or delegate whose group is itself because no peer is within the RTT budget (for a delegate: and the root is beyond it too). Its acknowledgements to its own node's clients are **Layer A**: the one speculation place (§5.7). | new `Core::lone(gen)` (C2) |
| **Non-stealable authority** | An offline designation, and a continuation epoch's hold (§11 Q3, decided): no successor can be installed while the holder lives except by its own handoff or an operator's retire. Its acknowledgements are final, not speculation. | `Delegate { designated: true }`; `promise.rs` hold |
| **Member copy** | Rows a node holds as a member of a group it no longer sequences for (a deposed holder's committed tail; a deposed root's appended rows of a live generation), kept outside its journal until the archive carries them, served to a reconfigurer. New in C1/C3 (§5.6). | the `backup_tail` keyspace, keyed by `(epoch, first)` or `DELEG_BACKUP_BASE + gen` |

## 4. The invariants

Stated so that the simulation checks them at the moment they could be
violated and the models check them as properties. Each has a name used
in the code, in PROGRESS and in failure messages.

- **I1 `acked_implies_committed`.** Every *definitive* answer a client
  receives for rid `r` — `OK`, or a refusal other than the in-doubt
  `EIO` — names, at the moment it is sent, a transaction `T` carrying
  `Completed { r }` or `Refused { r, .. }` that satisfies one of:
  - **Archive**: `T`'s records are in an applied, unfenced log segment;
  - **RootGroup**: `T` was journaled by the holder of epoch `e` under
    configuration version `v`, and the holder's journal and the
    `backup_tail` of every backup listed in the lease object at version
    `v` hold `T`;
  - **DelegGroup**: `T` is row `(g, i)` of generation `g`; the
    delegate's journal holds it, and so does every member of the
    generation's group in force when it was journaled — each recorded
    backup's delegate tail, or, when none is recorded, the root's
    journal (or the archive) as an appended row with origin `(g, i)`;
  - **NonStealable**: the answering sequencer is a designee for the
    keys, or an epoch's hold owner (§11 Q3);
  - **Lone**: the answering sequencer is the client's own node, and at
    the answer it is a lone sequencer (§3). Counted.
  An answer that satisfies none is a violation. The class is reported
  per answer (`AckClass`), so the oracle verifies the claim against the
  other nodes' stores, not against the answerer's word. I1 is judged
  **at the answering sequencer's emission** of the answer (its
  `MutateReply` send, or its local finish), reading the stores at that
  instant, and RootGroup **by the lease object in the bucket** at that
  instant (the configuration a reconfigurer reads). With two or more
  backups this needs the gate to count a candidate **from the moment its
  add-CAS is issued** (today it counts only after `on_reconfig_put`,
  `backup.rs:1443`; with `backups_max = 1` the old set is empty during an
  add and the bring-up rule waits for the archive, so there is no window;
  with `CONSTELLATION_BACKUPS=2` there is one, and `permit_allows` lets
  the new backup seal and claim on that object without the rows the old
  set acknowledged) — C6 fixes it, with its fail-before.
- **I2 `one_identity_per_op`.** (a) No rid completes more than once in
  the log (today's `check_exactly_once`). (b) At no instant is a rid
  queued for execution in two places: the union of every node's replay
  queue and every in-flight client submission holds each rid at most
  once (the client's own same-rid retry and its node's queue entry for
  the same op merge into one, as `also_client` does today; an `Abandon`
  replaces the rid's client entry, it does not add one). At C1 (b) is
  checked for gated rids only: a lone root's answer to a remote forward
  legitimately has two copies (the deposed lone root's queue and the
  requester's retry) until C2 makes it Archive-class. (c)
  **Verbatim**: every committed `T` appears in the archive exactly once,
  with the records it was committed with, under its original identity
  — the segment's origin for each row names `(epoch, jseq)` of the
  journal that first held it (carried transitively: a row re-shipped
  twice, A → B → C, keeps A's identity), or `(g, i)`. No committed row is ever
  executed again by rid (no `Records` copy, no fold, no replay).
  Evidence for (b): `load-probe-takeover` seed 46 (a log-sink stall)
  decided one rid twice, `Completed` then `Refused`, in one segment: the
  deposed node's fast-path write was a live client op (resubmitted in
  doubt) and its own queue entry at once, and the two were not merged. The
  in-doubt mark (`Event::SubmitInDoubt`) keeps the second copy from
  deciding until C1/C3 remove the deposed tenure's replay.
- **I3 `authority_state_in_log`.** Every lock grant, release, expiry
  and move, and every delegation start, group change, end and cut, is a
  log record applied in log order by every replica. At every step, each
  replica's derived lock table (grant ids, mode, holder, inode,
  generation, floor) equals the fold of its applied log plus its own
  journal (or, for a lock owner, of its own journal plus what it
  committed). No message installs a grant, a floor, a barrier or a cut;
  windows (`until`) remain per-replica, clock-bounded (§7.6 of the
  study, unchanged).
- **I4 `unacked_fails_never_lands_late`.** A client is answered `EIO`
  for rid `r` (in doubt) only after `r` is *settled*: `Completed { r }`
  or `Refused { r, .. }` is committed somewhere (then the client gets
  that outcome instead) or `Refused { r, Abandoned }` is committed by
  the sequencer now in force (then `EIO`). The one exception is an
  *isolated requester* (no sequencer, inbox or lease path answered for
  `2 × acquire_deadline_ms`), counted as `client.unsettled_eio`. For
  every settled `EIO`, no execution of `r` exists anywhere afterwards;
  for an unsettled one, any later execution of `r` is ordered in the log
  before every op its node's clients invoked after the answer.
- **I5 `speculation_only_lone`.** An effect a client was told succeeded
  is rolled back only if its answer was class **Lone**; a conflict copy
  is created only from (a) a replay of the lone branch, (b) an operator
  action (`repair drop-held`, the new `repair abandon-group`, §11 Q1), or
  (c) a fenced writer's refused close (cluster-locks: a close refused for
  a lapsed grant keeps its content as a copy; nothing was acknowledged).
  The sim's `tentative` set is a subset of the Lone-class answers.

What does not change (the study's "keep" list): request ids and
`Completed`/`Refused` records; the fencing token at the write and the
owner fence; the takeover gate and the epoch marker; the slot CAS; the
inbox; continuation epochs and promises; offline designations; the
time discipline of derived grants (`sent + ttl − margin` /
`granted + ttl + margin`) — C7 revisits only its clock assumption; the
chunk store; E2E (every new log record is inside the encrypted segment;
nothing new is plaintext in the bucket); the sans-IO core, the sim and
the models.

## 5. Design

### 5.1 The commit point per sequencer, and the acknowledgement classes

One function decides, for a row about to be journaled, what its answer
will wait for: `Core::ack_class(origin, gen, requester_is_remote) ->
AckClass` (new, `crates/authority/src/core/backup.rs`). The class is
decided **at journal time** and recorded with the row (§5.2 item 1): it
is what tells a deposed sequencer whether a row is committed-or-pending
(member copy) or its own lone speculation (replay queue). With

```rust
pub enum AckClass {
    /// Wait for the root group (today's `durable_jseq` under `Backup`).
    RootGroup,
    /// Wait for the archive (today's `S3`; also a lone root's answer to
    /// another node, C2).
    Archive,
    /// Wait for the delegation's group (its recorded backups; or the
    /// root's append when it records none), C2.
    DelegGroup { gen: u64 },
    /// No wait: a designation or an epoch hold (§11 Q3).
    NonStealable,
    /// No wait: a lone sequencer answering its own node's client
    /// (Layer A). Counted.
    Lone,
}
```

| Sequencer state | Own node's client (FUSE, local ops) | Another node's forward |
|---|---|---|
| Root, `ack=s3` | Archive | Archive |
| Root with ≥ 1 committed backup | RootGroup | RootGroup |
| Root with none but one could be had (candidate, eligible, unassessed) | Archive (today's bring-up rule) | Archive |
| Root, lone (no peer in budget) | **Lone** | **Archive** (C2; today: Layer A with a requester shadow) |
| Delegate with a recorded backup | DelegGroup | DelegGroup |
| Delegate, no backup recorded, root in budget | DelegGroup (the root is the second member: wait for `DelegateStreamAck` through the row, C2) | DelegGroup |
| Delegate, lone (no backup, root beyond budget) | **Lone** | DelegGroup (the group `{delegate, root}`: wait for the root's journaled append, `DelegateStreamAck.through ≥ i`; §11 Q16) |
| Designee | NonStealable | NonStealable |
| Epoch hold owner | NonStealable (Q3, decided: non-stealable except by an operator's retire) | NonStealable |

`AckClass` is carried with every answer inside the core (new field on
`ParkedWhat::Reply`/`Finish` and on the immediate reply path) so the
sim can record it, and is counted per class in `status.ack.classes`.

### 5.2 C1: a backup-acknowledged row is committed

Today a row acknowledged under `Backup` is committed in fact
(durability-and-failover.md "Why it is safe") but is still treated as
speculation by three parties: the deposed holder (replays it by rid),
requesters (their shadows strand at the successor's marker and replay
by rid), subscribers (their `Streamed` entries strand and roll back).
C1 makes every party believe the commit:

1. **Journal rows record their class.** `JournalTx` gains
   `gated: bool` (`crates/meta/src/store/local.rs:110-155`), set from
   the row's `AckClass` decided at journal time: `gated := class ∈
   {RootGroup, Archive, DelegGroup}` — **not** from `durable_jseq() !=
   u64::MAX`: a lone root's `durable_jseq()` is `u64::MAX`, yet after C2
   its row for another node's forward is Archive-class, and must never
   enter the replay queue (the review's H1: it would be replayed by rid
   while its requester retries it, and a refusal would make a conflict
   copy of an op nobody was acknowledged). Set through a new
   `journal::PendingGate` (carrying the class) alongside
   `PendingLockTag` by `Meta::begin_local`; the core publishes the
   node's class for its own clients to the session (`set_durable` gains
   it; the fast path reads the same value through `LeaseView`), and a
   forward's class is set by the core before it executes. Until C2 the
   classes in use are C1's (RootGroup, Archive, Lone for everything
   else); C2 adds DelegGroup and the forward rule without changing the
   field. `JournalTxHead` gains it too (hot paths need it without the
   op).
2. **A deposed holder replays no gated row.** `Meta::strand_below_epoch`
   (`spec.rs:3136-3150`) splits — a deposed sequencer's gated rows of
   every class go to the member copy; only Lone-class rows enter the
   replay queue: a gated `Local { gen: 0 }` row is
   rolled back from its before-images and **moved, records unchanged,
   into the member-copy store** (the `backup_tail` keyspace under the
   node's own `(epoch, first)`; §5.6) — never queued in
   `pending_replay`; an ungated row (lone branch) is queued as today.
   Its `completed` row goes with it unless the log carries it (as today).
   The parked client ops of those rows are already turned into in-doubt
   same-rid retries by `abort_durable_parks` (`readindex.rs:880-985`):
   they now dedup against the successor's re-ship (committed rows) or
   execute once (uncommitted rows the sealer did not hold). Drop
   `self.replay.unacked.insert(rid)` for gated rows there (nothing is
   queued to mark).
3. **The seal path's successor re-ships with identity.** `apply_backup_tail`
   keeps its current re-application (verbatim identity is C3) but each
   adopted transaction now records `tail_of: Some(JournalPos { epoch:
   prev, jseq: tx.first })` in its `JournalTx` (the tail row's own
   `tail_of` if it has one: the first journal's identity, transitively),
   and the ship plan writes it into the segment envelope (§5.4's origin
   variant). The marker
   becomes `TailFollows { prev_epoch, through }`: `through` is the
   predecessor's last jseq in the sealed tail.
4. **Subscribers and requesters keep committed copies.** A `Shadow`,
   `Hint` or `Streamed` entry of epoch `e` is **not** stranded by a
   segment of a higher epoch whose marker carries `TailFollows {
   prev_epoch: e }` and which was installed for a gated answer
   (`Shadow`/`Hint` gain `gated: bool` from the reply — the holder's
   `MutateReply` gains `committed: bool`, true for RootGroup/Archive
   answers; a `Streamed` entry is gated by construction: `stream_ahead`
   sends only rows ≤ `durable_jseq` outside an epoch; an epoch hold
   streams to its tip, but a hold's successor writes no `TailFollows`, so
   those entries strand as today). It retires when a segment carries
   `Completed { rid }` (shadow) or a tail origin covering
   `(e, first..=last)` (streamed). New predicate in `LiveEntry`:
   `stranded_by_epoch(epoch, tail_follows: Option<u64>)`
   (`spec.rs:264`). An ungated entry strands as today.
5. **The TTL path carries the tail (tail fetch).** A node about to claim
   an expired `Backup` lease it is not the sealer of — a non-listed
   node after the grace (`lease.rs:345-372`), or a listed backup at
   expiry that has not sealed — first runs **`SealTail`**: new
   `PeerMsg::SealTail { req, epoch, config_version }` to every member
   of the old configuration (the holder and each listed backup); a
   member persists and syncs a seal of `epoch` (`Meta::backup_seal`;
   the holder itself marks the epoch sealed and stops acknowledging:
   `mark_lost` with the new `self_sealed` flag, so its gate never
   releases a parked answer of that epoch again) and answers
   `PeerMsg::SealedTail { req, epoch, txs: Vec<BackupTx>, through }`
   with its sealed tail (a backup's `backup_tail`; the holder's own
   journal rows of that epoch plus its member copy). The first answer
   is the tail the claimer re-ships (write-all ⇒ any member has every
   committed row); the claim then proceeds as a seal takeover
   (`takeover_permit`, `backup_tail_epoch` set, `TailFollows` in the
   marker). **If no member answers** within `backup_takeover_ms`, the
   behaviour is §11 Q1's decision: recommended, the claim waits (status
   `lease.waiting_for_group`, retry every `backup_takeover_ms`, log at
   WARN once a minute) until a member answers or an operator runs
   `constellation repair abandon-group --epoch E` (new, §5.8). The
   listed backup's claim at expiry also seals itself first (today it
   does not; `on_takeover_get`'s seal is reused). `backup_claim_grace_ms`
   stays: a listed backup should still get to its own seal and claim
   first; the grace orders the race, the tail fetch makes losing it
   harmless. **Acceptance and recovery** (the review's M2): a member
   accepts `SealTail` only for the epoch and `config_version` it holds
   (a backup: its `BackupRole`; the holder: its tenure); a holder that
   self-sealed runs `recover_deposed` at once (its gated rows go to the
   member copy, item 2) and is from then on an ordinary claimant of its
   own lease once expired — so if the sealing claimer never claims
   (lost the CAS race, crashed, lost S3), the outage is bounded by the
   lease TTL and the holder brings its tail along; `classify`'s refusal
   of a `lost` node applies only until that recovery finished. Any
   enrolled peer may seal a holder this way — the same trust as a
   backup's seal (semi-trusted peers, IAM as the root; ADR-12: the
   claimer's expiry judgement only *triggers* the seal, safety rests on
   the seal and the CAS). `SealedTail` is batched like `BackupAppend`
   (`backup_batch_rows`, one batch in flight, the claimer acks each).
6. **Docs and the stale comment.** `backup.rs:27-36` says what the fast
   path does (writes, then waits for the gate; `Ungated` handled in C2).

Why C1 is safe alone: it only stops *re-executing* rows that are
committed; every committed row is still delivered by the successor's
re-ship (seal path: write-all ⇒ the sealer has it; TTL path: the tail
fetch), and an uncommitted row's client was never answered and retries
by rid. The slot CAS still fences a deposed holder's late segment. What
it does not yet cover (delegates, the lone root's forwards, the `Ungated`
fast-path answer) are C2's.

### 5.3 C2: no acknowledgement outside a group where one exists; failed ops are settled

1. **Delegations get a configuration** (the study's step 2a, made
   exact). New log record `LogRecord::DelegGroup { dir: u64, gen: u64,
   version: u64, backups: Vec<u64> }`, journaled by the root only, in
   the generation's lifetime, version increasing. The delegate asks for
   a change with `DelegRenew.backup` (as today) and the root journals
   the record when the request differs from the generation's last
   record; the delegate counts a backup's acknowledgements only once it
   has applied (from the root's stream or the archive) a `DelegGroup`
   naming it, and stops needing a removed backup only once the record
   without it is applied — the lease's add-then-CAS and
   remove-after-CAS rules (`backup_promote`, `removal_safe`) transposed:
   the delegate **requests** a record that drops backup B1 only when its
   own `removal_safe` holds — every index acknowledged via B1 is at or
   below the root's appended cursor or the replacement's `backup_acked`
   (`backup.rs:1418` transposed) — so no acknowledged row is left only
   on a dropped backup (§11 Q14, decided).
   A generation with no record (or an empty `backups`) has the group
   `{delegate, root}`: the root's journaled append of `(g, i)` is the
   second copy. `deleg_stream_durable_with` (`delegate.rs:1127-1143`)
   becomes: archive carries it, or every recorded backup's
   `backup_acked ≥ i`, or (no backup recorded) the root's
   `DelegateStreamAck.through ≥ i` from the current root. The `None =>
   true` arm is deleted. `deleg_fast_path_gated` becomes "not lone".
2. **The lone delegate.** A delegate with no backup recorded and the
   root beyond `backup_rtt_budget_ms` (from `Core`'s link RTTs) is lone:
   its own node's ops are class Lone, others' forwards wait for the root's
   append (DelegGroup with the root as member). `status.delegation.mine[]`
   gains `ack_class`. Placement (`placement.rs:409-458`) is unchanged:
   a far dominant writer still gets its subtree (that is the
   offline-first case Layer A exists for). Whether placement should
   prefer a non-lone candidate is deferred (§11.1) until
   `status.delegation.mine[].ack_class` has shown how often it happens.
3. **The lone root answers other nodes from the archive.** In
   `holder.rs:285-405`, a forward's `durable` wait for class Archive is
   `shipped_through`; the reply carries `committed: true`; the requester
   installs no replayable shadow (it waits for the log or the stream, as
   `AwaitingLog` does). Shadows installed after C2 are therefore always
   for committed answers (I5). The row is journaled Archive-class
   (item 1 of §5.2): on the lone root's deposition it goes to the member
   copy, never to the replay queue; the requester's same-rid retry is
   its one copy.
4. **The `Ungated` fast-path answer.** `durable_jseq() == u64::MAX`
   (the gate off, `session.rs:1286-1308`) holds in exactly two states:
   a lone `Local` tenure and an epoch hold (`backup.rs:489-530`).
   `DurableWait::Ungated` therefore answers OK **iff the node is lone or
   holds an epoch** (class Lone or NonStealable; the session learns both
   from `set_durable`); any other `Ungated` is an invariant violation
   (`debug_assert!`, counted in `ack.ungated_unexpected`, answered by
   resubmitting in doubt). It must never wait for the archive in an
   epoch: S3 is unreachable there by definition, and that wait is the
   plan 30 §M10 regression the comment in `durable_jseq` records.
5. **Settling an op before `EIO` (`Abandon`).** New
   `MutateOp::Abandon {}` (executed by rid only) and portable
   `Code::Abandoned` (`crates/types/src/errno.rs`, a new wire number;
   `EIO` at the FUSE boundary): the executor answers `r`'s outcome if
   `completed` has one; otherwise journals `Refused { rid: r, code:
   Abandoned }` and answers it. `on_client_deadline`
   (`client.rs:2463-2518`) no longer calls `finish_in_doubt` directly:
   it submits `Abandon` under the same rid (P2P, then inbox, then the
   lease path: the ordinary routing) and answers the client with the
   outcome — the late success (`OK`), the refusal, or `EIO` for
   `Abandoned`. A sequencer's takeover gate re-ships its sealed tail
   before it executes anything (the gate is closed until then), so an
   `Abandon` never overtakes a committed copy of `r`. After a further
   `acquire_deadline_ms` with no answer (isolated requester), the client
   hears `EIO` unsettled (`stats.client_unsettled_eio`, I4's exception);
   the `in_doubt_rids` set keeps `r` so a resubmission still dedups.
   Every dedup site maps `Refused { Abandoned }` to `EIO` (never
   re-executes). The ledger's late-landing allowance is removed for
   settled answers. Precisely:
   - **One client entry.** The `Abandon` reuses the rid's `clients`
     entry (its phase becomes `Settling`); it never adds a second
     submission of the rid (I2(b)).
   - **It waits for its own commit.** `Refused { r, Abandoned }` is a
     row like any other: under RootGroup or DelegGroup its answer waits
     for its commit before the `EIO` leaves.
   - **A copy in flight across a routing change.** `r` forwarded to root
     R and delayed in the network; `r`'s directory delegated to D
     meanwhile; the deadline routes `Abandon` to D, D journals the
     refusal; the delayed `r` arrives at R. R does not execute in a
     delegated subtree without recalling it first, the recall drains D's
     stream (which carries the refusal) and `completed` then dedups `r`
     at R. The takeover case is the gate's (the sealed tail is re-shipped
     before anything executes). These two are the whole of "no execution
     of `r` exists anywhere afterwards"; the code comment on
     `on_client_deadline` states both.
   - **The deadline.** The existing client deadline (`acquire_deadline_ms`,
     2 × TTL) is split: the op's own attempts until 1.5 × TTL, the
     `Abandon` until 2 × TTL, then the unsettled `EIO` (§11 Q9, decided
     in favour of keeping the user-visible bound at 2 × TTL; the
     alternative, a second full deadline, would make it 240 s).
   - **Shape** (§11 Q15, open): a `settle: bool` flag on
     `PeerMsg::MutateRequest`, the inbox entry and the lease-path submit
     instead of a `MutateOp::Abandon` variant (an op "executed by rid
     only" would otherwise leak into the fast path, `derive_replay_op`,
     `TouchSet::from_op` and the inbox batch format). Recommended: the
     flag; the rest of this plan writes `Abandon` for either.
   - `Code::Abandoned`: check `crates/types/src/errno.rs`'s portable
     table for a reserved hole before appending a new wire number.
6. **Stranded unacknowledged ops of a delegation are dropped, not
   replayed.** At a `Recall` (`stranded_by_recall`,
   `spec.rs:275-283`), a delegate's rows past the cut are uncommitted
   (after C2 a committed row is at or below the cut, item 7) and their
   clients were never answered: roll back, drop, and let the parked
   clients retry by rid (`deleg_abort_stream_parks` already does it for a
   sealed backup). A requester's `Shadow { gen }` past the cut cannot
   exist (shadows are only installed for committed answers).
7. **A generation is ended only with its committed rows, and the cut
   is in the record.** The root ends a generation (recall drained, or
   outwait) only once its cursor covers every row a member of the
   generation's group may hold: with the root as the second member that
   is its own appended cursor; with recorded backups, the sealed tail
   of **every backup any `DelegGroup` record of the generation named**
   whose rows may be uncovered (one `DelegSeal` each; today
   `on_deleg_expiry`, `delegate.rs:3010-3117`, seals only the latest
   `gs.backup`, once, and then reclaims without it). A named backup that
   does not answer is §11 Q1's case for a delegation: the generation is
   not ended (its subtree's writes wait,
   `status.delegation.gens[].waiting_for_group`) until a member answers
   or `constellation repair abandon-group --gen G`.
   The end is journaled as `LogRecord::Recall { dir, gen, through: u64 }`
   (`record.rs:243`, field added): `through` is the root's cursor at the
   end (`end_generation`, `delegate.rs:3164-3238`). Every replica's void
   cut is `through` (`replay.rs:782-826`: `void_stream(gen, through)`,
   `note_void_cut(gen, through)`; `session.rs` `seed_generations`), not
   what the replica happens to hold of the stream; `deleg_log_idx:{gen}`
   becomes a cache of the log. The generation counter stays the root's
   (`next_deleg_gen` floored by `max_gen`, both from the log). (Moved
   here from C4 so that C3 can rely on it; the review's H4.)
8. **The delegate's fast path keeps the root's shape.** Today
   `deleg_fast_path_gated` *closes* the delegate's fast path whenever it
   has a backup (ops go through the core and park, `delegate.rs:411`),
   while the root's fast path writes on the FUSE thread and waits after
   (`write_gate.rs:453-536`). C2 gives the delegate the root's shape: the
   delegate's fast path (`DelegateView::admit_for`,
   `crates/engine/src/lease.rs:549-576`; `Meta::delegate_execute_admitted`)
   journals on the FUSE thread, then waits on a per-generation durable
   watermark (`SessionState::wait_deleg_durable(gen, idx, budget)`, new,
   fed by the core from `backup_acked`/`DelegateStreamAck` as
   `set_durable` is for the root), falling back to an in-doubt
   resubmission on `Lost`/`TimedOut` exactly as the root's does. Under
   load the core thread is the bottleneck (the F2 review measured 73 %
   of a loaded delegate's core samples in the parked-acknowledgement
   check); routing every delegated write through it would put the C2
   latency gate and the "only latency may degrade" requirement at
   risk.

Why C2 is safe alone: it only adds waits (a delegate's answer waits for
a second copy; a lone root's answer to a forward waits for the archive;
an `Ungated` answer waits for the archive) and replaces an unsettled
`EIO` by a settled one. Every acknowledged row now satisfies I1, so
C3's deletions become possible. The generation end records its cut
(item 7) and covers every committed row (the root appended every row
acknowledged via the root; every named backup's rows are drained by the
seal path, `on_deleg_sealed`, before the end).

### 5.4 C3: verbatim re-ship; no re-execution of a committed row

1. **Segment origins** (`crates/authority/src/segment.rs:29-33`):
   `origins: Vec<(u64, u64)>` becomes `origins: Vec<Origin>` with
   ```rust
   pub enum Origin { Own, Deleg { gen: u64, idx: u64 },
                     Tail { epoch: u64, jseq: u64 } }
   ```
   (`Own` replaces `(0, 0)`). `JournalTx` gains `tail_of:
   Option<JournalPos>` (set in C1) and the ship plan maps it to `Tail`.
2. **The successor applies a sealed tail verbatim.** `apply_backup_tail`
   (`backup.rs:2364-2428`) appends each tail transaction to its journal
   with its records unchanged and `tail_of` set, and applies them
   through the *tail apply* (`Replica::apply_segment`'s record apply,
   not `apply_records_journaled_inner`'s skip-on-conflict): the
   successor's replica is the archive prefix the predecessor built the
   tail on (it tailed to head before the marker), so the records apply
   exactly. A record that does not apply cleanly is an invariant
   violation (`MetaError::TailDiverged`, logged at ERROR with the tail
   position; the gate stays closed; the sim fails the seed). Rows of
   the tail whose rid the archive already completed are kept (verbatim:
   the archive cannot hold a committed row twice unless a bug made it,
   which I2's check catches) — the dedup-by-rid skip in
   `apply_backup_tail` becomes a `debug_assert!` plus a counted error.
3. **A delegate that takes the root over** (the two-node headline case:
   node a is the delegate of `d1` and the backup of root b; at a's seal
   takeover a's journal already holds its own rows `(g, 5)`, `(g, 6)`
   *before* the tail, and b's tail holds b's append of `(g, 5)` with
   origin `Deleg { g, 5 }` and b's later ops). In the gate, in this
   order:
   1. **move** this node's rows of every live generation it holds as
      delegate out of the journal, their effects rolled back from their
      before-images, into its member copy of `g` (`backup_tail[DELEG_BACKUP_BASE
      + g]`, §5.6). The strand stays (it is what keeps the tail from
      being applied on top of state from later in the log —
      `delegated-holder-cut` seed 772: a create in `d1` would otherwise
      ship ahead of the tail's rename of the same name out of `d1`); what
      is deleted is the re-execution by rid and the generation hole;
   2. apply the sealed tail **verbatim** (item 2);
   3. **re-append** the moved rows verbatim under `(g, idx)`, in index
      order, **skipping every `(g, idx)` the tail already carries**
      (dedup by delegation origin, not by rid); a skipped row and the
      tail's copy must be record-identical, else a hard error
      (`MetaError::TailDiverged`). If the tail itself ends `g`
      (`Recall { gen: g, through }` in it), the moved rows above
      `through` are **dropped**, not re-appended: they were never
      committed (after C2 a delegate row without a recorded backup is
      acknowledged only once the root appended it, i.e. at or below the
      cursor the `Recall` recorded; with a recorded backup, the root
      drained that backup before ending `g`), and their clients retry by
      rid;
   4. end each still-live generation with `Recall { through }` (C2's
      item 7) covering the re-appended rows.
   Why step 3 applies cleanly on the post-tail replica (the ownership
   argument): the root executes nothing on keys a live generation owns —
   an op wholly inside the subtree is the delegate's, and a
   cross-subtree op recalls the generation first (ADR-23, no 2PC). So if
   the tail does not end `g`, no tail row other than `g`'s own appended
   rows (skipped) touches `g`'s keys, and the moved rows re-apply on the
   post-tail replica exactly as they applied before; if the tail ends
   `g`, everything above its cut is dropped, and seed 772's shape (the
   tail's rename of a name out of `d1`, which needed the recall) is
   ordered correctly by construction. A row that still does not apply is
   `TailDiverged` (gate closed, the sim fails the seed).
   Deleted: `strand_for_takeover`'s re-execution of `own_gens` rows by
   rid (`spec.rs:3181-3215`), the `holed` generations,
   `mark_deleg_stranded_tx`, `delegate_stranded`, the stopped-at-install
   rule and the recall answer at `log_stream_idx` (`96f4565`'s
   machinery): a generation never gets a hole.
   **Not deleted without a replacement: the fence.** The mark is also what
   keeps the node's own delegate fast path off the replica during the
   gate (`Meta::begin_local` refuses a delegate's own execution under a
   marked generation, `NotAdmitted`, at the write under the writer lock;
   the engine then sends the op through the core, whose gate parks it).
   The engine's admission is a mirror of the core's table, refreshed
   after an event, so without a store-level fence an own write queued
   behind step 1's "move" transaction executes between the move and the
   re-append: its fresh row precedes the moved rows and the verbatim
   re-append of `Create f7` fails with `TailDiverged` (the gate closed,
   a stuck takeover; load campaign `load-probe-takeover` seeds
   55/69/75/77 lost the acknowledged file to this race before the
   fence). Step 1's "move" must therefore end the node's own generations
   in its own transaction, under a key of C3's own, and `begin_local`
   must refuse the delegate execution under an ended generation: the
   rule is that while a sequencer's gate is open none of its own paths
   executes a mutation on the replica.
4. **A deposed root keeps another generation's rows as a member copy**
   (§5.6) instead of the `RebuildReplica` → `reset_for_rebuilt_ns_tx`
   → `Records` replays path. `recover_deposed` (`jobs.rs:1381-1410`)
   never emits `RebuildReplica` for captured journals;
   `reset_for_rebuilt_ns_tx` keeps only the uncaptured fallback
   (`CONSTELLATION_HOLDER_CAPTURE=0`), where it queues nothing for gated
   rows.
5. **Deleted with the paths**: `folded_into_later_manifest` and its two
   call sites (`replay.rs:75-99`, `:140-144`, `:533-537`),
   `rebase_on_truncate`, `truncate_base` in `QueuedReplay` (or the
   takeover-loss fix's conditional-truncate replacement) — **except**
   inside the lone branch (C5 keeps the conditional truncate there);
   `MutateOp::Records` as a replay of a committed row: the foreign
   `Streamed` stranding arm (`spec.rs:1288-1307`), `PendingLocalOp =
   Records` for appended delegate rows and adopted tails
   (`local.rs:850-857`); `derive_replay_op` is kept for lone-branch rows
   only; `settled_outcome`'s `lost && journal_carries_outcome` branch
   (`client.rs:2547-2569`; the deposed journal no longer holds anything
   but the lone branch); the 2945 fix's `QueuedReplay.deps` (no deposed
   replay of a forwarded op exists).
6. `TailFollows` stays as the seal record (Delos: the sealed loglet's
   suffix follows) with `through` (C1); its session effect (`owe`) is
   unchanged.

**Design inputs found before C3 started (2026-10-17).** Two shapes the
pre-C3 fixes left open, which C3's re-ship must close: (1) on the rebuild
path (`recover_deposed` → `RebuildReplica` → `reset_for_rebuilt_ns_tx`)
a deposed root's appended delegate rows become `Records` copies queued
*behind* a dependent op with empty deps, so its own queue can land a
marker before its data (the 2945 symptom via the copies); (2) a
delegate's acknowledged rows under a grant the deposed root never shipped
come back only as the delegate's own replays by rid, unordered against
the root's dependent replay. For a never-shipped generation the root's
appended copies are the natural carrier and sit ahead of the dependent op
in journal order: C3's verbatim re-ship keeps that order, and the
delegate's replays of the same rids then dedup. Also: nothing of a node
may execute on its replica between a takeover's strand and the end of its
re-ship (a delegate fast-path write landed in that window, `f7`); C3
step 1 must keep the fence the stopgap adds (own generations marked
stranded in the strand transaction, `begin_local` answering
`NotAdmitted`), since C3 deletes `mark_deleg_stranded_tx` as written.

### 5.5 C4: lock and delegation state in the log

**Delegation state.** The generation's start (`Delegate`), group
(`DelegGroup`) and end with its cut (`Recall { through }`) are already
records when C4 starts (C2). C4 adds the locks.

**Lock records** (`crates/meta/src/record.rs`, appended to the enum):

```rust
LockGrant { id: GrantId, ino: Ino, node: u64, mode: u8, gen: u64,
            floor: Position },   // floor: what the holder must wait for
LockRelease { id: GrantId, floor: Position },  // the releaser's frontier
LockExpire { id: GrantId, barrier: Position }, // outwaited; the cut
LockMove { ids: Vec<GrantId>, gen: u64 },      // to a generation (0: root)
```

- The owning sequencer journals them in the same transaction as the
  decision: a grant when it mints (`lock_try_grant`, `locks.rs:1300`),
  a release when it processes `LockReleased` (`on_lock_released`,
  `:2395`), an expiry when it outwaits (`on_lock_grant_expiry`,
  `lock_take_outwaited`), a move inside the transaction of the
  `Delegate` record (subtree grants to `gen`), of the `Recall` record
  (the generation's live grants back to 0), and of an unlink's stream
  row in a delegated subtree (today's `leaving` grants, moved to 0).
  A delegate's records are rows of its stream `(gen, i)` and reach the
  log through the root's append.
- **A renewal that only extends the window is not logged**: it changes
  no identity. Windows stay per-replica in memory (`Grant.until_ms`,
  `HeldGrant.until_ms`); plan 41's lane keeps extending them. A renewal
  that **installs** a grant — a reclaim during a fast successor's grace
  (`lock_renew_one`'s "unknown: a reclaim", `core/locks.rs:2666-2697`) —
  or a request that **changes a grant's mode** (`lock_try_grant`'s
  `own.mode.max(mode)` upgrade, `:1326`) is a grant decision and
  journals a `LockGrant` (the same id, the new mode), answered after its
  commit like any grant.
- **Derived table**: `Replica::apply_segment` and the journal path
  apply the records to `LockTables` owner-side (`grants`, `ended`)
  through one function `LockTables::apply_record(&LogRecord, now)`;
  a grant learned from the log is installed with
  `confirmed_ms = UNCONFIRMED` and `until_ms = restamp(now)` (today's
  mirror semantics, now exact: the set of live ids is the log's). A
  grant's answer to its holder waits for its record's commit (class of
  the owner; under RootGroup one LAN round trip; the restart-horizon
  wait stays and overlaps it).
- **Floors and barriers** are the records' `floor`/`barrier` fields;
  the in-memory `floors`, `dir_floors`, `handed_floor`, `barriers`,
  `dir_barriers`, `cut` (`locks.rs:296-387`) become caches rebuilt from
  the derived table (a successor rebuilds them at its gate from the
  log, instead of from a mirror or a handoff).
- **Fencing token**: the minter judges by the derived table (exact, at
  its serial apply, inside the transaction — `check_tag_at_write`
  unchanged); a non-minting executor still judges by the token's window
  until it applies the `LockExpire`/`LockRelease` (the study's §2.2
  residual: one mechanism, a window nonetheless).
- **Deleted** (each listed with the bug it was for, so the reviewer can
  check the class is gone): `PeerMsg::LockMirror`, `bk_mirror`,
  `lock_install_mirror`, `mirror_dirty`/`mirror_ver`
  (`df14686`; model `fast_takeover_with_only_the_async_mirror_violates`);
  `DelegRenewed.{locks, lock_floor, lock_cut_at, lock_cut,
  lock_barrier}` and `handoff`, `handed`, `moved_seen`,
  `lock_take_handoff`, `lock_install_moved`, `install_if_consistent`'s
  copy rule (`381c66d`, seeds 196252/196102/196004); `DelegRecalled.locks`
  and `lock_hand_back`/`lock_install_returned` with the late-answer rules
  (`9d82ad0`, `fb0c909`: `unlinked-exclusion-7455`); `DelegateStream.{leaving,
  leaving_barriers}`, `lock_install_leaving*`, `lock_take_back_left`,
  `deleg_drop_left` (`4c5d184`); `lock_on_generation_outwaited`'s
  restamped grace and the handoff kept until end (`c465b07`:
  `unlinked-exclusion-5681`); `restamp` at the five install sites except
  the log-derived install. `DelegRenewed.lock_grace_ms` stays (a grace is
  a time window, not state) — or, if C4's model shows it derivable from
  the log (the takeover marker time and the predecessor's expiry are in
  the log and the lease), is derived; C4's model tests decide.
- `DelegRenew.{stream_head, stream_head_at}` stay (a renewal reports
  liveness and progress; the barrier it feeds is now recorded by the
  owner's `LockExpire.barrier`).

### 5.6 Member copies (C1 for the root's own rows, C3 for appended delegate rows)

A node that stops sequencing for a group it is a member of keeps the
rows it holds for that group until the archive carries them:

- **A deposed holder** (C1): its gated unshipped rows of epoch `e`
  move from `journal` to `backup_tail[(e, first)]` (same `BackupTx`
  shape the backup role uses), with their before-images applied (the
  replica returns to the archive prefix). It answers `SealTail` for `e`
  from there. It drops rows `≤ through` when it applies the segment
  whose `Tail { epoch: e, jseq: through }` origin covers them, and the
  rest (uncommitted: the sealer did not hold them) when it applies the
  successor's `TailFollows { prev_epoch: e, through }`. A marker of an
  epoch above `e` that carries **no** `TailFollows { prev_epoch: e }`
  drops `e`'s member copy too: after C1 a `Backup` lease's successor
  always carries `TailFollows`, so such a marker is the TTL successor of
  a `Local` lease, whose gated rows were Archive-class and never
  acknowledged (their clients retry by rid). A member copy of an epoch
  the operator abandoned (§5.8) becomes the lone-branch replay queue
  (the only path by which it is re-executed).
- **A deposed root** (C3): another generation's appended rows
  (`Local { gen != 0 }`) move to `backup_tail[DELEG_BACKUP_BASE + gen]`
  (the delegate-backup tail keyspace, `crates/meta/src/store/backup.rs:390-444`),
  and are served on `DelegSeal { gen }` exactly as a recorded backup's
  are (`on_deleg_sealed` appends them under `(g, i)`). The next root,
  outwaiting an inherited generation whose group has no recorded backup,
  sends `DelegSeal` to the previous root(s) named by the archive's
  epoch markers since the generation's last appended index. Dropped when
  the log's `deleg_log_idx` passes them or the `Recall { through }`
  lands.
- `status.ack.member_copies` lists `(kind, epoch|gen, rows, oldest_ms)`.

This answers the paused seeds' design question: a deposed root neither
drops nor rebuilds-and-replays; it keeps the copy it holds as a group
member, so no acknowledged row loses a copy and no copy is executed.

### 5.7 C5: the lone branch, isolated and documented

After C3 the replay queue has exactly one producer: a deposed **lone**
sequencer's ungated rows (and an operator's abandon). C5:

- renames the queue's meaning in code (`QueuedReplay` → `LoneBranchOp`
  with `kind: LoneOwn | Abandoned | DropHeld`), asserts at enqueue that
  the row is ungated (`debug_assert!` and a counted error), and keeps
  the lone branch's replay shapes (the original op; the conditional
  truncate; `derive_replay_op` for op-less rows);
- conflict copies only from that queue (I5); `status.ack` reports
  `lone`, `lone_acks`, `lone_branch_replays`, `lone_branch_conflicts`;
- documents the contract (GOALS, DESIGN §4/§9, durability-and-failover):
  "a writer with no peer within the RTT budget is offline-first: its own
  acknowledgements rest on its disk; a deposition while it is alive and
  partitioned brings them back as a branch, with conflict copies where
  the namespace moved on. `ack=s3` (every acknowledgement waits for the
  archive) and an offline designation (non-stealable) are the two ways
  to have none";
- documents §11 Q3's decision for epoch holds: **non-stealable except
  by an operator's retire.** The hold owner's deposition paths are
  `Control::Retire` (`promise.rs:495-507`) — the documented abandonment
  of a non-stealable authority: "retiring a node gives up its unshipped
  journal", whose rows go to the lone-branch queue of whoever runs it —
  and the supersession of an epoch that carried no lease
  (`release_superseded`, `epochs_superseded`), which acknowledged
  nothing. C0's flex property `hold_owner_never_deposed_except_by_retire`
  checks exactly this; if it finds another path, the hold owner's
  answers to members' forwards wait for one member's copy of its
  stream-ahead (a new `StreamAheadAck`) — never for S3.

### 5.8 Operator escape: `constellation repair abandon-group`

`constellation repair abandon-group --epoch E` (and `--gen G`): run on
the node that waits to claim (§5.2 item 5) or to end a generation whose
group is unreachable. It journals `LogRecord::GroupAbandoned { epoch |
gen, through_known }` and lets the claim or the generation end proceed;
every member copy of `E` (or `G`) that later reappears is moved to that
node's lone-branch queue (replayed by rid, conflict copies on refusal:
explicitly an operator's choice, counted in `status.ack.abandoned`).
Refused while any member of the group answers `SealTail`. The command
exists because §11 Q1 recommends the strict wait; if the maintainer
chooses the bounded variant instead, it is replaced by the bound.

### 5.9 Why the rules are sufficient (the integrity argument, to be extended in code)

- *Write-all, read-any*: a row is committed only when every member of
  its configuration holds it (I1). A reconfiguration (seal path, tail
  fetch, generation end) reads one member's sealed tail after sealing
  it; the seal stops the old sequencer from committing anything more
  (it needs every member). So the new sequencer holds every committed
  row, and re-ships it verbatim (I2c). This is Vertical Paxos with
  write quorum = all, read quorum = one; the configuration master (the
  lease CAS, or the root's log for a delegation) guarantees exactly one
  reconfiguration wins (same object version / serial log).
- *Nothing committed is lost to a deposition*: the deposed sequencer
  keeps its copy as a member copy until the archive has it (§5.6), so
  even a successor that sealed a different member can be cross-checked
  (and, with Q1's strict rule, a TTL successor cannot start without one).
- *Nothing uncommitted is acknowledged*: by I1; its client retries by
  rid, deduped by the re-ship if a member had it, executed once
  otherwise, or settled by `Abandon` at the deadline (I4).
- *Authority state cannot be revived by a late message*: no message
  carries it (I3); a late recall answer or renewal reply is a stale
  proposal the owner ignores.
- *What still rests on clocks*: lock and delegation windows, read
  delegations, the successor's quarantine, the fast-path admission
  check — unchanged rules (C7 changes their clock assumption, not their
  structure).

## 6. Specification changes this plan authorizes

CONVENTIONS rule 5 forbids editing `docs/explanation/DESIGN.md` unless
a plan authorizes it. This plan does, section by section, each in the
milestone that makes the text true (never earlier). The substance of
each new text is given; wording is the executor's, in the existing
style.

| DESIGN section (line at `05ef4c9`) | Milestone | New substance |
|---|---|---|
| §1 Overview (L30-37): "Correctness never depends on P2P; S3 remains the arbiter of record" | C2 | S3 is the **configuration master** (who sequences, with which group, at which epoch) and the **archive**; a write is committed when the configured group holds it; P2P carries the group's copies. Correctness never depends on failure detection; a node with no peer in budget commits through S3 or acknowledges as the documented lone writer. |
| §1 Hard constraints (L70-93) | C2 | Unchanged list; constraint 4 gains: "a writer with no peer in budget acknowledges its own node's writes at local speed as a lone writer (§4), and other nodes' writes once archived". |
| §4 intro (L582-587) | C0 | The Delos vocabulary (§3 of this plan): configuration master, configuration, group, commit, seal, sealed tail, archive, lone sequencer, member copy. |
| §4 Local store (L589-626) | C1, C3, C5 | `backup_tail` holds a backup's tail **and member copies** (C1/C3); `spec`/`pending_replay` described as committed-ahead installs plus the lone branch's queue (C5). The before-image funnel stays (it now serves ordering and the lone branch). |
| §4 One log, one root lease (L628-685) | C1, C3 | Acquire and takeover: a successor re-ships the sealed tail **verbatim** (C3), obtained by its own seal or by a tail fetch from any member (C1); "a predecessor's unshipped tail is replayed by its requesters (Layer A)" holds only for a lone predecessor. `TailFollows { prev_epoch, through }`. Records list gains `DelegGroup` and `Recall.through` (C2), the lock records (C4), `GroupAbandoned` (C1). |
| §4 Request ids and exactly-once (L687-707) | C2 | "An op still in doubt at its deadline fails with `EIO`" becomes: the requester **settles** the rid (`Abandon`, answered by the sequencer in force) and answers the outcome, `EIO` only for `Abandoned`; an isolated requester's `EIO` after twice the deadline is the documented exception. Replays are no longer listed among the exactly-once users except the lone branch. |
| §4 "The replica is a log prefix plus explicit speculation" (L709-730) | C1, C3, C5 | Rewritten: the replica is the archive prefix plus **committed rows ahead of the archive** (the group's stream, committed shadows) plus, on a lone sequencer only, its own speculation. Committed entries never strand; they retire when the archive carries their origin. "A replay the namespace no longer admits ... the only source of conflict copies" is narrowed to the lone branch and operator actions. |
| §4 Positions and log streams (L732-753) | C1 | The pre-S3 stream carries **committed** rows (installed as committed, never rolled back by a seal takeover). |
| §4 Acknowledgement and durability (L755-774) | C2, C5 | Replaced by the commit rule: the `AckClass` table (§5.1), the delegation group, the lone sequencer, the forward-from-archive rule. Layers A/B/C remain as names for the user-facing policy (`local` = commit by group where one exists, lone otherwise; `s3` = archive). |
| §4 Delegated sub-sequencers (L776-825) | C2, C4, C3 | The delegate acknowledges once its group holds the row (a recorded backup, or the root's append); `DelegGroup` is the delegation's configuration; the cut is in `Recall`; a delegate that takes the root over appends its own rows as rows of its generation. Fix the TTL drift (L787 says 5 s; the engine default is the lock TTL, 20 s, at least 5 s). |
| §4 Locks (L843-847) | C4 | Grants, releases, expiries and moves are log records of the owning sequencer; renewals are not. |
| §5 One Rule (L849-877) | C7 | The time discipline restated as rate-bound: the holder measures from its own send on a clock that never under-counts, the grantor from its own receipt on a clock that never over-counts; `margin ≥ 2ρ·T + transit` replaces `margin > 2 × drift`; the root lease is judged by ETag observation, not by comparing `expires_unix_ms` across nodes; the non-minting token check rests on a one-way delay bound. |
| §5.2 Offline designation (L895-915) | C5 | A designee's acknowledgements are non-stealable, hence not speculation; the "no speculation" alternative for an offline writer. |
| §5.3 Continuation epochs (L917-993) | C5 | Per Q3: the hold is a non-stealable authority (its journal is not speculation; "a deposed hold owner rolls back and replays by rid" removed or restricted to the operator's retire), or the hold's acknowledgements join the group rule. |
| §5 Availability matrix (L995-1014) | C1, C2 | Rows "Holder dies, no backup", "Delegate dies", "Delegate cut from the root": no requester replay; a lone holder's own writes come back as its branch; a group cut from S3 and from the claimer past the TTL: the claimer waits for a member (Q1). |
| §6 Session guarantees (L1036-1049) | C2 | A shadow is installed only for a committed answer. |
| §6 Cluster locks (L1091-1168) | C4 | Floors and barriers are record fields; "move with the lock table ... through the backup mirror" removed; failover: a fast successor derives the grant set from the log (exact) and keeps the grace for windows; the fencing-token paragraph's "keeps a refused close's content as a conflict copy" stays (fenced writer). |
| §6 Staleness, precisely (L1170-1202) | C1, C3 | "prefix plus explicit speculation" → prefix plus committed rows plus a lone sequencer's own speculation; the takeover gate re-ships verbatim. |
| §9 Failover by topology (L1360-1375) | C1, C2, C5 | Rows rewritten per the commit rule; the "Only distant peers" row is the lone writer's branch; the bring-up rule stays. |
| §9 Seal-based failover (L1377-1408) | C1, C3 | Verbatim re-ship; the tail fetch on the TTL path; a listed backup seals before it claims at expiry. |
| §9 Nobody observes a tentative effect (L1410-1418) | C2 | Renamed "Reads observe committed state". **The mechanism is unchanged** (`durability_pending`: under RootGroup the holder's read wait already *is* the commit wait; the study's "retire" of these waits was loose) — only the name and the framing move; the lone-root sentence stays as the lone branch's contract. |
| §9 Delegates (L1420-1439) | C2, C3 | No requester replay; the delegate's group; a crashed delegate without a recorded backup loses nothing acknowledged (the root holds it); `Held` remains only for the lone branch. |
| §9 Other failures (L1454-1466) | C1, C3 | "holder deposed ... replays it by rid ... conflict copies" → keeps a member copy; only a lone holder's branch replays. |
| §9 Other failures, "clock skew" row (L1454-1466) | C7 | Rate within 0.1 % on `CLOCK_MONOTONIC_RAW`, any offset; what stays offset-bound (GC's horizon, timestamps). |
| §14 GC (L1924-1926) | C2 | "fails with `EIO` rather than being re-executed" → settled by `Abandon`. |

GOALS.md L17-20 ("no conflict copies in the default modes", C5:
"except a lone writer's branch after it was deposed while partitioned,
which `ack=s3` or an offline designation avoids") and L21-31
(offline-first; local-FS-fast: the lone writer's contract).

DECISIONS.md: a new **ADR-33 "Commit is the configured group's
acknowledgement (Vertical Paxos over S3)"** (C1 writes the decision and
the rejected options — study §4 A, B1, C, D — and each later milestone
amends it); amendments to ADR-2 (S3 as configuration master and archive,
not the per-write arbiter; C2), ADR-14 (the shadow; C2), ADR-18
(settling before `EIO`; C2), ADR-19 ("superseded in part by ADR-33":
speculation only in the lone branch; C5), ADR-21 (Layer B commit; the
tail fetch; C1), ADR-22 (Q3; C5), ADR-23 (delegation group, cut in the
log, verbatim; C2–C3), ADR-25 (locks in the log; C4).

Reference pages: `forwarded-mutations.md` (exactly-once and in-doubt,
acknowledgement and durability, speculation and stranded-op recovery
— the main rewrite, C1/C2/C3/C5; status counters), `durability-and-failover.md`
(Layer A/B, seal-based failover incl. "Why it is safe", pre-S3
streaming, epochs, known limits; C1/C2/C5), `delegations.md` (how a
delegate executes, append path, failures; C2/C4/C3), `cluster-locks.md`
(the fencing token's replay paragraphs, coherence, failover, delegations;
C4/C3), `cto-modes.md` (positions of a tenure that ended, the session
wait's replay step, direct log streams; C1/C3), and
`docs/how-to-guides/development/TESTING.md` (the oracles, configs and
scenarios; every milestone).

## 7. Milestones

Each milestone ends green on `cargo fmt`, `clippy -D warnings` and
`cargo test --workspace`; its own sim sweeps at the seed counts given;
its harness scenarios 3× green; its campaign cells; its performance
gates (§9). The full CONVENTIONS gates run at C5's end and at the end of
the plan (§14). **Every milestone is reviewed (fable) before the next
starts**: C1, C2 and C3 are complex by any measure; C0 and C4 touch the
oracles and the lock table that every later proof rests on. A milestone
that cannot meet its exit criteria stops and reports; it never loosens
an oracle or an assertion to pass. An existing test whose assertion
encodes behaviour this plan removes (for example a count of replays
under a backup) is **rewritten to the new contract**, listed in
PROGRESS with the old and new assertion and the reason; this plan
authorizes exactly those rewrites and no other loosening.

Seed counts: "S×N" is N seeds via `sweep_config`
(`AUTHORITY_SIM_CONFIG=S AUTHORITY_SIM_SEEDS=N cargo test -p
constellation-authority --test sim -- --ignored sweep_config`); a new
config is added to both `sweep_config` and `replay_seed`
(`crates/authority/tests/sim.rs:236-387`, `:2640-2703`), and gets a
non-ignored test over a small range so CI runs it.

### C0 — Name the commit point and check it (no behaviour change)

**Code.**

- `crates/authority/tests/sim/`: a `ClusterStores` registry passed in
  `NodeEnv` (node id → `Arc<Meta>`, plus read access to the sim
  bucket's objects: `Bucket::peek(key)` in `store.rs`) so a check can
  read every node's store at the instant of an answer.
- **I1 oracle** `check::committed_at_answer(stores, cfg, rid) ->
  Option<AckClass>` evaluated **at the answering sequencer's emission**:
  in the answering node's `Node::dispatch` for `Action::Send {
  MutateReply }` with a definitive outcome (a forward's answer) and for
  `Action::Reply` with `ClientReply::Outcome` when the node answers its
  own client (`node.rs:1306-1320`), reading every store at that instant
  — not when the requester processes the reply (a `CrashBackup` in
  between would drop an unsynced tail and give a false violation; a
  segment landing in between would give a false pass).
  It classifies independently of the core: Archive (the bucket's
  segments carry the rid's outcome), RootGroup (the lease object read
  from the bucket names holder `h`, epoch `e`, `backups` at version
  `v`; `h`'s journal and every listed backup's `backup_tail` hold the
  row — the object read at the answer is the right set to check: a
  backup is listed only after it caught up and removed only once
  `removal_safe`; after a takeover, the successor's journal holding the
  re-shipped row and its own group count), DelegGroup (the delegate's journal plus each backup the
  delegate's `DelegateState` names — C2 switches this to the
  `DelegGroup` record — or the root's journal row with origin
  `(g, i)`), NonStealable (`SimConfig.designations` covers the keys;
  epoch hold), Lone (the answering node is the client's node and has no
  peer within `backup_rtt_budget_ms` in `SimConfig.rtts`/the bus RTT —
  for a delegate, the root beyond it too). The first class that holds is
  recorded in a new `AckLog` (`(node, rid, sim ms, Option<AckClass>)`,
  shared like `InDoubtLog`, `node.rs:219-224`). `None` is a violation
  when `SimConfig.strict_commit` (new, default `false`), always counted
  in `RunReport.uncommitted_acks` (per class that was expected).
- **I2 oracle**: an `IdentityLedger` (rid → set of `(node, Copy)` with
  `Copy::{Client, Queued, Executing}`) fed by the client threads
  (submit/resubmit/answer, `run.rs:1065-1208`) and by each node after
  every event (`Meta::pending_replays()`, the core's `clients` map via
  `CoreView`); a rid with copies on two nodes at once (a `Client` copy
  and a `Queued` copy on the same node with `also_client` merge into
  one) is a violation under `SimConfig.strict_identity` (new). **I2c**
  (verbatim): `check::replay_log` additionally records, per rid, the
  records and origin of the segment row that completed it; at the end,
  for every rid answered OK under a RootGroup/DelegGroup class, the
  completing row's records must equal the records the answer named
  (`AckLog` keeps them) and its origin must be the original identity —
  violated today by every adopted tail (new jseqs, re-application).
- **I4 oracle** `check::no_late_landing(in_doubt_log, oracle, history)`:
  for every `InDoubt` answer `(node, rid, t)` and a later completion of
  `rid` in the log at `(seq, i)`, every op invoked by that node's
  clients after `t` and completed in the log must be at a later
  position; violation under `SimConfig.strict_settle` (new). Reports
  `late_landings`.
- **I5 oracle**: `tentative` rids (`node.rs:928-944`) whose answer was
  not class Lone (from `AckLog`) are counted (`RunReport.non_lone_rollbacks`)
  and, under `strict_durability`, fail — today `acked_rolled_back` is
  only reported (`run.rs:2685-2718`).
- **The sim's `RebuildReplica`** (`node.rs`, the stub answering
  `ok: false`) runs the production rebuild: a fresh `Meta` from the
  bucket's commit chain and log (the code path of
  `authority_driver.rs:5455` `rebuild_replica` →
  `Meta::replace_ns_from_rebuilt`, `crates/meta/src/store/bootstrap.rs:323`),
  then `Event::RebuildDone { ok: true }`. Fail-before: the paused seed
  configs log "rebuilding the deposed replica from the log failed"
  repeatedly on the old stub; with the rebuild the deposed root recovers.
- **The operator escape in the sim** (for Q1's strict wait, used from
  C1): a `FaultKind::Operator(AbandonGroup { epoch | gen })` that the
  sim fires once a claim or a generation end has waited
  `SimConfig.abandon_after_ms` (default: never; the double-fault configs
  set it) — through the same control path as `constellation repair
  abandon-group` — and a run classification `waiting_for_group` for a
  run that ends with a claim still waiting and no escape configured.
  The strict checks accept a `waiting_for_group` run (they check what
  converged, and that nothing acknowledged was lost or rolled back) and
  classify an abandoned group's branch as operator-caused (I5(b)), so a
  double fault under the strict rule neither times out nor passes
  vacuously.
- **Models** (fail-before for later milestones, recorded now):
  - `crates/model/src/backup.rs`: property `acked_never_rolled_back`
    (no write acknowledged under a non-`Local` policy is ever in a
    replay set or a rolled-back journal), knob `commit_rule: bool`
    (C0: `false`, today's protocol). New test
    `today_a_deposed_holder_replays_committed_rows` (2 nodes, crash of
    the backup's link, takeover): **violates** with `commit_rule:
    false`. C1 implements `true`.
  - `crates/model/src/delegation.rs`: property
    `acked_ops_are_never_replayed`; test
    `today_a_delegate_without_a_backup_acks_alone` violates
    (`backups: false`, the delegate crashes before the root appends).
  - `crates/model/src/flex.rs`: property
    `hold_owner_never_deposed_except_by_retire` (a live hold owner's
    epoch journal is stranded only by an operator's retire, or belongs
    to an epoch that carried no lease) checked in `full_rule_is_clean`'s
    config. §11 Q3 is **decided** (non-stealable except retire, §5.7);
    the property checks the decision, and a counterexample stops C0.
  - `crates/model/src/protocol.rs`: property `eio_never_lands_late` and
    knob `abandon: bool` (C0 `false`); test
    `today_an_in_doubt_op_lands_after_its_eio` violates (the requester's
    deadline passes, the deposed holder's replay lands afterwards).
- **Bench**: a new measurement scenario `ack-latency` (harness,
  `crates/harness/src/scenarios/commit.rs`, in `CAMPAIGN` — run by name,
  measure-only, prints JSON): per topology and op, p50/p90/p99 over 300
  ops each of `create`, 4 KiB `write`+`fsync`, `rename`, `unlink`,
  `chmod`, and the first `flock` from a non-sequencer:
  1. one node;
  2. two nodes on loopback: the holder's own ops and a forwarded op (the
     other node), `ack=backup` automatic;
  3. two nodes: node a the delegate of its own directory, the root on
     node b (a's ops; b's forwards into a's directory);
  4. three nodes: a delegate with a backup;
  5. two nodes under `net:delay 60ms 20ms` (`CONSTELLATION_HARNESS_LOAD=net-wan`):
     the lone root's own ops and a forwarded op;
  6. `ack=s3` holder (reference).
  `bench/commit-rule/run.sh` runs it (release build, 5 runs; p50 and
  p90 gate, p99 is reported — five runs cannot gate a p99) and writes
  `bench/commit-rule/RESULTS.md`. The state dir is on ext4 by default;
  the four-file-system matrix (ext4, xfs, btrfs, tmpfs) runs at C0 (the
  baseline) and at the milestones that change what the journal does on
  disk — C1 (the journal row's shape), C3 (the apply path of a tail) and
  C4 (journal rows per lock decision) — and at C7a (clock reads). **C0
  records the baseline**, plus the
  existing gates' baselines: `make perf-gate`, `make vfs-bench`,
  `harness meta-bench` (`3node-p2pon-shared-*`, `1node-*`),
  `lock-latency`, `nonowner-op-latency`, `delegated-op-latency`,
  `cto-latency`, `backup-failover`'s failover time, `make
  perf-regression` (full, latency250, bw50).
- **Docs**: DESIGN §4's vocabulary paragraph (authorized); TESTING.md
  (the oracles, the flags, `ack-latency`); ADR-33's first draft (status
  "proposed", decision text only).

**Fail-before (non-vacuity of the oracles).** Each new oracle must find
today's states, or it proves nothing later. On `05ef4c9` (plus the
recommended interim fixes), with the strict flags on:
- I1: `long-delegated` ×2000 and `delegated-backup` ×2000 report
  uncommitted DelegGroup answers (a delegate with no backup acks
  alone); `far_config` (`no_peer_in_budget_is_todays_behaviour`,
  1000..1020) reports Lone-class answers to remote requesters.
- I2: `backup-crash`'s pinned seeds (`regression_backup_crash_classes`)
  and `delegated-holder-cut` (seeds 2, 22, 330, 1719, 772, 1039) report
  two copies (a deposed holder's queue and the requester's retry; a
  successor's replay of its stranded delegate rows); I2c fails on any
  `backup_failover_reships_the_tail` seed with a re-shipped tail.
- I4: `long-delegated-late-answers` ×2000 and `long-backup` ×1000
  report `late_landings > 0`, or C0 constructs a pinned seed that does
  (a `CutS3Holder` + `CrashHolder` pair under `acquire_deadline_ms`
  short enough that the requester's deadline passes before the
  successor's drain), recorded as `regression_c0_late_landing_seed_N`.
- I5: `backup_no_acked_op_lost` (700..1300) reports
  `non_lone_rollbacks > 0` (the transient shadow rollbacks the test's
  comment describes).
- The four model tests above violate.
- Baseline for C6 (the review's M4): `backup-two` (§7 C6) under
  `strict_commit` on C0's code reports I1 violations in the add window
  of a second backup (rows acknowledged by `{B1}` after the add-CAS
  listing B2 landed).
An oracle that finds nothing it should is a C0 bug: fix the oracle, not
the expectation.

**Also in C0 (§11 Q20 (h), decided 2026-10-20).** "Applied, unfenced"
gets one meaning for every reader: `Core::start` and both rebuild arms
seed `ship.max_epoch` from the replica's applied position
(`Replica::applied_epoch`, the epoch of `KV_APPLIED_POS`; a fenced
segment does not move it), and `shipper::replay_from` (every bootstrap
and every rebuild) skips a superseded-epoch segment as the core's tail
does and records the applied position. Before, a restarted or rebuilt
replica fenced from 0. The only behaviour change of C0.

**Exit criteria.** The oracles, flags and counters in; every existing
test green (no behaviour change; the flags default off); the baseline
table of every oracle per config in PROGRESS (counts per class, per
seed range); the sim rebuild in, with the paused seeds' configs
recovering; the four model counterexamples recorded; Q3's property
checked; the performance baselines recorded on four file systems.

### C1 — A backup-acknowledged row is committed (the root group)

**Code** (§5.2): `JournalTx.gated` and `JournalTxHead.gated`
(`crates/meta/src/store/local.rs`), `journal::PendingGate`
(`crates/meta/src/store/journal.rs`); `Meta::strand_below_epoch` splits
gated rows into the member copy (`crates/meta/src/store/spec.rs:3136`;
member-copy rows in `crates/meta/src/store/backup.rs` under the node's
own `(epoch, first)`, a new `BackupRole`-like marker
`member_copy:{epoch}` in `local`); `abort_durable_parks` stops marking
gated rows `unacked` (`readindex.rs:880-985`); `LiveEntry::stranded_by_epoch(epoch,
tail_follows)` and `Shadow`/`Hint`'s `gated` (`spec.rs:146-282`);
`MutateReply.committed` (`crates/authority/src/event.rs`); `JournalTx.tail_of`
and `TailFollows { prev_epoch, through }` (`record.rs:245-253`,
`jobs.rs:4242-4247`; the ship plan writes `tail_of` as an origin —
until C3's `Origin` enum, as a second parallel vector
`tail_origins: Vec<(u64, u64)>` in the envelope, folded into `Origin`
by C3); `PeerMsg::SealTail`/`SealedTail` (`event.rs`; engine transport
in `crates/engine/src/p2p.rs` and `authority_driver.rs` as a
request/response like `DelegSeal`, timeout `backup_takeover_ms`); the
claim path (`LeaseState::classify`, `crates/authority/src/core/lease.rs:306-394`:
a non-listed claimer of an expired `Backup` lease, and a listed backup
claiming at expiry without a seal, go through a new job phase
`Phase::TailFetch` before `Acquire`); the holder's self-seal on
`SealTail` (`mark_lost` with `self_sealed`, persisted with
`Meta::backup_seal`); `status.ack.member_copies`,
`status.lease.waiting_for_group`; the stale module doc at
`backup.rs:27-36`. For Q1's strict wait (recommended):
`constellation repair abandon-group` (`crates/cli`, the control method in
`crates/control`, the engine binding) and `LogRecord::GroupAbandoned`.

**Deleted.** Replay-by-rid of gated root rows after a deposition; the
transient rollback of committed shadows and streamed entries at a seal
takeover; the "a double fault: the tail is lost" TTL claim of a `Backup`
lease (`lease.rs:345-356`'s comment and the sim's corresponding
`budget_exceeded` relaxation for this case).

**Fail-before tests.**
- Sim, `strict_durability` now fails on `non_lone_rollbacks > 0` and
  `strict_identity` is on for every `strict_durability` config:
  `backup_no_acked_op_lost` (700..1300), `backup_failover_reships_the_tail`
  (800..1400), `ack_s3_no_acked_op_lost`, `backup_departs_reconfigures`,
  `backup_partition_reconfigures_or_seals`,
  `fast_failover_with_delegations_keeps_close_to_open`,
  `pre_s3_streaming_installs_and_retires`, `regression_backup_crash_classes`
  (whose double-fault seeds 609417 and 802797 must now be clean, not
  `durability_budget_exceeded`, when one member survives; when none
  survives for the rest of the run they end `waiting_for_group`, or,
  with `abandon_after_ms` set, as an operator-abandoned group) — **fail
  before, pass after**. `long-backup`×5000, `long-acks3`×2000,
  `backup-hot`×2000.
- New config **`backup-group-cut-ttl`** (3 nodes; 1 the holder, 2 its
  backup (RTT within budget), 3 beyond budget; at 1.5 s `CutS3` on 1
  and 2 and `Partition {1,2} | {3}` for `lease TTL + 4 × backup_takeover_ms`,
  then heal; workload on 1 and 3): before — 3 claims by TTL without the
  tail, 1's acknowledged rows come back as replays and conflict copies
  (`non_lone_rollbacks > 0`); after — 3 waits (Q1 strict) or proceeds
  per Q1, then fetches the tail from 1 or 2 at the heal, re-ships, no
  rollback, no conflict copy. ×2000; variants `-holder-only` (2 crashed:
  3 fetches from 1) and `-backup-only` (1 crashed: from 2).
- **A candidate's tail discarded under the holder's listing CAS** (found
  by C0's baseline: `long-backup` seed 892, the one `root-group` answer
  of ×1000 that no group held; same at `e5e7c20`). The holder brings
  node 1 up as a candidate and counts its acknowledgements; node 1,
  resuming from a stall with its old role and the holder's lease past
  the expiry it knew, reads the lease *before* the CAS that lists it
  lands, finds itself unlisted and discards its tail (`backup.rs`, "no
  longer a listed backup"; deliberately no seal, so the holder may
  invite it back); the CAS listing `[1]` lands and `durable_jseq` counts
  the acknowledgements given before the discard, so the parked answer
  leaves (there a refusal, `ENOENT`; an `OK` takes the same path) while
  the only listed backup's tail lacks the row. The holder's crash then
  loses an acknowledged row: a durability hole of today's protocol, not
  only an I1 classification. C1 closes it with the root group's commit:
  a listing CAS counts only acknowledgements of the candidacy it lists
  (a candidate that discarded its tail answers the next append so that
  `peers[n].acked` restarts), or a candidate whose listing is pending
  keeps its tail. Fail before: `long-backup` 892 under `+commit`
  (`uncommitted_acks["root-group/refused:ENOENT"]`), and a core unit test
  of the discard-then-list order.
- Core unit tests (`crates/authority/src/core/tests.rs`):
  `a_deposed_backed_holder_queues_no_replay_for_gated_rows`,
  `a_seal_takeover_keeps_a_committed_shadow_until_the_tail_lands`,
  `a_ttl_claimer_fetches_the_sealed_tail_before_its_marker`,
  `a_holder_that_answered_seal_tail_acknowledges_nothing_more`,
  `a_listed_backup_seals_before_claiming_at_expiry`,
  `no_member_answering_the_tail_fetch_holds_the_claim` (per Q1). Meta:
  `strand_below_epoch_moves_gated_rows_to_the_member_copy`,
  `a_tail_origin_retires_a_streamed_entry`,
  `the_member_copy_is_dropped_when_the_archive_carries_it`,
  `a_marker_without_tail_follows_drops_the_member_copy` (M1),
  `seal_tail_for_another_epoch_or_config_version_is_refused`,
  `a_self_sealed_holder_recovers_and_claims_its_own_expired_lease`,
  `sealed_tail_is_sent_in_batches` (M2). Each fails on the parent.
- Model: `backup.rs` with `commit_rule: true` (deposed holder keeps a
  member copy, successor re-ships, TTL claimer fetches): new tests
  `commit_rule_two_nodes_is_clean`, `commit_rule_three_nodes_is_clean`
  (`acked_never_lost`, `ack_order_is_log_order`,
  `acked_never_rolled_back`); `today_a_deposed_holder_replays_committed_rows`
  keeps violating with the knob off; a new
  `a_ttl_claim_without_the_tail_fetch_rolls_back_an_acked_write`
  violates with the fetch disabled.

**Harness.** Must pass 3×: `backup-failover`, `backup-failover-with-delegation`,
`backup-departs`, `backup-partition`, `no-peer-in-budget`,
`single-node-unchanged`, `ack-s3-failover`, `deposed-reintegration`,
`deposed-reintegration-backup`, `holder-publishes-log-prefix-backup`,
`backup-takeover-holds-missing-chunks`, `backup-takeover-drops-held-chunks`,
`root-failover-with-delegates`, `slow-s3-no-seal`, `holder-kill-rejoin`.
New **`backup-group-isolated`** (default catalog; `m9.rs`): a and b on
the harness's LAN, c a third node; a and b's S3 cut by toxiproxy and
their P2P to c dropped by the `net:` item past the lease TTL; c's
writes wait (Q1) and resume after the heal by tail fetch; ledger clean,
`speculation.stranded_replayed == 0` on every node, no conflict copy.

**Campaign.** Phase 3 cells for `backup-failover*`, `backup-partition`,
`backup-group-isolated`, `slow-s3-no-seal`, `load-probe` under
`pause-takeover`, `step-pause`, `runtime-hog:4:200@b`, `net-flap`,
`s3-jitter`, `clock-skew`, seeds 42–44: no failure; the counters
`stranded_replayed`, `replay_conflicts` and the new `non_lone_rollbacks`
(harness `verify::counters`) are 0 on every group tenure.

**Performance.** C1 changes no acknowledgement wait. Gate: `ack-latency`
topologies 1–6 within ±3 % (or one IQR) of C0's baseline on the four
file systems; `make perf-gate` green; `meta-bench` neutral;
`backup-failover`'s failover time within +10 % (the tail fetch adds
nothing to the seal path). The TTL path gains one round trip plus a
synced seal at a member: reported in `holder-kill-rejoin` and
`ack-s3-failover` against C0, within +10 % of their failover time.

**Exit criteria.** Every listed sim config green at the counts with the
strict flags; the new config and variants green; the model tests; the
harness and campaign cells; the performance gate; DESIGN §4 (Local
store, One log, Positions and log streams), §9 (Seal-based failover,
Other failures) and the reference pages updated for C1 (§6); ADR-33
accepted with C1's decision; PROGRESS.

### C2 — No acknowledgement outside a group where one exists; settle before `EIO`

*Evidence (2026-10-20):* `delegated-long-delegation` seeds 11, 86, 200
(and 13 more of 300): a delegate cut off from the root but still inside
its grant refuses a resubmitted rid (`EEXIST`/`ENOENT`) from rows the
root never received; the root later ends the generation and executes the
parked copy of the same rid. The application saw a refusal for an op
that took effect. The simulation's oracle counts the lone delegate's
refusal as tentative (no backup); C2's rule removes the answer itself.

**Code** (§5.3): `Recall.through` and the recorded void cut (§5.3
item 7: `record.rs:243`, `end_generation` `delegate.rs:3164-3238`,
`replay.rs:782-826`, `session.rs` `void_stream`, `note_void_cut`,
`seed_generations`), and the end-of-generation seal of every backup a
record named; the delegate's `removal_safe` before it requests a
backup's removal; the delegate's fast path in the root's write-then-wait
shape (§5.3 item 8: `DelegateView::admit_for`, `write_gate.rs`,
`SessionState::wait_deleg_durable`); `AckClass` and `Core::ack_class` (`backup.rs`),
carried on `ParkedWhat::{Reply, Finish}` and the immediate reply path,
counted in `status.ack.classes`; `Core::lone(gen)` (from link RTTs and
`backup_rtt_budget_ms`); `LogRecord::DelegGroup` (`record.rs`, applied in
`crates/meta/src/replay.rs` into the delegation row,
`crates/meta/src/delegation.rs`), journaled by `on_deleg_renew`
(`delegate.rs:2170-2269`) when `DelegRenew.backup` differs; the
delegate's `deleg_stream_durable_with` rewrite and
`deleg_fast_path_gated` = not lone (`delegate.rs:411-413`,
`:1096-1143`; the engine's `DelegateView::set_gated`,
`authority_driver.rs:1806`); `DelegateStreamAck` acknowledgements count
toward the delegate's commit (no wire change: `through` already is the
root's journaled cursor); the holder's Archive wait for forwards to a
lone root (`holder.rs:285-405`); `MutateReply.committed` set for every
committed class; `DurableWait::Ungated` handling (`session.rs:1286-1308`,
`write_gate.rs:498-537`); `MutateOp::Abandon`, `Code::Abandoned`
(`crates/types/src/errno.rs`, `crates/frontend-fuse/src/reply.rs` maps it
to `EIO`), executed in `crates/meta/src/mutate.rs` (`execute_inner`:
outcome if any, else `journal_refusal(rid, Abandoned)`), the deadline
path (`on_client_deadline`, `client.rs:2463-2518`: submit `Abandon`
under the rid; `finish_in_doubt` only on the second deadline;
`stats.client_unsettled_eio`); `Recall` strand drops (not replays)
uncommitted delegate rows (`spec.rs:275-283`, `:3068-3102`,
`strand_recalled_speculation` `:3221`); `status.delegation.mine[].ack_class`;
the ledger (`crates/harness/src/verify.rs`): a new
`Ledger::with_lone(bool)` — a scenario declares whether its topology
can have a lone sequencer (it reads `status.ack.lone` of every node at
the start and after every fault); `false` removes the late-landing
allowance (`late`, `verify.rs:330-338`, `failed_create`/`failed_rename`
`:454-484`), so any late landing is a violation; `true` keeps it for
the lone branch only.

**Deleted.** A delegate's acknowledgement on its own disk when it is not
lone (`deleg_stream_durable_with`'s `None => true`); Layer A
acknowledgements to remote requesters (the requester's replayable shadow
for them); the `Ungated` local-only OK on a non-lone node; the unsettled
`EIO` at the first deadline; replay by rid of a delegation's rows past
its cut.

**Fail-before tests.**
- Sim with `strict_commit: true` (and `strict_identity`, `strict_settle`):
  `long-delegated`×5000, `long-delegated-backup`×5000,
  `long-delegated-late-answers`×10000 (the config that found seeds 9287
  and 2945; their pinned regressions from the interim fixes stay green),
  `delegated-backup`×2000, `delegated-crash`×2000,
  `delegated-partition`×2000, `delegated-root-crash`×2000,
  `delegated-root-gone`×2000, `delegated-delegate-restart`×2000,
  `delegated-two-gens-root-crash`×2000, `delegated-holder-cut`×2000,
  `shared_dir_*` ×500 each, `far_config`×1000, `long-sessions`×2000,
  `flex-crash`×2000 — **fail before** (uncommitted DelegGroup answers,
  Lone answers to remote requesters, late landings), pass after.
- New config **`tloss-two-node`**: the 2026-10-16 trace. Two nodes
  (`deleg_backup_core_config`, so each is within the other's budget);
  node 2 holds the delegation of `d1` and is the backup of root 1; workload
  on node 2: per file in `d1`, a size-only `Setattr` (the `O_TRUNC`)
  then a `SetManifest` (write + fsync) — a new workload op
  `TruncCommit` in `run.rs` (`SimConfig.trunc_commit_ratio`); faults:
  `StallStep { node: 1 }` timed after its `apply_delegate_tx` of node
  2's rows and before its backup stream (a new `StallStep` trigger
  `after_event: DelegateStream`), long enough for node 2 to seal and
  take the root over; then 1 resumes deposed. Checks: each file's final
  size and manifest are those of the last acknowledged commit (a new
  `history.rs` check for `TruncCommit`, against the oracle's replay),
  I1, I2, I4. Before C2: I1 fails (node 2's
  acknowledgements of T and M rest on its disk alone); with the
  takeover-loss fix landed the data check passes but I2 fails (node 1's
  `Records` copies queued while node 2 replays) — I2 passes only after
  C3. ×5000.
- New config **`delegated-no-backup-root-near`** (`delegated_config`
  with `backup_rtt_budget_ms` large enough for the root only, i.e. the
  delegate's only in-budget peer is the root): I1 fails before (Layer A
  delegate acks), passes after. ×2000.
- Core tests: `a_delegate_without_a_recorded_backup_waits_for_the_roots_append`,
  `a_delegate_counts_a_backup_only_after_its_deleg_group_record`,
  `a_removed_backup_is_needed_until_the_record_without_it_is_applied`,
  `a_lone_delegate_acks_its_own_node_and_waits_for_the_root_for_others`,
  `a_lone_root_answers_a_forward_from_the_archive`,
  `an_ungated_fast_path_wait_on_a_non_lone_node_waits_for_the_archive`,
  `the_deadline_abandons_the_rid_before_eio`,
  `an_abandon_after_the_tail_reship_answers_the_late_success`,
  `a_recalled_delegates_uncommitted_rows_are_dropped_not_replayed`,
  `a_lone_roots_forwarded_row_never_enters_the_replay_queue` (H1),
  `an_ungated_wait_in_an_epoch_hold_answers_at_once` and
  `an_unexpected_ungated_wait_is_counted_and_resubmitted` (H2),
  `a_backup_is_dropped_from_the_record_only_once_its_rows_are_covered`
  and `the_generation_end_seals_every_backup_a_record_named` (M3),
  `the_abandon_reuses_the_rids_client_entry`,
  `the_abandons_refusal_waits_for_its_commit`,
  `an_abandon_at_a_new_delegate_dedups_the_delayed_copy_at_the_root`
  (M7), `the_recall_record_carries_the_cut`,
  `the_delegates_fast_path_writes_then_waits_for_its_group`. Meta:
  `abandon_answers_an_existing_outcome_and_refuses_otherwise`,
  `a_deleg_group_record_updates_the_table_row`,
  `a_void_cut_is_the_records_through`.
- `far_config` ×1000 under `strict_identity` (H1: the lone root's
  remote-forward rows): fails before (the deposed lone root queues them
  while the requester retries), passes after.
- Model: `delegation.rs` design with `ack_via_root: true` (new knob;
  `false` = today) — `acked_ops_are_never_replayed` clean on
  `design_local_writes_are_clean`'s, `design_forwarded_writes_are_clean`'s,
  `design_delegate_crash_without_backup_...`'s and
  `design_unreachable_delegate_is_outwaited_...`'s configs (the two
  latter tests are renamed to `..._loses_no_acked_op` and
  `..._fails_unacked_ops` — rename authorized, their old assertion,
  "replays by rid", encodes removed behaviour); `protocol.rs` with
  `abandon: true`: `eio_never_lands_late` clean on
  `recovery_fixes_bug_b_*`'s configs; the C0 counterexamples keep
  violating with the knobs off.

**Harness.** 3×: `delegated-subtrees`, `cross-subtree-rename`,
`delegate-crash`, `delegate-crash-default-ttl`, `marker-order`,
`delegate-partition`, `delegate-crash-backup`, `delegate-root-loss`,
`delegate-root-blackhole`, `delegate-root-loss-ttl`,
`delegate-handoff-renewal`, `delegate-backup-handoff-failover`,
`root-failover-with-delegates`, `auto-placement`,
`designation-as-delegation`, `shared-dir-multi-writer`,
`hash-range-split-merge`, `cross-range-rename`, `forward-timeout-reexec`,
`holder-crash-phantom-shadow`, `holder-crash-phantom-new-holder`,
`takeover-marker-strands-promptly`, `inbox-*`, `p2p-partition-*`,
`delegated-op-latency`, `nonowner-op-latency`. New
**`takeover-loss-two-node`** (default catalog, `delegroot.rs`): two
nodes, a the delegate of its own directory and the backup of root b
(placement forced with `constellation delegate`), b paused across
`takeover` (`Client::pause`) while a overwrites 10 files (`O_TRUNC`,
write, `fsync`); every fsynced file byte-exact on both nodes after
quiescence, no conflict copy, `ack.classes` on a shows no Lone answer.
Fails on `05ef4c9` (the loss); with the takeover-loss fix landed it
still fails the class assertion; passes after C2.

**Campaign.** `load-probe` under `pause:lock-lapse+500:10000@b` (the
profile that found the loss) ×20 runs; phase 3's `delegate-*`,
`root-failover-with-delegates`, `delegate-paused-across-ttl`,
`two-clients-shared`, `p2p-partition-*` under every phase-3 profile,
seeds 42–44; the ledger **without** the late-landing allowance on every
non-lone topology; the errno classifier's in-doubt class must be 0
except `client.unsettled_eio` (which must be 0 in every cell: no cell
isolates a requester for twice the deadline).

**Performance.** The only new waits: a delegate with no recorded backup
waits one round trip to the root (DelegateStreamAck); a lone root's
answer to another node waits for the archive. Gates:
- `ack-latency` topology 3 (delegate, root on the LAN): p50 and p90 no
  more than topology 2's (a backup-gated holder) + 0.5 ms on loopback;
  topology 4 within ±3 % of C0; topologies 1, 2, 6 within ±3 %.
- `delegated-op-latency`: every non-owner's median still under half an
  S3 round trip (the scenario's own bound); the delegate's own median ≤
  C0's topology-2 holder-own-op p50 + 0.5 ms (relative, not absolute:
  plan 30 M9 measured one LAN acknowledgement at +2.0 ms p50 on
  loopback, 1.9 → 3.9 ms, so the delegate moves from local-disk cost to
  a backed holder's cost — the regression the maintainer accepts or not
  at §10's gate G1).
- Topology 5 (WAN, lone root): own ops within ±3 % (Lone); forwarded ops
  = C0 + one archive commit (expected 28–190 ms by S3 distance,
  measured, reported; accepted per §11 Q2, not a regression of the
  gate).
- `make perf-gate`, `meta-bench` `3node-p2pon-*` within the 20 %
  tolerance and reported against C0; four file systems.
If the delegate-without-backup cost exceeds the bound, the fallback
recorded in §11 Q4 (the root as the delegate's recorded backup: a
backup append instead of the stream append, same copy count) is
measured before anything is loosened.

**Exit criteria.** All of the above; DESIGN §1, §4 (Request ids,
Acknowledgement and durability, Delegated sub-sequencers), §5
availability matrix, §6 session guarantees, §9 (Nobody observes,
Delegates), §14, GOALS untouched yet (C5); reference pages; ADR-33
amended, ADR-2/14/18/23 amended; PROGRESS.

### C3 — Verbatim re-ship; no re-execution of a committed row

**Code** (§5.4, §5.6): `Origin` in the segment envelope
(`crates/authority/src/segment.rs`; readers in `apply_segment_rows`,
`journal_origins`, the harness's log checks `crates/harness/src/verify.rs`
`s3_log_check`, `crates/chaos`); `apply_backup_tail`'s verbatim append
and apply, `MetaError::TailDiverged`; the delegate-turned-root appends
its own rows as generation rows before its `Recall`
(`complete_gate`, `jobs.rs:1179-1265`; `delegation_sync`,
`delegate.rs:611-615`); the deposed root's member copy of other
generations (`recover_deposed`, `jobs.rs:1381-1410`; `backup.rs`
`DELEG_BACKUP_BASE`); `DelegSeal` to previous roots for an inherited
generation without a recorded backup (`on_deleg_expiry`,
`delegate.rs:3010-3117`); the deletions below.

**Deleted.** `folded_into_later_manifest` (`replay.rs:75-99`, `:140-144`,
`:533-537`), `rebase_on_truncate`, `truncate_pre_manifest`,
`QueuedReplay.truncate_base` and the takeover-loss fix's fold
replacement — except as the lone branch's replay shape (C5); the foreign
`Streamed` `Records` replay (`spec.rs:1288-1307`); `PendingLocalOp =
Records` for appended delegate rows and adopted tails (`local.rs:850-857`);
`reset_for_rebuilt_ns_tx`'s replay queueing for captured journals
(`spec.rs:2016-2052`); `strand_for_takeover`'s `own_gens`, the
`deleg_stranded` mark and everything that reads it (`96f4565`); the
dedup-by-rid skip in the tail re-ship (becomes an assertion);
`settled_outcome`'s `lost && journal_carries_outcome` branch; the 2945
fix's `QueuedReplay.deps` and its `submit` merge; `Held` answers for
`deps_lost` outside the lone branch (a dependency on a committed row is
never lost).

**Fail-before tests.**
- Sim, `strict_identity` with **I2c (verbatim)**: `backup_failover_reships_the_tail`
  (800..1400), `regression_backup_crash_classes`, `delegated-holder-cut`×2000
  (its regression test `regression_delegated_holder_cut_takeover_keeps_delegate_rows_in_log_order`
  asserts `local_rolled_back > 0` on seeds 772 and 1039: rewritten to
  assert the successor appended its own rows as generation rows,
  `deleg_appended_after_takeover > 0`, and `local_rolled_back == 0` —
  rewrite authorized), `delegated-two-gens-root-crash`×2000,
  `long-delegated-late-answers`×10000, `long-delegated-backup`×5000,
  `tloss-two-node`×5000, `long-backup`×5000 — **fail before** (re-applied
  tails with new identities, `Records` copies, own-row replays), pass after.
- The deposed root's rebuild-then-replay, as C0's real sim rebuild first
  ran it: at `5f3a0e8` + C0, `long-delegated-late-answers` seeds 403 and
  900 failed (403 linearizability, a `d2` unlink; 900 a marker seen
  without its data, the data's create replayed by rid at t=11 272 after
  the marker was accepted at t=10 493) — the deposed root's recovery
  finished only because the rebuild now succeeds, queued the ops of its
  uncaptured journal for replay by rid (5 in 900, 8 in 403), and the
  replays ran seconds later, after the clients' next ops. Not the cursor bug
  (`1e107f0` alone leaves both failing); `97e806b` hides both by
  changing the schedule (×1000 at `e5e7c20`: none). C3's sweep of
  `long-delegated-late-answers`×10000 under the real rebuild is where
  this class must be gone.
- Core tests: `the_takeover_gates_local_replay_folds_a_truncate_into_its_manifest_commit`
  (`core/tests.rs:1931`, pins the deleted fold) is rewritten as
  `a_delegate_taking_the_root_appends_its_own_rows_under_their_generation`,
  covering the overlap (`(g, 5)` both in the tail and among the moved
  rows: skipped, record-identical), the drop above a tail's own `Recall`,
  and seed 772's order (the tail's rename of a name out of `d1` after
  its recall, the moved create above the cut dropped);
  `delegated-holder-cut` seeds 772 and 1719 are the sim fail-before;
  `a_restarted_delegate_re_adopts_a_stranded_generation_stopped` and
  `the_takeover_gate_stops_a_generation_whose_rows_it_stranded`
  (`:8561`, `:8596`) are replaced by
  `a_takeover_by_a_delegate_leaves_no_hole_in_its_generation`; the
  takeover-loss fix's tests are rewritten as "the state cannot arise"
  assertions (`no_records_copy_of_a_committed_row_is_ever_queued`,
  `a_deposed_root_keeps_appended_delegate_rows_as_a_member_copy`,
  `a_member_copy_is_served_to_the_next_roots_seal`). New:
  `a_sealed_tail_is_applied_verbatim`, `a_diverging_tail_is_an_error_and_keeps_the_gate_closed`,
  `a_deposed_root_never_asks_for_a_rebuild_with_a_captured_journal`.
- Model: `holder_side.rs` `recovery_deposed_holder_rolls_back_and_replays`
  stays for `Protocol::Recovery` (historical); a new protocol variant
  `Protocol::Commit` in `crates/model/src/protocol.rs` (deposed holder
  keeps a member copy; successor re-ships verbatim; requester settles at
  the deadline) with `commit_protocol_is_clean` (`linearizable`,
  `converged_at_quiescence`, `no_rid_executes_twice`,
  `eio_never_lands_late`) on the bug-B configs of `today_bugs.rs`.

**Harness.** Everything of C1–C2 again, plus `deposed-reintegration`
(lone: still replays and copies the true overlap) and
`takeover-loss-two-node`, `chaos-ci`, `chaos-ci-strict`,
`stress-ng-fs-nodes` (known-bug repro: report).

**Campaign.** The full `smoke` (with every deliberate break), phase 2,
phase 3 and `known-bugs` matrices, seeds 42–44; the soak (chaos
coordinator, `stress-ng-fs-nodes`) 2 × 3 h with the rotating injector
schedule (`tools/load-campaign/inject.sh rotate`); every checker clean.

**Performance.** The ack path is unchanged from C2. Gates: `ack-latency`
all topologies ±3 % of C2 (four file systems: the tail apply changes); `backup-failover`'s failover time ≤ C0 + 10 %
(verbatim apply is no more work than re-application);
`root-failover-with-delegates` failover time ≤ C0 + 10 %.

**Exit criteria.** All of the above; DESIGN §4 (One log, the replica
section), §6 Staleness, §9 Seal-based failover and Other failures;
`forwarded-mutations.md`'s recovery sections; ADR-19 "superseded in
part"; PROGRESS with every deleted function and the test that pinned it.

### C4 — Lock and delegation state into the log

**Code** (§5.5; `Recall.through` landed in C2): `LockGrant`, `LockRelease`,
`LockExpire`, `LockMove` (`record.rs`, appended to the enum; `TouchSet`
touches nothing; E2E: inside segments); `LockTables::apply_record`
(`crates/meta/src/locks.rs`) called from the journal path and
`Replica::apply_segment`; the owner journals each decision in the same
transaction (`lock_try_grant` `core/locks.rs:1300`, `on_lock_released`
`:2395`, the expiry/outwait paths `:1650-1668`, `lock_on_delegated`
`:4268` becomes a `LockMove` in the `Delegate` transaction,
`end_generation` adds the generation's `LockMove { gen: 0 }`, an unlink in
a delegated subtree adds `LockMove { gen: 0 }` to its stream row);
grant answers wait for the record's commit (the existing `lock_answer`
hold, `core/locks.rs:940`, gains the commit condition beside the
horizon); the successor builds its table and floor caches from the log
at its gate (replacing `lock_install_mirror` at `jobs.rs:1247`);
`DelegRenewed`, `DelegRecalled`, `DelegateStream` lose their lock fields
(`event.rs:549-641`); the model.

**Deleted** (§5.4's list): `LockMirror` and the mirror state, the
handoff/handed/moved_seen/reinstatement machinery, `LockHandback` and
the late-answer rules, the leaving-grant fields and functions,
`restamp` outside the log-derived install, `pending_returns`' remnants;
the inferred void cut.

**Carry across `LockMove` (PROGRESS "Fix: a delegation move carries what is
left of a lock grant's record").** The log-derived install restamps, which is
the 21 s per move the carry removed (sim
`locks-delegated-production-ttl-heavy`: 729 of 2000 seeds failed, 33 now). So
`LockMove` must carry, per moved grant, the remaining duration measured at the
append (`lock_left`, `core/locks.rs`), and the derived install records
`min(restamp, now + left + margin)` at the apply (`carried_until`: the apply
is after the append), never below a record the table already has of the node
on the inode (`install_keeping_later`). The derived install must not restamp
them, or the 21 s wait returns with C4. C4's model keeps `MoveStamp::CarryAll`
(`crates/model/src/locks.rs`).

**Keep (the ask-again rule, PROGRESS "Fix: a pushed lock grant is never older
than its holder's first renewal").** The rewrite of `core/locks.rs` carries
over what only withholds, orders or tells: `lock_try_grant` pushes a grant to
a waiter whose RPC is gone only from a request no older than half of
`ttl − margin` of the granted (capped) ttl, and otherwise asks the waiter to
re-send (`Waiter::ask`, `Blocked::AskAgain`, the `LockGranted` push carrying
`Waiting { retry_ms: 0 }`); the asked waiter's turn is held, with no
conflicting grant to anyone else meanwhile (`lock_reserved_against`), then it
is passed over (`lock_serve_waiters`); its node's handler re-sends idle ops
and leaves an op with a request in flight alone (`on_lock_granted_push`). A
request carries how long its application has waited (`LockRequest::waited_ms`,
`Control::Lock::waited_ms`) and is queued from there (`lock_waited_since`); a
grant installed, not recalled, answers the ops of its node that it covers
(`lock_wake_covered`); one that arrives recalled makes the others ask again
(`lock_resend_idle`); `LockCancel` drops the waiters of an op that left.
**Not in that patch, to be decided with C4's records** (safety-sensitive, a
model check first: two ops per node, a move between a push and the first
renewal): the restamp of a grant moved with its subtree to a full lock ttl and
margin (`restamp`: 21 s for one that lapsed at its holder meanwhile; the
narrow fix is the remaining duration for grants the sender knows were never
renewed and stops renewing at the take, the full restamp kept for renewed
grants, root copies, mirrors and reclaims), the residual slow-transit lapse
path, and the model's two ops per node with the holder's max-merge.

**Fail-before tests.**
- Model (`crates/model/src/locks.rs`): knob `lock_state_in_log: bool`
  (new; `false` = today's model with its message-borne `Handoff`,
  `Mirror`, `DelegRecalled` tables). With `true`, the owner's `Grant`,
  release, expiry and move are entries of a per-model log applied in
  order by every node (the delegate learns the moved grants by applying
  the entry, not from `Handoff`), and the knobs `replicate`,
  `move_state`, `handoff_in_flight`, `reinstate_handed`,
  `resend_handoff`, `handoff_grace` are ignored. New tests:
  `a_late_recall_answer_revives_a_grant_without_log_state` (knob
  `false`, `reinstate_handed: false`, a recall answer delivered after the
  generation ended: **violates** `mutual_exclusion`),
  `lock_state_in_log_is_clean` (the configs of
  `delegation_moves_with_state_are_clean`,
  `overtaken_handoffs_and_redelegation_are_clean`,
  `lost_handoffs_resent_are_clean`,
  `an_outwaited_delegate_with_reinstatement_needs_no_grace` and
  `fast_takeover_with_grace_is_clean`, all with the knob `true` and the
  handoff knobs off: clean), `lock_state_in_log_needs_the_grace`
  (knob `true`, `grace: false`: still violates — the grace guards
  windows, not state; this keeps the window rule honest). The
  `fast_takeover_with_only_the_async_mirror_violates` test stays (knob
  `false`). Also update the model's stale action → code table
  (`locks.rs:113-130` names `Core::lock_fresh_release`, which does not
  exist, and `BackupAppend { locks }`).
- Sim oracle **I3** `lock_table_matches_log` (new, under
  `SimConfig.strict_locks_log`): after every event, every node's
  owner-side grant set equals the fold of the lock records it applied and
  journaled; **fails on the parent** in every delegated or failed-over
  lock config (grants installed from `DelegRenewed`/`LockMirror` that no
  record names). New config **`locks-late-answers`**
  (`locks_unlinked_partition_config` with `deleg_answer_delay` extended
  to `DelegRecalled` and `LockRenewed`, 0.4–3 s; a new
  `SimConfig.lock_answer_delay`) ×5000.
- Regression seeds must stay green with the machinery deleted (the
  proof that the log replaces it): `regression_locks_unlinked_delegated_dbackup_random_seed_5681`,
  `regression_locks_unlinked_delegated_partition_late_recall_answer`
  (7455, 1144, 1682, 1945), `regression_stale_read_after_an_outwait`
  (276, 297, 935, 2716, 68, 81, 597, 4067, 390),
  `regression_locks_unlinked_delegated_hcrash_backup_seed_41`,
  `..._seeds_2477_2982`, `..._seeds_292_6475`,
  `lease_cap_fenced_seeds_keep_their_grants`, `regression_f1_stall_seeds`.
  `long_locks` with `AUTHORITY_SIM_SEEDS=1000` (14 configs) and every
  `locks-*` config ×2000 with `strict_locks_log`.
- Core tests: `a_grant_is_answered_after_its_record_commits`,
  `a_delegation_moves_its_subtrees_grants_in_the_delegate_record`,
  `a_late_recall_answer_installs_nothing`, `a_fast_successor_derives_its_grants_from_the_log`,
  `a_reclaim_during_the_grace_journals_a_lock_grant`,
  `a_mode_upgrade_journals_a_lock_grant` (M9); meta:
  `lock_records_fold_into_the_owner_table`.

**Harness.** 3×: `flock-cross-node`, `sqlite-two-nodes`,
`lock-holder-partitioned`, `lock-failover`,
`lock-holder-killed-contention`, `lock-fence-at-close`,
`lock-release-at-close`, `fence-token-at-write`, `lock-lapse-lone-node`,
`lock-grant-dead-generation`, `git-under-flock`, `-gc`, `-b2b`,
`-rounds`, `-causal`, `-faults`, `cto-recall-unreachable`; new
**`lock-state-from-log-failover`** (`m14.rs`): a holder with a backup
and 50 grants out to two peers is killed; the successor's table (status
`locks.owner.grants`) equals the set the log names, before the first
reclaim; no grant granted twice.

**Campaign.** Phase 3's lock scenarios, `lock-contention-paused-holder`
(20 rounds per boundary), `lock-contention-stalled-owner`,
`delegate-paused-across-ttl`, `git-under-flock*`, under every phase-3
profile, seeds 42–44: counter exact, token order clean, errno classes,
no stall.

**Performance.**
- `lock-latency`: first lock from a non-sequencer p50/p90 ≤ C3 + 1 LAN
  round trip under a backed root (≤ +2 ms on loopback, by plan 30 M9's
  datum — the cost the maintainer accepts or not at §10's gate G3);
  cached re-lock unchanged ±3 % (no message, no record); the sequencer's
  own lock ≤ C3 + 50 µs p50 (one journal row); a contended handoff ≤
  C3 + 2 ms; lone node `--locks cluster` vs `local` ratio unchanged ±3 %.
- S3: an idle cluster with 100 grants held for 10 min issues the same
  number of PUTs as C3 (pure window extensions are not logged; `status.s3` counters);
  `git-under-flock`'s segment count within +5 %.
- Journal: four file systems, `meta-bench` neutral.

**Exit criteria.** All of the above; DESIGN §4 Locks, §6 Cluster locks;
`cluster-locks.md`, `delegations.md`; ADR-25 amended; PROGRESS with the
deletion list and, per deleted mechanism, the test that pinned it and
its fate.

### C5 — The lone branch, isolated and documented

**Code** (§5.7): `LoneBranchOp` and the enqueue assertion; the lone
branch's replay shapes (original op, conditional truncate,
`derive_replay_op`); conflict copies only from it; status
(`status.ack.lone`, `lone_acks`, `lone_branch_replays`,
`lone_branch_conflicts`, `abandoned`); the documentation of Q3's
decision for epoch holds (§5.7).

**Fail-before tests.**
- Sim oracle I5 `speculation_only_lone` under `strict_durability` on
  **every** config (not only the backup ones): before C5 the flex and
  epoch configs (`flex`, `flex-crash`, `flex-tight`, `flex_backup`)
  report non-lone rollbacks from a retired hold owner's journal unless
  its rows are classified operator-abandoned (I5(b)); after C5 clean.
  ×2000 each.
- Meta: `a_gated_row_never_enters_the_lone_branch_queue` (debug
  assertion; the parent accepts it).
- Harness: `deposed-reintegration` asserts `lone_branch_replays > 0`
  and exactly one conflict copy (the true overlap); `no-peer-in-budget`
  asserts `ack.lone == true`; `single-node-unchanged` asserts
  `ack.lone == true` and no lone-branch replay ever (a single node is
  never deposed).

**Docs.** GOALS (L17-20, L21-31), DESIGN §4 (Local store, the replica
section, Acknowledgement and durability), §5.2, §5.3, §9 (Failover by
topology), `durability-and-failover.md` (Layer A, choosing a mode, known
limits), `forwarded-mutations.md`, ADR-19, ADR-22; troubleshooting
entry "my writes came back as conflict copies" (only a lone writer
deposed while partitioned, or an operator action).

**Exit criteria.** Oracles clean on every config; docs; and **the full
CONVENTIONS gates** (fmt, clippy, `cargo test --workspace`, smoke,
integration, the full harness matrix, pjdfstest 8798/8798, PROGRESS).

### C6 — (decision, §11 Q5) Write-all to two backups on a LAN of three

`CONSTELLATION_BACKUPS=2` exists (`backups_max`); C6 makes it correct,
tested and measured, and decides the default. **Correctness first**
(the review's M4): from the moment a candidate's add-CAS is issued the
candidate counts in the gate (`durable_jseq` takes the minimum over the
committed set *and* any candidate whose add-CAS is in flight or landed
but not yet applied by `on_reconfig_put`, `backup.rs:1343-1478`), so no
row is acknowledged by `{B1}` alone once an object listing B2 can exist.
Fail-before: `backup-two` under `strict_commit` on the parent (C0's
baseline row) shows I1 violations in the add window; after, clean. A
core test `a_candidate_counts_from_its_add_cas`. Model: `backup_three_nodes_with_reconfiguration_is_clean`
(today `#[ignore]`) with `commit_rule: true` and two backups, run in the
nightly; sim config `backup-two` (4 nodes, two backups in budget, two
`CrashBackup` and one `CrashHolder` spread over the run) ×2000: no
rollback, no loss with one failure at a time, and with two
simultaneous; harness `backup-failover` with `CONSTELLATION_BACKUPS=2`
and two kills in sequence. Performance: `ack-latency` topology 2 with
two backups p50 ≤ one backup + 0.3 ms (appends in parallel). Exit: the
measurement and the maintainer's default decision in PROGRESS.


### C7 — Clock envelope: from a ±500 ms offset bound to a rate bound

The maintainer added the study's §7 "clock envelope" to this plan.
Today every safety decision that reads time reads the **wall clock**
(`store_s3::lease::now_unix_ms`, `crates/store-s3/src/lease.rs:67-73`:
`SystemTime` plus the fault offset; the core's `now` is sampled from it
once per step, `authority_driver.rs:743`), and six decisions compare an
absolute wall timestamp written by one node against another node's
clock. Both make safety depend on cross-node *offset* (±500 ms at the
default margins, `margin > 2 × drift`, DESIGN §5). The literature's
result (Gray & Cheriton's leases, Liskov 1991, Raft §6.4.1, Chubby,
ZooKeeper sessions, FaRM) is that a lease needs only a bounded clock
*rate* if each side measures from its own send (holder) or receipt
(granter) on its own clock — which is already the shape of every
derived grant here (`sent + ttl − margin` / `granted + ttl + margin`).
The inventory (22 sites) is Appendix B; this milestone is its rule set.

**Two holes in today's code, independent of the commit rule** — both
recommended to **land first as standalone integrity fixes** (before C0,
with the paused work; §11 Q10). They are not the same size: C7-H2 is a
two-line change with a bounded latency cost (a maintainer decision);
C7-H1 is the step-safe half of C7a (every local duration on a clock a
step cannot move; four commits) and gets its own reviews:

- **C7-H1: a wall-clock step extends an honoured window or shortens a
  waited one.**
  *The hazard.* Every "local" duration compares a stamp taken with
  `now_unix_ms` (a stamp written by the core, or by the engine's FUSE,
  heartbeat and lane threads) against a later `now_unix_ms`.
  - Honour side: a backward step (an NTP step, an operator's `date`, a VM
    restore) on a node that *holds* a right makes a lapsed window look live.
    The node keeps honouring a grant its owner outwaited, or answers strict
    reads after a seal. The sites are a lock holder's `HeldGrant`, the
    root's `liveness_fresh` (`backup.rs:235-242`, plan 41 L0's freshness),
    `LeaseView::usable` (`crates/engine/src/lease.rs:173-181`), a delegate's
    `until`, and a reader's held read delegation.
  - Wait side: a forward step on a node that *waits out* someone else's
    right ends the wait early. The sites are an owner's grant records, a
    root's generation outwait, quarantines, graces and a successor's
    takeover floor.
  - The F-a review measured the wait side: a +2.5 s step of the root broke
    lock exclusion in 73 of 2 000 seeds, with the beat cap and without it.
  - F-a's cap itself breaks under a forward step of the holder (row 27).
  - The ask-again patch (PROGRESS "a pushed lock grant is never older than
    its holder's first renewal") adds two wall stamps of a requester that
    belong on the WAIT clock: the `sent` of a lock request and the `upto` of
    a `LockCancel`, compared only with the same node's earlier `sent` (never
    across nodes: `waited_ms` is a duration). A backward step of the
    requester's wall clock between a request and a cancel makes the owner
    drop a live waiter (`w.sent < upto`); liveness only, its next re-send
    repairs it. C7-H1 moves both with the other `sent` stamps.
  The delegation move's carry (PROGRESS "Fix: a delegation move carries what
  is left of an unused lock grant") adds two more wait-side stamps, both
  durations: `lock_left` (the sender's `until − now` of a grant leaving its
  table, `DelegRenewed::lock_carry`, `LockHandback::carry`) and
  `carried_until` (the receiver's `now + left + margin`) are WAIT-clock
  sites, like every other record of the owner's grant table. Nothing crosses
  nodes but the duration. C7-H1 converts them with `restamp` and the grant
  records (a forward step of the sender between the grant and the move
  shortens `left`: the same early-outwait class as a step of the root's own
  grant table, covered by the same fix).
  *Specified 2026-10-19* (the coder spec is kept with the reviews). The
  fix is the step-safe half of C7a: no local duration reads the wall
  clock.
  - *Three readings per node.* `ProtocolClock` (`constellation_types::clock`,
    pure; the host's sources and the fault hooks in
    `constellation_platform::clock`) gives:
    - WALL, unchanged;
    - HONOUR = `wall₀ + RAW elapsed + S + J`, where `S` is the running
      maximum of the time suspended (`BOOTTIME − MONOTONIC`, slewed
      together, so a slew cancels) and `J` the running maximum of how far
      the wall clock has been ahead of that elapsed time. HONOUR is monotone and never under-counts. It follows a
      forward step once and ignores a backward one.
    - WAIT = `wall₀ + RAW elapsed`. WAIT is monotone,
      never over-counts, and ignores every step.

    HONOUR ≥ WALL and HONOUR ≥ WAIT, but WAIT and WALL are unordered after
    a step, so any mix with WAIT is unsafe in one direction. Each reading
    costs one to three vDSO calls, about 19 ns each, measured.
  - *The rule.* A stamp and its comparison use one clock, and three
    newtypes (`Ms` = WALL, `HonourMs`, `WaitMs`) make the compiler enforce
    it.
    - A holder's stamps and checks are HONOUR. That covers the lease
      renewal's send, `s3_sent`/`last_s3_fresh`, `FreshHold`, `HeldGrant`,
      a delegate's renewal send and `until`, a reader's `sent_at`, and the
      lease caps on grants.
    - A grantor's or waiter's stamps and checks are WAIT, and so are the
      liveness timers and the deadlines that fail a client op.
    - WALL stays for anything compared across nodes, written to S3,
      persisted, used as an identity key, or shown in status.
    - A cross-node absolute is always `wall_now + remaining` through one
      named conversion (`Now::wall_of_wait`, `wall_of_honour`,
      `wait_of_wall`). Its offset dependence is the one Appendix B already
      lists.
    - Timers become relative (`SetTimer { after_ms }`) and are computed
      from the same clock as their deadline.
  - *The core* takes a `Now { wall, honour, wait }` per step, `Core::handle`
    and `start`. This answers the review's M5: one step makes honour-side
    and wait-side checks, and each reads its own clock.
  - *The lease.* The body keeps `expires_unix_ms` on WALL, for claimants,
    the seal watch and identity keys. It gains the holder's own
    `holder_expires_ms` on its HONOUR and `holder_clock`, its clock's
    random id.
    - The holder honours only a body with its own `holder_clock`. A body
      from an earlier incarnation is not usable until the next own CAS.
    - The body is not stamped from HONOUR because, after a backward step,
      every TTL claimant would wait out the step.
  - *The beat* (row 27) carries `sent`, `expires` (the lease's honour end)
    and `guard`, all three on the holder's HONOUR, plus its clock id and
    the wall expiry for the seal watch.
    - The guard's WAIT deadlines are mapped onto HONOUR at send.
    - The successor's arrival stamp `at` is its WAIT.
    - `beat_cap` combines lags only within one clock id and takes the
      maximum over ids. `prev.holder_expires_ms` replaces the wall expiry
      in `X`.
    - A forward step then moves the holder's `sent` and its honour end
      together, which closes F-a's dependency on the holder's clock.
  - *Tokens* stay WALL, minted as `wall_now + the remaining honour window`.
    The non-minting executor's check is row 6 (C7b). The holder's own
    release wait keeps a WAIT copy taken when it pins.
  - *Persisted horizons* and the promise's `issued` are written as WALL and
    read back as WAIT. A step while the node is down still moves them, and
    so does a forward step while it is up after the last write, if a
    restart follows (the value on WAIT does not move, so nothing
    re-persists until a later grant raises the horizon); that is row 14,
    C7a (which may also re-persist on a counted forward jump).
  - *Steps are counted* (`status.clock.steps_forward/backward`, the
    distance by which HONOUR runs ahead of WALL). They are counted, never
    acted on: the clocks' definitions are the safety.
    - A forward step larger than what a held window has left ends that
      window early. That is the safe direction, but a lock owner can then
      get the owner fence's `EIO` (§11 Q19 b).

  *Fail-before:* a sim fault `ClockStep { who: Node | Holder | Backup |
  Locker, ms }` steps one node's wall reading only (the sim builds a fresh
  `ProtocolClock` per incarnation over the node's sources; the step
  survives restarts). It comes with a combined `PartitionLockerStepped`.
  The configurations:
  - `locks-root-step-forward`: the review's case;
  - `locks-locker-step-back`;
  - `locks-holder-step-back-sealed`: the S3-cut, sealed holder of
    `locks-failover-backup-renew` with strict reads. `backup-runtime-lag`
    cannot carry this case: it fails any seal and has no strict reads;
  - `fast-takeover-holder-step-forward`: F-a's forward step, on
    `locks-long-grants-seal`;
  - `fast-takeover-successor-step-forward`;
  - `delegated-root-step-forward` and `delegated-delegate-step-back`;
  - `strict-reader-step-back`;
  - two controls: a root stepped back (only longer waits) and a locker
    stepped forward (early lapses, counted, not violations).

  Each fails on its parent at a measured rate, or is strengthened until it
  does, and is clean ×2000 after. Core tests cover each clock and each
  role. A harness scenario, `clock-step-lock-holder`, steps a running
  daemon's wall clock through `CONSTELLATION_FAULT_CLOCK_STEP_FILE` (the
  load item `clock:step:<ms>:<at_ms>[@node]`). It steps the root forward,
  the holder back before a cut, and the successor forward inside its
  floor.

  *Four commits:*
  1. The clocks, the types, `Now` through the core, relative timers, the
     sim's clock model, the faults and configurations, and the step file.
     Behaviour-neutral without steps: the sweeps are identical.
  2. The honour side, with the lease body, the beat and the tokens. Fable
     review.
  3. The wait side, with the floor, the owner tables, the beat's arrival
     and guard, and the persistence conversions. Fable review.
  4. The liveness sites (client deadlines first: they fail ops), then an
     audit that every remaining wall read is cross-node, persisted,
     identity or status. **Landed.**

  1 + 2 + 3 close both measured classes; 4 closes the error-producing
  liveness sites.

  *Commit 1 landed* (PROGRESS, "plan 43 C7-H1, commit 1 of 4"): the clocks,
  `Now` through the core, relative timers, the sim's clock model and
  faults, the step file and the load item; no protocol site reads a clock
  other than WALL yet. Two things its measurements changed for the next
  commits. (1) `fast-takeover-holder-step-forward` fails by two classes at
  once, the old root's own grant table (the wait-side commit, 16 of its 18
  failing seeds, 71 of 81 before plan 44's ask-again fix) and the successor's early grant after the takeover (2): the
  honour-side commit's beat fields alone do not make it clean, and no
  variant isolating the cap alone failed at all (the root's `LockMirror`,
  restamped at the successor, covers most grants the cap would not).
  (2) `locks-holder-step-back-sealed` fails on the old holder's lock grants,
  not on a stale strict answer, and `delegated-delegate-step-back` could not
  be made to fail: both are listed in PROGRESS with the variants tried.
  A site found on the way: `lock_waited_since` (core/locks.rs, plan 44's
  ask-again fix) turns a client-reported wait (measured on an `Instant`)
  into a WALL queue key, so a step on the owner reorders the queue
  (fairness, not exclusion): a wait-side site for commit 3.
  *Commit 2 landed* (PROGRESS, "plan 43 C7-H1, commit 2 of 4"): the honour
  half. The lease body carries `holder_expires_ms` and `holder_clock`; a
  holder's lease, lock grants, delegation, read delegations, liveness proof
  and beat stamps are on its honour clock; tokens are minted on the wall
  clock from the honour window, with the holder's own release wait on a
  wait-clock copy; the beat carries the holder's honour readings with its
  clock id and the successor keeps them per id. `locks-locker-step-back` and
  `strict-reader-step-back` went to 0 of 2 000, `locks-holder-step-back-
  sealed` to 1 (a stepped-back node's wall expiry read by claimants: C7b).
  **One finding for the next commits:** the honour clock does not close the
  forward step of the *holder* in the beat cap (Appendix B row 27). It counts
  a forward step as elapsed time, so after the step `sent` has jumped by `J`
  while the end of a lease written before it has not: grants made before the
  step are capped by `X − sent`, early by `J`. Closing it needs the pair
  `(sent, X)` on a clock that ignores forward steps (the wait clock, with an
  end stamped on it in the lease body) or the honour clock's cumulative jump
  carried in the beat and the body; the simulator cannot reach it (the
  failing seeds of `fast-takeover-holder-step-forward` have no seal).
  *Commit 3 landed* (PROGRESS, "plan 43 C7-H1, commit 3 of 4"): the wait
  half. An owner's grant records and lock tables, a root's generation
  outwaits, read-delegation grants, restart and takeover quarantines, graces,
  the takeover floor, the beats' arrival, the seal watch and the persisted
  horizons (written as wall values, read back as time left on the wait
  clock) are on the wait clock; `Grant` has a wire twin (`GrantWire`) whose
  end is informational wall. **The beat cap's open item (R7) is closed:** the
  beat's `sent`, lease end and guard are readings of the holder's wait clock
  (the lease body gains `holder_wait_ms`; the honour end stays for what the
  holder honours), so `end − sent` never under-estimates what a lease has
  left whatever its wall clock did after the last renewal; the guard's mapping
  onto the honour clock is gone. `strict-isolated-reader-holder-step-forward`
  reaches it in the simulator (an S3-cut holder that keeps beating, stepped
  forward 4 s: 3 of 2 000 seeds failed before). Appendix B rows 5, 13, 14 and
  27 are fixed locally by this commit (the rate half and a step while the node
  is down stay with C7a).
  *Commit 4 landed* (PROGRESS, "plan 43 C7-H1, commit 4 of 4"): the
  liveness sites. Every timer is a wait timer (`set_timer_wall`,
  `TimerAt::Wall` and `later_than_wall` are gone); the client ops' deadlines
  and `submitted` stamps (the S3-less bound, the inbox deadline, the forward,
  acquire and own-record waits), the inbox, stream, replay, placement,
  handoff and read-index housekeeping, the lease's dwell, idle and handoff
  pause stamps, `HeldGrant.idle_since`, the two delegation timeouts commit 2
  left on the honour clock, and the engine's S3-path stall stamps (a forward
  step made a healthy path look stalled, which sends ops down the S3-less path
  and fails them at its deadline), the alive book's dismissals and the sync
  heartbeat are on the wait clock. The audit of every remaining wall read in
  `authority`, `engine`, `meta`, `net` and `store-s3` (PROGRESS) classifies
  each as cross-node, persisted, an identity key, status or an external
  credential. **C7-H1 is done** (its code; pjdfstest, the spec's end-of-commit-4 gate, has not been run yet and goes with the next measurement campaign). Handed to C7a: rate (ρ) in the margins and in
  the sim, suspend in the sim, the models' per-node clocks, boot-id horizons
  (row 14, the step *while the node is down* and a forward step after the last
  horizon write followed by a restart), and the re-persisting of a horizon
  when a forward jump is observed. Handed to C7b: every cross-node compare
  (rows 2, 6, 9, 24, 25: lease and promise expiries, the token window, the
  root's lock cut, `stream_head_at`, the seal watch's `known_expiry`),
  including the stepped-back claimant of seed 1892.
  *Not in C7-H1:* rate (ρ), suspend in the sim, the models' clocks and
  boot-id horizons (C7a); every cross-node compare (C7b). The models have
  one clock per node and cannot express the split; C7a reworks them with
  `rate_ppm` (§11 Q19 d).
- **C7-H2: a fast successor whose clock is ahead gets no quarantine.**
  `note_marker_landed` (`backup.rs:846-893`) computes the read and lock
  quarantines as `prev_expires.min(bound(..))`: `prev_expires` is the
  predecessor's `expires_unix_ms` (its wall clock), compared on the
  successor's. A successor ahead by more than the remaining lease sees
  it in the past, the `min` yields no quarantine, and it acknowledges
  mutations and grants locks over the predecessor's live read
  delegations and lock grants. The bound alone is sound and rate-only
  (`W + ttl + 2M ≥ (W + ttl − M)(1 + ρ)`); the `min` is an optimisation.
  *As landed (PROGRESS, "a fast successor whose clock is ahead got no
  quarantine"): not two lines.* A successor ahead by more than what the
  lease had left never reached the `min`: `acquire_won` took its takeover
  for a TTL takeover (`!p.is_expired(now)`) and armed no floor at all, so
  the fast/TTL classification goes by the permit too (Appendix B row 26);
  and the whole-bound floor outlasts a dwell, which exposed an idle
  release inside the gate (the lease is now held through the gate and the
  floor). **Decided 2026-10-18 (maintainer, Q10a):** landed with the full
  floor as is, including the S3-cut holder's failover (measured in the
  harness scenario `s3-cut-holder-failover-with-locks`: the first
  acknowledged write and the first lock grant after the takeover moved
  from +0.3 s / +0.1 s to about +8.5 s / +23.5 s, with `EAGAIN` on
  non-blocking requests for a free lock meanwhile); the two follow-ups are
  §7 C7-F.
  Standalone fix: drop the `min` (both lines). Fail-before: a sim
  config `fast-takeover-skewed-successor` (`backup_strict_config` with
  the backup's clock `+(lease TTL)` ahead via per-node skew; a strict
  read delegation and a lock grant live at the seal): today the cto
  check / lock ghost fails; after, clean ×2000. Cost: a fast successor
  of a tenure that served strict reads or locks always waits the full
  bound, where today it sometimes waited only the predecessor's
  remaining lease. **The cost is bounded** (the review's M6): the `min`
  bites only when the fast takeover lands within `bound − now` of the
  predecessor's lease end; otherwise today's wait already is the bound
  (F4). The read quarantine is ≈ 1.5 + 5 + 2 s (+ 6 s with live
  delegations), the lock quarantine ≈ 1.5 + 20 + 2 s (+ 6 s); so
  dropping the `min` adds at most ≈ 14.5 s to acknowledgements and
  ≈ 29.5 s to new lock grants, only when a fast takeover happens in the
  predecessor's last seconds and the tenure served strict reads or
  locks. A knowing latency trade for integrity: the maintainer's
  decision (§10 gate G5, §11 Q10a; recommended yes). Measured in
  `delegate-backup-handoff-failover` (whose bound already reads the
  logged `wait_ms`, F4) and a sim count of how often the `min` bit in
  `long-backup`.

**The rule set** (C7 proper):

- **Rate budget** ρ = 1 000 ppm on `CLOCK_MONOTONIC_RAW` (oscillators
  20–100 ppm; RAW is immune to NTP/chrony slewing — chrony's default
  `maxslewrate` is 83 333 ppm, so a slewed clock must not be budgeted).
  The clock part of each margin is `2ρ·T` (lease 60 s → 120 ms, lock
  20 s → 40 ms, delegation 5 s → 10 ms, the 1.5 s takeover window → 3 ms);
  `expiry_margin_ms` (1 s) stays as transit and processing slack. The
  documented rule becomes `margin ≥ 2ρ·T + transit`.
- **Two clocks by role** (`ProtocolClock` in `crates/store-s3/src/lease.rs`
  or `crates/platform`, with the fault hooks applied there):
  - *honour side* (a holder of a lease, grant, delegation, read
    delegation or token; `liveness_fresh`; the fast path's `usable`)
    must never under-count elapsed time: `elapsed_h = max(RAW, BOOTTIME,
    wall_delta)` (BOOTTIME counts suspend, the wall delta a VM pause
    whose guest clocks stopped);
  - *wait side* (a granter's outwait, a claimant's observation, the seal
    watch, quarantines, token memory) must never over-count:
    `elapsed_w = RAW`, never the wall clock.
  Linux: `clock_gettime` (vDSO); macOS: `mach_continuous_time` /
  `CLOCK_UPTIME_RAW` (plans 34–36); `Instant` elsewhere.
- **`Ms` becomes node-local protocol time.** Every value that crosses a
  node boundary is an opaque echo (`sent`), a relative duration
  (`ttl_ms`, `remaining_ms`, a promise's `d`), or an S3 observation (an
  ETag).
- **The six cross-node compares and their replacements** (Appendix B
  rows 2, 3, 6, 9, 10, 11):
  1. *Root-lease claimability* (`Lease::is_claimable(now)` in
     `classify`, `live_leases_held_by`, `fence_retired`): the
     **ETag-observation rule** — a claimant that saw the object with ETag
     E at wait-clock t₀ may CAS on E once it still sees E at
     t₁ ≥ t₀ + ttl_ms·(1 + 2ρ), or at once if `released` or `fenced`.
     The lease body gains `ttl_ms` (a claimant never assumes the holder's
     configuration) and `fenced` (an admin retirement, replacing
     `fence_retired`'s `now.min(expires)`); `expires_unix_ms` stays for
     status and as the earliest time worth re-reading. A node that
     already watches the object (renewals, `wanted_by`, the promise
     watch, the backup's seal watch) has its t₀; only a cold claimant
     waits a full TTL from its first read.
  2. *Heartbeat promises* (`heartbeat.rs` `Promise::outlasts`,
     `takeover_check`; `promise.rs`): **request-anchored** — the taker
     asks (`PeerMsg::PromiseRequest`) at its wait-clock s, a promise
     says "no epoch for d_ms from receipt", the taker's CAS must precede
     s + d·(1 − ρ); without P2P, an `heartbeat/ask-<taker>` nonce object
     the promisers echo. The membership argument (an epoch's N − f
     members share a node with the taker's f promisers) is unchanged;
     the clock argument is replaced.
  3. *The fencing token at a non-minting executor* (`LockToken::until_ms`
     judged by `check_tag`/`check_tag_at_write` on the executor's
     clock): the token carries `remaining_ms` (= `until − now` on the
     holder at send); the executor stamps receipt r on its wait clock
     and refuses at r + remaining. Phase 1 judges by `min(absolute,
     r + remaining)` (safe if either the offset bound or the one-way
     delay bound `transit(request) + transit(op) < 2M` holds); phase 2
     drops the absolute stamp once the sim and the campaign show the
     delay bound holds under load (§11 Q10b). The minter, and a delegate
     the grant moved to, still judge exactly (C4).
  4. *The fast-takeover floor*: the bound alone, and the fast/TTL
     classification by the permit (C7-H2, landed first; Appendix B rows
     10 and 26).
  5. *The lock cut* (`DelegRenew::stream_head_at`, `lock_cut_here`,
     `lock_barrier_ready`): **root-anchored** — a delegate sends renewal
     k only after reply k − 1, so the root's own send time of reply
     k − 1 is a lower bound on the head's as-of. After C4 this is the
     `LockExpire.barrier` computation; it costs at most one renewal
     interval of extra wait after an outwait.
  6. *`ack=s3` fast takeover timing* (`core/lease.rs:296`, `now −
     (prev.expires − ttl) ≥ backup_takeover_ms`): "the lease ETag
     unchanged for `backup_takeover_ms` on my wait clock".
- **Restart horizons** (`KV_READ_GRANT_HORIZON`, `KV_LOCK_GRANT_HORIZON`):
  persisted as `(boot_id, CLOCK_BOOTTIME until, wall until)`; same boot →
  wait the longer of the two (a same-boot restart after a backward wall
  step still waits the BOOTTIME bound, and the wall bound covers a
  BOOTTIME anomaly); another boot or missing → wait the full
  `ttl + margin` from start (a reboot with grants out waits ≈ 21 s
  before granting).
- **Suspend/resume**: on resume the honour clock has jumped by the
  suspend, every window held is past (owner fence, taint, as today); the
  lease is renewed if its own ETag is still current, else the CAS fails.

**What stays offset-dependent** (documented, Appendix B rows 17–19):
GC's 7-day horizon against S3's `Last-Modified` (tolerance a day or
more) and hold expiries; HLC ctime stamps and atime clamps (timestamps,
not safety); status, log stamps and snapshot names. The non-minting
token check trades the clock bound for a one-way-delay bound (stated as
such in `cluster-locks.md`). Cross-restart deadlines across a reboot
fall back to a full wait.

**Code areas.** `crates/store-s3/src/lease.rs` (`ProtocolClock`;
`Lease { ttl_ms, fenced }`; `is_claimable` taking an observation
`(etag, since_wait_ms)`); `crates/authority/src/core/{mod,lease,jobs,promise,backup,delegate,locks}.rs`
(two readings per step or a role per comparison; the claimant's
observation state in `LeaseState`; request-anchored promises; the cut
as-of; the `ack=s3` observation); `crates/meta/src/{locks,readdeleg}.rs`
(`LockToken { remaining_ms }`, receipt-stamped judging,
`HeldGrant`/held delegations on the honour clock, boot-id horizons);
`crates/engine/src/{lease,epoch,authority_driver,p2p,view/write_gate}.rs`
(feed both clocks; `LeaseView::usable` on the honour clock);
`crates/store-s3/src/heartbeat.rs`; models `crates/model/src/{cto,locks,flex}.rs`
(`rate_ppm` beside `max_offset`; the promise proof restated);
`crates/authority/tests/sim/clock.rs` (per-node `rate_ppm`,
`suspend(ms)`, `step(ms)`; `clock_skew_ms` allowed far beyond 500);
`crates/harness/src/load.rs` (lift the ±500 cap on `clock:`; add
`clock:rate:<ppm>` and `clock:step:<ms>@<s>`; `campaign_load.rs`'s
`StaleWatch` skew term rate-based). No compatibility: old lease objects,
tokens and promises are not read.

**Fail-before tests** (each fails on the parent):
1. Sim `strict_close_to_open_holds_with_clock_skew_and_loss` at
   `clock_skew_ms = 60_000` (constant offsets, zero rate error): today a
   claimant 60 s ahead takes a live lease over (two holders); after,
   clean over 60 seeds (and in CI).
2. Sim `locks-*` at `clock_skew_ms = 5_000`: a non-minting executor
   behind the holder lands a tagged op after the next grant today
   (row 6); after, refused within the delay bound.
3. Sim: a fast takeover with the successor ahead of the predecessor's
   remaining lease (row 10) — C7-H2's config, kept.
4. Sim: an `f = 1` continuation epoch with a taker 20 s ahead of a
   promiser (row 3): today `outlasts` passes and the epoch splits;
   after, refused until anchored promises cover the CAS (`flex` model
   with `rate_ppm` and a 20 s offset: `single_authority` clean).
5. Sim: the configurations of C7-H1 (a holder, locker, delegate and
   reader stepped back; a root, successor and F-a holder stepped forward;
   rows 4, 5, 7, 8, 12, 13, 27), kept.
6. Sim: a holder suspended (MONOTONIC and RAW stopped, BOOTTIME and wall
   advancing) across its lock window: a naive `Instant` port would
   honour on resume; the honour clock lapses it.
7. Sim: a delegate 2 s ahead of the root under an outwaited grant
   (row 9): today the floor misses a row (`turn_read` stale); after,
   the as-of is root-anchored.
8. Unit: the lease observation rule (ETag unchanged for ttl·(1 + 2ρ); a
   renewal in between restarts the watch; `fenced` claimable at once;
   `released` unchanged); `takeover_check` with anchored promises;
   boot-id horizons.
9. Load campaign: `clock:5000` and `clock:spread:5000` (refused by the
   cap today; after, accepted and every checker green on
   `lock-contention-paused-holder`, `delegate-paused-across-ttl`,
   `git-under-flock-causal`, `visibility-s3-latency`), plus
   `clock:rate:50000` on one node and `clock:step:-3000@30` on the
   holder during the lock scenarios.

**Performance.** No new per-op clock read on the FUSE fast path beyond
today's one (`usable`); `clock_gettime(RAW)` and `BOOTTIME` are vDSO
calls on Linux; `vfs-bench` and the single-node stat/open measurements
neutral. Rate-only costs, reported as latency bounds, not failures: a
cold claim after a crash waits ttl·(1 + 2ρ) from its first read
(≈ 60.1 s; `backup`-policy failover is unchanged, it is the seal path);
a reboot with grants out waits the full horizon.

**Exit criteria.** No safety decision compares a wall-clock value
produced on one node with a clock on another, except Appendix B row 17
(documented, tolerance ≥ 1 day); the models check ρ = 1 000 ppm with
offsets up to a whole TTL; the sim runs `clock_skew_ms = 60_000` in CI
for the strict and lock configurations, with suspend and step in the
default fault mix; the campaign's `clock:` injector accepts any offset
and the campaign requirement reads "clocks with rate error within 0.1 %
(`CLOCK_MONOTONIC_RAW`), any offset"; DESIGN §5 (the time discipline)
and §9 ("clock skew" row), `cto-modes.md` "Clocks and margins",
`cluster-locks.md` (the margin rule and the token's delay assumption),
`durability-and-failover.md`, ADR-25, a new ADR "Clocks: rate, not
offset", TESTING.md, and ARBITER-STUDY §7's row rewritten to "rate ≤
0.1 %; offset unconstrained; delay < 2M for the non-minting token check".

**Place in the order and dependencies.** C7-H1 and C7-H2 land first,
standalone. C7 proper is largely independent of the commit rule, but it
edits the same functions as C1 (`LeaseState::classify` and the claim
path: C1 adds the tail fetch *after* claimability; C7 changes the
claimability predicate) and C4 (the lock cut: C4 records the barrier;
C7 changes how its as-of is computed). It is split (C7-H1 above is the
first part of C7a, landed early):
- **C7a — clocks by rate** (on top of C7-H1, which already puts every
  local duration on the honour or the wait clock: the rate budget and the
  `2ρ·T` margins, suspend and rate in the sim (`ClockStep` is C7-H1's),
  `ClockStep` in the default fault mix, boot-id restart horizons (row 14,
  with the promise's `issued`), the harness's `clock:rate:` item, the
  models' `rate_ppm`; rows 1, 4, 5, 7, 8, 12–15, 21, 22 for their rate
  half): no overlap with C1–C5; recommended right after C0, so every
  later milestone's sweeps run with rate faults and large constant
  offsets.
  *Specified 2026-10-20* (the coder spec is kept with the reviews; the
  decisions are §11 Q20).
  - *ρ in the clocks.* Each node's WAIT runs slow by its budget ρ and its
    honour elapsed fast by it (`WAIT = wall₀ + e_raw/(1+ρ)`, `e_h =
    e_raw/(1−ρ) + S`), so P2 and P3 hold exactly for any oscillator within
    the budget and every honour/wait pair is safe for any span with the
    margins it has: per window that is `ρ·T` on each side (production:
    lease 60 ms, lock 20 ms, read delegation 5 ms, the lock floor 23.5 ms),
    and `M` stays transit slack. ρ is a property of one node's clock
    (1 000 ppm where the rate source is unslewed). The beat cap's span
    stays, derived from ρ (`M/(4ρ)`, 250·M at the defaults): a beat sent
    before the renewal that set `X` measures a past interval on the
    holder's wait clock, which under-counts it by up to `2ρ`. Wait timers
    sleep `after·(1+ρ)`.
  - *Restart horizons by boot.* The lock and read horizons and the
    promise's `issued` are persisted as `<wall> <span> <boot id> <raw
    until>` (append-only tokens; a value with only the wall is read as
    unstamped): on the same boot the remaining time is read on
    `CLOCK_MONOTONIC_RAW` (not BOOTTIME, which is slewed), immune to steps
    while up or down; on another boot, or unstamped, the recorded span is
    waited from start (≈ 22 s for locks, ≈ 7 s for acknowledgements, only
    after an ungraceful stop: a graceful shutdown clears a passed horizon).
    Re-persisting on a forward jump is then not needed.
  - *Also:* the takeover floor persisted (the horizons at arming, a
    pending-floor key before the fast claim CAS); M4's own-lease arm gated
    whatever the wall clock says of the lease's expiry (33adc8e's rule,
    which the sans-IO port narrowed); the models' per-node rate and the
    honour/wait/wall split in `cto` and `locks` (`flex` is C7b's); the
    sim's `RateSkew`, `Suspend`, `RebootNode` faults, and clock faults in
    the default mix with steps bounded to ±200 ms until C7b.
  - *Six commits:* (1) the sim's clock model, faults and fail-before
    configurations; (2) rate-budgeted readings (fable review); (3)
    boot-stamped horizons and M4's arm (fable review); (4) the floor
    persisted (fable review); (5) the models; (6) the default mix, the
    harness's `clock:rate:` and reboot items, the campaign preset and the
    docs. 1 and 5 land on their own at any time; 2 and 3 each need 1; 4
    needs 3; 6 needs 2 and 3.
  - *Commit 1 landed (2026-10-20; PROGRESS "Plan 43 C7a, commit 1 of 6"):*
    the simulator's oscillator (`NodeClockState`: rate, suspends, boots, the
    boot id; timers count through its inverse), `RateSkew`, `Suspend`,
    `RebootNode`, `CrashHolderStepped`, `CrashSuccessorInFloor`,
    `SimConfig::{rate_budget_ppm, node_rates, wall_follows_rate}`,
    `ProtocolClock::new/with_id(.., budget_ppm)` and
    `Config::clock_rate_budget_ppm` carried unused, the platform's
    `CONSTELLATION_FAULT_CLOCK_RATE_PPM` and
    `CONSTELLATION_CLOCK_RATE_BUDGET_PPM`, the harness's `clock:rate:`. No
    protocol site reads the budget: the sweeps of every configuration without
    a rate, a suspend or a reboot are identical to the parent's. Fail-before
    (2 000 seeds): `locks-root-rate-fast` 1 364, `fast-takeover-successor-rate-
    fast` 1 984 (the rate it needs is about 25 %, not the table's 10 %),
    `fast-takeover-holder-rate-fast` 224 (holder slow, backup fast: both fast
    cancels), `strict-reader-rate-fast` 613, `locks-root-restart-step-forward`
    1 143, `locks-root-step-forward-then-restart` 774, `locks-root-reboot-step-
    forward` 1 143, `strict-holder-restart-step-forward` 212, `fast-takeover-
    successor-restart-before-marker` 57; clean on the parent and kept as
    regressions: `delegated-root-rate-fast`, `flex-promiser-restart-step-
    forward`, `holder-restart-step-forward-readopt`, `fast-takeover-successor-
    restart-in-floor` (the lock mirror's persisted horizon covers the grants
    of its geometry), and the suspend pair (dropping `S` alone fails nothing:
    HONOUR follows a disciplined wall through `J` across the suspend; dropping
    both fails `locks-locker-suspend` 557 of 1 000). Found: a delegate
    partitioned from its root for longer than a 20 s delegation (no clock
    fault, `delegated-long-delegation`) fails 16 of 300 seeds on the parent
    ("a rid refused with Eexist completed once in the log"): not
    clock-related, to be looked at.
  - *Commit 2 landed (2026-10-20; PROGRESS "Plan 43 C7a, commit 2 of 6"):*
    ρ is in the clocks (Q20, D1): WAIT counts `1/(1 + ρ)` of the raw clock's
    elapsed time and HONOUR `1/(1 − ρ)` of it (fixed point, one `u128`
    multiply per reading; ρ = 0 reads exactly as C7-H1), so for any
    oscillator within a node's budget a wait never over-counts and a honoured
    window never under-counts, whatever the span, with the margins as they
    were. The beat cap's span filter is `M/(4ρ)` (250 margins at 1 000 ppm; no
    limit at 0), timers sleep `ρ` longer, `status.clock.rate_budget_ppm`. The
    five rate configurations are clean at 2 000 seeds and a fresh range
    (fail-before counts above); `long_locks` has them; the identity sweeps
    (every no-rate configuration) are unchanged.
  - *Commit 3 landed (2026-10-20; PROGRESS "Plan 43 C7a, commit 3 of 6"):*
    the persisted horizons (read grants, lock grants, the promise's `issued`)
    are boot stamps: `<wall> <span> <boot id> <raw until>`, append-only (a
    reader ignores tokens it does not know; a bare number, an older build's
    value, waits the caller's default span). `Now` carries `boot` (`id`, the
    raw clock since boot, the budget); a restart on the same boot reads the
    time left on `CLOCK_MONOTONIC_RAW` (never stepped, slewed or counting
    suspend, which is conservative for a wait), so no step of the wall, up or
    down, while the node was down or after the last write, moves it; another
    boot waits the recorded span. A write never lowers what the previous
    process left (`Meta` keeps the loaded quarantine), a graceful shutdown
    deletes a passed horizon (so an idle node's reboot waits for nothing),
    `Action::PersistLockHorizon`/`Event::LockHorizonPersisted` carry `WaitMs`
    (the core's wall-to-wait conversions are gone), and M4's own-lease arm is
    gated again whatever the wall says of the lease's expiry. No re-persist
    on a counted forward jump (D4). The restart configurations are clean at
    2 000 seeds and a fresh range (fail-before counts above);
    `fast-takeover-successor-restart-before-marker` is the floor's, commit 4.
  - *Commit 5 landed (2026-10-20; PROGRESS "Plan 43 C7a, commit 5 of 6"):*
    `crates/model/src/clk.rs` (a node's oscillator with a rate, and its wall,
    honour and wait readings), per-node rate, `rate_budget`, `clock_split`,
    `wall_follows_rate`, `beat_span` and `step_node`/`step_dir` in `cto` and
    `locks`, every `local(i)` of both on one of the readings (the modules'
    "Plan 43 C7a: the readings" tables), and the violate/clean pairs of §8.2.
    Found: a disciplined wall catches a slow oscillator up (`J`), so the honour
    side of rate needs an undisciplined host in the model as in the
    simulator; a staircase oscillator at one-tick resolution leaves the budget
    one tick of rounding, which the violating variants' margins of 0 or 1
    and the clean ones' keep apart (the spec's `deleg_ttl 6` with margins 1 has
    no budget-0 violation at that resolution); the beat cap's end-to-end
    search finds nothing in 144 cells, so its pair is a trace over the
    renewal tick (the floor ends up to one tick inside the holder's renewed
    window without the span, never with it).
- **C7b — the cross-node compares** (rows 2, 3, 6, 9, 11, and the sites
  C7-H2 found and left: 23 the released-takeover lock grace, 24 the seal
  watch's expiry compares, 25 `promise_check_needed`; and those C7-H1's
  inventory found: 28 the reader's holder check, 29 `confirmed_ms`, 30 the
  barriers carried between root and delegate, 31 tokens in inbox batches
  and the replay queue: the observation
  rule, anchored promises, `remaining_ms` tokens, the root-anchored cut,
  the `ack=s3` observation, a lock grace that does not read the releaser's
  expiry): after C1 (the claim path is settled) and
  C4 (the cut lives in `LockExpire` then); before C5 (no overlap).
  *Evidence found by the ask-again review (row 6's class, a clock offset
  beyond the envelope):* `fast-takeover-skewed-successor` (the real cap)
  violates mutual exclusion at the second failover, t about 15 to 16 s, with
  nodes 2 and 3 four seconds ahead, about once in 15000 seeds on 63da672
  (seeds 11087, 18138) and with the ask-again patch (5570, 11942, 202261;
  its trajectories differ): a node whose clock is ahead reads the live
  holder's lease as expired and seals its epoch, the next holder takes the
  lease and grants 0.35 to 0.8 s later, and the old holder, alive, keeps
  admitting I/O under its own grants (seed 5570: its own client enters at
  15274 after "a backup sealed our epoch" at 15209) or even grants
  (seed 202261: node 2 at 15651, after node 3's takeover at 15458). Pinned
  as `#[ignore]`d known-gap tests (`fast_takeover_skewed_successor_known_gaps`
  in `crates/authority/tests/sim.rs`). To check with C7b: a holder that
  learns it was sealed stops admitting new local sections under its
  self-grants; a successor of a lease it read as expired on a skewed clock
  waits out the bound.

  The observation rule's cold-claim wait (a full TTL from the first
  read) is a knowing latency cost: §10 gate G6.

### C7-F — The floor's cost, taken back (follow-ups of C7-H2; decided 2026-10-18)

The maintainer accepted C7-H2's full floor (§10 gate G5, Q10a) and asked
for two separate pieces of work right after it. Baseline for both: the
harness scenario `s3-cut-holder-failover-with-locks` (an S3-cut holder
still alive on P2P, strict reads and a held lock; PROGRESS "a fast
successor whose clock is ahead got no quarantine").

- **F-a — the floor capped by the predecessor's own heartbeat (specified
  2026-10-18; the approach is §11 Q17). Landed (PROGRESS "Plan 43 C7-F
  F-a"): the beat carries `guard_unix_ms`; `Core::beat_cap` (`X +
  min(at − sent) + M`, beats within `250·M` of `X`) caps the floor of a
  fast takeover, none if it has passed when the marker lands; the
  slack is `M` until C7a names ρ. Not changed: F-a0 (its own commit,
  before this one), `dl.inherit_slack_ms` and the inherited generations'
  reclaim wait (a possible later use of the same cap). Found: most
  simulator configurations model no holder beat
  (`SimConfig::holder_beats` is `Off`), so their floors stay the bound
  and the cap is inert there; the cap's sweeps use
  `AUTHORITY_SIM_BEATS=thread` and the new beat configurations.**
  *Why not the renewal premise.* The bound counts from the marker because
  the predecessor answers within `backup_takeover_ms` (W) of its latest
  proof of S3 liveness, and those proofs are not only renewals:
  `note_s3_liveness` counts `SegmentRun` and `SegmentPut` successes and a
  `LeaseGet` that shows the holder its own lease (plan 41 L0's
  `renew_probe`, which review R2-3 did not list); neither read is
  observable by anyone. Narrowing `liveness_fresh` to lease writes costs a
  conditional PUT every W while strict reads or locks are served (about
  0.7 PUT/s per busy holder, each a CAS racing the renewals and the
  requesters' `wanted_by`); counting announced proofs needs the backups
  to acknowledge each before the holder may use it, a co-signed freshness
  protocol of its own. Neither is needed: what the S3-cut holder's
  successor cannot see end is the predecessor's *lease*, not its
  freshness window, and the lease is observable rate-only.
  *The rule.* Every answer a successor waits out is capped by the
  predecessor's lease: strict answers only while `usable` (the expiry
  less M), read delegations (`maybe_grant`), lock grants and renewals
  (`lock_cap_ms`), write delegations and renewals (`grant_cap_ms`) end at
  the expiry less M, and their holders honour them from their own send
  less another M; what a delegate grants is capped by its own `until`.
  So nothing is honoured after the holder's clock reads `E_h`, the expiry
  it holds. The holder's off-core heartbeat (`HolderAlive`, every
  `backup_heartbeat_ms`, to its listed backups, or to its stream
  subscribers under `ack=s3`; it stops at the expiry) carries `sent` and
  `expires`, both on the holder's clock, and is stamped `at` on arrival
  on the successor's: each beat says the holder's clock read `sent` no
  later than the successor's read `at`. The **cap** is
  `X + min(at − sent) + M`, the minimum over the beats of the replaced
  tenure sent within `250·M` of `X` (so the two clocks' rate error,
  `2ρ·(X − sent)` at ρ = 1 000 ppm, stays under `M/2`), with
  `X = max(prev.expires_unix_ms, the highest expiry its beats named, the
  highest guard they named)`. The floor is `min(bound, cap)`, for
  acknowledgements and new lock grants alike; a cap already past when the
  marker lands sets none. `X − sent` is a difference on the holder's
  clock, `at` and the cap are stamps of the successor's: rate-only, like
  the bound, and C7-H2's skewed configurations stay its test (the
  arrival, never the beat's credit `min(at, sent + M)`, which is the
  holder's clock).
  *Why `E_h ≤ X`.* The successor's CAS replaced version T (`prev`), so no
  later write of the holder landed; the holder adopts only versions its
  own PUT or read returned, all at or before T; along that chain the
  expiry only grows (renewals write `now + ttl`, everything else copies
  it). The two ways it could shrink are covered: a holder that restarted
  in its epoch with a shorter TTL (its earlier incarnation's grants are
  behind its restart quarantine, which the guard names), and a backward
  step of the holder's wall clock (C7-H1's hole, which breaks every
  honour-side check alike; the honour clock makes the chain monotone).
  *The guard.* A predecessor may itself be waiting out grants its lease
  does not cap: its own takeover floor (the harness's 20 s lease is
  shorter than the 23.5 s lock floor), a released takeover's lock grace,
  a restart quarantine, and the grants it made under a continuation
  epoch's hold (capped by the lock or delegation ttl alone, no lease
  having an expiry then, and kept across the re-claim: with a ttl longer
  than the lease's they outlast the re-claimed lease; review F1). The
  beat carries `guard_unix_ms`: `i64::MAX` while its takeover gate is
  pending or a floor is due, else the latest of its `floor_until`, the
  read and lock tables' quarantines, its subtree graces and its hold-era
  grants (0: none). The successor takes the maximum over the epoch's
  beats: a `MAX` heard at the epoch disables the cap for all of it
  (conservative; the refinement that lets a later finite beat clear it
  is not taken: it would forget a restarted incarnation's floor, which
  lives in memory). A finite guard may rise after a beat named it only by
  what protects a lease-capped right (a subtree grace; the hold-era
  grants): the simulator asserts the base of the guard never does.
  *F-a0: a tenure that inherits a floor is marked from its first CAS.*
  A successor arms a floor only if the replaced lease says
  `granted_delegations`, and a fresh taker's lease is marked at once
  only on strict mounts (`granted_lease`); so the seal of a tenure that
  is still inside its own floor and served nothing yet (locks without
  strict mounts) arms none, and the grants that floor protected (the
  unmirrored ones of the tenure before) are open. Found while designing
  F-a and then shown by seeds: the claim CAS body (`acquire_cas`) is
  marked when the takeover inherits such a duty (a permitted takeover of
  a marked lease; a released lease whose release marked it because the
  tenure left lock grants or a grace behind: the plan's first form, any
  released lease under locks, made every seal after a start-up handoff pay
  the floor, and `a_lagging_backup_still_seals_a_dead_holder` failed on it). *Landed on its own,
  ahead of F-a* (the maintainer's decision, 2026-10-18). Fail-before:
  the core test
  `a_takeover_that_inherits_a_floor_marks_its_lease_from_its_first_cas`,
  the model test
  `a_tenure_sealed_inside_its_floor_leaves_no_floor_without_the_inherited_mark`
  and the sim configuration `double-seal-inside-the-grace` (5 of 12000
  seeds violate mutual exclusion before, seed 548 among them; none after
  for that cause; PROGRESS has the counts and an unrelated late failure
  the configuration also meets).
  *F-a0 follow-ups* (found by F-a0's review; each its own fail-before
  first). (b) *The inherited mark outlives its floor.* A
  tenure that takes over a marked lease is marked for its life, so its own
  sealer arms a floor even if the tenure never granted, and a chain of
  seals keeps paying 8.5 s / 23.5 s at the defaults for nothing; only the
  release chain is cut (a released lease's mark is exactly `lock_duty_live`).
  Clear an inherited mark by CAS once the floor has passed, `lock_duty_live`
  is false, no read or write delegation is live and the tenure made no
  grant of its own; `ensure_granting_marked` re-marks before the next
  grant, so a racing seal at worst over-waits. Decision D1 (2026-10-20):
  inherited marks only; a mark a tenure made for its own grants is not
  cleared (the cost of a bursty workload paying a mark CAS on its first
  grant after each idle period, and a clear CAS per idle period, is not
  taken until the floor-cost table shows it matters). *Landed
  (2026-10-20)*: `Core::maybe_clear_inherited_mark`, in the holder's
  housekeeping, issues a dedicated CAS (`S3For::ClearMark`;
  `Lease::without_granted_delegations`) only when the mark is inherited and
  not own (`ack.mark_inherited && !ack.mark_own`: own for a grant or answer
  gate, `strict_mounts`, or the carry of an own lease; inherited for the
  claim CAS's `inherits_duty`, the `acquire_won` re-mark and the
  reconfiguration's mark), the lease is marked, not lost, releasing, gated
  or an epoch's hold, no floor is pending or running, `tenure_duty_live` is
  false (no grant record at all: a record that expired but is still in the
  table can be revived by a renewal), the delegation table is empty, no
  answer is held and no mark, reconfiguration or lease read is out. While
  it is out (`ack.unmarking`) `tenure_marked` is false and
  `ensure_granting_marked` issues nothing, so renewals and reclaims are
  held and lock grants and strict reads wait; a clear in doubt
  (`unmark_unsettled`: a conflict, a failure, a lost answer) is read back
  before anything is answered and the tenure adopts whichever mark the
  object holds, deposed if it is no longer ours. *Safety argument.* At the
  decision nothing is live: every inherited grant has lapsed (the floor has
  ended on this node's wait clock), every quarantine and grace has passed,
  there are no records and no read grants, and the tenure answered nothing
  of its own. From then until the CAS settles nothing is answered. Once it
  lands the tenure is a fresh unmarked fast tenure, the state 97e806b
  already treats as safe for the taker of an unmarked release, which any
  grant or renewal marks first. A seal racing the clear only over-waits (it
  replaces the still-marked version) or replaces the unmarked one and
  finds nothing live. Fail-before (core, each on the L3 tree):
  `an_inherited_mark_is_cleared_once_the_floor_has_passed_and_nothing_is_live`,
  `a_grant_during_the_clear_cas_waits_and_re_marks` and
  `a_clear_in_doubt_rereads_before_any_grant`
  (`an_inherited_mark_stays_while_anything_it_stands_on_is_live` guards the
  negatives); sim `double-seal-after-the-grace`. *Limit:* a record that
  stays in the table (the node holding the grant died with it cached, and
  no conflicting request purges it) keeps the mark: the clear needs an empty
  table, the revival rule's price; a grant released by its holder (idle
  release after `lock_cache_idle_ms`, a recall) leaves the table at once.
  (L3) *Duties no claim CAS marks* — *landed (2026-10-20)*: a node
  re-claiming its own released lease with a live quarantine
  (`lock_quarantine_dropped_tenure`; `granted_lease`'s `own` excludes
  released leases), a restart quarantine over an unmarked previous lease,
  and reclaims admitted into a `Local` tenure (its renewals are not held,
  so the reclaimed grants install unmarked; the lock mirror goes only to
  the backups listed when the table changed), and the grants an epoch's
  close keeps (a re-claim continues the tenure under its own mark). Each
  becomes F-a0's hole if that tenure lists a backup and is sealed before
  its next mark CAS. A tenure is sealable (`fast_tenure`) in exactly two
  places, since a claim CAS writes `backups: []` and `Local` unless
  `ack=s3`: the claim CAS under `ack=s3`, and the reconfiguration that
  lists the first backup (`issue_reconfig`, and its retry in
  `on_reconfig_reread`). Both now mark the lease when
  `Core::tenure_duty_live` (lock duty or any grant record, a live write
  delegation, a pending or running takeover floor, read grants or a read
  quarantine, grants made under an epoch's hold); `on_reconfig_put` and
  `acquire_won` mark a sealable unmarked tenure whose duty arose in between
  (the catch-all). A
  reconfiguration CAS that makes the tenure sealable and does not mark it
  holds lock renewals and reclaims, and the mark CAS, until it settles
  (`tenure_marked`, `ensure_granting_marked`): a reclaim under a grace that
  began since would otherwise stand in a version a sealer may replace as it
  lands; so does one whose outcome is not known (an answer lost: it may
  have landed) until a lease version is next adopted from S3
  (`LeaseState::reconfig_doubt`). The mark of the release (`issue_release`)
  and the lock part of the
  predicate share one function, so they cannot drift; read terms are not
  part of the release (read delegations are recalled before a release).
  The fourth candidate, the grants an epoch's close keeps, needs no path
  of its own: the close writes no released lease (the re-claim is
  `own`, mark carried), the grants stand in the table, and the re-claim
  is sealable at the same two places. *Safety argument.* A sealer's CAS
  replaces a version that lists it. Every version that lists a backup was
  written either by the reconfiguration or after it (renewals copy the
  mark), or was written sealable by the `ack=s3` claim. At those writes any
  live duty marks the version; while the write is out no new duty is
  answered unheld; after it, every new grant or answer goes through the
  marking gates of a fast tenure; quarantines and floors start only at the
  claim (inherited and covered there) or at `note_marker_landed`, which is
  already marked. So a sealer never replaces an unmarked version while a
  duty is live. The cost: a tenure with nothing live at its first backup
  listing (a start-up, a handoff) stays unmarked, so M1's cost does not
  come back. Fail-before (core, each on 4a0fb20): the six tests named
  `a_reclaim_of_its_own_released_lease_with_live_grants_is_marked_when_it_lists_a_backup`,
  `a_restart_quarantine_marks_the_first_backup_listing`,
  `a_reclaim_admitted_into_a_local_tenure_marks_the_first_backup_listing`,
  `grants_kept_at_an_epoch_close_mark_the_first_backup_listing`,
  `a_reclaim_during_a_reconfig_in_flight_is_held_until_marked` and
  `an_ack_s3_claim_with_a_duty_live_is_marked_from_its_first_cas`; the sim
  configuration `locks-restart-reclaim-seal` (56 of 2 000 seeds violate
  mutual exclusion on 4a0fb20, none after); the model pairs below.
  (L4) *The lock model has no mark* — *landed (2026-10-20)*: `Reg::marked`
  (a root grant or a reclaim marks, a release marks per its table, grace
  and handed copies, a takeover inherits per `acquire_cas`, the grace arms
  only on a marked `prev`), `backup_of` (a backup per holder, for a double
  seal) and the knobs `inherit_mark`, `release_mark_duty`, `mark_at_sealable`;
  violate/clean pairs for the double seal, the released chain and the own
  re-claim (`crates/model/tests/locks.rs`).
  *What it buys.* An S3-cut holder alive on P2P beats until its expiry,
  so its successor's floor ends about M after it reads the lease as
  expired: the scenario's first acknowledged write and first lock grant
  go from +22.2 s / +37.2 s to about +14.6 s (+13.8 / +13.7 s before
  C7-H2). A holder that died or lost P2P mid-lease: unchanged (with
  half-TTL renewals its lease has at least 30 s left, more than the
  23.5 s bound), except in the lease's last seconds, where the cap gives
  back the pre-C7-H2 `min` without its offset. No beat heard at the epoch
  (the successor restarted, the beats were lost): the bound.
  *Interactions.* Appendix B row 6 (the non-minting token check,
  offset-bound until C7b) loses the slack the whole bound gave it: back to
  its pre-C7-H2 exposure, inside the documented ±500 ms. F-c: the grace
  in which the successor admits reclaims gets short or vanishes; F-c's
  "park until the reclaim is admitted" must not assume a grace. C7a moves
  the beat's arrival stamp to the wait clock together with the floor's own
  (a stamp and its comparison use one clock). Appendix B row 27.
  *The holder's clock.* The cap is rate-only for offsets but assumes the
  holder's wall clock does not step forward: a forward step `J` lowers
  every later beat's `at − sent` by `J` (the minimum keeps it for the
  beats retained), which moves the cap early by `J`, unsafe when `J > 3M +
  δ` with the holder beating on past the step and sealed mid-lease. The
  same class as C7-H1 (the root's own grant table judges expiry on its
  wall clock; a 2.5 s forward step broke lock exclusion in 73 of 2 000
  review-sim seeds with the cap and with it off alike): F-a adds a
  dependency on it. **C7-H1 and C7a must stamp `sent` (and read `sent`'s
  clock for `X`) on the holder's honour clock**, which the step does not
  move. A detection (wall against monotonic time between beats, guard
  `MAX` after a step) was left out.
  *Follow-up (not in F-a).* Most simulator configurations model no holder
  beat, so the cap is inert in them; beats on by default
  (`HolderBeats::Thread` in `SimConfig::default`) is a separate commit
  with full re-sweeps of every backup and `ack=s3` configuration and
  re-pinned regression seeds (65, 97, 548, and the credited control's 193262,
  197625 and 198196, which replaced 194557 and 194714 when the ask-again
  traffic moved the trajectories, among them),
  keeping `-nobeats` variants for the no-beat path.
  Fail-before: core tests of the cap (the parent waits the bound), of the
  arrival against the credit (a holder clock behind), of the guard, of a
  `MAX` guard and of stale or foreign beats; the sim's
  `fast-takeover-skewed-successor` clean over 2 000 seeds and failing
  with the credit used instead of the arrival; a new
  `fast-takeover-skewed-holder` (the holder ahead) and
  `locks-s3-cut-holder-alive` (the holder cut from S3 only, sealed at its
  expiry), the latter's floors measured shortened; the models with a beat
  action (clean far ahead; failing when a beat older than a renewal is
  used); the scenario's first write and lock grant within M + 1 s of the
  takeover.
- **F-b — lock requests wait for a grace instead of failing (specified
  2026-10-18; the semantics are §11 Q18).**
  *Semantics.* A non-blocking request (`F_SETLK`, `flock(LOCK_NB)`) on a
  lock the owner's table shows no conflict for, refused today only because
  the owner may not grant yet, waits until it may, and is then granted, or
  refused `EAGAIN` if the table then shows a conflict. A known conflict
  (or the requester's own grant under recall) is `EAGAIN` at once, as
  today. POSIX's "return immediately" is about waiting for a conflicting
  lock; a network file system answers when it knows. Precedent: Linux's
  NLM client resends a request the server refused for its grace period
  every 5 s until the grace ends, blocking or not (only a signal ends the
  wait, `EINTR`); the NFSv4 client retries `NFS4ERR_GRACE` the same way;
  DLM-based file systems (GFS2, OCFS2) hold lock requests through
  recovery. Linux documents `EINTR` for `F_SETLK` on a remote file.
  *Which refusals.* At the owner (`lock_serve`): the grace
  (`lock_grace_active`: the takeover, restart and released-takeover
  quarantines, the subtree graces), and the refusals of `lock_try_grant`
  that name no conflict: the tenure floor (`lock_tenure_floor_ready`,
  which after F-a is the main one), an outwaited holder's barrier, a
  delegate's short authority. For a non-blocking request they answer
  `Waiting { retry_ms, grace: true }`: the grace's remainder (a
  difference on the owner's clock), at most 1 s, or
  `lock_resume_retry_ms` for the others. The requester spends no attempt
  on it and asks again at `retry_ms`, up to `lock_grace_wait_max_ms` from
  the request (the floor's largest bound plus the S3-less deadline,
  49.5 s at the defaults); past that it answers `EAGAIN`, counted
  (`lock_grace_wait_gave_up`, which the campaign's errno checker expects
  at 0). A stale owner's `Busy` for a free lock (no fresh S3 liveness, a
  lease not yet marked), which still fails a non-blocking request
  `ENOLCK` after three attempts (documented), joins the same wait if
  Q18 (c) says so.
  *No FUSE thread held.* A non-blocking request is answered on the
  calling worker today; over the ring that is the ring's one offload
  thread, so a wait of up to the 23.5 s lock grace there would stall
  every offloaded request of those queues. A request that has to wait is
  handed to a `lock-wait` thread like a blocking one: the core answers
  the engine's request `LockAnswer::Wait { retry_ms }` instead of
  retrying itself, `ClusterLocks` moves the retries to that thread, and
  the adapter tracks the reply as deferred (a handover refuses while it
  waits) and interruptible (a signal ends it with `EINTR`) on cluster-lock
  mounts. A frontend that cannot defer waits inline, as its blocking
  requests do.
  *The ring.* A waiting request holds its ring entry until it is
  answered, and while every entry of a queue is held the kernel holds back
  every further request of that CPU. So a wait is counted against the
  queue's lock-wait budget (`depth − 1` entries per queue, one budget
  with `FUSE_SETLKW`'s: the frontend is asked when the view decides to
  wait, `Responder::begin_wait`, over a ring `ReplyEmpty::hold_for_wait`
  in the vendored fuser), so that one entry stays free; with none to give,
  the request is answered at once as before: `EAGAIN` for a non-blocking
  request, `ENOLCK` for a `FUSE_SETLKW` served as non-blocking past the
  budget (plan 38 Z2c's contract), counted
  (`grace_wait_refused_ring`, the ring's `lock_wait_downgrades`; the
  campaign fails on them). (The first version let these waits hold
  entries outside the budget: two waiters at depth 2 or 4 stalled every
  request of their CPU for the whole grace, 9.95 s measured, found by the
  review.) A request's first attempt, which asks the new owner on its
  offload thread and holds its entry meanwhile (4.5 s while the owner is
  found), is as before F-b: a follow-up is to hand it to the wait pool
  after a short inline budget. The same attempts are why `EINTR` is
  observed between attempts (within 100 ms) and not during one: the core's
  answer is awaited with a blocking receive. Follow-up: await it in
  slices, looking at the cancellation, so a signal ends a request against
  an unreachable owner at once instead of after its round trip.
  *The grace's end.* A timer at each grace's end serves the parked
  waiters (`lock_reserve_all`); today a blocking waiter is served only
  at its next re-attach, up to `recall_hold_ms` plus a retry later.
  Fail-before: the scenario's non-blocking attempts (75 of 108 `EAGAIN`
  on the landed patch; 0 after, each attempt across the grace answered
  by the grant); core tests per refusal kind (a non-blocking request in
  the grace, behind the tenure floor, behind a barrier: `WouldBlock` on
  the parent, `Waiting { grace }` after; a known conflict still
  `WouldBlock`; the requester's bound); an engine test that the waiting
  request does not hold the calling thread; and a harness ring test
  (`transport-lock-grace`: a ring cluster-lock mount at queue depth 2,
  three blocking `flock`s on free files across a seal: `ENOLCK` on the
  parent, granted after).
  *Status (2026-10-18): (a) and (b) landed; the review of 2026-10-19
  changed the ring part, a known conflict behind the tenure floor, the
  threads (a bounded pool) and the adapter's registration (when the
  request waits)* (PROGRESS "Fix: lock requests inside a lock grace wait
  for it"): the owner answers `Waiting { retry_ms,
  grace: true }`, the requester `LockAnswer::Wait`, the engine's
  `lock-wait` pool retries up to `lock_grace_wait_max_ms` (second commit:
  `lock_owner_wait_max_ms`, which has it as its last term; computed from
  the core's configuration; no knob; `grace_wait_gave_up` counts the
  `EAGAIN` past it and the campaign's errno judge fails on one); the FUSE
  adapter registers a request that has to wait as deferred and
  interruptible (nothing for one answered at once); `Timer::LockGraceEnd`
  serves parked waiters at the grace's end. Measured on the landed tree
  (`s3-cut-holder-failover-with-locks` at the 20 s lease, two runs
  each): 75 of 108 non-blocking attempts `EAGAIN` before (7ef243f), 0 of 34
  after (one request waited 25.7 s, the whole grace; F-a shortens it).
  `transport-lock-grace`, `transport-lock-grace-queue` (depth 2 and 4)
  and `transport-lock-wait-budget` ran on a ring host, on a real kernel.
  *(c) landed as the second commit* (PROGRESS "Fix: a stale or
  unreachable owner is a wait, not `ENOLCK`"): measured at the production
  lease first (`ENOLCK` on 6 to 7 of 42 attempts before), the stale
  owner's `Busy` and an owner that cannot be reached are waits too, with
  one bound for every wait from the request's start,
  `lock_owner_wait_max_ms` (lease TTL + margin + acquisition retry +
  `lock_grace_wait_max_ms`: 112.5 s at the defaults); a run of polling
  answers backs off to a second; the refusals and waits are no longer
  queued behind a held renewal: 0 errors on 35 attempts in three runs, the
  longest wait 61.5 s, 0 give-ups. Follow-up: every re-issue of a request
  whose owner's link is dead reads the lease once from S3 (not coalesced),
  about one GET per waiter per second after the backoff; with hundreds of
  waiters on a node in such a window, hundreds of GETs a second. Coalesce
  the lease reads in flight per node, or hint `forward_backoff_ms` for an
  unreachable wait.

- **F-c — an application whose lock grant lapsed during a root failover
  gets `EIO`.** In `s3-cut-holder-failover-with-locks` the locker's grant
  (20 s TTL, renewed at half) lapses at about +9.5 s after the cut: the
  holder cannot renew, and the successor only exists at +13.6 s. From then
  the *owner fence* (`lock_owner_fenced`: the lock owner is fenced on
  every file of the mount, until its local locks are gone) answers `EIO`
  to every I/O the lock owner issues, whatever the successor does after
  (documented: cluster-locks.md, "Leases, renewal and fencing"). That is
  an error caused by a fault, an application that holds a lock and
  reads or writes through a root failover, in tension with "under host
  load only latency may degrade". Candidate fixes: park the fenced
  owner's I/O until the successor admits its reclaim (resume: exclusion
  held throughout, nobody else could be granted during the grace) or
  until someone else is granted (then `EIO`); or keep grants renewable
  across the failover gap (the renewal point and the lock TTL against the
  S3-cut failover time, `lease_ttl`). Baseline: the scenario with the
  locker in the harness process (`CONSTELLATION_HARNESS_LOCKER_IN_PROCESS=1`)
  — numbers in PROGRESS, "a fast successor whose clock is ahead got no
  quarantine". Not part of C7-H2. After F-a the grace in which a
  successor admits reclaims is short or absent after an S3-cut failover,
  so the first candidate needs a reclaim rule that does not assume one
  (admitted while nothing conflicting was granted since, by the mirrored
  table); F-b's deferred wait is the machinery a parked owner's I/O can
  reuse.
  *Status (2026-10-20): landed* (PROGRESS "Fix: a lapsed lock holder parks
  instead of failing (plan 43 C7-F F-c)"). The first candidate, with the
  reclaim rule made exact: a holder whose grant lapsed while it pins local
  locks or writes keeps it as *lapsing* (nothing captured or tainted), its
  fenced I/O parks on F-b's `lock-wait` pool (no frontend worker; `EINTR` on
  a signal; `EIO` where it cannot be held or the owner refuses) and a late
  renewal is sent every `lock_poll_ms`; the owner admits it only from a
  *settled* record or its memory of a lapsed one (`LockTables::admit_late`),
  never as a grace's reclaim; the old root sends a **final mirror** of its
  lease when it can add nothing more under it, and a fast successor whose
  takeover replaced that very lease trusts it (`settled` kept, lapsed
  memory installed), any other mirror not. The model has the rule and three
  must-violate variants (`late_admit_unknown`, `late_admit_nonfinal`,
  `late_lapsed_unsuperseded`); the sim two configurations
  (`locks-s3-cut-holder-lapse`, `locks-late-stale-mirror`, with a naive
  successor as the negative control). Not done (the spec's residuals): a
  grant reclaimed in a grace that lapses later still fences, `ack=s3`
  successors (no mirror), a TTL or released takeover, a predecessor that
  died, locks in delegated subtrees whose delegate's authority lapsed with
  the root's.

F-a changes the floor's argument by one step (the floor ends at the
earlier of the bound and the cap, each sufficient on its own); F-b and
F-c only what an application sees while it runs. All three land right
after C7-H2 and before C0 (they touch `note_marker_landed` and
`core/locks.rs`, which C4 rewrites). F-a and F-b touch different code
(F-a: `core/{backup,jobs,lease}.rs`, the beat in `constellation_net`, the
driver's alive targets, the sim's beat loop, the models; F-b:
`core/locks.rs`, `LockOutcome`/`LockAnswer` and their wire form,
`engine/locks.rs`, the view's `lock_acquire`, the FUSE adapter) and meet
only in additions to `event.rs`, `net/message.rs`, `core/mod.rs`'s
config, stats and timers: they are implemented in parallel, in separate
worktrees, and land F-a0, F-a, then F-b rebased on them, the lock
simulations and the scenario re-run on the combined tree (F-b's measured
waits are F-a's grace lengths). F-c starts after F-b (it needs F-b's
wait machinery and F-a's grace lengths).

## 8. Tests, collected

### 8.1 Simulation (`crates/authority/tests/sim{.rs,/}`)

New `SimConfig` fields (`run.rs:147-305`, defaults `false`/empty):
`strict_commit` (I1), `strict_identity` (I2, with I2c), `strict_settle`
(I4), `strict_locks_log` (I3), `trunc_commit_ratio`,
`lock_answer_delay`, `clock_offsets` (per node, C7-H2),
`abandon_after_ms` (the operator escape), and the `AckLog`,
`IdentityLedger` and `ClusterStores` plumbing; I1 is evaluated at the
answerer's emission. New run classification `waiting_for_group` (C0). `strict_durability`
additionally fails on `non_lone_rollbacks > 0` (I5) from C1. New
`FaultKind`s: `Operator(AbandonGroup)` (C0, used from C1),
`ClockStep { who, ms }` and `PartitionLockerStepped` (C7-H1), `StallStep`'s
`after_event` trigger (C2's `tloss-two-node`), and C7a's `RateSkew` /
`Suspend`. The sim's `RebuildReplica` runs the production rebuild (C0).

| Config (new unless marked) | Milestone | Fails before because | Seeds |
|---|---|---|---|
| every `strict_durability` config (existing: `backup`, `ack-s3`, `backup-crash`, `ack-s3-crash`, `backup-departs`, `backup-partition`, `backup-strict`, `backup-hot`, `placement-hot`, `long-backup`, `long-acks3`) | C1 | transient rollbacks of acknowledged shadows/streamed rows at a seal; deposed holder's replays of committed rows | each test's range; `long-backup`×5000 |
| `backup-group-cut-ttl` (+ `-holder-only`, `-backup-only`) | C1 | a TTL claimer of a `Backup` lease gets no tail; acknowledged rows replayed and copied | ×2000 each |
| `long-delegated`, `long-delegated-backup`, `delegated-*` (existing) with `strict_commit` | C2 | a delegate without a backup acknowledges alone | ×2000–5000 (§7 C2) |
| `long-delegated-late-answers` (existing) with all strict flags | C2, C3 | the same; late landings; copies in two queues | ×10000 (incl. the 9287/2945 regressions) |
| `tloss-two-node` | C2 (I1), C3 (I2) | the 2026-10-16 loss shape | ×5000 |
| `delegated-no-backup-root-near` | C2 | Layer A delegate acknowledgements with the root in budget | ×2000 |
| `far_config` (existing) with `strict_commit` and `strict_identity` | C2 | Layer A answers to remote requesters; a deposed lone root's queue holds a remote forward its requester also retries (H1) | 1000..1020 → ×1000 |
| `locks-late-answers` | C4 | grants installed from late messages (I3) | ×5000 |
| every `locks-*` (existing) with `strict_locks_log` | C4 | grants installed from mirrors/handoffs that no record names | ×2000 each; `long_locks` ×1000 |
| `delegated-holder-cut` (seeds 772, 1719 pinned), `delegated-two-gens-root-crash` (existing) with I2c | C3 | own-row replays as root rows; generation holes; (with the naive C3 rule: `TailDiverged` on the overlap, seed 772's order) | ×2000 |
| every flex config (existing) with I5 | C5 | a retired hold owner's rows not classified operator-abandoned | ×2000 |
| `backup-two` | C0 baseline, C6 | rows acknowledged by `{B1}` in the add window of B2 (I1, M4) | ×2000 |
| `fast-takeover-skewed-successor` | C7-H2 | no quarantine for a successor ahead | ×2000 |
| `locks-root-step-forward`, `locks-locker-step-back`, `locks-holder-step-back-sealed`, `fast-takeover-holder-step-forward`, `fast-takeover-successor-step-forward`, `delegated-root-step-forward`, `delegated-delegate-step-back`, `strict-reader-step-back` (+ the controls `locks-root-step-back`, `locks-holder-step-forward`) | C7-H1 | a backward step extends an honoured window; a forward step shortens a waited one or moves F-a's cap | ×2000 each |
| C7's seven sim tests (§7 C7) | C7a/C7b | offsets far beyond the margin | per test |
| `locks-root-rate-fast`, `fast-takeover-successor-rate-fast`, `fast-takeover-holder-rate-fast`, `delegated-root-rate-fast`, `strict-reader-rate-fast` (+ the control `locks-rate-within-budget`); `locks-locker-suspend`, `strict-holder-suspend` (mutant fail-before); `locks-root-restart-step-forward`, `locks-root-step-forward-then-restart`, `locks-root-reboot-step-forward`, `strict-holder-restart-step-forward`, `flex-promiser-restart-step-forward` (+ `locks-root-reboot`); `fast-takeover-successor-restart-in-floor`, `-before-marker`; `holder-restart-step-forward-readopt` | C7a | a waiter's oscillator beyond the unbudgeted clock; a persisted horizon shortened by a step while down or after the last write; a forgotten floor | ×2000 each |

Pinned regressions that must stay green throughout (they prove the
deleted machinery's classes stay closed): the 9287 and 2945 seeds,
`regression_long_delegated_late_answers_seeds_45_1381_3498`,
`regression_backup_crash_classes`, `regression_long_backup_release_in_doubt_and_deposed_journal`,
`regression_refused_forward_is_not_re_executed`, the lock regressions
listed under C4, `regression_f1_stall_seeds`,
`regression_stale_read_after_an_outwait`, `long_delegated_seed_70075_keeps_stream_order`,
`flex_crash_regression_seeds`. A pinned regression whose *assertion*
encodes removed behaviour is rewritten as listed in §7 (C3: the
delegated-holder-cut test); none is deleted.

### 8.2 Models (`crates/model`)

| Model | New property / knob | Violating test (knob off) | Clean test (design) | Milestone |
|---|---|---|---|---|
| `backup.rs` | `acked_never_rolled_back`; `commit_rule` | `today_a_deposed_holder_replays_committed_rows`, `a_ttl_claim_without_the_tail_fetch_rolls_back_an_acked_write` | `commit_rule_two_nodes_is_clean`, `commit_rule_three_nodes_is_clean` | C0 / C1 |
| `delegation.rs` | `acked_ops_are_never_replayed`; `ack_via_root` | `today_a_delegate_without_a_backup_acks_alone` | the four design configs renamed (§7 C2) | C0 / C2 |
| `protocol.rs` | `eio_never_lands_late`; `abandon`; `Protocol::Commit` | `today_an_in_doubt_op_lands_after_its_eio` | `commit_protocol_is_clean` | C0 / C2 / C3 |
| `flex.rs` | `hold_owner_never_deposed_except_by_retire` | (checks Q3's decision; a counterexample stops C0) | `full_rule_is_clean` with it | C0 |
| `locks.rs` | `lock_state_in_log` | `a_late_recall_answer_revives_a_grant_without_log_state` | `lock_state_in_log_is_clean`; `lock_state_in_log_needs_the_grace` violates | C4 |
| `cto.rs`, `locks.rs` | per-node rate, `rate_budget`, the honour/wait/wall split, `beat_span` | a rate beyond the budget; a beat before the renewal without the span; a step judged on the wall clock | rate within the budget; the span; the split | C7a |
| `cto.rs`, `locks.rs`, `flex.rs` | offsets up to a TTL | offsets beyond the margin with the old rules | ρ = 1 000 ppm, offsets up to a TTL | C7b |

Every new test carries an explicit state-count / depth / time cap
(`today_bugs.rs`'s convention) and finishes under ~60 s in release;
larger configs are `#[ignore]` and added to the nightly's
`cargo test -p constellation-model --release -- --ignored` list.

### 8.3 Harness (`crates/harness`)

New scenarios: `ack-latency` (`CAMPAIGN`, measure-only, C0),
`backup-group-isolated` (C1), `takeover-loss-two-node` (C2),
`lock-state-from-log-failover` (C4). Changed expectations (authorized
rewrites, listed in PROGRESS): `deposed-reintegration` gains
`lone_branch_replays > 0` (C5); `no-peer-in-budget` and
`single-node-unchanged` assert `ack.lone` (C5). The ledger
(`verify.rs`): the late-landing allowance only for lone topologies (C2).
`verify::counters` gains `non_lone_rollbacks`, `ack.classes`,
`member_copies`, `client_unsettled_eio`; the errno classifier treats an
`EIO` with a settled `Abandoned` as the documented in-doubt class and an
unsettled one as a failure unless the scenario isolates the requester.

### 8.4 Load campaign (`tools/load-campaign/run.py`)

Per milestone as listed in §7. At C3 and at the plan's end: the full
`smoke` matrix (with the deliberate breaks), phases 2 and 3, and
`known-bugs`, seeds 42–44; the 2 × 3 h soak. A new preset `phase
"commit"` in `PHASES` (C2) bundles the commit-rule scenarios
(`takeover-loss-two-node`, `backup-group-isolated`,
`lock-state-from-log-failover`, `load-probe`, `root-failover-with-delegates`,
`delegate-paused-across-ttl`) under every phase-3 profile plus
`pause:lock-lapse+500:10000@b` and `pause-takeover`.

## 9. Performance gates

The measurements (C0 records each baseline; every milestone compares
against the previous milestone and against C0). p50 and p90 gate; p99
is reported (five runs cannot gate a p99). The state dir is on ext4
except where the four-file-system column says otherwise.

| Gate | What | Budget | Four FS |
|---|---|---|---|
| `ack-latency` (new) | p50/p90 (p99 reported) per op and topology (§7 C0) | topologies 1, 2, 4, 6: ±3 % (or one IQR) at every milestone; topology 3 (delegate, root on the LAN, no backup): ≤ topology 2 + 0.5 ms from C2 (one LAN acknowledgement, measured +2.0 ms p50 on loopback in plan 30 M9: 1.9 → 3.9 ms) — gate G1; topology 5 forwards: + one archive commit (28–190 ms by S3 distance) — gate G2; topology 5 own ops: ±3 % | C0, C1, C3, C4, C7a |
| `delegated-op-latency`, `nonowner-op-latency` | per-syscall medians | the scenarios' own bound (under half an S3 round trip) holds; the delegate's own median ≤ C0's topology-2 holder-own-op p50 + 0.5 ms (G1) | C0, C1 |
| `lock-latency` | first lock from a non-sequencer, cached re-lock, the sequencer's own lock, contended handoff, lone `cluster` vs `local` | first lock ≤ +1 LAN round trip under a backed root (≤ +2 ms on loopback) — gate G3; cached re-lock ±3 %; own lock ≤ +50 µs; handoff ≤ +2 ms; lone ratio ±3 % (C4) | C0, C4 |
| S3 requests | `status.s3` counters over 10 min idle with 100 held grants; `git-under-flock` segment count | idle: equal; segments: ≤ +5 % (C4) | — |
| failover time | `backup-failover`, `root-failover-with-delegates`, `delegate-backup-handoff-failover`, `holder-kill-rejoin`, `ack-s3-failover` | ≤ C0 + 10 % (C1, C3); C7-H2's longer quarantine (gate G5) reported against the logged `wait_ms`; C7b's cold claim (gate G6) reported | — |
| `make perf-gate` | vfs-bench ceilings; harness bench rates (20 % tolerance) | green at every milestone | — |
| `make vfs-bench` | dispatch < 1 µs/op, allocations | unchanged (no milestone touches the dispatch path) | — |
| `harness meta-bench` | `1node-*`, `3node-p2pon-{shared,disjoint}-*` | within ±5 % of C0 except where a gate above explains a delta (reported) | C1, C3, C4 |
| `make perf-regression` | corpus replay, full / latency250 / bw50 | geometric mean within ±5 % of C0 | — |

The four-file-system runs (ext4, xfs, btrfs, tmpfs; the maintainer's
rule) are where a milestone changes what the journal does on disk: C1
(a bool and an option in the journal row), C3 (a tail applied by the
plain record apply instead of re-application), C4 (journal rows per
lock decision), and C7-H1's honour-side commit (clock reads on the FUSE
path; for completeness, `meta-bench` and `vfs-bench`); C0 records the
baseline on all four. Everything runs on the release build, a Linux
test host, the usual harness S3 setup, 5 runs each;
`bench/commit-rule/RESULTS.md` records the table per milestone. A gate
that fails stops the milestone: report the numbers and hypotheses; do
not loosen the bar.

## 10. The maintainer's standing requirements, and how this plan meets them

- **Integrity first.** Every milestone strengthens an invariant
  (I1–I5) checked at the moment it could break, with its fail-before
  evidence; no milestone trades an invariant for latency.
- **Knowing latency costs are explicit maintainer gates.** Each is
  measured on C0's baseline, stated with its budget here, and **the
  milestone does not start until the maintainer accepted it** (recorded
  in PROGRESS); none is a silent relaxation:

  | Gate | Milestone | Cost | Budget | Question |
  |---|---|---|---|---|
  | G1 | C2 | a delegate without a recorded backup goes from local-disk acknowledgement to one LAN round trip (the root's append) | ≤ a backed holder's own-op p50 + 0.5 ms (loopback: ≈ +2 ms over today) | Q4 |
  | G2 | C2 | another node's forward to a lone root (or lone delegate) waits for the archive | + one S3 conditional PUT (28–190 ms by distance), WAN topologies only | Q2 |
  | G3 | C4 | a peer's first lock under a backed root waits for its `LockGrant`'s commit | + one LAN round trip (≤ +2 ms on loopback); the cached re-lock free | Q6 |
  | G4 | C1 (if Q1 strict) | a claim (or a generation end) waits while no member of the old group answers | unbounded until a member answers or the operator abandons; only when the whole group is cut off | Q1 |
  | G5 | C7-H2 | a fast successor always waits the full quarantine bound | ≤ +14.5 s on acknowledgements, ≤ +29.5 s on new lock grants, after strict reads or locks: for a takeover in the predecessor's last seconds, **and** for a takeover after the lease ran out of an S3-cut holder still alive on P2P (its backup seals at the expiry it reads: before C7-H2 that was a TTL takeover with no floor, now it is fast and waits the bound, on top of the ≈ one-TTL failover; applications saw the lock grace as `EAGAIN` on non-blocking requests, `ENOLCK` past the ring's queue depth: F-b made both a wait) | Q10a, **accepted 2026-10-18** with the full floor as landed (safety first); the follow-ups F-a, F-b and F-c (§7 C7-F) take the cost back; `leave`, unmount and the upgrade's handover right after a seal also wait out the floor before they release the lease |
  | G6 | C7b | a cold claimant waits a full TTL from its first read of the lease | ≈ 60.1 s for a cold claim; the seal path and warm claimants unchanged | Q10c |
- **Under host load, only latency may degrade.** The rules remove whole
  recovery paths (replays, folds, conflict copies under group tenures),
  so a false seal under load (F2's class) now costs a reconfiguration
  and nothing else. The delegate's fast path keeps the root's
  write-then-wait shape (C2 §5.3 item 8), so C2 adds no core-thread
  serialization under load. The campaign runs every milestone's
  scenarios under CPU starvation, runtime hogs, store stalls, pauses at
  every protocol boundary, S3 jitter and black holes, WAN and flapping
  links, and clock skew; the checkers (data, ledger without the late
  allowance, exclusion, token order, split brain, errno classes, hangs)
  must be green. A new wait introduced by this plan is bounded or is a
  gate above: a delegate's wait for the root (one round trip; a root
  that does not answer is the existing renewal/lapse path), the lone
  root's archive wait (S3; an S3 outage already stalls `ack=s3`), the
  claim's wait for a group member (G4, with the operator escape), the
  `Abandon` settle (inside the existing 2 × TTL client deadline).
- **No regression on any backing file system**: §9's four-FS column.
- **Fail-before tests**: every milestone lists them; an oracle that
  finds nothing it should is a bug (C0).
- **Fable review**: before each next milestone; C1, C2, C3, C7-H1 and
  C7b at least, as complex changes.
- **No backcompat, no version bumps**: every wire, record and row change
  is in place (`MutateReply.committed`, `SealTail`/`SealedTail`,
  `DelegGroup`, `Recall.through`, the lock records, `Origin`,
  `JournalTx.{gated, tail_of}`, `Code::Abandoned`, the `Abandon`
  request (Q15), `GroupAbandoned`, C7's lease body and token fields); a
  mixed-version cluster is unsupported, as always.

## 11. Open questions for the maintainer

The final list. Each says what it **blocks**; a question that the code
answered is marked **decided** (the plan implements the decision; the
maintainer may still overturn it). Blocking before C0: Q7, Q10a.
Before C1: Q1, Q13. Before C2: Q2, Q4, Q9, Q14, Q15, Q16. Before C4:
Q6. Before C6: Q5. Before C7b: Q10b, Q10c. Before C7-F: Q17 (F-a),
Q18 (F-b). Before C7-H1: Q19. Before C7a's commits 2–4: Q20.

**Answered by the maintainer (2026-10-17): every recommendation below
is accepted** — Q1 (a) strict, Q2 yes, Q4 yes, Q6 yes, Q7 the
recommended order, Q10 (a) land C7-H2 and C7-H1 first, (b) phase 1
only, (c) accept the cold-claim wait, Q15 the request flag; the gates
G1–G6 are accepted at their stated budgets (a gate that fails still
stops its milestone). Q5 stays open until C6's measurement. **2026-10-18:** G5's
full floor accepted as landed in C7-H2, with two follow-ups (§7 C7-F:
F-a, the floor from the successor's own observation of the last renewal;
F-b, lock requests during the grace wait instead of failing; F-c, an
application whose lock grant lapsed during a root failover gets `EIO`)
to land before C0.

- **Q1 — A claim (or a generation end) when no member of the old group
  answers.** *Blocks C1 (G4).* Vertical Paxos requires the new
  configuration to read the old group's tail from a member; today a
  non-listed claimer of an expired `Backup` lease proceeds after 3 s
  without it, and the group's acknowledged rows come back as replays
  and conflict copies. Options: (a) **strict**: wait until a member
  answers `SealTail` (or `DelegSeal`), with `constellation repair
  abandon-group` as the escape; `constellation leave --node-id` of every
  member is equivalent (it already fences their leases and gives up
  their journals); (b) bounded: proceed after `N × backup_takeover_ms`
  and accept the branch (counted). *Recommended: (a)*. It needs the
  holder **and** every backup cut from S3 and from the claimer past the
  lease TTL (a LAN that lost its uplink; both machines down with disks
  intact); the rest of the cluster then waits — availability, never
  acknowledged data.
- **Q2 — Remote requesters of a lone sequencer are acknowledged after
  the archive.** *Blocks C2 (G2).* What "speculation only for the lone
  sequencer's own clients" means: + one S3 PUT on a forward to a root
  (or delegate) with no peer in budget. *Recommended: yes*;
  the alternative (keep requester shadows) re-creates a speculation
  place.
- **Q3 — Epoch holds and designations: decided.** Non-stealable except
  by an operator's retire (`promise.rs:495-507`); supersession of an
  epoch that carried no lease acknowledged nothing. C0's
  `hold_owner_never_deposed_except_by_retire` checks it; a
  counterexample stops C0 (then the hold's answers to members' forwards
  wait for a member's `StreamAheadAck`, never for S3).
- **Q4 — A delegate without a recorded backup uses the root's append as
  its second copy.** *Blocks C2 (G1).* + one LAN round trip
  (≈ +2 ms on loopback) over today's local-disk acknowledgement.
  *Recommended: yes*; measure the alternative (the root as the
  delegate's recorded backup: a backup append instead of the stream
  append, same copy count) only if G1's budget fails.
- **Q5 — Two backups.** *Blocks C6's default.* Keep
  `CONSTELLATION_BACKUPS=1` as the default; decide after C6 has fixed
  the add window (M4) and measured `=2`. *Recommended: keep 1*.
- **Q6 — Lock grants wait for their record's commit.** *Blocks C4
  (G3).* + one LAN round trip on a peer's first lock under a backed
  root. *Recommended: yes* — answering before commit keeps lock state
  outside the commit rule, the class C4 exists to remove.
- **Q7 — Landing order.** *Blocks C0.* *Recommended*: the F2 fix → the
  takeover-loss fix → the seed 9287 and 2945 fixes → C7-H2 → C7-H1 → C0
  → the rest of C7a → C1 → C2 → C3 → C4 → C7b → C5 → C6 (decision).
- **Q8 — Placement and lone delegates: deferred** (§11.1).
- **Q9 — The deadline for `Abandon`: decided.** The existing client
  deadline (2 × TTL) is split (the op's attempts until 1.5 × TTL, the
  `Abandon` until 2 × TTL), so the user-visible bound stays; the
  isolated requester's unsettled `EIO` at 2 × TTL is counted
  (`client.unsettled_eio`) and documented. *Blocks C2* only if
  overturned (the alternative is a 240 s worst case).
- **Q10 — Clock envelope.** (a) *Blocks C0 (G5).* Land C7-H2 and C7-H1
  first: *recommended yes*, C7-H1 with its real scope and its own
  review. (b) *Blocks C7b.* The non-minting token check judges by
  `min(absolute, receipt + remaining)` in phase 1; dropping the absolute
  stamp is a separate decision on campaign data: *recommended* phase 1
  only. (c) *Blocks C7b (G6).* A cold claimant waits a full TTL from its
  first read: *recommended accept* (only a cold `Local`/lone claim pays
  it; every backup and every waiting node already watches the object).
- **Q11 — `TailFollows`: decided.** Kept as the seal record, with
  `through`.
- **Q12 — `gated` per acknowledgement class: decided** (the review's
  H1). `gated := class ∈ {RootGroup, Archive, DelegGroup}`, decided at
  journal time; a lone root's row for a remote forward is Archive-class
  and goes to the member copy on deposition, never to the replay queue.
- **Q13 — Who may seal a holder, and how a self-sealed holder
  recovers: decided** (M2). Any enrolled peer, for the epoch and
  `config_version` the holder holds (the same trust as a backup's seal);
  a self-sealed holder recovers at once and claims its own lease once
  expired, so a claimer that never claims costs at most a TTL.
  *Blocks C1* only if overturned.
- **Q14 — Catch-up at a `DelegGroup` backup switch: decided** (M3). The
  delegate requests a removal only under its `removal_safe`; the root's
  generation end seals every backup any record of the generation named.
- **Q15 — `Abandon` as a request flag or a `MutateOp` variant.** *Blocks
  C2.* *Recommended: a `settle: bool` flag* on `MutateRequest`, the
  inbox entry and the lease-path submit (a variant "executed by rid
  only" would leak into the fast path, `derive_replay_op`,
  `TouchSet::from_op` and the inbox batch format).
- **Q16 — `DelegateStreamAck.through`: the root's journaled cursor:
  decided** (L4). The group without a recorded backup is `{delegate,
  root}`; the delegate cannot observe the root's own backup
  acknowledgements, and does not need to (two copies are the single-
  failure contract). The alternative (the root's committed cursor,
  three copies on a LAN of three, ~2 round trips pipelined) is recorded,
  not planned.

- **Q17 — F-a's approach.** *Blocks F-a.* (a) **The beat cap**
  (recommended): the floor ends at the earlier of the bound and the
  predecessor's lease as its own heartbeat places it on the successor's
  clock, plus M; no change to `liveness_fresh`, one field on the beat;
  it gives back the S3-cut failover (about +1 s over the takeover) and
  the last-seconds case, not a mid-lease crash. (b) The maintainer's
  framing, narrowing `liveness_fresh` to lease writes: a conditional PUT
  every `backup_takeover_ms` while strict reads or locks are served, the
  renewals announced to the backups (the successor must see them early),
  and on a mid-lease crash it saves only the time from the last renewal
  to the marker (the seal's own window and the claim: a few seconds of
  the 8.5 s / 23.5 s). (c) Both, (b) later on top of (a), if a mid-lease
  crash's floor turns out to matter.
  Also: the slack is M (1 s) until C7a names ρ, then `2ρ·(X − sent)`
  (≈ 0.1 s); and F-a0 (mark a tenure that inherits a floor) lands on its
  own, fail-before first: *recommended yes*.
  **Answered 2026-10-18:** (a) the beat cap; the slack is M until C7a;
  F-a0 lands on its own as soon as its fail-before and review are done,
  ahead of F-a (a safety fix does not wait for the latency work). F-a0
  has landed (see F-a0 below).
- **Q18 — F-b's semantics.** *Blocks F-b.* (a) A non-blocking request on
  a lock with no known conflict waits out a grace (and the tenure floor,
  barriers, a delegate's short authority) instead of `EAGAIN`: up to
  23.5 s (29.5 s) after a mid-lease crash at the defaults, about 1 s
  after an S3-cut failover once F-a is in. *Recommended yes* (NLM, NFSv4
  and DLM do; "latency, not errors"). (b) Past `lock_grace_wait_max_ms`
  (49.5 s) the wait gives up with `EAGAIN`, counted; a signal ends it with
  `EINTR` (as a blocking lock): *recommended yes*. (c) A stale owner's
  `Busy` for a free lock (the pre-takeover window of an S3-cut holder,
  today `ENOLCK` after three attempts) waits the same way, bounded the
  same: *recommended yes*, as F-b's second commit, after measuring it in
  the scenario at the default 60 s lease. A contended request past the
  ring's lock-wait budget stays `ENOLCK` (plan 38 Z2c), not in scope.
  **Answered 2026-10-18:** (a) yes, wait; (b) yes, 49.5 s then `EAGAIN`,
  counted, and `EINTR` on a signal; (c) yes, measured first at the
  default 60 s lease, then as F-b's second commit.

- **Q19 — C7-H1's design (specified 2026-10-19).** *Blocks C7-H1.*
  (a) *Scope.* The honour side only, as the text first said, or every
  local duration (honour, wait and liveness)? The measured hazard (+2.5 s
  on the root, 73 of 2 000 seeds) is wait-side, F-a's cap needs both of
  its ends on consistent clocks, and half a conversion leaves honour/wall
  mixes where the two sides meet. *Recommended: every local duration*, in
  four commits, with a fable review after the honour and after the wait
  commit. C7a keeps rate, suspend, boot-id horizons and the models.
  (b) *The honour clock follows forward wall steps.* With it, a guest
  whose monotonic clocks froze and whose wall clock NTP later steps
  forward ends its windows at the step. The cost: a forward step larger
  than what a held window has left ends it early. A lock owner can then
  get the owner fence's `EIO`, read delegations are re-fetched, and the
  lease is renewed at once. *Recommended: follow*, counted
  (`status.clock.steps_forward`) and documented; F-c may later park the
  fenced owner instead.
  (c) *The lease body.* `expires_unix_ms` stays wall, and the body gains
  `holder_expires_ms` and `holder_clock` (recommended). The alternative
  stamps `expires_unix_ms` from the honour clock: no new fields, but after
  a backward step every TTL claimant waits out the step for the rest of
  the holder's process lifetime.
  (d) *Models.* None in C7-H1 (recommended). The models have one clock per
  node, and C7a reworks them with `rate_ppm`. C7-H1's evidence is the sim
  and the core tests.
  (e) *`CLOCK_MONOTONIC_RAW` in the honour and wait clocks now* (one more
  vDSO read, about 19 ns), not in C7a: *recommended now*. Drop it on the
  FUSE hot path only if the vfs or meta bench gate fails, and report it.
  (f) *The seal watch's beat credit* `min(at, sent + M)` mixes the two
  nodes' clocks once `sent` is the holder's honour clock. *Recommended:*
  drop the credit and use the raw arrival (liveness, within `M`),
  re-measuring the beat sims.
  (g) *The harness step.* An in-process step of the daemon's wall
  reading through a step file (stepping the host clock would move every
  container's). The item is `clock:step:<ms>:<at_ms>[@node]`, because
  `@<s>` collides with node targeting. *Recommended.*
  **Answered 2026-10-19:** (a) every local duration — honour, wait and
  liveness — in the four commits; (b) yes, the honour clock follows a
  forward wall step, and an early end is counted and documented; (c)–(g)
  as recommended.

- **Q20 — C7a's design (specified 2026-10-20).** *Blocks C7a's commits
  2–4.* 
  (a) *ρ in the clocks* (WAIT scaled by `1/(1+ρ)`, the honour
  elapsed by `1/(1−ρ)`), not `2ρ·T` added at the ~60 margin sites:
  *recommended* (one place, exact for chained floors; WAIT and HONOUR then
  drift from WALL by ρ·uptime, status only). (b) ρ per node, 1 000 ppm
  (2 000 where the rate source is slewed or unknown), an environment
  override for testing only: *recommended*. (c) Restart horizons on
  `CLOCK_MONOTONIC_RAW` with the boot id, no wall term, the recorded span
  on another boot; a graceful shutdown clears a passed horizon:
  *recommended* (the text above said BOOTTIME and "the longer of the
  two"). (d) No re-persisting on a forward jump: *recommended* (the boot
  stamp makes it moot). (e) M4's own-lease arm always gated: *recommended*
  (a tail before re-adopting an own lease the wall clock reads as
  expired). (f) The takeover floor persisted through the horizon keys and
  a pending-floor key before the fast claim CAS (one fsync before such a
  CAS): *recommended* over a lease-body field. (g) Clock faults in the
  sim's default mix: rates within the budget, steps bounded to ±200 ms
  per node until C7b, suspends of 0.3–3 s, an occasional reboot:
  *recommended*. (h) Seeding `ship.max_epoch` from the applied position
  (and `replay_from` setting it) is not a clock item: *recommended* in C0,
  where "applied, unfenced" gets one meaning for every reader.
  **Answered 2026-10-20:** every recommendation taken.

### 11.1 Deferred (not in this plan)

- **A longer TTL for a `Local` tenure with no `wanted_by`** (the study's
  step 5: nothing fast depends on it). Deferred: it changes TTL-path
  availability, which Q1 and C7b already move; revisit with C7b's data.
- **Placement preferring a non-lone candidate** (the study's step 2a,
  "placement may prefer not to delegate there"; was Q8). Deferred until
  `status.delegation.mine[].ack_class` shows how often a lone delegation
  happens; the placement status decoration with a would-be class is cut
  from C2 for the same reason.

## 12. Risks

- **The recovery paths are the bug source, and this plan rewrites
  them.** Mitigated by order (each milestone tightens one rule while
  every old mechanism still runs, so a missed case is caught by the old
  safety net and by the new oracle at once), by the oracles at the
  moment of violation (not end-of-run), and by C3 — the deletion — coming
  only after C1 and C2 have run the strict oracles at full seed counts,
  and C4 — the new lock records — only after C3, so no lock record ever
  meets a re-execution path.
- **A missed acknowledgement path.** I1 classifies every definitive
  answer independently of the core's own claim; a path the plan did not
  list (an inbox ack, a snapshot control op, a scratch publish) shows up
  as an unclassified answer. `ParkedWhat::Control` (snapshot and admin
  ops) is included: a control op's success waits for the same commit.
- **Availability under Q1's strict rule**: a group cut from everything
  blocks the claimer. Visible (status, WARN) and operator-escapable.
- **Latency on WAN clusters (Q2)** for forwards to a lone root.
  Measured; placement mitigates; the alternative is documented.
- **Member copies accumulate** if the archive never carries an epoch's
  suffix (abandoned groups). Bounded: dropped at the `TailFollows`/tail
  origin, at `GroupAbandoned`, or at a `Recall`; reported with age in
  status.
- **The derived lock table at a fast successor** keeps today's grace
  (windows are clock-bounded); the exact grant set removes the mirror's
  staleness but not the need for the grace (C4's
  `lock_state_in_log_needs_the_grace`).
- **C7 touches every time comparison.** Split into C7a (mechanical) and
  C7b (protocol); the two holes land first as small fixes.
- **The sim's coverage of the engine**: the fast path's `Ungated` rule,
  the `SealTail` transport and the member-copy store's disk behaviour are
  covered by core/meta unit tests, in-process engine tests (the
  `shipper.rs` pattern) and the harness, not by the sim alone.

## 13. The hard problems that remain (study §7), and how this plan bounds them

| Problem | What this plan does | Bound |
|---|---|---|
| Leases need time bounds (locks, read delegations, kernel caches, strict reads) | Grant *identity* moves into the log (C4); windows stay clock-bounded; C7 moves the clock assumption from offset to rate | ρ ≤ 0.1 % on `CLOCK_MONOTONIC_RAW`; margins `2ρT + transit` |
| A holder under a pause | The admission re-check inside the transaction (F1) stays; a sealed group refuses the paused holder's rows; a row journaled after a seal is never acknowledged (I1) | a missed check is a refused proposal, never a lost write |
| Failure detection under load | Unchanged mechanisms (F2 lands first); the commit rule makes a false seal cost a reconfiguration only (no rollback, no replay, no copy) | the seal watch's documented 5T bound (F2) |
| Quorum availability at small sizes | Two nodes: seal + CAS as today; the TTL path now carries the tail (C1) or waits (Q1) | one failure tolerated with a group; the second waits or is an operator action |
| The lone voter | Exactly one speculation place, documented, visible in status (C5); `ack=s3` and designations as the alternatives | Layer A for a lone writer's own clients only |
| Clock envelope | C7 | rate, not offset (Appendix B lists what stays offset-bound: GC's horizon, timestamps) |
| Kernel and store (plan 42 races, `FORGET`, `execve` close, fjall stalls) | Not touched | — |

## 14. Exit criteria

- [ ] Interim work landed or explicitly deferred by the maintainer (Q7):
      the F2 fix, the takeover-loss fix, the 9287 and 2945 fixes, C7-H2,
      C7-H1 (landed in four commits) — each with its fail-before evidence
      in PROGRESS.
- [ ] The maintainer's acceptance of each latency gate (§10, G1–G6)
      recorded before its milestone started.
- [ ] C0: oracles I1–I5 in the sim with their flags, evaluated at the
      answerer's emission; the sim rebuild; the operator escape and the
      `waiting_for_group` classification; the model properties and
      counterexamples; Q3's property checked; the baseline counts per
      config (including `backup-two`); the performance baselines on four
      file systems.
- [ ] C1: committed rows never re-executed after a deposition or a seal;
      the tail fetch on the TTL path; strict durability fails on non-lone
      rollbacks and passes on every backup config; Q1's rule in place.
- [ ] C2: I1 strict on every config except lone topologies; `gated` per
      class; `Ungated` only for lone or epoch-held nodes; `Recall.through`
      and the end-of-generation seal of every named backup; the
      delegate's write-then-wait fast path; `Abandon` and I4 strict; the
      ledger's late allowance only for lone topologies;
      `takeover-loss-two-node` green; the `load-probe` cell that found
      the loss green ×20.
- [ ] C3: I2c (verbatim) strict on every config; the delegate-turned-root
      rule (move, apply verbatim, re-append deduped by origin, end);
      the fold, `Records` copies, holed generations and the deposed-root
      rebuild-replay gone (each deleted function listed with the test
      that pinned it).
- [ ] C4: no lock or delegation state in any message; reclaims and mode
      upgrades journal `LockGrant`; I3 strict on every lock config;
      `long_locks` ×1000 clean; the lock model's `lock_state_in_log`
      clean, its counterexamples violating.
- [ ] C5: I5 strict on every config; the lone branch is the only replay
      producer; the docs' offline-first contract; the full CONVENTIONS
      gates (pjdfstest 8798/8798).
- [ ] C6: the add-window rule with its fail-before; measured; the maintainer's default recorded.
- [ ] C7a, C7b: every exit item of §7 C7.
- [ ] §9's gates met at every milestone, on the four file systems where
      §9 says so; `bench/commit-rule/RESULTS.md` complete.
- [ ] Every §6 document change made in its milestone; ADR-33 and the
      clock ADR written; the amended ADRs marked.
- [ ] Every milestone reviewed (fable) before the next started; the
      reviews' required changes done.

## 15. PROGRESS.md

One section per milestone in the established style (intro paragraph,
"What landed" table, "Deleted" table with, per removed function or
message, the commit that introduced it, the bug it was for and the test
that now shows the class cannot arise, "Unchanged on purpose", "Gates"
with dates, kernel and host file systems), plus the interim fixes' own
sections. The first section, `## Plan 43 C0 — The commit point, named
and checked`, carries the baseline table:

| Config | Seeds | I1 uncommitted (by expected class) | I2 copies | I2c non-verbatim | I4 late landings | I5 non-lone rollbacks |
|---|---|---|---|---|---|---|
| … | … | … | … | … | … | … |

and each later milestone's section the same table after it, so the
progression to zero is the plan's evidence.

## Appendix A — The bugs of 2026-10-01..16 (study §2), mapped

"Impossible" means the state the bug needs cannot exist once the
milestone lands (and the named oracle would catch it if it did).

| Bug (study §) | Shape | Milestone that makes it impossible | Oracle |
|---|---|---|---|
| The 2026-10-16 acknowledged-write loss (§2.1) | delegate's truncate+commit acknowledged on one disk; two copies (stranded delegate rows, the deposed root's `Records` copies); the fold re-executed | **C2** (the delegate's ack waits for the root's append: never on one disk) + **C3** (no fold, no `Records` copy, the deposed root keeps a member copy) | I1, I2, I2c (`tloss-two-node`, `takeover-loss-two-node`) |
| Its review's H1 (a remote sequencer's unjournaled `Conflict`/`LockLapsed` refusal lets a `Records` copy land over a newer acknowledged write) | a second copy executes after a refusal | **C3** (no `Records` copy of a committed row exists); the takeover-loss fix's journaled refusals stay as a general rule | I2 |
| Its review's H2 (a fold's no-op completion misread as the truncate's effect: false conflict) | re-execution logic over copies | **C3** (the fold exists only in the lone branch, which has one copy) | I2c |
| Its review's M3, L6 (a persisted fold destroys `truncate_base`; `[T1, T2, M]` false conflict) | fold machinery | **C3** for group tenures; the lone branch keeps the conditional truncate, whose tests carry over | — |
| Seed 9287 (in-doubt answer for a landed write: a deposed root's journal still carries another generation's row) | two places claim one row | **C3** (the row is a member copy, outside the journal) | `check_in_doubt_answers`, I2 |
| Seed 2945 (a marker logged ahead of its data: a deposed root's replay by rid with its own deps) | re-execution of a copy with the wrong identity's deps | **C1** (a deposed group tenure replays nothing) + **C3** | causal cut, I2 |
| The ledger's late landing (an in-doubt create/rename lands after later acknowledged ops) | a stranded copy re-executes after its client heard `EIO` | **C2** (`Abandon` settles before `EIO`) | I4 |
| F1 (token checked before the writer lock, `b4fe2e6`) | time-of-check vs serialization point | landed; **C4** makes the minter's judgement the serial apply of the derived table; the non-minter window remains (study §2.2) | the lock ghost, `check_landings` |
| F2 / F2b (false seals under host load) | failure-detector false positive | **not addressed** (liveness; the F2 fix in review addresses it); **C1** makes a false seal cost a reconfiguration only | — |
| F3 (a `flush` hangs behind a recalled grant renewed forever) | a local wait without bound; endpoint wedge | **not addressed** (independent; under bisect) | — |
| F4 (successor quarantine in `delegate-backup-handoff-failover`) | a design wait read as load | **not addressed** (independent harness bound); C7-H2 lengthens the quarantine to the bound and the scenario reads the logged `wait_ms` | — |
| F5 (one lease renewal series at half TTL) | liveness through one connection | **not addressed** (independent); **C1**'s tail fetch makes the consequent TTL takeover strand nothing | — |
| F6, F7 (close-wait `EAGAIN`; degraded strict reads) | documented contracts | **not addressed** (independent) | — |
| F8 (stale `now`) | audit | **not addressed**; C4's records follow its rule (decide at the serialization point) | — |
| F9 (fast-path rows journaled after the lease ran out, stranded and replayed) | Layer A acknowledgement | **C1 + C2** for every group tenure (not acknowledged until committed; a sealed group refuses it); remains, documented, for the lone branch (C5) | I1, I5 |
| Plan 41 L0 (a fast-tenure root renewed grants without fresh S3 liveness, `9d00417`) | authority correlated across three time bases | landed; **C4** removes the lock mirror (one of the three); the freshness rule stays (a window); C7-H1 protects it from a backward step | lock ghost |
| `unlinked-exclusion-5681` (an outwaited generation's undelivered grants revived, `c465b07`) | authority state moved by message and reinstalled restamped | **C4** | I3, model `lock_state_in_log` |
| `unlinked-exclusion-7455` (a late recall answer put lapsed grants back, `9d82ad0`/`fb0c909`) | the same | **C4** | I3, `a_late_recall_answer_revives_a_grant_without_log_state` |
| `stale-read-outwaited` (a grant after an outwait carried too old a position, `304c30c`) | floors/barriers carried beside the log | **C4** (the barrier is a field of `LockExpire`); C7b root-anchors its as-of | `turn_read` |
| `lease-cap-fenced` (four nested margins shrank a delegated grant to zero, `17ee59c`) | time bounds | **not addressed** (a window rule, fixed by the poll); C7's `2ρT` margins shrink the clock part of each margin | — |
| `unlinked-under-lock` (`4c5d184`'s first draft adopted unknown grants) | state moved by message | **C4** (leaving grants are `LockMove` records) | I3 |
| `96f4565` (a generation a takeover strand holed; a restarted delegate re-sent forever) | delegate rows re-executed as root rows (new identity) | **C3** (the delegate's own rows are appended as its generation's rows) | I2c |
| `strand-takeover-quiescence` (the same family) | the same | **C3** | I2c |
| `c465b07`'s second half (an expired record kept a grant out) | the copy rule `install_if_consistent` | **C4** (no copies) | I3 |
| `backup-crash` seed 606255 and 603322 (Local acknowledgements while a backup could be had) | an acknowledgement on one disk | landed (the bring-up rule); **C2** generalizes it (`Ungated` waits) | I1 |
| `long-backup` 802943 (a non-backup claimed at expiry while the backup held an acknowledged create) | TTL claim without the tail | landed partly (the claim grace); **C1** (the tail fetch, Q1) | I1/I5 (`backup-group-cut-ttl`) |
| `long_backup` 56774, `acks3` 700087, `flex-crash` 2140, `long-delegated-late-answers` 1381 (in-doubt answers vs the log) | which store's outcome to trust | landed; **C3** removes the deposed-journal branch they patched | `check_in_doubt_answers` |
| Plan 30 bug B (phantom effects of a dead holder) and bug A (at-least-once forwarding) | — | landed in plan 30; kept (rids, `Completed`) | `check_exactly_once` |
| The clock holes found for C7 (a backward wall step extends an honoured window; a skewed fast successor gets no quarantine) | offset dependence | **C7-H1**, **C7-H2** (standalone, first) | C7's sim tests |
| Plan 42's races, the partial `FORGET` reap (ADR-31), orphan/`O_PATH` fixes, `F_GETLK`, OFD/`flock` at `RELEASE`, the `execve` close deadlock, fjall/lsm-tree stalls, io_uring hangs, mtime/ctime, staging, `copy_file_range`, READDIRPLUS, CSI, CI (§2.11) | kernel, store, transport | **not addressed** (irrelevant to the commit rule, study §2.11) | — |

## Appendix B — Clock inventory: where time is compared

From the clock-envelope research (C7). Clock: W = wall
(`now_unix_ms`, `SystemTime` plus the fault offset), I = `Instant`/tokio
(`CLOCK_MONOTONIC`), S3 = the object store's clock. Breaks: S = safety,
L = liveness only.

| # | Site | Compared | Clock | If offset > margin | Rate-only re-expression |
|---|---|---|---|---|---|
| 1 | Root lease, holder side — `engine/lease.rs` `usable`, `core/lease.rs` `usable`, `store-s3/lease.rs` `granted`/`renewed` | own stamp vs own clock | W | L (a backward step on the holder: honours longer = S) | honour until `renew_sent + ttl − M` on the honour clock; `expires_unix_ms` for status **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 2 | Root lease, claimant — `Lease::is_claimable(now)` in `classify`/jobs, `live_leases_held_by`, `fence_retired` | holder's `expires_unix_ms` vs claimant's W | W↔W | **S** (a claimant ahead takes over while the holder writes) | ETag-observation rule; `released` unchanged; `fenced` flag; `ttl_ms` in the body |
| 3 | Heartbeat promise — `heartbeat.rs` `Promise::outlasts`, `takeover_check`; `promise.rs` | promiser's `no_epoch_until` vs holder's `expires`, read by a third node | W↔W | **S** (split brain through an epoch) | request-anchored promises ("d from receipt"); `ask-<taker>` nonce without P2P |
| 4 | Lock grant, holder side — `core/locks.rs` (`sent + ttl − margin`), `HeldGrant::until_ms`, `renew_point`, `due_renewals`, `fenced` | own send stamp vs own clock | W | L for offset; **S** for a backward step / suspend | honour clock (C7-H1 first) **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 5 | Lock grant, owner side — `until = recv + ttl + margin`, `restamp`, expiry timers, `drop_expired`, quarantines, graces | own receipt stamp vs own clock | W | L (a forward step on the owner: early outwait = S; 73 of 2 000 seeds at +2.5 s, the F-a review) | wait clock (C7-H1); margin `2ρ·ttl` + transit (C7a) **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 6 | Fencing token at a non-minting executor — `LockToken::until_ms` judged by `check_tag`/`check_tag_at_write`; `unsettled_writes`; token memory | holder's absolute window end vs executor's clock | W↔W | **S** (an executor behind runs a stale op after the next grant) | `remaining_ms` judged from receipt; `min` with the absolute stamp in phase 1 |
| 7 | Read delegations — `readdeleg.rs`, `write_gate.rs` `deleg.valid`, `model/cto.rs` | own stamps vs own clock | W | L / a step = S as 4–5 | clock change only; the cto model gains a rate knob **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 8 | Write delegations — `delegate.rs` `until = sent + ttl − margin`, `deleg_lapse_at`, `reclaim_horizon`, `grant_cap_ms` | own stamps vs own clock | W | as 4–5 | clock change only; `until + 2M` becomes `until + 2ρ·ttl + transit` **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 9 | Lock cut / barriers — `DelegRenew::stream_head_at` folded into `lock_cut_here`, `lock_barrier_ready` | delegate's W vs root's W | W↔W | **S** (a delegate ahead: the floor misses a row) | root-anchored (send time of reply k − 1) |
| 10 | Fast-takeover floor — `note_fast_takeover`, `note_marker_landed` `prev_expires.min(..)` | predecessor's `expires` vs successor's W | W↔W | **S** (a successor ahead gets no quarantine) | the bound alone (C7-H2 first; **landed**: the floor is the bound alone, `acquire_won` and the models classify fast by the permit, row 26) |
| 11 | `ack=s3` fast takeover timing — `core/lease.rs:296` | holder's renewal stamp vs taker's W | W↔W | L (the slot CAS fences) | ETag unchanged for `backup_takeover_ms` |
| 12 | Holder liveness (plan 41 L0) — `liveness_fresh`, `note_s3_liveness`, `strict_answer_allowed`, `LeaseView::reads_locally` | own send stamp vs own clock | W | **S on a backward step > 1.5 s** | honour clock (C7-H1 first) **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 13 | Backup seal watch, holder heartbeat, `OffCore::core_responsive`, `note_holder_heard` | own arrival stamps vs own clock | W | L | wait clock (C7-H1) **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 14 | Restart horizons — `KV_READ_GRANT_HORIZON`/`KV_LOCK_GRANT_HORIZON`, `load_*_quarantine` | own earlier W vs own later W across a restart | W (persisted) | **S on a forward step across the restart** | **C7a landed (commit 3):** a boot stamp `<wall> <span> <boot id> <raw until>`, read on `CLOCK_MONOTONIC_RAW` on the same boot and as the full span on another (or unknown, or an older value: the default span); a graceful shutdown deletes a passed horizon; the promise's `issued` likewise; the takeover floor is commit 4. (The text before it: `(boot_id, BOOTTIME until)`, else the full horizon (C7a; C7-H1 writes them as wall and reads them back onto the wait clock, so only a step while the node is down, or a forward step while it is up after the last write followed by a restart, remains). The promise's `issued` (`promise.rs:366-378`, the KV `epoch_promise_issued_ms`, judged by `promise_join_begin`) is the same kind of horizon. Also: a successor's takeover floor (C7-H2) is not persisted at all (`set_quarantine` in memory), so a successor that restarts inside it, or whose upgrade handover gives up its release, forgets it: persist it under these keys.) |
| 15 | Epoch claim views — `epoch_closed_claim`, `epoch_reclaim_pending`, `resolve_epoch_claims` | own lease stamp vs own clock; resolution by epoch | W | L | clock change only; **rate half: C7a landed** (commit 2) |
| 16 | Singleton leases (`_snapsched`, GC) — `singleton.rs`, `snapsched.rs` | holder: own; claimant: holder's W vs own W | W↔W | L (the CAS on the holder's ETag is the safety) | observation rule if wanted; not required |
| 17 | GC — `gc.rs:407` `last_modified ≤ now − horizon` (7 d), completion retention, `mtree_gc.rs:653`; holds' `expires_unix_ms` | S3 `Last-Modified` vs W; hold writer's W vs GC's W | S3↔W, W↔W | S only at offsets of days (or the hold slack) | **stays** (documented tolerance) |
| 18 | Inbox `submitted_unix_ms`, registry `last_modified`, designation `created_unix_ms`, names, stall stamps, status | stats, naming | W | none / L | leave |
| 19 | HLC ctime stamps (`hlc.rs`, 300 s tolerance), atime clamp | peer's W vs own W | W↔W | none (timestamps) | leave |
| 20 | Kernel attr/entry TTLs, `dentries.rs` filter, session waits, ReadIndex deadlines, forward timeout | kernel / `Instant` | I | L | already monotonic (`Instant` stops in suspend: conservative there) |
| 21 | Fault hook `CONSTELLATION_FAULT_CLOCK_OFFSET_MS` (reaches `now_unix_ms` and rows 1–17) | — | — | capped at ±500 by the campaign (`load.rs:366`) | becomes a constant-offset test (harmless at ±5 s); add rate and step hooks (**landed**: `CONSTELLATION_FAULT_CLOCK_RATE_PPM`, C7a commits 1 and 2) |
| 22 | Sim `tests/sim/clock.rs` `Clock::skewed` (constant offset), `SimConfig::clock_skew_ms` (plus `clock_offsets`, C7-H2); models' `max_offset`, `Jump` | — | — | — | add per-node `rate_ppm`, suspend and step (**C7a landed**: commits 1 and 2) |
| 23 | Released-takeover lock grace — `lock_on_released_takeover`'s gate in `acquire_won` (`takeover && p.released && !p.is_expired(now)`) | released lease's `expires_unix_ms` (its W) vs the taker's W | W↔W | **S** (a taker ahead by more than the released lease's remaining TTL gets no lock grace, while the releaser's grants are honoured) | grace by the taker's own observation of the release, or always (the grace is bounded by `lock_ttl`): C7b |
| 24 | Seal watch's expiry compares — `on_takeover_get`, `known_expiry`/`lag_discount`, `watch_due_by_expiry` (`backup.rs`) | the holder's `expires` (its W) vs the backup's W | W↔W | L (after C7-H2 an early seal is safe: it gets the whole floor; a late one is latency) | wait clock plus the ETag observation: C7b, with row 13 |
| 25 | `promise_check_needed` — `!prev.is_expired(now)` (`promise.rs`) | predecessor's `expires` vs the taker's W | W↔W | L (a sealed taker ahead does a promise check it would skip; only conservative) | with row 3's anchored promises: C7b |
| 26 | `acquire_won`'s fast classification — `fast_prev` (`!p.is_expired(now)`) | predecessor's `expires` vs the successor's W | W↔W | was **S** (a sealed backup ahead by more than the lease's remainder took a TTL takeover: no floor) | **fixed (C7-H2)**: a takeover the permit allowed is fast whatever the clock says; the expiry is no longer kept |
| 27 | Fast-takeover floor's beat cap (C7-F F-a) — `HolderBeats` (`note_beat`), `note_fast_takeover`, `note_marker_landed` | the predecessor's `expires`/`guard` minus its beat's `sent` (one clock), added to the beat's arrival `at` (the successor's) | W−W (holder), W (successor) | none for offset (a difference on one clock plus a stamp on the other); a backward step on the holder lengthens its honour (C7-H1); a forward step on the successor shortens every wait-side window alike; **a forward step `J` on the holder moves the cap early by `J` (unsafe when `J > 3M + δ`, the holder beating on and sealed mid-lease): the same class as the root's own grant table (C7-H1), which breaks exclusion with or without the cap** | already rate-only: `2ρ·(X − sent) ≤ M/2` for beats within `250·M`. C7-H1: `sent`, the named expiry and the guard on the holder's honour clock with its clock id; the lease body's `holder_expires_ms` replaces `expires_unix_ms` in `X`; `at` and the floor on the successor's wait clock; lags combined within one clock id: closes the forward step **C7-H1 landed** (the local half); **rate half: C7a landed** (commit 2: ρ is in the honour and wait readings) |
| 28 | A reader's `on_read_holder_learned` — `!l.is_claimable(now)` (`readindex.rs:1754`) | the holder's `expires_unix_ms` vs the reader's W | W↔W | **S** (a reader ahead takes a live holder for gone and answers a strict read with a tail to head, missing the holder's unshipped acknowledged writes) | with row 2: C7b |
| 29 | `Grant.confirmed_ms` across a move (`core/locks.rs:4806, 5093` keep it; `meta/src/locks.rs:1357` compares it with a token's `until_ms`) | one sequencer's receipt stamp vs the holder's window, on a third node | W↔W | S at offsets (row 6's class) | with row 6: C7b |
| 30 | Lock barriers across nodes, both ways (`DelegateStream.leaving_barriers`, `LockHandback.barrier`, `DelegRenewed.lock_barrier`/`lock_cut_at`; `core/locks.rs:4711-4737, 4916/5006, 2212-2248`) | one node's record end vs another's cut as-of | W↔W | S (row 9's class) | with row 9: root-anchored, C7b |
| 31 | The fencing token in an S3 inbox batch and in the persisted replay queue (`core/inbox.rs:490, 1541`; `replay.rs:759`) | the holder's window vs the executor's clock, after up to `inbox_deadline_ms` or a restart | W↔W | S (row 6) | C7b: these paths have no transit bound, so `remaining_ms` from receipt does not apply; keep the absolute check or leave them to the minter's exact judgement |

## Sources

- In-repo: `docs/explanation/ARBITER-STUDY.md` (§2, §4–§7);
  `docs/explanation/{GOALS,DESIGN,DECISIONS}.md`;
  `docs/reference/features/{forwarded-mutations,durability-and-failover,delegations,cluster-locks,cto-modes}.md`;
  `docs/plans/v1/done/30-write-path-resilience-and-scale-out.md`; plans
  40, 41, 42; `docs/plans/v1/PROGRESS.md`;
  `docs/how-to-guides/development/TESTING.md` (host-load campaign,
  authority simulation); `crates/authority/src/core/{backup,holder,client,delegate,replay,jobs,lease,locks,readindex,stream,promise,placement}.rs`,
  `crates/authority/src/{event,action,segment,replica}.rs`;
  `crates/meta/src/{record,mutate,replay,session,locks,readdeleg,delegation,reintegrate,rid}.rs`,
  `crates/meta/src/store/{spec,local,journal,backup,bootstrap,held,inbox}.rs`;
  `crates/store-s3/src/{lease,log,heartbeat}.rs`;
  `crates/engine/src/{authority_driver,lease,p2p,recovery}.rs`,
  `crates/engine/src/view/write_gate.rs`;
  `crates/authority/tests/sim{.rs,/}`; `crates/model/{src,tests}`;
  `crates/harness/src/{verify,load,hostlag,scenarios}.rs` and
  `scenarios/{m9,m11,m14,delegroot,campaign,campaign_load}.rs`;
  `tools/load-campaign/{run.py,inject.sh}`; `bench/remote/RESULTS.md`
  (failover times); PROGRESS's plan 30 M9 measurements (the LAN backup's
  acknowledgement cost).
- The reviews of the paused fixes (the takeover-loss fix, the F2 fix),
  the host-load timing review (F1–F11), the notes on seeds 9287 and
  2945, and the clock-envelope research (summarized here; not in the
  repository).
- External, as cited by the study (§8) and the clock research: Delos
  (OSDI'20); Vertical Paxos (Lamport, Malkhi, Zhou 2009); PacificA;
  Flexible Paxos; Aurora; Neon; RIFL; Gray & Cheriton, "Leases" (SOSP
  1989); Liskov, "Practical uses of synchronized clocks" (PODC 1991);
  Ongaro's thesis §6.4.1; Chubby (OSDI 2006); ZooKeeper sessions; FaRM
  (SOSP 2015); Spanner (OSDI 2012); Kleppmann on fencing tokens;
  `clock_gettime(2)` and chrony's `maxslewrate`.
