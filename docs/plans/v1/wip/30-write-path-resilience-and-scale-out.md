# Plan 30 — Exactly-once, recoverable, session-consistent writes, then scale-out authority

Read `docs/plans/v1/CONVENTIONS.md` first, then plans 28 and 29 (the
metadata plane this plan builds on).

**Execution override (user, 2026-09-22).** As in plan 29:
- Each milestone is executed by a subagent.
- The coordinator verifies it and **commits per milestone**
  (`git -c commit.gpgsign=false`). Subagents never commit.
- **No backward compatibility is required at any level.** Bucket objects,
  on-disk state, P2P wire messages, CLI flags, env knobs and tests may all
  change freely. Mixed-version clusters are unsupported, so all nodes upgrade
  together, and there are no migrations. Delete tests that no longer apply.
- **Formal modelling uses Stateright** (`stateright` 0.31).
- **Every harness run uses `CONSTELLATION_HARNESS_DOCKER_PREFIX=constellation-harness-p30`,**
  so it cannot tear down a concurrent session's environment.
- **Run long commands in the foreground with explicit timeouts.**

Every milestone leaves the tree releasable. The coordinator may stop after
any phase boundary (§4) and still have a consistent system.

## 1. Why

The architecture is sound. The bucket is the only arbiter, and
multiple writers get linearizable metadata without a second coordinator.
Almost no other system in the field does this: the S3-backed systems that
give many writers low-latency commits (WarpStream, Kafka diskless topics,
Bufstream, Neon, Materialize) all run a separate consensus store. What is
wrong today is in the write path. Two correctness bugs and a set of
structural limits follow from three properties:
- one sequencer for the whole namespace;
- acks that exist in one place until S3 has them;
- reads with no freshness barrier.

### 1.1 Correctness bugs

**A — forwarded mutations are at-least-once.**
- `request_mutate_with` maps a timeout to `Busy`
  (`crates/cli/src/forward.rs`, the `tokio::time::timeout` arm).
  `mutate_op_rebasable` then acquires the lease and executes the op again
  locally (`crates/cli/src/fusefs.rs`, the `Busy | NotHolder` arm followed
  by `require_lease_for` + `execute_mutate`).
- If the holder executed the op before the 500 ms timeout, the second
  execution sees the op's own effect. `O_EXCL` create, `mkdir` and `link`
  return `EEXIST`; `unlink`, `rmdir` and `rename` return `ENOENT`. Lock-file
  protocols (git's `index.lock`) then fail spuriously and leave a stale lock
  behind.
- The trigger is a slow holder: suspend, I/O stall, or WAN latency spikes.
  It is therefore likelier with intercontinental nodes.

**B — stranded forwarded ops leave phantom state.**
- A requester applies an accepted op to its replica immediately
  (`forward::apply_accepted`). The shadow row is retired only when matching
  records arrive from the log (`shipper.rs`, `shadow_retire_matching`).
- If the holder dies before shipping and another node takes over, those
  records never arrive, and their effects stay in the requester's replica
  for good.
- The consequences:
  - The requester can publish them. `mtree_publish` builds commits from
    dirty keys and has no notion of shadows, so a node that bootstraps from
    such a commit inherits entries the log never contained.
  - If the requester becomes the next holder, it validates new ops against
    the phantom state and answers `EEXIST` for a name that never existed.
    The M6 `Exists` hint then copies the phantom entry to other nodes.
- The holder side has the same shape. A holder publishes commits whose
  trees can include effects of journal records not yet shipped. A deposed
  holder's replica keeps its stranded journal's effects until reintegration
  classifies them.

### 1.2 Structural limits

| # | Limit | Consequence |
|---|---|---|
| L1 | One lease and one sequencer for the whole namespace (plan 29 M0a) | Throughput is capped by one node's fjall writer. Every other writer pays a round trip to it (a WAN round trip across continents). Only one node ever runs at local speed. |
| L2 | Acks exist in one place until S3 has them | Stranded branches (bug B). Conflict copies after a holder failure. |
| L3 | Failover is timed by the TTL | A dead holder blocks every writer for 30–60 s. FUSE returns `EIO` after 2×TTL of stall. |
| L4 | No read barrier, no session guarantees | Close-to-open is really bounded staleness: `open()` does no revalidation (`fusefs_ops.rs`). Measured p50 16 s and p99 21 s of cross-node visibility after a burst on EC2 (`bench/remote/RESULTS.md`, row 6). Each session anomaly (M6 `Exists`) needs its own point fix. |
| L5 | Continuation epochs need every write-eligible node | With one bucket they are the only way to keep writing through a bucket outage, and they get harder to form as N grows. |
| L6 | Strict mode (cross-node `fcntl`/`flock`) is not implemented | SQLite or any other lock-based app on two nodes can corrupt data. |
| L7 | One unrecoverable pending chunk blocks every manifest from publishing (plan 29 M6 cascade) | One bad file halts a node's metadata sync. |
| L8 | Every write-eligible node publishes commits | Redundant head/condemned-list reads and planning work that grows with N. |

## 2. Hard constraints

1. **Portable S3 only.** GET, PUT, LIST and DELETE, plus `If-None-Match`
   and `If-Match` on PUT (what `constellation doctor` probes). Nothing
   AWS-only: no S3 Express One Zone, no reliance on conditional DELETE, no
   `RenameObject`.
2. **One bucket.** No second bucket for disaster recovery, and no quorum
   across buckets.
3. **Works with a single node.** Every mechanism degrades to today's
   single-node behaviour when there are no peers.
4. **Works across continents.** No mechanism may put a WAN round trip on
   every write. Peers are chosen by measured RTT, never by assuming a LAN.
5. **Safety never depends on failure detection** (ADR-12). Only CAS and
   time-bounded leases or promises decide who may write. A false suspicion
   may cost availability, never correctness.
6. **Minimize S3 requests and bytes.** Every milestone that touches an S3
   path reports request counts per op (use `CountingProxy`).
7. **Model first.** Every protocol milestone first extends the Stateright
   model (M1) and makes it pass, then writes code.

A milestone that cannot meet a constraint stops and reports; it does not
bend the constraint.

## 3. Target design (one page)

- **Every mutation has an identity.** Each mutation carries a request id
  `rid = (node, incarnation, seq)`. The record batch it produces includes
  `Completed{rid}`, so any successor that tails the log knows which requests
  took effect. A timed-out request is *in doubt* and is retried under the
  same id, never re-executed blindly.
- **The replica equals a log prefix plus explicit speculation.** Every
  effect not yet in the durable log is written with its before-images in a
  node-local speculation log. That covers requester shadows, hints, a
  holder's unshipped journal, streamed-ahead records and delegate records.
  Speculation retires when the log confirms it. On stranding it is rolled
  back and the stranded ops are replayed by id. Commits are only ever
  published from log-prefix state.
- **Positions and sessions.** Replies carry the position they were evaluated
  at. A node never answers from a state older than one it has observed.
  In `cto=strict` mode, `open()` is covered by a read delegation or a
  ReadIndex round trip to the owning sequencer.
- **Durability comes in layers matched to the topology.**
  - Layer A (always, no added latency): requesters keep what they were
    acked and replay it after a takeover.
  - Layer B (only when a peer is within an RTT budget): a synchronous
    backup, with seal-based failover that skips the TTL.
  - Layer C (opt-in): `--fsync-mode s3`, and `ack=s3`, which acks only
    after S3 has the records.
- **Authority.** One root lease in S3 and one log. The root delegates
  subtrees to their dominant writers over P2P. Delegates sequence at local
  speed and stream records to the root, which appends them in
  dependency-respecting order. Cross-subtree ops recall delegations; there
  is no two-phase commit.
- **Availability through a bucket outage.** Held leases (until their TTL),
  offline designations, and continuation epochs, which with flexible
  quorums can form with up to f nodes missing.
- **P2P.** Direct QUIC streams carry the log; gossip carries only membership
  and digests.

| Topology | Added ack latency | Holder crashes, returns before takeover | Holder away past takeover | Failover |
|---|---|---|---|---|
| Single node | none | nothing lost | n/a | n/a |
| Only distant peers (no backup) | none | nothing lost | forwarded ops replayed by their requesters; the holder's last un-fsynced moments come back as its branch | TTL |
| A peer within the RTT budget | +1 round trip to the backup | nothing lost | nothing lost | detection + 1 CAS (~1–2 s) |
| `ack=s3`, any topology | +1 S3 round trip per op (group-committed) | nothing lost | nothing lost | detection + 1 CAS |

## 4. Milestones

| Phase | Milestone | Fixes / delivers | Depends on | Size |
|---|---|---|---|---|
| 1. Correctness | **M0** Reproduce A and B | failing scenarios, fault knob, per-node S3 switch | — | S |
| | **M1** Stateright model v1 | finds A and B as counterexamples | M0 | M |
| | **M2** Exactly-once forwarding | bug A | M1 | M |
| | **M2b** Stop cancelling the holder's ship round | ship starvation under forwarding load (found measuring M2) | M2 | M |
| | **M3** Speculation log, stranded-op recovery, clean commits | bug B, both sides | M2b | L |
| | **M4** Hygiene and history checkers | L7, L8, error codes, checkers | M3 | M |
| 2. Test infrastructure | **M5** Sans-IO authority core + deterministic simulation | deterministic tests of the real code | M4 | L |
| 3. Consistency | **M6** Positions and session guarantees | L4 (sessions) | M5 | M |
| | **M7** Direct log streams, visibility | L4 (visibility) | M6 | M |
| | **M8** `cto=strict`: read delegations and ReadIndex | L4 (close-to-open) | M7 | M |
| 4. Resilience | **M9** Backup peer, seal failover, `ack=s3`, pre-S3 streaming | L2, L3 | M8 | L |
| | **M10** Flexible-quorum continuation epochs | L5 | M9 | M |
| 5. Scale-out | **M11** Delegated sub-sequencers | L1 | M10 | XL |
| | **M12** Hot directories | L1 (shared dirs) | M11 | M |
| | **M13** Forwarding through S3 without P2P | P2P-off ping-pong | M2 | M |
| | **M14** Strict mode: cross-node `flock`/`fcntl` | L6 | M11 | L |
| | **M15** Exact chunk-location reconciliation | cooperative cache precision | — | M |
| 6. Close-out | **M16** Docs, ADRs, real-S3 verification | — | all | M |

M13 and M15 are independent and may be scheduled earlier if convenient.

### Gates

- **Every milestone:**
  - `cargo fmt --all` with no diff.
  - `cargo clippy --workspace --all-targets -- -D warnings`.
  - `cargo test --workspace`, which includes the model crate.
  - The harness scenarios the milestone touches, plus its new ones.
  - PROGRESS.md rows and an exit-criteria checklist in the established
    style.
- **Phase boundaries (after M4, M9, M12 and M16):** the full CONVENTIONS
  gate list, including `bash tests/smoke.sh`, `bash tests/integration.sh`,
  the full `harness run` (every scenario in `SCENARIOS`), and pjdfstest
  8798/8798.
- **Measurements:** every milestone that touches an S3 path records
  requests and bytes per operation; every milestone that touches the write
  path records `harness meta-bench` numbers before and after.

---

### M0 — Reproduce A and B

**Goal.** Scenarios that fail today for exactly the reasons in §1.1, and
pass once M2 and M3 land. There are no fixes in this milestone.

**Deliverables.**

1. **A holder-side fault knob, `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`**
   (default 0). In `node_runtime.rs`'s `SyncRequest::Mutate` task, after
   `outcome` is computed and before `reply.send(outcome)`, sleep for the
   configured time. The keepers lock is already released at that point, so
   a handoff can proceed while the reply is held back.
   - Read the value once.
   - `tracing::warn!` once at startup when it is non-zero.
   - Document it in `docs/reference/configuration.md` under a new "Fault
     injection (testing only)" heading.

   This is the only product change in M0. Why a knob: the holder's
   `HandOff` arm and a forwarded execution race for the keepers lock, so a
   SIGSTOP-based trigger is not deterministic.

2. **A per-node S3 switch.** Extend `reqlog::CountingProxy` with `cut()` and
   `heal()`:
   - `cut` closes every live relayed connection and immediately closes new
     ones.
   - `heal` restores normal relaying.

   A client whose endpoint is `counter.endpoint()` can then lose S3 while
   the others keep it.

3. **A known-bug registry.** Add `pub const KNOWN_BUG_REPROS: &[Scenario]`
   in `crates/harness/src/scenarios.rs`.
   - `harness list` prints these under "known-bug reproductions (expected
     to FAIL until fixed)".
   - `harness run <name>` resolves names in both lists.
   - `harness run` with no names runs `SCENARIOS` only.
   - The milestone that fixes a bug moves its scenario into `SCENARIOS`,
     unchanged, as the regression test.

   Generalize `wait_for_p2p` and `ensure_no_conflicts` to take slices.

4. **`forward-timeout-reexec` (bug A).**
   - Setup: two nodes with their own node keys,
     `CONSTELLATION_LEASE_TTL_MS=10000`, and
     `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS=1500` on both.
   - Five rounds, alternating which node holds the lease: `O_EXCL` create,
     `mkdir`, `unlink` of a file every node sees, `rename`, and `link`.
   - Each round:
     1. confirm the holder with `lease_of`;
     2. run the op on the other node;
     3. expect the POSIX-correct result (success);
     4. verify the namespace shows exactly one execution.
   - Non-vacuity: the requester's `forwarded_err` must rise in every round.
     If it doesn't, fail with "fault injection did not engage".
   - The failure message lists every anomalous round: op, errno, and a
     one-line diagnosis ("holder executed, reply timed out, requester
     re-executed").

5. **`holder-crash-phantom-shadow` (bug B, a third node takes over).**
   - Setup: nodes A, B and C with their own keys and
     `CONSTELLATION_LEASE_TTL_MS=6000`. A's S3 goes through a
     `CountingProxy` switch.
   - Steps:
     1. A writes a marker (takes the lease), and all three nodes see it.
     2. Cut A's S3.
     3. B runs `create_new("phantom")`, which must succeed (forwarded,
        acked by A).
     4. `kill9(A)`.
     5. C writes `after` and takes over once A's lease expires.
     6. Wait until B sees `after`.
   - Assert that B and C agree on whether `phantom` exists, within 20 s.
   - Then:
     1. Unmount B cleanly (it publishes a commit).
     2. Mount a fresh node D, which bootstraps from the head commit.
     3. Wait until D sees `after`.
     4. Assert that D agrees with C.
   - Expected today: B ≠ C. D usually inherits the phantom; the scenario
     reports whether it did.

6. **`holder-crash-phantom-new-holder` (bug B, the requester takes over).**
   - Setup: the same as item 5, but B writes `after` and becomes the holder.
   - C's `create_new("phantom")` must then succeed, because the name was
     never created in the durable history.
   - Expected today: `EEXIST`, and C then carries the phantom too, via the
     `Exists` reply.

7. **Docs.**
   - TESTING.md: add a "Known-bug reproductions" subsection.
   - PROGRESS.md: add rows, with the verbatim failure lines of the three
     scenarios.

**Gates.** The standard milestone gates, plus:
- all three new scenarios FAIL with the documented diagnostics;
- `forwarded-mutations`, `lease-handover`, `kill9-remount`,
  `deposed-reintegration` and `mkdir-p-race` still PASS (the knob is off by
  default).

### M1 — Stateright model of the authority protocol

**Goal.** An executable model of lease, forward, ship, tail, takeover,
shadows and publish that finds bugs A and B as counterexamples. Every later
protocol milestone extends it before any code changes.

**Crate.** New workspace crate `crates/model` (`constellation-model`),
depending on `stateright = "0.31"` and nothing from the product. Add it to
`members` and `default-members` so `cargo test --workspace` runs it. No
product crate depends on it.

**Model.**

- **Actors:**
  - two or three `Node`s;
  - one `S3`, holding:
    - a lease register with an etag CAS;
    - log slots `seq → (epoch, records)` with create-if-absent;
    - a commit pointer: `published_state` plus its `applied` position.
- **Network:** Stateright's lossy, reordering network for P2P messages.
  S3 requests are messages to the `S3` actor.
- **Time:** logical ticks drive the forward timeout, lease renew and expiry,
  and the idle publish.
- **Faults:**
  - a bounded number of fail-stop crashes, with a variant that restarts
    with the durable journal intact;
  - a pause, where a node takes no steps while its timers keep running.
- **State:**
  - the namespace is a small set of names, plus one directory for `mkdir`
    and `rename`;
  - ops are `create_excl`, `unlink` and `rename`;
  - each node has a role, a lease view, a journal, an applied position, a
    replica, shadows, and in-flight client ops.
- **Today's behaviour is modelled faithfully:**
  - the holder executes and journals, and ships later;
  - requesters apply shadows immediately;
  - tailing applies segments in order, with epoch fencing;
  - takeover requires tail-to-head;
  - a timeout falls back to lease acquisition and local re-execution;
  - any node may publish its replica with its applied position.
- **Client histories** are recorded with Stateright's
  `LinearizabilityTester`, against a sequential spec of the namespace with
  POSIX errnos.

**Properties.**
1. `linearizable` (always). Bug A violates it.
2. `converged_at_quiescence` (always): if no op is in flight and every
   shipped segment is applied everywhere, every live replica equals the
   state derived from the durable log. Bug B violates it.
3. `commits_are_log_prefixes` (always): every published state equals the
   log-derived state at its claimed position. The bug B publish path
   violates it.
4. `progress` (sometimes: all ops complete). This guards against vacuous
   passes.

**Variants.** A `Protocol` enum selects the behaviour: `Today`, then
`ExactlyOnce` (M2), `Recovery` (M3), `Positions` (M6), `Backup` (M9),
`FlexEpochs` (M10), `Delegation` (M11).

**Tests.**
- `today_finds_bug_a` and `today_finds_bug_b` assert that the checker finds
  a counterexample, and print the path.
- `single_writer_is_clean`: forwarding off, one writer, all properties hold.
- CI budget: each test ≤ 60 s under `cargo test -p constellation-model
  --release`. Larger configurations are `#[ignore]`.

**Docs.** The crate-level doc maps every model action to the code path it
abstracts. For example: "Forward timeout → `request_mutate_with` returns
`Busy` → `mutate_op_rebasable` → `require_lease_for` → `execute_mutate`".

**Exit.** Both counterexamples found; the model's code mapping reviewed by
the coordinator.

### M2 — Exactly-once forwarded mutations (fixes A)

This is the RIFL design (SOSP'15) applied to forwarding.

**Identity.**
- `Rid { node: u64, incarnation: u32, seq: u64 }`.
- `incarnation` is persisted in the `local` keyspace and bumped at every
  mount before serving.
- `seq` is per-incarnation and monotonic.
- Every `MutateOp` a FUSE call issues gets its rid at the top of
  `mutate_op_rebasable`, and keeps it whether it runs locally, is
  forwarded, or is retried.

**Completion records.**
- Executing an op appends `LogRecord::Completed { rid }` in the same fjall
  transaction as the op's records. It ships with them.
- Applying `Completed` inserts `rid → position` into a `completed`
  keyspace on every replica. That keyspace is node-local, is never
  published into the tree, and survives a re-bootstrap (node-local
  keyspaces are kept).
- **Coverage rule.** An in-doubt op may only be resolved against a
  `completed` table that has seen every record since the op was first
  sent.
  - Log GC therefore retains every segment younger than the completion
    retention window, whatever the head commit says.
  - A node that re-bootstraps across a gap marks its in-doubt ops as
    unresolvable, and they fail with `EIO`. They are never re-executed.
- Refusals are not recorded. A retried refused op is re-evaluated, and
  takes effect (or not) at the retry, which is linearizable.

**Holder.**
- Before executing a forwarded op, the holder checks `completed` and an
  in-memory map of recent outcomes. The map keeps an `Accepted` reply's
  records, so a retry gets an identical reply.
- An executed rid is never executed again.

**Requester.** A timeout, a transport error, or `Busy` leaves the op in
doubt. The requester retries the **same rid**, in this order:
1. the same holder, with backoff (three attempts or 2 s; a slow holder is
   more common than a dead one);
2. a redirected holder;
3. only then the lease path. After `TailedToHead`, it looks the rid up in
   `completed`. If found, it returns success without executing; if not, it
   executes locally with the same rid.

Only an explicit refusal (`Errno`, `Conflict` or `Exists`) ends an op
without execution. A `SetManifest` rebase is a new op with a new rid.

**GC.**
- Requests carry `acked_through`, the highest contiguous seq of this
  incarnation whose reply arrived. The holder drops recent outcomes up to
  that seq.
- Every replica prunes `completed` entries older than
  `CONSTELLATION_COMPLETION_RETENTION_S` (default 900).
- An in-doubt op older than the retention fails with `EIO`, never with a
  re-execution. The FUSE deadline (2×TTL) keeps ops far inside the window.

**Model.** The `ExactlyOnce` variant: `linearizable` holds.

**Tests.**
- Holder dedup, as a unit test.
- The in-doubt retry, in-process on two replicas, with the M0 knob.
- `completed` retention.
- `kill9-remount` must not reuse rids (incarnation bump).
- Harness: `forward-timeout-reexec` passes and moves to `SCENARIOS`.

**Measure.** Forwarded-op latency on `meta-bench` 3-node configs must stay
within ±10%. Record segment bytes per op before and after.

### M2b — Stop cancelling the holder's ship round (found while measuring M2)

**Finding (coordinator, 2026-09-23).** In `node_runtime.rs`'s sync loop,
the in-flight sync round (`run_managed_sync_round`) is polled in a
`select!` against `sync_rx`. Every request other than `Nudge` breaks out
of that loop and **drops the round**. That includes the holder's
`SyncRequest::Mutate` for each forwarded mutation, and a requester's
`SyncRequest::Forward`.

Under forwarding load a request arrives every ~0.7 ms, while a round takes
~2 ms (one S3 PUT), so the holder almost never finishes a round:
- An instrumented `3node-p2pon-shared-create-lat0` run entered ~1,200
  rounds, and only 5 reached the end of `sync_all`.
- The journal backlog sat at 1,000–1,600 records.
- Every restarted round re-read and re-encoded the whole backlog, and
  started a PUT, before being cancelled again.

M2's extra `Completed` record doubles that wasted work, and that is the
+0.4–0.5 ms per forwarded op M2 measured. The costs:
- starved shipping, which delays S3 durability and cross-node visibility;
- a growing stranding window (bug B's exposure);
- wasted CPU.

**Why it isn't a one-liner.** `run_sync_round` holds the keepers lock
across `renew_if_due` (an S3 CAS) and `sync_all` (an S3 PUT). A forwarded
execute needs that lock, so simply *not* cancelling the round would make
every forwarded op wait on S3.

**Design.**
1. **Don't interrupt the round for spawn-only requests.** `Mutate`, and a
   `Forward` that goes over the network, only spawn tasks (plan 29 M5), so
   dispatch them from inside the round's `select!` loop without dropping
   the round. Other requests keep today's behaviour.
2. **Stop holding the keepers lock across S3 I/O on the ordinary ship
   path.** Read the ship epoch under the lock, release it for the PUT, and
   re-take it for acks and bookkeeping. The ship mutex already serializes
   shipping against handoff. The invariant the lock protects ("an accepted
   op never lands after the *final* flush of a release/handoff") is kept
   by holding the lock across flush + release CAS on the release and
   handoff paths only.
3. **Renew outside the lock.** Executing under a still-valid lease while
   its renewal CAS is in flight is safe; the lease view is updated when
   the CAS returns.

Update the model (M1) if the lock scope changes what it abstracts.

**Tests.**
- A harness scenario, `holder-ships-under-forward-load`: 3 nodes, a
  sustained forwarded create burst. The holder's `journal_backlog` in
  `status` stays below a small bound throughout, and followers see creates
  within 2 s.
- A unit or in-process test proving a forwarded execute cannot land
  between a release's final flush and its release CAS.

**Measure.** `meta-bench` 3-node P2P rows against the pre-M2 baseline,
run several times on an idle host. This milestone must bring M2's rows
back within ±10% of the pre-M2 baseline. Also count completed rounds per
second on the holder.

### M3 — Speculation log, stranded-op recovery, clean commits (fixes B)

**Definitions.**
- *Speculative* state is any effect in `ns` that the durable log does not
  (yet) contain:
  - requester shadows;
  - `Exists` hints;
  - a holder's unshipped journal;
  - from M9, records streamed ahead of S3;
  - from M11, delegate records not yet appended by the root.
- The capture hook is `ns_insert`/`ns_remove` in
  `crates/meta/src/store/ns.rs`, the funnel every `ns` write already goes
  through. Extend the `Dirty` parameter (or add a sibling parameter) with a
  capture context.
- When capture is active, each write records the key's before-image
  (`Option<value>`) in a node-local `spec` keyspace, in the same
  transaction. Entries are `{spec_seq, kind, before: [(key, Option<value>)],
  usage_delta}`. Kinds:
  - `Shadow { rid, epoch }`;
  - `Hint { floor }`;
  - `Local { journal_seq, rid }`;
  - `Foreign { segment_seq, records }`: a log segment applied while older
    speculation is outstanding, captured so it can be redone.
- Journal rows and shadow rows store `(rid, MutateOp, records)`. The op is
  what replay re-executes.

**Retirement.**
- A shadow retires when its `Completed{rid}` arrives from the log. A hint
  retires when the applied position passes its floor. A local entry retires
  when its record ships.
- When the oldest outstanding entry retires, delete every `spec` entry up
  to the next outstanding one, captured foreign entries included.

**Stranding.**
- Requester side: applying a segment whose epoch is higher than an
  outstanding shadow's epoch strands every lower-epoch shadow.
- Holder side: deposition (a renew finds another holder, or a takeover
  after a pause) strands every outstanding local entry.

**Recovery**, run from the sync task:
1. **Roll back.** Restore before-images in reverse `spec_seq` order down to
   the earliest stranded entry, restoring usage deltas too, in one
   transaction.
2. **Redo.** Re-apply the later non-stranded entries in original order:
   foreign entries from their captured records, and still-valid speculation
   by re-applying its records.
3. **Replay by rid.** Send the stranded ops, by rid and in original order,
   to the current sequencer (or execute them locally if this node is now
   the holder). M2's dedup makes this exactly-once even if another node
   also replays the same rid.
4. **Refusals.** A refused replay (for example, the name now exists) is
   materialized exactly as reintegration does today: a
   `.constellation-conflict/` copy and a status counter. That leaves only
   genuine overlaps as a conflict source.

**Takeover gate.** `LeaseKeeper::commit` for a takeover requires that no
stranded speculation remains (recovery runs first). A new holder therefore
never validates against phantom state.

**Commits are log prefixes.**
- A node with outstanding speculation does not publish, except the holder.
- The holder publishes its snapshot with every key that carries unshipped
  local speculation replaced by that key's earliest before-image. That is
  exactly the log-prefix state at its last shipped position.
- A test helper rebuilds the root by replaying the log up to `applied`, and
  asserts equality in tests.

**Deposed holder.** `reintegrate::classify` is replaced by rollback plus
replay-by-rid of the deposed holder's journal.

**Performance gate.** Holder-side capture must cost ≤ 10% of single-node
`create` and `write4k` throughput (`harness meta-bench`, `harness bench`).
If it costs more:
- disable holder-side capture;
- on deposition, rebuild the replica from the head commit instead;
- record the decision and the numbers in PROGRESS.md.

**Model.** The `Recovery` variant: `converged_at_quiescence`,
`commits_are_log_prefixes` and `linearizable` all hold.

**Tests.**
- A property test in `crates/meta`: random interleavings of speculative
  entries, foreign segments, retirements and strandings. The final `ns`
  must equal a reference built by replaying the log from scratch plus the
  surviving speculation.
- Harness:
  - both phantom scenarios pass and move to `SCENARIOS`;
  - `deposed-reintegration` is updated: conflicts appear only for true
    overlaps;
  - `mkdir-p-race` still passes (hints are speculation now).

### M4 — Hygiene and history checkers

1. **Error-code robustness.**
   - Add a fault-injecting `ObjectStore` wrapper in `store-s3` tests
     (`InMemory` plus scripted 412, 409, 404, 500 and timeouts).
   - Every CAS site handles the codes distinctly: lease create/swap,
     segment create, commit create, designation, registry, and condemned
     lists. 409 retries the same attempt, 404 on `If-Match` re-reads, and
     412 is a lost race.
   - `doctor` records per-provider behaviour for these codes and warns on
     unknown semantics.
2. **Poison-record isolation.** When a pending upload is unrecoverably
   missing:
   - hold back only that inode's records, plus records that depend on them
     (same keys, transitively), and ship the rest;
   - show the held-back set in `status` and the web UI;
   - add `constellation repair drop-held <ino>`, which discards the records
     into a conflict copy.

   Plan 29 M6's characterization test flips to "other inodes still publish".
3. **Only the lease holder publishes commits.** A follower clears its dirty
   keys once a head commit's `applied` covers its own applied position.
   Measure S3 requests on an idle and a busy 3-node cluster with the
   counting proxy.
4. **`doctor` reports bucket versioning** (informational; nothing relies on
   it).
5. **History checkers in `crates/chaos`:**
   - exactly-once: no op takes effect twice, and none reports failure after
     taking effect;
   - convergence at quiescence, including a freshly bootstrapped replica;
   - Elle-style dependency-cycle detection over rename and link histories.

   Wire them into `chaos-ci` and `chaos-soak-4`.
6. **Path visibility.** `status` shows each peer's path kind and whether
   several paths are live (iroh 1.x on noq does multipath). Document the
   failover from a direct path to a relay.

**Phase 1 boundary.** Full gate list.

### M5 — Sans-IO authority core and deterministic simulation

**Goal.** Test every later protocol milestone deterministically, against the
real code.

**Extraction.**
- Move the authority decision logic into `crates/cli/src/authority/` as a
  sans-IO state machine: `Core::handle(Event) -> Vec<Action>`.
  - The logic comes from `node_runtime.rs` (the sync loop's forward,
    mutate, handoff and acquire arms), from `shipper.rs` (only the ship,
    tail and publish *decisions*), from `lease.rs` and from `forward.rs`.
  - Events: FUSE mutation submitted, P2P message received, S3 result, timer
    fired, segment tailed.
  - Actions: send P2P, issue an S3 op, reply to FUSE, apply records,
    schedule a timer.
- In scope: lease transitions, forwarding (in-doubt retry, holder dedup and
  execute), the speculation lifecycle, and ship/tail/publish sequencing.
- Out of scope, wired as today: GC, prune, atime, the cooperative cache,
  pins and designations.
- `node_runtime` becomes the core's IO driver. This must preserve
  behaviour: every test and scenario is green before and after.

**Simulation.** A `sim` test module runs N cores on a current-thread tokio
runtime with paused time. It uses:
- real `Meta` (`open_in_memory`);
- real `LogStore`, `LeaseStore` and `CommitChain`, over a simulated object
  store (`InMemory`, seeded latency, scripted error codes, per-node
  reachability);
- a P2P bus with seeded delay, drop, reorder and partitions;
- seeded op generators.

Recorded histories are checked with Stateright's `LinearizabilityTester`
(used as a library), plus M1's convergence and commit-prefix properties.

**CI.** A fixed seed set runs under `cargo test` (≥ 1,000 seeds in ≤ 2
min); a long randomized run is `#[ignore]`. A failing seed prints a replay
command.

**Stretch.** Stateright node actors that delegate to the real `Core`, so the
model checker explores the production decision code. Do it if the event
interface allows it without IO.

**Exit.**
- All tests and scenarios are green.
- The sim contains regression seeds for A and B, and they pass.
- The coordinator reviews the extraction boundaries.

### M6 — Positions and session guarantees

- **Positions on replies.** Every `MutateReply` variant carries
  `position`: the holder journal position the op was evaluated at (for
  `Accepted`, the position of its records). It replaces `ship_floor`.
- **An `observed` watermark per node.** It is the maximum of positions from
  replies, ReadIndex answers (M8) and applied segments.
  - Reads (lookup, getattr, readdir, open, readlink, getxattr, listxattr)
    wait until `applied ≥ observed`, unless speculation covers the keys.
  - The wait is bounded by `CONSTELLATION_SESSION_WAIT_MS` (default 2000).
    On timeout, answer anyway and log once (degraded, not an error).
  - Mutations are unaffected, since the sequencer validates them.
- **`Exists`** becomes an instance of the general rule (a hint entry plus
  the wait).
- **Model:** the `Positions` variant satisfies read-your-writes and
  monotonic reads per node.
- **Chaos checker:** per-node monotonic reads and read-your-writes.
- **Measure:** the read-latency distribution, which should be ~0 when idle.

### M7 — Direct log streams and visibility

- **Log streams.** The holder serves `LogSubscribe { from_seq }` over P2P,
  streaming *shipped* segments in order.
  - Subscribers apply them through exactly the tail code path, fencing
    included.
  - On stream loss they fall back to S3 GET-next and resubscribe.
  - Slow subscribers are dropped back to S3 tailing.
  - Gossip `SegmentPublished` stops carrying log payloads; gossip keeps
    membership and digests.
- **Visibility root cause** (EC2 row 6):
  - add tracing spans at ship, stream send, receive and apply;
  - add a harness scenario, `visibility-after-burst` (3 nodes, a large
    write burst, then a paced marker series), asserting cross-node
    visibility p99 < 2 s;
  - re-check on EC2 in M16.
- **Measure:** S3 tail GETs while streams are up should be ~0 (counting
  proxy).

### M8 — `cto=strict`: read delegations and ReadIndex

- **Mount option `--cto bounded|strict`.** Default `bounded`; decide the
  default in M16 from measurements.
- **ReadIndex.** In strict mode, `open()` on a node that isn't the
  sequencer asks the owning sequencer (the holder until M11) for
  `(position, current inode record)`. The node installs the record as a
  hint if it is newer, then waits per M6.
- **Read delegations.** The sequencer may grant a short read delegation on
  a file (attrs and manifest) or a directory (entries), in the ReadIndex
  reply or on request.
  - TTL `CONSTELLATION_READ_DELEGATION_TTL_MS` (default 5000), renewed
    while in use.
  - While a node holds one, opens and lookups under it are local.
  - Before acking any mutation that touches an inode delegated to another
    node (the sequencer's own writes included), the sequencer recalls the
    delegation (P2P recall plus ack), or waits out its TTL if the
    delegation's holder is unreachable.
  - Delegations are void when the sequencer's epoch changes.
- **Topology costs:**
  - single node: nothing;
  - LAN: about one round trip on a first open;
  - across continents: files nobody else writes cost one WAN round trip on
    first open, then nothing. A write-then-open cycle across continents
    costs one WAN round trip, which is the minimum strict close-to-open
    allows at that distance.
- **Tests:**
  - a close-to-open checker in `crates/chaos`: a read that starts after
    another node's close completed must see it;
  - a harness scenario, `cto-strict`: the writer closes, signals the reader
    out of band, and the reader opens and must see the new content;
  - the same scenario in bounded mode, documenting the staleness.
- **Measure:** open latency for a LAN non-sequencer, recall rate, and the
  effect on write latency.

### M9 — Backup peer, seal-based failover, `ack=s3`, pre-S3 streaming

**Lease object** (format change):
`{holder, epoch, expires, backups: Vec<NodeId>, config_version, ack_policy: Local|Backup|S3}`.

**Backup selection.**
- Candidates are peers whose measured RTT to the holder is within
  `CONSTELLATION_BACKUP_RTT_BUDGET_MS` (default 5).
- Prefer the peer that has been connected the longest. Keep at most
  `CONSTELLATION_BACKUPS` (default 1).
- No candidate means `backups=[]` and `ack_policy=Local`, which is today's
  behaviour.

**Append path.**
- The holder streams journal batches (`rid`, op, records, journal seq) to
  its backups.
- Backups persist them in a `backup_tail` keyspace and ack.
- The holder acks FUSE calls and forwarded requests only after every backup
  has acked, group-committed every few hundred µs.
- Backups trim on shipped notifications.

**Reconfiguration.**
- A backup that misses acks for `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS` is
  removed by a lease CAS (`config_version+1`) before the holder acks
  anything further.
- Adding a backup is a CAS plus a snapshot of the unshipped tail, which the
  backup must hold before it counts.

**Failover.** A backup that hasn't heard from the holder for
`CONSTELLATION_BACKUP_TAKEOVER_MS` (default 1500):
1. **seals**: persists "epoch e sealed" and refuses further epoch-e
   appends;
2. CASes the lease to `{self, e+1, new backups}` (this fails harmlessly if
   the holder reconfigured first);
3. tails S3 to head, adopting old-epoch segments below its first slot;
4. re-ships the rest of its backup tail, deduplicating by rid;
5. resumes.

Safety: after the seal, the old holder cannot collect a write-all ack, so
it cannot ack anything.

**`ack=s3`** (mount option and per-filesystem policy). Every mutation is
acked after its segment is CAS-created, group-committed. The lease records
`ack_policy=S3`, which allows takeover without waiting for the TTL: the log
slot CAS fences the old holder. This is intended for servers near the
bucket.

**Pre-S3 streaming.** Once a batch is backup-acked, the holder streams it
to subscribers (M7), who apply it as speculation (M3); it retires when it
ships. Visibility no longer waits for S3.

**Model.** The `Backup` variant (with `AckS3`): no acked op is lost under
any single failure, and the model stays linearizable and converged.

**Harness scenarios.**
- `backup-failover`: 3 LAN nodes; `kill -9` the holder mid-burst. Failover
  under 3 s, zero lost acked ops, zero conflicts, and in-doubt ops complete
  exactly once.
- `backup-departs`: the backup unmounts; the lease is reconfigured and
  writes continue.
- `no-peer-in-budget`: budget 0 gives today's behaviour and TTL failover.
- `ack-s3-failover`: fast takeover without any backup.
- `single-node-unchanged`: P2P off, one node; no behavioural or performance
  change.

**Measure.** Added ack latency on a LAN, S3 requests per reconfiguration,
and the failover-time distribution.

**Phase 4 boundary.** Full gate list.

### M10 — Continuation epochs with flexible quorums

- **`epoch_slack` (f).** Set with `fs create --epoch-slack` and a new
  `constellation fs set epoch-slack`, stored in `meta.json`. The default 0
  keeps today's rule.
- **Heartbeats become promises when f > 0.**
  - `heartbeat/<node>` holds `{node, no_epoch_until_unix_ms}`, refreshed
    every `CONSTELLATION_PROMISE_REFRESH_S` (default 5).
  - A node persists a promise locally before publishing it.
  - A node joins an epoch only after its own last published promise has
    expired.
  - Members publish no promises while their epoch is open.
- **Epoch formation.** An epoch needs N − f members of the write-eligible
  roster, each with an expired promise. It holds only leases that a member
  held validly when it formed.
- **Takeover.** Taking an expired lease via S3 additionally requires at
  least f *other* nodes with unexpired promises, read after the lease's
  expiry plus a margin. Otherwise the node refuses and waits.
- **Validation.** The promise TTL must be ≤ lease TTL / 4.
- **Model.** The `FlexEpochs` variant: S3 takeover and epoch formation
  never produce two concurrent authorities, under partitions and crashes.
- **Harness:**
  - `epoch-missing-node`: 3 nodes with f = 1. Two lose S3 and form an
    epoch while the third is offline, and they keep writing. When the third
    returns with S3, it must not take the lease the epoch holds.
  - `epoch-slack-zero-unchanged`.
- **Measure:** heartbeat PUTs per node per day with f = 1.

### M11 — Delegated sub-sequencers (one log)

**Delegation state.**
- New log records `Delegate { dir, node, gen }` and `Recall { dir, gen }`
  maintain a replicated delegation table.
- The root renews delegations over P2P with the designation-delegation TTL
  mechanics (5 s).
- Delegations never overlap. There is no sub-delegation in this plan.

**Ownership.**
- A `0x02|parent|name` key belongs to the delegation containing `parent`.
- An inode's keys (`0x01`, `0x03`, `0x04`) belong to the delegation
  containing its primary link's parent.
- An op whose keys span two owners is *cross-subtree*.
- Ownership is resolved by a local ancestor walk.

**Delegate execution.**
- The delegate validates against its replica, which is authoritative for D
  because every mutation under D goes through it.
- It writes speculation (M3): its records are speculative until the root
  appends them.
- It acks under its own durability layer: a backup chosen by RTT to the
  delegate (M9), or Layer A.

**Append path.**
- The delegate sends an ordered stream `(gen, rid, op, records, deps)` to
  the root.
- The root appends in stream order after checking the generation and key
  ownership, without re-validating.
- **Dependencies:** a forward carries `deps` (delegate → highest ack
  position the requester has observed), and the root appends a record only
  after everything in its `deps`. Reads from the root's log need no
  tracking.

**Recall.**
- Cross-subtree ops go to the root: renames and hard links across
  delegations, renaming or `rmdir` of a delegated root.
- The root recalls the involved delegations first: the delegate drains its
  stream and stops, and an unreachable delegate is waited out by its TTL.
- The root then executes the op and may re-delegate. There is no two-phase
  commit.

**Placement** (ADR-15 generalized).
- The root sees every record's origin. It delegates a subtree to a node
  once that node writes ≥ 70% of the subtree's ops over the window, and the
  subtree's rate passes a threshold.
- Recall happens when the pattern changes, with dwell and hysteresis.
- Delegate the highest directory that one node dominates, to avoid a swarm
  of tiny delegations. Ceph's SC'15 finding says to delegate to the
  dominant writer, not to spread load.

**Designations** become non-stealable delegations (the same machinery,
without TTL expiry while designated).

**Model.** The `Delegation` variant: per-key linearizability; the
causal-cut property (no replica state contains a record whose `deps` are
missing); recall safety.

**Harness scenarios.**
- `delegated-subtrees`: three nodes, each writing its own subtree.
  Per-node op latency is close to local, aggregate throughput scales, and
  the replicas converge.
- `cross-subtree-rename`
- `delegate-crash`, with and without a backup
- `marker-order`: data written in D1, then a marker in D2. A reader never
  sees the marker without the data.
- `root-failover-with-delegates`

**Measure.** Throughput against the M9 baseline, locally and on EC2 (M16).
S3 request counts must stay unchanged.

### M12 — Hot directories

- **Commutative parent attributes.**
  - Each node runs a hybrid logical clock (HLC).
  - Records carry HLC stamps, and parent mtime/ctime updates become `max`
    merges.
  - Parent nlink and child counts become additive deltas, applied
    commutatively on replay (a record-format change).
- **Conflict keys.** In `KeyGate` and in the sequencer, a create or unlink
  takes `(parent, name)` plus a *shared* hold on the parent. rmdir,
  renaming the parent, and setattr on the parent take it exclusive.
- **Shared ingest directories.** When M11's placement sees several nodes
  each writing ≥ 20% of a hot directory's creates, split its name-hash
  space into ranges delegated to those nodes (GIGA+).
  - `readdir` is unaffected, since the log is single.
  - A rename across ranges goes through the root's recall path.
- **pjdfstest** must remain a full pass: timestamps must still increase.
- **Harness:** `shared-dir-multi-writer` (4 nodes creating unique names in
  one directory): throughput against a single sequencer, plus correctness.

**Phase 5 boundary** (scale-out core). Full gate list.

### M13 — Forwarding through S3 when P2P is unavailable

- **Inbox objects.** A requester that can't reach the holder over P2P
  writes batched ops (with rids) as CAS-created objects
  `inbox/<epoch>/<node>/<n>`.
- **The holder polls** each known requester with GET-next and idle
  backoff, and executes batches in order.
- **Outcomes ride the log**: `Completed{rid}`, plus
  `Refused { rid, errno }` for refusals. The requester already tails the
  log, so it reads outcomes there.
- **Effect:** this replaces lease ping-pong in P2P-off clusters.
- **Measure:**
  - `create-storm-s3-only` and the P2P-off `meta-bench` configs, against
    the 41–57 ops/s baseline;
  - S3 requests per op.

### M14 — Strict mode: cross-node `flock` and `fcntl`

- **Kernel hooks.** Implement FUSE `getlk`, `setlk` and `setlkw`, and
  enable flock handling.
- **Lock table.** The owning sequencer (holder or delegate) holds the lock
  table, keyed by `(node, lock_owner)`.
- **Locks are leased.** They are renewed with the node's P2P heartbeat. A
  node whose lock lease expired fails further I/O on those files with
  `EIO` until it re-acquires the lock, which fences it NFSv4-style.
- **Blocking locks** queue at the sequencer; waiters are woken over P2P.
- **Failover.** Lock state is replicated to backups (M9). Without a backup,
  a grace period after the sequencer changes lets nodes reclaim their locks
  before new ones are granted.
- **Tests:**
  - `flock-cross-node`;
  - `sqlite-two-nodes`: concurrent writers on one database, then
    `PRAGMA integrity_check`, with no lost commits;
  - existing POSIX lock tests.

### M15 — Exact chunk-location reconciliation

- **Replace the bloom digests.** Use range-based set reconciliation
  (Negentropy-style range fingerprints) over each node's chunk set, so
  peers know one another's holdings exactly.
- **Keep or drop the blooms by measurement:** digest bytes per second,
  false-positive peer fetches (target: 0), and CPU.
- **Regression:** `coop-cache-hit`, `web-fleet` and `existence-peer-hint`
  stay green.
- **New counter:** false-positive peer fetches.

### M16 — Docs, ADRs, real-S3 verification, close-out

- **DESIGN.md** (coordinator only):
  - rewrite §4–§6 and §9 for the new write path: rids, speculation,
    positions, delegations, durability layers, flexible epochs;
  - state the portable-S3 and single-bucket constraints;
  - remove the stale partitions text.
- **DECISIONS.md:** add ADRs for
  - exactly-once forwarding;
  - the speculation log;
  - layered durability and seal failover;
  - delegated sub-sequencers;
  - flexible epochs;
  - portable-S3-only and single-bucket (this retires plan 28 §P12);
  - leaseless OCC stays deferred, with the conditions under which to
    revisit it.
- **Reference docs:**
  - `forwarded-mutations.md`;
  - `lease-placement.md`, now per subtree;
  - `configuration.md`, with every new knob;
  - new feature pages for `cto` modes, backups, delegations and strict
    locks.
- **EC2** (if hosts are available; see `bench/remote`):
  - rerun Phase A rows 1, 2, 5 and 6;
  - add failover-time, visibility, delegation-throughput and S3-requests-
    per-op rows;
  - record everything in `bench/remote/RESULTS.md`.
- **Wrap-up:**
  - decide the `cto` default from measurements;
  - run the full gate list;
  - move this plan to `done/`.

## 5. Out of scope

- AWS-only features: an S3 Express journal or tier, conditional DELETE,
  `RenameObject`.
- More than one bucket: disaster-recovery copies, multi-bucket quorums
  (CASPaxos or Disk Paxos across providers).
- Leaseless optimistic commits (plan 28 §P3). They stay deferred: a
  portable commit takes 28–190 ms, and the OCC systems that work
  (FoundationDB, Aurora DSQL, Tango, Aria) rely on a commit path in the low
  milliseconds. Revisit only if M11's delegations can't cover a workload
  with no locality.
- Byzantine members.

## 6. References

- Exactly-once:
  - RIFL (SOSP'15): https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf
  - CURP (NSDI'19): https://www.usenix.org/system/files/nsdi19-park.pdf
- Configuration master plus replicas:
  - Vertical Paxos: https://www.microsoft.com/en-us/research/wp-content/uploads/2009/05/podc09v6.pdf
  - PacificA: https://www.microsoft.com/en-us/research/wp-content/uploads/2008/02/tr-2008-25.pdf
- Sessions and reads:
  - ZooKeeper (ATC'10): https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf
  - Raft ReadIndex (Ongaro §6.4): https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
  - Paxos quorum leases: https://dl.acm.org/doi/10.1145/2670979.2671001
  - Hermes: https://arxiv.org/abs/2001.09804
  - Bodega: https://arxiv.org/abs/2509.07158
- Delegation and ownership:
  - Farsite (OSDI'06): https://www.usenix.org/conference/osdi-06/distributed-directory-service-farsite-file-system
  - Ceph subtrees (SC'04): https://ceph.io/assets/pdfs/weil-mds-sc04.pdf
  - Mantle (SC'15): https://users.soe.ucsc.edu/~carlosm/dev/publication/sevilla-sc-15/sevilla-sc-15.pdf
  - Zeus (EuroSys'21): https://www.microsoft.com/en-us/research/wp-content/uploads/2021/04/eurosys21-final101.pdf
  - Panzura: https://panzura.com/technology/distributed-file-locking
- Order across shards:
  - FuzzyLog (OSDI'18): https://www.usenix.org/conference/osdi18/presentation/lockerman
  - Scalog (NSDI'20): https://www.usenix.org/system/files/nsdi20-paper-ding.pdf
- Hot directories:
  - CFS (EuroSys'23): https://dl.acm.org/doi/10.1145/3552326.3587443
  - SingularFS (ATC'23): https://www.usenix.org/conference/atc23/presentation/guo
  - Mantle (SOSP'25): https://dl.acm.org/doi/10.1145/3731569.3764824
  - GIGA+ (FAST'11): https://www.usenix.org/conference/fast11/scale-and-concurrency-giga-file-system-directories-millions-files
  - HLC: https://cse.buffalo.edu/tech-reports/2014-04.pdf
- Quorums:
  - Flexible Paxos: https://arxiv.org/abs/1608.06696
  - Cloud witness: https://learn.microsoft.com/en-us/windows-server/failover-clustering/deploy-quorum-witness
- Verification:
  - Stateright: https://www.stateright.rs/achieving-linearizability.html
  - TigerBeetle protocol-aware DST: https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/
  - Elle: https://github.com/jepsen-io/elle
  - P at AWS: https://queue.acm.org/doi/10.1145/3712057
- Set reconciliation:
  - RBSR: https://arxiv.org/pdf/2212.13567
  - Negentropy: https://github.com/hoytech/negentropy
