# Plan 41 — Lock renewals through local disk stalls: a renewal lane beside the authority core

Read `docs/plans/v1/CONVENTIONS.md` first. This plan keeps cluster-lock
grant renewals running while a node's metadata store, or the disk under
it, stalls, so that a local disk stall no longer lapses the lock grants
the node holds or owns. Today every renewal is a step of the node's
authority core, the core's step writes the metadata store synchronously,
and a store write that waits seconds on the device stops every renewal
of the node with it: a grant renewed half-way through its window lapses
after `(ttl − margin) / 2` = 9.5 s of such a stall at the defaults, and
the lock owner's process is fenced with `EIO` on every write and
namespace operation (the xcheckfs run of 2026-10-13, an 18 s and a 24 s
store stall on one node).

The fix is a **renewal lane**: a small component beside the core that
extends grant windows from in-memory state only — the lock tables, which
already live in memory and are shared with the FUSE threads — under a
**renewal certificate** the core publishes after every step. The lane
never grants, reclaims, refuses, releases or drops a grant; it only
extends a window the core could have extended at that moment, within
time-bounded facts the core vouched for. Everything else stays in the
core. The certificate is the lease mirror the driver already publishes
after every step (`LeaseView`), extended by a few fields (§5.2).

A drift found while planning (§3.4) is **confirmed** by a model
counterexample: a root under a fast-takeover tenure renews grants — its
own and its peers', and its delegations — without the fresh S3 liveness
its new grants and the model require. That fix (L0) **lands separately
as an integrity fix** (as plan 40's R2 did); this plan keeps its
description as the spec and only verifies it is present.

Staging: L1–L4 (own grants and grants held at a healthy owner) deliver
finding 08 and the "stalled holder" case with no new S3 traffic and no
change to any restart quarantine; they are the plan's committed scope.
L5 (answering peers' renewals at a stalled owner, the horizon lead) and
L6 (the lane's own lease probe on fast tenures, routing to a successor)
are a maintainer decision, recommended as a follow-up plan (§11), so the
common case lands first.

Rules carried into this plan from the maintainer: consistency and
integrity first, then performance, never one for the other (a trade-off
is reported, not shipped); trace the history and the existing integrity
arguments before changing the authority or lock code, and extend them;
no backward-compatibility shims and **no version bumps** (wire messages,
handoff and on-disk formats change in place); every fix or behaviour
change comes with a test that fails before it (or, for a new mechanism,
against the naive variant it guards against); a change made for one host
file system must be neutral or better on ext4, xfs, btrfs and tmpfs,
measured; complex fixes get an independent review before they land.

## Dependencies

- Plan 30 (§M9 backups, seals, `ack=s3` and the strict-answer liveness
  rule; §M10 continuation epochs; §M11 delegations; §M14 cluster locks,
  phase 2 fencing tokens). All committed.
- Plan 31 (the engine crate, the authority driver, the control
  protocol), plan 39 (per-descriptor error reporting for discarded
  writes, which a lapse triggers). Committed in practice.
- The finding-14 work: `e5a35fd`, `343ed48`, `513ce96`, `e412ecc`
  (§1). Committed.
- L0, the renewal-freshness integrity fix (§3.4, §7 L0), landed
  separately: must be committed before L2.
- Coordinates with nothing in flight. Plan 40 touches the FUSE adapter
  and the view; this plan touches neither's lock paths except through
  `LockTables`.

## 1. History

Verified against `main` at `579e196` (`fbcb3ce` plus a plans-only
commit).

| When | Commit | What |
|---|---|---|
| 2026-09-24 | `0e1017b` (plan 30 M9) | Backups, seal-based failover, `ack=s3`. `strict_answer_allowed` (`crates/authority/src/core/backup.rs:579`): a tenure that may be taken over before its lease expires answers strict reads only within `backup_takeover_ms` of a fresh S3 observation (`note_s3_liveness`, `:552`). The successor's floor waits `marker + backup_takeover_ms + ttl + 2M` (module doc `:66-80`). |
| 2026-09-25 | `df14686` (plan 30 M14) | Cluster locks: leased grants, `EIO` fencing, `on_lock_renew_tick`. The model `crates/model/src/locks.rs` with the probe-freshness rule for a fast root (`owner_until`, `:526-545`). |
| 2026-09-26 | `a1bed13` | Grants renewed half-way through the window the node honours them for (`renew_point`, `crates/meta/src/locks.rs:110`). |
| 2026-10-03 | `c612585` | First answer to "a slow step silences the node": `priority_writes` (the core's writes go ahead of the FUSE workers', `crates/meta/src/store/gate.rs:94`), the holder's off-core liveness heartbeat `holder_alive_task` (`crates/engine/src/authority_driver.rs:992`) that stops after `CONSTELLATION_HOLDER_STALL_MS` (15 s) without driver progress. |
| 2026-10-03 | `247fc59` | A node's grants to itself leave no restart horizon (they die with the process). |
| 2026-10-04 | `f57db3c` (M14 phase 2) | Fencing tokens; the restart horizon written **off the core** (`Action::PersistLockHorizon`, `authority_driver.rs:2969`), with answers to peers held until it is durable (`lock_answer`, `crates/authority/src/core/locks.rs:910`): "a 7.6 s sync blocked the core 12 s, and another node's grant lapsed meanwhile" (`git-under-flock-b2b` seed 7). |
| 2026-10-06 | `4c5d184` | Late renewal answers still count (`LateRenew`, `LATE_RENEW_MS`, `core/locks.rs:141-159`). |
| 2026-10-07 | `a2b932b`, `d7d2eb8` | Renewal answers on an **urgent lane** ahead of the driver's internal channel; the renewal tick and a delegate's renewal and lapse timers on a **prompt lane** (`prompt_timer`, `authority_driver.rs:1262`). Both still wait for the core's step to end. |
| 2026-10-13 | `4ffbc08` | The xcheckfs run of 2026-10-13 (finding 08): an fjall journal write held the store's writer lock 18 s, the core waited on it, its own renewals (`Route::Me`) did not run, the grants lapsed and xcheckfs (one process holding every stressor's locks) was owner-fenced. Documented as by design: `docs/reference/features/cluster-locks.md:317-339`, ADR-25 (`docs/explanation/DECISIONS.md:808-811`). New scenario `lock-lapse-lone-node` (`crates/harness/src/scenarios/m14.rs:1140`, SIGSTOP). |
| 2026-10-14 | `e5a35fd` | The own-S3 stall detector needs a request left waiting through a window of ticks (a daemon whose core waited 18 s read as an S3 outage). |
| 2026-10-14 | `343ed48`, `e412ecc` | Concurrent `Meta::sync` calls share journal syncs (vendored fjall change 8, `persist_with`, `vendor/fjall/CONSTELLATION-PATCH.md:446`): under 8 fsyncing threads at a 1 s disk latency the worst core step went from 5–7 s to 2–3 s. |
| 2026-10-14 | `513ce96` | Store writes from async tasks go through `store_io::store_call` (`block_in_place`, at most 8). "The authority core's step [writes] synchronously on threads of [its] own" by design (`crates/engine/src/store_io.rs:34-37`). |

Reported by the maintainer's investigation and **not recorded in the
repository** (this plan could not verify them; L8 re-measures them):
after the finding-14 commits, a 15 s device freeze still lapses grants
depending on timing; the synchronous log writer
(`crates/engine/src/log_buffer.rs:77-95`, a `write_all` to stderr, which
a daemonized node redirects to `daemon.log` in its state dir,
`crates/cli/src/daemonize.rs:83-87`) blocked core steps 9–14 s on a
loaded btrfs.

**Superseded interim guidance.** On 2026-10-14 the maintainer declined
a non-blocking log writer for now and accepted host mitigations instead:
mount the state dir's file system with `lazytime` (or `noatime`), and put
the daemon's log on a fast disk (or a journald/pipe target that does not
share the stalling device). On 2026-10-18 the decision was reversed on
new evidence (core steps blocked 0.7-12.6 s in `LogWriter::write`, one of
them losing a delegation by itself, and the F2 heartbeat thread blocked
1.6 s and 7.3 s in stderr writes): **logging never blocks a daemon
thread; lines that cannot be queued are dropped and counted** — plan 44
(`done/44-non-blocking-log-writer.md`). The host mitigations stay useful as
general guidance (fewer stalls of the store's journal, fewer dropped log
lines), no longer as the answer to a blocked log writer; L7 writes them
as such.

## 2. What the code does today

### 2.1 The core and its store

- The core is sans-IO (`crates/authority/src/core/mod.rs`,
  `Core::handle` `:1554`), but its `Replica` port is a synchronous call
  into `Meta` from inside the step (`crates/authority/src/replica.rs:1-13`:
  "a fjall read or write transaction, sub-millisecond ... none of them
  can wait on anything the core, a peer or S3 holds"). It can wait on
  the disk: every write takes fjall's single writer lock
  (`GatedDb::write_tx`, `crates/meta/src/store/gate.rs:141`) and appends
  to the journal; FUSE workers on the fast path hold the same lock.
- The driver (`crates/engine/src/authority_driver.rs`) runs the core as
  one tokio task: `Driver::run` (`:1444`) serves the urgent lane, then
  the prompt timers, then the internal channel, then `sync_rx`; `step`
  (`:1622`) runs `Core::handle` inside `priority_writes`, then
  `refresh()` (the FUSE-facing mirrors and status), then `dispatch`.
  Nothing else of the node's authority runs while a step waits. A step
  over 500 ms logs `slow core step` (`SLOW_STEP_US`, `:58`) — through
  the same synchronous log writer (non-blocking after plan 44).

### 2.2 The renewal paths

All in `crates/authority/src/core/locks.rs`; the tables in
`crates/meta/src/locks.rs` (`LockTables`, in memory, one mutex, shared
with the FUSE threads, `Meta::locks()` `:2276`).

- **Node side, the tick** (`on_lock_renew_tick`, `:3272`): drops lapsed
  held grants, releases idle and recalled-idle caches, then
  `LockTables::due_renewals` (`crates/meta/src/locks.rs:1463`: honoured,
  past `renew_at`, no renewal in flight, renewable; marks them
  `renewing`) and routes each with `lock_route_for(.., renewal = true)`
  (`:543`):
  - `Route::Me` (this node owns the inode: the root of a lone node, the
    lease holder's own locks): `lock_renew_one(now, me, ..)` and
    `lock_apply_renew_result` in the same step. **Nothing is persisted**
    (`from != node_id` guards `lock_need_horizon`, `:2418`, `:2434`).
  - `Route::Node(n)`: one `LockRenew` per owner (`:3363-3400`), answered
    on the urgent lane (`lock_renew_rpc`, `authority_driver.rs:4127`),
    applied by `on_lock_renewed` (`:3493`) with the request's `sent`;
    a timeout (`on_lock_renew_timeout`, `:3579`) lets the next tick
    re-send, and a late `Ok` still counts (`lock_late_renewed`).
  - otherwise: relearn the owner from the lease (`lock_relearn_owner`).
  The tick is armed for the earliest renewal point
  (`lock_arm_renew_tick`, `:3255`).
- **Owner side** (`on_lock_renew`, `:2345`; `lock_renew_one`, `:2365`):
  route; `ttl = min(lock_ttl, cap)`; extend the known grant to
  `now + ttl + margin` (`LockTables::extend`, `crates/meta/src/locks.rs:970`);
  an unknown grant is reclaimed during a grace, else `Lost`. A peer's
  renewal moves the restart horizon (`lock_need_horizon(until, until +
  ttl/4)`, `:2418-2420`); the answer is held until the horizon write lands
  (`lock_answer`, `:910`; `on_lock_horizon_persisted`, `:938`;
  `Meta::note_lock_grant_horizon` adds 1 s, `crates/meta/src/readdeleg.rs:545-559`).
  Side effects: `served`, `unproven`, `lock_peer_heard`, stats.
- **P2P entry**: a peer's `LockRenew` reaches the driver through
  `P2pBridge::lock_renew_requested` (`crates/engine/src/p2p.rs:931`) on
  the urgent lane, and is answered when the core gets to it.
- **Timing** (`docs/reference/features/cluster-locks.md:190-235`):
  `CONSTELLATION_LOCK_TTL_MS` 20 s, margin `min(1 s, lease TTL / 4)`;
  the node honours a grant until `sent + ttl − margin`, the owner keeps
  its record until `granted + ttl + margin`; renewed at
  `sent + (ttl − margin)/2`. Root lease TTL 60 s
  (`DEFAULT_LEASE_TTL_MS`, `crates/store-s3/src/lease.rs:41`), renewed
  when half is gone; `backup_takeover_ms` 1.5 s
  (`crates/authority/src/core/mod.rs:472`).

### 2.3 The safety rules a renewal relies on

- **Cap.** A root's grant never outlives its lease's usable end
  (`lock_cap_ms`, `:659`: `expires − margin − now`; under a held
  continuation epoch the TTL stands). A TTL takeover therefore finds
  every old grant lapsed (model: `capped_grants_are_clean_across_a_ttl_takeover`).
- **Fast takeovers.** A backup's seal or an `ack=s3` takeover deposes a
  holder before its lease expires. The successor quarantines new grants
  until `min(prev_expires, marker + backup_takeover_ms + max(read_ttl,
  lock_ttl) + deleg + 2M)` (`note_marker_landed`, `backup.rs:681-716`);
  the model's grace (`crates/model/src/locks.rs:1093-1106`) is
  `now + takeover_window + lock_ttl + 2·seq_margin`. That only covers a
  deposed root's answers if it gave them within `takeover_window` of its
  last fresh S3 observation — the model's `probe_freshness`
  (`owner_until`, `:526-545`), the code's `strict_answer_allowed` for
  **new** grants (`lock_serve`, `:1056-1064`; `lock_serve_waiters`,
  `:2140`).
- **Released takeover.** A successor of a released lease quarantines
  `now + ttl + margin` (`lock_on_released_takeover`, `:4614`).
- **Restart horizon.** A peer's grant or renewal is answered only once
  `granted + ttl + margin` is covered by the durable horizon; a
  restarted owner waits it out (`load_lock_quarantine`,
  `readdeleg.rs:578`).
- **Fencing tokens** carry the holder's window end; a non-minting
  executor refuses past it.

### 2.4 What already runs off the core

The restart-horizon write (blocking pool), the holder's backup heartbeat
(`holder_alive_task`, stops after 15 s of no driver progress), the
backup's arrival stamps (`OffCore`, `:1167`), async-task store writes
(`store_call`). Renewals do not: they are queued ahead of other work,
but a step that waits on the disk holds them all.

## 3. The problem, with evidence

### 3.1 Finding 08

One node, write-back, `--locks cluster`, an overloaded host: a FUSE
thread held the store's writer lock during an fjall journal `write(2)`
that blocked on device I/O (btrfs stable pages, an fsync holding the
journal inode's lock; ext4's jbd2 on `file_update_time`); the core's
step waited for the lock 18 s, later on its own commit's `write(2)`
(24 s). Its `LockRenewTick` was queued behind the step; `Route::Me`
renewals did not run; the grants lapsed at `sent + ttl − margin`; the
owner fence (`LockTables::fenced_owners`) answered `EIO` to every
mutation of xcheckfs's process until it dropped its locks
(`4ffbc08`'s message). Nothing was applied and reported failed.

### 3.2 Why it is by design today

`cluster-locks.md:317-339`: "a node cannot honour its own grants past
their window from inside a stall: a backup's seal or an `ack=s3`
takeover deposes a holder well inside its lease TTL, and the successor's
grace assumes every grant lapsed at its holder within a lock window."
That is right about honouring a window the core never extended. It does
not forbid extending the window: the renewal of a node's own grant on a
lone node writes nothing, and every fact it depends on (the lease is
held and usable; nobody can take it over before it expires; the grant
record is live) is either in memory or a bound in time.

### 3.3 Who is affected

| Stalled node's role | Grants that lapse today | L1–L4 (committed scope) | L5/L6 (decision, §11) |
|---|---|---|---|
| Lone node (root, `Local`, no backups) | its own (finding 08) | renewed by the lane up to the lease's usable end (≥ 28 s at defaults) | — |
| Lease holder with peers, `Local`, no delegations | its own; peers' grants it owns (their renewals wait for the core) | own: as above; peers': unchanged | peers': answered by the lane within the durable restart horizon (§5.3) |
| Lease holder with backups (`ack=backup`) | as above, plus deposition after 15 s + 1.5 s (§5.5) | own: renewed only while the core's last S3 observation is fresh (≤ 1.5 s into a stall): little gain | renewed with the lane's own lease probe until deposition; own grants then reclaimed at the successor |
| Lease holder under `ack=s3` | its own and peers' — and the lease itself after ~1.5 s of core silence (§3.5) | almost nothing: the holder is deposed after 1.5 s whatever the lane does | almost nothing either; the probe's S3 `GET`s apply to fast tenures only |
| A node holding grants from a healthy owner | its held grants | renewed by the lane (any configuration, delegations included) | — |
| A delegate (owner of a subtree) | its own and its subtree's grants | unchanged (deferred, §5.10) | unchanged |
| Frozen process (SIGSTOP, swapped out) | everything | unchanged (the lane is frozen too) | unchanged |

### 3.4 A drift found while planning: fast-tenure renewals without fresh liveness (confirmed; L0, landed separately)

`lock_renew_one` (`:2365`) routes with `lock_route_answering` →
`lock_route_for(.., renewal = true)`, which checks `lease.usable()` only
(`crates/authority/src/core/lease.rs:163`: unexpired past the margin,
held by this node) — not `strict_answer_allowed`. A root whose backups
sealed it (P2P cut from them, S3 reachable, a lock holder still reaching
it) keeps extending its peers' grants by up to `lock_ttl` until it
notices the deposition (its next lease renewal CAS, a backup
reconfiguration, a ship), which can be past the successor's quarantine
(`marker + 1.5 + 20 + 2 s` at the defaults) while its lease (60 s) is
still unexpired in its own view. The model forbids it:
`Msg::Renew` is answered through `owner_until` (`model/src/locks.rs:1491`),
which refuses a fast root outside `takeover_window` of its last probe, so
every clean fast-takeover run (`fast_takeover_with_grace_is_clean`, the
design's `max_renews: 1`) assumed the rule the code lacks. The tests that
show the probe is load-bearing (`fast_takeover_without_the_probe_violates`,
`crates/model/tests/locks.rs:246-280`) disable renewals (`max_renews: 0`)
and the probe for grants and renewals together; no test isolates the
renewal half, and the module doc's action → code table maps `Renew` to
`LockRenew` → `LockRenewed` without the check. ADR-12 ("safety never depends on
failure detection") says the eventual discovery is not an argument.

**Confirmed** by the plan's review with a model counterexample (the
renewal-freshness knob of §6 emulating today's code): scripts
`[[Lock(X), Io], [], [Lock(X)]]`, `backup: 2, replicate: false, grace:
true, lease_ttl: 30, lock_ttl: 3, max_tick: 8, max_renews: 6` — node 2
takes the lease over at t = 0 with a grace to t = 6; the deposed root 0
renews **its own** grant at t = 1..5 (`Renew(0)` × 5, answered by
itself); node 2 grants itself at t = 6; root 0 performs `Io` under grant
1: `mutual_exclusion` violated (grants 0, 1, 2). The case is the
deposed root's own grant, the `Route::Me` path of finding 08 itself.

The same gap is in the root's **delegation** renewals: `on_deleg_renew`
(`core/delegate.rs:2152`) grants when `root_usable(now)` (`:431`:
`ship_epoch(..).is_some() && !lease.fenced()`), with no liveness check.

**L0 is the integrity fix, landed separately** (§7 L0 is its spec): one
freshness predicate for new grants, lock renewals (own and peers') and
delegation renewals on a fast tenure. The lane needs the same rule and
uses the same predicate.

### 3.5 Adjacent findings

- `LockTables::take_where` runs its predicate under the tables' mutex.
  It has three callers: `lock_on_delegated` (`:3944`) and
  `lock_take_generation` (`:4481`), whose predicates read the store
  (`resolve_ownership` → `delegation_table()` and `is_under`), and
  `deleg_drop_left` (`:4105`), whose predicate is store-free
  (`inos.contains`). On a frozen device the first two hold the tables'
  mutex — every FUSE fence check and every lane call — for as long as an
  uncached read takes. **Fixed in L1**; load-bearing for finding 08:
  the lane is only as live as that mutex.
- `LockTables::check_tag` logs (`target: constellation::token_exec`,
  debug, off by default) while holding the mutex
  (`crates/meta/src/locks.rs:1066-1077`): enabled, a blocked log writer
  holds the mutex. **Fixed in L1** (log after the guard drops);
  load-bearing for the same reason. After plan 44 a log call no longer
  waits on the disk, but formatting under the mutex still costs every
  FUSE fence check: L1's fix stays.
- Under `ack=s3` the holder's stream heartbeat is a core timer
  (`core/stream.rs:369`), and a subscriber takes the lease over after
  `backup_takeover_ms` of silence (`watch_s3_holder`, `backup.rs:2522`):
  a 1.5 s disk stall deposes an `ack=s3` holder. Under `ack=s3` the lane
  therefore buys almost nothing for a stalled holder. Out of scope; a
  maintainer decision (§11).
- The root lease is renewed only by the core (an S3 CAS): a core stalled
  past the lease's usable end loses it. At 60 s renewed at half, that is
  ≥ 29 s of stall. Out of scope (§5.10, §11).

## 4. Design options

Each option is judged against the lock model (`crates/model/src/locks.rs`:
mutual exclusion over grants in issue order), the lease/epoch rules
(§2.3), the authority simulation's mutual-exclusion ghost
(`crates/authority/tests/sim/locks.rs`), and four failure modes: the
node is dead, partitioned (P2P or S3), its process frozen, or only its
disk slow.

### A. Asynchronous store writes in the core

A store-writer thread owns every `Meta` write; the core issues writes as
actions and continues on completion events; dependent state is gated on
those events.

- Integrity: the core's correctness rests on synchronous decide-then-write
  atomicity (`replica.rs:1-13`; `dispatch_forward`'s view check and
  journal write; the FUSE fast path's delegation gate,
  `Meta::root_fast_path`; the release's journal re-read). Each of the
  127 `Replica` methods becomes a request/continuation pair with new
  interleavings for the sim and the model to re-verify. Reads stall too
  on a frozen device (uncached pages), so the core still blocks unless
  reads go async as well.
- Failure modes: fixes "slow disk" for everything the core does, not
  only renewals.
- Complexity: a rewrite of most of `crates/authority/src/core` (≈ 30k
  lines, plus 20k of tests), the sim driver and the M5 model assumptions.
- Performance: a channel hop per store call on the hottest path.
- Verdict: **rejected for this plan.** Renewals depend on no write at
  all; A buys nothing for them that B does not, at a hundred times the
  cost. A long-term direction, recorded.

### B. A renewal lane beside the core (chosen)

A component off the core's step extends grant windows from the
in-memory lock tables under a certificate the core publishes after every
step (the existing lease mirror, `LeaseView`, extended). Details §5.

- Integrity: the lane extends only (§5.1); every input is either checked
  live under the tables' mutex or a time-bounded fact the core vouched
  for and the lane bounds (§5.9). Needs model work (§6) and an ADR-25
  amendment.
- Failure modes: slow disk — renewals continue; dead or frozen — nothing
  renews (unchanged); partitioned — the cap and freshness rules stop the
  owner side exactly where the core would stop; S3 cut — a fast tenure's
  lane stops when its freshness runs out, a `Local` one continues to the
  cap (as the core would).
- Complexity: one sans-IO module in `crates/authority`, a task in the
  engine driver and the sim driver, a handful of atomic `LockTables`
  primitives, a few `LeaseView` fields and the core's report handling.
- Performance: the same renewal messages, sent from the lane instead of
  the core; no S3 traffic in the committed scope; (L6) on a fast tenure at most
  one lease `GET` per renewal round when nothing else proved liveness.

### C. Bound the core's blocking (timeouts and a degraded mode)

- Integrity: a journal `write(2)` cannot be cancelled; a timed-out
  transaction holds the writer lock with an unknown outcome; a core that
  continues past it diverges from its store. Unsafe; a "renewal-only
  mode" inside the same thread is impossible while the thread is in the
  syscall. **Rejected.**

### D. Others

- **D1. Longer default lock TTL** (e.g. 60 s: 29.5 s guaranteed). A
  crashed holder costs its waiters 61 s instead of 21 s; restart
  quarantines grow. Rejected as a default; documented as a mitigation
  (`CONSTELLATION_LOCK_TTL_MS` exists).
- **D2. Own grants capped only by the lease** on a tenure nobody can take
  over early (a lone `Local` node): survives ~28 s with the core's
  renewals. Breaks the fencing token's argument for non-minting
  executors (a long `until` is accepted by a delegate after the root
  ended the grant), must shrink windows at a release, a backup addition,
  an epoch; covers neither non-root holders nor owners. Rejected.
- **D3. Renew earlier** (a third of the window): 12.7 s guaranteed, 1.5×
  the messages; does not cover 18 s. Rejected.
- **D4. The restart horizon in its own file** (not the store's journal
  and writer lock): helps the owner side under writer-lock contention
  (finding 08's shape), not under a device freeze. Relevant only with
  L5 (§5.3: the opt-in horizon lead is the alternative); recorded.
- **D5. A separate lock core** with its own event loop: the lock code
  reads the replica for routing, positions, floors and flushes; B is the
  part of it that needs nothing from the store. Rejected for now.
- **D6. Host mitigations** (`lazytime`/`noatime` on the state dir, the
  log on a fast disk, the journal on its own device). Reduce stalls,
  never remove them; written as documentation in L7.
- **D7. Honour a window past its end while the node "knows" it is
  stalled.** Exactly what `cluster-locks.md:317-339` rightly rejects:
  from inside a stall the node cannot know nobody acted. B never
  honours past a window; it extends the window by the owner's rule.

| | Integrity argument | Covers | Cost |
|---|---|---|---|
| A | new, large | everything the core does | very high |
| **B** | the existing one, reused per input (§5.9) + model | renewals: own (`Local` root, no delegations), held (any); owned by peers with L5 | moderate |
| C | none | — | — |
| D1 | unchanged | stalls < 29.5 s | waiters 3× longer behind a crash |
| D2 | breaks tokens | lone node only | low |
| D3 | unchanged | stalls < 12.7 s | 1.5× messages |

## 5. Decision and design

### 5.1 Scope: what the lane does and never does

The lane (`crates/authority/src/lane.rs`, sans-IO; engine and sim
drivers run it) does exactly three things:

1. **Own renewals** (node side, `Route::Me`): extends this node's own
   grant, its owner record and its held window together, in one
   `LockTables` critical section.
2. **Remote renewals** (node side): sends `LockRenew` for due held grants
   to their owner and applies the `Ok` answers (`LockTables::renewed`).
3. **Peer renewals** (owner side; **L5, a maintainer decision**, §11):
   answers a peer's `LockRenew` with `Ok` for grants whose owner record
   is live here, when it can answer every entry of the message;
   otherwise the message goes to the core unchanged. Without L5 every
   peer's `LockRenew` goes to the core as today.

It never: grants, upgrades, reclaims (an unknown id), answers `Lost` or
`NotOwner`, releases, flushes, drops a held grant, installs a mirror,
changes routing state, writes the store, reads the store, or logs. A
case it cannot decide goes to the core, which decides it as today when
its step gets there.

The lane runs **always**, not only during stalls: a healthy node renews
through it too, so the path is exercised by every lock workload and
every existing sim seed, not only by stall tests. The core's renewal
tick stays; `LockTables`' `renewing` mark keeps the two from sending the
same renewal twice, and a duplicate is harmless anyway (extensions are
monotone). `CONSTELLATION_LOCK_LANE=off` disables it (A/B runs,
fail-before scenarios).

### 5.2 The renewal certificate is the lease mirror

The driver already publishes what the lane needs after every step:
`Driver::refresh()` (`crates/engine/src/authority_driver.rs:1738`,
called in `step` after `Core::handle` and **before** `dispatch`) calls
`LeaseView::mirror(lease, now, cfg, ack_gated)` (`:1755`) and
`LeaseView::set_liveness(fast_tenure, last_s3_fresh)` (`:1758`).
`LeaseView` (`crates/engine/src/lease.rs:66`) is a lock-free snapshot
the FUSE threads read, and `LeaseView::reads_locally()` (`:174`: not
releasing, no gate pending, usable, and fresh on a fast tenure) is
already the lane's root check minus the cap value and the epoch,
delegation and resuming exclusions. So the certificate is **`LeaseView`,
extended** — no new shared structure, one publish point (`refresh()`),
the same ordering argument the FUSE fast path already relies on (the
mirror is refreshed before the step's actions go out).

New `LeaseView` fields, set in `mirror`/a new `mirror_locks` call from
`refresh()`, computed by a new `Core::lock_lane_view(now, replica) ->
LockLaneView` (in `core/locks.rs`, so the sim publishes the same facts):

- `lock_cap_until_ms: AtomicI64` — `lease.expires_unix_ms − margin`
  (`lock_cap_ms`'s bound as an instant); 0 when there is no root
  authority for the lane.
- `lock_lane_ok: AtomicBool` — false unless **all** hold: `cfg.locks`;
  `lease.usable(now)`; `!lease.lost`; `!lease.fenced()`; no continuation
  epoch open, held or being re-claimed (`lease.epoch_held()`,
  `epoch.active`, `epoch_reclaim_pending`); this node delegates nothing
  and is no delegate (`dl.gens` empty and `replica.delegations_any()`
  false); `!lock_owner_resuming(now)`. Each excluded state is one whose
  core rule differs from "extend a live record up to the lease cap" (an
  epoch's TTL-only cap, a gate's floor, a delegation's route); the lane
  does not reproduce them.
- `lock_owner: AtomicU64` — where gen-0 renewals of held grants go when
  this node is not the owner (`lease.cached_holder`), 0 if unknown.
- freshness: the existing `fast_tenure`, `last_s3_fresh_ms` and
  `fresh_window_ms` (`backup_takeover_ms`) fields, unchanged. A
  `Local` tenure with no listed backup is unconditional
  (`fast_tenure == false`, as `strict_answer_allowed` and
  `reads_locally` treat it).
- (L5 only) the held-answer set and the durable horizon, §5.3.

The lane's check, one function on `LeaseView`:
`lane_root(now) -> Option<cap_until>` = `lock_lane_ok && reads_locally()
&& now < lock_cap_until_ms && !meta.delegations_any()`. `releasing` and
`gate_pending` are already stored `SeqCst` last/first by `mirror`; the
new fields are written before `releasing` (which `mirror` stores last),
so a reader that sees `releasing == false` sees fields at least as new
as that store. `set_liveness` moves into the same publish, before that
last store (today it runs after `mirror`); freshness only grows within a
tenure, and a backup added to the tenure cannot seal before the
reconfiguration CAS the step dispatches after `refresh()`.

A step that changes the root's authority — issues the release CAS,
learns a deposition, opens an epoch, writes a `Delegate` record — makes
the mirror it publishes before its actions are dispatched say so. A
`Delegate` record flips `deleg_any` inside the step's own store write;
the lane re-checks that atomic on every decision (§5.6).

### 5.3 Owner side: own grants (L3) and peers' renewals (L5, decision)

For a grant with owner record `g` (this node's table), with
`lane_root(now)` from §5.2 giving `cap_until`:

- **Own renewal** (L3): `LockTables::renew_own(ino, id, me, lock_ttl,
  cap_until, margin)` (new), under one guard, with `now` read
  **inside** the guard and `ttl = min(lock_ttl, cap_until − now)`
  computed there (`ttl <= 0`: nothing, defer): the owner record `g`
  must exist with `g.node == me`, `g.gen == 0` and
  `g.until_ms > now` (live here), the held grant must have id `g.id`,
  be honoured (`until_ms > now`) and `renewable`; then
  `g.until_ms = max(g.until_ms, now + ttl + margin)`,
  `g.confirmed_ms = min(g.confirmed_ms, now)`, and the held window,
  `renew_at` and `recalled` exactly as `renewed(.., sent = now, ttl,
  margin, g.recalled)` sets them; `renewing = None`. Anything else:
  `false`, and the grant stays for the core. This is
  `lock_renew_one(me)` + `lock_apply_renew_result` (`:2365`, `:3525`)
  with the authority facts taken from the mirror. Own grants need no
  restart horizon (`247fc59`), so this costs no write and no S3 request.
- **Peer renewal** (L5; a maintainer decision, recommended as a
  follow-up plan, §11) (`LockRenew { req, entries }` from `from != me`):
  for every entry, `lane_root(now)`; **no core answer to `from` is held
  or being built** (below); the record `entries[i].grant` exists with
  `node == from`, `gen == 0`, `until_ms > now`;
  `ttl = min(lock_ttl, cap − now) > 0`; `until = now + ttl + margin`;
  and `until ≤ horizon_durable`, read from a shared `AtomicI64` the
  horizon-write task raises after `note_lock_grant_horizon` returns
  (not the core's copy, which a stalled core does not update). All
  entries pass, or the whole message goes to the core. Extensions use
  `LockTables::extend_live(id, node, until)` (new: `now` inside the
  guard, record must be live); one that fails at apply time sends the
  whole message to the core as well (the extensions already made are
  harmless: a longer owner record only keeps others out longer). The
  answer is `Ok { ttl_ms, recalled, id, mode }` per entry, as
  `lock_renew_one` makes it.
  - **Answer order to one peer.** `lock_answer` (`:910`) keeps answers
    to one peer in order: one made after a held answer waits behind it.
    A held-peer set published by `refresh()` reflects only holds made
    in an earlier step; the lane, answering synchronously inside
    `P2pBridge::lock_renew_requested`, could overtake an answer the
    core holds in the step that is running. So the set is a shared
    structure (`LockTables`-adjacent, e.g. `Meta::lock_answer_holds`: a
    small mutex-protected per-peer count, or one `AtomicU64` bitmap per
    peer slot) that the core updates **inside `lock_answer`** at the
    moment it holds an answer (before the step returns) and inside
    `on_lock_horizon_persisted` when it sends one; the lane defers any
    peer with a nonzero count. While the core is building an answer for
    a peer whose renewal the lane also received, the lane cannot know;
    that is two different requests, and answers to different requests
    of one peer are not ordered against each other today either (the
    renewer matches by request id, late answers count). `lock_answer`'s
    doc gains one sentence saying so, and stays true.
- **The horizon lead** (L5). Today a peer's renewal asks the horizon
  for `until + ttl/4` (`:2419`), plus 1 s in `note_horizon`: the
  durable horizon runs about 6 s ahead of the latest window, so a lane
  at a stalled owner can answer peers for ~6 s, and a peer's grant
  survives an owner stall of about `6 + (ttl − margin)/2` ≈ 15.5 s at
  the defaults. **Default unchanged** (`ttl/4`). Opt-in knob
  `CONSTELLATION_LOCK_HORIZON_LEAD_MS`: `lock_need_horizon(until, until
  + lead)` for grants and renewals alike; at `lead = ttl` a peer's grant
  survives ≥ 28 s (capped by the lease), and every crash-restart
  quarantine with peers' grants live grows to up to `crash + lead + ttl
  + margin + 1 s` (≈ 42 s) instead of ≈ 27 s — a cost paid on every
  crash for a corner case, hence opt-in (§11). A clean shutdown is
  unchanged (it releases).

### 5.4 Node side: renewals of held grants

- `LockTables::due_renewals_lane(now, stale_ms)` (new): as
  `due_renewals`, but a `renewing` mark older than `stale_ms`
  (`forward_timeout_ms`) counts as no renewal in flight. A renewal the
  core sent right before its step stalled has its answer queued behind
  that step; the lane re-sends it.
- **Route.** Own grants (`HeldGrant.owner == me` and `lane_root`):
  §5.3. Otherwise the owner is, in order: the node the held grant's last
  `Ok` came from (`HeldGrant.route`, new, set by `renewed` and by the
  core's `lock_apply_renew_result`; cleared by a `NotOwner` the core
  applies), `LeaseView`'s `lock_owner`, and (L6 only) the lane's
  `owner_hint` (§5.5). None: `renewal_failed`
  (clears the mark; the core relearns when it runs). Node-side routing
  is liveness only: a wrong owner answers `NotOwner` (`lock_route_answering`
  checks ownership before anything else), and the lane hands every
  non-`Ok` result to the core. So this half works under delegations and
  epochs too.
- **Sending.** One `LockRenew` per owner, request ids from the lane's
  own space (`OpId` with the top bit set, so a core never mistakes a
  lane answer for its own `renews` entry); `sent` = the `now` read
  under `due_renewals_lane`'s guard (never later than the send, so the
  holder never honours longer than the owner records).
- **Answers.** `Ok`: `LockTables::renewed(ino, id, now_id, mode, sent,
  ttl, margin, recalled)` at once (the liveness-critical part, and
  monotone). Every result, `Ok` included, also goes to the core as
  `Event::LaneRenewed { from, sent, results }` (§5.7), which applies it
  with `lock_apply_renew_result` (idempotent for `Ok`; `Lost`,
  `NotOwner`, the release of a recalled grant as today).
- **Timeouts and late answers**: as the core's (`on_lock_renew_timeout`,
  `LateRenew`): at `forward_timeout_ms` the entries get
  `renewal_failed` and the next lane tick may re-send; the request's
  future lives up to `LATE_RENEW_MS` (60 s), and a late `Ok` is applied
  with that request's own `sent`; a late non-`Ok` is dropped.
- **Bound.** The lane stops remote renewals once the driver has made no
  progress for `CONSTELLATION_LOCK_LANE_MAX_STALL_MS` (default 120 s,
  maintainer decision §11; read from `OffCore::busy_since`): a node
  whose core is hung, not slow, then lets its grants lapse as today
  instead of holding a remote lock for ever. Own and owner-side
  renewals need no such bound: the lease cap ends them (the core renews
  no lease while hung).
- What the lane does **not** do on the node side: release a recalled
  grant (it needs the flush, a store write), drop a lapsed one, release
  an idle cache. A recalled grant is renewed while something pins it
  (`renewable`), as the core does; one with nothing pinning it lapses
  if the core does not get to its release first — as today.

### 5.5 Fast tenures (L6, a maintainer decision; recommended as a follow-up)

Without L6, a fast tenure (listed backups, or `ack=s3`) gets the lane's
owner side only while the core's last S3 observation is fresh
(`last_s3_fresh + backup_takeover_ms`, the existing `LeaseView`
freshness): about 1.5 s into a stall, then nothing more than today.
That is safe and costs no S3 request; it is also nearly no gain for a
stalled holder with backups. Its node side (grants it holds from other
owners) is fully covered.

**Under `ack=s3` the lane buys almost nothing whatever L6 does**: the
holder's stream heartbeat is a core timer, so after `backup_takeover_ms`
(1.5 s) of core silence a subscriber takes the lease over (§3.5); the
holder is deposed and its owner-side authority ends there.

L6, if chosen, adds:

- **The lane's own liveness probe.** When an in-scope renewal is due
  within `window_ms / 2` of `last_s3_fresh + window_ms`, the lane issues
  one lease `GET` (`LaneAction::ProbeLease`; at most one in flight; **at
  most one per renewal round**, a hard acceptance number, §9; through
  the same `LeaseStore` and counting store as the core's, so the
  own-S3 watch of `e5a35fd` sees it). The probe's `GET`s apply only to
  fast tenures; a `Local` tenure never probes. The result is judged as
  `note_s3_liveness` (`backup.rs:552`) judges a `LeaseGet`:
  - names this node, the mirrored epoch, not released:
    `lane_fresh_ms = max(lane_fresh_ms, sent)`; `lane_root` uses
    `max(last_s3_fresh_ms, lane_fresh_ms)`;
  - anything else: the lane stops owner-side decisions until the next
    mirror and, if the object names an unexpired holder `h ≠ me`, sets
    `owner_hint = h`; own held grants then renew at `h` like remote
    ones (a reclaim during the successor's grace, accepted by
    `lock_renew_one`'s grace branch when nothing conflicts).
  Every probe result also goes to the core (`Event::LaneProbe { sent,
  result }`), which treats it as its own `LeaseGet` (fresh liveness;
  `note_object`; a deposition through the existing path).
- Under `ack=backup` the holder's off-core heartbeat keeps the backups
  from sealing for `CONSTELLATION_HOLDER_STALL_MS` (15 s); past it they
  seal and the lane's next probe sees the successor. So L6 extends a
  backed holder's own grants from ~1.5 s to ~16 s of stall, and then to
  the successor's grace by reclaim.

### 5.6 Atomicity with the core

Today only the core writes grant windows (owner records and held
windows); with the lane there are two writers. Two writers are safe for
*extensions* because both extension primitives are monotone:
`LockTables::extend` (`crates/meta/src/locks.rs:970`) and
`LockTables::renewed` (`:1517`) take the `max` of the old and the new
end, so concurrent extensions commute and none shortens another. What is
not safe is a core decision that *ends* or *shortens* on the strength of
a value read earlier. Every core read-decide-write on a window must be
one `LockTables` critical section, with `now` read inside it where the
decision is "expired":

- `on_lock_grant_expiry` (`:2270`): `get` then `forget` →
  `LockTables::forget_if_expired(id, now) -> Option<Grant>` (re-arm on
  `None` with the live record's `until`).
- `on_lock_renew_tick`'s lapse drop (`:3309-3315`): `held_all` then
  `drop_held` → `drop_held_if_lapsed(ino, id, now)`.
- The tick's recalled-idle release check reads a snapshot: harmless
  (a release of a grant the lane just renewed is a release the core
  decided anyway).
- Audit every `replica.locks()` call in `crates/authority/src/core`
  outside `tests.rs`: 79 call sites (67 in `locks.rs`, 5 in `jobs.rs`, 4
  in `promise.rs`, 1 each in `backup.rs`, `delegate.rs`, `holder.rs`;
  count multi-line call chains and `let locks = replica.locks()`
  bindings too), for the same pattern; record the list and the verdict
  for each in PROGRESS.
- **The core's `now` after a stall.** `Core::handle`'s `now` is taken at
  the step's start; a step that waited 20 s on the store carries a
  `now` 20 s stale into its lock decisions. That is conservative for
  expiry and conflict reads (fewer records look expired), and it must
  never be used to shorten or end anything the lane set meanwhile: the
  new primitives read `now` inside the guard, and no core path computes
  a new window end from the step's `now` and stores it with anything but
  a `max`. The audit checks this per site.

No `LockTables` critical section may perform store I/O or log:

- `take_where`'s two store-reading callers (§3.5: `:3944`, `:4481`;
  `:4105`'s predicate is store-free and stays) become: snapshot
  `(id, ino)` pairs, evaluate the predicate outside the guard, then
  `take_ids(&ids)` (new) under the guard (a grant added meanwhile is not
  taken: the next event handles it, as one added a moment later would
  be).
- `check_tag`'s debug log moves after the guard.
- A test-only guard enforces it: `LockTables::lock()` sets a
  thread-local flag while the guard lives; `GatedDb::write_tx` and a new
  `GatedDb::read_tx` wrapper `debug_assert!` the flag is clear (debug
  and test builds only).

### 5.7 Bookkeeping back to the core

The lane's decisions reach the core as events on the **urgent lane**
(so an owner's `LockGrantExpiry`, which drains the urgent lane first,
sees them): `Event::LanePeerRenewed { from, entries }` (the core does
`served.remove`, `lock_peer_heard`, `unproven.remove`, stats — the
bookkeeping half of `lock_renew_one`), `Event::LaneRenewed` (§5.4),
`Event::LaneProbe` (§5.5, L6 only), and `Event::LaneOwn { renewed }`
(stats). `LanePeerRenewed` exists only with L5.
The core never waits for them for safety; they keep its liveness
heuristics (unused grants, unreachable peers, queue positions) exact.

### 5.8 Where the lane runs; no blocking, no logging

- Engine: `crates/engine/src/lock_lane.rs` (new), one tokio task on the
  daemon's runtime, started by `Driver::run` beside `holder_alive_task`
  and stopped with it: a timer armed at the earliest of
  `LockTables::next_renewal_ms`, the next stale in-flight mark, and the
  next freshness refresh; P2P through `Peers::request_to_node_timeout`;
  (L6) the probe through `LeaseStore`. (L5) Peer renewals are answered
  synchronously inside `P2pBridge::lock_renew_requested` (`p2p.rs:931`)
  by calling the lane's pure `answer_peer` before falling back to the
  urgent lane.
- It touches `Meta` only through `meta.locks()` and
  `meta.delegations_any()` (an atomic), reads `LeaseView`'s atomics, and
  its own state. It holds no lock across an await.
- It **never logs**: no `tracing` macro in `lane.rs` or `lock_lane.rs`
  (a unit test greps both sources). Its counters are in status; the
  core logs one `info` line when a driver stall ends during which the
  lane renewed anything ("the renewal lane renewed N own, M held and K
  peer grants during a core stall of X ms"), from the core's thread,
  which logs anyway. Plan 44 makes every log write non-blocking (lines
  dropped and counted when the sink cannot keep up); the lane still does
  not log: a dropped line is lost evidence, a log storm could crowd out
  the lines that matter, and the lane's liveness must not depend on the
  log's queue.
- Sim: the same `lane.rs` driven by a task in
  `crates/authority/tests/sim/node.rs`, the bus routing `LockRenew` to
  it first (§8.1).

### 5.9 The integrity argument

Claim: every window extension the lane makes at time `t` is one the core
could have made at `t` by its own rules, so the lock model's argument
(and the successor's grace, the cap, the restart horizon, the fencing
tokens) covers it unchanged.

1. *Same clock.* The lane and the core run in one process on one clock:
   no new skew term (§2.3's `M > 2D` unchanged).
2. *Authority, owner side.* The core extends under `Route::Me` when the
   lease is usable and (L0, landed separately) fresh on a fast tenure. The lane
   requires `now < cap_until` (the lease's usable end: no TTL takeover
   before it), `fresh` (the M9 argument: an answer within
   `backup_takeover_ms` of an S3 observation that preceded any
   successor's lease CAS is covered by the successor's quarantine), no
   release in flight (revoked before the release CAS is dispatched, so
   any extension precedes the release; the successor's
   `now + ttl + margin` covers it), no epoch, no delegation (checked
   live), no gate. Each is either time-bounded and enforced by the lane,
   or revoked before the core acts on its change.
3. *The record.* The lane extends only a record that is live at `t`
   under the tables' mutex. Every core decision that ends a record
   (expiry, release, move, outwait) is one critical section (§5.6), so
   it happens either before the extension (which then finds nothing and
   defers) or after it (and sees it). No record ends while a lane
   extension of it is in flight.
4. *Restart.* (L5) A peer's extension is answered only below the durable
   horizon; own grants need none (they die with the process, `247fc59`).
5. *Node side.* The held window is extended from an owner's `Ok` with a
   `sent` no later than the real send, exactly as `renewed` does for the
   core; routing is liveness only.
6. *Tokens.* A token's `until` is the held window at the mutation's
   send; windows are never longer than the core could have made them.

Failure modes:

| Situation | Lane | Outcome |
|---|---|---|
| Disk slow, process alive | renews | no lapse up to the cap (own grants of a `Local` root; fast tenures only while fresh), the max-stall bound (held grants) |
| Process frozen / dead | frozen / gone | lapses as today (`lock-lapse-lone-node` stays green unchanged) |
| P2P cut from peers | own: renews to the cap (`Local`) or while fresh; remote: fails | as the core would |
| S3 cut, `Local` | renews to the cap | as the core would (no early takeover possible) |
| S3 cut, fast tenure | stops when freshness ends | as the core would after L0 |
| Deposed (seal, `ack=s3`) | stops when the core's freshness ends (≤ 1.5 s); with L6 at its next probe, own grants then reclaimed at the successor | covered by the successor's grace |
| Core hung for ever | node side stops at max stall; owner side at the cap | bounded |

### 5.10 What stays as it is

- Delegations: the lane's owner side is off on a delegate and on a root
  with live delegations (its node side works). A delegate's renewals of
  its delegation stay core-driven in this plan (and the root's answers to
  them get L0's freshness predicate). **Stopgap landed** (2026-10-18, ahead
  of this plan's lane, which will replace it): an off-core sender on the
  holder-heartbeat thread, active only while the driver is stuck (500 ms to
  `CONSTELLATION_HOLDER_STALL_MS`), registers each delegation renewal on
  the urgent lane and counts its answer as a late one from its own send
  (`crates/authority/src/deleg_lane.rs`, `DelegLane` in
  `authority_driver.rs`, `delegations.md` "Grants, renewal and expiry",
  PROGRESS "a delegation lost to a stuck authority core"). It sets no
  honoured end and decides nothing; the full design (the delegation
  model's tenure facts in the mirror, route stamps per grant, the root's
  answering off the core) remains deferred to this plan's follow-up.
- Without L5/L6 (§11): peers' renewals at a stalled owner, the horizon
  lead and the lane's lease probe.
- Continuation epochs: the owner side defers.
- The root lease's renewal (S3 CAS) stays in the core; a lane-driven
  lease renewal would race the core's CAS tags. Stalls past ~29 s lapse
  at the cap (§11 decision).
- `CONSTELLATION_HOLDER_STALL_MS` and the `ack=s3` stream heartbeat:
  unchanged (§11).
- Read delegations (`--cto strict`): unchanged.
- `lock-lapse-lone-node` (SIGSTOP) is unchanged and must stay green: a
  frozen process lapses.

### 5.11 Observability and configuration

- `LockStatus` (`crates/control/src/proto/types.rs:1361`, filled at
  `crates/engine/src/control/service.rs:486`; schema
  `crates/control/schema/control.schema.json`): `lane` = `{ enabled,
  cert: "root" | "holder" | "none:<reason>", own_renewals,
  remote_renewals, peer_renewals_answered, peer_renewals_deferred,
  late_ok, probes, probes_deposed, max_core_stall_covered_ms,
  max_stall_stops, idle_rounds }` (the L5/L6 counters only with those
  milestones), plus `horizon_lead_ms` (L5). `/metrics` counters of the
  same.
- Knobs (documented where read and in
  `docs/reference/configuration.md`): `CONSTELLATION_LOCK_LANE`
  (`on`/`off`, default `on`), (L5) `CONSTELLATION_LOCK_HORIZON_LEAD_MS`
  (opt-in; default today's `ttl/4`), `CONSTELLATION_LOCK_LANE_MAX_STALL_MS`
  (default 120000), and the test-only fault point
  `CONSTELLATION_FAULT_STORE_STALL_FILE` (§7 L3).

## 6. Model changes (`crates/model/src/locks.rs`, `crates/model/tests/locks.rs`)

- **L0 (renewal freshness; lands with the separate integrity fix,
  verified here).** New knob `renew_probe_freshness` (design: `true`).
  With `false`, `owner_until` drops its probe clause **for renewals
  only** — both where it gates a node's `Renew` (`owner_ready`, which
  calls `owner_until(owner_for(i))`) and where `Msg::Renew` is delivered
  (`:1491`) — while new grants keep it. Tests:
  - `fast_takeover_renewals_without_the_probe_violate`: scripts
    `[[Lock(X), Io], [], [Lock(X)]]`, `backup: 2, replicate: false,
    grace: true, lease_ttl: 30, lock_ttl: 3, max_tick: 8, max_renews: 6`,
    `renew_probe_freshness: false` → `mutual_exclusion` violated. The
    counterexample (the review's): node 2 takes over at t = 0 (grace to
    t = 6); root 0 renews its own grant at t = 1..5 (`Renew(0)` × 5);
    node 2 grants itself at t = 6; root 0 does `Io` under grant 1. This
    is the deposed root's **own** grant — finding 08's `Route::Me` path.
  - `fast_takeover_renewals_with_the_probe_are_clean`: the same with the
    design, sized for exhaustive BFS: `max_tick: 7, max_renews: 3`
    (≈ 25M states; 8/6 exceeds a 60M-state cap).
  - The remote-holder variant (a holder on another node renewing at the
    deposed root) finds nothing as the model stands, because `owner_for`
    routes a non-root's renewals by the register's holder (the
    successor), not by a stale cache. Add a `cached_owner` routing knob
    (a holder renews at the last node that answered it `Ok`, as the
    code's `lease.cached_holder` routes) and assert that variant
    violates with `renew_probe_freshness: false` and is clean with the
    design; if the executor cannot make that knob faithful within the
    state budget, it records so in PROGRESS and the own-grant test
    stands alone as the fail-before.
  - Update the module doc's action → code table: `Renew` at a fast root
    → the freshness predicate in `lock_renew_one` and `on_deleg_renew`.
- **The lane.** New config `lane: bool` (design: `true`), `max_stalls`,
  and the non-vacuity knobs below. Node state gains `stalled: bool`,
  `cert: Option<Cert { epoch, cap, fresh_from }>` set by every core
  action of the node (what `Core::lock_lane_view` publishes into
  `LeaseView`: `owner_until`'s cap and `last_probe`), cleared by
  `Release`, by a deposing `Probe`, and while the node is not the root,
  and `lane_probe: i16` (used only by the L6 knob `lane_probe: bool`).
  - `Stall(i)` / `Unstall(i)`: while stalled, node `i`'s core actions
    are disabled: delivery of messages to its core (`LockReq`,
    `Released`, `Recall`, `Mirror`, `Handoff`, ...), `Probe`,
    `RenewLease`, `Takeover`, `Expire`, `FloorPassed`, `Release`,
    `Delegate`, `SendLock`. The application's `Io` under an honoured
    grant and `Unlock` stay enabled (they are local).
  - `LaneRenewOwn(i)` (any time, stalled or not): node `i`'s cache is
    due and its owner is `i`; `cert` valid (`local(i) < cap`, fresh by
    `max(cert.fresh_from, lane_probe) + takeover_window` under a
    backup); the table record is live at `local(i)`; extend record and
    cache with `ttl = min(lock_ttl, cap − now)`.
  - `LaneAnswer(k)` (the L5 knob `lane_answers_peers`; modeled now so
    the follow-up starts from a checked design): a `Renew` message to node `h` answered by `h`'s
    lane under the same conditions (record known and live), `Ok` only;
    the message otherwise stays for the core.
  - `LaneProbe(i)` (with the L6 knob): reads the register: own and same
    epoch → `lane_probe = now`; else `cert = None`.
  - Node side: `Renew(i)` and the delivery of `RenewAck` to `i` stay
    enabled while `i` is stalled (the lane sends and applies them).
- **Non-vacuity** (each must violate `mutual_exclusion`):
  `lane_ignores_freshness` (fast takeover while the old root's lane
  renews), `lane_ignores_cap` (TTL takeover after the lease expired),
  `lane_keeps_cert_after_release` (released takeover),
  `lane_extends_expired_records` (the owner expired a record and
  granted another node; the lane revives it). And the design with
  stalls enabled (`max_stalls = 1`, contention scripts, backups, TTL
  and released takeovers) must be clean with the "sometimes" witnesses
  `lane_renewed_while_stalled` and `fast_takeover` reached.
- ADR-25 amendment text (L7) cites these tests.

The restart horizon is not in the lock model (it never was); it is
argued and tested in code (L5, if chosen). The lease/epoch models
(`flex.rs`, `backup.rs`) need no change: the lane consumes the lease
facts they establish and never changes the lease.

## 7. Milestones

Each milestone ends green on `cargo fmt`/`clippy -D warnings`/`cargo
test --workspace`; the full CONVENTIONS gates run at the end (§12).
Complex milestones (L1, L2, and L5/L6 if chosen) get an independent
review before the next starts.

**Committed scope: L0 (verify), L1–L4, L7, L8.** L5 and L6 are a
maintainer decision (§11), recommended as a follow-up plan; where a
milestone below mentions them, it applies only if they were chosen.

- **L0 — Renewal freshness on fast tenures: landed separately
  (integrity fix), verify present.** Confirmed by a model counterexample
  (§3.4) and implemented outside this plan, as plan 40's R2 was. What
  follows is its spec; this plan's executor checks each item is in the
  tree, completes anything missing before L2, and records the check:
  - Model: §6's L0 tests — the renewal-only knob affecting `owner_ready`
    and `Msg::Renew` delivery; the own-grant counterexample violates
    with the knob, the design is clean at `max_tick: 7, max_renews: 3`;
    the `cached_owner` variant, or a PROGRESS note why not.
  - One predicate `Core::renew_fresh(now) -> bool` (the
    `strict_answer_allowed` rule without its side effects), used by
    `lock_renew_one` for gen-0 routes (a peer's renewal and the node's
    own `Route::Me` renewal at the tick) and by `on_deleg_renew`
    (`core/delegate.rs:2152`, whose `root_usable`, `:431`, has no
    liveness check). Owner answers that fail it are held behind a probe
    (`probe_now`) with the existing `horizon_held` mechanism generalised
    to "held until a condition" (or a sibling list), and answered when
    the probe shows this node still holds, or refused (`NotOwner { 0 }`,
    the deposition path) when it does not; an own renewal at the tick is
    deferred and the tick re-armed.
  - Core unit tests (`crates/authority/src/core/tests.rs`): a holder
    with a listed backup and `ack.last_s3_fresh` older than
    `backup_takeover_ms`: a peer's `LockRenew`, the node's own renewal
    at the tick, and a delegate's `DelegRenew` are each not answered
    until the probe returns (each answered at once on the old code:
    **fail-before**); own lease → answered, another holder → refused
    and deposed.
  - Sim: `locks-failover-backup-renew` (the holder's backup partitioned
    from the holder while a lock is held, `PartitionBackup` after
    `lock_ios` begin) in `long_locks`; the seeds run on the old code
    and whether the ghost caught a violation recorded.
  - Docs: the renewal paragraph of `cluster-locks.md` (`:204-213`,
    "A renewal ... only while") and ADR-25's grant bullet say a
    fast-tenure owner renews (locks and delegations) only with fresh S3
    liveness, as it grants.
  - PROGRESS: the fix's own section (§13) with the model counterexample
    trace as its fail-before evidence.
- **L1 — Lock tables safe for two writers, and never blocked on I/O.**
  The §3.5 prerequisites are **load-bearing for finding 08**: the lane
  is only as live as the tables' mutex, and today two `take_where`
  predicates read the store under it and `check_tag` can log under it.
  - `crates/meta/src/locks.rs`: `forget_if_expired`,
    `drop_held_if_lapsed`, `extend_live`, `renew_own`,
    `due_renewals_lane`, `take_ids`, `HeldGrant.route`; `renewed` sets
    `route`. Core call sites switched (§5.6); the 79-site audit list,
    with the stale-`now` check per site, in PROGRESS.
  - `take_where`'s store-reading callers (`:3944`, `:4481`) restructured
    (snapshot, predicate outside the guard, `take_ids`); `:4105` stays;
    `check_tag` logs after the guard; the thread-local guard flag and
    the `debug_assert!`s in `GatedDb` (`write_tx`, new `read_tx`).
  - Tests (`crates/meta/src/locks.rs` `mod tests`): an extension landing
    between the core's expiry check and its forget is never lost (a
    `#[cfg(test)]` hook between the two in the old sequence makes the
    old code fail: **fail-before**; the new primitive passes);
    `drop_held_if_lapsed` keeps a window renewed after the snapshot;
    `take_ids` never takes a grant added after the snapshot;
    `lock_on_delegated` with the store stalled (fault point, L3's,
    brought forward) does not hold the tables' mutex: a concurrent
    `fenced()` call returns within 10 ms (**fail-before** on the old
    `take_where`); every `LockTables` method under the guard flag makes
    no store access (`Meta::open_in_memory`, assertions on).
- **L2 — The lane model, the lane-view mirror, the sans-IO lane, the
  sim.**
  - First the model (§6, the lane part): clean with the design, each
    non-vacuity knob violates — **before** any lane code.
  - `Core::lock_lane_view(now, replica) -> LockLaneView` (cap, the
    exclusions, `lock_owner`) in `core/locks.rs`; `LeaseView` gains the
    §5.2 fields and `lane_root(now)`; `Driver::refresh()` publishes them
    (with `set_liveness` folded into the same ordered publish).
  - `crates/authority/src/lane.rs` (new; exported from `lib.rs`):
    `LockLaneView` and `RenewLane` with `handle(now, LaneEvent,
    &LockTables, &dyn LaneFacts) -> Vec<LaneAction>` (`Tick`,
    `Renewed`, `RenewFailed` in; `Send`, `Report`, `SetTimer` out; L5
    adds `answer_peer`, L6 `Probe`/`ProbeLease`). `LaneFacts` is the
    read side of the mirror (`LeaseView` in the engine, the sim's
    equivalent), so both drivers judge the same facts. Module doc in the
    style of `core/locks.rs`, with §5.9's argument.
  - The `Event::LaneRenewed`/`LaneOwn` handlers (`core/locks.rs`),
    `event_kind` names in the driver.
  - Sim (`crates/authority/tests/sim/node.rs`, `bus.rs`, `run.rs`): the
    lane task per node reading a per-node mirror refreshed after each
    core event; `Bus::send` hands `LockRenewed` with a lane request id
    to the sender's lane (and, with L5, `LockRenew` to the target's lane
    first); `FaultKind::StallCore { node, for_ms }` (the core loop
    pauses as `Pause` does; the lane, its timers and its bus traffic go
    on); `SimConfig::lane: bool` (default `true`); an off-core holder
    beat (`SimConfig::holder_beats`: while a node holding a lease is
    stalled but not paused or crashed, its backups receive
    `HolderAlive` every `backup_heartbeat_ms` up to a scaled
    `holder_stall_ms`, as `holder_alive_task` does) so the "stalled but
    not sealed" case is reachable.
  - Tests: lane unit tests over `Meta::open_in_memory()` and synthetic
    mirrors — never past `cap_until`; nothing when `lane_root` is
    `None` (each exclusion), on an expired record, a missing held id,
    `deleg_any`, a stale freshness; `sent` ≤ the real send; stale
    in-flight marks re-sent; a late `Ok` applied with its own `sent`.
    Core tests: `lock_lane_view` excludes each state of §5.2, and the
    step that issues the release CAS publishes the exclusion before its
    dispatch. Sim configs §8.1, with the **fail-before**
    `locks-core-stall` at `lane: false` (fenced I/O > 0) against
    `lane: true` (fenced I/O = 0 for stalls under the cap).
- **L3 — Engine lane task: own grants (finding 08).** Recorded first:
  the lane-off baselines of §9 (5–7) on every host file system.
  - `crates/engine/src/lock_lane.rs` (new), started in `Driver::run`;
    `CONSTELLATION_LOCK_LANE`.
  - Fault point: `CONSTELLATION_FAULT_STORE_STALL_FILE=<path>` in
    `crates/meta/src/store/gate.rs` (`GatedDb::write_tx`: while the file
    exists, a writer that took the lock waits, polling every 10 ms, and
    writes `<path>.held` once; one cached env lookup when unset, as
    `crates/engine/src/fault.rs` does), plus `Meta::set_write_stall`
    for in-process tests.
  - No-logging test (grep of the two lane sources); the core's stall
    summary line.
  - Tests (engine, in-process, one node, `--locks cluster`): a held
    `flock`, the store stalled for 3 × the lock TTL (short TTL), a
    write and `fsync` attempted after the stall: **fail-before** with
    `CONSTELLATION_LOCK_LANE=off` (`EIO`, `owners_fenced = 1`), clean
    with the lane (no `EIO`, `lane.own_renewals > 0`, the lock still
    held, `fenced_io = 0`); a stall past the lease's usable end (short
    lease TTL) lapses at the cap, never later (the held window's end ≤
    `cap_until`); with `ack=backup` and a stale freshness the lane does
    not renew (the L0 rule holds in the lane too).
- **L4 — Engine: held grants at a remote owner.**
  - Node-side remote renewals in `lock_lane.rs`; route memory; max-stall
    bound.
  - Tests (in-process multi-node, the `shipper.rs` pattern): B holds a
    grant from A; B's store stalled for 3 × TTL: **fail-before** with
    the lane off (B's owner fenced), clean with it; A recalls during the
    stall (C asks): B's renewals return `recalled`, C waits, and gets
    the lock after B's core resumes and releases (no overlap); B's core
    hung past the max-stall bound (short bound): B's grant lapses, C
    gets the lock after A outwaits it; the same under a delegation
    (B's grant from a delegate): renewed through the route memory.
- **L5 — (decision, §11) Engine: peers' renewals at a stalled owner;
  opt-in horizon lead.**
  - `P2pBridge::lock_renew_requested` calls `answer_peer`; the shared
    held-answer counts updated inside `lock_answer` and
    `on_lock_horizon_persisted` (§5.3), `lock_answer`'s doc sentence;
    the durable-horizon `AtomicI64` raised in the `PersistLockHorizon`
    task (`authority_driver.rs:2969`);
    `CONSTELLATION_LOCK_HORIZON_LEAD_MS` (opt-in) in
    `lock_need_horizon`; `Event::LanePeerRenewed`.
  - Tests: A owns B's grant; A's store stalled 3 × TTL (TTL scaled so
    the default `ttl/4` lead is short against it): with the default
    lead B lapses (**fail-before** for the lead), with the opt-in lead
    = TTL it does not; a crash of A after the lane answered a renewal:
    A's restart quarantine covers the extended window
    (`load_lock_quarantine ≥` the answered `until`); an answer the core
    holds for B **in the step that is running** (a test hook inside
    `lock_answer`) makes a concurrent lane answer to B defer (order
    kept; **fail-before** against a set published only by `refresh()`).
- **L6 — (decision, §11) Engine: fast tenures.**
  - The lane's probe (at most one per renewal round); `owner_hint`;
    `Event::LaneProbe` in the core.
  - Tests: a holder with a backup, its store stalled 10 s (under
    `holder_stall_ms`): own grants renewed, probes ≤ renewal rounds, no
    deposition; stalled 25 s: sealed at ~16.5 s, the lane's probe sees
    the successor, own grants reclaimed at the successor (no `EIO` if
    the successor's grace admits the reclaim), no conflicting grant
    issued before the old windows end; `ack=s3` holder stalled 5 s:
    taken over at ~1.5 s, same checks (and the expectation that the
    lane adds nothing there, stated in the test's doc).
- **L7 — Observability, docs, ADR.**
  - Status/metrics/schema (§5.11); `docs/reference/configuration.md`
    (the knobs, the fault point under the test-only table);
    `docs/reference/features/cluster-locks.md` — rewrite
    `:317-339` (a grant lapses when its node *stops* — frozen, dead,
    partitioned past its window — or its core is hung past the bounds;
    a slow disk alone no longer lapses a `Local` tenure's own grants or
    grants held at a healthy owner, up to the lease cap; what still
    lapses: a stalled owner's peers past the horizon, fast tenures past
    their freshness, `ack=s3` holders deposed), a new subsection
    "Renewals off the core" after `:236`, the max-stall bound,
    troubleshooting entries; `docs/reference/features/durability-and-failover.md`
    (L6 only: the lane's probe as an S3 request); **the host
    guidance** (§1, D6): `lazytime`/`noatime` on the state dir's file
    system and the journal on its own device (fewer store stalls), the
    daemon log on a device that does not stall (fewer dropped log lines:
    since plan 44 the log no longer stalls the daemon), in the
    operations/troubleshooting docs and in `cluster-locks.md`'s
    troubleshooting; `docs/how-to-guides/development/TESTING.md` (sim
    configs, scenarios, the fault point).
  - ADR-25 (`docs/explanation/DECISIONS.md:786`): "**Amended (renewals
    off the core)**" — the lane, what it may judge (§5.1), the mirror
    as its certificate, the argument (§5.9), the model tests, the
    rejected options (A, C, D1, D2, D7); and correct `:808-811` ("sure to
    survive ... only up to `(ttl − margin)/2`" holds now only for a
    frozen process, a hung core, or the cases still listed above).
  - Proposed DESIGN.md text for the coordinator (CONVENTIONS rule 5:
    not edited here), recorded in PROGRESS: the lock section's renewal
    paragraph.
- **L8 — System tests, the real-disk reproduction, benchmarks.** §8 and
  §9; the after-numbers in `bench/lock-lane/RESULTS.md`; xcheckfs runs.

Order: L0 (verify), L2's model part, L1, the rest of L2, L3, L4, L7,
L8; L5 and L6 only if chosen, after L4.

## 8. System tests

### 8.1 Authority simulation (`crates/authority/tests/sim.rs`)

New configurations (registered in both config tables, `:333`-style and
`:2656`-style, and in `long_locks`), each with `run_m14`'s checks (the
mutual-exclusion ghost, visibility, turn writes where enabled):

- `locks-core-stall`: 3 nodes, `StallCore` of the holder and of a
  non-holder for 2 s each (over `ttl − margin` = 1 s, under the lease's
  usable remainder), plus one random fault. Fixed seeds in a `#[test]`
  `locks_survive_core_stalls`: zero violations; `fenced_ios == 0` for
  the stall faults (liveness, the lane's point); grants > 500.
  **Fail-before**: the same seeds with `lane: false` have
  `fenced_ios > 0`.
- `locks-core-stall-single`: one node (finding 08), stalls of 2 s and
  8 s (past the 6 s lease: the cap must end the grants, the ghost and a
  window-end ≤ cap check pass).
- `locks-core-stall-backup` (backup core config, `holder_beats` on and
  off): stalls of 2 s and 5 s; seals, takeovers, reclaims (safety only
  without L6: the lane stops at the core's freshness).
- `locks-core-stall-s3`: `ack=s3` (fast takeovers on silence; safety
  only — the lane is expected to add nothing here).
- `locks-core-stall-writes`: `with_lock_writes` of the first.
- `locks-core-stall-blips`: with `EpochOutage` (the certificate's epoch
  exclusion under transitions).
- `locks-core-stall-delegated`: with a delegation (owner side deferred,
  node side through the lane).
- `locks-core-stall-peers` (L5 only): the root stalled while peers hold
  its grants.
- `locks-failover-backup-renew` (L0, verified present).
- Non-vacuity in the sim: a `#[cfg(test)]` lane switch that ignores
  freshness must make `locks-core-stall-backup` find a violation within
  its seed range (keep the seed that does as a regression test of the
  switch; if none is found, say so and rely on the model's).
- Every existing `locks-*` config runs with the lane on (its default):
  `long_locks` with `AUTHORITY_SIM_SEEDS=1000` stays clean.

### 8.2 Harness scenarios (`crates/harness/src/scenarios.rs`, `scenarios/m14.rs`)

- `lock-store-stall-lone-node`: `lock-lapse-lone-node`'s workload (a
  locked file with one fsynced and one dirty write, other threads
  creating/linking/renaming), but the store stalled through
  `CONSTELLATION_FAULT_STORE_STALL_FILE` for 3 × a 3 s lock TTL instead
  of SIGSTOP. Expect: every op answers OK (ops blocked on the store
  finish after it), no `EIO`, both writes present after the close, the
  lock never lost (`locks.lost == 0`, `owners_fenced == 0`,
  `lane.own_renewals > 0`); remount, tree and file as acknowledged,
  fsck clean. **Fail-before**: the scenario run with
  `CONSTELLATION_LOCK_LANE=off` must fail its no-`EIO` check (the
  scenario has a `--expect-lapse` variant asserting exactly that, run
  in the same scenario so the fault point is proven effective).
- `lock-store-stall-past-lease`: lease TTL 12 s, lock TTL 3 s, the
  store stalled 20 s: the lapse happens no later than the lease's usable
  end (from status timestamps), the application sees `EIO` after it,
  and the fence lifts with the lock (the cap is enforced).
- `lock-store-stall-holder` (two nodes): node B holds the lock, B's
  store stalled 10 s while A's client blocks on the same lock: B keeps
  the lock (no `EIO`), A gets it only after B's unlock.
- (L5 only) `lock-store-stall-owner` (two nodes): the root A's store
  stalled 15 s while B holds a grant from A and writes under it: no
  `EIO` on B with the opt-in lead = TTL; with the default lead, B lapses
  (the variant run in the same scenario).
- `lock-store-stall-backup` (three nodes, a backup): the holder's store
  stalled 25 s: sealed, taken over; no two holders (the harness's
  `fenced_owners` / held-grant timestamps compared across nodes as
  `lock-failover` does); the holder's own lock lapsed (without L6) or
  reclaimed (with L6), never overlapping. A safety scenario in the
  committed scope.
- `lock-lapse-lone-node` unchanged and green (a frozen process lapses).

### 8.3 xcheckfs

Re-run the 2026-10-13 cell that produced finding 08 (one node,
write-back, an overloaded test host) and a two-node cell, with the
state dir on a slow device (§9): zero lock-lapse `EIO` findings; any
other finding is triaged as unrelated or fixed.

## 9. Benchmarks and acceptance

`bench/lock-lane/run.sh` and `RESULTS.md` (new; **before L3** the
baseline with the lane off is recorded on every host file system — the
stall-tolerance runs, `lock-latency` (5), the renewal traffic (6) and
the idle cost (7); L8 records the after-numbers), release build, a
Linux test host, an S3 bucket of the usual harness setup; state-dir
host file systems ext4, xfs, btrfs and tmpfs (the maintainer's
no-regression rule).

Stall tolerance (fault point; defaults unless stated; 5 runs each, stall
length D ∈ {5, 10, 15, 20, 25, 30, 40} s; record lapses, `EIO`s, the
held window's end against `cap_until`):

1. Lone node, own grants: **zero lapses for D ≤ 25 s**; for D > the
   lease's usable remainder, the lapse at the cap ± 1 s, never later.
   Baseline (lane off): lapses for D > 9.5 s in some runs, always for
   D ≥ 19 s (recorded).
2. Holder at a healthy owner: zero lapses for D ≤ 60 s; stop at the
   max-stall bound (a run with the bound at 30 s).
3. (L5 only) Owner with peers' grants: default lead (`ttl/4`):
   reported, expected ≥ 15 s survival at peers; opt-in lead = TTL: zero
   lapses at peers for D ≤ 25 s.
4. Fast tenure with a backup: without L6, no overlap and the own grants'
   lapse no later than the core's freshness allows (reported, expected
   as the lane-off baseline: the lane adds ~1.5 s); with L6, zero lapses
   for D ≤ 14 s, and for D = 25 s no overlap, own grants reclaimed or
   lapsed (reported). `ack=s3`: no overlap; no gain expected (§3.3).

Real-device reproduction (the finding-14 approach, on a test host; not
in CI): the state dir on a `dm-delay` device (1 s delay) under 8
threads fsyncing, and a 15 s freeze (`dmsetup suspend`/`resume` of the
state dir's device, or a cgroup v2 `io.max` throttle on the daemon's
cgroup only), on ext4, xfs and btrfs: **zero lapses and zero
owner-fenced `EIO`** over 10 runs each; the baseline numbers recorded
with the lane off (expected: lapses in some runs, as reported).

Overhead (no regression; ≤ 3 % or ≤ 1 IQR, whichever is larger):

5. `lock-latency`'s measurements (first lock from a non-sequencer,
   cached re-lock, the sequencer's own lock, a contended handoff, lone
   node `cluster` vs `local`) on every host file system.
6. Renewal traffic: `LockRenew` messages per held grant per minute
   unchanged (± 10 %); zero additional S3 requests in the committed
   scope (any tenure); with L6, **at most one lease `GET` per renewal
   round** (a hard acceptance number, counted per round in status), and
   only on fast tenures.
7. CPU and the always-on lane's idle cost: a renewal round with zero due
   grants (no lock held anywhere, and 10,000 grants held none due) —
   wake-ups per minute and CPU per round reported, the lane armed only
   when `next_renewal_ms` says so (zero wake-ups with no grant held);
   the lane task under 0.5 % of one core with 10,000 held grants (a
   round is O(due grants), as the core's tick).
8. (L5 only) Restart quarantine after a crash with peers' grants live:
   measured at the default lead and the opt-in lead = TTL; must not
   exceed `lead + ttl + margin + 1 s` past the crash.

If 1–4 or the real-device acceptance fail, the plan is not done: report
the numbers and hypotheses; do not loosen the bar.

## 10. Risks

- **Two writers of grant windows.** A missed read-decide-write in the
  core would let a lane extension race an expiry (an owner forgets what
  its holder still honours). Mitigated by §5.6's primitives, the audit
  list, the model's `lane_extends_expired_records` knob, the sim's
  ghost under always-on lanes.
- **A stale mirror.** Mitigated by excluding every state whose rule
  differs (§5.2), publishing in `refresh()` before dispatch (the order
  the FUSE fast path already relies on), and time bounds enforced by the
  lane; the model's non-vacuity knobs. (L5) The held-answer set is the
  one fact a step-old mirror would get wrong; it is a shared structure
  the core updates inside `lock_answer` instead (§5.3).
- **Hidden blocking in the lane.** Store access (prevented by
  construction and the debug guard), logging (no `tracing` in the lane;
  the P2P stack's own logging on its request path is out of the lane's
  control — L3 checks it does not log per request at the default
  level), a runtime starved by blocked workers (`store_call` bounds the
  store's share; the lane is one task; recorded).
- **Holding locks for a hung node.** Bounded: the lease cap (owner
  side), the max-stall bound (node side).
- **Hiding a sick node.** The slow-step warnings, `own_s3`, and the new
  `lane.max_core_stall_covered_ms` keep it visible.
- **(L6) Fast-tenure probes add S3 GETs**: at most one per renewal
  round, fast tenures only (hard acceptance, §9).
- **L0 changes timing**: renewals at an idle fast-tenure root wait one
  S3 round trip when liveness is stale (the late-answer path covers the
  holder's 500 ms timeout).
- **(L5) Restart quarantine grows** with the opt-in horizon lead; the
  default is unchanged.
- **Gain is uneven across tenures**: large for `Local` tenures and for
  holders at a healthy owner; about 1.5 s for a backed holder without
  L6; nothing under `ack=s3` (§3.3). The docs say so (L7).
- **The sim's coverage of the engine task.** The decision logic is
  shared (`lane.rs`); the engine wrapper (timers, P2P, the bridge hook)
  is covered by the in-process tests and the harness only.

## 11. Maintainer decisions before start

1. **Design B as scoped** (owner side only for a root without
   delegations or epochs; node side everywhere), A recorded as a
   long-term direction, C/D1/D2/D7 rejected.
2. **Staging: L5 and L6.** Recommended: **a follow-up plan** for both.
   L1–L4 deliver finding 08 and the stalled-holder case with no new S3
   traffic and no quarantine cost; L5 (answering peers at a stalled
   owner) brings the held-answer ordering and the horizon question, L6
   (the lane's lease probe, routing to a successor) brings S3 `GET`s and
   only helps backed holders (nothing under `ack=s3`). Alternatively,
   keep either in this plan after L4.
3. **(If L5) Horizon lead default**: recommended **keep today's
   `ttl/4`** as the default and make `CONSTELLATION_LOCK_HORIZON_LEAD_MS`
   opt-in. A lead of one TTL raises peers' survival at a stalled owner
   from ≈ 15.5 s to ≈ 28 s but lengthens *every* crash-restart
   quarantine with peers' grants live from ≈ 27 s to ≈ 42 s, for a
   corner case.
4. **Max-stall bound** for node-side renewals: 120 s (proposed), or
   none (a process stuck on a slow disk keeps its lock, as on a local
   file system).
5. **Always-on lane** (proposed; its idle cost measured, §9.7) vs lane
   only while the core is stalled.
6. **Root lease renewal stays in the core** (stalls past ~29 s lapse at
   the cap) — or a follow-up for an off-core lease renewal.
7. **`CONSTELLATION_HOLDER_STALL_MS` stays 15 s**, and the `ack=s3`
   stream heartbeat stays a core timer (a 1.5 s stall deposes an
   `ack=s3` holder, so the lane buys almost nothing there) — or a
   follow-up plan moves that heartbeat off the core like
   `holder_alive_task`.
8. **No logging from the lane** plus the core's summary line. Decided
   since: the non-blocking log writer the maintainer declined on
   2026-10-14 was decided for on 2026-10-18 (plan 44); the host-level
   mitigations (`lazytime`/`noatime` on the state dir, logs on a fast
   disk) remain general guidance, which L7 writes into the docs.
9. **The test-only fault point** in `meta`'s write path
   (`CONSTELLATION_FAULT_STORE_STALL_FILE`).
10. **Delegated owner side** as a follow-up plan.

Resolved: **L0** is confirmed (model counterexample) and lands
separately as an integrity fix, holding answers behind a probe; this
plan verifies it (§7 L0).

## 12. Exit criteria

- [ ] L0 (landed separately) verified present: the renewal-only model
      knob with the own-grant counterexample and the clean BFS run, the
      `cached_owner` variant or its PROGRESS note, one freshness
      predicate for new grants, lock renewals and `on_deleg_renew`, the
      core unit tests, the docs; anything missing completed.
- [ ] L1: atomic primitives, every core window decision one critical
      section (79-site audit list with the stale-`now` check in
      PROGRESS), no store access or logging under the tables' mutex
      (`take_where` at `:3944`/`:4481`, `check_tag`; debug guard on),
      the fail-before race and mutex-latency tests.
- [ ] Model: the lane clean with stalls; each of the four non-vacuity
      knobs violates.
- [ ] L2: `lane.rs`, `lock_lane_view` and the extended `LeaseView`
      published in `refresh()`, the `Lane*` events, the sim with
      `StallCore` and holder beats; §8.1 configs clean, the lane-off
      fail-before showing fenced I/O.
- [ ] L3–L4: engine lane (own grants, held grants), the fault point,
      the max-stall bound, each with its fail-before test; the lane-off
      baselines recorded before L3.
- [ ] L5/L6 only if chosen (§11): peers' answers with the in-`lock_answer`
      held set and the opt-in lead; the probe at ≤ 1 per round.
- [ ] L7: status/metrics/schema, configuration, cluster-locks,
      durability-and-failover, host guidance, TESTING docs, ADR-25
      amended, the proposed DESIGN text recorded.
- [ ] L8: harness scenarios 3× green (`lock-lapse-lone-node`
      included), `long_locks` at 1,000 seeds clean, §9 acceptance met on
      every host file system, the real-device reproduction clean,
      xcheckfs runs with no lock-lapse findings.
- [ ] Every CONVENTIONS gate green; pjdfstest 8798/8798.

## 13. PROGRESS.md

Two sections in the established style (intro paragraph, "What landed"
table, "Unchanged on purpose", "Gates" with dates, kernel and host file
systems). First, written by the separate L0 integrity fix:
`## Fix: a fast-tenure root renewed lock grants and delegations without
fresh S3 liveness (plan 41 L0)`, carrying **the model counterexample
trace as its fail-before evidence** (scripts `[[Lock(X), Io], [],
[Lock(X)]]`, `backup: 2, replicate: false, grace: true, lease_ttl: 30,
lock_ttl: 3, max_tick: 8, max_renews: 6`, renewal-only freshness off:
node 2 takes over at t = 0, grace to t = 6; root 0 renews its own grant
at t = 1..5, `Renew(0)` × 5; node 2 grants itself at t = 6; root 0 does
`Io` under grant 1 — `mutual_exclusion` violated, grants 0, 1, 2), the
clean run's size (`max_tick: 7, max_renews: 3`, ≈ 25M states), and the
core unit tests that failed before. Then `## Plan 41 — Lock renewals
through local disk stalls`, with these rows:

| Item | State | Where |
|---|---|---|
| Renewal freshness on fast tenures (L0, landed separately: verified) | | the fix's section; `core/locks.rs` `lock_renew_one`, `core/delegate.rs` `on_deleg_renew`, `crates/model/src/locks.rs` |
| Lock tables safe for two writers (L1) | | `crates/meta/src/locks.rs`, `crates/meta/src/store/gate.rs`, `core/locks.rs` call sites |
| Lane model (L2) | | `crates/model/src/locks.rs`, `crates/model/tests/locks.rs` |
| Lane view in `LeaseView`, sans-IO lane, sim `StallCore` (L2) | | `crates/engine/src/lease.rs`, `crates/authority/src/lane.rs`, `core/locks.rs`, `crates/authority/tests/sim/` |
| Engine lane: own grants, fault point (L3) | | `crates/engine/src/lock_lane.rs`, `authority_driver.rs`, `crates/meta/src/store/gate.rs` |
| Engine lane: held grants (L4) | | `crates/engine/src/lock_lane.rs` |
| Engine lane: peers' renewals, opt-in horizon lead (L5, if chosen) | | `crates/engine/src/p2p.rs`, `core/locks.rs` `lock_answer`, `lock_need_horizon` |
| Engine lane: fast tenures (L6, if chosen) | | `crates/engine/src/lock_lane.rs`, `core/locks.rs` |
| Observability, docs, ADR-25 (L7) | | `crates/control/src/proto/types.rs`, `docs/reference/`, `docs/explanation/DECISIONS.md` |
| Harness, real-device runs, benchmarks (L8) | | `crates/harness/src/scenarios/m14.rs`, `bench/lock-lane/RESULTS.md` |

Also record: the `replica.locks()` audit list (79 sites) with a verdict
per site; the L0 verification; the §11 staging decision for L5/L6; the
proposed DESIGN.md
text; that `docs/reference/features/cluster-locks.md:317-339` and ADR-25
`:808-811` were corrected; the measured baseline (lane off) next to the
after-numbers; the claims of §1 this plan could not verify, with what L8
measured for them.

## Sources

- In-repo: `crates/authority/src/core/{locks,backup,lease,mod,stream,delegate}.rs`,
  `crates/authority/src/replica.rs`, `crates/meta/src/{locks,readdeleg,delegation}.rs`,
  `crates/meta/src/store/gate.rs`, `crates/engine/src/{authority_driver,p2p,store_io,log_buffer,fault}.rs`,
  `crates/cli/src/daemonize.rs`, `crates/model/src/locks.rs`,
  `crates/model/tests/locks.rs`, `crates/authority/tests/sim{.rs,/}`,
  `crates/harness/src/scenarios/m14.rs`,
  `docs/reference/features/{cluster-locks,durability-and-failover}.md`,
  `docs/explanation/DECISIONS.md` (ADR-12, ADR-21, ADR-25),
  `vendor/fjall/CONSTELLATION-PATCH.md`; commits listed in §1.
- Chubby (Burrows, OSDI 2006): lock sequencers and lease grace after a
  master failover; Kleppmann, "How to do distributed locking" (fencing
  tokens); NFSv4 (RFC 8881 §8.4, §9.6) lease renewal and the expired
  lease rule.
