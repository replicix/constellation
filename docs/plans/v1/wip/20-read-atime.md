# Plan 20 — Optional read-time atime (batched, eventually consistent, best effort)

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §4 (metadata plane, log, partitions, leases),
`docs/reference/features/forwarded-mutations.md`.

## Goal

Make `read()` bump `atime` when the operator asks for it, without giving
up any of the properties that made noatime the default:

- **Optional.** Default stays `off`; today's behaviour is bit-identical.
- **High performance.** Zero extra locks, zero extra DB reads, and zero
  extra S3 traffic on the read hot path.
- **Eventually consistent.** A bump on node A becomes visible on node B
  after a flush interval plus normal segment shipping. No bound tighter
  than that is promised.
- **Best effort.** Losing a bump is legal. We aim not to, but atime must
  never block a read, never acquire a lease, never fail an operation,
  and never keep a node from unmounting.

Non-goals: POSIX-strict atime, `atime` on `readdir` (see §11), atime as
an input to any correctness decision (GC, eviction, reintegration).

## Why the existing `Setattr` path cannot be reused

Three hard blockers, each of which motivates a piece of the design:

1. `Setattr` bumps `ctime` — both in `meta::sqlite::setattr`
   (`crates/meta/src/sqlite.rs:3034`) and in replay
   (`crates/meta/src/replay.rs:697-700`). A read must not change ctime.
2. `TouchSet` treats `Setattr { ino }` as touching the inode
   (`crates/meta/src/replay.rs:99-104`). Pending local atime records
   would suppress a *foreign* `chmod`/`chown`/`truncate` on the same
   inode during tail (`crates/cli/src/shipper.rs:600-607`) — permanent
   divergence, exactly the failure mode the forwarded-mutations doc
   warns about. Atime must be invisible to conflict detection.
3. `Setattr` replay is last-record-wins per field, so it depends on log
   order. Atime wants an order-free merge so a dropped, delayed, or
   reordered record can never make replicas disagree.

So: a dedicated record with `max()` merge semantics.

## Step 0 — Segment version bump

`SegmentEnvelope` pins `v == 2` exactly
(`crates/cli/src/shipper.rs:161-169`) and `records: Vec<LogRecord>` is a
closed postcard enum, so a node running an older binary that tails a
segment containing a new `LogRecord` variant fails to decode **the whole
segment**, not just the unknown record.

Mixed-version clusters are out of scope, so this is simply a version
bump: set `v = 3`, keep the exact-match check, and let a mismatch raise
the existing loud "unsupported log segment version" error. No
forward-compatible framing, no capability negotiation, no staged
rollout.

## Step 1 — `LogRecord::Atime`

Add to `crates/meta/src/record.rs`:

```rust
/// A read-time access-time bump. Best-effort and order-free: replay
/// merges with max(), so a duplicate, delayed, or out-of-order record
/// can never make replicas disagree, and a dropped one only costs
/// freshness. Deliberately *not* a `Setattr`: it must not touch ctime
/// and must not participate in conflict detection.
Atime {
    ino: Ino,
    /// The access time being claimed.
    atime_ns: i64,
    /// When the emitting node observed the read. Used to drop bumps
    /// that were already in flight when someone set atime explicitly.
    time_ns: i64,
},
```

`crates/meta/src/replay.rs`:

- `apply_one`: skip if the inode is gone (`Applied::Done`, attrs moot),
  then

  ```sql
  UPDATE inode SET atime_ns = MAX(atime_ns, ?2)
   WHERE ino = ?1 AND ctime_ns < ?3
  ```

  The `ctime_ns < time_ns` guard is what keeps explicit `utimensat`
  authoritative: `touch -a -d 2020-01-01` sets atime backwards *and*
  bumps ctime to now, so any read-atime record emitted before it is
  dropped rather than resurrecting a newer value. Never write ctime.
- `TouchSet::add`: `LogRecord::Atime { .. } => {}` — no dentry, no ino.
  An atime record therefore neither suppresses a foreign record nor is
  suppressed by pending local work, in either direction.

Clock skew: `max()` means the fastest clock wins. Clamp on apply to
`now + CONSTELLATION_ATIME_SKEW_TOLERANCE_S` (default 300) so one badly
skewed node cannot park an inode's atime in the far future.

## Step 2 — Bump policy (the main performance lever)

`crates/cli/src/atime.rs` (new). Pure function, no I/O:

```rust
pub enum AtimeMode { Off, Relatime, Lazy }

/// Linux relatime semantics: bump only if atime is older than the
/// granularity, or if it trails mtime/ctime (so "has this been read
/// since it changed?" keeps working, which is the only thing most
/// tooling actually asks atime).
pub fn should_bump(mode: AtimeMode, attr: &FileAttr, now_ns: i64, granularity_ns: i64) -> bool
```

- `Off` — never. Default.
- `Relatime` — `now - atime >= granularity` (default 24 h) **or**
  `atime < mtime` **or** `atime < ctime`.
- `Lazy` — bump whenever `now - atime >= granularity` with a small
  default (1 s). This is "strict-ish" but still coalesced per flush
  interval and still lossy. Do **not** offer a mode named `strict`: we
  cannot honour strict-atime semantics and should not imply we do.

Under `Relatime` a read-heavy workload emits at most one record per
inode per day. That is what makes the feature affordable.

## Step 3 — Read-path hook (zero added cost)

`do_read` (`crates/cli/src/fusefs_ops.rs:1155-1180`) already holds the
per-inode write shard for the whole read
(`let mut writes = self.writes.lock(ino);`) and already loads the full
`FileAttr` via `self.meta.getattr(ino)` to get `size`. Both of the
expensive things are therefore already paid for.

- Keep the whole `attr` instead of `.map(|a| a.size)`.
- Store the pending-atime map **inside the existing write shard**
  (`WriteShards`, `crates/cli/src/fusefs.rs:66-89`) as a second field,
  so the hook adds no lock acquisition at all — just a policy check
  against values already in registers and, in the rare bump case, one
  `HashMap` insert.
- Coalesce by inode with `max()`. Repeat reads of a hot file are a
  branch and nothing else.
- Cap total pending entries at `CONSTELLATION_ATIME_MAX_PENDING`
  (default 65536). On overflow drop the new entry and count it. Best
  effort means an unbounded accumulator is never the right answer.
- `Off` short-circuits before touching anything, so the default path is
  a single predictable-branch load.

Explicit `setattr` with an atime must purge that inode from the
accumulator, so a queued read-bump cannot clobber a fresh `touch -a`
locally (the ctime guard in Step 1 handles the remote case).

## Step 4 — Flush

New background ticker on the sync task, `CONSTELLATION_ATIME_FLUSH_MS`
(default 10_000 — deliberately far slower than the 500 ms sync tick;
atime does not need freshness). One flush:

1. Drain every shard into `Vec<(Ino, atime_ns)>`; group by partition via
   `meta.partition_of`.
2. Apply locally in **one** transaction
   (`UPDATE inode SET atime_ns = MAX(atime_ns, ?)`, no journal), so
   local `stat` reflects local reads immediately regardless of what
   happens next. This is the "always at least try" half.
3. Per partition, choose a publication path:
   - **Local holder and `view.usable()`** → journal `Atime` records.
     Do **not** call `LeaseView::touch()` (see Step 5).
   - **Not holder** → one batched forward,
     `MutateOp::AtimeBatch { entries: Vec<(Ino, i64)> }`, with
     `CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS` (default 200, shorter than
     the 500 ms mutation forward). One attempt. On `Busy`, `NotHolder`,
     timeout, or transport error: count it and **discard**. Never fall
     back to `require_lease_for`. Never retry into a lease acquisition.
     Requeue at most once, then drop.
   - **Read-only member** → local-only by default; with
     `CONSTELLATION_ATIME_RO_FORWARD=1`, forward the batch like any
     other non-holder. Atime carries no authority, so this is the one
     record class a read-only member may publish. Nothing else about
     the read-only invariant changes, and the flag must gate *only*
     `AtimeBatch` — assert that in a test.
   - **Any other read-only gate** (`departed`, `epoch_frozen`, offline
     `ReadOnly`, synthetic/snapshot inode) → stop after step 2.
     Local-only atime, nothing journalled, no flag to override.
4. On unmount: one final flush with a short deadline, then drop whatever
   remains. Atime must never extend an unmount.

`MutateOp::AtimeBatch` on the holder side
(`crates/meta/src/mutate.rs::execute`) applies the same max-merge and
journals one `Atime` record per entry, so a whole node's read activity
costs one round trip per flush interval rather than one per read.

## Step 5 — Lease and shipping interactions (the traps)

These are the things that turn a cheap feature into a lease-thrash bug.
Each needs an explicit test.

- **Idle lease release.** `LeaseKeeper::idle_release_due`
  (`crates/cli/src/lease.rs:482-487`) requires `journal_backlog == 0`.
  Atime records in the journal would pin a write lease on a node doing
  nothing but reading. Compute that backlog **excluding `Atime`
  records**: a partition whose only pending records are atime is idle.
- **`LeaseView::touch()`.** Never called for atime. Otherwise a pure
  reader looks like an active writer to placement and holds the lease
  against a real writer.
- **Never acquire.** Atime is the one mutation class that must not reach
  `require_lease_for`. A read-only node that never holds a lease and
  never successfully forwards simply keeps atime local forever, which is
  an acceptable outcome by construction.
- **No `Nudge`.** Journalling an atime record must not wake the shipper.
  Atime-only partitions ship on the next round that happens anyway, or
  after `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` (default 300) — otherwise
  a read-heavy workload turns into a steady stream of S3 PUTs, which is
  a real bill, not just CPU.
- **Reintegration.** `Atime` records stranded on a deposed branch are
  droppable by definition; reintegration should discard rather than
  report them.

## Step 6 — Configuration

Mount flag `--atime <off|relatime|lazy>` (default `off`), overridable by
`CONSTELLATION_ATIME`. Env-only tunables, all `CONSTELLATION_*` with the
existing `fn foo() -> T` + default-constant pattern
(`crates/cli/src/shipper.rs:52-58`):

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_ATIME_GRANULARITY_S` | 86400 (relatime) / 1 (lazy) | bump threshold |
| `CONSTELLATION_ATIME_FLUSH_MS` | 10000 | accumulator flush period |
| `CONSTELLATION_ATIME_MAX_PENDING` | 65536 | accumulator cap |
| `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` | 300 | max delay before atime-only partitions ship |
| `CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS` | 200 | batched forward timeout |
| `CONSTELLATION_ATIME_SKEW_TOLERANCE_S` | 300 | clamp on apply |
| `CONSTELLATION_ATIME_RO_FORWARD` | 0 | let a read-only member forward atime batches |

Document in `docs/reference/configuration.md` and a new
`docs/reference/features/atime.md` following the feature template.

## Step 7 — Observability

`AtimeStats` of `AtomicU64`, modelled on `ForwardState`
(`crates/cli/src/forward.rs:41-88`): `queued`, `coalesced`, `applied`,
`dropped_cap`, `forward_ok`, `forward_err`, `local_only`,
`skew_clamped`. Surface on `StatusReport`
(`crates/api/src/types.rs`), in `constellation status`
(`crates/cli/src/main.rs:3295-3302`), and on the web UI dashboard.

`dropped_cap` and `forward_err` climbing steadily is the operator's
signal that atime is being lossy — which is legal, but worth seeing.

## Step 8 — Tests

Unit:
- `should_bump` decision table, including `atime < mtime` and the
  granularity boundary.
- Accumulator: coalescing to max, cap behaviour, purge on explicit
  setattr.
- Replay: `max()` merge is idempotent and order-free; ctime guard drops
  a pre-`touch -a` record; inode-gone is a no-op; skew clamp.
- `TouchSet`: an `Atime` record neither conflicts nor is conflicted with.

Multi-node in-process (the pattern at the bottom of
`crates/cli/src/shipper.rs`):
- A reads, B eventually observes the atime.
- A and B read concurrently; every replica converges on the max.
- B runs an explicit `touch -a` backwards while A has a read-bump in
  flight; the explicit value survives on every replica.
- A read-heavy loop on a non-holder never acquires a lease and never
  increments `handed the lease to a peer`.

Harness scenario `atime-eventual` (`crates/harness/src/scenarios.rs`):
two nodes, read-heavy seeded workload on one, `eventually()` asserts
visibility on the other; a variant that cuts S3 via toxiproxy and
asserts reads keep succeeding at full speed while atime updates are
simply lost.

Perf and compliance:
- `tests/perf_regression`: read throughput with `--atime relatime`
  within a small delta of `off`; and a read-only fio workload must
  produce a bounded number of log records (this is the assertion that
  catches an accidental per-read record).
- pjdfstest stays a FULL pass in **both** `off` and `relatime`.
- xfstests: default `off` leaves the baseline unchanged. Run the
  `relatime` lane too and, if `generic/003` / `generic/120` now pass,
  remove them from `tests/xfstests-baseline.txt` with a note — they are
  currently baselined precisely because atime does not move.

## Step 9 — Suggested implementation order

1. `LogRecord::Atime` + replay + `TouchSet` exemption + segment `v = 3`
   + unit tests. (Inert: nothing emits the record yet.)
2. Policy module + accumulator in the write shard + read hook, flushing
   to **local only**, plus the mode flag. Usable single-node at this
   point, and independently reviewable.
3. Journal/forward publication and the Step 5 lease guards.
4. Metrics, docs, harness scenario, perf lane.

## Settled decisions

- **Wire compat**: no backwards compatibility, no mixed clusters —
  straight `v = 3` bump (Step 0).
- **Read-only members**: local-only by default, opt-in forwarding via
  `CONSTELLATION_ATIME_RO_FORWARD=1`.
- **Modes**: `off` / `relatime` / `lazy`. No `strict` — we cannot honour
  strict-atime semantics and will not name a mode as though we could.

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- pjdfstest FULL pass under both `--atime off` and `--atime relatime`.
- The read-only fio record-volume assertion, with the actual number
  pasted into the report.
- Explicit confirmation that a read-only workload on a non-holder
  acquires no leases (paste the relevant status counters).
