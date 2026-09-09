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

One accuracy limit follows from the mount itself and is not fixable
here: `open` replies `FopenFlags::empty()`
(`crates/cli/src/fusefs_ops.rs:686`) — no `direct_io`, no `keep_cache` —
so repeated reads within one open handle are served from the kernel page
cache and never reach `read()`. Atime therefore moves on page-cache
misses, not on every `read(2)`. Under `relatime`'s 24 h granularity this
is invisible; under `lazy` it means a hot cached file's atime lags. The
same caching cuts the other way on observation: a peer's `stat` serves a
cached attr until the kernel's attr timeout expires, so a test asserting
cross-node visibility must allow for that on top of the flush and ship
intervals. Both facts belong in `docs/reference/features/atime.md`.

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

## Step 0 — No segment version bump

`SegmentEnvelope` pins `v == 2` exactly
(`crates/cli/src/shipper.rs`) and `records: Vec<LogRecord>` is a
closed postcard enum, so a node running an older binary that tails a
segment containing a new `LogRecord` variant fails to decode **the whole
segment**, not just the unknown record.

Mixed-version clusters and existing-bucket upgrades are out of scope, so
there is nothing useful for a version bump to gate: keep `v = 2`, add
`LogRecord::Atime`, and treat any decode failure (postcard or otherwise)
as unsupported. No forward-compatible framing, no capability negotiation,
no staged rollout.

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

  The `ctime_ns < time_ns` guard keeps an explicit `utimensat`
  authoritative: `touch -a -d 2020-01-01` sets atime backwards *and*
  bumps ctime to now, so a read-atime record emitted before it is
  dropped rather than resurrecting a newer value. The rule is broader
  than that motivating case — *any* ctime bump (a peer's `write`,
  `chmod`, `truncate`) also discards an older in-flight read-bump. That
  is fine and self-healing: under `relatime` the next read sees
  `atime < mtime` and bumps again. Never write ctime.

  This statement — guard, clamp, and `MAX()` — lives in **one helper**
  used by all three apply sites: replay of a foreign record, the
  holder's `AtimeBatch` handler, and the emitting node's own local
  apply (Step 4). Divergent local and replayed semantics would break
  convergence in exactly the case Step 8 asserts.
- `TouchSet::add`: `LogRecord::Atime { .. } => {}` — no dentry, no ino.
  An atime record therefore neither suppresses a foreign record nor is
  suppressed by pending local work, in either direction.

Clock skew: `max()` means the fastest clock wins. Clamp on apply to
`now + CONSTELLATION_ATIME_SKEW_TOLERANCE_S` (default 300) so one badly
skewed node cannot park an inode's atime in the far future.

## Step 1b — Atime lives in its own keyed table, never in `journal`

Atime rows must not enter the `journal` table. Three existing scans
would pay for them, none of which is a filter away from being cheap:

- `journal_backlog_of` (`crates/cli/src/shipper.rs:1096`) implements a
  *count* by calling `take_journal_grouped(usize::MAX)`, which
  postcard-decodes every journal row of every partition. It runs once
  per partition per sync round (`crates/cli/src/main.rs:2619`).
- The tail path rebuilds the pending `TouchSet` with
  `take_journal(usize::MAX)` for **every foreign segment applied**
  (`crates/cli/src/shipper.rs:610`). The `TouchSet::add` no-op of Step 1
  makes atime rows semantically inert there, not free.
- Partition merge eligibility requires `journal_backlog_of(&id) == 0`
  (`crates/cli/src/shipper.rs:1211`), so atime rows deliberately held
  back by `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` would keep an
  atime-only partition from ever merging.

The `journal` table is `(seq, record BLOB, part)`
(`crates/meta/src/sqlite.rs:119`) with no record-kind column, so
"exclude `Atime`" cannot be a cheap SQL predicate there. Give atime its
own table:

```sql
CREATE TABLE IF NOT EXISTS atime_journal (
    ino      INTEGER PRIMARY KEY,
    part     TEXT NOT NULL,
    atime_ns INTEGER NOT NULL,
    time_ns  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS atime_journal_part ON atime_journal(part);
```

written with an upsert that keeps the max:

```sql
INSERT INTO atime_journal (ino, part, atime_ns, time_ns) VALUES (?1,?2,?3,?4)
ON CONFLICT(ino) DO UPDATE SET
    part     = excluded.part,
    atime_ns = MAX(atime_ns, excluded.atime_ns),
    time_ns  = MAX(time_ns,  excluded.time_ns)
```

Consequences, all of them the point:

- **One row per inode, ever.** The in-memory accumulator (Step 3)
  coalesces reads *within* a flush interval; this coalesces *across*
  them. A hundred reads of one file cost one row, and so do a hundred
  flushes of it — which matters under `lazy`, where a 1 s granularity
  and a 10 s flush would otherwise append ~30 rows per inode per ship
  window. Pending size is bounded by **distinct inodes read since the
  last ship**, never by read count and never by flush count.
- Every existing journal scan is untouched: no `kind` column, no
  filter, no added decode cost, and `part_for_record`
  (`crates/meta/src/sqlite.rs:815`) needs no `Atime` arm.
  `journal_backlog_of` keeps its current definition, which is what makes
  the Step 5 idle-release exemption free instead of a migration.
- Shipping drains this table into `LogRecord::Atime` records at segment
  build time (Step 5); nothing else in the system ever sees an atime row.
- No backwards compatibility is required (Step 0), so this is a plain
  `CREATE TABLE IF NOT EXISTS` in the schema with no migration path.

`part` is stored rather than recomputed so a drain never has to walk
`partition_of` per row; it is refreshed on every upsert, so a rename
across partitions between two reads settles on the newer partition.

### The meta API surface

`MetaStore` (`crates/meta/src/lib.rs:170`) has a single implementor,
`SqliteMeta` (`crates/meta/src/sqlite.rs:2898`), so this is four cheap
additions. Name them up front — the first one existing exactly once is
what makes "local apply and replay cannot diverge" a property of the
code rather than of the implementer's discipline:

| Method | Used by | Does |
|---|---|---|
| `apply_atime(&[(Ino, i64)], time_ns) -> Result<u64>` | replay, `AtimeBatch`, local flush | **The** shared helper: skew clamp, `ctime_ns < time_ns` guard, `MAX()` merge, never writes ctime, never journals. Returns rows applied (feeds `applied`/`skew_clamped`). |
| `queue_atime(&[(Ino, i64, i64)]) -> Result<()>` | holder-side publication (Step 4.3, `AtimeBatch`) | The `atime_journal` upsert, resolving `part` once per inode. |
| `atime_backlog_of(part) -> Result<u64>` | Step 5 release/merge checks | `SELECT COUNT(*) ... WHERE part = ?`. Deliberately *not* folded into `journal_backlog_of`, which must keep meaning "real write backlog". |
| `take_atime_of(part, max) -> Result<Vec<(Ino, i64, i64)>>` and `clear_atime(part, &[Ino])` | shipper drain | Read rows, then delete **only after** the segment PUT succeeds (see Step 5). |

Splitting read from delete is what makes the drain safe: a failed PUT
leaves the rows for the next round, and a PUT that succeeded but whose
delete did not simply re-ships a duplicate, which `MAX()` absorbs. The
inverse (delete-then-PUT) silently loses atime on every failed ship, so
do not collapse these into one call.

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

`do_read` (`crates/cli/src/fusefs_ops.rs:1205`) already holds the
per-inode write shard for the whole read
(`let mut writes = self.writes.lock(ino);`) and already loads the full
`FileAttr` via `self.meta.getattr(ino)` to get `size`. Both of the
expensive things are therefore already paid for.

- Keep the whole `attr` instead of `.map(|a| a.size)`. Note it is an
  `Option`: `do_read` falls back to `manifest.file_len` when the inode
  has no attr row, and the hook must simply not bump in that case
  rather than assuming an attr.
- `WriteShards` is a tuple struct over
  `[Mutex<HashMap<Ino, WriteState>>; WRITE_SHARDS]` whose `lock()` hands
  back the `HashMap` guard directly
  (`crates/cli/src/fusefs.rs:115-125`), so "add a second field" means
  changing the shard payload to a small struct and touching every call
  site. Do that — the hook then costs a policy check against values
  already in registers plus, in the rare bump case, one `HashMap`
  insert under a lock the read already holds.
- **The flusher must `try_lock` each shard and skip on contention**
  (counting the skip). `do_read` holds the shard across
  `read_committed_chunk`, i.e. across an S3 fetch, so a blocking drain
  would stall behind network I/O. Skipping is legal: the entry is
  simply picked up by the next flush.
- Coalesce by inode with `max()`. Repeat reads of a hot file are a
  branch and nothing else.
- Bump before the `offset >= file_len` early return is *not* required:
  a read that returns zero bytes need not move atime, and letting the
  early return skip the hook is the intended behaviour.
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

1. Drain every shard (`try_lock`, skip on contention) into
   `Vec<(Ino, atime_ns, time_ns)>`; resolve each inode's partition once
   via `meta.partition_of`.
2. Apply locally in **one** transaction, through the shared helper of
   Step 1 — same ctime guard, same skew clamp, same `MAX()` — so local
   `stat` reflects local reads within one flush interval regardless of
   what happens next, and so a value applied here can never be one a
   replica would have rejected. This is the "always at least try" half.
   It writes `inode`, not `journal`.
3. Per partition, choose a publication path:
   - **Local holder and `view.usable()`** → upsert into
     `atime_journal` (Step 1b). Do **not** call `LeaseView::touch()`
     (see Step 5).
   - **Not holder** → one batched forward,
     `MutateOp::AtimeBatch { entries: Vec<(Ino, i64, i64)> }` —
     `(ino, atime_ns, time_ns)`; the emitter's `time_ns` must travel
     with the batch or the holder cannot apply the Step 1 ctime guard
     and would have to substitute its own clock, which is exactly the
     value the guard exists to avoid. With
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
4. On unmount: one final flush, then — with whatever remains of a short
   deadline — one drain of `atime_journal` into a final segment for each
   partition still held, exactly as at idle release (Step 5). Past the
   deadline, or for any partition not held, drop what remains. Atime
   must never extend an unmount, but an orderly unmount is the last
   chance these rows have, so spend the deadline on them rather than
   discarding them unconditionally.

`MutateOp::AtimeBatch` on the holder side
(`crates/meta/src/mutate.rs::execute`) runs the same shared helper and
upserts one `atime_journal` row per entry, so a whole node's read
activity costs one round trip per flush interval rather than one per
read — and lands as at most one pending row per inode on the holder.

## Step 5 — Lease and shipping interactions (the traps)

These are the things that turn a cheap feature into a lease-thrash bug.
Each needs an explicit test.

- **Idle lease release — ship, then release.**
  `LeaseKeeper::idle_release_due` (`crates/cli/src/lease.rs:482-487`)
  requires `journal_backlog == 0`. Because atime rows live in
  `atime_journal` (Step 1b), that backlog already excludes them for
  free: a partition whose only pending work is atime reads as idle and
  is free to hand its lease back, which is the behaviour we want —
  a pure reader must not pin a write lease.

  But a release must not silently strand those rows. Once the lease is
  gone the shipper has no `ship_epoch` for the partition and will never
  ship them; they would sit in `atime_journal` forever, invisible to
  every backlog check, and only escape if the node happened to
  reacquire. So idle release is **ship-then-release**: before
  `k.release()` (`crates/cli/src/main.rs:2619`), if the partition has
  any `atime_journal` rows, drain them into one final segment under the
  still-held epoch, then release. This is the one place atime is worth
  an extra S3 PUT — the lease is being given up anyway, the PUT is
  bounded by one segment, and it is what makes a read-heavy node's
  work actually reach the cluster rather than dying with its lease.

  It stays best effort: if that final ship fails, log at debug, **drop
  the rows**, and release anyway. Atime must never delay or block a
  lease handoff, and the same rule applies to an explicit handoff and
  to a lost lease (drop; the rows belong to an epoch we no longer own).
- **`LeaseView::touch()`.** Never called for atime. Otherwise a pure
  reader looks like an active writer to placement and holds the lease
  against a real writer.
- **Never acquire.** Atime is the one mutation class that must not reach
  `require_lease_for`. A read-only node that never holds a lease and
  never successfully forwards simply keeps atime local forever, which is
  an acceptable outcome by construction.
- **No `Nudge`.** Upserting an atime row must not wake the shipper.
  Atime-only partitions ship on the next round that happens anyway,
  after `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` (default 300), or at
  idle release (above) — otherwise a read-heavy workload turns into a
  steady stream of S3 PUTs, which is a real bill, not just CPU. Because
  `atime_journal` coalesces per inode, waiting longer strictly *shrinks*
  the segment rather than growing a backlog.
- **Partition merge.** Merge eligibility also gates on
  `journal_backlog_of(&id) == 0` (`crates/cli/src/shipper.rs:1211`).
  Atime rows must not block a merge either — the separate table gives
  this for free, but assert it: a partition read into existence and
  never written still merges. A merge, like a release, drains
  `atime_journal` for the source partition first (or drops it).
- **The drain seam.** Where atime turns back into log records: after
  the shipper has built a partition's segment from
  `take_journal_grouped` and decided it is shipping that partition, it
  appends `take_atime_of(part, max)` as `LogRecord::Atime` records to
  the same segment, then deletes those rows with `clear_atime` **only
  after the PUT succeeds**. Ordering matters and only one direction is
  safe: a failed PUT must leave the rows for the next round, and a
  successful PUT whose delete is lost re-ships duplicates, which
  `MAX()` absorbs harmlessly. Delete-then-PUT would lose atime on every
  failed ship. Atime records never cause a segment to be shipped on
  their own before `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S`; they ride
  along with one that is going anyway.
- **Reintegration.** `Atime` records stranded on a deposed branch are
  droppable by definition; reintegration should discard rather than
  report them, and `atime_journal` rows for a partition whose epoch we
  lost are dropped outright.

## Step 6 — Configuration

Mount flag `--atime <off|relatime|lazy>` (default `off`), overridable by
`CONSTELLATION_ATIME`. Env-only tunables, all `CONSTELLATION_*` with the
existing `fn foo() -> T` + default-constant pattern
(`crates/cli/src/shipper.rs:52-58`):

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_ATIME_GRANULARITY_S` | mode-dependent (see below) | bump threshold |
| `CONSTELLATION_ATIME_FLUSH_MS` | 10000 | accumulator flush period |
| `CONSTELLATION_ATIME_MAX_PENDING` | 65536 | accumulator cap |
| `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` | 300 | max delay before atime-only partitions ship |
| `CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS` | 200 | batched forward timeout |
| `CONSTELLATION_ATIME_SKEW_TOLERANCE_S` | 300 | clamp on apply |
| `CONSTELLATION_ATIME_RO_FORWARD` | 0 | let a read-only member forward atime batches |

`CONSTELLATION_ATIME_GRANULARITY_S` has no single default: it falls back
to 86400 under `relatime` and 1 under `lazy`, resolved *after* the mode
is known, and an explicit value overrides both. Implement it as
`fn granularity(mode: AtimeMode) -> Duration` with two constants — not
one constant with a mode-specific override, which is how the two
defaults end up silently collapsing into one.

Document in `docs/reference/configuration.md` and a new
`docs/reference/features/atime.md` following the feature template. The
feature doc must state the page-cache limit from the Goal — an operator
comparing `lazy` atime against a local filesystem will otherwise file it
as a bug.

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
  setattr, flusher `try_lock` skip leaves the entry queued.
- `atime_journal`: N flushes of the same inode leave exactly one row
  holding the max; a rename across partitions updates `part`.
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
- Ship-then-release: A reads, goes idle, releases the partition lease;
  B observes the atime afterwards (the release did not strand it), and
  A's `atime_journal` is empty.
- Drain ordering: a failed segment PUT leaves the `atime_journal` rows
  intact and the next round ships them; a duplicated ship converges.
- Unmount drains held partitions within the deadline; past it, unmount
  still completes promptly.
- An atime-only partition is still merge-eligible.

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

1. `LogRecord::Atime` + the shared apply helper + replay + `TouchSet`
   exemption + `atime_journal` table + unit tests.
   (Inert: nothing emits the record yet; envelope stays at `v = 2`.)
2. Policy module + accumulator in the write shard + read hook, flushing
   to **local only**, plus the mode flag. Usable single-node at this
   point, and independently reviewable.
3. `atime_journal` publication, batched forward, segment drain, and the
   Step 5 lease guards including ship-then-release.
4. Metrics, docs, harness scenario, perf lane.

## Settled decisions

- **Wire compat**: no backwards compatibility, no mixed clusters — keep
  `v = 2` and add `LogRecord::Atime` without an envelope bump (Step 0).
- **Read-only members**: local-only by default, opt-in forwarding via
  `CONSTELLATION_ATIME_RO_FORWARD=1`.
- **Modes**: `off` / `relatime` / `lazy`. No `strict` — we cannot honour
  strict-atime semantics and will not name a mode as though we could.
- **Storage**: atime never enters the `journal` table; it gets its own
  upsert-coalesced `atime_journal` (Step 1b), so existing journal scans
  keep their current cost and the backlog exemptions are free.
- **Lease handoff**: ship-then-release on idle release, best effort —
  preserve the atimes if one bounded PUT can, drop them if it cannot.
  Never delay a handoff.

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- pjdfstest FULL pass under both `--atime off` and `--atime relatime`.
- The read-only fio record-volume assertion, with the actual number
  pasted into the report.
- Explicit confirmation that a read-only workload on a non-holder
  acquires no leases (paste the relevant status counters).
