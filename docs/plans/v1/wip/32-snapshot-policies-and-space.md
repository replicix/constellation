# Plan 32 — Automatic snapshot policies and per-snapshot space accounting

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §13 (snapshots — **stale**, see "Where we
are" below), §14 (GC), §10 (control plane). Feature docs:
`docs/reference/features/prune.md` (the xattr-policy precedent this plan
follows), `docs/reference/features/forwarded-mutations.md`. Plans:
09 (snapshots), 22 (prune policies), 28 (snapshots became retained tree
roots), 30 (forwarding, exactly-once `rid`s). Code context:

- `crates/cli/src/snapshot.rs` — `SnapshotManager::create` (:218-282),
  `covering` (:293-337), `delete` (:405-412), `snapshot_chunk_refs`
  (:516-550), `split_selector`/`validate_name` (:567-580)
- `crates/cli/src/main.rs` — `snapshot_barrier` (:5197-5220), the
  `Snapshot`/`Prune` subcommands (:341, :430-515), `DaemonStatus`'s
  `StatusSource` impl (:5980-6040)
- `crates/store-s3/src/snapshot.rs` — `SnapshotRecord`, `SnapshotStore`
- `crates/meta/src/store/snapshot.rs` — the replicated snapshot row
  (`snapshot_record`/`parse_snapshot_record`, :21-53)
- `crates/meta/src/record.rs` — `SnapCreate`/`SnapDelete` (:131-142)
- `crates/cli/src/gc.rs` — `mark_chunks` (:344), `snapshot_roots` (:510)
- `crates/cli/src/prune.rs`, `crates/meta/src/prune/{policy,eval}.rs`,
  `crates/cli/src/singleton.rs` — the pruner, its parser, `SingletonLease`
- `crates/mtree/src/tree.rs` — `Tree::diff` (:453), cost ∝ difference
- `crates/api/src/{types.rs,lib.rs,web.rs}`, `crates/api/webui/`

Depends on plan 30 being committed. Plans 19, 23 and 31 do not affect it.

## Goal

1. **Snapshots that manage themselves.** An operator declares once, on
   a directory, "every 5 minutes, keep them a day; hourly for a week;
   daily for a month". From then on the cluster creates and expires
   those snapshots with nobody watching. The operator touches them only
   to keep one forever (`hold`) or to delete one early, for example
   because it pins a lot of space.
2. **Space you can see.** For any snapshot, show the ZFS-style numbers:
   what it holds alone (`USED`), what it added over its predecessor
   (`WRITTEN`), what it references (`REFER`). For any set of snapshots,
   show what deleting them would return. These must be cheap enough to
   show on every `snapshot ls` and every web UI refresh.
3. **Both surfaces.** The CLI, and a graphical web UI page with a policy
   editor, a retention timeline and a space view.

The whole feature is **optional and inert by default**. With no policy
anywhere, the scheduler runs no work, and the accountant runs only when
someone asks for sizes.

Non-goals:
- Replicating snapshots to another bucket (`zfs send`).
- Cron expressions. Calendar-aligned intervals cover the requested
  cases; a cron-style `at=` can be added later without a format change.
- Per-user quotas, or snapshot space counting against the quota.
- Writable snapshots (that is still what clones are).
- Exact *physical* (compressed, encrypted) byte accounting per snapshot.
  See Step 6.3 for what is exact and what is estimated.

## Where we are (verified 2026-09-28)

What the tree does today. Several of these are prerequisites that must
be fixed first (Step 0), because automatic schedules multiply the
snapshot count by two orders of magnitude.

- **A snapshot is a retained metadata root.** It is the triple
  `(commit seq, whole-FS mtree root, dir ino)`, stored twice with no
  transaction spanning the two copies:
  - the bucket object `snaps/<blake3(path@name)>.json`, which is
    `SnapshotRecord` v2;
  - a replicated `ns` row `0x30 | Subsystem::Snapshot | id`, which has
    exactly 4 fields `[path, name, root_hash, created_unix_ms]`. The
    parser rejects any other field count.

  Creation copies nothing, but it forces a metadata publish.
  **DESIGN §13 still describes plan 09's tree blobs and lazy clones.**
  Both are gone. Record that in PROGRESS.md; do not edit DESIGN.md.
- **Creation pulls the write lease.** `snapshot_barrier` sends
  `SyncRequest::Acquire` before its `Barrier`. A scheduler node taking a
  snapshot every 5 minutes would steal the root lease from the node
  actually writing, every 5 minutes. This is a blocker.
- **GC re-walks every snapshot in full, every round.**
  `snapshot_roots` → `snapshot_chunk_refs` does a readdir walk of each
  snapshot's whole subtree and GETs every spilled chunk list. The only
  dedup is by identical root string. The cost is O(Σ subtree size over
  all snapshots): 400 auto snapshots of a 1M-file tree means 400M
  entries per round. This is a blocker. The *metadata* mark in
  `mtree_gc` already shares structure (a `seen` set).
- **No per-snapshot size exists anywhere.** `chunk_ref` covers the live
  tree only, and has no refcounts and no birth times. `UsageTracker` is
  whole-FS logical bytes. The virtual xattr `rsize` inside a snapshot
  view is an O(n) scan on every call.
- **No protection flag, no origin.** Nothing distinguishes a manual
  snapshot from one a policy would own. `Subsystem::Hold` (0x05) is
  reserved and unwritten. The `holds/` bucket objects are unrelated:
  they cover open-unlinked files.
- **Identity is the path string.** After a directory rename, its old
  snapshots no longer appear under the new path's
  `.constellation/snapshot`, because `covering` does prefix matching.
- **Partial-failure windows.** Create writes the `snaps/` object, then
  the row. Delete removes the row, then the object. Both orders fail
  toward a *leak*: an orphan `snaps/` object pins its chunks with no
  listing. They never fail toward a dangling row. Churn makes leaks
  routine.
- **`delete_snapshot` linearly scans all rows** even though the id is
  computable.
- **`snapshot ls` prints pretty JSON** with no sizes and no times. The
  web UI snapshot panel (`webui/index.html:32`, `:167-169`) shows
  `path@name` and a Delete button.

## Prior art, and what this plan takes from it

A survey done for this plan covered ZFS tooling (zfs-auto-snapshot,
sanoid, znapzend, zrepl, TrueNAS periodic tasks), btrfs (snapper,
btrbk, qgroups), backup retention (restic, borg, Proxmox Backup Server,
kopia, Time Machine), and cloud/enterprise systems (NetApp ONTAP, AWS
DLM, GCP schedules, Azure, CephFS snap-schedule, Longhorn,
snapscheduler, Synology, VSS). Sources are listed at the end. The
lessons below drive the design. Each is cited where it is applied.

| # | Lesson | Evidence | Applied as |
|---|---|---|---|
| L1 | **One snapshot stream, retention as a filter.** Separate snapshots per tier (a "daily" and an "hourly" both taken at midnight) are clutter. | zfs-auto-snapshot and sanoid do this. restic, snapper, btrbk, Ceph, Synology and znapzend do not. | One stream per policy; tiers only select what to keep |
| L2 | **A snapshot counts toward every tier it satisfies** (union). borg and PBS instead make later rules skip already-kept snapshots, so "7 daily" reaches back past 7 days; users trip on it (borg #7687). | restic, snapper, Ceph, Synology | Union semantics |
| L3 | **Outages must not age history away.** Age measured from *now* (kopia #3758, DLM, TrueNAS lifetime) deletes everything after a long stop. Counting populated buckets, or measuring from the newest snapshot, does not. | restic `--keep-within` measures from the latest snapshot | Retention windows anchor on the policy's newest snapshot, not on the clock |
| L4 | **Calendar-aligned buckets, explicit timezone.** zrepl's grid, anchored on the youngest snapshot, is its most-reported confusion (#92, #91, #122). Every calendar tool must pick a timezone. | snapper, btrbk, PBS: local time. Ceph, zrepl: UTC | Aligned buckets; `tz=` in the policy, default UTC |
| L5 | **The representative is the *first* snapshot of a bucket.** It is final the moment it exists, so "which tiers keep this" can be shown at once and never flips. | snapper, btrbk, Synology, Azure | Oldest-in-bucket |
| L6 | **Ownership by metadata, never by name pattern.** Name matching is the top footgun: TrueNAS schema reuse deleted year-old snapshots; zrepl #400 destroyed everything unmatched. | snapper `cleanup=`, DLM tags, PBS `protected`, kopia pins, ZFS holds | `origin` + `policy_ino` fields and a `held` flag; names are labels only |
| L7 | **Explain every decision.** Show *why* a snapshot is kept, and preview a policy against real data before it deletes anything. | kopia `(hourly-2,daily-1)`, the PBS prune simulator (color per rule, struck-through removals), `borg prune --list --dry-run` | `KEPT BY` / `EXPIRES` columns; a server-side simulator behind both CLI and UI |
| L8 | **Skip empty snapshots.** | TrueNAS "allow empty" (off by default), btrbk `onchange`, Longhorn | `skip-empty=yes` by default; cheap because roots are Merkle |
| L9 | **`used` (unique) is not additive; `written` is.** Show both, and answer "what if I delete these" as a set query. | ZFS: `used`, `written`, `usedbysnapshots`, `destroy -nv a%b`. Ahrens, BSDCan 2019 | `USED`, `WRITTEN`, `REFER`, range dry-run, FS breakdown |
| L10 | **Never do global accounting on the write path.** btrfs qgroups slow every commit, so distros turn them off. ONTAP's `compute-reclaimable` is capped at 3 snapshots and hurts client I/O. | btrfs docs, snapper, NetApp | Accounting is a derived, off-path, incremental index |
| L11 | **ZFS's deadlist trick assumes "no afterlife"** (a block never reappears). Content addressing breaks this: a reverted file re-references an old chunk, and dedup shares chunks across lineages. | Ahrens | Per-chain *interval runs* with multiplicity, instead of birth times (Step 6.2) |

## Settled decisions

These were taken while writing this plan. Do not relitigate them. If
one proves unimplementable, stop and report.

- **The policy is the marker.** It lives in full in the xattr
  `user.constellation.snapshots` on the directory, as plan 22 does for
  prune. There is no registry, and the setting is replicated by
  construction. The web UI edits the same xattr through the API.
- **No inheritance.** A policy covers exactly the subtree of the
  directory that carries it. Nested policies are independent streams
  (a `/` policy and a `/db` policy both run), just as manual snapshots
  of nested paths are independent today.
- **One stream per policy, union retention, first-of-bucket,
  anchored on the newest snapshot** (L1–L5).
- **Only `origin=auto` snapshots of that policy's directory inode are
  ever deleted automatically.** Manual snapshots, held snapshots, and
  snapshots of a directory whose policy was removed are never deleted
  automatically.
- **Expiry uses no clock.** Which snapshots survive is a pure function
  of the replicated snapshot rows plus the policy text. The clock only
  decides *when to create* and the *grace* after a policy change. Both
  only delay; neither can cause a deletion.
- **Accounting is advisory, derived and node-local.** It never gates
  GC and is never published as truth. Every number carries the commit
  sequence it is "as of". GC keeps deciding reclaimability from the
  bucket alone.
- **Snapshot creation never moves the write lease.** (Step 0.1.)

## Step 0 — Prerequisites: snapshots at automatic scale

This step lands alone, and before anything else. Each fix is valuable
without the rest of the plan.

### 0.1 Create a snapshot at the holder, not by taking the lease

Replace `snapshot_barrier`'s `SyncRequest::Acquire` with a forwarded
snapshot operation, executed by the node that holds the root lease:

- **`SnapshotBatch { rid, items: [ {path, name, origin, policy_ino,
  skip_if_unchanged_since: Option<root>} ] }`**. The holder does the
  following, in order:
  1. drains what `Barrier` drains today;
  2. forces one publish;
  3. evaluates each item's empty check (Step 3.4) against that commit;
  4. for every non-skipped item, writes `snaps/` (CAS create), then the
     row;
  5. returns per-item results: `Created{id, seq}`, `Skipped`, or
     `AlreadyExists` (treated as success; see names in Step 3.3).

  One publish covers all policies due at the same tick.
- If this node is the holder, run the batch locally, as today. If there
  is a live holder, forward it over the plan-30 path, with exactly-once
  semantics through the `rid`. If no holder exists at all, acquire the
  lease as today: an unheld lease is free to take.
- Manual `snapshot create` uses the same path, as a batch of one.
- Test: node B holds the lease and writes continuously. Node A creates
  10 snapshots. The lease never moves, and every snapshot's content is
  a prefix of B's writes.

### 0.2 GC marks snapshot chunks per chain, by diff

New module `crates/cli/src/snapwalk.rs`, shared with Step 6.

**Definitions.**
- A **chain** is the set of snapshots whose `SnapshotRoot.ino` is the
  same directory, ordered by `(seq, created_unix_ms)`.
- The **chunk occurrences** of a snapshot are the multiset of chunk
  hashes (data chunks plus spilled-list objects) of every file
  reachable under `ino` in that root.

**The walker.**
- `ChainWalk::first(root, ino)` enumerates the occurrences of the
  chain's oldest snapshot with a full walk. This is today's
  `snapshot_chunk_refs`.
- `ChainWalk::step(prev_root, next_root, ino)` yields occurrence
  **deltas** (`+hash`, `-hash`). It runs `Tree::diff(prev, next)` and
  maps each changed key into the subtree:
  - `0x01 ino` (inode row changed): if the inode is in the subtree,
    emit the diff of its old and new manifests.
  - `0x02 parent|name` / `0x04 ino|parent|name` (names changed): a name
    appearing or disappearing *under* the subtree adds or removes that
    inode's manifest occurrences. **If the named inode is a directory
    that crossed the subtree boundary** (moved in or out), walk that
    directory fully in the relevant root and add or remove every
    occurrence below it. This is the one case where cost is
    proportional to the moved subtree, not to the diff. It is correct,
    and it is rare.
  - **Membership** means: an inode is in the subtree at root R if any
    of its `0x04` back-references leads, through the ancestor walk, to
    `ino` in R. Cache the ancestor answers per `(root, dir ino)`.
  - **Hardlinks** weigh by the number of in-subtree links. Adds and
    removes are symmetric, so a zero-crossing is exact.
  - Atime-only changes to an inode row (plan 20) produce no delta.
- Spilled chunk lists are immutable, so cache them by hash in the
  node-local chunk cache. Never re-GET one per round.

**GC's protected set** for a chain is
`occurrences(first) ∪ ⋃ additions(step_k)`. Every snapshot's chunk set
is a subset of this union, so it is a **superset of what the old walk
protected**, and that is the only direction GC safety needs. The cost
is O(oldest snapshot + Σ diffs). A chain of 400 five-minute snapshots
now costs roughly one walk.

**Roots.** GC takes its roots from `snaps/` **∪** the replica's
snapshot rows, tailed to head. The union is belt and braces against any
future window where a row exists without its object.

**Transition knob.** `CONSTELLATION_GC_SNAP_WALK=diff|full` (default
`diff`) keeps the old walk for one release. Add a test that runs both
on randomized histories and asserts `diff ⊇ full`, and `diff == full`
when no snapshot was deleted from the middle of a chain.

### 0.3 Orphan reconciliation and delete-by-id

- `delete_snapshot` looks up by computed id, not by scan.
- **Orphan reconciliation**, in the GC round. A `snaps/` object with no
  row, whose `created_unix_ms` is older than `gc.horizon`, is deleted
  and journaled with rule `snap-orphan-object`. A row with no object
  is re-PUT from the row and journaled with rule `snap-object-restored`.
  That case should be impossible; log at `warn`.

### 0.4 Snapshot row and record extensions

- The row value gains **optional trailing fields**:
  `origin: u8` (0 manual, 1 auto), `policy_ino: u64`, `held: u8`,
  `creator: u64`, `held_by: Option<String>`. `parse_snapshot_record`
  accepts 4 or more fields. Absent fields mean manual, 0, not held,
  unknown, no owner.
  - **`held_by` is the hold's owner namespace** (added for plan 37's
    CSI driver, coordinated with plans 31/33 — see "Coordination with
    plans 31, 33 and 37" below): an empty/absent value means a plain
    hold with no recorded owner (today's `snapshot hold` behavior,
    unchanged); `user:<name>` is an operator hold set with an explicit
    owner (`snapshot hold --by user:<name>`, Step 5); `csi:<VolumeSnapshotContent
    uid>` is a hold set by plan 37's CSI driver via `snapshot.create{hold}`
    (plan 31 §9.2); `policy:<id>` is reserved for a future
    policy-owned hold, unused by this plan. `held_by` is **display and
    ownership metadata only** — it changes nothing about *whether* a
    held snapshot is protected: §Step 2 and §4.2's "held snapshots are
    invisible to the policy, never deleted automatically" already cover
    every value of `held_by` uniformly, including external owners like
    `csi:…`, because that rule keys on the boolean `held`, not on who
    set it.
- `LogRecord`: **append** at the end of the enum, as the postcard
  ordering comment requires:
  - `SnapCreate2 { id, path, name, root_hash, created_unix_ms, origin,
    policy_ino, creator }`
  - `SnapHold { id, held: bool, by: Option<String> }`

  Writers emit `SnapCreate2`. Replay handles both create variants.
- `SnapshotRecord` (the bucket JSON) gains the same fields with
  `#[serde(default)]`. `SNAPSHOT_RECORD_VERSION` stays 2, because the
  change is additive.
- `held`/`held_by` live only in the row. GC does not care about either.

### 0.5 Rename-safe listing

`covering()` also matches rows whose `SnapshotRoot.ino` equals the
looked-up directory's ino. A renamed policy root therefore still shows
its whole history under `.constellation/snapshot/`.

A path-prefix match whose ino differs (the directory was replaced) keeps
today's behavior. Test both cases.

## Step 1 — The policy language

`crates/meta/src/snapsched/policy.rs` (new). A hand-written parser in
the style of `prune/policy.rs`: pure, total, no environment, no clock,
no locale. Reuse `parse_duration`/`parse_size`/`fmt_*` from
`prune/policy.rs`, factored into a shared `policy_lex` module.

### What the user writes

```
5m:1d 1h:7d 1d:30d 1mo:1y
```

Each **tier** is `<every>:<keep>`: "take one every *every*, keep them
for *keep*". The whole policy reads left to right the way people say it
("every five minutes for a day, hourly for a week, …"). It fits in one
`setfattr`, and the web UI's row editor maps one row to one tier.

### Grammar

```
policy   := clause ((";" | WS) clause)*
clause   := tier | key "=" value | flag
tier     := interval ":" keep
interval := <n>"m"  (n divides 60)          5m 10m 15m 20m 30m
          | <n>"h"  (n divides 24)          1h 2h 3h 4h 6h 8h 12h
          | "1d" | "1w" | "1mo" | "1y"
          | <n>"s"  (n divides 60, n ≥ 10)  testing only; see below
keep     := duration | "*"                  "*" = forever
flag     := "paused"
```

`1m` is also accepted, since 1 divides 60.

**Units** follow plan 22: `m` is minutes, `mo` is months. `1M`, or a
bare `m` used where a month was plausible (`6m` as a *keep* when a
monthly tier exists), is a parse error that suggests both readings.
This is a delete-your-data feature, and minute-versus-month is the most
likely misreading.

**Constraints**, each with an error at a byte offset:
- at least one tier;
- no duplicate intervals;
- `keep ≥ every`;
- intervals are exactly the calendar-dividing set above. Otherwise the
  error is "`7m` does not divide an hour; use 5m, 10m or 15m". Buckets
  must tile the calendar, or a daily boundary would fall mid-bucket
  (L4).

**Settings:**

| Key | Default | Meaning |
|---|---|---|
| `tz=<IANA name>` | `UTC` | The timezone that bucket boundaries of 1h and coarser are aligned in. Validated against the bundled tzdb. |
| `day-start=HH:MM` | `00:00` | Shifts day/week/month/year boundaries, e.g. `02:00` for a nightly off-peak daily. Must be a multiple of the finest interval ≥ 1h (btrbk `preserve_hour_of_day`). |
| `week-start=mon\|sun` | `mon` | ISO weeks by default. |
| `last=<n>` | `1` | Always keep the `n` newest snapshots. Minimum 1: the newest is never auto-deleted (sanoid's count floor, L3). |
| `skip-empty=yes\|no` | `yes` | Do not create a snapshot if nothing under the root changed since the previous one (L8). |
| `budget=<size>` | none | Step 8, optional: a soft cap on this policy's `USED`. |

`paused` stops both creation and expiry and keeps everything. It is the
maintenance switch, and it avoids "remove the policy and re-add it"
(which would trip the grace period of Step 4.3).

**Seconds.** `s` intervals exist so the harness can run a real schedule
in minutes. They are part of the pure grammar, because validity must
not depend on the environment (plan 22's purity invariant). `snapshot
policy set` warns on any sub-minute tier, and the feature doc marks them
test-only.

**Canonical form.** Tiers sorted finest first, then settings in table
order, defaults omitted. For example:
`5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest`.
Round-trip `parse(to_string(p)) == p` is a property test.

**Timezones.** Use `jiff` with its bundled tzdb feature, so static musl
builds need no system zoneinfo. No tz crate is in the tree today;
`chrono` is transitive only.

### Examples, and what they mean

| Policy | Meaning | Steady-state count (upper bound) |
|---|---|---|
| `1h:1d 1d:7d` | hourly for a day, daily for a week | ≤ 31 |
| `15m:1d 1h:2d 1d:30d 1mo:1y` | a typical project tree | ≤ 96+48+30+12 = 186 |
| `5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest` | the user story, dense | ≤ 288+168+30+12+12 = 510 |
| `1d:14d 1mo:*; day-start=02:00` | nightly at 02:00, monthlies forever | 14 + months elapsed |
| `1d:7d; last=3; skip-empty=no` | exactly one per day, even when idle | ≤ 7 |

Because of union semantics the real count is lower than the upper bound
(the midnight snapshot counts as 5m, 1h and 1d at once). `policy check`
prints both the bound and a simulated figure.

**The UI does not have its own vocabulary.** The "Presets" menu in
Step 7 writes the explicit tiers above. The stored value is always the
explicit expression, so `getfattr` shows exactly what will happen.

### Why this language, and not the alternatives

- **Not per-tier counts (sanoid/restic `hourly=24`).** A count answers
  "how many", but users think "how long" ("keep a day"), and the two
  diverge exactly when snapshots are skipped. With `skip-empty`, idle
  periods have few populated buckets. A duration anchored on the newest
  snapshot (L3) keeps the "for a day" meaning and is still safe across
  outages.
- **Not btrbk/Ceph's bare `48h 14d 8w`.** Those strings encode *keep*
  only, with the cadence implied by the unit. That cannot say "every
  5 minutes for a day", which is the user's first example.
- **Not a separate schedule object plus a retention object (zrepl,
  TrueNAS tasks).** Two objects per directory reintroduce registry
  drift and TrueNAS's reuse footgun (L6).
- **Not a predicate or cron language.** It is a closed rule set, so the
  worst case of every policy is legible at a glance (plan 22's argument
  holds unchanged).

## Step 2 — Retention: the pure function

`crates/meta/src/snapsched/retention.rs` (new). No I/O and no clock.

```rust
pub struct SnapFacts { id: String, created_unix_ms: i64, held: bool, held_by: Option<String> }
pub struct Verdict { keep: bool, reasons: SmallVec<[Reason; 4]>, expires_at: Option<i64> }
pub enum Reason { Tier(Interval), Last(u32), Held(Option<String>), Grace }

pub fn evaluate(policy: &Policy, snaps: &[SnapFacts]) -> Vec<Verdict>;
```

`Reason::Held` carries `held_by` through unchanged — it is display
metadata for `KEPT BY`/the simulator (Step 7.4), not an input to the
rule below. Whether the owner is empty, `user:…` or `csi:…`, a held
candidate is excluded from the policy's candidate set the same way.

### The rule, stated once

Take the policy's **candidates**: its `origin=auto` snapshots of
`policy_ino` that are **not held**. Held snapshots are invisible to the
policy regardless of `held_by` — a `csi:…`-owned hold (plan 37) is
excluded exactly like a plain or `user:…`-owned one, so an external
owner's hold is never expired or pruned by any policy, with no
owner-specific logic. They never count and are never counted against
(PBS `protected`). Then:

1. **anchor** = the `created_unix_ms` of the newest candidate.
2. For each tier `I:K`, a candidate is its bucket's
   **representative** if it is the *oldest* candidate whose
   `created_unix_ms` falls in that `I`-bucket (aligned in `tz` and
   shifted by `day-start`).
3. A candidate is **kept** if either:
   - it is among the newest `last` candidates; or
   - for some tier it is the bucket representative **and**
     `bucket_start > bucket_start_I(anchor) − K`.

   `K = *` means the second condition is always true. Month and year
   subtraction is calendar arithmetic in `tz`. The comparison is strict,
   so `5m:1d` keeps exactly 288 buckets including the anchor's.
4. Every other candidate **expires**.

`reasons` lists every tier that keeps a snapshot (`5m·1h·1d`, L2 and
L7). `expires_at` is the time at which, *if snapshots keep arriving on
schedule*, the anchor passes the last tier window that keeps it. It is
a forecast for display, not an input.

### Properties, each a test

- **Idempotent:** `evaluate` over the survivors of `evaluate` expires
  nothing.
- **Monotone:** adding newer snapshots never revives an expired one.
  The same holds for deleting any non-representative.
- **Outage-safe:** removing all candidates newer than time T (the
  "scheduler was down" history) never expires anything that the full
  history keeps and that is older than T.
- **Nesting:** with `day-start=00:00`, the daily representative of a
  day is also that day's first hourly and first 5-minute
  representative.
- **Calendar edge cases:**
  - Europe/Budapest across both DST transitions (the 23h day has 23
    hourly buckets, the 25h day 25, and there is one daily each);
  - `Asia/Kolkata` (+05:30) hourly alignment;
  - `Pacific/Chatham` (+12:45);
  - 29 February and month-end (`1mo` buckets on the 31st);
  - an ISO week spanning a year boundary;
  - `day-start=02:00` on a DST-transition day, where 02:00 may not
    exist. Use the first instant ≥ 02:00, as `jiff`'s compatible
    disambiguation does. Document it.
- **Differential test** against a second, naive implementation that
  materializes every bucket. The naive version is slow and obviously
  correct. Run it over random policies and random snapshot histories.

### Simulation

`simulate(policy, existing: &[SnapFacts], horizon) -> Timeline` also
lives here. It appends synthetic future snapshots on the policy's
schedule up to `horizon` and evaluates at each step. It returns
per-snapshot verdicts plus synthetic ticks. The CLI (`policy check
--simulate`) and the web UI both call it. **The UI must never
reimplement retention in JavaScript.** One implementation, one
behavior.

## Step 3 — Binding, the scheduler, creation

### 3.1 Binding and validation

This mirrors plan 22 Step 2, without inheritance.

- The constant `SNAPSHOT_POLICY_XATTR = "user.constellation.snapshots"`
  goes next to `PRUNE_XATTR`. Root discovery generalizes
  `Meta::prune_roots` (`crates/meta/src/store/misc.rs:345`) over the
  `xattr_by_name` index.
- Validation at `setxattr`: parse, and on failure return `EINVAL`.
  Cache the reason in `SnapSchedStats::last_parse_error`, so it shows
  in `status`. Only directories may carry the xattr; anything else gets
  `EINVAL`.
- Scratch roots (`docs/reference/features/scratch-directories.md`) may
  not carry a policy. Snapshots of node-private content are meaningless.
- **Replay never validates.** The scheduler re-parses at run time and
  skips an unparseable root with `unparseable_roots += 1`. Fail-closed:
  it neither creates nor deletes.

### 3.2 The scheduler singleton

`crates/cli/src/snapsched.rs` (new), ticking every
`CONSTELLATION_SNAPSCHED_TICK_MS` (default 10 000) on every daemon.

- **A sticky singleton.** It uses the `_snapsched` `SingletonLease`,
  which it *keeps* across ticks and renews every tick. Leadership is
  continuous, and there is no acquire/release churn every 10 s. Other
  nodes' ticks see the lease held and return.
  - It must **renew before every destructive step**, and stop on a
    `Fenced` renewal. Plan 22's pruner never renews (a known gap); do
    not copy that.
  - Use its own lease, not `_prune`: a long prune walk must not delay a
    5-minute snapshot.
- **Refusal gates**, as the pruner's: `departed`, `epoch_frozen`,
  offline `ReadOnly`, read-only member, or replica lag above
  `CONSTELLATION_SNAPSCHED_MAX_LAG_S` (300). A refused tick creates
  nothing and deletes nothing. It counts `refused_*`.
- `CONSTELLATION_SNAPSCHED=0` disables the scheduler on that node. It
  is still eligible to be accounted from, just never to lead.

### 3.3 When to create, and names

On each tick, for each armed (non-`paused`, parseable) root:

- Let `f` be the finest interval and `b` the current `f`-bucket (in
  `tz`, shifted). The root is **due** if no candidate *or held* auto
  snapshot of this root has `created_unix_ms` inside `b`.
- **Catch-up, not backfill.** After downtime, create one snapshot now.
  It becomes the first of the current bucket. Never a burst for the
  missed buckets (anacron / `Persistent=true` behavior).
- **Name = the scheduled bucket**, in UTC basic ISO-8601:
  `auto-20260928T1405Z` (`auto-20260928T140510Z` for `s` tiers).
  - The name is UTC so DST never produces two identical local labels
    (zrepl, btrbk long-iso). Displays convert to `tz`.
  - It carries no tier label: a snapshot's tiers change over its life
    (L1, L6).
  - Because the name is derived from the bucket, a new leader retrying
    after failover hits `AlreadyExists` on the `snaps/` CAS. That
    counts as success. No bucket ever gets two scheduled snapshots.
  - `created_unix_ms` stays the truthful creation time. If the holder
    executed late and crossed into the next bucket, the next bucket
    counts as covered. That is cosmetic, and documented.
- Every due root goes into **one `SnapshotBatch`** (Step 0.1) per tick.
- **Cap:** a root with more than `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT`
  (5000) live auto snapshots stops creating, counts
  `capped_roots`, and surfaces an error in `status` and the UI. This
  guards against unbounded growth; with `last=` and `*` tiers it
  should not trigger.

### 3.4 Skip-empty

`skip_if_unchanged_since = <previous snapshot's root>`. The holder
decides after its publish (Step 0.1):

- **Unchanged:** the new root equals the previous root, **or** no key
  in `Tree::diff(prev, new)` maps into the subtree. Use the membership
  rule of Step 0.2, and ignore atime-only inode changes.
- Examine at most `CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS` (100 000)
  diff keys. Beyond that, treat the subtree as changed: it was busy
  anyway.
- A skipped bucket needs no snapshot, because the previous snapshot *is*
  that state. Retention is still correct, because windows anchor on the
  newest snapshot and `last ≥ 1` keeps it.

## Step 4 — Expiry

This is the only step that deletes snapshots, so it is reviewed on its
own.

### 4.1 One run

This runs after creation, in the same tick, at most once per
`CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` (60), per root:

1. Read the root's candidates from the replica, and evaluate
   (Step 2).
2. Apply the grace rule (4.3).
3. For each victim, oldest first, up to
   `CONSTELLATION_SNAPSCHED_MAX_DELETES` (500) per run:
   1. **renew the lease**;
   2. **re-read the row**. It must still exist, still be `auto`, still
      belong to this `policy_ino`, and still be not held;
   3. delete through the normal `SnapshotDelete` path (row, then
      `snaps/`).

   A `hold` that lands between evaluation and delete wins.
4. **Audit**: one object per run, `snapsched/journal/{ts}-{nonce}.json`,
   written with `Create`. It carries the root, the canonical policy,
   the created list, the skipped list, and the deleted list with each
   victim's `created` time and "no tier keeps it". It uses
   `append_journal`'s shape, so `fsck` and future tooling read one
   format.

### 4.2 What is never deleted automatically

- manual snapshots;
- held snapshots;
- auto snapshots whose `policy_ino` currently carries no policy, or an
  unparseable one: **orphaned**, listed as `auto (orphaned)`;
- anything while the root is `paused`;
- anything while a refusal gate is up.

**Removing the policy freezes its snapshots.** It never sweeps them.
`snapshot policy rm --expire` is the explicit, confirmed way to delete
a root's non-held auto snapshots together with its policy.

### 4.3 Grace after a policy change (the TrueNAS footgun, L6)

The scheduler keeps `snapsched/state.json` in the bucket. It is written
only by the lease holder, with a CAS on the ETag. It records, per
`policy_ino`, the canonical policy last seen and the time it was first
seen.

- A root whose policy changed less than `CONSTELLATION_SNAPSCHED_GRACE_S`
  (86 400) ago expires only what **both** the old and the new policy
  would expire: the intersection of the two verdicts. Creation follows
  the new policy immediately.
- So shortening `1d:1y` to `1d:7d` by `setfattr` deletes nothing for a
  day. `snapshot policy set` and the UI show the pending delta (Step 5)
  *before* writing.
- A root seen for the first time has no "old" policy. The intersection
  becomes "expire nothing" for the grace period. Adopting a directory
  that already holds orphaned auto snapshots is then safe.

### 4.4 Policy-root lifecycle

- **Directory deleted:** its auto snapshots are orphaned and kept, as
  today's snapshots of deleted paths are.
- **Directory renamed:** nothing changes. Identity is `policy_ino`, and
  listing is rename-safe (Step 0.5).
- **Assumption to verify:** inode numbers are never reused within a
  filesystem. If they can be, `policy_ino` must become
  `(ino, generation)`. Check the allocator before M3, and record the
  answer in PROGRESS.md.

## Step 5 — CLI

```
constellation snapshot policy set   <fs:path> '<expr>' [--yes] [--dry-run]
constellation snapshot policy show  <fs:path>
constellation snapshot policy ls    [<fs>]
constellation snapshot policy rm    <fs:path> [--expire] [--yes]
constellation snapshot policy check '<expr>' [--simulate <dur>] [--against <fs:path>]
constellation snapshot policy pause|resume <fs:path>

constellation snapshot ls     [<fs:path>] [-o cols] [-s col] [-p] [--json] [--auto|--manual|--orphaned]
constellation snapshot space  [<fs:path>] [--verify]
constellation snapshot delete <sel>... [--dry-run] [--yes]     # sel: path@name | path@a%b | path@auto-2026092*
constellation snapshot hold    <sel>... [--by <owner>]
constellation snapshot release <sel>...
constellation snapshot sched status | run [--dry-run]
```

**`snapshot hold --by <owner>`** sets `held_by` (Step 0.4); omitting
`--by` leaves it empty, matching today's behavior exactly. `--by` takes
a bare namespace-prefixed string (`user:attila`, `csi:<uid>`) and is
otherwise unvalidated — this plan does not enforce who may use which
prefix (that is a plan-33 authorization question: a `kind = "service"`
principal, plan 33's own mechanism for the CSI driver, is what actually
restricts who can call `snapshot.create{hold}`/`snapshot hold`, not
anything in this plan's hold storage).

**`policy set`:**
- Parses the expression, printing a caret on error (plan 22).
- Evaluates the new policy against the root's *real* snapshots.
- Prints the delta: "creates every 5m; will expire 312 snapshots
  (reclaim ≈ 41.3 GiB, returned after GC ≈ 2026-10-06); grace ends in
  24h".
- Asks for confirmation when it would expire anything (`--yes` skips
  the prompt, `--dry-run` never writes).
- Then writes the xattr. It warns on sub-minute tiers and on a
  steady-state bound over 2000.

**`snapshot ls`** becomes a table by default. `--json` keeps today's
JSON shape, extended with the new fields. Update
`crates/harness/src/client.rs` (`snapshot_count`, :413) and any test
that parses the old output. Default columns:

```
NAME                              CREATED (Europe/Budapest)  ORIGIN  USED    WRITTEN  REFER   KEPT BY     EXPIRES
/projects@monday-release          2026-09-21 18:02           manual  4.1G    6.0G     80.2G   —           never
/projects@auto-20260927T2200Z     2026-09-28 00:00           auto    210M    1.9G     81.0G   5m·1h·1d    in 29d
/projects@auto-20260928T1100Z     2026-09-28 13:00           auto    12M     55M      81.3G   5m·1h       in 6d 23h
/projects@auto-20260928T1155Z     2026-09-28 13:55  ⚑       auto    3M      9M       81.3G   held        never
/projects@pvc-a1b2c3d4             2026-09-28 09:00  ⚑       manual  1.1G    1.1G     81.1G   held: csi   never
USED/WRITTEN/REFER as of commit 88213 (4s ago) · logical bytes, pre-compression
```

The `KEPT BY` cell shows `held: csi`/`held: user`/plain `held` (the
`held_by` namespace prefix, or nothing when `held_by` is empty), so an
externally-owned hold reads differently from an operator's own —
`held: csi` is never confused for something a `snapshot release` on
this filesystem's own CLI put there casually.

- `-o` selects columns. Extra columns: `lsize`, `seq`, `id`, `creator`,
  `policy`.
- `-s used` sorts by a column. This is the "find the one eating the
  space" command.
- `-p` gives parsable exact bytes (ZFS `-p`).

**`snapshot space`** is the ZFS `usedby*` breakdown for a filesystem or
policy root:

```
live data (logical)                 1.02T
snapshots, total (usedbysnapshots)  96.4G   ← deleting every snapshot returns this
  unique to one snapshot            7.1G    ← Σ USED; not the total (see note)
  shared by ≥2 snapshots only       89.3G
shared between live and snapshots   —       (costs nothing extra)
awaiting GC                         12.0G   (freed snapshots; horizon 7d)
estimated physical (×0.61 compression, from last GC round)
```

A note line explains that per-snapshot `USED` values do not sum to the
total. This is the most common confusion in ZFS (L9).

**`snapshot delete --dry-run`** accepts several selectors and ranges.
`a%b` means every snapshot of that chain from `a` to `b` inclusive (ZFS
syntax). It prints `would reclaim ≈ X in N chunks (after GC)`. Without
`--dry-run`, deleting more than one snapshot prompts with the same
line. Deleting a held snapshot fails with "held; `snapshot release`
first", as `zfs hold` and PBS `protected` do.

## Step 6 — Space accounting

### 6.1 Definitions (all in logical, deduplicated chunk bytes)

For a snapshot S in chain C:

| Name | Definition | ZFS analogue | Additive? |
|---|---|---|---|
| `REFER` | Σ size of the distinct chunks S references | `referenced` | no |
| `LSIZE` | Σ `file_len` under S (apparent size, sparse-aware) | `logicalreferenced` | no |
| `WRITTEN` | Σ size of chunks in S that are not in C's previous surviving snapshot (for C's first snapshot, all of them) | `written` | **yes**, along a chain |
| `USED` | Σ size of chunks referenced by S and by **nothing else**: no other snapshot of any chain, and not the live tree | `used` (unique) | no |
| `reclaim(D)` | Σ size of chunks all of whose referrers are in the set D, and not live | `destroy -nv` | — |
| `usedbysnapshots` | `reclaim(all snapshots)` | same | — |

A chunk's **size** is its plaintext length, derived from the manifest
(`min(chunk_size, file_len − offset)`, sparse-aware). Spilled chunk
lists count at their encoded length. Physical size (after zstd and E2E)
is not per-chunk knowable without a HEAD, so physical figures are
**estimates**. They are logical bytes times the filesystem's
compression ratio, from the last GC round's LIST totals. The GC round
writes `{chunk_objects, physical_bytes, as_of}` into its journal
summary if it does not already. Label estimates with `≈` everywhere.

Deliberately excluded from per-snapshot numbers:
- **Metadata:** a snapshot pins a whole-FS mtree root, and its pack
  bytes are shared almost entirely with neighbors. Stretch goal:
  `snapshot space` shows *metadata pinned only by snapshots* from
  `mtree_gc`'s mark, as a filesystem-level figure.
- **`holds/`:** transient.
- **Clones:** they are live trees, so their chunks count as live.

### 6.2 The index: interval runs, not birth times (L10, L11)

Module `crates/cli/src/snapacct.rs`, plus node-local fjall keyspaces.
These are **not published**: derived, rebuildable, and versioned with a
header `{format, fs_uuid, accounted_seq}`.

```
snapacct_chunk   hash                 -> { size: u32, live: bool,
                                           runs: SmallVec<[(chain u32, first u32, last u32|OPEN, occ u32); 1]> }
snapacct_birth   chain | first | hash -> ()      // range queries by birth
snapacct_death   chain | last  | hash -> ()      // endpoint fix-ups on delete
snapacct_snap    chain | ord          -> { id, root, used, written, refer, lsize }
snapacct_meta    chain registry (chain id <-> dir ino), per-chain ordinal counter,
                 accounted commit seq
```

**Invariant.** Within a chain, a chunk is present in exactly the
snapshots whose ordinals fall in one of its runs. A run is **open**
while the chunk is still present in the chain's newest snapshot. `occ`
is the occurrence count at the chain head, for open runs only.

A chunk that disappears and later reappears (a revert, a restored file)
gets a **second run**. That is the "afterlife" ZFS's deadlists cannot
express. Cross-chain sharing (nested policy roots, dedup across trees)
is simply runs in more than one chain.

**Maintenance.** Every operation is O(what changed). The index never
scans itself.

- **Snapshot created in C.** `ChainWalk::step(prev, new)` gives deltas.
  Apply them to `occ`: a zero-to-positive crossing opens a run at the
  new ordinal; a positive-to-zero crossing closes the open run at the
  previous ordinal. Unchanged chunks need no work, because open runs
  extend implicitly. `WRITTEN[new]` = Σ runs opened at `new`.
  `REFER[new] = REFER[prev] + Σ size(opened) − Σ size(closed)`. `LSIZE`
  follows the same pattern with file lengths.
- **Snapshot deleted.** Scan `snapacct_birth` and `snapacct_death` at
  its ordinal:
  - a run `[k,k]` is removed;
  - a run `[k,j]` becomes `[next(k), j]`;
  - a run `[i,k]` becomes `[i, prev(k)]`, or re-opens if k was the head
    and prev(k) still has the chunk. The latter is impossible by
    construction; assert it.

  Then run `WRITTEN` fix-ups on `next(k)`. This is ZFS's deadlist merge,
  and it costs O(chunks born or dying at k).
- **Live-tree changes.** Every `CONSTELLATION_SNAPACCT_REFRESH_S` (60),
  `Tree::diff(accounted_root, current_commit_root)`. For every chunk in
  a changed manifest that **is in the index**, refresh `live` from
  `chunk_ref_exists` (`crates/meta/src/store/misc.rs:162`). Chunks
  referenced only by the live tree are never indexed; the live total
  comes from `UsageTracker`.
- **Contributions.** After any change to one chunk entry, recompute its
  *sole owner*: if it is not live and has exactly one run with
  `first == last`, that snapshot owns it. Move `size` from the old
  owner's `USED` to the new one's. Keep a filesystem-level counter for
  the `snapshot space` buckets the same way.
  So deleting S makes chunks that S shared with exactly one neighbor
  appear in that neighbor's `USED`. That is the documented ZFS
  behavior, and a test asserts it.
- **`reclaim(D)`** scans `snapacct_birth` over `[min ord, max ord]` of
  each chain in D. A chunk counts if every run is covered by D, it is
  not live, and it has no run in a chain outside D. The cost is
  O(chunks born in the range). This avoids ZFS's O(n²) sublist problem
  because runs carry their death ordinal directly.
- **Awaiting GC.** When a chunk loses its last run and is not live, it
  becomes a tombstone with `since_ms`. It is dropped once it is older
  than `gc.horizon + gc interval` (assumed collected), or revived if it
  is referenced again. Σ tombstones is the "awaiting GC" line.

### 6.3 Where it runs, how fresh, how verified

- **Every node may build its own index.** It is a pure function of
  replicated state, so no RPC is needed and every node's CLI and UI can
  answer locally. `CONSTELLATION_SNAPACCT=auto|on|off`, default `auto`.
  With `auto`, the index is built on the first size request, and
  maintained from then on while the node has a policy root or the web
  UI is enabled. With `on`, it is always maintained. With `off`, size
  columns show `-`.
- **Build cost:** one full walk of each chain's oldest snapshot, plus
  all diffs. That is the same as the Step 0.2 GC mark. The index is
  resumable (a cursor in `snapacct_meta`) and budgeted per tick.
  Queries during a build return `building (37%)` rather than wrong
  numbers.
- **Freshness:** every response carries `as_of_seq` and `as_of_ms`.
  Snapshot creation and deletion update the index on apply, so the
  snapshot list is fresh. Live-tree refresh lags by at most the refresh
  interval.
- **Size budget:** about 60 B per indexed chunk. 10 M snapshot-referenced
  chunks (≈ 40 TiB at 4 MiB) is about 600 MiB of local disk. Report the
  index footprint in `status`.
- **`snapshot space --verify`** is a brute-force oracle. It fully walks
  every snapshot, computes the exact sets, and diffs them against the
  index. It is the unit-test and harness oracle, and an operator's
  "do I trust these numbers" button.

### 6.4 Why not the alternatives

| Alternative | Why not |
|---|---|
| ZFS birth-time deadlists | Need "no afterlife" and one lineage. CAS violates both (L11). |
| Synchronous refcounts on the write path (btrfs qgroups) | Every commit pays for global accounting, and distros disable it for that reason (L10). The write path here is plan 30's latency-critical sequencer. |
| Per-snapshot full chunk sets on disk | O(snapshots × tree size), i.e. 500 × 1 M entries per policy. |
| Compute on demand only (ONTAP `compute-reclaimable`) | Seconds to minutes per query, capped to a few snapshots. It cannot back an `ls` column. |
| Publish one index in S3, maintained by a singleton | It becomes something to keep consistent, and every other node would need an RPC path to arbitrary `reclaim(D)` queries. Local and derived is simpler, and the build cost equals one GC mark. |

## Step 7 — Web UI

This is a new page, `crates/api/webui/snapshots.html`, linked from the
nav of `index.html`. Vanilla JS, no build step, hand-drawn SVG charts,
and the existing dark theme, per plan 11. The old snapshot panel in
`index.html` becomes a summary card linking to the page.

**The UI holds no retention or accounting logic.** It renders what the
API returns (Step 2 simulation, Step 6 numbers). It follows the
same-API rule of plan 11.

### 7.1 Space overview

This is a stacked horizontal bar across the top: live, shared
live+snapshots, snapshot-only shared, snapshot unique, awaiting GC,
each with a legend and exact bytes. It is the Ahrens slide (L9) as one
picture. Clicking a segment filters the table (for example "unique" →
sort by `USED`).

### 7.2 Policy roots

One card per policy root, showing:
- the path;
- the canonical expression, in monospace, so users learn the CLI form;
- the state: next snapshot in 3m 12s, last created, last error, and
  paused, capped or orphaned badges;
- the steady-state count;
- this policy's snapshot `USED` total;
- Edit, Pause and Remove buttons.

"Add policy" opens the editor with a directory picker. It reuses the
Files panel's `ReadDir` API.

### 7.3 Policy editor

The editor has these parts:

- **Tier rows.** `[every ▾ 5 min] keep for [1 ▾][day ▾]  ✕`, with `+ Add
  tier`. The interval dropdown offers only valid calendar-dividing
  values (Step 1), so an invalid interval cannot be chosen. "Forever"
  is a keep option.
- **Presets:**
  - *Light* `1h:1d 1d:7d`
  - *Standard* `15m:1d 1h:2d 1d:30d 1mo:1y`
  - *Dense* `5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y`

  Each preset fills the rows. It is not a stored name.
- **Advanced**, collapsed:
  - timezone, defaulting to the browser's zone, pre-selected but
    explicit;
  - day-start, week-start, `last`, skip-empty;
  - budget, once Step 8 exists.
- **The expression field**, two-way synced with the rows. Editing the
  text calls `SnapPolicyCheck` and shows the error with a caret, as the
  CLI does. Power users can paste.
- **Save** calls `SnapPolicySet`. If the delta expires anything, a
  confirm dialog quotes it: "expires 312 snapshots, returns ≈ 41 GiB
  after GC (≈ Oct 6); grace: nothing is deleted for 24 h". The API
  refuses an expiring change without `confirm_expiring: <n>` matching
  the previewed count. This is the server-side guard, so a stale UI
  cannot confirm a different delta.

### 7.4 Retention timeline (the simulator, L7)

It sits next to the editor and updates live (debounced
`SnapPolicySimulate`):

- **Axes:** x is time, from the oldest window a tier keeps to the
  simulation horizon (default 30 days forward, adjustable). There is
  one swim-lane row per tier, plus a "held/manual" row.
- **Existing snapshots** are solid ticks, colored by the tiers that
  keep them. A snapshot kept by several tiers shows in each lane.
  Snapshots the new policy would expire are hollow, struck-through red
  ticks. Held snapshots show a pin glyph, labeled with `held_by`'s
  namespace when set (`csi`, `user`, …) so an externally-owned hold
  (plan 37) is visibly distinct from an operator's own — the simulator
  never shows a `csi:`-held snapshot as if a policy or an operator could
  expire it, because it structurally cannot: held snapshots are outside
  the candidate set (see "The rule, stated once" above).
- **Future snapshots** are faded ticks, showing when they would be
  created and when they would expire.
- Hovering a tick shows its name, created time, reasons, `expires_at`
  and `USED`.
- Below it, a step chart of the **snapshot count over time** for the
  simulated policy. This makes "how many will I have" concrete.
- Colors use the `dataviz` skill's palette guidance: categorical by
  tier, validated for both themes, and never color alone (tier labels
  on lanes).

This is PBS's prune simulator, but against the user's **real**
snapshots rather than a synthetic schedule.

### 7.5 Snapshot table

- **Columns:** the CLI's columns, with sortable headers. `USED` and
  `WRITTEN` get inline bars, scaled to the table maximum. `KEPT BY`
  renders as chips. `EXPIRES` is relative, with the absolute time on
  hover. The hold toggle is a pin button. A snapshot with a non-empty,
  non-`user:` `held_by` (e.g. `csi:…`, plan 37) shows an "externally
  held" chip instead of a plain pin, and its hold toggle is disabled —
  releasing it here would fight whatever external system (the CSI
  driver) still believes it owns that hold, per plan 33's own
  "externally owned" UI note for this same case.
- **Filters:** origin (auto, manual, orphaned), policy root, and name
  search.
- **Multi-select** (checkboxes, shift-click for a range) shows a live
  footer: "12 selected · would reclaim ≈ 3.4 GiB (after GC)", from
  `SnapshotReclaim`. "Delete selected" confirms with the same figure.
  Held snapshots are not selectable for delete.
- **Written over time:** a bar chart of per-snapshot `WRITTEN` along a
  chain. It is additive, so it shows *when* the churn happened. This
  is the view that answers "which snapshot is huge, and why".
- `as_of` is shown in the footer. While the index builds, the size
  columns show a progress indicator, not zeros.

### 7.6 API

Additive `Request` variants, snake_case tagged, as established:

- `snap_policy_check {expr, against?: path, simulate_ms?}`
- `snap_policy_set {path, expr, confirm_expiring?: u32}`
- `snap_policy_remove {path, expire: bool, confirm_expiring?: u32}`
- `snap_policy_list`, `snap_policy_show {path}`
- `snap_policy_pause {path, paused}`
- `snap_policy_simulate {path?, expr, horizon_ms}` → `Timeline`
- `snapshot_hold {selectors, held, by: Option<String>}`
- `snapshot_reclaim {selectors}` → `{bytes, chunks, as_of_seq}`
- `snapshot_space {path?}` → the Step 5 breakdown
- `snapshot_delete_many {selectors, dry_run}`
- `snap_sched_status`, `snap_sched_run {dry_run}`

`SnapshotStatus` gains optional, `skip_serializing_if` fields: `origin`,
`policy_ino`, `held`, `held_by`, `kept_by`, `expires_unix_ms`, `used`,
`written`, `refer`, `lsize`, `as_of_seq`. `used`/`written`/`refer` are
exactly the numbers plan 37's CSI driver reads for `size_bytes` (see
"Coordination with plans 31, 33 and 37" below) — this plan adds no new
field for that purpose, only exposes these through whichever protocol
surface owns them at the time (plan 31 C5's "Plan 32 overlap" rule,
same section).

The plan-11 table-driven parity test must cover every new variant.
Extend the table, not a special case.

## Step 8 — Optional: space budget (`budget=`)

This lands last. It is the only rule that deletes *beyond* the tier
windows, so it is reviewed separately.

- `budget=500G` bounds this policy's snapshot-only bytes, meaning
  `reclaim(all of this root's candidates)` from the index.
- When the figure exceeds the budget, expire the **oldest
  non-representative-of-the-coarsest-tier** candidates first, then the
  oldest remaining, until under budget. This is snapper's
  `SPACE_LIMIT`, VSS `MaxSize` and the CCC snapshot cap. Never go below
  `last` or touch held snapshots.
- **Only on a fresh index:** `as_of` must be within
  `2 × CONSTELLATION_SNAPACCT_REFRESH_S`, and the index must not be
  building. Otherwise skip, and count `budget_stale`.
- An exceeded budget that cannot be met (everything left is `last` or
  held) is reported, not an error (plan 22's `lru` posture).
- The budget uses logical bytes. Document that the physical figure is
  roughly logical times the compression ratio.

## Step 9 — Observability

`SnapSchedStats` of `AtomicU64` on `StatusReport`, modeled on
`PruneStats`:

- `ticks`, `leader` (bool), `roots`, `paused_roots`, `unparseable_roots`,
  `capped_roots`, `orphaned_snapshots`;
- `created`, `skipped_empty`, `create_failed`, `expired`,
  `skipped_reverify`, `skipped_grace`, `budget_expired`, `budget_stale`;
- `refused_lag`, `refused_state`;
- `last_create_unix_ms`, `last_error`, `last_parse_error`.

`SnapAcctStats`: `building`, `build_progress_pct`, `indexed_chunks`,
`index_bytes`, `as_of_seq`, `refresh_ms_last`, `verify_mismatches`.

Export both on `/metrics` as `constellation_snapsched_*` and
`constellation_snapacct_*`. Add per-root gauges for snapshot count and
`USED` total. Label by root *ino*, never by path: paths are unbounded
cardinality and may be sensitive.

`create_failed` climbing while `expired` stays at 0 is the healthy
outage signature: anchoring on the newest snapshot freezes expiry.
`unparseable_roots`, `capped_roots` or `refused_*` above 0 means
snapshots are silently not being taken. Surface these in `status` and
as a UI banner, the same way plan 22 treats prune.

## Step 10 — Configuration and docs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_SNAPSCHED` | 1 | This node may lead the scheduler |
| `CONSTELLATION_SNAPSCHED_TICK_MS` | 10000 | Scheduler tick |
| `CONSTELLATION_SNAPSCHED_MAX_LAG_S` | 300 | Refuse to act from a stale replica |
| `CONSTELLATION_SNAPSCHED_GRACE_S` | 86400 | Intersection window after a policy change |
| `CONSTELLATION_SNAPSCHED_EXPIRE_EVERY_S` | 60 | Minimum spacing of expiry runs per root |
| `CONSTELLATION_SNAPSCHED_MAX_DELETES` | 500 | Expiry cap per run |
| `CONSTELLATION_SNAPSCHED_MAX_PER_ROOT` | 5000 | Stop creating beyond this many auto snapshots |
| `CONSTELLATION_SNAPSCHED_EMPTY_CHECK_KEYS` | 100000 | Diff budget for skip-empty |
| `CONSTELLATION_SNAPACCT` | auto | Build and maintain the accounting index |
| `CONSTELLATION_SNAPACCT_REFRESH_S` | 60 | Live-tree refresh cadence |
| `CONSTELLATION_GC_SNAP_WALK` | diff | Transitional: `full` restores the old GC walk |

Documentation:
- `docs/reference/configuration.md`: the table above.
- New `docs/reference/features/snapshot-policies.md`, following
  `docs/documentation-guide/feature-documentation-template.md`, with:
  - the grammar;
  - the tier table and examples;
  - the retention rule stated once;
  - "why the newest snapshot, not the clock";
  - holds and orphans;
  - grace;
  - the space columns, with the "`USED` does not sum" note;
  - "space returns after GC".
- Add a cross-link to `prune.md`: a pruned file stays in every auto
  snapshot that captured it until those expire. Prune alone will not
  free space under a dense policy, and `snapshot space` shows where the
  space is.
- PROGRESS.md: the DESIGN §13 staleness (tree blobs, lazy clones,
  delete `--force`/warning).

## Step 11 — Tests

**Unit, parser:**
- the round-trip property;
- every documented example;
- rejections with byte offsets: `7m:1d`, `5h:1d`, `1M:1y`, `1h:30m`
  (keep < every), a duplicate `1h`, `last=0`, an unknown `tz`,
  `day-start=02:30` with an `1h` finest tier, `paused` combined with an
  unknown key, an empty policy;
- fuzz `parse`: it never panics and always terminates.

**Unit, retention:** every Step 2 property, including the differential
test against the naive implementation, and the timezone and calendar
cases.

**Unit, snapwalk/snapacct:**
- **The model test.** Run seeded random histories of create, write,
  truncate, hardlink, rename (including directories across a policy
  root boundary), revert-to-old-content (afterlife), delete, snapshot,
  snapshot delete in the middle, hold, and nested roots. After every
  step, assert the index's `USED`/`WRITTEN`/`REFER`/`reclaim` equal
  the brute-force computation.
- `diff ⊇ full` for the GC protected set (Step 0.2).
- Deleting S moves S's shared-with-one-neighbor chunks into that
  neighbor's `USED`.

**Multi-node in-process** (the `shipper.rs` pattern):
- The scheduler on A while B holds the lease: snapshots are created,
  the lease never moves, and both replicas list the same set.
- Two nodes race for `_snapsched`: exactly one leads. Kill the leader,
  and the other takes over within one lease TTL and one tick, with no
  duplicate snapshot in any bucket (the name CAS).
- A `hold` issued between evaluation and delete survives.
- A policy shortened by `setfattr` deletes nothing inside the grace
  window. After it, it deletes exactly what the new policy says.
- Removing a policy orphans its snapshots; none are ever deleted.
- An unparseable xattr injected past the `setxattr` gate is inert on
  both nodes.
- `skip-empty`: an idle tree gets no new snapshots, and a single-byte
  write produces exactly one. An atime-only change produces none.

**Harness scenarios** (`crates/harness/src/scenarios.rs`), documented in
TESTING.md:

- **`snapsched`**
  - Setup: two nodes, the policy `10s:1m 1m:4m; last=2` on `/proj`, and
    a seeded writer that bumps a counter file.
  - Run about 6 minutes, then assert:
    - identical snapshot lists on both mounts;
    - the surviving set equals `retention::evaluate` over the creation
      list from the audit journal (the pure function as oracle);
    - each snapshot's counter file reads a value consistent with its
      `created` order;
    - a manual snapshot and a held auto snapshot survive.
  - `kill9` the leader mid-run: no duplicate buckets, and no gap longer
    than lease TTL plus one tick.
- **`snapsched-s3-outage`**
  - Cut S3 with toxiproxy for 90 s.
  - Assert: `create_failed` rises, **nothing expires**, and after
    restore exactly one catch-up snapshot appears, not a burst.
- **`snapacct`**
  - Run a seeded workload with snapshots, then:
    - `snapshot space --verify` reports 0 mismatches on both nodes;
    - `delete --dry-run a%b` lists a chunk set;
    - delete for real with `CONSTELLATION_GC_HORIZON_S` shortened, run
      GC, and assert that the chunks GC journaled as deleted equal the
      dry-run set exactly.
  - This is the end-to-end proof that the reclaim estimate is the truth.
- **`web-ui-smoke`** (extend it): `snap_policy_check`,
  `snap_policy_simulate`, `snapshot_space` and `snapshot_reclaim` over
  HTTP, all returning well-formed responses.

**Compliance and performance:**
- pjdfstest must be a FULL pass with the scheduler enabled and no
  policies. The default posture must be inert.
- With a `10s:1h` policy active on a 100k-file tree during an fio
  write run, sequencer write throughput must regress by no more than
  3%, and the root lease must never move. Paste the numbers.
- Paste GC round time on 300 snapshots of a 100k-file tree, `full`
  versus `diff`.

## Step 12 — Milestones and implementation order

| M | Scope | Deletes data? | Commit alone? |
|---|---|---|---|
| M0 | Step 0: holder-side `SnapshotBatch`, diff-based GC mark, orphan reconciliation, row/record extensions, rename-safe listing, table `snapshot ls` | no (GC changes are protective) | yes; valuable alone |
| M1 | Step 1 + Step 2: parser, retention, simulation, `policy check` | no | yes |
| M2 | Step 3.1: the xattr gate, root discovery, `policy set/show/ls/rm/pause`; nothing runs | no | yes |
| M3 | Step 3.2–3.4: scheduler, creation, skip-empty, stats, audit | no | yes |
| M4 | Step 4: expiry, grace, holds, orphans | **yes: review alone** | yes |
| M5 | Step 6: snapwalk-based index, sizes in `ls`, `space`, `delete --dry-run`, `--verify` | no | yes |
| M6 | Step 7: web UI and its API | via existing delete | yes |
| M7 | Step 8: `budget=` | **yes: review alone** | yes; optional |
| M8 | Step 9–11 remainder: metrics, docs, harness scenarios, perf numbers | no | — |

M0–M3 are safe to land with zero deletion risk. After M3 a policy
creates snapshots and never removes them, which is a usable (if
unbounded) state to test in.

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- the `snapsched` survivor set next to `retention::evaluate`'s output,
  pasted side by side;
- the `snapacct` dry-run chunk set versus the GC-journaled deletions
  (counts, and the first 5 hashes of each);
- the lease-never-moved assertion from Step 0.1, with the lease epoch
  before and after;
- GC round time `full` versus `diff`, and the throughput regression
  numbers;
- the answer to Step 4.4's inode-reuse question.

## Coordination with plans 31, 33 and 37

Added for this session; the rest of this plan is unchanged in structure
and step/milestone IDs. Plans 31 and 33 are being written in parallel in
this same session; plan 37 (Kubernetes CSI driver) is planned separately
and depends on both. None of the three own this plan's file; this
section only records where this plan's surface is a dependency of
theirs, so a reader of any of the four plans can find the others'
matching commitment.

- **Control protocol and web UI (plan 31 §C5's "Plan 32 overlap" rule).**
  This plan's Step 7.6 methods (`snap_policy_*`, `snapshot_*`) and Step 7's
  `webui/snapshots.html` page are written, as drafted above, as additive
  `crates/api` `Request`/`Response` variants and a vanilla-JS page — the
  only shape that exists to target while `crates/api` is still the
  control surface. Whichever of {this plan, plan 31's C5} lands second
  carries the other's work, exactly as plan 31 §C5 and plan 33's own
  "Coordination with plan 32" section already commit to:
  - If plan 31's C5 lands after this plan: C5 ports every method above
    into `constellation-control` as control-protocol methods (e.g.
    `snapshot.policy.check`, `snapshot.hold`) with the same semantics,
    before `crates/api` is deleted, and plan 33 builds Step 7's page as
    its Screen 5 instead of hosting the standalone HTML file.
  - If plan 31's C5 lands first: any future revision of this plan
    targets `constellation-control` directly and plan 33's SPA directly,
    skipping `crates/api`/`webui/snapshots.html` entirely.
  - Either way, this plan's methods are not lost and its "the UI holds
    no retention or accounting logic" rule (§7) carries over unchanged
    to whichever host renders Step 7's screen.
- **Space accounting for CSI `size_bytes` (plan 37).** Plan 37's CSI
  driver reports a snapshot's size via `NodeGetVolumeStats`/status
  fields backed by this plan's per-snapshot numbers (Step 6, exposed via
  `SnapshotStatus`/`snapshot_space` above). **It reads `REFER`**, not
  `USED`: `REFER` is what the snapshot presents when mounted or restored
  (ZFS's `referenced`), matching what a CSI consumer means by "how big is
  this volume", whereas `USED` (the chunks unique to that one snapshot)
  is a much smaller and unrelated number. This plan makes no code change
  for this — `refer` is already a Step 6.1 definition and a
  `SnapshotStatus` field — this note only settles which of the two
  numbers plan 37 is specified to read, since the two plans' briefs left
  it open ("REFER or USED; the plan picks one").
- **Holds and owner namespaces (plan 37).** Plan 37's `CreateSnapshot` is
  a `snapshot.create` call (plan 31 §9.2) carrying `hold:
  "csi:<VolumeSnapshotContent uid>"`, which this plan's Step 0.4/Step 2
  changes (`held_by`, above) store and treat as any other hold: excluded
  from every policy's candidate set, never expired or pruned, and shown
  as `held: csi` in the CLI and "externally held" in the UI (Steps 5,
  7.4, 7.5, above). Plan 37's `DeleteSnapshot` calls `snapshot release`
  (clearing `held`) then `snapshot.delete` — no new mechanism, the
  existing "deleting a held snapshot fails; release first" rule (Step 5)
  already means a CSI-held snapshot cannot be deleted out from under the
  driver by an unrelated `snapshot delete` call.
- **Name collisions.** Policy-created (`origin=auto`) snapshot names are
  always the scheduled-bucket form `auto-<UTC-ISO8601>` (Step 3.3).
  Plan 37's CSI-created snapshots are `origin=manual` (created through
  `snapshot.create`, the same path as a manual/CLI snapshot, never
  through the scheduler), so they are excluded from every policy's
  candidate set by `origin` alone regardless of what name the CSI driver
  picks — a policy can never expire, and never even considers, a
  CSI-created snapshot, even in the unlikely case its name happens to
  collide textually with an `auto-…` bucket name.

## Sources (prior-art survey, 2026-09-28)

- ZFS: zfs-auto-snapshot (github.com/zfsonlinux/zfs-auto-snapshot);
  sanoid (github.com/jimsalterjrs/sanoid, `sanoid.defaults.conf`,
  issue #791); zrepl pruning (zrepl.github.io/configuration/prune.html,
  issues #92, #400); TrueNAS periodic snapshot tasks
  (truenas.com/docs/scale/dataprotection/periodicsnapshottasks);
  zfsprops(7) `used`/`written`/`usedbysnapshots`;
  `zfs destroy -nv` (openzfs.github.io man zfs-destroy.8).
- Matt Ahrens, "How ZFS Snapshots Really Work", BSDCan 2019
  (papers.freebsd.org/2019/BSDCan/ahrens-How_ZFS_Snapshots_Really_Work):
  deadlists, the "no afterlife" invariant, O(n²) range reclaim.
- btrfs: qgroups (btrfs.readthedocs.io/en/latest/Qgroups.html); snapper
  (github.com/openSUSE/snapper, `client/cleanup.cc`); btrbk
  (digint.ch/btrbk/doc/btrbk.conf.5.html).
- Backup retention: restic forget
  (restic.readthedocs.io/en/latest/060_forget.html); borg prune
  (borgbackup.readthedocs.io, issue #7687); Proxmox Backup Server
  maintenance and prune simulator (pbs.proxmox.com/docs/maintenance.html,
  pbs.proxmox.com/docs/prune-simulator/); kopia (kopia.io, issue #3758).
- Cloud/enterprise: NetApp ONTAP snapshot policies and
  `volume snapshot compute-reclaimable` (docs.netapp.com); AWS EBS DLM
  (docs.aws.amazon.com/ebs/latest/userguide/snapshot-ami-policy.html);
  GCP snapshot schedules; CephFS snap-schedule
  (docs.ceph.com/en/latest/cephfs/snap-schedule/); Longhorn recurring
  jobs; backube snapscheduler; Synology advanced retention; Windows VSS
  shadow storage limits.
