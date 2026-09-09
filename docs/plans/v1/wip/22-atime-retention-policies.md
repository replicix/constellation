# Plan 22 — Retention policies as xattr expressions

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §4 (metadata plane, log, partitions, leases),
§14 (bucket GC), `docs/reference/features/scratch-directories.md`
(the xattr-marked-root precedent). Depends on plan 20 (`LogRecord::Atime`)
for the atime-driven rules only; `age` and `keep` work with atime
disabled.

## Goal

Let an operator declare, **once, in the tree itself**, that entries under
a directory are removed when they go cold — and have every node agree on
exactly which entries those are.

- **The policy is the marker.** A directory carries its complete
  retention policy in `user.constellation.reap`. There is no policy
  registry, no name to resolve, no second object to keep in sync, and
  therefore no such thing as a dangling reference or an orphaned root.
  Deleting the policy is `setfattr -x`, and it takes effect the same way
  every other metadata change does.
- **Cluster state by construction.** Xattrs are replicated shared
  metadata. No node has its own retention rules and no node can be
  configured to disagree.
- **Convergent.** The reaping *decision* is made once, by one node, from
  replicated state, and published as ordinary `Unlink` mutations. Every
  replica converges on the same namespace. Divergent local deletion is
  the one outcome this plan must make impossible.
- **Lossy inputs are fine.** A dropped atime bump means an entry is
  judged on its previous atime — worst case it is reaped one cold
  interval early or kept one late. What is *not* acceptable is two nodes
  reaping different sets.
- **Namespace-only.** Reaping unlinks names. Reclaiming bytes stays the
  existing bucket GC's job (`crates/cli/src/gc.rs`), driven by the
  `deref` index (`crates/meta/src/sqlite.rs:446`).

Non-goals: per-user quotas, a general expression/predicate language,
reaping as a correctness input to anything, per-node retention
overrides, undo.

## Settled decisions

Taken from the design review; do not relitigate them.

- **Policies live in the xattr, in full.** No `LogRecord::SetPolicy`, no
  `policy` table, no names.
- **Action is hard unlink.** No trash tree, no quarantine, no
  tombstones. The name is gone from the live namespace.
- **Snapshots and clones keep their own references.** A reaped file
  stays fully readable in any snapshot or clone that captured it; its
  chunks are reclaimable only once the last reference — live *or*
  snapshot — is gone. That is already `Unlink` + `deref` semantics; the
  plan's job is to not break it and to prove it.
- **Execution is a singleton**, one node at a time under a `_reaper`
  cluster lease, mirroring `GcLease` (`crates/cli/src/gc.rs:73-136`).
- **Rules must not all require atime.** An `age` policy on mtime works
  with the plan-20 feature entirely off.
- **A bad expression fails the `setfattr`.** Validation happens at set
  time, so an unparseable policy never reaches the log.

## Step 1 — The policy language

`crates/meta/src/reap/policy.rs` (new): a hand-written parser over a
tiny, closed grammar. Pure, no I/O, no dependencies beyond `std`.

```
policy    := clause (";" clause)*
clause    := ident "(" args ")"        // a rule
           | ident                      // a flag:  off, dry
           | ident "=" value            // a setting: every=1h, max=5000
args      := (value | ident "=" value) ("," (value | ident "=" value))*
```

Whitespace is insignificant. Idents are `[a-z][a-z0-9-]*`. Values are
durations (`30s 15m 12h 60d 8w 1y`), sizes (`4K 10M 2G 1T`, powers of
1024), percentages (`85%`), integers, or globs in single or double
quotes.

### Rules

| Rule | Meaning | Needs atime |
|---|---|---|
| `age(<dur>)` | mtime older than `<dur>` | no |
| `unused(<dur>)` | atime older than `<dur>` | **yes** |
| `lru(high=<n>, low=<n>)` | usage crossed `high`: unlink coldest-by-atime *within this subtree* until it drops below `low`. Each watermark is a size or a percentage | **yes** |
| `keep(<n>)` | keep the newest `<n>` entries per directory, unlink the rest | no |

`unused` rather than `age(atime, …)`: the rule that needs the optional
feature has its own name, so a policy that works without plan 20 is
visually distinguishable from one that does not, and the error message
for "atime is off" can name the exact token.

`lru` takes named arguments only. Two bare numbers are inherently
ambiguous, and this is a delete-your-data feature — `lru(70,80)` is a
parse error that tells the operator to write
`lru(high=80%,low=70%)`.

`high` and `low` each take **either a size or a percentage**, which
settles "of what" with one rule:

> **A size is bytes of the marked subtree. A percentage is of the
> filesystem quota.**

`of=fs|subtree` overrides the measured quantity when the default is not
what you meant. Defaults: `of=fs` if either watermark is a percentage,
`of=subtree` otherwise. Under `of=subtree` a percentage is taken of
`high`, which must then be a size.

| Policy on `/scratch` | Measures | Behavior |
|---|---|---|
| `lru(high=500G, low=300G)` | `/scratch` bytes | keeps the subtree between 300 GB and 500 GB |
| `lru(high=500G, low=70%)` | `/scratch` bytes | same, evicting to 350 GB (70% of `high`) |
| `lru(high=85%, low=70%)` | whole-FS vs quota | when the *filesystem* passes 85% full, `/scratch` is the tree sacrificed until it is back under 70% |
| `lru(high=500G, low=300G, of=fs)` | whole-FS bytes | same global trigger without needing a quota set |

The percentage form is the "sacrifice zone" case: the trigger is global,
the victims are always local. Nothing outside the mark is ever unlinked,
and the filesystem may remain above `low` if the subtree runs out of
victims — a reported outcome, not an error.

Both watermarks are required. `low >= high` is a parse error when both
are sizes, or both percentages; when the units differ it is checked at
run time and the root is skipped with a counted error. `of=subtree`
with a percentage `high` is a parse error — there is nothing for it to
be a percentage of.

**A missing quota is not a parse error.** Quota is live-settable
(`constellation quota set`), so a policy's validity must not depend on
it — parse verdicts have to be identical on every node and at every
moment, which is the Step 5 purity invariant. A percentage-watermark
policy on a filesystem with no quota is simply **inert**: it never
triggers, it deletes nothing, and it is counted in `inert_roots` and
listed as inert by `constellation reap ls`. Set a quota later and it
starts working; clear the quota and it stops. No edit to the policy in
either direction.

### Per-rule filters (named args on any rule)

`min-size=`, `max-size=`, `only=<glob>`, `except=<glob>` (repeatable),
`uid=`, `gid=`, `min-age=<dur>` (a floor: never unlink anything younger,
whatever the rule says; defaults to 1h and cannot be set to 0).

### Flags and settings (clause position)

- `off` — this subtree is exempt. Cancels any inherited policy and must
  be the only clause.
- `dry` — evaluate and audit, never unlink. **A policy is `dry` unless
  it is not**: see Step 3.
- `every=<dur>` — minimum interval between runs for this root (default
  1h).
- `max=<n>` — cap on unlinks per run (default 10000).
- `rate=<n>/s` — unlink rate ceiling (default 50/s).

### Examples

```
age(90d)
unused(30d, except='*.keep', min-size=1M)
lru(high=85%, low=70%)
lru(high=500G, low=300G)
age(180d); every=6h; max=50000
keep(10, only='core.*')
off
```

Semantics of multiple rules in one policy: **union** — an entry is
reaped if any rule selects it. Order is irrelevant, which keeps the
policy a set of independent statements rather than a program.

`Policy::parse(&str) -> Result<Policy, PolicyError>` where `PolicyError`
carries a byte offset and a specific message ("expected `high=` or
`low=`", "duration `60` has no unit", "`lru` requires atime; mount with
`--atime relatime`"). `Policy::to_string()` emits a canonical
normalized form, and `parse(to_string(x)) == x` is a property test.

### Why not a predicate language

`mtime > 60d and size > 1M -> delete` is more expressive and strictly
worse here: it needs precedence rules, an evaluator in the metadata
plane, and it lets an operator write a policy whose blast radius is not
obvious from reading it. The closed rule set makes every policy's worst
case legible at a glance — the thing that matters most for a feature
that deletes data.

## Step 2 — Binding, inheritance and validation

```bash
setfattr -n user.constellation.reap -v 'age(90d)' /mnt/c/scratch/build
setfattr -n user.constellation.reap -v 'off'      /mnt/c/scratch/build/keep
setfattr -x user.constellation.reap               /mnt/c/scratch/build
```

- Constant `REAP_XATTR = "user.constellation.reap"` next to
  `SCRATCH_XATTR` (`crates/meta/src/sqlite.rs:23`).
- **Inheritance**: the nearest ancestor carrying the xattr wins,
  entirely. Policies do not merge, so the effective policy for any path
  is one expression from one directory. `off` is the opt-out.
- **The FS root may carry it**, which is how a whole-filesystem policy is
  expressed. No special case in the code.
- Only directories may carry it: `setfattr` on a non-directory returns
  `EINVAL`.
- Root discovery needs `CREATE INDEX IF NOT EXISTS xattr_by_name ON
  xattr (name)` — the table is `WITHOUT ROWID` keyed `(ino, name)`, so a
  name-only lookup is a full scan today.
- **Scratch roots are exempt.** Scratch entries are node-private and
  unreplicated (`docs/reference/features/scratch-directories.md`), so a
  singleton node deciding about them is inconsistent by construction.
  The walk does not descend into a scratch root; assert it in a test.

### Validation and failure

Validation is a pure function of the value, so every node reaches the
same verdict.

1. **At `setxattr`**, before the mutation is journaled: parse, and on
   failure return `EINVAL` — the `setfattr` call fails and nothing is
   written. Rejecting at the FUSE boundary is what keeps an unparseable
   policy out of the log entirely.
2. FUSE cannot carry the reason, so the reason goes where it can be
   read: `constellation reap set <path> '<expr>'` prints the message and
   a caret under the offending token, and `constellation reap check
   '<expr>'` validates without writing.
3. The last rejection is also cached in `ReapStats::last_parse_error`
   (expression, offset, message) and shown in `constellation status`,
   so an operator who hit a bare `EINVAL` from `setfattr` can find out
   why without retyping anything.
4. **Replay never validates.** A record that reached the log is applied
   verbatim, or a node with a different parser version would diverge
   from its peers. Instead the *reaper* re-parses at run time and, on
   failure, **skips the entire subtree** and counts it in
   `unparseable_roots`. Fail-closed: an unreadable policy deletes
   nothing, ever.

## Step 3 — Arming: `dry` by default

A policy that parses is not yet a policy that deletes.

- A newly written expression is **dry-run** unless it carries the
  explicit arming token `!` as its final clause — `age(90d); !`.
- Rationale: `setfattr` is a one-line command that is easy to run
  against the wrong path, and the difference between "I described a
  retention rule" and "I authorised deletion" should be visible in the
  stored value, not implied by its absence. `getfattr -R` over a tree
  then shows exactly which subtrees are armed.
- `constellation reap set --arm` appends the token; `constellation reap
  disarm <path>` removes it. Both are ordinary xattr writes.
- `dry` may be written explicitly for symmetry; `dry` and `!` together
  is a parse error.

## Step 4 — The reaper

`crates/cli/src/reap.rs` (new). One run:

1. **Acquire** the `_reaper` lease — generalize `GcLease`
   (`crates/cli/src/gc.rs:73`) into
   `SingletonLease::acquire(store, "_reaper", mode)` rather than
   copy-pasting the CAS dance. Held for the whole run, released on every
   path.
2. **Refuse to run** if any of: the node is `departed`, `epoch_frozen`,
   offline `ReadOnly`, a read-only member, or its replica trails the log
   tail by more than `CONSTELLATION_REAP_MAX_LAG_S` (default 300).
   Reaping from a stale replica is how you delete a file someone
   recreated. This is the single most important guard in the plan.
3. **Enumerate roots** via `xattr_by_name`, parse each expression, drop
   `off` and unparseable ones, and skip any root whose directory
   `ctime_ns` is newer than `now - CONSTELLATION_REAP_GRACE_S`
   (default 86400). The marked directory's own ctime is the "policy
   installed at" timestamp — `SetXattr` bumps it, so no extra state is
   needed, and any unrelated metadata change on the root only *delays*
   reaping, which is the safe direction.
4. **Walk** each root depth-first over `dentry`, skipping scratch roots
   and any nested directory carrying its own policy (that subtree gets
   its own pass). Evaluate filters, then rules, against the **replicated
   inode row**. Resumable via a per-root cursor in `kv`
   (`reap_cursor/<root_ino>`) under
   `CONSTELLATION_REAP_SCAN_BUDGET_MS` (default 5000), so a huge tree
   spans runs instead of monopolizing one.
5. **Select.** `age`/`unused`/`keep` select as they walk.
   `lru` selects against a bounded min-heap ordered by atime, scoped by
   its effective `of=`:
   - **`of=subtree` roots are independent.** Each has its own budget, so
     each gets its own heap and its own stop condition. Subtree size
     comes from the walk itself — the reaper is already visiting every
     entry, so the recursive size it needs is a running total, not a
     second pass. (It is *not* read from the maintained whole-FS
     counter, which has no per-directory breakdown; per-directory
     aggregates were explicitly deferred — see the `SetQuota` note in
     DESIGN.md §"logical size cap".)
   - **`of=fs` roots share one pool.** Candidates from every armed
     `of=fs` root go into a single heap and selection stops as soon as
     projected whole-FS usage is under the lowest `low` in play. One
     merged pool is the only way several roots aiming at the same
     filesystem avoid each independently over-deleting to hit the same
     target.
6. **Watermark input.** For `of=fs`, the maintained whole-FS usage
   counter `SqliteMeta::usage_bytes_files()`
   (`crates/meta/src/sqlite.rs:589`) — the same number quota and
   `statfs` use — compared against the quota
   (`crates/meta/src/sqlite.rs:605`) for percentage watermarks or
   against the raw byte figure for size ones. It is O(1), so a run that
   is under `high` skips the walk entirely, and a percentage-watermark
   root on a filesystem with **no quota set is inert**: counted in
   `inert_roots`, walked never, deleted from never. For `of=subtree`,
   the running subtree total from step 5, which needs no quota and no
   maintained aggregate.
7. **Execute.** Per victim, an ordinary `Unlink` mutation
   (`crates/meta/src/mutate.rs:61`) — local when this node holds the
   partition, forwarded otherwise, rate-limited to `rate`.
   `NotHolder`/`Busy`/timeout: skip, count, continue. Never retry into a
   lease acquisition; the next run re-selects the entry anyway.
8. **Re-verify at delete time.** Immediately before issuing each unlink,
   re-read the inode row and re-check the rule. A file read or written
   between selection and execution must survive. This is what makes a
   lossy atime stream safe: the losing case is one extra cold interval,
   never a delete against fresh state.
9. **Never reap**: any directory (reaping only ever removes non-
   directory entries, so a tree's shape survives its contents),
   anything with an open write handle on the executing node, and
   snapshot/clone synthetic inodes. Hardlinks have their own rule, see
   Step 4b.
10. **Audit.** One S3 journal entry per run via `append_journal`
    (`crates/store-s3/src/gc.rs:92`): root path, canonical policy
    expression, counts, bytes, duration, and the first N victim paths.
    A dry run writes the same entry with zero deletes — that is how an
    operator validates a policy before arming it.

## Step 4b — Hardlinks

Bytes belong to inodes; names do not. `recursive_size_conn`
(`crates/meta/src/sqlite.rs:570`) walks with `UNION` over inos, so a
hardlinked file counts **once** however many names it has, and `unlink`
only sheds its bytes from the usage counter when `nlink == 1`. The
reaper must account the same way, and the right behavior then differs by
rule:

- **`age` / `unused` / `keep` reap `nlink > 1` entries normally.**
  Removing one name of a multiply-linked file is exactly what
  `find … -delete` does, and it is safe by construction: the data
  survives through the remaining link, so the worst case is that a name
  the policy was told to remove is removed. No flag, no exception.
- **`lru` never selects an `nlink > 1` entry.** Unlinking it frees zero
  bytes, so it cannot move the number `lru` exists to move. Including
  such entries has no safe accounting: credit the bytes and the run
  stops early having freed nothing; do not credit them and the run keeps
  deleting until the candidate pool is empty, still having freed
  nothing. That second failure mode empties the tree, so `lru` skips
  them and counts `skipped_hardlink`.
- **Subtree totals dedup by ino**, matching `recursive_size`. Two names
  for one inode inside the same marked tree contribute their size once.
  When both names are selected by a namespace rule, the second unlink is
  the one that frees the bytes.
- **The audit entry reports `bytes_unlinked` and `bytes_freed`
  separately** — the gap between them is precisely the hardlinked
  fraction, and it is the number an operator needs when a reap run did
  not reclaim what they expected.

A hardlink reaching *out* of the marked tree only ever makes the reaper
more conservative: atime and mtime are per-inode, so reads through the
outside name keep the inside name looking warm. Under-deletion is the
failing direction, which is the correct one.

## Step 4c — Partitioned trees

A marked root is a namespace subtree; partitions are an orthogonal
sharding of that namespace, and a tree large enough to be worth reaping
is exactly one that has been split. Three consequences:

**The walk does not care.** Every node holds the full metadata replica,
so the depth-first walk over `dentry` crosses partition boundaries
without noticing, and the `reap_cursor/<root_ino>` cursor is a position
in the *namespace*, not in the partition map. A split or merge mid-walk
therefore cannot cause the walk to skip or revisit entries — which is
the reason to walk the namespace rather than iterate partitions.

**Execution fans out.** Victims are grouped by `partition_of`
(`crates/meta/src/sqlite.rs:883`) and issued as per-partition batches,
round-robin across partitions so one holder is never saturated while
others idle; the `rate` setting is the per-root ceiling, divided across
the partitions in play. The partition is **re-resolved at execution
time**, not at selection time: `PartSplit`/`PartMerge`
(`crates/meta/src/record.rs:133`) can move an inode between the two, and
a stale mapping just produces a `NotHolder` skip. Entries carrying an
in-flight cross-partition rename (`RenameXpartSrc`,
`crates/meta/src/record.rs:150`) are skipped for the run — the
re-verification in Step 4.8 already fails them, since the `(parent,
name)` the unlink names no longer resolves to the selected inode.

**Cold partitions must not be permanently unreapable.** This is a
correction to the inherited plan-20 posture. Atime could safely say
"never acquire a lease" because the cost of never publishing was one
stale timestamp. Retention cannot: a partition with no live holder is a
partition nobody is writing to, which is precisely the cold data a
retention policy exists to remove. "Never acquire" would make the
feature silently inert on the trees it is most for. So the reaper **may
acquire a lease for a partition that is currently unheld**, under three
restrictions:

- **Never preempt.** If a live holder exists, forward to it or skip.
  The reaper takes leases nobody wants, never one somebody has.
- **One at a time, released immediately** after that partition's batch
  drains. A reap run must not accumulate leases across a whole tree.
- **Never `touch()`** (`crates/cli/src/lease.rs:78`), so a
  reaper-acquired lease still looks idle to placement and to
  `idle_release_due`, and a real writer arriving mid-batch wins the
  partition on the next release rather than queueing behind background
  work.

**Reaping shrinks partitions**, so a large run is likely to make
neighbouring partitions merge-eligible. That is desirable, but the
reaper must not race it: skip any partition with an in-flight split or
merge for the current run and pick it up on the next one.

## Step 5 — The consistency rules

These are the invariants that make "one node deletes for everyone"
sound. Each gets a test.

- **Decide only from replicated state.** The reaper reads `inode`,
  `dentry`, `xattr` — never a node-local accumulator, never the
  in-memory pending atime map from plan 20, never local cache residency,
  never local pins (explicitly node-local, `crates/cli/src/pin.rs:3`). A
  node's own unflushed reads cannot change what it reaps.
- **Publish through the normal mutation path.** No direct SQL deletes,
  no reaper-specific record type. Every other node learns about a reap
  exactly as it learns about `rm`.
- **Lag gate before decisions** (Step 4.2) and re-verification before
  each delete (Step 4.8).
- **Parsing is pure and total.** Same bytes in, same policy out, on
  every node and every run. No environment, no locale, no clock in the
  parser.
- **No lease pinning.** A reap run must not make a partition look busy:
  never call `LeaseView::touch()` (`crates/cli/src/lease.rs:78`), and
  exclude reaper-issued unlinks from the idle-release backlog accounting
  the way plan 20 excludes `Atime`
  (`crates/cli/src/lease.rs:482-487`). Background work must not fight a
  real writer for a lease — including a lease the reaper itself took
  under Step 4c.

## Step 6 — Snapshots and clones

A reap is an unlink, so existing rules already give the required
behavior; the obligation is to prove it:

- The name disappears from the live tree.
- Any snapshot or clone that captured the inode still resolves and reads
  it.
- The `deref` index offers the chunks to bucket GC only when the last
  reference — including every snapshot's — is gone
  (`crates/meta/src/sqlite.rs:446`; see the existing
  `deref_tracks_snapshot_delete` at `:3705`).
- Reaping never descends into a snapshot or clone view.

## Step 7 — CLI and configuration

```
constellation reap ls                     # every marked root: path, policy, armed?, last run
constellation reap show <path>            # effective policy for a path + which ancestor set it
constellation reap set <path> '<expr>' [--arm]
constellation reap check '<expr>'         # parse only, no write, exit 2 on error
constellation reap disarm <path> | rm <path>
constellation reap run [<path>] [--dry-run]
constellation reap status
```

`reap set` and `reap check` print parse failures with the expression,
a caret under the offending token, and a suggested fix. `reap show` on
an unmarked path prints the inherited policy and the ancestor it came
from, or "no policy".

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_REAP` | 1 | master switch for the background reaper |
| `CONSTELLATION_REAP_INTERVAL_S` | 3600 | scheduler tick |
| `CONSTELLATION_REAP_GRACE_S` | 86400 | quiet period after a root's ctime changes |
| `CONSTELLATION_REAP_MAX_LAG_S` | 300 | replica-staleness refusal threshold |
| `CONSTELLATION_REAP_SCAN_BUDGET_MS` | 5000 | per-run walk budget before cursor save |
| `CONSTELLATION_REAP_FORWARD_TIMEOUT_MS` | 2000 | unlink forward timeout |

Document in `docs/reference/configuration.md` and a new
`docs/reference/features/retention.md` following the feature template:
the full grammar, a table of rules, the arming token, the fail-closed
rule, "atime is best-effort so an `unused`/`lru` policy can be off by
one cold interval", and the snapshot-retention semantics.

## Step 8 — Observability

`ReapStats` of `AtomicU64` on `StatusReport` (`crates/api/src/types.rs`),
modelled on the plan-20 `AtimeStats`: `runs`, `roots`, `armed_roots`,
`unparseable_roots`, `inert_roots`, `entries_examined`, `selected`, `deleted`,
`bytes_deleted`, `skipped_reverify`, `skipped_forward_err`,
`skipped_open`, `skipped_hardlink`, `skipped_repartition`,
`leases_acquired`, `bytes_freed`, `refused_lag`, `last_run_unix_ms`, plus the
non-atomic `last_parse_error` from Step 2.3. Surfaced in
`constellation status`, on the web UI, and in `/metrics`.

`skipped_reverify` climbing is the healthy signal that Step 4.8 is doing
its job. `unparseable_roots`, `inert_roots` or `refused_lag` above zero means
retention is silently not happening, which is the failure mode an
operator will otherwise notice only by running out of space.

## Step 9 — Tests

Unit (parser):
- Round-trip property: `parse(to_string(p)) == p` over generated
  policies.
- Every documented example parses to the expected structure.
- Rejections, each asserting the byte offset: `lru(70,80)`, `age(60)`,
  `age(60d) off`, `dry; !`, unknown rule, unknown filter key,
  `min-age=0`, `lru` with a missing watermark, `lru(low=…)` above
  `high` in the same unit, `of=subtree` with a percentage `high`,
  `unused` with atime off,
  duplicate rule kinds, unterminated glob quote.
- Fuzz `parse` over random bytes: never panics, always terminates.

Unit (evaluation):
- Rule decision tables per kind, including the `min-age` floor,
  `except` globs, `keep` ordering ties, and grace-period suppression.
- Nearest-ancestor resolution, `off` opt-out, nested roots.

Multi-node in-process (the pattern at the bottom of
`crates/cli/src/shipper.rs`):
- A holds `_reaper` and reaps under a marked root; B converges on the
  same namespace and never reaps anything itself.
- Two nodes race for `_reaper`; exactly one runs, and the loser's
  namespace still converges.
- B reads a file between A's selection and execution; re-verification
  drops it and the file survives on both replicas.
- A snapshot taken before the reap still reads the reaped file on both
  nodes; deleting that snapshot then releases the chunks to GC.
- A `high=` root that runs out of victims leaves the filesystem above
  `low` and reports it, rather than escaping the mark to find more.
- Two `of=subtree` roots evict to their own budgets independently; an
  `of=subtree` root and an `of=fs` root in the same tree do not
  double-count.
- A file with two names inside a marked tree: `age` removes both, the
  usage counter drops exactly once, and `lru` never selects it at all.
- A file linked from outside the marked tree is unlinked by `age` inside
  it, stays readable through the outside name, and frees no bytes.
- A percentage-watermark root on a filesystem with no quota is inert;
  setting a quota makes the same unedited policy start reaping, and
  clearing it makes it stop.
- A root whose xattr is garbage (written directly into the log, past the
  `setxattr` gate) is skipped on every node, and its subtree is intact.
- A run over a tree spanning several partitions unlinks from all of
  them, and both replicas converge.
- A partition with no holder is reaped: the reaper acquires the lease,
  releases it immediately after the batch, and `idle_release_due` is
  unaffected.
- A partition with a live holder is never preempted; its victims are
  forwarded instead.
- A split executed mid-run neither skips nor double-reaps entries, and
  stale partition mappings surface as counted `NotHolder` skips.
- A scratch subtree under a marked root is untouched.

Harness scenario `reap-retention`
(`crates/harness/src/scenarios.rs`): two nodes, seeded tree, an armed
`age` policy on mtime with a short TTL; `eventually()` asserts identical
listings on both mounts and the expected survivor set. Variant 1 cuts S3
with toxiproxy mid-run and asserts the run aborts cleanly, the lease is
released, and no partial divergence is observable. Variant 2 drives
`lru(high=…,low=…)` against a quota and asserts whole-FS usage returns
below the low watermark and stops there. Variant 3 drives
`lru(high=<size>,low=<size>)` on a subtree with no quota set at all and
asserts the subtree converges to its own budget while the rest of the
tree is untouched.

Compliance and perf:
- pjdfstest FULL pass with the reaper enabled and no marked roots — the
  default posture must be inert.
- A run over a 1M-entry tree stays within the scan budget and does not
  measurably regress steady-state read throughput; paste the numbers.

## Step 10 — Suggested implementation order

1. Parser + canonical form + the whole unit and fuzz suite. Pure code,
   nothing wired, independently reviewable.
2. `REAP_XATTR`, the `setxattr` validation gate, `xattr_by_name`,
   ancestor resolution, and `constellation reap ls|show|check|set`.
   Policies can be written and inspected; nothing runs.
3. `SingletonLease` extraction, the walk, and dry runs with the audit
   journal. Reviewable at zero deletion risk.
4. Arming, unlink execution, re-verification, rate limits, the Step 5
   lease guards.
5. `lru` (both `of=` scopes, the shared `of=fs` pool, inert handling),
   `keep`, metrics, docs, harness scenarios.

Steps 1–3 are safe to commit independently. Step 4 is the only one that
can lose data and should be reviewed on its own.

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- The two-node convergence assertion, with the actual listings pasted.
- The snapshot-survival assertion, with `deref` state before and after
  the snapshot is deleted.
- The fail-closed assertion for an unparseable policy on both nodes.
- Confirmation that a reap run acquires no partition lease it did not
  already hold and does not delay idle release (paste the counters).
- pjdfstest FULL pass with `CONSTELLATION_REAP=1` and no marked roots.
