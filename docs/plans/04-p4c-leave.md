# Plan 04c — Phase 4c: `constellation leave`

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–04
committed (leases, P2P, pin/offline, continuation epochs). Spec:
`docs/DESIGN.md` §1 / §8 (membership = bucket access, self-enrollment),
§5.3 (write-eligible roster is every non-RO registry record), §9
(enrollment is deferred or rejected when S3 is down; safety never
depends on failure detection). Heartbeats (`heartbeat/*`) are UX-only
and are **not** this plan.

This is the missing half of dynamic membership. Join already exists
(`claim_node_id` on first mount; peers poll `nodes/` every 5 s). Leave
does not: unmount flushes and releases leases but **leaves the
registry record in place forever**, so a retired laptop still counts
as write-eligible and can block continuation epochs forever. Unmount
without leave is the *temporary* departure (correct: the node may
return with S3 and must not be dropped from the roster by timeout).
`leave` is the *permanent* departure.

Do **not** auto-leave on unmount. Do **not** drop a node from the
roster because it missed a ping or a heartbeat. Either would let the
remaining component open an epoch while a still-enrolled writer that
merely lost P2P (or was unmounted) could take expired leases via S3.

## What `leave` must do

`constellation leave --state-dir ...` (live mount, via the control
API) removes **this** node from the cluster:

1. Refuse if an epoch promise is open locally (`epochs` table /
   `EpochManager::is_open`). The operator waits for S3 to return and
   the epoch to drain; shrinking the registry mid-epoch does not
   rewrite already-persisted member lists and would freeze survivors
   on the next liveness check.
2. Refuse if this node holds any offline designation (`online` first).
   Designations are non-stealable (DESIGN.md §5.2); leaving with one
   live would pin the subtree read-only for everyone else until
   someone deleted the object by hand.
3. Flush the journal and release every partition lease (same sequence
   as clean unmount: `shutdown_all` then `LeaseKeeper::release`).
   Refuse if the flush cannot complete (S3 down, lease lost with a
   stranded journal — tell the operator to `reintegrate` first).
4. DELETE `nodes/<id>.json`. This is the membership commit. Use a
   dedicated `constellation_store_s3::leave_node(store, node_id)` (or
   `delete_node_record`); there is no such helper today. Treat
   `NotFound` as success (idempotent).
5. Persist locally that this state dir has left (`kv_set("left", "1")`
   or clear `node_id` — pick one, document it). A later `mount` of the
   same state dir without `--rejoin` must fail with a clear error
   pointing at `leave --rejoin` / a fresh state dir. Do **not** reuse
   the retired numeric id: `claim_node_id` already allocates
   `max+1` and must keep doing so. Old log segments still carry the
   id; recycling it onto a different host would scramble origin and
   ino-prefix identity.
6. After a successful registry delete, the running daemon must stop
   writing: unmount (preferred) or flip to a terminal departed state
   that fails mutations with EIO. Do not stay mounted as a ghost
   writer whose id is gone from the allowlist.

`--force` on **this** node skips only the "nice" refusals that cannot
be true of a dead local daemon (e.g. designation check when the
operator already deleted the designation object). It must still
refuse an open epoch and an unflushed stranded journal — those are
safety, not courtesy.

## Admin removal of a *different* node

A laptop that will never come back and never ran `leave` is the
operator's problem, not a timeout's. Add:

```
constellation leave --state-dir <live-peer> --node-id <retired>
```

This is a control-API request executed by a **still-mounted** peer
with bucket write. It DELETEs `nodes/<retired>.json` only. It does
**not** flush that node's journal (it can't). Guards:

- Refuse if `<retired>` is the calling node (use the no-`--node-id`
  form).
- Refuse if `<retired>` currently holds a non-expired, non-released
  lease or an unreleased designation, unless `--force`. `--force`
  documents that any unflushed tail on the retired node becomes a
  stranded branch to `reintegrate` if that machine ever mounts again
  (it will re-enroll as a **new** id if its state dir was marked
  left; if it wasn't, remounting a node whose registry record was
  deleted under it is the `--rejoin` / fresh-state-dir case — detect
  `NotFound` on the expected `nodes/<id>.json` at mount and refuse
  rather than silently reclaiming the id).
- Do not require the retired node to be unreachable. The operator is
  asserting it is done; a live node that races will fail its next
  `publish_p2p` overwrite or see its record gone on the next refresh.
  If it is still running, it should treat a vanished own record as
  deposition: stop writing, log loudly, require `leave`/`rejoin`.

Roster refresh already runs every 5 s (`refresh_peers` →
`write_eligible_roster` → `epochs.set_roster`). After the DELETE,
remaining nodes must observe the smaller roster without a remount.
That is the whole point: two remaining writers can then form a
continuation epoch that would have been illegal while the retired
id was still listed.

## Rejoin

A left state dir is spent. Coming back is a **new** enrollment:

- `constellation mount ...` with a **new** `--state-dir` (or delete
  the old one) claims a fresh id — already works.
- Optional sugar, only if it stays small: `leave --rejoin` is **not**
  a flag on leave. Remount of a `left` state dir with an explicit
  `--rejoin` may clear the local `left`/`node_id` keys and call
  `claim_node_id` again (new id, same cache/DB replica). Skip this
  sugar if it threatens bootstrap/ino-prefix assumptions; a new
  state dir plus `bootstrap` is the conservative path and is
  already tested. Prefer the conservative path unless `--rejoin` is
  a few dozen lines.

`--read-only-member` on first mount of the new id still marks `ro`.

## Control API / CLI

- `Request::Leave { node_id: Option<u64>, force: bool }` processed by
  the running daemon. `node_id: None` means self.
- `Response::Ok` / `Error` as elsewhere. Self-leave should trigger
  daemon shutdown after the response is written (same idea as a
  clean unmount after `shutdown_all`).
- CLI: `constellation leave --state-dir DIR [--node-id N] [--force]`.
- `status` should show this node's registry id and whether its
  record is present; not a new dashboard, just enough that `leave`
  is observable (`StatusReport` already has `node_id` — add
  `enrolled: bool` if it is free).

`docs/DESIGN.md` §10's CLI highlight list does not mention `leave`.
Do **not** edit DESIGN.md (CONVENTIONS.md). Note the omission in
`docs/PROGRESS.md`.

## Tests

Unit (`store-s3::nodes`):

- `leave_node` deletes the record; `list_node_ids` / 
  `write_eligible_roster` shrink; `NotFound` is `Ok`.
- Leaving one of three writers drops it from the roster and leaves
  the other two.
- `claim_node_id` after a leave still returns `max+1` (retired ids
  are not recycled even though the object is gone — this means
  `claim_node_id` cannot use "lowest free" if that would reuse a
  hole. Today it uses `max(taken)+1`, which already does the right
  thing when the retired id was the maximum, and would **reuse** a
  hole if a middle id left. **Fix that:** never reuse an id. Persist
  a `nodes/NEXT` (or scan log/history — too heavy) **or** keep a
  tombstone object `nodes/<id>.json` with `{retired: true}` that
  `claim_node_id` treats as taken and `write_eligible_roster`
  skips. Tombstones are the safer default: `max+1` still works,
  holes stay reserved, LIST still sees the id, epochs ignore `ro`
  **and** `retired`. Implement tombstones, not silent DELETE, unless
  you also add an allocator object. Document the choice in the
  module docs.

  Preferred shape:

  ```
  nodes/<id>.json  { ..., "retired": true, "retired_unix": ... }
  ```

  `leave` overwrites (or CAS-updates) the record to `retired: true`
  rather than DELETE. `claim_node_id` continues to skip taken stems.
  `write_eligible_roster` omits `ro || retired`. `list_nodes` for
  P2P omits retired records (or includes them without `p2p_addr` so
  they are not dialed). A tombstone is still a parseable registry
  object — the roster fail-closed rule still applies.

Unit (`cli` / in-process):

- Self-leave refuses on open epoch, live designation, and unflushed
  stranded journal.
- Happy path: two in-memory nodes; B leaves (tombstone); A's roster
  becomes `[A]`; A can propose a single-node epoch (existing
  `component_covers_roster` already treats a one-node roster as
  covering itself).

Harness:

- `node-leave`: three clients on one bucket. A and B write; C runs
  `leave` (self). Assert C's registry record is retired; A and B's
  `status`/roster (via control API or a write under S3-cut after the
  5 s refresh) no longer include C. Cut S3 on A and B; they **must**
  be able to activate a continuation epoch (would have failed while
  C was enrolled and unreachable). Heal; model-verify; C staying
  unmounted is the point.
- `node-leave-blocks-epoch-until-left` (or a second half of the
  same scenario): with C merely **unmounted** (no `leave`), cut S3
  on A and B; they must **not** open an epoch. Then `leave --node-id
  C` from A; after roster refresh, they may. This is the proof that
  unmount ≠ leave.

## Scope limits (honest, leave them as limits)

- Heartbeats / `heartbeat/<node-id>` objects: not this plan. Status
  UX for "seen 12s ago" can wait for the web UI (plan 08) or a later
  polish pass.
- Automatic retirement of unreachable nodes: never. Operator `leave
  --node-id` is the tool.
- `umount` CLI: DESIGN.md lists it; the daemon still exits via
  fusermount. Out of scope unless you trip over it.
- Reclaiming cache bytes of a left node: local disk, operator's
  problem (`rm -rf state-dir`).

## Gates + report

Per CONVENTIONS.md. Add the milestone table to `docs/PROGRESS.md`
as **Phase 4c** under phase 4 (do not reopen 4b's "functionally
complete" verdict; this is additive). Update `docs/TESTING.md` with
the new scenario(s). Update this file's row in `docs/plans/README.md`
is already done; do not renumber plans 05–11.
