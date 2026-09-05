# Plan 00 — Verify and finish milestone M3.1 (lease-based write authority)

Read `docs/plans/v1/CONVENTIONS.md` first. This plan is special: the
working tree ALREADY CONTAINS the milestone's implementation,
uncommitted. Your job is to verify it end-to-end, fix what is broken,
and finish its documentation. Expect mostly-working code; do not
rewrite it wholesale.

## What is in the tree (context)

Milestone M3.1 implemented single-partition ("p0") lease-based write
authority per `docs/DESIGN.md` §4 "Leases (the authority mechanism)"
and §5 "Write Authority: the One Rule":

- `crates/store-s3/src/lease.rs` — `Lease` object (`{v, partition,
  holder, epoch, expires_unix_ms, released}`) + `LeaseStore` with CAS
  primitives (`get`/`try_create`/`try_swap`) over object_store
  `PutMode::Create` / `PutMode::Update{etag}`; `StoreError::CasConflict`;
  `LeaseMode::SingleWriter` fallback for backends without etag CAS
  (e.g. the local `file://` backend used by tests/smoke.sh).
- `crates/cli/src/lease.rs` — `LeaseKeeper` state machine (classify →
  commit with `TailedToHead` witness for takeover ordering, renew at
  half-TTL, idle release after `CONSTELLATION_LEASE_IDLE_RELEASE_MS`
  default 2000, deposition detection marking the node permanently
  `lost`), `LeaseView` lock-free snapshot for FUSE threads.
- `crates/cli/src/shipper.rs` — segments now stamped with the lease
  epoch; shipping refused without a valid lease; fencing on tail.
- `crates/cli/src/fusefs.rs` + `fusefs_ops.rs` — mutating ops gated on
  the lease (lazy acquire on first mutation, reads never acquire),
  bounded wait (~2x TTL) then EIO.
- `crates/cli/src/main.rs` — lease keeper wired into the sync task;
  TTL env `CONSTELLATION_LEASE_TTL_MS` (default 60000).
- `crates/api` — `StatusReport.lease: {held, holder, epoch,
  expires_in_ms, lost}`.
- `crates/harness` — `Client::pause()/resume()` (SIGSTOP/SIGCONT); new
  scenarios `lease-handover` and `lease-fencing`.
- `docs/ROADMAP.md` gained a phase-9 section (crash reporting) — that
  is intentional, leave it.

## Step 1 — Reconstruct the milestone's claims

Run `git status` and `git diff --stat` to see the full change set.
Read every changed/new file. Build a mental model of the lease flow:
mount → first mutation → acquire → ship (epoch-stamped) → renew →
idle-release → reacquire; and the failure path: SIGSTOP → TTL expiry →
peer takeover (tail-to-head first, epoch+1) → resume → renew CAS fails
→ deposed (lost=true, shipping refused, journal preserved).

## Step 2 — Gates (fix anything red)

Run the full gate list from CONVENTIONS.md, in order. Known
environment trap: `unset CARGO_TARGET_DIR` first. Points of specific
attention while verifying:

1. `cargo test --workspace`: the lease unit tests
   (`store-s3/src/lease.rs` bottom, `cli/src/lease.rs` if present, and
   the shipper's in-process two-node tests) must cover: CAS create /
   renew / expired takeover / epoch bump / release / CAS-conflict
   loser; acquire-before-ship; refuse-when-deposed; epoch in envelope.
   If any of these cases is untested, ADD the missing test.
2. `bash tests/smoke.sh` — exercises the `file://` backend, i.e. the
   SingleWriter fallback path. Must pass.
3. `target/release/harness run` — the FULL matrix (17 scenarios
   including `lease-handover` and `lease-fencing`; fio ones may SKIP).
   Every previously-green scenario must stay green: pay attention to
   `two-clients-shared` and `git-workflow`, which now depend on the
   idle-release handover being fast enough for their `eventually()`
   deadlines, and to `s3-outage`/`s3-flap`/`stress-ng-flap`, where the
   lease renewal path must survive S3 being unreachable (a holder that
   cannot renew during an outage must NOT mark itself lost as long as
   nobody else took the lease — verify the code distinguishes
   "transient store error" from "CAS conflict"; fix if it does not).
4. `two-clients-shared` must assert `conflicts == 0` on both nodes
   (the leaseless TouchSet path must be unreachable now). If the
   scenario does not assert it, add the assertion.
5. `docker compose --profile test run --rm compliance` — pjdfstest
   full pass. The lease gating sits on every mutation, so this is the
   latency regression canary. If pjdfstest times out or slows down
   dramatically, the acquire path is too hot — investigate (the
   `LeaseView` fast path should make gated checks ~ns when held).
6. Kill/crash interplay: run `kill9-remount` a few times; a SIGKILLed
   holder leaves an unreleased lease; on remount the same node must
   re-adopt its own live lease without waiting out the TTL
   (`Plan::Claim{needs_tail: false}` path). Verify this works.

## Step 3 — Documentation

`docs/PROGRESS.md` has no phase-3 section yet. Add one in the
established style: "Phase 3 — Leases + P2P fast path: **IN PROGRESS**",
an M3.1 table (item / state / where) covering the bullets from "What
is in the tree" above, the phase-3 exit-criteria checklist from
ROADMAP.md with M3.1's contribution checked (single-authority
invariant under simulated partitions — via `lease-fencing`; lease
transfer ~1 RTT is M3.3/P2P, leave unchecked), and a scope-limits
paragraph: idle-release handover latency (no P2P yet, so handover =
idle window + CAS, not 1 RTT), stranded-journal reintegration deferred
to phase 4, single partition p0 until M3.2.

`docs/TESTING.md`: add `lease-handover` and `lease-fencing` to the
scenario paragraph (mirror the existing prose style).

## Report

Per CONVENTIONS.md. Include: every red gate you found and the fix;
the full harness summary lines; pjdfstest tally; the wall-clock of
`lease-handover` (a proxy for handover latency to compare after M3.3).
