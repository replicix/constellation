# Conventions for plan-executing models

You are executing one implementation plan for **Constellation**, a
distributed POSIX filesystem on S3, written in Rust, living at the repo
root (this file is `docs/plans/v1/CONVENTIONS.md`). Read this file fully
before starting any plan. Every plan file assumes you did.

## Ground rules

1. **Do NOT `git commit`, `git push`, or rewrite history.** Leave all
   changes in the working tree. A coordinator reviews and commits.
2. **Do not start a plan on a dirty tree** unless the plan says the
   in-tree changes are its subject (the verification plan does).
   If `git status` shows unrelated junk, stop and report.
3. Work until **all gates are green** (see below). Do not report
   success with failing or skipped-because-broken tests. If you are
   genuinely stuck after several distinct attempts, stop and write an
   honest report of what works, what does not, and your hypotheses.
4. Never weaken an existing test/scenario to make it pass unless the
   plan explicitly authorizes it. Fixing a test's *bug* is fine —
   loosening its assertion is not.
5. Do not edit `docs/explanation/DESIGN.md` (the spec). If you find a genuine
   contradiction between the spec and reality, implement what the plan
   says and record the contradiction in `docs/plans/v1/PROGRESS.md`.

## Orientation (read in this order, skim where obvious)

- `docs/explanation/DESIGN.md` — the system spec. Each plan names its sections.
- `docs/plans/v1/ROADMAP.md`, `docs/plans/v1/PROGRESS.md` — what exists, what is next.
- `docs/how-to-guides/development/TESTING.md` — the test lanes and the fault-injection harness.
- Crates: `fs-core` (chunking, manifests, disk cache), `store-s3`
  (S3 layout/chunk store/log store/nodes/lease), `meta` (fjall
  replica, log records, convergent replay), `api` (control API types +
  unix-socket server), `engine` (every storage algorithm: the authority
  driver and shipper, leases, coop, GC, snapshots, epochs, cluster
  locks, uploads, prefetch — plan 31 C3), `cli` (the `constellation`
  binary: CLI, daemon host, FUSE adapter, mount wiring), `net` (P2P — may be
  empty until phase 3), `harness` (fault-injection orchestrator:
  docker floci S3 + toxiproxy, model oracle, seeded workloads).

## Build & test commands

**IMPORTANT:** first thing in every shell: `unset CARGO_TARGET_DIR`.
If it is set (some sandboxes inject it), builds land outside
`target/` and the test scripts + harness cannot find the binaries.

```bash
cargo build --workspace                 # debug (used by tests/*.sh)
cargo build --release --workspace      # release (used by the harness)
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash tests/smoke.sh                     # e2e, local file backend
bash tests/integration.sh              # e2e against floci S3 (docker)
target/release/harness list            # scenario catalog
target/release/harness run             # FULL fault-injection matrix
target/release/harness run <name>...   # selected scenarios
docker compose --profile test run --rm compliance   # pjdfstest lane
```

The harness needs docker + fusermount3 on the host. fio/stress-ng
scenarios SKIP loudly if the tools are missing — that is acceptable on
a dev host (CI installs them), any FAILED is not.

## Definition of done (the gates)

Every plan ends with ALL of these green, run in this order:

1. `cargo fmt --all` produces no diff; clippy clean with `-D warnings`.
2. `cargo test --workspace` — zero failures.
3. `bash tests/smoke.sh` and `bash tests/integration.sh` — pass.
4. `target/release/harness run` — every scenario PASSED (fio/stress
   SKIP acceptable if binary absent).
5. `docker compose --profile test run --rm compliance` — pjdfstest
   stays a FULL pass (8798/8798, empty baseline). Constellation has no
   compliance exceptions; do not add any.
6. `docs/plans/v1/PROGRESS.md` updated: add your milestone's table rows
   (item / state / where) and exit-criteria checklist in the
   established style. `docs/how-to-guides/development/TESTING.md` updated if you added scenarios.

## Code style

- Rust 2024 edition, stable toolchain. `fuser` 0.18 with
  `default-features = false`; Linux mounts use host-sized concurrent
  event loops. tokio multithread runtime.
- FUSE callbacks run on synchronous worker threads: to reach async code use the
  existing channel patterns (see `SyncHandle` in `cli/src/fusefs.rs`
  and its `SyncRequest` in `engine/src/sync.rs`: unbounded mpsc +
  `blocking_recv` oneshot barriers). Never block the
  tokio runtime with sync waits.
- Comments explain non-obvious intent and trade-offs, never narrate
  code. Match the existing prose-heavy module-doc style (look at
  `engine/src/shipper.rs` or `store-s3/src/lease.rs`).
- Errors: `thiserror` enums in library crates, `anyhow` with `context`
  in the binary/harness. Refusals are the portable
  `constellation_types::Code` everywhere (`MetaError::code()`); it
  becomes a Linux errno at the FUSE boundary only (`reply_code()` in
  `cli/src/fusefs.rs`), and a real syscall's failure becomes a `Code`
  through `Code::from_io_error`. No `libc::E*` outside `crates/types`.
- New config knobs: env vars named `CONSTELLATION_*` with sane
  defaults; document them where they are read.
- Tests: unit tests co-located (`#[cfg(test)] mod tests`), using
  `object_store::memory::InMemory` for S3-shaped things and
  `Meta::open_in_memory()` for metadata. Cross-node logic gets
  in-process multi-node tests (see the pattern at the bottom of
  `engine/src/shipper.rs`). System-level behavior gets a harness scenario.

## Harness scenario checklist (when a plan asks for one)

- Add to `crates/harness/src/scenarios.rs` (`SCENARIOS` array: name,
  desc, `requires` for host binaries, run fn).
- Reuse `Client` (mount/unmount/kill9/pause/resume/control_status),
  `Model` (the oracle), `Workload` (seeded ops), `eventually()` for
  asynchronous cross-node assertions with deadlines.
- Fault injection via `env.s3_proxy()` toxics (latency, bandwidth,
  cut) — see existing scenarios for the idioms.
- Deterministic: seeded via the scenario's `seed` argument.
- Scenario must clean up (clients unmount in all paths; `Drop` on
  `Client` is the backstop).

## Reporting back

End with a concise report: files added/changed; design decisions where
the plan was ambiguous; exact gate results (paste the harness summary
lines and the pjdfstest tally); anything deliberately deferred with a
one-line justification each.
