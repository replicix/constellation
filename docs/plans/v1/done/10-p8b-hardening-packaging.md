# Plan 10 — Phase 8b: hardening, performance gates, packaging (v1)

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–09
committed. Spec: `docs/ROADMAP.md` phase 8 ("xfstests sweep,
performance regression gates, packaging (static musl builds, Linux +
macOS). Exit: nightly full matrix green; v1"). This is the closing
plan: breadth over novelty. Timebox each part; report honestly what
did not fit.

## Part A — xfstests sweep

- Add an xfstests lane to the containerized test framework
  (tests/docker + docker-compose service `xfstests`, mirroring the
  pjdfstest lane): clone a pinned xfstests-dev ref, build, run the
  `generic` group against a constellation mount with a scratch mount
  on a second FS prefix.
- Constellation is a network FS: many generic tests will be
  unsupportable (no direct block device, no freeze, etc.). Follow the
  pjdfstest-lane pattern: an exclusions file with a REASON per entry
  (`tests/xfstests-exclude.txt`), a baseline of known failures
  (`tests/xfstests-baseline.txt`), and a runner script that fails CI
  on regressions against the baseline, not on historical failures.
  Populate the baseline from the first honest run; triage the top
  failures (fix cheap real bugs now, baseline the rest with notes).
- Wire into the Makefile (`make xfstests`) and as a manually-
  triggered/nightly GitHub Actions job (it is too slow for PR
  builders — schedule `cron` nightly + `workflow_dispatch`).

## Part B — Performance regression gates

- `harness bench` already measures import / walk / cold-read. Add
  JSON output (`harness bench --json`) with the key rates.
- New `tests/perf-gate.sh`: run the bench against local floci,
  compare against `tests/perf-baseline.json` (commit one from the
  current code), fail if any metric regresses by more than 20%
  (tolerance in the baseline file). Add `make perf-gate` and a
  nightly CI job step next to xfstests.
- Extend the bench with two more probes while you are there:
  sequential large-file read MB/s (cold cache) and small-random-read
  IOPS (warm cache) — both already measurable with existing pieces.

## Part C — Packaging

- Static musl build for Linux: `x86_64-unknown-linux-musl` target;
  fix what breaks (rusqlite → bundled feature; ring/rustls are
  usually fine; document any cfg shims). `make dist-linux` producing
  `constellation-<version>-x86_64-linux-musl.tar.gz` with the binary
  + LICENSE + a minimal README.
- macOS: DO NOT attempt cross-compiling FUSE from Linux. Deliver:
  `make dist-macos` that works when run ON macOS (macfuse via fuser's
  macos support), guarded by an OS check, plus a CI job definition
  (GitHub Actions `macos-latest`) that builds and runs `cargo test
  --workspace` (no FUSE mounts in CI — mount-less tests only; harness
  and mount tests are Linux-lane concerns). If fuser/macfuse turns
  out to need real code changes, timebox to small fixes and otherwise
  document the gap precisely in PROGRESS.md.
- Version stamping: `--version` from git describe via build.rs
  (vergen or 10 lines by hand); release checklist in
  `docs/RELEASING.md` (tag → CI builds artifacts → attach).

## Part D — Nightly full matrix

One GitHub Actions workflow `nightly.yml` (cron + dispatch):
lint → unit → integration → compliance (pjdfstest) → harness full
matrix → xfstests → perf-gate → (macos build+test). Each job uploads
its logs as artifacts. A final job posts a summary table. PR CI
(existing ci.yml) stays fast and unchanged.

## Part E — v1 sweep

- `cargo audit` (add to nightly; fix or acknowledge advisories).
- README.md at the repo root: what it is, quick start (create/mount/
  status), pointers into docs/. Honest status paragraph.
- PROGRESS.md: final pass — phase table 1–8 all checked with
  pointers, deferred list accurate (crash-reporting phase 9 from
  ROADMAP stays future work).
- Tag nothing; the coordinator cuts v1 after review.

## Gates + report

Per CONVENTIONS.md, PLUS: xfstests baseline committed with triage
notes; perf baseline committed; `make dist-linux` artifact builds and
`./constellation --version` runs on a musl-less host (static — check
with `ldd`). Report the xfstests pass/fail/excluded tally and the
bench numbers table.
