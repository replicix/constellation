# Plan 11 — Phase 9: secure automated crash reporting pipeline

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plans 00–10
committed (this is post-v1 work). Spec: `docs/plans/v1/ROADMAP.md` phase 9 —
capture and centralize crashes without exposing customer PII or
proprietary strings in the client binary.

## Goal

End-to-end: a crash (Rust panic or native fault) in the daemon
produces a minidump, staged locally, uploaded on next start; the
server side symbolicates to exact file:line while the shipped binary
contains no plaintext symbol names.

## Step 1 — Capture

- Integrate `crash-handler` + `minidump-writer` crates: install the
  handler early in `main()`; on panic or native fault write
  `<state_dir>/crash/<ts>-<pid>.dmp` plus a small sidecar JSON
  `{version, git_sha, os, arch, uptime_s, mount_args_scrubbed}`.
  Scrub: NO paths from inside the mounted namespace, no env, no
  backend URL (bucket names can be sensitive) — only a hash of the fs
  UUID for correlation.
- Panics: also route `std::panic::set_hook` through tracing so the
  local log always has the message even when dump-writing fails.

## Step 2 — Binary hygiene

- Release profile: `debug = true` (full debuginfo generated) +
  `strip = "symbols"` in `[profile.release]` — the separated
  debuginfo stays server-side. Produce split debuginfo artifacts in
  the dist targets (`make dist-linux` gains
  `constellation-<version>.debug` via objcopy --only-keep-debug
  before strip).
- Verification (this is the acceptance criterion): a check script
  `tests/binary-hygiene.sh` that (a) runs `strings` on the release
  binary and asserts the absence of function-name markers (grep for a
  sentinel set of internal symbol substrings, e.g.
  `flush_inode`, `LeaseKeeper`, `apply_foreign`) and of local source
  paths (`/home/`, `crates/cli/src`), and (b) confirms the .debug
  file DOES contain them. Wire into `make dist-linux` and nightly CI.

## Step 3 — Collection backend

- Support a generic minidump upload endpoint, configured via
  `CONSTELLATION_CRASH_URL` (default: unset = crash reporting fully
  disabled; local dumps still written). Implement the Sentry envelope
  minidump format (plain multipart HTTP POST; no sentry SDK — keep
  deps light and PII control absolute) so GlitchTip/Sentry work as
  the backend, with `send_default_pii` semantics enforced client-side
  by construction (we simply never include PII fields).
- Docs: `docs/CRASH-REPORTING.md` — how to stand up GlitchTip via
  docker compose, upload the .debug file for symbolication
  (sentry-cli or the UI), and what the client sends (exhaustive field
  list).

## Step 4 — Staged upload

- On daemon start: scan `<state_dir>/crash/`, upload each dump
  (bounded retries, exponential backoff, never blocking mount
  readiness — background task), delete on 2xx, keep max N=10 dumps
  (oldest evicted) to bound disk.
- `constellation status` shows pending crash-dump count;
  `constellation crash ls|rm|upload` for manual control (control-API
  requests as usual).

## Step 5 — Tests

- Unit: sidecar scrubbing (feed a config with sensitive values,
  assert absence); staging eviction; envelope encoding golden test.
- Integration: a hidden `constellation debug crash [--native]`
  subcommand (panic, or deliberate SIGSEGV via raw pointer write in
  unsafe) so tests can produce real crashes. Harness scenario
  `crash-report`: mount, trigger the debug crash, restart the daemon
  pointing `CONSTELLATION_CRASH_URL` at a tiny in-scenario HTTP sink
  (hyper/axum one-shot listener in the harness), assert the dump
  arrives, decodes as a valid minidump (`minidump` crate reader), and
  the sidecar has no scrubbed fields.
- End-to-end acceptance (manual, documented in CRASH-REPORTING.md):
  GlitchTip shows exact file:line from the uploaded .debug — verify
  once and paste a screenshot/description into the PR... into the
  report (no commits from you).

## Gates + report

Per CONVENTIONS.md, plus `tests/binary-hygiene.sh` green on the dist
artifact. The ROADMAP acceptance criterion — server shows file:line,
binary has zero plaintext internal names — must be explicitly
demonstrated in the report.
