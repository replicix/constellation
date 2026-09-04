# Plan 08 — Phase 7: web UI over the control API

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–07
committed. Spec: `docs/DESIGN.md` §10 "Control Plane". Roadmap exit:
UI feature parity with the CLI, same API, verified by shared tests.

## Architecture

- Embedded UI: **axum + rust-embed**, served by the daemon, binding
  `127.0.0.1:<port>` (default 0 = disabled; `--web-ui <port>` or
  `CONSTELLATION_WEB_UI_PORT` enables). No auth for localhost (as
  designed; remote access via tunnel is out of scope — note it).
- The HTTP layer is a thin adapter over the SAME `api::Request` /
  `Response` types the unix socket speaks: `POST /api` with the JSON
  request body, plus convenience `GET /api/status`. One handler,
  zero duplicated logic. Add the axum server to `crates/api` (feature
  flag `web` so library consumers do not pull axum).
- Also expose Prometheus `GET /metrics` (plain text; counters from
  StatusReport — spool, cache, coop, lease; use a tiny hand-rolled
  encoder, no prometheus crate needed).
- Frontend: a single-page vanilla HTML+JS+CSS app (NO build step, no
  npm — rust-embed serves static files from `crates/api/webui/`).
  Poll `/api/status` every 2 s. Keep it clean and readable; dark
  theme; no framework.

## Pages/panels (parity targets, per DESIGN §10)

1. **Dashboard**: health, node id, uptime, backend, sync head/lag,
   spool backlog + last error (the §9 "outstanding blocks" view),
   epoch/designation state if active.
2. **Peers**: registry list, connected, RTT, last seen.
3. **Cache**: used/budget, chunk count, pinned bytes, hit rates,
   coop counters (peer hits, hedges), per-source EWMAs.
4. **Leases/partitions**: partition map with holder/epoch/expiry;
   force-release button (admin op — add `api::Request::ForceRelease
   {part}`, holder-side voluntary release; document that it is
   cooperative, not fencing).
5. **Files**: read-only browser over a new `api::Request::ReadDir
   {path}` + `Inspect {path}` (attrs, manifest summary, chunk count).
   Mutations (rename/delete/upload/download) are STRETCH — implement
   ReadDir/Inspect first, add mutations only if time permits; note
   whichever you skip.
6. **Snapshots**: list + create + delete snapshot.
7. **Ops**: doctor output, log tail (last N daemon log lines — keep
   an in-memory ring buffer subscriber in the daemon, expose via
   `api::Request::LogTail {lines}`).

## New API requests

`ReadDir`, `Inspect`, `ListSnapshots`, `SnapshotCreate/Delete`,
`ForceRelease`, `LogTail`, plus
whatever plans 03–07 already added — keep one enum, serde-tagged as
established, versioned by additive change only. The CLI gains
matching subcommands where DESIGN §10 lists them (`inspect`,
`cache ls|stat`, `log tail`) so CLI parity holds too.

## Shared tests (the exit criterion)

- Table-driven parity test in `crates/api`: for every `Request`
  variant, assert the unix socket and the HTTP adapter return
  identical responses against a mock `StatusSource`/handler. This is
  the "same API, verified by shared tests" requirement.
- Handler unit tests for the new requests against in-memory daemons
  (reuse the mount-less harness pieces where possible).
- Harness scenario `web-ui-smoke`: mount with the UI enabled; hit
  `/api/status`, `/metrics`, one ReadDir, one snapshot create/delete
  through HTTP; assert well-formed responses (serde round-trip) and
  that `/metrics` exposes the spool/cache/lease gauges. No browser
  automation — HTTP-level checks only.

## Gates + report

Per CONVENTIONS.md. List any parity gaps you left (files-panel
mutations etc.) explicitly in PROGRESS.md under "phase 7 scope
notes".
