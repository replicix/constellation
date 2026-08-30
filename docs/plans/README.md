# Implementation plans

Sequenced, self-contained work orders for finishing Constellation
(see `../ROADMAP.md`). Each plan is executed by a coding model in a
fresh session; `CONVENTIONS.md` carries the shared rules (gates, code
style, harness checklist, reporting format) and is required reading
before any plan.

Execution protocol:

1. Hand the model exactly one plan file (plus CONVENTIONS.md).
2. The model implements until all gates are green, does NOT commit,
   and writes a report.
3. A coordinator (separate session) reviews the diff, re-runs the
   gates independently, and commits.
4. Only then does the next plan start — plans assume all
   predecessors are committed.

| # | file | milestone | depends on |
|---|---|---|---|
| 00 | `00-verify-m31-leases.md` | verify + finish M3.1 lease authority (code already in tree) | — |
| 01 | `01-m32-partitions.md` | partition split/merge, cross-partition rename | 00 |
| 02 | `02-m33-p2p.md` | iroh P2P: gossip invalidation, ~1 RTT lease handoff | 01 |
| 03 | `03-p4a-pin-offline.md` | pin/unpin, offline designation + delegation | 02 |
| 04 | `04-p4b-epochs-reintegration.md` | continuation epochs, stranded-branch reintegration | 03 |
| 04c | `04-p4c-leave.md` | `constellation leave`: permanent node departure, roster shrink | 04 |
| 05 | `05-p5-coop-cache.md` | cooperative cache: digests, peer serving, source selection | 02 (03–04c committed in practice) |
| 05a | `05a-p5a-write-staging.md` | bounded-memory write staging (disk-backed in-flight writes) + durable `pending_upload` | 05 |
| 05b | `05b-p5b-streaming-writeback.md` | eager/parallel chunk upload, dedup ladder, `--write-mode through/back` | 05a |
| 06 | `06-p6a-snapshots-clones.md` | snapshots, clones, subtree/snapshot mounts | 01 (05 committed in practice) |
| 07 | `07-p6-e2e.md` | E2E passphrase mode | 06 |
| 08 | `08-p7-web-ui.md` | embedded web UI + /metrics over the control API | 07 |
| 09 | `09-p8a-gc-fsck.md` | bucket GC (lease, deref, condemned handshake) + fsck --repair | 06 (07–08 committed in practice) |
| 10 | `10-p8b-hardening-packaging.md` | xfstests, perf gates, musl/macOS packaging, nightly matrix | 09 |
| 12 | `12-p8c-existence-bloom.md` | LIST-seeded S3 existence bloom + peer-digest upload hints | 10 |
| 13 | `13-p8d-snapshot-churn.md` | concurrent snapshot/clone churn oracle, audit trail, replay | 12 |
| 14 | `14-p8e-fallocate-holes.md` | fallocate, hole punch, sparse manifests (no zero-chunk) | 13 |
| 15 | `15-p8f-xattr.md` | POSIX xattr + virtual rsize/rcount | 14 |
| 11 | `11-p9-crash-reporting.md` | secure crash reporting pipeline | 10 |

"Committed in practice": the plan does not technically build on the
intermediate milestones, but the protocol is strictly sequential, so
every earlier plan is in the tree anyway.
