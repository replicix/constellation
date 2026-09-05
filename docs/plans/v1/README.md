# Implementation plans (v1)

Sequenced, self-contained work orders for Constellation (see
[`ROADMAP.md`](ROADMAP.md)). Each plan is executed by a coding model in a
fresh session; [`CONVENTIONS.md`](CONVENTIONS.md) carries the shared rules
(gates, code style, harness checklist, reporting format).

Layout:

- [`done/`](done/) — completed plans
- [`wip/`](wip/) — plans in progress or not yet started

Execution protocol:

1. Hand the model exactly one plan file (plus CONVENTIONS.md).
2. The model implements until all gates are green, does NOT commit,
   and writes a report.
3. A coordinator (separate session) reviews the diff, re-runs the
   gates independently, and commits.
4. Only then does the next plan start — plans assume all
   predecessors are committed.
5. When a plan finishes, move it from `wip/` to `done/`.

| # | file | status | milestone | depends on |
|---|---|---|---|---|
| 00 | `done/00-verify-m31-leases.md` | done | verify + finish M3.1 lease authority (code already in tree) | — |
| 01 | `done/01-m32-partitions.md` | done | partition split/merge, cross-partition rename | 00 |
| 02 | `done/02-m33-p2p.md` | done | iroh P2P: gossip invalidation, ~1 RTT lease handoff | 01 |
| 03 | `done/03-p4a-pin-offline.md` | done | pin/unpin, offline designation + delegation | 02 |
| 04 | `done/04-p4b-epochs-reintegration.md` | done | continuation epochs, stranded-branch reintegration | 03 |
| 04c | `done/04-p4c-leave.md` | done | `constellation leave`: permanent node departure, roster shrink | 04 |
| 05 | `done/05-p5-coop-cache.md` | done | cooperative cache: digests, peer serving, source selection | 02 (03–04c committed in practice) |
| 05a | `done/05a-p5a-write-staging.md` | done | bounded-memory write staging (disk-backed in-flight writes) + durable `pending_upload` | 05 |
| 05b | `done/05b-p5b-streaming-writeback.md` | done | eager/parallel chunk upload, dedup ladder, `--write-mode through/back` | 05a |
| 06 | `done/06-p6a-snapshots-clones.md` | done | snapshots, clones, subtree/snapshot mounts | 01 (05 committed in practice) |
| 07 | `done/07-p6-e2e.md` | done | E2E passphrase mode | 06 |
| 08 | `done/08-p7-web-ui.md` | done | embedded web UI + /metrics over the control API | 07 |
| 09 | `done/09-p8a-gc-fsck.md` | done | bucket GC (lease, deref, condemned handshake) + fsck --repair | 06 (07–08 committed in practice) |
| 10 | `done/10-p8b-hardening-packaging.md` | done | xfstests, perf gates, musl/macOS packaging, nightly matrix | 09 |
| 12 | `done/12-p8c-existence-bloom.md` | done | LIST-seeded S3 existence bloom + peer-digest upload hints | 10 |
| 13 | `done/13-p8d-snapshot-churn.md` | done | concurrent snapshot/clone churn oracle, audit trail, replay | 12 |
| 14 | `done/14-p8e-fallocate-holes.md` | done | fallocate, hole punch, sparse manifests (no zero-chunk) | 13 |
| 15 | `done/15-p8f-xattr.md` | done | POSIX xattr + virtual rsize/rcount | 14 |
| 16 | `done/16-p10a-adaptive-prefetch.md` | done | adaptive prefetch, streaming fetch, scan-ahead | 15 |
| 11 | `wip/11-p9-crash-reporting.md` | wip | secure crash reporting pipeline | 10 |

"Committed in practice": the plan does not technically build on the
intermediate milestones, but the protocol is strictly sequential, so
every earlier plan is in the tree anyway.
