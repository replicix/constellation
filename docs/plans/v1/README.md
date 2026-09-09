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
| 05 | `done/05-p4c-leave.md` | done | `constellation leave`: permanent node departure, roster shrink | 04 |
| 06 | `done/06-p5-coop-cache.md` | done | cooperative cache: digests, peer serving, source selection | 02 (03–05 committed in practice) |
| 07 | `done/07-p5a-write-staging.md` | done | bounded-memory write staging (disk-backed in-flight writes) + durable `pending_upload` | 06 |
| 08 | `done/08-p5b-streaming-writeback.md` | done | eager/parallel chunk upload, dedup ladder, `--write-mode through/back` | 07 |
| 09 | `done/09-p6a-snapshots-clones.md` | done | snapshots, clones, subtree/snapshot mounts | 01 (06 committed in practice) |
| 10 | `done/10-p6-e2e.md` | done | E2E passphrase mode | 09 |
| 11 | `done/11-p7-web-ui.md` | done | embedded web UI + /metrics over the control API | 10 |
| 12 | `done/12-p8a-gc-fsck.md` | done | bucket GC (lease, deref, condemned handshake) + fsck --repair | 09 (10–11 committed in practice) |
| 13 | `done/13-p8b-hardening-packaging.md` | done | xfstests, perf gates, musl/macOS packaging, nightly matrix | 12 |
| 14 | `done/14-p8c-existence-bloom.md` | done | LIST-seeded S3 existence bloom + peer-digest upload hints | 13 |
| 15 | `done/15-p8d-snapshot-churn.md` | done | concurrent snapshot/clone churn oracle, audit trail, replay | 14 |
| 16 | `done/16-p8e-fallocate-holes.md` | done | fallocate, hole punch, sparse manifests (no zero-chunk) | 15 |
| 17 | `done/17-p8f-xattr.md` | done | POSIX xattr + virtual rsize/rcount | 16 |
| 18 | `done/18-p10a-adaptive-prefetch.md` | done | adaptive prefetch, streaming fetch, scan-ahead | 17 |
| 19 | `wip/19-p9-crash-reporting.md` | wip | secure crash reporting pipeline | 13 |
| 20 | `done/20-read-atime.md` | done | optional read-time atime (batched, eventually consistent) | 18 |
| 21 | `done/21-fs-registry-and-daemon.md` | done | named filesystems, shared mount daemon, local registry | 18 |
| 22 | `wip/22-atime-retention-policies.md` | wip | retention policies as xattr expressions, reaped by a singleton reaper | 20 (`age`/`keep` rules independent) |
| 23 | `wip/23-remote-support-mode.md` | wip | compile-time-gated remote support mode: mount-armed `ro`/`rw` P2P sessions | 02 (independent of 19–22) |

"Committed in practice": the plan does not technically build on the
intermediate milestones, but the protocol is strictly sequential, so
every earlier plan is in the tree anyway.
