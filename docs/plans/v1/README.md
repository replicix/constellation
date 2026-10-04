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
| 22 | `done/22-prune-policies.md` | done | prune policies as xattr expressions, run by a singleton pruner | 20 (`age`/`keep` rules independent) |
| 23 | `wip/23-remote-support-mode.md` | wip | compile-time-gated remote support mode: mount-armed `ro`/`rw` P2P sessions | 02 (independent of 19–22) |
| 24 | `done/24-e2e-keyring-master-key.md` | done | single-file keyring: one wrapped master key in meta.json, all keys derived; live `fs passwd`, no keyring.json | 10 |
| 25 | `done/25-checkpoint-strip-pending-upload.md` | done | strip node-local `pending_upload` from cluster checkpoints; heal poisoned joiners | 07 (08 committed in practice) |
| 26 | `done/26-metadata-plane-s3-efficiency.md` | done | proportional checkpoint cadence + inline prune, per-partition log retention (bug fix), holder skips self-tail, GET-next tailer with idle backoff, ranged checkpoint I/O, sticky leases with `wanted_by`, existence from `chunk_ref` | 25 |
| 27 | `done/27-merkle-packed-checkpoints.md` | done (superseded by 28) | checkpoint = packed Merkle tree + sidecar — goals delivered via plan 28's prolly-tree format (option B); plan as written not shipped | 26 |
| 28 | `done/28-s3-native-metadata-store.md` | done | S3-native metadata: prolly tree + packs + CAS commit chain replace the checkpoint; reachability GC + compaction | 27 |
| 29 | `done/29-fjall-metadata-engine.md` | done | fjall 3 local engine, partitions removed, key-delta publishing, concurrent ordered forwarding | 28 |
| 30 | `done/30-write-path-resilience-and-scale-out.md` | done | exactly-once forwarding, speculation log, session consistency + `cto=strict`, layered durability, flexible epochs, delegated sub-sequencers, strict locks | 29 |
| 31 | `wip/31-core-frontend-backend.md` | wip | core-isation: `constellation-engine` (one storage backend), `constellation-vfs` frontend contract (responder barriers, events, caps, policies), `constellation-platform` host layer, the control protocol, conformance kit + parity test architecture, engine profiles/lifecycle | 30 |
| 32 | `wip/32-snapshot-policies-and-space.md` | wip | automatic snapshot policies (`every:keep` tiers in an xattr, singleton scheduler, clock-free expiry, holds), ZFS-style per-snapshot USED/WRITTEN/REFER + reclaim dry-run, web UI policy editor/simulator; prerequisite fixes for snapshots at scale | 30 |
| 33 | `wip/33-control-plane-and-ui.md` | wip | control-plane security (per-user transports, roles, audit, token-auth headless web) + the cross-platform Tauri UI app exposing all management features; optional paired remote management | 31 (coordinates with 32) |
| 34 | `wip/34-macos-port.md` | wip | macOS port on the core: embedded NFSv4.1 frontend, `platform::macos`, macOS CI lanes in the parity framework, universal signed package | 31 (33 for the UI bundle) |
| 35 | `wip/35-windows-port.md` | wip | Windows port on the core: WinFsp frontend via own `winfsp-sys` FFI, `platform::windows`, named-pipe control transport, Windows CI lanes | 31 (33 for the UI bundle) |
| 36 | `wip/36-android-port.md` | wip | Android port on the core: in-app engine (mobile profile), SAF DocumentsProvider + proxy-fd frontend, media mirror folders, Tauri mobile UI, emulator CI | 31, 33 |
| 37 | `wip/37-kubernetes-csi.md` | wip | Kubernetes CSI driver: engine pod per (filesystem, k8s node) serving PVs as views, FUSE fd passing + session handover for zero-ENOTCONN upgrades, snapshots/clones/expansion, RWX, csi-sanity + kind e2e | 31, 33 (coordinates with 32) |
| 38 | `done/38-fuse-read-path-transport.md` | done | Linux FUSE read-path transport: FUSE-over-io_uring (adopt), its zero-copy extension (7.3+), passthrough for single-chunk read-only opens, `--cache-verify admit/always` trust model, runtime fallback ladder to today's `/dev/fuse` path | 31 (coordinates with 37, 32) |
| 39 | `wip/39-fsync-durability.md` | wip | fsync durability under S3 outages: NFS-`hard` waiting by default (classified transient/permanent S3 errors, capped-backoff retry, `FUSE_INTERRUPT` → `EINTR`), opt-in `--fsync-timeout` (`soft`) capped below the kernel's FUSE request timeout, `fsyncdir`, errseq-style per-descriptor `EIO` for lock-fence discards; `--fsync-mode local`'s wait deferred to the maintainer | 30, 31 (coordinates with 38's vendored fuser) |

"Committed in practice": the plan does not technically build on the
intermediate milestones, but the protocol is strictly sequential, so
every earlier plan is in the tree anyway.
