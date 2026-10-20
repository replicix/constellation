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
| 31 | `done/31-core-frontend-backend.md` | done | core-isation: `constellation-engine` (one storage backend), `constellation-vfs` frontend contract (responder barriers, events, caps, policies), `constellation-platform` host layer, the control protocol, conformance kit + parity test architecture, engine profiles/lifecycle | 30 |
| 32 | `done/32-snapshot-policies-and-space.md` | done | automatic snapshot policies (`every:keep` tiers in an xattr, singleton scheduler, clock-free expiry, holds), ZFS-style per-snapshot USED/WRITTEN/REFER + reclaim dry-run, web UI policy editor/simulator; prerequisite fixes for snapshots at scale | 30 |
| 33 | `wip/33-control-plane-and-ui.md` | wip | control-plane security (per-user transports, roles, audit, token-auth headless web) + the cross-platform Tauri UI app exposing all management features; optional paired remote management | 31 (coordinates with 32) |
| 34 | `wip/34-macos-port.md` | wip | macOS port on the core: embedded NFSv4.1 frontend, `platform::macos`, macOS CI lanes in the parity framework, universal signed package | 31 (33 for the UI bundle) |
| 35 | `wip/35-windows-port.md` | wip | Windows port on the core: WinFsp frontend via own `winfsp-sys` FFI, `platform::windows`, named-pipe control transport, Windows CI lanes | 31 (33 for the UI bundle) |
| 36 | `wip/36-android-port.md` | wip | Android port on the core: in-app engine (mobile profile), SAF DocumentsProvider + proxy-fd frontend, media mirror folders, Tauri mobile UI, emulator CI | 31, 33 |
| 37 | `done/37-kubernetes-csi.md` | done | Kubernetes CSI driver: engine pod per (filesystem, k8s node) serving PVs as views, FUSE fd passing + session handover for zero-ENOTCONN upgrades, snapshots/clones/expansion, RWX, csi-sanity + kind e2e | 31, 33 (coordinates with 32) |
| 38 | `done/38-fuse-read-path-transport.md` | done | Linux FUSE read-path transport: FUSE-over-io_uring (adopt), its zero-copy extension (7.3+), passthrough for single-chunk read-only opens, `--cache-verify admit/always` trust model, runtime fallback ladder to today's `/dev/fuse` path | 31 (coordinates with 37, 32) |
| 39 | `wip/39-fsync-durability.md` | wip | fsync durability under S3 outages: NFS-`hard` waiting by default (classified transient/permanent S3 errors, capped-backoff retry, `FUSE_INTERRUPT` → `EINTR`), opt-in `--fsync-timeout` (`soft`) capped below the kernel's FUSE request timeout, `fsyncdir`, errseq-style per-descriptor `EIO` for lock-fence discards; `--fsync-mode local`'s wait deferred to the maintainer | 30, 31 (coordinates with 38's vendored fuser) |
| 40 | `wip/40-readdirplus.md` | wip | FUSE `READDIRPLUS`: plus entries carrying a lookup's attributes (§P6 dentry copy under the write shard, pending-size overlay) after `getattr`'s session wait, name-only under strict, `auto`/`always`/`off` knob, fuser fork name-only entries; name-ordered O(page) listings with keyed stream-independent cookies and a hint cache keyed by directory and cookie, pinned while a directory is open so `fts`-style deletion, `seekdir` and handover stay exact (plain `READDIR` too); builds on the separately landed kernel lookup counts; benchmarks with a no-regression bar | 31 (30, 38, 39 committed in practice; the kernel lookup-count integrity fix) |
| 41 | `wip/41-lock-renewals-through-disk-stalls.md` | wip | cluster-lock renewals through local disk stalls: a renewal lane beside the authority core that extends grant windows from the in-memory lock tables under the lease mirror (`LeaseView`, extended) the driver publishes each step, and never grants, refuses or drops; committed scope: own grants of a `Local` root and grants held at any healthy owner (finding 08), atomic lock-table primitives for two writers, no store I/O or logging under the tables' mutex, lock-model and simulation (`StallCore`) work, a store-stall fault point, harness scenarios, real-device stall acceptance; peers' renewals at a stalled owner (opt-in horizon lead) and the fast-tenure lease probe are a maintainer decision (recommended follow-up); verifies the separately landed L0 integrity fix (fast-tenure lock and delegation renewals need fresh S3 liveness) | 30 (31, 39, the finding-14 fixes and the L0 integrity fix committed in practice) |
| 42 | `wip/42-kernel-race-workarounds.md` | wip | correct on today's Linux kernels against three FUSE client races (stale page after a write-through write; cached attributes marked valid before they are stored; a pre-change reply cancelling a directory's invalidation): `FOPEN_KEEP_CACHE` only when a replica content stamp, the notification pipeline and the frontend's own evidence say the pages are current (a suspect inode drops, as today), with engine invalidations for sessions retired uncommitted, commits through the node's other views and stranding rollbacks; a write-race repair that arms every known page drop, records reads, and before answering the first write that completes after a drop invalidates exactly the pages reads refetched, on a repair lane whose bound runs only while the write is alone on the inode (no time-bounded notice record; the candidate write follows from the kernel's inode-lock serialization); an attribute-only notification for the changed parents and moved inodes before every namespace reply (closes the pre-change reply race); directory attribute TTL 0 with the entry TTL kept (closes the install race; material measured cost, bounded-mode default pending a quiet-host measurement); mount-time probes plus a kernel-fix table with overrides switch the last two off on fixed kernels; reproducers as harness scenarios with fail-before thresholds, xcheckfs lockstep runs, load-injected campaigns, a run on a patched kernel | 31 (30, 38, 39 and plan 40's R2 committed in practice) |
| 43 | `wip/43-commit-rule.md` | wip | the arbiter study's Option B2: S3 stays the configuration master (lease, epochs, slot fence, membership, designations); the commit rule changes — nothing acknowledged that the configured group (holder + listed backups; delegate + recorded backup or the root) has not committed, a TTL claim fetches the sealed tail from a member, committed rows re-shipped verbatim under their original identity (no fold, no `Records` copy, no replay by rid; a deposed sequencer keeps a member copy), lock grants/releases/expiries/moves and delegation groups and cuts as log records, stranded unacknowledged ops settled (`Abandon`) before `EIO` and never landing late, speculation only in a lone sequencer's own journal; sim oracles first (acked ⇒ committed at the answer, one identity per op, no late landing, rollbacks only of lone answers), model knobs per milestone, campaign and latency gates; plus the clock envelope (rate, not offset: honour/wait clocks, ETag-observation lease claims, anchored promises and tokens), with two clock holes as standalone fixes first | 30 (41 L0, the F1 fix and the campaign tooling committed in practice; recommends the paused takeover-loss, F2 and seed fixes land first) |
| 44 | `done/44-non-blocking-log-writer.md` | done | logging never blocks a daemon thread (the maintainer's decision of 2026-10-18, reversing the 2026-10-14 interim guidance on the evidence of core steps and the holder heartbeat blocked 0.7-12.6 s in stderr writes on a loaded btrfs): the control ring and a bounded sink queue share one mutex and one `Arc<str>` per line, one writer thread per daemon drains it in batches; a full queue drops the newest line (WARN/ERROR keep a reserved eighth), counts it in status and `/metrics` and marks the gap with one summary line; started after the daemonizer's fork, flushed with a bound at exit, before the handover's `exec` and before an abort, panics queued; the harness runs a queue that never drops in practice and fails verification on any drop (summary line, status counter), with a flush barrier before `kill -9`; a log-stall fault point, load item and scenario with a synchronous control arm; measured cheaper per line than today | 31 (21, the F2 heartbeat thread and the P2P close before `exec` committed in practice; coordinates with 41) |

"Committed in practice": the plan does not technically build on the
intermediate milestones, but the protocol is strictly sequential, so
every earlier plan is in the tree anyway.
